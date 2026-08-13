#!/usr/bin/env python3
"""
Program : EIMAR102.py
Purpose : Loans in Arrears Report for HPCCD (Month-End Version)
          Old program: LNCCD002.
          Continuation of EIMAR101 — reads EIMAR101's CCDTXT2 output
          (SAP.PBB.CCDTXT2, opened DISP=MOD in the original JCL) and
          appends this program's own report content onto it, writing
          the combined result to a new EIMAR102 output path.

          Produces two report sections onto CCDTXT2:
            - EIMAR102-A : main 17-bucket (ARREAR)   report, always run
            - EIMAR102-B : special 15-bucket (ARREAR2) report, run ONLY
                           when REPTDAY = '15' (mirrors %MACRO PROC15)
"""

import os
import gc
import duckdb
import polars as pl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pathlib import Path
from datetime import date, datetime, timedelta

from REPTDATE import get_monthly_reptdate_values
from input_date import get_latest_file
# from output_date import build_output_file
# build_output_file() is NOT used: EIMAR102's own output filename must carry
# the exact yymmdd suffix derived from the MONTHLY reptdate below (matching
# EIMAR101's convention), which build_output_file() cannot produce since it
# always derives its date component from the DAILY REPTDATE.get_reptdate_values().

# %INC PGM(PBBLNFMT);
# PBBLNFMT is included at session level in the original SAS source, but no
# PUT(var, fmt.) call anywhere in this program body traces to a
# PBBLNFMT-defined format. No functions from PBBLNFMT.py are imported here
# for that reason.

# ============================================================================
# &HPD MACRO  (referenced as "PRODUCT IN &HPD" in DATA LOANTEM2)
# ============================================================================
HPD_PRODUCTS = (380, 381, 700, 705, 720, 725)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat")

INPUT_BNM_DIR       = STG_DIR
INPUT_BRANCH_FILE   = Path("/sasdata/rawdata/lookup") / "LKP_BRANCH"
EIMAR101_OUTPUT_DIR = BASE_DIR / "output" / "EIMAR101"   # predecessor program's CCDTXT2

CACHE_DIR  = BASE_DIR / "input" / "cache" / "EIMAR101"
OUTPUT_DIR = BASE_DIR / "output" / "EIMAR102"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# CHUNK SIZE FOR STREAMING LARGE .sas7bdat FILES
# ============================================================================
CHUNK_ROWS = 500_000
ROW_LIMIT  = int(os.environ.get("ROW_LIMIT", 0))   # 0 = no limit (test mode)

# ============================================================================
# REPORT PAGE CONFIGURATION
# ============================================================================
PAGE_SIZE     = 60   # lines per page (SAS default)
HEADER_LINES  = 8    # NEWPAGE header block occupies 8 lines (both A and B)
LRECL_CCDTXT2 = 133

CAT_TYPE_LABELS = {
    "A": "(HPD-C)",
    "B": "(HP 380/381)",
    "C": "(AITAB)",
    "D": "(-HPD-)",
}

# ============================================================================
# STEP 1: REPORT DATE  (month-end version -> monthly REPTDATE helper)
# DATA REPTDATE; SET BNM.REPTDATE; CALL SYMPUT(...); RUN;
# No reptdate.parquet exists -- derived from REPTDATE.py instead.
# ============================================================================
print("Step 1: Deriving report date...")

# -----------------------------------------------
# reptdate_values = get_monthly_reptdate_values(year_format="%Y")
# reptdate = reptdate_values.reptdate

# RDATE    = reptdate.strftime("%d/%m/%y")   # &RDATE    : DDMMYY8.
# RDATE2   = reptdate.strftime("%y%m%d")     # yymmdd suffix for own output filename
# REPTYEAR = reptdate.strftime("%Y")         # &REPTYEAR : YEAR4.
# REPTMON  = reptdate.strftime("%m")         # &REPTMON  : Z2.
# REPTDAY  = reptdate.strftime("%d")         # &REPTDAY  : Z2.  (drives %PROC15 gate)
# -----------------------------------------------

# reptdate_values = get_reptdate_values(year_format="%Y")
# reptdate        = reptdate_values.reptdate

# reptdate = date.today() - timedelta(days=1)

# Testing purposes
reptdate = date(2026, 7, 31)

RDATE    = reptdate.strftime("%d/%m/%y")   # &RDATE    : DDMMYY8.
RDATE2   = reptdate.strftime("%y%m%d")     # &RDATE    : YYMMDD6.
REPTYEAR = reptdate.strftime("%Y")         # &REPTYEAR : YEAR4.  (unused downstream, kept for parity)
REPTMON  = reptdate.strftime("%m")         # &REPTMON  : Z2.     (unused downstream, kept for parity)
REPTDAY  = reptdate.strftime("%d")         # &REPTDAY  : Z2.     (unused downstream, kept for parity)


RUN_DAY15_REPORT = (REPTDAY == "15")       # %IF "&REPTDAY" = "15" %THEN %DO;

OUTPUT_FILE = OUTPUT_DIR / f"CCDTXT2_{RDATE2}.txt"

print(f"  Report date     : {RDATE}")
print(f"  REPTDAY         : {REPTDAY}  (day-15 sub-report: {RUN_DAY15_REPORT})")
print(f"  Output file     : {OUTPUT_FILE.name}")

# ============================================================================
# STEP 2: RESOLVE BNM.LOANTEMP INPUT FILE  (fixed filename, GDG(0))
# ============================================================================
print("\nStep 2: Resolving BNM.LOANTEMP input file...")

bnm_path = INPUT_BNM_DIR / "loantemp.sas7bdat"
print(f"  BNM.LOANTEMP : {bnm_path.name}")

# ============================================================================
# HELPER: CACHE STAMP  (skip re-conversion if .sas7bdat hasn't changed)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )

# ============================================================================
# HELPER: STREAM .sas7bdat -> PARQUET  (memory-efficient chunked conversion)
# ============================================================================
def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a large .sas7bdat to Parquet in streaming chunks."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer    = None
    schema    = None
    total     = 0
    rows_read = 0

    reader = pd.read_sas(
        sas_path,
        encoding="latin1",
        chunksize=CHUNK_ROWS,
    )
    for chunk in reader:
        if ROW_LIMIT and rows_read >= ROW_LIMIT:
            break
        if ROW_LIMIT:
            chunk = chunk.iloc[: ROW_LIMIT - rows_read]
        rows_read += len(chunk)

        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if schema is None:
            schema = table.schema
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        else:
            cast_arrays = []
            for field in schema:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        print(f"  [{tag}] WARNING: Cannot cast '{field.name}' "
                              f"from {col.type} to {field.type}: {e} — filling nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)

        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done — {total:,} rows cached.")

# ============================================================================
# STEP 3: CACHE BNM.LOANTEMP TO PARQUET
# ============================================================================
print("\nStep 3: Caching BNM.LOANTEMP to Parquet (if needed)...")

BNM_CACHE = CACHE_DIR / f"{bnm_path.stem}.parquet"

if not _cache_is_fresh(bnm_path, BNM_CACHE):
    sas_to_parquet(bnm_path, BNM_CACHE, "BNM")
else:
    print("  [BNM] Cache fresh — skipping conversion.")

# ============================================================================
# STEP 4: READ BRHFILE  (fixed-width flat file)
# DATA BRHDATA; INFILE BRHFILE LRECL=80;
#   INPUT @2 BRANCH 3. @6 BRHCODE $3.;
# RUN;
# ============================================================================
print("\nStep 4: Reading branch flat file (BRHDATA)...")

brh_rows = []
with open(INPUT_BRANCH_FILE, "rb") as fh:
    for raw in fh:
        line = raw.rstrip(b"\r\n")
        if len(line) < 8:
            continue
        branch  = int(line[1:4].decode("latin1").strip() or 0)   # @2 BRANCH 3.
        brhcode = line[5:8].decode("latin1")                      # @6 BRHCODE $3.
        brh_rows.append({"BRANCH": branch, "BRHCODE": brhcode})

brhdata = pl.DataFrame(brh_rows).with_columns(pl.col("BRANCH").cast(pl.Int64))
branch_to_brhcode = dict(zip(brhdata["BRANCH"].to_list(), brhdata["BRHCODE"].to_list()))
print(f"  BRHDATA rows: {len(brhdata):,}")

# ============================================================================
# STEP 5: BUILD LOANTEM2  (categorise loans; CAT/TYPE per product group)
# DATA LOANTEM2;
#   FORMAT TYPE $13.;
#   SET BNM.LOANTEMP;
#   IF BALANCE > 0 AND BORSTAT NE 'Z';
#   IF PRODUCT IN (380,381,700,705,720,725) THEN DO; CAT='A'; TYPE='(HPD-C)'; OUTPUT; END;
#   IF PRODUCT IN (380,381)                 THEN DO; CAT='B'; TYPE='(HP 380/381)'; OUTPUT; END;
#   IF PRODUCT IN (128,130,131,132)         THEN DO; CAT='C'; TYPE='(AITAB)'; OUTPUT; END;
#   IF PRODUCT IN &HPD                      THEN DO; CAT='D'; TYPE='(-HPD-)'; OUTPUT; END;
# RUN;
#
# ARREAR and ARREAR2 are both carried through: the main report groups by
# ARREAR (17 buckets), the day-15 sub-report groups by ARREAR2 (15 buckets).
# The four independent IF/OUTPUT blocks are intentional duplication (a row
# can qualify for more than one CAT), replicated as UNION ALL.
# ============================================================================
print("\nStep 5: Building LOANTEM2 (categorise loans)...")

con = duckdb.connect(database=":memory:")

_hpd_sql = ",".join(str(p) for p in HPD_PRODUCTS) if HPD_PRODUCTS else "NULL"

loantem2 = con.execute(f"""
    WITH base AS (
        SELECT
            CAST(BRANCH  AS INTEGER) AS BRANCH,
            CAST(ARREAR  AS INTEGER) AS ARREAR,
            CAST(ARREAR2 AS INTEGER) AS ARREAR2,
            CAST(BALANCE AS DOUBLE)  AS BALANCE,
            COALESCE(CAST(BORSTAT AS VARCHAR), '') AS BORSTAT,
            CAST(PRODUCT AS INTEGER) AS PRODUCT
        FROM read_parquet('{BNM_CACHE}')
        WHERE BALANCE > 0 AND COALESCE(CAST(BORSTAT AS VARCHAR), '') <> 'Z'
    )
    SELECT BRANCH, ARREAR, ARREAR2, BALANCE, BORSTAT, PRODUCT, 'A' AS CAT, '(HPD-C)' AS TYPE
    FROM base
    WHERE PRODUCT IN (380,381,700,705,720,725)

    UNION ALL

    SELECT BRANCH, ARREAR, ARREAR2, BALANCE, BORSTAT, PRODUCT, 'B' AS CAT, '(HP 380/381)' AS TYPE
    FROM base
    WHERE PRODUCT IN (380,381)

    UNION ALL

    SELECT BRANCH, ARREAR, ARREAR2, BALANCE, BORSTAT, PRODUCT, 'C' AS CAT, '(AITAB)' AS TYPE
    FROM base
    WHERE PRODUCT IN (128,130,131,132)

    UNION ALL

    SELECT BRANCH, ARREAR, ARREAR2, BALANCE, BORSTAT, PRODUCT, 'D' AS CAT, '(-HPD-)' AS TYPE
    FROM base
    WHERE PRODUCT IN ({_hpd_sql})
""").pl()

con.close()
gc.collect()
print(f"  LOANTEM2 rows: {len(loantem2):,}")

# ============================================================================
# STEP 6: MERGE WITH BRHDATA
# DATA LOANTEMP; MERGE LOANTEM2(IN=PRESENT) BRHDATA; BY BRANCH;
#   IF PRESENT=1 THEN OUTPUT LOANTEMP;
# RUN;
# -> left join of LOANTEM2 with BRHDATA on BRANCH (keep every LOANTEM2 row).
# ============================================================================
print("\nStep 6: Merging with BRHDATA...")

loantem2 = loantem2.with_columns(pl.col("BRANCH").cast(pl.Int64))
loantemp = loantem2.join(brhdata, on="BRANCH", how="left")

del loantem2
gc.collect()
print(f"  LOANTEMP rows: {len(loantemp):,}")

# ============================================================================
# FORMATTING HELPERS
# ============================================================================
def _fmt_comma(value, width, decimals=0):
    """COMMAw.d equivalent – commas if they fit, otherwise no commas."""
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width

    if decimals > 0:
        s = f"{v:,.{decimals}f}"
    else:
        s = f"{int(round(v)):,}"

    if len(s) <= width:
        return s.rjust(width)

    if decimals > 0:
        s = f"{v:.{decimals}f}"
    else:
        s = str(int(round(v)))

    if len(s) <= width:
        return s.rjust(width)

    return s.rjust(width)


def _fmt_z(value, width: int) -> str:
    """Zw. equivalent — zero-padded integer."""
    return f"{int(value or 0):0{width}d}"

# ============================================================================
# ASA LINE-BUFFER HELPERS  (LRECL=133, RECFM=FBA)
# SAS column @N maps directly to buf[N]; buf[0] is reserved for the ASA
# control byte, fused on at finalize time.
# ============================================================================
def _new_buf() -> list:
    return [" "] * LRECL_CCDTXT2


def _place(buf: list, col: int, text: str) -> None:
    start = col
    end = start + len(text)
    buf[start:end] = list(text)


def _finalize(buf: list, asa_char: str) -> str:
    buf[0] = asa_char
    return "".join(buf)

# ============================================================================
# MAIN REPORT (EIMAR102-A) — 17 buckets, keyed on ARREAR
# ============================================================================
def _build_header_main(progid: str, type_label: str, pagecnt: int) -> list:
    lines = []

    buf = _new_buf()
    _place(buf, 1, f"PROGRAM-ID : {progid}")
    _place(buf, 43, "P U B L I C   B A N K   B E R H A D")
    _place(buf, 118, f"PAGE NO.: {pagecnt}")
    lines.append(_finalize(buf, "1"))

    buf = _new_buf()
    _place(buf, 40, "OUTSTANDING LOANS IN ARREARS ")
    _place(buf, 69, f"{type_label:<13.13s}")
    _place(buf, 83, RDATE)
    lines.append(_finalize(buf, " "))

    # buf = _new_buf()
    # _place(buf, 1, " ")
    # lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "BRH    NO          < 1 MTH")
    _place(buf, 33, "NO     1 TO < 2 MTH")
    _place(buf, 58, "NO     2 TO < 3 MTH")
    _place(buf, 84, "NO      3 TO < 4 MTH")
    _place(buf, 111, "NO      4 TO < 5 MTH")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "       NO     5 TO < 6 MTH")
    _place(buf, 33, "NO     6 TO < 7 MTH")
    _place(buf, 58, "NO     7 TO < 8 MTH")
    _place(buf, 84, "NO      8 TO < 9 MTH")
    _place(buf, 111, "NO     9 TO < 10 MTH")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "       NO   10 TO < 11 MTH")
    _place(buf, 33, "NO   11 TO < 12 MTH")
    _place(buf, 58, "NO   12 TO < 18 MTH")
    _place(buf, 84, "NO    18 TO < 24 MTH")
    _place(buf, 111, "NO    24 TO < 36 MTH")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "       NO         > 36 MTH")
    _place(buf, 33, "NO          DEFICIT")
    _place(buf, 58, "NO   SUBTOTAL >=3MTH")
    _place(buf, 84, "NO   SUBTOTAL >=6MTH")
    _place(buf, 111, "NO             TOTAL")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "-" * 40)
    _place(buf, 41, "-" * 40)
    _place(buf, 81, "-" * 40)
    _place(buf, 121, "-" * 10)
    lines.append(_finalize(buf, " "))

    return lines  # 8 lines


def _build_branch_main(branch: int, brhcode: str, noacc: dict, brhamt: dict) -> list:
    sub_brh = sum(brhamt[i] for i in range(4, 18))
    sub_br2 = sum(brhamt[i] for i in range(7, 18))
    sub_acc = sum(noacc[i] for i in range(4, 18))
    sub_ac2 = sum(noacc[i] for i in range(7, 18))
    tot_brh = sub_brh + brhamt[1] + brhamt[2] + brhamt[3]
    sot_acc = sub_acc + noacc[1] + noacc[2] + noacc[3]

    lines = []

    buf = _new_buf()
    _place(buf, 1, _fmt_z(branch, 3))
    _place(buf, 5, _fmt_comma(noacc[1], 7, 0))
    _place(buf, 13, _fmt_comma(brhamt[1], 16, 2))
    _place(buf, 30, _fmt_comma(noacc[2], 7, 0))
    _place(buf, 38, _fmt_comma(brhamt[2], 15, 2))
    _place(buf, 54, _fmt_comma(noacc[3], 7, 0))
    _place(buf, 62, _fmt_comma(brhamt[3], 15, 2))
    _place(buf, 78, _fmt_comma(noacc[4], 8, 0))
    _place(buf, 87, _fmt_comma(brhamt[4], 17, 2))
    _place(buf, 105, _fmt_comma(noacc[5], 8, 0))
    _place(buf, 114, _fmt_comma(brhamt[5], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, f"{(brhcode or ''):<3.3s}")
    _place(buf, 5, _fmt_comma(noacc[6], 7, 0))
    _place(buf, 13, _fmt_comma(brhamt[6], 16, 2))
    _place(buf, 30, _fmt_comma(noacc[7], 7, 0))
    _place(buf, 38, _fmt_comma(brhamt[7], 15, 2))
    _place(buf, 54, _fmt_comma(noacc[8], 7, 0))
    _place(buf, 62, _fmt_comma(brhamt[8], 15, 2))
    _place(buf, 78, _fmt_comma(noacc[9], 8, 0))
    _place(buf, 87, _fmt_comma(brhamt[9], 17, 2))
    _place(buf, 105, _fmt_comma(noacc[10], 8, 0))
    _place(buf, 114, _fmt_comma(brhamt[10], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 5, _fmt_comma(noacc[11], 7, 0))
    _place(buf, 13, _fmt_comma(brhamt[11], 16, 2))
    _place(buf, 30, _fmt_comma(noacc[12], 7, 0))
    _place(buf, 38, _fmt_comma(brhamt[12], 15, 2))
    _place(buf, 54, _fmt_comma(noacc[13], 7, 0))
    _place(buf, 62, _fmt_comma(brhamt[13], 15, 2))
    _place(buf, 78, _fmt_comma(noacc[14], 8, 0))
    _place(buf, 87, _fmt_comma(brhamt[14], 17, 2))
    _place(buf, 105, _fmt_comma(noacc[15], 8, 0))
    _place(buf, 114, _fmt_comma(brhamt[15], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 5, _fmt_comma(noacc[16], 7, 0))
    _place(buf, 13, _fmt_comma(brhamt[16], 16, 2))
    _place(buf, 30, _fmt_comma(noacc[17], 7, 0))
    _place(buf, 38, _fmt_comma(brhamt[17], 15, 2))
    _place(buf, 54, _fmt_comma(sub_acc, 7, 0))
    _place(buf, 62, _fmt_comma(sub_brh, 15, 2))
    _place(buf, 78, _fmt_comma(sub_ac2, 8, 0))
    _place(buf, 87, _fmt_comma(sub_br2, 17, 2))
    _place(buf, 105, _fmt_comma(sot_acc, 8, 0))
    _place(buf, 114, _fmt_comma(tot_brh, 17, 2))
    lines.append(_finalize(buf, " "))

    return lines  # 4 lines


def _build_total_main(totamt: dict, totacc: dict) -> list:
    sgtotbrh = sum(totamt[i] for i in range(4, 18))
    sgtotbr2 = sgtotbrh - totamt[4] - totamt[5] - totamt[6]
    sgtotacc = sum(totacc[i] for i in range(4, 18))
    sgtotac2 = sgtotacc - totacc[4] - totacc[5] - totacc[6]
    gtotbrh  = sgtotbrh + totamt[1] + totamt[2] + totamt[3]
    gtotacc  = sgtotacc + totacc[1] + totacc[2] + totacc[3]

    lines = []

    buf = _new_buf()
    _place(buf, 1, "-" * 40); _place(buf, 41, "-" * 40)
    _place(buf, 81, "-" * 40); _place(buf, 121, "-" * 10)
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "TOT")
    _place(buf, 5, _fmt_comma(totacc[1], 7, 0))
    _place(buf, 13, _fmt_comma(totamt[1], 16, 2))
    _place(buf, 30, _fmt_comma(totacc[2], 7, 0))
    _place(buf, 38, _fmt_comma(totamt[2], 15, 2))
    _place(buf, 54, _fmt_comma(totacc[3], 7, 0))
    _place(buf, 62, _fmt_comma(totamt[3], 15, 2))
    _place(buf, 78, _fmt_comma(totacc[4], 8, 0))
    _place(buf, 87, _fmt_comma(totamt[4], 17, 2))
    _place(buf, 105, _fmt_comma(totacc[5], 8, 0))
    _place(buf, 114, _fmt_comma(totamt[5], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 5, _fmt_comma(totacc[6], 7, 0))
    _place(buf, 13, _fmt_comma(totamt[6], 16, 2))
    _place(buf, 30, _fmt_comma(totacc[7], 7, 0))
    _place(buf, 38, _fmt_comma(totamt[7], 15, 2))
    _place(buf, 54, _fmt_comma(totacc[8], 7, 0))
    _place(buf, 62, _fmt_comma(totamt[8], 15, 2))
    _place(buf, 78, _fmt_comma(totacc[9], 8, 0))
    _place(buf, 87, _fmt_comma(totamt[9], 17, 2))
    _place(buf, 105, _fmt_comma(totacc[10], 8, 0))
    _place(buf, 114, _fmt_comma(totamt[10], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 5, _fmt_comma(totacc[11], 7, 0))
    _place(buf, 13, _fmt_comma(totamt[11], 16, 2))
    _place(buf, 30, _fmt_comma(totacc[12], 7, 0))
    _place(buf, 38, _fmt_comma(totamt[12], 15, 2))
    _place(buf, 54, _fmt_comma(totacc[13], 7, 0))
    _place(buf, 62, _fmt_comma(totamt[13], 15, 2))
    _place(buf, 78, _fmt_comma(totacc[14], 8, 0))
    _place(buf, 87, _fmt_comma(totamt[14], 17, 2))
    _place(buf, 105, _fmt_comma(totacc[15], 8, 0))
    _place(buf, 114, _fmt_comma(totamt[15], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 5, _fmt_comma(totacc[16], 7, 0))
    _place(buf, 13, _fmt_comma(totamt[16], 16, 2))
    _place(buf, 30, _fmt_comma(totacc[17], 7, 0))
    _place(buf, 38, _fmt_comma(totamt[17], 15, 2))
    _place(buf, 54, _fmt_comma(sgtotacc, 7, 0))
    _place(buf, 62, _fmt_comma(sgtotbrh, 15, 2))
    _place(buf, 78, _fmt_comma(sgtotac2, 8, 0))
    _place(buf, 87, _fmt_comma(sgtotbr2, 17, 2))
    _place(buf, 105, _fmt_comma(gtotacc, 8, 0))
    _place(buf, 114, _fmt_comma(gtotbrh, 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "-" * 40); _place(buf, 41, "-" * 40)
    _place(buf, 81, "-" * 40); _place(buf, 121, "-" * 10)
    lines.append(_finalize(buf, " "))

    # lines.append(_finalize(_new_buf(), " "))  # PUT; blank line

    return lines  # 7 lines

# ============================================================================
# DAY-15 SUB-REPORT (EIMAR102-B) — 15 buckets, keyed on ARREAR2
# Mirrors %MACRO PROC15, only executed when REPTDAY = '15'.
# ============================================================================
def _build_header_day15(progid: str, type_label: str, pagecnt: int) -> list:
    lines = []

    buf = _new_buf()
    _place(buf, 1, f"PROGRAM-ID : {progid}")
    _place(buf, 43, "P U B L I C   B A N K   B E R H A D")
    _place(buf, 118, f"PAGE NO.: {pagecnt}")
    lines.append(_finalize(buf, "1"))

    buf = _new_buf()
    _place(buf, 45, "OUTSTANDING LOANS IN ARREARS ")
    _place(buf, 74, f"{type_label:<13.13s}")
    _place(buf, 88, RDATE)
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, " ")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "BRH    NO          < 1 MTH")
    _place(buf, 33, "NO     1 TO < 2 MTH")
    _place(buf, 58, "NO     2 TO < 3 MTH")
    _place(buf, 84, "NO      3 TO < 4 MTH")
    _place(buf, 111, "NO      4 TO < 5 MTH")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "       NO     5 TO < 6 MTH")
    _place(buf, 33, "NO     6 TO < 7 MTH")
    _place(buf, 58, "NO     7 TO < 8 MTH")
    _place(buf, 84, "NO      8 TO < 9 MTH")
    _place(buf, 111, "NO     9 TO < 12 MTH")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "       NO   12 TO < 18 MTH")
    _place(buf, 33, "NO   18 TO < 24 MTH")
    _place(buf, 58, "NO   24 TO < 36 MTH")
    _place(buf, 84, "NO          > 36 MTH")
    _place(buf, 111, "NO           DEFICIT")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 58, "NO   SUBTOTAL >=3MTH")
    _place(buf, 84, "NO   SUBTOTAL >=6MTH")
    _place(buf, 111, "NO             TOTAL")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "-" * 40)
    _place(buf, 41, "-" * 40)
    _place(buf, 81, "-" * 40)
    _place(buf, 121, "-" * 10)
    lines.append(_finalize(buf, " "))

    return lines  # 8 lines


def _build_branch_day15(branch: int, brhcode: str, noacc: dict, brhamt: dict) -> list:
    sub_brh = sum(brhamt[i] for i in range(4, 16))
    sub_br2 = sum(brhamt[i] for i in range(7, 16))
    sub_acc = sum(noacc[i] for i in range(4, 16))
    sub_ac2 = sum(noacc[i] for i in range(7, 16))
    tot_brh = sub_brh + brhamt[1] + brhamt[2] + brhamt[3]
    sot_acc = sub_acc + noacc[1] + noacc[2] + noacc[3]

    lines = []

    buf = _new_buf()
    _place(buf, 1, _fmt_z(branch, 3))
    _place(buf, 5, _fmt_comma(noacc[1], 7, 0))
    _place(buf, 13, _fmt_comma(brhamt[1], 16, 2))
    _place(buf, 30, _fmt_comma(noacc[2], 7, 0))
    _place(buf, 38, _fmt_comma(brhamt[2], 15, 2))
    _place(buf, 54, _fmt_comma(noacc[3], 7, 0))
    _place(buf, 62, _fmt_comma(brhamt[3], 15, 2))
    _place(buf, 78, _fmt_comma(noacc[4], 8, 0))
    _place(buf, 87, _fmt_comma(brhamt[4], 17, 2))
    _place(buf, 105, _fmt_comma(noacc[5], 8, 0))
    _place(buf, 114, _fmt_comma(brhamt[5], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, f"{(brhcode or ''):<3.3s}")
    _place(buf, 5, _fmt_comma(noacc[6], 7, 0))
    _place(buf, 13, _fmt_comma(brhamt[6], 16, 2))
    _place(buf, 30, _fmt_comma(noacc[7], 7, 0))
    _place(buf, 38, _fmt_comma(brhamt[7], 15, 2))
    _place(buf, 54, _fmt_comma(noacc[8], 7, 0))
    _place(buf, 62, _fmt_comma(brhamt[8], 15, 2))
    _place(buf, 78, _fmt_comma(noacc[9], 8, 0))
    _place(buf, 87, _fmt_comma(brhamt[9], 17, 2))
    _place(buf, 105, _fmt_comma(noacc[10], 8, 0))
    _place(buf, 114, _fmt_comma(brhamt[10], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 5, _fmt_comma(noacc[11], 7, 0))
    _place(buf, 13, _fmt_comma(brhamt[11], 16, 2))
    _place(buf, 30, _fmt_comma(noacc[12], 7, 0))
    _place(buf, 38, _fmt_comma(brhamt[12], 15, 2))
    _place(buf, 54, _fmt_comma(noacc[13], 7, 0))
    _place(buf, 62, _fmt_comma(brhamt[13], 15, 2))
    _place(buf, 78, _fmt_comma(noacc[14], 8, 0))
    _place(buf, 87, _fmt_comma(brhamt[14], 17, 2))
    _place(buf, 105, _fmt_comma(noacc[15], 8, 0))
    _place(buf, 114, _fmt_comma(brhamt[15], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 54, _fmt_comma(sub_acc, 7, 0))
    _place(buf, 62, _fmt_comma(sub_brh, 15, 2))
    _place(buf, 78, _fmt_comma(sub_ac2, 8, 0))
    _place(buf, 87, _fmt_comma(sub_br2, 17, 2))
    _place(buf, 105, _fmt_comma(sot_acc, 8, 0))
    _place(buf, 114, _fmt_comma(tot_brh, 17, 2))
    lines.append(_finalize(buf, " "))

    return lines  # 4 lines


def _build_total_day15(totamt: dict, totacc: dict) -> list:
    sgtotbrh = sum(totamt[i] for i in range(4, 16))
    sgtotbr2 = sum(totamt[i] for i in range(7, 16))
    sgtotacc = sum(totacc[i] for i in range(4, 16))
    sgtotac2 = sum(totacc[i] for i in range(7, 16))
    gtotbrh  = sgtotbrh + totamt[1] + totamt[2] + totamt[3]
    gtotacc  = sgtotacc + totacc[1] + totacc[2] + totacc[3]

    lines = []

    buf = _new_buf()
    _place(buf, 1, "-" * 40); _place(buf, 41, "-" * 40)
    _place(buf, 81, "-" * 40); _place(buf, 121, "-" * 10)
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "TOT")
    _place(buf, 5, _fmt_comma(totacc[1], 7, 0))
    _place(buf, 13, _fmt_comma(totamt[1], 16, 2))
    _place(buf, 30, _fmt_comma(totacc[2], 7, 0))
    _place(buf, 38, _fmt_comma(totamt[2], 15, 2))
    _place(buf, 54, _fmt_comma(totacc[3], 7, 0))
    _place(buf, 62, _fmt_comma(totamt[3], 15, 2))
    _place(buf, 78, _fmt_comma(totacc[4], 8, 0))
    _place(buf, 87, _fmt_comma(totamt[4], 17, 2))
    _place(buf, 105, _fmt_comma(totacc[5], 8, 0))
    _place(buf, 114, _fmt_comma(totamt[5], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 5, _fmt_comma(totacc[6], 7, 0))
    _place(buf, 13, _fmt_comma(totamt[6], 16, 2))
    _place(buf, 30, _fmt_comma(totacc[7], 7, 0))
    _place(buf, 38, _fmt_comma(totamt[7], 15, 2))
    _place(buf, 54, _fmt_comma(totacc[8], 7, 0))
    _place(buf, 62, _fmt_comma(totamt[8], 15, 2))
    _place(buf, 78, _fmt_comma(totacc[9], 8, 0))
    _place(buf, 87, _fmt_comma(totamt[9], 17, 2))
    _place(buf, 105, _fmt_comma(totacc[10], 8, 0))
    _place(buf, 114, _fmt_comma(totamt[10], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 5, _fmt_comma(totacc[11], 7, 0))
    _place(buf, 13, _fmt_comma(totamt[11], 16, 2))
    _place(buf, 30, _fmt_comma(totacc[12], 7, 0))
    _place(buf, 38, _fmt_comma(totamt[12], 15, 2))
    _place(buf, 54, _fmt_comma(totacc[13], 7, 0))
    _place(buf, 62, _fmt_comma(totamt[13], 15, 2))
    _place(buf, 78, _fmt_comma(totacc[14], 8, 0))
    _place(buf, 87, _fmt_comma(totamt[14], 17, 2))
    _place(buf, 105, _fmt_comma(totacc[15], 8, 0))
    _place(buf, 114, _fmt_comma(totamt[15], 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 54, _fmt_comma(sgtotacc, 7, 0))
    _place(buf, 62, _fmt_comma(sgtotbrh, 15, 2))
    _place(buf, 78, _fmt_comma(sgtotac2, 8, 0))
    _place(buf, 87, _fmt_comma(sgtotbr2, 17, 2))
    _place(buf, 105, _fmt_comma(gtotacc, 8, 0))
    _place(buf, 114, _fmt_comma(gtotbrh, 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "-" * 40); _place(buf, 41, "-" * 40)
    _place(buf, 81, "-" * 40); _place(buf, 121, "-" * 10)
    lines.append(_finalize(buf, " "))

    lines.append(_finalize(_new_buf(), " "))  # PUT; blank line

    return lines  # 7 lines

# ============================================================================
# GENERIC REPORT ENGINE  (shared by main + day-15 sub-report)
# BY CAT BRANCH row-by-row array accumulation is equivalent to grouping by
# (CAT, BRANCH) and pivoting the arrears buckets keyed by *arrear_col*.
# ============================================================================
def _generate_report(df: pl.DataFrame, arrear_col: str, n_buckets: int,
                      progid: str, build_header, build_branch, build_total) -> list:
    work = df.filter(pl.col("BALANCE") > 0)

    agg = (
        work.group_by(["CAT", "BRANCH", arrear_col])
            .agg([
                pl.col("BALANCE").sum().alias("AMT"),
                pl.len().alias("CNT"),
            ])
    )

    output_lines: list = []
    pagecnt = 0

    cats_present = sorted(agg["CAT"].unique().to_list())

    for cat in cats_present:
        type_label = CAT_TYPE_LABELS.get(cat, "")
        cat_df = agg.filter(pl.col("CAT") == cat)
        branches_present = sorted(cat_df["BRANCH"].unique().to_list())

        totamt = {i: 0.0 for i in range(1, n_buckets + 1)}
        totacc = {i: 0 for i in range(1, n_buckets + 1)}

        pagecnt += 1
        output_lines.extend(build_header(progid, type_label, pagecnt))
        lines_on_page = HEADER_LINES

        for branch in branches_present:
            branch_df = cat_df.filter(pl.col("BRANCH") == branch)

            noacc  = {i: 0 for i in range(1, n_buckets + 1)}
            brhamt = {i: 0.0 for i in range(1, n_buckets + 1)}
            for row in branch_df.iter_rows(named=True):
                a = int(row[arrear_col])
                if 1 <= a <= n_buckets:
                    noacc[a]  = row["CNT"]
                    brhamt[a] = row["AMT"]

            if lines_on_page + 4 > PAGE_SIZE:
                pagecnt += 1
                output_lines.extend(build_header(progid, type_label, pagecnt))
                lines_on_page = HEADER_LINES

            brhcode = branch_to_brhcode.get(branch, "")
            output_lines.extend(build_branch(branch, brhcode, noacc, brhamt))
            lines_on_page += 4

            for i in range(1, n_buckets + 1):
                totamt[i] += brhamt[i]
                totacc[i] += noacc[i]

        if lines_on_page + 7 > PAGE_SIZE:
            pagecnt += 1
            output_lines.extend(build_header(progid, type_label, pagecnt))
            lines_on_page = HEADER_LINES

        total_lines = build_total(totamt, totacc)
        if cat != cats_present[-1]:
            total_lines = total_lines[:-1]   # drop trailing PUT; blank line
        output_lines.extend(total_lines)
        pagecnt = 0

    return output_lines

# ============================================================================
# STEP 7: RUN EIMAR102-A  (main 17-bucket report, always executed)
# ============================================================================
print("\nStep 7: Generating EIMAR102-A report (main, ARREAR)...")

lines_a = _generate_report(
    loantemp, "ARREAR", 17, "EIMAR102-A",
    _build_header_main, _build_branch_main, _build_total_main,
)
print(f"  EIMAR102-A lines: {len(lines_a):,}")

# ============================================================================
# STEP 8: RUN EIMAR102-B  (day-15 15-bucket sub-report, %PROC15 gate)
# %MACRO PROC15; %IF "&REPTDAY"="15" %THEN %DO; ... %END; %MEND; %PROC15;
# ============================================================================
if RUN_DAY15_REPORT:
    print("\nStep 8: Generating EIMAR102-B report (day-15, ARREAR2)...")
    lines_b = _generate_report(
        loantemp, "ARREAR2", 15, "EIMAR102-B",
        _build_header_day15, _build_branch_day15, _build_total_day15,
    )
    print(f"  EIMAR102-B lines: {len(lines_b):,}")
else:
    print("\nStep 8: REPTDAY <> '15' — EIMAR102-B sub-report skipped.")
    lines_b = []

# ============================================================================
# STEP 9: LOCATE EIMAR101's CCDTXT2 OUTPUT  (predecessor to append onto)
# JCL: //CCDTXT2 DD DSN=SAP.PBB.CCDTXT2,DISP=MOD  -- same physical dataset
# EIMAR101 wrote earlier the same day. Resolved by latest dated file rather
# than reconstructing the filename from this program's own reptdate, since
# the two programs' date derivations are independent in this environment.
# ============================================================================
print("\nStep 9: Locating EIMAR101's CCDTXT2 output to append onto...")

eimar101_file = get_latest_file(EIMAR101_OUTPUT_DIR, prefix="CCDTXT2_")
print(f"  Found predecessor file: {eimar101_file.name}")

with open(eimar101_file, "r", encoding="latin1") as fh:
    eimar101_lines = [ln.rstrip("\n") for ln in fh]

print(f"  EIMAR101 lines carried forward: {len(eimar101_lines):,}")

# ============================================================================
# STEP 10: COMBINE AND WRITE OUTPUT  (append EIMAR102 content onto EIMAR101)
# ============================================================================
print("\nStep 10: Writing combined CCDTXT2 output...")

all_lines = eimar101_lines + lines_a + lines_b

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in all_lines:
        fh.write(f"{ln:<{LRECL_CCDTXT2}}\n")

print(f"  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(all_lines):,} "
      f"(EIMAR101: {len(eimar101_lines):,} + EIMAR102-A: {len(lines_a):,} + EIMAR102-B: {len(lines_b):,})")

# ============================================================================
# STEP 11: RESULTS SUMMARY  (printed to terminal)
# ============================================================================
print("\n--- Combined CCDTXT2 (first 20 lines) ---")
for ln in all_lines[:20]:
    print(ln)

print("\n--- EIMAR102 appended section (first 20 lines) ---")
for ln in (lines_a + lines_b)[:20]:
    print(ln)

# ============================================================================
# STEP 12: CLEANUP
# PROC DATASETS LIB=WORK NOLIST; DELETE LOANTEMP LOANTEM2; RUN;
# ============================================================================
del loantemp, lines_a, lines_b, eimar101_lines, all_lines
gc.collect()

print("\nEIMAR102 complete.")
