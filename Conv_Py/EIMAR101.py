#!/usr/bin/env python3
"""
Program : EIMAR101.py
Purpose : Loans in Arrears Report for HPCCD (Month-End Version)
          Old program: LNCCD007. Produces:
            - CCDTXT2  : Printed loans-in-arrears ageing report
                         (ASA carriage control, LRECL=133, RECFM=FBA)
            - CCDTXT7A : Semicolon-delimited branch summary extract
                         (RECFM=FB, LRECL=400)
"""

import os
import gc
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import date, datetime, timedelta

from REPTDATE import get_monthly_reptdate_values
from input_date import get_latest_file
# from output_date import build_output_file
# build_output_file() is NOT used verbatim here: it always derives its date
# component internally from REPTDATE.get_reptdate_values() (the DAILY
# variant). EIMAR101 is explicitly the month-end version of this report and
# must use the monthly reptdate resolved below instead, so the yymmdd
# filename suffix is built directly from reptdate_values (same ordering
# build_output_file would have applied).

# %INC PGM(PBBLNFMT);
# PBBLNFMT is included at session level in the original SAS source, but no
# PUT(var, fmt.) call anywhere in this program body traces to a
# PBBLNFMT-defined format. No functions from PBBLNFMT.py are imported here
# for that reason.

# ============================================================================
# &HPD MACRO
# ============================================================================
# &HPD is referenced twice in the original SAS source ("PRODUCT IN &HPD")
# HPD_PRODUCTS: tuple = ()
HPD_PRODUCTS = (380, 381, 700, 705, 720, 725)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat")

INPUT_BNM_DIR     = STG_DIR
INPUT_BRANCH_FILE = Path("/sasdata/rawdata/lookup") / "LKP_BRANCH"

CACHE_DIR  = BASE_DIR / "input" / "cache" / "EIMAR101"
OUTPUT_DIR = BASE_DIR / "output" / "EIMAR101"

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
HEADER_LINES  = 8     # NEWPAGE header block occupies 8 lines
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

# reptdate_values = get_monthly_reptdate_values(year_format="%Y")

# RDATE    = reptdate_values.ddmmyy8      # PUT(REPTDATE, DDMMYY8.)
# REPTYEAR = reptdate_values.reptyear     # PUT(REPTDATE, YEAR4.)
# REPTMON  = reptdate_values.reptmon      # PUT(MONTH(REPTDATE), Z2.)
# REPTDAY  = reptdate_values.reptday      # PUT(DAY(REPTDATE), Z2.)

# _date_suffix = reptdate_values.reptdate.strftime("%y%m%d")   # yymmdd suffix


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

# CCDTXT2_FILE  = OUTPUT_DIR / f"CCDTXT2_{_date_suffix}.txt"
# CCDTXT7A_FILE = OUTPUT_DIR / f"CCDTXT7A_{_date_suffix}.txt"

CCDTXT2_FILE  = OUTPUT_DIR / f"CCDTXT2_{RDATE2}.txt"
CCDTXT7A_FILE = OUTPUT_DIR / f"CCDTXT7A_{RDATE2}.txt"

print(f"  Report date : {RDATE}")
print(f"  Output dir  : {OUTPUT_DIR}")
print(f"  CCDTXT2     : {CCDTXT2_FILE.name}")
print(f"  CCDTXT7A    : {CCDTXT7A_FILE.name}")

# ============================================================================
# STEP 2: RESOLVE BNM.LOANTEMP INPUT FILE  (BNM = .sas7bdat dataset)
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

# ----------------------------------------------------------------------
# PROC SORT DATA=BNM.LOANTEMP OUT=LOAN; BY BRANCH ARREAR2; RUN;
# The WORK.LOAN dataset produced by this sort is never referenced again
# anywhere else in the SAS program (every subsequent step reads from
# BNM.LOANTEMP directly, or from LOANTEM2/LOANTEMP built below). It is
# dead output and has therefore been omitted; no equivalent materialised
# sort is needed.
# ----------------------------------------------------------------------

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
            CAST(ARREAR2 AS INTEGER) AS ARREAR2,
            CAST(BALANCE AS DOUBLE)  AS BALANCE,
            COALESCE(CAST(BORSTAT AS VARCHAR), '') AS BORSTAT,
            CAST(PRODUCT AS INTEGER) AS PRODUCT
        FROM read_parquet('{BNM_CACHE}')
        WHERE BALANCE > 0 AND COALESCE(CAST(BORSTAT AS VARCHAR), '') <> 'Z'
    )
    SELECT BRANCH, ARREAR2, BALANCE, BORSTAT, PRODUCT, 'A' AS CAT, '(HPD-C)' AS TYPE
    FROM base
    WHERE PRODUCT IN (380,381,700,705,720,725)

    UNION ALL

    SELECT BRANCH, ARREAR2, BALANCE, BORSTAT, PRODUCT, 'B' AS CAT, '(HP 380/381)' AS TYPE
    FROM base
    WHERE PRODUCT IN (380,381)

    UNION ALL

    SELECT BRANCH, ARREAR2, BALANCE, BORSTAT, PRODUCT, 'C' AS CAT, '(AITAB)' AS TYPE
    FROM base
    WHERE PRODUCT IN (128,130,131,132)

    UNION ALL

    SELECT BRANCH, ARREAR2, BALANCE, BORSTAT, PRODUCT, 'D' AS CAT, '(-HPD-)' AS TYPE
    FROM base
    WHERE PRODUCT IN ({_hpd_sql})
""").pl()

con.close()
gc.collect()
print(f"  LOANTEM2 rows: {len(loantem2):,}")

# ----------------------------------------------------------------------
# PROC SORT DATA=LOANTEM2; BY BRANCH; RUN;
# This sort existed only to prepare the BY-BRANCH merge below; the
# equivalent join/groupby operations used here do not require it, so it
# has been omitted for efficiency.
# ----------------------------------------------------------------------

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

# ----------------------------------------------------------------------
# PROC SORT DATA=LOANTEMP; BY CAT BRANCH ARREAR2; RUN;
# Superseded by the group-by/sort-on-output logic in generate_arrears_report
# and build_loan7a_rows below; omitted for efficiency.
# ----------------------------------------------------------------------

# ============================================================================
# FORMATTING HELPERS
# ============================================================================
# def _fmt_comma(value, width: int, decimals: int = 0) -> str:
#     """COMMAw.d equivalent — comma-separated, right-justified to *width*."""
#     if value is None:
#         return " " * width
#     try:
#         v = float(value)
#     except (TypeError, ValueError):
#         return " " * width
#     if decimals > 0:
#         s = f"{v:,.{decimals}f}"
#     else:
#         s = f"{int(round(v)):,}"
#     return s.rjust(width)

def _fmt_comma(value, width, decimals=0):
    """COMMAw.d equivalent – commas if they fit, otherwise no commas."""
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width

    # Try with commas and proper decimals
    if decimals > 0:
        s = f"{v:,.{decimals}f}"
    else:
        s = f"{int(round(v)):,}"

    if len(s) <= width:
        return s.rjust(width)

    # Too wide -> drop commas
    if decimals > 0:
        s = f"{v:.{decimals}f}"
    else:
        s = str(int(round(v)))

    if len(s) <= width:
        return s.rjust(width)

    # If too wide, Fallback to BASIC formatting (shouldn't happen with correct widths)
    # It will just right-justify without further truncation.
    return s.rjust(width)


def _fmt_z(value, width: int) -> str:
    """Zw. equivalent — zero-padded integer."""
    return f"{int(value or 0):0{width}d}"


def _fmt_best(value) -> str:
    """Mimic SAS default (unformatted) numeric-to-character conversion."""
    if value is None:
        return ""
    v = float(value)
    if v.is_integer():
        return str(int(v))
    return f"{v:.2f}"

# ============================================================================
# ASA LINE-BUFFER HELPERS  (LRECL=133, RECFM=FBA)
# SAS column @N maps directly to buf[N-1]; the ASA control byte then
# occupies buf[0], fused onto (overwriting) whatever column-1 content the
# SAS @1 addressing would otherwise have placed there.
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
# HEADER BLOCK  (HEADER=NEWPAGE label; 8 lines; first line carries ASA '1')
# ============================================================================
def _build_header_lines(progid: str, type_label: str, pagecnt: int) -> list:
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
    # lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "BRH    NO          < 1 MTH")
    _place(buf, 33, "NO     1 TO < 2 MTH")
    _place(buf, 58, "NO     2 TO < 3 MTH")
    _place(buf, 84, "NO      3 TO < 4 MTH")
    _place(buf, 111, "NO      4 TO < 5 MTH")
    lines.append(_finalize(buf, "0"))

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
    _place(buf, 111, "NO   SUBTOTAL >=3MTH")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 84, "NO   SUBTOTAL >=6MTH")
    _place(buf, 111, "NO             TOTAL")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "-" * 40)
    _place(buf, 41, "-" * 40)
    _place(buf, 81, "-" * 40)
    _place(buf, 121, "-" * 10)
    lines.append(_finalize(buf, " "))

    return lines  # HEADER_LINES = 8

# ============================================================================
# BRANCH DETAIL BLOCK  (4 lines per branch; LAST.BRANCH accumulation)
# ============================================================================
def _build_branch_lines(branch: int, brhcode: str, noacc: dict, brhamt: dict):
    sub_brh = sum(brhamt[i] for i in range(4, 15))
    sub_br2 = sum(brhamt[i] for i in range(7, 15))
    sub_acc = sum(noacc[i] for i in range(4, 15))
    sub_ac2 = sum(noacc[i] for i in range(7, 15))
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
    _place(buf, 105, _fmt_comma(sub_acc, 8, 0))
    _place(buf, 114, _fmt_comma(sub_brh, 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 78, _fmt_comma(sub_ac2, 8, 0))
    _place(buf, 87, _fmt_comma(sub_br2, 17, 2))
    _place(buf, 105, _fmt_comma(sot_acc, 8, 0))
    _place(buf, 114, _fmt_comma(tot_brh, 17, 2))
    lines.append(_finalize(buf, " "))

    return lines  # 4 lines

# ============================================================================
# CAT GRAND-TOTAL BLOCK  (LAST.CAT; 7 lines)
# ============================================================================
def _build_total_lines(totamt: dict, totacc: dict) -> list:
    sgtotbrh = sum(totamt[i] for i in range(4, 15))
    sgtotbr2 = sum(totamt[i] for i in range(7, 15))
    sgtotacc = sum(totacc[i] for i in range(4, 15))
    sgtotac2 = sum(totacc[i] for i in range(7, 15))
    gtotbrh  = sgtotbrh + totamt[1] + totamt[2] + totamt[3]
    gtotacc  = sgtotacc + totacc[1] + totacc[2] + totacc[3]

    lines = []

    buf = _new_buf()
    _place(buf, 1, "-" * 40)
    _place(buf, 41, "-" * 40)
    _place(buf, 81, "-" * 40)
    _place(buf, 121, "-" * 10)
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
    _place(buf, 105, _fmt_comma(sgtotacc, 8, 0))
    _place(buf, 114, _fmt_comma(sgtotbrh, 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 78, _fmt_comma(sgtotac2, 8, 0))
    _place(buf, 87, _fmt_comma(sgtotbr2, 17, 2))
    _place(buf, 105, _fmt_comma(gtotacc, 8, 0))
    _place(buf, 114, _fmt_comma(gtotbrh, 17, 2))
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "-" * 40)
    _place(buf, 41, "-" * 40)
    _place(buf, 81, "-" * 40)
    _place(buf, 121, "-" * 10)
    lines.append(_finalize(buf, " "))

    # PUT;  (blank line, PAGECNT = 0 handled by caller)
    lines.append(_finalize(_new_buf(), " "))

    return lines  # 7 lines

# ============================================================================
# %PRNPROC EQUIVALENT
# BY CAT BRANCH ARREAR2 row-by-row array accumulation is equivalent to
# grouping by (CAT, BRANCH, ARREAR2) and pivoting the 14 arrears buckets.
# ============================================================================
def generate_arrears_report(df: pl.DataFrame, progid: str) -> list:
    # IF BALANCE GT 0 THEN DO; BRHAMT(ARREAR2)+BALANCE;
    #    /* IF PRODUCT IN (110,115,700,705) THEN DO;
    #       IF BALANCE GT 200 THEN NOACC(ARREAR2)+1; END; ELSE */
    #    NOACC(ARREAR2)+1; END;
    work = df.filter(pl.col("BALANCE") > 0)

    agg = (
        work.group_by(["CAT", "BRANCH", "ARREAR2"])
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

        totamt = {i: 0.0 for i in range(1, 15)}
        totacc = {i: 0 for i in range(1, 15)}

        # FIRST.CAT -> PUT _PAGE_ (forced new page at the start of every CAT)
        pagecnt += 1
        output_lines.extend(_build_header_lines(progid, type_label, pagecnt))
        lines_on_page = HEADER_LINES

        for branch in branches_present:
            branch_df = cat_df.filter(pl.col("BRANCH") == branch)

            noacc  = {i: 0 for i in range(1, 15)}
            brhamt = {i: 0.0 for i in range(1, 15)}
            for row in branch_df.iter_rows(named=True):
                a = int(row["ARREAR2"])
                if 1 <= a <= 14:
                    noacc[a]  = row["CNT"]
                    brhamt[a] = row["AMT"]

            # Automatic page overflow before this branch's 4-line block
            if lines_on_page + 4 > PAGE_SIZE:
                pagecnt += 1
                output_lines.extend(_build_header_lines(progid, type_label, pagecnt))
                lines_on_page = HEADER_LINES

            brhcode = branch_to_brhcode.get(branch, "")
            output_lines.extend(_build_branch_lines(branch, brhcode, noacc, brhamt))
            lines_on_page += 4

            for i in range(1, 15):
                totamt[i] += brhamt[i]
                totacc[i] += noacc[i]

        # # LAST.CAT -> grand total block (7 lines)
        # if lines_on_page + 7 > PAGE_SIZE:
        #     pagecnt += 1
        #     output_lines.extend(_build_header_lines(progid, type_label, pagecnt))
        #     lines_on_page = HEADER_LINES

        # output_lines.extend(_build_total_lines(totamt, totacc))
        # pagecnt = 0   # PAGECNT = 0; (reset after LAST.CAT)

        # LAST.CAT -> grand total block (7 lines)
        if lines_on_page + 7 > PAGE_SIZE:
            pagecnt += 1
            output_lines.extend(_build_header_lines(progid, type_label, pagecnt))
            lines_on_page = HEADER_LINES

        total_lines = _build_total_lines(totamt, totacc)
        # If there's another CAT coming, drop the final blank line (PUT;)
        if cat != cats_present[-1]:
            total_lines = total_lines[:-1]
        output_lines.extend(total_lines)
        pagecnt = 0   # PAGECNT = 0; (reset after LAST.CAT)

    return output_lines

# ============================================================================
# DATA LOAN7A (KEEP=BRHCODE TYPE BRHAMT1-14 NOACC1-14);
#   SET PRNDATA (the EIMAR101-B pass);
#   Same BY CAT BRANCH ARREAR2 array accumulation as %PRNPROC, one output
#   row per branch.
# ============================================================================
def build_loan7a_rows(df: pl.DataFrame) -> list:
    work = df.filter(pl.col("BALANCE") > 0)

    agg = (
        work.group_by(["CAT", "BRANCH", "ARREAR2"])
            .agg([
                pl.col("BALANCE").sum().alias("AMT"),
                pl.len().alias("CNT"),
            ])
    )

    rows = []
    for cat in sorted(agg["CAT"].unique().to_list()):
        type_label = CAT_TYPE_LABELS.get(cat, "")
        cat_df = agg.filter(pl.col("CAT") == cat)
        for branch in sorted(cat_df["BRANCH"].unique().to_list()):
            branch_df = cat_df.filter(pl.col("BRANCH") == branch)
            noacc  = {i: 0 for i in range(1, 15)}
            brhamt = {i: 0.0 for i in range(1, 15)}
            for row in branch_df.iter_rows(named=True):
                a = int(row["ARREAR2"])
                if 1 <= a <= 14:
                    noacc[a]  = row["CNT"]
                    brhamt[a] = row["AMT"]
            rows.append({
                "BRHCODE": branch_to_brhcode.get(branch, ""),
                "TYPE": type_label,
                "NOACC": noacc,
                "BRHAMT": brhamt,
            })
    return rows


def write_ccdtxt7a(rows: list, path: Path) -> None:
    header_fields = ["BRHCODE", "TYPE"]
    for i in range(1, 15):
        header_fields.append(f"NOACC{i}")
        header_fields.append(f"BRHAMT{i}")

    with open(path, "w", encoding="latin1") as fh:
        fh.write(";".join(header_fields) + ";\n")
        for row in rows:
            fields = [row["BRHCODE"], row["TYPE"]]
            for i in range(1, 15):
                fields.append(_fmt_best(row["NOACC"][i]))
                fields.append(_fmt_best(row["BRHAMT"][i]))
            fh.write(";".join(fields) + ";\n")

# ============================================================================
# STEP 7: RUN A) HP DIRECT  -- DATA PRNDATA; SET LOANTEMP; PROGID='EIMAR101-A'
# ============================================================================
print("\nStep 7: Generating EIMAR101-A report (HP Direct)...")
# CALL SYMPUT('RPTTITLE', 'OUTSTANDING LOANS IN ARREARS '); -- unused, commented in source
PROGID_A = "EIMAR101-A"
lines_a = generate_arrears_report(loantemp, PROGID_A)
print(f"  EIMAR101-A lines: {len(lines_a):,}")

# ============================================================================
# STEP 8: RUN B) ARREARS 3-8 MTHS
# DATA PRNDATA; SET LOANTEMP;
#   IF TYPE IN ('(HPD-C)','(-HPD-)') THEN DELETE;
#   IF (BORSTAT NE 'F' AND BORSTAT NE 'I' AND BORSTAT NE 'R') AND
#      PRODUCT IN &HPD;
#   PROGID = 'EIMAR101-B';
# ============================================================================
print("\nStep 8: Generating EIMAR101-B report (Arrears 3-8 Mths)...")
# CALL SYMPUT('RPTTITLE', 'OUTSTANDING LOANS IN ARREARS(EXCLUDE BORR. STAT F/I/R/T) '); -- unused, commented in source
PROGID_B = "EIMAR101-B"

part_b_df = loantemp.filter(
    (~pl.col("TYPE").is_in(["(HPD-C)", "(-HPD-)"]))
    & (~pl.col("BORSTAT").is_in(["F", "I", "R"]))
    & (pl.col("PRODUCT").is_in(list(HPD_PRODUCTS)))
)
lines_b = generate_arrears_report(part_b_df, PROGID_B)
print(f"  EIMAR101-B lines: {len(lines_b):,}")

# ============================================================================
# STEP 9: WRITE CCDTXT2  (ASA report)
# ============================================================================
print("\nStep 9: Writing CCDTXT2...")

all_lines = lines_a + lines_b

with open(CCDTXT2_FILE, "w", encoding="latin1") as fh:
    for ln in all_lines:
        fh.write(ln + "\n")

print(f"  CCDTXT2 written : {CCDTXT2_FILE}")
print(f"  Total lines     : {len(all_lines):,}")

# ============================================================================
# STEP 10: WRITE CCDTXT7A  (LOAN7A branch summary extract from EIMAR101-B)
# OPTIONS NONUMBER NODATE;
# ============================================================================
print("\nStep 10: Building CCDTXT7A (branch summary extract)...")

loan7a_rows = build_loan7a_rows(part_b_df)
write_ccdtxt7a(loan7a_rows, CCDTXT7A_FILE)

print(f"  CCDTXT7A written : {CCDTXT7A_FILE}")
print(f"  Total rows       : {len(loan7a_rows):,}")

# ============================================================================
# STEP 11: RESULTS SUMMARY  (printed to terminal)
# ============================================================================
print("\n--- CCDTXT2 (first 20 lines) ---")
for ln in all_lines[:20]:
    print(ln)

print("\n--- CCDTXT7A (first 5 rows) ---")
for row in loan7a_rows[:5]:
    print(f"  BRHCODE={row['BRHCODE']} TYPE={row['TYPE']} "
          f"NOACC1={row['NOACC'][1]} BRHAMT1={row['BRHAMT'][1]}")

# ============================================================================
# STEP 12: CLEANUP
# PROC DATASETS LIB=WORK NOLIST; DELETE LOANTEMP LOANTEM2 PRNDATA LOAN7A; RUN;
# ============================================================================
del loantemp, part_b_df, lines_a, lines_b, all_lines, loan7a_rows
gc.collect()

print("\nEIMAR101 complete.")
