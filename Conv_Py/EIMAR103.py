#!/usr/bin/env python3
"""
Program : EIMAR103.py
Purpose : Loans in Arrears Classified as NPL Report for HPCCD
          (Month-End Version). Old program: LNCCD008.
          Continuation of EIMAR102 — reads EIMAR102's CCDTXT2 output
          (SAP.PBB.CCDTXT2, opened DISP=MOD in the original JCL) and
          appends this program's own report content onto it, writing
          the combined result to a new EIMAR103 output path.

          Produces two report sections onto CCDTXT2 (14-bucket, ARREAR2):
            - EIMAR103-A : all NPL-qualifying loans, always run
            - EIMAR103-B : NPL loans excluding BORSTAT F/I/R, restricted
                           to &HPD / (15,20,71,72) products, always run

NOTE (NKW/SAM email 19-OCT-2001): BORSTAT 'I' exclusion for Housing Loan
and Fixed Loan was removed from this program per that instruction — this
is already reflected in the SAS source as-is (no further code change
needed here beyond faithfully reproducing the existing filter logic).
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
# build_output_file() is NOT used: EIMAR103's own output filename must carry
# the exact yymmdd suffix derived from the MONTHLY reptdate below (matching
# EIMAR101/EIMAR102's convention), which build_output_file() cannot produce
# since it always derives its date component from the DAILY
# REPTDATE.get_reptdate_values().

# %INC PGM(PBBLNFMT);
# PBBLNFMT is included at session level in the original SAS source, but no
# PUT(var, fmt.) call anywhere in this program body traces to a
# PBBLNFMT-defined format. No functions from PBBLNFMT.py are imported here
# for that reason.

# ============================================================================
# &HPD MACRO  (referenced as "PRODUCT IN &HPD" in DATA LOANTEMP / PRNDATA-B)
# ============================================================================
HPD_PRODUCTS   = (380, 381, 700, 705, 720, 725)
EXTRA_B_PRODUCTS = (15, 20, 71, 72)   # PRODUCT IN (15,20,71,72) in PRNDATA-B filter

# CAT product lists  (DATA LOANTEMP categorisation)
CAT_A_PRODUCTS = HPD_PRODUCTS + EXTRA_B_PRODUCTS          # (380,381,700,705,720,725,15,20,71,72)
CAT_B_PRODUCTS = (380, 381)
CAT_C_PRODUCTS = (103, 104, 107, 108, 128, 130, 131, 132)
CAT_D_PRODUCTS = HPD_PRODUCTS

CAT_TYPE_LABELS = {
    "A": "(HPD-C)",
    "B": "(HP 380/381)",
    "C": "(AITAB)",
    "D": "(-HPD-)",
}

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat")

INPUT_BNM_DIR       = STG_DIR
INPUT_BRANCH_FILE   = Path("/sasdata/rawdata/lookup") / "LKP_BRANCH"
EIMAR102_OUTPUT_DIR = BASE_DIR / "output" / "EIMAR102"   # predecessor program's CCDTXT2

CACHE_DIR  = BASE_DIR / "input" / "cache" / "EIMAR101"
OUTPUT_DIR = BASE_DIR / "output" / "EIMAR103"

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
HEADER_LINES  = 8    # NEWPAGE header block occupies 8 lines
LRECL_CCDTXT2 = 133
N_BUCKETS     = 14   # ARREAR2 buckets

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

OUTPUT_FILE = OUTPUT_DIR / f"CCDTXT2_{RDATE2}.txt"

print(f"  Report date     : {RDATE}")
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

# ----------------------------------------------------------------------
# PROC SORT DATA=BNM.LOANTEMP OUT=LOAN; BY BRANCH ARREAR2; RUN;
# DATA LOANTEMP; SET LOAN; ... (categorisation step below)
# The sort exists only to give LOANTEMP a particular physical row order;
# the categorisation logic itself performs no BY-group / FIRST.-LAST.
# processing, so row order has no effect on its output. The dataset is
# re-sorted again later (PROC SORT DATA=LOAN1 OUT=LOAN1; BY CAT BRANCH
# ARREAR2;) immediately before the row order actually matters (PRNPROC).
# The intermediate sort is therefore redundant and has been omitted;
# LOANTEMP is built directly from the cached BNM.LOANTEMP parquet.
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
# STEP 5: BUILD LOANTEMP  (categorise loans classified as NPL; CAT/TYPE)
# DATA LOANTEMP;
#   FORMAT TYPE $13.;
#   SET LOAN;
#   CENSUS9 = SUBSTR(PUT(CENSUS,8.2),7,1);
#   IF BALANCE > 0 AND BORSTAT NE 'Z';
#   IF ARREAR2 > 3 OR BORSTAT='R' OR BORSTAT='I' OR BORSTAT='F'
#      OR CENSUS9='9' OR USER5='N' THEN DO;
#      <4 CAT blocks, each requiring PRODUCT IN (...) AND
#         (BORSTAT IN ('R','I','F') OR ARREAR2>3 OR USER5='N')>
#   END;
#
# NOTE: The per-CAT condition (BORSTAT IN ('R','I','F') OR ARREAR2>3 OR
# USER5='N') is always a subset of the outer gate (which additionally
# allows CENSUS9='9' on its own). Since CENSUS9 is never re-tested inside
# the per-CAT condition, a row admitted to the outer DO block solely via
# CENSUS9='9' can never satisfy any per-CAT condition on its own — the
# outer gate is therefore logically redundant for output purposes, but is
# still evaluated explicitly below (as OUTER_GATE) to mirror the SAS
# nesting exactly.
#
# CENSUS9 = tenths digit of CENSUS under an 8-wide, 2-decimal PUT format
# (SAS PUT(CENSUS,8.2) is always exactly 8 chars wide with the decimal
# point fixed at position 6, so position 7 is always the tenths digit).
# ============================================================================
print("\nStep 5: Building LOANTEMP (categorise NPL loans)...")

con = duckdb.connect(database=":memory:")

_cat_a_sql = ",".join(str(p) for p in CAT_A_PRODUCTS)
_cat_b_sql = ",".join(str(p) for p in CAT_B_PRODUCTS)
_cat_c_sql = ",".join(str(p) for p in CAT_C_PRODUCTS)
_cat_d_sql = ",".join(str(p) for p in CAT_D_PRODUCTS)

loantemp = con.execute(f"""
    WITH base AS (
        SELECT
            CAST(BRANCH  AS INTEGER) AS BRANCH,
            CAST(ARREAR2 AS INTEGER) AS ARREAR2,
            CAST(BALANCE AS DOUBLE)  AS BALANCE,
            COALESCE(CAST(BORSTAT AS VARCHAR), '') AS BORSTAT,
            COALESCE(CAST(USER5   AS VARCHAR), '') AS USER5,
            CAST(PRODUCT AS INTEGER) AS PRODUCT,
            SUBSTR(printf('%8.2f', CAST(CENSUS AS DOUBLE)), 7, 1) AS CENSUS9
        FROM read_parquet('{BNM_CACHE}')
        WHERE BALANCE > 0 AND COALESCE(CAST(BORSTAT AS VARCHAR), '') <> 'Z'
    ),
    gated AS (
        SELECT *,
            (ARREAR2 > 3 OR BORSTAT IN ('R','I','F')
                OR CENSUS9 = '9' OR USER5 = 'N')      AS OUTER_GATE,
            (BORSTAT IN ('R','I','F') OR ARREAR2 > 3
                OR USER5 = 'N')                        AS CAT_GATE
        FROM base
    )
    SELECT BRANCH, ARREAR2, BALANCE, BORSTAT, USER5, PRODUCT, 'A' AS CAT, '(HPD-C)' AS TYPE
    FROM gated
    WHERE OUTER_GATE AND CAT_GATE AND PRODUCT IN ({_cat_a_sql})

    UNION ALL

    SELECT BRANCH, ARREAR2, BALANCE, BORSTAT, USER5, PRODUCT, 'B' AS CAT, '(HP 380/381)' AS TYPE
    FROM gated
    WHERE OUTER_GATE AND CAT_GATE AND PRODUCT IN ({_cat_b_sql})

    UNION ALL

    SELECT BRANCH, ARREAR2, BALANCE, BORSTAT, USER5, PRODUCT, 'C' AS CAT, '(AITAB)' AS TYPE
    FROM gated
    WHERE OUTER_GATE AND CAT_GATE AND PRODUCT IN ({_cat_c_sql})

    UNION ALL

    SELECT BRANCH, ARREAR2, BALANCE, BORSTAT, USER5, PRODUCT, 'D' AS CAT, '(-HPD-)' AS TYPE
    FROM gated
    WHERE OUTER_GATE AND CAT_GATE AND PRODUCT IN ({_cat_d_sql})
""").pl()

con.close()
gc.collect()
print(f"  LOANTEMP rows: {len(loantemp):,}")

# ----------------------------------------------------------------------
# PROC SORT DATA=LOANTEMP; BY BRANCH; RUN;
# This sort existed only to prepare the BY-BRANCH merge below; the
# equivalent join used here does not require it, so it has been omitted
# for efficiency.
# ----------------------------------------------------------------------

# ============================================================================
# STEP 6: MERGE WITH BRHDATA -> LOAN1
# DATA LOAN1; MERGE LOANTEMP(IN=PRESENT) BRHDATA; BY BRANCH;
#   IF PRESENT=1 THEN OUTPUT LOAN1;
# RUN;
# -> left join of LOANTEMP with BRHDATA on BRANCH (keep every LOANTEMP row).
# ============================================================================
print("\nStep 6: Merging with BRHDATA...")

loantemp = loantemp.with_columns(pl.col("BRANCH").cast(pl.Int64))
loan1 = loantemp.join(brhdata, on="BRANCH", how="left")

del loantemp
gc.collect()
print(f"  LOAN1 rows: {len(loan1):,}")

# ----------------------------------------------------------------------
# PROC SORT DATA=LOAN1 OUT=LOAN1; BY CAT BRANCH ARREAR2; RUN;
# Superseded by the group-by/sort-on-output logic in _generate_report
# below; omitted for efficiency.
# ----------------------------------------------------------------------

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
# HEADER BLOCK  (HEADER=NEWPAGE label; 8 lines; first line carries ASA '1')
# ============================================================================
def _build_header_npl(progid: str, type_label: str, pagecnt: int) -> list:
    lines = []

    buf = _new_buf()
    _place(buf, 1, f"PROGRAM-ID : {progid}")
    _place(buf, 43, "P U B L I C   B A N K   B E R H A D")
    _place(buf, 118, f"PAGE NO.: {pagecnt}")
    lines.append(_finalize(buf, "1"))

    buf = _new_buf()
    _place(buf, 41, "OUTSTANDING LOANS CLASSIFIED AS NPL ")
    _place(buf, 77, f"{type_label:<13.13s}")
    _place(buf, 91, RDATE)
    lines.append(_finalize(buf, " "))

    # buf = _new_buf()
    # _place(buf, 1, " ")
    # lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "BRH     NO         < 1 MTH")
    _place(buf, 34, "NO     1 TO < 2 MTH")
    _place(buf, 59, "NO     2 TO < 3 MTH")
    _place(buf, 84, "NO      3 TO < 4 MTH")
    _place(buf, 111, "NO      4 TO < 5 MTH")
    lines.append(_finalize(buf, "0"))

    buf = _new_buf()
    _place(buf, 1, "        NO    5 TO < 6 MTH")
    _place(buf, 34, "NO     6 TO < 7 MTH")
    _place(buf, 59, "NO     7 TO < 8 MTH")
    _place(buf, 84, "NO      8 TO < 9 MTH")
    _place(buf, 111, "NO     9 TO < 12 MTH")
    lines.append(_finalize(buf, " "))

    buf = _new_buf()
    _place(buf, 1, "        NO  12 TO < 18 MTH")
    _place(buf, 34, "NO   18 TO < 24 MTH")
    _place(buf, 59, "NO   24 TO < 36 MTH")
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

    return lines  # 8 lines


# ============================================================================
# BRANCH DETAIL BLOCK  (4 lines per branch; LAST.BRANCH accumulation)
# ============================================================================
def _build_branch_npl(branch: int, brhcode: str, noacc: dict, brhamt: dict) -> list:
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
def _build_total_npl(totamt: dict, totacc: dict) -> list:
    sgtotbrh = sum(totamt[i] for i in range(4, 15))
    sgtotbr2 = sum(totamt[i] for i in range(7, 15))
    sgtotacc = sum(totacc[i] for i in range(4, 15))
    sgtotac2 = sum(totacc[i] for i in range(7, 15))
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
    _place(buf, 1, "-" * 40); _place(buf, 41, "-" * 40)
    _place(buf, 81, "-" * 40); _place(buf, 121, "-" * 10)
    lines.append(_finalize(buf, " "))

    lines.append(_finalize(_new_buf(), " "))  # PUT; blank line

    return lines  # 7 lines

# ============================================================================
# %PRNPROC EQUIVALENT
# BY CAT BRANCH ARREAR2 row-by-row array accumulation is equivalent to
# grouping by (CAT, BRANCH, ARREAR2) and pivoting the 14 arrears buckets.
# ============================================================================
def _generate_report(df: pl.DataFrame, progid: str) -> list:
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

        totamt = {i: 0.0 for i in range(1, N_BUCKETS + 1)}
        totacc = {i: 0 for i in range(1, N_BUCKETS + 1)}

        # FIRST.CAT -> PUT _PAGE_ (forced new page at the start of every CAT)
        pagecnt += 1
        output_lines.extend(_build_header_npl(progid, type_label, pagecnt))
        lines_on_page = HEADER_LINES

        for branch in branches_present:
            branch_df = cat_df.filter(pl.col("BRANCH") == branch)

            noacc  = {i: 0 for i in range(1, N_BUCKETS + 1)}
            brhamt = {i: 0.0 for i in range(1, N_BUCKETS + 1)}
            for row in branch_df.iter_rows(named=True):
                a = int(row["ARREAR2"])
                if 1 <= a <= N_BUCKETS:
                    noacc[a]  = row["CNT"]
                    brhamt[a] = row["AMT"]

            if lines_on_page + 4 > PAGE_SIZE:
                pagecnt += 1
                output_lines.extend(_build_header_npl(progid, type_label, pagecnt))
                lines_on_page = HEADER_LINES

            brhcode = branch_to_brhcode.get(branch, "")
            output_lines.extend(_build_branch_npl(branch, brhcode, noacc, brhamt))
            lines_on_page += 4

            for i in range(1, N_BUCKETS + 1):
                totamt[i] += brhamt[i]
                totacc[i] += noacc[i]

        # LAST.CAT -> grand total block (7 lines)
        if lines_on_page + 7 > PAGE_SIZE:
            pagecnt += 1
            output_lines.extend(_build_header_npl(progid, type_label, pagecnt))
            lines_on_page = HEADER_LINES

        total_lines = _build_total_npl(totamt, totacc)
        if cat != cats_present[-1]:
            total_lines = total_lines[:-1]   # drop trailing PUT; blank line
        output_lines.extend(total_lines)
        pagecnt = 0   # PAGECNT = 0; (reset after LAST.CAT)

    return output_lines

# ============================================================================
# STEP 7: RUN A) ALL NPL-QUALIFYING LOANS
# DATA PRNDATA; SET LOAN1; PROGID='EIMAR103-A';
# ============================================================================
print("\nStep 7: Generating EIMAR103-A report (all NPL loans)...")
PROGID_A = "EIMAR103-A"
lines_a = _generate_report(loan1, PROGID_A)
print(f"  EIMAR103-A lines: {len(lines_a):,}")

# ============================================================================
# STEP 8: RUN B) EXCLUDE BORR. STAT F/I/R, RESTRICT TO &HPD / (15,20,71,72)
# DATA PRNDATA; SET LOAN1;
#   IF TYPE IN ('(HPD-C)','(-HPD-)') THEN DELETE;
#   IF (BORSTAT NE 'F' AND BORSTAT NE 'I' AND BORSTAT NE 'R') AND
#      (PRODUCT IN &HPD OR PRODUCT IN (15,20,71,72));
#   PROGID = 'EIMAR103-B';
# ============================================================================
print("\nStep 8: Generating EIMAR103-B report (exclude BORSTAT F/I/R)...")
PROGID_B = "EIMAR103-B"

_part_b_products = set(HPD_PRODUCTS) | set(EXTRA_B_PRODUCTS)

part_b_df = loan1.filter(
    (~pl.col("TYPE").is_in(["(HPD-C)", "(-HPD-)"]))
    & (~pl.col("BORSTAT").is_in(["F", "I", "R"]))
    & (pl.col("PRODUCT").is_in(list(_part_b_products)))
)
lines_b = _generate_report(part_b_df, PROGID_B)
print(f"  EIMAR103-B lines: {len(lines_b):,}")

# ============================================================================
# STEP 9: LOCATE EIMAR102's CCDTXT2 OUTPUT  (predecessor to append onto)
# JCL: //CCDTXT2 DD DSN=SAP.PBB.CCDTXT2,DISP=MOD  -- same physical dataset
# EIMAR102 wrote earlier the same run. Resolved by latest dated file rather
# than reconstructing the filename from this program's own reptdate, since
# each program's date derivation is independent in this environment.
# ============================================================================
print("\nStep 9: Locating EIMAR102's CCDTXT2 output to append onto...")

eimar102_file = get_latest_file(EIMAR102_OUTPUT_DIR, prefix="CCDTXT2_")
print(f"  Found predecessor file: {eimar102_file.name}")

with open(eimar102_file, "r", encoding="latin1") as fh:
    eimar102_lines = [ln.rstrip("\n") for ln in fh]

print(f"  EIMAR102 lines carried forward: {len(eimar102_lines):,}")

# ============================================================================
# STEP 10: COMBINE AND WRITE OUTPUT  (append EIMAR103 content onto EIMAR102)
# ============================================================================
print("\nStep 10: Writing combined CCDTXT2 output...")

all_lines = eimar102_lines + lines_a + lines_b

# Remove any trailing blank lines
while all_lines and all_lines[-1].strip() == "":
    all_lines.pop()

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in all_lines:
        fh.write(f"{ln:<{LRECL_CCDTXT2}}\n")

print(f"  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(all_lines):,} "
      f"(EIMAR102: {len(eimar102_lines):,} + EIMAR103-A: {len(lines_a):,} + EIMAR103-B: {len(lines_b):,})")

# ============================================================================
# STEP 11: RESULTS SUMMARY  (printed to terminal)
# ============================================================================
# print("\n--- Combined CCDTXT2 (first 20 lines) ---")
# for ln in all_lines[:20]:
#     print(ln)

# print("\n--- EIMAR103 appended section (first 20 lines) ---")
# for ln in (lines_a + lines_b)[:20]:
#     print(ln)

# ============================================================================
# STEP 12: CLEANUP
# PROC DATASETS LIB=WORK NOLIST; DELETE LOANTEMP LOAN1 PRNDATA; RUN;
# ============================================================================
del loan1, part_b_df, lines_a, lines_b, eimar102_lines, all_lines
gc.collect()

print("\nEIMAR103 complete.")
