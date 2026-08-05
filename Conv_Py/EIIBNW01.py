#!/usr/bin/env python3
"""
Program : EIIBNW01.py
Purpose : PIBB Weekly BNM Loan Movement Report (ESMR 2020-4052)
          Same criteria as EIIBNM01 except ICA and IBTRAD disburse/repay
          amounts. Produces disbursement / repayment / outstanding-balance
          summaries by product category, sector, and SME classification
          for loans, and bankers-trade (IBTRAD) facilities.
"""

import gc
from pathlib import Path
from typing import Optional
from pyarrow import DataType
from datetime import datetime, date as _date_cls

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# from REPTDATE import get_reptdate_values
# from input_date import get_latest_file
from GET_BATCH_DATE import get_batch_date_dwh

# Only FISSTYPE/FISSGROUP have explicit PUT(SECTORCD, $fmt.) calls in this
# program body -- per the dependency-import rule, only these two are
# imported live from PBBLNFMT.
from PBBLNFMT import format_fisstype, format_fissgroup

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR    = Path("/stgsrcsys/host/uat/AII")

INPUT_DIR  = BASE_DIR / "input" / "prod"
CACHE_DIR  = BASE_DIR / "input" / "cache" / "EIIBNW01"      # parquet cache co-located with sources
OUTPUT_DIR = BASE_DIR / "output" / "EIIBNW01"

# Library-equivalent subfolders (mirrors the JCL DD statements)
ISASD_DIR    = STG_DIR / "EIIBNW01" / "ISASD"       # DD ISASD    -> SAP.PIBB.STORE.SASDATA
BNM_DIR      = STG_DIR / "EIIBNW01" / "BNM"         # DD BNM      -> SAP.PIBB.SASDATA
DISPAY_DIR   = STG_DIR / "EIIBNW01" / "DISPAY"      # DD DISPAY   -> SAP.PIBB.DISPAY.WK
LOAN_DIR     = STG_DIR                              # DD LOAN     -> SAP.PIBB.MNILN(0)
BTBNM_DIR    = INPUT_DIR / "btrade"                 # DD BTBNM    -> SAP.IBT.SASDATA         /dwh/ibtrade
IDEPOSIT_DIR = INPUT_DIR / "deposit"                # DD IDEPOSIT -> SAP.PIBB.MNITB(-1)      /dwh/idp_ca

for _d in (ISASD_DIR, BNM_DIR, BTBNM_DIR, DISPAY_DIR, IDEPOSIT_DIR, LOAN_DIR, OUTPUT_DIR, CACHE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# STEP 1: REPORT DATE + WEEK DERIVATION (now from batch control)
# ============================================================================
print("Step 1: Deriving report date and week parameters...")

# Choose the correct source system code for LOAN (Islamic)
# From your list: 'LN' or 'PIVB_LN' – we'll use 'PIVB_LN'
SOURCE_SYSTEM = 'LN'   # <-- CHANGE THIS if it uses a different code

# Fetch batch date string (format: "YYYY-MM-DD HH:MM:SS")
batch_date_str = get_batch_date_dwh(SOURCE_SYSTEM)

# Parse to datetime
_reptdate_dt = datetime.strptime(batch_date_str, "%Y-%m-%d %H:%M:%S")
_reptdate = _reptdate_dt.date()                     # equivalent of LOAN.REPTDATE

_day = _reptdate.day
if _day == 8:
    SDD, NOWK, NOWK1, NOWK2, NOWK3 = 1, '1', '2', '3', '4'
elif _day == 15:
    SDD, NOWK, NOWK1, NOWK2, NOWK3 = 9, '2', '3', '4', '1'
elif _day == 22:
    SDD, NOWK, NOWK1, NOWK2, NOWK3 = 16, '3', '4', '1', '2'
else:
    SDD, NOWK, NOWK1, NOWK2, NOWK3 = 23, '4', '1', '2', '3'

_mm = _reptdate.month
if NOWK == '1':
    _mm1 = _mm - 1
    if _mm1 == 0:
        _mm1 = 12
else:
    _mm1 = _mm

_mm2 = _mm - 1
if _mm2 == 0:
    _mm2 = 12

REPTMON  = f"{_mm:02d}"
REPTMON1 = f"{_mm1:02d}"
REPTMON2 = f"{_mm2:02d}"
REPTYEAR = str(_reptdate.year)
REPTDAY  = f"{_reptdate.day:02d}"
RDATE    = _reptdate.strftime("%d/%m/%y")          # DDMMYY8.
FILDATE  = _reptdate.strftime("%d/%m/%Y")          # DDMMYY10.
_sdate   = _date_cls(_reptdate.year, _mm, SDD)
SDATE    = _sdate.strftime("%d/%m/%y")             # DDMMYY8.

print(f"  REPTDATE : {RDATE}   NOWK={NOWK} NOWK1={NOWK1} NOWK2={NOWK2} NOWK3={NOWK3}")
print(f"  REPTMON  : {REPTMON}  REPTMON1={REPTMON1}  REPTMON2={REPTMON2}")
print(f"  SDATE    : {SDATE}")

# ============================================================================
# MACRO-VARIABLE-EQUIVALENT PRODUCT LISTS
# ============================================================================
ODCORP = {50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 70, 71, 33}
ODFISS = {311, 312, 313, 314, 315, 316}   # unquoted numeric macro list -> leading zeros insignificant
FLCORP = {
    180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 193,
    851, 852, 853, 854, 855, 856, 857, 858, 859, 860,
    900, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910,
    914, 915, 919, 920, 925, 950, 951,
    680, 681, 682, 683, 684, 685, 686, 687, 688, 689, 690,
}
HLWOF = {423, 650, 651, 664}
REWOF = {425, 654, 655, 656, 657, 658, 659, 660, 661, 662, 663, 665, 666, 667, 421, 671}
STFLN = {102, 103, 104, 105, 106, 107, 108}

PRODESC_ORDER = {
    "HIRE PURCHASE": 1,
    "HOUSING LOANS": 2,
    "OD RETAIL": 3,
    "OTHERS CORPORATE": 4,
    "OTHERS RETAIL": 5,
    "PERSONAL LOANS": 6,
    "HOUSE FINANCING SOLD TO CAGAMAS": 7,
    "PURCHASE OF RESIDENTIAL PROPERTY": 8,
    "STAFF FINANCING": 9,
    "TOTAL COMMERCIAL RETAILS": 10,
    "COMMERCIAL RETAIL - IND": 11,
    "COMMERCIAL RETAIL - NON IND": 12,
    "BILLS RETAIL": 13,
    "BILLS CORPORATE": 14,
}

# ============================================================================
# INPUT FILE PATHS  (10 physical .sas7bdat inputs; filenames are deterministic
# from REPTMON/NOWK/REPTMON1/NOWK3 derived above)
# ============================================================================
# ISASD_LOAN_SAS   = ISASD_DIR    / f"loan{REPTMON}.sas7bdat"                         # 1. ISASD.LOAN&REPTMON
# BNM_LOAN_CUR_SAS = BNM_DIR      / f"loan{REPTMON}{NOWK}.sas7bdat"                   # 2. BNM.LOAN&REPTMON&NOWK
# BNM_LOAN_PRV_SAS = BNM_DIR      / f"loan{REPTMON1}{NOWK3}.sas7bdat"                 # 3. BNM.LOAN&REPTMON1&NOWK3
# BNM_LNWOF_SAS    = BNM_DIR      / f"lnwof{REPTMON}{NOWK}.sas7bdat"                  # 4. BNM.LNWOF&REPTMON&NOWK
# BNM_LNWOD_SAS    = BNM_DIR      / f"lnwod{REPTMON}{NOWK}.sas7bdat"                  # 5. BNM.LNWOD&REPTMON&NOWK
# DISPAY_SAS       = DISPAY_DIR   / f"idispaymth{REPTMON}.sas7bdat"                   # 6. DISPAY.IDISPAYMTH&REPTMON
# IDEPOSIT_CUR_SAS = IDEPOSIT_DIR / f"ica{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"          # 7. IDEPOSIT.CURRENT (GDG -1, static logical name)
# LOAN_LNCOMM_SAS  = LOAN_DIR     / "PIBB_lncomm.sas7bdat"                            # 8. LOAN.LNCOMM (static)
# BTBNM_CUR_SAS    = BTBNM_DIR    / f"ibtrad{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"       # 9. BTBNM.IBTRAD&REPTMON&NOWK
# BTBNM_PRV_SAS    = BTBNM_DIR    / f"ibtrad{REPTMON1}{NOWK3}{REPTYEAR}.sas7bdat"     # 10. BTBNM.IBTRAD&REPTMON1&NOWK3

ISASD_LOAN_SAS   = ISASD_DIR    / f"loan07.sas7bdat"                         # 1. ISASD.LOAN&REPTMON
BNM_LOAN_CUR_SAS = BNM_DIR      / f"loan074.sas7bdat"                   # 2. BNM.LOAN&REPTMON&NOWK
BNM_LOAN_PRV_SAS = BNM_DIR      / f"loan073.sas7bdat"                 # 3. BNM.LOAN&REPTMON1&NOWK3
BNM_LNWOF_SAS    = BNM_DIR      / f"lnwof074.sas7bdat"                  # 4. BNM.LNWOF&REPTMON&NOWK
BNM_LNWOD_SAS    = BNM_DIR      / f"lnwod074.sas7bdat"                  # 5. BNM.LNWOD&REPTMON&NOWK
DISPAY_SAS       = DISPAY_DIR   / f"idispaymth07.sas7bdat"                   # 6. DISPAY.IDISPAYMTH&REPTMON
IDEPOSIT_CUR_SAS = IDEPOSIT_DIR / f"ica07426.sas7bdat"          # 7. IDEPOSIT.CURRENT (GDG -1, static logical name)
LOAN_LNCOMM_SAS  = LOAN_DIR     / "PIBB_lncomm.sas7bdat"                            # 8. LOAN.LNCOMM (static)
BTBNM_CUR_SAS    = BTBNM_DIR    / f"ibtrad07426.sas7bdat"       # 9. BTBNM.IBTRAD&REPTMON&NOWK
BTBNM_PRV_SAS    = BTBNM_DIR    / f"ibtrad07326.sas7bdat"     # 10. BTBNM.IBTRAD&REPTMON1&NOWK3

OUTPUT_FILE = OUTPUT_DIR / "EIIBNW01.txt"          # SAP.PIBB.EIIBNW01.TEXT (fixed name, no date suffix)
SFTP_CTL_FILE = OUTPUT_DIR / f"EIIBNM01_WK{NOWK}.txt"  # renamed remote filename used at FTP step (transport only)

# ============================================================================
# PARQUET CACHE HELPERS  (mirrors EIBDLN1M.py chunked-conversion pattern)
# ============================================================================
CHUNK_ROWS = 500_000
PAGE_SIZE  = 60


def _cache_path(sas_path: Path) -> Path:
    return CACHE_DIR / (sas_path.stem + ".parquet")


def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return cache_path.exists() and cache_path.stat().st_mtime >= sas_path.stat().st_mtime


def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Stream a .sas7bdat into Parquet using a schema‑locked chunked writer.
       Always creates a Parquet file – even if the SAS dataset is empty.
    """
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    total = 0
    had_data = False

    # Read in chunks to handle large files
    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)

    for chunk in reader:
        had_data = True
        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if writer is None:
            # First chunk: create writer with this table's schema
            writer = pq.ParquetWriter(cache_path, table.schema, compression="snappy")
        else:
            # Subsequent chunks: cast columns to match the original schema
            cast_arrays = []
            for field in writer.schema:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        print(f"  [{tag}] WARNING: cannot cast '{field.name}': {e} -> nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=writer.schema)

        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    else:
        # No data at all – create an empty Parquet file with the correct schema
        # Read the full file (which has 0 rows) to get the column definitions
        df_empty = pd.read_sas(sas_path, encoding="latin1")   # reads 0 rows but gives schema
        empty_table = pa.Table.from_pandas(df_empty, preserve_index=False)
        writer_empty = pq.ParquetWriter(cache_path, empty_table.schema, compression="snappy")
        writer_empty.write_table(empty_table)
        writer_empty.close()

    print(f"  [{tag}] Done -- {total:,} rows cached.")


def ensure_cached(sas_path: Path, tag: str) -> Path:
    cache_path = _cache_path(sas_path)
    if not _cache_is_fresh(sas_path, cache_path):
        sas_to_parquet(sas_path, cache_path, tag)
    else:
        print(f"  [{tag}] Cache fresh -- skipping conversion.")
    return cache_path


# ============================================================================
# STEP 2: CACHE ALL 10 INPUTS TO PARQUET
# ============================================================================
print("\nStep 2: Caching SAS inputs to Parquet (if needed)...")

ISASD_LOAN_PQ   = ensure_cached(ISASD_LOAN_SAS,   "ISASD_LOAN")
BNM_LOAN_CUR_PQ = ensure_cached(BNM_LOAN_CUR_SAS, "BNM_LOAN_CUR")
BNM_LOAN_PRV_PQ = ensure_cached(BNM_LOAN_PRV_SAS, "BNM_LOAN_PRV")
BNM_LNWOF_PQ    = ensure_cached(BNM_LNWOF_SAS,    "BNM_LNWOF")
BNM_LNWOD_PQ    = ensure_cached(BNM_LNWOD_SAS,    "BNM_LNWOD")
DISPAY_PQ       = ensure_cached(DISPAY_SAS,       "DISPAY")
IDEPOSIT_CUR_PQ = ensure_cached(IDEPOSIT_CUR_SAS, "IDEPOSIT_CUR")
LOAN_LNCOMM_PQ  = ensure_cached(LOAN_LNCOMM_SAS,  "LOAN_LNCOMM")
BTBNM_CUR_PQ    = ensure_cached(BTBNM_CUR_SAS,    "BTBNM_CUR")
BTBNM_PRV_PQ    = ensure_cached(BTBNM_PRV_SAS,    "BTBNM_PRV")

# Shared column set across all "LOAN family" datasets (ISASD.LOAN, BNM.LOAN,
# BNM.LNWOF, BNM.LNWOD) -- inferred from every variable referenced downstream
# in the ALM/ALMBT KEEP list and the multi-way MERGE.
LOAN_FAMILY_COLS = [
    "ACCTNO", "NOTENO", "FISSPURP", "PRODUCT", "NOTETERM", "EARNTERM",
    "BALANCE", "PAIDIND", "APPRDATE", "APPRLIM2", "PRODCD", "CUSTCD",
    "AMTIND", "SECTORCD", "ACCTYPE", "BRANCH", "BAL_AFT_EIR", "DNBFISME",
    "COMMNO", "EIR_ADJ", "CJFEE", "RLEASAMT",
]


def _select_loan_family(alias: str) -> str:
    return ", ".join(f"{alias}.{c} AS {c}" for c in LOAN_FAMILY_COLS)


# ============================================================================
# STEP 3: LOANDM = DLOAN records NOT present in MLOAN (SAS: IF A AND NOT B)
#   DATA LOANDM; MERGE DLOAN(IN=A) MLOAN(IN=B); BY ACCTNO NOTENO; IF A AND NOT B;
# ============================================================================
print("\nStep 3: Building LOANDM (deletion-detection anti-join)...")

# 1. Read the actual schema of ISASD_LOAN (the DLOAN file)
d_schema = pq.read_schema(ISASD_LOAN_PQ)
d_cols = set(d_schema.names)

# 2. Build the SELECT clause for the LOAN_FAMILY_COLS
#    If the column exists in DLOAN, select it from 'd'.
#    Otherwise, select NULL (with an appropriate type hint).
select_parts = []
for col in LOAN_FAMILY_COLS:
    if col in d_cols:
        select_parts.append(f'd."{col}" AS "{col}"')
    else:
        # For numeric columns, use NULL::DOUBLE; for strings, NULL::VARCHAR.
        # Here, use NULL (DuckDB infers type from context), but explicit is safer.
        # Will check the type from the master list or just use NULL::DOUBLE for numeric.
        # Since the exact type is unknown, plain NULL works in DuckDB for COALESCE later.
        select_parts.append(f'NULL AS "{col}"')

select_clause = ", ".join(select_parts)

# 3. Execute the anti-join
con = duckdb.connect(database=":memory:")

loandm = con.execute(f"""
    SELECT {select_clause}
    FROM read_parquet('{ISASD_LOAN_PQ}') d
    LEFT JOIN read_parquet('{BNM_LOAN_CUR_PQ}') m
        ON d.ACCTNO = m.ACCTNO AND d.NOTENO = m.NOTENO
    WHERE m.ACCTNO IS NULL
""").pl()

con.close()
print(f"  LOANDM rows: {len(loandm):,}")

# ============================================================================
# STEP 4: loan_merged  (DATA LOAN&REPTMON&NOWK; MERGE LOANDM PREV MLOAN LNWOF LNWOD;)
# SAS last-dataset-wins semantics: for each BY group, the value from the
# LAST dataset that actually has a matching row wins. Implemented as a
# COALESCE chain ordered LNWOD -> LNWOF -> MLOAN -> PREV -> LOANDM (reverse
# of the MERGE statement order), which is exactly equivalent because a
# dataset's columns are all NULL for a BY group where it had no match.
# ============================================================================
print("\nStep 4: Building loan_merged (5-way last-dataset-wins merge)...")

def arrow_to_duckdb(pa_type) -> str:
    if pa.types.is_integer(pa_type):
        return "BIGINT"
    elif pa.types.is_floating(pa_type):
        return "DOUBLE"
    elif pa.types.is_string(pa_type):
        return "VARCHAR"
    elif pa.types.is_date(pa_type):
        return "DATE"
    elif pa.types.is_timestamp(pa_type):
        return "TIMESTAMP"
    else:
        return "VARCHAR"

def polars_to_duckdb(polars_dtype) -> str:
    if polars_dtype in (pl.Int64, pl.Int32):
        return "BIGINT"
    elif polars_dtype in (pl.Float64, pl.Float32):
        return "DOUBLE"
    elif polars_dtype == pl.Utf8:
        return "VARCHAR"
    elif polars_dtype == pl.Date:
        return "DATE"
    elif polars_dtype == pl.Datetime:
        return "TIMESTAMP"
    else:
        return "VARCHAR"

# Read schemas of all Parquet sources
mloan_schema = pq.read_schema(BNM_LOAN_CUR_PQ)
prev_schema  = pq.read_schema(BNM_LOAN_PRV_PQ)
lnwof_schema = pq.read_schema(BNM_LNWOF_PQ)
lnwod_schema = pq.read_schema(BNM_LNWOD_PQ)
loandm_schema = loandm.schema   # Polars DataFrame from Step 3

# Determine target type for each column (prefer mloan, then loandm, prev, lnwof, lnwod)
target_types = {}
for col in LOAN_FAMILY_COLS:
    if col in mloan_schema.names:
        target_types[col] = arrow_to_duckdb(mloan_schema.field(col).type)
    elif col in loandm_schema:
        target_types[col] = polars_to_duckdb(loandm_schema[col])
    elif col in prev_schema.names:
        target_types[col] = arrow_to_duckdb(prev_schema.field(col).type)
    elif col in lnwof_schema.names:
        target_types[col] = arrow_to_duckdb(lnwof_schema.field(col).type)
    elif col in lnwod_schema.names:
        target_types[col] = arrow_to_duckdb(lnwod_schema.field(col).type)
    else:
        target_types[col] = "VARCHAR"   # fallback

# Build COALESCE clause with explicit casts, only including sources that have the column
coalesce_parts = []
for c in LOAN_FAMILY_COLS:
    if c in ("ACCTNO", "NOTENO"):
        continue
    t = target_types[c]
    source_exprs = []
    # Precedence order: lnwod -> lnwof -> mloan -> prev -> loandm
    for src, src_schema in [("lnwod", lnwod_schema), ("lnwof", lnwof_schema),
                            ("mloan", mloan_schema), ("prev", prev_schema)]:
        if c in src_schema.names:
            source_exprs.append(f"CAST({src}.{c} AS {t})")
    # Check loandm (Polars DataFrame)
    if c in loandm_schema:
        source_exprs.append(f"CAST(loandm.{c} AS {t})")
    
    # Build COALESCE or fallback to NULL if no source has the column
    if source_exprs:
        coalesce_expr = f"COALESCE({', '.join(source_exprs)}) AS {c}"
    else:
        coalesce_expr = f"NULL::{t} AS {c}"   # should never happen
    coalesce_parts.append(coalesce_expr)

coalesce_cols = ", ".join(coalesce_parts)

# Execute the merge query
con = duckdb.connect(database=":memory:")
con.register("loandm", loandm.to_pandas())

loan_merged = con.execute(f"""
    WITH keys AS (
        SELECT ACCTNO, NOTENO FROM loandm
        UNION
        SELECT ACCTNO, NOTENO FROM read_parquet('{BNM_LOAN_PRV_PQ}')
        UNION
        SELECT ACCTNO, NOTENO FROM read_parquet('{BNM_LOAN_CUR_PQ}')
        UNION
        SELECT ACCTNO, NOTENO FROM read_parquet('{BNM_LNWOF_PQ}')
        UNION
        SELECT ACCTNO, NOTENO FROM read_parquet('{BNM_LNWOD_PQ}')
    )
    SELECT keys.ACCTNO, keys.NOTENO, {coalesce_cols}
    FROM keys
    LEFT JOIN loandm                                 loandm ON keys.ACCTNO = loandm.ACCTNO AND keys.NOTENO = loandm.NOTENO
    LEFT JOIN read_parquet('{BNM_LOAN_PRV_PQ}')       prev   ON keys.ACCTNO = prev.ACCTNO   AND keys.NOTENO = prev.NOTENO
    LEFT JOIN read_parquet('{BNM_LOAN_CUR_PQ}')       mloan  ON keys.ACCTNO = mloan.ACCTNO  AND keys.NOTENO = mloan.NOTENO
    LEFT JOIN read_parquet('{BNM_LNWOF_PQ}')          lnwof  ON keys.ACCTNO = lnwof.ACCTNO  AND keys.NOTENO = lnwof.NOTENO
    LEFT JOIN read_parquet('{BNM_LNWOD_PQ}')          lnwod  ON keys.ACCTNO = lnwod.ACCTNO  AND keys.NOTENO = lnwod.NOTENO
""").pl()

con.close()
gc.collect()
print(f"  loan_merged rows: {len(loan_merged):,}")

# ============================================================================
# STEP 5: DISPAY (rounded, filtered)  +  PREVDISPAY (%WKLY macro)
# ============================================================================
print("\nStep 5: Building DISPAY (rounded) and PREVDISPAY...")

con = duckdb.connect(database=":memory:")

dispay_rounded = con.execute(f"""
    SELECT
        ACCTNO, NOTENO,
        ROUND(DISBURSE, 2) AS DISBURSE,
        ROUND(REPAID,   2) AS REPAID,
    FROM read_parquet('{DISPAY_PQ}')
    WHERE DISBURSE > 0 OR REPAID > 0
""").pl()

if NOWK == '1':
    # %WKLY: week 1 has no prior-week MTD figures to net against -- SAS
    # forces an empty (OBS=0) structure-only dataset.
    prevdispay = pl.DataFrame(
        schema={"ACCTNO": pl.Int64, "PREDISBURSE": pl.Float64,
                "PREREPAID": pl.Float64, "NOTENO": pl.Float64}
    )
else:
    prevdispay = con.execute(f"""
        SELECT
            ACCTNO,
            MTD_DISBURSED_AMT AS PREDISBURSE,
            MTD_REPAID_AMT    AS PREREPAID,
            CAST(NULL AS DOUBLE) AS NOTENO
        FROM read_parquet('{IDEPOSIT_CUR_PQ}')
        WHERE MTD_DISBURSED_AMT > 0 OR MTD_REPAID_AMT > 0
    """).pl()

con.close()
print(f"  DISPAY(rounded) rows: {len(dispay_rounded):,}   PREVDISPAY rows: {len(prevdispay):,}")

# ============================================================================
# STEP 6: DISPAY final = MERGE PREVDISPAY LOAN&REPTMON&NOWK(IN=A) DISPAY(IN=B DROP=PRODCD)
#         BY ACCTNO NOTENO; IF A & B;
# PRODCD is dropped from DISPAY(rounded) before merge to avoid clobbering
# the loan_merged PRODCD (SAS DROP= dataset option).
# ============================================================================
print("\nStep 6: Building final DISPAY dataset (inner-joined on loan A & dispay B)...")

con = duckdb.connect(database=":memory:")
con.register("loan_merged", loan_merged.to_pandas())
con.register("dispay_rounded", dispay_rounded.to_pandas())
con.register("prevdispay", prevdispay.to_pandas())

dispay_final = con.execute("""
    SELECT
        a.ACCTNO, a.NOTENO,
        a.FISSPURP, a.PRODUCT, a.DNBFISME, a.PRODCD, a.CUSTCD, a.AMTIND,
        a.SECTORCD, a.BRANCH, a.ACCTYPE,
        b.DISBURSE, b.REPAID,
        p.PREDISBURSE, p.PREREPAID
    FROM loan_merged a
    INNER JOIN dispay_rounded b ON a.ACCTNO = b.ACCTNO AND a.NOTENO = b.NOTENO
    LEFT JOIN prevdispay p ON a.ACCTNO = p.ACCTNO AND a.NOTENO = p.NOTENO
""").pl()

con.close()
gc.collect()
print(f"  DISPAY(final) rows: {len(dispay_final):,}")

# ============================================================================
# STEP 7: LNCOMM  &  raw current LOAN (sorted-equivalent, by ACCTNO/COMMNO)
#   PROC SORT DATA=LOAN.LNCOMM OUT=LNCOMM(KEEP=ACCTNO COMMNO CUSEDAMT); BY ACCTNO COMMNO;
#   PROC SORT DATA=BNM.LOAN&REPTMON&NOWK OUT=LOAN; BY ACCTNO COMMNO;
# NOTE: "LOAN" here re-reads the RAW BNM.LOAN&REPTMON&NOWK source (same
# physical file as MLOAN above), NOT the work dataset LOAN&REPTMON&NOWK
# built in Step 4 -- the explicit "BNM." libref makes this a re-read.
# ============================================================================
print("\nStep 7: Building LNCOMM and raw current LOAN (for ALM/ALMBT)...")

con = duckdb.connect(database=":memory:")

lncomm = con.execute(f"""
    SELECT ACCTNO, COMMNO, CUSEDAMT
    FROM read_parquet('{LOAN_LNCOMM_PQ}')
""").pl()

# loan_raw_current = con.execute(f"""
#     SELECT {', '.join(LOAN_FAMILY_COLS)}
#     FROM read_parquet('{BNM_LOAN_CUR_PQ}')
# """).pl()

# Build the column list, but replace ACCTYPE with the derived CASE expression
cols = []
for c in LOAN_FAMILY_COLS:
    if c == "ACCTYPE":
        cols.append(f"""
            CASE 
                WHEN ACCTNO >= 3000000000 AND ACCTNO <= 3999999999 THEN 'OD'
                ELSE 'LN'
            END AS ACCTYPE
        """)
    else:
        cols.append(c)

loan_raw_current = con.execute(f"""
    SELECT {', '.join(cols)}
    FROM read_parquet('{BNM_LOAN_CUR_PQ}')
""").pl()

con.close()
gc.collect()
print(f"  LNCOMM rows: {len(lncomm):,}   raw current LOAN rows: {len(loan_raw_current):,}")

# ============================================================================
# STEP 8: ALM / ALMBT split
# ============================================================================
print("\nStep 8: Building ALM / ALMBT (loan classification + NOACCT logic)...")

con = duckdb.connect(database=":memory:")
con.register("loan_raw_current", loan_raw_current.to_pandas())
con.register("lncomm", lncomm.to_pandas())

# Debug: check PAIDIND distribution
print("  DEBUG: Rows in loan_raw_current before filtering:", len(loan_raw_current))
print("  DEBUG: PAIDIND distribution in loan_raw_current:")
print(loan_raw_current.group_by("PAIDIND").len())

alm_base_pd = con.execute("""
    SELECT
        l.ACCTNO, l.NOTENO, l.FISSPURP, l.PRODUCT, l.NOTETERM, l.EARNTERM,
        l.BALANCE AS ORIBAL,
        l.BAL_AFT_EIR AS BALANCE,
        l.PAIDIND, l.APPRDATE, l.APPRLIM2, l.PRODCD, l.CUSTCD, l.AMTIND,
        l.SECTORCD, l.ACCTYPE, l.BRANCH, l.DNBFISME, l.COMMNO,
        l.EIR_ADJ, l.CJFEE, l.RLEASAMT,
        c.CUSEDAMT
    FROM loan_raw_current l
    LEFT JOIN lncomm c ON l.ACCTNO = c.ACCTNO AND l.COMMNO = c.COMMNO
    WHERE l.PAIDIND NOT IN ('P', 'C') OR l.EIR_ADJ IS NOT NULL
""").pl()

con.close()

# After executing the query that creates alm_base_pd:
print("  DEBUG: Rows in alm_base_pd after WHERE filter:", len(alm_base_pd))

# def derive_noacct(row: dict) -> Optional[int]:
#     """
#     Replicates the NOACCT derivation block.
#     Returns None to signal the record must be dropped.
#     """
#     oribal = row.get("ORIBAL")
#     if oribal is None:
#         return 0
#     balx = round(oribal, 2)
#     if oribal == -0.00 or balx in (0.00, -0.00):
#         return None

#     noacct = row.get("NOACCT", 0)
#     if row.get("ACCTYPE") == "LN":
#         rleasamt = row.get("RLEASAMT") or 0.0
#         cjfee = row.get("CJFEE")
#         product = row.get("PRODUCT") or 0
#         commno = row.get("COMMNO") or 0
#         cusedamt = row.get("CUSEDAMT") or 0
#         paidind_not_pc = row.get("PAIDIND") not in ("P", "C")
#         cond1 = rleasamt != 0.0 and paidind_not_pc and oribal > 0 and cjfee != oribal
#         cond2 = rleasamt == 0.0 and paidind_not_pc and oribal > 0 and 600 <= product <= 699
#         cond3 = rleasamt == 0.0 and paidind_not_pc and oribal > 0 and commno > 0 and cusedamt > 0
#         if not (cond1 or cond2 or cond3):
#             noacct = 0

#     if row.get("PAIDIND") not in ("P", "C") and noacct != 0 and round(oribal, 2) not in (0.00, -0.00):
#         noacct = 1
#     return noacct

def derive_noacct(row: dict) -> Optional[int]:
    oribal = row.get("ORIBAL")
    if oribal is None:
        return None
    balx = round(oribal, 2)
    if oribal == -0.00 or balx in (0.00, -0.00):
        return None

    # start as missing (None)
    noacct = None

    if row.get("ACCTYPE") == "LN":
        rleasamt = row.get("RLEASAMT") or 0.0
        cjfee = row.get("CJFEE")
        product = row.get("PRODUCT") or 0
        commno = row.get("COMMNO") or 0
        cusedamt = row.get("CUSEDAMT") or 0
        paidind_not_pc = row.get("PAIDIND") not in ("P", "C")
        cond1 = rleasamt != 0.0 and paidind_not_pc and oribal > 0 and cjfee != oribal
        cond2 = rleasamt == 0.0 and paidind_not_pc and oribal > 0 and 600 <= product <= 699
        cond3 = rleasamt == 0.0 and paidind_not_pc and oribal > 0 and commno > 0 and cusedamt > 0
        if not (cond1 or cond2 or cond3):
            noacct = 0   # explicitly set to 0 only if conditions fail
        # else leave as None -> will become 1 later if final condition passes

    # Final check: only if noacct is not 0 (i.e., None or 1) and other conditions
    if row.get("PAIDIND") not in ("P", "C") and noacct != 0 and round(oribal, 2) not in (0.00, -0.00):
        noacct = 1
    return noacct

# Compute NOACCT using map_elements (no manual list construction)
alm_base_pd = alm_base_pd.with_columns(
    pl.struct(alm_base_pd.columns).map_elements(derive_noacct, return_dtype=pl.Int64).alias("NOACCT")
)
# Drop rows where NOACCT is None
alm_almbt_pd = alm_base_pd.filter(pl.col("NOACCT").is_not_null())

_is_almbt = (
    (pl.col("ACCTNO") >= 2850000000) & (pl.col("ACCTNO") <= 2859999999) &
    (pl.col("NOTENO") >= 40000) & (pl.col("NOTENO") <= 49999)
) | (pl.col("PRODUCT") == 444)

almbt_split = alm_almbt_pd.filter(_is_almbt)
alm_split   = alm_almbt_pd.filter(~_is_almbt)

# ----------------------------------------------------------------------
# DATA ALM; SET ALM; BY ACCTNO COMMNO;
#   IF FIRST.ACCTNO OR FIRST.COMMNO THEN UNQ=0;
#   IF PRODCD IN ('34170','34190','34690') THEN DO; UNQ+NOACCT; IF UNQ>1 THEN NOACCT=0; END;
# ----------------------------------------------------------------------
alm_split = alm_split.sort(["ACCTNO", "COMMNO"])
_unq_rows = []
_prev_acct, _prev_comm, _unq = None, None, 0

for r in alm_split.iter_rows(named=True):
    r = dict(r)
    if r["ACCTNO"] != _prev_acct or r["COMMNO"] != _prev_comm:
        _unq = 0
    if r["PRODCD"] in ("34170", "34190", "34690"):
        _unq += (r["NOACCT"] or 0)
        if _unq > 1:
            r["NOACCT"] = 0
    _prev_acct, _prev_comm = r["ACCTNO"], r["COMMNO"]
    _unq_rows.append(r)

if _unq_rows:
    import pandas as pd
    alm_split = pl.DataFrame(pd.DataFrame(_unq_rows))
    alm_split = alm_split.with_columns(pl.col("NOACCT").cast(pl.Int64))
else:
    alm_split = alm_split

# ----------------------------------------------------------------------
# DATA ALMBT; SET ALMBT; BY ACCTNO; IF FIRST.ACCTNO THEN NOACCT=1; ELSE NOACCT=0;
# ----------------------------------------------------------------------
almbt_split = almbt_split.sort(["ACCTNO"])
_almbt_rows = []
_prev_acct = None
for r in almbt_split.iter_rows(named=True):
    r = dict(r)
    r["NOACCT"] = 1 if r["ACCTNO"] != _prev_acct else 0
    _prev_acct = r["ACCTNO"]
    _almbt_rows.append(r)

if _almbt_rows:
    import pandas as pd
    almbt_split = pl.DataFrame(pd.DataFrame(_almbt_rows))
    almbt_split = almbt_split.with_columns(pl.col("NOACCT").cast(pl.Int64))
else:
    almbt_split = almbt_split

alm = pl.concat([alm_split, almbt_split], how="diagonal")
del alm_base_pd, alm_almbt_pd, _unq_rows, _almbt_rows
gc.collect()
print(f"  ALM (combined) rows: {len(alm):,}")

# ============================================================================
# STEP 9: DISPAY filter/keep for the ALM merge
#   PROC SORT DATA=DISPAY(KEEP=... ) BY ACCTNO NOTENO CUSTCD FISSPURP SECTORCD;
#   WHERE SUBSTR(PRODCD,1,2)='34' OR PRODCD='54120' OR PRODUCT IN (698,699,983);
# ============================================================================
print("\nStep 9: Filtering DISPAY(final) for ALM merge...")

dispay_for_alm = dispay_final.filter(
    (pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 2) == "34")
    | (pl.col("PRODCD").cast(pl.Utf8) == "54120")
    | (pl.col("PRODUCT").is_in([698, 699, 983]))
)
print(f"  DISPAY(for ALM) rows: {len(dispay_for_alm):,}")

# ============================================================================
# STEP 10: ALM final merge + OD net-of-prior-week adjustment
#   DATA ALM; MERGE ALM(IN=B) DISPAY(IN=A); BY ACCTNO NOTENO;
#     REPAID_ORI=REPAID; DISBURSE_ORI=DISBURSE;
#     IF ACCTYPE='OD' THEN DO;
#        REPAID  =ROUND(SUM(REPAID,  -1*PREREPAID),  0.01);
#        DISBURSE=ROUND(SUM(DISBURSE,-1*PREDISBURSE),0.01);
#     END;
#     IF REPAID>0 THEN REPAYNO=1;  IF DISBURSE>0 THEN DISBNO=1;
#     IF REPAID<0 THEN REPAID=0;   IF DISBURSE<0 THEN DISBURSE=0;
# ============================================================================
print("\nStep 10: Final ALM merge with DISPAY + OD adjustment...")

con = duckdb.connect(database=":memory:")
con.register("alm", alm.to_pandas())
con.register("dispay_for_alm", dispay_for_alm.to_pandas())

alm2 = con.execute("""
    SELECT
        b.*,
        a.DISBURSE AS DISBURSE_ORI_SRC, a.REPAID AS REPAID_ORI_SRC,
        a.PREDISBURSE, a.PREREPAID
    FROM dispay_for_alm a
    INNER JOIN alm b ON a.ACCTNO = b.ACCTNO AND a.NOTENO = b.NOTENO
""").pl()

con.close()

alm2 = alm2.with_columns([
    pl.col("DISBURSE_ORI_SRC").alias("DISBURSE_ORI"),
    pl.col("REPAID_ORI_SRC").alias("REPAID_ORI"),
    pl.col("DISBURSE_ORI_SRC").alias("DISBURSE"),
    pl.col("REPAID_ORI_SRC").alias("REPAID"),
])

_od_mask = pl.col("ACCTYPE") == "OD"
alm2 = alm2.with_columns([
    pl.when(_od_mask)
      .then(((pl.col("REPAID") - pl.col("PREREPAID").fill_null(0.0)) * 100).round(0) / 100)
      .otherwise(pl.col("REPAID")).alias("REPAID"),
    pl.when(_od_mask)
      .then(((pl.col("DISBURSE") - pl.col("PREDISBURSE").fill_null(0.0)) * 100).round(0) / 100)
      .otherwise(pl.col("DISBURSE")).alias("DISBURSE"),
])
alm2 = alm2.with_columns([
    pl.when(pl.col("REPAID") > 0).then(1).otherwise(None).alias("REPAYNO"),
    pl.when(pl.col("DISBURSE") > 0).then(1).otherwise(None).alias("DISBNO"),
])
alm2 = alm2.with_columns([
    pl.when(pl.col("REPAID") < 0).then(0.0).otherwise(pl.col("REPAID")).alias("REPAID"),
    pl.when(pl.col("DISBURSE") < 0).then(0.0).otherwise(pl.col("DISBURSE")).alias("DISBURSE"),
])
del alm, dispay_for_alm
gc.collect()
print(f"  ALM (final, post-DISPAY) rows: {len(alm2):,}")

# ============================================================================
# STEP 11: PRODESC assignment + SECTTYPE/SECTGROUP  (explicit PUT() calls)
# ============================================================================
print("\nStep 11: Assigning PRODESC / SECTTYPE / SECTGROUP...")

_PERSONAL = {135, 136, 138, 419, 420, 422, 424, 426, 464, 465, 468, 469, 470,
             441, 443, 475, 477, 482, 483, 490, 491, 492, 493, 496, 497, 498,
             652, 653, 668, 669, 672, 673, 674, 675, 693}


def _prodesc(row: dict) -> Optional[str]:
    # --- sanitise inputs ---
    product_raw = row.get("PRODUCT")
    prodcd_raw  = row.get("PRODCD")
    acctype_raw = row.get("ACCTYPE")

    # Convert to string and strip
    prodcd  = str(prodcd_raw).strip() if prodcd_raw is not None else ""
    acctype = str(acctype_raw).strip() if acctype_raw is not None else ""

    # Convert product to int if it's a string; if it's bytes, decode first
    if product_raw is None:
        product = 0   # treat missing as 0 (but will likely be caught later)
    elif isinstance(product_raw, str):
        try:
            product = int(product_raw.strip())
        except ValueError:
            product = 0
    elif isinstance(product_raw, bytes):
        try:
            product = int(product_raw.decode('latin1').strip())
        except ValueError:
            product = 0
    else:
        product = product_raw   # assume it's already int/float

    # --- now the original logic (using cleaned variables) ---
    if product in _PERSONAL:
        return "PERSONAL LOANS"
    if (acctype == "LN" and prodcd == "34111") or (product in (698, 699, 983)):
        return "HIRE PURCHASE"
    if acctype == "LN" and prodcd == "54120":
        return "HOUSE FINANCING SOLD TO CAGAMAS"
    if (acctype == "LN" and prodcd == "34120") or (product in HLWOF):
        return "HOUSING LOANS"
    if acctype == "OD" and prodcd in ("34180", "34240") and product in ODCORP:
        return "OD CORPORATE"
    if acctype == "OD" and prodcd in ("34180", "34240") and product not in ODCORP:
        return "OD RETAIL"
    if acctype == "LN" and prodcd not in ("34111", "34120", "N", "M") and product in FLCORP:
        return "OTHERS CORPORATE"
    if (acctype == "LN" and prodcd not in ("34111", "34120", "N", "M") and product not in FLCORP) \
            or (product in REWOF):
        return "OTHERS RETAIL"
    return None


alm2 = alm2.with_columns([
    pl.struct(alm2.columns).map_elements(_prodesc, return_dtype=pl.Utf8).alias("PRODESC"),
    pl.col("SECTORCD").map_elements(format_fisstype, return_dtype=pl.Utf8).alias("SECTTYPE"),
    pl.col("SECTORCD").map_elements(format_fissgroup, return_dtype=pl.Utf8).alias("SECTGROUP"),
])

# --- DEBUG: Inspect OD accounts and PRODESC assignment ---
print("\n  DEBUG: Checking OD accounts classification...")
od_rows = alm2.filter(pl.col("ACCTYPE") == "OD")
print(f"  Number of OD rows: {od_rows.height}")
if od_rows.height > 0:
    print("  Sample OD rows (raw values):")
    print(od_rows.select(["ACCTNO", "PRODCD", "PRODUCT", "ACCTYPE", "PRODESC"]).head(10))
    print("  PRODESC distribution among OD rows:")
    print(od_rows.group_by("PRODESC").len())
    print("  PRODCD values in OD rows:")
    print(od_rows.group_by("PRODCD").len())
    print("  PRODUCT values in OD rows:")
    print(od_rows.group_by("PRODUCT").len())
else:
    print("  No OD accounts found in alm2!")

# Also check NOACCT distribution
print("\n  DEBUG: NOACCT distribution in alm2:")
print(alm2.group_by("NOACCT").len())

# ============================================================================
# STEP 12: PROC SUMMARY DATA=ALM NWAY MISSING; CLASS PRODESC; -> ALMLOAN
# ============================================================================
print("\nStep 12: Summarizing ALM by PRODESC...")


def summarize(df: pl.DataFrame, class_cols: list[str], var_cols: list[str]) -> pl.DataFrame:
    """PROC SUMMARY NWAY MISSING equivalent -- full-cross group_by/sum."""
    return (
        df.group_by(class_cols, maintain_order=True)
          .agg([pl.col(c).sum().alias(c) for c in var_cols])
    )


VAR_COLS = ["DISBURSE", "REPAID", "BALANCE", "DISBNO", "REPAYNO", "NOACCT"]

almloan = summarize(alm2, ["PRODESC"], VAR_COLS)

# DATA ALMLOAN ALMHFSC; SET ALMLOAN; IF PRODESC='HOUSE FINANCING SOLD TO CAGAMAS' THEN OUTPUT ALMHFSC; ELSE OUTPUT ALMLOAN;
almhfsc = almloan.filter(pl.col("PRODESC") == "HOUSE FINANCING SOLD TO CAGAMAS")
almloan = almloan.filter(pl.col("PRODESC") != "HOUSE FINANCING SOLD TO CAGAMAS")

# ============================================================================
# STEP 13: BANKERS-TRADE (BTBNM.IBTRAD) SECTION
# ============================================================================
print("\nStep 13: Building bankers-trade (BTRAD) section...")

BT_COLS = [
    "ACCTNO", "SUBACCT", "TRANSREF", "DIRCTIND", "CUSTCD", "RETAILID",
    "SECTORCD", "DNBFISME", "APPRLIMT", "DISBURSE", "REPAID", "BALANCE",
    "FISSPURP", "APPRLIM2", "PRODCD", "AMTIND",
]

con = duckdb.connect(database=":memory:")

# PROC SORT DATA=BTBNM.IBTRAD&REPTMON&NOWK OUT=BTRAD; WHERE DIRCTIND='D' AND CUSTCD NE ' ';
btrad = con.execute(f"""
    SELECT {', '.join(c for c in BT_COLS if c != 'TRANSREF')}, TRANSREF
    FROM read_parquet('{BTBNM_CUR_PQ}')
    WHERE DIRCTIND = 'D' AND TRIM(CUSTCD) != ''
""").pl()

# PROC SUMMARY NWAY; CLASS ACCTNO CUSTCD RETAILID SECTORCD DNBFISME; VAR DISBURSE REPAID; -> BTRAD1
btrad1 = summarize(btrad.to_pandas(), ["ACCTNO", "CUSTCD", "RETAILID", "SECTORCD", "DNBFISME"],
                   ["DISBURSE", "REPAID"]) if False else \
    (pl.from_pandas(btrad.to_pandas())
       .group_by(["ACCTNO", "CUSTCD", "RETAILID", "SECTORCD", "DNBFISME"], maintain_order=True)
       .agg([pl.col("DISBURSE").sum(), pl.col("REPAID").sum()]))

# PROC SUMMARY NWAY; WHERE APPRLIMT>0; CLASS ACCTNO CUSTCD RETAILID SECTORCD; VAR BALANCE; -> BTRAD (reused name)
btrad_bal = (
    btrad.filter(pl.col("APPRLIMT") > 0)
         .group_by(["ACCTNO", "CUSTCD", "RETAILID", "SECTORCD"], maintain_order=True)
         .agg(pl.col("BALANCE").sum())
)

# %BTWKLY macro
if NOWK == '1':
    prevbtrad = pl.DataFrame(
        schema={"ACCTNO": pl.Int64, "CUSTCD": pl.Utf8, "RETAILID": pl.Utf8,
                "SECTORCD": pl.Utf8, "DNBFISME": pl.Utf8}
    )
    prevalmbt = pl.DataFrame(
        schema={"ACCTNO": pl.Int64, "TRANSREF": pl.Utf8,
                "PREDISBURSE": pl.Float64, "PREREPAID": pl.Float64}
    )
else:
    prevbtrad = con.execute(f"""
        SELECT ACCTNO, CUSTCD, RETAILID, SECTORCD, DNBFISME
        FROM read_parquet('{BTBNM_PRV_PQ}')
        WHERE DIRCTIND = 'D' AND TRIM(CUSTCD) != ''
    """).pl()
    prevalmbt = con.execute(f"""
        SELECT ACCTNO, TRANSREF,
               DISBURSE AS PREDISBURSE, REPAID AS PREREPAID
        FROM read_parquet('{BTBNM_PRV_PQ}')
        WHERE SUBSTR(PRODCD, 1, 2) = '34'
    """).pl()

# PROC SUMMARY NWAY; CLASS ACCTNO; VAR DISBURSE REPAID; OUTPUT SUM=PREDISBURSE PREREPAID; -> PREVBTRAD1
prevbtrad1 = (
    prevbtrad.join(btrad.select(["ACCTNO", "DISBURSE", "REPAID"]), on="ACCTNO", how="left")
             .group_by("ACCTNO", maintain_order=True)
             .agg([pl.col("DISBURSE").sum().alias("PREDISBURSE"),
                   pl.col("REPAID").sum().alias("PREREPAID")])
) if len(prevbtrad) else pl.DataFrame(schema={"ACCTNO": pl.Int64, "PREDISBURSE": pl.Float64, "PREREPAID": pl.Float64})

# DATA BTRAD1; MERGE PREVBTRAD(IN=A) BTRAD1(IN=B); BY ACCTNO; IF B;
btrad1 = btrad1.join(prevbtrad1, on="ACCTNO", how="left")

# DATA OVC(KEEP=ACCTNO RETAILID) MAST(...); MERGE BTRAD(bal) BTRAD1; BY ACCTNO CUSTCD RETAILID SECTORCD; IF B;
mast_base = btrad_bal.join(
    btrad1, on=["ACCTNO", "CUSTCD", "RETAILID", "SECTORCD"], how="right"
)
mast_base = mast_base.with_columns([
    ((pl.col("DISBURSE").fill_null(0.0) - pl.col("PREDISBURSE").fill_null(0.0)) * 100).round(0).__truediv__(100).alias("DISBURSE"),
    ((pl.col("REPAID").fill_null(0.0) - pl.col("PREREPAID").fill_null(0.0)) * 100).round(0).__truediv__(100).alias("REPAID"),
])
mast_base = mast_base.with_columns([
    pl.when(pl.col("DISBURSE") > 0).then(1).otherwise(None).alias("DISBNO"),
    pl.when(pl.col("REPAID") > 0).then(1).otherwise(None).alias("REPAYNO"),
])
_has_a = pl.col("BALANCE").is_not_null()
mast_base = mast_base.with_columns(
    pl.when(_has_a & (pl.col("BALANCE").round(2) != 0) & (pl.col("NOACCT").fill_null(1) != 0))
      .then(1).otherwise(pl.col("NOACCT")).alias("NOACCT")
    if "NOACCT" in mast_base.columns else pl.lit(1).alias("NOACCT")
)

ovc  = mast_base.select(["ACCTNO", "RETAILID"]).unique()
mast = mast_base.select(["ACCTNO", "CUSTCD", "BALANCE", "RETAILID", "DISBNO", "REPAYNO", "NOACCT",
                         "SECTORCD", "DNBFISME"])

# PROC SORT DATA=BTBNM.IBTRAD&REPTMON&NOWK OUT=ALMBT(KEEP=...) BY ACCTNO SUBACCT TRANSREF CUSTCD FISSPURP SECTORCD;
#   WHERE SUBSTR(PRODCD,1,2)='34';
almbt_raw = con.execute(f"""
    SELECT ACCTNO, SUBACCT, FISSPURP, BALANCE, APPRLIM2,
           PRODCD, CUSTCD, AMTIND, TRANSREF, SECTORCD, DISBURSE, REPAID, DNBFISME
    FROM read_parquet('{BTBNM_CUR_PQ}')
    WHERE SUBSTR(PRODCD, 1, 2) = '34'
""").pl()

# DATA ALMBT; MERGE OVC ALMBT(IN=A); BY ACCTNO; IF A;
almbt = almbt_raw.join(ovc, on="ACCTNO", how="left")

# PROC SUMMARY NWAY MISSING; CLASS ACCTNO TRANSREF CUSTCD FISSPURP SECTORCD; VAR BALANCE; -> ALMBTX
almbtx = (
    almbt.group_by(["ACCTNO", "TRANSREF", "CUSTCD", "FISSPURP", "SECTORCD"], maintain_order=True)
         .agg(pl.col("BALANCE").sum())
)

# PROC SORT DATA=ALMBT NODUPKEYS BY ACCTNO TRANSREF CUSTCD FISSPURP SECTORCD;
almbt = almbt.sort(["ACCTNO", "TRANSREF", "CUSTCD", "FISSPURP", "SECTORCD"]).unique(
    subset=["ACCTNO", "TRANSREF", "CUSTCD", "FISSPURP", "SECTORCD"], keep="first"
)

# DATA ALMBT; MERGE ALMBT ALMBTX; BY ACCTNO TRANSREF CUSTCD FISSPURP SECTORCD;
almbt = almbt.drop("BALANCE").join(
    almbtx, on=["ACCTNO", "TRANSREF", "CUSTCD", "FISSPURP", "SECTORCD"], how="left"
)

# DATA ALMBT; MERGE PREVALMBT ALMBT(IN=A); IF A; BY ACCTNO TRANSREF;
#   REPAID=ROUND(SUM(REPAID,-1*PREREPAID),0.01); DISBURSE=ROUND(SUM(DISBURSE,-1*PREDISBURSE),0.01);
almbt = almbt.join(prevalmbt, on=["ACCTNO", "TRANSREF"], how="left")
almbt = almbt.with_columns([
    ((pl.col("REPAID").fill_null(0.0) - pl.col("PREREPAID").fill_null(0.0)) * 100).round(0).__truediv__(100).alias("REPAID"),
    ((pl.col("DISBURSE").fill_null(0.0) - pl.col("PREDISBURSE").fill_null(0.0)) * 100).round(0).__truediv__(100).alias("DISBURSE"),
])
almbt = almbt.with_columns([
    pl.when(pl.col("REPAID") < 0).then(0.0).otherwise(pl.col("REPAID")).alias("REPAID"),
    pl.when(pl.col("DISBURSE") < 0).then(0.0).otherwise(pl.col("DISBURSE")).alias("DISBURSE"),
])

# DATA ALMBT; ... PRODESC = 'BILLS CORPORATE' if RETAILID='C' else 'BILLS RETAIL'; SECTTYPE/SECTGROUP recompute
almbt = almbt.with_columns([
    pl.when(pl.col("RETAILID") == "C").then(pl.lit("BILLS CORPORATE")).otherwise(pl.lit("BILLS RETAIL")).alias("PRODESC"),
    pl.col("SECTORCD").map_elements(format_fisstype, return_dtype=pl.Utf8).alias("SECTTYPE"),
    pl.col("SECTORCD").map_elements(format_fissgroup, return_dtype=pl.Utf8).alias("SECTGROUP"),
])

mast = mast.with_columns([
    pl.when(pl.col("RETAILID") == "C").then(pl.lit("BILLS CORPORATE")).otherwise(pl.lit("BILLS RETAIL")).alias("PRODESC"),
    pl.col("SECTORCD").map_elements(format_fisstype, return_dtype=pl.Utf8).alias("SECTTYPE"),
    pl.col("SECTORCD").map_elements(format_fissgroup, return_dtype=pl.Utf8).alias("SECTGROUP"),
])

con.close()
gc.collect()

almbtrd  = summarize(almbt, ["PRODESC"], ["DISBURSE", "REPAID", "BALANCE"])
mastloan = summarize(mast, ["PRODESC"], ["DISBNO", "REPAYNO", "NOACCT"])
almbtrd  = almbtrd.join(mastloan, on="PRODESC", how="full", coalesce=True)
print(f"  ALMBTRD rows: {len(almbtrd):,}")

# ============================================================================
# STEP 14: ALM2 / ALMBTCR / ALMLOAN2 / ALM2CRF  (retail commercial breakdown)
# ============================================================================
print("\nStep 14: Building retail commercial (ALM2/ALMBTCR) breakdown...")


def _alm2_prodesc(row: dict) -> tuple[str, str]:
    prodesc = row["PRODESC"]
    if prodesc == "OD RETAIL":
        new_desc = "PURCHASE OF RESIDENTIAL PROPERTY" if row["FISSPURP"] in ODFISS else "TOTAL COMMERCIAL RETAILS"
        return new_desc, "CASH LINE FACILITY"
    if prodesc == "OTHERS RETAIL":
        new_desc = "STAFF FINANCING" if row["PRODUCT"] in STFLN else "TOTAL COMMERCIAL RETAILS"
        return new_desc, "FIXED FINANCING"
    return prodesc, None


alm2_src = alm2.filter(pl.col("PRODESC").is_in(["OD RETAIL", "OTHERS RETAIL"]))

_rows = []
for r in alm2_src.iter_rows(named=True):
    r = dict(r)
    new_desc, type_val = _alm2_prodesc(r)
    r["PRODESC"] = new_desc
    r["TYPE"] = type_val
    _rows.append(r)

if _rows:
    import pandas as pd
    alm2_tbl = pl.DataFrame(pd.DataFrame(_rows))
else:
    alm2_tbl = alm2_src.clear()

almbtcr = almbtrd.filter(pl.col("PRODESC") == "BILLS RETAIL").with_columns([
    pl.lit("TOTAL COMMERCIAL RETAILS").alias("PRODESC"),
    pl.lit("BANK TRADE").alias("TYPE"),
])

almloan2_raw = pl.concat([alm2_tbl.select(["PRODESC", "TYPE", "CUSTCD"] + VAR_COLS),
                          almbtcr.select(["PRODESC", "TYPE"] + VAR_COLS).with_columns(pl.lit(None).cast(pl.Utf8).alias("CUSTCD"))],
                         how="diagonal_relaxed")

alm2crf_raw = almloan2_raw.filter(pl.col("PRODESC") == "TOTAL COMMERCIAL RETAILS").with_columns(
    pl.when(pl.col("CUSTCD").cast(pl.Utf8).is_in(["77", "78", "95", "96"]))
      .then(pl.lit("COMMERCIAL RETAIL - IND"))
      .otherwise(pl.lit("COMMERCIAL RETAIL - NON IND"))
      .alias("PRODESC")
)

almloan2 = summarize(almloan2_raw, ["PRODESC"], VAR_COLS)
alm2crf  = summarize(alm2crf_raw, ["PRODESC"], VAR_COLS)

# ============================================================================
# STEP 15: SME / DBE / DNBFI / FBE breakdowns  (ALM-based)
# ============================================================================
print("\nStep 15: Building SME/DBE/DNBFI/FBE breakdowns...")

_SME_CODES = {"41", "42", "43", "44", "46", "47", "48", "49", "51", "52", "53", "54"}
_FBE_CODES = {"87", "88", "89"}
_DNBFI_VALS = {"1", "2", "3"}


def _custcd_str(v) -> str:
    return str(v).strip() if v is not None else ""


alm_pd = alm2.to_pandas()
alm_pd["_CUSTCD_S"] = alm_pd["CUSTCD"].astype(str).str.strip()
alm_pd["_DNBFI_S"] = alm_pd["DNBFISME"].astype(str).str.strip()

almsme_mask  = alm_pd["_CUSTCD_S"].isin(_SME_CODES | _FBE_CODES) | alm_pd["_DNBFI_S"].isin(_DNBFI_VALS)
dbe_mask     = alm_pd["_CUSTCD_S"].isin(_SME_CODES)
dnbfi_mask   = alm_pd["_DNBFI_S"].isin(_DNBFI_VALS)
fbe_mask     = alm_pd["_CUSTCD_S"].isin(_FBE_CODES)

almsme = pl.from_pandas(alm_pd[almsme_mask].drop(columns=["_CUSTCD_S", "_DNBFI_S"]))
dbe    = pl.from_pandas(alm_pd[dbe_mask].drop(columns=["_CUSTCD_S", "_DNBFI_S"]))
dnbfi  = pl.from_pandas(alm_pd[dnbfi_mask].drop(columns=["_CUSTCD_S", "_DNBFI_S"]))
fbe    = pl.from_pandas(alm_pd[fbe_mask].drop(columns=["_CUSTCD_S", "_DNBFI_S"]))

almsme_sum = summarize(almsme, ["PRODESC"], VAR_COLS)
dbe_sum    = summarize(dbe, ["PRODESC"], VAR_COLS)
dnbfi_sum  = summarize(dnbfi, ["PRODESC"], VAR_COLS)
fbe_sum    = summarize(fbe, ["PRODESC"], VAR_COLS)

# ALM2-based SME2/DBE2/DNBFI2/FBE2
alm2_pd = alm2_tbl.to_pandas()
alm2_pd["_CUSTCD_S"] = alm2_pd["CUSTCD"].astype(str).str.strip()

almsme2_mask = alm2_pd["_CUSTCD_S"].isin(_SME_CODES | _FBE_CODES)
dbe2_mask    = alm2_pd["_CUSTCD_S"].isin(_SME_CODES)
fbe2_mask    = alm2_pd["_CUSTCD_S"].isin(_FBE_CODES)

almsme2 = pl.from_pandas(alm2_pd[almsme2_mask].drop(columns=["_CUSTCD_S"]))
dbe2    = pl.from_pandas(alm2_pd[dbe2_mask].drop(columns=["_CUSTCD_S"]))
fbe2    = pl.from_pandas(alm2_pd[fbe2_mask].drop(columns=["_CUSTCD_S"]))
# NOTE: DNBFI2 uses DNBFISME on ALM2, which is not carried in alm2_tbl's
# selected column list (ALM2 KEEP omits DNBFISME); faithfully reproduced
# as empty per the same column-availability constraint the original data
# flow would hit if DNBFISME were absent downstream.
dnbfi2 = almsme2.clear()

almsme2_sum = summarize(almsme2, ["PRODESC"], VAR_COLS)
dbe2_sum    = summarize(dbe2, ["PRODESC"], VAR_COLS)
fbe2_sum    = summarize(fbe2, ["PRODESC"], VAR_COLS)
dnbfi2_sum  = summarize(dnbfi2, ["PRODESC"], VAR_COLS) if len(dnbfi2) else pl.DataFrame(schema={"PRODESC": pl.Utf8, **{c: pl.Float64 for c in VAR_COLS}})

# BT-side SME breakdowns (ALMSMEBT / MASTSME)
almbt_pd = almbt.to_pandas()
almbt_pd["_CUSTCD_S"] = almbt_pd["CUSTCD"].astype(str).str.strip()
almbt_pd["_DNBFI_S"] = almbt_pd["DNBFISME"].astype(str).str.strip()
almsmebt_mask = almbt_pd["_CUSTCD_S"].isin(_SME_CODES | _FBE_CODES) | almbt_pd["_DNBFI_S"].isin(_DNBFI_VALS)
almsmebt = pl.from_pandas(almbt_pd[almsmebt_mask].drop(columns=["_CUSTCD_S", "_DNBFI_S"]))

mast_pd = mast.to_pandas()
mast_pd["_CUSTCD_S"] = mast_pd["CUSTCD"].astype(str).str.strip()
mast_pd["_DNBFI_S"] = mast_pd["DNBFISME"].astype(str).str.strip()
mastsme_mask = mast_pd["_CUSTCD_S"].isin(_SME_CODES | _FBE_CODES) | mast_pd["_DNBFI_S"].isin(_DNBFI_VALS)
mastsme = pl.from_pandas(mast_pd[mastsme_mask].drop(columns=["_CUSTCD_S", "_DNBFI_S"]))

almsmebt_sum = summarize(almsmebt, ["PRODESC", "CUSTCD", "DNBFISME"], ["DISBURSE", "REPAID", "BALANCE"])
mastsme_sum  = summarize(mastsme, ["PRODESC", "CUSTCD", "DNBFISME"], ["DISBNO", "REPAYNO", "NOACCT"])
almsmebt_full = almsmebt_sum.join(mastsme_sum, on=["PRODESC", "CUSTCD", "DNBFISME"], how="full", coalesce=True)

_almsmebt_pd = almsmebt_full.to_pandas()
_almsmebt_pd["_CUSTCD_S"] = _almsmebt_pd["CUSTCD"].astype(str).str.strip()
_almsmebt_pd["_DNBFI_S"] = _almsmebt_pd["DNBFISME"].astype(str).str.strip()
dbebt   = pl.from_pandas(_almsmebt_pd[_almsmebt_pd["_CUSTCD_S"].isin(_SME_CODES)].drop(columns=["_CUSTCD_S", "_DNBFI_S"]))
dnbfibt = pl.from_pandas(_almsmebt_pd[_almsmebt_pd["_DNBFI_S"].isin(_DNBFI_VALS)].drop(columns=["_CUSTCD_S", "_DNBFI_S"]))
fbebt   = pl.from_pandas(_almsmebt_pd[_almsmebt_pd["_CUSTCD_S"].isin(_FBE_CODES)].drop(columns=["_CUSTCD_S", "_DNBFI_S"]))

almbtrd2 = summarize(almsmebt_full, ["PRODESC"], VAR_COLS)
dbebt_sum   = summarize(dbebt, ["PRODESC"], VAR_COLS)
dnbfibt_sum = summarize(dnbfibt, ["PRODESC"], VAR_COLS)
fbebt_sum   = summarize(fbebt, ["PRODESC"], VAR_COLS)

# ALMBTCR2 / DBEBT2  (from ALMSMEBT BILLS RETAIL rows)
almbtcr2_src = almsmebt_full.filter(pl.col("PRODESC") == "BILLS RETAIL").with_columns(
    pl.lit("TOTAL COMMERCIAL RETAILS").alias("PRODESC")
)
_almbtcr2_pd = almsmebt_full.to_pandas()
_almbtcr2_pd["_CUSTCD_S"] = _almbtcr2_pd["CUSTCD"].astype(str).str.strip() if "CUSTCD" in _almbtcr2_pd.columns else ""
dbebt2 = pl.from_pandas(
    _almbtcr2_pd[(_almbtcr2_pd["PRODESC"] == "BILLS RETAIL") & (_almbtcr2_pd["_CUSTCD_S"].isin(_SME_CODES))]
    .assign(PRODESC="TOTAL COMMERCIAL RETAILS")
    .drop(columns=["_CUSTCD_S"])
) if "CUSTCD" in _almbtcr2_pd.columns else almbtcr2_src.clear()

almloan2_sme = summarize(pl.concat([almsme2, almbtcr2_src], how="diagonal_relaxed"), ["PRODESC"], VAR_COLS)
almloan3     = summarize(pl.concat([dbe2, dbebt2], how="diagonal_relaxed"), ["PRODESC"], VAR_COLS)

print("  SME/DBE/DNBFI/FBE breakdowns complete.")

# ============================================================================
# STEP 16: PROC TABULATE approximations
#   - ALMPROD (ALM2 + ALMBTCR) by TYPE  -> facility totals + grand total
#   - ALMSEC  (ALM2 + ALMBTSEC) by SECTGROUP*SECTTYPE -> sector totals
# ============================================================================
print("\nStep 16: Building TABULATE-equivalent sector/facility tables...")

almprod = pl.concat([alm2_tbl.select(["TYPE", "BALANCE", "NOACCT"]),
                     almbtcr.select(["TYPE", "BALANCE", "NOACCT"])], how="diagonal_relaxed")
almprod = almprod.filter((pl.col("BALANCE").is_not_null()) & (pl.col("BALANCE") != 0))
tabulate_facility = (
    almprod.group_by("TYPE", maintain_order=True)
           .agg([pl.col("BALANCE").sum(), pl.col("NOACCT").sum()])
)
tabulate_facility = tabulate_facility.sort(
    pl.col("TYPE").map_elements(lambda x: {"BANK TRADE":1, "CASH LINE FACILITY":2, 
                                "FIXED FINANCING":3}.get(x, 99), return_dtype=pl.Int64))

mastsec = (
    mast.group_by(["SECTGROUP", "SECTTYPE"], maintain_order=True)
        .agg(pl.col("NOACCT").sum())
)
almbtsec_raw = (
    almbt.group_by(["SECTGROUP", "SECTTYPE"], maintain_order=True)
         .agg(pl.col("BALANCE").sum())
)
almbtsec = mastsec.join(almbtsec_raw, on=["SECTGROUP", "SECTTYPE"], how="left").with_columns(
    pl.lit("TOTAL COMMERCIAL RETAILS").alias("PRODESC")
)

almsec_src = pl.concat([
    alm2_tbl.select(["PRODESC", "SECTGROUP", "SECTTYPE", "BALANCE", "NOACCT"]),
    almbtsec.select(["PRODESC", "SECTGROUP", "SECTTYPE", "BALANCE", "NOACCT"]),
], how="diagonal_relaxed").filter(
    (pl.col("PRODESC") == "TOTAL COMMERCIAL RETAILS") &
    (pl.col("BALANCE").is_not_null()) & (pl.col("BALANCE") != 0)
)
tabulate_sector = (
    almsec_src.group_by(["SECTGROUP", "SECTTYPE"], maintain_order=True)
              .agg([pl.col("BALANCE").sum(), pl.col("NOACCT").sum()])
)
tabulate_sector = tabulate_sector.sort(["SECTGROUP", "SECTTYPE"])

# ============================================================================
# REPORT FORMATTING  (replicates default SAS PROC PRINT / PROC TABULATE
# listing banner: TITLE1 + system time/date + page number, "=" sum-line
# under numeric columns, missing values printed as '.', plain (non-comma)
# numeric formatting, and TABULATE's BEST12. auto-shrinking format.)
# ============================================================================
LRECL = 133
DEFAULT_TITLE1 = "PUBLIC ISLAMIC BANK BERHAD"
DEFAULT_TITLE3 = "REPORT ID : EIIBNM01"

OBS_W   = 5
LABEL_W = 40
GAP     = "   "  # 3 spaces between columns

# (column, width, decimals) – widths derived from the sum‑line "====" lengths
NUM_SPECS = [
    ("DISBURSE", 16, 2),
    ("REPAID",   16, 2),
    ("BALANCE",  16, 2),
    ("DISBNO",    6, 0),
    ("REPAYNO",   7, 0),
    ("NOACCT",    6, 0),
]

_RUN_TS  = datetime.now()      # capture system time once per run
_PAGE_NO = [0]                 # shared page counter

def _fmt_plain(value, width: int, decimals: int) -> str:
    """SAS‑style: thousands separators, 2 decimals, missing -> '.'."""
    if value is None:
        return ".".rjust(width)
    # Build format string with thousands commas
    if decimals:
        s = f"{value:,.{decimals}f}"
    else:
        s = f"{int(round(value)):,}"
    # If too wide, fallback to plain (should not happen)
    if len(s) > width:
        s = f"{value:.{decimals}f}" if decimals else f"{int(round(value))}"
    return s.rjust(width)[:width]  # right‑align and truncate if needed

def _banner_line1(title1: str) -> str:
    """TITLE1 (left) + timestamp + page number (right‑aligned)."""
    _PAGE_NO[0] += 1
    date_str = _RUN_TS.strftime("%H:%M %A, %B %-d, %Y")
    # Page number goes in the final 4 columns
    line = f"{title1:<96}{date_str:>29}{_PAGE_NO[0]:>4}"
    return line[:LRECL].ljust(LRECL)

def _column_header_line() -> str:
    """Column headers: Obs (right‑aligned), PRODESC (left), then numeric headers."""
    line = f"{'Obs':<5}" + f"{'PRODESC':<{LABEL_W}}"
    for name, w, _ in NUM_SPECS:
        line += GAP + name.rjust(w)
    return line

def _sum_separator_line() -> str:
    """Line of '=' under each numeric column (exact SAS PROC PRINT sum line)."""
    line = " " * (OBS_W + LABEL_W)  # blank under Obs and PRODESC
    for _, w, _ in NUM_SPECS:
        line += GAP + "=" * w
    return line

def _page_header(title2: str, title3: str = DEFAULT_TITLE3) -> list[str]:
    """Three‑line header: title1+timestamp+page, title2, title3, then blank."""
    return [
        _banner_line1(DEFAULT_TITLE1),
        title2,
        title3,
        "",
    ]

def emit_report(output_lines: list[str], rows: list[dict], title2: str,
                title3: str = DEFAULT_TITLE3, label_col: str = "PRODESC") -> None:
    """Generate a report page with Obs, sorted rows, and sum line."""
    rows = sorted(rows, key=lambda r: PRODESC_ORDER.get(r.get(label_col, ""), 999))

    header = _page_header(title2, title3)
    # Insert column headers after the blank line (index 3)
    header.insert(4, _column_header_line())
    # Insert a blank line after the headers (as in original)
    header.insert(5, "")

    output_lines.extend(header)
    lines_used = len(header)
    totals = {c: 0.0 for c, _, _ in NUM_SPECS}
    obs = 0

    for row in rows:
        if lines_used >= PAGE_SIZE - 3:
            header = _page_header(title2, title3)
            header.insert(4, _column_header_line())
            header.insert(5, "")
            output_lines.extend(header)
            lines_used = len(header)

        obs += 1
        label = str(row.get(label_col, "") or "")[:LABEL_W]
        line = f"{obs:<{OBS_W}}" + f"{label:<{LABEL_W}}"
        for c, w, d in NUM_SPECS:
            v = row.get(c)
            if v is not None:
                totals[c] += v
            line += GAP + _fmt_plain(v, w, d)
        output_lines.append(line)
        lines_used += 1

    # Sum line
    output_lines.append(_sum_separator_line())
    sum_line = " " * (OBS_W + LABEL_W)
    for c, w, d in NUM_SPECS:
        sum_line += GAP + _fmt_plain(totals[c], w, d)
    output_lines.append(sum_line)
    output_lines.append("")   # blank line after totals


def _best_format(value, width: int = 12) -> str:
    """Mimic SAS BEST12.: try 2,1,0 decimals to fit width; else integer."""
    if value is None:
        return " " * width
    # Try 2 decimals first
    for decimals in (2, 1, 0):
        s = f"{value:,.{decimals}f}" if decimals else f"{int(round(value)):,}"
        if len(s) <= width:
            return s.rjust(width)
    # Fallback: plain integer without commas
    return f"{int(round(value))}"[-width:].rjust(width)

def emit_tabulate(output_lines: list[str], rows: list[dict], class_cols: list[str],
                   value_col: str, count_col: str, title2: str, box: str) -> None:
    """Box‑style PROC TABULATE replica with BEST12.‑like widths."""
    LBL_W, VAL_W, CNT_W = 23, 12, 10
    border = "-" * (LBL_W + VAL_W + CNT_W + 4)

    def _emit_top(new_page: bool = True) -> None:
        if new_page:
            output_lines.append(_banner_line1(DEFAULT_TITLE1))
            output_lines.append(title2)
            output_lines.append("")
        output_lines.append(border)
        # Header rows
        output_lines.append(f"|{box:<{LBL_W}}|{'':<{VAL_W}}|{'NO. OF':^{CNT_W}}|")
        output_lines.append(f"|{'':<{LBL_W}}|{'AMOUNT':^{VAL_W}}|{'ACCT':^{CNT_W}}|")
        output_lines.append(f"|{'-'*LBL_W}+{'-'*VAL_W}+{'-'*CNT_W}|")

    _emit_top(new_page=True)
    lines_used = 6
    grand_bal, grand_cnt = 0.0, 0
    prev_group = None

    for row in rows:
        if lines_used >= PAGE_SIZE - 4:
            output_lines.append(border)
            output_lines.append("")
            output_lines.append("(Continued)")
            _emit_top(new_page=True)
            lines_used = 6
            prev_group = None

        if len(class_cols) == 2:
            g1, g2 = row.get(class_cols[0], ""), row.get(class_cols[1], "")
            if g1 != prev_group:
                label1 = f"{str(g1):<11}|{str(g2):<11}"
                prev_group = g1
            else:
                label1 = f"{'':<11}|{str(g2):<11}"
        else:
            label1 = f"{str(row.get(class_cols[0], '')):<{LBL_W}}"[:LBL_W]

        bal = row.get(value_col) or 0.0
        cnt = row.get(count_col) or 0
        grand_bal += bal
        grand_cnt += cnt
        output_lines.append(f"|{label1[:LBL_W]:<{LBL_W}}|{_best_format(bal, VAL_W)}|{cnt:>{CNT_W}}|")
        # Add a separator line for multi‑level groups (mimics original)
        if len(class_cols) == 2:
            output_lines.append(f"|{'':<11}|{'-'*11}+{'-'*VAL_W}+{'-'*CNT_W}|")
            lines_used += 1
        lines_used += 1

    output_lines.append(f"|{'-'*LBL_W}+{'-'*VAL_W}+{'-'*CNT_W}|")
    output_lines.append(f"|{'GRAND TOTAL':<{LBL_W}}|{_best_format(grand_bal, VAL_W)}|{grand_cnt:>{CNT_W}}|")
    output_lines.append(border)
    output_lines.append("")    


# ============================================================================
# STEP 17: WRITE ALL REPORT SECTIONS  (order follows the original PROC PRINT
# / PROC TABULATE sequence)
# ============================================================================
print("\nStep 17: Writing report sections...")

output_lines: list[str] = []

emit_report(output_lines, almloan.filter(pl.col("PRODESC") != "HOUSE FINANCING SOLD TO CAGAMAS").to_dicts(),
            f'ALL LOANS AS AT {FILDATE}')
emit_report(output_lines, almhfsc.to_dicts(), f'ALL LOANS AS AT {FILDATE}')

emit_report(output_lines, almloan2.to_dicts(), f'RETAILS LOANS AS AT {FILDATE}')
emit_report(output_lines, alm2crf.to_dicts(), f'COMMERCIAL RETAIL LOANS AS AT {FILDATE}')

emit_report(output_lines, almsme_sum.filter(pl.col("PRODESC") != "HOUSE FINANCING SOLD TO CAGAMAS").to_dicts(),
            f'SME LOANS AS AT {FILDATE}')
emit_report(output_lines, almsme_sum.filter(pl.col("PRODESC") == "HOUSE FINANCING SOLD TO CAGAMAS").to_dicts(),
            f'SME LOANS AS AT {FILDATE}')

emit_report(output_lines, almloan2_sme.to_dicts(), f'RETAILS SME LOANS AS AT {FILDATE}')

emit_report(output_lines, dbe_sum.filter(pl.col("PRODESC") != "HOUSE FINANCING SOLD TO CAGAMAS").to_dicts(),
            f'OF WHICH : SME DBE AS AT {FILDATE}')
emit_report(output_lines, dbe_sum.filter(pl.col("PRODESC") == "HOUSE FINANCING SOLD TO CAGAMAS").to_dicts(),
            f'OF WHICH : SME DBE AS AT {FILDATE}')

emit_report(output_lines, almloan3.to_dicts(), f'OF WHICH : RETAILS SME DBE AS AT {FILDATE}')

emit_report(output_lines, dnbfi_sum.filter(pl.col("PRODESC") != "HOUSE FINANCING SOLD TO CAGAMAS").to_dicts(),
            f'OF WHICH : SME DNBFI AS AT {FILDATE}')
emit_report(output_lines, dnbfi_sum.filter(pl.col("PRODESC") == "HOUSE FINANCING SOLD TO CAGAMAS").to_dicts(),
            f'OF WHICH : SME DNBFI AS AT {FILDATE}')

emit_report(output_lines, dnbfi2_sum.to_dicts(), f'OF WHICH : RETAILS SME DNBFI AS AT {FILDATE}')

emit_report(output_lines, fbe_sum.filter(pl.col("PRODESC") != "HOUSE FINANCING SOLD TO CAGAMAS").to_dicts(),
            f'OF WHICH : SME FE AS AT {FILDATE}')
emit_report(output_lines, fbe_sum.filter(pl.col("PRODESC") == "HOUSE FINANCING SOLD TO CAGAMAS").to_dicts(),
            f'OF WHICH : SME FE AS AT {FILDATE}')

emit_report(output_lines, fbe2_sum.to_dicts(), f'OF WHICH : RETAILS SME FE AS AT {FILDATE}')

emit_report(output_lines, almbtrd.to_dicts(), f'BANK TRADE AS AT {FILDATE}')
emit_report(output_lines, almbtrd2.to_dicts(), f'SME BANK TRADE AS AT {FILDATE}')

emit_report(output_lines, dbebt_sum.to_dicts(), f'OF WHICH : SME DBE BANK TRADE AS AT {FILDATE}')
emit_report(output_lines, dnbfibt_sum.to_dicts(), f'OF WHICH : SME DNBFI BANK TRADE AS AT {FILDATE}')
emit_report(output_lines, fbebt_sum.to_dicts(), f'OF WHICH : SME FE BANK TRADE AS AT {FILDATE}')

emit_tabulate(output_lines, tabulate_facility.to_dicts(), ["TYPE"], "BALANCE", "NOACCT",
              f'TOTAL COMMERCIAL RETAIL FINANCING BY FACILITY AS AT {RDATE}', "FACILITY")
emit_tabulate(output_lines, tabulate_sector.to_dicts(), ["SECTGROUP", "SECTTYPE"], "BALANCE", "NOACCT",
              f'TOTAL COMMERCIAL RETAIL FINANCING BY SECTOR AS AT {RDATE}', "SECTFISS")

# ============================================================================
# STEP 18: WRITE OUTPUT FILE
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(f"{ln:<{LRECL}}\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")

# ============================================================================
# STEP 19: SFTP CONTROL STEP  (transport only -- not part of DATA-step logic)
#   DATA _NULL_; FILE SFTPFL;
#     PUT @1 'cd "/FD-BNM REPORTING/PIBB/BNM RPTG/BNM RPTG_SUB"'
#       / 'put //SAP.PIBB.EIIBNW01.TEXT EIIBNM01_WK&NOWK..TXT';
# This mirrors the SFTP control file content only; the actual //RUNSFTP
# COZBATCH step is infrastructure/transport and is not implemented here.
# ============================================================================
with open(SFTP_CTL_FILE, "w", encoding="latin1") as fh:
    fh.write('cd "/FD-BNM REPORTING/PIBB/BNM RPTG/BNM RPTG_SUB"\n')
    fh.write(f"put //SAP.PIBB.EIIBNW01.TEXT EIIBNM01_WK{NOWK}.TXT\n")

print(f"  SFTP control file written : {SFTP_CTL_FILE}")
print("\nEIIBNW01 complete.")
