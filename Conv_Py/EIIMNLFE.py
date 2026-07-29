#!/usr/bin/env python3
"""
Program : EIIMNLFE.py
Purpose : PIBB National Loans/Deposit Behavioural Trend Report
          Computes weekly/monthly/quarterly/half-yearly/yearly percentage
          movement bands per deposit product (RM only: Fixed/Savings/
          Current/CASA), re-derives BNM product coding (including ISTIMA
          code normalisation into RM Fixed Deposit codes), and produces
          the BEHAVIORAL TABLE / HIGH-LOW / HIGHLOW(monetary) reports.
"""

import os
import gc
import duckdb
import polars as pl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import date, timedelta

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
# from output_date import build_output_file  # not used - output filename has no date component

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# STORE1 DD -> SAP.PIBB.NLF.DAILY ("i" prefix per PIBB naming convention)
# INPUT_NOTE_DIR      = BASE_DIR / "input" / "prod" / "EIBMNLFE"          # note

STG_DIR             = Path("/stgsrcsys/host/uat/AII/EIIMNLFE")
INPUT_NOTE_DIR      = STG_DIR  / "STORE1"           # note

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIMNLFE"

# Persistent library replicating SAS permanent libraries (BASE/STORE/FINAL).
LIB_ROOT   = BASE_DIR / "input" / "lib" / "EIIMNLFE"
BASE_LIB   = LIB_ROOT / "base"     # BASE.<PROD> - true daily/milestone accumulator
STORE_LIB  = LIB_ROOT / "store"    # STORE.* - snapshot of this run's working sets
FINAL_LIB  = LIB_ROOT / "final"    # FINAL.* - snapshot of this run's report sets

OUTPUT_DIR = BASE_DIR / "output" / "EIIMNLFE"

for _d in (INPUT_NOTE_DIR, CACHE_DIR, BASE_LIB, STORE_LIB, FINAL_LIB, OUTPUT_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# Output filename carries no date component (GDG "+1" generation) - fixed name.
OUTPUT_FILE = OUTPUT_DIR / "NLFBEHAVE_EIIMNLFE.txt"

# ============================================================================
# REPORT PAGE CONFIGURATION
# ============================================================================
PAGE_SIZE    = 60   # lines per page (default, not otherwise specified)
ROW_LIMIT    = int(os.environ.get("ROW_LIMIT", 0))
CHUNK_ROWS   = 500_000

# ============================================================================
# PRODUCT CONSTANTS (PASS 1 - raw NOTE codes, excluding ISTIMA)
# ============================================================================
# Raw BNM code (as it appears in NOTE before normalisation) -> DESC.
# Two codes (9531508/9531708 and 9531509/9531709) map to the same DESC,
# mirroring the original SAS "IF PROD = 'xxxx' THEN DESC = ..." cascade.
PASS1_BNMCODE_TO_DESC = {
    "9531508": "INDRMFD",
    "9531509": "NONRMFD",
    "9531208": "INDRMSA",
    "9531209": "NONRMSA",
    "9531308": "INDRMDD",
    "9531309": "NONRMDD",
    "9631108": "INDFXFD",
    "9631109": "NONFXFD",
    "9631308": "INDFXCA",
    "9631309": "NONFXCA",
    "9531708": "INDRMFD",
    "9531709": "NONRMFD",
}

# ITEM assignment for STORE.DEPRMP2 / STORE.DEPFXP2 (keyed by raw PROD).
# Includes the ISTIMA codes (9532908/9532909) exactly as in the SAS source;
# these branches are UNREACHABLE in this program because the pass-1 NOTE
# filter excludes ISTIMA codes (`WHERE ... NOT IN ('9532908','9532909')`),
# but are kept for fidelity with the original IF cascade.
ITEM_MAP_PASS1 = {
    "9531508": ("INDRMFD", "A1.15", "- FIXED  "),
    "9531708": ("INDRMFD", "A1.15", "- FIXED  "),
    "9531509": ("NONRMFD", "A1.12", "- FIXED  "),
    "9531709": ("NONRMFD", "A1.12", "- FIXED  "),
    "9532908": ("INDRMFD", "A1.20", None),   # unreachable - ISTIMA excluded upstream
    "9532909": ("NONRMFD", "A1.20", None),   # unreachable - ISTIMA excluded upstream
    "9531208": ("INDRMSA", "A1.16", "- SAVINGS"),
    "9531209": ("NONRMSA", "A1.13", "- SAVINGS"),
    "9531308": ("INDRMDD", "A1.17", "- CURRENT"),
    "9531309": ("NONRMDD", "A1.14", "- CURRENT"),
    "9631108": ("INDFXFD", "B1.15", "- FIXED  "),
    "9631109": ("NONFXFD", "B1.12", "- FIXED  "),
    "9631308": ("INDFXCA", "B1.17", "- CURRENT"),
    "9631309": ("NONFXCA", "B1.14", "- CURRENT"),
}
# NOTE: original SAS also carries a fully-commented alternate mapping for
# 9531708/9531709 -> DESC='INDRMCM'/'NONRMCM', ITEM='A1.17A'/'A1.14A'. That
# block was commented out in the source and is intentionally NOT applied:
#
#   IF PROD = '9531708' THEN DO;
#      DESC = 'INDRMCM'; ITEM = 'A1.17A'; ITEM4 = '- FIXED';
#   END;
#   IF PROD = '9531709' THEN DO;
#      DESC = 'NONRMCM'; ITEM = 'A1.14A'; ITEM4 = '- FIXED';
#   END;

# ============================================================================
# PRODUCT CONSTANTS (PASS 2 - normalised codes -> BASE.DEPOSIT)
# ============================================================================
# BNM code normalisation applied to the SECOND NOTE read:
#   '95317xx...' -> '95315' + same trailing digits (SECONDBNM)
#   explicit ISTIMA -> RM Fixed Deposit code rewrites (12 exact codes)
ISTIMA_TO_RMFD = {
    "9532908010000Y": "9531508010000Y",
    "9532908020000Y": "9531508020000Y",
    "9532908030000Y": "9531508030000Y",
    "9532908040000Y": "9531508040000Y",
    "9532908050000Y": "9531508050000Y",
    "9532908060000Y": "9531508060000Y",
    "9532909010000Y": "9531509010000Y",
    "9532909020000Y": "9531509020000Y",
    "9532909030000Y": "9531509030000Y",
    "9532909040000Y": "9531509040000Y",
    "9532909050000Y": "9531509050000Y",
    "9532909060000Y": "9531509060000Y",
}

# Normalised BNM code (post pass-2 rewrite) -> DESC (straightforward map,
# no combined codes since '95317xx' has already been folded into '95315xx').
PASS2_BNMCODE_TO_DESC = {
    "9531508": "INDRMFD",
    "9531509": "NONRMFD",
    "9531208": "INDRMSA",
    "9531209": "NONRMSA",
    "9531308": "INDRMDD",
    "9531309": "NONRMDD",
    "9631108": "INDFXFD",
    "9631109": "NONFXFD",
    "9631308": "INDFXCA",
    "9631309": "NONFXCA",
}

# ITEM assignment applied directly onto BASE.DEPOSIT (keyed by normalised
# PROD). ISTIMA branches (9532908/9532909) ARE reachable here since pass 2
# folds ISTIMA amounts into the RM-FD codes before this stage... however,
# because the fold rewrites BNMCODE itself to '9531508xxx'/'9531509xxx'
# *before* PROD is derived, PROD will actually already read '9531508'/
# '9531509' by the time this DATA step runs. The '9532908'/'9532909' IF
# branches are therefore also unreachable here - kept for 1:1 fidelity with
# the original SAS IF cascade.
ITEM_MAP_PASS2 = {
    "9531508": ("INDRMFD", "A1.15", "- FIXED  "),
    "9531509": ("NONRMFD", "A1.12", "- FIXED  "),
    "9532908": ("INDRMFD", "A1.20", None),   # unreachable, see note above
    "9532909": ("NONRMFD", "A1.20", None),   # unreachable, see note above
    "9531208": ("INDRMSA", "A1.16", "- SAVINGS"),
    "9531209": ("NONRMSA", "A1.13", "- SAVINGS"),
    "9531308": ("INDRMDD", "A1.17", "- CURRENT"),
    "9531309": ("NONRMDD", "A1.14", "- CURRENT"),
    "9631108": ("INDFXFD", "B1.15", "- FIXED  "),
    "9631109": ("NONFXFD", "B1.12", "- FIXED  "),
    "9631308": ("INDFXCA", "B1.17", "- CURRENT"),
    "9631309": ("NONFXCA", "B1.14", "- CURRENT"),
}

# DESC -> re-derived BNM code used for the STORE.BEHAVENOTE stage.
# Only the first 6 entries are reachable (this program only processes RM
# products); FX/ISTIMA entries mirror the original SAS IF cascade verbatim
# even though those DESC values never occur given the 6-product %MISAPPD/
# %REPORT invocation list below.
DESC_TO_BNMCODE2 = {
    "INDRMFD": "9331108",
    "NONRMFD": "9331109",
    "INDRMSA": "9331208",
    "NONRMSA": "9331209",
    "INDRMDD": "9331308",
    "NONRMDD": "9331309",
    "INDFXFD": "9431108",   # unreachable - no FX product processed
    "NONFXFD": "9431109",   # unreachable - no FX product processed
    "INDFXDD": "9431308",   # unreachable - no FX product processed
    "NONFXDD": "9431309",   # unreachable - no FX product processed
    "INDISTI": "9532908",   # unreachable - ISTIMA folded away upstream
    "NONISTI": "9532909",   # unreachable - ISTIMA folded away upstream
}

# ITEM assignment for STORE.DEPRMP1 / STORE.DEPFXP1 (keyed by re-derived
# PROD from DESC_TO_BNMCODE2). Only the '9331xxx' branches are reachable.
ITEM_MAP_STAGE2 = {
    "9331108": ("A1.15", "- FIXED  "),
    "9331109": ("A1.12", "- FIXED  "),
    "9532908": (None, "A1.20"),     # special-cases DESC too (see report() use)
    "9532909": (None, "A1.20"),
    "9331208": ("A1.16", "- SAVINGS"),
    "9331209": ("A1.13", "- SAVINGS"),
    "9331308": ("A1.17", "- CURRENT"),
    "9331309": ("A1.14", "- CURRENT"),
    "9631108": ("B1.15", "- FIXED  "),
    "9631109": ("B1.12", "- FIXED  "),
    "9631308": ("B1.17", "- CURRENT"),
    "9631309": ("B1.14", "- CURRENT"),
}

# Products processed by MISAPPD / REPORT, in original invocation order.
# RM products ONLY - EIIMNLFE never invokes %MISAPPD/%REPORT for FX codes.
PRODUCTS = ["INDRMDD", "INDRMFD", "INDRMSA", "NONRMDD", "NONRMFD", "NONRMSA"]

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
REPTDATE  = reptdate_values.reptdate          # date used as "REPTDATE" macro var
REPTYEAR  = reptdate_values.reptyear          # 4-digit year (YEAR4.)
REPTMON   = reptdate_values.reptmon           # Z2.
REPTDAY   = reptdate_values.reptday           # Z2.

# DATEX uses DDMMYY10. in this program (4-digit year), unlike EIBMNLFE's
# DDMMYY8. - preserved exactly as coded in the original SAS.
DATEX     = REPTDATE.strftime("%d/%m/%Y")     # DDMMYY10.
RDATE     = REPTDATE.strftime("%d/%m/%y")     # DDMMYY8. (unchanged - used in titles)
WKLYDATE  = REPTDATE

# LAST = day-of-month of (last day of previous month), relative to TODAY(),
# not REPTDATE - mirrors the original SAS logic exactly.
_today = date.today()
_first_of_month = _today.replace(day=1)
_last_day_prev_month = _first_of_month - timedelta(days=1)
LAST_DAY_STR = _last_day_prev_month.strftime("%d")

if REPTDAY in ("08", "15", "22"):
    INSERT = "Y"
elif REPTDAY == LAST_DAY_STR and _today.day < 8:
    INSERT = "Y"
else:
    # Original SAS carries INSERT over from DEPOSIT.REPTDATE when neither
    # condition is met. No such upstream dataset is available, so INSERT
    # defaults to blank (no milestone insert) per project convention.
    INSERT = ""

print(f"  REPTDATE     : {REPTDATE}  (DATEX={DATEX} / RDATE={RDATE})")
print(f"  REPTYEAR/MON/DAY : {REPTYEAR}/{REPTMON}/{REPTDAY}")
print(f"  LAST (prev month end day) : {LAST_DAY_STR}")
print(f"  INSERT flag  : '{INSERT}'")

# ============================================================================
# HELPERS: CACHE STAMP + STREAM .sas7bdat -> PARQUET  (per EIBDLN1M.py pattern)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a .sas7bdat to Parquet in streaming chunks (schema-locked)."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0
    rows_read = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
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
                        print(f"  [{tag}] WARNING: cannot cast '{field.name}' "
                              f"{col.type}->{field.type}: {e} - nulling")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)

        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows cached.")


# ============================================================================
# STEP 2: RESOLVE + CACHE NOTE INPUT FILE  (exact date known; "i" PIBB prefix)
# ============================================================================
print("\nStep 2: Resolving NOTE input file...")

_note_prefix = f"inote{REPTYEAR}{REPTMON}{REPTDAY}"

note_path = get_latest_file(INPUT_NOTE_DIR, prefix=_note_prefix)
print(f"  NOTE : {note_path.name}")

NOTE_CACHE = CACHE_DIR / f"{note_path.stem}.parquet"

if not _cache_is_fresh(note_path, NOTE_CACHE):
    sas_to_parquet(note_path, NOTE_CACHE, "NOTE")
else:
    print("  [NOTE] Cache fresh - skipping conversion.")

# ============================================================================
# HELPER: build a two-column ITEM/ITEM4 (and optional DESC) polars expr set
# ============================================================================
def _item_expr(mapping: dict, key_col: str, include_desc: bool = False):
    item_dict  = {k: v[1] for k, v in mapping.items()}
    item4_dict = {k: v[2] for k, v in mapping.items()}
    exprs = [
        pl.col(key_col).replace_strict(item_dict, default=None).alias("ITEM"),
        pl.col(key_col).replace_strict(item4_dict, default=None).alias("ITEM4"),
    ]
    if include_desc:
        desc_dict = {k: v[0] for k, v in mapping.items()}
        exprs.append(
            pl.col(key_col).replace_strict(desc_dict, default=None).alias("DESC")
        )
    return exprs


# ============================================================================
# STEP 3: PASS 1 - DEPOSIT (excludes ISTIMA)  -> STORE.DEP (transpose)
# DATA NOTE; WHERE BNMCODE(1:7) NOT IN ('9532908','9532909');
# PROC SUMMARY NWAY CLASS BNMCODE VAR AMOUNT AMTUSD AMTSGD AMTHKD AMTAUD;
# DATA DEPOSIT; PROD/DESC derivation; AMOUNT rounded to thousands.
# ============================================================================
print("\nStep 3: Building pass-1 DEPOSIT (NOTE, ISTIMA excluded)...")

_case_when_p1 = "\n".join(
    f"            WHEN '{code}' THEN '{desc}'" for code, desc in PASS1_BNMCODE_TO_DESC.items()
)

con = duckdb.connect(database=":memory:")
deposit_p1 = con.execute(f"""
    WITH filtered AS (
        SELECT
            CAST(BNMCODE AS VARCHAR) AS BNMCODE,
            CAST(AMOUNT  AS DOUBLE)  AS AMOUNT,
            CAST(AMTUSD  AS DOUBLE)  AS AMTUSD,
            CAST(AMTSGD  AS DOUBLE)  AS AMTSGD,
            CAST(AMTHKD  AS DOUBLE)  AS AMTHKD,
            CAST(AMTAUD  AS DOUBLE)  AS AMTAUD
        FROM read_parquet('{NOTE_CACHE}')
        WHERE SUBSTR(CAST(BNMCODE AS VARCHAR), 1, 7) NOT IN ('9532908', '9532909')
    ),
    summed AS (
        -- PROC SUMMARY NWAY CLASS BNMCODE; VAR AMOUNT AMTUSD AMTSGD AMTHKD AMTAUD;
        -- AMTUSD/AMTSGD/AMTHKD/AMTAUD are summed here for parity with the SAS
        -- source but are not referenced further downstream (same as original).
        SELECT
            BNMCODE,
            SUM(AMOUNT) AS AMOUNT,
            SUM(AMTUSD) AS AMTUSD,
            SUM(AMTSGD) AS AMTSGD,
            SUM(AMTHKD) AS AMTHKD,
            SUM(AMTAUD) AS AMTAUD
        FROM filtered
        GROUP BY BNMCODE
    )
    SELECT
        SUBSTR(BNMCODE, 1, 7) AS PROD,
        SUBSTR(BNMCODE, 6, 2) AS INDNON,
        CASE SUBSTR(BNMCODE, 1, 7)
{_case_when_p1}
            ELSE NULL
        END AS DESC,
        ROUND(AMOUNT / 1000.0, 0) AS AMOUNT
    FROM summed
""").pl()
con.close()
gc.collect()
print(f"  Pass-1 DEPOSIT rows: {len(deposit_p1):,}")

# PROC SORT DATA=DEPOSIT; BY PROD DESC; -> PROC TRANSPOSE (BY PROD DESC)
_transpose_cols = ["WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST"]

grouped_p1 = (
    deposit_p1
    .filter(pl.col("DESC").is_not_null())
    .group_by(["PROD", "DESC"], maintain_order=True)
    .agg(pl.col("AMOUNT"))
)

_records = []
for row in grouped_p1.iter_rows(named=True):
    amounts = row["AMOUNT"]
    rec = {"PROD": row["PROD"], "DESC": row["DESC"]}
    for i, cname in enumerate(_transpose_cols):
        rec[cname] = amounts[i] if i < len(amounts) else None
    _records.append(rec)

store_dep = pl.DataFrame(
    _records,
    schema={"PROD": pl.Utf8, "DESC": pl.Utf8, **{c: pl.Float64 for c in _transpose_cols}},
) if _records else pl.DataFrame(
    schema={"PROD": pl.Utf8, "DESC": pl.Utf8, **{c: pl.Float64 for c in _transpose_cols}}
)

print(f"  STORE.DEP rows: {len(store_dep):,}")

# ============================================================================
# STEP 4: STORE.DEPRMP2 / STORE.DEPFXP2  (ITEM assignment, negate, RM/FX split)
# NOTE: unlike EIBMNLFE, these datasets are NOT consumed further in this
# program (no STOREGL/GL merge, and BASE.DEPOSIT is built independently in
# Step 5 from a second, re-normalised NOTE read). They are computed and
# persisted here purely for 1:1 fidelity with the original SAS DATA step.
# ============================================================================
print("\nStep 4: Building STORE.DEPRMP2 / STORE.DEPFXP2 (not used downstream)...")

item_exprs_p1 = _item_expr(ITEM_MAP_PASS1, "PROD")

store_dep2 = store_dep.with_columns(item_exprs_p1).with_columns(
    pl.col("PROD").str.slice(5, 2).alias("INDNON")
)

_balance_expr = sum(pl.col(c).fill_null(0.0) for c in _transpose_cols)
store_dep2 = store_dep2.with_columns(_balance_expr.alias("BALANCE"))

_neg_cols = _transpose_cols + ["BALANCE"]
store_dep2 = store_dep2.with_columns(
    [(-pl.col(c).fill_null(0.0)).alias(c) for c in _neg_cols]
).with_columns([
    pl.lit(DATEX).alias("DATEX"),
    pl.lit(REPTDATE).alias("DATE"),
])

deprmp2 = store_dep2.filter(pl.col("PROD").str.slice(0, 2) == "95")
depfxp2 = store_dep2.filter(pl.col("PROD").str.slice(0, 2) == "96")

deprmp2.write_parquet(STORE_LIB / "deprmp2.parquet")
depfxp2.write_parquet(STORE_LIB / "depfxp2.parquet")

print(f"  DEPRMP2 rows: {len(deprmp2):,} | DEPFXP2 rows: {len(depfxp2):,}")

# ============================================================================
# (Commented in original SAS - ISTIMA-only branch, never executed)
# ============================================================================
# DATA NOTE;
#    SET STORE1.NOTE&REPTYEAR&REPTMON&REPTDAY;
#    WHERE SUBSTR(BNMCODE,1,7) IN ('9532908','9532909');
# RUN;
# PROC SUMMARY DATA=NOTE NWAY;
# CLASS BNMCODE;
# VAR   AMOUNT AMTUSD AMTSGD AMTHKD AMTAUD;
# OUTPUT OUT=NOTE (DROP=_TYPE_ _FREQ_) SUM=;
# RUN;
# DATA DEPOSIT;
#    SET NOTE;
#    PROD = SUBSTR(BNMCODE,1,7);
#    INDNON = SUBSTR(BNMCODE,6,2);
#    IF PROD ='9532908' THEN DO; DESC = 'INDRMFD'; END;
#    IF PROD ='9532909' THEN DO; DESC = 'NONRMFD'; END;
#    AMOUNT = ROUND(AMOUNT,1000.)/1000;
# RUN;
# PROC SORT DATA=DEPOSIT; BY PROD DESC; RUN;
# PROC TRANSPOSE DATA=DEPOSIT OUT=STORE.ISTIMA
#    (RENAME=(COL1=WEEK COL2=MONTH COL3=QTR COL4=HALFYR COL5=YEAR
#             COL6=LAST));
#    BY PROD DESC;
#    VAR AMOUNT;
# RUN;

# ============================================================================
# STEP 5: PASS 2 - re-read NOTE, normalise BNM codes, build BASE.DEPOSIT
# DATA NOTE; '95317xx'->'95315xx' fold; ISTIMA->RM-FD explicit rewrites.
# PROC SUMMARY NWAY CLASS BNMCODE VAR AMOUNT AMTUSD AMTSGD AMTHKD AMTAUD;
# DATA DEPOSIT; PROD/DESC/AMOUNT derivation -> PROC TRANSPOSE -> BASE.DEPOSIT
# ============================================================================
print("\nStep 5: Building BASE.DEPOSIT (normalised BNM codes)...")

_istima_case_when = "\n".join(
    f"        WHEN '{old}' THEN '{new}'" for old, new in ISTIMA_TO_RMFD.items()
)

con = duckdb.connect(database=":memory:")
normalised = con.execute(f"""
    WITH step1 AS (
        -- IF SUBSTR(BNMCODE,1,5)='95317' THEN BNMCODE = '95315'||SUBSTR(BNMCODE,6,9);
        SELECT
            CASE
                WHEN SUBSTR(CAST(BNMCODE AS VARCHAR), 1, 5) = '95317'
                THEN '95315' || SUBSTR(CAST(BNMCODE AS VARCHAR), 6, 9)
                ELSE CAST(BNMCODE AS VARCHAR)
            END AS BNMCODE_STEP1,
            CAST(AMOUNT AS DOUBLE) AS AMOUNT,
            CAST(AMTUSD AS DOUBLE) AS AMTUSD,
            CAST(AMTSGD AS DOUBLE) AS AMTSGD,
            CAST(AMTHKD AS DOUBLE) AS AMTHKD,
            CAST(AMTAUD AS DOUBLE) AS AMTAUD
        FROM read_parquet('{NOTE_CACHE}')
    ),
    step2 AS (
        -- 12 explicit ISTIMA -> RM-FD code rewrites
        SELECT
            CASE BNMCODE_STEP1
{_istima_case_when}
                ELSE BNMCODE_STEP1
            END AS BNMCODE,
            AMOUNT, AMTUSD, AMTSGD, AMTHKD, AMTAUD
        FROM step1
    )
    SELECT
        BNMCODE,
        SUM(AMOUNT) AS AMOUNT,
        SUM(AMTUSD) AS AMTUSD,
        SUM(AMTSGD) AS AMTSGD,
        SUM(AMTHKD) AS AMTHKD,
        SUM(AMTAUD) AS AMTAUD
    FROM step2
    GROUP BY BNMCODE
""").pl()
con.close()
gc.collect()

_case_when_p2 = "\n".join(
    f"            WHEN '{code}' THEN '{desc}'" for code, desc in PASS2_BNMCODE_TO_DESC.items()
)

con = duckdb.connect(database=":memory:")
con.register("normalised", normalised.to_pandas())
deposit_p2 = con.execute(f"""
    SELECT
        SUBSTR(BNMCODE, 1, 7) AS PROD,
        SUBSTR(BNMCODE, 6, 2) AS INDNON,
        CASE SUBSTR(BNMCODE, 1, 7)
{_case_when_p2}
            ELSE NULL
        END AS DESC,
        ROUND(AMOUNT / 1000.0, 0) AS AMOUNT
    FROM normalised
""").pl()
con.close()
gc.collect()
print(f"  Pass-2 DEPOSIT rows: {len(deposit_p2):,}")

# PROC SORT DATA=DEPOSIT; BY PROD DESC; -> PROC TRANSPOSE (BY PROD DESC)
grouped_p2 = (
    deposit_p2
    .filter(pl.col("DESC").is_not_null())
    .group_by(["PROD", "DESC"], maintain_order=True)
    .agg(pl.col("AMOUNT"))
)

_records2 = []
for row in grouped_p2.iter_rows(named=True):
    amounts = row["AMOUNT"]
    rec = {"PROD": row["PROD"], "DESC": row["DESC"]}
    for i, cname in enumerate(_transpose_cols):
        rec[cname] = amounts[i] if i < len(amounts) else None
    _records2.append(rec)

deposit_t2 = pl.DataFrame(
    _records2,
    schema={"PROD": pl.Utf8, "DESC": pl.Utf8, **{c: pl.Float64 for c in _transpose_cols}},
) if _records2 else pl.DataFrame(
    schema={"PROD": pl.Utf8, "DESC": pl.Utf8, **{c: pl.Float64 for c in _transpose_cols}}
)

# DATA BASE.DEPOSIT: ITEM assignment (keyed by normalised PROD), negate, stamp.
item_exprs_p2 = _item_expr(ITEM_MAP_PASS2, "PROD")

base_deposit = deposit_t2.with_columns(item_exprs_p2).with_columns(
    pl.col("PROD").str.slice(5, 2).alias("INDNON")
)
base_deposit = base_deposit.with_columns(_balance_expr.alias("BALANCE"))
base_deposit = base_deposit.with_columns(
    [(-pl.col(c).fill_null(0.0)).alias(c) for c in _neg_cols]
).with_columns([
    pl.lit(DATEX).alias("DATEX"),
    pl.lit(REPTDATE).alias("DATE"),
])

base_deposit.write_parquet(BASE_LIB / "deposit.parquet")
print(f"  BASE.DEPOSIT rows: {len(base_deposit):,}")

# ============================================================================
# HELPER: MISAPPD  (per-product accumulator update / STORE.<PROD> build)
# ============================================================================
def misappd(prod: str) -> pl.DataFrame:
    """
    Extract today's record for `prod` from BASE.DEPOSIT, then either append
    it to the persistent BASE.<PROD> history file (milestone/INSERT day) or
    build a transient STORE.<PROD> view without touching the persistent
    history (non-milestone day). Returns STORE.<PROD> (full history up to
    and including today, sorted by DATE).
    """
    base_path = BASE_LIB / f"{prod.lower()}.parquet"

    today_rec = (
        base_deposit
        .filter((pl.col("DATE") == REPTDATE) & (pl.col("DESC") == prod))
        .select(["DATEX", "DATE", "BALANCE"])
        .with_columns((-pl.col("BALANCE")).alias("BALANCE"))
    )

    if base_path.exists():
        base_hist = pl.read_parquet(base_path)
    else:
        base_hist = pl.DataFrame(
            schema={"DATEX": pl.Utf8, "DATE": pl.Date, "BALANCE": pl.Float64}
        )

    if INSERT == "Y":
        # Idempotent re-run: drop any existing record for today, then append.
        base_hist = base_hist.filter(pl.col("DATE") != REPTDATE).sort("DATE")
        base_hist = pl.concat([base_hist, today_rec], how="vertical_relaxed")
        base_hist.write_parquet(base_path)
        store_df = base_hist.filter(pl.col("DATE") <= REPTDATE).sort("DATE")
    else:
        store_df = pl.concat([base_hist, today_rec], how="vertical_relaxed")
        store_df = store_df.filter(pl.col("DATE") <= REPTDATE).sort("DATE")

    store_df.write_parquet(STORE_LIB / f"{prod.lower()}.parquet")
    return store_df


# ============================================================================
# HELPERS FOR %REPORT
# ============================================================================
def _nth(df: pl.DataFrame, n: int):
    """1-based row access mirroring SAS _N_=n. Returns None if out of range."""
    if n < 1 or n > df.height:
        return None
    return df.row(n - 1, named=True)


def _qtr_month(month: int, offset: int) -> int:
    m = month - offset
    return 12 + m if m <= 0 else m


def _process(store_df: pl.DataFrame, weekstdt: date, weekend: date, lastdt: date):
    """Mirrors %PROCESS: MIN/MAX balance in window, % change vs LASTDT balance."""
    window = store_df.filter(
        (pl.col("DATE") >= weekstdt) & (pl.col("DATE") <= weekend)
    )
    if window.height == 0:
        return None

    last_rows = store_df.filter(pl.col("DATE") == lastdt)
    if last_rows.height == 0:
        return None
    last_balance = last_rows["BALANCE"][0]
    if last_balance in (None, 0):
        return None

    min_row = window.sort("BALANCE").row(0, named=True)
    max_row = window.sort("BALANCE", descending=True).row(0, named=True)

    if max_row["DATE"] > min_row["DATE"]:
        pct = round(((max_row["BALANCE"] - min_row["BALANCE"]) / last_balance) * 100, 2)
    else:
        pct = round(((min_row["BALANCE"] - max_row["BALANCE"]) / last_balance) * 100, 2)

    return {"DATE": weekend, "PCTAGE": pct}


def _fmt_comma(value, width: int, decimals: int = 2) -> str:
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    s = f"{v:,.{decimals}f}" if decimals > 0 else f"{int(round(v)):,}"
    return s.rjust(width)


def _print_table(title_lines, columns, rows, page_size=PAGE_SIZE):
    """
    Approximate PROC PRINT rendering with ASA carriage control.
    columns: list of (header, width, align) tuples; rows: list of str tuples.
    """
    lines = []

    def _header_block():
        hb = []
        for i, t in enumerate(title_lines):
            hb.append(("1" if i == 0 else " ") + t)
        hdr_line = "OBS".rjust(5) + " " + " ".join(
            (h.ljust(w) if align == "left" else h.rjust(w))
            for h, w, align in columns
        )
        hb.append(" " + hdr_line)
        hb.append(" " + "-" * len(hdr_line))
        return hb

    header_block = _header_block()
    lines.extend(header_block)
    on_page = len(header_block)

    for idx, row in enumerate(rows, start=1):
        if on_page >= page_size:
            hb = _header_block()
            lines.extend(hb)
            on_page = len(hb)
        data_line = str(idx).rjust(5) + " " + " ".join(
            (val.ljust(w) if align == "left" else val.rjust(w))
            for (h, w, align), val in zip(columns, row)
        )
        lines.append(" " + data_line)
        on_page += 1

    return lines


# ============================================================================
# %REPORT  (per-product WEEKLY/MONTHLY/QTRLY/HALFYRLY/YEARLY ->
#           BEHAVE / HIGHLOW(percentage) / HIGHLOWRATE(abs %) / HIGHLOW(monetary))
# ============================================================================
def report(prod: str, store_df: pl.DataFrame):
    report_lines = []

    if store_df.height == 0:
        print(f"  [{prod}] No history available - skipping report.")
        return report_lines, None

    count = store_df.height
    acount  = count - 49
    wklyday = WKLYDATE.day
    if wklyday <= 8:
        wcount = count - 45
    elif wklyday <= 15:
        wcount = count - 46
    else:
        wcount = count - 47
    mcount = count - 44
    qcount = count - 36
    hcount = count - 24
    ycount = count

    qtr1 = _qtr_month(REPTDATE.month, 3)
    qtr2 = _qtr_month(REPTDATE.month, 6)
    qtr3 = _qtr_month(REPTDATE.month, 9)

    # --- WEEKLY --------------------------------------------------------
    weekly_rows = []
    for i in range(max(1, wcount), count + 1):
        row = _nth(store_df, i)
        if row is None:
            continue
        d = row["DATE"]
        if d.day <= 8:
            weekstdt = d.replace(day=1) - timedelta(days=1)
        elif d.day <= 15:
            weekstdt = d.replace(day=8)
        elif d.day <= 22:
            weekstdt = d.replace(day=15)
        else:
            weekstdt = d.replace(day=22)
        # NOTE: original SAS CALL SYMPUT('LASTDT',WEEKSTDT) overrides the
        # separately-computed LASTDT variable with WEEKSTDT - replicated here
        # verbatim (faithful to the source, including this quirk).
        lastdt = weekstdt
        res = _process(store_df, weekstdt, d, lastdt)
        if res:
            weekly_rows.append(res)

    # --- MONTHLY ---------------------------------------------------------
    monthly_rows = []
    for i in range(max(1, mcount), count + 1):
        row = _nth(store_df, i)
        if row is None:
            continue
        d = row["DATE"]
        weekstdt = d.replace(day=1)
        lastdt = d.replace(day=1) - timedelta(days=1)
        res = _process(store_df, weekstdt, d, lastdt)
        if res:
            monthly_rows.append(res)
    if monthly_rows:
        last_date = monthly_rows[-1]["DATE"]
        monthly_rows = [
            r for r in monthly_rows
            if r["DATE"].day not in (8, 15, 22) or r["DATE"] == last_date
        ]

    # --- QTRLY -------------------------------------------------------------
    qtrly_rows = []
    for i in range(max(1, qcount), count + 1):
        row = _nth(store_df, i)
        if row is None:
            continue
        d = row["DATE"]
        setmthx = d.month - 2
        year = d.year
        if setmthx <= 0:
            setmth = 12 + setmthx
            year -= 1
        else:
            setmth = setmthx
        weekstdt = date(year, setmth, 1)
        lastdt = weekstdt - timedelta(days=1)
        res = _process(store_df, weekstdt, d, lastdt)
        if res:
            qtrly_rows.append(res)
    if qtrly_rows:
        last_date = qtrly_rows[-1]["DATE"]
        qtrly_rows = [
            r for r in qtrly_rows
            if r["DATE"] == last_date
            or (r["DATE"].day not in (8, 15, 22) and r["DATE"].month in (qtr1, qtr2, qtr3))
        ]

    # --- HALFYRLY ------------------------------------------------------
    halfyrly_rows = []
    for i in range(max(1, hcount), count + 1):
        row = _nth(store_df, i)
        if row is None:
            continue
        d = row["DATE"]
        setmthx = d.month - 5
        year = d.year
        if setmthx <= 0:
            setmth = 12 + setmthx
            year -= 1
        else:
            setmth = setmthx
        weekstdt = date(year, setmth, 1)
        lastdt = weekstdt - timedelta(days=1)
        res = _process(store_df, weekstdt, d, lastdt)
        if res:
            halfyrly_rows.append(res)
    if halfyrly_rows:
        last_date = halfyrly_rows[-1]["DATE"]
        halfyrly_rows = [
            r for r in halfyrly_rows
            if r["DATE"] == last_date
            or (r["DATE"].day not in (8, 15, 22) and r["DATE"].month == qtr2)
        ]

    # --- YEARLY --------------------------------------------------------
    yearly_rows = []
    for i in range(max(1, ycount), count + 1):
        row = _nth(store_df, i)
        if row is None:
            continue
        d = row["DATE"]
        setmthx = d.month - 11
        year = d.year
        if setmthx <= 0:
            setmth = 12 + setmthx
            year -= 1
        else:
            setmth = setmthx
        weekstdt = date(year, setmth, 1)
        lastdt = weekstdt - timedelta(days=1)
        res = _process(store_df, weekstdt, d, lastdt)
        if res:
            yearly_rows.append(res)
    if yearly_rows:
        yearly_rows = yearly_rows[-1:]   # IF LAST.STATUS; -> keep only last

    # --- BEHAVE base: tail window (rows with _N_ > ACOUNT) --------------
    behave_base = store_df.tail(max(0, count - acount)) if acount >= 0 else store_df
    # FINBAL = BALANCE; (commented ISTIMA adjustment in original SAS not
    # applicable here - ISTIMA is folded into RM-FD amounts far upstream,
    # in the pass-2 BASE.DEPOSIT build, not adjusted again at this point):
    #   IF "&PROD" = "INDRMFD" THEN FINBAL = BALANCE + &INDISTI;
    #   ELSE IF "&PROD" = "NONRMFD" THEN FINBAL = BALANCE + &NONISTI;
    #   ELSE FINBAL = BALANCE;
    outstand = float(store_df.row(-1, named=True)["BALANCE"] or 0.0)

    def _series_dict(rows):
        return {r["DATE"]: r["PCTAGE"] for r in rows} if rows else {}

    w_map = _series_dict(weekly_rows)
    m_map = _series_dict(monthly_rows)
    q_map = _series_dict(qtrly_rows)
    h_map = _series_dict(halfyrly_rows)
    y_map = _series_dict(yearly_rows)

    behave_rows = []
    for r in behave_base.iter_rows(named=True):
        d = r["DATE"]
        behave_rows.append({
            "DATE": d,
            "WPCTAGE": w_map.get(d),
            "MPCTAGE": m_map.get(d),
            "QPCTAGE": q_map.get(d),
            "BALANCE": r["BALANCE"],
            "HPCTAGE": h_map.get(d),
            "YPCTAGE": y_map.get(d),
        })

    # --- Print BEHAVIORAL TABLE ------------------------------------------
    columns = [
        ("DATE", 10, "right"), ("WPCTAGE", 10, "right"), ("MPCTAGE", 10, "right"),
        ("QPCTAGE", 10, "right"), ("BALANCE", 15, "right"),
        ("HPCTAGE", 10, "right"), ("YPCTAGE", 10, "right"),
    ]
    rows_fmt = []
    for r in behave_rows:
        rows_fmt.append((
            r["DATE"].strftime("%d/%m/%y"),
            "" if r["WPCTAGE"] is None else f"{r['WPCTAGE']:.2f}",
            "" if r["MPCTAGE"] is None else f"{r['MPCTAGE']:.2f}",
            "" if r["QPCTAGE"] is None else f"{r['QPCTAGE']:.2f}",
            _fmt_comma(r["BALANCE"], 15, 2).strip(),
            "" if r["HPCTAGE"] is None else f"{r['HPCTAGE']:.2f}",
            "" if r["YPCTAGE"] is None else f"{r['YPCTAGE']:.2f}",
        ))
    report_lines += _print_table(
        [f"{prod} BEHAVIORAL TABLE {RDATE}"], columns, rows_fmt
    )

    # --- HIGHLOW (percentage, monotonic cascade + cap) -----------------
    def _min_max(values):
        vals = [v for v in values if v is not None]
        if not vals:
            return None, None
        return min(vals), max(vals)

    lowest, highest = {}, {}
    for label, series in (("WEEK", w_map), ("MONTH", m_map), ("QTR", q_map),
                           ("HALFYR", h_map), ("YEAR", y_map)):
        lo, hi = _min_max(series.values())
        lowest[label] = lo if lo is not None else 0.0
        highest[label] = hi if hi is not None else 0.0

    if highest["WEEK"] > highest["MONTH"]:
        highest["MONTH"] = highest["WEEK"]
    if highest["MONTH"] > highest["QTR"]:
        highest["QTR"] = highest["MONTH"]
    if highest["QTR"] > highest["HALFYR"]:
        highest["HALFYR"] = highest["QTR"]
    if highest["HALFYR"] > highest["YEAR"]:
        highest["YEAR"] = highest["HALFYR"]
    for k in highest:
        if highest[k] > 100:
            highest[k] = 100.0

    if lowest["WEEK"] < lowest["MONTH"]:
        lowest["MONTH"] = lowest["WEEK"]
    if lowest["MONTH"] < lowest["QTR"]:
        lowest["QTR"] = lowest["MONTH"]
    if lowest["QTR"] < lowest["HALFYR"]:
        lowest["HALFYR"] = lowest["QTR"]
    if lowest["HALFYR"] < lowest["YEAR"]:
        lowest["YEAR"] = lowest["HALFYR"]
    for k in lowest:
        if lowest[k] < -100:
            lowest[k] = -100.0

    highlow_columns = [
        ("STATUS", 10, "left"), ("WEEK", 10, "right"), ("MONTH", 10, "right"),
        ("QTR", 10, "right"), ("HALFYR", 10, "right"), ("YEAR", 10, "right"),
    ]
    highlow_rows = [
        ("HIGHEST",
         f"{highest['WEEK']:.2f}", f"{highest['MONTH']:.2f}", f"{highest['QTR']:.2f}",
         f"{highest['HALFYR']:.2f}", f"{highest['YEAR']:.2f}"),
        ("LOWEST",
         f"{lowest['WEEK']:.2f}", f"{lowest['MONTH']:.2f}", f"{lowest['QTR']:.2f}",
         f"{lowest['HALFYR']:.2f}", f"{lowest['YEAR']:.2f}"),
    ]
    report_lines += _print_table([f"{prod} BEHAVIORAL TABLE {RDATE}"], highlow_columns, highlow_rows)

    # --- FINAL.HIGHLOWRATE&PROD (abs-merged % figures, NOT printed) -----
    # Mirrors: MERGE HIGH LOW; ABS(WEEK1) vs ABS(WEEK) etc. This dataset is
    # never PROC PRINT'ed in the original SAS - computed/stored only.
    abs_pct = {
        k: max(abs(lowest[k]), abs(highest[k])) for k in ("WEEK", "MONTH", "QTR", "HALFYR", "YEAR")
    }
    highlowrate_dict = {"DESC": prod, **abs_pct}

    # --- FINAL.HIGHLOW&PROD (monetary, printed as "BEHAVIORAL FIGURE") --
    week_amt   = round(abs_pct["WEEK"] * outstand / 100)
    month_amt  = round((abs_pct["MONTH"] * outstand / 100) - week_amt)
    qtr_amt    = round((abs_pct["QTR"] * outstand / 100) - (week_amt + month_amt))
    halfyr_amt = round((abs_pct["HALFYR"] * outstand / 100) - (week_amt + month_amt + qtr_amt))
    year_amt   = round((abs_pct["YEAR"] * outstand / 100) - (week_amt + month_amt + qtr_amt + halfyr_amt))
    last_amt   = round(outstand - (week_amt + month_amt + qtr_amt + halfyr_amt + year_amt))
    total_amt  = round(outstand)

    week_amt   = max(week_amt, 0)
    month_amt  = max(month_amt, 0)
    qtr_amt    = max(qtr_amt, 0)
    halfyr_amt = max(halfyr_amt, 0)
    year_amt   = max(year_amt, 0)

    highlow_monetary_columns = [
        ("DESC", 10, "left"), ("WEEK", 15, "right"), ("MONTH", 15, "right"),
        ("QTR", 15, "right"), ("HALFYR", 15, "right"), ("YEAR", 15, "right"),
        ("LAST", 15, "right"), ("TOTAL", 15, "right"),
    ]
    highlow_monetary_row = (
        prod, _fmt_comma(week_amt, 15, 0).strip(), _fmt_comma(month_amt, 15, 0).strip(),
        _fmt_comma(qtr_amt, 15, 0).strip(), _fmt_comma(halfyr_amt, 15, 0).strip(),
        _fmt_comma(year_amt, 15, 0).strip(), _fmt_comma(last_amt, 15, 0).strip(),
        _fmt_comma(total_amt, 15, 0).strip(),
    )
    report_lines += _print_table(
        [f"{prod} BEHAVIORAL FIGURE TO BE REPORTED {RDATE}"],
        highlow_monetary_columns, [highlow_monetary_row]
    )

    highlow_dict = {
        "DESC": prod, "WEEK": week_amt, "MONTH": month_amt, "QTR": qtr_amt,
        "HALFYR": halfyr_amt, "YEAR": year_amt, "LAST": last_amt, "TOTAL": total_amt,
    }
    return report_lines, highlow_dict, highlowrate_dict


# ============================================================================
# STEP 6: MISAPPD + REPORT FOR EACH PRODUCT (RM PRODUCTS ONLY)
# ============================================================================
print("\nStep 6: Running MISAPPD + REPORT per product (RM only)...")

all_report_lines = []
highlow_results = []
highlowrate_results = []

for prod in PRODUCTS:
    print(f"\n  Processing product: {prod}")
    store_df = misappd(prod)
    lines, highlow_dict, highlowrate_dict = report(prod, store_df)
    all_report_lines += lines
    if highlow_dict:
        highlow_results.append(highlow_dict)
    if highlowrate_dict:
        highlowrate_results.append(highlowrate_dict)

# Persist FINAL.HIGHLOWRATE&PROD equivalents (not printed) for traceability.
if highlowrate_results:
    pl.DataFrame(highlowrate_results).write_parquet(FINAL_LIB / "highlowrate.parquet")

# ============================================================================
# STEP 7: STORE.BEHAVENOTE  (re-derive BNM code from DESC)
# ============================================================================
print("\nStep 7: Building STORE.BEHAVENOTE / DEPRMP1 / DEPFXP1...")

behavenote = pl.DataFrame(highlow_results) if highlow_results else pl.DataFrame(
    schema={"DESC": pl.Utf8, "WEEK": pl.Float64, "MONTH": pl.Float64, "QTR": pl.Float64,
            "HALFYR": pl.Float64, "YEAR": pl.Float64, "LAST": pl.Float64, "TOTAL": pl.Float64}
)

behavenote = behavenote.with_columns(
    pl.col("DESC").replace_strict(DESC_TO_BNMCODE2, default=None).alias("PROD")
)

# NOTE (faithful SAS bug reproduction): the original SAS DATA step computes
#   INDNON = SUBSTR(BNMCODE,6,2);
# but BNMCODE does not exist anywhere in STORE.BEHAVENOTE's input (only
# DESC/PROD/WEEK/.../TOTAL do) - it is an uninitialised variable reference
# in the original source. SAS would treat it as missing/blank at runtime.
# Reproduced here as a null INDNON rather than "fixed" to SUBSTR(PROD,6,2).
behavenote = behavenote.with_columns(pl.lit(None, dtype=pl.Utf8).alias("INDNON"))
behavenote = behavenote.with_columns(
    (pl.col("TOTAL") / 1000.0).round(0).alias("AMOUNT")  # AMOUNT = ROUND(AMOUNT,1000.)/1000;
)

behavenote.write_parquet(STORE_LIB / "behavenote.parquet")

# ============================================================================
# STEP 8: STORE.DEPRMP1 / STORE.DEPFXP1
# Only '9331xxx' PROD values are reachable given the 6 RM products above,
# so DEPFXP1 (SUBSTR(PROD,1,2)='94') will always be empty - split condition
# is preserved verbatim regardless.
# ============================================================================
print("\nStep 8: Building STORE.DEPRMP1 / STORE.DEPFXP1...")

_item1_dict = {k: v[0] for k, v in ITEM_MAP_STAGE2.items()}
_item4_dict = {k: v[1] for k, v in ITEM_MAP_STAGE2.items()}

deprmp1_source = behavenote.with_columns([
    pl.col("PROD").replace_strict(_item1_dict, default=None).alias("ITEM"),
    pl.col("PROD").replace_strict(_item4_dict, default=None).alias("ITEM4"),
]).with_columns([
    # IF PROD = '9532908' THEN DESC='INDISIT'; IF PROD = '9532909' THEN DESC='NONISTI';
    # (unreachable given only RM products are ever populated; DESC left as-is otherwise)
    pl.when(pl.col("PROD") == "9532908").then(pl.lit("INDISIT"))
      .when(pl.col("PROD") == "9532909").then(pl.lit("NONISTI"))
      .otherwise(pl.col("DESC")).alias("DESC"),
    pl.col("PROD").str.slice(5, 2).alias("INDNON"),
])

_deprmp1_neg_cols = ["WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST"]
deprmp1_source = deprmp1_source.with_columns(
    sum(pl.col(c).fill_null(0.0) for c in _deprmp1_neg_cols).alias("BALANCE")
).with_columns(
    [(-pl.col(c).fill_null(0.0)).alias(c) for c in _deprmp1_neg_cols + ["BALANCE"]]
)

deprmp1 = deprmp1_source.filter(pl.col("PROD").str.slice(0, 2).is_in(["93", "95"]))
depfxp1 = deprmp1_source.filter(pl.col("PROD").str.slice(0, 2) == "94")

# PROC SORT DATA=STORE.DEPRMP1; BY DESCENDING INDNON; and same for DEPFXP1 -
# omitted: subsequent access (final REPORT dataset) filters/labels by INDNON
# value, not physical order, so the sort adds no observable effect here.
deprmp1.write_parquet(STORE_LIB / "deprmp1.parquet")
depfxp1.write_parquet(STORE_LIB / "depfxp1.parquet")

print(f"  DEPRMP1 rows: {len(deprmp1):,} | DEPFXP1 rows: {len(depfxp1):,}")

# ============================================================================
# STEP 9: FINAL REPORT DATASET  (OPTIONS MISSING=0; - computed, not printed)
# ============================================================================
print("\nStep 9: Building final REPORT dataset (DEPRMP1-based, not printed)...")

report_ds = deprmp1.with_columns([
    pl.lit("DEPOSIT :").alias("ITEM2"),
    pl.when(pl.col("INDNON") == "08").then(pl.lit("INDIVIDUALS    "))
      .when(pl.col("INDNON") == "09").then(pl.lit("NON-INDIVUDUALS"))
      .otherwise(None).alias("ITEM3"),
]).with_columns(
    [(-pl.col(c).fill_null(0.0)).alias(c) for c in
     ["BALANCE", "WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST"]]
)
report_ds.write_parquet(STORE_LIB / "report.parquet")

# ============================================================================
# (Commented in original SAS - ISTIMA figure-to-be-reported, never executed
# because INDISTI/NONISTI macro variables are only populated by the also-
# commented STORE.ISTIMA branch above.)
# ============================================================================
# DATA FINAL.HIGHLOWINDISTI;
#    SET FINAL.HIGHLOWRATEINDRMFD;
#    DESC = "INDISTI";
#    AMOUNT = ABS(&INDISTI);
#    WEEK   = ROUND((WEEK * AMOUNT / 100),1.);
#    MONTH  = ROUND(((MONTH * AMOUNT / 100) - WEEK),1.);
#    QTR    = ROUND(((QTR * AMOUNT / 100) - SUM(WEEK,MONTH)),1.);
#    HALFYR = ROUND(((HALFYR * AMOUNT /100) - SUM(WEEK,MONTH,QTR)),1.);
#    YEAR   = ROUND(((YEAR * AMOUNT /100) - SUM(WEEK,MONTH,QTR,HALFYR)),1.);
#    LAST   = ROUND((AMOUNT - SUM(WEEK,MONTH,QTR,HALFYR,YEAR)),1.);
#    TOTAL  = ROUND(AMOUNT,1.);
#    IF WEEK   < 0 THEN WEEK   = 0;
#    IF MONTH  < 0 THEN MONTH  = 0;
#    IF QTR    < 0 THEN QTR    = 0;
#    IF HALFYR < 0 THEN HALFYR = 0;
#    IF YEAR   < 0 THEN YEAR   = 0;
# RUN;
# DATA FINAL.HIGHLOWNONISTI;
#    SET FINAL.HIGHLOWRATENONRMFD;
#    DESC = "NONISTI";
#    [... same pattern as above ...]
# RUN;

# ============================================================================
# STEP 10: WRITE OUTPUT  (ASA carriage control, LRECL=250)
# ============================================================================
print("\nStep 10: Writing SASLIST output...")

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in all_report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(all_report_lines):,}")

print("\n--- Report preview (terminal echo) ---")
for ln in all_report_lines[:80]:
    print(ln)
if len(all_report_lines) > 80:
    print(f"... ({len(all_report_lines) - 80} more lines)")

gc.collect()
print("\nEIIMNLFE complete.")
