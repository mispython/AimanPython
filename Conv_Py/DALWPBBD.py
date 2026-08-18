#!/usr/bin/env python3
"""
Program : DALWPBBD.py
Function: PBB deposit manipulation extracted from SAP.PBB.MNITB
          (SAVING and CURRENT accounts) — invoked by weekly BNM
          reporting jobs (e.g. EIIWREXL) via %INC PGM(DALWPBBD).

Original : DALWPBBD (invoked by job EIBWEEK1-3)

Dependency (format library):
    %INC PGM(PBBDPFMT);  -> already converted as PBBDPFMT.py
    from PBBDPFMT import (statecd_format, saprod_format, sadenom_format,
                           sacustcd_format, caprod_format, cadenom_format,
                           ddcustcd_format, ACE_PRODUCTS)

============================================================================
PHYSICAL INPUT DATASETS USED BY THIS PROGRAM  (all .sas7bdat, cached to
Parquet on first read per EIBDLN1M.py's chunked-conversion pattern)
============================================================================
1. DEPOSIT.SAVING  (SAP.PBB.MNITB savings extract)
   File     : sa<mmwyy>.sas7bdat  (weekly, e.g. sa07226 — filename
              convention assumed per project convention; unconfirmed)
   Path     : INPUT_SAVING_DIR, resolved via input_date.get_latest_file()
   Used in  : Step 2 - build BNM_SAVG (CUSTCD/STATECD/PRODCD/AMTIND
              formats applied; filtered OPENIND NOT IN ('B','C','P')
              AND CURBAL >= 0)

2. DEPOSIT.CURRENT  (SAP.PBB.MNITB current extract)
   File     : ca<mmwyy>.sas7bdat  (weekly — filename convention assumed;
              unconfirmed)
   Path     : INPUT_CURRENT_DIR, resolved via input_date.get_latest_file()
   Used in  : Step 3 - build BNM_CURN / BNM_FCY (STATECD/PRODCD/AMTIND/
              CUSTCD formats + ACE / FCY-range branching)

3. CISDP.DEPOSIT  (CIS deposit customer-number extension — fixed
   filename, no date token; same physical source used by EIBDLN1M.py)
   File     : CISDP_deposit.sas7bdat
   Path     : INPUT_CISDP_DIR
   Used in  : Step 3 - left join onto BNM_FCY rows (BY ACCTNO, IF A) to
              attach CUSTNO before appending FCY rows onto BNM_CURN

------------------------------------------------------------------------
NON-FILE / DERIVED / TEMPORARY OUTPUTS PRODUCED BY THIS PROGRAM
------------------------------------------------------------------------
This module is designed to be IMPORTED (module-level execution, mirroring
SAS %INC PGM(DALWPBBD)) by downstream programs such as EIIWREXL.py. It
produces the following in-memory/cached artefacts for their consumption —
none of these are physical mainframe inputs, they are built here:

- BNM_SAVG  (module-level polars DataFrame) : SAP.PBB.MNITB savings, BNM
  library equivalent of BNM.SAVG&REPTMON&NOWK
- BNM_CURN  (module-level polars DataFrame) : SAP.PBB.MNITB current +
  appended FCY rows, equivalent of BNM.CURN&REPTMON&NOWK
- BNM_DEPT  (module-level polars DataFrame) : branch-level PROC SUMMARY
  rollup of SAVG + CURN, equivalent of BNM.DEPT&REPTMON&NOWK
- SAVG<REPTMON><NOWK>.parquet / CURN<REPTMON><NOWK>.parquet /
  DEPT<REPTMON><NOWK>.parquet, written under OUTPUT_CACHE_DIR — persisted
  copies of the three DataFrames above so other programs in the same
  pipeline can read them via read_parquet() without re-running this module.

REPTDATE.py / no reptdate.parquet:
  DEPOSIT.REPTDATE has no physical Parquet/SAS equivalent in this project;
  REPTDATE is derived from REPTDATE.py's get_reptdate_values() (Step 0).
  NOWK here replicates the SAS SELECT(DAY(REPTDATE)) EXACT-MATCH logic
  (WHEN 8/15/22/OTHERWISE) — intentionally NOT the ranged NOWK returned
  by get_reptdate_values(); this program only fires on exact cut-off days.
"""

import gc
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from PBBDPFMT import (
    statecd_format,
    saprod_format,
    sadenom_format,
    sacustcd_format,
    caprod_format,
    cadenom_format,
    ddcustcd_format,
    ACE_PRODUCTS,
)

# ============================================================================
# PATH CONFIGURATION (each physical input kept independent)
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_SAVING_DIR = BASE_DIR / "input" / "prod" / "DALWPBBD" / "saving"   # deposit_saving
SAVING_PREFIX     = "sa"

INPUT_CURRENT_DIR = BASE_DIR / "input" / "prod" / "DALWPBBD" / "current"  # deposit_current
CURRENT_PREFIX     = "ca"

INPUT_CISDP_DIR = Path("/stgsrcsys/host/uat") / "CISDP_deposit.sas7bdat"  # cisdp_deposit

# Parquet cache directory for the .sas7bdat -> Parquet conversion step
CACHE_DIR = BASE_DIR / "cache" / "DALWPBBD"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Output cache directory — where BNM_SAVG/BNM_CURN/BNM_DEPT are persisted
# for downstream programs (e.g. EIIWREXL.py) to read via read_parquet()
OUTPUT_CACHE_DIR = BASE_DIR / "work" / "BNM"
OUTPUT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000

# ============================================================================
# STEP 0: REPORT DATE / WEEK NUMBER  (DATA REPTDATE; SET DEPOSIT.REPTDATE;
# SELECT(DAY(REPTDATE)) ...  — no physical file, see module docstring)
# ============================================================================
print("Step 0: Deriving report date / week number...")

_reptdate_values = get_reptdate_values()
REPTDATE = _reptdate_values.reptdate

_day = REPTDATE.day
if _day == 8:
    NOWK = "1"
elif _day == 15:
    NOWK = "2"
elif _day == 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTMON = f"{REPTDATE.month:02d}"        # PUT(MONTH(REPTDATE),Z2.)

print(f"  REPTDATE : {REPTDATE}")
print(f"  REPTMON  : {REPTMON}")
print(f"  NOWK     : {NOWK}")

SAVG_CACHE = OUTPUT_CACHE_DIR / f"SAVG{REPTMON}{NOWK}.parquet"
CURN_CACHE = OUTPUT_CACHE_DIR / f"CURN{REPTMON}{NOWK}.parquet"
DEPT_CACHE = OUTPUT_CACHE_DIR / f"DEPT{REPTMON}{NOWK}.parquet"


# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET
# (identical pattern to EIBDLN1M.py: freshness check via mtime, PyArrow
# ParquetWriter with schema locked on first chunk)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
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
                        print(f"  [{tag}] WARNING casting '{field.name}': {e}")
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


def _load_cached(sas_path: Path, cache_path: Path, tag: str) -> Path:
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# STEP 1: RESOLVE & CACHE INPUT FILES  (see module docstring items 1-3)
# ============================================================================
print("\nStep 1: Resolving and caching input files...")

saving_path  = get_latest_file(INPUT_SAVING_DIR, prefix=SAVING_PREFIX)
current_path = get_latest_file(INPUT_CURRENT_DIR, prefix=CURRENT_PREFIX)

SAVING_SAS_CACHE  = CACHE_DIR / f"{saving_path.stem}.parquet"
CURRENT_SAS_CACHE = CACHE_DIR / f"{current_path.stem}.parquet"
CISDP_SAS_CACHE   = CACHE_DIR / "cisdp.parquet"

_load_cached(saving_path, SAVING_SAS_CACHE, "SAVING")
_load_cached(current_path, CURRENT_SAS_CACHE, "CURRENT")
_load_cached(INPUT_CISDP_DIR, CISDP_SAS_CACHE, "CISDP")


# ============================================================================
# STEP 2: BUILD BNM.SAVG&REPTMON&NOWK  (input: item 1 — DEPOSIT.SAVING)
# DATA BNM.SAVG&REPTMON&NOWK &SAVG2;
#   SET DEPOSIT.SAVING &SAVG1;
#   IF OPENIND NOT IN ('B','C','P') AND CURBAL GE 0;
#   CUSTCD=PUT(CUSTCODE, SACUSTCD.); STATECD=PUT(BRANCH, STATECD.);
#   PRODCD=PUT(PRODUCT, SAPROD.);    AMTIND=PUT(PRODUCT, SADENOM.);
# ============================================================================
print("\nStep 2: Building BNM_SAVG...")

con = duckdb.connect(database=":memory:")
_savg_raw = con.execute(f"""
    SELECT
        CAST(BRANCH   AS INTEGER) AS BRANCH,
        CAST(PRODUCT  AS INTEGER) AS PRODUCT,
        CAST(CUSTCODE AS INTEGER) AS CUSTCODE,
        CAST(NAME     AS VARCHAR) AS NAME,
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CURBAL   AS DOUBLE)  AS CURBAL,
        CAST(INTPAYBL AS DOUBLE)  AS INTPAYBL,
        CAST(COSTCTR  AS VARCHAR) AS COSTCTR,
        CAST(DNBFISME AS VARCHAR) AS DNBFISME,
        CAST(CURCODE  AS VARCHAR) AS CURCODE
    FROM read_parquet('{SAVING_SAS_CACHE.as_posix()}')
    WHERE OPENIND NOT IN ('B','C','P') AND CURBAL >= 0
""").pl()
con.close()

BNM_SAVG = _savg_raw.with_columns([
    pl.col("CUSTCODE").map_elements(sacustcd_format, return_dtype=pl.Utf8).alias("CUSTCD"),
    pl.col("BRANCH").map_elements(statecd_format, return_dtype=pl.Utf8).alias("STATECD"),
    pl.col("PRODUCT").map_elements(saprod_format, return_dtype=pl.Utf8).alias("PRODCD"),
    pl.col("PRODUCT").map_elements(sadenom_format, return_dtype=pl.Utf8).alias("AMTIND"),
]).select([
    "BRANCH", "PRODUCT", "CUSTCD", "STATECD", "PRODCD", "NAME", "ACCTNO",
    "CURBAL", "INTPAYBL", "AMTIND", "COSTCTR", "DNBFISME", "CURCODE",
])

del _savg_raw
gc.collect()
print(f"  BNM_SAVG rows: {len(BNM_SAVG):,}")


# ============================================================================
# STEP 3: BUILD BNM.CURN&REPTMON&NOWK / BNM.FCY&REPTMON&NOWK
# (inputs: item 2 — DEPOSIT.CURRENT; item 3 — CISDP.DEPOSIT)
# ============================================================================
print("\nStep 3: Building BNM_CURN / BNM_FCY...")

con = duckdb.connect(database=":memory:")
_curn_raw = con.execute(f"""
    SELECT
        CAST(BRANCH     AS INTEGER) AS BRANCH,
        CAST(PRODUCT    AS INTEGER) AS PRODUCT,
        CAST(CUSTCODE   AS INTEGER) AS CUSTCODE,
        CAST(NAME       AS VARCHAR) AS NAME,
        CAST(ACCTNO     AS BIGINT)  AS ACCTNO,
        CAST(CURBAL     AS DOUBLE)  AS CURBAL,
        CAST(INTPAYBL   AS DOUBLE)  AS INTPAYBL,
        CAST(ODINTACC   AS VARCHAR) AS ODINTACC,
        CAST(COSTCTR    AS VARCHAR) AS COSTCTR,
        CAST(SECTOR     AS INTEGER) AS SECTOR,
        CAST(DNBFISME   AS VARCHAR) AS DNBFISME,
        CAST(CURCODE    AS VARCHAR) AS CURCODE,
        CAST(INTRATE    AS DOUBLE)  AS INTRATE,
        CAST(BILLERIND  AS VARCHAR) AS BILLERIND
    FROM read_parquet('{CURRENT_SAS_CACHE.as_posix()}')
    WHERE OPENIND NOT IN ('B','C','P') AND CURBAL >= 0
""").pl()
con.close()

_curn_raw = _curn_raw.with_columns([
    pl.col("BRANCH").map_elements(statecd_format, return_dtype=pl.Utf8).alias("STATECD"),
    pl.col("PRODUCT").map_elements(caprod_format, return_dtype=pl.Utf8).alias("PRODCD"),
    pl.col("PRODUCT").map_elements(cadenom_format, return_dtype=pl.Utf8).alias("AMTIND"),
])

# SELECT(PRODUCT): 104 -> '02', 105 -> '81', OTHERWISE PUT(CUSTCODE, DDCUSTCD.)
_curn_raw = _curn_raw.with_columns(
    pl.when(pl.col("PRODUCT") == 104).then(pl.lit("02"))
      .when(pl.col("PRODUCT") == 105).then(pl.lit("81"))
      .otherwise(pl.col("CUSTCODE").map_elements(ddcustcd_format, return_dtype=pl.Utf8))
      .alias("CUSTCD")
)

is_ace = pl.col("PRODUCT").is_in(list(ACE_PRODUCTS))
is_fcy_range = (
    ((pl.col("PRODUCT") >= 400) & (pl.col("PRODUCT") <= 444))
    | pl.col("PRODUCT").is_in([450, 451, 452, 453, 454])
)

# ACE branch: INTPAYBL forced to 0; PRODCD/AMTIND recomputed (same formula,
# same result as the initial computation above — replicated for fidelity).
_ace_rows = _curn_raw.filter(is_ace).with_columns([
    pl.lit(0.0).alias("INTPAYBL"),
    pl.col("PRODUCT").map_elements(caprod_format, return_dtype=pl.Utf8).alias("PRODCD"),
    pl.col("PRODUCT").map_elements(cadenom_format, return_dtype=pl.Utf8).alias("AMTIND"),
])

# FCY branch: SECTOR reclassification based on CUSTCD residency grouping.
_fcy_rows = _curn_raw.filter(~is_ace & is_fcy_range)
_is_resident = pl.col("CUSTCD").is_in(["77", "78", "95"])
_fcy_rows = _fcy_rows.with_columns(
    pl.when(_is_resident & pl.col("SECTOR").is_in([4, 5])).then(1)
      .when(_is_resident & ~pl.col("SECTOR").is_in([1, 2, 3, 4, 5])).then(1)
      .when(~_is_resident & pl.col("SECTOR").is_in([1, 2, 3])).then(4)
      .when(~_is_resident & ~pl.col("SECTOR").is_in([1, 2, 3, 4, 5])).then(4)
      .otherwise(pl.col("SECTOR"))
      .alias("SECTOR")
)

# Remaining rows: neither ACE nor FCY range -> straight to CURN.
_plain_rows = _curn_raw.filter(~is_ace & ~is_fcy_range)

_curn_out_cols = [
    "BRANCH", "PRODUCT", "CUSTCD", "STATECD", "PRODCD", "NAME", "ACCTNO",
    "CURBAL", "INTPAYBL", "AMTIND", "ODINTACC", "COSTCTR", "SECTOR",
    "DNBFISME", "CURCODE", "INTRATE", "BILLERIND",
]

BNM_CURN_BASE = pl.concat(
    [_ace_rows.select(_curn_out_cols), _plain_rows.select(_curn_out_cols)]
)
BNM_FCY = _fcy_rows.select(_curn_out_cols + ["CUSTCODE"])

del _curn_raw, _ace_rows, _plain_rows
gc.collect()

# ----------------------------------------------------------------------------
# PROC SORT DATA=CISDP.DEPOSIT OUT=CISDP(KEEP=ACCTNO CUSTNO); BY ACCTNO;
# MERGE BNM.FCY(IN=A) CISDP; BY ACCTNO; IF A;   -- left join FCY -> CISDP
# PROC APPEND FCY(DROP=CUSTNO) BASE=CURN         -- append FCY rows to CURN
# ----------------------------------------------------------------------------
print("  Joining FCY with CISDP and appending onto CURN...")

con = duckdb.connect(database=":memory:")
cisdp = con.execute(f"""
    SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(CUSTNO AS VARCHAR) AS CUSTNO
    FROM read_parquet('{CISDP_SAS_CACHE.as_posix()}')
""").pl()
con.close()

BNM_FCY = BNM_FCY.join(cisdp, on="ACCTNO", how="left")  # IF A -> keep all FCY rows
BNM_FCY_FOR_APPEND = BNM_FCY.select(_curn_out_cols)      # DROP=CUSTNO

BNM_CURN = pl.concat([BNM_CURN_BASE, BNM_FCY_FOR_APPEND])

del BNM_CURN_BASE, BNM_FCY_FOR_APPEND, cisdp
gc.collect()
print(f"  BNM_CURN rows: {len(BNM_CURN):,}  (incl. FCY appended)")


# ============================================================================
# STEP 4: BUILD BNM.DEPT&REPTMON&NOWK  (branch-level summary; not a
# physical input — built from BNM_SAVG/BNM_CURN produced in Steps 2-3)
# PROC DATASETS ... DELETE DEPT&REPTMON&NOWK;   -- fresh rebuild every run
# PROC SUMMARY SAVG NWAY CLASS BRANCH STATECD PRODCD CUSTCD AMTIND -> DEPT
# PROC SUMMARY CURN NWAY MISSING CLASS BRANCH STATECD PRODCD CUSTCD SECTOR
#              AMTIND -> DEPT (FORCE append, differing class columns)
# ============================================================================
print("\nStep 4: Building BNM_DEPT (branch-level summary)...")

_dept_savg = (
    BNM_SAVG.group_by(["BRANCH", "STATECD", "PRODCD", "CUSTCD", "AMTIND"])
    .agg([pl.col("CURBAL").sum(), pl.col("INTPAYBL").sum()])
)

_dept_curn = (
    BNM_CURN.group_by(["BRANCH", "STATECD", "PRODCD", "CUSTCD", "SECTOR", "AMTIND"])
    .agg([pl.col("CURBAL").sum(), pl.col("INTPAYBL").sum()])
)

# PROC APPEND FORCE: differing columns (SECTOR only in CURN summary) are
# unioned; SAVG summary rows get SECTOR = null.
BNM_DEPT = pl.concat([_dept_savg, _dept_curn], how="diagonal_relaxed")

del _dept_savg, _dept_curn
gc.collect()
print(f"  BNM_DEPT rows: {len(BNM_DEPT):,}")


# ============================================================================
# STEP 5: WRITE PARQUET CACHE  (temporary artefacts for this program's own
# and downstream programs' use — see module docstring "NON-FILE / DERIVED /
# TEMPORARY OUTPUTS")
# ============================================================================
BNM_SAVG.write_parquet(SAVG_CACHE)
BNM_CURN.write_parquet(CURN_CACHE)
BNM_DEPT.write_parquet(DEPT_CACHE)

print(f"\nDALWPBBD complete. Cached: {SAVG_CACHE.name}, {CURN_CACHE.name}, {DEPT_CACHE.name}")
