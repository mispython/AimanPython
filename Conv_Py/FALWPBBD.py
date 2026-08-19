#!/usr/bin/env python3
"""
Program : FALWPBBD.py
Function: Filter out invalid customer code & product code — builds
          BNM.FDWKLY (fixed-deposit weekly extract) and BNM.UMA
          (unclaimed moneys account) datasets.

Original : FALWPBBD (invoked via %INC PGM(FALWPBBD), e.g. by EIIWREXL)

Dependency (format library):
    %INC PGM(PBBDPFMT);  -> already converted as PBBDPFMT.py
    from PBBDPFMT import (fdprod_format, fddenom_format, statecd_format,
                           fdcustcd_format, ifdcuscd_format)

============================================================================
PHYSICAL INPUT DATASETS USED BY THIS PROGRAM  (all .sas7bdat, cached to
Parquet on first read per EIBDLN1M.py's chunked-conversion pattern)
============================================================================
1. FD.FD  (fixed deposit master extract)
   File     : fd<yymmdd>.sas7bdat  (filename convention assumed 'fd'
              prefix per project convention; unconfirmed)
   Path     : INPUT_FD_DIR, resolved via input_date.get_latest_file()
   Used in  : Step 2 - build BNM_FDWKLY (BIC/AMTIND/STATE/CUSTCODE
              derivation, ACCTTYPE override, OPENIND filter)

2. DEPOSIT.UMA  (unclaimed moneys account extract)
   File     : uma<yymmdd>.sas7bdat  (filename convention assumed 'uma'
              prefix; unconfirmed)
   Path     : INPUT_UMA_DIR, resolved via input_date.get_latest_file()
   Used in  : Step 3 - build BNM_UMA (CUSTCODE/AMTIND/STATE derivation,
              BIC fixed '42199', OPENIND filter)

------------------------------------------------------------------------
NON-FILE / DERIVED / TEMPORARY OUTPUTS PRODUCED BY THIS PROGRAM
------------------------------------------------------------------------
This module is designed to be IMPORTED (module-level execution, mirroring
SAS %INC PGM(FALWPBBD)) by downstream programs such as EIIWREXL.py. It
produces the following in-memory/cached artefacts for their consumption —
none of these are physical mainframe inputs, they are built here:

- BNM_FDWKLY (module-level polars DataFrame) : equivalent of BNM.FDWKLY
- BNM_UMA    (module-level polars DataFrame) : equivalent of BNM.UMA
- FDWKLY<REPTMON><NOWK>.parquet / UMA<REPTMON><NOWK>.parquet, written
  under OUTPUT_CACHE_DIR — persisted copies of the two DataFrames above
  so other programs in the same pipeline can read them via
  read_parquet() without re-running this module.

REPTDATE.py / no reptdate.parquet:
  This program shares REPTMON/NOWK macro variables (via %INC) with
  DALWPBBD in the original SAS job, so the same exact-match
  SELECT(DAY(REPTDATE)) logic (WHEN 8/15/22/OTHERWISE) is replicated here
  from REPTDATE.py's get_reptdate_values() rather than the module's
  ranged NOWK — see DALWPBBD.py Step 0 for the identical derivation.
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
    fdprod_format,
    fddenom_format,
    statecd_format,
    fdcustcd_format,
    ifdcuscd_format,
)

# ============================================================================
# PATH CONFIGURATION (each physical input kept independent)
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# INPUT_FD_DIR = BASE_DIR / "input" / "prod" / "deposit"    # fd_fd
# FD_PREFIX     = "ifdcd"

# INPUT_UMA_DIR = BASE_DIR / "input" / "prod" / "deposit"   # deposit_uma
# UMA_PREFIX     = "iuma"

INPUT_FD_DIR  = STG_DIR / "MNIFD" / "fd.sas7bdat"

INPUT_UMA_DIR = STG_DIR / "MNITB" / "uma.sas7bdat"

# Parquet cache directory for the .sas7bdat -> Parquet conversion step
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIWREXL"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Output cache directory — where BNM_FDWKLY/BNM_UMA are persisted for
# downstream programs (e.g. EIIWREXL.py) to read via read_parquet()
# OUTPUT_CACHE_DIR = BASE_DIR / "work" / "BNM"
OUTPUT_CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIWREXL"
OUTPUT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000

# ============================================================================
# STEP 0: REPORT DATE / WEEK NUMBER  (shared exact-match logic with
# DALWPBBD.py — see module docstring; no physical file)
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

REPTMON = f"{REPTDATE.month:02d}"

print(f"  REPTDATE : {REPTDATE}")
print(f"  REPTMON  : {REPTMON}")
print(f"  NOWK     : {NOWK}")

FDWKLY_CACHE = OUTPUT_CACHE_DIR / f"FDWKLY{REPTMON}{NOWK}.parquet"
UMA_CACHE    = OUTPUT_CACHE_DIR / f"UMA{REPTMON}{NOWK}.parquet"


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
# STEP 1: RESOLVE & CACHE INPUT FILES  (see module docstring items 1-2)
# ============================================================================
print("\nStep 1: Resolving and caching input files...")

# fd_path  = get_latest_file(INPUT_FD_DIR, prefix=FD_PREFIX)
# uma_path = get_latest_file(INPUT_UMA_DIR, prefix=UMA_PREFIX)

fd_path  = INPUT_FD_DIR
uma_path = INPUT_UMA_DIR

FD_SAS_CACHE  = CACHE_DIR / f"{fd_path.stem}.parquet"
UMA_SAS_CACHE = CACHE_DIR / f"{uma_path.stem}.parquet"

_load_cached(fd_path, FD_SAS_CACHE, "FD")
_load_cached(uma_path, UMA_SAS_CACHE, "UMA")


# ============================================================================
# STEP 2: BUILD BNM.FDWKLY  (input: item 1 — FD.FD)
# DATA BNM.FDWKLY(KEEP=BRANCH ACCTNO CUSTCODE NAME AMTIND ACCTTYPE OPENIND
#                      CURBAL BIC INTPAY STATE TERM INTPLAN MATDATE);
#   SET FD.FD;
#   BIC = PUT(INTPLAN, FDPROD.);   AMTIND = PUT(INTPLAN, FDDENOM.);
#   STATE = PUT(BRANCH, STATECD.);
#   IF BIC IN ('42130','42630') THEN CUSTCODE = PUT(CUSTCD, FDCUSTCD.);
#      ELSE CUSTCODE = PUT(CUSTCD, IFDCUSCD.);
#   IF BIC = '42630' THEN <adjust PURPOSE>;   -- PURPOSE is not in the KEEP
#      list, so this mutation has no effect on the final BNM.FDWKLY output;
#      replicated below only for behavioral fidelity per project convention
#      of preserving intentional SAS logic even when it is dead w.r.t. output.
#   IF ACCTTYPE IN (315,394) THEN BIC='42132'; ELSE
#   IF ACCTTYPE IN (397,398) THEN BIC='42199';
#   IF OPENIND = 'O' OR OPENIND = 'D' THEN OUTPUT;
# ============================================================================
print("\nStep 2: Building BNM_FDWKLY...")

con = duckdb.connect(database=":memory:")
_fd_raw = con.execute(f"""
    SELECT
        CAST(BRANCH   AS INTEGER) AS BRANCH,
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CUSTCD   AS INTEGER) AS CUSTCD,
        CAST(NAME     AS VARCHAR) AS NAME,
        CAST(ACCTTYPE AS INTEGER) AS ACCTTYPE,
        CAST(OPENIND  AS VARCHAR) AS OPENIND,
        CAST(CURBAL   AS DOUBLE)  AS CURBAL,
        CAST(INTPAY   AS DOUBLE)  AS INTPAY,
        CAST(TERM     AS INTEGER) AS TERM,
        CAST(INTPLAN  AS INTEGER) AS INTPLAN,
        DATE '1960-01-01' + CAST(MATDATE AS INTEGER) AS MATDATE,
        CAST(PURPOSE  AS VARCHAR) AS PURPOSE
    FROM read_parquet('{FD_SAS_CACHE.as_posix()}')
    WHERE OPENIND = 'O' OR OPENIND = 'D'
""").pl()
con.close()

_fd_raw = _fd_raw.with_columns([
    pl.col("INTPLAN").map_elements(fdprod_format, return_dtype=pl.Utf8).alias("BIC"),
    pl.col("INTPLAN").map_elements(fddenom_format, return_dtype=pl.Utf8).alias("AMTIND"),
    pl.col("BRANCH").map_elements(statecd_format, return_dtype=pl.Utf8).alias("STATE"),
])

_fd_raw = _fd_raw.with_columns(
    pl.when(pl.col("BIC").is_in(["42130", "42630"]))
      .then(pl.col("CUSTCD").map_elements(fdcustcd_format, return_dtype=pl.Utf8))
      .otherwise(pl.col("CUSTCD").map_elements(ifdcuscd_format, return_dtype=pl.Utf8))
      .alias("CUSTCODE")
)

# PURPOSE adjustment — dead code w.r.t. output (PURPOSE dropped by KEEP=),
# replicated only for behavioral fidelity; does not affect BNM_FDWKLY output.
_is_resident_purpose = pl.col("CUSTCODE").is_in(["77", "78", "95"])
_fd_raw = _fd_raw.with_columns(
    pl.when(pl.col("BIC") == "42630")
      .then(
          pl.when(_is_resident_purpose & ~pl.col("PURPOSE").is_in(["1", "2", "3"]))
            .then(pl.lit("1"))
            .when(~_is_resident_purpose & ~pl.col("PURPOSE").is_in(["4", "5"]))
            .then(pl.lit("4"))
            .otherwise(pl.col("PURPOSE"))
      )
      .otherwise(pl.col("PURPOSE"))
      .alias("PURPOSE")
)

_fd_raw = _fd_raw.with_columns(
    pl.when(pl.col("ACCTTYPE").is_in([315, 394])).then(pl.lit("42132"))
      .when(pl.col("ACCTTYPE").is_in([397, 398])).then(pl.lit("42199"))
      .otherwise(pl.col("BIC"))
      .alias("BIC")
)

BNM_FDWKLY = _fd_raw.select([
    "BRANCH", "ACCTNO", "CUSTCODE", "NAME", "AMTIND", "ACCTTYPE", "OPENIND",
    "CURBAL", "BIC", "INTPAY", "STATE", "TERM", "INTPLAN", "MATDATE",
])

del _fd_raw
gc.collect()
print(f"  BNM_FDWKLY rows: {len(BNM_FDWKLY):,}")


# ============================================================================
# STEP 3: BUILD BNM.UMA  (input: item 2 — DEPOSIT.UMA)
# DATA BNM.UMA(KEEP=BRANCH ACCTNO CUSTCODE NAME AMTIND OPENIND CURBAL BIC STATE);
#   SET DEPOSIT.UMA(RENAME=(CUSTCODE=CUSTCD));
#   IF PRODUCT = 297 THEN DO; CUSTCODE=PUT(CUSTCD,FDCUSTCD.); AMTIND='D'; END;
#   ELSE DO; CUSTCODE=PUT(CUSTCD,IFDCUSCD.); AMTIND='I'; END;
#   STATE = PUT(BRANCH, STATECD.);  BIC='42199';
#   IF OPENIND = 'O' OR OPENIND = 'D' THEN OUTPUT;
# ============================================================================
print("\nStep 3: Building BNM_UMA...")

con = duckdb.connect(database=":memory:")
_uma_raw = con.execute(f"""
    SELECT
        CAST(BRANCH   AS INTEGER) AS BRANCH,
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CUSTCODE AS INTEGER) AS CUSTCD,   -- RENAME=(CUSTCODE=CUSTCD)
        CAST(NAME     AS VARCHAR) AS NAME,
        CAST(PRODUCT  AS INTEGER) AS PRODUCT,
        CAST(OPENIND  AS VARCHAR) AS OPENIND,
        CAST(CURBAL   AS DOUBLE)  AS CURBAL
    FROM read_parquet('{UMA_SAS_CACHE.as_posix()}')
    WHERE OPENIND = 'O' OR OPENIND = 'D'
""").pl()
con.close()

_uma_raw = _uma_raw.with_columns([
    pl.when(pl.col("PRODUCT") == 297)
      .then(pl.col("CUSTCD").map_elements(fdcustcd_format, return_dtype=pl.Utf8))
      .otherwise(pl.col("CUSTCD").map_elements(ifdcuscd_format, return_dtype=pl.Utf8))
      .alias("CUSTCODE"),
    pl.when(pl.col("PRODUCT") == 297).then(pl.lit("D")).otherwise(pl.lit("I")).alias("AMTIND"),
    pl.col("BRANCH").map_elements(statecd_format, return_dtype=pl.Utf8).alias("STATE"),
    pl.lit("42199").alias("BIC"),
])

BNM_UMA = _uma_raw.select([
    "BRANCH", "ACCTNO", "CUSTCODE", "NAME", "AMTIND", "OPENIND", "CURBAL",
    "BIC", "STATE",
])

del _uma_raw
gc.collect()
print(f"  BNM_UMA rows: {len(BNM_UMA):,}")


# ============================================================================
# STEP 4: WRITE PARQUET CACHE  (temporary artefacts for this program's own
# and downstream programs' use — see module docstring "NON-FILE / DERIVED /
# TEMPORARY OUTPUTS")
# ============================================================================
BNM_FDWKLY.write_parquet(FDWKLY_CACHE)
BNM_UMA.write_parquet(UMA_CACHE)

print(f"\nFALWPBBD complete. Cached: {FDWKLY_CACHE.name}, {UMA_CACHE.name}")
