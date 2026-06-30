#!/usr/bin/env python3
"""
Program : EIBDUNDP.py
Purpose : Deposit Activities for Credit Card Holders.
          Identifies cardholders with significant deposit withdrawals
          and summarises their current balances across SA/CA/FD accounts.
"""

import gc
import os
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from pathlib import Path
from datetime import date, timedelta

from REPTDATE import get_reptdate_values
from input_date import get_latest_file

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# BASE_DIR = Path("/dwh")
BASE_DIR_XMIS   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR_XMIS  = BASE_DIR_XMIS / "input/prod"

# ── DEPO (current day: PBB) ──────────────────────────────────────────────────
# DEPO_CA_DIR  = BASE_DIR / "dpd_ca"  # dpd_ca
# DEPO_SA_DIR  = BASE_DIR / "dpd_sa"  # dpd_sa
# DEPO_FD_DIR  = BASE_DIR / "dpd_fd"  # dpd_fd
DEPO_CA_DIR  = INPUT_DIR_XMIS / "deposit"
DEPO_SA_DIR  = INPUT_DIR_XMIS / "deposit"
DEPO_FD_DIR  = INPUT_DIR_XMIS / "deposit"

# ── IDEPO (current day: PIBB) ────────────────────────────────────────────────
# IDEPO_CA_DIR = BASE_DIR / "idpd_ca"  # idpd_ca
# IDEPO_SA_DIR = BASE_DIR / "idpd_sa"  # idpd_sa
# IDEPO_FD_DIR = BASE_DIR / "idpd_fd"  # idpd_fd
IDEPO_CA_DIR = INPUT_DIR_XMIS / "deposit"
IDEPO_SA_DIR = INPUT_DIR_XMIS / "deposit"
IDEPO_FD_DIR = INPUT_DIR_XMIS / "deposit"

# ── PDEPO (previous day: PBB) ────────────────────────────────────────────────
# PDEPO_CA_DIR  = BASE_DIR / "dpd_ca"  # dpd_ca (prev date)
# PDEPO_SA_DIR  = BASE_DIR / "dpd_sa"  # dpd_sa (prev date)
# PDEPO_FD_DIR  = BASE_DIR / "dpd_fd"  # dpd_fd (prev date)
PDEPO_CA_DIR  = INPUT_DIR_XMIS / "deposit"
PDEPO_SA_DIR  = INPUT_DIR_XMIS / "deposit"
PDEPO_FD_DIR  = INPUT_DIR_XMIS / "deposit"

# ── PIDEPO (previous day: PIBB) ──────────────────────────────────────────────
# PIDEPO_CA_DIR = BASE_DIR / "idpd_ca"  # idpd_ca (prev date)
# PIDEPO_SA_DIR = BASE_DIR / "idpd_sa"  # idpd_sa (prev date)
# PIDEPO_FD_DIR = BASE_DIR / "idpd_fd"  # idpd_fd (prev date)
PIDEPO_CA_DIR = INPUT_DIR_XMIS / "deposit"
PIDEPO_SA_DIR = INPUT_DIR_XMIS / "deposit"
PIDEPO_FD_DIR = INPUT_DIR_XMIS / "deposit"

# ── CARD ─────────────────────────────────────────────────────────────────────
# CARD_DIR      = BASE_DIR / "rs_cis"  # unicardYYMMWK.sas7bdat
CARD_DIR      = BASE_DIR_XMIS / "input/prod/EIBDUNDP"

# ── CIS files (3 separate files) ─────────────────────────────────────────────
# CISDP_CA_FILE = BASE_DIR / "rs_cis"  # CISCA source
# CISDP_SA_FILE = BASE_DIR / "rs_cis"  # CISSA source
# CISDP_FD_FILE = BASE_DIR / "rs_cis"  # CISFD source
CISDP_CA_FILE = INPUT_DIR_XMIS / "cis"
CISDP_SA_FILE = INPUT_DIR_XMIS / "cis"
CISDP_FD_FILE = INPUT_DIR_XMIS / "cis"

# ── Parquet cache directory ───────────────────────────────────────────────────
CACHE_DIR     = INPUT_DIR_XMIS / "EIBDUNDP" / "cache"

# ── Output ────────────────────────────────────────────────────────────────────
OUTPUT_DIR    = BASE_DIR_XMIS / "output" / "EIBDUNDP"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# CHUNK / ROW LIMIT CONFIGURATION  (mirrors EIBDLN1M pattern)
# ============================================================================
CHUNK_ROWS = 500_000
ROW_LIMIT  = int(os.environ.get("ROW_LIMIT", 0))   # 0 = no limit; set for test mode

# ============================================================================
# STEP 1: REPORT DATE DERIVATION
# SAS: DATA REPTDATE; SET DEPO.REPTDATE; ...
# This program uses a non-standard week boundary mapping and derives
# RDTEA (today-1 formatted) and RDTEB (today-2 formatted).
# Base reptdate comes from REPTDATE.py (today - 1), then the date is snapped
# to the nearest period boundary per the SAS SELECT block.
# ============================================================================
print("Step 1: Deriving report date...")

_rv        = get_reptdate_values()
_base_date = _rv.reptdate          # today - 1

_rdate  = _base_date               # RDATE  = REPTDATE  (before adjustment)
_rdate1 = _rdate - timedelta(days=1)   # RDATE1 = RDATE - 1

_day = _rdate.day
_mon = _rdate.month
_yr  = _rdate.year

if 2 <= _day <= 9:
    # Last day of previous month
    REPTDATE = date(_yr, _mon, 1) - timedelta(days=1)
    WK = "04"
elif 10 <= _day <= 16:
    REPTDATE = date(_yr, _mon, 8)
    WK = "01"
elif 17 <= _day <= 23:
    REPTDATE = date(_yr, _mon, 15)
    WK = "02"
else:
    REPTDATE = date(_yr, _mon, 1) - timedelta(days=1) if _day == 1 else date(_yr, _mon, 22)
    WK = "03"

REPTDAY  = f"{REPTDATE.day:02d}"
REPTMON  = f"{REPTDATE.month:02d}"
# SAS YEAR2. with YEARCUTOFF=1990: two-digit year
REPTYEAR = f"{REPTDATE.year % 100:02d}"
NOWK     = WK

# RDTEA = PUT(RDATE,  DDMMYY8.)   e.g. "28/06/26"
# RDTEB = PUT(RDATE1, DDMMYY8.)
RDTEA = _rdate.strftime("%d/%m/%y")
RDTEB = _rdate1.strftime("%d/%m/%y")

OUTPUT_FILE = OUTPUT_DIR / "CARD_DPACTV.txt"

print(f"  REPTDATE : {REPTDATE}  (WK={WK})")
print(f"  RDTEA    : {RDTEA}   RDTEB : {RDTEB}")
print(f"  REPTYEAR={REPTYEAR}  REPTMON={REPTMON}  REPTDAY={REPTDAY}  NOWK={NOWK}")
print(f"  Output   : {OUTPUT_FILE}")

# ============================================================================
# HELPER: CACHE FRESHNESS CHECK  (mirrors EIBDLN1M)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )

# ============================================================================
# HELPER: STREAM .sas7bdat → PARQUET  (memory-efficient chunked conversion)
# ============================================================================
def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a large .sas7bdat to Parquet in streaming chunks."""
    print(f"  [{tag}] Converting {sas_path.name} → {cache_path.name} ...")
    writer    = None
    schema    = None
    total     = 0
    rows_read = 0

    reader = pd.read_sas(str(sas_path), encoding="latin1", chunksize=CHUNK_ROWS)
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
            for i, field in enumerate(schema):
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
# STEP 2: RESOLVE INPUT FILE PATHS
# All 16 input files resolved here — PDEPO/PIDEPO are the files dated one
# calendar day before the latest DEPO/IDEPO files.
# ============================================================================
print("\nStep 2: Resolving all 16 input file paths...")

import re as _re

def _prev_day_path(latest_path: Path, prefix: str, search_dir: Path) -> Path:
    """
    Given the latest deposit file, find the file from the previous calendar day.
    Filename date suffix format: YYMMDD  (e.g. dpd_ca260628.sas7bdat → 26/06/28)
    YEARCUTOFF=1990: yy >= 90 → 19xx, else 20xx.
    """
    m = _re.search(r"(\d{6})(?:\.\w+)?$", latest_path.stem)
    if not m:
        raise ValueError(f"Cannot parse YYMMDD suffix from: {latest_path.name}")
    yy, mm, dd = int(m.group(1)[0:2]), int(m.group(1)[2:4]), int(m.group(1)[4:6])
    base_year  = (1900 + yy) if yy >= 90 else (2000 + yy)
    cur_date   = date(base_year, mm, dd)
    prev_date  = cur_date - timedelta(days=1)
    prev_stem  = f"{prefix}{prev_date.strftime('%y%m%d')}"
    candidates = sorted(search_dir.glob(f"{prev_stem}*.sas7bdat"))
    if not candidates:
        raise FileNotFoundError(
            f"Previous-day file not found in {search_dir}  (expected prefix '{prev_stem}')"
        )
    return candidates[0]

# ── Current-day DEPO (PBB) ────────────────────────────────────────────────────
depo_ca_path  = get_latest_file(DEPO_CA_DIR,  prefix="ca")
depo_sa_path  = get_latest_file(DEPO_SA_DIR,  prefix="sa")
depo_fd_path  = get_latest_file(DEPO_FD_DIR,  prefix="fd")

# ── Current-day IDEPO (PIBB) ──────────────────────────────────────────────────
idepo_ca_path = get_latest_file(IDEPO_CA_DIR, prefix="ica")
idepo_sa_path = get_latest_file(IDEPO_SA_DIR, prefix="isa")
idepo_fd_path = get_latest_file(IDEPO_FD_DIR, prefix="ifd")

# ── Previous-day PDEPO (PBB) ─────────────────────────────────────────────────
pdepo_ca_path  = _prev_day_path(depo_ca_path,  "ca",  PDEPO_CA_DIR)
pdepo_sa_path  = _prev_day_path(depo_sa_path,  "sa",  PDEPO_SA_DIR)
pdepo_fd_path  = _prev_day_path(depo_fd_path,  "fd",  PDEPO_FD_DIR)

# ── Previous-day PIDEPO (PIBB) ───────────────────────────────────────────────
pidepo_ca_path = _prev_day_path(idepo_ca_path, "ica", PIDEPO_CA_DIR)
pidepo_sa_path = _prev_day_path(idepo_sa_path, "isa", PIDEPO_SA_DIR)
pidepo_fd_path = _prev_day_path(idepo_fd_path, "ifd", PIDEPO_FD_DIR)

# ── CARD ─────────────────────────────────────────────────────────────────────
card_prefix = f"unicard{REPTYEAR}{REPTMON}{NOWK}"
card_path   = get_latest_file(CARD_DIR, prefix=card_prefix)

# ── CIS (3 separate fixed files) ─────────────────────────────────────────────
# cisca_path = CISDP_CA_FILE   # source for CISCA  (CA accounts)
# cissa_path = CISDP_SA_FILE   # source for CISSA  (SA accounts)
# cisfd_path = CISDP_FD_FILE   # source for CISFD  (FD accounts)
cisca_path = get_latest_file(CISDP_CA_FILE, "cisr1ca")   # source for CISCA  (CA accounts)
cissa_path = get_latest_file(CISDP_SA_FILE, "cisr1sa")   # source for CISSA  (SA accounts)
cisfd_path = get_latest_file(CISDP_FD_FILE, "cisr1fd")   # source for CISFD  (FD accounts)

print(f"  DEPO   CA : {depo_ca_path.name}")
print(f"  DEPO   SA : {depo_sa_path.name}")
print(f"  DEPO   FD : {depo_fd_path.name}")
print(f"  IDEPO  CA : {idepo_ca_path.name}")
print(f"  IDEPO  SA : {idepo_sa_path.name}")
print(f"  IDEPO  FD : {idepo_fd_path.name}")
print(f"  PDEPO  CA : {pdepo_ca_path.name}")
print(f"  PDEPO  SA : {pdepo_sa_path.name}")
print(f"  PDEPO  FD : {pdepo_fd_path.name}")
print(f"  PIDEPO CA : {pidepo_ca_path.name}")
print(f"  PIDEPO SA : {pidepo_sa_path.name}")
print(f"  PIDEPO FD : {pidepo_fd_path.name}")
print(f"  CARD      : {card_path.name}")
print(f"  CIS CA    : {cisca_path.name}")
print(f"  CIS SA    : {cissa_path.name}")
print(f"  CIS FD    : {cisfd_path.name}")

# ============================================================================
# STEP 3: CACHE ALL 16 FILES TO PARQUET  (skip if already fresh)
# ============================================================================
print("\nStep 3: Caching SAS files to Parquet (if needed)...")

DEPO_CA_CACHE   = CACHE_DIR / f"{depo_ca_path.stem}.parquet"
DEPO_SA_CACHE   = CACHE_DIR / f"{depo_sa_path.stem}.parquet"
DEPO_FD_CACHE   = CACHE_DIR / f"{depo_fd_path.stem}.parquet"

IDEPO_CA_CACHE  = CACHE_DIR / f"{idepo_ca_path.stem}.parquet"
IDEPO_SA_CACHE  = CACHE_DIR / f"{idepo_sa_path.stem}.parquet"
IDEPO_FD_CACHE  = CACHE_DIR / f"{idepo_fd_path.stem}.parquet"

PDEPO_CA_CACHE  = CACHE_DIR / f"{pdepo_ca_path.stem}.parquet"
PDEPO_SA_CACHE  = CACHE_DIR / f"{pdepo_sa_path.stem}.parquet"
PDEPO_FD_CACHE  = CACHE_DIR / f"{pdepo_fd_path.stem}.parquet"

PIDEPO_CA_CACHE = CACHE_DIR / f"{pidepo_ca_path.stem}.parquet"
PIDEPO_SA_CACHE = CACHE_DIR / f"{pidepo_sa_path.stem}.parquet"
PIDEPO_FD_CACHE = CACHE_DIR / f"{pidepo_fd_path.stem}.parquet"

CARD_CACHE      = CACHE_DIR / f"{card_path.stem}.parquet"
CISCA_CACHE     = CACHE_DIR / f"{cisca_path.stem}.parquet"
CISSA_CACHE     = CACHE_DIR / f"{cissa_path.stem}.parquet"
CISFD_CACHE     = CACHE_DIR / f"{cisfd_path.stem}.parquet"

_cache_jobs = [
    (depo_ca_path,   DEPO_CA_CACHE,   "DEPO_CA"),
    (depo_sa_path,   DEPO_SA_CACHE,   "DEPO_SA"),
    (depo_fd_path,   DEPO_FD_CACHE,   "DEPO_FD"),
    (idepo_ca_path,  IDEPO_CA_CACHE,  "IDEPO_CA"),
    (idepo_sa_path,  IDEPO_SA_CACHE,  "IDEPO_SA"),
    (idepo_fd_path,  IDEPO_FD_CACHE,  "IDEPO_FD"),
    (pdepo_ca_path,  PDEPO_CA_CACHE,  "PDEPO_CA"),
    (pdepo_sa_path,  PDEPO_SA_CACHE,  "PDEPO_SA"),
    (pdepo_fd_path,  PDEPO_FD_CACHE,  "PDEPO_FD"),
    (pidepo_ca_path, PIDEPO_CA_CACHE, "PIDEPO_CA"),
    (pidepo_sa_path, PIDEPO_SA_CACHE, "PIDEPO_SA"),
    (pidepo_fd_path, PIDEPO_FD_CACHE, "PIDEPO_FD"),
    (card_path,      CARD_CACHE,      "CARD"),
    (cisca_path,     CISCA_CACHE,     "CISCA"),
    (cissa_path,     CISSA_CACHE,     "CISSA"),
    (cisfd_path,     CISFD_CACHE,     "CISFD"),
]

for sas_p, cache_p, tag in _cache_jobs:
    if not _cache_is_fresh(sas_p, cache_p):
        sas_to_parquet(sas_p, cache_p, tag)
    else:
        print(f"  [{tag}] Cache fresh — skipping conversion.")

# ============================================================================
# STEP 4: READ CARD  (small enough to hold in memory after Parquet load)
# DATA CARD(KEEP=CARDNO MONITOR SOURCE CLOSECD NEWIC OLDIC CUSTNAME APPRLIMT)
#      CARD1(KEEP=NEWIC);
#   SET CARD.UNICARD&REPTYEAR&REPTMON&NOWK;
#   IF CLOSECD NE ' '          → DELETE
#   IF ACCTYPE IN ('IS')       → DELETE
#   IF ACCTYPE='IA' & CARDHOLD NE 1 → DELETE
#   IF NEWIC=' ' THEN NEWIC=OLDIC
#   CARD  : MONITOR IN ('Z','I') OR SOURCE='GCPIFD0209'
#   CARD1 : MONITOR IN ('Z')
# ============================================================================
print("\nStep 4: Processing CARD file...")

con = duckdb.connect(database=":memory:")

card_df = con.execute(f"""
    SELECT
        TRIM(CAST(CARDNO   AS VARCHAR)) AS CARDNO,
        TRIM(CAST(MONITOR  AS VARCHAR)) AS MONITOR,
        TRIM(CAST(SOURCE   AS VARCHAR)) AS SOURCE,
        COALESCE(TRIM(CAST(CLOSECD AS VARCHAR)), '') AS CLOSECD,
        COALESCE(TRIM(CAST(ACCTYPE AS VARCHAR)), '') AS ACCTYPE,
        COALESCE(CAST(CARDHOLD AS DOUBLE), 0)        AS CARDHOLD,
        TRIM(CAST(NEWIC    AS VARCHAR)) AS NEWIC,
        TRIM(CAST(OLDIC    AS VARCHAR)) AS OLDIC,
        TRIM(CAST(CUSTNAME AS VARCHAR)) AS CUSTNAME,
        CAST(APPRLIMT AS DOUBLE)        AS APPRLIMT
    FROM read_parquet('{CARD_CACHE}')
    WHERE COALESCE(TRIM(CAST(CLOSECD AS VARCHAR)), '') = ''
      AND COALESCE(TRIM(CAST(ACCTYPE AS VARCHAR)), '') <> 'IS'
      AND NOT (COALESCE(TRIM(CAST(ACCTYPE AS VARCHAR)), '') = 'IA'
               AND COALESCE(CAST(CARDHOLD AS DOUBLE), 0) <> 1)
""").pl()

con.close()
gc.collect()

print(f"  CARD raw rows after SQL filer: {len(card_df):,}")

print(card_df.select(["MONITOR", "SOURCE"]).group_by("MONITOR").len().sort("len", descending=True))
print(card_df.filter(pl.col("SOURCE") == "GCPIFD0209").height)

# IF NEWIC=' ' THEN NEWIC=OLDIC
card_df = card_df.with_columns(
    pl.when(pl.col("NEWIC").is_null() | (pl.col("NEWIC") == ""))
    .then(pl.col("OLDIC"))
    .otherwise(pl.col("NEWIC"))
    .alias("NEWIC")
)

# CARD  : MONITOR IN ('Z','I') OR SOURCE='GCPIFD0209'
card_keep_cols = ["CARDNO", "MONITOR", "SOURCE", "CLOSECD",
                  "NEWIC", "OLDIC", "CUSTNAME", "APPRLIMT"]
card_main = card_df.filter(
    pl.col("MONITOR").is_in(["Z", "I"]) | (pl.col("SOURCE") == "GCPIFD0209")
).select([c for c in card_keep_cols if c in card_df.columns])

# CARD1 : MONITOR IN ('Z')
card1 = card_df.filter(
    pl.col("MONITOR").is_in(["Z"])
).select(["NEWIC"])

del card_df
gc.collect()

# PROC SORT DATA=CARD NODUPKEY; BY NEWIC;
card_main = card_main.sort("NEWIC").unique(subset=["NEWIC"], keep="first")
# PROC SORT DATA=CARD1 NODUPKEY; BY NEWIC;
card1     = card1.sort("NEWIC").unique(subset=["NEWIC"], keep="first")

print(f"  CARD  rows : {len(card_main):,}")
print(f"  CARD1 rows : {len(card1):,}")

# ============================================================================
# STEP 5: BUILD CISCA / CISSA / CISFD
# PROC SORT DATA=CISDP.DEPOSIT  OUT=CISCA (KEEP=ACCTNO NEWIC); BY NEWIC;
# PROC SORT DATA=CISSAFD.DEPOSIT OUT=CISSAFD(KEEP=ACCTNO NEWIC); BY NEWIC;
#
# Each CIS file is now a separate source:
#   CISCA  ← cisr1ca file  (CA accounts)
#   CISSA  ← cisr1sa file  (SA accounts)
#   CISFD  ← cisr1fd file  (FD accounts)
#
# DATA CISCA;  MERGE CARD(IN=A) CISCA(IN=B);  BY NEWIC; IF A AND B; TYPE='CA';
# DATA CISSA;  MERGE CARD(IN=A) CISSA(IN=B);  BY NEWIC; IF A AND B; TYPE='SA';
# DATA CISFD;  MERGE CARD(IN=A) CISFD(IN=B);  BY NEWIC; IF A AND B; TYPE='FD';
# ============================================================================
print("\nStep 5: Building CISCA / CISSA / CISFD via DuckDB joins...")

# Register CARD as an in-memory Arrow table so DuckDB can join against it
# without touching disk.
con = duckdb.connect(database=":memory:")
con.register("card_tbl", card_main.to_arrow())

def _build_cis(cache: Path, tag: str) -> pl.DataFrame:
    """Inner-join CARD ∩ CIS on NEWIC, keeping ACCTNO + NEWIC from CIS side."""
    return con.execute(f"""
        SELECT
            CAST(c.ACCTNO AS BIGINT)             AS ACCTNO,
            TRIM(CAST(c.NEWIC AS VARCHAR))        AS NEWIC
        FROM read_parquet('{cache}') c
        INNER JOIN card_tbl k
            ON TRIM(CAST(c.NEWIC AS VARCHAR)) = k.NEWIC
    """).pl()

cisca_df = _build_cis(CISCA_CACHE, "CISCA")
cissa_df = _build_cis(CISSA_CACHE, "CISSA")
cisfd_df = _build_cis(CISFD_CACHE, "CISFD")

con.close()
gc.collect()

# PROC SORT DATA=CISCA; BY ACCTNO;
# PROC SORT DATA=CISFD; BY ACCTNO;
# PROC SORT DATA=CISSA; BY ACCTNO;
cisca_df = cisca_df.sort("ACCTNO")
cissa_df = cissa_df.sort("ACCTNO")
cisfd_df = cisfd_df.sort("ACCTNO")

print(f"  CISCA rows : {len(cisca_df):,}")
print(f"  CISSA rows : {len(cissa_df):,}")
print(f"  CISFD rows : {len(cisfd_df):,}")

# ============================================================================
# STEP 6: BUILD PSA / PCA / PFD  (previous-period balances)
# PROC SORT DATA=PDEPO.SAVING  OUT=PSA (KEEP=ACCTNO CURBAL RENAME=(CURBAL=PRE_CURBAL));
# PROC SORT DATA=PIDEPO.SAVING OUT=PISA(KEEP=ACCTNO CURBAL RENAME=(CURBAL=PRE_CURBAL));
# DATA PSA; SET PSA PISA;
# MERGE CISSA(IN=A) PSA(IN=B); BY ACCTNO; IF A;
# (same pattern for PCA / PFD)
# ============================================================================
print("\nStep 6: Building PSA / PCA / PFD (previous-period balances)...")

con = duckdb.connect(database=":memory:")
con.register("cisca_tbl", cisca_df.to_arrow())
con.register("cissa_tbl", cissa_df.to_arrow())
con.register("cisfd_tbl", cisfd_df.to_arrow())

def _prev_balance(cis_tbl: str, p_cache: Path, pi_cache: Path) -> pl.DataFrame:
    """
    Combine PBB + PIBB previous-day files, then left-join from the CIS side
    (IF A semantics: keep all CIS rows; PRE_CURBAL is null when no prior record).
    """
    return con.execute(f"""
        WITH prev AS (
            SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO,
                   CAST(CURBAL  AS DOUBLE) AS PRE_CURBAL
            FROM read_parquet('{p_cache}')
            UNION ALL
            SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO,
                   CAST(CURBAL  AS DOUBLE) AS PRE_CURBAL
            FROM read_parquet('{pi_cache}')
        )
        SELECT c.ACCTNO, c.NEWIC, COALESCE(p.PRE_CURBAL, 0.0) AS PRE_CURBAL
        FROM {cis_tbl} c
        LEFT JOIN prev p ON c.ACCTNO = p.ACCTNO
    """).pl()

psa_df = _prev_balance("cissa_tbl", PDEPO_SA_CACHE,  PIDEPO_SA_CACHE)
pca_df = _prev_balance("cisca_tbl", PDEPO_CA_CACHE,  PIDEPO_CA_CACHE)
pfd_df = _prev_balance("cisfd_tbl", PDEPO_FD_CACHE,  PIDEPO_FD_CACHE)

con.close()
gc.collect()

print(f"  PSA rows : {len(psa_df):,}")
print(f"  PCA rows : {len(pca_df):,}")
print(f"  PFD rows : {len(pfd_df):,}")

# ============================================================================
# STEP 7: BUILD SA / CA / FD  (current-period balances)
# PROC SORT DATA=DEPO.SAVING  OUT=SA(KEEP=ACCTNO CURBAL);
# PROC SORT DATA=IDEPO.SAVING OUT=ISA(KEEP=ACCTNO CURBAL);
# DATA SA; SET SA ISA;
# MERGE CISSA(IN=A) SA(IN=B); BY ACCTNO; IF A;
# (same pattern for CA / FD)
# ============================================================================
print("\nStep 7: Building SA / CA / FD (current-period balances)...")

con = duckdb.connect(database=":memory:")
con.register("cisca_tbl", cisca_df.to_arrow())
con.register("cissa_tbl", cissa_df.to_arrow())
con.register("cisfd_tbl", cisfd_df.to_arrow())

def _curr_balance(cis_tbl: str, d_cache: Path, id_cache: Path) -> pl.DataFrame:
    """
    Combine PBB + PIBB current-day files, then left-join from the CIS side
    (IF A semantics: keep all CIS rows; CURBAL is null when no current record).
    """
    return con.execute(f"""
        WITH curr AS (
            SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO,
                   CAST(CURBAL  AS DOUBLE) AS CURBAL
            FROM read_parquet('{d_cache}')
            UNION ALL
            SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO,
                   CAST(CURBAL  AS DOUBLE) AS CURBAL
            FROM read_parquet('{id_cache}')
        )
        SELECT c.ACCTNO, c.NEWIC, COALESCE(curr.CURBAL, 0.0) AS CURBAL
        FROM {cis_tbl} c
        LEFT JOIN curr ON c.ACCTNO = curr.ACCTNO
    """).pl()

sa_df = _curr_balance("cissa_tbl", DEPO_SA_CACHE,  IDEPO_SA_CACHE)
ca_df = _curr_balance("cisca_tbl", DEPO_CA_CACHE,  IDEPO_CA_CACHE)
fd_df = _curr_balance("cisfd_tbl", DEPO_FD_CACHE,  IDEPO_FD_CACHE)

con.close()
del cisca_df, cissa_df, cisfd_df
gc.collect()

print(f"  SA rows : {len(sa_df):,}")
print(f"  CA rows : {len(ca_df):,}")
print(f"  FD rows : {len(fd_df):,}")

# ============================================================================
# STEP 8: COMBINE DEPO / PDEPO AND CALCULATE WITHDRAWALS
# DATA DEPO;  SET SA  CA  FD;
# DATA PDEPO; SET PSA PCA PFD;
# PROC SORT DATA=DEPO;  BY ACCTNO;
# PROC SORT DATA=PDEPO; BY ACCTNO;
# DATA DEPO;
#   MERGE DEPO(IN=A) PDEPO(IN=B); BY ACCTNO; IF A;
#   WITHDR = PRE_CURBAL - CURBAL;
#   IF WITHDR < 0 THEN WITHDR = 0;
# ============================================================================
print("\nStep 8: Combining DEPO / PDEPO and calculating withdrawals...")

con = duckdb.connect(database=":memory:")
con.register("sa_tbl",  sa_df.to_arrow())
con.register("ca_tbl",  ca_df.to_arrow())
con.register("fd_tbl",  fd_df.to_arrow())
con.register("psa_tbl", psa_df.to_arrow())
con.register("pca_tbl", pca_df.to_arrow())
con.register("pfd_tbl", pfd_df.to_arrow())

depo_df = con.execute("""
    WITH depo AS (
        SELECT ACCTNO, NEWIC, CURBAL     FROM sa_tbl
        UNION ALL
        SELECT ACCTNO, NEWIC, CURBAL     FROM ca_tbl
        UNION ALL
        SELECT ACCTNO, NEWIC, CURBAL     FROM fd_tbl
    ),
    pdepo AS (
        SELECT ACCTNO, NEWIC, PRE_CURBAL FROM psa_tbl
        UNION ALL
        SELECT ACCTNO, NEWIC, PRE_CURBAL FROM pca_tbl
        UNION ALL
        SELECT ACCTNO, NEWIC, PRE_CURBAL FROM pfd_tbl
    ),
    merged AS (
        SELECT
            d.ACCTNO,
            d.NEWIC,
            d.CURBAL,
            COALESCE(p.PRE_CURBAL, 0.0)  AS PRE_CURBAL
        FROM depo d
        LEFT JOIN pdepo p ON d.ACCTNO = p.ACCTNO
    )
    SELECT
        ACCTNO,
        NEWIC,
        CURBAL,
        PRE_CURBAL,
        CASE WHEN PRE_CURBAL - CURBAL < 0 THEN 0.0
             ELSE PRE_CURBAL - CURBAL
        END AS WITHDR
    FROM merged
""").pl()

con.close()
del sa_df, ca_df, fd_df, psa_df, pca_df, pfd_df
gc.collect()

print(f"  DEPO rows : {len(depo_df):,}")

# ============================================================================
# STEP 9: PROC SUMMARY — summarise by NEWIC
# PROC SUMMARY DATA=DEPO NWAY;
# CLASS NEWIC; VAR PRE_CURBAL CURBAL WITHDR;
# OUTPUT OUT=DEPO1 SUM=;
# ============================================================================
print("\nStep 9: Summarising DEPO by NEWIC...")

con = duckdb.connect(database=":memory:")
con.register("depo_tbl", depo_df.to_arrow())

depo1_df = con.execute("""
    SELECT
        NEWIC,
        SUM(PRE_CURBAL) AS PRE_CURBAL,
        SUM(CURBAL)     AS CURBAL,
        SUM(WITHDR)     AS WITHDR
    FROM depo_tbl
    GROUP BY NEWIC
""").pl()

con.close()
del depo_df
gc.collect()

print(f"  DEPO1 rows : {len(depo1_df):,}")

# ============================================================================
# STEP 10: SPLIT INTO DEPO2 / DEPO3
# PERCEN = (WITHDR / PRE_CURBAL) * 100
# DEPO2 : PERCEN >= 50
# DEPO3 : PERCEN <  50 AND CURBAL <= 500000
# ============================================================================
print("\nStep 10: Splitting into DEPO2 / DEPO3...")

depo1_df = depo1_df.with_columns(
    pl.when(pl.col("PRE_CURBAL") != 0)
    .then((pl.col("WITHDR") / pl.col("PRE_CURBAL")) * 100)
    .otherwise(pl.lit(0.0))
    .alias("PERCEN")
)

# PROC SORT DATA=DEPO2(KEEP=NEWIC CURBAL RENAME=(CURBAL=SUMBAL)); BY NEWIC;
depo2_df = (
    depo1_df
    .filter(pl.col("PERCEN") >= 50)
    .select([pl.col("NEWIC"), pl.col("CURBAL").alias("SUMBAL")])
    .sort("NEWIC")
)

# PROC SORT DATA=DEPO3(KEEP=NEWIC CURBAL RENAME=(CURBAL=SUMBAL)); BY NEWIC;
depo3_df = (
    depo1_df
    .filter((pl.col("PERCEN") < 50) & (pl.col("CURBAL") <= 500_000))
    .select([pl.col("NEWIC"), pl.col("CURBAL").alias("SUMBAL")])
    .sort("NEWIC")
)

del depo1_df
gc.collect()

print(f"  DEPO2 rows : {len(depo2_df):,}")
print(f"  DEPO3 rows : {len(depo3_df):,}")

# ============================================================================
# STEP 11: BUILD TOT
# DATA DEPO3A; MERGE DEPO3(IN=A) CARD1(IN=B); BY NEWIC; IF A AND B;
# DATA TOT;    SET DEPO2 DEPO3A;
# PROC SORT DATA=TOT NODUPKEY; BY NEWIC;
# ============================================================================
print("\nStep 11: Building TOT...")

# Inner join DEPO3 ∩ CARD1 on NEWIC
depo3a_df = depo3_df.join(card1, on="NEWIC", how="inner")

tot_df = pl.concat([depo2_df, depo3a_df], how="diagonal")
tot_df = tot_df.sort("NEWIC").unique(subset=["NEWIC"], keep="first")

del depo2_df, depo3_df, depo3a_df, card1
gc.collect()

print(f"  TOT rows : {len(tot_df):,}")

# ============================================================================
# STEP 12: BUILD FINAL
# Account-level DEPO detail is rebuilt from Parquet caches via DuckDB to
# avoid re-reading .sas7bdat files a second time.
# DATA DEPO; SET SA CA FD; (account-level, before PROC SUMMARY)
# PROC SORT DATA=DEPO;  BY NEWIC;
# PROC SORT DATA=TOT NODUPKEY; BY NEWIC;
# DATA FINAL; MERGE DEPO(IN=A) TOT(IN=B); BY NEWIC; IF B;
# PROC SORT DATA=FINAL; BY CUSTNAME ACCTNO;
# Card attributes (CUSTNAME, CARDNO, APPRLIMT, MONITOR, OLDIC) are
# attached via a join with card_main.
# ============================================================================
print("\nStep 12: Building FINAL dataset...")

con = duckdb.connect(database=":memory:")
con.register("card_tbl", card_main.to_arrow())
con.register("tot_tbl",  tot_df.to_arrow())

final_df = con.execute(f"""
    WITH depo_acct AS (
        -- SA accounts
        SELECT
            CAST(cis.ACCTNO AS BIGINT)               AS ACCTNO,
            TRIM(CAST(cis.NEWIC AS VARCHAR))          AS NEWIC,
            COALESCE(CAST(cur.CURBAL AS DOUBLE), 0.0) AS CURBAL,
            'SA' AS TYPE
        FROM read_parquet('{CISSA_CACHE}') cis
        LEFT JOIN (
            SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(CURBAL AS DOUBLE) AS CURBAL
            FROM read_parquet('{DEPO_SA_CACHE}')
            UNION ALL
            SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(CURBAL AS DOUBLE) AS CURBAL
            FROM read_parquet('{IDEPO_SA_CACHE}')
        ) cur ON CAST(cis.ACCTNO AS BIGINT) = cur.ACCTNO

        UNION ALL

        -- CA accounts
        SELECT
            CAST(cis.ACCTNO AS BIGINT)               AS ACCTNO,
            TRIM(CAST(cis.NEWIC AS VARCHAR))          AS NEWIC,
            COALESCE(CAST(cur.CURBAL AS DOUBLE), 0.0) AS CURBAL,
            'CA' AS TYPE
        FROM read_parquet('{CISCA_CACHE}') cis
        LEFT JOIN (
            SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(CURBAL AS DOUBLE) AS CURBAL
            FROM read_parquet('{DEPO_CA_CACHE}')
            UNION ALL
            SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(CURBAL AS DOUBLE) AS CURBAL
            FROM read_parquet('{IDEPO_CA_CACHE}')
        ) cur ON CAST(cis.ACCTNO AS BIGINT) = cur.ACCTNO

        UNION ALL

        -- FD accounts
        SELECT
            CAST(cis.ACCTNO AS BIGINT)               AS ACCTNO,
            TRIM(CAST(cis.NEWIC AS VARCHAR))          AS NEWIC,
            COALESCE(CAST(cur.CURBAL AS DOUBLE), 0.0) AS CURBAL,
            'FD' AS TYPE
        FROM read_parquet('{CISFD_CACHE}') cis
        LEFT JOIN (
            SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(CURBAL AS DOUBLE) AS CURBAL
            FROM read_parquet('{DEPO_FD_CACHE}')
            UNION ALL
            SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(CURBAL AS DOUBLE) AS CURBAL
            FROM read_parquet('{IDEPO_FD_CACHE}')
        ) cur ON CAST(cis.ACCTNO AS BIGINT) = cur.ACCTNO
    ),
    -- MERGE DEPO(IN=A) TOT(IN=B); BY NEWIC; IF B → keep only TOT-matching NEWICs
    filtered AS (
        SELECT d.ACCTNO, d.NEWIC, d.CURBAL, d.TYPE
        FROM depo_acct d
        INNER JOIN tot_tbl t ON d.NEWIC = t.NEWIC
    )
    -- Attach card attributes (CUSTNAME, CARDNO, APPRLIMT, MONITOR, OLDIC)
    SELECT
        f.ACCTNO,
        f.NEWIC,
        f.CURBAL,
        f.TYPE,
        k.CUSTNAME,
        k.CARDNO,
        COALESCE(k.APPRLIMT, 0.0) AS APPRLIMT,
        k.MONITOR,
        k.OLDIC
    FROM filtered f
    LEFT JOIN card_tbl k ON f.NEWIC = k.NEWIC
    ORDER BY k.CUSTNAME, f.ACCTNO
""").pl()

con.close()
del card_main, tot_df
gc.collect()

print(f"  FINAL rows : {len(final_df):,}")
print(final_df.head(10))

# ============================================================================
# STEP 13: PROC REPORT  (ASA carriage control, LRECL=133, RECFM=FB)
# TITLE1 'P U B L I C   B A N K   B E R H A D'
# TITLE2 'REPORT PERIOD :' "&RDTEB" ' - ' "&RDTEA"
# TITLE3 'CARDHOLDERS DEPOSITS ACCOUNT'
# COLUMN CUSTNAME NEWIC OLDIC CARDNO APPRLIMT MONITOR TYPE ACCTNO CURBAL
# DEFINE CUSTNAME / ORDER  FORMAT=$27.
# DEFINE APPRLIMT / DISPLAY 'CREDIT CARD LIMIT'
# DEFINE MONITOR  / DISPLAY 'CO*DE'   (header split on *)
# DEFINE TYPE     / DISPLAY 'TY*PE'
# DEFINE ACCTNO   / DISPLAY CENTER FORMAT=13.
# DEFINE CURBAL   / ANALYSIS SUM FORMAT=COMMA15.2 'BALANCE'
# BREAK AFTER CUSTNAME / OL SUMMARIZE SUPPRESS SKIP
# COMPUTE AFTER; LINE ' '; ENDCOMP;
# PAGE_SIZE = 60 lines (LRECL=133)
# ============================================================================
print("\nStep 13: Generating report...")

PAGE_SIZE    = 60
# Header block: 3 title lines + 1 blank + 2 column header lines + 1 separator
HEADER_LINES = 7

TITLE1 = "P U B L I C   B A N K   B E R H A D"
TITLE2 = f"REPORT PERIOD : {RDTEB} - {RDTEA}"
TITLE3 = "CARDHOLDERS DEPOSITS ACCOUNT"

# Column header line 1  (SAS DEFINE 'CO*DE' / 'TY*PE' — split on *)
COL_HDR1 = (
    f"{'CUSTOMER NAME':<27s}  "
    f"{'NEW ICNO':<15s}  "
    f"{'OLD ICNO':<15s}  "
    f"{'CARD NUMBER':<16s}  "
    f"{'CREDIT CARD LIMIT':>17s}  "
    f"{'CO':>2s}  "
    f"{'TY':>2s}  "
    f"{'ACCTNO':>13s}  "
    f"{'BALANCE':>15s}"
)
# Column header line 2  (second row of split headers)
COL_HDR2 = (
    f"{'':27s}  "
    f"{'':15s}  "
    f"{'':15s}  "
    f"{'':16s}  "
    f"{'':17s}  "
    f"{'DE':>2s}  "
    f"{'PE':>2s}  "
    f"{'':13s}  "
    f"{'':15s}"
)

SEPARATOR = "-" * 132


def _page_header(new_page: bool) -> list[str]:
    """
    Build the 7-line page header block.
    First line carries ASA '1' (form-feed / new page) or ' ' (continue).
    """
    asa = "1" if new_page else " "
    return [
        f"{asa}{TITLE1:^132s}",
        f" {TITLE2:^132s}",
        f" {TITLE3:^132s}",
        f" ",
        f" {COL_HDR1}",
        f" {COL_HDR2}",
        f" {SEPARATOR}",
    ]


def _fmt_comma15_2(val) -> str:
    """COMMA15.2 — right-justified in 15 characters."""
    if val is None:
        return " " * 15
    try:
        return f"{float(val):>15,.2f}"
    except (TypeError, ValueError):
        return " " * 15


def _fmt_apprlimt(val) -> str:
    """APPRLIMT display — right-justified in 17 characters, no decimals."""
    if val is None:
        return " " * 17
    try:
        return f"{float(val):>17,.0f}"
    except (TypeError, ValueError):
        return " " * 17


def _detail_line(row: dict, asa: str = " ") -> str:
    """Format a single detail row into a 133-character ASA record."""
    custname = str(row.get("CUSTNAME") or "")[:27]
    newic    = str(row.get("NEWIC")    or "")[:15]
    oldic    = str(row.get("OLDIC")    or "")[:15]
    cardno   = str(row.get("CARDNO")   or "")[:16]
    apprlimt = _fmt_apprlimt(row.get("APPRLIMT"))
    monitor  = str(row.get("MONITOR")  or "")[:2]
    typ      = str(row.get("TYPE")     or "")[:2]
    acctno   = f"{int(row.get('ACCTNO') or 0):>13d}"
    curbal   = _fmt_comma15_2(row.get("CURBAL"))

    body = (
        f"{custname:<27s}  "
        f"{newic:<15s}  "
        f"{oldic:<15s}  "
        f"{cardno:<16s}  "
        f"{apprlimt}  "
        f"{monitor:>2s}  "
        f"{typ:>2s}  "
        f"{acctno}  "
        f"{curbal}"
    )
    return f"{asa}{body}"


output_lines: list[str] = []
lines_on_page = PAGE_SIZE   # force a header on the very first customer group
first_page    = True
all_rows      = list(final_df.iter_rows(named=True))

if not all_rows:
    for h in _page_header(True):
        output_lines.append(h)
    lines_on_page = HEADER_LINES

i = 0
while i < len(all_rows):
    cust = str(all_rows[i].get("CUSTNAME") or "")

    # Collect all account rows belonging to this customer
    cust_group: list[dict] = []
    while i < len(all_rows) and str(all_rows[i].get("CUSTNAME") or "") == cust:
        cust_group.append(all_rows[i])
        i += 1

    # Lines needed for this customer block:
    #   detail rows  +  1 overline  +  1 summary  +  1 skip blank
    rows_needed = len(cust_group) + 3

    # Emit a new page header if the customer block would overflow the page
    if lines_on_page + rows_needed > PAGE_SIZE:
        for h in _page_header(not first_page):
            output_lines.append(h)
        first_page    = False
        lines_on_page = HEADER_LINES

    # Detail lines
    cust_total = 0.0
    for j, row in enumerate(cust_group):
        # ASA '0' (double-space before) on the first detail row after a header
        asa = "0" if (j == 0 and lines_on_page == HEADER_LINES) else " "
        output_lines.append(_detail_line(row, asa))
        lines_on_page += 1
        try:
            cust_total += float(row.get("CURBAL") or 0)
        except (TypeError, ValueError):
            pass

    # BREAK AFTER CUSTNAME / OL — overline before summary
    output_lines.append(f" {'=' * 132}")
    lines_on_page += 1

    # SUMMARIZE — CURBAL sum; CUSTNAME suppressed (SUPPRESS option)
    sum_body = (
        f"{'':27s}  "
        f"{'':15s}  "
        f"{'':15s}  "
        f"{'':16s}  "
        f"{'':17s}  "
        f"{'':2s}  "
        f"{'':2s}  "
        f"{'':13s}  "
        f"{_fmt_comma15_2(cust_total)}"
    )
    output_lines.append(f" {sum_body}")
    lines_on_page += 1

    # SKIP — blank line after the break group
    output_lines.append(" ")
    lines_on_page += 1

# COMPUTE AFTER; LINE ' '; ENDCOMP; — trailing blank line at report end
output_lines.append(" ")

# ============================================================================
# WRITE OUTPUT  (RECFM=FB, LRECL=133, ASA carriage control)
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(f"{ln:<133s}\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")

del final_df
gc.collect()

print("\nEIBDUNDP complete.")
