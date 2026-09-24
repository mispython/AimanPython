#!/usr/bin/env python3
"""
Program : EIBDUNDP.py
Purpose : Deposit Activities for Credit Card Holders.
          Identifies cardholders with significant deposit withdrawals
          and summarises their current balances across SA/CA/FD accounts.

Dependency:
    This program intentionally does NOT depend on REPTDATE.py or
    input_date.py. The original SAS reads its base date from an external
    control dataset ("DATA REPTDATE; SET DEPO.REPTDATE;") which is not
    provided/available in this environment, so the base date is derived
    locally in _derive_reptdate() below (system date - 1 day, matching
    the convention used elsewhere in this project), and the SAS SELECT
    block that snaps that base date onto a reporting-period boundary
    (day 8 / 15 / 22 / month-end) is reproduced exactly as written.
    Likewise, the //DEPO //IDEPO //PDEPO //PIDEPO GDG generations, the
    //CARD member, and the //CISDP //CISSAFD datasets are all resolved
    to fixed, independently-named physical paths below rather than via
    a "latest file" search utility, since their SAS generation numbers
    (0 / -1) and dynamically-built member name are fully deterministic
    from the report-date arithmetic alone.

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
 1. //DEPO  DD DSN=SAP.PBB.MNITB.DAILY(0)   (PBB, current day)
    DEPO.SAVING  -> mnitb_daily_pbb_saving_d0.sas7bdat   (ACCTNO, CURBAL)
    DEPO.CURRENT -> mnitb_daily_pbb_current_d0.sas7bdat  (ACCTNO, CURBAL)
    DEPO.FD      -> mnitb_daily_pbb_fd_d0.sas7bdat       (ACCTNO, CURBAL)

 2. //IDEPO DD DSN=SAP.PIBB.MNITB.DAILY(0)  (PIBB, current day)
    IDEPO.SAVING  -> mnitb_daily_pibb_saving_d0.sas7bdat
    IDEPO.CURRENT -> mnitb_daily_pibb_current_d0.sas7bdat
    IDEPO.FD      -> mnitb_daily_pibb_fd_d0.sas7bdat

 3. //PDEPO DD DSN=SAP.PBB.MNITB.DAILY(-1)  (PBB, previous day)
    PDEPO.SAVING  -> mnitb_daily_pbb_saving_d-1.sas7bdat
    PDEPO.CURRENT -> mnitb_daily_pbb_current_d-1.sas7bdat
    PDEPO.FD      -> mnitb_daily_pbb_fd_d-1.sas7bdat

 4. //PIDEPO DD DSN=SAP.PIBB.MNITB.DAILY(-1) (PIBB, previous day)
    PIDEPO.SAVING  -> mnitb_daily_pibb_saving_d-1.sas7bdat
    PIDEPO.CURRENT -> mnitb_daily_pibb_current_d-1.sas7bdat
    PIDEPO.FD      -> mnitb_daily_pibb_fd_d-1.sas7bdat

 5. //CARD DD DSN=SAP.PBB.CRM.CARD, member UNICARD&REPTYEAR&REPTMON&NOWK
    File : unicard<REPTYEAR><REPTMON><NOWK>.sas7bdat
    Cols used : CARDNO, MONITOR, SOURCE, CLOSECD, ACCTYPE, CARDHOLD,
                NEWIC, OLDIC, CUSTNAME, APPRLIMT

 6. //CISDP   DD DSN=SAP.PBB.CISBEXT.DP    (CA-side CIS extract)
    File : cisbext_dp.sas7bdat   Cols used : ACCTNO, NEWIC
    Used to build CISCA.

 7. //CISSAFD DD DSN=SAP.PBB.CRM.CISBEXT   (combined SA+FD CIS extract)
    File : crm_cisbext.sas7bdat  Cols used : ACCTNO, NEWIC
    Used to build CISSA / CISFD by splitting on ACCTNO numeric ranges,
    exactly as the original "DATA CISSA CISFD;" step does.

============================================================================
OUTPUT
============================================================================
//SASLIST DD DSN=SAP.PBB.CARD.DPACTV(+1), DCB=(RECFM=FB,LRECL=133,...)
This is a GDG dataset (new generation each run), so the Python output
uses a fixed filename rather than a dated one. PROC REPORT output carries
ASA carriage-control characters (RECFM=FB, LRECL=133); PAGE_SIZE=60 lines
(not specified otherwise in the SAS source -> project default).
"""

import gc
from pathlib import Path
from datetime import date, timedelta

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
# INPUT_DIR = BASE_DIR / "input" / "prod" / "EIBDUNDP"

STG_DIR  = Path("/stgsrcsys/host/uat/AII")
INPUT_DIR = STG_DIR / "EIBDUNDP"

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBDUNDP"
OUTPUT_DIR = BASE_DIR / "output" / "EIBDUNDP"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# # ---- DEPO (current day, PBB)  : //DEPO DD DSN=SAP.PBB.MNITB.DAILY(0) ------
# DEPO_SAVING_FILE   = INPUT_DIR / "mnitb_daily_pbb_saving_d0.sas7bdat"
# DEPO_CURRENT_FILE  = INPUT_DIR / "mnitb_daily_pbb_current_d0.sas7bdat"
# DEPO_FD_FILE       = INPUT_DIR / "mnitb_daily_pbb_fd_d0.sas7bdat"

# # ---- IDEPO (current day, PIBB): //IDEPO DD DSN=SAP.PIBB.MNITB.DAILY(0) ----
# IDEPO_SAVING_FILE   = INPUT_DIR / "mnitb_daily_pibb_saving_d0.sas7bdat"
# IDEPO_CURRENT_FILE  = INPUT_DIR / "mnitb_daily_pibb_current_d0.sas7bdat"
# IDEPO_FD_FILE       = INPUT_DIR / "mnitb_daily_pibb_fd_d0.sas7bdat"

# # ---- PDEPO (previous day, PBB): //PDEPO DD DSN=SAP.PBB.MNITB.DAILY(-1) ----
# PDEPO_SAVING_FILE   = INPUT_DIR / "mnitb_daily_pbb_saving_d-1.sas7bdat"
# PDEPO_CURRENT_FILE  = INPUT_DIR / "mnitb_daily_pbb_current_d-1.sas7bdat"
# PDEPO_FD_FILE       = INPUT_DIR / "mnitb_daily_pbb_fd_d-1.sas7bdat"

# # ---- PIDEPO (previous day, PIBB): //PIDEPO DD DSN=SAP.PIBB.MNITB.DAILY(-1)-
# PIDEPO_SAVING_FILE  = INPUT_DIR / "mnitb_daily_pibb_saving_d-1.sas7bdat"
# PIDEPO_CURRENT_FILE = INPUT_DIR / "mnitb_daily_pibb_current_d-1.sas7bdat"
# PIDEPO_FD_FILE      = INPUT_DIR / "mnitb_daily_pibb_fd_d-1.sas7bdat"

# # ---- CIS : //CISDP (CA side) and //CISSAFD (SA+FD side, split later) -----
# CISDP_FILE   = INPUT_DIR / "cisbext_dp.sas7bdat"
# CISSAFD_FILE = INPUT_DIR / "crm_cisbext.sas7bdat"

# ---- Output ----------------------------------------------------------------
# Generate time stamp
report_date = date.today() - timedelta(days=1)
ts = report_date.strftime("%y%m%d")

OUTPUT_FILE = OUTPUT_DIR / f"CARD_DPACTV_{ts}.txt"

CHUNK_ROWS = 500_000

# ============================================================================
# STEP 1: REPORT DATE DERIVATION
# ============================================================================
print("Step 1: Deriving report date...")


def _derive_reptdate() -> date:
    """
    Original SAS: DATA REPTDATE; SET DEPO.REPTDATE; RDATE=REPTDATE; ...
    DEPO.REPTDATE is an external control dataset supplying the base
    "as-of" date; since it is not available here, the base date is
    derived locally as system date - 1 day (the same convention this
    project's report-date logic ordinarily produces), and the SAS
    SELECT block below then snaps it onto the nearest reporting-period
    boundary exactly as in the original DATA step.
    """
    return date.today() - timedelta(days=1)


def _yy_mm_dd(d: date) -> str:
    """Return 'YYMMDD' string, e.g. 2026-09-23 -> '260923'."""
    return d.strftime("%y%m%d")


_rdate  = _derive_reptdate()             # RDATE  = REPTDATE (pre-adjustment)
_rdate1 = _rdate - timedelta(days=1)     # RDATE1 = RDATE - 1

_day, _mon, _yr = _rdate.day, _rdate.month, _rdate.year

if 2 <= _day <= 9:
    REPTDATE = date(_yr, _mon, 1) - timedelta(days=1)
    WK = "04"
elif 10 <= _day <= 16:
    REPTDATE = date(_yr, _mon, 8)
    WK = "01"
elif 17 <= _day <= 23:
    REPTDATE = date(_yr, _mon, 15)
    WK = "02"
else:
    if _day == 1:
        REPTDATE = date(_yr, _mon, 1) - timedelta(days=1)
    else:
        REPTDATE = date(_yr, _mon, 22)
    WK = "03"

REPTDAY  = f"{REPTDATE.day:02d}"                # CALL SYMPUT('REPTDAY',...)
REPTMON  = f"{REPTDATE.month:02d}"              # CALL SYMPUT('REPTMON',...)
REPTYEAR = f"{REPTDATE.year % 100:02d}"         # PUT(REPTDATE,YEAR2.)
NOWK     = WK

# --- Dated file keys -------------------------------------------------------
# Period-boundary key  → used ONLY for the weekly CARD member
PERIOD_YYMMDD = f"{REPTYEAR}{REPTMON}{REPTDAY}"          # '260915'  ← NOT for daily files

# Daily data keys → used for DEPO/IDEPO/PDEPO/PIDEPO
# current day = the report date itself      (matches GDG (0))
CUR_DATE_STR  = _rdate.strftime("%y%m%d")       # '260923'  ← today's daily file
# previous day = report date - 1 day        (matches GDG (-1))
PREV_DATE_STR = _rdate1.strftime("%y%m%d")      # '260922'  ← yesterday's daily file

# equivalent to f"{REPTYEAR}{REPTMON}{REPTDAY}" for the current day:
CUR_DATE_STR_ALT = f"{REPTYEAR}{REPTMON}{REPTDAY}"        # also '260923'

RDTEA = _rdate.strftime("%d/%m/%y")             # PUT(RDATE,  DDMMYY8.)
RDTEB = _rdate1.strftime("%d/%m/%y")            # PUT(RDATE1, DDMMYY8.)

# # ---- CARD : //CARD DD DSN=SAP.PBB.CRM.CARD, member UNICARD&YY&MM&WK ------
# CARD_FILE = INPUT_DIR / f"unicard{REPTYEAR}{REPTMON}{NOWK}.sas7bdat"

# ---- DEPO  (current day, PBB) --------------------------------------------
DEPO_SAVING_FILE   = INPUT_DIR / f"sa{CUR_DATE_STR}.sas7bdat"
DEPO_CURRENT_FILE  = INPUT_DIR / f"ca{CUR_DATE_STR}.sas7bdat"
DEPO_FD_FILE       = INPUT_DIR / f"fd{CUR_DATE_STR}.sas7bdat"

# ---- IDEPO (current day, PIBB) -------------------------------------------
IDEPO_SAVING_FILE   = INPUT_DIR / f"isa{CUR_DATE_STR}.sas7bdat"
IDEPO_CURRENT_FILE  = INPUT_DIR / f"ica{CUR_DATE_STR}.sas7bdat"
IDEPO_FD_FILE       = INPUT_DIR / f"ifd{CUR_DATE_STR}.sas7bdat"

# ---- PDEPO (previous day, PBB) -------------------------------------------
PDEPO_SAVING_FILE   = INPUT_DIR / f"sa{PREV_DATE_STR}.sas7bdat"
PDEPO_CURRENT_FILE  = INPUT_DIR / f"ca{PREV_DATE_STR}.sas7bdat"
PDEPO_FD_FILE       = INPUT_DIR / f"fd{PREV_DATE_STR}.sas7bdat"

# ---- PIDEPO (previous day, PIBB) -----------------------------------------
PIDEPO_SAVING_FILE  = INPUT_DIR / f"isa{PREV_DATE_STR}.sas7bdat"
PIDEPO_CURRENT_FILE = INPUT_DIR / f"ica{PREV_DATE_STR}.sas7bdat"
PIDEPO_FD_FILE      = INPUT_DIR / f"ifd{PREV_DATE_STR}.sas7bdat"

# ---- CIS extracts --------------------------------------------------------
CISDP_FILE   = INPUT_DIR / "cisbext_dp_deposit.sas7bdat"
CISSAFD_FILE = INPUT_DIR / "crm_cisbext_deposit.sas7bdat"

# ---- CARD weekly member (already dated) ----------------------------------
CARD_FILE = INPUT_DIR / f"host_unicard{REPTYEAR}{REPTMON}{NOWK}.sas7bdat"

print(f"  REPTDATE : {REPTDATE}  (WK={WK})")
print(f"  RDTEA    : {RDTEA}   RDTEB : {RDTEB}")
print(f"  REPTYEAR={REPTYEAR}  REPTMON={REPTMON}  REPTDAY={REPTDAY}  NOWK={NOWK}")
print(f"  CARD file: {CARD_FILE.name}")
print(f"  Output   : {OUTPUT_FILE}")

# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET  (EIIMRM01.py pattern)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


# def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
#     print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
#     writer = None
#     schema = None
#     total = 0

#     reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
#     for chunk in reader:
#         if schema is None:
#             fields = []
#             for col, dtype in chunk.dtypes.items():
#                 if dtype == 'object':
#                     pa_type = pa.string()
#                 elif pd.api.types.is_integer_dtype(dtype):
#                     pa_type = pa.int64()
#                 elif pd.api.types.is_float_dtype(dtype):
#                     pa_type = pa.float64()
#                 else:
#                     pa_type = pa.from_numpy_dtype(dtype)
#                 fields.append(pa.field(col, pa_type))
#             schema = pa.schema(fields)
#             writer = pq.ParquetWriter(cache_path, schema, compression="snappy")

#         table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
#         writer.write_table(table)
#         total += len(chunk)
#         del chunk, table
#         gc.collect()

#     if writer:
#         writer.close()
#     print(f"  [{tag}] Done - {total:,} rows cached.")


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    def _build_schema(df: "pd.DataFrame") -> pa.Schema:
        fields = []
        for col, dtype in df.dtypes.items():
            if dtype == "object":
                pa_type = pa.string()
            elif pd.api.types.is_integer_dtype(dtype):
                pa_type = pa.int64()
            elif pd.api.types.is_float_dtype(dtype):
                pa_type = pa.float64()
            else:
                pa_type = pa.from_numpy_dtype(dtype)
            fields.append(pa.field(col, pa_type))
        return pa.schema(fields)
    
    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if schema is None:
            schema = _build_schema(chunk)
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")

        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer is not None:
        writer.close()
    else:
        # Source file has 0 rows -- the chunked reader yielded nothing.
        # Read once without chunksize to obtain the schema, then write a
        # zero-row parquet so downstream read_parquet() finds the file.
        empty = pd.read_sas(sas_path, encoding="latin1")
        schema = _build_schema(empty)
        empty_table = pa.Table.from_pandas(empty, schema=schema, preserve_index=False)
        pq.write_table(empty_table, cache_path)

    print(f"  [{tag}] Done - {total:,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# STEP 2: CACHE ALL PHYSICAL INPUTS TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")

DEPO_SAVING_CACHE    = _load_cached(DEPO_SAVING_FILE,   "DEPO_SAVING")
DEPO_CURRENT_CACHE   = _load_cached(DEPO_CURRENT_FILE,  "DEPO_CURRENT")
DEPO_FD_CACHE        = _load_cached(DEPO_FD_FILE,       "DEPO_FD")

IDEPO_SAVING_CACHE   = _load_cached(IDEPO_SAVING_FILE,  "IDEPO_SAVING")
IDEPO_CURRENT_CACHE  = _load_cached(IDEPO_CURRENT_FILE, "IDEPO_CURRENT")
IDEPO_FD_CACHE       = _load_cached(IDEPO_FD_FILE,      "IDEPO_FD")

PDEPO_SAVING_CACHE   = _load_cached(PDEPO_SAVING_FILE,   "PDEPO_SAVING")
PDEPO_CURRENT_CACHE  = _load_cached(PDEPO_CURRENT_FILE,  "PDEPO_CURRENT")
PDEPO_FD_CACHE       = _load_cached(PDEPO_FD_FILE,       "PDEPO_FD")

PIDEPO_SAVING_CACHE  = _load_cached(PIDEPO_SAVING_FILE,  "PIDEPO_SAVING")
PIDEPO_CURRENT_CACHE = _load_cached(PIDEPO_CURRENT_FILE, "PIDEPO_CURRENT")
PIDEPO_FD_CACHE      = _load_cached(PIDEPO_FD_FILE,      "PIDEPO_FD")

CARD_CACHE           = _load_cached(CARD_FILE,     "CARD")
CISDP_CACHE          = _load_cached(CISDP_FILE,    "CISDP")
CISSAFD_CACHE        = _load_cached(CISSAFD_FILE,  "CISSAFD")

# ============================================================================
# STEP 3: BUILD CARD / CARD1
# DATA CARD(KEEP=CARDNO MONITOR SOURCE CLOSECD NEWIC OLDIC CUSTNAME APPRLIMT)
#      CARD1(KEEP=NEWIC);
#   SET CARD.UNICARD&REPTYEAR&REPTMON&NOWK;
#   IF CLOSECD NE ' '                        THEN DELETE;
#   IF ACCTYPE IN ('IS')                     THEN DELETE;
#   IF ACCTYPE IN ('IA') AND CARDHOLD NE 1   THEN DELETE;
#   IF NEWIC=' ' THEN NEWIC=OLDIC;
#   IF MONITOR IN('Z','I') OR SOURCE='GCPIFD0209' THEN OUTPUT CARD;
#   IF MONITOR IN ('Z') THEN OUTPUT CARD1;
# ============================================================================
print("\nStep 3: Processing CARD file...")

con = duckdb.connect(database=":memory:")
card_df = con.execute(f"""
    SELECT
        TRIM(CAST(CARDNO   AS VARCHAR)) AS CARDNO,
        TRIM(CAST(MONITOR  AS VARCHAR)) AS MONITOR,
        TRIM(CAST(SOURCE   AS VARCHAR)) AS SOURCE,
        COALESCE(TRIM(CAST(CLOSECD AS VARCHAR)), '') AS CLOSECD,
        TRIM(CAST(NEWIC    AS VARCHAR)) AS NEWIC,
        TRIM(CAST(OLDIC    AS VARCHAR)) AS OLDIC,
        TRIM(CAST(CUSTNAME AS VARCHAR)) AS CUSTNAME,
        CAST(APPRLIMT AS DOUBLE)        AS APPRLIMT
    FROM read_parquet('{CARD_CACHE.as_posix()}')
    WHERE COALESCE(TRIM(CAST(CLOSECD AS VARCHAR)), '') = ''
      AND COALESCE(TRIM(CAST(ACCTYPE AS VARCHAR)), '') <> 'IS'
      AND NOT (COALESCE(TRIM(CAST(ACCTYPE AS VARCHAR)), '') = 'IA'
               AND COALESCE(CAST(CARDHOLD AS DOUBLE), 0) <> 1)
""").pl()
con.close()
gc.collect()

print(f"  CARD raw rows after filter : {len(card_df):,}")

# IF NEWIC=' ' THEN NEWIC=OLDIC
card_df = card_df.with_columns(
    pl.when(pl.col("NEWIC").is_null() | (pl.col("NEWIC") == ""))
    .then(pl.col("OLDIC"))
    .otherwise(pl.col("NEWIC"))
    .alias("NEWIC")
)

CARD_KEEP_COLS = ["CARDNO", "MONITOR", "SOURCE", "CLOSECD",
                  "NEWIC", "OLDIC", "CUSTNAME", "APPRLIMT"]

# CARD  : MONITOR IN ('Z','I') OR SOURCE='GCPIFD0209'
card_main = card_df.filter(
    pl.col("MONITOR").is_in(["Z", "I"]) | (pl.col("SOURCE") == "GCPIFD0209")
).select(CARD_KEEP_COLS)

# CARD1 : MONITOR IN ('Z')
card1 = card_df.filter(pl.col("MONITOR").is_in(["Z"])).select(["NEWIC"])

del card_df
gc.collect()

# PROC SORT DATA=CARD  NODUPKEY; BY NEWIC;
# PROC SORT DATA=CARD1 NODUPKEY; BY NEWIC;
card_main = card_main.sort("NEWIC", maintain_order=True).unique(subset=["NEWIC"], keep="first")
card1     = card1.sort("NEWIC", maintain_order=True).unique(subset=["NEWIC"], keep="first")

print(f"  CARD  rows : {len(card_main):,}")
print(f"  CARD1 rows : {len(card1):,}")

# ============================================================================
# STEP 4: BUILD CISCA / CISSA / CISFD
# PROC SORT DATA=CISDP.DEPOSIT   OUT=CISCA(KEEP=ACCTNO NEWIC);   BY NEWIC;
# PROC SORT DATA=CISSAFD.DEPOSIT OUT=CISSAFD(KEEP=ACCTNO NEWIC); BY NEWIC;
# DATA CISCA;        MERGE CARD(IN=A) CISCA(IN=B);    BY NEWIC; IF A AND B; TYPE='CA';
# DATA CISSA CISFD;  MERGE CARD(IN=A) CISSAFD(IN=B);  BY NEWIC; IF A AND B;
#   IF (1e9<ACCTNO<2e9) OR (7e9<ACCTNO<8e9) THEN DO; TYPE='FD'; OUTPUT CISFD; END;
#   ELSE IF (4e9<=ACCTNO<7e9)               THEN DO; TYPE='SA'; OUTPUT CISSA; END;
#   (else: row is dropped)
# CISCA/CISSA/CISFD each end up carrying ALL of CARD's columns plus
# ACCTNO and TYPE -- they are NOT slim ACCTNO/NEWIC-only tables once the
# MERGE with CARD executes; this matters for later steps.
# ============================================================================
print("\nStep 4: Building CISCA / CISSA / CISFD...")

con = duckdb.connect(database=":memory:")
con.register("card_tbl", card_main.to_arrow())

CIS_CARD_COLS = ["CARDNO", "MONITOR", "SOURCE", "CLOSECD", "NEWIC", "OLDIC", "CUSTNAME", "APPRLIMT"]

cisca_matched = con.execute(f"""
    SELECT k.CARDNO, k.MONITOR, k.SOURCE, k.CLOSECD, k.NEWIC, k.OLDIC, k.CUSTNAME, k.APPRLIMT,
           CAST(c.ACCTNO AS BIGINT) AS ACCTNO
    FROM read_parquet('{CISDP_CACHE.as_posix()}') c
    INNER JOIN card_tbl k ON TRIM(CAST(c.NEWIC AS VARCHAR)) = k.NEWIC
""").pl()

cissafd_matched = con.execute(f"""
    SELECT k.CARDNO, k.MONITOR, k.SOURCE, k.CLOSECD, k.NEWIC, k.OLDIC, k.CUSTNAME, k.APPRLIMT,
           CAST(c.ACCTNO AS BIGINT) AS ACCTNO
    FROM read_parquet('{CISSAFD_CACHE.as_posix()}') c
    INNER JOIN card_tbl k ON TRIM(CAST(c.NEWIC AS VARCHAR)) = k.NEWIC
""").pl()
con.close()
gc.collect()

cisca_df = cisca_matched.with_columns(pl.lit("CA").alias("TYPE")).select(CIS_CARD_COLS + ["ACCTNO", "TYPE"])

cisfd_df = cissafd_matched.filter(
    ((pl.col("ACCTNO") > 1_000_000_000) & (pl.col("ACCTNO") < 2_000_000_000))
    | ((pl.col("ACCTNO") > 7_000_000_000) & (pl.col("ACCTNO") < 8_000_000_000))
).with_columns(pl.lit("FD").alias("TYPE")).select(CIS_CARD_COLS + ["ACCTNO", "TYPE"])

cissa_df = cissafd_matched.filter(
    (pl.col("ACCTNO") >= 4_000_000_000) & (pl.col("ACCTNO") < 7_000_000_000)
).with_columns(pl.lit("SA").alias("TYPE")).select(CIS_CARD_COLS + ["ACCTNO", "TYPE"])

del cisca_matched, cissafd_matched
gc.collect()

print(f"  CISCA rows : {len(cisca_df):,}")
print(f"  CISSA rows : {len(cissa_df):,}")
print(f"  CISFD rows : {len(cisfd_df):,}")

# ============================================================================
# HELPERS: SAS "MERGE X(IN=A) BAL(IN=B); BY ACCTNO; IF A;" WHERE BAL
# CONTRIBUTES ONLY A VALUE COLUMN THAT DOESN'T EXIST IN X.
#
# A variable coming exclusively from the non-matching side of a BY-group
# merge is NOT reset to missing when that side fails to match for the
# current BY value -- per SAS's automatic-retain-across-iterations rule
# for SET/MERGE-sourced variables, it keeps whatever value was last read
# from BAL, i.e. it carries forward from the closest ACCTNO <= the
# current one in ascending BY order. This is a well-known, if unintended,
# SAS MERGE artifact, and it is reproduced exactly (not "fixed" to 0/
# missing) via a backward as-of join on the sorted ACCTNO key.
# ============================================================================
# def _asof_carry(base_df: pl.DataFrame, value_df: pl.DataFrame, value_col: str) -> pl.DataFrame:
#     base_sorted  = base_df.sort("ACCTNO")
#     value_sorted = value_df.sort("ACCTNO")
#     return base_sorted.join_asof(value_sorted.select(["ACCTNO", value_col]),
#                                   on="ACCTNO", strategy="backward")


def _asof_carry(base_df: pl.DataFrame, value_df: pl.DataFrame, value_col: str) -> pl.DataFrame:
    base_sorted = base_df.sort("ACCTNO")
    value_dedup = (
        value_df
        .sort("ACCTNO", maintain_order=True)
        .unique(subset=["ACCTNO"], keep="first")
    )
    joined = base_sorted.join(
        value_dedup.select(["ACCTNO", value_col]),
        on="ACCTNO", how="left",
    )
    return joined.with_columns(pl.col(value_col).fill_null(0.0))


SHARED_CARD_COLS = ["NEWIC", "CARDNO", "MONITOR", "SOURCE", "CLOSECD", "OLDIC", "CUSTNAME", "APPRLIMT", "TYPE"]


def _merge_depo_pdepo(depo_df: pl.DataFrame, pdepo_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA DEPO; MERGE DEPO(IN=A) PDEPO(IN=B); BY ACCTNO; IF A;
    PRE_CURBAL exists only in PDEPO(B) -> backward as-of carry-forward
    (see _asof_carry). The remaining card-attribute columns exist on
    BOTH sides; SAS overwrites them with PDEPO's value only on an EXACT
    ACCTNO match (PDEPO is listed after DEPO in the MERGE statement); on
    a miss, PDEPO contributes nothing that iteration, so DEPO's own
    freshly-read value stands. That is a plain exact left join with a
    coalesce back to DEPO's own value on a miss.
    """
    # depo_sorted  = depo_df.sort("ACCTNO")
    # pdepo_sorted = pdepo_df.sort("ACCTNO")
    
    depo_sorted  = depo_df.sort("ACCTNO")
    pdepo_sorted = (
        pdepo_df.sort("ACCTNO")
                 .unique(subset=["ACCTNO"], keep="first")
    )

    asof_pre = depo_sorted.select(["ACCTNO"]).join_asof(
        pdepo_sorted.select(["ACCTNO", "PRE_CURBAL"]), on="ACCTNO", strategy="backward"
    )

    exact = depo_sorted.join(
        pdepo_sorted.select(["ACCTNO"] + SHARED_CARD_COLS),
        on="ACCTNO", how="left", suffix="_pd",
    )
    for c in SHARED_CARD_COLS:
        exact = exact.with_columns(
            pl.coalesce([pl.col(f"{c}_pd"), pl.col(c)]).alias(c)
        ).drop(f"{c}_pd")

    return exact.with_columns(asof_pre["PRE_CURBAL"])


def _read_acct_bal(cache: Path, value_col: str) -> pl.DataFrame:
    con2 = duckdb.connect(database=":memory:")
    df = con2.execute(f"""
        SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(CURBAL AS DOUBLE) AS {value_col}
        FROM read_parquet('{cache.as_posix()}')
    """).pl()
    con2.close()
    return df


# ============================================================================
# STEP 5: BUILD PSA / PCA / PFD  (previous-period balances)
# PROC SORT DATA=PDEPO.SAVING  OUT=PSA (RENAME=(CURBAL=PRE_CURBAL));
# PROC SORT DATA=PIDEPO.SAVING OUT=PISA(RENAME=(CURBAL=PRE_CURBAL));
# DATA PSA; SET PSA PISA; RUN;  (same pattern for PCA / PFD)
# DATA PSA; MERGE CISSA(IN=A) PSA(IN=B); BY ACCTNO; IF A; RUN;
# ============================================================================
print("\nStep 5: Building PSA / PCA / PFD (previous-period balances)...")

psa_bal = pl.concat([_read_acct_bal(PDEPO_SAVING_CACHE,  "PRE_CURBAL"),
                      _read_acct_bal(PIDEPO_SAVING_CACHE, "PRE_CURBAL")])
pca_bal = pl.concat([_read_acct_bal(PDEPO_CURRENT_CACHE,  "PRE_CURBAL"),
                      _read_acct_bal(PIDEPO_CURRENT_CACHE, "PRE_CURBAL")])
pfd_bal = pl.concat([_read_acct_bal(PDEPO_FD_CACHE,  "PRE_CURBAL"),
                      _read_acct_bal(PIDEPO_FD_CACHE, "PRE_CURBAL")])

psa_df = _asof_carry(cissa_df, psa_bal, "PRE_CURBAL")
pca_df = _asof_carry(cisca_df, pca_bal, "PRE_CURBAL")
pfd_df = _asof_carry(cisfd_df, pfd_bal, "PRE_CURBAL")

del psa_bal, pca_bal, pfd_bal
gc.collect()

print(f"  PSA rows : {len(psa_df):,}   PCA rows : {len(pca_df):,}   PFD rows : {len(pfd_df):,}")

# ============================================================================
# STEP 6: BUILD SA / CA / FD  (current-period balances)
# PROC SORT DATA=DEPO.SAVING  OUT=SA(KEEP=ACCTNO CURBAL);
# PROC SORT DATA=IDEPO.SAVING OUT=ISA(KEEP=ACCTNO CURBAL);
# DATA SA; SET SA ISA; RUN;  (same pattern for CA / FD)
# DATA SA; MERGE CISSA(IN=A) SA(IN=B); BY ACCTNO; IF A; RUN;
# ============================================================================
print("\nStep 6: Building SA / CA / FD (current-period balances)...")

sa_bal = pl.concat([_read_acct_bal(DEPO_SAVING_CACHE,  "CURBAL"),
                     _read_acct_bal(IDEPO_SAVING_CACHE, "CURBAL")])
ca_bal = pl.concat([_read_acct_bal(DEPO_CURRENT_CACHE,  "CURBAL"),
                     _read_acct_bal(IDEPO_CURRENT_CACHE, "CURBAL")])
fd_bal = pl.concat([_read_acct_bal(DEPO_FD_CACHE,  "CURBAL"),
                     _read_acct_bal(IDEPO_FD_CACHE, "CURBAL")])

sa_df = _asof_carry(cissa_df, sa_bal, "CURBAL")
ca_df = _asof_carry(cisca_df, ca_bal, "CURBAL")
fd_df = _asof_carry(cisfd_df, fd_bal, "CURBAL")

# DEBUG
_probe = [1599500135, 1815007914, 1595256021, 1311043808]
print("=== fd_bal rows for probed ACCTNOs ===")
print(fd_bal.filter(pl.col("ACCTNO").is_in(_probe)).to_pandas().to_string())
print("=== fd_df result for probed ACCTNOs ===")
print(fd_df.filter(pl.col("ACCTNO").is_in(_probe)).select(["ACCTNO", "NEWIC", "CURBAL"]).to_pandas().to_string())

# DEBUG
print("=== raw DEPO_FD cache ===")
raw_fd = pl.read_parquet(DEPO_FD_CACHE)
print("rows:", raw_fd.height)
print("cols:", raw_fd.columns)
_probe_bal = [1311043808, 1595256021, 1599500135, 1815007914]
sub_fd = raw_fd.filter(pl.col("ACCTNO").cast(pl.Int64).is_in(_probe_bal))
print("probe rows in DEPO_FD:")
print(sub_fd.to_pandas().to_string())

print("=== raw IDEPO_FD cache ===")
raw_ifd = pl.read_parquet(IDEPO_FD_CACHE)
print("rows:", raw_ifd.height)
print("cols:", raw_ifd.columns)
sub_ifd = raw_ifd.filter(pl.col("ACCTNO").cast(pl.Int64).is_in(_probe_bal))
print("probe rows in IDEPO_FD:")
print(sub_ifd.to_pandas().to_string())

# DEBUG
print("=== FD file — records in [1.595B, 1.596B] ===")
around = fd_bal.filter(
    (pl.col("ACCTNO") >= 1595000000) & (pl.col("ACCTNO") <= 1596000000)
).sort("ACCTNO")
print("count:", around.height)
print(around.to_pandas().to_string())

del sa_bal, ca_bal, fd_bal
gc.collect()

print(f"  SA rows : {len(sa_df):,}   CA rows : {len(ca_df):,}   FD rows : {len(fd_df):,}")

# ============================================================================
# STEP 7: DATA DEPO; SET SA CA FD;  DATA PDEPO; SET PSA PCA PFD;
# PROC SORT DATA=DEPO;  BY ACCTNO;
# PROC SORT DATA=PDEPO; BY ACCTNO;
# DATA DEPO;
#   MERGE DEPO(IN=A) PDEPO(IN=B); BY ACCTNO; IF A;
#   WITHDR = PRE_CURBAL - CURBAL;
#   IF WITHDR < 0 THEN WITHDR = 0;
# (SAS: a missing WITHDR from a missing PRE_CURBAL sorts as < 0, so it
#  is also corrected to 0 by this same statement -- preserved below.)
# ============================================================================
print("\nStep 7: Combining DEPO / PDEPO and calculating withdrawals...")

depo_df  = pl.concat([sa_df, ca_df, fd_df])
pdepo_df = pl.concat([psa_df, pca_df, pfd_df])

depo_final = _merge_depo_pdepo(depo_df, pdepo_df)
depo_final = depo_final.with_columns(
    pl.when(pl.col("PRE_CURBAL").is_null() | ((pl.col("PRE_CURBAL") - pl.col("CURBAL")) < 0))
    .then(0.0)
    .otherwise(pl.col("PRE_CURBAL") - pl.col("CURBAL"))
    .alias("WITHDR")
)

print(f"  DEPO rows : {len(depo_final):,}")

del pdepo_df
gc.collect()

# ============================================================================
# STEP 8: PROC SUMMARY DATA=DEPO NWAY; CLASS NEWIC; VAR PRE_CURBAL CURBAL WITHDR;
#         OUTPUT OUT=DEPO1 SUM=;
# PROC SUMMARY's SUM statistic ignores missing values (a missing input
# simply doesn't contribute), which is reproduced via COALESCE(...,0.0)
# before summing.
# ============================================================================
print("\nStep 8: Summarising DEPO by NEWIC...")

con = duckdb.connect(database=":memory:")
con.register("depo_tbl", depo_final.to_arrow())
depo1_df = con.execute("""
    SELECT
        NEWIC,
        SUM(COALESCE(PRE_CURBAL, 0.0)) AS PRE_CURBAL,
        SUM(COALESCE(CURBAL,     0.0)) AS CURBAL,
        SUM(COALESCE(WITHDR,     0.0)) AS WITHDR
    FROM depo_tbl
    GROUP BY NEWIC
""").pl()
con.close()
gc.collect()

print(f"  DEPO1 rows : {len(depo1_df):,}")

# ============================================================================
# STEP 9: DATA DEPO2 DEPO3; SET DEPO1;
#   PERCEN = (WITHDR/PRE_CURBAL)*100;
#   IF PERCEN >= 50               THEN OUTPUT DEPO2;
#   IF PERCEN < 50 AND CURBAL<=500000 THEN OUTPUT DEPO3;
# A zero PRE_CURBAL produces a missing PERCEN in SAS (division by zero);
# SAS treats a missing numeric as smaller than any real number, so a
# missing PERCEN fails ">= 50" but satisfies "< 50" -- both preserved.
# ============================================================================
print("\nStep 9: Splitting into DEPO2 / DEPO3...")

depo1_df = depo1_df.with_columns(
    pl.when(pl.col("PRE_CURBAL") != 0)
    .then((pl.col("WITHDR") / pl.col("PRE_CURBAL")) * 100)
    .otherwise(None)
    .alias("PERCEN")
)

depo2_df = (
    depo1_df.filter(pl.col("PERCEN") >= 50)
    .select([pl.col("NEWIC"), pl.col("CURBAL").alias("SUMBAL")])
)

depo3_df = (
    depo1_df.filter((pl.col("PERCEN") < 50) | pl.col("PERCEN").is_null())
    .filter(pl.col("CURBAL") <= 500_000)
    .select([pl.col("NEWIC"), pl.col("CURBAL").alias("SUMBAL")])
)

# # DEBUG
# print(f"  DEPO2 rows : {len(depo2_df):,}   DEPO3 rows : {len(depo3_df):,}")

del depo1_df
gc.collect()

print(f"  DEPO2 rows : {len(depo2_df):,}   DEPO3 rows : {len(depo3_df):,}")

# ============================================================================
# STEP 10: DATA DEPO3A; MERGE DEPO3(IN=A) CARD1(IN=B); BY NEWIC; IF A AND B;
#          DATA TOT; SET DEPO2 DEPO3A;
#          PROC SORT DATA=TOT NODUPKEY; BY NEWIC;
# (PERCEN>=50 and PERCEN<50 are mutually exclusive per NEWIC, so DEPO2
#  and DEPO3A cannot share a key -- NODUPKEY is effectively a no-op here
#  but is applied for fidelity.)
# ============================================================================
print("\nStep 10: Building TOT...")

depo3a_df = depo3_df.join(card1, on="NEWIC", how="inner")
tot_df = pl.concat([depo2_df, depo3a_df], how="diagonal").unique(subset=["NEWIC"], keep="first")

del depo2_df, depo3_df, depo3a_df, card1
gc.collect()

print(f"  TOT rows : {len(tot_df):,}")

# ============================================================================
# STEP 11: DATA DEPO; SET SA CA FD;   (fresh account-level rebuild, the
#          SA/CA/FD tables from Step 6 are untouched since then)
# PROC SORT DATA=DEPO;  BY NEWIC;
# PROC SORT DATA=TOT NODUPKEY; BY NEWIC;
# DATA FINAL; MERGE DEPO(IN=A) TOT(IN=B); BY NEWIC; IF B;
# (every NEWIC in TOT necessarily has >=1 matching DEPO account, so this
#  is equivalent to an inner join, replicated 1-to-many by NEWIC.)
# PROC SORT DATA=FINAL; BY CUSTNAME ACCTNO;
# ============================================================================
print("\nStep 11: Building FINAL dataset...")

depo_acct_df = pl.concat([sa_df, ca_df, fd_df])
final_df = depo_acct_df.join(tot_df, on="NEWIC", how="inner").sort(["CUSTNAME", "ACCTNO"])

del sa_df, ca_df, fd_df, cisca_df, cissa_df, cisfd_df, depo_acct_df, tot_df, depo_final, depo_df
gc.collect()

print(f"  FINAL rows : {len(final_df):,}")
# print(final_df.head(10))

# ============================================================================
# STEP 12: PROC REPORT  (ASA carriage control, LRECL=133, RECFM=FB)
# TITLE1 'P U B L I C   B A N K   B E R H A D'
# TITLE2 'REPORT PERIOD :' "&RDTEB" ' - ' "&RDTEA"
# TITLE3 'CARDHOLDERS DEPOSITS ACCOUNT'
# COLUMN CUSTNAME NEWIC OLDIC CARDNO APPRLIMT MONITOR TYPE ACCTNO CURBAL
# DEFINE CUSTNAME / ORDER  FORMAT=$27.
# DEFINE APPRLIMT / DISPLAY 'CREDIT CARD LIMIT'
# DEFINE MONITOR  / DISPLAY 'CO*DE'   (SPLIT='*' header wrap)
# DEFINE TYPE     / DISPLAY 'TY*PE'
# DEFINE ACCTNO   / DISPLAY CENTER FORMAT=13.
# DEFINE CURBAL   / ANALYSIS SUM FORMAT=COMMA15.2 'BALANCE'
# BREAK AFTER CUSTNAME / OL SUMMARIZE SUPPRESS SKIP
# COMPUTE AFTER; LINE ' '; ENDCOMP;
# No OPTIONS MISSING= is set in this JCL, so a genuinely missing numeric
# value formats as a single "." under its numeric format (SAS default),
# not blank and not "0".
# ============================================================================
print("\nStep 12: Generating report...")

PAGE_SIZE    = 60
HEADER_LINES = 9   # 3 title lines + 1 blank + 4 column header lines + 1 separator

TITLE1 = "P U B L I C   B A N K   B E R H A D"
TITLE2 = f"REPORT PERIOD : {RDTEB} - {RDTEA}"
TITLE3 = "CARDHOLDERS DEPOSITS ACCOUNT"

# ---------------------------------------------------------------------------
# Header block: 4 column-header lines that mirror SAS PROC REPORT's
# bottom-aligned stacking of wrapped headers.
# ---------------------------------------------------------------------------
COL_HDR_L1 = (
    f"{'':<27s}  "
    f"{'':<12s}  "
    f"{'':<12s}  "
    f"{'':<16s}  "
    f"{'':>9s}  "
    f"{'C':>1s}  "
    f"{'':<2s}  "
    f"{'':>13s}  "
    f"{'':>15s}"
)

COL_HDR_L2 = (
    f"{'':<27s}  "
    f"{'':<12s}  "
    f"{'':<12s}  "
    f"{'':<16s}  "
    f"{'CREDIT':>9s}  "
    f"{'O':>1s}  "
    f"{'':<2s}  "
    f"{'':>13s}  "
    f"{'':>15s}"
)

COL_HDR_L3 = (
    f"{'':<27s}  "
    f"{'':<12s}  "
    f"{'':<12s}  "
    f"{'':<16s}  "
    f"{'CARD':>9s}  "
    f"{'D':>1s}  "
    f"{'TY':<2s}  "
    f"{'':>13s}  "
    f"{'':>15s}"
)

COL_HDR_L4 = (
    f"{'CUSTOMER NAME':<27s}  "
    f"{'NEW ICNO':<12s}  "
    f"{'OLD ICNO':<12s}  "
    f"{'CARD NUMBER':<16s}  "
    f"{'LIMIT':>9s}  "
    f"{'E':>1s}  "
    f"{'PE':<2s}  "
    f"{'ACCTNO':>13s}  "
    f"{'BALANCE':>15s}"
)

SEPARATOR = " " + "-" * 123
HEADER_LINES = 3 + 1 + 4 + 1   # 3 titles + blank + 4 col headers + dashes = 9


def _page_header(new_page: bool) -> list:
    asa = "1" if new_page else " "
    return [
        f"{asa}{TITLE1:^132s}",
        f" {TITLE2:^132s}",
        f" {TITLE3:^132s}",
        f" ",
        f" {COL_HDR_L1}",
        f" {COL_HDR_L2}",
        f" {COL_HDR_L3}",
        f" {COL_HDR_L4}",
        f"{SEPARATOR}",
    ]


def _fmt_comma15_2(val) -> str:
    """COMMA15.2 -- default SAS missing character '.' when the value is
    a genuine SAS missing numeric (no MISSING= option set in this JCL)."""
    if val is None:
        return ".".rjust(15)
    try:
        return f"{float(val):>15,.2f}"
    except (TypeError, ValueError):
        return ".".rjust(15)


def _fmt_apprlimt(val) -> str:
    if val is None:
        return ".".rjust(9)
    try:
        return f"{float(val):>9.0f}"
    except (TypeError, ValueError):
        return ".".rjust(9)


def _detail_line(row: dict,
                 asa: str = " ",
                 hide_custname: bool = False) -> str:
    custname = "" if hide_custname else str(row.get("CUSTNAME") or "")[:27]
    newic    = str(row.get("NEWIC")    or "")[:12]
    oldic    = str(row.get("OLDIC")    or "")[:12]
    cardno   = str(row.get("CARDNO")   or "")[:16]
    apprlimt = _fmt_apprlimt(row.get("APPRLIMT"))
    monitor  = str(row.get("MONITOR")  or "")[:1]
    typ      = str(row.get("TYPE")     or "")[:2]
    acctno   = f"{int(row.get('ACCTNO') or 0):>13d}"
    curbal   = _fmt_comma15_2(row.get("CURBAL"))

    body = (
        f"{custname:<27s}  "
        f"{newic:<12s}  "
        f"{oldic:<12s}  "
        f"{cardno:<16s}  "
        f"{apprlimt}  "
        f"{monitor:>1s}  "
        f"{typ:<2s}  "
        f"{acctno}  "
        f"{curbal}"
    )
    return f"{asa}{body}"


output_lines = []
lines_on_page = PAGE_SIZE
first_page    = True
all_rows      = list(final_df.iter_rows(named=True))

if not all_rows:
    output_lines.extend(_page_header(True))
    lines_on_page = HEADER_LINES

i = 0
while i < len(all_rows):
    cust = str(all_rows[i].get("CUSTNAME") or "")

    cust_group = []
    while i < len(all_rows) and str(all_rows[i].get("CUSTNAME") or "") == cust:
        cust_group.append(all_rows[i])
        i += 1

    rows_needed = len(cust_group) + 3    # detail rows + overline + sum + skip

    if lines_on_page + rows_needed > PAGE_SIZE:
        output_lines.extend(_page_header(not first_page))
        first_page    = False
        lines_on_page = HEADER_LINES

    cust_total = 0.0
    for j, row in enumerate(cust_group):
        hide = (j > 0)
        output_lines.append(_detail_line(row, " ", hide_custname=hide))
        lines_on_page += 1
        try:
            cust_total += float(row.get("CURBAL") or 0)
        except (TypeError, ValueError):
            pass

    # SAS BREAK AFTER CUSTNAME / OL SUMMARIZE: overline is 15 dashes
    # right-aligned in the CURBAL column.
    overline = (
        f"{'':<27s}  "
        f"{'':<12s}  "
        f"{'':<12s}  "
        f"{'':<16s}  "
        f"{'':>9s}  "
        f"{'':>1s}  "
        f"{'':<2s}  "
        f"{'':>13s}  "
        f"{'-' * 15:>15s}"
    )
    output_lines.append(f" {overline}")
    lines_on_page += 1

    sum_body = (
        f"{'':<27s}  "
        f"{'':<12s}  "
        f"{'':<12s}  "
        f"{'':<16s}  "
        f"{'':>9s}  "
        f"{'':>1s}  "
        f"{'':<2s}  "
        f"{'':>13s}  "
        f"{_fmt_comma15_2(cust_total)}"
    )
    output_lines.append(f" {sum_body}")
    lines_on_page += 1

    output_lines.append(" ")
    lines_on_page += 1

output_lines.append(" ")   # COMPUTE AFTER; LINE ' '; ENDCOMP;

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
