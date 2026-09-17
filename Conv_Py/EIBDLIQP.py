#!/usr/bin/env python3
"""
Program : EIBDLIQP.py
Invoke  : JCL job EIBDLIQP (SAS609 step). Owns ALL physical input paths
          for this job and its two dependency programs (EIBDRLFM and
          DALWPBBD), caches every .sas7bdat input to parquet ONCE here
          then calls the dependency modules passing in the
          already-cached parquet paths - this avoids each dependency
          resolving/caching its own inputs independently, since DALWPBBD
          in particular is shared by several unrelated jobs whose input
          dates vary.

Dependencies:
    DALWPBBD.build_savg_curn_dept(...) -> BNM_SAVG / BNM_CURN / BNM_DEPT
    EIBDRLFM.run_eibdrlfm(...)         -> FISS / NSRS report lines,
                                           LCR.*/NLF.* persisted extracts

JCL notes reproduced as comments (not executed):
    - DD1-DD4 delete/recreate the FISS/NSRS text datasets at the start of
      each run (IEFBR14 DISP=(MOD,DELETE,DELETE) then (NEW,CATLG,DELETE)).
      This is reproduced by truncating (mode 'w') FISS/NSRS at the start
      of this run and writing every subsequent line to the same in-memory
      list, matching the DISP=MOD append-within-run behaviour.
    - RUNSFTP EXEC COZBATCH is an external SFTP utility step, not SAS
      logic; only the FTP *command* file (SFTP01/&&FTPPUT) is built here.
      Actual file transfer is out of scope for this conversion and is
      left as a placeholder comment.
    - The second job step (EIIDLIQP, the PIBB counterpart) is a real,
      active JCL step in the same job. The "/*" lines in the JCL are
      in-stream data terminators (end of "EOB" blocks for the COZBATCH
      RUNSFTP steps), NOT a SAS block comment, so the PIBB step runs in
      production. It is reproduced below as a second execution of the
      same functions, using PIBB input paths and PIBB output targets,
      running after the PBB step.
"""

import gc
from pathlib import Path
from datetime import date

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from DALWPBBD import build_savg_curn_dept
from EIBDRLFM import run_eibdrlfm

# ============================================================================
# PATH CONFIGURATION - every physical input/output owned by THIS program
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# ---- Inputs (all .sas7bdat) ------------------------------------------------
# INPUT_SAVING_FILE  = STG_DIR / "MNITB" / "saving.sas7bdat"    # DEPOSIT.SAVING
# INPUT_CURRENT_FILE = STG_DIR / "MNITB" / "current.sas7bdat"   # DEPOSIT.CURRENT
# INPUT_FD_FILE      = STG_DIR / "MNIFD" / "fd.sas7bdat"        # FD.FD
# INPUT_CISDP_FILE   = STG_DIR / "CIS"   / "deposit.sas7bdat"   # CISDP.DEPOSIT
# INPUT_FORATE_FILE  = STG_DIR / "FCYCA" / "foratebkp.sas7bdat" # FORATE.FORATEBKP

# Saving / Current: single PBB-version files containing BOTH entities,
# discriminated by ENTITY_CD.
INPUT_SAVING_FILE  = STG_DIR / "MNITB" / "intg_dp_acct_saving_d16.sas7bdat"
INPUT_CURRENT_FILE = STG_DIR / "MNITB" / "intg_dp_acct_current_d16.sas7bdat"

# FD: cert-level (no ENTITY_CD) + account-level (has ACCTNO + ENTITY_CD).
INPUT_FD_CERT_FILE = STG_DIR / "MNIFD" / "enrh_dp_fd_cert_d16.sas7bdat"
INPUT_FD_ACCT_FILE = STG_DIR / "MNIFD" / "intg_dp_acct_fd_d16.sas7bdat"     # <- confirm actual name

# Shared PBB files (unchanged).
INPUT_CISDP_FILE   = STG_DIR / "CIS"      / "deposit.sas7bdat"
INPUT_FORATE_FILE  = STG_DIR / "EIBDLIQP" / "foratebkp.sas7bdat"

# ---- Parquet caches (converted once, shared by DALWPBBD + EIBDRLFM) -------
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBDLIQP"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# SAVING_CACHE  = CACHE_DIR / f"{INPUT_SAVING_FILE.stem}.parquet"
# CURRENT_CACHE = CACHE_DIR / f"{INPUT_CURRENT_FILE.stem}.parquet"
# FD_CACHE      = CACHE_DIR / f"{INPUT_FD_FILE.stem}.parquet"
# CISDP_CACHE   = CACHE_DIR / f"{INPUT_CISDP_FILE.stem}.parquet"
# FORATE_CACHE  = CACHE_DIR / f"{INPUT_FORATE_FILE.stem}.parquet"

# Raw caches (one per physical file, cached once)
SAVING_CACHE_FULL  = CACHE_DIR / "saving_full.parquet"
CURRENT_CACHE_FULL = CACHE_DIR / "current_full.parquet"
FD_CERT_CACHE      = CACHE_DIR / "fd_cert_full.parquet"
FD_ACCT_CACHE      = CACHE_DIR / "fd_acct_full.parquet"
CISDP_CACHE        = CACHE_DIR / "cisdp_deposit.parquet"
FORATE_CACHE       = CACHE_DIR / "foratebkp.parquet"

# Entity-split caches (derived from the raw caches)
SAVING_PBB_CACHE   = CACHE_DIR / "saving_pbb.parquet"
SAVING_PIBB_CACHE  = CACHE_DIR / "saving_pibb.parquet"
CURRENT_PBB_CACHE  = CACHE_DIR / "current_pbb.parquet"
CURRENT_PIBB_CACHE = CACHE_DIR / "current_pibb.parquet"
FD_PBB_CACHE       = CACHE_DIR / "fd_pbb.parquet"
FD_PIBB_CACHE      = CACHE_DIR / "fd_pibb.parquet"

# ---- Output cache dir for DALWPBBD's BNM_SAVG/BNM_CURN/BNM_DEPT parquet ----
DALWPBBD_OUTPUT_CACHE_DIR = BASE_DIR / "input" / "cache" / "DALWPBBD_PBB"

# ---- Outputs ----------------------------------------------------------------
OUTPUT_DIR = BASE_DIR / "output" / "EIBDLIQP"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LCR_OUTPUT_DIR = OUTPUT_DIR / "LCR"    # SAP.PBB.LCR.SASDATA.DAILY(+1)
NLF_OUTPUT_DIR = OUTPUT_DIR / "NLF"    # SAP.PBB.NLF.DAILY
FISS_FILE      = OUTPUT_DIR / "PBB_FISS_TEXT_DAILY.txt"   # SAP.PBB.FISS.TEXT.DAILY
NSRS_FILE      = OUTPUT_DIR / "PBB_NSRS_TEXT_DAILY.txt"   # SAP.PBB.NSRS.TEXT.DAILY
SFTP_CMD_FILE  = OUTPUT_DIR / "PBB_DAILY_NSRS_FTP.txt"    # &&FTPPUT (SFTP01)

for d in (LCR_OUTPUT_DIR, NLF_OUTPUT_DIR):
    d.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000

# ============================================================================
# PIBB (EIIDLIQP) PATH CONFIGURATION - second pass, same logic as PBB.
# CISDP.DEPOSIT and FORATE.FORATEBKP are PBB files shared by both passes,
# so they are NOT redeclared here; the existing CISDP_CACHE / FORATE_CACHE
# (and FORATE_MAP) are reused.
# ============================================================================
INPUT_SAVING_FILE_PIBB  = STG_DIR / "MNITB" / "saving_pibb.sas7bdat"
INPUT_CURRENT_FILE_PIBB = STG_DIR / "MNITB" / "current_pibb.sas7bdat"
INPUT_FD_FILE_PIBB      = STG_DIR / "MNIFD" / "fd_pibb.sas7bdat"

CACHE_DIR_PIBB = BASE_DIR / "input" / "cache" / "EIIDLIQP"
CACHE_DIR_PIBB.mkdir(parents=True, exist_ok=True)

SAVING_CACHE_PIBB  = CACHE_DIR_PIBB / f"{INPUT_SAVING_FILE_PIBB.stem}.parquet"
CURRENT_CACHE_PIBB = CACHE_DIR_PIBB / f"{INPUT_CURRENT_FILE_PIBB.stem}.parquet"
FD_CACHE_PIBB      = CACHE_DIR_PIBB / f"{INPUT_FD_FILE_PIBB.stem}.parquet"

DALWPBBD_OUTPUT_CACHE_DIR_PIBB = BASE_DIR / "input" / "cache" / "DALWPBBD_PIBB"

OUTPUT_DIR_PIBB     = BASE_DIR / "output" / "EIIDLIQP"
OUTPUT_DIR_PIBB.mkdir(parents=True, exist_ok=True)
LCR_OUTPUT_DIR_PIBB = OUTPUT_DIR_PIBB / "LCR"   # SAP.PIBB.LCR.SASDATA.DAILY(+1)
NLF_OUTPUT_DIR_PIBB = OUTPUT_DIR_PIBB / "NLF"   # SAP.PIBB.NLF.DAILY
FISS_FILE_PIBB      = OUTPUT_DIR_PIBB / "PIBB_FISS_TEXT_DAILY.txt"
NSRS_FILE_PIBB      = OUTPUT_DIR_PIBB / "PIBB_NSRS_TEXT_DAILY.txt"
SFTP_CMD_FILE_PIBB  = OUTPUT_DIR_PIBB / "PIBB_DAILY_NSRS_FTP.txt"  # &&FTPPUT2

for d in (LCR_OUTPUT_DIR_PIBB, NLF_OUTPUT_DIR_PIBB):
    d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet -- derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values()
REPTDATE = reptdate_values.reptdate

_day = REPTDATE.day
NOWK = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"

REPTYEAR = REPTDATE.strftime("%Y")
REPTMON  = REPTDATE.strftime("%m")
REPTDAY  = REPTDATE.strftime("%d")
RDATE    = REPTDATE.strftime("%d/%m/%y")

RPYR, RPMTH, RPDAY = REPTDATE.year, REPTDATE.month, REPTDATE.day

RD_DAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
if RPYR % 4 == 0:
    RD_DAYS[1] = 29

print(f"  RDATE   : {RDATE}")
print(f"  REPTMON/DAY/YEAR : {REPTMON}/{REPTDAY}/{REPTYEAR}   NOWK: {NOWK}")

# ============================================================================
# STEP 2: CACHE ALL .sas7bdat INPUTS TO PARQUET (EIIMRM01.py pattern)
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
        if schema is None:
            fields = []
            for col, dtype in chunk.dtypes.items():
                if dtype == 'object':
                    pa_type = pa.string()
                elif pd.api.types.is_integer_dtype(dtype):
                    pa_type = pa.int64()
                elif pd.api.types.is_float_dtype(dtype):
                    pa_type = pa.float64()
                else:
                    pa_type = pa.from_numpy_dtype(dtype)
                fields.append(pa.field(col, pa_type))
            schema = pa.schema(fields)
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
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


# print("\nStep 2: Caching input SAS datasets to Parquet...")
# _load_cached(INPUT_SAVING_FILE, SAVING_CACHE, "SAVING")
# _load_cached(INPUT_CURRENT_FILE, CURRENT_CACHE, "CURRENT")
# _load_cached(INPUT_FD_FILE, FD_CACHE, "FD")
# _load_cached(INPUT_CISDP_FILE, CISDP_CACHE, "CISDP")
# _load_cached(INPUT_FORATE_FILE, FORATE_CACHE, "FORATE")

print("\nStep 2: Caching input SAS datasets to Parquet...")
_load_cached(INPUT_SAVING_FILE,  SAVING_CACHE_FULL,  "SAVING")
_load_cached(INPUT_CURRENT_FILE, CURRENT_CACHE_FULL, "CURRENT")
_load_cached(INPUT_FD_CERT_FILE, FD_CERT_CACHE,      "FD-CERT")
_load_cached(INPUT_FD_ACCT_FILE, FD_ACCT_CACHE,      "FD-ACCT")
_load_cached(INPUT_CISDP_FILE,   CISDP_CACHE,        "CISDP")
_load_cached(INPUT_FORATE_FILE,  FORATE_CACHE,       "FORATE")

# ============================================================================
# STEP 2b: Split raw caches into PBB / PIBB entity-specific parquets
# ----------------------------------------------------------------------------
#  - SAVING / CURRENT: filter by ENTITY_CD ( 'PIBB' = PIBB, else PBB )
#  - FD: cert-level has no ENTITY_CD, so join to the FD account-level file
#        on ACCTNO to obtain each account's entity, then split.
# ============================================================================
print("\nStep 2b: Splitting caches by ENTITY_CD (PBB vs PIBB)...")

ENTITY_COL   = "ENTITY_CD"
PIBB_VALUE   = "PIBB"


def _split_by_entity(src_cache: Path, out_pbb: Path, out_pibb: Path, tag: str):
    con = duckdb.connect(database=":memory:")
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{src_cache.as_posix()}')
            WHERE CAST({ENTITY_COL} AS VARCHAR) <> '{PIBB_VALUE}'
        ) TO '{out_pbb.as_posix()}' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{src_cache.as_posix()}')
            WHERE CAST({ENTITY_COL} AS VARCHAR) = '{PIBB_VALUE}'
        ) TO '{out_pibb.as_posix()}' (FORMAT PARQUET)
    """)
    n_pbb  = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_pbb.as_posix()}')").fetchone()[0]
    n_pibb = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_pibb.as_posix()}')").fetchone()[0]
    con.close()
    print(f"  [{tag}] PBB rows: {n_pbb:,}   PIBB rows: {n_pibb:,}")

_split_by_entity(SAVING_CACHE_FULL,  SAVING_PBB_CACHE,  SAVING_PIBB_CACHE,  "SAVING")
_split_by_entity(CURRENT_CACHE_FULL, CURRENT_PBB_CACHE, CURRENT_PIBB_CACHE, "CURRENT")


def _split_fd_by_entity(cert_cache: Path, acct_cache: Path,
                        out_pbb: Path, out_pibb: Path):
    """FD cert-level has no ENTITY_CD; join to FD account-level (by ACCTNO)
    to obtain each account's entity, then split cert rows accordingly.
    Uses DISTINCT account numbers from the account-level file so a
    one-to-many relationship on the account side does not duplicate
    cert-level rows."""
    con = duckdb.connect(database=":memory:")
    for op, out_path in [("<>", out_pbb), ("=", out_pibb)]:
        con.execute(f"""
            COPY (
                SELECT c.*
                FROM read_parquet('{cert_cache.as_posix()}') c
                INNER JOIN (
                    SELECT DISTINCT ACCTNO
                    FROM read_parquet('{acct_cache.as_posix()}')
                    WHERE CAST({ENTITY_COL} AS VARCHAR) {op} '{PIBB_VALUE}'
                ) a
                ON c.ACCTNO = a.ACCTNO
            ) TO '{out_path.as_posix()}' (FORMAT PARQUET)
        """)
    n_pbb  = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_pbb.as_posix()}')").fetchone()[0]
    n_pibb = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_pibb.as_posix()}')").fetchone()[0]
    con.close()
    print(f"  [FD] PBB rows: {n_pbb:,}   PIBB rows: {n_pibb:,}")

_split_fd_by_entity(FD_CERT_CACHE, FD_ACCT_CACHE, FD_PBB_CACHE, FD_PIBB_CACHE)


# ============================================================================
# STEP 3: BUILD $FORATE LOOKUP
# PROC SORT DATA=FORATE.FORATEBKP OUT=FXRATE BY CURCODE DESCENDING REPTDATE
#   WHERE REPTDATE <= &RPTDTE; PROC SORT ... NODUPKEY BY CURCODE;
# ============================================================================
print("\nStep 3: Building $FORATE lookup...")

con = duckdb.connect(database=":memory:")
_forate_df = con.execute(f"""
    SELECT CURCODE, SPOTRATE FROM (
        SELECT
            CAST(CURCODE  AS VARCHAR) AS CURCODE,
            CAST(SPOTRATE AS DOUBLE)  AS SPOTRATE,
            CAST(REPTDATE AS DATE)    AS REPTDATE,
            ROW_NUMBER() OVER (PARTITION BY CURCODE ORDER BY REPTDATE DESC) AS RN
        FROM read_parquet('{FORATE_CACHE.as_posix()}')
        WHERE CAST(REPTDATE AS DATE) <= DATE '{REPTDATE.isoformat()}'
    ) WHERE RN = 1
""").pl()
con.close()

FORATE_MAP = dict(zip(_forate_df["CURCODE"].to_list(), _forate_df["SPOTRATE"].to_list()))
del _forate_df
print(f"  Currencies in $FORATE: {len(FORATE_MAP)}")

# ============================================================================
# STEP 4: DALWPBBD -- build BNM_SAVG / BNM_CURN / BNM_DEPT
# ============================================================================
print("\nStep 4: Running DALWPBBD (BNM_SAVG / BNM_CURN / BNM_DEPT)...")

# BNM_SAVG, BNM_CURN, BNM_DEPT = build_savg_curn_dept(
#     saving_cache=SAVING_CACHE,
#     current_cache=CURRENT_CACHE,
#     cisdp_cache=CISDP_CACHE,
#     reptmon=REPTMON,
#     nowk=NOWK,
#     output_cache_dir=DALWPBBD_OUTPUT_CACHE_DIR,
# )
BNM_SAVG, BNM_CURN, BNM_DEPT = build_savg_curn_dept(
    saving_cache=SAVING_PBB_CACHE,
    current_cache=CURRENT_PBB_CACHE,
    cisdp_cache=CISDP_CACHE,
    reptmon=REPTMON,
    nowk=NOWK,
    output_cache_dir=DALWPBBD_OUTPUT_CACHE_DIR,
)
print(f"  BNM_SAVG: {len(BNM_SAVG):,} rows   BNM_CURN: {len(BNM_CURN):,} rows")

# ============================================================================
# STEP 5: EIBDRLFM -- FISS/NSRS report + LCR/NLF extracts
# ============================================================================
print("\nStep 5: Running EIBDRLFM (FISS / NSRS / LCR / NLF)...")

# report_lines = run_eibdrlfm(
#     fd_cache=FD_CACHE,
#     current_cache=CURRENT_CACHE,
#     bnm_savg=BNM_SAVG,
#     bnm_curn=BNM_CURN,
#     forate_map=FORATE_MAP,
#     reptdate=REPTDATE,
#     rpyr=RPYR, rpmth=RPMTH, rpday=RPDAY,
#     rd_days=RD_DAYS,
#     reptday=REPTDAY, reptmon=REPTMON, reptyear=REPTYEAR,
#     lcr_output_dir=LCR_OUTPUT_DIR,
#     nlf_output_dir=NLF_OUTPUT_DIR,
# )

report_lines = run_eibdrlfm(
    fd_cache=FD_PBB_CACHE,
    current_cache=CURRENT_PBB_CACHE,
    bnm_savg=BNM_SAVG,
    bnm_curn=BNM_CURN,
    forate_map=FORATE_MAP,
    reptdate=REPTDATE,
    rpyr=RPYR, rpmth=RPMTH, rpday=RPDAY,
    rd_days=RD_DAYS,
    reptday=REPTDAY, reptmon=REPTMON, reptyear=REPTYEAR,
    lcr_output_dir=LCR_OUTPUT_DIR,
    nlf_output_dir=NLF_OUTPUT_DIR,
)

# ============================================================================
# STEP 6: WRITE FISS / NSRS  (DD1-DD4 delete+recreate -> truncate here;
# RECFM=FB (no ASA), plain fixed/';'-delimited text)
# ============================================================================
print("\nStep 6: Writing FISS / NSRS...")

with open(FISS_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines["FISS"]:
        fh.write(ln + "\n")

with open(NSRS_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines["NSRS"]:
        fh.write(ln + "\n")

print(f"  FISS lines: {len(report_lines['FISS']):,}  -> {FISS_FILE}")
print(f"  NSRS lines: {len(report_lines['NSRS']):,}  -> {NSRS_FILE}")

# ============================================================================
# STEP 7: SFTP01 command file  (the actual RUNSFTP/COZBATCH transfer step
# is an external JCL utility, not SAS logic - only the FTP command text
# is produced here; the transfer itself is out of scope for this
# conversion)
# ============================================================================
sftp_cmd = (
    'CD "FD-BNM REPORTING/PBB/BNM RPTG/BNM RPTG_SUB"\n'
    f"PUT {NSRS_FILE.as_posix()} PBB_DAILY_NSRS_{REPTDAY}.TXT\n"
)
with open(SFTP_CMD_FILE, "w", encoding="latin1") as fh:
    fh.write(sftp_cmd)
print(f"  SFTP command file -> {SFTP_CMD_FILE}")
# Placeholder: actual SFTP transfer (RUNSFTP EXEC COZBATCH) is an external
# job step and is not executed by this program.

print("\nEIBDLIQP (PBB) complete.")

# ============================================================================
# STEP 8: EIIDLIQP - PIBB pass (active second execution of the same logic)
# Runs strictly AFTER the PBB pass above. CISDP.DEPOSIT and
# FORATE.FORATEBKP are the PBB files, shared; their caches / FORATE_MAP
# from Steps 2-3 are reused as-is. Only SAVING / CURRENT / FD differ.
# ============================================================================
print("\n" + "=" * 70)
print("EIIDLIQP (PIBB) - starting")
print("=" * 70)

# print("\nStep 8a: Caching PIBB input SAS datasets to Parquet...")
# _load_cached(INPUT_SAVING_FILE_PIBB,  SAVING_CACHE_PIBB,  "SAVING-PIBB")
# _load_cached(INPUT_CURRENT_FILE_PIBB, CURRENT_CACHE_PIBB, "CURRENT-PIBB")
# _load_cached(INPUT_FD_FILE_PIBB,      FD_CACHE_PIBB,      "FD-PIBB")

print("\nStep 8a: Using PIBB entity-split caches (from Step 2b) ...")
SAVING_CACHE_PIBB  = SAVING_PIBB_CACHE
CURRENT_CACHE_PIBB = CURRENT_PIBB_CACHE
FD_CACHE_PIBB      = FD_PIBB_CACHE

# Step 8b: $FORATE is identical to PBB (same FORATE.FORATEBKP) -> reuse
# FORATE_MAP built in Step 3. No re-query needed.

# ----------------------------------------------------------------------------
# Step 8c: DALWPBBD (PIBB) - BNM_SAVG / BNM_CURN / BNM_DEPT for PIBB
# ----------------------------------------------------------------------------
print("\nStep 8c: Running DALWPBBD (PIBB) ...")
BNM_SAVG_PIBB, BNM_CURN_PIBB, BNM_DEPT_PIBB = build_savg_curn_dept(
    saving_cache=SAVING_CACHE_PIBB,
    current_cache=CURRENT_CACHE_PIBB,
    cisdp_cache=CISDP_CACHE,                       # shared PBB file
    reptmon=REPTMON,
    nowk=NOWK,
    output_cache_dir=DALWPBBD_OUTPUT_CACHE_DIR_PIBB,
)
print(f"  PIBB BNM_SAVG: {len(BNM_SAVG_PIBB):,} rows   "
      f"PIBB BNM_CURN: {len(BNM_CURN_PIBB):,} rows")

# ----------------------------------------------------------------------------
# Step 8d: EIBDRLFM (PIBB) - FISS / NSRS / LCR / NLF for PIBB
# ----------------------------------------------------------------------------
print("\nStep 8d: Running EIBDRLFM (PIBB) ...")
report_lines_pibb = run_eibdrlfm(
    fd_cache=FD_CACHE_PIBB,
    current_cache=CURRENT_CACHE_PIBB,
    bnm_savg=BNM_SAVG_PIBB,
    bnm_curn=BNM_CURN_PIBB,
    forate_map=FORATE_MAP,                         # shared from PBB pass
    reptdate=REPTDATE,
    rpyr=RPYR, rpmth=RPMTH, rpday=RPDAY,
    rd_days=RD_DAYS,
    reptday=REPTDAY, reptmon=REPTMON, reptyear=REPTYEAR,
    lcr_output_dir=LCR_OUTPUT_DIR_PIBB,
    nlf_output_dir=NLF_OUTPUT_DIR_PIBB,
)

# ----------------------------------------------------------------------------
# Step 8e: Write PIBB FISS / NSRS
# ----------------------------------------------------------------------------
print("\nStep 8e: Writing PIBB FISS / NSRS ...")
with open(FISS_FILE_PIBB, "w", encoding="latin1") as fh:
    for ln in report_lines_pibb["FISS"]:
        fh.write(ln + "\n")

with open(NSRS_FILE_PIBB, "w", encoding="latin1") as fh:
    for ln in report_lines_pibb["NSRS"]:
        fh.write(ln + "\n")

print(f"  PIBB FISS lines: {len(report_lines_pibb['FISS']):,}  -> {FISS_FILE_PIBB}")
print(f"  PIBB NSRS lines: {len(report_lines_pibb['NSRS']):,}  -> {NSRS_FILE_PIBB}")

# ----------------------------------------------------------------------------
# Step 8f: SFTP02 command file (actual COZBATCH RUNSFTP transfer out of scope)
# ----------------------------------------------------------------------------
sftp_cmd_pibb = (
    'CD "FD-BNM REPORTING/PIBB/BNM RPTG/BNM RPTG_SUB"\n'
    f"PUT {NSRS_FILE_PIBB.as_posix()} PIBB_DAILY_NSRS_{REPTDAY}.TXT\n"
)
with open(SFTP_CMD_FILE_PIBB, "w", encoding="latin1") as fh:
    fh.write(sftp_cmd_pibb)
print(f"  PIBB SFTP command file -> {SFTP_CMD_FILE_PIBB}")

print("\nEIIDLIQP (PIBB) complete.")
