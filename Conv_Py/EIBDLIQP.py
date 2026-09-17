#!/usr/bin/env python3
"""
Program : EIBDLIQP.py
Invoke  : JCL job EIBDLIQP (SAS609 step). Owns ALL physical input paths
          for this job and its two dependency programs (EIBDRLFM and
          DALWPBBD), caches every .sas7bdat input to parquet ONCE here
          (per EIIMRM01.py's cache pattern), then calls the dependency
          modules passing in the already-cached parquet paths - this
          avoids each dependency resolving/caching its own inputs
          independently, since DALWPBBD in particular is shared by
          several unrelated jobs whose input dates vary.

Dependencies:
    DALWPBBD.build_savg_curn_dept(...) -> BNM_SAVG / BNM_CURN / BNM_DEPT
    EIBDRLFM.run_eibdrlfm(...)         -> FISS / NSRS report lines,
                                           LCR.*/NLF.* persisted extracts

Report date:
    No reptdate.parquet exists; the report date is derived via
    REPTDATE.get_reptdate_values(), exactly as EIIMRM01.py / DALWPBBD.py
    already do (this replaces "DATA BNM.REPTDATE; SET DEPOSIT.REPTDATE").

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
    - The second job step (EIIDLIQP, the PIBB counterpart) is wrapped in
      a SAS/JCL comment block ("/* ... */") in the original source,
      i.e. DISABLED in production. It is reproduced below as a commented
      block only, calling the same functions with PIBB paths, and is
      NOT executed.
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
INPUT_SAVING_FILE  = STG_DIR / "MNITB" / "saving.sas7bdat"    # DEPOSIT.SAVING
INPUT_CURRENT_FILE = STG_DIR / "MNITB" / "current.sas7bdat"   # DEPOSIT.CURRENT
INPUT_FD_FILE      = STG_DIR / "MNIFD" / "fd.sas7bdat"        # FD.FD
INPUT_CISDP_FILE   = STG_DIR / "CIS"   / "deposit.sas7bdat"   # CISDP.DEPOSIT
INPUT_FORATE_FILE  = STG_DIR / "FCYCA" / "foratebkp.sas7bdat" # FORATE.FORATEBKP

# ---- Parquet caches (converted once, shared by DALWPBBD + EIBDRLFM) -------
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBDLIQP"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

SAVING_CACHE  = CACHE_DIR / f"{INPUT_SAVING_FILE.stem}.parquet"
CURRENT_CACHE = CACHE_DIR / f"{INPUT_CURRENT_FILE.stem}.parquet"
FD_CACHE      = CACHE_DIR / f"{INPUT_FD_FILE.stem}.parquet"
CISDP_CACHE   = CACHE_DIR / f"{INPUT_CISDP_FILE.stem}.parquet"
FORATE_CACHE  = CACHE_DIR / f"{INPUT_FORATE_FILE.stem}.parquet"

# ---- Output cache dir for DALWPBBD's BNM_SAVG/BNM_CURN/BNM_DEPT parquet ----
DALWPBBD_OUTPUT_CACHE_DIR = BASE_DIR / "input" / "cache" / "DALWPBBD"

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


print("\nStep 2: Caching input SAS datasets to Parquet...")
_load_cached(INPUT_SAVING_FILE, SAVING_CACHE, "SAVING")
_load_cached(INPUT_CURRENT_FILE, CURRENT_CACHE, "CURRENT")
_load_cached(INPUT_FD_FILE, FD_CACHE, "FD")
_load_cached(INPUT_CISDP_FILE, CISDP_CACHE, "CISDP")
_load_cached(INPUT_FORATE_FILE, FORATE_CACHE, "FORATE")

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

BNM_SAVG, BNM_CURN, BNM_DEPT = build_savg_curn_dept(
    saving_cache=SAVING_CACHE,
    current_cache=CURRENT_CACHE,
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

report_lines = run_eibdrlfm(
    fd_cache=FD_CACHE,
    current_cache=CURRENT_CACHE,
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

print("\nEIBDLIQP complete.")

# ============================================================================
# DISABLED IN PRODUCTION - the SAS source wraps this whole second step
# (EIIDLIQP, the PIBB counterpart of this job) inside a "/* ... */" SAS
# comment block, i.e. it never actually runs. Reproduced here as a
# commented placeholder only, for documentation parity:
#
# INPUT_SAVING_FILE_PIBB  = STG_DIR / "MNITB" / "saving_pibb.sas7bdat"
# INPUT_CURRENT_FILE_PIBB = STG_DIR / "MNITB" / "current_pibb.sas7bdat"
# INPUT_FD_FILE_PIBB      = STG_DIR / "MNIFD" / "fd_pibb.sas7bdat"
# INPUT_CISDP_FILE_PIBB   = STG_DIR / "CIS"   / "deposit.sas7bdat"  # shared
# INPUT_FORATE_FILE_PIBB  = STG_DIR / "FCYCA" / "foratebkp.sas7bdat"  # shared
# ... (same caching / FORATE / DALWPBBD / EIBDRLFM calls with PIBB paths,
#      writing to SAP.PIBB.FISS.TEXT.DAILY / SAP.PIBB.NSRS.TEXT.DAILY,
#      SAP.PIBB.LCR.SASDATA.DAILY(+1), NLF library on the PIBB side) ...
# ============================================================================
