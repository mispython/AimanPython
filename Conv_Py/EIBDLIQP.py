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
    - RUNSFTP EXEC COZBATCH (SFTP01/SFTP02) is reproduced in Step 9: the
      NSRS text files are uploaded to the DRR server via paramiko, using
      the same remote folders and file names as the JCL PUT statements.
    - The report date comes from the DP batch date (ctl_dwh_batch_dttm).
      All parquet caches live in a temporary folder that is deleted
      before the SFTP step.
    - The second job step (EIIDLIQP, the PIBB counterpart) is a real,
      active JCL step in the same job. The "/*" lines in the JCL are
      in-stream data terminators (end of "EOB" blocks for the COZBATCH
      RUNSFTP steps), NOT a SAS block comment, so the PIBB step runs in
      production. It is reproduced below as a second execution of the
      same functions, using PIBB input paths and PIBB output targets,
      running after the PBB step.
"""

import gc
import sys
import tempfile
from pathlib import Path
from datetime import date

import duckdb
import pandas as pd
import polars as pl
import paramiko
import pyarrow as pa
import pyarrow.parquet as pq

from DALWPBBD import build_savg_curn_dept
from EIBDRLFM import run_eibdrlfm

# ============================================================================
# CONTROL FILES (batch date + SFTP) - DWH control library
# ============================================================================
CTL_BATCH_DTTM    = Path("/sasdata/dwh/control/ctl_dwh_batch_dttm.sas7bdat")
BATCH_SOURCE      = "DP"          # SOURCE_SYSTEM_CD used for the report date
SFTP_CONTROL_FILE = Path("/sasdata/dwh/control/ctl_dwh_sftp_info.sas7bdat")

# DRR (Data Report Repository) - folders from the JCL "CD" statements
DRR_HOST_DESC       = "Data Report Repository (DRR)"
DRR_REMOTE_DIR_PBB  = "FD-BNM REPORTING/PBB/BNM RPTG/BNM RPTG_SUB"
DRR_REMOTE_DIR_PIBB = "FD-BNM REPORTING/PIBB/BNM RPTG/BNM RPTG_SUB"


def get_batch_date(source_system_cd: str) -> date:
    """Read BATCH_DTTM for the given SOURCE_SYSTEM_CD from the control file."""
    ctl = pd.read_sas(str(CTL_BATCH_DTTM), encoding="latin1")
    ctl["SOURCE_SYSTEM_CD"] = ctl["SOURCE_SYSTEM_CD"].astype(str).str.strip()
    hit = ctl.loc[ctl["SOURCE_SYSTEM_CD"] == source_system_cd, "BATCH_DTTM"]
    if hit.empty:
        raise ValueError(
            f"SOURCE_SYSTEM_CD '{source_system_cd}' not found in {CTL_BATCH_DTTM}"
        )
    return pd.Timestamp(hit.iloc[0]).date()


def get_sftp_info(host_desc: str):
    """Read one SFTP configuration row directly from the DWH control file
    (same approach as EIWFRMCR.py)."""
    if not SFTP_CONTROL_FILE.exists():
        raise FileNotFoundError(f"SFTP control file not found: {SFTP_CONTROL_FILE}")

    ctl = pd.read_sas(SFTP_CONTROL_FILE, format="sas7bdat", encoding="utf-8")
    ctl.columns = [str(c).upper().strip() for c in ctl.columns]

    required = {"HOST_DESC", "SFTP_ID", "SFTP_PW", "HOST_IP"}
    missing_columns = sorted(required.difference(ctl.columns))
    if missing_columns:
        raise RuntimeError(
            "SFTP control file is missing columns: " + ", ".join(missing_columns)
        )

    descriptions = ctl["HOST_DESC"].fillna("").astype(str).str.strip()
    rows = ctl[descriptions.eq(host_desc)]
    if len(rows) != 1:
        raise RuntimeError(
            f"Expected exactly one SFTP row for '{host_desc}'; found {len(rows)}"
        )

    row = rows.iloc[0]

    # SAS character values can contain fixed-width padding.
    clean = lambda value: "".join(str(value).split()) if pd.notna(value) else ""
    sftp_id = clean(row["SFTP_ID"])
    sftp_pw = clean(row["SFTP_PW"])
    host_ip = clean(row["HOST_IP"])
    host_key = clean(row["HOST_KEY"]) if "HOST_KEY" in ctl.columns else ""

    empty_values = [
        name for name, value in (
            ("SFTP_ID", sftp_id), ("SFTP_PW", sftp_pw), ("HOST_IP", host_ip)
        ) if not value
    ]
    if empty_values:
        raise RuntimeError(
            f"SFTP row for '{host_desc}' has blank values: {', '.join(empty_values)}"
        )

    return sftp_id, sftp_pw, host_ip, host_key


def sftp_upload_to_drr(uploads: list) -> None:
    """Upload files to the DRR server.

    uploads: list of (local_path: Path, remote_dir: str, remote_name: str).
    Equivalent of the JCL RUNSFTP steps (CD <remote_dir> / PUT <dataset> <name>).
    The remote folder is addressed by path (relative to the login directory,
    exactly as the JCL CD), so no session-level chdir state is carried between
    the PBB and PIBB uploads.
    """
    sftp_id, sftp_pw, host_ip, _host_key = get_sftp_info(DRR_HOST_DESC)

    ssh = paramiko.SSHClient()
    # HOST_KEY is blank for the DRR row in the control file, so strict host-key
    # pinning is not possible; AutoAddPolicy is used (same as EIWFRMCR.py).
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print(f"  Connecting to DRR host {host_ip} as {sftp_id} ...")
    ssh.connect(hostname=host_ip, username=sftp_id, password=sftp_pw)
    sftp = ssh.open_sftp()
    try:
        for local_path, remote_dir, remote_name in uploads:
            try:
                sftp.stat(remote_dir)
            except IOError:
                sys.exit(f"Aborting: Remote DRR folder '{remote_dir}' not found")
            remote_path = f"{remote_dir}/{remote_name}"
            print(f"  SFTP upload: {local_path.name} -> {remote_path}")
            sftp.put(str(local_path), remote_path)
    finally:
        sftp.close()
        ssh.close()
    print("  DRR upload complete.")


# ============================================================================
# STEP 1: REPORT DATE  (DP batch date from ctl_dwh_batch_dttm)
# ============================================================================
print("Step 1: Deriving report date from DP batch date...")

REPTDATE = get_batch_date(BATCH_SOURCE)

_day = REPTDATE.day
NOWK = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"

REPTYEAR = REPTDATE.strftime("%Y")
REPTYY   = REPTDATE.strftime("%y")
REPTMON  = REPTDATE.strftime("%m")
REPTDAY  = REPTDATE.strftime("%d")
RDATE    = REPTDATE.strftime("%d/%m/%y")

RPYR, RPMTH, RPDAY = REPTDATE.year, REPTDATE.month, REPTDATE.day

RD_DAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
if RPYR % 4 == 0:
    RD_DAYS[1] = 29

print(f"  REPTDATE: {REPTDATE}")
print(f"  RDATE   : {RDATE}")
print(f"  REPTMON/DAY/YEAR : {REPTMON}/{REPTDAY}/{REPTYEAR}   NOWK: {NOWK}")

# ============================================================================
# PATH CONFIGURATION - every physical input/output owned by THIS program
# ============================================================================
# Production Path
BASE_DIR            = Path("/sas/deposit/dwh")

# Saving / Current: single PBB-version files containing BOTH entities,
# discriminated by ENTITY_CD.
INPUT_SAVING_FILE   = BASE_DIR / "integration" / f"intg_dp_acct_saving_d{REPTDAY}.sas7bdat"
INPUT_CURRENT_FILE  = BASE_DIR / "integration" / f"intg_dp_acct_current_d{REPTDAY}.sas7bdat"

# FD: cert-level (no ENTITY_CD) + account-level (has ACCTNO + ENTITY_CD).
INPUT_FD_CERT_FILE  = BASE_DIR / "enrichment"  / f"enrh_dp_fd_cert_d{REPTDAY}.sas7bdat"
INPUT_FD_ACCT_FILE  = BASE_DIR / "integration" / f"intg_dp_acct_fd_d{REPTDAY}.sas7bdat"

# Shared PBB files (unchanged).
# INPUT_CISDP_FILE    = Path("/sas/cis/mart/enrichment") / "enrh_exdwh_cisr1.sas7bdat"
INPUT_CISDP_SA_FILE = Path("/dwh/rsd_cis") / f"cisr1sa{REPTYY}{REPTMON}{REPTDAY}.sas7bdat"
INPUT_CISDP_FD_FILE = Path("/dwh/rsd_cis") / f"cisr1fd{REPTYY}{REPTMON}{REPTDAY}.sas7bdat"
INPUT_FORATE_FILE   = Path("/exdwh/host/forate") / "foratebkp.sas7bdat"

# # Testing Path
# BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
# STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# # ---- Inputs (all .sas7bdat) ------------------------------------------------
# # Saving / Current: single PBB-version files containing BOTH entities,
# # discriminated by ENTITY_CD.
# INPUT_SAVING_FILE  = STG_DIR / "MNITB" / f"intg_dp_acct_saving_d{REPTDAY}.sas7bdat"
# INPUT_CURRENT_FILE = STG_DIR / "MNITB" / f"intg_dp_acct_current_d{REPTDAY}.sas7bdat"

# # FD: cert-level (no ENTITY_CD) + account-level (has ACCTNO + ENTITY_CD).
# INPUT_FD_CERT_FILE = STG_DIR / "MNIFD" / f"enrh_dp_fd_cert_d{REPTDAY}.sas7bdat"
# INPUT_FD_ACCT_FILE = STG_DIR / "MNITB" / f"intg_dp_acct_fd_d{REPTDAY}.sas7bdat"

# # Shared PBB files (unchanged).
# INPUT_CISDP_FILE   = STG_DIR / "CIS"      / "deposit.sas7bdat"
# INPUT_FORATE_FILE  = STG_DIR / "EIBDLIQP" / "foratebkp.sas7bdat"

# ---- Parquet caches (converted once, shared by DALWPBBD + EIBDRLFM) -------
# Temporary folder: parquet is used only for fast reads during this run and is
# deleted (see _TMP.cleanup() before Step 9). Nothing is kept between runs.
TEMP_DIR = None   # None = system temp; set a path if the default temp area is too small
_TMP = tempfile.TemporaryDirectory(dir=TEMP_DIR, prefix="EIBDLIQP_")
CACHE_DIR = Path(_TMP.name)

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

# ---- Outputs ----------------------------------------------------------------
# Production Path
BASE_OUTPUT_DIR = Path("host_pq/mis")
OUTPUT_DIR      = BASE_OUTPUT_DIR / "output" / "report" / "EIBDLIQP"
# LCR_OUTPUT_DIR  = BASE_OUTPUT_DIR / "input" / "d_lcr"                 # SAP.PBB.LCR.SASDATA.DAILY(+1)
# NLF_OUTPUT_DIR  = BASE_OUTPUT_DIR / "input" / "d_nlf"                 # SAP.PBB.NLF.DAILY
LCR_OUTPUT_DIR  = Path("/exdwh/host/lcr") / "d_sasdata"           # SAP.PBB.LCR.SASDATA.DAILY(+1)
NLF_OUTPUT_DIR  = Path("/exdwh/host") / "d_nlf"                   # SAP.PBB.NLF.DAILY
FISS_FILE       = OUTPUT_DIR / f"PBB_FISS_TEXT_DAILY_{REPTDAY}.txt"   # SAP.PBB.FISS.TEXT.DAILY
NSRS_FILE       = OUTPUT_DIR / f"PBB_NSRS_TEXT_DAILY_{REPTDAY}.txt"   # SAP.PBB.NSRS.TEXT.DAILY

for d in (BASE_OUTPUT_DIR, OUTPUT_DIR, LCR_OUTPUT_DIR, NLF_OUTPUT_DIR):
    d.mkdir(parents=True, exist_ok=True)

# # Testing Path
# OUTPUT_DIR = BASE_DIR / "output" / "EIBDLIQP"
# OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# LCR_OUTPUT_DIR = OUTPUT_DIR / "LCR"    # SAP.PBB.LCR.SASDATA.DAILY(+1)
# NLF_OUTPUT_DIR = OUTPUT_DIR / "NLF"    # SAP.PBB.NLF.DAILY
# FISS_FILE      = OUTPUT_DIR / f"PBB_FISS_TEXT_DAILY_{REPTDAY}.txt"   # SAP.PBB.FISS.TEXT.DAILY
# NSRS_FILE      = OUTPUT_DIR / f"PBB_NSRS_TEXT_DAILY_{REPTDAY}.txt"   # SAP.PBB.NSRS.TEXT.DAILY

# for d in (LCR_OUTPUT_DIR, NLF_OUTPUT_DIR):
#     d.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000

# ============================================================================
# PIBB (EIIDLIQP) PATH CONFIGURATION - second pass, same logic as PBB.
# CISDP.DEPOSIT and FORATE.FORATEBKP are PBB files shared by both passes,
# so they are NOT redeclared here; the existing CISDP_CACHE / FORATE_CACHE
# (and FORATE_MAP) are reused.
# ============================================================================
# Production Path
BASE_OUTPUT_DIR     = Path("host_pq/mis")
OUTPUT_DIR_PIBB     = BASE_OUTPUT_DIR / "output" / "report" / "EIIDLIQP"
# LCR_OUTPUT_DIR_PIBB = BASE_OUTPUT_DIR / "input" / "d_ilcr"    # SAP.PIBB.LCR.SASDATA.DAILY(+1)
# NLF_OUTPUT_DIR_PIBB = BASE_OUTPUT_DIR / "input" / "d_inlf"    # SAP.PIBB.NLF.DAILY
LCR_OUTPUT_DIR_PIBB  = Path("/exdwh/host/lcr") / "d_sasdata" / "pibb"       # SAP.PIBB.LCR.SASDATA.DAILY(+1)
NLF_OUTPUT_DIR_PIBB  = Path("/exdwh/host") / "d_nlf" / "pibb"               # SAP.PIBB.NLF.DAILY
FISS_FILE_PIBB      = OUTPUT_DIR / f"PIBB_FISS_TEXT_DAILY_{REPTDAY}.txt"    # SAP.PBB.FISS.TEXT.DAILY
NSRS_FILE_PIBB      = OUTPUT_DIR / f"PIBB_NSRS_TEXT_DAILY_{REPTDAY}.txt"    # SAP.PBB.NSRS.TEXT.DAILY

for d in (BASE_OUTPUT_DIR, OUTPUT_DIR_PIBB, LCR_OUTPUT_DIR_PIBB, NLF_OUTPUT_DIR_PIBB):
    d.mkdir(parents=True, exist_ok=True)

# # Testing Path
# OUTPUT_DIR_PIBB     = BASE_DIR / "output" / "EIIDLIQP"
# OUTPUT_DIR_PIBB.mkdir(parents=True, exist_ok=True)
# LCR_OUTPUT_DIR_PIBB = OUTPUT_DIR_PIBB / "LCR"   # SAP.PIBB.LCR.SASDATA.DAILY(+1)
# NLF_OUTPUT_DIR_PIBB = OUTPUT_DIR_PIBB / "NLF"   # SAP.PIBB.NLF.DAILY
# FISS_FILE_PIBB      = OUTPUT_DIR_PIBB / f"PIBB_FISS_TEXT_DAILY_{REPTDAY}.txt"
# NSRS_FILE_PIBB      = OUTPUT_DIR_PIBB / f"PIBB_NSRS_TEXT_DAILY_{REPTDAY}.txt"

# for d in (LCR_OUTPUT_DIR_PIBB, NLF_OUTPUT_DIR_PIBB):
#     d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# HELPER: persist DataFrames returned by EIBDRLFM (LCR / NLF DD equivalents)
# ============================================================================
def _persist_frames(frames: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for stem, df in frames.items():
        df.write_parquet(out_dir / f"{stem}.parquet")


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


def _split_is_fresh(src: Path, *outputs: Path) -> bool:
    if not all(p.exists() for p in outputs):
        return False
    src_mtime = src.stat().st_mtime
    return all(p.stat().st_mtime >= src_mtime for p in outputs)


def _split_by_entity(src_cache: Path, out_pbb: Path, out_pibb: Path, tag: str):
    if _split_is_fresh(src_cache, out_pbb, out_pibb):
        print(f"  [{tag}] Split fresh - skipping.")
        return
    con = duckdb.connect(database=":memory:")
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{src_cache.as_posix()}')
            WHERE {ENTITY_COL} IS NULL
               OR TRIM(UPPER(CAST({ENTITY_COL} AS VARCHAR))) <> '{PIBB_VALUE}'
        ) TO '{out_pbb.as_posix()}' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{src_cache.as_posix()}')
            WHERE TRIM(UPPER(CAST({ENTITY_COL} AS VARCHAR))) = '{PIBB_VALUE}'
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
    if _split_is_fresh(cert_cache, out_pbb, out_pibb) and \
       _split_is_fresh(acct_cache, out_pbb, out_pibb):
        print("  [FD] Split fresh - skipping.")
        return
    con = duckdb.connect(database=":memory:")
    # (filter_sql, amtind_value, out_path): 'D' for PBB, 'I' for PIBB.
    for filter_sql, amtind_value, out_path in [
        (f"({ENTITY_COL} IS NULL OR "
         f"TRIM(UPPER(CAST({ENTITY_COL} AS VARCHAR))) <> '{PIBB_VALUE}')", 'D', out_pbb),
        (f"(TRIM(UPPER(CAST({ENTITY_COL} AS VARCHAR))) = '{PIBB_VALUE}')",   'I', out_pibb),
    ]:
        con.execute(f"""
            COPY (
                SELECT c.*,
                       a.CUST_NAME,
                       CAST('{amtind_value}' AS VARCHAR) AS AMTIND
                FROM read_parquet('{cert_cache.as_posix()}') c
                INNER JOIN (
                    SELECT
                        CAST(ACCTNO AS BIGINT) AS acct_key,
                        ANY_VALUE(NAME)        AS CUST_NAME
                    FROM read_parquet('{acct_cache.as_posix()}')
                    WHERE {filter_sql}
                    GROUP BY 1
                ) a
                    ON CAST(c.ACCT_NUM AS BIGINT) = a.acct_key
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
            (DATE '1960-01-01' + CAST(CAST(REPTDATE AS INTEGER) AS INTEGER) * INTERVAL 1 DAY) AS REPTDATE,
            ROW_NUMBER() OVER (
                PARTITION BY CURCODE
                ORDER BY CAST(REPTDATE AS INTEGER) DESC
            ) AS RN
        FROM read_parquet('{FORATE_CACHE.as_posix()}')
        WHERE (DATE '1960-01-01'
               + CAST(CAST(REPTDATE AS INTEGER) AS INTEGER) * INTERVAL 1 DAY)
              <= DATE '{REPTDATE.isoformat()}'
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
    saving_cache=SAVING_PBB_CACHE,
    current_cache=CURRENT_PBB_CACHE,
    cisdp_cache=CISDP_CACHE,
)
print(f"  BNM_SAVG: {len(BNM_SAVG):,} rows   BNM_CURN: {len(BNM_CURN):,} rows")

# ============================================================================
# STEP 5: EIBDRLFM -- FISS/NSRS report + LCR/NLF extracts
# ============================================================================
print("\nStep 5: Running EIBDRLFM (FISS / NSRS / LCR / NLF)...")

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
)
_persist_frames(report_lines["LCR"], LCR_OUTPUT_DIR)
_persist_frames(report_lines["NLF"], NLF_OUTPUT_DIR)

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
# STEP 7 (SFTP01) is executed in Step 9 together with the PIBB upload.
# ============================================================================
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

print("\nStep 8a: Using PIBB entity-split caches (from Step 2b) ...")
# in Step 2b; the PIBB pass below consumes them directly.

# Step 8b: $FORATE is identical to PBB (same FORATE.FORATEBKP) -> reuse
# FORATE_MAP built in Step 3. No re-query needed.

# ----------------------------------------------------------------------------
# Step 8c: DALWPBBD (PIBB) - BNM_SAVG / BNM_CURN / BNM_DEPT for PIBB
# ----------------------------------------------------------------------------
print("\nStep 8c: Running DALWPBBD (PIBB) ...")
BNM_SAVG_PIBB, BNM_CURN_PIBB, BNM_DEPT_PIBB = build_savg_curn_dept(
    saving_cache=SAVING_PIBB_CACHE,
    current_cache=CURRENT_PIBB_CACHE,
    cisdp_cache=CISDP_CACHE,                       # shared PBB file
)
print(f"  PIBB BNM_SAVG: {len(BNM_SAVG_PIBB):,} rows   "
      f"PIBB BNM_CURN: {len(BNM_CURN_PIBB):,} rows")

# ----------------------------------------------------------------------------
# Step 8d: EIBDRLFM (PIBB) - FISS / NSRS / LCR / NLF for PIBB
# ----------------------------------------------------------------------------
print("\nStep 8d: Running EIBDRLFM (PIBB) ...")
report_lines_pibb = run_eibdrlfm(
    fd_cache=FD_PIBB_CACHE,
    current_cache=CURRENT_PIBB_CACHE,
    bnm_savg=BNM_SAVG_PIBB,
    bnm_curn=BNM_CURN_PIBB,
    forate_map=FORATE_MAP,                         # shared from PBB pass
    reptdate=REPTDATE,
    rpyr=RPYR, rpmth=RPMTH, rpday=RPDAY,
    rd_days=RD_DAYS,
    reptday=REPTDAY, reptmon=REPTMON, reptyear=REPTYEAR,
)
_persist_frames(report_lines_pibb["LCR"], LCR_OUTPUT_DIR_PIBB)
_persist_frames(report_lines_pibb["NLF"], NLF_OUTPUT_DIR_PIBB)

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

print("\nEIIDLIQP (PIBB) complete.")

# ============================================================================
# CLEANUP: remove the temporary parquet caches (not kept between runs).
# Done before the upload so they are deleted even if the SFTP step fails.
# ============================================================================
_TMP.cleanup()
print("\nTemporary parquet caches removed.")

# ============================================================================
# STEP 9: SFTP NSRS FILES TO DATA REPORT REPOSITORY SYSTEM (DRR)
# Equivalent of the two JCL RUNSFTP (COZBATCH) steps:
#   CD "FD-BNM REPORTING/PBB/BNM RPTG/BNM RPTG_SUB"
#   PUT //SAP.PBB.NSRS.TEXT.DAILY  PBB_DAILY_NSRS_&REPTDAY..TXT
#   CD "FD-BNM REPORTING/PIBB/BNM RPTG/BNM RPTG_SUB"
#   PUT //SAP.PIBB.NSRS.TEXT.DAILY PIBB_DAILY_NSRS_&REPTDAY..TXT
# ============================================================================
print("\nStep 9: Uploading NSRS files to DRR via SFTP...")
sftp_upload_to_drr([
    (NSRS_FILE,      DRR_REMOTE_DIR_PBB,  f"PBB_DAILY_NSRS_{REPTDAY}.TXT"),
    (NSRS_FILE_PIBB, DRR_REMOTE_DIR_PIBB, f"PIBB_DAILY_NSRS_{REPTDAY}.TXT"),
])

print("\nEIBDLIQP / EIIDLIQP finished.")
