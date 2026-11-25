#!/usr/bin/env python3
"""
Program : EIBWDLPS.py
Purpose : Extraction of SAS customer data for the DLP (Data Loss Prevention)
          system. Consolidates identity/contact data from CRM deposit
          (PBB), CRM deposit-extract (CISBEXT.DP), loan-extract
          (CISBEXT.LN), and card-holder (CRM.CARD) sources, enriches with
          collateral numbers (PBB + PIBB MNICOL), masks sensitive fields,
          and writes a pipe-delimited customer extract split into
          5,000,000-row segments, plus an SFTP put-command list describing
          the segments.

Dependency:
    JCL //DELETE step (IEFBR14 + ADRDSSU DUMP DELETE PURGE of prior
    generations of SAP.PBB.DLP.**.TEXT) is pure dataset housekeeping with
    no data-transformation content. It has no Python equivalent here; the
    output directory is simply (re)written fresh on each run.

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
1. CARD.UNICARD&REPTYEAR&REPTMON&NOWK  (JCL //CARD DD DSN=SAP.PBB.CRM.CARD)
   Deterministic member name built directly from REPTYEAR/REPTMON/NOWK
   tokens (YEAR2./Z2./exact-day-match respectively)
   Cols used : CARDNO, CUSTNBR, NEWIC, OLDIC, BUSTELNO, HOMTELNO,
               HPHONENO, CLOSECD, RECLASS

2. CISR.DEPOSIT  (JCL //CISR DD DSN=SAP.PBB.CRM.CISBEXT)
   Files : INPUT_CISR_SA_FILE (cisr1sa) + INPUT_CISR_FD_FILE (cisr1fd),
           combined (SA first, then FD) into one CISR.DEPOSIT source
   Cols used : ACCTNO, CUSTNO, NEWIC, OLDIC, PRIPHONE, SECPHONE, MOBIPHON,
               BUSSREG, NEWICIND

3. CISD.DEPOSIT  (JCL //CISD DD DSN=SAP.PBB.CISBEXT.DP)
   File : INPUT_CISD_FILE -> cisbext_dp_deposit.sas7bdat
   Cols used : same as CISR.DEPOSIT

4. CISL.LOAN     (JCL //CISL DD DSN=SAP.PBB.CISBEXT.LN)
   File : INPUT_CISL_FILE -> cisbext_ln_loan.sas7bdat
   Cols used : same as CISR.DEPOSIT

5. COLL.COLLATER  (JCL //COLL DD DSN=SAP.PBB.MNICOL(0))
   File : INPUT_COLL_FILE -> pbb_mnicol_collater.sas7bdat
   Cols used : ACCTNO, CCOLLNO

6. ICOLL.COLLATER (JCL //ICOLL DD DSN=SAP.PIBB.MNICOL(0))
   File : INPUT_ICOLL_FILE -> pibb_mnicol_collater.sas7bdat
   Cols used : ACCTNO, CCOLLNO

============================================================================
OUTPUT
============================================================================
//DLP  DD DSN=SAP.PBB.DLP           (backing library for DLP.CUSTDATA;
        not itself a report/extract file -- superseded below)
//SFTP DD DSN=SAP.PBB.DLP.SFTP.TEXT, RECFM=FB, LRECL=80
        -> local file  : SFTP_OUTPUT_FILE (EIBWDLPS_SFTP.txt)
Split extract members SAP.PBB.DLP.C<NNN>.TEXT, RECFM=VB, LRECL=450,
one member per 5,000,000-row segment of DLP.CUSTDATA
        -> local files : OUTPUT_DIR / EIBWDLPS_C<NNN>.txt

None of these are printed reports (no titles/page headers/ASA control in
the original JCL -- RECFM=FB/VB, not FBA), so no ASA carriage-control byte
or page-break logic is produced; they are plain pipe-delimited / command
text files.
"""

import atexit
import gc
import itertools
import shutil
import sys
import tempfile
from math import ceil
from pathlib import Path
from datetime import date

import duckdb
import pandas as pd
import paramiko
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Production Path
BASE_DIR = Path("/host_pq/mis")

# DWH control files (report date + SFTP info)
CTL_DIR          = Path("/sasdata/dwh/control")
CTL_BATCH_DTTM   = CTL_DIR / "ctl_dwh_batch_dttm.sas7bdat"
CTL_SFTP_INFO    = CTL_DIR / "ctl_dwh_sftp_info.sas7bdat"
SOURCE_SYSTEM_CD = "SAS"

INPUT_CARD_DIR = Path("/")     # Kena konfemkan dulu dengan adli
CIS_DIR        = Path("/dwh/rs_cis")
MNICOL_DIR     = Path("/sas/ln/mart/enrichment")

OUTPUT_DIR = BASE_DIR / "output"  / "report" / "EIBWDLPS"
WORK_DIR   = BASE_DIR / "parquet" / "EIBWDLPS"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
WORK_DIR.mkdir(parents=True, exist_ok=True)

# No date in the SAS output DSNs (SAP.PBB.DLP.SFTP.TEXT / SAP.PBB.DLP.Cnnn.TEXT)
# -> output filenames carry no date.
SFTP_OUTPUT_FILE = OUTPUT_DIR / "EIBWDLPS_SFTP.txt"

# Kena konfemkan dulu dengan adli
# SFTP target (ask.py / COZ#DLP3): DLP server
DLP_HOST_DESC  = "DLP Server"   # must equal the HOST_DESC row in ctl_dwh_sftp_info
DLP_REMOTE_DIR = ""             # "" = remote login directory

# # Testing Path
# BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
# STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# # INPUT_CARD_DIR  = STG_DIR / "EIBWDLPS"
# INPUT_CISR_DIR  = STG_DIR / "EIBDUNDP"
# INPUT_CISD_DIR  = STG_DIR / "EIBDUNDP"
# INPUT_CISL_DIR  = STG_DIR / "EIBWDLPS"
# INPUT_COLL_DIR  = STG_DIR / "MNICOL"
# INPUT_ICOLL_DIR = STG_DIR / "MNICOL"

# INPUT_CISR_FILE  = INPUT_CISR_DIR  / "crm_cisbext_deposit.sas7bdat"
# INPUT_CISD_FILE  = INPUT_CISD_DIR  / "cisbext_dp_deposit.sas7bdat"
# INPUT_CISL_FILE  = INPUT_CISL_DIR  / "cisbext_ln_loan.sas7bdat"
# INPUT_COLL_FILE  = INPUT_COLL_DIR  / "collater.sas7bdat"
# INPUT_ICOLL_FILE = INPUT_ICOLL_DIR / "icollater.sas7bdat"
# # INPUT_CARD_FILE is built below once REPTYEAR/REPTMON/WK are known.

# CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBWDLPS"
# CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000
OBSNUM     = 5_000_000   # DATA DLP; OBSNUM = 5000000; (segment size)


# ============================================================================
# DWH CONTROL FILE: BATCH DATE
# ============================================================================
def get_batch_date(source_system_cd: str) -> date:
    """Read BATCH_DTTM for the given SOURCE_SYSTEM_CD from ctl_dwh_batch_dttm."""
    if not CTL_BATCH_DTTM.exists():
        raise FileNotFoundError(f"Batch date control file not found: {CTL_BATCH_DTTM}")
    ctl = pd.read_sas(str(CTL_BATCH_DTTM), encoding="latin1")
    ctl.columns = [str(c).upper().strip() for c in ctl.columns]
    ctl["SOURCE_SYSTEM_CD"] = ctl["SOURCE_SYSTEM_CD"].astype(str).str.strip()
    hit = ctl.loc[ctl["SOURCE_SYSTEM_CD"] == source_system_cd, "BATCH_DTTM"]
    if hit.empty:
        raise ValueError(f"SOURCE_SYSTEM_CD '{source_system_cd}' not found in {CTL_BATCH_DTTM}")
    return pd.Timestamp(hit.iloc[0]).date()


# ============================================================================
# STEP 1: REPORT DATE  (batch date of SOURCE_SYSTEM_CD = 'SAS')
# ============================================================================
print("Step 1: Deriving report date...")

reptdate = get_batch_date(SOURCE_SYSTEM_CD)

# DATA DATES; SET BNM.REPTDATE; SELECT(DAY(REPTDATE)); ... WHEN(8/15/22)
# OTHERWISE; -- exact-day matching, same as the original SAS.
# _day = reptdate.day
# if _day == 8:
#     SDD, WK, WK1 = 1, "01", "04"
# elif _day == 15:
#     SDD, WK, WK1 = 9, "02", "01"
# elif _day == 22:
#     SDD, WK, WK1 = 16, "03", "02"
# else:
#     SDD, WK, WK1 = 23, "04", "03"
_day = reptdate.day
if 1 <= _day <= 8:
    SDD, WK, WK1 = 1, "01", "04"
elif 9 <= _day <= 15:
    SDD, WK, WK1 = 9, "02", "01"
elif 16 <= _day <= 22:
    SDD, WK, WK1 = 16, "03", "02"
else:
    SDD, WK, WK1 = 23, "04", "03"
# SDD and WK1 (CALL SYMPUT('NOWK1',...)) are never referenced again in the
# original SAS; kept for documentation parity only.

REPTYEAR = reptdate.strftime("%y")   # CALL SYMPUT(...,PUT(REPTDATE,YEAR2.))
REPTMON  = reptdate.strftime("%m")   # CALL SYMPUT(...,PUT(MONTH(REPTDATE),Z2.))
REPTDAY  = reptdate.strftime("%d")   # CALL SYMPUT(...,PUT(DAY(REPTDATE),Z2.))
NOWK     = str(int(WK))              # week digit used in the CIS file names

# ----------------------------------------------------------------------------
# INPUT FILE PATHS
# ----------------------------------------------------------------------------
# CARD.UNICARD&REPTYEAR&REPTMON&NOWK
CARD_MEMBER = f"UNICARD{REPTYEAR}{REPTMON}{WK}"
# CARD_MEMBER = f"UNICARD260903"
INPUT_CARD_FILE = INPUT_CARD_DIR / f"{CARD_MEMBER.lower()}.sas7bdat"

INPUT_CISD_FILE    = CIS_DIR / f"cisr1ca{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
INPUT_CISL_FILE    = CIS_DIR / f"cisr1ln{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
INPUT_CISR_SA_FILE = CIS_DIR / f"cisr1sa{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
INPUT_CISR_FD_FILE = CIS_DIR / f"cisr1fd{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"

INPUT_COLL_FILE  = MNICOL_DIR / f"enrh_exdwh_ln_collater_d{REPTDAY}.sas7bdat"
INPUT_ICOLL_FILE = MNICOL_DIR / f"enrh_exdwh_ln_icollater_d{REPTDAY}.sas7bdat"

print(f"  REPTDATE     : {reptdate}")
print(f"  REPTYEAR/MON : {REPTYEAR}/{REPTMON}   NOWK: {WK}")
print(f"  CARD member  : {CARD_MEMBER}")

# ============================================================================
# MACRO-CONSTANT EQUIVALENTS
# ============================================================================
# %LET STRING = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ=`&-/\@#*+:().,"%<?>!$?_ ';
# The single character shown here as "<?>" is a non-ASCII/EBCDIC special
# character in the original SAS source listing that could not be reliably
# transcribed; it is preserved as the Unicode replacement character so the
# remainder of the literal set stays faithful to the original.
STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ=`&-/\\@#*+:().,\"%\ufffd!$?_ "
NUMBER = "0123456789"
ZERO   = "0"
FALSIC = (
    "1234567", "12345678", "123456789", "0123456789",
    "SA9999", "SA99999", "999999A", "999999W", "999999X",
)

MASK = "###**###"

# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET
# ============================================================================
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


# Temporary Parquet area: every SAS dataset is converted ONCE per run, reused
# by every step, and the whole folder is dropped when the program ends.
TMP_PARQUET_DIR = Path(tempfile.mkdtemp(prefix="parquet_", dir=WORK_DIR))
atexit.register(shutil.rmtree, TMP_PARQUET_DIR, ignore_errors=True)
_TEMP_PARQUET: dict = {}


def _load_temp(sas_path: Path, tag: str) -> Path:
    key = str(sas_path)
    if key not in _TEMP_PARQUET:
        tmp_path = TMP_PARQUET_DIR / f"{sas_path.stem}.parquet"
        _sas_to_parquet(sas_path, tmp_path, tag)
        _TEMP_PARQUET[key] = tmp_path
    return _TEMP_PARQUET[key]


# ============================================================================
# SFTP HELPERS  (ctl_dwh_sftp_info.sas7bdat)
# ============================================================================
def get_sftp_info(host_desc: str):
    """Read one SFTP configuration row from the DWH control file."""
    if not CTL_SFTP_INFO.exists():
        raise FileNotFoundError(f"SFTP control file not found: {CTL_SFTP_INFO}")

    ctl = pd.read_sas(str(CTL_SFTP_INFO), encoding="utf-8")
    ctl.columns = [str(c).upper().strip() for c in ctl.columns]

    required = {"HOST_DESC", "SFTP_ID", "SFTP_PW", "HOST_IP"}
    missing_columns = sorted(required.difference(ctl.columns))
    if missing_columns:
        raise RuntimeError("SFTP control file is missing columns: " + ", ".join(missing_columns))

    descriptions = ctl["HOST_DESC"].fillna("").astype(str).str.strip()
    rows = ctl[descriptions.eq(host_desc)]
    if len(rows) != 1:
        raise RuntimeError(f"Expected exactly one SFTP row for '{host_desc}'; found {len(rows)}")

    row = rows.iloc[0]
    clean = lambda value: "".join(str(value).split()) if pd.notna(value) else ""
    sftp_id = clean(row["SFTP_ID"])
    sftp_pw = clean(row["SFTP_PW"])
    host_ip = clean(row["HOST_IP"])
    host_key = clean(row["HOST_KEY"]) if "HOST_KEY" in ctl.columns else ""

    empty_values = [n for n, v in (("SFTP_ID", sftp_id), ("SFTP_PW", sftp_pw),
                                   ("HOST_IP", host_ip)) if not v]
    if empty_values:
        raise RuntimeError(f"SFTP row for '{host_desc}' has blank values: {', '.join(empty_values)}")
    return sftp_id, sftp_pw, host_ip, host_key


def sftp_upload_to_dlp(local_files: list) -> None:
    """
    Equivalent of the JCL RUNSFTP step (ask.py): PUT each SAP.PBB.DLP.Cnnn.TEXT
    member as DLPnnn.CSV on the DLP server.
    local_files: list of (local_path: Path, remote_filename: str)
    """
    sftp_id, sftp_pw, host_ip, _host_key = get_sftp_info(DLP_HOST_DESC)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print(f"  Connecting to DLP host {host_ip} as {sftp_id} ...")
    ssh.connect(hostname=host_ip, username=sftp_id, password=sftp_pw)
    sftp = ssh.open_sftp()

    if DLP_REMOTE_DIR:
        try:
            sftp.chdir(DLP_REMOTE_DIR)
        except IOError:
            sftp.close()
            ssh.close()
            sys.exit(f"Aborting: Remote DLP folder '{DLP_REMOTE_DIR}' not found")

    for local_path, remote_name in local_files:
        print(f"  SFTP upload: {local_path.name} -> {remote_name}")
        sftp.put(str(local_path), remote_name)

    sftp.close()
    ssh.close()
    print("  DLP upload complete.")


# ============================================================================
# STEP 2: CONVERT INPUT SAS FILES TO TEMPORARY PARQUET  (once per run)
# ============================================================================
print("\nStep 2: Converting input SAS datasets to temporary Parquet...")
CARD_CACHE    = _load_temp(INPUT_CARD_FILE, "CARD")
CISR_SA_CACHE = _load_temp(INPUT_CISR_SA_FILE, "CISR_SA")
CISR_FD_CACHE = _load_temp(INPUT_CISR_FD_FILE, "CISR_FD")
CISD_CACHE    = _load_temp(INPUT_CISD_FILE, "CISD")
CISL_CACHE    = _load_temp(INPUT_CISL_FILE, "CISL")
COLL_CACHE    = _load_temp(INPUT_COLL_FILE, "COLL")
ICOLL_CACHE   = _load_temp(INPUT_ICOLL_FILE, "ICOLL")
CISR_CACHES   = [CISR_SA_CACHE, CISR_FD_CACHE]   # CISR = cisr1sa + cisr1fd

# ============================================================================
# DUCKDB WORK DATABASE  (disk-backed, bounded memory)
# ============================================================================
TMP_DIR  = WORK_DIR / "tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)
DB_FILE  = WORK_DIR / "eibwdlps.duckdb"
for _p in (DB_FILE, Path(str(DB_FILE) + ".wal")):
    _p.unlink(missing_ok=True)

con = duckdb.connect(str(DB_FILE))
con.execute("SET memory_limit='16GB'")          # set to about 50% of server RAM
con.execute(f"SET temp_directory='{TMP_DIR.as_posix()}'")
con.execute("SET threads=4")
con.execute("SET preserve_insertion_order=true")  # row_number() OVER () relies on file order


def _s(col: str) -> str:
    """SAS character semantics: missing -> blank."""
    return f"COALESCE(CAST({col} AS VARCHAR),'')"


# ============================================================================
# SMALL SAS-SEMANTIC HELPERS
# ============================================================================
def _is_blank(s) -> bool:
    """SAS char IN ('',' ') / missing test."""
    return s is None or str(s).strip() == ""


def _compress_blanks(s) -> str:
    """COMPRESS(var) with no chars arg -- removes blanks only."""
    return (s or "").replace(" ", "")


def _compress_keep(s, keep_chars: str) -> str:
    """COMPRESS(var, chars, 'K') -- keep only characters in `keep_chars`."""
    return "".join(ch for ch in (s or "") if ch in keep_chars)


def _compress_remove(s, remove_chars: str) -> str:
    """COMPRESS(var, chars) -- remove characters that are in `remove_chars`."""
    return "".join(ch for ch in (s or "") if ch not in remove_chars)


def _leading_zero_str(raw) -> str:
    """Emulates: FORMAT PHONEn 10.; PHONEn = <char phone>;  (a numeric
    FORMAT declared before a variable's first assignment fixes its type as
    NUMERIC, per documented SAS behaviour, so this assignment triggers an
    implicit character->numeric conversion) ... PRxPHxN3 = LEFT(PHONEn);
    (numeric->character via the 10. format, then left-justified). The net,
    intentional effect (per the original "/*** REMOVE LEADING ZERO ***/"
    comment) is that leading zeros are stripped from the digit string.
    Only the resulting string's *content* is ever used downstream (for
    LENGTH() and COMPRESS() tests), so trailing-blank padding from the
    LEFT() step is immaterial and omitted here.
    """
    s = (raw or "").strip()
    if s == "":
        return "."   # SAS missing numeric formats to a bare '.'
    try:
        return str(int(s))
    except ValueError:
        return "."


def _sort_key(v):
    """Stable ascending sort key where SAS missing (None/blank) sorts first."""
    return (v is None or str(v).strip() == "", v if v is not None else "")


def _stable_sort(rows: list, field: str) -> list:
    return sorted(rows, key=lambda r: _sort_key(r.get(field)))


# ============================================================================
# STEP 3: CISRM / CISDP / CISLN  (PROC SORT ... KEEP=... BY ACCTNO)
# ============================================================================
print("\nStep 3: Registering CISR/CISD/CISL sources...")
_CIS_COLS = ["ACCTNO", "CUSTNO", "NEWIC", "OLDIC", "PRIPHONE",
             "SECPHONE", "MOBIPHON", "BUSSREG", "NEWICIND"]


def _cis_select(cache_paths: list) -> str:
    cols = ", ".join(f"{_s(c)} AS {c}" for c in _CIS_COLS)
    parts = " UNION ALL ".join(
        f"SELECT {cols} FROM read_parquet('{p.as_posix()}')" for p in cache_paths)
    return f"SELECT row_number() OVER () AS rid, * FROM ({parts})"

# ============================================================================
# STEP 4: COLL  (DATA COLL; SET ICOLL.COLLATER COLL.COLLATER; PROC SORT)
# ============================================================================
print("\nStep 4: Building COLL (ICOLL first, then COLL, ordered by ACCTNO)...")
con.execute(f"""
    CREATE TABLE coll AS
    SELECT ACCTNO, CCOLLNO,
           row_number() OVER (PARTITION BY ACCTNO ORDER BY src, rid) AS rn
    FROM (
        SELECT 0 AS src, row_number() OVER () AS rid,
               {_s('ACCTNO')} AS ACCTNO, CAST(CCOLLNO AS VARCHAR) AS CCOLLNO
        FROM read_parquet('{ICOLL_CACHE.as_posix()}')
        UNION ALL
        SELECT 1 AS src, row_number() OVER () AS rid,
               {_s('ACCTNO')} AS ACCTNO, CAST(CCOLLNO AS VARCHAR) AS CCOLLNO
        FROM read_parquet('{COLL_CACHE.as_posix()}')
    )
""")
con.execute("CREATE TABLE coll_cnt AS SELECT ACCTNO, COUNT(*) AS nb FROM coll GROUP BY ACCTNO")
print(f"  COLL combined: {con.execute('SELECT COUNT(*) FROM coll').fetchone()[0]:,} rows.")

# ============================================================================
# GENERIC SAS MERGE SIMULATOR  (many-to-many + cross-group "leak" fidelity)
# ============================================================================
def _emit_group(pdv: dict, out: list, key, a_group: list, b_group: list,
                by_key: str, a_cols: list, b_cols: list, keep_only_a: bool) -> None:
    n = max(len(a_group), len(b_group))
    for pos in range(n):
        a_in = pos < len(a_group)
        b_in = pos < len(b_group)
        if a_in:
            pdv.update({c: a_group[pos].get(c) for c in a_cols})
        if b_in:
            pdv.update({c: b_group[pos].get(c) for c in b_cols})
        if keep_only_a and not a_in:
            continue
        row = dict(pdv)
        row[by_key] = key
        out.append(row)


def sas_merge_by_group(a_rows: list, b_rows: list, by_key: str,
                        a_cols: list, b_cols: list, keep_only_a: bool = False) -> list:
    """Emulates `MERGE a(IN=A) b(IN=B); BY by_key;`, preserving its two
    well-known SAS gotchas rather than "fixing" them:
      1. Many-to-many BY groups are paired POSITIONALLY (i-th obs of A
         with i-th obs of B), never cross-joined; once one side runs out
         for the group, its IN= flag reads false for the remaining
         positions (mirrors `keep_only_a` = a trailing `IF A;`).
      2. A variable is NOT reset to missing when its source data set does
         not contribute a fresh observation in the current iteration -- it
         silently retains whatever value was last read for it, even from
         an earlier, unrelated BY group. This cross-group "leak-forward"
         is intentional and preserved verbatim.
    a_rows / b_rows must already be sorted (stably) by `by_key`.
    """
    groups_a = [(k, list(g)) for k, g in itertools.groupby(a_rows, key=lambda r: r[by_key])]
    groups_b = [(k, list(g)) for k, g in itertools.groupby(b_rows, key=lambda r: r[by_key])]

    pdv = {c: None for c in a_cols + b_cols}
    out: list = []
    i = j = 0
    while i < len(groups_a) or j < len(groups_b):
        key_a = groups_a[i][0] if i < len(groups_a) else None
        key_b = groups_b[j][0] if j < len(groups_b) else None
        a_first = i < len(groups_a) and (j >= len(groups_b) or _sort_key(key_a) < _sort_key(key_b))
        b_first = j < len(groups_b) and (i >= len(groups_a) or _sort_key(key_b) < _sort_key(key_a))
        if a_first:
            _emit_group(pdv, out, key_a, groups_a[i][1], [], by_key, a_cols, b_cols, keep_only_a)
            i += 1
        elif b_first:
            _emit_group(pdv, out, key_b, [], groups_b[j][1], by_key, a_cols, b_cols, keep_only_a)
            j += 1
        else:
            _emit_group(pdv, out, key_a, groups_a[i][1], groups_b[j][1], by_key, a_cols, b_cols, keep_only_a)
            i += 1
            j += 1
    return out


# ============================================================================
# STEP 5: CISRM/CISDP/CISLN  MERGE  COLL   (each: IF A; -- keep_only_a=True)
# ============================================================================
print("\nStep 5: Merging CISRM/CISDP/CISLN with COLL (IF A;)...")


def _merge_coll_sql(cache_paths: list, src: int) -> str:
    # MERGE CISx(IN=A) COLL; BY ACCTNO; IF A;
    # Rows are paired by position inside each ACCTNO. When COLL has fewer rows
    # than CISx, SAS keeps COLL's last value (LEAST(rn, nb)). If ACCTNO is not in
    # COLL, CCOLLNO is missing (NULL).
    return f"""
        WITH s AS ({_cis_select(cache_paths)}),
             a AS (SELECT *, row_number() OVER (PARTITION BY ACCTNO ORDER BY rid) AS rn FROM s)
        SELECT {src} AS src, a.rid, a.ACCTNO, a.CUSTNO, a.NEWIC, a.OLDIC,
               a.PRIPHONE, a.SECPHONE, a.MOBIPHON, a.BUSSREG, a.NEWICIND, c.CCOLLNO
        FROM a
        LEFT JOIN coll_cnt g ON g.ACCTNO = a.ACCTNO
        LEFT JOIN coll c     ON c.ACCTNO = a.ACCTNO AND c.rn = LEAST(a.rn, g.nb)
    """


for _name, _caches, _src in (("cisrm", CISR_CACHES, 1),
                             ("cisdp", [CISD_CACHE], 2),
                             ("cisln", [CISL_CACHE], 3)):
    con.execute(f"CREATE TABLE {_name} AS {_merge_coll_sql(_caches, _src)}")
    print(f"  {_name.upper()}: {con.execute(f'SELECT COUNT(*) FROM {_name}').fetchone()[0]:,}")
con.execute("DROP TABLE coll")
con.execute("DROP TABLE coll_cnt")

# ============================================================================
# STEP 6: CIS  (SET CISRM CISDP CISLN; default NEWIC/NEWICIND)
# ============================================================================
print("\nStep 6: Stacking CIS and applying NEWIC default...")
con.execute("""
    CREATE TABLE cis AS
    SELECT src, rid, ACCTNO, CUSTNO,
           CASE WHEN TRIM(NEWIC)='' THEN OLDIC ELSE NEWIC END AS NEWIC,
           OLDIC, PRIPHONE, SECPHONE, MOBIPHON, BUSSREG,
           CASE WHEN TRIM(NEWIC)='' THEN 'OC' ELSE NEWICIND END AS NEWICIND,
           CCOLLNO
    FROM (SELECT * FROM cisrm UNION ALL SELECT * FROM cisdp UNION ALL SELECT * FROM cisln)
""")
for _t in ("cisrm", "cisdp", "cisln"):
    con.execute(f"DROP TABLE {_t}")
print(f"  CIS rows: {con.execute('SELECT COUNT(*) FROM cis').fetchone()[0]:,}")

# ============================================================================
# STEP 7: CCARD  (PROC SORT ... WHERE CLOSECD/RECLASS blank; BY CUSTNBR)
# ============================================================================
print("\nStep 7: Loading + filtering CARD.UNICARD...")
con.execute(f"""
    CREATE TABLE ccard AS
    SELECT row_number() OVER () AS rid,
           {_s('CUSTNBR')} AS CUSTNBR, CAST(CARDNO AS VARCHAR) AS CARDNO,
           {_s('NEWIC')} AS NEWIC, {_s('OLDIC')} AS OLDIC,
           {_s('BUSTELNO')} AS BUSTELNO, {_s('HOMTELNO')} AS HOMTELNO,
           {_s('HPHONENO')} AS HPHONENO
    FROM read_parquet('{CARD_CACHE.as_posix()}')
    WHERE COALESCE(TRIM(CLOSECD),'')='' AND COALESCE(TRIM(RECLASS),'')=''
""")
print(f"  CCARD rows: {con.execute('SELECT COUNT(*) FROM ccard').fetchone()[0]:,}")

# ============================================================================
# STEP 8: CARD FREQUENCY (&C, &CARD) + TRANSPOSE  (BASECARD padding folded in)
# ============================================================================
print("\nStep 8: Computing card-count width and building TRANPCARD...")
_max_freq = con.execute(
    "SELECT COALESCE(MAX(c),0) FROM (SELECT COUNT(*) AS c FROM ccard GROUP BY CUSTNBR)"
).fetchone()[0]
N_CARDS = _max_freq + 2                       # &C
CARD_NAMES = [f"CARD{i}" for i in range(1, N_CARDS + 1)]
print(f"  Max cards/customer: {_max_freq}  ->  N_CARDS (&C): {N_CARDS}")

_card_pivot = ", ".join(f"MAX(CASE WHEN rn={i} THEN CARDNO END) AS CARD{i}"
                        for i in range(1, N_CARDS + 1))
# COPY vars (NEWIC OLDIC BUSTELNO HOMTELNO HPHONENO) = first obs per CUSTNBR.
con.execute(f"""
    CREATE TABLE tranpcard AS
    SELECT CUSTNO,
           CASE WHEN TRIM(NEWIC0)='' THEN OLDIC ELSE NEWIC0 END AS NEWIC,
           OLDIC, PRIPHONE, SECPHONE, MOBIPHON, {", ".join(CARD_NAMES)}
    FROM (
        SELECT CUSTNBR AS CUSTNO,
               MAX(CASE WHEN rn=1 THEN NEWIC    END) AS NEWIC0,
               MAX(CASE WHEN rn=1 THEN OLDIC    END) AS OLDIC,
               MAX(CASE WHEN rn=1 THEN HOMTELNO END) AS PRIPHONE,
               MAX(CASE WHEN rn=1 THEN BUSTELNO END) AS SECPHONE,
               MAX(CASE WHEN rn=1 THEN HPHONENO END) AS MOBIPHON,
               {_card_pivot}
        FROM (SELECT *, row_number() OVER (PARTITION BY CUSTNBR ORDER BY rid) AS rn FROM ccard)
        GROUP BY CUSTNBR
    )
""")
con.execute("DROP TABLE ccard")
print(f"  TRANPCARD rows: {con.execute('SELECT COUNT(*) FROM tranpcard').fetchone()[0]:,}")

# ============================================================================
# STEP 9: SORT CIS + TRANPCARD BY NEWIC, THEN MERGE (no IF filter)
# ============================================================================
print("\nStep 9: Merging CIS with TRANPCARD by NEWIC...")
# MERGE CIS(IN=A) TRANPCARD(IN=B); BY NEWIC;  (no IF filter)
#  - per NEWIC group, row `pos` pairs the pos-th CIS row with the pos-th TRANPCARD row
#  - a side that runs out keeps its LAST row's values (LEAST(pos, count))
#  - a side absent from the group is missing (NULL)
#  - variables in both (CUSTNO OLDIC PRIPHONE SECPHONE MOBIPHON): B wins when B
#    contributes a fresh row (pos <= nb), otherwise A's value stays
_common = ["CUSTNO", "OLDIC", "PRIPHONE", "SECPHONE", "MOBIPHON"]
_common_sql = ", ".join(
    f"CASE WHEN p.pos <= p.nb THEN b.{c} ELSE a.{c} END AS {c}" for c in _common)
_cards_sql = ", ".join(f"b.{c} AS {c}" for c in CARD_NAMES)

con.execute(f"""
    CREATE TABLE ciscard AS
    WITH a  AS (SELECT *, row_number() OVER (PARTITION BY NEWIC ORDER BY src, rid) AS rn FROM cis),
         b  AS (SELECT *, row_number() OVER (PARTITION BY NEWIC ORDER BY CUSTNO)     AS rn FROM tranpcard),
         ga AS (SELECT NEWIC, COUNT(*) AS na FROM cis GROUP BY NEWIC),
         gb AS (SELECT NEWIC, COUNT(*) AS nb FROM tranpcard GROUP BY NEWIC),
         g  AS (SELECT COALESCE(ga.NEWIC, gb.NEWIC) AS NEWIC,
                       COALESCE(na,0) AS na, COALESCE(nb,0) AS nb
                FROM ga FULL OUTER JOIN gb ON ga.NEWIC = gb.NEWIC),
         p  AS (SELECT NEWIC, na, nb, UNNEST(range(1, GREATEST(na, nb) + 1)) AS pos FROM g)
    SELECT p.NEWIC, p.pos,
           a.ACCTNO, a.BUSSREG, a.NEWICIND, a.CCOLLNO,
           {_common_sql},
           {_cards_sql}
    FROM p
    LEFT JOIN a ON a.NEWIC = p.NEWIC AND a.rn = LEAST(p.pos, p.na)
    LEFT JOIN b ON b.NEWIC = p.NEWIC AND b.rn = LEAST(p.pos, p.nb)
""")
con.execute("DROP TABLE cis")
con.execute("DROP TABLE tranpcard")
print(f"  CISCARD rows: {con.execute('SELECT COUNT(*) FROM ciscard').fetchone()[0]:,}")

# ============================================================================
# STEP 10: PER-ROW TRANSFORM PIPELINE  (CISCARD compress -> leading-zero ->
#          blank masking -> NEWIC validation -> phone validation)
# ============================================================================
print("\nStep 10: Applying compress / mask / validation pipeline...")
_M = f"'{MASK}'"
_STRING_RE = "[" + "".join("\\" + ch if ch in "\\]^-[" else ch for ch in STRING).replace("'", "''") + "]"
_FALSIC_SQL = ", ".join(f"'{x}'" for x in FALSIC)

_card_list   = ", ".join(CARD_NAMES)
_card_mask   = ", ".join(
    f"CASE WHEN COALESCE(TRIM({c}),'')='' THEN {_M} ELSE {c} END AS {c}" for c in CARD_NAMES)
_blank_mask  = lambda c: f"CASE WHEN {c} IN ('','.') THEN {_M} ELSE {c} END AS {c}"

con.execute(f"""
    CREATE TABLE custdata AS
    WITH s1 AS (   -- COMPRESS(...)
        SELECT NEWIC AS SORT_NEWIC, pos, COALESCE(NEWICIND,'') AS NEWICIND,
               replace(COALESCE(NEWIC,''),' ','')  AS NEWIC,
               replace(COALESCE(ACCTNO,''),' ','') AS ACCTNO,
               regexp_replace(COALESCE(PRIPHONE,''),'[^0-9]','','g') AS PRIPHONE,
               regexp_replace(COALESCE(SECPHONE,''),'[^0-9]','','g') AS SECPHONE,
               regexp_replace(COALESCE(MOBIPHON,''),'[^0-9]','','g') AS MOBIPHON,
               replace(COALESCE(BUSSREG,''),' ','') AS BUSSREG,
               {_card_list}
        FROM ciscard),
    s2 AS (        -- REMOVE LEADING ZERO
        SELECT *,
               COALESCE(substr(CAST(TRY_CAST(PRIPHONE AS HUGEINT) AS VARCHAR), 1, 10),'.') AS PR1PH0N3,
               COALESCE(substr(CAST(TRY_CAST(SECPHONE AS HUGEINT) AS VARCHAR), 1, 10),'.') AS S3CPH0N3,
               COALESCE(substr(CAST(TRY_CAST(MOBIPHON AS HUGEINT) AS VARCHAR), 1, 10),'.') AS M0B1PH0N
        FROM s1),
    s3 AS (        -- mask blanks
        SELECT * REPLACE ({_blank_mask('NEWIC')}, {_blank_mask('ACCTNO')},
                          {_blank_mask('PRIPHONE')}, {_blank_mask('SECPHONE')},
                          {_blank_mask('MOBIPHON')}, {_blank_mask('BUSSREG')},
                          {_card_mask})
        FROM s2),
    s4 AS (        -- check columns
        SELECT *,
               regexp_replace(NEWIC,'[0-9]','','g')        AS NUMCHECK,
               regexp_replace(NEWIC,'{_STRING_RE}','','g') AS STRCHECK,
               replace(NEWIC,'0','')     AS ZROCHECK,
               replace(PR1PH0N3,'0','')  AS ZRCHKPRI,
               replace(S3CPH0N3,'0','')  AS ZRCHKSEC,
               replace(M0B1PH0N,'0','')  AS ZRCHKMOB
        FROM s3)
    SELECT row_number() OVER (ORDER BY SORT_NEWIC, pos) AS rn,
           -- masking is idempotent, so the sequential IFs collapse into one OR
           CASE WHEN (NEWICIND='IC' AND length(NEWIC) < 12)
                  OR (NEWICIND='OC' AND length(NEWIC) < 7)
                  OR NEWICIND IN ('SA','PC','BC')
                  OR (NUMCHECK='' AND length(NEWIC) < 6)
                  OR STRCHECK=''
                  OR NEWIC IN ({_FALSIC_SQL})
                  OR length(NEWIC) < 4
                  OR ZROCHECK=''
                THEN {_M} ELSE NEWIC END AS NEWIC,
           CASE WHEN length(PR1PH0N3) < 8 OR ZRCHKPRI='' OR length(ZRCHKPRI) < 4
                THEN {_M} ELSE PRIPHONE END AS PRIPHONE,
           CASE WHEN length(S3CPH0N3) < 8 OR ZRCHKSEC='' OR length(ZRCHKSEC) < 4
                THEN {_M} ELSE SECPHONE END AS SECPHONE,
           CASE WHEN length(M0B1PH0N) < 8 OR ZRCHKMOB='' OR length(ZRCHKMOB) < 4
                THEN {_M} ELSE MOBIPHON END AS MOBIPHON,
           ACCTNO, BUSSREG, {_card_list}
    FROM s4
""")
con.execute("DROP TABLE ciscard")

# ============================================================================
# STEP 11: WRITE SPLIT CUSTOMER EXTRACT + SFTP COMMAND LIST + SFTP UPLOAD
# ============================================================================
print("\nStep 11: Writing split customer extract + SFTP file list...")
NOBS = con.execute("SELECT COUNT(*) FROM custdata").fetchone()[0]
print(f"  DLP.CUSTDATA rows: {NOBS:,}")
NUMFILE = ceil(NOBS / OBSNUM) if NOBS else 0
print(f"  NOBS: {NOBS:,}   OBSNUM: {OBSNUM:,}   NUMFILE: {NUMFILE}")

# CUSTNO and CCOLLNO are intentionally not in the extract (commented out in the SAS PUT).
_FIELDS = ["NEWIC", "PRIPHONE", "SECPHONE", "MOBIPHON", "ACCTNO", "BUSSREG"] + CARD_NAMES
_LINE_SQL = "concat_ws('|', " + ", ".join(f"rtrim({c})" for c in _FIELDS) + ")"

sftp_lines = []
upload_files = []
for i in range(1, NUMFILE + 1):
    no = f"{i:03d}"
    start, end = (i - 1) * OBSNUM + 1, min(i * OBSNUM, NOBS)
    out_path = OUTPUT_DIR / f"EIBWDLPS_C{no}.txt"
    cur = con.execute(
        f"SELECT {_LINE_SQL} FROM custdata WHERE rn BETWEEN ? AND ? ORDER BY rn", [start, end])
    with open(out_path, "w", encoding="latin1") as fh:
        while True:
            batch = cur.fetchmany(200_000)
            if not batch:
                break
            fh.write("\n".join(r[0] for r in batch) + "\n")
    print(f"  Written {out_path.name} ({end - start + 1:,} rows)")

    files_str = f"//SAP.PBB.DLP.C{no}.TEXT  DLP{no}.CSV"
    sftp_lines.append(f"PUT {files_str}")
    upload_files.append((out_path, f"DLP{no}.CSV"))

with open(SFTP_OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in sftp_lines:
        fh.write(ln + "\n")
print(f"  Written {SFTP_OUTPUT_FILE.name} ({len(sftp_lines)} lines)")
for ln in sftp_lines:
    print(f"    {ln}")

con.close()
DB_FILE.unlink(missing_ok=True)

# SFTP the DLP extract members to the DLP server (JCL SFTPDLPS / ask.py)
if upload_files:
    print("\nUploading extract files to DLP server via SFTP...")
    sftp_upload_to_dlp(upload_files)

# All inputs are done being used: drop the temporary Parquet files.
shutil.rmtree(TMP_PARQUET_DIR, ignore_errors=True)
print("\nEIBWDLPS complete.")
