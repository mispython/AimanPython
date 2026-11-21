#!/usr/bin/env python3
"""
Program : EIBMLIQP.py
Purpose : JCL orchestrator for the New Liquidity Framework / Top-50/100
          depositor batch (originally JOB EIBMLIQP, EXEC SAS609). Declares
          every physical input dataset used across the job family, converts
          each one ONCE to a temporary Parquet file, derives the report date
          from the DP batch date (ctl_dwh_batch_dttm.sas7bdat), and drives the
          child programs in the same order as the original JCL/SYSIN:
              %INC PGM(DALWPBBD);
              %INC PGM(EIBMRLFM);
              %INC PGM(EIBMTOP5);
          All temporary Parquet files are deleted when the run ends.
          TOP50 / INDTOP50 / CORTOP50 are SFTP'd to the EDW folder (ask.py).

          This is the PBB job: DEPOSIT / FD / LOAN / BNM1 inputs hold PBB and
          PIBB together, so rows with ENTITY_CD = 'PIBB' are dropped while
          converting. MNIFD (FD.FD) has no usable ENTITY_CD, so it is limited
          to the accounts present in the (already PBB-only) DEPOSIT FD file.
"""
import gc
import sys
import tempfile
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import paramiko
import pyarrow as pa
import pyarrow.parquet as pq

from DALWPBBD import build_savg_curn_dept
from EIBMRLFM import run_eibmrlfm
from EIBMTOP5 import run_eibmtop5

# ============================================================================
# CONTROL FILES (batch date + SFTP info)
# ============================================================================
CTL_BATCH_DTTM_FILE = Path("/sasdata/dwh/control/ctl_dwh_batch_dttm.sas7bdat")
CTL_SFTP_INFO_FILE = Path("/sasdata/dwh/control/ctl_dwh_sftp_info.sas7bdat")
BATCH_SOURCE = "DP"          # SOURCE_SYSTEM_CD used for the report date


def get_batch_date(source_system_cd: str) -> date:
    """Read BATCH_DTTM for the given SOURCE_SYSTEM_CD from the control file."""
    if not CTL_BATCH_DTTM_FILE.exists():
        raise FileNotFoundError(f"Batch date control file not found: {CTL_BATCH_DTTM_FILE}")
    ctl = pd.read_sas(str(CTL_BATCH_DTTM_FILE), format="sas7bdat", encoding="latin1")
    ctl.columns = [str(c).upper().strip() for c in ctl.columns]
    ctl["SOURCE_SYSTEM_CD"] = ctl["SOURCE_SYSTEM_CD"].astype(str).str.strip()
    hit = ctl.loc[ctl["SOURCE_SYSTEM_CD"] == source_system_cd, "BATCH_DTTM"]
    if hit.empty:
        raise ValueError(f"SOURCE_SYSTEM_CD '{source_system_cd}' not found in {CTL_BATCH_DTTM_FILE}")
    return pd.Timestamp(hit.iloc[0]).date()


def get_sftp_info(host_desc: str):
    """Read one SFTP configuration row from the DWH SFTP control file."""
    if not CTL_SFTP_INFO_FILE.exists():
        raise FileNotFoundError(f"SFTP control file not found: {CTL_SFTP_INFO_FILE}")
    ctl = pd.read_sas(str(CTL_SFTP_INFO_FILE), format="sas7bdat", encoding="utf-8")
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
    sftp_id, sftp_pw, host_ip = clean(row["SFTP_ID"]), clean(row["SFTP_PW"]), clean(row["HOST_IP"])
    host_key = clean(row["HOST_KEY"]) if "HOST_KEY" in ctl.columns else ""

    blanks = [n for n, v in (("SFTP_ID", sftp_id), ("SFTP_PW", sftp_pw), ("HOST_IP", host_ip)) if not v]
    if blanks:
        raise RuntimeError(f"SFTP row for '{host_desc}' has blank values: {', '.join(blanks)}")
    return sftp_id, sftp_pw, host_ip, host_key


# ============================================================================
# REPORT DATE CONTEXT (DP batch date)
# ============================================================================
def _derive_reptdate_context() -> dict:
    """DATA BNM.REPTDATE; SELECT(DAY(REPTDATE)) ... -- report date = DP batch date.
    NOWK uses exact-day matching (8/15/22/else->4), matching the SAS source."""
    reptdate = get_batch_date(BATCH_SOURCE)
    day = reptdate.day
    nowk = "1" if day == 8 else "2" if day == 15 else "3" if day == 22 else "4"
    rd_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if reptdate.year % 4 == 0:
        rd_days[1] = 29
    return {
        "reptdate": reptdate, "tdate": reptdate, "nowk": nowk,
        "reptyear": reptdate.strftime("%Y"), "reptyea2": reptdate.strftime("%y"),
        "reptmon": reptdate.strftime("%m"), "reptday": reptdate.strftime("%d"),
        "rdate": reptdate.strftime("%d/%m/%y"),
        "rpyr": reptdate.year, "rpmth": reptdate.month, "rpday": reptdate.day,
        "rd_days": rd_days,
    }


CTX = _derive_reptdate_context()
REPTMON = CTX["reptmon"]
NOWK = CTX["nowk"]
REPTYEAR = CTX["reptyear"]
REPTYEA2 = CTX["reptyea2"]
REPTDAY = CTX["reptday"]

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas")

# //DEPOSIT DD DSN=SAP.PBB.MNITB(0) -- SAVING / CURRENT / FD (PBB+PIBB together -> ENTITY_CD != 'PIBB')
MNTIB_DIR = BASE_DIR / "deposit/dwh/integration"
INPUT_SAVING_FILE = MNTIB_DIR / f"intg_dp_acct_saving_m{REPTMON}.sas7bdat"
INPUT_CURRENT_FILE = MNTIB_DIR / f"intg_dp_acct_current_m{REPTMON}.sas7bdat"
INPUT_DEPOSIT_FD_FILE = MNTIB_DIR / f"intg_dp_acct_fd_m{REPTMON}.sas7bdat"      # DEPOSIT.FD

# //FD DD DSN=SAP.PBB.MNIFD(0) -- FD.FD (no ENTITY_CD -> limited to accounts in DEPOSIT.FD by ACCT_NUM)
MNIFD_DIR = BASE_DIR / "deposit/dwh/enrichment"
INPUT_FD_FD_FILE = MNIFD_DIR / f"enrh_dp_fd_cert_m{REPTMON}.sas7bdat"           # FD.FD

# //LOAN DD DSN=SAP.PBB.MNILN(0) -- LNCOMM (ENTITY_CD != 'PIBB')
MNILN_DIR = BASE_DIR / "ln/dwh/enrichment"
INPUT_LNCOMM_FILE = MNILN_DIR / f"enrh_ln_comm_m{REPTMON}.sas7bdat"

# //CISLN DD DSN=SAP.PBB.CISBEXT.DP
INPUT_CISLN_DEPOSIT_FILE = Path("/dwh/rs_cis") / f"cisr1ca{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"

# //CISDP DD DSN=SAP.PBB.CRM.CISBEXT
INPUT_CISDP_DEPOSIT_FILE = BASE_DIR / "cis/mart/enrichment" / "enrh_exdwh_cisr1.sas7bdat"

# //FORATE DD DSN=SAP.PBB.FCYCA -- FORATE.FORATE / FORATE.FORATEBKP
FORATE_DIR = Path("/exdwh/host/forate")
INPUT_FORATE_FILE = FORATE_DIR / "forate.sas7bdat"
INPUT_FORATEBKP_FILE = FORATE_DIR / "foratebkp.sas7bdat"

# //DCIWH DD DSN=SAP.PBB.DCIWH - Need to confirm from Adli
INPUT_DCIWH_DIR = Path("/")

# //PROVSUB DD DSN=SAP.PBB.CCRIS.PROVSUB(0) -- flat text file, kept as .txt
INPUT_PROVSUB_FILE = Path("/host_pq/mis/input/textfile") / "PBB_CCRIS_PROVSUB.txt"

# //BNMK DD DSN=SAP.PBB.KAPITI.SASDATA -- K1TBL / K3TBL / DCIWTB members
INPUT_BNMK_DIR = Path("/exdwh/host/kapiti")

# //PAY DD DSN=SAP.PBB.LNPAYSCH(0) - Need to confirm from Jay
INPUT_PAY_DIR = Path("/")

# //NID DD DSN=SAP.PBB.RNID.SASDATA - Need to confirm from Adli
INPUT_NID_DIR = Path("/")

# //BNM1 DD DSN=SAP.PBB.SASDATA -- LOAN / ULOAN members (ENTITY_CD != 'PIBB')
INPUT_BNM1_DIR = Path("/dwh/dwh_m/tempsource/ln")

OUTPUT_DIR = Path("/host_pq/mis/output/report/EIBMLIQP")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TEMP_DIR = None          # None = system temp; set a path if the default temp area is too small
CHUNK_ROWS = 500_000

# ---- SFTP (ask.py : FTPTOP50 -> EDW, cd /stgsrcsys/host/ftpfiles) ----------
EDW_HOST_DESC = "PIVB File Server"                 # HOST_DESC row in ctl_dwh_sftp_info.sas7bdat
EDW_REMOTE_DIR = "/stgsrcsys/host/ftpfiles"
SFTP_OUTPUTS = [                                   # (local file in OUTPUT_DIR, remote name per ask.py)
    ("INDTOP50.txt", "INDTOP50.TXT"),
    ("CORTOP50.txt", "CORTOP50.TXT"),
    ("TOP50.txt", "TOP50.TXT"),
]

# ============================================================================
# SAS7BDAT -> TEMPORARY PARQUET
# ============================================================================
_CONVERTED: dict = {}      # source path -> temp parquet path (each source is converted once per run)


def _lock_schema(table: pa.Table) -> pa.Schema:
    """Never lock a 'null' type from a chunk that was blank for a text column."""
    return pa.schema([
        pa.field(f.name, pa.string()) if pa.types.is_null(f.type) else f
        for f in table.schema
    ])


def _sas_to_parquet(sas_path: Path, dst: Path, tag: str, exclude_pibb: bool = False) -> None:
    """Chunked .sas7bdat -> Parquet. With exclude_pibb, rows where ENTITY_CD = 'PIBB'
    are dropped (blank/missing ENTITY_CD is kept, as in SAS missing != 'PIBB')."""
    print(f"  [{tag}] Converting {sas_path.name} -> {dst.name} ...")
    writer, schema, total, warned = None, None, 0, False

    for chunk in pd.read_sas(str(sas_path), encoding="latin1", chunksize=CHUNK_ROWS):
        for col in chunk.columns[chunk.dtypes == object]:
            chunk[col] = chunk[col].astype("string")
        if schema is None:
            schema = _lock_schema(pa.Table.from_pandas(chunk, preserve_index=False))
            writer = pq.ParquetWriter(str(dst), schema, compression="snappy")
        if exclude_pibb:
            if "ENTITY_CD" in chunk.columns:
                chunk = chunk[chunk["ENTITY_CD"].fillna("").str.strip() != "PIBB"]
            elif not warned:
                print(f"  [{tag}] WARNING: ENTITY_CD column not found - no PIBB filter applied.")
                warned = True
        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer is not None:
        writer.close()
    else:
        # 0-row source: chunked reader yielded nothing; write an empty parquet.
        empty = pd.read_sas(str(sas_path), encoding="latin1")
        for col in empty.columns[empty.dtypes == object]:
            empty[col] = empty[col].astype("string")
        tbl = pa.Table.from_pandas(empty, preserve_index=False)
        pq.write_table(tbl.cast(_lock_schema(tbl), safe=False), str(dst))
    print(f"  [{tag}] Done - {total:,} rows kept.")


def _load_cached(sas_path: Path, tmp_dir: Path, tag: str, exclude_pibb: bool = False) -> Path:
    """Convert a source to a temp parquet once; reuse it on every later request."""
    key = str(sas_path)
    if key in _CONVERTED:
        print(f"  [{tag}] Already converted this run - reusing.")
        return _CONVERTED[key]
    if not sas_path.exists():
        raise FileNotFoundError(f"[{tag}] input not found: {sas_path}")
    dst = tmp_dir / f"{sas_path.parent.name}__{sas_path.stem}.parquet"
    _sas_to_parquet(sas_path, dst, tag, exclude_pibb)
    _CONVERTED[key] = dst
    return dst


def _date_expr(con, parquet: Path, col: str) -> str:
    """DuckDB expression returning a DATE whether the column landed as TIMESTAMP/DATE
    or as a raw SAS date number (days since 1960-01-01)."""
    col_type = con.execute(
        f"DESCRIBE SELECT {col} FROM read_parquet('{parquet.as_posix()}')"
    ).fetchone()[1].upper()
    if col_type.startswith(("TIMESTAMP", "DATE")):
        return f"CAST({col} AS DATE)"
    return (f"CASE WHEN {col} IS NULL OR ISNAN({col}) THEN NULL "
            f"ELSE DATE '1960-01-01' + CAST(FLOOR({col}) AS INTEGER) END")


def _build_fd_fd_parquet(fd_cert_raw: Path, deposit_fd_cache: Path, dst: Path) -> Path:
    """FD.FD (MNIFD): keep only accounts present in DEPOSIT.FD (already PBB-only) by
    ACCT_NUM = ACCTNO, and rename MNIFD columns back to the names the program uses.
    MATURE_DT is a real date, so MATDATE is output as DATE (not YYYYMMDD)."""
    print("  [FD.FD] Limiting MNIFD to PBB accounts and renaming columns ...")
    con = duckdb.connect(database=":memory:")
    try:
        mat_expr = _date_expr(con, fd_cert_raw, "MATURE_DT")
        con.execute(f"""
            COPY (
                SELECT
                    f.* EXCLUDE (BRANCH_CD, ACCT_NUM, PURPOSE_CD, CUSTOMER_CD, OPEN_IND,
                                 CURR_BAL, MATURE_DT, ACCOUNT_TYPE_CD, INT_PLAN, CURRENCY_CD),
                    CAST(f.BRANCH_CD       AS DOUBLE)  AS BRANCH,
                    CAST(f.ACCT_NUM        AS BIGINT)  AS ACCTNO,
                    CAST(f.PURPOSE_CD      AS VARCHAR) AS PURPOSE,
                    CAST(f.CUSTOMER_CD     AS DOUBLE)  AS CUSTCD,
                    CAST(f.OPEN_IND        AS VARCHAR) AS OPENIND,
                    CAST(f.CURR_BAL        AS DOUBLE)  AS CURBAL,
                    {mat_expr}                         AS MATDATE,
                    CAST(f.ACCOUNT_TYPE_CD AS DOUBLE)  AS ACCTTYPE,
                    CAST(f.INT_PLAN        AS DOUBLE)  AS INTPLAN,
                    CAST(f.CURRENCY_CD     AS VARCHAR) AS CURCODE
                FROM read_parquet('{fd_cert_raw.as_posix()}') f
                WHERE CAST(f.ACCT_NUM AS BIGINT) IN (
                    SELECT CAST(ACCTNO AS BIGINT) FROM read_parquet('{deposit_fd_cache.as_posix()}')
                )
            ) TO '{dst.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        kept = con.execute(f"SELECT COUNT(*) FROM read_parquet('{dst.as_posix()}')").fetchone()[0]
    finally:
        con.close()
    print(f"  [FD.FD] Done - {kept:,} rows kept.")
    return dst


# ============================================================================
# SFTP (ask.py : FTPTOP50)
# ============================================================================
def sftp_upload_to_edw(local_files: list) -> None:
    """Upload (local_path, remote_name) pairs to EDW_REMOTE_DIR."""
    for local_path, _ in local_files:
        if not local_path.exists():
            raise FileNotFoundError(f"SFTP source file not found: {local_path}")

    sftp_id, sftp_pw, host_ip, _host_key = get_sftp_info(EDW_HOST_DESC)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"  Connecting to {EDW_HOST_DESC} {host_ip} as {sftp_id} ...")
    ssh.connect(hostname=host_ip, username=sftp_id, password=sftp_pw)
    sftp = ssh.open_sftp()
    try:
        try:
            sftp.chdir(EDW_REMOTE_DIR)
        except IOError:
            sys.exit(f"Aborting: Remote folder '{EDW_REMOTE_DIR}' not found")
        for local_path, remote_name in local_files:
            print(f"  SFTP upload: {local_path.name} -> {EDW_REMOTE_DIR}/{remote_name}")
            sftp.put(str(local_path), remote_name)
    finally:
        sftp.close()
        ssh.close()
    print("  EDW upload complete.")


def main() -> None:
    ctx = CTX
    print(f"REPTDATE: {ctx['reptdate']}  NOWK: {ctx['nowk']}  RDATE: {ctx['rdate']}")
    reptmon, nowk, reptday, reptyea2 = ctx["reptmon"], ctx["nowk"], ctx["reptday"], ctx["reptyea2"]

    with tempfile.TemporaryDirectory(dir=TEMP_DIR, prefix="EIBMLIQP_") as tmp:
        tmp_dir = Path(tmp)

        print("\nConverting physical inputs to temporary parquet...")
        # PBB+PIBB sources -> ENTITY_CD != 'PIBB'
        saving_cache        = _load_cached(INPUT_SAVING_FILE, tmp_dir, "SAVING", exclude_pibb=True)
        current_cache       = _load_cached(INPUT_CURRENT_FILE, tmp_dir, "CURRENT", exclude_pibb=True)
        deposit_fd_cache    = _load_cached(INPUT_DEPOSIT_FD_FILE, tmp_dir, "DEPOSIT.FD", exclude_pibb=True)
        lncomm_cache        = _load_cached(INPUT_LNCOMM_FILE, tmp_dir, "LNCOMM", exclude_pibb=True)
        bnm1_loan_cache     = _load_cached(INPUT_BNM1_DIR / f"loan{reptmon}{nowk}.sas7bdat", tmp_dir, "BNM1.LOAN", exclude_pibb=True)
        bnm1_uloan_cache    = _load_cached(INPUT_BNM1_DIR / f"uloan{reptmon}{nowk}.sas7bdat", tmp_dir, "BNM1.ULOAN", exclude_pibb=True)

        # FD.FD (MNIFD): no ENTITY_CD -> limited to DEPOSIT.FD accounts, columns renamed
        fd_cert_raw         = _load_cached(INPUT_FD_FD_FILE, tmp_dir, "MNIFD raw")
        fd_fd_cache         = _build_fd_fd_parquet(fd_cert_raw, deposit_fd_cache, tmp_dir / "FD__FD.parquet")
        fd_cert_raw.unlink(missing_ok=True)          # raw MNIFD is not needed after this point

        # Sources with no PBB/PIBB split
        cisln_deposit_cache = _load_cached(INPUT_CISLN_DEPOSIT_FILE, tmp_dir, "CISLN.DEPOSIT")
        cisdp_deposit_cache = _load_cached(INPUT_CISDP_DEPOSIT_FILE, tmp_dir, "CISDP.DEPOSIT")
        forate_cache        = _load_cached(INPUT_FORATE_FILE, tmp_dir, "FORATE")
        foratebkp_cache     = _load_cached(INPUT_FORATEBKP_FILE, tmp_dir, "FORATEBKP")
        dciwh_dci_cache     = _load_cached(INPUT_DCIWH_DIR / f"dci{reptmon}{nowk}.sas7bdat", tmp_dir, "DCIWH.DCI")
        k1tbl_cache         = _load_cached(INPUT_BNMK_DIR / f"k1tbl{reptmon}{nowk}.sas7bdat", tmp_dir, "BNMK.K1TBL")
        k3tbl_cache         = _load_cached(INPUT_BNMK_DIR / f"k3tbl{reptmon}{nowk}.sas7bdat", tmp_dir, "BNMK.K3TBL")
        bnmk_dciwtb_cache   = _load_cached(INPUT_BNMK_DIR / f"dciwtb{reptmon}{nowk}.sas7bdat", tmp_dir, "BNMK.DCIWTB")
        lnpay_cache         = _load_cached(INPUT_PAY_DIR / f"lnpay{reptmon}{nowk}{reptyea2}.sas7bdat", tmp_dir, "PAY.LNPAY")
        nid_rnid_cache      = _load_cached(INPUT_NID_DIR / f"rnid{reptday}.sas7bdat", tmp_dir, "NID.RNID")

        provsub_txt_path = INPUT_PROVSUB_FILE  # flat file, no parquet conversion

        # ---- %INC PGM(DALWPBBD) -- build BNM_SAVG / BNM_CURN / BNM_DEPT ------
        print("\nBuilding BNM_SAVG / BNM_CURN / BNM_DEPT via DALWPBBD...")
        bnm_savg, bnm_curn, _bnm_dept = build_savg_curn_dept(
            saving_cache=saving_cache, current_cache=current_cache, cisdp_cache=cisdp_deposit_cache,
        )
        bnm_savg_cache = tmp_dir / f"BNM__SAVG{reptmon}{nowk}.parquet"
        bnm_curn_cache = tmp_dir / f"BNM__CURN{reptmon}{nowk}.parquet"
        bnm_savg.write_parquet(bnm_savg_cache)
        bnm_curn.write_parquet(bnm_curn_cache)
        del bnm_savg, bnm_curn, _bnm_dept
        gc.collect()

        # ---- %INC PGM(EIBMRLFM) ------------------------------------------------
        print("\nRunning EIBMRLFM...")
        run_eibmrlfm(
            bnm1_loan_cache=bnm1_loan_cache, bnm1_uloan_cache=bnm1_uloan_cache, lncomm_cache=lncomm_cache,
            provsub_txt_path=provsub_txt_path, lnpay_cache=lnpay_cache, fd_fd_cache=fd_fd_cache,
            bnm_savg_cache=bnm_savg_cache, bnm_curn_cache=bnm_curn_cache,
            deposit_current_cache=current_cache, deposit_fd_cache=deposit_fd_cache,
            forate_cache=forate_cache, foratebkp_cache=foratebkp_cache,
            dciwh_dci_cache=dciwh_dci_cache, bnmk_dciwtb_cache=bnmk_dciwtb_cache, nid_rnid_cache=nid_rnid_cache,
            k1tbl_cache=k1tbl_cache, k3tbl_cache=k3tbl_cache,
            cisln_deposit_cache=cisln_deposit_cache, cisdp_deposit_cache=cisdp_deposit_cache,
            ctx=ctx, output_dir=OUTPUT_DIR,
        )

        # ---- %INC PGM(EIBMTOP5) -------------------------------------------------
        print("\nRunning EIBMTOP5...")
        run_eibmtop5(
            cisln_deposit_cache=cisln_deposit_cache, deposit_current_cache=current_cache,
            cisdp_deposit_cache=cisdp_deposit_cache, deposit_fd_cache=deposit_fd_cache,
            rdate=ctx["rdate"], output_dir=OUTPUT_DIR,
        )
    # temp folder and every temporary parquet are removed here
    _CONVERTED.clear()

    print("\nUploading TOP50 files to EDW via SFTP...")
    sftp_upload_to_edw([(OUTPUT_DIR / local, remote) for local, remote in SFTP_OUTPUTS])

    print("\nEIBMLIQP complete.")


if __name__ == "__main__":
    main()
