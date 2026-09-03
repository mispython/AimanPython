"""
Program : EIVQDCPR,py
Purpose : Automate the Financial Inclusion Data Collection
          Package (FIDCPR) report.
          - Reads EQU (fixed-width) and BOS (CSV, DSD) source
            extracts.
          - Consolidates both sources, forcing FICODE=1204 and
            BRANCH=14 on every record (matches SAS CONSO step).
          - Writes:
              * FIDCPR.csv  - plain comma-delimited extract
              * FIDCPR.xls  - 0x05-delimited extract with a
                              3-line banner/header block, used
                              by the legacy mainframe pseudo-XLS
                              technique.
"""

import sys
import paramiko
import polars as pl
import pandas as pd

from pathlib import Path
from datetime import date, datetime, timedelta

# ------------------------------------------------------------------
# NOTE ON REMOVED IMPORTS:
#   from GET_BATCH_DATE import first_date_of_month, get_past_n_date
#   from EDW_TRANSFORMATION import get_sftp_info
#
# These modules are NOT imported directly because importing them also
# executes their unrelated top-level dependencies (sas7bdat library,
# requests/HTTPBasicAuth for Azure DevOps, oracledb, and
# PASSWORD_DECRYPTOR.decrypt_password), several of which are not
# installed on this server and are not needed by this program. Only
# the three functions actually used here — first_date_of_month(),
# get_past_n_date(), and get_sftp_info() — are reproduced below,
# using libraries this program already imports (pandas, datetime).
# This keeps report-date derivation and SFTP behaviour identical to
# the original design.
# ------------------------------------------------------------------

def first_date_of_month(date_str: str) -> str:
    """
    Return the first day of the month (00:00:00) for the given
    "YYYY-MM-DD HH:MM:SS" string. Pure date-math — no external
    dependency beyond the standard library.
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return dt.replace(day=1, hour=0, minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")


def get_past_n_date(date_str: str, n: int) -> str:
    """
    Return the datetime n days before the given "YYYY-MM-DD HH:MM:SS"
    string. Pure date-math — no external dependency beyond the
    standard library.
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return (dt - timedelta(days=n)).strftime("%Y-%m-%d %H:%M:%S")


SFTP_CONTROL_FILE = Path("/sasdata/dwh/control/ctl_dwh_sftp_info.sas7bdat")


def get_sftp_info(host_desc: str):
    """
    Read one SFTP configuration row directly from the DWH control file.

    Original EDW_TRANSFORMATION.get_sftp_info() is not imported because
    importing EDW_TRANSFORMATION also imports oracledb and
    PASSWORD_DECRYPTOR.decrypt_password (which in turn has its own
    dependencies), none of which this program needs and which are not
    available on this server.
    """
    if not SFTP_CONTROL_FILE.exists():
        raise FileNotFoundError(
            f"SFTP control file not found: {SFTP_CONTROL_FILE}"
        )

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


# ------------------------------------------------------------
# COMPUTE REPORT DATE (last day of previous month) via inlined helpers
# ------------------------------------------------------------
_today_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
_first_day_str = first_date_of_month(_today_str)            # "YYYY-MM-DD 00:00:00"
_prev_month_last_day_str = get_past_n_date(_first_day_str, 1)

reptdate: date = datetime.strptime(_prev_month_last_day_str, "%Y-%m-%d %H:%M:%S").date()

# Filename suffix: YYMMDD only, no dashes, no time (e.g. "260731")
_ts: str = reptdate.strftime("%y%m%d")

# ------------------------------------------------------------
# Path setup
# ------------------------------------------------------------
# # Testing Path
# BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
# INPUT_DIR  = Path("/stgsrcsys/host/uat/AII")
# OUTPUT_DIR = BASE_DIR / "output" / "EIVQDCPR"
# OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Production Path
BASE_DIR   = Path("/host_pq/mis")
INPUT_DIR  = BASE_DIR / "input" / "pivb"
OUTPUT_DIR = BASE_DIR / "output" / "pivb"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

EQU_FILE = INPUT_DIR / f"UTGE231_{_ts}.txt"
BOS_FILE = INPUT_DIR / f"BOS1204_{_ts}.txt"

OUTPUT_CSV_FILE = OUTPUT_DIR / f"FIDCPR_{_ts}.csv"
OUTPUT_XLS_FILE = OUTPUT_DIR / f"FIDCPR_{_ts}.xls"

FILE_ENCODING = "latin1"

# ------------------------------------------------------------
# Constants (from CONSO step)
# ------------------------------------------------------------
FICODE_CONST = 1204
BRANCH_CONST = 14

XLS_DELIM = "\x05"          # DLM = '05'X
BANK_TITLE = "PUBLIC INVESTMENT BANK BERHAD"

HEADER_FIELDS = [
    "FI Code",
    "ID Number",
    "Date of Birth",
    "Branch",
    "Date of Account",
]


# ------------------------------------------------------------
# DATA REPTDATE step equivalent (using inlined helpers)
# ------------------------------------------------------------
def print_reptdate() -> None:
    """Equivalent of: DATA REPTDATE; REPTDATE = TODAY()-DAY(TODAY());"""
    print(f"[REPTDATE] Report date (REPTDT): {reptdate}")


# ------------------------------------------------------------
# DATA EQU step equivalent
# ------------------------------------------------------------
def read_equ_file(path: Path) -> list[dict]:
    """
    Fixed-width layout (1-based SAS columns -> 0-based Python slices):
        @001 UTRDT $8.   -> [0:8]    Reporting Date   (not used downstream)
        @009 UTCIC $20.  -> [8:28]   ID Number Character
        @021 UTDOB $8.   -> [20:28]  Date of Birth (embedded within UTCIC range)
        @029 UTDOC $8.   -> [28:36]  Date of Account
    """
    records: list[dict] = []
    with path.open("r", encoding=FILE_ENCODING) as infile:
        for line in infile:
            line = line.rstrip("\n").rstrip("\r")
            if not line.strip():
                continue

            utrdt = line[0:8].strip()   # noqa: F841 (kept for traceability, unused in CONSO)
            utcic = line[8:28].strip()
            utdob = line[20:28].strip()
            utdoc = line[28:36].strip()

            records.append(
                {
                    "UTCIC": utcic,
                    "UTDOB": utdob,
                    "UTDOC": utdoc,
                    "SOURCE": "EQU",
                }
            )
    return records


# ------------------------------------------------------------
# DATA BOS step equivalent
# ------------------------------------------------------------
def read_bos_file(path: Path) -> list[dict]:
    """
    CSV layout (DELIMITER=',' DSD FIRSTOBS=2):
        FICODE : 8.    (overridden by CONSO, not carried forward)
        UTCIC  : $20.
        UTDOB  : $8.
        BRANCH : 8.    (overridden by CONSO, not carried forward)
        UTDOC  : $8.
    """
    bos_df = pl.read_csv(
        path,
        has_header=False,
        skip_rows=1,  # FIRSTOBS=2
        new_columns=["FICODE", "UTCIC", "UTDOB", "BRANCH", "UTDOC"],
        infer_schema_length=0,
    )

    records: list[dict] = []
    for row in bos_df.iter_rows(named=True):
        records.append(
            {
                "UTCIC": (row["UTCIC"] or "").strip(),
                "UTDOB": (row["UTDOB"] or "").strip(),
                "UTDOC": (row["UTDOC"] or "").strip(),
                "SOURCE": None,  # BOS step never assigns SOURCE in the SAS source
            }
        )
    return records


# ------------------------------------------------------------
# DATA CONSO step equivalent: SET BOS EQU; FICODE=1204; BRANCH=14;
# ------------------------------------------------------------
def build_conso(bos_records: list[dict], equ_records: list[dict]) -> list[dict]:
    conso: list[dict] = []
    for rec in bos_records + equ_records:  # SET BOS EQU (stack order preserved)
        conso.append(
            {
                "FICODE": FICODE_CONST,
                "UTCIC": rec["UTCIC"],
                "UTDOB": rec["UTDOB"],
                "BRANCH": BRANCH_CONST,
                "UTDOC": rec["UTDOC"],
            }
        )
    return conso


# ------------------------------------------------------------
# FILE DCPCSV write (comma-delimited, no header, no trailing DLM)
# ------------------------------------------------------------
def write_csv_output(conso: list[dict], path: Path) -> None:
    with path.open("w", encoding=FILE_ENCODING, newline="") as outfile:
        for rec in conso:
            line = ",".join(
                [
                    str(rec["FICODE"]),
                    rec["UTCIC"],
                    rec["UTDOB"],
                    str(rec["BRANCH"]),
                    rec["UTDOC"],
                ]
            )
            outfile.write(line + "\n")


# ------------------------------------------------------------
# FILE DCPXLS write (0x05-delimited, banner + header on first record)
# ------------------------------------------------------------
def write_xls_output(conso: list[dict], path: Path) -> None:
    with path.open("w", encoding=FILE_ENCODING, newline="") as outfile:
        first = True
        for rec in conso:
            if first:
                outfile.write(BANK_TITLE + "\n")
                outfile.write("\n")
                header_line = XLS_DELIM.join(HEADER_FIELDS) + XLS_DELIM
                outfile.write(header_line + "\n")
                first = False

            data_line = (
                XLS_DELIM.join(
                    [
                        str(rec["FICODE"]),
                        rec["UTCIC"],
                        rec["UTDOB"],
                        str(rec["BRANCH"]),
                        rec["UTDOC"],
                    ]
                )
                + XLS_DELIM
            )
            outfile.write(data_line + "\n")


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print_reptdate()

    # DEL01 / DEL02 (IEFBR14 delete-old-output step): equivalent behaviour
    # is achieved by opening the output files in write ("w") mode below,
    # which truncates/overwrites any pre-existing file.

    equ_records = read_equ_file(EQU_FILE)
    bos_records = read_bos_file(BOS_FILE)

    conso = build_conso(bos_records, equ_records)

    write_csv_output(conso, OUTPUT_CSV_FILE)
    write_xls_output(conso, OUTPUT_XLS_FILE)

    # print("\n[RESULT] Consolidated records (FICODE, UTCIC, UTDOB, BRANCH, UTDOC):")
    # for rec in conso:
    #     print(
    #         f"{rec['FICODE']},{rec['UTCIC']},{rec['UTDOB']},"
    #         f"{rec['BRANCH']},{rec['UTDOC']}"
    #     )

    print(f"[OUTPUT] CSV written to : {OUTPUT_CSV_FILE}")
    print(f"[OUTPUT] XLS written to : {OUTPUT_XLS_FILE}")

    # ------------------------------------------------------------
    # //RUNSFTP EXEC COZBATCH ... (FTP TO PIVSVFIL101 - PIVB FILE SERVER)
    # ------------------------------------------------------------
    print("\nUploading reports to PIVSVFIL101 - PIVB File Server via SFTP...")
    sftp_upload_to_pivb(OUTPUT_CSV_FILE, OUTPUT_XLS_FILE)

# ============================================================
# SFTP CONFIGURATION (PIVSVFIL101 - PIVB File Server)
# ============================================================
# NOTE: Original JCL connects directly to sas2lcr@192.168.56.10 via
# cozsftp, using credentials from OPER.PBB.CONTROL(SAS#SFTP) - a
# DIFFERENT control dataset than ctl_dwh_sftp_info.sas7bdat used
# elsewhere in this project.
PIVB_HOST_DESC = "PIVB File Server"

# Remote folder on the PIVB file server (from JCL: cd "...")
PIVB_REMOTE_DIR = "Financial Inclusion Data Collection Package"

# ============================================================
# SFTP UPLOAD TO PIVSVFIL101 - PIVB File Server
# ============================================================
def _pivb_remote_filenames() -> tuple[str, str]:
    """
    Build the two remote filenames per the JCL PUT lines:
        PUT //SAP.PIVB.FIDCPR.CSV   1204D.csv
        PUT //SAP.PIVB.FIDCPR.XLS   1204D(%ODD.%OMM.%OYY.).xls

    NOTE: Original JCL's %ODD.%OMM.%OYY. tokens represent the FTP
    run date, not the report date. This has been changed on request
    to instead follow REPTDT (the same date used to name the local
    input/output files, e.g. UTGE231_260731.txt), so the remote XLS
    filename date matches the report period rather than today's date.
    """
    # today = datetime.now() - timedelta(days=1)
    csv_name = "1204D.csv"
    xls_name = f"1204D({reptdate:%d%m%y}).xls"
    return csv_name, xls_name


def sftp_upload_to_pivb(csv_path: Path, xls_path: Path) -> None:
    """
    Upload FIDCPR.csv / FIDCPR.xls to the PIVB file server.

    Equivalent of the JCL RUNSFTP step:
        //RUNSFTP  EXEC COZBATCH
        export PASSWD_DSN='OPER.PBB.CONTROL(SAS#SFTP)'
        $coz_bin/cozsftp $ssh_opts -b- sas2lcr@192.168.56.10 <<EOB
        lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
        lzopts linerule=$lr
        cd "Financial Inclusion Data Collection Package"
        PUT //SAP.PIVB.FIDCPR.CSV   1204D.csv
        PUT //SAP.PIVB.FIDCPR.XLS   1204D(%ODD.%OMM.%OYY.).xls
        EOB
    """
    sftp_id, sftp_pw, host_ip, host_key = get_sftp_info(PIVB_HOST_DESC)

    ssh = paramiko.SSHClient()
    # NOTE: HOST_KEY format unconfirmed (see EIWFRMCR.py note) - using
    # AutoAddPolicy() in the interim.
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print(f"  Connecting to PIVB host {host_ip} as {sftp_id} ...")
    ssh.connect(hostname=host_ip, username=sftp_id, password=sftp_pw)
    sftp = ssh.open_sftp()

    try:
        sftp.chdir(PIVB_REMOTE_DIR)
    except IOError:
        sftp.close()
        ssh.close()
        sys.exit(f"Aborting: Remote PIVB folder '{PIVB_REMOTE_DIR}' not found")

    csv_name, xls_name = _pivb_remote_filenames()

    print(f"  SFTP upload: {csv_path.name} -> {PIVB_REMOTE_DIR}/{csv_name}")
    sftp.put(str(csv_path), csv_name)

    print(f"  SFTP upload: {xls_path.name} -> {PIVB_REMOTE_DIR}/{xls_name}")
    sftp.put(str(xls_path), xls_name)

    sftp.close()
    ssh.close()
    print("  PIVB upload complete.")


if __name__ == "__main__":
    main()
