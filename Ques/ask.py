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

from pathlib import Path
from datetime import date, datetime, timedelta

from GET_BATCH_DATE import first_date_of_month, get_past_n_date
from EDW_TRANSFORMATION import get_sftp_info
# from REPTDATE import get_monthly_reptdate_values
# NOTE: The original SAS step computes:
#           REPTDATE = TODAY() - DAY(TODAY())
#       which is TODAY minus its own day-of-month number, i.e. the
#       LAST CALENDAR DAY OF THE PREVIOUS MONTH. This matches
#       get_monthly_reptdate_values() in REPTDATE.py, not the daily
#       get_reptdate_values() variant. REPTDT is only stored via
#       CALL SYMPUT and is never referenced again in this program,
#       so it is derived and printed here for parity/traceability
#       only; it does not affect file naming or processing.

# ------------------------------------------------------------
# COMPUTE REPORT DATE (last day of previous month)
# ------------------------------------------------------------
today_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
first_day_str = first_date_of_month(today_str)                  # e.g. "2026-08-01 00:00:00"
prev_month_last_day_str = get_past_n_date(first_day_str, 1)     # e.g. "2026-07-31 00:00:00"
reptdate = datetime.strptime(prev_month_last_day_str, "%Y-%m-%d %H:%M:%S").date()

# Format suffix as DDMMYY (e.g., 260901)
# date_suffix = reptdate.strftime("%y%m%d")
reptdate = date.today() - timedelta(days=1)
_ts = reptdate.strftime("%y%m%d")

# ------------------------------------------------------------
# Path setup
# ------------------------------------------------------------
# # Testing Path
# BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
# # INPUT_DIR = BASE_DIR / "input"
# # OUTPUT_DIR = BASE_DIR / "output"

# INPUT_DIR  = Path("/stgsrcsys/host/uat/AII")
# OUTPUT_DIR = BASE_DIR / "output" / "EIVQDCPR"
# OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# EQU_FILE = INPUT_DIR / "UTGE231.txt"
# BOS_FILE = INPUT_DIR / "BOS1204.txt"

# Production Path
BASE_DIR   = Path("/host_pq/mis")
INPUT_DIR  = BASE_DIR / "input" / "pivb"
OUTPUT_DIR = BASE_DIR / "output" / "pivb"

# # Generate time stamp
# reptdate = date.today() - timedelta(days=1)
# _ts = reptdate.strftime("%y%m%d")

_ts = date(2026, 7, 31)

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
# DATA REPTDATE step equivalent (using GET_BATCH_DATE)
# ------------------------------------------------------------
def print_reptdate() -> None:
    """Equivalent of: DATA REPTDATE; REPTDATE = TODAY()-DAY(TODAY());"""
    # Get today's date in the format expected by GET_BATCH_DATE functions
    today_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # First day of current month
    first_day_str = first_date_of_month(today_str)   # returns "YYYY-MM-DD 00:00:00"
    
    # Subtract 1 day to get last day of previous month
    prev_month_last_day_str = get_past_n_date(first_day_str, 1)
    
    # Convert to date object for printing (or just print the string)
    reptdate = datetime.strptime(prev_month_last_day_str, "%Y-%m-%d %H:%M:%S").date()
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
    # This is a separate JCL step invoking cozsftp to push the two output
    # files to the PIVB file server, renaming them on arrival:
    #   PUT //SAP.PIVB.FIDCPR.CSV   1204D.csv
    #   PUT //SAP.PIVB.FIDCPR.XLS   1204D(%ODD.%OMM.%OYY.).xls
    # It is infrastructure/transport, not part of the SAS DATA step logic,
    # and is left as a placeholder only:
    #
    # def sftp_upload_outputs(csv_path: Path, xls_path: Path) -> None:
    #     """Upload OUTPUT_CSV_FILE / OUTPUT_XLS_FILE to PIVSVFIL101."""
    #     raise NotImplementedError(
    #         "SFTP transport step - implement using project-approved "
    #         "SFTP client/credentials if this step needs to be automated."
    #     )

# ============================================================
# SFTP CONFIGURATION (PIVSVFIL101 - PIVB File Server)
# ============================================================
# NOTE: Original JCL connects directly to sas2lcr@192.168.56.10 via
# cozsftp, using credentials from OPER.PBB.CONTROL(SAS#SFTP) - a
# DIFFERENT control dataset than ctl_dwh_sftp_info.sas7bdat used
# elsewhere in this project. The HOST_DESC value below is a
# placeholder; CONFIRM with infra team which row in
# ctl_dwh_sftp_info.sas7bdat (if any) corresponds to this PIVB file
# server before production use. If no equivalent row exists, this
# server's credentials may need to be sourced differently.
PIVB_HOST_DESC = "PIVSVFIL101"

# Remote folder on the PIVB file server (from JCL: cd "...")
PIVB_REMOTE_DIR = "Financial Inclusion Data Collection Package"

# ============================================================
# SFTP UPLOAD TO PIVSVFIL101
# ============================================================
def _pivb_remote_filenames() -> tuple[str, str]:
    """
    Build the two remote filenames per the JCL PUT lines:
        PUT //SAP.PIVB.FIDCPR.CSV   1204D.csv
        PUT //SAP.PIVB.FIDCPR.XLS   1204D(%ODD.%OMM.%OYY.).xls

    The CSV name is static. The XLS name embeds the transfer run date
    (%O-prefixed = run date, not the report date REPTDT) as
    DD.MM.YY inside literal parentheses.
    """
    today = datetime.now()
    csv_name = "1204D.csv"
    xls_name = f"1204D({today:%d.%m.%y}).xls"
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

# ------------------------------------------------------------
# //RUNSFTP EXEC COZBATCH ... (FTP TO PIVSVFIL101 - PIVB FILE SERVER)
# ------------------------------------------------------------
print("\nUploading reports to PIVSVFIL101 via SFTP...")
sftp_upload_to_pivb(OUTPUT_CSV_FILE, OUTPUT_XLS_FILE)


if __name__ == "__main__":
    main()
