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

from pathlib import Path

import polars as pl

from REPTDATE import get_monthly_reptdate_values
# NOTE: The original SAS step computes:
#           REPTDATE = TODAY() - DAY(TODAY())
#       which is TODAY minus its own day-of-month number, i.e. the
#       LAST CALENDAR DAY OF THE PREVIOUS MONTH. This matches
#       get_monthly_reptdate_values() in REPTDATE.py, not the daily
#       get_reptdate_values() variant. REPTDT is only stored via
#       CALL SYMPUT and is never referenced again in this program,
#       so it is derived and printed here for parity/traceability
#       only; it does not affect file naming or processing.

# NOTE: input_date.py (get_latest_file) is NOT used here. The
# source datasets (SAP.PIVB.UTGE231.TXT and SAP.PIVB.BOS1204.CSV)
# are GDG "current generation" (0) references with no date token
# embedded in the filename, so there is no "latest file by date"
# resolution to perform.

# NOTE: output_date.py (build_output_file) is NOT used here. The
# SAS output DSNs (SAP.PIVB.FIDCPR.CSV / .XLS) carry no date
# component; the date-stamped name only appears later, at the FTP
# step, as the remote filename - which is outside the scope of
# this DATA-step conversion.

# ------------------------------------------------------------
# Path setup
# ------------------------------------------------------------
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIVQDCPR")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

EQU_FILE = INPUT_DIR / "UTGE231.txt"
BOS_FILE = INPUT_DIR / "BOS1204.txt"

OUTPUT_CSV_FILE = OUTPUT_DIR / "FIDCPR.csv"
OUTPUT_XLS_FILE = OUTPUT_DIR / "FIDCPR.xls"

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
# DATA REPTDATE step equivalent
# ------------------------------------------------------------
def print_reptdate() -> None:
    """Equivalent of: DATA REPTDATE; REPTDATE = TODAY()-DAY(TODAY());"""
    monthly_reptdate_values = get_monthly_reptdate_values()
    print(f"[REPTDATE] Report date (REPTDT): {monthly_reptdate_values.reptdate}")


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

    print(f"[OUTPUT] CSV written to : {OUTPUT_CSV_FILE}")
    print(f"[OUTPUT] XLS written to : {OUTPUT_XLS_FILE}")

    print("\n[RESULT] Consolidated records (FICODE, UTCIC, UTDOB, BRANCH, UTDOC):")
    for rec in conso:
        print(
            f"{rec['FICODE']},{rec['UTCIC']},{rec['UTDOB']},"
            f"{rec['BRANCH']},{rec['UTDOC']}"
        )

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


if __name__ == "__main__":
    main()
