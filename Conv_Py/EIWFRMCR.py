"""
Program : EIWFRMCR
Purpose : Generate Foreign Remittance Report (Inward & Outward) for
          compliance checking. Reads remittance transactions, filters
          foreign ('F') transactions with STATUS IN ('TI','TO','BR'),
          and splits them into an INWARD extract (STATUS='TI') and an
          OUTWARD extract (STATUS IN ('TO','BR')).
"""

from pathlib import Path

import pandas as pd
import polars as pl

from REPTDATE import get_reptdate_values
from input_date import get_latest_file

# ============================================================
# PATH CONFIGURATION
# ============================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output" / "EIWFRMCR"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RMT_PREFIX = "remtran"

# Original DSNs: SAP.PBB.COMP.INWARD.REPT / SAP.PBB.COMP.OUTWARD.REPT
# No date component is present in these SAS-catalogued output DSNs, so
# output_date.py's date-stamped naming is not applicable here; filenames
# are static.
INWARD_REPORT_FILE = OUTPUT_DIR / "SAP_PBB_COMP_INWARD_REPT.txt"
OUTWARD_REPORT_FILE = OUTPUT_DIR / "SAP_PBB_COMP_OUTWARD_REPT.txt"

# Delimiter used in SAS: DLM = '05'X (hex 05 / ASCII ENQ control character)
DLM = chr(0x05)

# NOTE: Original JCL DELETE step (PGM=IEFBR14) removed the previously
# catalogued INWARD/OUTWARD datasets before the SAS step ran. This is not
# needed in Python: writing the output files below (open in write mode)
# recreates/overwrites them automatically.

# ============================================================
# REPORT DATE DERIVATION
# ============================================================
# NOTE: Original SAS reads DEPOSIT.REPTDATE (DD DEPOSIT, DSN=SAP.PBB.MNITB)
# to derive REPTYEAR / REPTMON / NOWK via macro variables, then builds the
# input dataset name RMT.REMTRAN&REPTMON&NOWK&REPTYEAR. No reptdate.parquet
# exists in production, so report-date values are derived through
# REPTDATE.py instead, and the latest RMT input file is resolved directly
# by filename (see below) rather than by manually reconstructing the name.
reptdate_values = get_reptdate_values()

REPTYEAR = reptdate_values.reptyear
REPTMON = reptdate_values.reptmon
RDATE = reptdate_values.rdate

# NOTE: Original SAS NOWK derivation (kept here for reference only; no
# longer needed since get_latest_file() below resolves the RMT file by
# scanning the input directory for the most recent remtran{MM}{W}{YY} file):
#
# DATA REPTDATE;
#    SET DEPOSIT.REPTDATE;
#    SELECT(DAY(REPTDATE));
#       WHEN(8)   CALL SYMPUT('NOWK', PUT('1', $1.));
#       WHEN(15)  CALL SYMPUT('NOWK', PUT('2', $1.));
#       WHEN(22)  CALL SYMPUT('NOWK', PUT('3', $1.));
#       OTHERWISE CALL SYMPUT('NOWK', PUT('4', $1.));
#    END;
#    CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR2.));
#    CALL SYMPUT('REPTMON',PUT(MONTH(REPTDATE),Z2.));
# RUN;

print(f"Report Date  : {RDATE}")
print(f"Report Year  : {REPTYEAR}")
print(f"Report Month : {REPTMON}")

# ============================================================
# LOCATE LATEST RMT (REMITTANCE TRANSACTION) FILE
# ============================================================
# Original: SET RMT.REMTRAN&REPTMON&NOWK&REPTYEAR;
# Filename pattern: remtran{MM}{W}{YY}.sas7bdat  (e.g. remtran07126.sas7bdat)
rmt_file = get_latest_file(INPUT_DIR, prefix=RMT_PREFIX)
print(f"Input RMT File : {rmt_file}")


# ============================================================
# FIELD FORMATTING HELPERS (SAS PUT var +(-1) DLM +(-1) idiom)
# ============================================================
def _fmt_str(value) -> str:
    """Format a character field: trim leading/trailing blanks.

    Equivalent of SAS `var +(-1)` trimming the trailing blank that list
    output would otherwise add before the delimiter.
    """
    if value is None:
        return ""
    return str(value).strip()


def _fmt_num(value) -> str:
    """Format a numeric field the way SAS's default (unformatted) PUT would.

    SAS stores all numerics as Float64 internally; whole values are written
    without a trailing '.0'.
    """
    if value is None:
        return ""
    if isinstance(value, float):
        if value != value:  # NaN
            return ""
        if value.is_integer():
            return str(int(value))
    return str(value).strip()


def _fmt_date(value) -> str:
    """Format a transaction date field.

    The original PUT statement carries no explicit date format, so the
    variable's stored/permanent format is assumed. DDMMYYYY is used here
    for report readability.
    """
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%d/%m/%Y")
    return str(value).strip()


def _build_line(fields: list) -> str:
    """Join fields with DLM, including a trailing DLM after the last field
    (matches the SAS `... +(-1)DLM+(-1)` pattern repeated on every field,
    including the last one in each PUT statement)."""
    return DLM.join(fields) + DLM


# ============================================================
# HEADER DEFINITIONS
# ============================================================
INWARD_HEADERS = [
    "TRANSACTION DATE",
    "BRANCH NAME",
    "REFERENCE NO",
    "BENEFICIARY NAME 1",
    "BENEFICIARY NAME 2",
    "BENEFICIARY NAME 3",
    "BENEFICIARY NAME 4",
    "ORDERING CUSTOMER 1",
    "ORDERING CUSTOMER 2",
    "ORDERING CUSTOMER 3",
    "ORDERING CUSTOMER 4",
    "AMOUNT (FCY)",
    "AMOUNT (MYR)",
    "ACCOUNT NO",
    "CURRENCY",
    "PAYMENT MODE",
    "STAFF ID",
    "RESIDENCY",
    "NATIONALITY",
    "BMR STATUS",
    "COUNTRY",
    "CBOP CODE",
]

OUTWARD_HEADERS = [
    "TRANSACTION DATE",
    "BRANCH NAME",
    "REFERENCE NO",
    "ORDERING CUSTOMER 1",
    "ORDERING CUSTOMER 2",
    "ORDERING CUSTOMER 3",
    "ORDERING CUSTOMER 4",
    "ORDERING CUSTOMER ACCOUNT NO",
    "BENEFICIARY NAME 1",
    "BENEFICIARY NAME 2",
    "BENEFICIARY NAME 3",
    "BENEFICIARY NAME 4",
    "AMOUNT (FCY)",
    "AMOUNT (MYR)",
    "ACCOUNT NO",
    "CURRENCY",
    "PAYMENT MODE",
    "STAFF ID",
    "RESIDENCY",
    "NATIONALITY",
    "BMR STATUS",
    "COUNTRY",
    "CBOP CODE",
]


# ============================================================
# DETAIL LINE BUILDERS
# ============================================================
def _build_inward_line(row: dict) -> str:
    """Equivalent of the SAS `DATA _NULL_; SET INWARD;` PUT statement."""
    fields = [
        _fmt_date(row["LASTTRAN"]),
        _fmt_str(row["BRANCHABB"]),
        _fmt_num(row["SERIAL"]),
        _fmt_str(row["BNAD1"]),
        _fmt_str(row["BNAD2"]),
        _fmt_str(row["BNAD3"]),
        _fmt_str(row["BNAD4"]),
        _fmt_str(row["ANAD1"]),
        _fmt_str(row["ANAD2"]),
        _fmt_str(row["ANAD3"]),
        _fmt_str(row["ANAD4"]),
        _fmt_num(row["FORAMT"]),
        _fmt_num(row["AMOUNT"]),
        _fmt_num(row["ACCTNO"]),
        _fmt_str(row["CURRENCY"]),
        _fmt_str(row["PAYMODE"]),
        _fmt_str(row["USERID"]),
        _fmt_str(row["RESIDENT"]),
        _fmt_str(row["APPLNATIONAL"]),
        _fmt_str(row["BMRSTATUS"]),
        _fmt_str(row["COUNTRY"]),
        _fmt_str(row["ADMIN"]),
    ]
    return _build_line(fields)


def _build_outward_line(row: dict) -> str:
    """Equivalent of the SAS `DATA _NULL_; SET OUTWARD;` PUT statement."""
    fields = [
        _fmt_date(row["LASTTRAN"]),
        _fmt_str(row["BRANCHABB"]),
        _fmt_num(row["SERIAL"]),
        _fmt_str(row["ANAD1"]),
        _fmt_str(row["ANAD2"]),
        _fmt_str(row["ANAD3"]),
        _fmt_str(row["ANAD4"]),
        _fmt_str(row["PAYREF"]),
        _fmt_str(row["BNAD1"]),
        _fmt_str(row["BNAD2"]),
        _fmt_str(row["BNAD3"]),
        _fmt_str(row["BNAD4"]),
        _fmt_num(row["FORAMT"]),
        _fmt_num(row["AMOUNT"]),
        _fmt_num(row["ACCTNO"]),
        _fmt_str(row["CURRENCY"]),
        _fmt_str(row["PAYMODE"]),
        _fmt_str(row["USERID"]),
        _fmt_str(row["RESIDENT"]),
        _fmt_str(row["APPLNATIONAL"]),
        _fmt_str(row["BMRSTATUS"]),
        _fmt_str(row["COUNTRY"]),
        _fmt_str(row["ADMIN"]),
    ]
    return _build_line(fields)


def _write_report(
    df: pl.DataFrame,
    headers: list,
    build_line_fn,
    output_path: Path,
) -> list:
    """Write header line + one detail line per row.

    NOTE: Original DD is RECFM=FB, LRECL=1000 (fixed-block, no ASA carriage
    control - this is a plain delimited data extract, not a printed report).
    Fixed-length byte padding to LRECL=1000 is not reproduced here; each
    record is written as a newline-terminated delimited line, consistent
    with how this extract is consumed downstream (read/split by DLM).
    """
    lines = [_build_line(headers)]
    for row in df.iter_rows(named=True):
        lines.append(build_line_fn(row))

    output_path.write_text("\n".join(lines) + "\n", encoding="latin1")
    return lines


# ============================================================
# READ RMT (REMITTANCE TRANSACTION) DATA
# ============================================================
rmt_pdf = pd.read_sas(rmt_file, encoding="latin1")
rmt_df = pl.from_pandas(rmt_pdf)

# Normalize fixed-width character columns (SAS pads char values with blanks)
str_cols = [c for c, dt in zip(rmt_df.columns, rmt_df.dtypes) if dt == pl.Utf8]
if str_cols:
    rmt_df = rmt_df.with_columns([pl.col(c).str.strip_chars() for c in str_cols])

# ============================================================
# FILTER & SPLIT: INWARD / OUTWARD
# ============================================================
# Original:
#   WHERE REMTYPE = 'F' AND STATUS IN ('TI','TO','BR');
#   IF STATUS = 'TI' THEN OUTPUT INWARD; ELSE OUTPUT OUTWARD;
#   KEEP BRANCHABB STATUS SERIAL LASTTRAN ANAD1 ANAD2 ANAD3 ANAD4
#        BNAD1 BNAD2 BNAD3 BNAD4 AMOUNT FORAMT PAYREF
#        ACCTNO CURRENCY PAYMODE USERID RESIDENT APPLNATIONAL
#        BMRSTATUS COUNTRY ADMIN;
KEEP_COLUMNS = [
    "BRANCHABB", "STATUS", "SERIAL", "LASTTRAN",
    "ANAD1", "ANAD2", "ANAD3", "ANAD4",
    "BNAD1", "BNAD2", "BNAD3", "BNAD4",
    "AMOUNT", "FORAMT", "PAYREF",
    "ACCTNO", "CURRENCY", "PAYMODE", "USERID", "RESIDENT",
    "APPLNATIONAL", "BMRSTATUS", "COUNTRY", "ADMIN",
]

filtered_df = rmt_df.filter(
    (pl.col("REMTYPE") == "F") & (pl.col("STATUS").is_in(["TI", "TO", "BR"]))
).select(KEEP_COLUMNS)

inward_df = filtered_df.filter(pl.col("STATUS") == "TI")
outward_df = filtered_df.filter(pl.col("STATUS") != "TI")  # STATUS IN ('TO','BR')

# ============================================================
# WRITE OUTPUT FILES
# ============================================================
inward_lines = _write_report(inward_df, INWARD_HEADERS, _build_inward_line, INWARD_REPORT_FILE)
outward_lines = _write_report(outward_df, OUTWARD_HEADERS, _build_outward_line, OUTWARD_REPORT_FILE)

# ============================================================
# TERMINAL OUTPUT
# ============================================================
print(f"\nInward Report Output Path  : {INWARD_REPORT_FILE}")
print(f"Inward Records Written     : {inward_df.height}")
print("Inward Report Preview:")
for line in inward_lines[:5]:
    print(line.replace(DLM, "|"))

print(f"\nOutward Report Output Path : {OUTWARD_REPORT_FILE}")
print(f"Outward Records Written    : {outward_df.height}")
print("Outward Report Preview:")
for line in outward_lines[:5]:
    print(line.replace(DLM, "|"))

# ============================================================
# NOTE: Original JCL step RUNSFTP transmits the two output files to the
# Data Report Repository (DRR) system via SFTP. This is an external file
# transfer step outside the scope of this SAS-to-Python data conversion
# and is not implemented here:
#
# //RUNSFTP  EXEC COZBATCH
# //CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)
# //           DD *
# lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
# lzopts linerule=$lr
# CD CD-BRC
# PUT //SAP.PBB.COMP.INWARD.REPT   \
#           ForeignInwardRemittances_%OYYYY.%OMM.%ODD..txt
# PUT //SAP.PBB.COMP.OUTWARD.REPT  \
#           ForeignOutwardRemittances_%OYYYY.%OMM.%ODD..txt
# EOB
