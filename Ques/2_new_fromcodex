# !/usr/bin/env python3
"""
Program  : EIBMECPT.py
Purpose  : Create ECP Transaction Dataset & export to SAS Warehouse
Desc     : Read daily ECP flat file, append to weekly ECP Parquet store,
           then produce CIS and TRN subset Parquet files for downstream FTP.
Note     : The FTP steps (PUT to EDW) in the original JCL are out of scope
           for this Python conversion and must be handled by the scheduler /
           operations team via the equivalent SFTP mechanism.

Tested: 12 May 2025
"""

import os
import sys
import struct
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import duckdb

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# Input flat file  (mainframe fixed-width, RECFM=FB LRECL=1150+)
INPUT_DIR = BASE_DIR / "input/uat" / "DP_PBECP_20260510"

# # Weekly ECP Parquet store (equivalent of ECPOUT library)
# ECPOUT_DIR      = BASE_DIR / "ecpout"

# Output Parquet files (equivalent of SAP.PBB.ECPTRN.ETRNFTP / ECISFTP)
OUTPUT_DIR      = BASE_DIR / "output" / "EIBMECPT"
# ETRNFTP_FILE    = OUTPUT_DIR / "ETRNFTP.parquet"
# ECISFTP_FILE    = OUTPUT_DIR / "ECISFTP.parquet"
# ETRNFTP_FILE    = OUTPUT_DIR / "ETRNFTP.csv"
# ECISFTP_FILE    = OUTPUT_DIR / "ECISFTP.csv"
ETRNFTP_FILE    = OUTPUT_DIR / "ETRNFTP.txt"
ECISFTP_FILE    = OUTPUT_DIR / "ECISFTP.txt"

# Weekly ECP Parquet store (equivalent of ECPOUT library)
ECPOUT_DIR      = OUTPUT_DIR / "ECPOUT"

# Ensure directories exist
ECPOUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# DATE / WEEK DERIVATION  (equivalent of DATA REPTDATE step)
# =============================================================================
# REPTDATE = TODAY() - 1  (SAS: OPTIONS YEARCUTOFF=1950)
reptdate: date = date.today() - timedelta(days=1)

day_of_month = reptdate.day
if 1 <= day_of_month <= 8:
    nowk = 1
elif 9 <= day_of_month <= 15:
    nowk = 2
elif 16 <= day_of_month <= 22:
    nowk = 3
else:
    nowk = 4

# Macro variable equivalents
REPTYEAR = reptdate.strftime("%y")           # 2-digit year  (PUT(REPTDATE,YEAR2.))
REPTMON  = reptdate.strftime("%m")           # zero-padded month (Z2.)
REPTDAY  = reptdate.strftime("%d")           # zero-padded day   (Z2.)
REPTDT   = reptdate.toordinal()              # raw SAS date integer equivalent (used for filter)
RDATE    = reptdate                          # date object used in DATA ECP step
NOWK     = f"{nowk:01d}"                     # zero-padded 1-digit week number (Z1.)

# Weekly dataset name  (e.g.  ECP0312  for March week 1, year 2025)
WEEKLY_NAME = f"ECP{REPTMON}{NOWK}"         # e.g. ECP031
# WEEKLY_FILE = ECPOUT_DIR / f"{WEEKLY_NAME}.parquet"
# WEEKLY_FILE = ECPOUT_DIR / f"{WEEKLY_NAME}.csv"
WEEKLY_FILE = ECPOUT_DIR / f"{WEEKLY_NAME}.parquet"

# Work dataset names  (e.g. ECPTRAN0312YY, ECP0312YY)
TRN_WORK_NAME = f"ECPTRAN{REPTMON}{NOWK}{REPTYEAR}"
CIS_WORK_NAME = f"ECP{REPTMON}{NOWK}{REPTYEAR}"

print(f"[EIBMECPT] reptdate={reptdate}  REPTMON={REPTMON}  NOWK={NOWK}  REPTYEAR={REPTYEAR}")
print(f"[EIBMECPT] Weekly store : {WEEKLY_FILE}")
print(f"[EIBMECPT] TRN output   : {ETRNFTP_FILE}")
print(f"[EIBMECPT] CIS output   : {ECISFTP_FILE}")

# =============================================================================
# COLUMN KEEP LISTS  (equivalent of %LET TRN / %LET CIS macro variables)
# =============================================================================
TRN_COLS = [
    "ACCTNO", "SERIAL", "TRANDATE", "BENEBANKBIC", "BENEACCTNO",
    "AMOUNT", "PAYORCORP", "PAYDESC", "PAYORCORPREF", "BENEREF",
    "STATUS", "RSONDESC",
]

CIS_COLS = [
    "ACCTNO", "SERIAL", "BENENAME", "BNAD", "BENEID", "BENEIDIND",
    "MOBIPHON", "EMAILADD",
]

# =============================================================================
# PACKED-DECIMAL DECODER  (SAS PD9.2 = 9-byte BCD packed-decimal, 2 dec places)
# =============================================================================
def decode_packed_decimal(raw: bytes, decimal_places: int = 2) -> float:
    """
    Decode a mainframe packed-decimal (BCD) byte string.
    Last nibble: 0xC = positive, 0xD = negative, 0xF = unsigned positive.
    Each byte (except the last) contributes two decimal digits.
    The last byte contributes one digit (high nibble) and the sign (low nibble).
    """
    if not raw or all(b == 0 for b in raw):
        return 0.0

    digits = ""
    sign_nibble = 0xF  # default: unsigned positive

    for i, byte in enumerate(raw):
        high = (byte >> 4) & 0x0F
        low  = byte & 0x0F
        if i < len(raw) - 1:
            # Guard: both nibbles must be valid BCD digits (0-9)
            if high > 9 or low > 9:
                return 0.0
            digits += str(high) + str(low)
        else:
            # Last byte: high nibble = last digit, low nibble = sign
            if high > 9:
                return 0.0
            digits += str(high)
            sign_nibble = low

    if not digits:
        return 0.0

    value = int(digits) / (10 ** decimal_places)
    if sign_nibble == 0xD:          # negative
        value = -value
    # 0xC = positive, 0xF = unsigned positive — both treated as positive
    return value

# =============================================================================
# FIXED-WIDTH FLAT FILE READER  (equivalent of DATA ECP / INFILE DPECPT step)
# Column positions are 1-based in SAS → converted to 0-based slicing in Python.
# LRECL is at least 1150 bytes based on the widest field @1101 + 50 chars.
# =============================================================================
#
# SAS INPUT statement field map:
#   @0001  ACCTNO        11.       numeric  11 bytes (chars '0'-'9')
#   @0012  SERIAL        $16.      char     16 bytes
#   @0092  TRANYY        4.        numeric   4 bytes (year)
#   @0097  TRANMM        2.        numeric   2 bytes (month)
#   @0100  TRANDD        2.        numeric   2 bytes (day)
#   @0110  AMOUNT        PD9.2     packed    9 bytes
#   @0129  PAYORCORPREF  $16.      char     16 bytes
#   @0145  PAYORCORP     $80.      char     80 bytes
#   @0225  BENEBANKBIC   $11.      char     11 bytes
#   @0257  BENEREF       $16.      char     16 bytes
#   @0273  BENENAME      $120.     char    120 bytes
#   @0393  BENEACCTNO    $35.      char     35 bytes
#   @0428  BENEID        $18.      char     18 bytes
#   @0446  BENEIDIND     $2.       char      2 bytes
#   @0492  BNAD          $160.     char    160 bytes
#   @0747  PAYDESC       $140.     char    140 bytes
#   @0930  STATUS        $2.       char      2 bytes
#   @0932  RSONDESC      $40.      char     40 bytes
#   @1085  MOBIPHON      $16.      char     16 bytes
#   @1101  EMAILADD      $50.      char     50 bytes
#
# Note: All @positions are 1-based; Python slices use [pos-1 : pos-1+length].

FIELD_SPECS = [
    # (field_name,    start_1based, length, type)  type: 'num'|'str'|'pd'
    ("ACCTNO",        1,    11, "num"),
    ("SERIAL",       12,    16, "str"),
    ("TRANYY",       92,     4, "num"),
    ("TRANMM",       97,     2, "num"),
    ("TRANDD",      100,     2, "num"),
    ("AMOUNT",      110,     9, "pd"),    # packed-decimal PD9.2
    ("PAYORCORPREF",129,    16, "str"),
    ("PAYORCORP",   145,    80, "str"),
    ("BENEBANKBIC", 225,    11, "str"),
    ("BENEREF",     257,    16, "str"),
    ("BENENAME",    273,   120, "str"),
    ("BENEACCTNO",  393,    35, "str"),
    ("BENEID",      428,    18, "str"),
    ("BENEIDIND",   446,     2, "str"),
    ("BNAD",        492,   160, "str"),
    ("PAYDESC",     747,   140, "str"),
    ("STATUS",      930,     2, "str"),
    ("RSONDESC",    932,    40, "str"),
    ("MOBIPHON",   1085,    16, "str"),
    ("EMAILADD",   1101,    50, "str"),
]

SOURCE_ENCODING = "cp037"          # Mainframe EBCDIC. Change to "latin-1"/"utf-8" only if the source is not EBCDIC.
RECORD_LENGTH = max(start + length - 1 for _, start, length, _ in FIELD_SPECS)

# EBCDIC space byte (0x40) used to right-pad short records to RECORD_LENGTH.
EBCDIC_SPACE = b"\x40"


def decode_text(raw: bytes, encoding: str = SOURCE_ENCODING) -> str:
    """Decode a fixed-width EBCDIC text field and strip trailing spaces."""
    return raw.decode(encoding, errors="replace").rstrip()


def decode_zoned_number(raw: bytes, encoding: str = SOURCE_ENCODING) -> int:
    """Decode ordinary numeric fields stored as display digits in EBCDIC."""
    text = raw.decode(encoding, errors="replace").strip()
    return int(text) if text else 0


def iter_fixed_width_records(filepath: Path, record_length: int):
    """
    Yield complete logical records from a mainframe FB (Fixed Block) binary file.

    Mainframe FB files are raw binary streams where each record occupies exactly
    LRECL bytes with NO newline or record delimiter between records.  Earlier
    versions attempted newline-splitting and RDW detection, which corrupted
    EBCDIC data because EBCDIC 0x25 (which maps to newline in many codecs) is a
    valid data byte inside fields.

    Strategy (in priority order):
      1. FB  — raw binary: total file size is an exact multiple of record_length.
               Slice every record_length bytes directly from the raw buffer.
      2. VB  — Variable Block with 4-byte RDW prefix per record.  Only attempted
               when the FB check fails and the RDW parse covers the entire file.
      3. NL  — Newline-delimited fallback (text-mode transfers / ASCII files).
               Only used when neither FB nor VB applies.  Each non-empty line is
               padded / truncated to record_length bytes encoded in SOURCE_ENCODING
               so that field extraction remains position-accurate.

    This function is the single authoritative place for record iteration and is
    reusable across all converted mainframe programs in this project.
    """
    data = filepath.read_bytes()
    if not data:
        return

    # ------------------------------------------------------------------
    # Strategy 1 — FB: file length is an exact multiple of record_length.
    # This is the normal case for untouched mainframe binary extracts.
    # ------------------------------------------------------------------
    if len(data) % record_length == 0:
        for offset in range(0, len(data), record_length):
            record = data[offset : offset + record_length]
            yield record.ljust(record_length, EBCDIC_SPACE)
        return

    # ------------------------------------------------------------------
    # Strategy 2 — VB: each record is prefixed with a 4-byte RDW.
    # RDW bytes 0-1 = total record length (including the 4-byte RDW itself).
    # Bytes 2-3 = 0x0000 (reserved).
    # ------------------------------------------------------------------
    rdw_records: list[bytes] = []
    pos = 0
    valid_rdw = True
    while pos < len(data):
        if pos + 4 > len(data):
            valid_rdw = False
            break
        rdw_length = int.from_bytes(data[pos : pos + 2], byteorder="big")
        if rdw_length < 4 or pos + rdw_length > len(data):
            valid_rdw = False
            break
        rdw_records.append(data[pos + 4 : pos + rdw_length])
        pos += rdw_length

    if valid_rdw and pos == len(data) and rdw_records:
        for record in rdw_records:
            yield record.ljust(record_length, EBCDIC_SPACE)[:record_length]
        return

    # ------------------------------------------------------------------
    # Strategy 3 — Newline-delimited fallback (ASCII / text-mode transfer).
    # Re-encode each line back to SOURCE_ENCODING so that byte-position
    # slicing in parse_ecp_flat_file stays accurate.
    # ------------------------------------------------------------------
    for raw_line in data.splitlines():
        if not raw_line:
            continue
        # Re-encode to SOURCE_ENCODING so positional slicing is consistent.
        try:
            text_line = raw_line.decode(SOURCE_ENCODING, errors="replace")
            encoded_line = text_line.encode(SOURCE_ENCODING, errors="replace")
        except Exception:
            encoded_line = raw_line
        yield encoded_line.ljust(record_length, EBCDIC_SPACE)[:record_length]


def parse_ecp_flat_file(filepath: Path, reptdate_val: date) -> pl.DataFrame:
    """
    Read the ECP mainframe flat file and return a Polars DataFrame.
    Equivalent of the DATA ECP / INFILE DPECPT INPUT ... step.
    """
    rows = []
    for line in iter_fixed_width_records(filepath, RECORD_LENGTH):
        record: dict = {}
        for (name, start1, length, ftype) in FIELD_SPECS:
            s = start1 - 1  # 0-based start
            e = s + length
            chunk = line[s:e]

            if ftype == "str":
                record[name] = decode_text(chunk)
            elif ftype == "num":
                try:
                    record[name] = decode_zoned_number(chunk)
                except ValueError:
                    record[name] = 0
            elif ftype == "pd":
                try:
                    record[name] = decode_packed_decimal(chunk, decimal_places=2)
                except Exception:
                    record[name] = 0.0

        # TRANDATE = MDY(TRANMM, TRANDD, TRANYY)  FORMAT YYMMDD10.
        try:
            trandate = date(record["TRANYY"], record["TRANMM"], record["TRANDD"])
        except (ValueError, KeyError):
            trandate = None

        record["TRANDATE"] = trandate
        record["REPTDATE"] = reptdate_val

        # DROP TRANYY TRANMM TRANDD
        for drop_col in ("TRANYY", "TRANMM", "TRANDD"):
            record.pop(drop_col, None)

        rows.append(record)

    if not rows:
        # Return empty DataFrame with correct schema
        schema = {
            "ACCTNO": pl.Int64, "SERIAL": pl.Utf8, "TRANDATE": pl.Date,
            "REPTDATE": pl.Date, "AMOUNT": pl.Float64,
            "PAYORCORPREF": pl.Utf8, "PAYORCORP": pl.Utf8,
            "BENEBANKBIC": pl.Utf8, "BENEREF": pl.Utf8,
            "BENENAME": pl.Utf8, "BENEACCTNO": pl.Utf8,
            "BENEID": pl.Utf8, "BENEIDIND": pl.Utf8,
            "BNAD": pl.Utf8, "PAYDESC": pl.Utf8,
            "STATUS": pl.Utf8, "RSONDESC": pl.Utf8,
            "MOBIPHON": pl.Utf8, "EMAILADD": pl.Utf8,
        }
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(rows)


def write_output_file(df: pl.DataFrame, filepath: Path) -> None:
    """Write a DataFrame using the format implied by the file extension."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    suffix = filepath.suffix.lower()

    if suffix == ".parquet":
        df.write_parquet(filepath)
    elif suffix == ".csv":
        df.write_csv(filepath)
    elif suffix == ".txt":
        # Text output is for human comparison, so write decoded character data
        # instead of binary/parquet bytes.  A pipe delimiter is safer than comma
        # for names, addresses, and descriptions that may contain punctuation.
        df.write_csv(filepath, separator="|")
    else:
        raise ValueError(f"Unsupported output extension for {filepath}. Use .parquet, .csv, or .txt")


# =============================================================================
# READ DAILY ECP FLAT FILE
# =============================================================================
print(f"[EIBMECPT] Reading flat file: {INPUT_DIR}")
ecp_daily: pl.DataFrame = parse_ecp_flat_file(INPUT_DIR, RDATE)
print(f"[EIBMECPT] Records read from flat file: {len(ecp_daily)}")

# =============================================================================
# %MACRO APPENDWKLY — Append / initialise weekly ECP Parquet store
# =============================================================================
# Equivalent logic:
#   IF EXIST(ECPOUT.ECP&REPTMON&NOWK) THEN
#     DELETE rows where REPTDATE = &REPTDT, then APPEND
#   ELSE
#     Create new dataset from today's data

if WEEKLY_FILE.exists():
    # Load existing weekly store
    weekly_existing: pl.DataFrame = pl.read_parquet(WEEKLY_FILE)

    # Remove rows for the current reptdate (idempotent re-run safety)
    # SAS: IF REPTDATE EQ "&REPTDT" THEN DELETE;
    weekly_existing = weekly_existing.filter(pl.col("REPTDATE") != RDATE)

    # Append today's records
    weekly_combined: pl.DataFrame = pl.concat(
        [weekly_existing, ecp_daily], how="diagonal_relaxed"
    )
else:
    # First run for this week — create from today's data
    weekly_combined = ecp_daily.clone()

# Persist updated weekly store
weekly_combined.write_parquet(WEEKLY_FILE)
print(f"[EIBMECPT] Weekly store updated: {WEEKLY_FILE}  ({len(weekly_combined)} rows)")

# =============================================================================
# CIS SUBSET  — DATA ECP&REPTMON&NOWK&REPTYEAR (KEEP=... CIS cols)
#               equivalent of PROC CPORT SELECT ECP&REPTMON&NOWK&REPTYEAR
# =============================================================================
ecp_cis: pl.DataFrame = weekly_combined.select(
    [c for c in CIS_COLS if c in weekly_combined.columns]
)

# =============================================================================
# TRN SUBSET  — DATA ECPTRAN&REPTMON&NOWK&REPTYEAR (KEEP=... TRN cols)
#               equivalent of PROC CPORT SELECT ECPTRAN&REPTMON&NOWK&REPTYEAR
# =============================================================================
ecp_trn: pl.DataFrame = weekly_combined.select(
    [c for c in TRN_COLS if c in weekly_combined.columns]
)

# =============================================================================
# WRITE OUTPUT PARQUET FILES
# (replaces PROC CPORT LIBRARY=WORK FILE=TRANFILE / ECISFTP transport writes)
# =============================================================================
write_output_file(ecp_trn, ETRNFTP_FILE)
print(f"[EIBMECPT] TRN file written : {ETRNFTP_FILE}  ({len(ecp_trn)} rows)")

write_output_file(ecp_cis, ECISFTP_FILE)
print(f"[EIBMECPT] CIS file written : {ECISFTP_FILE}  ({len(ecp_cis)} rows)")

# =============================================================================
# FTP / SFTP STEPS (out of scope for Python conversion)
# ---------------------------------------------------------------------------
# The original JCL RUNSFTP step PUT the following datasets to EDW via SFTP:
#   PUT //SAP.PBB.ECPTRN.ETRNFTP  → /stgsrcsys/host/ftpfiles/ETRNFTP
#   PUT //SAP.PBB.ECPCIS.ECISFTP  → /stgsrcsys/host/ftpfiles/ECISFTP
#   PUT //SAP.DAY.CONTROL         → /stgsrcsys/host/control/EIBMECPT.TXT
# These transfers must be handled by the operations scheduler / SFTP utility
# using ETRNFTP_FILE and ECISFTP_FILE produced above as source files.
# =============================================================================

print("[EIBMECPT] Program completed successfully.")

# To show data - For testing purposes only
print("\n ========== PREVIEW ========== \n")
print(ecp_trn)
print(ecp_cis)
