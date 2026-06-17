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
from typing import Optional

import polars as pl
import duckdb

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# sini
# Input flat file (mainframe fixed-width, RECFM=FB LRECL=1150)
INPUT_DIR = BASE_DIR / "input/uat" / "DP_PBECP_20210508"

# Output files (equivalent of SAP.PBB.ECPTRN.ETRNFTP / ECISFTP)
OUTPUT_DIR   = BASE_DIR / "output" / "EIBMECPT"
ETRNFTP_FILE = OUTPUT_DIR / "ETRNFTP.csv"
ECISFTP_FILE = OUTPUT_DIR / "ECISFTP.csv"

# Weekly ECP Parquet store (equivalent of ECPOUT library)
ECPOUT_DIR = OUTPUT_DIR / "ECPOUT"

# ---------------------------------------------------------------------------
# FILE ENCODING NOTE
# ---------------------------------------------------------------------------
# The input file is pure EBCDIC IBM Code Page 037 (cp037).  WinSCP displays
# it as "Encoding: 1252 (ANSI)" because it renders the raw EBCDIC byte values
# through the Windows-1252 display font -- this is a display artefact only
# and does NOT mean the file was converted to latin-1.
#
# Record structure: RECFM=FB, LRECL=1150.
# Records are separated by EBCDIC newline byte 0x25 (one byte per record),
# which is the EBCDIC equivalent of a line-feed.  When read as raw binary
# the separator byte value is 0x25, NOT the ASCII 0x0A.
#
# The AMOUNT field (PD9.2, packed-decimal BCD, 9 bytes at offset @110) is
# pure binary and must be decoded with the packed-decimal algorithm, not
# as an EBCDIC character string.
# ---------------------------------------------------------------------------
LRECL_CONTENT  = 1150       # bytes of actual record content (no separator)
MAX_TRAILING_BYTES = 8      # scan up to this many non-content bytes per second
STR_ENCODING   = "cp037"     # all string/numeric fields: EBCDIC IBM Code Page 037

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

# Weekly dataset name  (e.g. ECP0312 for March week 1)
WEEKLY_NAME = f"ECP{REPTMON}{NOWK}"
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
    This field is raw binary and is not subject to EBCDIC character encoding.
    """
    if not raw:
        return 0.0

    sign_nibble = 0xF   # default: unsigned positive
    digits = ""
    for i, byte in enumerate(raw):
        high = (byte >> 4) & 0x0F
        low  = byte & 0x0F
        if i < len(raw) - 1:
            digits += str(high) + str(low)
        else:
            # Last byte: high nibble = last digit, low nibble = sign
            digits += str(high)
            sign_nibble = low

    value = int(digits) / (10 ** decimal_places) if digits else 0.0
    if sign_nibble == 0xD:      # negative
        value = -value
    return value

# =============================================================================
# RECORD-LENGTH AUTO-DETECTION
# =============================================================================
def _decode_ebcdic_int(raw: bytes) -> Optional[int]:
    """Decode an EBCDIC numeric field, returning None when it is not numeric."""
    decoded = raw.decode(STR_ENCODING, errors="replace").strip()
    return int(decoded) if decoded.isdigit() else None


def _is_valid_packed_decimal(raw: bytes) -> bool:
    """Return True when raw bytes look like a valid packed-decimal field."""
    if not raw:
        return False

    for byte in raw[:-1]:
        if (byte >> 4) > 9 or (byte & 0x0F) > 9:
            return False

    last_byte = raw[-1]
    return (last_byte >> 4) <= 9 and (last_byte & 0x0F) in (0x0C, 0x0D, 0x0F)


def _score_lrecl_candidate(
    filepath: Path,
    content_len: int,
    lrecl: int,
    sample_size: int = 25,
) -> tuple[int, int]:
    """
    Score a candidate physical record length using fields that must be valid
    when record boundaries are correct.  Returns (score, sampled_records).
    """
    file_size = filepath.stat().st_size
    records_to_sample = min(sample_size, file_size // lrecl)
    if records_to_sample == 0:
        return (0, 0)
    
    score = 0
    with open(filepath, "rb") as fh:
        for _ in range(records_to_sample):
            raw = fh.read(lrecl)[:content_len]
            if len(raw) < content_len:
                break

            acctno = _decode_ebcdic_int(raw[0:11])
            tranyy = _decode_ebcdic_int(raw[91:95])
            tranmm = _decode_ebcdic_int(raw[96:98])
            trandd = _decode_ebcdic_int(raw[99:101])

            if acctno and acctno > 0:
                score += 2
            if tranyy and 1950 <= tranyy <= 2099:
                score += 1
            if tranmm and 1 <= tranmm <= 12:
                score += 1
            if trandd and 1 <= trandd <= 31:
                score += 1
            if tranyy and tranmm and trandd:
                try:
                    date(tranyy, tranmm, trandd)
                    score += 2
                except ValueError:
                    pass
            if _is_valid_packed_decimal(raw[109:118]):
                score += 2

    return (score, records_to_sample)


def _format_candidate_list(candidates: list[int]) -> str:
    """Format candidate LRECLs for messages."""
    return ", ".join(str(candidate) for candidate in candidates)


def _detect_lrecl(filepath: Path, content_len: int) -> int:
    """
    Determine the physical bytes to read per record.

    The ECP content layout is fixed at 1150 bytes, but file transfers may add
    per-record separator/control bytes.  The daily file size changes with the
    number of records, so detection must use divisibility by the physical record
    stride, not yesterday's total file size.

    We scan content_len through content_len + MAX_TRAILING_BYTES.  This covers:
      - 1150     => pure fixed-block content
      - 1151     => one-byte separator such as EBCDIC NL (0x25) or LF (0x0A)
      - 1152     => two-byte separator such as CRLF
      - 1153+    => transferred files with extra per-record control bytes

    If no candidate divides the file exactly, stop instead of producing shifted
    rows.  A shifted fixed-width parse is worse than no output because columns
    such as ACCTNO, SERIAL, and TRANDATE silently become corrupted.
    """
    file_size = filepath.stat().st_size
    candidates = list(range(content_len, content_len + MAX_TRAILING_BYTES + 1))

    if file_size == 0:
        print(f"[EIBMECPT] Empty input file; using content LRECL={content_len}")
        return content_len

    exact_candidates = [
        candidate for candidate in candidates if file_size % candidate == 0
    ]
    if not exact_candidates:
        raise ValueError(
            f"[EIBMECPT] Cannot determine fixed record length: file size {file_size} "
            f"is not divisible by candidates ({_format_candidate_list(candidates)}). "
            "Do not continue with LRECL=1150 because output would be misaligned. "
            "Verify the source file transfer mode, trailing per-record bytes, or LRECL."
        )

    scored_candidates = []
    for candidate in exact_candidates:
        score, sampled = _score_lrecl_candidate(filepath, content_len, candidate)
        scored_candidates.append((score, sampled, candidate))

    score, sampled, detected_lrecl = max(scored_candidates)
    record_count = file_size // detected_lrecl

    with open(filepath, "rb") as fh:
        fh.seek(content_len)
        trailing_sample = fh.read(max(0, detected_lrecl - content_len))

    if detected_lrecl == content_len:
        separator_desc = "none (pure 1150-byte content)"
    elif trailing_sample in (b"\x25", b"\x0A"):
        separator_desc = f"single byte separator 0x{trailing_sample[0]:02X}"
    elif trailing_sample == b"\x0D\x0A":
        separator_desc = "CRLF separator 0x0D0A"
    else:
        separator_desc = (
            f"{detected_lrecl - content_len} trailing byte(s): "
            f"{trailing_sample.hex().upper()}"
        )

    print(
        f"[EIBMECPT] Detected effective LRECL={detected_lrecl} "
        f"(content={content_len}, {separator_desc}, records={record_count}, "
        f"validation_score={score}/{sampled * 9 if sampled else 0})"
    )
    return detected_lrecl

# =============================================================================
# EBCDIC CONTROL-CHARACTER SANITISER
# =============================================================================
# In cp037 several byte values decode to control characters that must not
# appear in the pipe-delimited output file:
#   0x25 => \n  (EBCDIC newline -- the record separator itself)
#   0x0D => \r  (EBCDIC carriage return)
#   0x15 => \n  (EBCDIC NL variant)
#   0x25 => \n  (EBCDIC LF)
# Stripping all C0 Unicode control characters (U+0000-U+001F) and DEL
# (U+007F) from decoded strings prevents write_csv() from splitting a
# single logical row across multiple physical lines in the output file.
_CTRL_TABLE = dict.fromkeys(range(0x00, 0x20), None)
_CTRL_TABLE[0x7F] = None

def _sanitise(value: str) -> str:
    """Strip C0 control characters and DEL from a decoded EBCDIC string."""
    return value.translate(_CTRL_TABLE)

# =============================================================================
# FIXED-WIDTH FLAT FILE READER  (equivalent of DATA ECP / INFILE DPECPT step)
# Column positions are 1-based in SAS => converted to 0-based slicing in Python.
#
# SAS INPUT statement field map:
#   @0001  ACCTNO        11.       numeric  11 bytes
#   @0012  SERIAL        $16.      char     16 bytes
#   @0092  TRANYY        4.        numeric   4 bytes (year)
#   @0097  TRANMM        2.        numeric   2 bytes (month)
#   @0100  TRANDD        2.        numeric   2 bytes (day)
#   @0110  AMOUNT        PD9.2     packed    9 bytes  <- raw binary BCD
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
# =============================================================================

FIELD_SPECS = [
    # (field_name,    start_1based, length, type)   type: 'num' | 'str' | 'pd'
    ("ACCTNO",        1,    11, "num"),
    ("SERIAL",       12,    16, "str"),
    ("TRANYY",       92,     4, "num"),
    ("TRANMM",       97,     2, "num"),
    ("TRANDD",      100,     2, "num"),
    ("AMOUNT",      110,     9, "pd"),   # packed-decimal BCD -- raw binary
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

def parse_ecp_flat_file(filepath: Path, reptdate_val: date) -> pl.DataFrame:
    """
    Read the ECP mainframe flat file and return a Polars DataFrame.
    Equivalent of the DATA ECP / INFILE DPECPT INPUT ... step.

    Opens the file in binary mode and reads fixed-length chunks of
    effective_lrecl bytes.  String and numeric fields are decoded from
    EBCDIC cp037.  The AMOUNT packed-decimal field is decoded from raw
    binary bytes.  Control characters are stripped from all string fields
    before they enter the DataFrame.
    """
    effective_lrecl = _detect_lrecl(filepath, LRECL_CONTENT)

    rows = []
    with open(filepath, "rb") as fh:
        while True:
            raw = fh.read(effective_lrecl)
            if not raw:
                break
            if len(raw) < LRECL_CONTENT:
                # Partial final block -- pad with EBCDIC spaces (0x40)
                raw = raw + b"\x40" * (LRECL_CONTENT - len(raw))

            # Only the content portion is parsed; the trailing separator
            # byte(s) beyond LRECL_CONTENT are silently discarded.
            record_bytes = raw[:LRECL_CONTENT]

            record: dict = {}
            for (name, start1, length, ftype) in FIELD_SPECS:
                s = start1 - 1
                e = s + length
                chunk = record_bytes[s:e]

                if ftype == "str":
                    raw_str = chunk.decode(STR_ENCODING, errors="replace").rstrip()
                    record[name] = _sanitise(raw_str)
                elif ftype == "num":
                    try:
                        decoded = chunk.decode(STR_ENCODING, errors="replace").strip()
                        record[name] = int(decoded) if decoded else 0
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

# =============================================================================
# READ DAILY ECP FLAT FILE
# =============================================================================
print(f"[EIBMECPT] Reading flat file: {INPUT_DIR}")
ecp_daily: pl.DataFrame = parse_ecp_flat_file(INPUT_DIR, RDATE)
print(f"[EIBMECPT] Records read from flat file: {len(ecp_daily)}")

# =============================================================================
# %MACRO APPENDWKLY -- Append / initialise weekly ECP Parquet store
# =============================================================================
# Equivalent logic:
#   IF EXIST(ECPOUT.ECP&REPTMON&NOWK) THEN
#     DELETE rows where REPTDATE = &REPTDT, then APPEND
#   ELSE
#     Create new dataset from today's data

if WEEKLY_FILE.exists():
    weekly_existing: pl.DataFrame = pl.read_parquet(WEEKLY_FILE)

    # Remove rows for the current reptdate (idempotent re-run safety)
    # SAS: IF REPTDATE EQ "&REPTDT" THEN DELETE;
    # weekly_existing = weekly_existing.filter(pl.col("REPTDATE") != RDATE)

    weekly_combined: pl.DataFrame = pl.concat(
        [weekly_existing, ecp_daily], how="diagonal_relaxed"
    )
else:
    weekly_combined = ecp_daily.clone()

weekly_combined.write_parquet(WEEKLY_FILE)
print(f"[EIBMECPT] Weekly store updated: {WEEKLY_FILE}  ({len(weekly_combined)} rows)")

# =============================================================================
# CIS SUBSET  -- DATA ECP&REPTMON&NOWK&REPTYEAR (KEEP=... CIS cols)
#                equivalent of PROC CPORT SELECT ECP&REPTMON&NOWK&REPTYEAR
# =============================================================================
ecp_cis: pl.DataFrame = weekly_combined.select(
    [c for c in CIS_COLS if c in weekly_combined.columns]
)

# =============================================================================
# TRN SUBSET  -- DATA ECPTRAN&REPTMON&NOWK&REPTYEAR (KEEP=... TRN cols)
#                equivalent of PROC CPORT SELECT ECPTRAN&REPTMON&NOWK&REPTYEAR
# =============================================================================
ecp_trn: pl.DataFrame = weekly_combined.select(
    [c for c in TRN_COLS if c in weekly_combined.columns]
)

# =============================================================================
# WRITE OUTPUT TEXT FILES
# (replaces PROC CPORT LIBRARY=WORK FILE=TRANFILE / ECISFTP transport writes)
#
# Pipe-delimited UTF-8 text files. Date columns cast to ISO YYYY-MM-DD string.
# Control characters were stripped during parsing so write_csv() will never
# split a logical row across multiple physical lines in the output file.
# =============================================================================
def write_txt(df: pl.DataFrame, filepath: Path) -> None:
    """Write a Polars DataFrame to a pipe-delimited UTF-8 text file."""
    cast_exprs = [
        pl.col(c).cast(pl.Utf8) if df[c].dtype == pl.Date else pl.col(c)
        for c in df.columns
    ]
    df.select(cast_exprs).write_csv(filepath, separator="|")


write_txt(ecp_trn, ETRNFTP_FILE)
print(f"[EIBMECPT] TRN file written : {ETRNFTP_FILE}  ({len(ecp_trn)} rows)")

write_txt(ecp_cis, ECISFTP_FILE)
print(f"[EIBMECPT] CIS file written : {ECISFTP_FILE}  ({len(ecp_cis)} rows)")

# =============================================================================
# FTP / SFTP STEPS (out of scope for Python conversion)
# ---------------------------------------------------------------------------
# The original JCL RUNSFTP step PUT the following datasets to EDW via SFTP:
#   PUT //SAP.PBB.ECPTRN.ETRNFTP  -> /stgsrcsys/host/ftpfiles/ETRNFTP
#   PUT //SAP.PBB.ECPCIS.ECISFTP  -> /stgsrcsys/host/ftpfiles/ECISFTP
#   PUT //SAP.DAY.CONTROL         -> /stgsrcsys/host/control/EIBMECPT.TXT
# These transfers must be handled by the operations scheduler / SFTP utility
# using ETRNFTP_FILE and ECISFTP_FILE produced above as source files.
# =============================================================================

print("[EIBMECPT] Program completed successfully.")

# To show data - For testing purposes only
print("\n ========== PREVIEW ========== \n")
print(ecp_trn)
print(ecp_cis)
