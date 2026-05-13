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
INPUT_DIR = BASE_DIR / "input/uat" / "DP_PBECP_20260511"

# Output Parquet files (equivalent of SAP.PBB.ECPTRN.ETRNFTP / ECISFTP)
OUTPUT_DIR      = BASE_DIR / "output" / "EIBMECPT"
ETRNFTP_FILE    = OUTPUT_DIR / "ETRNFTP.txt"
ECISFTP_FILE    = OUTPUT_DIR / "ECISFTP.txt"

# Weekly ECP Parquet store (equivalent of ECPOUT library)
ECPOUT_DIR      = OUTPUT_DIR / "ECPOUT"

# Mainframe logical record length (RECFM=FB LRECL=1150).
# The rightmost defined field ends at byte 1150 (1-based: @1101 + 50 chars).
# When mainframe datasets are transferred via FTP in text/ASCII mode the
# transfer process appends a line-feed (0x0A, 1 byte) or CRLF (0x0D 0x0A,
# 2 bytes) after every record.  _detect_lrecl() inspects the raw file to
# determine which variant is present so that fh.read(effective_lrecl)
# always consumes exactly one complete record plus its separator, keeping
# every subsequent read correctly aligned.
LRECL_CONTENT = 1150   # bytes of actual record content (no separator)

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
    sign_nibble is initialised to 0xF before the loop so it is always
    defined even when raw is empty or contains a single byte.
    """
    if not raw:
        return 0.0

    sign_nibble = 0xF   # default: unsigned positive (safe fallback)
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
    if sign_nibble == 0xD:          # negative
        value = -value
    return value

# =============================================================================
# RECORD-LENGTH AUTO-DETECTION
# =============================================================================
def _detect_lrecl(filepath: Path, content_len: int) -> int:
    """
    Inspect the raw binary file to determine the effective record length,
    accounting for any line-feed or CRLF separator appended by FTP text-mode
    transfer.

    Strategy:
      1. Read the first content_len bytes — the expected content of record 1.
      2. Peek at the next 1-2 bytes.
         - If next bytes are 0x0D 0x0A (CRLF) → effective = content_len + 2
         - If next byte is 0x0A (LF)           → effective = content_len + 1
         - Otherwise                            → effective = content_len
      3. Validate by confirming file_size % effective_lrecl == 0.
         If not, try the remaining candidates before falling back.

    Returns the effective record length (content bytes + separator bytes).
    """
    file_size = filepath.stat().st_size

    with open(filepath, "rb") as fh:
        fh.read(content_len)        # consume first record's content bytes
        peek = fh.read(2)           # inspect the bytes that follow

    if len(peek) >= 2 and peek[0] == 0x0D and peek[1] == 0x0A:
        candidate = content_len + 2    # CRLF separator
    elif len(peek) >= 1 and peek[0] == 0x0A:
        candidate = content_len + 1    # LF separator
    else:
        candidate = content_len        # no separator (pure binary FB)

    sep_labels = {
        content_len:     "none (pure binary FB)",
        content_len + 1: "LF (0x0A)",
        content_len + 2: "CRLF (0x0D 0x0A)",
    }

    if file_size % candidate == 0:
        print(f"[EIBMECPT] Detected effective LRECL={candidate} "
              f"(content={content_len}, separator={sep_labels.get(candidate, '?')}, "
              f"records={file_size // candidate})")
        return candidate

    # Fallback: try all three candidates in order
    for alt in [content_len, content_len + 1, content_len + 2]:
        if file_size % alt == 0:
            print(f"[EIBMECPT] Fallback LRECL={alt} "
                  f"(separator={sep_labels.get(alt, '?')}, "
                  f"records={file_size // alt})")
            return alt

    # Last resort: trust the content length and warn
    print(f"[EIBMECPT] WARNING: file size {file_size} is not divisible by any "
          f"candidate LRECL ({content_len}, {content_len+1}, {content_len+2}). "
          f"Proceeding with LRECL={content_len} — output may be misaligned.")
    return content_len

# =============================================================================
# EBCDIC CONTROL-CHARACTER SANITISER
# =============================================================================
# After decoding EBCDIC to Unicode some bytes map to ASCII control characters
# (e.g. EBCDIC 0x25 → U+000A line-feed, 0x0D → U+000D carriage-return in
# cp037).  If these survive into the DataFrame and are then written via
# write_csv() they appear as literal newlines in the output file, splitting
# a single data row across multiple physical lines.  _sanitise() strips all
# C0 control characters (U+0000–U+001F) and DEL (U+007F) from decoded strings.
_CTRL_TABLE = dict.fromkeys(range(0x00, 0x20), None)   # U+0000–U+001F → delete
_CTRL_TABLE[0x7F] = None                                # DEL → delete

def _sanitise(value: str) -> str:
    """Strip ASCII/C0 control characters from a decoded EBCDIC string."""
    return value.translate(_CTRL_TABLE)

# =============================================================================
# FIXED-WIDTH FLAT FILE READER  (equivalent of DATA ECP / INFILE DPECPT step)
# Column positions are 1-based in SAS → converted to 0-based slicing in Python.
# LRECL is at least 1150 bytes based on the widest field @1101 + 50 chars.
#
# The file is read as a binary stream using fixed-length fh.read(effective_lrecl)
# calls.  Reading line-by-line with rstrip() would corrupt byte offsets because
# EBCDIC bytes whose numeric value coincides with ASCII 0x0D or 0x0A can appear
# legitimately inside record content and would be misinterpreted as line endings.
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

def parse_ecp_flat_file(filepath: Path, reptdate_val: date) -> pl.DataFrame:
    """
    Read the ECP mainframe flat file and return a Polars DataFrame.
    Equivalent of the DATA ECP / INFILE DPECPT INPUT ... step.

    The effective record length (including any FTP-added separator) is
    determined once by _detect_lrecl().  Each call to fh.read(effective_lrecl)
    consumes exactly one record; only the first LRECL_CONTENT bytes are sliced
    for field extraction — trailing separator byte(s) are silently discarded.
    """
    effective_lrecl = _detect_lrecl(filepath, LRECL_CONTENT)

    rows = []
    with open(filepath, "rb") as fh:
        while True:
            raw = fh.read(effective_lrecl)
            if not raw:
                break                       # end of file
            if len(raw) < LRECL_CONTENT:
                # Partial final block — pad with EBCDIC spaces (0x40)
                raw = raw + b"\x40" * (LRECL_CONTENT - len(raw))

            # Only the content portion is parsed; separator bytes (if any)
            # sit beyond index LRECL_CONTENT and are never accessed.
            record_bytes = raw[:LRECL_CONTENT]

            record: dict = {}
            for (name, start1, length, ftype) in FIELD_SPECS:
                s = start1 - 1          # 0-based start
                e = s + length
                chunk = record_bytes[s:e]

                if ftype == "str":
                    # Decode EBCDIC (IBM Code Page 037 is standard US mainframe;
                    # adjust to cp1047 if the source system uses a variant).
                    raw_str = chunk.decode("cp037", errors="replace").rstrip()
                    record[name] = _sanitise(raw_str)
                elif ftype == "num":
                    try:
                        decoded = chunk.decode("cp037", errors="replace").strip()
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
# WRITE OUTPUT TEXT FILES
# (replaces PROC CPORT LIBRARY=WORK FILE=TRANFILE / ECISFTP transport writes)
#
# Outputs are pipe-delimited UTF-8 text files.  Date columns are cast to
# string (ISO YYYY-MM-DD) before writing.  The _sanitise() step applied
# during parsing ensures no embedded control characters reach the CSV writer,
# preventing data rows from being split across multiple physical lines.
# =============================================================================
def write_txt(df: pl.DataFrame, filepath: Path) -> None:
    """
    Write a Polars DataFrame to a pipe-delimited UTF-8 text file.
    Date columns are formatted as YYYY-MM-DD (ISO, matching SAS YYMMDD10.).
    """
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
