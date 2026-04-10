#!/usr/bin/env python3
"""
Program  : EIQBILEX.py
Purpose  : Billing file validation and extraction.
           Reads a packed-decimal binary billing flat file (BILFILE),
           validates the file header (HT) date against yesterday's date
           and the file footer (FT) record count against the actual data
           row count, then extracts billing records into a Parquet dataset
           (STG_LN_BILL) for downstream EDW consumption.

SAS Original : EIQBILEX (JCL job MISEIS,EIFWLNEX)
Migrated to  : Python / Polars / Parquet

Inputs  : BILFILE  - RBP2.B033.BILLFILE.MIS(0)
                     Packed-decimal fixed-width binary flat file.
                     LRECL=27648, RECFM=FS (fixed, spanned/unblocked).
                     Each logical record is one "row" of the billing file.

Outputs : STG_LN_BILL  - <OUTPUT_DIR>/STG_LN_BILL.parquet
                         Extracted billing records (SAS dataset equivalent).

Notes   :
  - PROC CPORT / FTP steps (SAP.PBB.BLDATAWH.BILFTP -> EDW SFTP) are
    mainframe-infrastructure operations and are not replicated here.
    The downstream consumer should pick up STG_LN_BILL.parquet directly.
  - Packed-decimal (PD) fields are decoded from raw binary bytes.
  - SAS YEARCUTOFF=1950 applies to DDMMYY8. date parsing in the header.
  - SAS date arithmetic: days since 1960-01-01.
"""

import sys
import struct
import logging
from datetime import date, timedelta
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
INPUT_DIR  = Path("input")
OUTPUT_DIR = Path("output")

BILFILE_PATH    = INPUT_DIR  / "BILLFILE.MIS"          # RBP2.B033.BILLFILE.MIS(0)
STG_LN_BILL_OUT = OUTPUT_DIR / "STG_LN_BILL.parquet"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Logical record length of the billing flat file (LRECL=27648 per JCL DCB).
# Each record is exactly LRECL bytes.
LRECL = 27648

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PACKED-DECIMAL HELPERS
# ---------------------------------------------------------------------------

def decode_pd(raw: bytes, implied_decimals: int = 0) -> float | None:
    """Decode IBM mainframe packed-decimal bytes into a Python numeric value.

    Packed-decimal format: each byte holds two BCD digits; the last nibble
    is the sign (C/F = positive, D = negative).  A missing/invalid field
    is represented by bytes of 0x00 in the first byte and returned as None
    (matching SAS missing-value semantics for PD informats).

    Parameters
    ----------
    raw              : bytes  – raw byte string of length n (e.g., 6 for PD6.)
    implied_decimals : int    – number of implied decimal places (e.g., 2 for PD8.2)

    Returns
    -------
    float | None  – decoded numeric value, or None if the field is missing.
    """
    if not raw or all(b == 0 for b in raw):
        return None

    digits = []
    for i, byte in enumerate(raw):
        high = (byte >> 4) & 0x0F
        low  =  byte       & 0x0F
        if i == len(raw) - 1:
            # Last byte: high nibble is last digit, low nibble is sign
            digits.append(high)
            sign_nibble = low
        else:
            digits.append(high)
            digits.append(low)

    value = int("".join(str(d) for d in digits))

    # sign_nibble: 0xC or 0xF = positive, 0xD = negative
    if sign_nibble == 0xD:
        value = -value

    if implied_decimals:
        return value / (10 ** implied_decimals)
    return float(value)


# ---------------------------------------------------------------------------
# DATE HELPERS  (SAS YEARCUTOFF=1950, DDMMYY8. format)
# ---------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)

def parse_ddmmyy8(text: str) -> date | None:
    """Parse a DDMMYY8. formatted date string (e.g. '01012024' or '010124').

    SAS YEARCUTOFF=1950: two-digit years 50-99 map to 1950-1999;
    00-49 map to 2000-2049.
    """
    text = text.strip()
    if len(text) < 6:
        return None
    try:
        dd = int(text[0:2])
        mm = int(text[2:4])
        yy_raw = text[4:]                      # may be 2 or 4 digits
        if len(yy_raw) == 2:
            yy = int(yy_raw)
            year = (1900 + yy) if yy >= 50 else (2000 + yy)
        else:
            year = int(yy_raw)
        return date(year, mm, dd)
    except (ValueError, IndexError):
        return None


def format_ddmmyy10(d: date) -> str:
    """Format a date as DD/MM/YYYY (SAS DDMMYY10. format)."""
    return d.strftime("%d/%m/%Y")


def format_z15(n: int | None) -> str:
    """Format an integer with leading zeros, width 15 (SAS Z15. format)."""
    if n is None:
        return " " * 15
    return str(n).zfill(15)


# ---------------------------------------------------------------------------
# READ ALL LOGICAL RECORDS
# ---------------------------------------------------------------------------

def read_records(path: Path, lrecl: int) -> list[bytes]:
    """Read a fixed-length binary flat file and return a list of byte records."""
    records = []
    file_size = path.stat().st_size
    if file_size % lrecl != 0:
        log.warning(
            "File size %d is not a multiple of LRECL=%d; trailing bytes will be ignored.",
            file_size, lrecl,
        )
    with open(path, "rb") as fh:
        while True:
            rec = fh.read(lrecl)
            if not rec:
                break
            if len(rec) < lrecl:
                # Pad short final record to LRECL (should not happen with RECFM=FS)
                rec = rec.ljust(lrecl, b"\x00")
            records.append(rec)
    return records


# ---------------------------------------------------------------------------
# STEP 1 – VALIDATION  (DATA _NULL_ equivalent)
# ---------------------------------------------------------------------------

def validate_bilfile(records: list[bytes]) -> None:
    """Validate the billing flat file header date and footer record count.

    Replicates the SAS DATA _NULL_ step logic:
      - First record (@1 FHFT $2.) must be 'HT'; reads @3 LNBIL_DTE DDMMYY8.
      - Last  record (@1 FHFT $2.) must be 'FT'; reads @3 LNBIL_OBS 15.
      - LNBIL_DTE must equal TODAY()-1.
      - LNBIL_OBS must equal _N_ - 2  (total records minus header and footer).
      - If any check fails, logs the SAS-equivalent error banner and aborts.

    Parameters
    ----------
    records : list[bytes]  – all logical records from BILFILE (including HT/FT).
    """
    n_records = len(records)

    # --- Extract FHFT from first and last records ---
    first_rec = records[0]  if n_records >= 1 else b""
    last_rec  = records[-1] if n_records >= 1 else b""

    first_fhft = first_rec[0:2].decode("cp1047", errors="replace").strip()  # EBCDIC
    last_fhft  = last_rec[0:2].decode("cp1047", errors="replace").strip()

    # --- Parse header date (first record) ---
    lnbil_dte: date | None = None
    if first_fhft == "HT":
        raw_dte = first_rec[2:10].decode("cp1047", errors="replace")
        lnbil_dte = parse_ddmmyy8(raw_dte)

    # --- Parse footer record count (last record) ---
    lnbil_obs: int | None = None
    if last_fhft == "FT":
        raw_obs = last_rec[2:17].decode("cp1047", errors="replace").strip()
        try:
            lnbil_obs = int(raw_obs)
        except ValueError:
            lnbil_obs = None

    # --- Derive expected values ---
    sas_dte = date.today() - timedelta(days=1)    # TODAY()-1
    sas_obs = n_records - 2                        # _N_ - 2 (exclude HT & FT)

    # --- Format for display (SAS FORMAT statements) ---
    lnbil_dte_fmt = format_ddmmyy10(lnbil_dte) if lnbil_dte else "(missing)"
    sas_dte_fmt   = format_ddmmyy10(sas_dte)
    lnbil_obs_fmt = format_z15(lnbil_obs)
    sas_obs_fmt   = format_z15(sas_obs)

    # --- Check conditions ---
    date_mismatch   = (lnbil_dte != sas_dte)
    obs_mismatch    = (lnbil_obs != sas_obs)
    hf_incomplete   = (last_fhft not in ("HT", "FT")) or (n_records == 1)

    error_found = date_mismatch or obs_mismatch or hf_incomplete

    if error_found:
        # Replicate SAS PUTLOG error banner verbatim
        log.error('DSN= "RBP2.B033.BILLFILE.MIS(0)"')
        log.error("+--------+--------+-------+-------+--------+-------+")
        log.error("|  ERROR MESSAGE(S) STATUS                         |")
        log.error("+--------+--------+-------+-------+--------+-------+")

        if date_mismatch:
            log.error("|  File Header   : ERROR!!                         |")
            log.error(
                "|      LNBIL_DTE=%s <> SAS_DTE=%s  |",
                lnbil_dte_fmt, sas_dte_fmt,
            )

        if obs_mismatch:
            log.error("|  File Footer   : ERROR!!                         |")
            log.error(
                "|      LNBIL_OBS=%s <> SAS_OBS=%s  |",
                lnbil_obs_fmt, sas_obs_fmt,
            )

        if hf_incomplete:
            log.error("|  Header/Footer : INCOMPLETE!!                    |")
            log.error("|      NO FOOTER. PLEASE CONTACT LN & SAS-TEAM     |")

        log.error("|                                                  |")
        log.error("+========+========+=======+=======+========+=======+")

        # Equivalent of SAS ABORT ABEND 77
        sys.exit(77)

    log.info("Validation passed: LNBIL_DTE=%s, LNBIL_OBS=%s", lnbil_dte_fmt, lnbil_obs_fmt)


# ---------------------------------------------------------------------------
# STEP 2 – DATA EXTRACTION  (DATA BILL.STG_LN_BILL equivalent)
# ---------------------------------------------------------------------------

def extract_stg_ln_bill(records: list[bytes]) -> pl.DataFrame:
    """Parse billing data records and return a Polars DataFrame.

    Replicates the SAS DATA BILL.STG_LN_BILL step:
      - FIRSTOBS=2 : skip the first record (HT header).
      - Last record (FT footer) also skipped via ACCTCHK IS NULL filter.
      - All fields are packed-decimal (PD informat), byte-offset as per SAS.
      - ACCTCHK NE . : only retain records where ACCTCHK is not missing.

    PD field layout (1-based SAS column positions → 0-based Python byte offsets):
      @001 ACCTCHK    PD6.    →  bytes  0– 5  (6 bytes, no decimals)
      @001 ACCTNO     PD6.    →  bytes  0– 5  (6 bytes, no decimals)
      @007 NOTENO     PD3.    →  bytes  6– 8  (3 bytes, no decimals)
      @010 BLDATE     PD6.    →  bytes  9–14  (6 bytes, no decimals)
      @016 BLPDDATE   PD6.    →  bytes 15–20  (6 bytes, no decimals)
      @022 DAYSLATE   PD2.    →  bytes 21–22  (2 bytes, no decimals)
      @024 PRODUCT    PD2.    →  bytes 23–24  (2 bytes, no decimals)
      @026 COSTCTR    PD4.    →  bytes 25–28  (4 bytes, no decimals)
      @030 BILL_AMT   PD8.2   →  bytes 29–36  (8 bytes, 2 implied decimals)
      @038 BILL_AMT_PRIN      PD8.2 → bytes 37–44
      @046 BILL_AMT_INT       PD8.2 → bytes 45–52
      @054 BILL_AMT_ESCROW    PD8.2 → bytes 53–60
      @062 BILL_AMT_FEE       PD8.2 → bytes 61–68
      @070 BILL_NOT_PAY_AMT   PD8.2 → bytes 69–76
      @078 BILL_NOT_PAY_AMT_PRIN    PD8.2 → bytes 77–84
      @086 BILL_NOT_PAY_AMT_INT     PD8.2 → bytes 85–92
      @094 BILL_NOT_PAY_AMT_ESCROW  PD8.2 → bytes 93–100
      @102 BILL_NOT_PAY_AMT_FEE     PD8.2 → bytes 101–108

    Parameters
    ----------
    records : list[bytes]  – all logical records from BILFILE (including HT/FT).

    Returns
    -------
    pl.DataFrame  – parsed billing records, mirroring BILL.STG_LN_BILL columns.
    """

    # FIRSTOBS=2 means skip records[0] (the HT header record); the FT footer
    # will be filtered out below via ACCTCHK IS NULL (its packed-decimal bytes
    # decode to None because the footer's first 6 bytes are not PD-encoded).
    data_records = records[1:]

    rows = []
    for rec in data_records:
        # --- Primary filter: ACCTCHK (@001 PD6.) ---
        # SAS: INPUT @001 ACCTCHK PD6. @;  IF ACCTCHK NE .;
        acctchk = decode_pd(rec[0:6], implied_decimals=0)
        if acctchk is None:
            continue    # Skip header/footer/padding rows (IF ACCTCHK NE .)

        # --- Read all remaining PD fields ---
        acctno               = decode_pd(rec[0:6],     0)   # @001 PD6.
        noteno               = decode_pd(rec[6:9],     0)   # @007 PD3.
        bldate               = decode_pd(rec[9:15],    0)   # @010 PD6.
        blpddate             = decode_pd(rec[15:21],   0)   # @016 PD6.
        dayslate             = decode_pd(rec[21:23],   0)   # @022 PD2.
        product              = decode_pd(rec[23:25],   0)   # @024 PD2.
        costctr              = decode_pd(rec[25:29],   0)   # @026 PD4.
        bill_amt             = decode_pd(rec[29:37],   2)   # @030 PD8.2
        bill_amt_prin        = decode_pd(rec[37:45],   2)   # @038 PD8.2
        bill_amt_int         = decode_pd(rec[45:53],   2)   # @046 PD8.2
        bill_amt_escrow      = decode_pd(rec[53:61],   2)   # @054 PD8.2
        bill_amt_fee         = decode_pd(rec[61:69],   2)   # @062 PD8.2
        bill_not_pay_amt     = decode_pd(rec[69:77],   2)   # @070 PD8.2
        bill_not_pay_amt_prin   = decode_pd(rec[77:85],  2) # @078 PD8.2
        bill_not_pay_amt_int    = decode_pd(rec[85:93],  2) # @086 PD8.2
        bill_not_pay_amt_escrow = decode_pd(rec[93:101], 2) # @094 PD8.2
        bill_not_pay_amt_fee    = decode_pd(rec[101:109],2) # @102 PD8.2

        rows.append({
            "ACCTNO"                  : acctno,
            "NOTENO"                  : noteno,
            "BLDATE"                  : bldate,
            "BLPDDATE"                : blpddate,
            "DAYSLATE"                : dayslate,
            "PRODUCT"                 : product,
            "COSTCTR"                 : costctr,
            "BILL_AMT"                : bill_amt,
            "BILL_AMT_PRIN"           : bill_amt_prin,
            "BILL_AMT_INT"            : bill_amt_int,
            "BILL_AMT_ESCROW"         : bill_amt_escrow,
            "BILL_AMT_FEE"            : bill_amt_fee,
            "BILL_NOT_PAY_AMT"        : bill_not_pay_amt,
            "BILL_NOT_PAY_AMT_PRIN"   : bill_not_pay_amt_prin,
            "BILL_NOT_PAY_AMT_INT"    : bill_not_pay_amt_int,
            "BILL_NOT_PAY_AMT_ESCROW" : bill_not_pay_amt_escrow,
            "BILL_NOT_PAY_AMT_FEE"    : bill_not_pay_amt_fee,
        })

    schema = {
        "ACCTNO"                  : pl.Float64,
        "NOTENO"                  : pl.Float64,
        "BLDATE"                  : pl.Float64,
        "BLPDDATE"                : pl.Float64,
        "DAYSLATE"                : pl.Float64,
        "PRODUCT"                 : pl.Float64,
        "COSTCTR"                 : pl.Float64,
        "BILL_AMT"                : pl.Float64,
        "BILL_AMT_PRIN"           : pl.Float64,
        "BILL_AMT_INT"            : pl.Float64,
        "BILL_AMT_ESCROW"         : pl.Float64,
        "BILL_AMT_FEE"            : pl.Float64,
        "BILL_NOT_PAY_AMT"        : pl.Float64,
        "BILL_NOT_PAY_AMT_PRIN"   : pl.Float64,
        "BILL_NOT_PAY_AMT_INT"    : pl.Float64,
        "BILL_NOT_PAY_AMT_ESCROW" : pl.Float64,
        "BILL_NOT_PAY_AMT_FEE"    : pl.Float64,
    }

    if not rows:
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(rows, schema=schema)


# ---------------------------------------------------------------------------
# PROC CPORT / FTP STUB
# ---------------------------------------------------------------------------
# SAS:
#   FILENAME TRANFILE 'SAP.PBB.BLDATAWH.BILFTP' DISP=OLD;
#   PROC CPORT LIBRARY=BILL FILE=TRANFILE;
#   RUN;
#
# The PROC CPORT step serialises the BILL library to a transport file
# (SAP.PBB.BLDATAWH.BILFTP) for subsequent FTP transfer to the EDW
# landing zone (/stgsrcsys/host/ftpfiles/BILFTP).
#
# This is a mainframe-only infrastructure operation.  In the Python
# pipeline the output Parquet file (STG_LN_BILL.parquet) serves as the
# equivalent serialised dataset and should be transferred to the EDW via
# the appropriate modern file-transfer mechanism (SFTP / Azure Blob / etc.).
#
# JCL FTP step (RUNSFTP / COZBATCH) transferring:
#   //SAP.PBB.BLDATAWH.BILFTP  -> /stgsrcsys/host/ftpfiles/BILFTP
#   //SAP.DAY.CONTROL           -> /stgsrcsys/host/control/FTPBILWH.TXT
# is likewise not replicated here; EDW landing is handled externally.


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("EIQBILEX started.")

    # --- Read all records ---
    log.info("Reading BILFILE: %s", BILFILE_PATH)
    records = read_records(BILFILE_PATH, LRECL)
    log.info("Total records read (including HT/FT): %d", len(records))

    # --- Step 1: Validate file header and footer ---
    validate_bilfile(records)

    # --- Step 2: Extract billing records ---
    log.info("Extracting STG_LN_BILL records ...")
    df = extract_stg_ln_bill(records)
    log.info("Records extracted: %d", len(df))

    # --- Write output Parquet ---
    df.write_parquet(STG_LN_BILL_OUT)
    log.info("Output written: %s", STG_LN_BILL_OUT)

    log.info("EIQBILEX completed successfully.")


if __name__ == "__main__":
    main()
