# !/usr/bin/env python3
"""
Program  : EIBMDPIC.py
Purpose  : Extraction for Inward Clearing Cheques from Deposits (PBB)
           Transferred dataset to SAS Server under folder: DP_CA
           Dataset: ICLRGYYMMDD
ESMR     : 2008-1270
Date     : 09-10-08 (HHH)
Entity   : Public Bank Berhad (PBB)
"""

import os
from datetime import date, datetime
from pathlib import Path

import polars as pl

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_INPUT_PATH  = Path(os.environ.get("BASE_INPUT_PATH",
                        "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/Outsource/input"))
BASE_OUTPUT_PATH = Path(os.environ.get("BASE_OUTPUT_PATH",
                        "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output"))

BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Input paths
REPTDATE_LOAN = BASE_INPUT_PATH / "LOAN.REPTDATE.parquet"   # SAS dataset → Parquet
RPTFILE2      = BASE_INPUT_PATH / "RPTFILE2.txt"            # fixed-width flat file
RPTFILE       = BASE_INPUT_PATH / "RPTFILE.txt"             # fixed-width flat file
BRHCODE       = BASE_INPUT_PATH / "BRHCODE.txt"             # fixed-width flat file

# =============================================================================
# DATE / WEEK DERIVATION  (DATA REPTDATE; SET LOAN.REPTDATE)
# LOAN.REPTDATE is a SAS dataset converted to Parquet.
# =============================================================================
reptdate_df = pl.read_parquet(REPTDATE_LOAN)
reptdate_val = reptdate_df["REPTDATE"][0]

# Handle date or datetime column types from Parquet
if isinstance(reptdate_val, datetime):
    REPTDATE: date = reptdate_val.date()
elif isinstance(reptdate_val, date):
    REPTDATE = reptdate_val
else:
    REPTDATE = datetime.strptime(str(reptdate_val)[:10], "%Y-%m-%d").date()

day   = REPTDATE.day
month = REPTDATE.month
year  = REPTDATE.year

# SELECT; WHEN(1 <= DAY(REPTDATE) <= 8) ... OTHERWISE ...
if 1 <= day <= 8:
    NOWK = "1"
elif 9 <= day <= 15:
    NOWK = "2"
elif 16 <= day <= 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTYEAR = str(year)[-2:]       # PUT(REPTDATE, YEAR2.)
REPTMON  = f"{month:02d}"       # PUT(MONTH(REPTDATE), Z2.)
REPTDAY  = f"{day:02d}"         # PUT(DAY(REPTDATE), Z2.)
RDATE    = REPTDATE.strftime("%d%m%Y")  # PUT(REPTDATE, DDMMYY8.)

print(f"REPTDATE={REPTDATE}  REPTMON={REPTMON}  REPTDAY={REPTDAY}  "
      f"REPTYEAR={REPTYEAR}  NOWK={NOWK}  RDATE={RDATE}")

# =============================================================================
# OUTPUT PATHS  (derived after macro variables are known)
# =============================================================================
# ICLRGD library   → SAP.PBB.ICLRG.INPUT
ICLRGD_FILE   = BASE_OUTPUT_PATH / f"ICLRGA{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
# ICLRGNEW library → SAP.PBB.ICLRG.INPUTX
ICLRGNEW_FILE = BASE_OUTPUT_PATH / f"ICLRG{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
# SAP.PBB.ICLRGFTP (FTP staging — PROC CPORT replacement)
ICLRGFTP_FILE = BASE_OUTPUT_PATH / f"ICLRGNEW_ICLRGFTP{REPTMON}{NOWK}{REPTYEAR}.parquet"

# =============================================================================
# FIXED-WIDTH FLAT FILE PARSERS
# All @positions are 1-based in SAS; Python uses 0-based slicing [pos-1 : pos-1+len].
# =============================================================================

def _slice_str(line: bytes, start1: int, length: int) -> str:
    """Extract a character field from a fixed-width byte line (1-based start)."""
    s = start1 - 1
    chunk = line[s: s + length]
    return chunk.decode("cp037", errors="replace").rstrip()


def _slice_num(line: bytes, start1: int, length: int) -> int:
    """Extract a zoned-decimal / display numeric field (1-based start)."""
    s = start1 - 1
    chunk = line[s: s + length]
    try:
        return int(chunk.decode("cp037", errors="replace").strip() or "0")
    except ValueError:
        return 0


def _slice_num_float(line: bytes, start1: int, length: int,
                     decimals: int = 0) -> float:
    """
    Extract a display numeric with implied decimals (e.g. SAS 10.2 informat).
    SAS 10.2 means 10-char field with 2 implied decimal places.
    """
    s = start1 - 1
    chunk = line[s: s + length]
    raw = chunk.decode("cp037", errors="replace").strip()
    try:
        value = float(raw)
    except ValueError:
        value = 0.0
    # If SAS informat specifies implied decimals and no decimal point in data
    if "." not in raw and decimals:
        value /= 10 ** decimals
    return value

# -----------------------------------------------------------------------------
# DATA ICLRGORI — INFILE RPTFILE2
# Fixed-width field layout:
#   @001 BNKTYPE  $2.    → [0:2]   char
#   @003 BNKCODE   2.    → [2:4]   num
#   @005 YY        4.    → [4:8]   num
#   @009 MM        2.    → [8:10]  num
#   @011 DD        2.    → [10:12] num
#   @015 CHKNUM   $6.    → [14:20] char
#   @021 PAYBANK   2.    → [20:22] num
#   @023 MICRPAY   7.    → [22:29] num
#   @030 ACCTNO   10.    → [29:39] num
#   @040 TRXCODE  $2.    → [39:41] char
#   @042 AMOUNT   10.2   → [41:51] float (implied 2 dec)
#   @052 PREBANK   2.    → [51:53] num
#   @054 MICRPRE   7.    → [53:60] num
#   @061 CHKTYPE   2.    → [60:62] num
#   @063 BRCODE    5.    → [62:67] num
#   @068 UICCODE  $30.   → [67:97] char
# -----------------------------------------------------------------------------
def parse_rptfile2(filepath: Path) -> pl.DataFrame:
    """
    Read RPTFILE2 (INWARD.SAS.KL) fixed-width flat file.
    Returns a DataFrame with raw columns; CLRGDT is computed afterwards.
    """
    rows = []
    with open(filepath, "rb") as fh:
        for raw_line in fh:
            line = raw_line.rstrip(b"\r\n")
            if not line:
                continue
            rows.append({
                "BNKTYPE": _slice_str(line,   1,  2),
                "BNKCODE": _slice_num(line,   3,  2),
                "YY":      _slice_num(line,   5,  4),
                "MM":      _slice_num(line,   9,  2),
                "DD":      _slice_num(line,  11,  2),
                "CHKNUM":  _slice_str(line,  15,  6),
                "PAYBANK": _slice_num(line,  21,  2),
                "MICRPAY": _slice_num(line,  23,  7),
                "ACCTNO":  _slice_num(line,  30, 10),
                "TRXCODE": _slice_str(line,  40,  2),
                "AMOUNT":  _slice_num_float(line, 42, 10, decimals=2),
                "PREBANK": _slice_num(line,  52,  2),
                "MICRPRE": _slice_num(line,  54,  7),
                "CHKTYPE": _slice_num(line,  61,  2),
                "BRCODE":  _slice_num(line,  63,  5),
                "UICCODE": _slice_str(line,  68, 30),
            })
    df = pl.DataFrame(rows) if rows else pl.DataFrame()
    return df

# -----------------------------------------------------------------------------
# DATA BR — INFILE BRHCODE
# Fixed-width field layout:
#   @007 MICRPAY  7.  → [6:13]  num
#   @017 BRANCH   3.  → [16:19] num
# Note: *IF (001<=BRANCH<=500) is commented out in SAS — not applied here.
# -----------------------------------------------------------------------------
def parse_brhcode(filepath: Path) -> pl.DataFrame:
    """Read BRHCODE fixed-width flat file. Returns MICRPAY, BRANCH."""
    rows = []
    with open(filepath, "rb") as fh:
        for raw_line in fh:
            line = raw_line.rstrip(b"\r\n")
            if not line:
                continue
            rows.append({
                "MICRPAY": _slice_num(line,  7, 7),
                "BRANCH":  _slice_num(line, 17, 3),
            })
    return pl.DataFrame(rows) if rows else pl.DataFrame()

# -----------------------------------------------------------------------------
# DATA ICLRGUIC — INFILE RPTFILE
# Fixed-width field layout (only RCRDTYPE=02 records are kept):
#   @001 RCRDTYPE   2.    → [0:2]   num   (filter: keep only == 2)
#   @003 CHKNUM    $6.    → [2:8]   char
#   @009 MICRPAY    7.    → [8:15]  num
#   @016 ACCTNO    10.    → [15:25] num
#   @026 TRXCODE   $2.    → [25:27] char
#   @028 TRXAMT    10.2   → [27:37] float (PBB field name: TRXAMT, not AMOUNT)
#   @038 MICR       7.    → [37:44] num   (PBB field name: MICR, not MICRPRE)
#   @045 REJECT    $2.    → [44:46] char
#   @057 TRXIND    $1.    → [56:57] char
#   @058 TRXTYPE   $1.    → [57:58] char
#   @059 YY2        4.    → [58:62] num
#   @063 MM2        2.    → [62:64] num
#   @065 DD2        2.    → [64:66] num
#   @070 UICCODE   $30.   → [69:99] char
#
# Note: PBB RPTFILE uses TRXAMT and MICR — different from PIBB which uses
#       AMOUNT and MICRPRE for the same positional fields.
# -----------------------------------------------------------------------------
def parse_rptfile_pbb(filepath: Path) -> pl.DataFrame:
    """
    Read RPTFILE (CCIPS.HOSTDR.KL.CTCS) fixed-width flat file for PBB.
    Filters to RCRDTYPE == 2. Returns DataFrame with DRDATE computed.
    """
    rows = []
    with open(filepath, "rb") as fh:
        for raw_line in fh:
            line = raw_line.rstrip(b"\r\n")
            if not line:
                continue
            rcrdtype = _slice_num(line, 1, 2)
            if rcrdtype != 2:
                continue
            rows.append({
                "CHKNUM":  _slice_str(line,   3,  6),
                "MICRPAY": _slice_num(line,   9,  7),
                "ACCTNO":  _slice_num(line,  16, 10),
                "TRXCODE": _slice_str(line,  26,  2),
                "TRXAMT":  _slice_num_float(line, 28, 10, decimals=2),
                "MICR":    _slice_num(line,  38,  7),
                "REJECT":  _slice_str(line,  45,  2),
                "TRXIND":  _slice_str(line,  57,  1),
                "TRXTYPE": _slice_str(line,  58,  1),
                "YY2":     _slice_num(line,  59,  4),
                "MM2":     _slice_num(line,  63,  2),
                "DD2":     _slice_num(line,  65,  2),
                "UICCODE": _slice_str(line,  70, 30),
            })
    df = pl.DataFrame(rows) if rows else pl.DataFrame()
    if not df.is_empty():
        df = df.with_columns(
            pl.date(pl.col("YY2"), pl.col("MM2"), pl.col("DD2")).alias("DRDATE")
        )
    return df

# =============================================================================
# DATA ICLRGORI  — parse and derive CLRGDT
# PROC SORT; BY MICRPAY  — removed; Polars joins do not require pre-sorting.
# =============================================================================
ICLRGORI = parse_rptfile2(RPTFILE2)
ICLRGORI = ICLRGORI.with_columns(
    pl.date(pl.col("YY"), pl.col("MM"), pl.col("DD")).alias("CLRGDT")
)

# =============================================================================
# DATA BR  — parse BRHCODE
# *IF (001<=BRANCH<=500) is commented out in SAS — not applied here.
# PROC SORT; BY MICRPAY  — removed.
# =============================================================================
BR = parse_brhcode(BRHCODE)

# =============================================================================
# DATA ICLRG1 — MERGE ICLRGORI(IN=A) BR(IN=B); BY MICRPAY; IF A AND B;
# Both A and B must match → inner join on MICRPAY.
# =============================================================================
ICLRG1 = ICLRGORI.join(
    BR,
    on="MICRPAY",
    how="inner",
)

# =============================================================================
# DATA ICLRGD.ICLRGA&REPTYEAR&REPTMON&REPTDAY — KEEP specified columns
# PROC SORT BY UICCODE  — applied; downstream merge requires UICCODE ordering.
# Sort is retained here because the SAS program explicitly PROC SORTs this
# intermediate dataset before the second merge.
# =============================================================================
ICLRGD = ICLRG1.select([
    "BNKTYPE", "BNKCODE", "CLRGDT", "CHKNUM", "PAYBANK", "CHKTYPE",
    "MICRPAY", "ACCTNO",  "AMOUNT", "TRXCODE", "PREBANK", "MICRPRE",
    "BRCODE",  "UICCODE", "BRANCH",
]).sort("UICCODE")

ICLRGD.write_parquet(ICLRGD_FILE)
print(f"ICLRGD written: {ICLRGD_FILE}  ({len(ICLRGD)} rows)")

# =============================================================================
# DATA ICLRGUIC — parse RPTFILE; filter RCRDTYPE=02; compute DRDATE
# PROC SORT BY UICCODE  — removed; Polars left join does not require pre-sort.
# =============================================================================
ICLRGUIC = parse_rptfile_pbb(RPTFILE)

# =============================================================================
# DATA ICLRG5 — MERGE ICLRGD(IN=A) ICLRGUIC; BY UICCODE; IF A;
# IF A (not IF A AND B) → left join: keep all rows from ICLRGD regardless of
# whether ICLRGUIC has a matching UICCODE.
# =============================================================================
ICLRG5 = ICLRGD.join(
    ICLRGUIC,
    on="UICCODE",
    how="left",
)

# =============================================================================
# DATA ICLRGNEW.ICLRG&REPTYEAR&REPTMON&REPTDAY — KEEP specified columns
# Note: AMOUNT comes from the ICLRGD side (the original clearing record).
#       TRXAMT / MICR from ICLRGUIC are not retained in the final KEEP list.
# =============================================================================
# Columns available after the left join; suffix "_right" added by Polars for
# duplicated names from the right-hand side — select only the required columns.
ICLRGNEW = ICLRG5.select([
    "BNKTYPE",  "CLRGDT",   "MICRPRE",  "ACCTNO",  "TRXCODE", "AMOUNT",
    "MICRPAY",  "REJECT",   "TRXIND",   "TRXTYPE", "DRDATE",  "UICCODE",
    "BNKCODE",  "CHKNUM",   "PAYBANK",  "PREBANK", "CHKTYPE", "BRANCH",
])

ICLRGNEW.write_parquet(ICLRGNEW_FILE)
print(f"ICLRGNEW written: {ICLRGNEW_FILE}  ({len(ICLRGNEW)} rows)")

# =============================================================================
# FILENAME TRANFILE / PROC CPORT LIBRARY=ICLRGNEW
# PROC CPORT exports the entire ICLRGNEW library as a SAS transport file.
# Replaced by writing a staging Parquet for FTP/downstream consumption.
# (SAP.PBB.ICLRGFTP equivalent)
# =============================================================================
ICLRGNEW.write_parquet(ICLRGFTP_FILE)
print(f"FTP staging file written: {ICLRGFTP_FILE}  ({len(ICLRGNEW)} rows)")

print("[EIBMDPIC] Program completed successfully.")
