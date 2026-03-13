#!/usr/bin/env python3
"""
Program : EIBQIRER.py
Date    : 30.10.2001
Report  : NEW LIQUIDITY FRAMEWORK FOR PUBLIC BANK

Date Modified :
SMR/Others    :
Changes Made  :

Purpose:
    - Reads KAPITI Table 1 (BNMTBL1) and Table 3 (BNMTBL3) binary input files.
    - Classifies forex/securities/lending/borrowing records into ITEM categories.
    - Computes remaining months (REMMTH) to maturity from the report date.
    - Produces two PROC TABULATE-style cross-tab reports with ASA carriage
      control characters:
        (1) Breakdown by TYPE (CONVENTIONAL / ISLAMIC / FOREX)
        (2) Consolidated total across all types.
"""

from __future__ import annotations

import os
import struct
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# Dependency: PBBLNFMT – placeholder; import formats/utilities if available.
# from PBBLNFMT import <functions_or_formats>
# Note: PBBLNFMT is referenced via %INC but no specific functions from it are
#       directly called in the logic converted here; it likely defines library
#       assignments and common formats used at the SAS session level.

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BNMTBL1_FILE  = INPUT_DIR / "BNMTBL1.dat"    # binary flat file – Table 1
BNMTBL3_FILE  = INPUT_DIR / "BNMTBL3.dat"    # binary flat file – Table 3
REPORT_TXT    = OUTPUT_DIR / "EIBQIRER_REPORT.txt"

# ---------------------------------------------------------------------------
# Page layout constants
# ---------------------------------------------------------------------------
PAGE_LENGTH = 60   # lines per page (default, not specified in SAS)

# ---------------------------------------------------------------------------
# OPTIONS YEARCUTOFF=1950  →  two-digit year pivot
# ---------------------------------------------------------------------------
YEARCUTOFF = 1950


def _yymmdd6_to_date(raw: str) -> Optional[date]:
    """
    Parse a 6-char YYMMDD string using YEARCUTOFF=1950.
    e.g. '011231' → 2001-12-31,  '491231' → 1949-12-31
    """
    raw = raw.strip()
    if not raw or raw == "000000":
        return None
    try:
        yy = int(raw[0:2])
        mm = int(raw[2:4])
        dd = int(raw[4:6])
        year = (1900 + yy) if (1900 + yy) >= YEARCUTOFF else (2000 + yy)
        return date(year, mm, dd)
    except (ValueError, IndexError):
        return None


def _yymmdd10_to_date(raw: str) -> Optional[date]:
    """Parse YYYY-MM-DD or YYMMDD10 (10-char) date string."""
    raw = raw.strip()
    if not raw or raw in ("0000000000", "          "):
        return None
    try:
        # Accept YYYY-MM-DD or YYYYMMDD
        raw_clean = raw.replace("-", "")
        return date(int(raw_clean[0:4]), int(raw_clean[4:6]), int(raw_clean[6:8]))
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Packed-decimal decoder  (PD8.2, PD5., etc.)
# ---------------------------------------------------------------------------
def _decode_pd(raw_bytes: bytes, decimals: int = 0) -> float:
    """
    Decode IBM packed-decimal (COMP-3) bytes.
    Each byte holds two decimal digits (BCD); last nibble is sign (C=+, D=-).
    """
    if not raw_bytes:
        return 0.0
    hex_str = raw_bytes.hex()
    digits  = hex_str[:-1]          # all nibbles except sign
    sign_c  = hex_str[-1].upper()
    sign    = -1 if sign_c == 'D' else 1
    try:
        value = int(digits) * sign
        return value / (10 ** decimals)
    except ValueError:
        return 0.0


def _month_days(year: int) -> list[int]:
    """Return days-per-month list (1-indexed positions 0..11) for given year."""
    feb = 29 if year % 4 == 0 else 28
    return [31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


# ---------------------------------------------------------------------------
# %MACRO REMMTH  – compute remaining months from reptdate to matdt
# ---------------------------------------------------------------------------
def compute_remmth(matdt: date, reptdate: date) -> float:
    """
    Equivalent of %MACRO REMMTH.

    Commented-out alternatives in original SAS are preserved as comments below.
    Active rule: IF MDDAY > RPDAYS(RPMTH) THEN MDDAY = RPDAYS(RPMTH)
    """
    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rp_days = _month_days(rpyr)

    mdyr  = matdt.year
    mdmth = matdt.month
    mdday = matdt.day
    # md_days = _month_days(mdyr)   # MD1-MD12 array (used in commented blocks)

    #  IF RPDAY = RPDAYS(RPMTH) AND MDDAY = MDDAYS(MDMTH) AND
    #     MDDAY < RPDAY THEN MDDAY = RPDAY;         ← commented in SAS
    #  IF MDDAY = MDDAYS(MDMTH) AND MDDAY < RPDAYS(RPMTH) THEN
    #     MDDAY = RPDAYS(RPMTH);                     ← commented in SAS
    if mdday > rp_days[rpmth - 1]:
        mdday = rp_days[rpmth - 1]

    rem_y = mdyr  - rpyr
    rem_m = mdmth - rpmth
    rem_d = mdday - rpday
    remmth = rem_y * 12 + rem_m + rem_d / rp_days[rpmth - 1]
    return remmth


# ---------------------------------------------------------------------------
# PROC FORMAT REMFMT  – numeric → bucket label
# ---------------------------------------------------------------------------
REMFMT_BUCKETS: list[tuple[float, float, str]] = [
    (float("-inf"), 0.1,  "UP TO 1 WEEK"),
    (0.1,           1.0,  ">1 WK - 1 MTH"),
    (1.0,           3.0,  ">1 MTH - 3 MTHS"),
    (3.0,           6.0,  ">3 - 6 MTHS"),
    (6.0,           12.0, ">6 - 12 MTHS"),
    (12.0,          24.0, ">1 - 2 YEARS"),
    (24.0,          36.0, ">2 - 3 YEARS"),
    (36.0,          48.0, ">3 - 4 YEARS"),
    (48.0,          60.0, ">4 - 5 YEARS"),
    (60.0,          84.0, ">5 - 7 YEARS"),
    (84.0,          120.0,">7 - 10 YEARS"),
    (120.0,         180.0,">10 - 15 YEARS"),
]

def remfmt(remmth: float) -> str:
    """Map numeric REMMTH to REMFMT bucket label."""
    for lo, hi, label in REMFMT_BUCKETS:
        if lo <= remmth < hi:
            return label
    return "OVER 15 YEARS"


# ---------------------------------------------------------------------------
# PROC FORMAT $ITEMF  – ITEM code → description
# ---------------------------------------------------------------------------
ITEMF: dict[str, str] = {
    'A1.01': 'A1.01  FCYFD USD',
    'A1.02': 'A1.02  FCY DEPOSITS USD',
    'A1.03': 'A1.03  FCY BORROWING USD',
    'A1.04': 'A1.04  FCY LENDING USD',
    'A1.05': 'A1.05  FX PURCHASES USD',
    'A1.06': 'A1.06  FX SALES USD',
    'A2.01': 'A1.01  FCYFD OTHER CCY',
    'A2.02': 'A2.02  FCY DEPOSITS OTHER CCY',
    'A2.03': 'A2.03  FCY BORROWING OTHER CCY',
    'A2.04': 'A2.04  FCY LENDING OTHER CCY',
    'A2.05': 'A2.05  FX PURCHASES OTHER CCY',
    'A2.06': 'A2.06  FX SALES OTHER CCY',
    'B1.01': 'B1.01  DEPOSIT PLACED : SLD',
    'B1.02': 'B1.02  DEPOSIT PLACED : SSD',
    'B1.03': 'B1.03  DEPOSIT PLACED : SZD',
    'C1.01': 'C1.01  DEBT SECURITIES HELD : SBA',
    'C1.02': 'C1.02  DEBT SECURITIES HELD : CB1',
    'C1.03': 'C1.03  DEBT SECURITIES HELD : MGS',
    'C1.04': 'C1.04  DEBT SECURITIES HELD : BNB',
    'C1.05': 'C1.05  DEBT SECURITIES HELD : CNT',
    'C1.06': 'C1.06  DEBT SECURITIES HELD : DHB',
    'C1.07': 'C1.07  DEBT SECURITIES HELD : DMB',
    'C1.08': 'C1.08  DEBT SECURITIES HELD : GRL',
    'C1.09': 'C1.09  DEBT SECURITIES HELD : MTN',
    'C1.10': 'C1.10  DEBT SECURITIES HELD : GNL',
    'C1.11': 'C1.11  DEBT SECURITIES HELD : RUL',
    'C1.12': 'C1.12  DEBT SECURITIES HELD : MTB',
    'C1.13': 'C1.13  DEBT SECURITIES HELD : CBB',
    'C1.14': 'C1.14  DEBT SECURITIES HELD : IDS',
    'C1.15': 'C1.15  DEBT SECURITIES HELD : DBD',
    'C1.16': 'C1.16  DEBT SECURITIES HELD : PBA',
    'C1.17': 'C1.17  DEBT SECURITIES HELD : MGI',
    'C1.18': 'C1.18  DEBT SECURITIES HELD : BNN',
    'C1.19': 'C1.19  DEBT SECURITIES HELD : KHA',
    'C1.20': 'C1.20  DEBT SECURITIES HELD : ISB',
    'C1.21': 'C1.21  DEBT SECURITIES HELD : PNB',
    'D1.01': 'D1.01  FLOATING RATE : FRN',
    'E1.01': 'E1.01  LIABILITIES : SSD',
    'E1.02': 'E1.02  LIABILITIES : SZNID',
    'F1.01': 'F1.01  MYR BORROWING CONVENTIONAL',
    'G1.01': 'G1.01  MYR LENDING CONVENTIONAL',
    'H1.01': 'H1.01  FX PURCHASE MYR CONVENTIONAL',
    'I1.01': 'I1.01  FX SALES MYR CONVENTIONAL',
    'B2.01': 'B2.01  DEPOSIT PLACED : SLD',
    'B2.02': 'B2.02  DEPOSIT PLACED : SSD',
    'B2.03': 'B2.03  DEPOSIT PLACED : SZD',
    'C2.01': 'C2.01  DEBT SECURITIES HELD : SBA',
    'C2.02': 'C2.02  DEBT SECURITIES HELD : CB1',
    'C2.03': 'C2.03  DEBT SECURITIES HELD : MGS',
    'C2.04': 'C2.04  DEBT SECURITIES HELD : BNB',
    'C2.05': 'C2.05  DEBT SECURITIES HELD : CNT',
    'C2.06': 'C2.06  DEBT SECURITIES HELD : DHB',
    'C2.07': 'C2.07  DEBT SECURITIES HELD : DMB',
    'C2.08': 'C2.08  DEBT SECURITIES HELD : GRL',
    'C2.09': 'C2.09  DEBT SECURITIES HELD : MTN',
    'C2.10': 'C2.10  DEBT SECURITIES HELD : GNL',
    'C2.11': 'C2.11  DEBT SECURITIES HELD : RUL',
    'C2.12': 'C2.12  DEBT SECURITIES HELD : MTB',
    'C2.13': 'C2.13  DEBT SECURITIES HELD : CBB',
    'C2.14': 'C2.14  DEBT SECURITIES HELD : IDS',
    'C2.15': 'C2.15  DEBT SECURITIES HELD : DBD',
    'C2.16': 'C2.16  DEBT SECURITIES HELD : PBA',
    'C2.17': 'C2.17  DEBT SECURITIES HELD : MGI',
    'C2.18': 'C2.18  DEBT SECURITIES HELD : BNN',
    'C2.19': 'C2.19  DEBT SECURITIES HELD : KHA',
    'C2.20': 'C2.20  DEBT SECURITIES HELD : ISB',
    'C2.21': 'C2.21  DEBT SECURITIES HELD : PNB',
    'D2.01': 'D2.01  FLOATING RATE : FRN',
    'E2.01': 'E2.01  LIABILITIES : SSD',
    'E2.02': 'E2.02  LIABILITIES : SZNID',
    'F2.01': 'F2.01  MYR BORROWING ISLAMIC',
    'G2.01': 'G2.01  MYR LENDING ISLAMIC',
    'H2.01': 'H2.01  FX PURCHASE MYR ISLAMIC',
    'I2.01': 'I2.01  FX SALES MYR ISLAMIC',
}


# ---------------------------------------------------------------------------
# DATA REPTDATE – read report date and derived week/month macros from BNMTBL1
# ---------------------------------------------------------------------------
def read_reptdate(bnmtbl1_path: Path) -> dict:
    """
    Read first record of BNMTBL1, extract REPTDATE at column 50 (YYMMDD6.),
    derive week number (WK), start-date (SDATE), and macro substitution values.
    Columns are 1-based in SAS; Python uses 0-based slicing.
    """
    with open(bnmtbl1_path, "rb") as fh:
        first_line = fh.readline()

    # @50 REPTDATE YYMMDD6.  → bytes 49..54 (0-based)
    raw_date = first_line[49:55].decode("latin-1", errors="replace")
    reptdate = _yymmdd6_to_date(raw_date)
    if reptdate is None:
        raise ValueError(f"Cannot parse REPTDATE from BNMTBL1 first record: {raw_date!r}")

    day = reptdate.day
    if day == 8:
        sdd, wk, wk1 = 1,  "1", "4"
    elif day == 15:
        sdd, wk, wk1 = 9,  "2", "1"
    elif day == 22:
        sdd, wk, wk1 = 16, "3", "2"
    else:
        sdd, wk, wk1 = 23, "4", "3"

    mm = reptdate.month
    if wk == "1":
        mm1 = mm - 1 if mm > 1 else 12
    else:
        mm1 = mm

    sdate = date(reptdate.year, mm, sdd)

    return {
        "reptdate":  reptdate,
        "nowk":      wk,
        "nowk1":     wk1,
        "reptmon":   f"{mm:02d}",
        "reptmon1":  f"{mm1:02d}",
        "reptyear":  str(reptdate.year),
        "reptday":   f"{reptdate.day:02d}",
        "rdate":     reptdate.strftime("%d%m%Y"),
        "sdate":     sdate.strftime("%d%m%Y"),
    }


# ---------------------------------------------------------------------------
# DATA K1TBL – read KAPITI Table 1 (BNMTBL1), skip first record
# ---------------------------------------------------------------------------
def read_k1tbl(bnmtbl1_path: Path, reptdate: date) -> list[dict]:
    """
    Parse BNMTBL1 binary flat file.  Column positions are 1-based in SAS.
    PD8.2 = 8-byte packed decimal, 2 implied decimal places.
    PD5.  = 5-byte packed decimal, 0 implied decimal places (date stored as YYMMDD).
    YYMMDD6. date at column 284 uses YEARCUTOFF=1950.
    """
    records = []
    with open(bnmtbl1_path, "rb") as fh:
        for i, raw in enumerate(fh):
            if i == 0:
                continue  # skip header record

            def _str(start: int, length: int) -> str:
                return raw[start - 1: start - 1 + length].decode("latin-1", errors="replace")

            def _pd(start: int, length: int, dec: int = 0) -> float:
                return _decode_pd(raw[start - 1: start - 1 + length], dec)

            gwsdt_raw = _pd(208, 5, 0)
            gwmdt_raw = _pd(242, 5, 0)

            # IF GWSDT NE 0 THEN GWSDT = INPUT(PUT(GWSDT,Z8.), YYMMDD8.)
            if gwsdt_raw != 0:
                gwsdt_str = f"{int(gwsdt_raw):08d}"
                gwsdt = _yymmdd6_to_date(gwsdt_str[2:])   # YYMMDD from Z8.
            else:
                gwsdt = None

            # IF GWMDT NE 0 THEN GWMDT = INPUT(PUT(GWMDT,Z8.), YYMMDD8.)
            if gwmdt_raw != 0:
                gwmdt_str = f"{int(gwmdt_raw):08d}"
                gwmdt = _yymmdd6_to_date(gwmdt_str[2:])
            else:
                gwmdt = None

            gwcbd_raw = _str(284, 6)
            gwcbd = _yymmdd6_to_date(gwcbd_raw)

            records.append({
                "REPTDATE": reptdate,
                "GWAB":     _str(1,  4),
                "GWAN":     _str(5,  6),
                "GWAS":     _str(11, 3),
                "GWAPP":    _str(14, 2),
                "GWBALA":   _pd(17,  8, 2),
                "GWBALC":   _pd(25,  8, 2),
                "GWSHN":    _str(49, 15),
                "GWCTP":    _str(64, 2),
                "GWACT":    _str(66, 2),
                "GWACD":    _str(68, 2),
                "GWSAC":    _str(70, 2),
                "GWCNAL":   _str(77, 2),
                "GWCCY":    _str(79, 3),
                "GWDIAC":   _pd(94,  8, 2),
                "GWCIAC":   _pd(110, 8, 2),
                "GWDLP":    _str(192, 3),
                "GWDLR":    _str(195, 13),
                "GWSDT":    gwsdt,
                "GWMDT":    gwmdt,
                "GWMVT":    _str(251, 1),
                "GWMVTS":   _str(252, 1),
                "GWOCY":    _str(280, 3),
                "GWCBD":    gwcbd,
            })
    return records


# ---------------------------------------------------------------------------
# DATA K3TBL – read KAPITI Table 3 (BNMTBL3), skip first record
# ---------------------------------------------------------------------------
def read_k3tbl(bnmtbl3_path: Path, reptdate: date) -> list[dict]:
    """
    Parse BNMTBL3 binary flat file.  Column positions are 1-based in SAS.
    PD8.2, PD7.7, PD4. = packed-decimal with respective implied decimals.
    Date fields (UTMDT, UTOSD, UTCBD) are stored as 10-char strings (YYMMDD10.).
    """
    records = []
    with open(bnmtbl3_path, "rb") as fh:
        for i, raw in enumerate(fh):
            if i == 0:
                continue  # skip header record

            def _str(start: int, length: int) -> str:
                return raw[start - 1: start - 1 + length].decode("latin-1", errors="replace")

            def _pd(start: int, length: int, dec: int = 0) -> float:
                return _decode_pd(raw[start - 1: start - 1 + length], dec)

            utmdt_raw = _str(101, 10)
            utosd_raw = _str(210, 10)
            # utcbd_raw = _str(111, 10)   # commented in SAS: REPTDATE not overridden

            matdt = _yymmdd10_to_date(utmdt_raw)
            issdt = _yymmdd10_to_date(utosd_raw)
            # IF UTCBD NE ' ' THEN REPTDATE = ...  ← commented out in SAS

            records.append({
                "REPTDATE": reptdate,
                "UTSTY":    _str(1,   3),
                "UTREF":    _str(4,   16),
                "UTDLP":    _str(20,  3),
                "UTDLR":    _str(23,  13),
                "UTSMN":    _str(36,  16),
                "UTCUS":    _str(52,  6),
                "UTCLC":    _str(58,  3),
                "UTCTP":    _str(61,  2),
                "UTFCV":    _pd(63,   8, 2),
                "UTIDT":    _str(71,  10),
                "UTLCD":    _str(81,  10),
                "UTNCD":    _str(91,  10),
                "UTMDT":    utmdt_raw,
                "UTCBD":    _str(111, 10),
                "UTCPR":    _pd(121,  7, 7),
                "UTQDS":    _pd(128,  7, 7),
                "UTPCP":    _pd(135,  7, 7),
                "UTAMOC":   _pd(142,  8, 2),
                "UTDPF":    _pd(150,  8, 2),
                "UTAICT":   _pd(158,  8, 2),
                "UTAICY":   _pd(166,  8, 2),
                "UTAIT":    _pd(174,  8, 2),
                "UTDPET":   _pd(182,  8, 2),
                "UTDPEY":   _pd(190,  8, 2),
                "UTDPE":    _pd(198,  8, 2),
                "UTASN":    _pd(206,  4, 0),
                "UTOSD":    utosd_raw,
                "UTCA2":    _str(220, 2),
                "UTSAC":    _str(222, 2),
                "UTCNAP":   _str(224, 2),
                "UTCNAR":   _str(226, 2),
                "UTCNAL":   _str(228, 2),
                "MATDT":    matdt,
                "ISSDT":    issdt,
            })
    return records


# ---------------------------------------------------------------------------
# DATA K1TBL1  – classify K1TBL records into ITEM / CAT / TYPE
# BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 3)
# ---------------------------------------------------------------------------
def classify_k1tbl(records: list[dict]) -> list[dict]:
    """
    Apply the IF-ELSE chain from DATA K1TBL1.
    KEEP=TYPE CAT AMOUNT MATDT ITEM MONEY
    """
    out = []
    for r in records:
        gwdlp  = r["GWDLP"].strip()
        gwccy  = r["GWCCY"].strip()
        gwctp  = r["GWCTP"]
        gwapp  = r["GWAPP"].strip()
        gwmvts = r["GWMVTS"].strip()
        gwact  = r["GWACT"].strip()
        gwab   = r["GWAB"].strip()
        matdt  = r["GWMDT"]
        amount = abs(r["GWBALC"])
        typ    = "FOREX"
        item   = ""
        cat    = ""

        ctp1 = gwctp[0:1]

        if gwdlp in ("BF", "BO") and gwccy == "USD" and ctp1 != "B":
            item = "A1.01"; cat = "FCYFD USD"
        elif gwdlp in ("BF", "BO") and gwccy not in ("USD", "MYR") and ctp1 != "B":
            item = "A2.01"; cat = "FCYFD OTHER CCY"
        elif gwdlp in ("BF", "BO") and ctp1 == "B" and gwccy == "USD":
            item = "A1.03"; cat = "FCY BORROWING USD"
        elif gwdlp in ("BF", "BO") and ctp1 == "B" and gwccy not in ("USD", "MYR"):
            item = "A2.03"; cat = "FCY BORROWING OTHER CCY"
        elif gwdlp in ("FDA", "FDB", "FDS", "FDL", "LC", "LF", "LO", "LS") and gwccy == "USD":
            item = "A1.04"; cat = "FCY LENDING USD"
        elif gwdlp in ("FDA", "FDB", "FDS", "FDL", "LC", "LF", "LO", "LS") and gwccy not in ("USD", "MYR"):
            item = "A2.04"; cat = "FCY LENDING OTHER CCY"
        elif gwapp == "FX" and gwccy == "USD" and gwmvts == "P" and gwact not in ("RV", "RW"):
            item = "A1.05"; cat = "FX PURCHASES USD"
        elif gwapp == "FX" and gwccy not in ("USD", "MYR") and gwmvts == "P" and gwact not in ("RV", "RW"):
            item = "A2.05"; cat = "FX PURCHASES OTHER CCY"
        elif gwapp == "FX" and gwccy == "USD" and gwmvts == "S" and gwact not in ("RV", "RW"):
            item = "A1.06"; cat = "FX SALES USD"
        elif gwapp == "FX" and gwccy not in ("USD", "MYR") and gwmvts == "S" and gwact not in ("RV", "RW"):
            item = "A2.06"; cat = "FX SALES OTHER CCY"
        elif gwdlp in ("BF", "BFI", "BO", "BOI") and gwab == "1000" and ctp1 == "B" and gwccy == "MYR":
            typ = "CONVENTIONAL"; item = "F1.01"; cat = "MYR BORROWING CONVENTIONAL"
        elif gwdlp in ("BF", "BFI", "BO", "BOI") and gwab == "2000" and ctp1 == "B" and gwccy == "MYR":
            typ = "ISLAMIC"; item = "F2.01"; cat = "MYR BORROWING ISLAMIC"
        elif gwdlp in ("FDA", "FDB", "FDS", "FDL", "LC", "LF", "LO", "LOI", "LS", "LSI") and \
             gwab == "1000" and ctp1 == "B" and gwccy == "MYR":
            typ = "CONVENTIONAL"; item = "G1.01"; cat = "MYR LENDING CONVENTIONAL"
        elif gwdlp in ("FDA", "FDB", "FDS", "FDL", "LC", "LF", "LO", "LOI", "LS", "LSI") and \
             gwab == "2000" and ctp1 == "B" and gwccy == "MYR":
            typ = "ISLAMIC"; item = "G2.01"; cat = "MYR LENDING ISLAMIC"
        elif gwapp == "FX" and gwccy == "MYR" and gwab == "1000" and gwmvts == "P" and gwact not in ("RV", "RW"):
            typ = "CONVENTIONAL"; item = "H1.01"; cat = "FX PURCHASES MYR CONVENTIONAL"
        elif gwapp == "FX" and gwccy == "MYR" and gwab == "2000" and gwmvts == "P" and gwact not in ("RV", "RW"):
            typ = "ISLAMIC"; item = "H2.01"; cat = "FX PURCHASES MYR ISLAMIC"
        elif gwapp == "FX" and gwccy == "MYR" and gwab == "1000" and gwmvts == "S" and gwact not in ("RV", "RW"):
            typ = "CONVENTIONAL"; item = "I1.01"; cat = "FX SALES MYR CONVENTIONAL"
        elif gwapp == "FX" and gwccy == "MYR" and gwab == "2000" and gwmvts == "S" and gwact not in ("RV", "RW"):
            typ = "ISLAMIC"; item = "I2.01"; cat = "FX SALES MYR ISLAMIC"

        if not item:
            continue  # no output unless item matched

        out.append({
            "TYPE":     typ,
            "CAT":      cat,
            "ITEM":     item,
            "AMOUNT":   amount,
            "MONEY":    amount,
            "MATDT":    matdt,
            "REPTDATE": r["REPTDATE"],
        })
    return out


# ---------------------------------------------------------------------------
# DATA K3TBL1  – classify K3TBL records into ITEM / CAT / TYPE
# BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 3)
# ---------------------------------------------------------------------------
def _k3_conventional(r: dict) -> list[dict]:
    """
    Process UTREF IN ('INV','DRI','DLG') – CONVENTIONAL block.
    IF UTREF IN ('INV','TRD','TAP') → commented in SAS; active refs shown.
    """
    utsty  = r["UTSTY"].strip()
    utamoc = r["UTAMOC"]
    utdpf  = r["UTDPF"]
    utaict = r["UTAICT"]
    utdlp  = r["UTDLP"].strip()

    item   = " "
    cat    = " "
    amount = 0.0

    if utsty == "SLD":
        amount = (utamoc - utdpf) + utaict; item = "B1.01"; cat = "DEPOSIT PLACED"
    elif utsty == "SSD":
        amount = (utamoc - utdpf) + utaict; item = "B1.02"; cat = "DEPOSIT PLACED"
    elif utsty == "SZD":
        amount = (utamoc - utdpf) + utaict; item = "B1.03"; cat = "DEPOSIT PLACED"
    elif utsty == "SBA":
        amount = utamoc - utdpf;            item = "C1.01"; cat = "DEBT SECURITIES HELD"
    elif utsty == "CB1":
        amount = (utamoc - utdpf) + utaict; item = "C1.02"; cat = "DEBT SECURITIES HELD"
    elif utsty == "MGS":
        amount = (utamoc - utdpf) + utaict; item = "C1.03"; cat = "DEBT SECURITIES HELD"
    elif utsty == "BNB":
        amount = utamoc - utdpf;            item = "C1.04"; cat = "DEBT SECURITIES HELD"
    elif utsty == "CNT":
        amount = utamoc - utdpf;            item = "C1.05"; cat = "DEBT SECURITIES HELD"
    elif utsty == "DHB":
        amount = utamoc - utdpf;            item = "C1.06"; cat = "DEBT SECURITIES HELD"
    elif utsty == "DMB":
        amount = utamoc - utdpf;            item = "C1.07"; cat = "DEBT SECURITIES HELD"
    elif utsty == "GRL":
        amount = utamoc - utdpf;            item = "C1.08"; cat = "DEBT SECURITIES HELD"
    elif utsty == "MTN":
        amount = utamoc - utdpf;            item = "C1.09"; cat = "DEBT SECURITIES HELD"
    elif utsty == "GNL":
        amount = utamoc - utdpf;            item = "C1.10"; cat = "DEBT SECURITIES HELD"
    elif utsty == "RUL":
        amount = utamoc - utdpf;            item = "C1.11"; cat = "DEBT SECURITIES HELD"
    elif utsty == "MTB":
        amount = utamoc - utdpf;            item = "C1.12"; cat = "DEBT SECURITIES HELD"
    elif utsty == "CBB":
        amount = utamoc - utdpf;            item = "C1.13"; cat = "DEBT SECURITIES HELD"
    elif utsty == "IDS":
        amount = utamoc - utdpf;            item = "C1.14"; cat = "DEBT SECURITIES HELD"
    elif utsty == "DBD":
        amount = (utamoc - utdpf) + utaict; item = "C1.15"; cat = "DEBT SECURITIES HELD"
    elif utsty == "PNB":
        amount = (utamoc - utdpf) + utaict; item = "C1.21"; cat = "DEBT SECURITIES HELD"
    elif utsty == "PBA":
        if utdlp == "MOP":
            amount = utamoc - utdpf;        item = "C1.16"; cat = "DEBT SECURITIES HELD"
    elif utsty == "MGI":
        amount = (utamoc - utdpf) + utaict; item = "C1.17"; cat = "DEBT SECURITIES HELD"
    elif utsty == "BNN":
        amount = utamoc - utdpf;            item = "C1.18"; cat = "DEBT SECURITIES HELD"
    elif utsty == "KHA":
        amount = utamoc - utdpf;            item = "C1.19"; cat = "DEBT SECURITIES HELD"
    elif utsty == "ISB":
        amount = (utamoc - utdpf) + utaict; item = "C1.20"; cat = "DEBT SECURITIES HELD"
    elif utsty == "FRN":
        amount = (utamoc - utdpf) + utaict; item = "D1.01"; cat = "FLOATING RATE"

    if item.strip():
        amount = abs(amount)
        return [{"TYPE": "CONVENTIONAL", "CAT": cat, "ITEM": item,
                 "AMOUNT": amount, "MONEY": amount,
                 "MATDT": r["MATDT"], "REPTDATE": r["REPTDATE"]}]
    return []


def _k3_liabilities_conventional(r: dict) -> list[dict]:
    """Process UTREF IN ('PZD','PSD') – CONVENTIONAL liabilities."""
    utsty  = r["UTSTY"].strip()
    utamoc = r["UTAMOC"]
    utdpf  = r["UTDPF"]
    item   = " "
    cat    = " "
    amount = 0.0

    if utsty == "ISD":
        amount = utamoc - utdpf; item = "E1.01"; cat = "LIABILITIES"
    elif utsty == "IZD":
        amount = utamoc - utdpf; item = "E1.02"; cat = "LIABILITIES"

    if item.strip():
        amount = abs(amount)
        return [{"TYPE": "CONVENTIONAL", "CAT": cat, "ITEM": item,
                 "AMOUNT": amount, "MONEY": amount,
                 "MATDT": r["MATDT"], "REPTDATE": r["REPTDATE"]}]
    return []


def _k3_islamic(r: dict) -> list[dict]:
    """
    Process UTREF IN ('IINV','IDRI','IDLG') – ISLAMIC block.
    IF UTREF IN ('IINV','ITRD','ITAP') → commented in SAS; active refs shown.
    """
    utsty  = r["UTSTY"].strip()
    utamoc = r["UTAMOC"]
    utdpf  = r["UTDPF"]
    utaict = r["UTAICT"]
    utdlp  = r["UTDLP"].strip()

    item   = " "
    cat    = " "
    amount = 0.0

    if utsty == "SLD":
        amount = (utamoc - utdpf) + utaict; item = "B2.01"; cat = "DEPOSIT PLACED"
    elif utsty == "SSD":
        amount = (utamoc - utdpf) + utaict; item = "B2.02"; cat = "DEPOSIT PLACED"
    elif utsty == "SZD":
        amount = (utamoc - utdpf) + utaict; item = "B2.03"; cat = "DEPOSIT PLACED"
    elif utsty == "SBA":
        amount = utamoc - utdpf;            item = "C2.01"; cat = "DEBT SECURITIES HELD"
    elif utsty == "CB1":
        amount = (utamoc - utdpf) + utaict; item = "C2.02"; cat = "DEBT SECURITIES HELD"
    elif utsty == "MGS":
        amount = (utamoc - utdpf) + utaict; item = "C2.03"; cat = "DEBT SECURITIES HELD"
    elif utsty == "BNB":
        amount = utamoc - utdpf;            item = "C2.04"; cat = "DEBT SECURITIES HELD"
    elif utsty == "CNT":
        amount = utamoc - utdpf;            item = "C2.05"; cat = "DEBT SECURITIES HELD"
    elif utsty == "DHB":
        amount = utamoc - utdpf;            item = "C2.06"; cat = "DEBT SECURITIES HELD"
    elif utsty == "DMB":
        amount = utamoc - utdpf;            item = "C2.07"; cat = "DEBT SECURITIES HELD"
    elif utsty == "GRL":
        amount = utamoc - utdpf;            item = "C2.08"; cat = "DEBT SECURITIES HELD"
    elif utsty == "MTN":
        amount = utamoc - utdpf;            item = "C2.09"; cat = "DEBT SECURITIES HELD"
    elif utsty == "GNL":
        amount = utamoc - utdpf;            item = "C2.10"; cat = "DEBT SECURITIES HELD"
    elif utsty == "RUL":
        amount = utamoc - utdpf;            item = "C2.11"; cat = "DEBT SECURITIES HELD"
    elif utsty == "MTB":
        amount = utamoc - utdpf;            item = "C2.12"; cat = "DEBT SECURITIES HELD"
    elif utsty == "CBB":
        amount = utamoc - utdpf;            item = "C2.13"; cat = "DEBT SECURITIES HELD"
    elif utsty == "IDS":
        amount = utamoc - utdpf;            item = "C2.14"; cat = "DEBT SECURITIES HELD"
    elif utsty == "DBD":
        amount = (utamoc - utdpf) + utaict; item = "C2.15"; cat = "DEBT SECURITIES HELD"
    elif utsty == "PNB":
        amount = (utamoc - utdpf) + utaict; item = "C2.21"; cat = "DEBT SECURITIES HELD"
    elif utsty == "PBA":
        if utdlp == "MOP":
            amount = utamoc - utdpf;        item = "C2.16"; cat = "DEBT SECURITIES HELD"
    elif utsty == "MGI":
        amount = (utamoc - utdpf) + utaict; item = "C2.17"; cat = "DEBT SECURITIES HELD"
    elif utsty == "BNN":
        amount = utamoc - utdpf;            item = "C2.18"; cat = "DEBT SECURITIES HELD"
    elif utsty == "KHA":
        amount = utamoc - utdpf;            item = "C2.19"; cat = "DEBT SECURITIES HELD"
    elif utsty == "ISB":
        amount = (utamoc - utdpf) + utaict; item = "C2.20"; cat = "DEBT SECURITIES HELD"
    elif utsty == "FRN":
        amount = (utamoc - utdpf) + utaict; item = "D2.01"; cat = "FLOATING RATE"

    if item.strip():
        amount = abs(amount)
        return [{"TYPE": "ISLAMIC", "CAT": cat, "ITEM": item,
                 "AMOUNT": amount, "MONEY": amount,
                 "MATDT": r["MATDT"], "REPTDATE": r["REPTDATE"]}]
    return []


def _k3_liabilities_islamic(r: dict) -> list[dict]:
    """Process UTREF IN ('IPZD','IPSD') – ISLAMIC liabilities."""
    utsty  = r["UTSTY"].strip()
    utamoc = r["UTAMOC"]
    utdpf  = r["UTDPF"]
    item   = " "
    cat    = " "
    amount = 0.0

    if utsty == "ISD":
        amount = utamoc - utdpf; item = "E2.01"; cat = "LIABILITIES"
    elif utsty == "IZD":
        amount = utamoc - utdpf; item = "E2.02"; cat = "LIABILITIES"

    if item.strip():
        amount = abs(amount)
        return [{"TYPE": "ISLAMIC", "CAT": cat, "ITEM": item,
                 "AMOUNT": amount, "MONEY": amount,
                 "MATDT": r["MATDT"], "REPTDATE": r["REPTDATE"]}]
    return []


def classify_k3tbl(records: list[dict]) -> list[dict]:
    """Apply all classification blocks from DATA K3TBL1."""
    out = []
    for r in records:
        utref = r["UTREF"].strip()
        # IF UTREF IN ('INV','TRD','TAP') → commented; active: ('INV','DRI','DLG')
        if utref in ("INV", "DRI", "DLG"):
            out.extend(_k3_conventional(r))
        if utref in ("PZD", "PSD"):
            out.extend(_k3_liabilities_conventional(r))
        # IF UTREF IN ('IINV','ITRD','ITAP') → commented; active: ('IINV','IDRI','IDLG')
        if utref in ("IINV", "IDRI", "IDLG"):
            out.extend(_k3_islamic(r))
        if utref in ("IPZD", "IPSD"):
            out.extend(_k3_liabilities_islamic(r))
    return out


# ---------------------------------------------------------------------------
# DATA KTBL – merge K1TBL1 + K3TBL1, compute REMMTH
# BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 3)
# ---------------------------------------------------------------------------
def build_ktbl(k1_rows: list[dict], k3_rows: list[dict], reptdate: date) -> pl.DataFrame:
    """
    Combine K1TBL1 and K3TBL1, apply %REMMTH macro, filter ITEM ne ' '.
    KEEP = TYPE CAT ITEM REMMTH AMOUNT MONEY
    """
    all_rows = k1_rows + k3_rows
    out = []
    for r in all_rows:
        if not r.get("ITEM", "").strip():
            continue
        matdt = r.get("MATDT")
        if matdt is None:
            continue
        delta_days = (matdt - reptdate).days
        if delta_days < 8:
            remmth = 0.1
        else:
            remmth = compute_remmth(matdt, reptdate)

        out.append({
            "TYPE":   r["TYPE"],
            "CAT":    r["CAT"],
            "ITEM":   r["ITEM"],
            "REMMTH": remmth,
            "AMOUNT": r["AMOUNT"],
            "MONEY":  r["MONEY"],
        })

    return pl.DataFrame(out, schema={
        "TYPE":   pl.Utf8,
        "CAT":    pl.Utf8,
        "ITEM":   pl.Utf8,
        "REMMTH": pl.Float64,
        "AMOUNT": pl.Float64,
        "MONEY":  pl.Float64,
    })


# ---------------------------------------------------------------------------
# PROC TABULATE  – cross-tab formatting helpers
# ---------------------------------------------------------------------------
# Ordered REMFMT bucket labels for column layout
BUCKET_ORDER = [
    "UP TO 1 WEEK",
    ">1 WK - 1 MTH",
    ">1 MTH - 3 MTHS",
    ">3 - 6 MTHS",
    ">6 - 12 MTHS",
    ">1 - 2 YEARS",
    ">2 - 3 YEARS",
    ">3 - 4 YEARS",
    ">4 - 5 YEARS",
    ">5 - 7 YEARS",
    ">7 - 10 YEARS",
    ">10 - 15 YEARS",
    "OVER 15 YEARS",
    "TOTAL",
]

# Ordered ITEM codes for row layout (matches $ITEMF definition order)
ITEM_ORDER = [
    "A1.01","A1.02","A1.03","A1.04","A1.05","A1.06",
    "A2.01","A2.02","A2.03","A2.04","A2.05","A2.06",
    "B1.01","B1.02","B1.03",
    "C1.01","C1.02","C1.03","C1.04","C1.05","C1.06","C1.07",
    "C1.08","C1.09","C1.10","C1.11","C1.12","C1.13","C1.14",
    "C1.15","C1.16","C1.17","C1.18","C1.19","C1.20","C1.21",
    "D1.01","E1.01","E1.02",
    "F1.01","G1.01","H1.01","I1.01",
    "B2.01","B2.02","B2.03",
    "C2.01","C2.02","C2.03","C2.04","C2.05","C2.06","C2.07",
    "C2.08","C2.09","C2.10","C2.11","C2.12","C2.13","C2.14",
    "C2.15","C2.16","C2.17","C2.18","C2.19","C2.20","C2.21",
    "D2.01","E2.01","E2.02",
    "F2.01","G2.01","H2.01","I2.01",
]

COL_WIDTH = 20    # width of each numeric column (COMMA18. → 18 digits + sign + comma)
RTS       = 45    # row title size (BOX/ITEM description width)


def _fmt_amount(val: float) -> str:
    """Format value as COMMA18. (right-justified in COL_WIDTH)."""
    if val == 0.0:
        return f"{'0':>{COL_WIDTH}}"
    formatted = f"{val:,.0f}"
    return f"{formatted:>{COL_WIDTH}}"


def _build_pivot(
    df: pl.DataFrame,
    value_col: str,
    type_filter: Optional[str] = None,
) -> dict[tuple[str, str], float]:
    """
    Build a {(item, bucket): amount} pivot from KTBL.
    If type_filter is given, pre-filter by TYPE.
    """
    src = df if type_filter is None else df.filter(pl.col("TYPE") == type_filter)
    pivot: dict[tuple[str, str], float] = {}
    for row in src.iter_rows(named=True):
        key = (row["ITEM"], remfmt(row["REMMTH"]))
        pivot[key] = pivot.get(key, 0.0) + row[value_col]
    return pivot


def _render_tabulate_section(
    pivot: dict[tuple[str, str], float],
    items_in_data: set[str],
    section_label: str,
    value_col_label: str,
) -> list[str]:
    """
    Render one TYPE section of the PROC TABULATE output as fixed-width text.
    Returns list of lines (no ASA prefix – caller adds that).
    """
    # Column header row
    header_parts = [f"{'ITEMS':<{RTS}}"]
    for bkt in BUCKET_ORDER:
        header_parts.append(f"{bkt:>{COL_WIDTH}}")
    lines = [" ".join(header_parts)]

    sep = "-" * (RTS + len(BUCKET_ORDER) * (COL_WIDTH + 1))
    lines.append(sep)

    # Section type label
    lines.append(f"{section_label}")

    # Item rows
    grand_row: dict[str, float] = {b: 0.0 for b in BUCKET_ORDER}
    for item in ITEM_ORDER:
        if item not in items_in_data:
            continue
        item_label = ITEMF.get(item, item)
        row_parts  = [f"  {item_label:<{RTS - 2}}"]
        row_total  = 0.0
        for bkt in BUCKET_ORDER[:-1]:
            val = pivot.get((item, bkt), 0.0)
            grand_row[bkt] = grand_row.get(bkt, 0.0) + val
            row_total += val
            row_parts.append(_fmt_amount(val))
        grand_row["TOTAL"] = grand_row.get("TOTAL", 0.0) + row_total
        row_parts.append(_fmt_amount(row_total))
        lines.append(" ".join(row_parts))

    # TOTAL row
    total_parts = [f"{'TOTAL':<{RTS}}"]
    for bkt in BUCKET_ORDER:
        total_parts.append(_fmt_amount(grand_row.get(bkt, 0.0)))
    lines.append(sep)
    lines.append(" ".join(total_parts))
    return lines


def generate_tabulate_report(
    ktbl: pl.DataFrame,
    rdate_str: str,
    title4: str,
) -> tuple[list[str], list[str]]:
    """
    Produce two PROC TABULATE reports:
      report1_lines – breakdown by TYPE (CONVENTIONAL / ISLAMIC / FOREX)
      report2_lines – consolidated total (ALL)
    Returns lines with ASA carriage control prefix character.
    """
    title1 = "PUBLIC BANK BERHAD              REPORT NO : EIBQIRER"
    title2 = "TREASURY PROCESSING"
    title3 = ("VALUE OF CONTRACT CLASSIFIED BY REMAINING PERIOD TO MATURITY/NEXT "
               "REPRICING DATE")

    items_in_data = set(ktbl["ITEM"].to_list())

    # ---- REPORT 1: by TYPE ----
    r1_lines: list[str] = []
    r1_lines.append(f"1{title1}")    # '1' = new page
    r1_lines.append(f" {title2}")
    r1_lines.append(f" {title3}")
    r1_lines.append(f" {title4}")
    r1_lines.append(f" {'CONTRACT VALUES OVER REMAINING PERIOD':^{RTS + len(BUCKET_ORDER) * (COL_WIDTH + 1)}}")

    for typ in ("FOREX", "CONVENTIONAL", "ISLAMIC"):
        pivot = _build_pivot(ktbl, "AMOUNT", type_filter=typ)
        items_in_type = {item for (item, _) in pivot}
        section_lines = _render_tabulate_section(
            pivot, items_in_type, typ, "AMOUNT"
        )
        for ln in section_lines:
            r1_lines.append(f" {ln}")
        r1_lines.append(f" ")   # blank separator between types

    # TOTAL across all types
    pivot_all = _build_pivot(ktbl, "AMOUNT")
    items_all = {item for (item, _) in pivot_all}
    for ln in _render_tabulate_section(pivot_all, items_all, "TOTAL", "AMOUNT"):
        r1_lines.append(f" {ln}")

    # ---- REPORT 2: ALL consolidated (using MONEY) ----
    r2_lines: list[str] = []
    r2_lines.append(f"1{title1}")
    r2_lines.append(f" {title2}")
    r2_lines.append(f" {title3}")
    r2_lines.append(f" {title4}")
    r2_lines.append(f" {'CONTRACT VALUES OVER REMAINING PERIOD':^{RTS + len(BUCKET_ORDER) * (COL_WIDTH + 1)}}")

    pivot_money = _build_pivot(ktbl, "MONEY")
    items_money = {item for (item, _) in pivot_money}
    for ln in _render_tabulate_section(pivot_money, items_money, "ALL", "MONEY"):
        r2_lines.append(f" {ln}")

    return r1_lines, r2_lines


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # Step 1 – read report date from BNMTBL1
    meta = read_reptdate(BNMTBL1_FILE)
    reptdate  = meta["reptdate"]
    rdate_str = meta["rdate"]
    print(f"REPTDATE: {reptdate}  RDATE: {rdate_str}")

    # Step 2 – read and classify K1TBL (Table 1)
    k1_raw  = read_k1tbl(BNMTBL1_FILE, reptdate)
    k1_rows = classify_k1tbl(k1_raw)
    print(f"K1TBL1 classified rows: {len(k1_rows)}")

    # Step 3 – read and classify K3TBL (Table 3)
    k3_raw  = read_k3tbl(BNMTBL3_FILE, reptdate)
    k3_rows = classify_k3tbl(k3_raw)
    print(f"K3TBL1 classified rows: {len(k3_rows)}")

    # Step 4 – build KTBL with REMMTH
    ktbl = build_ktbl(k1_rows, k3_rows, reptdate)
    print(f"KTBL rows: {len(ktbl)}")

    # Step 5 – PROC TABULATE reports
    # PRODUCE REPORTS
    title4 = f"QUARTERLY INTEREST RATE EXPOSURE REPORT AS AT {rdate_str}"
    r1_lines, r2_lines = generate_tabulate_report(ktbl, rdate_str, title4)

    all_lines = r1_lines + [""] + r2_lines
    REPORT_TXT.write_text("\n".join(all_lines) + "\n", encoding="utf-8")
    print(f"Written: {REPORT_TXT}")


if __name__ == "__main__":
    main()
