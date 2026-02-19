# !/usr/bin/env python3
"""
Program : EIQPROM1
Purpose : Process billing file to produce LNBILL dataset (BILL records within
            2.5 years and with positive overdue days). Reads packed-decimal
            BILFILE binary input, computes days late, filters and saves results.
          Also prints first 100 rows of LNBILL.BILL with DATE8. formatting.
"""

import duckdb
import polars as pl
import struct
import os
from datetime import date, timedelta

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR      = r"/data"
BNM_DIR       = os.path.join(BASE_DIR, "bnm")           # BNM.REPTDATE parquet
BILFILE_PATH  = os.path.join(BASE_DIR, "bilfile", "BILLFILE.dat")  # binary packed-decimal input
LNBILL_DIR    = os.path.join(BASE_DIR, "lnbill")        # LNBILL.BILL output directory
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

LNBILL_OUTPUT = os.path.join(LNBILL_DIR, "BILL.parquet")
PRINT_OUTPUT  = os.path.join(OUTPUT_DIR,  "EIQPROM1_BILL_PRINT.txt")

os.makedirs(LNBILL_DIR,  exist_ok=True)
os.makedirs(OUTPUT_DIR,  exist_ok=True)

PAGE_LENGTH   = 60   # lines per page (ASA default)

# =============================================================================
# PACKED-DECIMAL HELPER FUNCTIONS
# =============================================================================

def _unpack_pd(raw: bytes) -> int:
    """Decode IBM packed-decimal bytes to integer.
       Each byte holds two BCD digits; last nibble is sign (C=+, D=-)."""
    digits = ""
    for b in raw:
        digits += f"{(b >> 4) & 0x0F}{b & 0x0F}"
    sign_nibble = raw[-1] & 0x0F
    value = int(digits[:-1])   # last nibble is sign
    return -value if sign_nibble == 0x0D else value

def read_packed(raw: bytes, offset: int, length: int) -> int:
    """Read `length` packed-decimal bytes starting at `offset`."""
    return _unpack_pd(raw[offset: offset + length])


# Input record layout (from SAS INPUT statement):
# @001 ACCTNO   PD6.   -> 6 packed-decimal bytes  -> 11-digit number (positions 0-5)
# @007 NOTENO   PD3.   -> 3 packed-decimal bytes   -> 5-digit number  (positions 6-8)
# @010 BLDATE   PD6.   -> 6 packed-decimal bytes   (positions 9-14)
# @016 BLPDDATE PD6.   -> 6 packed-decimal bytes   (positions 15-20)
# @022 DAYSLATE PD2.   -> 2 packed-decimal bytes   (positions 21-22)
# Total record length = 23 bytes (SAS column positions are 1-based)

RECORD_LENGTH = 23  # bytes per record

# Date helpers
SAS_EPOCH   = date(1960, 1, 1)
UNIX_EPOCH  = date(1970, 1, 1)

def sas_date_to_py(sas_days: int) -> date:
    """Convert SAS date (days since 1960-01-01) to Python date."""
    return SAS_EPOCH + timedelta(days=sas_days)


def yymmdd8_to_date(val: int) -> date | None:
    """Parse integer YYYYMMDD -> date (SAS YYMMDD8. informat)."""
    s = f"{val:011d}"   # pad to 11 chars (PD6 max = 11 digits)
    s = s[:8]           # take first 8 digits
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except (ValueError, TypeError):
        return None


def mmddyy8_to_date(val: int) -> date | None:
    """Parse integer MMDDYYYY -> date (SAS MMDDYY8. informat)."""
    s = f"{val:011d}"
    s = s[:8]
    try:
        return date(int(s[4:8]), int(s[:2]), int(s[2:4]))
    except (ValueError, TypeError):
        return None


def date_to_str(d: date | None) -> str:
    """Format date as DATE8. (e.g. 31JUL24)."""
    if d is None:
        return "        "
    return d.strftime("%d%b%y").upper()


# =============================================================================
# STEP 1: Read REPTDATE -> derive REPTDTE and PREPTDTE
# =============================================================================
reptdate_path = os.path.join(BNM_DIR, "REPTDATE.parquet")
con = duckdb.connect()

reptdate_row = con.execute(f"""
    SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1
""").fetchone()

reptdate_raw = reptdate_row[0]
if isinstance(reptdate_raw, (int, float)):
    reptdate = sas_date_to_py(int(reptdate_raw))
elif isinstance(reptdate_raw, date):
    reptdate = reptdate_raw
else:
    reptdate = reptdate_raw.date()

# PREPTDTE calculation (SAS logic):
#   IF MONTH(REPTDATE) >= 6 THEN PMTH = MONTH - 5, PYEAR = YEAR - 2
#   ELSE                         PMTH = MONTH + 7, PYEAR = YEAR - 3
if reptdate.month >= 6:
    pmth  = reptdate.month - 5
    pyear = reptdate.year  - 2
else:
    pmth  = reptdate.month + 7
    pyear = reptdate.year  - 3

preptdte = date(pyear, pmth, 1)

# Print REPTDATE and PDATE (as PROC PRINT DATA=REPTDATE would show)
print(f"REPTDATE: {date_to_str(reptdate)}  PDATE: {date_to_str(preptdte)}")

# SAS numeric date values for comparison
reptdte_sas  = (reptdate  - SAS_EPOCH).days
preptdte_sas = (preptdte  - SAS_EPOCH).days

# =============================================================================
# STEP 2: Read BILFILE (binary packed-decimal) -> BILL dataset
# =============================================================================
records = []

with open(BILFILE_PATH, "rb") as f:
    while True:
        raw = f.read(RECORD_LENGTH)
        if len(raw) < RECORD_LENGTH:
            break

        try:
            acctno   = read_packed(raw,  0, 6)
            noteno   = read_packed(raw,  6, 3)
            bldate   = read_packed(raw,  9, 6)
            blpddate = read_packed(raw, 15, 6)
            dayslate = read_packed(raw, 21, 2)
        except Exception:
            continue  # skip malformed records

        # SAS: BLDAT  = INPUT(SUBSTR(PUT(BLDATE,11.),1,8),YYMMDD8.)
        bldat   = yymmdd8_to_date(bldate)

        # SAS: BLPDDAT = INPUT(SUBSTR(PUT(BLPDDATE,11.),1,8),MMDDYY8.)
        blpddat = mmddyy8_to_date(blpddate)

        # SAS: IF BLPDDAT EQ . THEN DAYS = SUM(REPTDTE, -BLDAT)
        #      ELSE               DAYS = SUM(BLPDDAT,  -BLDAT)
        #      IF BLDAT EQ .      THEN DAYS = 0
        if bldat is None:
            days = 0
        elif blpddat is None:
            days = (reptdate - bldat).days
        else:
            days = (blpddat - bldat).days

        records.append({
            "ACCTNO":   acctno,
            "NOTENO":   noteno,
            "BLDAT":    bldat,
            "BLPDDAT":  blpddat,
            "DAYSLATE": dayslate,
            "DAYS":     days,
        })

bill_df = pl.DataFrame({
    "ACCTNO":   [r["ACCTNO"]   for r in records],
    "NOTENO":   [r["NOTENO"]   for r in records],
    "BLDAT":    [r["BLDAT"]    for r in records],
    "BLPDDAT":  [r["BLPDDAT"]  for r in records],
    "DAYSLATE": [r["DAYSLATE"] for r in records],
    "DAYS":     [r["DAYS"]     for r in records],
})

# =============================================================================
# STEP 3: Filter BILL: BLDAT >= PREPTDTE and DAYS > 0  (within 2.5 yrs)
# =============================================================================
# *** BILL WITHIN 2 1/2 YRS AND MORE THEN 61 DAYS DUE ***
lnbill_df = bill_df.filter(
    (pl.col("BLDAT") >= preptdte) &
    (pl.col("DAYS")  > 0)
)

# =============================================================================
# STEP 4: Save LNBILL.BILL as parquet
# =============================================================================
lnbill_df.write_parquet(LNBILL_OUTPUT)

# Sort by ACCTNO NOTENO (PROC SORT)
lnbill_sorted = lnbill_df.sort(["ACCTNO","NOTENO"])

# =============================================================================
# STEP 5: Print first 100 rows  (PROC PRINT DATA=LNBILL.BILL (OBS=100))
#         FORMAT BLDAT BLPDDAT DATE8.;
# =============================================================================
class ReportWriter:
    """Handles ASA carriage-control output with pagination."""

    def __init__(self, filepath: str, page_length: int = 60):
        self.filepath    = filepath
        self.page_length = page_length
        self._lines      = []

    def put(self, cc: str, text: str):
        self._lines.append((cc, text))

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            page_count = 0
            for (cc, text) in self._lines:
                if cc == '1':
                    if page_count > 0:
                        while (page_count % self.page_length) != 0:
                            f.write(f" \n")
                            page_count += 1
                    f.write(f"1{text}\n")
                    page_count += 1
                else:
                    f.write(f"{cc}{text}\n")
                    page_count += 1


rpt = ReportWriter(PRINT_OUTPUT, PAGE_LENGTH)

# Header
rpt.put('1', 'OBS  ACCTNO          NOTENO  BLDAT       BLPDDAT     DAYSLATE  DAYS')
rpt.put(' ', '-' * 70)

obs_rows = lnbill_sorted.head(100)
for i, row in enumerate(obs_rows.iter_rows(named=True), start=1):
    bldat_str  = date_to_str(row.get("BLDAT"))
    blpddat_str= date_to_str(row.get("BLPDDAT"))
    line = (
        f"{i:>4}  "
        f"{row.get('ACCTNO', ''):>15}  "
        f"{row.get('NOTENO', ''):>6}  "
        f"{bldat_str:<12}"
        f"{blpddat_str:<12}"
        f"{row.get('DAYSLATE', 0):>8}  "
        f"{row.get('DAYS', 0):>4}"
    )
    rpt.put(' ', line)

rpt.write()
con.close()
print(f"LNBILL.BILL saved to : {LNBILL_OUTPUT}")
print(f"Print output saved to: {PRINT_OUTPUT}")
