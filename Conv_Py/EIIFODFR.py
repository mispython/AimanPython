#!/usr/bin/env python3
"""
Program  : EIIFODFR.py
Purpose  : OD Balance by Individuals and Corporates for Liquidity Framework.
           PUBLIC ISLAMIC BANK BERHAD
           Reads OD monthly data; filters by PRODCD IN ('34180','34240');
            prints PROC PRINT report grouped by REPTDATE.
"""

# ============================================================================
# No %INC program dependencies declared in SAS source.
# All logic is self-contained within this program.
# ============================================================================

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# OPTIONS (SAS equivalents)
# NOCENTER YEARCUTOFF=1950 -> handled in date parsing and formatting
# ============================================================================

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR    = Path("/data")
DEPOSIT_DIR = BASE_DIR / "deposit"   # DEPOSIT libref
BNM_DIR     = BASE_DIR / "bnm"       # BNM libref
OUTPUT_DIR  = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = DEPOSIT_DIR / "reptdate.parquet"
OUTPUT_REPORT = OUTPUT_DIR  / "eiifodfr_report.txt"

# ============================================================================
# ASA CARRIAGE CONTROL CONSTANTS
# ============================================================================

ASA_NEWPAGE     = '1'
ASA_SINGLESPACE = ' '
ASA_DOUBLESPACE = '0'

PAGE_LENGTH = 60
PAGE_WIDTH  = 132

# ============================================================================
# REPORT TITLES
# ============================================================================

TITLE1 = 'REPORT ID : EIIFODFR'
TITLE2 = 'PUBLIC ISLAMIC BANK BERHAD'
TITLE3 = ''

# ============================================================================
# DATA REPTDATE
#   SET DEPOSIT.REPTDATE
#   MM = MONTH(REPTDATE)
#   YY = YEAR(REPTDATE)
#   STDATE = MDY(MM,1,YY)   -> first day of reporting month
#   REPTYEAR = YEAR4.
#   REPTMON  = Z2.
#   REPTDAY  = Z2.
#   STDATE   = Z5.           -> SAS date serial of month start
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(
    f"SELECT * FROM read_parquet('{REPTDATE_FILE}')"
).pl()
row_rep = reptdate_df.row(0, named=True)

REPTDATE_VAL: date = row_rep["REPTDATE"]
if isinstance(REPTDATE_VAL, datetime):
    REPTDATE_VAL = REPTDATE_VAL.date()

MM       = REPTDATE_VAL.month
YY       = REPTDATE_VAL.year
STDATE   = date(YY, MM, 1)                          # MDY(MM,1,YY)

REPTYEAR = f"{YY}"                                   # YEAR4.
REPTMON  = f"{MM:02d}"                               # Z2.
REPTDAY  = f"{REPTDATE_VAL.day:02d}"                 # Z2.
# STDATE as SAS Z5. serial (days since 01JAN1960) — used for comparison
STDATE_SERIAL = (STDATE - date(1960, 1, 1)).days

# ============================================================================
# DATA CURRENT
#   SET BNM.OD&REPTMON
#   IF REPTDATE GE &STDATE
# PROC SORT BY REPTDATE PRODCD
# ============================================================================

OD_FILE = BNM_DIR / f"od{REPTMON}.parquet"

od_df = con.execute(
    f"SELECT * FROM read_parquet('{OD_FILE}')"
).pl()

# Filter: IF REPTDATE GE &STDATE
# REPTDATE in source may be stored as date or SAS serial integer
current_rows = []
for r in od_df.to_dicts():
    rd_raw = r.get("REPTDATE")
    if isinstance(rd_raw, (date, datetime)):
        rd = rd_raw.date() if isinstance(rd_raw, datetime) else rd_raw
        rd_serial = (rd - date(1960, 1, 1)).days
    else:
        rd_serial = int(rd_raw or 0)
        rd = date(1960, 1, 1) + __import__("datetime").timedelta(days=rd_serial)

    if rd_serial >= STDATE_SERIAL:
        r["_REPTDATE"] = rd    # resolved date for formatting
        current_rows.append(r)

# Sort BY REPTDATE PRODCD
current_rows.sort(key=lambda r: (r["_REPTDATE"], str(r.get("PRODCD") or "")))

# ============================================================================
# PROC PRINT DATA=CURRENT SPLIT='/'
#   BY REPTDATE; ID REPTDATE
#   VAR PRODCD ODTOT ODINDV ODCORP
#   WHERE PRODCD IN ('34180','34240')
#   FORMAT REPTDATE DDMMYY8. ODTOT ODINDV ODCORP COMMA18.2
# ============================================================================

VALID_PRODCD = {"34180", "34240"}

TITLE4 = "OD BALANCE BY INDIVIDUALS AND CORPORATES"
TITLE5 = f"FOR LIQUIDITY FRAMEWORK AS AT: {REPTMON}/{REPTYEAR}"


def fmt_ddmmyy8(d: date) -> str:
    """DDMMYY8. format -> DD/MM/YY (8 chars)."""
    return d.strftime("%d/%m/%y")


def fmt_comma18(val) -> str:
    """COMMA18.2 format."""
    try:
        return f"{float(val or 0.0):,.2f}".rjust(18)
    except (TypeError, ValueError):
        return "0.00".rjust(18)


def page_header() -> list:
    """Generate report page header with ASA carriage control."""
    lines = []
    lines.append(f"{ASA_NEWPAGE}{TITLE1}")
    lines.append(f"{ASA_SINGLESPACE}{TITLE2}")
    lines.append(f"{ASA_SINGLESPACE}{TITLE3}")
    lines.append(f"{ASA_SINGLESPACE}{TITLE4}")
    lines.append(f"{ASA_SINGLESPACE}{TITLE5}")
    lines.append(f"{ASA_DOUBLESPACE}")
    # Column header (SPLIT='/' splits label at '/' onto separate header lines)
    lines.append(
        f"{ASA_SINGLESPACE}"
        f"{'REPORT DATE':<12}  "
        f"{'PRODUCT':<10}  "
        f"{'CODE':<10}  "
        f"{'TOTAL OD':>18}  "
        f"{'INDIVIDUALS':>18}  "
        f"{'CORPORATES':>18}"
    )
    lines.append(
        f"{ASA_SINGLESPACE}"
        f"{'':12}  "
        f"{'':10}  "
        f"{'':10}  "
        f"{'-'*18}  "
        f"{'-'*18}  "
        f"{'-'*18}"
    )
    return lines


report_lines = page_header()

# Group by REPTDATE (BY REPTDATE; ID REPTDATE)
from itertools import groupby as _groupby

filtered = [
    r for r in current_rows
    if str(r.get("PRODCD") or "").strip() in VALID_PRODCD
]

for reptdate_key, grp in _groupby(filtered, key=lambda r: r["_REPTDATE"]):
    grp_rows     = list(grp)
    rdate_str    = fmt_ddmmyy8(reptdate_key)
    first_in_grp = True

    for r in grp_rows:
        prodcd = str(r.get("PRODCD") or "").strip()
        odtot  = r.get("ODTOT")
        odindv = r.get("ODINDV")
        odcorp = r.get("ODCORP")

        # ID REPTDATE -> print date only on first row of BY group
        date_col = rdate_str if first_in_grp else ""
        first_in_grp = False

        report_lines.append(
            f"{ASA_SINGLESPACE}"
            f"{date_col:<12}  "
            f"{prodcd:<20}  "
            f"{fmt_comma18(odtot)}  "
            f"{fmt_comma18(odindv)}  "
            f"{fmt_comma18(odcorp)}"
        )

    # Blank line between BY groups
    report_lines.append(f"{ASA_SINGLESPACE}")

# ============================================================================
# WRITE REPORT OUTPUT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as fh:
    fh.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
