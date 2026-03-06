#!/usr/bin/env python3
"""
Program  : EIBMDISC.py
Purpose  : TOP 10 DISBURSEMENTS & REPAYMENTS - FOR NORHISHAM
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
# This program depends on the output of EIBMDISB (DISB.DISB parquet file).
# Ensure EIBMDISB has been executed prior to running this program.

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = Path("/data")
BNM_DIR       = BASE_DIR / "bnm"
DISB_DIR      = BASE_DIR / "disb"

REPTDATE_FILE = BNM_DIR  / "reptdate.parquet"
SDESC_FILE    = BNM_DIR  / "sdesc.parquet"
DISB_FILE     = DISB_DIR / "disb.parquet"       # output of EIBMDISB

OUTPUT_REPORT = DISB_DIR / "EIBMDISC_report.txt"

# Ensure output directory exists
DISB_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# PAGE / REPORT CONSTANTS
# ============================================================================

PAGE_LENGTH = 60    # lines per page (default)
LINE_WIDTH  = 132

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_ddmmyy8(d: date) -> str:
    """Format date as DD/MM/YY (SAS DDMMYY8.)"""
    return d.strftime("%d/%m/%y")


def fmt_z2(n: int) -> str:
    """Zero-padded 2-digit integer"""
    return f"{n:02d}"


def fmt_comma15_2(val) -> str:
    """Format number as COMMA15.2 (right-justified, 15 chars)"""
    if val is None:
        return " " * 15
    try:
        formatted = f"{float(val):,.2f}"
        return formatted.rjust(15)
    except (TypeError, ValueError):
        return " " * 15


# ============================================================================
# STEP 1: READ REPTDATE & DERIVE MACRO VARIABLES
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(f"SELECT * FROM read_parquet('{REPTDATE_FILE}')").pl()
row = reptdate_df.row(0, named=True)

REPTDATE: date = row["REPTDATE"]
if isinstance(REPTDATE, datetime):
    REPTDATE = REPTDATE.date()

MM  = REPTDATE.month
MM1 = MM - 1 if MM > 1 else 12

RDATE = fmt_ddmmyy8(REPTDATE)

# ============================================================================
# STEP 2: READ SDESC
# ============================================================================

sdesc_df = con.execute(f"SELECT * FROM read_parquet('{SDESC_FILE}')").pl()
SDESC = str(sdesc_df["SDESC"][0]).ljust(26)[:26]

# ============================================================================
# STEP 3: LOAD DISB.DISB, SORT BY DESCENDING DISBURSE, TAKE OBS=20
# ============================================================================

alw_query = f"""
SELECT *
FROM read_parquet('{DISB_FILE}')
ORDER BY DISBURSE DESC
LIMIT 20
"""
alw = con.execute(alw_query).pl()

# ============================================================================
# STEP 4: PRODUCE REPORT WITH ASA CARRIAGE CONTROL CHARACTERS
# ============================================================================

def write_report(df: pl.DataFrame, output_path: Path) -> None:
    """Write the PROC PRINT equivalent report with ASA carriage control."""
    lines      = []
    page_num   = 0
    line_count = PAGE_LENGTH  # trigger new page on first record

    title1 = "REPORT ID: EIBMDISB(1)"
    title2 = SDESC.strip()
    title3 = "FINANCE DIVISION"
    title4 = f"TOP 10 ACCOUNTS DISBURSED FOR THE MONTH AS AT {RDATE}"

    header_cols = (
        f"{'BRANCH':<10}  {'ACCTNO':<15}  {'NAME':<30}  "
        f"{'PRODUCT':<10}  {'SECTORMA':<10}  "
        f"{'LIMIT':>15}  {'BALANCE':>15}  {'DISBURSE':>15}"
    )
    separator = "-" * LINE_WIDTH

    def new_page() -> list:
        nonlocal page_num
        page_num += 1
        pg_lines = []
        # ASA '1' = form feed / new page
        pg_lines.append(f"1{title1.center(LINE_WIDTH)}")
        pg_lines.append(f" {title2.center(LINE_WIDTH)}")
        pg_lines.append(f" {title3.center(LINE_WIDTH)}")
        pg_lines.append(f" {title4.center(LINE_WIDTH)}")
        pg_lines.append(f" ")
        pg_lines.append(f" {header_cols}")
        pg_lines.append(f" {separator}")
        return pg_lines

    for row in df.iter_rows(named=True):
        if line_count >= PAGE_LENGTH - 4:
            lines.extend(new_page())
            line_count = 7  # header takes ~7 lines

        branch   = str(row.get("BRANCH",   "") or "").ljust(10)
        acctno   = str(row.get("ACCTNO",   "") or "").ljust(15)
        name     = str(row.get("NAME",     "") or "").ljust(30)
        product  = str(row.get("PRODUCT",  "") or "").ljust(10)
        sectorma = str(row.get("SECTORMA", "") or "").ljust(10)
        limit    = fmt_comma15_2(row.get("LIMIT"))
        balance  = fmt_comma15_2(row.get("BALANCE"))
        disburse = fmt_comma15_2(row.get("DISBURSE"))

        detail = (
            f"{branch}  {acctno}  {name}  "
            f"{product}  {sectorma}  "
            f"{limit}  {balance}  {disburse}"
        )
        # ASA ' ' = single space (advance one line)
        lines.append(f" {detail}")
        line_count += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


write_report(alw, OUTPUT_REPORT)

print(f"Report written to: {OUTPUT_REPORT}")
