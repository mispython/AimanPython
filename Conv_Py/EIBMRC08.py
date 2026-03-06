#!/usr/bin/env python3
"""
Program  : EIBMRC08.py
Purpose  : Loans & Advances Discounted with CAGAMAS by Sector.
           USER      : PUBLIC BANK BERHAD, RETAIL CREDIT DIVISION
           OBJECTIVE : LOANS & ADVANCES DISCOUNTED WITH CAGAMAS BY SECTOR
           FREQUENCY : MONTHLY
           SELECTION CRITERIA : PRODUCTS : 225, 226
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
# %INC PGM(EIBMMISF) - Format definitions (lookup maps and classification functions)

from EIBMMISF import fmt_sectdes

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# OPTIONS (SAS equivalents)
# NOCENTER NONUMBER -> handled in report formatting
# ============================================================================

MISSING_NUM = 0  # numeric missing displayed as 0

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR   = Path("/data")
BNM_DIR    = BASE_DIR / "bnm"
LOAN_DIR   = BASE_DIR / "loan"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = BNM_DIR  / "reptdate.parquet"
SDESC_FILE    = LOAN_DIR / "sdesc.parquet"

OUTPUT_REPORT = OUTPUT_DIR / "eibmrc08_report.txt"

# ============================================================================
# ASA CARRIAGE CONTROL CONSTANTS
# ============================================================================

ASA_NEWPAGE     = '1'  # Form feed / new page
ASA_SINGLESPACE = ' '  # Single space (advance 1 line before print)
ASA_DOUBLESPACE = '0'  # Double space (advance 2 lines before print)

PAGE_LENGTH = 60   # lines per page (default)
PAGE_WIDTH  = 132

# ============================================================================
# DATA _NULL_: READ REPTDATE -> derive macro variables
#   NOWK   = '4'  (hardcoded in SAS)
#   RDATE  = DDMMYY10. format  e.g. '01/06/2024'
#   REPTMON = Z2. format        e.g. '06'
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(
    f"SELECT * FROM read_parquet('{REPTDATE_FILE}')"
).pl()
row_rep = reptdate_df.row(0, named=True)

REPTDATE_VAL: date = row_rep["REPTDATE"]
if isinstance(REPTDATE_VAL, datetime):
    REPTDATE_VAL = REPTDATE_VAL.date()

# NOWK is hardcoded as '4' in the SAS program (PUT('4',$1.))
NOWK    = '4'
RDATE   = REPTDATE_VAL.strftime("%d/%m/%Y")   # DDMMYY10. -> DD/MM/YYYY
REPTMON = f"{REPTDATE_VAL.month:02d}"

# ============================================================================
# DATA DESC: READ LOAN.SDESC -> derive SDESC macro variable
# ============================================================================

sdesc_df = con.execute(
    f"SELECT * FROM read_parquet('{SDESC_FILE}')"
).pl()
row_sdesc = sdesc_df.row(0, named=True)

# PUT(SDESC,$26.) -> left-justified in 26 characters
SDESC: str = str(row_sdesc.get("SDESC") or "").ljust(26)[:26]

# ============================================================================
# DATA CAGAMAS
#   SET LOAN.LOAN&REPTMON&NOWK
#   WHERE PRODUCT IN (225,226)
#         AND SECTORCD IN ('0311','0312','0313','0314','0315','0316','0321')
#   IF SUBSTR(SECTORCD,1,3) = '031' THEN SECTTYPE = '0310'
#   ELSE IF SECTORCD = '0321'        THEN SECTTYPE = '0320'
# ============================================================================

LOAN_FILE = LOAN_DIR / f"loan{REPTMON}{NOWK}.parquet"

loan_df = con.execute(
    f"SELECT * FROM read_parquet('{LOAN_FILE}')"
).pl()

VALID_PRODUCTS  = {225, 226}
VALID_SECTORCDS = {'0311', '0312', '0313', '0314', '0315', '0316', '0321'}

cagamas_rows = []
for r in loan_df.to_dicts():
    product  = int(r.get("PRODUCT")  or 0)
    sectorcd = str(r.get("SECTORCD") or "").strip()

    if product not in VALID_PRODUCTS:
        continue
    if sectorcd not in VALID_SECTORCDS:
        continue

    # Derive SECTTYPE
    if sectorcd[:3] == '031':
        r["SECTTYPE"] = '0310'
    elif sectorcd == '0321':
        r["SECTTYPE"] = '0320'
    else:
        r["SECTTYPE"] = ''

    cagamas_rows.append(r)

cagamas = (
    pl.DataFrame(cagamas_rows).sort("SECTORCD")
    if cagamas_rows else pl.DataFrame()
)

# ============================================================================
# PROC TABULATE DATA=CAGAMAS NOSEPS
#   CLASS SECTORCD
#   VAR   APPRLIMT BALANCE
#   FORMAT SECTTYPE $SECTDES. SECTORCD $SECTDES.
#   TABLE SECTORCD='SECTOR ' ALL='TOTAL',
#         N*F=COMMA10.
#         APPRLIMT='APPROVED LIMIT'*F=COMMA20.2
#         BALANCE='OUTSTANDING CURBAL'*F=COMMA20.2
#         / RTS=20 BOX=_PAGE_
#   KEYLABEL SUM=' ' N='NO. OF ACCOUNT'
# ============================================================================

# ============================================================================
# REPORT GENERATION HELPERS
# ============================================================================

TITLE1_STR = f"{SDESC.strip()}  RETAIL CREDIT DIVISION"
TITLE2_STR = "008 LOANS & ADVANCES DISCOUNTED WITH CAGAMAS"
TITLE3_STR = f"REPORTING DATE AS AT : {RDATE}"

# Column widths matching SAS FORMAT specs
RTS_W    = 20   # RTS=20  -> row label width
N_W      = 10   # COMMA10.
AMT_W    = 20   # COMMA20.2


def fmt_comma(val, width: int = 20, dec: int = 2) -> str:
    """Format number with commas to specified width and decimal places."""
    try:
        f = float(val if val is not None else MISSING_NUM)
        return f"{f:,.{dec}f}".rjust(width)
    except (TypeError, ValueError):
        return str(MISSING_NUM).rjust(width)


def fmt_comma_int(val, width: int = 10) -> str:
    """Format integer with commas (COMMA10.)."""
    try:
        f = float(val if val is not None else MISSING_NUM)
        return f"{int(round(f)):,}".rjust(width)
    except (TypeError, ValueError):
        return str(MISSING_NUM).rjust(width)


def build_report() -> list:
    """Build the full report lines with ASA carriage control."""
    lines = []

    # Page header (new page)
    lines.append(f"{ASA_NEWPAGE}{TITLE1_STR}")
    lines.append(f"{ASA_SINGLESPACE}{TITLE2_STR}")
    lines.append(f"{ASA_SINGLESPACE}{TITLE3_STR}")
    lines.append(f"{ASA_DOUBLESPACE}")

    # Column header
    # BOX=_PAGE_ -> page title acts as top-left box label (already in title)
    # KEYLABEL N='NO. OF ACCOUNT'
    sep_line = f"{'-' * RTS_W}  {'-' * N_W}  {'-' * AMT_W}  {'-' * AMT_W}"
    header_row = (
        f"{ASA_SINGLESPACE}{'SECTOR':<{RTS_W}}  "
        f"{'NO. OF ACCOUNT':>{N_W}}  "
        f"{'APPROVED LIMIT':>{AMT_W}}  "
        f"{'OUTSTANDING CURBAL':>{AMT_W}}"
    )
    lines.append(header_row)
    lines.append(f"{ASA_SINGLESPACE}{sep_line}")

    if cagamas.is_empty():
        lines.append(f"{ASA_SINGLESPACE}NO DATA")
        return lines

    # Aggregate by SECTORCD: N (count), SUM(APPRLIMT), SUM(BALANCE)
    agg = (
        cagamas.group_by("SECTORCD")
               .agg([
                   pl.len().alias("N"),
                   pl.col("APPRLIMT").sum().alias("APPRLIMT"),
                   pl.col("BALANCE").sum().alias("BALANCE"),
               ])
               .sort("SECTORCD")
    )

    total_n        = 0
    total_apprlimt = 0.0
    total_balance  = 0.0

    for r in agg.to_dicts():
        sectorcd  = str(r.get("SECTORCD") or "").strip()
        n_val     = int(r.get("N")        or 0)
        apprlimt  = float(r.get("APPRLIMT") or 0.0)
        balance   = float(r.get("BALANCE")  or 0.0)

        # FORMAT SECTORCD $SECTDES. -> use fmt_sectdes from EIBMMISF
        sector_label = fmt_sectdes(sectorcd)

        lines.append(
            f"{ASA_SINGLESPACE}{sector_label:<{RTS_W}}  "
            f"{fmt_comma_int(n_val)}  "
            f"{fmt_comma(apprlimt)}  "
            f"{fmt_comma(balance)}"
        )

        total_n        += n_val
        total_apprlimt += apprlimt
        total_balance  += balance

    # ALL='TOTAL' row
    lines.append(f"{ASA_SINGLESPACE}{sep_line}")
    lines.append(
        f"{ASA_SINGLESPACE}{'TOTAL':<{RTS_W}}  "
        f"{fmt_comma_int(total_n)}  "
        f"{fmt_comma(total_apprlimt)}  "
        f"{fmt_comma(total_balance)}"
    )

    return lines


# ============================================================================
# WRITE REPORT OUTPUT
# ============================================================================

report_lines = build_report()

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
