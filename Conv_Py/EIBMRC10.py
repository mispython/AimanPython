#!/usr/bin/env python3
"""
Program  : EIBMRC10.py
Purpose  : Progress Report on Retail & Corporate Loans by State.
           USER      : PUBLIC BANK BERHAD, RETAIL CREDIT DIVISION
           OBJECTIVE : PROGRESS REPORT ON RETAIL & CORPORATE LOANS
                       BY STATE - COMPARISION OF PREVIOUS & CURRENT
           FREQUENCY : MONTHLY
           SELECTION CRITERIA : ALL LOANS & OD
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
# %INC PGM(EIBMMISF) - Included in SAS source but no EIBMMISF formats are
#                      referenced in this program; omitted from import.
# %INC PGM(PBBELF)   - Branch code / region / format lookup functions

from PBBELF import format_brchcd

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# OPTIONS (SAS equivalents)
# NOCENTER NONUMBER NODATE MISSING=0 LS=132 -> handled in report formatting
# ============================================================================

MISSING_NUM = 0   # SAS MISSING=0: numeric missing displayed as 0
LINE_SIZE   = 132

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR   = Path("/data")
BNM_DIR    = BASE_DIR / "bnm"
LOAN_DIR   = BASE_DIR / "loan"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = BNM_DIR / "reptdate.parquet"
OUTPUT_REPORT = OUTPUT_DIR / "eibmrc10_report.txt"

# ============================================================================
# ASA CARRIAGE CONTROL CONSTANTS
# ============================================================================

ASA_NEWPAGE     = '1'  # Form feed / new page
ASA_SINGLESPACE = ' '  # Single space (advance 1 line before print)
ASA_DOUBLESPACE = '0'  # Double space (advance 2 lines before print)

PAGE_LENGTH = 60   # lines per page (default)

# ============================================================================
# DATA REPTDATE: derive macro variables from BNM.REPTDATE
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(
    f"SELECT * FROM read_parquet('{REPTDATE_FILE}')"
).pl()
row_rep = reptdate_df.row(0, named=True)

REPTDATE_VAL: date = row_rep["REPTDATE"]
if isinstance(REPTDATE_VAL, datetime):
    REPTDATE_VAL = REPTDATE_VAL.date()

day_of_month = REPTDATE_VAL.day
MM           = REPTDATE_VAL.month
yy           = REPTDATE_VAL.year

# SELECT(DAY(REPTDATE)) -> derive SDD, WK, WK1
if day_of_month == 8:
    SDD = 1;  WK = '1'; WK1 = '4'
elif day_of_month == 15:
    SDD = 9;  WK = '2'; WK1 = '1'
elif day_of_month == 22:
    SDD = 16; WK = '3'; WK1 = '2'
else:
    SDD = 23; WK = '4'; WK1 = '3'

MM1 = MM - 1
if MM1 == 0:
    MM1 = 12

SDATE     = date(yy, MM, SDD)
NOWK      = WK
NOWK1     = WK1
REPTMON   = f"{MM:02d}"
REPTMON1  = f"{MM1:02d}"
REPTYEAR  = f"{yy}"                            # YEAR4. -> 4-digit year
RDATE     = REPTDATE_VAL.strftime("%d/%m/%y")  # DDMMYY8.  e.g. '01/06/24'
REPTDAY   = f"{day_of_month:02d}"
SDATE_FMT = SDATE.strftime("%d/%m/%y")         # SDATE as DDMMYY8.

# ============================================================================
# SELECTION HELPER: WHERE (SUBSTR(PRODCD,1,2) EQ '34' OR PRODCD EQ '54120')
# ============================================================================

def is_selected(prodcd: str) -> bool:
    """Replicate WHERE clause: PRODCD starts with '34' or equals '54120'."""
    p = str(prodcd or "").strip()
    return p[:2] == '34' or p == '54120'


# ============================================================================
# DATA PREV&REPTMON1&NOWK
#   KEEP STATECD BRANCH BALPI BALPC OLDBAL
#   SET  LOAN.LOAN&REPTMON1&NOWK
#   WHERE (SUBSTR(PRODCD,1,2) EQ '34' OR PRODCD EQ '54120')
#   Classification:
#     ACCTYPE='LN': PRODUCT<900 -> BALPI=BALANCE, else BALCC=BALANCE
#     ACCTYPE='OD': ORGCODE IN ('1','001','100') -> BALPC=BALANCE, else BALPI=BALANCE
#   OLDBAL = SUM(BALPI, BALPC)
# ============================================================================

PREV_LOAN_FILE = LOAN_DIR / f"loan{REPTMON1}{NOWK1}.parquet"

prev_raw = con.execute(
    f"SELECT * FROM read_parquet('{PREV_LOAN_FILE}')"
).pl()


def build_prev(df: pl.DataFrame) -> pl.DataFrame:
    """Build PREV dataset (previous month/week loan snapshot)."""
    rows = []
    for r in df.to_dicts():
        prodcd  = str(r.get("PRODCD")  or "").strip()
        acctype = str(r.get("ACCTYPE") or "").strip()
        orgcode = str(r.get("ORGCODE") or "").strip()
        product = int(r.get("PRODUCT") or 0)
        balance = float(r.get("BALANCE") or 0.0)

        if not is_selected(prodcd):
            continue

        balpi = 0.0
        balpc = 0.0

        if acctype == 'LN':
            if product < 900:
                balpi = balance
            else:
                balpc = balance
        elif acctype == 'OD':
            if orgcode in ('1', '001', '100'):
                balpc = balance
            else:
                balpi = balance

        oldbal = balpi + balpc

        rows.append({
            "STATECD": str(r.get("STATECD") or "").strip(),
            "BRANCH":  r.get("BRANCH"),
            "BALPI":   balpi,
            "BALPC":   balpc,
            "OLDBAL":  oldbal,
        })
    return pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={"STATECD": pl.Utf8, "BRANCH": pl.Int64,
                "BALPI": pl.Float64, "BALPC": pl.Float64, "OLDBAL": pl.Float64}
    )


prev_ds = build_prev(prev_raw).sort("STATECD")

# ============================================================================
# DATA CURRENT
#   KEEP STATECD BRANCH BALCI BALCC CURBAL
#   SET  LOAN.LOAN&REPTMON&NOWK
#   WHERE (SUBSTR(PRODCD,1,2) EQ '34' OR PRODCD EQ '54120')
#   Classification:
#     ACCTYPE='LN': PRODUCT<900 -> BALCI=BALANCE, else BALCC=BALANCE
#     ACCTYPE='OD': ORGCODE IN ('1','001','100') -> BALCC=BALANCE, else BALCI=BALANCE
#   CURBAL = SUM(BALCI, BALCC)
# ============================================================================

CURR_LOAN_FILE = LOAN_DIR / f"loan{REPTMON}{NOWK}.parquet"

curr_raw = con.execute(
    f"SELECT * FROM read_parquet('{CURR_LOAN_FILE}')"
).pl()


def build_current(df: pl.DataFrame) -> pl.DataFrame:
    """Build CURRENT dataset (current month/week loan snapshot)."""
    rows = []
    for r in df.to_dicts():
        prodcd  = str(r.get("PRODCD")  or "").strip()
        acctype = str(r.get("ACCTYPE") or "").strip()
        orgcode = str(r.get("ORGCODE") or "").strip()
        product = int(r.get("PRODUCT") or 0)
        balance = float(r.get("BALANCE") or 0.0)

        if not is_selected(prodcd):
            continue

        balci = 0.0
        balcc = 0.0

        if acctype == 'LN':
            if product < 900:
                balci = balance
            else:
                balcc = balance
        elif acctype == 'OD':
            if orgcode in ('1', '001', '100'):
                balcc = balance
            else:
                balci = balance

        curbal = balci + balcc

        rows.append({
            "STATECD": str(r.get("STATECD") or "").strip(),
            "BRANCH":  r.get("BRANCH"),
            "BALCI":   balci,
            "BALCC":   balcc,
            "CURBAL":  curbal,
        })
    return pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={"STATECD": pl.Utf8, "BRANCH": pl.Int64,
                "BALCI": pl.Float64, "BALCC": pl.Float64, "CURBAL": pl.Float64}
    )


current_ds = build_current(curr_raw).sort("STATECD")

# ============================================================================
# DATA TEMP
#   SET PREV&REPTMON1&NOWK CURRENT
#   BY STATECD
# Merge previous and current into a single dataset aligned on STATECD/BRANCH
# for PROC SUMMARY aggregation. Rows from both datasets are concatenated;
# missing columns from each side are filled with 0 (MISSING=0).
# ============================================================================

# Align schemas: add missing columns as 0.0 before concat
def align_df(df: pl.DataFrame, cols: list) -> pl.DataFrame:
    """Add missing numeric columns (as 0.0) so both datasets share the same schema."""
    for col in cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0.0).alias(col))
    return df


ALL_VARS = ["CURBAL", "OLDBAL", "BALPI", "BALPC", "BALCI", "BALCC"]

prev_aligned    = align_df(prev_ds,    ALL_VARS)
current_aligned = align_df(current_ds, ALL_VARS)

temp = pl.concat([prev_aligned, current_aligned], how="diagonal")

# ============================================================================
# PROC SUMMARY DATA=TEMP NWAY: CLASS STATECD BRANCH; VAR ...; SUM= -> STATE
# ============================================================================

if not temp.is_empty():
    state_df = (
        temp.group_by(["STATECD", "BRANCH"])
            .agg([pl.col(v).sum() for v in ALL_VARS])
            .sort(["STATECD", "BRANCH"])
    )
else:
    state_df = pl.DataFrame()

# ============================================================================
# PROC SUMMARY DATA=TEMP NWAY: CLASS BRANCH; VAR ...; SUM= -> BRANCH
# ============================================================================

if not temp.is_empty():
    branch_df = (
        temp.group_by("BRANCH")
            .agg([pl.col(v).sum() for v in ALL_VARS])
            .sort("BRANCH")
    )
else:
    branch_df = pl.DataFrame()

# ============================================================================
# DATA REPT010A (from STATE summary)
#   DIFFBAL = SUM(CURBAL, (-1)*OLDBAL)
#   DIFFI   = SUM(BALCI,  (-1)*BALPI)
#   DIFFC   = SUM(BALCC,  (-1)*BALPC)
# ============================================================================

if not state_df.is_empty():
    rept010a = state_df.with_columns([
        (pl.col("CURBAL") - pl.col("OLDBAL")).alias("DIFFBAL"),
        (pl.col("BALCI")  - pl.col("BALPI")).alias("DIFFI"),
        (pl.col("BALCC")  - pl.col("BALPC")).alias("DIFFC"),
    ])
else:
    rept010a = pl.DataFrame()

# ============================================================================
# DATA REPT010B (from BRANCH summary)
#   BRCH    = PUT(BRANCH, BRCHCD.)   -> format_brchcd() from PBBELF
#   DIFFBAL = SUM(CURBAL, (-1)*OLDBAL)
#   DIFFI   = SUM(BALCI,  (-1)*BALPI)
#   DIFFC   = SUM(BALCC,  (-1)*BALPC)
# PROC SORT DATA=REPT010B BY BRCH
# ============================================================================

if not branch_df.is_empty():
    rept010b_rows = []
    for r in branch_df.to_dicts():
        branch  = r.get("BRANCH")
        brch    = format_brchcd(int(branch) if branch is not None else 0)
        curbal  = float(r.get("CURBAL") or 0.0)
        oldbal  = float(r.get("OLDBAL") or 0.0)
        balci   = float(r.get("BALCI")  or 0.0)
        balpi   = float(r.get("BALPI")  or 0.0)
        balcc   = float(r.get("BALCC")  or 0.0)
        balpc   = float(r.get("BALPC")  or 0.0)
        rept010b_rows.append({
            "BRANCH":  branch,
            "BRCH":    brch,
            "CURBAL":  curbal,
            "OLDBAL":  oldbal,
            "BALCI":   balci,
            "BALPI":   balpi,
            "BALCC":   balcc,
            "BALPC":   balpc,
            "DIFFBAL": curbal - oldbal,
            "DIFFI":   balci  - balpi,
            "DIFFC":   balcc  - balpc,
        })
    rept010b = pl.DataFrame(rept010b_rows).sort("BRCH")
else:
    rept010b = pl.DataFrame()

# ============================================================================
# PROC TABULATE DATA=REPT010B NOSEPS FORMAT=COMMA17.2
#   CLASS BRCH
#   VAR   CURBAL DIFFBAL BALCI DIFFI BALCC DIFFC
#   FORMAT BRANCH BRCHCD.
#   TABLE BRCH ALL,
#         CURBAL='TOTAL OUTSTANDING LOANS & ADVANCES'
#         DIFFBAL='INC/(DEC) FOR CURRENT MONTH'
#         BALCI='RETAIL LOANS OUTSTANDING'
#         DIFFI='INC/(DEC) FOR CURRENT MONTH'
#         BALCC='CORPORATE LOANS OUTSTANDING'
#         DIFFC='INC/(DEC) FOR CURRENT MONTH'
#         / RTS=20 BOX=_PAGE_
#   KEYLABEL ALL='TOTAL' SUM=' '
#   TITLE1 'RETAIL CREDIT DIVISION'
#   TITLE2 '010 PROGRESS REPORT ON RETAIL AND CORPORATE LOANS & ADVANCES'
#   TITLE3 'OVERALL TOTAL BY BRANCHES'
#   TITLE4 'REPORTING DATE AS AT : ' &RDATE
# ============================================================================

# ============================================================================
# REPORT GENERATION HELPERS
# ============================================================================

TITLE1_STR = "RETAIL CREDIT DIVISION"
TITLE2_STR = "010 PROGRESS REPORT ON RETAIL AND CORPORATE LOANS & ADVANCES"
TITLE3_STR = "OVERALL TOTAL BY BRANCHES"
TITLE4_STR = f"REPORTING DATE AS AT : {RDATE}"

RTS_W  = 20    # RTS=20 -> row label width
AMT_W  = 17    # COMMA17.2

# Column definitions matching TABLE statement order
COLUMNS = [
    ("CURBAL",  "TOTAL OUTSTANDING LOANS & ADVANCES"),
    ("DIFFBAL", "INC/(DEC) FOR CURRENT MONTH"),
    ("BALCI",   "RETAIL LOANS OUTSTANDING"),
    ("DIFFI",   "INC/(DEC) FOR CURRENT MONTH"),
    ("BALCC",   "CORPORATE LOANS OUTSTANDING"),
    ("DIFFC",   "INC/(DEC) FOR CURRENT MONTH"),
]


def fmt_comma(val, width: int = AMT_W, dec: int = 2) -> str:
    """Format number as COMMA17.2 (with commas, 2 decimal places)."""
    try:
        f = float(val if val is not None else MISSING_NUM)
        return f"{f:,.{dec}f}".rjust(width)
    except (TypeError, ValueError):
        return str(MISSING_NUM).rjust(width)


def build_report() -> list:
    """Build the full tabulate report with ASA carriage control."""
    lines = []

    # Page header
    lines.append(f"{ASA_NEWPAGE}{TITLE1_STR}")
    lines.append(f"{ASA_SINGLESPACE}{TITLE2_STR}")
    lines.append(f"{ASA_SINGLESPACE}{TITLE3_STR}")
    lines.append(f"{ASA_SINGLESPACE}{TITLE4_STR}")
    lines.append(f"{ASA_DOUBLESPACE}")

    # Build column header rows
    # Header line 1: column labels (wrapped to fit AMT_W each)
    hdr1 = f"{ASA_SINGLESPACE}{'':>{RTS_W}}"
    for _, label in COLUMNS:
        hdr1 += f"  {label:>{AMT_W}}"
    lines.append(hdr1)

    sep_len = RTS_W + (AMT_W + 2) * len(COLUMNS)
    lines.append(f"{ASA_SINGLESPACE}{'-' * sep_len}")

    if rept010b.is_empty():
        lines.append(f"{ASA_SINGLESPACE}NO DATA")
        return lines

    # Aggregation accumulators for TOTAL row (KEYLABEL ALL='TOTAL')
    totals: dict = {col: 0.0 for col, _ in COLUMNS}

    for r in rept010b.to_dicts():
        brch   = str(r.get("BRCH") or "").strip()
        row_str = f"{ASA_SINGLESPACE}{brch:<{RTS_W}}"
        for col, _ in COLUMNS:
            val = float(r.get(col) or 0.0)
            row_str    += f"  {fmt_comma(val)}"
            totals[col] += val
        lines.append(row_str)

    # ALL='TOTAL' row (KEYLABEL ALL='TOTAL')
    lines.append(f"{ASA_SINGLESPACE}{'-' * sep_len}")
    total_str = f"{ASA_SINGLESPACE}{'TOTAL':<{RTS_W}}"
    for col, _ in COLUMNS:
        total_str += f"  {fmt_comma(totals[col])}"
    lines.append(total_str)

    return lines


# ============================================================================
# WRITE REPORT OUTPUT
# ============================================================================

report_lines = build_report()

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
