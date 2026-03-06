#!/usr/bin/env python3
"""
Program  : EIBMHEXT.py
Date     : 17.09.01
Purpose  : Listing of Undrawn Housing Loans
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = Path("/data")
BNM_DIR       = BASE_DIR / "bnm"

REPTDATE_FILE = BNM_DIR / "reptdate.parquet"
SDESC_FILE    = BNM_DIR / "sdesc.parquet"

OUTPUT_DIR    = BASE_DIR / "output"
OUTPUT_REPORT = OUTPUT_DIR / "eibmhext_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT / PAGE CONSTANTS
# ============================================================================

PAGE_LENGTH = 60
LINE_WIDTH  = 132

# Housing loan PRODUCT codes eligible for this report
HOUSING_PRODUCTS = {
    110, 111, 112, 113, 114, 115, 116,
    200, 201, 204, 205,
    209, 210, 211, 212, 214, 215,
    219, 220, 225, 226,
    227, 228, 229, 230, 231, 232, 233, 234,
}

# Excluded PRODCDs (revolving / staff / islamic)
EXCLUDED_PRODCDS = {"34190", "34230", "54120"}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_worddatx18(d: date) -> str:
    """
    Format date as SAS WORDDATX18. e.g. '17 September 2001'
    (day month-name year, no leading zero on day)
    """
    return d.strftime("%-d %B %Y")


def fmt_z2(n: int) -> str:
    return f"{n:02d}"


def fmt_comma17_2(val) -> str:
    if val is None:
        return " " * 17
    try:
        return f"{float(val):,.2f}".rjust(17)
    except (TypeError, ValueError):
        return " " * 17


# ============================================================================
# STEP 1: READ REPTDATE & DERIVE MACRO VARIABLES
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(f"SELECT * FROM read_parquet('{REPTDATE_FILE}')").pl()
row_rep = reptdate_df.row(0, named=True)

REPTDATE: date = row_rep["REPTDATE"]
if isinstance(REPTDATE, datetime):
    REPTDATE = REPTDATE.date()

day = REPTDATE.day
if day == 8:
    NOWK = "1"
elif day == 15:
    NOWK = "2"
elif day == 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTMON = fmt_z2(REPTDATE.month)
RDATE   = fmt_worddatx18(REPTDATE)

# ============================================================================
# STEP 2: READ SDESC
# ============================================================================

sdesc_df = con.execute(f"SELECT * FROM read_parquet('{SDESC_FILE}')").pl()
SDESC = str(sdesc_df["SDESC"][0]).ljust(26)[:26]

# ============================================================================
# STEP 3: LOAD LOAN + ULOAN, FILTER, COMPUTE REMMTH AND UTL
# ============================================================================

loan_file  = BNM_DIR / f"loan{REPTMON}{NOWK}.parquet"
uloan_file = BNM_DIR / f"uloan{REPTMON}{NOWK}.parquet"

loan_raw  = con.execute(f"SELECT * FROM read_parquet('{loan_file}')").pl()
uloan_raw = con.execute(f"SELECT * FROM read_parquet('{uloan_file}')").pl()

# Combine LOAN + ULOAN
combined = pl.concat([loan_raw, uloan_raw], how="diagonal")

# Filter: ACCTYPE = 'LN'
combined = combined.filter(pl.col("ACCTYPE") == "LN")

# IF SUBSTR(PRODCD,1,2) = '34'
combined = combined.filter(
    pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 2) == "34"
)

# Exclude specific PRODCDs
combined = combined.filter(
    ~pl.col("PRODCD").cast(pl.Utf8).is_in(EXCLUDED_PRODCDS)
)

# Filter: PRODUCT IN housing product set
combined = combined.filter(pl.col("PRODUCT").is_in(HOUSING_PRODUCTS))

# REMMTH: IF ORIGMT < '20' THEN REMMTH = 1; ELSE REMMTH = 13
combined = combined.with_columns(
    pl.when(pl.col("ORIGMT").cast(pl.Utf8) < "20")
      .then(pl.lit(1))
      .otherwise(pl.lit(13))
      .alias("REMMTH")
)

# UTL = UNDRAWN; filter UTL NE 0
combined = combined.with_columns(
    pl.col("UNDRAWN").alias("UTL")
)
combined = combined.filter(pl.col("UTL") != 0)

# Keep required columns
keep_cols = [c for c in [
    "ACCTYPE", "BRANCH", "ACCTNO", "NAME", "NOTENO", "PRODUCT",
    "REMMTH", "UTL", "LIABCODE", "BALANCE",
] if c in combined.columns]
loan_df = combined.select(keep_cols)

# ============================================================================
# STEP 4: PROC SUMMARY BY REMMTH + BRANCH
# ============================================================================

remmth_branch = (
    loan_df.group_by(["REMMTH", "BRANCH"])
           .agg(pl.col("UTL").sum())
           .sort(["REMMTH", "BRANCH"])
)

# ============================================================================
# STEP 5: PROC SUMMARY BY BRANCH ONLY (total)
# ============================================================================

branch_total = (
    loan_df.group_by("BRANCH")
           .agg(pl.col("UTL").sum())
           .sort("BRANCH")
)

# ============================================================================
# STEP 6: REPORT RENDERING
# ============================================================================

report_titles = [
    "REPORT ID: EIBMHEXT",
    SDESC.strip(),
    f"LISTING OF UNDRAWN HOUSING LOANS  {RDATE}",
]


def write_section(
    df: pl.DataFrame,
    title4: str,
    lines: list[str],
    line_count_ref: list[int],
) -> None:
    """
    Replicate PROC PRINT LABEL NOOBS with SUM UTL.
    Uses NOOBS so no observation number is printed.
    ASA '1' = form-feed, ' ' = advance one line.
    """
    col_header = f"{'BRANCH':<15}  {'UNDRAWN HOUSING LOAN':>17}"
    separator  = "-" * LINE_WIDTH

    def new_page() -> None:
        lines.append(f"1{report_titles[0].center(LINE_WIDTH)}")
        for t in report_titles[1:]:
            lines.append(f" {t.center(LINE_WIDTH)}")
        lines.append(f" {title4.center(LINE_WIDTH)}")
        lines.append(f" ")
        lines.append(f" {col_header}")
        lines.append(f" {separator}")
        line_count_ref[0] = 6 + len(report_titles)

    if df.is_empty():
        return

    new_page()
    total_utl = 0.0

    for data_row in df.iter_rows(named=True):
        if line_count_ref[0] >= PAGE_LENGTH - 4:
            new_page()

        branch = str(data_row.get("BRANCH") or "").ljust(15)
        utl    = float(data_row.get("UTL") or 0)
        lines.append(f" {branch}  {fmt_comma17_2(utl)}")
        line_count_ref[0] += 1
        total_utl += utl

    # SUM line
    lines.append(f" {separator}")
    lines.append(f" {'TOTAL':<15}  {fmt_comma17_2(total_utl)}")
    line_count_ref[0] += 2


report_lines:  list[str] = []
line_count_ref = [0]

# --- Section 1: REMMTH = 1 (Original Maturity UP TO 1 Year) ---
remmth1 = remmth_branch.filter(pl.col("REMMTH") == 1).select(["BRANCH", "UTL"])
write_section(
    df=remmth1,
    title4="ORIGINAL MATURITY OF UP TO 1 YEAR",
    lines=report_lines,
    line_count_ref=line_count_ref,
)

# --- Section 2: REMMTH != 1 (Original Maturity OVER 1 Year) ---
remmth_over = remmth_branch.filter(pl.col("REMMTH") != 1).select(["BRANCH", "UTL"])
write_section(
    df=remmth_over,
    title4="ORIGINAL MATURITY OF OVER 1 YEAR",
    lines=report_lines,
    line_count_ref=line_count_ref,
)

# --- Section 3: TOTAL BY BRANCH ---
write_section(
    df=branch_total,
    title4="TOTAL BY BRANCH",
    lines=report_lines,
    line_count_ref=line_count_ref,
)

# ============================================================================
# STEP 7: WRITE REPORT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
