#!/usr/bin/env python3
"""
Program  : EIBMCUST.py
Date     : 12.09.01
Purpose  : Listing of Accounts (OD & Loans) for Specific Customer Codes
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
OUTPUT_REPORT = OUTPUT_DIR / "eibmcust_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT / PAGE CONSTANTS
# ============================================================================

PAGE_LENGTH = 60
LINE_WIDTH  = 132

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_ddmmyy8(d: date) -> str:
    """Format date as DD/MM/YY (SAS DDMMYY8.)"""
    return d.strftime("%d/%m/%y")


def fmt_z2(n: int) -> str:
    return f"{n:02d}"


def fmt_comma15_2(val) -> str:
    if val is None:
        return " " * 15
    try:
        return f"{float(val):,.2f}".rjust(15)
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

day = REPTDATE.day
if day == 8:
    SDD, WK, WK1 = 1, "1", "4"
elif day == 15:
    SDD, WK, WK1 = 9, "2", "1"
elif day == 22:
    SDD, WK, WK1 = 16, "3", "2"
else:
    SDD, WK, WK1 = 23, "4", "3"

MM = REPTDATE.month
if WK == "1":
    MM1 = MM - 1 if MM > 1 else 12
else:
    MM1 = MM

NOWK    = WK
NOWK1   = WK1
REPTMON = fmt_z2(MM)
RDATE   = fmt_ddmmyy8(REPTDATE)

# ============================================================================
# STEP 2: READ SDESC
# ============================================================================

sdesc_df = con.execute(f"SELECT * FROM read_parquet('{SDESC_FILE}')").pl()
SDESC = str(sdesc_df["SDESC"][0]).ljust(26)[:26]

# ============================================================================
# STEP 3: LOAD LOAN DATA, FILTER BY CUSTCD, SPLIT INTO OD / LOAN
# ============================================================================

loan_file = BNM_DIR / f"loan{REPTMON}{NOWK}.parquet"

raw_query = f"SELECT * FROM read_parquet('{loan_file}')"
raw = con.execute(raw_query).pl()

# Filter: CUSTCD IN ('33','10','02','03','11','12','81','82','83','84')
custcd_codes = {"33", "10", "02", "03", "11", "12", "81", "82", "83", "84"}
raw = raw.filter(pl.col("CUSTCD").cast(pl.Utf8).is_in(custcd_codes))


def split_od_loan(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    od_rows   = []
    loan_rows = []

    for r in rows:
        acctype = str(r.get("ACCTYPE") or "")
        col1    = str(r.get("COL1")    or "")

        if acctype == "OD":
            od_rows.append({
                "BRANCH":  r.get("BRANCH"),
                "CUSTCD":  r.get("CUSTCD"),
                "ACCTNO":  r.get("ACCTNO"),
                "NAME":    r.get("NAME"),
                "BALANCE": r.get("CURBAL"),   # BALANCE=CURBAL for OD
                "LIAB1":   col1[0:2] if len(col1) >= 2 else col1.ljust(2)[:2],
                "LIAB2":   col1[3:5] if len(col1) >= 5 else "",
            })
        elif acctype == "LN":
            loan_rows.append({
                "BRANCH":   r.get("BRANCH"),
                "CUSTCD":   r.get("CUSTCD"),
                "ACCTNO":   r.get("ACCTNO"),
                "NAME":     r.get("NAME"),
                "NOTENO":   r.get("NOTENO"),
                "BALANCE":  r.get("BALANCE"),
                "LIABCODE": r.get("LIABCODE"),
            })

    return od_rows, loan_rows


od_rows, loan_rows = split_od_loan(raw.to_dicts())

od_df   = pl.DataFrame(od_rows)   if od_rows   else pl.DataFrame()
loan_df = pl.DataFrame(loan_rows) if loan_rows else pl.DataFrame()

# ============================================================================
# STEP 4: SORT
# ============================================================================

if not od_df.is_empty():
    od_df = od_df.sort(["BRANCH", "CUSTCD", "ACCTNO"])

if not loan_df.is_empty():
    loan_df = loan_df.sort(["BRANCH", "CUSTCD", "ACCTNO"])

# ============================================================================
# STEP 5: REPORT RENDERING
# ============================================================================

report_titles = [
    "REPORT ID: EIBMCUST",
    SDESC.strip(),
    f"LISTING OF ACCOUNTS WITH CUSTOMER CODES 2,3,10-12,33,81-84  DATE : {RDATE}",
]


def write_report(
    df: pl.DataFrame,
    by_cols: list[str],
    var_cols: list[str],
    col_labels: dict[str, str],
    title4: str,
    lines: list[str],
    sum_col: str = "BALANCE",
    sumby_col: str = "BRANCH",
) -> None:
    """
    Replicate PROC PRINT with BY, SUM, SUMBY, PAGEBY BRANCH.
    PAGEBY BRANCH = new page at each new BRANCH value.
    ASA carriage control: '1' = form-feed, ' ' = single space.
    """

    def col_header() -> str:
        return "  ".join(col_labels.get(c, c).ljust(17) for c in var_cols)

    separator = "-" * LINE_WIDTH

    def new_page(by_vals: dict) -> list[str]:
        pg = []
        pg.append(f"1{report_titles[0].center(LINE_WIDTH)}")
        for t in report_titles[1:]:
            pg.append(f" {t.center(LINE_WIDTH)}")
        pg.append(f" {title4.center(LINE_WIDTH)}")
        pg.append(f" ")
        by_header = "  ".join(
            f"{col_labels.get(k, k)}={by_vals.get(k, '')}" for k in by_cols
        )
        pg.append(f" {by_header}")
        pg.append(f" {separator}")
        pg.append(f" {col_header()}")
        pg.append(f" {separator}")
        return pg

    if df.is_empty():
        return

    # PAGEBY BRANCH: new page for each branch
    for branch_val in sorted(df[by_cols[0]].unique().to_list()):
        branch_grp  = df.filter(pl.col(by_cols[0]) == branch_val)
        branch_total = 0.0

        # New page per BRANCH (PAGEBY BRANCH)
        lines.extend(new_page({by_cols[0]: branch_val}))
        line_count = 8 + len(report_titles)

        for custcd_val in sorted(branch_grp[by_cols[1]].unique().to_list()):
            grp = branch_grp.filter(pl.col(by_cols[1]) == custcd_val)

            if line_count >= PAGE_LENGTH - 4:
                lines.extend(new_page({by_cols[0]: branch_val, by_cols[1]: custcd_val}))
                line_count = 8 + len(report_titles)
            else:
                cust_header = (
                    f"  {col_labels.get(by_cols[1], by_cols[1])}={custcd_val}"
                )
                lines.append(f" {cust_header}")
                lines.append(f" {separator}")
                line_count += 2

            for data_row in grp.iter_rows(named=True):
                if line_count >= PAGE_LENGTH - 4:
                    lines.extend(
                        new_page({by_cols[0]: branch_val, by_cols[1]: custcd_val})
                    )
                    line_count = 8 + len(report_titles)

                cells = []
                for c in var_cols:
                    val = data_row.get(c)
                    if c == sum_col:
                        cells.append(fmt_comma15_2(val).rjust(17))
                    else:
                        cells.append(str(val or "").ljust(17))
                lines.append(f" {'  '.join(cells)}")
                line_count += 1
                branch_total += float(data_row.get(sum_col) or 0)

        # SUMBY BRANCH: subtotal per branch
        lines.append(f" {separator}")
        lines.append(
            f" {'BRANCH SUBTOTAL':<30}"
            f"{fmt_comma15_2(branch_total):>15}"
        )
        lines.append(f" {separator}")
        line_count += 3


report_lines: list[str] = []

# --- OD REPORT ---
if not od_df.is_empty():
    write_report(
        df=od_df,
        by_cols=["BRANCH", "CUSTCD"],
        var_cols=["ACCTNO", "NAME", "BALANCE", "LIAB1", "LIAB2"],
        col_labels={
            "BRANCH":  "BRANCH NO",
            "CUSTCD":  "CUSTOMER CODE",
            "ACCTNO":  "ACCOUNT NO",
            "NAME":    "CUSTOMER NAME",
            "LIAB1":   "COLL CODE 1",
            "LIAB2":   "COLL CODE 2",
            "BALANCE": "OUTSTANDING BALANCE",
        },
        title4="OD ACCOUNTS",
        lines=report_lines,
        sum_col="BALANCE",
        sumby_col="BRANCH",
    )

# --- LOAN REPORT ---
if not loan_df.is_empty():
    write_report(
        df=loan_df,
        by_cols=["BRANCH", "CUSTCD"],
        var_cols=["ACCTNO", "NAME", "NOTENO", "BALANCE", "LIABCODE"],
        col_labels={
            "BRANCH":   "BRANCH NO",
            "CUSTCD":   "CUSTOMER  CODE",
            "ACCTNO":   "ACCOUNT NO",
            "NAME":     "CUSTOMER NAME",
            "NOTENO":   "NOTE NO",
            "LIABCODE": "COLL CODE",
            "BALANCE":  "OUTSTANDING BALANCE",
        },
        title4="LOAN ACCOUNTS",
        lines=report_lines,
        sum_col="BALANCE",
        sumby_col="BRANCH",
    )

# ============================================================================
# STEP 6: WRITE REPORT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
