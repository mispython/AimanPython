#!/usr/bin/env python3
"""
Program  : EIBMRC07.py
Purpose  : Loans & Advances by Loan Size on Approved Limit.
           USER      : RETAIL CREDIT DIVISION
           OBJECTIVE : LOANS & ADVANCES BY LOAN SIZE ON APPROVED LIMIT
           FREQUENCY : MONTHLY
           SELECTION CRITERIA : ALL LOANS & OD BY FACILITY TYPE
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
from EIBMMISF import (
    fmt_lnfacil,
    fmt_loanrnge,
    fmt_range,
)

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR       = Path("/data")
BNM_DIR        = BASE_DIR / "bnm"
LOAN_DIR       = BASE_DIR / "loan"

REPTDATE_FILE  = BNM_DIR  / "reptdate.parquet"
SDESC_FILE     = LOAN_DIR / "sdesc.parquet"

OUTPUT_DIR     = BASE_DIR / "output"
OUTPUT_REPORT  = OUTPUT_DIR / "eibmrc07_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT CONSTANTS
# ============================================================================

PAGE_LENGTH = 60
LINE_WIDTH  = 132

# Columns kept per %LET LOAN macro
LOAN_KEEP = [
    "BRANCH", "NAME", "STATECD", "PRODUCT", "SECTORCD", "SPTF",
    "ACCTYPE", "LIABCODE", "APPRLIMT", "FACTYPE", "LNSIZE",
    "BALANCE", "RLEASAMT", "UNDRAWN", "ACCTNO", "PRODCD",
]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_ddmmyy10(d: date) -> str:
    """Format date as DD/MM/YYYY (SAS DDMMYY10.)"""
    return d.strftime("%d/%m/%Y")


def fmt_z2(n: int) -> str:
    return f"{n:02d}"


def fmt_comma10(val) -> str:
    if val is None:
        return " " * 10
    try:
        return f"{int(val):,}".rjust(10)
    except (TypeError, ValueError):
        return " " * 10


def fmt_comma20_2(val) -> str:
    if val is None:
        return " " * 20
    try:
        return f"{float(val):,.2f}".rjust(20)
    except (TypeError, ValueError):
        return " " * 20


# Ordered loan size bands A-L for consistent sort
LNSIZE_ORDER = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"]

# ============================================================================
# STEP 1: READ REPTDATE & DERIVE MACRO VARIABLES
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(f"SELECT * FROM read_parquet('{REPTDATE_FILE}')").pl()
row = reptdate_df.row(0, named=True)

REPTDATE: date = row["REPTDATE"]
if isinstance(REPTDATE, datetime):
    REPTDATE = REPTDATE.date()

NOWK    = "4"
RDATE   = fmt_ddmmyy10(REPTDATE)
REPTMON = fmt_z2(REPTDATE.month)

# ============================================================================
# STEP 2: READ SDESC
# ============================================================================

sdesc_df = con.execute(f"SELECT * FROM read_parquet('{SDESC_FILE}')").pl()
SDESC = str(sdesc_df["SDESC"][0]).ljust(26)[:26]

# ============================================================================
# STEP 3: LOAD LOAN + ULOAN, COMBINE AND FILTER
# ============================================================================

loan_file  = LOAN_DIR / f"loan{REPTMON}{NOWK}.parquet"
uloan_file = LOAN_DIR / f"uloan{REPTMON}{NOWK}.parquet"

loan_df  = con.execute(f"SELECT * FROM read_parquet('{loan_file}')").pl()
uloan_df = con.execute(f"SELECT * FROM read_parquet('{uloan_file}')").pl()

# Align columns before concat
for col in ["SPTF", "FACTYPE", "LNSIZE"]:
    for df in [loan_df, uloan_df]:
        if col not in df.columns:
            pass  # will be added after concat

combined = pl.concat([loan_df, uloan_df], how="diagonal")

# Filter: PRODCD prefix '34' OR PRODCD = '54120'
combined = combined.filter(
    (pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 2) == "34") |
    (pl.col("PRODCD").cast(pl.Utf8) == "54120")
)

# ============================================================================
# STEP 4: APPLY FACTYPE, SPTF, LNSIZE LOGIC
# ============================================================================

def apply_facil_logic(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        acctype = str(r.get("ACCTYPE") or "")
        prodcd  = str(r.get("PRODCD")  or "").strip()
        product = int(r.get("PRODUCT") or 0)
        apprlimt = float(r.get("APPRLIMT") or 0.0)

        sptf    = r.get("SPTF") or ""
        factype = r.get("FACTYPE") or ""

        if acctype == "OD":
            if prodcd in ("34180", "34240"):
                factype = "OD"
            if 160 <= product <= 164:
                sptf = "SPTF"
        else:
            if (100 <= product <= 103) or (110 <= product <= 116) or \
               product in (120, 127, 170):
                sptf = "SPTF"
            factype = fmt_lnfacil(prodcd)

        r["FACTYPE"] = factype
        r["SPTF"]    = sptf
        r["LNSIZE"]  = fmt_loanrnge(apprlimt)
        out.append(r)
    return out


facil_rows = apply_facil_logic(combined.to_dicts())
facil_df   = pl.DataFrame(facil_rows)

# PROC SORT: BY PRODCD PRODUCT
facil_df = facil_df.sort(["PRODCD", "PRODUCT"])

# ============================================================================
# STEP 5: REPORT RENDERING HELPERS
# ============================================================================

def build_col_header() -> tuple[str, str]:
    col = (
        f"{'FACILITY TYPE / LOAN QUANTUM':<30}  "
        f"{'NO OF ACCOUNTS':>10}  "
        f"{'APPROVED LIMIT':>20}  "
        f"{'OUTSTANDING BALANCE':>20}  "
        f"{'UNDRAWN AMOUNT':>20}"
    )
    sep = "-" * LINE_WIDTH
    return col, sep


def render_tabulate(df: pl.DataFrame, class_col: str, class_label: str,
                    titles: list[str], lines: list,
                    include_total: bool = True) -> None:
    """
    Replicate PROC TABULATE:
    TABLE class_col = class_label ALL, LNSIZE = 'LOAN QUANTUM' ALL,
          N APPRLIMT BALANCE UNDRAWN
    """
    col_header, separator = build_col_header()
    line_count = PAGE_LENGTH  # force new page on first use

    def new_page() -> None:
        nonlocal line_count
        lines.append(f"1{titles[0].center(LINE_WIDTH)}")
        for t in titles[1:]:
            lines.append(f" {t.center(LINE_WIDTH)}")
        lines.append(f" ")
        lines.append(f" {col_header}")
        lines.append(f" {separator}")
        line_count = 4 + len(titles)

    # Only rows where class_col is not null/empty
    subset = df.filter(pl.col(class_col).is_not_null() & (pl.col(class_col) != ""))
    if subset.is_empty():
        return

    grand_n   = 0
    grand_lmt = 0.0
    grand_bal = 0.0
    grand_und = 0.0

    for class_val in sorted(subset[class_col].unique().to_list()):
        class_grp = subset.filter(pl.col(class_col) == class_val)

        class_n   = 0
        class_lmt = 0.0
        class_bal = 0.0
        class_und = 0.0

        # Iterate loan size bands in defined order
        for band in LNSIZE_ORDER:
            band_grp = class_grp.filter(pl.col("LNSIZE") == band)
            if band_grp.is_empty():
                continue

            n   = len(band_grp)
            lmt = float(band_grp["APPRLIMT"].sum() or 0)
            bal = float(band_grp["BALANCE"].sum()   or 0)
            und = float(band_grp["UNDRAWN"].sum()   or 0)

            if line_count >= PAGE_LENGTH - 4:
                new_page()

            band_label = fmt_range(band)
            label = f"  {class_val} / {band_label}"
            lines.append(
                f" {label:<30}  "
                f"{fmt_comma10(n)}  "
                f"{fmt_comma20_2(lmt)}  "
                f"{fmt_comma20_2(bal)}  "
                f"{fmt_comma20_2(und)}"
            )
            line_count += 1
            class_n   += n
            class_lmt += lmt
            class_bal += bal
            class_und += und

        # ALL (total for this class value across all loan sizes)
        if line_count >= PAGE_LENGTH - 4:
            new_page()
        lines.append(
            f" {'  TOTAL':<30}  "
            f"{fmt_comma10(class_n)}  "
            f"{fmt_comma20_2(class_lmt)}  "
            f"{fmt_comma20_2(class_bal)}  "
            f"{fmt_comma20_2(class_und)}"
        )
        line_count += 1
        grand_n   += class_n
        grand_lmt += class_lmt
        grand_bal += class_bal
        grand_und += class_und

    # Grand TOTAL row
    if include_total:
        if line_count >= PAGE_LENGTH - 4:
            new_page()
        lines.append(f" {separator}")
        lines.append(
            f" {'TOTAL':<30}  "
            f"{fmt_comma10(grand_n)}  "
            f"{fmt_comma20_2(grand_lmt)}  "
            f"{fmt_comma20_2(grand_bal)}  "
            f"{fmt_comma20_2(grand_und)}"
        )
        line_count += 2


# ============================================================================
# STEP 6: PRODUCE REPORTS
# ============================================================================

report_lines: list[str] = []

# --- Report 1: BY FACTYPE ---
render_tabulate(
    df=facil_df,
    class_col="FACTYPE",
    class_label="FACILITY TYPE : ",
    titles=[
        f"{SDESC.strip()}  - RETAIL CREDIT DIVISION   ATTN : MS KHOO",
        f"007  LOANS AND ADVANCES BY LOAN SIZE ON APPROVED LIMIT AS AT {RDATE}",
    ],
    lines=report_lines,
    include_total=True,
)

# --- Report 2: BY SPTF ---
render_tabulate(
    df=facil_df,
    class_col="SPTF",
    class_label="FACILITY TYPE : ",
    titles=[
        f"{SDESC.strip()} - RETAIL CREDIT DIVISION   ATTN : MS KHOO",
        f"007  LOANS AND ADVANCES BY LOAN SIZE ON APPROVED LIMIT AS AT {RDATE}",
    ],
    lines=report_lines,
    include_total=False,  # SAS PROC TABULATE for SPTF has no ALL= on outer dimension
)

# ============================================================================
# STEP 7: WRITE REPORT TO FILE
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
