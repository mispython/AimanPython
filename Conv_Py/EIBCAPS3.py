# !/usr/bin/env python3
"""
Program: EIBCAPS3
Purpose: Generate PBB(Staff) Movement of CAP by Category report
"""

import duckdb
import polars as pl
import os
from datetime import date, timedelta

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR   = r"C:\data"
LOAN_DIR   = os.path.join(BASE_DIR, "loan")
NPL_DIR    = os.path.join(BASE_DIR, "npl")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT1_PATH = os.path.join(OUTPUT_DIR, "EIBCAPS3.txt")

con = duckdb.connect()

# ─────────────────────────────────────────────
# REPTDATE – derive macro variables
# ─────────────────────────────────────────────
reptdate_df = con.execute(
    f"SELECT REPTDATE FROM read_parquet('{LOAN_DIR}/reptdate.parquet') LIMIT 1"
).pl()

reptdate_val: date = reptdate_df["REPTDATE"][0]
day_val   = reptdate_val.day
month_val = reptdate_val.month
year_val  = reptdate_val.year

if   1  <= day_val <=  8:  wk = "1"
elif 9  <= day_val <= 15:  wk = "2"
elif 16 <= day_val <= 22:  wk = "3"
else:                       wk = "4"

REPTMON  = f"{month_val:02d}"
NOWK     = wk
REPTYEAR = f"{year_val % 100:02d}"
REPTDAY  = f"{day_val:02d}"
DATE_STR = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

# TBL3A = PBB(STAFF) MOVEMENT OF CAP BY CATEGORY AS AT
TBL3A = "PBB(STAFF) MOVEMENT OF CAP BY CATEGORY AS AT"

# ─────────────────────────────────────────────
# Load NPL.CAP_STAFF{REPTMON}{REPTYEAR}
# Dependency: EIBCAPS2.py → produces npl/cap_staff{REPTMON}{REPTYEAR}.parquet
# ─────────────────────────────────────────────
cap_staff_path = os.path.join(NPL_DIR, f"cap_staff{REPTMON}{REPTYEAR}.parquet")
cap_df = con.execute(f"SELECT * FROM read_parquet('{cap_staff_path}')").pl()

# ─────────────────────────────────────────────
# Assign sort NO by CATEGORY
# ─────────────────────────────────────────────
CATEGORY_ORDER = {
    "CURRENT":                1,
    "1-2 MTHS":               2,
    "3-5 MTHS":               3,
    "6-11 MTHS":              4,
    ">=12 MTHS":              5,
    "IRREGULAR":              6,
    "REPOSSESSED <12 MTHS":   7,
    "REPOSSESSED >=12 MTHS":  8,
    "DEFICIT":                9,
}

bycat_staff_df = cap_df.with_columns(
    pl.col("CATEGORY").map_elements(
        lambda c: CATEGORY_ORDER.get(c, 99), return_dtype=pl.Int64
    ).alias("NO")
).sort("NO")

# ─────────────────────────────────────────────
# GENERATE SUMMARY REPORT – BY CATEGORY / BRANCH
# TBL3A = PBB(STAFF) MOVEMENT OF CAP BY CATEGORY AS AT
# PROC TABULATE equivalent:
#   TABLE NO*(CATEGORY=' '*(BRANCH1=' ') ALL='SUB TOTAL') ALL='GRAND TOTAL',
#         SUM=' '*(vars)
# ─────────────────────────────────────────────
VAR_COLS = ["BALANCE", "OPEN_BALANCE", "SUSPEND", "WRBACK", "WRIOFF_BAL", "CAP", "NET"]
VAR_LABELS = {
    "BALANCE":      "BALANCE",
    "OPEN_BALANCE": "OPENING BALANCE",
    "SUSPEND":      "CHARGE FOR THE YEAR",
    "WRBACK":       "WRITTEN BACK TO P & L",
    "WRIOFF_BAL":   "WRITTEN-OFF",
    "CAP":          "CLOSING BALANCE",
    "NET":          "NET INCREASE/DECREASE",
}

output_lines: list[str] = []

def emit(cc: str, content: str = "") -> None:
    output_lines.append(cc + content)

def fc(val) -> str:
    """Format COMMA20.2"""
    if val is None: return " " * 20
    return f"{float(val):>20,.2f}"

RTS   = 40
COL_W = 20

def sep_line(char: str = "-") -> str:
    return char * (RTS + COL_W * len(VAR_COLS))

def data_row(label: str, row_data: dict) -> str:
    vals = "".join(fc(row_data.get(c)) for c in VAR_COLS)
    return f"{label:<{RTS}}{vals}"

# Page header
emit("1", f"{TBL3A} {DATE_STR}")
emit(" ", "")

# Column headers
emit(" ", f"{'CATEGORY':^20}{'BRANCH':^20}" +
     "".join(f"{VAR_LABELS[c]:>{COL_W}}" for c in VAR_COLS))
emit(" ", sep_line())

grand_totals = {c: 0.0 for c in VAR_COLS}

for no_val in sorted(bycat_staff_df["NO"].unique().to_list()):
    cat_block = bycat_staff_df.filter(pl.col("NO") == no_val)
    if cat_block.is_empty():
        continue
    category = cat_block["CATEGORY"][0]

    # Branch-level detail within this category
    branch_agg = (
        cat_block.group_by("BRANCH1")
        .agg([pl.sum(c).alias(c) for c in VAR_COLS])
        .sort("BRANCH1")
    )

    for br_row in branch_agg.iter_rows(named=True):
        label = f"{category:<20}{(br_row['BRANCH1'] or ''):<20}"
        emit(" ", data_row(label, br_row))

    # SUB TOTAL for this category
    sub = cat_block.select([pl.sum(c).alias(c) for c in VAR_COLS]).row(0, named=True)
    emit(" ", data_row(f"{category:<20}{'SUB TOTAL':<20}", sub))
    emit(" ", sep_line("-"))

    for c in VAR_COLS:
        grand_totals[c] = grand_totals.get(c, 0.0) + (sub.get(c) or 0.0)

# GRAND TOTAL
emit(" ", sep_line("="))
emit(" ", data_row(f"{'GRAND TOTAL':<{RTS}}", grand_totals))
emit(" ", sep_line("-"))

with open(OUTPUT1_PATH, "w", encoding="utf-8") as fout:
    for line in output_lines:
        fout.write(line + "\n")

print(f"Report written : {OUTPUT1_PATH}")
