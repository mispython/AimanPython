#!/usr/bin/env python3
"""
Program: EIBCAPS3.py
Purpose: Generate PBB(Staff) Movement of CAP by Category report
"""

# NOTE: The original SAS program includes PBBELF via %INC PGM(PBBELF),
#       which defines branch/region/customer-type format mappings and BNM code
#       lookup tables (e.g. format_brchcd, format_regioff, format_ctype, etc.).
#       However, none of those functions or lookup tables are actually invoked
#       anywhere in this program — the PBBELF include is standard boilerplate
#       carried across many jobs in this system.
#       Therefore, no import from PBBELF is required here.
# from PBBELF import (format_brchcd, format_regioff, format_ctype, ...)

import duckdb
import polars as pl
import os
from datetime import date

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
# OPTIONS MISSING=0 – fill numeric nulls with 0 before aggregation
# ─────────────────────────────────────────────
VAR_COLS = ["BALANCE", "OPEN_BALANCE", "SUSPEND", "WRBACK", "WRIOFF_BAL", "CAP", "NET"]
for col in VAR_COLS:
    if col in cap_df.columns:
        cap_df = cap_df.with_columns(pl.col(col).fill_null(0.0))

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
# PROC TABULATE rendering
#
# SAS specification:
#   FORMCHAR(1,2,3,4,5,6,7,8,9,10,11)='|-+++++++++';
#   → vertical bar = '|', horizontal bar = '-', intersections = '+'
#
#   TABLE NO*(CATEGORY=' '*(BRANCH1=' ') ALL='SUB TOTAL') ALL='GRAND TOTAL',
#         SUM=' '*(vars)
#   BOX='              CATEGORY      BRANCH'  RTS=40
#
# Column labels (from VAR aliases in PROC TABULATE):
#   BALANCE | OPENING BALANCE | CHARGE FOR THE YEAR |
#   WRITTEN BACK TO P & L | WRITTEN-OFF | CLOSING BALANCE |
#   NET INCREASE/DECREASE
# ─────────────────────────────────────────────
VAR_LABELS = {
    "BALANCE":      "BALANCE",
    "OPEN_BALANCE": "OPENING BALANCE",
    "SUSPEND":      "CHARGE FOR THE YEAR",
    "WRBACK":       "WRITTEN BACK TO P & L",
    "WRIOFF_BAL":   "WRITTEN-OFF",
    "CAP":          "CLOSING BALANCE",
    "NET":          "NET INCREASE/DECREASE",
}

# Layout constants
RTS   = 40    # row title size (BOX RTS=40)
COL_W = 20    # FORMAT=COMMA20.2

# FORMCHAR: '|' = vertical, '-' = horizontal, '+' = intersection
VC = "|"
HC = "-"
XC = "+"

def sep_line(fill: str = HC) -> str:
    """Full separator line using FORMCHAR characters."""
    return XC + fill * RTS + (XC + fill * COL_W) * len(VAR_COLS) + XC

def col_header_line(box_label: str) -> str:
    """Column header line: BOX label + VAR labels."""
    label_part = f"{box_label:<{RTS}}"[:RTS]
    return VC + label_part + "".join(
        VC + f"{VAR_LABELS[c]:^{COL_W}}"[:COL_W] for c in VAR_COLS
    ) + VC

def fc(val) -> str:
    """Format COMMA20.2 – right-justified, missing/zero as 0.00."""
    if val is None:
        return f"{'0.00':>{COL_W}}"
    return f"{float(val):>{COL_W},.2f}"

def data_row(label: str, row_data: dict) -> str:
    """Data row with vertical separators matching FORMCHAR."""
    label_part = f"{label:<{RTS}}"[:RTS]
    return VC + label_part + "".join(VC + fc(row_data.get(c)) for c in VAR_COLS) + VC

# ─────────────────────────────────────────────
# Build output lines with ASA carriage control characters
# ─────────────────────────────────────────────
output_lines: list[str] = []

def emit(asa_cc: str, content: str = "") -> None:
    """Emit line with ASA carriage control character prefix."""
    output_lines.append(asa_cc + content)

# Page header – ASA '1' = skip to new page
emit("1", f"{TBL3A} {DATE_STR}")
emit(" ", "")

# Table top border
emit(" ", sep_line())

# Column header: BOX label occupies the RTS area (top-left corner)
BOX_LABEL = "              CATEGORY      BRANCH"
emit(" ", col_header_line(BOX_LABEL))
emit(" ", sep_line())

# ─────────────────────────────────────────────
# Iterate by NO → CATEGORY → BRANCH1
# ─────────────────────────────────────────────
grand_totals = {c: 0.0 for c in VAR_COLS}

for no_val in sorted(bycat_staff_df["NO"].unique().to_list()):
    cat_block = bycat_staff_df.filter(pl.col("NO") == no_val)
    if cat_block.is_empty():
        continue
    category = cat_block["CATEGORY"][0]

    # Branch-level detail rows within this category
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
    emit(" ", sep_line())
    emit(" ", data_row(f"{category:<20}{'SUB TOTAL':<20}", sub))
    emit(" ", sep_line())

    for c in VAR_COLS:
        grand_totals[c] = grand_totals.get(c, 0.0) + (sub.get(c) or 0.0)

# GRAND TOTAL
emit(" ", data_row(f"{'GRAND TOTAL':<{RTS}}", grand_totals))
emit(" ", sep_line())

# ─────────────────────────────────────────────
# Write output file
# ─────────────────────────────────────────────
with open(OUTPUT1_PATH, "w", encoding="utf-8") as fout:
    for line in output_lines:
        fout.write(line + "\n")

print(f"Report written : {OUTPUT1_PATH}")
