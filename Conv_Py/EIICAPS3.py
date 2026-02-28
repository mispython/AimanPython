# !/usr/bin/env python3
"""
Program: EIICAPS3.py
Purpose: PIBB(STAFF) MOVEMENT OF CAP BY CATEGORY AS AT
"""

import duckdb
import polars as pl
import os
from datetime import date, timedelta

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR   = r"C:/data"
LOAN_DIR   = os.path.join(BASE_DIR, "loan")
NPL_DIR    = os.path.join(BASE_DIR, "npl")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT1_PATH = os.path.join(OUTPUT_DIR, "EIICAPS3.txt")

con = duckdb.connect()

# ─────────────────────────────────────────────
# REPTDATE – derive macro variables
# ─────────────────────────────────────────────
reptdate_df = con.execute(f"""
    SELECT REPTDATE FROM '{LOAN_DIR}/REPTDATE.parquet'
""").pl()

reptdate_val: date = reptdate_df["REPTDATE"][0]
day = reptdate_val.day
if 1 <= day <= 8:
    wk = "1"
elif 9 <= day <= 15:
    wk = "2"
elif 16 <= day <= 22:
    wk = "3"
else:
    wk = "4"

REPTMON  = f"{reptdate_val.month:02d}"
NOWK     = wk
REPTYEAR = str(reptdate_val.year)[-2:]
REPTDAY  = f"{reptdate_val.day:02d}"
DATE     = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

TBL4  = "PIBB(STAFF) MOVEMENT OF CAP BY CATEGORY AS AT"
title = f"{TBL4} {DATE}"

# ─────────────────────────────────────────────
# CATEGORY → NO ordering map
# ─────────────────────────────────────────────
CATEGORY_NO = {
    "CURRENT":               1,
    "1-2 MTHS":              2,
    "3-5 MTHS":              3,
    "6-11 MTHS":             4,
    ">=12 MTHS":             5,
    "IRREGULAR":             6,
    "REPOSSESSED <12 MTHS":  7,
    "REPOSSESSED >=12 MTHS": 8,
    "DEFICIT":               9,
}

# ─────────────────────────────────────────────
# Load ICAP_STAFF{REPTMON}{REPTYEAR}
# ─────────────────────────────────────────────
icap_staff_path = os.path.join(NPL_DIR, f"ICAP_STAFF{REPTMON}{REPTYEAR}.parquet")
ibycat_df = con.execute(f"SELECT * FROM '{icap_staff_path}'").pl()

# Assign NO
no_list = [CATEGORY_NO.get(c, None) for c in ibycat_df["CATEGORY"].to_list()]
ibycat_df = ibycat_df.with_columns(pl.Series("NO", no_list, dtype=pl.Int64))
ibycat_df = ibycat_df.sort("NO")

# Fill numeric nulls with 0 (OPTIONS MISSING=0)
for col in ["BALANCE", "OPEN_BALANCE", "SUSPEND", "WRBACK", "WRIOFF_BAL", "CAP", "NET"]:
    if col in ibycat_df.columns:
        ibycat_df = ibycat_df.with_columns(pl.col(col).fill_null(0.0))

# ─────────────────────────────────────────────
# Build PROC TABULATE equivalent
# Structure:
#   Rows  : NO * (CATEGORY * (BRANCH1 + SUB TOTAL)) + GRAND TOTAL
#   Cols  : BALANCE | OPENING BALANCE | CHARGE FOR THE YEAR |
#           WRITTEN BACK TO P & L | WRITTEN-OFF | CLOSING BALANCE |
#           NET INCREASE/DECREASE
# ─────────────────────────────────────────────

VAR_COLS = [
    ("BALANCE",      "BALANCE"),
    ("OPEN_BALANCE", "OPENING BALANCE"),
    ("SUSPEND",      "CHARGE FOR THE YEAR"),
    ("WRBACK",       "WRITTEN BACK TO P & L"),
    ("WRIOFF_BAL",   "WRITTEN-OFF"),
    ("CAP",          "CLOSING BALANCE"),
    ("NET",          "NET INCREASE/DECREASE"),
]

# RTS=40  → row title space = 40 chars
RTS     = 40
COL_W   = 20          # COMMA20.2
COL_SEP = "|"
LINE_CH = "-"
PLUS    = "+"

# separator line
sep_line = (
    PLUS + LINE_CH * RTS
    + "".join(PLUS + LINE_CH * COL_W for _ in VAR_COLS)
    + PLUS
)

def fc(v) -> str:
    """Format numeric as COMMA20.2, right-justified in COL_W."""
    if v is None or v == 0.0:
        return f"{'0.00':>{COL_W}}"
    return f"{v:>{COL_W},.2f}"

def row_line(label: str, vals: list) -> str:
    label_part = f"{label:<{RTS}}"[:RTS]
    return label_part + "".join(COL_SEP + fc(v) for v in vals) + COL_SEP

def header_line(label: str, col_labels: list) -> str:
    label_part = f"{label:<{RTS}}"[:RTS]
    return label_part + "".join(
        COL_SEP + f"{cl:^{COL_W}}"[:COL_W] for cl in col_labels
    ) + COL_SEP

def sum_vars(df: pl.DataFrame) -> list:
    return [df[src].sum() if src in df.columns else 0.0 for src, _ in VAR_COLS]

# ─────────────────────────────────────────────
# Build output lines
# ─────────────────────────────────────────────
lines_out: list[str] = []

def wr(line: str, asa: str = " "):
    lines_out.append(asa + line)

# Title (ASA '1' = new page)
wr(title, "1")
wr(sep_line)

# Column header row – variable labels
box_label = "              CATEGORY      BRANCH"
col_lbls  = [lbl for _, lbl in VAR_COLS]
wr(header_line(box_label, col_lbls))
wr(sep_line)

# Iterate over each NO group (sorted)
for no_val in sorted(CATEGORY_NO.values()):
    no_df = ibycat_df.filter(pl.col("NO") == no_val)
    if no_df.is_empty():
        continue

    # There is only one CATEGORY per NO
    category = no_df["CATEGORY"][0]

    # Branches within this category, sorted
    branches = sorted(no_df["BRANCH1"].drop_nulls().unique().to_list())

    for branch in branches:
        b_df  = no_df.filter(pl.col("BRANCH1") == branch)
        vals  = sum_vars(b_df)
        label = f"  {category}  {branch}"
        wr(row_line(label, vals))

    # SUB TOTAL for this category
    sub_vals  = sum_vars(no_df)
    sub_label = f"  {category}  SUB TOTAL"
    wr(sep_line)
    wr(row_line(sub_label, sub_vals))
    wr(sep_line)

# GRAND TOTAL
grand_vals  = sum_vars(ibycat_df)
grand_label = "GRAND TOTAL"
wr(row_line(grand_label, grand_vals))
wr(sep_line)

# ─────────────────────────────────────────────
# Write output file
# ─────────────────────────────────────────────
with open(OUTPUT1_PATH, "w", encoding="utf-8") as f:
    for line in lines_out:
        f.write(line + "\n")

print(f"Report written to : {OUTPUT1_PATH}")
