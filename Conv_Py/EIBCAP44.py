# !/usr/bin/env python3
"""
Program: EIBCAP44
Purpose: Write CAP and CAP_STAFF records to CCRIS output file where CAP > 0
"""

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

CCRIS_PATH = os.path.join(OUTPUT_DIR, "ccris.txt")

con = duckdb.connect()

# ─────────────────────────────────────────────
# REPTDATE – derive REPTMON, NOWK, REPTYEAR, REPTDAY
# ─────────────────────────────────────────────
reptdate_df = con.execute(
    f"SELECT REPTDATE FROM read_parquet('{LOAN_DIR}/reptdate.parquet') LIMIT 1"
).pl()

reptdate_val: date = reptdate_df["REPTDATE"][0]
day_val   = reptdate_val.day
month_val = reptdate_val.month
year_val  = reptdate_val.year % 100  # YEAR2. → 2-digit year

if   1  <= day_val <=  8:  wk = "1"
elif 9  <= day_val <= 15:  wk = "2"
elif 16 <= day_val <= 22:  wk = "3"
else:                       wk = "4"

REPTMON  = f"{month_val:02d}"
NOWK     = wk
REPTYEAR = f"{year_val:02d}"
REPTDAY  = f"{day_val:02d}"

# ─────────────────────────────────────────────
# Load NPL.CAP and NPL.CAP_STAFF
# Dependencies: EIBCAP41.py  → produces npl/cap{REPTMON}{REPTYEAR}.parquet
#               EIBCAPS1.py  → produces npl/cap_staff{REPTMON}{REPTYEAR}.parquet
# ─────────────────────────────────────────────
cap_parquet       = os.path.join(NPL_DIR, f"cap{REPTMON}{REPTYEAR}.parquet")
cap_staff_parquet = os.path.join(NPL_DIR, f"cap_staff{REPTMON}{REPTYEAR}.parquet")

cap_df       = con.execute(f"SELECT * FROM read_parquet('{cap_parquet}')").pl()
cap_staff_df = con.execute(f"SELECT * FROM read_parquet('{cap_staff_parquet}')").pl()

# Combine both datasets (SET in SAS)
combined_df = pl.concat([cap_df, cap_staff_df], how="diagonal")

# Filter: IF CAP > 0
combined_df = combined_df.filter(pl.col("CAP") > 0)

# ─────────────────────────────────────────────
# Format helpers matching SAS PUT formats
# ─────────────────────────────────────────────
def fmt_numeric(val, width: int) -> str:
    """Right-justified numeric, no decimals (SAS w. format)."""
    if val is None:
        return " " * width
    try:
        return f"{int(val):>{width}d}"
    except Exception:
        return f"{float(val):>{width}.0f}"


def fmt_z(val, width: int) -> str:
    """Zero-padded integer (SAS Zw. format)."""
    if val is None:
        return "0" * width
    try:
        return f"{int(val):0>{width}d}"
    except Exception:
        return "0" * width


def fmt_decimal(val, width: int, dec: int) -> str:
    """Right-justified numeric with decimals (SAS w.d format)."""
    if val is None:
        return " " * width
    try:
        formatted = f"{float(val):{width}.{dec}f}"
        return formatted
    except Exception:
        return " " * width


def fmt_char(val, width: int) -> str:
    """Left-justified character, space-padded (SAS $CHARw. format)."""
    if val is None:
        return " " * width
    s = str(val)
    return s[:width].ljust(width)


def build_record(row: dict) -> str:
    """
    Build a fixed-width record matching SAS PUT layout:
      @001 ACCTNO  10.       → cols  1-10
      @012 NOTENO  Z5.       → cols 12-16
      @018 BRANCH  Z5.       → cols 18-22
      @024 CAP     20.2      → cols 24-43
      @045 AANO    $CHAR13.  → cols 45-57
      @059 PD      6.2       → cols 59-64
      @066 LGD     6.2       → cols 66-71
    Total record width: 71 characters
    """
    record = [" "] * 71

    def place(start_col: int, text: str) -> None:
        idx = start_col - 1
        for i, ch in enumerate(text):
            pos = idx + i
            if pos < len(record):
                record[pos] = ch

    place(1,  fmt_numeric(row.get("ACCTNO"), 10))
    place(12, fmt_z(row.get("NOTENO"), 5))
    place(18, fmt_z(row.get("BRANCH"), 5))
    place(24, fmt_decimal(row.get("CAP"), 20, 2))
    place(45, fmt_char(row.get("AANO"), 13))
    place(59, fmt_decimal(row.get("PD"), 6, 2))
    place(66, fmt_decimal(row.get("LGD"), 6, 2))

    return "".join(record)


# ─────────────────────────────────────────────
# Write CCRIS output file
# ─────────────────────────────────────────────
with open(CCRIS_PATH, "w", encoding="utf-8") as fout:
    for row in combined_df.iter_rows(named=True):
        fout.write(build_record(row) + "\n")

print(f"CCRIS output written : {CCRIS_PATH}")
print(f"Records written      : {combined_df.height}")
