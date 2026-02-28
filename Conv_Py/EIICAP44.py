# !/usr/bin/env python3
"""
Program: EIICAP44.py
Purpose: Write combined ICAP and ICAP_STAFF records to CCRIS fixed-width output file
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

# CCRIS output file (fixed-width text)
CCRIS_PATH = os.path.join(OUTPUT_DIR, "CCRIS.txt")

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

# ─────────────────────────────────────────────
# Load ICAP and ICAP_STAFF parquet files
# ─────────────────────────────────────────────
icap_path       = os.path.join(NPL_DIR, f"ICAP{REPTMON}{REPTYEAR}.parquet")
icap_staff_path = os.path.join(NPL_DIR, f"ICAP_STAFF{REPTMON}{REPTYEAR}.parquet")

icap_df       = con.execute(f"SELECT * FROM '{icap_path}'").pl()
icap_staff_df = con.execute(f"SELECT * FROM '{icap_staff_path}'").pl()

# Combine both datasets (SET statement – vertical stack)
combined_df = pl.concat([icap_df, icap_staff_df], how="diagonal")

# ─────────────────────────────────────────────
# Write fixed-width CCRIS output file
#
# SAS layout (1-based column positions):
#   @001  ACCTNO   10.        → cols  1-10   numeric, right-justified, width 10
#   @012  NOTENO   Z5.        → cols 12-16   zero-padded numeric, width 5
#   @018  BRANCH   Z5.        → cols 18-22   zero-padded numeric, width 5
#   @024  CAP      20.2       → cols 24-43   numeric 20.2, right-justified, width 20
#   @045  AANO     $CHAR13.   → cols 45-57   character, left-justified, width 13
#   @059  PD       6.2        → cols 59-64   numeric 6.2, right-justified, width 6
#   @066  LGD      6.2        → cols 66-71   numeric 6.2, right-justified, width 6
#
# Total line width = 71 characters (positions 1-71)
# Gaps between fields are filled with spaces.
# ─────────────────────────────────────────────

LINE_WIDTH = 71

def fmt_numeric(val, width: int) -> str:
    """Right-justify a numeric value in the given width; blank if None/missing."""
    if val is None:
        return " " * width
    try:
        return f"{float(val):>{width}.0f}"[:width]
    except (TypeError, ValueError):
        return " " * width

def fmt_zero_padded(val, width: int) -> str:
    """Zero-padded integer of the given width; zeros if None/missing (OPTIONS MISSING=0)."""
    if val is None:
        return "0" * width
    try:
        return f"{int(val):0{width}d}"[:width]
    except (TypeError, ValueError):
        return "0" * width

def fmt_decimal(val, width: int, decimals: int) -> str:
    """Right-justify a fixed-decimal numeric in the given width; blank if None/missing."""
    if val is None:
        return " " * width
    try:
        s = f"{float(val):>{width}.{decimals}f}"
        return s[:width]
    except (TypeError, ValueError):
        return " " * width

def fmt_char(val, width: int) -> str:
    """Left-justify a character value, padded/truncated to width."""
    if val is None:
        return " " * width
    return f"{str(val):<{width}}"[:width]

def build_line(row: dict) -> str:
    """
    Build a fixed-width output line matching the SAS PUT statement.
    Positions are 1-based; gaps are spaces.
    """
    buf = [" "] * LINE_WIDTH

    def place(text: str, col: int):
        """Write text into buf starting at 1-based column col."""
        for i, ch in enumerate(text):
            pos = col - 1 + i
            if 0 <= pos < LINE_WIDTH:
                buf[pos] = ch

    # @001  ACCTNO  10.   – right-justified numeric, width 10
    place(fmt_numeric(row.get("ACCTNO"), 10),  1)

    # @012  NOTENO  Z5.   – zero-padded, width 5
    place(fmt_zero_padded(row.get("NOTENO"), 5), 12)

    # @018  BRANCH  Z5.   – zero-padded, width 5
    place(fmt_zero_padded(row.get("BRANCH"), 5), 18)

    # @024  CAP     20.2  – right-justified 20.2, width 20
    place(fmt_decimal(row.get("CAP"), 20, 2),   24)

    # @045  AANO    $CHAR13.  – left-justified character, width 13
    place(fmt_char(row.get("AANO"), 13),         45)

    # @059  PD      6.2   – right-justified 6.2, width 6
    place(fmt_decimal(row.get("PD"), 6, 2),      59)

    # @066  LGD     6.2   – right-justified 6.2, width 6
    place(fmt_decimal(row.get("LGD"), 6, 2),     66)

    return "".join(buf)

# ─────────────────────────────────────────────
# Write output
# ─────────────────────────────────────────────
with open(CCRIS_PATH, "w", encoding="utf-8") as f:
    for row in combined_df.iter_rows(named=True):
        f.write(build_line(row) + "\n")

print(f"CCRIS file written to : {CCRIS_PATH}")
print(f"Total records written : {combined_df.height}")
