#!/usr/bin/env python3
"""
Program: FALQPBBI.py
Purpose: RM FD ACCEPTED BY REMAINING MATURITY
         - Computes remaining maturity (REMMTH) for fixed deposits from FDMTHLY.
         - Summarises balances by BIC, CUSTCODE, REMMTH.
         - Builds BNMCODE and filters for BIC prefix '42132'.
         - Prints a report with ASA carriage control characters.
"""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import duckdb
import polars as pl

# Dependency: KALMPBBF provides format_kremmth
from KALMPBBF import format_kremmth

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FDMTHLY_PARQUET = INPUT_DIR / "FDMTHLY.parquet"
ALMDEPT1_TXT    = OUTPUT_DIR / "ALMDEPT1.txt"
REPORT_TXT      = OUTPUT_DIR / "FALQPBBI_REPORT.txt"

# ---------------------------------------------------------------------------
# Report / page layout constants
# ---------------------------------------------------------------------------
PAGE_LENGTH   = 60   # lines per page (default)
LINE_WIDTH    = 132  # print line width
DETAIL_HEADER = 5    # lines consumed by titles + blank + column header + rule

# ---------------------------------------------------------------------------
# Runtime parameter  (&RDATE  in SAS)
# Provide as env var RDATE in DDMMYYYY format, e.g. "31122024"
# ---------------------------------------------------------------------------
RDATE_STR = os.environ.get("RDATE", "")
if not RDATE_STR:
    print("ERROR: Environment variable RDATE must be set (DDMMYYYY), e.g. RDATE=31122024")
    sys.exit(1)

# Parse RDATE (DDMMYY8. format → DD MM YYYY)
try:
    rp_day   = int(RDATE_STR[0:2])
    rp_month = int(RDATE_STR[2:4])
    rp_year  = int(RDATE_STR[4:8])
    reptdate = date(rp_year, rp_month, rp_day)
except (ValueError, IndexError):
    print(f"ERROR: Cannot parse RDATE='{RDATE_STR}'. Expected DDMMYYYY.")
    sys.exit(1)

# Days in each month for the report period year (RD1-RD12)
def _month_days(year: int) -> list[int]:
    """Return list of days-per-month [1..12] for the given year."""
    feb = 29 if year % 4 == 0 else 28
    return [31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

rp_days = _month_days(rp_year)   # RPDAYS array  (0-indexed: rp_days[mth-1])


# ---------------------------------------------------------------------------
# %MACRO DCLVAR / %MACRO REMMTH – implemented as a Python function
# ---------------------------------------------------------------------------
def compute_remmth(
    fddate: date,
    rpyr: int,
    rpmth: int,
    rpday: int,
    rp_days_arr: list[int],
) -> float:
    """
    Equivalent of %REMMTH macro.

    FD2 (days in Feb of FD year) is recomputed per observation as in SAS.
    If FDDAY equals the last day of FDMTH, it is replaced by RPDAY
    (end-of-month alignment).
    """
    fd_yr  = fddate.year
    fd_mth = fddate.month
    fd_day = fddate.day

    # FD2 adjustment (leap-year for FD year)
    fd_days = _month_days(fd_yr)   # FD1-FD12

    # IF FDDAY = FDDAYS(FDMTH) THEN FDDAY = RPDAYS(RPMTH)
    if fd_day == fd_days[fd_mth - 1]:
        fd_day = rp_days_arr[rpmth - 1]

    rem_y = fd_yr  - rpyr
    rem_m = fd_mth - rpmth
    rem_d = fd_day - rpday

    remmth = rem_y * 12 + rem_m + rem_d / rp_days_arr[rpmth - 1]
    return remmth


# ---------------------------------------------------------------------------
# Step 1 – DATA ALM  (equivalent of DATA ALM step)
# ---------------------------------------------------------------------------
def build_alm(fdmthly_path: Path) -> pl.DataFrame:
    """
    Read FDMTHLY, filter AMTIND='I', compute REMMTH, keep BIC CUSTCODE REMMTH CURBAL.
    Columns expected in FDMTHLY parquet:
        BIC, CUSTCODE, AMTIND, OPENIND, MATDATE (integer YYYYMMDD), CURBAL
    """
    con = duckdb.connect()
    raw = con.execute(f"SELECT * FROM read_parquet('{fdmthly_path}')").pl()
    con.close()

    # IF AMTIND='I'
    raw = raw.filter(pl.col("AMTIND") == "I")

    records = []
    for row in raw.iter_rows(named=True):
        open_ind = row["OPENIND"]
        if open_ind == "O":
            # FDDATE = INPUT(PUT(MATDATE,Z8.),YYMMDD8.)  → parse YYYYMMDD integer
            mat_str  = str(int(row["MATDATE"])).zfill(8)
            fd_date  = date(int(mat_str[0:4]), int(mat_str[4:6]), int(mat_str[6:8]))
            remmth   = compute_remmth(fd_date, rp_year, rp_month, rp_day, rp_days)
        elif open_ind == "D":
            remmth = -1.0
        else:
            # OTHERWISE – skip (no output in SAS)
            continue

        records.append({
            "BIC":      row["BIC"],
            "CUSTCODE": row["CUSTCODE"],
            "REMMTH":   remmth,
            "CURBAL":   row["CURBAL"],
        })

    alm_df = pl.DataFrame(records, schema={
        "BIC":      pl.Utf8,
        "CUSTCODE": pl.Utf8,
        "REMMTH":   pl.Float64,
        "CURBAL":   pl.Float64,
    })
    return alm_df


# ---------------------------------------------------------------------------
# Step 2 – PROC SUMMARY (ALM → ALM summarised by BIC CUSTCODE REMMTH)
# ---------------------------------------------------------------------------
def summarise_alm(alm_df: pl.DataFrame) -> pl.DataFrame:
    """PROC SUMMARY DATA=ALM NWAY; CLASS BIC CUSTCODE REMMTH; VAR CURBAL; SUM=AMOUNT"""
    return (
        alm_df
        .group_by(["BIC", "CUSTCODE", "REMMTH"])
        .agg(pl.col("CURBAL").sum().alias("AMOUNT"))
    )


# ---------------------------------------------------------------------------
# Step 3 – DATA ALMDEPT (build BNMCODE)
# ---------------------------------------------------------------------------
def build_almdept(alm_sum: pl.DataFrame) -> pl.DataFrame:
    """
    Apply KREMMTH format and build BNMCODE per CUSTCODE grouping.
    LENGTH BNMCODE $14.
    """
    rows = []
    for row in alm_sum.iter_rows(named=True):
        bic      = str(row["BIC"])
        custcode = str(row["CUSTCODE"])
        amount   = row["AMOUNT"]
        remmth   = row["REMMTH"]

        rm = format_kremmth(remmth)   # KREMMTH format from KALMPBBF

        # SELECT(CUSTCODE)
        if custcode in ("81", "82", "83", "84"):
            bnmcode = (bic + custcode + rm + "0000Y")[:14]
            rows.append({"BNMCODE": bnmcode, "AMOUNT": amount,
                         "CUSTCODE": custcode, "REMMTH": remmth})
        elif custcode in ("85", "86", "90", "91", "92", "95", "96", "98", "99"):
            bnmcode = (bic + "85" + rm + "0000Y")[:14]
            rows.append({"BNMCODE": bnmcode, "AMOUNT": amount,
                         "CUSTCODE": custcode, "REMMTH": remmth})
        # OTHERWISE – no output

    return pl.DataFrame(rows, schema={
        "BNMCODE":  pl.Utf8,
        "AMOUNT":   pl.Float64,
        "CUSTCODE": pl.Utf8,
        "REMMTH":   pl.Float64,
    })


# ---------------------------------------------------------------------------
# Step 4 – PROC SUMMARY on ALMDEPT (filter SUBSTR(BNMCODE,1,5)='42132')
# ---------------------------------------------------------------------------
def summarise_almdept(almdept: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SUMMARY DATA=ALMDEPT NWAY;
    WHERE SUBSTR(BNMCODE,1,5) = '42132';
    CLASS BNMCODE; VAR AMOUNT; OUTPUT OUT=ALMDEPT1 SUM=AMOUNT;
    (DROP=_TYPE_ _FREQ_)
    """
    filtered = almdept.filter(pl.col("BNMCODE").str.slice(0, 5) == "42132")
    return (
        filtered
        .group_by("BNMCODE")
        .agg(pl.col("AMOUNT").sum().alias("AMOUNT"))
    )


# ---------------------------------------------------------------------------
# Step 5 – PROC PRINT with ASA carriage control characters
# ---------------------------------------------------------------------------
def format_amount(value: float) -> str:
    """Format numeric amount like SAS default numeric print (comma, 2dp)."""
    return f"{value:>20,.2f}"


def generate_report(almdept1: pl.DataFrame, rdate_str: str) -> str:
    """
    Produce a fixed-width report with ASA carriage control characters.

    ASA codes used:
        '1' – skip to new page before printing
        ' ' – single space (advance one line before printing)
        '0' – double space (advance two lines before printing)
        '+' – overprint (no advance)
    """
    title1 = "SPECIAL PURPOSE ITEMS (QUARTERLY): EXTERNAL LIABILITIES"
    title2 = f"AS AT {rdate_str}"
    title3 = "CODE 81 & 85 FOR 42132-80-XX-0000Y"

    col_hdr_bnmcode = "BNMCODE"
    col_hdr_amount  = "             AMOUNT"
    rule            = "-" * 35

    # Sort by BNMCODE for consistent print order (PROC PRINT default)
    data = almdept1.sort("BNMCODE")

    lines: list[str] = []
    total_amount = 0.0

    page_num        = 0
    lines_on_page   = PAGE_LENGTH  # force header on first iteration

    def emit_page_header() -> None:
        nonlocal page_num, lines_on_page
        page_num += 1
        asa = "1"   # skip to new page
        lines.append(f"{asa}{title1}")
        lines.append(f" {title2}")
        lines.append(f" {title3}")
        lines.append(f" {col_hdr_bnmcode:<14}  {col_hdr_amount}")
        lines.append(f" {rule}")
        lines_on_page = DETAIL_HEADER

    for row in data.iter_rows(named=True):
        if lines_on_page >= PAGE_LENGTH:
            emit_page_header()

        bnmcode = str(row["BNMCODE"])
        amount  = float(row["AMOUNT"])
        total_amount += amount

        amt_str = format_amount(amount)
        lines.append(f" {bnmcode:<14}  {amt_str}")
        lines_on_page += 1

    # SUM statement – print total with double-space before it
    total_str = format_amount(total_amount)
    lines.append(f"0{'':14}  {total_str}")   # '0' = double space

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # Step 1 – build ALM
    alm_df  = build_alm(FDMTHLY_PARQUET)

    # Step 2 – summarise ALM
    alm_sum = summarise_alm(alm_df)

    # Step 3 – build ALMDEPT with BNMCODE
    almdept = build_almdept(alm_sum)

    # Step 4 – summarise ALMDEPT (filter + group by BNMCODE)
    almdept1 = summarise_almdept(almdept)

    # Persist ALMDEPT1 as text output
    almdept1.write_csv(ALMDEPT1_TXT, separator="\t")
    print(f"Written: {ALMDEPT1_TXT}  ({len(almdept1)} rows)")

    # Step 5 – generate report with ASA carriage control
    report = generate_report(almdept1, RDATE_STR)
    REPORT_TXT.write_text(report, encoding="utf-8")
    print(f"Written: {REPORT_TXT}")


if __name__ == "__main__":
    main()
