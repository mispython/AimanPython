"""
File Name: EIBMLIBT
Loan Maturity Profile Processor (BT)
Converts SAS program X_EIBMLIBT to Python using DuckDB and Polars.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import calendar
import sys

import duckdb
import polars as pl


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/path/to")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

REPTDATE_PATH = INPUT_DIR / "REPTDATE.parquet"
BTRAD_PATH_TEMPLATE = INPUT_DIR / "BTRAD{}{}.parquet"

BT_OUTPUT_PATH = OUTPUT_DIR / "BT.txt"
BT_REPORT_PATH = OUTPUT_DIR / "BT_REPORT.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# DuckDB Session
# ============================================================================
con = duckdb.connect()


# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

def get_rem_format(remmth: float) -> str:
    """Map remaining months to SAS REMFMT code."""
    if remmth <= 0.1:
        return "01"
    if remmth <= 1:
        return "02"
    if remmth <= 3:
        return "03"
    if remmth <= 6:
        return "04"
    if remmth <= 12:
        return "05"
    return "06"


def get_prod_format(product: int | None) -> str:
    """Map product to HL/RC/FL buckets based on SAS PRDFMT."""
    hl_products = {
        4, 5, 6, 7, 31, 32, 100, 101, 102, 103, 110, 111, 112, 113, 114, 115,
        116, 170, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
        225, 226, 227, 228, 229, 230, 231, 232, 233, 234,
    }
    rc_products = {350, 910, 925}

    if product in hl_products:
        return "HL"
    if product in rc_products:
        return "RC"
    return "FL"


# ============================================================================
# DATE HELPERS
# ============================================================================

def to_date(value) -> date | None:
    """Convert parquet values to Python date, supporting SAS numeric dates."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        return date(1960, 1, 1) + timedelta(days=int(value))
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def days_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def calculate_next_bldate(bldate: date, issdte: date | None, payfreq: str, freq: int) -> date:
    """Calculate the next billing date based on SAS NXTBLDT logic."""
    if payfreq == "6":
        return bldate + timedelta(days=14)

    dd = issdte.day if issdte else bldate.day
    mm = bldate.month + freq
    yy = bldate.year

    while mm > 12:
        mm -= 12
        yy += 1

    max_day = days_in_month(yy, mm)
    if dd > max_day:
        dd = max_day

    return date(yy, mm, dd)


def calculate_remaining_months(matdt: date, reptdate: date) -> float:
    """Calculate remaining months per SAS REMMTH macro."""
    rpyr = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rpdays = days_in_month(rpyr, rpmth)

    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    if mdday > rpdays:
        mdday = rpdays

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    return remy * 12 + remm + remd / rpdays


# ============================================================================
# READ REPTDATE
# ============================================================================

def read_reptdate() -> dict:
    """Read REPTDATE and derive macro variables."""
    print("Using REPTDATE:", REPTDATE_PATH)

    if not REPTDATE_PATH.exists():
        raise FileNotFoundError(f"REPTDATE file not found: {REPTDATE_PATH}")

    df = duckdb.query(
        "SELECT REPTDATE FROM read_parquet(?)",
        # [str(REPTDATE_PATH)],
        params=[str(REPTDATE_PATH)],
    ).pl()

    # df = con.execute(
    #     "SELECT REPTDATE FROM read_parquet(?)",
    #     [str(REPTDATE_PATH)],
    # ).pl()

    reptdate = to_date(df[0, 0])
    if reptdate is None:
        raise ValueError("REPTDATE is missing or invalid.")

    day = reptdate.day
    if day == 8:
        nowk = "1"
    elif day == 15:
        nowk = "2"
    elif day == 22:
        nowk = "3"
    else:
        nowk = "4"

    return {
        "REPTDATE": reptdate,
        "NOWK": nowk,
        "REPTMON": f"{reptdate.month:02d}",
        "REPTDAY": f"{reptdate.day:02d}",
        "REPTYEAR": f"{reptdate.year:04d}",
        "RDATE": reptdate.strftime("%d%m%Y"),
    }


# ============================================================================
# PROCESS LOAN DATA
# ============================================================================

def process_loan_data(macro_vars: dict) -> pl.DataFrame:
    """Process BTRAD loan data and produce BNMCODE/AMOUNT output."""
    reptdate = macro_vars["REPTDATE"]
    reptmon = macro_vars["REPTMON"]
    nowk = macro_vars["NOWK"]

    btrad_path = Path(str(BTRAD_PATH_TEMPLATE).format(reptmon, nowk))
    df = duckdb.query(
        "SELECT * FROM read_parquet(?)",
        # [str(btrad_path)],
        params=[str(btrad_path)],
    ).pl()

    df = df.filter(
        (pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 2) == "34")
        | (pl.col("PRODUCT").is_in([225, 226]))
    )

    output_records: list[dict] = []

    for row in df.iter_rows(named=True):
        custcd = str(row.get("CUSTCD", ""))
        product = row.get("PRODUCT")
        balance = float(row.get("BALANCE", 0) or 0)
        payamt = float(row.get("PAYAMT", 0) or 0)

        bldate = to_date(row.get("BLDATE"))
        issdte = to_date(row.get("ISSDTE"))
        exprdate = to_date(row.get("EXPRDATE"))

        if exprdate is None:
            continue

        if custcd in {"77", "78", "95", "96"}:
            cust = "08"
        else:
            cust = "09"

        prod_type = get_prod_format(product)

        if custcd in {"77", "78", "95", "96"}:
            item = "214" if prod_type == "HL" else "219"
        else:
            if prod_type == "FL":
                item = "211"
            elif prod_type == "RC":
                item = "212"
            else:
                item = "219"

        days = (reptdate - bldate).days if bldate else 0

        if (exprdate - reptdate).days < 8:
            remmth = 0.1
        else:
            payfreq = "3"
            if payfreq == "1":
                freq = 1
            elif payfreq == "2":
                freq = 3
            elif payfreq == "3":
                freq = 6
            elif payfreq == "4":
                freq = 12
            else:
                freq = 6

            if product in (350, 910, 925):
                bldate = exprdate
            elif not bldate:
                bldate = issdte
                if bldate is None:
                    bldate = reptdate
                while bldate <= reptdate:
                    bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)

            if payamt < 0:
                payamt = 0

            if bldate > exprdate or balance <= payamt:
                bldate = exprdate

            while bldate <= exprdate:
                remmth = calculate_remaining_months(bldate, reptdate)
                if remmth > 12 or bldate == exprdate:
                    break

                amount = payamt
                balance -= payamt
                bnmcode = f"95{item}{cust}{get_rem_format(remmth)}0000Y"
                output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})

                remmth_code = 13 if days > 89 else remmth
                bnmcode = f"93{item}{cust}{get_rem_format(remmth_code)}0000Y"
                output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})

                bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)
                if bldate > exprdate or balance <= payamt:
                    bldate = exprdate

        amount = balance
        bnmcode = f"95{item}{cust}{get_rem_format(remmth)}0000Y"
        output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})

        remmth_code = 13 if days > 89 else remmth
        bnmcode = f"93{item}{cust}{get_rem_format(remmth_code)}0000Y"
        output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})

    if not output_records:
        return pl.DataFrame({"BNMCODE": [], "AMOUNT": []})

    return pl.DataFrame(output_records)


# ============================================================================
# AGGREGATE AND OUTPUT
# ============================================================================

def aggregate_output(df_output: pl.DataFrame) -> pl.DataFrame:
    """Aggregate amounts by BNMCODE (PROC SUMMARY NWAY)."""
    if df_output.is_empty():
        return df_output

    return df_output.group_by("BNMCODE").agg(pl.col("AMOUNT").sum())


def write_output_txt(df_agg: pl.DataFrame) -> None:
    """Write BNMCODE/AMOUNT output to text file."""
    with open(BT_OUTPUT_PATH, "w", encoding="utf-8") as file_handle:
        for row in df_agg.iter_rows(named=True):
            file_handle.write(f"{row['BNMCODE']}|{row['AMOUNT']:.2f}\n")


# ============================================================================
# REPORT GENERATION
# ============================================================================

def build_report_lines(df_agg: pl.DataFrame) -> list[str]:
    """Build report lines with ASA carriage control characters."""
    lines: list[str] = []
    page_length = 60

    def add_line(text: str, new_page: bool = False) -> int:
        control = "1" if new_page else " "
        lines.append(control + text)
        return 1 if new_page else 0

    def add_header() -> int:
        count = 0
        count += add_line("LOAN MATURITY PROFILE REPORT", new_page=True)
        lines.append(" " + "=" * 60)
        lines.append(" " + f"{'BNMCODE':<20}{'AMOUNT':>20}")
        lines.append(" " + "-" * 60)
        return 4

    line_count = add_header()
    total_amount = 0.0

    for row in df_agg.iter_rows(named=True):
        if line_count >= page_length:
            line_count = add_header()
        amount = float(row["AMOUNT"])
        total_amount += amount
        lines.append(" " + f"{row['BNMCODE']:<20}{amount:>20.2f}")
        line_count += 1

    if line_count >= page_length:
        line_count = add_header()

    lines.append(" " + "-" * 60)
    lines.append(" " + f"{'TOTAL':<20}{total_amount:>20.2f}")
    lines.append(" " + "=" * 60)

    return lines


def write_report(lines: list[str]) -> None:
    """Write report with ASA control characters."""
    with open(BT_REPORT_PATH, "w", encoding="utf-8") as file_handle:
        for line in lines:
            file_handle.write(line + "\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main() -> int:
    """Main execution function."""
    try:
        macro_vars = read_reptdate()
        df_output = process_loan_data(macro_vars)
        df_agg = aggregate_output(df_output)

        write_output_txt(df_agg)
        report_lines = build_report_lines(df_agg)
        write_report(report_lines)

        print("Processing completed successfully.")
        print(f"Output file: {BT_OUTPUT_PATH}")
        print(f"Report file: {BT_REPORT_PATH}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    sys.exit(main())
