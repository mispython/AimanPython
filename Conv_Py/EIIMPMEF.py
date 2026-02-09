#!/usr/bin/env python3
"""
File Name: EIIMPMEF
Performance Report on Product 428 and 439
Generates two PROC PRINT-style reports with ASA carriage control characters.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl


# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = Path("/data/input")
OUTPUT_DIR = Path("/data/output")

REPTDATE_FILE = INPUT_DIR / "reptdate.parquet"
LOAN_FILE_TEMPLATE = INPUT_DIR / "loan_{month}{week}.parquet"
DISPAY_FILE_TEMPLATE = INPUT_DIR / "idispaymth{month}.parquet"
CCRIS_FILE_TEMPLATE = INPUT_DIR / "credmsubac{month}{year}.parquet"

OUTPUT_REPORT = OUTPUT_DIR / "EIIMPMEF.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# INITIALIZE DUCKDB CONNECTION
# =============================================================================
con = duckdb.connect()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
ASA_NEW_PAGE = "1"
ASA_SINGLE_SPACE = " "
PAGE_LENGTH = 60


def format_number(value: float | int | None, width: int, decimals: int = 0) -> str:
    if value is None:
        return " " * width
    if decimals > 0:
        formatted = f"{float(value):,.{decimals}f}"
    else:
        formatted = f"{int(round(value)):,.0f}"
    return formatted.rjust(width)


@dataclass
class ReportSection:
    title_lines: list[str]
    column_lines: list[str]
    data_lines: list[str]
    total_line: str | None = None


def write_report_section(handle, section: ReportSection) -> None:
    line_count = 0

    def write_line(control: str, text: str = "") -> None:
        nonlocal line_count
        if control == ASA_NEW_PAGE:
            handle.write(f"{ASA_NEW_PAGE}{text}\n")
            line_count = 1
            return
        handle.write(f"{ASA_SINGLE_SPACE}{text}\n")
        line_count += 1

    def ensure_page(lines_needed: int) -> None:
        nonlocal line_count
        if line_count == 0 or line_count + lines_needed > PAGE_LENGTH:
            write_line(ASA_NEW_PAGE, section.title_lines[0])
            for title in section.title_lines[1:]:
                write_line(ASA_SINGLE_SPACE, title)
            write_line(ASA_SINGLE_SPACE, "")
            for header in section.column_lines:
                write_line(ASA_SINGLE_SPACE, header)
            write_line(ASA_SINGLE_SPACE, "")

    for line in section.data_lines:
        ensure_page(1)
        write_line(ASA_SINGLE_SPACE, line)

    if section.total_line:
        ensure_page(1)
        write_line(ASA_SINGLE_SPACE, section.total_line)


# =============================================================================
# STEP 1: READ REPTDATE AND DERIVE MACRO VARIABLES
# =============================================================================
reptdate_row = con.execute(
    f"SELECT REPTDATE FROM read_parquet('{REPTDATE_FILE}') LIMIT 1"
).fetchone()
if not reptdate_row:
    raise ValueError("REPTDATE dataset is empty.")

reptdate = reptdate_row[0]
if isinstance(reptdate, str):
    reptdate = datetime.fromisoformat(reptdate).date()

prev_month = reptdate.month - 1 or 12
reptmon = f"{reptdate.month:02d}"
prevmon = f"{prev_month:02d}"
nowk = "4"
reptyear = f"{reptdate.year % 100:02d}"
rdate = reptdate.strftime("%d/%m/%y")

prev_loan_file = LOAN_FILE_TEMPLATE.with_name(f"loan_{prevmon}{nowk}.parquet")
cur_loan_file = LOAN_FILE_TEMPLATE.with_name(f"loan_{reptmon}{nowk}.parquet")
dispay_file = DISPAY_FILE_TEMPLATE.with_name(f"idispaymth{reptmon}.parquet")
credmsub_file = CCRIS_FILE_TEMPLATE.with_name(f"credmsubac{reptmon}{reptyear}.parquet")


# =============================================================================
# STEP 2: BUILD PREVIOUS AND CURRENT MONTH DATA
# =============================================================================
prvmth = con.execute(
    f"""
    SELECT *
    FROM read_parquet('{prev_loan_file}')
    WHERE (PRODUCT = 428 AND CENSUS IN (428.00, 428.01, 428.02, 428.03))
       OR PRODUCT = 439
    """
).pl()

curmth = con.execute(
    f"""
    SELECT *, 1 AS NOACC
    FROM read_parquet('{cur_loan_file}')
    WHERE PAIDIND NOT IN ('P', 'C')
      AND BALANCE <> 0
      AND ((PRODUCT = 428 AND CENSUS IN (428.00, 428.01, 428.02, 428.03))
           OR PRODUCT = 439)
    """
).pl()


# =============================================================================
# STEP 3: MERGE LOAN DATA (PREVIOUS + CURRENT)
# =============================================================================
loan = prvmth.rename({"BALANCE": "LASTBAL"}).join(
    curmth,
    on=["ACCTNO", "NOTENO"],
    how="full",
    suffix="_cur",
)

loan = loan.with_columns(
    [
        pl.coalesce([pl.col("PRODUCT_cur"), pl.col("PRODUCT")]).alias("PRODUCT"),
        pl.coalesce([pl.col("CENSUS_cur"), pl.col("CENSUS")]).alias("CENSUS"),
    ]
).drop([col for col in ["PRODUCT_cur", "CENSUS_cur"] if col in loan.columns])


# =============================================================================
# STEP 4: LOAD DISBURSEMENT/PAYMENT DATA AND MERGE
# =============================================================================
dispay = con.execute(
    f"""
    SELECT ACCTNO, NOTENO, DISBURSE, REPAID, CENSUS
    FROM read_parquet('{dispay_file}')
    WHERE DISBURSE > 0 OR REPAID > 0
    """
).pl()

loan = loan.join(dispay, on=["ACCTNO", "NOTENO"], how="left")
loan = loan.filter(
    (pl.col("CENSUS").is_in([428.00, 428.01, 428.02, 428.03]))
    | (pl.col("PRODUCT") == 439)
)


# =============================================================================
# STEP 5: SUMMARY FOR FIRST REPORT
# =============================================================================
ln1 = loan.select(
    [
        pl.col("BALANCE").fill_null(0).sum().alias("BALANCE"),
        pl.col("NOACC").fill_null(0).sum().alias("NOACC"),
        pl.col("DISBURSE").fill_null(0).sum().alias("DISBURSE"),
        pl.col("REPAID").fill_null(0).sum().alias("REPAID"),
    ]
)


# =============================================================================
# STEP 6: BUILD LOAN STATUS REPORT DATA
# =============================================================================
credmsub = con.execute(
    f"""
    SELECT
        ACCTNUM AS ACCTNO,
        NOTENO,
        BRANCH,
        DAYSARR AS DAYARR,
        MTHARR
    FROM read_parquet('{credmsub_file}')
    """
).pl()

ln2 = curmth.join(
    credmsub,
    on=["ACCTNO", "NOTENO", "BRANCH"],
    how="left",
)

ln2 = ln2.filter((pl.col("LOANSTAT") == 3) & (pl.col("BALANCE") != 0))


# =============================================================================
# STEP 7: FORMAT REPORT SECTIONS WITH ASA CARRIAGE CONTROL
# =============================================================================
ln1_row = ln1.to_dicts()[0] if len(ln1) > 0 else {}
ln1_values = [
    format_number(ln1_row.get("BALANCE"), 15, 2),
    format_number(ln1_row.get("NOACC"), 10, 0),
    format_number(ln1_row.get("DISBURSE"), 15, 2),
    format_number(ln1_row.get("REPAID"), 15, 2),
]
ln1_line = " ".join(ln1_values)
ln1_total = " ".join(ln1_values)

ln1_section = ReportSection(
    title_lines=[
        "PUBLIC ISLAMIC BANK BERHAD",
        f"PERFORMANCE REPORT ON PRODUCT 428 AND 439 AS AT {rdate}",
        "REPORT ID : EIIMPMEF",
    ],
    column_lines=[
        "O/S BALANCE".center(15)
        + " "
        + "NO OF".center(10)
        + " "
        + "DISBURSEMENT".center(15)
        + " "
        + "REPAYMENT".center(15),
        "".center(15)
        + " "
        + "ACCOUNT".center(10)
        + " "
        + "AMOUNT".center(15)
        + " "
        + "AMOUNT".center(15),
        "-" * (15 + 1 + 10 + 1 + 15 + 1 + 15),
    ],
    data_lines=[ln1_line],
    total_line=ln1_total,
)

ln2_rows = ln2.select(
    ["ACCTNO", "NOTENO", "LOANSTAT", "BALANCE", "DAYARR", "MTHARR"]
).to_dicts()

ln2_data_lines = []
balance_total = 0.0

for row in ln2_rows:
    balance = row.get("BALANCE")
    if balance is not None:
        balance_total += float(balance)
    ln2_data_lines.append(
        f"{str(row.get('ACCTNO', '')):>12} "
        f"{str(row.get('NOTENO', '')):>10} "
        f"{str(row.get('LOANSTAT', '')):>10} "
        f"{format_number(balance, 15, 2)} "
        f"{str(row.get('DAYARR', '')):>12} "
        f"{str(row.get('MTHARR', '')):>13}"
    )

ln2_total_line = (
    f"{'SUM':<12} "
    f"{'':>10} "
    f"{'':>10} "
    f"{format_number(balance_total, 15, 2)} "
    f"{'':>12} "
    f"{'':>13}"
)

ln2_section = ReportSection(
    title_lines=[
        "PUBLIC ISLAMIC BANK BERHAD",
        f"PRODUCT 428 AND 439  WITH LOAN STATUS 3 AS AT {rdate}",
        "REPORT ID : EIIMPMEF",
    ],
    column_lines=[
        "ACCOUNT".center(12)
        + " "
        + "NOTE".center(10)
        + " "
        + "LOAN".center(10)
        + " "
        + "O/S BALANCE".center(15)
        + " "
        + "DAYS IN".center(12)
        + " "
        + "MTHS IN".center(13),
        "NO".center(12)
        + " "
        + "NO".center(10)
        + " "
        + "STATUS".center(10)
        + " "
        + "".center(15)
        + " "
        + "ARREARS".center(12)
        + " "
        + "ARREARS".center(13),
        "-" * (12 + 1 + 10 + 1 + 10 + 1 + 15 + 1 + 12 + 1 + 13),
    ],
    data_lines=ln2_data_lines,
    total_line=ln2_total_line,
)


# =============================================================================
# STEP 8: WRITE REPORT
# =============================================================================
with open(OUTPUT_REPORT, "w", encoding="utf-8") as report_file:
    write_report_section(report_file, ln1_section)
    write_report_section(report_file, ln2_section)
