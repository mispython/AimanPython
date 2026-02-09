#!/usr/bin/env python3
"""
File Name: EIIMPBMC
Performance Report on PB Micro Finance (EIIMPBMC)
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
INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

LOAN_REPTDATE_FILE = INPUT_DIR / "sap_pibb_mniln_0.parquet"

SAS_LOAN_PREV_TEMPLATE = "sap_pibb_sasdata_loan{mon}{nowk}.parquet"
SAS_LOAN_CURR_TEMPLATE = "sap_pibb_sasdata_loan{mon}{nowk}.parquet"
DISPAY_TEMPLATE = "sap_pibb_dispay_idispaymth{mon}.parquet"
CCRIS_TEMPLATE = "sap_pbb_ccris_credsub_credmsubac{mon}{year}.parquet"

OUTPUT_REPORT = OUTPUT_DIR / "EIIMPBMC.txt"


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


def normalize_balance(frame: pl.DataFrame) -> pl.DataFrame:
    if "BAL_AFT_EIR" not in frame.columns:
        return frame
    if "BALANCE" in frame.columns:
        frame = frame.drop("BALANCE")
    frame = frame.rename({"BAL_AFT_EIR": "BALANCE"})
    return frame


# =============================================================================
# STEP 1: READ REPTDATE AND DERIVE MACRO VARIABLES
# =============================================================================
print("Step 1: Reading report date...")

reptdate_row = con.execute(
    f"SELECT REPTDATE FROM '{LOAN_REPTDATE_FILE}' LIMIT 1"
).fetchone()
if not reptdate_row:
    raise ValueError("REPTDATE dataset is empty.")

reptdate = reptdate_row[0]
if isinstance(reptdate, str):
    reptdate = datetime.fromisoformat(reptdate).date()

mm1 = reptdate.month - 1
if mm1 == 0:
    mm1 = 12

NOWK = "4"
REPTMON = f"{reptdate.month:02d}"
PREVMON = f"{mm1:02d}"
REPTYEAR = f"{reptdate.year % 100:02d}"
RDATE = reptdate.strftime("%d/%m/%y")

print(f"Report Date: {reptdate} ({RDATE})")
print(f"Current Month: {REPTMON}, Previous Month: {PREVMON}")

SAS_LOAN_PREV_FILE = INPUT_DIR / SAS_LOAN_PREV_TEMPLATE.format(mon=PREVMON, nowk=NOWK)
SAS_LOAN_CURR_FILE = INPUT_DIR / SAS_LOAN_CURR_TEMPLATE.format(mon=REPTMON, nowk=NOWK)
DISPAY_FILE = INPUT_DIR / DISPAY_TEMPLATE.format(mon=REPTMON)
CCRIS_FILE = INPUT_DIR / CCRIS_TEMPLATE.format(mon=REPTMON, year=REPTYEAR)


# =============================================================================
# STEP 2: BUILD LOAN&REPTMON&NOWK (MERGED LOAN)
# =============================================================================
print("Step 2: Preparing loan base...")

loan_prev = None
if SAS_LOAN_PREV_FILE.exists():
    loan_prev = con.execute(
        f"SELECT * FROM '{SAS_LOAN_PREV_FILE}'"
    ).pl()

loan_curr = con.execute(
    f"SELECT * FROM '{SAS_LOAN_CURR_FILE}'"
).pl()

loan_curr = normalize_balance(loan_curr)

if loan_prev is not None:
    loan_prev = normalize_balance(loan_prev)
    loanx = loan_prev.join(loan_curr, on=["ACCTNO", "NOTENO"], how="outer_coalesce")
else:
    loanx = loan_curr

loanx = loanx.filter(
    ~pl.col("PRODCD").is_in(["34180", "34240"])
).filter(
    ((pl.col("PRODUCT") == 428) & pl.col("CENSUS").is_in([428.00, 428.01, 428.02, 428.03]))
    | (pl.col("PRODUCT") == 439)
)

print(f"Loan base rows: {len(loanx)}")


# =============================================================================
# STEP 3: LOAD DISPAY AND FILTER
# =============================================================================
print("Step 3: Loading disbursement/repayment data...")

dispay = con.execute(
    f"SELECT * FROM '{DISPAY_FILE}'"
).pl()
dispay = dispay.filter((pl.col("DISBURSE") > 0) | (pl.col("REPAID") > 0))


# =============================================================================
# STEP 4: MERGE DISPAY WITH LOAN BASE (INNER)
# =============================================================================
print("Step 4: Matching dispay to loan base...")

dispay = loanx.join(dispay, on=["ACCTNO", "NOTENO"], how="inner")
dispay = dispay.select(
    [
        "ACCTNO",
        "NOTENO",
        "FISSPURP",
        "PRODUCT",
        "PRODCD",
        "CUSTCD",
        "AMTIND",
        "SECTORCD",
        "DISBURSE",
        "REPAID",
        "BRANCH",
        "ACCTYPE",
    ]
)


# =============================================================================
# STEP 5: BUILD CURMTH
# =============================================================================
print("Step 5: Building current month dataset...")

curmth = loan_curr.filter(
    (~pl.col("PAIDIND").is_in(["P", "C"])) | (pl.col("EIR_ADJ").is_not_null())
).filter(
    ((pl.col("PRODUCT") == 428) & pl.col("CENSUS").is_in([428.00, 428.01, 428.02, 428.03]))
    | (pl.col("PRODUCT") == 439)
)

curmth = curmth.with_columns(
    pl.when((pl.col("BALANCE").is_not_null()) & (pl.col("BALANCE") != 0))
    .then(pl.lit(1))
    .otherwise(pl.lit(0))
    .alias("NOACC")
)


# =============================================================================
# STEP 6: MERGE CURMTH WITH DISPAY AND SUMMARIZE
# =============================================================================
print("Step 6: Summarizing balances and movements...")

loan = curmth.join(dispay, on=["ACCTNO", "NOTENO"], how="left")

ln1 = loan.select(
    [
        pl.col("BALANCE"),
        pl.col("NOACC"),
        pl.col("DISBURSE"),
        pl.col("REPAID"),
    ]
).sum()


# =============================================================================
# STEP 7: LOAD CCRIS AND BUILD LN2
# =============================================================================
print("Step 7: Building LN2 dataset...")

credmsub = con.execute(
    f"SELECT ACCTNUM, NOTENO, BRANCH, DAYSARR, MTHARR FROM '{CCRIS_FILE}'"
).pl()
credmsub = credmsub.rename({"ACCTNUM": "ACCTNO", "DAYSARR": "DAYARR"})

ln2 = curmth.join(credmsub, on=["ACCTNO", "NOTENO", "BRANCH"], how="left")
ln2 = ln2.filter((pl.col("LOANSTAT") == 3) & (pl.col("BALANCE") != 0))


# =============================================================================
# STEP 8: FORMAT REPORT SECTIONS WITH ASA CARRIAGE CONTROL
# =============================================================================
print("Step 8: Generating report...")

OUTPUT_REPORT.parent.mkdir(parents=True, exist_ok=True)

# Section 1: Summary
ln1_row = ln1.to_dicts()[0] if len(ln1) > 0 else {}

ln1_values = [
    format_number(ln1_row.get("BALANCE"), 15, 2),
    format_number(ln1_row.get("NOACC"), 10, 0),
    format_number(ln1_row.get("DISBURSE"), 15, 2),
    format_number(ln1_row.get("REPAID"), 15, 2),
]
ln1_line = " ".join(ln1_values)

ln1_total = " ".join(
    [
        format_number(ln1_row.get("BALANCE"), 15, 2),
        format_number(ln1_row.get("NOACC"), 10, 0),
        format_number(ln1_row.get("DISBURSE"), 15, 2),
        format_number(ln1_row.get("REPAID"), 15, 2),
    ]
)

ln1_section = ReportSection(
    title_lines=[
        "PUBLIC ISLAMIC BANK BERHAD",
        f"PERFORMANCE REPORT ON PB MICRO FINANCE AS AT {RDATE}",
        "REPORT ID : EIIMPBMC",
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

# Section 2: Loan status details
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
        f"PB MICRO A/C WITH LOAN STATUS 3 4 AS AT {RDATE}",
        "REPORT ID : EIIMPBMC",
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

with open(OUTPUT_REPORT, "w", encoding="utf-8") as report_file:
    write_report_section(report_file, ln1_section)
    write_report_section(report_file, ln2_section)

print(f"Report generated: {OUTPUT_REPORT}")
