#!/usr/bin/env python3
"""
File Name: EIIMNL42
BNM Liquidity Framework - GL Consolidation
Processes cash holdings and SRR from Walker data
Consolidates all BNM liquidity data
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import polars as pl


# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Input paths
BNM_REPTDATE_PATH = "/data/input/bnm_reptdate.parquet"
WALK_PATH = "/data/input/walk.parquet"
BNM_NOTE_PATH = "/data/input/bnm_note.parquet"
BNM_CALC_PATH = "/data/input/bnm_calc.parquet"

# Output paths
OUTPUT_DIR = "/data/output"
BNM_GLSET_PATH = f"{OUTPUT_DIR}/bnm_glset.parquet"
BNM_FINAL_PATH = f"{OUTPUT_DIR}/bnm_final.parquet"
BNM_FINALSUM_PATH = f"{OUTPUT_DIR}/bnm_finalsum.parquet"

REPORT_REPTDATE_PATH = f"{OUTPUT_DIR}/reptdate_report.txt"
REPORT_CASH_PATH = f"{OUTPUT_DIR}/cash_holdings_report.txt"
REPORT_SRR_PATH = f"{OUTPUT_DIR}/srr_report.txt"
REPORT_GLSET_PATH = f"{OUTPUT_DIR}/glset_report.txt"

# Create output directory
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def read_parquet(path: str) -> pl.DataFrame:
    """Read parquet using DuckDB."""
    return duckdb.query(f"SELECT * FROM read_parquet('{path}')").pl()


def to_date(value) -> date:
    """Convert a value to a date if possible."""
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError(f"Unsupported date value: {value!r}")


def sas_date_number(value: date) -> int:
    """Convert a date to SAS date number (days since 1960-01-01)."""
    return (value - date(1960, 1, 1)).days


# ============================================================================
# DATE PROCESSING
# ============================================================================

def get_report_dates() -> dict:
    """Get report dates and calculate week number."""
    reptdate_df = read_parquet(BNM_REPTDATE_PATH)
    reptdate = to_date(reptdate_df.select("REPTDATE").item(0, 0))

    day = reptdate.day
    if day == 8:
        wk = "1"
    elif day == 15:
        wk = "2"
    elif day == 22:
        wk = "3"
    else:
        wk = "0"

    if reptdate.month == 12:
        sreptdate = date(reptdate.year + 1, 1, 1)
    else:
        sreptdate = date(reptdate.year, reptdate.month + 1, 1)

    prvrptdate = sreptdate - timedelta(days=1)
    if reptdate == prvrptdate:
        wk = "4"

    return {
        "reptdate": reptdate,
        "nowk": wk,
        "rdate": reptdate.strftime("%d/%m/%y"),
        "rdat1": sas_date_number(reptdate),
        "reptmon": f"{reptdate.month:02d}",
        "reptday": f"{reptdate.day:02d}",
    }


dates = get_report_dates()
REPTDATE = dates["reptdate"]
NOWK = dates["nowk"]
RDATE = dates["rdate"]
RDAT1 = dates["rdat1"]
REPTMON = dates["reptmon"]
REPTDAY = dates["reptday"]

print(f"Report Date: {RDATE}")
print(f"Week Number: {NOWK}")
print(f"RDAT1 (SAS date): {RDAT1}")


# ============================================================================
# READ WALKER DATA
# ============================================================================

print("\nStep 1: Reading Walker data...")

walk_df = read_parquet(WALK_PATH)

if "REPTDATE" in walk_df.columns:
    reptdate_col = walk_df["REPTDATE"]
    if reptdate_col.dtype in (pl.Date, pl.Datetime):
        walk_df = walk_df.with_columns(pl.col("REPTDATE").cast(pl.Date))
        walk_df = walk_df.filter(pl.col("REPTDATE") == pl.lit(REPTDATE))
    elif reptdate_col.dtype in (pl.Int64, pl.Int32, pl.Float64, pl.Float32):
        walk_df = walk_df.filter(pl.col("REPTDATE") == pl.lit(RDAT1))
    else:
        walk_df = walk_df.with_columns(
            pl.col("REPTDATE")
            .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            .alias("REPTDATE")
        ).filter(pl.col("REPTDATE") == pl.lit(REPTDATE))
else:
    walk_df = walk_df.with_columns(
        pl.when(pl.col("YY") >= 50)
        .then(pl.col("YY") + 1900)
        .otherwise(pl.col("YY") + 2000)
        .alias("YEAR")
    ).with_columns(
        pl.date(pl.col("YEAR"), pl.col("MM"), pl.col("DD")).alias("REPTDATE")
    ).filter(pl.col("REPTDATE") == pl.lit(REPTDATE))

walk_df = walk_df.select(["DESC", "REPTDATE", "AMOUNT"])
print(f"  Walker records loaded for {RDATE}: {len(walk_df)}")


# ============================================================================
# CASH HOLDINGS
# ============================================================================

print("\nStep 2: Processing Cash Holdings...")

cash_walk = walk_df.filter(pl.col("DESC") == "39110")
cash_amount = None
if len(cash_walk) > 0:
    cash_amount = cash_walk.select(pl.col("AMOUNT").sum()).item(0, 0)
    print(f"  Cash Holdings amount: {cash_amount:,.2f}")

if cash_amount is None:
    cash = pl.DataFrame({"BNMCODE": [], "AMOUNT": []})
else:
    cash = pl.DataFrame(
        [
            {"BNMCODE": "9322100010000Y", "AMOUNT": cash_amount},
            {"BNMCODE": "9522100010000Y", "AMOUNT": cash_amount},
        ]
    )

print(f"  Cash Holdings records: {len(cash)}")


# ============================================================================
# SRR (Statutory Reserve Requirement)
# ============================================================================

print("\nStep 3: Processing SRR...")

srr_default = pl.DataFrame(
    [
        {"BNMCODE": f"93222000{n}0000Y", "AMOUNT": 0.0} for n in range(1, 7)
    ]
    + [{"BNMCODE": f"95222000{n}0000Y", "AMOUNT": 0.0} for n in range(1, 7)]
).sort("BNMCODE")

srr_walk = walk_df.filter(pl.col("DESC") == "32110")
srr_amount = None
if len(srr_walk) > 0:
    srr_amount = srr_walk.select(pl.col("AMOUNT").sum()).item(0, 0)
    print(f"  SRR amount: {srr_amount:,.2f}")

srr = srr_default.with_columns(
    pl.when(
        pl.col("BNMCODE").is_in(["9322200060000Y", "9522200060000Y"])
        & pl.lit(srr_amount is not None)
    )
    .then(pl.lit(srr_amount if srr_amount is not None else 0.0))
    .otherwise(pl.col("AMOUNT"))
    .alias("AMOUNT")
)

print(f"  SRR records: {len(srr)}")


# ============================================================================
# CREATE GLSET
# ============================================================================

print("\nStep 4: Creating GLSET...")

bnm_glset = pl.concat([cash, srr])
bnm_glset.write_parquet(BNM_GLSET_PATH)
print(f"  GLSET records: {len(bnm_glset)}")
print(f"  Saved to: {BNM_GLSET_PATH}")


# ============================================================================
# CONSOLIDATION
# ============================================================================

print("\nStep 5: Consolidating all BNM data...")

bnm_note = read_parquet(BNM_NOTE_PATH)
bnm_calc = read_parquet(BNM_CALC_PATH)

print(f"  BNM NOTE records: {len(bnm_note)}")
print(f"  BNM CALC records: {len(bnm_calc)}")
print(f"  BNM GLSET records: {len(bnm_glset)}")

bnm_final = pl.concat([bnm_note, bnm_calc, bnm_glset]).sort("BNMCODE")
print(f"  BNM FINAL records (before summarizing): {len(bnm_final)}")

bnm_final.write_parquet(BNM_FINAL_PATH)
print(f"  Saved to: {BNM_FINAL_PATH}")


# ============================================================================
# SUMMARIZE BY BNMCODE
# ============================================================================

print("\nStep 6: Summarizing by BNMCODE...")

bnm_finalsum = (
    bnm_final.group_by("BNMCODE")
    .agg(pl.col("AMOUNT").sum().alias("AMOUNT"))
    .sort("BNMCODE")
)

print(f"  BNM FINALSUM records: {len(bnm_finalsum)}")
bnm_finalsum.write_parquet(BNM_FINALSUM_PATH)
print(f"  Saved to: {BNM_FINALSUM_PATH}")


# ============================================================================
# GENERATE REPORTS (ASA CARRIAGE CONTROL)
# ============================================================================

print("\nStep 7: Generating reports...")


def write_report_with_asa(filepath: str, title: str, data_df: pl.DataFrame) -> None:
    """Write a report with ASA carriage control characters."""

    page_length = 60
    line_count = 0

    with open(filepath, "w") as report_file:

        def write_line(asa_char: str, content: str) -> None:
            nonlocal line_count
            report_file.write(f"{asa_char}{content}\n")
            line_count += 1

        def write_header() -> None:
            nonlocal line_count
            write_line("1", " " * 100)
            write_line(" ", title.center(100))
            write_line(" ", RDATE.center(100))
            write_line(" ", " " * 100)
            write_line(" ", f"{'BNMCODE':<20} {'AMOUNT':>20}")
            write_line(" ", "-" * 100)
            line_count = 6

        write_header()

        for row in data_df.iter_rows(named=True):
            if line_count >= page_length - 3:
                write_header()

            line = f" {row['BNMCODE']:<20} {row['AMOUNT']:20,.2f}"
            write_line(" ", line)

        total_amount = data_df.select(pl.col("AMOUNT").sum()).item(0, 0) if len(data_df) else 0.0
        write_line(" ", "-" * 100)
        write_line(" ", f" {'TOTAL':<20} {total_amount:20,.2f}")

    print(f"    Report written to: {filepath}")


def write_reptdate_report(filepath: str) -> None:
    """Write REPTDATE report with ASA carriage control characters."""
    with open(filepath, "w") as report_file:
        report_file.write(f"1{' ' * 100}\n")
        report_file.write(f" {'REPTDATE'.center(100)}\n")
        report_file.write(f" {RDATE.center(100)}\n")
        report_file.write(f" {' ' * 100}\n")
        report_file.write(f" {'REPTDATE':<15} {'NOWK':<5} {'REPTMON':<7} {'REPTDAY':<7}\n")
        report_file.write(f" {'-' * 100}\n")
        report_file.write(
            f" {REPTDATE.strftime('%Y-%m-%d'):<15} {NOWK:<5} {REPTMON:<7} {REPTDAY:<7}\n"
        )
    print(f"    Report written to: {filepath}")


write_reptdate_report(REPORT_REPTDATE_PATH)
write_report_with_asa(REPORT_CASH_PATH, "CASH HOLDINGS", cash)
write_report_with_asa(REPORT_SRR_PATH, "SRR", srr)
write_report_with_asa(REPORT_GLSET_PATH, "GLSET", bnm_glset)


# ============================================================================
# SUMMARY STATISTICS
# ============================================================================

print("\n" + "=" * 70)
print("BNM GL Consolidation completed successfully!")
print("=" * 70)

cash_total = cash.select(pl.col("AMOUNT").sum()).item(0, 0) if len(cash) else 0.0
srr_total = srr.select(pl.col("AMOUNT").sum()).item(0, 0)
final_total = bnm_finalsum.select(pl.col("AMOUNT").sum()).item(0, 0)

print("\nSummary Statistics:")
print(f"  Cash Holdings total: {cash_total:,.2f}")
print(f"  SRR total: {srr_total:,.2f}")
print(f"  GL Set total: {cash_total + srr_total:,.2f}")
print(f"  Final consolidated total: {final_total:,.2f}")

print("\nRecord Counts:")
print(f"  BNM NOTE: {len(bnm_note)}")
print(f"  BNM CALC: {len(bnm_calc)}")
print(f"  BNM GLSET: {len(bnm_glset)}")
print(f"  BNM FINAL (before sum): {len(bnm_final)}")
print(f"  BNM FINALSUM (after sum): {len(bnm_finalsum)}")

print("\nOutput files:")
print(f"  - GLSET: {BNM_GLSET_PATH}")
print(f"  - FINAL: {BNM_FINAL_PATH}")
print(f"  - FINALSUM: {BNM_FINALSUM_PATH}")
print(f"  - REPTDATE Report: {REPORT_REPTDATE_PATH}")
print(f"  - Cash Holdings Report: {REPORT_CASH_PATH}")
print(f"  - SRR Report: {REPORT_SRR_PATH}")
print(f"  - GLSET Report: {REPORT_GLSET_PATH}")
