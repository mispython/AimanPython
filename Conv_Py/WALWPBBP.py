#!/usr/bin/env python3
"""
File Name: WALWPBBP
RDAL Part I (WALKER ITEMS) Report
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import duckdb
import polars as pl
from decimal import Decimal

# =====================================================
# Configuration and Path Setup
# =====================================================

# Define base paths
DATA_DIR = Path("data")
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
PARQUET_DIR = INPUT_DIR / "parquet"

# Create directories if they don't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input files (parquet format)
R1R115_FILE = PARQUET_DIR / "alw_r1r115.parquet"
R1R913_FILE = PARQUET_DIR / "alw_r1r913.parquet"
R1GL4000_FILE = PARQUET_DIR / "alw_r1gl4000.parquet"
R1GL4100_FILE = PARQUET_DIR / "alw_r1gl4100.parquet"
WALW_FILE = PARQUET_DIR / "walw_formats.parquet"
DAYITEM_FILE = PARQUET_DIR / "dayitem.parquet"

# Output files
WALW_OUTPUT_FILE = OUTPUT_DIR / "walw_output.txt"


# =====================================================
# Format Definitions
# =====================================================

def apply_banktype_format(branch: int) -> str:
    """Apply BANKTYPE format to branch code"""
    if branch <= 2999:
        return 'D'
    elif branch <= 3999:
        return 'I'
    else:
        return 'D'


# =====================================================
# Data Processing Functions
# =====================================================

def process_branch_code(brno: str) -> str:
    """Transform branch number based on first character"""
    if not brno or len(brno) < 1:
        return brno

    first_char = brno[0]
    if first_char == 'H':
        remainder = brno[1:4]
        return '0' + remainder
    elif first_char == 'I':
        remainder = brno[1:4]
        return '3' + remainder
    else:
        return brno


def calculate_week_number(reptday: int) -> str:
    """Calculate week number based on day of month"""
    if reptday <= 8:
        return '1'
    elif reptday <= 15:
        return '2'
    elif reptday <= 22:
        return '3'
    else:
        return '4'


def apply_usercd_logic(usercd: str, curbal: float) -> float:
    """Apply user code logic to balance"""
    if usercd == '6666':
        return max(0, curbal)
    elif usercd == '7777':
        return min(0, curbal)
    elif usercd == '8888':
        if curbal > 0:
            return 0
        else:
            return abs(curbal)
    elif usercd == '9999':
        return -curbal
    else:
        return curbal


# =====================================================
# Main Processing Steps
# =====================================================

def step1_join_datasets():
    """Step 1: Create R1R913 by joining R1R115 and R1R913 tables"""

    df_r1r115 = pl.read_parquet(R1R115_FILE)
    df_r1r913 = pl.read_parquet(R1R913_FILE)

    # SQL JOIN: SELECT T1.SET_ID, T1.USERCD, T2.ACCT_NO FROM R1R115 T1, R1R913 T2
    # WHERE T1.SET_ID = T2.SET_ID
    result = df_r1r115.join(
        df_r1r913,
        left_on="SET_ID",
        right_on="SET_ID",
        how="inner"
    ).select(["SET_ID", "USERCD", "ACCT_NO"])

    return result


def step2_process_gl_data(r1r913_df):
    """Step 2: Process GL (General Ledger) data - ALWGL tables"""

    df_r1gl4000 = pl.read_parquet(R1GL4000_FILE)

    # JOIN with GL4000
    alwgl = r1r913_df.join(
        df_r1gl4000,
        left_on="ACCT_NO",
        right_on="ACCT_NO",
        how="inner"
    ).filter(pl.col("ACKIND") == 'N').select([
        "SET_ID", "USERCD", "ACCT_NO", "GLAMT"
    ])

    # Extract branch number
    alwgl = alwgl.with_columns(
        pl.col("ACCT_NO").str.slice(0, 4).alias("BRNO")
    )

    # Process branch codes
    alwgl = alwgl.with_columns(
        pl.col("BRNO").map_elements(process_branch_code, return_dtype=pl.Utf8).alias("BRNO_PROC")
    ).drop("BRNO").rename({"BRNO_PROC": "BRNO"})

    # Summary by CLASS
    alwgs = alwgl.group_by(["BRNO", "SET_ID", "USERCD", "ACCT_NO"]).agg(
        pl.col("GLAMT").sum().alias("GLAMT")
    )

    return alwgs, alwgl


def step3_process_je_data(r1r913_df):
    """Step 3: Process JE (Journal Entry) data - ALWJE tables"""

    df_r1gl4100 = pl.read_parquet(R1GL4100_FILE)

    # JOIN with GL4100
    alwje = r1r913_df.join(
        df_r1gl4100,
        left_on="ACCT_NO",
        right_on="ACCT_NO",
        how="inner"
    ).filter(pl.col("ACKIND") == 'N').select([
        "SET_ID", "USERCD", "ACCT_NO", "JEAMT", "EFFDATE"
    ])

    # Extract branch number
    alwje = alwje.with_columns(
        pl.col("ACCT_NO").str.slice(0, 4).alias("BRNO")
    )

    # Process branch codes
    alwje = alwje.with_columns(
        pl.col("BRNO").map_elements(process_branch_code, return_dtype=pl.Utf8).alias("BRNO_PROC")
    ).drop("BRNO").rename({"BRNO_PROC": "BRNO"})

    # Summary by CLASS
    alwjs = alwje.group_by(["BRNO", "SET_ID", "USERCD", "ACCT_NO"]).agg(
        pl.col("JEAMT").sum().alias("JEAMT")
    )

    return alwjs, alwje


def step4_merge_and_calculate(alwgs_df, alwjs_df, format_df):
    """Step 4: Merge GL and JE summaries, apply logic"""

    # Merge GL and JE by BRNO, SET_ID, USERCD, ACCT_NO
    merged = alwgs_df.join(
        alwjs_df,
        on=["BRNO", "SET_ID", "USERCD", "ACCT_NO"],
        how="outer"
    ).fill_null(0)

    # Calculate CURBAL
    merged = merged.with_columns(
        (pl.col("GLAMT") + pl.col("JEAMT")).alias("CURBAL")
    )

    # Apply user code logic
    def apply_logic(row):
        return apply_usercd_logic(row["USERCD"], row["CURBAL"])

    merged = merged.with_columns(
        pl.struct(["USERCD", "CURBAL"]).map_elements(
            apply_logic, return_dtype=pl.Float64
        ).alias("CURBAL_ADJ")
    ).drop("CURBAL").rename({"CURBAL_ADJ": "CURBAL"})

    # Convert BRNO to numeric BRANCH
    merged = merged.with_columns(
        pl.col("BRNO").str.to_integer().alias("BRANCH")
    )

    # Apply BANKTYPE format
    merged = merged.with_columns(
        pl.col("BRANCH").map_elements(apply_banktype_format, return_dtype=pl.Utf8).alias("AMTIND")
    )

    # Join with format table to get BNMCODE
    merged = merged.join(
        format_df.select(["SET_ID", "BNMCODE"]),
        on="SET_ID",
        how="left"
    )

    # Process cumulative amount by SET_ID
    merged = merged.with_columns(
        pl.col("CURBAL").sum().over(["SET_ID"]).alias("AMOUNT")
    )

    # Filter to output records
    output = merged.select([
        "BRANCH", "BNMCODE", "SET_ID", "AMOUNT", "USERCD", "AMTIND"
    ]).distinct()

    return output


def step5_final_summary(w1al_df):
    """Step 5: Final summary by BNMCODE and AMTIND"""

    walw_summary = w1al_df.group_by(["BNMCODE", "AMTIND"]).agg(
        pl.col("AMOUNT").sum().alias("AMOUNT")
    )

    # Apply special transformations
    walw_summary = walw_summary.with_columns(
        pl.when(pl.col("BNMCODE") == '4269982000000Y')
        .then(pl.lit('4269981000000Y'))
        .otherwise(pl.col("BNMCODE"))
        .alias("BNMCODE_PROC")
    ).drop("BNMCODE").rename({"BNMCODE_PROC": "BNMCODE"})

    # Apply criteria from LWU's FAX dated 30-9-1999
    records = []
    grouped = walw_summary.group_by("BNMCODE").agg(
        pl.col("AMOUNT").sum().alias("AMOUNT"),
        pl.col("AMTIND").first().alias("AMTIND")
    )

    for row in grouped.to_dicts():
        bnmcode = row["BNMCODE"]
        amount = row["AMOUNT"]
        amtind = row["AMTIND"]

        if bnmcode in ('3314013000000Y', '3314017000000Y'):
            records.append({
                "BNMCODE": bnmcode,
                "AMTIND": amtind,
                "AMOUNT": amount
            })
        else:
            records.append({
                "BNMCODE": bnmcode,
                "AMTIND": amtind,
                "AMOUNT": amount
            })

    # Add special calculated records
    amt1 = walw_summary.filter(
        pl.col("BNMCODE").is_in(['3314013000000Y', '3314017000000Y'])
    )["AMOUNT"].sum()

    amt2 = walw_summary.filter(
        pl.col("BNMCODE").is_in(['4314013000000Y', '4314017000000Y'])
    )["AMOUNT"].sum()

    if amt1 > 0:
        records.append({
            "BNMCODE": '3314020X00000Y',
            "AMTIND": 'D',
            "AMOUNT": amt1
        })

    if amt2 > 0:
        records.append({
            "BNMCODE": '4314020000000Y',
            "AMTIND": 'D',
            "AMOUNT": amt2
        })

    return pl.DataFrame(records).sort_by(["BNMCODE", "AMTIND"])


def step6_daily_items(alwje_df, dayitem_df):
    """Step 6: Process daily items (DAYITEM)"""

    # Merge with DAYITEM
    alwjd = alwje_df.join(
        dayitem_df,
        left_on="SET_ID",
        right_on="SET_ID",
        how="inner"
    ).select(alwje_df.columns)

    # Aggregate by day
    alwjd_summary = alwjd.group_by([
        "BRNO", "SET_ID", "USERCD", "ACCT_NO", "EFFDATE"
    ]).agg(
        pl.col("JEAMT").sum().alias("JEAMT")
    )

    return alwjd_summary


def step7_daily_summary(alwgs_df, alwjd_df, reptday: int):
    """Step 7: Create daily summary table with DAY1-DAY9 columns"""

    # Define day thresholds based on REPTDAY
    day_mappings = {
        8: [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)],
        15: [(9, 1), (10, 2), (11, 3), (12, 4), (13, 5), (14, 6), (15, 7)],
        22: [(16, 1), (17, 2), (18, 3), (19, 4), (20, 5), (21, 6), (22, 7)],
        28: [(23, 1), (24, 2), (25, 3), (26, 4), (27, 5), (28, 6)],
        29: [(23, 1), (24, 2), (25, 3), (26, 4), (27, 5), (28, 6), (29, 7)],
        30: [(23, 1), (24, 2), (25, 3), (26, 4), (27, 5), (28, 6), (29, 7), (30, 8)],
        31: [(23, 1), (24, 2), (25, 3), (26, 4), (27, 5), (28, 6), (29, 7), (30, 8), (31, 9)]
    }

    mappings = day_mappings.get(reptday, [])

    # Process daily items
    alwjd_pivot = alwjd_df.with_columns(
        pl.col("EFFDATE").dt.day().alias("DAY_OF_MONTH")
    )

    # Create day columns
    for day_threshold, day_num in mappings:
        alwjd_pivot = alwjd_pivot.with_columns(
            pl.when(pl.col("DAY_OF_MONTH") <= day_threshold)
            .then(pl.col("JEAMT"))
            .otherwise(0)
            .alias(f"DAY{day_num}")
        )

    # Merge with GL data
    alwds = alwgs_df.join(
        alwjd_pivot.select(["BRNO", "SET_ID", "USERCD", "ACCT_NO"] +
                           [f"DAY{i}" for i in range(1, 10)]),
        on=["BRNO", "SET_ID", "USERCD", "ACCT_NO"],
        how="left"
    ).fill_null(0)

    # Add GLAMT to daily totals
    for i in range(1, 10):
        alwds = alwds.with_columns(
            (pl.col(f"DAY{i}") + pl.col("GLAMT")).alias(f"TDAY{i}")
        )

    return alwds


def format_report_output(data_df, page_length: int = 60) -> str:
    """Format output as a report with ASA carriage control and page breaks"""

    lines = []

    # Report header with ASA carriage control
    lines.append("1" + " " * 79 + "REPORT ON DOMESTIC ASSETS AND LIABILITIES PART I - WALKER")
    lines.append("0" + " " * 79 + "REPORT DATE : " + datetime.now().strftime("%d/%m/%Y"))
    lines.append(" ")

    # Column headers
    header = "0" + f"{'BNMCODE':<20} {'AMTIND':<10} {'AMOUNT':>25}"
    lines.append(header)
    lines.append(" " * 80)

    line_count = 5

    # Data rows
    for row in data_df.to_dicts():
        bnmcode = row.get("BNMCODE", "")
        amtind = row.get("AMTIND", "")
        amount = row.get("AMOUNT", 0)

        # Skip SSTS and NSSTS records
        if bnmcode in ('SSTS', 'NSSTS'):
            continue

        # Format amount with comma separator
        formatted_amount = f"{amount:,.2f}"

        data_line = " " + f"{bnmcode:<20} {amtind:<10} {formatted_amount:>25}"
        lines.append(data_line)
        line_count += 1

        # Page break handling
        if line_count >= page_length - 5:
            lines.append("\n")
            line_count = 0

    return "\n".join(lines)


def main():
    """Main processing function"""

    print("Starting WALWPBBP Report Processing...")

    try:
        # Read format file
        format_df = pl.read_parquet(WALW_FILE)

        # Step 1: Join datasets
        print("Step 1: Joining R1R115 and R1R913...")
        r1r913_df = step1_join_datasets()

        # Step 2: Process GL data
        print("Step 2: Processing GL data...")
        alwgs_df, alwgl_df = step2_process_gl_data(r1r913_df)

        # Step 3: Process JE data
        print("Step 3: Processing JE data...")
        alwjs_df, alwje_df = step3_process_je_data(r1r913_df)

        # Step 4: Merge and calculate
        print("Step 4: Merging and calculating...")
        w1al_df = step4_merge_and_calculate(alwgs_df, alwjs_df, format_df)

        # Step 5: Final summary
        print("Step 5: Creating final summary...")
        walw_final = step5_final_summary(w1al_df)

        # Generate report
        print("Generating report...")
        report_text = format_report_output(walw_final)

        # Write output
        with open(WALW_OUTPUT_FILE, 'w') as f:
            f.write(report_text)

        print(f"Report successfully written to {WALW_OUTPUT_FILE}")

        # Also save data in CSV format for verification
        csv_output = OUTPUT_DIR / "walw_data.csv"
        walw_final.write_csv(csv_output)
        print(f"Data also saved to {csv_output}")

    except FileNotFoundError as e:
        print(f"Error: Input file not found - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error during processing: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
