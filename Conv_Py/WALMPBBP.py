#!/usr/bin/env python3
"""
Program: WALMPBBP
Processes Walker items for RDAL Part II report
Joins datasets, applies transformations, and generates formatted report
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime

# Setup paths
# INPUT_DIR = Path("input")
# OUTPUT_DIR = Path("output")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

ALM_DIR = INPUT_DIR / "alm"
BNM_DIR = OUTPUT_DIR / "bnm"

# Create directories if they don't exist
INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
ALM_DIR.mkdir(exist_ok=True)
BNM_DIR.mkdir(exist_ok=True)

# Define macro variables (these would typically come from environment or config)
REPTMON = "01"
NOWK = "4"
SDESC = "BANK SAMPLE"
RDATE = "15/01/2025"

# Define output file paths
ALMGL_FILE = BNM_DIR / f"ALMGL{REPTMON}{NOWK}.parquet"
W2AL_FILE = BNM_DIR / f"W2AL{REPTMON}{NOWK}.parquet"
WALM_OUTPUT_FILE = BNM_DIR / f"WALM{REPTMON}{NOWK}.parquet"
REPORT_FILE = OUTPUT_DIR / f"WALM{REPTMON}{NOWK}_REPORT.txt"

# Initialize DuckDB connection
conn = duckdb.connect()


def load_parquet(filepath):
    """Load parquet file using DuckDB and return Polars DataFrame"""
    return conn.execute(f"SELECT * FROM '{filepath}'").pl()


# Load input datasets
print("Loading input datasets...")
r2r115 = load_parquet(ALM_DIR / "R2R115.parquet")
r2r913 = load_parquet(ALM_DIR / "R2R913.parquet")
r2gl4000 = load_parquet(ALM_DIR / "R2GL4000.parquet")
walm_format = load_parquet(INPUT_DIR / "WALM.parquet")

# SQL Join 1: Create R2R913 with SET_ID, USERCD, ACCT_NO
print("Creating R2R913 joined dataset...")
r2r913_joined = r2r115.join(
    r2r913,
    on="SET_ID",
    how="inner"
).select(["SET_ID", "USERCD", "ACCT_NO"])

# SQL Join 2: Create ALMGL dataset
print("Creating ALMGL dataset...")
almgl = r2r913_joined.join(
    r2gl4000.filter(pl.col("ACKIND") == "N"),
    on="ACCT_NO",
    how="inner"
).with_columns([
    pl.col("ACCT_NO").str.slice(0, 4).alias("BRNO")
]).select(["SET_ID", "USERCD", "ACCT_NO", "GLAMT", "BRNO"])

# Sort by BRNO and SET_ID
almgl = almgl.sort(["BRNO", "SET_ID"])

# Save ALMGL
almgl.write_parquet(ALMGL_FILE)
print(f"Saved {ALMGL_FILE}")

# Process W2AL dataset with USERCD transformations
print("Processing W2AL dataset...")

# Create BNMCODE lookup from WALM format
bnmcode_lookup = walm_format.select([
    pl.col("START").alias("SET_ID"),
    pl.col("LABEL").alias("BNMCODE")
])

# Group by BRNO and SET_ID, apply USERCD logic
w2al_data = []

for (brno, set_id), group_df in almgl.group_by(["BRNO", "SET_ID"]):
    tglamt = 0.0
    usercd = group_df["USERCD"][0]

    for row in group_df.iter_rows(named=True):
        glamt = row["GLAMT"]

        # Apply USERCD transformations
        if usercd == "6666":
            if glamt < 0:
                glamt = 0
        elif usercd == "7777":
            if glamt > 0:
                glamt = 0
        elif usercd == "8888":
            if glamt > 0:
                glamt = 0
            else:
                glamt = (-1) * glamt
        elif usercd == "9999":
            glamt = (-1) * glamt

        tglamt += glamt

    # Convert BRNO to integer
    try:
        branch = int(brno)
    except:
        branch = 0

    # Get BNMCODE from format
    bnmcode_row = bnmcode_lookup.filter(pl.col("SET_ID") == set_id)
    if len(bnmcode_row) > 0:
        bnmcode = bnmcode_row["BNMCODE"][0]
    else:
        bnmcode = ""

    w2al_data.append({
        "BRANCH": branch,
        "SET_ID": set_id,
        "BNMCODE": bnmcode,
        "AMOUNT": tglamt
    })

w2al = pl.DataFrame(w2al_data)

# Save W2AL
w2al.write_parquet(W2AL_FILE)
print(f"Saved {W2AL_FILE}")

# Summarize by BNMCODE
print("Summarizing by BNMCODE...")
walm_summary = w2al.group_by("BNMCODE").agg([
    pl.col("AMOUNT").sum().alias("AMOUNT")
])

# Get unique BNMCODE list from WALM format
walm_bnmcodes = walm_format.select([
    pl.col("LABEL").alias("BNMCODE")
]).unique().sort("BNMCODE")

# Merge to ensure all BNMCODEs are present
walm_final = walm_bnmcodes.join(
    walm_summary,
    on="BNMCODE",
    how="left"
).with_columns([
    pl.col("AMOUNT").fill_null(0.0)
])

# Save final WALM dataset
walm_final.write_parquet(WALM_OUTPUT_FILE)
print(f"Saved {WALM_OUTPUT_FILE}")

# Generate report with ASA carriage control characters
print("Generating report...")

PAGE_LENGTH = 60
lines_on_page = 0


def write_line(f, carriage_control, text):
    """Write line with ASA carriage control character"""
    f.write(f"{carriage_control}{text}\n")


with open(REPORT_FILE, 'w') as f:
    # Title lines (new page)
    write_line(f, "1", f"{SDESC:^132}")
    write_line(f, " ", "REPORT ON DOMESTIC ASSETS AND LIABILITIES PART II - WALKER".center(132))
    write_line(f, " ", f"REPORT DATE : {RDATE}".center(132))
    write_line(f, " ", "")
    lines_on_page = 4

    # Column headers
    write_line(f, " ", "")
    write_line(f, " ", f"{'Obs':>5}    {'BNMCODE':<20}    {'AMOUNT':>25}")
    write_line(f, " ", "")
    lines_on_page += 3

    # Data rows
    obs = 0
    for row in walm_final.iter_rows(named=True):
        obs += 1
        bnmcode = row["BNMCODE"]
        amount = row["AMOUNT"]

        # Format amount with commas and 2 decimal places
        amount_str = f"{amount:,.2f}"

        # Check if we need a new page
        if lines_on_page >= PAGE_LENGTH - 2:
            write_line(f, "1", f"{SDESC:^132}")
            write_line(f, " ", "REPORT ON DOMESTIC ASSETS AND LIABILITIES PART II - WALKER".center(132))
            write_line(f, " ", f"REPORT DATE : {RDATE}".center(132))
            write_line(f, " ", "")
            write_line(f, " ", "")
            write_line(f, " ", f"{'Obs':>5}    {'BNMCODE':<20}    {'AMOUNT':>25}")
            write_line(f, " ", "")
            lines_on_page = 7

        write_line(f, " ", f"{obs:>5}    {bnmcode:<20}    {amount_str:>25}")
        lines_on_page += 1

print(f"Report saved to {REPORT_FILE}")

# Close DuckDB connection
conn.close()

print("\nProcessing complete!")
print(f"Total BNMCODEs: {len(walm_final)}")
print(f"Total Amount: {walm_final['AMOUNT'].sum():,.2f}")
