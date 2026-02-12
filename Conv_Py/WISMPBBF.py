#!/usr/bin/env python3
"""
Program: WISMPBBF
Creates SET_ID lookup table for MIS reporting from RDAL1 and RDAL2
"""

import polars as pl
from pathlib import Path

# Setup paths
# INPUT_DIR = Path("input")
# OUTPUT_DIR = Path("output")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Create directories if they don't exist
INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# Define output file path
OUTPUT_FILE = OUTPUT_DIR / "SETITEMS.parquet"

# SET_ID data from CARDS section
setitems_data = [
    "R142150NIDI",
    "R142170WT",
    "R142510FDA",
    "R1423001DEPA",
    "R249299OL",
    "R249170IIS",
]

# Create Polars DataFrame
df = pl.DataFrame({
    "SET_ID": setitems_data
})

# Write to parquet
df.write_parquet(OUTPUT_FILE)

print(f"Successfully created {OUTPUT_FILE}")
print(f"Total records: {len(df)}")
