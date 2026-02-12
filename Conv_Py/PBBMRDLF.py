#!/usr/bin/env python3
"""
File Name: PBBMRDLF
Reads ITCODE values and creates PBBRDAL.parquet
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
OUTPUT_FILE = OUTPUT_DIR / "PBBRDAL.parquet"

# ITCODE data from CARDS section
itcode_data = [
    "3313002000000Y",
    "3313003000000Y",
    "4019000000000Y",
    "4216060000000Y",
    "4261076000000Y",
    "4261085000000Y",
    "4263076000000Y",
    "4263085000000Y",
    "4269981000000Y",
    "4313002000000Y",
    "4313003000000Y",
    "7200000008310Y",
    "7300000003000Y",
    "7300000006100Y",
    "7300000008310Y",
    "7300000008320Y",
    "5422000000000Y",
    "4017000000000Y",
    "3051577000000Y",
    "3054077000000Y",
    "3055060000000Y",
    "3055061000000Y",
    "3055076000000Y",
    "3055077000000Y",
    "3056000000000Y",
    "3400010000310Y",
    "3400010008100Y",
    "3400020000100Y",
    "3400020000110Y",
    "3400000000132Y",
    "3400077000420Y",
    "3400078000132Y",
    "3415100000000Y",
    "3415200000000Y",
    "3415900000000Y",
    "3416000000000Y",
    "3420000000420Y",
    "7211500000000Y",
    "7312000000000Y",
    "7318000000000Y",
    "7411000000000Y",
    "7412000000000Y",
    "7413000000000Y",
    "7414000000000Y",
]

# Create Polars DataFrame
df = pl.DataFrame({
    "ITCODE": itcode_data
})

# Write to parquet
df.write_parquet(OUTPUT_FILE)

print(f"Successfully created {OUTPUT_FILE}")
print(f"Total records: {len(df)}")
