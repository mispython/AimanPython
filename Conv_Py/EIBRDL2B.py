#!/usr/bin/env python3
"""
Program: EIBRDL2B
Processes RDAL Part II output for Walker only
Merges various datasets and appends monthly items to weekly data
"""

import polars as pl
import duckdb
from pathlib import Path

# Setup paths
# INPUT_DIR = Path("input")
# OUTPUT_DIR = Path("output")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

BNM_DIR = INPUT_DIR / "bnm"
OUTPUT_BNM_DIR = OUTPUT_DIR / "bnm"

# Create directories if they don't exist
INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
BNM_DIR.mkdir(exist_ok=True)
OUTPUT_BNM_DIR.mkdir(exist_ok=True)

# Define macro variables (these would typically come from environment or config)
REPTMON = "01"
NOWK = "4"

# Define file paths
ALWWK_FILE = OUTPUT_BNM_DIR / f"ALWWK{REPTMON}{NOWK}.parquet"
ALMWK_FILE = OUTPUT_BNM_DIR / f"ALMWK{REPTMON}.parquet"

# Initialize DuckDB connection
conn = duckdb.connect()


def load_parquet(filepath):
    """Load parquet file using DuckDB and return Polars DataFrame"""
    return conn.execute(f"SELECT * FROM '{filepath}'").pl()


print("Loading input datasets...")

# Load input datasets for OTHER
dalm = load_parquet(BNM_DIR / f"DALM{REPTMON}{NOWK}.parquet")
falm = load_parquet(BNM_DIR / f"FALM{REPTMON}{NOWK}.parquet")
kalm = load_parquet(BNM_DIR / f"KALM{REPTMON}{NOWK}.parquet")
nalm = load_parquet(BNM_DIR / f"NALM{REPTMON}{NOWK}.parquet")
lalm = load_parquet(BNM_DIR / f"LALM{REPTMON}{NOWK}.parquet")

# Load WALW
walw = load_parquet(BNM_DIR / f"WALW{REPTMON}{NOWK}.parquet")

# Load ALWWK (created by previous program)
alwwk = load_parquet(ALWWK_FILE)

print("Creating OTHER dataset...")

# Combine all datasets and set AMOUNT to 0
other = pl.concat([dalm, falm, kalm, nalm, lalm]).with_columns([
    pl.lit(0.0).alias("AMOUNT")
])

# Summarize OTHER by BNMCODE and AMTIND
other = other.group_by(["BNMCODE", "AMTIND"]).agg([
    pl.col("AMOUNT").sum()
])

print(f"OTHER records after summary: {len(other)}")

print("Merging WALW and OTHER to create ALMWK...")

# Merge WALW and OTHER
almwk = walw.join(
    other,
    on=["BNMCODE", "AMTIND"],
    how="outer",
    suffix="_other"
).with_columns([
    pl.col("AMOUNT").alias("WISAMT"),
    pl.col("AMOUNT_other").alias("OTHAMT")
])

# Process merged data
almwk_records = []

for row in almwk.iter_rows(named=True):
    bnmcode = row["BNMCODE"]
    amtind = row["AMTIND"]
    wisamt = row["WISAMT"]
    othamt = row["OTHAMT"]

    has_walw = wisamt is not None
    has_other = othamt is not None

    if has_walw and has_other:
        amount = othamt
    elif has_walw and not has_other:
        amount = wisamt
    elif has_other and not has_walw:
        amount = othamt
    else:
        continue

    almwk_records.append({
        "ITCODE": bnmcode,
        "AMOUNT": amount,
        "AMTIND": amtind
    })

almwk_temp = pl.DataFrame(almwk_records)

print(f"ALMWK records after merge: {len(almwk_temp)}")

print("Filtering ALMWK based on ALWWK...")

# Merge ALWWK and ALMWK to filter
almwk_final = alwwk.join(
    almwk_temp,
    on=["ITCODE", "AMTIND"],
    how="outer_coalesce",
    suffix="_almwk"
).with_columns([
    pl.col("AMOUNT_almwk").alias("AMOUNT_temp")
])

# Filter: Keep only records that are in ALMWK but not in ALWWK, and exclude ITCODE starting with '37'
almwk_final = almwk_final.filter(
    (pl.col("AMOUNT").is_null()) &  # Not in ALWWK
    (pl.col("AMOUNT_temp").is_not_null()) &  # In ALMWK
    (~pl.col("ITCODE").str.slice(0, 2).eq("37"))  # Not starting with '37'
).select([
    pl.col("ITCODE"),
    pl.col("AMOUNT_temp").alias("AMOUNT"),
    pl.col("AMTIND")
])

print(f"ALMWK records after filtering: {len(almwk_final)}")

# Save ALMWK (temporary)
almwk_final.write_parquet(ALMWK_FILE)
print(f"Saved temporary {ALMWK_FILE}")

print("Appending ALMWK to ALWWK...")

# Append ALMWK to ALWWK (merge monthly items into weekly)
alwwk_updated = pl.concat([alwwk, almwk_final])

print(f"ALWWK records after append: {len(alwwk_updated)}")

# Save updated ALWWK
alwwk_updated.write_parquet(ALWWK_FILE)
print(f"Saved updated {ALWWK_FILE}")

# Delete temporary ALMWK file
if ALMWK_FILE.exists():
    ALMWK_FILE.unlink()
    print(f"Deleted temporary {ALMWK_FILE}")

# Close DuckDB connection
conn.close()

print("\nProcessing complete!")
print(f"Final ALWWK{REPTMON}{NOWK} records: {len(alwwk_updated)}")
print(f"  Original ALWWK records: {len(alwwk)}")
print(f"  Appended ALMWK records: {len(almwk_final)}")
