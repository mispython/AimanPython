#!/usr/bin/env python3
"""
Program: EIBRDL1B
Processes RDAL Part I output for Walker only
Merges various datasets and applies complex filtering logic
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

# Define macro lists
AL100 = ['42110', '42120', '42130', '42131', '42132', '42150', '42160',
         '42170', '42180', '42199', '42510', '42520', '42599', '42190']
AL500 = ['42510', '42520', '42599']
AL600 = ['42610', '42620', '42630', '42631', '42632', '42660', '42699']

# Define output file path
ALWWK_FILE = OUTPUT_BNM_DIR / f"ALWWK{REPTMON}{NOWK}.parquet"

# Initialize DuckDB connection
conn = duckdb.connect()


def load_parquet(filepath):
    """Load parquet file using DuckDB and return Polars DataFrame"""
    return conn.execute(f"SELECT * FROM '{filepath}'").pl()


print("Loading input datasets...")

# Load input datasets
dalw = load_parquet(BNM_DIR / f"DALW{REPTMON}{NOWK}.parquet")
falw = load_parquet(BNM_DIR / f"FALW{REPTMON}{NOWK}.parquet")
lalw = load_parquet(BNM_DIR / f"LALW{REPTMON}{NOWK}.parquet")
kalw = load_parquet(BNM_DIR / f"KALW{REPTMON}{NOWK}.parquet")
walw = load_parquet(BNM_DIR / f"WALW{REPTMON}{NOWK}.parquet")

print("Creating OTHER dataset...")

# Combine all datasets and set AMOUNT to 0
other = pl.concat([dalw, falw, lalw, kalw]).with_columns([
    pl.lit(0.0).alias("AMOUNT")
])

# Summarize OTHER by BNMCODE and AMTIND
other = other.group_by(["BNMCODE", "AMTIND"]).agg([
    pl.col("AMOUNT").sum()
])

print(f"OTHER records after summary: {len(other)}")

print("Applying BNMCODE transformations...")

# Apply BNMCODE transformations with additional outputs
other_expanded = []

for row in other.iter_rows(named=True):
    bnmcode = row["BNMCODE"]
    amtind = row["AMTIND"]
    amount = row["AMOUNT"]

    # Always output the original record
    other_expanded.append({"BNMCODE": bnmcode, "AMTIND": amtind, "AMOUNT": amount})

    bnmcode_prefix5 = bnmcode[:5]
    bnmcode_pos6_2 = bnmcode[5:7]

    # AL100 logic
    if bnmcode_prefix5 in AL100 and bnmcode_pos6_2 in ['57', '75']:
        if bnmcode_pos6_2 == '75':
            other_expanded.append({"BNMCODE": "4210075000000Y", "AMTIND": amtind, "AMOUNT": amount})
        if bnmcode_pos6_2 == '57':
            other_expanded.append({"BNMCODE": "4210057000000Y", "AMTIND": amtind, "AMOUNT": amount})

    # AL500 logic
    if bnmcode_prefix5 in AL500 and bnmcode_pos6_2 in ['01', '71']:
        if bnmcode_pos6_2 == '75':
            other_expanded.append({"BNMCODE": "4250001000000Y", "AMTIND": amtind, "AMOUNT": amount})
        if bnmcode_pos6_2 == '57':
            other_expanded.append({"BNMCODE": "4250071000000Y", "AMTIND": amtind, "AMOUNT": amount})

    # AL600 logic
    if bnmcode_prefix5 in AL600 and bnmcode_pos6_2 in ['57', '75']:
        if bnmcode_pos6_2 == '75':
            other_expanded.append({"BNMCODE": "4260075000000Y", "AMTIND": amtind, "AMOUNT": amount})
        if bnmcode_pos6_2 == '57':
            other_expanded.append({"BNMCODE": "4260057000000Y", "AMTIND": amtind, "AMOUNT": amount})

other = pl.DataFrame(other_expanded)

print(f"OTHER records after expansion: {len(other)}")

print("Merging WALW and OTHER...")

# Merge WALW and OTHER
merged = walw.join(
    other,
    on=["BNMCODE", "AMTIND"],
    how="outer",
    suffix="_other"
).with_columns([
    pl.col("AMOUNT").alias("WISAMT"),
    pl.col("AMOUNT_other").alias("OTHAMT")
])

print(f"Merged records: {len(merged)}")

print("Processing final output logic...")

# Process merged data with complex logic
alwwk_records = []

for row in merged.iter_rows(named=True):
    bnmcode = row["BNMCODE"]
    amtind = row["AMTIND"]
    wisamt = row["WISAMT"] if row["WISAMT"] is not None else 0.0
    othamt = row["OTHAMT"] if row["OTHAMT"] is not None else 0.0

    has_walw = row["WISAMT"] is not None
    has_other = row["OTHAMT"] is not None

    weekly = 'Y'

    bnmcode_prefix2 = bnmcode[:2]
    bnmcode_prefix3 = bnmcode[:3]
    bnmcode_prefix4 = bnmcode[:4]
    bnmcode_prefix7 = bnmcode[:7]

    # Check if should be filtered out
    is_monthly = (
            bnmcode_prefix2 in ['35', '36', '37', '59'] or
            bnmcode_prefix3 in ['821', '411', '391', '392', '491', '492']
    )

    if not is_monthly:
        # Filter out monthly items logic
        if bnmcode_prefix2 == '30':
            amtind = ' '

        if has_walw and has_other:
            amount = othamt
        elif has_walw and not has_other:
            amount = wisamt
        elif has_other and not has_walw:
            amount = othamt
        else:
            continue

        alwwk_records.append({
            "ITCODE": bnmcode,
            "AMOUNT": amount,
            "AMTIND": amtind
        })

    else:
        # Monthly items logic
        if NOWK == '4':
            weekly = 'N'

        if has_walw and has_other:
            amount = othamt
            if bnmcode_prefix7 in ['4911050', '4911080', '4929950']:
                amount = wisamt
        elif has_walw and not has_other:
            amount = wisamt
        elif has_other and not has_walw:
            amount = othamt
        else:
            continue

        # Handle specific prefixes
        if bnmcode_prefix2 == '37' and weekly == 'Y':
            alwwk_records.append({
                "ITCODE": bnmcode,
                "AMOUNT": amount,
                "AMTIND": amtind
            })

        if bnmcode_prefix3 in ['391', '392']:
            if bnmcode_prefix4 == '3911':
                alwwk_records.append({
                    "ITCODE": bnmcode,
                    "AMOUNT": amount,
                    "AMTIND": amtind
                })
            if weekly == 'Y':
                alwwk_records.append({
                    "ITCODE": "3910000000000Y",
                    "AMOUNT": amount,
                    "AMTIND": amtind
                })

        if bnmcode_prefix4 in ['4110', '4111']:
            if weekly == 'Y':
                alwwk_records.append({
                    "ITCODE": "4100000000000Y",
                    "AMOUNT": amount,
                    "AMTIND": amtind
                })

        if bnmcode_prefix3 in ['491', '492']:
            if bnmcode_prefix7 in ['4911050', '4911080', '4912050', '4912080',
                                   '4929950', '4929980', '4929000']:
                alwwk_records.append({
                    "ITCODE": bnmcode,
                    "AMOUNT": amount,
                    "AMTIND": amtind
                })
            if weekly == 'Y':
                alwwk_records.append({
                    "ITCODE": "4910000000000Y",
                    "AMOUNT": amount,
                    "AMTIND": amtind
                })

# Create final DataFrame
alwwk = pl.DataFrame(alwwk_records)

print(f"Final ALWWK records: {len(alwwk)}")

# Save output
alwwk.write_parquet(ALWWK_FILE)
print(f"Saved {ALWWK_FILE}")

# Close DuckDB connection
conn.close()

print("\nProcessing complete!")
print(f"Total records in ALWWK{REPTMON}{NOWK}: {len(alwwk)}")
