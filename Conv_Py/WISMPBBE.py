#!/usr/bin/env python3
"""
Program: WISMPBBE
Extracts information from RDAL1 and RDAL2 for MIS reporting
Processes Walker GL and JE data, merges with SETITEMS, and creates WISMON output
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
MIS_DIR = OUTPUT_DIR / "mis"

# Create directories if they don't exist
INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
BNM_DIR.mkdir(exist_ok=True)
MIS_DIR.mkdir(exist_ok=True)

# Define macro variables (these would typically come from environment or config)
REPTMON = "01"
NOWK = "4"

# Define output file path
WISMON_FILE = MIS_DIR / f"WISMON{REPTMON}.parquet"

# Initialize DuckDB connection
conn = duckdb.connect()

def load_parquet(filepath):
    """Load parquet file using DuckDB and return Polars DataFrame"""
    return conn.execute(f"SELECT * FROM '{filepath}'").pl()

print("Loading input datasets...")

# Load SETITEMS format file
setitems = load_parquet(INPUT_DIR / "SETITEMS.parquet")

# Extract unique SET_ID values from SETITEMS
items = setitems.select("SET_ID").unique()
print(f"Unique SET_IDs: {len(items)}")

# Load Walker GL data
alwgl_file = BNM_DIR / f"ALWGL{REPTMON}{NOWK}.parquet"
alwgl = load_parquet(alwgl_file)

# Load Walker JE data
alwje_file = BNM_DIR / f"ALWJE{REPTMON}{NOWK}.parquet"
alwje = load_parquet(alwje_file)

# Load Moorgate GL data
almgl_file = BNM_DIR / f"ALMGL{REPTMON}{NOWK}.parquet"
almgl = load_parquet(almgl_file)

print("Processing Walker GL data (GLW)...")

# SQL: Create GLW - join ALWGL with ITEMS, extract ACK_NO
glw = alwgl.join(
    items,
    on="SET_ID",
    how="inner"
).with_columns([
    pl.col("ACCT_NO").str.slice(7, 5).alias("ACK_NO")
]).select(["BRNO", "SET_ID", "GLAMT", "ACK_NO"])

# Summarize GLW by BRNO, SET_ID, ACK_NO
glw = glw.group_by(["BRNO", "SET_ID", "ACK_NO"]).agg([
    pl.col("GLAMT").sum()
])

print(f"GLW records: {len(glw)}")

print("Processing Walker JE data (JEW)...")

# SQL: Create JEW - join ALWJE with ITEMS, extract ACK_NO
jew = alwje.join(
    items,
    on="SET_ID",
    how="inner"
).with_columns([
    pl.col("ACCT_NO").str.slice(7, 5).alias("ACK_NO")
]).select(["BRNO", "SET_ID", "JEAMT", "ACK_NO", "EFFDATE"])

# Summarize JEW by BRNO, SET_ID, ACK_NO
jew = jew.group_by(["BRNO", "SET_ID", "ACK_NO"]).agg([
    pl.col("JEAMT").sum()
])

print(f"JEW records: {len(jew)}")

print("Merging GLW and JEW...")

# Merge GLW and JEW on BRNO, SET_ID, ACK_NO
wismon = glw.join(
    jew,
    on=["BRNO", "SET_ID", "ACK_NO"],
    how="outer"
).with_columns([
    # Sum GLAMT and JEAMT, treating nulls as 0
    (pl.col("GLAMT").fill_null(0) + pl.col("JEAMT").fill_null(0)).alias("AMOUNT")
]).select(["BRNO", "SET_ID", "ACK_NO", "AMOUNT"])

print(f"WISMON (Walker) records: {len(wismon)}")

print("Processing Moorgate GL data (GLM)...")

# SQL: Create GLM - join ALMGL with ITEMS, extract ACK_NO
glm = almgl.join(
    items,
    on="SET_ID",
    how="inner"
).with_columns([
    pl.col("ACCT_NO").str.slice(7, 5).alias("ACK_NO")
]).select([
    pl.col("BRNO"),
    pl.col("SET_ID"),
    pl.col("GLAMT").alias("AMOUNT"),
    pl.col("ACK_NO")
])

# Summarize GLM by BRNO, SET_ID, ACK_NO
glm = glm.group_by(["BRNO", "SET_ID", "ACK_NO"]).agg([
    pl.col("AMOUNT").sum()
])

print(f"GLM records: {len(glm)}")

print("Appending GLM to WISMON...")

# Append GLM to WISMON (equivalent to PROC APPEND)
wismon_final = pl.concat([wismon, glm])

print(f"Final WISMON records: {len(wismon_final)}")

# Save final WISMON dataset
wismon_final.write_parquet(WISMON_FILE)
print(f"Saved {WISMON_FILE}")

# Close DuckDB connection
conn.close()

print("\nProcessing complete!")
print(f"Total records in WISMON{REPTMON}: {len(wismon_final)}")
print(f"  Walker records (GLW + JEW): {len(wismon)}")
print(f"  Moorgate records (GLM): {len(glm)}")
