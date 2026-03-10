#!/usr/bin/env python3
"""
Program : P124DL2B.py (converted from EIBRDL2B)
Date    : 30/9/01
Report  : RDAL PART II (OUTPUT TO WALKER ONLY)
"""

import duckdb
import polars as pl
import os

# ── Path configuration ────────────────────────────────────────────────────────
BASE_DIR   = os.environ.get("BASE_DIR", "/data")
BNM_DIR    = os.path.join(BASE_DIR, "bnm")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

REPTMON  = os.environ.get("REPTMON", "")   # e.g. "202401"
NOWK     = os.environ.get("NOWK",    "")   # e.g. "01"

# Parquet paths
LALM_PARQUET   = os.path.join(BNM_DIR, f"LALM{REPTMON}{NOWK}.parquet")
ALMWK_PARQUET  = os.path.join(BNM_DIR, f"ALMWK{REPTMON}.parquet")
ALWWK_PARQUET  = os.path.join(BNM_DIR, f"ALWWK{REPTMON}{NOWK}.parquet")

os.makedirs(BNM_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()

# ── Step 1 : Read LALM and force AMOUNT = 0 ───────────────────────────────────
other = con.execute(f"""
    SELECT BNMCODE, AMTIND, 0.0 AS AMOUNT
    FROM read_parquet('{LALM_PARQUET}')
""").pl()

# ── Step 2 : PROC SUMMARY – sum AMOUNT (all zeros) by BNMCODE, AMTIND ─────────
other = (
    other
    .group_by(["BNMCODE", "AMTIND"])
    .agg(pl.col("AMOUNT").sum())
)

# Ensure BNMCODE is up to 14 characters (LENGTH $14.)
other = other.with_columns(
    pl.col("BNMCODE").cast(pl.Utf8).str.ljust(14, " ").alias("BNMCODE")
)

# ── Step 3 : PROC SORT by BNMCODE, AMTIND ────────────────────────────────────
other = other.sort(["BNMCODE", "AMTIND"])

# ── Step 4 : Build BNM.ALMWK&REPTMON ─────────────────────────────────────────
# OUTPUT all rows; KEEP ITCODE AMOUNT AMTIND (rename BNMCODE → ITCODE)
almwk_df = other.rename({"BNMCODE": "ITCODE"})[["ITCODE", "AMOUNT", "AMTIND"]]
almwk_df.write_parquet(ALMWK_PARQUET)

# ── Step 5 : PROC SORT BNM.ALWWK&REPTMON&NOWK by ITCODE AMTIND ───────────────
alwwk_df = con.execute(f"""
    SELECT ITCODE, AMOUNT, AMTIND
    FROM read_parquet('{ALWWK_PARQUET}')
    ORDER BY ITCODE, AMTIND
""").pl()

# ── Step 6 : Merge ALWWK (A) and ALMWK (B) – keep B AND NOT A ───────────────
# Then exclude rows where ITCODE[:2] IN ('37')
almwk_fresh = con.execute(f"""
    SELECT ITCODE, AMOUNT, AMTIND
    FROM read_parquet('{ALMWK_PARQUET}')
""").pl()

merged = almwk_fresh.join(
    alwwk_df.select(["ITCODE", "AMTIND"]),
    on=["ITCODE", "AMTIND"],
    how="anti"
)

# Exclude rows where ITCODE starts with '37'
merged = merged.filter(~pl.col("ITCODE").str.slice(0, 2).is_in(["37"]))

# Replace BNM.ALMWK&REPTMON with filtered merge result
merged.write_parquet(ALMWK_PARQUET)

# ── Step 7 : PROC APPEND – append ALMWK into ALWWK (base) ────────────────────
almwk_to_append = con.execute(f"""
    SELECT ITCODE, AMOUNT, AMTIND
    FROM read_parquet('{ALMWK_PARQUET}')
""").pl()

alwwk_base = con.execute(f"""
    SELECT ITCODE, AMOUNT, AMTIND
    FROM read_parquet('{ALWWK_PARQUET}')
""").pl()

alwwk_updated = pl.concat([alwwk_base, almwk_to_append], how="diagonal")
alwwk_updated.write_parquet(ALWWK_PARQUET)

# ── Step 8 : PROC DATASETS – DELETE ALMWK&REPTMON ────────────────────────────
if os.path.exists(ALMWK_PARQUET):
    os.remove(ALMWK_PARQUET)
    print(f"Deleted temporary dataset: {ALMWK_PARQUET}")

print(f"P124DL2B complete. BNM.ALWWK updated at: {ALWWK_PARQUET}")
