#!/usr/bin/env python3
"""
Program : P124DAL2.py (converted from PBBRDAL2)
Date    : 22/1/97
Report  : RDAL PART II
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
ALM_PARQUET    = os.path.join(BNM_DIR, f"ALM{REPTMON}.parquet")
ALW_PARQUET    = os.path.join(BNM_DIR, f"ALW{REPTMON}{NOWK}.parquet")

os.makedirs(BNM_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()

# ── Step 1 : Read LALM (BNM.LALM&REPTMON&NOWK) ───────────────────────────────
other = con.execute(f"""
    SELECT BNMCODE, AMTIND, AMOUNT
    FROM read_parquet('{LALM_PARQUET}')
""").pl()

# ── Step 2 : PROC SUMMARY – sum AMOUNT by BNMCODE, AMTIND ────────────────────
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

# ── Step 4 : Build BNM.ALM&REPTMON ───────────────────────────────────────────
# OUTPUT all rows; KEEP ITCODE AMOUNT AMTIND (rename BNMCODE → ITCODE)
alm_df = other.rename({"BNMCODE": "ITCODE"})[["ITCODE", "AMOUNT", "AMTIND"]]
alm_df.write_parquet(ALM_PARQUET)

# ── Step 5 : PROC SORT BNM.ALW&REPTMON&NOWK by ITCODE AMTIND ─────────────────
alw_df = con.execute(f"""
    SELECT ITCODE, AMOUNT, AMTIND
    FROM read_parquet('{ALW_PARQUET}')
    ORDER BY ITCODE, AMTIND
""").pl()

# ── Step 6 : Merge ALW (A) and ALM (B) – keep B AND NOT A ───────────────────
# Equivalent: left-anti join B against A on ITCODE, AMTIND
# Then exclude rows where ITCODE[:2] IN ('37')
alm_fresh = con.execute(f"""
    SELECT ITCODE, AMOUNT, AMTIND
    FROM read_parquet('{ALM_PARQUET}')
""").pl()

merged = alm_fresh.join(
    alw_df.select(["ITCODE", "AMTIND"]),
    on=["ITCODE", "AMTIND"],
    how="anti"
)

# Exclude rows where ITCODE starts with '37'
merged = merged.filter(~pl.col("ITCODE").str.slice(0, 2).is_in(["37"]))

# This replaces BNM.ALM&REPTMON with the filtered merge result
merged.write_parquet(ALM_PARQUET)

# ── Step 7 : PROC APPEND – append ALM into ALW (base) ────────────────────────
# MERGE MONTHLY ITEMS INTO WEEKLY (19/11)
# AS EL REPORTING REQUIRE MONTHLY ITEMS
alm_to_append = con.execute(f"""
    SELECT ITCODE, AMOUNT, AMTIND
    FROM read_parquet('{ALM_PARQUET}')
""").pl()

alw_base = con.execute(f"""
    SELECT ITCODE, AMOUNT, AMTIND
    FROM read_parquet('{ALW_PARQUET}')
""").pl()

alw_updated = pl.concat([alw_base, alm_to_append], how="diagonal")
alw_updated.write_parquet(ALW_PARQUET)

# ── Step 8 : PROC DATASETS – DELETE ALM&REPTMON ──────────────────────────────
if os.path.exists(ALM_PARQUET):
    os.remove(ALM_PARQUET)
    print(f"Deleted temporary dataset: {ALM_PARQUET}")

print(f"P124DAL2 complete. BNM.ALW updated at: {ALW_PARQUET}")
