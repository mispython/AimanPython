#!/usr/bin/env python3
"""
Program : P124DAL3.py (converted from PBBRDAL3)
Date    : 22/1/97
Report  : RDAL PART III
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
LALQ_PARQUET = os.path.join(BNM_DIR, f"LALQ{REPTMON}{NOWK}.parquet")
ALQ_PARQUET  = os.path.join(BNM_DIR, f"ALQ{REPTMON}.parquet")
ALW_PARQUET  = os.path.join(BNM_DIR, f"ALW{REPTMON}{NOWK}.parquet")

os.makedirs(BNM_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()

# ── Step 1 : Build BNM.ALQ&REPTMON from BNM.LALQ&REPTMON&NOWK ───────────────
# KEEP=ITCODE AMTIND AMOUNT; RENAME BNMCODE=ITCODE
alq_df = con.execute(f"""
    SELECT BNMCODE AS ITCODE, AMTIND, AMOUNT
    FROM read_parquet('{LALQ_PARQUET}')
""").pl()

alq_df.write_parquet(ALQ_PARQUET)

# ── Step 2 : PROC APPEND – append ALQ into ALW (base) ────────────────────────
alw_base = con.execute(f"""
    SELECT ITCODE, AMOUNT, AMTIND
    FROM read_parquet('{ALW_PARQUET}')
""").pl()

alq_to_append = con.execute(f"""
    SELECT ITCODE, AMTIND, AMOUNT
    FROM read_parquet('{ALQ_PARQUET}')
""").pl()

alw_updated = pl.concat([alw_base, alq_to_append], how="diagonal")
alw_updated.write_parquet(ALW_PARQUET)

# ── Step 3 : PROC DATASETS – DELETE ALQ&REPTMON ──────────────────────────────
if os.path.exists(ALQ_PARQUET):
    os.remove(ALQ_PARQUET)
    print(f"Deleted temporary dataset: {ALQ_PARQUET}")

print(f"P124DAL3 complete. BNM.ALW updated at: {ALW_PARQUET}")
