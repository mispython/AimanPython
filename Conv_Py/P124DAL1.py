#!/usr/bin/env python3
"""
Program : P124DAL1.py (converted from PBBRDAL1)
Date    : 21/1/97
Report  : RDAL PART I
"""

import duckdb
import polars as pl
import os

# ── Path configuration ────────────────────────────────────────────────────────
BASE_DIR   = os.environ.get("BASE_DIR", "/data")
BNM_DIR    = os.path.join(BASE_DIR, "bnm")
INPUT_DIR  = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

REPTMON  = os.environ.get("REPTMON", "")   # e.g. "202401"
NOWK     = os.environ.get("NOWK",    "")   # e.g. "01"

# Input / output parquet / txt paths
LALW_PARQUET   = os.path.join(BNM_DIR, f"LALW{REPTMON}{NOWK}.parquet")
ALW_PARQUET    = os.path.join(BNM_DIR, f"ALW{REPTMON}{NOWK}.parquet")
ALW_TXT        = os.path.join(OUTPUT_DIR, f"ALW{REPTMON}{NOWK}.txt")

os.makedirs(BNM_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── OPTIONS YEARCUTOFF=1950 ───────────────────────────────────────────────────
# Python's date parsing handles two-digit years differently; YEARCUTOFF=1950
# means years 50-99 → 1950-1999, 00-49 → 2000-2049. Apply if needed at
# read time; no global setting required here.

# ── Step 1 : Read LALW (BNM.LALW&REPTMON&NOWK) ───────────────────────────────
con = duckdb.connect()

other = con.execute(f"""
    SELECT BNMCODE, AMTIND, AMOUNT
    FROM read_parquet('{LALW_PARQUET}')
""").pl()

# ── Step 2 : PROC SUMMARY – sum AMOUNT by BNMCODE, AMTIND ────────────────────
other = (
    other
    .group_by(["BNMCODE", "AMTIND"])
    .agg(pl.col("AMOUNT").sum())
)

# Ensure BNMCODE is treated as a string of up to 14 characters (LENGTH $14.)
other = other.with_columns(
    pl.col("BNMCODE").cast(pl.Utf8).str.ljust(14, " ").alias("BNMCODE")
)

# ── Step 3 : Data-step filtering and routing ──────────────────────────────────
#
# Two output datasets:
#   BNM.ALW&REPTMON&NOWK  → alw_main  (kept: ITCODE, AMOUNT, AMTIND)
#   ALW (work)            → alw_work  (kept: ITCODE, AMOUNT, AMTIND)

alw_main_rows = []
alw_work_rows = []

for row in other.iter_rows(named=True):
    bnmcode = row["BNMCODE"]
    amtind  = row["AMTIND"]
    amount  = row["AMOUNT"]

    # Work copy so we can mutate BNMCODE per branch
    bc = bnmcode

    # Primary exclusion / inclusion logic
    p1_2 = bc[:2]
    p1_3 = bc[:3]

    exclude_main = (
        p1_2 in ("35", "36", "37", "59")
        or p1_3 in ("821",)
        or p1_3 in ("414",)
        or p1_3 in ("411", "391", "392", "491", "492")
    )

    if not exclude_main:
        out_amtind = " " if p1_2 == "30" else amtind
        alw_main_rows.append({"ITCODE": bc, "AMOUNT": amount, "AMTIND": out_amtind})

    # Independent branch: starts with '37' → BNM.ALW
    if p1_2 == "37":
        alw_main_rows.append({"ITCODE": bc, "AMOUNT": amount, "AMTIND": amtind})

    # Independent branch: starts with '391' or '392'
    if p1_3 in ("391", "392"):
        # Always output to ALW work dataset
        alw_work_rows.append({"ITCODE": bc, "AMOUNT": amount, "AMTIND": amtind})
        # If starts with '3911' → BNM.ALW
        if bc[:4] == "3911":
            alw_main_rows.append({"ITCODE": bc, "AMOUNT": amount, "AMTIND": amtind})
        # Replace BNMCODE and output to BNM.ALW
        bc_new = "3910000000000Y"
        alw_main_rows.append({"ITCODE": bc_new, "AMOUNT": amount, "AMTIND": amtind})

    # Independent branch: starts with '4110', '4111', or '4140'
    if bc[:4] in ("4110", "4111", "4140"):
        bc_new = "4100000000000Y"
        alw_main_rows.append({"ITCODE": bc_new, "AMOUNT": amount, "AMTIND": amtind})

    # Independent branch: starts with '491' or '492'
    if p1_3 in ("491", "492"):
        # Always output to ALW work dataset
        alw_work_rows.append({"ITCODE": bc, "AMOUNT": amount, "AMTIND": amtind})
        # Conditional BNM.ALW output for specific 7-char prefixes
        if bc[:7] in (
            "4911050", "4911080", "4912050", "4912080",
            "4929950", "4929980", "4929000"
        ):
            alw_main_rows.append({"ITCODE": bc, "AMOUNT": amount, "AMTIND": amtind})
        # Replace BNMCODE and output to BNM.ALW
        bc_new = "4910000000000Y"
        alw_main_rows.append({"ITCODE": bc_new, "AMOUNT": amount, "AMTIND": amtind})

# ── Build final DataFrames ────────────────────────────────────────────────────
schema = {"ITCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTIND": pl.Utf8}

alw_main_df = pl.DataFrame(alw_main_rows, schema=schema) if alw_main_rows else pl.DataFrame(schema=schema)
alw_work_df = pl.DataFrame(alw_work_rows, schema=schema) if alw_work_rows else pl.DataFrame(schema=schema)

# ── Write BNM.ALW&REPTMON&NOWK as parquet ────────────────────────────────────
alw_main_df.write_parquet(ALW_PARQUET)

# ── Write ALW (work copy) as txt  ─────────────────────────────────────────────
with open(ALW_TXT, "w") as f:
    for row in alw_work_df.iter_rows(named=True):
        f.write(f"{row['ITCODE']}\t{row['AMOUNT']}\t{row['AMTIND']}\n")

print(f"P124DAL1 complete. BNM.ALW written to: {ALW_PARQUET}")
print(f"ALW (work) written to: {ALW_TXT}")
