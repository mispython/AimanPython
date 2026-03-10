#!/usr/bin/env python3
"""
Program : P124DL1B.py

- Equivalent to P124DAL1 but sets AMOUNT=0 throughout, outputs to
    BNM.ALWWK&REPTMON&NOWK instead of BNM.ALW&REPTMON&NOWK.
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
LALW_PARQUET   = os.path.join(BNM_DIR, f"LALW{REPTMON}{NOWK}.parquet")
ALWWK_PARQUET  = os.path.join(BNM_DIR, f"ALWWK{REPTMON}{NOWK}.parquet")

os.makedirs(BNM_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── OPTIONS YEARCUTOFF=1950 ───────────────────────────────────────────────────
# Python's date parsing handles two-digit years differently; YEARCUTOFF=1950
#   means years 50-99 → 1950-1999, 00-49 → 2000-2049. Apply if needed at
#   read time; no global setting required here.

con = duckdb.connect()

# ── Step 1 : Read LALW and force AMOUNT = 0 ───────────────────────────────────
other = con.execute(f"""
    SELECT BNMCODE, AMTIND, 0.0 AS AMOUNT
    FROM read_parquet('{LALW_PARQUET}')
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

# ── Step 3 : Data-step filtering and routing to BNM.ALWWK&REPTMON&NOWK ───────
#
# Note: Unlike P124DAL1, there is no separate ALW work dataset here.
# The SAS code only outputs to BNM.ALWWK&REPTMON&NOWK.
# Also note: '4140' is NOT excluded in this program (only '4110','4111' present).

alwwk_rows = []

for row in other.iter_rows(named=True):
    bnmcode = row["BNMCODE"]
    amtind  = row["AMTIND"]
    amount  = row["AMOUNT"]   # always 0

    bc    = bnmcode
    p1_2  = bc[:2]
    p1_3  = bc[:3]
    p1_4  = bc[:4]

    # Primary exclusion / inclusion block (no OUTPUT in original – just
    # sets AMTIND for the '30' prefix; this block has no OUTPUT statement
    # so no rows are written from here directly)
    exclude_main = (
        p1_2 in ("35", "36", "37", "59")
        or p1_3 in ("821",)
        or p1_3 in ("411", "391", "392", "491", "492")
    )

    if not exclude_main:
        # Only sets AMTIND; no OUTPUT here in the original SAS code
        if p1_2 == "30":
            amtind = " "
        # (no OUTPUT BNM.ALWWK here – SAS code omits it in this program)

    # Branch: starts with '37' → ALWWK
    if p1_2 == "37":
        alwwk_rows.append({"ITCODE": bc, "AMOUNT": amount, "AMTIND": amtind})

    # Branch: starts with '391' or '392'
    if p1_3 in ("391", "392"):
        # If starts with '3911' → ALWWK
        if p1_4 == "3911":
            alwwk_rows.append({"ITCODE": bc, "AMOUNT": amount, "AMTIND": amtind})
        # Replace BNMCODE and output to ALWWK
        bc_new = "3910000000000Y"
        alwwk_rows.append({"ITCODE": bc_new, "AMOUNT": amount, "AMTIND": amtind})

    # Branch: starts with '4110' or '4111' (note: '4140' not listed here)
    if p1_4 in ("4110", "4111"):
        bc_new = "4100000000000Y"
        alwwk_rows.append({"ITCODE": bc_new, "AMOUNT": amount, "AMTIND": amtind})

    # Branch: starts with '491' or '492'
    if p1_3 in ("491", "492"):
        # Conditional ALWWK output for specific 7-char prefixes
        if bc[:7] in (
            "4911050", "4911080", "4912050", "4912080",
            "4929950", "4929980", "4929000"
        ):
            alwwk_rows.append({"ITCODE": bc, "AMOUNT": amount, "AMTIND": amtind})
        # Replace BNMCODE and output to ALWWK
        bc_new = "4910000000000Y"
        alwwk_rows.append({"ITCODE": bc_new, "AMOUNT": amount, "AMTIND": amtind})

# ── Build final DataFrame ─────────────────────────────────────────────────────
schema = {"ITCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTIND": pl.Utf8}

alwwk_df = (
    pl.DataFrame(alwwk_rows, schema=schema)
    if alwwk_rows
    else pl.DataFrame(schema=schema)
)

# ── Write BNM.ALWWK&REPTMON&NOWK as parquet ──────────────────────────────────
alwwk_df.write_parquet(ALWWK_PARQUET)

print(f"P124DL1B complete. BNM.ALWWK written to: {ALWWK_PARQUET}")
