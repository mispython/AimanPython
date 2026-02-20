#!/usr/bin/env python3
"""
Program : EIBRDL3A (PBBRDAL3)
Date    : 22/1/97
Purpose : RDAL PART III
          Combines LALQ and KALQ datasets (renaming BNMCODE to ITCODE),
            appends the result into the weekly ALWKM dataset,
            then removes the temporary ALQKM dataset.
"""

import duckdb
import polars as pl
import os

# ─────────────────────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR     = os.path.join(BASE_DIR, "input")
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Macro variables (equivalent to &REPTMON and &NOWK)
REPTMON = "202412"
NOWK    = "01"

# Input parquets (outputs from X_LALQPBBP and X_KALQPBBP)
LALQ_PARQUET  = os.path.join(OUTPUT_DIR, f"LALQ{REPTMON}{NOWK}.parquet")
KALQ_PARQUET  = os.path.join(OUTPUT_DIR, f"KALQ{REPTMON}{NOWK}.parquet")

# Intermediate dataset: BNM.ALQKM{REPTMON}
ALQKM_PARQUET = os.path.join(OUTPUT_DIR, f"ALQKM{REPTMON}.parquet")

# Base (weekly) dataset: BNM.ALWKM{REPTMON}{NOWK}
ALWKM_PARQUET = os.path.join(OUTPUT_DIR, f"ALWKM{REPTMON}{NOWK}.parquet")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# DATA BNM.ALQKM{REPTMON} (KEEP=ITCODE AMTIND AMOUNT)
#   SET BNM.LALQ{REPTMON}{NOWK}
#       BNM.KALQ{REPTMON}{NOWK};
#   RENAME BNMCODE=ITCODE;
# ─────────────────────────────────────────────────────────────
frames = []

for path in (LALQ_PARQUET, KALQ_PARQUET):
    if os.path.exists(path):
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
        con.close()
        frames.append(df)

if not frames:
    raise FileNotFoundError(
        f"Neither {LALQ_PARQUET} nor {KALQ_PARQUET} were found. "
        "Ensure X_LALQPBBP.py and X_KALQPBBP.py have been run first."
    )

# Concatenate LALQ and KALQ, then rename BNMCODE -> ITCODE, keep only required columns
alqkm = pl.concat(frames, how="diagonal")
alqkm = alqkm.rename({"BNMCODE": "ITCODE"}).select(["ITCODE", "AMTIND", "AMOUNT"])

# Write intermediate BNM.ALQKM{REPTMON}
alqkm.write_parquet(ALQKM_PARQUET)

# ─────────────────────────────────────────────────────────────
# MERGE QUARTERLY ITEMS INTO WEEKLY (19/11)
# AS EL REPORTING MAY REQUIRE QUARTERLY ITEMS
# PROC APPEND DATA=BNM.ALQKM{REPTMON} BASE=BNM.ALWKM{REPTMON}{NOWK}
# ─────────────────────────────────────────────────────────────
if os.path.exists(ALWKM_PARQUET):
    con = duckdb.connect()
    existing = con.execute(f"SELECT * FROM read_parquet('{ALWKM_PARQUET}')").pl()
    con.close()
    alwkm = pl.concat([existing, alqkm], how="diagonal")
else:
    alwkm = alqkm

alwkm.write_parquet(ALWKM_PARQUET)

# ─────────────────────────────────────────────────────────────
# PROC DATASETS LIB=BNM NOLIST; DELETE ALQKM{REPTMON}
# Remove the temporary intermediate dataset
# ─────────────────────────────────────────────────────────────
if os.path.exists(ALQKM_PARQUET):
    os.remove(ALQKM_PARQUET)

print(f"ALWKM dataset updated : {ALWKM_PARQUET}")
