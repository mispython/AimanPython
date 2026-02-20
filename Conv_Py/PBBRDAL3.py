# !/usr/bin/env python3
"""
Program : PBBRDAL3
Date    : 22/1/97
Purpose : RDAL PART III
          Combines LALQ and KALQ datasets (renaming BNMCODE to ITCODE),
            appends the result into the weekly ALW dataset,
            then removes the temporary ALQ dataset.
"""

import duckdb
import polars as pl
import os

# ─────────────────────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Macro variables (equivalent to &REPTMON and &NOWK)
REPTMON = "202412"
NOWK    = "01"

# Input parquets (outputs from X_LALQPBBP and X_KALQPBBP)
LALQ_PARQUET = os.path.join(OUTPUT_DIR, f"LALQ{REPTMON}{NOWK}.parquet")
KALQ_PARQUET = os.path.join(OUTPUT_DIR, f"KALQ{REPTMON}{NOWK}.parquet")

# Intermediate dataset: BNM.ALQ{REPTMON}
ALQ_PARQUET  = os.path.join(OUTPUT_DIR, f"ALQ{REPTMON}.parquet")

# Base (weekly) dataset: BNM.ALW{REPTMON}{NOWK}
ALW_PARQUET  = os.path.join(OUTPUT_DIR, f"ALW{REPTMON}{NOWK}.parquet")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# DATA BNM.ALQ{REPTMON} (KEEP=ITCODE AMTIND AMOUNT)
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

# Concatenate LALQ and KALQ, rename BNMCODE -> ITCODE, keep only required columns
alq = pl.concat(frames, how="diagonal")
alq = alq.rename({"BNMCODE": "ITCODE"}).select(["ITCODE", "AMTIND", "AMOUNT"])

# Write intermediate BNM.ALQ{REPTMON}
alq.write_parquet(ALQ_PARQUET)

# ─────────────────────────────────────────────────────────────
# MERGE QUARTERLY ITEMS INTO WEEKLY (19/11)
# AS EL REPORTING MAY REQUIRE QUARTERLY ITEMS
# PROC APPEND DATA=BNM.ALQ{REPTMON} BASE=BNM.ALW{REPTMON}{NOWK}
# ─────────────────────────────────────────────────────────────
if os.path.exists(ALW_PARQUET):
    con = duckdb.connect()
    existing = con.execute(f"SELECT * FROM read_parquet('{ALW_PARQUET}')").pl()
    con.close()
    alw = pl.concat([existing, alq], how="diagonal")
else:
    alw = alq

alw.write_parquet(ALW_PARQUET)

# ─────────────────────────────────────────────────────────────
# PROC DATASETS LIB=BNM NOLIST; DELETE ALQ{REPTMON}
# Remove the temporary intermediate dataset
# ─────────────────────────────────────────────────────────────
if os.path.exists(ALQ_PARQUET):
    os.remove(ALQ_PARQUET)

print(f"ALW dataset updated : {ALW_PARQUET}")
