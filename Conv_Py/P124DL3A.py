# !/usr/bin/env python3
"""
Program : P124DL3A.py
Purpose : Read LALQ data, rename BNMCODE to ITCODE, keep ITCODE/AMTIND/AMOUNT,
            write to ALQKM output, append to ALWKM cumulative dataset,
            then delete the temporary ALQKM dataset.
"""

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import os

# ---------------------------------------------------------------------------
# Path configuration – adjust these variables to match your environment
# ---------------------------------------------------------------------------
BNM_LIB       = "/data/bnm"                           # BNM SAS library path equivalent
REPTMON       = os.environ.get("REPTMON", "202412")   # Reporting month, e.g. "202412"
NOWK          = os.environ.get("NOWK",   "01")        # Week number,       e.g. "01"

# Derived file paths
LALQ_PATH     = os.path.join(BNM_LIB, f"LALQ{REPTMON}{NOWK}.parquet")
ALWKM_PATH    = os.path.join(BNM_LIB, f"ALWKM{REPTMON}{NOWK}.parquet")

# Temporary ALQKM file (deleted at the end, mirroring PROC DATASETS DELETE)
ALQKM_PATH    = os.path.join(BNM_LIB, f"ALQKM{REPTMON}.parquet")

# ---------------------------------------------------------------------------
# Step 1: DATA BNM.ALQKM&REPTMON (KEEP=ITCODE AMTIND AMOUNT)
#         SET BNM.LALQ&REPTMON&NOWK;
#         RENAME BNMCODE=ITCODE;
#         Read source, rename BNMCODE → ITCODE, keep only required columns
# ---------------------------------------------------------------------------
con = duckdb.connect()

alqkm_df = con.execute(f"""
    SELECT
        BNMCODE  AS ITCODE,
        AMTIND,
        AMOUNT
    FROM read_parquet('{LALQ_PATH}')
""").pl()

con.close()

# Write temporary ALQKM parquet
pq.write_table(alqkm_df.to_arrow(), ALQKM_PATH)

# ---------------------------------------------------------------------------
# Step 2: PROC APPEND DATA=BNM.ALQKM&REPTMON BASE=BNM.ALWKM&REPTMON&NOWK
#         Append ALQKM rows into ALWKM (create ALWKM if it doesn't exist yet)
# ---------------------------------------------------------------------------
if os.path.exists(ALWKM_PATH):
    alwkm_existing = pl.read_parquet(ALWKM_PATH)
    alwkm_combined = pl.concat([alwkm_existing, alqkm_df], how="diagonal")
else:
    alwkm_combined = alqkm_df

pq.write_table(alwkm_combined.to_arrow(), ALWKM_PATH)

# ---------------------------------------------------------------------------
# Step 3: PROC DATASETS LIB=BNM NOLIST; DELETE ALQKM&REPTMON;
#         Remove the temporary ALQKM parquet file
# ---------------------------------------------------------------------------
if os.path.exists(ALQKM_PATH):
    os.remove(ALQKM_PATH)
