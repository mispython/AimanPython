# !/usr/bin/env python3
"""
Program : P124DL2A.py
Purpose : Summarize LALM data by BNMCODE and AMTIND, aggregate AMOUNT,
              write to ALMKM output, then append to ALWKM cumulative dataset
              and delete the temporary ALMKM dataset.
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
LALM_PATH     = os.path.join(BNM_LIB, f"LALM{REPTMON}{NOWK}.parquet")
ALWKM_PATH    = os.path.join(BNM_LIB, f"ALWKM{REPTMON}{NOWK}.parquet")

# Temporary ALMKM file (deleted at the end, mirroring PROC DATASETS DELETE)
ALMKM_PATH    = os.path.join(BNM_LIB, f"ALMKM{REPTMON}.parquet")

# ---------------------------------------------------------------------------
# Step 1: Read LALM&REPTMON&NOWK  →  equivalent to  DATA OTHER; SET BNM.LALM...
# ---------------------------------------------------------------------------
con = duckdb.connect()

other_df = con.execute(f"""
    SELECT BNMCODE, AMTIND, AMOUNT
    FROM read_parquet('{LALM_PATH}')
""").pl()

# ---------------------------------------------------------------------------
# Step 2: PROC SUMMARY … NWAY; CLASS BNMCODE AMTIND; VAR AMOUNT; OUTPUT SUM=
#         Equivalent: group by BNMCODE, AMTIND and sum AMOUNT (DROP _TYPE_ _FREQ_ implicit)
# ---------------------------------------------------------------------------
other_summary = (
    other_df
    .group_by(["BNMCODE", "AMTIND"])
    .agg(pl.col("AMOUNT").sum())
    # PROC SORT BY BNMCODE AMTIND – keep consistent ordering
    .sort(["BNMCODE", "AMTIND"])
)

# ---------------------------------------------------------------------------
# Step 3: DATA BNM.ALMKM&REPTMON (KEEP=ITCODE AMOUNT AMTIND)
#         LENGTH ITCODE $14.;  ITCODE=BNMCODE;
# ---------------------------------------------------------------------------
almkm_df = (
    other_summary
    .with_columns(
        pl.col("BNMCODE").cast(pl.String).str.slice(0, 14).alias("ITCODE")
    )
    .select(["ITCODE", "AMOUNT", "AMTIND"])
)

# Write temporary ALMKM parquet
pq.write_table(almkm_df.to_arrow(), ALMKM_PATH)

# ---------------------------------------------------------------------------
# Step 4: PROC APPEND DATA=BNM.ALMKM&REPTMON BASE=BNM.ALWKM&REPTMON&NOWK
#         Append ALMKM rows into ALWKM (create ALWKM if it doesn't exist yet)
# ---------------------------------------------------------------------------
if os.path.exists(ALWKM_PATH):
    alwkm_existing = pl.read_parquet(ALWKM_PATH)
    alwkm_combined = pl.concat([alwkm_existing, almkm_df], how="diagonal")
else:
    alwkm_combined = almkm_df

pq.write_table(alwkm_combined.to_arrow(), ALWKM_PATH)

# ---------------------------------------------------------------------------
# Step 5: PROC DATASETS LIB=BNM NOLIST; DELETE ALMKM&REPTMON;
#         Remove the temporary ALMKM parquet file
# ---------------------------------------------------------------------------
if os.path.exists(ALMKM_PATH):
    os.remove(ALMKM_PATH)

con.close()
