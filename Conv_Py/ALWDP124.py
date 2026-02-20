# !/usr/bin/env python3
"""
Program : ALWDP124
Purpose : Copy ALW{REPTMON}{NOWK} dataset from BNM library to BNMX library
"""

import duckdb
import polars as pl
import os

# ─────────────────────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))

# Macro variables (equivalent to &REPTMON and &NOWK)
REPTMON = "202412"
NOWK    = "01"

# BNM library directory (source)
BNM_DIR  = os.path.join(BASE_DIR, "output")

# BNMX library directory (destination)
BNMX_DIR = os.path.join(BASE_DIR, "output", "bnmx")

ALW_SOURCE = os.path.join(BNM_DIR,  f"ALW{REPTMON}{NOWK}.parquet")
ALW_DEST   = os.path.join(BNMX_DIR, f"ALW{REPTMON}{NOWK}.parquet")

os.makedirs(BNMX_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# DATA BNMX.ALW{REPTMON}{NOWK}
#   SET BNM.ALW{REPTMON}{NOWK};
# ─────────────────────────────────────────────────────────────
con = duckdb.connect()
df = con.execute(f"SELECT * FROM read_parquet('{ALW_SOURCE}')").pl()
con.close()

df.write_parquet(ALW_DEST)

print(f"Copied : {ALW_SOURCE}")
print(f"     -> {ALW_DEST}")
