# !/usr/bin/env python3
"""
Program  : EIBWSMSS.py
Purpose  : Signature Management System – Mode of Operation for SA/CA/FD.
           Reads the CIS signature flat file, standardises CONDIMODE values,
           deduplicates by ACCTNO, and writes SIGNA.SMSACC as Parquet.
ESMR     : 2017-00002332, 2018-488 (ESIGNATURE field)
"""

from pathlib import Path
import polars as pl

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
BASE_FLAT = Path("input_flat")          # fixed-width .txt flat file inputs
BASE_OUT  = Path("output_parquet")      # Parquet output root

SMSTXT_TXT = BASE_FLAT / "SMSTXT" / "SMSTXT.txt"
SIGNA_DIR  = BASE_OUT  / "SIGNA"

SIGNA_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------
# Helper: write a Polars DataFrame as a Parquet file under a given library
# --------------------------------------------------------------------------
def write_tbl(df: pl.DataFrame, lib: str, name: str) -> None:
    outdir = BASE_OUT / lib
    outdir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(outdir / f"{name}.parquet")


# --------------------------------------------------------------------------
# Allowed CONDIMODE values exactly as listed in SAS.
# SAS $UPCASE20. reads exactly 20 chars and uppercases them, so the raw
# field is always 20 characters wide (space-padded on the right).
# We therefore compare against right-padded 20-char strings, matching what
# SAS stores before the NOT IN test.
# The SAS literal ' ' (one space) maps to a field that is all spaces when
# the source contains no meaningful value; we handle this by stripping and
# checking for empty after the main membership test.
# --------------------------------------------------------------------------
_ALLOWED_RAW = {
    v.ljust(20) for v in (
        'ANY 1 OF 1 TO SIGN',
        'ANY 1 OF 2 TO SIGN',
        'ANY 1 OF 3 TO SIGN',
        'ANY 1 OF 4 TO SIGN',
        'ANY 2 OF 2 TO SIGN',
        'ANY 2 OF 3 TO SIGN',
        'ANY 2 OF 4 TO SIGN',
        'ANY 3 OF 3 TO SIGN',
        'ANY 3 OF 4 TO SIGN',
        'ANY 4 OF 4 TO SIGN',
    )
}

# --------------------------------------------------------------------------
# JCL DELETE / CREATE of SAP.PBB.SMS.SIGNAFTP:
#   IEFBR14 DISP=(MOD,DELETE,DELETE) → remove pre-existing transport file
#   The transport file itself (PROC CPORT target) has no Python equivalent;
#   only the Parquet dataset output is produced here.
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# DATA SIGNA.SMSACC;
#   INFILE SMSTXT;
#   INPUT @001  ACCTNO      10.
#         @011  ESIGNATURE  $1.
#         @012  CONDIMODE   $UPCASE20.;
#   IF CONDIMODE NOT IN (...) THEN CONDIMODE = 'OTHERS';
#
# Read the SMSTXT mainframe flat file (fixed-width .txt).
# Each line is one record; columns are addressed by 1-based byte positions.
# --------------------------------------------------------------------------
raw_lines = SMSTXT_TXT.read_bytes().decode("latin-1").splitlines()

records = []
for line in raw_lines:
    # Pad to at least 31 chars (1 + 10 + 1 + 20 - 1 = col 31) to avoid
    # IndexError on short or empty trailing records.
    rec = line.ljust(31)

    # @001 ACCTNO 10.  (numeric, positions 1-10, 0-based: 0-9)
    acctno_raw = rec[0:10].strip()
    acctno = int(acctno_raw) if acctno_raw.lstrip("-").isdigit() else None

    # @011 ESIGNATURE $1.  (position 11, 0-based: 10)
    esignature = rec[10:11]

    # @012 CONDIMODE $UPCASE20.  (positions 12-31, 0-based: 11-30)
    # $UPCASE20. reads exactly 20 chars and uppercases them.
    condimode_raw = rec[11:31].upper()

    # SAS: IF CONDIMODE NOT IN (list, ' ') THEN CONDIMODE = 'OTHERS';
    # ' ' in the SAS list represents a blank/missing CONDIMODE (all spaces).
    # We treat any all-whitespace 20-char field as the blank case (allowed).
    if condimode_raw.strip() == '' or condimode_raw in _ALLOWED_RAW:
        condimode = condimode_raw
    else:
        condimode = 'OTHERS              '  # padded to 20 chars for consistency

    records.append({
        "ACCTNO":      acctno,
        "ESIGNATURE":  esignature,
        "CONDIMODE":   condimode,
    })

SIGNA_SMSACC = pl.DataFrame(
    records,
    schema={
        "ACCTNO":     pl.Int64,
        "ESIGNATURE": pl.Utf8,
        "CONDIMODE":  pl.Utf8,
    },
)

# --------------------------------------------------------------------------
# PROC SORT DATA=SIGNA.SMSACC NODUPKEY; BY ACCTNO;
# --------------------------------------------------------------------------
SIGNA_SMSACC_NODUP = (
    SIGNA_SMSACC
    .sort("ACCTNO")
    .unique(subset=["ACCTNO"], keep="first")
)

write_tbl(SIGNA_SMSACC_NODUP, "SIGNA", "SMSACC")

# --------------------------------------------------------------------------
# PROC DATASETS LIBRARY=SIGNA;
#   MODIFY SMSACC; INDEX CREATE ACCTNO;
# Index creation on a Parquet file has no direct equivalent and is omitted.
# DuckDB or a downstream consumer can filter/join on ACCTNO directly.
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# PROC CPORT LIBRARY=SIGNA FILE=TRANFILE;
# FTP to SAS data warehouse (RUNSFTP / COZBATCH step):
# Transport-file generation and FTP transfer have no Python equivalent.
# The downstream data warehouse should consume the Parquet file directly.
# --------------------------------------------------------------------------
