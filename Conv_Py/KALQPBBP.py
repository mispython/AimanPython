#!/usr/bin/env python3
"""
Program : KALQPBBP.py
Date    : 11/12/96
Report  : RDAL PART III (KAPITI ITEMS)

Purpose:
    - Reads KAPITI K1TBL<MM><WK> parquet (GWBALC renamed AMOUNT).
    - Classifies deposit records for non-residents by remaining maturity
      into BNMCODE using BIC prefix and KREMMTH format.
    - Summarises by BNMCODE / AMTIND and appends to BNM.KALQ<MM><WK>.
"""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# Dependency: KALMPBBF provides format_kremmth and BNMCODE reference data.
from KALMPBBF import format_kremmth

# ---------------------------------------------------------------------------
# Runtime parameters (mirrors SAS macro variables set upstream)
# REPTMON  – two-digit month  e.g. "03"
# NOWK     – week number      e.g. "1"
# RDATE    – report date      e.g. "31032024"  (DDMMYYYY)
# ---------------------------------------------------------------------------
REPTMON = os.environ.get("REPTMON", "")
NOWK    = os.environ.get("NOWK",    "")
RDATE   = os.environ.get("RDATE",   "")

if not REPTMON or not NOWK or not RDATE:
    print("ERROR: Environment variables REPTMON, NOWK, and RDATE must be set.")
    print("  e.g.  REPTMON=03  NOWK=1  RDATE=31032024")
    sys.exit(1)

try:
    rp_day   = int(RDATE[0:2])
    rp_month = int(RDATE[2:4])
    rp_year  = int(RDATE[4:8])
    REPTDATE = date(rp_year, rp_month, rp_day)
except (ValueError, IndexError):
    print(f"ERROR: Cannot parse RDATE='{RDATE}'. Expected DDMMYYYY.")
    sys.exit(1)

# %LET AMTIND = 'I'
AMTIND = "I"

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# BNMK.K1TBL<MM><WK>  → input/K1TBL<MM><WK>.parquet
K1TBL_PARQUET = INPUT_DIR / f"K1TBL{REPTMON}{NOWK}.parquet"

# BNM.KALQ<MM><WK>     → output/KALQ<MM><WK>.parquet  (append target)
KALQ_PARQUET  = OUTPUT_DIR / f"KALQ{REPTMON}{NOWK}.parquet"


# ---------------------------------------------------------------------------
# %MACRO DCLVAR – days-per-month arrays
# ---------------------------------------------------------------------------
def _month_days(year: int) -> list[int]:
    """Return days-per-month list for the given year (0-indexed positions 0..11)."""
    feb = 29 if year % 4 == 0 else 28
    return [31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


# ---------------------------------------------------------------------------
# %MACRO REMMTH – compute remaining months to NDDATE from REPTDATE
# ---------------------------------------------------------------------------
def compute_remmth(nddate: date, rpyr: int, rpmth: int, rpday: int,
                   rp_days: list[int]) -> float:
    """
    Equivalent of %MACRO REMMTH.
    ND2 (Feb days for ND year) is computed per observation.
    IF NDDAY = NDDAYS(NDMTH) THEN NDDAY = RPDAYS(RPMTH)  (end-of-month alignment).
    """
    nd_yr  = nddate.year
    nd_mth = nddate.month
    nd_day = nddate.day
    nd_days = _month_days(nd_yr)

    # IF NDDAY = NDDAYS(NDMTH) THEN NDDAY = RPDAYS(RPMTH)
    if nd_day == nd_days[nd_mth - 1]:
        nd_day = rp_days[rpmth - 1]

    rem_y = nd_yr  - rpyr
    rem_m = nd_mth - rpmth
    rem_d = nd_day - rpday
    return rem_y * 12 + rem_m + rem_d / rp_days[rpmth - 1]


# ---------------------------------------------------------------------------
# DATA K1TABL – classify deposit records
# ---------------------------------------------------------------------------
def build_k1tabl(k1tbl_path: Path, reptdate: date) -> pl.DataFrame:
    """
    Read K1TBL, apply filter and classification logic, return K1TABL rows.

    Columns expected in K1TBL parquet (RENAME=(GWBALC=AMOUNT) applied here):
        GWBALC (→ AMOUNT), GWMVT, GWMVTS, GWDLP, GWMDT, GWNANC, GWACT,
        GWSAC, GWAN, GWCNAP, GWCTP, GWCNAL
    """
    con = duckdb.connect()
    raw = con.execute(f"SELECT * FROM read_parquet('{k1tbl_path}')").pl()
    con.close()

    # RENAME=(GWBALC=AMOUNT)
    if "GWBALC" in raw.columns:
        raw = raw.rename({"GWBALC": "AMOUNT"})

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rp_days = _month_days(rpyr)

    out_rows: list[dict] = []

    for row in raw.iter_rows(named=True):
        gwmvt  = (row.get("GWMVT")  or "").strip()
        gwmvts = (row.get("GWMVTS") or "").strip()

        # IF GWMVT EQ 'P' AND GWMVTS EQ 'M'
        if gwmvt != "P" or gwmvts != "M":
            continue

        gwdlp  = (row.get("GWDLP")  or "").strip()
        gwnanc = (row.get("GWNANC") or "").strip()
        gwact  = (row.get("GWACT")  or "").strip()
        gwsac  = (row.get("GWSAC")  or "").strip()
        gwan   = (row.get("GWAN")   or "").strip()
        gwcnap = (row.get("GWCNAP") or "").strip()
        gwctp  = (row.get("GWCTP")  or "").strip()
        gwcnal = (row.get("GWCNAL") or "").strip()
        amount = float(row.get("AMOUNT") or 0.0)

        # Determine BIC
        bic = ""
        if gwdlp in ("FDA", "FDB", "FDL", "FDS"):
            bic = "42000"
        elif gwdlp in ("BF", "BO"):
            bic = "48000"

        # IF BIC ^= ' '
        if not bic:
            continue

        # NDDATE = GWMDT
        gwmdt = row.get("GWMDT")
        if gwmdt is None:
            continue
        if hasattr(gwmdt, "date"):
            gwmdt = gwmdt.date()

        remmth = compute_remmth(gwmdt, rpyr, rpmth, rpday, rp_days)

        # RM determination
        if gwnanc == "00001" and gwact == "CV":
            rm = "51"
        else:
            rm = format_kremmth(remmth)

        bnmcode = ""

        # First conditional block
        if (gwsac == "UO" and gwact == "CV"
                and gwan[:1] == "0"
                and (gwcnap == "MY" or gwan == "000418")):
            bnmcode = bic + "82" + rm + "0000Y"
            out_rows.append({"BNMCODE": bnmcode, "AMOUNT": amount, "AMTIND": AMTIND})
        elif (not ("BA" <= gwctp <= "BZ") and gwcnal != "MY" and gwsac == "UF"):
            bnmcode = bic + "85" + rm + "0000Y"
            out_rows.append({"BNMCODE": bnmcode, "AMOUNT": amount, "AMTIND": AMTIND})

        # IF BNMCODE=' ' – second conditional block
        if not bnmcode:
            if gwcnal != "MY":
                g7_countries = ("CA", "FR", "IT", "DE", "JP", "US", "GB")
                if gwcnap in g7_countries:
                    bnmcode = bic + "83" + rm + "0000Y"
                else:
                    bnmcode = bic + "84" + rm + "0000Y"
                out_rows.append({"BNMCODE": bnmcode, "AMOUNT": amount, "AMTIND": AMTIND})

    return pl.DataFrame(out_rows, schema={
        "BNMCODE": pl.Utf8,
        "AMOUNT":  pl.Float64,
        "AMTIND":  pl.Utf8,
    })


# ---------------------------------------------------------------------------
# PROC SUMMARY DATA=K1TABL NWAY; CLASS BNMCODE AMTIND; VAR AMOUNT; SUM=AMOUNT
# ---------------------------------------------------------------------------
def summarise_k1tabl(df: pl.DataFrame) -> pl.DataFrame:
    """Group by BNMCODE / AMTIND and sum AMOUNT."""
    return (
        df.group_by(["BNMCODE", "AMTIND"])
          .agg(pl.col("AMOUNT").sum())
    )


# ---------------------------------------------------------------------------
# PROC APPEND DATA=K1TABL BASE=BNM.KALQ<MM><WK>
# PROC DATASETS LIB=BNM NOLIST; DELETE KALQ<MM><WK>  →  delete before append
# ---------------------------------------------------------------------------
def append_to_kalq(summary: pl.DataFrame, kalq_path: Path) -> None:
    """
    Equivalent of:
      PROC DATASETS LIB=BNM NOLIST; DELETE KALQ&REPTMON&NOWK;  (at program start)
      PROC APPEND DATA=K1TABL BASE=BNM.KALQ&REPTMON&NOWK;
    The DELETE is handled in main() before this function is called.
    If the target already exists (from a prior append in the same run), merge.
    """
    if kalq_path.exists():
        con  = duckdb.connect()
        existing = con.execute(f"SELECT * FROM read_parquet('{kalq_path}')").pl()
        con.close()
        combined = pl.concat([existing, summary])
    else:
        combined = summary

    combined.write_parquet(str(kalq_path))


# ---------------------------------------------------------------------------
# PROC PRINT (informational – print summary to stdout)
# ---------------------------------------------------------------------------
def proc_print(df: pl.DataFrame) -> None:
    """Print K1TABL summary to stdout (PROC PRINT equivalent)."""
    print(df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print(f"KALQPBBP  REPTMON={REPTMON}  NOWK={NOWK}  RDATE={RDATE}")

    # PROC DATASETS LIB=BNM NOLIST; DELETE KALQ&REPTMON&NOWK  (reset output)
    if KALQ_PARQUET.exists():
        KALQ_PARQUET.unlink()
        print(f"Deleted existing {KALQ_PARQUET}")

    # DATA K1TABL – classify records
    k1tabl = build_k1tabl(K1TBL_PARQUET, REPTDATE)
    print(f"K1TABL rows before summary: {len(k1tabl)}")

    # PROC SUMMARY
    summary = summarise_k1tabl(k1tabl)

    # PROC PRINT
    proc_print(summary)

    # PROC APPEND → BNM.KALQ<MM><WK>
    append_to_kalq(summary, KALQ_PARQUET)
    print(f"Appended {len(summary)} rows → {KALQ_PARQUET}")

    # PROC DATASETS LIB=WORK NOLIST; DELETE K1TABL  (work dataset auto-cleaned; no-op)


if __name__ == "__main__":
    main()
