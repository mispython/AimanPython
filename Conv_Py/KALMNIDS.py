#!/usr/bin/env python3
"""
Program : KALMNIDS.py
Purpose : Build K3TABI — classified NID (Negotiable Instrument of Deposit)
          records from BNMK.K3TBL, producing three datasets (CUSTM, RM, OM)
          which are concatenated into K3TABI for downstream RDAL reporting.

Logic overview:
  1. K3TB1     — read raw BNMK.K3TBL records
  2. K3X       — PROC SUMMARY: total UTAMOC per UTSMN group
  3. K3TABI    — merge K3X back; compute UTDPX and AMOUNT; filter UTSMN prefix 'I'
  4. CUSTM     — classify by UTCTP -> BNMCODE (customer type mapping)
  5. RM        — classify by remaining maturity (MATDT > REPTDATE -> REMMTH -> FDRMMT.)
  6. OM        — classify by original maturity from issue date (FDORGMT.)
  7. K3TABI    — SET CUSTM OM RM  (final concatenation)

Output : output/K3TABI<REPTMON><NOWK>.parquet

Input  : input/BNMK/K3TBL<REPTMON><NOWK>.parquet  (BNMK.K3TBL&REPTMON&NOWK)

Dependencies:
    - PBBDPFMT : fdrmmt_format  (SAS: FDRMMT.)
                 fdorgmt_format (SAS: FDORGMT.)

Note: %DCLVAR and %REMMTH macros are reproduced as Python helper functions
     using the same end-of-month alignment logic as in KALMPBBI.
     REPTDATE is sourced from the K3TBL records (column REPTDATE, SAS integer).
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# SAS: %INC PGM(PBBDPFMT);
# fdrmmt_format  -> SAS: PUT(REMMTH, FDRMMT.)
# fdorgmt_format -> SAS: PUT(REMMTH, FDORGMT.)
from PBBDPFMT import fdrmmt_format, fdorgmt_format

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BNMK_DIR = INPUT_DIR / "BNMK"

# Runtime macro variables — set by orchestrator or overridden here
REPTMON: str = "09"   # e.g. "09"
NOWK:    str = "4"    # e.g. "4"
# REPTDATE is read from the K3TBL dataset itself (column REPTDATE); not injected
# as a macro here since the SAS program derives it row-by-row via YEAR/MONTH/DAY.

# ---------------------------------------------------------------------------
# SAS date helpers
# ---------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_python(sas_days: int) -> date:
    """Convert SAS date integer (days since 1960-01-01) to Python date."""
    return SAS_EPOCH + timedelta(days=int(sas_days))


def python_date_to_sas(d: date) -> int:
    """Convert Python date to SAS date integer."""
    return (d - SAS_EPOCH).days


# ---------------------------------------------------------------------------
# %MACRO DCLVAR — days-per-month array (leap-year aware)
# ---------------------------------------------------------------------------
def _month_days(year: int) -> list[int]:
    """Return list of days-per-month [1..12] for given year (leap-year aware).
    Equivalent to SAS: IF MOD(RPYR,4)=0 THEN RD2=29; arrays RD1..RD12.
    """
    feb = 29 if year % 4 == 0 else 28
    return [31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


# ---------------------------------------------------------------------------
# %MACRO REMMTH — compute remaining months (KALMPBBI / K3TBL1 variant)
#
# This variant (used in DATA RM) caps MDDAY to RPDAYS(RPMTH) if it
# exceeds it (same as KALMPBBI K3TBL1 logic):
#   IF MDDAY > RPDAYS(RPMTH) THEN MDDAY = RPDAYS(RPMTH)
#
# Compare: FALQPBBI / KALQPBBI variant uses:
#   IF NDDAY = NDDAYS(NDMTH) THEN NDDAY = RPDAYS(RPMTH)   (only when end-of-month)
# ---------------------------------------------------------------------------
def compute_remmth_rm(
    matdt: date,
    rpyr:  int,
    rpmth: int,
    rpday: int,
    rp_days_arr: list[int],
) -> float:
    """
    Equivalent of %REMMTH in DATA RM block.

    MDYR  = YEAR(MATDT)
    MDMTH = MONTH(MATDT)
    MDDAY = DAY(MATDT)
    IF MDDAY > RPDAYS(RPMTH) THEN MDDAY = RPDAYS(RPMTH)
    REMMTH = (MDYR-RPYR)*12 + (MDMTH-RPMTH) + (MDDAY-RPDAY)/RPDAYS(RPMTH)
    """
    md_yr  = matdt.year
    md_mth = matdt.month
    md_day = matdt.day

    rp_mth_days = rp_days_arr[rpmth - 1]

    # Cap MDDAY to RPDAYS(RPMTH) if it exceeds it
    if md_day > rp_mth_days:
        md_day = rp_mth_days

    rem_y = md_yr  - rpyr
    rem_m = md_mth - rpmth
    rem_d = md_day - rpday

    return rem_y * 12 + rem_m + rem_d / rp_mth_days


# ---------------------------------------------------------------------------
# %MACRO REMMTH — compute remaining months (DATA OM variant: from ISSDT)
#
# In DATA OM, the reference date is ISSDT (issue date), NOT REPTDATE.
# The macro variables are overridden at observation level:
#   RPYR  = YEAR(ISSDT)
#   RPMTH = MONTH(ISSDT)
#   RPDAY = DAY(ISSDT)
# and the arrays are recomputed per-observation based on ISSDT's year.
# MATDT used in OM is effectively REPTDATE (the report date).
# ---------------------------------------------------------------------------
def compute_remmth_om(
    issdt:    date,
    reptdate: date,
) -> float:
    """
    Equivalent of %REMMTH in DATA OM block.

    In DATA OM, SAS sets:
        RPYR  = YEAR(ISSDT)
        RPMTH = MONTH(ISSDT)
        RPDAY = DAY(ISSDT)
    and then runs %REMMTH with MATDT = REPTDATE.
    So REMMTH measures elapsed months from ISSDT to REPTDATE
    (i.e. original term / seasoning), not remaining life.

    Applies same cap rule: IF MDDAY > RPDAYS(RPMTH) THEN MDDAY=RPDAYS(RPMTH).
    Here RPDAYS is relative to ISSDT's month.
    """
    # "RP" context = ISSDT
    rpyr  = issdt.year
    rpmth = issdt.month
    rpday = issdt.day
    rp_days_arr = _month_days(rpyr)
    rp_mth_days = rp_days_arr[rpmth - 1]

    # "MD" context = REPTDATE (the target date in the original macro)
    md_yr  = reptdate.year
    md_mth = reptdate.month
    md_day = reptdate.day

    if md_day > rp_mth_days:
        md_day = rp_mth_days

    rem_y = md_yr  - rpyr
    rem_m = md_mth - rpmth
    rem_d = md_day - rpday

    return rem_y * 12 + rem_m + rem_d / rp_mth_days


# ---------------------------------------------------------------------------
# UTCTP -> BNMCODE mapping (DATA CUSTM SELECT/WHEN block)
# ---------------------------------------------------------------------------
# Eligible UTCTP values (the IF UTCTP IN (...) guard)
_CUSTM_ELIGIBLE = {
    'BB','BQ','BM','BN','BG','BR','BF','BH','BZ',
    'BU','AD','BT','BV','BS','AC','DD','CG','CA',
    'CB','CC','CD','CF','BW','BA','BE','DA',
    'DB','DC','EC','EA','FA','EB','CE','GA','BJ',
}


def _map_utctp_to_bnmcode(utctp: str) -> str:
    """
    SELECT(UTCTP) mapping from DATA CUSTM.
    Returns BNMCODE string (14 chars) or '' if no match.

    Note: 'BQ' is mapped to '4215002000000Y' alongside 'BB' (the commented-out
    '4215011000000Y' line is preserved as a comment below).
    """
    utctp = (utctp or '').strip()
    if utctp in ('BB', 'BQ'):
        return '4215002000000Y'
        # WHEN('BQ') BNMCODE='4215011000000Y';  <- commented out in original
    if utctp == 'BJ':
        return '4215007000000Y'
    if utctp == 'BM':
        return '4215012000000Y'
    if utctp == 'BN':
        return '4215013000000Y'
    if utctp == 'BG':
        return '4215017000000Y'
    if utctp in ('BR','BF','BH','BZ','BU','AD','BT','BV','BS','BN'):
        return '4215020000000Y'
    if utctp in ('AC','DD','CG','CA','CB','CC','CD','CF'):
        return '4215060000000Y'
    if utctp == 'DA':
        return '4215071000000Y'
    if utctp == 'DB':
        return '4215072000000Y'
    if utctp == 'DC':
        return '4215074000000Y'
    if utctp in ('EC', 'EA'):
        return '4215076000000Y'
    if utctp == 'FA':
        return '4215079000000Y'
    if utctp in ('BW','BA','BE'):
        return '4215081000000Y'
    if utctp in ('EB','CE','GA'):
        return '4215085000000Y'
    return ''


# ---------------------------------------------------------------------------
# Helper: read parquet
# ---------------------------------------------------------------------------
def _read(path: Path) -> pl.DataFrame:
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    return df


# ---------------------------------------------------------------------------
# Step 1 — DATA K3TB1: read BNMK.K3TBL
# ---------------------------------------------------------------------------
def build_k3tb1() -> pl.DataFrame:
    """
    DATA K3TB1;
      SET BNMK.K3TBL&REPTMON&NOWK;
    """
    path = BNMK_DIR / f"K3TBL{REPTMON}{NOWK}.parquet"
    return _read(path)


# ---------------------------------------------------------------------------
# Step 2 — PROC SUMMARY: total UTAMOC per UTSMN (-> K3X)
# Step 3 — DATA K3TABI: merge K3X + K3TB1, compute UTDPX / AMOUNT
#           Filter: SUBSTR(UTSMN,1,1)='I'; AMTIND='I'
# ---------------------------------------------------------------------------
def build_k3tabi(k3tb1: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SUMMARY DATA=K3TB1 NWAY;
    CLASS UTSMN; VAR UTAMOC;
    OUTPUT OUT=K3X (DROP=_TYPE_ _FREQ_) SUM=TTAMOC;

    PROC SORT DATA=K3TB1; BY UTSMN;

    DATA K3TABI;
      MERGE K3X K3TB1; BY UTSMN;
      UTDPX  = (UTAMOC / TTAMOC) * UTDPF;
      IF UTDPX < 0 THEN UTDPX = (-1) * UTDPX;   <- abs()
      AMOUNT = UTAMOC - UTDPX;
      IF SUBSTR(UTSMN,1,1)='I';
      AMTIND='I';
    """
    # PROC SUMMARY -> K3X: sum UTAMOC by UTSMN
    k3x = (
        k3tb1
        .group_by("UTSMN")
        .agg(pl.col("UTAMOC").sum().alias("TTAMOC"))
    )

    # MERGE K3X K3TB1 BY UTSMN (left join: K3TB1 is the driver)
    k3tabi = k3tb1.join(k3x, on="UTSMN", how="left")

    # Compute UTDPX and AMOUNT
    k3tabi = k3tabi.with_columns([
        # UTDPX = (UTAMOC / TTAMOC) * UTDPF; IF UTDPX < 0 THEN UTDPX = ABS(UTDPX)
        (
            pl.when(pl.col("TTAMOC") != 0)
            .then((pl.col("UTAMOC") / pl.col("TTAMOC")) * pl.col("UTDPF"))
            .otherwise(pl.lit(0.0))
            .abs()
            .alias("UTDPX")
        ),
    ]).with_columns([
        # AMOUNT = UTAMOC - UTDPX
        (pl.col("UTAMOC") - pl.col("UTDPX")).alias("AMOUNT"),
        # AMTIND = 'I'
        pl.lit("I").alias("AMTIND"),
    ])

    # IF SUBSTR(UTSMN,1,1)='I'  (keep only 'I' prefix UTSMN)
    k3tabi = k3tabi.filter(
        pl.col("UTSMN").cast(pl.Utf8).str.slice(0, 1) == "I"
    )

    return k3tabi


# ---------------------------------------------------------------------------
# Step 4 — DATA CUSTM: classify by UTCTP
# ---------------------------------------------------------------------------
def build_custm(k3tabi: pl.DataFrame) -> pl.DataFrame:
    """
    DATA CUSTM;
      LENGTH BNMCODE $14.;
      SET K3TABI;
      BNMCODE=' ';
      IF UTCTP IN (...);
        SELECT(UTCTP); ... BNMCODE=...; END;
      IF BNMCODE ^= ' ' THEN OUTPUT;
    """
    rows = []
    for row in k3tabi.iter_rows(named=True):
        utctp = (row.get("UTCTP") or "").strip()
        if utctp not in _CUSTM_ELIGIBLE:
            continue
        bnmcode = _map_utctp_to_bnmcode(utctp)
        if not bnmcode or bnmcode.strip() == '':
            continue
        r = dict(row)
        r["BNMCODE"] = bnmcode
        rows.append(r)

    if not rows:
        return pl.DataFrame(schema={**{c: k3tabi.schema[c] for c in k3tabi.columns},
                                     "BNMCODE": pl.Utf8})
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Step 5 — DATA RM: classify by remaining maturity (MATDT > REPTDATE)
# ---------------------------------------------------------------------------
def build_rm(k3tabi: pl.DataFrame) -> pl.DataFrame:
    """
    DATA RM;
      LENGTH BNMCODE $14.;
      %DCLVAR
      SET K3TABI;
      IF _N_=1 THEN DO;
         RPYR  = YEAR(REPTDATE);
         RPMTH = MONTH(REPTDATE);
         RPDAY = DAY(REPTDATE);
         IF MOD(RPYR,4) = 0 THEN RD2 = 29;
      END;
      BNMCODE='  ';
      IF MATDT > REPTDATE THEN DO;
         %REMMTH
         REMMT  = PUT(REMMTH, FDRMMT.);
         BNMCODE = '42150' || REMMT || '000000Y';
      END;
      IF BNMCODE^=' ' THEN OUTPUT;

    Note: REPTDATE is taken from the K3TABI rows (SAS date integer column).
    The RP* variables are set once from the FIRST observation (_N_=1).
    """
    # Determine REPTDATE from first row (_N_=1)
    if k3tabi.height == 0:
        return pl.DataFrame(schema={**{c: k3tabi.schema[c] for c in k3tabi.columns},
                                     "BNMCODE": pl.Utf8})

    first_row = k3tabi.row(0, named=True)
    reptdate_sas = int(first_row.get("REPTDATE") or 0)
    reptdate = sas_date_to_python(reptdate_sas)

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rp_days_arr = _month_days(rpyr)

    rows = []
    for row in k3tabi.iter_rows(named=True):
        matdt_sas = row.get("MATDT")
        if matdt_sas is None:
            continue
        reptdate_row_sas = int(row.get("REPTDATE") or reptdate_sas)
        # IF MATDT > REPTDATE
        if int(matdt_sas) <= reptdate_row_sas:
            continue

        matdt = sas_date_to_python(int(matdt_sas))

        remmth = compute_remmth_rm(matdt, rpyr, rpmth, rpday, rp_days_arr)
        remmt  = fdrmmt_format(remmth)          # PUT(REMMTH, FDRMMT.)

        bnmcode = '42150' + remmt + '000000Y'   # LENGTH 14
        # IF BNMCODE ^= ' ' — always non-blank since prefix is fixed

        r = dict(row)
        r["BNMCODE"] = bnmcode
        rows.append(r)

    if not rows:
        return pl.DataFrame(schema={**{c: k3tabi.schema[c] for c in k3tabi.columns},
                                     "BNMCODE": pl.Utf8})
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Step 6 — DATA OM: classify by original maturity (from ISSDT)
# ---------------------------------------------------------------------------
def build_om(k3tabi: pl.DataFrame) -> pl.DataFrame:
    """
    DATA OM;
      LENGTH ORIGMT $2. BNMCODE $14.;
      %DCLVAR
      SET K3TABI;
      IF ISSDT=' ' THEN ISSDT=REPTDATE;       <- fallback: use REPTDATE as ISSDT
      RPYR  = YEAR(ISSDT);
      RPMTH = MONTH(ISSDT);
      RPDAY = DAY(ISSDT);
      IF MOD(RPYR,4) = 0 THEN RD2 = 29;
      %REMMTH                                  <- REMMTH from ISSDT to REPTDATE
      ORIGMT = PUT(REMMTH, FDORGMT.);
      IF ORIGMT IN ('12','13','14') THEN ORIGMT='14';
      BNMCODE = '42150' || ORIGMT || '000000Y';
      IF BNMCODE^=' ' THEN OUTPUT;

    Note: In DATA OM the macro %REMMTH uses ISSDT as the "RP" reference (not
    REPTDATE), and REPTDATE as the target date (MATDT equivalent). So REMMTH
    here is the elapsed months from issue date to report date — the original
    term / seasoning indicator — used to classify into FDORGMT buckets.
    """
    if k3tabi.height == 0:
        return pl.DataFrame(schema={**{c: k3tabi.schema[c] for c in k3tabi.columns},
                                     "BNMCODE": pl.Utf8})

    rows = []
    for row in k3tabi.iter_rows(named=True):
        reptdate_sas = int(row.get("REPTDATE") or 0)
        reptdate     = sas_date_to_python(reptdate_sas)

        issdt_val = row.get("ISSDT")
        # IF ISSDT=' ' THEN ISSDT=REPTDATE
        if issdt_val is None or issdt_val == '' or issdt_val == 0:
            issdt = reptdate
        else:
            issdt = sas_date_to_python(int(issdt_val))

        remmth = compute_remmth_om(issdt, reptdate)
        origmt = fdorgmt_format(remmth)          # PUT(REMMTH, FDORGMT.)

        # IF ORIGMT IN ('12','13','14') THEN ORIGMT='14'
        if origmt in ('12', '13', '14'):
            origmt = '14'

        bnmcode = '42150' + origmt + '000000Y'   # LENGTH 14
        # IF BNMCODE^=' ' — always non-blank since prefix is fixed

        r = dict(row)
        r["BNMCODE"] = bnmcode
        rows.append(r)

    if not rows:
        return pl.DataFrame(schema={**{c: k3tabi.schema[c] for c in k3tabi.columns},
                                     "BNMCODE": pl.Utf8})
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Step 7 — DATA K3TABI: SET CUSTM OM RM
# ---------------------------------------------------------------------------
def build_final_k3tabi(
    custm: pl.DataFrame,
    om:    pl.DataFrame,
    rm:    pl.DataFrame,
) -> pl.DataFrame:
    """
    DATA K3TABI;
      SET CUSTM OM RM;
    (Note: SAS order is CUSTM, OM, RM — OM before RM)
    """
    return pl.concat([custm, om, rm], how="diagonal")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print(f"[KALMNIDS] REPTMON={REPTMON}  NOWK={NOWK}")

    # DATA K3TB1
    k3tb1 = build_k3tb1()
    print(f"  K3TB1 rows: {k3tb1.height}")

    # DATA K3TABI (intermediate)
    k3tabi = build_k3tabi(k3tb1)
    print(f"  K3TABI (filtered) rows: {k3tabi.height}")

    # DATA CUSTM
    custm = build_custm(k3tabi)
    print(f"  CUSTM rows: {custm.height}")

    # DATA RM
    rm = build_rm(k3tabi)
    print(f"  RM rows: {rm.height}")

    # DATA OM
    om = build_om(k3tabi)
    print(f"  OM rows: {om.height}")

    # DATA K3TABI (final): SET CUSTM OM RM
    k3tabi_final = build_final_k3tabi(custm, om, rm)
    print(f"  K3TABI (final) rows: {k3tabi_final.height}")

    # Write output
    output_path = OUTPUT_DIR / f"K3TABI{REPTMON}{NOWK}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    k3tabi_final.write_parquet(output_path)
    print(f"  -> {output_path}")
    print("[KALMNIDS] Done.")


if __name__ == "__main__":
    main()
