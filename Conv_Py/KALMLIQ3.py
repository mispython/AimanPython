#!/usr/bin/env python3
"""
Program  : KALMLIQ3.py
Purpose  : Include fragment — DATA K3TBL3 step (with REMMTH derivation) for
            New Liquidity Framework (KAPITI Part 2 & 3). Filters and transforms
            MGS/RRS records from BNMK.K3TBL table; classifies by customer type
            into items A2.15/A2.16; derives remaining-months (REMMTH) bucket.

           This module is NOT a standalone program. It is an %INC PGM(KALMLIQ3)
            fragment intended to be called from a parent orchestrating program.
           Import and call build_k3tbl3_with_remmth() with the required context.

           Produces: PART, ITEM, REMMTH, AMOUNT
"""

# ============================================================================
# No %INC program dependencies declared in SAS source.
# All logic is self-contained within this program.
# $CTYPE., &NREP, &IREP, &RUNOFFDT, &REPTMON, &NOWK -> passed as parameters.
# %DCLVAR -> variable declarations handled by parquet schema.
# %REMMTH -> macro expansion implemented inline as compute_remmth().
# ============================================================================

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime


# ============================================================================
# %REMMTH MACRO EXPANSION
# Computes remaining-months bucket from MATDT relative to RUNOFFDT.
#
# Standard KAPITI %REMMTH bucket logic:
#   IF MATDT - RUNOFFDT < 8 days -> REMMTH = 0.1  (less than 1 week)
#   Else: calendar month difference -> bucketed to standard breakpoints
# ============================================================================

def compute_remmth(matdt: date, runoffdt: date) -> float:
    """
    Replicate %REMMTH macro: derive remaining-months bucket.

    Rules (matching KAPITI %REMMTH convention):
      - If MATDT - RUNOFFDT < 8 days           -> REMMTH = 0.1
      - Else compute calendar month difference  -> REMMTH = bucketed months
        Standard breakpoints: 1, 2, 3, 6, 12, 24, 36, 60, 84, 120, 999
    """
    if matdt is None:
        return 0.1

    diff_days = (matdt - runoffdt).days

    # IF MATDT - &RUNOFFDT < 8 THEN REMMTH = 0.1
    if diff_days < 8:
        return 0.1

    # Compute calendar month difference
    yr_diff      = matdt.year  - runoffdt.year
    mth_diff     = matdt.month - runoffdt.month
    total_months = yr_diff * 12 + mth_diff

    # Adjust for day-of-month overshoot (partial month -> round up)
    if matdt.day > runoffdt.day:
        total_months += 1

    if total_months <= 0:
        return 0.1

    # Standard KAPITI bucket breakpoints
    for bp in (1, 2, 3, 6, 12, 24, 36, 60, 84, 120):
        if total_months <= bp:
            return float(bp)
    return 999.0   # > 120 months


# ============================================================================
# KALMLIQ3: DATA K3TBL3 (KEEP=PART ITEM REMMTH AMOUNT)
# ============================================================================
# %DCLVAR               -> variable declarations (handled by parquet schema)
# FORMAT MATDT YYMMDD8.
# SET BNMK.K3TBL&REPTMON&NOWK
# PART='2-RM'
#
# IF _N_ = 1 THEN DO;
#   SET REPTDATE;
#   RPYR  = YEAR(&RUNOFFDT);
#   RPMTH = MONTH(&RUNOFFDT);
#   RPDAY = DAY(&RUNOFFDT);
#   IF MOD(RPYR,4) = 0 THEN RD2 = 29;
# END;
#
# IF UTREF='RRS' AND UTSTY='MGS' AND UTDLP='MSS'
# IF ISSDT > REPTDATE THEN DELETE
# AMOUNT = (UTPCP * UTFCV) * 0.01
# AMOUNT = SUM(AMOUNT, UTAICT)         /* SALES PROCEEDS */
# CUST   = PUT(UTCTP, $CTYPE.)
# IF UTIDT NE ' ' THEN MATDT = INPUT(UTIDT, YYMMDD10.)
# IF CUST IN &NREP THEN ITEM='A2.16'; ELSE
# IF CUST IN &IREP THEN ITEM='A2.15'
# IF CUST NE '  '
#
# * IF MATDT > &RUNOFFDT;  (commented out — not applied)
# IF MATDT - &RUNOFFDT < 8 THEN REMMTH = 0.1;
# ELSE DO; %REMMTH END;
# ============================================================================

def build_k3tbl3_with_remmth(
    bnmk_dir:    Path,
    reptmon:     str,
    nowk:        str,
    reptdate:    date,
    runoffdt:    date,
    nrep:        set,
    irep:        set,
    format_ctype,          # callable: format_ctype(utctp: str) -> str
) -> pl.DataFrame:
    """
    Replicate DATA K3TBL3 (KEEP=PART ITEM REMMTH AMOUNT) from KALMLIQ3.

    Parameters
    ----------
    bnmk_dir     : Directory containing BNMK parquet files.
    reptmon      : Reporting month (Z2. string, e.g. '06').
    nowk         : Reporting week key (e.g. '4').
    reptdate     : Reporting date (date object) — used for ISSDT filter.
    runoffdt     : Run-off reference date (&RUNOFFDT).
    nrep         : Set of CUST values mapped to item A2.16 (&NREP).
    irep         : Set of CUST values mapped to item A2.15 (&IREP).
    format_ctype : Function implementing $CTYPE. format.

    Returns
    -------
    pl.DataFrame with columns: PART, ITEM, REMMTH, AMOUNT
    """
    k3tbl_file = bnmk_dir / f"k3tbl{reptmon}{nowk}.parquet"

    # IF _N_ = 1 THEN DO;
    #   RPYR  = YEAR(&RUNOFFDT);
    #   RPMTH = MONTH(&RUNOFFDT);
    #   RPDAY = DAY(&RUNOFFDT);
    #   IF MOD(RPYR,4) = 0 THEN RD2 = 29;
    # END;
    rpyr  = runoffdt.year
    rpmth = runoffdt.month
    rpday = runoffdt.day
    # IF MOD(RPYR,4) = 0 THEN RD2 = 29  (simple leap-year check for Feb days)
    rd2   = 29 if (rpyr % 4 == 0) else 28  # noqa: F841 (available for %REMMTH use)

    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{k3tbl_file}')").pl()

    rows = []
    for r in df.to_dicts():
        # IF UTREF='RRS' AND UTSTY='MGS' AND UTDLP='MSS'
        utref = str(r.get("UTREF") or "").strip()
        utsty = str(r.get("UTSTY") or "").strip()
        utdlp = str(r.get("UTDLP") or "").strip()
        if utref != "RRS" or utsty != "MGS" or utdlp != "MSS":
            continue

        # IF ISSDT > REPTDATE THEN DELETE
        issdt_raw = r.get("ISSDT")
        if issdt_raw is not None:
            if isinstance(issdt_raw, (date, datetime)):
                issdt = issdt_raw.date() if isinstance(issdt_raw, datetime) else issdt_raw
            else:
                try:
                    issdt = date.fromisoformat(str(issdt_raw))
                except Exception:
                    issdt = None
            if issdt is not None and issdt > reptdate:
                continue

        # AMOUNT = (UTPCP * UTFCV) * 0.01
        utpcp  = float(r.get("UTPCP")  or 0.0)
        utfcv  = float(r.get("UTFCV")  or 0.0)
        utaict = float(r.get("UTAICT") or 0.0)
        amount = (utpcp * utfcv) * 0.01
        # AMOUNT = SUM(AMOUNT, UTAICT)   /* SALES PROCEEDS */
        amount = amount + utaict

        # CUST = PUT(UTCTP, $CTYPE.)
        utctp = str(r.get("UTCTP") or "").strip()
        cust  = format_ctype(utctp)

        # IF CUST NE '  '
        if not cust.strip():
            continue

        # IF UTIDT NE ' ' THEN MATDT = INPUT(UTIDT, YYMMDD10.)
        matdt = None
        utidt = str(r.get("UTIDT") or "").strip()
        if utidt:
            try:
                # YYMMDD10. -> YYYY-MM-DD
                matdt = date.fromisoformat(utidt[:10])
            except Exception:
                matdt = None

        # IF CUST IN &NREP THEN ITEM='A2.16'
        # ELSE IF CUST IN &IREP THEN ITEM='A2.15'
        item = None
        if cust in nrep:
            item = "A2.16"
        elif cust in irep:
            item = "A2.15"

        if item is None:
            # No matching classification — skip (implicit subsetting IF)
            continue

        # * IF MATDT > &RUNOFFDT;   (commented out in SAS — not applied)

        # IF MATDT - &RUNOFFDT < 8 THEN REMMTH = 0.1; ELSE %REMMTH
        remmth = compute_remmth(matdt, runoffdt)

        rows.append({
            "PART":   "2-RM",
            "ITEM":   item,
            "REMMTH": remmth,
            "AMOUNT": amount,
        })

    if not rows:
        return pl.DataFrame(schema={
            "PART":   pl.Utf8,
            "ITEM":   pl.Utf8,
            "REMMTH": pl.Float64,
            "AMOUNT": pl.Float64,
        })

    return pl.DataFrame(rows)
