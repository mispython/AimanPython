#!/usr/bin/env python3
"""
Program  : KALMLIQ1.py
Purpose  : Include fragment — DATA K3TBL3 step for New Liquidity Framework
            (KAPITI Part 2 & 3). Filters and transforms MGS/RRS records from
            BNMK.K3TBL table; classifies by customer type into items A2.15/A2.16.

           This module is NOT a standalone program. It is an %INC PGM(KALMLIQ1)
            fragment intended to be called from a parent orchestrating program.
           Import and call build_k3tbl3() with the required context parameters.
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
# $CTYPE. format -> format_ctype() expected from parent program's format library.
# &NREP, &IREP   -> customer type classification lists passed as parameters.
# &REPTMON, &NOWK -> reporting period identifiers passed as parameters.
# REPTDATE        -> reporting date value passed as parameter.

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime


# ============================================================================
# KALMLIQ1: DATA K3TBL3
# ============================================================================
# FORMAT MATDT YYMMDD8.
# SET BNMK.K3TBL&REPTMON&NOWK
# PART='2-RM'
# IF UTREF='RRS' AND UTSTY='MGS' AND UTDLP='MSS'
# IF ISSDT > REPTDATE THEN DELETE
# AMOUNT = (UTPCP * UTFCV) * 0.01
# AMOUNT = SUM(AMOUNT, UTAICT)         /* SALES PROCEEDS */
# CUST   = PUT(UTCTP, $CTYPE.)
# IF UTIDT NE ' ' THEN MATDT = INPUT(UTIDT, YYMMDD10.)
# IF CUST IN &NREP THEN ITEM='A2.16'; ELSE
# IF CUST IN &IREP THEN ITEM='A2.15'
# IF CUST NE '  '
# ============================================================================

def build_k3tbl3(
    bnmk_dir:    Path,
    reptmon:     str,
    nowk:        str,
    reptdate:    date,
    nrep:        set,
    irep:        set,
    format_ctype,          # callable: format_ctype(utctp: str) -> str
) -> pl.DataFrame:
    """
    Replicate DATA K3TBL3 from KALMLIQ1.

    Parameters
    ----------
    bnmk_dir     : Directory containing BNMK parquet files.
    reptmon      : Reporting month (Z2. string, e.g. '06').
    nowk         : Reporting week key (e.g. '4').
    reptdate     : Reporting date (date object) — used for ISSDT filter.
    nrep         : Set of CUST values mapped to item A2.16 (&NREP).
    irep         : Set of CUST values mapped to item A2.15 (&IREP).
    format_ctype : Function implementing $CTYPE. format.

    Returns
    -------
    pl.DataFrame with columns: PART, ITEM, MATDT, AMOUNT
    """
    k3tbl_file = bnmk_dir / f"k3tbl{reptmon}{nowk}.parquet"

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
        matdt  = None
        utidt  = str(r.get("UTIDT") or "").strip()
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

        rows.append({
            "PART":   "2-RM",
            "ITEM":   item,
            "MATDT":  matdt,
            "AMOUNT": amount,
        })

    if not rows:
        return pl.DataFrame(schema={
            "PART":   pl.Utf8,
            "ITEM":   pl.Utf8,
            "MATDT":  pl.Date,
            "AMOUNT": pl.Float64,
        })

    return pl.DataFrame(rows)
