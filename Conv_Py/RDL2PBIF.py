#!/usr/bin/env python3
"""
Program Name: RDL2PBIF.py
Purpose:      Process PBIF (Public Bank Invoice Financing) factoring loan data.
              - Loads PBIF client data filtered for entity='PBBH'
              - Merges with MECHRG (mechanism charge) fixed-width text file
              - Computes FIU balance, DISBURSE, REPAID, UNDRAWN
              - Derives MATDTE (next billing date) via NXTBLDT logic
              - Outputs deduplicated PBIF dataset (by CLIENTNO MATDTE)
              This module is called via %INC PGM(RDL2PBIF) from EIMBNM01.

Dependency notes
----------------
%INC PGM(PBBLNFMT) is present in the SAS source as a session-level include.
Scanning every PUT(x, fmt.) call in the SAS body reveals that no PBBLNFMT
format function (LNPROD, LNDENOM, LNRATE, etc.) is called anywhere in this
program's DATA steps.  All assignments are literal strings or arithmetic.
No import from PBBLNFMT is therefore required or added here.

  PBBLNFMT : session-level include – no format called in this program
"""

import os
import calendar
from datetime import date, timedelta
from typing import Optional

import duckdb
import polars as pl

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

PBIF_CLIEN_PREFIX = "input/pbif/clien"        # clien<YYYY><MM><DD>.parquet
MECHRG_TXT        = "input/mechrg/mechrg.txt"  # fixed-width text input (.txt)

# =============================================================================
# DATE HELPERS
# =============================================================================

def sas_date_to_pydate(val) -> Optional[date]:
    """Convert SAS date integer (days since 1960-01-01) to Python date."""
    if val is None or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, date):
        return val
    return None


def pydate_to_sasdate(d: date) -> int:
    """Convert Python date to SAS date integer (days since 1960-01-01)."""
    return (d - date(1960, 1, 1)).days


# =============================================================================
# %MACRO DCLVAR — day arrays
#
# SAS:
#   RETAIN D1-D12 31 D4 D6 D9 D11 30
#          RD1-RD12 MD1-MD12 31 RD2 MD2 28 ...
#   ARRAY LDAY D1-D12;
#
# Defaults: all months=31, then Apr/Jun/Sep/Nov overridden to 30, Feb=28.
# Leap-year check uses MOD(YY,4)=0 (SAS simple 4-year rule).
# =============================================================================

def make_lday(year: int) -> list:
    """
    Build LDAY array (index 1..12) matching SAS %MACRO DCLVAR defaults,
    using the SAS simple leap-year rule: MOD(YY,4)=0.
    """
    # SAS RETAIN default: all months 31
    lday = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    # Feb: SAS uses MOD(YY,4)=0, not the full Gregorian rule
    lday[2] = 29 if (year % 4 == 0) else 28
    # Apr, Jun, Sep, Nov already set to 30 in the defaults above
    return lday


# =============================================================================
# %MACRO NXTBLDT — compute next billing date
#
# SAS:
#   DD = DAY(MATDTE);
#   MM = MONTH(MATDTE) + FREQ;
#   YY = YEAR(MATDTE);
#   IF MM > 12 THEN DO; MM = MM - 12; YY + 1; END;
#   IF MM = 2 THEN IF MOD(YY,4) = 0 THEN D2=29; ELSE D2=28;
#   IF DD > LDAY(MM) THEN DD = LDAY(MM);
#   MATDTE = MDY(MM, DD, YY);
# =============================================================================

def nxtbldt(matdte: date, freq: int, lday: list) -> date:
    """
    Advance MATDTE by FREQ months, clamping DD to month-end when needed.
    Uses the SAS simple leap-year rule (year % 4 == 0) for Feb.
    """
    dd = matdte.day
    mm = matdte.month + freq
    yy = matdte.year
    if mm > 12:
        mm -= 12
        yy += 1
    # Update Feb cap for new YY (SAS: IF MM=2 THEN ...)
    lday_local = list(lday)
    if mm == 2:
        lday_local[2] = 29 if (yy % 4 == 0) else 28
    if dd > lday_local[mm]:
        dd = lday_local[mm]
    return date(yy, mm, dd)


# =============================================================================
# LOAD MECHRG — fixed-width text file
#
# SAS INPUT layout (1-based column pointers):
#   @001 CLIENTNO  $9.      → 0-based slice  [0:9]
#   @010 PDATE    YYMMDD8.  → 0-based slice  [9:17]
#   @020 UVAL1    12.2      → 0-based slice [19:31]
#   @034 UVAL2    12.2      → 0-based slice [33:45]
#   @048 UVAL3    12.2      → 0-based slice [47:59]
#
# Numeric informat 12.2:
#   If the field contains a decimal point, use it as-is.
#   If no decimal point is present, the implied scale is 2, so divide by 100.
#   (SAS behaviour: "w.d informat — d specifies implied decimal places when
#   no explicit decimal point is present in the data.")
#
# YYMMDD8. year inference uses SAS YEARCUTOFF=1950:
#   2-digit year 50-99 → 1950-1999
#   2-digit year 00-49 → 2000-2049
#
# PROC SUMMARY after load: SUM INTVAL BY CLIENTNO.
# =============================================================================

def _parse_informat_12_2(raw: str) -> float:
    """
    Parse a SAS 12.2 numeric informat field.
    If the string contains an explicit decimal point, convert directly.
    If not, apply implied 2 decimal places (divide by 100).
    Returns 0.0 for blank/unparseable fields.
    """
    s = raw.strip()
    if not s:
        return 0.0
    try:
        if '.' in s:
            return float(s)
        else:
            return float(s) / 100.0
    except ValueError:
        return 0.0


def _parse_yymmdd8(s: str) -> Optional[date]:
    """
    Parse an 8-character YYMMDD8. field using SAS YEARCUTOFF=1950.
      50-99 → 1950-1999
      00-49 → 2000-2049
    Returns None if the string is blank or malformed.
    """
    s = s.strip()
    if len(s) < 6:
        return None
    try:
        yy_ = int(s[0:2])
        mm_ = int(s[2:4])
        dd_ = int(s[4:6])
        # SAS YEARCUTOFF=1950: <50 → 2000s, >=50 → 1900s
        year_ = (2000 + yy_) if yy_ < 50 else (1900 + yy_)
        return date(year_, mm_, dd_)
    except (ValueError, IndexError):
        return None


def load_mechrg(mdate_int: int) -> pl.DataFrame:
    """
    Read MECHRG fixed-width text file, filter to PDATE == &MDATE, then
    PROC SUMMARY (SUM INTVAL BY CLIENTNO).

    &MDATE = PUT(REPTDATE, Z5.) — the SAS date integer of the report date.
    """
    empty = pl.DataFrame(schema={'clientno': pl.Utf8, 'intval': pl.Float64})

    if not os.path.exists(MECHRG_TXT):
        return empty

    rows = []
    with open(MECHRG_TXT, 'r', encoding='latin-1') as f:
        for line in f:
            if len(line) < 48:
                continue
            try:
                clientno  = line[0:9].strip()
                pdate_str = line[9:17]           # YYMMDD8. — 8 chars at @010
                uval1_str = line[19:31]          # 12.2 at @020
                uval2_str = line[33:45]          # 12.2 at @034
                uval3_str = line[47:59]          # 12.2 at @048

                pdate = _parse_yymmdd8(pdate_str)
                if pdate is None:
                    continue

                # IF PDATE = &MDATE
                if pydate_to_sasdate(pdate) != mdate_int:
                    continue

                uval1  = _parse_informat_12_2(uval1_str)
                uval2  = _parse_informat_12_2(uval2_str)
                uval3  = _parse_informat_12_2(uval3_str)
                intval = uval1 + uval2 + uval3

                rows.append({'clientno': clientno, 'intval': intval})

            except (ValueError, IndexError):
                continue

    if not rows:
        return empty

    df = pl.from_dicts(rows)
    # PROC SUMMARY DATA=MECHRG NWAY; CLASS CLIENTNO; VAR INTVAL; SUM=
    return df.group_by('clientno').agg(pl.col('intval').sum())


# =============================================================================
# CUSTFISS RECLASSIFICATION
#
# SAS:
#   CUSTFISS=CUSTCD;
#   IF CUSTFISS IN ('41','42','43','66')  THEN CUSTFISS='41';  ELSE
#   IF CUSTFISS IN ('44','47','67')       THEN CUSTFISS='44';  ELSE
#   IF CUSTFISS IN ('46')                 THEN CUSTFISS='46';  ELSE
#   IF CUSTFISS IN ('48','49','51','68')  THEN CUSTFISS='48';  ELSE
#   IF CUSTFISS IN ('52','53','54','69')  THEN CUSTFISS='52';
# =============================================================================

def reclassify_custfiss(custfiss: str) -> str:
    """Reclassify CUSTFISS per the SAS IF-ELSE chain."""
    if custfiss in ('41', '42', '43', '66'):
        return '41'
    if custfiss in ('44', '47', '67'):
        return '44'
    if custfiss == '46':
        return '46'
    if custfiss in ('48', '49', '51', '68'):
        return '48'
    if custfiss in ('52', '53', '54', '69'):
        return '52'
    return custfiss


# =============================================================================
# MAIN BUILD FUNCTION
# Called from EIMBNM01 with report date variables.
# =============================================================================

def build_pbif(
    reptdate:  date,
    reptyear:  str,
    reptmon:   str,
    reptday:   str,
    mdate_int: int,
) -> pl.DataFrame:
    """
    Full RDL2PBIF logic:
      1. Load PBIF.CLIEN<YYYY><MM><DD>, filter ENTITY='PBBH'
      2. Assign fixed fields, reclassify CUSTFISS
      3. Load & summarise MECHRG, merge into PBIF
      4. Compute FIU, BALANCE, DISBURSE, REPAID, UNDRAWN
      5. Compute MATDTE via %NXTBLDT loop
      6. PROC SORT NODUPKEY BY CLIENTNO MATDTE
    Returns the final PBIF DataFrame.
    """

    # -------------------------------------------------------------------------
    # Step 1 — DATA PBIF: SET PBIF.CLIEN<YY><MM><DD>; IF ENTITY='PBBH';
    # -------------------------------------------------------------------------
    clien_path = f"{PBIF_CLIEN_PREFIX}{reptyear}{reptmon}{reptday}.parquet"
    if not os.path.exists(clien_path):
        return pl.DataFrame()

    con  = duckdb.connect()
    pbif = con.execute(
        f"SELECT * FROM read_parquet('{clien_path}') WHERE entity = 'PBBH'"
    ).pl()
    con.close()

    if pbif.is_empty():
        return pl.DataFrame()

    # -------------------------------------------------------------------------
    # Assign fixed fields and reclassify CUSTFISS
    # SAS:
    #   APPRLIMX = INLIMIT;
    #   PRODCD   = '30591';
    #   FISSPURP = '0470';
    #   AMTIND   = 'D';
    #   CUSTFISS = CUSTCD;
    #   <reclassify CUSTFISS>
    #   CUSTCX   = CUSTFISS;
    # -------------------------------------------------------------------------
    rows = pbif.to_dicts()
    for row in rows:
        custcd  = str(row.get('custcd') or '').strip()
        inlimit = float(row.get('inlimit') or 0.0)

        row['apprlimx'] = inlimit
        row['prodcd']   = '30591'
        row['fisspurp'] = '0470'
        row['amtind']   = 'D'

        custfiss        = reclassify_custfiss(custcd)
        row['custfiss'] = custfiss
        row['custcx']   = custfiss

    # PROC SORT; BY CLIENTNO  (no NODUPKEY here — first sort only)
    pbif = pl.from_dicts(rows).sort('clientno')

    # -------------------------------------------------------------------------
    # Step 2 — Load MECHRG and merge
    # SAS: MERGE PBIF(IN=A) MECHRG; BY CLIENTNO; IF A;
    # -------------------------------------------------------------------------
    mechrg_df = load_mechrg(mdate_int)

    if not mechrg_df.is_empty():
        pbif = pbif.join(mechrg_df, on='clientno', how='left', suffix='_mc')
        # Resolve the merged intval column
        if 'intval_mc' in pbif.columns:
            pbif = pbif.with_columns(
                pl.when(pl.col('intval_mc').is_not_null())
                  .then(pl.col('intval_mc'))
                  .otherwise(
                      pl.col('intval') if 'intval' in pbif.columns
                      else pl.lit(None)
                  )
                  .alias('intval')
            ).drop('intval_mc')
    else:
        if 'intval' not in pbif.columns:
            pbif = pbif.with_columns(
                pl.lit(None).cast(pl.Float64).alias('intval')
            )

    # -------------------------------------------------------------------------
    # Step 3 — Compute FIU, BALANCE, DISBURSE, REPAID, UNDRAWN
    #
    # SAS (exact sequence):
    #   IF FIU=0.00 AND PRMTHFIU=0.00 THEN DELETE;     ← before intval added
    #   IF INTVAL=. THEN INTVAL=0.00;
    #   FIU=SUM(FIU,INTVAL,PRMTHFIU);
    #   BALANCE=FIU;
    #   UFIU=0; DISBURSE=0; REPAID=0; ROLLOVER=0;
    #   IF BALANCE  < 0.00 THEN BALANCE=0.00;
    #   IF FIU      < 0.00 THEN UFIU=FIU;
    #   IF PRMTHFIU < 0.00 THEN PRMTHFIU=0.00;
    #   IF BALANCE GE 0.00 THEN DO;
    #     IF BALANCE > PRMTHFIU THEN DISBURSE=BALANCE-PRMTHFIU;
    #                            ELSE REPAID  =PRMTHFIU-BALANCE;
    #   END;
    #   UNDRAWN=(INLIMIT-BALANCE);
    #   IF FIU=0.00 THEN DELETE;                        ← after intval added
    # -------------------------------------------------------------------------
    out_rows = []
    for row in pbif.to_dicts():
        fiu      = float(row.get('fiu')      or 0.0)
        prmthfiu = float(row.get('prmthfiu') or 0.0)

        # DELETE if both zero BEFORE adding intval
        if fiu == 0.0 and prmthfiu == 0.0:
            continue

        # INTVAL null → 0
        intval_raw = row.get('intval')
        intval = (
            0.0
            if intval_raw is None or (isinstance(intval_raw, float) and intval_raw != intval_raw)
            else float(intval_raw)
        )
        row['intval'] = intval

        # FIU = SUM(FIU, INTVAL, PRMTHFIU)
        fiu = fiu + intval + prmthfiu

        balance  = fiu
        ufiu     = 0.0
        disburse = 0.0
        repaid   = 0.0
        rollover = 0.0

        if balance  < 0.0: balance  = 0.0
        if fiu      < 0.0: ufiu     = fiu
        if prmthfiu < 0.0: prmthfiu = 0.0

        if balance >= 0.0:
            if balance > prmthfiu:
                disburse = balance - prmthfiu
            else:
                repaid   = prmthfiu - balance

        inlimit = float(row.get('inlimit') or 0.0)
        undrawn = inlimit - balance

        row['fiu']      = fiu
        row['balance']  = balance
        row['ufiu']     = ufiu
        row['disburse'] = disburse
        row['repaid']   = repaid
        row['rollover'] = rollover
        row['undrawn']  = undrawn
        row['prmthfiu'] = prmthfiu

        # DELETE if FIU still zero AFTER adding intval
        if fiu == 0.0:
            continue

        out_rows.append(row)

    if not out_rows:
        return pl.DataFrame()

    pbif = pl.from_dicts(out_rows)

    # -------------------------------------------------------------------------
    # Step 4 — Compute MATDTE (third DATA PBIF step)
    #
    # SAS:
    #   DROP CUSTCD;
    #   %DCLVAR
    #   FORMAT CUSTCX $2.;
    #   SET PBIF;
    #   IF _N_=1 THEN DO;
    #     SET REPTDATE;
    #     RPYR=YEAR(REPTDATE); RPMTH=MONTH(REPTDATE); RPDAY=DAY(REPTDATE);
    #     IF MOD(RPYR,4)=0 THEN RD2=29;
    #   END;
    #   FREQ=6;
    #   IF INLIMIT < 1000000.00 THEN FREQ=12;
    #   MATDTE=REPTDATE;
    #   IF STDATES > 0 THEN DO;
    #     MATDTE=STDATES;
    #     DO WHILE (MATDTE <= REPTDATE); %NXTBLDT END;
    #   END;
    # -------------------------------------------------------------------------
    lday = make_lday(reptdate.year)

    out_rows = []
    for row in pbif.to_dicts():
        inlimit = float(row.get('inlimit') or 0.0)
        freq    = 6 if inlimit >= 1000000.0 else 12

        row['freq'] = freq

        # MATDTE = REPTDATE (default)
        matdte = date(reptdate.year, reptdate.month, reptdate.day)

        # IF STDATES > 0
        stdates_raw = row.get('stdates')
        stdates     = sas_date_to_pydate(stdates_raw) if stdates_raw is not None else None

        if stdates is not None and stdates_raw > 0:
            matdte = stdates
            while matdte <= reptdate:
                matdte = nxtbldt(matdte, freq, lday)

        row['matdte'] = pydate_to_sasdate(matdte)

        # DROP CUSTCD
        row.pop('custcd', None)

        out_rows.append(row)

    if not out_rows:
        return pl.DataFrame()

    pbif = pl.from_dicts(out_rows)

    # -------------------------------------------------------------------------
    # PROC SORT DATA=PBIF OUT=PBIF NODUPKEY; BY CLIENTNO MATDTE;
    # -------------------------------------------------------------------------
    pbif = (
        pbif
        .sort(['clientno', 'matdte'])
        .unique(subset=['clientno', 'matdte'], keep='first')
    )

    return pbif
