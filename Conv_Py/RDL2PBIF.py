# !/usr/bin/env python3
"""
Program Name: RDL2PBIF
Purpose: Process PBIF (Public Bank Invoice Financing) factoring loan data.
         - Loads PBIF client data filtered for entity='PBBH'
         - Merges with MECHRG (mechanism charge) fixed-width text file
         - Computes FIU balance, DISBURSE, REPAID, UNDRAWN
         - Derives MATDTE (next billing date) via NXTBLDT logic
         - Outputs deduplicated PBIF dataset (by CLIENTNO MATDTE)
         This module is called via %INC PGM(RDL2PBIF) from EIMBNM01.

Dependencies: PBBLNFMT (format maps) — referenced inline where needed
"""

import os
import re
import calendar
from datetime import date, timedelta
from typing import Optional

import duckdb
import polars as pl

# %INC PGM(PBBLNFMT)
# from PBBLNFMT import FISSTYPE_MAP, FISSGROUP_MAP  # (placeholder)
from PBBLNFMT import format_lnprod, format_lndenom  # (placeholder)

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

PBIF_CLIEN_PREFIX   = "input/pbif/clien"     # clien<YY><MM><DD>.parquet
MECHRG_TXT          = "input/mechrg/mechrg.txt"   # fixed-width text input

# =============================================================================
# FISS FORMAT HELPERS
# =============================================================================

def format_fisstype(sectorcd) -> str:
    """$FISSTYPE. format — map FISS sector code to type string."""
    if sectorcd is None:
        return ''
    s = str(sectorcd).strip().zfill(4)
    prefix = s[:2]
    mapping = {
        '01': 'AGRICULTURE',  '02': 'MINING',       '03': 'MANUFACTURING',
        '04': 'ELECTRICITY',  '05': 'WATER',         '06': 'CONSTRUCTION',
        '07': 'WHOLESALE',    '08': 'TRANSPORT',     '09': 'FINANCE',
        '10': 'REAL ESTATE',  '11': 'EDUCATION',     '12': 'HEALTH',
        '13': 'ARTS',         '14': 'OTHER SVC',     '15': 'GOVERNMENT',
        '16': 'HOUSEHOLD',    '17': 'EXTRA-TERR',
    }
    return mapping.get(prefix, 'OTHERS')


def format_fissgroup(sectorcd) -> str:
    """$FISSGROUP. format — map FISS sector code to group string."""
    if sectorcd is None:
        return ''
    s = str(sectorcd).strip().zfill(4)
    prefix = s[:2]
    grp_map = {
        '01': 'A', '02': 'B', '03': 'C', '04': 'D', '05': 'D',
        '06': 'E', '07': 'F', '08': 'G', '09': 'H', '10': 'I',
        '11': 'J', '12': 'K', '13': 'L', '14': 'M', '15': 'N',
        '16': 'O', '17': 'P',
    }
    return grp_map.get(prefix, 'Z')

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
    """Convert Python date to SAS date integer."""
    return (d - date(1960, 1, 1)).days


# =============================================================================
# %MACRO DCLVAR — day arrays
# Days per month (1-indexed, ignoring index 0); Feb default 28
# RETAIN D1-D12 31 D4 D6 D9 D11 30 (override specific months)
# =============================================================================

def make_lday(year: int) -> list:
    """
    Build LDAY array (index 1..12) = days in each month for given year.
    %MACRO DCLVAR:
      RETAIN D1-D12 31 D4 D6 D9 D11 30
             RD1-RD12 MD1-MD12 31 RD2 MD2 28 RD4 RD6 RD9 RD11
             MD4 MD6 MD9 MD11 30 RPYR RPMTH RPDAY
    """
    lday = [0] * 13  # index 0 unused
    for m in range(1, 13):
        lday[m] = calendar.monthrange(year, m)[1]
    # Override with SAS RETAIN defaults (standard Gregorian — same result)
    lday[4]  = 30
    lday[6]  = 30
    lday[9]  = 30
    lday[11] = 30
    # Feb: leap year check
    lday[2]  = 29 if calendar.isleap(year) else 28
    return lday


# =============================================================================
# %MACRO NXTBLDT — compute next billing date
# =============================================================================

def nxtbldt(matdte: date, freq: int, lday: list) -> date:
    """
    %MACRO NXTBLDT:
      DD = DAY(MATDTE);
      MM = MONTH(MATDTE) + FREQ;
      YY = YEAR(MATDTE);
      IF MM > 12 THEN DO; MM = MM - 12; YY + 1; END;
      IF MM = 2 THEN IF MOD(YY,4)=0 THEN D2=29; ELSE D2=28;
      IF DD > LDAY(MM) THEN DD = LDAY(MM);
      MATDTE = MDY(MM, DD, YY);
    """
    dd = matdte.day
    mm = matdte.month + freq
    yy = matdte.year
    if mm > 12:
        mm -= 12
        yy += 1
    # Update Feb days for the new year
    lday_local = list(lday)
    if mm == 2:
        lday_local[2] = 29 if (yy % 4 == 0) else 28
    if dd > lday_local[mm]:
        dd = lday_local[mm]
    return date(yy, mm, dd)


# =============================================================================
# LOAD MECHRG — fixed-width text file
# @001 CLIENTNO $9.
# @010 PDATE    YYMMDD8.
# @020 UVAL1    12.2
# @034 UVAL2    12.2
# @048 UVAL3    12.2
# =============================================================================

def load_mechrg(mdate_int: int) -> pl.DataFrame:
    """
    DATA MECHRG:
      INFILE MECHRG;
      INPUT @001 CLIENTNO $9. @010 PDATE YYMMDD8.
            @020 UVAL1 12.2 @034 UVAL2 12.2 @048 UVAL3 12.2;
      INTVAL=SUM(UVAL1,UVAL2,UVAL3);
      IF PDATE=&MDATE;
    Then PROC SUMMARY sum INTVAL by CLIENTNO.
    &MDATE is PUT(REPTDATE,Z5.) — SAS date as 5-digit zero-padded integer.
    """
    if not os.path.exists(MECHRG_TXT):
        return pl.DataFrame(schema={'clientno': pl.Utf8, 'intval': pl.Float64})

    rows = []
    with open(MECHRG_TXT, 'r', encoding='latin-1') as f:
        for line in f:
            if len(line) < 48:
                continue
            try:
                clientno = line[0:9].strip()
                pdate_str = line[9:17].strip()   # YYMMDD8. = 8 chars at pos @010
                uval1_str = line[19:31].strip()  # 12.2 at @020
                uval2_str = line[33:45].strip()  # 12.2 at @034
                uval3_str = line[47:59].strip()  # 12.2 at @048

                # Parse YYMMDD8 -> date
                if len(pdate_str) == 8:
                    yy_ = int(pdate_str[0:2])
                    mm_ = int(pdate_str[2:4])
                    dd_ = int(pdate_str[4:6])
                    year_ = (2000 + yy_) if yy_ < 60 else (1900 + yy_)
                    pdate = date(year_, mm_, dd_)
                    pdate_sas = pydate_to_sasdate(pdate)
                else:
                    continue

                # IF PDATE = &MDATE  (MDATE = PUT(REPTDATE, Z5.))
                if pdate_sas != mdate_int:
                    continue

                uval1 = float(uval1_str) if uval1_str else 0.0
                uval2 = float(uval2_str) if uval2_str else 0.0
                uval3 = float(uval3_str) if uval3_str else 0.0
                intval = uval1 + uval2 + uval3
                rows.append({'clientno': clientno, 'intval': intval})
            except (ValueError, IndexError):
                continue

    if not rows:
        return pl.DataFrame(schema={'clientno': pl.Utf8, 'intval': pl.Float64})

    df = pl.from_dicts(rows)
    # PROC SUMMARY: SUM intval by clientno
    return df.group_by('clientno').agg(pl.col('intval').sum())


# =============================================================================
# CUSTFISS RECLASSIFICATION
# =============================================================================

def reclassify_custfiss(custfiss: str) -> str:
    """
    IF CUSTFISS IN ('41','42','43','66')  THEN CUSTFISS='41';  ELSE
    IF CUSTFISS IN ('44','47','67')       THEN CUSTFISS='44';  ELSE
    IF CUSTFISS IN ('46')                 THEN CUSTFISS='46';  ELSE
    IF CUSTFISS IN ('48','49','51','68')  THEN CUSTFISS='48';  ELSE
    IF CUSTFISS IN ('52','53','54','69')  THEN CUSTFISS='52';
    """
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
# MAIN PBIF BUILD FUNCTION
# Called from EIMBNM01 with report date variables
# =============================================================================

def build_pbif(reptdate: date, reptyear: str, reptmon: str, reptday: str,
               mdate_int: int) -> pl.DataFrame:
    """
    Full RDL2PBIF logic:
      1. Load PBIF.CLIEN<YY><MM><DD> filter ENTITY='PBBH'
      2. Load & summarise MECHRG, merge into PBIF
      3. Compute FIU, BALANCE, DISBURSE, REPAID, UNDRAWN
      4. Compute MATDTE via NXTBLDT loop
      5. PROC SORT NODUPKEY BY CLIENTNO MATDTE
    Returns PBIF DataFrame.
    """
    # -------------------------------------------------------------------------
    # DATA PBIF: SET PBIF.CLIEN<YY><MM><DD>; IF ENTITY='PBBH';
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

    # Derive initial fields
    rows = pbif.to_dicts()
    for row in rows:
        custcd   = str(row.get('custcd') or '').strip()
        inlimit  = float(row.get('inlimit') or 0.0)

        row['apprlimx'] = inlimit
        row['prodcd']   = '30591'
        row['fisspurp'] = '0470'
        row['amtind']   = 'D'

        custfiss = reclassify_custfiss(custcd)
        row['custfiss'] = custfiss
        row['custcx']   = custfiss

    pbif = pl.from_dicts(rows).sort('clientno')

    # -------------------------------------------------------------------------
    # Load MECHRG and merge
    # -------------------------------------------------------------------------
    mechrg_df = load_mechrg(mdate_int)

    if not mechrg_df.is_empty():
        pbif = pbif.join(mechrg_df, on='clientno', how='left', suffix='_mc')
        if 'intval_mc' in pbif.columns:
            pbif = pbif.with_columns(
                pl.when(pl.col('intval_mc').is_not_null())
                  .then(pl.col('intval_mc'))
                  .otherwise(pl.col('intval') if 'intval' in pbif.columns else pl.lit(None))
                  .alias('intval')
            ).drop('intval_mc')
    else:
        if 'intval' not in pbif.columns:
            pbif = pbif.with_columns(pl.lit(None).cast(pl.Float64).alias('intval'))

    # DATA PBIF (second step): compute FIU, BALANCE, etc.
    # IF FIU=0.00 AND PRMTHFIU=0.00 THEN DELETE;
    # IF INTVAL=. THEN INTVAL=0.00;
    # FIU=SUM(FIU,INTVAL,PRMTHFIU);
    rows = pbif.to_dicts()
    out_rows = []
    for row in rows:
        fiu      = float(row.get('fiu')      or 0.0)
        prmthfiu = float(row.get('prmthfiu') or 0.0)

        # IF FIU=0.00 AND PRMTHFIU=0.00 THEN DELETE
        if fiu == 0.0 and prmthfiu == 0.0:
            continue

        intval = row.get('intval')
        if intval is None or (isinstance(intval, float) and intval != intval):
            intval = 0.0
        else:
            intval = float(intval)

        fiu = fiu + intval + prmthfiu
        row['intval'] = intval

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

        # IF FIU=0.00 THEN DELETE
        if fiu == 0.0:
            continue

        out_rows.append(row)

    if not out_rows:
        return pl.DataFrame()

    pbif = pl.from_dicts(out_rows)

    # -------------------------------------------------------------------------
    # DATA PBIF (third step — MATDTE computation):
    # DROP CUSTCD
    # %DCLVAR
    # FREQ=6; IF INLIMIT<1000000 THEN FREQ=12;
    # Compute MATDTE via NXTBLDT loop
    # -------------------------------------------------------------------------
    lday = make_lday(reptdate.year)
    # Update Feb for leap year
    if reptdate.year % 4 == 0:
        lday[2] = 29

    rows = pbif.to_dicts()
    out_rows = []
    for row in rows:
        inlimit = float(row.get('inlimit') or 0.0)
        freq    = 6 if inlimit >= 1000000.0 else 12

        row['freq'] = freq

        # MATDTE = REPTDATE initially
        matdte = date(reptdate.year, reptdate.month, reptdate.day)

        # IF STDATES > 0 THEN DO; MATDTE=STDATES; ...
        stdates_raw = row.get('stdates')
        stdates = sas_date_to_pydate(stdates_raw) if stdates_raw is not None else None

        if stdates is not None and (isinstance(stdates_raw, (int, float)) and
                                    stdates_raw > 0):
            matdte = stdates
            while matdte <= reptdate:
                matdte = nxtbldt(matdte, freq, lday)

        row['matdte']  = pydate_to_sasdate(matdte)
        # DROP CUSTCD
        row.pop('custcd', None)
        out_rows.append(row)

    if not out_rows:
        return pl.DataFrame()

    pbif = pl.from_dicts(out_rows)

    # PROC SORT DATA=PBIF OUT=PBIF NODUPKEY; BY CLIENTNO MATDTE;
    pbif = pbif.sort(['clientno', 'matdte']).unique(
        subset=['clientno', 'matdte'], keep='first'
    )

    return pbif
