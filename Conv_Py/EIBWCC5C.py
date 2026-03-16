#!/usr/bin/env python3
"""
Program  : EIBWCC5C.py
Purpose  : PBB (Public Bank Berhad) CCRIS split data preparation.
           Reads loan and current account (OD) data, merges ELDS, collateral,
           NPL, written-off, and related reference data, then writes:
             CCRISP.LOAN     - Loan records split dataset (parquet)
             CCRISP.OVERDFS  - Overdraft/OD records split dataset (parquet)
             CCRISP.ELDS     - ELDS records without matched loan (parquet)
             CCRISP.SUMM1    - LU address summary (parquet)
"""

# ---------------------------------------------------------------------------
# JCL DD references:
# BNM     DSN=SAP.PBB.MNILN(0)         -> BNM_DIR/LNNOTE.parquet, LNCOMM.parquet
# BNMO    DSN=SAP.PBB.MNILN(-4)        -> BNMO_DIR/LNNOTE.parquet
# LIMIT   DSN=SAP.PBB.MNILIMT(0)       -> LIMIT_DIR/OVERDFT.parquet, LIMITS.parquet, GP3.parquet
# SASD    DSN=SAP.PBB.SASDATA          -> SASD_DIR/LOANmmWk.parquet
# GP3     DSN=RBP2.B033.GP3.KLUNION    -> fixed-width text file GP3_FILE
# NPL     DSN=SAP.PBB.NPL.CCRIS        -> NPL_DIR/TOTIISmmm.parquet
# ELDS    DSN=SAP.ELDS.ELAA(0)         -> ELDS_DIR/ELBNMAX.parquet
# ELDS2   DSN=SAP.ELDS.ELHP(0)         -> ELDS2_DIR/ELHPAX.parquet
# ELNRJ   DSN=SAP.ELDS.AAREJ.BACKDATE  -> ELNRJ_DIR/ELNMAX_REJ.parquet
# EHPRJ   DSN=SAP.ELDS.HPREJ.BACKDATE  -> EHPRJ_DIR/EHPMAX_REJ.parquet
# BNMSUM  DSN=SAP.PBB.ELDS.ISS3(0)     -> BNMSUM_DIR/SUMM1yyyymmdd.parquet
# DEPOSIT DSN=SAP.PBB.MNITB(0)         -> DEPOSIT_DIR/CURRENT.parquet
# COLL    DSN=SAP.PBB.MNICOL(0)        -> COLL_DIR/COLLATER.parquet
# CCRISX  DSN=SAP.BNM.PROGRAM(CCRISX)  -> fixed-width text file CCRISX_FILE
# CCRISP  DSN=SAP.PBB.CCRIS.SPLIT      -> CCRISP_DIR (output + CCRISP.SUMM1/ODFEE)
# EREV    DSN=SAP.ELDS.EREV.NEW(0)     -> EREV_DIR (EMUL1617, EMUL12, EREVPRE parquets)
# ODWOF   DSN=SAP.PBB.ODWOF(0)         -> ODWOF_DIR/ODWOF.parquet
# WOFF    DSN=SAP.PBB.NPL.HP.SASDATA   -> WOFF_DIR/WOFFTOT.parquet
# ---------------------------------------------------------------------------

import os
import math
import calendar
from pathlib import Path
from datetime import date, timedelta
from typing import Optional

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# DEPENDENCIES
# ---------------------------------------------------------------------------
# %INC PGM(PBBDPFMT) - format_caprod used for PRODCD derivation
from PBBDPFMT import format_caprod

# %INC PGM(PBBLNFMT) - format_lnprod used for loan product code derivation
from PBBLNFMT import format_lnprod

# %INC PGM(PFBCRFMT) - BNM taxonomy formats
from PFBCRFMT import (
    format_faccode,
    format_odfaccode,
    format_purphp,
)

# ---------------------------------------------------------------------------
# PATH SETUP
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).parent

# Input libraries
BNM_DIR    = BASE_DIR / "data" / "BNM"          # SAP.PBB.MNILN(0)
BNMO_DIR   = BASE_DIR / "data" / "BNMO"         # SAP.PBB.MNILN(-4)
LIMIT_DIR  = BASE_DIR / "data" / "LIMIT"         # SAP.PBB.MNILIMT(0)
SASD_DIR   = BASE_DIR / "data" / "SASD"          # SAP.PBB.SASDATA
NPL_DIR    = BASE_DIR / "data" / "NPL"           # SAP.PBB.NPL.CCRIS
ELDS_DIR   = BASE_DIR / "data" / "ELDS"          # SAP.ELDS.ELAA(0)
ELDS2_DIR  = BASE_DIR / "data" / "ELDS2"         # SAP.ELDS.ELHP(0)
ELNRJ_DIR  = BASE_DIR / "data" / "ELNRJ"         # SAP.ELDS.AAREJ.BACKDATE
EHPRJ_DIR  = BASE_DIR / "data" / "EHPRJ"         # SAP.ELDS.HPREJ.BACKDATE
BNMSUM_DIR = BASE_DIR / "data" / "BNMSUM"        # SAP.PBB.ELDS.ISS3(0)
DEPOSIT_DIR= BASE_DIR / "data" / "DEPOSIT"       # SAP.PBB.MNITB(0)
COLL_DIR   = BASE_DIR / "data" / "COLL"          # SAP.PBB.MNICOL(0)
EREV_DIR   = BASE_DIR / "data" / "EREV"          # SAP.ELDS.EREV.NEW(0)
ODWOF_DIR  = BASE_DIR / "data" / "ODWOF"         # SAP.PBB.ODWOF(0)
WOFF_DIR   = BASE_DIR / "data" / "WOFF"          # SAP.PBB.NPL.HP.SASDATA

# Fixed-width text input files
GP3_FILE   = BASE_DIR / "data" / "txt" / "GP3.txt"       # RBP2.B033.GP3.KLUNION
CCRISX_FILE= BASE_DIR / "data" / "txt" / "CCRISX.txt"    # SAP.BNM.PROGRAM(CCRISX)

# Output / split library
CCRISP_DIR = BASE_DIR / "data" / "CCRISP"        # SAP.PBB.CCRIS.SPLIT
CCRISP_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# MACRO VARIABLE EQUIVALENTS  (populated from BNM.REPTDATE)
# ---------------------------------------------------------------------------
con = duckdb.connect()

_dates_df = con.execute(
    f"SELECT * FROM read_parquet('{BNM_DIR}/REPTDATE.parquet') LIMIT 1"
).pl()

_reptdate_val = int(_dates_df["REPTDATE"][0])
REPTDATE = date(1960, 1, 1) + timedelta(days=_reptdate_val)

_day = REPTDATE.day
if _day == 8:
    _sdd, _wk, _wk1 = 1,  '1', '4'
elif _day == 15:
    _sdd, _wk, _wk1 = 9,  '2', '1'
elif _day == 22:
    _sdd, _wk, _wk1 = 16, '3', '2'
else:
    _sdd, _wk, _wk1 = 23, '4', '3'

_mm = REPTDATE.month
if _wk == '1':
    _mm1 = _mm - 1 if _mm > 1 else 12
else:
    _mm1 = _mm
_mm2 = _mm if _wk == '4' else (_mm - 1 if _mm > 1 else 12)

NOWK      = _wk
NOWK1     = _wk1
REPTMON   = f"{_mm:02d}"
REPTMON1  = f"{_mm1:02d}"
REPTMON2  = f"{_mm2:02d}"
REPTYEAR  = str(REPTDATE.year)
REPTDAY   = f"{_day:02d}"
RDATE     = REPTDATE.strftime("%d/%m/%Y")
RDATE2    = (date.today() - timedelta(days=1)).strftime("%y%m%d")  # &RDATE2 = YYMMDD6. of today-1
SDATE_NUM = _reptdate_val
SDATE     = f"{_reptdate_val:05d}"
MDATE     = f"{_reptdate_val:05d}"

print(f"EIBWCC5C started. REPTDATE={REPTDATE} NOWK={NOWK} REPTMON={REPTMON} REPTYEAR={REPTYEAR}")

# ---------------------------------------------------------------------------
# HP macro list (used throughout)
# ---------------------------------------------------------------------------
HP = {128, 130, 380, 381, 700, 705, 983, 993, 996}

# ---------------------------------------------------------------------------
# HELPER UTILITIES
# ---------------------------------------------------------------------------

def _sas_date(numeric_val) -> Optional[date]:
    """Convert SAS numeric date (days since 1960-01-01) to Python date."""
    if numeric_val is None or (isinstance(numeric_val, float) and math.isnan(numeric_val)):
        return None
    v = int(numeric_val)
    if v <= 0:
        return None
    try:
        return date(1960, 1, 1) + timedelta(days=v)
    except Exception:
        return None


def _sas_date_num(d: Optional[date]) -> int:
    """Convert Python date to SAS numeric date."""
    if d is None:
        return 0
    return (d - date(1960, 1, 1)).days


def _mmddyy_from_z11(val) -> Optional[date]:
    """
    SAS pattern: INPUT(SUBSTR(PUT(val, Z11.), 1, 8), MMDDYY8.)
    Z11. pads val with leading zeros to 11 digits; first 8 chars = MMDDYYYY.
    """
    if val is None or (isinstance(val, float) and math.isnan(val)) or int(val) <= 0:
        return None
    s = f"{int(val):011d}"
    try:
        mm = int(s[0:2]); dd = int(s[2:4]); yy = int(s[4:8])
        return date(yy, mm, dd)
    except Exception:
        return None


def _z11_ymd(val) -> tuple:
    """
    Return (YYYY_str, MM_str, DD_str) from PUT(val,Z11.) layout (MMDDYYYY in first 8 chars).
    SAS: PROCYEAR = SUBSTR(PUT(x,Z11.),5,4)  -> YYYY
         PROCMONT = SUBSTR(PUT(x,Z11.),1,2)  -> MM
    """
    if val is None or (isinstance(val, float) and math.isnan(val)) or int(val) <= 0:
        return ('0000', '00', '00')
    s = f"{int(val):011d}"
    return (s[4:8], s[0:2], s[2:4])  # (YYYY, MM, DD)


def _nv(val, default=0):
    """Return numeric val or default if None/NaN."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    return val


def _sv(val, default=''):
    """Return string val or default if None."""
    if val is None:
        return default
    return str(val)


# ---------------------------------------------------------------------------
# STEP 1: DATA DATES - already resolved above
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# STEP 2: DEPOSIT.CURRENT -> CURRENT (current accounts / CA)
# ---------------------------------------------------------------------------
current_raw = con.execute(
    f"SELECT * FROM read_parquet('{DEPOSIT_DIR}/CURRENT.parquet')"
).pl()

def _build_current(rows):
    out = []
    for r in rows:
        r = dict(r)
        product  = _nv(r.get('PRODUCT'))
        prodcd   = _sv(format_caprod(int(product)))
        r['PRODCD'] = prodcd
        lstrevdt = _nv(r.get('LSTREVDT'))
        if lstrevdt not in (None, 0) and not (isinstance(lstrevdt, float) and math.isnan(lstrevdt)):
            d = _mmddyy_from_z11(lstrevdt)
            r['LAST_LIMIT_REV_DATE'] = _sas_date_num(d) if d else 0
        else:
            r['LAST_LIMIT_REV_DATE'] = 0
        out.append(r)
    return out

current = pl.DataFrame(_build_current(current_raw.to_dicts()))

# RENAME SECTOR->SECTOR1, APPRLIMT->OPERLIMT; filter PRODCD NE 'N' and PRODUCT NE 105; NODUPKEY BY ACCTNO
current = (current
           .rename({'SECTOR': 'SECTOR1', 'APPRLIMT': 'OPERLIMT'})
           .filter((pl.col('PRODCD') != 'N') & (pl.col('PRODUCT') != 105))
           .sort('ACCTNO')
           .unique(subset=['ACCTNO'], keep='first'))

# ---------------------------------------------------------------------------
# STEP 3: NPL.TOTIISmmm2 (REPTMON2) dataset
# ---------------------------------------------------------------------------
npl_drop = {'BRANCH', 'MODELDES', 'RESTRUCT', 'MATUREDT', 'DLIVRYDT',
            'PAIDIND', 'LASTTRAN', 'LSTTRNCD', 'SPOTRATE'}
npl_raw = con.execute(
    f"SELECT * FROM read_parquet('{NPL_DIR}/TOTIIS{REPTMON2}.parquet')"
).pl()

# Drop specified columns if they exist
drop_cols = [c for c in npl_drop if c in npl_raw.columns]
npl_raw = npl_raw.drop(drop_cols)

# Filter: 99 < LOANTYPE < 4 is always false in SAS (likely a typo/dead code);
# keep rows where LOANTYPE NOT IN exclusion set
npl_excl = {380, 381, 390, 500, 516, 520, 199, 517, 521, 522, 523, 528}
npl = (npl_raw
       .filter(~pl.col('LOANTYPE').is_in(list(npl_excl)))
       .sort(['ACCTNO', 'NOTENO'])
       .unique(subset=['ACCTNO', 'NOTENO'], keep='first'))

# ---------------------------------------------------------------------------
# STEP 4: BNM.LNNOTE -> LOAN (with PENDBRH -> BRANCH rename)
# ---------------------------------------------------------------------------
loan_raw = con.execute(
    f"SELECT * FROM read_parquet('{BNM_DIR}/LNNOTE.parquet')"
).pl()

if 'PENDBRH' in loan_raw.columns:
    loan_raw = loan_raw.rename({'PENDBRH': 'BRANCH'})

# WHERE filter
loan_raw = loan_raw.filter(
    (pl.col('ACCTNO') > 8000000000) |
    (
        (pl.col('ACCTNO') < 2999999999) &
        (~pl.col('LOANTYPE').is_in([500, 520, 199])) &
        ~((pl.col('LOANTYPE') == 606) & (pl.col('CENSUS').is_in([500.0, 520.0])))
    )
)

loan_raw = loan_raw.sort(['ACCTNO', 'NOTENO'])

# ---------------------------------------------------------------------------
# STEP 5: OLDNOTE - extract BONUSANO from NOTENO via LASTTRAN MIGRATDT
# ---------------------------------------------------------------------------
def _build_oldnote(rows):
    out = []
    for r in rows:
        lasttran = _nv(r.get('LASTTRAN'))
        if lasttran > 0:
            yyyy, mm, dd = _z11_ymd(lasttran)
            migratdt = yyyy + mm + dd
        else:
            migratdt = ''
        out.append({
            'ACCTNO':   _nv(r.get('ACCTNO')),
            'BONUSANO': _nv(r.get('NOTENO')),
            'MIGRATDT': migratdt,
        })
    return out

oldnote = pl.DataFrame(_build_oldnote(loan_raw.to_dicts())).sort(['ACCTNO', 'BONUSANO'])

# Sort LOAN by ACCTNO BONUSANO (= NOTENO used as join key)
loan = loan_raw.rename({'NOTENO': '_NOTENO_ORIG'})
loan = loan.with_columns(pl.col('_NOTENO_ORIG').alias('BONUSANO'))
loan = loan.sort(['ACCTNO', 'BONUSANO'])

loan = loan.join(oldnote, on=['ACCTNO', 'BONUSANO'], how='left', suffix='_OLD')
# If NOTENO was already in loan, keep original; restore NOTENO
loan = loan.rename({'_NOTENO_ORIG': 'NOTENO'})

loan = loan.sort(['ACCTNO', 'NOTENO'])

# ---------------------------------------------------------------------------
# STEP 6: BNMO.LNNOTE (-4) -> LOANO (OLDBRH)
# ---------------------------------------------------------------------------
loano = con.execute(
    f"SELECT ACCTNO, NOTENO, PENDBRH AS OLDBRH "
    f"FROM read_parquet('{BNMO_DIR}/LNNOTE.parquet')"
).pl().sort(['ACCTNO', 'NOTENO'])

loan = loan.join(loano.select(['ACCTNO', 'NOTENO', 'OLDBRH']),
                 on=['ACCTNO', 'NOTENO'], how='left', suffix='_LOANO')

# SCORE = SCORE2; if SCORE='  ' use SCORE1
# APPLDT, CHKDT logic, exclusion deletes
aug04 = _sas_date_num(date(2004, 8, 31))   # '31AUG04'D

def _apply_loan_filters(rows):
    out = []
    for r in rows:
        r = dict(r)
        # OLDBRH default
        if _nv(r.get('OLDBRH')) in (None,) or (isinstance(r.get('OLDBRH'), float) and math.isnan(r.get('OLDBRH'))):
            r['OLDBRH'] = 0

        # SCORE
        score2 = _sv(r.get('SCORE2')).strip()
        score1 = _sv(r.get('SCORE1')).strip()
        r['SCORE'] = score2 if score2 else score1

        # APPLDT
        appldate = _nv(r.get('APPLDATE'))
        if appldate in (None, 0) or (isinstance(appldate, float) and math.isnan(appldate)):
            r['APPLDT'] = 0
        else:
            d = _mmddyy_from_z11(appldate)
            r['APPLDT'] = _sas_date_num(d) if d else 0

        # CHKDT delete logic
        loantype = _nv(r.get('LOANTYPE'))
        lsttrncd = _nv(r.get('LSTTRNCD'))
        appldt   = _nv(r.get('APPLDT'))
        chkdt    = aug04

        if (loantype in (981,982,983,991,992,993) or
            (loantype in (128,130,380,381,700,705) and
             lsttrncd in (658,662) and appldt > 0)):
            if chkdt > appldt:
                continue  # DELETE

        # Exclude 500,520,199 and 606/CENSUS=500/520
        census = _nv(r.get('CENSUS'))
        if loantype in (500, 520, 199):
            continue
        if loantype == 606 and census in (500.0, 520.0):
            continue

        out.append(r)
    return out

loan = pl.DataFrame(_apply_loan_filters(loan.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 7: Merge LOAN with NPL
# ---------------------------------------------------------------------------
loan = loan.sort(['ACCTNO', 'NOTENO'])
loan = loan.join(npl, on=['ACCTNO', 'NOTENO'], how='outer_coalesce', suffix='_NPL')

# If B (NPL) then set GP3IND
def _apply_npl_merge(rows):
    out = []
    for r in rows:
        r = dict(r)
        in_b = r.pop('_IN_NPL', False)
        # GP3IND from NPL
        if in_b:
            nplind = _sv(r.get('NPLIND'))
            r['GP3IND'] = 'R' if nplind == 'N' else 'A'

        # Keep if: (REVERSED NE 'Y' AND NOTENO NE . AND BORSTAT NOT IN ('Z')) OR B
        reversed_v = _sv(r.get('REVERSED'))
        noteno_v   = r.get('NOTENO')
        borstat_v  = _sv(r.get('BORSTAT'))
        noteno_ok  = noteno_v is not None and not (isinstance(noteno_v, float) and math.isnan(noteno_v))

        if (reversed_v != 'Y' and noteno_ok and borstat_v != 'Z') or in_b:
            out.append(r)
    return out

loan = pl.DataFrame(_apply_npl_merge(loan.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 8: LIMIT.OVERDFT -> OVERDFT (PRODUCT renamed LOANTYPE)
# ---------------------------------------------------------------------------
overdft = con.execute(
    f"SELECT * FROM read_parquet('{LIMIT_DIR}/OVERDFT.parquet') "
    f"ORDER BY ACCTNO, LMTSTDTE DESC"
).pl()

if 'PRODUCT' in overdft.columns and 'LOANTYPE' not in overdft.columns:
    overdft = overdft.rename({'PRODUCT': 'LOANTYPE'})

# WHERE filter: exclude specific OPENIND/PRODUCT combos
overdft = overdft.filter(
    ~((pl.col('OPENIND').is_in(['B','C','P'])) &
      (pl.col('LOANTYPE').is_in([105, 107, 160])))
)
overdft = overdft.sort('ACCTNO').unique(subset=['ACCTNO'], keep='first')

# ---------------------------------------------------------------------------
# STEP 9: LIMIT.GP3 -> LMGP3 (EXCESSDT, TODDATE)
# ---------------------------------------------------------------------------
lmgp3 = con.execute(
    f"SELECT ACCTNO, EXCESSDT, TODDATE "
    f"FROM read_parquet('{LIMIT_DIR}/GP3.parquet')"
).pl().sort('ACCTNO')

# ---------------------------------------------------------------------------
# STEP 10: LIMIT.LIMITS -> LIMITS
# ---------------------------------------------------------------------------
limits = con.execute(
    f"SELECT * FROM read_parquet('{LIMIT_DIR}/LIMITS.parquet')"
).pl().sort('ACCTNO')

# ---------------------------------------------------------------------------
# STEP 11: CURRENT - filter by OPENIND and period, merge LIMITS
# ---------------------------------------------------------------------------
def _filter_current(rows):
    """Filter CURRENT by OPENIND and date logic."""
    out = []
    for r in rows:
        r = dict(r)
        openind  = _sv(r.get('OPENIND'))
        closedt  = _nv(r.get('CLOSEDT'))
        opendt   = _nv(r.get('OPENDT'))

        procyear = ''; procmont = ''; openyear = ''; openmont = ''
        if closedt not in (None, 0) and not (isinstance(closedt, float) and math.isnan(closedt)):
            yy, mm, dd = _z11_ymd(closedt)
            procyear = yy; procmont = mm
            yy2, mm2, dd2 = _z11_ymd(opendt)
            openyear = yy2; openmont = mm2

        r['PROCYEAR'] = procyear; r['PROCMONT'] = procmont
        r['OPENYEAR'] = openyear; r['OPENMONT'] = openmont

        if openind not in ('B','C','P'):
            out.append(r)
        elif openind in ('B','C','P') and procyear == REPTYEAR and procmont == REPTMON:
            out.append(r)
    return out

current_rows = _filter_current(current.to_dicts())

def _filter_current2(rows):
    """Second pass: delete same-month open+close."""
    out = []
    for r in rows:
        openind  = _sv(r.get('OPENIND'))
        procyear = _sv(r.get('PROCYEAR'))
        procmont = _sv(r.get('PROCMONT'))
        openyear = _sv(r.get('OPENYEAR'))
        openmont = _sv(r.get('OPENMONT'))
        closedt  = _nv(r.get('CLOSEDT'))
        if openind in ('B','C','P') and closedt not in (None, 0):
            if procyear == openyear and procmont == openmont:
                continue
        out.append(r)
    return out

current_rows = _filter_current2(current_rows)
current = pl.DataFrame(current_rows)

# Merge LIMITS into CURRENT
current = current.join(limits, on='ACCTNO', how='left', suffix='_LMT')
current = current.with_columns([
    pl.when(pl.col('LMTAMT').is_null()).then(0).otherwise(pl.col('LMTAMT')).alias('LMTAMT'),
])
# SCORE = FAACRR; if '  ' use CRRCODE
def _set_score(rows):
    out = []
    for r in rows:
        r = dict(r)
        faacrr  = _sv(r.get('FAACRR')).strip()
        crrcode = _sv(r.get('CRRCODE')).strip()
        r['SCORE'] = faacrr if faacrr else crrcode
        out.append(r)
    return out

current = pl.DataFrame(_set_score(current.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 12: SASD.LOANmmWk (SASBAL) - balance/limit data
# ---------------------------------------------------------------------------
sasbal = con.execute(
    f"SELECT ACCTNO, NOTENO, BALANCE, APPRLIMT, BILTOT, EXPRDATE, BLDATE, "
    f"       UNDRAWN, APPRLIM2, EIR_ADJ, CFINDEX, FORATE, CUSTFISS, SECTFISS, "
    f"       ORIBALANCE "
    f"FROM read_parquet('{SASD_DIR}/LOAN{REPTMON}{NOWK}.parquet') "
    f"WHERE PRODUCT NOT IN (983, 993, 996)"
).pl()

# Add CUSTFISS/SECTFISS from CUSTCD/SECTORCD for non-983/993/996
# (the WHERE above handles it; for rows not in exclusion, CUSTFISS=CUSTCD, SECTFISS=SECTORCD)
# Those columns are already selected from the correct source in the parquet.

sasbal = sasbal.sort(['ACCTNO', 'NOTENO'])
loan   = loan.sort(['ACCTNO', 'NOTENO'])

loan = loan.join(sasbal, on=['ACCTNO', 'NOTENO'], how='left', suffix='_SAS')

# Resolve EXPRDATE: if missing, use NOTEMAT
def _fix_exprdate(rows):
    out = []
    for r in rows:
        r = dict(r)
        exprdate = _nv(r.get('EXPRDATE'))
        if exprdate in (None, 0) or (isinstance(exprdate, float) and math.isnan(exprdate)):
            notemat = _nv(r.get('NOTEMAT'))
            if notemat > 0:
                d = _mmddyy_from_z11(notemat)
                r['EXPRDATE'] = _sas_date_num(d) if d else 0
            else:
                r['EXPRDATE'] = 0

        # PROCYEAR / PROCMONT / ISSUYEAR / ISSUMONT from LASTTRAN / ISSUEDT
        lasttran = _nv(r.get('LASTTRAN'))
        issuedt  = _nv(r.get('ISSUEDT'))
        if lasttran > 0:
            r['PROCYEAR'], r['PROCMONT'], _ = _z11_ymd(lasttran)
        else:
            r['PROCYEAR'] = ''; r['PROCMONT'] = ''
        if issuedt > 0:
            r['ISSUYEAR'], r['ISSUMONT'], _ = _z11_ymd(issuedt)
        else:
            r['ISSUYEAR'] = ''; r['ISSUMONT'] = ''

        out.append(r)
    return out

loan = pl.DataFrame(_fix_exprdate(loan.to_dicts()))

# Filter loan records by paid/NPL/EIR logic
def _filter_loan_paid(rows):
    out = []
    for r in rows:
        r = dict(r)
        paidind  = _sv(r.get('PAIDIND'))
        procyear = _sv(r.get('PROCYEAR'))
        procmont = _sv(r.get('PROCMONT'))
        nplind   = _sv(r.get('NPLIND'))
        loantype = _nv(r.get('LOANTYPE'))
        lsttrncd = _nv(r.get('LSTTRNCD'))
        eir_adj  = r.get('EIR_ADJ')
        eir_ok   = eir_adj is not None and not (isinstance(eir_adj, float) and math.isnan(eir_adj))

        keep = False
        if (paidind == 'P' and procyear == REPTYEAR and procmont == REPTMON):
            keep = True
        elif nplind == 'P':
            keep = True
        elif (600 <= loantype <= 699 and paidind == 'P' and lsttrncd == 658):
            keep = True
        elif (loantype not in range(600, 700) and paidind == 'P' and
              lsttrncd in (662, 663, 658)):
            keep = True
        elif paidind != 'P' or eir_ok:
            keep = True

        if keep:
            out.append(r)
    return out

loan = pl.DataFrame(_filter_loan_paid(loan.to_dicts()))

# Second pass: delete same-month issue vs last-tran paid records (non-HP)
def _filter_loan_paid2(rows):
    out = []
    for r in rows:
        r = dict(r)
        eir_adj  = r.get('EIR_ADJ')
        eir_ok   = eir_adj is not None and not (isinstance(eir_adj, float) and math.isnan(eir_adj))
        if eir_ok:
            out.append(r); continue

        paidind  = _sv(r.get('PAIDIND'))
        lasttran = _nv(r.get('LASTTRAN'))
        loantype = _nv(r.get('LOANTYPE'))
        procyear = _sv(r.get('PROCYEAR'))
        procmont = _sv(r.get('PROCMONT'))
        issuyear = _sv(r.get('ISSUYEAR'))
        issumont = _sv(r.get('ISSUMONT'))

        if paidind == 'P' and lasttran > 0:
            if procyear == issuyear and procmont == issumont and loantype not in HP:
                continue  # DELETE
        out.append(r)
    return out

loan = pl.DataFrame(_filter_loan_paid2(loan.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 13: GP3 - read fixed-width text file
# ---------------------------------------------------------------------------
gp3_rows = []
with open(GP3_FILE, 'r', encoding='ascii', errors='replace') as fg:
    for line in fg:
        line = line.rstrip('\n').ljust(89)
        try:
            branch_g   = int(line[0:3].strip() or '0')
            acctno_g   = int(line[3:13].strip() or '0')
            zero5_g    = int(line[13:18].strip() or '0')
            rptday_g   = int(line[18:20].strip() or '0')
            rptmon_g   = int(line[20:22].strip() or '0')
            rptyear_g  = int(line[22:26].strip() or '0')
            realisab_g = float(line[26:38].strip() or '0')
            totiis_g   = float(line[38:50].strip() or '0')
            totiisr_g  = float(line[50:62].strip() or '0')
            totwof_g   = float(line[62:74].strip() or '0')
            prinwoff_g = float(line[74:86].strip() or '0')
            gp3ind_g   = line[87:88]
        except ValueError:
            continue
        gp3_rows.append({
            'BRANCH':   branch_g,
            'ACCTNO':   acctno_g,
            'REALISAB': realisab_g,
            'TOTIIS':   totiis_g,
            'TOTIISR':  totiisr_g,
            'TOTWOF':   totwof_g,
            'PRINWOFF': prinwoff_g,
            'GP3IND':   gp3ind_g,
            'NOTENO':   None,
        })

gp3 = pl.DataFrame(gp3_rows).sort('ACCTNO')

# ---------------------------------------------------------------------------
# STEP 14: COMM - BNM.LNCOMM
# ---------------------------------------------------------------------------
comm = con.execute(
    f"SELECT * FROM read_parquet('{BNM_DIR}/LNCOMM.parquet')"
).pl().unique(subset=['ACCTNO','COMMNO'], keep='first').sort(['ACCTNO','COMMNO'])

# ---------------------------------------------------------------------------
# STEP 15: CCPT - credit policy tracking from LIMIT.OVERDFT
# ---------------------------------------------------------------------------
limit_overdft_full = con.execute(
    f"SELECT ACCTNO, LMTID, LMTSTDTE, CLIMATE_PRIN_TAXONOMY_CLASS "
    f"FROM read_parquet('{LIMIT_DIR}/OVERDFT.parquet')"
).pl()

# Join LAST_LIMIT_REV_DATE from current
current_llrd = current.select(['ACCTNO','LAST_LIMIT_REV_DATE'])
ccpt = limit_overdft_full.join(current_llrd, on='ACCTNO', how='left')

def _build_ccpt(rows):
    out = []
    for r in rows:
        r = dict(r)
        llrd    = _nv(r.get('LAST_LIMIT_REV_DATE'))
        lmtstdte= _nv(r.get('LMTSTDTE'))
        if llrd <= 0 or llrd < lmtstdte:
            ccpt_days = 0
        else:
            ccpt_days = llrd - lmtstdte
        r['CCPT_DAYS'] = ccpt_days
        r['CCPT_SEQ']  = 0 if ccpt_days > 0 else 1
        out.append(r)
    return out

ccpt = pl.DataFrame(_build_ccpt(ccpt.to_dicts()))
ccpt = ccpt.sort(['ACCTNO','CCPT_SEQ','LMTSTDTE','LAST_LIMIT_REV_DATE'],
                  descending=[False, False, True, True])
ccpt = ccpt.unique(subset=['ACCTNO'], keep='first')
ccpt = ccpt.select(['ACCTNO','LMTID','LMTSTDTE','LAST_LIMIT_REV_DATE',
                    'CCPT_DAYS','CCPT_SEQ','CLIMATE_PRIN_TAXONOMY_CLASS'])

# ---------------------------------------------------------------------------
# STEP 16: Merge LOAN with COMM -> set IND='A'/'B', ACTY='LN'
# ---------------------------------------------------------------------------
loan = loan.sort(['ACCTNO','COMMNO'])
loan = loan.join(comm, on=['ACCTNO','COMMNO'], how='left', suffix='_COMM')

def _set_ind(rows):
    out = []
    for r in rows:
        r = dict(r)
        in_b = r.get('_IN_COMM', False)
        r['IND']  = 'B' if in_b else 'A'
        r['ACTY'] = 'LN'
        if r['IND'] in ('A','B'):
            out.append(r)
    return out

loan = pl.DataFrame(_set_ind(loan.to_dicts()))
loan = loan.sort('ACCTNO')

# ---------------------------------------------------------------------------
# STEP 17: CURRENT - merge GP3, LMGP3, CCPT; build OD day logic; TAKEN flag
# ---------------------------------------------------------------------------
current = current.sort('ACCTNO')
current = current.join(gp3,   on='ACCTNO', how='left', suffix='_GP3')
current = current.join(lmgp3, on='ACCTNO', how='left', suffix='_LMGP3')
current = current.join(ccpt,  on='ACCTNO', how='left', suffix='_CCPT')

def _build_current_taken(rows):
    out = []
    for r in rows:
        r = dict(r)
        r['TAKEN'] = 'N'

        # EXODDATE / TEMPODDT from LMGP3
        excessdt = _nv(r.get('EXCESSDT'))
        toddate  = _nv(r.get('TODDATE'))
        if excessdt > 0:
            r['EXODDATE'] = excessdt
        if toddate > 0:
            r['TEMPODDT'] = toddate

        exoddate = _nv(r.get('EXODDATE'))
        tempoddt = _nv(r.get('TEMPODDT'))
        curbal   = _nv(r.get('CURBAL'))

        if (exoddate != 0 or tempoddt != 0) and curbal < 0:
            nodays = 0
            oddays = 0
            if exoddate == 0 and tempoddt == 0:
                oddays = 0
            elif exoddate > 0 and tempoddt == 0:
                odedate = _mmddyy_from_z11(exoddate)
                oddays  = _sas_date_num(odedate) if odedate else 0
            elif exoddate == 0 and tempoddt > 0:
                odtdate = _mmddyy_from_z11(tempoddt)
                oddays  = _sas_date_num(odtdate) if odtdate else 0
            else:
                odedate = _mmddyy_from_z11(exoddate)
                odtdate = _mmddyy_from_z11(tempoddt)
                odedays = _sas_date_num(odedate) if odedate else 0
                odtdays = _sas_date_num(odtdate) if odtdate else 0
                oddays  = odedays if odedays <= odtdays else odtdays

            if oddays > 0:
                nodays = SDATE_NUM - oddays
            r['NODAYS'] = nodays
            if nodays > 89:
                r['TAKEN'] = 'Y'

        # Additional TAKEN='Y' conditions
        riskcode = _sv(r.get('RISKCODE'))
        product  = _nv(r.get('PRODUCT'))
        mda      = _nv(r.get('MTD_DISBURSED_AMT'))
        mra      = _nv(r.get('MTD_REPAID_AMT'))
        openind  = _sv(r.get('OPENIND'))

        if (riskcode in ('1','2','3','4') or curbal < 0 or
            mda > 0 or mra > 0 or
            30 <= product <= 34 or
            openind in ('B','C','P')):
            r['TAKEN'] = 'Y'

        out.append(r)
    return out

current = pl.DataFrame(_build_current_taken(current.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 18: Merge OVERDFT with CURRENT -> OVERDFS
# ---------------------------------------------------------------------------
overdft_sorted = overdft.sort('ACCTNO')
current_sorted = current.sort('ACCTNO')

# Merge OVERDFT(A) with CURRENT(B) - RISKCODE, LMTDESC from OVERDFT added to CURRENT
current_ext = current_sorted.join(
    overdft_sorted.select(['ACCTNO','RISKCODE','LMTDESC']),
    on='ACCTNO', how='left', suffix='_OD'
)

# Full outer merge of overdft(A) and current(B)
overdfs = overdft_sorted.join(current_ext, on='ACCTNO', how='outer_coalesce', suffix='_CUR')

# Drop INTERDUE from overdfs
if 'INTERDUE' in overdfs.columns:
    overdfs = overdfs.drop('INTERDUE')

def _filter_overdfs(rows):
    """Keep rows matching OD filter conditions."""
    out = []
    for r in rows:
        r = dict(r)
        in_a    = r.get('_IN_A_OD', True)
        in_b    = r.get('_IN_B_CUR', False)
        apprlimt= _nv(r.get('APPRLIMT'))
        curbal  = _nv(r.get('CURBAL'))
        branch  = _nv(r.get('BRANCH'))
        taken   = _sv(r.get('TAKEN'))

        cond1 = in_a and in_b and apprlimt > 1
        cond2 = in_b and apprlimt <= 1 and curbal < 0 and branch != 998
        cond3 = taken == 'Y'

        if cond1 or cond2 or cond3:
            r['ACTY'] = 'OD'
            out.append(r)
    return out

overdfs_rows = _filter_overdfs(overdfs.to_dicts())
overdfs = pl.DataFrame(overdfs_rows) if overdfs_rows else pl.DataFrame()

# ---------------------------------------------------------------------------
# STEP 19: CCRISX - exclusion list
# ---------------------------------------------------------------------------
ccrisx_rows = []
with open(CCRISX_FILE, 'r', encoding='ascii', errors='replace') as fcx:
    for line in fcx:
        line = line.rstrip('\n').ljust(25)
        try:
            acctno_x = int(line[3:13].strip() or '0')
            noteno_x = int(line[16:21].strip() or '0')
        except ValueError:
            continue
        ccrisx_rows.append({'ACCTNO': acctno_x, 'NOTENO': noteno_x})

ccrisx = pl.DataFrame(ccrisx_rows).sort(['ACCTNO','NOTENO'])

# Exclude from LOAN
ccrisx_keys = set(zip(ccrisx['ACCTNO'].to_list(), ccrisx['NOTENO'].to_list()))
loan = loan.filter(
    ~(pl.struct(['ACCTNO','NOTENO']).map_elements(
        lambda s: (s['ACCTNO'], s['NOTENO']) in ccrisx_keys, return_dtype=pl.Boolean
    ))
)

# ---------------------------------------------------------------------------
# STEP 20: LOANW / LOANA split for products 984/985/994/995/996
# ---------------------------------------------------------------------------
loanw = loan.filter(pl.col('LOANTYPE').is_in([984, 985, 994, 995, 996]))
loana = loan.filter(~pl.col('LOANTYPE').is_in([984, 985, 994, 995, 996]))

# LOANW: sort by ACCTNO DESC NOTENO, keep first per ACCTNO
loanw = loanw.sort(['ACCTNO', 'NOTENO'], descending=[False, True])
loanw = loanw.unique(subset=['ACCTNO'], keep='first')

loan = pl.concat([loanw, loana], how='diagonal_relaxed')

# ---------------------------------------------------------------------------
# STEP 21: Additional LOAN transformations
# ---------------------------------------------------------------------------
def _loan_transforms(rows):
    out = []
    for r in rows:
        r = dict(r)
        # MATURED1
        maturedt = _nv(r.get('MATUREDT'))
        r['MATURED1'] = _sas_date_num(_mmddyy_from_z11(maturedt)) if maturedt > 0 else 0

        # ORIBRH = BRANCH (2017-1096)
        r['ORIBRH'] = _nv(r.get('BRANCH'))

        # COSTCTR branch overrides
        costctr = _nv(r.get('COSTCTR'))
        if costctr == 8044:
            r['BRANCH'] = 902; r['PENDBRH'] = 902
        elif costctr == 8048:
            r['BRANCH'] = 903; r['PENDBRH'] = 903

        # LEG='A' for legal action delq codes
        acctno  = _nv(r.get('ACCTNO'))
        delqcd  = _sv(r.get('DELQCD'))
        legal_codes = {'10','11','12','13','14','15','16','17','18',' ','09','19','20','21'}
        if acctno > 8000000000 and delqcd in legal_codes:
            r['LEG'] = 'A'
        elif acctno < 2999999999 and delqcd in legal_codes:
            r['LEG'] = 'A'

        out.append(r)
    return out

loan = pl.DataFrame(_loan_transforms(loan.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 22: ELDS.ELBNMAX + ELDS2.ELHPAX -> ELDS dataset
# ---------------------------------------------------------------------------
elds_aa = con.execute(
    f"SELECT AANO, AMOUNT, AADATE, RECONAME, REFTYPE, NMREF1, NMREF2, "
    f"       APVNME1, APVNME2, COMPLIBY, COMPLIDT, COMPLIGR, ACCTNO, "
    f"       BRANCH, PRODUCT, NEWIC, ADLREFNO, DEVNAME, APPRXSC, ACCTNO1, "
    f"       PCODCRIS, LNTERM AS TERM, CUSTCODE, SECTOR, FACGPTYPE, "
    f"       APPAMTSC, INDEXCD, MTHINSTLMT, PRODESC, FACCODE AS FACICODE, "
    f"       REFIN_FLG, SPAAMT, PRICING, PRITYPE, PRIRATE, SMESIZE, "
    f"       TRANBRNO, STRUPCO_3YR, LN_UTILISE_LOCAT_CD, CURRENCY_CD, "
    f"       APP_LIMIT_AMT_CCY, DSRISS3 "
    f"FROM read_parquet('{ELDS_DIR}/ELBNMAX.parquet')"
).pl()

elds_hp = con.execute(
    f"SELECT AANO, AMOUNT, AADATE, RECONAME, REFTYPE, NMREF1, NMREF2, "
    f"       APVNME1, APVNME2, COMPLIBY, COMPLIDT, COMPLIGR, ACCTNO, "
    f"       BRANCH, PRODUCT, NEWIC, ADLREFNO, DEVNAME, APPRXSC, ACCTNO1, "
    f"       PCODCRIS, TERM, RATE, SPAAMT, PRICING, PRITYPE, PRIRATE, "
    f"       SMESIZE, TRANBRNO, MTHINSTLMT, PRODESC, CUSTCODE, SECTOR, "
    f"       FACGPTYPE, APPAMTSC, STRUPCO_3YR, LN_UTILISE_LOCAT_CD, DSRISS3 "
    f"FROM read_parquet('{ELDS2_DIR}/ELHPAX.parquet')"
).pl()

elds = pl.concat([elds_aa, elds_hp], how='diagonal_relaxed')

def _fix_elds(rows):
    out = []
    for r in rows:
        r = dict(r)
        acctno = _nv(r.get('ACCTNO'))
        acctno1= _sv(r.get('ACCTNO1')).strip()
        if acctno in (None, 0) or (isinstance(acctno, float) and math.isnan(acctno)):
            # ACCTNO = COMPRESS(ACCTNO1, digits only)
            digits = ''.join(c for c in acctno1 if c.isdigit())
            r['ACCTNO'] = int(digits) if digits else 0

        refin_flg = _sv(r.get('REFIN_FLG')).strip()
        if not refin_flg:
            r['REFIN_FLG'] = 'NO'

        r['PREACCT'] = _nv(r.get('ACCTNO'))
        r['DSR']     = _nv(r.get('DSRISS3')) * 1
        out.append(r)
    return out

elds = pl.DataFrame(_fix_elds(elds.to_dicts()))
elds = elds.unique(subset=['AANO'], keep='first').sort('AANO')

# ---------------------------------------------------------------------------
# STEP 23: LNHPRJ - rejection records from ELNRJ + EHPRJ
# ---------------------------------------------------------------------------
elnrj = con.execute(
    f"SELECT AANO, ICDATE, AADATE, REASON, REASONFOR, STATUS "
    f"FROM read_parquet('{ELNRJ_DIR}/ELNMAX_REJ.parquet')"
).pl()

ehprj = con.execute(
    f"SELECT AANO, ICDATE, AADATE, REASON, REASONFOR, STATUS "
    f"FROM read_parquet('{EHPRJ_DIR}/EHPMAX_REJ.parquet')"
).pl()

lnhprj = pl.concat([elnrj, ehprj], how='diagonal_relaxed')

reject_statuses = {'REJECTED BY CUSTOMER(ACCEPTED)', 'REJECTED BY CUSTOMER'}
reject_reasons  = {'Cancelled AA at Pending Utilisation Stage',
                   'Declined AA at Pending Utilisation Stage',
                   'Non-Acceptance by Customer'}

def _fix_lnhprj(rows):
    out = []
    for r in rows:
        r = dict(r)
        status    = _sv(r.get('STATUS')).strip()
        reasonfor = _sv(r.get('REASONFOR')).strip()
        if status not in reject_statuses:
            r['AADATE'] = None
        if reasonfor not in reject_reasons:
            r['REASON'] = ' '
        out.append(r)
    return out

lnhprj = pl.DataFrame(_fix_lnhprj(lnhprj.to_dicts()))
lnhprj = lnhprj.rename({'AADATE': 'RJDATE'})
lnhprj = lnhprj.sort('ICDATE', descending=True)
lnhprj = lnhprj.unique(subset=['AANO'], keep='first')

elds = elds.join(lnhprj.select(['AANO','RJDATE','REASON']), on='AANO', how='left')
# Filter: BRANCH < 3000
elds = elds.filter(pl.col('BRANCH') < 3000)

# ---------------------------------------------------------------------------
# STEP 24: BNMSUM.SUMM1yyyymmdd -> CCRISP.SUMM1 (LU_ columns)
# ---------------------------------------------------------------------------
summ1_file = f"SUMM1{RDATE2}"
summ1 = con.execute(
    f"SELECT * FROM read_parquet('{BNMSUM_DIR}/{summ1_file}.parquet') "
    f"WHERE DATE <= {_reptdate_val}"
).pl()

# Keep only AANO and LU_* columns
lu_cols = [c for c in summ1.columns if c.startswith('LU_')]
summ1   = summ1.select(['AANO'] + lu_cols)
summ1   = summ1.sort(['AANO'])
summ1   = summ1.unique(subset=['AANO'], keep='first')
# Write CCRISP.SUMM1
summ1.write_parquet(str(CCRISP_DIR / "SUMM1.parquet"))

# ---------------------------------------------------------------------------
# STEP 25: LOAN - build AANO from VINNO
# ---------------------------------------------------------------------------
def _build_aano(rows):
    out = []
    for r in rows:
        r = dict(r)
        vinno = _sv(r.get('VINNO')).strip()
        aano1 = vinno[:13] if len(vinno) >= 13 else vinno
        comp  = ''.join(c for c in aano1 if c not in '@*(#)-')
        comp  = comp.strip()
        r['AANO'] = comp if len(comp) == 13 else ' '
        out.append(r)
    return out

loan = pl.DataFrame(_build_aano(loan.to_dicts()))
loan = loan.sort('AANO')

# ---------------------------------------------------------------------------
# STEP 26: Merge ELDS(A) + LOAN(B) + SUMM1; output LOAN and ELDS subsets
# ---------------------------------------------------------------------------
# Merge on AANO
merged = elds.join(loan, on='AANO', how='outer_coalesce', suffix='_LN')
merged = merged.join(summ1, on='AANO', how='left', suffix='_S1')

def _apply_elds_loan_merge(rows):
    loan_out  = []
    elds_out  = []
    for r in rows:
        r = dict(r)
        in_a = r.get('_IN_A_ELDS', False)
        in_b = r.get('_IN_B_LN',   False)

        # CURCODE from CURRENCY_CD
        currency_cd = _sv(r.get('CURRENCY_CD')).strip()
        if currency_cd:
            r['CURCODE'] = currency_cd

        # AMOUNT from APP_LIMIT_AMT_CCY > APPAMTSC
        app_lim = _nv(r.get('APP_LIMIT_AMT_CCY'))
        appamtsc= _nv(r.get('APPAMTSC'))
        if app_lim > 0:
            r['AMOUNT'] = app_lim
        elif appamtsc > 0:
            r['AMOUNT'] = appamtsc
        amount = _nv(r.get('AMOUNT'))
        if amount is not None and not (isinstance(amount, float) and math.isnan(amount)):
            r['APPRLMAA'] = amount

        # LOANTYPE from PRODUCT if missing
        if _nv(r.get('LOANTYPE')) in (None, 0):
            r['LOANTYPE'] = _nv(r.get('PRODUCT'))
        loantype = int(_nv(r.get('LOANTYPE')))
        census   = _nv(r.get('CENSUS'))

        # PRODCD
        if 600 <= loantype <= 699:
            prodcd = _sv(format_lnprod(int(math.floor(census))))
        elif loantype == 678:
            prodcd = '34111'
        else:
            prodcd = _sv(format_lnprod(loantype))
        r['PRODCD'] = prodcd

        # FACILITY from PRODCD/LOANTYPE
        facility = 0
        if prodcd in ('34190','34690'):     facility = 34210
        elif prodcd in ('34120','54120'):   facility = 34322
        elif prodcd in ('34111','54124'):   facility = 34331
        elif prodcd in ('34112',):          facility = 34341
        elif prodcd in ('34114',):          facility = 34351
        elif prodcd in ('34113',):          facility = 34361
        elif prodcd in ('34230',):          facility = 34371
        elif prodcd in ('34170',):          facility = 34460
        elif prodcd in ('34149','34117','34115'): facility = 34391

        # LOANTYPE-based FACILITY overrides
        if loantype in (110,141,200,201,225,227,230,232,237,255,258,259,239,244,246,249,409):
            facility = 34321
        elif loantype in (981,984,991,994,410):      facility = 34322
        elif loantype in (993,996,983):               facility = 34331
        elif loantype in (130,132,381,705,725,679):   facility = 34332
        elif loantype in (500,520,982,985,992,995):   facility = 34391
        elif loantype in (800,801,804,851,852,853,854,855,805,807,809,811,813): facility = 34391
        elif loantype in (802,803,856,857,858,859,860,817,818,806,808,810,812,814): facility = 34210
        elif loantype in (102,103,104,105):            facility = 34371

        r['FACILITY'] = facility

        # PURPOSES
        crispurp = _sv(r.get('CRISPURP')).strip()
        pcodcris = _sv(r.get('PCODCRIS')).strip()
        r['PURPOSES'] = crispurp if crispurp else pcodcris

        # FACCODE
        faccode = int(_sv(format_faccode(loantype)) or 0)
        if 600 <= loantype <= 699:
            if loantype in (678, 679):
                faccode = 34333  # SMR 2017-4375
            else:
                faccode = int(_sv(format_faccode(int(math.floor(census)))) or 0)
        r['FACCODE'] = faccode

        # HP PURPOSES -> FACCODE override
        hp_lt = {128,130,131,132,380,381,700,705,709,710,720,725,
                 750,752,760,983,993,996,678,679}
        if loantype in hp_lt:
            purposes = _sv(r.get('PURPOSES')).strip()
            # format_purphp equivalent
            purp_mapped = _sv(format_purphp(purposes))
            r['PURPOSES'] = purp_mapped
            if purp_mapped in ('3100','3101','3110','3120'):
                r['FACCODE'] = 34333
            elif purp_mapped in ('3300','3900','3901','4100','5100','5200','3200','3201'):
                r['FACCODE'] = 34334

        if r['FACCODE'] == 0:
            r['FACCODE'] = r['FACILITY']

        if in_b:
            loan_out.append(r)
        elif in_a and not in_b:
            elds_out.append(r)

    return loan_out, elds_out

loan_rows, elds_rows = _apply_elds_loan_merge(merged.to_dicts())
loan  = pl.DataFrame(loan_rows)  if loan_rows  else pl.DataFrame()
elds2 = pl.DataFrame(elds_rows)  if elds_rows  else pl.DataFrame()

# Branch 70-79 -> BRANCH=903 for ELDS records
def _fix_elds_branch(rows):
    out = []
    for r in rows:
        r = dict(r)
        loantype = _nv(r.get('LOANTYPE'))
        if 70 <= loantype <= 79:
            r['BRANCH'] = 903
        out.append(r)
    return out

elds2 = pl.DataFrame(_fix_elds_branch(elds2.to_dicts()))

# Save CCRISP.ELDS
elds2.sort('ACCTNO').write_parquet(str(CCRISP_DIR / "ELDS.parquet"))

loan = loan.sort(['ACCTNO','NOTENO'])

# ---------------------------------------------------------------------------
# STEP 27: CCOLLAT - COLL.COLLATER (CPRVDOLV, CDOLARV, ORIGVAL)
# ---------------------------------------------------------------------------
ccollat = con.execute(
    f"SELECT ACCTNO, NOTENO, CPRVDOLV, CDOLARV, ORIGVAL "
    f"FROM read_parquet('{COLL_DIR}/COLLATER.parquet')"
).pl().unique(subset=['ACCTNO','NOTENO'], keep='first').sort(['ACCTNO','NOTENO'])

loan = loan.join(ccollat, on=['ACCTNO','NOTENO'], how='left', suffix='_COL')

# ---------------------------------------------------------------------------
# STEP 28: OVERDFS - merge ELDX (PREACCT from ELDS), SUMM1, CCOLLAT
# ---------------------------------------------------------------------------
# ELDX: AANO -> LMTDESC
eldx = elds2.select(['AANO','PREACCT']).rename({'AANO': 'LMTDESC'}).sort('LMTDESC')

if 'LMTDESC' not in overdfs.columns:
    overdfs = overdfs.with_columns(pl.lit(None).cast(pl.Utf8).alias('LMTDESC'))

overdfs = overdfs.sort('LMTDESC')
overdfs = overdfs.join(eldx, on='LMTDESC', how='left', suffix='_EX')

# Merge CCRISP.SUMM1 (renaming AANO -> LMTDESC)
summ1_od = summ1.rename({'AANO': 'LMTDESC'})
overdfs  = overdfs.join(summ1_od, on='LMTDESC', how='left', suffix='_S1')
overdfs  = overdfs.drop('LMTDESC')
overdfs  = overdfs.sort('ACCTNO')

# Merge CCOLLAT (drop NOTENO)
ccollat_od = ccollat.select(['ACCTNO','CPRVDOLV','CDOLARV','ORIGVAL']).sort('ACCTNO')
overdfs = overdfs.join(ccollat_od, on='ACCTNO', how='left', suffix='_COL')

# DELQCD, FACCODE, LEG from USER2
def _build_overdfs(rows):
    out = []
    for r in rows:
        r = dict(r)
        r['DELQCD'] = 'X'
        product = _nv(r.get('PRODUCT', r.get('LOANTYPE', 0)))
        r['FACCODE'] = int(_sv(format_odfaccode(int(product))) or 0)

        user2 = _sv(r.get('USER2')).strip()
        if   user2 == '':  r['DELQCD'] = '  '
        elif user2 == 'A': r['DELQCD'] = '  '
        elif user2 == 'F': r['DELQCD'] = '10'
        elif user2 == 'B': r['DELQCD'] = '11'
        elif user2 == 'C': r['DELQCD'] = '12'
        elif user2 == 'G': r['DELQCD'] = '13'
        elif user2 == 'H': r['DELQCD'] = '14'
        elif user2 == 'I': r['DELQCD'] = '15'
        elif user2 == 'J': r['DELQCD'] = '16'
        elif user2 == 'E': r['DELQCD'] = '17'
        elif user2 == 'K': r['DELQCD'] = '18'
        elif user2 == 'L': r['DELQCD'] = '19'
        elif user2 == 'D': r['DELQCD'] = '20'
        elif user2 == 'M': r['DELQCD'] = '21'

        legal_codes = {'10','11','12','13','14','15','16','17','  ','18','19','20','21'}
        if r['DELQCD'] in legal_codes:
            r['LEG'] = 'A'

        out.append(r)
    return out

overdfs = pl.DataFrame(_build_overdfs(overdfs.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 29: EREV - review records (EMUL1617, EMUL12, EREVPRE)
# ---------------------------------------------------------------------------
erev_1617 = con.execute(
    f"SELECT NEWID, REVIEWNO, ACCTNOD, NOTENO, ADDRB02, ADDRB03, ADDRB04, LASTMDT "
    f"FROM read_parquet('{EREV_DIR}/EMUL1617.parquet')"
).pl()

erev_12 = con.execute(
    f"SELECT NEWID, REVIEWNO, ACCTNOD, NOTENO, ADDRB02, ADDRB03, ADDRB04, LASTMDT "
    f"FROM read_parquet('{EREV_DIR}/EMUL12.parquet')"
).pl()

erev_pre = con.execute(
    f"SELECT NEWID, REVIEWNO, ACCTNOD, NOTENO, ADDRB02, ADDRB03, ADDRB04, LASTMDT "
    f"FROM read_parquet('{EREV_DIR}/EREVPRE.parquet')"
).pl()

# Merge EMUL1617(A) with EMUL12 and EREVPRE by NEWID, REVIEWNO
erev = erev_1617.join(erev_12,  on=['NEWID','REVIEWNO'], how='outer_coalesce', suffix='_12')
erev = erev.join( erev_pre, on=['NEWID','REVIEWNO'], how='outer_coalesce', suffix='_PRE')

# Keep only EMUL1617 records (IN=A)
erev = erev_1617.join(
    pl.concat([erev_12, erev_pre], how='diagonal_relaxed'),
    on=['NEWID','REVIEWNO'], how='left', suffix='_OTHER'
)

# FORMAT ACCTNO1 10. NOTENO1 5.
def _build_erev(rows):
    out = []
    for r in rows:
        r = dict(r)
        r['ACCTNO1'] = int(_nv(r.get('ACCTNOD')))
        r['NOTENO1'] = int(_nv(r.get('NOTENO')))
        # RENAME for output
        r['ACCTNO'] = r['ACCTNO1']
        r['NOTENO'] = r['NOTENO1']
        out.append({k: v for k, v in r.items()
                    if k in ('ACCTNO','NOTENO','ADDRB02','ADDRB03','ADDRB04','LASTMDT')})
    return out

erev_out = pl.DataFrame(_build_erev(erev.to_dicts()))
erev_out = erev_out.sort('LASTMDT', descending=True)
erev_out = erev_out.unique(subset=['ACCTNO','NOTENO'], keep='first')

# ---------------------------------------------------------------------------
# STEP 30: ODWOF - OD written-off data
# ---------------------------------------------------------------------------
odwof_drop = {'LEDGBAL','ACCBRCH','COSTCTR','WRITE_DOWN_BAL','PRODUCT'}
odwof_raw  = con.execute(
    f"SELECT * FROM read_parquet('{ODWOF_DIR}/ODWOF.parquet')"
).pl()
drop_cols = [c for c in odwof_drop if c in odwof_raw.columns]
odwof = odwof_raw.drop(drop_cols).sort('ACCTNO')

# ---------------------------------------------------------------------------
# STEP 31: WOFFTOT / WOFFTOT1 - written-off totals from WOFF.WOFFTOT
# ---------------------------------------------------------------------------
wofftot_raw = con.execute(
    f"SELECT * FROM read_parquet('{WOFF_DIR}/WOFFTOT.parquet')"
).pl()

wofftot_rows  = []
wofftot1_rows = []
for r in wofftot_raw.to_dicts():
    r = dict(r)
    r['WFDATE'] = _nv(r.get('WOFFDT'))
    keep = {'ACCTNO': _nv(r.get('ACCTNO')),
            'NOTENO': r.get('NOTENO'),
            'WFDATE': r['WFDATE']}
    noteno = r.get('NOTENO')
    if noteno is None or (isinstance(noteno, float) and math.isnan(noteno)):
        wofftot_rows.append(keep)
    else:
        wofftot1_rows.append(keep)

wofftot  = pl.DataFrame(wofftot_rows).sort('ACCTNO')
wofftot1 = pl.DataFrame(wofftot1_rows).sort(['ACCTNO','NOTENO'])

# Merge LOAN with WOFFTOT (by ACCTNO)
loan = loan.sort('ACCTNO')
loan = loan.join(wofftot.drop('NOTENO'), on='ACCTNO', how='left', suffix='_WFT')

# Merge LOAN with WOFFTOT1 (by ACCTNO, NOTENO)
loan = loan.sort(['ACCTNO','NOTENO'])
loan = loan.join(wofftot1, on=['ACCTNO','NOTENO'], how='left', suffix='_WFT1')

# ---------------------------------------------------------------------------
# STEP 32: LOAN additional field calculations
# ---------------------------------------------------------------------------
def _loan_final_calcs(rows):
    out = []
    for r in rows:
        r = dict(r)
        if _nv(r.get('CAVAIAMT')) < 0:
            r['CAVAIAMT'] = 0

        loantype = int(_nv(r.get('LOANTYPE')))
        commno   = _nv(r.get('COMMNO'))
        netproc  = _nv(r.get('NETPROC'))
        corgamt  = _nv(r.get('CORGAMT'))
        cavaiamt = _nv(r.get('CAVAIAMT'))
        cusedamt = _nv(r.get('CUSEDAMT'))
        orgbal   = _nv(r.get('ORGBAL'))

        if loantype in (128,130,131,132,380,381,700,705,720,725,750,752,760):
            r['RLEASAMT'] = netproc
        elif commno > 0:
            no_rleas = {146,184,302,350,351,364,365,506,902,903,910,925,951,
                        604,605,634,660,685}
            if loantype not in no_rleas:
                r['RLEASAMT'] = corgamt - cavaiamt
            else:
                r['RLEASAMT'] = cusedamt
        else:
            r['RLEASAMT'] = orgbal

        out.append(r)
    return out

loan = pl.DataFrame(_loan_final_calcs(loan.to_dicts()))
loan = loan.sort(['ACCTNO','NOTENO'])

# ---------------------------------------------------------------------------
# STEP 33: Write CCRISP.LOAN - merge with EREV
# ---------------------------------------------------------------------------
loan_out = loan.join(erev_out, on=['ACCTNO','NOTENO'], how='left', suffix='_EREV')

def _fix_loan_refin(rows):
    out = []
    for r in rows:
        r = dict(r)
        r['REFIN_FLG'] = _sv(r.get('REFINANC_LN')).strip()[:1]
        r['OLD_FI_CD'] = _sv(r.get('OLD_FI')).strip()
        out.append(r)
    return out

loan_out = pl.DataFrame(_fix_loan_refin(loan_out.to_dicts()))
loan_out.write_parquet(str(CCRISP_DIR / "LOAN.parquet"), compression='zstd')

# ---------------------------------------------------------------------------
# STEP 34: Write CCRISP.OVERDFS - merge with EREV (no NOTENO), ODWOF, CCRISP.ODFEE
# ---------------------------------------------------------------------------
erev_od = erev_out.drop('NOTENO').sort('ACCTNO').unique(subset=['ACCTNO'], keep='first')

# CCRISP.ODFEE - pre-existing split dataset
odfee_path = CCRISP_DIR / "ODFEE.parquet"
if odfee_path.exists():
    odfee = con.execute(f"SELECT * FROM read_parquet('{odfee_path}')").pl().sort('ACCTNO')
else:
    odfee = pl.DataFrame({'ACCTNO': []})

overdfs_out = overdfs.sort('ACCTNO')
overdfs_out = overdfs_out.join(erev_od, on='ACCTNO', how='left', suffix='_EREV')
overdfs_out = overdfs_out.join(odwof,   on='ACCTNO', how='left', suffix='_ODWOF')
overdfs_out = overdfs_out.join(odfee,   on='ACCTNO', how='left', suffix='_ODFEE')

overdfs_out.write_parquet(str(CCRISP_DIR / "OVERDFS.parquet"), compression='zstd')

con.close()
print("EIBWCC5C completed successfully.")
print(f"  CCRISP.LOAN    -> {CCRISP_DIR}/LOAN.parquet")
print(f"  CCRISP.OVERDFS -> {CCRISP_DIR}/OVERDFS.parquet")
print(f"  CCRISP.ELDS    -> {CCRISP_DIR}/ELDS.parquet")
print(f"  CCRISP.SUMM1   -> {CCRISP_DIR}/SUMM1.parquet")
