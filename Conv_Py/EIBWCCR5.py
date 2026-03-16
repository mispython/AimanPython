#!/usr/bin/env python3
"""
Program  : EIBWCCR5.py
Purpose  : PBB (Public Bank Berhad) CCRIS2 data preparation.
           Reads loan and OD account data, applies BNM CCRIS taxonomy and
           product classification rules, merges collateral/write-off/ELDS/
           factor/SMC data, and produces fixed-width output files:
             ACCTCRED  (LRECL=800)  - Account credit record
             SUBACRED  (LRECL=1550) - Sub-account credit record
             CREDITPO  (LRECL=700)  - Credit position record
             PROVISIO  (LRECL=600)  - Provision record
             LEGALACT  (LRECL=100)  - Legal action record
             CREDITOD  (LRECL=250)  - Credit OD (closed/no-balance) record
             REPAID7B  (LRECL=200)  - Repaid 7B record (OD repayment types)
"""

# ---------------------------------------------------------------------------
# JCL: //DELETE EXEC PGM=IEFBR14  (pre-run cleanup of output files)
# DD2  DSN=SAP.PBB.CCRIS2.ACCTCRED  DISP=(MOD,DELETE,DELETE)
# DD3  DSN=SAP.PBB.CCRIS2.SUBACRED  DISP=(MOD,DELETE,DELETE)
# DD4  DSN=SAP.PBB.CCRIS2.CREDITPO  DISP=(MOD,DELETE,DELETE)
# DD5  DSN=SAP.PBB.CCRIS2.PROVISIO  DISP=(MOD,DELETE,DELETE)
# DD6  DSN=SAP.PBB.CCRIS2.LEGALACT  DISP=(MOD,DELETE,DELETE)
# DD7  DSN=SAP.PBB.CCRIS2.CREDITOD  DISP=(MOD,DELETE,DELETE)
# DD8  DSN=SAP.PBB.CCRIS2.REPAID7B  DISP=(MOD,DELETE,DELETE)
# ---------------------------------------------------------------------------

import os
import sys
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
# %INC PGM(PBBLNFMT) - format_lnprod used for PRODCD derivation
from PBBLNFMT import format_lnprod

# %INC PGM(PFBCRFMT) - BNM taxonomy formats used throughout
from PFBCRFMT import (
    format_faccode,
    format_odfaccode,
    format_odfacility,
    format_odfconcept,
    format_odfundsch,
    format_fconcept,
    format_fundsch,
    format_fschemec,
    format_synd,
)

# ---------------------------------------------------------------------------
# PATH SETUP
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).parent

# Input libraries
BNM_DIR    = BASE_DIR / "data" / "BNM"          # BNM.REPTDATE, BNM.LNCOMM, BNM.LNACC4
CCRISP_DIR = BASE_DIR / "data" / "CCRISP"       # CCRISP.LOAN, CCRISP.OVERDFS
CCRIS_DIR  = BASE_DIR / "data" / "CCRIS"        # CCRIS.PREVODFT, CCRIS.SASDATA (write target)
LIMT_DIR   = BASE_DIR / "data" / "LIMT"         # LIMT.OVERDFT
ELDS_DIR   = BASE_DIR / "data" / "ELDS"         # ELDS.ELNMAX
DISPAY_DIR = BASE_DIR / "data" / "DISPAY"       # DISPAY.DISPAYMTHnn
WOPS_DIR   = BASE_DIR / "data" / "WOPS"         # WOPS.WOPOSnnWk
WOMV_DIR   = BASE_DIR / "data" / "WOMV"         # WOMV.SUMOVnn
ODSQ_DIR   = BASE_DIR / "data" / "ODSQ"         # ODSQ.ODSQ
LNSQ_DIR   = BASE_DIR / "data" / "LNSQ"         # LNSQ.LNSQ
IMPRLN_DIR = BASE_DIR / "data" / "IMPRLN"       # IMPRLN.LNHIST780 (TC780)
CRDTLN_DIR = BASE_DIR / "data" / "CRDTLN"       # CRDTLN.LNHIST310 (TC310)

# Fixed-width text input files
WRITOFF_FILE  = BASE_DIR / "data" / "txt" / "WRIOFAC.txt"    # SAP.BNM.PROGRAM(WRIOFAC)
ELDSTX3_FILE  = BASE_DIR / "data" / "txt" / "ELDSTX3.txt"    # SAP.PBB.ELDS.NEWBNM3.TEXT
APP7_FILE     = BASE_DIR / "data" / "txt" / "APP7.txt"        # SAP.PBB.ELDS.AABASEL.APP17.TEXT (concat)
APP10_FILE    = BASE_DIR / "data" / "txt" / "APP10.txt"       # SAP.PBB.ELDS.AABASEL.APP10.TEXT (concat)
ELDSRV12_FILE = BASE_DIR / "data" / "txt" / "ELDSRV12.txt"   # SAP.PBB.ELDS.BASEL.RV12.TEXT (concat)
ELDSRV5_FILE  = BASE_DIR / "data" / "txt" / "ELDSRV5.txt"    # SAP.PBB.ELDS.BASEL.RV05.TEXT (concat)
ELDSRV1_FILE  = BASE_DIR / "data" / "txt" / "ELDSRV1.txt"    # SAP.PBB.ELDS.BASEL.RV01.TEXT (concat)

# Output files
OUTPUT_DIR   = BASE_DIR / "output" / "CCRIS2"
ACCTCRED_OUT = OUTPUT_DIR / "ACCTCRED.txt"
SUBACRED_OUT = OUTPUT_DIR / "SUBACRED.txt"
CREDITPO_OUT = OUTPUT_DIR / "CREDITPO.txt"
PROVISIO_OUT = OUTPUT_DIR / "PROVISIO.txt"
LEGALACT_OUT = OUTPUT_DIR / "LEGALACT.txt"
CREDITOD_OUT = OUTPUT_DIR / "CREDITOD.txt"
REPAID7B_OUT = OUTPUT_DIR / "REPAID7B.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Pre-run cleanup (JCL IEFBR14 DISP=(MOD,DELETE,DELETE))
for _f in [ACCTCRED_OUT, SUBACRED_OUT, CREDITPO_OUT,
           PROVISIO_OUT, LEGALACT_OUT, CREDITOD_OUT, REPAID7B_OUT]:
    _f.unlink(missing_ok=True)

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
REPTYEA2  = str(REPTDATE.year)[2:]
RDATE     = REPTDATE.strftime("%d/%m/%Y")
CDATE_NUM = _reptdate_val                          # SAS &CDATE (numeric days)
REPTDAY   = f"{_day:02d}"
SDATE_NUM = _reptdate_val                          # &SDATE as SAS numeric days
SDATE     = f"{_reptdate_val:05d}"
MDATE     = f"{_reptdate_val:05d}"

print(f"EIBWCCR5 started. REPTDATE={REPTDATE} NOWK={NOWK} REPTMON={REPTMON} REPTYEAR={REPTYEAR}")
print(f"  ACCTCRED -> {ACCTCRED_OUT}")
print(f"  SUBACRED -> {SUBACRED_OUT}")
print(f"  CREDITPO -> {CREDITPO_OUT}")
print(f"  PROVISIO -> {PROVISIO_OUT}")
print(f"  LEGALACT -> {LEGALACT_OUT}")
print(f"  CREDITOD -> {CREDITOD_OUT}")
print(f"  REPAID7B -> {REPAID7B_OUT}")

# ---------------------------------------------------------------------------
# PRODUCT LIST MACROS
# ---------------------------------------------------------------------------
HP    = {128, 130, 380, 381, 700, 705, 983, 993, 996}
ALHP  = {128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 983, 993, 678, 679, 698, 699}
PRDA  = {302, 350, 364, 365, 506, 902, 903, 910, 925, 951}
PRDB  = {983, 993, 996}
PRDC  = {110, 112, 114, 200, 201, 209, 210, 211, 212, 225, 227, 141,
         230, 232, 237, 239, 243, 244, 246}
PRDD  = {111, 113, 115, 116, 117, 118, 119, 120, 126, 127, 129, 139, 140, 147, 173,
         170, 180, 181, 182, 193, 204, 205, 214, 215, 219, 220, 226, 228, 231, 233,
         234, 235, 245, 247, 142, 143, 183, 532, 533, 573, 574, 248,
         236, 238, 240, 241, 242, 300, 301, 304, 305, 345, 359, 361, 362, 363,
         504, 505, 509, 510, 515, 516, 531, 517, 518, 519, 521, 522,
         523, 524, 525, 526, 527, 528, 529, 530, 555, 556, 559, 560, 561, 564,
         565, 566, 567, 568, 569, 570, 900, 901, 906, 907, 909, 914, 915, 950}
PRDE  = {135, 136, 138, 182, 194, 196, 315, 320, 325, 330, 335, 340,
         355, 356, 357, 358, 391, 148, 174, 195, 461}
PRDF  = {110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 122, 126,
         141, 142, 143, 147, 148, 173, 174,
         127, 129, 139, 140, 170, 172, 181, 194, 195, 196}
PRDG  = {6, 7, 61, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
         225, 226, 300, 301, 302, 304, 305, 309, 310, 315, 320, 325, 330, 335,
         340, 345, 350, 355, 356, 357, 358, 362, 363, 364, 365, 391, 504, 505,
         506, 509, 510, 515, 516, 900, 901, 902, 903, 904, 905, 909, 914, 915,
         919, 920, 950, 951}
PRDH  = {500, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529,
         530, 555, 556, 559, 560, 561, 564, 565, 566, 567, 568, 569, 570}
PRDJ  = (set(range(4, 8)) | {15, 20} | set(range(25, 35)) | {60, 61, 62, 63} |
         set(range(70, 79)) | {100, 108})

ALP = set('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ')

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


def _mmddyy_from_z11(val) -> Optional[date]:
    """
    SAS pattern: INPUT(SUBSTR(PUT(val, Z11.), 1, 8), MMDDYY8.)
    The Z11. format pads val with leading zeros to 11 digits.
    The first 8 chars represent MMDDYYYY.
    """
    if val is None or (isinstance(val, float) and math.isnan(val)) or int(val) <= 0:
        return None
    s = f"{int(val):011d}"
    mmddyyyy = s[:8]
    try:
        mm = int(mmddyyyy[0:2])
        dd = int(mmddyyyy[2:4])
        yy = int(mmddyyyy[4:8])
        return date(yy, mm, dd)
    except Exception:
        return None


def _z11_parts(val) -> tuple:
    """
    Return (dd_str2, mm_str2, yy_str4) from PUT(val,Z11.) layout:
    positions (1-based): 1-2=MM, 3-4=DD, 5-8=YYYY
    SAS: DLVRDD = SUBSTR(PUT(x,Z11.),3,2) -> DD
         DLVRMM = SUBSTR(PUT(x,Z11.),1,2) -> MM
         DLVRYR = SUBSTR(PUT(x,Z11.),5,4) -> YYYY
    """
    if val is None or (isinstance(val, float) and math.isnan(val)) or int(val) <= 0:
        return ('00', '00', '0000')
    s = f"{int(val):011d}"
    mm = s[0:2]
    dd = s[2:4]
    yy = s[4:8]
    return (dd, mm, yy)


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


def _intnx_month_end(d: date, n: int) -> date:
    """Equivalent to SAS INTNX('MONTH', d, n, 'E') - end of month n months from d."""
    total_months = d.year * 12 + d.month - 1 + n
    y = total_months // 12
    m = total_months % 12 + 1
    last_day = calendar.monthrange(y, m)[1]
    return date(y, m, last_day)


def _sas_date_num(d: Optional[date]) -> int:
    """Convert Python date to SAS numeric date."""
    if d is None:
        return 0
    return (d - date(1960, 1, 1)).days


def _fmt_z(val, width: int) -> str:
    """Format numeric as zero-padded integer string of given width."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return '0' * width
    return f"{int(round(val)):0{width}d}"


def _fmt_n(val, width: int) -> str:
    """Format numeric as right-aligned integer string (SAS numeric format n.)."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return ' ' * width
    return f"{int(round(val)):>{width}d}"


def _fmt_f(val, width: int, dec: int) -> str:
    """Format numeric as fixed decimal (SAS width.dec)."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return ' ' * width
    fmt = f"{{:>{width}.{dec}f}}"
    return fmt.format(float(val))


def _fmt_s(val, width: int, upcase: bool = False) -> str:
    """Format string left-justified, space-padded to width."""
    s = str(val) if val is not None else ''
    if upcase:
        s = s.upper()
    return s[:width].ljust(width)


def _build_buf(size: int) -> bytearray:
    """Create a space-filled bytearray for a fixed-width record."""
    return bytearray(b' ' * size)


def _put_rec(buf: bytearray, pos: int, val: str) -> None:
    """Write string val into buf at 1-based position pos."""
    s = val if isinstance(val, str) else str(val)
    start = pos - 1
    end   = start + len(s)
    if end > len(buf):
        s = s[:len(buf) - start]
        end = len(buf)
    buf[start:end] = s.encode('ascii', errors='replace')


def _mdy(mm, dd, yy) -> Optional[date]:
    """SAS MDY equivalent."""
    try:
        return date(int(yy), int(mm), int(dd))
    except Exception:
        return None


def _yymmddn8(d: Optional[date]) -> str:
    """SAS YYMMDDN8. format -> YYYYMMDD."""
    if d is None:
        return '00000000'
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


# ---------------------------------------------------------------------------
# STEP 1: DATES DATA STEP (already resolved above via _dates_df)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# STEP 2: Load LOAN data (CCRISP.LOAN + CCRISP.OVERDFS)
# ---------------------------------------------------------------------------
loan_df = con.execute(
    f"SELECT *, 'L' AS ACCTIND FROM read_parquet('{CCRISP_DIR}/LOAN.parquet')"
).pl()
od_df_raw = con.execute(
    f"SELECT *, 'D' AS ACCTIND FROM read_parquet('{CCRISP_DIR}/OVERDFS.parquet')"
).pl()

loan = pl.concat([loan_df, od_df_raw], how='diagonal_relaxed')

# Compute FINRELDD/FINRELMM/FINRELYY and ODEDD/ODEMM/ODEYY
def _add_date_parts(rows):
    out = []
    for r in rows:
        r = dict(r)
        r['FINRELDD'] = 0; r['FINRELMM'] = 0; r['FINRELYY'] = 0
        r['ODEDD']    = 0; r['ODEMM']    = 0; r['ODEYY']    = 0

        freleas = _nv(r.get('FRELEAS'))
        if freleas > 0:
            frepdate = _mmddyy_from_z11(freleas)
            if frepdate:
                r['FINRELDD'] = frepdate.day
                r['FINRELMM'] = frepdate.month
                r['FINRELYY'] = frepdate.year

        exoddate = _nv(r.get('EXODDATE'))
        if exoddate > 0:
            odedate = _mmddyy_from_z11(exoddate)
            if odedate:
                r['ODEDD'] = odedate.day
                r['ODEMM'] = odedate.month
                r['ODEYY'] = odedate.year

        # 2020-2189: LN_UTILISE_LOCAT_CD
        loantype = _nv(r.get('LOANTYPE'))
        acctno   = _nv(r.get('ACCTNO'))
        noteno   = _nv(r.get('NOTENO'))
        issxdte  = _nv(r.get('ISSXDTE'))
        mniapdte = _nv(r.get('MNIAPDTE'))
        state    = _sv(r.get('STATE'))

        oct2019  = _sas_date_num(date(2019, 10, 1))

        if (loantype == 392 or
            (2500000000 <= acctno <= 2599999999 and 40000 <= noteno <= 49999) or
            3000000000 <= acctno <= 3999999999):
            r['LN_UTILISE_LOCAT_CD'] = state if issxdte >= oct2019 else ''
        elif loantype != 392:
            r['LN_UTILISE_LOCAT_CD'] = state if (issxdte >= oct2019 and mniapdte >= oct2019) else ''

        out.append(r)
    return out

loan = pl.DataFrame(_add_date_parts(loan.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 3: Sort LOAN by ACCTNO; merge LIMT.OVERDFT
# ---------------------------------------------------------------------------
loan = loan.sort('ACCTNO')

limt = con.execute(
    f"SELECT * FROM read_parquet('{LIMT_DIR}/OVERDFT.parquet') "
    f"WHERE LMTAMT > 1 AND APPRLIMT > 1 "
    f"ORDER BY ACCTNO, LMTRATE DESC"
).pl()

def _seq_lmt(r: dict) -> int:
    idx = _nv(r.get('LMTINDEX'))
    fdrcno = _nv(r.get('FDRCNO'))
    if idx in (1, 30):        return 1
    if idx in (38, 39):       return 2
    if idx == 52:             return 3
    if idx == 0 and fdrcno > 0: return 4
    if idx in (18, 19):       return 5
    if idx == 0:              return 6
    return 7

limt_rows = limt.to_dicts()
for r in limt_rows:
    lmtenddt = _nv(r.get('LMTENDDT'))
    lmtdte   = _mmddyy_from_z11(lmtenddt)
    r['LMTDTE']  = _sas_date_num(lmtdte)
    r['LMTDATE'] = _sas_date_num(lmtdte)
    r['SEQ']     = _seq_lmt(r)
    # DROP INTERDUE CLIMATE_PRIN_TAXONOMY_CLASS
    r.pop('INTERDUE', None)
    r.pop('CLIMATE_PRIN_TAXONOMY_CLASS', None)

limt_pl = pl.DataFrame(limt_rows).sort(['ACCTNO', 'SEQ'])
limt_pl = limt_pl.unique(subset=['ACCTNO'], keep='first')

# CCRIS.PREVODFT
odcr = con.execute(
    f"SELECT ACCTNO, APPRLIMT AS PREVALIMT, OUTSTAND AS PREVOBAL "
    f"FROM read_parquet('{CCRIS_DIR}/PREVODFT.parquet') "
    f"WHERE ACCTNO BETWEEN 3000000000 AND 3999999999"
).pl().unique(subset=['ACCTNO'], keep='first').sort('ACCTNO')

loan = loan.join(limt_pl.select(['ACCTNO'] + [c for c in limt_pl.columns
                  if c not in loan.columns or c == 'ACCTNO']),
                 on='ACCTNO', how='left', suffix='_LIMT')
loan = loan.join(odcr, on='ACCTNO', how='left', suffix='_ODCR')

# ---------------------------------------------------------------------------
# STEP 4: ELDS.ELNMAX merge (SPAAMT, DESGRECO)
# ---------------------------------------------------------------------------
elds = con.execute(
    f"SELECT AANO, SPAAMT, DESGRECO "
    f"FROM read_parquet('{ELDS_DIR}/ELNMAX.parquet') "
    f"WHERE AANO IS NOT NULL AND AANO <> ''"
).pl().sort('AANO')

loan = loan.sort('AANO')
loan = loan.join(elds, on='AANO', how='left', suffix='_ELDS')

def _fix_spaamt(rows):
    out = []
    for r in rows:
        r = dict(r)
        spaamt = _nv(r.get('SPAAMT'))
        if spaamt in (None, 0) or (isinstance(spaamt, float) and math.isnan(spaamt)):
            cdolarv  = _nv(r.get('CDOLARV'))
            cprvdolv = _nv(r.get('CPRVDOLV'))
            origval  = _nv(r.get('ORIGVAL'))
            if cdolarv not in (None, 0) and not (isinstance(cdolarv, float) and math.isnan(cdolarv)):
                r['SPAAMT'] = cdolarv
            elif cprvdolv not in (None, 0) and not (isinstance(cprvdolv, float) and math.isnan(cprvdolv)):
                r['SPAAMT'] = cprvdolv
            elif origval not in (None, 0) and not (isinstance(origval, float) and math.isnan(origval)):
                r['SPAAMT'] = origval
            else:
                r['SPAAMT'] = 0
        out.append(r)
    return out

loan = pl.DataFrame(_fix_spaamt(loan.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 5: DISPAY merge (DISBURSE, REPAID per ACCTNO/NOTENO)
# ---------------------------------------------------------------------------
loan = loan.sort(['ACCTNO', 'NOTENO'])

dispay = con.execute(
    f"SELECT ACCTNO, NOTENO, DISBURSE, REPAID "
    f"FROM read_parquet('{DISPAY_DIR}/DISPAYMTH{REPTMON}.parquet')"
).pl().sort(['ACCTNO', 'NOTENO'])

loan = loan.join(dispay, on=['ACCTNO', 'NOTENO'], how='left', suffix='_DISP')
for col in ['DISBURSE', 'REPAID']:
    if f'{col}_DISP' in loan.columns:
        loan = loan.with_columns(
            pl.when(pl.col(f'{col}_DISP').is_not_null())
            .then(pl.col(f'{col}_DISP'))
            .otherwise(pl.col(col))
            .alias(col)
        ).drop(f'{col}_DISP')

# ---------------------------------------------------------------------------
# STEP 6: WRITTEN-OFF data
# ---------------------------------------------------------------------------
wopos = con.execute(
    f"SELECT ACCTNO, NOTENO, ACTOWE "
    f"FROM read_parquet('{WOPS_DIR}/WOPOS{REPTMON}{NOWK}.parquet')"
).pl().sort(['ACCTNO', 'NOTENO'])

sumov = con.execute(
    f"SELECT ACCTNO, NOTENO, BDR_MTH, SC_MTH, RC_MTH, NAI_MTH "
    f"FROM read_parquet('{WOMV_DIR}/SUMOV{REPTMON}.parquet')"
).pl().sort(['ACCTNO', 'NOTENO'])

odsq_df  = con.execute(
    f"SELECT ACCTNO, WOAMT, ISWO, SPWO "
    f"FROM read_parquet('{ODSQ_DIR}/ODSQ.parquet')"
).pl()
lnsq_df  = con.execute(
    f"SELECT ACCTNO, NOTENO, WOAMT, ISWO, SPWO, WODATE "
    f"FROM read_parquet('{LNSQ_DIR}/LNSQ.parquet')"
).pl()
wosq = pl.concat([odsq_df, lnsq_df], how='diagonal_relaxed').sort(['ACCTNO', 'NOTENO'])

wof = wopos.join(sumov, on=['ACCTNO', 'NOTENO'], how='outer_coalesce')
wof = wof.join(wosq,   on=['ACCTNO', 'NOTENO'], how='outer_coalesce')
wof = wof.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

loan = loan.join(wof, on=['ACCTNO', 'NOTENO'], how='left', suffix='_WOF')

def _apply_wof(rows):
    out = []
    reptmon_num = int(REPTMON)
    for r in rows:
        r = dict(r)
        actowe = r.get('ACTOWE')
        if actowe is not None and not (isinstance(actowe, float) and math.isnan(actowe)):
            r['LEDGBAL'] = actowe

        if reptmon_num in (3, 6, 9, 12):
            r['TOTWOF'] = _nv(r.get('ISWO'))
            r['SPWOF']  = _nv(r.get('SPWO'))
        else:
            r['WOAMT'] = None

        apprdate = _nv(r.get('APPRDATE'))
        if apprdate > 0:
            apprdt = _mmddyy_from_z11(apprdate)
            r['ORIWODATE'] = int(f"{apprdt.year}{apprdt.month:02d}{apprdt.day:02d}") if apprdt else 0
        else:
            r['ORIWODATE'] = 0

        wodate_val = _nv(r.get('WODATE'))
        if wodate_val > 0:
            wd = _sas_date(wodate_val)
            r['LASTMDATE'] = int(f"{wd.year}{wd.month:02d}{wd.day:02d}") if wd else 0
        else:
            r['LASTMDATE'] = 0

        acty     = _sv(r.get('ACTY'))
        product  = _nv(r.get('PRODUCT'))
        loantype = _nv(r.get('LOANTYPE'))
        wdb      = _nv(r.get('WRITE_DOWN_BAL'))
        wdb_zero = wdb in (0,) or (isinstance(wdb, float) and math.isnan(wdb))
        if wdb_zero:
            if (acty == 'OD' and 30 <= product <= 34) or \
               (acty == 'LN' and 600 <= loantype <= 699):
                if actowe is not None and not (isinstance(actowe, float) and math.isnan(actowe)):
                    r['BALANCE'] = actowe
        out.append(r)
    return out

loan = pl.DataFrame(_apply_wof(loan.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 7: TRAN780 (LNHIST780 TC780) merge
# ---------------------------------------------------------------------------
tran780 = con.execute(
    f"SELECT ACCTNO, NOTENO, HCURBAL, EFFDATE "
    f"FROM read_parquet('{IMPRLN_DIR}/LNHIST780.parquet') "
    f"ORDER BY ACCTNO, NOTENO, EFFDATE DESC"
).pl().unique(subset=['ACCTNO', 'NOTENO'], keep='first')

loan = loan.join(tran780, on=['ACCTNO', 'NOTENO'], how='left', suffix='_T780')

# LNHIST310 (TC310) merge
lnhist310 = con.execute(
    f"SELECT ACCTNO, NOTENO, TOTAMT, CHANNEL, FRTDISCODE, LSTDISCODE, "
    f"       POSTDATE1, FRTDIDTIND "
    f"FROM read_parquet('{CRDTLN_DIR}/LNHIST310.parquet')"
).pl()

loan = loan.join(lnhist310, on=['ACCTNO', 'NOTENO'], how='left', suffix='_T310')

# STP flag and date parts from EFFDATE / POSTDATE1
def _add_stp_dates(rows):
    out = []
    for r in rows:
        r = dict(r)
        loantype = _nv(r.get('LOANTYPE'))
        channel  = _nv(r.get('CHANNEL'))
        if loantype in HP and channel == 500:
            r['STP'] = 'Y'
        elif loantype not in HP and channel == 5:
            r['STP'] = 'Y'
        else:
            r['STP'] = 'N'

        effdate = _nv(r.get('EFFDATE'))
        if effdate > 0:
            ed = _sas_date(effdate)
            r['IMPAIRYY'] = str(ed.year) if ed else ''
            r['IMPAIRMM'] = f"{ed.month:02d}" if ed else ''
            r['IMPAIRDD'] = f"{ed.day:02d}" if ed else ''
        else:
            r['IMPAIRYY'] = ''
            r['IMPAIRMM'] = ''
            r['IMPAIRDD'] = ''

        postdate1 = _nv(r.get('POSTDATE1'))
        if postdate1 > 0:
            pd = _sas_date(postdate1)
            r['POSTDTYY'] = str(pd.year) if pd else ''
            r['POSTDTMM'] = f"{pd.month:02d}" if pd else ''
            r['POSTDTDD'] = f"{pd.day:02d}" if pd else ''
        else:
            r['POSTDTYY'] = ''
            r['POSTDTMM'] = ''
            r['POSTDTDD'] = ''
        out.append(r)
    return out

loan = pl.DataFrame(_add_stp_dates(loan.to_dicts()))
loan = loan.sort(['ACCTNO', 'COMMNO'])

# ---------------------------------------------------------------------------
# STEP 8: DATA ACCTCRED - main processing loop
# ---------------------------------------------------------------------------
# (Sorted by ACCTNO COMMNO before the loop)
acctcred_rows = []

f_subacred = open(SUBACRED_OUT, 'wb')
f_creditpo = open(CREDITPO_OUT, 'wb')
f_provisio = open(PROVISIO_OUT, 'wb')
f_legalact = open(LEGALACT_OUT, 'wb')

for r in loan.to_dicts():
    r = dict(r)

    # Init fields
    r['OLDBRH']  = 0
    r['ARREARS'] = 0
    r['NODAYS']  = 0
    r['XNODAYS'] = 0
    r['INSTALM'] = 0
    r['FICODE']  = _nv(r.get('BRANCH'))
    r['RRPAYCNT'] = 0
    r['RRTAG']    = ''

    r['PAYDD']   = 0;  r['PAYMM']   = 0;  r['PAYYR']   = 0
    r['GRANTDD'] = 0;  r['GRANTMM'] = 0;  r['GRANTYR'] = 0
    r['LNMATDD'] = 0;  r['LNMATMM'] = 0;  r['LNMATYR'] = 0
    r['ORMATDD'] = 0;  r['ORMATMM'] = 0;  r['ORMATYR'] = 0
    r['DLVRDD']  = 0;  r['DLVRMM']  = 0;  r['DLVRYR']  = 0
    r['BILDUEDD']= 0;  r['BILDUEMM']= 0;  r['BILDUEYR']= 0
    r['RRCNTDD'] = 0;  r['RRCNTMM'] = 0;  r['RRCNTYR'] = 0
    r['RRCOMPDD']= 0;  r['RRCOMPMM']= 0;  r['RRCOMPYR']= 0
    r['COLLMM']  = 0;  r['COLLYR']  = 0;  r['FCONCEPT']= 0

    acctno  = int(_nv(r.get('ACCTNO')))
    noteno  = int(_nv(r.get('NOTENO')))

    # Exclusions
    if ((acctno == 8027370307 and noteno == 10) or
        (acctno == 8124498008 and noteno == 10010) or
        (acctno == 8124948801 and noteno == 10)):
        continue

    # FX adjustments
    curcode = _sv(r.get('CURCODE'))
    forate  = _nv(r.get('FORATE'))
    if curcode != 'MYR' and forate in (0, None):
        r['FORATE'] = _nv(r.get('SPOTRATE'))
        forate = r['FORATE']

    if curcode not in ('MYR', ''):
        r['CURBAL']           = round(_nv(r.get('CURBAL'))           * forate, 2)
        r['FEEAMT']           = round(_nv(r.get('FEEAMT'))           * forate, 2)
        r['CCRIS_INSTLAMT']   = round(_nv(r.get('CCRIS_INSTLAMT'))   * forate, 2)

    acty     = _sv(r.get('ACTY'))
    product  = int(_nv(r.get('PRODUCT')))
    loantype = int(_nv(r.get('LOANTYPE')))

    # ------------------------------------------------------------------
    # OD BRANCH
    # ------------------------------------------------------------------
    if acty == 'OD':
        r['FACILITY'] = int(_sv(format_odfacility(product)).strip() or 0) \
                        if format_odfacility(product) else 0
        r['BORSTAT']  = _sv(r.get('RRIND'))
        r['ASSMDATE'] = _nv(r.get('RRMAINDT'))
        r['SYNDICAT'] = ' '
        r['COLLVAL']  = _nv(r.get('REALISAB'))
        r['PURPOSES'] = _sv(r.get('CCRICODE'))[:4]
        if _nv(r.get('COLLVAL')) in (0, None):
            r['COLLVAL'] = 0

        r['ODXSAMT']  = _nv(r.get('ODXSAMT'))  * 100
        r['LMTAMT']   = _nv(r.get('LMTAMT'))   * 100
        r['REPAID']   = _nv(r.get('REPAID'))    * 100
        r['DISBURSE'] = _nv(r.get('DISBURSE'))  * 100
        r['REBATE']   = _nv(r.get('REBATE'))    * 100
        r['BILTOT']   = 0
        r['LIMTCURR'] = _nv(r.get('APPRLIMT'))  * 100
        r['LOANTYPE'] = product
        r['APCODE']   = product
        r['NOTENO']   = 0
        r['SECTOR']   = _sv(r.get('SECTOR1'))

        undrawn = _nv(r.get('UNDRAWN'))
        if undrawn in (None,) or (isinstance(undrawn, float) and math.isnan(undrawn)):
            undrawn = 0
        apprlimt = _nv(r.get('APPRLIMT'))
        balance  = _nv(r.get('BALANCE'))
        if balance < 0:
            undrawn = apprlimt
        else:
            undrawn = apprlimt + (-1) * balance
        if balance > apprlimt:
            undrawn = 0
        undrawn1 = round(undrawn, 2)
        r['UNDRAWN']  = undrawn1 * 100

        if product in (124, 165, 191):
            r['FACILITY'] = 34111
        else:
            r['FACILITY'] = 34110
        if product == 73:
            r['FACCODE'] = 34110
        if apprlimt <= 1:
            r['FACILITY'] = 34112
        if product in (177, 178):
            r['FACILITY'] = 34220
            if apprlimt in (0, None):
                r['FACILITY'] = 34112
        if r['FACILITY'] == 34112:
            r['FACCODE'] = 34120

        if product == 114:
            r['SPECIALF'] = '12'
            r['FCONCEPT'] = 60
            r['NOTETERM'] = 12
            r['PAYFREQC'] = '20'
        elif product in (160, 161, 162, 163, 164, 165, 166, 167):
            r['SPECIALF'] = '00'
            r['FCONCEPT'] = 10
            r['NOTETERM'] = 12
            r['PAYFREQC'] = '20'
        else:
            r['SPECIALF'] = '00'
            r['FCONCEPT'] = 51
            r['NOTETERM'] = 12
            r['PAYFREQC'] = '20'

        if loantype in (133, 134, 177, 178):
            r['FCONCEPT'] = int(_sv(format_odfconcept(loantype)) or 0)
            r['FACILITY'] = 34220
            r['FACCODE']  = 34220

        if _nv(r.get('FEEAMT')) in (None,):   r['FEEAMT']   = 0
        if _nv(r.get('INTERDUE')) in (None,):  r['INTERDUE'] = 0

        # CENSUS
        censust = _nv(r.get('CENSUST'))
        censust_s = _sv(r.get('CENSUST')).strip()
        if len(censust_s) == 5:
            r['CENSUS'] = censust * 0.01
        else:
            r['CENSUS'] = censust

        # Expiry/OD date
        exoddate = _nv(r.get('EXODDATE'))
        tempoddt = _nv(r.get('TEMPODDT'))
        curbal   = _nv(r.get('CURBAL'))
        if (exoddate != 0 or tempoddt != 0) and curbal <= 0:
            exoddt = _mmddyy_from_z11(exoddate) if exoddate != 0 else None
            tempdt = _mmddyy_from_z11(tempoddt)  if tempoddt != 0 else None
            exoddt_n = _sas_date_num(exoddt)
            tempdt_n = _sas_date_num(tempdt)
            if exoddt_n not in (None, 0) and tempdt_n not in (None, 0):
                oddate = exoddt if exoddt_n < tempdt_n else tempdt
            elif exoddate == 0:
                oddate = tempdt
            else:
                oddate = exoddt
            if oddate:
                oddays = _sas_date_num(oddate)
                r['PAYDD'] = oddate.day
                r['PAYMM'] = oddate.month
                r['PAYYR'] = oddate.year
                nodays = SDATE_NUM - oddays + 1
                r['NODAYS'] = nodays
                if nodays > 0:
                    r['ARREARS'] = math.floor(nodays / 30.00050)  # 22-5723
                    r['INSTALM'] = math.ceil(nodays  / 30.00050)

        rrmaindt = _nv(r.get('RRMAINDT'))
        if rrmaindt > 0:
            dd, mm, yy = _z11_parts(rrmaindt)
            r['DLVRDD'] = dd; r['DLVRMM'] = mm; r['DLVRYR'] = yy

        r['RRPAYCNT'] = _nv(r.get('RRCOUNT'))
        rrind = _sv(r.get('RRIND'))
        if (len(rrind) >= 2 and
            rrind[0] in 'CTRS' and
            rrind[1] in '123456789'):
            r['RRTAG']    = rrind
            r['RESTRUCT'] = rrind[0]
            rrcompldt = _nv(r.get('RRCOMPLDT'))
            if rrcompldt > 0:
                dd, mm, yy = _z11_parts(rrcompldt)
                r['RRCOMPDD'] = dd; r['RRCOMPMM'] = mm; r['RRCOMPYR'] = yy
                r['RESTRUCT'] = ''

        riskcode = _sv(r.get('RISKCODE'))
        odstatus = _sv(r.get('ODSTATUS'))
        if riskcode == '1':   r['CLASSIFI'] = 'C'
        elif riskcode == '2': r['CLASSIFI'] = 'S'
        elif riskcode == '3': r['CLASSIFI'] = 'D'
        elif riskcode == '4': r['CLASSIFI'] = 'B'
        elif odstatus == 'AC' or (exoddate == 0 and tempoddt == 0):
            r['CLASSIFI'] = 'P'

        if _nv(r.get('RISKCODE'), 0) > 0 or loantype in range(30, 35):
            r['IMPAIRED'] = 'Y'
        else:
            r['IMPAIRED'] = 'N'

        # LIABCODE from COL1..COL5
        liabcode = ' '
        for colfld in ['COL1','COL2','COL3','COL4','COL5']:
            colval = _sv(r.get(colfld, ''))
            l1 = colval[0:2] if len(colval) >= 2 else ''
            l2 = colval[3:5] if len(colval) >= 5 else ''
            if l1.strip():
                liabcode = l1; break
            if l2.strip():
                liabcode = l2; break
        r['LIABCODE'] = liabcode

        issuedt_v = _nv(r.get('ISSUEDT'))
        dd_i, mm_i, yy_i = _z11_parts(issuedt_v) if issuedt_v > 0 else ('00','00','0000')
        r['ISSUEDD'] = dd_i; r['ISSUEMM'] = mm_i; r['ISSUEYY'] = yy_i

        r['ACCTSTAT'] = 'O'
        r['CURBAL']   = _nv(r.get('CURBAL')) * -1
        curbalori = _nv(r.get('CURBAL'))
        r['CURBALORI'] = curbalori

        odintacc = _nv(r.get('ODINTACC'))
        outstand  = _nv(r.get('CURBAL'))
        interdue  = _nv(r.get('INTERDUE'))
        feeamt    = _nv(r.get('FEEAMT'))
        if odintacc > 0:
            outstand = _nv(r.get('CURBAL')) + odintacc
            interdue = interdue + odintacc
        r['OUTSTAND']  = outstand
        r['INTERDUE']  = interdue

        if round(outstand, 2) <= 0:
            r['INTERDUE'] = 0
            r['FEEAMT']   = 0
        r['CURBAL'] = _nv(r.get('OUTSTAND')) - _nv(r.get('INTERDUE')) - _nv(r.get('FEEAMT'))

        # *** BNM TAXONOMY & CCRIS ENHANCEMENT (2012-0752) ***
        r['SYNDICAT'] = 'N'
        r['SPECIALF'] = _sv(format_odfundsch(product))
        r['FCONCEPT'] = int(_sv(format_odfconcept(product)) or 0)

        if product in range(30, 35):
            oriproduct = _nv(r.get('ORIPRODUCT'))
            r['FCONCEPT'] = int(_sv(format_odfconcept(int(oriproduct))) or 0)
            r['ORICODE']  = int(oriproduct)
            if r['ORICODE'] == 0 and product in (32, 33):
                r['FCONCEPT'] = 10
            if product == 34:
                r['FACILITY'] = 34220
                r['FACCODE']  = 34220
            # P-I-O for inactive product
            wdb = _nv(r.get('WRITE_DOWN_BAL'))
            if wdb not in (0,) and not (isinstance(wdb, float) and math.isnan(wdb)):
                r['CURBAL']   = wdb
                r['OUTSTAND'] = wdb
                r['FEEAMT']   = 0
                r['INTERDUE'] = 0
                r['TOTIIS']   = 0
            wostat = _sv(r.get('WOSTAT'))
            wdb2 = _nv(r.get('WRITE_DOWN_BAL'))
            if wostat == 'W' and wdb2 <= 0:   r['ACCTSTAT'] = wostat
            elif wostat == 'W' and wdb2 > 0:  r['ACCTSTAT'] = 'P'
            elif wostat == 'P':               r['ACCTSTAT'] = wostat
            elif wostat == 'C':               r['ACCTSTAT'] = 'X'
        else:
            if _sv(r.get('WOSTAT')) == 'R':
                r['ACCTSTAT'] = 'O'

        r['OUTSTAND'] = _nv(r.get('OUTSTAND')) * 100

        lmtid    = _nv(r.get('LMTID'))
        odbasert = _nv(r.get('ODBASERT'))
        odtempadj= _nv(r.get('ODTEMPADJ'))
        lmtrate  = _nv(r.get('LMTRATE'))
        if lmtid < 1:
            r['INTRATE'] = (odbasert + odtempadj) * 100
        else:
            r['INTRATE'] = lmtrate * 100

        lmtindex = _nv(r.get('LMTINDEX'))
        lmtbaser = _nv(r.get('LMTBASER'))
        fdrcno   = _nv(r.get('FDRCNO'))
        lmtamt   = _nv(r.get('LMTAMT'))
        odplan   = _nv(r.get('ODPLAN'))
        if lmtindex in (18, 19):               r['TYPEPRC'] = '59'
        elif lmtindex in (1, 30) and lmtrate >= lmtbaser: r['TYPEPRC'] = '41'
        elif lmtindex in (1, 30) and lmtrate < lmtbaser:  r['TYPEPRC'] = '42'
        elif lmtindex in (38, 39):             r['TYPEPRC'] = '43'
        elif lmtindex in (0,) and fdrcno > 0:  r['TYPEPRC'] = '68'
        elif lmtindex in (0,) and lmtamt in (0, None):
            if odplan in (110, 111, 114):      r['TYPEPRC'] = '43'
            elif odplan in (100, 101, 103, 104, 105, 107): r['TYPEPRC'] = '41'
            elif odplan in (106, 998):         r['TYPEPRC'] = '00'
            elif odplan in (120, 121):         r['TYPEPRC'] = '44'
            else:                              r['TYPEPRC'] = '79'
        elif lmtindex in (0,) and lmtamt > 0:  r['TYPEPRC'] = '59'
        elif lmtindex == 52:                   r['TYPEPRC'] = '44'
        else:                                  r['TYPEPRC'] = '79'

        # Closed/settled OD logic
        prevobal   = _nv(r.get('PREVOBAL'))
        prevalimt  = _nv(r.get('PREVALIMT'))
        closedt    = _nv(r.get('CLOSEDT'))
        openind    = _sv(r.get('OPENIND'))
        outstand_v = _nv(r.get('OUTSTAND'))
        riskcode_v = _sv(r.get('RISKCODE'))
        if ((apprlimt in (None, 0) and round(outstand_v, 1) <= 0 and round(prevobal, 1) > 0) or
            (apprlimt in (None, 0) and round(outstand_v, 1) <= 0 and prevalimt > 0) or
            (closedt not in (None, 0) and openind in ('B', 'C', 'P'))):
            if not (riskcode_v in ('1','2','3','4') or product in range(30, 35)) or \
               openind in ('B','C','P'):
                r['ACCTSTAT'] = 'S'
                r['ARREARS']  = 0
                r['INSTALM']  = 0
                r['NODAYS']   = 0

    # ------------------------------------------------------------------
    # LOAN BRANCH
    # ------------------------------------------------------------------
    else:
        r['COLLVAL']  = _nv(r.get('APPVALUE')) * 100
        r['OUTSTAND'] = _nv(r.get('BALANCE'))  * 100
        r['CAVAIAMT'] = _nv(r.get('CAVAIAMT')) * 100
        r['REPAID']   = _nv(r.get('REPAID'))   * 100
        r['DISBURSE'] = _nv(r.get('DISBURSE')) * 100
        r['LMTAMT']   = 0
        r['ODXSAMT']  = 0
        r['BILTOT']   = _nv(r.get('BILTOT'))   * 100
        limtcurr      = _nv(r.get('APPRLIMT'))
        if limtcurr in (None, 0):
            limtcurr  = _nv(r.get('NETPROC'))
        r['LIMTCURR'] = limtcurr
        r['INTRATE']  = _nv(r.get('INTRATE'))  * 100

        issuedt_v = _nv(r.get('ISSUEDT'))
        dd_i, mm_i, yy_i = _z11_parts(issuedt_v) if issuedt_v > 0 else ('00','00','0000')
        r['ISSUEDD'] = dd_i; r['ISSUEMM'] = mm_i; r['ISSUEYY'] = yy_i

        if ((acctno > 8000000000 and
             loantype in {122,106,128,130,120,121,700,705,709,710}) or
            (acctno < 2999999999 and
             loantype in {100,101,102,103,110,111,112,113,114,115,116,117,
                          118,120,121,122,123,124,125,127,135,136,170,180,
                          106,128,130,700,705,709,710,
                          194,195,380,381})):
            r['LIMTCURR'] = _nv(r.get('NETPROC'))
            if (10010 <= noteno <= 10099) or (30010 <= noteno <= 30099):
                if _sv(r.get('IND')) == 'B':
                    r['LIMTCURR'] = _nv(r.get('CORGAMT')) - _nv(r.get('INTAMT'))

        commno = _nv(r.get('COMMNO'))
        if not (2500000000 <= acctno <= 2599999999) and \
           not (800 <= loantype <= 899):
            if commno > 0:
                # FIRST.ACCTNO / FIRST.COMMNO logic handled per-group
                # We keep full LIMTCURR for now; summary step adjusts later
                pass  # handled in PROC SUMMARY equivalent below

        r['CAGAMAS']  = 0
        r['SYNDICAT'] = 'N'
        r['SPECIALF'] = '00'
        r['FCONCEPT'] = 99
        r['RESTRUCT'] = ' '

        prodcd = _sv(format_lnprod(loantype))
        r['PRODCD'] = prodcd

        # Inactive product
        if prodcd == '34190':
            r['FACILITY'] = 34210
            r['LIMTCURR'] = _nv(r.get('APPRLIM2'))

        # SPECIALF assignments
        if loantype in (124, 145):             r['SPECIALF'] = '00'
        if loantype == 566:                    r['SPECIALF'] = '10'
        if loantype in (991, 994) and _nv(r.get('PZIPCODE')) in (566,): r['SPECIALF'] = '10'
        if loantype in (170, 564, 565):        r['SPECIALF'] = '11'
        if loantype in (991, 994) and _nv(r.get('PZIPCODE')) in (170,564,565): r['SPECIALF'] = '11'
        if loantype in (559, 560, 567):        r['SPECIALF'] = '12'
        if loantype in (991, 994) and _nv(r.get('PZIPCODE')) in (559,560,567): r['SPECIALF'] = '12'
        if loantype in (568, 570, 532, 533, 573, 574): r['SPECIALF'] = '14'
        if loantype in (991, 994) and _nv(r.get('PZIPCODE')) in (568,570,573): r['SPECIALF'] = '14'
        if loantype == 561:                    r['SPECIALF'] = '16'
        if loantype in (991, 994) and _nv(r.get('PZIPCODE')) in (561,): r['SPECIALF'] = '16'
        if loantype in (555, 556):             r['SPECIALF'] = '18'
        if loantype in (991, 994) and _nv(r.get('PZIPCODE')) in (555,556): r['SPECIALF'] = '18'
        if loantype in (521, 522, 523, 528):   r['SPECIALF'] = '99'
        if loantype in (991, 994) and _nv(r.get('PZIPCODE')) in (521,522,523,528): r['SPECIALF'] = '99'

        # FCONCEPT assignments
        pzipcode = _nv(r.get('PZIPCODE'))
        if loantype in (124, 145, 461):         r['FCONCEPT'] = 10
        if loantype in PRDF:                    r['FCONCEPT'] = 10
        if loantype in (981,982,984,985) and pzipcode in PRDF: r['FCONCEPT'] = 10
        if loantype == 180:                     r['FCONCEPT'] = 13
        if loantype in (981,982,984,985) and pzipcode == 180: r['FCONCEPT'] = 13
        if loantype in (128,130,131,132,983):   r['FCONCEPT'] = 16
        if loantype == 193:                     r['FCONCEPT'] = 18
        if loantype in (135,136,138,182):       r['FCONCEPT'] = 26
        if loantype in (982,985) and pzipcode in (135,136): r['FCONCEPT'] = 26
        if loantype in PRDG:                    r['FCONCEPT'] = 51
        if loantype in (981,982,994,995) and pzipcode in PRDG: r['FCONCEPT'] = 51
        if loantype in (910, 925):              r['FCONCEPT'] = 52
        if loantype in (992, 995) and pzipcode in (910,925): r['FCONCEPT'] = 52
        if loantype in (4,5,6,7,15,20,25,26,27,28,29,30,31,32,33,34,60,61,
                        62,63,70,71,72,73,74,75,76,77,78,360,380,381,
                        390,700,705,993,996):    r['FCONCEPT'] = 59
        if loantype in (227,228,230,231,232,233,234,235,236,237,
                        532,533,573,574,248,
                        238,239,240,241,242,243,359,361,531,906,907): r['FCONCEPT'] = 60
        if loantype in PRDH:                    r['FCONCEPT'] = 99
        if loantype in (991,992,994,995) and pzipcode in PRDH: r['FCONCEPT'] = 99
        if loantype in (800,801,804,851,852,853,854,855,
                        802,803,856,857,858,859,860): r['FCONCEPT'] = 52

        if loantype in (180,914,915,919,920,925,950,951): r['SYNDICAT'] = 'Y'

        # INTERDUE
        ntint   = _sv(r.get('NTINT'))
        balance = _nv(r.get('BALANCE'))
        curbal  = _nv(r.get('CURBAL'))
        feeamt  = _nv(r.get('FEEAMT'))
        rebate  = _nv(r.get('REBATE'))
        intearn4= _nv(r.get('INTEARN4'))
        intearn = _nv(r.get('INTEARN'))
        intamt  = _nv(r.get('INTAMT'))

        if ntint != 'A':
            r['INTERDUE'] = balance - curbal - feeamt
        else:
            if loantype in (380,381,700,705,709,710,752,760):
                r['CURBAL']   = curbal - rebate - intearn4
                r['INTERDUE'] = 0
            else:
                r['CURBAL']   = curbal + intearn - intamt
                r['INTERDUE'] = 0
                r['CURBALORI'] = r['CURBAL']
                if r['CURBAL'] < 0:
                    r['CURBAL'] = 0

        r['INSTALM'] = _nv(r.get('BILLCNT'))

        facility_v = _nv(r.get('FACILITY'))
        if facility_v in (34331, 34332):
            r['UNDRAWN'] = 0

        r['LNTY']    = 0
        r['ACCTSTAT'] = 'O'

        paidind  = _sv(r.get('PAIDIND'))
        if paidind == 'P':
            r['OUTSTAND'] = 0; r['CURBAL'] = 0
            r['INTERDUE'] = 0; r['FEEAMT'] = 0

        if loantype in HP and paidind == 'P':
            r['ACCTSTAT'] = 'S'; r['ARREARS'] = 0
            r['INSTALM']  = 0;   r['NODAYS']  = 0
            r['OUTSTAND'] = 0;   r['UNDRAWN'] = 0
        elif 600 <= loantype <= 699 and paidind == 'P' and loantype not in HP:
            lsttrncd = _nv(r.get('LSTTRNCD'))
            wdb = _nv(r.get('WRITE_DOWN_BAL'))
            if wdb in (0, None) and lsttrncd in (658, 662, 663):
                r['ACCTSTAT'] = 'W'; r['IMPAIRED'] = 'Y'
            elif wdb in (0, None) and lsttrncd not in (658, 662, 663):
                r['ACCTSTAT'] = 'S'; r['IMPAIRED'] = 'N'
                r['ARREARS'] = 0; r['INSTALM'] = 0
                r['NODAYS'] = 0;  r['UNDRAWN'] = 0
        elif 600 <= loantype <= 699 and paidind != 'P' and loantype not in HP:
            wdb = _nv(r.get('WRITE_DOWN_BAL'))
            if wdb <= 0:
                r['ACCTSTAT'] = 'W'; r['IMPAIRED'] = 'Y'
            elif wdb > 0:
                r['ACCTSTAT'] = 'P'; r['IMPAIRED'] = 'Y'
        elif loantype not in range(600, 700) and paidind == 'P' and \
             _nv(r.get('OUTSTAND')) in (0, None) and loantype not in HP:
            lsttrncd = _nv(r.get('LSTTRNCD'))
            if lsttrncd in (662, 663, 658):
                r['ACCTSTAT'] = 'W'; r['IMPAIRED'] = 'Y'
            else:
                r['ACCTSTAT'] = 'S'; r['IMPAIRED'] = 'N'
                r['ARREARS'] = 0; r['INSTALM'] = 0
                r['NODAYS'] = 0;  r['UNDRAWN'] = 0

        if loantype in (128,130,700,705,380,381,709,710,131,132,720,725):
            r['LNTY'] = 1
        borstat = _sv(r.get('BORSTAT'))
        if r['LNTY'] == 1 and (noteno >= 98000 or borstat == 'S'):
            r['ACCTSTAT'] = 'T'
        if r['LNTY'] == 0 and borstat == 'S':
            r['ACCTSTAT'] = 'T'

        if loantype in (950, 951, 915, 953):  r['SYNDICAT'] = 'Y'
        if loantype == 804:                    r['SYNDICAT'] = 'Y'

        if loantype in (248, 532, 533, 573, 574):
            r['NOTETERM'] = _nv(r.get('EARNTERM'))
        if loantype in (330, 302, 506, 902, 903, 951, 953) and \
           _sv(r.get('CPRODUCT')) == '170':
            r['NOTETERM'] = 12

        if loantype == 521:                                            r['PAYFREQC'] = '13'
        if loantype in (124,145,532,533,573,574,248):                  r['PAYFREQC'] = '14'
        if loantype in (5,6,25,15,20,111,139,140,122,106,128,130,
                        120,121,201,204,205,225,300,301,307,320,330,
                        504,505,506,511,568,555,556,559,567,564,565,
                        570,566,700,705,709,710,900,901,910,912,
                        930,126,127,359,981,982,983,991,993):          r['PAYFREQC'] = '14'
        if loantype == 992 and pzipcode in (300,304,305,307,310,330,504,505,506,511,515,
                                            359,555,556,559,560,564,565,570,571,574,575,
                                            900,901,902,910,912,930):  r['PAYFREQC'] = '14'
        if loantype in (117,113,118,115,119,116,227,228,230,231,234): r['PAYFREQC'] = '14'
        if loantype in (235,236,237,238,239,240,241,242):             r['PAYFREQC'] = '14'
        if loantype == 330 and _nv(r.get('CPRODUCT')) in (None, 0):   r['PAYFREQC'] = '16'
        if loantype in (302,506,902,903,951,953,330) and \
           _sv(r.get('CPRODUCT')) == '170':                            r['PAYFREQC'] = '19'
        if loantype in (569, 904, 932, 933, 950, 915):                r['PAYFREQC'] = '21'

        # RESTRUCT
        modeldes  = _sv(r.get('MODELDES'))
        cpnstdte  = _nv(r.get('CPNSTDTE'))
        dlivrydt  = _nv(r.get('DLIVRYDT'))
        assmdate  = _nv(r.get('ASSMDATE'))
        rrcycle   = _sv(r.get('RRCYCLE'))
        fclosuredt= _nv(r.get('FCLOSUREDT'))
        cyc       = 0

        if loantype in (128,130,131,132,720,725,380,381,700,705,709,710,983,993,996):
            if noteno >= 98000:   r['RESTRUCT'] = 'C'
            if borstat == 'S':    r['RESTRUCT'] = 'T'
            if modeldes[:4] == 'RARM' or modeldes[:3] == 'RAM':
                r['RESTRUCT'] = 'Y'
            if cpnstdte > 0:
                dd, mm, yy = _z11_parts(cpnstdte)
                r['RRCOMPDD'] = dd; r['RRCOMPMM'] = mm; r['RRCOMPYR'] = yy
            if rrcycle[:2] == 'HP':
                try:    cyc = int(rrcycle[2:3])
                except: cyc = 0
            if cyc <= 0 and fclosuredt > 0:
                cyc = 6
        else:
            if modeldes in ('C','T','Y'):    r['RESTRUCT'] = modeldes
            elif borstat == 'S':             r['RESTRUCT'] = 'T'
            elif borstat == 'Q':             r['RESTRUCT'] = 'C'

        r['CYC'] = cyc

        if loantype in HP:
            if borstat == 'W':   r['ACCTSTAT'] = 'P'
            elif borstat == 'X': r['ACCTSTAT'] = 'W'

        # CAGAMAS
        pzipcode = int(_nv(r.get('PZIPCODE')))
        cagamas  = 0
        if pzipcode in (149071, 149073):  cagamas = 5112014
        elif pzipcode == 179100:          cagamas = 9032017
        elif pzipcode == 179118:          cagamas = 13122017
        elif pzipcode == 179107:          cagamas = 6072017
        elif pzipcode == 179116:          cagamas = 3112017
        elif pzipcode == 228014:          cagamas = 6072022
        elif pzipcode == 228036:          cagamas = 8112022
        elif pzipcode == 228042:          cagamas = 13122022
        elif pzipcode == 248039:          cagamas = 29112024
        r['CAGAMAS'] = cagamas
        if cagamas > 0: r['RECOURSE'] = 'Y'

        if pzipcode == 149071:  r['MASMATDT'] = '03112017'
        elif pzipcode == 149073:r['MASMATDT'] = '08052018'
        elif pzipcode == 179100:r['MASMATDT'] = '09032022'
        elif pzipcode == 179118:r['MASMATDT'] = '13122022'
        elif pzipcode == 179107:r['MASMATDT'] = '06072022'
        elif pzipcode == 179116:r['MASMATDT'] = '03112020'
        elif pzipcode == 228014:r['MASMATDT'] = '06072027'
        elif pzipcode == 228036:r['MASMATDT'] = '08112027'
        elif pzipcode == 228042:r['MASMATDT'] = '13122027'
        elif pzipcode == 248039:r['MASMATDT'] = '29112029'

        bldate = _nv(r.get('BLDATE'))
        if bldate > 0:
            nodays = SDATE_NUM - int(bldate)
            r['NODAYS'] = max(0, nodays)
        if _nv(r.get('NODAYS')) < 0:
            r['NODAYS'] = 0

        oldnotedayarr = _nv(r.get('OLDNOTEDAYARR'))
        if oldnotedayarr > 0 and 98000 <= noteno <= 98999:
            nd = _nv(r.get('NODAYS'))
            if nd < 0: nd = 0
            r['NODAYS']  = nd + oldnotedayarr
            r['XNODAYS'] = oldnotedayarr

        mordayarr = r.get('MORDAYARR')
        if mordayarr is not None and not (isinstance(mordayarr, float) and math.isnan(mordayarr)):
            r['NODAYS'] = mordayarr

        if bldate > 0:
            bd = _sas_date(bldate)
            if bd:
                r['PAYDD'] = bd.day; r['PAYMM'] = bd.month; r['PAYYR'] = bd.year

        dlvdate = _nv(r.get('DLVDATE'))
        if dlvdate > 0:
            dd_d = _sas_date(dlvdate)
            if dd_d:
                r['GRANTDD'] = dd_d.day; r['GRANTMM'] = dd_d.month; r['GRANTYR'] = dd_d.year

        exprdate = _nv(r.get('EXPRDATE'))
        if exprdate > 0:
            ed = _sas_date(exprdate)
            if ed:
                r['LNMATDD'] = ed.day; r['LNMATMM'] = ed.month; r['LNMATYR'] = ed.year

        maturedt = _nv(r.get('MATUREDT'))
        if maturedt > 0:
            dd, mm, yy = _z11_parts(maturedt)
            r['ORMATDD'] = dd; r['ORMATMM'] = mm; r['ORMATYR'] = yy

        dlivrydt_v = _nv(r.get('DLIVRYDT'))
        if dlivrydt_v > 0:
            dd, mm, yy = _z11_parts(dlivrydt_v)
            r['DLVRDD'] = dd; r['DLVRMM'] = mm; r['DLVRYR'] = yy

        # MORSTDTE / MORENDTE
        firsildte = _nv(r.get('FIRSILDTE'))
        r['MORSTDTE'] = '00000000'
        if firsildte > 0:
            d = _mmddyy_from_z11(firsildte)
            r['MORSTDTE'] = _yymmddn8(d)

        currildte = _nv(r.get('CURRILDTE'))
        r['MORENDTE'] = '00000000'
        if currildte > 0:
            d = _mmddyy_from_z11(currildte)
            r['MORENDTE'] = _yymmddn8(d)

        r['RRPAYCNT'] = _nv(r.get('NUMCPNS'))

        # Restructure from MODELDES pattern
        md = _sv(r.get('MODELDES'))
        if (len(md) >= 6 and
            md[0] in 'CTRSWM' and
            md[1:4].isdigit() and
            md[4] in 'UD' and
            md[5] in ('K', ' ')):
            r['RRTAG']    = md[:6]
            r['RESTRUCT'] = md[0]
            lsttrncd = _nv(r.get('LSTTRNCD'))
            if loantype not in range(600, 700) and paidind == 'P' and \
               lsttrncd not in (662, 663, 658):
                r['ACCTSTAT'] = 'S'
            elif loantype not in range(600, 700):
                if r.get('RESTRUCT') in ('C','R','W'): r['ACCTSTAT'] = 'C'
                else:                                   r['ACCTSTAT'] = 'T'

            dlvdate_n = _nv(r.get('DLVDATE'))
            if (md[4] in ('U','D') and CDATE_NUM < dlvdate_n) or \
               (fclosuredt > 0 and assmdate > 0):
                r['RESTRUCT'] = ' '
                if r.get('ACCTSTAT') not in ('S','W'):
                    r['ACCTSTAT'] = 'O'
            if assmdate > 0:
                dd, mm, yy = _z11_parts(assmdate)
                r['RRCOMPDD'] = dd; r['RRCOMPMM'] = mm; r['RRCOMPYR'] = yy

            cyc = int(md[2:4]) if md[2:4].isdigit() else 0
            r['CYC'] = cyc
            if r.get('RESTRUCT') == 'W': r['RESTRUCT'] = 'R'
            elif r.get('RESTRUCT') == 'M': r['RESTRUCT'] = 'S'

        # FCLOSUREDT
        if fclosuredt > 0:
            rrcountdte = _mmddyy_from_z11(fclosuredt)
            if rrcountdte:
                r['RRCNTDD'] = rrcountdte.day
                r['RRCNTMM'] = rrcountdte.month
                r['RRCNTYR'] = rrcountdte.year
                cyc = _nv(r.get('CYC'))
                if cyc > 0:
                    rrenddt = _intnx_month_end(rrcountdte, cyc - 1)
                    rrenddt_n = _sas_date_num(rrenddt)
                    if CDATE_NUM >= rrenddt_n and (assmdate > 0 or cpnstdte > 0):
                        lnty = r.get('LNTY', 0)
                        if lnty == 1:
                            user5 = _sv(r.get('USER5'))
                            issxdte_v = _nv(r.get('ISSXDTE'))
                            apr2015 = _sas_date_num(date(2015, 4, 6))
                            if user5 != 'N' and issxdte_v >= apr2015:
                                r['RESTRUCT'] = ' '
                        else:
                            r['RESTRUCT'] = ' '
                        if r.get('ACCTSTAT') not in ('S','W'):
                            r['ACCTSTAT'] = 'O'
                        r['COMPLYIND'] = 'Y'

        nxbildt = _nv(r.get('NXBILDT'))
        if nxbildt > 0:
            dd, mm, yy = _z11_parts(nxbildt)
            r['BILDUEDD'] = dd; r['BILDUEMM'] = mm; r['BILDUEYR'] = yy

        # COLLYR / COLLMM
        collyear = _nv(r.get('COLLYEAR'))
        cy_str   = f"{int(collyear):05d}"
        if loantype in (128,130,131,132,380,381,700,705,750,720,725,752,760,
                        4,5,6,7,15,20,25,26,27,28,29,30,31,32,33,34,60,61,
                        62,63,70,71,72,73,74,75,76,77,78,100,101,108,
                        199,500,520,983,993,996) or _sv(r.get('ORGTYPE')) == 'S':
            r['COLLYR'] = cy_str[3:5]
        else:
            r['COLLMM'] = cy_str[1:3]
            r['COLLYR'] = cy_str[3:5]

        nodays = _nv(r.get('NODAYS'))
        r['ARREARS'] = math.floor(nodays / 30.00050)  # 22-5723

        mo_instl_arr = r.get('MO_INSTL_ARR')
        if mo_instl_arr is not None and not (isinstance(mo_instl_arr, float) and math.isnan(mo_instl_arr)):
            r['BILLCNT'] = mo_instl_arr
        r['INSTALM'] = _nv(r.get('BILLCNT'))

        bldate_dt = _sas_date(bldate) if bldate > 0 else None
        billcnt_v = _nv(r.get('BILLCNT'))
        if bldate_dt and (bldate_dt + timedelta(days=1)).day == 1 and billcnt_v > 0:
            lmo_tag = _sv(r.get('LMO_TAG'))
            mostdte  = _nv(r.get('MOSTDTE'))
            moenddte = _nv(r.get('MOENDDTE'))
            mdate_n  = int(MDATE)
            if loantype not in HP:
                if lmo_tag[0] not in ('R','F','V','A','B') or \
                   (lmo_tag[0] in ('R','F','V','A','B') and
                    not (mostdte <= mdate_n <= moenddte)):
                    r['INSTALM'] = billcnt_v - 1
            else:
                valid_models = ('A1AM','A1BM','A1CM','A1DM','A1EM','A1FM')
                if (modeldes not in valid_models and modeldes[:3] != 'RAM') or \
                   ((modeldes in valid_models or modeldes[:3] == 'RAM') and
                    not (mostdte <= mdate_n <= moenddte)):
                    r['INSTALM'] = billcnt_v - 1

        # CRRINI / CRRNOW
        r['CRRINI'] = (_sv(r.get('SCORE1')) + _sv(r.get('MORTGIND'))).replace(' ', '')
        r['CRRNOW'] = (_sv(r.get('SCORE2')) + _sv(r.get('CONTRTYPE'))).replace(' ', '')

        undrawn = _nv(r.get('UNDRAWN'))
        if undrawn in (None,) or (isinstance(undrawn, float) and math.isnan(undrawn)):
            undrawn = 0
        balance = _nv(r.get('BALANCE'))
        if balance < 0:
            undrawn = _nv(r.get('APPRLIM2'))
        undrawn1 = round(undrawn, 2)
        r['UNDRAWN'] = undrawn1 * 100

        # CLASSIFI
        riskrate = _sv(r.get('RISKRATE'))
        nplind   = _sv(r.get('NPLIND'))
        if riskrate == ' ' or riskrate == '': r['CLASSIFI'] = 'P'
        elif riskrate == '1':                  r['CLASSIFI'] = 'C'
        elif riskrate == '2':                  r['CLASSIFI'] = 'S'
        elif riskrate == '3':                  r['CLASSIFI'] = 'D'
        elif riskrate == '4':                  r['CLASSIFI'] = 'B'
        if nplind == 'P':                      r['CLASSIFI'] = 'P'

        loanstat = _nv(r.get('LOANSTAT'))
        if loanstat == 3: r['IMPAIRED'] = 'Y'
        else:             r['IMPAIRED'] = 'N'

        faccode_v = _nv(r.get('FACCODE'))
        if faccode_v == 34371:  # STAFF LOAN
            nodays_v = _nv(r.get('NODAYS'))
            borstat_v = _sv(r.get('BORSTAT'))
            user5_v   = _sv(r.get('USER5'))
            if nodays_v >= 90 or borstat_v in ('F','I','R') or user5_v == 'N':
                r['IMPAIRED'] = 'Y'

        sector_v = _sv(r.get('SECTOR')).replace(' ', '')
        r['SECTOR']   = sector_v
        r['LIMTCURR'] = _nv(r.get('LIMTCURR')) * 100
        r['COLLREF']  = acctno

        branch = _nv(r.get('BRANCH'))
        ntbrch = _nv(r.get('NTBRCH'))
        accbrch= _nv(r.get('ACCBRCH'))
        if branch == 0: branch = ntbrch
        if branch == 0: branch = accbrch
        r['BRANCH'] = branch
        r['APCODE'] = loantype
        r['FICODE'] = branch
        r['REALISAB'] = _nv(r.get('REALISAB')) * 100

        payfreq = _sv(r.get('PAYFREQ'))
        if payfreq == '1':   r['PAYFREQC'] = '14'
        elif payfreq == '2': r['PAYFREQC'] = '15'
        elif payfreq == '3': r['PAYFREQC'] = '16'
        elif payfreq == '4': r['PAYFREQC'] = '17'
        elif payfreq == '5': r['PAYFREQC'] = '18'
        elif payfreq == '6': r['PAYFREQC'] = '13'
        elif payfreq == '9': r['PAYFREQC'] = '21'

        # 984/985/994/995 special
        if loantype in (984, 985, 994, 995) and noteno == 10010:
            r['OUTSTAND'] = 0; r['ACCTSTAT'] = 'S'
            r['ARREARS']  = 0; r['INSTALM']  = 0; r['UNDRAWN'] = 0

        if loantype in (981, 982, 983, 991, 992, 993, 678, 679):
            r['OUTSTAND'] = 0; r['BALANCE'] = 0; r['UNDRAWN'] = 0
            r['ACCTSTAT'] = 'W'; r['CURBAL'] = 0
            r['INTERDUE'] = 0;   r['FEEAMT'] = 0
            if loantype in (983, 993, 678, 679) and (paidind == 'P' or _sv(r.get('USER5')) == 'F'):
                r['ACCTSTAT'] = 'S'; r['ARREARS'] = 0; r['INSTALM'] = 0
                r['NODAYS']   = 0;   r['UNDRAWN'] = 0
                r['CLASSIFI'] = 'P'; r['IMPAIRED'] = 'N'

        if _sv(r.get('BORSTAT')) == 'F':
            r['INSTALM'] = 999
            if loantype in (983, 993, 678, 679):
                r['CLASSIFI'] = 'B'; r['IMPAIRED'] = 'Y'

        # *** BNM TAXONOMY & CCRIS ENHANCEMENT (2011-4096) ***
        r['SYNDICAT']    = _sv(format_synd(loantype))
        r['SPECIALF']    = _sv(format_fundsch(loantype))
        r['FCONCEPT_VAL']= int(_sv(format_fconcept(loantype)) or 0)

        # 2020-3587
        census_v = _nv(r.get('CENSUS'))
        if loantype in (301, 320, 510, 531, 568, 570, 575):
            r['SPECIALF'] = _sv(format_fschemec(census_v))

        if loantype in (412, 413, 414, 911, 415, 466):
            r['FACILITY'] = 34322; r['FACCODE'] = 34320
        if loantype in (912, 575, 416, 467, 461):
            r['FACILITY'] = 34391; r['FACCODE'] = 34391
        if loantype in (307, 464, 465):
            r['FACILITY'] = 34391; r['FACCODE'] = 34392
        if loantype in (103, 104):
            r['FACILITY'] = 34371; r['FACCODE'] = 34371
        if loantype in (191, 417):
            r['FACILITY'] = 34351; r['FACCODE'] = 34364

        if 600 <= loantype <= 699:
            if loantype in (678, 679):
                r['SYNDICAT'] = 'N'; r['SPECIALF'] = '00'; r['FCONCEPT_VAL'] = 99
            else:
                census_floor = math.floor(census_v)
                r['SYNDICAT']     = _sv(format_synd(census_floor))
                r['SPECIALF']     = _sv(format_fundsch(census_floor))
                r['FCONCEPT_VAL'] = int(_sv(format_fconcept(census_floor)) or 0)
                if census_floor in (301, 320, 510, 531, 568, 570, 575):
                    r['SPECIALF'] = _sv(format_fschemec(census_v))

            # P-I-O for inactive product (600-699)
            wdb = _nv(r.get('WRITE_DOWN_BAL'))
            if wdb not in (0,) and not (isinstance(wdb, float) and math.isnan(wdb)):
                outstand_v = _nv(r.get('OUTSTAND'))
                feeamt_v   = _nv(r.get('FEEAMT'))
                curbal_v   = _nv(r.get('CURBAL'))
                balance_v  = _nv(r.get('BALANCE'))
                if outstand_v > 0 and feeamt_v > 0:  # 23-4366
                    if balance_v <= feeamt_v:
                        feeamt_v = balance_v
                    if balance_v <= feeamt_v + curbal_v:
                        r['CURBAL']   = wdb - feeamt_v
                        r['INTERDUE'] = 0
                    else:
                        r['INTERDUE'] = wdb - curbal_v - feeamt_v
                    r['FEEAMT'] = feeamt_v
                else:
                    if balance_v > curbal_v:
                        r['INTERDUE'] = wdb - curbal_v
                    else:
                        r['CURBAL']   = wdb
                        r['INTERDUE'] = 0
                    r['FEEAMT'] = 0
            r['TOTIIS'] = 0
            freleas_v = _nv(r.get('FRELEAS'))
            if freleas_v > 0 or _sv(r.get('FLAG1')) == 'F':
                r['UNDRAWN'] = 0

        # TYPEPRC (loan branch)
        wofdt   = _sas_date_num(date(2017, 5, 31))  # '31MAY17'D
        ntindex = _nv(r.get('NTINDEX'))
        cfindex = _nv(r.get('CFINDEX'))
        spread  = _nv(r.get('SPREAD'))
        costfund= _nv(r.get('COSTFUND'))

        if loantype in (983, 993, 128, 130, 380, 381, 700, 705):
            r['TYPEPRC'] = '80'
        elif loantype in (678, 679, 698, 699, 131, 132, 720, 725):
            if ntindex in (1,30) and spread < 0:   r['TYPEPRC'] = '42'
            elif ntindex in (1,30) and spread >= 0: r['TYPEPRC'] = '41'
            elif ntindex in (38, 39):              r['TYPEPRC'] = '43'
            else:                                   r['TYPEPRC'] = '80'
        elif (1 <= loantype <= 99) or (100 <= loantype <= 109):
            if ntindex in (1,30) and spread < 0:   r['TYPEPRC'] = '42'
            elif ntindex in (1,30) and spread >= 0: r['TYPEPRC'] = '41'
            elif ntindex in (38, 39):              r['TYPEPRC'] = '43'
            else:                                   r['TYPEPRC'] = '59'
        elif cfindex == 997:                        r['TYPEPRC'] = '68'
        elif ntindex in (1,30) and spread < 0:     r['TYPEPRC'] = '42'
        elif ntindex in (1,30) and spread >= 0:    r['TYPEPRC'] = '41'
        elif ntindex in (38, 39):                  r['TYPEPRC'] = '43'
        elif ntindex == 52:                        r['TYPEPRC'] = '44'
        elif 900 <= cfindex <= 903:                r['TYPEPRC'] = '53'
        elif 904 <= cfindex <= 907:                r['TYPEPRC'] = '57'
        elif 908 <= cfindex <= 911:                r['TYPEPRC'] = '54'
        elif 912 <= cfindex <= 915:                r['TYPEPRC'] = '55'
        elif 916 <= cfindex <= 919:                r['TYPEPRC'] = '56'
        elif 920 <= cfindex <= 923:                r['TYPEPRC'] = '58'
        elif cfindex == 925:                       r['TYPEPRC'] = '79'
        elif cfindex == 926:                       r['TYPEPRC'] = '61'
        elif cfindex == 927:                       r['TYPEPRC'] = '62'
        elif cfindex == 928:                       r['TYPEPRC'] = '63'
        elif cfindex == 929:                       r['TYPEPRC'] = '64'
        elif cfindex == 930:                       r['TYPEPRC'] = '65'
        elif cfindex == 931:                       r['TYPEPRC'] = '66'
        elif cfindex == 932:                       r['TYPEPRC'] = '67'
        elif cfindex == 933:                       r['TYPEPRC'] = '51'
        elif cfindex == 924 or costfund > 0:       r['TYPEPRC'] = '68'
        else:                                      r['TYPEPRC'] = '59'

        # Additional 600-699 ACCTSTAT overrides
        if 600 <= loantype <= 699 and paidind != 'P':
            loanstat_v = _nv(r.get('LOANSTAT'))
            if loanstat_v == 3 and _sv(r.get('IMPAIRED')) == 'Y':
                wdb = _nv(r.get('WRITE_DOWN_BAL'))
                if wdb <= 0: r['ACCTSTAT'] = 'W'
                else:        r['ACCTSTAT'] = 'P'

        if 600 <= loantype <= 699 and paidind == 'P' and \
           _nv(r.get('LSTTRNCD')) in (662, 663, 658):
            r['ACCTSTAT'] = 'W'; r['IMPAIRED'] = 'Y'
        if 600 <= loantype <= 699 and paidind == 'P' and \
           _nv(r.get('LSTTRNCD')) not in (662, 663, 658):
            r['ACCTSTAT'] = 'S'; r['IMPAIRED'] = 'N'
        if loantype in (983, 993, 678, 679) and paidind != 'P':
            r['CLASSIFI'] = 'B'; r['IMPAIRED'] = 'Y'

        r['RMSBBA'] = 0

        # UTILISE
        rleasamt = _nv(r.get('RLEASAMT'))
        cusedamt = _nv(r.get('CUSEDAMT'))
        commno_v = _nv(r.get('COMMNO'))
        if rleasamt != 0:
            r['UTILISE'] = 'Y'
        else:
            if loantype in range(600, 700) or loantype in (983, 993):
                r['UTILISE'] = 'Y'
            elif _sv(r.get('IMPAIRED')) == 'Y':
                r['UTILISE'] = 'Y'
            elif commno_v > 0 and cusedamt > 0:
                r['UTILISE'] = 'Y'
            else:
                r['UTILISE'] = 'N'

    # END OF OD/LOAN BRANCHING
    # ------------------------------------------------------------------

    # Null-safe coercions
    for fld in ('TOTIIS','TOTIISR','TOTWOF','IISOPBAL','IISSUAMT','IISBWAMT',
                'SPOPBAL','SPWBAMT','SPWOF','SPDANAH','SPTRANS','LEDGBAL',
                'CUMRC','BDR','CUMSC','WOAMT','BDR_MTH','SC_MTH','RC_MTH',
                'NAI_MTH','CUMWOSP','CUMWOIS','CUMNAI'):
        if _nv(r.get(fld)) in (None,) or (isinstance(r.get(fld), float) and math.isnan(r.get(fld))):
            r[fld] = 0

    if _nv(r.get('UNDRAWN'))  < 0: r['UNDRAWN']  = 0

    # Scale monetary fields
    r['FXRATE']   = _nv(r.get('FORATE'))   * 100000
    r['CENSUS']   = _nv(r.get('CENSUS'))   * 100
    r['EIR_ADJ']  = _nv(r.get('EIR_ADJ'))  * 100
    r['CURBAL']   = _nv(r.get('CURBAL'))   * 100
    r['REBATE']   = _nv(r.get('REBATE'))   * 100
    r['INTERDUE'] = _nv(r.get('INTERDUE')) * 100
    r['FEEAMT']   = _nv(r.get('FEEAMT'))   * 100
    r['TOTIIS']   = _nv(r.get('TOTIIS'))   * 100
    r['TOTIISR']  = _nv(r.get('TOTIISR'))  * 100
    r['TOTWOF']   = _nv(r.get('TOTWOF'))   * 100
    r['IISOPBAL'] = _nv(r.get('IISOPBAL')) * 100
    r['IISSUAMT'] = _nv(r.get('IISSUAMT')) * 100
    r['IISBWAMT'] = _nv(r.get('IISBWAMT')) * 100
    r['SPWBAMT']  = _nv(r.get('SPWBAMT'))  * 100
    r['SPWOF']    = _nv(r.get('SPWOF'))    * 100
    r['SPOPBAL']  = _nv(r.get('SPOPBAL'))  * 100
    r['SPDANAH']  = _nv(r.get('SPDANAH'))  * 100
    r['SPTRANS']  = _nv(r.get('SPTRANS'))  * 100
    r['SPAAMT']   = _nv(r.get('SPAAMT'))   * 100
    r['IISDANAH'] = 0
    r['IISTRANS'] = 0
    r['SPCHARGE'] = 0
    r['LEDGBAL']  = _nv(r.get('LEDGBAL'))  * 100
    r['CUMRC']    = _nv(r.get('CUMRC'))    * 100
    r['BDR']      = _nv(r.get('BDR'))      * 100
    r['CUMSC']    = _nv(r.get('CUMSC'))    * 100
    r['WOAMT']    = _nv(r.get('WOAMT'))    * 100
    r['BDR_MTH']  = _nv(r.get('BDR_MTH'))  * 100
    r['SC_MTH']   = _nv(r.get('SC_MTH'))   * 100
    r['RC_MTH']   = _nv(r.get('RC_MTH'))   * 100
    r['NAI_MTH']  = _nv(r.get('NAI_MTH'))  * 100
    r['CUMWOSP']  = _nv(r.get('CUMWOSP'))  * 100
    r['CUMWOIS']  = _nv(r.get('CUMWOIS'))  * 100
    r['CUMNAI']   = _nv(r.get('CUMNAI'))   * 100
    r['NETPROC']  = _nv(r.get('NETPROC'))  * 100
    r['TOTAMT']   = _nv(r.get('TOTAMT'))   * 100

    if _nv(r.get('CCRIS_INSTLAMT')) in (None, 0) or \
       (isinstance(r.get('CCRIS_INSTLAMT'), float) and math.isnan(r['CCRIS_INSTLAMT'])):
        r['CCRIS_INSTLAMT'] = 0
    if _nv(r.get('DSR')) in (None, 0) or \
       (isinstance(r.get('DSR'), float) and math.isnan(r.get('DSR'))):
        r['DSR'] = 0
    if _nv(r.get('MTD_REPAID_AMT')) in (None, 0) or \
       (isinstance(r.get('MTD_REPAID_AMT'), float) and math.isnan(r.get('MTD_REPAID_AMT'))):
        r['MTD_REPAID_AMT'] = 0

    repay_source = _nv(r.get('REPAY_SOURCE'))
    if repay_source > 0:
        r['REPAYSRC'] = f"{int(repay_source):04d}"

    if 3000000000 <= acctno <= 3999999999:
        r['LEDGBAL'] = _nv(r.get('LEDGBAL')) * -1

    score = _sv(r.get('SCORE'))
    if not score or score[0] not in ALP:
        r['SCORE'] = '   '

    # ISSUED date
    issuemm_v = _nv(r.get('ISSUEMM'))
    issuedd_v = _nv(r.get('ISSUEDD'))
    issueyy_v = _nv(r.get('ISSUEYY'))
    try:
        issued = date(int(issueyy_v), int(issuemm_v), int(issuedd_v))
    except Exception:
        issued = None
    r['ISSUED'] = _sas_date_num(issued)

    # HP noteno >= 98000 RESTRUCT overrides
    if loantype in (128,130,380,381,700,705,720,725) and noteno >= 98000:
        sep05   = _sas_date_num(date(2005, 9, 30))
        dec07   = _sas_date_num(date(2007, 12, 19))
        aug08   = _sas_date_num(date(2008, 8, 31))
        sep08   = _sas_date_num(date(2008, 9, 1))
        oct05   = _sas_date_num(date(2005, 10, 1))
        dec07b  = _sas_date_num(date(2007, 12, 18))
        iss_n   = _nv(r.get('ISSUED'))
        if iss_n <= sep05 or (dec07 <= iss_n <= aug08):
            r['RESTRUCT'] = 'T'
        elif iss_n >= sep08 or (oct05 <= iss_n <= dec07b):
            r['RESTRUCT'] = 'C'
        if r.get('RESTRUCT') == 'C':   r['ACCTSTAT'] = 'C'
        elif r.get('RESTRUCT') == 'T': r['ACCTSTAT'] = 'T'
        if paidind == 'P':
            r['ACCTSTAT'] = 'S'; r['ARREARS'] = 0; r['INSTALM'] = 0
            r['NODAYS']   = 0;   r['OUTSTAND'] = 0; r['UNDRAWN'] = 0

    if loantype in (128,130,380,381,700,705,720,725,131,132):
        special_models = ('A1A','A1B','A1C','A1D','A1E','A1F',
                          'AK1A','AK1B','AK1C','AK1D','AK1E','AK1F')
        if noteno < 98000 and modeldes in special_models:
            r['RESTRUCT'] = 'C'; r['ACCTSTAT'] = 'C'; r['RRTAG'] = modeldes

    if 800 <= _nv(r.get('APCODE')) <= 899:
        if 2000000000 <= acctno <= 2999999999: r['FICODE'] = 904
        if 8000000000 <= acctno <= 8999999999: r['FICODE'] = 904

    if 800 <= loantype <= 899:
        r['COSTCTR'] = 904

    # RATAG / RADTDD/MM/YY
    facility_v = _nv(r.get('FACILITY'))
    issuedt_v  = _nv(r.get('ISSUEDT'))
    if facility_v in (34331, 34332):
        r['RATAG']  = _sv(r.get('AKPK_STATUS'))
        dd, mm, yy  = _z11_parts(issuedt_v) if issuedt_v > 0 else ('00','00','0000')
        r['RADTDD'] = dd; r['RADTMM'] = mm; r['RADTYY'] = yy
    else:
        nurs_tag    = _sv(r.get('NURS_TAG'))
        nurs_tagdt  = _nv(r.get('NURS_TAGDT'))
        tfa_tag     = _sv(r.get('TFA_NURS_TAG'))
        tfa_tagdt   = _nv(r.get('TFA_NURS_TAG_DT'))

        valid_nurs  = ('D','X','L','D1','D2','D3','D4','D5','D6','D7',
                       'D8','D9','X1','X2','X3','X4','X5','X6','X7',
                       'X8','X9','L1','L2','L3','L4','L5','L6','L7',
                       'L8','L9','O','P','R1','R2','R3','E','T','W',
                       'E1','E2','E3','T1','T2','T3')
        valid_tfa   = ('C1','C2','C3','C4','C5','C6','C7','C8','C9',
                       'G1','G2','G3','G4','G5','G6','G7','G8','G9',
                       'M1','M2','M3','M4','M5','M6','M7','M8','M9',
                       'N1','N2','N3','N4','N5','N6','N7','N8','N9',
                       'Q1','Q2','Q3','Q4','Q5','Q6','Q7','Q8','Q9',
                       'K1','K2','K3','K4','K5','K6','K7','K8','K9')

        def _set_ratag_from_nurs():
            nd = _sas_date(nurs_tagdt)
            r['RATAG']  = nurs_tag
            r['RADTYY'] = str(nd.year) if nd else '0000'
            r['RADTMM'] = f"{nd.month:02d}" if nd else '00'
            r['RADTDD'] = f"{nd.day:02d}" if nd else '00'

        def _set_ratag_from_tfa():
            td = _sas_date(tfa_tagdt)
            r['RATAG']  = tfa_tag
            r['RADTYY'] = str(td.year) if td else '0000'
            r['RADTMM'] = f"{td.month:02d}" if td else '00'
            r['RADTDD'] = f"{td.day:02d}" if td else '00'

        if nurs_tag in valid_nurs:
            _set_ratag_from_nurs()
            if tfa_tag in valid_tfa:
                if nurs_tagdt > tfa_tagdt:
                    _set_ratag_from_nurs()
                else:
                    _set_ratag_from_tfa()
        elif tfa_tag in valid_tfa:
            _set_ratag_from_tfa()
        else:
            r['RATAG'] = ' '

    # SMR 2022-3163
    r['RRSTRDD'] = 0; r['RRSTRMM'] = 0; r['RRSTRYR'] = 0
    r['RRCMPLDD']= 0; r['RRCMPLMM']= 0; r['RRCMPLYR']= 0

    if loantype in (380, 381, 700, 705, 720, 725):
        issuedt_v = _nv(r.get('ISSUEDT'))
        if issuedt_v > 0:
            dd, mm, yy = _z11_parts(issuedt_v)
            r['RRSTRDD'] = dd; r['RRSTRMM'] = mm; r['RRSTRYR'] = yy
        cpnstdte_v = _nv(r.get('CPNSTDTE'))
        if cpnstdte_v > 0:
            dd, mm, yy = _z11_parts(cpnstdte_v)
            r['RRCMPLDD'] = dd; r['RRCMPLMM'] = mm; r['RRCMPLYR'] = yy
    else:
        dlivrydt_v = _nv(r.get('DLIVRYDT'))
        assmdate_v = _nv(r.get('ASSMDATE'))
        if dlivrydt_v > 0:
            dd, mm, yy = _z11_parts(dlivrydt_v)
            r['RRSTRDD'] = dd; r['RRSTRMM'] = mm; r['RRSTRYR'] = yy
        if assmdate_v > 0:
            dd, mm, yy = _z11_parts(assmdate_v)
            r['RRCMPLDD'] = dd; r['RRCMPLMM'] = mm; r['RRCMPLYR'] = yy

    # ESMR 2025-1525 LU_SOURCE
    lu_source = _sv(r.get('LU_SOURCE'))
    if lu_source and lu_source[0] == 'A':
        r['LU_SOURCE'] = lu_source if lu_source in ('A1','A2') else 'A2'
    elif len(lu_source) >= 2 and lu_source[1] == 'P':
        r['LU_SOURCE'] = lu_source[:2]

    acctcred_rows.append(r)

    # -----------------------------------------------------------------------
    # Write SUBACRED record (LRECL=1550)
    # -----------------------------------------------------------------------
    buf = _build_buf(1550)
    _put_rec(buf, 1,    _fmt_n(_nv(r.get('FICODE')),  9))
    _put_rec(buf, 10,   _fmt_n(_nv(r.get('APCODE')),  3))
    _put_rec(buf, 13,   _fmt_n(acctno,                10))
    _put_rec(buf, 43,   _fmt_n(_nv(r.get('NOTENO')),  10))
    _put_rec(buf, 73,   _fmt_n(_nv(r.get('FACILITY')), 5))
    _put_rec(buf, 78,   _fmt_s(_sv(r.get('SYNDICAT')), 1))
    _put_rec(buf, 79,   _fmt_s(_sv(r.get('SPECIALF')), 2))
    _put_rec(buf, 81,   _fmt_s(_sv(r.get('PURPOSES')), 4))
    _put_rec(buf, 85,   _fmt_n(_nv(r.get('FCONCEPT_VAL', r.get('FCONCEPT'))), 2))
    _put_rec(buf, 87,   _fmt_z(_nv(r.get('NOTETERM')), 3))
    _put_rec(buf, 90,   _fmt_s(_sv(r.get('PAYFREQC')), 2))
    _put_rec(buf, 92,   _fmt_s(_sv(r.get('RESTRUCT')), 1))
    _put_rec(buf, 93,   _fmt_n(_nv(r.get('CAGAMAS')), 8))
    _put_rec(buf, 101,  _fmt_s(_sv(r.get('RECOURSE')), 1))
    _put_rec(buf, 102,  '00000000')
    _put_rec(buf, 110,  _fmt_n(_nv(r.get('CUSTCODE')), 2))
    _put_rec(buf, 112,  _fmt_s(_sv(r.get('SECTOR')), 4))
    _put_rec(buf, 116,  _fmt_n(_nv(r.get('OLDBRH')), 5))
    _put_rec(buf, 121,  _fmt_s(_sv(r.get('SCORE')), 3))
    _put_rec(buf, 124,  _fmt_n(_nv(r.get('COSTCTR')), 4))
    _put_rec(buf, 128,  _fmt_z(_nv(r.get('PAYDD')), 2))
    _put_rec(buf, 130,  _fmt_z(_nv(r.get('PAYMM')), 2))
    _put_rec(buf, 132,  _fmt_z(_nv(r.get('PAYYR')), 4))
    _put_rec(buf, 136,  _fmt_z(_nv(r.get('GRANTDD')), 2))
    _put_rec(buf, 138,  _fmt_z(_nv(r.get('GRANTMM')), 2))
    _put_rec(buf, 140,  _fmt_z(_nv(r.get('GRANTYR')), 4))
    _put_rec(buf, 144,  _fmt_z(_nv(r.get('CENSUS')), 6))
    _put_rec(buf, 150,  _fmt_z(_nv(r.get('LNMATDD')), 2))
    _put_rec(buf, 152,  _fmt_z(_nv(r.get('LNMATMM')), 2))
    _put_rec(buf, 154,  _fmt_z(_nv(r.get('LNMATYR')), 4))
    _put_rec(buf, 158,  _fmt_z(_nv(r.get('ORMATDD')), 2))
    _put_rec(buf, 160,  _fmt_z(_nv(r.get('ORMATMM')), 2))
    _put_rec(buf, 162,  _fmt_z(_nv(r.get('ORMATYR')), 4))
    _put_rec(buf, 166,  _fmt_n(_nv(r.get('CAVAIAMT')), 16))
    _put_rec(buf, 182,  _fmt_s(_sv(r.get('COLLMAKE')), 6))
    _put_rec(buf, 188,  _fmt_s(_sv(r.get('MODELDES')), 6))
    _put_rec(buf, 194,  (_fmt_z(_nv(r.get('DLVRDD')), 2)
                         if str(r.get('DLVRDD','')).isdigit()
                         else _fmt_s(_sv(r.get('DLVRDD')), 2)))
    _put_rec(buf, 196,  (_fmt_z(_nv(r.get('DLVRMM')), 2)
                         if str(r.get('DLVRMM','')).isdigit()
                         else _fmt_s(_sv(r.get('DLVRMM')), 2)))
    _put_rec(buf, 198,  (_fmt_z(_nv(r.get('DLVRYR')), 4)
                         if str(r.get('DLVRYR','')).isdigit()
                         else _fmt_s(_sv(r.get('DLVRYR')), 4)))
    _put_rec(buf, 202,  _fmt_z(_nv(r.get('COLLMM')), 2))
    _put_rec(buf, 204,  _fmt_z(_nv(r.get('COLLYR')), 2))
    _put_rec(buf, 206,  (_fmt_z(_nv(r.get('BILDUEDD')), 2)
                         if str(r.get('BILDUEDD','')).isdigit()
                         else _fmt_s(_sv(r.get('BILDUEDD')), 2)))
    _put_rec(buf, 208,  (_fmt_z(_nv(r.get('BILDUEMM')), 2)
                         if str(r.get('BILDUEMM','')).isdigit()
                         else _fmt_s(_sv(r.get('BILDUEMM')), 2)))
    _put_rec(buf, 210,  (_fmt_z(_nv(r.get('BILDUEYR')), 4)
                         if str(r.get('BILDUEYR','')).isdigit()
                         else _fmt_s(_sv(r.get('BILDUEYR')), 4)))
    _put_rec(buf, 214,  _fmt_s(_sv(r.get('PAYFREQ')), 1))
    _put_rec(buf, 215,  _fmt_s(_sv(r.get('ORGTYPE')), 1))
    _put_rec(buf, 216,  _fmt_s(_sv(r.get('PAIDIND')), 1))
    _put_rec(buf, 217,  _fmt_s(_sv(r.get('AANO')), 13))
    _put_rec(buf, 230,  _fmt_z(_nv(r.get('FINRELDD')), 2))
    _put_rec(buf, 232,  _fmt_z(_nv(r.get('FINRELMM')), 2))
    _put_rec(buf, 234,  _fmt_z(_nv(r.get('FINRELYY')), 4))
    _put_rec(buf, 238,  _fmt_s(_sv(r.get('ACCTSTAT')), 1))
    _put_rec(buf, 239,  _fmt_n(_nv(r.get('BONUSANO')), 5))
    _put_rec(buf, 244,  _fmt_s(_sv(r.get('MIGRATDT')), 11))
    _put_rec(buf, 260,  _fmt_s(_sv(r.get('SMCRITE1')).upper(), 1))
    _put_rec(buf, 262,  _fmt_s(_sv(r.get('SMCRITE2')).upper(), 1))
    _put_rec(buf, 264,  _fmt_s(_sv(r.get('SMCRITE3')).upper(), 1))
    _put_rec(buf, 266,  _fmt_s(_sv(r.get('SMCRITE4')).upper(), 1))
    _put_rec(buf, 268,  _fmt_s(_sv(r.get('SMCRITE5')).upper(), 1))
    _put_rec(buf, 270,  _fmt_s(_sv(r.get('SMCRITE6')).upper(), 1))
    _put_rec(buf, 272,  _fmt_s(_sv(r.get('FACTOR1')).upper(), 1))
    _put_rec(buf, 274,  _fmt_s(_sv(r.get('FACTOR2')).upper(), 1))
    _put_rec(buf, 276,  _fmt_s(_sv(r.get('FACTOR3')).upper(), 1))
    _put_rec(buf, 278,  _fmt_s(_sv(r.get('FACTOR4')).upper(), 1))
    _put_rec(buf, 280,  _fmt_s(_sv(r.get('FACTOR5')).upper(), 1))
    _put_rec(buf, 282,  _fmt_s(_sv(r.get('FACTOR6')).upper(), 1))
    _put_rec(buf, 284,  _fmt_s(_sv(r.get('FACTOR7')).upper(), 1))
    _put_rec(buf, 286,  _fmt_s(_sv(r.get('ACCTIND')), 1))
    _put_rec(buf, 288,  _fmt_s(_sv(r.get('SM_STATUS')), 1))
    _put_rec(buf, 290,  _fmt_s(_sv(r.get('SM_DAT1')), 8))
    _put_rec(buf, 299,  _fmt_s(_sv(r.get('ODSTATUS')), 2))
    _put_rec(buf, 302,  _fmt_s(_sv(r.get('CRR1')), 7))
    _put_rec(buf, 309,  _fmt_z(_nv(r.get('RMSBBA')), 15))
    _put_rec(buf, 324,  _fmt_z(_nv(r.get('NXRVWDD')), 2))
    _put_rec(buf, 326,  _fmt_z(_nv(r.get('NXRVWMM')), 2))
    _put_rec(buf, 328,  _fmt_z(_nv(r.get('NXRVWYY')), 4))
    _put_rec(buf, 332,  _fmt_s(_sv(r.get('CURCODE')), 3))
    _put_rec(buf, 335,  _fmt_s(_sv(r.get('ISSUEDD')), 2))
    _put_rec(buf, 337,  _fmt_s(_sv(r.get('ISSUEMM')), 2))
    _put_rec(buf, 339,  _fmt_s(_sv(r.get('ISSUEYY')), 4))
    _put_rec(buf, 343,  _fmt_n(_nv(r.get('INTRATE')), 5))
    _put_rec(buf, 348,  _fmt_z(_nv(r.get('REBATE')), 16))
    _put_rec(buf, 364,  _fmt_z(_nv(r.get('SPAAMT')), 16))
    _put_rec(buf, 380,  _fmt_s(_sv(r.get('TYPEPRC')), 2))
    _put_rec(buf, 400,  _fmt_n(_nv(r.get('CRRTOTSC')), 4))
    _put_rec(buf, 410,  _fmt_s(_sv(r.get('CACCRSCK')), 30))
    _put_rec(buf, 450,  _fmt_s(_sv(r.get('CADCHQCK')), 30))
    _put_rec(buf, 490,  _fmt_s(_sv(r.get('CADCHEQS')), 3))
    _put_rec(buf, 495,  _fmt_s(_sv(r.get('CACCRIS')), 3))
    _put_rec(buf, 500,  _fmt_s(_sv(r.get('LEGALACC')), 3))
    _put_rec(buf, 505,  _fmt_s(_sv(r.get('LEGALBOR')), 3))
    _put_rec(buf, 509,  _fmt_n(_nv(r.get('FACCODE')), 5))
    _put_rec(buf, 515,  _fmt_s(_sv(r.get('EREVDATE')), 8))
    _put_rec(buf, 524,  _fmt_z(_nv(r.get('HISTBAL')), 17))
    _put_rec(buf, 542,  _fmt_n(_nv(r.get('ORIWODATE')), 8))
    _put_rec(buf, 551,  _fmt_n(_nv(r.get('LASTMDATE')), 8))
    _put_rec(buf, 559,  _fmt_n(_nv(r.get('ORICODE')), 3))
    _put_rec(buf, 563,  _fmt_s(_sv(r.get('CMMATURDT')), 8))
    _put_rec(buf, 571,  _fmt_s(_sv(r.get('SECTFISS')), 4))
    _put_rec(buf, 575,  _fmt_s(_sv(r.get('CUSTFISS')), 2))
    _put_rec(buf, 578,  _fmt_s(_sv(r.get('USER5')), 1))
    _put_rec(buf, 580,  _fmt_s(_sv(r.get('BORSTAT')), 1))
    _put_rec(buf, 582,  _fmt_z(_nv(r.get('RRPAYCNT')), 4))
    _put_rec(buf, 590,  _fmt_s(_sv(r.get('CRRINI')), 5))
    _put_rec(buf, 596,  _fmt_s(_sv(r.get('CRRNOW')), 5))
    _put_rec(buf, 602,  _fmt_n(_nv(r.get('PZIPCODE')), 9))
    _put_rec(buf, 612,  _fmt_s(_sv(r.get('DNBFISME')), 1))
    _put_rec(buf, 614,  _fmt_s(_sv(r.get('FLOODIND')), 1))
    _put_rec(buf, 616,  _fmt_f(_nv(r.get('CASHPRICE')), 17, 2))
    _put_rec(buf, 634,  _fmt_f(_nv(r.get('ACCRUAL1')), 15, 2))
    _put_rec(buf, 650,  _fmt_z(_nv(r.get('CYC')), 3))
    _put_rec(buf, 653,  (_fmt_z(_nv(r.get('RRCNTYR')), 4)
                         if str(r.get('RRCNTYR','')).isdigit()
                         else _fmt_s(_sv(r.get('RRCNTYR')), 4)))
    _put_rec(buf, 657,  (_fmt_z(_nv(r.get('RRCNTMM')), 2)
                         if str(r.get('RRCNTMM','')).isdigit()
                         else _fmt_s(_sv(r.get('RRCNTMM')), 2)))
    _put_rec(buf, 659,  (_fmt_z(_nv(r.get('RRCNTDD')), 2)
                         if str(r.get('RRCNTDD','')).isdigit()
                         else _fmt_s(_sv(r.get('RRCNTDD')), 2)))
    _put_rec(buf, 661,  (_fmt_z(_nv(r.get('RRCOMPYR')), 4)
                         if str(r.get('RRCOMPYR','')).isdigit()
                         else _fmt_s(_sv(r.get('RRCOMPYR')), 4)))
    _put_rec(buf, 665,  (_fmt_z(_nv(r.get('RRCOMPMM')), 2)
                         if str(r.get('RRCOMPMM','')).isdigit()
                         else _fmt_s(_sv(r.get('RRCOMPMM')), 2)))
    _put_rec(buf, 667,  (_fmt_z(_nv(r.get('RRCOMPDD')), 2)
                         if str(r.get('RRCOMPDD','')).isdigit()
                         else _fmt_s(_sv(r.get('RRCOMPDD')), 2)))
    _put_rec(buf, 669,  _fmt_s(_sv(r.get('DELQCD')), 2))
    utilise_v = r.get('UTILISE')
    _put_rec(buf, 672,  (_fmt_n(_nv(utilise_v), 1)
                         if isinstance(utilise_v, (int, float))
                         else _fmt_s(_sv(utilise_v), 1)))
    _put_rec(buf, 673,  _fmt_s(_sv(r.get('RRTAG')), 6))
    _put_rec(buf, 679,  _fmt_s(_sv(r.get('MASMATDT')), 8))
    _put_rec(buf, 687,  _fmt_s(_sv(r.get('RSN')), 1))
    _put_rec(buf, 688,  _fmt_s(_sv(r.get('OLDRRACC')), 1))
    _put_rec(buf, 690,  _fmt_n(_nv(r.get('STAFFNO')), 11))
    _put_rec(buf, 702,  _fmt_s(_sv(r.get('MORSTDTE')), 8))
    _put_rec(buf, 711,  _fmt_s(_sv(r.get('MORENDTE')), 8))
    _put_rec(buf, 720,  _fmt_s(_sv(r.get('IA_LRU')), 1))
    _put_rec(buf, 721,  _fmt_z(_nv(r.get('TOTAMT')), 15))
    _put_rec(buf, 736,  _fmt_z(_nv(r.get('NETPROC')), 15))
    _put_rec(buf, 751,  _fmt_z(_nv(r.get('CURBAL')), 15))
    _put_rec(buf, 766,  _fmt_n(_nv(r.get('COMMNO_OLD')), 3))
    _put_rec(buf, 769,  _fmt_s(_sv(r.get('OLDRR')), 1))
    _put_rec(buf, 770,  _fmt_s(_sv(r.get('RISKCODE')), 1))
    _put_rec(buf, 771,  _fmt_z(_nv(r.get('ODEDD')), 2))
    _put_rec(buf, 773,  _fmt_z(_nv(r.get('ODEMM')), 2))
    _put_rec(buf, 775,  _fmt_z(_nv(r.get('ODEYY')), 4))
    _put_rec(buf, 779,  _fmt_s(_sv(r.get('ISSXDAY')), 2))
    _put_rec(buf, 781,  _fmt_s(_sv(r.get('ISSXMONT')), 2))
    _put_rec(buf, 783,  _fmt_s(_sv(r.get('ISSXYEAR')), 4))
    _put_rec(buf, 788,  _fmt_s(_sv(r.get('FDB')), 1))
    _put_rec(buf, 790,  _fmt_s(_sv(r.get('RJDATEYY')), 4))
    _put_rec(buf, 794,  _fmt_s(_sv(r.get('RJDATEMM')), 2))
    _put_rec(buf, 796,  _fmt_s(_sv(r.get('RJDATEDD')), 2))
    _put_rec(buf, 799,  _fmt_s(_sv(r.get('REASON')), 294))
    _put_rec(buf, 1094, _fmt_s(_sv(r.get('STP')), 1))
    _put_rec(buf, 1096, _fmt_s(_sv(r.get('FRTDISCODE')), 3))
    _put_rec(buf, 1100, _fmt_s(_sv(r.get('LSTDISCODE')), 3))
    _put_rec(buf, 1103, _fmt_s(_sv(r.get('FLAG1')), 1))
    _put_rec(buf, 1104, _fmt_z(_nv(r.get('FULLREL_DD')), 2))
    _put_rec(buf, 1106, _fmt_z(_nv(r.get('FULLREL_MM')), 2))
    _put_rec(buf, 1108, _fmt_z(_nv(r.get('FULLREL_YY')), 4))
    _put_rec(buf, 1112, _fmt_n(_nv(r.get('LMTINDEX')), 5))
    _put_rec(buf, 1117, _fmt_n(_nv(r.get('ODPLAN')), 5))
    _put_rec(buf, 1122, _fmt_n(_nv(r.get('UNUSEAMT')), 16))
    _put_rec(buf, 1138, _fmt_n(_nv(r.get('CORGAMT')), 16))
    _put_rec(buf, 1154, _fmt_s(_sv(r.get('STRUPCO_3YR')), 5))    # 2018-00001435
    _put_rec(buf, 1160, _fmt_f(_nv(r.get('DSR')), 6, 2))          # 2018-00001438 & 2018-00001439
    _put_rec(buf, 1166, _fmt_s(_sv(r.get('REFIN_FLG')), 3))        # 2018-1432 & 2018-1442
    _put_rec(buf, 1169, _fmt_s(_sv(r.get('OLD_FI_CD')), 10))       # 2018-1432 & 2018-1442
    _put_rec(buf, 1179, _fmt_s(_sv(r.get('OLD_MACC_NO')), 30))     # 2018-1432 & 2018-1442 & 2019-532
    _put_rec(buf, 1209, _fmt_s(_sv(r.get('OLD_SUBACC_NO')), 30))   # 2018-1432 & 2018-1442 & 2019-532
    _put_rec(buf, 1239, _fmt_f(_nv(r.get('REFIN_AMT')), 20, 2))    # 2018-1432 & 2018-1442
    _put_rec(buf, 1260, _fmt_s(_sv(r.get('LN_UTILISE_LOCAT_CD')), 5))  # 2018-00001435
    _put_rec(buf, 1266, _fmt_s(_sv(r.get('AKPK_STATUS')), 9))      # 2020-1212
    _put_rec(buf, 1276, _fmt_s(_sv(r.get('INDUSTRIAL_SECTOR_CD')), 5))
    _put_rec(buf, 1282, _fmt_z(_nv(r.get('RRSTRYR')), 4))
    _put_rec(buf, 1286, _fmt_z(_nv(r.get('RRSTRMM')), 2))
    _put_rec(buf, 1288, _fmt_z(_nv(r.get('RRSTRDD')), 2))
    _put_rec(buf, 1291, _fmt_z(_nv(r.get('RRCMPLYR')), 4))
    _put_rec(buf, 1295, _fmt_z(_nv(r.get('RRCMPLMM')), 2))
    _put_rec(buf, 1297, _fmt_z(_nv(r.get('RRCMPLDD')), 2))
    _put_rec(buf, 1300, _fmt_s(_sv(r.get('CLIMATE_PRIN_TAXONOMY_CLASS')), 5))
    _put_rec(buf, 1305, _fmt_s(_sv(r.get('LU_ADD1')), 40))
    _put_rec(buf, 1345, _fmt_s(_sv(r.get('LU_ADD2')), 40))
    _put_rec(buf, 1385, _fmt_s(_sv(r.get('LU_ADD3')), 40))
    _put_rec(buf, 1425, _fmt_s(_sv(r.get('LU_ADD4')), 40))
    _put_rec(buf, 1465, _fmt_s(_sv(r.get('LU_TOWN_CITY')), 20))
    _put_rec(buf, 1485, _fmt_s(_sv(r.get('LU_POSTCODE')), 5))
    _put_rec(buf, 1490, _fmt_s(_sv(r.get('LU_STATE_CD')), 2))
    _put_rec(buf, 1492, _fmt_s(_sv(r.get('LU_COUNTRY_CD')), 2))
    _put_rec(buf, 1494, _fmt_s(_sv(r.get('LU_SOURCE')), 5))
    f_subacred.write(bytes(buf) + b'\n')

    # -----------------------------------------------------------------------
    # Write CREDITPO record (LRECL=700)
    # -----------------------------------------------------------------------
    buf = _build_buf(700)
    _put_rec(buf, 1,   _fmt_n(_nv(r.get('FICODE')),  9))
    _put_rec(buf, 10,  _fmt_n(_nv(r.get('APCODE')),  3))
    _put_rec(buf, 13,  _fmt_n(acctno,                10))
    _put_rec(buf, 43,  _fmt_n(_nv(r.get('NOTENO')),  10))
    _put_rec(buf, 73,  REPTDAY)
    _put_rec(buf, 75,  REPTMON)
    _put_rec(buf, 77,  REPTYEAR)
    _put_rec(buf, 81,  _fmt_z(_nv(r.get('OUTSTAND')), 16))
    _put_rec(buf, 97,  _fmt_z(_nv(r.get('ARREARS')), 3))
    _put_rec(buf, 100, _fmt_z(_nv(r.get('INSTALM')), 3))
    _put_rec(buf, 103, _fmt_z(_nv(r.get('UNDRAWN')), 17))
    _put_rec(buf, 120, _fmt_s(_sv(r.get('ACCTSTAT')), 1))
    _put_rec(buf, 121, _fmt_z(_nv(r.get('NODAYS')), 5))
    _put_rec(buf, 126, _fmt_n(_nv(r.get('OLDBRH')), 5))
    _put_rec(buf, 131, _fmt_z(_nv(r.get('BILTOT')), 17))
    _put_rec(buf, 148, _fmt_z(_nv(r.get('ODXSAMT')), 17))
    _put_rec(buf, 165, _fmt_n(_nv(r.get('COSTCTR')), 4))
    _put_rec(buf, 169, _fmt_s(_sv(r.get('AANO')), 13))
    _put_rec(buf, 182, _fmt_n(_nv(r.get('FACILITY')), 5))
    _put_rec(buf, 187, _fmt_s(_sv(r.get('COMPLIBY')), 60))
    _put_rec(buf, 247, _fmt_s(_sv(r.get('COMPLIYY')), 4))
    _put_rec(buf, 251, _fmt_s(_sv(r.get('COMPLIMM')), 2))
    _put_rec(buf, 253, _fmt_s(_sv(r.get('COMPLIDD')), 2))
    _put_rec(buf, 255, _fmt_s(_sv(r.get('COMPLIGR')), 15))
    _put_rec(buf, 270, _fmt_z(_nv(r.get('EIR_ADJ')), 16))
    _put_rec(buf, 286, _fmt_s(_sv(r.get('PAIDIND')), 1))
    _put_rec(buf, 287, _fmt_z(_nv(r.get('LSTTRNCD')), 3))
    _put_rec(buf, 290, _fmt_z(_nv(r.get('DISBURSE')), 15))
    _put_rec(buf, 305, _fmt_z(_nv(r.get('REPAID')), 15))
    _put_rec(buf, 321, _fmt_z(_nv(r.get('CURBAL')), 17))
    _put_rec(buf, 338, _fmt_z(_nv(r.get('INTERDUE')), 17))
    _put_rec(buf, 355, _fmt_z(_nv(r.get('FEEAMT')), 17))
    _put_rec(buf, 373, _fmt_n(_nv(r.get('FACCODE')), 5))
    _put_rec(buf, 378, _fmt_s(_sv(r.get('NTINT')), 1))
    _put_rec(buf, 379, _fmt_s(_sv(r.get('WOSTAT')), 1))
    _put_rec(buf, 380, _fmt_n(_nv(r.get('LOANSTAT')), 1))
    _put_rec(buf, 381, _fmt_f(_nv(r.get('PAYAMT')), 17, 2))
    _put_rec(buf, 398, _sv(r.get('IMPAIRYY', '')))
    _put_rec(buf, 402, _sv(r.get('IMPAIRMM', '')))
    _put_rec(buf, 404, _sv(r.get('IMPAIRDD', '')))
    _put_rec(buf, 407, _fmt_f(_nv(r.get('HCURBAL')), 15, 2))
    _put_rec(buf, 423, _fmt_z(_nv(r.get('XNODAYS')), 5))
    _put_rec(buf, 429, _fmt_s(_sv(r.get('ASSMYY')), 4))
    _put_rec(buf, 434, _fmt_s(_sv(r.get('ASSMMM')), 2))
    _put_rec(buf, 437, _fmt_s(_sv(r.get('ASSMDD')), 2))
    _put_rec(buf, 439, _fmt_f(_nv(r.get('CCRIS_INSTLAMT')), 17, 2))
    _put_rec(buf, 490, _fmt_s(_sv(r.get('REPAYSRC')), 4))
    _put_rec(buf, 496, _fmt_s(_sv(r.get('REPAY_TYPE_CD')), 2))
    _put_rec(buf, 498, _fmt_f(_nv(r.get('MTD_REPAID_AMT')), 17, 2))
    _put_rec(buf, 515, _fmt_f(_nv(r.get('MAN_REV_RATE')), 9, 6))   # MANUAL REVIEW RATE 2017-3654
    _put_rec(buf, 525, _fmt_s(_sv(r.get('MAN_REV_DATE')), 8))       # MANUAL REVIEW DATE 2017-3654
    _put_rec(buf, 533, _fmt_f(_nv(r.get('SYS_REV_RATE')), 9, 6))   # SYSTEM REVIEW RATE 2017-3654
    _put_rec(buf, 543, _fmt_s(_sv(r.get('SYS_REV_DATE')), 8))       # SYSTEM REVIEW DATE 2017-3654
    _put_rec(buf, 553, _fmt_s(_sv(r.get('NURS_TAG')), 6))            # SMR 2018-4594 & 2020-1986
    _put_rec(buf, 559, _fmt_s(_sv(r.get('NURSYY')), 4))              # SMR 2018-4594
    _put_rec(buf, 563, _fmt_s(_sv(r.get('NURSMM')), 2))              # SMR 2018-4594
    _put_rec(buf, 565, _fmt_s(_sv(r.get('NURSDD')), 2))              # SMR 2018-4594
    _put_rec(buf, 567, _fmt_s(_sv(r.get('LMO_TAG')), 2))             # SMR 2020-779
    _put_rec(buf, 569, _fmt_s(_sv(r.get('RATAG')), 10))              # SMR 2020-4857
    _put_rec(buf, 579, _fmt_s(_sv(r.get('RADTDD')), 2))              # SMR 2020-4857
    _put_rec(buf, 581, _fmt_s(_sv(r.get('RADTMM')), 2))              # SMR 2020-4857
    _put_rec(buf, 583, _fmt_s(_sv(r.get('RADTYY')), 4))              # SMR 2020-4857
    _put_rec(buf, 587, _fmt_s(_sv(r.get('WRIOFF_CLOSE_FILE_TAG')), 1))  # SMR 2024-4579
    _put_rec(buf, 588, _fmt_s(_sv(r.get('WRIOFFDD')), 2))            # SMR 2024-4579
    _put_rec(buf, 590, _fmt_s(_sv(r.get('WRIOFFMM')), 2))            # SMR 2024-4579
    _put_rec(buf, 592, _fmt_s(_sv(r.get('WRIOFFYY')), 4))            # SMR 2024-4579
    _put_rec(buf, 596, _fmt_z(_nv(r.get('FWRITE_DOWN_BAL')), 15))    # SMR 2025-1546
    f_creditpo.write(bytes(buf) + b'\n')

    # -----------------------------------------------------------------------
    # Write PROVISIO record (if impaired or GP3IND set, and not HP)
    # -----------------------------------------------------------------------
    gp3ind   = _sv(r.get('GP3IND'))
    impaired = _sv(r.get('IMPAIRED'))
    if (impaired == 'Y' or gp3ind.strip() != '') and loantype not in HP:
        buf = _build_buf(600)
        _put_rec(buf, 1,   _fmt_n(_nv(r.get('FICODE')),  9))
        _put_rec(buf, 10,  _fmt_n(_nv(r.get('APCODE')),  3))
        _put_rec(buf, 13,  _fmt_n(acctno,                10))
        _put_rec(buf, 43,  _fmt_n(_nv(r.get('NOTENO')),  10))
        _put_rec(buf, 73,  REPTDAY)
        _put_rec(buf, 75,  REPTMON)
        _put_rec(buf, 77,  REPTYEAR)
        _put_rec(buf, 81,  _fmt_s(_sv(r.get('CLASSIFI')), 1))
        _put_rec(buf, 82,  _fmt_z(_nv(r.get('ARREARS')), 3))
        _put_rec(buf, 85,  _fmt_z(_nv(r.get('CURBAL')), 17))
        _put_rec(buf, 102, _fmt_z(_nv(r.get('INTERDUE')), 17))
        _put_rec(buf, 119, _fmt_z(_nv(r.get('FEEAMT')), 16))
        _put_rec(buf, 135, _fmt_z(_nv(r.get('REALISAB')), 17))
        _put_rec(buf, 152, _fmt_z(_nv(r.get('IISOPBAL')), 17))
        _put_rec(buf, 169, _fmt_z(_nv(r.get('TOTIIS')), 17))
        _put_rec(buf, 186, _fmt_z(_nv(r.get('TOTIISR')), 17))
        _put_rec(buf, 203, _fmt_z(_nv(r.get('TOTWOF')), 17))
        _put_rec(buf, 220, _fmt_z(_nv(r.get('IISDANAH')), 17))
        _put_rec(buf, 237, _fmt_z(_nv(r.get('IISTRANS')), 17))
        _put_rec(buf, 254, _fmt_z(_nv(r.get('SPOPBAL')), 17))
        _put_rec(buf, 271, _fmt_z(_nv(r.get('SPCHARGE')), 17))
        _put_rec(buf, 288, _fmt_z(_nv(r.get('SPWBAMT')), 17))
        _put_rec(buf, 305, _fmt_z(_nv(r.get('SPWOF')), 17))
        _put_rec(buf, 322, _fmt_z(_nv(r.get('SPDANAH')), 17))
        _put_rec(buf, 339, _fmt_z(_nv(r.get('SPTRANS')), 17))
        _put_rec(buf, 356, _fmt_s(gp3ind, 1))
        _put_rec(buf, 357, _fmt_n(_nv(r.get('OLDBRH')), 5))
        _put_rec(buf, 362, _fmt_n(_nv(r.get('COSTCTR')), 5))
        _put_rec(buf, 367, _fmt_s(_sv(r.get('AANO')), 13))
        _put_rec(buf, 380, _fmt_n(_nv(r.get('FACILITY')), 5))
        _put_rec(buf, 385, _fmt_s(_sv(r.get('NPLIND')), 1))
        _put_rec(buf, 386, _fmt_n(_nv(r.get('FACCODE')), 5))
        _put_rec(buf, 392, _fmt_z(_nv(r.get('LEDGBAL')), 17))
        _put_rec(buf, 409, _fmt_z(_nv(r.get('CUMRC')), 17))
        _put_rec(buf, 426, _fmt_z(_nv(r.get('BDR')), 17))
        _put_rec(buf, 443, _fmt_z(_nv(r.get('CUMSC')), 17))
        _put_rec(buf, 460, _fmt_z(_nv(r.get('WOAMT')), 17))
        _put_rec(buf, 477, _fmt_z(_nv(r.get('BDR_MTH')), 17))
        _put_rec(buf, 494, _fmt_z(_nv(r.get('SC_MTH')), 17))
        _put_rec(buf, 511, _fmt_z(_nv(r.get('RC_MTH')), 17))
        _put_rec(buf, 528, _fmt_z(_nv(r.get('NAI_MTH')), 17))
        _put_rec(buf, 545, _fmt_z(_nv(r.get('CUMWOSP')), 17))
        _put_rec(buf, 562, _fmt_z(_nv(r.get('CUMWOIS')), 17))
        _put_rec(buf, 579, _fmt_z(_nv(r.get('CUMNAI')), 17))
        _put_rec(buf, 596, _fmt_s(impaired, 1))
        f_provisio.write(bytes(buf) + b'\n')

    # -----------------------------------------------------------------------
    # Write LEGALACT record (if LEG='A')
    # -----------------------------------------------------------------------
    if _sv(r.get('LEG')) == 'A':
        buf = _build_buf(100)
        _put_rec(buf, 1,  _fmt_n(_nv(r.get('FICODE')), 9))
        _put_rec(buf, 10, _fmt_n(_nv(r.get('APCODE')), 3))
        _put_rec(buf, 13, _fmt_n(acctno,               10))
        _put_rec(buf, 43, _fmt_s(_sv(r.get('DELQCD')), 2))
        _put_rec(buf, 45, REPTDAY)
        _put_rec(buf, 47, REPTMON)
        _put_rec(buf, 49, REPTYEAR)
        _put_rec(buf, 53, _fmt_n(_nv(r.get('OLDBRH')), 5))
        _put_rec(buf, 58, _fmt_n(_nv(r.get('COSTCTR')), 4))
        _put_rec(buf, 62, _fmt_s(_sv(r.get('AANO')), 13))
        _put_rec(buf, 75, _fmt_n(_nv(r.get('FACILITY')), 5))
        _put_rec(buf, 80, _fmt_n(_nv(r.get('FACCODE')), 5))
        f_legalact.write(bytes(buf) + b'\n')

f_subacred.close()
f_creditpo.close()
f_provisio.close()
f_legalact.close()

# ---------------------------------------------------------------------------
# STEP 9: WRIOFAC - read written-off file
# ---------------------------------------------------------------------------
# DATA WRIOFAC: INFILE WRITOFF
wriofac_rows = []
with open(WRITOFF_FILE, 'r', encoding='ascii', errors='replace') as fwf:
    for line in fwf:
        line = line.rstrip('\n')
        if len(line) < 75:
            line = line.ljust(75)
        try:
            acctno_w  = int(line[9:19].strip() or '0')
            noteno_w  = int(line[19:29].strip() or '0')
            restruct_w = line[73:74]
        except ValueError:
            continue
        # ESMR2013-2133: CONVERSION FROM 'R','S' TO 'C','T'
        if restruct_w == 'R': restruct_w = 'C'
        if restruct_w == 'S': restruct_w = 'T'
        wriofac_rows.append({'ACCTNO': acctno_w, 'NOTENO': noteno_w, 'RESTRUCT': restruct_w})

wriofac = pl.DataFrame(wriofac_rows).sort(['ACCTNO', 'NOTENO'])

# ---------------------------------------------------------------------------
# STEP 10: FAC / FACTOR from APP10 file
# ---------------------------------------------------------------------------
fac_rows = []
with open(APP10_FILE, 'r', encoding='ascii', errors='replace') as f10:
    for line in f10:
        line = line.rstrip('\n').ljust(700)
        row = {
            'AANO':     line[0:13],
            'ACCEPT':   line[120:124].strip(),
            'MARGIN':   line[124:128].strip(),
            'ADEQU':    line[128:132].strip(),
            'SUPPORT':  line[132:136].strip(),
            'GAIN':     line[136:140].strip(),
            'CGC':      line[140:144].strip(),
            'GUARANT':  line[144:148].strip(),
            'CHARAC':   line[148:152].strip(),
            'SIMILR':   line[152:156].strip(),
            'FIRST':    line[156:160].strip(),
            'CACCRSCK': line[424:454],
            'CADCHQCK': line[457:487],
            'LEGALACC': line[490:493].strip(),
            'CADCHEQS': line[508:511].strip(),
            'CACCRIS':  line[514:517].strip(),
            'LEGALBOR': line[657:660].strip(),
        }
        fac_rows.append(row)

factor_rows = []
for r in fac_rows:
    acc   = r['ACCEPT'];  mar = r['MARGIN']; ade = r['ADEQU']
    sup   = r['SUPPORT']; gai = r['GAIN'];   cgc = r['CGC']
    gua   = r['GUARANT']; cha = r['CHARAC']; sim = r['SIMILR']
    fir   = r['FIRST']

    f1 = 'Y' if acc else 'N'
    f2 = 'Y' if mar else 'N'
    f3 = 'Y' if ade else 'N'
    f4 = 'Y' if sup else 'N'
    # FACTOR5: Y if GAIN='Y' OR CGC='Y'; N only if both empty
    f5 = 'Y' if (gai == 'Y' or cgc == 'Y' or gai or cgc) else 'N'
    # Exact SAS logic: multiple overlapping IF statements, last one wins
    if gai == '' and cgc == '': f5 = 'N'
    f6 = 'Y' if gua else 'N'
    # FACTOR7: Y if any of CHARAC/SIMILAR/FIRST is 'Y'; N only if all empty
    f7 = 'Y' if (cha == 'Y' or sim == 'Y' or fir == 'Y') else 'N'
    if cha == '' and sim == '' and fir == '': f7 = 'N'

    factor_rows.append({
        'AANO':     r['AANO'],
        'FACTOR1':  f1, 'FACTOR2': f2, 'FACTOR3': f3, 'FACTOR4': f4,
        'FACTOR5':  f5, 'FACTOR6': f6, 'FACTOR7': f7,
        'CACCRSCK': r['CACCRSCK'], 'CADCHQCK': r['CADCHQCK'],
        'CADCHEQS': r['CADCHEQS'], 'CACCRIS':  r['CACCRIS'],
        'LEGALACC': r['LEGALACC'], 'LEGALBOR': r['LEGALBOR'],
    })

factor = pl.DataFrame(factor_rows).sort('AANO')

# ---------------------------------------------------------------------------
# STEP 11: TOTSC from APP7
# ---------------------------------------------------------------------------
totsc_rows = []
with open(APP7_FILE, 'r', encoding='ascii', errors='replace') as f7:
    for line in f7:
        line = line.rstrip('\n').ljust(90)
        try:
            sc = int(line[81:85].strip() or '0')
        except ValueError:
            sc = 0
        totsc_rows.append({'AANO': line[0:13], 'CRRTOTSC': sc})

totsc = pl.DataFrame(totsc_rows).sort('AANO')

factor = factor.join(totsc, on='AANO', how='left').sort('AANO')

# ---------------------------------------------------------------------------
# STEP 12: Merge FACTOR into ACCTCRED
# ---------------------------------------------------------------------------
acctcred = pl.DataFrame(acctcred_rows).sort('AANO')
acctcred = acctcred.join(factor, on='AANO', how='left', suffix='_FAC')

def _fix_factor(rows):
    out = []
    for r in rows:
        r = dict(r)
        in_b = r.pop('_IN_FAC', False)
        if in_b:
            r['ACCTIND'] = 'E'
        for fld in ('FACTOR1','FACTOR2','FACTOR3','FACTOR4','FACTOR5','FACTOR6','FACTOR7'):
            if _sv(r.get(fld)) in ('.', '', None):
                r[fld] = 'N'
        out.append(r)
    return out

acctcred = pl.DataFrame(_fix_factor(acctcred.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 13: ELN12, ELN5, ELN1 from ELDSRV12/RV05/RV01 files
# ---------------------------------------------------------------------------
def _read_eln12():
    rows = []
    with open(ELDSRV12_FILE, 'r', encoding='ascii', errors='replace') as f:
        next(f, None)  # FIRSTOBS=2
        for line in f:
            line = line.rstrip('\n').ljust(60)
            newid    = line[0:12].upper().strip()
            reviewno = line[15:32].upper().strip()
            try:
                acctno_e = int(line[35:55].strip() or '0')
            except ValueError:
                acctno_e = 0
            if not newid or not reviewno:
                continue
            rows.append({'NEWID': newid, 'REVIEWNO': reviewno, 'ACCTNO': acctno_e})
    return pl.DataFrame(rows).sort(['NEWID','REVIEWNO'])

def _read_eln5():
    rows = []
    with open(ELDSRV5_FILE, 'r', encoding='ascii', errors='replace') as f:
        next(f, None)
        for line in f:
            line = line.rstrip('\n').ljust(420)
            newid    = line[0:12].strip()
            reviewno = line[15:32].upper().strip()
            if not newid or not reviewno:
                continue
            rows.append({
                'NEWID': newid, 'REVIEWNO': reviewno,
                'SMCRITE1': line[386:387].upper(),
                'SMCRITE2': line[390:391].upper(),
                'SMCRITE3': line[394:395].upper(),
                'SMCRITE4': line[398:399].upper(),
                'SMCRITE5': line[402:403].upper(),
                'SMCRITE6': line[406:407].upper(),
            })
    return pl.DataFrame(rows).sort(['NEWID','REVIEWNO'])

def _read_eln1():
    rows = []
    with open(ELDSRV1_FILE, 'r', encoding='ascii', errors='replace') as f:
        next(f, None)
        for line in f:
            line = line.rstrip('\n').ljust(1020)
            newid    = line[0:12].upper().strip()
            reviewno = line[15:32].upper().strip()
            if not newid or not reviewno:
                continue
            crr1     = line[188:191]
            try:
                nxdd = int(line[613:615].strip() or '0')
                nxmm = int(line[616:618].strip() or '0')
                nxyy = int(line[619:623].strip() or '0')
            except ValueError:
                nxdd = nxmm = nxyy = 0
            rvdate   = line[1009:1019].strip()
            erevdate = rvdate.replace('/', '')
            nxrvdate = 0
            try:
                if nxdd > 0 and nxmm > 0 and nxyy > 0:
                    nxrvdate = _sas_date_num(date(nxyy, nxmm, nxdd))
            except Exception:
                pass
            rows.append({
                'NEWID': newid, 'REVIEWNO': reviewno,
                'CRR1': crr1, 'NXRVWDD': nxdd, 'NXRVWMM': nxmm, 'NXRVWYY': nxyy,
                'EREVDATE': erevdate, 'NXRVDATE': nxrvdate,
            })
    return pl.DataFrame(rows).sort(['NEWID','REVIEWNO'])

eln12 = _read_eln12()
eln5  = _read_eln5()
eln1  = _read_eln1()

smc = eln12.join(eln5, on=['NEWID','REVIEWNO'], how='left')
smc = smc.join(eln1, on=['NEWID','REVIEWNO'], how='left')
smc = smc.unique(subset=['ACCTNO'], keep='first')

acctcred = acctcred.sort('ACCTNO')
smc_sel  = smc.select(['ACCTNO','SMCRITE1','SMCRITE2','SMCRITE3','SMCRITE4',
                        'SMCRITE5','SMCRITE6','CRR1','NXRVDATE','EREVDATE'])
acctcred = acctcred.join(smc_sel, on='ACCTNO', how='left', suffix='_SMC')

def _fix_smc(rows):
    out = []
    for r in rows:
        r = dict(r)
        in_b = r.pop('_IN_SMC', False)
        if in_b:
            r['ACCTIND'] = 'E'
        for fld in ('SMCRITE1','SMCRITE2','SMCRITE3','SMCRITE4','SMCRITE5','SMCRITE6'):
            if _sv(r.get(fld)) in ('.', '', None):
                r[fld] = 'N'
        # NXRVWDD/MM/YY from NXRVDATE
        nxrvdate = _nv(r.get('NXRVDATE'))
        if nxrvdate > 0:
            nd = _sas_date(nxrvdate)
            r['NXRVWDD'] = f"{nd.day:02d}" if nd else '00'
            r['NXRVWMM'] = f"{nd.month:02d}" if nd else '00'
            r['NXRVWYY'] = str(nd.year) if nd else '0000'
        else:
            r['NXRVWDD'] = '00'
            r['NXRVWMM'] = '00'
            r['NXRVWYY'] = '0000'
        out.append(r)
    return out

acctcred = pl.DataFrame(_fix_smc(acctcred.to_dicts()))

# ---------------------------------------------------------------------------
# STEP 14: CM (LNCOMM) merge - CMMATURDT
# ---------------------------------------------------------------------------
cm = con.execute(
    f"SELECT ACCTNO, COMMNO, EXPIREDT FROM read_parquet('{BNM_DIR}/LNCOMM.parquet')"
).pl()

def _build_cm(rows):
    out = []
    for r in rows:
        r = dict(r)
        expiredt = _nv(r.get('EXPIREDT'))
        cmmaturdt = ''
        if expiredt not in (0, None) and not (isinstance(expiredt, float) and math.isnan(expiredt)):
            exprdate = _mmddyy_from_z11(expiredt)
            if exprdate:
                expiredd = f"{exprdate.day:02d}"
                expiremm = f"{exprdate.month:02d}"
                expireyy = str(exprdate.year)
                cmmaturdt = (expireyy + expiremm + expiredd).strip()
        r['CMMATURDT'] = cmmaturdt
        out.append({'ACCTNO': r['ACCTNO'], 'COMMNO': r['COMMNO'], 'CMMATURDT': cmmaturdt})
    return out

cm_rows = _build_cm(cm.to_dicts())
cm_pl   = pl.DataFrame(cm_rows).sort(['ACCTNO','COMMNO'])

acctcred = acctcred.sort(['ACCTNO','COMMNO'])
acctcred = acctcred.join(cm_pl, on=['ACCTNO','COMMNO'], how='left', suffix='_CM')

# ---------------------------------------------------------------------------
# STEP 15: Merge WRIOFAC into ACCTCRED
# ---------------------------------------------------------------------------
acctcred = acctcred.sort(['ACCTNO','NOTENO'])

def _apply_wriofac(rows, wriofac_dict):
    out = []
    for r in rows:
        r = dict(r)
        key = (int(_nv(r.get('ACCTNO'))), int(_nv(r.get('NOTENO'))))
        wf  = wriofac_dict.get(key)
        if wf:
            apcode_v = _nv(r.get('APCODE'))
            paidind_v = _sv(r.get('PAIDIND'))
            rs = wf['RESTRUCT']
            if apcode_v not in (983, 993) and paidind_v != 'P':
                if rs == 'C':   r['ACCTSTAT'] = 'C'
                elif rs == 'T': r['ACCTSTAT'] = 'T'

        # COMPLIDD/MM/YY
        complidt = _nv(r.get('COMPLIDT'))
        if complidt > 0:
            cd = _sas_date(complidt)
            r['COMPLIDD'] = f"{cd.day:02d}" if cd else '00'
            r['COMPLIMM'] = f"{cd.month:02d}" if cd else '00'
            r['COMPLIYY'] = str(cd.year) if cd else '0000'
        else:
            r['COMPLIDD'] = '00'; r['COMPLIMM'] = '00'; r['COMPLIYY'] = '0000'

        # WRIOFFDD/MM/YY
        wcftdt = _nv(r.get('WRIOFF_CLOSE_FILE_TAG_DT'))
        if wcftdt > 0:
            wd = _sas_date(wcftdt)
            r['WRIOFFDD'] = f"{wd.day:02d}" if wd else '00'
            r['WRIOFFMM'] = f"{wd.month:02d}" if wd else '00'
            r['WRIOFFYY'] = str(wd.year) if wd else '0000'

        # SM_DAT1
        sm_date = _nv(r.get('SM_DATE'))
        if sm_date > 0:
            sd = _sas_date(sm_date)
            if sd:
                dd = f"{sd.day:02d}"; mm = f"{sd.month:02d}"; yy = str(sd.year)
                r['SM_DAT1'] = dd + mm + yy
            else:
                r['SM_DAT1'] = '00000000'
        else:
            r['SM_DAT1'] = '00000000'
            r['ACCTIND'] = ''

        if _sv(r.get('SM_STATUS')) != 'Y':
            r['SM_STATUS'] = 'N'

        # OLDRR logic (16-387/16-210)
        user5  = _sv(r.get('USER5'))
        oldrr  = _sv(r.get('OLDRR'))
        if user5 == 'R' or oldrr == 'R':
            r['OLDRRACC'] = 'R'
            dlvdate_v = _nv(r.get('DLVDATE'))
            mar2016   = _sas_date_num(date(2016, 3, 1))
            if user5 == 'R' or (oldrr == 'R' and dlvdate_v < mar2016):
                r['RESTRUCT'] = ' '
                if _sv(r.get('ACCTSTAT')) not in ('S','W','P'):
                    r['ACCTSTAT'] = 'O'

        # AKPK_STATUS
        loantype_v   = int(_nv(r.get('LOANTYPE')))
        akpk_status  = _sv(r.get('AKPK_STATUS'))
        noteno_v     = int(_nv(r.get('NOTENO')))
        if loantype_v in (128,130,131,132,380,381,700,705,720,725,
                          678,679,698,699,983,993):
            if akpk_status in ('AKPK','RAKPK') and _sv(r.get('ACCTSTAT')) not in ('S','F'):
                r['ACCTSTAT'] = 'K'
            if akpk_status == 'RAKPK' and noteno_v >= 98000:
                r['RESTRUCT'] = ''
                r['RRTAG']    = akpk_status

        # HISTBAL
        dlvrmm_v = _sv(r.get('DLVRMM')).strip()
        if dlvrmm_v == REPTMON:
            r['HISTBAL'] = _nv(r.get('OUTSTAND'))
        else:
            r['HISTBAL'] = 0

        # FLOODIND
        modeldes_v = _sv(r.get('MODELDES'))
        rrind_v    = _sv(r.get('RRIND'))
        rrtag_v    = _sv(r.get('RRTAG'))
        if ((len(modeldes_v) >= 5 and modeldes_v[4] == 'F') or
            (len(rrind_v) >= 2 and rrind_v[1] == 'F')) and rrtag_v:
            r['FLOODIND'] = 'Y'

        # HP MODELDES overrides for DLVR dates
        if loantype_v in HP:
            r['LEGIND'] = ' '
            if modeldes_v in ('RARM1','RAM1'):
                r['DLVRDD']='31'; r['DLVRMM']='12'; r['DLVRYR']='2007'
            elif modeldes_v in ('RARM2','RAM2'):
                r['DLVRDD']='31'; r['DLVRMM']='07'; r['DLVRYR']='2011'
            elif modeldes_v in ('RARM3','RAM3'):
                r['DLVRDD']='31'; r['DLVRMM']='05'; r['DLVRYR']='2012'
            elif modeldes_v == 'RAM4':
                r['DLVRDD']='31'; r['DLVRMM']='12'; r['DLVRYR']='2012'
            elif modeldes_v == 'RAM5':
                r['DLVRDD']='30'; r['DLVRMM']='04'; r['DLVRYR']='2014'

        # MODELDES='Z'
        if modeldes_v == 'Z':
            r['RRTAG'] = 'Z'; r['CYC'] = 6
            dlivrydt_v = _nv(r.get('DLIVRYDT'))
            if dlivrydt_v > 0:
                dd, mm, yy = _z11_parts(dlivrydt_v)
                r['DLVRDD'] = dd; r['DLVRMM'] = mm; r['DLVRYR'] = yy

        # NOTETERM calc for LN accounts
        acty_v = _sv(r.get('ACTY'))
        if acty_v == 'LN':
            notemat_v  = _nv(r.get('NOTEMAT'))
            issuedt_v  = _nv(r.get('ISSUEDT'))
            notemat2   = _mmddyy_from_z11(notemat_v)  if notemat_v > 0 else None
            issuedat   = _mmddyy_from_z11(issuedt_v)  if issuedt_v > 0 else None
            issuyear   = issuedat.year  if issuedat else 0
            issumont   = issuedat.month if issuedat else 0
            facility_v = _nv(r.get('FACILITY'))
            cmmaturdt  = _sv(r.get('CMMATURDT')).strip()
            if facility_v == 34210 and cmmaturdt:
                try:
                    ntyr = (int(cmmaturdt[0:4]) - issuyear) * 12
                    ntmt = int(cmmaturdt[4:6]) - issumont
                    r['NOTETERM'] = ntyr + ntmt
                except Exception:
                    pass
            elif facility_v != 34210 and notemat2:
                ntyr = (notemat2.year  - issuyear) * 12
                ntmt =  notemat2.month - issumont
                r['NOTETERM'] = ntyr + ntmt

        # WFDATE for FACCODE 34333/34334
        faccode_v = _nv(r.get('FACCODE'))
        wfdate    = _sv(r.get('WFDATE')).strip()
        if faccode_v in (34333, 34334) and wfdate:
            try:
                wf_mm = int(wfdate[0:2]); wf_yy = int(wfdate[3:7])
                mm1 = wf_mm + 1; yy1 = wf_yy
                if mm1 > 12: mm1 = 1; yy1 = wf_yy + 1
                last = calendar.monthrange(yy1, mm1)[1]
                r['LASTMDATE'] = int(f"{yy1:04d}{mm1:02d}{last:02d}")
            except Exception:
                pass

        # ASSMDATE2 and nursing tag
        assmdate_v = _nv(r.get('ASSMDATE'))
        assmdate2  = _mmddyy_from_z11(assmdate_v) if assmdate_v > 0 else None
        nurs_tag_v = _sv(r.get('NURS_TAG'))
        nurs_tagdt = _nv(r.get('NURS_TAGDT'))
        acctstat_v = _sv(r.get('ACCTSTAT'))
        loantype_k = int(_nv(r.get('LOANTYPE')))
        if loantype_k not in HP and acctstat_v != 'S' and nurs_tag_v == 'K':
            r['ACCTSTAT'] = 'K'
            assmdate2 = _sas_date(nurs_tagdt) if nurs_tagdt > 0 else None

        r['ASSMYY'] = str(assmdate2.year)   if assmdate2 else '0000'
        r['ASSMMM'] = f"{assmdate2.month:02d}" if assmdate2 else '00'
        r['ASSMDD'] = f"{assmdate2.day:02d}"   if assmdate2 else '00'

        nurs_dt = _sas_date(nurs_tagdt) if nurs_tagdt > 0 else None
        r['NURSYY'] = str(nurs_dt.year)    if nurs_dt else '0000'
        r['NURSMM'] = f"{nurs_dt.month:02d}" if nurs_dt else '00'
        r['NURSDD'] = f"{nurs_dt.day:02d}"   if nurs_dt else '00'

        # ISSUEDAX
        issuedt_v2 = _nv(r.get('ISSUEDT'))
        issuedax   = _mmddyy_from_z11(issuedt_v2) if issuedt_v2 > 0 else None
        r['ISSXDAY']  = f"{issuedax.day:02d}"   if issuedax else '00'
        r['ISSXYEAR'] = str(issuedax.year)       if issuedax else '0000'
        r['ISSXMONT'] = f"{issuedax.month:02d}"  if issuedax else '00'

        # ACCRUAL1
        r['ACCRUAL1'] = int(_nv(r.get('ACCRUAL')) * 100) / 100

        # RJDATE
        rjdate = _nv(r.get('RJDATE'))
        if rjdate > 0:
            rd = _sas_date(rjdate)
            r['RJDATEDD'] = f"{rd.day:02d}"    if rd else '00'
            r['RJDATEMM'] = f"{rd.month:02d}"  if rd else '00'
            r['RJDATEYY'] = str(rd.year)        if rd else '0000'
        else:
            r['RJDATEDD'] = '00'; r['RJDATEMM'] = '00'; r['RJDATEYY'] = '0000'

        # FACCODE 34651/34652 + FRTDIDTIND
        if faccode_v in (34651, 34652) and _sv(r.get('FRTDIDTIND')) == 'Y':
            r['ISSUEDD'] = _sv(r.get('POSTDTDD'))
            r['ISSUEMM'] = _sv(r.get('POSTDTMM'))
            r['ISSUEYY'] = _sv(r.get('POSTDTYY'))

        # FULLREL dates
        freleas_v = _nv(r.get('FRELEAS'))
        fullrel_dt = _mmddyy_from_z11(freleas_v) if freleas_v > 0 else None
        r['FULLREL_DD'] = fullrel_dt.day   if fullrel_dt else 0
        r['FULLREL_MM'] = fullrel_dt.month if fullrel_dt else 0
        r['FULLREL_YY'] = fullrel_dt.year  if fullrel_dt else 0

        # Scale UNUSEAMT, CORGAMT, FWRITE_DOWN_BAL
        r['UNUSEAMT']        = _nv(r.get('UNUSEAMT'))        * 100
        r['CORGAMT']         = _nv(r.get('CORGAMT'))         * 100
        r['FWRITE_DOWN_BAL'] = _nv(r.get('WRITE_DOWN_BAL'))  * 100

        # ACCTSTAT='J' logic
        manual_rr_tag  = _sv(r.get('MANUAL_RR_TAG'))
        manual_rr_dt   = _nv(r.get('MANUAL_RR_DT'))
        akpk_ra_tag    = _sv(r.get('AKPK_RA_TAG'))
        akpk_ra_start  = _nv(r.get('AKPK_RA_START_DT'))
        akpk_ra_end    = _nv(r.get('AKPK_RA_END_DT'))
        acctstat_cur   = _sv(r.get('ACCTSTAT'))
        fac_cur        = _nv(r.get('FACILITY'))

        if fac_cur in (34331, 34332):
            if manual_rr_dt > 0:
                mrd = _sas_date(manual_rr_dt)
                if mrd:
                    if manual_rr_tag in ('UAJ','MAJ'):
                        end5 = _sas_date_num(_intnx_month_end(mrd, 5))
                        if manual_rr_dt <= CDATE_NUM <= end5 and acctstat_cur != 'S':
                            r['ACCTSTAT'] = 'J'
                    elif manual_rr_tag in ('UBJ','MBJ','FJ','MJ'):
                        end23 = _sas_date_num(_intnx_month_end(mrd, 23))
                        if manual_rr_dt <= CDATE_NUM <= end23 and acctstat_cur != 'S':
                            r['ACCTSTAT'] = 'J'
        else:
            if akpk_ra_start > 0 and len(akpk_ra_tag) >= 3:
                prefix2 = akpk_ra_tag[:2]; digit = akpk_ra_tag[2:3]
                if digit.isdigit() and int(digit) in range(1, 10):
                    if prefix2 in ('AU','CU'):
                        end5 = _sas_date_num(_intnx_month_end(
                            _sas_date(akpk_ra_start), 5))
                        if akpk_ra_start <= CDATE_NUM <= end5 and acctstat_cur != 'S':
                            r['ACCTSTAT'] = 'J'
                    elif prefix2 in ('BU','DU','AF'):
                        end_dt = _sas_date_num(_intnx_month_end(
                            _sas_date(akpk_ra_end) if akpk_ra_end > 0 else date.today(), 0))
                        if akpk_ra_start <= CDATE_NUM <= end_dt and acctstat_cur != 'S':
                            r['ACCTSTAT'] = 'J'

        # SMR 2022-3163 RRSTR/RRCMPL
        r['RRSTRDD']  = 0; r['RRSTRMM']  = 0; r['RRSTRYR']  = 0
        r['RRCMPLDD'] = 0; r['RRCMPLMM'] = 0; r['RRCMPLYR'] = 0
        lt = int(_nv(r.get('LOANTYPE')))
        if lt in (380, 381, 700, 705, 720, 725):
            idt = _nv(r.get('ISSUEDT'))
            if idt > 0:
                dd, mm, yy = _z11_parts(idt)
                r['RRSTRDD'] = dd; r['RRSTRMM'] = mm; r['RRSTRYR'] = yy
            cdt = _nv(r.get('CPNSTDTE'))
            if cdt > 0:
                dd, mm, yy = _z11_parts(cdt)
                r['RRCMPLDD'] = dd; r['RRCMPLMM'] = mm; r['RRCMPLYR'] = yy
        else:
            ddt = _nv(r.get('DLIVRYDT'))
            if ddt > 0:
                dd, mm, yy = _z11_parts(ddt)
                r['RRSTRDD'] = dd; r['RRSTRMM'] = mm; r['RRSTRYR'] = yy
            asm = _nv(r.get('ASSMDATE'))
            if asm > 0:
                dd, mm, yy = _z11_parts(asm)
                r['RRCMPLDD'] = dd; r['RRCMPLMM'] = mm; r['RRCMPLYR'] = yy

        # ESMR 2025-1525 LU_SOURCE
        lu_src = _sv(r.get('LU_SOURCE'))
        if lu_src and lu_src[0] == 'A':
            r['LU_SOURCE'] = lu_src if lu_src in ('A1','A2') else 'A2'
        elif len(lu_src) >= 2 and lu_src[1] == 'P':
            r['LU_SOURCE'] = lu_src[:2]

        out.append(r)
    return out

wriofac_dict = {(r['ACCTNO'], r['NOTENO']): r for r in wriofac.to_dicts()}
acctcred = pl.DataFrame(_apply_wriofac(acctcred.to_dicts(), wriofac_dict))
acctcred = acctcred.sort(['ACCTNO','NOTENO'])

# ---------------------------------------------------------------------------
# STEP 16: ODR7B - OD repaid 7B records (ACCTNO 3000000000-3999999999)
# ---------------------------------------------------------------------------
od_full = acctcred.filter(
    (pl.col('ACCTNO') >= 3000000000) & (pl.col('ACCTNO') <= 3999999999)
).sort('ACCTNO')

with open(REPAID7B_OUT, 'wb') as f_repaid7b:
    for r in od_full.filter(pl.col('REPAID') > 0).to_dicts():
        branch_v = _nv(r.get('BRANCH'))
        acctno_v = int(_nv(r.get('ACCTNO')))
        mrd      = _nv(r.get('MTD_REPAID_AMT'))

        for rtype, amt_field in [('10','MTD_REPAY_TYPE10_AMT'),
                                  ('20','MTD_REPAY_TYPE20_AMT'),
                                  ('30','MTD_REPAY_TYPE30_AMT')]:
            amt = _nv(r.get(amt_field))
            if amt > 0:
                buf = _build_buf(70)
                _put_rec(buf, 1,  _fmt_n(branch_v, 4))
                _put_rec(buf, 6,  _fmt_z(acctno_v, 11))
                _put_rec(buf, 19, REPTDAY)
                _put_rec(buf, 21, REPTMON)
                _put_rec(buf, 23, REPTYEAR)
                _put_rec(buf, 28, '1200')
                _put_rec(buf, 33, rtype)
                _put_rec(buf, 36, _fmt_f(amt, 16, 2))
                _put_rec(buf, 53, _fmt_f(mrd, 16, 2))
                f_repaid7b.write(bytes(buf) + b'\n')

# ---------------------------------------------------------------------------
# STEP 17: CREDITOD - closed OD accounts (in ODCR but not in acctcred OD)
# ---------------------------------------------------------------------------
odcr_full = con.execute(
    f"SELECT * FROM read_parquet('{CCRIS_DIR}/PREVODFT.parquet') "
    f"WHERE ACCTNO BETWEEN 3000000000 AND 3999999999"
).pl().unique(subset=['ACCTNO'], keep='first').sort('ACCTNO')

od_accts = set(od_full['ACCTNO'].to_list())
dispay_od = dispay.filter(
    (pl.col('ACCTNO') >= 3000000000) & (pl.col('ACCTNO') <= 3999999999)
).sort('ACCTNO').unique(subset=['ACCTNO'], keep='first')

closed_od = odcr_full.filter(~pl.col('ACCTNO').is_in(list(od_accts)))
closed_od = closed_od.join(dispay_od, on='ACCTNO', how='left', suffix='_DISP')

with open(CREDITOD_OUT, 'wb') as f_creditod:
    for r in closed_od.to_dicts():
        acctno_v  = int(_nv(r.get('ACCTNO')))
        repaid_v  = _nv(r.get('REPAID_DISP', r.get('REPAID',  0))) * 100
        disburse_v= _nv(r.get('DISBURSE_DISP', r.get('DISBURSE', 0))) * 100
        buf = _build_buf(250)
        _put_rec(buf, 1,   _fmt_n(_nv(r.get('FICODE', r.get('BRANCH', 0))), 9))
        _put_rec(buf, 10,  _fmt_n(_nv(r.get('APCODE', 0)), 3))
        _put_rec(buf, 13,  _fmt_n(acctno_v, 10))
        _put_rec(buf, 43,  _fmt_n(0, 10))           # NOTENO=0 for closed OD
        _put_rec(buf, 73,  REPTDAY)
        _put_rec(buf, 75,  REPTMON)
        _put_rec(buf, 77,  REPTYEAR)
        _put_rec(buf, 81,  _fmt_z(_nv(r.get('OUTSTAND', 0)), 16))
        _put_rec(buf, 97,  _fmt_z(0, 3))            # ARREARS=0
        _put_rec(buf, 100, _fmt_z(0, 3))            # INSTALM=0
        _put_rec(buf, 103, _fmt_z(0, 17))           # UNDRAWN=0
        _put_rec(buf, 120, 'S')                     # ACCTSTAT='S'
        _put_rec(buf, 121, _fmt_z(0, 5))            # NODAYS=0
        _put_rec(buf, 126, _fmt_n(0, 5))            # OLDBRH=0
        _put_rec(buf, 131, _fmt_z(0, 17))           # BILTOT=0
        _put_rec(buf, 148, _fmt_z(0, 17))           # ODXSAMT=0
        _put_rec(buf, 165, _fmt_n(0, 4))            # COSTCTR=0
        _put_rec(buf, 169, _fmt_s('', 13))          # AANO
        _put_rec(buf, 182, _fmt_n(_nv(r.get('FACILITY', 0)), 5))
        _put_rec(buf, 187, _fmt_n(_nv(r.get('FACCODE', 0)), 5))
        _put_rec(buf, 192, _fmt_z(disburse_v, 15))
        _put_rec(buf, 207, _fmt_z(repaid_v,   15))
        f_creditod.write(bytes(buf) + b'\n')

# ---------------------------------------------------------------------------
# STEP 18: ELN3 from ELDSTX3 file
# ---------------------------------------------------------------------------
eln3_rows = []
with open(ELDSTX3_FILE, 'r', encoding='ascii', errors='replace') as fe3:
    next(fe3, None)  # FIRSTOBS=2
    for line in fe3:
        line = line.rstrip('\n').ljust(40)
        aano_e  = line[0:13].upper().strip()
        apvby_e = line[32:34].upper().strip()
        eln3_rows.append({'AANO': aano_e, 'APVBY': apvby_e})

eln3 = pl.DataFrame(eln3_rows).sort('AANO')

# ---------------------------------------------------------------------------
# STEP 19: CREDIT final dataset preparation
# ---------------------------------------------------------------------------
credit = acctcred.clone()

def _add_credit_dates(rows):
    out = []
    for r in rows:
        r = dict(r)
        # ISSUED = MDY(ISSUEMM, ISSUEDD, ISSUEYY)
        try:
            iss = date(int(_nv(r.get('ISSUEYY'))),
                       int(_nv(r.get('ISSUEMM'))),
                       int(_nv(r.get('ISSUEDD'))))
            r['ISSUED'] = _sas_date_num(iss)
        except Exception:
            r['ISSUED'] = 0
        # AADATE parts
        aadate_v = _nv(r.get('AADATE'))
        if aadate_v > 0:
            ad = _sas_date(aadate_v)
            r['AADATEDD'] = f"{ad.day:02d}"    if ad else '00'
            r['AADATEMM'] = f"{ad.month:02d}"  if ad else '00'
            r['AADATEYY'] = str(ad.year)        if ad else '0000'
        else:
            r['AADATEDD'] = '00'; r['AADATEMM'] = '00'; r['AADATEYY'] = '0000'
        out.append(r)
    return out

credit = pl.DataFrame(_add_credit_dates(credit.to_dicts()))
credit = credit.sort('AANO')
credit = credit.join(eln3, on='AANO', how='left', suffix='_ELN3')

# Deduplicate and filter (FIRST.BRANCH/ACCTNO/AANO/COMMNO/NOTENO)
credit = credit.sort(['BRANCH','ACCTNO','AANO','COMMNO','NOTENO'])
credit = credit.unique(subset=['BRANCH','ACCTNO','AANO','COMMNO','NOTENO'], keep='first')

# Keep OD, or (COMMNO=0 AND IND='A') OR (COMMNO>0 AND IND='B') OR AANO non-blank
credit = credit.filter(
    (pl.col('ACTY') == 'OD') |
    ((pl.col('COMMNO') == 0) & (pl.col('IND') == 'A')) |
    ((pl.col('COMMNO') > 0)  & (pl.col('IND') == 'B')) |
    (pl.col('AANO').str.strip_chars() != '')
)

# ---------------------------------------------------------------------------
# STEP 20: %MONTHLY macro - if NOWK='4' write CCRIS.COLLATER / CCRIS.WRITOFF
# ---------------------------------------------------------------------------
if NOWK == '4':
    credit.write_parquet(str(CCRIS_DIR / 'COLLATER.parquet'))
    (credit.filter(pl.col('ACCTSTAT') == 'W')
           .select(['ACCTNO','NOTENO','ACCTSTAT','RESTRUCT'])
           .write_parquet(str(CCRIS_DIR / 'WRITOFF.parquet')))

# ---------------------------------------------------------------------------
# STEP 21: PROC SUMMARY equivalent - sum LIMTCURR by BRANCH/ACCTNO/AANO
# ---------------------------------------------------------------------------
credit = credit.sort(['BRANCH','ACCTNO','AANO'])

acctcd = credit.group_by(['BRANCH','ACCTNO','AANO']).agg(
    pl.col('LIMTCURR').sum().alias('LIMTCURR_SUM')
)

credit1 = credit.unique(subset=['BRANCH','ACCTNO','AANO'], keep='first')

acctcrex = credit1.join(acctcd, on=['BRANCH','ACCTNO','AANO'], how='left')
acctcrex = acctcrex.with_columns(
    pl.when(pl.col('LIMTCURR_SUM').is_not_null())
    .then(pl.col('LIMTCURR_SUM'))
    .otherwise(pl.col('LIMTCURR'))
    .alias('LIMTCURR')
).drop('LIMTCURR_SUM')

# ---------------------------------------------------------------------------
# STEP 22: LNACC4 merge
# ---------------------------------------------------------------------------
lnacc4 = con.execute(
    f"SELECT ACCTNO, AANO, PRODUCT, LNTERM, MNIAPDTE, REVIEWDT, "
    f"       FIRST_DISBDT, SETTLEMNTDT, TOTACBAL, MNIAPLMT, EXPRDATE "
    f"FROM read_parquet('{BNM_DIR}/LNACC4.parquet')"
).pl()

def _build_lnacc4(rows):
    out = []
    for r in rows:
        r = dict(r)
        r['ACCT_AANO']   = _sv(r.get('AANO'))
        r['ACCT_LNTYPE'] = _nv(r.get('PRODUCT'))
        r['ACCT_LNTERM'] = _nv(r.get('LNTERM'))
        r['ACCT_OUTBAL'] = _nv(r.get('TOTACBAL')) * 100
        r['ACCT_APLMT']  = _nv(r.get('MNIAPLMT')) * 100

        def _set_date_parts(fld_in, dd_key, mm_key, yy_key):
            val = _nv(r.get(fld_in))
            if val > 0:
                d = _mmddyy_from_z11(val)
                r[dd_key] = f"{d.day:02d}"   if d else '00'
                r[mm_key] = f"{d.month:02d}" if d else '00'
                r[yy_key] = str(d.year)       if d else '0000'
            else:
                r[dd_key] = '00'; r[mm_key] = '00'; r[yy_key] = '0000'

        _set_date_parts('REVIEWDT',    'ACCT_REVDD',  'ACCT_REVMM',  'ACCT_REVYY')
        _set_date_parts('FIRST_DISBDT','ACCT_1DISDD', 'ACCT_1DISMM', 'ACCT_1DISYY')
        _set_date_parts('SETTLEMNTDT', 'ACCT_SETDD',  'ACCT_SETMM',  'ACCT_SETYY')
        _set_date_parts('EXPRDATE',    'ACCT_EXPDD',  'ACCT_EXPMM',  'ACCT_EXPYY')
        _set_date_parts('MNIAPDTE',    'ACCT_ALDD',   'ACCT_ALMM',   'ACCT_ALYY')

        out.append({k: v for k, v in r.items()
                    if k not in ('AANO','PRODUCT','LNTERM','MNIAPDTE','REVIEWDT',
                                 'FIRST_DISBDT','SETTLEMNTDT','TOTACBAL','MNIAPLMT','EXPRDATE')})
    return out

lnacc4_rows = _build_lnacc4(lnacc4.to_dicts())
lnacc4_pl   = pl.DataFrame(lnacc4_rows).sort('ACCTNO')

acctcrex = acctcrex.sort('ACCTNO')
acctcrex = acctcrex.join(lnacc4_pl, on='ACCTNO', how='left', suffix='_L4')

# ---------------------------------------------------------------------------
# STEP 23: Write ACCTCRED file (LRECL=800)
# ---------------------------------------------------------------------------
with open(ACCTCRED_OUT, 'wb') as f_acctcred:
    for r in acctcrex.to_dicts():
        acctno_v   = int(_nv(r.get('ACCTNO')))
        auth_lim   = _nv(r.get('AUTHORISE_LIMIT')) * 100
        oper_lim   = _nv(r.get('OPERLIMT'))        * 100
        limtcurr   = _nv(r.get('LIMTCURR'))
        lmtamt_v   = _nv(r.get('LMTAMT'))
        mniaplmt_v = _nv(r.get('MNIAPLMT'))
        mniapdt_v  = _nv(r.get('MNIAPDT'))

        buf = _build_buf(800)
        _put_rec(buf, 1,   _fmt_n(_nv(r.get('FICODE')), 9))
        _put_rec(buf, 10,  _fmt_n(_nv(r.get('APCODE')), 3))
        _put_rec(buf, 13,  _fmt_n(acctno_v,             10))
        _put_rec(buf, 43,  _fmt_s(_sv(r.get('CURCODE')), 3))
        _put_rec(buf, 46,  _fmt_z(limtcurr, 24))
        _put_rec(buf, 70,  _fmt_z(limtcurr, 16))
        _put_rec(buf, 86,  _fmt_s(_sv(r.get('ISSUEDD')), 2))
        _put_rec(buf, 88,  _fmt_s(_sv(r.get('ISSUEMM')), 2))
        _put_rec(buf, 90,  _fmt_s(_sv(r.get('ISSUEYY')), 4))
        _put_rec(buf, 94,  _fmt_z(_nv(r.get('OLDBRH')), 5))
        _put_rec(buf, 99,  _fmt_z(lmtamt_v, 16))
        _put_rec(buf, 115, _fmt_s(_sv(r.get('LIABCODE')), 2))
        _put_rec(buf, 117, _fmt_n(_nv(r.get('COSTCTR')), 4))
        _put_rec(buf, 121, _fmt_s(_sv(r.get('AANO')), 13))
        _put_rec(buf, 134, _fmt_n(_nv(r.get('APPRLMAA')), 15))
        _put_rec(buf, 149, _fmt_s(_sv(r.get('AADATEYY')), 4))
        _put_rec(buf, 153, _fmt_s(_sv(r.get('AADATEMM')), 2))
        _put_rec(buf, 155, _fmt_s(_sv(r.get('AADATEDD')), 2))
        _put_rec(buf, 157, _fmt_s(_sv(r.get('RECONAME')), 60))
        _put_rec(buf, 217, _fmt_s(_sv(r.get('REFTYPE')), 70))
        _put_rec(buf, 287, _fmt_s(_sv(r.get('NMREF1')), 60))
        _put_rec(buf, 347, _fmt_s(_sv(r.get('NMREF2')), 60))
        _put_rec(buf, 407, _fmt_s(_sv(r.get('APVNME1')), 60))
        _put_rec(buf, 467, _fmt_s(_sv(r.get('APVNME2')), 60))
        _put_rec(buf, 527, _fmt_n(_nv(r.get('FACILITY')), 5))
        _put_rec(buf, 532, _fmt_s(_sv(r.get('NEWIC')), 20))
        _put_rec(buf, 552, _fmt_z(_nv(r.get('APPRXSC')), 15))
        _put_rec(buf, 570, _fmt_s(_sv(r.get('APVBY')), 5))
        _put_rec(buf, 576, _fmt_z(_nv(r.get('FXRATE')), 8))
        _put_rec(buf, 585, _fmt_n(_nv(r.get('FACCODE')), 5))
        _put_rec(buf, 592, _fmt_s(_sv(r.get('DESGRECO')), 30))
        _put_rec(buf, 622, _fmt_z(mniaplmt_v, 16))
        _put_rec(buf, 638, _fmt_z(mniapdt_v,  8))
        _put_rec(buf, 647, _fmt_n(_nv(r.get('ORIBRH')), 3))
        _put_rec(buf, 650, _fmt_n(_nv(r.get('PREACCT')), 10))
        _put_rec(buf, 660, _fmt_s(_sv(r.get('ACCT_AANO')), 13))
        _put_rec(buf, 673, _fmt_n(_nv(r.get('ACCT_LNTYPE')), 9))
        _put_rec(buf, 682, _fmt_n(_nv(r.get('ACCT_LNTERM')), 10))
        _put_rec(buf, 692, _fmt_z(_nv(r.get('ACCT_OUTBAL')), 16))
        _put_rec(buf, 708, _fmt_z(_nv(r.get('ACCT_APLMT')), 16))
        _put_rec(buf, 724, _fmt_s(_sv(r.get('ACCT_ALDD')), 2))
        _put_rec(buf, 726, _fmt_s(_sv(r.get('ACCT_ALMM')), 2))
        _put_rec(buf, 728, _fmt_s(_sv(r.get('ACCT_ALYY')), 4))
        _put_rec(buf, 732, _fmt_s(_sv(r.get('ACCT_REVDD')), 2))
        _put_rec(buf, 734, _fmt_s(_sv(r.get('ACCT_REVMM')), 2))
        _put_rec(buf, 736, _fmt_s(_sv(r.get('ACCT_REVYY')), 4))
        _put_rec(buf, 740, _fmt_s(_sv(r.get('ACCT_1DISDD')), 2))
        _put_rec(buf, 742, _fmt_s(_sv(r.get('ACCT_1DISMM')), 2))
        _put_rec(buf, 744, _fmt_s(_sv(r.get('ACCT_1DISYY')), 4))
        _put_rec(buf, 748, _fmt_s(_sv(r.get('ACCT_SETDD')), 2))
        _put_rec(buf, 750, _fmt_s(_sv(r.get('ACCT_SETMM')), 2))
        _put_rec(buf, 752, _fmt_s(_sv(r.get('ACCT_SETYY')), 4))
        _put_rec(buf, 756, _fmt_s(_sv(r.get('ACCT_EXPDD')), 2))
        _put_rec(buf, 758, _fmt_s(_sv(r.get('ACCT_EXPMM')), 2))
        _put_rec(buf, 760, _fmt_s(_sv(r.get('ACCT_EXPYY')), 4))
        _put_rec(buf, 764, _fmt_z(auth_lim, 16))
        _put_rec(buf, 780, _fmt_z(oper_lim, 16))
        f_acctcred.write(bytes(buf) + b'\n')

con.close()
print("EIBWCCR5 completed successfully.")
