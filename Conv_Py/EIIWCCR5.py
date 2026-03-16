#!/usr/bin/env python3
"""
Program  : EIIWCCR5.py
Purpose  : PIBB (Public Islamic Bank Berhad) CCRIS2 data preparation.
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
# DD2  DSN=SAP.PIBB.CCRIS2.ACCTCRED  DISP=(MOD,DELETE,DELETE)
# DD3  DSN=SAP.PIBB.CCRIS2.SUBACRED  DISP=(MOD,DELETE,DELETE)
# DD4  DSN=SAP.PIBB.CCRIS2.CREDITPO  DISP=(MOD,DELETE,DELETE)
# DD5  DSN=SAP.PIBB.CCRIS2.PROVISIO  DISP=(MOD,DELETE,DELETE)
# DD6  DSN=SAP.PIBB.CCRIS2.LEGALACT  DISP=(MOD,DELETE,DELETE)
# DD7  DSN=SAP.PIBB.CCRIS2.CREDITOD  DISP=(MOD,DELETE,DELETE)
# DD8  DSN=SAP.PIBB.CCRIS2.REPAID7B  DISP=(MOD,DELETE,DELETE)
# ---------------------------------------------------------------------------

import os
import sys
import math
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
DISPAY_DIR = BASE_DIR / "data" / "DISPAY"       # DISPAY.IDISPAYMTHnn
WOPS_DIR   = BASE_DIR / "data" / "WOPS"         # WOPS.IWOPOSnnWk
WOMV_DIR   = BASE_DIR / "data" / "WOMV"         # WOMV.ISUMOVnn
ODSQ_DIR   = BASE_DIR / "data" / "ODSQ"         # ODSQ.IODSQ
LNSQ_DIR   = BASE_DIR / "data" / "LNSQ"         # LNSQ.ILNSQ
IMPRLN_DIR = BASE_DIR / "data" / "IMPRLN"       # IMPRLN.LNHIST780
CRDTLN_DIR = BASE_DIR / "data" / "CRDTLN"       # CRDTLN.LNHIST310

# Fixed-width text input files
WRITOFF_FILE  = BASE_DIR / "data" / "txt" / "WRIOFAC.txt"   # SAP.BNM.PROGRAM(WRIOFAC)
ELDSTX3_FILE  = BASE_DIR / "data" / "txt" / "ELDSTX3.txt"   # SAP.PBB.ELDS.NEWBNM3.TEXT
APP7_FILE     = BASE_DIR / "data" / "txt" / "APP7.txt"       # SAP.PBB.ELDS.AABASEL.APP17.TEXT (concat)
APP10_FILE    = BASE_DIR / "data" / "txt" / "APP10.txt"      # SAP.PBB.ELDS.AABASEL.APP10.TEXT (concat)
ELDSRV12_FILE = BASE_DIR / "data" / "txt" / "ELDSRV12.txt"  # SAP.PBB.ELDS.BASEL.RV12.TEXT (concat)
ELDSRV5_FILE  = BASE_DIR / "data" / "txt" / "ELDSRV5.txt"   # SAP.PBB.ELDS.BASEL.RV05.TEXT (concat)
ELDSRV1_FILE  = BASE_DIR / "data" / "txt" / "ELDSRV1.txt"   # SAP.PBB.ELDS.BASEL.RV01.TEXT (concat)

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
RDATE     = REPTDATE.strftime("%d/%m/%Y")         # DDMMYY8. -> DD/MM/YYYY
CDATE_NUM = _reptdate_val                          # SAS numeric date (days since 1960-01-01)
REPTDAY   = f"{_day:02d}"
# SDATE = MDY(MONTHS,DAYS,YEARS) = REPTDATE itself; then PUT(SDATE,Z5.) = SAS date number zero-padded to 5
SDATE_NUM = _reptdate_val                          # used in arithmetic as &SDATE
SDATE     = f"{_reptdate_val:05d}"
MDATE     = f"{_reptdate_val:05d}"

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
         565, 566, 567, 568, 569, 570, 573, 900, 901, 906, 907, 909, 914, 915, 950}
PRDE  = {135, 136, 138, 182, 194, 196, 315, 320, 325, 330, 335, 340,
         355, 356, 357, 358, 391, 148, 174, 195, 461}
PRDF  = {110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 122, 126,
         141, 142, 143, 127, 129, 139, 140, 170, 172, 181, 194, 195, 196}
PRDG  = {6, 7, 61, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
         225, 226, 300, 301, 302, 304, 305, 309, 310, 315, 320, 325, 330, 335,
         340, 345, 350, 355, 356, 357, 358, 362, 363, 364, 365, 391, 504, 505,
         506, 509, 510, 515, 516, 900, 901, 902, 903, 904, 905, 909, 914, 915,
         919, 920, 950, 951}
PRDH  = {500, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529,
         532, 530, 555, 556, 559, 560, 561, 564, 565, 566, 567, 568, 569, 570, 573}
PRDJ  = set(range(4, 8)) | {15, 20} | set(range(25, 35)) | {60, 61, 62, 63} | \
        set(range(70, 79)) | {100, 108}

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
    mmddyyyy = s[:8]   # MMDDYYYY
    try:
        mm = int(mmddyyyy[0:2])
        dd = int(mmddyyyy[2:4])
        yy = int(mmddyyyy[4:8])
        return date(yy, mm, dd)
    except Exception:
        return None


def _z11_parts(val) -> tuple:
    """
    Return (dd_str2, mm_str2, yy_str4) extracted from PUT(val,Z11.) layout:
    positions (1-based): 1-2=MM, 3-4=DD, 5-8=YYYY  (MMDDYYYY in first 8 chars)
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
    import calendar
    total_months = d.year * 12 + d.month - 1 + n
    y = total_months // 12
    m = total_months % 12 + 1
    last_day = calendar.monthrange(y, m)[1]
    return date(y, m, last_day)


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
    s = '' if val is None else str(val)
    if upcase:
        s = s.upper()
    return s[:width].ljust(width)


def _put_rec(buf: bytearray, pos1: int, text: str):
    """Write text into bytearray buf at 1-based position pos1 (overwrites)."""
    start = pos1 - 1
    b = text.encode('ascii', errors='replace')
    for i, ch in enumerate(b):
        if start + i < len(buf):
            buf[start + i] = ch


def _build_buf(size: int) -> bytearray:
    return bytearray(b' ' * size)


# ---------------------------------------------------------------------------
# STEP 1: READ DATES  (DATA DATES / PROC PRINT)
# ---------------------------------------------------------------------------
# Already computed above into macro variables.

# ---------------------------------------------------------------------------
# STEP 2: BUILD LOAN DATASET
# ---------------------------------------------------------------------------
loan_dfs = []
for _src, _acctind in [(CCRISP_DIR / "LOAN.parquet", 'L'),
                       (CCRISP_DIR / "OVERDFS.parquet", 'D')]:
    _df = con.execute(f"SELECT * FROM read_parquet('{_src}')").pl()
    _df = _df.with_columns(pl.lit(_acctind).alias('ACCTIND'))
    loan_dfs.append(_df)

loan = pl.concat(loan_dfs, how='diagonal_relaxed')

def _derive_freleas_ode(df: pl.DataFrame) -> pl.DataFrame:
    """Derive FINREL* and ODE* date components from FRELEAS / EXODDATE."""
    rows = df.to_dicts()
    out = []
    for r in rows:
        # FRELEAS
        freleas = _nv(r.get('FRELEAS'))
        if freleas > 0:
            fd = _mmddyy_from_z11(freleas)
            r['FINRELDD'] = fd.day   if fd else 0
            r['FINRELMM'] = fd.month if fd else 0
            r['FINRELYY'] = fd.year  if fd else 0
        else:
            r['FINRELDD'] = 0; r['FINRELMM'] = 0; r['FINRELYY'] = 0

        # EXODDATE
        exoddate = _nv(r.get('EXODDATE'))
        if exoddate > 0:
            od = _mmddyy_from_z11(exoddate)
            r['ODEDD'] = od.day   if od else 0
            r['ODEMM'] = od.month if od else 0
            r['ODEYY'] = od.year  if od else 0
        else:
            r['ODEDD'] = 0; r['ODEMM'] = 0; r['ODEYY'] = 0

        # 2020-2189: LN_UTILISE_LOCAT_CD
        loantype  = _nv(r.get('LOANTYPE'))
        acctno    = _nv(r.get('ACCTNO'))
        noteno    = _nv(r.get('NOTENO'))
        issxdte   = _nv(r.get('ISSXDTE'))   # SAS date numeric
        mniapdte  = _nv(r.get('MNIAPDTE'))
        oct2019   = (date(2019, 10, 1) - date(1960, 1, 1)).days
        state     = _sv(r.get('STATE'))

        if (loantype == 392 or
            (2850000000 <= acctno <= 2859999999 and 40000 <= noteno <= 49999) or
            (3000000000 <= acctno <= 3999999999)):
            r['LN_UTILISE_LOCAT_CD'] = state if issxdte >= oct2019 else ''
        elif loantype != 392:
            r['LN_UTILISE_LOCAT_CD'] = (state if issxdte >= oct2019 and mniapdte >= oct2019
                                        else '')
        else:
            r['LN_UTILISE_LOCAT_CD'] = ''

        out.append(r)
    return pl.DataFrame(out)

loan = _derive_freleas_ode(loan)
loan = loan.sort(['ACCTNO'])

# ---------------------------------------------------------------------------
# STEP 3: LIMT (OD limit records)
# ---------------------------------------------------------------------------
limt_raw = con.execute(
    f"SELECT * FROM read_parquet('{LIMT_DIR}/OVERDFT.parquet') "
    f"WHERE LMTAMT > 1 AND APPRLIMT > 1"
).pl()
limt_raw = limt_raw.sort(['ACCTNO', 'LMTRATE'], descending=[False, True])

def _derive_limt(df: pl.DataFrame) -> pl.DataFrame:
    rows = df.to_dicts()
    out = []
    for r in rows:
        lmtenddt = _nv(r.get('LMTENDDT'))
        lmtdte   = _mmddyy_from_z11(lmtenddt)
        r['LMTDTE']  = (lmtdte - date(1960, 1, 1)).days if lmtdte else 0
        r['LMTDATE'] = r['LMTDTE']

        lmtindex = _nv(r.get('LMTINDEX'))
        fdrcno   = _nv(r.get('FDRCNO'))
        if lmtindex in (1, 30):        r['SEQ'] = 1
        elif lmtindex in (38, 39):     r['SEQ'] = 2
        elif lmtindex == 52:           r['SEQ'] = 3
        elif lmtindex == 0 and fdrcno > 0: r['SEQ'] = 4
        elif lmtindex in (18, 19):     r['SEQ'] = 5
        elif lmtindex == 0:            r['SEQ'] = 6
        else:                          r['SEQ'] = 7
        out.append(r)
    return pl.DataFrame(out)

limt_raw = _derive_limt(limt_raw)
# DROP INTERDUE, CLIMATE_PRIN_TAXONOMY_CLASS
for _col in ['INTERDUE', 'CLIMATE_PRIN_TAXONOMY_CLASS']:
    if _col in limt_raw.columns:
        limt_raw = limt_raw.drop(_col)
limt_raw = limt_raw.sort(['ACCTNO', 'SEQ'])
limt = limt_raw.unique(subset=['ACCTNO'], keep='first')

# CCRIS.PREVODFT - OD closed records
odcr = con.execute(
    f"SELECT ACCTNO, APPRLIMT, OUTSTAND FROM read_parquet('{CCRIS_DIR}/PREVODFT.parquet') "
    f"WHERE ACCTNO BETWEEN 3000000000 AND 3999999999"
).pl()
odcr = odcr.rename({'APPRLIMT': 'PREVALIMT', 'OUTSTAND': 'PREVOBAL'})
odcr = odcr.sort(['ACCTNO'])
odcr = odcr.unique(subset=['ACCTNO'], keep='first')

# Merge LIMT and ODCR into LOAN
loan = loan.join(limt, on='ACCTNO', how='left', suffix='_LIMT')
loan = loan.join(odcr, on='ACCTNO', how='left')

# ---------------------------------------------------------------------------
# STEP 4: ELDS merge
# ---------------------------------------------------------------------------
elds = con.execute(
    f"SELECT AANO, SPAAMT, DESGRECO FROM read_parquet('{ELDS_DIR}/ELNMAX.parquet') "
    f"WHERE AANO IS NOT NULL AND AANO != ''"
).pl().sort('AANO').unique(subset=['AANO'], keep='first')

loan = loan.sort('AANO')
loan = loan.join(elds, on='AANO', how='left')

def _fix_spaamt(df: pl.DataFrame) -> pl.DataFrame:
    rows = df.to_dicts()
    out = []
    for r in rows:
        spa = _nv(r.get('SPAAMT'))
        if spa in (None, 0):
            cdolarv  = _nv(r.get('CDOLARV'))
            cprvdolv = _nv(r.get('CPRVDOLV'))
            origval  = _nv(r.get('ORIGVAL'))
            if cdolarv not in (None, 0):   r['SPAAMT'] = cdolarv
            elif cprvdolv not in (None, 0): r['SPAAMT'] = cprvdolv
            elif origval not in (None, 0):  r['SPAAMT'] = origval
            else:                           r['SPAAMT'] = 0
        out.append(r)
    return pl.DataFrame(out)

loan = _fix_spaamt(loan)
loan = loan.sort(['ACCTNO', 'NOTENO'])

# ---------------------------------------------------------------------------
# STEP 5: DISPAY merge
# ---------------------------------------------------------------------------
dispay = con.execute(
    f"SELECT ACCTNO, NOTENO, REPAID, DISBURSE "
    f"FROM read_parquet('{DISPAY_DIR}/IDISPAYMTH{REPTMON}.parquet')"
).pl().sort(['ACCTNO', 'NOTENO'])

loan = loan.join(dispay, on=['ACCTNO', 'NOTENO'], how='left',
                 suffix='_DISP')
# prefer dispay REPAID/DISBURSE over existing
for col in ['REPAID', 'DISBURSE']:
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
    f"FROM read_parquet('{WOPS_DIR}/IWOPOS{REPTMON}{NOWK}.parquet')"
).pl().sort(['ACCTNO', 'NOTENO'])

sumov = con.execute(
    f"SELECT ACCTNO, NOTENO, BDR_MTH, SC_MTH, RC_MTH, NAI_MTH "
    f"FROM read_parquet('{WOMV_DIR}/ISUMOV{REPTMON}.parquet')"
).pl().sort(['ACCTNO', 'NOTENO'])

odsq_df = con.execute(
    f"SELECT ACCTNO, WOAMT, ISWO, SPWO "
    f"FROM read_parquet('{ODSQ_DIR}/IODSQ.parquet')"
).pl()
lnsq_df = con.execute(
    f"SELECT ACCTNO, NOTENO, WOAMT, ISWO, SPWO, WODATE "
    f"FROM read_parquet('{LNSQ_DIR}/ILNSQ.parquet')"
).pl()
wosq = pl.concat([odsq_df, lnsq_df], how='diagonal_relaxed').sort(['ACCTNO', 'NOTENO'])

wof = wopos.join(sumov, on=['ACCTNO', 'NOTENO'], how='outer_coalesce')
wof = wof.join(wosq, on=['ACCTNO', 'NOTENO'], how='outer_coalesce')
wof = wof.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

loan = loan.join(wof, on=['ACCTNO', 'NOTENO'], how='left', suffix='_WOF')

def _apply_wof(df: pl.DataFrame) -> pl.DataFrame:
    rows = df.to_dicts()
    out = []
    for r in rows:
        actowe = r.get('ACTOWE')
        if actowe is not None and not (isinstance(actowe, float) and math.isnan(actowe)):
            r['LEDGBAL'] = actowe

        reptmon_num = int(REPTMON)
        if reptmon_num in (3, 6, 9, 12):
            r['TOTWOF'] = _nv(r.get('ISWO'))
            r['SPWOF']  = _nv(r.get('SPWO'))
        else:
            r['WOAMT'] = None

        # ORIWODATE from APPRDATE
        apprdate = _nv(r.get('APPRDATE'))
        if apprdate > 0:
            apprdt = _mmddyy_from_z11(apprdate)
            if apprdt:
                r['ORIWODATE'] = int(f"{apprdt.year}{apprdt.month:02d}{apprdt.day:02d}")
            else:
                r['ORIWODATE'] = 0
        else:
            r['ORIWODATE'] = 0

        # LASTMDATE from WODATE
        wodate_val = _nv(r.get('WODATE'))
        if wodate_val > 0:
            wd = _sas_date(wodate_val)
            if wd:
                r['LASTMDATE'] = int(f"{wd.year}{wd.month:02d}{wd.day:02d}")
            else:
                r['LASTMDATE'] = 0
        else:
            r['LASTMDATE'] = 0

        # BALANCE override for write-down accounts
        acty    = _sv(r.get('ACTY'))
        product = _nv(r.get('PRODUCT'))
        loantype = _nv(r.get('LOANTYPE'))
        wdb     = _nv(r.get('WRITE_DOWN_BAL'))
        if wdb in (0, None) or (isinstance(wdb, float) and math.isnan(wdb)):
            if (acty == 'OD' and 30 <= product <= 34) or \
               (acty == 'LN' and 600 <= loantype <= 699):
                r['BALANCE'] = actowe if actowe else r.get('BALANCE')

        out.append(r)
    return pl.DataFrame(out)

loan = _apply_wof(loan)

# ---------------------------------------------------------------------------
# STEP 7: TRAN780 / LNHIST310
# ---------------------------------------------------------------------------
tran780 = con.execute(
    f"SELECT ACCTNO, NOTENO, HCURBAL, EFFDATE "
    f"FROM read_parquet('{IMPRLN_DIR}/LNHIST780.parquet')"
).pl().sort(['ACCTNO', 'NOTENO', 'EFFDATE'], descending=[False, False, True])
tran780 = tran780.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

lnhist310 = con.execute(
    f"SELECT ACCTNO, NOTENO, TOTAMT, CHANNEL, FRTDISCODE, LSTDISCODE, "
    f"POSTDATE1, FRTDIDTIND "
    f"FROM read_parquet('{CRDTLN_DIR}/LNHIST310.parquet')"
).pl()

loan = loan.join(tran780, on=['ACCTNO', 'NOTENO'], how='left', suffix='_T780')
loan = loan.join(lnhist310, on=['ACCTNO', 'NOTENO'], how='left', suffix='_310')

def _derive_stp_and_dates(df: pl.DataFrame) -> pl.DataFrame:
    rows = df.to_dicts()
    out = []
    for r in rows:
        product = _nv(r.get('PRODUCT'))
        channel = _nv(r.get('CHANNEL'))

        if product in HP and channel == 500:
            r['STP'] = 'Y'
        elif product not in HP and channel == 5:
            r['STP'] = 'Y'
        else:
            r['STP'] = 'N'

        effdate = _nv(r.get('EFFDATE'))
        if effdate > 0:
            ed = _sas_date(effdate)
            r['IMPAIRYY'] = str(ed.year)        if ed else ''
            r['IMPAIRMM'] = f"{ed.month:02d}"   if ed else ''
            r['IMPAIRDD'] = f"{ed.day:02d}"     if ed else ''
        else:
            r['IMPAIRYY'] = ''; r['IMPAIRMM'] = ''; r['IMPAIRDD'] = ''

        postdate1 = _nv(r.get('POSTDATE1'))
        if postdate1 > 0:
            pd1 = _sas_date(postdate1)
            r['POSTDTYY'] = str(pd1.year)       if pd1 else ''
            r['POSTDTMM'] = f"{pd1.month:02d}"  if pd1 else ''
            r['POSTDTDD'] = f"{pd1.day:02d}"    if pd1 else ''
        else:
            r['POSTDTYY'] = ''; r['POSTDTMM'] = ''; r['POSTDTDD'] = ''

        out.append(r)
    return pl.DataFrame(out)

loan = _derive_stp_and_dates(loan)
loan = loan.sort(['ACCTNO', 'COMMNO'])

# ---------------------------------------------------------------------------
# STEP 8: MAIN ACCTCRED DATA STEP
# ---------------------------------------------------------------------------
# This large step processes each row and writes SUBACRED, CREDITPO, PROVISIO, LEGALACT

f_subacred = open(SUBACRED_OUT, 'wb')
f_creditpo = open(CREDITPO_OUT, 'wb')
f_provisio = open(PROVISIO_OUT, 'wb')
f_legalact = open(LEGALACT_OUT, 'wb')

acctcred_rows = []   # accumulate for ACCTCRED file (written later)

for row in loan.to_dicts():

    # Exclude specific known bad accounts
    acctno = int(_nv(row.get('ACCTNO')))
    noteno = int(_nv(row.get('NOTENO')))
    if ((acctno == 8027370307 and noteno == 10) or
        (acctno == 8124498008 and noteno == 10010) or
        (acctno == 8124948801 and noteno == 10)):
        continue

    r = dict(row)   # mutable copy

    # --- Init variables ---
    r['OLDBRH']  = 0
    r['ARREARS'] = 0
    r['NODAYS']  = 0
    r['XNODAYS'] = 0
    r['INSTALM'] = 0
    r['UNDRAWN'] = 0
    r['FICODE']  = _nv(r.get('BRANCH'))

    r.setdefault('RRPAYCNT', 0)
    r.setdefault('RRTAG', '')

    for dt_field in ['PAYDD','PAYMM','PAYYR','GRANTDD','GRANTMM','GRANTYR',
                     'LNMATDD','LNMATMM','LNMATYR','ORMATDD','ORMATMM','ORMATYR',
                     'DLVRDD','DLVRMM','DLVRYR','BILDUEDD','BILDUEMM','BILDUEYR',
                     'RRCNTDD','RRCNTMM','RRCNTYR','RRCOMPDD','RRCOMPMM','RRCOMPYR']:
        r.setdefault(dt_field, 0)
    r.setdefault('COLLMM', 0)
    r.setdefault('COLLYR', 0)
    r.setdefault('FCONCEPT_VAL', 0)

    # Currency FX
    curcode  = _sv(r.get('CURCODE'))
    forate   = _nv(r.get('FORATE'))
    spotrate = _nv(r.get('SPOTRATE'))
    if curcode != 'MYR' and forate in (0, None):
        r['FORATE'] = spotrate
        forate = spotrate
    if curcode not in ('MYR', ''):
        r['CURBAL']          = round(_nv(r.get('CURBAL'))          * forate, 2)
        r['FEEAMT']          = round(_nv(r.get('FEEAMT'))          * forate, 2)
        r['CCRIS_INSTLAMT']  = round(_nv(r.get('CCRIS_INSTLAMT'))  * forate, 2)

    acty    = _sv(r.get('ACTY'))
    product = int(_nv(r.get('PRODUCT')))
    loantype = int(_nv(r.get('LOANTYPE')))

    # -----------------------------------------------------------------------
    # OD BRANCH
    # -----------------------------------------------------------------------
    if acty == 'OD':
        ccricode  = _sv(r.get('CCRICODE'))
        rrind     = _sv(r.get('RRIND'))
        rrmaindt  = _nv(r.get('RRMAINDT'))
        exoddate  = _nv(r.get('EXODDATE'))
        tempoddt  = _nv(r.get('TEMPODDT'))
        curbal    = _nv(r.get('CURBAL'))
        apprlimt  = _nv(r.get('APPRLIMT'))
        odintacc  = _nv(r.get('ODINTACC'))
        interdue  = _nv(r.get('INTERDUE'))
        feeamt    = _nv(r.get('FEEAMT'))
        odxsamt   = _nv(r.get('ODXSAMT'))
        lmtamt    = _nv(r.get('LMTAMT'))
        repaid    = _nv(r.get('REPAID'))
        disburse  = _nv(r.get('DISBURSE'))
        rebate    = _nv(r.get('REBATE'))
        balance   = _nv(r.get('BALANCE'))
        prevalimt = _nv(r.get('PREVALIMT'))
        prevobal  = _nv(r.get('PREVOBAL'))
        closedt   = _nv(r.get('CLOSEDT'))
        openind   = _sv(r.get('OPENIND'))
        riskcode  = _sv(r.get('RISKCODE'))
        odstatus  = _sv(r.get('ODSTATUS'))
        odlmtdd   = _sv(r.get('ODLMTDD'))
        odlmtmm   = _sv(r.get('ODLMTMM'))
        odlmtyy   = _sv(r.get('ODLMTYY'))
        lmtindex  = _nv(r.get('LMTINDEX'))
        lmtrate   = _nv(r.get('LMTRATE'))
        lmtbaser  = _nv(r.get('LMTBASER'))
        lmtid     = _nv(r.get('LMTID'))
        odbasert  = _nv(r.get('ODBASERT'))
        odtempadj = _nv(r.get('ODTEMPADJ'))
        fdrcno    = _nv(r.get('FDRCNO'))
        odplan    = _nv(r.get('ODPLAN'))
        oriproduct = _nv(r.get('ORIPRODUCT'))
        wostat    = _sv(r.get('WOSTAT'))
        wdb       = _nv(r.get('WRITE_DOWN_BAL'))
        totiis    = _nv(r.get('TOTIIS'))
        sector1   = _sv(r.get('SECTOR1'))
        rrcount   = _nv(r.get('RRCOUNT'))
        rrcompldt = _nv(r.get('RRCOMPLDT'))
        censust   = _nv(r.get('CENSUST'))

        r['FACILITY']  = int(format_odfacility(product).replace('', '0') if not format_odfacility(product) else format_odfacility(product))
        r['BORSTAT']   = rrind
        r['ASSMDATE']  = rrmaindt
        r['SYNDICAT']  = ' '
        r['COLLVAL']   = _nv(r.get('REALISAB'))
        r['PURPOSES']  = ccricode[:4].ljust(4) if ccricode else '    '
        if r['COLLVAL'] in (0, None): r['COLLVAL'] = 0

        r['ODXSAMT']  = odxsamt  * 100
        r['LMTAMT']   = lmtamt   * 100
        r['REPAID']   = repaid   * 100
        r['DISBURSE'] = disburse * 100
        r['REBATE']   = rebate   * 100
        r['BILTOT']   = 0
        r['LIMTCURR'] = apprlimt * 100
        r['LOANTYPE'] = product
        r['APCODE']   = product
        r['NOTENO']   = 0
        r['SECTOR']   = sector1

        if balance < 0:
            r['UNDRAWN'] = apprlimt
        else:
            r['UNDRAWN'] = apprlimt + (-1) * balance
        if balance > apprlimt:
            r['UNDRAWN'] = 0
        undrawn1 = round(r['UNDRAWN'], 2)
        r['UNDRAWN'] = round(undrawn1, 2) * 100

        if product in (124, 165, 191): r['FACILITY'] = 34111
        else:                          r['FACILITY'] = 34110
        if product == 73:              r['FACCODE']  = 34110
        if apprlimt <= 1:              r['FACILITY'] = 34112
        if product in (177, 178):      r['FACILITY'] = 34220
        if r['FACILITY'] == 34112:     r['FACCODE']  = 34120

        if product == 114:
            r['SPECIALF'] = '12'; r['FCONCEPT_VAL'] = 60
            r['NOTETERM'] = 12;   r['PAYFREQC'] = '20'
        elif product in range(160, 168):
            r['SPECIALF'] = '00'; r['FCONCEPT_VAL'] = 10
            r['NOTETERM'] = 12;   r['PAYFREQC'] = '20'
        else:
            r['SPECIALF'] = '00'; r['FCONCEPT_VAL'] = 51
            r['NOTETERM'] = 12;   r['PAYFREQC'] = '20'

        if loantype in (133, 134, 177, 178):
            r['FCONCEPT_VAL'] = format_odfconcept(loantype)
            r['FACILITY'] = 34220
            r['FACCODE']  = 34220

        if r.get('FEEAMT') is None:   r['FEEAMT']   = 0
        if r.get('INTERDUE') is None: r['INTERDUE'] = 0
        feeamt   = _nv(r.get('FEEAMT'))
        interdue = _nv(r.get('INTERDUE'))

        # CENSUS
        censust_str = str(int(censust)) if censust else ''
        if len(censust_str.lstrip('0')) == 5:
            r['CENSUS'] = censust * 0.01
        else:
            r['CENSUS'] = censust

        # OD arrears
        if (exoddate != 0 or tempoddt != 0) and curbal <= 0:
            exoddt_d = _mmddyy_from_z11(exoddate) if exoddate else None
            tempdt_d = _mmddyy_from_z11(tempoddt) if tempoddt else None
            exoddt_n = (exoddt_d - date(1960,1,1)).days if exoddt_d else 0
            tempdt_n = (tempdt_d - date(1960,1,1)).days if tempdt_d else 0

            if exoddt_d and tempdt_d:
                oddate = exoddt_d if exoddt_d < tempdt_d else tempdt_d
            elif exoddate == 0:
                oddate = tempdt_d
            else:
                oddate = exoddt_d

            if oddate:
                oddays_n = (oddate - date(1960,1,1)).days
                r['PAYDD']  = oddate.day
                r['PAYMM']  = oddate.month
                r['PAYYR']  = oddate.year
                nodays = SDATE_NUM - oddays_n + 1
                r['NODAYS'] = nodays
                if nodays > 0:
                    r['ARREARS'] = math.floor(nodays / 30.00050)
                    r['INSTALM'] = math.ceil(nodays  / 30.00050)

        if rrmaindt > 0:
            dd, mm, yy = _z11_parts(rrmaindt)
            r['DLVRDD'] = dd; r['DLVRMM'] = mm; r['DLVRYR'] = yy

        r['RRPAYCNT'] = rrcount

        # Restructure tag for OD
        ri1 = rrind[0] if len(rrind) > 0 else ''
        ri2 = rrind[1] if len(rrind) > 1 else ''
        if ri1 in 'CTRS' and ri2 in '123456789':
            r['RRTAG']    = rrind
            r['RESTRUCT'] = ri1
            if rrcompldt > 0:
                dd, mm, yy = _z11_parts(rrcompldt)
                r['RRCOMPDD'] = dd; r['RRCOMPMM'] = mm; r['RRCOMPYR'] = yy
                r['RESTRUCT'] = ''

        # Classification OD
        if riskcode == '1':   r['CLASSIFI'] = 'C'
        elif riskcode == '2': r['CLASSIFI'] = 'S'
        elif riskcode == '3': r['CLASSIFI'] = 'D'
        elif riskcode == '4': r['CLASSIFI'] = 'B'
        elif odstatus == 'AC' or (exoddate == 0 and tempoddt == 0):
            r['CLASSIFI'] = 'P'

        if riskcode > '0' or loantype in range(30, 35):
            r['IMPAIRED'] = 'Y'
        else:
            r['IMPAIRED'] = 'N'

        # Liability codes
        col1 = _sv(r.get('COL1')); col2 = _sv(r.get('COL2'))
        col3 = _sv(r.get('COL3')); col4 = _sv(r.get('COL4'))
        col5 = _sv(r.get('COL5'))
        liabs = [col1[0:2], col1[3:5], col2[0:2], col2[3:5],
                 col3[0:2], col3[3:5], col4[0:2], col4[3:5],
                 col5[0:2], col5[3:5]]
        r['LIABCODE'] = next((lb for lb in liabs if lb.strip() != ''), ' ')

        r['ISSUEDD'] = odlmtdd; r['ISSUEMM'] = odlmtmm; r['ISSUEYY'] = odlmtyy
        r['ACCTSTAT'] = 'O'

        curbal = curbal * -1
        r['CURBAL'] = curbal
        r['CURBALORI'] = curbal

        if odintacc > 0:
            r['OUTSTAND']  = curbal + odintacc
            r['INTERDUE']  = interdue + odintacc
        else:
            r['OUTSTAND']  = curbal

        outstand = _nv(r.get('OUTSTAND'))
        if round(outstand, 2) <= 0:
            r['INTERDUE'] = 0; r['FEEAMT'] = 0
        r['CURBAL'] = _nv(r.get('OUTSTAND')) - _nv(r.get('INTERDUE')) - _nv(r.get('FEEAMT'))

        # BNM TAXONOMY & CCRIS ENHANCEMENT (2012-0752)
        r['SYNDICAT']      = 'N'
        r['SPECIALF']      = format_odfundsch(product)
        r['FCONCEPT_VAL']  = format_odfconcept(product)

        if product in range(30, 35):
            r['FCONCEPT_VAL'] = format_odfconcept(oriproduct)
            r['ORICODE']      = oriproduct
            if r['ORICODE'] == 0 and product in (32, 33):
                r['FCONCEPT_VAL'] = 10
            if product == 34:
                r['FACILITY'] = 34220; r['FACCODE'] = 34220

            # P-I-O for inactive
            if wdb not in (0, None):
                r['CURBAL']   = wdb; r['OUTSTAND'] = wdb
                r['FEEAMT']   = 0;   r['INTERDUE'] = 0; r['TOTIIS']  = 0

            if   wostat == 'W' and wdb <= 0: r['ACCTSTAT'] = wostat
            elif wostat == 'W' and wdb >  0: r['ACCTSTAT'] = 'P'
            elif wostat == 'P':              r['ACCTSTAT'] = wostat
            elif wostat == 'C':              r['ACCTSTAT'] = 'X'
        else:
            if wostat == 'R': r['ACCTSTAT'] = 'O'

        r['OUTSTAND'] = _nv(r.get('OUTSTAND')) * 100

        if lmtid < 1:
            r['INTRATE'] = (odbasert + odtempadj) * 100
        else:
            r['INTRATE'] = lmtrate * 100

        # TYPEPRC for OD
        if lmtindex in (18, 19):      r['TYPEPRC'] = '59'
        elif lmtindex in (1, 30) and lmtrate >= lmtbaser:  r['TYPEPRC'] = '41'
        elif lmtindex in (1, 30) and lmtrate < lmtbaser:   r['TYPEPRC'] = '42'
        elif lmtindex in (38, 39):    r['TYPEPRC'] = '43'
        elif lmtindex in (0,) and fdrcno > 0:   r['TYPEPRC'] = '68'
        elif lmtindex in (0,) and lmtamt in (0, None):
            if odplan in (110, 111, 114):              r['TYPEPRC'] = '43'
            elif odplan in (100, 101) or 103 <= odplan <= 107: r['TYPEPRC'] = '41'
            elif odplan in (106, 998):                 r['TYPEPRC'] = '00'
            elif odplan in (120, 121):                 r['TYPEPRC'] = '44'
            else:                                      r['TYPEPRC'] = '79'
        elif lmtindex in (0,) and lmtamt > 0:  r['TYPEPRC'] = '59'
        elif lmtindex == 52:           r['TYPEPRC'] = '44'
        else:                          r['TYPEPRC'] = '79'

        outstand = _nv(r.get('OUTSTAND'))
        if ((apprlimt in (None, 0) and round(outstand, 1) <= 0 and round(prevobal, 1) > 0) or
            (apprlimt in (None, 0) and round(outstand, 1) <= 0 and prevalimt > 0) or
            (closedt not in (None, 0) and openind in ('B', 'C', 'P'))):
            if not (riskcode in ('1', '2', '3', '4') or product in range(30, 35)) or \
               openind in ('B', 'C', 'P'):
                r['ACCTSTAT'] = 'S'; r['ARREARS'] = 0
                r['INSTALM']  = 0;   r['NODAYS']  = 0

    # -----------------------------------------------------------------------
    # LOAN BRANCH
    # -----------------------------------------------------------------------
    else:
        collval   = _nv(r.get('APPVALUE'))
        balance   = _nv(r.get('BALANCE'))
        cavaiamt  = _nv(r.get('CAVAIAMT'))
        repaid    = _nv(r.get('REPAID'))
        disburse  = _nv(r.get('DISBURSE'))
        biltot    = _nv(r.get('BILTOT'))
        apprlimt  = _nv(r.get('APPRLIMT'))
        netproc   = _nv(r.get('NETPROC'))
        intrate   = _nv(r.get('INTRATE'))
        ntint     = _sv(r.get('NTINT'))
        riskrate  = _sv(r.get('RISKRATE'))
        nplind    = _sv(r.get('NPLIND'))
        loanstat  = _nv(r.get('LOANSTAT'))
        faccode   = _nv(r.get('FACCODE'))
        paidind   = _sv(r.get('PAIDIND'))
        borstat   = _sv(r.get('BORSTAT'))
        modeldes  = _sv(r.get('MODELDES'))
        issuedt   = _nv(r.get('ISSUEDT'))
        curbal    = _nv(r.get('CURBAL'))
        rebate    = _nv(r.get('REBATE'))
        intearn4  = _nv(r.get('INTEARN4'))
        intearn   = _nv(r.get('INTEARN'))
        intamt    = _nv(r.get('INTAMT'))
        billcnt   = _nv(r.get('BILLCNT'))
        bldate    = _nv(r.get('BLDATE'))
        dlvdate   = _nv(r.get('DLVDATE'))
        exprdate  = _nv(r.get('EXPRDATE'))
        maturedt  = _nv(r.get('MATUREDT'))
        dlivrydt  = _nv(r.get('DLIVRYDT'))
        firsildte = _nv(r.get('FIRSILDTE'))
        currildte = _nv(r.get('CURRILDTE'))
        numcpns   = _nv(r.get('NUMCPNS'))
        fclosuredt = _nv(r.get('FCLOSUREDT'))
        assmdate  = _nv(r.get('ASSMDATE'))
        cpnstdte  = _nv(r.get('CPNSTDTE'))
        nxbildt   = _nv(r.get('NXBILDT'))
        collyear  = _nv(r.get('COLLYEAR'))
        bldate_n  = bldate
        pzipcode  = _nv(r.get('PZIPCODE'))
        lsttrncd  = _nv(r.get('LSTTRNCD'))
        orgtype   = _sv(r.get('ORGTYPE'))
        payfreq   = _sv(r.get('PAYFREQ'))
        cproduct  = _sv(r.get('CPRODUCT'))
        flag1     = _sv(r.get('FLAG1'))
        score1    = _sv(r.get('SCORE1'))
        mortgind  = _sv(r.get('MORTGIND'))
        score2    = _sv(r.get('SCORE2'))
        contrtype = _sv(r.get('CONTRTYPE'))
        rleasamt  = _nv(r.get('RLEASAMT'))
        commno    = _nv(r.get('COMMNO'))
        cusedamt  = _nv(r.get('CUSEDAMT'))
        rrcycle   = _sv(r.get('RRCYCLE'))
        user5     = _sv(r.get('USER5'))
        issxdte   = _nv(r.get('ISSXDTE'))
        ind       = _sv(r.get('IND'))
        ntindex   = _nv(r.get('NTINDEX'))
        spread    = _nv(r.get('SPREAD'))
        cfindex   = _nv(r.get('CFINDEX'))
        costfund  = _nv(r.get('COSTFUND'))
        akpk_status = _sv(r.get('AKPK_STATUS'))
        wdb       = _nv(r.get('WRITE_DOWN_BAL'))
        wostat    = _sv(r.get('WOSTAT'))
        totiis    = _nv(r.get('TOTIIS'))
        oldnotedayarr = _nv(r.get('OLDNOTEDAYARR'))
        mordayarr = r.get('MORDAYARR')
        mo_instl_arr = r.get('MO_INSTL_ARR')
        lmostdte  = _nv(r.get('MOSTDTE'))
        moenddte  = _nv(r.get('MOENDDTE'))
        lmo_tag   = _sv(r.get('LMO_TAG'))
        nurs_tag  = _sv(r.get('NURS_TAG'))
        nurs_tagdt = _nv(r.get('NURS_TAGDT'))
        tfa_nurs_tag    = _sv(r.get('TFA_NURS_TAG'))
        tfa_nurs_tag_dt = _nv(r.get('TFA_NURS_TAG_DT'))
        census    = _nv(r.get('CENSUS'))
        freleas   = _nv(r.get('FRELEAS'))
        cagamas   = 0
        realisab  = _nv(r.get('REALISAB'))
        apprlim2  = _nv(r.get('APPRLIM2'))
        ntbrch    = _nv(r.get('NTBRCH'))
        accbrch   = _nv(r.get('ACCBRCH'))
        branch    = _nv(r.get('BRANCH'))
        score     = _sv(r.get('SCORE'))

        r['COLLVAL']  = collval * 100
        r['OUTSTAND'] = balance * 100
        r['CAVAIAMT'] = cavaiamt * 100
        r['REPAID']   = repaid * 100
        r['DISBURSE'] = disburse * 100
        r['LMTAMT']   = 0
        r['ODXSAMT']  = 0
        r['BILTOT']   = biltot * 100
        r['LIMTCURR'] = apprlimt
        if apprlimt in (None, 0): r['LIMTCURR'] = netproc

        r['INTRATE']  = intrate * 100

        # Issue date derivation
        if ((acctno > 8000000000 and loantype in (122, 106, 128, 130, 120, 121, 700, 705, 709, 710)) or
            (acctno < 2999999999 and loantype in (100, 101, 102, 103, 110, 111, 112, 113, 114, 115, 116, 117,
                                                   118, 120, 121, 122, 123, 124, 125, 127, 135, 136, 170, 180,
                                                   106, 128, 130, 700, 705, 709, 710, 194, 195, 380, 381))):
            dd, mm, yy = _z11_parts(issuedt)
            r['ISSUEDD'] = dd; r['ISSUEMM'] = mm; r['ISSUEYY'] = yy
            r['LIMTCURR'] = netproc
            if (10010 <= noteno <= 10099) or (30010 <= noteno <= 30099):
                if ind == 'B':
                    corgamt = _nv(r.get('CORGAMT'))
                    intamt_v = _nv(r.get('INTAMT'))
                    r['LIMTCURR'] = corgamt - intamt_v
        else:
            dd, mm, yy = _z11_parts(issuedt)
            r['ISSUEDD'] = dd; r['ISSUEMM'] = mm; r['ISSUEYY'] = yy

        if not (2500000000 <= acctno <= 2599999999) and not (800 <= loantype <= 899):
            if commno == 0 and ntint == 'A':
                r['LIMTCURR'] = netproc

        r['CAGAMAS']  = 0
        r['SYNDICAT'] = 'N'
        r['SPECIALF'] = '00'
        r['FCONCEPT_VAL'] = 99
        r['RESTRUCT']  = ' '
        r['PRODCD']    = format_lnprod(loantype)

        # INACTIVE PRODUCT
        if r['PRODCD'] == '34190':
            r['FACILITY'] = 34210
            r['LIMTCURR'] = apprlim2
            # FIRST.ACCTNO/FIRST.COMMNO logic deferred to post-sort de-dup

        # SPECIALF assignments
        if loantype in (124, 145):            r['SPECIALF'] = '00'
        if loantype in (566,):                r['SPECIALF'] = '10'
        if loantype in (991, 994) and pzipcode in (566,): r['SPECIALF'] = '10'
        if loantype in (170, 564, 565):       r['SPECIALF'] = '11'
        if loantype in (991, 994) and pzipcode in (170, 564, 565): r['SPECIALF'] = '11'
        if loantype in (559, 560, 567):       r['SPECIALF'] = '12'
        if loantype in (991, 994) and pzipcode in (559, 560, 567): r['SPECIALF'] = '12'
        if loantype in (568, 570, 532, 533, 573, 574): r['SPECIALF'] = '14'
        if loantype in (991, 994) and pzipcode in (568, 570, 573): r['SPECIALF'] = '14'
        if loantype in (561,):                r['SPECIALF'] = '16'
        if loantype in (991, 994) and pzipcode in (561,): r['SPECIALF'] = '16'
        if loantype in (555, 556):            r['SPECIALF'] = '18'
        if loantype in (991, 994) and pzipcode in (555, 556): r['SPECIALF'] = '18'
        if loantype in (521, 522, 523, 528):  r['SPECIALF'] = '99'
        if loantype in (991, 994) and pzipcode in (521, 522, 523, 528): r['SPECIALF'] = '99'

        # FCONCEPT assignments (pre-taxonomy override)
        if loantype in (124, 145, 461) or loantype in PRDF:     r['FCONCEPT_VAL'] = 10
        if loantype in (981, 982, 984, 985) and pzipcode in PRDF: r['FCONCEPT_VAL'] = 10
        if loantype == 180:  r['FCONCEPT_VAL'] = 13
        if loantype in (981, 982, 984, 985) and pzipcode == 180: r['FCONCEPT_VAL'] = 13
        if loantype in (128, 130, 131, 132, 983): r['FCONCEPT_VAL'] = 16
        if loantype == 193:  r['FCONCEPT_VAL'] = 18
        if loantype in (135, 136, 138, 182): r['FCONCEPT_VAL'] = 26
        if loantype in (982, 985) and pzipcode in (135, 136): r['FCONCEPT_VAL'] = 26
        if loantype in PRDG or (loantype in (981, 982, 994, 995) and pzipcode in PRDG):
            r['FCONCEPT_VAL'] = 51
        if loantype in (910, 925): r['FCONCEPT_VAL'] = 52
        if loantype in (992, 995) and pzipcode in (910, 925): r['FCONCEPT_VAL'] = 52
        if loantype in {4,5,15,20,25,26,27,28,29,30,31,32,33,34,60,62,63,70,71,72,73,
                        74,75,76,77,78,360,380,381,390,700,705,993,996}:
            r['FCONCEPT_VAL'] = 59
        if loantype in {227,228,230,231,232,233,234,235,236,237,238,239,240,241,242,
                        243,359,361,531,906,907}:
            r['FCONCEPT_VAL'] = 60
        if loantype in PRDH or (loantype in (991,992,994,995) and pzipcode in PRDH):
            r['FCONCEPT_VAL'] = 99
        if loantype in (180, 914, 915, 919, 920, 925, 950, 951): r['SYNDICAT'] = 'Y'

        # INTERDUE calculation
        r['INTERDUE'] = balance - curbal - _nv(r.get('FEEAMT'))
        if ntint != 'A':
            r['INTERDUE'] = balance - curbal - _nv(r.get('FEEAMT'))
        else:
            if loantype in (128, 130, 700, 705, 709, 710, 752, 760):
                r['CURBAL']   = curbal - rebate - intearn4
                r['INTERDUE'] = 0
            else:
                r['CURBAL']   = curbal + intearn - intamt
                r['INTERDUE'] = 0
                r['CURBALORI'] = r['CURBAL']
                if r['CURBAL'] < 0: r['CURBAL'] = 0

        curbal = _nv(r.get('CURBAL'))
        r['INSTALM'] = billcnt
        r['UNDRAWN'] = 0 if cavaiamt is None else cavaiamt / 100
        if faccode in (34331, 34332): r['UNDRAWN'] = 0

        r['LNTY']    = 0
        r['ACCTSTAT'] = 'O'

        if paidind == 'P':
            r['OUTSTAND'] = 0; r['CURBAL'] = 0; r['INTERDUE'] = 0; r['FEEAMT'] = 0

        if loantype in HP and paidind == 'P':
            r['ACCTSTAT'] = 'S'; r['ARREARS'] = 0; r['INSTALM'] = 0
            r['NODAYS']   = 0;   r['OUTSTAND'] = 0; r['UNDRAWN'] = 0
        elif 600 <= loantype <= 699 and paidind == 'P' and loantype not in HP:
            if wdb in (0, None) and lsttrncd in (658, 662, 663):
                r['ACCTSTAT'] = 'W'; r['IMPAIRED'] = 'Y'
            elif wdb in (0, None) and lsttrncd not in (658, 662, 663):
                r['ACCTSTAT'] = 'S'; r['IMPAIRED'] = 'N'
                r['ARREARS'] = 0; r['INSTALM'] = 0; r['NODAYS'] = 0; r['UNDRAWN'] = 0
        elif 600 <= loantype <= 699 and paidind != 'P' and loantype not in HP:
            if wdb <= 0:  r['ACCTSTAT'] = 'W'; r['IMPAIRED'] = 'Y'
            elif wdb > 0: r['ACCTSTAT'] = 'P'; r['IMPAIRED'] = 'Y'
        elif loantype not in range(600, 700) and paidind == 'P' and \
             _nv(r.get('OUTSTAND')) in (0, None) and loantype not in HP:
            if lsttrncd in (662, 663, 658):
                r['ACCTSTAT'] = 'W'; r['IMPAIRED'] = 'Y'
            else:
                r['ACCTSTAT'] = 'S'; r['IMPAIRED'] = 'N'
                r['ARREARS'] = 0; r['INSTALM'] = 0; r['NODAYS'] = 0; r['UNDRAWN'] = 0

        if loantype in (128, 130, 131, 132, 720, 725, 380, 381, 700, 705, 709, 710):
            r['LNTY'] = 1
        if r['LNTY'] == 1 and (noteno >= 98000 or borstat == 'S'): r['ACCTSTAT'] = 'T'
        if r['LNTY'] == 0 and borstat == 'S':                       r['ACCTSTAT'] = 'T'
        if loantype in (950, 951, 915, 953):                         r['SYNDICAT'] = 'Y'

        if loantype in (330, 302, 506, 902, 903, 951, 953) and cproduct == '170':
            r['NOTETERM'] = 12

        # PAYFREQC
        if loantype in (521,):                r['PAYFREQC'] = '13'
        if loantype in (124, 145):            r['PAYFREQC'] = '14'
        _pf14 = {5,6,25,15,20,111,139,140,122,106,128,130,120,121,201,204,205,225,300,301,307,
                 320,330,504,505,506,511,568,555,556,559,567,564,565,570,574,566,700,705,709,710,
                 900,901,910,912,930,126,127,359,981,982,983,991,993,117,113,118,115,119,116,
                 227,228,230,231,234,235,236,237,238,239,240,241,242}
        _pf14_992 = {300,304,305,307,310,330,504,505,506,511,515,359,555,556,559,560,564,565,
                     570,571,574,575,900,901,902,910,912,930}
        if loantype in _pf14 or (loantype == 992 and pzipcode in _pf14_992): r['PAYFREQC'] = '14'
        if loantype == 330 and cproduct is None: r['PAYFREQC'] = '16'
        if loantype in (302, 506, 902, 903, 951, 953, 330) and cproduct == '170': r['PAYFREQC'] = '19'
        if loantype in (569, 904, 932, 933, 950, 915):  r['PAYFREQC'] = '21'

        # HP restruct / CYC
        if loantype in (128,130,131,132,720,725,380,381,700,705,709,710,983,993,996):
            if noteno >= 98000:   r['RESTRUCT'] = 'C'
            if borstat == 'S':    r['RESTRUCT'] = 'T'
            if modeldes[:4] == 'RARM' or modeldes[:3] == 'RAM': r['RESTRUCT'] = 'Y'
            if cpnstdte > 0:
                dd, mm, yy = _z11_parts(cpnstdte)
                r['RRCOMPDD'] = dd; r['RRCOMPMM'] = mm; r['RRCOMPYR'] = yy
            r['CYC'] = 0
            if rrcycle[:2] == 'HP' and len(rrcycle) > 2:
                try: r['CYC'] = int(rrcycle[2]) * 1
                except: r['CYC'] = 0
            if r['CYC'] <= 0 and fclosuredt > 0: r['CYC'] = 6
        else:
            if modeldes in ('C', 'T', 'Y'):  r['RESTRUCT'] = modeldes
            elif borstat == 'S':              r['RESTRUCT'] = 'T'
            elif borstat == 'Q':              r['RESTRUCT'] = 'C'

        if loantype in HP:
            if borstat == 'W':   r['ACCTSTAT'] = 'P'
            elif borstat == 'X': r['ACCTSTAT'] = 'W'

        if pzipcode == 218022: cagamas = 25112021; r['RECOURSE'] = 'Y'
        elif pzipcode == 248039: cagamas = 29112024; r['RECOURSE'] = 'Y'
        r['CAGAMAS'] = cagamas
        if pzipcode == 218022: r['MASMATDT'] = '25112024'
        elif pzipcode == 248039: r['MASMATDT'] = '29112029'

        # NODAYS from BLDATE
        if bldate > 0:
            r['NODAYS'] = SDATE_NUM - int(bldate)
        if _nv(r.get('NODAYS')) < 0: r['NODAYS'] = 0
        if oldnotedayarr > 0 and 98000 <= noteno <= 98999:
            if _nv(r.get('NODAYS')) < 0: r['NODAYS'] = 0
            r['NODAYS']  = _nv(r.get('NODAYS')) + oldnotedayarr
            r['XNODAYS'] = oldnotedayarr
        if mordayarr is not None and not (isinstance(mordayarr, float) and math.isnan(mordayarr)):
            r['NODAYS'] = mordayarr

        if bldate > 0:
            bd = _sas_date(bldate)
            if bd: r['PAYDD'] = bd.day; r['PAYMM'] = bd.month; r['PAYYR'] = bd.year

        if dlvdate > 0:
            dd_d = _sas_date(dlvdate)
            if dd_d: r['GRANTDD'] = dd_d.day; r['GRANTMM'] = dd_d.month; r['GRANTYR'] = dd_d.year

        if exprdate > 0:
            ed_d = _sas_date(exprdate)
            if ed_d: r['LNMATDD'] = ed_d.day; r['LNMATMM'] = ed_d.month; r['LNMATYR'] = ed_d.year

        if maturedt > 0:
            dd, mm, yy = _z11_parts(maturedt)
            r['ORMATDD'] = dd; r['ORMATMM'] = mm; r['ORMATYR'] = yy

        if dlivrydt > 0:
            dd, mm, yy = _z11_parts(dlivrydt)
            r['DLVRDD'] = dd; r['DLVRMM'] = mm; r['DLVRYR'] = yy

        # MORSTDTE / MORENDTE
        r['MORSTDTE'] = '00000000'
        if firsildte > 0:
            fd = _mmddyy_from_z11(firsildte)
            if fd: r['MORSTDTE'] = f"{fd.year}{fd.month:02d}{fd.day:02d}"

        r['MORENDTE'] = '00000000'
        if currildte > 0:
            cd = _mmddyy_from_z11(currildte)
            if cd: r['MORENDTE'] = f"{cd.year}{cd.month:02d}{cd.day:02d}"

        r['RRPAYCNT'] = numcpns

        # MODELDES restruct tag
        md = modeldes
        if (len(md) >= 6 and
            md[0] in 'CTRSWM' and
            all(c in '0123456789' for c in md[1:4]) and
            md[4] in 'UD' and
            md[5] in 'K '):
            r['RRTAG']    = md[:6]
            r['RESTRUCT'] = md[0]
            if loantype not in range(600, 700) and paidind == 'P' and lsttrncd not in (662, 663, 658):
                r['ACCTSTAT'] = 'S'
            elif loantype not in range(600, 700):
                if r['RESTRUCT'] in ('C', 'R', 'W'): r['ACCTSTAT'] = 'C'
                else:                                  r['ACCTSTAT'] = 'T'
            cdate_d = date(1960,1,1) + timedelta(days=CDATE_NUM)
            dlvdate_d = _sas_date(dlvdate) if dlvdate else None
            if (md[4] in ('U', 'D') and dlvdate_d and cdate_d < dlvdate_d) or \
               (fclosuredt > 0 and assmdate > 0):
                r['RESTRUCT'] = ' '
                if r.get('ACCTSTAT') not in ('S', 'W'): r['ACCTSTAT'] = 'O'
            if assmdate > 0:
                dd, mm, yy = _z11_parts(assmdate)
                r['RRCOMPDD'] = dd; r['RRCOMPMM'] = mm; r['RRCOMPYR'] = yy
            try: r['CYC'] = int(md[2:4]) * 1
            except: r['CYC'] = 0
            if r['RESTRUCT'] == 'W': r['RESTRUCT'] = 'R'
            elif r['RESTRUCT'] == 'M': r['RESTRUCT'] = 'S'

        if fclosuredt > 0:
            rrcountdte = _mmddyy_from_z11(fclosuredt)
            if rrcountdte:
                r['RRCNTDD']  = f"{rrcountdte.day:02d}"
                r['RRCNTMM']  = f"{rrcountdte.month:02d}"
                r['RRCNTYR']  = str(rrcountdte.year)
                cyc_val = _nv(r.get('CYC'))
                if cyc_val > 0:
                    rrenddt = _intnx_month_end(rrcountdte, cyc_val - 1)
                    cdate_d = date(1960,1,1) + timedelta(days=CDATE_NUM)
                    if cdate_d >= rrenddt and (assmdate > 0 or cpnstdte > 0):
                        if r['LNTY'] == 1:
                            if user5 != 'N' and issxdte >= (date(2015,4,6)-date(1960,1,1)).days:
                                r['RESTRUCT'] = ' '
                        else:
                            r['RESTRUCT'] = ' '
                        if r.get('ACCTSTAT') not in ('S', 'W'): r['ACCTSTAT'] = 'O'
                        r['COMPLYIND'] = 'Y'

        if nxbildt > 0:
            dd, mm, yy = _z11_parts(nxbildt)
            r['BILDUEDD'] = dd; r['BILDUEMM'] = mm; r['BILDUEYR'] = yy

        _colly_loan = set(range(4,8)) | {15,20,25,26,27,28,29,30,31,32,33,34,60,61,62,63,
                                          70,71,72,73,74,75,76,77,78,100,101,108,199,500,520,
                                          983,993,996} | set(range(128,134)) | {380,381,700,705,750,720,725,752,760}
        cy_str = f"{int(collyear):05d}" if collyear else '00000'
        if loantype in _colly_loan or orgtype == 'S':
            r['COLLYR'] = cy_str[3:5]
        else:
            r['COLLMM'] = cy_str[1:3]
            r['COLLYR'] = cy_str[3:5]

        r['ARREARS'] = math.floor(_nv(r.get('NODAYS')) / 30.00050)

        if mo_instl_arr is not None and not (isinstance(mo_instl_arr, float) and math.isnan(mo_instl_arr)):
            billcnt = int(mo_instl_arr)
            r['BILLCNT'] = billcnt
        r['INSTALM'] = billcnt

        # Billing date adjustment
        bldate_d = _sas_date(bldate) if bldate else None
        if bldate_d and (bldate_d + timedelta(days=1)).day == 1 and billcnt > 0:
            if loantype not in HP:
                lmo1 = _sv(lmo_tag[0]) if lmo_tag else ''
                if lmo1 not in ('R','F','V','A','B') or \
                   (lmo1 in ('R','F','V','A','B') and not (lmostdte <= SDATE_NUM <= moenddte)):
                    r['INSTALM'] = billcnt - 1
            else:
                _ram_models = {'A1AM','A1BM','A1CM','A1DM','A1EM','A1FM'}
                _is_ram = modeldes in _ram_models or modeldes[:3] == 'RAM'
                if not _is_ram or (_is_ram and not (lmostdte <= SDATE_NUM <= moenddte)):
                    r['INSTALM'] = billcnt - 1

        if flag1 == 'F': r['UNDRAWN'] = 0
        if flag1 in ('P', 'I'): r['UNDRAWN'] = cavaiamt / 100

        r['CRRINI'] = (score1 + mortgind).strip()
        r['CRRNOW'] = (score2 + contrtype).strip()

        undrawn = _nv(r.get('UNDRAWN'))
        if undrawn is None: r['UNDRAWN'] = 0
        if balance < 0:     r['UNDRAWN'] = apprlim2
        undrawn1 = round(_nv(r.get('UNDRAWN')), 2)
        r['UNDRAWN'] = undrawn1 * 100

        # Classification LN
        if riskrate in (' ', ''):   r['CLASSIFI'] = 'P'
        elif riskrate == '1':       r['CLASSIFI'] = 'C'
        elif riskrate == '2':       r['CLASSIFI'] = 'S'
        elif riskrate == '3':       r['CLASSIFI'] = 'D'
        elif riskrate == '4':       r['CLASSIFI'] = 'B'
        if nplind == 'P':           r['CLASSIFI'] = 'P'

        if loanstat == 3:   r['IMPAIRED'] = 'Y'
        else:               r['IMPAIRED'] = 'N'

        if faccode == 34371:
            if _nv(r.get('NODAYS')) >= 90 or borstat in ('F','I','R') or user5 == 'N':
                r['IMPAIRED'] = 'Y'

        r['SECTOR']   = _sv(r.get('SECTOR')).strip()
        r['LIMTCURR'] = _nv(r.get('LIMTCURR')) * 100
        r['COLLREF']  = acctno

        if branch == 0: branch = ntbrch
        if branch == 0: branch = accbrch
        r['APCODE']   = loantype
        r['FICODE']   = branch
        r['REALISAB'] = realisab * 100

        if payfreq == '1':   r['PAYFREQC'] = '14'
        elif payfreq == '2': r['PAYFREQC'] = '15'
        elif payfreq == '3': r['PAYFREQC'] = '16'
        elif payfreq == '4': r['PAYFREQC'] = '17'
        elif payfreq == '5': r['PAYFREQC'] = '18'
        elif payfreq == '6': r['PAYFREQC'] = '13'
        elif payfreq == '9': r['PAYFREQC'] = '21'

        if loantype in (984, 985, 994, 995) and noteno == 10010:
            r['OUTSTAND'] = 0; r['ACCTSTAT'] = 'S'; r['BALANCE'] = 0
            r['ARREARS']  = 0; r['INSTALM']  = 0;  r['UNDRAWN'] = 0

        if loantype in (981, 982, 983, 991, 992, 993, 698, 699):
            r['OUTSTAND'] = 0; r['BALANCE'] = 0; r['UNDRAWN'] = 0
            r['ACCTSTAT'] = 'W'; r['CURBAL'] = 0; r['INTERDUE'] = 0; r['FEEAMT'] = 0
            if loantype in (983, 993, 698, 699) and (paidind == 'P' or user5 == 'F'):
                r['ACCTSTAT'] = 'S'; r['ARREARS'] = 0; r['INSTALM'] = 0
                r['NODAYS']   = 0;   r['UNDRAWN'] = 0
                r['CLASSIFI'] = 'P'; r['IMPAIRED'] = 'N'

        if borstat == 'F':
            r['INSTALM'] = 999
            if loantype in (983, 993, 698, 699):
                r['CLASSIFI'] = 'B'; r['IMPAIRED'] = 'Y'

        # BNM TAXONOMY & CCRIS ENHANCEMENT (2011-4096)
        r['SYNDICAT']     = format_synd(loantype)
        r['SPECIALF']     = format_fundsch(loantype)
        r['FCONCEPT_VAL'] = format_fconcept(loantype)

        # 2020-3587
        if loantype in (169, 434, 448, 144, 172):
            r['SPECIALF'] = format_fschemec(census)

        if loantype in (412, 413, 414, 911, 415, 466):
            r['FACILITY'] = 34322; r['FACCODE'] = 34320
        if loantype in (912, 575, 416, 467, 461):
            r['FACILITY'] = 34391; r['FACCODE'] = 34391
        if loantype in (307, 422, 464, 465):
            r['FACILITY'] = 34391; r['FACCODE'] = 34392
        if loantype in (103, 104):
            r['FACILITY'] = 34371; r['FACCODE'] = 34371
        if loantype in (191, 417):
            r['FACILITY'] = 34351; r['FACCODE'] = 34364

        if 600 <= loantype <= 699 or loantype in (972, 973):
            if loantype in (698, 699):
                r['SYNDICAT'] = 'N'; r['SPECIALF'] = '00'; r['FCONCEPT_VAL'] = 16
            else:
                r['SYNDICAT']     = format_synd(math.floor(census))
                r['SPECIALF']     = format_fundsch(math.floor(census))
                r['FCONCEPT_VAL'] = format_fconcept(math.floor(census))
                if math.floor(census) in (169, 434, 448, 144, 172):
                    r['SPECIALF'] = format_fschemec(census)

            # P-I-O for inactive product
            if wdb not in (0, None) and not (isinstance(wdb, float) and math.isnan(wdb)):
                feeamt_v  = _nv(r.get('FEEAMT'))
                curbal_v  = _nv(r.get('CURBAL'))
                outstand_v = _nv(r.get('OUTSTAND'))
                balance_v = _nv(r.get('BALANCE'))
                if outstand_v > 0 and feeamt_v > 0:
                    if balance_v <= feeamt_v:
                        feeamt_v = balance_v
                    if balance_v <= feeamt_v + curbal_v:
                        r['CURBAL']   = wdb - feeamt_v
                        r['INTERDUE'] = 0
                    else:
                        r['INTERDUE'] = wdb - curbal_v - feeamt_v
                else:
                    if balance_v > curbal_v:
                        r['INTERDUE'] = wdb - curbal_v
                    else:
                        r['CURBAL']   = wdb
                        r['INTERDUE'] = 0
                    r['FEEAMT'] = 0

            r['TOTIIS'] = 0
            if freleas > 0 or flag1 == 'F': r['UNDRAWN'] = 0

        # TYPEPRC for LN
        _wofdt = (date(2017,5,31) - date(1960,1,1)).days
        if loantype in (983, 993, 128, 130, 380, 381, 700, 705):
            r['TYPEPRC'] = '80'
        elif loantype in (678, 679, 698, 699, 131, 132, 720, 725):
            if ntindex in (1, 30) and spread < 0:    r['TYPEPRC'] = '42'
            elif ntindex in (1, 30) and spread >= 0: r['TYPEPRC'] = '41'
            elif ntindex in (38, 39):                r['TYPEPRC'] = '43'
            else:                                    r['TYPEPRC'] = '80'
        elif (1 <= loantype <= 99) or (100 <= loantype <= 109):
            if ntindex in (1, 30) and spread < 0:    r['TYPEPRC'] = '42'
            elif ntindex in (1, 30) and spread >= 0: r['TYPEPRC'] = '41'
            elif ntindex in (38, 39):                r['TYPEPRC'] = '43'
            else:                                    r['TYPEPRC'] = '59'
        elif cfindex == 997:                         r['TYPEPRC'] = '68'
        elif ntindex in (1, 30) and spread < 0:      r['TYPEPRC'] = '42'
        elif ntindex in (1, 30) and spread >= 0:     r['TYPEPRC'] = '41'
        elif ntindex in (38, 39):                    r['TYPEPRC'] = '43'
        elif ntindex == 52:                          r['TYPEPRC'] = '44'
        elif 900 <= cfindex <= 903:                  r['TYPEPRC'] = '53'
        elif 904 <= cfindex <= 907:                  r['TYPEPRC'] = '57'
        elif 908 <= cfindex <= 911:                  r['TYPEPRC'] = '54'
        elif 912 <= cfindex <= 915:                  r['TYPEPRC'] = '55'
        elif 916 <= cfindex <= 919:                  r['TYPEPRC'] = '56'
        elif 920 <= cfindex <= 923:                  r['TYPEPRC'] = '58'
        elif cfindex == 925:                         r['TYPEPRC'] = '79'
        elif cfindex == 926:                         r['TYPEPRC'] = '61'
        elif cfindex == 927:                         r['TYPEPRC'] = '62'
        elif cfindex == 928:                         r['TYPEPRC'] = '63'
        elif cfindex == 929:                         r['TYPEPRC'] = '64'
        elif cfindex == 930:                         r['TYPEPRC'] = '65'
        elif cfindex == 931:                         r['TYPEPRC'] = '66'
        elif cfindex == 932:                         r['TYPEPRC'] = '67'
        elif cfindex == 933:                         r['TYPEPRC'] = '51'
        elif cfindex == 924 or costfund > 0:         r['TYPEPRC'] = '68'
        else:                                        r['TYPEPRC'] = '59'

        if 600 <= loantype <= 699 and paidind != 'P':
            if loanstat == 3 and r.get('IMPAIRED') == 'Y':
                if wdb <= 0: r['ACCTSTAT'] = 'W'
                else:        r['ACCTSTAT'] = 'P'

        if 600 <= loantype <= 699 and paidind == 'P' and lsttrncd in (662, 663, 658):
            r['ACCTSTAT'] = 'W'; r['IMPAIRED'] = 'Y'
        if 600 <= loantype <= 699 and paidind == 'P' and lsttrncd not in (662, 663, 658):
            r['ACCTSTAT'] = 'S'; r['IMPAIRED'] = 'N'

        if loantype in (983, 993, 698, 699) and paidind != 'P':
            r['CLASSIFI'] = 'B'; r['IMPAIRED'] = 'Y'

        r['RMSBBA'] = 0

        # UTILISE
        if rleasamt != 0:
            r['UTILISE'] = 'Y'
        elif rleasamt == 0:
            if loantype in set(range(600, 700)) | {983, 993}: r['UTILISE'] = 'Y'
            elif r.get('IMPAIRED') == 'Y':                    r['UTILISE'] = 'Y'
            elif commno > 0 and cusedamt > 0:                 r['UTILISE'] = 'Y'
            else:                                             r['UTILISE'] = 'N'

    # -----------------------------------------------------------------------
    # POST OD/LN: common cleanup
    # -----------------------------------------------------------------------
    for _nv_field, _default in [('TOTIIS',0),('TOTIISR',0),('TOTWOF',0),('IISOPBAL',0),
                                  ('IISSUAMT',0),('IISBWAMT',0),('SPOPBAL',0),('SPWBAMT',0),
                                  ('SPWOF',0),('SPDANAH',0),('SPTRANS',0),('LEDGBAL',0),
                                  ('CUMRC',0),('BDR',0),('CUMSC',0),('WOAMT',0),
                                  ('BDR_MTH',0),('SC_MTH',0),('RC_MTH',0),('NAI_MTH',0),
                                  ('CUMWOSP',0),('CUMWOIS',0),('CUMNAI',0)]:
        v = r.get(_nv_field)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            r[_nv_field] = _default

    # Scale-up numeric fields
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
    r['IISDANAH'] = 0; r['IISTRANS'] = 0; r['SPCHARGE'] = 0
    r['LEDGBAL']  = _nv(r.get('LEDGBAL'))  * 100
    r['CUMRC']    = _nv(r.get('CUMRC'))    * 100
    r['BDR']      = _nv(r.get('BDR'))      * 100
    r['CUMSC']    = _nv(r.get('CUMSC'))    * 100
    r['WOAMT']    = _nv(r.get('WOAMT'))    * 100
    r['BDR_MTH']  = _nv(r.get('BDR_MTH')) * 100
    r['SC_MTH']   = _nv(r.get('SC_MTH'))  * 100
    r['RC_MTH']   = _nv(r.get('RC_MTH'))  * 100
    r['NAI_MTH']  = _nv(r.get('NAI_MTH')) * 100
    r['CUMWOSP']  = _nv(r.get('CUMWOSP')) * 100
    r['CUMWOIS']  = _nv(r.get('CUMWOIS')) * 100
    r['CUMNAI']   = _nv(r.get('CUMNAI'))  * 100
    r['NETPROC']  = _nv(r.get('NETPROC')) * 100
    r['TOTAMT']   = _nv(r.get('TOTAMT'))  * 100

    for _sf in ['CCRIS_INSTLAMT','DSR','MTD_REPAID_AMT','MTD_TAWIDH_AMT','MTD_GHARAMAH_AMT']:
        v = r.get(_sf)
        if v is None or (isinstance(v, float) and math.isnan(v)): r[_sf] = 0

    repay_src = r.get('REPAY_SOURCE')
    if repay_src and _nv(repay_src) > 0:
        r['REPAYSRC'] = f"{int(repay_src):04d}"

    if 3000000000 <= acctno <= 3999999999:
        r['LEDGBAL'] = _nv(r.get('LEDGBAL')) * -1

    # Score validation
    sc = _sv(r.get('SCORE'))
    if sc and sc[0] not in ALP:
        r['SCORE'] = '   '

    # ISSUED date
    issuemm = _nv(r.get('ISSUEMM'))
    issuedd = _nv(r.get('ISSUEDD'))
    issueyy = _nv(r.get('ISSUEYY'))
    try:
        r['ISSUED'] = (date(int(issueyy), int(issuemm), int(issuedd)) - date(1960,1,1)).days
    except Exception:
        r['ISSUED'] = 0
    issued_d = _sas_date(r['ISSUED']) if r['ISSUED'] else None

    # HP old notes restruct override
    if loantype in (128, 130, 380, 381, 700, 705, 720, 725) and noteno >= 98000:
        if issued_d:
            cutoff1 = date(2005, 9, 30)
            cutoff2a = date(2007, 12, 19); cutoff2b = date(2008, 8, 31)
            cutoff3  = date(2008, 9, 1);   cutoff4a = date(2005, 10, 1); cutoff4b = date(2007, 12, 18)
            if issued_d <= cutoff1 or cutoff2a <= issued_d <= cutoff2b:
                r['RESTRUCT'] = 'T'
            elif issued_d >= cutoff3 or cutoff4a <= issued_d <= cutoff4b:
                r['RESTRUCT'] = 'C'
            if r.get('RESTRUCT') == 'C': r['ACCTSTAT'] = 'C'
            elif r.get('RESTRUCT') == 'T': r['ACCTSTAT'] = 'T'
        if paidind == 'P':
            r['ACCTSTAT'] = 'S'; r['ARREARS'] = 0; r['INSTALM'] = 0
            r['NODAYS']   = 0;   r['OUTSTAND'] = 0; r['UNDRAWN'] = 0

    if loantype in (128, 130, 380, 381, 700, 705, 720, 725, 131, 132):
        _old_models = {'A1A','A1B','A1C','A1D','A1E','A1F','AK1A','AK1B','AK1C','AK1D','AK1E','AK1F'}
        if noteno < 98000 and _sv(r.get('MODELDES')) in _old_models:
            r['RESTRUCT'] = 'C'; r['ACCTSTAT'] = 'C'; r['RRTAG'] = _sv(r.get('MODELDES'))

    apcode = _nv(r.get('APCODE'))
    if 800 <= apcode <= 899 and 2000000000 <= acctno <= 2999999999: r['FICODE'] = 904
    if 800 <= apcode <= 899 and 8000000000 <= acctno <= 8999999999: r['FICODE'] = 904

    if 800 <= loantype <= 899: r['COSTCTR'] = 3904

    # RATAG / RA dates
    facility = _nv(r.get('FACILITY'))
    if facility in (34331, 34332):
        r['RATAG']  = akpk_status
        dd, mm, yy  = _z11_parts(issuedt) if 'issuedt' in dir() else _z11_parts(r.get('ISSUEDT', 0))
        r['RADTDD'] = dd; r['RADTMM'] = mm; r['RADTYY'] = yy
    else:
        nurs_tag_v   = _sv(r.get('NURS_TAG'))
        tfa_nurs_tag_v = _sv(r.get('TFA_NURS_TAG'))
        nurs_tagdt_v = _nv(r.get('NURS_TAGDT'))
        tfa_dt_v     = _nv(r.get('TFA_NURS_TAG_DT'))

        _valid_nurs = {'D','X','L','D1','D2','D3','D4','D5','D6','D7','D8','D9',
                       'X1','X2','X3','X4','X5','X6','X7','X8','X9',
                       'L1','L2','L3','L4','L5','L6','L7','L8','L9',
                       'O','P','R1','R2','R3','E','T','W','E1','E2','E3','T1','T2','T3'}
        _valid_tfa  = {'C1','C2','C3','C4','C5','C6','C7','C8','C9',
                       'G1','G2','G3','G4','G5','G6','G7','G8','G9',
                       'M1','M2','M3','M4','M5','M6','M7','M8','M9',
                       'N1','N2','N3','N4','N5','N6','N7','N8','N9',
                       'Q1','Q2','Q3','Q4','Q5','Q6','Q7','Q8','Q9',
                       'K1','K2','K3','K4','K5','K6','K7','K8','K9'}

        def _set_ratag(tag, dt_sas):
            d = _sas_date(dt_sas)
            r['RATAG']  = tag
            r['RADTYY'] = str(d.year)          if d else '0000'
            r['RADTMM'] = f"{d.month:02d}"     if d else '00'
            r['RADTDD'] = f"{d.day:02d}"       if d else '00'

        if nurs_tag_v in _valid_nurs:
            _set_ratag(nurs_tag_v, nurs_tagdt_v)
            if tfa_nurs_tag_v in _valid_tfa:
                if nurs_tagdt_v > tfa_dt_v:
                    _set_ratag(nurs_tag_v, nurs_tagdt_v)
                else:
                    _set_ratag(tfa_nurs_tag_v, tfa_dt_v)
        elif tfa_nurs_tag_v in _valid_tfa:
            _set_ratag(tfa_nurs_tag_v, tfa_dt_v)
        else:
            r['RATAG'] = ' '

    if apcode == 461:
        r['PRODCD']   = '34149'
        r['FACILITY'] = 34391; r['FACCODE'] = 34391
        r['SPECIALF'] = '00';  r['SYNDICAT'] = 'N'
        r['FCONCEPT_VAL'] = 36

    # -----------------------------------------------------------------------
    # Additional computed fields needed later
    # -----------------------------------------------------------------------
    # ASSMDATE2
    assmdate_val = _nv(r.get('ASSMDATE'))
    assmdate2    = _mmddyy_from_z11(assmdate_val) if assmdate_val > 0 else None
    if assmdate2:
        r['ASSMDATE2'] = (assmdate2 - date(1960,1,1)).days
    else:
        r['ASSMDATE2'] = 0

    nurs_tag_v   = _sv(r.get('NURS_TAG'))
    nurs_tagdt_v = _nv(r.get('NURS_TAGDT'))
    if loantype not in HP and r.get('ACCTSTAT') != 'S' and nurs_tag_v == 'K':
        r['ACCTSTAT']  = 'K'
        r['ASSMDATE2'] = nurs_tagdt_v

    assmdate2_d = _sas_date(r['ASSMDATE2'])
    r['ASSMYY'] = str(assmdate2_d.year)        if assmdate2_d else '0000'
    r['ASSMMM'] = f"{assmdate2_d.month:02d}"   if assmdate2_d else '00'
    r['ASSMDD'] = f"{assmdate2_d.day:02d}"     if assmdate2_d else '00'

    nurs_tagdt_d = _sas_date(nurs_tagdt_v)
    r['NURSYY'] = str(nurs_tagdt_d.year)       if nurs_tagdt_d else '0000'
    r['NURSMM'] = f"{nurs_tagdt_d.month:02d}"  if nurs_tagdt_d else '00'
    r['NURSDD'] = f"{nurs_tagdt_d.day:02d}"    if nurs_tagdt_d else '00'

    # ISSUEDAT from ISSUEDT
    issuedt_v  = _nv(r.get('ISSUEDT'))
    issuedax_d = _mmddyy_from_z11(issuedt_v) if issuedt_v > 0 else None
    r['ISSXDAY']  = f"{issuedax_d.day:02d}"    if issuedax_d else '00'
    r['ISSXYEAR'] = str(issuedax_d.year)        if issuedax_d else '0000'
    r['ISSXMONT'] = f"{issuedax_d.month:02d}"   if issuedax_d else '00'
    r['ACCRUAL1'] = int(_nv(r.get('ACCRUAL')) * 100) / 100

    # RJDATE
    rjdate_v = _nv(r.get('RJDATE'))
    rjdate_d = _sas_date(rjdate_v) if rjdate_v > 0 else None
    r['RJDATEDD'] = f"{rjdate_d.day:02d}"    if rjdate_d else '00'
    r['RJDATEMM'] = f"{rjdate_d.month:02d}"  if rjdate_d else '00'
    r['RJDATEYY'] = str(rjdate_d.year)        if rjdate_d else '0000'

    # FACCODE 34651/34652 + FRTDIDTIND
    faccode_v   = _nv(r.get('FACCODE'))
    frtdidtind  = _sv(r.get('FRTDIDTIND'))
    if faccode_v in (34651, 34652) and frtdidtind == 'Y':
        r['ISSUEDD'] = r.get('POSTDTDD', '00')
        r['ISSUEMM'] = r.get('POSTDTMM', '00')
        r['ISSUEYY'] = r.get('POSTDTYY', '0000')

    # FULLREL
    freleas_v  = _nv(r.get('FRELEAS'))
    fullrel_d  = _mmddyy_from_z11(freleas_v) if freleas_v > 0 else None
    r['FULLREL_DD'] = fullrel_d.day   if fullrel_d else 0
    r['FULLREL_MM'] = fullrel_d.month if fullrel_d else 0
    r['FULLREL_YY'] = fullrel_d.year  if fullrel_d else 0

    r['UNUSEAMT']         = _nv(r.get('UNUSEAMT'))         * 100
    r['CORGAMT']          = _nv(r.get('CORGAMT'))          * 100
    r['FWRITE_DOWN_BAL']  = _nv(r.get('WRITE_DOWN_BAL'))   * 100

    # ACCTSTAT 'J' logic
    manual_rr_tag  = _sv(r.get('MANUAL_RR_TAG'))
    manual_rr_dt   = _nv(r.get('MANUAL_RR_DT'))
    akpk_ra_tag    = _sv(r.get('AKPK_RA_TAG'))
    akpk_ra_start  = _nv(r.get('AKPK_RA_START_DT'))
    akpk_ra_end    = _nv(r.get('AKPK_RA_END_DT'))
    cdate_d        = date(1960,1,1) + timedelta(days=CDATE_NUM)
    facility_v     = _nv(r.get('FACILITY'))

    if facility_v in (34331, 34332):
        mrd_d  = _sas_date(manual_rr_dt)
        if mrd_d and r.get('ACCTSTAT') != 'S':
            end5  = _intnx_month_end(mrd_d, 5)
            end23 = _intnx_month_end(mrd_d, 23)
            if ((manual_rr_tag in ('UAJ','MAJ') and mrd_d <= cdate_d <= end5) or
                (manual_rr_tag in ('UBJ','MBJ','FJ','MJ') and mrd_d <= cdate_d <= end23)):
                r['ACCTSTAT'] = 'J'
    else:
        ra_start_d = _sas_date(akpk_ra_start)
        ra_end_d   = _sas_date(akpk_ra_end)
        ra_pre = akpk_ra_tag[:2] if len(akpk_ra_tag) >= 2 else ''
        ra_dig = akpk_ra_tag[2]  if len(akpk_ra_tag) >= 3 else ''
        if ra_start_d and r.get('ACCTSTAT') != 'S':
            end5u = _intnx_month_end(ra_start_d, 5)
            end_e = _intnx_month_end(ra_end_d, 0) if ra_end_d else None
            if ((ra_pre in ('AU','CU') and ra_dig in '123456789' and
                 ra_start_d <= cdate_d <= end5u) or
                (ra_pre in ('BU','DU','AF') and ra_dig in '123456789' and
                 end_e and ra_start_d <= cdate_d <= end_e)):
                r['ACCTSTAT'] = 'J'

    # SMR 2022-3163: RRSTRDD/RRSTRMM etc.
    r['RRSTRDD'] = 0; r['RRSTRMM'] = 0; r['RRSTRYR'] = 0
    r['RRCMPLDD'] = 0; r['RRCMPLMM'] = 0; r['RRCMPLYR'] = 0

    if loantype in (128, 130, 131, 132):
        if issuedt_v > 0:
            dd, mm, yy = _z11_parts(issuedt_v)
            r['RRSTRDD'] = dd; r['RRSTRMM'] = mm; r['RRSTRYR'] = yy
        if _nv(r.get('CPNSTDTE')) > 0:
            dd, mm, yy = _z11_parts(r.get('CPNSTDTE', 0))
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
    # Write SUBACRED record (LRECL=1550, 1-based positions)
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
    _put_rec(buf, 85,   _fmt_n(_nv(r.get('FCONCEPT_VAL')), 2))
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
    _put_rec(buf, 194,  _fmt_z(_nv(r.get('DLVRDD')), 2) if str(r.get('DLVRDD','')).isdigit()
             else _fmt_s(_sv(r.get('DLVRDD')), 2))
    _put_rec(buf, 196,  _fmt_z(_nv(r.get('DLVRMM')), 2) if str(r.get('DLVRMM','')).isdigit()
             else _fmt_s(_sv(r.get('DLVRMM')), 2))
    _put_rec(buf, 198,  _fmt_z(_nv(r.get('DLVRYR')), 4) if str(r.get('DLVRYR','')).isdigit()
             else _fmt_s(_sv(r.get('DLVRYR')), 4))
    _put_rec(buf, 202,  _fmt_z(_nv(r.get('COLLMM')), 2))
    _put_rec(buf, 204,  _fmt_z(_nv(r.get('COLLYR')), 2))
    _put_rec(buf, 206,  _fmt_z(_nv(r.get('BILDUEDD')), 2) if str(r.get('BILDUEDD','')).isdigit()
             else _fmt_s(_sv(r.get('BILDUEDD')), 2))
    _put_rec(buf, 208,  _fmt_z(_nv(r.get('BILDUEMM')), 2) if str(r.get('BILDUEMM','')).isdigit()
             else _fmt_s(_sv(r.get('BILDUEMM')), 2))
    _put_rec(buf, 210,  _fmt_z(_nv(r.get('BILDUEYR')), 4) if str(r.get('BILDUEYR','')).isdigit()
             else _fmt_s(_sv(r.get('BILDUEYR')), 4))
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
    _put_rec(buf, 634,  _fmt_f(_nv(r.get('ACCRUAL1')), 15, 7))
    _put_rec(buf, 650,  _fmt_z(_nv(r.get('CYC')), 3))
    _put_rec(buf, 653,  _fmt_z(_nv(r.get('RRCNTYR')), 4) if str(r.get('RRCNTYR','')).isdigit()
             else _fmt_s(_sv(r.get('RRCNTYR')), 4))
    _put_rec(buf, 657,  _fmt_z(_nv(r.get('RRCNTMM')), 2) if str(r.get('RRCNTMM','')).isdigit()
             else _fmt_s(_sv(r.get('RRCNTMM')), 2))
    _put_rec(buf, 659,  _fmt_z(_nv(r.get('RRCNTDD')), 2) if str(r.get('RRCNTDD','')).isdigit()
             else _fmt_s(_sv(r.get('RRCNTDD')), 2))
    _put_rec(buf, 661,  _fmt_z(_nv(r.get('RRCOMPYR')), 4) if str(r.get('RRCOMPYR','')).isdigit()
             else _fmt_s(_sv(r.get('RRCOMPYR')), 4))
    _put_rec(buf, 665,  _fmt_z(_nv(r.get('RRCOMPMM')), 2) if str(r.get('RRCOMPMM','')).isdigit()
             else _fmt_s(_sv(r.get('RRCOMPMM')), 2))
    _put_rec(buf, 667,  _fmt_z(_nv(r.get('RRCOMPDD')), 2) if str(r.get('RRCOMPDD','')).isdigit()
             else _fmt_s(_sv(r.get('RRCOMPDD')), 2))
    _put_rec(buf, 669,  _fmt_s(_sv(r.get('DELQCD')), 2))
    _put_rec(buf, 672,  _fmt_n(_nv(r.get('UTILISE')), 1) if isinstance(r.get('UTILISE'), (int,float))
             else _fmt_s(_sv(r.get('UTILISE')), 1))
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
    _put_rec(buf, 1154, _fmt_s(_sv(r.get('STRUPCO_3YR')), 5))
    _put_rec(buf, 1160, _fmt_f(_nv(r.get('DSR')), 6, 2))
    _put_rec(buf, 1166, _fmt_s(_sv(r.get('REFIN_FLG')), 3))
    _put_rec(buf, 1169, _fmt_s(_sv(r.get('OLD_FI_CD')), 10))
    _put_rec(buf, 1179, _fmt_s(_sv(r.get('OLD_MACC_NO')), 30))
    _put_rec(buf, 1209, _fmt_s(_sv(r.get('OLD_SUBACC_NO')), 30))
    _put_rec(buf, 1239, _fmt_f(_nv(r.get('REFIN_AMT')), 20, 2))
    _put_rec(buf, 1260, _fmt_s(_sv(r.get('LN_UTILISE_LOCAT_CD')), 5))
    _put_rec(buf, 1266, _fmt_s(_sv(r.get('AKPK_STATUS')), 9))
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
    _put_rec(buf, 398, _sv(r.get('IMPAIRYY')))
    _put_rec(buf, 402, _sv(r.get('IMPAIRMM')))
    _put_rec(buf, 404, _sv(r.get('IMPAIRDD')))
    _put_rec(buf, 407, _fmt_f(_nv(r.get('HCURBAL')), 15, 2))
    _put_rec(buf, 423, _fmt_z(_nv(r.get('XNODAYS')), 5))
    _put_rec(buf, 429, _fmt_s(_sv(r.get('ASSMYY')), 4))
    _put_rec(buf, 434, _fmt_s(_sv(r.get('ASSMMM')), 2))  # SAS says ASSMMM (typo in SAS for ASSMMM)
    _put_rec(buf, 437, _fmt_s(_sv(r.get('ASSMDD')), 2))
    _put_rec(buf, 439, _fmt_f(_nv(r.get('CCRIS_INSTLAMT')), 17, 2))
    _put_rec(buf, 456, _fmt_f(_nv(r.get('MTD_TAWIDH_AMT')), 17, 2))
    _put_rec(buf, 473, _fmt_f(_nv(r.get('MTD_GHARAMAH_AMT')), 17, 2))
    _put_rec(buf, 490, _fmt_s(_sv(r.get('REPAYSRC')), 4))
    _put_rec(buf, 496, _fmt_s(_sv(r.get('REPAY_TYPE_CD')), 2))
    _put_rec(buf, 498, _fmt_f(_nv(r.get('MTD_REPAID_AMT')), 17, 2))
    _put_rec(buf, 515, _fmt_f(_nv(r.get('MAN_REV_RATE')), 9, 6))
    _put_rec(buf, 525, _fmt_s(_sv(r.get('MAN_REV_DATE')), 8))
    _put_rec(buf, 533, _fmt_f(_nv(r.get('SYS_REV_RATE')), 9, 6))
    _put_rec(buf, 543, _fmt_s(_sv(r.get('SYS_REV_DATE')), 8))
    _put_rec(buf, 553, _fmt_s(_sv(r.get('NURS_TAG')), 6))
    _put_rec(buf, 559, _fmt_s(_sv(r.get('NURSYY')), 4))
    _put_rec(buf, 563, _fmt_s(_sv(r.get('NURSMM')), 2))
    _put_rec(buf, 565, _fmt_s(_sv(r.get('NURSDD')), 2))
    _put_rec(buf, 567, _fmt_s(_sv(r.get('LMO_TAG')), 2))
    _put_rec(buf, 569, _fmt_s(_sv(r.get('RATAG')), 10))
    _put_rec(buf, 579, _fmt_s(_sv(r.get('RADTDD')), 2))
    _put_rec(buf, 581, _fmt_s(_sv(r.get('RADTMM')), 2))
    _put_rec(buf, 583, _fmt_s(_sv(r.get('RADTYY')), 4))
    _put_rec(buf, 587, _fmt_s(_sv(r.get('WRIOFF_CLOSE_FILE_TAG')), 1))
    _put_rec(buf, 588, _fmt_s(_sv(r.get('WRIOFFDD')), 2))
    _put_rec(buf, 590, _fmt_s(_sv(r.get('WRIOFFMM')), 2))
    _put_rec(buf, 592, _fmt_s(_sv(r.get('WRIOFFYY')), 4))
    _put_rec(buf, 596, _fmt_z(_nv(r.get('FWRITE_DOWN_BAL')), 15))
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
# STEP 9: PROC SORT ACCTCRED -> OD (3000000000-3999999999)
#         ODR7B and CREDITOD output
# ---------------------------------------------------------------------------
acctcred_df = pl.DataFrame(acctcred_rows)

od_df = acctcred_df.filter(
    (pl.col('ACCTNO') >= 3000000000) & (pl.col('ACCTNO') <= 3999999999)
).sort('ACCTNO')

# ODR7B - OD repaid records by repayment type
odr7b_rows = od_df.filter(pl.col('REPAID') > 0).to_dicts()
with open(REPAID7B_OUT, 'wb') as f_repaid7b:
    for r in odr7b_rows:
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

# CREDITOD - OD accounts in ODCR but not in OD (B and NOT A)
odcr_full = con.execute(
    f"SELECT * FROM read_parquet('{CCRIS_DIR}/PREVODFT.parquet') "
    f"WHERE ACCTNO BETWEEN 3000000000 AND 3999999999"
).pl().sort('ACCTNO').unique(subset=['ACCTNO'], keep='first')

od_accts = set(od_df['ACCTNO'].to_list())
dispay_od = dispay.filter(
    (pl.col('ACCTNO') >= 3000000000) & (pl.col('ACCTNO') <= 3999999999)
).sort('ACCTNO').unique(subset=['ACCTNO'], keep='first')

# B AND NOT A: in odcr_full but not in od_accts
closed_od = odcr_full.filter(~pl.col('ACCTNO').is_in(list(od_accts)))
closed_od = closed_od.join(dispay_od, on='ACCTNO', how='left',
                           suffix='_DISP')

with open(CREDITOD_OUT, 'wb') as f_creditod:
    for r in closed_od.to_dicts():
        acctno_v = int(_nv(r.get('ACCTNO')))
        repaid_v  = _nv(r.get('REPAID_DISP', r.get('REPAID', 0)))  * 100
        disburse_v = _nv(r.get('DISBURSE_DISP', r.get('DISBURSE', 0))) * 100
        buf = _build_buf(250)
        _put_rec(buf, 1,   _fmt_n(_nv(r.get('FICODE', r.get('BRANCH', 0))), 9))
        _put_rec(buf, 10,  _fmt_n(_nv(r.get('APCODE', 0)), 3))
        _put_rec(buf, 13,  _fmt_n(acctno_v,                10))
        _put_rec(buf, 43,  _fmt_n(0,                       10))  # NOTENO=0 for closed OD
        _put_rec(buf, 73,  REPTDAY)
        _put_rec(buf, 75,  REPTMON)
        _put_rec(buf, 77,  REPTYEAR)
        _put_rec(buf, 81,  _fmt_z(_nv(r.get('OUTSTAND', 0)), 16))
        _put_rec(buf, 97,  _fmt_z(0, 3))   # ARREARS=0
        _put_rec(buf, 100, _fmt_z(0, 3))   # INSTALM=0
        _put_rec(buf, 103, _fmt_z(0, 17))  # UNDRAWN=0
        _put_rec(buf, 120, 'S')            # ACCTSTAT='S'
        _put_rec(buf, 121, _fmt_z(0, 5))   # NODAYS=0
        _put_rec(buf, 126, _fmt_n(0, 5))
        _put_rec(buf, 131, _fmt_z(0, 17))
        _put_rec(buf, 148, _fmt_z(0, 17))
        _put_rec(buf, 165, _fmt_n(0, 4))
        _put_rec(buf, 169, _fmt_s(_sv(r.get('AANO', '')), 13))
        _put_rec(buf, 182, _fmt_n(_nv(r.get('FACILITY', 0)), 5))
        _put_rec(buf, 187, _fmt_n(_nv(r.get('FACCODE', 0)), 5))
        _put_rec(buf, 192, _fmt_z(disburse_v, 15))
        _put_rec(buf, 207, _fmt_z(repaid_v,   15))
        f_creditod.write(bytes(buf) + b'\n')

# ---------------------------------------------------------------------------
# STEP 10: READ WRIOFAC fixed-width text
# ---------------------------------------------------------------------------
wriofac_rows = []
with open(WRITOFF_FILE, 'r', encoding='ascii', errors='replace') as fwf:
    for line in fwf:
        if len(line) < 75:
            continue
        acctno_w  = int(line[9:19].strip() or '0')
        noteno_w  = int(line[19:29].strip() or '0')
        restruct_w = line[73:74]
        if restruct_w == 'R': restruct_w = 'C'
        if restruct_w == 'S': restruct_w = 'T'
        wriofac_rows.append({'ACCTNO': acctno_w, 'NOTENO': noteno_w, 'RESTRUCT': restruct_w})

wriofac_df = pl.DataFrame(wriofac_rows).sort(['ACCTNO', 'NOTENO'])

# ---------------------------------------------------------------------------
# STEP 11: FAC (APP10 - LRECL ~700)
# ---------------------------------------------------------------------------
fac_rows = []
with open(APP10_FILE, 'r', encoding='ascii', errors='replace') as fapp:
    for line in fapp:
        if len(line) < 520:
            line = line.ljust(520)
        fac_rows.append({
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
        })

def _yn(val): return 'Y' if val else 'N'

factor_rows = []
for f in fac_rows:
    accept  = f['ACCEPT'];  margin  = f['MARGIN']
    adequ   = f['ADEQU'];   support = f['SUPPORT']
    gain    = f['GAIN'];    cgc     = f['CGC']
    guarant = f['GUARANT']; charac  = f['CHARAC']
    similr  = f['SIMILR'];  first   = f['FIRST']

    f1 = 'Y' if accept  else 'N'
    f2 = 'Y' if margin  else 'N'
    f3 = 'Y' if adequ   else 'N'
    f4 = 'Y' if support else 'N'
    f5 = 'Y' if (gain or cgc) else 'N'   # any one true => Y; both empty => N
    f6 = 'Y' if guarant else 'N'
    # FACTOR7: if any of charac/similar/first non-empty => Y; all empty => N
    f7 = 'Y' if (charac or similr or first) else 'N'

    factor_rows.append({
        'AANO':     f['AANO'],
        'FACTOR1':  f1, 'FACTOR2': f2, 'FACTOR3': f3, 'FACTOR4': f4,
        'FACTOR5':  f5, 'FACTOR6': f6, 'FACTOR7': f7,
        'CACCRSCK': f['CACCRSCK'], 'CADCHQCK': f['CADCHQCK'],
        'CADCHEQS': f['CADCHEQS'], 'CACCRIS':  f['CACCRIS'],
        'LEGALACC': f['LEGALACC'], 'LEGALBOR': f['LEGALBOR'],
    })

factor_df = pl.DataFrame(factor_rows).sort('AANO')

# TOTSC from APP7
totsc_rows = []
with open(APP7_FILE, 'r', encoding='ascii', errors='replace') as fapp7:
    for line in fapp7:
        if len(line) < 86:
            continue
        aano_v = line[0:13]
        sc_str = line[81:85].strip()
        totsc_rows.append({'AANO': aano_v, 'CRRTOTSC': int(sc_str) if sc_str.isdigit() else 0})

totsc_df = pl.DataFrame(totsc_rows).sort('AANO')
factor_df = factor_df.join(totsc_df, on='AANO', how='left').sort('AANO')

# Merge FACTOR into acctcred_df
acctcred_df = acctcred_df.sort('AANO')
acctcred_df = acctcred_df.join(factor_df, on='AANO', how='left', suffix='_FAC')
for fld in ['FACTOR1','FACTOR2','FACTOR3','FACTOR4','FACTOR5','FACTOR6','FACTOR7']:
    acctcred_df = acctcred_df.with_columns(
        pl.when(pl.col(fld).is_null() | (pl.col(fld) == ''))
        .then(pl.lit('N'))
        .otherwise(pl.col(fld))
        .alias(fld)
    )
# ACCTIND = 'E' when both A and B
acctcred_df = acctcred_df.with_columns(
    pl.when(pl.col('CRRTOTSC').is_not_null())
    .then(pl.lit('E'))
    .otherwise(pl.col('ACCTIND'))
    .alias('ACCTIND')
)

# ---------------------------------------------------------------------------
# STEP 12: ELN12, ELN5, ELN1 -> SMC
# ---------------------------------------------------------------------------
def _read_eln_firstobs2(filepath, parsefn):
    rows = []
    with open(filepath, 'r', encoding='ascii', errors='replace') as fh:
        next(fh, None)  # skip header (FIRSTOBS=2)
        for line in fh:
            r = parsefn(line)
            if r:
                rows.append(r)
    return rows

def _parse_eln12(line):
    if len(line) < 55: return None
    newid    = line[0:12].upper().strip()
    reviewno = line[15:32].upper().strip()
    acctno_v = line[35:55].strip()
    if not newid and not reviewno: return None
    try: acctno_n = int(acctno_v)
    except: acctno_n = 0
    return {'NEWID': newid, 'REVIEWNO': reviewno, 'ACCTNO': acctno_n}

def _parse_eln5(line):
    if len(line) < 410: return None
    newid    = line[0:12].strip()
    reviewno = line[15:32].upper().strip()
    if not newid and not reviewno: return None
    return {
        'NEWID': newid, 'REVIEWNO': reviewno,
        'SMCRITE1': line[386:387].upper().strip(),
        'SMCRITE2': line[390:391].upper().strip(),
        'SMCRITE3': line[394:395].upper().strip(),
        'SMCRITE4': line[398:399].upper().strip(),
        'SMCRITE5': line[402:403].upper().strip(),
        'SMCRITE6': line[406:407].upper().strip(),
    }

def _parse_eln1(line):
    if len(line) < 625: return None
    newid    = line[0:12].upper().strip()
    reviewno = line[15:32].upper().strip()
    if not newid and not reviewno: return None
    crr1_v   = line[188:191].strip()
    nxrvwdd_v = int(line[613:615].strip() or '0')
    nxrvwmm_v = int(line[616:618].strip() or '0')
    nxrvwyy_v = int(line[619:623].strip() or '0')
    rvdate_v  = line[1009:1019].strip().replace('/', '')
    nxrvdate  = 0
    try:
        nxrvdate = (date(nxrvwyy_v, nxrvwmm_v, nxrvwdd_v) - date(1960,1,1)).days
    except: pass
    return {
        'NEWID': newid, 'REVIEWNO': reviewno, 'CRR1': crr1_v,
        'NXRVWDD': nxrvwdd_v, 'NXRVWMM': nxrvwmm_v, 'NXRVWYY': nxrvwyy_v,
        'EREVDATE': rvdate_v, 'NXRVDATE': nxrvdate,
    }

eln12_df = pl.DataFrame(_read_eln_firstobs2(ELDSRV12_FILE, _parse_eln12)).sort(['NEWID','REVIEWNO'])
eln5_df  = pl.DataFrame(_read_eln_firstobs2(ELDSRV5_FILE,  _parse_eln5 )).sort(['NEWID','REVIEWNO'])
eln1_df  = pl.DataFrame(_read_eln_firstobs2(ELDSRV1_FILE,  _parse_eln1 )).sort(['NEWID','REVIEWNO'])

smc_df = eln12_df.join(eln5_df, on=['NEWID','REVIEWNO'], how='left', suffix='_5')
smc_df = smc_df.join(eln1_df,  on=['NEWID','REVIEWNO'], how='left', suffix='_1')
smc_df = smc_df.unique(subset=['ACCTNO'], keep='first').sort('ACCTNO')

acctcred_df = acctcred_df.sort('ACCTNO')
acctcred_df = acctcred_df.join(
    smc_df.select(['ACCTNO','SMCRITE1','SMCRITE2','SMCRITE3','SMCRITE4',
                   'SMCRITE5','SMCRITE6','CRR1','EREVDATE','NXRVDATE']),
    on='ACCTNO', how='left', suffix='_SMC'
)
for fld in ['SMCRITE1','SMCRITE2','SMCRITE3','SMCRITE4','SMCRITE5','SMCRITE6']:
    acctcred_df = acctcred_df.with_columns(
        pl.when(pl.col(fld).is_null() | (pl.col(fld) == ''))
        .then(pl.lit('N'))
        .otherwise(pl.col(fld))
        .alias(fld)
    )

# NXRVWDD/MM/YY from NXRVDATE
def _derive_nxrvw(df):
    rows = df.to_dicts()
    out = []
    for r in rows:
        nd = _sas_date(_nv(r.get('NXRVDATE')))
        r['NXRVWDD'] = f"{nd.day:02d}"    if nd else '00'
        r['NXRVWMM'] = f"{nd.month:02d}"  if nd else '00'
        r['NXRVWYY'] = str(nd.year)        if nd else '0000'
        out.append(r)
    return pl.DataFrame(out)

acctcred_df = _derive_nxrvw(acctcred_df)

# ---------------------------------------------------------------------------
# STEP 13: CM (BNM.LNCOMM)
# ---------------------------------------------------------------------------
lncomm_df = con.execute(
    f"SELECT ACCTNO, COMMNO, EXPIREDT FROM read_parquet('{BNM_DIR}/LNCOMM.parquet')"
).pl()

def _derive_cm(df):
    rows = df.to_dicts()
    out = []
    for r in rows:
        expiredt_v = _nv(r.get('EXPIREDT'))
        cmmaturdt  = ''
        if expiredt_v not in (0, None):
            ed = _mmddyy_from_z11(expiredt_v)
            if ed:
                cmmaturdt = f"{ed.year}{ed.month:02d}{ed.day:02d}"
        r['CMMATURDT'] = cmmaturdt
        out.append(r)
    return pl.DataFrame(out)

cm_df = _derive_cm(lncomm_df).sort(['ACCTNO','COMMNO'])

acctcred_df = acctcred_df.sort(['ACCTNO','COMMNO'])
acctcred_df = acctcred_df.join(
    cm_df.select(['ACCTNO','COMMNO','CMMATURDT']),
    on=['ACCTNO','COMMNO'], how='left', suffix='_CM'
)

# ---------------------------------------------------------------------------
# STEP 14: Merge WRIOFAC, update ACCTSTAT
# ---------------------------------------------------------------------------
acctcred_df = acctcred_df.sort(['ACCTNO','NOTENO'])

def _apply_wriofac(df, wriofac):
    wmap = {(r['ACCTNO'], r['NOTENO']): r['RESTRUCT']
            for r in wriofac.to_dicts()}
    rows = df.to_dicts()
    out  = []
    for r in rows:
        key = (int(_nv(r.get('ACCTNO'))), int(_nv(r.get('NOTENO'))))
        wr  = wmap.get(key)
        if wr is not None:
            apcode  = _nv(r.get('APCODE'))
            paidind = _sv(r.get('PAIDIND'))
            if apcode not in (983, 993) and paidind != 'P':
                if   wr == 'C': r['ACCTSTAT'] = 'C'
                elif wr == 'T': r['ACCTSTAT'] = 'T'

        # COMPLIDT components
        complidt_v = _nv(r.get('COMPLIDT'))
        if complidt_v > 0:
            cd = _sas_date(complidt_v)
            r['COMPLIDD'] = f"{cd.day:02d}"    if cd else '00'
            r['COMPLIMM'] = f"{cd.month:02d}"  if cd else '00'
            r['COMPLIYY'] = str(cd.year)        if cd else '0000'
        else:
            r['COMPLIDD'] = '00'; r['COMPLIMM'] = '00'; r['COMPLIYY'] = '0000'

        # WRIOFF_CLOSE_FILE_TAG_DT
        wrioff_dt = _nv(r.get('WRIOFF_CLOSE_FILE_TAG_DT'))
        if wrioff_dt > 0:
            wd = _sas_date(wrioff_dt)
            r['WRIOFFDD'] = f"{wd.day:02d}"    if wd else '00'
            r['WRIOFFMM'] = f"{wd.month:02d}"  if wd else '00'
            r['WRIOFFYY'] = str(wd.year)        if wd else '0000'

        # SM_DATE
        sm_date_v = _nv(r.get('SM_DATE'))
        if sm_date_v > 0:
            sd = _sas_date(sm_date_v)
            if sd:
                r['SM_DAT1'] = f"{sd.day:02d}{sd.month:02d}{sd.year}"
        else:
            r['SM_DAT1']  = '00000000'
            r['ACCTIND']  = ''

        if _sv(r.get('SM_STATUS')) != 'Y': r['SM_STATUS'] = 'N'

        # OLDRR / USER5 = 'R'
        user5_v  = _sv(r.get('USER5'))
        oldrr_v  = _sv(r.get('OLDRR'))
        if user5_v == 'R' or oldrr_v == 'R':
            r['OLDRRACC'] = 'R'
            if user5_v == 'R' or (oldrr_v == 'R' and
               _sas_date(_nv(r.get('DLVDATE'))) < date(2016,3,1)
               if _sas_date(_nv(r.get('DLVDATE'))) else True):
                r['RESTRUCT'] = ' '
                if r.get('ACCTSTAT') not in ('S','W','P'): r['ACCTSTAT'] = 'O'

        # AKPK HP products
        loantype_v = _nv(r.get('LOANTYPE'))
        akpk_v     = _sv(r.get('AKPK_STATUS'))
        noteno_v   = _nv(r.get('NOTENO'))
        if loantype_v in {128,130,131,132,380,381,700,705,720,725,678,679,698,699,983,993}:
            if akpk_v in ('AKPK','RAKPK') and r.get('ACCTSTAT') not in ('S','F'):
                r['ACCTSTAT'] = 'K'
            if akpk_v == 'RAKPK' and noteno_v >= 98000:
                r['RESTRUCT'] = ''
                r['RRTAG']    = akpk_v

        # HISTBAL
        dlvrmm_v = _sv(r.get('DLVRMM'))
        r['HISTBAL'] = _nv(r.get('OUTSTAND')) if dlvrmm_v == REPTMON else 0

        # FLOODIND
        if (_sv(r.get('MODELDES'))[-2:-1] == 'F' if len(_sv(r.get('MODELDES'))) >= 5 else False) or \
           (_sv(r.get('RRIND'))[1:2] == 'F' if len(_sv(r.get('RRIND'))) >= 2 else False):
            if _sv(r.get('RRTAG')).strip():
                r['FLOODIND'] = 'Y'

        # HP RAM models special DLVR dates
        ltype = loantype_v
        md_v  = _sv(r.get('MODELDES'))
        if ltype in HP:
            r['LEGIND'] = ' '
            if md_v in ('RARM1','RAM1'):
                r['DLVRDD'] = '31'; r['DLVRMM'] = '12'; r['DLVRYR'] = '2007'
            elif md_v in ('RARM2','RAM2'):
                r['DLVRDD'] = '31'; r['DLVRMM'] = '07'; r['DLVRYR'] = '2011'
            elif md_v in ('RARM3','RAM3'):
                r['DLVRDD'] = '31'; r['DLVRMM'] = '05'; r['DLVRYR'] = '2012'
            elif md_v == 'RAM4':
                r['DLVRDD'] = '31'; r['DLVRMM'] = '12'; r['DLVRYR'] = '2012'
            elif md_v == 'RAM5':
                r['DLVRDD'] = '30'; r['DLVRMM'] = '04'; r['DLVRYR'] = '2014'

        # MODELDES='Z'
        if md_v == 'Z':
            r['RRTAG'] = 'Z'; r['CYC'] = 6
            dlivrydt_v = _nv(r.get('DLIVRYDT'))
            if dlivrydt_v > 0:
                dd, mm, yy = _z11_parts(dlivrydt_v)
                r['DLVRDD'] = dd; r['DLVRMM'] = mm; r['DLVRYR'] = yy

        # NOTETERM from LNACC4 data
        acty_v = _sv(r.get('ACTY'))
        if acty_v == 'LN':
            notemat_v  = _nv(r.get('NOTEMAT'))
            issuedt_v2 = _nv(r.get('ISSUEDT'))
            notemat2   = _mmddyy_from_z11(notemat_v)  if notemat_v  > 0 else None
            issuedat   = _mmddyy_from_z11(issuedt_v2) if issuedt_v2 > 0 else None
            issuyear   = issuedat.year  if issuedat else 0
            issumont   = issuedat.month if issuedat else 0
            fac_v      = _nv(r.get('FACILITY'))
            cmmaturdt  = _sv(r.get('CMMATURDT'))
            if fac_v == 34210 and cmmaturdt:
                try:
                    ntyr = (int(cmmaturdt[:4]) - issuyear) * 12
                    ntmt = int(cmmaturdt[4:6]) - issumont
                    r['NOTETERM'] = ntyr + ntmt
                except: pass
            elif fac_v != 34210 and notemat2:
                ntyr = (notemat2.year  - issuyear) * 12
                ntmt =  notemat2.month - issumont
                r['NOTETERM'] = ntyr + ntmt

        # FACCODE 34333/34334 WFDATE LASTMDATE
        faccode_v2 = _nv(r.get('FACCODE'))
        wfdate_v   = _sv(r.get('WFDATE'))
        if faccode_v2 in (34333, 34334) and wfdate_v:
            try:
                mm_w = int(wfdate_v[:2]);  yy_w = int(wfdate_v[3:7])
                mm1_w = mm_w + 1;          yy1_w = yy_w
                if mm1_w > 12: mm1_w = 1;  yy1_w += 1
                import calendar
                last = calendar.monthrange(yy1_w, mm1_w)[1]
                end_d = date(yy1_w, mm1_w, last)
                r['LASTMDATE'] = f"{end_d.year}{end_d.month:02d}{end_d.day:02d}"
            except: pass

        out.append(r)
    return pl.DataFrame(out)

acctcred_df = _apply_wriofac(acctcred_df, wriofac_df)

# ---------------------------------------------------------------------------
# STEP 15: ELN3 (ELDSTX3)
# ---------------------------------------------------------------------------
eln3_rows = []
with open(ELDSTX3_FILE, 'r', encoding='ascii', errors='replace') as feln3:
    next(feln3, None)   # FIRSTOBS=2
    for line in feln3:
        if len(line) < 35:
            continue
        eln3_rows.append({'AANO': line[0:13].upper().strip(), 'APVBY': line[32:34].upper().strip()})

eln3_df = pl.DataFrame(eln3_rows).sort('AANO')

# ---------------------------------------------------------------------------
# STEP 16: CREDIT dataset (copy of ACCTCRED + computed ISSUED, AADATE)
# ---------------------------------------------------------------------------
def _derive_credit_dates(df):
    rows = df.to_dicts()
    out  = []
    for r in rows:
        issuemm = _nv(r.get('ISSUEMM')); issuedd = _nv(r.get('ISSUEDD'))
        issueyy = _nv(r.get('ISSUEYY'))
        try:
            r['ISSUED'] = (date(int(issueyy), int(issuemm), int(issuedd)) - date(1960,1,1)).days
        except: r['ISSUED'] = 0

        aadate_v = _nv(r.get('AADATE'))
        ad = _sas_date(aadate_v) if aadate_v > 0 else None
        r['AADATEDD'] = f"{ad.day:02d}"    if ad else '00'
        r['AADATEMM'] = f"{ad.month:02d}"  if ad else '00'
        r['AADATEYY'] = str(ad.year)        if ad else '0000'
        out.append(r)
    return pl.DataFrame(out)

credit = _derive_credit_dates(acctcred_df).sort('AANO')
credit = credit.join(eln3_df, on='AANO', how='left', suffix='_ELN3').sort('ACCTNO')

# ---------------------------------------------------------------------------
# STEP 17: LNACC4 merge
# ---------------------------------------------------------------------------
lnacc4_df = con.execute(
    f"SELECT ACCTNO, AANO, PRODUCT, LNTERM, MNIAPDTE, REVIEWDT, "
    f"FIRST_DISBDT, SETTLEMNTDT, TOTACBAL, MNIAPLMT, EXPRDATE "
    f"FROM read_parquet('{BNM_DIR}/LNACC4.parquet')"
).pl()

def _derive_lnacc4(df):
    rows = df.to_dicts()
    out  = []
    for r in rows:
        r['ACCT_AANO']   = _sv(r.get('AANO'))
        r['ACCT_LNTYPE'] = _nv(r.get('PRODUCT'))
        r['ACCT_LNTERM'] = _nv(r.get('LNTERM'))
        r['ACCT_OUTBAL'] = _nv(r.get('TOTACBAL')) * 100
        r['ACCT_APLMT']  = _nv(r.get('MNIAPLMT')) * 100

        for (src_fld, dd_out, mm_out, yy_out) in [
            ('REVIEWDT',    'ACCT_REVDD', 'ACCT_REVMM', 'ACCT_REVYY'),
            ('FIRST_DISBDT','ACCT_1DISDD','ACCT_1DISMM','ACCT_1DISYY'),
            ('SETTLEMNTDT', 'ACCT_SETDD', 'ACCT_SETMM', 'ACCT_SETYY'),
            ('EXPRDATE',    'ACCT_EXPDD', 'ACCT_EXPMM', 'ACCT_EXPYY'),
            ('MNIAPDTE',    'ACCT_ALDD',  'ACCT_ALMM',  'ACCT_ALYY'),
        ]:
            v = _nv(r.get(src_fld))
            if v > 0:
                d = _mmddyy_from_z11(v)
                r[dd_out] = f"{d.day:02d}"    if d else '00'
                r[mm_out] = f"{d.month:02d}"  if d else '00'
                r[yy_out] = str(d.year)        if d else '0000'
            else:
                r[dd_out] = '00'; r[mm_out] = '00'; r[yy_out] = '0000'
        out.append(r)
    return pl.DataFrame(out)

lnacc4_out = _derive_lnacc4(lnacc4_df).sort('ACCTNO')
credit = credit.join(lnacc4_out, on='ACCTNO', how='left', suffix='_LA4')

# ---------------------------------------------------------------------------
# STEP 18: MONTHLY macro - CCRIS.COLLATER and CCRIS.WRITOFF (if WK='4')
# ---------------------------------------------------------------------------
if NOWK == '4':
    credit.write_parquet(str(CCRIS_DIR / 'COLLATER.parquet'))
    credit.filter(pl.col('ACCTSTAT') == 'W') \
          .select(['ACCTNO','NOTENO','ACCTSTAT','RESTRUCT']) \
          .write_parquet(str(CCRIS_DIR / 'WRITOFF.parquet'))

# ---------------------------------------------------------------------------
# STEP 19: PROC SORT -> de-dup CREDIT by BRANCH/ACCTNO/AANO/COMMNO/NOTENO
# ---------------------------------------------------------------------------
credit = credit.sort(['BRANCH','ACCTNO','AANO','COMMNO','NOTENO'])
credit = credit.unique(subset=['BRANCH','ACCTNO','AANO','COMMNO','NOTENO'], keep='first')

# Filter: keep OD, or (COMMNO=0 AND IND='A') OR (COMMNO>0 AND IND='B') OR AANO non-blank
def _keep_filter(r):
    acty_v  = _sv(r.get('ACTY'))
    commno_v = _nv(r.get('COMMNO'))
    ind_v   = _sv(r.get('IND'))
    aano_v  = _sv(r.get('AANO')).strip()
    return (acty_v == 'OD' or
            (commno_v == 0 and ind_v == 'A') or
            (commno_v > 0  and ind_v == 'B') or
            aano_v != '')

credit = credit.filter(
    (pl.col('ACTY') == 'OD') |
    ((pl.col('COMMNO') == 0) & (pl.col('IND') == 'A')) |
    ((pl.col('COMMNO') > 0)  & (pl.col('IND') == 'B')) |
    (pl.col('AANO').str.strip_chars() != '')
)

# PROC SUMMARY: LIMTCURR sum by BRANCH/ACCTNO/AANO
acctcd = credit.group_by(['BRANCH','ACCTNO','AANO']).agg(
    pl.col('LIMTCURR').sum().alias('LIMTCURR')
)

# FIRST record per BRANCH/ACCTNO/AANO for CREDIT1
credit1 = credit.sort(['BRANCH','ACCTNO','AANO']).unique(
    subset=['BRANCH','ACCTNO','AANO'], keep='first'
)

# Merge CREDIT1 with ACCTCD (sum of LIMTCURR)
acctcrex = credit1.join(acctcd, on=['BRANCH','ACCTNO','AANO'], how='left', suffix='_SUM')
# Use the summed LIMTCURR
acctcrex = acctcrex.with_columns(
    pl.when(pl.col('LIMTCURR_SUM').is_not_null())
    .then(pl.col('LIMTCURR_SUM'))
    .otherwise(pl.col('LIMTCURR'))
    .alias('LIMTCURR')
)

# ---------------------------------------------------------------------------
# STEP 20: Write ACCTCRED file (LRECL=800)
# ---------------------------------------------------------------------------
with open(ACCTCRED_OUT, 'wb') as f_acctcred:
    for r in acctcrex.to_dicts():
        acctno_v = int(_nv(r.get('ACCTNO')))
        auth_lim = _nv(r.get('AUTHORISE_LIMIT')) * 100
        oper_lim = _nv(r.get('OPERLIMT'))        * 100
        limtcurr = _nv(r.get('LIMTCURR'))
        lmtamt_v = _nv(r.get('LMTAMT'))
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
print("EIIWCCR5 completed successfully.")
print(f"  ACCTCRED -> {ACCTCRED_OUT}")
print(f"  SUBACRED -> {SUBACRED_OUT}")
print(f"  CREDITPO -> {CREDITPO_OUT}")
print(f"  PROVISIO -> {PROVISIO_OUT}")
print(f"  LEGALACT -> {LEGALACT_OUT}")
print(f"  CREDITOD -> {CREDITOD_OUT}")
print(f"  REPAID7B -> {REPAID7B_OUT}")
