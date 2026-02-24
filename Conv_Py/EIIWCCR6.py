#!/usr/bin/env python3
"""
Program : EIIWCCR6
Function: CCRIS Enhancement - Generate CCRIS submission files from BNM/ELDS data.
          Produces:
            - ACCTCRED  (LRECL=900)  : Account Credit master record
            - SUBACRED  (LRECL=950)  : Sub-account Credit detail
            - CREDITPO  (LRECL=400)  : Credit position
            - PROVISIO  (LRECL=400)  : Provision data (NPL accounts)
            - LEGALACT  (LRECL=100)  : Legal action
            - CREDITOD  (LRECL=210)  : Credit OD (commented out in original)
            - ACCTREJT  (LRECL=200)  : Account rejects / HP rejects
"""

import sys
import math
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import polars as pl
import duckdb

# ---------------------------------------------------------------------------
# Dependency: PBBLNFMT  (format definitions from PBB loan format module)
# %INC PGM(PBBLNFMT);
# ---------------------------------------------------------------------------
from PBBLNFMT import (
    format_oddenom, format_lndenom, format_lnprod,
    format_odcustcd, format_locustcd, format_lncustcd,
    format_statecd, format_apprlimt, format_loansize,
    format_mthpass, format_lnormt, format_lnrmmt,
    format_collcd, format_riskcd, format_busind,

    # format_lntype, format_product, format_purpose,
    # format_prisec, format_prisecd, format_secsec,
    # format_secseccd, format_othsec, format_othseccd,
)

# ---------------------------------------------------------------------------
# Dependency: PFBCRFMT  (BNM taxonomy format definitions)
# %INC PGM(PFBCRFMT);
# ---------------------------------------------------------------------------
# Import all format functions from the migrated PFBCRFMT module
from PFBCRFMT import (
    format_faccode, format_odfaccode, format_facname,
    format_fundsch, format_fschemec, format_synd,
    format_odfundsch, format_odfacility, format_fconcept,
    format_odfconcept, format_fincept, format_cpurphp,
    format_lnprodc, format_purphp,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
BASE_DIR = Path('/data')

# Input parquet datasets
BNM_DIR      = BASE_DIR / 'bnm/mniln'          # SAP.PIBB.MNILN(0)
BNMPREV_DIR  = BASE_DIR / 'bnm/mniln_prev'     # SAP.PIBB.MNILN(-4)
BNMD_DIR     = BASE_DIR / 'bnmd'               # SAP.PIBB.MNILN.DAILY(0)
CCRIS_DIR    = BASE_DIR / 'ccris/sasdata'      # SAP.PIBB.CCRIS.SASDATA
CCRISP_DIR   = BASE_DIR / 'ccris/split'        # SAP.PIBB.CCRIS.SPLIT
FORATE_DIR   = BASE_DIR / 'forate'             # SAP.PBB.FCYCA
HPREJ_DIR    = BASE_DIR / 'hprej'             # SAP.ELDS.HPREJ.BACKDATE(0)
BNMSUM_DIR   = BASE_DIR / 'bnmsum'            # SAP.PBB.ELDS.ISS3(0)

# Input text files
WRITOFF_FILE = BASE_DIR / 'program/wriofac.txt'   # SAP.BNM.PROGRAM(WRIOFAC)

# Output text files
OUT_DIR = BASE_DIR / 'output/ccris2'
OUT_DIR.mkdir(parents=True, exist_ok=True)

ACCTCRED_FILE = OUT_DIR / 'acctcred.txt'   # LRECL=900
SUBACRED_FILE = OUT_DIR / 'subacred.txt'   # LRECL=950
CREDITPO_FILE = OUT_DIR / 'creditpo.txt'   # LRECL=400
PROVISIO_FILE = OUT_DIR / 'provisio.txt'   # LRECL=400
LEGALACT_FILE = OUT_DIR / 'legalact.txt'   # LRECL=100
CREDITOD_FILE = OUT_DIR / 'creditod.txt'   # LRECL=210 (commented out in original)
ACCTREJT_FILE = OUT_DIR / 'acctrejt.txt'   # LRECL=200

# ---------------------------------------------------------------------------
# PRODUCT GROUP DEFINITIONS  (%LET macros)
# ---------------------------------------------------------------------------
HP    = {128, 130, 380, 381, 700, 705, 983, 993, 996}
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
         355, 356, 357, 358, 391, 148, 174, 195}
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
PRDJ  = {4, 5, 6, 7, 15, 20, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 60, 61, 62, 63,
         70, 71, 72, 73, 74, 75, 76, 77, 78, 100, 108}

ALP   = set('ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ')

# ---------------------------------------------------------------------------
# HELPER UTILITIES
# ---------------------------------------------------------------------------

def coalesce(*args):
    for a in args:
        if a is not None and a != '' and not (isinstance(a, float) and math.isnan(a)):
            return a
    return None

def safe_int(v, default=0) -> int:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return default
    try:
        return int(v)
    except (ValueError, TypeError):
        return default

def safe_float(v, default=0.0) -> float:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default

def safe_str(v, default='') -> str:
    if v is None:
        return default
    return str(v)

def mdy(mm, dd, yy) -> Optional[date]:
    try:
        mm, dd, yy = int(mm), int(dd), int(yy)
        if mm == 0 or dd == 0 or yy == 0:
            return None
        return date(yy, mm, dd)
    except Exception:
        return None

def date_from_z11(val) -> Optional[date]:
    """Parse a Z11 numeric date stored as MMDDYYYY (first 8 chars of zero-padded 11-digit)."""
    try:
        s = f'{int(val):011d}'
        mm = int(s[0:2])
        dd = int(s[2:4])
        yy = int(s[4:8])
        return mdy(mm, dd, yy)
    except Exception:
        return None

def fmt_z(v, width: int) -> str:
    """Format integer as zero-padded string of given width."""
    try:
        return f'{int(v):0{width}d}'
    except Exception:
        return '0' * width

def fmt_n(v, width: int) -> str:
    """Format integer right-justified in given width."""
    try:
        return f'{int(v):{width}d}'
    except Exception:
        return ' ' * width

def fmt_f(v, width: int, dec: int) -> str:
    """Format float with fixed decimals right-justified."""
    try:
        return f'{float(v):{width}.{dec}f}'
    except Exception:
        return ' ' * width

def fmt_s(v, width: int) -> str:
    """Format string left-justified padded to width."""
    s = safe_str(v)
    return s[:width].ljust(width)

def fmt_ddmmyy8(dt) -> str:
    """Format date as DD/MM/YY (8 chars with slashes)."""
    if dt is None:
        return '        '
    try:
        if isinstance(dt, (int, float)):
            dt = date.fromordinal(int(dt) + date(1960, 1, 1).toordinal() - 1)
        return dt.strftime('%d/%m/%y')
    except Exception:
        return '        '

def build_record(fields: list[tuple]) -> str:
    """
    Build a fixed-width record from a list of (start_pos_1based, value_str) tuples.
    start_pos is 1-based column position.
    """
    # find max width needed
    max_col = max(p + len(v) for p, v in fields)
    buf = list(' ' * max_col)
    for pos, val in fields:
        for i, ch in enumerate(val):
            idx = pos - 1 + i
            if idx < max_col:
                buf[idx] = ch
    return ''.join(buf)

# ---------------------------------------------------------------------------
# NDAYS FORMAT  (SAS INFORMAT NDAYS.)
# Returns arrears bucket code from number of days overdue
# ---------------------------------------------------------------------------

def ndays_format(nodays: int) -> int:
    """Convert number of days overdue to arrears instalment code (SAS NDAYS informat)."""
    if nodays <= 0:
        return 0
    if nodays <= 30:
        return 1
    if nodays <= 60:
        return 2
    if nodays <= 90:
        return 3
    if nodays <= 120:
        return 4
    if nodays <= 150:
        return 5
    if nodays <= 180:
        return 6
    if nodays <= 210:
        return 7
    if nodays <= 240:
        return 8
    if nodays <= 270:
        return 9
    if nodays <= 300:
        return 10
    if nodays <= 330:
        return 11
    if nodays <= 365:
        return 12
    # > 365 days: 24 means "use round(days/30)"
    return 24

# ---------------------------------------------------------------------------
# LOAD DATES FROM BNM.REPTDATE
# ---------------------------------------------------------------------------

def load_dates(bnm_dir: Path) -> dict:
    """Load and derive date variables from BNM.REPTDATE parquet."""
    path = bnm_dir / 'reptdate.parquet'
    con = duckdb.connect()
    row = con.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 1").fetchone()
    cols = [d[0] for d in con.description]
    con.close()
    rec = dict(zip(cols, row))

    reptdate = rec['REPTDATE']
    if not isinstance(reptdate, date):
        reptdate = date.fromordinal(int(reptdate) + date(1960, 1, 1).toordinal() - 1)

    day = reptdate.day
    mm  = reptdate.month
    yy  = reptdate.year

    if day == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif day == 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif day == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'

    if wk == '1':
        mm1 = mm - 1 if mm > 1 else 12
    else:
        mm1 = mm

    if wk == '4':
        mm2 = mm
    else:
        mm2 = mm - 1 if mm > 1 else 12

    sdate = date(yy, mm, day)
    idate = date(yy, mm, 1)
    # XDATE = first day of the month before current month
    xdate = date(yy if mm > 1 else yy - 1, mm - 1 if mm > 1 else 12, 1)
    currdte = date.today() - __import__('datetime').timedelta(days=1)

    # SAS date ordinals (days since 1960-01-01)
    sas_epoch = date(1960, 1, 1)
    def to_sas(d: date) -> int:
        return (d - sas_epoch).days

    return {
        'reptdate': reptdate, 'nowk': wk, 'nowk1': wk1,
        'reptmon':  f'{mm:02d}', 'reptmon1': f'{mm1:02d}', 'reptmon2': f'{mm2:02d}',
        'reptyear': str(yy), 'reptyr2': str(yy)[2:],
        'rdate':    reptdate.strftime('%d/%m/%y'),
        'reptday':  f'{day:02d}',
        'sdate':    to_sas(sdate), 'idate': to_sas(idate),
        'xdate':    to_sas(xdate), 'mdate': to_sas(reptdate),
        'rdate1':   to_sas(reptdate),
        'rdate2':   reptdate.strftime('%y%m%d'),
        'currdte':  currdte,
        'sas_epoch': sas_epoch,
    }

# ---------------------------------------------------------------------------
# LOAD FX RATES
# ---------------------------------------------------------------------------

def load_fx_rates(forate_dir: Path, rdate1: int) -> dict:
    """
    Load FX rates from FORATE.FORATEBKP parquet.
    Keep latest rate per currency <= rdate1 (SAS date).
    Returns dict of {CURCODE: SPOTRATE}.
    """
    path = forate_dir / 'foratebkp.parquet'
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT CURCODE, SPOTRATE
        FROM (
            SELECT CURCODE, SPOTRATE, REPTDATE,
                   ROW_NUMBER() OVER (PARTITION BY CURCODE ORDER BY REPTDATE DESC) AS rn
            FROM read_parquet('{path}')
            WHERE REPTDATE <= {rdate1}
        ) t
        WHERE rn = 1
    """).fetchall()
    con.close()
    return {r[0]: r[1] for r in rows}

# ---------------------------------------------------------------------------
# LOAD BLR / LENDING RATES
# ---------------------------------------------------------------------------

def load_blrate(bnm_dir: Path, bnmprev_dir: Path) -> tuple[str, str, str]:
    """
    Load current (IDX=BNM.LNRATE) and previous (BNMPREV.LNRATE) lending rates.
    Returns (blrate, blreff, olrate).
    """
    blrate = '0.00'
    blreff_sas = 0
    olrate = '0.00'

    for parq, is_idx in [
        (bnm_dir / 'lnrate.parquet', True),
        (bnmprev_dir / 'lnrate.parquet', False),
    ]:
        if not parq.exists():
            continue
        con = duckdb.connect()
        rows = con.execute(
            f"SELECT CURRATE, EFFDATE FROM read_parquet('{parq}') WHERE RINDEX=1 LIMIT 1"
        ).fetchall()
        con.close()
        for row in rows:
            currate_str = f'{float(row[0]):.2f}'
            if is_idx:
                blrate = currate_str
                blreff_sas = row[1]
            olrate = currate_str

    return blrate, blreff_sas, olrate

# ---------------------------------------------------------------------------
# LOAD WRIOFAC (write-off file)
# ---------------------------------------------------------------------------

def load_wriofac(filepath: Path) -> dict:
    """
    Parse WRITOFF text file.
    INPUT @10 ACCTNO 10. @20 NOTENO 10. @74 RESTRUCT $1.
    Returns dict keyed by (ACCTNO, NOTENO) -> {RESTRUCT, ACCTSTAT}
    """
    result = {}
    if not filepath.exists():
        logger.warning(f"WRITOFF file not found: {filepath}")
        return result
    with open(filepath, 'r', encoding='latin-1') as f:
        for line in f:
            line = line.rstrip('\n')
            if len(line) < 74:
                continue
            acctno  = safe_int(line[9:19].strip(), 0)
            noteno  = safe_int(line[19:29].strip(), 0)
            restruct = line[73:74] if len(line) >= 74 else ' '
            if restruct == 'R':
                restruct = 'C'
            if restruct == 'S':
                restruct = 'T'
            acctstat = ''
            if restruct == 'C':
                acctstat = 'C'
            elif restruct == 'T':
                acctstat = 'T'
            result[(acctno, noteno)] = {'RESTRUCT': restruct, 'ACCTSTAT': acctstat}
    return result

# ---------------------------------------------------------------------------
# MAIN PROCESSING
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting EIIWCCR6")

    # --- Load date context ---
    ctx = load_dates(BNM_DIR)
    reptday   = ctx['reptday']
    reptmon   = ctx['reptmon']
    reptyear  = ctx['reptyear']
    nowk      = ctx['nowk']
    rdate1    = ctx['rdate1']
    sdate_sas = ctx['sdate']
    idate_sas = ctx['idate']
    xdate_sas = ctx['xdate']
    rdate2    = ctx['rdate2']
    sas_epoch = ctx['sas_epoch']
    blreff_sas = 0

    logger.info(f"REPTDATE={ctx['reptdate']} WK={nowk} RDATE={ctx['rdate']}")

    def sas_to_date(n) -> Optional[date]:
        try:
            return sas_epoch + __import__('datetime').timedelta(days=int(n))
        except Exception:
            return None

    # --- FX Rates ---
    fx_rates = load_fx_rates(FORATE_DIR, rdate1)

    def get_forate(curcode: str) -> float:
        return float(fx_rates.get(str(curcode).strip(), 1.0))

    # --- BLR rates ---
    blrate, blreff_sas, olrate = load_blrate(BNM_DIR, BNMPREV_DIR)
    logger.info(f"BLRATE={blrate} OLRATE={olrate} BLREFF={blreff_sas}")

    # --- WRIOFAC ---
    wriofac = load_wriofac(WRITOFF_FILE)

    # --- Load CCRISP.ELDS (main loan/OD source) ---
    elds_path = CCRISP_DIR / 'elds.parquet'
    con = duckdb.connect()
    elds_df = con.execute(f"SELECT * FROM read_parquet('{elds_path}')").pl()
    con.close()

    # --- Load HP Reject ---
    hprej_path = HPREJ_DIR / 'ehpa123.parquet'
    con = duckdb.connect()
    hprj_df = con.execute(f"""
        SELECT AANO, ICDATE, CCRIS_STAT
        FROM read_parquet('{hprej_path}')
        WHERE CCRIS_STAT = 'D'
          AND ICDATE >= {xdate_sas}
          AND ICDATE <= {sdate_sas}
    """).pl()
    con.close()

    # --- Load RVNOTE from BNMD ---
    rvnote_path = BNMD_DIR / 'rvnote.parquet'
    con = duckdb.connect()
    rvnote_df = con.execute(f"SELECT * FROM read_parquet('{rvnote_path}')").pl()
    con.close()

    # Process RVNOTE
    rvnote_records = []
    for row in rvnote_df.to_dicts():
        vinno = safe_str(row.get('VINNO', ''))
        aano1 = vinno.strip()[:13]
        comp  = aano1.replace('@', '').replace('*', '').replace('(', '').replace(
            '#', '').replace(')', '').replace('-', '').replace(' ', '')
        aano  = comp if len(comp) == 13 else ''
        branch = safe_int(row.get('NTBRCH', 0))
        costctr = safe_int(row.get('COSTCTR', 0))
        if costctr == 4043:
            branch = 906
        if costctr == 4048:
            branch = 903
        # FOLLOWING MAPPING MUST BE IN SYNC WITH EIIWCC5C
        lasttran = row.get('LASTTRAN')
        icdate   = None
        if lasttran is not None and safe_float(lasttran, 0) not in (0.0,):
            icdate = date_from_z11(lasttran)
            if icdate is not None:
                icdate_sas2 = (icdate - sas_epoch).days
                if icdate_sas2 < xdate_sas:
                    continue
        else:
            continue
        rvnote_records.append({
            'AANO': aano, 'ACCTNO': row.get('ACCTNO'),
            'NOTENO': row.get('NOTENO'), 'BRANCH': branch,
            'COSTCTR': costctr, 'ICDATE': icdate_sas2 if icdate else None,
            'REVERSED': row.get('REVERSED'),
        })

    rvnote_proc = pl.DataFrame(rvnote_records) if rvnote_records else pl.DataFrame()

    # Merge HPRJ with RVNOTE (IF (A AND B) OR B)
    if not hprj_df.is_empty() and not rvnote_proc.is_empty():
        hprj_merged = hprj_df.join(
            rvnote_proc, on='AANO', how='outer_coalesce'
        ).filter(
            (pl.col('CCRIS_STAT').is_not_null()) | (pl.col('ACCTNO').is_not_null())
        )
    elif not rvnote_proc.is_empty():
        hprj_merged = rvnote_proc
    else:
        hprj_merged = hprj_df

    # --- Load SUMM1 from BNMSUM ---
    summ1_path = BNMSUM_DIR / f'summ1{rdate2}.parquet'
    summ1_cols = [
        'AANO', 'DTECOMPLETE', 'SPECIALFUND', 'PURPOSE_LOAN', 'APPTYPE',
        'FIN_CONCEPT', 'PRIORITY_SECTOR', 'LN_UTILISE_LOCAT_CD',
        'STRUPCO_3YR', 'DSRISS3', 'EIR', 'STAGE', 'AMOUNT', 'FACICODE',
        'ASSET_PURCH_AMT', 'CIR', 'PRICING_TYPE', 'CCPT_TAG',
    ] + [c for c in ['LU_ADD1', 'LU_ADD2', 'LU_ADD3', 'LU_ADD4',
                     'LU_TOWN_CITY', 'LU_POSTCODE', 'LU_STATE_CD',
                     'LU_COUNTRY_CD', 'LU_SOURCE']]

    con = duckdb.connect()
    try:
        summ1_df = con.execute(f"""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY AANO ORDER BY STAGE) AS rn
                FROM read_parquet('{summ1_path}')
                WHERE DATE <= {rdate1}
            ) t
            WHERE rn = 1
        """).pl()
    except Exception as e:
        logger.warning(f"Could not load SUMM1: {e}")
        summ1_df = pl.DataFrame()
    con.close()

    # --- Load LNACC4 ---
    lnacc4_path = BNM_DIR / 'lnacc4.parquet'
    con = duckdb.connect()
    lnacc4_df = con.execute(f"""
        SELECT ACCTNO, AANO, PRODUCT, LNTERM, MNIAPDTE, REVIEWDT,
               FIRST_DISBDT, SETTLEMNTDT, TOTACBAL, MNIAPLMT, EXPRDATE
        FROM read_parquet('{lnacc4_path}')
    """).pl()
    con.close()

    # Build LNACC4 processed records
    def process_lnacc4_row(row: dict) -> dict:
        r = {
            'ACCTNO': row.get('ACCTNO'),
            'ACCT_AANO': safe_str(row.get('AANO')),
            'ACCT_LNTYPE': safe_int(row.get('PRODUCT')),
            'ACCT_LNTERM': safe_int(row.get('LNTERM')),
            'ACCT_OUTBAL': round(safe_float(row.get('TOTACBAL', 0)) * 100),
            'ACCT_APLMT':  round(safe_float(row.get('MNIAPLMT', 0)) * 100),
            'ACCT_REVDD': '', 'ACCT_REVMM': '', 'ACCT_REVYY': '',
            'ACCT_1DISDD': '', 'ACCT_1DISMM': '', 'ACCT_1DISYY': '',
            'ACCT_SETDD': '', 'ACCT_SETMM': '', 'ACCT_SETYY': '',
            'ACCT_EXPDD': '', 'ACCT_EXPMM': '', 'ACCT_EXPYY': '',
            'ACCT_ALDD': '', 'ACCT_ALMM': '', 'ACCT_ALYY': '',
        }

        def parse_dt_field(key):
            v = row.get(key)
            if v is not None and safe_float(v, 0) != 0:
                return date_from_z11(v)
            return None

        for key, dd_f, mm_f, yy_f in [
            ('REVIEWDT',    'ACCT_REVDD',   'ACCT_REVMM',   'ACCT_REVYY'),
            ('FIRST_DISBDT','ACCT_1DISDD',  'ACCT_1DISMM',  'ACCT_1DISYY'),
            ('SETTLEMNTDT', 'ACCT_SETDD',   'ACCT_SETMM',   'ACCT_SETYY'),
            ('EXPRDATE',    'ACCT_EXPDD',   'ACCT_EXPMM',   'ACCT_EXPYY'),
            ('MNIAPDTE',    'ACCT_ALDD',    'ACCT_ALMM',    'ACCT_ALYY'),
        ]:
            dt = parse_dt_field(key)
            if dt:
                r[dd_f] = f'{dt.day:02d}'
                r[mm_f] = f'{dt.month:02d}'
                r[yy_f] = f'{dt.year:04d}'

        return r

    lnacc4_processed = pl.DataFrame(
        [process_lnacc4_row(row) for row in lnacc4_df.to_dicts()]
    )

    # -----------------------------------------------------------------------
    # PROCESS ELDS -> LOAN / LNRJ
    # -----------------------------------------------------------------------
    loan_rows  = []
    lnrj_rows  = []

    for row in elds_df.to_dicts():
        row['ACTY'] = 'LN'
        branch   = safe_int(row.get('BRANCH', 0))
        tranbrno = safe_int(row.get('TRANBRNO', 0))
        smesize  = safe_int(row.get('SMESIZE', 0))
        custcode = safe_int(row.get('CUSTCODE', 0))

        if branch == 902:
            branch = 904
        row['BRANCH'] = branch
        if tranbrno not in (None, 0):
            row['BRANCH'] = tranbrno
        if smesize not in (None, 0):
            row['CUSTCODE'] = smesize

        aadate = safe_int(row.get('AADATE', 0))
        icdate = safe_int(row.get('ICDATE', 0))

        if idate_sas <= aadate <= sdate_sas:
            loan_rows.append(dict(row))
        if xdate_sas <= icdate <= sdate_sas:
            lnrj_rows.append(dict(row))

    # LNHPRJ = LNRJ + HPRJ
    lnhprj_rows = list(lnrj_rows)
    for row in hprj_merged.to_dicts():
        r = dict(row)
        if r.get('NOTENO') is None:
            r['NOTENO'] = 0
        lnhprj_rows.append(r)

    # -----------------------------------------------------------------------
    # Build ACCTCRED / SUBACRED / CREDITPO / PROVISIO / LEGALACT
    # -----------------------------------------------------------------------
    acctcred_records = []

    for row in loan_rows:
        acty = safe_str(row.get('ACTY', 'LN'))
        loantype = safe_int(row.get('LOANTYPE', 0))
        product  = safe_int(row.get('PRODUCT', loantype))
        acctno   = safe_int(row.get('ACCTNO', 0))
        noteno   = safe_int(row.get('NOTENO', 0))
        branch   = safe_int(row.get('BRANCH', 0))
        ntbrch   = safe_int(row.get('NTBRCH', 0))
        accbrch  = safe_int(row.get('ACCBRCH', 0))
        costctr  = safe_int(row.get('COSTCTR', 0))
        sector   = safe_str(row.get('SECTOR', ''))
        sector1  = safe_str(row.get('SECTOR1', ''))
        sectcd   = safe_str(row.get('SECTCD', ''))
        curcode  = safe_str(row.get('CURCODE', 'MYR')) or 'MYR'
        aano     = safe_str(row.get('AANO', ''))
        commno   = safe_int(row.get('COMMNO', 0))
        ind      = safe_str(row.get('IND', ''))
        paidind  = safe_str(row.get('PAIDIND', ''))
        borstat  = safe_str(row.get('BORSTAT', ''))
        accstat  = safe_str(row.get('ACCSTAT', ''))
        orgtype  = safe_str(row.get('ORGTYPE', ''))
        facgptype = safe_str(row.get('FACGPTYPE', ''))
        restruct = safe_str(row.get('RESTRUCT', ''))
        refinanc = safe_str(row.get('REFINANC', ''))
        recourse = safe_str(row.get('RECOURSE', ''))
        riskcode = safe_str(row.get('RISKCODE', ''))
        riskrate = safe_str(row.get('RISKRATE', ''))
        nplind   = safe_str(row.get('NPLIND', ''))
        odstatus = safe_str(row.get('ODSTATUS', ''))
        classifi = safe_str(row.get('CLASSIFI', ''))
        gp3ind   = safe_str(row.get('GP3IND', ''))
        leg      = safe_str(row.get('LEG', ''))
        cproduct = safe_str(row.get('CPRODUCT', ''))
        pritype  = safe_str(row.get('PRITYPE', ''))
        ntint    = safe_str(row.get('NTINT', ''))
        score    = safe_str(row.get('SCORE', ''))
        modeldes = safe_str(row.get('MODELDES', ''))
        prodesc  = safe_str(row.get('PRODESC', ''))
        aprvdt   = safe_str(row.get('APRVDT', ''))
        facicode = safe_str(row.get('FACICODE', ''))
        reconame = safe_str(row.get('RECONAME', ''))
        reftype  = safe_str(row.get('REFTYPE', ''))
        nmref1   = safe_str(row.get('NMREF1', ''))
        nmref2   = safe_str(row.get('NMREF2', ''))
        apvnme1  = safe_str(row.get('APVNME1', ''))
        apvnme2  = safe_str(row.get('APVNME2', ''))
        newic    = safe_str(row.get('NEWIC', ''))
        dnbfisme = safe_str(row.get('DNBFISME', ''))
        reason   = safe_str(row.get('REASON', ''))
        refin_flg  = safe_str(row.get('REFIN_FLG', ''))
        old_fi_cd  = safe_str(row.get('OLD_FI_CD', ''))
        old_mastacc = safe_str(row.get('OLD_MASTACC', ''))
        old_subacc  = safe_str(row.get('OLD_SUBACC', ''))
        refin_amt   = safe_float(row.get('REFIN_AMT', 0))
        strupco_3yr = safe_str(row.get('STRUPCO_3YR', ''))
        dsr         = safe_float(row.get('DSR', 0))
        lu_loc      = safe_str(row.get('LN_UTILISE_LOCAT_CD', ''))
        priority_sector = safe_str(row.get('PRIORITY_SECTOR', ''))
        ccpt_tag    = safe_str(row.get('CCPT_TAG', ''))
        lu_add1     = safe_str(row.get('LU_ADD1', ''))
        lu_add2     = safe_str(row.get('LU_ADD2', ''))
        lu_add3     = safe_str(row.get('LU_ADD3', ''))
        lu_add4     = safe_str(row.get('LU_ADD4', ''))
        lu_town     = safe_str(row.get('LU_TOWN_CITY', ''))
        lu_postcode = safe_str(row.get('LU_POSTCODE', ''))
        lu_state    = safe_str(row.get('LU_STATE_CD', ''))
        lu_country  = safe_str(row.get('LU_COUNTRY_CD', ''))
        lu_source   = safe_str(row.get('LU_SOURCE', ''))
        collmake    = safe_str(row.get('COLLMAKE', ''))
        payfreq     = safe_str(row.get('PAYFREQ', ''))
        liabcode    = ''
        custcode    = safe_int(row.get('CUSTCODE', 0))

        amount      = safe_float(row.get('AMOUNT', 0))
        balance     = safe_float(row.get('BALANCE', 0))
        curbal      = safe_float(row.get('CURBAL', 0))
        appvalue    = safe_float(row.get('APPVALUE', 0))
        realisab    = safe_float(row.get('REALISAB', 0))
        cavaiamt    = safe_float(row.get('CAVAIAMT', 0))
        limtcurr    = safe_float(row.get('LIMTCURR', 0))
        apprlim2    = safe_float(row.get('APPRLIM2', 0))
        netproc     = safe_float(row.get('NETPROC', 0))
        apprlmaa    = safe_float(row.get('APPRLMAA', 0))
        lmtamt      = safe_float(row.get('LMTAMT', 0))
        odxsamt     = safe_float(row.get('ODXSAMT', 0))
        biltot      = safe_float(row.get('BILTOT', 0))
        outstand    = 0.0
        collval     = 0.0
        undrawn     = amount
        census      = safe_float(row.get('CENSUS', 0))
        ccriscode   = safe_str(row.get('CCRICODE', ''))
        purposes    = safe_str(row.get('PURPOSES', ''))
        spaamt      = safe_float(row.get('SPAAMT', 0))
        intrate     = safe_float(row.get('INTRATE', 0))
        prirate     = safe_float(row.get('PRIRATE', 0))
        pricing     = safe_float(row.get('PRICING', 0))
        rate        = safe_float(row.get('RATE', 0))
        mthinstlmt  = safe_float(row.get('MTHINSTLMT', 0))
        feeamt      = safe_float(row.get('FEEAMT', 0))
        interdue    = 0.0
        rebate      = safe_float(row.get('REBATE', 0))
        intearn     = safe_float(row.get('INTEARN', 0))
        intearn4    = safe_float(row.get('INTEARN4', 0))
        intamt      = safe_float(row.get('INTAMT', 0))
        iisopbal    = safe_float(row.get('IISOPBAL', 0))
        iissuamt    = safe_float(row.get('IISSUAMT', 0))
        iisbwamt    = safe_float(row.get('IISBWAMT', 0))
        totiis      = safe_float(row.get('TOTIIS', 0))
        totiisr     = safe_float(row.get('TOTIISR', 0))
        totwof      = safe_float(row.get('TOTWOF', 0))
        spopbal     = safe_float(row.get('SPOPBAL', 0))
        spwbamt     = safe_float(row.get('SPWBAMT', 0))
        spwof       = safe_float(row.get('SPWOF', 0))
        spdanah     = safe_float(row.get('SPDANAH', 0))
        sptrans     = safe_float(row.get('SPTRANS', 0))
        billcnt     = safe_int(row.get('BILLCNT', 0))
        bldate      = safe_int(row.get('BLDATE', 0))
        dlvdate     = safe_int(row.get('DLVDATE', 0))
        exprdate    = safe_int(row.get('EXPRDATE', 0))
        maturedt    = row.get('MATUREDT')
        dlivrydt    = row.get('DLIVRYDT')
        nxbildt     = row.get('NXBILDT')
        collyear    = row.get('COLLYEAR')
        issuedt     = row.get('ISSUEDT')
        cappdate    = row.get('CAPPDATE')
        complidt    = row.get('COMPLIDT')
        rjdate      = row.get('RJDATE')
        aadate_v    = safe_int(row.get('AADATE', 0))
        icdate_v    = safe_int(row.get('ICDATE', 0))
        earnterm    = safe_int(row.get('EARNTERM', 0))
        noteterm    = safe_int(row.get('NOTETERM', 0))
        term        = safe_int(row.get('TERM', 0))
        pzipcode    = safe_int(row.get('PZIPCODE', 0))
        indexcd     = safe_int(row.get('INDEXCD', 0))
        corgamt     = safe_float(row.get('CORGAMT', 0))
        payfreqc    = safe_str(row.get('PAYFREQC', ''))
        cagamas     = 0
        syndicat    = 'N'
        specialf    = '00'
        fconcept    = 0
        fcencus     = 0.0
        facility    = safe_int(row.get('FACILITY', 0))
        faccode     = safe_int(row.get('FACCODE', 0))
        apcode      = 0
        lnty        = 0
        acctstat    = 'O'
        arrears     = 0
        nodays      = 0
        instalm     = 0
        oldbrh      = 0
        ficode      = branch

        # Date output fields
        paydd='00'; paymm='00'; payyr='0000'
        grantdd='00'; grantmm='00'; grantyr='0000'
        lnmatdd='00'; lnmatmm='00'; lnmatyr='0000'
        ormatdd='00'; ormatmm='00'; ormatyr='0000'
        dlvrdd='00'; dlvrmm='00'; dlvryr='0000'
        bilduedd='00'; bilduemm='00'; bildueyr='0000'
        collmm='00'; collyr='00'; fconcept_v = 0
        issuedd=''; issuemm=''; issueyy=''

        # Exclude specific accounts
        if ((acctno == 8027370307 and noteno == 10) or
                (acctno == 8124498008 and noteno == 10010) or
                (acctno == 8124948801 and noteno == 10)):
            continue

        # ---- OD PROCESSING ----
        if acty == 'OD':
            syndicat = ' '
            fcencus  = census
            collval  = safe_float(row.get('REALISAB', 0))
            purposes = ccriscode[:4] if ccriscode else ''
            if collval in (0, 0.0):
                collval = 0
            odxsamt = odxsamt * 100
            lmtamt  = lmtamt  * 100
            biltot  = 0.0
            limtcurr = apprlim2 * 100
            loantype = product
            apcode   = product
            noteno   = 0
            sector   = sector1
            if product in (124, 165, 191):
                facility = 34111
            else:
                facility = 34110

            if product == 114:
                specialf = '12'
                fconcept_v = 60
                noteterm = 12
                payfreqc = '20'
            elif product in (160, 161, 162, 163, 164, 165, 166, 167):
                specialf = '00'
                fconcept_v = 10
                noteterm = 12
                payfreqc = '20'
            else:
                specialf = '00'
                fconcept_v = 51
                noteterm = 12
                payfreqc = '20'

            feeamt   = 0.0
            interdue = 0.0

            exoddate = safe_int(row.get('EXODDATE', 0))
            tempoddt = safe_int(row.get('TEMPODDT', 0))
            if (exoddate != 0 or tempoddt != 0) and curbal < 0:
                if exoddate == 0:
                    exoddate = tempoddt
                oddate = date_from_z11(exoddate)
                if oddate:
                    oddays = (oddate - sas_epoch).days
                    paydd = f'{oddate.day:02d}'
                    paymm = f'{oddate.month:02d}'
                    payyr = f'{oddate.year:04d}'
                    nodays = sdate_sas - oddays + 1
                    if nodays > 0:
                        arrears = ndays_format(nodays)
                        if arrears == 24:
                            arrears = round(nodays / 30)
                        instalm = arrears

            if riskcode == '1':
                classifi = 'C'
            elif riskcode == '2':
                classifi = 'S'
            elif riskcode == '3':
                classifi = 'D'
            elif riskcode == '4':
                classifi = 'B'
            elif odstatus == 'AC' or (exoddate == 0 and tempoddt == 0):
                classifi = 'P'

            # Liability codes from COL1-COL5
            for col_key in ['COL1', 'COL2', 'COL3', 'COL4', 'COL5']:
                col_val = safe_str(row.get(col_key, ''))
                lb1 = col_val[0:2].strip()
                lb2 = col_val[3:5].strip() if len(col_val) >= 5 else ''
                for lb in (lb1, lb2):
                    if lb:
                        liabcode = lb
                        break
                if liabcode:
                    break

            issuedd = f'{safe_int(row.get("ODLMTDD", 0)):02d}'
            issuemm = f'{safe_int(row.get("ODLMTMM", 0)):02d}'
            issueyy = f'{safe_int(row.get("ODLMTYY", 0)):04d}'

            openind = safe_str(row.get('OPENIND', ''))
            if openind in ('P', 'C', 'B'):
                acctstat = 'S'
            else:
                acctstat = 'O'

            curbal = curbal * -1
            outstand = curbal * 100

            # BNM TAXANOMY & CCRIS ENHANCEMENT (2012-0752)
            syndicat = 'N'
            specialf = '00'
            fconcept_v = format_odfconcept(product)

        else:
            # ---- LOAN PROCESSING ----
            collval  = appvalue * 100
            outstand = balance  * 100
            cavaiamt = cavaiamt * 100
            lmtamt   = 0.0
            odxsamt  = 0.0
            biltot   = biltot   * 100
            census   = census   * 100
            limtcurr = apprlim2
            if limtcurr in (None, 0.0):
                limtcurr = netproc

            hp_loan_types = {122, 106, 128, 130, 120, 121, 700, 705, 709, 710}
            hp_acctno_lt  = {100,101,102,103,110,111,112,113,114,115,116,117,
                             118,120,121,122,123,124,125,127,135,136,170,180,
                             106,128,130,700,705,709,710,194,195,380,381}

            if ((acctno > 8000000000 and loantype in hp_loan_types) or
                    (acctno < 2999999999 and loantype in hp_acctno_lt)):
                if issuedt is not None and safe_float(issuedt, 0) != 0:
                    s = f'{int(issuedt):011d}'
                    issuedd = s[2:4]; issuemm = s[0:2]; issueyy = s[4:8]
                limtcurr = netproc
                if (10010 <= noteno <= 10099) or (30010 <= noteno <= 30099):
                    if ind == 'B':
                        limtcurr = corgamt + (-1 * intamt)
            else:
                use_dt = cappdate if (cappdate is not None and
                                     safe_float(cappdate, 0) != 0) else issuedt
                if use_dt is not None and safe_float(use_dt, 0) != 0:
                    s = f'{int(use_dt):011d}'
                    issuedd = s[2:4]; issuemm = s[0:2]; issueyy = s[4:8]

            cagamas  = 0
            syndicat = 'N'
            specialf = '00'
            fconcept_v = 99
            # RESTRUCT=' '
            # ESMR 2013-2133: RESTRUCT MAINTAINED AS 'C' 'T' IN HOST
            #                 SAS DWH MAINTAIN AS 'R' 'S'

            # SPECIALF mapping
            if loantype in (124, 145):
                specialf = '00'
            if loantype == 566:
                specialf = '10'
            if loantype in (991, 994) and pzipcode == 566:
                specialf = '10'
            if loantype in (170, 564, 565):
                specialf = '11'
            if loantype in (991, 994) and pzipcode in (170, 564, 565):
                specialf = '11'
            if loantype in (559, 560, 567):
                specialf = '12'
            if loantype in (991, 994) and pzipcode in (559, 560, 567):
                specialf = '12'
            if loantype in (568, 570, 532, 533, 573, 574):
                specialf = '14'
            if loantype in (991, 994) and pzipcode in (568, 570, 573):
                specialf = '14'
            if loantype == 561:
                specialf = '16'
            if loantype in (991, 994) and pzipcode == 561:
                specialf = '16'
            if loantype in (555, 556):
                specialf = '18'
            if loantype in (991, 994) and pzipcode in (555, 556):
                specialf = '18'
            if loantype in (521, 522, 523, 528):
                specialf = '99'
            if loantype in (991, 994) and pzipcode in (521, 522, 523, 528):
                specialf = '99'

            # FCONCEPT mapping
            if loantype in (124, 145):
                fconcept_v = 10
            if loantype in PRDF:
                fconcept_v = 10
            if loantype in (981, 982, 984, 985) and pzipcode in PRDF:
                fconcept_v = 10
            if loantype == 180:
                fconcept_v = 13
            if loantype in (981, 982, 984, 985) and pzipcode == 180:
                fconcept_v = 13
            if loantype in (128, 130, 131, 132, 983):
                fconcept_v = 16
            if loantype == 193:
                fconcept_v = 18
            if loantype in (135, 136, 138, 182):
                fconcept_v = 26
            if loantype in (982, 985) and pzipcode in (135, 136):
                fconcept_v = 26
            if loantype in PRDG:
                fconcept_v = 51
            if loantype in (981, 982, 994, 995) and pzipcode in PRDG:
                fconcept_v = 51
            if loantype in (910, 925):
                fconcept_v = 52
            if loantype in (992, 995) and pzipcode in (910, 925):
                fconcept_v = 52
            if loantype in {4,5,15,20,25,26,27,28,29,30,31,32,33,34,60,62,
                            63,70,71,72,73,74,75,76,77,78,360,380,381,
                            390,700,705,993,996}:
                fconcept_v = 59
            if loantype in {227,228,230,231,232,233,234,235,236,237,
                            532,533,573,574,248,
                            238,239,240,241,242,243,359,361,531,906,907}:
                fconcept_v = 60
            if loantype in PRDH:
                fconcept_v = 99
            if loantype in (991, 992, 994, 995) and pzipcode in PRDH:
                fconcept_v = 99
            if loantype in (180, 914, 915, 919, 920, 925, 950, 951):
                syndicat = 'Y'

            # INTERDUE
            if ntint != 'A':
                interdue = balance + (-1 * curbal) + (-1 * feeamt)
            else:
                curbal = curbal + intearn + (-1 * intamt)
                interdue = 0.0
                if curbal < 0:
                    curbal = 0.0

            if loantype in (700, 705, 709, 710, 752, 760):
                curbal   = curbal + (-1 * rebate) + (-1 * intearn4)
                interdue = 0.0

            instalm  = billcnt
            lnty     = 0
            acctstat = 'O'

            if paidind == 'P':
                acctstat = 'S'; arrears = 0; instalm = 0

            if loantype in (128, 130, 700, 705, 380, 381, 709, 710):
                lnty = 1
            if lnty == 1 and (noteno >= 98000 or borstat == 'S'):
                acctstat = 'T'
            if lnty == 0 and borstat == 'S':
                acctstat = 'T'

            if loantype in (950, 951, 915, 953):
                syndicat = 'Y'
            if loantype in (248, 532, 533, 573, 574):
                noteterm = earnterm
            if loantype in (330, 302, 506, 902, 903, 951, 953) and cproduct == '170':
                noteterm = 12
            if loantype == 521:
                payfreqc = '13'
            if loantype in (124, 145, 532, 533, 573, 574, 248):
                payfreqc = '14'

            _payfreq14_types = {
                5,6,25,15,20,111,139,140,122,106,128,130,
                120,121,201,204,205,225,300,301,307,320,330,
                504,505,506,511,568,555,556,559,567,564,565,
                570,566,700,705,709,710,900,901,910,912,
                930,126,127,359,981,982,983,991,993,
                117,113,118,115,119,116,227,228,230,231,234,
                235,236,237,238,239,240,241,242,
            }
            _payfreq14_pzipcode = {
                300,304,305,307,310,330,504,505,506,511,515,
                359,555,556,559,560,564,565,570,571,574,575,900,
                901,902,910,912,930,
            }
            if loantype in _payfreq14_types:
                payfreqc = '14'
            if loantype == 992 and pzipcode in _payfreq14_pzipcode:
                payfreqc = '14'

            if loantype == 330 and safe_float(cproduct, 0) == 0:
                payfreqc = '16'
            if loantype in (302, 506, 902, 903, 951, 953, 330) and cproduct == '170':
                payfreqc = '19'
            if loantype in (569, 904, 932, 933, 950, 915):
                payfreqc = '21'

            # RESTRUCT
            if loantype in (128,130,131,132,720,725,380,381,700,705,709,710):
                if noteno >= 98000:
                    restruct = 'C'
                if borstat == 'S':
                    restruct = 'T'
            else:
                if borstat == 'S':
                    restruct = 'T'
                if borstat == 'Q':
                    restruct = 'C'

            if noteterm in (None, 0):
                noteterm = term
            if borstat == 'W':
                acctstat = 'P'
            elif borstat == 'X':
                acctstat = 'W'

            if loantype in (135, 136, 166, 168):
                intrate = pricing
            elif facgptype == '6':
                intrate = rate
            else:
                intrate = prirate

            # CAGAMAS
            cagamas_map = {
                'B': 20041998, 'C': 20041998, 'F': 25081997, 'G': 30051997,
                'H': 12031999, 'I': 25081999, 'K':  3082000, 'L': 18082000,
                'M': 18082000, 'J':  8062000, 'P': 25082000, 'Q': 25082000,
            }
            cagamas = cagamas_map.get(refinanc, 0)
            if cagamas > 0:
                recourse = 'Y'

            # NODAYS / PAYDD
            if bldate > 0:
                nodays = sdate_sas - bldate
            if nodays < 0:
                nodays = 0
            if bldate > 0:
                bd = sas_to_date(bldate)
                if bd:
                    paydd = f'{bd.day:02d}'
                    paymm = f'{bd.month:02d}'
                    payyr = f'{bd.year:04d}'

            if dlvdate > 0:
                dd = sas_to_date(dlvdate)
                if dd:
                    grantdd = f'{dd.day:02d}'
                    grantmm = f'{dd.month:02d}'
                    grantyr = f'{dd.year:04d}'

            if exprdate > 0:
                ed = sas_to_date(exprdate)
                if ed:
                    lnmatdd = f'{ed.day:02d}'
                    lnmatmm = f'{ed.month:02d}'
                    lnmatyr = f'{ed.year:04d}'

            def parse_z11_dd_mm_yy(val):
                if val is None or safe_float(val, 0) == 0:
                    return '', '', ''
                s = f'{int(val):011d}'
                return s[2:4], s[0:2], s[4:8]

            ormatdd, ormatmm, ormatyr = parse_z11_dd_mm_yy(maturedt)
            dlvrdd_v, dlvrmm_v, dlvryr_v = parse_z11_dd_mm_yy(dlivrydt)
            if dlvrdd_v:
                dlvrdd = dlvrdd_v; dlvrmm = dlvrmm_v; dlvryr = dlvryr_v
            bilduedd_v, bilduemm_v, bildueyr_v = parse_z11_dd_mm_yy(nxbildt)
            if bilduedd_v:
                bilduedd = bilduedd_v; bilduemm = bilduemm_v; bildueyr = bildueyr_v

            # COLLMM / COLLYR
            _staffloan_types = {
                128,130,131,132,380,381,700,705,750,720,725,752,760,
                4,5,6,7,15,20,25,26,27,28,29,30,31,32,33,34,60,61,
                62,63,70,71,72,73,74,75,76,77,78,100,101,108,
                199,500,520,983,993,996,
            }
            if collyear is not None and safe_float(collyear, 0) != 0:
                cy_s = f'{int(collyear):05d}'
                if loantype in _staffloan_types or orgtype == 'S':
                    collyr = cy_s[3:5]
                else:
                    collmm = cy_s[1:3]
                    collyr = cy_s[3:5]

            arrears = ndays_format(nodays)
            if arrears == 24:
                arrears = round((nodays / 365) * 12)
            instalm = billcnt

            # CLASSIFI
            if riskrate == ' ' or riskrate == '':
                classifi = 'P'
            elif riskrate == '1':
                classifi = 'C'
            elif riskrate == '2':
                classifi = 'S'
            elif riskrate == '3':
                classifi = 'D'
            elif riskrate == '4':
                classifi = 'B'
            if nplind == 'P':
                classifi = 'P'

            sector = sector.replace(' ', '')
            if not sector:
                sector = sectcd.strip()

            limtcurr = limtcurr * 100
            if limtcurr in (None, 0.0):
                limtcurr = amount

            collref = acctno
            if branch == 0:
                branch = ntbrch
            if branch == 0:
                branch = accbrch
            apcode  = loantype
            if facgptype == '5':
                apcode = 0
            ficode  = branch
            interdue = interdue * 100
            realisab = realisab * 100

            # PAYFREQC from PAYFREQ
            payfreq_map = {'1':'14','2':'15','3':'16','4':'17','5':'18','6':'13','9':'21'}
            if payfreq in payfreq_map:
                payfreqc = payfreq_map[payfreq]

            # Restructured accounts (984, 985, 994, 995)
            if loantype in (984, 985, 994, 995):
                if noteno == 10010:
                    outstand = 0; acctstat = 'S'
                    arrears = 999; instalm = 999; undrawn = 0
            if loantype in (981, 982, 983, 991, 992, 993):
                outstand = 0; arrears = 999; balance = 0
                instalm  = 999; undrawn = 0
                acctstat = 'W'

            # BNM TAXANOMY & CCRIS ENHANCEMENT (2011-4096)
            syndicat = format_synd(loantype)
            specialf = format_fundsch(loantype)
            fconcept_v = format_fconcept(loantype)

            if loantype in (412, 413, 414, 415, 466):
                facility = 34322
                faccode  = 34320
            if loantype in (575, 416, 467, 461):
                facility = 34391
                faccode  = 34391
            if loantype in (307, 464, 465):
                facility = 34391
                faccode  = 34392
            if loantype in (103, 104):
                facility = 34371
                faccode  = 34371
            if loantype in (191, 417):
                facility = 34351
                faccode  = 34364

            if 600 <= loantype <= 699:
                if loantype in (698, 699):
                    syndicat = 'N'
                    specialf = '00'
                    fconcept_v = 16
                else:
                    # SYNDICAT = PUT(FLOOR(CENSUS),SYND.);  -- commented out
                    # SPECIALF = PUT(FLOOR(CENSUS),FUNDSCH.); -- commented out
                    fconcept_v = format_fconcept(math.floor(safe_float(census, 0)))
                if loantype in (610, 611, 668, 669):
                    facility = 34391
                    faccode  = 34392
                if loantype in (670, 690):
                    facility = 34351
                    faccode  = 34364

            if accstat == 'W' and balance > 10:
                acctstat = 'P'
            elif accstat == 'P' and balance <= 2:
                acctstat = 'W'

            # MODELR block (commented out in original):
            # MODELR=SUBSTR(MODELDES,1,1);
            # IF MODELR IN ('S','R') THEN DO;
            #    ACCTSTAT=MODELR;
            #    RESTRUCT=MODELR;
            # END;

        # ---- Common post-processing (both OD and LOAN) ----
        if not curcode:
            curcode = 'MYR'
        if curcode != 'MYR':
            fo = get_forate(curcode)
            mthinstlmt = mthinstlmt * fo
            if 2850000000 <= acctno <= 2859999999:
                undrawn = undrawn * fo

        if undrawn < 0:
            undrawn = 0.0

        # Null-coalesce financial fields
        for var_name in ['totiis','totiisr','totwof','iisopbal','iissuamt',
                         'iisbwamt','spopbal','spwbamt','spwof','spdanah',
                         'sptrans','spaamt','intrate','mthinstlmt']:
            locals()[var_name] = safe_float(locals().get(var_name), 0.0)

        iisdanah = 0; iistrans = 0; spcharge = 0

        if score and score[0].upper() not in ALP:
            score = '   '

        # Issued date
        try:
            issued = mdy(int(issuemm or 0), int(issuedd or 0), int(issueyy or 0))
        except Exception:
            issued = None

        # RESTRUCT / ACCTSTAT overrides for HP restructured
        if loantype in (128,130,380,381,700,705,720,725) and noteno >= 98000:
            if issued:
                d_30sep05  = date(2005, 9, 30)
                d_19dec07  = date(2007, 12, 19)
                d_31aug08  = date(2008, 8, 31)
                d_01sep08  = date(2008, 9, 1)
                d_01oct05  = date(2005, 10, 1)
                d_18dec07  = date(2007, 12, 18)
                if issued <= d_30sep05 or d_19dec07 <= issued <= d_31aug08:
                    restruct = 'T'
                elif issued >= d_01sep08 or d_01oct05 <= issued <= d_18dec07:
                    restruct = 'C'
                if restruct == 'C':
                    acctstat = 'C'
                elif restruct == 'T':
                    acctstat = 'T'

        if loantype in (128,130,380,381,700,705,720,725):
            if noteno < 98000 and modeldes[:4] in (
                    'CAM4','CAM5','CAM6','CAM7','CAM8','CAM9'):
                restruct = 'C'; acctstat = 'C'
            if noteno < 98000 and modeldes[:5] == 'CAM10':
                restruct = 'C'; acctstat = 'C'

        # IF AANO NE ' ' THEN NOTENO = FACILITY; -- commented out
        if 800 <= apcode <= 899 and 2000000000 <= acctno <= 2999999999:
            ficode = 904
        if 800 <= apcode <= 899 and 8000000000 <= acctno <= 8999999999:
            ficode = 904
        if 800 <= loantype <= 899:
            costctr = 904

        # Scale financial fields * 100
        undrawn    = undrawn * 100
        spaamt     = spaamt * 100
        intrate    = intrate * 100
        curbal_out = curbal * 100
        feeamt     = feeamt * 100
        totiis     = totiis * 100
        totiisr    = totiisr * 100
        totwof     = totwof * 100
        iisopbal   = iisopbal * 100
        iissuamt   = iissuamt * 100
        iisbwamt   = iisbwamt * 100
        spwbamt    = spwbamt * 100
        spwof      = spwof * 100
        spopbal    = spopbal * 100
        spdanah    = spdanah * 100
        sptrans    = sptrans * 100
        mthinstlmt = mthinstlmt * 100

        # AADATE formatted
        aadate_dt = sas_to_date(aadate_v)
        aadatedd = f'{aadate_dt.day:02d}'   if aadate_dt else '00'
        aadatemm = f'{aadate_dt.month:02d}' if aadate_dt else '00'
        aadateyy = f'{aadate_dt.year:04d}'  if aadate_dt else '0000'

        # Store record for later output and merges
        rec = {
            # keys
            'ACCTNO': acctno, 'NOTENO': noteno, 'AANO': aano,
            'BRANCH': branch, 'FICODE': ficode, 'APCODE': apcode,
            'COMMNO': commno, 'IND': ind, 'PAIDIND': paidind,
            # account
            'ACCTSTAT': acctstat, 'CLASSIFI': classifi, 'GP3IND': gp3ind,
            'LEG': leg, 'LOANTYPE': loantype, 'PRODUCT': product,
            'ACTY': acty, 'FACILITY': facility, 'FACCODE': faccode,
            # financial
            'OUTSTAND': outstand, 'ARREARS': arrears, 'INSTALM': instalm,
            'UNDRAWN': undrawn, 'NODAYS': nodays, 'BILTOT': biltot,
            'ODXSAMT': odxsamt, 'LIMTCURR': limtcurr, 'LMTAMT': lmtamt,
            'CURBAL': curbal_out, 'INTERDUE': interdue, 'FEEAMT': feeamt,
            'REALISAB': realisab, 'IISOPBAL': iisopbal, 'TOTIIS': totiis,
            'TOTIISR': totiisr, 'TOTWOF': totwof, 'IISDANAH': iisdanah,
            'IISTRANS': iistrans, 'SPOPBAL': spopbal, 'SPCHARGE': spcharge,
            'SPWBAMT': spwbamt, 'SPWOF': spwof, 'SPDANAH': spdanah,
            'SPTRANS': sptrans, 'SPAAMT': spaamt, 'INTRATE': intrate,
            'MTHINSTLMT': mthinstlmt, 'COLLVAL': collval, 'CAGAMAS': cagamas,
            'APPRLMAA': apprlmaa, 'CAVAIAMT': cavaiamt,
            # loan attributes
            'SYNDICAT': syndicat, 'SPECIALF': specialf, 'FCONCEPT': fconcept_v,
            'NOTETERM': noteterm, 'PAYFREQC': payfreqc, 'PURPOSES': purposes,
            'RESTRUCT': restruct, 'RECOURSE': recourse, 'SECTOR': sector,
            'OLDBRH': oldbrh, 'SCORE': score, 'COSTCTR': costctr,
            'LIABCODE': liabcode, 'CUSTCODE': custcode, 'CENSUS': census,
            'COLLMAKE': collmake, 'MODELDES': modeldes, 'ORGTYPE': orgtype,
            'PAYFREQ': payfreq, 'CURCODE': curcode,
            # dates
            'PAYDD': paydd, 'PAYMM': paymm, 'PAYYR': payyr,
            'GRANTDD': grantdd, 'GRANTMM': grantmm, 'GRANTYR': grantyr,
            'LNMATDD': lnmatdd, 'LNMATMM': lnmatmm, 'LNMATYR': lnmatyr,
            'ORMATDD': ormatdd, 'ORMATMM': ormatmm, 'ORMATYR': ormatyr,
            'DLVRDD': dlvrdd, 'DLVRMM': dlvrmm, 'DLVRYR': dlvryr,
            'BILDUEDD': bilduedd, 'BILDUEMM': bilduemm, 'BILDUEYR': bildueyr,
            'COLLMM': collmm, 'COLLYR': collyr,
            'ISSUEDD': issuedd, 'ISSUEMM': issuemm, 'ISSUEYY': issueyy,
            'AADATE': aadate_v, 'AADATEDD': aadatedd,
            'AADATEMM': aadatemm, 'AADATEYY': aadateyy,
            'ISSUED': issued,
            'COMPLIDT': complidt, 'RJDATE': rjdate,
            # extra
            'FACGPTYPE': facgptype, 'RECONAME': reconame, 'REFTYPE': reftype,
            'NMREF1': nmref1, 'NMREF2': nmref2, 'APVNME1': apvnme1,
            'APVNME2': apvnme2, 'NEWIC': newic, 'DNBFISME': dnbfisme,
            'REASON': reason,
            'STRUPCO_3YR': strupco_3yr, 'REFIN_FLG': refin_flg,
            'OLD_FI_CD': old_fi_cd, 'OLD_MASTACC': old_mastacc,
            'OLD_SUBACC': old_subacc, 'REFIN_AMT': refin_amt,
            'DSR': dsr, 'LN_UTILISE_LOCAT_CD': lu_loc,
            'PRIORITY_SECTOR': priority_sector, 'EIR': 0.0,
            'CCPT_TAG': ccpt_tag,
            'LU_ADD1': lu_add1, 'LU_ADD2': lu_add2,
            'LU_ADD3': lu_add3, 'LU_ADD4': lu_add4,
            'LU_TOWN_CITY': lu_town, 'LU_POSTCODE': lu_postcode,
            'LU_STATE_CD': lu_state, 'LU_COUNTRY_CD': lu_country,
            'LU_SOURCE': lu_source,
            'PRODESC': prodesc, 'FACICODE': facicode, 'APRVDT': aprvdt,
            'PRITYPE': pritype, 'INDEXCD': indexcd,
            'PRIRATE': prirate, 'PRICING': pricing, 'RATE': rate,
            'BLDATE': bldate,
        }
        acctcred_records.append(rec)

    # -----------------------------------------------------------------------
    # Merge SUMM1 into ACCTCRED
    # -----------------------------------------------------------------------
    summ1_map = {}
    if not summ1_df.is_empty():
        for s in summ1_df.to_dicts():
            summ1_map[safe_str(s.get('AANO'))] = s

    for rec in acctcred_records:
        aano = rec['AANO']
        s = summ1_map.get(safe_str(aano), {})
        aano_x = safe_str(aano)[11:13] + safe_str(aano)[4:10] if len(safe_str(aano)) >= 13 else ''
        rec['AANO_X'] = aano_x
        rec['CIR'] = safe_float(s.get('CIR', 0)) * 100
        rec['EIR'] = safe_float(s.get('EIR', 0)) * 100

        prodesc = rec.get('PRODESC', '')
        if (prodesc[:5] == 'FEFCL' or prodesc[5:10] == 'FEFCL'):
            facicode = safe_str(s.get('FACICODE', ''))
            fmap = {
                '0091000320': 207, '0091000321': 208, '0091000322': 210,
                '0091000323': 209, '0091000324': 211, '0091000325': 212,
            }
            if facicode in fmap:
                rec['LOANTYPE'] = fmap[facicode]
            rec['LN_UTILISE_LOCAT_CD'] = safe_str(s.get('LN_UTILISE_LOCAT_CD', ''))
            rec['STRUPCO_3YR']  = safe_str(s.get('STRUPCO_3YR', ''))
            rec['APRVDT']       = safe_str(s.get('DTECOMPLETE', ''))
            rec['UNDRAWN']      = safe_float(s.get('AMOUNT', 0)) * 100
            rec['FACILITY']     = safe_int(s.get('FACICODE', 0))
            rec['APCODE']       = rec['LOANTYPE']
            rec['FACCODE']      = rec['FACILITY']
            rec['SYNDICAT']     = 'N'
            rec['PURPOSES']     = safe_str(s.get('PURPOSE_LOAN', ''))
            rec['FCONCEPT']     = safe_int(s.get('FIN_CONCEPT', 0))
            rec['NOTETERM']     = 12
            rec['PAYFREQC']     = '19'  # REVOLVING CREDIT
            sf = safe_str(s.get('SPECIALFUND', ''))
            rec['SPECIALF']     = sf if sf not in ('', '0', None) else '00'

        elif 2850000000 <= rec['ACCTNO'] <= 2859999999:
            dtec = safe_str(s.get('DTECOMPLETE', ''))
            if len(dtec) >= 10:
                try:
                    aadate_new = mdy(int(dtec[3:5]), int(dtec[0:2]), int(dtec[6:10]))
                    if aadate_new:
                        rec['AADATE'] = (aadate_new - sas_epoch).days
                except Exception:
                    pass
            rec['FACILITY']  = 34480
            rec['FACCODE']   = 34480
            rec['APCODE']    = 888
            sf = safe_str(s.get('SPECIALFUND', ''))
            rec['SPECIALF']  = sf if sf not in ('', '0', None) else '00'
            rec['PURPOSES']  = safe_str(s.get('PURPOSE_LOAN', ''))
            if rec['PURPOSES'] not in ('4500', '5300'):
                rec['PURPOSES'] = '5300'
            rec['FCONCEPT']  = 36
            rec['INTRATE']   = 6.95 * 100
            rec['STRUPCO_3YR'] = safe_str(s.get('STRUPCO_3YR', ''))
            rec['DSR']       = safe_float(s.get('DSRISS3', 0))

        else:
            ap = safe_float(s.get('ASSET_PURCH_AMT', 0))
            rec['SPAAMT'] = (ap * 100) if ap else 0.0
            rec['LN_UTILISE_LOCAT_CD'] = safe_str(s.get('LN_UTILISE_LOCAT_CD', ''))
            rec['STRUPCO_3YR'] = safe_str(s.get('STRUPCO_3YR', ''))
            rec['DSR']       = safe_float(s.get('DSRISS3', 0))
            curcode = rec.get('CURCODE', 'MYR') or 'MYR'
            amt = safe_float(s.get('AMOUNT', 0))
            if curcode in ('', 'MYR') and amt not in (None, 0.0):
                rec['APPRLMAA'] = amt
            if rec.get('FACILITY') == 34460:
                rec['APRVDT']    = safe_str(s.get('DTECOMPLETE', ''))
                rec['NOTETERM']  = 12
                rec['PAYFREQC']  = '18'
                if safe_str(s.get('APPTYPE', '')) == 'I':
                    rec['_DELETE'] = True  # LIMIT INCREASE - delete

        # BLRATE / OLRATE
        aadate_v2 = safe_int(rec.get('AADATE', 0))
        rec['BLRATE'] = blrate if aadate_v2 >= blreff_sas else olrate

        # TYPEPRC
        acctno_v = rec['ACCTNO']
        facgptype2 = rec.get('FACGPTYPE', '')
        pritype2   = rec.get('PRITYPE', '')
        product2   = rec.get('PRODUCT', 0)
        indexcd2   = rec.get('INDEXCD', 0)
        prirate2   = rec.get('PRIRATE', 0.0)
        pricing2   = rec.get('PRICING', 0.0)
        blrate_f   = safe_float(rec.get('BLRATE', '0.00'))

        if 2850000000 <= acctno_v <= 2859999999:
            typeprc = '79'
        elif facgptype2 == '6':
            typeprc = '80'
        else:
            if pritype2 == 'BLR':
                if product2 in (166, 168) and pricing2 < blrate_f:
                    typeprc = '42'
                elif product2 in (166, 168) and pricing2 >= blrate_f:
                    typeprc = '41'
                elif prirate2 < blrate_f:
                    typeprc = '42'
                else:
                    typeprc = '41'
            elif pritype2 == 'FIXED':
                typeprc = '59'
            elif pritype2 == 'BR':
                typeprc = '43'
            elif pritype2 == 'COF':
                cof_map = {900:'53',908:'54',912:'55',916:'56',904:'57',920:'58',924:'68'}
                typeprc = cof_map.get(indexcd2, '59')
            elif pritype2 == 'SOR':
                typeprc = '79' if indexcd2 == 925 else '59'
            elif pritype2 == 'ORFR':
                typeprc = '51'
            elif pritype2 == 'SBR':
                typeprc = '44'
            elif pritype2 == 'MYOR':
                typeprc = '61'
            elif pritype2 == 'MYORi':
                typeprc = '62'
            elif pritype2 == 'SOFR':
                typeprc = '63'
            elif pritype2 == 'SONIA':
                typeprc = '64'
            elif pritype2 == 'ESTR':
                typeprc = '65'
            elif pritype2 == 'SORA':
                typeprc = '66'
            elif pritype2 == 'TONAR':
                typeprc = '67'
            elif pritype2 == 'OORFR':
                typeprc = '51'
            else:
                typeprc = '59'
        rec['TYPEPRC'] = typeprc

    # Remove deleted records
    acctcred_records = [r for r in acctcred_records if not r.get('_DELETE')]

    # Sort by ACCTNO, NOTENO, AANO_X
    acctcred_records.sort(key=lambda r: (
        safe_int(r.get('ACCTNO', 0)),
        safe_int(r.get('NOTENO', 0)),
        safe_str(r.get('AANO_X', '')),
    ))

    # -----------------------------------------------------------------------
    # Merge WRIOFAC into ACCTCRED
    # -----------------------------------------------------------------------
    for rec in acctcred_records:
        key = (safe_int(rec.get('ACCTNO', 0)), safe_int(rec.get('NOTENO', 0)))
        wf = wriofac.get(key, {})
        if wf:
            rec['RESTRUCT'] = wf.get('RESTRUCT', rec.get('RESTRUCT', ''))
            if wf.get('ACCTSTAT'):
                rec['ACCTSTAT'] = wf['ACCTSTAT']

        # COMPLIDT formatted
        complidt = rec.get('COMPLIDT')
        if complidt is not None and safe_float(complidt, 0) != 0:
            cd = sas_to_date(int(complidt))
            rec['COMPLIDD'] = f'{cd.day:02d}'   if cd else '00'
            rec['COMPLIMM'] = f'{cd.month:02d}' if cd else '00'
            rec['COMPLIYY'] = f'{cd.year:04d}'  if cd else '0000'
        else:
            rec['COMPLIDD'] = '00'
            rec['COMPLIMM'] = '00'
            rec['COMPLIYY'] = '0000'

        # MODELDES AKPK override
        if rec.get('LOANTYPE', 0) in (128,130,380,381,700,705,720,725):
            if rec.get('MODELDES', '') in ('EAKPK', 'AKPK') and rec.get('ACCTSTAT') != 'S':
                rec['ACCTSTAT'] = 'K'

        # Delete if AADATE and ICDATE are same month/year
        aadate_v3 = safe_int(rec.get('AADATE', 0))
        icdate_v3 = safe_int(rec.get('ICDATE', 0)) if rec.get('ICDATE') else 0
        if aadate_v3 and icdate_v3:
            aa_dt = sas_to_date(aadate_v3)
            ic_dt = sas_to_date(icdate_v3)
            if (aa_dt and ic_dt and
                    aa_dt.month == ic_dt.month and aa_dt.year == ic_dt.year):
                rec['_DELETE'] = True

        # RJDATE formatted
        rjdate_v = rec.get('RJDATE')
        if rjdate_v is not None and safe_float(rjdate_v, 0) != 0:
            rj_dt = sas_to_date(int(rjdate_v))
            rec['RJDATEDD'] = f'{rj_dt.day:02d}'   if rj_dt else '00'
            rec['RJDATEMM'] = f'{rj_dt.month:02d}' if rj_dt else '00'
            rec['RJDATEYY'] = f'{rj_dt.year:04d}'  if rj_dt else '0000'
        else:
            rec['RJDATEDD'] = '00'
            rec['RJDATEMM'] = '00'
            rec['RJDATEYY'] = '0000'

        # ESMR 2025-1525: LU_SOURCE normalization
        lu_src = safe_str(rec.get('LU_SOURCE', ''))
        if lu_src and lu_src[0] == 'A':
            if lu_src not in ('A1', 'A2'):
                lu_src = 'A2'
            rec['LU_SOURCE'] = lu_src
        elif len(lu_src) >= 2 and lu_src[1] == 'P':
            rec['LU_SOURCE'] = lu_src[:2]

    acctcred_records = [r for r in acctcred_records if not r.get('_DELETE')]

    # -----------------------------------------------------------------------
    # PROC SORT: CREDIT by BRANCH, ACCTNO, AANO, COMMNO, NOTENO
    # + dedup (FIRST.BRANCH etc.) + output filter
    # -----------------------------------------------------------------------
    acctcred_records.sort(key=lambda r: (
        safe_int(r.get('BRANCH', 0)),
        safe_int(r.get('ACCTNO', 0)),
        safe_str(r.get('AANO', '')),
        safe_int(r.get('COMMNO', 0)),
        safe_int(r.get('NOTENO', 0)),
    ))

    # Keep first per group and apply output filter
    credit_records = []
    seen_keys = set()
    for rec in acctcred_records:
        key = (
            safe_int(rec.get('BRANCH', 0)),
            safe_int(rec.get('ACCTNO', 0)),
            safe_str(rec.get('AANO', '')),
            safe_int(rec.get('COMMNO', 0)),
            safe_int(rec.get('NOTENO', 0)),
        )
        if key not in seen_keys:
            seen_keys.add(key)
            acty  = rec.get('ACTY', 'LN')
            commno = safe_int(rec.get('COMMNO', 0))
            ind    = safe_str(rec.get('IND', ''))
            aano   = safe_str(rec.get('AANO', ''))
            paidind = safe_str(rec.get('PAIDIND', ''))

            if acty == 'OD':
                credit_records.append(rec)
            elif ((commno == 0 and ind == 'A') or
                  (commno > 0 and ind == 'B') or aano != ''):
                if paidind != 'P':
                    credit_records.append(rec)

    # PROC SUMMARY: SUM LIMTCURR by BRANCH, ACCTNO, AANO
    from collections import defaultdict
    limtcurr_sum = defaultdict(float)
    for rec in credit_records:
        key = (
            safe_int(rec.get('BRANCH', 0)),
            safe_int(rec.get('ACCTNO', 0)),
            safe_str(rec.get('AANO', '')),
        )
        limtcurr_sum[key] += safe_float(rec.get('LIMTCURR', 0))

    # Credit1: first per BRANCH, ACCTNO, AANO
    credit1_records = []
    seen2 = set()
    for rec in credit_records:
        key = (
            safe_int(rec.get('BRANCH', 0)),
            safe_int(rec.get('ACCTNO', 0)),
            safe_str(rec.get('AANO', '')),
        )
        if key not in seen2:
            seen2.add(key)
            credit1_records.append(rec)

    # Merge LNACC4 into credit records (by ACCTNO)
    lnacc4_map = {}
    if not lnacc4_processed.is_empty():
        for lr in lnacc4_processed.to_dicts():
            lnacc4_map[safe_int(lr.get('ACCTNO', 0))] = lr

    # -----------------------------------------------------------------------
    # Write output files
    # -----------------------------------------------------------------------
    subacred_lines = []
    creditpo_lines = []
    provisio_lines = []
    legalact_lines = []
    acctcred_lines = []

    def lpad(v, w):
        """Right-align integer in field width."""
        return fmt_n(v, w)

    def zpad(v, w):
        return fmt_z(v, w)

    def spad(v, w):
        return fmt_s(v, w)

    def fpad(v, w, d):
        return fmt_f(v, w, d)

    for rec in credit1_records:
        acctno  = safe_int(rec.get('ACCTNO', 0))
        lna     = lnacc4_map.get(acctno, {})
        fxrate  = 0
        curcode = rec.get('CURCODE', 'MYR') or 'MYR'
        if curcode != 'MYR':
            fxrate = int(get_forate(curcode) * 100000)

        k = (
            safe_int(rec.get('BRANCH', 0)),
            safe_int(rec.get('ACCTNO', 0)),
            safe_str(rec.get('AANO', '')),
        )
        limtcurr_tot = limtcurr_sum.get(k, safe_float(rec.get('LIMTCURR', 0)))
        aadate_dt2 = sas_to_date(safe_int(rec.get('AADATE', 0)))
        aadate_str = fmt_ddmmyy8(aadate_dt2) if aadate_dt2 else '        '

        # ACCTCRED record (LRECL=900)
        acctcred_line = build_record([
            (1,   lpad(rec.get('FICODE', 0), 9)),
            (10,  lpad(rec.get('APCODE', 0), 3)),
            (13,  lpad(acctno, 10)),
            (43,  spad(curcode, 3)),
            (46,  zpad(int(limtcurr_tot), 24)),
            (70,  zpad(int(safe_float(rec.get('LIMTCURR', 0))), 16)),
            (86,  spad(rec.get('ISSUEDD', ''), 2)),
            (88,  spad(rec.get('ISSUEMM', ''), 2)),
            (90,  spad(rec.get('ISSUEYY', ''), 4)),
            (94,  zpad(rec.get('OLDBRH', 0), 5)),
            (99,  zpad(int(safe_float(rec.get('LMTAMT', 0))), 16)),
            (115, spad(rec.get('LIABCODE', ''), 2)),
            (117, lpad(rec.get('COSTCTR', 0), 4)),
            (121, spad(rec.get('AANO', ''), 13)),
            (134, fpad(safe_float(rec.get('APPRLMAA', 0)), 15, 0)),
            (149, spad(rec.get('AADATEYY', ''), 4)),
            (153, spad(rec.get('AADATEMM', ''), 2)),
            (155, spad(rec.get('AADATEDD', ''), 2)),
            (157, spad(rec.get('RECONAME', ''), 60)),
            (217, spad(rec.get('REFTYPE', ''), 70)),
            (287, spad(rec.get('NMREF1', ''), 60)),
            (347, spad(rec.get('NMREF2', ''), 60)),
            (407, spad(rec.get('APVNME1', ''), 60)),
            (467, spad(rec.get('APVNME2', ''), 60)),
            (527, lpad(rec.get('FACILITY', 0), 5)),
            (532, spad(rec.get('NEWIC', ''), 20)),
            (553, aadate_str),
            (561, spad(rec.get('FACGPTYPE', ''), 1)),
            (563, lpad(rec.get('FACCODE', 0), 5)),
            (568, spad(lna.get('ACCT_AANO', ''), 13)),
            (581, lpad(lna.get('ACCT_LNTYPE', 0), 9)),
            (590, lpad(lna.get('ACCT_LNTERM', 0), 10)),
            (600, zpad(int(lna.get('ACCT_OUTBAL', 0)), 16)),
            (616, zpad(int(lna.get('ACCT_APLMT', 0)), 16)),
            (632, spad(lna.get('ACCT_ALDD', ''), 2)),
            (634, spad(lna.get('ACCT_ALMM', ''), 2)),
            (636, spad(lna.get('ACCT_ALYY', ''), 4)),
            (640, spad(lna.get('ACCT_REVDD', ''), 2)),
            (642, spad(lna.get('ACCT_REVMM', ''), 2)),
            (644, spad(lna.get('ACCT_REVYY', ''), 4)),
            (648, spad(lna.get('ACCT_1DISDD', ''), 2)),
            (650, spad(lna.get('ACCT_1DISMM', ''), 2)),
            (652, spad(lna.get('ACCT_1DISYY', ''), 4)),
            (656, spad(lna.get('ACCT_SETDD', ''), 2)),
            (658, spad(lna.get('ACCT_SETMM', ''), 2)),
            (660, spad(lna.get('ACCT_SETYY', ''), 4)),
            (664, spad(lna.get('ACCT_EXPDD', ''), 2)),
            (666, spad(lna.get('ACCT_EXPMM', ''), 2)),
            (668, spad(lna.get('ACCT_EXPYY', ''), 4)),
            (672, zpad(fxrate, 8)),
            (680, spad(rec.get('APRVDT', ''), 10)),
            (690, zpad(int(safe_float(rec.get('UNDRAWN', 0))), 17)),
        ])
        acctcred_lines.append(acctcred_line.ljust(900)[:900])

    # -----------------------------------------------------------------------
    # SUBACRED / CREDITPO / PROVISIO / LEGALACT  (per original credit record)
    # -----------------------------------------------------------------------
    for rec in credit_records:
        acctno = safe_int(rec.get('ACCTNO', 0))
        noteno = safe_int(rec.get('NOTENO', 0))
        aadate_dt3 = sas_to_date(safe_int(rec.get('AADATE', 0)))
        aadate_str3 = fmt_ddmmyy8(aadate_dt3) if aadate_dt3 else '        '

        # SUBACRED (LRECL=950)
        sub_line = build_record([
            (1,    lpad(rec.get('FICODE', 0), 9)),
            (10,   lpad(rec.get('APCODE', 0), 3)),
            (13,   lpad(acctno, 10)),
            (43,   lpad(noteno, 10)),
            (73,   lpad(rec.get('FACILITY', 0), 5)),
            (78,   spad(rec.get('SYNDICAT', 'N'), 1)),
            (79,   spad(rec.get('SPECIALF', '00'), 2)),
            (81,   spad(rec.get('PURPOSES', ''), 4)),
            (85,   lpad(rec.get('FCONCEPT', 0), 2)),
            (87,   zpad(rec.get('NOTETERM', 0), 3)),
            (90,   spad(rec.get('PAYFREQC', ''), 2)),
            (92,   spad(rec.get('RESTRUCT', ''), 1)),
            (93,   lpad(rec.get('CAGAMAS', 0), 8)),
            (101,  spad(rec.get('RECOURSE', ''), 1)),
            (102,  '00000000'),
            (110,  lpad(rec.get('CUSTCODE', 0), 2)),
            (112,  spad(rec.get('SECTOR', ''), 4)),
            (116,  lpad(rec.get('OLDBRH', 0), 5)),
            (121,  spad(rec.get('SCORE', ''), 3)),
            (124,  lpad(rec.get('COSTCTR', 0), 4)),
            (128,  spad(rec.get('PAYDD', '00'), 2)),
            (130,  spad(rec.get('PAYMM', '00'), 2)),
            (132,  spad(rec.get('PAYYR', '0000'), 4)),
            (136,  spad(rec.get('GRANTDD', '00'), 2)),
            (138,  spad(rec.get('GRANTMM', '00'), 2)),
            (140,  spad(rec.get('GRANTYR', '0000'), 4)),
            (144,  zpad(int(safe_float(rec.get('CENSUS', 0))), 6)),
            (150,  spad(rec.get('LNMATDD', '00'), 2)),
            (152,  spad(rec.get('LNMATMM', '00'), 2)),
            (154,  spad(rec.get('LNMATYR', '0000'), 4)),
            (158,  spad(rec.get('ORMATDD', '00'), 2)),
            (160,  spad(rec.get('ORMATMM', '00'), 2)),
            (162,  spad(rec.get('ORMATYR', '0000'), 4)),
            (166,  lpad(int(safe_float(rec.get('CAVAIAMT', 0))), 16)),
            (182,  spad(rec.get('COLLMAKE', ''), 6)),
            (188,  spad(rec.get('MODELDES', ''), 6)),
            (194,  spad(rec.get('DLVRDD', '00'), 2)),
            (196,  spad(rec.get('DLVRMM', '00'), 2)),
            (198,  spad(rec.get('DLVRYR', '0000'), 4)),
            (202,  spad(rec.get('COLLMM', '00'), 2)),
            (204,  spad(rec.get('COLLYR', '00'), 2)),
            (206,  spad(rec.get('BILDUEDD', '00'), 2)),
            (208,  spad(rec.get('BILDUEMM', '00'), 2)),
            (210,  spad(rec.get('BILDUEYR', '0000'), 4)),
            (214,  spad(rec.get('PAYFREQ', ''), 1)),
            (215,  spad(rec.get('ORGTYPE', ''), 1)),
            (216,  spad(rec.get('PAIDIND', ''), 1)),
            (217,  spad(rec.get('AANO', ''), 13)),
            (231,  aadate_str3),
            (239,  spad(rec.get('FACGPTYPE', ''), 1)),
            (240,  lpad(rec.get('FACCODE', 0), 5)),
            (245,  lpad(int(safe_float(rec.get('CIR', 0))), 5)),
            (250,  spad(rec.get('TYPEPRC', ''), 2)),
            (252,  zpad(int(safe_float(rec.get('SPAAMT', 0))), 16)),
            (269,  spad(rec.get('DNBFISME', ''), 1)),
            (271,  spad(rec.get('RJDATEYY', '0000'), 4)),
            (275,  spad(rec.get('RJDATEMM', '00'), 2)),
            (277,  spad(rec.get('RJDATEDD', '00'), 2)),
            (280,  spad(rec.get('REASON', ''), 294)),
            (574,  spad(rec.get('STRUPCO_3YR', ''), 5)),     # 2018-1432 & 2018-1442
            (579,  spad(rec.get('REFIN_FLG', ''), 3)),       # 2018-1432 & 2018-1442
            (582,  spad(rec.get('OLD_FI_CD', ''), 10)),      # 2018-1432 & 2018-1442
            (592,  spad(rec.get('OLD_MASTACC', ''), 30)),    # 2018-1432 & 2018-1442
            (622,  spad(rec.get('OLD_SUBACC', ''), 30)),     # 2018-1432 & 2018-1442
            (652,  fpad(safe_float(rec.get('REFIN_AMT', 0)), 20, 2)),  # 2018-1432 & 2018-1442
            (672,  fpad(safe_float(rec.get('DSR', 0)), 6, 2)),        # 2018-1432 & 2018-1442
            (678,  spad(rec.get('LN_UTILISE_LOCAT_CD', ''), 5)),       # 2018-1432 & 2018-1442
            (683,  spad(rec.get('PRIORITY_SECTOR', ''), 2)),           # 2020-296
            (685,  spad(rec.get('LN_UTILISE_LOCAT_CD', ''), 5)),       # 2020-296
            (690,  lpad(int(safe_float(rec.get('EIR', 0))), 10)),      # 2020-296
            (700,  spad(rec.get('CURCODE', ''), 3)),                    # 2021-2603
            (703,  spad(rec.get('CCPT_TAG', ''), 5)),                  # 2023-3235
            (710,  spad(rec.get('LU_ADD1', ''), 40)),                  # 2023-3931
            (750,  spad(rec.get('LU_ADD2', ''), 40)),
            (790,  spad(rec.get('LU_ADD3', ''), 40)),
            (830,  spad(rec.get('LU_ADD4', ''), 40)),
            (870,  spad(rec.get('LU_TOWN_CITY', ''), 20)),
            (890,  spad(rec.get('LU_POSTCODE', ''), 5)),
            (895,  spad(rec.get('LU_STATE_CD', ''), 2)),
            (897,  spad(rec.get('LU_COUNTRY_CD', ''), 2)),
            (899,  spad(rec.get('LU_SOURCE', ''), 5)),
        ])
        subacred_lines.append(sub_line.ljust(950)[:950])

        # CREDITPO (LRECL=400)
        cp_line = build_record([
            (1,    lpad(rec.get('FICODE', 0), 9)),
            (10,   lpad(rec.get('APCODE', 0), 3)),
            (13,   lpad(acctno, 10)),
            (43,   lpad(noteno, 10)),
            (73,   reptday),
            (75,   reptmon),
            (77,   reptyear),
            (81,   zpad(int(safe_float(rec.get('OUTSTAND', 0))), 16)),
            (97,   zpad(safe_int(rec.get('ARREARS', 0)), 3)),
            (100,  zpad(safe_int(rec.get('INSTALM', 0)), 3)),
            (103,  zpad(int(safe_float(rec.get('UNDRAWN', 0))), 17)),
            (120,  spad(rec.get('ACCTSTAT', 'O'), 1)),
            (121,  zpad(safe_int(rec.get('NODAYS', 0)), 5)),
            (126,  lpad(rec.get('OLDBRH', 0), 5)),
            (131,  zpad(int(safe_float(rec.get('BILTOT', 0))), 17)),
            (148,  zpad(int(safe_float(rec.get('ODXSAMT', 0))), 17)),
            (165,  lpad(rec.get('COSTCTR', 0), 4)),
            (169,  spad(rec.get('AANO', ''), 13)),
            (182,  lpad(rec.get('FACILITY', 0), 5)),
            (187,  spad(rec.get('COMPLIBY', ''), 60)),
            (247,  spad(rec.get('COMPLIYY', '0000'), 4)),
            (251,  spad(rec.get('COMPLIMM', '00'), 2)),
            (253,  spad(rec.get('COMPLIDD', '00'), 2)),
            (255,  spad(rec.get('COMPLIGR', ''), 15)),
            (270,  lpad(rec.get('FACCODE', 0), 5)),
            (276,  lpad(int(safe_float(rec.get('MTHINSTLMT', 0))), 16)),
            (292,  spad(rec.get('CURCODE', ''), 3)),
        ])
        creditpo_lines.append(cp_line.ljust(400)[:400])

        # PROVISIO (LRECL=400) - only for NPL
        classifi = rec.get('CLASSIFI', '')
        loantype = safe_int(rec.get('LOANTYPE', 0))
        if (classifi in ('S', 'D', 'B') or rec.get('GP3IND', '') != '') and \
                loantype not in HP:
            pv_line = build_record([
                (1,    lpad(rec.get('FICODE', 0), 9)),
                (10,   lpad(rec.get('APCODE', 0), 3)),
                (13,   lpad(acctno, 10)),
                (43,   lpad(noteno, 10)),
                (73,   reptday),
                (75,   reptmon),
                (77,   reptyear),
                (81,   spad(classifi, 1)),
                (82,   zpad(safe_int(rec.get('ARREARS', 0)), 3)),
                (85,   zpad(int(safe_float(rec.get('CURBAL', 0))), 17)),
                (102,  zpad(int(safe_float(rec.get('INTERDUE', 0))), 17)),
                (119,  zpad(int(safe_float(rec.get('FEEAMT', 0))), 16)),
                (135,  zpad(int(safe_float(rec.get('REALISAB', 0))), 17)),
                (152,  zpad(int(safe_float(rec.get('IISOPBAL', 0))), 17)),
                (169,  zpad(int(safe_float(rec.get('TOTIIS', 0))), 17)),
                (186,  zpad(int(safe_float(rec.get('TOTIISR', 0))), 17)),
                (203,  zpad(int(safe_float(rec.get('TOTWOF', 0))), 17)),
                (220,  zpad(0, 17)),  # IISDANAH
                (237,  zpad(0, 17)),  # IISTRANS
                (254,  zpad(int(safe_float(rec.get('SPOPBAL', 0))), 17)),
                (271,  zpad(0, 17)),  # SPCHARGE
                (288,  zpad(int(safe_float(rec.get('SPWBAMT', 0))), 17)),
                (305,  zpad(int(safe_float(rec.get('SPWOF', 0))), 17)),
                (322,  zpad(int(safe_float(rec.get('SPDANAH', 0))), 17)),
                (339,  zpad(int(safe_float(rec.get('SPTRANS', 0))), 17)),
                (356,  spad(rec.get('GP3IND', ''), 1)),
                (357,  lpad(rec.get('OLDBRH', 0), 5)),
                (362,  lpad(rec.get('COSTCTR', 0), 5)),
                (367,  spad(rec.get('AANO', ''), 13)),
                (380,  lpad(rec.get('FACILITY', 0), 5)),
                (385,  lpad(rec.get('FACCODE', 0), 5)),
            ])
            provisio_lines.append(pv_line.ljust(400)[:400])

        # LEGALACT (LRECL=100)
        if rec.get('LEG', '') == 'A':
            la_line = build_record([
                (1,   lpad(rec.get('FICODE', 0), 9)),
                (10,  lpad(rec.get('APCODE', 0), 3)),
                (13,  lpad(acctno, 10)),
                (43,  spad(rec.get('DELQCD', ''), 2)),
                (45,  reptday),
                (47,  reptmon),
                (49,  reptyear),
                (53,  lpad(rec.get('OLDBRH', 0), 5)),
                (58,  lpad(rec.get('COSTCTR', 0), 4)),
                (62,  spad(rec.get('AANO', ''), 13)),
                (75,  lpad(rec.get('FACILITY', 0), 5)),
                (80,  lpad(rec.get('FACCODE', 0), 5)),
            ])
            legalact_lines.append(la_line.ljust(100)[:100])

    # ACCTREJT - write from LNHPRJ
    acctrejt_lines = []
    for rec in lnhprj_rows:
        icdate_v4 = safe_int(rec.get('ICDATE', 0))
        icdate_dt4 = sas_to_date(icdate_v4) if icdate_v4 else None
        icdate_str4 = fmt_ddmmyy8(icdate_dt4)
        ar_line = build_record([
            (1,   lpad(safe_int(rec.get('BRANCH', 0)), 9)),
            (11,  lpad(safe_int(rec.get('ACCTNO', 0)), 10)),
            (22,  spad(safe_str(rec.get('AANO', '')), 13)),
            (36,  icdate_str4),
            (45,  lpad(safe_int(rec.get('NOTENO', 0)), 5)),
            (51,  spad(safe_str(rec.get('CCRIS_STAT', '')), 1)),
            (53,  spad(safe_str(rec.get('REVERSED', '')), 1)),
        ])
        acctrejt_lines.append(ar_line.ljust(200)[:200])

    # -----------------------------------------------------------------------
    # Write output files
    # -----------------------------------------------------------------------
    with open(SUBACRED_FILE, 'w', encoding='latin-1') as f:
        f.writelines(line + '\n' for line in subacred_lines)
    logger.info(f"Written SUBACRED ({len(subacred_lines)} records): {SUBACRED_FILE}")

    with open(CREDITPO_FILE, 'w', encoding='latin-1') as f:
        f.writelines(line + '\n' for line in creditpo_lines)
    logger.info(f"Written CREDITPO ({len(creditpo_lines)} records): {CREDITPO_FILE}")

    with open(PROVISIO_FILE, 'w', encoding='latin-1') as f:
        f.writelines(line + '\n' for line in provisio_lines)
    logger.info(f"Written PROVISIO ({len(provisio_lines)} records): {PROVISIO_FILE}")

    with open(LEGALACT_FILE, 'w', encoding='latin-1') as f:
        f.writelines(line + '\n' for line in legalact_lines)
    logger.info(f"Written LEGALACT ({len(legalact_lines)} records): {LEGALACT_FILE}")

    with open(ACCTCRED_FILE, 'w', encoding='latin-1') as f:
        f.writelines(line + '\n' for line in acctcred_lines)
    logger.info(f"Written ACCTCRED ({len(acctcred_lines)} records): {ACCTCRED_FILE}")

    with open(ACCTREJT_FILE, 'w', encoding='latin-1') as f:
        f.writelines(line + '\n' for line in acctrejt_lines)
    logger.info(f"Written ACCTREJT ({len(acctrejt_lines)} records): {ACCTREJT_FILE}")

    # -----------------------------------------------------------------------
    # %MONTHLY macro: save CCRIS.COLLATER and CCRIS.WRITOFF if WK != 4
    # %MACRO MONTHLY;
    #    %IF "&NOWK" NE "4" %THEN %DO;
    #         DATA CCRIS.COLLATER;  SET CREDIT;
    #         DATA CCRIS.WRITOFF;   KEEP ACCTNO NOTENO ACCTSTAT RESTRUCT;
    #                               SET CREDIT; IF ACCTSTAT='W';
    #    %END;
    # %MEND MONTHLY;
    # %MONTHLY;  -- (commented out in original)
    # -----------------------------------------------------------------------

    # DATA OD block -- entire section is commented out in original SAS:
    # DATA OD;  SET ACCTCRED; IF  (3000000000<=ACCTNO<=3999999999);
    # PROC SORT; BY ACCTNO;
    # PROC SORT DATA=CCRIS.COLLATER OUT=ODCR NODUPKEY; BY ACCTNO;
    # WHERE (3000000000<=ACCTNO<=3999999999);
    # DATA OD;  MERGE OD(IN=A) ODCR(IN=B); BY ACCTNO;
    # IF B AND NOT A; ACCTSTAT='S';
    # FILE CREDITOD; PUT ...;

    logger.info("EIIWCCR6 completed successfully.")


if __name__ == '__main__':
    main()
