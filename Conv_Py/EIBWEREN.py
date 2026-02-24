# !/usr/bin/env python3
"""
Program : EIBWEREN
Function: ELDS Basel Review Data Extraction and Combination
          Reads 19 ELDS Basel review text files, validates dates against
            current report date, parses fixed-width records into structured
            datasets, appends previous period data, merges into combined
            tables, and exports for FTP transfer.

SMR 2010-2289
TOTAL 19 TABLES
"""

import sys
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import duckdb

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
BASE_DIR = Path('/data')

# Input text files (ELDSRV1 .. ELDSRV19) - generation 0 (current)
ELDS_INPUT_DIR = BASE_DIR / 'elds/input'
ELDSRV_FILES = {i: ELDS_INPUT_DIR / f'rv{i:02d}.txt' for i in range(1, 20)}

# Previous period parquet datasets (EREVO1, EREVO2, EREVO3)
EREVO1_DIR = BASE_DIR / 'elds/erevo1'
EREVO2_DIR = BASE_DIR / 'elds/erevo2'
EREVO3_DIR = BASE_DIR / 'elds/erevo3'

# Current period output parquet datasets (EREV1, EREV2, EREV3)
EREV1_DIR = BASE_DIR / 'elds/erev1'
EREV2_DIR = BASE_DIR / 'elds/erev2'
EREV3_DIR = BASE_DIR / 'elds/erev3'

# Combined ELDSRV output directory
ELDSRV_OUT_DIR = BASE_DIR / 'elds/eldsrv'

# FTP output directory
FTP_OUT_DIR = BASE_DIR / 'elds/erevftp'

for d in [EREV1_DIR, EREV2_DIR, EREV3_DIR, ELDSRV_OUT_DIR, FTP_OUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# UTILITY
# ---------------------------------------------------------------------------

def safe_int(s: str) -> int | None:
    s = s.strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None

def safe_float(s: str) -> float | None:
    s = s.strip().replace(',', '')
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None

def mdy(mm, dd, yy) -> date | None:
    try:
        mm = int(mm) if mm else 0
        dd = int(dd) if dd else 0
        yy = int(yy) if yy else 0
        if mm == 0 or dd == 0 or yy == 0:
            return None
        return date(yy, mm, dd)
    except Exception:
        return None

def read_fixed(line: str, start: int, length: int, upcase: bool = False) -> str:
    """Read fixed-width field from line (1-based start). Returns stripped string."""
    s = line[start - 1: start - 1 + length] if len(line) >= start - 1 + length else \
        line[start - 1:].ljust(length)
    s = s.strip()
    return s.upper() if upcase else s

def read_num(line: str, start: int, length: int) -> int | None:
    return safe_int(line[start - 1: start - 1 + length] if len(line) >= start - 1 + length else '')

def read_float_comma(line: str, start: int, length: int) -> float | None:
    return safe_float(line[start - 1: start - 1 + length] if len(line) >= start - 1 + length else '')

# ---------------------------------------------------------------------------
# REPTDATE DERIVATION
# ---------------------------------------------------------------------------

def get_reptdate() -> dict:
    """
    Derive report date and week from today's date.
    Equivalent to DATA ELDSRV.REPTDATE in SAS.
    """
    today = date.today()
    d = today.day

    if 8 <= d <= 14:
        reptdate = date(today.year, today.month, 8)
        wk = '1'
    elif 15 <= d <= 21:
        reptdate = date(today.year, today.month, 15)
        wk = '2'
    elif 22 <= d <= 27:
        reptdate = date(today.year, today.month, 22)
        wk = '3'
    else:
        # Last day of previous month
        first_of_month = date(today.year, today.month, 1)
        reptdate = first_of_month - timedelta(days=1)
        wk = '4'

    reptmon  = f'{reptdate.month:02d}'
    reptyear = str(reptdate.year)[2:]
    rdate    = reptdate.strftime('%d/%m/%y')

    return {
        'reptdate': reptdate,
        'nowk': wk,
        'reptmon': reptmon,
        'reptyear': reptyear,
        'rdate': rdate,
    }

# ---------------------------------------------------------------------------
# DATE VALIDATION: Read date from first record of each input file
# Cols 1-9: files 1-9 use @041 DD, @044 MM, @047 YY
# Files 10-19 use @042 DD, @045 MM, @048 YY
# ---------------------------------------------------------------------------

def read_file_date(filepath: Path, file_num: int) -> date | None:
    """
    Read the date embedded in the first data record of an ELDS text file.
    Files 1-9:  @041 DD 2., @044 MM 2., @047 YY 4.
    Files 10-19: @042 DD 2., @045 MM 2., @048 YY 4.
    """
    if not filepath.exists():
        logger.warning(f"Input file not found: {filepath}")
        return None
    try:
        with open(filepath, 'r', encoding='latin-1') as f:
            line = f.readline()   # OBS=1 means first record
        if file_num <= 9:
            dd = safe_int(line[40:42])
            mm = safe_int(line[43:45])
            yy = safe_int(line[46:50])
        else:
            dd = safe_int(line[41:43])
            mm = safe_int(line[44:46])
            yy = safe_int(line[47:51])
        return mdy(mm, dd, yy)
    except Exception as e:
        logger.error(f"Error reading date from {filepath}: {e}")
        return None

# ---------------------------------------------------------------------------
# PARSERS FOR EACH TABLE
# ---------------------------------------------------------------------------

def parse_eln1(filepath: Path) -> list[dict]:
    """Parse ELDSRV1 -> ELN1 (FIRSTOBS=2)."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:  # FIRSTOBS=2
        if len(line) < 2:
            continue
        line = line.rstrip('\n')
        newid     = read_fixed(line,    1, 12)
        reviewno  = read_fixed(line,   16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        aano      = read_fixed(line,   36, 20, upcase=True)
        custname  = read_fixed(line,   59, 50, upcase=True)
        seqno     = read_num(line,    112, 2)
        revstat   = read_fixed(line,  117, 19, upcase=True)
        dservra   = read_fixed(line,  139, 16, upcase=True)   # DEBT SERV RATIO
        networth  = read_fixed(line,  158, 19, upcase=True)
        maval     = read_num(line,    180, 6)                  # MA
        crr1      = read_fixed(line,  189,  3, upcase=True)   # LATEST CRR
        crr2      = read_fixed(line,  195,  3, upcase=True)   # PREV CRR
        crr3      = read_fixed(line,  201,  3, upcase=True)   # FIRST CRR
        yearc     = read_num(line,    207, 22)                 # GOOD CONDUCT
        yearcr    = read_fixed(line,  232, 38, upcase=True)   # GOOD CONDUCT-RE
        yearbs    = read_num(line,    273, 26)                 # BUSS.YEAR
        mgmexp    = read_fixed(line,  302, 25, upcase=True)   # MGM EXPERIENCES
        indtype   = read_fixed(line,  330, 10, upcase=True)   # TYPE OF IND.
        turnovr   = read_fixed(line,  343, 25, upcase=True)
        netproft  = read_fixed(line,  371, 15, upcase=True)   # TYPE OF IND.
        tstratio  = read_fixed(line,  389, 15, upcase=True)   # ACID TEST RATIO
        leverge   = read_fixed(line,  407, 11, upcase=True)
        intcovr   = read_fixed(line,  421, 20, upcase=True)   # INT COVERG RAT.
        avgcper   = read_fixed(line,  444, 26, upcase=True)   # AVG COLL PERIOD
        cashflw   = read_fixed(line,  473, 35, upcase=True)   # CURR. YEAR NET
        prholder  = read_fixed(line,  564, 40, upcase=True)   # PRIME MOVER
        expernc   = read_num(line,    607,  4)                 # EXPR. SINCE
        nrdd      = read_num(line,    614,  2)                 # NEXT REVIEW DD
        nrmm      = read_num(line,    617,  2)                 # NEXT REVIEW MM
        nryy      = read_num(line,    620,  4)                 # NEXT REVIEW YY
        cparti    = read_fixed(line,  627,  3, upcase=True)   # CONNECTING PART
        staffnm   = read_fixed(line,  633, 50, upcase=True)   # STAFF NAME
        bodmem    = read_fixed(line,  686,  3, upcase=True)
        staffid   = read_num(line,    692,  5)
        sbranch   = read_fixed(line,  700, 30, upcase=True)   # BRANCH / HO
        totsval   = read_fixed(line,  839,  6)                # TOTAL SEC VAL
        maodfl    = read_fixed(line,  848,  6, upcase=True)   # MA ON OD+FL
        netexpo   = read_fixed(line,  857, 15, upcase=True)   # NET EXPOSURE
        sblcbg    = read_fixed(line,  875, 15, upcase=True)   # SBLC/BG AMOUNT
        sbdd      = read_num(line,    893,  2)                 # SBLC EXPIRY DD
        sbmm      = read_num(line,    896,  2)                 # SBLC EXPIRY MM
        sbyy      = read_num(line,    899,  4)                 # SBLC EXPIRY YY
        issbank   = read_fixed(line,  907, 30, upcase=True)   # ISSUING BANK
        clmprd    = read_fixed(line,  940, 11, upcase=True)   # CLAIM PERIOD
        sblcbgno  = read_fixed(line,  954, 10, upcase=True)   # SBLC/BG NO
        cgcamt    = read_fixed(line,  967, 15, upcase=True)   # CGC AMOUNT
        cgcdd     = read_num(line,    985,  2)                 # CGC EXPIRY DD
        cgcmm     = read_num(line,    988,  2)                 # CGC EXPIRY MM
        cgcyy     = read_num(line,    991,  4)                 # CGC EXPIRY YY
        cgcnum    = read_fixed(line,  999,  8, upcase=True)   # CGC NO
        apdd      = read_num(line,   1010,  2)                 # APPROVAL DD
        apmm      = read_num(line,   1013,  2)                 # APPROVAL MM
        apyy      = read_num(line,   1016,  4)                 # APPROVAL YY
        oriexpdt  = read_fixed(line, 1156, 10)                 # ORIGINAL EXPIRY DATE

        nxrevdt = mdy(nrmm, nrdd, nryy)
        sblcxdt = mdy(sbmm, sbdd, sbyy)
        cgcexdt = mdy(cgcmm, cgcdd, cgcyy)
        apprdt  = mdy(apmm, apdd, apyy)

        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno, 'AANO': aano,
            'CUSTNAME': custname, 'SEQNO': seqno, 'REVSTAT': revstat,
            'DSERVRA': dservra, 'NETWORTH': networth, 'MAVAL': maval,
            'CRR1': crr1, 'CRR2': crr2, 'CRR3': crr3,
            'YEARC': yearc, 'YEARCR': yearcr, 'YEARBS': yearbs,
            'MGMEXP': mgmexp, 'INDTYPE': indtype, 'TURNOVR': turnovr,
            'NETPROFT': netproft, 'TSTRATIO': tstratio, 'LEVERGE': leverge,
            'INTCOVR': intcovr, 'AVGCPER': avgcper, 'CASHFLW': cashflw,
            'PRHOLDER': prholder, 'EXPERNC': expernc,
            'CPARTI': cparti, 'STAFFNM': staffnm, 'BODMEM': bodmem,
            'STAFFID': staffid, 'SBRANCH': sbranch,
            'TOTSVAL': totsval, 'MAODFL': maodfl, 'NETEXPO': netexpo,
            'SBLCBG': sblcbg, 'ISSBANK': issbank, 'CLMPRD': clmprd,
            'SBLCBGNO': sblcbgno, 'CGCAMT': cgcamt, 'CGCNUM': cgcnum,
            'ORIEXPDT': oriexpdt,
            'NXREVDT': nxrevdt, 'SBLCXDT': sblcxdt,
            'CGCEXDT': cgcexdt, 'APPRDT': apprdt,
        })
    return records


def parse_eln2(filepath: Path) -> list[dict]:
    """Parse ELDSRV2 -> ELN2."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,  1, 12, upcase=True)
        reviewno = read_fixed(line, 16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        appramt  = read_float_comma(line, 612, 11)   # APPROVED AMT
        operlmt  = read_float_comma(line, 626, 11)   # APPROVED AMT
        balance  = read_float_comma(line, 640, 11)   # BALANCE
        excsarr  = read_float_comma(line, 654, 11)   # EXCESS/ARREARS
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'APPRAMT': appramt, 'OPERLMT': operlmt,
            'BALANCE': balance, 'EXCSARR': excsarr,
        })
    return records


def parse_eln3(filepath: Path) -> list[dict]:
    """Parse ELDSRV3 -> ELN3."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,  1, 12)
        reviewno = read_fixed(line, 16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        records.append({'NEWID': newid, 'REVIEWNO': reviewno})
    return records


def parse_eln4(filepath: Path) -> list[dict]:
    """Parse ELDSRV4 -> ELN4."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        odacct   = read_fixed(line,  36, 43, upcase=True)   # CONDUCT OF ACC -OD
        flacct   = read_fixed(line,  82, 29, upcase=True)   # CONDUCT OF ACC -FL
        tfacct   = read_fixed(line, 114, 29, upcase=True)   # CONDUCT OF ACC -TF
        bbacct   = read_fixed(line, 146, 29, upcase=True)   # CONDUCT - BASIS
        bbreas   = read_fixed(line, 178, 70, upcase=True)   # CONDUCT-REASON BASIS
        ccacct   = read_fixed(line, 251, 56, upcase=True)   # CONDUCT OF ACC -CCR
        ccreas   = read_fixed(line, 310, 70, upcase=True)   # CONDUCT-REASON CCRIS
        # @383  BNMCD     $4.   BNMCODE  (commented out in SAS)
        bnmcd    = read_fixed(line, 940,  6, upcase=True)   # BNMCODE
        custcd   = read_fixed(line, 390, 38, upcase=True)   # CUSTOMER CODE - DESC
        smicd    = read_fixed(line, 431,  3)                 # SMI - Y/N
        noncom1  = read_fixed(line, 437, 100, upcase=True)  # NON COMPLIANCE
        noncom2  = read_fixed(line, 537, 100, upcase=True)  # NON COMPLIANCE-CONTD
        noncom3  = read_fixed(line, 637, 100, upcase=True)  # NON COMPLIANCE-CONTD
        noncom4  = read_fixed(line, 737, 100, upcase=True)  # NON COMPLIANCE-CONTD
        noncom5  = read_fixed(line, 837, 100, upcase=True)  # NON COMPLIANCE-CONTD
        bnmsect  = read_fixed(line, 940,  6, upcase=True)   # BNM SECTOR CODE
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'ODACCT': odacct, 'FLACCT': flacct, 'TFACCT': tfacct,
            'BBACCT': bbacct, 'BBREAS': bbreas, 'CCACCT': ccacct,
            'CCREAS': ccreas, 'BNMCD': bnmcd, 'CUSTCD': custcd,
            'SMICD': smicd, 'NONCOM1': noncom1, 'NONCOM2': noncom2,
            'NONCOM3': noncom3, 'NONCOM4': noncom4, 'NONCOM5': noncom5,
            'BNMSECT': bnmsect,
        })
    return records


def parse_eln5(filepath: Path) -> list[dict]:
    """Parse ELDSRV5 -> ELN5."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        joint    = read_fixed(line,  36,  1)                 # JOINT INDICATOR
        lmdd     = read_num(line,    40,  2)                  # LAST MAINTENANCE DD
        lmmm     = read_num(line,    43,  2)                  # LAST MAINTENANCE MM
        lmyy     = read_num(line,    46,  4)                  # LAST MAINTENANCE YY
        reason   = read_fixed(line,  53, 72, upcase=True)    # UNSATIS.COND-ACCT
        reason1  = read_fixed(line, 128, 71, upcase=True)    # UNSATIS.COND-REL ACC
        turnovr  = read_fixed(line, 202, 25, upcase=True)    # TURNOVER
        capital  = read_fixed(line, 230, 16, upcase=True)    # CAPITAL
        assets   = read_fixed(line, 249, 16, upcase=True)    # ASSETS
        liabil   = read_fixed(line, 268, 16, upcase=True)    # LIABILITIES
        fincost  = read_fixed(line, 287, 16, upcase=True)    # TOT INT/FIN COST
        netprof  = read_fixed(line, 306, 16, upcase=True)    # NET PROF B4 TAX/INTR.
        stock    = read_fixed(line, 325, 16, upcase=True)    # STOCK
        tolscore = read_fixed(line, 344,  3, upcase=True)    # TOTAL SCORE
        ptolscore = read_fixed(line, 350, 3, upcase=True)    # PERCENTAGE TOTAL SCORE
        newspacct = read_fixed(line, 356, 1, upcase=True)    # NEW SPECIAL MENTION ACCT
        upspacct  = read_fixed(line, 360, 1, upcase=True)    # UPLIFTED SPECIAL MEN ACCT
        lastmdt = mdy(lmmm, lmdd, lmyy)
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'JOINT': joint, 'REASON': reason, 'REASON1': reason1,
            'TURNOVR': turnovr, 'CAPITAL': capital, 'ASSETS': assets,
            'LIABIL': liabil, 'FINCOST': fincost, 'NETPROF': netprof,
            'STOCK': stock, 'TOLSCORE': tolscore, 'PTOLSCORE': ptolscore,
            'NEWSPACCT': newspacct, 'UPSPACCT': upspacct,
            'LASTMDT': lastmdt,
        })
    return records


def parse_eln6(filepath: Path) -> list[dict]:
    """Parse ELDSRV6 -> ELN6."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)   # REVIEW NO
        if not newid or not reviewno:
            continue
        tyreview = read_fixed(line,  36, 20, upcase=True)   # TYPE OF REVIEW
        tycrr    = read_fixed(line,  59, 13, upcase=True)   # TYPE OF CRR
        revnoc   = read_fixed(line,  75, 17, upcase=True)   # REVIEW NO.(CURRENT)
        dddate   = read_num(line,    95,  2)                  # DATE DD
        mmdate   = read_num(line,    98,  2)                  # DATE MM
        yydate   = read_num(line,   101,  4)                  # DATE YY
        expsin   = read_fixed(line, 108, 10, upcase=True)   # EXPERIENCE SINCE
        bnmcode  = read_fixed(line, 121,  6, upcase=True)   # BNM CODE
        otas     = read_fixed(line, 130,  3, upcase=True)   # OTAS
        cobf     = read_fixed(line, 136,  3, upcase=True)   # COBF
        dcheqs   = read_fixed(line, 142,  3, upcase=True)   # DCHEQS
        ccris    = read_fixed(line, 148,  3, upcase=True)   # CCRIS
        limitfac = read_fixed(line, 154,  4, upcase=True)   # IF YES/NO LMT FAC 50
        otas1    = read_fixed(line, 161,  3, upcase=True)   # OTAS1
        cobf1    = read_fixed(line, 167,  3, upcase=True)   # COBF1
        dcheqs1  = read_fixed(line, 173,  3, upcase=True)   # DCHEQS1
        ccris1   = read_fixed(line, 179,  3, upcase=True)   # CCRIS1
        ynlitfac = read_fixed(line, 185,  4, upcase=True)   # IF Y/N LIMIT FAC RM50
        crtcmv   = read_fixed(line, 192, 15, upcase=True)   # CURRENT CMV OF TOTAL
        crcmvma  = read_fixed(line, 210, 15, upcase=True)   # CCT CMV OF MA TOTAL
        dt = mdy(mmdate, dddate, yydate)
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'TYREVIEW': tyreview, 'TYCRR': tycrr, 'REVNOC': revnoc,
            'EXPSIN': expsin, 'BNMCODE': bnmcode,
            'OTAS': otas, 'COBF': cobf, 'DCHEQS': dcheqs, 'CCRIS': ccris,
            'LIMITFAC': limitfac, 'OTAS1': otas1, 'COBF1': cobf1,
            'DCHEQS1': dcheqs1, 'CCRIS1': ccris1, 'YNLITFAC': ynlitfac,
            'CRTCMV': crtcmv, 'CRCMVMA': crcmvma,
            'DATE': dt,
        })
    return records


def parse_eln7(filepath: Path) -> list[dict]:
    """Parse ELDSRV7 -> ELN7."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)   # REVIEW NO
        if not newid or not reviewno:
            continue
        finyrend = read_fixed(line,  36, 10, upcase=True)   # FINANCIAL YEAR ENDED
        aumd     = read_fixed(line,  49,  1, upcase=True)   # AUD(A)/UN(U)/MNG/DRF
        collprd  = read_fixed(line,  53, 15, upcase=True)   # COLLECTION PERIOD
        salesgr  = read_fixed(line,  71, 15, upcase=True)   # SALES GROWTH
        netpro   = read_fixed(line,  89, 15, upcase=True)   # NET PROFITMARGIN
        leverage = read_fixed(line, 107, 15, upcase=True)   # LEVERAGE
        actest   = read_fixed(line, 125, 15, upcase=True)   # ACID TEST ROTIO
        intcov   = read_fixed(line, 143, 15, upcase=True)   # INTEREST COVERAGE
        turrnover = read_fixed(line, 161, 15, upcase=True)  # TURRNOVER
        pretaxpro = read_fixed(line, 179, 15, upcase=True)  # PRE-TAX NET PROFIT
        networth1 = read_fixed(line, 197, 15, upcase=True)  # NETWORTH
        curasst  = read_fixed(line, 215, 15, upcase=True)   # CURRENT ASSET
        curlia   = read_fixed(line, 233, 15, upcase=True)   # CURRENT LIABILITIES
        longlia  = read_fixed(line, 251, 15, upcase=True)   # LONG TERM LIABILITIES
        netprobf = read_fixed(line, 269, 15, upcase=True)   # NET PROFIT BEFORE TAX
        tolintfin = read_fixed(line, 287, 15, upcase=True)  # TOTALINTEREST/FINANCE
        stock1   = read_fixed(line, 305, 15, upcase=True)   # STOCK
        tradedeb = read_fixed(line, 323, 15, upcase=True)   # TRADE DEBTORS
        interloan = read_fixed(line, 341, 15, upcase=True)  # INTER COMPANY LOANS
        loandic  = read_fixed(line, 359, 15, upcase=True)   # LOANS TO DIRECTOR
        netope   = read_fixed(line, 377, 15, upcase=True)   # NET OPERATING CASHFLOW
        deflia   = read_fixed(line, 395, 15, upcase=True)   # DEFFERRED LIABILITY
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'FINYREND': finyrend, 'AUMD': aumd, 'COLLPRD': collprd,
            'SALESGR': salesgr, 'NETPRO': netpro, 'LEVERAGE': leverage,
            'ACTEST': actest, 'INTCOV': intcov, 'TURRNOVER': turrnover,
            'PRETAXPRO': pretaxpro, 'NETWORTH1': networth1,
            'CURASST': curasst, 'CURLIA': curlia, 'LONGLIA': longlia,
            'NETPROBF': netprobf, 'TOLINTFIN': tolintfin, 'STOCK1': stock1,
            'TRADEDEB': tradedeb, 'INTERLOAN': interloan, 'LOANDIC': loandic,
            'NETOPE': netope, 'DEFLIA': deflia,
        })
    return records


def parse_eln8(filepath: Path) -> list[dict]:
    """Parse ELDSRV8 -> ELN8."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)   # REVIEW NO
        if not newid or not reviewno:
            continue
        extolloan = read_fixed(line,  36, 15, upcase=True)  # EXACT VAL TOTAL LOAN
        exann    = read_fixed(line,   54, 15, upcase=True)  # EXACT VAL ANNUAL GROSS
        exdebtser = read_fixed(line,  72, 15, upcase=True)  # EXACT VAL DEBT SERVICE
        rangedebt = read_fixed(line,  90, 40, upcase=True)  # RANGE DEBT SERVICE
        scodebt  = read_fixed(line,  133,  1, upcase=True)  # SCORE DEBT SERVICE RT
        weightdeb = read_fixed(line, 137,  2, upcase=True)  # WEIGHT DEBT SERVICE
        tolsco   = read_fixed(line,  142,  3, upcase=True)  # TOTAL SCORE DEBT SERV
        exvyrgcon = read_fixed(line, 148,  4, upcase=True)  # EXACT YR GOOD CONDUCT
        ranyrgcon = read_fixed(line, 155, 30, upcase=True)  # RANGE YR GOOD CONDUCT
        scoyrgcon = read_fixed(line, 188,  1, upcase=True)  # SCORE YR GOOD CONDUCT
        weiyrgcon = read_fixed(line, 192,  2, upcase=True)  # WEIGHT YR GD CONDUCT
        tolyrgcon = read_fixed(line, 197,  3, upcase=True)  # TOTAL YR GD CONDUCT
        exgconre  = read_fixed(line, 203,  4, upcase=True)  # EXACT YR GDCN RELATED
        rangconre = read_fixed(line, 210, 30, upcase=True)  # RANGE YR GDCN RELATED
        scogconre = read_fixed(line, 243,  1, upcase=True)  # SCORE YR GDCN RELATED
        weigconre = read_fixed(line, 247,  2, upcase=True)  # WEIGHT GDCN RELATED
        tolgconre = read_fixed(line, 252,  3, upcase=True)  # TOTAL GDCN RELATED
        tolasset  = read_fixed(line, 258, 15, upcase=True)  # TOTAL ASSET
        tollia    = read_fixed(line, 276, 15, upcase=True)  # TOTAL LIABILITIES
        rangnet   = read_fixed(line, 294, 40, upcase=True)  # RANGE FOR NET WORTH
        sconet    = read_fixed(line, 337,  1, upcase=True)  # SCORE FOR NET WORTH
        weinet    = read_fixed(line, 341,  2, upcase=True)  # WEIGHT FOR NET WORTH
        tolscnet  = read_fixed(line, 346,  3, upcase=True)  # TOTAL SCORE NET WORTH
        rangage   = read_fixed(line, 352, 30, upcase=True)  # RANGE FOR AGE 1YEAR
        scoage    = read_fixed(line, 385,  1, upcase=True)  # SCORE FOR RANGE 1YEAR
        weiage    = read_fixed(line, 389,  2, upcase=True)  # WEIGHT FOR AGE 1YEAR
        tolscage  = read_fixed(line, 394,  3, upcase=True)  # TOTAL SCORE AGE 1YEAR
        tolcrelmt = read_fixed(line, 400, 15, upcase=True)  # TOTAL CREDIT LIMIT
        tolsec    = read_fixed(line, 418, 15, upcase=True)  # TOTAL SECURITIES VALUE
        ma        = read_fixed(line, 436, 15, upcase=True)  # M/A
        rantysec  = read_fixed(line, 454, 200, upcase=True) # RANGE TYPE SECURITY
        scotysec  = read_fixed(line, 657,  1, upcase=True)  # SCORE TYPE SECURITY
        weitysec  = read_fixed(line, 661,  2, upcase=True)  # WEIGHT TYPE SECURUTY
        tolscty   = read_fixed(line, 666,  3, upcase=True)  # TOTAL SCORE TYP SEC
        ranmaradv = read_fixed(line, 672, 25, upcase=True)  # RANGE MARGIN ADVANCE
        scomaradv = read_fixed(line, 700,  1, upcase=True)  # SCORE MARGIN ADVANCE
        weimaradv = read_fixed(line, 704,  2, upcase=True)  # WEIGHT MARGIN ADVANCE
        tolscmar  = read_fixed(line, 709,  3, upcase=True)  # TOTAL SC MARGIN ADV
        tolsctlse = read_fixed(line, 715,  3, upcase=True)  # TOTAL SC FOR TOL SEC
        tolsccrr  = read_fixed(line, 721,  3, upcase=True)  # TOTAL SC OVERALL CRR
        oricrr    = read_fixed(line, 727,  3, upcase=True)  # ORIGINAL CRR SCORE
        extcrr    = read_fixed(line, 733,  3, upcase=True)  # EXACT CRR SCORE
        grdcrr    = read_fixed(line, 739,  1, upcase=True)  # GRADE CRR SCORE
        oriccr    = read_fixed(line, 743,  3, upcase=True)  # ORIGINAL CCR SCORE
        extccr    = read_fixed(line, 749,  3, upcase=True)  # EXACT CCR SCORE
        grdccr    = read_fixed(line, 755,  1, upcase=True)  # GRADE CCR SCORE
        grdsecrat = read_fixed(line, 759,  1, upcase=True)  # GRADE SECURITY RATING
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'EXTOLLOAN': extolloan, 'EXANN': exann, 'EXDEBTSER': exdebtser,
            'RANGEDEBT': rangedebt, 'SCODEBT': scodebt, 'WEIGHTDEB': weightdeb,
            'TOLSCO': tolsco, 'EXVYRGCON': exvyrgcon, 'RANYRGCON': ranyrgcon,
            'SCOYRGCON': scoyrgcon, 'WEIYRGCON': weiyrgcon, 'TOLYRGCON': tolyrgcon,
            'EXGCONRE': exgconre, 'RANGCONRE': rangconre, 'SCOGCONRE': scogconre,
            'WEIGCONRE': weigconre, 'TOLGCONRE': tolgconre,
            'TOLASSET': tolasset, 'TOLLIA': tollia, 'RANGNET': rangnet,
            'SCONET': sconet, 'WEINET': weinet, 'TOLSCNET': tolscnet,
            'RANGAGE': rangage, 'SCOAGE': scoage, 'WEIAGE': weiage,
            'TOLSCAGE': tolscage, 'TOLCRELMT': tolcrelmt, 'TOLSEC': tolsec,
            'MA': ma, 'RANTYSEC': rantysec, 'SCOTYSEC': scotysec,
            'WEITYSEC': weitysec, 'TOLSCTY': tolscty, 'RANMARADV': ranmaradv,
            'SCOMARADV': scomaradv, 'WEIMARADV': weimaradv, 'TOLSCMAR': tolscmar,
            'TOLSCTLSE': tolsctlse, 'TOLSCCRR': tolsccrr,
            'ORICRR': oricrr, 'EXTCRR': extcrr, 'GRDCRR': grdcrr,
            'ORICCR': oriccr, 'EXTCCR': extccr, 'GRDCCR': grdccr,
            'GRDSECRAT': grdsecrat,
        })
    return records


def parse_eln9(filepath: Path) -> list[dict]:
    """Parse ELDSRV9 -> ELN9."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)   # REVIEW NO
        if not newid or not reviewno:
            continue
        ccrisbor   = read_fixed(line,  36, 80, upcase=True)  # CCRIS BORROWER ACCT
        ccrisrel   = read_fixed(line, 119, 80, upcase=True)  # CCRIS RELATED ACCT
        tccrisbor  = read_fixed(line, 202,  3, upcase=True)  # TOL CCRIS BORROWER AC
        tccrisrel  = read_fixed(line, 208,  3, upcase=True)  # TOL CCRIS RELATED ACCT
        ranincome  = read_fixed(line, 214, 20, upcase=True)  # RANGE FOR INCOME LVL
        scoincome  = read_fixed(line, 237,  1, upcase=True)  # SCORE FOR INCOME LVL
        weiincome  = read_fixed(line, 241,  2, upcase=True)  # WEIGHT FOR INCOME LVL
        tolincome  = read_fixed(line, 246,  3, upcase=True)  # TOTAL SCORE INCOME
        ranmaradv1 = read_fixed(line, 252, 30, upcase=True)  # RANGE MARGIN ADVANCE
        scomaradv1 = read_fixed(line, 285,  1, upcase=True)  # SCORE MARGIN ADVANCE
        weimaradv1 = read_fixed(line, 289,  2, upcase=True)  # WEIGHT MARGIN ADVANCE
        tolmaradv1 = read_fixed(line, 294,  3, upcase=True)  # TOTAL SC MARGIN ADVANCE
        ranmarad1  = read_fixed(line, 300, 30, upcase=True)  # RANGE MARGIN ADVANCE1
        scomarad1  = read_fixed(line, 333,  1, upcase=True)  # SCORE MARGIN ADVANCE1
        weimarad1  = read_fixed(line, 337,  2, upcase=True)  # WEIGHT MARGIN ADVANCE1
        tolmarad1  = read_fixed(line, 342,  3, upcase=True)  # TOTAL SC MARGIN ADVANCE1
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'CCRISBOR': ccrisbor, 'CCRISREL': ccrisrel,
            'TCCRISBOR': tccrisbor, 'TCCRISREL': tccrisrel,
            'RANINCOME': ranincome, 'SCOINCOME': scoincome,
            'WEIINCOME': weiincome, 'TOLINCOME': tolincome,
            'RANMARADV1': ranmaradv1, 'SCOMARADV1': scomaradv1,
            'WEIMARADV1': weimaradv1, 'TOLMARADV1': tolmaradv1,
            'RANMARAD1': ranmarad1, 'SCOMARAD1': scomarad1,
            'WEIMARAD1': weimarad1, 'TOLMARAD1': tolmarad1,
        })
    return records


def parse_eln10(filepath: Path) -> list[dict]:
    """Parse ELDSRV10 -> ELN10."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)   # REVIEW NO
        if not newid or not reviewno:
            continue
        extbus    = read_fixed(line,  36,  4, upcase=True)  # EXACT VAL IN BUSINESS
        ranbus    = read_fixed(line,  43, 30, upcase=True)  # RANGE VAL IN BUSINESS
        scobus    = read_fixed(line,  76,  1, upcase=True)  # SCORE VAL IN BUSINESS
        weibus    = read_fixed(line,  80,  2, upcase=True)  # WEIGHT VA IN BUSINESS
        tolbus    = read_fixed(line,  85,  3, upcase=True)  # TOTAL VAL IN BUSINESS
        ranmag    = read_fixed(line,  91, 30, upcase=True)  # RANGE MANAGEMENT
        scomag    = read_fixed(line, 124,  1, upcase=True)  # SCORE MANAGEMENT
        weimag    = read_fixed(line, 128,  2, upcase=True)  # WEIGHT MANAGEMENT
        tolmag    = read_fixed(line, 133,  3, upcase=True)  # TOTAL SCORE MANAGEMENT
        extgcbor  = read_fixed(line, 139,  4, upcase=True)  # EXACT GD CON-BORROWER
        rangcbor  = read_fixed(line, 146, 40, upcase=True)  # RANGE GD CON-BORROWER
        scodcbor  = read_fixed(line, 189,  2, upcase=True)  # SCORE GD CON-BORROWER
        weigcbor  = read_fixed(line, 194,  2, upcase=True)  # WEIGHT GD CO-BORROWER
        tolgcbor  = read_fixed(line, 199,  3, upcase=True)  # TOTAL GD CON-BORROWER
        extgcrel  = read_fixed(line, 205,  4, upcase=True)  # EXACT GD CON-RELATED
        rangcrel  = read_fixed(line, 212, 40, upcase=True)  # RANGE GD CON-RELATED
        scogcrel  = read_fixed(line, 255,  2, upcase=True)  # SCORE GD CON-RELATED
        weigcrel  = read_fixed(line, 260,  2, upcase=True)  # WEIGHT GD CO-RELATED
        tolgcrel  = read_fixed(line, 265,  3, upcase=True)  # TOTAL SCORE-RELATED
        rantype   = read_fixed(line, 271, 10, upcase=True)  # RANGE FR TYPE INDUSTRY
        scotype   = read_fixed(line, 284,  1, upcase=True)  # SCORE FR TYPE INDUSTRY
        weitype   = read_fixed(line, 288,  2, upcase=True)  # WEIGHT FR TYPE INDUSTRY
        toltype   = read_fixed(line, 293,  3, upcase=True)  # TOTAL FR TYPE INDUSTRY
        tolcrelmt1 = read_fixed(line, 299, 15, upcase=True) # TOTAL CREDIT LIMIT
        tolsecval = read_fixed(line, 317, 15, upcase=True)  # TOTAL SECURITIES VALUE
        ma1       = read_fixed(line, 335, 15, upcase=True)  # M/A
        rantysec1 = read_fixed(line, 353, 300, upcase=True) # RANGE TYPE SECURITY
        scotysec1 = read_fixed(line, 656,  1, upcase=True)  # SCORE TYPE SECURITY
        weitysec1 = read_fixed(line, 660,  2, upcase=True)  # WEIGHT TYPE SECURITY
        toltysec  = read_fixed(line, 665,  3, upcase=True)  # TOTAL TYPE SECURITY
        ranmaradv2 = read_fixed(line, 671, 25, upcase=True) # RANGE MARGIN ADVANCE
        scomaradv2 = read_fixed(line, 699,  1, upcase=True) # SCORE MARGIN ADVANCE
        weimaradv2 = read_fixed(line, 703,  2, upcase=True) # WEIGHT MARGIN ADVANCE
        tolmaradv2 = read_fixed(line, 708,  3, upcase=True) # TOTAL MARGIN ADVANCE
        tolsec1   = read_fixed(line, 714,  3, upcase=True)  # TOTAL SECURITY SCORE
        extturn   = read_fixed(line, 720,  5, upcase=True)  # EXACT TURNOVER GROWTH
        ranturn   = read_fixed(line, 728, 30, upcase=True)  # RANGE TURNOVER GROWTH
        scoturn   = read_fixed(line, 761,  1, upcase=True)  # SCORE TURNOVER GROWTH
        weiturn   = read_fixed(line, 765,  2, upcase=True)  # WEIGHT TURNOVER GROWTH
        tolturn   = read_fixed(line, 770,  3, upcase=True)  # TOTAL TURNOVER GROWTH
        extnpro   = read_fixed(line, 776,  5, upcase=True)  # EXACT NET PROFIT
        rannpro   = read_fixed(line, 784, 30, upcase=True)  # RANGE NET PROFIT
        sconpro   = read_fixed(line, 817,  1, upcase=True)  # SCORE NET PROFIT
        weinpro   = read_fixed(line, 821,  2, upcase=True)  # WEIGHT NET PROFIT
        tolnpro   = read_fixed(line, 826,  3, upcase=True)  # TOTAL NET PROFIT
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'EXTBUS': extbus, 'RANBUS': ranbus, 'SCOBUS': scobus,
            'WEIBUS': weibus, 'TOLBUS': tolbus, 'RANMAG': ranmag,
            'SCOMAG': scomag, 'WEIMAG': weimag, 'TOLMAG': tolmag,
            'EXTGCBOR': extgcbor, 'RANGCBOR': rangcbor, 'SCODCBOR': scodcbor,
            'WEIGCBOR': weigcbor, 'TOLGCBOR': tolgcbor,
            'EXTGCREL': extgcrel, 'RANGCREL': rangcrel, 'SCOGCREL': scogcrel,
            'WEIGCREL': weigcrel, 'TOLGCREL': tolgcrel,
            'RANTYPE': rantype, 'SCOTYPE': scotype, 'WEITYPE': weitype,
            'TOLTYPE': toltype, 'TOLCRELMT1': tolcrelmt1, 'TOLSECVAL': tolsecval,
            'MA1': ma1, 'RANTYSEC1': rantysec1, 'SCOTYSEC1': scotysec1,
            'WEITYSEC1': weitysec1, 'TOLTYSEC': toltysec,
            'RANMARADV2': ranmaradv2, 'SCOMARADV2': scomaradv2,
            'WEIMARADV2': weimaradv2, 'TOLMARADV2': tolmaradv2,
            'TOLSEC1': tolsec1, 'EXTTURN': extturn, 'RANTURN': ranturn,
            'SCOTURN': scoturn, 'WEITURN': weiturn, 'TOLTURN': tolturn,
            'EXTNPRO': extnpro, 'RANNPRO': rannpro, 'SCONPRO': sconpro,
            'WEINPRO': weinpro, 'TOLNPRO': tolnpro,
        })
    return records


def parse_eln11(filepath: Path) -> list[dict]:
    """Parse ELDSRV11 -> ELN11."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)   # REVIEW NO
        if not newid or not reviewno:
            continue
        extacd   = read_fixed(line,  36,  5, upcase=True)  # EXACT ACID TEST RATIO
        ranacd   = read_fixed(line,  44, 15, upcase=True)  # RANGE ACID TEST RATIO
        scoacd   = read_fixed(line,  62,  1, upcase=True)  # SCORE ACID TEST RATIO
        weiacd   = read_fixed(line,  66,  2, upcase=True)  # WEIGHT ACID TEST RATIO
        tolacd   = read_fixed(line,  71,  3, upcase=True)  # TOTAL ACID TEST RATIO
        extlev   = read_fixed(line,  77,  5, upcase=True)  # EXACT LEVERAGE RATIO
        ranlev   = read_fixed(line,  85, 30, upcase=True)  # RANGE LEVERAGE RATIO
        scolev   = read_fixed(line, 118,  1, upcase=True)  # SCORE LEVERAGE RATIO
        weilev   = read_fixed(line, 122,  2, upcase=True)  # WEIGHT LEVERAGE RATIO
        tollev   = read_fixed(line, 127,  3, upcase=True)  # TOTAL LEVERAGE RATIO
        extint   = read_fixed(line, 133,  5, upcase=True)  # EXACT INTEREST COV
        ranint   = read_fixed(line, 141, 30, upcase=True)  # RANGE INTEREST COV
        scoint   = read_fixed(line, 174,  1, upcase=True)  # SCORE INTEREST COV
        weiint   = read_fixed(line, 178,  2, upcase=True)  # WEIGHT INTEREST COV
        tolint   = read_fixed(line, 183,  3, upcase=True)  # TOTAL INTEREST COV
        extave   = read_fixed(line, 189,  5, upcase=True)  # EXACT AVERAGE COLL
        ranave   = read_fixed(line, 197, 30, upcase=True)  # RANGE AVERAGE COLL
        scoave   = read_fixed(line, 230,  1, upcase=True)  # SCORE AVERAGE COLL
        weiave   = read_fixed(line, 234,  2, upcase=True)  # WEIGHT AVERAGE COLL
        tolave   = read_fixed(line, 239,  3, upcase=True)  # TOTAL AVERAGE COLL
        rancurr  = read_fixed(line, 245, 80, upcase=True)  # RANGE CURRENT CASHFLOW
        scocurr  = read_fixed(line, 328,  1, upcase=True)  # SCORE CURRENT CASHFLOW
        weicurr  = read_fixed(line, 332,  2, upcase=True)  # WEIGHT CURRENT CASHFLOW
        tolcurr  = read_fixed(line, 337,  3, upcase=True)  # TOTAL CURRENT CASHFLOW
        ranbor   = read_fixed(line, 343, 80, upcase=True)  # RANGE CCRIS-BORROWER
        scobor   = read_fixed(line, 426,  3, upcase=True)  # SCORE CCRIS-BORROWER
        ranrel   = read_fixed(line, 432, 80, upcase=True)  # RANGE CCRIS-RELATED
        scorel   = read_fixed(line, 515,  3, upcase=True)  # SCORE CCRIS-RELATED
        deptcrr  = read_fixed(line, 521, 300, upcase=True) # DEMERIT POINT CRR
        scodept  = read_fixed(line, 824,  3, upcase=True)  # SCORE DEMERIT POINT
        tolcrr   = read_fixed(line, 830,  3, upcase=True)  # TOTAL SCORE CRR
        oridecrr = read_fixed(line, 836,  3, upcase=True)  # ORIGINAL DENOMINATOR
        extcrr2  = read_fixed(line, 842,  3, upcase=True)  # EXACT VALUE CRR
        grdcrr2  = read_fixed(line, 848,  1, upcase=True)  # GRADE VALUE CRR
        orideccr = read_fixed(line, 852,  3, upcase=True)  # ORIGINAL DEN CCR
        extccr2  = read_fixed(line, 858,  3, upcase=True)  # EXACT VALUE CCR
        grdccr2  = read_fixed(line, 864,  1, upcase=True)  # GRADE VALUE CCR
        grdsec   = read_fixed(line, 868,  1, upcase=True)  # GRADE FR SECURITY RAT
        ranmarad  = read_fixed(line, 872, 30, upcase=True) # RANGE MARGIN ADVANCE1
        scomarad  = read_fixed(line, 905,  1, upcase=True) # SCORE MARGIN ADVANCE1
        weimarad  = read_fixed(line, 909,  2, upcase=True) # WEIGHT MARGIN ADVANCE1
        tolmarad  = read_fixed(line, 914,  3, upcase=True) # TOTAL MARGIN ADVANCE1
        ranmarad2 = read_fixed(line, 920, 30, upcase=True) # RANGE MARGIN ADVANCE2
        scomarad2 = read_fixed(line, 953,  1, upcase=True) # SCORE MARGIN ADVANCE2
        weimarad2 = read_fixed(line, 957,  2, upcase=True) # WEIGHT MARGIN ADVANCE2
        tolmarad2 = read_fixed(line, 962,  3, upcase=True) # TOTAL MARGIN ADVANCE2
        crrind    = read_fixed(line, 968, 15, upcase=True) # CRR INDICATOR
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'EXTACD': extacd, 'RANACD': ranacd, 'SCOACD': scoacd,
            'WEIACD': weiacd, 'TOLACD': tolacd,
            'EXTLEV': extlev, 'RANLEV': ranlev, 'SCOLEV': scolev,
            'WEILEV': weilev, 'TOLLEV': tollev,
            'EXTINT': extint, 'RANINT': ranint, 'SCOINT': scoint,
            'WEIINT': weiint, 'TOLINT': tolint,
            'EXTAVE': extave, 'RANAVE': ranave, 'SCOAVE': scoave,
            'WEIAVE': weiave, 'TOLAVE': tolave,
            'RANCURR': rancurr, 'SCOCURR': scocurr, 'WEICURR': weicurr,
            'TOLCURR': tolcurr, 'RANBOR': ranbor, 'SCOBOR': scobor,
            'RANREL': ranrel, 'SCOREL': scorel, 'DEPTCRR': deptcrr,
            'SCODEPT': scodept, 'TOLCRR': tolcrr, 'ORIDECRR': oridecrr,
            'EXTCRR2': extcrr2, 'GRDCRR2': grdcrr2, 'ORIDECCR': orideccr,
            'EXTCCR2': extccr2, 'GRDCCR2': grdccr2, 'GRDSEC': grdsec,
            'RANMARAD': ranmarad, 'SCOMARAD': scomarad, 'WEIMARAD': weimarad,
            'TOLMARAD': tolmarad, 'RANMARAD2': ranmarad2, 'SCOMARAD2': scomarad2,
            'WEIMARAD2': weimarad2, 'TOLMARAD2': tolmarad2, 'CRRIND': crrind,
        })
    return records


def parse_eln12(filepath: Path) -> list[dict]:
    """Parse ELDSRV12 -> ELN12."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,  1, 12, upcase=True)
        reviewno = read_fixed(line, 16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        acctnod  = read_fixed(line, 36, 20, upcase=True)
        noteno   = read_fixed(line, 59,  5, upcase=True)
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'ACCTNOD': acctnod, 'NOTENO': noteno,
        })
    return records


def parse_eln13(filepath: Path) -> list[dict]:
    """Parse ELDSRV13 -> ELN13."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,  1, 12, upcase=True)
        reviewno = read_fixed(line, 16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        relname  = read_fixed(line, 36, 60, upcase=True)   # NAME OF RELATIVE
        relate   = read_fixed(line, 99, 60, upcase=True)   # RELATIONSHIP WITH STAFF
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'RELNAME': relname, 'RELATE': relate,
        })
    return records


def parse_eln14(filepath: Path) -> list[dict]:
    """Parse ELDSRV14 -> ELN14."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        custnme  = read_fixed(line,  36, 60, upcase=True)  # APPL DETAIL-CUST NAME
        occpat   = read_fixed(line,  99, 60, upcase=True)  # APPL DETAIL-BUSINESS/OCCP
        identy   = read_fixed(line, 162, 20, upcase=True)  # APPL DETAIL-IC/REG NO/
        custid   = read_fixed(line, 185, 10, upcase=True)  # APPL DETAIL-CISNO
        dobdd    = read_num(line,    198,  2)               # APPL DETAIL-DATE OF BIRTH DD
        dobmm    = read_num(line,    201,  2)               # APPL DETAIL-DATE OF BIRTH MM
        dobyy    = read_num(line,    204,  4)               # APPL DETAIL-DATE OF BIRTH YY
        yrcomt   = read_fixed(line, 211,  4, upcase=True)  # APPL DETAIL-YEARS OF COMMT
        dobdec = mdy(dobmm, dobdd, dobyy)
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'CUSTNME': custnme, 'OCCPAT': occpat, 'IDENTY': identy,
            'CUSTID': custid, 'YRCOMT': yrcomt, 'DOBDEC': dobdec,
        })
    return records


def parse_eln15(filepath: Path) -> list[dict]:
    """Parse ELDSRV15 -> ELN15."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        guarantr = read_fixed(line,  36, 60, upcase=True)  # NAME
        guarages = read_fixed(line,  99,  3, upcase=True)  # AGE
        guarnetw = read_fixed(line, 105, 16, upcase=True)  # NETWORTH(RM)
        guaridno = read_fixed(line, 124, 20, upcase=True)  # IC/REG NO/INCORP NO
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'GUARANTR': guarantr, 'GUARAGES': guarages,
            'GUARNETW': guarnetw, 'GUARIDNO': guaridno,
        })
    return records


def parse_eln16(filepath: Path) -> list[dict]:
    """Parse ELDSRV16 -> ELN16."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        prptdes  = read_fixed(line,  36, 100, upcase=True) # DESCRIPTION OF PROPERTY
        cmscode  = read_fixed(line, 139,   6, upcase=True) # CMS CODE PROPERTY
        cmvttl   = read_fixed(line, 148,  15, upcase=True) # TOTAL CMV(RM)
        addrb01  = read_fixed(line, 166,  60, upcase=True) # ADDRESS
        addrb02  = read_fixed(line, 229,  40, upcase=True) # HOUSE NO
        addrb03  = read_fixed(line, 272,  40, upcase=True) # BUILDING NAME
        addrb04  = read_fixed(line, 315,  40, upcase=True) # JALAN
        addrb05  = read_fixed(line, 358,  40, upcase=True) # TAMAN
        addrb06  = read_fixed(line, 401,  40, upcase=True) # LOCALITY
        addrb07  = read_fixed(line, 444,  40, upcase=True) # STATECODE
        addrb08  = read_fixed(line, 487,  40, upcase=True) # POSTCODE
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'PRPTDES': prptdes, 'CMSCODE': cmscode, 'CMVTTL': cmvttl,
            'ADDRB01': addrb01, 'ADDRB02': addrb02, 'ADDRB03': addrb03,
            'ADDRB04': addrb04, 'ADDRB05': addrb05, 'ADDRB06': addrb06,
            'ADDRB07': addrb07, 'ADDRB08': addrb08,
        })
    return records


def parse_eln17(filepath: Path) -> list[dict]:
    """Parse ELDSRV17 -> ELN17."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        othsec   = read_fixed(line,  36,  60, upcase=True) # OTHER SECURITY
        accothsc = read_fixed(line,  99,  12, upcase=True) # A/C NO.(IF APPLICABLE)
        amount   = read_fixed(line, 114,  16, upcase=True) # AMOUNT
        tosecure = read_fixed(line, 133, 100, upcase=True) # TO SECURE
        cmscode1 = read_fixed(line, 236,   6, upcase=True) # CMS CODE OTHER SECURITY
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'OTHSEC': othsec, 'ACCOTHSC': accothsc,
            'AMOUNT': amount, 'TOSECURE': tosecure, 'CMSCODE1': cmscode1,
        })
    return records


def parse_eln18(filepath: Path) -> list[dict]:
    """Parse ELDSRV18 -> ELN18."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        aanos    = read_fixed(line,  36, 20, upcase=True)  # A/A NUMBER
        facilt   = read_fixed(line,  59, 100, upcase=True) # FACILITY TYPE
        applmt   = read_fixed(line, 162, 30, upcase=True)  # APPROVED LIMIT
        opelmt   = read_fixed(line, 195, 30, upcase=True)  # OPERATIVE LIMIT
        baldd    = read_num(line,    228,  2)               # BALANCE DATE DD
        balmm    = read_num(line,    231,  2)               # BALANCE DATE MM
        balyy    = read_num(line,    234,  4)               # BALANCE DATE YY
        bal1_1   = read_fixed(line, 241, 14, upcase=True)  # BALANCE1_1
        bal1_2   = read_fixed(line, 258, 14, upcase=True)  # BALANCE1_2
        bal1_3   = read_fixed(line, 275, 14, upcase=True)  # BALANCE1_3
        bal1_4   = read_fixed(line, 292, 14, upcase=True)  # BALANCE1_4
        bal1_5   = read_fixed(line, 309, 14, upcase=True)  # BALANCE1_5
        bal1_6   = read_fixed(line, 326, 14, upcase=True)  # BALANCE1_6
        bal1_7   = read_fixed(line, 343, 14, upcase=True)  # BALANCE1_7
        bal1_8   = read_fixed(line, 360, 14, upcase=True)  # BALANCE1_8
        bal1_9   = read_fixed(line, 377, 14, upcase=True)  # BALANCE1_9
        bal1_10  = read_fixed(line, 394, 14, upcase=True)  # BALANCE1_10
        applmtsub = read_fixed(line, 411, 20, upcase=True) # APPROVED LIMIT(SUB)
        opelmtsub = read_fixed(line, 434, 20, upcase=True) # OPERATIVE LIMIT(SUB)
        baldate = mdy(balmm, baldd, balyy)
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'AANOS': aanos, 'FACILT': facilt, 'APPLMT': applmt, 'OPELMT': opelmt,
            'BAL1_1': bal1_1, 'BAL1_2': bal1_2, 'BAL1_3': bal1_3,
            'BAL1_4': bal1_4, 'BAL1_5': bal1_5, 'BAL1_6': bal1_6,
            'BAL1_7': bal1_7, 'BAL1_8': bal1_8, 'BAL1_9': bal1_9,
            'BAL1_10': bal1_10, 'APPLMTSUB': applmtsub, 'OPELMTSUB': opelmtsub,
            'BALDATE': baldate,
        })
    return records


def parse_eln19(filepath: Path) -> list[dict]:
    """Parse ELDSRV19 -> ELN19."""
    records = []
    if not filepath.exists():
        return records
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    for line in lines[1:]:
        line = line.rstrip('\n')
        newid    = read_fixed(line,   1, 12, upcase=True)
        reviewno = read_fixed(line,  16, 17, upcase=True)
        if not newid or not reviewno:
            continue
        aanot2   = read_fixed(line,  36, 20, upcase=True)  # A/A NUMBER(T2)
        factype  = read_fixed(line,  59, 100, upcase=True) # FACILITY TYPE
        applmt1  = read_fixed(line, 162, 30, upcase=True)  # APPROVED LIMIT (RM)
        opelmt1  = read_fixed(line, 195, 30, upcase=True)  # OPERATIVE LIMIT (RM)
        bal1dd   = read_num(line,    228,  2)               # BALANCE DATE1 DD
        bal1mm   = read_num(line,    231,  2)               # BALANCE DATE1 MM
        bal1yy   = read_num(line,    234,  4)               # BALANCE DATE1 YY
        bal2_1   = read_fixed(line, 241, 14, upcase=True)  # BALANCE2_1
        bal2_2   = read_fixed(line, 258, 14, upcase=True)  # BALANCE2_2
        bal2_3   = read_fixed(line, 275, 14, upcase=True)  # BALANCE2_3
        bal2_4   = read_fixed(line, 292, 14, upcase=True)  # BALANCE2_4
        bal2_5   = read_fixed(line, 309, 14, upcase=True)  # BALANCE2_5
        bal2_6   = read_fixed(line, 326, 14, upcase=True)  # BALANCE2_6
        bal2_7   = read_fixed(line, 343, 14, upcase=True)  # BALANCE2_7
        bal2_8   = read_fixed(line, 360, 14, upcase=True)  # BALANCE2_8
        bal2_9   = read_fixed(line, 377, 14, upcase=True)  # BALANCE2_9
        bal2_10  = read_fixed(line, 394, 14, upcase=True)  # BALANCE2_10
        applmtsu1 = read_fixed(line, 411, 20, upcase=True) # APPROVED LIMIT(SUB)1
        opelmtsu1 = read_fixed(line, 434, 20, upcase=True) # OPERATIVE LIMIT(SUB)1
        baldate1 = mdy(bal1mm, bal1dd, bal1yy)
        records.append({
            'NEWID': newid, 'REVIEWNO': reviewno,
            'AANOT2': aanot2, 'FACTYPE': factype, 'APPLMT1': applmt1,
            'OPELMT1': opelmt1,
            'BAL2_1': bal2_1, 'BAL2_2': bal2_2, 'BAL2_3': bal2_3,
            'BAL2_4': bal2_4, 'BAL2_5': bal2_5, 'BAL2_6': bal2_6,
            'BAL2_7': bal2_7, 'BAL2_8': bal2_8, 'BAL2_9': bal2_9,
            'BAL2_10': bal2_10, 'APPLMTSU1': applmtsu1, 'OPELMTSU1': opelmtsu1,
            'BALDATE1': baldate1,
        })
    return records

# ---------------------------------------------------------------------------
# HELPERS: load previous, save current, merge
# ---------------------------------------------------------------------------

PARSERS = {
    1: parse_eln1,   2: parse_eln2,   3: parse_eln3,
    4: parse_eln4,   5: parse_eln5,   6: parse_eln6,
    7: parse_eln7,   8: parse_eln8,   9: parse_eln9,
    10: parse_eln10, 11: parse_eln11, 12: parse_eln12,
    13: parse_eln13, 14: parse_eln14, 15: parse_eln15,
    16: parse_eln16, 17: parse_eln17, 18: parse_eln18,
    19: parse_eln19,
}

TABLE_DIRS = {
    **{i: (EREV1_DIR, EREVO1_DIR) for i in range(1, 7)},
    **{i: (EREV2_DIR, EREVO2_DIR) for i in range(7, 13)},
    **{i: (EREV3_DIR, EREVO3_DIR) for i in range(13, 20)},
}


def load_previous(table_num: int) -> pl.DataFrame:
    """Load previous period data (EREVOn.ELNn)."""
    _, prev_dir = TABLE_DIRS[table_num]
    path = prev_dir / f'eln{table_num}.parquet'
    if path.exists():
        return pl.read_parquet(path)
    logger.warning(f"Previous period file not found: {path}")
    return pl.DataFrame()


def save_current(df: pl.DataFrame, table_num: int) -> None:
    """Save current period ELNn to EREV dir."""
    cur_dir, _ = TABLE_DIRS[table_num]
    path = cur_dir / f'eln{table_num}.parquet'
    df.write_parquet(path)
    logger.info(f"Saved ELN{table_num}: {path}")


def deduplicate(df: pl.DataFrame) -> pl.DataFrame:
    """Remove exact duplicate rows (NODUPRECS equivalent)."""
    return df.unique()


def sort_by_keys(df: pl.DataFrame) -> pl.DataFrame:
    """Sort by NEWID, REVIEWNO."""
    if 'NEWID' in df.columns and 'REVIEWNO' in df.columns:
        return df.sort(['NEWID', 'REVIEWNO'])
    return df


def merge_by_keys(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
    """
    Full outer join on NEWID+REVIEWNO (equivalent to SAS merge with BY).
    Overlapping non-key columns from right take precedence where left is null.
    """
    if left.is_empty() and right.is_empty():
        return pl.DataFrame()
    if left.is_empty():
        return right
    if right.is_empty():
        return left
    return left.join(right, on=['NEWID', 'REVIEWNO'], how='outer_coalesce')

# ---------------------------------------------------------------------------
# FULL JOIN for tables 14/15, 16/17, 18/19
# (SAS PROC SQL FULL JOIN with NULL fill-in)
# ---------------------------------------------------------------------------

def full_join_pair(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
    """
    Full outer join two tables; when NEWID/REVIEWNO are null, fill from
    NEWID1/REVIEWNO1 (the renamed keys of the right table).
    Equivalent to SAS PROC SQL FULL JOIN ... with NULL substitution.
    """
    if left.is_empty() and right.is_empty():
        return pl.DataFrame()
    if left.is_empty():
        return right
    if right.is_empty():
        return left

    right_renamed = right.rename({'NEWID': 'NEWID1', 'REVIEWNO': 'REVIEWNO1'})
    joined = left.join(right_renamed, left_on=['NEWID', 'REVIEWNO'],
                       right_on=['NEWID1', 'REVIEWNO1'], how='outer')

    # Fill NEWID from NEWID1 when null
    if 'NEWID_right' in joined.columns:
        joined = joined.with_columns(
            pl.when(pl.col('NEWID').is_null())
            .then(pl.col('NEWID_right'))
            .otherwise(pl.col('NEWID'))
            .alias('NEWID')
        ).drop(['NEWID_right'])
    if 'REVIEWNO_right' in joined.columns:
        joined = joined.with_columns(
            pl.when(pl.col('REVIEWNO').is_null())
            .then(pl.col('REVIEWNO_right'))
            .otherwise(pl.col('REVIEWNO'))
            .alias('REVIEWNO')
        ).drop(['REVIEWNO_right'])

    # Drop renamed key columns if they remain
    for col in ['NEWID1', 'REVIEWNO1']:
        if col in joined.columns:
            joined = joined.drop([col])

    return joined

# ---------------------------------------------------------------------------
# MAIN PROCESS
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting EIBWEREN")

    # Derive report date
    ctx = get_reptdate()
    reptdate = ctx['reptdate']
    rdate    = ctx['rdate']
    reptmon  = ctx['reptmon']
    reptyear = ctx['reptyear']
    logger.info(f"REPTDATE={reptdate}, RDATE={rdate}, REPTMON={reptmon}, WK={ctx['nowk']}")

    # Save REPTDATE
    reptdate_df = pl.DataFrame({'REPTDATE': [reptdate], 'WK': [ctx['nowk']]})
    reptdate_df.write_parquet(ELDSRV_OUT_DIR / 'reptdate.parquet')

    # -----------------------------------------------------------------------
    # CHECK INPUT FILE DATES
    # -----------------------------------------------------------------------
    logger.info("Checking input file dates...")
    file_dates = {}
    for i in range(1, 20):
        dt = read_file_date(ELDSRV_FILES[i], i)
        file_dates[i] = dt
        dt_str = dt.strftime('%d/%m/%y') if dt else 'MISSING'
        logger.info(f"ELDSDT{i} = {dt_str}")
        # Equivalent to PROC PRINT
        print(f"ELDSDT{i}: {dt_str}")

    # -----------------------------------------------------------------------
    # %PROCESS MACRO: validate all dates match RDATE
    # -----------------------------------------------------------------------
    all_match = all(
        file_dates.get(i) is not None and
        file_dates[i].strftime('%d/%m/%y') == rdate
        for i in range(1, 20)
    )

    if not all_match:
        # Report which files don't match
        for i in range(1, 20):
            dt = file_dates.get(i)
            dt_str = dt.strftime('%d/%m/%y') if dt else 'MISSING'
            if dt_str != rdate:
                logger.warning(f"THE SAP.PBB.ELDS.BASEL.RV{i:02d}.TEXT NOT DATED {rdate}")
                print(f"THE SAP.PBB.ELDS.BASEL.RV{i:02d}.TEXT NOT DATED {rdate}")
        print("THE JOB IS NOT DONE !!")
        logger.error("Date validation failed. Aborting with code 77.")
        sys.exit(77)

    # -----------------------------------------------------------------------
    # Parse all 19 tables and build current period ELNn
    # -----------------------------------------------------------------------
    logger.info("All input dates validated. Parsing tables...")

    # Tables 1-6 -> EREV1
    for i in range(1, 7):
        logger.info(f"Parsing ELN{i}")
        records = PARSERS[i](ELDSRV_FILES[i])
        df_new = pl.DataFrame(records) if records else pl.DataFrame()
        save_current(df_new, i)

    # Tables 7-12 -> EREV2
    for i in range(7, 13):
        logger.info(f"Parsing ELN{i}")
        records = PARSERS[i](ELDSRV_FILES[i])
        df_new = pl.DataFrame(records) if records else pl.DataFrame()
        save_current(df_new, i)

    # Tables 13-19 -> EREV3
    for i in range(13, 20):
        logger.info(f"Parsing ELN{i}")
        records = PARSERS[i](ELDSRV_FILES[i])
        df_new = pl.DataFrame(records) if records else pl.DataFrame()
        save_current(df_new, i)

    # -----------------------------------------------------------------------
    # Append previous period data and sort (tables 1-6)
    # DATA EREV1.ELN1 = SET EREV1.ELN1 EREVO1.ELN1; (etc.)
    # -----------------------------------------------------------------------
    logger.info("Appending previous period data for tables 1-6")
    for i in range(1, 7):
        cur_dir, _ = TABLE_DIRS[i]
        cur_path = cur_dir / f'eln{i}.parquet'
        df_cur  = pl.read_parquet(cur_path) if cur_path.exists() else pl.DataFrame()
        df_prev = load_previous(i)
        combined = pl.concat([df_cur, df_prev], how='diagonal_relaxed') if not df_prev.is_empty() else df_cur
        combined = sort_by_keys(combined)
        combined.write_parquet(cur_path)

    # -----------------------------------------------------------------------
    # Merge tables 1-6 into EREVPRE
    # -----------------------------------------------------------------------
    logger.info("Merging ELN1-6 into EREVPRE")
    dfs16 = []
    for i in range(1, 7):
        cur_dir, _ = TABLE_DIRS[i]
        p = cur_dir / f'eln{i}.parquet'
        df = deduplicate(pl.read_parquet(p)) if p.exists() else pl.DataFrame()
        df = sort_by_keys(df)
        dfs16.append(df)

    erevpre = dfs16[0]
    for df in dfs16[1:]:
        erevpre = merge_by_keys(erevpre, df)
    erevpre = deduplicate(erevpre)
    erevpre = sort_by_keys(erevpre)
    erevpre.write_parquet(ELDSRV_OUT_DIR / 'erevpre.parquet')
    logger.info(f"Written EREVPRE: {ELDSRV_OUT_DIR / 'erevpre.parquet'}")

    # -----------------------------------------------------------------------
    # Table 7: append previous, sort, save as EMUL7
    # DATA EREV2.ELN7 = SET EREVO2.ELN7 EREV2.ELN7;
    # -----------------------------------------------------------------------
    logger.info("Processing ELN7 -> EMUL7")
    cur7_path = EREV2_DIR / 'eln7.parquet'
    df7_cur  = pl.read_parquet(cur7_path) if cur7_path.exists() else pl.DataFrame()
    df7_prev = load_previous(7)
    df7 = pl.concat([df7_prev, df7_cur], how='diagonal_relaxed') if not df7_prev.is_empty() else df7_cur
    df7 = sort_by_keys(df7)
    df7.write_parquet(cur7_path)
    emul7 = deduplicate(df7)
    emul7 = sort_by_keys(emul7)
    emul7.write_parquet(ELDSRV_OUT_DIR / 'emul7.parquet')
    logger.info(f"Written EMUL7: {ELDSRV_OUT_DIR / 'emul7.parquet'}")

    # -----------------------------------------------------------------------
    # Tables 8-9: append previous, sort, merge -> EREVCRR1
    # -----------------------------------------------------------------------
    logger.info("Processing ELN8-9 -> EREVCRR1")
    for i in [8, 9]:
        cur_dir, _ = TABLE_DIRS[i]
        p = cur_dir / f'eln{i}.parquet'
        df_cur  = pl.read_parquet(p) if p.exists() else pl.DataFrame()
        df_prev = load_previous(i)
        combined = pl.concat([df_prev, df_cur], how='diagonal_relaxed') if not df_prev.is_empty() else df_cur
        combined = sort_by_keys(combined)
        combined.write_parquet(p)

    df8 = deduplicate(pl.read_parquet(EREV2_DIR / 'eln8.parquet')) if (EREV2_DIR / 'eln8.parquet').exists() else pl.DataFrame()
    df9 = deduplicate(pl.read_parquet(EREV2_DIR / 'eln9.parquet')) if (EREV2_DIR / 'eln9.parquet').exists() else pl.DataFrame()
    df8 = sort_by_keys(df8)
    df9 = sort_by_keys(df9)
    erevcrr1 = merge_by_keys(df8, df9)
    erevcrr1 = deduplicate(erevcrr1)
    erevcrr1 = sort_by_keys(erevcrr1)
    erevcrr1.write_parquet(ELDSRV_OUT_DIR / 'erevcrr1.parquet')
    logger.info(f"Written EREVCRR1: {ELDSRV_OUT_DIR / 'erevcrr1.parquet'}")

    # -----------------------------------------------------------------------
    # Tables 10-11: append previous, sort, merge -> EREVCRR2
    # DATA EREV2.ELN10 = SET EREV2.ELN10 EREVO2.ELN10; (note order)
    # -----------------------------------------------------------------------
    logger.info("Processing ELN10-11 -> EREVCRR2")
    for i in [10, 11]:
        cur_dir, _ = TABLE_DIRS[i]
        p = cur_dir / f'eln{i}.parquet'
        df_cur  = pl.read_parquet(p) if p.exists() else pl.DataFrame()
        df_prev = load_previous(i)
        combined = pl.concat([df_cur, df_prev], how='diagonal_relaxed') if not df_prev.is_empty() else df_cur
        combined = sort_by_keys(combined)
        combined.write_parquet(p)

    df10 = deduplicate(pl.read_parquet(EREV2_DIR / 'eln10.parquet')) if (EREV2_DIR / 'eln10.parquet').exists() else pl.DataFrame()
    df11 = deduplicate(pl.read_parquet(EREV2_DIR / 'eln11.parquet')) if (EREV2_DIR / 'eln11.parquet').exists() else pl.DataFrame()
    df10 = sort_by_keys(df10)
    df11 = sort_by_keys(df11)
    erevcrr2 = merge_by_keys(df10, df11)
    erevcrr2 = deduplicate(erevcrr2)
    erevcrr2 = sort_by_keys(erevcrr2)
    erevcrr2.write_parquet(ELDSRV_OUT_DIR / 'erevcrr2.parquet')
    logger.info(f"Written EREVCRR2: {ELDSRV_OUT_DIR / 'erevcrr2.parquet'}")

    # -----------------------------------------------------------------------
    # Table 12: append previous, sort, save as EMUL12
    # DATA EREV2.ELN12 = SET EREV2.ELN12 EREVO2.ELN12;
    # -----------------------------------------------------------------------
    logger.info("Processing ELN12 -> EMUL12")
    cur12_path = EREV2_DIR / 'eln12.parquet'
    df12_cur  = pl.read_parquet(cur12_path) if cur12_path.exists() else pl.DataFrame()
    df12_prev = load_previous(12)
    df12 = pl.concat([df12_cur, df12_prev], how='diagonal_relaxed') if not df12_prev.is_empty() else df12_cur
    df12 = sort_by_keys(df12)
    df12.write_parquet(cur12_path)
    emul12 = deduplicate(df12)
    emul12 = sort_by_keys(emul12)
    emul12.write_parquet(ELDSRV_OUT_DIR / 'emul12.parquet')
    logger.info(f"Written EMUL12: {ELDSRV_OUT_DIR / 'emul12.parquet'}")

    # -----------------------------------------------------------------------
    # Table 13: append previous, sort, save as EREV13
    # DATA EREV3.ELN13 = SET EREV3.ELN13 EREVO3.ELN13;
    # -----------------------------------------------------------------------
    logger.info("Processing ELN13 -> EREV13")
    cur13_path = EREV3_DIR / 'eln13.parquet'
    df13_cur  = pl.read_parquet(cur13_path) if cur13_path.exists() else pl.DataFrame()
    df13_prev = load_previous(13)
    df13 = pl.concat([df13_cur, df13_prev], how='diagonal_relaxed') if not df13_prev.is_empty() else df13_cur
    df13 = sort_by_keys(df13)
    df13.write_parquet(cur13_path)
    erev13 = deduplicate(df13)
    erev13 = sort_by_keys(erev13)
    erev13.write_parquet(ELDSRV_OUT_DIR / 'erev13.parquet')
    logger.info(f"Written EREV13: {ELDSRV_OUT_DIR / 'erev13.parquet'}")

    # -----------------------------------------------------------------------
    # Tables 14-15: append previous, full join -> EMUL1415
    # PROC SORT DATA=EREV3.ELN14 NODUPRECS; PROC SORT DATA=EREV3.ELN15 NODUPRECS;
    # DATA EREV3.ELN15(RENAME=(NEWID=NEWID1 REVIEWNO=REVIEWNO1));
    # PROC SQL FULL JOIN ON A.NEWID=B.NEWID1 AND A.REVIEWNO=B.REVIEWNO1;
    # -----------------------------------------------------------------------
    logger.info("Processing ELN14-15 -> EMUL1415")
    for i in [14, 15]:
        cur_dir, _ = TABLE_DIRS[i]
        p = cur_dir / f'eln{i}.parquet'
        df_cur  = pl.read_parquet(p) if p.exists() else pl.DataFrame()
        df_prev = load_previous(i)
        combined = pl.concat([df_cur, df_prev], how='diagonal_relaxed') if not df_prev.is_empty() else df_cur
        combined.write_parquet(p)

    df14 = deduplicate(pl.read_parquet(EREV3_DIR / 'eln14.parquet')) if (EREV3_DIR / 'eln14.parquet').exists() else pl.DataFrame()
    df15 = deduplicate(pl.read_parquet(EREV3_DIR / 'eln15.parquet')) if (EREV3_DIR / 'eln15.parquet').exists() else pl.DataFrame()
    df14 = sort_by_keys(df14)
    df15 = sort_by_keys(df15)
    emul1415 = full_join_pair(df14, df15)
    emul1415 = deduplicate(emul1415)
    emul1415 = sort_by_keys(emul1415)
    emul1415.write_parquet(ELDSRV_OUT_DIR / 'emul1415.parquet')
    logger.info(f"Written EMUL1415: {ELDSRV_OUT_DIR / 'emul1415.parquet'}")

    # -----------------------------------------------------------------------
    # Tables 16-17: append previous, full join -> EMUL1617
    # -----------------------------------------------------------------------
    logger.info("Processing ELN16-17 -> EMUL1617")
    for i in [16, 17]:
        cur_dir, _ = TABLE_DIRS[i]
        p = cur_dir / f'eln{i}.parquet'
        df_cur  = pl.read_parquet(p) if p.exists() else pl.DataFrame()
        df_prev = load_previous(i)
        combined = pl.concat([df_cur, df_prev], how='diagonal_relaxed') if not df_prev.is_empty() else df_cur
        combined.write_parquet(p)

    df16 = deduplicate(pl.read_parquet(EREV3_DIR / 'eln16.parquet')) if (EREV3_DIR / 'eln16.parquet').exists() else pl.DataFrame()
    df17 = deduplicate(pl.read_parquet(EREV3_DIR / 'eln17.parquet')) if (EREV3_DIR / 'eln17.parquet').exists() else pl.DataFrame()
    df16 = sort_by_keys(df16)
    df17 = sort_by_keys(df17)
    emul1617 = full_join_pair(df16, df17)
    emul1617 = deduplicate(emul1617)
    emul1617 = sort_by_keys(emul1617)
    emul1617.write_parquet(ELDSRV_OUT_DIR / 'emul1617.parquet')
    logger.info(f"Written EMUL1617: {ELDSRV_OUT_DIR / 'emul1617.parquet'}")

    # -----------------------------------------------------------------------
    # Tables 18-19: append previous, full join -> EMUL1819
    # -----------------------------------------------------------------------
    logger.info("Processing ELN18-19 -> EMUL1819")
    for i in [18, 19]:
        cur_dir, _ = TABLE_DIRS[i]
        p = cur_dir / f'eln{i}.parquet'
        df_cur  = pl.read_parquet(p) if p.exists() else pl.DataFrame()
        df_prev = load_previous(i)
        combined = pl.concat([df_cur, df_prev], how='diagonal_relaxed') if not df_prev.is_empty() else df_cur
        combined.write_parquet(p)

    df18 = deduplicate(pl.read_parquet(EREV3_DIR / 'eln18.parquet')) if (EREV3_DIR / 'eln18.parquet').exists() else pl.DataFrame()
    df19 = deduplicate(pl.read_parquet(EREV3_DIR / 'eln19.parquet')) if (EREV3_DIR / 'eln19.parquet').exists() else pl.DataFrame()
    df18 = sort_by_keys(df18)
    df19 = sort_by_keys(df19)
    emul1819 = full_join_pair(df18, df19)
    emul1819 = deduplicate(emul1819)
    emul1819 = sort_by_keys(emul1819)
    emul1819.write_parquet(ELDSRV_OUT_DIR / 'emul1819.parquet')
    logger.info(f"Written EMUL1819: {ELDSRV_OUT_DIR / 'emul1819.parquet'}")

    # -----------------------------------------------------------------------
    # CPORT equivalent: copy all ELDSRV output files to FTP transfer directory
    # PROC CPORT LIBRARY=ELDSRV FILE=TRANFILE;
    # -----------------------------------------------------------------------
    logger.info("Copying ELDSRV outputs to FTP directory (EREVFTP)")
    import shutil
    ftp_outputs = [
        'reptdate.parquet', 'erevpre.parquet', 'emul7.parquet',
        'erevcrr1.parquet', 'erevcrr2.parquet', 'emul12.parquet',
        'erev13.parquet', 'emul1415.parquet', 'emul1617.parquet',
        'emul1819.parquet',
    ]
    for fname in ftp_outputs:
        src = ELDSRV_OUT_DIR / fname
        if src.exists():
            dst = FTP_OUT_DIR / fname
            shutil.copy2(src, dst)
            logger.info(f"FTP copy: {src} -> {dst}")
        else:
            logger.warning(f"FTP source not found: {src}")

    logger.info("EIBWEREN completed successfully.")


if __name__ == '__main__':
    main()
