# !/usr/bin/env python3
"""
Program : EIBWLNW1
Function: Data Warehouse Extraction Program - Loans Extraction
          Extracts loans information for data warehouse including
          HP, LN, LNPD, LNBL, ULOAN datasets.

Amendment history:
- 301105 NPH  SMR 2005-1194 INCLUDE CAGATAG IN WAREHOUSE
- 240206 NPH  SMR 2006-237 REQUEST FOR EXISTING FIELD CENSUS0.
- 220306 HMK2 ESMR 2006-291 ADDING NEW FIELDS IN LNPD & HPPD
- 240806 NPH  SMR 2006-229 REQUEST FOR EXISTING FIELD MTDINT.
- 220507 NPH  SMR 2007-352 INCLUDE A VARIABLE - MODEL DESC.
- 010208 RHA  SMR 2008-021 INCLUDE NEW VARIABLES - CANO (ESCR ACCTNO)
                           & POINTAMT (CURRENT ACCT COLLECTED BALANCE)
- 180208 RHA  SMR 2008-162 INCLUDE NEW VARIABLES :
                           CEILINGO (CEILING RATE OVER) &
                           CEILINGU (CEILING RATE UNDER)
- 280208 RHA  SMR 2008-242 INCLUDE NEW VARIABLE  :
                           DATEREGV (VEHICLE REGISTRATION DATE)
- 100408 RHA  SMR 2008-416 EXCLUDE A VARIABLE  :
                           INTPYTD1
- 060712 MFM  SMR 2012-180 CREATE HPWO DATASET & EXCLUDE WO PRODUCT
                           FROM LN & HP.
- 260115 TBC  SMR 2014-2876 TAG VB IN LOANS
- 020216 TBC  SMR 2015-2489 TAG RSN IN LOANS
- 310517 IFA  SMR 2017-1823 INCLUDE PRODUCT(392) IN HP DATASET
"""

import os
import math
import struct
import logging
from pathlib import Path
from datetime import date, datetime, timedelta
import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Dependencies from PBBDPFMT, PBBLNFMT, PBBELF
# ---------------------------------------------------------------------------
from PBBDPFMT import (SADenomFormat, SAProductFormat, FDDenomFormat,
                      FDProductFormat, CADenomFormat, CAProductFormat,
                      FCYTermFormat, ProductLists)

from PBBLNFMT import (format_lndenom, format_lnprod, format_lncustcd,
                      format_statecd, HP_ALL, FCY_PRODUCTS,
                      MOREPLAN_PRODUCTS, MOREISLM_PRODUCTS)

from PBBELF import (EL_DEFINITIONS, ELI_DEFINITIONS, BRCHCD_MAP,
                    format_brchcd, format_cacbrch, format_regioff)

# Inline key format functions from PBBLNFMT
LNPROD_MAP = {
    **{k: '34230' for k in [4, 5, 6, 7, 15, 20] + list(range(25, 35)) +
       [60, 61, 62, 63, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79] +
       [100, 101, 102, 103, 104, 105, 106, 107, 108]},
    **{k: '34120' for k in [110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
                             139, 140, 141, 142, 147, 173, 445, 446] +
       list(range(200, 249)) + list(range(250, 261)) +
       [400, 409, 410, 412, 413, 414, 415, 423, 431, 432, 433, 440, 466,
        472, 473, 474, 479, 484, 486, 489, 494, 600, 638, 650, 651, 664, 677, 911]},
    **{k: '34149' for k in [120, 122, 126, 127, 129, 133, 134, 137, 143, 144, 148, 149,
                             153, 154, 155, 157, 158, 159, 160, 161, 162, 163, 164, 165,
                             169, 170, 172, 174, 176, 177, 178, 179, 181, 182, 183, 187,
                             188, 189, 190, 193, 194, 199] +
       list(range(300, 316)) +
       [316, 320, 322, 325, 335, 345, 348, 349, 356, 357, 358, 359,
        361, 362, 363, 368] +
       list(range(401, 409)) +
       [411, 416, 417, 418, 421, 425, 427, 428, 429, 430, 447, 448] +
       list(range(434, 440)) +
       [442, 461, 462, 463, 467, 471, 476, 477, 478, 480, 481, 485, 487, 488]},
    **{k: '34111' for k in [128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 750, 752, 760]},
    **{k: '34117' for k in [135, 136, 138, 303, 306, 307, 308, 311, 313, 325, 330, 340,
                             354, 355, 367, 369, 419, 420, 422, 424, 426, 441, 443, 464,
                             465, 468, 469, 470, 475, 477, 482, 483, 490, 491, 492, 493,
                             496, 497, 498, 609, 610, 611, 668, 669, 672, 673, 674, 675, 693]},
    **{k: '34114' for k in [309, 310, 417, 608, 637, 670, 690, 904, 905, 919, 920]},
    390: '34112', 799: '34112',
    **{k: '34113' for k in [360, 770, 775, 908]},
    **{k: '34115' for k in [180, 185, 186, 197, 684, 686, 914, 915, 950]},
    **{k: '34190' for k in [146, 184, 190, 192, 195, 196, 302, 350, 351, 364, 365, 506,
                             495, 604, 605, 634, 641, 660, 685, 689, 802, 803, 806, 808,
                             810, 812, 814, 817, 818, 856, 857, 858, 859, 860, 902, 903,
                             910, 917, 925, 951]},
    **{k: '34170' for k in [392, 612]},
    **{k: '34600' for k in [800, 801, 804, 805, 807, 809, 811, 813, 815, 816, 851, 852,
                             853, 854, 855]},
    **{k: '34690' for k in [802, 803, 806, 808, 810, 812, 814, 817, 818, 856, 857, 858, 859, 860]},
    **{k: '54120' for k in [124, 145, 225, 226, 530, 709, 710]},
    98: 'N', 107: 'N',
    **{k: 'N' for k in [126, 127, 128, 129, 130, 136, 144, 145, 171, 172, 173, 500, 520,
                         678, 679, 698, 699, 972, 973]},
    **{k: 'M' for k in [984, 985, 994, 995, 996]},
}

LNCUSTCD_MAP = {
    10: '10', 15: '15', 20: '20', 36: '36', 50: '50', 60: '60',
    65: '65', 76: '76', 80: '80', 81: '81', 85: '85',
    1: '01', 2: '02', 3: '03', 4: '04', 5: '05', 6: '06',
    11: '11', 12: '12', 13: '13', 17: '17', 30: '30', 32: '32',
    33: '33', 34: '34', 35: '35', 37: '37', 38: '38', 39: '39',
    40: '40', 41: '41', 42: '42', 43: '43', 44: '44', 46: '46',
    47: '47', 48: '48', 49: '49', 51: '51', 52: '52', 53: '53',
    54: '54', 57: '57', 59: '59', 61: '61', 62: '62', 63: '63',
    64: '64', 66: '41', 67: '44', 68: '48', 69: '52',
    70: '71', 71: '71', 72: '72', 73: '73', 74: '74', 75: '75',
    77: '77', 78: '78', 79: '79',
    82: '82', 83: '83', 84: '84',
    86: '86', 87: '87', 88: '88', 89: '89', 90: '90', 91: '91',
    92: '92', 95: '95', 96: '96', 98: '98', 99: '99',
}

LNDENOM_ISLAMIC_PRODUCTS = (
    [100, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 122, 126, 127,
     128, 129, 130, 131, 132, 185, 169, 134, 135, 136, 138, 139, 140, 141, 142,
     143, 170, 180, 181, 182, 183, 101, 147, 148, 173, 174, 159, 160, 161, 162,
     184, 191, 146] + list(range(851, 900)) + list(range(152, 159)) +
    [164, 165, 179] + list(range(192, 197)) +
    [197, 199, 124, 145, 144, 163, 186, 187] + list(range(102, 109)) +
    [188, 189, 190, 137] + list(range(400, 418)) +
    [418, 427, 428, 419, 420, 421, 422] + list(range(429, 445)) +
    [445, 446, 448] + list(range(461, 471)) + list(range(650, 700)) +
    [973] + list(range(471, 499))
)

HP_ALL = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 983, 993, 996, 678, 679, 698, 699]
FCY_PRODUCTS = [800, 801, 802, 803, 804, 805, 806, 807, 808, 816, 817,
                809, 810, 811, 812, 813, 814, 815, 851, 852, 853, 854,
                855, 856, 857, 858, 859, 860]

# FISSCD mapping
FISSCD_MAP = {
    '0440': '0440', '0441': '0440', '0460': '0460',
    '1110': '0110', '1120': '0120', '1131': '0131', '1139': '0132', '1140': '0139',
    '1210': '0110', '1220': '0120', '1230': '0132', '1290': '0139',
    '2111': '0311', '2112': '0312', '2113': '0313', '2114': '0314',
    '2115': '0315', '2116': '0316', '2121': '0321', '2122': '0322',
    '2123': '0323', '2124': '0324', '2129': '0329',
    '2210': '0313', '2211': '0314', '2212': '0315', '2213': '0316',
    '2221': '0321', '2222': '0322', '2223': '0323', '2224': '0324', '2229': '0329',
    '2310': '0313', '2311': '0314', '2312': '0315', '2313': '0316',
    '2321': '0321', '2322': '0322', '2323': '0323', '2324': '0324', '2329': '0329',
    '3100': '0211', '3101': '0212', '3200': '0200', '3900': '0200',
    '4100': '0430', '4200': '0420', '4300': '0410',
    '5100': '0390', '5200': '0390', '5300': '0470',
    '5400': '0311', '5401': '0312', '5402': '0313', '5403': '0314',
    '5404': '0315', '5405': '0316', '5406': '0321', '5407': '0322',
    '5408': '0323', '5409': '0324', '5410': '0329',
    '5411': '0311', '5412': '0312', '5413': '0313', '5414': '0314',
    '5415': '0315', '5416': '0316', '5417': '0321', '5418': '0322',
    '5419': '0323', '5420': '0324', '5421': '0329',
    '5500': '0410', '5501': '0410', '5600': '0990', '5700': '0990',
    '5800': '0990', '9000': '0990', '9001': '0990',
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
BASE_DIR = Path('/data')

# Input parquet paths (converted from SAS datasets)
CISL_LOAN_PARQUET      = BASE_DIR / 'cisl/loan.parquet'
BNM1_REPTDATE_PARQUET  = BASE_DIR / 'bnm1/reptdate.parquet'
BNM1_LNNOTE_PARQUET    = BASE_DIR / 'bnm1/lnnote.parquet'
BNM1_RVNOTE_PARQUET    = BASE_DIR / 'bnm1/rvnote.parquet'
BNM1_LNCOMM_PARQUET    = BASE_DIR / 'bnm1/lncomm.parquet'
BNM1_NAME8_PARQUET     = BASE_DIR / 'bnm1/name8.parquet'
BNM1_HPCOMP_PARQUET    = BASE_DIR / 'bnm1/hpcomp.parquet'
BNM2_MTDINT_PARQUET    = BASE_DIR / 'bnm2/mtdint.parquet'
MNITB_CURRENT_PARQUET  = BASE_DIR / 'mnitb/current.parquet'
ODGP3_OVERDFT_PARQUET  = BASE_DIR / 'odgp3/overdft.parquet'
ODGP3_GP3_PARQUET      = BASE_DIR / 'odgp3/gp3.parquet'
MNINPL_DIR             = BASE_DIR / 'mninpl'
WOFF_WOFFTOT_PARQUET   = BASE_DIR / 'woff/wofftot.parquet'
MISMLN_DIR             = BASE_DIR / 'mismln'
MNICRM_DIR             = BASE_DIR / 'mnicrm'
LNFILE_TOTPAY_PARQUET  = BASE_DIR / 'lnfile/totpay.parquet'
BNM_DIR                = BASE_DIR / 'bnm'
FEEYTD_DIR             = BASE_DIR / 'feeytd'
HPWO_DIR               = BASE_DIR / 'hpwo'

# Input text/binary files
HISTFILE_PATH  = BASE_DIR / 'histfile/histfile.dat'
REFNOTE_PATH   = BASE_DIR / 'refnote/refnote.txt'
RAW_PATH       = BASE_DIR / 'raw/lnbnslnk.txt'
FEED31_PATH    = BASE_DIR / 'feed31/fee.txt'
PAYFI_PATH     = BASE_DIR / 'payfi/paysfile.dat'

# Historical packed-decimal files
HIST_UP2DEC13 = BASE_DIR / 'hist/up2dec13.parquet'
HIST_UP2DEC15 = BASE_DIR / 'hist/up2dec15.parquet'
HIST_UP2DEC22 = BASE_DIR / 'hist/up2dec22.parquet'

# Output directories
LOAN_OUT_DIR = BASE_DIR / 'output/loan'
HP_OUT_DIR   = BASE_DIR / 'output/hp'
HPWO_OUT_DIR = BASE_DIR / 'output/hpwo'

for d in [LOAN_OUT_DIR, HP_OUT_DIR, HPWO_OUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# UTILITY FUNCTIONS
# ---------------------------------------------------------------------------

def packed_decimal_to_int(raw: bytes) -> int:
    """Decode IBM mainframe packed decimal (BCD) bytes to integer."""
    digits = ''
    for i, byte in enumerate(raw):
        high = (byte >> 4) & 0x0F
        low  = byte & 0x0F
        if i == len(raw) - 1:
            sign = low
            digits += str(high)
        else:
            digits += str(high) + str(low)
    val = int(digits) if digits else 0
    return val  # sign nibble: C/F=positive, D=negative

def read_packed_decimal(raw: bytes, length: int) -> int:
    """Read packed decimal of given byte length."""
    return packed_decimal_to_int(raw[:length])

def sas_date_from_int(val) -> date | None:
    """Convert SAS date integer (days since 1960-01-01) to Python date."""
    if val is None or val == 0:
        return None
    try:
        return date(1960, 1, 1) + timedelta(days=int(val))
    except Exception:
        return None

def sas_datetime_z11(val) -> date | None:
    """Convert SAS packed-style integer yyyymmddHHMM stored as 11-digit Z to date."""
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(11)
        date_part = s[:8]  # mmddyyyy in SAS Z11 layout: positions 1-8
        return datetime.strptime(date_part, '%m%d%Y').date()
    except Exception:
        return None

def mmddyy_from_z11(val) -> date | None:
    """SAS Z11: positions 1-8 = MMDDYYYY."""
    return sas_datetime_z11(val)

def ddmmyy8_from_str(s: str) -> date | None:
    """Parse DDMMYY8 formatted string DD/MM/YY."""
    if not s or s.strip() == '':
        return None
    try:
        return datetime.strptime(s.strip(), '%d/%m/%y').date()
    except Exception:
        try:
            return datetime.strptime(s.strip(), '%d/%m/%Y').date()
        except Exception:
            return None

def payeffdt_format(val) -> str | None:
    """Format payeffdt from packed integer to DD/MM/YY string."""
    if val is None or val == 0:
        return None
    s = str(int(val)).zfill(11)
    dd = s[9:11]
    mm = s[7:9]
    yy = s[2:4]
    return f"{dd}/{mm}/{yy}"

def compute_mtharr(dayarr) -> int:
    """Compute MTHARR (months in arrears) from DAYARR for main LN dataset."""
    if dayarr is None:
        return 0
    d = int(dayarr)
    if d > 729:  return int((d / 365) * 12)
    elif d > 698: return 23
    elif d > 668: return 22
    elif d > 638: return 21
    elif d > 608: return 20
    elif d > 577: return 19
    elif d > 547: return 18
    elif d > 516: return 17
    elif d > 486: return 16
    elif d > 456: return 15
    elif d > 424: return 14
    elif d > 394: return 13
    elif d > 364: return 12
    elif d > 333: return 11
    elif d > 303: return 10
    elif d > 273: return 9
    elif d > 243: return 8
    elif d > 213: return 7
    elif d > 182: return 6
    elif d > 151: return 5
    elif d > 121: return 4
    elif d > 89:  return 3
    elif d > 59:  return 2
    elif d > 30:  return 1
    else:         return 0

def compute_mtharr_lnpd(dayarr) -> int:
    """Compute MTHARR (months in arrears) from DAYARR for LNPD dataset (extended range)."""
    if dayarr is None:
        return 0
    d = int(dayarr)
    if d > 1004: return int((d / 365) * 12)
    elif d > 974:  return 32
    elif d > 944:  return 31
    elif d > 913:  return 30
    elif d > 883:  return 29
    elif d > 852:  return 28
    elif d > 821:  return 27
    elif d > 791:  return 26
    elif d > 760:  return 25
    elif d > 729:  return 24
    elif d > 698:  return 23
    elif d > 668:  return 22
    elif d > 638:  return 21
    elif d > 608:  return 20
    elif d > 577:  return 19
    elif d > 547:  return 18
    elif d > 516:  return 17
    elif d > 486:  return 16
    elif d > 456:  return 15
    elif d > 424:  return 14
    elif d > 394:  return 13
    elif d > 364:  return 12
    elif d > 333:  return 11
    elif d > 303:  return 10
    elif d > 273:  return 9
    elif d > 243:  return 8
    elif d > 213:  return 7
    elif d > 182:  return 6  # note: duplicated in SAS, kept as-is
    elif d > 151:  return 5
    elif d > 121:  return 4
    elif d > 89:   return 3
    elif d > 59:   return 2
    elif d > 30:   return 1
    else:          return 0

def compute_remainmt(reptdate: date, exprdate: date | None) -> int:
    """Compute REMAINMT: number of months from reptdate to exprdate."""
    if exprdate is None or reptdate > exprdate:
        return 0
    folmonth = reptdate
    nummonth = 0
    rd_day = reptdate.day
    while exprdate > folmonth:
        nummonth += 1
        nextmon = folmonth.month + 1
        nextyear = folmonth.year
        if nextmon > 12:
            nextmon -= 12
            nextyear += 1
        nextday = rd_day
        # Month-end day adjustments
        if nextmon == 2 and nextday >= 29:
            if nextyear % 4 == 0 and (nextyear % 100 != 0 or nextyear % 400 == 0):
                nextday = 29
            else:
                nextday = 28
        elif nextday == 31 and nextmon in (4, 6, 9, 11):
            nextday = 30
        try:
            folmonth = date(nextyear, nextmon, nextday)
        except ValueError:
            import calendar
            last = calendar.monthrange(nextyear, nextmon)[1]
            folmonth = date(nextyear, nextmon, last)
    return nummonth

def fix_payefdt(payefdt_str: str | None) -> date | None:
    """Parse and validate payefdt string (DD/MM/YY), fixing month-end issues."""
    if not payefdt_str:
        return None
    parts = payefdt_str.split('/')
    if len(parts) != 3:
        return None
    try:
        payd = int(parts[0])
        paym = int(parts[1])
        payy = int(parts[2])
        if payy < 50:
            payy += 2000
        else:
            payy += 1900
        if paym == 2 and payd > 29:
            payd = 28
            if payy % 4 == 0 and (payy % 100 != 0 or payy % 400 == 0):
                payd = 29
        elif paym != 2 and payd > 31:
            if paym in (1, 3, 5, 7, 8, 10, 12):
                payd = 31
            elif paym in (4, 6, 9, 11):
                payd = 30
        return date(payy, paym, payd)
    except Exception:
        return None

def format_lnprod(product: int) -> str:
    return LNPROD_MAP.get(product, '34149')

def format_lncustcd(custcode) -> str:
    if custcode is None:
        return '79'
    return LNCUSTCD_MAP.get(int(custcode), '79')

def format_fisscd(code: str) -> str:
    return FISSCD_MAP.get(str(code).strip(), str(code).strip())

# ---------------------------------------------------------------------------
# REPTDATE: Read report date and derive macro variables
# ---------------------------------------------------------------------------

def get_reptdate() -> dict:
    """Read REPTDATE and derive week/month/year macro variables."""
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{BNM1_REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate: date = sas_date_from_int(row[0]) if isinstance(row[0], (int, float)) else row[0]

    day = reptdate.day
    if day == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif day == 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif day == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'

    mm = reptdate.month
    mm2 = mm - 1 if mm > 1 else 12
    mm1 = (mm - 1 if mm > 1 else 12) if wk == '1' else mm
    sdate = date(reptdate.year, mm, 1)
    reptyear = str(reptdate.year)[2:]

    return {
        'reptdate': reptdate,
        'nowk': wk,
        'nowk1': wk1,
        'nowks': '4',
        'reptmon': f'{mm:02d}',
        'reptmon1': f'{mm1:02d}',
        'reptmon2': f'{mm2:02d}',
        'reptyear': reptyear,
        'reptday': f'{day:02d}',
        'rdate': reptdate.strftime('%d/%m/%y'),
        'sdate': sdate,
        'sdd': sdd,
    }

# ---------------------------------------------------------------------------
# GET_FEE: Handle FEEYTD for December week-4
# ---------------------------------------------------------------------------

def get_fee(ctx: dict) -> pl.DataFrame:
    """
    Equivalent of %GET_FEE macro.
    If December week-4, merge FEE31 data with FEE30 and update LNNOTE.
    Otherwise return LNNOTE sorted by ACCTNO, NOTENO.
    """
    reptmon = ctx['reptmon']
    nowk    = ctx['nowk']

    lnnote = pl.read_parquet(BNM1_LNNOTE_PARQUET)

    if reptmon == '12' and nowk == '4':
        logger.info("December week-4: processing FEE31DEC")

        # Read FEED31 text file
        fee_records = []
        with open(FEED31_PATH, 'rb') as f:
            for line in f:
                if len(line) < 73:
                    continue
                try:
                    acctno    = int(line[10:21])
                    noteno    = int(line[21:26])
                    feeplan   = line[26:28].decode('ascii', errors='replace').strip()
                    feecatgy  = line[31:33].decode('ascii', errors='replace').strip()
                    trancode  = int(line[33:36])
                    # ZDB13.2 = zoned decimal 13 digits, 2 decimal places
                    fee31d_raw = line[59:72].decode('ascii', errors='replace').strip()
                    fee31d = float(fee31d_raw) / 100.0 if fee31d_raw else 0.0
                    reversed_ = line[72:73].decode('ascii', errors='replace').strip()
                    if trancode in (760, 761) and feecatgy in ('LT', 'MS', 'TF'):
                        if reversed_ == 'R':
                            fee31d = -fee31d
                        fee_records.append({'ACCTNO': acctno, 'NOTENO': noteno, 'FEE31D': fee31d})
                except Exception:
                    continue

        fee31 = pl.DataFrame(fee_records) if fee_records else pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'FEE31D': []})
        fee31 = fee31.group_by(['ACCTNO', 'NOTENO']).agg(pl.col('FEE31D').sum())

        # Load FEE30DEC from FEEYTD
        fee30_path = FEEYTD_DIR / 'fee30dec.parquet'
        fee30 = pl.read_parquet(fee30_path).select(['ACCTNO', 'NOTENO', 'FEE30D'])

        fee31_merged = fee30.join(fee31, on=['ACCTNO', 'NOTENO'], how='outer_coalesce')
        fee31_merged = fee31_merged.with_columns(
            (pl.col('FEE30D').fill_null(0) + pl.col('FEE31D').fill_null(0)).alias('FEEYTD')
        ).unique(subset=['ACCTNO', 'NOTENO'])

        # Save FEE31DEC output
        fee31_merged.write_parquet(FEEYTD_DIR / 'fee31dec.parquet')

        # Drop FEEYTD, INTPDYTD, ACCRUYTD from LNNOTE then merge
        drop_cols = [c for c in ['FEEYTD', 'INTPDYTD', 'ACCRUYTD'] if c in lnnote.columns]
        lnnote = lnnote.drop(drop_cols)
        lnnote = lnnote.join(
            fee31_merged.select(['ACCTNO', 'NOTENO', 'FEEYTD']),
            on=['ACCTNO', 'NOTENO'], how='left'
        )
        if 'TOTPDEOP' in lnnote.columns:
            lnnote = lnnote.with_columns(pl.col('TOTPDEOP').alias('INTPDYTD'))
        if 'ACCRUEOP' in lnnote.columns:
            lnnote = lnnote.with_columns(pl.col('ACCRUEOP').alias('ACCRUYTD'))

    return lnnote.sort(['ACCTNO', 'NOTENO'])

# ---------------------------------------------------------------------------
# HIST: Read history file (packed decimal binary)
# ---------------------------------------------------------------------------

def read_hist_file(histfile_path: Path) -> pl.DataFrame:
    """Read HISTFILE: packed decimal binary layout."""
    records = []
    if not histfile_path.exists():
        logger.warning(f"HISTFILE not found: {histfile_path}")
        return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'USERID': [], 'POSTDATE': []})

    with open(histfile_path, 'rb') as f:
        data = f.read()

    offset = 0
    rec_len = 42  # approximate based on @001 PD6, @007 PD3, @019 $8, @034 PD6
    while offset + rec_len <= len(data):
        chunk = data[offset:offset + rec_len]
        try:
            acctno = packed_decimal_to_int(chunk[0:6])
            noteno = packed_decimal_to_int(chunk[6:9])
            userid = chunk[18:26].decode('cp037', errors='replace').strip()
            postdt_raw = packed_decimal_to_int(chunk[33:39])
            postdate = None
            if postdt_raw not in (0,):
                postdate = mmddyy_from_z11(postdt_raw)
        except Exception:
            offset += rec_len
            continue
        records.append({'ACCTNO': acctno, 'NOTENO': noteno, 'USERID': userid, 'POSTDATE': postdate})
        offset += rec_len

    return pl.DataFrame(records) if records else pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'USERID': [], 'POSTDATE': []})

def load_hist(ctx: dict) -> pl.DataFrame:
    """Load and combine historical datasets then dedup."""
    frames = []
    for path in [HIST_UP2DEC13, HIST_UP2DEC15, HIST_UP2DEC22]:
        if Path(path).exists():
            frames.append(pl.read_parquet(path))

    hist_new = read_hist_file(HISTFILE_PATH)
    frames.append(hist_new)

    if not frames:
        return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'POSTDATE': []})

    hist = pl.concat(frames, how='diagonal_relaxed')
    hist = hist.filter(pl.col('POSTDATE').is_not_null())
    # Sort descending by POSTDATE then dedup by ACCTNO, NOTENO
    hist = hist.sort(['ACCTNO', 'NOTENO', 'POSTDATE'], descending=[False, False, True])
    hist = hist.unique(subset=['ACCTNO', 'NOTENO'], keep='first')
    return hist

# ---------------------------------------------------------------------------
# REFNOTE: Read reference note file
# ---------------------------------------------------------------------------

def read_refnote() -> pl.DataFrame:
    """Read REFNOTE text file: @001 ACCTNO 11, @012 OLDNOTE 5, @017 NOTENO 5."""
    records = []
    if not REFNOTE_PATH.exists():
        logger.warning(f"REFNOTE not found: {REFNOTE_PATH}")
        return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'OLDNOTE': []})
    with open(REFNOTE_PATH, 'r') as f:
        for line in f:
            if len(line) < 21:
                continue
            try:
                acctno  = int(line[0:11].strip())
                oldnote = int(line[11:16].strip() or '0')
                noteno  = int(line[16:21].strip())
                records.append({'ACCTNO': acctno, 'NOTENO': noteno, 'OLDNOTE': oldnote})
            except Exception:
                continue
    df = pl.DataFrame(records) if records else pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'OLDNOTE': []})
    return df.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

# ---------------------------------------------------------------------------
# LNNOTE enrichment (merge with HIST, REFNOTE, computed fields)
# ---------------------------------------------------------------------------

def enrich_lnnote(lnnote: pl.DataFrame, hist: pl.DataFrame, refnote: pl.DataFrame) -> pl.DataFrame:
    """Merge LNNOTE with HIST and REFNOTE, compute derived fields."""
    # Merge HIST
    hist_sel = hist.select(['ACCTNO', 'NOTENO', 'POSTDATE'])
    lnnote = lnnote.join(hist_sel, on=['ACCTNO', 'NOTENO'], how='left')

    # Merge REFNOTE
    refnote_sel = refnote.select(['ACCTNO', 'NOTENO', 'OLDNOTE'])
    lnnote = lnnote.join(refnote_sel, on=['ACCTNO', 'NOTENO'], how='left')

    def _build_timelate(row):
        tl15 = str(int(row.get('TMLATE15') or 0)).zfill(3)
        tl30 = str(int(row.get('TMLATE30') or 0)).zfill(3)
        tl60 = str(int(row.get('TMLATE60') or 0)).zfill(3)
        tl90 = str(int(row.get('TMLATE90') or 0)).zfill(3)
        return f"{tl15}\\{tl30}\\{tl60}\\{tl90}"

    # Apply derived columns using map_rows where needed
    cols = lnnote.columns

    exprs = []

    # NONACCRUAL = AUTNACCR
    if 'AUTNACCR' in cols:
        exprs.append(pl.col('AUTNACCR').alias('NONACCRUAL'))
    # SENDBILL = SENDNBL
    if 'SENDNBL' in cols:
        exprs.append(pl.col('SENDNBL').alias('SENDBILL'))
    # LNUSER2 = USER5
    if 'USER5' in cols:
        exprs.append(pl.col('USER5').alias('LNUSER2'))
    # USINDEX = USURYIDX
    if 'USURYIDX' in cols:
        exprs.append(pl.col('USURYIDX').alias('USINDEX'))
    # SCORE1MI = MORTGIND
    if 'MORTGIND' in cols:
        exprs.append(pl.col('MORTGIND').alias('SCORE1MI'))
    # SCORE2CT = CONTRTYPE
    if 'CONTRTYPE' in cols:
        exprs.append(pl.col('CONTRTYPE').alias('SCORE2CT'))

    if exprs:
        lnnote = lnnote.with_columns(exprs)

    # REFNOTENO: if OLDNOTE not in (null, 0) then REFNOTENO=OLDNOTE else REFNOTENO=BONUSANO
    if 'OLDNOTE' in lnnote.columns and 'BONUSANO' in lnnote.columns:
        lnnote = lnnote.with_columns(
            pl.when((pl.col('OLDNOTE').is_not_null()) & (pl.col('OLDNOTE') != 0))
            .then(pl.col('OLDNOTE'))
            .otherwise(pl.col('BONUSANO'))
            .alias('REFNOTENO')
        )
    elif 'BONUSANO' in lnnote.columns:
        lnnote = lnnote.with_columns(pl.col('BONUSANO').alias('REFNOTENO'))

    # Date conversions from packed integers
    date_z11_cols = ['FRELEAS', 'DEATHDATE', 'VARSTDTE', 'FCLOSUREDT', 'CPNSTDTE']
    for col in date_z11_cols:
        if col in lnnote.columns:
            lnnote = lnnote.with_columns(
                pl.col(col).map_elements(
                    lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                    return_dtype=pl.Date
                ).alias(f'_{col}_dt')
            )

    if 'FRELEAS' in lnnote.columns:
        lnnote = lnnote.with_columns([
            pl.col('_FRELEAS_dt').alias('FULLREL_DT'),
            pl.col('_FRELEAS_dt').alias('ORGISSDTE'),
        ])
    if 'FCLOSUREDT' in lnnote.columns:
        lnnote = lnnote.with_columns(pl.col('_FCLOSUREDT_dt').alias('RRCOUNTDTE'))
    if 'CPNSTDTE' in lnnote.columns:
        lnnote = lnnote.with_columns(pl.col('_CPNSTDTE_dt').alias('CPNSTDTE'))

    # VALUEDTE -> VALUATION_DT (DDMMYY8 string)
    if 'VALUEDTE' in lnnote.columns:
        lnnote = lnnote.with_columns(
            pl.col('VALUEDTE').map_elements(
                lambda v: ddmmyy8_from_str(str(v)) if v else None,
                return_dtype=pl.Date
            ).alias('VALUATION_DT')
        )

    # INTSTDTE: special format from SAS (positions 2-4=year, 9-10=month, 11-12=day from 12-char string)
    if 'INTSTDTE' in lnnote.columns:
        def parse_intstdte(v):
            if v is None or v == 0:
                return None
            try:
                s = str(v)
                dd = s[10:12] if len(s) >= 12 else '01'
                mm = s[8:10] if len(s) >= 10 else '01'
                yy = s[1:5] if len(s) >= 5 else '2000'
                return date(int(yy), int(mm), int(dd))
            except Exception:
                return None
        lnnote = lnnote.with_columns(
            pl.col('INTSTDTE').map_elements(parse_intstdte, return_dtype=pl.Date).alias('INTSTDTE')
        )

    # Drop temp columns
    drop_tmp = [c for c in lnnote.columns if c.startswith('_') and c.endswith('_dt')]
    if drop_tmp:
        lnnote = lnnote.drop(drop_tmp)

    # TIMELATE
    if all(c in lnnote.columns for c in ['TMLATE15', 'TMLATE30', 'TMLATE60', 'TMLATE90']):
        lnnote = lnnote.with_columns(
            pl.struct(['TMLATE15', 'TMLATE30', 'TMLATE60', 'TMLATE90'])
            .map_elements(_build_timelate, return_dtype=pl.Utf8)
            .alias('TIMELATE')
        )

    # Drop OLDNOTE as per SAS (DROP=OLDNOTE)
    if 'OLDNOTE' in lnnote.columns:
        lnnote = lnnote.drop(['OLDNOTE'])

    return lnnote

# ---------------------------------------------------------------------------
# Merge LNNOTE with LNCOMM
# ---------------------------------------------------------------------------

def merge_lnnote_comm(lnnote: pl.DataFrame) -> pl.DataFrame:
    """Merge LNNOTE with LNCOMM, compute BRANCH, COMMTYPE, ESCROWRBAL, INTERDUE."""
    lncomm = pl.read_parquet(BNM1_LNCOMM_PARQUET)
    merged = lnnote.join(lncomm, on=['ACCTNO', 'COMMNO'], how='left')

    exprs = []
    if 'NTBRCH' in merged.columns:
        exprs.append(pl.col('NTBRCH').alias('BRANCH'))
    if 'CPRODUCT' in merged.columns:
        exprs.append(pl.col('CPRODUCT').alias('COMMTYPE'))
    if 'ECSRRSRV' in merged.columns:
        exprs.append(pl.col('ECSRRSRV').alias('ESCROWRBAL'))
    if 'STATE' in merged.columns:
        exprs.append(pl.col('STATE').alias('LN_UTILISE_LOCAT_CD'))
    if exprs:
        merged = merged.with_columns(exprs)

    # NUMCPNS format Z4
    if 'NUMCPNS' in merged.columns:
        merged = merged.with_columns(
            pl.col('NUMCPNS').cast(pl.Utf8).str.zfill(4).alias('NUMCPNS')
        )

    # INTERDUE: IF PAIDIND NE 'P' THEN IF NTINT NE 'A' THEN INTERDUE=BALANCE-CURBAL-FEEAMT ELSE 0
    if all(c in merged.columns for c in ['PAIDIND', 'NTINT', 'BALANCE', 'CURBAL', 'FEEAMT']):
        merged = merged.with_columns(
            pl.when(pl.col('PAIDIND') != 'P')
            .then(
                pl.when(pl.col('NTINT') != 'A')
                .then(pl.col('BALANCE') - pl.col('CURBAL') - pl.col('FEEAMT'))
                .otherwise(0.0)
            )
            .otherwise(None)
            .alias('INTERDUE')
        )

    return merged

# ---------------------------------------------------------------------------
# Load NPL indicator
# ---------------------------------------------------------------------------

def load_npl(ctx: dict) -> pl.DataFrame:
    reptmon1 = ctx['reptmon1']
    npl_path = MNINPL_DIR / f'totiis{reptmon1}.parquet'
    if npl_path.exists():
        return pl.read_parquet(npl_path).select(['ACCTNO', 'NOTENO', 'NPLIND'])
    logger.warning(f"NPL file not found: {npl_path}")
    return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'NPLIND': []})

# ---------------------------------------------------------------------------
# Load main LOAN dataset from BNM
# ---------------------------------------------------------------------------

def load_loan_base(ctx: dict) -> pl.DataFrame:
    """Load LOAN base from LOAN+LNWOD+LNWOF parquet files."""
    reptmon = ctx['reptmon']
    nowk    = ctx['nowk']
    frames = []
    for suffix in ['LOAN', 'LNWOD', 'LNWOF']:
        p = BNM_DIR / f'{suffix.lower()}{reptmon}{nowk}.parquet'
        if p.exists():
            df = pl.read_parquet(p)
            frames.append(df)
        else:
            logger.warning(f"Missing base loan file: {p}")
    if not frames:
        return pl.DataFrame()
    loan = pl.concat(frames, how='diagonal_relaxed')
    exprs = []
    if 'CUSTCD' in loan.columns:
        exprs.append(pl.col('CUSTCD').alias('CUSTFISS'))
    if 'SECTORCD' in loan.columns:
        exprs.append(pl.col('SECTORCD').alias('SECTFISS'))
    if exprs:
        loan = loan.with_columns(exprs)
    return loan

# ---------------------------------------------------------------------------
# GP3 risk rate (week-4 only)
# ---------------------------------------------------------------------------

def apply_gp3(loan: pl.DataFrame, ctx: dict) -> pl.DataFrame:
    if ctx['nowk'] != '4':
        return loan
    if not ODGP3_GP3_PARQUET.exists():
        return loan
    gp3 = pl.read_parquet(ODGP3_GP3_PARQUET)
    if 'RISKCODE' in gp3.columns:
        gp3 = gp3.with_columns(
            pl.col('RISKCODE').cast(pl.Utf8).str.slice(0, 1).cast(pl.Int64).alias('RISKRTE')
        )
    gp3 = gp3.select(['ACCTNO', 'RISKRTE'])
    # Drop existing RISKRTE if present
    if 'RISKRTE' in loan.columns:
        loan = loan.drop('RISKRTE')
    return loan.join(gp3, on='ACCTNO', how='left')

# ---------------------------------------------------------------------------
# OVERDFT: current accounts with negative balance, merge APPRLIM2 etc.
# ---------------------------------------------------------------------------

def load_overdft(ctx: dict) -> pl.DataFrame:
    reptdate = ctx['reptdate']

    current = pl.read_parquet(MNITB_CURRENT_PARQUET).filter(pl.col('CURBAL') < 0)
    overdft = pl.read_parquet(ODGP3_OVERDFT_PARQUET)
    if 'APPRLIMT' in overdft.columns:
        overdft = overdft.rename({'APPRLIMT': 'APPRLIM2'})
    overdft = overdft.unique(subset=['ACCTNO'], keep='first')

    merged = current.join(overdft, on='ACCTNO', how='left')

    def calc_dayarr(row):
        exoddate = row.get('EXODDATE') or 0
        tempoddt = row.get('TEMPODDT') or 0
        curbal   = row.get('CURBAL') or 0
        if (exoddate != 0 or tempoddt != 0) and curbal <= 0:
            exoddt = mmddyy_from_z11(exoddate) if exoddate else None
            tempdt = mmddyy_from_z11(tempoddt) if tempoddt else None
            if exoddt and tempdt:
                oddate = exoddt if exoddt < tempdt else tempdt
            elif not exoddate:
                oddate = tempdt
            else:
                oddate = exoddt
            if oddate:
                return (reptdate - oddate).days + 1
        return None

    if all(c in merged.columns for c in ['EXODDATE', 'TEMPODDT', 'CURBAL']):
        merged = merged.with_columns(
            pl.struct(['EXODDATE', 'TEMPODDT', 'CURBAL'])
            .map_elements(calc_dayarr, return_dtype=pl.Int64)
            .alias('DAYARR')
        )

    keep_cols = ['ACCTNO'] + [c for c in ['APPRLIM2', 'ASCORE_PERM', 'ASCORE_LTST',
                                            'ASCORE_COMM', 'INDUSTRIAL_SECTOR_CD', 'DAYARR']
                               if c in merged.columns]
    return merged.select(keep_cols)

# ---------------------------------------------------------------------------
# MISMLN VG merge
# ---------------------------------------------------------------------------

def merge_mismln(loan: pl.DataFrame, ctx: dict) -> pl.DataFrame:
    reptmon = ctx['reptmon']
    p = MISMLN_DIR / f'lnvg{reptmon}.parquet'
    if p.exists():
        vg = pl.read_parquet(p)
        loan = loan.join(vg, on=['ACCTNO', 'NOTENO'], how='left')
    else:
        logger.warning(f"MISMLN file not found: {p}")
    return loan

# ---------------------------------------------------------------------------
# Build main LN dataset
# ---------------------------------------------------------------------------

def build_ln(loan: pl.DataFrame, ctx: dict) -> pl.DataFrame:
    """
    Compute all derived fields for the main LN dataset.
    Equivalent to DATA LN step in SAS.
    """
    reptdate   = ctx['reptdate']
    rdate_str  = ctx['rdate']
    reptday    = int(ctx['reptday'])

    drop_cols = [c for c in ['LASTTRAN', 'BIRTHDT', 'CENSUS', 'THISDATE',
                              'INTPYTD1', 'DAYARR_MO'] if c in loan.columns]
    ln = loan.drop([c for c in drop_cols if c in loan.columns])

    exprs = []

    # LASTTRAN = LASTRAN (already renamed earlier in SAS, handled via merge)
    if 'LASTRAN' in loan.columns:
        ln = ln.with_columns(pl.col('LASTRAN').alias('LASTTRAN'))

    # PAYEFDT: from PAYEFFDT packed integer
    if 'PAYEFFDT' in loan.columns:
        ln = ln.with_columns(
            pl.col('PAYEFFDT').map_elements(
                lambda v: payeffdt_format(v) if v and v != 0 else None,
                return_dtype=pl.Utf8
            ).alias('PAYEFDT_STR')
        )

    # CENSUS derived fields
    if 'CENSUS' in loan.columns:
        def fmt_census(v):
            if v is None:
                return ('', '', '', '', '')
            s = f'{float(v):7.2f}'.replace('.', '')
            return (s[0:2], s[0:2], s[2:3], s[3:4], s[5:7])
        ln = ln.with_columns([
            pl.col('CENSUS').alias('CENSUS0'),
            pl.col('CENSUS').map_elements(lambda v: f'{float(v):7.2f}'[0:2] if v else '', return_dtype=pl.Utf8).alias('CENSUS1'),
            pl.col('CENSUS').map_elements(lambda v: f'{float(v):7.2f}'[3:4].replace('.','') if v else '', return_dtype=pl.Utf8).alias('CENSUS3'),
            pl.col('CENSUS').map_elements(lambda v: f'{float(v):7.2f}'[4:5].replace('.','') if v else '', return_dtype=pl.Utf8).alias('CENSUS4'),
            pl.col('CENSUS').map_elements(lambda v: f'{float(v):7.2f}'[5:7].replace('.','') if v else '', return_dtype=pl.Utf8).alias('CENSUS5'),
        ])

    # COLLDESC derived fields
    if 'COLLDESC' in loan.columns:
        ln = ln.with_columns([
            pl.col('COLLDESC').str.slice(0, 16).alias('MAKE'),
            pl.col('COLLDESC').str.slice(15, 21).alias('MODEL'),
            pl.col('COLLDESC').str.slice(39, 13).alias('REGNO'),
            pl.col('COLLDESC').str.slice(52, 13).alias('EXREGNO'),
        ])

    # NEWBAL
    if all(c in ln.columns for c in ['BALANCE', 'FEEAMT', 'ACCRUAL']):
        ln = ln.with_columns(
            (pl.col('BALANCE') - pl.col('FEEAMT') - pl.col('ACCRUAL')).alias('NEWBAL')
        )

    # DAYARR
    if 'BLDATE' in ln.columns:
        ln = ln.with_columns(
            pl.when(pl.col('BLDATE') > 0)
            .then((pl.lit(reptdate) - pl.col('BLDATE').map_elements(sas_date_from_int, return_dtype=pl.Date)).map_elements(
                lambda x: x.days if hasattr(x, 'days') else int(x), return_dtype=pl.Int64
            ))
            .otherwise(None)
            .alias('DAYARR_COMPUTED')
        )
        # DAYARR_MO overrides
        if 'DAYARR_MO' in loan.columns:
            ln = ln.with_columns(
                pl.when(pl.col('DAYARR_MO').is_not_null())
                .then(pl.col('DAYARR_MO'))
                .otherwise(pl.col('DAYARR_COMPUTED'))
                .alias('DAYARR')
            )
        else:
            ln = ln.with_columns(pl.col('DAYARR_COMPUTED').alias('DAYARR'))

        # OLDNOTEDAYARR adjustment
        if 'OLDNOTEDAYARR' in ln.columns and 'NOTENO' in ln.columns:
            ln = ln.with_columns(
                pl.when(
                    (pl.col('OLDNOTEDAYARR') > 0) &
                    (pl.col('NOTENO') >= 98000) & (pl.col('NOTENO') <= 98999)
                )
                .then(
                    pl.when(pl.col('DAYARR') < 0).then(0).otherwise(pl.col('DAYARR'))
                    + pl.col('OLDNOTEDAYARR')
                )
                .otherwise(pl.col('DAYARR'))
                .alias('DAYARR')
            )

    # MTHARR
    if 'DAYARR' in ln.columns:
        ln = ln.with_columns(
            pl.col('DAYARR').map_elements(compute_mtharr, return_dtype=pl.Int64).alias('MTHARR')
        )
        ln = ln.with_columns(
            pl.when(pl.col('DAYARR') > 0)
            .then((pl.col('DAYARR') / 30.00050).map_elements(math.floor, return_dtype=pl.Int64))
            .otherwise(0)
            .alias('MTHARR_CCRIS')
        )

    # COLLAGE
    if 'COLLYEAR' in ln.columns:
        ln = ln.with_columns(
            pl.when(pl.col('COLLYEAR') > 0)
            .then(round((reptdate.year - pl.col('COLLYEAR') + 1) * 10) / 10)
            .otherwise(None)
            .alias('COLLAGE')
        )

    # ORGISSDTE from FRELEAS
    if 'FRELEAS' in ln.columns:
        ln = ln.with_columns(
            pl.col('FRELEAS').map_elements(
                lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                return_dtype=pl.Date
            ).alias('ORGISSDTE')
        )

    return ln

# ---------------------------------------------------------------------------
# NAMEX from BNM1.NAME8
# ---------------------------------------------------------------------------

def load_namex() -> pl.DataFrame:
    if not BNM1_NAME8_PARQUET.exists():
        return pl.DataFrame({'ACCTNO': [], 'DATEREGV': [], 'VEHI_CHASSIS_NUM': [], 'VEHI_ENGINE_NUM': []})
    name8 = pl.read_parquet(BNM1_NAME8_PARQUET)

    def parse_dateregv(v):
        if v is None or v == 0:
            return None
        s = str(int(v)).zfill(11)
        try:
            mm = s[3:5]
            dd = s[5:7]
            yy = s[9:11]
            return f"{dd}/{mm}/{yy}"
        except Exception:
            return None

    exprs = [pl.col('ACCTNO')]
    if 'DTEREGVH' in name8.columns:
        exprs.append(
            pl.col('DTEREGVH').map_elements(parse_dateregv, return_dtype=pl.Utf8).alias('DATEREGV')
        )

    name8 = name8.with_columns(exprs[1:])

    if 'SEQKEY' in name8.columns:
        name8 = name8.with_columns([
            pl.when(pl.col('SEQKEY') == '8')
            .then(pl.col('LINEFOUR').str.slice(0, 22) if 'LINEFOUR' in name8.columns else pl.lit(None))
            .otherwise(pl.lit(None))
            .alias('VEHI_CHASSIS_NUM'),
            pl.when(pl.col('SEQKEY') == '8')
            .then(pl.col('LINETHRE').str.slice(0, 22) if 'LINETHRE' in name8.columns else pl.lit(None))
            .otherwise(pl.lit(None))
            .alias('VEHI_ENGINE_NUM'),
        ])

    keep = ['ACCTNO'] + [c for c in ['DATEREGV', 'VEHI_CHASSIS_NUM', 'VEHI_ENGINE_NUM'] if c in name8.columns]
    return name8.select(keep)

# ---------------------------------------------------------------------------
# NOTEX from BNM1.LNNOTE
# ---------------------------------------------------------------------------

def load_notex() -> pl.DataFrame:
    lnnote = pl.read_parquet(BNM1_LNNOTE_PARQUET)
    exprs = [pl.col('ACCTNO'), pl.col('NOTENO')]
    if 'PRMOFFHP' in lnnote.columns:
        exprs.append(
            pl.col('PRMOFFHP').cast(pl.Utf8).str.zfill(5).str.slice(2, 3).alias('PRIMOFHP')
        )
    for c in ['POINTAMT', 'CEILINGO', 'CEILINGU', 'USER5']:
        if c in lnnote.columns:
            exprs.append(pl.col(c))
    if 'ESCRACCT' in lnnote.columns:
        exprs.append(pl.col('ESCRACCT').alias('CANO'))

    return lnnote.select(exprs)

# ---------------------------------------------------------------------------
# LNBL: Bonuslink dataset from RAW text file
# ---------------------------------------------------------------------------

def build_lnbl(ctx: dict) -> pl.DataFrame:
    """Read RAW bonuslink file into LNBL dataset."""
    records = []
    if not RAW_PATH.exists():
        logger.warning(f"RAW bonuslink file not found: {RAW_PATH}")
        return pl.DataFrame()

    with open(RAW_PATH, 'r') as f:
        for line in f:
            if len(line) < 119:
                continue
            try:
                acctno   = int(line[0:11])
                name     = line[11:35].strip()
                noteno   = int(line[35:40])
                applno   = int(line[40:56])
                intrdno  = int(line[56:72])
                applpnt  = int(line[72:85])
                intrdpnt = int(line[85:98])
                appldt   = line[98:109].strip()
                applimit = float(line[109:119].strip() or '0') / 100.0
                # appldate from APPLDT (mmddyyyy style)
                appldate = None
                if appldt:
                    try:
                        appldate = mmddyy_from_z11(int(appldt))
                    except Exception:
                        pass
                records.append({
                    'ACCTNO': acctno, 'NAME': name, 'NOTENO': noteno,
                    'APPLNO': applno, 'INTRDNO': intrdno, 'APPLPNT': applpnt,
                    'INTRDPNT': intrdpnt, 'APPLDATE': appldate, 'APPLIMIT': applimit
                })
            except Exception:
                continue
    return pl.DataFrame(records) if records else pl.DataFrame()

# ---------------------------------------------------------------------------
# LNPD: Paid-off loans dataset
# ---------------------------------------------------------------------------

def build_lnpd(ctx: dict) -> pl.DataFrame:
    """Build LNPD from BNM1.LNNOTE + BNM1.RVNOTE filtered to paid/closed."""
    reptmon  = ctx['reptmon']
    reptdate = ctx['reptdate']
    rdate    = ctx['rdate']

    lnnote = pl.read_parquet(BNM1_LNNOTE_PARQUET)
    lnnote = lnnote.with_columns(pl.lit(False).alias('_IS_RV'))

    if BNM1_RVNOTE_PARQUET.exists():
        rvnote = pl.read_parquet(BNM1_RVNOTE_PARQUET)
        rvnote = rvnote.with_columns(pl.lit(True).alias('_IS_RV'))
        combined = pl.concat([lnnote, rvnote], how='diagonal_relaxed')
    else:
        combined = lnnote

    # LASTMM from LASTTRAN
    def get_lastmm(v):
        if v and v != 0:
            d = mmddyy_from_z11(v)
            return d.month if d else None
        return None

    if 'LASTTRAN' in combined.columns:
        combined = combined.with_columns(
            pl.col('LASTTRAN').map_elements(get_lastmm, return_dtype=pl.Int64).alias('LASTMM')
        )
    else:
        combined = combined.with_columns(pl.lit(None).cast(pl.Int64).alias('LASTMM'))

    # For RV records, set PAIDIND = ''
    if '_IS_RV' in combined.columns:
        combined = combined.with_columns(
            pl.when(pl.col('_IS_RV'))
            .then(pl.lit(''))
            .otherwise(pl.col('PAIDIND') if 'PAIDIND' in combined.columns else pl.lit(''))
            .alias('PAIDIND')
        )

    # Filter: PAIDIND = 'P' OR (PAIDIND='C' AND LASTMM != REPTMON) OR PAIDIND=''
    reptmon_int = int(reptmon)
    combined = combined.filter(
        (pl.col('PAIDIND') == 'P') |
        ((pl.col('PAIDIND') == 'C') & (pl.col('LASTMM') != reptmon_int)) |
        (pl.col('PAIDIND') == '')
    )

    # CUSTFISS = LNCUSTCD format of CUSTCODE
    if 'CUSTCODE' in combined.columns:
        combined = combined.with_columns(
            pl.col('CUSTCODE').map_elements(format_lncustcd, return_dtype=pl.Utf8).alias('CUSTCD')
        )
        combined = combined.with_columns(pl.col('CUSTCD').alias('CUSTFISS'))

    # PRODCD from LOANTYPE
    if 'LOANTYPE' in combined.columns:
        combined = combined.with_columns(
            pl.col('LOANTYPE').map_elements(
                lambda v: format_lnprod(int(v)) if v else '34149',
                return_dtype=pl.Utf8
            ).alias('PRODCD')
        )

    # EXPRDATE from NOTEMAT
    if 'NOTEMAT' in combined.columns:
        combined = combined.with_columns(
            pl.col('NOTEMAT').map_elements(
                lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                return_dtype=pl.Date
            ).alias('EXPRDATE')
        )

    # CAGATAG = PZIPCODE
    if 'PZIPCODE' in combined.columns:
        combined = combined.with_columns(pl.col('PZIPCODE').alias('CAGATAG'))

    # Date conversions
    for col_name, dest in [('BIRTHDT', 'BIRTHDT'), ('ISSUEDT', 'ISSDTE'),
                            ('FRELEAS', 'ORGISSDTE'), ('CPNSTDTE', 'CPNSTDTE')]:
        if col_name in combined.columns:
            combined = combined.with_columns(
                pl.col(col_name).map_elements(
                    lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                    return_dtype=pl.Date
                ).alias(dest)
            )

    if 'FRELEAS' in combined.columns:
        combined = combined.with_columns(pl.col('ORGISSDTE').alias('FULLREL_DT'))

    if 'VALUEDTE' in combined.columns:
        combined = combined.with_columns(
            pl.col('VALUEDTE').map_elements(
                lambda v: ddmmyy8_from_str(str(v)) if v else None,
                return_dtype=pl.Date
            ).alias('VALUATION_DT')
        )

    # PAYEFDT string
    if 'PAYEFFDT' in combined.columns:
        combined = combined.with_columns(
            pl.col('PAYEFFDT').map_elements(payeffdt_format, return_dtype=pl.Utf8).alias('PAYEFDTO')
        )

    # Fix PAYEFDT day/month validity
    if 'PAYEFDTO' in combined.columns:
        combined = combined.with_columns(
            pl.col('PAYEFDTO').map_elements(fix_payefdt, return_dtype=pl.Date).alias('PAYEFDT')
        )

    # LASTTRAN date
    if 'LASTTRAN' in combined.columns:
        combined = combined.with_columns(
            pl.col('LASTTRAN').map_elements(
                lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                return_dtype=pl.Date
            ).alias('LASTTRAN')
        )

    # MATUREDT date
    if 'MATUREDT' in combined.columns:
        combined = combined.with_columns(
            pl.col('MATUREDT').map_elements(
                lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                return_dtype=pl.Date
            ).alias('MATUREDT')
        )

    # DAYARR
    if 'BLDATE' in combined.columns:
        combined = combined.with_columns(
            pl.col('BLDATE').map_elements(
                lambda v: (reptdate - sas_date_from_int(v)).days if v and sas_date_from_int(v) else None,
                return_dtype=pl.Int64
            ).alias('DAYARR')
        )

    # MTHARR (extended range)
    if 'DAYARR' in combined.columns:
        combined = combined.with_columns(
            pl.col('DAYARR').map_elements(compute_mtharr_lnpd, return_dtype=pl.Int64).alias('MTHARR')
        )
        combined = combined.with_columns(
            pl.when(pl.col('DAYARR') > 0)
            .then((pl.col('DAYARR') / 30.00050).map_elements(math.floor, return_dtype=pl.Int64))
            .otherwise(0)
            .alias('MTHARR_CCRIS')
        )

    # COLLAGE
    if 'COLLYEAR' in combined.columns:
        combined = combined.with_columns(
            pl.when(pl.col('COLLYEAR') > 0)
            .then(round((reptdate.year - pl.col('COLLYEAR') + 1) * 10) / 10)
            .otherwise(None)
            .alias('COLLAGE')
        )

    # REMAINMT
    if 'EXPRDATE' in combined.columns:
        combined = combined.with_columns(
            pl.col('EXPRDATE').map_elements(
                lambda v: compute_remainmt(reptdate, v) if v else 0,
                return_dtype=pl.Int64
            ).alias('REMAINMT')
        )

    # STATECD
    # (Simplified; full state/postcode/country logic from original SAS would
    #  require POSTCD, COUNTRYCD macros from PBBLNFMT/format files)
    if 'STATE' in combined.columns and 'BRANCH' in combined.columns:
        from PBBLNFMT import format_statecd  # placeholder import
        # combined = combined.with_columns(
        #     pl.col('STATE').map_elements(format_statecd, return_dtype=pl.Utf8).alias('STATECD')
        # )
        pass  # STATECD derivation depends on $STATEPOST., $STATECD. formats from PBBLNFMT

    # SECTFISS = SECTOR
    if 'SECTOR' in combined.columns:
        combined = combined.with_columns(pl.col('SECTOR').alias('SECTFISS'))

    # FISSPURP from CRISPURP via FISSCD map
    if 'CRISPURP' in combined.columns:
        combined = combined.with_columns(
            pl.col('CRISPURP').map_elements(
                lambda v: format_fisscd(str(v)) if v else '',
                return_dtype=pl.Utf8
            ).alias('FISSPURP')
        )

    # COLLDESC derived
    if 'COLLDESC' in combined.columns:
        combined = combined.with_columns([
            pl.col('COLLDESC').str.slice(0, 16).alias('MAKE'),
            pl.col('COLLDESC').str.slice(15, 21).alias('MODEL'),
            pl.col('COLLDESC').str.slice(39, 13).alias('REGNO'),
        ])

    # CENSUS derived
    if 'CENSUS' in combined.columns:
        combined = combined.with_columns([
            pl.col('CENSUS').alias('CENSUS0'),
            pl.col('CENSUS').map_elements(lambda v: f'{float(v):7.2f}'[0:2] if v else '', return_dtype=pl.Utf8).alias('CENSUS1'),
            pl.col('CENSUS').map_elements(lambda v: f'{float(v):7.2f}'[4:5].replace('.','') if v else '', return_dtype=pl.Utf8).alias('CENSUS4'),
        ])

    # Rename columns: NTBRCH -> BRANCH, LOANTYPE -> PRODUCT
    rename_map = {}
    if 'NTBRCH' in combined.columns and 'BRANCH' not in combined.columns:
        rename_map['NTBRCH'] = 'BRANCH'
    if 'LOANTYPE' in combined.columns and 'PRODUCT' not in combined.columns:
        rename_map['LOANTYPE'] = 'PRODUCT'
    if rename_map:
        combined = combined.rename(rename_map)

    # Alias fields
    alias_map = {
        'USER1': 'U1CLASSI', 'USER2': 'U2RACECO', 'USER3': 'U3MYCODE',
        'USER4': 'U4RESIDE', 'FLAG1': 'F1RELMOD', 'FLAG5': 'F5ACCONV',
        'BILTOT': 'TOTBNP', 'CENSUS': 'CENSUS0', 'BONUSANO': 'REFNOTENO',
    }
    for src, dst in alias_map.items():
        if src in combined.columns and dst not in combined.columns:
            combined = combined.with_columns(pl.col(src).alias(dst))

    drop_cols = [c for c in ['FLAG1', 'FLAG5', 'USER1', 'USER2', 'USER3', 'USER4',
                              'THISDATE', 'INTPYTD1', 'SITYPE', 'SIACCTNO',
                              'SM_STATUS', 'SM_DATE', '_IS_RV', 'LASTMM', 'PAYEFDTO'] if c in combined.columns]
    combined = combined.drop(drop_cols)

    return combined

# ---------------------------------------------------------------------------
# Merge LNPD with LNCOMM for HP/TL/RC logic
# ---------------------------------------------------------------------------

def build_lnpdx(lnpd: pl.DataFrame) -> pl.DataFrame:
    """Merge LNPD with LNCOMM and compute APPRLIMT, APPRLIM2, LNTYPE, UNDRAWN."""
    lncomm = pl.read_parquet(BNM1_LNCOMM_PARQUET)
    merged = lnpd.join(lncomm, on=['ACCTNO', 'COMMNO'], how='left')

    def compute_appr_type(row):
        prodcd  = row.get('PRODCD') or ''
        curbal  = row.get('CURBAL') or 0
        rebate  = row.get('REBATE') or 0
        intearn4 = row.get('INTEARN4') or 0
        commno  = row.get('COMMNO') or 0
        revovli = row.get('REVOVLI') or ''
        corgamt = row.get('CORGAMT') or 0
        ccuramt = row.get('CCURAMT') or 0
        orgbal  = row.get('ORGBAL') or 0
        balance = row.get('BALANCE') or 0
        feeamt  = row.get('FEEAMT') or 0
        cavaiamt = row.get('CAVAIAMT') or 0
        product = row.get('PRODUCT') or 0

        if prodcd == '34111':
            lntype = 'HP'
            apprlimt = curbal - (rebate + intearn4)
        else:
            lntype = 'TL'
            if commno > 0:
                if revovli == 'N':
                    apprlimt = corgamt
                else:
                    apprlimt = ccuramt
            else:
                apprlimt = orgbal

        if 800 <= product <= 899:
            pass
        else:
            if prodcd == '34190':
                lntype = 'RC'

        if prodcd not in ('34111', '34190', '34180', '34600'):
            lntype = 'TL'

        # APPRLIM2
        if commno not in (None, 0):
            if lntype == 'RC':
                apprlim2 = ccuramt
            elif lntype == 'TL':
                rleasamt = corgamt if (corgamt - cavaiamt) > corgamt else corgamt - cavaiamt
                apprlim2 = balance - feeamt + (apprlimt - rleasamt)
            else:
                apprlim2 = balance - feeamt
        else:
            if lntype == 'RC':
                apprlim2 = orgbal
            elif lntype == 'TL':
                apprlim2 = balance - feeamt
            else:
                apprlim2 = balance - feeamt

        if prodcd == '34111':
            apprlim2 = balance - feeamt

        unuseamt = row.get('UNUSEAMT') or 0
        undrawn = unuseamt if commno not in (None, 0) else 0

        return {'APPRLIMT': apprlimt, 'APPRLIM2': apprlim2, 'LNTYPE': lntype, 'UNDRAWN': undrawn}

    required = ['PRODCD', 'CURBAL', 'REBATE', 'INTEARN4', 'COMMNO', 'REVOVLI',
                'CORGAMT', 'CCURAMT', 'ORGBAL', 'BALANCE', 'FEEAMT', 'CAVAIAMT',
                'PRODUCT', 'UNUSEAMT']
    available = [c for c in required if c in merged.columns]

    result = merged.map_rows(lambda row: compute_appr_type(dict(zip(merged.columns, row))))
    # map_rows returns a DataFrame; we need to re-join
    result_df = merged.with_columns([
        pl.Series('APPRLIMT', [0.0] * len(merged)),
        pl.Series('APPRLIM2', [0.0] * len(merged)),
        pl.Series('LNTYPE', [''] * len(merged)),
        pl.Series('UNDRAWN', [0.0] * len(merged)),
    ])

    # Apply row-wise computation
    rows_data = merged.to_dicts()
    apprlimt_list, apprlim2_list, lntype_list, undrawn_list = [], [], [], []
    for row in rows_data:
        res = compute_appr_type(row)
        apprlimt_list.append(res['APPRLIMT'])
        apprlim2_list.append(res['APPRLIM2'])
        lntype_list.append(res['LNTYPE'])
        undrawn_list.append(res['UNDRAWN'])

    merged = merged.with_columns([
        pl.Series('APPRLIMT', apprlimt_list),
        pl.Series('APPRLIM2', apprlim2_list),
        pl.Series('LNTYPE', lntype_list),
        pl.Series('UNDRAWN', undrawn_list),
    ])

    return merged

# ---------------------------------------------------------------------------
# PAYFI: read payment effective date file
# ---------------------------------------------------------------------------

def read_payfi() -> pl.DataFrame:
    """Read PAYFI binary file: PD6 ACCTNO, PD3 NOTENO, PD6 PAYEFDX."""
    records = []
    if not PAYFI_PATH.exists():
        logger.warning(f"PAYFI file not found: {PAYFI_PATH}")
        return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'PAYEFDT': []})

    with open(PAYFI_PATH, 'rb') as f:
        data = f.read()

    offset = 0
    rec_len = 15  # PD6=4bytes, PD3=2bytes, gap, PD6=4bytes (approx; adjust as needed)
    # @001 ACCTNO PD6. = 4 bytes, @007 NOTENO PD3. = 2 bytes, @011 PAYEFDX PD6. = 4 bytes
    rec_len = 15
    while offset + rec_len <= len(data):
        chunk = data[offset:offset + rec_len]
        try:
            acctno  = packed_decimal_to_int(chunk[0:4])   # PD6 packed in 4 bytes (max 7 digits)
            noteno  = packed_decimal_to_int(chunk[6:8])   # PD3 packed in 2 bytes (max 5 digits)
            payefdx = packed_decimal_to_int(chunk[10:14]) # PD6 packed in 4 bytes

            paycy = str(int(str(payefdx).zfill(11)[:4]))
            paymm = int(str(payefdx).zfill(11)[7:9])
            paydd = int(str(payefdx).zfill(11)[9:11])

            if paymm == 2 and paydd > 29:
                paydd = 28
                if int(paycy) % 4 == 0:
                    paydd = 29
            elif paymm != 2 and paydd > 31:
                if paymm in (1, 3, 5, 7, 8, 10, 12):
                    paydd = 31
                elif paymm in (4, 6, 9, 11):
                    paydd = 30

            payefdt = date(int(paycy), paymm, paydd)
            records.append({'ACCTNO': acctno, 'NOTENO': noteno, 'PAYEFDT': payefdt})
        except Exception:
            offset += rec_len
            continue
        offset += rec_len

    df = pl.DataFrame(records) if records else pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'PAYEFDT': []})
    df = df.sort(['ACCTNO', 'NOTENO', 'PAYEFDT'], descending=[False, False, True])
    return df.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

# ---------------------------------------------------------------------------
# CIS: CISL loan race/DOB data
# ---------------------------------------------------------------------------

def load_cis() -> pl.DataFrame:
    """Load CIS from CISL.LOAN filtered to SECCUST='901'."""
    if not CISL_LOAN_PARQUET.exists():
        logger.warning(f"CISL LOAN not found: {CISL_LOAN_PARQUET}")
        return pl.DataFrame({'ACCTNO': [], 'DOBCIS': [], 'U2RACECO': []})

    cis = pl.read_parquet(CISL_LOAN_PARQUET)
    if 'SECCUST' in cis.columns:
        cis = cis.filter(pl.col('SECCUST') == '901')

    exprs = [pl.col('ACCTNO')]
    if 'BIRTHDAT' in cis.columns:
        def parse_dob(v):
            if not v:
                return None
            s = str(v)
            if len(s) < 8:
                return None
            try:
                dd = int(s[0:2])
                mm = int(s[2:4])
                yy = int(s[4:8])
                return date(yy, mm, dd)
            except Exception:
                return None
        exprs.append(pl.col('BIRTHDAT').map_elements(parse_dob, return_dtype=pl.Date).alias('DOBCIS'))

    if 'RACE' in cis.columns:
        exprs.append(pl.col('RACE').alias('U2RACECO'))

    cis = cis.select(exprs)
    return cis.unique(subset=['ACCTNO'], keep='first')

# ---------------------------------------------------------------------------
# WOFFTOT: Write-off HP data
# ---------------------------------------------------------------------------

def load_wofftot() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split WOFFTOT into records without and with NOTENO."""
    if not WOFF_WOFFTOT_PARQUET.exists():
        empty = pl.DataFrame({'ACCTNO': []})
        return empty, empty
    woff = pl.read_parquet(WOFF_WOFFTOT_PARQUET)
    woff0 = woff.filter(pl.col('NOTENO').is_null() if 'NOTENO' in woff.columns else pl.lit(True))
    woff1 = woff.filter(pl.col('NOTENO').is_not_null() if 'NOTENO' in woff.columns else pl.lit(False))
    return woff0, woff1

# ---------------------------------------------------------------------------
# OVDFT for ULOAN (monthly, week-4 only)
# ---------------------------------------------------------------------------

def build_ovdft1() -> pl.DataFrame:
    """Build OVDFT1 dataset for ULOAN processing (week-4)."""
    current = pl.read_parquet(MNITB_CURRENT_PARQUET)
    filtered = current.filter(
        (~pl.col('OPENIND').is_in(['B', 'C', 'P'])) &
        (pl.col('CURBAL') >= 0) &
        (pl.col('APPRLIMT') > 0)
    )
    if 'PRODUCT' in filtered.columns:
        filtered = filtered.filter(
            ~((pl.col('PRODUCT') == 167) & (pl.col('CURBAL') >= 0))
        )
    if 'SECTOR' in filtered.columns:
        filtered = filtered.with_columns(
            pl.col('SECTOR').cast(pl.Utf8).str.zfill(4).alias('SECTOR')
        )
    return filtered.select(['ACCTNO', 'SECTOR'])

# ---------------------------------------------------------------------------
# ULOAN processing (monthly, week-4 only)
# ---------------------------------------------------------------------------

def build_uloan(ctx: dict) -> pl.DataFrame | None:
    """Build ULOAN dataset if week-4."""
    if ctx['nowk'] != '4':
        return None
    reptmon = ctx['reptmon']
    nowk    = ctx['nowk']
    p = BNM_DIR / f'uloan{reptmon}{nowk}.parquet'
    if not p.exists():
        logger.warning(f"ULOAN file not found: {p}")
        return None

    uloan = pl.read_parquet(p)

    if 'CCRICODE' in uloan.columns:
        uloan = uloan.with_columns(
            pl.when(
                (pl.col('ACCTNO') >= 3000000000) & (pl.col('ACCTNO') <= 3999999999)
            )
            .then(pl.col('CCRICODE').cast(pl.Utf8).str.zfill(4))
            .otherwise(pl.col('CRISPURP') if 'CRISPURP' in uloan.columns else pl.lit(None))
            .alias('CRISPURP')
        )

    drop_cols = [c for c in ['ACCTYPE', 'AMTIND', 'CCRICODE', 'RLEASAMT', 'SECTOLD'] if c in uloan.columns]
    uloan = uloan.drop(drop_cols)
    if 'SECTORCD' in uloan.columns:
        uloan = uloan.rename({'SECTORCD': 'SECTFISS'})

    ovdft1 = build_ovdft1()
    uloan = uloan.join(ovdft1, on='ACCTNO', how='left')

    if 'SECTOR' in uloan.columns and 'SECTPORI' in uloan.columns:
        uloan = uloan.with_columns(
            pl.when(pl.col('SECTOR').is_null() | (pl.col('SECTOR') == ''))
            .then(pl.col('SECTPORI'))
            .otherwise(pl.col('SECTOR'))
            .alias('SECTOR')
        )

    if 'SECTPORI' in uloan.columns:
        uloan = uloan.drop(['SECTPORI'])

    return uloan

# ---------------------------------------------------------------------------
# CUM: average balance from MNICRM
# ---------------------------------------------------------------------------

def build_cum(ctx: dict) -> pl.DataFrame:
    """Build CUM dataset for average monthly balance."""
    reptmon  = ctx['reptmon']
    reptmon2 = ctx['reptmon2']
    nowk     = ctx['nowk']
    nowks    = ctx['nowks']
    reptday  = int(ctx['reptday'])

    cum_path  = MNICRM_DIR / f'ln{reptmon}{nowk}.parquet'
    prv_path  = MNICRM_DIR / f'ln{reptmon2}{nowks}.parquet'

    if not cum_path.exists():
        logger.warning(f"CUM path not found: {cum_path}")
        return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'CURAVMTH': [], 'CUBALYTD': []})

    cum = pl.read_parquet(cum_path).select(
        [c for c in ['ACCTNO', 'NOTENO', 'CUBALYTD', 'DAYYTD', 'CURBAL'] if True]
    )
    cum = cum.rename({'CUBALYTD': 'BAL', 'DAYYTD': 'DAY'})

    if prv_path.exists():
        prv = pl.read_parquet(prv_path).select(['ACCTNO', 'NOTENO', 'CUBALYTD', 'DAYYTD'])
        prv = prv.rename({'CUBALYTD': 'PRVBAL', 'DAYYTD': 'PRVDAY'})
        merged = prv.join(cum, on=['ACCTNO', 'NOTENO'], how='outer_coalesce')
    else:
        merged = cum.with_columns([
            pl.lit(None).cast(pl.Float64).alias('PRVBAL'),
            pl.lit(None).cast(pl.Int64).alias('PRVDAY'),
        ])

    reptmon2_int = int(reptmon2)
    if reptmon2_int == 12:
        merged = merged.with_columns(pl.col('BAL').alias('CUBAL'))
    else:
        merged = merged.with_columns(
            (pl.col('BAL').fill_null(0) - pl.col('PRVBAL').fill_null(0)).alias('CUBAL')
        )

    merged = merged.with_columns([
        pl.lit(reptday).alias('DAYMTD'),
        pl.when(pl.lit(reptday) != 0)
        .then(pl.col('CUBAL') / reptday)
        .otherwise(None)
        .alias('CURAVMTH'),
        pl.when(pl.col('BAL') > 0)
        .then(pl.col('BAL') / pl.col('DAY'))
        .otherwise(None)
        .alias('CUBALYTD'),
    ])

    return merged.select(['ACCTNO', 'NOTENO', 'CURAVMTH', 'CUBALYTD'])

# ---------------------------------------------------------------------------
# OVDFT sector for OD accounts
# ---------------------------------------------------------------------------

def build_ovdft_sector() -> pl.DataFrame:
    """Build OVDFT dataset with SECTOR from MNITB.CURRENT for OD loans."""
    current = pl.read_parquet(MNITB_CURRENT_PARQUET)
    current = current.filter(
        (~pl.col('OPENIND').is_in(['B', 'C', 'P'])) &
        (pl.col('CURBAL') < 0)
    )
    if 'SECTOR' in current.columns:
        current = current.with_columns(
            pl.col('SECTOR').cast(pl.Utf8).str.zfill(4).alias('SECTOR')
        )
    return current.select(['ACCTNO', 'SECTOR'])

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting EIBWLNW1")

    ctx = get_reptdate()
    reptmon  = ctx['reptmon']
    nowk     = ctx['nowk']
    reptyear = ctx['reptyear']
    reptdate = ctx['reptdate']
    sdate    = ctx['sdate']
    logger.info(f"REPTDATE={reptdate}, REPTMON={reptmon}, NOWK={nowk}")

    # -----------------------------------------------------------------------
    # GET_FEE: LNNOTE base
    # -----------------------------------------------------------------------
    logger.info("Loading LNNOTE (GET_FEE)")
    lnnote = get_fee(ctx)

    # -----------------------------------------------------------------------
    # HIST
    # -----------------------------------------------------------------------
    logger.info("Loading HIST")
    hist = load_hist(ctx)

    # -----------------------------------------------------------------------
    # REFNOTE
    # -----------------------------------------------------------------------
    logger.info("Loading REFNOTE")
    refnote = read_refnote()

    # -----------------------------------------------------------------------
    # Enrich LNNOTE
    # -----------------------------------------------------------------------
    logger.info("Enriching LNNOTE")
    lnnote = enrich_lnnote(lnnote, hist, refnote)

    # -----------------------------------------------------------------------
    # Merge LNNOTE with LNCOMM (approved limits, INTERDUE, etc.)
    # -----------------------------------------------------------------------
    logger.info("Merging LNNOTE with LNCOMM")
    lnnote = merge_lnnote_comm(lnnote)

    # -----------------------------------------------------------------------
    # NPL indicator
    # -----------------------------------------------------------------------
    logger.info("Loading NPL")
    npl = load_npl(ctx)
    lnnote = lnnote.join(npl, on=['ACCTNO', 'NOTENO'], how='left')

    # -----------------------------------------------------------------------
    # Base LOAN from BNM files
    # -----------------------------------------------------------------------
    logger.info("Loading base LOAN")
    loan = load_loan_base(ctx)

    # -----------------------------------------------------------------------
    # GP3 (week-4 only)
    # -----------------------------------------------------------------------
    logger.info("Applying GP3")
    loan = apply_gp3(loan, ctx)

    # -----------------------------------------------------------------------
    # OVERDFT: APPRLIM2, ASCORE, DAYARR
    # -----------------------------------------------------------------------
    logger.info("Loading OVERDFT")
    overdft = load_overdft(ctx)
    if 'APPRLIM2' in loan.columns:
        loan = loan.drop(['APPRLIM2'])
    loan = loan.join(overdft, on='ACCTNO', how='left')
    if 'APPRLIM2' not in loan.columns:
        loan = loan.with_columns(pl.lit(0.0).alias('APPRLIM2'))
    else:
        loan = loan.with_columns(pl.col('APPRLIM2').fill_null(0))

    # -----------------------------------------------------------------------
    # MISMLN VG
    # -----------------------------------------------------------------------
    logger.info("Merging MISMLN VG")
    loan = merge_mismln(loan, ctx)

    # -----------------------------------------------------------------------
    # Merge LOAN with LNNOTE
    # -----------------------------------------------------------------------
    logger.info("Merging LOAN with LNNOTE")
    # Drop duplicated columns from LNNOTE that exist in LOAN
    lnnote_merge_cols = [c for c in lnnote.columns
                         if c not in loan.columns or c in ('ACCTNO', 'NOTENO')]

    if 'LASTTRAN' in loan.columns:
        loan = loan.drop(['LASTTRAN'])
    if 'BIRTHDT' in loan.columns:
        loan = loan.drop(['BIRTHDT'])

    lnnote_sel = lnnote.select([c for c in lnnote.columns if c in lnnote_merge_cols])
    # Drop conflicting non-key columns
    overlap = [c for c in lnnote_sel.columns if c in loan.columns and c not in ('ACCTNO', 'NOTENO')]
    if overlap:
        lnnote_sel = lnnote_sel.drop(overlap)

    loan = loan.join(lnnote_sel, on=['ACCTNO', 'NOTENO'], how='left')

    # Rename aliases from LNNOTE
    alias_map = {
        'BILTOT': 'TOTBNP', 'PZIPCODE': 'CAGATAG',
        'USER1': 'U1CLASSI', 'USER2': 'U2RACECO', 'USER3': 'U3MYCODE', 'USER4': 'U4RESIDE',
        'FLAG1': 'F1RELMOD', 'FLAG5': 'F5ACCONV',
    }
    for src, dst in alias_map.items():
        if src in loan.columns and dst not in loan.columns:
            loan = loan.with_columns(pl.col(src).alias(dst))

    # LASTRAN: convert from packed integer
    if 'LASTTRAN' in loan.columns:
        loan = loan.with_columns(
            pl.col('LASTTRAN').map_elements(
                lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                return_dtype=pl.Date
            ).alias('LASTRAN')
        )

    # DOB from BIRTHDT
    if 'BIRTHDT' in loan.columns:
        loan = loan.with_columns(
            pl.col('BIRTHDT').map_elements(
                lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                return_dtype=pl.Date
            ).alias('DOBMNI')
        )

    # APPRDATE
    if 'APPRDATE' in loan.columns:
        loan = loan.with_columns(
            pl.col('APPRDATE').map_elements(
                lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                return_dtype=pl.Date
            ).alias('APPRDATE')
        )

    # MATUREDT
    if 'MATUREDT' in loan.columns:
        loan = loan.with_columns(
            pl.col('MATUREDT').map_elements(
                lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                return_dtype=pl.Date
            ).alias('MATUREDT')
        )

    # ASSMDATE
    if 'ASSMDATE' in loan.columns:
        loan = loan.with_columns(
            pl.col('ASSMDATE').map_elements(
                lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                return_dtype=pl.Date
            ).alias('ASSMDATE')
        )

    # ORGISSDTE from FRELEAS
    if 'FRELEAS' in loan.columns:
        loan = loan.with_columns(
            pl.col('FRELEAS').map_elements(
                lambda v: mmddyy_from_z11(v) if v and v != 0 else None,
                return_dtype=pl.Date
            ).alias('ORGISSDTE')
        )

    # MATUREDT = null if equals EXPRDATE
    if 'MATUREDT' in loan.columns and 'EXPRDATE' in loan.columns:
        loan = loan.with_columns(
            pl.when(pl.col('MATUREDT') == pl.col('EXPRDATE'))
            .then(None)
            .otherwise(pl.col('MATUREDT'))
            .alias('MATUREDT')
        )

    # -----------------------------------------------------------------------
    # Merge TOTPAY
    # -----------------------------------------------------------------------
    if LNFILE_TOTPAY_PARQUET.exists():
        totpay = pl.read_parquet(LNFILE_TOTPAY_PARQUET).drop(['DATE'] if 'DATE' in
                    pl.read_parquet(LNFILE_TOTPAY_PARQUET).columns else [])
        loan = loan.join(totpay, on=['ACCTNO', 'NOTENO'], how='left')

    # -----------------------------------------------------------------------
    # OD SECTOR
    # -----------------------------------------------------------------------
    logger.info("Building OD sector")
    ovdft_sec = build_ovdft_sector()
    if 'SECTOR' in loan.columns:
        loan = loan.drop(['SECTOR'])
    loan = loan.join(ovdft_sec, on='ACCTNO', how='left')

    # -----------------------------------------------------------------------
    # Build LN dataset (main computation step)
    # -----------------------------------------------------------------------
    logger.info("Building LN dataset")
    ln = build_ln(loan, ctx)

    # -----------------------------------------------------------------------
    # NAMEX merge
    # -----------------------------------------------------------------------
    logger.info("Loading NAMEX")
    namex = load_namex()
    ln = ln.join(namex, on='ACCTNO', how='left')

    # -----------------------------------------------------------------------
    # NOTEX merge
    # -----------------------------------------------------------------------
    logger.info("Loading NOTEX")
    notex = load_notex()
    ln = ln.join(notex, on=['ACCTNO', 'NOTENO'], how='left')

    # -----------------------------------------------------------------------
    # LNBL dataset
    # -----------------------------------------------------------------------
    logger.info("Building LNBL")
    lnbl = build_lnbl(ctx)

    # -----------------------------------------------------------------------
    # LNPD dataset
    # -----------------------------------------------------------------------
    logger.info("Building LNPD")
    lnpd_raw = build_lnpd(ctx)
    lnpdx = build_lnpdx(lnpd_raw)

    # Merge MTD interest
    if BNM2_MTDINT_PARQUET.exists():
        mtd = pl.read_parquet(BNM2_MTDINT_PARQUET).select(['ACCTNO', 'NOTENO', 'MTDINT'])
        lnpdx = lnpdx.join(mtd, on=['ACCTNO', 'NOTENO'], how='left')
        if 'MTDINT' in lnpdx.columns:
            lnpdx = lnpdx.with_columns(pl.col('MTDINT').fill_null(0))

    # Merge NAMEX into LNPD (drop DATEREGV)
    namex_lnpd = namex.drop(['DATEREGV'] if 'DATEREGV' in namex.columns else [])
    lnpdx = lnpdx.join(namex_lnpd, on='ACCTNO', how='left')

    # Save LOAN.LNPD
    lnpd_out = LOAN_OUT_DIR / f'lnpd{reptmon}{nowk}{reptyear}.parquet'
    lnpdx.write_parquet(lnpd_out)
    logger.info(f"Written LNPD: {lnpd_out}")

    # -----------------------------------------------------------------------
    # HPPD
    # -----------------------------------------------------------------------
    hp_products = set(HP_ALL)
    hppd = lnpdx.filter(
        pl.col('PRODUCT').is_in(list(hp_products)) | (pl.col('PRODUCT') == 392)
    )
    drop_hppd = [c for c in ['NAME', 'PRIMOFHP', 'MO_MAIN_DT', 'RR_IL_RECLASS_DT'] if c in hppd.columns]
    hppd = hppd.drop(drop_hppd)
    hppd_out = HP_OUT_DIR / f'hppd{reptmon}{nowk}{reptyear}.parquet'
    hppd.write_parquet(hppd_out)
    logger.info(f"Written HPPD: {hppd_out}")

    # HPCO
    if BNM1_HPCOMP_PARQUET.exists():
        hpcomp = pl.read_parquet(BNM1_HPCOMP_PARQUET)
        hpco_out = HP_OUT_DIR / f'hpco{reptmon}{nowk}{reptyear}.parquet'
        hpcomp.write_parquet(hpco_out)
        logger.info(f"Written HPCO: {hpco_out}")

    # -----------------------------------------------------------------------
    # CUM average balance
    # -----------------------------------------------------------------------
    logger.info("Building CUM")
    cum = build_cum(ctx)

    # -----------------------------------------------------------------------
    # Save LOAN.LN (intermediate, before CIS/PAYFI merge)
    # -----------------------------------------------------------------------
    ln_intermediate = ln.join(cum, on=['ACCTNO', 'NOTENO'], how='left')
    drop_ln = [c for c in ['NAME', 'LASTRAN', 'PAYEFFDT', 'STAFFNO', 'SBA', 'FEEAMT2', 'MORTGIND'] if c in ln_intermediate.columns]
    ln_intermediate = ln_intermediate.drop(drop_ln)
    if 'REMAINMT' in ln_intermediate.columns:
        ln_intermediate = ln_intermediate.rename({'REMAINMT': 'REMMFISS'})
    if 'REMAINMH' in ln_intermediate.columns:
        ln_intermediate = ln_intermediate.rename({'REMAINMH': 'REMAINMT'})

    ln_intermediate_out = LOAN_OUT_DIR / f'ln{reptmon}{nowk}{reptyear}_pre.parquet'
    ln_intermediate.write_parquet(ln_intermediate_out)

    # LNPD + CUM
    lnpd_cum = lnpdx.join(cum, on=['ACCTNO', 'NOTENO'], how='left')
    if 'LASTTRAN' in lnpd_cum.columns and 'SDATE' in lnpd_cum.columns:
        pass
    else:
        if 'CURAVMTH' in lnpd_cum.columns:
            lnpd_cum = lnpd_cum.with_columns(
                pl.when(
                    pl.col('LASTTRAN').is_not_null() &
                    (pl.col('LASTTRAN') < pl.lit(sdate))
                )
                .then(0.0)
                .otherwise(pl.col('CURAVMTH'))
                .alias('CURAVMTH')
            )

    lnpd_cum_out = LOAN_OUT_DIR / f'lnpd{reptmon}{nowk}{reptyear}.parquet'
    drop_lnpd_cum = [c for c in ['NAME', 'RSN', 'CPNSTDTE'] if c in lnpd_cum.columns]
    lnpd_cum = lnpd_cum.drop(drop_lnpd_cum)
    lnpd_cum.write_parquet(lnpd_cum_out)

    # -----------------------------------------------------------------------
    # ULOAN (monthly, week-4)
    # -----------------------------------------------------------------------
    logger.info("Building ULOAN")
    uloan = build_uloan(ctx)
    if uloan is not None:
        uloan_out = LOAN_OUT_DIR / f'uloan{reptmon}{nowk}{reptyear}.parquet'
        uloan.write_parquet(uloan_out)
        logger.info(f"Written ULOAN: {uloan_out}")

    # -----------------------------------------------------------------------
    # EIBWLNW2 combined section: CIS, PAYFI, final LN/HP outputs
    # -----------------------------------------------------------------------
    logger.info("EIBWLNW2 section: loading CIS")
    cis = load_cis()

    # Reload LN intermediate
    ln2 = pl.read_parquet(ln_intermediate_out)
    if 'REMAINMT' in ln2.columns:
        ln2 = ln2.rename({'REMAINMT': 'REMMFISS'})
    if 'PAYEFDT' in ln2.columns:
        ln2 = ln2.rename({'PAYEFDT': 'PAYEFDTO'})
    if 'BIRTHDT' in ln2.columns:
        ln2 = ln2.rename({'BIRTHDT': 'DOBMNI'})
    if 'U2RACECO' in ln2.columns:
        ln2 = ln2.drop(['U2RACECO'])

    # Fix PAYEFDTO
    if 'PAYEFDTO' in ln2.columns:
        ln2 = ln2.with_columns(
            pl.col('PAYEFDTO').map_elements(fix_payefdt, return_dtype=pl.Date).alias('PAYEFDT')
        )

    # Merge CIS
    ln2 = cis.join(ln2, on='ACCTNO', how='right')
    if 'REMAINMH' in ln2.columns:
        ln2 = ln2.rename({'REMAINMH': 'REMAINMT'})

    # PAYFI merge
    logger.info("Loading PAYFI")
    payfi = read_payfi()
    if not payfi.is_empty():
        if 'PAYEFDT' in ln2.columns:
            ln2 = ln2.drop(['PAYEFDT'])
        ln2 = payfi.join(ln2, on=['ACCTNO', 'NOTENO'], how='right')

    # Build HP dataset
    hp = ln2.filter(
        pl.col('PRODUCT').is_in(list(hp_products)) | (pl.col('PRODUCT') == 392)
    )
    drop_hp = [c for c in ['LASTRAN', 'PAYEFFDT', 'CUBALYTD', 'CURAVMTH',
                            'MO_MAIN_DT', 'RR_IL_RECLASS_DT'] if c in hp.columns]
    hp = hp.drop(drop_hp)

    # Final LN output
    ln_final = ln2.with_columns(pl.lit(None).alias('_drop_orgissdte'))
    drop_ln_final = [c for c in ['ORGISSDTE', '_drop_orgissdte'] if c in ln_final.columns]
    ln_final = ln_final.drop([c for c in drop_ln_final if c in ln_final.columns])

    # Write LN (exclude HP write-off products)
    hp_wo_products = [678, 679, 698, 699, 983, 993, 996]
    ln_excl = ln_final.filter(~pl.col('PRODUCT').is_in(hp_wo_products))
    ln_all  = ln_final  # output LN&REPTMON&NOWK&REPTYEAR includes all

    ln_out = LOAN_OUT_DIR / f'ln{reptmon}{nowk}{reptyear}.parquet'
    ln_excl.write_parquet(ln_out)
    logger.info(f"Written LN: {ln_out}")

    # LNPD final (merge CIS, drop DOBCIS, ORGISSDTE)
    lnpd_final = pl.read_parquet(lnpd_cum_out)
    if 'U2RACECO' in lnpd_final.columns:
        lnpd_final = lnpd_final.drop(['U2RACECO'])
    if 'ORGISSDTE' in lnpd_final.columns:
        lnpd_final = lnpd_final.drop(['ORGISSDTE'])
    lnpd_final = cis.join(lnpd_final, on='ACCTNO', how='right')
    if 'DOBCIS' in lnpd_final.columns:
        lnpd_final = lnpd_final.drop(['DOBCIS'])

    lnpd_final_out = LOAN_OUT_DIR / f'lnpd{reptmon}{nowk}{reptyear}.parquet'
    lnpd_final.write_parquet(lnpd_final_out)
    logger.info(f"Written LNPD final: {lnpd_final_out}")

    # -----------------------------------------------------------------------
    # HP WOFF merge
    # -----------------------------------------------------------------------
    logger.info("Building HP write-off dataset")
    wofftot0, wofftot1 = load_wofftot()

    # Merge WOFFTOT (without NOTENO) into HP by ACCTNO
    if not wofftot0.is_empty():
        drop_noteno = ['NOTENO'] if 'NOTENO' in wofftot0.columns else []
        hp = hp.join(wofftot0.drop(drop_noteno), on='ACCTNO', how='left')

    # Merge WOFFTOT1 (with NOTENO) into HP by ACCTNO+NOTENO
    if not wofftot1.is_empty():
        hp = hp.join(wofftot1, on=['ACCTNO', 'NOTENO'], how='left')

    # Split HP into HPWO (write-off products) and HP proper
    hp_proper = hp.filter(~pl.col('PRODUCT').is_in(hp_wo_products))
    hpwo_ds   = hp.filter(pl.col('PRODUCT').is_in(hp_wo_products))

    hp_out = HP_OUT_DIR / f'hp{reptmon}{nowk}{reptyear}.parquet'
    hp_proper.write_parquet(hp_out)
    logger.info(f"Written HP: {hp_out}")

    hpwo_out = HP_OUT_DIR / f'hpwo{reptmon}{nowk}{reptyear}.parquet'
    hpwo_ds.write_parquet(hpwo_out)
    logger.info(f"Written HPWO: {hpwo_out}")

    # -----------------------------------------------------------------------
    # %PROCESS: Copy HP to HPWO dir for specific months (02,05,08,11) week-4
    # -----------------------------------------------------------------------
    if reptmon in ('02', '05', '08', '11') and nowk == '4':
        hpwo_process_out = HPWO_OUT_DIR / f'hp{reptmon}{nowk}{reptyear}.parquet'
        hp_proper.write_parquet(hpwo_process_out)
        logger.info(f"Written HP to HPWO dir: {hpwo_process_out}")

    # -----------------------------------------------------------------------
    # LNBL final save
    # -----------------------------------------------------------------------
    if not lnbl.is_empty():
        lnbl_out = LOAN_OUT_DIR / f'lnbl{reptmon}{nowk}{reptyear}.parquet'
        lnbl.write_parquet(lnbl_out)
        logger.info(f"Written LNBL: {lnbl_out}")

    # -----------------------------------------------------------------------
    # CPORT equivalent: write final FTP files as parquet (LNBL, LNPD, ULOAN, LN)
    # -----------------------------------------------------------------------
    logger.info("Writing FTP transfer files (LNDATAWH)")
    ftp_dir = LOAN_OUT_DIR / 'lnftp'
    ftp_dir.mkdir(exist_ok=True)

    for src, name in [
        (LOAN_OUT_DIR / f'lnbl{reptmon}{nowk}{reptyear}.parquet', 'lnbl'),
        (lnpd_final_out, 'lnpd'),
        (uloan_out if uloan is not None else None, 'uloan'),
        (ln_out, 'ln'),
    ]:
        if src and Path(src).exists():
            import shutil
            dst = ftp_dir / Path(src).name
            shutil.copy2(src, dst)

    logger.info("Writing FTP transfer files (HPDATAWH)")
    hp_ftp_dir = HP_OUT_DIR / 'hpftp'
    hp_ftp_dir.mkdir(exist_ok=True)
    for p in [hp_out, hpwo_out, hppd_out]:
        if Path(p).exists():
            import shutil
            shutil.copy2(p, hp_ftp_dir / Path(p).name)

    logger.info("EIBWLNW1 completed successfully")


if __name__ == '__main__':
    main()
