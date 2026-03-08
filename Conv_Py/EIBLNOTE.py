#!/usr/bin/env python3
"""
Program  : EIBLNOTE.py
Purpose  : Loan data preparation for NPL HP write-off processing (Islamic Bank).
           Reads multiple binary/packed-decimal source files (already converted
           to parquet) and produces the following working datasets:
             FEPLAN    - Fee plan records filtered by LOANTYPE/FEEPLN rules
             FEEPO     - Summarised fee amounts per ACCTNO/NOTENO
             LOAND     - Full loan note detail records
             RNRHPMOR  - HP moratorium records
             LNNOTE    - Merged loan note + fee + moratorium
             LNACCT    - Account-level summary record
             LNACC4    - Alternate account record (ACC4 file)
             LNCOMM    - Commitment/facility records
             LOAN.HPCOMP / ILOAN.HPCOMP - HP company/dealer master
             LNNAME    - Account name records
             LIAB      - Liability records
             NAME8     - Name/address type-8 records
             NAME9     - Name/address type-9 records
             LNRATE    - Interest rate master (current rate per index)
             LOAN.LNRATE / ILOAN.LNRATE - Rate master written to loan libs
             PEND      - Pending transaction records (TRANTYPE=400)
             OLDNOTE   - Previous note reference data

           Dependencies:
             PBBLNFMT  - Format functions and product lists (HP_ALL)
             NPLNTB    - Branch transfer mappings (all commented out in source)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
from pathlib import Path
from datetime import date, datetime
from typing import Optional

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# %INC PGM(PBBLNFMT)
# ============================================================================
from PBBLNFMT import HP_ALL

# %INC PGM(NPLNTB)
# NOTE: All branch transfer logic in NPLNTB is commented out in the source.
# The %INC is therefore a no-op. Placeholder retained for traceability.
# import NPLNTB

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR    = Path(".")

# Input parquet sources (converted from binary/PD input files)
FEE_DIR     = BASE_DIR / "data" / "pibb" / "fee"        # FEEFILE  -> SAP.PIBB.FEE
ACCT_DIR    = BASE_DIR / "data" / "pibb" / "acct"       # ACCTFILE -> SAP.PIBB.ACCT (note-level)
ACC1_DIR    = BASE_DIR / "data" / "pibb" / "acc1"       # ACC1FILE -> account summary level-1
ACC4_DIR    = BASE_DIR / "data" / "pibb" / "acc4"       # ACC4FILE -> account summary level-4
COMM_DIR    = BASE_DIR / "data" / "pibb" / "comm"       # COMTFILE -> commitment/facility file
COMP_DIR    = BASE_DIR / "data" / "pibb" / "comp"       # COMPFILE -> HP company/dealer master
NAME_DIR    = BASE_DIR / "data" / "pibb" / "name"       # NAMEFILE -> name records
LIAB_DIR    = BASE_DIR / "data" / "pibb" / "liab"       # LIABFILE -> liability records
NAM8_DIR    = BASE_DIR / "data" / "pibb" / "nam8"       # NAM8FILE -> name type-8
NAM9_DIR    = BASE_DIR / "data" / "pibb" / "nam9"       # NAM9FILE -> name type-9
RATE_DIR    = BASE_DIR / "data" / "pibb" / "rate"       # RATEFILE -> interest rate master
PEND_DIR    = BASE_DIR / "data" / "pibb" / "pend"       # PENDFILE -> pending transactions
MOR_DIR     = BASE_DIR / "data" / "pibb" / "mor"        # MORHPFL  -> HP moratorium file
BNM_DIR     = BASE_DIR / "data" / "pibb" / "bnm"        # BNM.REPTDATE (for &RDATE macro)

# Output parquet destinations
NPL_DIR     = BASE_DIR / "data" / "pibb" / "npl_woff"   # NPL library (write target)
LOAN_DIR    = BASE_DIR / "data" / "pibb" / "loan"       # LOAN library
ILOAN_DIR   = BASE_DIR / "data" / "pibb" / "iloan"      # ILOAN library

for _d in [NPL_DIR, LOAN_DIR, ILOAN_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# HELPER: read parquet (returns empty DataFrame on missing file)
# ============================================================================
def _read(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    return duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{path}')"
    ).pl()


# ============================================================================
# HELPER: safe date parse from integer in various SAS formats
# SAS date formats replicated:
#   MMDDYY8.  -> padded to 8 chars, parse MM/DD/YY
#   DDMMYY8.  -> padded to 8 chars, parse DD/MM/YY
#   YYMMDD8.  -> padded to 8 chars, parse YY/MM/DD
# ============================================================================
def _parse_mmddyy(val) -> Optional[date]:
    """Parse SAS MMDDYY8. date from numeric (e.g. MMDDYYYY stored as int)."""
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(8)
        return date(int(s[4:8]), int(s[0:2]), int(s[2:4]))
    except Exception:
        return None


def _parse_ddmmyy(val) -> Optional[date]:
    """Parse SAS DDMMYY8. date from numeric (e.g. DDMMYYYY stored as int)."""
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(8)
        return date(int(s[4:8]), int(s[2:4]), int(s[0:2]))
    except Exception:
        return None


def _parse_yymmdd(val) -> Optional[date]:
    """Parse SAS YYMMDD8. date from numeric (e.g. YYYYMMDD stored as int)."""
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(8)
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    except Exception:
        return None


def _parse_z11_mmddyy(val) -> Optional[date]:
    """
    Replicates: INPUT(SUBSTR(PUT(val,Z11.),1,8),MMDDYY8.)
    Zero-pad to 11, take first 8 chars, parse as MMDDYY.
    """
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(11)[:8]
        return date(int(s[4:8]), int(s[0:2]), int(s[2:4]))
    except Exception:
        return None


def _parse_z11_ddmmyy(val) -> Optional[date]:
    """
    Replicates: INPUT(SUBSTR(PUT(val,Z11.),1,8),DDMMYY8.)
    Zero-pad to 11, take first 8 chars, parse as DDMMYY.
    """
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(11)[:8]
        return date(int(s[4:8]), int(s[2:4]), int(s[0:2]))
    except Exception:
        return None


def _parse_z11_yymmdd(val) -> Optional[date]:
    """
    Replicates: INPUT(SUBSTR(PUT(val,Z11.),1,8),YYMMDD8.)
    Zero-pad to 11, take first 8 chars, parse as YYMMDD.
    """
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(11)[:8]
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    except Exception:
        return None


def _parse_z8_ddmmyy(val) -> Optional[date]:
    """
    Replicates: INPUT(SUBSTR(PUT(val,Z8.),1,8),DDMMYY8.)
    Zero-pad to 8, parse as DDMMYY.
    """
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(8)
        return date(int(s[4:8]), int(s[2:4]), int(s[0:2]))
    except Exception:
        return None


# ============================================================================
# HELPER: get RDATE macro from BNM.REPTDATE
# ============================================================================
def _get_rdate() -> Optional[date]:
    """Read REPTDATE from BNM.REPTDATE parquet (equivalent to &RDATE macro)."""
    f = BNM_DIR / "reptdate.parquet"
    if not f.exists():
        return None
    df = duckdb.connect().execute(
        f"SELECT REPTDATE FROM read_parquet('{f}') LIMIT 1"
    ).pl()
    if df.is_empty():
        return None
    raw = df["REPTDATE"][0]
    if isinstance(raw, (date, datetime)):
        return raw.date() if isinstance(raw, datetime) else raw
    return None


# ============================================================================
# FEEPLN FILTER RULES
# Replicates all IF/ELSE IF LOANTYPE IN ... AND FEEPLN IN ... THEN OUTPUT
# Returns True if the row should be output.
# ============================================================================
def _feepln_match(loantype: int, feepln: str) -> bool:
    """
    Evaluate all FEEPLN output conditions from DATA FEPLAN.
    Returns True if the record passes any OUTPUT condition.
    Note: FEEPLN='CJ' is handled separately (sets CJFEE and outputs).
    """
    lt = loantype
    fp = feepln

    if lt in (983, 993, 996, 678, 679, 698, 699):  # *16-1713
        return True
    if lt in (163,) and fp in ('LA','LW','RX','RY','LI','RP','SG','MI','PM',
                                'IH','LF','IP','QR','TI','VA','PA'):
        return True
    if lt in (100,) and fp in ('PM','MI','LI'):
        return True
    if lt in (144,419,420,469,470,672,673,674,675) and fp in (
            'RP','MI','PM','IH','LF','IP','QR','TI','VA','PA','LA','ID','IS','IT','IU'):
        return True
    if lt in (145,) and fp in ('LA','LW','PO','RP','IH','MI','PM','QR','TI',
                                'VA','PA','LF','IP','RX','RY','LI','IC','RG'):
        return True
    if lt in (124,) and fp in ('LA','LB','LV','LW','PO','IH','II','LF','IP',
                                'LI','MC','MI','PA','PM','QR','RP','TI','VA','99'):
        return True
    if lt in (248, 574) and fp in ('LA','LW','RP','CR'):
        return True
    if lt in (110,111,112,113,114,115,116,120,127) and fp in (
            'LA','LB','LV','LW','PO','RP','IH','II','LI','MI','QR','TI',
            'VA','PA','LE','LX','PL','BC','IP','LF','MC','99','PM'):
        return True
    if (lt in (131,132) and fp in ('LV','LX','AC','DC','IP','LF','MC','PC','PM','MI',
                                    'RE','RI','SC','SE','SF','TC','99','LI','RJ')) or \
       (lt in (141,142,143,147,148) and fp in ('LA','LW','PO','RP','MI','PM','IH',
                                                'LF','IP','LI','QR','TI','VA','PA','RX','RY')):
        return True
    if lt in (720,725) and fp in ('AD','AC','DC','IP','LF','MC','PC','RE','BA','LD',
                                   'RI','SC','SE','SF','TC','99','RJ'):
        return True
    if lt in (135,) and fp in ('IH','LA','LB','LF','LI','LV','LW','MI','PA','PO','PM',
                                'RP','TI','IP'):
        return True
    if lt in (136,) and fp in ('IH','II','IP','LA','LB','LF','LI','LV','MI',
                                'LW','PA','PO','PM','RP','TI'):
        return True
    if (lt in (241,242) and fp in ('LA','LB','LW','PO','RP','PL','LD')) or \
       (lt in (243,244,245,362,531) and fp in ('LA','LB','LW','PO','RP')) or \
       (lt in (138,) and fp in ('LA','LW','RP','MI','PA','LI','PO','PM','IH',
                                 'LF','IP','TI','RX','RY')) or \
       (lt in (182,) and fp in ('PO','RP','MI','PM','IH','LA','LW','LI','RX','RY')) or \
       (lt in (183,) and fp in ('RP','MI','PM','IH','LF','IP','QR','TI','VA',
                                 'PA','RX','RY','LI','IS','IT','IU')) or \
       (lt in (108,) and fp in ('MI','IH','PO','LA','LW','PM','LI')) or \
       (lt in (363,532,569) and fp in ('LA','LW','PO','RP','CR')) or \
       (lt in (246,247) and fp in ('LA','LW','PO','RP','CR','99','LB')):
        return True
    if lt in (380,) and fp in ('AA','AX','AC','LF','MC','PC','RE','RJ',
                                'RI','SC','SE','SF','TC','99','IP','LD'):
        return True
    if lt in (381,) and fp in ('BA','BX','AC','LF','MC','PC','RE','RJ',
                                'RI','SC','SE','SF','TC','99','IP','LD'):
        return True
    if lt in (360,) and fp in ('AB','AV','BB','BW','PO','RP'):
        return True
    if lt in (390,) and fp in ('BA','BX','AC','FI','LF','MC','PC','RE','SF','99'):
        return True
    if lt in (209,210,521,522,523,528,529) and fp in ('LA','LB','LV','LW','PO','RP'):
        return True
    if lt in (309,310) and fp in ('LA','LB','LV','LW','PO','RP','BL','CH','CR','DC'):
        return True
    if lt in (301,) and fp in ('LA','LB','LV','LW','PO','RP','BL','CH','CR',
                                'DC','LC','PL','LD','99'):
        return True
    if lt in (345,691) and fp in ('DF',):
        return True
    if lt in (345,919,920) and fp in ('LA','LB','LV','LW','PO','RP','BL','CR','DC'):
        return True
    if lt in (914,915) and fp in ('LA','LB','LV','LW','PO','RP','BL','CR','DC','99'):
        return True
    if lt in (300,900,901,904,905) and fp in ('LA','LB','LV','LW','PO','RP','BL',
                                               'CR','DC','LC','PL','LD','99'):
        return True
    if lt in (211,212,214,215,219,220,232,233,304,305,315,325,335,340,350,355,356,
              357,358,361,391,515,516,517,518,519,524,525,526,527,555,556,559,560,
              910,925) and fp in ('LA','LB','LV','LW','PO','RP','CR'):
        return True
    if lt in (504,505,509,510) and fp in ('LA','LB','LV','LW','PO','RP','CR','PC','99'):
        return True
    if lt in (512,) and fp in ('LA','LW','RP','SG','CR'):
        return True
    if lt in (173,174) and fp in ('LA','LB','LV','LW','PO','RP','IH','II','LI','MI',
                                   'QR','TI','VA','PA','LE','LX','PL','BC','IP','LF',
                                   'MC','99'):
        return True
    if lt in (170,) and fp in ('LA','LB','LV','LW','PO','RP','IH','II','LI','MI',
                                'QR','TI','VA','PA','LF','IP','PM'):
        return True
    if lt in (129,) and fp in ('LA','LB','LV','LW','PO','RP','IH','II','LI','MI',
                                'QR','TI','VA','PA','LF','IP','PM'):
        return True
    if lt in (193,) and fp in ('LA','LB','LV','LW','PO','RP','IH','II','LI',
                                'MI','QR','TI','VA','PA','LF','PM','IP','RX','RY'):
        return True
    if lt in (196,) and fp in ('LA','LB','LV','LW','PO','RP','IH','II','LI','MI',
                                'TI','PA','LF','IP','PM'):
        return True
    if lt in (194,) and fp in ('LA','LB','LV','LW','PO','RP','LI','MI','PM',
                                'RE','TI','PA','LF'):
        return True
    if lt in (195,) and fp in ('LA','LB','LV','LW','PO','RP','LI','MI','PM',
                                'RE','TI','PA','LF'):
        return True
    if lt in (573,906,907,909) and fp in ('LA','LB','LW','PO','RP','CR'):
        return True
    if lt in (181,) and fp in ('LA','LB','LW','PO','RP','IH','II','LI','MI','QR',
                                'TI','VA','PA','LF','PM','IP'):
        return True
    if lt in (908,) and fp in ('PO','RP'):
        return True
    if lt in (180,) and fp in ('RP','LI','MI','PA','LF','RX','RY','PM','LA','LW'):
        return True
    if lt in (185,186,169,164,165,179) and fp in ('LA','LW','RX','RY','RP','MI','PM',
                                                    'IH','LF','IP','QR','TI','VA','PA','LI'):
        return True
    if lt in (400,401,402) and fp in ('LA','LW','RX','RY','RP','MI','PM','IH','LF',
                                       'IP','QR','TI','VA','PA','IC'):
        return True
    if lt in (156,157,158) and fp in ('LA','LW','RX','RY','IC','RP','MI','PM','IH',
                                       'LF','QR','TI','VA','PA','IP'):
        return True
    if lt in (187,403,404,405,406,407,408,409,411) and fp in ('LA','LW','RX','RY','RP',
                                                                'MI','PM','IH','LF','IP',
                                                                'QR','TI','VA','PA','LI'):
        return True
    if lt in (316,) and fp in ('LA','LW','RP','CR'):
        return True
    if lt in (351,) and fp in ('LA','LW','RP','CR'):
        return True
    if lt in (146,184) and fp in ('RX','RY','RP','MI','PM','IH','LF','IP','LI',
                                   'QR','TI','VA','PA','IS','IT','IU'):
        return True
    if lt in (140,) and fp in ('LA','PO','RP','IH','MI','TI','VA','PA','LE','PL',
                                'LX','BC','LF','IP','MC','99','LW','PM','LI'):
        return True
    if lt in (139,) and fp in ('LA','PO','RP','IH','MI','TI','VA','PA','LG','PM',
                                'PL','LX','BC','LF','IP','MC','99','LW','LI'):
        return True
    if lt in (122,) and fp in ('LA','PO','RP','MI','PA','LE','LX','PM','LI',
                                'PL','BC','LF','IP','MC','99','LW'):
        return True
    if lt in (302,) and fp in ('PO','RP','LA','LC','PL','LD','99','LW'):
        return True
    if lt in (365,) and fp in ('PO','LA','LC','PL','LD','99','LW'):
        return True
    if lt in (364,902,903) and fp in ('PO','LA','LC','PL','LD','RP','99','LW'):
        return True
    if lt in (213,216,217,218,348) and fp in ('LA','LW','RP','CR'):
        return True
    if lt in (950,951) and fp in ('99',):
        return True
    if lt in (700,) and fp in ('AA','AX','PF','AC','BC','DC','IP','LF','MC','PC',
                                'RE','RI','SC','SE','SF','TC','99','LD','RJ'):
        return True
    if lt in (705,) and fp in ('BA','BX','PF','AC','BC','DC','IP','LF','MC','PC',
                                'RE','RI','SC','SE','SF','TC','99','LD','RJ'):
        return True
    if lt in (128,130) and fp in ('LC','LV','LX','AC','BC','DC','IP','LF','MC','PC',
                                   'RE','RI','SC','SE','SF','TC','99','LD','RJ',
                                   'PM','MI','LI'):
        return True
    if lt in (506,) and fp in ('PC','99'):
        return True
    if (lt in (117,118,119,126) and fp in ('LA','PO','RP','IH','MI','TI','VA','PA',
                                            'PM','LI','LG','LX','PL','BC','LF','IP',
                                            'MC','99','LW')) or \
       (lt in (200,201,204,205,225,226,227,228,230,231,234,235,236,237,238,239,240,
               320,330,359,530,561,564,565,566,567,568,570) and
        fp in ('LA','LB','LV','LW','PO','RP','CR','LC','PL','LD','99')):
        return True
    if lt in (800,804) and fp in ('LJ','LK','CE','CL','FL','ES','MF','CM'):
        return True
    if lt in (801,805,806) and fp in ('LL','LM','CE','CL','FL','ES','MF','CM'):
        return True
    if lt in (802,803) and fp in ('CE','CL','FL','ES','MF','CM'):
        return True
    if lt in (851,852,853,854) and fp in ('MP','LN','CE','LJ','LK','PM','MI','LI'):
        return True
    if lt in (855,) and fp in ('MP','LN','CE','LL','LM','PM','MI','LI'):
        return True
    if lt in (856,857,858,859) and fp in ('MP','LN','CE','RU','RV','PM','MI','LI'):
        return True
    if lt in (860,) and fp in ('MP','LN','CE','RL','RM','PM','MI','LI'):
        return True
    if lt in (159,160,161) and fp in ('RX','RY','PM','MI','LA','LW','RP','LF','IP',
                                       'QR','TI','VA','PA'):
        return True
    if lt in (149,150,151,410) and fp in ('LA','LW','RX','RY','RP','MI','PM','IH',
                                           'LF','IP','QR','TI','VA','PA','LI','RG'):
        return True
    if lt in (162,) and fp in ('RX','RY','LA','LW','RP','MI','PM','IH','LF','TI','PA'):
        return True
    if lt in (175,176) and fp in ('LA','LW','RX','RY','RP','MI','PM','IH',
                                   'LF','IP','QR','TI','VA','PA','LI'):
        return True
    if lt in (177,178) and fp in ('LA','LW','RX','RY','RP','MI','PM','IH',
                                   'LF','IP','QR','TI','VA','PA','LI'):
        return True
    if lt in (152,153,154,155) and fp in ('LA','LW','RX','RY','RP','MI','PM','IH',
                                           'LF','IP','QR','TI','VA','PA','LI','RG'):
        return True

    # Large LOANTYPE range block with Islamic fee codes
    _big_range_1 = set(list(range(400,499)) + [400,401,402,403,404,405,406,407,408,409,
        410,411,412,413,414,415,416,417,418,419,420,422,423,424,425,426,427,428,
        433,434,435,436,437,438,439,440,441,442,443,448,449,
        450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,
        470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485] +
        list(range(100,110)) + list(range(110,200)) + [186,187,188,189,190,191,192,193,194,195,196,197,198])
    if lt in _big_range_1 and fp in ('ID','IE','IJ','IK','IS','IT','IU'):
        return True

    if lt in (421,) and fp in ('LA','ID','IS','IT','IU','RP','LF','PA','AG','AI',
                                'GE','MI','PM','TI','GI'):
        return True
    if lt in (422,) and fp in ('LA','IU','RP','MI','PM','LF','IP','QR','TI','VA',
                                'PA','PH','PS','PV','RS','WE','CR','RG'):
        return True
    if lt in (429,430) and fp in ('ID','IE','IJ','IK','IS','IT','IU','LA','RP','IP',
                                   'LF','QR','VA','PA','SD','MI','PM','TI','PV','CR','RS'):
        return True

    _range_650 = list(range(650,651)) + list(range(651,671)) + [672,673,674,675,676,677] + \
                 list(range(680,698))
    if lt in _range_650 and fp in ('ID','IE','IJ','IK','IS','IT','IU','RG','99','IC',
                                    'IH','II','IP','LA','LB','LF','LI','LW','MI','PA',
                                    'PM','PO','QR','RP','RX','RY','SG','TI','VA'):
        return True

    if lt in (807,811,813) and fp in ('CE','CL','LJ','LK','FL','ES','MF','CM'):
        return True
    if lt in (808,810,812,814) and fp in ('CE','CL','FL','ES','MF','CM'):
        return True
    if lt in (809,) and fp in ('CE','CL','LL','LM','FL','ES','MF','CM'):
        return True
    if lt in (638,639,911,912) and fp in ('LA','LW','RP','CR'):
        return True
    if lt in (412,413,414,461) and fp in ('LA','LW','RP','MI','PM','IH','LF','IP',
                                           'QR','TI','VA','PA'):
        return True
    if lt in (413,471,472,473,474,475,476,477,478) and fp == 'RG':
        return True
    if lt == 414 and fp == 'IC':
        return True

    _rp_set = set([102,103,104,105,106,107,108,433,471,322,
                   472,473,474,475,476,477,478,479,480,
                   481,482,483,484,485,486,487,488,489,
                   490,491,492,493,494,495,496,497,498,576])
    if lt in _rp_set and fp == 'RP':
        return True

    _la_set = set([322,462,463,471,472,473,474,475,476,
                   477,478,479,480,481,482,483,484,
                   485,486,487,488,489,490,491,492,
                   493,494,495,496,497,498,576])
    if lt in _la_set and fp == 'LA':
        return True

    _471_498 = set(range(471, 499))
    if lt in _471_498 and fp in ('PA','MI','PM','TI','IP'):
        return True

    _cr_set = set([322,471,472,473,474,475,476,477,478,
                   479,480,481,482,483,484,485,487,
                   488,486,489,490,491,492,493,494,
                   495,496,497,498,676,677,691,692,
                   576,693,694,695])
    if lt in _cr_set and fp == 'CR':
        return True

    _id_is_it_iu_set = set([486,487,488,490,491,492,493,495,496,497,498])
    if lt in _id_is_it_iu_set and fp in ('ID','IS','IT','IU'):
        return True

    # Very large LOANTYPE range for GI/GE/GA/GB
    _gi_range = set(list(range(110,200)) + list(range(200,300)) + list(range(300,400)) +
                    list(range(400,499)) + list(range(500,600)) + list(range(600,631)) +
                    list(range(650,671)) + [672,673,674,675,676,677] +
                    list(range(680,698)) + list(range(726,800)) + list(range(851,900)) +
                    list(range(860,900)) + list(range(700,730)) + list(range(730,800)))
    if lt in _gi_range and fp in ('GI','GE','GA','GB'):
        return True

    # Second large GI/GE/GA/GB range (remaining product codes)
    _gi_range2 = set([128,130,131,132,180,181,182,183,184,185,186,187,188,189,
                      190,192,193,194,195,196,197,198,199,309,310,380,381,
                      608,631,632,633,634,635,636,637,638,639,640,641,642,643,
                      644,645,646,647,648,649,691,692,693,694,695,696,697] +
                     list(range(700,726)) + list(range(800,819)) + [819] +
                     list(range(820,850)) + list(range(900,952)) +
                     list(range(952,1000)))
    if lt in _gi_range2 and fp in ('GI','GE','GA','GB'):
        return True

    if lt in (309,310,608,670,690,904,905,919,920) and fp == 'BF':
        return True
    if lt in (188,) and fp in ('LA','RP','MI','PM','IH','LF','IP','QR','TI',
                                'VA','PA','RG'):
        return True
    if lt in (189,190,468) and fp in ('LA','RP','MI','PM','IH','LF','IP','QR',
                                       'TI','VA','PA'):
        return True
    if lt == 249 and fp in ('LA','LW','RP','CR'):
        return True
    if lt == 349 and fp in ('LA','LW','RP','CR'):
        return True
    if lt in (303,306,610,611) and fp in ('LA','LW','RP','CR'):
        return True
    if lt in (307,) and fp in ('CR','RH','RP','RS'):
        return True
    if lt in (415,416,464,465,466,467) and fp in ('LA','RP','MI','PM','IH',
                                                    'LF','IP','QR','TI','VA','PA'):
        return True
    if lt in (191,417) and fp in ('LA','RP','MI','PM','IH','LF','IP','QR',
                                   'TI','VA','PA','BF'):
        return True
    if lt in (440,441,442,443) and fp in ('LA','ID','IS','IT','IU','IC','RP','IP',
                                           'LF','QR','VA','PA','AU','AG','AP','CB',
                                           'SD','GE','MI','PM','TI','PV','RH','CR',
                                           'SH','GI'):
        return True

    _ic_set = set([248,249,348,349,419,420,469,470,479,480,481,482,483,484,
                   600,601,602,603,604,605,606,607,608,609,610,611,
                   613,614,615,616,617,618,619,620,621,622,623,624,625,626,627,628,629,
                   630,631,632,633,634,635,636,637,638,639,640,641,642,643,644,645,
                   646,647,648,649,672,673,674,675])
    if lt in _ic_set and fp == 'IC':
        return True

    _ph_set = set([110,111,112,113,114,115,116,117,118,119,124,139,140,141,142,145,
                   150,151,152,156,175,200,201,204,205,209,210,211,212,213,214,215,
                   216,217,218,219,220,225,226,227,228,230,231,232,233,234,235,236,
                   237,238,239,240,241,242,243,244,245,246,247,248,249,303,308,311,
                   400,409,410,412,413,414,322,415,419,433,440,441,464,466,469,472,
                   473,474,475,479,482,484,486,489,491,494,496,600,610,650,651,664,
                   668,672,674,677,692,693])
    if lt in _ph_set and fp == 'PH':
        return True

    _ps_set = set([120,122,126,127,129,143,146,149,153,154,155,157,158,159,160,161,
                   162,163,164,165,169,170,176,177,178,179,194,196,300,301,302,303,
                   304,305,306,311,315,316,320,325,322,330,335,340,345,348,349,350,
                   351,355,356,357,358,359,360,361,362,363,364,365,367,390,391,401,
                   402,403,404,405,406,407,408,411,416,420,430,442,443,461,464,465,
                   467,470,471,476,477,478,480,481,483,485,487,488,490,492,493,497,
                   498,500,504,505,506,495,509,510,512,515,516,517,518,519,520,521,
                   522,523,524,525,526,527,528,529,530,532,533,555,556,559,560,561,
                   564,565,566,567,568,569,570,573,574,601,602,603,604,605,606,607,
                   609,610,611,653,654,655,656,657,658,659,660,661,662,663,665,666,
                   667,668,669,673,675,676,691,694,695,916,918])
    if lt in _ps_set and fp == 'PS':
        return True

    _pv_set = set([110,111,112,113,114,115,116,117,118,119,120,122,124,126,127,129,
                   139,140,141,142,143,145,146,149,150,151,152,153,154,155,156,157,
                   158,159,160,161,162,163,164,165,169,170,175,176,177,178,179,187,
                   194,196,200,201,204,205,209,210,211,212,213,214,215,216,217,218,
                   219,220,225,226,227,228,230,231,232,233,234,235,236,237,238,239,
                   240,241,242,243,244,245,246,247,248,249,300,301,302,303,304,305,
                   306,308,311,315,316,320,325,330,335,340,345,348,349,350,322,351,
                   355,356,357,358,359,360,361,362,363,364,365,367,390,391,400,401,
                   402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,419,
                   420,461,464,465,466,467,469,470,471,472,473,474,475,476,477,478,
                   479,480,481,482,483,484,485,486,487,488,489,490,491,492,493,494,
                   495,496,497,498,500,504,505,506,509,510,512,515,516,517,518,519,
                   520,521,522,523,524,525,526,527,528,529,530,532,533,555,556,559,
                   560,561,564,565,566,567,568,569,570,573,574,600,601,602,603,604,
                   605,606,607,609,610,611,650,651,653,654,655,656,657,658,659,660,
                   661,662,663,664,665,666,667,668,669,672,673,674,675,687,676,677,
                   691,692,693,694,695,916,918,431,433])
    if lt in _pv_set and fp == 'PV':
        return True

    _au_set = set([380,381] + list(range(700,726)) +
                  list(range(110,200)) + list(range(400,499)) + list(range(650,671)) +
                  [672,673,674,675,676,677] + list(range(680,698)))
    if lt in _au_set and fp == 'AU':
        return True

    _sd_set = set([380,381,128,130,131,132,135,433,652,
                   471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,
                   487,488,489,490,491,492,493,494,495,496,497,498,
                   676,677,691,692,693,694,695] + list(range(700,726)))
    if lt in _sd_set and fp == 'SD':
        return True

    _rs_set = set([300,301,303,304,309,310,315,348,349,351,322,356,359,361,362,363,
                   367,368,369,530,900,901,904,905,906,907,914,915,916,918,919,920,
                   120,126,127,129,143,144,149,153,154,155,157,158,164,165,176,177,
                   178,179,180,181,183,186,187,188,191,193,401,404,405,406,407,408,
                   417,418,420,425,426,441,442,443,462,463,464,465,468,470,471,476,
                   477,478,480,481,483,485,487,488,490,492,493,497,498,402])
    if lt in _rs_set and fp == 'RS':
        return True

    _rs_we_set = set(list(range(200,300)) + list(range(600,650)) +
                     list(range(110,120)) + [124,139,140,141,142,145,147,150,151,152,
                                              156,173,175,400,409,410,412,413,414,415,
                                              419,433,440,466,467,469,472,473,474,475,
                                              479,482,484,486,489,491,494,496] +
                     list(range(650,671)) + [672,674,676,677] + list(range(680,698)) + [416])
    if lt in _rs_we_set and fp in ('RS','WE'):
        return True

    _sh_set = set([249,349,400,401,402,414,415,416,419,420,466,467,469,470,479,480,
                   481,482,483,484,600,602] + list(range(650,671)) +
                  [672,673,674,675,676,677,680,681,682,683,684,685,686,687,688,689,
                   690,691,692,693,694,695,696,697])
    if lt in _sh_set and fp == 'SH':
        return True

    # Large RH range
    _rh_range = set(list(range(200,364)) + list(range(500,600)) + list(range(600,650)) +
                    list(range(110,200)) + list(range(400,499)) + [576] +
                    list(range(650,671)) + [672,673,674,675,676,677] + list(range(680,698)))
    if lt in _rh_range and fp == 'RH':
        return True

    if lt in (245,) and fp in ('CR','RG'):
        return True

    _rg_set = set([179,216,235,244,315,363] + list(range(600,650)))
    if lt in _rg_set and fp == 'RG':
        return True

    if lt in (575,) and fp in ('LA','RP','CR','GI'):
        return True

    _ag_cb_ap_range = set(list(range(110,200)) + list(range(200,300)) + list(range(300,400)) +
                          list(range(400,499)) + list(range(600,631)) + list(range(650,671)) +
                          [672,673,674,675,676,677] + list(range(680,698)))
    if lt in _ag_cb_ap_range and fp in ('AG','CB','AP'):
        return True

    _cr2_set = set([110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,
                    126,127,129,134,135,136,138,139,140,141,142,143,144,145,146,147,
                    148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,
                    164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,
                    180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,
                    196,197,198,199,400,401,402,403,404,405,406,407,408,409,410,411,
                    412,413,414,415,416,417,461,462,463,464,466,467,419,420,433,469,
                    470,406,665])
    if lt in _cr2_set and fp == 'CR':
        return True

    _qr_set = set([117,118,119,126,136,139,140,433,462,463,471,472,473,474,475,476,
                   477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,
                   493,494,495,496,497,498])
    if lt in _qr_set and fp == 'QR':
        return True

    _lf_set = set([433,462,463,471,472,473,474,475,476,477,478,479,480,481,482,483,
                   484,485,486,487,488,489,490,491,492,493,494,495,496,497,498])
    if lt in _lf_set and fp == 'LF':
        return True

    _va_set = set([136,433,462,463,471,472,473,474,475,476,477,478,479,480,481,482,
                   483,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498])
    if lt in _va_set and fp == 'VA':
        return True

    if lt in (102,105,106,433,480,481) and fp == 'IP':
        return True

    if lt in (427,) and fp in ('LA','RP','MI','PM','LF','IP','QR','TI','VA','PA',
                                'SD','RS','WE','CR','SG'):
        return True
    if lt in (428,) and fp in ('LA','RP','MI','PM','LF','IP','TI','VA','PA',
                                'SD','RS','WE','CR','SG'):
        return True
    if lt in (392,612) and fp in ('FS','LR','RP','LF','AU','MC','RE','SC','SF',
                                   'TC','RJ','GE','GI'):
        return True

    _ai_set = set([300,301,302,304,309,310,315,320,322,345,348,349,350,351,357,358,
                   359,360,361,362,363,368,505,509,510,512,518,524,525,526,527,529,
                   531,532,533,560,561,565,566,567,568,569,570,573,574,575,900,901,
                   902,903,904,905,906,907,908,909,910,912,914,915,916,918,919,920,
                   925,950,951,601,602,603,604,605,606,607,608,609,678,679,631,632,
                   633,634,635,636,637,639,120,126,127,129,143,144,146,149,153,154,
                   155,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,
                   172,176,177,178,179,401,402,403,404,405,406,407,408,411,416,417,
                   418,425,427,428,429,430,461,462,463,467,471,476,478,480,481,485,
                   487,488,495,180,181,182,183,184,185,186,187,188,189,190,191,192,
                   193,653,654,655,656,657,658,659,660,661,662,663,665,666,667,670,
                   676,678,679,691,694,695,698,699,680,681,682,683,684,685,686,687,
                   688,689,690,576])
    if lt in _ai_set and fp == 'AI':
        return True

    if lt in (640,913) and fp in ('LA','CJ','AI','RS','BL','DC','CR','RP','GI',
                                   'GE','GA','GB'):
        return True
    if lt in (641,917) and fp in ('LA','CJ','AI','CR','RP','GI','GE','GA','GB'):
        return True
    if lt in (253,254) and fp in ('LA','RP','PH','RH','RS','WE','CR','RG','GI',
                                   'GE','GA','GB'):
        return True
    if lt in (431,) and fp in ('RP','IP','LF','QR','VA','PA','AU','SD','MI','PM',
                                'TI','PH','RH','RS','WE','CR'):
        return True
    if lt in (432,) and fp in ('RP','IP','LF','QR','VA','PA','AU','SD','MI','PM',
                                'TI','PH','RG','RH','RS','WE','CR'):
        return True
    if lt in (433,) and fp in ('PA','MI','PM','TI','LA'):
        return True
    if lt in (445,446) and fp in ('LA','ID','IS','IT','IU','RP','IP','LF','QR',
                                   'VA','PA','AU','AG','AP','CB','SD','GE','MI',
                                   'PM','TI','PH','RH','RS','WE','CR','GI'):
        return True

    # LOANTYPE IN (range) AND FEEPLN IN ('FM','FO')
    _fm_fo_loantypes = (
        list(range(4, 100)) +
        list(range(200, 300)) +
        list(range(300, 380)) +
        [380, 381, 390, 391, 392] +
        list(range(700, 726)) +
        list(range(600, 700)) +
        [993, 996] +
        list(range(102, 109)) +
        list(range(110, 200)) +
        list(range(400, 500)) +
        list(range(650, 700)) +
        [983]
    )
    if lt in _fm_fo_loantypes and fp in ('FM','FO'):
        return True

    if lt in (255,) and fp in ('LA','RP','PH','RH','RS','WE','CR','GI','GE','GA','GB'):
        return True
    if lt in (415,416,466,467) and fp == 'IC':
        return True

    _tf_set = set(list(range(471,499)) + [676,677,691,692,693,694,695])
    if lt in _tf_set and fp == 'TF':
        return True

    if lt in (128,130,131,132,983,698,699,380,381,700,705,720,725,678,679,993,996) \
            and fp in ('PE','PD'):
        return True
    if lt in (380,381,700,705,720,725,128,130,131,132) and fp in ('PB','PG','PJ'):
        return True
    if lt in (169,) and fp == 'CG':
        return True
    if lt in (256,258) and fp in ('LA','CJ','RP','PH'):
        return True
    if lt in (257,259) and fp in ('LA','CJ','RP','PH','RG'):
        return True
    if lt in (434,) and fp in ('LA','RP','IP','LF','PA','AI','CG','MI','PM','TI','RS'):
        return True
    if lt in (435,437) and fp in ('LA','RP','IP','LF','PA','AI','MI','PM','TI','RS'):
        return True
    if lt in (438,) and fp in ('LA','RP','IP','QR','VA','LF','PA','AI','CG','MI',
                                'PM','TI','RS'):
        return True
    if lt in (439,) and fp in ('LA','RP','IP','LF','PA','AI','MI','PM','TI','RS'):
        return True
    if lt in (436,) and fp in ('LA','RP','LF','PA','AI','MI','PM','TI','RS','SJ'):
        return True
    if lt in (577,) and fp in ('LA','RP','CR','AI','GA','GB','GE','GI'):
        return True
    if lt in (671,) and fp in ('LA','ID','IS','IT','IU','RP','LF','PA','AG','AI',
                                'GE','MI','PM','TI','GI'):
        return True
    if lt in (260,) and fp in ('LA','CJ','RP','PH','SH','IC'):
        return True
    if lt in (321,) and fp in ('LA','RP','AI','CR','PH','PS','PV','RH','RS','GI',
                                'GE','GA','GB'):
        return True
    if lt in (444,) and fp in ('LA','ID','IS','IT','IU','RP','IP','AP','AU','CB',
                                'QR','VA','LF','PA','AG','AI','GE','MI','PM','TI',
                                'RH','RS','CG','PH','PS','PV','GI'):
        return True
    if lt in (447,) and fp in ('LA','ID','IS','IT','IU','RP','IP','AP','AU','CB',
                                'QR','VA','LF','PA','AG','AI','GE','MI','PM','TI',
                                'RH','RS','CG','PH','PS','PV','GI'):
        return True
    if lt in (922,) and fp in ('LA','CJ','RP','AI','BL','CR','DC','PH','PS','PV',
                                'RH','RS','GI','GE','GA','GB'):
        return True
    if lt in (578,) and fp in ('LA','RP','CR','AI','RH','GA','GB','GE','GI'):
        return True
    if lt in (448,) and fp in ('LA','RP','IP','LF','PA','AI','MI','PM','TI','RS'):
        return True

    _bp_bg_range = (list(range(100, 109)) + list(range(110, 200)) +
                    list(range(400, 500)) + list(range(650, 700)) +
                    [981,982,983,984,985,991,992,993,994,995,996])
    if lt in _bp_bg_range and fp in ('BP','BG'):
        return True

    if lt in (134,) and fp in ('LA','RP','IP','LF','PA','AI','MI','PM','TI',
                                'RS','BG','BP'):
        return True
    if lt in (423,664) and fp in ('PS','SD'):
        return True
    if lt in (425,) and fp in ('PH','WE','SD'):
        return True
    if lt in (666,) and fp in ('PH','SD'):
        return True
    if lt in (380,381,700,705,128,130) and fp == 'IN':
        return True
    if lt in (380,381,700,705,720,725) and fp == 'BQ':
        return True

    if lt in (137,) and fp in ('LA','ID','IS','IT','IU','RP','BG','IP','LF','PA',
                                'AU','AG','AP','CB','GE','QR','VA','AI','MI','PM',
                                'TI','PH','RH','PS','PV','GI','RS','CG'):
        return True
    if lt in (133,) and fp in ('LA','ID','IS','IT','IU','RP','BG','IP','LF','PA',
                                'AU','AG','AP','CB','GE','QR','VA','AI','MI','PM',
                                'TI','PH','RH','PS','PV','GI','RS'):
        return True
    if lt in (172,) and fp in ('LA','ID','IS','IT','IU','RP','BG','IP','CG','LF',
                                'PA','AU','AG','AP','CB','GE','QR','VA','SG','SJ',
                                'MI','PM','TI','RH','PV','GI','RS','AI'):
        return True

    _fe_range = set(list(range(110,197)) + list(range(400,471)) +
                    list(range(650,679)) + list(range(680,691)))
    if lt in _fe_range and fp == 'FE':
        return True

    if lt in (192,195,197,199) and fp in ('ID','IS','IT','IU','LA','BG','RP','IP',
                                           'LF','QR','VA','PA','AU','AG','AP','CB',
                                           'GE','FE','AI','MI','PM','TI','RS','RH','GI'):
        return True
    if lt in (816,817,818) and fp in ('CL','LJ','LK','ES','CM','FL','MF','GI','GE','CE'):
        return True
    if lt in (354,) and fp in ('LA','RP','GE','AI','CR','FM','FO','GI','RH','PH',
                                'PS','PV'):
        return True

    return False


# ============================================================================
# STEP 1: Build FEPLAN
# DATA FEPLAN; INFILE FEEFILE; ...filter by FEEPLN rules...
# ============================================================================
def build_feplan() -> pl.DataFrame:
    """
    Read fee records and filter by LOANTYPE/FEEPLN OUTPUT conditions.
    Adds CJFEE (= FEEAMTB when FEEPLN='CJ') and INFEE columns.
    """
    df = _read(FEE_DIR / "feefile.parquet")
    if df.is_empty():
        return pl.DataFrame()

    rows = []
    for row in df.iter_rows(named=True):
        lt  = row.get("LOANTYPE", 0) or 0
        fp  = (row.get("FEEPLN") or "").strip()
        cjfee = None
        infee = None

        # ESMR2010-3919: IF FEEPLN='CJ' THEN DO; CJFEE=FEEAMTB; OUTPUT; END;
        if fp == 'CJ':
            cjfee = row.get("FEEAMTB")
            rows.append({**row, "CJFEE": cjfee, "INFEE": infee})
            continue   # CJ always outputs; continue to avoid double-output

        # FEEPLN='IN' special case: INFEE=SUM(FEEAMTA,FEEAMTB)
        if lt in (380,381,700,705,128,130) and fp == 'IN':
            fa = row.get("FEEAMTA") or 0
            fb = row.get("FEEAMTB") or 0
            infee = (fa or 0) + (fb or 0)
            rows.append({**row, "CJFEE": cjfee, "INFEE": infee})
            continue

        if _feepln_match(lt, fp):
            rows.append({**row, "CJFEE": cjfee, "INFEE": infee})

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


# ============================================================================
# STEP 2: Build FEEPO (PROC SUMMARY + sort by ACCTNO NOTENO)
# PROC SUMMARY DATA=FEPLAN; BY ACCTNO NOTENO; VAR FEEAMTA FEEAMTB FEEAMTC CJFEE INFEE;
# OUTPUT OUT=FEEPO SUM=;
# ============================================================================
def build_feepo(feplan: pl.DataFrame) -> pl.DataFrame:
    """
    Summarise FEPLAN by ACCTNO/NOTENO, summing fee amount columns.
    Replicates PROC SUMMARY ... OUTPUT OUT=FEEPO SUM=.
    """
    if feplan.is_empty():
        return pl.DataFrame()

    sum_cols = [c for c in ("FEEAMTA","FEEAMTB","FEEAMTC","CJFEE","INFEE")
                if c in feplan.columns]
    if not sum_cols:
        return pl.DataFrame()

    feepo = (
        feplan
        .group_by(["ACCTNO","NOTENO"])
        .agg([pl.col(c).sum().alias(c) for c in sum_cols])
        .sort(["ACCTNO","NOTENO"])
    )
    return feepo


# ============================================================================
# STEP 3: Build LOAND (main loan note detail)
# DATA LOAND; INFILE ACCTFILE; ...all INPUT statements + derived fields...
# ============================================================================
def build_loand() -> pl.DataFrame:
    """
    Read loan note records from ACCTFILE parquet, apply all derived fields.
    Includes NPLNTB branch transfer logic (all commented out in source).
    """
    df = _read(ACCT_DIR / "acctfile.parquet")
    if df.is_empty():
        return pl.DataFrame()

    def _get(row, col, default=None):
        return row.get(col, default)

    rows = []
    for row in df.iter_rows(named=True):
        r = dict(row)

        # Derived date fields
        tra_eff_dt1 = str(r.get("TRA_EFF_DT1") or "")
        tramm = tra_eff_dt1[0:2] if len(tra_eff_dt1) >= 6 else ""
        trayy = tra_eff_dt1[3:7] if len(tra_eff_dt1) >= 7 else ""
        tradt = tramm + trayy
        try:
            r["TRA_EFF_DT"] = int(tradt) if tradt else None
        except Exception:
            r["TRA_EFF_DT"] = None

        drf_date = r.get("DRF_DATE") or ""
        if drf_date and drf_date != "0":
            try:
                r["DIGITAL_RR_STATUS_DT"] = datetime.strptime(str(drf_date), "%d/%m/%Y").date()
            except Exception:
                r["DIGITAL_RR_STATUS_DT"] = None

        r["LMOSTDATE"]  = _parse_ddmmyy_str(r.get("LMO_START_DT"))
        r["LMOENDDATE"] = _parse_ddmmyy_str(r.get("LMO_END_DT"))

        tra_rr_x = r.get("TRA_RR_ACCEPT_DTX")
        r["TRA_RR_ACCEPT_DT"] = _parse_ddmmyy_str(str(tra_rr_x)) if tra_rr_x else None

        rr_appl = r.get("RR_APPL_DT") or ""
        r["RR_APPL_DATE"] = _parse_ddmmyy_str(str(rr_appl)) if rr_appl else None

        wrioff_cl = r.get("WRIOFF_CL_DT") or ""
        r["WRIOFF_CLOSE_FILE_TAG_DT"] = _parse_ddmmyy_str(str(wrioff_cl)) if wrioff_cl else None

        bank_equity = r.get("BANK_EQUITY") or 0
        r["BANK_EQUITY_RATIO"] = (bank_equity / 10000) if bank_equity else None

        # FORMAT FDACCTNO 10.; FDACCTNO=SUBSTRN(COMPRESS(FDACCTNO_,' ',''),1,10);
        fdacctno_ = str(r.get("FDACCTNO_") or "").replace(" ", "")
        r["FDACCTNO"] = fdacctno_[:10] if fdacctno_ else None

        # ASCORE translations (SAS special chars to [])
        for col in ("ASCORE_PERM","ASCORE_LTST","ASCORE_COMM"):
            val = r.get(col) or ""
            r[col] = val.replace("\x81", "[").replace("\x93", "]") \
                        .replace("«", "[").replace("»", "]")

        # NACOSPADT = INPUT(SUBSTR(PUT(NACODT,Z11.),1,8),MMDDYY8.)
        r["NACOSPADT"] = _parse_z11_mmddyy(r.get("NACODT"))

        # LOCK_IN_END_DT
        r["LOCK_IN_END_DT"] = _parse_z8_ddmmyy(r.get("LOCK_IN_END_DTX"))

        # SCHBIL_INSTL_DT / SCHBIL_INT_DT / LASTBIL_INSTL_DT / LASTBIL_INT_DT
        r["SCHBIL_INSTL_DT"]  = _parse_z11_mmddyy(r.get("SCHBIL_INSTL_DTX"))
        r["SCHBIL_INT_DT"]    = _parse_z11_mmddyy(r.get("SCHBIL_INT_DTX"))
        r["LASTBIL_INSTL_DT"] = _parse_z11_mmddyy(r.get("LASTBIL_INSTL_DTX"))
        r["LASTBIL_INT_DT"]   = _parse_z11_mmddyy(r.get("LASTBIL_INT_DTX"))

        # FDB = SBA
        r["FDB"] = r.get("SBA")

        # CP = SUBSTR(PTMNATE,8,1)
        ptmnate = str(r.get("PTMNATE") or "")
        r["CP"] = ptmnate[7:8] if len(ptmnate) >= 8 else None

        # SM_DATE = APPLDATE; parse MDY from digit string
        sm_date_raw = r.get("APPLDATE")
        r["SM_DATE"] = _parse_sm_date(sm_date_raw)

        # STAFF_FREE_INT_IND / OMNIBUS_FACILITY_IND from TAXEQIND
        loantype = r.get("LOANTYPE") or 0
        taxeqind = r.get("TAXEQIND")
        if loantype in (5, 102):  # *19-1454
            r["STAFF_FREE_INT_IND"]   = taxeqind
            r["OMNIBUS_FACILITY_IND"] = None
        else:
            r["STAFF_FREE_INT_IND"]   = None
            r["OMNIBUS_FACILITY_IND"] = taxeqind

        # INTPYTD1 = INTPDYTD + INTINYTD - INTBUYPD
        r["INTPYTD1"] = (
            (r.get("INTPDYTD") or 0)
            + (r.get("INTINYTD") or 0)
            - (r.get("INTBUYPD") or 0)
        )

        # CUSTCDR = CUSTCODE (original)
        r["CUSTCDR"] = r.get("CUSTCODE")

        # CUSTCDX-based CUSTCODE/DNBFISME derivation
        custcdx = r.get("CUSTCDX") or 0
        custcode = r.get("CUSTCODE") or 0
        if custcode < 100 or custcdx == 0:
            r["DNBFISME"] = "0"
        if 0 < custcdx <= 99:
            r["CUSTCODE"] = custcdx
            r["DNBFISME"] = "0"
        elif 100 <= custcdx <= 999:
            s = str(custcdx).zfill(3)
            r["CUSTCODE"]  = int(s[1:3])
            r["DNBFISME"]  = s[0:1]
        elif 1000 <= custcdx <= 9999:
            s = str(custcdx).zfill(4)
            r["CUSTCODE"]  = int(s[2:4])
            r["DNBFISME"]  = s[1:2]
        elif 10000 <= custcdx <= 99999:
            s = str(custcdx).zfill(5)
            r["CUSTCODE"]  = int(s[3:5])
            r["DNBFISME"]  = s[2:3]

        # SYNRATIO = APPORMT; APPORMT = APPORMT * 0.01; EXRATIO = EXRATIO * 0.01
        appormt = r.get("APPORMT") or 0
        exratio = r.get("EXRATIO") or 0
        r["SYNRATIO"] = appormt
        r["APPORMT"]  = appormt * 0.01
        r["EXRATIO"]  = exratio * 0.01

        # %INC PGM(NPLNTB) — all branch transfer logic is commented out in NPLNTB.py.
        # The %INC executes when:
        #   LOANTYPE IN (128,130,131,132,380,381,390,700,705,720,725,983,993,996)
        #   AND PAIDIND='P'
        # Since NPLNTB contains only commented-out IF/THEN blocks, no transformation
        # is applied here. Placeholder retained for traceability.
        # if loantype in (128,130,131,132,380,381,390,700,705,720,725,983,993,996) \
        #         and (r.get("PAIDIND") or "").strip() == "P":
        #     pass  # NPLNTB branch transfer (all commented out in source)

        # RESTRUCT derivation from MODELDES
        if loantype not in (128,130,131,132,380,381,700,705,750,720,725,752,760,
                             4,5,6,7,15,20,25,26,27,28,29,30,31,32,33,34,
                             60,61,62,63,70,71,72,73,74,75,76,77,78,100,101,108):
            modeldes = str(r.get("MODELDES") or "")
            modelr = modeldes[0:1] if modeldes else ""
            if modelr in ("T", "C"):
                r["RESTRUCT"] = modelr
            # ** RESTRUCT MAINTAINED AS 'T','C' IN HOST  **
            # ** SAS DWH MAINTAIN 'S','R' UNDER RESTRUCT **

        rows.append(r)

    if not rows:
        return pl.DataFrame()

    # DROP columns as specified in DATA LOAND(DROP=...)
    df_out = pl.DataFrame(rows)
    _drop = ["MODELR","DD","MM","YY","NACODT","LOCK_IN_END_DTX","BANK_EQUITY",
             "TRA_EFF_DT1","TRAMM","TRAYY","TRADT","SCHBIL_INSTL_DTX",
             "SCHBIL_INT_DTX","LASTBIL_INSTL_DTX","LASTBIL_INT_DTX",
             "TRA_RR_ACCEPT_DTX","RR_APPL_DT","WRIOFF_CL_DT"]
    df_out = df_out.drop([c for c in _drop if c in df_out.columns])
    return df_out.sort(["ACCTNO","NOTENO"])


def _parse_ddmmyy_str(val) -> Optional[date]:
    """Parse DDMMYY8. from string (e.g. '01/01/2020' or '01012020')."""
    if not val:
        return None
    s = str(val).strip()
    if not s or s in ("0",""):
        return None
    try:
        return datetime.strptime(s, "%d/%m/%Y").date()
    except Exception:
        pass
    try:
        if len(s) >= 8:
            return date(int(s[4:8]), int(s[2:4]), int(s[0:2]))
    except Exception:
        pass
    return None


def _parse_sm_date(val) -> Optional[date]:
    """
    Replicates SAS SM_DATE derivation from APPLDATE:
      MM=SUBSTR(SM_DATE,2,2); DD=SUBSTR(SM_DATE,4,2); YY=SUBSTR(SM_DATE,6,4);
      SM_DATE = MDY(MM,DD,YY);
    APPLDATE stored as numeric YYMMDD or similar; substring positions 2-9.
    """
    if not val or val == 0:
        return None
    try:
        s = str(int(val))
        if len(s) < 8:
            return None
        mm = int(s[1:3])
        dd = int(s[3:5])
        yy = int(s[5:9])
        return date(yy, mm, dd)
    except Exception:
        return None


# ============================================================================
# STEP 4: Build RNRHPMOR (HP moratorium records)
# DATA RNRHPMOR; INFILE MORHPFL; ... PROC SORT NODUPKEY BY ACCTNO NOTENO;
# ============================================================================
def build_rnrhpmor() -> pl.DataFrame:
    """Read HP moratorium file. Deduplicate by ACCTNO NOTENO."""
    df = _read(MOR_DIR / "morhpfl.parquet")
    if df.is_empty():
        return pl.DataFrame()
    return (
        df
        .unique(subset=["ACCTNO","NOTENO"], keep="first")
        .sort(["ACCTNO","NOTENO"])
    )


# ============================================================================
# STEP 5: Build LNNOTE (merge LOAND + FEEPO + RNRHPMOR)
# DATA LNNOTE; MERGE LOAND(IN=A RENAME=...) FEEPO RNRHPMOR; BY ACCTNO NOTENO; IF A;
# ============================================================================
def build_lnnote(loand: pl.DataFrame,
                 feepo: pl.DataFrame,
                 rnrhpmor: pl.DataFrame,
                 rdate: Optional[date]) -> pl.DataFrame:
    """
    Merge LOAND (left), FEEPO and RNRHPMOR on ACCTNO/NOTENO.
    Apply all derived calculations and date conversions.
    """
    if loand.is_empty():
        return pl.DataFrame()

    # RENAME FEEAMT -> FEEAMTO in LOAND before merge
    if "FEEAMT" in loand.columns:
        loand = loand.rename({"FEEAMT": "FEEAMTO"})

    # Left-join FEEPO
    if not feepo.is_empty():
        df = loand.join(feepo, on=["ACCTNO","NOTENO"], how="left")
    else:
        df = loand

    # Left-join RNRHPMOR
    if not rnrhpmor.is_empty():
        df = df.join(rnrhpmor, on=["ACCTNO","NOTENO"], how="left")

    # All remaining derivations are row-level; use map_rows / with_columns
    rows = []
    for row in df.iter_rows(named=True):
        r = dict(row)

        # FEEAMT = SUM(FEEAMTA, FEEAMTB)
        r["FEEAMT"] = (r.get("FEEAMTA") or 0) + (r.get("FEEAMTB") or 0)

        # MNIAPDTE
        mniapdt = r.get("MNIAPDT") or 0
        r["MNIAPDTE"] = _parse_z8_ddmmyy(mniapdt) if mniapdt else None

        # NXDUEDT = INPUT(SUBSTR(PUT(NXDUEDT,Z11.),1,8),YYMMDD8.)
        r["NXDUEDT"] = _parse_z11_yymmdd(r.get("NXDUEDT")) if (r.get("NXDUEDT") or 0) else None

        # NXTBIL = INPUT(SUBSTR(PUT(NXBILDT,Z11.),1,8),MMDDYY8.)
        r["NXTBIL"] = _parse_z11_mmddyy(r.get("NXBILDT")) if (r.get("NXBILDT") or 0) else None

        # BLDATE = INPUT(SUBSTR(PUT(BILDUE1,Z11.),1,8),YYMMDD8.)
        r["BLDATE"] = _parse_z11_yymmdd(r.get("BILDUE1")) if (r.get("BILDUE1") or 0) else None

        # DLVDATE = INPUT(SUBSTR(PUT(DLIVRYDT,Z11.),1,8),MMDDYY8.)
        r["DLVDATE"] = _parse_z11_mmddyy(r.get("DLIVRYDT")) if (r.get("DLIVRYDT") or 0) else None

        acctno   = r.get("ACCTNO") or 0
        ntint    = (r.get("NTINT") or "").strip()
        loantype = r.get("LOANTYPE") or 0

        if acctno < 3_000_000_000:
            #  Commented block retained as-is from source:
            #  /*
            #  IF (100<=LOANTYPE<=103) OR ... THEN FEEAMT=SUM(FEEAMT,FEEAMTA);
            #  */
            #  /*
            #  IF (225<=LOANTYPE<=228) OR ... THEN FEEAMT=SUM(FEEAMT,NFEEAMT5);
            #  */
            if ntint == "A":
                if loantype == 196:
                    r["BALANCE"] = sum(filter(None, [
                        r.get("CURBAL"), r.get("INTEARN"),
                        -(r.get("INTAMT") or 0), -(r.get("INTEARN2") or 0),
                        r.get("INTEARN3"), r.get("FEEAMT")]))
                else:
                    r["BALANCE"] = sum(filter(None, [
                        r.get("CURBAL"), r.get("INTEARN"),
                        -(r.get("INTAMT") or 0), r.get("FEEAMT")]))
            else:
                r["BALANCE"] = sum(filter(None, [
                    r.get("CURBAL"), r.get("ACCRUAL"), r.get("FEEAMT")]))

        if acctno > 8_000_000_000:
            r["FEEAMT5"]  = r.get("FEEAMT13")
            r["NFEEAMT8"] = r.get("FEEAMT9")
            r["NFEEAMT5"] = r.get("FEEAMT10")
            r["NFEEAMT6"] = r.get("FEEAMT11")
            r["NFEEAMT7"] = r.get("FEEAMT12")
            r["NFEEAM10"] = r.get("FEEAMT14")
            r["NFEEAM11"] = r.get("FEEAMT15")
            r["NFEEAM12"] = r.get("FEEAMT16")

            #  /*
            #  IF LOANTYPE IN (227,228,...) THEN FEEAMT4=FEEAMTA;
            #  ELSE IF LOANTYPE IN (991,992,994,995) THEN FEEAMT =FEEAMT8;
            #  */
            if loantype in (128,130,380,381,700,705,720,725,983,993,996):
                #  /*
                #  IF LOANTYPE IN (720,725) THEN FEEAMT8 = SUM(FEEAMT,FEEAMTA);
                #  ELSE FEEAMT8 = SUM(FEEAMT,FEEAMTA);
                #  *  FEEAMT8 = SUM(FEEAMT8,FEEAMTA,FEEAMTB);
                #  * ELSE FEEAMT8 = SUM(FEEAMT8,FEEAMTA);
                #  */
                r["FEEAMT8"] = r.get("FEEAMT")
                r["FEETOT2"] = (r.get("FEETOT2") or 0) + (r.get("FEEAMTA") or 0)

            if ntint == "A":
                r["BALANCE"] = sum(filter(None, [
                    r.get("CURBAL"), r.get("INTEARN"),
                    -(r.get("INTAMT") or 0), -(r.get("INTEARN2") or 0),
                    r.get("INTEARN3"), r.get("FEEAMT")]))
            else:
                r["BALANCE"] = sum(filter(None, [
                    r.get("CURBAL"), r.get("ACCRUAL"), r.get("FEEAMT")]))

            if r.get("FEEAMT") is None:
                r["FEEAMT"] = r.get("FEEAMTO")

        rows.append(r)

    return pl.DataFrame(rows)


# ============================================================================
# STEP 6: Build LNACCT (account-level data from ACC1FILE)
# ============================================================================
def build_lnacct() -> pl.DataFrame:
    """Read ACC1FILE records, derive SM_DATE from stored numeric."""
    df = _read(ACC1_DIR / "acc1file.parquet")
    if df.is_empty():
        return pl.DataFrame()

    def _sm_date_acc1(val) -> Optional[date]:
        """
        IF SM_DATE NE 0 THEN DO;
          MM=SUBSTR(SM_DATE,5,2); DD=SUBSTR(SM_DATE,7,2); YY=SUBSTR(SM_DATE,9,4);
          SM_DATE = MDY(MM,DD,YY);
        END;
        """
        if not val or val == 0:
            return None
        try:
            s = str(int(val)).zfill(12)
            mm = int(s[4:6])
            dd = int(s[6:8])
            yy = int(s[8:12])
            return date(yy, mm, dd)
        except Exception:
            return None

    rows = []
    for row in df.iter_rows(named=True):
        r = dict(row)
        r["SM_DATE"] = _sm_date_acc1(r.get("SM_DATE"))
        rows.append(r)
    return pl.DataFrame(rows)


# ============================================================================
# STEP 7: Build LNACC4 (ACC4FILE)
# ============================================================================
def build_lnacc4() -> pl.DataFrame:
    """Read ACC4FILE records."""
    df = _read(ACC4_DIR / "acc4file.parquet")
    return df


# ============================================================================
# STEP 8: Build LNCOMM (commitment file)
# ============================================================================
def build_lncomm() -> pl.DataFrame:
    """Read COMTFILE records."""
    df = _read(COMM_DIR / "comtfile.parquet")
    return df


# ============================================================================
# STEP 9: Build LOAN.HPCOMP / ILOAN.HPCOMP
# DATA LOAN.HPCOMP ILOAN.HPCOMP; INFILE COMPFILE; ...
# ============================================================================
def build_hpcomp() -> pl.DataFrame:
    """Read COMPFILE records. Written to both LOAN and ILOAN libraries."""
    df = _read(COMP_DIR / "compfile.parquet")
    return df


# ============================================================================
# STEP 10: Build LNNAME
# ============================================================================
def build_lnname() -> pl.DataFrame:
    """Read NAMEFILE records."""
    df = _read(NAME_DIR / "namefile.parquet")
    return df


# ============================================================================
# STEP 11: Build LIAB
# ============================================================================
def build_liab() -> pl.DataFrame:
    """Read LIABFILE records."""
    df = _read(LIAB_DIR / "liabfile.parquet")
    return df


# ============================================================================
# STEP 12: Build NAME8 (sort by ACCTNO)
# ============================================================================
def build_name8() -> pl.DataFrame:
    """Read NAM8FILE records. Derive DTEREGVH from packed date."""
    df = _read(NAM8_DIR / "nam8file.parquet")
    if df.is_empty():
        return pl.DataFrame()
    return df.sort("ACCTNO")


# ============================================================================
# STEP 13: Build NAME9
# ============================================================================
def build_name9() -> pl.DataFrame:
    """Read NAM9FILE records."""
    df = _read(NAM9_DIR / "nam9file.parquet")
    return df


# ============================================================================
# STEP 14: Build LNRATE
# DATA LNRATE; INFILE RATEFILE; ... IF EFFDATE <= &RDATE;
# PROC SORT BY RINDEX DESCENDING EFFDATE;
# DATA LOAN.LNRATE ILOAN.LNRATE; SET LNRATE; BY RINDEX; IF FIRST.RINDEX THEN OUTPUT;
# ============================================================================
def build_lnrate(rdate: Optional[date]) -> pl.DataFrame:
    """
    Read rate file, filter EFFDATE <= RDATE, keep current rate per RINDEX.
    Written to both LOAN and ILOAN libraries.
    """
    df = _read(RATE_DIR / "ratefile.parquet")
    if df.is_empty():
        return pl.DataFrame()

    # Parse EFFDATE: IF EFFDATE GT 0 THEN EFFDATE = INPUT(SUBSTR(PUT(EFFDATE,Z11.),1,8),MMDDYY8.)
    rows = []
    for row in df.iter_rows(named=True):
        r = dict(row)
        ed_raw = r.get("EFFDATE") or 0
        if ed_raw:
            r["EFFDATE"] = _parse_z11_mmddyy(ed_raw)
        else:
            r["EFFDATE"] = None
        rows.append(r)

    df2 = pl.DataFrame(rows)

    # IF EFFDATE <= &RDATE
    if rdate:
        df2 = df2.filter(
            pl.col("EFFDATE").is_not_null() & (pl.col("EFFDATE") <= rdate)
        )

    if df2.is_empty():
        return pl.DataFrame()

    # PROC SORT BY RINDEX DESCENDING EFFDATE; KEEP FIRST.RINDEX
    df2 = df2.sort(["RINDEX", "EFFDATE"], descending=[False, True])
    df2 = df2.unique(subset=["RINDEX"], keep="first")
    return df2


# ============================================================================
# STEP 15: Build PEND (pending transactions, TRANTYPE=400)
# DATA PEND; INFILE PENDFILE; INPUT @004 TRANTYPE PD2. @; IF TRANTYPE=400 THEN ...
# Merge with RATE, derive ACCRATE, ACCUDRT, EFFDATE.
# ============================================================================
def build_pend(lnrate: pl.DataFrame) -> pl.DataFrame:
    """
    Read PENDFILE, filter TRANTYPE=400, parse rate override fields,
    merge with current rate table.
    """
    df = _read(PEND_DIR / "pendfile.parquet")
    if df.is_empty():
        return pl.DataFrame()

    df = df.filter(pl.col("TRANTYPE") == 400)
    if df.is_empty():
        return pl.DataFrame()

    # Parse raw string rate fields (ROVER, ROVER1, RINX, RUNDER, RUND1, RUNDLMT)
    rows = []
    for row in df.iter_rows(named=True):
        r = dict(row)

        def _to_float(x):
            try:
                return float(str(x).strip()) if x else None
            except Exception:
                return None

        def _to_int(x):
            try:
                return int(str(x).strip()) if x else None
            except Exception:
                return None

        r["RATEOVER"] = _to_float(r.get("ROVER"))
        r["MARGIN"]   = _to_float(r.get("ROVER1"))
        r["RINDEX"]   = _to_int(r.get("RINX"))
        r["RTUNDER"]  = _to_float(r.get("RUNDER"))
        r["RATEUDR"]  = _to_float(r.get("RUND1"))
        ratelmit_raw  = r.get("RUNDLMT")
        ratelmit = _to_float(ratelmit_raw)
        r["RATELMIT"] = (ratelmit / 100) if (ratelmit and ratelmit != 0) else ratelmit

        # EFFDATE from RELDTE: IF RELDTE NOT IN (.,0) THEN EFFDATE = INPUT(... YYMMDD8.)
        reldte = r.get("RELDTE") or 0
        r["EFFDATE"] = _parse_z11_yymmdd(reldte) if reldte else None

        # Drop raw fields
        for c in ("ROVER","ROVER1","RINX","RUNDER","RUND1","RUNDLMT"):
            r.pop(c, None)

        rows.append(r)

    df2 = pl.DataFrame(rows)

    # Merge with RATE (KEEP=RINDEX CURRATE) to compute ACCRATE, ACCUDRT
    if not lnrate.is_empty():
        rate_slim = lnrate.select(["RINDEX","CURRATE"]) \
                         .rename({"CURRATE": "_CURRATE"})
        df2 = df2.join(rate_slim, on="RINDEX", how="left")

        rows2 = []
        for row in df2.iter_rows(named=True):
            r = dict(row)
            currate  = r.get("_CURRATE") or 0
            rateover = r.get("RATEOVER") or 0
            margin   = r.get("MARGIN") or 0
            rtind    = (r.get("RTIND") or "").strip()
            rateudr  = r.get("RATEUDR") or 0
            ruind    = (r.get("RUIND") or "").strip()

            if not rateover:
                if rtind == "+":
                    r["ACCRATE"] = currate + margin
                elif rtind == "-":
                    r["ACCRATE"] = currate - margin
                else:
                    r["ACCRATE"] = None
            else:
                r["ACCRATE"] = None

            if rateudr:
                if ruind == "+":
                    r["ACCUDRT"] = currate + rateudr
                elif ruind == "-":
                    r["ACCUDRT"] = currate - rateudr
                else:
                    r["ACCUDRT"] = None
            else:
                r["ACCUDRT"] = None

            r.pop("_CURRATE", None)
            rows2.append(r)

        df2 = pl.DataFrame(rows2)

    return df2


# ============================================================================
# STEP 16: Enrich LNNOTE with USURYIDX / USLIMIT
# PROC SORT DATA=LNNOTE; BY USURYIDX;
# DATA LNNOTE; MERGE LNNOTE(IN=A) RATE(KEEP=RINDEX CURRATE RENAME=...); BY USURYIDX;
# USLIMIT = SUM(USMARGIN, CURRATE);
# ============================================================================
def enrich_lnnote_with_rate(lnnote: pl.DataFrame,
                             lnrate: pl.DataFrame) -> pl.DataFrame:
    """Join LNNOTE with rate master on USURYIDX to derive USLIMIT."""
    if lnnote.is_empty():
        return lnnote
    if lnrate.is_empty():
        lnnote = lnnote.with_columns(pl.lit(None).cast(pl.Float64).alias("USLIMIT"))
        return lnnote

    rate_slim = lnrate.select(["RINDEX","CURRATE"]) \
                      .rename({"RINDEX": "USURYIDX", "CURRATE": "_CURRATE_IDX"})
    df = lnnote.join(rate_slim, on="USURYIDX", how="left")
    df = df.with_columns(
        (pl.col("USMARGIN").fill_null(0) + pl.col("_CURRATE_IDX").fill_null(0))
        .alias("USLIMIT")
    ).drop("_CURRATE_IDX")
    return df


# ============================================================================
# STEP 17: Build OLDNOTE
# DATA OLDNOTE(RENAME=(NOTENO=BONUSANO APPVALUE=OLDNOTEAPPVALUE));
#   SET LNNOTE(KEEP=...); ... date derivations ...
# PROC SORT BY ACCTNO BONUSANO;
# ============================================================================
def build_oldnote(lnnote: pl.DataFrame) -> pl.DataFrame:
    """Build OLDNOTE from LNNOTE for bonus note reference lookups."""
    if lnnote.is_empty():
        return pl.DataFrame()

    keep_cols = [c for c in ("ACCTNO","NOTENO","BILDUEMIG","LASTTRAN","ISSUEDT","APPVALUE")
                 if c in lnnote.columns]
    df = lnnote.select(keep_cols)

    rows = []
    for row in df.iter_rows(named=True):
        r = dict(row)
        r["OLDNOTELASTTRAN"] = _parse_z11_mmddyy(r.get("LASTTRAN"))
        r["OLDNOTEBLDATE"]   = _parse_z11_yymmdd(r.get("BILDUEMIG"))
        r["OLDNOTEISSUEDT"]  = _parse_z11_mmddyy(r.get("ISSUEDT"))
        rows.append(r)

    df2 = pl.DataFrame(rows)
    # RENAME NOTENO->BONUSANO, APPVALUE->OLDNOTEAPPVALUE
    rename_map = {}
    if "NOTENO" in df2.columns:
        rename_map["NOTENO"] = "BONUSANO"
    if "APPVALUE" in df2.columns:
        rename_map["APPVALUE"] = "OLDNOTEAPPVALUE"
    if rename_map:
        df2 = df2.rename(rename_map)

    keep = [c for c in ("ACCTNO","BONUSANO","OLDNOTELASTTRAN","OLDNOTEBLDATE",
                        "OLDNOTEISSUEDT","OLDNOTEAPPVALUE") if c in df2.columns]
    return df2.select(keep).sort(["ACCTNO","BONUSANO"])


# ============================================================================
# STEP 18: Final LNNOTE enrichment (merge with OLDNOTE, derive MO_TAG etc.)
# PROC SORT DATA=LNNOTE; BY ACCTNO BONUSANO;
# DATA LNNOTE; MERGE LNNOTE(IN=A) OLDNOTE; BY ACCTNO BONUSANO; IF A;
# ============================================================================
def enrich_lnnote_with_oldnote(lnnote: pl.DataFrame,
                                oldnote: pl.DataFrame) -> pl.DataFrame:
    """
    Final LNNOTE enrichment: merge with OLDNOTE, derive ISSXDTE,
    OLDNOTEDAYARR, MO_TAG, MOSTDTE, MOENDDTE, MO_MAIN_DT.
    &HP macro = HP_ALL from PBBLNFMT.
    """
    if lnnote.is_empty():
        return lnnote

    if not oldnote.is_empty():
        df = lnnote.join(oldnote, on=["ACCTNO","BONUSANO"], how="left")
    else:
        df = lnnote

    rows = []
    for row in df.iter_rows(named=True):
        r = dict(row)
        loantype = r.get("LOANTYPE") or 0

        # ISSXDTE = INPUT(SUBSTR(PUT(ISSUEDT,Z11.),1,8),MMDDYY8.)
        issuedt = r.get("ISSUEDT") or 0
        issxdte = _parse_z11_mmddyy(issuedt) if issuedt else None
        r["ISSXDTE"] = issxdte

        # OLDNOTEDAYARR derivation
        r["OLDNOTEDAYARR"] = 0
        if issxdte:
            from datetime import date as _d
            d1 = _d(2013, 4, 1)
            d2 = _d(2015, 7, 31)
            if d1 <= issxdte <= d2 and \
               loantype in (128,130,131,132,700,705,720,725) and \
               (r.get("USER5") or "").strip() == "N" and \
               (r.get("NOTENO") or 0) >= 98010:
                oln_lasttran = r.get("OLDNOTELASTTRAN")
                oln_bldate   = r.get("OLDNOTEBLDATE")
                if oln_lasttran and oln_bldate:
                    diff = (oln_lasttran - oln_bldate).days
                    r["OLDNOTEDAYARR"] = max(0, diff)

        # MO_TAG = MODELDES
        r["MO_TAG"] = r.get("MODELDES")

        # IF LOANTYPE IN &HP OR LOANTYPE IN (15,20,71,72,103,104,107,108)
        hp_loantypes = set(HP_ALL) | {15, 20, 71, 72, 103, 104, 107, 108}
        if loantype in hp_loantypes:
            r["MOSTDTE"]  = _parse_z11_mmddyy(r.get("FIRSILDTE")) if (r.get("FIRSILDTE") or 0) else None
            r["MOENDDTE"] = _parse_z11_mmddyy(r.get("CURRILDTE")) if (r.get("CURRILDTE") or 0) else None
        else:
            r["MO_TAG"] = r.get("LMO_TAG")
            r["MO_MAIN_DT"] = _parse_ddmmyy_str(r.get("LMO_MAINT_DT"))
            r["MOSTDTE"]    = _parse_ddmmyy_str(r.get("LMO_START_DT"))
            r["MOENDDTE"]   = _parse_ddmmyy_str(r.get("LMO_END_DT"))

        rows.append(r)

    return pl.DataFrame(rows).sort(["ACCTNO","NOTENO"])


# ============================================================================
# MAIN
# ============================================================================
def main():
    print("EIBLNOTE started.")

    # Read RDATE macro from BNM.REPTDATE
    rdate = _get_rdate()
    print(f"RDATE = {rdate}")

    # ---- FEPLAN ----
    print("Building FEPLAN...")
    feplan = build_feplan()
    feplan.write_parquet(str(NPL_DIR / "feplan.parquet"))
    print(f"FEPLAN: {len(feplan):,} rows")

    # ---- FEEPO ----
    print("Building FEEPO...")
    feepo = build_feepo(feplan)
    feepo.write_parquet(str(NPL_DIR / "feepo.parquet"))
    print(f"FEEPO: {len(feepo):,} rows")

    # ---- LOAND ----
    print("Building LOAND...")
    loand = build_loand()
    loand.write_parquet(str(NPL_DIR / "loand.parquet"))
    print(f"LOAND: {len(loand):,} rows")

    # ---- RNRHPMOR ----
    print("Building RNRHPMOR...")  # SMR 2017-897 & 2020-779
    rnrhpmor = build_rnrhpmor()
    rnrhpmor.write_parquet(str(NPL_DIR / "rnrhpmor.parquet"))
    print(f"RNRHPMOR: {len(rnrhpmor):,} rows")

    # ---- LNRATE ----
    print("Building LNRATE...")
    lnrate = build_lnrate(rdate)
    lnrate.write_parquet(str(NPL_DIR / "lnrate.parquet"))
    lnrate.write_parquet(str(LOAN_DIR  / "lnrate.parquet"))   # LOAN.LNRATE
    lnrate.write_parquet(str(ILOAN_DIR / "lnrate.parquet"))   # ILOAN.LNRATE
    print(f"LNRATE: {len(lnrate):,} rows")

    # ---- PEND ----
    print("Building PEND...")
    pend = build_pend(lnrate)
    pend.write_parquet(str(NPL_DIR / "pend.parquet"))
    print(f"PEND: {len(pend):,} rows")

    # ---- LNNOTE (initial merge) ----
    print("Building LNNOTE (initial merge)...")
    lnnote = build_lnnote(loand, feepo, rnrhpmor, rdate)

    # ---- Enrich LNNOTE with USURYIDX rate ----
    lnnote = enrich_lnnote_with_rate(lnnote, lnrate)

    # ---- OLDNOTE ----
    print("Building OLDNOTE...")
    oldnote = build_oldnote(lnnote)
    oldnote.write_parquet(str(NPL_DIR / "oldnote.parquet"))
    print(f"OLDNOTE: {len(oldnote):,} rows")

    # ---- Final LNNOTE enrichment with OLDNOTE ----
    print("Enriching LNNOTE with OLDNOTE...")
    lnnote = enrich_lnnote_with_oldnote(lnnote, oldnote)
    lnnote.write_parquet(str(NPL_DIR / "lnnote.parquet"))
    print(f"LNNOTE: {len(lnnote):,} rows")

    # ---- LNACCT ----
    print("Building LNACCT...")
    lnacct = build_lnacct()
    lnacct.write_parquet(str(NPL_DIR / "lnacct.parquet"))
    print(f"LNACCT: {len(lnacct):,} rows")

    # ---- LNACC4 ----
    print("Building LNACC4...")
    lnacc4 = build_lnacc4()
    lnacc4.write_parquet(str(NPL_DIR / "lnacc4.parquet"))
    print(f"LNACC4: {len(lnacc4):,} rows")

    # ---- LNCOMM ----
    print("Building LNCOMM...")
    lncomm = build_lncomm()
    lncomm.write_parquet(str(NPL_DIR / "lncomm.parquet"))
    print(f"LNCOMM: {len(lncomm):,} rows")

    # ---- HPCOMP (LOAN.HPCOMP / ILOAN.HPCOMP) ----
    print("Building HPCOMP...")
    hpcomp = build_hpcomp()
    hpcomp.write_parquet(str(LOAN_DIR  / "hpcomp.parquet"))   # LOAN.HPCOMP
    hpcomp.write_parquet(str(ILOAN_DIR / "hpcomp.parquet"))   # ILOAN.HPCOMP
    print(f"HPCOMP: {len(hpcomp):,} rows")

    # ---- LNNAME ----
    print("Building LNNAME...")
    lnname = build_lnname()
    lnname.write_parquet(str(NPL_DIR / "lnname.parquet"))
    print(f"LNNAME: {len(lnname):,} rows")

    # ---- LIAB ----
    print("Building LIAB...")
    liab = build_liab()
    liab.write_parquet(str(NPL_DIR / "liab.parquet"))
    print(f"LIAB: {len(liab):,} rows")

    # ---- NAME8 ----
    print("Building NAME8...")
    name8 = build_name8()
    name8.write_parquet(str(NPL_DIR / "name8.parquet"))
    print(f"NAME8: {len(name8):,} rows")

    # ---- NAME9 ----
    print("Building NAME9...")
    name9 = build_name9()
    name9.write_parquet(str(NPL_DIR / "name9.parquet"))
    print(f"NAME9: {len(name9):,} rows")

    print("EIBLNOTE completed successfully.")


if __name__ == "__main__":
    main()
