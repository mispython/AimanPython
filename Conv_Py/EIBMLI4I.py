# !/usr/bin/env python3
"""
PROGRAM : EIBMLI4I
JCL     : EIBMLI4I JOB MISEIS,EIBMLIQ4,CLASS=A
PURPOSE : ADHOC - SAIFUL ADZLIM  ISLAMIC PART 4
          READS FIXED-WIDTH BINARY INPUT (BNMTBL4), CLASSIFIES LIQUEFIABLE ASSETS, COMPUTES DISCOUNT AMOUNTS,
            AND PRODUCES PROC PRINT + PROC TABULATE
          REPORTS WITH ASA CARRIAGE CONTROL CHARACTERS.
INPUT   : BNMTBL4  -- SAP.PBB.KAPITI4(0)  (binary .dat)
OUTPUT  : REPORT   -- EIBMLI4I_report.txt (ASA CC, PS=60)
"""

import struct
import datetime
import math
import sys
import polars as pl
from pathlib import Path
from collections import defaultdict

# ---------------------------------------------------------------------------
# Dependency: MATDTEX (%INC PGM(MATDTEX))
# ---------------------------------------------------------------------------
from MATDTEX import apply_matdtex, _mdy, _month, _year, SAS_EPOCH

# ---------------------------------------------------------------------------
# Path Configuration
# ---------------------------------------------------------------------------
INPUT_DIR   = Path("input")
OUTPUT_DIR  = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# //BNMTBL4 DD DSN=SAP.PBB.KAPITI4(0) -- binary fixed-width input
INPUT_FILE  = INPUT_DIR / "bnmtbl4.dat"
OUTPUT_FILE = OUTPUT_DIR / "EIBMLI4I_report.txt"

# Report page settings
PAGE_LENGTH = 60    # PS=60
LINE_SIZE   = 132   # LS=132

# ---------------------------------------------------------------------------
# OPTIONS NOCENTER YEARCUTOFF=1950 PS=60 LS=132
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# PROC FORMAT equivalents
# ---------------------------------------------------------------------------
def fmt_remfmt(remmth) -> str:
    """VALUE REMFMT bucketed remaining-maturity label (15 chars)."""
    try:
        v = float(remmth)
    except (TypeError, ValueError):
        return '>1 YEAR        '
    if   v <= 0.255:                 return 'UP TO 1 WK     '
    elif 0.255 < v <= 1:             return '>1 WK - 1 MTH  '
    elif 1     < v <= 3:             return '>1 MTH - 3 MTHS'
    elif 3     < v <= 6:             return '>3 - 6 MTHS    '
    elif 6     < v <= 12:            return '>6 MTHS - 1 YR '
    else:                            return '>1 YEAR        '


def fmt_remfmta(remmth) -> str:
    """VALUE REMFMTA -- 2-char bucket code."""
    try:
        v = float(remmth)
    except (TypeError, ValueError):
        return '06'
    if   v <= 0.255:                 return '01'
    elif 0.255 < v <= 1:             return '02'
    elif 1     < v <= 3:             return '03'
    elif 3     < v <= 6:             return '04'
    elif 6     < v <= 12:            return '05'
    else:                            return '06'


CLASSF_MAP = {
    'A': 'RM MKTBL SECUR/PAPERS ISSUED BY FED GOVT/BNM',
    'B': 'CAGAMAS BONDS & NOTES',
    'C': 'BAS ISSUED BY TIER1/AAA-RATED INST.',
    'D': 'BAS ISSUED BY TIER2 & NON-AAA',
    'E': 'NIDS ISSUED BY RATING',
    'F': 'STOCK                ',
    'R': 'REVERSE REPO            ',
    'X': 'NIDS UDR REPO (LIABILITIES)',
    'Y': 'NIDS UDR REPO (ASSETS)  ',
}


# ---------------------------------------------------------------------------
# Packed-decimal (PD) decoder
# SAS PD format: PDw.d  -> w bytes packed decimal, d implied decimal places
# ---------------------------------------------------------------------------
def decode_pd(raw: bytes, d: int = 0) -> float:
    """
    Decode IBM packed-decimal bytes into a Python float.
    Last nibble: C/F = positive, D = negative.
    d = number of implied decimal places.
    """
    if not raw or all(b == 0 for b in raw):
        return 0.0
    hex_str = raw.hex()
    sign_nibble = hex_str[-1].upper()
    digits      = hex_str[:-1]
    try:
        value = int(digits)
    except ValueError:
        return 0.0
    if sign_nibble == 'D':
        value = -value
    return value / (10 ** d)


# ---------------------------------------------------------------------------
# Date helpers (SAS date integer <-> Python date)
# ---------------------------------------------------------------------------
def sas_to_date(sas_int) -> datetime.date | None:
    if sas_int is None:
        return None
    try:
        return SAS_EPOCH + datetime.timedelta(days=int(sas_int))
    except (ValueError, OverflowError):
        return None


def date_to_sas(dt: datetime.date | None) -> int | None:
    if dt is None:
        return None
    return (dt - SAS_EPOCH).days


def parse_ddmmyy10(s: str) -> int | None:
    """Parse DD-MM-YYYY or DD/MM/YYYY string -> SAS date int."""
    s = str(s).strip()
    for fmt in ('%d-%m-%Y', '%d/%m/%Y', '%d%m%Y'):
        try:
            dt = datetime.datetime.strptime(s, fmt).date()
            return date_to_sas(dt)
        except ValueError:
            pass
    return None


def parse_ddmmyy8(s: str) -> int | None:
    """Parse DDMMYYYY or DD-MM-YY string -> SAS date int."""
    s = str(s).strip()
    for fmt in ('%d%m%Y', '%d-%m-%y', '%d/%m/%y', '%d-%m-%Y'):
        try:
            dt = datetime.datetime.strptime(s, fmt).date()
            return date_to_sas(dt)
        except ValueError:
            pass
    return None


def fmt_ddmmyy8(sas_int) -> str:
    """Format SAS date as DDMMYYYY (8 chars)."""
    dt = sas_to_date(sas_int)
    if dt is None:
        return '        '
    return dt.strftime('%d/%m/%y')


def fmt_date_display(sas_int) -> str:
    """Display date for report, blank if missing."""
    if sas_int is None:
        return '        '
    return fmt_ddmmyy8(sas_int)


# ---------------------------------------------------------------------------
# Read binary input file (BNMTBL4)
# Record length from field layout: last field ends at @191+3=194 bytes -> LRECL=194
#
# Field layout (1-based SAS positions -> 0-based Python slices):
#   @1   UTOSD  $10   -> [0:10]
#   @11  UTSMN  $16   -> [10:26]
#   @27  UTCPR  PD6.7 -> [26:32]   6 bytes, 7 dec
#   @33  UTYLD  PD4.4 -> [32:36]   4 bytes, 4 dec
#   @37  UTFCV  PD8.2 -> [36:44]   8 bytes, 2 dec
#   @45  UTDCV  PD8.2 -> [44:52]   8 bytes, 2 dec
#   @53  UTMKV  PD8.2 -> [52:60]   8 bytes, 2 dec
#   @61  UTMDT  $10   -> [60:70]
#   @71  UTINT  PD8.2 -> [70:78]   8 bytes, 2 dec
#   @79  UTPLS  PD8.2 -> [78:86]   8 bytes, 2 dec
#   @87  UTSTY  $3    -> [86:89]
#   @90  UTTTY  $1    -> [89:90]
#   @91  UTITY  $1    -> [90:91]
#   @92  UTIPI  $1    -> [91:92]
#   @93  UTLCD  $10   -> [92:102]
#   @103 UTNCD  $10   -> [102:112]
#   @113 UTRPT  $10   -> [112:122]
#   @123 UTSTS  $5    -> [122:127]
#   @128 UTAMS  PD8.2 -> [127:135]  8 bytes, 2 dec
#   @136 UTCLS  $1    -> [135:136]
#   @137 UTPGMID $10  -> [136:146]
#   @147 UTIDT  $10   -> [146:156]
#   @157 UTRMD  $10   -> [156:166]
#   @167 UTAMP  PD8.2 -> [166:174]  8 bytes, 2 dec
#   @175 UTREF  $4    -> [174:178]
#   @175 UTREF1 $1    -> [174:175]   (overlaps UTREF)
#   @178 UTREF4 $1    -> [177:178]   (SAS @178 = position 178 = index 177)
#   @191 UTTYP  $3    -> [190:193]
# ---------------------------------------------------------------------------
LRECL = 193   # last field ends at position 193 (0-based index 190+3)


def read_input_records(path: Path) -> list[dict]:
    """Read fixed-width binary BNMTBL4 file, decode all fields per record."""
    records = []
    raw_bytes = path.read_bytes()
    # Records are LRECL bytes each (no newline in true mainframe FB format).
    # If file has newline-delimited records, strip them.
    if b'\n' in raw_bytes:
        lines = raw_bytes.split(b'\n')
    else:
        lines = [raw_bytes[i:i+LRECL] for i in range(0, len(raw_bytes), LRECL)]

    for line in lines:
        if len(line) < LRECL:
            if len(line) == 0:
                continue
            line = line.ljust(LRECL, b' ')

        def s(start, end):
            return line[start:end].decode('cp037', errors='replace').strip()

        def pd(start, end, d):
            return decode_pd(line[start:end], d)

        rec = {
            'UTOSD'  : s(0,   10),
            'UTSMN'  : s(10,  26),
            'UTCPR'  : pd(26, 32, 7),
            'UTYLD'  : pd(32, 36, 4),
            'UTFCV'  : pd(36, 44, 2),
            'UTDCV'  : pd(44, 52, 2),
            'UTMKV'  : pd(52, 60, 2),
            'UTMDT'  : s(60,  70),
            'UTINT'  : pd(70, 78, 2),
            'UTPLS'  : pd(78, 86, 2),
            'UTSTY'  : s(86,  89),
            'UTTTY'  : s(89,  90),
            'UTITY'  : s(90,  91),
            'UTIPI'  : s(91,  92),
            'UTLCD'  : s(92,  102),
            'UTNCD'  : s(102, 112),
            'UTRPT'  : s(112, 122),
            'UTSTS'  : s(122, 127),
            'UTAMS'  : pd(127, 135, 2),
            'UTCLS'  : s(135, 136),
            'UTPGMID': s(136, 146),
            'UTIDT'  : s(146, 156),
            'UTRMD'  : s(156, 166),
            'UTAMP'  : pd(166, 174, 2),
            'UTREF'  : s(174, 178),
            'UTREF1' : s(174, 175),
            'UTREF4' : s(177, 178),
            'UTTYP'  : s(190, 193),
        }
        records.append(rec)
    return records


# ---------------------------------------------------------------------------
# %MACRO MTHENDDT — adjust DAYTRAN to month-end day when MTHEND='Y'
# ---------------------------------------------------------------------------
def macro_mthenddt(mthend: str, mthipd: int, yrtran: int, daytran: int) -> int:
    """
    Replicate %MTHENDDT macro.
    Returns adjusted DAYTRAN.
    """
    if mthend != 'Y':
        return daytran
    if mthipd in (1, 3, 5, 7, 8, 10, 12):
        mthday = 31
    elif mthipd in (4, 6, 9, 11):
        mthday = 30
    else:
        mthday = 29 if (yrtran % 4 == 0) else 28
    if daytran > mthday:
        daytran = mthday
    return daytran


# ---------------------------------------------------------------------------
# %MACRO CALCIPD — calculate PREINTDT and CURINTDT
# ---------------------------------------------------------------------------
def macro_calcipd(matdt_sas: int, reptdt_sas: int):
    """
    Replicate %CALCIPD macro.
    Returns (preintdt_sas, curintdt_sas) as SAS date integers or None.
    """
    dt_mat  = sas_to_date(matdt_sas)
    if dt_mat is None:
        return None, None

    mthend  = 'N'
    npay    = 6
    nyear   = 0

    # IF DAY(MATDT+1)=1 THEN MTHEND='Y'
    dt_mat_plus1 = sas_to_date(matdt_sas + 1)
    if dt_mat_plus1 and dt_mat_plus1.day == 1:
        mthend = 'Y'

    daytran = dt_mat.day
    yrtran  = dt_mat.year
    mthtran = dt_mat.month

    trandays = matdt_sas - reptdt_sas
    numofn   = int(trandays / (npay * 30))
    nummth   = numofn * npay
    nyear    = int(nummth / 12)
    nmth     = nummth % 12
    mthipd   = (mthtran - nmth) - npay

    if mthipd <= 0:
        mthipd  = mthipd + 12
        nyear  += 1

    yrtran = yrtran - nyear
    daytran = macro_mthenddt(mthend, mthipd, yrtran, daytran)

    preintdt_sas = _mdy(mthipd, daytran, yrtran)
    if preintdt_sas is None:
        return None, None

    # Recalculate from PREINTDT
    pre_dt  = sas_to_date(preintdt_sas)
    if pre_dt is None:
        return None, None

    daytran = pre_dt.day
    yrtran  = pre_dt.year
    mthipd  = pre_dt.month

    mthipd += npay
    if mthipd > 12:
        yrtran += 1
        mthipd  = mthipd % 12

    daytran = macro_mthenddt(mthend, mthipd, yrtran, daytran)
    curintdt_sas = _mdy(mthipd, daytran, yrtran)

    return preintdt_sas, curintdt_sas


# ---------------------------------------------------------------------------
# Process REPTDATE (first record only, @113 UTRPT $10.)
# ---------------------------------------------------------------------------
def get_reptdate(records: list[dict]) -> tuple[int, str]:
    """Returns (reptdate_sas_int, rdate_ddmmyy8_str)."""
    utrpt      = records[0]['UTRPT']
    reptdate   = parse_ddmmyy10(utrpt)
    rdate_str  = fmt_ddmmyy8(reptdate)
    return reptdate, rdate_str


# ---------------------------------------------------------------------------
# Build LIQCLASS DataFrame from raw records
# ---------------------------------------------------------------------------
def build_liqclass(records: list[dict], reptdate_global: int) -> pl.DataFrame:
    """
    Replicate:
      DATA LIQCLASS; INFILE BNMTBL4; INPUT ...; IF UTREF1 EQ 'I' AND UTREF4 NE '  ';
      DATA LIQCLASS; SET LIQCLASS; AMOUNT=UTMKV; REPTDATE=...; STATUS=...; ISSDT=...; C2=...;
    """
    rows = []
    chkdt_sas = date_to_sas(datetime.date(2004, 9, 4))   # CHKDT='04SEP04'D

    for rec in records:
        # IF UTREF1 EQ 'I' AND UTREF4 NE '  '
        if rec['UTREF1'] != 'I':
            continue
        if rec['UTREF4'].strip() == '':
            continue

        amount   = rec['UTMKV'] if rec['UTMKV'] != 0 else 0.0
        reptdate = parse_ddmmyy10(rec['UTRPT'])
        if reptdate is None:
            reptdate = reptdate_global

        rpyr  = _year(reptdate)
        rpmth = _month(reptdate)
        rpday = sas_to_date(reptdate).day if sas_to_date(reptdate) else 0

        rd2 = 29 if (rpyr % 4 == 0) else 28

        status = rec['UTSTS'][:2] if len(rec['UTSTS']) >= 2 else rec['UTSTS']

        # ISSDT parsing from UTIDT (format MM-DD-YYYY per label)
        issdt_sas = 0
        utidt     = rec['UTIDT'].strip()
        if utidt and utidt != '':
            issdd_s = utidt[3:5] if len(utidt) >= 5 else ''
            issmm_s = utidt[0:2] if len(utidt) >= 2 else ''
            issyy_s = utidt[6:10] if len(utidt) >= 10 else ''
            try:
                issdt_sas = _mdy(int(issmm_s), int(issdd_s), int(issyy_s)) or 0
            except (ValueError, TypeError):
                issdt_sas = 0

        c2 = 'R' if issdt_sas <= chkdt_sas else 'Y'

        row = dict(rec)
        row.update({
            'AMOUNT'  : amount,
            'REPTDATE': reptdate,
            'RPYR'    : rpyr,
            'RPMTH'   : rpmth,
            'RPDAY'   : rpday,
            'RD2'     : rd2,
            'CHKDT'   : chkdt_sas,
            'STATUS'  : status,
            'ISSDT'   : issdt_sas,
            'C2'      : c2,
            'CLASS'   : '',
            'SLIPPAGE': 0,
        })
        rows.append(row)

    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Classify records -> LIQCLAS1, LIQCLAS2, LIQCLAS3
# ---------------------------------------------------------------------------
def classify(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replicate DATA LIQCLAS1, LIQCLAS2, LIQCLAS3 classification logic.
    Returns combined DataFrame equivalent to:
      DATA LIQCLASS; SET LIQCLAS1 LIQCLAS2 LIQCLAS3;
    """
    liqclas1_rows = []
    liqclas2_rows = []

    for row in df.to_dicts():
        utsty = row['UTSTY'].strip()
        uttty = row['UTTTY'].strip()
        c2    = row['C2']
        utsts = row['UTSTS'].strip()
        cls   = ''
        slip  = 0

        # ----- LIQCLAS1 classification -----
        if utsty in ('MGS','MTB','BNB','BNN','BMN','BMC','KHA','MGI','ITB'):
            cls = 'A'; slip = 2
            if uttty == 'X': cls = 'R'

        elif utsty in ('IDS','DHB'):
            cls = 'A'; slip = 3
            if uttty == 'X': cls = 'R'

        elif utsty == 'DMB':
            cls = 'A'; slip = 4
            if uttty == 'X': cls = 'R'

        elif utsty in ('CB2','CF1','CF2','CMB','PNB'):
            cls = 'B'; slip = 4
            if utsty == 'CB2' and uttty == 'X' and c2 == 'R':
                cls = 'R'

        elif utsty in ('CB1','CNT','SMC','SAC'):
            cls = 'B'; slip = 4
            if uttty == 'X' and c2 == 'R':
                cls = 'R'
            if c2 == 'Y':
                cls = 'F'; slip = 6

        elif utsty == 'SBA':
            status2 = utsts[:2] if len(utsts) >= 2 else utsts
            if status2 in ('P1','P2','AA'):
                cls = 'C'; slip = 4
            else:
                cls = 'D'; slip = 6

        if cls:
            r = dict(row); r['CLASS'] = cls; r['SLIPPAGE'] = slip
            liqclas1_rows.append(r)

        # ----- LIQCLAS2 classification -----
        cls2 = ''; slip2 = 0
        if utsty in ('SSD','SDC') and utsts == 'P1':
            cls2 = 'E'; slip2 = 6
            r = dict(row); r['CLASS'] = cls2; r['SLIPPAGE'] = slip2
            liqclas2_rows.append(r)

        if utsty in ('SLD','SFD','SZD') and utsts in ('AA','AAA','MARC1','MARC2'):
            cls2 = 'E'; slip2 = 6
            r = dict(row); r['CLASS'] = cls2; r['SLIPPAGE'] = slip2
            liqclas2_rows.append(r)

    # ----- LIQCLAS3: expand UTTTY='R' rows from LIQCLAS2 into X and Y -----
    liqclas3_rows = []
    for row in liqclas2_rows:
        if row['UTTTY'].strip() == 'R':
            rx = dict(row); rx['CLASS'] = 'X'; liqclas3_rows.append(rx)
            ry = dict(row); ry['CLASS'] = 'Y'; liqclas3_rows.append(ry)

    all_rows = liqclas1_rows + liqclas2_rows + liqclas3_rows
    if not all_rows:
        return pl.DataFrame()

    return pl.DataFrame(all_rows)


# ---------------------------------------------------------------------------
# Compute financial fields (discount, DISTAMT, interest dates)
# Replicates the large DATA LIQCLASS SET LIQCLASS computation block
# ---------------------------------------------------------------------------
def compute_financials(df: pl.DataFrame, reptdate_global: int) -> pl.DataFrame:
    """
    For each row apply the discount/DISTAMT/interest-date calculations.
    """
    out_rows = []

    for row in df.to_dicts():
        cls     = row.get('CLASS', '')
        # IF CLASS NE ' ' OR CLASS NE .
        if not cls or cls == ' ':
            continue

        utsty   = row['UTSTY'].strip()
        uttty   = row['UTTTY'].strip()
        utity   = row['UTITY'].strip()
        utipi   = row['UTIPI'].strip()
        slippage= float(row.get('SLIPPAGE', 0))

        # FORMAT DISCOUNT P 15.6
        distyld  = float(row['UTYLD']) + slippage
        discount = (distyld / 100) * float(row['UTMKV'])
        p        = discount
        cpn      = float(row['UTCPR'])
        yld      = distyld
        rv       = 100.0
        distamt  = 0.0

        reptdt   = parse_ddmmyy10(row['UTRPT']) or reptdate_global
        settledt = parse_ddmmyy10(row['UTOSD'])
        preintdt = parse_ddmmyy10(row['UTLCD'])
        curintdt = parse_ddmmyy10(row['UTNCD'])
        matdt    = parse_ddmmyy10(row['UTMDT'])

        if uttty == 'X':
            matdt = parse_ddmmyy10(row['UTRMD'])
        if uttty == 'R':
            if cls == 'X':
                matdt = parse_ddmmyy10(row['UTMDT'])
            elif cls == 'Y':
                matdt = parse_ddmmyy10(row['UTRMD'])

        if matdt is None:
            out_rows.append(row)
            continue

        tsm = matdt - reptdt

        # TM
        if curintdt is None or curintdt == 0:
            tm = matdt - reptdt
        else:
            tm = curintdt - reptdt

        remmth = round((tsm / 365) * 12, 2)
        if tsm < 8:
            remmth = 0.1   # FIXED THIS TO BE MORE ACCURATE

        # Interest calculation block
        npay = 6
        dsc  = None
        dcs  = None
        dcc  = None
        ndays= None

        if (utity in ('I', 'F') and utipi != '') or \
           (utsty in ('SZD','DHB','DMB','IDS','KHA','MGI')):

            # %CALCIPD for specific types
            if utsty in ('DHB','IDS','KHA','DMB','MGI'):
                # CALCULATE INTEREST PAYMENT DATES
                preintdt, curintdt = macro_calcipd(matdt, reptdt)

            if preintdt is not None and curintdt is not None:
                dsc   = curintdt - reptdt
                dcs   = reptdt   - preintdt
                dcc   = curintdt - preintdt
                ndays = matdt    - curintdt
            else:
                dsc = dcs = dcc = ndays = None

            if utsty in ('SZD','DHB','DMB','IDS','KHA'):
                cpn = 0.0

            if remmth < 6 and utity == 'I' and dsc is not None and dcc is not None:
                cpn2     = cpn / 100
                yld_frac = yld / 100
                accint   = cpn2 * (dcs / (2 * dcc))
                discount = (1 + cpn2 / 2) / (1 + yld_frac * (tsm / (2 * dcc))) - accint
                p        = discount * 100
            else:
                # APPENDIX 1 FORMULA
                if utsty not in ('SFD','CF1','CF2','CFB','SSD') and \
                   dsc is not None and dcc is not None and ndays is not None:
                    # NPAY from UTIPI
                    if   utipi == 'Q': npay = 3
                    elif utipi == 'H': npay = 6
                    elif utipi == 'Y': npay = 12

                    nmth = round(ndays / 365 * 12, 1)
                    n    = int(nmth / npay) + 1

                    poweri = n - 1 + dsc / dcc
                    i_val  = rv / ((1 + yld / 200) ** poweri)
                    ii_val = 0.0

                    for k in range(1, n + 1):
                        powerii = k - 1 + dsc / dcc
                        ii_val += (cpn / 2) / ((1 + yld / 200) ** powerii)

                    iii_val  = 100 * ((cpn / 200) * (dcs / dcc))
                    discount = i_val + ii_val - iii_val
                    p        = discount

        reptdays = reptdt - (preintdt if preintdt else reptdt)
        orgtenor = matdt  - (preintdt if preintdt else matdt)
        cpn2     = cpn / 100
        yld2     = yld / 100

        # DISTAMT SELECT(UTSTY)
        if dcc is None or dcc == 0:
            dcc_safe = 1
        else:
            dcc_safe = dcc

        if utsty in ('MGS','CBB','SLD','CB1','CB2','CMB','PNB'):
            distamt = float(row['UTFCV']) * ((p / 100) + (cpn2 * reptdays) / (dcc_safe * 2))

        elif utsty in ('CFB','SFD','CF1','CF2'):
            distamt = float(row['UTFCV']) * ((36500 + (cpn * dcc_safe)) /
                                              (36500 + (yld  * tm)))

        elif utsty in ('SZD','KHA','IDS','DHB','DMB'):
            # WHEN('SZD','KHA','IDS','DHB','DMB') -- ISB was commented out
            distamt = float(row['UTFCV']) * (p / 100)

        elif utsty in ('SSD','SLD'):
            distamt = float(row['UTFCV']) * (36500 + cpn * orgtenor) / \
                                             (36500 + yld  * tsm)

        else:
            distamt = float(row['UTFCV']) * (1 - (yld * tsm) / 36500)

        r = dict(row)
        r.update({
            'DISCOUNT' : discount,
            'P'        : p,
            'CPN'      : cpn,
            'YLD'      : yld,
            'DISTAMT'  : distamt,
            'REPTDT'   : reptdt,
            'SETTLEDT' : settledt,
            'PREINTDT' : preintdt,
            'CURINTDT' : curintdt,
            'MATDT'    : matdt,
            'TSM'      : tsm,
            'TM'       : tm,
            'REMMTH'   : remmth,
            'DSC'      : dsc,
            'DCS'      : dcs,
            'DCC'      : dcc_safe,
            'NDAYS'    : ndays,
            'REPTDAYS' : reptdays,
            'ORGTENOR' : orgtenor,
        })
        out_rows.append(r)

    return pl.DataFrame(out_rows)


# ---------------------------------------------------------------------------
# ASA report writer
# ---------------------------------------------------------------------------
class ReportWriter:
    def __init__(self, path: Path, page_length: int = PAGE_LENGTH,
                 line_size: int = LINE_SIZE):
        self.path        = path
        self.page_length = page_length
        self.line_size   = line_size
        self._lines      = []
        self._page_lines = 0
        self._titles     = []

    def set_titles(self, *titles):
        self._titles = list(titles)

    def _emit(self, cc: str, text: str = ''):
        self._lines.append(cc + text[:self.line_size])

    def _check_page(self):
        """Emit page break + title header if page is full."""
        if self._page_lines >= self.page_length:
            self._new_page()

    def _new_page(self):
        if not self._titles:
            self._emit('1')
        else:
            first = True
            for t in self._titles:
                cc = '1' if first else ' '
                self._emit(cc, t.center(self.line_size))
                first = False
        self._emit(' ')
        self._page_lines = len(self._titles) + 2

    def begin(self):
        self._new_page()

    def detail(self, text: str, double_space: bool = False):
        self._check_page()
        cc = '0' if double_space else ' '
        self._emit(cc, text)
        self._page_lines += 1

    def blank(self):
        self._emit(' ')
        self._page_lines += 1

    def separator(self, char: str = '-'):
        self._emit(' ', char * min(self.line_size, 132))
        self._page_lines += 1

    def save(self):
        with open(self.path, 'w', encoding='utf-8') as fh:
            for line in self._lines:
                fh.write(line + '\n')


# ---------------------------------------------------------------------------
# Formatting helpers for report columns
# ---------------------------------------------------------------------------
def fmt_comma16_2(v) -> str:
    try:
        return f"{float(v):>16,.2f}"
    except (TypeError, ValueError):
        return f"{'0.00':>16}"


def fmt_num(v, width: int = 12, dec: int = 6) -> str:
    try:
        return f"{float(v):>{width}.{dec}f}"
    except (TypeError, ValueError):
        return ' ' * width


# ---------------------------------------------------------------------------
# PROC PRINT equivalent
# VAR UTOSD CPN YLD UTMKV UTMDT UTSTY UTFCV PREINTDT CURINTDT
#     DISCOUNT DISTAMT UTTTY STATUS UTRMD MRNGE;
# SUM DISTAMT;
# FORMAT PREINTDT CURINTDT DDMMYY8.;
# ---------------------------------------------------------------------------
def proc_print(df: pl.DataFrame, rw: ReportWriter):
    # Column headers (SPLIT='*' in SAS wraps on '*'; we use plain labels)
    hdr = (
        f"{'OBS':<5} {'UTOSD':<10} {'CPN':>10} {'YLD':>10} {'UTMKV':>14} "
        f"{'UTMDT':<10} {'UTSTY':<6} {'UTFCV':>14} {'PREINTDT':<10} "
        f"{'CURINTDT':<10} {'DISCOUNT':>12} {'DISTAMT':>14} "
        f"{'UTTTY':<6} {'STATUS':<8} {'UTRMD':<10} {'MRNGE':<15}"
    )
    rw.detail(hdr)
    rw.separator('-')

    distamt_sum = 0.0
    for i, row in enumerate(df.to_dicts(), 1):
        distamt_val = float(row.get('DISTAMT') or 0)
        distamt_sum += distamt_val
        line = (
            f"{i:<5} "
            f"{str(row.get('UTOSD') or ''):<10} "
            f"{fmt_num(row.get('CPN'), 10, 4)} "
            f"{fmt_num(row.get('YLD'), 10, 4)} "
            f"{fmt_comma16_2(row.get('UTMKV')):>14} "
            f"{str(row.get('UTMDT') or ''):<10} "
            f"{str(row.get('UTSTY') or ''):<6} "
            f"{fmt_comma16_2(row.get('UTFCV')):>14} "
            f"{fmt_date_display(row.get('PREINTDT')):<10} "
            f"{fmt_date_display(row.get('CURINTDT')):<10} "
            f"{fmt_num(row.get('DISCOUNT'), 12, 6)} "
            f"{fmt_comma16_2(distamt_val):>14} "
            f"{str(row.get('UTTTY') or ''):<6} "
            f"{str(row.get('STATUS') or ''):<8} "
            f"{str(row.get('UTRMD') or ''):<10} "
            f"{str(row.get('MRNGE') or ''):<15}"
        )
        rw.detail(line)

    rw.separator('=')
    # SUM DISTAMT
    sum_line = (
        f"{'':5} {'':10} {'':10} {'':10} {'':14} "
        f"{'':10} {'':6} {'':14} {'':10} "
        f"{'':10} {'':12} {fmt_comma16_2(distamt_sum):>14} "
        f"{'':6} {'':8} {'':10} {'':15}"
    )
    rw.detail(sum_line)
    rw.blank()


# ---------------------------------------------------------------------------
# PROC TABULATE equivalent
# Renders a simple cross-tab table with CLASS rows and UTSTY/REMMTH columns
# ---------------------------------------------------------------------------
def _tabulate_by_utsty(
    df: pl.DataFrame,
    rw: ReportWriter,
    box_label: str,
    var_specs: list,   # [(col, label, fmt_fn)]
    class_filter,      # list of CLASS values or single value
    where_col: str = 'CLASS',
):
    """
    Replicate:
      PROC TABULATE; CLASS CLASS UTSTY; VAR ...; TABLE CLASS,UTSTY ALL, vars;
    """
    if isinstance(class_filter, str):
        class_filter = [class_filter]

    sub = df.filter(pl.col(where_col).is_in(class_filter))
    if sub.is_empty():
        return

    utst_vals = sorted(sub['UTSTY'].cast(pl.Utf8).unique().to_list())
    cls_vals  = sorted(sub[where_col].cast(pl.Utf8).unique().to_list())

    # Header
    col_w = 18
    rw.detail(f" {box_label}")
    rw.separator('-')

    # Variable header row
    var_header = f"{'CLASS':<8} {'UTSTY':<8}"
    for _, lbl, _ in var_specs:
        var_header += f" {lbl:>{col_w}}"
    rw.detail(var_header)
    rw.separator('-')

    totals = {lbl: 0.0 for _, lbl, _ in var_specs}

    for cls in cls_vals:
        cls_label = CLASSF_MAP.get(cls, cls)
        rw.detail(f" {cls_label}")
        cls_sub   = sub.filter(pl.col(where_col) == cls)
        utsty_tot = {lbl: 0.0 for _, lbl, _ in var_specs}

        for utsty in utst_vals:
            u_sub = cls_sub.filter(pl.col('UTSTY') == utsty)
            if u_sub.is_empty():
                continue
            row_str = f"  {'':<6} {utsty:<8}"
            for col, lbl, fmt_fn in var_specs:
                val = u_sub[col].sum() if col in u_sub.columns else 0.0
                utsty_tot[lbl] += float(val or 0)
                row_str += f" {fmt_fn(val):>{col_w}}"
            rw.detail(row_str)

        # UTSTY TOTAL row
        tot_str = f"  {'TOTAL':<14}"
        for _, lbl, fmt_fn in var_specs:
            totals[lbl] += utsty_tot[lbl]
            tot_str += f" {fmt_fn(utsty_tot[lbl]):>{col_w}}"
        rw.detail(tot_str)

    rw.separator('=')
    # ALL / TOTAL row
    all_str = f" {'TOTAL':<14}"
    for _, lbl, fmt_fn in var_specs:
        all_str += f" {fmt_fn(totals[lbl]):>{col_w}}"
    rw.detail(all_str)
    rw.blank()


def _tabulate_by_remmth(
    df: pl.DataFrame,
    rw: ReportWriter,
    box_label: str,
    var_specs: list,
    class_filter,
):
    """
    Replicate:
      PROC TABULATE; FORMAT REMMTH REMFMT.; CLASS CLASS REMMTH;
      TABLE CLASS,REMMTH ALL, vars;
    """
    if isinstance(class_filter, str):
        class_filter = [class_filter]

    sub = df.filter(pl.col('CLASS').is_in(class_filter))
    if sub.is_empty():
        return

    # Bucket order
    bucket_order = ['UP TO 1 WK     ', '>1 WK - 1 MTH  ', '>1 MTH - 3 MTHS',
                    '>3 - 6 MTHS    ', '>6 MTHS - 1 YR ', '>1 YEAR        ']

    col_w      = 22
    cls_vals   = sorted(sub['CLASS'].cast(pl.Utf8).unique().to_list())

    rw.detail(f" {box_label}")
    rw.separator('-')

    var_header = f"{'CLASS':<8} {'REMAINING MATURITY':<18}"
    for _, lbl, _ in var_specs:
        var_header += f" {lbl:>{col_w}}"
    rw.detail(var_header)
    rw.separator('-')

    totals = {lbl: 0.0 for _, lbl, _ in var_specs}

    for cls in cls_vals:
        cls_label = CLASSF_MAP.get(cls, cls)
        rw.detail(f" {cls_label}")
        cls_sub   = sub.filter(pl.col('CLASS') == cls)
        bkt_tot   = {lbl: 0.0 for _, lbl, _ in var_specs}

        for bkt in bucket_order:
            # Filter rows whose REMMTH maps to this bucket label
            bkt_rows = [r for r in cls_sub.to_dicts()
                        if fmt_remfmt(r.get('REMMTH')) == bkt]
            if not bkt_rows:
                continue
            row_str = f"  {'':<6} {bkt:<18}"
            for col, lbl, fmt_fn in var_specs:
                val = sum(float(r.get(col) or 0) for r in bkt_rows)
                bkt_tot[lbl] += val
                row_str += f" {fmt_fn(val):>{col_w}}"
            rw.detail(row_str)

        tot_str = f"  {'TOTAL':<24}"
        for _, lbl, fmt_fn in var_specs:
            totals[lbl] += bkt_tot[lbl]
            tot_str += f" {fmt_fn(bkt_tot[lbl]):>{col_w}}"
        rw.detail(tot_str)

    rw.separator('=')
    all_str = f" {'TOTAL':<26}"
    for _, lbl, fmt_fn in var_specs:
        all_str += f" {fmt_fn(totals[lbl]):>{col_w}}"
    rw.detail(all_str)
    rw.blank()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # ---------------------------------------------------------------------------
    # Read binary input
    # ---------------------------------------------------------------------------
    raw_records = read_input_records(INPUT_FILE)
    if not raw_records:
        print("ERROR: No records read from input file.", file=sys.stderr)
        sys.exit(1)

    # DATA REPTDATE: INFILE BNMTBL4 OBS=1; INPUT @113 UTRPT $10.;
    reptdate_global, rdate_str = get_reptdate(raw_records)

    # DATA LIQCLASS: full read + filter (UTREF1='I', UTREF4 NE '  ')
    liqclass_df = build_liqclass(raw_records, reptdate_global)

    # DATA LIQCLAS1/2/3 + combine
    liqclass_df = classify(liqclass_df)
    if liqclass_df.is_empty():
        print("WARNING: No records after classification.", file=sys.stderr)

    # DATA LIQCLASS (re-read REPTDATE into first obs) + financial computations
    liqclass_df = compute_financials(liqclass_df, reptdate_global)

    # PROC SORT DATA=LIQCLASS OUT=LIQCLASS; BY UTSTY CLASS;
    liqclass_df = liqclass_df.sort(['UTSTY', 'CLASS'])

    # %INC PGM(MATDTEX) — compute REMMTH from REPTDATE/MATDT
    liqclass_df = apply_matdtex(liqclass_df)

    # DATA LIQCLASS; SET LIQCLASS; MRNGE=PUT(REMMTH,REMFMT.);
    mrnge_vals  = [fmt_remfmt(v) for v in liqclass_df['REMMTH'].to_list()]
    liqclass_df = liqclass_df.with_columns(pl.Series('MRNGE', mrnge_vals))

    # ---------------------------------------------------------------------------
    # Report titles
    # TITLE1 'REPORT ID: EIBMLIQ4';
    # TITLE2 'PUBLIC BANK BERHAD - STATISTICS DEPARTMENT';
    # TITLE3 'STOCK OF LIQUEFIABLE ASSETS (PART 4)';
    # TITLE4 'AS AT ' &RDATE;
    # ---------------------------------------------------------------------------
    titles = [
        'REPORT ID: EIBMLIQ4',
        'PUBLIC BANK BERHAD - STATISTICS DEPARTMENT',
        'STOCK OF LIQUEFIABLE ASSETS (PART 4)',
        f'AS AT {rdate_str}',
    ]

    rw = ReportWriter(OUTPUT_FILE, page_length=PAGE_LENGTH, line_size=LINE_SIZE)
    rw.set_titles(*titles)
    rw.begin()

    # ---------------------------------------------------------------------------
    # PROC PRINT DATA=LIQCLASS
    # ---------------------------------------------------------------------------
    proc_print(liqclass_df, rw)

    # ---------------------------------------------------------------------------
    # PROC SUMMARY -> LIQASSET
    # CLASS UTSTY CLASS REMMTH UTTTY; VAR DISTAMT UTMKV UTAMP;
    # ---------------------------------------------------------------------------
    # Build LIQASSET via the DATA LIQASSET step logic
    liqasset_rows = []

    for (utsty, cls, remmth), grp in liqclass_df.to_pandas().groupby(
            ['UTSTY', 'CLASS', 'REMMTH'], sort=True):
        stknid  = 0.0; repnid  = 0.0
        amtstk  = 0.0; amtrepo = 0.0; amtrev  = 0.0; totdsct = 0.0

        for _, row in grp.iterrows():
            uttty_r = str(row.get('UTTTY') or '').strip()
            distamt = float(row.get('DISTAMT') or 0)
            utmkv   = float(row.get('UTMKV')   or 0)
            utamp_v = float(row.get('UTAMP')    or 0)
            cls_r   = str(row.get('CLASS') or '')
            utsty_r = str(row.get('UTSTY') or '').strip()

            if uttty_r == 'S':
                amtstk += utmkv
            elif uttty_r == 'R':
                amtrepo += utmkv
                if cls_r == 'X': stknid += utamp_v
                if cls_r == 'Y': repnid += utamp_v
            elif uttty_r == 'X':
                amtrev += utmkv

            if utsty_r in ('SSD', 'SLD'):
                if uttty_r == 'R':
                    totdsct -= distamt
                else:
                    totdsct += distamt
            else:
                totdsct += distamt

        mkvbook = amtstk - amtrepo
        liqasset_rows.append({
            'CLASS'   : cls,
            'UTSTY'   : utsty,
            'REMMTH'  : remmth,
            'AMTSTK'  : amtstk,
            'AMTREPO' : amtrepo,
            'AMTREV'  : amtrev,
            'TOTDSCT' : totdsct,
            'MKVBOOK' : mkvbook,
            'MKVREV'  : amtrev,
            'MKVNIDX' : stknid,
            'MKVNIDY' : repnid,
            'UTAMP'   : grp['UTAMP'].sum(),
        })

    liqasset_df = pl.DataFrame(liqasset_rows) if liqasset_rows else pl.DataFrame()

    # Shared var_specs helpers
    mkvbook_totdsct = [
        ('MKVBOOK', 'MKT VALUE SECUR REPT IN BOOKS',       fmt_comma16_2),
        ('MKVREV',  'MKT VALUE SECUR RECV UDR REV REPO',   fmt_comma16_2),
        ('TOTDSCT', 'TOTAL VALUE OF SECUR AFTER DISCOUNT',  fmt_comma16_2),
    ]

    if not liqasset_df.is_empty():
        # ------------------------------------------------------------------
        # PROC TABULATE 1: CLASS IN ('A','B')  BOX='CLASS-1 LIQUIFIABLE ASSETS'
        # ------------------------------------------------------------------
        rw.set_titles(*titles)
        rw._new_page()
        _tabulate_by_utsty(
            liqasset_df, rw,
            box_label='CLASS-1 LIQUIFIABLE ASSETS',
            var_specs=mkvbook_totdsct,
            class_filter=['A', 'B'],
        )

        # ------------------------------------------------------------------
        # PROC TABULATE 2: CLASS='R'  BOX='CLASS-1 LIQUIFIABLE ASSETS'
        # ------------------------------------------------------------------
        _tabulate_by_utsty(
            liqasset_df, rw,
            box_label='CLASS-1 LIQUIFIABLE ASSETS',
            var_specs=mkvbook_totdsct,
            class_filter=['R'],
        )

        # ------------------------------------------------------------------
        # PROC TABULATE 3: CLASS='R' BY REMMTH
        # ------------------------------------------------------------------
        _tabulate_by_remmth(
            liqasset_df, rw,
            box_label='CLASS-1 LIQUID ASSETS BY',
            var_specs=[
                ('MKVREV',  'MKT VALUE SECUR RECV UDR REV REPO',  fmt_comma16_2),
                ('TOTDSCT', 'TOTAL VALUE OF SECUR AFTER DISCOUNT', fmt_comma16_2),
            ],
            class_filter=['R'],
        )

        # ------------------------------------------------------------------
        # PROC TABULATE 4: CLASS='F'  BOX='CLASS-2 LIQUIFIABLE ASSETS FOR CLASS F'
        # ------------------------------------------------------------------
        _tabulate_by_utsty(
            liqasset_df, rw,
            box_label='CLASS-2 LIQUIFIABLE ASSETS FOR CLASS F',
            var_specs=mkvbook_totdsct,
            class_filter=['F'],
        )

        # ------------------------------------------------------------------
        # PROC TABULATE 5: CLASS='F' BY REMMTH
        # ------------------------------------------------------------------
        _tabulate_by_remmth(
            liqasset_df, rw,
            box_label='CLASS-2 LIQUIFIABLE ASSETS FOR CLASS F',
            var_specs=mkvbook_totdsct,
            class_filter=['F'],
        )

        # ------------------------------------------------------------------
        # PROC TABULATE 6: CLASS IN ('C','D','E')
        #   BOX='CLASS-2 LIQUID ASSETS & CREDIT LINES'
        # ------------------------------------------------------------------
        _tabulate_by_utsty(
            liqasset_df, rw,
            box_label='CLASS-2 LIQUID ASSETS & CREDIT LINES',
            var_specs=mkvbook_totdsct,
            class_filter=['C', 'D', 'E'],
        )

        # ------------------------------------------------------------------
        # PROC TABULATE 7: CLASS IN ('C','D','E') BY REMMTH
        #   BOX='CLASS-2 LIQUID ASSETS BY'
        # ------------------------------------------------------------------
        _tabulate_by_remmth(
            liqasset_df, rw,
            box_label='CLASS-2 LIQUID ASSETS BY',
            var_specs=[
                ('MKVBOOK', 'MKT VALUE SECUR REPT IN BOOKS',       fmt_comma16_2),
                ('TOTDSCT', 'TOTAL VALUE OF SECUR AFTER DISCOUNT',  fmt_comma16_2),
            ],
            class_filter=['C', 'D', 'E'],
        )

        # ------------------------------------------------------------------
        # PROC TABULATE 8: CLASS='Y'
        #   BOX='CLASS-2 LIQUID ASSETS'
        #   VAR MKVNIDY='PURCHASE PROCEEDS FOR NIDS UDR REPO'
        # ------------------------------------------------------------------
        _tabulate_by_utsty(
            liqasset_df, rw,
            box_label='CLASS-2 LIQUID ASSETS',
            var_specs=[
                ('MKVNIDY', 'PURCHASE PROCEEDS FOR NIDS UDR REPO', fmt_comma16_2),
            ],
            class_filter=['Y'],
        )

        # ------------------------------------------------------------------
        # PROC TABULATE 9: CLASS='X' BY REMMTH
        #   BOX='CLASS-2 LIQUID ASSETS BY'
        #   VAR MKVNIDX='STOCK MATURITY DATE'   FORMAT COMMA20.2
        # ------------------------------------------------------------------
        def fmt_comma20_2(v) -> str:
            try: return f"{float(v):>20,.2f}"
            except: return f"{'0.00':>20}"

        _tabulate_by_remmth(
            liqasset_df, rw,
            box_label='CLASS-2 LIQUID ASSETS BY',
            var_specs=[
                ('MKVNIDX', 'STOCK MATURITY DATE', fmt_comma20_2),
            ],
            class_filter=['X'],
        )

        # ------------------------------------------------------------------
        # PROC TABULATE 10: CLASS='Y' BY REMMTH
        #   BOX='CLASS-2 LIQUID ASSETS BY'
        #   VAR MKVNIDY='REPO MATURITY DATE'   FORMAT COMMA20.2
        # ------------------------------------------------------------------
        _tabulate_by_remmth(
            liqasset_df, rw,
            box_label='CLASS-2 LIQUID ASSETS BY',
            var_specs=[
                ('MKVNIDY', 'REPO MATURITY DATE', fmt_comma20_2),
            ],
            class_filter=['Y'],
        )

    rw.save()
    print(f"Report written to: {OUTPUT_FILE}  ({len(rw._lines)} lines)")


if __name__ == "__main__":
    main()
