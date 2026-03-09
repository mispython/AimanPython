#!/usr/bin/env python3
"""
Program  : LALWPBBD.py
Invoke by: PBBWKLY job
Modified : 10-7-2001 (JS dated 11/7/01)
Changes  : Additional 4 fields kept for product 177 & TUK report changes
Objective: PBB loans manipulation extracted from SAP.PBB.MNILN

           Reads BNM.NOTE<REPTMON><NOWK>  (produced by LALWPBBC)
           Reads DEPOSIT.CURRENT          (overdraft accounts)
           Reads BNM.UNOTE<REPTMON><NOWK> (undrawn loan commitments)
           Produces:
             BNM.OVDFT<REPTMON><NOWK>   - utilised OD accounts
             BNM.LOAN<REPTMON><NOWK>    - combined loan + OD (utilised)
             BNM.UOD<REPTMON><NOWK>     - unutilised OD accounts
             BNM.ULOAN<REPTMON><NOWK>   - combined undrawn loan + OD
             BNM.LNWOD<REPTMON><NOWK>   - written-off domestic loans
             BNM.LNWOF<REPTMON><NOWK>   - written-off FCY loans

           Dependencies: PBBLNFMT
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import sys
import logging
from pathlib import Path
from datetime import date, datetime
from typing import Optional

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# Note: SAS source includes %INC PGM(PBBDPFMT,PBBLNFMT). PBBDPFMT is a
# deposit-only format module with no functions used in this loan program.
# Only PBBLNFMT loan format functions are imported.
# ============================================================================
from PBBLNFMT import (
    format_odcustcd,
    format_oddenom,
    format_statecd,
    format_riskcd,
    FCY_PRODUCTS,
    HP_ACTIVE,
)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path(".")
DATA_DIR   = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input parquet paths
DEPOSIT_CURRENT_PATH = DATA_DIR / "deposit"  / "current.parquet"    # DEPOSIT.CURRENT
SME08_PATH           = DATA_DIR / "bnm"      / "sme08.parquet"       # BNM.SME08
REPTDATE_PATH        = DATA_DIR / "bnm"      / "reptdate.parquet"    # BNM.REPTDATE
LOAN_LNNOTE_PATH     = DATA_DIR / "loan"     / "lnnote.parquet"      # LOAN.LNNOTE
CISCADP_PATH         = DATA_DIR / "ciscadp"  / "deposit.parquet"     # CISCADP.DEPOSIT
CISCALN_PATH         = DATA_DIR / "ciscaln"  / "loan.parquet"        # CISCALN.LOAN
FOFMT_PATH           = DATA_DIR / "forate"   / "fofmt.parquet"       # PROC FORMAT CNTLOUT (FCY rates)

# ============================================================================
# CONSTANTS
# ============================================================================
FCY_SET = set(FCY_PRODUCTS)
HP_SET  = set(HP_ACTIVE)

# Cutoff dates used in SECTORCD / FISSPURP logic
APR06_CUTOFF   = date(2006, 4, 8)
APR06_ISSSDTE  = date(2006, 4, 1)
JAN07_CUTOFF   = date(2007, 1, 1)
OCT09_CUTOFF   = date(2009, 10, 1)

POSTCD_SET    = set()    # Populate from &POSTCD  macro variable if available
COUNTRYCD_SET = set()    # Populate from &COUNTRYCD macro variable if available

# CUSTCD remapping sets for SECTOLD IN ('0410','0430')
CUSTCD_TO_77 = {'61', '41', '42', '43'}
CUSTCD_TO_78 = {
    '62', '44', '46', '47', '63', '48', '49', '51', '57', '75', '59',
    '52', '53', '54', '45', '20', '30', '32', '33', '34', '35', '36',
    '37', '38', '39', '40'
}

# Financial institution CUSTCD codes used in DNBFISME filter
DNBFI_CODES = {'04', '05', '06', '30', '31', '32', '33', '34', '35',
               '37', '38', '39', '40', '45'}

# LOAN keep-list columns (mirrors %LET LOAN=(...))
LOAN_KEEP = [
    'BRANCH', 'ACCTNO', 'NAME', 'PRODUCT', 'CUSTCD', 'PRODCD', 'LIABCODE',
    'SECTORMA', 'BALMNI', 'COSTFUND', 'REMAINMH', 'COSTCTR', 'CJFEE',
    'SECTORCD', 'APPRDATE', 'ORIGMT', 'COLLCD', 'STATECD', 'RISKCD', 'APPRLIM2',
    'BALANCE', 'CURBAL', 'REMAINMT', 'RLEASAMT', 'APPRLIMT', 'UNDRAWN',
    'BLDATE', 'SECURE', 'NOTENO', 'NOTETERM', 'EARNTERM', 'EXPRDATE', 'PAYTYPE',
    'BILPAY', 'PAYAMT', 'PAYFREQ', 'INTRATE', 'RISKRTE', 'LOANSTAT', 'ODSTAT',
    'CLOSEDTE', 'INTAMT', 'ACCRUYTD', 'INTPDYTD', 'ACCRUEOP', 'TOTPDEOP',
    'APPVALUE', 'MARKETVL', 'BILLTYPE', 'USURYIDX', 'AMTIND', 'CUSTCODE',
    'RATE1', 'RATE2', 'LIMIT1', 'LIMIT2', 'COL1', 'COL2', 'TODRATE', 'FLATRATE',
    'BASERATE', 'ODPLAN', 'SPREAD', 'INTEARN', 'FEEAMT', 'ISSDTE', 'BIGX',
    'ORGCODE', 'ACCRUAL', 'FEEAMT4', 'NTINDEX', 'BILTOT', 'AVGAMT', 'CGCREF',
    'CUSTIDNO', 'ACCTYIND', 'INTEARN4', 'REBATE', 'NETPROC', 'BORSTAT',
    'PAIDIND', 'INTEARN2', 'INTEARN3', 'ODINTACC', 'HSTPRIN', 'COMMNO',
    'CENSUS', 'RESTBALC', 'APPRLMTACCT', 'NTINT', 'REBATEI', 'CENSUST',
    'NEWSEC', 'CENSUS4', 'FISSPURP', 'COLLDESC', 'SECTOLD', 'FLAG3', 'PAYIND',
    'REMAINMX', 'ORIGMX', 'NXBILDT', 'NXTBIL', 'ASSMDATE', 'CURCODE',
    'CFINDEX', 'WRITE_DOWN_BAL', 'OLDNOTEDAYARR', 'OLDNOTEBLDATE',
    'PURPOSES', 'DNBFISME', 'CRISPURP', 'VB', 'FRELEAS', 'ESCRACCT',
    'SIACCTNO', 'ABM_HL', 'IA_LRU', 'ASCORE_PERM', 'ASCORE_LTST',
    'UNEARNED1', 'UNEARNED2', 'UNEARNED', 'FDACCTNO', 'FDCERTNO',
    'CCRIS_INSTLAMT', 'INDUSTRIAL_SECTOR_CD', 'SECTORCD_ORI',
    'INVALID_LOC', 'STATE', 'ASCORE_COMM', 'INFEE', 'SCH_REPAY_TERM',
]

# ULOAN keep-list columns (mirrors %LET ULOAN=(...))
ULOAN_KEEP = [
    'BRANCH', 'ACCTNO', 'NAME', 'CUSTCD', 'PRODUCT', 'PRODCD', 'RLEASAMT',
    'SECTORCD', 'APPRDATE', 'ORIGMT', 'APPRLIMT', 'UNDRAWN', 'EXPRDATE',
    'CUSTCODE', 'COMMNO', 'AMTIND', 'FISSPURP', 'COSTCTR', 'CURCODE',
    'CRISPURP', 'CCRICODE', 'INDUSTRIAL_SECTOR_CD', 'SECTORCD_ORI',
]

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ============================================================================
# HELPER: parse packed SAS date integer (Z11 → MMDDYYYY)
# ============================================================================
def parse_sas_z11_date(val) -> Optional[date]:
    if val is None or val == 0:
        return None
    try:
        s = f"{int(val):011d}"[:8]
        return datetime.strptime(s, "%m%d%Y").date()
    except (ValueError, TypeError):
        return None


# ============================================================================
# HELPER: safe keep — only retain columns that exist in the dataframe
# ============================================================================
def safe_keep(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    existing = [c for c in cols if c in df.columns]
    return df.select(existing)


# ============================================================================
# HELPER: derive STATECD (mirrors the three-branch STATECD logic)
# ============================================================================
def derive_statecd_od(state: str, product: int, branch: int) -> tuple[str, bool]:
    invalid_loc = False
    if state in POSTCD_SET:
        statecd = format_statecd(state)   # $STATEPOST. — placeholder
    elif product in FCY_SET and state in COUNTRYCD_SET:
        statecd = 'Z'
    else:
        statecd = format_statecd(state)
        if product in FCY_SET:
            if state in ('00', '99', '099', '0099', '00099', '000099'):
                statecd = 'Z'
    if statecd == ' ':
        invalid_loc = True
        statecd = format_statecd(str(branch))
    return statecd, invalid_loc


# ============================================================================
# HELPER: FISSPURP derivation for OD (mirrors both OVDFT and UOD blocks)
# $CRISCD. and $FISSPUR. are placeholder formats — replace when available.
# ============================================================================
def derive_fisspurp_od(sectold: str, sectorcd: str, purposes: str,
                       custcd: str, acctno: int, product: int,
                       apprdate: Optional[date]) -> tuple[str, str]:
    """
    Derive FISSPURP and revised CUSTCD for OD accounts.
    Returns (fisspurp, custcd).
    """
    fisspurp = ''

    if sectold.strip() not in ('', '    '):
        fisspurp = sectold   # placeholder: replace with format_fisspur(sectold)
        if sectold in ('0410', '0430') and custcd not in ('77', '78', '95', '96'):
            if custcd in CUSTCD_TO_77:
                custcd = '77'
            elif custcd in CUSTCD_TO_78:
                custcd = '78'
        # Commented-out block retained:
        # ELSE IF SECTORCD IN ('0211','0212','0220','0230') AND
        #         CUSTCD NOT IN ('77','78','95','96') THEN SECTORCD='9999';

    if apprdate is not None and apprdate < APR06_CUTOFF:
        if '0110' <= sectorcd <= '0430':
            fisspurp = sectorcd   # placeholder: replace with format_fisspur(sectorcd)
        # * ELSE IF '1111' <= SECTORCD <= '9600' THEN FISSPURP = '0470';
        else:
            fisspurp = purposes   # placeholder: replace with format_criscd(purposes)
        if custcd in ('77', '78', '95', '96') and 3_000_000_000 < acctno < 4_000_000_000:
            fisspurp = '0410'
    else:
        fisspurp = purposes   # placeholder: replace with format_criscd(purposes)

    if fisspurp >= '1111':
        fisspurp = purposes   # placeholder: replace with format_criscd(purposes)
    if product == 34:
        fisspurp = '0110'

    return fisspurp, custcd


# ============================================================================
# HELPER: SECTORCD invalid fallback mapping
# Mirrors DATA LOAN&REPTMON&NOWK / DATA ULOAN&REPTMON&NOWK SECVALID blocks.
# $VALIDSE. format is a placeholder — replace with format_validse(sector).
# ============================================================================
def apply_sector_fallback(sector: str, sectorcd: str) -> str:
    """
    Apply SECTORCD fallback rules when sector code is INVALID.
    $VALIDSE. format is a placeholder; the is_invalid logic below should be
    replaced with: if format_validse(sector) == 'INVALID'.
    """
    # placeholder: replace condition with format_validse(sector) == 'INVALID'
    def _is_invalid(s: str) -> bool:
        return False   # placeholder — implement when $VALIDSE. format is available

    if not _is_invalid(sector):
        return sectorcd

    s1 = sector[:1] if sector else ''
    s2 = sector[:2] if len(sector) >= 2 else ''

    if s1 == '1':   return '1400'
    if s1 == '2':   return '2900'
    if s1 == '3':   return '3919'
    if s1 == '4':   return '4010'
    if s1 == '5':   return '5999'
    if s2 == '61':  return '6120'
    if s2 == '62':  return '6130'
    if s2 == '63':  return '6310'
    if s2 in ('64', '65', '66', '67', '68', '69'): return '6130'
    if s1 == '7':   return '7199'
    if s2 in ('81', '82'): return '8110'
    if s2 in ('83', '84', '85', '86', '87', '88', '89'): return '8999'
    if s2 == '91':  return '9101'
    if s2 == '92':  return '9410'
    if s2 in ('93', '94', '95'): return '9499'
    if s2 in ('96', '97', '98', '99'): return '9999'
    return sectorcd


# ============================================================================
# HELPER: SECTORCD / CUSTCD / FISSPURP final normalisation
# Shared between LOAN and ULOAN final processing steps.
# ============================================================================
def normalise_sector_custcd(r: dict, is_fcy: bool) -> dict:
    """
    Apply the full chain of SECTORCD/CUSTCD/FISSPURP normalisation rules
    that appear in DATA BNM.LOAN and DATA ULOAN steps.
    Modifies and returns the row dict.
    """
    custcd   = str(r.get('CUSTCD',   '') or '')
    sectorcd = str(r.get('SECTORCD', '') or '')
    sector   = str(r.get('SECTOR',   '') or '')
    fisspurp = str(r.get('FISSPURP', '') or '')
    acctno   = r.get('ACCTNO', 0) or 0
    crispurp = str(r.get('CRISPURP', '') or '')
    ccricode = r.get('CCRICODE', 0) or 0

    # Remap custcd 77/78 for business sector codes
    if custcd in ('77', '78') and '1111' <= sector <= '9999' and sector != '9700':
        if custcd == '77': custcd = '41'
        elif custcd == '78': custcd = '44'

    # Financial institution custcds with sector 8110/8120/8130
    fi_custcds = {
        '01', '02', '03', '11', '12', '20', '13', '17', '30', '31', '32',
        '33', '34', '35', '36', '37', '38', '39', '40', '45', '04', '05',
        '06', '80', '81', '82', '83', '84', '85', '86', '87', '88', '90'
    }
    if custcd not in fi_custcds and sector in ('8110', '8120', '8130'):
        sectorcd = '9999'

    if custcd in ('77', '78', '95', '96') and '0111' <= sector <= '0430':
        sectorcd = '0410'

    if custcd not in ('77', '78', '95', '96') and sectorcd < '1111':
        cb_set = {'02', '03', '04', '05', '06', '11', '12', '13', '17',
                  '30', '31', '32', '33', '34', '35', '37', '38', '39', '45'}
        if custcd in cb_set:
            sectorcd = '8110'
        elif custcd == '40':
            sectorcd = '8130'
        elif custcd == '79':
            sectorcd = '9203'
        elif custcd in ('71', '72', '73', '74'):
            sectorcd = '9101'
        else:
            sectorcd = '9999'

    # Domestic-only custcds with 8110/8120/8130
    dom_custcds = {
        '79', '61', '62', '63', '75', '57', '59', '41', '42', '43',
        '44', '46', '47', '48', '49', '51', '52', '53', '54'
    }
    if custcd in dom_custcds and sectorcd in ('8110', '8120', '8130'):
        sectorcd = '9999'

    cb_set2 = {'02', '03', '04', '05', '06', '11', '12', '13', '17',
               '30', '31', '32', '33', '34', '35', '37', '38', '39', '40', '45'}
    if custcd in cb_set2 and sectorcd not in ('8110', '8120', '8130'):
        sectorcd = '8110'

    if custcd in ('71', '72', '73', '74'):
        if sectorcd[:2] not in ('91', '92', '93', '94', '95', '96', '97', '98', '99'):
            sectorcd = '9101'

    if custcd in ('77', '78') and '1111' <= sectorcd <= '9999' and sectorcd != '9700':
        if custcd == '77': custcd = '41'
        elif custcd == '78': custcd = '44'

    # FISSPURP '4300'/'4100' → '0990'
    biz_custcds = {
        '01', '02', '03', '11', '12', '20', '13', '17', '30', '31', '32',
        '33', '34', '35', '36', '37', '38', '39', '40', '45', '04', '05',
        '06', '61', '41', '42', '43', '62', '44', '46', '47', '63', '48',
        '49', '51', '57', '75', '59', '52', '53', '54', '66', '67', '68', '69'
    }
    if custcd in biz_custcds and crispurp in ('4300', '4100'):
        fisspurp = '0990'

    if custcd == '79':
        if sectorcd not in ('9203', '9420', '9440'):
            sectorcd = '9203'

    # FCY / non-FCY foreign custcd remapping
    product = r.get('PRODUCT', 0) or 0
    if product in FCY_SET:
        fcy_foreign = {'86', '90', '91', '92', '95', '96', '98', '99'}
        if custcd in fcy_foreign and '1000' <= sector <= '9999' and sector != '9700':
            custcd = '86'
    else:
        non_fcy_foreign = {'82', '83', '84', '86', '90', '91', '95', '96', '98', '99'}
        if custcd in non_fcy_foreign and '1000' <= sector <= '9999' and sector != '9700':
            custcd = '86'

    # Individual/retail custcds — HP / CA / other FISSPURP assignments
    indiv_custcds = {'77', '78', '95', '96'}
    if custcd in indiv_custcds:
        if 3_000_000_000 < acctno < 4_000_000_000:
            if (crispurp.strip() == '' and (ccricode == 0 or ccricode is None)
                    and r.get('SECTOLD', '').strip() in ('', '    ')):
                fisspurp = '0410'
            elif fisspurp in ('0390', '0440', '0470'):
                fisspurp = '0410'
        elif product in HP_SET:
            if (crispurp.strip() == '' and (ccricode == 0 or ccricode is None)
                    and r.get('SECTOLD', '').strip() in ('', '    ')):
                fisspurp = '0212'
            elif fisspurp in ('0390', '0440', '0470'):
                fisspurp = '0212'
        else:
            if (crispurp.strip() == '' and (ccricode == 0 or ccricode is None)
                    and r.get('SECTOLD', '').strip() in ('', '    ')):
                fisspurp = '0410'
            elif fisspurp in ('0390', '0440', '0470'):
                fisspurp = '0410'

        valid_fisspurp = {
            '0110', '0120', '0131', '0132', '0139', '0210', '0220', '0230',
            '0200', '0211', '0212', '0311', '0312', '0313', '0314', '0315',
            '0316', '0321', '0322', '0323', '0324', '0325', '0326', '0327',
            '0328', '0329', '0410', '0420', '0430', '0990'
        }
        if fisspurp in valid_fisspurp:
            sectorcd = '9700'

    # HP FISSPURP override (PRODCD='34111')
    prodcd = str(r.get('PRODCD', '') or '')
    if prodcd == '34111' and fisspurp not in ('0211', '0212', '0200', '0210', '0390', '0430'):
        fisspurp = '0212'

    # Staff loan (34230) overrides
    if prodcd == '34230':
        if custcd in ('77', '78', '95', '96'):
            sectorcd = '9700'
        elif custcd in ('61', '41', '42', '43', '64', '52', '53', '54'):
            custcd = '77'; sectorcd = '9700'
        elif custcd in ('62', '44', '46', '47'):
            custcd = '78'; sectorcd = '9700'
        else:
            sectorcd = '9700'

    r['CUSTCD']   = custcd
    r['SECTORCD'] = sectorcd
    r['FISSPURP'] = fisspurp
    r['SECTPORI'] = sectorcd
    return r


# ============================================================================
# STEP 1: Load DEPOSIT.CURRENT merged with BNM.SME08
# ============================================================================
def load_current(sme08: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SORT DATA=DEPOSIT.CURRENT; DATA CURRENT (MERGE with BNM.SME08).
    Keeps all CURRENT rows; attaches SECTOLD from SME08 where ACCTNO matches.
    """
    current = pl.read_parquet(DEPOSIT_CURRENT_PATH).sort("ACCTNO")
    sme08_s = sme08.select(["ACCTNO", "NOTENO", "SECTOLD"]).sort("ACCTNO")
    merged  = current.join(sme08_s, on="ACCTNO", how="left")
    return merged


# ============================================================================
# STEP 2: Build BNM.OVDFT — utilised OD accounts (CURBAL < 0)
# ============================================================================
def build_ovdft(current: pl.DataFrame, reptmon: str, nowk: str) -> pl.DataFrame:
    """
    DATA BNM.OVDFT&REPTMON&NOWK:
      Filter OPENIND not in ('B','C','P') AND CURBAL < 0.
      Compute BALANCE, INTRATE, APPRDATE, STATECD, PRODCD, AMTIND,
      PURPOSES, FISSPURP, CUSTCD, CLOSEDTE, ORIGMT, REMAINMT, COLLCD,
      RISKCD, RLEASAMT, APPRLIM2, UNDRAWN, BALMNI.
    """
    df = current.filter(
        ~pl.col("OPENIND").is_in(["B", "C", "P"]) &
        (pl.col("CURBAL") < 0)
    )

    result_rows = []
    for r in df.to_dicts():
        curbal   = float(r.get('CURBAL',   0) or 0)
        odintacc = float(r.get('ODINTACC', 0) or 0)
        rate1    = float(r.get('RATE1',    0) or 0)
        todrate  = float(r.get('TODRATE',  0) or 0)
        product  = r.get('PRODUCT', 0) or 0
        custcode = r.get('CUSTCODE', 0) or 0
        state    = str(r.get('STATE', '') or '')
        branch   = r.get('BRANCH', 0) or 0
        sector   = str(r.get('SECTOR', '') or '')
        sectold  = str(r.get('SECTOLD', '') or '').ljust(4)
        riskcode = str(r.get('RISKCODE', '') or '')
        apprlimt = float(r.get('APPRLIMT', 0) or 0)
        opendt   = r.get('OPENDT')
        closedt  = r.get('CLOSEDT', 0) or 0
        ccricode = r.get('CCRICODE', 0) or 0
        acctno   = r.get('ACCTNO', 0) or 0

        # BALANCE
        balance = (-1) * curbal
        if odintacc > 0:
            balance = balance + odintacc

        # INTRATE
        intrate = rate1 if rate1 > 0 else todrate   # FOR OD WITHOUT LIMIT

        # APPRDATE
        apprdate = parse_sas_z11_date(opendt)

        # SECTORCD — $SECTCD. placeholder
        sectorcd = f"{int(sector):04d}" if sector.strip().isdigit() else sector.ljust(4)
        # placeholder: replace with format_sectcd(f"{int(sector):04d}")

        # STATECD
        statecd, invalid_loc = derive_statecd_od(state, product, branch)

        # PRODCD — ODPROD. format placeholder
        # placeholder: replace with format_odprod(product)
        prodcd = str(product)   # placeholder

        # AMTIND — ODDENOM
        amtind = format_oddenom(product)

        # PURPOSES — $CRISCD. placeholder
        ccricode_str = f"{int(ccricode):04d}" if ccricode else '    '
        purposes = ccricode_str   # placeholder: replace with format_criscd(ccricode_str)

        # FISSPURP + CUSTCD
        fisspurp, custcd_tmp = derive_fisspurp_od(
            sectold, sectorcd, purposes,
            str(r.get('CUSTCD', '') or ''), acctno, product, apprdate
        )

        # Vostro overrides
        # IF VOSTRO ACCOUNT THEN COUNTER PARTY CODE = CB '02' OR '81'
        #   104 = VOSTRO LOCAL, 105 = VOSTRO FOREIGN
        if product == 104:
            custcd = '02'
        elif product == 105:
            custcd = '81'
        else:
            custcd = format_odcustcd(custcode)

        # CLOSEDTE
        closedte = parse_sas_z11_date(closedt) if closedt else None

        # ORIGMT — ORIGINAL MATURITY FOR OD IS ALWAYS <= 1 YEAR, CODE='10'
        origmt = '10'
        remainmh = 0

        # REMAINMT — REMAINING MATURITY FOR OD NOT APPLICABLE, CODE='50'
        remainmt = '50'

        # COLLCD — ALL OD AS '30570' (OTHER FORMS OF COLLATERAL)
        collcd = '30570'

        # RISKCD, RISKRTE — NON-PERFORMING OD ACCOUNT
        riskcd  = format_riskcd(riskcode)
        try:
            riskrte = int(riskcode[:1]) if riskcode else 0
        except (ValueError, TypeError):
            riskrte = 0

        # RLEASAMT, APPRLIM2, UNDRAWN, BALMNI
        rleasamt = balance
        apprlim2 = apprlimt
        undrawn  = max(0.0, apprlimt - rleasamt)
        if product == 167 and balance < 0:
            undrawn = apprlimt

        # REQUESTED BY RAYMOND(MIS) EXCLUDE ODINTACC
        balmni = balance

        r.update({
            'BALANCE':   balance,
            'INTRATE':   intrate,
            'APPRDATE':  apprdate,
            'SECTORCD':  sectorcd,
            'STATECD':   statecd,
            'INVALID_LOC': 'Y' if invalid_loc else '',
            'PRODCD':    prodcd,
            'AMTIND':    amtind,
            'PURPOSES':  purposes,
            'FISSPURP':  fisspurp,
            'CUSTCD':    custcd,
            'CLOSEDTE':  closedte,
            'ORIGMT':    origmt,
            'REMAINMH':  remainmh,
            'REMAINMT':  remainmt,
            'COLLCD':    collcd,
            'RISKCD':    riskcd,
            'RISKRTE':   riskrte,
            'RLEASAMT':  rleasamt,
            'APPRLIM2':  apprlim2,
            'UNDRAWN':   undrawn,
            'BALMNI':    balmni,
        })
        result_rows.append(r)

    return pl.DataFrame(result_rows) if result_rows else pl.DataFrame()


# ============================================================================
# STEP 3: Trim NOTE to LOAN keep-list, combine NOTE + OVDFT → BNM.LOAN
# ============================================================================
def build_loan(note_df: pl.DataFrame, ovdft_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA BNM.NOTE (trim to LOAN keep-list).
    DATA BNM.LOAN (SET NOTE + OVDFT, add ACCTYPE, adjust DNBFISME, INFEE).
    """
    note_trim  = safe_keep(note_df,  LOAN_KEEP)
    ovdft_trim = safe_keep(ovdft_df, LOAN_KEEP)

    note_trim  = note_trim.with_columns( pl.lit('LN').alias('ACCTYPE'))
    ovdft_trim = ovdft_trim.with_columns(pl.lit('OD').alias('ACCTYPE'))

    combined = pl.concat([note_trim, ovdft_trim], how="diagonal")

    # DNBFI_ORI = DNBFISME; IF CUSTCD NOT IN (...) THEN DNBFISME = '0'
    # 2022-3846 & 2022-3850: INFEE > 0 → adjust FEEAMT and ACCRUAL
    result_rows = []
    for r in combined.to_dicts():
        custcd   = str(r.get('CUSTCD',   '') or '')
        dnbfisme = str(r.get('DNBFISME', '') or '')
        infee    = float(r.get('INFEE',  0) or 0)
        feeamt   = float(r.get('FEEAMT', 0) or 0)
        accrual  = float(r.get('ACCRUAL',0) or 0)

        r['DNBFI_ORI'] = dnbfisme
        if custcd not in DNBFI_CODES:
            r['DNBFISME'] = '0'

        if infee > 0:
            r['FEEAMT']  = feeamt  - infee
            r['ACCRUAL'] = accrual + infee

        result_rows.append(r)

    return pl.DataFrame(result_rows) if result_rows else pl.DataFrame()


# ============================================================================
# STEP 4: Build BNM.UOD — unutilised OD accounts (CURBAL >= 0)
# ============================================================================
def build_uod(current: pl.DataFrame) -> pl.DataFrame:
    """
    DATA BNM.UOD: Filter OPENIND not in ('B','C','P'), CURBAL>=0, APPRLIMT>0.
    Mirrors the ULOAN step for OD unutilised accounts.
    """
    df = current.filter(
        ~pl.col("OPENIND").is_in(["B", "C", "P"]) &
        (pl.col("CURBAL") >= 0) &
        (pl.col("APPRLIMT") > 0)
    )

    result_rows = []
    for r in df.to_dicts():
        product  = r.get('PRODUCT', 0) or 0
        custcode = r.get('CUSTCODE', 0) or 0
        state    = str(r.get('STATE', '') or '')
        branch   = r.get('BRANCH', 0) or 0
        sector   = str(r.get('SECTOR', '') or '')
        sectold  = str(r.get('SECTOLD', '') or '').ljust(4)
        apprlimt = float(r.get('APPRLIMT', 0) or 0)
        opendt   = r.get('OPENDT')
        ccricode = r.get('CCRICODE', 0) or 0
        acctno   = r.get('ACCTNO', 0) or 0

        # SECTORCD — $SECTCD. placeholder
        sectorcd = f"{int(sector):04d}" if sector.strip().isdigit() else sector.ljust(4)

        # PRODCD — ODPROD. placeholder
        prodcd = str(product)   # placeholder: replace with format_odprod(product)

        # AMTIND
        amtind = format_oddenom(product)

        # APPRDATE
        apprdate = parse_sas_z11_date(opendt)

        # PURPOSES
        ccricode_str = f"{int(ccricode):04d}" if ccricode else '    '
        purposes = ccricode_str   # placeholder: replace with format_criscd(ccricode_str)

        # FISSPURP + CUSTCD
        fisspurp, custcd_tmp = derive_fisspurp_od(
            sectold, sectorcd, purposes,
            str(r.get('CUSTCD', '') or ''), acctno, product, apprdate
        )

        # Vostro overrides
        if product == 104:
            custcd = '02'
        elif product == 105:
            custcd = '81'
        else:
            custcd = format_odcustcd(custcode)

        # ORIGMT — ALWAYS <= 1 YEAR
        origmt = '10'

        # RLEASAMT = 0, APPRLIM2 = APPRLIMT, UNDRAWN = APPRLIMT
        rleasamt = 0.0
        apprlim2 = apprlimt
        undrawn  = apprlimt

        r.update({
            'SECTORCD': sectorcd,
            'PRODCD':   prodcd,
            'AMTIND':   amtind,
            'APPRDATE': apprdate,
            'PURPOSES': purposes,
            'FISSPURP': fisspurp,
            'CUSTCD':   custcd,
            'ORIGMT':   origmt,
            'RLEASAMT': rleasamt,
            'APPRLIM2': apprlim2,
            'UNDRAWN':  undrawn,
        })
        result_rows.append(r)

    return pl.DataFrame(result_rows) if result_rows else pl.DataFrame()


# ============================================================================
# STEP 5: Build BNM.ULOAN — combine UNOTE + UOD
# ============================================================================
def build_uloan(unote_df: pl.DataFrame, uod_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA BNM.ULOAN: SET UNOTE (ACCTYPE='LN') + UOD (ACCTYPE='OD').
    """
    unote_trim = safe_keep(unote_df, ULOAN_KEEP)
    uod_trim   = safe_keep(uod_df,   ULOAN_KEEP)

    unote_trim = unote_trim.with_columns( pl.lit('LN').alias('ACCTYPE'))
    uod_trim   = uod_trim.with_columns(  pl.lit('OD').alias('ACCTYPE'))

    return pl.concat([unote_trim, uod_trim], how="diagonal")


# ============================================================================
# HELPER: sector reversal ($RVRSE. format) — placeholder
# ============================================================================
def format_rvrse(sectorz: str) -> str:
    """
    $RVRSE. format — placeholder.
    Replace with actual reversal mapping when format table is available.
    Returns empty string (no reversal) by default.
    """
    return '    '   # placeholder


# ============================================================================
# STEP 6: Sector/SECTORCD normalisation pass (LOAN and ULOAN)
# Mirrors: DATA LOAN&REPTMON&NOWK (three sequential DATA steps).
# ============================================================================
def normalise_loan_sector(df: pl.DataFrame, acctype_filter: Optional[str] = None) -> pl.DataFrame:
    """
    Three-pass normalisation matching the SAS DATA LOAN/ULOAN steps:
      Pass 1: $RVRSE. reversal for LN with ISSDTE < 01APR06; SECTORCD/SECTOR update
      Pass 2: $VALIDSE. fallback + FISSPORI assignment
      Pass 3: final SECTORCD/CUSTCD/FISSPURP normalisation
    """
    result_rows = []
    for r in df.to_dicts():
        acctype = str(r.get('ACCTYPE', '') or '')
        issdte  = r.get('ISSDTE')

        # --- Pass 1: SECTORZ / SECTORZZ / SECTOR ---
        sectorc = str(r.get('SECTORCD', '') or r.get('SECTOR', '') or '')
        try:
            sectorx = int(float(sectorc)) if sectorc.strip() else 0
        except (ValueError, TypeError):
            sectorx = 0
        sectorz  = f"{sectorx:04d}"
        sectorzz = '    '

        if acctype == 'LN' and issdte and issdte < APR06_ISSSDTE:
            sectorzz = format_rvrse(sectorz)   # $RVRSE. placeholder

        if sectorzz.strip():
            sectorcd = sectorzz
            sector   = sectorzz
        else:
            sectorcd = sectorz
            sector   = sectorz

        r['SECTORCD'] = sectorcd
        r['SECTOR']   = sector

        # --- Pass 2: $VALIDSE. fallback + FISSPORI ---
        sectorcd = apply_sector_fallback(sector, sectorcd)
        r['SECTORCD'] = sectorcd
        r['FISSPORI'] = str(r.get('FISSPURP', '') or '')

        # --- Pass 3: SECTOR numeric re-encode then full normalise ---
        try:
            sectorx2 = int(float(r.get('SECTOR', '') or 0))
        except (ValueError, TypeError):
            sectorx2 = 0
        r['SECTOR'] = f"{sectorx2:04d}"

        r = normalise_sector_custcd(r, r.get('PRODUCT', 0) in FCY_SET)

        # CRISPURP update for CA accounts (3xxx ACCTNO range)
        acctno   = r.get('ACCTNO', 0) or 0
        ccricode = r.get('CCRICODE', 0) or 0
        purposes = str(r.get('PURPOSES', '') or '')
        if 3_000_000_000 < acctno < 4_000_000_000:
            if ccricode not in (0, None):
                r['CRISPURP'] = f"{int(ccricode):04d}"   # placeholder: format_criscd(...)
            else:
                r['CRISPURP'] = purposes
        r.pop('PURPOSES', None)

        result_rows.append(r)

    return pl.DataFrame(result_rows) if result_rows else pl.DataFrame()


# ============================================================================
# STEP 7: CIS individual customer matching (INDOD + INDLOAN)
# Mirrors DATA LNINDOD and DATA LNINDLN.
# ============================================================================
def apply_cis_ind(df: pl.DataFrame, cis_df: pl.DataFrame, is_od: bool) -> pl.DataFrame:
    """
    Merge with CIS (CISCADP.DEPOSIT for OD, CISCALN.LOAN for LN).
    Apply NEWICIND/BUSSIND classification rules for individual borrowers.
    """
    merged = df.join(cis_df, on="ACCTNO", how="left", suffix="_CIS")

    result_rows = []
    for r in merged.to_dicts():
        newicind = str(r.get('NEWICIND', '') or '')
        bussind  = str(r.get('BUSSIND',  '') or '')
        custcd   = str(r.get('CUSTCD',   '') or '')
        sectorcd = str(r.get('SECTORCD', '') or '')
        fisspori = str(r.get('FISSPORI', '') or '')
        fisspurp = str(r.get('FISSPURP', '') or '')
        sectpori = str(r.get('SECTPORI', '') or '')
        acctno   = r.get('ACCTNO', 0) or 0
        product  = r.get('PRODUCT', 0) or 0
        issdte   = r.get('ISSDTE')
        liabcode = str(r.get('LIABCODE', '') or '')
        crispurp = str(r.get('CRISPURP', '') or '')

        if newicind in ('BC', 'IC', 'ML', 'PL'):
            # REMOVE FROM ESMR 2024-357: custcd remapping block removed
            # ESMR 2014-261: maintain original FISSPURP
            fisspurp = fisspori
            sectorcd = '9700'

        if newicind == 'PP':
            custori = custcd
            if custcd in ('95', '96'):
                fisspurp = fisspori
            else:
                custcd = '95'
                # ESMR 2014-0261: maintain original FISSPURP
                fisspurp = fisspori
            sectorcd = '9700'

        if newicind in ('BR', 'CI', 'PC', 'SA') or bussind in ('BR', 'CI', 'PC', 'SA'):
            # REMOVE FROM ESMR 2024-357: custori restore removed
            if custcd in ('77', '78', '95', '96'):
                if custcd == '77':
                    custcd = '61'
                    if fisspori in ('0410', '0420', '0430'):
                        fisspurp = '0470' if 3_000_000_000 < acctno < 4_000_000_000 else '0990'
                    else:
                        fisspurp = fisspori
                    sectorcd = '9999' if sectorcd in ('0410', '9700') else sectpori
                if custcd == '95':
                    custcd = '86'
                    if fisspori in ('0410', '0420', '0430'):
                        fisspurp = '0470' if 3_000_000_000 < acctno < 4_000_000_000 else '0990'
                        sectorcd = '9999'
                    else:
                        fisspurp = fisspori
                        sectorcd = sectpori
                if custcd == '78':
                    custcd = '62'
                    if fisspori in ('0410', '0420', '0430'):
                        fisspurp = '0470' if 3_000_000_000 < acctno < 4_000_000_000 else '0990'
                    else:
                        fisspurp = fisspori
                    sectorcd = '9999' if sectorcd in ('0410', '9700') else sectpori
            else:
                sectorcd = sectpori

            sme_custcds = {
                '41', '42', '43', '44', '46', '47', '48', '49', '51',
                '52', '53', '54', '60', '61', '62', '63', '64', '65',
                '59', '75', '57'
            }
            if custcd in sme_custcds and sectorcd == '9700':
                sectorcd = '9999'
            if sectorcd < '1111':
                sectorcd = '9999'

        # HP FISSPURP override for loans (not OD) with ISSDTE >= 01OCT09
        if not is_od and product in HP_SET and issdte and issdte >= OCT09_CUTOFF:
            c7_group = {'C7', 'D1', 'D4', 'C9', 'D3', 'D6'}
            c8_group = {'C8', 'D2', 'D5'}
            d7_group = {'D7', 'D9', 'E2', 'E5', 'E6', 'D8', 'E1', 'E3'}
            if liabcode in c7_group:
                fisspurp = '0211'
            elif liabcode in c8_group:
                fisspurp = '0212'
            elif liabcode in d7_group:
                fisspurp = '0200'
            elif liabcode == '63':
                fisspurp = '0390'
            elif liabcode == '22':
                if crispurp == '4100':   fisspurp = '0430'
                elif crispurp == '5100': fisspurp = '0390'
                else:                    fisspurp = '0200'

        r['CUSTCD']   = custcd
        r['SECTORCD'] = sectorcd
        r['FISSPURP'] = fisspurp
        result_rows.append(r)

    return pl.DataFrame(result_rows) if result_rows else pl.DataFrame()


# ============================================================================
# STEP 8: Final BNM.LOAN consolidation (FCY rates, APPRLIM2 rules, FISSPURP)
# ============================================================================
def finalise_loan(df: pl.DataFrame, forate_map: dict) -> pl.DataFrame:
    """
    DATA BNM.LOAN (final):
      - Special case for ACCTNO=8954892013
      - FCY conversion (PRODUCT 800-899) using FORATE map
      - APPRLIM2ORI / APPRLIM2 zeroing rules (ESMR 2020-1452)
      - HP CRISPURP → FISSPURP override (ESMR 2014-791)
      - SECTORCD_ORI + INDUSTRIAL_SECTOR_CD override
      - CUSTCD 77/78 → 9700
      - Split into LOAN / LNWOD / LNWOF
    """
    # PROC FORMAT LIB=FORATE / PROC FORMAT CNTLIN — load from forate_map (pre-loaded)

    result_rows = []
    for r in df.to_dicts():
        acctno   = r.get('ACCTNO', 0) or 0
        product  = r.get('PRODUCT', 0) or 0
        curcode  = str(r.get('CURCODE', '') or '')
        balance  = float(r.get('BALANCE', 0) or 0)
        balmni   = float(r.get('BALMNI',  0) or 0)
        curbal   = float(r.get('CURBAL',  0) or 0)
        feeamt   = float(r.get('FEEAMT',  0) or 0)
        accrual  = float(r.get('ACCRUAL', 0) or 0)
        undrawn  = float(r.get('UNDRAWN', 0) or 0)
        apprlim2 = float(r.get('APPRLIM2',0) or 0)
        apprlimt = float(r.get('APPRLIMT',0) or 0)
        noteno   = r.get('NOTENO', 0) or 0
        custcd   = str(r.get('CUSTCD',   '') or '')
        sectorcd = str(r.get('SECTORCD', '') or '')
        fisspurp = str(r.get('FISSPURP', '') or '')
        prodcd   = str(r.get('PRODCD',   '') or '')
        crispurp = str(r.get('CRISPURP', '') or '')
        industrial_sector_cd = str(r.get('INDUSTRIAL_SECTOR_CD', '') or '')

        # Special case override
        if acctno == 8_954_892_013:
            balance  = curbal + accrual + feeamt
            apprlim2 = apprlimt

        # FCY conversion (PRODUCT 800–899)
        forate = 1.0
        if 800 <= product < 900:
            forate = float(forate_map.get(curcode, 1.0))
            r['FORATE']   = forate
            r['FCYBAL']   = balance
            balance  *= forate
            balmni   *= forate
            curbal   *= forate
            feeamt   *= forate
            accrual  *= forate
            undrawn  *= forate
            r['APPR2FCY'] = apprlim2
            apprlim2 *= forate

        # APPRLIM2ORI / zeroing (ESMR 2020-1452)
        r['APPRLIM2ORI'] = apprlim2
        if (
            (2_500_000_000 <= acctno <= 2_599_999_999 or
             2_850_000_000 <= acctno <= 2_859_999_999)
            and 40_000 <= noteno <= 49_999
        ) or product in (321, 444):
            apprlim2 = 0.0

        # RENAME CURCODE = CCY (FOR FCY LOAN)
        r['CCY'] = curcode

        # HP FISSPURP override from CRISPURP (ESMR 2014-791)
        hp_extended = {128, 130, 131, 132, 380, 381, 700, 705, 720, 725,
                       750, 752, 760, 983, 993}
        if product in hp_extended:
            if crispurp == '3110':  fisspurp = '0211'
            elif crispurp == '3120': fisspurp = '0212'
            elif crispurp in ('3200', '3201', '3300', '3900', '3901'): fisspurp = '0200'
            elif crispurp == '4100': fisspurp = '0430'
            elif crispurp in ('5100', '5200'): fisspurp = '0390'
            if prodcd == '34111' and fisspurp not in (
                    '0211', '0212', '0200', '0210', '0390', '0430'):
                fisspurp = '0212'

        # SECTORCD_ORI + INDUSTRIAL_SECTOR_CD override
        r['SECTORCD_ORI'] = sectorcd
        if (industrial_sector_cd and len(industrial_sector_cd) == 5
                and industrial_sector_cd > '0'):
            # placeholder: replace with format_indsect(industrial_sector_cd)
            sectorcd = industrial_sector_cd   # placeholder

        # CUSTCD 77/78/95/96 → SECTORCD = '9700'
        if custcd in ('77', '78', '95', '96'):
            sectorcd = '9700'

        r.update({
            'BALANCE':  balance,  'BALMNI':   balmni,  'CURBAL':   curbal,
            'FEEAMT':   feeamt,   'ACCRUAL':  accrual, 'UNDRAWN':  undrawn,
            'APPRLIM2': apprlim2, 'CUSTCD':   custcd,  'SECTORCD': sectorcd,
            'FISSPURP': fisspurp,
        })
        result_rows.append(r)

    return pl.DataFrame(result_rows) if result_rows else pl.DataFrame()


# ============================================================================
# STEP 9: Split finalised LOAN into LOAN / LNWOD / LNWOF
# ============================================================================
def split_loan(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    DATA BNM.LOAN / BNM.LNWOD / BNM.LNWOF split:
      LN + ACCTNO < 2999999999            → LOAN
      LN + ACCTNO > 8000000000 + PRODCD=M → LNWOD
      LN + ACCTNO > 8000000000 + PRODCD=N → LNWOF
      LN + ACCTNO > 8000000000 + other    → LOAN
      OD                                  → LOAN
    """
    loan_rows  = []
    lnwod_rows = []
    lnwof_rows = []

    for r in df.to_dicts():
        acctype = str(r.get('ACCTYPE', '') or '')
        acctno  = r.get('ACCTNO', 0) or 0
        prodcd  = str(r.get('PRODCD', '') or '')

        if acctype == 'LN':
            if acctno < 2_999_999_999:
                loan_rows.append(r)
            elif acctno > 8_000_000_000:
                if prodcd == 'M':
                    lnwod_rows.append(r)
                elif prodcd == 'N':
                    lnwof_rows.append(r)
                else:
                    loan_rows.append(r)
        else:
            loan_rows.append(r)

    to_df = lambda rows: pl.DataFrame(rows) if rows else pl.DataFrame()
    return to_df(loan_rows), to_df(lnwod_rows), to_df(lnwof_rows)


# ============================================================================
# STEP 10: Finalise BNM.ULOAN (FCY conversion + SECTORCD_ORI + CUSTCD 9700)
# ============================================================================
def finalise_uloan(df: pl.DataFrame, forate_map: dict) -> pl.DataFrame:
    """
    DATA BNM.ULOAN (DROP=FORATE):
      FCY conversion for PRODUCT 800–899.
      FISSPURP > '1000' → '0990'.
      SECTORCD blank → '9999'.
      SECTORCD_ORI assignment.
      INDUSTRIAL_SECTOR_CD override.
      CUSTCD 77/78/95/96 → SECTORCD = '9700'.
    """
    result_rows = []
    for r in df.to_dicts():
        product  = r.get('PRODUCT', 0) or 0
        curcode  = str(r.get('CURCODE', '') or '')
        undrawn  = float(r.get('UNDRAWN', 0) or 0)
        fisspurp = str(r.get('FISSPURP', '') or '')
        sectorcd = str(r.get('SECTORCD', '') or '')
        custcd   = str(r.get('CUSTCD',   '') or '')
        industrial_sector_cd = str(r.get('INDUSTRIAL_SECTOR_CD', '') or '')

        if 800 <= product < 900:
            forate  = float(forate_map.get(curcode, 1.0))
            undrawn *= forate
            # DROP FORATE — not stored

        if fisspurp > '1000':
            fisspurp = '0990'
        if sectorcd.strip() in ('', '    '):
            sectorcd = '9999'

        r['SECTORCD_ORI'] = sectorcd   # 18-2652
        if (industrial_sector_cd and len(industrial_sector_cd) == 5
                and industrial_sector_cd > '0'):
            # placeholder: replace with format_indsect(industrial_sector_cd)
            sectorcd = industrial_sector_cd   # placeholder

        if custcd in ('77', '78', '95', '96'):
            sectorcd = '9700'

        r.update({
            'UNDRAWN':  undrawn,
            'FISSPURP': fisspurp,
            'SECTORCD': sectorcd,
        })
        result_rows.append(r)

    return pl.DataFrame(result_rows) if result_rows else pl.DataFrame()


# ============================================================================
# MAIN
# ============================================================================
def main(reptmon: str = None, nowk: str = None,
         reptdate: date = None, sdate: str = None) -> None:
    """
    Main entry point for LALWPBBD.
    Parameters passed from the calling job (EIBDWKLX / PBBWKLY).
    """
    log.info("LALWPBBD started.")

    # ----------------------------------------------------------------
    # Read input datasets produced by LALWPBBC
    # ----------------------------------------------------------------
    note_path  = OUTPUT_DIR / f"NOTE{reptmon}{nowk}.parquet"
    unote_path = OUTPUT_DIR / f"UNOTE{reptmon}{nowk}.parquet"

    note_df  = pl.read_parquet(note_path)
    unote_df = pl.read_parquet(unote_path)
    log.info("NOTE rows: %d  UNOTE rows: %d", len(note_df), len(unote_df))

    sme08_df = pl.read_parquet(SME08_PATH)

    # ----------------------------------------------------------------
    # Load FCY rate map ($FORATE. format)
    # PROC FORMAT LIB=FORATE CNTLOUT=FOFMT + PROC FORMAT CNTLIN=FOFMT
    # ----------------------------------------------------------------
    try:
        fofmt    = pl.read_parquet(FOFMT_PATH)
        # Expect columns: FMTNAME='FORATE', START=currency_code, LABEL=rate
        forate_map = {
            str(r['START']): float(r['LABEL'])
            for r in fofmt.filter(pl.col("FMTNAME").str.to_uppercase() == "FORATE")
                          .to_dicts()
        }
    except Exception:
        log.warning("FOFMT not available — FCY rates defaulting to 1.0")
        forate_map = {}

    # ----------------------------------------------------------------
    # DEPOSIT.CURRENT + SME08 merge
    # ----------------------------------------------------------------
    current = load_current(sme08_df)
    log.info("CURRENT loaded: %d rows", len(current))

    # ----------------------------------------------------------------
    # Build BNM.OVDFT (utilised OD)
    # ----------------------------------------------------------------
    ovdft_df = build_ovdft(current, reptmon, nowk)
    log.info("OVDFT: %d rows", len(ovdft_df))

    # ----------------------------------------------------------------
    # Build BNM.LOAN (NOTE + OVDFT combined, trimmed to LOAN_KEEP)
    # ----------------------------------------------------------------
    loan_df = build_loan(note_df, ovdft_df)
    log.info("LOAN (pre-normalise): %d rows", len(loan_df))

    # ----------------------------------------------------------------
    # Build BNM.UOD (unutilised OD)
    # ----------------------------------------------------------------
    uod_df = build_uod(current)
    log.info("UOD: %d rows", len(uod_df))

    # ----------------------------------------------------------------
    # Build BNM.ULOAN (UNOTE + UOD combined)
    # ----------------------------------------------------------------
    uloan_df = build_uloan(unote_df, uod_df)
    log.info("ULOAN (pre-normalise): %d rows", len(uloan_df))

    # ----------------------------------------------------------------
    # PROC SORT LOAN + LNNOTE merge for SECTOR/CRISPURP enrichment
    # ----------------------------------------------------------------
    lnnote_src = (
        pl.read_parquet(LOAN_LNNOTE_PATH)
          .select(["ACCTNO", "NOTENO", "SECTOR", "CRISPURP"])
          .sort(["ACCTNO", "NOTENO"])
    )

    # DATA OVDFT (KEEP=ACCTNO SECTOR CCRICODE) from DEPOSIT.CURRENT
    ovdft_sect = (
        current.filter(
            ~pl.col("OPENIND").is_in(["B", "C", "P"]) &
            (pl.col("CURBAL") < 0)
        )
        .rename({"SECTOR": "SECT"})
        .with_columns(
            pl.col("SECT").cast(pl.Utf8).str.zfill(4).alias("SECTOR")
        )
        .select(["ACCTNO", "SECTOR", "CCRICODE"])
        .sort("ACCTNO")
    )

    # Merge LOAN with OVDFT sector
    loan_sorted = loan_df.sort("ACCTNO")
    loan_df = loan_sorted.join(ovdft_sect, on="ACCTNO", how="left", suffix="_OVD")

    # Merge LOAN with LNNOTE
    loan_sorted2 = loan_df.sort(["ACCTNO", "NOTENO"])
    loan_df = loan_sorted2.join(lnnote_src, on=["ACCTNO", "NOTENO"], how="left", suffix="_LN")

    # ----------------------------------------------------------------
    # Sector normalisation pass (LOAN)
    # ----------------------------------------------------------------
    loan_df = normalise_loan_sector(loan_df)
    log.info("LOAN sector normalised.")

    # ----------------------------------------------------------------
    # Split LOAN into LOANS (pre-JAN07) + INDOD + INDLOAN (post-JAN07)
    # ----------------------------------------------------------------
    loans_rows  = []
    indod_rows  = []
    indloan_rows = []
    for r in loan_df.to_dicts():
        acctype  = str(r.get('ACCTYPE', '') or '')
        apprdate = r.get('APPRDATE')
        issdte   = r.get('ISSDTE')
        if acctype == 'OD' and apprdate and apprdate > JAN07_CUTOFF:
            indod_rows.append(r)
        elif acctype == 'LN' and issdte and issdte > JAN07_CUTOFF:
            indloan_rows.append(r)
        else:
            loans_rows.append(r)

    loans_df  = pl.DataFrame(loans_rows)   if loans_rows   else pl.DataFrame()
    indod_df  = pl.DataFrame(indod_rows)   if indod_rows   else pl.DataFrame()
    indloan_df = pl.DataFrame(indloan_rows) if indloan_rows else pl.DataFrame()

    # LOANS: SME custcds with SECTORCD='9700' → '9999'
    if not loans_df.is_empty():
        sme_set = {
            '41', '42', '43', '44', '46', '47', '48', '49', '51',
            '52', '53', '54', '60', '61', '62', '63', '64', '65', '59', '75', '57'
        }
        loans_df = loans_df.with_columns(
            pl.when(
                pl.col("CUSTCD").is_in(list(sme_set)) & (pl.col("SECTORCD") == '9700')
            ).then(pl.lit('9999')).otherwise(pl.col("SECTORCD")).alias("SECTORCD")
        )

    # CIS matching for INDOD
    if not indod_df.is_empty():
        cisca_df = (
            pl.read_parquet(CISCADP_PATH)
              .filter(
                  (pl.col("ACCTNO").is_between(3_000_000_000, 3_999_999_999)) &
                  (pl.col("SECCUST") != "902")
              )
              .select(["ACCTNO", "SECCUST", "NEWICIND", "BUSSIND"])
              .unique(subset=["ACCTNO"], keep="first")
              .sort("ACCTNO")
        )
        indod_df = indod_df.sort("ACCTNO")
        indod_df = apply_cis_ind(indod_df, cisca_df, is_od=True)

    # CIS matching for INDLOAN
    if not indloan_df.is_empty():
        cisln_df = (
            pl.read_parquet(CISCALN_PATH)
              .filter(pl.col("SECCUST") != "902")
              .select(["ACCTNO", "SECCUST", "NEWICIND", "BUSSIND"])
              .unique(subset=["ACCTNO"], keep="first")
              .sort("ACCTNO")
        )
        indloan_df = indloan_df.sort(["ACCTNO", "NOTENO"])
        indloan_df = apply_cis_ind(indloan_df, cisln_df, is_od=False)

    # Combine all loan segments
    loan_combined = pl.concat(
        [df for df in [loans_df, indod_df, indloan_df] if not df.is_empty()],
        how="diagonal"
    )

    # ----------------------------------------------------------------
    # Final BNM.LOAN processing (FCY, APPRLIM2, FISSPURP, sector)
    # ----------------------------------------------------------------
    loan_final = finalise_loan(loan_combined, forate_map)
    loan_final = loan_final.sort(["ACCTNO", "NOTENO"])

    # ----------------------------------------------------------------
    # Split LOAN → LOAN / LNWOD / LNWOF
    # ----------------------------------------------------------------
    loan_out, lnwod_out, lnwof_out = split_loan(loan_final)
    log.info("LOAN: %d  LNWOD: %d  LNWOF: %d",
             len(loan_out), len(lnwod_out), len(lnwof_out))

    # ----------------------------------------------------------------
    # ULOAN sector normalisation pass
    # ----------------------------------------------------------------
    uloan_sorted = uloan_df.sort("ACCTNO")
    uloan_df = normalise_loan_sector(uloan_sorted)

    # Merge ULOAN with LOAN FISSPURP (NODUPKEYS by ACCTNO)
    loan_fisspurp = (
        loan_out.select(["ACCTNO", "FISSPURP"])
                .unique(subset=["ACCTNO"], keep="first")
                .sort("ACCTNO")
    )
    uloan_df = uloan_df.join(
        loan_fisspurp.rename({"FISSPURP": "FISSPURP_LN"}),
        on="ACCTNO", how="left"
    )
    # Use FISSPURP from LOAN where available and > '1000'
    uloan_df = uloan_df.with_columns(
        pl.when(
            pl.col("FISSPURP_LN").is_not_null() & (pl.col("FISSPURP_LN") > '1000')
        ).then(pl.lit('0990'))
         .otherwise(pl.col("FISSPURP"))
         .alias("FISSPURP")
    ).drop("FISSPURP_LN")

    uloan_final = finalise_uloan(uloan_df, forate_map)
    uloan_final = uloan_final.drop(
        [c for c in ('SECTOR', 'SECTORZZ', 'SECTORZ', 'SECVALID') if c in uloan_final.columns]
    ).sort("ACCTNO")
    log.info("ULOAN finalised: %d rows", len(uloan_final))

    # ----------------------------------------------------------------
    # Write output parquet files
    # ----------------------------------------------------------------
    outputs = {
        f"LOAN{reptmon}{nowk}.parquet":  loan_out,
        f"LNWOD{reptmon}{nowk}.parquet": lnwod_out,
        f"LNWOF{reptmon}{nowk}.parquet": lnwof_out,
        f"ULOAN{reptmon}{nowk}.parquet": uloan_final,
    }
    for fname, df in outputs.items():
        path = OUTPUT_DIR / fname
        if not df.is_empty():
            df.write_parquet(path)
            log.info("Written: %s (%d rows)", path, len(df))
        else:
            log.info("Skipped (empty): %s", path)

    log.info("LALWPBBD completed.")


if __name__ == "__main__":
    rd_df    = pl.read_parquet(REPTDATE_PATH)
    row      = rd_df.row(0, named=True)
    reptdate = row["REPTDATE"]
    reptmon  = f"{reptdate.month:02d}"
    nowk     = f"{reptdate.day:02d}"
    sdate    = reptdate.strftime("%d/%m/%Y")

    main(reptmon=reptmon, nowk=nowk, reptdate=reptdate, sdate=sdate)
