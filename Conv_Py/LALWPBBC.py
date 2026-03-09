#!/usr/bin/env python3
"""
Program  : LALWPBBC.py
Invoke by: EIBDWKLX (via PBBWKLX job)
Modified : 26-10-2008
Purpose  : Manipulate extracted loan (utilised account) data.
           Processes loan notes (LNNOTE), computes balances, unearned
           interest, remaining/original maturity, release amounts,
           approved limits, undrawn portions, and outputs:
             BNM.NOTE<REPTMON><NOWK>   - processed note dataset
             BNM.UNOTE<REPTMON><NOWK>  - undrawn commitment records

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
# Note: SAS source includes %INC PGM(PBBDPFMT,PBBLNFMT), but PBBDPFMT provides
#       deposit product formats only. LALWPBBC is a loan program — no PBBDPFMT
#       functions are used here. Only PBBLNFMT loan format functions are imported.
# ============================================================================
from PBBLNFMT import (
    format_locustcd,
    format_lndenom,
    format_lnprod,
    format_statecd,
    format_lnormt,
    format_lnrmmt,
    format_collcd,
    format_riskcd,
    FCY_PRODUCTS,
)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR  = Path(".")
DATA_DIR  = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input parquet paths
LNNOTE_PATH    = DATA_DIR / "loan"   / "lnnote.parquet"      # LOAN.LNNOTE
LNCOMM_PATH    = DATA_DIR / "loan"   / "lncomm.parquet"      # LOAN.LNCOMM
SME08_PATH     = DATA_DIR / "bnm"    / "sme08.parquet"       # BNM.SME08
REPTDATE_PATH  = DATA_DIR / "bnm"    / "reptdate.parquet"    # BNM.REPTDATE (set by EIBDWKLX)

# Output parquet paths  (named dynamically with REPTMON + NOWK at runtime)
# Final paths assigned after loading REPTDATE in main()
NOTE_OUT_PATH  = None   # BNM.NOTE<REPTMON><NOWK>
UNOTE_OUT_PATH = None   # BNM.UNOTE<REPTMON><NOWK>

# ============================================================================
# CONSTANTS
# ============================================================================
# %LET ABBA=(110,111,113,115,116,120,127,140,141,142,143)
ABBA_PRODUCTS = {110, 111, 113, 115, 116, 120, 127, 140, 141, 142, 143}

# HP products used in RLEASAMT assignment
HP_PRODUCTS = {128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 752, 760}

# HP products used in FISSPURP override
HP_FISSPURP = {128, 130, 131, 132, 380, 381, 700, 705, 720, 725}

# FCY product set from PBBLNFMT
FCY_SET = set(FCY_PRODUCTS)

# CUSTCD remapping sets for SECTOLD IN ('0410','0430')
CUSTCD_TO_77 = {'61', '41', '42', '43'}
CUSTCD_TO_78 = {
    '62', '44', '46', '47', '63', '48', '49', '51', '57', '75', '59',
    '52', '53', '54', '45', '20', '30', '32', '33', '34', '35', '36',
    '37', '38', '39', '40'
}

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
# MACRO: %ORIMTH — Original maturity in months
# EXYR/EXMTH from EXPRDATE; ISYR/ISMTH from ISSDTE
# ============================================================================
def calc_orimth(exprdate: Optional[date], issdte: Optional[date]) -> int:
    """
    Compute original maturity in months between issue date and expiry date.
    Returns 0 if exprdate <= issdte or either is None.
    """
    if exprdate is None or issdte is None:
        return 0
    if exprdate <= issdte:
        return 0
    oryy = exprdate.year  - issdte.year
    ormm = exprdate.month - issdte.month
    return oryy * 12 + ormm


# ============================================================================
# MACRO: %REMMTH — Remaining maturity in months
# MDYR/MDMTH from EXPRDATE; RPYY/RPMM from REPTDATE
# ============================================================================
def calc_remmth(exprdate: Optional[date], reptdate: Optional[date]) -> int:
    """
    Compute remaining maturity in months from report date to expiry date.
    Returns 0 if exprdate is None/missing or <= reptdate.
    """
    if exprdate is None or reptdate is None:
        return 0
    if exprdate <= reptdate:
        return 0
    remy = exprdate.year  - reptdate.year
    remm = exprdate.month - reptdate.month
    return remy * 12 + remm


# ============================================================================
# COUNTMON — count months from reptdate until folmonth >= exprdate
# Mirrors the COUNTMON LINK routine in SAS.
# ============================================================================
def count_months(reptdate: date, exprdate: date) -> float:
    """
    Count integer months by rolling reptdate forward month-by-month
    until folmonth >= exprdate.  Mirrors the SAS COUNTMON subroutine.
    Returns the number of months as a float (rounded by caller).
    """
    from calendar import monthrange

    if exprdate <= reptdate:
        return 0.0

    folmonth = reptdate
    nummonth = 0
    nextday  = reptdate.day

    while exprdate > folmonth:
        nummonth += 1
        nextmon  = folmonth.month + 1
        nextyear = folmonth.year
        if nextmon > 12:
            nextmon  -= 12
            nextyear += 1
        # Handle end-of-month edge cases (mirrors SAS MDY logic)
        if nextday in (29, 30, 31) and nextmon == 2:
            # Last day of February
            last_feb = monthrange(nextyear, 2)[1]
            folmonth = date(nextyear, 2, last_feb)
        elif nextday == 31 and nextmon in (4, 6, 9, 11):
            folmonth = date(nextyear, nextmon, 30)
        elif nextday == 30 and nextmon in (1, 3, 5, 7, 8, 10, 12):
            folmonth = date(nextyear, nextmon, 31)
        else:
            try:
                folmonth = date(nextyear, nextmon, nextday)
            except ValueError:
                last_day = monthrange(nextyear, nextmon)[1]
                folmonth = date(nextyear, nextmon, last_day)

    return float(nummonth)


# ============================================================================
# STATECD mapping helper
# Mirrors the three-branch STATECD logic in SAS:
#   1. POSTCD macro  → $STATEPOST. format  (not implemented here; falls to statecd)
#   2. FCY + COUNTRYCD → 'Z'
#   3. default $STATECD. format; FCY with 00/99 codes → 'Z'
# Note: $STATEPOST. and $SECTCD. are not provided — placeholders used.
# ============================================================================
POSTCD_SET = set()      # Populate from &POSTCD macro variable if available
COUNTRYCD_SET = set()   # Populate from &COUNTRYCD macro variable if available


def derive_statecd(state: str, product: int, branch: int) -> tuple[str, bool]:
    """
    Derive STATECD and INVALID_LOC flag.
    Returns (statecd, invalid_loc).
    """
    invalid_loc = False

    if state in POSTCD_SET:
        # $STATEPOST. format — placeholder; treat same as statecd
        statecd = format_statecd(state)
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
# FISSPURP derivation
# Mirrors the repeated FISSPURP / SECTOLD logic blocks in SAS.
# $CRISCD. and $FISSPUR. formats are not provided — placeholders used.
# ============================================================================
def derive_fisspurp(crispurp: str, sectold: str, colldesc: str,
                    census: float, product: int, liabcode: str,
                    custcd: str) -> tuple[str, str]:
    """
    Derive FISSPURP and potentially revised CUSTCD.
    Returns (fisspurp, custcd).

    $CRISCD. and $FISSPUR. format mappings are referenced but not provided;
    their calls are marked as placeholders and should be replaced when
    the format tables become available.
    """
    # FISSPURP = PUT(CRISPURP, $CRISCD.) — placeholder
    # from PBBDPFMT/$CRISCD format (not provided); using raw value
    fisspurp = crispurp   # placeholder: replace with format_criscd(crispurp)

    newsec  = colldesc[37:39] if len(colldesc) >= 39 else ''
    # CENSUS4 = SUBSTR(PUT(CENSUS,7.2),4,1) → 4th char of 7.2-formatted census
    census_str = f"{census:7.2f}"
    census4 = census_str[3] if len(census_str) >= 4 else ''

    if fisspurp == '3100':
        if newsec == 'N':
            fisspurp = '0211'
        elif census4 == '1':
            fisspurp = '0211'
        else:
            fisspurp = '0212'

    if sectold.strip() not in ('', '    '):
        # FISSPURP = PUT(SECTOLD, $FISSPUR.) — placeholder
        fisspurp = sectold   # placeholder: replace with format_fisspur(sectold)
        if sectold in ('0410', '0430') and custcd not in ('77', '78', '95', '96'):
            if custcd in CUSTCD_TO_77:
                custcd = '77'
            elif custcd in CUSTCD_TO_78:
                custcd = '78'
        # Commented-out block retained as comment:
        # ELSE IF SECTOLD IN ('0211','0212','0220','0230') AND
        #         CUSTCD NOT IN ('77','78','95','96') THEN
        #         SECTORCD='9999';

    # HP product override for FISSPURP when SECTOLD in range 1111–9999
    if '1111' <= sectold.strip() <= '9999':
        if product in HP_FISSPURP:
            if liabcode == '63':
                fisspurp = '0390'
            elif newsec == 'N':
                fisspurp = '0211'
            elif census4 == '1':
                fisspurp = '0211'
            else:
                fisspurp = '0212'

    return fisspurp, custcd


# ============================================================================
# REMAINMT computation (used in multiple output blocks)
# ============================================================================
def compute_remainmt(exprdate: Optional[date], reptdate: Optional[date],
                     thisdate: Optional[date]) -> tuple[str, str, float]:
    """
    Compute REMAINMT, REMAINMX, REMAINMH.
    Returns (remainmt, remainmx, remainmh).
    """
    if reptdate is None or exprdate is None or exprdate < reptdate:
        return '51', '51', 0.0

    nummonth  = count_months(reptdate, exprdate)
    remainmh  = round(nummonth)
    remainmx  = format_lnrmmt(nummonth)

    remmth = 0
    if exprdate and exprdate > reptdate:
        remmth = calc_remmth(exprdate, reptdate)
    remainmt = format_lnrmmt(float(remmth))

    return remainmt, remainmx, float(remainmh)


# ============================================================================
# Unearned interest computation (REBIND == 2 branch)
# ============================================================================
def compute_unearned_rebind2(row: dict, reptdate: date, thisdate: date) -> dict:
    """
    Handle the REBIND=2 SELECT-WHEN logic and return updated fields.
    Covers three cases:
      WHEN INTAMT == 0.01 : zeroise/written-off A/Cs
      WHEN round mismatch : dued A/Cs with sum-of-digits unearned
      OTHERWISE           : rebate/intearn4 zeroed
    """
    intamt   = row.get('INTAMT',   0) or 0
    intearn  = row.get('INTEARN',  0) or 0
    intearn2 = row.get('INTEARN2', 0) or 0
    intearn3 = row.get('INTEARN3', 0) or 0
    intearn4 = row.get('INTEARN4', 0) or 0
    rebate   = row.get('REBATE',   0) or 0
    curbal   = row.get('CURBAL',   0) or 0
    feeamt   = row.get('FEEAMT',   0) or 0
    accrual  = row.get('ACCRUAL',  0) or 0
    earnterm = row.get('EARNTERM', 0) or 0
    noteterm = row.get('NOTETERM', 0) or 0
    issdte   = row.get('ISSDTE')

    rmm = reptdate.month
    ryy = reptdate.year
    imm = issdte.month if issdte else rmm
    iyy = issdte.year  if issdte else ryy

    if intamt == 0.01:
        # Zeroise / written-off accounts
        rebate   = -rebate
        intearn4 = -intearn4
        unearned1 = rebate
        unearned2 = intearn4
        unearned  = unearned1 + unearned2
        balance   = curbal + feeamt + rebate + intearn4
        balmni    = balance
        intamt = 0; intearn = 0; accrual = 0
        intearn2 = 0; intearn3 = 0
    elif (round(rebate + intearn, 2) != intamt or
          round(intearn4 + intearn3, 2) != intearn2):
        # Dued A/Cs — sum-of-digits unearned calculation
        if thisdate.day == 1:
            remain = earnterm - (((ryy * 12) + rmm + 1) - ((iyy * 12) + imm))
        else:
            remain = earnterm - (((ryy * 12) + rmm) - ((iyy * 12) + imm))

        denom = earnterm * (earnterm + 1) if earnterm else 1
        unearn1 = (remain * (remain + 1) * intamt)  / denom if denom else 0
        unearn2 = (remain * (remain + 1) * intearn2) / denom if denom else 0
        unearned1 = unearn1
        unearned2 = unearn2
        unearned  = unearned1 + unearned2
        balance   = curbal + (-unearn1) + (-unearn2) + feeamt

        # REQUESTED BY RAYMOND (MIS)
        remai1  = noteterm - (((ryy * 12) + rmm + 1) - ((iyy * 12) + imm))
        ndenom  = noteterm * (noteterm + 1) if noteterm else 1
        unear1  = (remai1 * (remai1 + 1) * intamt)   / ndenom if ndenom else 0
        unear2  = (remai1 * (remai1 + 1) * intearn2) / ndenom if ndenom else 0
        # REVISE BALMNI VIA 2020-1469
        balmni  = curbal + (-rebate) + (-intearn4) + feeamt

        intamt = 0; intearn = 0; accrual = 0; rebate = 0
        intearn2 = 0; intearn3 = 0; intearn4 = 0
    else:
        rebate   = 0
        intearn4 = 0
        unearned1 = row.get('UNEARNED1', 0) or 0
        unearned2 = row.get('UNEARNED2', 0) or 0
        unearned  = unearned1 + unearned2
        balance   = row.get('BALANCE', 0) or 0
        balmni    = row.get('BALMNI',  0) or 0

    return {
        **row,
        'INTAMT': intamt, 'INTEARN': intearn, 'ACCRUAL': accrual,
        'REBATE': rebate, 'INTEARN2': intearn2, 'INTEARN3': intearn3,
        'INTEARN4': intearn4,
        'UNEARNED1': unearned1, 'UNEARNED2': unearned2, 'UNEARNED': unearned,
        'BALANCE': balance, 'BALMNI': balmni,
    }


# ============================================================================
# Parse packed SAS date integers (MMDDYYYY packed as integer Z11)
# ============================================================================
def parse_sas_z11_date(val) -> Optional[date]:
    """
    Parse an integer stored as Z11 (MMDDYYYY in first 8 chars).
    E.g. 12312023000 → SUBSTR(PUT(val,Z11.),1,8) = '12312023' → MMDDYY8.
    Returns None for missing/zero values.
    """
    if val is None or val == 0:
        return None
    try:
        s = f"{int(val):011d}"[:8]   # MMDDYYYY
        return datetime.strptime(s, "%m%d%Y").date()
    except (ValueError, TypeError):
        return None


# ============================================================================
# STEP 1: Load and merge LNNOTE + SME08
# ============================================================================
def load_lnnotex(reptdate: date, tdate: int, rdate: str, sdate: str) -> pl.DataFrame:
    """
    Merge LOAN.LNNOTE with BNM.SME08 by ACCTNO+NOTENO.
    Compute UNEARNED fields for NTINT='A' records.
    Mirrors: DATA LNNOTEX.
    """
    lnnote = pl.read_parquet(LNNOTE_PATH).sort(["ACCTNO", "NOTENO"])
    sme08  = pl.read_parquet(SME08_PATH).select(["ACCTNO", "NOTENO", "SECTOLD"])

    df = lnnote.join(sme08, on=["ACCTNO", "NOTENO"], how="left")

    # FEEAMT default 0
    df = df.with_columns(
        pl.col("FEEAMT").fill_null(0).alias("FEEAMT")
    )

    # UNEARNED for NTINT='A'
    def compute_unearned(row_ntint, intearn, intamt, intearn2, intearn3):
        if row_ntint == 'A':
            u1 = abs((intearn or 0) - (intamt or 0))
            u2 = abs(-(intearn2 or 0) + (intearn3 or 0))
            return u1, u2, u1 + u2
        return 0.0, 0.0, 0.0

    records = df.to_dicts()
    for r in records:
        u1, u2, u = compute_unearned(
            r.get('NTINT'), r.get('INTEARN', 0), r.get('INTAMT', 0),
            r.get('INTEARN2', 0), r.get('INTEARN3', 0)
        )
        r['UNEARNED1'] = u1
        r['UNEARNED2'] = u2
        r['UNEARNED']  = u

    return pl.DataFrame(records)


# ============================================================================
# STEP 2: Build BNM.NOTE<REPTMON><NOWK>
# ============================================================================
def build_note(lnnotex: pl.DataFrame, reptdate: date, tdate: int,
               rdate: str, sdate: str, reptmon: str, nowk: str) -> pl.DataFrame:
    """
    Main note processing step.
    Mirrors: DATA BNM.NOTE&REPTMON&NOWK; SET LNNOTEX (RENAME=...);
    Applies format mappings, date computations, unearned logic,
    and outputs records for PAIDIND NE 'P' and fee/early-settlement cases.
    """
    thisdt   = tdate
    thisdate = date.fromordinal(date(1960, 1, 1).toordinal() + thisdt + 1)  # SAS date offset

    # REPTDATE from macro
    reptdate_d = reptdate

    output_rows = []

    for r in lnnotex.rename({"LOANTYPE": "PRODUCT", "PENDBRH": "BRANCH"}).to_dicts():
        if r.get('REVERSED') == 'Y':
            continue

        # Date parsing
        exprdate = parse_sas_z11_date(r.get('NOTEMAT'))
        apprdate = parse_sas_z11_date(r.get('GRANTDT'))
        closedte = parse_sas_z11_date(r.get('LASTTRAN'))
        issdte   = parse_sas_z11_date(r.get('ISSUEDT'))

        product  = r.get('PRODUCT', 0) or 0
        branch   = r.get('BRANCH', 0) or 0
        state    = str(r.get('STATE', '') or '')
        custcode = r.get('CUSTCODE', 0) or 0
        sector   = str(r.get('SECTOR', '') or '')
        liabcode = str(r.get('LIABCODE', '') or '')
        colldesc = str(r.get('COLLDESC', '') or '').ljust(70)
        census   = float(r.get('CENSUS', 0) or 0)
        crispurp = str(r.get('CRISPURP', '') or '')
        sectold  = str(r.get('SECTOLD', '') or '').ljust(4)
        ntint    = str(r.get('NTINT', '') or '')
        rebind   = r.get('REBIND', 0) or 0
        paidind  = str(r.get('PAIDIND', '') or '')
        noteno   = r.get('NOTENO')
        taxno    = str(r.get('TAXNO', '') or '')
        guarend  = str(r.get('GUAREND', '') or '')
        riskrate = str(r.get('RISKRATE', '') or '')
        noteterm = r.get('NOTETERM', 0) or 0
        curbal   = float(r.get('CURBAL', 0) or 0)
        feeamt   = float(r.get('FEEAMT', 0) or 0)
        orgtype  = str(r.get('ORGTYPE', '') or '')

        # SECTORMA, SECTORCD — $SECTCD. format placeholder
        sectorma = sector
        sectorcd = sector   # placeholder: replace with format_sectcd(sector)

        # CUSTCD
        custcd = format_locustcd(custcode)

        # STATECD
        statecd, invalid_loc = derive_statecd(state, product, branch)

        # PRODCD
        prodcd = format_lnprod(product)
        if product == 606:
            if census in (500.00, 520.00):
                prodcd = 'N'

        # AMTIND (LNDENOM)
        amtind = format_lndenom(product)
        if prodcd == '34170':
            amtind = 'F'   # FLOOR STOCKING

        # COLLCD
        collcd = format_collcd(liabcode)

        # ORIGMX (noteterm via LNORMT)
        origmx = format_lnormt(float(noteterm))

        # ORIMTH
        if exprdate and issdte and exprdate <= issdte:
            orimth = 0
        else:
            orimth = calc_orimth(exprdate, issdte)
        origmt = format_lnormt(float(orimth))

        # RISKRTE, RISKCD
        try:
            riskrte = int(riskrate)
        except (ValueError, TypeError):
            riskrte = 0
        riskcd = format_riskcd(riskrate)

        # CGCREF
        cgcref = colldesc[55:70]

        # BALMNI
        balmni = curbal

        # CHK flag (17-4531)
        chk = 0
        if paidind in ('P', 'C') and prodcd in ('34190', '34690', '34170'):
            chk = 1

        # FISSPURP + CUSTCD revision
        fisspurp, custcd = derive_fisspurp(
            crispurp, sectold, colldesc, census, product, liabcode, custcd
        )

        # BRANCH fallback
        ntbrch  = r.get('NTBRCH', 0) or 0
        accbrch = r.get('ACCBRCH', 0) or 0
        if branch == 0:
            branch = ntbrch
        if ntbrch == 0:
            branch = accbrch

        # CUSTIDNO
        custidno = ''
        try:
            if taxno and int(taxno) != 0:
                custidno = taxno
            if taxno and int(taxno) == 0:
                custidno = taxno
        except (ValueError, TypeError):
            custidno = taxno
        if not custidno.strip():
            custidno = guarend

        base_row = {
            **r,
            'PRODUCT': product, 'BRANCH': branch,
            'REPTDATE': reptdate_d, 'EXPRDATE': exprdate,
            'APPRDATE': apprdate, 'CLOSEDTE': closedte, 'ISSDTE': issdte,
            'SECTORMA': sectorma, 'SECTORCD': sectorcd,
            'CUSTCD': custcd, 'STATECD': statecd, 'INVALID_LOC': 'Y' if invalid_loc else '',
            'PRODCD': prodcd, 'AMTIND': amtind, 'COLLCD': collcd,
            'ORIGMX': origmx, 'ORIGMT': origmt, 'ORIMTH': orimth,
            'RISKRTE': riskrte, 'RISKCD': riskcd,
            'CGCREF': cgcref, 'BALMNI': balmni, 'CHK': chk,
            'FISSPURP': fisspurp, 'CUSTIDNO': custidno,
            'FEEAMT': feeamt, 'CURBAL': curbal,
        }

        # PAIDIND NE 'P' — primary output block
        if noteno is not None and paidind != 'P':
            row = dict(base_row)

            if rebind == 2:
                row = compute_unearned_rebind2(row, reptdate_d, thisdate)

            if custcd == 'AA':
                if orgtype in ('C', 'P', 'I'):
                    row['CUSTCD'] = '66'
                else:
                    row['CUSTCD'] = '77'

            # REMAINMT
            remainmt, remainmx, remainmh = compute_remainmt(exprdate, reptdate_d, thisdate)
            row.update({'REMAINMT': remainmt, 'REMAINMX': remainmx, 'REMAINMH': remainmh})

            # BALMNI negative fix (REVISE BALMNI VIA 2022-3698)
            if row.get('BALMNI', 0) < 0 and ntint == 'S':
                row['BALMNI'] = (row.get('CURBAL', 0) or 0) + \
                                (row.get('FEEAMT', 0) or 0) + \
                                (row.get('ACCRUAL', 0) or 0)

            output_rows.append(row)

        # PAIDIND='P' + FEEAMT > 0 — outstanding fee for paid loans
        if paidind == 'P' and feeamt > 0:
            row = dict(base_row)
            row['BALANCE'] = feeamt
            row['INTAMT']  = 0; row['INTEARN'] = 0; row['REBATE'] = 0
            row['BALMNI']  = feeamt
            remainmt, remainmx, remainmh = compute_remainmt(exprdate, reptdate_d, thisdate)
            row.update({'REMAINMT': remainmt, 'REMAINMX': remainmx, 'REMAINMH': remainmh})
            output_rows.append(row)
        else:
            # Early settlement within same month (PAIDIND='P', same month, NTINT='A')
            if paidind == 'P' and closedte:
                curmonth  = reptdate_d.month; curyear  = reptdate_d.year
                lnmonth   = closedte.month;   lnyear   = closedte.year
                intamt    = float(base_row.get('INTAMT',  0) or 0)
                intearn   = float(base_row.get('INTEARN', 0) or 0)
                rebate    = float(base_row.get('REBATE',  0) or 0)
                if (lnmonth == curmonth and lnyear == curyear and ntint == 'A'):
                    balance = (intamt - intearn) - rebate
                    newbal  = -abs(balance) if balance < 0 else -balance
                    row = dict(base_row)
                    row['BALANCE'] = newbal
                    row['BALMNI']  = newbal
                    row['FEEAMT']  = 0
                    remainmt, remainmx, remainmh = compute_remainmt(exprdate, reptdate_d, thisdate)
                    row.update({'REMAINMT': remainmt, 'REMAINMX': remainmx, 'REMAINMH': remainmh})
                    output_rows.append(row)

    if not output_rows:
        return pl.DataFrame()
    return pl.DataFrame(output_rows)


# ============================================================================
# STEP 3: ABBA processing — merge with LNCOMM, update BALANCE / RLEASAMT
# ============================================================================
def process_abba(note_df: pl.DataFrame, lncomm: pl.DataFrame,
                 sdate: str, rdate: str) -> pl.DataFrame:
    """
    Filters ABBA product records (ACCTNO > 8000000000 AND PRODUCT IN ABBA_PRODUCTS),
    merges with LNCOMM, recalculates BALANCE, RLEASAMT, BALMNI.
    Returns only first ACCTNO+NOTENO per group.
    Mirrors: DATA ABBA; MERGE ABBA LNCOMM; DATA ABBA (FIRST.ACCTNO & FIRST.NOTENO).
    """
    sdate_d = datetime.strptime(sdate, "%d/%m/%Y").date()
    rdate_d = datetime.strptime(rdate, "%d/%m/%Y").date()

    abba_df = note_df.filter(
        (pl.col("ACCTNO") > 8_000_000_000) &
        pl.col("PRODUCT").is_in(list(ABBA_PRODUCTS))
    ).sort(["ACCTNO", "COMMNO"])

    lncomm_sorted = lncomm.sort(["ACCTNO", "COMMNO"])

    merged = abba_df.join(lncomm_sorted, on=["ACCTNO", "COMMNO"], how="left", suffix="_COMM")

    results = []
    for r in merged.to_dicts():
        product  = r.get('PRODUCT', 0) or 0
        if product not in ABBA_PRODUCTS:
            continue

        paidind  = str(r.get('PAIDIND', '') or '')
        intamt   = float(r.get('INTAMT',  0) or 0)
        intearn  = float(r.get('INTEARN', 0) or 0)
        curbal   = float(r.get('CURBAL',  0) or 0)
        feeamt   = float(r.get('FEEAMT',  0) or 0)
        accrual  = float(r.get('ACCRUAL', 0) or 0)
        ntint    = str(r.get('NTINT', '') or '')
        issdte   = r.get('ISSDTE')
        noteno   = r.get('NOTENO', 0) or 0
        netproc  = float(r.get('NETPROC', 0) or 0)
        cmhstadj = float(r.get('CMHSTADJ', 0) or 0)
        cavaiamt = float(r.get('CAVAIAMT', 0) or 0)

        rebmtd   = intamt - intearn
        has_comm = r.get('COMMNO') is not None and (r.get('COMMNO') or 0) > 0

        rleasamt = 0.0
        balance  = r.get('BALANCE', 0) or 0

        if paidind != 'P':
            if has_comm:
                balance  = (curbal - rebmtd) + feeamt - cavaiamt
                rleasamt = cmhstadj
            else:
                balance = (curbal - rebmtd) + feeamt
                if product != 101 and ntint == 'A':
                    if issdte and sdate_d <= issdte <= rdate_d:
                        if not (98000 <= noteno <= 99999):
                            rleasamt = netproc

            if product == 140:
                balance = (curbal - rebmtd) + feeamt
            elif product in (117, 118):
                balance = curbal + accrual + feeamt
            elif product == 139:
                balance = curbal + accrual + feeamt

        balmni = balance

        # SUBSTR(PUT(NOTENO,Z5.),2,1) = '2' → RLEASAMT = 0
        noteno_str = f"{int(noteno):05d}"
        if len(noteno_str) >= 2 and noteno_str[1] == '2':
            rleasamt = 0.0

        r['REBMTD']   = rebmtd
        r['BALANCE']  = balance
        r['RLEASAMT'] = rleasamt
        r['BALMNI']   = balmni
        results.append(r)

    if not results:
        return pl.DataFrame()

    abba_out = pl.DataFrame(results).select(
        ["ACCTNO", "NOTENO", "REBMTD", "BALANCE", "RLEASAMT", "BALMNI"]
    ).sort(["ACCTNO", "NOTENO"])

    # Keep FIRST.ACCTNO & FIRST.NOTENO
    abba_out = abba_out.unique(subset=["ACCTNO", "NOTENO"], keep="first")
    return abba_out


# ============================================================================
# STEP 4: Merge ABBA updates back into NOTE
# ============================================================================
def merge_abba_into_note(note_df: pl.DataFrame, abba_df: pl.DataFrame) -> pl.DataFrame:
    """
    Left-merge NOTE with ABBA updates on ACCTNO.
    Mirrors: DATA BNM.NOTE; MERGE NOTE(IN=A) ABBA(IN=B); BY ACCTNO; IF A.
    """
    if abba_df.is_empty():
        return note_df
    return note_df.join(
        abba_df.rename({"BALANCE": "BALANCE_ABBA", "BALMNI": "BALMNI_ABBA",
                        "RLEASAMT": "RLEASAMT_ABBA"}),
        on="ACCTNO", how="left"
    ).with_columns([
        pl.coalesce(["BALANCE_ABBA",  "BALANCE"]).alias("BALANCE"),
        pl.coalesce(["BALMNI_ABBA",   "BALMNI"]).alias("BALMNI"),
        pl.coalesce(["RLEASAMT_ABBA", "RLEASAMT"]).alias("RLEASAMT"),
    ]).drop(["BALANCE_ABBA", "BALMNI_ABBA", "RLEASAMT_ABBA"])


# ============================================================================
# STEP 5: Merge NOTE with LNCOMM — compute APPRLIMT, RLEASAMT, UNDRAWN, APPRLIM2
# ============================================================================
def compute_commitment(note_df: pl.DataFrame, lncomm: pl.DataFrame,
                       sdate: str, rdate: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Merge NOTE with LNCOMM by ACCTNO+COMMNO.
    Compute APPRLIMT, RLEASAMT, UNDRAWN, APPRLIM2.
    Returns (note_df_updated, alwnocom_df).
    Mirrors: DATA BNM.NOTE&REPTMON&NOWK ALWNOCOM; MERGE BNM.NOTE LNCOMM.
    """
    sdate_d = datetime.strptime(sdate, "%d/%m/%Y").date()
    rdate_d = datetime.strptime(rdate, "%d/%m/%Y").date()

    note_sorted = note_df.sort(["ACCTNO", "COMMNO", "CHK"])
    lncomm_s    = lncomm.sort(["ACCTNO", "COMMNO"])

    merged = note_sorted.join(lncomm_s, on=["ACCTNO", "COMMNO"], how="left", suffix="_COMM")

    note_rows   = []
    alwnocom_rows = []

    # Track first flags per ACCTNO+COMMNO
    seen_acct_comm: dict = {}

    for r in merged.to_dicts():
        acctno   = r.get('ACCTNO')
        commno   = r.get('COMMNO', 0) or 0
        prodcd   = str(r.get('PRODCD', '') or '')
        paidind  = str(r.get('PAIDIND', '') or '')
        ntint    = str(r.get('NTINT', '') or '')
        balance  = float(r.get('BALANCE', 0) or 0)
        feeamt   = float(r.get('FEEAMT', 0) or 0)
        curbal   = float(r.get('CURBAL', 0) or 0)
        accrual  = float(r.get('ACCRUAL', 0) or 0)
        balmni   = float(r.get('BALMNI', 0) or 0)
        corgamt  = float(r.get('CORGAMT', 0) or 0)
        cavaiamt = float(r.get('CAVAIAMT', 0) or 0)
        orgbal   = float(r.get('ORGBAL', 0) or 0)
        ccuramt  = float(r.get('CCURAMT', 0) or 0)
        cusedamt = float(r.get('CUSEDAMT', 0) or 0)
        netproc  = float(r.get('NETPROC', 0) or 0)
        payfreq  = r.get('PAYFREQ', 0) or 0
        rebate   = float(r.get('REBATE', 0) or 0)
        intearn4 = float(r.get('INTEARN4', 0) or 0)
        issdte   = r.get('ISSDTE')
        noteno   = r.get('NOTENO', 0) or 0
        product  = r.get('PRODUCT', 0) or 0

        # BALMNI negative fix — REVISE BALMNI VIA 2022-3698
        if paidind != 'P' and balmni < 0 and ntint == 'S':
            balmni = curbal + feeamt + accrual
            r['BALMNI'] = balmni

        key = (acctno, commno)
        is_first_acct  = acctno not in {k[0] for k in seen_acct_comm}
        is_first_comm  = key not in seen_acct_comm
        seen_acct_comm[key] = True

        apprlimt = 0.0; rleasamt = 0.0; apprlim2 = 0.0

        if prodcd == '34111':
            # HP — no commitment segment
            apprlimt = curbal - (rebate + intearn4)
            apprlim2 = balance - feeamt
            if issdte and sdate_d <= issdte <= rdate_d:
                if not (98000 <= noteno <= 99999):
                    rleasamt = netproc
        else:
            rc_prods = {'34190', '34690', '34170'}
            if commno > 0:
                if prodcd not in rc_prods:
                    rleasamt = corgamt - cavaiamt
                    if rleasamt > corgamt:
                        rleasamt = corgamt
                    apprlimt = corgamt
                    if prodcd not in {'34180', '34190', '34690', '34170'}:
                        apprlim2 = balance - feeamt + apprlimt - rleasamt
                    else:
                        if payfreq in (1, 2, 3, 4, 6, 9):
                            apprlim2 = curbal
                        else:
                            apprlim2 = corgamt
                        if prodcd in rc_prods:
                            if is_first_acct or is_first_comm:
                                apprlim2 = apprlimt
                            else:
                                apprlim2 = 0.0
                else:
                    rleasamt = cusedamt
                    apprlimt = ccuramt
                    if prodcd not in {'34180', '34190', '34690', '34170'}:
                        apprlim2 = balance - feeamt + apprlimt - rleasamt
                    else:
                        apprlim2 = ccuramt
                        if prodcd in rc_prods:
                            if is_first_acct or is_first_comm:
                                apprlim2 = apprlimt
                            else:
                                apprlim2 = 0.0
            else:
                rleasamt = orgbal
                apprlimt = orgbal
                if prodcd not in {'34180', '34190', '34690', '34170'}:
                    apprlim2 = balance - feeamt
                else:
                    apprlim2 = curbal
                    if prodcd in rc_prods:
                        apprlim2 = orgbal
                        alwnocom_rows.append({**r,
                            'APPRLIMT': apprlimt, 'RLEASAMT': rleasamt, 'APPRLIM2': apprlim2})

        undrawn = apprlimt - rleasamt
        if undrawn < 0:
            undrawn = 0.0

        if prodcd == '34111':
            undrawn  = 0.0
            rleasamt = netproc

        if product in HP_PRODUCTS:
            apprlimt = netproc

        r.update({
            'APPRLIMT': apprlimt, 'RLEASAMT': rleasamt, 'APPRLIM2': apprlim2,
            'UNDRAWN': undrawn,
        })
        note_rows.append(r)

    note_out    = pl.DataFrame(note_rows)    if note_rows    else pl.DataFrame()
    alwnocom_df = pl.DataFrame(alwnocom_rows) if alwnocom_rows else pl.DataFrame()
    return note_out, alwnocom_df


# ============================================================================
# STEP 6: RC manipulation (APPRLIM2 dedup for revolving credit)
# ============================================================================
def manipulate_rc(note_df: pl.DataFrame, alwnocom: pl.DataFrame) -> pl.DataFrame:
    """
    Mirrors: PROC SORT ALWNOCOM; DATA APPR1 DUP; PROC SORT DUP; DATA APPR1;
    Resolves duplicate APPRLIM2 for RC products and merges back into NOTE.
    """
    if alwnocom.is_empty():
        return note_df

    rc_prods = {'34190', '34690', '34170'}

    alwnocom_s = alwnocom.filter(
        pl.col("PRODCD").is_in(list(rc_prods))
    ).sort(["ACCTNO", "APPRLIM2"])

    appr1_rows = []; dup_rows = []
    seen: dict = {}
    for r in alwnocom_s.to_dicts():
        key = (r['ACCTNO'], r['APPRLIM2'])
        if key not in seen:
            seen[key] = True
            appr1_rows.append(r)
        else:
            r['DUPLI'] = 1
            dup_rows.append(r)

    dup_df = pl.DataFrame(dup_rows) if dup_rows else pl.DataFrame()
    if not dup_df.is_empty():
        dup_df = dup_df.filter(pl.col("BALANCE") >= pl.col("APPRLIM2")).sort(["ACCTNO", "APPRLIM2"])

    # Final APPR1 resolution
    appr1_final = []
    if appr1_rows:
        appr1_df = pl.DataFrame(appr1_rows)
        if not dup_df.is_empty():
            merged_dup = appr1_df.join(
                dup_df.select(["ACCTNO", "APPRLIM2", "DUPLI"]),
                on=["ACCTNO", "APPRLIM2"], how="left"
            )
        else:
            merged_dup = appr1_df.with_columns(pl.lit(None).cast(pl.Int64).alias("DUPLI"))

        seen2: dict = {}
        for r in merged_dup.to_dicts():
            key = (r['ACCTNO'], r['APPRLIM2'])
            dupli = r.get('DUPLI', 0) or 0
            if dupli == 1:
                if float(r.get('BALANCE', 0) or 0) >= float(r.get('APPRLIM2', 0) or 0):
                    pass  # APPRLIM2 stays
                else:
                    r['APPRLIM2'] = 0.0
            else:
                if key not in seen2:
                    seen2[key] = True
                else:
                    r['APPRLIM2'] = 0.0
            appr1_final.append({'ACCTNO': r['ACCTNO'], 'NOTENO': r['NOTENO'],
                                 'APPRLIM2': r['APPRLIM2']})

    if not appr1_final:
        return note_df

    appr1_out = pl.DataFrame(appr1_final).sort(["ACCTNO", "NOTENO"])
    note_s    = note_df.sort(["ACCTNO", "NOTENO"])

    merged = note_s.join(appr1_out.rename({"APPRLIM2": "APPRLIM2_RC"}),
                         on=["ACCTNO", "NOTENO"], how="left")
    merged = merged.with_columns(
        pl.coalesce(["APPRLIM2_RC", "APPRLIM2"]).alias("APPRLIM2")
    ).drop("APPRLIM2_RC")

    # Recompute UNDRAWN for RC
    merged = merged.with_columns(
        pl.when(pl.col("PRODCD").is_in(list(rc_prods)))
          .then((pl.col("APPRLIM2") - pl.col("RLEASAMT")).clip(lower_bound=0))
          .otherwise(pl.col("UNDRAWN"))
          .alias("UNDRAWN"),
        pl.when((pl.col("PRODCD").is_in(list(rc_prods))) & (pl.col("APPRLIM2") > 0))
          .then(pl.col("APPRLIM2"))
          .otherwise(pl.col("APPRLMTACCT") if "APPRLMTACCT" in merged.columns else pl.lit(None))
          .alias("APPRLMTACCT")
    )
    return merged


# ============================================================================
# STEP 7: X:1 Commitment manipulation
# ============================================================================
def manipulate_x1_commitment(note_df: pl.DataFrame) -> pl.DataFrame:
    """
    Mirrors the X:1 commitment manipulation:
      PROC SUMMARY → APPRSUM;
      DATA APPR2 DUP; SET NOTE; first-record APPRLMTACCT logic;
      Merge back with APPRSUM and recompute UNDRAWN / APPRLIM2 proportionally.
    """
    rc_prods = {'34190', '34690', '34170'}

    note_s = note_df.sort(["ACCTNO", "COMMNO", "NOTENO"])

    # PROC SUMMARY by ACCTNO+COMMNO for BALANCE and APPRLIM2 sums
    apprsum = (
        note_s.group_by(["ACCTNO", "COMMNO"])
              .agg([
                  pl.col("BALANCE").sum().alias("BALSUM"),
                  pl.col("APPRLIM2").sum().alias("APPRSUM"),
              ])
    )

    appr2_rows = []; dup_rows = []
    seen_comm: dict = {}

    for r in note_s.to_dicts():
        acctno  = r['ACCTNO']
        commno  = r.get('COMMNO', 0) or 0
        prodcd  = str(r.get('PRODCD', '') or '')
        apprlimt = float(r.get('APPRLIMT', 0) or 0)
        apprlim2 = float(r.get('APPRLIM2', 0) or 0)

        key = (acctno, commno)
        is_first_acct = acctno not in {k[0] for k in seen_comm}
        is_first_comm = key not in seen_comm
        seen_comm[key] = True

        if prodcd not in rc_prods and commno != 0:
            if is_first_acct or is_first_comm:
                apprlmtacct = apprlimt
                r['APPRLMTACCT'] = apprlmtacct
                appr2_rows.append(r)
            else:
                r['DUPLIAPR']    = 1
                r['APPRLMTACCT'] = 0.0
                dup_rows.append(r)
        else:
            r['APPRLMTACCT'] = apprlimt
            appr2_rows.append(r)

    # Merge DUP flags back
    merged_rows = note_s.to_dicts()
    dup_comm_set = {(r['ACCTNO'], r.get('COMMNO', 0)) for r in dup_rows}
    dup_note_map = {(r['ACCTNO'], r.get('NOTENO')): r.get('APPRLMTACCT', 0)
                    for r in dup_rows}

    for r in merged_rows:
        key_cn = (r['ACCTNO'], r.get('COMMNO', 0))
        key_nn = (r['ACCTNO'], r.get('NOTENO'))
        r['DUPLIAPR']    = 1 if key_cn in dup_comm_set else 0
        r['APPRLMTACCT'] = dup_note_map.get(key_nn, r.get('APPRLMTACCT', 0))

    note_merged = pl.DataFrame(merged_rows).join(apprsum, on=["ACCTNO", "COMMNO"], how="left")

    # Proportional UNDRAWN / APPRLIM2 for duplicate commitment rows
    result_rows = []
    for r in note_merged.to_dicts():
        prodcd   = str(r.get('PRODCD', '') or '')
        dupliapr = r.get('DUPLIAPR', 0) or 0
        balance  = float(r.get('BALANCE', 0) or 0)
        balsum   = float(r.get('BALSUM', 1) or 1)
        feeamt   = float(r.get('FEEAMT', 0) or 0)
        undrawn  = float(r.get('UNDRAWN', 0) or 0)
        apprsum_v = float(r.get('APPRSUM', 0) or 0)
        apprlim2 = float(r.get('APPRLIM2', 0) or 0)

        if prodcd not in rc_prods and dupliapr == 1:
            ratio = balance / balsum if balsum else 0
            undrawn = ratio * undrawn
            if undrawn > 0:
                apprlim2 = balance - feeamt + undrawn
            else:
                apprlim2 = ratio * apprsum_v
            r['UNDRAWN']  = undrawn
            r['APPRLIM2'] = apprlim2

        result_rows.append(r)

    return pl.DataFrame(result_rows)


# ============================================================================
# STEP 8: Build UNOTE (undrawn commitment records not in NOTE)
# ============================================================================
def build_unote(lnnotex_acctno_commno: pl.DataFrame,
                lncomm_full: pl.DataFrame, lnnote_lookup: pl.DataFrame,
                reptdate: date, rdate: str, sdate: str) -> pl.DataFrame:
    """
    Build BNM.UNOTE<REPTMON><NOWK>:
      Records in LNCOMM that have no matching LNNOTE (B AND NOT A).
    Applies format mappings and commitment logic.
    Mirrors: DATA BNM.UNOTE&REPTMON&NOWK; MERGE UNOTE UCOMM; IF NOT A AND B.
    """
    # Anti-join: LNCOMM rows not present in LNNOTEX by ACCTNO+COMMNO
    unote_keys = lnnotex_acctno_commno.select(["ACCTNO", "COMMNO"])
    ucomm = lncomm_full.join(unote_keys, on=["ACCTNO", "COMMNO"], how="anti")

    if ucomm.is_empty():
        return pl.DataFrame()

    # Merge with LNNOTE lookup (ACCBRCH, CUSTCODE, CRISPURP, SECTOR, SECTOLD, COLLDESC, CENSUS, LIABCODE)
    ucomm_merged = ucomm.join(lnnote_lookup, on="ACCTNO", how="left", suffix="_NOTE")

    result_rows = []
    for r in ucomm_merged.to_dicts():
        acctno   = r.get('ACCTNO')
        commno   = r.get('COMMNO', 0) or 0
        custcode = r.get('CUSTCODE', 0) or 0
        product  = r.get('CPRODUCT', 0) or 0
        crispurp = str(r.get('CRISPURP', '') or '')
        sector   = str(r.get('CSECTOR', '') or '')
        sectold  = str(r.get('SECTOLD', '') or '').ljust(4)
        colldesc = str(r.get('COLLDESC', '') or '').ljust(70)
        census   = float(r.get('CENSUS', 0) or 0)
        liabcode = str(r.get('LIABCODE', '') or '')
        revovli  = str(r.get('REVOVLI', '') or '')
        corigmt  = float(r.get('CORIGMT', 0) or 0)
        corgamt  = float(r.get('CORGAMT', 0) or 0)
        cavaiamt = float(r.get('CAVAIAMT', 0) or 0)
        ccuramt  = float(r.get('CCURAMT', 0) or 0)
        cusedamt = float(r.get('CUSEDAMT', 0) or 0)
        accbrch  = r.get('ACCBRCH', 0) or 0
        branch   = r.get('CMBRCH', 0) or 0
        cappdate = r.get('CAPPDATE')
        expiredt = r.get('EXPIREDT')

        if branch == 0:
            branch = accbrch

        custcd = format_locustcd(custcode)
        amtind = format_lndenom(product)
        sectorcd = sector

        fisspurp, custcd = derive_fisspurp(
            crispurp, sectold, colldesc, census, product, liabcode, custcd
        )

        # +--- LINE BELOW COMMENTED OUT BECAUSE IF CPRODUCT = 100,101,120 ---+
        # |  THEREFORE, CPRODUCT DOES NOT INDICATE ACTUAL LOAN PRODUCT.       |
        # |  FOR REPORTING PURPOSE, WE ONLY NEED TO KNOW WHETHER UNDRAWN      |
        # |    IS FOR TERM LOANS, OD OR OTHERS.                               |
        # |  AT COMMITMENT LEVEL, NO INFO ON LOAN PRODUCT. WE ARE MAKING      |
        # |    THE ASSUMPTION THAT REVOVLI 'Y' IS RC, OTHERWISE TERM LOANS NIE|
        # +--------------------------------------------------------------------+
        if revovli == 'Y':
            product_out = 925; prodcd = '34190'
        else:
            product_out = 999; prodcd = '34149'

        if product == 139:
            prodcd = '34190'

        origmt  = format_lnormt(corigmt)
        exprdate = parse_sas_z11_date(expiredt)
        apprdate = parse_sas_z11_date(cappdate)

        # BALANCE = 0 at commitment level
        balance = 0.0; balmni = 0.0

        if revovli == 'N':
            rleasamt = corgamt - cavaiamt
            apprlimt = corgamt
        else:
            rleasamt = cusedamt
            apprlimt = ccuramt

        undrawn = apprlimt - rleasamt
        if undrawn < 0:
            undrawn = 0.0

        result_rows.append({
            **r,
            'BRANCH': branch,
            'CUSTCD': custcd, 'AMTIND': amtind,
            'SECTORCD': sectorcd, 'SECTOR': sector,
            'FISSPURP': fisspurp,
            'PRODUCT': product_out, 'PRODCD': prodcd,
            'ORIGMT': origmt,
            'EXPRDATE': exprdate, 'APPRDATE': apprdate,
            'BALANCE': balance, 'BALMNI': balmni,
            'RLEASAMT': rleasamt, 'APPRLIMT': apprlimt, 'UNDRAWN': undrawn,
        })

    return pl.DataFrame(result_rows)


# ============================================================================
# MAIN
# ============================================================================
def main(reptmon: str = None, nowk: str = None,
         reptdate: date = None, tdate: int = None,
         rdate: str = None, sdate: str = None) -> None:
    """
    Main entry point for LALWPBBC.
    Parameters are set by the calling job (EIBDWKLX) from BNM.REPTDATE macros.
    """
    log.info("LALWPBBC started.")

    # ----------------------------------------------------------------
    # Load LNNOTE + SME08
    # ----------------------------------------------------------------
    lnnotex = load_lnnotex(reptdate, tdate, rdate, sdate)
    log.info("LNNOTEX loaded: %d rows", len(lnnotex))

    # ----------------------------------------------------------------
    # Build BNM.NOTE<REPTMON><NOWK>
    # ----------------------------------------------------------------
    note_df = build_note(lnnotex, reptdate, tdate, rdate, sdate, reptmon, nowk)
    log.info("NOTE built: %d rows", len(note_df))

    # ----------------------------------------------------------------
    # Load LNCOMM
    # ----------------------------------------------------------------
    lncomm_raw = pl.read_parquet(LNCOMM_PATH).sort(["ACCTNO", "COMMNO"])

    # ----------------------------------------------------------------
    # ABBA product processing
    # ----------------------------------------------------------------
    abba_df = process_abba(note_df, lncomm_raw, sdate, rdate)
    note_df = merge_abba_into_note(note_df, abba_df)
    log.info("ABBA merge done.")

    # ----------------------------------------------------------------
    # Merge NOTE with LNCOMM → compute APPRLIMT / UNDRAWN / APPRLIM2
    # ----------------------------------------------------------------
    note_df, alwnocom = compute_commitment(note_df, lncomm_raw, sdate, rdate)
    log.info("Commitment merge done. ALWNOCOM: %d rows", len(alwnocom))

    # ----------------------------------------------------------------
    # RC manipulation
    # ----------------------------------------------------------------
    note_df = manipulate_rc(note_df, alwnocom)
    log.info("RC manipulation done.")

    # ----------------------------------------------------------------
    # X:1 Commitment manipulation
    # ----------------------------------------------------------------
    note_df = manipulate_x1_commitment(note_df)
    log.info("X:1 commitment manipulation done.")

    # ----------------------------------------------------------------
    # Build LNNOTE lookup (NODUPKEYS by ACCTNO) for UNOTE
    # ----------------------------------------------------------------
    lnnote_lookup = (
        lnnotex.select(["ACCTNO", "ACCBRCH", "NAME", "CUSTCODE", "CRISPURP",
                        "SECTOR", "SECTOLD", "COLLDESC", "CENSUS", "LIABCODE"])
               .unique(subset=["ACCTNO"], keep="first")
               .sort("ACCTNO")
    )

    # LNCOMM2: active, fully available commitments
    lncomm2 = lncomm_raw.filter(
        (pl.col("ACTIND") != "N") &
        (pl.col("CAVAIAMT") == pl.col("CORGAMT"))
    ).sort("ACCTNO")

    # Merge LNNOTE lookup into LNCOMM2 (BRANCH fallback to ACCBRCH)
    lncomm_merged = lncomm2.join(lnnote_lookup, on="ACCTNO", how="left", suffix="_NOTE")
    lncomm_merged = lncomm_merged.with_columns(
        pl.when(pl.col("CMBRCH") == 0)
          .then(pl.col("ACCBRCH"))
          .otherwise(pl.col("CMBRCH"))
          .alias("BRANCH")
    )

    lnnotex_keys = lnnotex.select(["ACCTNO", "COMMNO"]).unique().sort(["ACCTNO", "COMMNO"])

    # ----------------------------------------------------------------
    # Build BNM.UNOTE<REPTMON><NOWK>
    # ----------------------------------------------------------------
    unote_df = build_unote(lnnotex_keys, lncomm_merged, lnnote_lookup,
                           reptdate, rdate, sdate)
    log.info("UNOTE built: %d rows", len(unote_df))

    # ----------------------------------------------------------------
    # Write outputs
    # ----------------------------------------------------------------
    note_path  = OUTPUT_DIR / f"NOTE{reptmon}{nowk}.parquet"
    unote_path = OUTPUT_DIR / f"UNOTE{reptmon}{nowk}.parquet"

    note_df.write_parquet(note_path)
    log.info("Written: %s", note_path)

    unote_df.write_parquet(unote_path)
    log.info("Written: %s", unote_path)

    log.info("LALWPBBC completed.")
    log.info("*** CONTINUE BY LALWPBBD UPON COMPLETION OF DEPOSIT ***")


if __name__ == "__main__":
    # Standalone execution: load REPTDATE from parquet
    rd_df = pl.read_parquet(REPTDATE_PATH)
    row   = rd_df.row(0, named=True)

    reptdate_val: date = row["REPTDATE"]
    reptmon_val  = f"{reptdate_val.month:02d}"
    nowk_val     = f"{reptdate_val.day:02d}"
    rdate_val    = reptdate_val.strftime("%d/%m/%Y")
    sdate_val    = rdate_val
    tdate_val    = (reptdate_val - date(1960, 1, 1)).days  # SAS date integer

    main(
        reptmon=reptmon_val,
        nowk=nowk_val,
        reptdate=reptdate_val,
        tdate=tdate_val,
        rdate=rdate_val,
        sdate=sdate_val,
    )
