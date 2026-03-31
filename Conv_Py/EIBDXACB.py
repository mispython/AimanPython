#!/usr/bin/env python3
"""
Program : EIBDXACB.py
Purpose : Generate report on daily external account balances (Detica AML)
          Extract from SA, CA, FD, UMA, DCI, K1TBL, K3TBL — PBB & PIBB

SMR 2015-2395

Original JCL Steps:
  DELETE    — Remove prior output datasets (XACB.DAILY, PBB/PIBB TEXT, XACBFTP)
  CREATE    — Pre-allocate XACBFTP dataset (FB, LRECL=80)
  EIBDXACB  — SAS program (this conversion)
  RUNSFTP   — FTP/SFTP delivery to CTCS server, DRR system, EDW
              (external step; Python equivalents write the FTP command scripts only)

Inputs  (Parquet):
  FD    : SAP.PBB.MNIFD.DAILY(0)   — PBB Fixed Deposits
  IFD   : SAP.PIBB.MNIFD.DAILY(0)  — PIBB Fixed Deposits
  DP    : SAP.PBB.MNITB.DAILY(0)   — PBB Deposit (CA/SA/UMA/REPTDATE)
  IDP   : SAP.PIBB.MNITB.DAILY(0)  — PIBB Deposit (CA/SA/UMA)
  UMAC  : SAP.PBB.UMA.DAILY(0)     — PBB UMA_HOE
  UMAI  : SAP.PIBB.UMA.DAILY(0)    — PIBB UMA_HOE
  DCID  : SAP.PBB.DCIWH.DAILY      — DCI (date-suffixed table)
  NID   : SAP.PBB.RNID.SASDATA     — PBB RNID (date-suffixed)
  INID  : SAP.PIBB.RNID.SASDATA    — PIBB RNID (date-suffixed)
  KA    : SAP.PBB.DKAPITI.SASDATA  — PBB K1TBL / K3TBL (date-suffixed)
  IKA   : SAP.PIBB.DKAPITI.SASDATA — PIBB K1TBL / K3TBL (date-suffixed)
  EQ    : SAP.PBB.EQUT(0)          — FX Forwards (date-suffixed)

Outputs (.txt, semicolon-delimited):
  PBBFILE  : SAP.PBB.XACB.DAILY.TEXT  — PBB external account balances report
  PIBBFILE : SAP.PIBB.XACB.DAILY.TEXT — PIBB external account balances report
  SFTP01   : FTP PUT command script for CTCS server
  SFTP02   : FTP PUT command script for DRR system

Note: PROC CPORT (XACBFTP SAS transport file for EDW) is a mainframe-only
      SAS binary format and cannot be reproduced in Python. The Parquet
      pipeline supersedes this delivery channel.

Dependencies:
  PBBDPFMT : caprod_format, cadenom_format, ddcustcd_format,
             fdprod_format, fddenom_format, fdcustcd_format,
             ifdcuscd_format, saprod_format, sadenom_format,
             sacustcd_format, CURX
"""

import os
from pathlib import Path
from datetime import date, timedelta

import duckdb
import polars as pl

from PBBDPFMT import (
    caprod_format,
    cadenom_format,
    ddcustcd_format,
    fdprod_format,
    fddenom_format,
    fdcustcd_format,
    ifdcuscd_format,
    saprod_format,
    sadenom_format,
    sacustcd_format,
    CURX,
)

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR     = Path(os.environ.get("BASE_DIR", "/data/xacb"))
PARQUET_DIR  = Path(os.environ.get("PARQUET_DIR", "/data/parquet"))
OUTPUT_DIR   = Path(os.environ.get("OUTPUT_DIR", "/data/output/xacb"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input Parquet paths (date-independent base tables; suffixed tables resolved at runtime)
P_FD        = PARQUET_DIR / "PBB_MNIFD_DAILY.parquet"        # FD
P_IFD       = PARQUET_DIR / "PIBB_MNIFD_DAILY.parquet"       # IFD
P_DP        = PARQUET_DIR / "PBB_MNITB_DAILY.parquet"        # DP  (CA/SA/UMA/REPTDATE)
P_IDP       = PARQUET_DIR / "PIBB_MNITB_DAILY.parquet"       # IDP (CA/SA/UMA)
P_UMAC      = PARQUET_DIR / "PBB_UMA_DAILY.parquet"          # UMAC UMA_HOE
P_UMAI      = PARQUET_DIR / "PIBB_UMA_DAILY.parquet"         # UMAI UMA_HOE
P_DCID      = PARQUET_DIR / "PBB_DCIWH_DAILY.parquet"        # DCID (date-suffixed)
P_NID       = PARQUET_DIR / "PBB_RNID.parquet"               # NID  (date-suffixed)
P_INID      = PARQUET_DIR / "PIBB_RNID.parquet"              # INID (date-suffixed)
P_KA        = PARQUET_DIR / "PBB_DKAPITI.parquet"            # KA K1/K3TBL (date-suffixed)
P_IKA       = PARQUET_DIR / "PIBB_DKAPITI.parquet"           # IKA K1/K3TBL (date-suffixed)
P_EQ        = PARQUET_DIR / "PBB_EQUT_DAILY.parquet"         # EQ UTFX (date-suffixed)

# Output file paths
OUT_PBBFILE  = OUTPUT_DIR / "PBB_XACB_DAILY.txt"
OUT_PIBBFILE = OUTPUT_DIR / "PIBB_XACB_DAILY.txt"
OUT_SFTP01   = OUTPUT_DIR / "SFTP01_CTCS.txt"
OUT_SFTP02   = OUTPUT_DIR / "SFTP02_DRR.txt"

# SAS epoch for date integer conversion
_SAS_EPOCH = date(1960, 1, 1)


# =============================================================================
# UTILITY: SAS date integer → Python date
# =============================================================================
def _sas_date(val) -> date:
    if val is None:
        return None
    return _SAS_EPOCH + timedelta(days=int(val))


# =============================================================================
# LOCAL FORMAT: REMFMT
# Equivalent of:
#   VALUE REMFMT
#      LOW-0.1 = '01'   (up to 1 week — strictly < 0.1 months)
#      0.1-1   = '02'   (>1 wk - 1 mth — 0.1 <= remmth <= 1)
#      OTHER   = '03'   (>1 month)
# =============================================================================
def _remfmt(remmth: float) -> str:
    if remmth < 0.1:
        return '01'
    elif remmth <= 1:
        return '02'
    else:
        return '03'


# =============================================================================
# UTILITY: Count months between two SAS date integers (COUNTMON subroutine)
# Used in ACB.K1 and ACB.IK1 (AT43000 subroutine within IK1).
# SAS COUNTMON rolls forward by calendar months preserving day-of-month,
# clamping to month-end where needed (e.g. Jan-31 → Feb-28/29).
# =============================================================================
def _count_months(gwsdt_int, gwmdt_int) -> int:
    """
    Equivalent of the COUNTMON LINK subroutine in SAS.
    Returns number of complete calendar months from GWSDT to GWMDT.
    Returns 0 if either date is 0/None or GWMDT < GWSDT.
    """
    if not gwsdt_int or not gwmdt_int:
        return 0
    gwsdt = _sas_date(gwsdt_int)
    gwmdt = _sas_date(gwmdt_int)
    if gwmdt < gwsdt:
        return 0

    folmonth = gwsdt
    nummonth = 0
    nextday  = gwsdt.day

    while gwmdt > folmonth:
        nummonth += 1
        nextmon  = folmonth.month + 1
        nextyear = folmonth.year
        if nextmon > 12:
            nextmon  -= 12
            nextyear += 1
        # Clamp day to valid calendar day (mirrors SAS MDY clamping)
        if nextday in (29, 30, 31) and nextmon == 2:
            # Last day of February
            feb_last = 29 if (nextyear % 4 == 0 and
                              (nextyear % 100 != 0 or nextyear % 400 == 0)) else 28
            folmonth = date(nextyear, 3, 1) - timedelta(days=1) \
                       if nextday > feb_last else date(nextyear, 2, nextday)
        elif nextday == 31 and nextmon in (4, 6, 9, 11):
            folmonth = date(nextyear, nextmon, 30)
        elif nextday == 30 and folmonth.month in (4, 6, 9, 11) \
                and nextmon in (1, 3, 5, 7, 8, 10, 12):
            folmonth = date(nextyear, nextmon, 31)
        else:
            folmonth = date(nextyear, nextmon, nextday)

    return nummonth


# =============================================================================
# UTILITY: REMMTH calculation for UTFX (DATA ACB.UTFX %REMMTH macro)
# Computes remaining months from STRTDATE to MATDATE using reporting-date
# day-count denominators.  When DAYS < 8, REMMTH = 0.1 (up to 1 week bucket).
# =============================================================================
def _calc_remmth(strtdate_int, matdate_int) -> float:
    """
    Equivalent of the %REMMTH SAS macro.
    Returns remaining months as a float.
    """
    if not strtdate_int or not matdate_int:
        return 0.0
    strtdate = _sas_date(strtdate_int)
    matdate  = _sas_date(matdate_int)

    days_in_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    # Leap year February adjustment for reporting month (STRTDATE month)
    yr   = strtdate.year
    mth  = strtdate.month
    if mth == 2 and (yr % 4 == 0 and (yr % 100 != 0 or yr % 400 == 0)):
        days_in_month[2] = 29

    rp_days_mth = days_in_month[mth]  # RPDAYS(RPMTH)

    # Clamp reporting day to days-in-month
    rpday = min(strtdate.day, rp_days_mth)

    mdday = min(matdate.day, rp_days_mth)
    remy  = matdate.year  - strtdate.year
    remm  = matdate.month - strtdate.month
    remd  = mdday - rpday

    return remy * 12 + remm + remd / rp_days_mth


# =============================================================================
# UTILITY: DP42190 — BNM code mapping for K1 conventional deposits (42190)
# Used in both ACB.K1 and ACB.IK1 LINK subroutines.
# =============================================================================
def _dp42190(gwc2r: str, gwctp: str) -> str:
    """
    Map GWC2R / GWCTP to a 14-character BNMCODE in the 4219x series.
    Returns '' if no match.
    """
    bnmcode = ''

    # Primary SELECT on GWC2R
    _select = {
        '01': '4219001000000Y',
        '02': '4219002000000Y',
        '03': '4219003000000Y',
        '07': '4219007000000Y',
        '12': '4219012000000Y',
        '17': '4219017000000Y',
        '57': '4219057000000Y',
        '59': '4219060000000Y',
        '71': '4219071000000Y',
        '72': '4219072000000Y',
        '73': '4219073000000Y',
        '74': '4219074000000Y',
        '75': '4219075000000Y',
        '79': '4219079000000Y',
    }
    if gwc2r in ('13', '20', '45'):
        bnmcode = '4219020000000Y'
    elif gwc2r in ('77', '78'):
        bnmcode = '4219076000000Y'
    elif gwc2r in ('82', '83', '84'):
        bnmcode = '4219081000000Y'
    elif gwc2r in _select:
        bnmcode = _select[gwc2r]

    # Range overrides (all override the SELECT result)
    try:
        c2r_int = int(gwc2r)
    except (ValueError, TypeError):
        c2r_int = -1

    if  4 <= c2r_int <=  6:  bnmcode = '4219020000000Y'
    if 30 <= c2r_int <= 40:  bnmcode = '4219020000000Y'
    if 41 <= c2r_int <= 44:  bnmcode = '4219060000000Y'
    if 46 <= c2r_int <= 49:  bnmcode = '4219060000000Y'
    if 51 <= c2r_int <= 54:  bnmcode = '4219060000000Y'
    if 61 <= c2r_int <= 64:  bnmcode = '4219060000000Y'
    if 66 <= c2r_int <= 69:  bnmcode = '4219060000000Y'
    if 86 <= c2r_int <= 99:  bnmcode = '4219085000000Y'

    # GWCTP fallback when BNMCODE still blank
    if not bnmcode:
        if gwctp in ('BB', 'BQ', 'BC', 'BP'): bnmcode = '4219002000000Y'
        if gwctp in ('BJ',):                   bnmcode = '4219007000000Y'
        if gwctp in ('BM', 'BN'):              bnmcode = '4219012000000Y'
        if gwctp in ('BF', 'BH', 'BR', 'BS',
                     'BT', 'BU', 'BV', 'BZ'):  bnmcode = '4219020000000Y'
        if gwctp in ('BG',):                   bnmcode = '4219017000000Y'
        if gwctp in ('AC', 'AD', 'CA', 'CB',
                     'CD', 'CF', 'CG', 'DD',
                     'CC'):                    bnmcode = '4219060000000Y'
        if gwctp in ('DA',):                   bnmcode = '4219071000000Y'
        if gwctp in ('DB',):                   bnmcode = '4219072000000Y'
        if gwctp in ('DC',):                   bnmcode = '4219074000000Y'
        if gwctp in ('EA', 'EC', 'EJ'):        bnmcode = '4219076000000Y'
        if gwctp in ('FA',):                   bnmcode = '4219090000000Y'
        if gwctp in ('BA', 'BE', 'BW'):        bnmcode = '4219081000000Y'
        if gwctp in ('EB', 'CE', 'GA'):        bnmcode = '4219085000000Y'

    return bnmcode


# =============================================================================
# UTILITY: DA42160 — BNM code mapping for K1 repo (42160)
# Used in both ACB.K1 and ACB.IK1 LINK subroutines.
# =============================================================================
def _da42160(gwc2r: str, gwctp: str) -> str:
    """
    Map GWC2R / GWCTP to a 14-character BNMCODE in the 4216x series.
    Default is '4216060000000Y'; overridden in priority order.
    Returns '' if bnmcode ends up empty (not possible here — default always set).
    """
    bnmcode = '4216060000000Y'

    # Priority chain (first match wins — mirrors SAS IF/ELSE chain)
    if   gwc2r == '57':                                  bnmcode = '4216057000000Y'
    elif gwc2r == '75':                                  bnmcode = '4216075000000Y'
    elif gwctp in ('BP', 'BC'):                          bnmcode = '4216001000000Y'
    elif gwctp == 'BB':                                  bnmcode = '4216002000000Y'
    elif gwctp == 'BI':                                  bnmcode = '4216003000000Y'
    elif gwctp == 'BJ':                                  bnmcode = '4216007000000Y'
    elif gwctp == 'BQ':                                  bnmcode = '4216011000000Y'
    elif gwctp == 'BM':                                  bnmcode = '4216012000000Y'
    elif gwctp == 'BN':                                  bnmcode = '4216013000000Y'
    elif gwctp == 'BG':                                  bnmcode = '4216017000000Y'
    elif gwctp in ('BR', 'BF', 'BH', 'BZ', 'BU',
                   'AD', 'BT', 'BV', 'BS'):              bnmcode = '4216020000000Y'
    elif gwctp == 'DA':                                  bnmcode = '4216071000000Y'
    elif gwctp == 'DB':                                  bnmcode = '4216072000000Y'
    elif gwctp == 'DC':                                  bnmcode = '4216074000000Y'
    elif gwctp in ('EA', 'EC'):                          bnmcode = '4216076000000Y'
    elif gwctp == 'FA':                                  bnmcode = '4216079000000Y'
    elif gwctp in ('BW', 'BA', 'BE'):                   bnmcode = '4216081000000Y'
    elif gwctp in ('CE', 'EB', 'GA'):                   bnmcode = '4216085000000Y'

    return bnmcode


# =============================================================================
# STEP 1: Derive REPTDATE and macro variables from DP.REPTDATE
# =============================================================================
def _load_reptdate() -> dict:
    """
    Equivalent of DATA ACB.REPTDATE REPTDATE step.
    Reads REPTDATE from the PBB deposit table and derives all date-related
    macro variables used throughout the program.
    Returns a dict of macro variable equivalents.
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{P_DP}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate_int = row[0]
    reptdate     = _sas_date(reptdate_int)
    dd           = reptdate.day

    if   dd <=  8:  wk = '1'
    elif dd <= 15:  wk = '2'
    elif dd <= 22:  wk = '3'
    else:           wk = '4'

    reptmon  = f"{reptdate.month:02d}"
    reptyear = str(reptdate.year)[-2:]   # YEAR2. format (2-digit year)
    reptday  = f"{reptdate.day:02d}"
    rdate    = reptdate.strftime("%d/%m/%Y")          # DDMMYYN. → dd/mm/yyyy
    fdate    = reptdate.strftime("%d%m%y")            # DDMMYY6. → ddmmyy
    edate    = reptdate.strftime("%y%m%d")            # YYMMDD6. → yymmdd

    return {
        'reptdate':     reptdate,
        'reptdate_int': reptdate_int,
        'nowk':         wk,
        'reptmon':      reptmon,
        'reptyear':     reptyear,
        'reptday':      reptday,
        'rdate':        rdate,
        'fdate':        fdate,
        'edate':        edate,
        'tdate':        reptdate_int,
    }


# =============================================================================
# STEP 2: Extract _CA — Current Account external balances (PBB + PIBB)
# =============================================================================
def _extract_ca() -> pl.DataFrame:
    """
    Equivalent of DATA ACB._CA.
    Combines PBB and PIBB CURRENT tables; filters to CURX products with
    external customer codes; maps to BNMCODE.
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    rows = []

    con = duckdb.connect()
    df = con.execute(
        f"""
        SELECT PRODUCT, CUSTCODE, CURBAL, OPENIND
        FROM read_parquet('{P_DP}')
        WHERE OPENIND NOT IN ('B','C','P') AND CURBAL >= 0
        UNION ALL
        SELECT PRODUCT, CUSTCODE, CURBAL, OPENIND
        FROM read_parquet('{P_IDP}')
        WHERE OPENIND NOT IN ('B','C','P') AND CURBAL >= 0
        """
    ).pl()
    con.close()

    for row in df.iter_rows(named=True):
        product  = int(row['PRODUCT'])  if row['PRODUCT']  is not None else None
        custcode = int(row['CUSTCODE']) if row['CUSTCODE'] is not None else None
        amount   = float(row['CURBAL'])

        prodcd  = caprod_format(product)
        amtind  = cadenom_format(product)

        # Customer code determination
        if product == 104:
            custcd = '02'
        elif product == 105:
            custcd = '81'
        else:
            custcd = ddcustcd_format(custcode)

        # Product 105 → VOSTRO always output regardless of CURX membership
        if product == 105:
            rows.append({'BNMCODE': 'VOSTROACCBALXX', 'AMTIND': amtind, 'AMOUNT': amount})

        # 42199 external FI (product 72, custcodes 82-84)
        if (prodcd == '42199' and product == 72 and
                custcode in (82, 83, 84)):
            rows.append({'BNMCODE': '4219981000000Y', 'AMTIND': amtind, 'AMOUNT': amount})

        # 42199 foreign entities (products 72/79/80, custcodes 86-99)
        if (prodcd == '42199' and product in (72, 79, 80) and
                custcode in (86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99)):
            rows.append({'BNMCODE': '4219985000000Y', 'AMTIND': amtind, 'AMOUNT': amount})

        # Only continue for CURX products (product 105 NOT in CURX per SAS comment)
        if product not in CURX:
            continue
        # Delete products 63 and 163
        if product in (63, 163):
            continue

        # External custcodes → domestic demand deposit
        if custcd in ('86', '87', '88', '89', '90', '91',
                      '92', '95', '96', '98', '99'):
            rows.append({'BNMCODE': '4211085000000Y', 'AMTIND': amtind, 'AMOUNT': amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 3: Extract _FD — Fixed Deposit external balances (PBB + PIBB)
# =============================================================================
def _extract_fd() -> pl.DataFrame:
    """
    Equivalent of DATA ACB._FD.
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    rows = []

    con = duckdb.connect()
    df = con.execute(
        f"""
        SELECT INTPLAN, CUSTCD, CURBAL, OPENIND, ACCTTYPE
        FROM read_parquet('{P_FD}')
        WHERE OPENIND IN ('O','D')
        UNION ALL
        SELECT INTPLAN, CUSTCD, CURBAL, OPENIND, ACCTTYPE
        FROM read_parquet('{P_IFD}')
        WHERE OPENIND IN ('O','D')
        """
    ).pl()
    con.close()

    for row in df.iter_rows(named=True):
        intplan  = int(row['INTPLAN'])  if row['INTPLAN']  is not None else None
        custcd_i = int(row['CUSTCD'])   if row['CUSTCD']   is not None else None
        accttype = int(row['ACCTTYPE']) if row['ACCTTYPE'] is not None else None
        amount   = float(row['CURBAL'])

        prodcd = fdprod_format(intplan)
        amtind = fddenom_format(intplan)

        # ACCTTYPE overrides
        if accttype in (302, 315, 394, 396):
            prodcd = '42133'
        if accttype in (397, 398):
            prodcd = '42199'

        # Customer code: conventional vs Islamic FD
        if prodcd in ('42130', '42630'):
            custcode = fdcustcd_format(custcd_i)
        else:
            custcode = ifdcuscd_format(custcd_i)

        # Map to BNMCODE (multiple outputs possible per record)
        _ext_81 = (81, 82, 83, 84)
        _ext_86 = (86, 87, 88, 89, 90, 91, 92, 95, 96, 98, 99)

        try:
            cc = int(custcode)
        except (ValueError, TypeError):
            cc = 0

        if prodcd == '42130':
            if cc in _ext_81:
                rows.append({'BNMCODE': '4213081000000Y', 'AMTIND': amtind, 'AMOUNT': amount})
            if cc in _ext_86:
                rows.append({'BNMCODE': '4213085000000Y', 'AMTIND': amtind, 'AMOUNT': amount})
        if prodcd == '42132':
            if cc in _ext_81:
                rows.append({'BNMCODE': '4213281000000Y', 'AMTIND': amtind, 'AMOUNT': amount})
            if cc in _ext_86:
                rows.append({'BNMCODE': '4213285000000Y', 'AMTIND': amtind, 'AMOUNT': amount})
        if prodcd == '42133':
            if cc in _ext_81:
                rows.append({'BNMCODE': '4213381000000Y', 'AMTIND': amtind, 'AMOUNT': amount})
            if cc in _ext_86:
                rows.append({'BNMCODE': '4213385000000Y', 'AMTIND': amtind, 'AMOUNT': amount})
        if prodcd == '42199':
            if cc in _ext_81:
                rows.append({'BNMCODE': '4219981000000Y', 'AMTIND': amtind, 'AMOUNT': amount})
            if cc in _ext_86:
                rows.append({'BNMCODE': '4219985000000Y', 'AMTIND': amtind, 'AMOUNT': amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 4: Extract _UMA — UMA external balances (PBB + PIBB)
# =============================================================================
def _extract_uma() -> pl.DataFrame:
    """
    Equivalent of DATA ACB._UMA.
    UMA from DP.UMA (PBB) and IDP.UMA (PIBB); product 297 → 'D', else 'I'.
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    rows = []

    con = duckdb.connect()
    df = con.execute(
        f"""
        SELECT PRODUCT, CUSTCODE, CURBAL
        FROM read_parquet('{P_DP}')
        WHERE OPENIND IN ('O','D')
          AND CURBAL IS NOT NULL
        UNION ALL
        SELECT PRODUCT, CUSTCODE, CURBAL
        FROM read_parquet('{P_IDP}')
        WHERE OPENIND IN ('O','D')
          AND CURBAL IS NOT NULL
        """
        # Note: SAS reads from DP.UMA and IDP.UMA sub-datasets; in the Parquet
        # pipeline these are filtered segments of the main MNITB daily file.
    ).pl()
    con.close()

    _ext_81 = ('81', '82', '83', '84')
    _ext_86 = ('86', '87', '88', '89', '90', '91', '92', '95', '96', '98', '99')

    for row in df.iter_rows(named=True):
        product  = int(row['PRODUCT'])  if row['PRODUCT']  is not None else None
        custcode = str(row['CUSTCODE']) if row['CUSTCODE'] is not None else ''
        amount   = float(row['CURBAL'])

        amtind = 'D' if product == 297 else 'I'
        # prodcd is always '42199' for UMA

        if custcode in _ext_81:
            rows.append({'BNMCODE': '4219981000000Y', 'AMTIND': amtind, 'AMOUNT': amount})
        if custcode in _ext_86:
            rows.append({'BNMCODE': '4219985000000Y', 'AMTIND': amtind, 'AMOUNT': amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 5: Extract _SA — Savings Account external balances (PBB + PIBB)
# =============================================================================
def _extract_sa() -> pl.DataFrame:
    """
    Equivalent of DATA ACB._SA.
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    rows = []

    con = duckdb.connect()
    df = con.execute(
        f"""
        SELECT PRODUCT, CUSTCODE, CURBAL
        FROM read_parquet('{P_DP}')
        WHERE OPENIND NOT IN ('B','C','P') AND CURBAL >= 0
        UNION ALL
        SELECT PRODUCT, CUSTCODE, CURBAL
        FROM read_parquet('{P_IDP}')
        WHERE OPENIND NOT IN ('B','C','P') AND CURBAL >= 0
        """
        # Note: SAS reads from DP.SAVING and IDP.SAVING sub-datasets.
    ).pl()
    con.close()

    _ext_86 = ('86', '87', '88', '89', '90', '91', '92', '95', '96', '98', '99')

    for row in df.iter_rows(named=True):
        product  = int(row['PRODUCT'])  if row['PRODUCT']  is not None else None
        custcode = int(row['CUSTCODE']) if row['CUSTCODE'] is not None else None
        amount   = float(row['CURBAL'])

        prodcd = saprod_format(product)
        amtind = sadenom_format(product)
        custcd = sacustcd_format(custcode)

        if prodcd in ('42120', '42320') and custcd in _ext_86:
            rows.append({'BNMCODE': '4212085000000Y', 'AMTIND': amtind, 'AMOUNT': amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 6: Extract _UMAHOE — UMA HOE external balances (PBB + PIBB)
# =============================================================================
def _extract_umahoe() -> pl.DataFrame:
    """
    Equivalent of DATA ACB._UMAHOE.
    PBB UMA_HOE → AMTIND='D'; PIBB UMA_HOE → AMTIND='I'.
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    rows = []

    _ext_86 = ('86', '87', '88', '89', '90', '91', '92', '95', '96', '98', '99')

    for parquet, amtind in [(P_UMAC, 'D'), (P_UMAI, 'I')]:
        con = duckdb.connect()
        df = con.execute(
            f"SELECT CUSTCODE, AMOUNT FROM read_parquet('{parquet}')"
        ).pl()
        con.close()
        for row in df.iter_rows(named=True):
            custcd = str(row['CUSTCODE']) if row['CUSTCODE'] is not None else ''
            amount = float(row['AMOUNT'])
            if custcd in _ext_86:
                rows.append({'BNMCODE': '4219985000000Y',
                             'AMTIND':  amtind,
                             'AMOUNT':  amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 7: Extract K3 — PBB K3TBL (conventional repo/interbank)
# =============================================================================
def _extract_k3(reptmon: str, nowk: str) -> pl.DataFrame:
    """
    Equivalent of DATA ACB.K3.
    Reads KA.K3TBL{REPTMON}{NOWK}; AMTIND='D'.
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    # In Parquet pipeline the table suffix is embedded as a column or filter;
    # assume the Parquet file is pre-partitioned by period and filtered here.
    _ifd_types  = ('IFD', 'ILD', 'ISD', 'IZD', 'IDC', 'IDP', 'IZP')
    _ref_types  = ('PFD', 'PLD', 'PSD', 'PZD', 'PDC')

    rows = []
    con = duckdb.connect()
    df  = con.execute(
        f"""
        SELECT UTAMOC, UTDPF, UTSTY, UTREF, UTCTP
        FROM read_parquet('{P_KA}')
        WHERE REPTMON='{reptmon}' AND WK='{nowk}'
        """
        # Column REPTMON and WK assumed present in the partitioned Parquet.
    ).pl()
    con.close()

    for row in df.iter_rows(named=True):
        utsty  = str(row['UTSTY'] or '')
        utref  = str(row['UTREF'] or '')
        utctp  = str(row['UTCTP'] or '')
        utamoc = float(row['UTAMOC'] or 0)
        utdpf  = float(row['UTDPF'] or 0)

        amount = utamoc - utdpf
        if utsty == 'IDC':
            amount = utamoc + utdpf

        if utsty in _ifd_types and utref in _ref_types:
            if   utctp in ('BA', 'BW', 'BE'):
                rows.append({'BNMCODE': '4215081000000Y', 'AMTIND': 'D', 'AMOUNT': amount})
            elif utctp in ('EB', 'CE', 'GA'):
                rows.append({'BNMCODE': '4215085000000Y', 'AMTIND': 'D', 'AMOUNT': amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 8: Extract IK3 — PIBB K3TBL (Islamic repo/interbank)
# =============================================================================
def _extract_ik3(reptmon: str, nowk: str) -> pl.DataFrame:
    """
    Equivalent of DATA ACB.IK3.
    Reads IKA.K3TBL{REPTMON}{NOWK}; AMTIND='I'.
    Amount = UTAMOC - UTDPF + UTAICT (IK3 differs from K3 by adding UTAICT).
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    _ifd_types = ('IFD', 'ILD', 'ISD', 'IZD', 'IDC', 'IDP')
    _ref_types = ('PFD', 'PLD', 'PSD', 'PZD', 'PDC')

    rows = []
    con = duckdb.connect()
    df  = con.execute(
        f"""
        SELECT UTAMOC, UTDPF, UTAICT, UTSTY, UTREF, UTCTP
        FROM read_parquet('{P_IKA}')
        WHERE REPTMON='{reptmon}' AND WK='{nowk}'
        """
    ).pl()
    con.close()

    for row in df.iter_rows(named=True):
        utsty  = str(row['UTSTY']  or '')
        utref  = str(row['UTREF']  or '')
        utctp  = str(row['UTCTP']  or '')
        utamoc = float(row['UTAMOC']  or 0)
        utdpf  = float(row['UTDPF']  or 0)
        utaict = float(row['UTAICT'] or 0)

        amount = utamoc - utdpf + utaict

        if utsty in _ifd_types and utref in _ref_types:
            if   utctp in ('BA', 'BW', 'BE'):
                rows.append({'BNMCODE': '4215081000000Y', 'AMTIND': 'I', 'AMOUNT': amount})
            elif utctp in ('EB', 'CE', 'GA'):
                rows.append({'BNMCODE': '4215085000000Y', 'AMTIND': 'I', 'AMOUNT': amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 9: Extract K1 — PBB K1TBL (conventional GW balances)
# =============================================================================
def _extract_k1(reptmon: str, nowk: str) -> pl.DataFrame:
    """
    Equivalent of DATA ACB.K1.
    Two LINK subroutines: DP42190 (4219x) and DA42160 (4216x).
    GWDLP IN ('BCQ','BCD')       → DP42190
    GWDLP substr(2,2) IN ('MI','MT') → DA42160
    AMTIND='D'.
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    rows = []
    con  = duckdb.connect()
    df   = con.execute(
        f"""
        SELECT GWBALC, GWCCY, GWMVT, GWMVTS, GWDLP, GWC2R, GWCTP
        FROM read_parquet('{P_KA}')
        WHERE REPTMON='{reptmon}' AND WK='{nowk}'
        """
    ).pl()
    con.close()

    for row in df.iter_rows(named=True):
        gwbalc = float(row['GWBALC'] or 0)
        gwccy  = str(row['GWCCY']  or '')
        gwmvt  = str(row['GWMVT']  or '')
        gwmvts = str(row['GWMVTS'] or '')
        gwdlp  = str(row['GWDLP']  or '')
        gwc2r  = str(row['GWC2R']  or '')
        gwctp  = str(row['GWCTP']  or '')

        amount = gwbalc

        # DP42190 link: BCQ or BCD products, MYR, P/M
        if (gwccy == 'MYR' and gwmvt == 'P' and gwmvts == 'M'
                and gwdlp in ('BCQ', 'BCD')):
            bnmcode = _dp42190(gwc2r, gwctp)
            if bnmcode:
                rows.append({'BNMCODE': bnmcode, 'AMTIND': 'D', 'AMOUNT': amount})

        # DA42160 link: GWDLP[1:3] IN ('MI','MT'), MYR, P/M
        if (len(gwdlp) >= 3 and gwdlp[1:3] in ('MI', 'MT')
                and gwccy == 'MYR' and gwmvt == 'P' and gwmvts == 'M'):
            bnmcode = _da42160(gwc2r, gwctp)
            if bnmcode:
                rows.append({'BNMCODE': bnmcode, 'AMTIND': 'D', 'AMOUNT': amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 10: Extract IK1 — PIBB K1TBL (Islamic GW balances)
# =============================================================================
def _extract_ik1(reptmon: str, nowk: str) -> pl.DataFrame:
    """
    Equivalent of DATA ACB.IK1.
    Three LINK subroutines: DP42190 (BCS/BCT/BCW/BQD), DA42160 (MI/MT suffix),
    AT43000 (BF/BOW/BSW/BOI/BFI → interbank 4314x).
    AMTIND='I'.
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    rows = []
    con  = duckdb.connect()
    df   = con.execute(
        f"""
        SELECT GWBALC, GWCCY, GWMVT, GWMVTS, GWDLP, GWC2R, GWCTP, GWSDT, GWMDT
        FROM read_parquet('{P_IKA}')
        WHERE REPTMON='{reptmon}' AND WK='{nowk}'
        """
    ).pl()
    con.close()

    for row in df.iter_rows(named=True):
        gwbalc = float(row['GWBALC'] or 0)
        gwccy  = str(row['GWCCY']  or '')
        gwmvt  = str(row['GWMVT']  or '')
        gwmvts = str(row['GWMVTS'] or '')
        gwdlp  = str(row['GWDLP']  or '')
        gwc2r  = str(row['GWC2R']  or '')
        gwctp  = str(row['GWCTP']  or '')
        gwsdt  = row['GWSDT']
        gwmdt  = row['GWMDT']

        amount = gwbalc

        # DP42190 link: BCS/BCT/BCW/BQD products, MYR, P/M
        if (gwccy == 'MYR' and gwmvt == 'P' and gwmvts == 'M'
                and gwdlp in ('BCS', 'BCT', 'BCW', 'BQD')):
            bnmcode = _dp42190(gwc2r, gwctp)
            if bnmcode:
                rows.append({'BNMCODE': bnmcode, 'AMTIND': 'I', 'AMOUNT': amount})

        # DA42160 link: GWDLP[1:3] IN ('MI','MT'), MYR, P/M
        if (len(gwdlp) >= 3 and gwdlp[1:3] in ('MI', 'MT')
                and gwccy == 'MYR' and gwmvt == 'P' and gwmvts == 'M'):
            bnmcode = _da42160(gwc2r, gwctp)
            if bnmcode:
                rows.append({'BNMCODE': bnmcode, 'AMTIND': 'I', 'AMOUNT': amount})

        # AT43000 link: BF/BOW/BSW/BOI/BFI products
        # (SMR 2019-3772 — Islamic interbank borrowings ≤1yr / >1yr)
        if gwdlp in ('BF', 'BOW', 'BSW', 'BOI', 'BFI'):
            if gwccy == 'MYR' and gwmvt == 'P' and gwmvts == 'M':
                if gwctp in ('BA', 'BE'):
                    nummonth = _count_months(gwsdt, gwmdt)
                    if nummonth <= 12:
                        bnmcode = '4314081100000Y'
                    else:
                        bnmcode = '4314081200000Y'
                    rows.append({'BNMCODE': bnmcode, 'AMTIND': 'I', 'AMOUNT': amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 11: Extract DCI — Domestic Certificate of Deposit external balances
# =============================================================================
def _extract_dci(reptday: str, tdate_int) -> pl.DataFrame:
    """
    Equivalent of DATA ACB.DCI.
    Filter: MATDT > REPTDATE AND STARTDT <= REPTDATE; INVCURR='MYR'; custcodes 82-99.
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    rows = []
    tdate = int(tdate_int)

    _ext_81 = set(range(82, 85))   # 82,83,84
    _ext_86 = set(range(86, 100))  # 86-99

    con = duckdb.connect()
    df  = con.execute(
        f"""
        SELECT CUSTCODE, INVAMT, INVCURR, MATDT, STARTDT
        FROM read_parquet('{P_DCID}')
        WHERE REPTDAY='{reptday}'
          AND MATDT > {tdate} AND STARTDT <= {tdate}
          AND INVCURR = 'MYR'
        """
    ).pl()
    con.close()

    for row in df.iter_rows(named=True):
        custcode = int(row['CUSTCODE']) if row['CUSTCODE'] is not None else 0
        amount   = float(row['INVAMT'] or 0)

        custcd = f"{custcode:02d}"

        if custcode in _ext_81:
            rows.append({'BNMCODE': '4219181000000Y', 'AMTIND': 'D', 'AMOUNT': amount})
        if custcode in _ext_86:
            rows.append({'BNMCODE': '4219185000000Y', 'AMTIND': 'D', 'AMOUNT': amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 12: Extract RNID — Negotiable Instruments of Deposit (PBB + PIBB)
# =============================================================================
def _extract_rnid(reptday: str) -> pl.DataFrame:
    """
    Equivalent of DATA ACB.RNID.
    Filter: NIDSTAT='N' AND CURBAL>0; custcodes 82-84 → 81 bucket, 86-99 → 85 bucket.
    PBB → AMTIND='D'; PIBB → AMTIND='I'.
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    rows = []
    _ext_81 = set(range(82, 85))
    _ext_86 = set(range(86, 100))

    for parquet, amtind in [(P_NID, 'D'), (P_INID, 'I')]:
        con = duckdb.connect()
        df  = con.execute(
            f"""
            SELECT CUSTCD, CURBAL
            FROM read_parquet('{parquet}')
            WHERE REPTDAY='{reptday}' AND NIDSTAT='N' AND CURBAL > 0
            """
        ).pl()
        con.close()

        for row in df.iter_rows(named=True):
            custcd = int(row['CUSTCD']) if row['CUSTCD'] is not None else 0
            amount = float(row['CURBAL'])

            if custcd in _ext_81:
                rows.append({'BNMCODE': '4215081000000Y', 'AMTIND': amtind, 'AMOUNT': amount})
            elif custcd in _ext_86:
                rows.append({'BNMCODE': '4215085000000Y', 'AMTIND': amtind, 'AMOUNT': amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 13: Extract UTFX — FX Forwards (PBB conventional, AMTIND='D')
# =============================================================================
def _extract_utfx(edate: str, tdate_int) -> pl.DataFrame:
    """
    Equivalent of DATA ACB.UTFX.
    Filter: CUSTFISS 80-99, PURCHCUR='MYR', CUSTEQTP<>'BW',
            STRTDATE<=TDATE, DEALTYPE IN ('BO','BF').
    REMMTH < 8 days → 0.1; otherwise use %REMMTH macro.
    DEALTYPE='BO' → '4314081100000Y'
    DEALTYPE='BF' → '42150812' || REMFMT(REMMTH) || '000Y'
    Output columns: BNMCODE (str), AMTIND (str), AMOUNT (float)
    """
    rows = []
    tdate = int(tdate_int)

    _ext_custfiss = {str(i) for i in range(80, 100)}

    con = duckdb.connect()
    df  = con.execute(
        f"""
        SELECT CUSTFISS, AMTPAY, PURCHCUR, CUSTEQTP,
               STRTDATE, MATDATE, DEALTYPE
        FROM read_parquet('{P_EQ}')
        WHERE EDATE='{edate}'
          AND PURCHCUR = 'MYR'
          AND CUSTEQTP <> 'BW'
          AND STRTDATE <= {tdate}
          AND DEALTYPE IN ('BO','BF')
        """
    ).pl()
    con.close()

    for row in df.iter_rows(named=True):
        custfiss  = str(row['CUSTFISS'] or '')
        strtdate  = row['STRTDATE']
        matdate   = row['MATDATE']
        amount    = float(row['AMTPAY'] or 0)
        dealtype  = str(row['DEALTYPE'] or '')

        if custfiss not in _ext_custfiss:
            continue

        days = (int(matdate or 0) - int(strtdate or 0))
        if days < 8:
            remmth = 0.1
        else:
            remmth = _calc_remmth(strtdate, matdate)

        if dealtype == 'BO':
            bnmcode = '4314081100000Y'
        else:  # BF
            bnmcode = '42150812' + _remfmt(remmth) + '000Y'

        rows.append({'BNMCODE': bnmcode, 'AMTIND': 'D', 'AMOUNT': amount})

    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 14: Consolidate — PROC SUMMARY (sum AMOUNT by BNMCODE within AMTIND)
# =============================================================================
def _summarise(all_df: pl.DataFrame, amtind_filter: str) -> pl.DataFrame:
    """
    Equivalent of PROC SUMMARY DATA=ALL NWAY CLASS BNMCODE AMTIND VAR AMOUNT SUM.
    Returns a wide-format DataFrame keyed on BNMCODE with summed AMOUNT.
    """
    return (
        all_df
        .filter(pl.col('AMTIND') == amtind_filter)
        .group_by(['BNMCODE', 'AMTIND'])
        .agg(pl.col('AMOUNT').sum())
    )


# =============================================================================
# STEP 15: Build CALC accumulators — PBB running subtotals (from ALM / AMTIND='D')
# =============================================================================
def _build_calc(alm: pl.DataFrame) -> pl.DataFrame:
    """
    Equivalent of DATA CALC followed by PROC TRANSPOSE.
    Iterates ALM (AMTIND='D') accumulating subtotal variables C1_0 … CTOT,
    then transposes to long form with BNMCODE / AMOUNT columns so it can be
    unioned with ALM for the PBB merge.
    """
    c = {k: 0.0 for k in ('C1_0', 'C1_03', 'C1_08', 'C1_11', 'C1_12',
                           'C1_14', 'C2_0', 'C4_0', 'CTOT')}

    for row in alm.iter_rows(named=True):
        bn  = row['BNMCODE']
        amt = float(row['AMOUNT'] or 0)

        if bn in ('4211085000000Y', '4212085000000Y',
                  '4213081000000Y', '4213085000000Y',
                  '4215081000000Y', '4215085000000Y',
                  '4219081000000Y', '4219085000000Y',
                  '4219181000000Y', '4219185000000Y',
                  '4219981000000Y', '4219985000000Y'):
            c['C1_0'] += amt
        if bn in ('4213081000000Y', '4213085000000Y'):
            c['C1_03'] += amt
        if bn in ('4215081000000Y', '4215085000000Y'):
            c['C1_08'] += amt
        if bn in ('4219081000000Y', '4219085000000Y'):
            c['C1_11'] += amt
        if bn in ('4219181000000Y', '4219185000000Y'):
            c['C1_12'] += amt
        if bn in ('4219981000000Y', '4219985000000Y'):
            c['C1_14'] += amt
        if bn in ('4216081000000Y', '4216085000000Y'):
            c['C2_0'] += amt
        if bn in ('4314081100000Y', '4314081201000Y',
                  '4314081202000Y', '4314081203000Y'):
            c['C4_0'] += amt
        if bn in ('4211085000000Y', '4212085000000Y',
                  '4213081000000Y', '4213085000000Y',
                  '4215081000000Y', '4215085000000Y',
                  '4219081000000Y', '4219085000000Y',
                  '4219181000000Y', '4219185000000Y',
                  '4219981000000Y', '4219985000000Y',
                  '4216081000000Y', '4216085000000Y',
                  'VOSTROACCBALXX',
                  '4314081100000Y', '4314081200000Y'):
            c['CTOT'] += amt

    # Transpose: one row per accumulator variable (mimics PROC TRANSPOSE)
    rows = [{'BNMCODE': k, 'AMTIND': 'D', 'AMOUNT': v} for k, v in c.items()]
    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 16: Build ICALC accumulators — PIBB running subtotals (from IALM / AMTIND='I')
# =============================================================================
def _build_icalc(ialm: pl.DataFrame) -> pl.DataFrame:
    """
    Equivalent of DATA ICALC followed by PROC TRANSPOSE.
    Iterates IALM (AMTIND='I') accumulating I1_0 … ITOT.
    """
    c = {k: 0.0 for k in ('I1_0', 'I1_05', 'I1_06', 'I1_08', 'I1_11',
                           'I1_14', 'I4_0', 'ITOT')}

    for row in ialm.iter_rows(named=True):
        bn  = row['BNMCODE']
        amt = float(row['AMOUNT'] or 0)

        if bn in ('4211085000000Y', '4212085000000Y',
                  '4213281000000Y', '4213285000000Y',
                  '4213381000000Y', '4213385000000Y',
                  '4215081000000Y', '4215085000000Y',
                  '4219081000000Y', '4219085000000Y',
                  '4219981000000Y', '4219985000000Y'):
            c['I1_0'] += amt
        if bn in ('4213281000000Y', '4213285000000Y'):
            c['I1_05'] += amt
        if bn in ('4213381000000Y', '4213385000000Y'):
            c['I1_06'] += amt
        if bn in ('4215081000000Y', '4215085000000Y'):
            c['I1_08'] += amt
        if bn in ('4219081000000Y', '4219085000000Y'):
            c['I1_11'] += amt
        if bn in ('4219981000000Y', '4219985000000Y'):
            c['I1_14'] += amt
        if bn in ('4314081100000Y', '4314081200000Y'):
            c['I4_0'] += amt
        if bn in ('4211085000000Y', '4212085000000Y',
                  '4213281000000Y', '4213285000000Y',
                  '4213381000000Y', '4213385000000Y',
                  '4215081000000Y', '4215085000000Y',
                  '4219081000000Y', '4219085000000Y',
                  '4219981000000Y', '4219985000000Y',
                  '4314081100000Y', '4314081200000Y'):
            c['ITOT'] += amt

    rows = [{'BNMCODE': k, 'AMTIND': 'I', 'AMOUNT': v} for k, v in c.items()]
    return pl.DataFrame(rows, schema={'BNMCODE': pl.Utf8,
                                      'AMTIND':  pl.Utf8,
                                      'AMOUNT':  pl.Float64})


# =============================================================================
# STEP 17: Assign ITEM numbers — PBB and PIBB
# =============================================================================
# Maps BNMCODE (or accumulator key) → float ITEM number for report ordering.
_PBB_ITEM_MAP: dict = {
    'C1_0':           1.0,
    '4211085000000Y': 1.01,
    '4212085000000Y': 1.02,
    'C1_03':          1.03,
    'C1_04':          1.04,
    'C1_05':          1.05,
    'C1_06':          1.06,
    'C1_07':          1.07,
    'C1_08':          1.08,
    'C1_09':          1.09,
    'C1_10':          1.10,
    'C1_11':          1.11,
    'C1_12':          1.12,
    'C1_13':          1.13,
    'C1_14':          1.14,
    'C2_0':           2.0,
    'VOSTROACCBALXX': 3.0,
    'C4_0':           4.0,
    '4314081100000Y': 4.01,
    '4314081201000Y': 4.02,
    '4314081202000Y': 4.03,
    '4314081203000Y': 4.04,
    'CTOT':           999.0,
}

_PIBB_ITEM_MAP: dict = {
    'I1_0':           1.0,
    '4211085000000Y': 1.01,
    '4212085000000Y': 1.02,
    'I1_03':          1.03,
    'I1_04':          1.04,
    'I1_05':          1.05,
    'I1_06':          1.06,
    'I1_07':          1.07,
    'I1_08':          1.08,
    'I1_09':          1.09,
    'I1_10':          1.10,
    'I1_11':          1.11,
    'I1_12':          1.12,
    'I1_13':          1.13,
    'I1_14':          1.14,
    'I2_0':           2.0,
    'VOSTROACCBALXX': 3.0,
    'I4_0':           4.0,
    '4314081100000Y': 4.01,
    '4314081200000Y': 4.02,
    'ITOT':           999.0,
}

# Required ITEM scaffold — ensures every line appears even with zero balance
_ALL_PBB_ITEMS  = [1.0, 1.01, 1.02, 1.03, 1.04, 1.05, 1.06, 1.07, 1.08,
                   1.09, 1.10, 1.11, 1.12, 1.13, 1.14,
                   2.0, 3.0, 4.0, 4.01, 4.02, 4.03, 4.04, 999.0]

_ALL_PIBB_ITEMS = [1.0, 1.01, 1.02, 1.03, 1.04, 1.05, 1.06, 1.07, 1.08,
                   1.09, 1.10, 1.11, 1.12, 1.13, 1.14,
                   2.0, 3.0, 4.0, 4.01, 4.02, 999.0]


def _assign_items(df: pl.DataFrame, item_map: dict) -> pl.DataFrame:
    """
    Equivalent of DATA PBB/PIBB: assign ITEM to each BNMCODE row.
    Drops rows with no ITEM mapping (IF ITEM NOT EQ '').
    """
    rows = []
    for row in df.iter_rows(named=True):
        bn  = row['BNMCODE']
        amt = row['AMOUNT']
        item = item_map.get(bn)
        if item is not None:
            rows.append({'ITEM': item, 'BNMCODE': bn, 'AMOUNT': float(amt or 0)})
    return pl.DataFrame(rows, schema={'ITEM':    pl.Float64,
                                      'BNMCODE': pl.Utf8,
                                      'AMOUNT':  pl.Float64})


def _merge_with_scaffold(data_df: pl.DataFrame,
                         all_items: list,
                         exclude_items: set | None = None) -> pl.DataFrame:
    """
    Equivalent of MERGE REPORT(IN=A) data(IN=B) BY ITEM; IF A.
    Ensures all scaffold items appear; fills AMOUNT=0 where not in data.
    Excludes specified ITEM values (for PIBB items 4.03, 4.04).
    """
    scaffold = pl.DataFrame({'ITEM': all_items}, schema={'ITEM': pl.Float64})

    # Sum duplicate ITEM rows from data (in case multiple BNCODEs share same ITEM)
    if len(data_df) > 0:
        agg = (
            data_df
            .group_by('ITEM')
            .agg(pl.col('AMOUNT').sum())
        )
        merged = scaffold.join(agg, on='ITEM', how='left')
    else:
        merged = scaffold.with_columns(pl.lit(None).cast(pl.Float64).alias('AMOUNT'))

    merged = merged.with_columns(
        pl.col('AMOUNT').fill_null(0.0)
    )

    if exclude_items:
        merged = merged.filter(~pl.col('ITEM').is_in(list(exclude_items)))

    return merged.sort('ITEM')


# =============================================================================
# STEP 18: Write PBBFILE — semicolon-delimited report (LRECL=200)
# =============================================================================
def _write_pbbfile(pbb_df: pl.DataFrame, rdate: str, fdate: str) -> None:
    """
    Equivalent of DATA _NULL_ SET PBB FILE PBBFILE PUT.
    Writes header rows then one semicolon-delimited line per ITEM.
    'N.A ' is written for items not applicable to PBB.
    """
    # Item descriptions and whether they have numeric AMOUNT or 'N.A '
    # Keyed on ITEM value
    _PBB_LINES = {
        999.0:  ('TOTAL EXTERNAL ACCOUNT BALANCES         ', True),
        1.0:    ('DEPOSITS ACCEPTED                       ', True),
        1.01:   ('DEMAND DEPOSITS ACCEPTED                ', True),
        1.02:   ('SAVINGS DEPOSITS ACCEPTED               ', True),
        1.03:   ('FIXED DEPOSITS ACCEPTED                 ', True),
        1.04:   ('SPECIFIC INVESTMENT ACCOUNTS ACCEPTED   ', False),
        1.05:   ('GENERAL INVESTMENT ACCOUNTS ACCEPTED    ', False),
        1.06:   ('COMMODITY MURABAHAH DEPOSITS ACCEPTED   ', False),
        1.07:   ('CALL DEPOSITS ACCEPTED                  ', False),
        1.08:   ('NEGOTIABLE INSTRUMENTS DEPOSITS ISSUED  ', True),
        1.09:   ('SPECIAL DEPOSITS ACCEPTED               ', False),
        1.10:   ('HOUSING DEVELOPMENT ACCOUNT DEPOSITS ACCEPTED                                ', False),
        1.11:   ('SHORT TERM DEPOSITS ACCEPTED            ', True),
        1.12:   ('INVESTMENT-LINKED TO DERIVATIVES OFFERED', True),
        1.13:   ('ELIGIBLE LIABILITIES EXEMPT DEPOSITS ACCEPTED                                ', False),
        1.14:   ('OTHER DEPOSITS ACCEPTED                 ', True),
        2.0:    ('REPURCHASE AGREEMENT                    ', True),
        3.0:    ('VOSTRO ACCOUNT BALANCES                 ', True),
        4.0:    ('INTERBANK BORROWINGS                    ', True),
        4.01:   ('INTERBANK BORROWING FOR ORIGINAL MATURITY EQUAL TO OVERNIGHT             ', True),
        4.02:   ('INTERBANK BORROWING FOR ORIGINAL MATURITY MORE THAN OVERNIGHT UP TO 1 WEEK ', True),
        4.03:   ('INTERBANK BORROWING FOR ORIGINAL MATURITY MORE THAN 1 WEEK UP TO 1 MONTH  ', True),
        4.04:   ('INTERBANK BORROWING FOR ORIGINAL MATURITY MORE THAN 1 MONTH              ', True),
    }

    with open(OUT_PBBFILE, 'w', encoding='ascii', errors='replace') as f:
        # Header lines (first record)
        f.write(f"REPORT ON DAILY EXTERNAL ACCOUNT BALANCES AS AT {rdate}\n")
        f.write(" ;DATA ITEM;PBB\n")
        f.write(" ; ;RM\n")

        for row in pbb_df.iter_rows(named=True):
            item   = row['ITEM']
            amount = row['AMOUNT']
            if item not in _PBB_LINES:
                continue
            desc, has_amount = _PBB_LINES[item]
            amt_str = str(amount) if has_amount else 'N.A '
            # item prefix: blank for 999, numeric otherwise
            item_str = ' ' if item == 999.0 else str(item)
            line = f"{item_str};{desc};{amt_str}"
            f.write(line + '\n')

    print(f"[PBBFILE] Written: {OUT_PBBFILE}")


# =============================================================================
# STEP 19: Write PIBBFILE — semicolon-delimited report (LRECL=200)
# =============================================================================
def _write_pibbfile(pibb_df: pl.DataFrame, rdate: str) -> None:
    """
    Equivalent of DATA _NULL_ SET PIBB FILE PIBBFILE PUT.
    PIBB has different N.A assignments and item labels compared to PBB.
    Items 4.03, 4.04 are excluded (deleted in SAS before output).
    """
    _PIBB_LINES = {
        1.0:    ('DEPOSITS ACCEPTED                       ', True),
        1.01:   ('DEMAND DEPOSITS ACCEPTED                ', True),
        1.02:   ('SAVINGS DEPOSITS ACCEPTED               ', True),
        1.03:   ('FIXED DEPOSITS ACCEPTED                 ', False),
        1.04:   ('SPECIFIC INVESTMENT ACCOUNTS ACCEPTED   ', False),
        1.05:   ('GENERAL INVESTMENT ACCOUNTS ACCEPTED    ', True),
        1.06:   ('COMMODITY MURABAHAH DEPOSITS ACCEPTED   ', True),
        1.07:   ('CALL DEPOSITS ACCEPTED                  ', False),
        1.08:   ('NEGOTIABLE INSTRUMENTS DEPOSITS ISSUED  ', True),
        1.09:   ('SPECIAL DEPOSITS ACCEPTED               ', False),
        1.10:   ('HOUSING DEVELOPMENT ACCOUNT DEPOSITS ACCEPTED                                ', False),
        1.11:   ('SHORT TERM DEPOSITS ACCEPTED            ', True),
        1.12:   ('INVESTMENT-LINKED TO DERIVATIVES OFFERED', False),
        1.13:   ('ELIGIBLE LIABILITIES EXEMPT DEPOSITS ACCEPTED                                ', False),
        1.14:   ('OTHER DEPOSITS ACCEPTED                 ', True),
        2.0:    ('REPURCHASE AGREEMENT                    ', False),
        3.0:    ('VOSTRO ACCOUNT BALANCES                 ', False),
        4.0:    ('INTERBANK BORROWINGS                    ', True),
        4.01:   ('INTERBANK BORROWING FOR ORIGINAL MATURITY EQUAL OR LESS THAN 1 YEAR      ', True),
        4.02:   ('INTERBANK BORROWING FOR ORIGINAL MATURITY MORE THAN 1 YEAR               ', True),
        999.0:  ('TOTAL                                   ', True),
    }

    with open(OUT_PIBBFILE, 'w', encoding='ascii', errors='replace') as f:
        f.write(f"REPORT ON DAILY EXTERNAL ACCOUNT BALANCES AS AT {rdate}\n")
        f.write(" ;DATA ITEM;PIBB\n")
        f.write(" ; ;RM\n")

        for row in pibb_df.iter_rows(named=True):
            item   = row['ITEM']
            amount = row['AMOUNT']
            if item not in _PIBB_LINES:
                continue
            desc, has_amount = _PIBB_LINES[item]
            amt_str = str(amount) if has_amount else 'N.A '
            item_str = ' ' if item == 999.0 else str(item)
            line = f"{item_str};{desc};{amt_str}"
            f.write(line + '\n')

    print(f"[PIBBFILE] Written: {OUT_PIBBFILE}")


# =============================================================================
# STEP 20: Write SFTP command scripts
# =============================================================================
def _write_sftp_scripts(fdate: str) -> None:
    """
    Equivalent of DATA _NULL_ FILE SFTP01/SFTP02 PUT statements.
    SFTP01: PUT commands for CTCS server (commented out in original JCL).
    SFTP02: PUT commands for DRR system.
    Both are plain text, LRECL=80 compatible.
    """
    # SFTP01 — CTCS server (originally commented out in JCL: //*RUNSFTP)
    with open(OUT_SFTP01, 'w') as f:
        f.write(f"PUT //SAP.PBB.XACB.DAILY.TEXT  daily_EAB_{fdate}.txt\n")
        f.write(f"PUT //SAP.PIBB.XACB.DAILY.TEXT daily_IEAB_{fdate}.txt\n")

    # SFTP02 — DRR system
    with open(OUT_SFTP02, 'w') as f:
        f.write(f'cd "FD-BNM REPORTING/PBB/BNM RPTG"\n')
        f.write(f"PUT //SAP.PBB.XACB.DAILY.TEXT  daily_EAB_{fdate}.txt\n")
        f.write(f'cd "/FD-BNM REPORTING/PIBB/BNM RPTG"\n')
        f.write(f"PUT //SAP.PIBB.XACB.DAILY.TEXT daily_IEAB_{fdate}.txt\n")

    print(f"[SFTP01] Written: {OUT_SFTP01}")
    print(f"[SFTP02] Written: {OUT_SFTP02}")


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    # -------------------------------------------------------------------------
    # Step 1: Derive reporting date and macro variables
    # -------------------------------------------------------------------------
    mv = _load_reptdate()
    reptdate     = mv['reptdate']
    reptdate_int = mv['reptdate_int']
    nowk         = mv['nowk']
    reptmon      = mv['reptmon']
    reptday      = mv['reptday']
    rdate        = mv['rdate']
    fdate        = mv['fdate']
    edate        = mv['edate']
    tdate_int    = mv['tdate']
    print(f"[REPTDATE] {reptdate}  WK={nowk}  MON={reptmon}  DAY={reptday}")

    # -------------------------------------------------------------------------
    # Step 2-13: Extract all segment datasets
    # (%INC PGM(PBBDPFMT) functions imported at module level from PBBDPFMT.py)
    # -------------------------------------------------------------------------
    print("[EXTRACT] CA ...")
    df_ca      = _extract_ca()
    print("[EXTRACT] FD ...")
    df_fd      = _extract_fd()
    print("[EXTRACT] UMA ...")
    df_uma     = _extract_uma()
    print("[EXTRACT] SA ...")
    df_sa      = _extract_sa()
    print("[EXTRACT] UMAHOE ...")
    df_umahoe  = _extract_umahoe()
    print("[EXTRACT] K3 ...")
    df_k3      = _extract_k3(reptmon, nowk)
    print("[EXTRACT] IK3 ...")
    df_ik3     = _extract_ik3(reptmon, nowk)
    print("[EXTRACT] K1 ...")
    df_k1      = _extract_k1(reptmon, nowk)
    print("[EXTRACT] IK1 ...")
    df_ik1     = _extract_ik1(reptmon, nowk)
    print("[EXTRACT] DCI ...")
    df_dci     = _extract_dci(reptday, tdate_int)
    print("[EXTRACT] RNID ...")
    df_rnid    = _extract_rnid(reptday)
    print("[EXTRACT] UTFX ...")
    df_utfx    = _extract_utfx(edate, tdate_int)

    # -------------------------------------------------------------------------
    # Step 14: Consolidate ALL → PROC SUMMARY
    # -------------------------------------------------------------------------
    all_df = pl.concat([
        df_ca, df_sa, df_fd, df_uma,
        df_k1, df_k3, df_dci,
        df_ik1, df_ik3, df_rnid,
        df_umahoe, df_utfx,
    ], how='vertical')

    alm  = _summarise(all_df, 'D')   # PBB  (conventional)
    ialm = _summarise(all_df, 'I')   # PIBB (Islamic)

    # -------------------------------------------------------------------------
    # Step 15-16: Build subtotal accumulator rows (CALC / ICALC)
    # -------------------------------------------------------------------------
    calc_df  = _build_calc(alm)
    icalc_df = _build_icalc(ialm)

    # -------------------------------------------------------------------------
    # Step 17: Assign ITEM numbers and merge with scaffold
    # DATA PBB = ALM + CALC with ITEM mapping
    # DATA PIBB = IALM + ICALC with ITEM mapping
    # -------------------------------------------------------------------------
    pbb_raw  = pl.concat([alm,  calc_df],  how='vertical')
    pibb_raw = pl.concat([ialm, icalc_df], how='vertical')

    pbb_items  = _assign_items(pbb_raw,  _PBB_ITEM_MAP)
    pibb_items = _assign_items(pibb_raw, _PIBB_ITEM_MAP)

    pbb_final  = _merge_with_scaffold(pbb_items,  _ALL_PBB_ITEMS,  exclude_items=None)
    pibb_final = _merge_with_scaffold(pibb_items, _ALL_PIBB_ITEMS,
                                      exclude_items={4.03, 4.04})

    # -------------------------------------------------------------------------
    # Step 18-19: Write PBBFILE and PIBBFILE
    # -------------------------------------------------------------------------
    _write_pbbfile(pbb_final,  rdate, fdate)
    _write_pibbfile(pibb_final, rdate)

    # -------------------------------------------------------------------------
    # Step 20: Write SFTP command scripts
    # -------------------------------------------------------------------------
    _write_sftp_scripts(fdate)

    # -------------------------------------------------------------------------
    # Note: PROC CPORT (XACBFTP) — SAS binary transport format for EDW.
    # This is a mainframe-only SAS utility and cannot be reproduced in Python.
    # The Parquet pipeline supersedes this delivery channel for EDW.
    # The XACBFTP dataset (SAP.XACB.DAILY.XACBFTP, LRECL=80) is not generated.
    # -------------------------------------------------------------------------
    print("[DONE] EIBDXACB completed.")


if __name__ == "__main__":
    main()
