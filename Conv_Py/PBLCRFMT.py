"""
Program : PBLCRFMT.py
Purpose : Liquidity Coverage Ratio (LCR) Mapping - Format Definitions
          Python equivalent of SAS PROC FORMAT library PBLCRFMT.
          Provides lookup dictionaries and helper functions replicating
          each SAS user-defined format (character value formats and
          numeric range formats) for use by downstream conversion
          programs.
"""

from typing import Optional, Union


# =====================================================================
# $BNMCD  - REF ONLY - "TAG" FIELD
# =====================================================================
BNMCD_FMT = {
    '01': 'TRANSACTIONAL ACCOUNTS (INSURED)',
    '02': 'NON-TRX WITH RELATIONSHIP (INSURED)',
    '03': 'NON-TRX WITH NON-RELATIONSHIP (INSURED)',
    '10': 'UNINSURED DEPOSITS',
    '20': 'QUALIFYING TERM DEPOSITS',
}


def bnmcd_fmt(code: Optional[str]) -> str:
    """VALUE $BNMCD - REF ONLY - "TAG" FIELD. No OTHER clause in SAS source;
    unmatched codes return empty string (SAS would print the raw value)."""
    if code is None:
        return ''
    return BNMCD_FMT.get(code, code)


# =====================================================================
# $LCRCDEQU
# =====================================================================
LCRCDEQU_FMT = {
    '08': 'A1.40',
    '19': 'B1.40',
    '29': 'B3.12',
    '39': 'B3.22',
    '49': 'B3.30',
    '59': 'B3.40',
}
LCRCDEQU_OTHER = '     '


def lcrcdequ_fmt(code: Optional[str]) -> str:
    """VALUE $LCRCDEQU"""
    if code is None:
        return LCRCDEQU_OTHER
    return LCRCDEQU_FMT.get(code, LCRCDEQU_OTHER)


# =====================================================================
# $LCRCDMNIOPR  (OPERATIONAL)
# =====================================================================
LCRCDMNIOPR_FMT = {
    '2902': 'B2.11',
    '2910': 'B2.12',
    '3902': 'B2.21',
    '3910': 'B2.22',
    '4902': 'B2.31',
    '4910': 'B2.32',
    '5902': 'B2.41',
    '5910': 'B2.42',
}
LCRCDMNIOPR_OTHER = '     '


def lcrcdmniopr_fmt(code: Optional[str]) -> str:
    """VALUE $LCRCDMNIOPR /*OPERATIONAL*/"""
    if code is None:
        return LCRCDMNIOPR_OTHER
    return LCRCDMNIOPR_FMT.get(code, LCRCDMNIOPR_OTHER)


# =====================================================================
# $LCRCDMNI  (NON-OPERATIONAL)
# =====================================================================
LCRCDMNI_FMT = {
    '0801': 'A1.13',
    '0802': 'A1.23',
    '0803': 'A1.31',
    '0810': 'A1.40',
    '0820': 'A1.50',
    '1901': 'B1.13',
    '1902': 'B1.23',
    '1903': 'B1.31',
    '1910': 'B1.40',
    '1920': 'B1.50',
    '2902': 'B3.11',
    '2903': 'B3.11',
    '2910': 'B3.12',
    '2920': 'B6.10',
    '3902': 'B3.21',
    '3903': 'B3.21',
    '3910': 'B3.22',
    '3920': 'B6.20',
    '4902': 'B3.30',
    '4903': 'B3.30',
    '4910': 'B3.30',
    '4920': 'B6.30',
    '5902': 'B3.40',
    '5910': 'B3.40',
    '5920': 'B6.40',
}
LCRCDMNI_OTHER = '     '


def lcrcdmni_fmt(code: Optional[str]) -> str:
    """VALUE $LCRCDMNI /*NON-OPERATIONAL*/"""
    if code is None:
        return LCRCDMNI_OTHER
    return LCRCDMNI_FMT.get(code, LCRCDMNI_OTHER)


# =====================================================================
# $LCRCDGL  (LCRMTH-MAIN)
# =====================================================================
LCRCDGL_FMT = {
    'F143110VCB': 'B3.30',    # 2015-3390/2017-2497
    # 'F142699OPE': 'B2.32',  # 2015-3390
    'F143620OPE': 'B2.32',    # 2017-2497
    'F142599OELED': 'B3.22',
    'F142199E': 'B3.30',
    'F142600FBI': 'B3.30',
    'F142699C': 'B3.30',
    'F142699D': 'B3.30',
    'F143130': 'B3.30',
    'F143110VFBI': 'B3.30',   # 2015-3390
    'F143620FNFBI': 'B3.30',  # 2015-3390
    'F143620USDOP': 'B2.32',  # 2017-2497
    'F143620SGDOP': 'B2.32',  # 2017-2497
    'F143620HKDOP': 'B2.32',  # 2017-2497
}
LCRCDGL_OTHER = '     '


def lcrcdgl_fmt(code: Optional[str]) -> str:
    """VALUE $LCRCDGL /* LCRMTH-MAIN */"""
    if code is None:
        return LCRCDGL_OTHER
    return LCRCDGL_FMT.get(code, LCRCDGL_OTHER)


# =====================================================================
# $LCRCDGLOTH  (LCRUSD-LCRSGD-LCRMYR-EXLC4LCRMTH-MAIN)
# =====================================================================
LCRCDGLOTH_FMT = {
    '42699USD': 'B3.30',
    '42699SGD': 'B3.30',
    '42699HKD': 'B3.30',
    'F143620USD': 'B3.30',
    'F143620SGD': 'B3.30',
    'F143620HKD': 'B3.30',
    'F143620USDOP': 'B2.32',  # 2017-2497
    'F143620SGDOP': 'B2.32',  # 2017-2497
    'F143620HKDOP': 'B2.32',  # 2017-2497
}
LCRCDGLOTH_OTHER = '     '


def lcrcdgloth_fmt(code: Optional[str]) -> str:
    """VALUE $LCRCDGLOTH /* LCRUSD-LCRSGD-LCRMYR-EXLC4LCRMTH-MAIN */"""
    if code is None:
        return LCRCDGLOTH_OTHER
    return LCRCDGLOTH_FMT.get(code, LCRCDGLOTH_OTHER)


# =====================================================================
# $LCRCDGLCCY
# =====================================================================
LCRCDGLCCY_FMT = {
    'F142599OELED': 'MYR',
    'F142199E': 'MYR',
    'F143130': 'MYR',
    'F143110VFBI': 'MYR',
    'F143110VCB': 'MYR',
    '42699USD': 'USD',
    'F143620USDOP': 'USD',  # 2017-2497
    '42699SGD': 'SGD',
    'F143620SGDOP': 'SGD',  # 2017-2497
    '42699HKD': 'HKD',
    'F143620HKDOP': 'HKD',  # 2017-2497
    'F143620USD': 'USD',
    'F143620SGD': 'SGD',
    'F143620HKD': 'HKD',
}
LCRCDGLCCY_OTHER = '   '


def lcrcdglccy_fmt(code: Optional[str]) -> str:
    """VALUE $LCRCDGLCCY"""
    if code is None:
        return LCRCDGLCCY_OTHER
    return LCRCDGLCCY_FMT.get(code, LCRCDGLCCY_OTHER)


# =====================================================================
# $LCRCDIGL
# =====================================================================
LCRCDIGL_FMT = {
    'F143120ODNCB': 'B3.30',  # 2017-2497
    'F143120ODNIB': 'B3.30',  # 2017-2497
    'F143130': 'B3.30',
    'F143620FNFBI': 'B3.30',
}
LCRCDIGL_OTHER = '     '


def lcrcdigl_fmt(code: Optional[str]) -> str:
    """VALUE $LCRCDIGL"""
    if code is None:
        return LCRCDIGL_OTHER
    return LCRCDIGL_FMT.get(code, LCRCDIGL_OTHER)


# =====================================================================
# $LCRCDIGLCCY
# =====================================================================
LCRCDIGLCCY_FMT = {
    'F143130': 'MYR',
    'F143120ODNCB': 'MYR',
    'F143120ODNIB': 'MYR',
    'F143620USD': 'USD',
}
LCRCDIGLCCY_OTHER = '   '


def lcrcdiglccy_fmt(code: Optional[str]) -> str:
    """VALUE $LCRCDIGLCCY"""
    if code is None:
        return LCRCDIGLCCY_OTHER
    return LCRCDIGLCCY_FMT.get(code, LCRCDIGLCCY_OTHER)


# =====================================================================
# $COLID
# =====================================================================
COLID_FMT = {
    '95311': 'FD95311RM ',
    '96311': 'FD96311FX ',
    '95312': 'SA95312RM ',
    '95313': 'CA95313RM ',
    '96313': 'CA96313FX ',
    '9531X': 'GLD9531X  ',
    '95315': 'FD95315RM ',
    '95317': 'FD95317RM ',
    '95830': 'STD95830V ',
    '9583X': 'STD95830Q ',
    '95840': 'NID95840  ',
    '95810': 'IBB9X810  ',
    '96810': 'IBB9X810  ',
    '95329': 'DCI9X329  ',
    '96329': 'DCI9X329  ',
    '95820': 'IBR95820  ',
    '95850': 'BAP95850  ',
}
COLID_OTHER = '          '


def colid_fmt(code: Optional[str]) -> str:
    """VALUE $COLID"""
    if code is None:
        return COLID_OTHER
    return COLID_FMT.get(code, COLID_OTHER)


# =====================================================================
# REMFMT  - numeric range format
#   LOW-1   = '01'   UP TO 1 MTH
#   1-3     = '02'   >1 MTH - 3 MTHS
#   3-6     = '03'   >3 - 6 MTHS
#   6-9     = '04'   >6 MTHS - 1 YR
#   9-12    = '05'   >6 MTHS - 1 YR
#   OTHER   = '06'   > 1 YEAR
# =====================================================================
def remfmt(value: Optional[Union[int, float]]) -> str:
    """VALUE REMFMT - numeric range format.
    SAS range bounds are inclusive on both ends unless '<' is used;
    ranges are evaluated in the order defined (first match wins)."""
    if value is None:
        return '06'
    if value <= 1:
        return '01'
    if value <= 3:
        return '02'
    if value <= 6:
        return '03'
    if value <= 9:
        return '04'
    if value <= 12:
        return '05'
    return '06'


# =====================================================================
# CMMFMT  - numeric range format
#   LOW-0.1 = '01'   UP TO 1 WK
#   0.1-1   = '02'   >1 WK - 1 MTH
#   1-3     = '03'   >1 MTH - 3 MTHS
#   3-6     = '04'   >3 - 6 MTHS
#   6-12    = '05'   >6 MTHS - 1 YR
#   OTHER   = '06'   > 1 YEAR
# =====================================================================
def cmmfmt(value: Optional[Union[int, float]]) -> str:
    """VALUE CMMFMT - numeric range format."""
    if value is None:
        return '06'
    if value <= 0.1:
        return '01'
    if value <= 1:
        return '02'
    if value <= 3:
        return '03'
    if value <= 6:
        return '04'
    if value <= 12:
        return '05'
    return '06'


# =====================================================================
# REMFMX  - numeric range format
#   LOW-<6  = '01'   < 6 MONTHS
#   6-<12   = '02'   >= 6 MONTHS TO < 1 YEAR
#   OTHER   = '03'   >= 1 YEAR
# =====================================================================
def remfmx(value: Optional[Union[int, float]]) -> str:
    """VALUE REMFMX - numeric range format (exclusive upper bounds)."""
    if value is None:
        return '03'
    if value < 6:
        return '01'
    if value < 12:
        return '02'
    return '03'
