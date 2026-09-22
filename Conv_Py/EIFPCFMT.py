#!/usr/bin/env python3
"""
Program : EIFPCFMT.py
Purpose : PROC FORMAT library (originally invoked via %INC PGM(EIFPCFMT), in
          the same family as PBBLNFMT / PBBELF / PBBDPFMT). Defines all
          BNM/MIS format, informat and picture-format lookups used by
          downstream reporting programs.

Dependency:
    This file has NO dependency on any other converted Python program - it
    is itself a leaf-level format-definition library (originally produced
    via PROC FORMAT CNTLOUT=PFMISFMT in SAS). Other programs are expected
    to do `from EIFPCFMT import <format_name>_format` for whichever formats
    they need.

Notes:
    - The original DATA _NULL_ step derives AGELIMIT/MAXAGE/AGEBELOW via
      CALL SYMPUT for later use inside VALUE AGEDESC; they are kept here as
      plain module-level constants and consumed directly by agedesc_format.
    - VALUE FDPROD here is LOCAL to EIFPCFMT (numeric code -> tenor label,
      e.g. 340 -> '1 MONTH') and is a COMPLETELY DIFFERENT value set from
      PBBDPFMT's FDPROD format (numeric plan -> BNMCODE, e.g. '42630').
      To avoid any accidental confusion/collision with PBBDPFMT.fdprod_format
      when both are imported side-by-side in a downstream program, this
      file's version is exposed as fdprod_term_format() instead of
      fdprod_format().
    - PROC FORMAT ranges (VALUE ... LOW-x, x-y, ...) are evaluated in the
      exact textual order they were written, first match wins - this
      matters at shared boundaries (e.g. the value 5 in SADPRG matches the
      first-listed "LOW-5" range, not the following "5-10" range). All
      range-based formats below preserve that first-match ordering via
      sequential if/elif chains.
    - SAS numeric missing (.) sorts as LOW, so a Python None input to any
      LOW-anchored range format is treated as falling into the first
      (LOW-...) range, matching SAS behaviour.
"""

# ============================================================================
# DATA _NULL_;  CALL SYMPUT('AGELIMIT',12); CALL SYMPUT('MAXAGE',18);
#               CALL SYMPUT('AGEBELOW',11); RUN;
# ============================================================================
AGELIMIT = 12
MAXAGE   = 18
AGEBELOW = 11


# ============================================================================
# VALUE BRCHCD  (branch code -> 3-letter branch mnemonic)
# Each entry maps TWO source codes (i and 3000+i) to the same label.
# ============================================================================
_BRCHCD_LABELS = [
    "KLM", "PMO", "MLK", "IMO", "TPG", "JBU", "JPU", "JSI", "KLC", "JBA",
    "SST", "BTW", "KKU", "SRB", "JPR", "ASR", "KLG", "KPR", "BGH", "KLS",
    "KPH", "TRI", "SGM", "SMG", "PKG", "MUA", "KTN", "STP", "KCG", "SKC",
    "LBN", "MSG", "KBH", "PJN", "RWG", "KPG", "TMH", "PDN", "KDN", "DJA",
    "APG", "TWU", "SJA", "MTK", "BMM", "TIN", "PKL", "KGR", "KJG", "KTU",
    "JKL", "BPR", "EDU", "KHG", "TKK", "SPI", "BBU", "SKN", "CKI", "TPN",
    "SBU", "BDR", "AIM", "YPG", "KKI", "BTG", "SDN", "MRI", "KKB", "KTI",
    "KLI", "JTA", "SRM", "SRR", "KKR", "PJA", "BEN", "LDU", "BPT", "KAP",
    "PRI", "GMG", "TST", "PLH", "SBP", "KLA", "TDI", "CAH", "JSN", "RAU",
    "SBR", "LLG", "JLP", "TSP", "BSI", "JMH", "PBR", "MLB", "BTL", "SAM",
    "KBU", "KBS", "SAN", "SJY", "JJG", "TML", "JIH", "BBB", "INN", "SBM",
    "SEA", "PJO", "IGN", "BBI", "WMU", "GRT", "SGB", "UYB", "KMY", "BTA",
    "KRK", "TRJ", "SLY", "NLI", "SSA", "SPG", "SBH", "SSH", "TIH", "RTU",
    "TJJ", "PCG", "SNI", "JRT", "SGK", "STL", "JTS", "JKA", "JDK", "SAB",
    "SDI", "TPI", "TCT", "NTL", "MSI", "CCE", "KJA", "JLT", "BDA", "BBM",
    "AST", "TMA", "USJ", "TMI", "TMK", "DUA", "JSB", "PIH", "SS2", "TSJ",
    "TCL", "TEA", "SFN", "JTT", "JBH", "BAM", "JPA", "STW",
]

_BRCHCD_MAP = {}
for _i, _label in enumerate(_BRCHCD_LABELS, start=1):
    _BRCHCD_MAP[_i] = _label
    _BRCHCD_MAP[3000 + _i] = _label


def brchcd_format(code):
    """VALUE BRCHCD. No OTHER clause in the original -> unmatched codes
    return None (SAS would print the raw numeric value)."""
    return _BRCHCD_MAP.get(code)


# ============================================================================
# VALUE SAPROD  (savings product code -> 6-char product bucket)
# ============================================================================
_SAPROD_MAP = {
    200: "SD    ",  # NORMAL SAVINGS
    201: "SD    ",  # STAFF SAVINGS
    202: "SD    ",  # YOUNG ACHIEVER ACCOUNT
    203: "SD    ",  # 50 PLUS SAVINGS
    204: "SPTFSD",  # ISLAMIC BANKING AL-WADIAH
    205: "SD    ",  # KIDDIE CLUB SAVINGS
    212: "SD    ",  # WISE ACCOUNT
    213: "SD    ",  # PB SAVELINK ACCOUNT
    220: "SD    ",  # BOON SIEW SAVINGS
}


def saprod_format(product):
    return _SAPROD_MAP.get(product, "SD    ")  # OTHER = 'SD    '


# ============================================================================
# VALUE SADPRG  (savings deposit range bucket, first-match range order)
# ============================================================================
def sadprg_format(value):
    if value is None or value <= 5:
        return "01)$       1.00 - $         5.00"
    if value <= 10:
        return "02)$       5.01 - $        10.00"
    if value <= 50:
        return "03)$      10.01 - $        50.00"
    if value <= 100:
        return "04)$      50.01 - $       100.00"
    if value <= 500:
        return "05)$     100.01 - $       500.00"
    if value <= 1000:
        return "06)$     500.01 - $     1,000.00"
    if value <= 1500:
        return "07)$   1,000.01 - $     1,500.00"
    if value <= 2000:
        return "08)$   1,500.01 - $     2,000.00"
    if value <= 2500:
        return "09)$   2,000.01 - $     2,500.00"
    if value <= 3000:
        return "10)$   2,500.01 - $     3,000.00"
    if value <= 3500:
        return "11)$   3,000.01 - $     3,500.00"
    if value <= 4000:
        return "12)$   3,500.01 - $     4,000.00"
    if value <= 4500:
        return "13)$   4,000.01 - $     4,500.00"
    if value <= 5000:
        return "14)$   4,500.01 - $     5,000.00"
    if value <= 6000:
        return "15)$   5,000.01 - $     6,000.00"
    if value <= 7000:
        return "16)$   6,000.01 - $     7,000.00"
    if value <= 8000:
        return "17)$   7,000.01 - $     8,000.00"
    if value <= 9000:
        return "18)$   8,000.01 - $     9,000.00"
    if value <= 10000:
        return "19)$   9,000.01 - $    10,000.00"
    if value <= 15000:
        return "20)$  10,000.01 - $    15,000.00"
    if value <= 20000:
        return "21)$  15,000.01 - $    20,000.00"
    if value <= 25000:
        return "22)$  20,000.01 - $    25,000.00"
    if value <= 30000:
        return "23)$  25,000.01 - $    30,000.00"
    if value <= 35000:
        return "24)$  30,000.01 - $    35,000.00"
    if value <= 40000:
        return "25)$  35,000.01 - $    40,000.00"
    if value <= 45000:
        return "26)$  40,000.01 - $    45,000.00"
    if value <= 50000:
        return "27)$  45,000.01 - $    50,000.00"
    if value <= 55000:
        return "28)$  50,000.01 - $    55,000.00"
    if value <= 60000:
        return "29)$  55,000.01 - $    60,000.00"
    if value <= 65000:
        return "30)$  60,000.01 - $    65,000.00"
    if value <= 70000:
        return "31)$  65,000.01 - $    70,000.00"
    if value <= 75000:
        return "32)$  70,000.01 - $    75,000.00"
    if value <= 80000:
        return "33)$  75,000.01 - $    80,000.00"
    if value <= 85000:
        return "34)$  80,000.01 - $    85,000.00"
    if value <= 90000:
        return "35)$  85,000.01 - $    90,000.00"
    if value <= 95000:
        return "36)$  90,000.01 - $    95,000.00"
    if value <= 100000:
        return "37)$  95,000.01 - $   100,000.00"
    if value <= 150000:
        return "38)$ 100,000.01 - $   150,000.00"
    if value <= 200000:
        return "39)$ 150,000.01 - $   200,000.00"
    if value <= 300000:
        return "40)$ 200,000.01 - $   300,000.00"
    if value <= 500000:
        return "41)$ 300,000.01 - $   500,000.00"
    if value <= 1000000:
        return "42)$ 500,000.01 - $ 1,000,000.00"
    return "43)ABOVE  $ 1,000,000.00"


# ============================================================================
# VALUE PROFNORM
# ============================================================================
def profnorm_format(value):
    if value is None or value <= 5000:
        return "1)UP TO RM 5,000.00"
    if value <= 10000:
        return "2)UP TO RM10,000.00"
    if value <= 30000:
        return "3)UP TO RM30,000.00"
    if value <= 50000:
        return "4)UP TO RM50,000.00"
    if value <= 75000:
        return "5)UP TO RM75,000.00"
    return "6)ABOVE RM75,000.00"


# ============================================================================
# VALUE SEXNORM
# ============================================================================
def sexnorm_format(value):
    if value is None or value <= 12:
        return "1)BELOW 12 YRS"
    if value <= 18:
        return "2)12 - 18 YRS"
    if value <= 50:
        return "3)18 - 50 YRS"
    return "4)50 YRS AND ABOVE"


# ============================================================================
# VALUE PROFYAA
# ============================================================================
def profyaa_format(value):
    if value is None or value <= 500:
        return "1)BELOW RM   500.00"
    if value <= 2000:
        return "2)UP TO RM 2,000.00"
    if value <= 5000:
        return "3)UP TO RM 5,000.00"
    if value <= 10000:
        return "4)UP TO RM10,000.00"
    if value <= 30000:
        return "5)UP TO RM30,000.00"
    if value <= 50000:
        return "6)UP TO RM50,000.00"
    if value <= 75000:
        return "7)UP TO RM75,000.00"
    return "8)ABOVE RM75,000.00"


# ============================================================================
# VALUE SEXYW  (no catch-all above 18 in the original -> None beyond range)
# ============================================================================
def sexyw_format(value):
    if value is None or value <= 12:
        return "1)BELOW 12 YRS"
    if value <= 18:
        return "2)12 - 18 YRS"
    return None


# ============================================================================
# VALUE PROFPLUS
# Note: LOW-2000 and 2000-5000 both resolve to a label starting '1)' in the
# original SAS source (a duplicated label, not a typo introduced here) --
# preserved exactly as written.
# ============================================================================
def profplus_format(value):
    if value is None or value <= 2000:
        return "1)UP TO RM 2,000.00"
    if value <= 5000:
        return "1)UP TO RM 5,000.00"
    if value <= 10000:
        return "2)UP TO RM10,000.00"
    if value <= 30000:
        return "3)UP TO RM30,000.00"
    if value <= 50000:
        return "4)UP TO RM50,000.00"
    if value <= 75000:
        return "5)UP TO RM75,000.00"
    return "6)ABOVE RM75,000.00"


# ============================================================================
# VALUE PROFWISE
# ============================================================================
def profwise_format(value):
    if value is None or value <= 5000:
        return "1)UP TO RM 5,000.00"
    if value <= 10000:
        return "2)UP TO RM10,000.00"
    if value <= 20000:
        return "3)UP TO RM20,000.00"
    if value <= 30000:
        return "4)UP TO RM30,000.00"
    if value <= 50000:
        return "5)UP TO RM50,000.00"
    return "6)ABOVE RM50,000.00"


# ============================================================================
# VALUE SDNAME
# ============================================================================
_SDNAME_MAP = {
    200: "NORMAL SAVINGS",
    201: "STAFF SAVINGS",
    202: "YOUNG ACHIEVER",
    203: "50 PLUS SAVINGS",
    204: "ISLAMIC SAVINGS",
    205: "KIDDIE CLUB",
    212: "WISE ACCOUNT",
    213: "PB SAVELINK",
    220: "BOON SIEW SAVINGS",
}


def sdname_format(product):
    """No OTHER clause -> unmatched codes return None."""
    return _SDNAME_MAP.get(product)


# ============================================================================
# VALUE $RACE
# ============================================================================
_RACE_MAP = {
    "0": "OTHERS",
    "1": "MALAY",
    "2": "CHINESE",
    "3": "INDIAN",
}


def race_format(code):
    return _RACE_MAP.get(code, "OTHERS")  # OTHER = 'OTHERS'


# ============================================================================
# VALUE CADPRG  (discrete/multi-value exact-match, NOT a range format)
# ============================================================================
_CADPRG_MAP = {
    1000: " 1)         RM0 -     RM1,000",
    2000: " 2)     RM1,001 -     RM2,000",
    2500: " 3)     RM2,001 -     RM2,500",
    3000: " 4)     RM2,501 -     RM5,000",
    5000: " 4)     RM2,501 -     RM5,000",
    10000: " 5)     RM5,001 -    RM10,000",
    20000: " 6)    RM10,001 -    RM20,000",
    30000: " 7)    RM20,001 -    RM30,000",
    40000: " 8)    RM30,001 -    RM40,000",
    50000: " 9)    RM40,001 -    RM50,000",
    75000: "10)   RM50,001 -   RM100,000",
    100000: "10)   RM50,001 -   RM100,000",
    150000: "11)  RM100,001 -   RM150,000",
    200000: "12)  RM150,001 -   RM200,000",
    250000: "13)  RM200,001 -   RM250,000",
    500000: "14)  RM250,001 -   RM500,000",
    1000000: "15)  RM500,001 - RM1,000,000",
    2000000: "16)RM1,000,001 - RM2,000,000",
    3000000: "17)RM2,000,001 - RM3,000,000",
    4000000: "18)RM3,000,001 - RM4,000,000",
    5000000: "19)RM4,000,001 - RM5,000,000",
}


def cadprg_format(value):
    return _CADPRG_MAP.get(value, "20)RM5,000,001 AND ABOVE")  # OTHER


# ============================================================================
# VALUE ICADPRG  (discrete/multi-value exact-match, NOT a range format)
# ============================================================================
_ICADPRG_MAP = {
    1000: " 1)     BELOW  RM2,000",
    2000: " 1)     BELOW  RM2,000",
    2500: " 2) RM2,000 -  RM3,000",
    3000: " 2) RM2,000 -  RM3,000",
    5000: " 3) RM3,000 -  RM5,000",
    10000: " 4) RM5,000 -  RM10,000",
    20000: " 5) RM10,000 - RM30,000",
    30000: " 5) RM10,000 - RM30,000",
    40000: " 6) RM30,000 - RM50,000",
    50000: " 6) RM30,000 - RM50,000",
    75000: " 7) RM50,000 - RM75,000",
    100000: " 8) RM75,000 - RM100,000",
    150000: " 9) RM100,000 -  RM150,000",
    200000: "10) RM150,000 -  RM200,000",
}


def icadprg_format(value):
    return _ICADPRG_MAP.get(value, "11) RM200,000 AND ABOVE")  # OTHER


# ============================================================================
# VALUE $PURPOSE
# ============================================================================
_PURPOSE_MAP = {
    "1": "PERSONAL",
    "2": "JOINT",
    "4": "PERSONAL",  # IS A STAFF A/C BUT STATES AS PERSONAL
}


def purpose_format(code):
    """No OTHER clause -> unmatched codes return None."""
    return _PURPOSE_MAP.get(code)


# ============================================================================
# VALUE CARANGED  (discrete/multi-value exact-match, NOT a range format;
# no OTHER clause -> unmatched values return None)
# ============================================================================
_CARANGED_MAP = {
    2000: " 1)         BELOW RM2,000",
    3000: " 2)     RM2,000 - RM3,000",
    5000: " 3)     RM3,000 - RM5,000",
    10000: " 4)    RM5,000 - RM10,000",
    30000: " 5)   RM10,000 - RM30,000",
    50000: " 6)   RM30,000 - RM50,000",
    75000: " 7)   RM50,000 - RM75,000",
    100000: " 8)  RM75,000 - RM100,000",
    150000: " 9) RM100,000 - RM150,000",
    200000: "10) RM150,000 - RM200,000",
    200001: "11) RM200,000 AND ABOVE  ",
}


def caranged_format(value):
    return _CARANGED_MAP.get(value)


# ============================================================================
# VALUE PROD  (no OTHER clause -> unmatched values return None)
# ============================================================================
_PROD_MAP = {
    150: "ACE ACCOUNT",
    160: "AL-WADIAH CURRENT A/C",
}


def prod_format(product):
    return _PROD_MAP.get(product)


# ============================================================================
# VALUE AGEDESC  (uses SYMPUT'd macro vars AGEBELOW/AGELIMIT/MAXAGE;
# no OTHER clause -> unmatched values return None)
# ============================================================================
_AGEDESC_MAP = {
    0: "WITHOUT BIRTHDATE",
    AGEBELOW: "BELOW 12 YEARS",
    AGELIMIT: "12 TO BELOW 18 YEARS",
    MAXAGE: "18 AND ABOVE",
}


def agedesc_format(age):
    return _AGEDESC_MAP.get(age)


# ============================================================================
# VALUE $STATE
# ============================================================================
_STATE_MAP = {
    "A": "PERAK",
    "B": "SELANGOR",
    "C": "PAHANG",
    "D": "KELANTAN",
    "J": "JOHOR",
    "K": "KEDAH",
    "L": "LABUAN",
    "M": "MELAKA",
    "N": "NEGERI SEMBILAN",
    "P": "PULAU PINANG",
    "Q": "SARAWAK",
    "R": "PERLIS",
    "S": "SABAH",
    "T": "TERENGGANU",
    "W": "WILAYAH PERSEKUTUAN",
}


def state_format(code):
    """No OTHER clause -> unmatched codes return None."""
    return _STATE_MAP.get(code)


# ============================================================================
# VALUE FDPROD  (LOCAL to EIFPCFMT: numeric tenor code -> tenor label).
# See module docstring - this is a DIFFERENT value set from PBBDPFMT's
# FDPROD format and is intentionally exposed under a distinct name here.
# ============================================================================
_FDPROD_TERM_MAP = {
    340: "1 MONTH",
    341: "3 MONTHS",
    342: "6 MONTHS",
    343: "9 MONTHS",
    344: "12 MONTHS",
    345: "15 MONTHS",
    346: "18 MONTHS",
    347: "21 MONTHS",
    348: "24 MONTHS",
    349: "36 MONTHS",
    350: "48 MONTHS",
    351: "60 MONTHS",
}


def fdprod_term_format(plan):
    return _FDPROD_TERM_MAP.get(plan, " ")  # OTHER = ' '


# ============================================================================
# INVALUE CARANGE  (informat: raw numeric -> bucketed numeric code,
# first-match range order, same LOW-anchored / HIGH-anchored semantics
# as the VALUE range formats above)
# ============================================================================
def carange_invalue(value):
    if value is None or value <= 2000:
        return 2000
    if value <= 3000:
        return 3000
    if value <= 5000:
        return 5000
    if value <= 10000:
        return 10000
    if value <= 30000:
        return 30000
    if value <= 50000:
        return 50000
    if value <= 75000:
        return 75000
    if value <= 100000:
        return 100000
    if value <= 150000:
        return 150000
    if value <= 200000:
        return 200000
    return 200001


# ============================================================================
# PICTURE HUNDRED / THOUSAND / MILLION
# Each PICTURE has a LOW-<0 branch (PREFIX='-') and a 0-HIGH branch; both
# branches share the same digit template and MULT, so the sign is simply
# applied to the formatted magnitude here rather than duplicated per-branch.
# ============================================================================
def _format_picture(value: float, mult: float, int_digits: int, dec_digits: int) -> str:
    """Renders `value` under a SAS PICTURE template of `int_digits` leading
    zero-padded, comma-grouped (groups of 3) integer digits, optionally
    followed by `dec_digits` decimal digits, after scaling by MULT and
    prefixing '-' for negative source values (PREFIX='-')."""
    is_negative = value < 0
    scaled = abs(value) * mult
    total_digits = int_digits + dec_digits
    scaled_units = round(scaled * (10 ** dec_digits))
    digits = str(int(scaled_units)).zfill(total_digits)

    if dec_digits > 0:
        int_part, dec_part = digits[:-dec_digits], digits[-dec_digits:]
    else:
        int_part, dec_part = digits, ""

    groups = []
    remaining = int_part
    while len(remaining) > 3:
        groups.insert(0, remaining[-3:])
        remaining = remaining[:-3]
    groups.insert(0, remaining)
    formatted_int = ",".join(groups)

    result = formatted_int + ("." + dec_part if dec_part else "")
    return ("-" + result) if is_negative else result


def hundred_format(value: float) -> str:
    """PICTURE HUNDRED: '0,000,009.99' (MULT=1)."""
    return _format_picture(value, mult=1, int_digits=7, dec_digits=2)


def thousand_format(value: float) -> str:
    """PICTURE THOUSAND: '0,000,000,009' (MULT=.001)."""
    return _format_picture(value, mult=0.001, int_digits=10, dec_digits=0)


def million_format(value: float) -> str:
    """PICTURE MILLION: '0,000,000,009' (MULT=.000001)."""
    return _format_picture(value, mult=0.000001, int_digits=10, dec_digits=0)
