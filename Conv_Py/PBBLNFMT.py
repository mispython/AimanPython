#!/usr/bin/env python3
"""
Program  : PBBLNFMT (Format Definitions)
Purpose  : To define format mappings for loan processing
           Equivalent to SAS PROC FORMAT definitions used throughout
           the loan and deposit processing system.
           Designed to be imported by orchestrator programs (%INC equivalent).

TAKE NOTE : ANY AMENDMENTS TO PRODUCTS MAPPING, THESE FORMATS
ARE AFFECTED : LNDENOM, LNPROD, LNTYPE & LNRATE (LOANS)
               ODPROD, ODDENOM & ODRATE
2006-1199  NPH  EXT NEW LOAN PRODUCT 532.
2008-1090  MAA  EXT NEW LOAN PRODUCT 533.
2010-2715  AAB  EXT NEW LOAN PRODUCT 512.
2011-2706  SKP  EXT NEW OD   PRODUCT 190.
2011-3893  SKP  EXT WOF OD   PRODUCT 32 & 33.
2011-3853  SKP  EXT WOF LOAN PRODUCT 650-679
2012-1088  CWK  EXT NEW OD   PRODUCT 133,134
2012-3170  MFM  EXT NEW LOAN PRODUCT 409 & 410
2012-3494  MFM  EXT NEW LOAN PRODUCT 411,187 & 687
2013-502   MFM  EXT NEW OD   PRODUCT 184
2013-1098  MFM  EXT NEW LN   PRODUCT 639 & 912
2013-758   CWK  EXT NEW LOAN PRODUCT 412-414
2013-414   CWK  EXT NEW LOAN PRODUCT 102-108
2013-1313  MFM  EXT NEW LOAN PRODUCT 461
2013-2577  RST  EXT NEW LOAN PRODUCT 462 & 463
2014-632   RST  EXT NEW LOAN PRODUCT 188,189,190,688,689
2014-885   SKP      NEW LOAN PRODUCT 249,349
2014-2252  TBC      NEW LOAN PRODUCT 191,417
                    INACTIVE LOAN PRODUCT 690,670
2015-606   TBC      NEW LOAN PRODUCT 307
2015-1190  TBC      NEW OD PRODUCT 185,186
2015-1043  SKP      NEW LOAN PRODUCT 144
2015-1044  TBC      NEW LOAN PRODUCT 468
2015-1368  TBC      EXCLUDE LOAN PRODUCT 698,699
2015-2284  TBC      NEW LOAN PRODUCT 419,420,469,470
                    INACTIVE LOAN PRODUCT 672,673,674,675
2016-630   NSA      NEW LOAN PRODUCT 422
2016-1409  TBC      EXCLUDE LOAN 973
2016-1407  TBC      ExCLUDE LOAN 972
2016-579   TBC      NEW OD PRODUCT
2016-678   NSA      NEW LOAN PRODUCT 918
2016-2771  CWK      NEW CA PRODUCT 92
2016-2203  NFJ      NEW LOAN PRODUCT 429,430
2017-65    CWK      NEW OD PRODUCT 77,78
2016-4701  MFM      NEW LOAN PRODUCT 440,441,442,443
2017-4244  TBC      UPDATE $SECTOR TO NEW SECTFISS
2019-816   IFA      EXCLUDE LOAN PRODUCT 972 & 973
"""

from typing import Dict, Any, List, Optional

# ============================================================================
# CONFIGURATION / PATH SETUP
# ============================================================================

# This module provides format mapping functions equivalent to SAS PROC FORMAT.
# Import this module in orchestrator programs instead of %INC PBBLNFMT.

# ============================================================================
# PRODUCT LIST MACROS (equivalent to SAS %LET declarations)
# ============================================================================

# MORE PLAN LOAN products
MOREPLAN_PRODUCTS = (116, 119, 234, 235, 236, 242)
# MORE PLAN LOAN ISLAMIC products
MOREISLM_PRODUCTS = (116, 119)

MORE_PLAN   = [116, 119, 234, 235, 236, 242]          # MORE PLAN LOAN
MORE_ISLAM  = [116, 119]                               # MORE PLAN LOAN ISLAMIC
HP_ALL      = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725,
               983, 993, 996, 678, 679, 698, 699]      # HP - ALL PRODUCTS
HP_ACTIVE   = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]  # HP - WITHOUT WOFF/WDOWN
AITAB       = [128, 130, 131, 132]                     # HP - AITAB
HOME_ISLAMIC      = [113, 115, 117, 118]               # HOME PLAN ISLAMIC
HOME_CONVENTIONAL = [227, 228, 230, 231, 237, 238, 239, 240, 241]  # HOME PLAN CONVENTIONAL
SWIFT_ISLAMIC     = [126, 127]                         # ABBA SWIFT PLAN
SWIFT_CONVENTIONAL = [359]                             # SWIFT PLAN CONVENTIONAL
FCY_PRODUCTS = [800, 801, 802, 803, 804, 805, 806, 807, 808, 816, 817,
                809, 810, 811, 812, 813, 814, 815, 851, 852, 853, 854,
                855, 856, 857, 858, 859, 860]          # FCY PRODUCT

# Country codes (ISO 2-letter codes)
COUNTRY_CODES = [
    'AF', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG', 'AR',
    'AM', 'AW', 'AU', 'AT', 'AZ', 'BS', 'BH', 'BD', 'BB', 'BY',
    'BE', 'BZ', 'BJ', 'BM', 'BT', 'BO', 'BA', 'BW', 'BV', 'BR',
    'IO', 'BN', 'BG', 'BF', 'BI', 'KH', 'CM', 'CA', 'CV', 'KY',
    'CF', 'TD', 'CL', 'CN', 'TW', 'CX', 'CC', 'CO', 'KM', 'CG',
    'CD', 'CK', 'CR', 'CI', 'HR', 'CU', 'CY', 'CZ', 'DK', 'DJ',
    'DM', 'DO', 'TP', 'EC', 'EG', 'SV', 'GQ', 'ER', 'EE', 'ET',
    'XE', 'FO', 'FK', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF', 'GA',
    'GM', 'GE', 'DE', 'GH', 'GI', 'GR', 'GL', 'GD', 'GP', 'GU',
    'GT', 'GG', 'GN', 'GW', 'GY', 'HT', 'HM', 'VA', 'HN', 'HK',
    'HU', 'IS', 'IN', 'ID', 'IR', 'IQ', 'IE', 'IM', 'IL', 'IT',
    'JM', 'JP', 'JE', 'JO', 'KZ', 'KE', 'KI', 'KP', 'KR', 'KW',
    'KG', 'LN', 'LA', 'LV', 'LB', 'LS', 'LR', 'LY', 'LI', 'LT',
    'LU', 'MO', 'MK', 'MG', 'MW', 'MV', 'ML', 'MT', 'MH', 'MQ',
    'MR', 'MU', 'YT', 'MX', 'FM', 'MD', 'MC', 'MN', 'MS', 'MA',
    'MZ', 'MM', 'NA', 'NR', 'NP', 'NL', 'AN', 'NT', 'NC', 'NZ',
    'NI', 'NE', 'NG', 'NU', 'NF', 'MP', 'NO', 'OM', 'OT', 'PK',
    'PW', 'PS', 'PA', 'PZ', 'PG', 'PY', 'PE', 'PH', 'PN', 'PL',
    'PT', 'PR', 'QA', 'RE', 'ME', 'RS', 'RO', 'RU', 'RW', 'SH',
    'KN', 'LC', 'PM', 'VC', 'WS', 'SM', 'ST', 'SA', 'SN', 'SC',
    'SL', 'SG', 'SK', 'SI', 'SB', 'SO', 'ZA', 'GS', 'ES', 'LK',
    'SD', 'SR', 'SJ', 'SZ', 'SE', 'CH', 'SY', 'TJ', 'TZ', 'TH',
    'TG', 'TK', 'TO', 'TT', 'TN', 'TR', 'TM', 'TC', 'TV', 'UG',
    'UA', 'AE', 'GB', 'US', 'UM', 'UY', 'UZ', 'VU', 'VE', 'VN',
    'VG', 'VI', 'WF', 'EH', 'YE', 'YU', 'ZM', 'ZW',
]

# ============================================================================
# ODDENOM FORMAT - OD DENOMINATION OF DOMESTIC OR ISLAMIC
# ============================================================================

ODDENOM_ISLAMIC_PRODUCTS = set(
    [32, 33, 60, 61, 62, 63, 64, 92, 93, 96, 81,
     70, 71, 73, 74] +
    list(range(160, 170)) +           # 160-169
    list(range(182, 189)) +           # 182-188
    [7, 8, 46, 47, 48, 49, 45,
     13, 14] +
    list(range(15, 20)) +             # 15-19
    list(range(23, 26)) +             # 23-25
    [20, 21]
)


def format_oddenom(product: int) -> str:
    """Format OD denomination - Islamic (I) or Domestic (D)"""
    return 'I' if product in ODDENOM_ISLAMIC_PRODUCTS else 'D'


# ============================================================================
# ODPROD FORMAT - OD PRODUCT CODE
# ============================================================================

ODPROD_MAP = {
    3:   '34180',   # OD/GGSM2
    4:   '34180',   # OD/GGSM2-FOCUS
    5:   '34180',   # CLF-I BAE GOVERN.GUARANTEE SCM MADANI
    6:   '34180',   # CLF-I BAE GOVERN.GUARANTEE SCM MADANI-FOCUS
    7:   '34180',   # CLF-i/PGGS-i(NEW FINANCING)
    8:   '34180',   # CLF-i/PGGS-i(ADDITIONAL WORKING CAPITAL)
    9:   '34180',   # PGGS-NEW FINANCING
    10:  '34180',   # PGGS-ADDITIONAL WORKING CAPITAL
    11:  '34180',   # OD/GGSM
    12:  '34180',   # OD/GGSM-FOCUS
    13:  '34180',   # BAE CASH LINE FACILITY-i (BAE CLF-i)
    14:  '34180',   # CLF-I/ BAE BNM SME AES-I (FLD)
    15:  '34180',   # CASHLINE FACILITY-I/ SWIFT
    16:  '34180',   # CASHLINE FACILITY-I/ SWIFT EXTRA
    17:  '34180',   # CLF-I/ BAE BNM SME AES-I
    18:  '34180',   # CLF-I/ WCGS-I EXPORT
    19:  '34180',   # CLF-I/ WCGS-I WOMEN
    20:  '34180',   # CLF-I/ BAE GGSM2
    21:  '34180',   # CLF-I/ BAE GGSM2-Focus
    22:  '34180',   # CLF-I BOOSTER(CASH LINE FACILITY-I BOOSTER)
    23:  '34180',   # (CM) PCA-I
    24:  '34180',   # (CM) PCA-I ENTREPRISE
    25:  '34180',   # (CM) CURRENT ACC-I
    26:  '34180',   # OD/WCGS EXPORT
    27:  '34180',   # OD/WCGS IMPORT
    30:  '34180',   # OD WRITE OFF (RETAIL)
    31:  '34180',   # OD WRITE OFF (CORPORATE)
    32:  '34180',   # RETAIL CLF-I WRITE OFF
    33:  '34180',   # CORP CLF-I WRITE OFF
    34:  '34240',   # RETAIL PB SHARE LINK WRITE OFF
    35:  '34180',   # BNM SME(FLD)
    36:  '34180',   # WCGS START-UP
    37:  '34180',   # BNM SME
    38:  '34180',   # BNM AGRICULTURAL
    39:  '34180',   # BNM DISASTER RELIEF
    41:  '34180',   # CASH MANAGEMENT COLLECTION ACCOUNT
    42:  '34180',   # Premier ACE-Normal
    43:  '34180',   # Premier ACE-External
    45:  '34180',   # COMM MURABAHAH CASH LINE FAC(CM CLF-I)
    46:  '34180',   # CASH LINE FACILITY-I/WCGS-I START UP
    47:  '34180',   # CASH LINE FACILITY-I/BNM SME
    48:  '34180',   # CASH LINE FACILITY-I/BNM SME AGRICULTURAL
    49:  '34180',   # CASH LINE FACILITY-I/BNM DISASTER RELIEF
    50:  '34180',   # CORP NORMAL CURRENT ACCOUNT
    51:  '34180',   # CORP GOVERNMENT ACCOUNT
    52:  '34180',   # CORP EXTERNAL ACCOUNT
    53:  '34180',   # CORP HOUSING DEVELOPER
    54:  '34180',   # CORP BLOCK DISCOUNT
    55:  '34180',   # CORP SWIFT
    56:  '34180',   # CORP OCP
    57:  '34180',   # CORP OD/OC
    58:  '34180',   # CORP SMILAX OD
    59:  '34180',   # CORP LOCAL CHEQUE PURCHASED
    60:  '34180',   # CORP AL-WADIAH CURRENT A/C
    61:  '34180',   # CORP AL-WADIAH GOVERNMENT
    62:  '34180',   # CORP AL-WADIAH EXTERNAL A/C
    63:  '34180',   # CORP AL-WADIAH HOUSING DEV
    64:  '34180',   # CORP AL-WADIAH OCP
    65:  '34180',   # CORP OD/OC
    68:  '34180',   # CURRENT ACC BY FIN INST. (NIB)
    69:  '34180',   # CURRENT ACC BY FIN INST. (IB)
    70:  '34180',   # CORP CLF-I
    71:  '34180',   # CORP PLUS CLF-I
    73:  '34180',   # CLF-I/NEF2
    74:  '34180',   # CASH MANAGEMENT COLLECTION ACCOUNT-I
    75:  '34180',   # CLF/APGS-I
    76:  '34180',   # CLF/IPGS-I
    77:  '34240',   # SHARELINK INDIVIDUAL
    78:  '34240',   # CORPORATE SHARELINK
    81:  '34180',   # EPLUS CA (NORMAL)
    82:  '34180',   # EPLUS CA (EXTERNAL)
    83:  '34180',   # EPLUS CA (STAFF)
    84:  '34180',   # EPLUS CA (STAFF REL)
    85:  '34180',   # OD PB TRADEPLUS (SME)
    86:  '34180',   # OD PB TRADEPLUS (LARGE ENTERPRISES)
    87:  '34180',   # WORKING CAPITAL GUARANTEE SCHEME
    88:  '34180',   # INDUS RESTRUC LN GUARANTEE SCHEME (IRLGS)
    89:  '34180',   # MAH SING OVERDRAFT
    90:  '34180',   # BASIC CURRENT A/C
    91:  '34180',   # BASIC 55 CURRENT A/C
    92:  '34180',   # PCA-I ENTERPRISE
    93:  '34180',   # AL WADIAH CURRENT
    94:  '34180',   # WORKING CAPITAL GUARANTEE SCHEME-I
    95:  '34180',   # INDUSTRY RESTRUCT. LOAN GUARANTEE SCHEME-i
    96:  '34180',   # MUDHARABAH CURRENT ACCT-I
    97:  '34180',   # MUDHARABAH CURRENT ACCT-I (S)
    98:  'N',       # RM OVERDRAFT LOAN
    100: '34180',   # NORMAL CURRENT ACCOUNT
    101: '34180',   # GOVERNMENT ACCOUNT
    102: '34180',   # EXTERNAL ACCOUNT
    103: '34180',   # HOUSING DEVELOPER
    104: '33110',   # VOSTRO LOCAL
    105: '33110',   # VOSTRO FOREIGN
    106: '34180',   # STAFF CURRENT ACCOUNT
    107: 'N',       # PBCS
    108: '34180',   # CGC
    109: '34180',   # SLS
    110: '34180',   # BLOCK DISCOUNT
    111: '34180',   # R/C
    112: '34180',   # PGS
    113: '34180',   # SHARE MARGIN
    114: '34180',   # NEF
    115: '34180',   # PAS
    116: '34180',   # HRS
    117: '34180',   # AUSTRALIAN
    118: '34180',   # CAIS
    119: '34180',   # HOS (<25K - 100K)
    120: '34180',   # HOS (>100K - 200K)
    121: '34180',   # HICOM SHARE
    122: '34180',   # SFFS
    123: '34180',   # PETRONAS
    124: '34180',   # OCP
    125: '34180',   # OD/OC
    126: 'N',       # NORMAL BANKERS CHEQUE
    127: 'N',       # MIDF BANKERS CHEQUE
    128: 'N',       # MIH BANKERS CHEQUE
    129: 'N',       # DD NORMAL
    130: 'N',       # USD C/A
    131: '34180',   # GBP C/A
    132: '34180',   # AUD C/A
    133: '34240',   # PB SHARELINK - NON MARGIN
    134: '34240',   # CORPORATE SHARELINK - NON MARGIN
    135: '34180',   # EMC CODO DEALERS
    136: 'N',       # HKD C/A
    137: '34180',   # 5 HOME PLAN OD LEGAL FEE
    138: '34180',   # MORE PLAN OD LEGAL FEE
    139: 'N',       # S.CR-HIRE PURCHASE
    140: 'N',       # SI-CLEARING ACCOUNT
    141: 'N',       # ATM-PISA CLEARING ACCT
    142: 'N',       # ATM-LOAN CLEARING ACCT
    143: 'N',       # ATM-FD CLEARING ACCT
    144: 'N',       # DD SHARE PUBLIC
    145: 'N',       # DD SHARE BUMI
    146: 'N',       # IBT COLLECTION ACCT
    147: 'N',       # HO INTERBANK SETTLEMENT A/C
    148: 'N',       # INTERBANK GIRO & MEPS CASH COLL
    149: 'N',       # BNM BONDS COLLECTION ACCOUNT
    150: '34180',   # ACE NORMAL
    151: '34180',   # ACE STAFF
    152: '34180',   # ACE EXTERNAL
    153: '34180',   # OD LIFE EXTERNAL
    154: '34180',   # MORE PLAN
    155: '34180',   # 5 HOME PLAN
    156: '34180',   # PB CURRENTLINK
    157: '34180',   # EXTERNAL PB CURRENTLINK
    158: '34180',   # STAFF PB CURRENTLINK
    159: '34180',   # GLOBAL OD/OC - RETAIL
    160: '34180',   # AL-WADIAH CURRENT A/C
    161: '34180',   # AL-WADIAH GOVERNMENT
    162: '34180',   # AL-WADIAH EXTERNAL A/C
    163: '34180',   # AL-WADIAH HOUSING DEV
    164: '34180',   # AL-WADIAH STAFF A/C
    165: '34180',   # AL-WADIAH OCP
    166: '34180',   # CASH LINE FACILITY
    167: '34180',   # CLF-I BAE ENHANCER-I
    168: '34180',   # PLUS CASH LINE FAC-I
    169: '34180',   # CLF-I MURABAHAH ENHANCER-I
    170: '34180',   # OD LIFE PLAN - NORMAL
    171: 'N',       # MYR OVERNIGHT A/C
    172: 'N',       # MYR OVERNIGHT A/C
    173: 'N',       # MYR OVERNIGHT A/C
    174: '34180',   # SHARE SERVICES A/C
    175: '34180',   # SHARE MARGIN A/C
    176: '34180',   # SMILAX OD
    177: '34240',   # PBB SHARELINK SETTLEMENT A/C
    178: '34240',   # PBB SHARELINK - CORPORATE
    180: '34180',   # STAFF RELATED-NORMAL CA
    181: '34180',   # STAFF REL - ACE A/C
    182: '34180',   # STAFF REL - AL WADIAH
    183: '34180',   # CASH LINE FACILITY-i
    184: '34180',   # CASH LINE FACILITY-i (UNIFLEX-i)
    185: '34180',   # CLF-I SERVICES SECTOR (SSGS-I)
    186: '34180',   # CLF-I FLOOD RELIEF FINANCIANG (FRFGS-I)
    187: '34180',   # CLF-I FSMI2 - FREE LEGAL DOCUMENTATION
    188: '34180',   # CLF-I FSMI2 - NON FREE LEGAL DOCUMENTATION
    190: '34180',   # od SME PORTFOLIO G'TEE CGC
    191: '34180',   # LOCAL CHEQUE PURCHASED
    192: '34180',   # MORE PLAN FIXED 3
    193: '34180',   # MORE PLAN FIXED 5
    194: '34180',   # 5 HOME PLAN FIXED 3
    195: '34180',   # 5 HOME PLAN FIXED 5
    196: '34180',   # SWIFT PLAN FREE LEGAL DOC
    197: '34180',   # WORKING CAPITAL FOR CAR DEALERS
    198: '34180',   # FINANCE/REFINANCING PRIME CUSTOMERS
    473: 'N',       # COLLECTION ACCT
    474: 'N',       # COLLECTION ACCT
    475: 'N',       # COLLECTION ACCT
    476: 'N',       # COLLECTION ACCT
    477: 'N',       # COLLECTION ACCT
    478: 'N',       # COLLECTION ACCT
    479: 'N',       # COLLECTION ACCT
    549: 'N',       # GENERAL MASTER COLLECTION
    550: 'N',       # MYR MASTER NOTE
}


def format_odprod(product: int) -> str:
    """Map OD product code to OD product category"""
    return ODPROD_MAP.get(product, '34180')


# ============================================================================
# LNDENOM FORMAT - LOAN DENOMINATION OF DOMESTIC OR ISLAMIC
# ============================================================================

LNDENOM_ISLAMIC_PRODUCTS = set(
    [100] +
    list(range(110, 120)) +          # 110-119
    [120, 122, 126, 127, 128, 129, 130, 131, 132, 185, 169,
     134, 135, 136, 138, 139, 140, 141, 142, 143, 170, 180, 181, 182, 183, 101,
     147, 148, 173, 174, 159, 160, 161, 162] +
    list(range(851, 900)) +          # 851-899
    list(range(152, 159)) +          # 152-158
    [164, 165, 179, 146, 184, 191] +
    list(range(192, 198)) +          # 192-196
    [197, 199, 124, 145, 144, 163, 186, 187] +
    list(range(102, 109)) +          # 102-108
    [188, 189, 190, 137] +
    list(range(400, 412)) +          # 400-411
    [412, 413, 414, 415, 416, 417, 419] +
    list(range(420, 423)) +          # 420-422
    list(range(461, 471)) +          # 461-470
    [418, 427, 428] +
    list(range(429, 445)) +          # 429-444
    [445, 446, 448] +
    list(range(650, 700)) +          # 650-699
    [973] +
    list(range(471, 499))            # 471-498
)


def format_lndenom(product: int) -> str:
    """Format loan denomination - Islamic (I) or Domestic (D)"""
    return 'I' if product in LNDENOM_ISLAMIC_PRODUCTS else 'D'


# ============================================================================
# LNPROD FORMAT - LOAN PRODUCT CODE
# ============================================================================

def _build_lnprod_map() -> Dict[int, str]:
    m: Dict[int, str] = {}

    # STAFF LOANS - 34230
    for k in range(4, 8):        m[k] = '34230'   # 4-7 STAFF LOAN - HOUSING LOAN
    m[15] = '34230'                                # STAFF LOAN - CAR LOAN
    m[20] = '34230'                                # STAFF LOAN - MOTORCYCLE LOAN
    m[25] = '34230'                                # STAFF LOAN - PURCHASE OF COMPUTER
    m[26] = '34230'                                # STAFF LOAN - MEDICAL EXPENSES
    m[27] = '34230'                                # STAFF LOAN - FUNERAL EXPENSES
    m[28] = '34230'                                # STAFF LOAN - DISASTER RELIEF
    m[29] = '34230'; m[30] = '34230'              # STAFF LOAN - PURSUIT OF FURTHER STUDIES
    m[31] = '34230'; m[32] = '34230'              # STAFF LOAN - RENOVATION LOAN
    m[33] = '34230'                                # STAFF LOAN - ECOPARK MEMBERSHIP
    m[34] = '34230'                                # STAFF LOAN - OTHER PURPOSE
    m[60] = '34230'                                # STAFF RENOVATION - PROGRESSIVE
    m[61] = '34230'                                # STAFF RENOVATION - FULL
    m[62] = '34230'                                # PIVB STAFF - RENOVATION
    m[63] = '34230'                                # HHB STAFF CAR LOAN
    m[70] = '34230'                                # PBS STAFF - HOUSING LOAN
    m[71] = '34230'                                # PBS STAFF - CAR LOAN
    m[72] = '34230'                                # PBS STAFF - MOTOCYCLE LOAN
    m[73] = '34230'                                # PBS SLS - PURCHASE OF COMPUTER
    m[74] = '34230'                                # PBS SLS - MEDICAL EXPENSES
    m[75] = '34230'                                # PBS SLS - FUNERAL EXPENSES
    m[76] = '34230'                                # PBS SLS - DISASTER RELIEF
    m[77] = '34230'                                # PBS SLS - STUDY LOAN
    m[78] = '34230'                                # PBS SLS - RENOVATION
    m[79] = '34230'                                # PIVB STAFF SPECIAL LOAN-OTHER PURPOSES
    m[100] = '34230'                               # STAFF LOAN - ABBA HOUSING LOAN
    m[101] = '34230'                               # ABBA STAFF ALTERNATE HOUSING LOAN
    for k in range(102, 109): m[k] = '34230'      # 102-108 ISLAMIC STAFF FINANCING

    # HOUSING LOANS - 34120
    m[110] = '34120'; m[111] = '34120'             # ABBA HOUSING LOAN
    m[112] = '34120'                               # HOME PLAN 4<RM100K- ABBA EQUAL INSTAL
    m[113] = '34120'                               # HOME PLAN 4>RM100K- ABBA EQUAL INSTAL
    m[114] = '34120'                               # HOME PLAN 5<RM100K- ABBA GRS
    m[115] = '34120'                               # HOME PLAN 5>RM100K- ABBA GRS
    m[116] = '34120'                               # ABBA MORTGAGE REFINANCING PLAN
    m[117] = '34120'                               # HOME 4 ABBA - CONSTRUCTION
    m[118] = '34120'                               # HOME 5 ABBA - CONSTRUCTION
    m[119] = '34120'                               # ABBA MORE PLAN - CONSTRUCTION
    m[139] = '34120'                               # ABBA MT RATE HSE FINANCING - PROFIT
    m[140] = '34120'                               # ABBA MT RATE HSE FINANCING - INSTAL
    m[141] = '34120'                               # ABBA HOME FIN.-I PRESC.VAR RATES
    m[142] = '34120'                               # ABBA HOME FIN.-I NON PRESC.VAR RATES
    m[147] = '34120'                               # ABBA HOUSE VARIABLE RATE - MRL
    m[173] = '34120'                               # ABBA HOUSE FINANCING - MRL
    m[150] = '34120'                               # GEN ABBA HSEFIN-I V/R PRCB REDRAW
    m[151] = '34120'                               # GEN ABBA HSE FIN-I V/R NON PRCB REDRAW
    m[152] = '34120'                               # HOME EQUITY FIN-I REDRAW
    m[156] = '34120'                               # RM HOUSING LOAN
    m[175] = '34120'                               # MUSHARAKAH MUTANAQISAH HOME EQUITY-I
    m[445] = '34120'                               # ABBA HOUSE FIN.-I PRESC.VAR RATES
    m[446] = '34120'                               # ABBA HOUSE FIN.-I NON PRESC.VAR RATES
    for k in range(200, 250): m[k] = '34120'       # 200-249 HOUSING LOANS
    for k in range(250, 261): m[k] = '34120'       # 250-260
    m[400] = '34120'                               # EQUITY HOMESAVE-i
    m[409] = '34120'                               # HOME EQUITY FIN-I VR PRCB
    m[410] = '34120'                               # HOME EQUITY FIN-I VR PRCB REDRAW
    m[412] = '34120'                               # HOME EQUITY MORE REFINANCING-I VR
    m[413] = '34120'                               # HOME EQUITY MORE REFINANCING-I VR REDRAW
    m[414] = '34120'                               # HOME EQUITY MORE HOMESAVE-I
    m[415] = '34120'                               # EQUITY HOMESAVE-I II
    m[423] = '34120'                               # EQUITY HOMESAVE-I III
    m[431] = '34120'                               # HOME EQUITY FINANCING-I (NON REDRAW)
    m[432] = '34120'                               # HOME EQUITY FINANCING-I (REDRAW)
    m[433] = '34120'                               # HOME EQUITY FINANCING-I PRCB (NON REDRAW)
    m[440] = '34120'                               # ABBA HOMESAVE-I III
    m[466] = '34120'                               # ABBA HOMESAVE-I II
    m[472] = '34120'                               # CM REDRAW HOUSE FINANCING-I
    m[473] = '34120'                               # CM PRESCRIBED HOUSE FINANCING-I REDRAW
    m[474] = '34120'                               # CM MORE HOUSE FINANCING-I REDRAW
    m[479] = '34120'                               # CM HOMESAVE-I III
    m[484] = '34120'                               # CM MORE HOMESAVE-I III
    m[486] = '34120'                               # CM GENERIC HOUSE FINANCING-I
    m[489] = '34120'                               # CM PRESCRIBED HOUSE FINANCING-I
    m[494] = '34120'                               # CM MORE HOUSE FINANCING-I
    m[600] = '34120'                               # MEMO HOUSING LOAN
    m[638] = '34120'                               # MEMO CORP HOUSING LOAN
    m[650] = '34120'; m[651] = '34120'             # BBA HOUSE FIN-I
    m[664] = '34120'                               # HOME EQUITY FIN-I
    m[677] = '34120'                               # RM HOUSING LOANS
    m[692] = '34120'                               # RM OTHER TERM LOANS
    m[911] = '34120'                               # CORPORATE HOUSING LOAN

    # ABBA TERM FINANCING - 34149
    m[120] = '34149'                               # RETAIL - ABBA TERM/FIXED FINANCING
    m[122] = '34149'                               # ABBA UNIT TRUST FINANCING
    m[126] = '34149'                               # ABBA SWIFT CONSTRUCTION
    m[127] = '34149'                               # ISLAMIC SWIFT PLAN
    m[129] = '34149'                               # SMILAX ABBA
    m[133] = '34149'                               # GENERIC BAE TERM FINANCING-I
    m[134] = '34149'                               # VBI BAE TERM FINANCING-I
    m[137] = '34149'                               # GENERIC PLUS BAE TERM FINANCING -i
    m[143] = '34149'                               # ABBA TERM FIN.-I VARIABLE RATES
    m[144] = '34149'                               # GENERIC ABBA TERM V - BNM FUNDED
    m[148] = '34149'                               # ABBA TERM FINANCING VAR RATE -MRL
    m[149] = '34149'                               # GEN ABBA TERM FIN-I V/R REDRAW
    m[153] = '34149'                               # TERM EQUITY FIN-I REDRAW
    m[154] = '34149'                               # SWIFT TERM EQUITY FIN-I REDRAW
    m[155] = '34149'                               # SMILAX TERM EQUITY FIN-I REDRAW
    m[157] = '34149'                               # RM OTHER TERM LOANS
    m[158] = '34149'                               # RM OTHER TERM LOANS
    m[159] = '34149'                               # CREDIT ENHANCER ISLAMIC -I
    m[160] = '34149'                               # PIBB MICRO FINANCE I (NON-CGC)
    m[161] = '34149'                               # PIBB MICRO FINANCE I (CGC/SEGS)
    m[162] = '34149'                               # BANK NEGARA LOAN GUARANTEE SCHEME-I
    m[163] = '34149'                               # PIBB SMALLBIZ EXPRESS-I (2010-2869)
    m[164] = '34149'                               # GEN ABBA TERM FIN V/R (SWIFTPLAN)
    m[165] = '34149'                               # GEN ABBA TERM FIN-I V/R (SMILAX)
    m[169] = '34149'                               # BAE CREDIT ENHANCER ISLAMIC FIN-I
    m[170] = '34149'                               # SPTF BNM FUND FOR FOOD SCHEME
    m[172] = '34149'                               # BAE BNM/FSMI
    m[174] = '34149'                               # ABBA TERM FINANCING - MRL
    m[176] = '34149'                               # MUSHARAKAH MUTANAQISAH TERM EQUITY-I
    m[177] = '34149'                               # MUSHARAKAH MUTANAQISAH SWIFT EQUITY-I
    m[178] = '34149'                               # MUSHARAKAH MUTANAQISAH SMILAX EQUITY-I
    m[179] = '34149'                               # GEN ABBA SWIFT PLAN V/R (REDRAW)
    m[181] = '34149'                               # CORPORATE - ABBA TERM/FIXED FINANCING
    m[182] = '34149'                               # CORPORATE - BAE TERM FINANCING
    m[183] = '34149'                               # ABBA TERM VARIABLE RATES FIN-I
    m[187] = '34149'                               # CORPORATE MUSY MUTANAQISAH TERM FIN-I VR
    m[188] = '34149'                               # CORPORATE MUSY MUTANAQISAH TERM FIN-I RED
    m[189] = '34149'                               # CORPORATE MURABAHAH TERM FIN-I
    m[193] = '34149'                               # AL IJARAH CORPORATE FINANCING
    m[194] = '34149'                               # ABBA CONSUMER DURABLES GOODS
    m[199] = '34149'                               # CORPORATE WAKALAH SUKUK-I
    m[300] = '34149'; m[301] = '34149'             # FIXED LOAN
    m[304] = '34149'; m[305] = '34149'             # MAH SING FL WITHOUT INT.SUBSIDY
    m[315] = '34149'                               # GENERIC FIXED LOAN REDRAW
    m[316] = '34149'                               # GENERIC SHOP SAVE SWIFT PLAN
    m[320] = '34149'                               # SGE PERSONAL LOAN
    m[322] = '34149'                               # FL/BAE TERM FINANCING
    m[335] = '34149'                               # UNIT TRUST PURCHASE PLAN
    m[345] = '34149'                               # CONTRACT FINANCING
    m[348] = '34149'                               # GENERIC SHOP SAVE
    m[349] = '34149'                               # SWIFT PLAN SHOP SAVE II
    m[356] = '34149'                               # HOME FURNISHING PACKAGE (HFP)
    m[357] = '34149'                               # INSTANT SHARE LOAN PLAN
    m[358] = '34149'                               # UNIFLEX PLAN
    m[359] = '34149'                               # RETAIL-CONVENTIONAL SWIFT PLAN
    m[361] = '34149'                               # RETAIL-CONVENTIONAL SMILAX PLAN
    m[362] = '34149'                               # SPECIAL/REFINANCING FL PACKAGE
    m[363] = '34149'                               # SWIFT PLAN FREE LEGAL DOC
    m[368] = '34149'                               # SHOPSAVE III
    m[401] = '34149'                               # EQUITY SHOPSAVE-i
    m[402] = '34149'                               # EQUITY SWIFTSHOPSAVE-i
    m[403] = '34149'                               # ABBA TERM FIN-I VR(SSME FUND)
    m[404] = '34149'                               # ABBA SWIFT TERM FIN-I VR
    m[405] = '34149'                               # ABBA SMILAX TERM FIN-I VR
    m[406] = '34149'                               # TERM EQUITY FIN-I VR
    m[407] = '34149'                               # SWIFT TERM EQUITY FIN-I VR
    m[408] = '34149'                               # SMILAX TERM EQUITY FIN-I VR
    m[411] = '34149'                               # TERM EQUITY FIN-I VR (UNIFLEX-i)
    m[416] = '34149'                               # EQUITY SHOPSAVE-I II
    m[418] = '34149'                               # TERM EQUITY FINANCING-I
    m[421] = '34149'                               # BAE GOVERNMENT FINANCING-I
    m[425] = '34149'                               # EQUITY SHOPSAVE-I III
    m[427] = '34149'                               # NEW ENTREPRENEUR FUND-I(MEF-I)
    m[428] = '34149'                               # MIRCRO ENTREPRENEUR FUND-I(MEF-I)
    m[429] = '34149'                               # TERM EQUITY FINANCING-I VR (SSME FUND 2)
    m[430] = '34149'                               # TERM EQUITY FINANCING-I VR (SSME FUND 2)
    for k in range(434, 440): m[k] = '34149'       # 434-439 BAE Government Guarantee Schemes-i
    m[442] = '34149'                               # ABBA SHOPSAVE-I III
    m[447] = '34149'                               # BAE TERM FINANCING
    m[448] = '34149'                               # BAE TF/HIGH TECH FACILITY
    m[461] = '34149'                               # ABBA TERM FIN-I VR (UNIFLEX-i)
    m[462] = '34149'                               # ABBA TERM FIN-I VR (TR-i)
    m[463] = '34149'                               # ABBA TERM FIN-I VR (AB-i)
    m[467] = '34149'                               # ABBA SHOPSAVE-I II
    m[471] = '34149'                               # CM REDRAW TERM FINANCING-I
    m[476] = '34149'                               # CM SMILAX REDRAW
    m[478] = '34149'                               # CM SWIFT REDRAW
    m[480] = '34149'                               # CM SHOPSAVE-I III
    m[481] = '34149'                               # CM SWIFT SHOPSAVE-I III
    m[485] = '34149'                               # CM GENERIC TERM FINANCING-I
    m[487] = '34149'                               # CM SWIFT TERM FINANCING-I
    m[488] = '34149'                               # CM SMILAX TERM FINANCING-I
    m[504] = '34149'; m[505] = '34149'             # FIXED LOAN UNDER NPGS
    m[509] = '34149'; m[510] = '34149'             # FIXED LOAN UNDER NPGS-BLK GUARANTEE SCHEME
    m[512] = '34149'                               # PBB SMALLBIZ EXPRESS
    m[515] = '34149'                               # FIXED LOAN UNDER PRINCIPAL GUARANTEE SCHEME
    m[516] = '34149'                               # CGC ASSOCIATED SLS 1990 SCHEME
    m[517] = '34149'                               # ASSOCIATION SPECIAL LOAN SCHEME
    m[518] = '34149'                               # GOVERNMENT LOAN GUARANTEE
    m[519] = '34149'                               # SLS NON TAX REBATE
    m[521] = '34149'; m[522] = '34149'; m[523] = '34149'  # CGC TUK PACKAGE 1,2,3
    m[524] = '34149'                               # CGC/FLEXI GUARANTEE SCHEME - FSMI
    m[525] = '34149'                               # CGC/FLEXI GUARANTEE SCHEME - RFSMI
    m[526] = '34149'                               # CGC/FLEXI GUARANTEE SCHEME - 3F
    m[527] = '34149'                               # CGC/FLEXI GUARANTEE SCHEME - NEF
    m[528] = '34149'                               # CGC TUK PACKAGE 4
    m[529] = '34149'                               # DIRECT ACCESS GUARANTEE SCHEME
    m[530] = '34149'                               # FIXED LOAN SOLD TO CAGAMAS
    m[531] = '34149'                               # SPECIAL RELIEF GUARANTEE FACILITY FOR SARS
    m[532] = '34149'                               # CREDIT ENHANCER SCHEME
    m[533] = '34149'                               # SME FACILITY FOR CGC
    m[555] = '34149'; m[556] = '34149'             # RETAIL-SPECIAL FUND FOR TOURISM
    m[559] = '34149'; m[560] = '34149'             # RETAIL-NEW ENTREPRENEUR FUND
    m[561] = '34149'                               # RETAIL-BNM FUND FOR LOW/MED HOUSE
    m[564] = '34149'; m[565] = '34149'             # RETAIL-FUND FOR FOOD
    m[566] = '34149'                               # RETAIL-BNM REHAB. FUND FOR SMI
    m[567] = '34149'                               # RETAIL-NEW ENTREPRENEUR FUND 2000
    m[568] = '34149'                               # RETAIL-FSMI2
    m[569] = '34149'                               # SPECIAL LN FOR SMI - FSMI2
    m[570] = '34149'                               # RETAIL-BNM FUND FOR SMI
    m[573] = '34149'                               # BNM-REHAB.FUND FOR SMALL BUSINESS.
    m[574] = '34149'                               # MICRO ENTERPRISE FUND (MEF)
    m[575] = '34149'                               # SPECIAL RELIEF FACILITY 2015 FOR FLOOD
    m[576] = '34149'                               # BNM FUNDED FIXED LOAN (FL/BNM SME ADF
    m[577] = '34149'                               # PENJANA SME FINANCING
    m[578] = '34149'                               # HIGH TECH FACILITY NATIONAL INVESTMENT
    m[601] = '34149'                               # MEMO RETAIL TERM LOAN
    m[602] = '34149'                               # MEMO RETAIL SMILAX
    m[603] = '34149'                               # MEMO RETAIL SWIFT
    m[606] = '34149'                               # MEMO CGC GUARANTEE TERM LOAN
    m[607] = '34149'                               # MEMO RETAIL BNM FUNDED
    m[631] = '34149'                               # MEMO CORP TERM LOAN
    m[632] = '34149'                               # MEMO CORP SWIF
    m[633] = '34149'                               # MEMO CORP SMILAX
    m[636] = '34149'                               # MEMO CORP BNM FUNDED LOAN
    m[639] = '34149'                               # MEMO CORP MTN LOAN
    m[640] = '34149'                               # MEMO CORP UNSECURED FIXED LOAN
    m[654] = '34149'; m[655] = '34149'             # RETAIL BBA TERM FIN-I
    m[656] = '34149'; m[657] = '34149'             # RETAIL BBA SWIFT
    m[658] = '34149'; m[659] = '34149'             # RETAIL BBA SMILAX
    m[661] = '34149'; m[662] = '34149'             # BNM FUNDED FIN-I
    m[663] = '34149'                               # CGC GUARANTEE FAC-I
    m[665] = '34149'                               # TERM EQUITY FIN-I
    m[666] = '34149'                               # SWIFT EQUITY FIN-I
    m[667] = '34149'                               # SMILAX EQUITY FIN-I
    m[671] = '34149'                               # RM OTHER TERM LOANS
    m[676] = '34149'                               # RM OTHER TERM LOANS
    m[680] = '34149'                               # CORP BAE FIN-I
    m[681] = '34149'; m[682] = '34149'             # CORP BBA TERM FIN-I
    m[683] = '34149'                               # CORP IJARAH FIN-I
    m[687] = '34149'                               # MEMO CORPORATE TERM EQUITY FIN-I VR
    m[688] = '34149'                               # MEMO CORPORATE COMMODITY MURABAHAH TERM F-I
    m[691] = '34149'                               # RM OTHER TERM LOANS
    m[694] = '34149'                               # RM OTHER TERM LOANS
    m[696] = '34149'                               # MEMO CORPORATE WAKALAH SUKUK-I
    m[900] = '34149'; m[901] = '34149'             # CBL - FIXED LOAN
    m[906] = '34149'                               # CORPORATE-CONVENTIONAL SWIFT PLAN
    m[907] = '34149'                               # CORPORATE-CONVENTIONAL SMILAX PLAN
    m[909] = '34149'                               # CORPORATE-FL FUNDED BY BNM
    m[912] = '34149'                               # CORPORATE MTN LOAN
    m[913] = '34149'                               # CORP UNSECURED FIXED LOAN
    m[916] = '34149'                               # CORPORATE SWIFTPLAN SHOPSAVE II
    m[918] = '34149'                               # CORPORATE SWIFTPLAN FIXED LOAN REDRAW

    # HIRE PURCHASE - 34111
    m[128] = '34111'                               # AITAB HP SCHEDULED GOODS
    m[130] = '34111'                               # AITAB HP UNSCHEDULED GOODS
    m[131] = '34111'                               # AITAB SCHEDULED GOODS
    m[132] = '34111'                               # AITAB UNSCHEDULED GOODS
    m[380] = '34111'                               # HP SCHEDULE GOODS
    m[381] = '34111'                               # HP UNSCHEDULE GOODS
    m[700] = '34111'; m[705] = '34111'             # HIRE PURCHASE
    m[720] = '34111'; m[725] = '34111'             # HP - SCHEDULED & UNSCHEDULED
    m[750] = '34111'                               # HIRE PURCHASE-AGENCY
    m[752] = '34111'; m[760] = '34111'             # HIRE PURCHASE

    # PERSONAL LOANS - 34117
    m[135] = '34117'                               # AL BAI'INAH FINANCING
    m[136] = '34117'                               # BAI-AL-EINAH PLUS (BAE PLUS)
    m[138] = '34117'                               # CASH BAE PERSONAL FINANCING
    m[303] = '34117'                               # PERSONAL LOAN SECURED
    m[306] = '34117'                               # PERSONAL LOAN UNSECURED
    m[307] = '34117'                               # PERSONAL LOAN/PLUS
    m[308] = '34117'                               # RM PERSONAL LOANS
    m[311] = '34117'                               # RM PERSONAL LOANS
    m[313] = '34117'                               # PERSONAL LOAN ORIGINATE HOMESAVE III
    m[325] = '34117'                               # PROFESSIONAL ADVANTAGE SCHEME
    m[330] = '34117'                               # QUICK CASH LOAN
    m[340] = '34117'                               # GLENMARIE GOLF & COUNTRY CLUB MEMBERSHIP FINANCING
    m[354] = '34117'                               # PERSONAL LOAN UNDER UNIFLEX PLAN
    m[355] = '34117'                               # PB EXECUTIVE LOAN SCHEME
    m[367] = '34117'                               # RM PERSONAL LOANS
    m[369] = '34117'                               # PERSONAL LOAN ORIGINATE SHOPSAVE III
    m[391] = '34117'                               # CONSUMER DURABLES
    m[419] = '34117'                               # EQUITY MORE ORIGINATE : HOMESAVE-I II
    m[420] = '34117'                               # EQUITY SWIFT/ORIGINATE : SHOPSAVE-I II
    m[422] = '34117'                               # PERSONAL FINANCING-I REDRAW
    m[424] = '34117'                               # PERSONAL FINANCING EQUITY HOMESAVE-I III
    m[426] = '34117'                               # PERSONAL FINANCING EQUITY SHOPSAVE-I III
    m[441] = '34117'                               # PERSONAL FINANCING ABBA HOMESAVE-I III
    m[443] = '34117'                               # PERSONAL FINANCING ABBA SHOPSAVE-I III
    m[464] = '34117'                               # BAE PERSONAL FINANCING-I SECURED VR
    m[465] = '34117'                               # BAE PERSONAL FINANCING-I UNSECURED VR
    m[468] = '34117'                               # BAE PERSONAL FINANCING-I/PLUS VR
    m[469] = '34117'                               # ABBA MORE ORIGINATE HOMESAVE-I II
    m[470] = '34117'                               # ABBA SWIFT ORIGINATE SHOPSAVE-I II
    m[475] = '34117'                               # CM PERSONAL FINANCING-I REDRAW(HOUSE)
    m[477] = '34117'                               # CM PERSONAL FINANCING=I REDRAW(TERM)
    m[482] = '34117'                               # CM PERSONAL FINANCING MORE HOME PLAN
    m[483] = '34117'                               # CM PERSONAL FINANCING SWIFT SHOPSAVE-I III
    m[490] = '34117'                               # CM PERSONAL FINANCING-I(TERM)
    m[491] = '34117'                               # CM PERSONAL FINANCING-I(HOUSE)
    m[492] = '34117'                               # CM PERSONAL FINANCING-I SWIFT/SRP
    m[493] = '34117'                               # CM PERSONAL FINANCING-I NON SWIFT/SRP
    m[496] = '34117'                               # CM PERSONAL FINANCING-I/MORE
    m[497] = '34117'                               # CM PERSONAL FINANCING-I/SWIFT
    m[498] = '34117'                               # CM PERSONAL FINANCING-I/PLUS
    m[609] = '34149'                               # MEMO PERSONAL LOAN
    m[610] = '34117'                               # MEMO PERSONAL LOAN SECURED
    m[611] = '34117'                               # MEMO PERSONAL LOAN UNSECURED
    m[652] = '34117'; m[653] = '34117'             # RETAIL BAE FIN-I
    m[668] = '34117'                               # BAE PERSONAL FINANCING-I SECURED VR
    m[669] = '34117'                               # BAE PERSONAL FINANCING-I UNSECURED VR
    m[672] = '34117'                               # MEMO PERSONAL FINANCING-I EQUITY MORE
    m[673] = '34117'                               # MEMO PERSONAL FINANCING-I EQUITY SWIFT
    m[674] = '34117'                               # MEMO PERSONAL FINANCING-I ABBA MORE
    m[675] = '34117'                               # MEMO PERSONAL FINANCING-I ABBA SWIFT
    m[693] = '34117'                               # RM PERSONAL LOANS

    # BRIDGING LOANS - 34114
    m[309] = '34114'; m[310] = '34114'             # BRIDGING LOAN
    m[417] = '34114'                               # TERM EQUITY BRIDGING FINANCING-I
    m[191] = '34114'                               # CORPORATE TERM EQUITY BRIDGING FINANCING-I
    m[608] = '34114'                               # MEMO RETAIL BRIDGING LOAN
    m[637] = '34114'                               # MEMO CORP BRIDGING LOAN
    m[670] = '34114'                               # MEMO TERM EQUITY BRIDGING FINANCIANG-I
    m[690] = '34114'                               # MEMO CORPORATE TERM EQUITY BRIDGING FAC-I
    m[904] = '34114'; m[905] = '34114'             # CBL - BRIDGING LOAN
    m[919] = '34114'; m[920] = '34114'             # CBL - BRIDGING LOAN

    # LEASING - 34112
    m[390] = '34112'                               # LEASING
    m[799] = '34112'                               # LEASING

    # BLOCK DISCOUNTING - 34113
    m[360] = '34113'                               # RETAIL-BLOCK DISCOUNTING
    m[770] = '34113'; m[775] = '34113'             # BLOCK DISCOUNTING
    m[908] = '34113'                               # CORPORATE-BLOCK DISCOUNTING

    # SYNDICATED LOANS - 34115
    m[180] = '34115'                               # CORP IJARAH SYND. FINANCING
    m[185] = '34115'                               # SYNDICATED CORPORATE BAE TERM FIN-I
    m[186] = '34115'                               # CORP COMMODITY MURABAHAH SYND FIN-I
    m[197] = '34115'                               # CORPORATE MURABAHAH SYNDICATED FINANCING-I
    m[635] = '34115'                               # MEMO CORP SYNDICATED LOAN
    m[684] = '34115'                               # CORP TAWARRUQ TERM FIN-I
    m[686] = '34115'                               # AL-MURABAHAH
    m[914] = '34115'; m[915] = '34115'             # CBL - SYNDICATED FIXED LOAN
    m[950] = '34115'                               # SYNDICATED SHARED LOAN TERM

    # REVOLVING CREDIT - 34190
    m[146] = '34190'                               # RETAIL REVOLV CREDIT FACILITY-I
    m[184] = '34190'                               # CORPORATE ABBA RC FACILITY-I
    m[190] = '34190'                               # CORPORATE COMMODITY MURABAHAH RV CR FACI-I
    m[192] = '34190'                               # CORPORATE BAE REVOLVING CREDIT FACILITY-I
    m[195] = '34190'                               # CORPORATE MURABAHAH REVOLVING CREDIT FAC-I
    m[196] = '34190'                               # CORPORATE MUSYARAKAH MUTANAQISAH REVOLVING
    m[302] = '34190'                               # FIXED LOAN REVOLVING CREDIT
    m[350] = '34190'                               # REVOLVING CREDIT
    m[351] = '34190'                               # GENERIC SWIFT RC
    m[364] = '34190'                               # REVOLVING SHARE LOAN
    m[365] = '34190'                               # PUBLIC SHARE ISSUE LOAN (PSIL)
    m[495] = '34190'                               # CM REVOLVING CREDIT FACILITY-I(RETAIL)
    m[506] = '34190'                               # FIXED LOAN NPGS - REVOLVING CREDIT
    m[604] = '34190'                               # MEMO RETAIL RC
    m[605] = '34190'                               # MEMO RETAIL SWIFT RC
    m[634] = '34190'                               # MEMO CORP RC
    m[641] = '34190'                               # MEMO CORP UNSECURED REVOLVING CREDIT
    m[660] = '34190'                               # RETAIL RC FAC-I
    m[685] = '34190'                               # OTHER ISLAMIC CONCEPTS FINANCING
    m[689] = '34190'                               # AL-MURABAHAH
    m[695] = '34190'                               # RM OTHER TERM LOANS
    m[802] = '34690'                               # FCY REVOLVING CREDIT (USD)
    m[803] = '34690'                               # FCY REVOLVING CREDIT (HKD)
    m[806] = '34690'                               # FCY CORPORATE REVOLVING CREDIT (GBP)
    m[808] = '34690'                               # FCY REVOLVING CREDIT (AUD)
    m[810] = '34690'                               # FCY REVOLVING CREDIT (SGD)
    m[812] = '34690'                               # FCY REVOLVING CREDIT (NZD)
    m[814] = '34690'                               # FCY REVOLVING CREDIT (EUR)
    m[817] = '34690'                               # FCY REVOLVING CREDIT (CHF)
    m[818] = '34690'                               # FCY REVOLVING CREDIT (JPY)
    m[856] = '34690'                               # FCY REVOLVING CREDIT FINANCING (USD)
    m[857] = '34690'                               # FCY REVOLVING CREDIT FINANCING (NZD)
    m[858] = '34690'                               # FCY REVOLVING CREDIT FINANCING (EUR)
    m[859] = '34690'                               # FCY REVOLVING CREDIT FINANCING (AUD)
    m[860] = '34690'                               # FCY REVOLVING CREDIT FINANCING (GBP)
    m[902] = '34190'                               # CORP FIXED LOAN - REVOLVING CREDIT
    m[903] = '34190'                               # CORP SHARE LOAN - REVOLVING CREDIT
    m[910] = '34190'                               # CBL - REVOLVING CREDIT
    m[917] = '34190'                               # CORP UNSECURED REVOLVING CREDIT
    m[925] = '34190'                               # CBL - SYNDICATED REVOLVING CREDIT
    m[951] = '34190'                               # SYNDICATED SHARED LOAN - REV. CREDIT

    # FLOOR STOCKING - 34170
    m[392] = '34170'                               # FLOOR STOCKING LOAN
    m[612] = '34170'                               # FLOOR STOCKING LOAN

    # FCY LOANS - 34600
    m[800] = '34600'                               # FCY TERM LOAN (USD)
    m[801] = '34600'                               # FCY TERM LOAN (HKD)
    m[804] = '34600'                               # FCY SYNDICATED TERM LN (USD)
    m[805] = '34600'                               # FCY CORPORATE TERM LN (GBP)
    m[807] = '34600'                               # FCY TERM LOAN (AUD)
    m[809] = '34600'                               # FCY TERM LOAN (SGD)
    m[811] = '34600'                               # FCY TERM LAON (NZD)
    m[813] = '34600'                               # FCY TERM LOAN (EUR)
    m[815] = '34600'                               # FCY SECURED USD FL (RETAIL)
    m[816] = '34600'                               # FCY TERM LOAN (CHF)
    m[851] = '34600'                               # FCY TERM FINANCING (USD)
    m[852] = '34600'                               # FCY TERM FINANCING (NZD)
    m[853] = '34600'                               # FCY TERM FINANCING (EUR)
    m[854] = '34600'                               # FCY TERM FINANCING (AUD)
    m[855] = '34600'                               # FCY COMMODITY MURABAHAH TERM FIN-I

    # SOLD TO CAGAMAS - 54120
    m[124] = '54120'                               # ABBA HSG FINANCG-I SOLD TO CAGAMAS
    m[145] = '54120'                               # ABBA HSG FIN.-I V/R SOLD TO CAGAMAS
    m[225] = '54120'                               # NORMAL HOUSING LOAN SOLD TO CAGAMAS
    m[226] = '54120'                               # NORMAL HOUSING LOAN SOLD TO CAGAMAS

    # SOLD TO CAGAMAS HP - 54124
    m[709] = '54124'                               # HIRE PURCHASE SOLD TO CAGAMAS
    m[710] = '54124'                               # HIRE PURCHASE SOLD TO CAGAMAS

    # SPECIAL CATEGORIES - N
    m[500] = 'N'                                   # LOAN FUND FOR HAWKER PETTY TRADER 1994 (LFHPT)
    m[520] = 'N'                                   # LOAN FUND FOR HAWKER PETTY TRADER 1990 (LFHPT)
    m[678] = 'N'                                   # MEMO HP SCHEDULE V/R
    m[679] = 'N'                                   # MEMO HP UNSCHEDULE V/R
    m[698] = 'N'                                   # MEMO AITAB SCHEDULE VARIABLE RATES
    m[699] = 'N'                                   # MEMO AITAB UNSCHEDULE VARIABLE RATES
    m[981] = 'N'                                   # ABBA HOUSE FINANCING WRITTEN OFF
    m[982] = 'N'                                   # ABBA FIXED LOANS WRITTEN OFF
    m[983] = 'N'                                   # AITAB FINANCING WRITTEN OFF
    m[991] = 'N'                                   # HOUSING LOANS WRITTEN OFF
    m[992] = 'N'                                   # FIXED LOANS WRITTEN OFF
    m[993] = 'N'                                   # HIRE PUCHASE WRITTEN OFF
    #  972 = 'N'  DUMMY CONVENTIONAL TERM LOAN  (excluded per 2016-1407/2019-816)
    #  973 = 'N'  DUMMY ISLAMIC TERM FINANCING  (excluded per 2016-1409/2019-816)

    # WRITTEN DOWN - M
    m[984] = 'M'                                   # ABBA HOUSE FINANCING WRITTEN DOWN
    m[985] = 'M'                                   # ABBA FIXED LOANS WRITTEN DOWN
    m[994] = 'M'                                   # HOUSING LOANS WRITTEN DOWN
    m[995] = 'M'                                   # FIXED LOANS WRITTEN DOWN
    m[996] = 'M'                                   # HIRE PURCHASE WRITTEN DOWN

    return m


LNPROD_MAP: Dict[int, str] = _build_lnprod_map()


def format_lnprod(product: int) -> str:
    """Map product code to loan product category"""
    return LNPROD_MAP.get(product, '34149')


# ============================================================================
# LIQPFMT FORMAT - LIQUIDITY PRODUCT FORMAT
# ============================================================================

LIQPFMT_HL = set(
    [117, 113, 118, 115, 116, 110, 111, 139, 140, 141, 142,
     227, 228, 230, 231, 234, 201, 204, 205, 226, 225,
     235, 236, 237, 238, 239, 240, 241, 242, 248,
     100, 101, 112, 114, 170, 200,
     209, 210, 211, 212, 214, 215, 213, 216, 217, 218,
     219, 220, 229, 232, 233, 244, 245, 246, 247,
     243, 150, 151, 175, 152, 156, 445, 446,
     400, 409, 410, 412, 413, 414, 415, 423,
     440, 466, 253, 254, 431, 432, 433, 255,
     256, 257, 258, 259, 260,
     638, 650, 651, 664, 600, 249, 911, 250]
)

LIQPFMT_RC = set(
    [302, 365, 506, 902, 903, 951,
     350, 910, 925, 364, 146, 184, 192, 195,
     802, 803, 856, 857, 858, 859, 351,
     860, 660, 685, 604, 605, 634, 806,
     808, 810, 812, 814, 190, 689, 641,
     917, 817]
)

LIQPFMT_FS = set([392, 612])


def format_liqpfmt(product: int) -> str:
    """Format liquidity product - HL, RC, FS, or FL"""
    if product in LIQPFMT_HL:
        return 'HL'
    elif product in LIQPFMT_RC:
        return 'RC'
    elif product in LIQPFMT_FS:
        return 'FS'
    return 'FL'


# ============================================================================
# SLTYPE FORMAT - STAFF LOAN TYPE
# ============================================================================

SLTYPE_HL = set(list(range(4, 8)) + [70] + list(range(100, 103)) + [106])  # HOUSING LOAN
SLTYPE_HP = set([15, 20, 71, 72, 103, 104, 107])                            # HIRE PURCHASE
SLTYPE_FL = set(list(range(25, 35)) + list(range(60, 63)) + list(range(73, 80)) + [105, 108])  # FIXED LOAN


def format_sltype(product: int) -> str:
    """Format staff loan type"""
    if product in SLTYPE_HL:
        return 'HL'
    elif product in SLTYPE_HP:
        return 'HP'
    elif product in SLTYPE_FL:
        return 'FL'
    return ' '


# ============================================================================
# LN03FMT FORMAT
# ============================================================================

LN03FMT_SL = set(
    list(range(4, 8)) + [15, 20] + list(range(25, 35)) +
    list(range(60, 64)) + list(range(70, 79)) +
    list(range(100, 109))
)
LN03FMT_OT = {225}
LN03FMT_P1 = set(
    [110, 112, 114, 200, 201, 141, 150,
     210, 212, 227, 230, 232, 409,
     237, 239, 244, 243, 246, 410,
     255, 433, 445, 258, 259]
)
LN03FMT_P2 = set(
    [170, 524, 525, 526, 527, 531,
     555, 556, 559, 560, 561, 564,
     565, 566, 567, 568, 569, 570,
     573, 909, 162, 575, 576, 144, 172,
     418, 427, 428, 473, 489, 434, 421,
     435, 437, 438, 439, 448, 577, 578, 436]
)
LN03FMT_P3 = set(
    [111, 113, 115, 116, 117, 118, 209,
     139, 140, 204, 205, 214, 142, 211,
     215, 219, 220, 228, 231, 233, 151, 152,
     156, 175, 256, 257,
     234, 235, 236, 238, 240, 241, 213, 216,
     242, 245, 247, 248, 217, 218, 260, 400,
     412, 413, 414, 415, 423, 466, 249,
     250, 440, 253, 254, 431, 432, 446,
     472, 474, 479, 484, 486, 494]
)
LN03FMT_P5 = set([392, 612])


def format_ln03fmt(product: int) -> str:
    """Format LN03 loan type"""
    if product in LN03FMT_SL:
        return 'SL'
    elif product in LN03FMT_OT:
        return 'OT'
    elif product in LN03FMT_P1:
        return 'P1'
    elif product in LN03FMT_P2:
        return 'P2'
    elif product in LN03FMT_P3:
        return 'P3'
    elif product in LN03FMT_P5:
        return 'P5'
    return 'P4'


# ============================================================================
# ODRATE FORMAT - OD RATE TYPE
# ============================================================================

ODRATE_FIXED_PRODUCTS = set(
    list(range(60, 64)) +            # 60-63
    list(range(160, 165)) +          # 160-164
    [166, 96, 93, 97]
)

ODRATE_FLOATING_PRODUCTS = set(
    [50, 51, 52, 53, 54, 55, 56, 57, 65] +
    list(range(100, 104)) +          # 100-103
    [106] +
    list(range(108, 126)) +          # 108-125
    [137, 138] +
    list(range(150, 159)) +          # 150-158
    [159, 170, 174, 175, 176, 177, 178, 179, 180] +
    list(range(191, 199)) +          # 191-198
    [135]
)


def format_odrate(product: int) -> str:
    """Format OD rate type"""
    if product in ODRATE_FIXED_PRODUCTS:
        return '30593'   # FIXED RATE
    # default is floating rate (30595)
    return '30595'       # FLOATING RATE


# ============================================================================
# LNRATE FORMAT - LOAN RATE TYPE
# ============================================================================

LNRATE_FIXED_HOUSE = set(
    list(range(110, 120)) +          # 110-119
    [124, 139, 140, 141, 142, 145,
     200, 201, 204, 205, 209, 210, 211, 212,
     214, 215, 219, 220, 225, 226, 227,
     228, 230] +
    list(range(230, 249))            # 230-248
)
LNRATE_FIXED_HP = set([128, 130, 380, 381, 700, 705])
LNRATE_BLR_PLUS = set([131, 132, 348, 720, 725, 392, 612])
LNRATE_OTHER_FIXED = set(
    list(range(4, 8)) + [15, 20] + list(range(25, 35)) +
    list(range(60, 64)) + [96, 97] + list(range(70, 80)) + [100, 533]
)
LNRATE_COST_PLUS = set(
    [120, 122, 126, 127, 129, 135, 136, 138, 143, 146] +
    list(range(180, 185)) +          # 180-184
    list(range(193, 197)) +          # 193-196
    list(range(300, 303)) +          # 300-302
    [304, 305] +
    list(range(309, 311)) +          # 309-310
    [315, 320, 325, 330, 335, 340, 345, 350] +
    list(range(355, 366)) +          # 355-365
    [390, 391] +
    list(range(900, 910)) +          # 900-909
    [910, 914, 915, 919, 920, 925, 950, 951]
)
LNRATE_OTHER_FLOATING = set(
    [144, 170] +
    list(range(504, 507)) +          # 504-506
    list(range(509, 511)) +          # 509-510
    [512] +
    list(range(515, 533)) +          # 515-532
    [555, 556, 559, 560, 561] +
    list(range(564, 571)) +          # 564-570
    [573, 574, 575, 576]
)


def format_lnrate(product: int) -> str:
    """Format loan rate type"""
    if product in LNRATE_FIXED_HOUSE:
        return '30591'   # FIXED RATE - HOUSE LOAN
    elif product in LNRATE_FIXED_HP:
        return '30592'   # FIXED RATE - HP
    elif product in LNRATE_BLR_PLUS:
        return '30595'   # BLR - PLUS
    elif product in LNRATE_OTHER_FIXED:
        return '30593'   # OTHER FIXED RATE
    elif product in LNRATE_COST_PLUS:
        return '30596'   # COST-PLUS
    elif product in LNRATE_OTHER_FLOATING:
        return '30597'   # OTHER FLOATING RATE
    return '30595'       # ASSUME THIS AS NO INSTRUCTION FR LWU


# ============================================================================
# CUSTOMER CODE FORMATS
# ============================================================================

# Shared base customer code map (used by ODCUSTCD and LOCUSTCD)
CUSTCD_BASE_MAP: Dict[int, str] = {
    1:  '01',   # BANK NEGARA MALAYSIA
    2:  '02',   # COMMERCIAL BANKS
    3:  '03',   # ISLAMIC BANKS
    4:  '04',   # SUBSIDIARY STOCKBROKING COMPANIES
    5:  '05',   # ASSOCIATE STOCKBROKING COMPANIES
    6:  '06',   # OTHER STOCKBROKING COMPANIES
    10: '11',   # DOMESTIC BANKING INSTITUTIONS
    11: '11',   # FINANCE COMPANIES
    12: '12',   # MERCHANT BANKS
    13: '13',   # DISCOUNT HOUSES
    15: '79',   # DOMESTIC NON-BANK ENTITIES
    17: '17',   # CAGAMAS BERHAD
    20: '30',   # DOMESTIC NON-BANK FINANCIAL INSTITUTIONS
    30: '30',   # DOMESTIC OTHER NBFI
    32: '32',   # CREDIT CARD COMPANIES
    33: '33',   # DEVELOPMENT FINANCE INSTITUTIONS
    34: '34',   # BUILDING SOCIETIES
    35: '35',   # CO-OPERATIVE SOCIETIES
    36: '04',   # STOCKBROKING COMPANIES (ODCUSTCD uses 04)
    37: '37',   # COMMODITY BROKERS
    38: '38',   # CREDIT & LEASING COMPANIES
    39: '39',   # UNIT TRUST COMPANIES
    40: '40',   # INSURANCE AND INSURANCE RELATED COMPANIES
    41: '41',   # DBE - BUMI CONTROLLED SME - MICRO
    42: '42',   # DBE - BUMI CONTROLLED SME - SMALL
    43: '43',   # DBE - BUMI CONTROLLED SME - MEDIUM
    44: '44',   # DBE - NON-BUMI CONTROLLED SME - MICRO
    46: '46',   # DBE - NON-BUMI CONTROLLED SME - SMALL
    47: '47',   # DBE - NON-BUMI CONTROLLED SME - MEDIUM
    48: '48',   # DBE - NON-RESIDENT CONTROL SME - MICRO
    49: '49',   # DBE - NON-RESIDENT CONTROL SME - SMALL
    51: '51',   # DBE - NON-RESIDENT CONTROL SME - MEDIUM
    52: '52',   # DBE - GOVERNMENT CONTROLLED SME - MICRO
    53: '53',   # DBE - GOVERNMENT CONTROLLED SME - SMALL
    54: '54',   # DBE - GOVERNMENT CONTROLLED SME - MEDIUM
    50: '79',   # RESIDENTS/DOMESTIC ENTITIES
    57: '57',   # PETRONAS
    59: '59',   # OTHER GOVERNMENT CONTROLLED DBE NIE
    60: '62',   # DOMESTIC BUSINESS ENTERPRISES (DBE)
    61: '61',   # BUMIPUTRA CONTROLLED DBE
    62: '62',   # NON-BUMIPUTRA CONTROLLED DBE
    63: '63',   # NON-RESIDENT CONTROLLED DBE
    64: '64',   # GOVERNMENT CONTROLLED DBE
    65: '44',   # SMALL MEDIUM INDUSTRIES (SMI)
    66: '41',   # BUMIPUTRA CONTROLLED SMI
    67: '44',   # NON-BUMIPUTRA CONTROLLED SMI
    68: '48',   # NON-RESIDENT CONTROLLED SMI
    69: '52',   # GOVERNMENT CONTROLLED SMI
    70: '71',   # GOVERNMENT
    71: '71',   # FEDERAL GOVERNMENT
    72: '72',   # STATE GOVERNMENT
    73: '73',   # LOCAL GOVERNMENT
    74: '74',   # STATUTORY AUTHORITIES
    75: '75',   # NFPE
    76: '78',   # INDIVIDUALS
    77: '77',   # BUMIPUTRA
    78: '78',   # NON-BUMIPUTRA
    79: '79',   # DOMESTIC OTHER ENTITIES NIE
    80: '86',   # NON-RESIDENTS/FOREIGN ENTITIES
    81: '86',   # FOREIGN BANKING INSTITUTIONS (OD uses 86)
    85: '86',   # FOREIGN NON-BANK ENTITIES
    86: '86',   # FOREIGN BUSINESS ENTERPRISES
    87: '87',   # FOREIGN BUSINESS ENTERPRISES - MICRO
    88: '88',   # FOREIGN BUSINESS ENTERPRISES - SMALL
    89: '89',   # FOREIGN BUSINESS ENTERPRISES - MEDIUM
    90: '90',   # FOREIGN GOVERNMENTS
    91: '91',   # FOREIGN CENTRAL BANKS
    92: '92',   # FRGN DIPLOMATIC REPRESENTATION IN M'SIA
    95: '95',   # FOREIGN INDIVIDUALS
    96: '96',   # FOREIGNERS EMPLOYED/STUDYING IN M'SIA
    98: '98',   # FOREIGN NON-COMMERCIAL INTERNATIONAL ORGANIZATION IN M'SIA
    99: '99',   # FOREIGN OTHER ENTITIES NIE
}


def format_odcustcd(custcode: int) -> str:
    """Format overdraft customer code"""
    return CUSTCD_BASE_MAP.get(custcode, '79')


def format_locustcd(custcode: int) -> str:
    """Format loan overdraft customer code (LOCUSTCD)
    Differs from ODCUSTCD: code 1->11, code 31->31, code 36->06,
    codes 81-84 map to themselves."""
    override = {
        1:  '11',   # BANK NEGARA MALAYSIA -> 11
        36: '06',   # STOCKBROKING COMPANIES -> 06
        31: '31',   # SAVINGS INSTITUTIONS
        81: '81',   # FOREIGN BANKING INSTITUTIONS
        82: '82',   # AFFILIATES ABROAD
        83: '83',   # G7 COUNTRIES
        84: '84',   # FOREIGN BANKS IN OTHER COUNTRIES
    }
    if custcode in override:
        return override[custcode]
    return CUSTCD_BASE_MAP.get(custcode, '79')


def format_lncustcd(custcode: int) -> str:
    """Format loan customer code (LNCUSTCD) - direct pass-through for most codes"""
    lncustcd_map: Dict[int, str] = {
        1:  '01',   # BANK NEGARA MALAYSIA
        2:  '02',   # COMMERCIAL BANKS
        3:  '03',   # ISLAMIC BANKS
        4:  '04',   # SUBSIDIARY STOCKBROKING COMPANIES
        5:  '05',   # ASSOCIATE STOCKBROKING COMPANIES
        6:  '06',   # OTHER STOCKBROKING COMPANIES
        10: '10',   # DOMESTIC BANKING INSTITUTIONS
        11: '11',   # FINANCE COMPANIES
        12: '12',   # MERCHANT BANKS
        13: '13',   # DISCOUNT HOUSES
        15: '15',   # DOMESTIC NON-BANK ENTITIES
        17: '17',   # CAGAMAS BERHAD
        20: '20',   # DOMESTIC NON-BANK FINANCIAL INSTITUTIONS
        30: '30',   # DOMESTIC OTHER NBFI
        32: '32',   # CREDIT CARD COMPANIES
        33: '33',   # DEVELOPMENT FINANCE INSTITUTIONS
        34: '34',   # BUILDING SOCIETIES
        35: '35',   # CO-OPERATIVE SOCIETIES
        36: '36',   # STOCKBROKING COMPANIES
        37: '37',   # COMMODITY BROKERS
        38: '38',   # CREDIT & LEASING COMPANIES
        39: '39',   # UNIT TRUST COMPANIES
        40: '40',   # INSURANCE AND INSURANCE RELATED COMPANIES
        41: '41',   # DBE - BUMI CONTROLLED SME - MICRO
        42: '42',   # DBE - BUMI CONTROLLED SME - SMALL
        43: '43',   # DBE - BUMI CONTROLLED SME - MEDIUM
        44: '44',   # DBE - NON-BUMI CONTROLLED SME - MICRO
        46: '46',   # DBE - NON-BUMI CONTROLLED SME - SMALL
        47: '47',   # DBE - NON-BUMI CONTROLLED SME - MEDIUM
        48: '48',   # DBE - NON-RESIDENT CONTROL SME - MICRO
        49: '49',   # DBE - NON-RESIDENT CONTROL SME - SMALL
        51: '51',   # DBE - NON-RESIDENT CONTROL SME - MEDIUM
        52: '52',   # DBE - GOVERNMENT CONTROLLED SME - MICRO
        53: '53',   # DBE - GOVERNMENT CONTROLLED SME - SMALL
        54: '54',   # DBE - GOVERNMENT CONTROLLED SME - MEDIUM
        50: '50',   # RESIDENTS/DOMESTIC ENTITIES
        57: '57',   # PETRONAS
        59: '59',   # OTHER GOVERNMENT CONTROLLED DBE NIE
        60: '60',   # DOMESTIC BUSINESS ENTERPRISES (DBE)
        61: '61',   # BUMIPUTRA CONTROLLED DBE
        62: '62',   # NON-BUMIPUTRA CONTROLLED DBE
        63: '63',   # NON-RESIDENT CONTROLLED DBE
        64: '64',   # GOVERNMENT CONTROLLED DBE
        65: '65',   # SMALL MEDIUM INDUSTRIES (SMI)
        66: '41',   # BUMIPUTRA CONTROLLED SMI
        67: '44',   # NON-BUMIPUTRA CONTROLLED SMI
        68: '48',   # NON-RESIDENT CONTROLLED SMI
        69: '52',   # GOVERNMENT CONTROLLED SMI
        70: '70',   # GOVERNMENT
        71: '71',   # FEDERAL GOVERNMENT
        72: '72',   # STATE GOVERNMENT
        73: '73',   # LOCAL GOVERNMENT
        74: '74',   # STATUTORY AUTHORITIES
        75: '75',   # NFPE
        76: '76',   # INDIVIDUALS
        77: '77',   # BUMIPUTRA
        78: '78',   # NON-BUMIPUTRA
        79: '79',   # DOMESTIC OTHER ENTITIES NIE
        80: '80',   # NON-RESIDENTS/FOREIGN ENTITIES
        81: '81',   # FOREIGN BANKING INSTITUTIONS
        85: '85',   # FOREIGN NON-BANK ENTITIES
        86: '86',   # FOREIGN BUSINESS ENTERPRISES
        87: '87',   # FOREIGN BUSINESS ENTERPRISES - MICRO
        88: '88',   # FOREIGN BUSINESS ENTERPRISES - SMALL
        89: '89',   # FOREIGN BUSINESS ENTERPRISES - MEDIUM
        90: '90',   # FOREIGN GOVERNMENTS
        91: '91',   # FOREIGN CENTRAL BANKS
        92: '92',   # FRGN DIPLOMATIC REPRESENTATION IN M'SIA
        95: '95',   # FOREIGN INDIVIDUALS
        96: '96',   # FOREIGNERS EMPLOYED/STUDYING IN M'SIA
        98: '98',   # FOREIGN NON-COMMERCIAL INTERNATIONAL ORGANIZATION IN M'SIA
        99: '99',   # FOREIGN OTHER ENTITIES NIE
    }
    return lncustcd_map.get(custcode, '79')


# ============================================================================
# BTCUSTCD FORMAT - BANKERS TRUST CUSTOMER CODE
# ============================================================================

def format_btcustcd(custcode: int) -> str:
    """Format Bankers Trust customer code (BTCUSTCD)
    Differs from LOCUSTCD: codes 66-69 map to themselves."""
    override = {
        1:  '11',   # BANK NEGARA MALAYSIA -> 11
        36: '06',   # STOCKBROKING COMPANIES -> 06
        31: '31',   # SAVINGS INSTITUTIONS
        66: '66',   # BUMIPUTRA CONTROLLED SMI (differs from LOCUSTCD)
        67: '67',   # NON-BUMIPUTRA CONTROLLED SMI (differs from LOCUSTCD)
        68: '68',   # NON-RESIDENT CONTROLLED SMI (differs from LOCUSTCD)
        69: '69',   # GOVERNMENT CONTROLLED SMI (differs from LOCUSTCD)
        81: '81',   # FOREIGN BANKING INSTITUTIONS
        82: '82',   # AFFILIATES ABROAD
        83: '83',   # G7 COUNTRIES
        84: '84',   # FOREIGN BANKS IN OTHER COUNTRIES
    }
    if custcode in override:
        return override[custcode]
    return CUSTCD_BASE_MAP.get(custcode, '79')


# ============================================================================
# STATE CODE FORMAT
# ============================================================================

STATE_CODE_MAP: Dict[str, str] = {}
# Build state code map for all zero-padded variants
for _state_num, _state_letter in [
    (1, 'J'), (2, 'K'), (3, 'D'), (4, 'M'), (5, 'N'),
    (6, 'C'), (7, 'P'), (8, 'A'), (9, 'R'),
]:
    for _n in range(1, 7):
        STATE_CODE_MAP[str(_state_num).zfill(_n)] = _state_letter

for _state_num, _state_letter in [
    (10, 'S'), (11, 'Q'), (12, 'B'), (13, 'T'), (15, 'L'),
]:
    for _n in range(2, 7):
        STATE_CODE_MAP[str(_state_num).zfill(_n)] = _state_letter

for _state_num in [14, 16]:
    for _n in range(2, 7):
        STATE_CODE_MAP[str(_state_num).zfill(_n)] = 'W'


def format_statecd(code: str) -> str:
    """Map state code to single letter"""
    if not code:
        return ' '
    return STATE_CODE_MAP.get(str(code).strip(), ' ')


# ============================================================================
# LOAN SIZE AND APPROVED LIMIT FORMATS
# ============================================================================

def format_apprlimt(amount: float) -> str:
    """Format approved limit ranges"""
    if amount < 100000:
        return '30511'
    elif amount < 500000:
        return '30512'
    elif amount < 1000000:
        return '30513'
    elif amount < 5000000:
        return '30514'
    elif amount < 20000000:
        return '30515'
    elif amount < 50000000:
        return '30516'
    else:
        return '30519'


def format_loansize(amount: float) -> str:
    """Format loan size ranges"""
    if amount < 100000:
        return '80511'
    elif amount < 500000:
        return '80512'
    elif amount < 1000000:
        return '80513'
    elif amount < 5000000:
        return '80514'
    elif amount < 20000000:
        return '80515'
    elif amount < 50000000:
        return '80516'
    else:
        return '80519'


# ============================================================================
# MTHPASS FORMAT - MONTHS PASSED DUE (based on days)
# ============================================================================

def format_mthpass(days: int) -> str:
    """Format months passed due based on days"""
    if days <= 30:
        return '0'   # DAYS PASS DUE 0-30
    elif days <= 59:
        return '1'   # DAYS PASS DUE 31-59
    elif days <= 89:
        return '2'   # DAYS PASS DUE 60-89
    elif days <= 121:
        return '3'   # DAYS PASS DUE 90-121
    elif days <= 151:
        return '4'   # DAYS PASS DUE 122-151
    elif days <= 182:
        return '5'   # DAYS PASS DUE 152-182
    elif days <= 213:
        return '6'   # DAYS PASS DUE 183-213
    elif days <= 243:
        return '7'   # DAYS PASS DUE 214-243
    elif days <= 273:
        return '8'   # DAYS PASS DUE 244-273
    elif days <= 303:
        return '9'   # DAYS PASS DUE 274-303
    elif days <= 333:
        return '10'  # DAYS PASS DUE 304-333
    elif days <= 364:
        return '11'  # DAYS PASS DUE 334-364
    elif days <= 394:
        return '12'  # DAYS PASS DUE 365-394
    elif days <= 424:
        return '13'  # DAYS PASS DUE 395-424
    elif days <= 456:
        return '14'  # DAYS PASS DUE 425-456
    elif days <= 486:
        return '15'  # DAYS PASS DUE 457-486
    elif days <= 516:
        return '16'  # DAYS PASS DUE 487-516
    elif days <= 547:
        return '17'  # DAYS PASS DUE 517-547
    elif days <= 577:
        return '18'  # DAYS PASS DUE 548-577
    elif days <= 608:
        return '19'  # DAYS PASS DUE 578-608
    elif days <= 638:
        return '20'  # DAYS PASS DUE 609-638
    elif days <= 668:
        return '21'  # DAYS PASS DUE 639-668
    elif days <= 698:
        return '22'  # DAYS PASS DUE 669-698
    elif days <= 729:
        return '23'  # DAYS PASS DUE 699-729
    else:
        return '24'  # DAYS PASS DUE 730>


# ============================================================================
# NDAYS INVALUE FORMAT - numeric equivalent of MTHPASS
# ============================================================================

def format_ndays(days: int) -> int:
    """INVALUE NDAYS - convert days to month bucket number"""
    if days <= 30:
        return 0
    elif days <= 59:
        return 1
    elif days <= 89:
        return 2
    elif days <= 121:
        return 3
    elif days <= 151:
        return 4
    elif days <= 182:
        return 5
    elif days <= 213:
        return 6
    elif days <= 243:
        return 7
    elif days <= 273:
        return 8
    elif days <= 303:
        return 9
    elif days <= 333:
        return 10
    elif days <= 364:
        return 11
    elif days <= 394:
        return 12
    elif days <= 424:
        return 13
    elif days <= 456:
        return 14
    elif days <= 486:
        return 15
    elif days <= 516:
        return 16
    elif days <= 547:
        return 17
    elif days <= 577:
        return 18
    elif days <= 608:
        return 19
    elif days <= 638:
        return 20
    elif days <= 668:
        return 21
    elif days <= 698:
        return 22
    elif days <= 729:
        return 23
    else:
        return 24


# ============================================================================
# LNORMT FORMAT - LOAN ORIGINAL MATURITY
# ============================================================================

def format_lnormt(months: float) -> str:
    """Format loan original maturity in months"""
    if months < 1:
        return '12'
    elif months < 2:
        return '13'
    elif months < 3:
        return '14'
    elif months < 6:
        return '15'
    elif months < 9:
        return '16'
    elif months < 12:
        return '17'
    elif months < 15:
        return '21'
    elif months < 18:
        return '22'
    elif months < 24:
        return '23'
    elif months < 36:
        return '24'
    elif months < 48:
        return '25'
    elif months < 60:
        return '26'
    elif months < 120:
        return '31'
    elif months < 180:
        return '32'
    else:
        return '33'


# ============================================================================
# LNRMMT FORMAT - LOAN REMAINING MATURITY
# ============================================================================

def format_lnrmmt(months: float) -> str:
    """Format loan remaining maturity in months"""
    if months < 0:
        return '51'
    elif months < 1:
        return '52'
    elif months < 2:
        return '53'
    elif months < 3:
        return '54'
    elif months < 6:
        return '55'
    elif months < 9:
        return '56'
    elif months < 12:
        return '57'
    elif months < 24:
        return '61'
    elif months < 36:
        return '62'
    elif months < 48:
        return '63'
    elif months < 60:
        return '64'
    elif months < 120:
        return '71'
    elif months < 180:
        return '72'
    else:
        return '73'


# ============================================================================
# COLLCD FORMAT - COLLATERAL CODE
# ============================================================================

COLLCD_MAP: Dict[str, str] = {
    '1':  '30570',  # GOVERNMENT & OTHER TRUSTEE
    '01': '30570',
    '2':  '30570',  # MERCHANDISE PLEDGE
    '02': '30570',
    '3':  '30570',  # HYPO TO BANK
    '03': '30570',
    '5':  '30570',  # CAR
    '05': '30570',
    '6':  '30570',  # MOTORCYCLES
    '06': '30570',
    '7':  '30570',  # CASH & FIXED DEPOSIT RECEIPTS OTHERS
    '07': '30570',
    '8':  '30520',  # PLEDGE OF QUOTED SHARES AS PRIMARY COLLATERAL
    '08': '30520',
    '9':  '30570',  # OUTPORT CHEQUES
    '09': '30570',
    '10': '30570',  # HIRE PURCHASE FINANCE
    '11': '30570',  # PACKING CREDIT (L/C)
    '12': '30570',  # PROMISSORY NOTE
    '13': '30530',  # OTHER SECURITIES
    '14': '30570',  # FIXED DEPOSITS (MARGIN LESS THAN 90%)
    '15': '30540',  # KLMF UNIT TRUST
    '20': '30580',  # UNSECURED-NEGATIVE PLEDGE/LETTER OF COMFORT
    '21': '30570',  # GUARANTEE - OTHERS
    '22': '30580',  # UNSECURED - CLEAN
    '23': '30580',  # UNSECURED - TEMPORARY
    '41': '30570',  # DEBENTURE - INSURANCE WITH LONPAC
    '42': '30570',  # DEBENTURE - INSURANCE WITH OTHER INS. CO.
    '43': '30570',  # DEBENTURE - INSURANCE PENDING
    '50': '30570',  # PROPERTY INSURED WITH LONPAC - DIRECT
    '51': '30570',  # PROPERTY INSURED WITH LONPAC - THIRD
    '52': '30570',  # PROPERTY INSURED WITH OTHER INS. CO.- DIRECT
    '53': '30570',  # PROPERTY INSURED WITH OTHER INS. CO.- THIRD
    '54': '30570',  # PROPERTIES - MIXED INS. CO. - DIRECT
    '55': '30570',  # PROPERTIES - MIXED INS. CO. - THIRD
    '56': '30570',  # INSURANCE PENDING(PROPERTY/LC)-DIRECT
    '57': '30570',  # INSURANCE PENDING(PROPERTY/LC)-THIRD
    '60': '30570',  # OTHER PROPERTIES(INS. NOT REQ)-DIRECT
    '61': '30570',  # OTHER PROPERTIES(INS. NOT REQ)-THIRD
}


def format_collcd(code: str) -> str:
    """Format collateral code"""
    return COLLCD_MAP.get(str(code).strip(), '30570')


# ============================================================================
# RISKCD FORMAT - RISK CODE
# ============================================================================

def format_riskcd(code: str) -> str:
    """Format risk code"""
    c = str(code).strip()
    if c in ('2', '02', '002', '0002'):
        return '34902'   # SUBSTANDARD
    elif c in ('3', '03', '003', '0003'):
        return '34903'   # DOUBTFUL
    elif c in ('4', '04', '004', '0004'):
        return '34904'   # BAD
    return ' '


# ============================================================================
# BUSIND FORMAT - BUSINESS INDICATOR
# ============================================================================

_BUSIND_BUSINESS_CODES = {
    '01', '02', '03', '04', '05', '06',
    '11', '12', '13',
    '17',
    '30', '31', '32', '33', '34', '35',
    '37', '38', '39', '40',
    '45',
    '57',
    '59',
    '61', '62', '63', '64',
    '66', '67', '68', '69',
    '71', '72', '73', '74', '75',
}


def format_busind(custcd: str) -> str:
    """Format business indicator - BUS or IND"""
    return 'BUS' if str(custcd).strip() in _BUSIND_BUSINESS_CODES else 'IND'


# ============================================================================
# ARRCLASS FORMAT - ARREARS CLASS
# ============================================================================

ARRCLASS_MAP: Dict[int, str] = {
    1:  '0 - < 1 MTH   ',
    2:  '1 - < 2 MTH   ',
    3:  '2 - < 3 MTH   ',
    4:  '3 - < 4 MTH   ',
    5:  '4 - < 5 MTH   ',
    6:  '5 - < 6 MTH   ',
    7:  '6 - < 7 MTH   ',
    8:  '7 - < 8 MTH   ',
    9:  '8 - < 9 MTH   ',
    10: '9 - < 12 MTH  ',
    11: '12 - < 18 MTH ',
    12: '18 - < 24 MTH ',
    13: '24 - < 36 MTH ',
    14: '36 MTH & ABOVE',
    15: 'DEFICIT       ',
}


def format_arrclass(code: int) -> str:
    """Format arrears class"""
    return ARRCLASS_MAP.get(code, '')


# ============================================================================
# DELQDES FORMAT - DELINQUENCY DESCRIPTION
# ============================================================================

DELQDES_MAP: Dict[str, str] = {
    '  ': 'NO LEGAL ACTION TAKEN',
    '09': 'LNOD/RECALL ISSUED',
    '10': 'SUMMON/WRIT FILED',
    '11': 'JUDGEMENT ORDER',
    '12': 'BANKRUPTCY',
    '13': 'CHARGING ORDER',
    '14': 'GARNISHEE ORDER',
    '15': 'WRIT OF SEIZURE AND SALE',
    '16': 'PROHIBITORY ORDER',
    '17': 'WINDING-UP',
    '18': 'AUCTION',
    '19': 'JUDGEMENT DEBTOR SUMMONS',
    '20': 'RECEIVER/SECTION 176',
    '21': 'SETTLED/DISCHARGED',
}


def format_delqdes(code: str) -> str:
    """Format delinquency description"""
    return DELQDES_MAP.get(str(code), '')


# ============================================================================
# HPCC FORMAT - HP CURRENCY CODE
# ============================================================================

HPCC_MAP: Dict[int, str] = {
    800: 'H024', 801: 'H204', 802: 'H053', 803: 'H228',  # H033 BEFORE 08032010
    804: 'H007', 805: 'H110', 806: 'H238', 807: 'H231',
    808: 'H266', 809: 'H005', 811: 'H168', 812: 'H002',
    813: 'H185', 814: 'H268', 815: 'H125', 816: 'H124',
    817: 'H006', 818: 'H040', 819: 'H264',  # H008 BEFORE 08032010
    820: 'H135', 821: 'H094', 822: 'H151', 823: 'H123',
    824: 'H057', 825: 'H270', 826: 'H042', 827: 'H277',
    828: 'H225', 844: 'H034', 845: 'H055', 846: 'H044',
    847: 'H283', 848: 'H090', 849: 'H050', 850: 'H273',
    851: 'H043', 852: 'H091', 853: 'H021', 854: 'H224',
    855: 'H061', 856: 'H202', 857: 'H209', 858: 'H113',
    859: 'H249', 860: 'H095', 861: 'H047', 862: 'H030',
    863: 'H233',
}


def format_hpcc(product: int) -> str:
    """Format HP currency code"""
    return HPCC_MAP.get(product, '')


# ============================================================================
# BTPROD FORMAT - BANKERS TRUST PRODUCT CODE
# ============================================================================

BTPROD_34152 = {'BAP', 'BAI', 'BAS', 'BAE', 'BPI', 'BII', 'BSI', 'BEI'}
BTPROD_34160 = {
    'TFL', 'TML', 'TFC', 'TMC', 'TFO', 'TMO',
    'TLL', 'TNL', 'TLC', 'TNC', 'TLO', 'TNO',
    'TLQ', 'TLZ', 'TFI', 'TBI', 'TLI', 'TXI',
    'PFT',
}
BTPROD_34600 = {'FTL', 'FTI', 'FFS', 'FFU', 'FCS', 'FCU', 'FFL'}
BTPROD_34159 = {
    'FAS', 'FAU', 'FDS', 'FDU', 'FCL', 'FTB',
    'DAS', 'DAU', 'DDS', 'DDU', 'DDT', 'ITB',
    'VAL', 'DIL', 'FIL', 'POS', 'PCR', 'PBA',
    'PRO', 'PRE', 'PBU', 'PBR', 'PUM', 'PAU',
    'PDU', 'PDT', 'PTB', 'PFU', 'PFD',
}


def format_btprod(code: str) -> str:
    """Format Bankers Trust product code"""
    c = str(code).strip()
    if c in BTPROD_34152:
        return '34152'
    elif c in BTPROD_34160:
        return '34160'
    elif c in BTPROD_34600:
        return '34600'
    return '34159'   # default (also covers BTPROD_34159 explicitly)


# ============================================================================
# BTPRODI FORMAT - BANKERS TRUST PRODUCT CODE (ISLAMIC)
# ============================================================================

BTPRODI_51100 = {'BGF'}
BTPRODI_51999 = {'APG'}
BTPRODI_52100 = {'BGP', 'BGT'}
BTPRODI_53100 = {
    'ALC', 'BFC', 'BLC', 'DLC', 'IFS', 'IFD',
    'IFU', 'IFO', 'ILB', 'ILS', 'ILL', 'ILU',
    'PLC', 'RFC', 'RLC', 'SFC', 'SLC', 'TFR',
    'TLR', 'CEX', 'UMB', 'FSI', 'FUI', 'LSI',
    'LUI', 'FSO', 'FUO', 'LSO', 'LUO',
}
BTPRODI_53200 = {'SGC', 'SGL', 'SLI', 'SCI'}
BTPRODI_53999 = {'BRA', 'BUF', 'BUL', 'GTI', 'GPI', 'GFI', 'UFI', 'UDI'}


def format_btprodi(code: str) -> str:
    """Format Bankers Trust product code (Islamic)"""
    c = str(code).strip()
    if c in BTPRODI_51100:
        return '51100'
    elif c in BTPRODI_51999:
        return '51999'
    elif c in BTPRODI_52100:
        return '52100'
    elif c in BTPRODI_53100:
        return '53100'
    elif c in BTPRODI_53200:
        return '53200'
    elif c in BTPRODI_53999:
        return '53999'
    return ''


# ============================================================================
# BTPRODA FORMAT - BANKERS TRUST PRODUCT CODE (ALTERNATIVE)
# ============================================================================

BTPRODA_53100 = {
    'ALC', 'BFC', 'BLC', 'DLC', 'IFS', 'IFD',
    'IFU', 'IFO', 'ILB', 'ILS', 'ILL', 'ILU',
    'PLC', 'RFC', 'RLC', 'SFC', 'SLC', 'TFR',
    'TLR', 'CEX', 'UMB',
}
BTPRODA_53200 = {'SGC', 'SGL'}
BTPRODA_53999 = {'BRA', 'BUF', 'BUL'}


def format_btproda(code: str) -> str:
    """Format Bankers Trust product code (Alternative - subset of BTPRODI)"""
    c = str(code).strip()
    if c in BTPRODI_51100:
        return '51100'
    elif c in BTPRODI_51999:
        return '51999'
    elif c in BTPRODI_52100:
        return '52100'
    elif c in BTPRODA_53100:
        return '53100'
    elif c in BTPRODA_53200:
        return '53200'
    elif c in BTPRODA_53999:
        return '53999'
    return ''


# ============================================================================
# BTRATE FORMAT - BANKERS TRUST RATE TYPE
# ============================================================================

BTRATE_30596 = {
    'IEF', 'MDL', 'MLL', 'MOL', 'MOF', 'MCL', 'MCF',
    'PEF', 'PCE', 'PCP', 'MFL',
    'BPI', 'BII', 'BSI', 'BEI',
}
BTRATE_30593 = {'BAP', 'BAI', 'BAS', 'BAE'}  # FIXED RATE


def format_btrate(code: str) -> str:
    """Format Bankers Trust rate type"""
    c = str(code).strip()
    if c in BTRATE_30596:
        return '30596'
    elif c in BTRATE_30593:
        return '30593'   # FIXED RATE
    return '30597'


# ============================================================================
# LNFMT FORMAT - LOAN FORMAT TYPE
# ============================================================================

LNFMT_LSTAFF = set(
    list(range(4, 8)) + [15, 20] + list(range(25, 35)) +
    list(range(60, 64)) + list(range(70, 79)) + [100]
)
LNFMT_LHOUSE = set(
    list(range(110, 120)) +
    [124, 139, 140, 141, 142, 145, 147, 173] +
    [200, 201, 204, 205] + list(range(209, 221)) +
    [225, 226, 227, 228] + list(range(230, 249))
)
LNFMT_LBAEPL = set([135, 136, 138])
LNFMT_LSWFSX = set([126, 127, 129, 359, 361, 363, 906, 907])
LNFMT_LHPPRD = set([128, 130, 380, 381, 700, 705, 720, 725, 983, 993])


def format_lnfmt(product: int) -> str:
    """Format loan format type"""
    if product in LNFMT_LSTAFF:
        return 'LSTAFF'
    elif product in LNFMT_LHOUSE:
        return 'LHOUSE'
    elif product in LNFMT_LBAEPL:
        return 'LBAEPL'
    elif product in LNFMT_LSWFSX:
        return 'LSWFSX'
    elif product in LNFMT_LHPPRD:
        return 'LHPPRD'
    return 'LOT_OT'


# ============================================================================
# LNLOB FORMAT - LOAN LINE OF BUSINESS
# ============================================================================

LNLOB_CORP = set(
    list(range(180, 185)) + [193] +
    list(range(900, 906)) + list(range(908, 911)) +
    [914, 915, 919, 920, 925, 950, 951, 906, 907]
)
LNLOB_HPOP = set([128, 130, 380, 381, 700, 705, 720, 725, 983, 993])


def format_lnlob(product: int) -> str:
    """Format loan line of business"""
    if product in LNLOB_CORP:
        return 'CORP'
    elif product in LNLOB_HPOP:
        return 'HPOP'
    return 'RETL'


# ============================================================================
# ODFMT FORMAT - OD FORMAT TYPE
# ============================================================================

ODFMT_OFOREX = set([171, 172, 173])
ODFMT_OOTHER = set([147, 126, 127, 128, 129, 144, 145])
ODFMT_LSHMRG = set([113, 177, 178])


def format_odfmt(product: int) -> str:
    """Format OD format type"""
    if product in ODFMT_OFOREX:
        return 'OFOREX'
    elif product in ODFMT_OOTHER:
        return 'OOTHER'
    elif product in ODFMT_LSHMRG:
        return 'LSHMRG'
    return 'LOT_OT'


# ============================================================================
# ODLOB FORMAT - OD LINE OF BUSINESS
# ============================================================================

ODLOB_TREA = set([171, 172, 173])
ODLOB_HDOF = set([147])
ODLOB_CORP = set(range(50, 66))  # 050-065


def format_odlob(product: int) -> str:
    """Format OD line of business"""
    if product in ODLOB_TREA:
        return 'TREA'
    elif product in ODLOB_HDOF:
        return 'HDOF'
    elif product in ODLOB_CORP:
        return 'CORP'
    return 'RETL'


# ============================================================================
# FISSTYPE FORMAT - FISS TYPE
# ============================================================================

def _build_fisstype_map() -> Dict[str, str]:
    m: Dict[str, str] = {}
    for k in ['1100', '1111', '1112', '1113', '1114', '1115',
              '1116', '1117', '1119', '1120', '1130', '1140', '1150']:
        m[k] = '1100'
    for k in ['2200', '2210', '2220']:
        m[k] = '2200'
    for k in ['2300', '2301', '2302', '2303']:
        m[k] = '2300'
    for k in ['3100', '3110', '3115', '3111', '3112', '3113', '3114']:
        m[k] = '3100'
    for k in ['3210', '3211', '3212', '3219']:
        m[k] = '3210'
    for k in ['3220', '3221', '3222']:
        m[k] = '3220'
    for k in ['3230', '3231', '3232']:
        m[k] = '3230'
    for k in ['3240', '3241', '3242']:
        m[k] = '3240'
    for k in ['3260', '3270', '3271', '3272', '3273', '3280', '3290']:
        m[k] = '3260'
    for k in ['3310', '3311', '3312', '3313']:
        m[k] = '3310'
    for k in ['3430', '3431', '3432', '3433']:
        m[k] = '3430'
    for k in ['3550', '3551', '3552']:
        m[k] = '3550'
    for k in ['3610', '3611', '3619']:
        m[k] = '3610'
    for k in ['3700', '3710', '3720', '3721', '3730', '3731', '3732']:
        m[k] = '3700'
    for k in ['3800', '3811', '3812', '3813', '3814', '3819']:
        m[k] = '3800'
    for k in ['3831', '3832', '3833', '3834', '3835']:
        m[k] = '3831'
    for k in ['3841', '3842', '3843', '3844']:
        m[k] = '3841'
    for k in ['3850', '3851', '3852', '3853']:
        m[k] = '3850'
    for k in ['3860', '3861', '3862', '3863', '3864', '3865', '3866']:
        m[k] = '3860'
    for k in ['3870', '3871', '3872', '3873']:
        m[k] = '3870'
    for k in ['3890', '3891', '3892', '3893', '3894']:
        m[k] = '3890'
    for k in ['3910', '3911', '3919']:
        m[k] = '3910'
    for k in ['3905', '3951', '3952', '3953', '3954', '3955', '3956', '3957']:
        m[k] = '3950'
    for k in ['7110', '7111', '7112', '7113', '7114', '7115', '7116', '7117']:
        m[k] = '7110'
    for k in ['7120', '7121', '7122', '7123', '7124']:
        m[k] = '7120'
    for k in ['7130', '7131', '7132', '7133', '7134']:
        m[k] = '7130'
    for k in ['7190', '7191', '7192', '7193', '7199']:
        m[k] = '7190'
    for k in ['7200', '7210', '7220']:
        m[k] = '7200'
    for k in ['8100', '8110', '8120', '8130']:
        m[k] = '8100'
    for k in ['8300', '8310', '8330', '8320', '8321', '8331', '8332', '8333', '8340']:
        m[k] = '8300'
    for k in ['8400', '8410', '8411', '8412', '8413', '8414', '8415', '8416', '8420']:
        m[k] = '8400'
    for k in ['8900', '8910', '8911', '8912', '8913', '8914',
              '8920', '8921', '8922', '8930', '8931', '8932',
              '8990', '8991', '8999']:
        m[k] = '8900'
    for k in ['9100', '9101', '9102', '9103']:
        m[k] = '9100'
    for k in ['9200', '9201', '9202', '9203']:
        m[k] = '9200'
    for k in ['9300', '9311', '9312', '9313', '9314']:
        m[k] = '9300'
    for k in ['9400', '9410', '9420', '9430', '9431', '9432',
              '9433', '9434', '9435', '9440', '9450', '9499']:
        m[k] = '9400'
    return m


FISSTYPE_MAP: Dict[str, str] = _build_fisstype_map()


def format_fisstype(code: str) -> str:
    """Format FISS type"""
    return FISSTYPE_MAP.get(str(code).strip(), '')


# ============================================================================
# FISSGROUP FORMAT - FISS GROUP
# ============================================================================

def _build_fissgroup_map() -> Dict[str, str]:
    m: Dict[str, str] = {}
    for k in ['1100', '1111', '1112', '1113', '1114', '1115',
              '1116', '1117', '1119', '1120', '1130', '1140',
              '1150', '1200', '1300', '1400']:
        m[k] = '1000'
    for k in ['2100', '2200', '2210', '2220', '2300', '2301',
              '2302', '2303', '2400', '2900']:
        m[k] = '2000'
    for k in ['3100', '3110', '3115', '3111', '3112', '3113',
              '3114', '3120', '3210', '3211', '3212', '3219',
              '3220', '3221', '3222', '3230', '3231', '3232',
              '3240', '3241', '3242', '3250', '3260', '3270',
              '3271', '3272', '3273', '3280', '3290', '3310',
              '3311', '3312', '3313', '3430', '3431', '3432',
              '3433', '3550', '3551', '3552', '3610', '3611',
              '3619', '3700', '3710', '3720', '3721', '3730',
              '3731', '3732', '3800', '3811', '3812', '3813',
              '3814', '3819', '3825', '3831', '3832', '3833',
              '3834', '3835', '3841', '3842', '3843', '3844',
              '3850', '3851', '3852', '3853', '3860', '3861',
              '3862', '3863', '3864', '3865', '3866', '3870',
              '3871', '3872', '3873', '3890', '3891', '3892',
              '3893', '3894', '3910', '3911', '3919', '3905',
              '3951', '3952', '3953', '3954', '3955', '3956',
              '3957', '3960']:
        m[k] = '3000'
    for k in ['4010', '4020', '4030']:
        m[k] = '4000'
    for k in ['5010', '5001', '5002', '5003', '5004', '5005',
              '5006', '5008', '5020', '5030', '5040', '5050', '5999']:
        m[k] = '5000'
    for k in ['6100', '6110', '6120', '6130', '6300', '6310', '6320']:
        m[k] = '6000'
    for k in ['7110', '7111', '7112', '7113', '7114', '7115',
              '7116', '7117', '7120', '7121', '7122', '7123',
              '7124', '7130', '7131', '7132', '7133', '7134',
              '7190', '7191', '7192', '7193', '7199', '7200',
              '7210', '7220']:
        m[k] = '7000'
    for k in ['8100', '8110', '8120', '8130', '8300', '8310',
              '8330', '8320', '8321', '8331', '8332', '8333',
              '8340', '8400', '8410', '8411', '8412', '8413',
              '8414', '8415', '8416', '8420', '8900', '8910',
              '8911', '8912', '8913', '8914', '8920', '8921',
              '8922', '8930', '8931', '8932', '8990', '8991', '8999']:
        m[k] = '8000'
    for k in ['9100', '9101', '9102', '9103', '9200', '9201',
              '9202', '9203', '9300', '9311', '9312', '9313',
              '9314', '9400', '9410', '9420', '9430', '9431',
              '9432', '9433', '9434', '9435', '9440', '9450',
              '9499', '9500', '9600']:
        m[k] = '9000'
    return m


FISSGROUP_MAP: Dict[str, str] = _build_fissgroup_map()


def format_fissgroup(code: str) -> str:
    """Format FISS group"""
    return FISSGROUP_MAP.get(str(code).strip(), '')


# ============================================================================
# SECTCD FORMAT - SECTOR CODE REMAP
# ============================================================================

SECTCD_MAP: Dict[str, str] = {
    '1112': '1119',
    '1114': '1119',
    '1116': '1119',
    '1130': '1120',
    '1140': '1120',
    '1150': '1120',
    '3111': '3114',
    '3112': '3114',
    '8410': '8416',
    '8910': '8914',
    '8920': '8922',
    '9440': '9430',
    '9450': '9430',
}


def format_sectcd(code: str) -> str:
    """Format sector code (SECTCD remap)"""
    return SECTCD_MAP.get(str(code).strip(), str(code).strip())


# ============================================================================
# SECDES FORMAT - SECTOR DESCRIPTION
# ============================================================================

SECDES_MAP: Dict[str, str] = {
    '0100': 'SECURITIES                 ',
    '0200': 'TRANSPORT VEHICLES         ',
    '0310': 'RESIDENTIAL PROPERTY       ',
    '0311': 'RESIDE PROP COST <=25K     ',
    '0312': 'RESIDE PROP COST >25K-60K  ',
    '0313': 'RESIDE PROP COST >60K-100K ',
    '0314': 'RESIDE PROP COST >100K-150K',
    '0315': 'RESIDE PROP COST >150K-250K',
    '0316': 'RESIDE PROP COST >250K     ',
    '0320': 'NON RESI. PROPERTY         ',
    '0321': 'PURCH IND BUILD & FACTORIES',
    '0322': 'PURCH OF LAND ONLY         ',
    '0323': 'PURCH OF COMMERCIAL COMPLEX',
    '0324': 'PURCH OF SHOPHOUSES        ',
    '0329': 'PURCH OTH NRESIDE PROP     ',
    '0410': 'PERSONAL USES              ',
    '0420': 'CREDIT CARD                ',
    '0430': 'CONSUMER DURABLES          ',
    '1000': 'AGRICULTURE, HUNTING, FORESTRY & FISHING         ',
    '2000': 'MINING & QUARRYING         ',
    '3000': 'MANUFACTURING              ',
    '4000': 'ELEC, GAS & WATER SUPPLY   ',
    '5000': 'CONSTRUCTION',
    '5001': 'GEN INCL CIVIL ENGINE CONT ',
    '5002': 'SPECIAL TRADE CONTRACTORS  ',
    '5003': 'INDUST BUILD & FACTORIES   ',
    '5004': 'CONSTRUCT INFRASTRUCTURE   ',
    '5005': 'CONSTRUCT COMMERCIAL COMPL ',
    '5006': 'CONSTRUCT RESIDENTIAL      ',
    '5009': 'CONSTRUCTION NEC           ',
    '6100': 'WHOLESALE',
    '6200': 'RETAIL TRADE',
    '6300': 'RESTAURANTS & HOTELS',
    '7000': 'TRANSPORT, STORAGE & COMM  ',
    '8100': 'FINANCIAL SERVICES',
    '8200': 'INSURANCE',
    '8310': 'REAL ESTATE',
    '8320': 'BUSINESS SERVICES',
    '8330': 'EQUIPMENT RENTAL',
    '9000': 'COMMUNITY, SOCIAL & PERSONAL SERVICES          ',
    '9999': 'SECTORS NEC                ',
}


def format_secdes(code: str) -> str:
    """Format sector description"""
    return SECDES_MAP.get(str(code).strip(), ' ')


# ============================================================================
# SECTA FORMAT - SECTOR A GROUPING
# ============================================================================

def format_secta(code: str) -> str:
    """Format sector A grouping"""
    c = str(code).strip()
    if '0110' <= c <= '0139':
        return '0100'   # SECURITIES
    elif '0210' <= c <= '0230':
        return '0200'   # TRANSPORT VEHICLES
    elif c == '0311':
        return '0311'   # RESIDE PROP COST <=25K
    elif c == '0312':
        return '0312'
    elif c == '0313':
        return '0313'
    elif c == '0314':
        return '0314'
    elif c == '0315':
        return '0315'
    elif c == '0316':
        return '0316'
    elif c == '0321':
        return '0321'
    elif c == '0322':
        return '0322'
    elif c == '0323':
        return '0323'
    elif c == '0324':
        return '0324'
    elif c == '0329':
        return '0329'
    elif c == '0410':
        return '0410'   # PERSONAL USES
    elif c == '0420':
        return '0420'   # CREDIT CARD
    elif c == '0430':
        return '0430'   # CONSUMER DURABLES
    elif '1100' <= c <= '1400':
        return '1000'   # AGRICULTURE, HUNTING, FORESTRY & FISHING
    elif '2100' <= c <= '2909':
        return '2000'   # MINING & QUARRYING
    elif '3100' <= c <= '3909':
        return '3000'   # MANUFACTURING
    elif '4100' <= c <= '4200':
        return '4000'   # ELEC, GAS & WATER SUPPLY
    elif c == '5001':
        return '5001'
    elif c == '5002':
        return '5002'
    elif c == '5003':
        return '5003'
    elif c == '5004':
        return '5004'
    elif c == '5005':
        return '5005'
    elif c == '5006':
        return '5006'
    elif c == '5009':
        return '5009'
    elif '7100' <= c <= '7220':
        return '7000'   # TRANSPORT, STORAGE & COMM
    elif '9100' <= c <= '9600':
        return '9000'   # COMMUNITY, SOCIAL & PERSONAL SERVICES
    elif c == '9999':
        return '9999'   # SECTORS NEC
    return ' '


# ============================================================================
# SECTB FORMAT - SECTOR B GROUPING
# ============================================================================

def format_sectb(code: str) -> str:
    """Format sector B grouping"""
    c = str(code).strip()
    if '0210' <= c <= '0212':
        return '0210'   # PASSENGER CARS
    elif '6110' <= c <= '6150':
        return '6100'   # WHOLESALE TRADE
    elif '6210' <= c <= '6250':
        return '6200'   # RETAIL TRADE
    elif '6310' <= c <= '6320':
        return '6300'   # RESTAURANTS & HOTELS
    elif c == '8100':
        return '8100'   # FINANCIAL SERVICES
    elif c == '8200':
        return '8200'   # INSURANCE
    elif c == '8310':
        return '8310'   # REAL ESTATE
    elif '8321' <= c <= '8329':
        return '8320'   # BUSINESS SERVICES
    elif c == '8330':
        return '8330'   # EQUIPMENT RENTAL
    return ' '


# ============================================================================
# INDSECT FORMAT - INDUSTRY SECTOR CODE (5-digit MSIC to 4-digit sector)
# ============================================================================

# This is a large mapping from 5-digit MSIC codes to 4-digit sector codes.
# Built as a dictionary for performance.
INDSECT_MAP: Dict[str, str] = {
    '01291': '1111', '01292': '1111',
    '01120': '1112',
    '01261': '1113', '01262': '1113',
    '01263': '1114',
    '01273': '1115',
    '01140': '1116',
    **{k: '1117' for k in [
        '01131', '01132', '01133', '01134', '01135', '01136',
        '01191', '01192', '01210', '01221', '01222', '01223',
        '01224', '01225', '01226', '01227', '01228', '01229',
        '01231', '01232', '01233', '01239', '01241', '01249',
        '01251', '01252', '01253', '01259', '01269', '01271',
        '01272', '01279', '01281', '01282', '01283', '01284',
        '01285', '01293', '01294', '01295', '01296', '01301',
        '01302', '01303', '01304',
    ]},
    **{k: '1119' for k in [
        '01111', '01112', '01113', '01119', '01137', '01138',
        '01150', '01160', '01193', '01199', '01289', '01299',
    ]},
    **{k: '1120' for k in [
        '01411', '01412', '01413', '01420', '01430', '01441',
        '01442', '01443', '01450', '01461', '01462', '01463',
        '01464', '01465', '01466', '01467', '01468', '01469',
        '01491', '01492', '01493', '01494', '01495', '01496',
        '01497', '01499', '03225',
    ]},
    **{k: '1130' for k in [
        '01610', '01620', '01631', '01632', '01633', '01634', '01640',
    ]},
    **{k: '1150' for k in ['01701', '01702']},
    **{k: '1200' for k in [
        '02101', '02102', '02103', '02104', '02105', '02201',
        '02202', '02203', '02204', '02301', '02302', '02303',
        '02309', '02401', '02402',
    ]},
    **{k: '1300' for k in [
        '03111', '03112', '03113', '03114', '03115', '03119',
        '03121', '03122', '03123', '03124', '03129', '03211',
        '03212', '03213', '03214', '03215', '03216', '03217',
        '03218', '03219', '03221', '03222', '03223', '03224', '03229',
    ]},
    '01500': '1400',
    **{k: '2100' for k in ['05100', '05200', '08921', '08922', '08923']},
    **{k: '2210' for k in [
        '06101', '06102', '06103', '06104', '06201', '06202',
        '06203', '06204', '06205',
    ]},
    **{k: '2220' for k in ['09101', '09102']},
    **{k: '2301' for k in ['07101', '07102']},
    **{k: '2302' for k in [
        '07292', '07293', '07294', '07295', '07296', '07297', '07298', '07299',
    ]},
    '07291': '2303',
    '07210': '2400',
    **{k: '2900' for k in [
        '08101', '08102', '08103', '08104', '08105', '08106',
        '08107', '08108', '08109', '08911', '08912', '08913',
        '08914', '08915', '08916', '08917', '08918', '08931',
        '08932', '08933', '08991', '08992', '08993', '08994',
        '08995', '08996', '08999', '09900',
    ]},
    **{k: '3110' for k in [
        '10101', '10102', '10103', '10104', '10109', '10201',
        '10202', '10203', '10204', '10301', '10302', '10303',
        '10304', '10306', '10404', '10405', '10406', '10407',
    ]},
    **{k: '3111' for k in ['10501', '10502', '10509']},
    **{k: '3112' for k in [
        '10611', '10612', '10613', '10619', '10621', '10622', '10623', '10800',
    ]},
    **{k: '3113' for k in ['11010', '11020', '11030', '11041', '11042']},
    **{k: '3114' for k in [
        '10305', '10711', '10712', '10713', '10714', '10721',
        '10722', '10731', '10732', '10733', '10741', '10742',
        '10750', '10791', '10792', '10793', '10794', '10795',
        '10799', '35303', '10205',
    ]},
    **{k: '3115' for k in ['10401', '10402', '10403']},
    '12000': '3120',
    **{k: '3211' for k in ['13110', '13120', '13131', '13132', '13139', '13910']},
    '14300': '3212',
    **{k: '3219' for k in ['13921', '13922', '13930', '13940', '13990']},
    **{k: '3221' for k in ['14101', '14102', '14103', '14109']},
    '14200': '3222',
    **{k: '3231' for k in ['15110', '15120']},
    **{k: '3232' for k in ['15201', '15202', '15203', '15209']},
    '16100': '3241',
    **{k: '3242' for k in ['16211', '16212', '16221', '16222', '16230', '16291', '16292']},
    **{k: '3250' for k in ['17010', '17020', '17091', '17092', '17093', '17094', '17099']},
    '58110': '3271',
    '58130': '3272',
    **{k: '3273' for k in ['58120', '58190']},
    **{k: '3280' for k in ['18110', '18120']},
    '18200': '3290',
    **{k: '3311' for k in [
        '20111', '20112', '20113', '20119', '20121', '20129', '20131', '20132', '20133',
    ]},
    '20300': '3312',
    **{k: '3313' for k in [
        '20210', '20221', '20222', '20231', '20232', '20291',
        '20292', '20299', '21001', '21002', '21003', '21004',
        '21005', '21006', '21007', '21009',
    ]},
    '19100': '3431',
    **{k: '3432' for k in ['19201', '19202']},
    **{k: '3551' for k in ['22111', '22112', '22191', '22192', '22193', '22199']},
    **{k: '3552' for k in ['22201', '22202', '22203', '22204', '22205', '22209']},
    **{k: '3611' for k in ['23101', '23109']},
    **{k: '3619' for k in [
        '23911', '23912', '23921', '23929', '23930', '23941',
        '23942', '23951', '23952', '23953', '23959', '23960', '23990',
    ]},
    **{k: '3710' for k in ['24101', '24102', '24103', '24104', '24109']},
    **{k: '3720' for k in ['24202', '24209']},
    '24201': '3721',
    **{k: '3731' for k in ['24311', '24312']},
    '24320': '3732',
    **{k: '3811' for k in ['25111', '25112', '25113', '25119', '25120', '25130', '33110']},
    '25910': '3813',
    '25920': '3814',
    **{k: '3819' for k in ['25930', '25991', '25992', '25993', '25994', '25999']},
    **{k: '3825' for k in ['26201', '26202', '28170']},
    **{k: '3832' for k in ['25200', '28210', '28230', '28240', '28250', '28260', '28290', '30400']},
    '28220': '3833',
    **{k: '3834' for k in [
        '28110', '28120', '28130', '28140', '28150', '28160',
        '28180', '28191', '28192', '28199', '33120', '33200',
    ]},
    '27500': '3835',
    **{k: '3842' for k in ['26101', '26102', '26103', '26104', '26105', '26109']},
    '26300': '3843',
    '26400': '3844',
    **{k: '3851' for k in ['23102', '26511', '26512', '26600', '32500', '33131', '33132']},
    **{k: '3852' for k in ['26701', '26702', '26800', '27310', '33133']},
    '26520': '3853',
    '27101': '3861',
    '27102': '3862',
    **{k: '3863' for k in ['27320', '27330']},
    '27200': '3864',
    '27400': '3865',
    **{k: '3866' for k in ['27900', '33140']},
    **{k: '3871' for k in ['29101', '29102']},
    '29200': '3872',
    '29300': '3873',
    **{k: '3891' for k in ['30110', '30120']},
    '30200': '3892',
    '30300': '3893',
    **{k: '3894' for k in ['30910', '30920', '30990', '33150']},
    **{k: '3911' for k in ['31001', '31002', '31003', '31009']},
    **{k: '3919' for k in ['32110', '32120', '32200', '32300', '32400', '32901', '32909', '33190']},
    '38301': '3952',
    '38302': '3953',
    **{k: '3956' for k in ['38303', '38304']},
    '38309': '3957',
    '38114': '3960',
    **{k: '4010' for k in ['35101', '35102', '36001', '36002']},
    **{k: '4020' for k in ['35201', '35202', '35203']},
    **{k: '4030' for k in ['35301', '35302']},
    '41009': '5001',
    '41003': '5002',
    '41002': '5003',
    **{k: '5004' for k in ['42101', '42102', '42103', '42104', '42105', '42106', '42109']},
    '41001': '5006',
    **{k: '5008' for k in [
        '42201', '42202', '42203', '42204', '42205', '42206',
        '42207', '42209', '42901', '42902', '42903', '42904',
        '42905', '42906', '42909', '43901', '43902', '43903', '43904', '43905',
    ]},
    **{k: '5020' for k in ['43110', '43121', '43122', '43123', '43124', '43125', '43126', '43129']},
    **{k: '5030' for k in [
        '43211', '43212', '43213', '43214', '43215', '43216',
        '43219', '43221', '43222', '43223', '43224', '43225',
        '43226', '43227', '43228', '43229', '43291', '43293',
        '43294', '43295', '43299',
    ]},
    **{k: '5040' for k in [
        '43292', '43301', '43302', '43303', '43304', '43305',
        '43306', '43307', '43309', '43906',
    ]},
    '43907': '5050',
    '43909': '5999',
    **{k: '6110' for k in [
        '45101', '45102', '45103', '45104', '45105', '45106',
        '45109', '45201', '45202', '45203', '45204', '45205',
        '45300', '45401', '45402', '45403',
    ]},
    **{k: '6120' for k in [
        '46100', '46201', '46202', '46203', '46204', '46205',
        '46209', '46311', '46312', '46313', '46314', '46319',
        '46321', '46322', '46323', '46324', '46325', '46326',
        '46327', '46329', '46411', '46412', '46413', '46414',
        '46415', '46416', '46417', '46419', '46421', '46422',
        '46431', '46432', '46433', '46434', '46441', '46442',
        '46443', '46444', '46491', '46492', '46493', '46494',
        '46495', '46496', '46497', '46499', '46510', '46521',
        '46522', '46531', '46532', '46591', '46592', '46593',
        '46594', '46595', '46596', '46599', '46611', '46612',
        '46619', '46621', '46622', '46631', '46632', '46633',
        '46634', '46635', '46636', '46637', '46639', '46691',
        '46692', '46693', '46694', '46695', '46696', '46697',
        '46698', '46699', '46901', '46902', '46909',
    ]},
    **{k: '6130' for k in [
        '47111', '47112', '47113', '47114', '47191', '47192',
        '47193', '47194', '47199', '47211', '47212', '47213',
        '47214', '47215', '47216', '47217', '47219', '47221',
        '47222', '47230', '47300', '47411', '47412', '47413',
        '47420', '47510', '47520', '47531', '47532', '47533',
        '47591', '47592', '47593', '47594', '47595', '47596',
        '47597', '47598', '47611', '47612', '47620', '47631',
        '47632', '47633', '47634', '47635', '47640', '47711',
        '47712', '47713', '47721', '47722', '47731', '47732',
        '47733', '47734', '47735', '47736', '47737', '47738',
        '47739', '47741', '47742', '47743', '47744', '47749',
        '47810', '47820', '47891', '47892', '47893', '47894',
        '47895', '47911', '47912', '47913', '47914', '47991',
        '47992', '47999', '95111', '95121', '95122', '95123',
        '95124', '95125', '95126', '95127', '95211', '95212',
        '95213', '95214', '95221', '95222', '95230', '95240',
        '95291', '95292', '95293', '95294', '95295', '95296', '95299',
    ]},
    **{k: '6310' for k in [
        '56101', '56102', '56103', '56104', '56105', '56106',
        '56107', '56210', '56290', '56301', '56302', '56303', '56304', '56309',
    ]},
    **{k: '6320' for k in [
        '55101', '55102', '55103', '55104', '55105', '55106',
        '55107', '55108', '55109', '55200', '55900',
    ]},
    **{k: '7111' for k in ['49110', '49120']},
    **{k: '7112' for k in ['49223', '49229']},
    **{k: '7113' for k in ['49211', '49212', '49221']},
    '49222': '7114',
    **{k: '7115' for k in ['49224', '49225']},
    '49230': '7116',
    '49300': '7117',
    **{k: '7121' for k in ['50111', '50112', '50113']},
    **{k: '7122' for k in ['50121', '50122']},
    **{k: '7123' for k in ['50211', '50212']},
    '50220': '7124',
    '51101': '7131',
    '51201': '7132',
    **{k: '7133' for k in ['51102', '51103', '51203']},
    '51202': '7134',
    '52291': '7191',
    '52100': '7192',
    **{k: '7193' for k in ['52241', '52249']},
    **{k: '7199' for k in [
        '52211', '52212', '52213', '52214', '52219', '52221',
        '52222', '52229', '52231', '52232', '52233', '52234',
        '52239', '52292', '52299', '79110', '79120', '79900',
    ]},
    **{k: '7210' for k in ['53100', '53200', '82195']},
    **{k: '7220' for k in [
        '61101', '61102', '61201', '61202', '61300', '61901',
        '61902', '61903', '61904', '61905', '61909',
    ]},
    **{k: '8110' for k in [
        '64110', '64191', '64192', '64193', '64194', '64195',
        '64199', '64200', '64301', '64302', '64303', '64304',
        '64309', '64910', '64921', '64922', '64923', '64924',
        '64925', '64929', '64991', '64992', '64993', '64999',
    ]},
    **{k: '8120' for k in [
        '66111', '66112', '66113', '66114', '66119', '66121',
        '66122', '66123', '66124', '66125', '66129', '66191',
        '66192', '66199', '66301', '66302', '66303',
    ]},
    **{k: '8130' for k in [
        '65111', '65112', '65121', '65122', '65123', '65124',
        '65125', '65201', '65202', '65203', '65204', '65205',
        '65206', '65207', '65301', '65302', '66211', '66212',
        '66221', '66222', '66223', '66224', '66290',
    ]},
    **{k: '8310' for k in [
        '68101', '68102', '68103', '68104', '68109', '68201', '68202', '68203', '68209',
    ]},
    **{k: '8320' for k in [
        '77211', '77212', '77213', '77219', '77220', '77291',
        '77293', '77294', '77295', '77296', '77297', '77299',
    ]},
    '77292': '8321',
    **{k: '8331' for k in ['77101', '77102', '77302', '77303', '77304']},
    **{k: '8332' for k in ['77301', '77305', '77306', '77309']},
    '77307': '8333',
    **{k: '8340' for k in [
        '58201', '58202', '58203', '62010', '62021', '62022',
        '62091', '62099', '63111', '63112', '63120', '95112', '95113',
    ]},
    '72101': '8411',
    '72104': '8412',
    **{k: '8413' for k in ['72102', '72106']},
    '72105': '8414',
    '72103': '8415',
    '72109': '8416',
    **{k: '8420' for k in ['72201', '72202', '72209']},
    **{k: '8910' for k in ['69100', '69200']},
    '73200': '8911',
    **{k: '8912' for k in ['70100', '70201']},
    '70203': '8913',
    **{k: '8914' for k in ['70202', '70209']},
    '71200': '8921',
    **{k: '8922' for k in ['71101', '71102', '71103', '71109']},
    '73100': '8932',
    '82920': '8991',
    **{k: '8999' for k in [
        '74101', '74102', '74103', '74109', '74200', '74901',
        '74902', '74903', '74904', '74905', '74909', '78100',
        '78200', '78300', '80100', '80200', '80300', '81100',
        '81210', '81291', '81292', '81293', '81294', '81295',
        '81296', '81297', '81299', '81300', '82110', '82191',
        '82192', '82193', '82194', '82196', '82199', '82200',
        '82301', '82302', '82910', '82990', '77400',
    ]},
    **{k: '9101' for k in [
        '84111', '84112', '84121', '84122', '84123', '84124',
        '84125', '84126', '84129', '84131', '84132', '84133',
        '84134', '84135', '84136', '84137', '84138', '84139',
    ]},
    **{k: '9102' for k in [
        '84210', '84220', '84231', '84232', '84233', '84234', '84235', '84236', '84239',
    ]},
    '84300': '9103',
    **{k: '9201' for k in [
        '86101', '86102', '86201', '86202', '86203', '86901',
        '86902', '86903', '86904', '86905', '86906', '86909',
        '87101', '87102', '87103',
    ]},
    '75000': '9202',
    **{k: '9203' for k in [
        '87201', '87209', '87300', '87901', '87902', '87909',
        '88101', '88109', '88901', '88902', '88909',
    ]},
    **{k: '9311' for k in ['85101', '85102', '85103', '85104']},
    **{k: '9312' for k in ['85211', '85212', '85221', '85222']},
    **{k: '9313' for k in ['85301', '85302']},
    **{k: '9314' for k in [
        '85411', '85412', '85419', '85421', '85429', '85491',
        '85492', '85493', '85494', '85499', '85500',
    ]},
    **{k: '9410' for k in [
        '37000', '38111', '38112', '38113', '38115', '38121',
        '38122', '38210', '38220', '39000',
    ]},
    **{k: '9420' for k in ['94110', '94120', '94200', '94910', '94920', '94990']},
    **{k: '9431' for k in [
        '59110', '59120', '59130', '59140', '59200', '60100',
        '60200', '90001', '90002', '90003', '90004', '90005',
        '90006', '90007', '90009', '93295',
    ]},
    '63910': '9432',
    '63990': '9435',
    **{k: '9440' for k in ['91011', '91012', '91021', '91022', '91031', '91032']},
    **{k: '9450' for k in [
        '92000', '93111', '93112', '93113', '93114', '93115',
        '93116', '93117', '93118', '93119', '93120', '93191',
        '93192', '93193', '93199', '93210', '93291', '93292',
        '93293', '93294', '93296', '93297', '93299',
    ]},
    **{k: '9499' for k in [
        '96011', '96012', '96013', '96014', '96020', '96031',
        '96032', '96033', '96034', '96035', '96091', '96092',
        '96093', '96094', '96095', '96096', '96097', '96099',
    ]},
    **{k: '9500' for k in ['97000', '98100', '98200']},
    '99000': '9600',
}


def format_indsect(code: str) -> str:
    """Map 5-digit MSIC code to 4-digit sector code"""
    return INDSECT_MAP.get(str(code).strip(), '9999')


# ============================================================================
# CRISCD FORMAT - CRISIS PURPOSE CODE REMAP
# ============================================================================

CRISCD_MAP: Dict[str, str] = {
    '0440': '0440', '0441': '0440', '0460': '0460',
    '1110': '0110', '1111': '0110', '1120': '0120', '1121': '0120',
    '1131': '0131', '1139': '0132', '1140': '0139',
    '1210': '0110', '1211': '0110', '1220': '0120', '1221': '0120',
    '1230': '0132', '1290': '0139',
    '2111': '0311', '2112': '0312', '2113': '0313', '2114': '0314',
    '2115': '0315', '2116': '0316', '2121': '0321', '2122': '0322',
    '2123': '0323', '2124': '0324', '2125': '0324', '2126': '0329', '2129': '0329',
    '2208': '0311', '2209': '0312', '2210': '0313', '2211': '0314',
    '2212': '0315', '2213': '0316', '2221': '0321', '2222': '0322',
    '2223': '0323', '2224': '0324', '2225': '0324', '2226': '0329', '2229': '0329',
    '2308': '0311', '2309': '0312', '2310': '0313', '2311': '0314',
    '2312': '0315', '2313': '0316', '2321': '0321', '2322': '0322',
    '2323': '0323', '2324': '0324', '2325': '0324', '2326': '0329', '2329': '0329',
    '3100': '3100', '3101': '3100', '3110': '3100', '3120': '3100',
    '3200': '0200', '3201': '0200', '3300': '0200', '3900': '0200', '3901': '0200',
    '4100': '0430', '4300': '0410', '4200': '0420',
    '4411': '0139', '4412': '0139', '4421': '0139', '4422': '0139',
    '4431': '0139', '4432': '0139', '4441': '0139', '4442': '0139',
    '5100': '0390', '5200': '0390', '5300': '0470', '5400': '0990',
    '5401': '0311', '5402': '0312', '5403': '0313', '5404': '0314',
    '5405': '0315', '5406': '0316', '5407': '0321', '5408': '0322',
    '5409': '0323', '5410': '0324', '5411': '0329',
    '5412': '0311', '5413': '0312', '5414': '0313', '5415': '0314',
    '5416': '0315', '5417': '0316', '5418': '0321', '5419': '0322',
    '5420': '0323', '5421': '0324', '5422': '0329',
    '5423': '0324', '5424': '0329', '5425': '0324', '5426': '0329',
    '5431': '0311', '5432': '0312', '5433': '0313', '5434': '0314',
    '5435': '0315', '5436': '0316', '5437': '0321', '5438': '0322',
    '5439': '0323', '5440': '0324', '5441': '0329',
    '5442': '0324', '5443': '0329',
    '5500': '0410', '5501': '0470', '5600': '0440',
    '5700': '0990', '5800': '0990',
    '6300': '0390',
    '9000': '0990', '9001': '0990',
}


def format_criscd(code: str) -> str:
    """Format crisis purpose code remap"""
    return CRISCD_MAP.get(str(code).strip(), '0990')


# ============================================================================
# RVRSECT FORMAT - REVERSE SECTOR MAPPING
# ============================================================================

RVRSECT_MAP: Dict[str, str] = {
    '1111': '1111', '1112': '1112', '1113': '1113', '1114': '1114',
    '1115': '1115', '1116': '1116', '1117': '1119', '1119': '1119',
    '1120': '1118', '1130': '1120', '1140': '1400', '1150': '1130',
    '1200': '1210', '1300': '1309', '1400': '1400',
    '2100': '2100', '2210': '2200', '2220': '2200',
    '2301': '2301', '2302': '2302', '2303': '2303',
    '2400': '2909', '2900': '2909',
    '3110': '3111', '3111': '3112', '3112': '3116', '3113': '3134',
    '3114': '3121', '3115': '3152', '3120': '3140',
    '3211': '3211', '3212': '3219', '3219': '3219',
    '3221': '3220', '3222': '3232', '3231': '3231', '3232': '3240',
    '3241': '3311', '3242': '3319', '3250': '3411',
    '3271': '3420', '3272': '3420', '3273': '3420', '3280': '3420', '3290': '3420',
    '3311': '3511', '3312': '3513', '3313': '3521',
    '3431': '3909', '3432': '3531', '3433': '3540',
    '3551': '3551', '3552': '3560',
    '3611': '3620', '3619': '3610',
    '3710': '3710', '3720': '3720', '3721': '3909',
    '3731': '3710', '3732': '3720',
    '3811': '3813', '3813': '3819', '3814': '3819', '3819': '3819',
    '3825': '3825', '3832': '3822', '3833': '3829', '3834': '3829', '3835': '3829',
    '3842': '3839', '3843': '3832', '3844': '3832',
    '3851': '3851', '3852': '3852', '3853': '3853',
    '3861': '3839', '3862': '3839', '3863': '3839', '3864': '3839',
    '3865': '3839', '3866': '3839',
    '3871': '3843', '3872': '3849', '3873': '3849',
    '3891': '3841', '3892': '3842', '3893': '3845', '3894': '3849',
    '3911': '3320', '3919': '3909',
    '3952': '3909', '3953': '3909', '3955': '3909', '3956': '3909',
    '3957': '3909', '3960': '3909',
    '4010': '4101', '4020': '4102', '4030': '4103',
    '5001': '5001', '5002': '5002', '5003': '5003', '5004': '5004',
    '5005': '5005', '5006': '5006', '5008': '5001',
    '5020': '5009', '5030': '5009', '5040': '5009', '5050': '5009', '5999': '5009',
    '6110': '6131', '6120': '6150', '6130': '6250',
    '6310': '6310', '6320': '6320',
    '7111': '7111', '7112': '7116', '7113': '7113', '7114': '7116',
    '7115': '7116', '7116': '7116', '7117': '7115',
    '7121': '7121', '7122': '7123', '7123': '7122', '7124': '7122',
    '7131': '7131', '7132': '7132', '7133': '7132', '7134': '7132',
    '7191': '7191', '7192': '7192', '7193': '7199', '7199': '7199',
    '7210': '7210', '7220': '7220',
    '8110': '8100', '8120': '8100', '8130': '8200',
    '8310': '8310', '8320': '8329', '8321': '8329', '8331': '8329',
    '8332': '8330', '8333': '8330', '8340': '8323',
    '8411': '9320', '8412': '9320', '8413': '9320', '8414': '9320',
    '8415': '9320', '8416': '9320', '8420': '9320',
    '8910': '8329', '8911': '8329', '8912': '8329', '8913': '8329', '8914': '8329',
    '8920': '8324', '8921': '8329', '8922': '8324',
    '8931': '8325', '8932': '8325', '8991': '8329', '8999': '8329',
    '9101': '9100', '9102': '9100', '9103': '9100',
    '9201': '9331', '9202': '9332', '9203': '9340',
    '9311': '9310', '9312': '9310', '9313': '9310', '9314': '9310',
    '9410': '9200', '9420': '9350',
    '9431': '9411', '9432': '9999', '9433': '9999', '9434': '9999', '9435': '9999',
    '9440': '9420', '9450': '9490', '9499': '9520',
    '9500': '9530', '9600': '9600', '9999': '9999',
}


def format_rvrsect(code: str) -> str:
    """Reverse sector mapping"""
    return RVRSECT_MAP.get(str(code).strip(), '    ')


# ============================================================================
# RVRCRIS FORMAT - REVERSE CRISIS PURPOSE MAPPING
# ============================================================================

RVRCRIS_MAP: Dict[str, str] = {
    '0440': '1000', '0441': '0410', '0460': '1000',
    '1110': '0110', '1120': '0120', '1131': '0131', '1139': '0132', '1140': '0139',
    '1210': '0110', '1220': '0120', '1230': '0132', '1290': '0139',
    '2111': '0311', '2112': '0312', '2113': '0313', '2114': '0314',
    '2115': '0315', '2116': '0316', '2121': '0321', '2122': '0322',
    '2123': '0323', '2124': '0324', '2129': '0329',
    '2208': '0311', '2209': '0312', '2210': '0313', '2211': '0314',
    '2212': '0315', '2213': '0316', '2221': '0321', '2222': '0322',
    '2223': '0323', '2224': '0324', '2229': '0329',
    '2308': '0311', '2309': '0312', '2310': '0313', '2311': '0314',
    '2312': '0315', '2313': '0316', '2321': '1000', '2322': '1000',
    '2323': '1000', '2324': '1000', '2329': '1000',
    '3100': '1000', '3101': '1000', '3200': '1000', '3201': '1000',
    '3900': '1000', '3901': '1000',
    '4100': '0430', '4300': '0410',
    '5100': '1000', '5200': '1000', '5300': '1000',
    '5401': '0311', '5402': '0312', '5403': '0313', '5404': '0314',
    '5405': '0315', '5406': '0316', '5407': '0321', '5408': '0322',
    '5409': '0323', '5410': '0324', '5411': '0329',
    '5412': '0311', '5413': '0312', '5414': '0313', '5415': '0314',
    '5416': '0315', '5417': '0316', '5418': '1000', '5419': '1000',
    '5420': '1000', '5421': '1000', '5422': '1000', '5423': '1000',
    '5424': '0410', '5500': '0410', '5501': '1000', '5600': '1000',
    '5700': '1000', '5800': '1000',
    '9000': '0410', '9001': '1000',
}


def format_rvrcris(code: str) -> str:
    """Reverse crisis purpose code mapping"""
    return RVRCRIS_MAP.get(str(code).strip(), '')


# ============================================================================
# RVRSE FORMAT - REVERSE SECTOR (FISS)
# ============================================================================

RVRSE_MAP: Dict[str, str] = {
    '1111': '1111', '1112': '1119', '1113': '1113', '1114': '1119',
    '1115': '1115', '1116': '1119', '1117': '1119', '1118': '1120',
    '1119': '1119', '1120': '1130', '1130': '1120', '1140': '1120',
    '1150': '1120', '1210': '1200', '1220': '1200',
    '1301': '1300', '1302': '1300', '1309': '1300', '1400': '1400',
    '2100': '2100', '2200': '2210', '2301': '2301', '2302': '2302',
    '2303': '2303', '2901': '2900', '2902': '2900', '2903': '2900', '2909': '2900',
    '3111': '3114', '3112': '3114', '3113': '3110', '3114': '3110',
    '3115': '3110', '3116': '3112', '3117': '3114', '3118': '3114',
    '3119': '3114', '3121': '3114', '3122': '3112', '3123': '3110',
    '3124': '3112', '3125': '3112', '3129': '3114', '3131': '3113',
    '3132': '3113', '3133': '3113', '3134': '3113', '3140': '3120',
    '3151': '3919', '3152': '3919', '3153': '3919', '3159': '3919',
    '3211': '3211', '3212': '3212', '3213': '3212', '3214': '3219',
    '3215': '3219', '3219': '3219', '3220': '3221', '3231': '3231',
    '3232': '3222', '3233': '3231', '3240': '3232',
    '3311': '3241', '3312': '3242', '3319': '3242', '3320': '3911',
    '3411': '3250', '3412': '3250', '3419': '3250', '3420': '3280',
    '3511': '3311', '3512': '3311', '3513': '3312',
    '3521': '3313', '3522': '3313', '3523': '3313', '3529': '3313',
    '3531': '3432', '3540': '3432', '3551': '3551', '3559': '3551',
    '3560': '3552', '3610': '3619', '3620': '3611',
    '3691': '3619', '3692': '3619', '3693': '3619', '3699': '3619',
    '3710': '3710', '3720': '3720',
    '3811': '3819', '3812': '3911', '3813': '3811', '3819': '3819',
    '3821': '3834', '3822': '3832', '3823': '3832', '3824': '3832',
    '3825': '3825', '3829': '3832', '3831': '3866', '3832': '3844',
    '3833': '3866', '3839': '3866', '3841': '3891', '3842': '3892',
    '3843': '3871', '3844': '3894', '3845': '3893', '3849': '3894',
    '3851': '3851', '3852': '3852', '3853': '3853',
    '3901': '3919', '3902': '3919', '3903': '3919', '3909': '3919',
    '4101': '4010', '4102': '4020', '4103': '4030', '4200': '4030',
    '5001': '5001', '5002': '5002', '5003': '5003', '5004': '5004',
    '5005': '5005', '5006': '5006', '5009': '5999',
    '6111': '6120', '6112': '6120', '6113': '6120', '6114': '6120',
    '6115': '6120', '6116': '6120', '6117': '6120', '6118': '6120',
    '6119': '6120', '6121': '6120', '6131': '6110', '6132': '6110',
    '6133': '6110', '6134': '6110', '6139': '6110',
    '6141': '6120', '6142': '6120', '6143': '6120', '6144': '6120',
    '6145': '6120', '6146': '6120', '6147': '6120', '6148': '6120',
    '6149': '6120', '6150': '6120',
    '6211': '6130', '6212': '6130', '6213': '6130', '6214': '6130',
    '6215': '6130', '6216': '6130', '6217': '6130', '6218': '6130',
    '6219': '6130', '6221': '6130', '6222': '6130', '6231': '6130',
    '6241': '6130', '6242': '6130', '6243': '6130', '6249': '6130',
    '6250': '6130', '6310': '6310', '6320': '6320',
    '7111': '7111', '7112': '7113', '7113': '7113', '7114': '7116',
    '7115': '7117', '7116': '7112', '7121': '7121', '7122': '7123',
    '7123': '7121', '7131': '7131', '7132': '7132',
    '7191': '7191', '7192': '7192', '7199': '7199',
    '7210': '7210', '7220': '7220',
    '8100': '8110', '8200': '8130', '8310': '8310',
    '8321': '8914', '8322': '8914', '8323': '8340', '8324': '8922',
    '8325': '8931', '8329': '8999', '8330': '8332',
    '8410': '8416', '8910': '8914', '8920': '8922',
    '9100': '9101', '9200': '9410', '9310': '9314', '9320': '8416',
    '9331': '9201', '9332': '9202', '9340': '9203', '9350': '9420',
    '9391': '9420', '9399': '9203',
    '9411': '9431', '9412': '9431', '9413': '9431', '9414': '9431',
    '9415': '9431', '9420': '9440', '9440': '9430', '9450': '9430',
    '9490': '9431', '9511': '6130', '9512': '6130', '9513': '6110',
    '9514': '6130', '9515': '6130', '9520': '9499', '9530': '9500',
    '9591': '9499', '9592': '9499', '9599': '9499',
    '9600': '9600', '9700': '9700', '9999': '9999',
}


def format_rvrse(code: str) -> str:
    """Reverse sector (FISS) mapping"""
    return RVRSE_MAP.get(str(code).strip(), '    ')


# ============================================================================
# FISSPUR FORMAT - FISS PURPOSE REMAP
# ============================================================================

FISSPUR_MAP: Dict[str, str] = {
    '0111': '0110', '0112': '0110', '0119': '0110',
    '0121': '0120', '0122': '0120', '0129': '0120',
    '0131': '0131', '0132': '0132', '0139': '0139',
    '0211': '0211', '0212': '0212',
    '0220': '0200', '0230': '0200',
    '0311': '0311', '0312': '0312', '0313': '0313', '0314': '0314',
    '0315': '0315', '0316': '0316', '0321': '0321', '0322': '0322',
    '0323': '0323', '0324': '0324', '0329': '0329',
    '0410': '0410', '0420': '0420', '0430': '0430',
}


def format_fisspur(code: str) -> str:
    """Format FISS purpose remap"""
    return FISSPUR_MAP.get(str(code).strip(), str(code).strip())


# ============================================================================
# NEWSECT FORMAT - NEW SECTOR CODE
# ============================================================================

def _build_newsect_map() -> Dict[str, str]:
    m: Dict[str, str] = {}
    for k in ['1111', '1113', '1115', '1117', '1119', '1200', '1300', '1400',
              '2100', '2301', '2303', '2400', '2900',
              '3115', '3113', '3114', '3120', '3250', '3271', '3272', '3273',
              '3280', '3290', '3825', '3710', '3721', '3731', '3732',
              '3811', '3813', '3814', '3819', '3833', '3834', '3835',
              '3911', '3919', '3952', '3953', '3955', '3956', '3957', '3960',
              '5001', '5002', '5003', '5004', '5005', '5006', '5008',
              '5020', '5030', '5040', '5050', '5999',
              '6110', '6120', '6130',
              '7111', '7114', '7116', '7117', '7122', '7124', '7131', '7132',
              '7133', '7134', '7191', '7192', '7193', '7199', '7210', '7220',
              '8310', '8321', '8331', '8333', '8340',
              '8411', '8412', '8413', '8414', '8415', '8416', '8420',
              '8911', '8912', '8913', '8921', '8931', '8932', '8991', '8999',
              '9431', '9433', '9434', '9500', '9600', '9999']:
        m[k] = k
    for k in ['2210', '2220']:
        m[k] = '2200'
    for k in ['3211', '3212', '3219']:
        m[k] = '3210'
    for k in ['3221', '3222']:
        m[k] = '3220'
    for k in ['3231', '3232']:
        m[k] = '3230'
    for k in ['3241', '3242']:
        m[k] = '3240'
    for k in ['3311', '3312', '3313']:
        m[k] = '3310'
    for k in ['3431', '3432', '3433']:
        m[k] = '3430'
    for k in ['3551', '3552']:
        m[k] = '3550'
    for k in ['3611', '3619']:
        m[k] = '3610'
    for k in ['3842', '3843', '3844']:
        m[k] = '3841'
    for k in ['3851', '3852', '3853']:
        m[k] = '3850'
    for k in ['3861', '3862', '3863', '3864', '3865', '3866']:
        m[k] = '3860'
    for k in ['3871', '3872', '3873']:
        m[k] = '3870'
    for k in ['3891', '3892', '3893', '3894']:
        m[k] = '3890'
    for k in ['4010', '4020', '4030']:
        m[k] = '4000'
    for k in ['6310', '6320']:
        m[k] = '6300'
    for k in ['8110', '8120', '8130']:
        m[k] = '8100'
    for k in ['9101', '9102', '9103']:
        m[k] = '9100'
    for k in ['9201', '9202', '9203']:
        m[k] = '9200'
    for k in ['9311', '9312', '9313', '9314']:
        m[k] = '9300'
    return m


NEWSECT_MAP: Dict[str, str] = _build_newsect_map()


def format_newsect(code: str) -> str:
    """Format new sector code"""
    return NEWSECT_MAP.get(str(code).strip(), '')


# ============================================================================
# VALIDSE FORMAT - VALID SECTOR CODE CHECK
# ============================================================================

VALIDSE_SET = {
    '1111', '1112', '1113', '1114', '1115', '1116', '1117', '1119',
    '1120', '1130', '1150', '1200', '1300', '1400', '2100', '1140',
    '2210', '2220', '2301', '2302', '2303', '2400', '2900', '3110', '3111',
    '3112', '3113', '3114', '3115', '3120', '3211', '3212', '3219',
    '3221', '3222', '3231', '3232', '3241', '3242', '3250', '3271', '3272', '3273',
    '3280', '3290', '3311', '3312', '3313', '3431', '3432', '3433', '3551', '3552',
    '3611', '3619', '3710', '3720', '3721', '3731', '3732',
    '3811', '3813', '3814', '3819', '3825',
    '3832', '3833', '3834', '3835', '3842', '3843', '3844', '3851',
    '3852', '3853', '3861', '3862', '3863', '3864', '3865', '3866',
    '3871', '3872', '3873', '3891', '3892', '3893', '3894', '3911', '3919',
    '3956', '3960', '3953', '3957', '3952', '3955',
    '4010', '4020', '4030', '5001', '5002', '5003', '5004',
    '5005', '5006', '5008', '5020', '5030', '5040', '5050',
    '5999', '6110', '6120', '6130', '6310',
    '6320', '7111', '7112', '7113', '7114', '7115', '7116', '7117', '7121', '7122',
    '7123', '7124', '7131', '7132', '7133', '7134', '7191', '7192', '7193',
    '7199', '7210', '7220',
    '8110', '8120', '8130', '8310', '8320', '8321', '8331',
    '8332', '8333', '8340', '8411', '8412', '8413', '8414', '8415', '8416', '8420',
    '8910', '8911', '8912', '8913', '8914', '8920', '8921', '8922',
    '8931', '8932', '8991', '8999',
    '9101', '9102', '9103', '9201', '9202',
    '9203', '9311', '9312', '9313', '9314', '9410', '9420', '9430', '9431',
    '9432', '9433', '9434', '9435', '9440', '9450', '9499',
    '9500', '9600', '9700', '9999',
}


def format_validse(code: str) -> str:
    """Check if sector code is valid"""
    return 'VALID' if str(code).strip() in VALIDSE_SET else 'INVALID'


# ============================================================================
# STATEPOST FORMAT - STATE BY POSTAL CODE
# ============================================================================

# Build a set-based lookup for state by postal code prefix ranges.
# For performance, we pre-build the full map.
def _build_statepost_map() -> Dict[str, str]:
    """Build postal-code-to-state mapping."""
    m: Dict[str, str] = {}
    # Johor (J) - 79xxx, 80xxx-86xxx
    for pc in [
        '79000','79050','79100','79150','79200','79250','79502','79503',
        '79504','79505','79511','79513','79514','79517','79518','79520',
        '79521','79523','79532','79538','79540','79546','79548','79550',
        '79552','79555','79570','79575','79576','79592','79601','79603',
        '79605','79606','79612','79626','79630','79632','79646','79658',
        '79660','79680','79681','79683','80000','80050','80100','80150',
        '80200','80250','80300','80350','80400','80500','80506','80508',
        '80516','80519','80534','80536','80542','80546','80558','80560',
        '80564','80568','80578','80584','80586','80590','80592','80594',
        '80596','80600','80604','80608','80620','80622','80628','80644',
        '80648','80662','80664','80668','80670','80672','80673','80676',
        '80700','80710','80720','80730','80900','80902','80904','80906',
        '80908','80988','80990','81000','81100','81200','81300','81310',
        '81400','81440','81450','81500','81550','81600','81700','81750',
        '81800','81850','81900','81920','81930','82000','82100','82200',
        '82300','83000','83100','83200','83300','83400','83500','83600',
        '83700','84000','84150','84200','84300','84400','84500','84600',
        '84700','84800','84900','85000','85100','85200','85300','85400',
        '86000','86100','86200','86300','86400','86500','86600','86700',
        '86800','86810','86888','86900',
    ]:
        m[pc] = 'J'
    # Kedah (K) - 05xxx-09xxx + some
    for pc in [
        '05000','05050','05100','05150','05200','05250','05300','05350',
        '05400','05460','05500','05502','05503','05504','05505','05506',
        '05508','05512','05514','05516','05517','05518','05520','05532',
        '05534','05536','05538','05550','05551','05552','05556','05558',
        '05560','05564','05576','05578','05580','05582','05586','05590',
        '05592','05594','05600','05604','05610','05612','05614','05620',
        '05621','05622','05626','05628','05630','05632','05644','05660',
        '05661','05664','05670','05672','05673','05674','05675','05676',
        '05680','05690','05696','05700','05710','05720','05990','06000',
        '06007','06009','06010','06050','06100','06150','06200','06207',
        '06209','06250','06300','06350','06400','06500','06507','06509',
        '06550','06570','06600','06650','06660','06700','06707','06709',
        '06710','06720','06750','06760','06800','06900','06910','07000',
        '07007','07009','08000','08007','08009','08010','08100','08110',
        '08200','08210','08300','08320','08330','08340','08400','08407',
        '08409','08500','08507','08509','08600','08700','08800','09000',
        '09007','09009','09010','09020','09100','09110','09120','09130',
        '09200','09300','09310','09400','09410','09600','09700','09800',
        '09810','14290','14390','34950',
    ]:
        m[pc] = 'K'
    # Kelantan (D) - 15xxx-18xxx
    for pc in [
        '15000','15050','15100','15150','15159','15200','15300','15350',
        '15400','15500','15502','15503','15504','15505','15506','15508',
        '15512','15514','15516','15517','15518','15519','15520','15524',
        '15529','15532','15534','15536','15538','15540','15546','15548',
        '15550','15551','15556','15558','15560','15564','15570','15572',
        '15576','15578','15582','15586','15590','15592','15594','15596',
        '15600','15604','15608','15609','15612','15614','15616','15622',
        '15623','15626','15628','15630','15632','15634','15644','15646',
        '15648','15658','15660','15661','15664','15670','15672','15673',
        '15674','15676','15680','15690','15710','15720','15730','15740',
        '15988','15990','16010','16020','16030','16040','16050','16060',
        '16070','16080','16090','16100','16109','16150','16200','16210',
        '16250','16300','16310','16320','16400','16450','16500','16600',
        '16700','16800','16810','17000','17007','17009','17010','17020',
        '17030','17040','17050','17060','17070','17200','17500','17507',
        '17509','17510','17600','17610','17700','18000','18050','18200',
        '18300','18400','18500',
    ]:
        m[pc] = 'D'
    # Melaka (M) - 75xxx-78xxx
    for pc in [
        '75000','75050','75100','75150','75200','75250','75260','75300',
        '75350','75400','75450','75460','75500','75502','75503','75504',
        '75505','75506','75508','75510','75512','75514','75516','75517',
        '75518','75519','75532','75536','75538','75540','75542','75546',
        '75550','75551','75552','75560','75564','75566','75570','75572',
        '75576','75578','75582','75584','75586','75590','75592','75594',
        '75596','75600','75604','75606','75608','75609','75610','75612',
        '75618','75620','75622','75626','75628','75630','75632','75646',
        '75648','75662','75670','75672','75673','75674','75676','75690',
        '75700','75710','75720','75730','75740','75750','75760','75900',
        '75902','75904','75906','75908','75910','75912','75914','75916',
        '75918','75990','76100','76109','76200','76300','76400','76409',
        '76450','77000','77007','77008','77009','77100','77109','77200',
        '77300','77309','77400','77409','77500','78000','78009','78100',
        '78200','78300','78307','78309',
    ]:
        m[pc] = 'M'
    # Negeri Sembilan (N) - 70xxx-73xxx
    for pc in [
        '70000','70100','70200','70300','70400','70450','70500','70502',
        '70503','70504','70505','70506','70508','70512','70516','70517',
        '70518','70532','70534','70536','70540','70546','70548','70550',
        '70551','70558','70560','70564','70570','70572','70576','70578',
        '70582','70586','70590','70592','70594','70596','70600','70604',
        '70606','70608','70609','70610','70620','70626','70628','70632',
        '70634','70644','70646','70648','70658','70664','70670','70672',
        '70673','70674','70676','70690','70700','70710','70720','70730',
        '70740','70750','70990','71000','71007','71009','71010','71050',
        '71059','71100','71109','71150','71159','71200','71209','71250',
        '71259','71300','71309','71350','71359','71400','71409','71450',
        '71459','71500','71509','71550','71559','71600','71609','71650',
        '71659','71700','71707','71709','71750','71759','71760','71770',
        '71800','71807','71809','71900','71907','71909','71950','71960',
        '72000','72007','72009','72100','72107','72109','72120','72127',
        '72129','72200','72207','72209','72300','72307','72309','72400',
        '72409','72500','72507','72509','73000','73007','73009','73100',
        '73109','73200','73207','73209','73300','73309','73400','73409',
        '73420','73430','73440','73450','73460','73470','73480','73500',
        '73507','73509',
    ]:
        m[pc] = 'N'
    # Pahang (C) - 25xxx-28xxx + some
    for pc in [
        '25000','25050','25100','25150','25200','25250','25300','25350',
        '25500','25502','25503','25504','25505','25506','25508','25509',
        '25512','25514','25516','25517','25518','25520','25524','25529',
        '25532','25534','25536','25538','25540','25546','25548','25550',
        '25551','25552','25556','25558','25560','25564','25570','25576',
        '25578','25582','25584','25586','25590','25592','25594','25596',
        '25598','25600','25604','25606','25608','25609','25610','25612',
        '25614','25620','25622','25626','25628','25630','25632','25644',
        '25646','25648','25656','25660','25661','25662','25670','25672',
        '25673','25674','25676','25690','25700','25710','25720','25730',
        '25740','25750','25990','26010','26040','26050','26060','26070',
        '26080','26090','26100','26140','26150','26180','26190','26200',
        '26250','26300','26310','26320','26330','26340','26350','26360',
        '26370','26400','26410','26420','26430','26440','26450','26460',
        '26485','26490','26500','26600','26607','26609','26610','26620',
        '26630','26640','26650','26660','26680','26690','26700','26800',
        '26810','26820','26900','27000','27010','27020','27030','27040',
        '27050','27060','27070','27090','27100','27150','27200','27207',
        '27209','27210','27300','27310','27400','27600','27607','27609',
        '27610','27620','27630','27650','27660','27670','28000','28007',
        '28009','28010','28020','28030','28040','28050','28100','28200',
        '28300','28310','28320','28330','28340','28380','28400','28407',
        '28409','28500','28600','28610','28620','28700','28707','28709',
        '28730','28740','28750','28800','39000','39007','39009','39010',
        '39100','39200','49000','69000',
    ]:
        m[pc] = 'C'
    # Perak (A) - 30xxx-36xxx
    for pc in [
        '30000','30010','30020','30100','30200','30250','30300','30350',
        '30450','30500','30502','30503','30504','30505','30506','30508',
        '30510','30512','30516','30517','30518','30519','30520','30524',
        '30532','30534','30536','30540','30542','30546','30548','30550',
        '30551','30552','30554','30556','30560','30564','30570','30576',
        '30580','30582','30586','30590','30592','30594','30596','30600',
        '30604','30606','30609','30610','30612','30614','30620','30621',
        '30622','30626','30628','30630','30632','30634','30644','30646',
        '30648','30656','30658','30660','30661','30662','30664','30668',
        '30670','30673','30674','30676','30682','30690','30700','30710',
        '30720','30730','30740','30750','30760','30770','30780','30790',
        '30800','30810','30820','30830','30840','30900','30902','30904',
        '30906','30908','30910','30912','30988','30990','31000','31007',
        '31009','31050','31100','31150','31200','31250','31300','31350',
        '31400','31407','31409','31450','31500','31550','31560','31600',
        '31610','31650','31700','31750','31800','31850','31900','31907',
        '31909','31910','31920','31950','32000','32040','32100','32200',
        '32300','32400','32500','32600','32610','32700','32800','32900',
        '33000','33007','33009','33010','33020','33030','33040','33100',
        '33200','33300','33310','33320','33400','33410','33420','33500',
        '33600','33700','33800','34000','34007','34008','34009','34010',
        '34020','34030','34100','34120','34130','34140','34200','34250',
        '34300','34310','34350','34400','34500','34510','34520','34600',
        '34650','34700','34750','34800','34850','34900','35000','35007',
        '35009','35300','35350','35400','35500','35600','35700','35800',
        '35820','35900','35907','35909','35910','35950','36000','36007',
        '36008','36009','36010','36020','36030','36100','36110','36200',
        '36207','36209','36300','36307','36309','36400','36500','36600',
        '36700','36750','36800','36810',
    ]:
        m[pc] = 'A'
    # Perlis (R) - 01xxx-02xxx
    for pc in [
        '01000','01007','01009','01500','01502','01503','01504','01505',
        '01506','01508','01512','01514','01516','01517','01518','01524',
        '01529','01532','01538','01540','01546','01550','01551','01556',
        '01560','01564','01570','01572','01576','01578','01582','01586',
        '01590','01592','01594','01596','01598','01600','01604','01606',
        '01608','01609','01610','01612','01614','01620','01622','01626',
        '01628','01630','01632','01634','01644','01646','01648','01660',
        '01664','01670','01672','01673','01674','01676','01680','01694',
        '02000','02100','02200','02400','02450','02500','02600','02607',
        '02609','02700','02707','02709','02800',
    ]:
        m[pc] = 'R'
    # Penang (P) - 10xxx-14xxx
    for pc in [
        '10000','10050','10100','10150','10200','10250','10300','10350',
        '10400','10450','10460','10470','10500','10502','10503','10504',
        '10505','10506','10508','10512','10514','10516','10518','10524',
        '10534','10538','10540','10542','10546','10550','10551','10552',
        '10558','10560','10564','10566','10570','10576','10578','10582',
        '10590','10592','10593','10594','10596','10600','10604','10609',
        '10610','10612','10620','10622','10626','10628','10634','10646',
        '10648','10660','10661','10662','10670','10672','10673','10674',
        '10676','10690','10710','10720','10730','10740','10750','10760',
        '10770','10780','10790','10800','10810','10820','10830','10840',
        '10850','10910','10920','10990','11000','11010','11020','11050',
        '11060','11100','11200','11300','11400','11409','11500','11600',
        '11609','11700','11800','11900','11910','11920','11950','11960',
        '12000','12100','12200','12300','12700','12710','12720','12990',
        '13000','13009','13020','13050','13100','13110','13200','13210',
        '13220','13300','13310','13400','13409','13500','13600','13700',
        '13800','14000','14007','14009','14020','14100','14101','14110',
        '14120','14200','14300','14310','14320','14400',
    ]:
        m[pc] = 'P'
    # Sabah (S) - 88xxx-91xxx
    for pc in [
        '88000','88100','88200','88300','88400','88450','88460','88500',
        '88502','88504','88505','88506','88508','88510','88512','88514',
        '88516','88518','88520','88526','88527','88532','88534','88538',
        '88540','88546','88550','88551','88552','88554','88556','88558',
        '88560','88562','88564','88566','88568','88570','88572','88576',
        '88580','88582','88586','88590','88592','88594','88596','88598',
        '88600','88602','88604','88606','88608','88609','88610','88612',
        '88614','88617','88618','88620','88621','88622','88624','88626',
        '88628','88630','88632','88634','88644','88646','88648','88656',
        '88658','88660','88661','88662','88670','88672','88673','88675',
        '88676','88680','88690','88700','88721','88722','88723','88724',
        '88725','88757','88758','88759','88760','88761','88762','88763',
        '88764','88765','88766','88767','88768','88769','88770','88771',
        '88772','88773','88774','88775','88776','88777','88778','88779',
        '88780','88781','88782','88783','88784','88785','88786','88787',
        '88788','88789','88790','88800','88801','88802','88803','88804',
        '88805','88806','88807','88808','88809','88810','88811','88812',
        '88813','88814','88815','88816','88817','88818','88819','88820',
        '88821','88822','88823','88824','88825','88826','88827','88828',
        '88829','88830','88831','88832','88833','88834','88835','88836',
        '88837','88838','88839','88840','88841','88842','88843','88844',
        '88845','88846','88847','88848','88849','88850','88851','88852',
        '88853','88854','88855','88856','88857','88858','88860','88861',
        '88862','88863','88865','88866','88867','88868','88869','88870',
        '88871','88872','88873','88874','88875','88900','88901','88902',
        '88903','88904','88905','88906','88988','88990','88991','88992',
        '88993','88994','88995','88996','88997','88998','88999','89000',
        '89007','89008','89009','89050','89057','89058','89059','89100',
        '89107','89108','89109','89130','89137','89138','89139','89150',
        '89157','89158','89159','89200','89207','89208','89209','89250',
        '89257','89258','89259','89260','89300','89307','89308','89309',
        '89320','89327','89328','89329','89330','89337','89338','89339',
        '89500','89507','89508','89509','89600','89607','89608','89609',
        '89650','89657','89658','89659','89700','89707','89708','89709',
        '89720','89727','89728','89729','89740','89747','89748','89749',
        '89760','89767','89768','89769','89800','89807','89808','89809',
        '89850','89857','89858','89859','89900','89907','89908','89909',
        '89950','89957','89958','89959','90000','90009','90100','90107',
        '90109','90200','90300','90307','90400','90700','90701','90702',
        '90703','90704','90705','90706','90707','90708','90709','90711',
        '90712','90713','90714','90715','90716','90717','90718','90719',
        '90720','90721','90722','90723','90724','90725','90726','90727',
        '90728','90729','90730','90731','90732','90733','90734','90735',
        '90736','90737','90738','90739','90740','90741','91000','91007',
        '91008','91009','91010','91011','91012','91013','91014','91015',
        '91016','91017','91018','91019','91020','91021','91022','91023',
        '91024','91025','91026','91027','91028','91029','91030','91031',
        '91032','91033','91034','91035','91100','91109','91110','91111',
        '91112','91113','91114','91115','91116','91117','91118','91119',
        '91120','91121','91122','91123','91124','91125','91126','91127',
        '91128','91150','91200','91207','91209','91300','91307','91308',
        '91309',
    ]:
        m[pc] = 'S'
    # Sarawak (Q) - 93xxx-98xxx
    for pc in [
        '93000','93010','93050','93100','93150','93200','93250','93300',
        '93350','93400','93450','93500','93502','93503','93504','93505',
        '93506','93507','93508','93514','93516','93517','93518','93519',
        '93520','93527','93529','93532','93540','93550','93551','93552',
        '93554','93556','93558','93560','93564','93566','93570','93572',
        '93576','93578','93582','93586','93590','93592','93594','93596',
        '93600','93604','93606','93608','93609','93610','93612','93614',
        '93618','93619','93620','93626','93628','93632','93634','93648',
        '93658','93660','93661','93662','93670','93672','93677','93690',
        '93694','93700','93702','93704','93706','93708','93710','93712',
        '93714','93716','93718','93720','93722','93724','93726','93728',
        '93730','93732','93734','93736','93738','93740','93742','93744',
        '93746','93748','93750','93752','93754','93756','93758','93760',
        '93762','93764','93900','93902','93904','93906','93908','93910',
        '93912','93914','93916','93990','94000','94007','94009','94200',
        '94300','94500','94507','94509','94600','94650','94700','94707',
        '94709','94750','94760','94800','94807','94809','94850','94900',
        '94950','95000','95007','95008','95009','95300','95400','95407',
        '95409','95500','95600','95700','95707','95709','95800','95900',
        '96000','96007','96008','96009','96100','96107','96108','96109',
        '96150','96200','96250','96300','96307','96309','96350','96400',
        '96410','96500','96507','96508','96509','96600','96700','96707',
        '96709','96800','96807','96809','96850','96900','96950','97000',
        '97007','97008','97009','97010','97011','97012','97013','97014',
        '97015','97100','97200','97300','98000','98007','98008','98009',
        '98050','98057','98058','98059','98100','98107','98109','98150',
        '98157','98159','98200','98300','98700','98707','98708','98709',
        '98750','98800','98850','98857','98859',
    ]:
        m[pc] = 'Q'
    # Selangor (B) - 40xxx-48xxx + some
    for pc in [
        '40000','40100','40150','40160','40170','40200','40300','40400',
        '40450','40460','40470','40500','40502','40503','40505','40512',
        '40517','40520','40529','40542','40548','40550','40551','40560',
        '40564','40570','40572','40576','40578','40582','40590','40592',
        '40594','40596','40598','40604','40607','40608','40610','40612',
        '40620','40622','40626','40632','40646','40648','40660','40664',
        '40670','40672','40673','40674','40675','40676','40680','40690',
        '40700','40702','40704','40706','40708','40710','40712','40714',
        '40716','40718','40720','40722','40724','40726','40728','40730',
        '40732','40800','40802','40804','40806','40808','40810','40990',
        '41000','41050','41100','41150','41200','41250','41300','41400',
        '41506','41560','41586','41672','41700','41710','41720','41900',
        '41902','41904','41906','41908','41910','41912','41914','41916',
        '41918','41990','42000','42009','42100','42200','42300','42425',
        '42500','42507','42509','42600','42610','42700','42800','42920',
        '42940','42960','43000','43007','43009','43100','43200','43207',
        '43300','43400','43500','43558','43600','43650','43700','43800',
        '43807','43900','43950','44000','44010','44020','44100','44110',
        '44200','44300','45000','45100','45200','45207','45209','45300',
        '45400','45500','45600','45607','45609','45620','45700','45800',
        '46000','46050','46100','46150','46200','46300','46350','46400',
        '46506','46547','46549','46551','46564','46582','46598','46662',
        '46667','46668','46672','46675','46700','46710','46720','46730',
        '46740','46750','46760','46770','46780','46781','46782','46783',
        '46784','46785','46786','46787','46788','46789','46790','46791',
        '46792','46793','46794','46795','46796','46797','46798','46799',
        '46800','46801','46802','46803','46804','46805','46806','46860',
        '46870','46960','46962','46964','46966','46968','46970','46972',
        '46974','46976','46978','47000','47100','47110','47120','47130',
        '47140','47150','47160','47170','47180','47190','47200','47300',
        '47301','47307','47308','47400','47410','47500','47507','47600',
        '47610','47620','47630','47640','47650','47800','47810','47820',
        '47830','48000','48010','48020','48050','48100','48200','48300',
        '63000','63100','63200','63300','64000','68000','68100',
    ]:
        m[pc] = 'B'
    # Terengganu (T) - 20xxx-24xxx
    for pc in [
        '20000','20050','20100','20200','20300','20400','20500','20502',
        '20503','20504','20505','20506','20508','20512','20514','20516',
        '20517','20518','20519','20520','20532','20534','20536','20538',
        '20540','20542','20546','20548','20550','20551','20552','20554',
        '20556','20560','20564','20566','20568','20570','20572','20576',
        '20578','20582','20586','20590','20592','20596','20600','20604',
        '20606','20608','20609','20610','20612','20614','20618','20620',
        '20622','20626','20628','20630','20632','20646','20648','20656',
        '20658','20660','20661','20662','20664','20668','20670','20672',
        '20673','20674','20676','20680','20690','20698','20700','20710',
        '20720','20900','20902','20904','20906','20908','20910','20912',
        '20914','20916','20918','20920','20922','20924','20926','20928',
        '20930','20990','21000','21009','21010','21020','21030','21040',
        '21060','21070','21080','21090','21100','21109','21200','21209',
        '21210','21220','21300','21309','21400','21450','21500','21600',
        '21610','21700','21800','21810','21820','22000','22010','22020',
        '22100','22107','22109','22110','22120','22200','22300','22307',
        '22309','23000','23007','23009','23050','23100','23200','23300',
        '23400','24000','24007','24009','24050','24060','24100','24107',
        '24109','24200','24207','24209','24300',
    ]:
        m[pc] = 'T'
    # WP Kuala Lumpur / WP Putrajaya / WP Labuan (W) - 50xxx-62xxx + 87xxx
    for pc in [
        '50000','50050','50088','50100','50150','50200','50250','50300',
        '50350','50400','50450','50460','50470','50480','50490','50500',
        '50502','50504','50505','50506','50507','50508','50512','50514',
        '50515','50519','50528','50529','50530','50532','50534','50536',
        '50540','50544','50546','50548','50550','50551','50552','50554',
        '50556','50560','50562','50564','50566','50568','50572','50576',
        '50578','50580','50582','50586','50588','50590','50592','50594',
        '50596','50598','50599','50600','50603','50604','50605','50608',
        '50609','50610','50612','50614','50620','50621','50622','50623',
        '50626','50632','50634','50636','50638','50640','50642','50644',
        '50646','50648','50650','50652','50653','50656','50658','50660',
        '50661','50662','50664','50666','50668','50670','50672','50673',
        '50676','50677','50678','50680','50682','50684','50688','50694',
        '50700','50702','50704','50706','50708','50710','50712','50714',
        '50716','50718','50720','50722','50724','50726','50728','50730',
        '50732','50734','50736','50738','50740','50742','50744','50746',
        '50748','50750','50752','50754','50758','50760','50762','50764',
        '50766','50768','50770','50772','50774','50776','50778','50780',
        '50782','50784','50786','50788','50790','50792','50794','50796',
        '50798','50800','50802','50804','50806','50808','50810','50812',
        '50814','50816','50818','50901','50902','50903','50904','50906',
        '50907','50908','50909','50910','50911','50912','50913','50914',
        '50915','50916','50917','50918','50919','50920','50921','50922',
        '50923','50924','50925','50926','50927','50928','50929','50930',
        '50931','50932','50933','50934','50935','50936','50937','50938',
        '50939','50940','50941','50942','50943','50944','50945','50946',
        '50947','50948','50949','50950','50988','50989','50990','51000',
        '51100','51200','51700','51990','52000','52100','52200','53000',
        '53100','53200','53300','53700','53800','53990','54000','54100',
        '54200','55000','55100','55200','55300','55700','55710','55720',
        '55900','55902','55904','55906','55908','55910','55912','55914',
        '55916','55918','55920','55922','55924','55926','55928','55930',
        '55932','55934','55990','56000','56100','57000','57100','57700',
        '57990','58000','58100','58200','58700','58990','59000','59100',
        '59200','59700','59800','59990','60000','62000','62007','62050',
        '62100','62150','62200','62250','62300','62502','62504','62505',
        '62506','62510','62512','62514','62516','62517','62518','62519',
        '62520','62522','62524','62526','62527','62530','62532','62536',
        '62540','62542','62546','62550','62551','62570','62574','62576',
        '62582','62584','62590','62592','62596','62602','62604','62605',
        '62606','62616','62618','62620','62623','62624','62628','62630',
        '62632','62648','62652','62654','62662','62668','62670','62674',
        '62675','62676','62677','62686','62692','62988',
    ]:
        m[pc] = 'W'
    # WP Labuan (L) - 87xxx
    for pc in [
        '87000','87010','87011','87012','87013','87014','87015','87016',
        '87017','87018','87019','87020','87021','87022','87023','87024',
        '87025','87026','87027','87028','87029','87030','87031','87032',
        '87033',
    ]:
        m[pc] = 'L'
    return m


STATEPOST_MAP: Dict[str, str] = _build_statepost_map()


def format_statepost(postcode: str) -> str:
    """Map postal code to state letter"""
    return STATEPOST_MAP.get(str(postcode).strip(), '')


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def is_more_plan(product: int) -> bool:
    """Check if product is MORE PLAN"""
    return product in MORE_PLAN


def is_hire_purchase(product: int) -> bool:
    """Check if product is Hire Purchase"""
    return product in HP_ALL


def is_islamic_product(product: int) -> bool:
    """Check if product is Islamic"""
    return format_lndenom(product) == 'I'


def is_fcy_product(product: int) -> bool:
    """Check if product is Foreign Currency"""
    return product in FCY_PRODUCTS


# ============================================================================
# EXPORT ALL FORMAT FUNCTIONS
# ============================================================================

__all__ = [
    # Format functions - OD
    'format_oddenom',
    'format_odprod',
    'format_odrate',
    'format_odcustcd',
    'format_locustcd',
    'format_odfmt',
    'format_odlob',

    # Format functions - Loan
    'format_lndenom',
    'format_lnprod',
    'format_lnrate',
    'format_lncustcd',
    'format_liqpfmt',
    'format_sltype',
    'format_ln03fmt',
    'format_lnfmt',
    'format_lnlob',
    'format_lnormt',
    'format_lnrmmt',

    # Format functions - customer
    'format_btcustcd',

    # Format functions - other
    'format_statecd',
    'format_statepost',
    'format_apprlimt',
    'format_loansize',
    'format_mthpass',
    'format_ndays',
    'format_collcd',
    'format_riskcd',
    'format_busind',
    'format_arrclass',
    'format_delqdes',
    'format_hpcc',

    # Bankers Trust formats
    'format_btprod',
    'format_btprodi',
    'format_btproda',
    'format_btrate',

    # FISS / sector formats
    'format_fisstype',
    'format_fissgroup',
    'format_sectcd',
    'format_secdes',
    'format_secta',
    'format_sectb',
    'format_indsect',
    'format_criscd',
    'format_rvrsect',
    'format_rvrcris',
    'format_rvrse',
    'format_fisspur',
    'format_newsect',
    'format_validse',

    # Helper functions
    'is_more_plan',
    'is_hire_purchase',
    'is_islamic_product',
    'is_fcy_product',

    # Product lists (macro equivalents)
    'MOREPLAN_PRODUCTS',
    'MOREISLM_PRODUCTS',
    'MORE_PLAN',
    'MORE_ISLAM',
    'HP_ALL',
    'HP_ACTIVE',
    'AITAB',
    'HOME_ISLAMIC',
    'HOME_CONVENTIONAL',
    'SWIFT_ISLAMIC',
    'SWIFT_CONVENTIONAL',
    'FCY_PRODUCTS',
    'COUNTRY_CODES',
]
