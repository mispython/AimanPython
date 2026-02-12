#!/usr/bin/env python3
"""
Program  : PBBLNFMT (Format Definitions)
To define format mappings for loan processing
"""

from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

# ============================================================================
# CONFIGURATION
# ============================================================================

# This module provides format mapping functions equivalent to SAS PROC FORMAT
# These formats are used throughout the loan and deposit processing system

# ============================================================================
# OVERDRAFT DENOMINATION FORMAT
# ============================================================================

ODDENOM_MAP = {
    # Islamic OD products
    **{k: 'I' for k in [32, 33, 60, 61, 62, 63, 64, 92, 93, 96, 81,
                        70, 71, 73, 74] + list(range(160, 170)) +
       list(range(182, 189)) + [7, 8, 46, 47, 48, 49, 45,
                                13, 14] + list(range(15, 20)) + list(range(23, 26)) +
       [20, 21]},
}


def format_oddenom(product: int) -> str:
    """Format OD denomination - Islamic (I) or Domestic (D)"""
    return ODDENOM_MAP.get(product, 'D')


# ============================================================================
# LOAN DENOMINATION FORMAT
# ============================================================================

LNDENOM_ISLAMIC_PRODUCTS = [
                               100, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 122, 126, 127,
                               128, 129, 130, 131, 132, 185, 169, 134, 135, 136, 138, 139, 140, 141, 142,
                               143, 170, 180, 181, 182, 183, 101, 147, 148, 173, 174, 159, 160, 161, 162,
                               184, 191, 146
                           ] + list(range(851, 900)) + list(range(152, 159)) + [164, 165, 179] + \
                           list(range(192, 197)) + [197, 199, 124, 145, 144, 163, 186, 187] + \
                           list(range(102, 109)) + [188, 189, 190, 137] + \
                           list(range(400, 418)) + [418, 427, 428, 419, 420, 421, 422] + \
                           list(range(429, 445)) + [445, 446, 448] + list(range(461, 471)) + \
                           list(range(650, 700)) + [973] + list(range(471, 499))


def format_lndenom(product: int) -> str:
    """Format loan denomination - Islamic (I) or Domestic (D)"""
    return 'I' if product in LNDENOM_ISLAMIC_PRODUCTS else 'D'


# ============================================================================
# LOAN PRODUCT CODE FORMAT (LNPROD)
# ============================================================================

LNPROD_MAP = {
    # Staff loans - Housing
    **{k: '34230' for k in [4, 5, 6, 7]},
    # Staff loans - Car
    15: '34230',
    # Staff loans - Motorcycle
    20: '34230',
    # Staff loans - Other purposes
    **{k: '34230' for k in range(25, 35)},
    # Staff loans - Additional categories
    **{k: '34230' for k in [60, 61, 62, 63, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79]},
    # ABBA staff loans
    **{k: '34230' for k in [100, 101, 102, 103, 104, 105, 106, 107, 108]},

    # Housing loans (34120)
    **{k: '34120' for k in [110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
                            139, 140, 141, 142, 147, 173, 445, 446]},
    **{k: '34120' for k in range(200, 249)},
    **{k: '34120' for k in range(250, 261)},
    **{k: '34120' for k in [400, 409, 410, 412, 413, 414, 415, 423, 431, 432, 433, 440, 466,
                            472, 473, 474, 479, 484, 486, 489, 494]},
    **{k: '34120' for k in [600, 638, 650, 651, 664, 677, 911]},

    # ABBA term financing (34149)
    **{k: '34149' for k in [120, 122, 126, 127, 129, 133, 134, 137, 143, 144, 148, 149,
                            153, 154, 155, 157, 158, 159, 160, 161, 162, 163, 164, 165,
                            169, 170, 172, 174, 176, 177, 178, 179, 181, 182, 183, 187,
                            188, 189, 190, 193, 194, 199]},
    **{k: '34149' for k in range(300, 316)},
    **{k: '34149' for k in [316, 320, 322, 325, 335, 345, 348, 349, 356, 357, 358, 359,
                            361, 362, 363, 368]},
    **{k: '34149' for k in range(401, 409)},
    **{k: '34149' for k in [411, 416, 417, 418, 421, 425, 427, 428, 429, 430, 447, 448]},
    **{k: '34149' for k in range(434, 440)},
    **{k: '34149' for k in [442, 461, 462, 463, 467, 471, 476, 477, 478, 480, 481, 485,
                            487, 488]},

    # Hire Purchase (34111)
    **{k: '34111' for k in [128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 750, 752, 760]},

    # Personal loans (34117)
    **{k: '34117' for k in [135, 136, 138, 303, 306, 307, 308, 311, 313, 325, 330, 340,
                            354, 355, 367, 369, 419, 420, 422, 424, 426, 441, 443, 464,
                            465, 468, 469, 470, 475, 477, 482, 483, 490, 491, 492, 493,
                            496, 497, 498, 609, 610, 611, 668, 669, 672, 673, 674, 675, 693]},

    # Bridging loans (34114)
    **{k: '34114' for k in [309, 310, 417, 608, 637, 670, 690, 904, 905, 919, 920]},

    # Leasing (34112)
    390: '34112',
    799: '34112',

    # Block discounting (34113)
    **{k: '34113' for k in [360, 770, 775, 908]},

    # Syndicated loans (34115)
    **{k: '34115' for k in [180, 185, 186, 197, 684, 686, 914, 915, 950]},

    # Revolving credit (34190)
    **{k: '34190' for k in [146, 184, 190, 192, 195, 196, 302, 350, 351, 364, 365, 506,
                            495, 604, 605, 634, 641, 660, 685, 689, 802, 803, 806, 808,
                            810, 812, 814, 817, 818, 856, 857, 858, 859, 860, 902, 903,
                            910, 917, 925, 951]},

    # Floor stocking (34170)
    **{k: '34170' for k in [392, 612]},

    # FCY loans (34600)
    **{k: '34600' for k in [800, 801, 804, 805, 807, 809, 811, 813, 815, 816, 851, 852,
                            853, 854, 855]},

    # FCY revolving credit (34690)
    **{k: '34690' for k in [802, 803, 806, 808, 810, 812, 814, 817, 818, 856, 857, 858,
                            859, 860]},

    # Sold to Cagamas
    **{k: '54120' for k in [124, 145, 225, 226, 530, 709, 710]},
    **{k: '54124' for k in [709, 710]},

    # Special categories
    98: 'N',
    107: 'N',
    **{k: 'N' for k in [126, 127, 128, 129, 130, 136, 144, 145, 171, 172, 173, 500, 520,
                        678, 679, 698, 699, 972, 973]},
    **{k: 'M' for k in [984, 985, 994, 995, 996]},
}


def format_lnprod(product: int) -> str:
    """Map product code to loan product category"""
    return LNPROD_MAP.get(product, '34149')


# ============================================================================
# CUSTOMER CODE FORMATS
# ============================================================================

CUSTCD_MAP = {
    1: '01',  # Bank Negara Malaysia
    2: '02',  # Commercial Banks
    3: '03',  # Islamic Banks
    4: '04',  # Subsidiary Stockbroking Companies
    5: '05',  # Associate Stockbroking Companies
    6: '06',  # Other Stockbroking Companies
    10: '11',  # Domestic Banking Institutions
    11: '11',  # Finance Companies
    12: '12',  # Merchant Banks
    13: '13',  # Discount Houses
    15: '79',  # Domestic Non-Bank Entities
    17: '17',  # Cagamas Berhad
    20: '30',  # Domestic Non-Bank Financial Institutions
    30: '30',  # Domestic Other NBFI
    32: '32',  # Credit Card Companies
    33: '33',  # Development Finance Institutions
    34: '34',  # Building Societies
    35: '35',  # Co-operative Societies
    36: '04',  # Stockbroking Companies
    37: '37',  # Commodity Brokers
    38: '38',  # Credit & Leasing Companies
    39: '39',  # Unit Trust Companies
    40: '40',  # Insurance and Insurance Related Companies
    # SME categories
    41: '41',  # DBE - Bumi Controlled SME - Micro
    42: '42',  # DBE - Bumi Controlled SME - Small
    43: '43',  # DBE - Bumi Controlled SME - Medium
    44: '44',  # DBE - Non-Bumi Controlled SME - Micro
    46: '46',  # DBE - Non-Bumi Controlled SME - Small
    47: '47',  # DBE - Non-Bumi Controlled SME - Medium
    48: '48',  # DBE - Non-Resident Control SME - Micro
    49: '49',  # DBE - Non-Resident Control SME - Small
    51: '51',  # DBE - Non-Resident Control SME - Medium
    52: '52',  # DBE - Government Controlled SME - Micro
    53: '53',  # DBE - Government Controlled SME - Small
    54: '54',  # DBE - Government Controlled SME - Medium
    50: '79',  # Residents/Domestic Entities
    57: '57',  # Petronas
    59: '59',  # Other Government Controlled DBE NIE
    60: '62',  # Domestic Business Enterprises (DBE)
    61: '61',  # Bumiputra Controlled DBE
    62: '62',  # Non-Bumiputra Controlled DBE
    63: '63',  # Non-Resident Controlled DBE
    64: '64',  # Government Controlled DBE
    65: '44',  # Small Medium Industries (SMI)
    66: '41',  # Bumiputra Controlled SMI
    67: '44',  # Non-Bumiputra Controlled SMI
    68: '48',  # Non-Resident Controlled SMI
    69: '52',  # Government Controlled SMI
    # Government
    70: '71',  # Government
    71: '71',  # Federal Government
    72: '72',  # State Government
    73: '73',  # Local Government
    74: '74',  # Statutory Authorities
    75: '75',  # NFPE
    # Individuals
    76: '78',  # Individuals
    77: '77',  # Bumiputra
    78: '78',  # Non-Bumiputra
    79: '79',  # Domestic Other Entities NIE
    # Foreign entities
    80: '86',  # Non-Residents/Foreign Entities
    81: '86',  # Foreign Banking Institutions (OD format uses 81)
    85: '86',  # Foreign Non-Bank Entities
    86: '86',  # Foreign Business Enterprises
    87: '87',  # Foreign Business Enterprises - Micro
    88: '88',  # Foreign Business Enterprises - Small
    89: '89',  # Foreign Business Enterprises - Medium
    90: '90',  # Foreign Governments
    91: '91',  # Foreign Central Banks
    92: '92',  # Foreign Diplomatic Representation in Malaysia
    95: '95',  # Foreign Individuals
    96: '96',  # Foreigners Employed/Studying in Malaysia
    98: '98',  # Foreign Non-Commercial International Organization
    99: '99',  # Foreign Other Entities NIE
}


def format_odcustcd(custcode: int) -> str:
    """Format overdraft customer code"""
    return CUSTCD_MAP.get(custcode, '79')


def format_locustcd(custcode: int) -> str:
    """Format loan customer code (same as OD but with 81,82,83,84 differences)"""
    if custcode == 81:
        return '81'
    elif custcode == 82:
        return '82'
    elif custcode == 83:
        return '83'
    elif custcode == 84:
        return '84'
    return CUSTCD_MAP.get(custcode, '79')


def format_lncustcd(custcode: int) -> str:
    """Format loan customer code with specific mappings"""
    specific_map = {
        10: '10', 15: '15', 20: '20', 36: '36', 50: '50', 60: '60',
        65: '65', 76: '76', 80: '80', 81: '81', 85: '85'
    }
    return specific_map.get(custcode, CUSTCD_MAP.get(custcode, '79'))


# ============================================================================
# STATE CODE FORMAT
# ============================================================================

STATE_CODE_MAP = {
    **{str(i).zfill(n): 'J' for n in range(1, 7) for i in [1]},
    **{str(i).zfill(n): 'K' for n in range(1, 7) for i in [2]},
    **{str(i).zfill(n): 'D' for n in range(1, 7) for i in [3]},
    **{str(i).zfill(n): 'M' for n in range(1, 7) for i in [4]},
    **{str(i).zfill(n): 'N' for n in range(1, 7) for i in [5]},
    **{str(i).zfill(n): 'C' for n in range(1, 7) for i in [6]},
    **{str(i).zfill(n): 'P' for n in range(1, 7) for i in [7]},
    **{str(i).zfill(n): 'A' for n in range(1, 7) for i in [8]},
    **{str(i).zfill(n): 'R' for n in range(1, 7) for i in [9]},
    **{str(10).zfill(n): 'S' for n in range(2, 7)},
    **{str(11).zfill(n): 'Q' for n in range(2, 7)},
    **{str(12).zfill(n): 'B' for n in range(2, 7)},
    **{str(13).zfill(n): 'T' for n in range(2, 7)},
    **{str(i).zfill(n): 'W' for n in range(2, 7) for i in [14, 16]},
    **{str(15).zfill(n): 'L' for n in range(2, 7)},
}


def format_statecd(code: str) -> str:
    """Map state code to single letter"""
    if not code:
        return ' '
    return STATE_CODE_MAP.get(code, ' ')


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
# MONTHS PASSED DUE FORMAT
# ============================================================================

def format_mthpass(days: int) -> str:
    """Format months passed due based on days"""
    if days <= 30:
        return '0'
    elif days <= 59:
        return '1'
    elif days <= 89:
        return '2'
    elif days <= 121:
        return '3'
    elif days <= 151:
        return '4'
    elif days <= 182:
        return '5'
    elif days <= 213:
        return '6'
    elif days <= 243:
        return '7'
    elif days <= 273:
        return '8'
    elif days <= 303:
        return '9'
    elif days <= 333:
        return '10'
    elif days <= 364:
        return '11'
    elif days <= 394:
        return '12'
    elif days <= 424:
        return '13'
    elif days <= 456:
        return '14'
    elif days <= 486:
        return '15'
    elif days <= 516:
        return '16'
    elif days <= 547:
        return '17'
    elif days <= 577:
        return '18'
    elif days <= 608:
        return '19'
    elif days <= 638:
        return '20'
    elif days <= 668:
        return '21'
    elif days <= 698:
        return '22'
    elif days <= 729:
        return '23'
    else:
        return '24'


# ============================================================================
# ORIGINAL MATURITY AND REMAINING MATURITY FORMATS
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
# COLLATERAL CODE FORMAT
# ============================================================================

COLLCD_MAP = {
    **{str(i): '30570' for i in [1, 2, 3, 5, 6, 7, 9, 10, 11, 12, 14, 21, 41, 42, 43,
                                 50, 51, 52, 53, 54, 55, 56, 57, 60, 61]},
    '8': '30520',  # Pledge of quoted shares as primary collateral
    '13': '30530',  # Other securities
    '14': '30570',  # Fixed deposits (margin less than 90%)
    '15': '30540',  # KLMF Unit Trust
    **{str(i): '30580' for i in [20, 22, 23]},  # Unsecured
}


def format_collcd(code: str) -> str:
    """Format collateral code"""
    return COLLCD_MAP.get(code, '30570')


# ============================================================================
# RISK CODE FORMAT
# ============================================================================

def format_riskcd(code: str) -> str:
    """Format risk code"""
    if code in ['2', '02', '002', '0002']:
        return '34902'  # Substandard
    elif code in ['3', '03', '003', '0003']:
        return '34903'  # Doubtful
    elif code in ['4', '04', '004', '0004']:
        return '34904'  # Bad
    return ' '


# ============================================================================
# BUSINESS INDICATOR FORMAT
# ============================================================================

def format_busind(custcd: str) -> str:
    """Format business indicator - BUS or IND"""
    business_codes = ['01', '02', '03', '04', '05', '06', '11', '12', '13', '17',
                      '30', '31', '32', '33', '34', '35', '37', '38', '39', '40',
                      '45', '57', '59', '61', '62', '63', '64', '66', '67', '68',
                      '69', '71', '72', '73', '74', '75']

    if custcd in business_codes:
        return 'BUS'
    return 'IND'


# ============================================================================
# PRODUCT LISTS (MACRO EQUIVALENTS)
# ============================================================================

# Product category lists
MORE_PLAN = [116, 119, 234, 235, 236, 242]
MORE_ISLAM = [116, 119]
HP_ALL = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 983, 993, 996, 678, 679, 698, 699]
HP_ACTIVE = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
AITAB = [128, 130, 131, 132]
HOME_ISLAMIC = [113, 115, 117, 118]
HOME_CONVENTIONAL = [227, 228, 230, 231, 237, 238, 239, 240, 241]
SWIFT_ISLAMIC = [126, 127]
SWIFT_CONVENTIONAL = [359]
FCY_PRODUCTS = [800, 801, 802, 803, 804, 805, 806, 807, 808, 816, 817,
                809, 810, 811, 812, 813, 814, 815, 851, 852, 853, 854,
                855, 856, 857, 858, 859, 860]

# Country codes (ISO 2-letter codes)
COUNTRY_CODES = [
    'AF', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG', 'AR', 'AM', 'AW', 'AU', 'AT',
    'AZ', 'BS', 'BH', 'BD', 'BB', 'BY', 'BE', 'BZ', 'BJ', 'BM', 'BT', 'BO', 'BA', 'BW',
    # ... (complete list from SAS code)
    'ZM', 'ZW'
]


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
    # Format functions
    'format_oddenom',
    'format_lndenom',
    'format_lnprod',
    'format_odcustcd',
    'format_locustcd',
    'format_lncustcd',
    'format_statecd',
    'format_apprlimt',
    'format_loansize',
    'format_mthpass',
    'format_lnormt',
    'format_lnrmmt',
    'format_collcd',
    'format_riskcd',
    'format_busind',
    # Helper functions
    'is_more_plan',
    'is_hire_purchase',
    'is_islamic_product',
    'is_fcy_product',
    # Product lists
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
