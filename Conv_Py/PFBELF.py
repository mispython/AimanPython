# !/usr/bin/env python3
"""
Program: PFBELF
Purpose: BNM (Bank Negara Malaysia) Exposure Limit and Branch Format Definitions
         Central format library for banking regulatory reporting

This module provides BNM code mappings and branch code formats used across
banking regulatory reports. Includes:
- RM (Ringgit Malaysia) Exposure Limits (RMEL)
- FX (Foreign Exchange) Exposure Limits (FXEL)
- RM Eligible Assets (RMEA)
- FX Eligible Assets (FXEA)
- Branch code mappings (BRCHCD, IBU)
- CAC (Central Administrative Centre) mappings

This is a companion module to PBBLNFMT and is used for BNM regulatory reporting.
"""

# BNM Exposure Limit Codes - RM (Ringgit Malaysia)
RMEL_CODES = {
    # Deposits and borrowings (positive exposure)
    '4211000000000Y': '+',  # RM DEMAND DEPOSITS ACCEPTED
    '4212000000000Y': '+',  # RM SAVINGS DEPOSITS ACCEPTED
    '4213000000000Y': '+',  # RM FIXED DEPOSITS ACCEPTED
    '4213100000000Y': '+',  # RM SPECIAL INVESTMENT DEPOSIT ACCEPTED
    '4213200000000Y': '+',  # RM GENERAL INVESTMENT DEPOSIT ACCEPTED
    '4215000000000Y': '+',  # RM NID ISSUED
    '4216000000000Y': '+',  # RM REPURCHASE AGREEMENTS
    '4217071000000Y': '+',  # RM SPECIAL DEPOSITS
    '4218060000000Y': '+',  # RM HOUSING DEVELOPMENT ACCOUNTS
    '4219900000000Y': '+',  # RM OTHER DEPOSITS ACCEPTED
    '4314001000000Y': '+',  # RM INTERBANK BORROWINGS FROM BNM
    '4314002000000Y': '+',  # RM INTERBANK BORROWINGS FROM CB
    '4314003000000Y': '+',  # RM INTERBANK BORROWINGS FROM IB
    '4314011000000Y': '+',  # RM INTERBANK BORROWINGS FROM FC
    '4314012000000Y': '+',  # RM INTERBANK BORROWINGS FROM MB
    '4314013000000Y': '+',  # RM INTERBANK BORROWINGS FROM DH
    '4314017000000Y': '+',  # RM INTERBANK BORROWINGS FROM CAGAMAS
    '4314081100000Y': '+',  # RM INTERBANK BORROWINGS FROM FBI <= 1 YR
    '4314081200000Y': '+',  # RM INTERBANK BORROWINGS FROM FBI > 1 YR
    '4410000000000Y': '+',  # RM MISC BORROWINGS
    '4911080000000Y': '+',  # RM INTEREST PAYABLE TO NON-RESIDENTS
    '4912080000000Y': '+',  # RM BILLS PAYABLE TO NON-RESIDENTS
    '4929980000000Y': '+',  # SPTF OTH RM MISC LIAB NIE DUE TO NON-RES
    # Subordinated debt (negative exposure)
    '4411100000000Y': '-',  # RM SUBORDINATED DEBT CAPITAL
    '4411200000000Y': '-',  # RM EXEMPT SUBORDINATED DEBT CAPITAL
}

# BNM Exposure Limit Codes - FX (Foreign Exchange)
FXEL_CODES = {
    '4260000000000Y': '+',  # FX DEPOSITS ACCEPTED
    '4362081000000Y': '+',  # FX OVERDRAWN NOSTRO ACCOUNTS WITH FBI
    '4364002000000Y': '+',  # FX INTERBANK BORROWINGS FROM CB
    '4364003000000Y': '+',  # FX INTERBANK BORROWINGS FROM IB
    '4364012000000Y': '+',  # FX INTERBANK BORROWINGS FROM MB
    '4364081100000Y': '+',  # FX INTERBANK BORROWINGS FROM FBI <= 1 YR
    '4364081200000Y': '+',  # FX INTERBANK BORROWINGS FROM FBI > 1 YR
    '4370000000000Y': '+',  # SPTF FX AMOUNT DUE TO FI
    '4460000000000Y': '+',  # FX MISC BORROWINGS
    '4760000000000Y': '+',  # FX DEBT SECURITIES ISSUED
    '4961050000000Y': '+',  # FX INTEREST PAYABLE TO RESIDENTS
    '4961080000000Y': '+',  # FX INTEREST PAYABLE TO NON-RESIDENTS
    '4969950000000Y': '+',  # OTHER FX MISC LIAB NIE DUE TO RESIDENTS
    '4979950000000Y': '+',  # SPTF OTHER FX MISC LIAB DUE TO RESIDENTS
    '4969980000000Y': '+',  # OTHER FX MISC LIAB NIE DUE TO NON-RES
    '4979980000000Y': '+',  # SPTF OTHER FX MISS LIAB DUE TO NON-RES
    '4461100000000Y': '-',  # FX SUBORDINATED DEBT CAPITAL
}

# BNM Eligible Assets - RM
RMEA_CODES = {
    '3212002000000Y': '+',  # RM BALANCES IN CURRENT ACCOUNTS WITH CB
    '3213002000000Y': '+',  # RM FIXED DEPOSITS PLACED WITH CB
    '3213011000000Y': '+',  # RM FIXED DEPOSITS PLACED WITH FC
    '3213012000000Y': '+',  # RM FIXED DEPOSITS PLACED WITH MB
    '3213202000000Y': '+',  # RM GENERAL INVESTMENTS WITH CB
    '3213203000000Y': '+',  # RM GENERAL INVESTMENTS WITH IB
    '3213211000000Y': '+',  # RM GENERAL INVESTMENTS WITH FC
    '3213212000000Y': '+',  # RM GENERAL INVESTMENTS WITH MB
    '3250002000000Y': '+',  # RM REVERSE REPOS WITH CB
    '3250011000000Y': '+',  # RM REVERSE REPOS WITH FC
    '3250012000000Y': '+',  # RM REVERSE REPOS WITH MB
    '3250013000000Y': '+',  # RM REVERSE REPOS WITH DH
    '3311002000000Y': '+',  # RM OVERDRAWN VOSTRO ACCOUNTS OF CB
    '3312002000000Y': '+',  # RM NOSTRO ACCOUNT BALANCES WITH CB
    '3313000000000Y': '+',  # RM AMOUNT LENT TO KLACH POOL
    '3314001000000Y': '+',  # RM INTERBANK PLACEMENTS WITH BNM
    '3314002000000Y': '+',  # RM INTERBANK PLACEMENTS WITH CB
    '3314003000000Y': '+',  # RM INTERBANK PLACEMENTS WITH IB
    '3314011000000Y': '+',  # RM INTERBANK PLACEMENTS WITH FC
    '3314012000000Y': '+',  # RM INTERBANK PLACEMENTS WITH MB
    '3314013000000Y': '+',  # RM INTERBANK PLACEMENTS WITH DH
    '3314017000000Y': '+',  # RM INTERBANK PLACEMENTS WITH CAGAMAS
    '3410002000000Y': '+',  # RM LOANS TO CB
    '3410003000000Y': '+',  # RM LOANS TO IB
    '3410011000000Y': '+',  # RM LOANS TO FC
    '3410012000000Y': '+',  # RM LOANS TO MB
    '3410013000000Y': '+',  # RM LOANS TO DH
    '3410017000000Y': '+',  # RM LOANS TO CAGAMAS
    '3703000000000Y': '+',  # RM NIDS HELD
    '3803000000000Y': '+',  # NIDS SOLD UNDER REPO
    '4015000000000Y': '+',  # ELIGIBLE TIER-2 LOANS SOLD TO CAGAMAS
    '3314013110000Y': '-',  # RM INTERBKS PLACEMENTS WITH DH OVERNIGHT
    '3330081000000Y': '-',  # SPTF RM AMOUNT DUE FROM FBI
}

# BNM Eligible Assets - FX
FXEA_CODES = {
    '3260000000000Y': '+',  # FX DEPOSITS PLACED
    '3280000000000Y': '+',  # FX REVERSE REPOS
    '3362081000000Y': '+',  # FX NOSTRO ACCOUNT BALANCES WITH FBI
    '3364002000000Y': '+',  # FX INTERBANK PLACEMENTS WITH CB
    '3364003000000Y': '+',  # FX INTERBANK PLACEMENTS WITH IB
    '3364012000000Y': '+',  # FX INTERBANK PLACEMENTS WITH MB
    '3364081100000Y': '+',  # FX INTERBANK PLACEMENTS WITH FBI <= 1 YR
    '3364081200000Y': '+',  # FX INTERBANK PLACEMENTS WITH FBI > 1 YR
    '3370000000000Y': '+',  # SPTF - FX AMOUNT DUE FROM DESIGNATED FI
    '3460000000000Y': '+',  # FX LOANS
    '3760000000000Y': '+',  # FX SECURITIES HELD
}

# Branch Code Mappings
BRANCH_CODES = {
    1: 'KLM', 2: 'PMO', 3: 'MLK', 4: 'IMO', 5: 'TPG', 6: 'JBU', 7: 'JPU', 8: 'JSI',
    9: 'KLC', 10: 'JBA', 11: 'SST', 12: 'BTW', 13: 'KKU', 14: 'SRB', 15: 'JPR', 16: 'ASR',
    17: 'KLG', 18: 'KPR', 19: 'BGH', 20: 'KLS', 21: 'KPH', 22: 'TRG', 23: 'SGM', 24: 'SMG',
    25: 'PKG', 26: 'MUA', 27: 'KTN', 28: 'STP', 29: 'KCG', 30: 'SKC', 31: 'LBN', 32: 'MSG',
    33: 'KBH', 34: 'PJN', 35: 'RWG', 36: 'KPG', 37: 'TMH', 38: 'PDN', 39: 'KDN', 40: 'DJA',
    41: 'APG', 42: 'TWU', 43: 'SJA', 44: 'MTK', 45: 'BMM', 46: 'TIN', 47: 'PKL', 48: 'KGR',
    49: 'KJG', 50: 'KTU', 51: 'JKL', 52: 'BPR', 53: 'EDU', 54: 'KHG', 55: 'TKK', 56: 'SPI',
    57: 'BBU', 58: 'SKN', 59: 'CKI', 60: 'TPN', 61: 'SBU', 62: 'BDR', 63: 'AIM', 64: 'YPG',
    65: 'KKI', 66: 'BTG', 67: 'SDN', 68: 'MRI', 69: 'KKB', 70: 'KTI', 71: 'KLI', 72: 'JTA',
    73: 'SRM', 74: 'SRR', 75: 'KKR', 76: 'PJA', 77: 'BEN', 78: 'LDU', 79: 'BPT', 80: 'KAP',
    81: 'PRI', 82: 'GMG', 83: 'TST', 84: 'PLH', 85: 'SBP', 86: 'KLA', 87: 'TDI', 88: 'CAH',
    89: 'JSN', 90: 'RAU', 91: 'SBR', 92: 'LLG', 93: 'JLP', 94: 'TSP', 95: 'BSI', 96: 'JMH',
    97: 'PBR', 98: 'MLB', 99: 'BTL', 100: 'SAM', 101: 'KBU', 102: 'KBS', 103: 'SAN', 104: 'SJY',
    105: 'JJG', 106: 'TML', 107: 'JIH', 108: 'BBB', 109: 'INN', 110: 'SBM', 111: 'SEA', 112: 'PJO',
    113: 'IGN', 114: 'BBI', 115: 'WMU', 116: 'GRT', 117: 'SGB', 118: 'UYB', 119: 'KMY', 120: 'BTA',
    121: 'KRK', 122: 'TRJ', 123: 'SLY', 124: 'NLI', 125: 'SSA', 126: 'SPG', 127: 'SBH', 128: 'SSH',
    129: 'TIH', 130: 'RTU', 131: 'TJJ', 132: 'PCG', 133: 'SNI', 134: 'JRT', 135: 'SGK', 136: 'STL',
    137: 'JTS', 138: 'JKA', 139: 'JDK', 140: 'SAB', 141: 'SDI', 142: 'TPI', 143: 'TCT', 144: 'NTL',
    145: 'MSI', 146: 'CCE', 147: 'KJA', 148: 'JLT', 149: 'BDA', 150: 'BBM', 151: 'AST', 152: 'TMA',
    153: 'USJ', 154: 'TMI', 155: 'TMK', 156: 'DUA', 157: 'JSB', 158: 'PIH', 159: 'SS2', 160: 'TSJ',
    161: 'TCL', 162: 'TEA', 163: 'SFN', 164: 'JTT', 165: 'JBH', 166: 'BAM', 167: 'JPA', 168: 'STW',
}

# IBU (Islamic Banking Unit) Branch Codes - offset by 2000
IBU_BRANCH_CODES = {k + 2000: v for k, v in BRANCH_CODES.items()}
IBU_BRANCH_CODES[4000] = 'IBU'

# CAC (Central Administrative Centre) Mappings
CAC_BRANCHES = {
    '333': [1, 7, 15, 34, 36, 37, 40, 43, 49, 51, 57, 58, 76, 87, 105, 107, 111, 112, 114,
            117, 118, 125, 128, 129, 132, 136, 137, 138, 143, 147, 149, 153, 154, 156, 157,
            158, 159, 163],  # K. LUMPUR
    '334': [9, 28, 35, 41, 69, 115, 123, 127, 135, 146, 152, 160],  # CITY CENTRE
    '335': [12, 33, 45, 81, 85, 102, 104, 144, 166],  # SEBERANG JAYA
    '336': [2, 10, 63, 108, 139, 148, 164],  # PENANG
    '337': [6, 71, 83, 131, 133, 141, 142, 145, 155, 70],  # JOHOR BAHRU
    '338': [25, 66, 80, 86, 94, 100, 140, 161, 162],  # KELANG
}

CAC_NAMES = {
    '333': 'CAC-K. LUMPUR ',
    '334': 'CAC-CITY CENTRE',
    '335': 'CAC-SEBERANG JAYA',
    '336': 'CAC-PENANG',
    '337': 'CAC-JOHOR BAHRU',
    '338': 'CAC-KELANG',
}


def format_brchcd(branch_code):
    """
    Format branch code to branch name
    Returns: Branch code string (3 letters)
    """
    if 2000 <= branch_code <= 4000:
        return 'IBU'
    elif branch_code >= 5000:
        return 'HOA'
    else:
        return BRANCH_CODES.get(branch_code, 'UNK')


def format_ibu(branch_code):
    """
    Format IBU (Islamic Banking Unit) branch code
    Returns: Branch code string (3 letters)
    """
    if branch_code >= 5000:
        return 'HOA'
    else:
        return IBU_BRANCH_CODES.get(branch_code, 'UNK')


def format_cacbrch(branch_code):
    """
    Get CAC (Central Administrative Centre) code for branch
    Returns: CAC code ('333', '334', '335', '336', '337', '338', or '000')
    """
    for cac_code, branches in CAC_BRANCHES.items():
        if branch_code in branches:
            return cac_code
    return '000'


def format_cacname(branch_code):
    """
    Get CAC (Central Administrative Centre) name for branch
    Returns: CAC name string
    """
    cac_code = format_cacbrch(branch_code)
    return CAC_NAMES.get(cac_code, 'NON CAC')


def get_bnm_sign(bnm_code, format_type='RMEL'):
    """
    Get the sign (+/-) for a BNM code

    Args:
        bnm_code: BNM code string (14 characters + 'Y')
        format_type: 'RMEL', 'FXEL', 'RMEA', or 'FXEA'

    Returns: '+' or '-' or None
    """
    code_maps = {
        'RMEL': RMEL_CODES,
        'FXEL': FXEL_CODES,
        'RMEA': RMEA_CODES,
        'FXEA': FXEA_CODES,
    }

    code_map = code_maps.get(format_type, {})
    return code_map.get(bnm_code)


def is_eligible_asset(bnm_code, currency='RM'):
    """
    Check if BNM code represents an eligible asset

    Args:
        bnm_code: BNM code string
        currency: 'RM' or 'FX'

    Returns: Boolean
    """
    if currency == 'RM':
        return bnm_code in RMEA_CODES
    elif currency == 'FX':
        return bnm_code in FXEA_CODES
    else:
        return False


def is_exposure_limit(bnm_code, currency='RM'):
    """
    Check if BNM code represents an exposure limit item

    Args:
        bnm_code: BNM code string
        currency: 'RM' or 'FX'

    Returns: Boolean
    """
    if currency == 'RM':
        return bnm_code in RMEL_CODES
    elif currency == 'FX':
        return bnm_code in FXEL_CODES
    else:
        return False


def get_all_cac_branches(cac_code):
    """
    Get all branch codes for a given CAC

    Args:
        cac_code: CAC code ('333', '334', etc.)

    Returns: List of branch codes
    """
    return CAC_BRANCHES.get(cac_code, [])


def get_branch_name_with_code(branch_code):
    """
    Get formatted branch name with code

    Args:
        branch_code: Branch code integer

    Returns: Formatted string like "KLM 001"
    """
    branch_name = format_brchcd(branch_code)
    return f"{branch_name} {branch_code:03d}"


# Export all functions and constants
__all__ = [
    # BNM Code Dictionaries
    'RMEL_CODES',
    'FXEL_CODES',
    'RMEA_CODES',
    'FXEA_CODES',
    'BRANCH_CODES',
    'IBU_BRANCH_CODES',
    'CAC_BRANCHES',
    'CAC_NAMES',
    # Format Functions
    'format_brchcd',
    'format_ibu',
    'format_cacbrch',
    'format_cacname',
    'get_bnm_sign',
    'is_eligible_asset',
    'is_exposure_limit',
    'get_all_cac_branches',
    'get_branch_name_with_code',
]
