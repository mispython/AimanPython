# !/usr/bin/env python3
"""
Program : PBMISFMT
Purpose : Python equivalent of SAS PBMISFMT PROC FORMAT definitions.
          Provides branch code, group, product, denomination, customer-type,
            state, deposit-range and various other format mappings used across
            MIS reporting programs (EIBMISL2, EIBMDPCA, EIBM619D/L, etc.).

Amendment history (from original SAS):
- Multiple branch additions and renames over the years.
- Age-limit macro variables: AGELIMIT=12, MAXAGE=18, AGEBELOW=11
"""

from typing import Optional

# ===========================================================================
# MACRO-VARIABLE EQUIVALENTS
# ===========================================================================
AGELIMIT = 12   # CALL SYMPUT('AGELIMIT', 12)
MAXAGE   = 18   # CALL SYMPUT('MAXAGE',   18)
AGEBELOW = 11   # CALL SYMPUT('AGEBELOW', 11)

# ===========================================================================
# FORMAT: BRCHCD  (numeric branch number -> 3-letter branch code)
# Handles individual branches plus ranges for HOE.
# ===========================================================================
_BRCHCD_MAP: dict[int, str] = {
    1: 'HOE', 2: 'JSS', 3: 'JRC', 4: 'MLK', 5: 'IMO',
    6: 'PPG', 7: 'JBU', 8: 'KTN', 9: 'JYK', 10: 'ASR',
    11: 'GRN', 12: 'PPH', 13: 'KBU', 14: 'TMH', 15: 'KPG',
    16: 'NLI', 17: 'TPN', 18: 'PJN', 19: 'DUA', 20: 'TCL',
    21: 'BPT', 22: 'SMY', 23: 'KMT', 24: 'RSH', 25: 'SAM',
    26: 'SPG', 27: 'NTL', 28: 'MUA', 29: 'JRL', 30: 'KTU',
    31: 'SKC', 32: 'WSS', 33: 'KKU', 34: 'KGR', 35: 'SSA',
    36: 'SS2', 37: 'TSA', 38: 'JKL', 39: 'KKG', 40: 'JSB',
    41: 'JIH', 42: 'BMM', 43: 'BTG', 44: 'TWU', 45: 'SRB',
    46: 'APG', 47: 'SGM', 48: 'MTK', 49: 'JLP', 50: 'MRI',
    51: 'SMG', 52: 'UTM', 53: 'TMI', 54: 'BBB', 55: 'LBN',
    56: 'KJG', 57: 'SPI', 58: 'SBU', 59: 'PKL', 60: 'BAM',
    61: 'KLI', 62: 'SDK', 63: 'GMS', 64: 'PDN', 65: 'BHU',
    66: 'BDA', 67: 'CMR', 68: 'SAT', 69: 'BKI', 70: 'PSA',
    71: 'BCG', 72: 'PPR', 73: 'SPK', 74: 'SIK', 75: 'CAH',
    76: 'PRS', 77: 'PLI', 78: 'SJA', 79: 'MSI', 80: 'MLB',
    81: 'SBH', 82: 'MCG', 83: 'JBB', 84: 'PMS', 85: 'SST',
    86: 'CLN', 87: 'MSG', 88: 'KUM', 89: 'TPI', 90: 'BTL',
    91: 'KUG', 92: 'KLG', 93: 'EDU', 94: 'STP', 95: 'TIN',
    96: 'SGK', 97: 'HSL', 98: 'TCY',
    102: 'PRJ', 103: 'JJG', 104: 'KKL', 105: 'KTI', 106: 'CKI',
    107: 'JLT', 108: 'BSI', 109: 'KSR', 110: 'TJJ', 111: 'AKH',
    112: 'LDO', 113: 'TML', 114: 'BBA', 115: 'KNG', 116: 'TRI',
    117: 'KKI', 118: 'TMW', 119: 'BNV', 120: 'PIH', 121: 'PRA',
    122: 'SKN', 123: 'IGN', 124: 'S14', 125: 'KJA', 126: 'PTS',
    127: 'TSM', 128: 'SGB', 129: 'BSR', 130: 'PDG', 131: 'TMG',
    132: 'CKT', 133: 'PKG', 134: 'RPG', 135: 'BSY', 136: 'TCS',
    137: 'JPP', 138: 'WMU', 139: 'JRT', 140: 'CPE', 141: 'STL',
    142: 'KBD', 143: 'LDU', 144: 'KHG', 145: 'BSD', 146: 'PSG',
    147: 'PNS', 148: 'PJO', 149: 'BFT', 150: 'LMM', 151: 'SLY',
    152: 'ATR', 153: 'USJ', 154: 'BSJ', 155: 'TTJ', 156: 'TMR',
    157: 'BPJ', 158: 'SPL', 159: 'RLU', 160: 'MTH', 161: 'DGG',
    162: 'SEA', 163: 'JKA', 164: 'KBS', 165: 'TKA', 166: 'PGG',
    167: 'BBG', 168: 'KLC', 169: 'CTD', 170: 'PJA', 171: 'JMR',
    172: 'TMJ', 173: 'SCA', 174: 'BBP', 175: 'LBG', 176: 'TPG',
    177: 'JRU', 178: 'MIN', 179: 'OUG', 180: 'KBG',
    182: 'JPU', 183: 'JCL', 184: 'JPN', 185: 'KCY', 186: 'JTZ',
    # 187 commented out in SAS
    188: 'PLT', 189: 'BNH', 190: 'BTR', 191: 'KPT', 192: 'MRD',
    193: 'MKH', 194: 'SRK', 195: 'BWK', 196: 'JHL', 197: 'TNM',
    198: 'TDA', 199: 'JTH',
    # 200 commented out in SAS
    201: 'PDA', 202: 'RWG', 203: 'SJM', 204: 'BTW', 205: 'SNG',
    206: 'TBM', 207: 'BCM', 208: 'JSI', 209: 'STW', 210: 'TMM',
    211: 'TPD', 212: 'JMA', 213: 'JKB', 214: 'JGA', 215: 'JKP',
    216: 'SKI', 217: 'TMB', 220: 'GHS', 221: 'TSK', 222: 'TDC',
    223: 'TRJ', 224: 'JAH', 225: 'TIH', 226: 'JPR', 227: 'KSB',
    228: 'INN', 229: 'TSJ', 230: 'SSH', 231: 'BBM', 232: 'TMD',
    233: 'BEN', 234: 'SRM', 235: 'SBM', 236: 'UYB', 237: 'KLS',
    238: 'JKT', 239: 'KMY', 240: 'KAP', 241: 'DJA', 242: 'TKK',
    243: 'KKR', 244: 'GRT', 245: 'BDR', 246: 'BGH', 247: 'BPR',
    248: 'JTS', 249: 'TAI', 250: 'TEA', 251: 'KPR', 252: 'TMA',
    253: 'JTT', 254: 'KPH', 255: 'SBP', 256: 'PBR', 257: 'RAU',
    258: 'JTA', 259: 'SAN', 260: 'KDN', 261: 'GMG', 262: 'TCT',
    263: 'BTA', 264: 'JBH', 265: 'JAI', 266: 'JDK', 267: 'TDI',
    268: 'BBT', 269: 'MKA', 270: 'BPI', 273: 'LHA', 274: 'STG',
    275: 'MSL', 276: 'JAS', 277: 'WSU', 278: 'JPI', 279: 'PTJ',
    280: 'KDA', 281: 'PLT', 282: 'PTT', 283: 'PSE', 284: 'BSP',
    285: 'BMC', 286: 'BIH', 287: 'SUA', 288: 'SPT', 289: 'TEE',
    290: 'TDY', 291: 'BSL', 292: 'BMJ', 293: 'BSA', 294: 'KKM',
    295: 'BKR', 296: 'BJL',
    701: 'IKB', 702: 'IPJ', 703: 'IWS', 704: 'IJK',
}
# Ranges that map to 'HOE': 3000-3001, 7000, 7500-8000, 8050-8750
_BRCHCD_HOE_RANGES = [(3000, 3001), (7000, 7000), (7500, 8000), (8050, 8750)]
# All other 3xxx codes map to their own code (same as subtracting 3000 from key)
_BRCHCD_3XXX = {(3000 + k): v for k, v in _BRCHCD_MAP.items() if 1 <= k <= 296}

def format_brchcd(branch: Optional[int]) -> str:
    """Map numeric branch code to 3-letter branch abbreviation."""
    if branch is None:
        return ''
    for lo, hi in _BRCHCD_HOE_RANGES:
        if lo <= branch <= hi:
            return 'HOE'
    if branch in _BRCHCD_3XXX:
        return _BRCHCD_3XXX[branch]
    return _BRCHCD_MAP.get(branch, '')

# ===========================================================================
# FORMAT: $GROUPF  (3-letter branch code -> group string)
# ===========================================================================
GROUPF_MAP: dict[str, str] = {
    # GROUP 1
    'BBB': 'GROUP 1', 'BDA': 'GROUP 1', 'BMM': 'GROUP 1', 'DUA': 'GROUP 1',
    'IMO': 'GROUP 1', 'JKL': 'GROUP 1', 'JRC': 'GROUP 1', 'JRL': 'GROUP 1',
    'JSS': 'GROUP 1', 'JSB': 'GROUP 1', 'JYK': 'GROUP 1', 'JBU': 'GROUP 1',
    'KPG': 'GROUP 1', 'KLC': 'GROUP 1', 'MLK': 'GROUP 1', 'PPG': 'GROUP 1',
    'SS2': 'GROUP 1', 'SPG': 'GROUP 1', 'SSA': 'GROUP 1', 'SAM': 'GROUP 1',
    'SJA': 'GROUP 1', 'TCL': 'GROUP 1', 'TSA': 'GROUP 1',
    # GROUP 2
    'BAM': 'GROUP 2', 'ASR': 'GROUP 2', 'BSY': 'GROUP 2', 'BSR': 'GROUP 2',
    'BTG': 'GROUP 2', 'BPT': 'GROUP 2', 'CMR': 'GROUP 2', 'GRN': 'GROUP 2',
    'IGN': 'GROUP 2', 'JIH': 'GROUP 2', 'JJG': 'GROUP 2', 'JBB': 'GROUP 2',
    'KJG': 'GROUP 2', 'KLG': 'GROUP 2', 'KBU': 'GROUP 2', 'KKU': 'GROUP 2',
    'KTN': 'GROUP 2', 'WSS': 'GROUP 2', 'KLI': 'GROUP 2', 'MSI': 'GROUP 2',
    'MLB': 'GROUP 2', 'MRI': 'GROUP 2', 'MUA': 'GROUP 2', 'NTL': 'GROUP 2',
    'PDG': 'GROUP 2', 'PJN': 'GROUP 2', 'SMY': 'GROUP 2', 'SRB': 'GROUP 2',
    'SGK': 'GROUP 2', 'SKN': 'GROUP 2', 'SAT': 'GROUP 2', 'SBH': 'GROUP 2',
    'TCS': 'GROUP 2', 'TMI': 'GROUP 2', 'TMG': 'GROUP 2', 'TMW': 'GROUP 2',
    'TPN': 'GROUP 2',
    # GROUP 3
    'BBA': 'GROUP 3', 'APG': 'GROUP 3', 'BHU': 'GROUP 3', 'BSD': 'GROUP 3',
    'BKI': 'GROUP 3', 'BNV': 'GROUP 3', 'BTL': 'GROUP 3', 'BCG': 'GROUP 3',
    'CAH': 'GROUP 3', 'CKT': 'GROUP 3', 'JPP': 'GROUP 3', 'JLP': 'GROUP 3',
    'JLT': 'GROUP 3', 'KBR': 'GROUP 3', 'KMT': 'GROUP 3', 'KJA': 'GROUP 3',
    'KUG': 'GROUP 3', 'KKG': 'GROUP 3', 'KUM': 'GROUP 3', 'LBN': 'GROUP 3',
    'LMM': 'GROUP 3', 'LDO': 'GROUP 3', 'MTK': 'GROUP 3', 'NLI': 'GROUP 3',
    'PIH': 'GROUP 3', 'PRS': 'GROUP 3', 'PJO': 'GROUP 3', 'PKL': 'GROUP 3',
    'PKG': 'GROUP 3', 'PTS': 'GROUP 3', 'PSG': 'GROUP 3', 'RSH': 'GROUP 3',
    'STL': 'GROUP 3', 'S14': 'GROUP 3', 'SGM': 'GROUP 3', 'SKC': 'GROUP 3',
    'STP': 'GROUP 3', 'SBU': 'GROUP 3', 'SPK': 'GROUP 3', 'SPI': 'GROUP 3',
    'TCY': 'GROUP 3', 'TJJ': 'GROUP 3', 'TSM': 'GROUP 3', 'TTJ': 'GROUP 3',
    'TPI': 'GROUP 3', 'TMH': 'GROUP 3', 'TWU': 'GROUP 3', 'USJ': 'GROUP 3',
    'UTM': 'GROUP 3', 'WMU': 'GROUP 3',
    # GROUP 4
    'AKH': 'GROUP 4', 'ATR': 'GROUP 4', 'BBG': 'GROUP 4', 'BBP': 'GROUP 4',
    'BSI': 'GROUP 4', 'BPJ': 'GROUP 4', 'BSJ': 'GROUP 4', 'BFT': 'GROUP 4',
    'CLN': 'GROUP 4', 'CKI': 'GROUP 4', 'CTD': 'GROUP 4', 'CPE': 'GROUP 4',
    'DGG': 'GROUP 4', 'EDU': 'GROUP 4', 'GMS': 'GROUP 4', 'JKA': 'GROUP 4',
    'JMR': 'GROUP 4', 'JRT': 'GROUP 4', 'HSL': 'GROUP 4', 'KHG': 'GROUP 4',
    'KGR': 'GROUP 4', 'KNG': 'GROUP 4', 'KBS': 'GROUP 4', 'KBD': 'GROUP 4',
    'KTI': 'GROUP 4', 'KKL': 'GROUP 4', 'KKI': 'GROUP 4', 'KSR': 'GROUP 4',
    'KTU': 'GROUP 4', 'LDU': 'GROUP 4', 'MCG': 'GROUP 4', 'MTH': 'GROUP 4',
    'MSG': 'GROUP 4', 'PJA': 'GROUP 4', 'PPR': 'GROUP 4', 'PRJ': 'GROUP 4',
    'PMS': 'GROUP 4', 'PPH': 'GROUP 4', 'PNS': 'GROUP 4', 'PSA': 'GROUP 4',
    'PGG': 'GROUP 4', 'PDN': 'GROUP 4', 'PRA': 'GROUP 4', 'PLI': 'GROUP 4',
    'RPJ': 'GROUP 4', 'RLU': 'GROUP 4', 'SDK': 'GROUP 4', 'SEA': 'GROUP 4',
    'SGB': 'GROUP 4', 'SLY': 'GROUP 4', 'SMG': 'GROUP 4', 'SIK': 'GROUP 4',
    'SPL': 'GROUP 4', 'SCA': 'GROUP 4', 'SST': 'GROUP 4', 'TMJ': 'GROUP 4',
    'TMR': 'GROUP 4', 'TPG': 'GROUP 4', 'TIN': 'GROUP 4', 'TML': 'GROUP 4',
    'TKA': 'GROUP 4', 'TRI': 'GROUP 4', 'GHS': 'GROUP 4', 'BPI': 'GROUP 4',
    'HOE': 'HOE',
}

def format_groupf(brchcd: Optional[str]) -> str:
    """Map branch code to group string. OTHER -> 'GROUP 4'."""
    if brchcd is None:
        return 'GROUP 4'
    return GROUPF_MAP.get(brchcd, 'GROUP 4')

# ===========================================================================
# FORMAT: $ACKNOF  (account number prefix -> account type)
# ===========================================================================
def format_acknof(ackno: Optional[str]) -> str:
    """Map account number code to account type label."""
    if ackno is None:
        return 'OTHER'
    if ackno == '21930':
        return 'NEF'
    if ackno == '21940':
        return 'NIF'
    if ackno == '21960':
        return 'SFT'
    if ackno == '21950':
        return '3F'
    if ackno in ('21910', '21920'):
        return 'SHP'
    try:
        v = int(ackno)
        if 32300 <= v <= 32699:
            return 'SPECIAL DEPOSIT WITHOLDING TAX'
    except (ValueError, TypeError):
        pass
    return 'OTHER'

# ===========================================================================
# FORMAT: SAPROD  (savings product code -> product label for EIBMISL2)
# Note: This is the PBMISFMT SAPROD (simpler than PBBDPFMT SAPROD).
# ===========================================================================
def format_saprod_mis(product: Optional[int]) -> str:
    """Map savings product to MIS label: product 204 -> 'SPTFSD', else 'SD'."""
    if product == 204:
        return 'SPTFSD'
    return 'SD    '

# ===========================================================================
# FORMAT: CAPROD  (current account product code -> product label)
# ===========================================================================
_CAPROD_OTHER = {
    104, 105, 107, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135,
    136, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 171, 172, 173,
}

def format_caprod_mis(product: Optional[int]) -> str:
    """Map current account product to MIS label."""
    if product is None:
        return 'DD   '
    return 'OTHER' if product in _CAPROD_OTHER else 'DD   '

# ===========================================================================
# FORMAT: ODPROD  (overdraft product code -> product label)
# ===========================================================================
_ODPROD_OTHER = {
    104, 105, 107, 126, 127, 128, 130, 131, 132, 133, 134, 135, 136,
    140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 171, 172, 173,
}

def format_odprod_mis(product: Optional[int]) -> str:
    """Map overdraft product to MIS label."""
    if product is None:
        return 'OD   '
    return 'OTHER' if product in _ODPROD_OTHER else 'OD   '

# ===========================================================================
# FORMAT: LNPROD  (loan product code -> loan label)
# ===========================================================================
_LNPROD_HL = {4, 5, 100, 101, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220}
_LNPROD_HLCAG = {225, 226}

def format_lnprod_mis(product: Optional[int]) -> str:
    """Map loan product to MIS label: HL / HLCAG / FL."""
    if product is None:
        return 'FL   '
    if product in _LNPROD_HL:
        return 'HL   '
    if product in _LNPROD_HLCAG:
        return 'HLCAG'
    return 'FL   '

# ===========================================================================
# FORMAT: SADPRG / SA1PRG / SA2PRG / SA3PRG
# Deposit amount range formats (numeric value -> label string).
# Each uses a step-ladder of thresholds. SAS uses exclusive upper bound for
# ranges (HIGH exclusive), so Python uses strict < comparisons.
# ===========================================================================

def format_sadprg(amount: Optional[float]) -> str:
    """SADPRG: Savings account deposit range (48 bands)."""
    if amount is None:
        return ''
    v = float(amount)
    if v <= 5:       return '01)RM        1.00 - RM         5.00'
    if v <= 10:      return '02)RM        5.01 - RM        10.00'
    if v <= 50:      return '03)RM       10.01 - RM        50.00'
    if v <= 100:     return '04)RM       50.01 - RM       100.00'
    if v <= 500:     return '05)RM      100.01 - RM       500.00'
    if v <= 1000:    return '06)RM      500.01 - RM     1,000.00'
    if v <= 1500:    return '07)RM    1,000.01 - RM     1,500.00'
    if v <= 2000:    return '08)RM    1,500.01 - RM     2,000.00'
    if v <= 2500:    return '09)RM    2,000.01 - RM     2,500.00'
    if v <= 3000:    return '10)RM    2,500.01 - RM     3,000.00'
    if v <= 3500:    return '11)RM    3,000.01 - RM     3,500.00'
    if v <= 4000:    return '12)RM    3,500.01 - RM     4,000.00'
    if v <= 4500:    return '13)RM    4,000.01 - RM     4,500.00'
    if v <= 5000:    return '14)RM    4,500.01 - RM     5,000.00'
    if v <= 6000:    return '15)RM    5,000.01 - RM     6,000.00'
    if v <= 7000:    return '16)RM    6,000.01 - RM     7,000.00'
    if v <= 8000:    return '17)RM    7,000.01 - RM     8,000.00'
    if v <= 9000:    return '18)RM    8,000.01 - RM     9,000.00'
    if v <= 10000:   return '19)RM    9,000.01 - RM    10,000.00'
    if v <= 15000:   return '20)RM   10,000.01 - RM    15,000.00'
    if v <= 20000:   return '21)RM   15,000.01 - RM    20,000.00'
    if v <= 25000:   return '22)RM   20,000.01 - RM    25,000.00'
    if v <= 30000:   return '23)RM   25,000.01 - RM    30,000.00'
    if v <= 35000:   return '24)RM   30,000.01 - RM    35,000.00'
    if v <= 40000:   return '25)RM   35,000.01 - RM    40,000.00'
    if v <= 45000:   return '26)RM   40,000.01 - RM    45,000.00'
    if v <= 50000:   return '27)RM   45,000.01 - RM    50,000.00'
    if v <= 55000:   return '28)RM   50,000.01 - RM    55,000.00'
    if v <= 60000:   return '29)RM   55,000.01 - RM    60,000.00'
    if v <= 65000:   return '30)RM   60,000.01 - RM    65,000.00'
    if v <= 70000:   return '31)RM   65,000.01 - RM    70,000.00'
    if v <= 75000:   return '32)RM   70,000.01 - RM    75,000.00'
    if v <= 80000:   return '33)RM   75,000.01 - RM    80,000.00'
    if v <= 85000:   return '34)RM   80,000.01 - RM    85,000.00'
    if v <= 90000:   return '35)RM   85,000.01 - RM    90,000.00'
    if v <= 95000:   return '36)RM   90,000.01 - RM    95,000.00'
    if v <= 100000:  return '37)RM   95,000.01 - RM   100,000.00'
    if v <= 150000:  return '38)RM  100,000.01 - RM   150,000.00'
    if v <= 200000:  return '39)RM  150,000.01 - RM   200,000.00'
    if v <= 300000:  return '40)RM  200,000.01 - RM   300,000.00'
    if v <= 500000:  return '41)RM  300,000.01 - RM   500,000.00'
    if v <= 1000000: return '42)RM  500,000.01 - RM 1,000,000.00'
    if v <= 2000000: return '43)RM1,000,000.01 - RM 2,000,000.00'
    if v <= 3000000: return '44)RM2,000,000.01 - RM 3,000,000.00'
    if v <= 4000000: return '45)RM3,000,000.01 - RM 4,000,000.00'
    if v <= 5000000: return '46)RM4,000,000.01 - RM 5,000,000.00'
    if v <= 10000000:return '47)RM5,000,000.01 - RM10,000,000.00'
    return              '48)          ABOVE RM 10,000,000.00'


def format_iwsrnge(amount: Optional[float]) -> str:
    """IWSRNGE: Islamic Wadiah savings range (7 bands). Exclusive upper bound."""
    if amount is None:
        return ''
    v = float(amount)
    if v < 3001:    return ' 1) UP TO   RM  3,000'
    if v < 10001:   return ' 2) UP TO   RM 10,000'
    if v < 30001:   return ' 3) UP TO   RM 30,000'
    if v < 50001:   return ' 4) UP TO   RM 50,000'
    if v < 75001:   return ' 5) UP TO   RM 75,000'
    if v < 100001:  return ' 6) UP TO   RM100,000'
    return              ' 7) ABOVE RM100,000  '


def format_iwsrngx(amount: Optional[float]) -> str:
    """IWSRNGX: Islamic Wadiah savings tier (interest tier labels)."""
    if amount is None:
        return ''
    v = float(amount)
    if v <= 5000:    return ' 1) FIRST RM  5,000'
    if v <= 10000:   return ' 2) NEXT  RM  5,000'
    if v <= 50000:   return ' 3) NEXT  RM 40,000'
    if v <= 150000:  return ' 4) NEXT  RM100,000'
    if v <= 350000:  return ' 5) NEXT  RM200,000'
    if v <= 650000:  return ' 6) NEXT  RM300,000'
    if v <= 1200000: return ' 6) NEXT  RM550,000'
    return              ' 7) THEREAFTER     '


def format_ibwsrngd(amount: Optional[float]) -> str:
    """IBWSRNGD: Interest-bearing savings range display (6 bands, exclusive)."""
    if amount is None:
        return ''
    v = float(amount)
    if v < 10001:   return ' 1) UP TO   RM 10,000'
    if v < 30001:   return ' 2) UP TO   RM 30,000'
    if v < 50001:   return ' 3) UP TO   RM 50,000'
    if v < 75001:   return ' 4) UP TO   RM 75,000'
    if v < 100001:  return ' 5) UP TO   RM100,000'
    return              ' 6) ABOVE RM100,000  '


# ===========================================================================
# INVALUE: IBWRNGE  (numeric -> discretised bucket value for IBWRNGE)
# ===========================================================================
def invalue_ibwrnge(amount: Optional[float]) -> int:
    """Bucket amount into IBWRNGE discrete values."""
    if amount is None:
        return 10000
    v = float(amount)
    if v <= 10000:  return 10000
    if v <= 30000:  return 30000
    if v <= 50000:  return 50000
    if v <= 75000:  return 75000
    if v <= 100000: return 100000
    return 100001


# ===========================================================================
# INVALUE: CARANGE  (numeric -> discrete bucket for current account range)
# ===========================================================================
def invalue_carange(amount: Optional[float]) -> int:
    """Bucket amount into CARANGE discrete values."""
    if amount is None:
        return 2000
    v = float(amount)
    if v <= 2000:   return 2000
    if v <= 3000:   return 3000
    if v <= 5000:   return 5000
    if v <= 10000:  return 10000
    if v <= 30000:  return 30000
    if v <= 50000:  return 50000
    if v <= 75000:  return 75000
    if v <= 100000: return 100000
    if v <= 150000: return 150000
    if v <= 200000: return 200000
    return 200001


# ===========================================================================
# INVALUE: ISARANGE  (numeric -> discrete bucket for Islamic savings range)
# ===========================================================================
def invalue_isarange(amount: Optional[float]) -> int:
    """Bucket amount into ISARANGE discrete values."""
    if amount is None:
        return 1000
    v = float(amount)
    if v <= 1000:  return 1000
    if v <= 5000:  return 5000
    if v <= 25000: return 25000
    if v <= 50000: return 50000
    return 50001


# ===========================================================================
# FORMAT: ISARANGD  (display label for ISARANGE bucket)
# ===========================================================================
def format_isarangd(bucket: Optional[int]) -> str:
    """Display label for ISARANGE discrete bucket value."""
    _map = {
        1000:  ' 1)       BELOW RM1,000',
        5000:  ' 2)   RM1,000 - RM5,000',
        25000: ' 3)  RM5,000 - RM25,000',
        50000: ' 4) RM25,000 - RM50,000',
        50001: ' 5) RM50,000 AND ABOVE ',
    }
    return _map.get(bucket, '')


# ===========================================================================
# FORMAT: CADPRG  (current account deposit range -> label)
# ===========================================================================
def format_cadprg(bucket: Optional[int]) -> str:
    """Map CADPRG discrete bucket to label string."""
    _map = {
        1000:   ' 1)         RM0 -     RM1,000',
        2000:   ' 2)     RM1,001 -     RM2,000',
        2500:   ' 3)     RM2,001 -     RM2,500',
        3000:   ' 4)     RM2,501 -     RM5,000',
        5000:   ' 4)     RM2,501 -     RM5,000',
        10000:  ' 5)     RM5,001 -    RM10,000',
        20000:  ' 6)    RM10,001 -    RM20,000',
        30000:  ' 7)    RM20,001 -    RM30,000',
        40000:  ' 8)    RM30,001 -    RM40,000',
        50000:  ' 9)    RM40,001 -    RM50,000',
        75000:  '10)   RM50,001 -   RM100,000',
        100000: '10)   RM50,001 -   RM100,000',
        150000: '11)  RM100,001 -   RM150,000',
        200000: '12)  RM150,001 -   RM200,000',
        250000: '13)  RM200,001 -   RM250,000',
        500000: '14)  RM250,001 -   RM500,000',
        1000000:  '15)  RM500,001 - RM1,000,000',
        2000000:  '16)RM1,000,001 - RM2,000,000',
        3000000:  '17)RM2,000,001 - RM3,000,000',
        4000000:  '18)RM3,000,001 - RM4,000,000',
        5000000:  '19)RM4,000,001 - RM5,000,000',
    }
    return _map.get(bucket, '20)RM5,000,001 AND ABOVE')


# ===========================================================================
# FORMAT: ICADPRG  (Islamic CA deposit range -> label, 11 bands)
# ===========================================================================
def format_icadprg(bucket: Optional[int]) -> str:
    """Map ICADPRG bucket to label string."""
    _map = {
        1000:   ' 1)     BELOW  RM2,000',
        2000:   ' 1)     BELOW  RM2,000',
        2500:   ' 2) RM2,000 -  RM3,000',
        3000:   ' 2) RM2,000 -  RM3,000',
        5000:   ' 3) RM3,000 -  RM5,000',
        10000:  ' 4) RM5,000 -  RM10,000',
        20000:  ' 5) RM10,000 - RM30,000',
        30000:  ' 5) RM10,000 - RM30,000',
        40000:  ' 6) RM30,000 - RM50,000',
        50000:  ' 6) RM30,000 - RM50,000',
        75000:  ' 7) RM50,000 - RM75,000',
        100000: ' 8) RM75,000 - RM100,000',
        150000: ' 9) RM100,000 -  RM150,000',
        200000: '10) RM150,000 -  RM200,000',
    }
    return _map.get(bucket, '11) RM200,000 AND ABOVE')


# ===========================================================================
# FORMAT: CARANGED  (current account range display, 11 steps)
# ===========================================================================
def format_caranged(bucket: Optional[int]) -> str:
    """Map CARANGE bucket to CARANGED display label."""
    _map = {
        2000:   ' 1)         BELOW RM2,000',
        3000:   ' 2)     RM2,000 - RM3,000',
        5000:   ' 3)     RM3,000 - RM5,000',
        10000:  ' 4)    RM5,000 - RM10,000',
        30000:  ' 5)   RM10,000 - RM30,000',
        50000:  ' 6)   RM30,000 - RM50,000',
        75000:  ' 7)   RM50,000 - RM75,000',
        100000: ' 8)  RM75,000 - RM100,000',
        150000: ' 9) RM100,000 - RM150,000',
        200000: '10) RM150,000 - RM200,000',
        200001: '11) RM200,000 AND ABOVE  ',
    }
    return _map.get(bucket, '11) RM200,000 AND ABOVE  ')


# ===========================================================================
# FORMAT: PROFNORM, PROFYAA, PROFPLUS, PROFWISE  (income range formats)
# ===========================================================================
def format_profnorm(income: Optional[float]) -> str:
    """PROFNORM: Normal savings income profile."""
    if income is None:
        return ''
    v = float(income)
    if v <= 5000:  return '1)UP TO RM 5,000.00'
    if v <= 10000: return '2)UP TO RM10,000.00'
    if v <= 30000: return '3)UP TO RM30,000.00'
    if v <= 50000: return '4)UP TO RM50,000.00'
    if v <= 75000: return '5)UP TO RM75,000.00'
    return              '6)ABOVE RM75,000.00'


def format_sexnorm(age: Optional[float]) -> str:
    """SEXNORM: Normal savings age group."""
    if age is None:
        return ''
    v = float(age)
    if v <= 12:  return '1)BELOW 12 YRS'
    if v <= 18:  return '2)12 - 18 YRS'
    if v <= 50:  return '3)18 - 50 YRS'
    return            '4)50 YRS AND ABOVE'


def format_profyaa(income: Optional[float]) -> str:
    """PROFYAA: Young Achiever Account income profile."""
    if income is None:
        return ''
    v = float(income)
    if v <= 500:   return '1)BELOW RM   500.00'
    if v <= 2000:  return '2)UP TO RM 2,000.00'
    if v <= 5000:  return '3)UP TO RM 5,000.00'
    if v <= 10000: return '4)UP TO RM10,000.00'
    if v <= 30000: return '5)UP TO RM30,000.00'
    if v <= 50000: return '6)UP TO RM50,000.00'
    if v <= 75000: return '7)UP TO RM75,000.00'
    return              '8)ABOVE RM75,000.00'


def format_sexyw(age: Optional[float]) -> str:
    """SEXYW: Young Achiever age group."""
    if age is None:
        return ''
    v = float(age)
    if v <= 12: return '1)BELOW 12 YRS'
    if v <= 18: return '2)12 - 18 YRS'
    return ''


def format_profplus(income: Optional[float]) -> str:
    """PROFPLUS: 50-plus savings income profile."""
    if income is None:
        return ''
    v = float(income)
    if v <= 5000:  return '1)UP TO RM 5,000.00'
    if v <= 10000: return '2)UP TO RM10,000.00'
    if v <= 30000: return '3)UP TO RM30,000.00'
    if v <= 50000: return '4)UP TO RM50,000.00'
    if v <= 75000: return '5)UP TO RM75,000.00'
    return              '6)ABOVE RM75,000.00'


def format_profwise(income: Optional[float]) -> str:
    """PROFWISE: WISE savings income profile."""
    if income is None:
        return ''
    v = float(income)
    if v <= 5000:  return '1)UP TO RM 5,000.00'
    if v <= 10000: return '2)UP TO RM10,000.00'
    if v <= 20000: return '3)UP TO RM20,000.00'
    if v <= 30000: return '4)UP TO RM30,000.00'
    if v <= 50000: return '5)UP TO RM50,000.00'
    return              '6)ABOVE RM50,000.00'


# ===========================================================================
# FORMAT: SDNAME  (savings product code -> product name)
# ===========================================================================
SDNAME_MAP: dict[int, str] = {
    200: 'PLUS SAVING',
    201: 'STAFF',
    202: 'YOUNG ACHIEVER',
    203: '50 PLUS',
    204: 'AL-WADIAH',
    205: 'BASIC SAVING',
    206: 'BASIC 55 SAVING',
    208: 'PB BRIGHT STAR SA',
    210: 'PB MYSALARY SA',
    212: 'WISE',
    213: 'PB SAVELINK',
    214: 'BESTARI SAVING',
    215: 'STAFF WADIAH',
    216: 'PB UNIONPAY SA',
    227: 'STAFF MONEYPLUS SA',
    228: 'MONEYPLUS SA',
    480: 'GIA',
    481: 'STAFF GIA',
}

def format_sdname(product: Optional[int]) -> str:
    """Map savings product code to product name."""
    if product is None:
        return ''
    return SDNAME_MAP.get(product, '')


# ===========================================================================
# FORMAT: $RACE  (race code -> race description)
# ===========================================================================
_RACE_MAP: dict[str, str] = {
    '0': 'OTHERS',
    '1': 'MALAY',
    '2': 'CHINESE',
    '3': 'INDIAN',
}

def format_race(code: Optional[str]) -> str:
    """Map race code to description."""
    if code is None:
        return 'OTHERS'
    return _RACE_MAP.get(str(code), 'OTHERS')


# ===========================================================================
# FORMAT: $PURPOSE  (purpose code -> description)
# ===========================================================================
def format_purpose(code: Optional[str]) -> str:
    """Map purpose code to description."""
    if code is None:
        return 'OTHERS  '
    c = str(code)
    if c in ('1', '4'):
        return 'PERSONAL'
    if c == '2':
        return 'JOINT'
    return 'OTHERS  '


# ===========================================================================
# FORMAT: PROD  (special product labels)
# ===========================================================================
_PROD_MAP: dict[int, str] = {
    150: 'ACE ACCOUNT',
    160: 'AL-WADIAH CURRENT A/C',
}

def format_prod(product: Optional[int]) -> str:
    """Map special product code to product label."""
    if product is None:
        return ''
    return _PROD_MAP.get(product, '')


# ===========================================================================
# FORMAT: AGEDESC  (age group -> description, uses macro vars)
# ===========================================================================
def format_agedesc(age_group: Optional[int]) -> str:
    """Map age group code to description."""
    if age_group is None:
        return ''
    if age_group == 0:
        return 'WITHOUT BIRTHDATE'
    if age_group == AGEBELOW:
        return 'BELOW 12 YEARS'
    if age_group == AGELIMIT:
        return '12 TO BELOW 18 YEARS'
    if age_group == MAXAGE:
        return '18 AND ABOVE'
    return ''


# ===========================================================================
# FORMAT: $STATE  (single-letter state code -> full state name)
# ===========================================================================
_STATE_MAP: dict[str, str] = {
    'A': 'PERAK',
    'B': 'SELANGOR',
    'C': 'PAHANG',
    'D': 'KELANTAN',
    'J': 'JOHOR',
    'K': 'KEDAH',
    'L': 'LABUAN',
    'M': 'MELAKA',
    'N': 'NEGERI SEMBILAN',
    'P': 'PULAU PINANG',
    'Q': 'SARAWAK',
    'R': 'PERLIS',
    'S': 'SABAH',
    'T': 'TERENGGANU',
    'W': 'WILAYAH PERSEKUTUAN',
}

def format_state(code: Optional[str]) -> str:
    """Map single-letter state code to full state name."""
    if code is None:
        return ''
    return _STATE_MAP.get(str(code).upper(), '')


# ===========================================================================
# FORMAT: FDPROD  (FD product code -> term label, used in PBMISFMT context)
# Note: This is the simplified PBMISFMT FDPROD, different from PBBDPFMT FDPROD
# which maps to BNM BIC codes.
# ===========================================================================
_FDPROD_MIS_MAP: dict[int, str] = {
    340: '1 MONTH',
    341: '3 MONTHS',
    342: '6 MONTHS',
    343: '9 MONTHS',
    344: '12 MONTHS',
    345: '15 MONTHS',
    346: '18 MONTHS',
    347: '21 MONTHS',
    348: '24 MONTHS',
    349: '36 MONTHS',
    350: '48 MONTHS',
    351: '60 MONTHS',
}

def format_fdprod_mis(product: Optional[int]) -> str:
    """Map FD product code to term label (PBMISFMT context)."""
    if product is None:
        return ' '
    return _FDPROD_MIS_MAP.get(product, ' ')


# ===========================================================================
# FORMAT: $CPARTYF / $CPARTY  (counterparty code -> BNM group code / label)
# ===========================================================================
_CPARTYF_MAP: dict[str, str] = {
    '01': '01', '02': '10', '03': '10', '04': '20', '05': '20',
    '06': '20', '10': '10', '11': '10', '12': '10', '13': '20',
    '15': '20', '17': '20', '20': '20', '30': '20', '32': '20',
    '33': '20', '34': '20', '35': '20', '36': '20', '37': '20',
    '38': '20', '39': '20', '40': '20', '50': '60', '57': '60',
    '59': '60', '60': '60', '61': '60', '62': '60', '63': '60',
    '64': '60', '65': '60', '66': '60', '67': '60', '68': '60',
    '69': '60', '70': '70', '71': '70', '72': '70', '73': '70',
    '74': '70', '75': '60', '76': '76', '77': '76', '78': '76',
    '79': '79', '80': '80', '81': '80', '85': '80', '86': '80',
    '90': '80', '91': '80', '92': '80', '95': '80', '96': '80',
    '98': '80', '99': '80',
}

def format_cpartyf(code: Optional[str]) -> str:
    """Map counterparty code to BNM group code."""
    if code is None:
        return '76'
    return _CPARTYF_MAP.get(str(code), '76')


_CPARTY_MAP: dict[str, str] = {
    '01': 'BANK NEGARA MALAYSIA',
    '10': 'DOMESTIC BANKING INSTITUTION',
    '20': 'DOMESTIC NON-BANK FI',
    '60': 'DOMESTIC BUSINESS ENTERPRISES',
    '70': 'GOVERNMENT',
    '76': 'INDIVIDUALS',
    '79': 'DOMESTIC OTHER ENTITIES NIE',
    '80': 'NON RESIDENTS/FOREIGN ENTITIES',
}

def format_cparty(code: Optional[str]) -> str:
    """Map BNM group code to counterparty description."""
    if code is None:
        return ''
    return _CPARTY_MAP.get(str(code), '')


# ===========================================================================
# FORMAT: LNPOGRP  (branch number -> loan portfolio group label)
# ===========================================================================
_LNPOGRP_RAW: list[tuple[set, str]] = [
    ({2,3,22,46,53,56,120,129,136,169,170,173,196,226,232,252,262,802,811,818,284,285,291},
     '01-SELANGOR/WILAYAH REG.   I'),
    ({15,29,40,41,66,83,96,97,101,103,118,128,141,151,195,197,248,269,701,812,821,822},
     '02-SELANGOR/WILAYAH REG.  II'),
    ({18,19,35,36,81,124,125,131,135,145,148,162,167,180,220,241,267,280,292,295,815,816},
     '03-SELANGOR/WILAYAH REG. III'),
    ({7,37,52,59,61,79,87,89,91,93,102,105,110,144,147,174,176,216,217,222,234,286,287,290,804,805},
     '04-SOUTHERN REGION I       '),
    ({5,9,49,51,67,71,76,80,85,95,123,137,146,152,158,207,208,209,210,244,245,251,809,823},
     '05-CENTRAL REGION           '),
    ({32,50,58,90,130,175,183,184,185,186,189,190,191,192,193,194,259,813,273,274,275,281},
     '06-SARAWAK REGION           '),
    ({33,44,55,62,72,112,115,140,142,143,149,161,228,803,278,276,282,283},
     '07-SABAH REGION             '),
    ({4,16,17,21,24,28,39,45,47,63,64,65,75,111,156,160,165,172,224,231,242,247,254,800,807},
     '08-SOUTHERN REGION II       '),
    ({6,10,34,54,70,74,77,86,104,107,114,126,150,159,171,205,238,258,265,266,806,808,817,704},
     '09-NORTHERN REGION I        '),
    ({8,13,14,30,48,106,113,116,117,139,233,237,239,257,260,261,263,264,819,277,703},
     '10-EAST COAST REGION        '),
    ({26,38,94,122,138,153,155,157,163,168,178,179,198,225,230,270,296,814,820,279,288,289,702},
     '11-SELANGOR/WILAYAH REG.  IV'),
    ({23,27,42,57,60,68,88,108,121,154,164,177,204,206,211,249,256,801,824,11,243},
     '12-NORTHERN REGION II       '),
    ({20,25,31,43,69,73,78,92,109,127,133,199,201,202,203,221,235,240,268,293,294},
     '13-SELANGOR/WILAYAH REG.   V'),
]

def format_lnpogrp(branch: Optional[int]) -> str:
    """Map branch number to loan portfolio group label."""
    if branch is None:
        return '99-OTHER'
    for branch_set, label in _LNPOGRP_RAW:
        if branch in branch_set:
            return label
    return '99-OTHER'


# ===========================================================================
# PICTURE FORMATS (numeric -> formatted string)
# These replicate SAS PICTURE statement behaviour.
# ===========================================================================

def format_hundred(value: Optional[float]) -> str:
    """HUNDRED PICTURE: format as 9,999,999.99 (MULT=1)."""
    if value is None:
        return '0,000,000.00'
    prefix = '-' if value < 0 else ''
    return f"{prefix}{abs(value):>12,.2f}"


def format_thousand(value: Optional[float]) -> str:
    """THOUSAND PICTURE: divide by 1000, format as integer with commas."""
    if value is None:
        return '0,000,000,000'
    v = value / 1000.0
    prefix = '-' if v < 0 else ''
    return f"{prefix}{abs(v):>13,.0f}"


def format_million(value: Optional[float]) -> str:
    """MILLION PICTURE: divide by 1,000,000, format as integer with commas."""
    if value is None:
        return '0,000,000,000'
    v = value / 1_000_000.0
    prefix = '-' if v < 0 else ''
    return f"{prefix}{abs(v):>13,.0f}"


# ===========================================================================
# CONVENIENCE: unified apply helper
# ===========================================================================
def apply_fmt(val, fmt_fn, default: str = '') -> str:
    """Apply a format function, returning default on None or exception."""
    try:
        result = fmt_fn(val)
        return result if result is not None else default
    except (TypeError, ValueError):
        return default


# ===========================================================================
# EXPORTS
# ===========================================================================
__all__ = [
    # Macro vars
    'AGELIMIT', 'MAXAGE', 'AGEBELOW',
    # Format functions
    'format_brchcd',
    'format_groupf',
    'format_acknof',
    'format_saprod_mis',
    'format_caprod_mis',
    'format_odprod_mis',
    'format_lnprod_mis',
    'format_sadprg',
    'format_iwsrnge',
    'format_iwsrngx',
    'format_ibwsrngd',
    'format_isarangd',
    'format_cadprg',
    'format_icadprg',
    'format_caranged',
    'format_profnorm',
    'format_sexnorm',
    'format_profyaa',
    'format_sexyw',
    'format_profplus',
    'format_profwise',
    'format_sdname',
    'format_race',
    'format_purpose',
    'format_prod',
    'format_agedesc',
    'format_state',
    'format_fdprod_mis',
    'format_cpartyf',
    'format_cparty',
    'format_lnpogrp',
    'format_hundred',
    'format_thousand',
    'format_million',
    # INVALUE bucket functions
    'invalue_ibwrnge',
    'invalue_carange',
    'invalue_isarange',
    # Dicts (for direct lookups if needed)
    'GROUPF_MAP',
    'SDNAME_MAP',
    # Utility
    'apply_fmt',
]


if __name__ == '__main__':
    print(f"BRCHCD(1)   = {format_brchcd(1)}")       # HOE
    print(f"BRCHCD(2)   = {format_brchcd(2)}")       # JSS
    print(f"BRCHCD(3000)= {format_brchcd(3000)}")    # HOE
    print(f"BRCHCD(3002)= {format_brchcd(3002)}")    # JSS
    print(f"GROUPF(JSS) = {format_groupf('JSS')}")   # GROUP 1
    print(f"GROUPF(HOE) = {format_groupf('HOE')}")   # HOE
    print(f"RACE(1)     = {format_race('1')}")        # MALAY
    print(f"PURPOSE(1)  = {format_purpose('1')}")     # PERSONAL
    print(f"IWSRNGE(3000)  = {format_iwsrnge(3000)}")  # band 1
    print(f"IWSRNGE(10001) = {format_iwsrnge(10001)}") # band 3
    print(f"CARANGE(bucket 5000) -> {format_caranged(invalue_carange(5000))}")
    print(f"FDPROD_MIS(340) = {format_fdprod_mis(340)}")
    print(f"STATE(B)    = {format_state('B')}")       # SELANGOR
    print(f"LNPOGRP(2)  = {format_lnpogrp(2)}")
