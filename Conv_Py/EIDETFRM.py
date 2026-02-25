#!/usr/bin/env python3
"""
Program: EIDETFRM
Purpose: Extract Remittance Foreign & Local Transaction IFS for DETICA
         Processes foreign remittance transactions (TT, WU, PBMT, BT types)
            and outputs a pipe-delimited text file for the DETICA AML system.
"""

import re
import duckdb
import polars as pl
from pathlib import Path
from datetime import date

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR = Path("/data/sap")

# Input parquet paths
DP_MNITB_PATH      = BASE_DIR / "pbb/mnitb/current.parquet"       # SAP.PBB.MNITB(0)
IDP_MNITB_PATH     = BASE_DIR / "pibb/mnitb/current.parquet"      # SAP.PIBB.MNITB(0)
LN_LNNOTE_PATH     = BASE_DIR / "pbb/mniln/lnnote.parquet"        # SAP.PBB.MNILN LNNOTE
ILN_LNNOTE_PATH    = BASE_DIR / "pibb/mniln/lnnote.parquet"       # SAP.PIBB.MNILN LNNOTE
DP_CURRENT_PATH    = BASE_DIR / "pbb/mnitb/dp_current.parquet"
IDP_CURRENT_PATH   = BASE_DIR / "pibb/mnitb/dp_current.parquet"
DP_SAVING_PATH     = BASE_DIR / "pbb/mnitb/dp_saving.parquet"
IDP_SAVING_PATH    = BASE_DIR / "pibb/mnitb/dp_saving.parquet"
DP_FD_PATH         = BASE_DIR / "pbb/mnitb/dp_fd.parquet"
IDP_FD_PATH        = BASE_DIR / "pibb/mnitb/dp_fd.parquet"
DP_UMA_PATH        = BASE_DIR / "pbb/mnitb/dp_uma.parquet"
IDP_UMA_PATH       = BASE_DIR / "pibb/mnitb/dp_uma.parquet"
DP_VOSTRO_PATH     = BASE_DIR / "pbb/mnitb/dp_vostro.parquet"
CIS_CUSTDLY_PATH   = BASE_DIR / "rbp2/b033/cis/custdly.parquet"   # RBP2.B033.CIS.CUST.DAILY

# Remittance transaction input - dataset name derived from report date
REM_BASE_PATH      = BASE_DIR / "pbb/crm"

# Output paths
OUTPUT_FORRMT_PATH = BASE_DIR / "aml/detica/remtran_foreign_text.txt"   # SAP.AML.DETICA.REMTRAN.FOREIGN.TEXT
OUTPUT_BKP_PATH    = BASE_DIR / "aml/detica/remtran_foreign_text_bkp.txt"  # SAP.AML.DETICA.REMTRAN.FOREIGN.TEXT.BKP

# ============================================================================
# PBBELF DEPENDENCY - Branch reverse map (from PBBELF.py)
# ============================================================================
# Reference: PBBELF.py - BRCHRVR_MAP and format_brchrvr()
# Reference: PBBELF.py - BRCHCD_MAP and format_brchcd()
from PBBELF import (
    BRCHCD_MAP, format_brchcd,
    CACBRCH_MAP, format_cacbrch,
    REGIOFF_MAP, format_regioff,
    CTYPE_MAP, format_ctype,
    BRCHRVR_MAP, format_brchrvr,
)

BRCHRVR_MAP = {
    'PCS': 1, 'JSS': 2, 'JRC': 3, 'MLK': 4, 'IMO': 5, 'PPG': 6, 'JBU': 7,
    'KTN': 8, 'JYK': 9, 'ASR': 10, 'GRN': 11, 'PPH': 12, 'KBU': 13, 'TMH': 14,
    'KPG': 15, 'NLI': 16, 'TPN': 17, 'PJN': 18, 'DUA': 19, 'TCL': 20, 'BPT': 21,
    'SMY': 22, 'KMT': 23, 'RSH': 24, 'SAM': 25, 'SPG': 26, 'NTL': 27, 'MUA': 28,
    'JRL': 29, 'KTU': 30, 'SKC': 31, 'WSS': 32, 'KKU': 33, 'KGR': 34, 'SSA': 35,
    'SS2': 36, 'TSA': 37, 'JKL': 38, 'KKG': 39, 'JSB': 40, 'JIH': 41, 'BMM': 42,
    'BTG': 43, 'TWU': 44, 'SRB': 45, 'APG': 46, 'SGM': 47, 'MTK': 48, 'JLP': 49,
    'MRI': 50, 'SMG': 51, 'UTM': 52, 'TMI': 53, 'BBB': 54, 'LBN': 55, 'KJG': 56,
    'SPI': 57, 'SBU': 58, 'PKL': 59, 'BAM': 60, 'KLI': 61, 'SDK': 62, 'GMS': 63,
    'PDN': 64, 'BHU': 65, 'BDA': 66, 'CMR': 67, 'SAT': 68, 'BKI': 69, 'PSA': 70,
    'BCG': 71, 'PPR': 72, 'SPK': 73, 'SIK': 74, 'CAH': 75, 'PRS': 76, 'PLI': 77,
    'SJA': 78, 'MSI': 79, 'MLB': 80, 'SBH': 81, 'MCG': 82, 'JBB': 83, 'PMS': 84,
    'SST': 85, 'CLN': 86, 'MSG': 87, 'KUM': 88, 'TPI': 89, 'BTL': 90, 'KUG': 91,
    'KLG': 92, 'EDU': 93, 'STP': 94, 'TIN': 95, 'SGK': 96, 'HSL': 97, 'TCY': 98,
    'XXX': 99, 'YYY': 100, 'KBR': 101, 'PRJ': 102, 'JJG': 103, 'KKL': 104, 'KTI': 105,
    'CKI': 106, 'JLT': 107, 'BSI': 108, 'KSR': 109, 'TJJ': 110, 'AKH': 111, 'LDO': 112,
    'TML': 113, 'BBA': 114, 'KNG': 115, 'TRI': 116, 'KKI': 117, 'TMW': 118, 'BNV': 119,
    'PIH': 120, 'PRA': 121, 'SKN': 122, 'IGN': 123, 'S14': 124, 'KJA': 125, 'PTS': 126,
    'TSM': 127, 'SGB': 128, 'BSR': 129, 'PDG': 130, 'TMG': 131, 'CKT': 132, 'PKG': 133,
    'RPG': 134, 'BSY': 135, 'TCS': 136, 'JPP': 137, 'WMU': 138, 'JRT': 139, 'CPE': 140,
    'STL': 141, 'KBD': 142, 'LDU': 143, 'KHG': 144, 'BSD': 145, 'PSG': 146, 'PNS': 147,
    'PJO': 148, 'BFT': 149, 'LMM': 150, 'SLY': 151, 'ATR': 152, 'USJ': 153, 'BSJ': 154,
    'TTJ': 155, 'TMR': 156, 'BPJ': 157, 'SPL': 158, 'RLU': 159, 'MTH': 160, 'DGG': 161,
    'SEA': 162, 'JKA': 163, 'KBS': 164, 'TKA': 165, 'PGG': 166, 'BBG': 167, 'KLC': 168,
    'CTD': 169, 'PJA': 170, 'JMR': 171, 'TMJ': 172, 'SCA': 173, 'BBP': 174, 'LBG': 175,
    'TPG': 176, 'JRU': 177, 'MIN': 178, 'OUG': 179, 'KBG': 180, 'SRO': 181, 'JPU': 182,
    'JCL': 183, 'JPN': 184, 'KCY': 185, 'JTZ': 186, 'BNH': 189, 'BTR': 190, 'KPT': 191,
    'MRD': 192, 'MKH': 193, 'SRK': 194, 'BWK': 195, 'JHL': 196, 'TNM': 197, 'TDA': 198,
    'JTH': 199, 'JSK': 200, 'PDA': 201, 'RWG': 202, 'SJM': 203, 'BTW': 204, 'SNG': 205,
    'TBM': 206, 'BCM': 207, 'JSI': 208, 'STW': 209, 'TMM': 210, 'TPD': 211, 'JMA': 212,
    'JKB': 213, 'JGA': 214, 'JKP': 215, 'SKI': 216, 'TMB': 217, 'ZZZ': 218, 'BC1': 219,
    'GHS': 220, 'TSK': 221, 'TDC': 222, 'TRJ': 223, 'JAH': 224, 'TIH': 225, 'JPR': 226,
    'KSB': 227, 'INN': 228, 'TSJ': 229, 'SSH': 230, 'BBM': 231, 'TMD': 232, 'BEN': 233,
    'SRM': 234, 'SBM': 235, 'UYB': 236, 'KLS': 237, 'JKT': 238, 'KMY': 239, 'KAP': 240,
    'DJA': 241, 'TKK': 242, 'KKR': 243, 'GRT': 244, 'BDR': 245, 'BGH': 246, 'BPR': 247,
    'JTS': 248, 'TAI': 249, 'TEA': 250, 'KPR': 251, 'TMA': 252, 'JTT': 253, 'KPH': 254,
    'SBP': 255, 'PBR': 256, 'RAU': 257, 'JTA': 258, 'SAN': 259, 'KDN': 260, 'GMG': 261,
    'TCT': 262, 'BTA': 263, 'JBH': 264, 'JAI': 265, 'JDK': 266, 'TDI': 267, 'BBT': 268,
    'MKA': 269, 'BPI': 270, 'LHA': 273, 'STG': 274, 'MSL': 275, 'JAS': 276, 'WSU': 277,
    'JPI': 278, 'PTJ': 279, 'KDA': 280, 'PLT': 281, 'PTT': 282, 'PSE': 283, 'BSP': 284,
    'BMC': 285, 'BIH': 286, 'SUA': 287, 'SPT': 288, 'TEE': 289, 'TDY': 290, 'BSL': 291,
    'BMJ': 292, 'BSA': 293, 'KKM': 294, 'BKR': 295, 'BJL': 296,
    'IKB': 701, 'IPJ': 702, 'IWS': 703, 'IJK': 704,
    'H01': 800, 'H02': 801, 'H03': 802, 'H04': 803, 'H05': 804, 'H06': 805, 'H07': 806,
    'H08': 807, 'H09': 808, 'H10': 809, 'H11': 811, 'H12': 812, 'H13': 813, 'H14': 814,
    'H15': 815, 'H16': 816, 'H17': 817, 'H18': 818, 'H19': 819, 'H20': 820, 'H21': 821,
    'H22': 822, 'H23': 823, 'H24': 824, 'H25': 825, 'H26': 826, 'H27': 827, 'H28': 828,
    'H44': 844, 'H45': 845, 'H46': 846, 'H47': 847, 'H48': 848, 'H49': 849, 'H50': 850,
    'H51': 851, 'H52': 852, 'H53': 853, 'H54': 854, 'H55': 855, 'H56': 856, 'H57': 857,
    'H58': 858, 'H59': 859, 'H60': 860, 'H61': 861, 'H62': 862, 'H63': 863,
    'HOE': 884,
    'CBR': 902, 'CCC': 903, 'SCD': 904, 'SCC': 905, 'CAD': 906, 'SDC': 907, 'ICC': 908,
    'PCC': 909, 'JCC': 910, 'KCA': 911, 'CCA': 912, 'LCA': 913, 'JCA': 914, 'PCA': 915,
    'BCA': 916, 'KCC': 917, 'MCC': 918, 'HCC': 919, 'LCC': 920,
    'TFC': 992, 'XPP': 993, 'XJB': 994, 'SMC': 996, 'CCP': 997, 'CPC': 998,
}

# BRCHCD_MAP: numeric branch code -> branch name string (for PUT(x, BRCHCD.) format)
BRCHCD_MAP = {
    1: 'HOE', 7000: 'HOE', 7001: 'HOE', 7002: 'HOE', 7003: 'HOE', 7004: 'HOE',
    7005: 'HOE', 7006: 'HOE', 7007: 'HOE', 7008: 'HOE', 7009: 'HOE',
    8000: 'HOE', 8001: 'HOE', 8002: 'HOE', 8003: 'HOE', 8004: 'HOE',
    8005: 'HOE', 8006: 'HOE', 8007: 'HOE', 8008: 'HOE', 8009: 'HOE',
    9000: 'HOE', 9001: 'HOE', 9002: 'HOE', 9003: 'HOE', 9004: 'HOE',
    9994: 'HOE', 9995: 'HOE', 9996: 'HOE', 9998: 'HOE', 9999: 'HOE',
    3000: 'IBU', 3001: 'IBU', 3999: 'IBU',
    4000: 'IBU', 4001: 'IBU', 4002: 'IBU', 4003: 'IBU', 4004: 'IBU',
    4005: 'IBU', 4006: 'IBU', 4007: 'IBU', 4008: 'IBU', 4009: 'IBU',
    800: 'H01', 3800: 'H01', 801: 'H02', 3801: 'H02', 802: 'H03', 3802: 'H03',
    803: 'H04', 3803: 'H04', 804: 'H05', 3804: 'H05', 805: 'H06', 3805: 'H06',
    806: 'H07', 3806: 'H07', 807: 'H08', 3807: 'H08', 808: 'H09', 3808: 'H09',
    809: 'H10', 3809: 'H10', 811: 'H11', 3811: 'H11', 812: 'H12', 3812: 'H12',
    813: 'H13', 3813: 'H13', 814: 'H14', 3814: 'H14', 815: 'H15', 3815: 'H15',
    816: 'H16', 3816: 'H16', 817: 'H17', 3817: 'H17', 818: 'H18', 3818: 'H18',
    819: 'H19', 3819: 'H19', 820: 'H20', 3820: 'H20', 821: 'H21', 3821: 'H21',
    822: 'H22', 3822: 'H22', 823: 'H23', 3823: 'H23', 824: 'H24', 3824: 'H24',
    825: 'H25', 3825: 'H25', 826: 'H26', 3826: 'H26', 827: 'H27', 3827: 'H27',
    828: 'H28', 3828: 'H28', 844: 'H44', 3844: 'H44', 845: 'H45', 3845: 'H45',
    846: 'H46', 3846: 'H46', 847: 'H47', 3847: 'H47', 848: 'H48', 3848: 'H48',
    849: 'H49', 3849: 'H49', 850: 'H50', 3850: 'H50', 851: 'H51', 3851: 'H51',
    852: 'H52', 3852: 'H52', 853: 'H53', 3853: 'H53', 854: 'H54', 3854: 'H54',
    855: 'H55', 3855: 'H55', 856: 'H56', 3856: 'H56', 857: 'H57', 3857: 'H57',
    858: 'H58', 3858: 'H58', 859: 'H59', 3859: 'H59', 860: 'H60', 3860: 'H60',
    861: 'H61', 3861: 'H61', 862: 'H62', 3862: 'H62', 863: 'H63', 3863: 'H63',
    2: 'JSS', 3002: 'JSS', 3: 'JRC', 3003: 'JRC', 4: 'MLK', 3004: 'MLK',
    5: 'IMO', 3005: 'IMO', 6: 'PPG', 3006: 'PPG', 7: 'JBU', 3007: 'JBU',
    8: 'KTN', 3008: 'KTN', 9: 'JYK', 3009: 'JYK', 10: 'ASR', 3010: 'ASR',
    11: 'GRN', 3011: 'GRN', 12: 'PPH', 3012: 'PPH', 13: 'KBU', 3013: 'KBU',
    14: 'TMH', 3014: 'TMH', 15: 'KPG', 3015: 'KPG', 16: 'NLI', 3016: 'NLI',
    17: 'TPN', 3017: 'TPN', 18: 'PJN', 3018: 'PJN', 19: 'DUA', 3019: 'DUA',
    20: 'TCL', 3020: 'TCL', 21: 'BPT', 3021: 'BPT', 22: 'SMY', 3022: 'SMY',
    23: 'KMT', 3023: 'KMT', 24: 'RSH', 3024: 'RSH', 25: 'SAM', 3025: 'SAM',
    26: 'SPG', 3026: 'SPG', 27: 'NTL', 3027: 'NTL', 28: 'MUA', 3028: 'MUA',
    29: 'JRL', 3029: 'JRL', 30: 'KTU', 3030: 'KTU', 31: 'SKC', 3031: 'SKC',
    32: 'WSS', 3032: 'WSS', 33: 'KKU', 3033: 'KKU', 34: 'KGR', 3034: 'KGR',
    35: 'SSA', 3035: 'SSA', 36: 'SS2', 3036: 'SS2', 37: 'TSA', 3037: 'TSA',
    38: 'JKL', 3038: 'JKL', 39: 'KKG', 3039: 'KKG', 40: 'JSB', 3040: 'JSB',
    41: 'JIH', 3041: 'JIH', 42: 'BMM', 3042: 'BMM', 43: 'BTG', 3043: 'BTG',
    44: 'TWU', 3044: 'TWU', 45: 'SRB', 3045: 'SRB', 46: 'APG', 3046: 'APG',
    47: 'SGM', 3047: 'SGM', 48: 'MTK', 3048: 'MTK', 49: 'JLP', 3049: 'JLP',
    50: 'MRI', 3050: 'MRI', 51: 'SMG', 3051: 'SMG', 52: 'UTM', 3052: 'UTM',
    53: 'TMI', 3053: 'TMI', 54: 'BBB', 3054: 'BBB', 55: 'LBN', 3055: 'LBN',
    56: 'KJG', 3056: 'KJG', 57: 'SPI', 3057: 'SPI', 58: 'SBU', 3058: 'SBU',
    59: 'PKL', 3059: 'PKL', 60: 'BAM', 3060: 'BAM', 61: 'KLI', 3061: 'KLI',
    62: 'SDK', 3062: 'SDK', 63: 'GMS', 3063: 'GMS', 64: 'PDN', 3064: 'PDN',
    65: 'BHU', 3065: 'BHU', 66: 'BDA', 3066: 'BDA', 67: 'CMR', 3067: 'CMR',
    68: 'SAT', 3068: 'SAT', 69: 'BKI', 3069: 'BKI', 70: 'PSA', 3070: 'PSA',
    71: 'BCG', 3071: 'BCG', 72: 'PPR', 3072: 'PPR', 73: 'SPK', 3073: 'SPK',
    74: 'SIK', 3074: 'SIK', 75: 'CAH', 3075: 'CAH', 76: 'PRS', 3076: 'PRS',
    77: 'PLI', 3077: 'PLI', 78: 'SJA', 3078: 'SJA', 79: 'MSI', 3079: 'MSI',
    80: 'MLB', 3080: 'MLB', 81: 'SBH', 3081: 'SBH', 82: 'MCG', 3082: 'MCG',
    83: 'JBB', 3083: 'JBB', 84: 'PMS', 3084: 'PMS', 85: 'SST', 3085: 'SST',
    86: 'CLN', 3086: 'CLN', 87: 'MSG', 3087: 'MSG', 88: 'KUM', 3088: 'KUM',
    89: 'TPI', 3089: 'TPI', 90: 'BTL', 3090: 'BTL', 91: 'KUG', 3091: 'KUG',
    92: 'KLG', 3092: 'KLG', 93: 'EDU', 3093: 'EDU', 94: 'STP', 3094: 'STP',
    95: 'TIN', 3095: 'TIN', 96: 'SGK', 3096: 'SGK', 97: 'HSL', 3097: 'HSL',
    98: 'TCY', 3098: 'TCY', 102: 'PRJ', 3102: 'PRJ', 103: 'JJG', 3103: 'JJG',
    104: 'KKL', 3104: 'KKL', 105: 'KTI', 3105: 'KTI', 106: 'CKI', 3106: 'CKI',
    107: 'JLT', 3107: 'JLT', 108: 'BSI', 3108: 'BSI', 109: 'KSR', 3109: 'KSR',
    110: 'TJJ', 3110: 'TJJ', 111: 'AKH', 3111: 'AKH', 112: 'LDO', 3112: 'LDO',
    113: 'TML', 3113: 'TML', 114: 'BBA', 3114: 'BBA', 115: 'KNG', 3115: 'KNG',
    116: 'TRI', 3116: 'TRI', 117: 'KKI', 3117: 'KKI', 118: 'TMW', 3118: 'TMW',
    120: 'PIH', 3120: 'PIH', 121: 'PRA', 3121: 'PRA', 122: 'SKN', 3122: 'SKN',
    123: 'IGN', 3123: 'IGN', 124: 'S14', 3124: 'S14', 125: 'KJA', 3125: 'KJA',
    126: 'PTS', 3126: 'PTS', 127: 'TSM', 3127: 'TSM', 128: 'SGB', 3128: 'SGB',
    129: 'BSR', 3129: 'BSR', 130: 'PDG', 3130: 'PDG', 131: 'TMG', 3131: 'TMG',
    132: 'CKT', 3132: 'CKT', 133: 'PKG', 3133: 'PKG', 134: 'RPG', 3134: 'RPG',
    135: 'BSY', 3135: 'BSY', 136: 'TCS', 3136: 'TCS', 137: 'JPP', 3137: 'JPP',
    138: 'WMU', 3138: 'WMU', 139: 'JRT', 3139: 'JRT', 140: 'CPE', 3140: 'CPE',
    141: 'STL', 3141: 'STL', 142: 'KBD', 3142: 'KBD', 143: 'LDU', 3143: 'LDU',
    144: 'KHG', 3144: 'KHG', 145: 'BSD', 3145: 'BSD', 146: 'PSG', 3146: 'PSG',
    147: 'PNS', 3147: 'PNS', 148: 'PJO', 3148: 'PJO', 149: 'BFT', 3149: 'BFT',
    150: 'LMM', 3150: 'LMM', 151: 'SLY', 3151: 'SLY', 152: 'ATR', 3152: 'ATR',
    153: 'USJ', 3153: 'USJ', 154: 'BSJ', 3154: 'BSJ', 155: 'TTJ', 3155: 'TTJ',
    156: 'TMR', 3156: 'TMR', 157: 'BPJ', 3157: 'BPJ', 158: 'SPL', 3158: 'SPL',
    159: 'RLU', 3159: 'RLU', 160: 'MTH', 3160: 'MTH', 161: 'DGG', 3161: 'DGG',
    162: 'SEA', 3162: 'SEA', 163: 'JKA', 3163: 'JKA', 164: 'KBS', 3164: 'KBS',
    165: 'TKA', 3165: 'TKA', 166: 'PGG', 3166: 'PGG', 167: 'BBG', 3167: 'BBG',
    168: 'KLC', 3168: 'KLC', 169: 'CTD', 3169: 'CTD', 170: 'PJA', 3170: 'PJA',
    171: 'JMR', 3171: 'JMR', 172: 'TMJ', 3172: 'TMJ', 173: 'SCA', 3173: 'SCA',
    174: 'BBP', 3174: 'BBP', 175: 'LBG', 3175: 'LBG', 176: 'TPG', 3176: 'TPG',
    177: 'JRU', 3177: 'JRU', 178: 'MIN', 3178: 'MIN', 179: 'OUG', 3179: 'OUG',
    180: 'KBG', 3180: 'KBG', 181: 'SRO', 3181: 'SRO', 182: 'JPU', 3182: 'JPU',
    183: 'JCL', 3183: 'JCL', 184: 'JPN', 3184: 'JPN', 185: 'KCY', 3185: 'KCY',
    186: 'JTZ', 3186: 'JTZ', 188: 'PLT', 3188: 'PLT', 189: 'BNH', 3189: 'BNH',
    190: 'BTR', 3190: 'BTR', 191: 'KPT', 3191: 'KPT', 192: 'MRD', 3192: 'MRD',
    193: 'MKH', 3193: 'MKH', 194: 'SRK', 3194: 'SRK', 195: 'BWK', 3195: 'BWK',
    196: 'JHL', 3196: 'JHL', 197: 'TNM', 3197: 'TNM', 198: 'TDA', 3198: 'TDA',
    199: 'JTH', 3199: 'JTH', 201: 'PDA', 3201: 'PDA', 202: 'RWG', 3202: 'RWG',
    203: 'SJM', 3203: 'SJM', 204: 'BTW', 3204: 'BTW', 205: 'SNG', 3205: 'SNG',
    206: 'TBM', 3206: 'TBM', 207: 'BCM', 3207: 'BCM', 208: 'JSI', 3208: 'JSI',
    209: 'STW', 3209: 'STW', 210: 'TMM', 3210: 'TMM', 211: 'TPD', 3211: 'TPD',
    212: 'JMA', 3212: 'JMA', 213: 'JKB', 3213: 'JKB', 214: 'JGA', 3214: 'JGA',
    215: 'JKP', 3215: 'JKP', 216: 'SKI', 3216: 'SKI', 217: 'TMB', 3217: 'TMB',
    220: 'GHS', 3220: 'GHS', 221: 'TSK', 3221: 'TSK', 222: 'TDC', 3222: 'TDC',
    223: 'TRJ', 3223: 'TRJ', 224: 'JAH', 3224: 'JAH', 225: 'TIH', 3225: 'TIH',
    226: 'JPR', 3226: 'JPR', 227: 'KSB', 3227: 'KSB', 228: 'INN', 3228: 'INN',
    229: 'TSJ', 3229: 'TSJ', 230: 'SSH', 3230: 'SSH', 231: 'BBM', 3231: 'BBM',
    232: 'TMD', 3232: 'TMD', 233: 'BEN', 3233: 'BEN', 234: 'SRM', 3234: 'SRM',
    235: 'SBM', 3235: 'SBM', 236: 'UYB', 3236: 'UYB', 237: 'KLS', 3237: 'KLS',
    238: 'JKT', 3238: 'JKT', 239: 'KMY', 3239: 'KMY', 240: 'KAP', 3240: 'KAP',
    241: 'DJA', 3241: 'DJA', 242: 'TKK', 3242: 'TKK', 243: 'KKR', 3243: 'KKR',
    244: 'GRT', 3244: 'GRT', 245: 'BDR', 3245: 'BDR', 246: 'BGH', 3246: 'BGH',
    247: 'BPR', 3247: 'BPR', 249: 'TAI', 3249: 'TAI', 248: 'JTS', 3248: 'JTS',
    250: 'TEA', 3250: 'TEA', 251: 'KPR', 3251: 'KPR', 252: 'TMA', 3252: 'TMA',
    253: 'JTT', 3253: 'JTT', 254: 'KPH', 3254: 'KPH', 255: 'SBP', 3255: 'SBP',
    256: 'PBR', 3256: 'PBR', 257: 'RAU', 3257: 'RAU', 258: 'JTA', 3258: 'JTA',
    259: 'SAN', 3259: 'SAN', 260: 'KDN', 3260: 'KDN', 261: 'GMG', 3261: 'GMG',
    262: 'TCT', 3262: 'TCT', 263: 'BTA', 3263: 'BTA', 264: 'JBH', 3264: 'JBH',
    265: 'JAI', 3265: 'JAI', 266: 'JDK', 3266: 'JDK', 267: 'TDI', 3267: 'TDI',
    268: 'BBT', 3268: 'BBT', 269: 'MKA', 3269: 'MKA', 270: 'BPI', 3270: 'BPI',
    273: 'LHA', 3273: 'LHA', 277: 'WSU', 3277: 'WSU', 278: 'JPI', 3278: 'JPI',
    274: 'STG', 3274: 'STG', 275: 'MSL', 3275: 'MSL', 276: 'JAS', 3276: 'JAS',
    279: 'PTJ', 3279: 'PTJ', 280: 'KDA', 3280: 'KDA', 281: 'PLT', 3281: 'PLT',
    282: 'PTT', 3282: 'PTT', 283: 'PSE', 3283: 'PSE', 284: 'BSP', 3284: 'BSP',
    285: 'BMC', 3285: 'BMC', 286: 'BIH', 3286: 'BIH', 287: 'SUA', 3287: 'SUA',
    288: 'SPT', 3288: 'SPT', 289: 'TEE', 3289: 'TEE', 290: 'TDY', 3290: 'TDY',
    291: 'BSL', 3291: 'BSL', 292: 'BMJ', 3292: 'BMJ', 293: 'BSA', 3293: 'BSA',
    294: 'KKM', 3294: 'KKM', 295: 'BKR', 3295: 'BKR', 296: 'BJL', 3296: 'BJL',
    701: 'IKB', 3701: 'IKB', 702: 'IPJ', 3702: 'IPJ', 703: 'IWS', 3703: 'IWS',
    704: 'IJK', 3704: 'IJK',
}


def format_brchrvr(branch_name: str) -> str:
    """Convert branch name (BRANCHABB) to numeric branch ID string."""
    val = BRCHRVR_MAP.get(str(branch_name).strip(), None)
    return str(val) if val is not None else ''


def format_brchcd(branch_id_int: int) -> str:
    """Convert numeric branch code to branch name string (BRCHCD format)."""
    if branch_id_int in BRCHCD_MAP:
        return BRCHCD_MAP[branch_id_int]
    return ''


# ============================================================================
# FILTER LIST - 2017-2058
# ============================================================================

EXCLUDE_LIST = {
    'PUBLIC BANK BHD COLOMBO BRANCH',
    'PUBLIC BANK VIETNAM LIMITED',
    'CAMBODIAN PUBLIC BANK PLC',
    'PUBLIC BANK VIENTIANE BR',
    'PB CARD SERVICES AC 1',
    'FIN DIV-BC NORM AC',
    'IBG COLLECTION ACCOUNT',
    'PUBLIC BANK (L) LTD',
    'PUBLIC MUTUAL BERHAD',
    'PB TRUSTEE SERVICES BERHAD',
    'PUBLIC BANK (HONG KONG) LIMITED',
    'AMANAHRAYA TRUSTEES BERHAD',
    'PUBLIC BANK BHD FOR COLLECTION A/C',
    'AKAUNTAN NEGARA MALAYSIA',
    'PUBLIC BANK',
    'PUBLIC BANK BERHAD',
    'PUBLIC BANK BHD',
    'PBB',
    'PUBLIC ISLAMIC BANK',
    'PUBLIC ISLAMIC BANK BERHAD',
    'PUBLIC ISLAMIC BANK BHD',
    'PIBB',
}

# Banned originator IDs - 2022-1211 REMOVE BANK TRANSACTIONS
BANNED_ORIGINATOR_IDS = {'0000000000000006463H', '0000000000000014328V'}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_reptdate_info(dp_mnitb_path: Path):
    """Read REPTDATE from MNITB and derive week/date macros."""
    con = duckdb.connect()
    df = con.execute(f"SELECT REPTDATE FROM read_parquet('{dp_mnitb_path}') LIMIT 1").fetchdf()
    con.close()
    reptdate = df['REPTDATE'].iloc[0]
    if hasattr(reptdate, 'date'):
        reptdate = reptdate.date()
    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'
    reptyear = reptdate.strftime('%y')   # 2-digit year
    reptmon  = reptdate.strftime('%m')   # zero-padded month
    rdate    = reptdate.strftime('%Y%m%d')  # YYMMDDN8 -> YYYYMMDD
    mm       = reptdate.strftime('%m')
    yyyy     = reptdate.strftime('%Y')
    return {
        'nowk': nowk,
        'reptyear': reptyear,
        'reptmon': reptmon,
        'rdate': rdate,
        'mm': mm,
        'yyyy': yyyy,
        'reptdate': reptdate,
    }


def compress_chars(s: str, chars: str = None, modifier: str = None) -> str:
    """
    Mimic SAS COMPRESS function.
    If modifier is 'A' (keep only alphanumeric), remove non-alphanum.
    Otherwise remove specified chars from string.
    """
    if s is None:
        return ''
    s = str(s)
    if modifier and 'A' in modifier.upper():
        # Keep only alphanumeric characters
        return re.sub(r'[^A-Za-z0-9]', '', s)
    if chars:
        for c in chars:
            s = s.replace(c, '')
    else:
        s = s.replace(' ', '')
    return s


def suppress_double_alias(s: str) -> str:
    """Suppress double @@ sequences in ID fields."""
    if not s:
        return s
    while '@@' in s:
        idx = s.index('@@')
        id_part1 = s[:idx]
        id_part2 = s[idx + 2:idx + 2 + 40]
        combined = (id_part1 + ' ' + id_part2).strip()
        s = combined + s[idx + 2 + 40:]
    return s


def remove_trailing_at(s: str) -> str:
    """2019-2828 REMOVE ENDING @"""
    if s and s.endswith('@'):
        return s[:-1]
    return s


def apply_branch_id_mapping(df: pl.DataFrame, acct_suffix: str, cust_suffix: str) -> pl.DataFrame:
    """
    Apply branch-ID-based account/customer source unique ID mapping.
    Generates RMT{BRANCHID:05d}{suffix} pattern per the SAS %BRH macro expansion.
    """
    branch_ids = [
        2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
        36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
        68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 83, 85,
        86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 102, 103, 104,
        105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117,
        118, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131,
        133, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146,
        147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
        160, 161, 162, 163, 164, 165, 167, 168, 169, 170, 171, 172, 173,
        174, 175, 176, 177, 178, 179, 180, 183, 184, 185, 186, 189, 190,
        191, 192, 193, 194, 195, 196, 197, 198, 199, 201, 202, 203, 204,
        205, 206, 207, 208, 209, 210, 211, 216, 217, 220, 221, 222, 224,
        225, 226, 228, 230, 231, 232, 233, 234, 235, 237, 239, 240, 241,
        242, 243, 244, 245, 247, 248, 249, 251, 252, 254, 256, 257, 258,
        259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 273,
        274, 275, 276, 278, 280, 281, 282, 283, 284, 285, 286, 287, 288,
        289, 290, 291, 292, 293, 294, 295, 296, 701, 702, 703, 704,
        800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 811, 812, 813,
        814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826,
        827, 828, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854,
        855, 856, 857, 858, 859, 860, 861, 862, 863,
    ]
    acct_col = pl.col('ACCOUNT_SOURCE_UNIQUE_ID')
    cust_col = pl.col('CUSTOMER_SOURCE_UNIQUE_ID')
    branch_col = pl.col('BRANCH_ID').cast(pl.Utf8)

    expr_acct = acct_col
    expr_cust = cust_col
    for bid in branch_ids:
        bid_str = str(bid)
        acct_val = f'RMT{bid:05d}{acct_suffix}'
        cust_val = f'RMT{bid:05d}{cust_suffix}'
        expr_acct = pl.when(branch_col == bid_str).then(pl.lit(acct_val)).otherwise(expr_acct)
        expr_cust = pl.when(branch_col == bid_str).then(pl.lit(cust_val)).otherwise(expr_cust)

    df = df.with_columns([
        expr_acct.alias('ACCOUNT_SOURCE_UNIQUE_ID'),
        expr_cust.alias('CUSTOMER_SOURCE_UNIQUE_ID'),
    ])
    return df


def apply_pbmt_branch_id_mapping(df: pl.DataFrame) -> pl.DataFrame:
    """
    PBMT uses different suffix pattern: PBOA / PBC
    Also includes branch 32 which is excluded in some other datasets.
    """
    branch_ids = [
        2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
        36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
        68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 83, 85,
        86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 102, 103, 104,
        105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117,
        118, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131,
        133, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146,
        147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
        160, 161, 162, 163, 164, 165, 167, 168, 169, 170, 171, 172, 173,
        174, 175, 176, 177, 178, 179, 180, 183, 184, 185, 186, 189, 190,
        191, 192, 193, 194, 195, 196, 197, 198, 199, 201, 202, 203, 204,
        205, 206, 207, 208, 209, 210, 211, 216, 217, 220, 221, 222, 224,
        225, 226, 228, 230, 231, 232, 233, 234, 235, 237, 239, 240, 241,
        242, 243, 244, 245, 247, 248, 249, 251, 252, 254, 256, 257, 258,
        259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 273,
        274, 275, 276, 278, 280, 281, 282, 283, 284, 285, 286, 287, 288,
        289, 290, 291, 292, 293, 294, 295, 296, 701, 702, 703, 704,
        800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 811, 812, 813,
        814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826,
        827, 828, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854,
        855, 856, 857, 858, 859, 860, 861, 862, 863,
    ]
    acct_col = pl.col('ACCOUNT_SOURCE_UNIQUE_ID')
    cust_col = pl.col('CUSTOMER_SOURCE_UNIQUE_ID')
    branch_col = pl.col('BRANCH_ID').cast(pl.Utf8)

    expr_acct = acct_col
    expr_cust = cust_col
    for bid in branch_ids:
        bid_str = str(bid)
        acct_val = f'RMT{bid:05d}PBOA'
        cust_val = f'RMT{bid:05d}PBC'
        expr_acct = pl.when(branch_col == bid_str).then(pl.lit(acct_val)).otherwise(expr_acct)
        expr_cust = pl.when(branch_col == bid_str).then(pl.lit(cust_val)).otherwise(expr_cust)

    df = df.with_columns([
        expr_acct.alias('ACCOUNT_SOURCE_UNIQUE_ID'),
        expr_cust.alias('CUSTOMER_SOURCE_UNIQUE_ID'),
    ])
    return df


def wu_inward_branch_ids_no32() -> list:
    """WU INWARD/OUTWARD excludes branch 32 from the list."""
    branch_ids = [
        2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 33, 34, 35,
        36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
        68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 83, 85,
        86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 102, 103, 104,
        105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117,
        118, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131,
        133, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146,
        147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
        160, 161, 162, 163, 164, 165, 167, 168, 169, 170, 171, 172, 173,
        174, 175, 176, 177, 178, 179, 180, 183, 184, 185, 186, 189, 190,
        191, 192, 193, 194, 195, 196, 197, 198, 199, 201, 202, 203, 204,
        205, 206, 207, 208, 209, 210, 211, 216, 217, 220, 221, 222, 224,
        225, 226, 228, 230, 231, 232, 233, 234, 235, 237, 239, 240, 241,
        242, 243, 244, 245, 247, 248, 249, 251, 252, 254, 256, 257, 258,
        259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 273,
        274, 275, 276, 278, 280, 281, 282, 283, 284, 285, 286, 287, 288,
        289, 290, 291, 292, 293, 294, 295, 296, 701, 702, 703, 704,
        800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 811, 812, 813,
        814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826,
        827, 828, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854,
        855, 856, 857, 858, 859, 860, 861, 862, 863,
    ]
    return branch_ids


def apply_wu_branch_id_mapping(df: pl.DataFrame, acct_suffix: str, cust_suffix: str) -> pl.DataFrame:
    """Apply WU inward/outward branch mapping (no branch 32)."""
    branch_ids = wu_inward_branch_ids_no32()
    acct_col = pl.col('ACCOUNT_SOURCE_UNIQUE_ID')
    cust_col = pl.col('CUSTOMER_SOURCE_UNIQUE_ID')
    branch_col = pl.col('BRANCH_ID').cast(pl.Utf8)

    expr_acct = acct_col
    expr_cust = cust_col
    for bid in branch_ids:
        bid_str = str(bid)
        acct_val = f'RMT{bid:05d}{acct_suffix}'
        cust_val = f'RMT{bid:05d}{cust_suffix}'
        expr_acct = pl.when(branch_col == bid_str).then(pl.lit(acct_val)).otherwise(expr_acct)
        expr_cust = pl.when(branch_col == bid_str).then(pl.lit(cust_val)).otherwise(expr_cust)

    df = df.with_columns([
        expr_acct.alias('ACCOUNT_SOURCE_UNIQUE_ID'),
        expr_cust.alias('CUSTOMER_SOURCE_UNIQUE_ID'),
    ])
    return df


def str_val(val) -> str:
    """Return string value or empty string for None/null."""
    if val is None:
        return ''
    return str(val).strip()


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    # -----------------------------------------------------------------------
    # Step 1: Get report date info
    # -----------------------------------------------------------------------
    date_info = get_reptdate_info(DP_MNITB_PATH)
    nowk      = date_info['nowk']
    reptyear  = date_info['reptyear']
    reptmon   = date_info['reptmon']
    rdate     = date_info['rdate']
    mm        = date_info['mm']
    yyyy      = date_info['yyyy']

    run_timestamp = rdate + '000000'

    # -----------------------------------------------------------------------
    # Step 2: Load REMTRAN file (foreign transactions only)
    # -----------------------------------------------------------------------
    rem_filename = f"remtran{reptmon}{nowk}{reptyear}.parquet"
    rem_path = REM_BASE_PATH / rem_filename

    con = duckdb.connect()
    foreign_df = con.execute(f"""
        SELECT *
        FROM read_parquet('{rem_path}')
        WHERE REMTYPE = 'F'
    """).pl()
    con.close()

    # 2017-2058: Filter out rows where excluded names appear in any name field
    def in_exclude_list(val) -> bool:
        if val is None:
            return False
        return str(val).strip().upper() in EXCLUDE_LIST

    def row_excluded(row) -> bool:
        for field in ['APPLNAME', 'BENENAME', 'BNAD1', 'BNAD2', 'ANAD1', 'ANAD2']:
            v = row.get(field, None)
            if v and str(v).strip().upper() in EXCLUDE_LIST:
                return True
        return False

    rows = foreign_df.to_dicts()
    filtered_rows = [r for r in rows if not row_excluded(r)]
    if filtered_rows:
        foreign_df = pl.DataFrame(filtered_rows, schema=foreign_df.schema)
    else:
        foreign_df = foreign_df.clear()

    # -----------------------------------------------------------------------
    # Step 3: Common field derivations on FOREIGN
    # -----------------------------------------------------------------------
    def enrich_foreign(df: pl.DataFrame) -> pl.DataFrame:
        records = df.to_dicts()
        enriched = []
        for r in records:
            r['RUN_TIMESTAMP']    = run_timestamp
            r['BRANCH_ID']        = format_brchrvr(str_val(r.get('BRANCHABB', '')))
            r['ORIGINATOR_NAME']  = str_val(r.get('ANAD1', ''))
            r['BENEFICIARY_NAME'] = str_val(r.get('BNAD2', ''))
            r['CURCODE']          = str_val(r.get('CURRENCY', ''))
            r['CURBASE']          = 'MYR'
            r['MENTION']          = str_val(r.get('PAYMODE', ''))
            r['CHANNEL']          = '999'
            r['REMITTANCE_REF_NO'] = str_val(r.get('SERIAL', ''))
            r['EMPLOYEE_ID']      = '88888'

            issdte = r.get('ISSDTE', None)
            if issdte:
                if hasattr(issdte, 'strftime'):
                    r['ORIGINATION_DATE'] = issdte.strftime('%Y%m%d')
                else:
                    r['ORIGINATION_DATE'] = str(issdte).replace('-', '')
            else:
                r['ORIGINATION_DATE'] = ''

            lasttran = str_val(r.get('LASTTRAN', ''))
            r['POSTING_DATE'] = lasttran.replace('-', '')

            ts = str_val(r.get('TIMESTAMP', ''))
            r['YYYY'] = ts[0:4] if len(ts) >= 4 else ''
            r['MM_TS'] = ts[5:7] if len(ts) >= 7 else ''
            r['DD'] = ts[8:10] if len(ts) >= 10 else ''
            r['HOUR'] = ts[11:13] if len(ts) >= 13 else ''
            r['MIN'] = ts[14:16] if len(ts) >= 16 else ''
            r['SEC'] = ts[17:19] if len(ts) >= 19 else ''
            r['LOCAL_TIMESTAMP'] = r['YYYY'] + r['MM_TS'] + r['DD'] + r['HOUR'] + r['MIN'] + r['SEC']

            branchabb = str_val(r.get('BRANCHABB', ''))
            if branchabb in ('701', '702', 'IKB', 'IPJ'):
                r['ORG_UNIT_CODE'] = 'PIBBTRSRY'
            else:
                r['ORG_UNIT_CODE'] = 'PBBTRSRY'

            enriched.append(r)
        return pl.DataFrame(enriched, infer_schema_length=None)

    foreign_df = enrich_foreign(foreign_df)

    # -----------------------------------------------------------------------
    # Step 4: Split into transaction type datasets
    # -----------------------------------------------------------------------
    def split_foreign(df: pl.DataFrame):
        records = df.to_dicts()
        tt_inward_rows   = []
        tt_outward_rows  = []
        wu_outward_rows  = []
        wu_inward_rows   = []
        pbmt_rows        = []
        bt_rows          = []
        for r in records:
            isttype = str_val(r.get('ISTTYPE', ''))
            status  = str_val(r.get('STATUS', ''))
            if isttype == 'TF' and status == 'TO':
                tt_outward_rows.append(r)
            if isttype == 'DF' and status == 'MO':
                tt_outward_rows.append(r)
            if isttype == 'BK' and status == 'TO':
                tt_outward_rows.append(r)
            if isttype == 'TF' and status == 'TI':
                tt_inward_rows.append(r)
            if isttype == 'DF' and status == 'PP':
                tt_inward_rows.append(r)
            if isttype == 'WF' and status == 'TO':
                wu_outward_rows.append(r)
            if isttype == 'WF' and status == 'TI':
                wu_inward_rows.append(r)
            if isttype == 'BF' and status == 'MO':
                pbmt_rows.append(r)
            if isttype == 'BT' and status == 'IS':
                bt_rows.append(r)

        def to_df(rows):
            return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()

        return (to_df(tt_inward_rows), to_df(tt_outward_rows),
                to_df(wu_outward_rows), to_df(wu_inward_rows),
                to_df(pbmt_rows), to_df(bt_rows))

    tt_inward_df, tt_outward_df, wu_outward_df, wu_inward_df, pbmt_df, bt_df = split_foreign(foreign_df)

    # -----------------------------------------------------------------------
    # Step 5: Load LOAN data
    # -----------------------------------------------------------------------
    con = duckdb.connect()
    loan_pbb = con.execute(f"""
        SELECT ACCTNO, NOTENO, COSTCTR, LOANTYPE
        FROM read_parquet('{LN_LNNOTE_PATH}')
    """).pl()
    loan_iln = con.execute(f"""
        SELECT ACCTNO, NOTENO, COSTCTR, LOANTYPE
        FROM read_parquet('{ILN_LNNOTE_PATH}')
    """).pl()
    con.close()

    loan_df = pl.concat([loan_pbb, loan_iln])
    loan_df = loan_df.with_columns([
        pl.lit('LN').alias('MNI_ACCTCODE'),
        pl.col('LOANTYPE').alias('PRODUCT'),
        (pl.lit('LN') + pl.col('LOANTYPE').cast(pl.Utf8).str.zfill(3)).alias('PROD'),
    ]).drop('LOANTYPE')
    # Sort by ACCTNO, NOTENO descending, keep first (NODUPKEY by ACCTNO)
    loan_df = loan_df.sort(['ACCTNO', 'NOTENO'], descending=[False, True])
    loan_df = loan_df.unique(subset=['ACCTNO'], keep='first')

    # -----------------------------------------------------------------------
    # Step 6: Load DEPO data
    # -----------------------------------------------------------------------
    con = duckdb.connect()

    def load_parquet_keep(path, extra_cols=None):
        cols = "ACCTNO, PRODUCT, BRANCH"
        if extra_cols:
            cols += ', ' + ', '.join(extra_cols)
        try:
            return con.execute(f"SELECT {cols} FROM read_parquet('{path}')").pl()
        except Exception:
            return pl.DataFrame(schema={'ACCTNO': pl.Utf8, 'PRODUCT': pl.Utf8, 'BRANCH': pl.Utf8})

    dp_current  = load_parquet_keep(DP_CURRENT_PATH)
    idp_current = load_parquet_keep(IDP_CURRENT_PATH)
    dp_saving   = load_parquet_keep(DP_SAVING_PATH)
    idp_saving  = load_parquet_keep(IDP_SAVING_PATH)
    dp_fd       = load_parquet_keep(DP_FD_PATH)
    idp_fd      = load_parquet_keep(IDP_FD_PATH)
    dp_uma      = load_parquet_keep(DP_UMA_PATH)
    idp_uma     = load_parquet_keep(IDP_UMA_PATH)
    dp_vostro   = load_parquet_keep(DP_VOSTRO_PATH)
    con.close()

    depo_df = pl.concat([dp_current, idp_current, dp_saving, idp_saving,
                         dp_fd, idp_fd, dp_uma, idp_uma, dp_vostro],
                        how='diagonal')
    depo_df = depo_df.with_columns([
        pl.lit('DP').alias('MNI_ACCTCODE'),
        (pl.lit('DP') + pl.col('PRODUCT').cast(pl.Utf8).str.zfill(3)).alias('PROD'),
        pl.col('BRANCH').alias('ACCTBRCH'),
    ]).select(['ACCTNO', 'PROD', 'MNI_ACCTCODE', 'ACCTBRCH'])

    # -----------------------------------------------------------------------
    # Step 7: Build ACCT combined dataset
    # -----------------------------------------------------------------------
    # DEPO
    acct_depo = depo_df.with_columns([
        (pl.col('MNI_ACCTCODE') + pl.col('ACCTNO').cast(pl.Utf8)).alias('ACCOUNT_SOURCE_UNIQUE_ID'),
        pl.lit('Y').alias('BANK_ACC_IND'),
    ])
    # LOAN - add empty ACCTBRCH
    loan_acct = loan_df.with_columns([
        (pl.col('MNI_ACCTCODE') + pl.col('ACCTNO').cast(pl.Utf8)).alias('ACCOUNT_SOURCE_UNIQUE_ID'),
        pl.lit('Y').alias('BANK_ACC_IND'),
        pl.lit(None).cast(pl.Utf8).alias('ACCTBRCH'),
    ]).select(['ACCTNO', 'PROD', 'MNI_ACCTCODE', 'ACCTBRCH', 'ACCOUNT_SOURCE_UNIQUE_ID', 'BANK_ACC_IND'])

    acct_df = pl.concat([acct_depo, loan_acct], how='diagonal')
    acct_df = acct_df.unique(subset=['ACCOUNT_SOURCE_UNIQUE_ID'], keep='first')

    # -----------------------------------------------------------------------
    # Step 8: Load CIS
    # -----------------------------------------------------------------------
    con = duckdb.connect()
    cis_df = con.execute(f"""
        SELECT ACCTCODE, ACCTNO, CUSTNO, ALIAS, PRISEC
        FROM read_parquet('{CIS_CUSTDLY_PATH}')
        WHERE PRISEC = 901 AND ACCTCODE IN ('DP', 'LN')
    """).pl()
    con.close()

    cis_df = cis_df.with_columns([
        (pl.col('ACCTCODE') + pl.col('ACCTNO').cast(pl.Utf8)).alias('ACCOUNT_SOURCE_UNIQUE_ID'),
        ('CIS' + pl.col('CUSTNO').cast(pl.Utf8)).alias('CIS'),
    ])
    cis_df = cis_df.unique(subset=['ACCOUNT_SOURCE_UNIQUE_ID'], keep='first')

    # -----------------------------------------------------------------------
    # Step 9: Process TT_INWARD
    # -----------------------------------------------------------------------
    def process_tt_inward(df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        records = df.to_dicts()
        processed = []
        for r in records:
            r['INCOMING_OUTGOING_FLG'] = 'I'
            r['TXN_CODE']              = 'RMT003'
            r['BENEFICIARY_ID']        = str_val(r.get('NEWIC', ''))
            r['SENDER_BRANCH']         = '0'
            r['BENE_BRANCH']           = str_val(r.get('BRANCHABB', ''))
            r['ORIGINATOR_BANK']       = str_val(r.get('SWIFTCODE', ''))
            r['BENEFICIARY_BANK']      = str_val(r.get('BRANCHABB', ''))
            r['CRDR']                  = 'C'

            bnad1 = str_val(r.get('BNAD1', ''))
            temp_acct = compress_chars(bnad1, chars=',:.()-/ ')
            if len(temp_acct) != 10:
                temp_acct = ''
            first_digit = temp_acct[0] if temp_acct else ''
            if first_digit == '2':
                temp_acctcode = 'LN'
            else:
                temp_acctcode = 'DP'
            if temp_acct:
                r['ACCOUNT_SOURCE_UNIQUE_ID'] = temp_acctcode + temp_acct
            else:
                r['ACCOUNT_SOURCE_UNIQUE_ID'] = ''
            r['CUSTOMER_SOURCE_UNIQUE_ID'] = ''
            processed.append(r)
        return pl.DataFrame(processed, infer_schema_length=None)

    tt_inward_df = process_tt_inward(tt_inward_df)

    # Merge TT_INWARD with ACCT and CIS
    def merge_tt_inward(tt_df: pl.DataFrame) -> pl.DataFrame:
        if tt_df.is_empty():
            return tt_df
        # Join with ACCT
        merged = tt_df.join(
            acct_df.select(['ACCOUNT_SOURCE_UNIQUE_ID', 'PROD', 'BANK_ACC_IND', 'ACCTBRCH']),
            on='ACCOUNT_SOURCE_UNIQUE_ID', how='left'
        )
        # Join with CIS
        merged = merged.join(
            cis_df.select(['ACCOUNT_SOURCE_UNIQUE_ID', 'CIS', 'ALIAS']),
            on='ACCOUNT_SOURCE_UNIQUE_ID', how='left'
        )

        records = merged.to_dicts()
        out = []
        for r in records:
            bank_acc = str_val(r.get('BANK_ACC_IND', ''))
            cis_val  = str_val(r.get('CIS', ''))
            alias    = str_val(r.get('ALIAS', ''))

            if bank_acc != 'Y':
                #ORG_UNIT_CODE = 'PBB'
                r['ACCOUNT_SOURCE_UNIQUE_ID']  = 'RMT00004A'
                r['CUSTOMER_SOURCE_UNIQUE_ID'] = 'RMT00004C'
                r['PROD'] = 'RT101'

            if cis_val:
                r['CUSTOMER_SOURCE_UNIQUE_ID'] = cis_val
            else:
                r['CUSTOMER_SOURCE_UNIQUE_ID'] = 'RMT00004C'
                r['ACCOUNT_SOURCE_UNIQUE_ID']  = 'RMT00004A'
                #ORG_UNIT_CODE = 'PBB'
                r['PROD'] = 'RT101'

            out.append(r)

        result = pl.DataFrame(out, infer_schema_length=None)
        # Apply branch-based mapping when not matched
        result = _apply_tt_inward_branch_mapping(result)
        return result

    def _apply_tt_inward_branch_mapping(df: pl.DataFrame) -> pl.DataFrame:
        """Apply branch ID mapping for TT_INWARD fallback accounts (TFIA/TFC)."""
        records = df.to_dicts()
        out = []
        for r in records:
            bank_acc = str_val(r.get('BANK_ACC_IND', ''))
            cis_val  = str_val(r.get('CIS', ''))
            if bank_acc != 'Y' or not cis_val:
                bid_str = str_val(r.get('BRANCH_ID', ''))
                if bid_str:
                    try:
                        bid = int(bid_str)
                        r['ACCOUNT_SOURCE_UNIQUE_ID']  = f'RMT{bid:05d}TFIA'
                        r['CUSTOMER_SOURCE_UNIQUE_ID'] = f'RMT{bid:05d}TFC'
                    except ValueError:
                        pass
            out.append(r)
        return pl.DataFrame(out, infer_schema_length=None)

    tt_inward_final = merge_tt_inward(tt_inward_df)

    # -----------------------------------------------------------------------
    # Step 10: Process TT_OUTWARD
    # -----------------------------------------------------------------------
    def process_tt_outward(df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        records = df.to_dicts()
        processed = []
        for r in records:
            r['INCOMING_OUTGOING_FLG']     = 'O'
            r['TXN_CODE']                  = 'RMT004'
            r['ORIGINATOR_ID']             = str_val(r.get('NEWIC', ''))
            r['SENDER_BRANCH']             = str_val(r.get('BRANCHABB', ''))
            r['BENE_BRANCH']               = '0'
            r['BENEFICIARY_NAME']          = str_val(r.get('BNAD1', ''))
            r['ORIGINATOR_BANK']           = str_val(r.get('BRANCHABB', ''))
            r['BENEFICIARY_BANK']          = str_val(r.get('SWIFTCODE', ''))
            r['ACCOUNT_SOURCE_UNIQUE_ID']  = 'RMT00003A'
            r['CUSTOMER_SOURCE_UNIQUE_ID'] = 'RMT00003C'
            r['PROD']                      = 'RT102'
            r['CRDR']                      = 'D'
            #ORG_UNIT_CODE = 'PBB'
            processed.append(r)
        result = pl.DataFrame(processed, infer_schema_length=None)
        # Apply branch mapping (TFOA/TFC)
        result = apply_branch_id_mapping(result, 'TFOA', 'TFC')
        return result

    tt_outward_final = process_tt_outward(tt_outward_df)

    # -----------------------------------------------------------------------
    # Step 11: Process WU_INWARD
    # -----------------------------------------------------------------------
    def process_wu_inward_base(df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        records = df.to_dicts()
        processed = []
        for r in records:
            r['INCOMING_OUTGOING_FLG'] = 'I'
            r['TXN_CODE']              = 'RMT005'
            r['BENEFICIARY_ID']        = str_val(r.get('NEWIC', ''))
            r['SENDER_BRANCH']         = '0'
            r['BENE_BRANCH']           = str_val(r.get('BRANCHABB', ''))
            r['BENEFICIARY_BANK']      = str_val(r.get('BRANCHABB', ''))
            r['BENEFICIARY_NAME']      = str_val(r.get('BNAD1', ''))
            r['CRDR']                  = 'C'
            paymode = str_val(r.get('PAYMODE', ''))
            payref  = str_val(r.get('PAYREF', ''))
            if paymode == 'CR A/C':
                r['ACCOUNT_SOURCE_UNIQUE_ID'] = compress_chars('DP' + payref)
            else:
                r['ACCOUNT_SOURCE_UNIQUE_ID'] = ''
            r['CUSTOMER_SOURCE_UNIQUE_ID'] = ''
            processed.append(r)
        return pl.DataFrame(processed, infer_schema_length=None)

    def merge_wu_inward(df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        merged = df.join(
            acct_df.select(['ACCOUNT_SOURCE_UNIQUE_ID', 'PROD', 'BANK_ACC_IND', 'ACCTBRCH']),
            on='ACCOUNT_SOURCE_UNIQUE_ID', how='left'
        )
        merged = merged.join(
            cis_df.select(['ACCOUNT_SOURCE_UNIQUE_ID', 'CIS', 'ALIAS']),
            on='ACCOUNT_SOURCE_UNIQUE_ID', how='left'
        )
        records = merged.to_dicts()
        out = []
        for r in records:
            bank_acc = str_val(r.get('BANK_ACC_IND', ''))
            cis_val  = str_val(r.get('CIS', ''))
            paymode  = str_val(r.get('PAYMODE', ''))
            branchabb = str_val(r.get('BRANCHABB', ''))
            acctbrch = r.get('ACCTBRCH', None)

            # EBNK DEBIT KLC special handling
            if bank_acc == 'Y' and paymode == 'EBNK DEBIT' and branchabb == 'KLC' and acctbrch is not None:
                try:
                    acctbrch_int = int(acctbrch)
                    r['BRANCH_ID']   = str(acctbrch_int)
                    r['BENE_BRANCH'] = format_brchcd(acctbrch_int)
                except (ValueError, TypeError):
                    pass

            if bank_acc != 'Y':
                #ORG_UNIT_CODE = 'PBB'
                r['ACCOUNT_SOURCE_UNIQUE_ID']  = 'RMT00005A'
                r['CUSTOMER_SOURCE_UNIQUE_ID'] = 'RMT00005C'
                r['PROD'] = 'RT105'

            if cis_val:
                r['CUSTOMER_SOURCE_UNIQUE_ID'] = cis_val
            else:
                r['CUSTOMER_SOURCE_UNIQUE_ID'] = 'RMT00005C'
                r['ACCOUNT_SOURCE_UNIQUE_ID']  = 'RMT00005A'
                #ORG_UNIT_CODE = 'PBB'
                r['PROD'] = 'RT105'

            out.append(r)

        result = pl.DataFrame(out, infer_schema_length=None)
        # Apply WU branch mapping (WFIA/WFC)
        result = _apply_wu_inward_branch_mapping(result)
        return result

    def _apply_wu_inward_branch_mapping(df: pl.DataFrame) -> pl.DataFrame:
        records = df.to_dicts()
        out = []
        for r in records:
            bank_acc = str_val(r.get('BANK_ACC_IND', ''))
            cis_val  = str_val(r.get('CIS', ''))
            if bank_acc != 'Y' or not cis_val:
                bid_str = str_val(r.get('BRANCH_ID', ''))
                if bid_str:
                    try:
                        bid = int(bid_str)
                        r['ACCOUNT_SOURCE_UNIQUE_ID']  = f'RMT{bid:05d}WFIA'
                        r['CUSTOMER_SOURCE_UNIQUE_ID'] = f'RMT{bid:05d}WFC'
                    except ValueError:
                        pass
            out.append(r)
        return pl.DataFrame(out, infer_schema_length=None)

    wu_inward_df = process_wu_inward_base(wu_inward_df)
    wu_inward_final = merge_wu_inward(wu_inward_df)

    # -----------------------------------------------------------------------
    # Step 12: Process WU_OUTWARD
    # -----------------------------------------------------------------------
    def process_wu_outward_base(df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        records = df.to_dicts()
        processed = []
        for r in records:
            r['INCOMING_OUTGOING_FLG'] = 'O'
            r['TXN_CODE']              = 'RMT006'
            r['ORIGINATOR_ID']         = str_val(r.get('NEWIC', ''))
            r['SENDER_BRANCH']         = str_val(r.get('BRANCHABB', ''))
            r['BENE_BRANCH']           = '0'
            r['BENEFICIARY_NAME']      = str_val(r.get('BNAD1', ''))
            r['ORIGINATOR_BANK']       = str_val(r.get('BRANCHABB', ''))
            r['CRDR']                  = 'D'
            paymode = str_val(r.get('PAYMODE', ''))
            payref  = str_val(r.get('PAYREF', ''))
            if paymode == 'EBNK DEBIT':
                r['ACCOUNT_SOURCE_UNIQUE_ID'] = compress_chars('DP' + payref)
            else:
                r['ACCOUNT_SOURCE_UNIQUE_ID'] = ''
            r['CUSTOMER_SOURCE_UNIQUE_ID'] = ''
            processed.append(r)
        return pl.DataFrame(processed, infer_schema_length=None)

    def merge_wu_outward(df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        merged = df.join(
            acct_df.select(['ACCOUNT_SOURCE_UNIQUE_ID', 'PROD', 'BANK_ACC_IND', 'ACCTBRCH']),
            on='ACCOUNT_SOURCE_UNIQUE_ID', how='left'
        )
        merged = merged.join(
            cis_df.select(['ACCOUNT_SOURCE_UNIQUE_ID', 'CIS', 'ALIAS']),
            on='ACCOUNT_SOURCE_UNIQUE_ID', how='left'
        )
        records = merged.to_dicts()
        out = []
        for r in records:
            bank_acc  = str_val(r.get('BANK_ACC_IND', ''))
            cis_val   = str_val(r.get('CIS', ''))
            paymode   = str_val(r.get('PAYMODE', ''))
            branchabb = str_val(r.get('BRANCHABB', ''))
            acctbrch  = r.get('ACCTBRCH', None)

            if bank_acc == 'Y' and paymode == 'EBNK DEBIT' and branchabb == 'KLC' and acctbrch is not None:
                try:
                    acctbrch_int = int(acctbrch)
                    r['BRANCH_ID']     = str(acctbrch_int)
                    r['SENDER_BRANCH'] = format_brchcd(acctbrch_int)
                except (ValueError, TypeError):
                    pass

            if bank_acc != 'Y':
                #ORG_UNIT_CODE = 'PBB'
                r['ACCOUNT_SOURCE_UNIQUE_ID']  = 'RMT00006A'
                r['CUSTOMER_SOURCE_UNIQUE_ID'] = 'RMT00006C'
                r['PROD'] = 'RT106'

            if cis_val:
                r['CUSTOMER_SOURCE_UNIQUE_ID'] = cis_val
            else:
                r['CUSTOMER_SOURCE_UNIQUE_ID'] = 'RMT00006C'
                r['ACCOUNT_SOURCE_UNIQUE_ID']  = 'RMT00006A'
                #ORG_UNIT_CODE = 'PBB'
                r['PROD'] = 'RT106'

            out.append(r)

        result = pl.DataFrame(out, infer_schema_length=None)
        result = _apply_wu_outward_branch_mapping(result)
        return result

    def _apply_wu_outward_branch_mapping(df: pl.DataFrame) -> pl.DataFrame:
        records = df.to_dicts()
        out = []
        for r in records:
            bank_acc = str_val(r.get('BANK_ACC_IND', ''))
            cis_val  = str_val(r.get('CIS', ''))
            if bank_acc != 'Y' or not cis_val:
                bid_str = str_val(r.get('BRANCH_ID', ''))
                if bid_str:
                    try:
                        bid = int(bid_str)
                        r['ACCOUNT_SOURCE_UNIQUE_ID']  = f'RMT{bid:05d}WFOA'
                        r['CUSTOMER_SOURCE_UNIQUE_ID'] = f'RMT{bid:05d}WFC'
                    except ValueError:
                        pass
            out.append(r)
        return pl.DataFrame(out, infer_schema_length=None)

    wu_outward_df = process_wu_outward_base(wu_outward_df)
    wu_outward_final = merge_wu_outward(wu_outward_df)

    # -----------------------------------------------------------------------
    # Step 13: Process PBMT
    # -----------------------------------------------------------------------
    def process_pbmt(df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        records = df.to_dicts()
        processed = []
        for r in records:
            r['INCOMING_OUTGOING_FLG']     = 'O'
            r['TXN_CODE']                  = 'RMT008'
            r['ORIGINATOR_ID']             = str_val(r.get('NEWIC', ''))
            r['SENDER_BRANCH']             = str_val(r.get('BRANCHABB', ''))
            r['BENE_BRANCH']               = '0'
            r['ORIGINATOR_BANK']           = str_val(r.get('BRANCHABB', ''))
            r['BENEFICIARY_BANK']          = str_val(r.get('SWIFTCODE', ''))
            r['CRDR']                      = 'D'
            r['PROD']                      = 'RT107'
            r['CUSTOMER_SOURCE_UNIQUE_ID'] = 'RMT00007C'
            r['ACCOUNT_SOURCE_UNIQUE_ID']  = 'RMT00007A'
            #ORG_UNIT_CODE = 'PBB'
            r['BENEFICIARY_NAME'] = str_val(r.get('BNAD1', ''))
            processed.append(r)
        result = pl.DataFrame(processed, infer_schema_length=None)
        result = apply_pbmt_branch_id_mapping(result)
        return result

    pbmt_final = process_pbmt(pbmt_df)

    # -----------------------------------------------------------------------
    # Step 14: Process BT
    # -----------------------------------------------------------------------
    def process_bt(df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        records = df.to_dicts()
        processed = []
        for r in records:
            r['INCOMING_OUTGOING_FLG'] = 'O'
            branchabb = str_val(r.get('BRANCHABB', ''))
            # BRANCH_ID = COMPRESS(BRANCHABB*1) - treat numeric string, strip leading zeros
            try:
                branch_id_int = int(branchabb)
                r['BRANCH_ID'] = str(branch_id_int)
            except (ValueError, TypeError):
                r['BRANCH_ID'] = branchabb

            r['TXN_CODE']      = 'RMT004'
            r['ORIGINATOR_ID'] = str_val(r.get('NEWIC', ''))

            try:
                bid = int(r['BRANCH_ID'])
                r['SENDER_BRANCH']  = format_brchcd(bid)
                r['ORIGINATOR_BANK'] = format_brchcd(bid)
            except (ValueError, TypeError):
                r['SENDER_BRANCH']   = ''
                r['ORIGINATOR_BANK'] = ''

            r['BENE_BRANCH']               = '0'
            r['BENEBANK']                  = str_val(r.get('SWIFTCODE', ''))
            r['CRDR']                      = 'D'
            r['PROD']                      = 'RT102'  # CHECK WITH USER
            r['CUSTOMER_SOURCE_UNIQUE_ID'] = 'RMT00003C'
            r['ACCOUNT_SOURCE_UNIQUE_ID']  = 'RMT00003A'
            #ORG_UNIT_CODE = 'PBB'
            r['BENEFICIARY_NAME'] = str_val(r.get('BNAD1', ''))
            processed.append(r)

        result = pl.DataFrame(processed, infer_schema_length=None)
        result = apply_branch_id_mapping(result, 'TFOA', 'TFC')
        return result

    bt_final = process_bt(bt_df)

    # -----------------------------------------------------------------------
    # Step 15: Combine all datasets
    # -----------------------------------------------------------------------
    all_dfs = [d for d in [tt_inward_final, tt_outward_final,
                             wu_inward_final, wu_outward_final,
                             pbmt_final, bt_final] if not d.is_empty()]

    if not all_dfs:
        print("No records to process.")
        return

    out_df = pl.concat(all_dfs, how='diagonal')

    # -----------------------------------------------------------------------
    # Step 16: Final OUT data processing
    # -----------------------------------------------------------------------
    records = out_df.to_dicts()
    final_records = []
    for r in records:
        io_flg        = str_val(r.get('INCOMING_OUTGOING_FLG', ''))
        beneficiary_id = str_val(r.get('BENEFICIARY_ID', ''))
        originator_id  = str_val(r.get('ORIGINATOR_ID', ''))
        alias          = str_val(r.get('ALIAS', ''))
        originator_name  = str_val(r.get('ORIGINATOR_NAME', ''))
        beneficiary_name = str_val(r.get('BENEFICIARY_NAME', ''))

        if io_flg == 'I' and not beneficiary_id:
            if alias:
                beneficiary_id = alias

        elif io_flg == 'O' and not originator_id:
            if alias:
                originator_id = alias

        if not beneficiary_id:
            beneficiary_id = beneficiary_name
        if not originator_id:
            originator_id  = originator_name

        if not originator_id or not beneficiary_id:
            continue  # DELETE

        # SUPPRESS DOUBLE ALIAS IN ORIGINATOR_ID
        originator_id  = suppress_double_alias(originator_id)
        # SUPPRESS DOUBLE ALIAS IN BENEFICIARY_ID
        beneficiary_id = suppress_double_alias(beneficiary_id)

        # 2019-2828 REMOVE ENDING @
        originator_id  = remove_trailing_at(originator_id)
        beneficiary_id = remove_trailing_at(beneficiary_id)

        r['ORIGINATOR_ID']  = originator_id
        r['BENEFICIARY_ID'] = beneficiary_id

        # 2022-1211 REMOVE BANK TRANSACTIONS
        orig_padded = originator_id.zfill(20)
        if orig_padded in BANNED_ORIGINATOR_IDS:
            continue

        final_records.append(r)

    # -----------------------------------------------------------------------
    # Step 17: Write output file (FORRMT)
    # -----------------------------------------------------------------------
    DELIM = '\x1D'  # '1D'X - group separator / field delimiter
    OUTPUT_FORRMT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FORRMT_PATH, 'w', encoding='utf-8', newline='\n') as f:
        for count, r in enumerate(final_records, start=1):
            source_txn_unique_id = f"FRM{rdate}{count:010d}"

            def fv(key, default=''):
                return str_val(r.get(key, default))

            fields = [
                fv('RUN_TIMESTAMP'),              # 1
                source_txn_unique_id,              # 2
                source_txn_unique_id,              # 3
                fv('ACCOUNT_SOURCE_UNIQUE_ID'),    # 4
                fv('ACCOUNT_SOURCE_UNIQUE_ID'),    # 5
                fv('CUSTOMER_SOURCE_UNIQUE_ID'),   # 6
                fv('CUSTOMER_SOURCE_UNIQUE_ID'),   # 7
                fv('BRANCH_ID'),                   # 8
                fv('TXN_CODE'),                    # 9
                '',                                # 10
                fv('CURCODE'),                     # 11
                fv('CURBASE'),                     # 12
                fv('ORIGINATION_DATE'),            # 13
                fv('POSTING_DATE'),                # 14
                '',                                # 15
                '',                                # 16
                fv('LOCAL_TIMESTAMP'),             # 17
                fv('PROD'),                        # 18
                '',                                # 19
                '',                                # 20
                fv('FORAMT'),                      # 21
                fv('AMOUNT'),                      # 22
                fv('CRDR'),                        # 23
                fv('MENTION'),                     # 24
                '',                                # 25
                '',                                # 26
                '',                                # 27
                '',                                # 28
                '',                                # 29
                '',                                # 30
                '',                                # 31
                fv('CHANNEL'),                     # 32
                '',                                # 33
                '',                                # 34
                '',                                # 35
                fv('ORG_UNIT_CODE'),               # 36
                '',                                # 37
                '',                                # 38
                '',                                # 39
                '',                                # 40
                fv('EMPLOYEE_ID'),                 # 41
                '',                                # 42
                '',                                # 43
                '',                                # 44
                '',                                # 45
                '',                                # 46
                '',                                # 47
                '',                                # 48
                '',                                # 49
                '',                                # 50
                '',                                # 51
                '',                                # 52
                '',                                # 53
                '',                                # 54
                '',                                # 55
                '',                                # 56
                '',                                # 57
                '',                                # 58
                '',                                # 59
                fv('ORIGINATOR_NAME'),             # 60
                fv('BENEFICIARY_NAME'),            # 61
                fv('ORIGINATOR_BANK'),             # 62
                fv('BENEFICIARY_BANK'),            # 63
                fv('USERID'),                      # 64
                '',                                # 65
                '',                                # 66
                '',                                # 67
                '',                                # 68
                '',                                # 69
                fv('BENEFICIARY_ID'),              # 70
                fv('ORIGINATOR_ID'),               # 71
                fv('SERIAL'),                      # 72
                fv('INCOMING_OUTGOING_FLG'),       # 73
                fv('SENDER_BRANCH'),               # 74
                fv('BENE_BRANCH'),                 # 75
            ]
            line = DELIM.join(fields)
            f.write(line + '\n')

    print(f"Output written to: {OUTPUT_FORRMT_PATH}")

    # -----------------------------------------------------------------------
    # BACKUP INTERFACE FILE (COPYFILE equivalent)
    # -----------------------------------------------------------------------
    import shutil
    OUTPUT_BKP_PATH.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(OUTPUT_FORRMT_PATH, OUTPUT_BKP_PATH)
    print(f"Backup written to: {OUTPUT_BKP_PATH}")


if __name__ == '__main__':
    main()
