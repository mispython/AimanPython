#!/usr/bin/env python3
"""
Program: EIDETLRM
Purpose: Extract Remittance Foreign & Local Transaction IFS for DETICA
"""

import os
import re
import duckdb
import polars as pl
from datetime import datetime
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR = Path("/data")

# Input paths
DP_DIR          = BASE_DIR / "SAP/PBB/MNITB"
IDP_DIR         = BASE_DIR / "SAP/PIBB/MNITB"
REM_DIR         = BASE_DIR / "SAP/PBB/CRM/RMTRNSAC"
CIS_DIR         = BASE_DIR / "RBP2/B033/CIS"

# Output paths
LOCRMT_PATH     = BASE_DIR / "SAP/AML/DETICA/REMTRAN/LOCAL/TEXT/locrmt.txt"
LOCRMT_BKP_PATH = BASE_DIR / "SAP/AML/DETICA/REMTRAN/LOCAL/TEXT/locrmt_bkp.txt"

# Intermediate / working parquet paths
DP_REPTDATE_PARQUET  = DP_DIR / "reptdate.parquet"
DP_CURRENT_PARQUET   = DP_DIR / "current.parquet"
IDP_CURRENT_PARQUET  = IDP_DIR / "current.parquet"
DP_SAVING_PARQUET    = DP_DIR / "saving.parquet"
IDP_SAVING_PARQUET   = IDP_DIR / "saving.parquet"
DP_FD_PARQUET        = DP_DIR / "fd.parquet"
IDP_FD_PARQUET       = IDP_DIR / "fd.parquet"
DP_UMA_PARQUET       = DP_DIR / "uma.parquet"
IDP_UMA_PARQUET      = IDP_DIR / "uma.parquet"
DP_VOSTRO_PARQUET    = DP_DIR / "vostro.parquet"
CIS_CUSTDLY_PARQUET  = CIS_DIR / "custdly.parquet"

DELIM = '\x1d'  # '1D'X field delimiter

# ============================================================================
# BRANCH CODE LOOKUP (from PBBELF)
# ============================================================================

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


def format_brchcd(branch_code) -> str:
    """Format branch code (integer) to branch name string (BRCHCD. format)."""
    if branch_code is None:
        return ''
    try:
        bc = int(branch_code)
    except (ValueError, TypeError):
        return ''
    if bc in range(7000, 9001) or bc in range(9994, 10000) or bc == 1:
        return 'HOE'
    if bc in [3000, 3001, 3999] or bc in range(4000, 5000):
        return 'IBU'
    return BRCHCD_MAP.get(bc, '')


# ============================================================================
# BRANCH ACCOUNT/CUSTOMER ID LOOKUP TABLE
# Used in multiple places replacing the %BRH macro expansions
# ============================================================================

# All branch IDs appearing in the SAS %BRH macros
BRH_LIST = [
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
    23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76,
    77, 78, 79, 80, 81, 83, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
    97, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
    116, 117, 118, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131,
    133, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148,
    149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163,
    164, 165, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179,
    180, 183, 184, 185, 186, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198,
    199, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 216, 217, 220,
    221, 222, 224, 225, 226, 228, 230, 231, 232, 233, 234, 235, 237, 239, 240,
    241, 242, 243, 244, 245, 247, 248, 249, 251, 252, 254, 256, 257, 258, 259,
    260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 273, 274, 275, 276,
    278, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293,
    294, 295, 296, 701, 702, 703, 704, 800, 801, 802, 803, 804, 805, 806, 807,
    808, 809, 811, 812, 813, 814, 815, 816, 817, 818, 819, 820, 821, 822, 823,
    824, 825, 826, 827, 828, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853,
    854, 855, 856, 857, 858, 859, 860, 861, 862, 863,
]

# Build BRH lookup dict: branch_id -> (acct_id, cust_id)
BRH_LOOKUP: dict = {}
for _bid in BRH_LIST:
    _bid_str = str(_bid).zfill(5)
    BRH_LOOKUP[_bid] = (f'RMT{_bid_str}TLOA', f'RMT{_bid_str}TLC')


def apply_brh_mapping(branch_id, default_acct='RMT00001A', default_cust='RMT00001C'):
    """Return (account_id, customer_id) for a given branch_id using BRH lookup."""
    try:
        bid = int(branch_id)
    except (ValueError, TypeError):
        return default_acct, default_cust
    if bid in BRH_LOOKUP:
        return BRH_LOOKUP[bid]
    return default_acct, default_cust


# ============================================================================
# EXCLUDED NAMES LIST (2017-2058)
# ============================================================================

EXCLUDED_NAMES = {
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

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def remove_double_at(value: str) -> str:
    """Suppress double '@@' in ID fields, replacing with single space-joined parts."""
    while '@@' in value:
        idx = value.index('@@')
        part1 = value[:idx]
        part2 = value[idx + 2:idx + 2 + 40]
        rest = value[idx + 2 + 40:] if len(value) > idx + 2 + 40 else ''
        value = ' '.join(filter(None, [part1.rstrip(), part2.strip()])) + rest
    return value


def remove_trailing_at(value: str) -> str:
    """2019-2828: Remove ending '@' from ID fields."""
    if value and value[-1] == '@':
        value = value[:-1]
    return value


def has_null_bytes(value: str) -> bool:
    """Check if string contains null bytes (x'00')."""
    return '\x00' in value if value else False


# ============================================================================
# STEP 1: READ REPTDATE AND DERIVE DATE MACROS
# ============================================================================

def get_reptdate_vars() -> dict:
    con = duckdb.connect()
    row = con.execute(f"SELECT REPTDATE FROM read_parquet('{DP_REPTDATE_PARQUET}') LIMIT 1").fetchone()
    con.close()

    reptdate: datetime = row[0] if isinstance(row[0], datetime) else datetime.strptime(str(row[0]), '%Y-%m-%d')
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
    reptmon  = reptdate.strftime('%m')   # 2-digit month
    rdate    = reptdate.strftime('%Y%m%d')
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


# ============================================================================
# STEP 2: LOAD REMITTANCE LOCAL TRANSACTIONS
# ============================================================================

def load_local(reptmon: str, nowk: str, reptyear: str) -> pl.DataFrame:
    """Load REMTRAN{MM}{WK}{YY} parquet and filter for REMTYPE='L'."""
    remtran_file = REM_DIR / f"remtran{reptmon}{nowk}{reptyear}.parquet"
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{remtran_file}') WHERE REMTYPE = 'L'").pl()
    con.close()

    # 2017-2058: Filter out excluded names in APPLNAME, BENENAME, BNAD1, BNAD2, ANAD1, ANAD2
    def _in_excluded(s) -> bool:
        if s is None:
            return False
        return str(s).upper().strip() in EXCLUDED_NAMES

    mask = pl.Series([
        _in_excluded(row['APPLNAME']) or
        _in_excluded(row['BENENAME']) or
        _in_excluded(row['BNAD1']) or
        _in_excluded(row['BNAD2']) or
        _in_excluded(row['ANAD1']) or
        _in_excluded(row['ANAD2'])
        for row in df.iter_rows(named=True)
    ])
    df = df.filter(~mask)
    return df


# ============================================================================
# STEP 3: ENRICH LOCAL DATAFRAME
# ============================================================================

def enrich_local(df: pl.DataFrame, rdate: str) -> pl.DataFrame:
    """
    Apply all derived columns from the second DATA LOCAL step in SAS.
    This replicates field assignments, BRH macro expansions for IG/SE records, etc.
    """
    records = df.to_dicts()
    out_rows = []

    for row in records:
        row = dict(row)

        # Fixed fields
        row['INCOMING_OUTGOING_FLG'] = 'O'
        row['RUN_TIMESTAMP'] = (rdate + '000000')[:14]
        row['BRANCH_ID'] = row.get('ISSBRANCH')
        row['CURCODE']   = 'MYR'
        row['CURBASE']   = 'MYR'

        # ISSDTE derived fields
        issdte_raw = row.get('ISSDTE')
        if issdte_raw:
            try:
                if isinstance(issdte_raw, datetime):
                    issdte = issdte_raw
                else:
                    issdte = datetime.strptime(str(issdte_raw)[:10], '%Y-%m-%d')
                row['ISSDTE_DAY']       = issdte.day
                row['ORIGINATION_DATE'] = issdte.strftime('%Y%m%d')
            except Exception:
                row['ISSDTE_DAY']       = None
                row['ORIGINATION_DATE'] = ''
        else:
            row['ISSDTE_DAY']       = None
            row['ORIGINATION_DATE'] = ''

        # POSTING_DATE: LASTTRAN with '-' removed
        lasttran = str(row.get('LASTTRAN') or '').replace('-', '')
        row['POSTING_DATE'] = lasttran

        # LOCAL_TIMESTAMP from TIMESTAMP field
        ts = str(row.get('TIMESTAMP') or '')
        if len(ts) >= 19:
            yyyy_ts = ts[0:4]
            mm_ts   = ts[5:7]
            dd_ts   = ts[8:10]
            hh_ts   = ts[11:13]
            mi_ts   = ts[14:16]
            ss_ts   = ts[17:19]
            row['LOCAL_TIMESTAMP'] = (yyyy_ts + mm_ts + dd_ts + hh_ts + mi_ts + ss_ts)[:14]
        else:
            row['LOCAL_TIMESTAMP'] = ''

        row['CRDR']           = 'D'
        row['MENTION']        = row.get('PAYMODE', '')
        row['CHANNEL']        = 999
        row['EMPLOYEE_ID']    = 88888
        row['ORIGINATOR_NAME']  = str(row.get('APPLNAME') or '')[:1000]
        row['BENEFICIARY_NAME'] = str(row.get('BENENAME') or '')[:1000]
        row['ORIGINATOR_BANK']  = format_brchcd(row.get('ISSBRANCH'))
        row['BENEFICIARY_BANK'] = str(row.get('BENEBANK') or '')
        row['SENDER_BRANCH']    = format_brchcd(row.get('ISSBRANCH'))
        row['BENE_BRANCH']      = '0'
        row['ORIGINATOR_ID']    = str(row.get('APPLID') or '')
        row['BENEFICIARY_ID']   = str(row.get('BENEID') or '')
        row['PROD']             = 'RT108'
        row['TXN_CODE']         = 'RMT002'

        issbranch = row.get('ISSBRANCH')
        if issbranch in (701, 702):
            row['ORG_UNIT_CODE'] = 'PIBBTRSRY'
        else:
            row['ORG_UNIT_CODE'] = 'PBBTRSRY'

        # Default ID placeholders (up to 32 chars for SOURCE, 17 for ACCOUNT/CUSTOMER)
        row['SOURCE_TXN_UNIQUE_ID']      = ''
        row['ACCOUNT_SOURCE_UNIQUE_ID']  = ''
        row['CUSTOMER_SOURCE_UNIQUE_ID'] = ''

        isttype = str(row.get('ISTTYPE') or '').strip()
        status  = str(row.get('STATUS')  or '').strip()
        paymode = str(row.get('PAYMODE') or '').strip()
        branch_id = row.get('BRANCH_ID')
        refno   = str(row.get('REFNO')   or '').strip()
        serial  = str(row.get('SERIAL')  or '').strip()

        emit = False

        if isttype == 'IG' and status == 'SE':
            if paymode == 'DEBIT ACC':
                row['ACCOUNT_SOURCE_UNIQUE_ID'] = ('DP' + refno)[:17]
            else:
                acct, cust = apply_brh_mapping(branch_id)
                row['ACCOUNT_SOURCE_UNIQUE_ID']  = acct[:17]
                row['CUSTOMER_SOURCE_UNIQUE_ID'] = cust[:17]
            emit = True

        elif isttype in ('A', 'A1', 'B', 'C', 'G', 'H', 'K', 'L', 'M', 'Q', 'R', 'S', 'T') \
                and status in ('L', 'O', 'IS'):
            # SERIAL = COMPRESS(PUT(ISSBRANCH,BRCHCD.)||SERIAL)
            row['SERIAL'] = (format_brchcd(issbranch) + serial).replace(' ', '')
            # IF VERIFY(PAYMODE,'1234567890') = 1 → paymode is all digits
            if paymode and all(c.isdigit() for c in paymode):
                row['ACCOUNT_SOURCE_UNIQUE_ID'] = ('DP' + paymode)[:17]
            emit = True

        if emit:
            out_rows.append(row)

    return pl.DataFrame(out_rows)


# ============================================================================
# STEP 4: LOAD DEPOSIT ACCOUNTS
# ============================================================================

def load_depo_acct() -> pl.DataFrame:
    """
    Load deposit accounts from multiple parquet sources.
    Corresponds to DATA DEPO_ACCT in SAS.
    """
    con = duckdb.connect()

    sources = [
        (DP_CURRENT_PARQUET,  'PBB'),
        (IDP_CURRENT_PARQUET, 'PIBB'),
        (DP_SAVING_PARQUET,   'PBB'),
        (IDP_SAVING_PARQUET,  'PIBB'),
        (DP_FD_PARQUET,       'PBB'),
        (IDP_FD_PARQUET,      'PIBB'),
        (DP_UMA_PARQUET,      'PBB'),
        (IDP_UMA_PARQUET,     'PIBB'),
        (DP_VOSTRO_PARQUET,   'PBB'),
    ]

    frames = []
    for parquet_path, _src in sources:
        if not parquet_path.exists():
            continue
        df = con.execute(
            f"SELECT ACCTNO, PRODUCT, BRANCH FROM read_parquet('{parquet_path}')"
        ).pl()
        frames.append(df)

    con.close()

    if not frames:
        return pl.DataFrame({'ACCOUNT_SOURCE_UNIQUE_ID': [], 'PROD': [], 'ACCTBRCH': []})

    combined = pl.concat(frames)

    # ACCOUNT_SOURCE_UNIQUE_ID = COMPRESS('DP'||ACCTNO)
    # PROD = COMPRESS('DP'||PRODUCT)
    # ACCTBRCH = BRANCH
    result = combined.with_columns([
        (pl.lit('DP') + pl.col('ACCTNO').cast(pl.Utf8)).str.replace_all(r'\s+', '').alias('ACCOUNT_SOURCE_UNIQUE_ID'),
        (pl.lit('DP') + pl.col('PRODUCT').cast(pl.Utf8)).str.replace_all(r'\s+', '').alias('PROD'),
        pl.col('BRANCH').alias('ACCTBRCH'),
    ]).select(['ACCOUNT_SOURCE_UNIQUE_ID', 'PROD', 'ACCTBRCH'])

    # Deduplicate by ACCOUNT_SOURCE_UNIQUE_ID (keep first occurrence, as in PROC SORT + DATA MERGE)
    result = result.unique(subset=['ACCOUNT_SOURCE_UNIQUE_ID'], keep='first')
    return result


# ============================================================================
# STEP 5: MERGE LOCAL WITH DEPO_ACCT → LOCAL_GETMNI
# ============================================================================

def merge_local_depo(local_df: pl.DataFrame, depo_df: pl.DataFrame) -> pl.DataFrame:
    """
    Left merge local with depo_acct on ACCOUNT_SOURCE_UNIQUE_ID.
    Applies branch logic when match found or not found.
    Corresponds to DATA TRAN.LOCAL_GETMNI (first pass).
    """
    merged = local_df.join(depo_df, on='ACCOUNT_SOURCE_UNIQUE_ID', how='left', suffix='_DEPO')

    rows = merged.to_dicts()
    out_rows = []

    for row in rows:
        acctbrch = row.get('ACCTBRCH')
        has_match = acctbrch is not None

        if has_match:
            isttype = str(row.get('ISTTYPE') or '').strip()
            issbranch = row.get('ISSBRANCH')
            userid    = str(row.get('USERID')  or '').strip()
            branch_id = row.get('BRANCH_ID')
            paymode   = str(row.get('PAYMODE') or '').strip()

            # IF ISTTYPE = 'IB' OR (special IG/CMSECP conditions) THEN update branch
            if isttype == 'IB' or \
               (((isttype == 'IG' and issbranch == 168) or
                 (userid == 'CMSECP' and branch_id == 0)) and
                paymode == 'DEBIT ACC'):
                row['BRANCH_ID']      = acctbrch
                row['SENDER_BRANCH']  = format_brchcd(acctbrch)
                # Update PROD from depo match
                if row.get('PROD_DEPO'):
                    row['PROD'] = row['PROD_DEPO']
        else:
            # A AND NOT B: fallback to BRH lookup
            branch_id = row.get('BRANCH_ID')
            acct, cust = apply_brh_mapping(branch_id)
            row['ACCOUNT_SOURCE_UNIQUE_ID']  = acct[:17]
            row['CUSTOMER_SOURCE_UNIQUE_ID'] = cust[:17]
            row['PROD'] = 'RT108'

        out_rows.append(row)

    df = pl.DataFrame(out_rows)
    # Drop DEPO join columns if present
    for col in ['ACCTBRCH', 'PROD_DEPO']:
        if col in df.columns:
            df = df.drop(col)
    return df


# ============================================================================
# STEP 6: LOAD CIS AND MERGE → LOCAL_GETMNI (second pass)
# ============================================================================

def load_cis() -> pl.DataFrame:
    """
    Load CIS CUSTDLY parquet, filter PRISEC=901 and ACCTCODE='DP'.
    Corresponds to DATA CIS in SAS.
    """
    con = duckdb.connect()
    df = con.execute(
        f"""SELECT ACCTCODE, ACCTNO, CUSTNO, ALIAS, PRISEC, INDORG
            FROM read_parquet('{CIS_CUSTDLY_PARQUET}')
            WHERE PRISEC = 901 AND ACCTCODE = 'DP'"""
    ).pl()
    con.close()

    df = df.with_columns([
        (pl.col('ACCTCODE').cast(pl.Utf8) + pl.col('ACCTNO').cast(pl.Utf8))
            .str.replace_all(r'\s+', '').alias('ACCOUNT_SOURCE_UNIQUE_ID'),
        ('CIS' + pl.col('CUSTNO').cast(pl.Utf8)).str.replace_all(r'\s+', '').alias('CIS'),
    ])

    # PROC SORT NODUPKEY BY ACCOUNT_SOURCE_UNIQUE_ID
    df = df.unique(subset=['ACCOUNT_SOURCE_UNIQUE_ID'], keep='first')
    return df.select(['ACCOUNT_SOURCE_UNIQUE_ID', 'CIS', 'ALIAS', 'INDORG'])


def merge_cis(local_getmni: pl.DataFrame, cis_df: pl.DataFrame) -> pl.DataFrame:
    """
    Merge LOCAL_GETMNI with CIS on ACCOUNT_SOURCE_UNIQUE_ID.
    Corresponds to final DATA TRAN.LOCAL_GETMNI merge with CIS in SAS.
    """
    merged = local_getmni.join(
        cis_df.select(['ACCOUNT_SOURCE_UNIQUE_ID', 'CIS', 'ALIAS', 'INDORG']),
        on='ACCOUNT_SOURCE_UNIQUE_ID',
        how='left',
        suffix='_CIS',
    )

    rows = merged.to_dicts()
    out_rows = []

    for row in rows:
        cis_val   = str(row.get('CIS') or '').strip()
        alias_cis = str(row.get('ALIAS_CIS') or row.get('ALIAS') or '').strip()
        indorg    = str(row.get('INDORG') or '').strip()

        if cis_val:
            row['CUSTOMER_SOURCE_UNIQUE_ID'] = cis_val[:17]
        else:
            # fallback BRH
            branch_id = row.get('BRANCH_ID')
            acct, cust = apply_brh_mapping(branch_id)
            row['CUSTOMER_SOURCE_UNIQUE_ID'] = cust[:17]
            row['ACCOUNT_SOURCE_UNIQUE_ID']  = acct[:17]
            # *ORG_UNIT_CODE = 'PBB';  (commented out in SAS)
            row['PROD'] = 'RT108'

        # Carry alias/indorg for use in OUT step
        row['ALIAS']  = alias_cis
        row['INDORG'] = indorg

        out_rows.append(row)

    df = pl.DataFrame(out_rows)
    # Drop duplicate CIS join columns
    for col in ['CIS', 'ALIAS_CIS', 'INDORG_CIS']:
        if col in df.columns:
            df = df.drop(col)
    return df


# ============================================================================
# STEP 7: BUILD OUT DATASET
# ============================================================================

def build_out(df: pl.DataFrame) -> pl.DataFrame:
    """
    Applies all transformations from DATA OUT in SAS:
    - Fill BENEFICIARY_ID / ORIGINATOR_ID from ALIAS
    - Null byte checks
    - Fallback to NAME if ID empty/UNKNOWN
    - Remove @@ double aliases
    - Remove trailing @
    - SMR 2021-2221: corporate alias pass-through
    - 2022-1211: remove bank transactions
    - Delete if either ID is blank
    """
    rows = df.to_dicts()
    out_rows = []

    # Bank transactions to exclude (2022-1211)
    BANK_EXCL = {'0000000000000006463H', '0000000000000014328V'}

    for row in rows:
        iof     = str(row.get('INCOMING_OUTGOING_FLG') or '').strip()
        alias   = str(row.get('ALIAS')  or '').strip()
        indorg  = str(row.get('INDORG') or '').strip()
        paymode = str(row.get('PAYMODE') or '').strip()
        ben_id  = str(row.get('BENEFICIARY_ID')  or '').strip()
        ori_id  = str(row.get('ORIGINATOR_ID')   or '').strip()
        ben_nm  = str(row.get('BENEFICIARY_NAME') or '').strip()
        ori_nm  = str(row.get('ORIGINATOR_NAME')  or '').strip()

        # Alias fill from CIS join
        if iof == 'I' and not ben_id:
            if alias:
                ben_id = alias
        elif iof == 'O' and not ori_id:
            if alias:
                ori_id = alias

        # Null byte suppression
        if has_null_bytes(ben_id):
            ben_id = ''
        if has_null_bytes(ori_id):
            ori_id = ''

        # Fallback to name
        if ben_id in ('', 'UNKNOWN'):
            ben_id = ben_nm
        if not ori_id:
            ori_id = ori_nm

        # Delete if either ID is blank
        if not ori_id or not ben_id:
            continue

        # Suppress double @@ in ORIGINATOR_ID
        ori_id = remove_double_at(ori_id)
        # Suppress double @@ in BENEFICIARY_ID
        ben_id = remove_double_at(ben_id)

        # 2019-2828: Remove ending @
        ori_id = remove_trailing_at(ori_id)
        ben_id = remove_trailing_at(ben_id)

        # SMR 2021-2221: FOR CORPORATE PASS BR/CI INTO ORIGINATOR_ID
        if indorg == 'O' and paymode == 'DEBIT ACC' and alias:
            ori_id = alias

        row['ORIGINATOR_ID']  = ori_id
        row['BENEFICIARY_ID'] = ben_id

        # 2022-1211: REMOVE BANK TRANSACTIONS
        ori_padded = ori_id.zfill(20)
        if ori_padded in BANK_EXCL:
            continue

        out_rows.append(row)

    return pl.DataFrame(out_rows)


# ============================================================================
# STEP 8: WRITE OUTPUT FILE (LOCRMT)
# ============================================================================

def write_locrmt(df: pl.DataFrame, rdate: str, output_path: Path) -> None:
    """
    Write the pipe-delimited (0x1D) output file.
    Corresponds to DATA _NULL_ / FILE LOCRMT / PUT ... in SAS.
    Fields: 75 fields delimited by 0x1D per row.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = df.to_dicts()
    count = 1

    with open(output_path, 'w', encoding='utf-8', newline='\n') as f:
        for row in rows:
            source_txn = f"LRM{rdate}{str(count).zfill(10)}"

            run_ts    = str(row.get('RUN_TIMESTAMP')            or '').strip()
            acct_id   = str(row.get('ACCOUNT_SOURCE_UNIQUE_ID') or '').strip()
            cust_id   = str(row.get('CUSTOMER_SOURCE_UNIQUE_ID')or '').strip()
            branch_id = str(row.get('BRANCH_ID') or '').strip()
            txn_code  = str(row.get('TXN_CODE')  or '').strip()
            curcode   = str(row.get('CURCODE')   or '').strip()
            curbase   = str(row.get('CURBASE')   or '').strip()
            orig_date = str(row.get('ORIGINATION_DATE') or '').strip()
            post_date = str(row.get('POSTING_DATE')     or '').strip()
            local_ts  = str(row.get('LOCAL_TIMESTAMP')  or '').strip()
            prod      = str(row.get('PROD')      or '').strip()
            amount_v  = str(row.get('AMOUNT')    or '').strip()
            crdr      = str(row.get('CRDR')      or '').strip()
            mention   = str(row.get('MENTION')   or '').strip()
            channel   = str(row.get('CHANNEL')   or '').strip()
            org_unit  = str(row.get('ORG_UNIT_CODE')    or '').strip()
            emp_id    = str(row.get('EMPLOYEE_ID')      or '').strip()
            ori_name  = str(row.get('ORIGINATOR_NAME')  or '').strip()
            ben_name  = str(row.get('BENEFICIARY_NAME') or '').strip()
            ori_bank  = str(row.get('ORIGINATOR_BANK')  or '').strip()
            ben_bank  = str(row.get('BENEFICIARY_BANK') or '').strip()
            userid    = str(row.get('USERID')    or '').strip()
            ben_id    = str(row.get('BENEFICIARY_ID')   or '').strip()
            ori_id    = str(row.get('ORIGINATOR_ID')    or '').strip()
            serial    = str(row.get('SERIAL')    or '').strip()
            iof_flag  = str(row.get('INCOMING_OUTGOING_FLG') or '').strip()
            sender_br = str(row.get('SENDER_BRANCH') or '').strip()
            bene_br   = str(row.get('BENE_BRANCH')   or '').strip()

            fields = [
                run_ts,          # 1.  RUN_TIMESTAMP
                source_txn,      # 2.  SOURCE_TXN_UNIQUE_ID
                source_txn,      # 3.  SOURCE_TXN_UNIQUE_ID (dup)
                acct_id,         # 4.  ACCOUNT_SOURCE_UNIQUE_ID
                acct_id,         # 5.  ACCOUNT_SOURCE_UNIQUE_ID (dup)
                cust_id,         # 6.  CUSTOMER_SOURCE_UNIQUE_ID
                cust_id,         # 7.  CUSTOMER_SOURCE_UNIQUE_ID (dup)
                branch_id,       # 8.  BRANCH_ID
                txn_code,        # 9.  TXN_CODE
                '',              # 10. (blank)
                curcode,         # 11. CURCODE
                curbase,         # 12. CURBASE
                orig_date,       # 13. ORIGINATION_DATE
                post_date,       # 14. POSTING_DATE
                '',              # 15. (blank)
                '',              # 16. (blank)
                local_ts,        # 17. LOCAL_TIMESTAMP
                prod,            # 18. PROD
                '',              # 19. (blank)
                '',              # 20. (blank)
                amount_v,        # 21. AMOUNT
                amount_v,        # 22. AMOUNT (dup)
                crdr,            # 23. CRDR
                mention,         # 24. MENTION
                '',              # 25. (blank)
                '',              # 26. (blank)
                '',              # 27. (blank)
                '',              # 28. (blank)
                '',              # 29. (blank)
                '',              # 30. (blank)
                '',              # 31. (blank)
                channel,         # 32. CHANNEL
                '',              # 33. (blank)
                '',              # 34. (blank)
                '',              # 35. (blank)
                org_unit,        # 36. ORG_UNIT_CODE
                '',              # 37. (blank)
                '',              # 38. (blank)
                '',              # 39. (blank)
                '',              # 40. (blank)
                emp_id,          # 41. EMPLOYEE_ID
                '',              # 42. (blank)
                '',              # 43. (blank)
                '',              # 44. (blank)
                '',              # 45. (blank)
                '',              # 46. (blank)
                '',              # 47. (blank)
                '',              # 48. (blank)
                '',              # 49. (blank)
                '',              # 50. (blank)
                '',              # 51. (blank)
                '',              # 52. (blank)
                '',              # 53. (blank)
                '',              # 54. (blank)
                '',              # 55. (blank)
                '',              # 56. (blank)
                '',              # 57. (blank)
                '',              # 58. (blank)
                '',              # 59. (blank)
                ori_name,        # 60. ORIGINATOR_NAME
                ben_name,        # 61. BENEFICIARY_NAME
                ori_bank,        # 62. ORIGINATOR_BANK
                ben_bank,        # 63. BENEFICIARY_BANK
                userid,          # 64. USERID
                '',              # 65. (blank)
                '',              # 66. (blank)
                '',              # 67. (blank)
                '',              # 68. (blank)
                '',              # 69. (blank)
                ben_id,          # 70. BENEFICIARY_ID
                ori_id,          # 71. ORIGINATOR_ID
                serial,          # 72. SERIAL
                iof_flag,        # 73. INCOMING_OUTGOING_FLG
                sender_br,       # 74. SENDER_BRANCH
                bene_br,         # 75. BENE_BRANCH
            ]

            line = DELIM.join(fields)
            f.write(line + '\n')
            count += 1


# ============================================================================
# STEP 9: BACKUP OUTPUT FILE
# ============================================================================

def backup_locrmt(src: Path, dst: Path) -> None:
    """
    Backup the LOCRMT output file.
    Corresponds to //COPYFILE EXEC PGM=ICEGENER in JCL.
    """
    import shutil
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(src), str(dst))


# ============================================================================
# MAIN
# ============================================================================

def main():
    # Step 1: Get report date variables
    date_vars = get_reptdate_vars()
    rdate     = date_vars['rdate']
    reptmon   = date_vars['reptmon']
    nowk      = date_vars['nowk']
    reptyear  = date_vars['reptyear']

    # Step 2: Load local remittance transactions
    local_df = load_local(reptmon, nowk, reptyear)

    # Step 3: Enrich local transactions
    local_df = enrich_local(local_df, rdate)

    # Sort by ACCOUNT_SOURCE_UNIQUE_ID (as in PROC SORT DATA=LOCAL)
    local_df = local_df.sort('ACCOUNT_SOURCE_UNIQUE_ID')

    # Step 4: Load deposit accounts
    depo_df = load_depo_acct()

    # Step 5: Merge local with deposit accounts
    local_getmni = merge_local_depo(local_df, depo_df)

    # Sort by ACCOUNT_SOURCE_UNIQUE_ID (as in PROC SORT DATA=TRAN.LOCAL_GETMNI)
    local_getmni = local_getmni.sort('ACCOUNT_SOURCE_UNIQUE_ID')

    # Step 6: Load CIS and merge
    cis_df = load_cis()
    local_getmni = merge_cis(local_getmni, cis_df)

    # Step 7: Build final output dataset
    out_df = build_out(local_getmni)

    # Step 8: Write output file
    write_locrmt(out_df, rdate, LOCRMT_PATH)

    # Step 9: Backup output file
    # --> BACKUP INTERFACE FILE (ICEGENER equivalent)
    backup_locrmt(LOCRMT_PATH, LOCRMT_BKP_PATH)

    print(f"EIDETLRM completed. Output: {LOCRMT_PATH}")
    print(f"Backup: {LOCRMT_BKP_PATH}")


if __name__ == '__main__':
    main()
