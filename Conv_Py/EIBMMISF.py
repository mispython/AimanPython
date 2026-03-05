#!/usr/bin/env python3
"""
Program  : EIBMMISF.py
Purpose  : Format definitions (PROC FORMAT equivalent) for EIBM reporting suite.
           Contains lookup maps and classification functions used by dependent programs.
"""

# ============================================================================
# FORMAT: $LNFACIL  (Character - Loan Facility Description by PRODCD)
# HMK2
# ============================================================================

LNFACIL: dict[str, str] = {
    "34230": "STAFF LOAN",
    "34120": "HOUSING LOAN",
    "54120": "HOUSING LOAN",
    "34110": "FIXED LOAN",
    "34114": "FIXED LOAN",
    "34115": "FIXED LOAN",
    "34116": "FIXED LOAN",
    "34117": "FIXED LOAN",
    "34149": "FIXED LOAN",
    "34190": "REVOLVING CREDIT",
}
LNFACIL_OTHER = "OTHERS"


def fmt_lnfacil(prodcd: str) -> str:
    """$LNFACIL format: map PRODCD string to loan facility description."""
    return LNFACIL.get(str(prodcd).strip(), LNFACIL_OTHER)


# ============================================================================
# FORMAT: LNDESC  (Numeric - Loan Description by PRODUCT code)
# CNU
# ============================================================================

def fmt_lndesc(product: int) -> str:
    """LNDESC format: map numeric PRODUCT code to loan description."""
    p = int(product)
    if 4 <= p <= 7:
        return "STAFF LOAN - HOUSING LOAN"
    if p == 10:
        return "STAFF LOAN - FESTIVE ADVANCES"
    if p == 15:
        return "STAFF LOAN - CAR LOAN"
    if p == 20:
        return "STAFF LOAN - MOTORCYCLE LOAN"
    if p == 25:
        return "STAFF LOAN - PURCHASE OF COMPUTER"
    if p == 26:
        return "STAFF LOAN - MEDICAL EXPENSES"
    if p == 27:
        return "STAFF LOAN - FUNERAL EXPENSES"
    if p == 28:
        return "STAFF LOAN - DISASTER RELIEF"
    if p in (29, 30):
        return "STAFF LOAN - PURSUIT OF FURTHER STUDIES"
    if p in (31, 32):
        return "STAFF LOAN - RENOVATION LOAN"
    if p == 33:
        return "STAFF LOAN - NUBE ECOPARK"
    if p == 70:
        return "PBS STAFF - HOUSING LOAN"
    if p == 71:
        return "PBS STAFF - CAR LOAN"
    if p == 72:
        return "PBS STAFF - MOTOCYCLE LOAN"
    if p == 73:
        return "PBS SLS -  PURCHASE OF COMPUTER"
    if p == 74:
        return "PBS SLS -  MEDICAL EXPENSES"
    if p == 75:
        return "PBS SLS -  FUNERAL EXPENSES"
    if p == 76:
        return "PBS SLS -  DISASTER RELIEF"
    if p == 77:
        return "PBS SLS -  STUDY LOAN"
    if p == 78:
        return "PBS SLS -  RENOVATION"
    if p in (100, 101):
        return "ABBA HOUSING LOAN"
    if p == 102:
        return "ABBA NORMAL HOUSING LOAN - PROGRESIVE"
    if p == 103:
        return "ABBA NORMAL HOUSING LOAN - FULL"
    if p == 110:
        return "ABBA PRIOR HOUSING FINANCE"
    if p == 111:
        return "ABBA NORMAL HOUSING FINANCE"
    if p == 112:
        return "HOME 4 ABBA < RM100K"
    if p == 113:
        return "HOME 4 ABBA > RM100K"
    if p == 114:
        return "HOME 3 ABBA GR < RM100K"
    if p == 115:
        return "HOME 3 ABBA GR > RM100K"
    if p == 116:
        return "ABBA MORTGAGE REFINANCING SCHEME"
    if p == 120:
        return "ABBA TERM/FIXED FINANCING"
    if p == 127:
        return "ABBA SWIFT"
    if p == 170:
        return "3F SFTF - BNM"
    if p in (200, 201):
        return "PRIORITY SECTOR HOUSING LOAN"
    if p in (204, 205):
        return "NORMAL HOUSING LOAN"
    if p in (209, 210):
        return "HOS SPECIAL (<25K(WM) & 32K(EM))"
    if p in (211, 212):
        return "HOS SPECIAL (25K-100K)"
    if p in (214, 215):
        return "HOS EXTRA (RM100,001-RM200,000)"
    if p in (219, 220):
        return "HOME REFINANCING SCHEME"
    if p in (225, 226):
        return "NORMAL HOUSING LOAN SOLD TO CAGAMAS"
    if 227 <= p <= 233:
        return "HOME OWNERSHIP SCHEME"
    if p == 234:
        return "HOME REFINANCING"
    if p in (300, 301):
        return "FIXED LOAN"
    if p in (304, 305):
        return "FLASH PLAN"
    if p in (309, 310):
        return "BRIDGING LOAN"
    if p == 315:
        return "VISIT MALAYSIA YEAR"
    if p == 320:
        return "UNSECURED LOAN"
    if p == 325:
        return "PROFESSIONAL ADVANTAGE SCHEME"
    if p == 330:
        return "QUICK CASH LOAN"
    if p == 335:
        return "UNIT TRUST PURCHASE PLAN"
    if p == 340:
        return "GLENMARIE GOLF & COUNTRY CLUB MEMBERSHIP FINANCING"
    if p == 345:
        return "CONTRACT FINANCING"
    if p == 350:
        return "REVOLVING CREDIT"
    if p == 355:
        return "PB EXECUTIVE LOAN SCHEME"
    if p == 356:
        return "HOME FURNISHING PACKAGE"
    if p == 358:
        return "UNIFLEX PLAN"
    if p == 359:
        return "SWIFT PLAN-CON"
    if p == 500:
        return "LOAN FUND FOR HAWKER PETTY TRADER 1994 (LFHPT)"
    if p in (504, 505):
        return "FIXED LOAN UNDER NPGS"
    if p in (509, 510):
        return "FIXED LOAN UNDER NPGS-BLK GUARANTEE SCHEME"
    if p == 515:
        return "FIXED LOAN UNDER PRINCIPAL GUARANTEE SCHEME"
    if p == 516:
        return "CGC ASSOCIATED SLS 1990 SCHEME"
    if p == 517:
        return "ASSOCIATION SPECIAL LOAN SCHEME"
    if p == 518:
        return "SPECIAL LOAN SCHEME"
    if p == 519:
        return "SLS NON TAX REBATE"
    if p == 520:
        return "LOAN FUND FOR HAWKER PETTY TRADER 1990 (LFHPT)"
    if p == 521:
        return "CGC TUK1"
    if p == 522:
        return "CGC TUK2"
    if p == 523:
        return "CGC TUK3"
    if p == 524:
        return "CGC/FLEXI GUARANTEE SCHEME - FSMI"
    if p == 525:
        return "CGC/FLEXI GUARANTEE SCHEME - RFSMI"
    if p == 528:
        return "CGC TUK PACKAGE 4"
    if p in (555, 556):
        return "SPECIAL FUND FOR TOURISM"
    if p in (559, 560):
        return "NEW ENTREPRENEUR FUND"
    if p == 561:
        return "BNM/FLMH"
    if p in (564, 565):
        return "FUND FOR FOOD"
    if p == 566:
        return "BNM REHAB FUND SMI"
    if p == 570:
        return "BNM/FSM1"
    if p in (900, 901):
        return "CBL - FIXED LOAN"
    if p in (904, 905):
        return "CBL - BRIDGING LOAN"
    if p == 910:
        return "CBL - REVOLVING CREDIT"
    if p in (914, 915):
        return "CBL - SYNDICATED FIXED LOAN"
    if p in (919, 920):
        return "CBL - SYNDICATED BRIDGING LOAN"
    if p == 925:
        return "CBL - SYNDICATED REVOLVING CREDIT"
    return ""


# ============================================================================
# FORMAT: ODDESC  (Numeric - Overdraft / Current Account Description)
# CNU
# ============================================================================

ODDESC: dict[int, str] = {
    100: "NORMAL CURRENT ACCOUNT ",
    101: "GOVERNMENT ACCOUNT",
    102: "EXTERNAL ACCOUNT      ",
    103: "HOUSING DEVELOPER    ",
    104: "VOSTRO LOCAL         ",
    105: "VOSTRO FOREIGN       ",
    106: "STAFF CURRENT ACCOUNT ",
    107: "PBCS                 ",
    108: "CGC                  ",
    109: "SLS                  ",
    110: "BLOCK DISCOUNT       ",
    111: "R/C                   ",
    112: "PGS                  ",
    113: "SHARE MARGIN          ",
    114: "NEF                  ",
    115: "PAS                   ",
    116: "HRS                   ",
    117: "AUSTRALIAN            ",
    118: "CAIS                  ",
    119: "HOS (<25K - 100K)     ",
    120: "HOS (>100K - 200K)    ",
    121: "HICOM SHARE           ",
    122: "SFFS                  ",
    123: "PETRONAS             ",
    124: "OCP                   ",
    125: "OD/OC                 ",
    126: "NORMAL BANKERS CHEQUE ",
    127: "MIDF BANKERS CHEQUE    ",
    128: "MIH BANKERS CHEQUE    ",
    129: "DD NORMAL             ",
    130: "USD C/A               ",
    131: "GBP C/A               ",
    132: "AUD C/A                ",
    133: "SGD C/A               ",
    134: "JPY C/A               ",
    135: "DEM C/A                ",
    136: "HKD C/A               ",
    140: "SI-CLEARING ACCOUNT    ",
    141: "ATM-PISA CLEARING ACCT ",
    142: "ATM-LOAN CLEARING ACCT ",
    143: "ATM-FD CLEARING ACCT   ",
    144: "DD SHARE PUBLIC       ",
    145: "DD SHARE BUMI         ",
    150: "ACE NORMAL",
    151: "ACE STAFF",
    152: "ACE EXTERNAL",
    154: "MORE PLAN",
    155: "5 HOME PLAN",
    160: "AL-WADIAH CURRENT A/C ",
    161: "AL-WADIAH GOVERNMENT ",
    162: "AL-WADIAH EXTERNAL A/C",
    163: "AL-WADIAH HOUSING DEV",
    164: "AL-WADIAH STAFF A/C ",
    170: "OD LIFE PLAN - NORMAL",
    180: "STAFF RELATED-NORMAL CA",
    181: "STAFF REL - ACE A/C",
    182: "STAFF REL - AL WADIAH",
}


def fmt_oddesc(product: int) -> str:
    """ODDESC format: map numeric product code to OD/CA description."""
    return ODDESC.get(int(product), "")


# ============================================================================
# FORMAT: $COLLDES  (Character - Collateral Description)
# CNU
# ============================================================================

COLLDES: dict[str, str] = {
    "1":  "(1,01) GOVERNMENT & OTHER TRUSTEE",
    "01": "(1,01) GOVERNMENT & OTHER TRUSTEE",
    "2":  "(2,02) MERCHANDISE PLEDGE",
    "02": "(2,02) MERCHANDISE PLEDGE",
    "3":  "(3,03) HYPO TO BANK",
    "03": "(3,03) HYPO TO BANK",
    "5":  "(5,05) CAR",
    "05": "(5,05) CAR",
    "6":  "(6,06) MOTORCYCLES",
    "06": "(6,06) MOTORCYCLES",
    "7":  "(7,07) CASH & FIXED DEPOSIT RECEIPTS OTHERS",
    "07": "(7,07) CASH & FIXED DEPOSIT RECEIPTS OTHERS",
    "8":  "(8,08) PLEDGE OF QUOTED SHARES AS PRIMARY COLLATERAL",
    "08": "(8,08) PLEDGE OF QUOTED SHARES AS PRIMARY COLLATERAL",
    "9":  "(9,09) OUTPORT CHEQUES",
    "09": "(9,09) OUTPORT CHEQUES",
    "10": "(10) HIRE PURCHASE FINANCE",
    "11": "(11) PACKING CREDIT (L/C) ",
    "12": "(12) PROMISSORY NOTE",
    "13": "(13) OTHER SECURITIES",
    "14": "(14) FIXED DEPOSITS (MARGIN LESS THAN 90%)",
    "15": "(15) KLMF UNIT TRUST",
    "20": "(20) UNSECURED-NEGATIVE PLEDGE LETTER OF COMFORT",
    "21": "(21) GUARANTEE - OTHERS",
    "22": "(22) UNSECURED - CLEAN ",
    "23": "(23) UNSECURED - TEMPORARY",
    "41": "(41) DEBENTURE - INSURANCE WITH LONPAC",
    "42": "(42) DEBENTURE - INSURANCE WITH OTHER INS. CO.",
    "43": "(43) DEBENTURE - INSURANCE PENDING",
    "50": "(50) PROPERTY INSURED WITH LONPAC - DIRECT",
    "51": "(51) PROPERTY INSURED WITH LONPAC - THIRD",
    "52": "(52) PROPERTY INSURED WITH OTHER INS. CO.- DIRECT",
    "53": "(53) PROPERTY INSURED WITH OTHER INS. CO.- THIRD",
    "54": "(54) PROPERTIES - MIXED INS. CO. - DIRECT",
    "55": "(55) PROPERTIES - MIXED INS. CO. - THIRD",
    "56": "(56) INSURANCE PENDING(PROPERTY/LC)-DIRECT",
    "57": "(57) INSURANCE PENDING(PROPERTY/LC)-THIRD",
    "60": "(60) OTHER PROPERTIES(INS. NOT REQ)-DIRECT",
    "61": "(61) OTHER PROPERTIES(INS. NOT REQ)-THIRD",
}
COLLDES_OTHER = "OTHERS"


def fmt_colldes(collcd: str) -> str:
    """$COLLDES format: map collateral code string to description."""
    return COLLDES.get(str(collcd).strip(), COLLDES_OTHER)


# ============================================================================
# FORMAT: LOANRNGE  (Numeric - Loan Amount Range Band Letter)
# ============================================================================

def fmt_loanrnge(amount: float) -> str:
    """LOANRNGE format: classify loan amount into range band A-L."""
    v = float(amount)
    if v <= 25_000:
        return "A"
    if v <= 50_000:
        return "B"
    if v <= 100_000:
        return "C"
    if v <= 150_000:
        return "D"
    if v <= 200_000:
        return "E"
    if v <= 300_000:
        return "F"
    if v <= 500_000:
        return "G"
    if v <= 1_000_000:
        return "H"
    if v <= 3_000_000:
        return "I"
    if v <= 5_000_000:
        return "J"
    if v <= 10_000_000:
        return "K"
    return "L"


# ============================================================================
# FORMAT: $RANGE  (Character - Loan Amount Range Band Label)
# ============================================================================

RANGE: dict[str, str] = {
    "A": "BELOW AND RM25,000",
    "B": "> RM25,000 - RM50,000",
    "C": "> RM50,000 - RM100,000",
    "D": "> RM100,000 - RM150,000",
    "E": "> RM150,000 - RM200,000",
    "F": "> RM200,000 - RM300,000",
    "G": "> RM300,000 - RM500,000",
    "H": "> RM500,000 - RM1,000,000",
    "I": "> RM1,000,000 - RM3,000,000",
    "J": "> RM3,000,000 - RM5,000,000",
    "K": "> RM5,000,000 - RM10,000,000",
    "L": "ABOVE RM10,000,000",
}


def fmt_range(band: str) -> str:
    """$RANGE format: map range band letter to descriptive label."""
    return RANGE.get(str(band).strip(), "")


# ============================================================================
# FORMAT: BLR  (Numeric - BLR spread band letter)
# ============================================================================

def fmt_blr(value: float) -> str:
    """BLR format: classify BLR spread value into band A-O."""
    v = float(value)
    if v < 0:
        return "A"
    if v <= 0.25:
        return "B"
    if v <= 0.50:
        return "C"
    if v <= 0.75:
        return "D"
    if v <= 1.00:
        return "E"
    if v <= 1.25:
        return "F"
    if v <= 1.50:
        return "G"
    if v <= 1.75:
        return "H"
    if v <= 2.00:
        return "I"
    if v <= 2.25:
        return "J"
    if v <= 2.50:
        return "K"
    if v <= 2.75:
        return "L"
    if v <= 3.00:
        return "M"
    if v <= 4.00:
        return "N"
    return "O"


# ============================================================================
# FORMAT: COF  (Numeric - Cost of Funds band letter)
# ============================================================================

def fmt_cof(value: float) -> str:
    """COF format: classify cost-of-funds rate into band A-H."""
    v = float(value)
    if v <= 4.0:
        return "A"
    if v <= 7.0:
        return "B"
    if v <= 8.0:
        return "C"
    if v <= 9.0:
        return "D"
    if v <= 10.0:
        return "E"
    if v <= 11.0:
        return "F"
    if v <= 12.0:
        return "G"
    return "H"


# ============================================================================
# FORMAT: BNM  (Numeric - BNM rate band letter)
# ============================================================================

def fmt_bnm(value: float) -> str:
    """BNM format: classify BNM rate into band A-G."""
    v = float(value)
    if v <= 1.0:
        return "A"
    if v <= 2.0:
        return "B"
    if v <= 3.0:
        return "C"
    if v <= 4.0:
        return "D"
    if v <= 6.0:
        return "E"
    if v <= 8.0:
        return "F"
    return "G"


# ============================================================================
# FORMAT: FIXEDF  (Numeric - Fixed rate band letter)
# ============================================================================

def fmt_fixedf(value: float) -> str:
    """FIXEDF format: classify fixed interest rate into band A-H."""
    v = float(value)
    if v <= 4.0:
        return "A"
    if v <= 7.0:
        return "B"
    if v <= 8.0:
        return "C"
    if v <= 9.0:
        return "D"
    if v <= 10.0:
        return "E"
    if v <= 11.0:
        return "F"
    if v <= 12.0:
        return "G"
    return "H"


# ============================================================================
# FORMAT: $BLR  (Character - BLR band letter to label)
# ============================================================================

BLR_LABEL: dict[str, str] = {
    "A": "<0.00%",
    "B": "0.00% TO 0.25%",
    "C": ">0.25% TO 0.50%",
    "D": ">0.50% TO 0.75%",
    "E": ">0.75% TO 1.00%",
    "F": ">1.00% TO 1.25%",
    "G": ">1.25% TO 1.50%",
    "H": ">1.50% TO 1.75%",
    "I": ">1.75% TO 2.00%",
    "J": ">2.00% TO 2.25%",
    "K": ">2.25% TO 2.50%",
    "L": ">2.50% TO 2.75%",
    "M": ">2.75% TO 3.00%",
    "N": ">3.00% TO 4.00%",
    "O": ">4.00%",
}


def fmt_blr_label(band: str) -> str:
    """$BLR format: map BLR band letter to descriptive label."""
    return BLR_LABEL.get(str(band).strip(), "")


# ============================================================================
# FORMAT: $COF  (Character - COF band letter to label)
# ============================================================================

COF_LABEL: dict[str, str] = {
    "A": " 0.00% TO 4.00%",
    "B": ">4.00% TO 7.00%",
    "C": ">7.00% TO 8.00%",
    "D": ">8.00% TO 9.00%",
    "E": ">9.00% TO 10.00%",
    "F": ">10.00% TO 11.00%",
    "G": ">11.00% TO 12.00%",
    "H": ">12.00%",
}


def fmt_cof_label(band: str) -> str:
    """$COF format: map COF band letter to descriptive label."""
    return COF_LABEL.get(str(band).strip(), "")


# ============================================================================
# FORMAT: $BNM  (Character - BNM band letter to label)
# ============================================================================

BNM_LABEL: dict[str, str] = {
    "A": " 0.00% TO 1.00%",
    "B": ">1.00% TO 2.00%",
    "C": ">2.00% TO 3.00%",
    "D": ">3.00% TO 4.00%",
    "E": ">4.00% TO 6.00%",
    "F": ">6.00% TO 8.00%",
    "G": ">8.00%",
}


def fmt_bnm_label(band: str) -> str:
    """$BNM format: map BNM band letter to descriptive label."""
    return BNM_LABEL.get(str(band).strip(), "")


# ============================================================================
# FORMAT: $FIXEDF  (Character - Fixed rate band letter to label)
# ============================================================================

FIXEDF_LABEL: dict[str, str] = {
    "A": " 0.00% TO 4.00%",
    "B": ">4.00% TO 7.00%",
    "C": ">7.00% TO 8.00%",
    "D": ">8.00% TO 9.00%",
    "E": ">9.00% TO 10.00%",
    "F": ">10.00% TO 11.00%",
    "G": ">11.00% TO 12.00%",
    "H": ">12.00%",
}


def fmt_fixedf_label(band: str) -> str:
    """$FIXEDF format: map fixed rate band letter to descriptive label."""
    return FIXEDF_LABEL.get(str(band).strip(), "")


# ============================================================================
# FORMAT: $SECTDES  (Character - Sector Description by sector code)
# ============================================================================

SECTDES: dict[str, str] = {
    "0310": "RESIDENTIAL PROPERTY",
    "0311": "LOW COST",
    "0312": "LOWER COST",
    "0313": "MEDIUM COST",
    "0314": "HIGHER MEDIUM COST",
    "0315": "HIGH COST",
    "0316": "0316",
    "0320": "NON-RESIDENTIAL PROPERTY",
    "0321": "INDUSTRIAL BUILDINGS AND FACTORIES",
}


def fmt_sectdes(sectorcd: str) -> str:
    """$SECTDES format: map sector code string to sector description."""
    return SECTDES.get(str(sectorcd).strip(), "")
