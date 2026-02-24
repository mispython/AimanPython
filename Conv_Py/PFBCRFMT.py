# !/usr/bin/env python3
"""
Program : PFBCRFMT
Function: BNM Taxonomy and Data Definition for CCRIS Enhancement
          Defines format lookup tables (equivalent to SAS PROC FORMAT) for:
          - FACCODE   : Facility code by product
          - ODFACCODE : OD facility code by product
          - FACNAME   : Facility code listing (description)
          - FUNDSCH   : Fund scheme by product
          - FSCHEMEC  : Fund scheme by census
          - SYND      : Syndicated indicator by product
          - ODFUNDSCH : OD fund scheme by product
          - ODFACILITY: OD facility by product
          - FCONCEPT  : Financing concept by product
          - ODFCONCEPT: OD financing concept
          - FINCEPT   : Financing concept listing (description)
          - $CPURPHP  : Character purpose HP mapping
          - LNPRODC   : Loan product code
          - $PURPHP   : Character purpose HP passthrough

CWK ESMR 2012-1132 21JUNE12
ESMR DESC: FACCODE 902 AND FUNDSCHEME REVISE
MFM ESMR 2012-3641
ESMR DESC: UPDATE ALL APPROVED CREDIT AS CANCELLED BY CUSTOMER
"""

from typing import Optional

# ===========================================================================
# FORMAT: FACCODE  -- FACILITY CODE BY PRODUCT
# ===========================================================================

def format_faccode(product: Optional[int]) -> int:
    """Map product code to BNM facility code."""
    if product is None:
        return 0

    # 34210 - Revolving Credit
    _34210 = {
        146, 184, 302, 350, 351, 364, 365, 506,
        604, 605, 634, 660, 685, 802, 803, 856,
        903, 910, 925, 951, 806, 808,
        810, 812, 814, 190, 689, 695, 495, 192,
        195, 697, 817, 818,
        # 857-860
        857, 858, 859, 860,
    }

    # 34320 - Housing Loans/Financing
    _34320 = {
        124, 139, 140, 141, 142, 145, 147, 150,
        151, 152, 156, 173, 175, 200, 201, 204,
        205, 225, 226, 227, 228, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
        240, 241, 242, 243, 244, 245, 246, 247, 248, 249,
        400, 600, 650, 651, 664, 981, 984, 991, 994,
        409, 410, 250, 423, 486, 489, 494, 479,
        484, 440, 677, 692, 412, 413,
        253, 431, 432, 433, 445, 446,
        # 110-119
        110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
        # 209-220
        209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220,
        # 254-260
        254, 255, 256, 257, 258, 259, 260,
        # 472-474
        472, 473, 474,
    }

    # 34341 - Leasing Receivables
    _34341 = {390, 799}

    # 34363 - Block Discounting Receivables
    _34363 = {360, 770, 775, 908}

    # 34364 - Bridging Loans/Financing
    _34364 = {309, 310, 608, 637, 904, 905, 919, 920}

    # 34371 - Staff Loans/Financing
    _34371 = {
        100, 101, 108,
        # 004-007
        4, 5, 6, 7,
        # 015, 020
        15, 20,
        # 025-034
        25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
        # 060-063
        60, 61, 62, 63,
        # 070-078
        70, 71, 72, 73, 74, 75, 76, 77, 78,
        # 102-105
        102, 103, 104, 105,
    }

    # 34391 - Other Term Loans/Financing
    _34391 = {
        120, 122, 126, 127, 129, 143, 144, 148,
        149, 169, 170, 172,
        174, 185, 186, 193, 194,
        315, 316, 335, 345, 348,
        349, 401, 402, 500,
        504, 505, 509, 510, 512, 555,
        556, 601, 602, 603,
        606, 607,
        800, 801, 804, 900, 901, 902, 906,
        907, 909, 914, 915, 950, 982, 187, 687,
        985, 992, 995, 403, 404, 405, 411, 462,
        463, 805, 807, 809, 811, 813, 188, 189,
        688, 406, 407, 320, 609, 916, 368, 418,
        425, 429, 430, 918, 485, 487, 676, 691,
        488, 471, 476, 478, 442, 694,
        434, 435, 437, 438, 436, 576, 421, 577,
        321, 322, 444, 447, 448, 922, 578, 134,
        137, 133, 197, 199, 696, 815, 816, 574,
        427, 428, 439,
        # 153-155
        153, 154, 155,
        # 157-165
        157, 158, 159, 160, 161, 162, 163, 164, 165,
        # 176-183
        176, 177, 178, 179, 180, 181, 182, 183,
        # 300, 301
        300, 301,
        # 304, 305
        304, 305,
        # 356-359
        356, 357, 358, 359,
        # 361-363
        361, 362, 363,
        # 515-533
        515, 516, 517, 518, 519, 520, 521, 522, 523, 524,
        525, 526, 527, 528, 529, 530, 531, 532, 533,
        # 559-561
        559, 560, 561,
        # 564-570
        564, 565, 566, 567, 568, 569, 570,
        # 631-633
        631, 632, 633,
        # 635, 636
        635, 636,
        # 654-659
        654, 655, 656, 657, 658, 659,
        # 661-663
        661, 662, 663,
        # 665-667
        665, 666, 667,
        # 681-684
        681, 682, 683, 684,
        # 686
        686,
        # 851-855
        851, 852, 853, 854, 855,
        # 480-481
        480, 481,
        # 573
        573,
    }

    # 34392 - Personal Loans/Financing
    _34392 = {
        135, 136, 138, 325, 330, 340, 355,
        391, 652, 653, 668, 669,
        680, 306, 307, 308, 311, 367, 313,
        369, 419, 420, 693, 422,
        # 303
        303,
        # 464, 465
        464, 465,
        # 672-675
        672, 673, 674, 675,
        # 468
        468,
        # 424, 426
        424, 426,
        # 490-493
        490, 491, 492, 493,
        # 496-498
        496, 497, 498,
        # 482-483
        482, 483,
        # 475, 477
        475, 477,
        # 441, 443
        441, 443,
    }

    # 34333 - Purchase of Passenger Cars
    _34333 = {678, 679, 698, 699}

    if product in _34210:
        return 34210
    if product in _34320:
        return 34320
    if product in _34341:
        return 34341
    if product in _34363:
        return 34363
    if product in _34364:
        return 34364
    if product in _34371:
        return 34371
    if product in _34391:
        return 34391
    if product in _34392:
        return 34392
    if product in _34333:
        return 34333
    return 0


# ===========================================================================
# FORMAT: ODFACCODE  -- OD FACILITY CODE BY PRODUCT
# ===========================================================================

def format_odfaccode(product: Optional[int]) -> int:
    """Map OD product code to BNM OD facility code."""
    if product is None:
        return 0

    _34110 = set()
    # 030-033
    _34110.update(range(30, 34))
    # 050-058
    _34110.update(range(50, 59))
    # 060-071
    _34110.update(range(60, 72))
    # 085-091
    _34110.update(range(85, 92))
    # 093-097
    _34110.update(range(93, 98))
    # 100-106
    _34110.update(range(100, 107))
    # 108-123
    _34110.update(range(108, 124))
    # 125
    _34110.add(125)
    # 135, 137, 138
    _34110.update({135, 137, 138})
    # 150-164
    _34110.update(range(150, 165))
    # 166-170
    _34110.update(range(166, 171))
    # 174-176
    _34110.update(range(174, 177))
    # 179-182
    _34110.update(range(179, 183))
    # 189, 190, 192-198
    _34110.add(189)
    _34110.add(190)
    _34110.update(range(192, 199))
    # 183, 184, 187, 188
    _34110.update({183, 184, 187, 188})
    # 045, 046, 092, 040
    _34110.update({45, 46, 92, 40})
    # 013-019
    _34110.update(range(13, 20))
    # 075-076
    _34110.update({75, 76})
    # 003-012
    _34110.update(range(3, 13))
    # 020-025
    _34110.update(range(20, 26))
    # 081, 082
    _34110.update({81, 82})

    _34111 = {59, 124, 191}

    _34220 = {34, 133, 134, 177, 178, 77, 78}

    if product in _34110:
        return 34110
    if product in _34111:
        return 34111
    if product in _34220:
        return 34220
    return 0


# ===========================================================================
# FORMAT: FACNAME  -- FACILITY CODE LISTING (DESCRIPTION)
# ===========================================================================

_FACNAME_MAP = {
    34333: 'PURCHASE OF PASSENGER CARS',
    34334: 'OTHER HIRE PURCHASE RECEIVABLES',
    34341: 'LEASING RECEIVABLES',
    34363: 'BLOCK DISCOUNTING RECEIVABLES',
    34364: 'BRIDGING LOANS/FINANCING',
    34450: 'FACTORING RECEIVABLE',
    34392: 'PERSONAL LOANS/FINANCING',
    34320: 'HOUSING LOANS/FINANCING',
    34391: 'OTHER TERM LOANS/FINANCING',
    34471: 'EXPORT, CUSTOMER LIABILITIES FOR BANKERS ACCEPTANCE',
    34472: 'IMPORT, CUSTOMER LIABILITIES FOR BANKERS ACCEPTANCE',
    34475: 'PURCHASE/SALES, CUSTOMER LIABILITIES FOR BA',
    34473: 'EXPORT, OWN BANKERS ACCEPTANCE BILLS DISCOUNTED',
    34474: 'IMPORT, OWN BANKERS ACCEPTANCE BILLS DISCOUNTED',
    34476: 'PURCHASE/SALES, OWN BA BILLS DISCOUNTED',
    34479: 'OTHER TRADE BILLS DISCOUNTED',
    34440: 'TRUST RECEIPTS',
    34460: 'FLOOR STOCKING LOANS/FINANCING',
    34530: 'EXPORT CREDIT REFINANCING',
    34480: 'OTHER TRADE FACILITIES',
    34110: 'OVERDRAFT',
    34120: 'TEMPORARY OVERDRAFT',
    34210: 'REVOLVING CREDIT',
    34510: 'CREDIT CARD',
    34520: 'CHARGE CARD',
    34220: 'SHARE MARGIN FINANCING',
    34371: 'STAFF LOANS/FINANCING',
    34540: 'MULTI-OPTION FACILITY',
    34111: 'OUTPORT CHEQUE PURCHASED',
    34610: 'PROJECT FINANCING',
    34620: 'EQUITY FINANCING',
    34630: 'PAWNBROKING',
    34640: 'BENEVOLENT CREDIT',
    34651: 'PEMBIAYAAN MIKRO - FINANCIAL INSTITUTION OWN FUND',
    34652: 'PEMBIAYAAN MIKRO - MICRO ENTERPRISE FUND',
    39999: 'OTHER LOANS/FINANCING',
    34810: 'LETTER OF CREDIT',
    34820: 'STANDBY LETTER OF CREDIT',
    34831: 'FINANCIAL GUARANTEE - TRADE',
    34832: 'FINANCIAL GUARANTEE - NON-TRADE',
    34840: 'PERFORMANCE GUARANTEE',
    34850: 'SHIPPING GUARANTEE',
    34860: 'FORWARD FOREIGN EXCHANGE',
    34899: 'OTHER OFF-BALANCE SHEET FACILITIES',
}


def format_facname(faccode: Optional[int]) -> str:
    """Map BNM facility code to facility description."""
    if faccode is None:
        return 'NOT APPLICABLE'
    return _FACNAME_MAP.get(faccode, 'NOT APPLICABLE')


# ===========================================================================
# FORMAT: FUNDSCH  -- FUND SCHEME BY PRODUCT
# ===========================================================================

def format_fundsch(product: Optional[int]) -> str:
    """Map product code to fund scheme code."""
    if product is None:
        return '00'

    # '20'
    _20 = {
        170, 555, 556, 559, 560, 427, 561, 564,
        566, 567, 569, 573, 909, 418,
    }
    # '24'
    _24 = {437, 565}
    # '25'
    _25 = {438}
    # '27'
    _27 = {578}
    # '29'
    _29 = {435, 576}
    # '30'
    _30 = {421, 577}
    # '63'
    _63 = {160, 161}
    # '64'
    _64 = {428, 439, 574}
    # '  ' (blank/spaces)
    _blank = {575, 169, 434, 448, 144, 172, 301, 320, 510, 531, 568, 570}

    if product in _20:
        return '20'
    if product in _24:
        return '24'
    if product in _25:
        return '25'
    if product in _27:
        return '27'
    if product in _29:
        return '29'
    if product in _30:
        return '30'
    if product in _63:
        return '63'
    if product in _64:
        return '64'
    if product in _blank:
        return '  '
    return '00'


# ===========================================================================
# FORMAT: FSCHEMEC  -- FUND SCHEME BY CENSUS
# ===========================================================================

def format_fschemec(census: Optional[float]) -> str:
    """Map census code (numeric with decimals) to fund scheme code."""
    if census is None:
        return '  '

    # '00'
    _00 = {
        301.00, 301.03, 301.04, 301.05, 301.06,
        301.07, 301.08, 301.10, 301.11, 301.12,
        301.16, 301.18, 301.19, 301.20, 301.21,
        301.22, 301.23, 301.24, 301.25, 301.26,
        320.00, 320.02, 320.03, 320.04, 510.02,
        448.07, 448.08, 448.09, 448.10, 448.11,
        169.00, 169.05,
    }
    # '20'
    _20 = {
        169.03, 169.04, 568.00, 568.01, 434.10,
        434.11,
    }
    # '22'
    _22 = {
        575.02, 575.03, 575.05, 169.01, 169.02,
        434.00, 434.01, 434.03, 144.01, 144.02,
    }
    # '23'
    _23 = {575.04, 434.02, 575.11, 434.08}
    # '25'
    _25 = {
        568.02, 568.04, 568.05, 568.06, 568.07,
        568.08, 568.09, 568.10, 568.11, 568.12,
        568.14, 568.15, 568.16, 568.17, 568.18,
        568.19, 568.20, 568.21, 568.22, 568.25,
        568.26, 568.27, 568.28, 568.29,
    }
    # '26'
    _26 = {
        172.10, 172.11, 570.03, 570.04, 570.08,
        570.09, 570.12, 570.13,
    }
    # '27'
    _27 = {
        448.00, 448.01, 448.02, 448.03, 448.05,
        448.06, 448.12, 448.13,
    }
    # '28'
    _28 = {
        172.03, 172.04, 172.07, 172.08, 172.12,
        172.13, 570.01, 570.02, 570.05, 570.06,
        570.07, 570.10, 570.11,
    }
    # '61'
    _61 = {
        144.00, 434.06, 434.07, 434.09, 434.12,
        434.13, 434.14, 434.15, 434.16, 434.17,
        434.18, 434.19, 531.02, 575.00, 575.01,
        575.08, 575.10, 575.12, 575.13, 575.14,
        575.15, 575.16, 575.17, 575.18, 575.19,
        575.20, 575.21, 575.22,
    }
    # '62'
    _62 = {434.04, 434.05, 575.06, 575.07, 575.09}
    # '63'
    _63 = {301.01, 320.01, 510.03}

    # Round to 2 decimal places to avoid floating-point mismatch
    val = round(float(census), 2)

    if val in _00:
        return '00'
    if val in _20:
        return '20'
    if val in _22:
        return '22'
    if val in _23:
        return '23'
    if val in _25:
        return '25'
    if val in _26:
        return '26'
    if val in _27:
        return '27'
    if val in _28:
        return '28'
    if val in _61:
        return '61'
    if val in _62:
        return '62'
    if val in _63:
        return '63'
    return '  '


# ===========================================================================
# FORMAT: SYND  -- SYNDICATED BY PRODUCT
# ===========================================================================

_SYND_Y = {180, 185, 186, 804, 914, 915, 919, 920, 925, 950, 951}


def format_synd(product: Optional[int]) -> str:
    """Map product code to syndicated indicator Y/N."""
    if product is None:
        return 'N'
    return 'Y' if product in _SYND_Y else 'N'


# ===========================================================================
# FORMAT: ODFUNDSCH  -- OD FUND SCHEME BY PRODUCT
# ===========================================================================

def format_odfundsch(product: Optional[int]) -> str:
    """Map OD product code to fund scheme code."""
    if product is None:
        return '00'

    _25 = {17, 187, 188, 14, 35, 37}
    _63 = {169}

    if product in _25:
        return '25'
    if product in _63:
        return '63'
    return '00'


# ===========================================================================
# FORMAT: ODFACILITY  -- OD FACILITY BY PRODUCT
# ===========================================================================

def format_odfacility(product: Optional[int]) -> str:
    """Map OD product code to OD facility code string."""
    if product is None:
        return '34110'

    _34111 = {124, 165, 191, 187, 188}
    _34220 = {34, 133, 134, 177, 178, 77, 78}

    if product in _34111:
        return '34111'
    if product in _34220:
        return '34220'
    return '34110'


# ===========================================================================
# FORMAT: FCONCEPT  -- FINANCING CONCEPT BY PRODUCT
# ===========================================================================

def format_fconcept(product: Optional[int]) -> int:
    """Map product code to financing concept code."""
    if product is None:
        return 99

    _10 = {
        118, 119, 122, 124, 126, 139,
        145, 147, 148, 170, 173, 174,
        462, 463, 469, 470, 674, 675,
    }
    _14 = {668, 669, 672, 673}
    # 16: 128, 130-132, 983, 103, 104, 108
    _16 = {128, 130, 131, 132, 983, 103, 104, 108}
    # 26: 133-137, 138, 160-163, 169, 172, 182, 159, 185,
    #     427, 428, 434, 435, 437, 438, 439, 436, 465, 421, 444, 447, 448, 192
    _26 = {
        133, 134, 135, 136, 137, 138,
        160, 161, 162, 163,
        169, 172, 182, 159, 185,
        427, 428, 434, 435, 437, 438, 439, 436, 465, 421, 444, 447, 448, 192,
    }
    # 28: 186, 190, 471-498
    _28 = {186, 190}
    _28.update(range(471, 499))
    # 29: 152-155, 175-178, 187, 188, 102, 105,
    #     400-402, 406, 407, 409, 410, 144,
    #     411, 412-416, 191, 417, 418-420, 422-426,
    #     429, 430-433, 464, 468, 106, 697
    _29 = {
        152, 153, 154, 155,
        175, 176, 177, 178,
        187, 188, 102, 105,
        400, 401, 402, 406, 407, 409, 410, 144,
        411, 412, 413, 414, 415, 416,
        191, 417,
        418, 419, 420,
        422, 423, 424, 425, 426,
        429, 430, 431, 432, 433,
        464, 468, 106, 697,
    }
    # 36: 110-117, 120, 127, 129, 140-143, 146, 149-151,
    #     156-158, 164, 165, 179, 183, 181,
    #     189, 194, 403, 404, 440-443, 461, 466,
    #     467, 445, 446, 851-860, 405, 184, 195, 197
    _36 = {
        110, 111, 112, 113, 114, 115, 116, 117,
        120, 127, 129,
        140, 141, 142, 143,
        146, 149, 150, 151,
        156, 157, 158,
        164, 165, 179, 183, 181,
        189, 194,
        403, 404,
        440, 441, 442, 443,
        461, 466, 467, 445, 446,
        851, 852, 853, 854, 855, 856, 857, 858, 859, 860,
        405, 184, 195, 197,
    }
    _45 = {193, 180}
    _22 = {199, 696}

    if product in _10:
        return 10
    if product in _14:
        return 14
    if product in _16:
        return 16
    if product in _26:
        return 26
    if product in _28:
        return 28
    if product in _29:
        return 29
    if product in _36:
        return 36
    if product in _45:
        return 45
    if product in _22:
        return 22
    return 99


# ===========================================================================
# FORMAT: ODFCONCEPT  -- OD FINANCING CONCEPT BY PRODUCT
# ===========================================================================

def format_odfconcept(product: Optional[int]) -> int:
    """Map OD product code to financing concept code."""
    if product is None:
        return 99

    # 11: 060-064, 066, 067, 097, 164, 165
    _11 = {60, 61, 62, 63, 64, 66, 67, 97, 164, 165}
    # 12: 169
    _12 = {169}
    # 26: 17-19, 46, 071, 075, 076, 094, 095, 167, 185, 14, 13, 7, 8, 22, 5, 6, 020, 021, 187, 188
    _26 = {
        17, 18, 19, 46, 71, 75, 76, 94, 95, 167,
        185, 14, 13, 7, 8, 22, 5, 6, 20, 21, 187, 188,
    }
    # 28: 045, 081, 023-025
    _28 = {45, 81, 23, 24, 25}
    # 31: 093, 096, 160, 161, 162, 163, 182, 092, 74
    _31 = {93, 96, 160, 161, 162, 163, 182, 92, 74}
    # 36: 047, 073, 166, 168, 183, 184, 15, 16, 70
    _36 = {47, 73, 166, 168, 183, 184, 15, 16, 70}

    if product in _11:
        return 11
    if product in _12:
        return 12
    if product in _26:
        return 26
    if product in _28:
        return 28
    if product in _31:
        return 31
    if product in _36:
        return 36
    return 99


# ===========================================================================
# FORMAT: FINCEPT  -- FINANCING CONCEPT LISTING (DESCRIPTION)
# ===========================================================================

_FINCEPT_MAP = {
    10: 'BAI BITHAMAN AJIL',
    11: 'QARD',
    12: 'MURABAHAH',
    13: 'ISTISNA',
    14: 'MUSYARAKAH',
    15: 'IJARAH MUNTAHIAH BIT TAMLIK',
    16: 'IJARAH THUMMA AL-BAI',
    17: 'BAI UL TAKJIRI',
    18: 'IJARAH',
    19: 'BAI AL-DAYN',
    20: 'MUDHARABAH',
    21: 'RAHNU',
    22: 'WAKALAH',
    23: 'KAFALAH',
    24: 'SARF',
    25: 'HIWALAH',
    26: 'BAI INNAH',
    27: 'BAI AS-SALAM',
    28: 'TAWARRUQ',
    29: 'SHIRKAH MUTANAQISAH',
    36: 'OTHER MURABAHAH',
    49: 'OTHER ISLAMIC FINANCING CONCEPT',
}


def format_fincept(concept: Optional[int]) -> str:
    """Map financing concept code to description."""
    if concept is None:
        return 'NOT APPLICABLE'
    return _FINCEPT_MAP.get(concept, 'NOT APPLICABLE')


# ===========================================================================
# FORMAT: $CPURPHP  -- CHARACTER PURPOSE HP MAPPING
# ===========================================================================

_CPURPHP_MAP: dict[str, str] = {}

# '3110'
for _v in ['5', '05', 'C7', 'C9', 'D1', 'D3', 'D4', 'D6']:
    _CPURPHP_MAP[_v] = '3110'
# '3120'
for _v in ['C8', 'D2', 'D5']:
    _CPURPHP_MAP[_v] = '3120'
# '3200'
for _v in ['4', '04', 'E2', 'E3']:
    _CPURPHP_MAP[_v] = '3200'
# '3300'
for _v in ['83', 'D7', 'D8', 'D9', 'E1', 'E5', 'E6']:
    _CPURPHP_MAP[_v] = '3300'
# '3900'
_CPURPHP_MAP['84'] = '3900'
# '4100'
_CPURPHP_MAP['22'] = '4100'
# '5100'
_CPURPHP_MAP['79'] = '5100'
# '5200'
for _v in ['63', '80', '81', '82']:
    _CPURPHP_MAP[_v] = '5200'


def format_cpurphp(code: Optional[str]) -> str:
    """Map character purpose HP code."""
    if code is None:
        return '5100'
    return _CPURPHP_MAP.get(str(code).strip(), '5100')


# ===========================================================================
# FORMAT: LNPRODC  -- LOAN PRODUCT CODE
# ===========================================================================

def format_lnprodc(product: Optional[int]) -> str:
    """Map product code to loan product category code."""
    if product is None:
        return '34710'

    # Individual explicit mappings
    _map: dict[int, str] = {
        5:   '34322',  # STAFF LOAN - HOUSING LOAN
        10:  '34312',  # STAFF LOAN - FESTIVE ADVANCES
        15:  '34312',  # STAFF LOAN - CAR LOAN
        20:  '34312',  # STAFF LOAN - MOTORCYCLE LOAN
        # 112, 113, 114, 116, 117, 118, 100, 102 = 34322 TIER 1 ABBA HOUSING LOAN
        112: '34322', 113: '34322', 114: '34322', 116: '34322',
        117: '34322', 118: '34322', 100: '34322', 102: '34322',
        # 101, 210 = 34322 HOUSING LOAN SOLD TO CAGAMAS
        101: '34322', 210: '34322',
        105: '34312',  # ABBA UNIT TRUST SCHEME
        110: '34332',  # AITAB - GOODS FINANCING
        115: '34332',  # AITAB - GOODS FINANCING
        133: '34391',  # GENERIC BAE TERM FINANCING-I
        172: '34391',  # BAE BNM/FSMI
        200: '34321',  # PRIORITY SECTOR HOUSING LOAN
        201: '34321',  # PRIORITY SECTOR HOUSING LOAN
        # 227, 228, 230, 231, 234, 204, 205 = 34322 NORMAL HOUSING LOAN
        227: '34322', 228: '34322', 230: '34322', 231: '34322',
        234: '34322', 204: '34322', 205: '34322',
        206: '34322',  # NORMAL HOUSING LOAN(PROPERTY>RM150K)
        300: '34312',  # FIXED LOAN
        304: '34312',  # FIXED LOAN
        305: '34312',  # FIXED LOAN
        307: '34312',  # FIXED LOAN SOLD TO CAGAMAS
        310: '34312',  # UNSECURED LOAN
        315: '34210',  # QUICK CASH LOAN
        325: '34460',  # FLOOR STOCKING
        320: '34210',  # REVOLVING CREDIT
        330: '34210',  # REVOLVING CREDIT
        421: '34391',  # BAE GOVERNMENT FINANCING-I
        434: '34391',  # BAE BNM SPECIAL RELIEF FACILITY-I
        435: '34391',  # BAE BNM SPECIAL RELIEF FACILITY-I
        436: '34391',  # BAE GOV GUARANTEE SCHEMES-I
        437: '34391',  # BAE BNM SPECIAL RELIEF FACILITY-I
        438: '34391',  # BAE BNM SPECIAL RELIEF FACILITY-I
        439: '34652',  # BAE BNM SPECIAL RELIEF FACILITY-I
        500: '34149',  # LOAN FUND FOR HAWKER PETTY TRADER 1994 (LFHPT)
        504: '34312',  # FIXED LOAN UNDER NPGS
        505: '34312',  # FIXED LOAN UNDER NPGS
        # 509, 510 = 34210 FIXED LOAN UNDER NPGS-BLK GUARANTEE SCHEME
        509: '34210', 510: '34210',
        555: '34312',  # SPECIAL FUND FOR TOURISM
        556: '34312',  # SPECIAL FUND FOR TOURISM
        559: '34312',  # NEW ENTREPRENEUR FUND
        560: '34312',  # NEW ENTREPRENEUR FUND
        564: '34312',  # FUND FOR FOOD
        565: '34312',  # FUND FOR FOOD
        576: '34391',  # FL/BNM SME ADF
        # 700, 705, 709, 710, 750 = 34332 HIRE PURCHASE
        700: '34332', 705: '34332', 709: '34332', 710: '34332', 750: '34332',
        752: '34332',  # HIRE PURCHASE
        760: '34332',  # HIRE PURCHASE
        770: '34360',  # BLOCK DISCOUNTING
        775: '34360',  # BLOCK DISCOUNTING
        799: '34312',  # LEASING
        900: '34312',  # CBL - FIXED LOAN
        901: '34312',  # CBL - FIXED LOAN
        902: '34312',  # CBL - FIXED LOAN
        910: '34312',  # CBL - FIXED LOAN SOLD TO CAGAMAS
        911: '34210',  # CBL - REVOLVING CREDIT
        913: '34210',  # CBL - REVOLVING CREDIT
        912: '34312',  # CBL - SHARE LOAN TERM
        920: '34351',  # BRIDGING LOAN
        930: '34312',  # CBL - UNSECURED BUSINESS LOAN
        950: '34311',  # CBL - SYNDICATED FIXED LOAN
        951: '34220',  # CBL - SYNDICATED REVOLVING CREDIT
        952: '34352',  # CBL - SYNDICATED BRIDGING LOAN
        960: '34450',  # CBL - FACTORING
        961: '34450',  # CBL - FACTORING
    }

    return _map.get(product, '34710')


# ===========================================================================
# FORMAT: $PURPHP  -- CHARACTER PURPOSE HP PASSTHROUGH
# ===========================================================================

_PURPHP_VALID = {'3110', '3120', '3200', '3201', '3300', '3900', '3901', '4100', '5100', '5200'}


def format_purphp(code: Optional[str]) -> str:
    """Passthrough purpose HP code; default '3120' for unrecognised values."""
    if code is None:
        return '3120'
    val = str(code).strip()
    return val if val in _PURPHP_VALID else '3120'


# ===========================================================================
# EXPORT  (convenience dict of all format functions)
# ===========================================================================

FORMATS = {
    'FACCODE':    format_faccode,
    'ODFACCODE':  format_odfaccode,
    'FACNAME':    format_facname,
    'FUNDSCH':    format_fundsch,
    'FSCHEMEC':   format_fschemec,
    'SYND':       format_synd,
    'ODFUNDSCH':  format_odfundsch,
    'ODFACILITY': format_odfacility,
    'FCONCEPT':   format_fconcept,
    'ODFCONCEPT': format_odfconcept,
    'FINCEPT':    format_fincept,
    'CPURPHP':    format_cpurphp,
    'LNPRODC':    format_lnprodc,
    'PURPHP':     format_purphp,
}


if __name__ == '__main__':
    # Smoke-test a sample of values
    print(f"FACCODE(146)     = {format_faccode(146)}")       # 34210
    print(f"FACCODE(110)     = {format_faccode(110)}")       # 34320
    print(f"FACCODE(390)     = {format_faccode(390)}")       # 34341
    print(f"FACCODE(360)     = {format_faccode(360)}")       # 34363
    print(f"FACCODE(309)     = {format_faccode(309)}")       # 34364
    print(f"FACCODE(4)       = {format_faccode(4)}")         # 34371
    print(f"FACCODE(120)     = {format_faccode(120)}")       # 34391
    print(f"FACCODE(135)     = {format_faccode(135)}")       # 34392
    print(f"FACCODE(678)     = {format_faccode(678)}")       # 34333
    print(f"FACCODE(999)     = {format_faccode(999)}")       # 0
    print(f"ODFACCODE(30)    = {format_odfaccode(30)}")      # 34110
    print(f"ODFACCODE(59)    = {format_odfaccode(59)}")      # 34111
    print(f"ODFACCODE(34)    = {format_odfaccode(34)}")      # 34220
    print(f"FACNAME(34210)   = {format_facname(34210)}")     # REVOLVING CREDIT
    print(f"FUNDSCH(170)     = {format_fundsch(170)}")       # 20
    print(f"FUNDSCH(160)     = {format_fundsch(160)}")       # 63
    print(f"FSCHEMEC(301.00) = {format_fschemec(301.00)}")   # 00
    print(f"SYND(180)        = {format_synd(180)}")          # Y
    print(f"SYND(100)        = {format_synd(100)}")          # N
    print(f"ODFUNDSCH(17)    = {format_odfundsch(17)}")      # 25
    print(f"ODFACILITY(124)  = {format_odfacility(124)}")    # 34111
    print(f"FCONCEPT(118)    = {format_fconcept(118)}")      # 10
    print(f"ODFCONCEPT(60)   = {format_odfconcept(60)}")     # 11
    print(f"FINCEPT(10)      = {format_fincept(10)}")        # BAI BITHAMAN AJIL
    print(f"CPURPHP('5')     = {format_cpurphp('5')}")       # 3110
    print(f"LNPRODC(5)       = {format_lnprodc(5)}")         # 34322
    print(f"PURPHP('3110')   = {format_purphp('3110')}")     # 3110
    print(f"PURPHP('9999')   = {format_purphp('9999')}")     # 3120
