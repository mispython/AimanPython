# !/usr/bin/env python3
"""
Program: PFBLNFMT
Purpose: Product and Loan Format Definitions
         Central format library for loan product mappings and classifications

Date: Original SAS format definitions

TAKE NOTE: ANY AMENDMENTS TO PRODUCTS MAPPING, THESE FORMATS ARE AFFECTED:
           LNDENOM, LNPROD, LNTYPE & LNRATE

This module provides all format mapping functions used across NPL processing programs.
Includes:
- Product denomination (Domestic/Islamic)
- Product codes and mappings
- Customer type codes
- Risk classifications
- Loan term classifications
- State codes
- Sector codes
- Collateral codes
- Arrears classifications
"""

# Product code constants
HP_PRODUCTS = [110, 115, 700, 705, 983, 993]  # HP PRODUCTS CODES
HPD_PRODUCTS = [110, 115, 700, 705]  # HP DIRECT
HOMEIS_PRODUCTS = [112, 113, 114, 116]  # HOME PLAN ISLAMIC
HOMECV_PRODUCTS = [227, 228, 230, 231, 237, 238, 239, 240, 241]  # HOME PLAN CONVENTIONAL
MOREIS_PRODUCTS = [117, 118]  # MORE PLAN ISLAMIC
MORECV_PRODUCTS = [234, 235, 236, 242]  # MORE PLAN CONVENTIONAL
SWIFTIS_PRODUCTS = [126, 127, 167]  # ABBA SWIFT PLAN
SWIFTCV_PRODUCTS = [359]  # SWIFT PLAN CONVENTIONAL


def format_lndenom(loantype):
    """
    Product denomination of Domestic or Islamic
    Returns: 'I' for Islamic, 'D' for Domestic
    """
    islamic_products = [
        100, 101, 102, 105, 106, 110,
        112, 113, 114, 116, 117, 118,
        126,
        115, 120, 121, 170, 127
    ]

    if loantype in islamic_products:
        return 'I'  # ISLAMIC PRODUCTS
    else:
        return 'D'  # DOMESTIC PRODUCTS


def format_lnprod(loantype):
    """
    Product code mapping for BNM reporting
    Returns: Product code string
    """
    product_map = {
        5: '34230',  # STAFF LOAN - HOUSING LOAN
        6: '34230',  # STAFF LOAN - HOUSING LOAN (ALTERNATE)
        10: '34230',  # STAFF LOAN - SPECIAL LOAN SCHEME
        15: '34231',  # STAFF LOAN - CAR LOAN
        20: '34231',  # STAFF LOAN - MOTORCYCLE LOAN
        100: '34120',  # ABBA NORMAL HOUSING FINANCING
        101: '34120',  # ABBA MULTI TIER RATE (SERVICE PROFIT)
        102: '34120',  # ABBA MULTI TIER RATE (SERVICE INSTLMNT)
        105: '34149',  # ABBA UNIT TRUST SCHEME
        106: '54129',  # ABBA UNIT TRUST SCHEME TO CAGAMAS
        110: '34111',  # AITAB - GOODS FINANCING
        115: '34111',  # AITAB - GOODS FINANCING
        112: '34120',  # HOME 4/5 ABBA - CONSTRUCTION
        114: '34120',  # HOME 4/5 ABBA - CONSTRUCTION
        113: '34120',  # HOME 4/5 ABBA - COMPL.PROPERTY
        116: '34120',  # HOME 4/5 ABBA - COMPL.PROPERTY
        117: '34120',  # ABBA MORE PLAN- CONSTR./COMPL.PROP.
        118: '34120',  # ABBA MORE PLAN- CONSTR./COMPL.PROP.
        120: '34149',  # ABBA ASSET FINANCING
        121: '54129',  # ABBA ASSET FINANCING SOLD TO CAGAMAS
        126: '34120',  # ABBA SWIFT LOAN - PROP UNDER CONSTR
        127: '34120',  # ABBA SWIFT LOAN - FIXED LOAN
        170: '34149',  # BNM/SPTF 3F FOOD FUND
        200: '34120',  # PRESCRIBED RATE HOUSING LOAN
        201: '34120',  # PRESCRIBED RATE HOUSING LOAN
        204: '34120',  # NORMAL HOUSING LOAN
        205: '34120',  # NORMAL HOUSING LOAN
        206: '34120',  # NORMAL HOUSING LOAN (PROPERTY>RM150K)
        210: '34120',  # HOUSING LOAN SOLD TO CAGAMAS
        227: '34120',  # HOME 1/2 (PRESCRIBED RATE)
        230: '34120',  # HOME 1/2 (PRESCRIBED RATE)
        228: '34120',  # HOME 1/2 (NON-PRESCRIBED RATE)
        231: '34120',  # HOME 1/2 (NON-PRESCRIBED RATE)
        234: '34120',  # MORE PLAN
        235: '34120',  # MORE PLAN 2
        236: '34120',  # MORE PLAN 3
        237: '34120',  # HOME PLAN 6 (PRESCRIBED RATE)
        238: '34120',  # HOME PLAN 6 (NON-PRESCRIBED RATE)
        239: '34120',  # HOME PLAN 7 (PRESCRIBED RATE)
        240: '34120',  # HOME PLAN 7 (NON-PRESCRIBED RATE)
        241: '34120',  # HOME PLAN 8
        242: '34120',  # MORE PLAN 4
        300: '34149',  # FIXED LOAN
        304: '34149',  # FIXED LOAN
        305: '34149',  # FIXED LOAN
        307: '54129',  # FIXED LOAN SOLD TO CAGAMAS
        310: '34117',  # UNSECURED / PERSONAL LOAN
        315: '34190',  # SHARE LOAN (QSL/RSL)
        325: '34170',  # FLOOR STOCKING
        330: '34190',  # REVOLVING CREDIT
        320: '34190',  # REVOLVING CREDIT
        359: '34149',  # CONVENTIONAL SWIFT (FIXED) LOANS
        504: '34149',  # FIXED LOAN UNDER NPGS
        505: '34149',  # FIXED LOAN UNDER NPGS
        506: '34149',  # SEGS SERVICING INSTALMENT
        509: '34190',  # FIXED LOAN UNDER NPGS-REVOVLING
        510: '34149',  # TABUNG USAHAWAN KECIL (TUK)
        511: '34190',  # FSMI2 SERVICING INTEREST
        515: '34149',  # FSMI2 INSTALLMENT
        555: '34149',  # SPECIAL FUND FOR TOURISM
        556: '34149',  # SPECIAL FUND FOR TOURISM
        559: '34149',  # NEW ENTREPRENEUR FUND
        560: '34149',  # NEW ENTREPRENEUR FUND
        564: '34149',  # FUND FOR FOOD
        565: '34149',  # FUND FOR FOOD
        569: '34114',  # BRIDGING LOAN - FSLMH
        570: '34149',  # FUND FOR SMI (FSMI)
        571: '34149',  # FUND FOR SMI (FSMI)
        574: '34149',  # REHABILITATION FUND FOR SMI (RFSMI)
        575: '34149',  # REHABILITATION FUND FOR SMI (RFSMI)
        700: '34111',  # HIRE PURCHASE
        705: '34111',  # HIRE PURCHASE
        750: '34111',  # HIRE PURCHASE
        709: '54124',  # HIRE PURCHASE SOLD TO CAGAMAS
        710: '54124',  # HIRE PURCHASE SOLD TO CAGAMAS
        752: '34111',  # HIRE PURCHASE
        760: '34111',  # HIRE PURCHASE
        770: '34113',  # BLOCK DISCOUNTING
        775: '34113',  # BLOCK DISCOUNTING
        799: '34112',  # LEASING
        900: '34149',  # CL - FIXED LOAN
        901: '34149',  # CL - FIXED LOAN
        902: '34149',  # CL - FIXED LOAN
        910: '54129',  # CL - FIXED LOAN SOLD TO CAGAMAS
        911: '34190',  # CL - REVOLVING CREDIT
        913: '34190',  # CL - REVOLVING CREDIT
        912: '34149',  # CL - SHARE LOAN TERM
        920: '34114',  # CL - BRIDGING LOAN
        930: '34149',  # CL - UNSECURED BUSINESS LOAN
        932: '34116',  # CL - FACTORING
        933: '34116',  # CL - FACTORING
        950: '34115',  # CL - SYNDICATED FIXED LOAN
        952: '34115',  # CL - SYNDICATED FIXED LOAN
        951: '34190',  # CL - SYNDICATED REVOLVING CREDIT
        953: '34190',  # CL - SYNDICATED REVOLVING CREDIT
        981: 'N',  # ABBA HOUSE FINANCING WRITTEN OFF
        982: 'N',  # ABBA FIXED LOANS WRITTEN OFF
        983: 'N',  # AITAB FINANCING WRITTEN OFF
        984: 'M',  # ABBA HOUSE FINANCING WRITTEN DOWN
        985: 'M',  # ABBA FIXED LOANS WRITTEN DOWN
        991: 'N',  # HOUSING LOANS WRITTEN OFF
        992: 'N',  # FIXED LOANS WRITTEN OFF
        993: 'N',  # HIRE PUCHASE WRITTEN OFF
        994: 'M',  # HOUSING LOANS WRITTEN DOWN
        995: 'M',  # FIXED LOANS WRITTEN DOWN
        996: 'M',  # HIRE PURCHASE WRITTEN DOWN
    }

    return product_map.get(loantype, '34149')


def format_cmprod(loantype):
    """
    Commitment product mapping
    Returns: Product code or blank
    """
    if loantype in [100, 101, 120, 150, 170, 180]:
        return '34190'  # DEFAULT AS 34190
    else:
        return ' '


def format_lnrate(loantype):
    """
    Interest rate classification
    Returns: Rate type code
    """
    # FIXED RATE - HOUSE LOAN
    if loantype in [113, 116, 118, 100, 101, 102, 200, 201, 237, 239]:
        return '30591'

    # FIXED RATE - HP
    elif loantype in [110, 115, 700, 705, 709, 710, 750, 752, 760]:
        return '30592'

    # FIXED RATE - OTHERS
    elif loantype in [105, 106, 120, 121, 170, 127, 510, 511, 515, 555, 556,
                      559, 560, 564, 565, 569, 570, 571, 574, 575, 770, 775, 799]:
        return '30593'

    # FLOATING RATE - BLR
    elif loantype in [112, 114, 117, 126, 227, 228, 230, 231, 234, 235, 236, 238,
                      240, 241, 242, 204, 205, 206, 300, 304, 359, 305, 307, 310,
                      315, 320, 325, 330, 504, 505, 506, 509, 900, 901, 902, 910,
                      920, 911, 912, 913, 930, 932, 950, 951, 952, 953]:
        return '30595'

    # FLOATING RATE - OTHER
    elif loantype in [5, 6, 10, 15, 20]:
        return '30597'

    else:
        return '30595'


def format_lntype(loantype):
    """
    Loan category classification
    Returns: 'HP', 'BL', 'OL', or blank
    """
    # HP ACCOUNTS
    if loantype in [15, 20, 110, 115, 700, 705, 709, 710, 750, 752, 760, 770, 775, 799]:
        return 'HP'

    # BULLET REPY. LOAN
    elif loantype in [200, 204, 300, 315, 320, 325, 330, 504, 509, 511, 555, 559,
                      564, 570, 574, 900, 911, 913, 932, 933, 951, 953]:
        return 'BL'

    # OTHER LOANS
    elif loantype in [5, 6, 10, 100, 101, 102, 105, 170, 201, 205, 206, 210, 304,
                      305, 307, 310, 505, 506, 510, 515, 556, 569, 560, 565, 571,
                      575, 901, 902, 910, 912, 920, 930, 950, 952]:
        return 'OL'

    else:
        return ' '


def format_prdfmt(loantype):
    """
    Product format classification
    Returns: 'FL', 'HL', 'SL', 'RC', 'FS', or blank
    """
    # FIXED LOANS (CONV)
    if loantype in [300, 304, 305, 307, 310, 504, 505, 506, 555, 556, 559, 510,
                    511, 515, 569, 570, 571, 574, 575, 560, 564, 565, 799, 900,
                    901, 902, 910, 912, 359, 920, 930, 932, 933, 950, 952]:
        return 'FL'

    # FIXED LOANS (ABBA)
    elif loantype in [126, 105, 106, 120, 121, 170, 127]:
        return 'FL'

    # HP DIRECT
    elif loantype in [700, 705, 709, 710]:
        return 'FL'

    # HP AGENCY
    elif loantype in [750, 752, 760]:
        return 'FL'

    # HP BLOCK DISCOUNTING
    elif loantype in [770, 775]:
        return 'FL'

    # HP (AITAB)
    elif loantype in [110, 115]:
        return 'FL'

    # FLOOR STOCKING
    elif loantype == 325:
        return 'FS'

    # HOUSING LOANS (ABBA)
    elif loantype in [112, 113, 114, 116, 117, 118, 100, 101, 102]:
        return 'HL'

    # HOUSING LOANS (CONV)
    elif loantype in [227, 228, 230, 231, 234, 200, 201, 204, 205, 206, 210]:
        return 'HL'

    # STAFF HOUSING LOANS
    elif loantype == 5:
        return 'SL'

    # STAFF CAR LOANS
    elif loantype in [15, 20]:
        return 'SL'

    # STAFF SPECIAL LOAN SCHEME
    elif loantype == 10:
        return 'SL'

    # REVOLVING CREDIT
    elif loantype in [315, 320, 330, 509, 911, 913, 951, 953]:
        return 'RC'

    else:
        return ' '


def format_locustcd(custcd):
    """
    Location customer code mapping (compressed format)
    Returns: Customer type code
    """
    custcd_map = {
        1: '11',  # BANK NEGARA MALAYSIA
        2: '02',  # COMMERCIAL BANKS
        3: '03',  # ISLAMIC BANKS
        4: '04',  # SUBSIDIARY STOCKBROKING COMPANIES
        5: '05',  # ASSOCIATE STOCKBROKING COMPANIES
        6: '06',  # OTHER STOCKBROKING COMPANIES
        10: '11',  # DOMESTIC BANKING INSTITUTIONS
        11: '11',  # FINANCE COMPANIES
        12: '12',  # MERCHANT BANKS
        13: '13',  # DISCOUNT HOUSES
        15: '79',  # DOMESTIC NON-BANK ENTITIES
        17: '17',  # CAGAMAS BERHAD
        20: '30',  # DOMESTIC NON-BANK FINANCIAL INSTITUTIONS
        30: '30',  # DOMESTIC OTHER NBFI
        31: '31',  # SAVINGS INSTITUTIONS
        32: '32',  # CREDIT CARD COMPANIES
        33: '33',  # DEVELOPMENT FINANCE INSTITUTIONS
        34: '34',  # BUILDING SOCIETIES
        35: '35',  # CO-OPERATIVE SOCIETIES
        36: '06',  # STOCKBROKING COMPANIES
        37: '37',  # COMMODITY BROKERS
        38: '38',  # CREDIT & LEASING COMPANIES
        39: '39',  # UNIT TRUST COMPANIES
        40: '40',  # INSURANCE AND INSURANCE RELATED COMPANIES
        46: '46',  # DANAHARTA  (TO BE UNDER '45')
        47: '47',  # DANAMODAL  (TO BE UNDER '45')
        50: '79',  # RESIDENTS/DOMESTIC ENTITIES
        57: '57',  # PETRONAS
        59: '59',  # OTHER GOVERNMENT CONTROLLED DBE NIE
        60: '62',  # DOMESTIC BUSINESS ENTERPRISES (DBE)
        61: '61',  # BUMIPUTRA CONTROLLED DBE
        62: '62',  # NON-BUMIPUTRA CONTROLLED DBE
        63: '63',  # NON-RESIDENT CONTROLLED DBE
        64: '64',  # GOVERNMENT CONTROLLED DBE
        65: '67',  # SMALL MEDIUM INDUSTRIES (SMI)
        66: '66',  # BUMIPUTRA CONTROLLED SMI
        67: '67',  # NON-BUMIPUTRA CONTROLLED SMI
        68: '68',  # NON-RESIDENT CONTROLLED SMI
        69: '69',  # GOVERNMENT CONTROLLED SMI
        70: '71',  # GOVERNMENT
        71: '71',  # FEDERAL GOVERNMENT
        72: '72',  # STATE GOVERNMENT
        73: '73',  # LOCAL GOVERNMENT
        74: '74',  # STATUTORY AUTHORITIES
        75: '75',  # NFPE
        76: '78',  # INDIVIDUALS
        77: '77',  # BUMIPUTRA
        78: '78',  # NON-BUMIPUTRA
        79: '79',  # DOMESTIC OTHER ENTITIES NIE
        80: '86',  # NON-RESIDENTS/FOREIGN ENTITIES
        81: '81',  # FOREIGN BANKING INSTITUTIONS
        82: '82',  # AFFILIATES ABROAD
        83: '83',  # G7 COUNTRIES
        84: '84',  # FOREIGN BANKS IN OTHER COUNTRIES
        85: '86',  # FOREIGN NON-BANK ENTITIES
        86: '86',  # FOREIGN BUSINESS ENTERPRISES
        90: '90',  # FOREIGN GOVERNMENTS
        91: '91',  # FOREIGN CENTRAL BANKS
        92: '92',  # FRGN DIPLOMATIC REPRESENTATION IN M'SIA
        95: '95',  # FOREIGN INDIVIDUALS
        96: '96',  # FOREIGNERS EMPLOYED/STUDYING IN M'SIA
        98: '98',  # FOREIGN NON-COMMERCIAL INTERNATIONAL ORGANIZATION IN M'SIA
        99: '99',  # FOREIGN OTHER ENTITIES NIE
    }

    return custcd_map.get(custcd, '79')


def format_lncustcd(custcd):
    """
    Loan customer code mapping (expanded format)
    Returns: Customer type code
    """
    custcd_map = {
        1: '01',  # BANK NEGARA MALAYSIA
        2: '02',  # COMMERCIAL BANKS
        3: '03',  # ISLAMIC BANKS
        4: '04',  # SUBSIDIARY STOCKBROKING COMPANIES
        5: '05',  # ASSOCIATE STOCKBROKING COMPANIES
        6: '06',  # OTHER STOCKBROKING COMPANIES
        10: '10',  # DOMESTIC BANKING INSTITUTIONS
        11: '11',  # FINANCE COMPANIES
        12: '12',  # MERCHANT BANKS
        13: '13',  # DISCOUNT HOUSES
        15: '15',  # DOMESTIC NON-BANK ENTITIES
        17: '17',  # CAGAMAS BERHAD
        20: '20',  # DOMESTIC NON-BANK FINANCIAL INSTITUTIONS
        30: '30',  # DOMESTIC OTHER NBFI
        32: '32',  # CREDIT CARD COMPANIES
        33: '33',  # DEVELOPMENT FINANCE INSTITUTIONS
        34: '34',  # BUILDING SOCIETIES
        35: '35',  # CO-OPERATIVE SOCIETIES
        36: '36',  # STOCKBROKING COMPANIES
        37: '37',  # COMMODITY BROKERS
        38: '38',  # CREDIT & LEASING COMPANIES
        39: '39',  # UNIT TRUST COMPANIES
        40: '40',  # INSURANCE AND INSURANCE RELATED COMPANIES
        50: '50',  # RESIDENTS/DOMESTIC ENTITIES
        57: '57',  # PETRONAS
        59: '59',  # OTHER GOVERNMENT CONTROLLED DBE NIE
        60: '60',  # DOMESTIC BUSINESS ENTERPRISES (DBE)
        61: '61',  # BUMIPUTRA CONTROLLED DBE
        62: '62',  # NON-BUMIPUTRA CONTROLLED DBE
        63: '63',  # NON-RESIDENT CONTROLLED DBE
        64: '64',  # GOVERNMENT CONTROLLED DBE
        65: '65',  # SMALL MEDIUM INDUSTRIES (SMI)
        66: '66',  # BUMIPUTRA CONTROLLED SMI
        67: '67',  # NON-BUMIPUTRA CONTROLLED SMI
        68: '68',  # NON-RESIDENT CONTROLLED SMI
        69: '69',  # GOVERNMENT CONTROLLED SMI
        70: '70',  # GOVERNMENT
        71: '71',  # FEDERAL GOVERNMENT
        72: '72',  # STATE GOVERNMENT
        73: '73',  # LOCAL GOVERNMENT
        74: '74',  # STATUTORY AUTHORITIES
        75: '75',  # NFPE
        76: '76',  # INDIVIDUALS
        77: '77',  # BUMIPUTRA
        78: '78',  # NON-BUMIPUTRA
        79: '79',  # DOMESTIC OTHER ENTITIES NIE
        80: '80',  # NON-RESIDENTS/FOREIGN ENTITIES
        81: '81',  # FOREIGN BANKING INSTITUTIONS
        85: '85',  # FOREIGN NON-BANK ENTITIES
        86: '86',  # FOREIGN BUSINESS ENTERPRISES
        90: '90',  # FOREIGN GOVERNMENTS
        91: '91',  # FOREIGN CENTRAL BANKS
        92: '92',  # FRGN DIPLOMATIC REPRESENTATION IN M'SIA
        95: '95',  # FOREIGN INDIVIDUALS
        96: '96',  # FOREIGNERS EMPLOYED/STUDYING IN M'SIA
        98: '98',  # FOREIGN NON-COMMERCIAL INTERNATIONAL ORGANIZATION IN M'SIA
        99: '99',  # FOREIGN OTHER ENTITIES NIE
    }

    return custcd_map.get(custcd, '79')


def format_riskcd(risk_str):
    """
    Risk code mapping
    Returns: Risk classification code
    """
    normalized = str(risk_str).strip()

    if normalized in ['2', '02', '002', '0002']:
        return '34902'  # SUBSTANDARD
    elif normalized in ['3', '03', '003', '0003']:
        return '34903'  # DOUBTFUL
    elif normalized in ['4', '04', '004', '0004']:
        return '34904'  # BAD
    else:
        return ' '


def format_lnormt(months):
    """
    Loan original maturity classification
    Returns: Maturity code
    """
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


def format_lnrmmt(months):
    """
    Loan remaining maturity classification
    Returns: Remaining maturity code
    """
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


def format_statecd(state_str):
    """
    State code mapping
    Returns: State code (single letter)
    """
    normalized = str(state_str).strip().lstrip('0')

    state_map = {
        '1': 'J',  # Johor
        '2': 'K',  # Kedah
        '3': 'D',  # Kelantan
        '4': 'M',  # Melaka
        '5': 'N',  # Negeri Sembilan
        '6': 'C',  # Pahang
        '7': 'P',  # Pulau Pinang
        '8': 'A',  # Perak
        '9': 'R',  # Perlis
        '10': 'S',  # Selangor
        '11': 'Q',  # Terengganu
        '12': 'B',  # Kuala Lumpur/Federal Territory
        '13': 'T',  # Sabah
        '14': 'W',  # Sarawak
        '15': 'L',  # Labuan
    }

    return state_map.get(normalized, 'B')


def format_apprlimt(amount):
    """
    Approved limit classification
    Returns: Limit range code
    """
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


def format_loansize(amount):
    """
    Loan size classification
    Returns: Size range code
    """
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


def format_collcd(coll_code):
    """
    Collateral code mapping
    Returns: Collateral type code
    """
    coll_str = str(coll_code).upper().strip()

    # Specific mappings
    if coll_str == '99':
        return '30570'  # OTHER SECURITIES
    elif coll_str == 'B1':
        return '30570'  # BONDS
    elif coll_str == 'B2':
        return '30540'  # UNIT TRUST
    elif coll_str in ['B3', 'B4']:
        return '30570'  # SAVINGS ACCOUNT & FD RECEIPTS
    elif coll_str.startswith('C') and coll_str in ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']:
        return '30560'  # VACANT LAND (various types)
    elif coll_str.startswith('D'):
        return '30560'  # RESIDENTIAL/NON-RESIDENTIAL PROPERTY
    elif coll_str.startswith('F'):
        return '30570'  # GUARANTEE (various types)
    elif coll_str.startswith('G'):
        return '30570'  # BLOCK DISCOUNTING
    elif coll_str.startswith('H'):
        return '30570'  # VEHICLES/CONSUMER DURABLES
    elif coll_str.startswith('J'):
        if coll_str == 'J1':
            return '30520'  # QUOTED SHARES
        elif coll_str == 'J2':
            return '30530'  # UNQUOTED SHARES
        else:
            return '30570'  # OTHER MARKETABLE SECURITIES
    elif coll_str.startswith('L') or coll_str.startswith('M'):
        return '30580'  # UNSECURED
    elif coll_str.startswith('A'):
        return '30570'  # ASSIGNMENT
    elif coll_str.startswith('E'):
        return '30570'  # DEBENTURE
    elif coll_str == 'K1':
        return '30570'  # POWER OF ATTORNEY
    else:
        return '30570'


def format_arrclass(arrears_months):
    """
    Arrears classification
    Returns: Arrears class description
    """
    if arrears_months < 1:
        return '0 - < 1 MTH   '
    elif arrears_months < 2:
        return '1 - < 2 MTH   '
    elif arrears_months < 3:
        return '2 - < 3 MTH   '
    elif arrears_months < 4:
        return '3 - < 4 MTH   '
    elif arrears_months < 5:
        return '4 - < 5 MTH   '
    elif arrears_months < 6:
        return '5 - < 6 MTH   '
    elif arrears_months < 7:
        return '6 - < 7 MTH   '
    elif arrears_months < 8:
        return '7 - < 8 MTH   '
    elif arrears_months < 9:
        return '8 - < 9 MTH   '
    elif arrears_months < 12:
        return '9 - < 12 MTH  '
    elif arrears_months < 18:
        return '12 - < 18 MTH '
    elif arrears_months < 24:
        return '18 - < 24 MTH '
    elif arrears_months < 36:
        return '24 - < 36 MTH '
    elif arrears_months < 999:
        return '36 MTH & ABOVE'
    else:
        return 'DEFICIT       '


# Export all format functions
__all__ = [
    'format_lndenom',
    'format_lnprod',
    'format_cmprod',
    'format_lnrate',
    'format_lntype',
    'format_prdfmt',
    'format_locustcd',
    'format_lncustcd',
    'format_riskcd',
    'format_lnormt',
    'format_lnrmmt',
    'format_statecd',
    'format_apprlimt',
    'format_loansize',
    'format_collcd',
    'format_arrclass',
    'HP_PRODUCTS',
    'HPD_PRODUCTS',
    'HOMEIS_PRODUCTS',
    'HOMECV_PRODUCTS',
    'MOREIS_PRODUCTS',
    'MORECV_PRODUCTS',
    'SWIFTIS_PRODUCTS',
    'SWIFTCV_PRODUCTS',
]
