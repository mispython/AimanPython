# !/usr/bin/env python3
"""
Program : BRANCHCD
Purpose : Python equivalent of SAS BRANCHCD PROC FORMAT definitions.
          Provides two numeric-to-string format mappings:
            BRCHCD : branch number (1-168, 2000-4000, 5000-9999)
                     -> 'XXX/NNN' branch code string
            IBU    : IBU branch number (2001-2168, 4000, 5000-9999)
                     -> '#### XXX' head-office branch label

          The BRCHCD format covers:
            1-168   -> 'KLM/001' .. 'STW/168'  (individual branch codes)
            2000-4000 -> 'IBU'                  (range: internal branch units)
            5000-9999 -> 'HOA'                  (range: head-office accounts)

          The IBU format covers:
            2001-2168 -> '2001 KLM' .. '2168 STW'
            4000      -> '4000 IBU'
            5000-9999 -> '>5000 HOA'

          Usage:
              from X_BRANCHCD import format_brchcd, format_ibu
"""

from typing import Optional

# =============================================================================
# FORMAT: BRCHCD
# VALUE BRCHCD
#   1='KLM/001' .. 168='STW/168'
#   2000-4000='IBU'
#   5000-9999='HOA'
# =============================================================================

_BRCHCD_MAP: dict[int, str] = {
    1:   'KLM/001',   # KLM
    2:   'PMO/002',   # PMO
    3:   'MLK/003',   # MLK
    4:   'IMO/004',   # IMO
    5:   'TPG/005',   # TPG
    6:   'JBU/006',   # JBU
    7:   'JPU/007',   # JPU
    8:   'JSI/008',   # JSI
    9:   'KLC/009',   # KLC
    10:  'JBA/010',   # JBA
    11:  'SST/011',   # SST
    12:  'BTW/012',   # BTW
    13:  'KKU/013',   # KKU
    14:  'SRB/014',   # SRB
    15:  'JPR/015',   # JPR
    16:  'ASR/016',   # ASR
    17:  'KLG/017',   # KLG
    18:  'KPR/018',   # KPR
    19:  'BGH/019',   # BGH
    20:  'KLS/020',   # KLS
    21:  'KPH/021',   # KPH
    22:  'TRG/022',   # TRG
    23:  'SGM/023',   # SGM
    24:  'SMG/024',   # SMG
    25:  'PKG/025',   # PKG
    26:  'MUA/026',   # MUA
    27:  'KTN/027',   # KTN
    28:  'STP/028',   # STP
    29:  'KCG/029',   # KCG
    30:  'SKC/030',   # SKC
    31:  'LBN/031',   # LBN
    32:  'MSG/032',   # MSG
    33:  'KBH/033',   # KBH
    34:  'PJN/034',   # PJN
    35:  'RWG/035',   # RWG
    36:  'KPG/036',   # KPG
    37:  'TMH/037',   # TMH
    38:  'PDN/038',   # PDN
    39:  'KDN/039',   # KDN
    40:  'DJA/040',   # DJA
    41:  'APG/041',   # APG
    42:  'TWU/042',   # TWU
    43:  'SJA/043',   # SJA
    44:  'MTK/044',   # MTK
    45:  'BMM/045',   # BMM
    46:  'TIN/046',   # TIN
    47:  'PKL/047',   # PKL
    48:  'KGR/048',   # KGR
    49:  'KJG/049',   # KJG
    50:  'KTU/050',   # KTU
    51:  'JKL/051',   # JKL
    52:  'BPR/052',   # BPR
    53:  'EDU/053',   # EDU
    54:  'KHG/054',   # KHG
    55:  'TKK/055',   # TKK
    56:  'SPI/056',   # SPI
    57:  'BBU/057',   # BBU
    58:  'SKN/058',   # SKN
    59:  'CKI/059',   # CKI
    60:  'TPN/060',   # TPN
    61:  'SBU/061',   # SBU
    62:  'BDR/062',   # BDR
    63:  'AIM/063',   # AIM
    64:  'YPG/064',   # YPG
    65:  'KKI/065',   # KKI
    66:  'BTG/066',   # BTG
    67:  'SDN/067',   # SDN
    68:  'MRI/068',   # MRI
    69:  'KKB/069',   # KKB
    70:  'KTI/070',   # KTI
    71:  'KLI/071',   # KLI
    72:  'JTA/072',   # JTA
    73:  'SRM/073',   # SRM
    74:  'SRR/074',   # SRR
    75:  'KKR/075',   # KKR
    76:  'PJA/076',   # PJA
    77:  'BEN/077',   # BEN
    78:  'LDU/078',   # LDU
    79:  'BPT/079',   # BPT
    80:  'KAP/080',   # KAP
    81:  'PRI/081',   # PRI
    82:  'GMG/082',   # GMG
    83:  'TST/083',   # TST
    84:  'PLH/084',   # PLH
    85:  'SBP/085',   # SBP
    86:  'KLA/086',   # KLA
    87:  'TDI/087',   # TDI
    88:  'CAH/088',   # CAH
    89:  'JSN/089',   # JSN
    90:  'RAU/090',   # RAU
    91:  'SBR/091',   # SBR
    92:  'LLG/092',   # LLG
    93:  'JLP/093',   # JLP
    94:  'TSP/094',   # TSP
    95:  'BSI/095',   # BSI
    96:  'JMH/096',   # JMH
    97:  'PBR/097',   # PBR
    98:  'MLB/098',   # MLB
    99:  'BTL/099',   # BTL
    100: 'SAM/100',   # SAM
    101: 'KBU/101',   # KBU
    102: 'KBS/102',   # KBS
    103: 'SAN/103',   # SAN
    104: 'SJY/104',   # SJY
    105: 'JJG/105',   # JJG
    106: 'TML/106',   # TML
    107: 'JIH/107',   # JIH
    108: 'BBB/108',   # BBB
    109: 'INN/109',   # INN
    110: 'SBM/110',   # SBM
    111: 'SEA/111',   # SEA
    112: 'PJO/112',   # PJO
    113: 'IGN/113',   # IGN
    114: 'BBI/114',   # BBI
    115: 'WMU/115',   # WMU
    116: 'GRT/116',   # GRT
    117: 'SGB/117',   # SGB
    118: 'UYB/118',   # UYB
    119: 'KMY/119',   # KMY
    120: 'BTA/120',   # BTA
    121: 'KRK/121',   # KRK
    122: 'TRJ/122',   # TRJ
    123: 'SLY/123',   # SLY
    124: 'NLI/124',   # NLI
    125: 'SSA/125',   # SSA
    126: 'SPG/126',   # SPG
    127: 'SBH/127',   # SBH
    128: 'SSH/128',   # SSH
    129: 'TIH/129',   # TIH
    130: 'RTU/130',   # RTU
    131: 'TJJ/131',   # TJJ
    132: 'PCG/132',   # PCG
    133: 'SNI/133',   # SNI
    134: 'JRT/134',   # JRT
    135: 'SGK/135',   # SGK
    136: 'STL/136',   # STL
    137: 'JTS/137',   # JTS
    138: 'JKA/138',   # JKA
    139: 'JDK/139',   # JDK
    140: 'SAB/140',   # SAB
    141: 'SDI/141',   # SDI
    142: 'TPI/142',   # TPI
    143: 'TCT/143',   # TCT
    144: 'NTL/144',   # NTL
    145: 'MSI/145',   # MSI
    146: 'CCE/146',   # CCE
    147: 'KJA/147',   # KJA
    148: 'JLT/148',   # JLT
    149: 'BDA/149',   # BDA
    150: 'BBM/150',   # BBM
    151: 'AST/151',   # AST
    152: 'TMA/152',   # TMA
    153: 'USJ/153',   # USJ
    154: 'TMI/154',   # TMI
    155: 'TMK/155',   # TMK
    156: 'DUA/156',   # DUA
    157: 'JSB/157',   # JSB
    158: 'PIH/158',   # PIH
    159: 'SS2/159',   # SS2
    160: 'TSJ/160',   # TSJ
    161: 'TCL/161',   # TCL
    162: 'TEA/162',   # TEA
    163: 'SFN/163',   # SFN - SECTION 14
    164: 'JTT/164',   # JTT - JALAN TANJUNG TOKONG
    165: 'JBH/165',   # JBH - JALAN BESERAH
    166: 'BAM/166',   # BAM - BAGAN AJAM
    167: 'JPA/167',   # JPA - JALAN PUTERA
    168: 'STW/168',   # STW - SITIAWAN
}


def format_brchcd(branch: Optional[int]) -> str:
    """
    Map numeric branch number to 'XXX/NNN' branch code string.

    Rules (matching SAS VALUE BRCHCD):
      1-168        -> individual code, e.g. 'KLM/001'
      2000-4000    -> 'IBU'
      5000-9999    -> 'HOA'
      otherwise    -> ''
    """
    if branch is None:
        return ''
    try:
        b = int(branch)
    except (TypeError, ValueError):
        return ''
    if b in _BRCHCD_MAP:
        return _BRCHCD_MAP[b]
    if 2000 <= b <= 4000:
        return 'IBU'
    if 5000 <= b <= 9999:
        return 'HOA'
    return ''


# =============================================================================
# FORMAT: IBU
# VALUE IBU
#   2001='2001 KLM' .. 2168='2168 STW'
#   4000='4000 IBU'
#   5000-9999='>5000 HOA'
# =============================================================================

# IBU branch label map: individual entries 2001-2168
# Each entry is the 4-digit prefix + space + 3-letter abbreviation
_IBU_MAP: dict[int, str] = {
    2001:  '2001 KLM',   # KLM
    2002:  '2002 PMO',   # PMO
    2003:  '2003 MLK',   # MLK
    2004:  '2004 IMO',   # IMO
    2005:  '2005 TPG',   # TPG
    2006:  '2006 JBU',   # JBU
    2007:  '2007 JPU',   # JPU
    2008:  '2008 JSI',   # JSI
    2009:  '2009 KLC',   # KLC
    2010:  '2010 JBA',   # JBA
    2011:  '2011 SST',   # SST
    2012:  '2012 BTW',   # BTW
    2013:  '2013 KKU',   # KKU
    2014:  '2014 SRB',   # SRB
    2015:  '2015 JPR',   # JPR
    2016:  '2016 ASR',   # ASR
    2017:  '2017 KLG',   # KLG
    2018:  '2018 KPR',   # KPR
    2019:  '2019 BGH',   # BGH
    2020:  '2020 KLS',   # KLS
    2021:  '2021 KPH',   # KPH
    2022:  '2022 TRG',   # TRG
    2023:  '2023 SGM',   # SGM
    2024:  '2024 SMG',   # SMG
    2025:  '2025 PKG',   # PKG
    2026:  '2026 MUA',   # MUA
    2027:  '2027 KTN',   # KTN
    2028:  '2028 STP',   # STP
    2029:  '2029 KCG',   # KCG
    2030:  '2030 SKC',   # SKC
    2031:  '2031 LBN',   # LBN
    2032:  '2032 MSG',   # MSG
    2033:  '2033 KBH',   # KBH
    2034:  '2034 PJN',   # PJN
    2035:  '2035 RWG',   # RWG
    2036:  '2036 KPG',   # KPG
    2037:  '2037 TMH',   # TMH
    2038:  '2038 PDN',   # PDN
    2039:  '2039 KDN',   # KDN
    2040:  '2040 DJA',   # DJA
    2041:  '2041 APG',   # APG
    2042:  '2042 TWU',   # TWU
    2043:  '2043 SJA',   # SJA
    2044:  '2044 MTK',   # MTK
    2045:  '2045 BMM',   # BMM
    2046:  '2046 TIN',   # TIN
    2047:  '2047 PKL',   # PKL
    2048:  '2048 KGR',   # KGR
    2049:  '2049 KJG',   # KJG
    2050:  '2050 KTU',   # KTU
    2051:  '2051 JKL',   # JKL
    2052:  '2052 BPR',   # BPR
    2053:  '2053 EDU',   # EDU
    2054:  '2054 KHG',   # KHG
    2055:  '2055 TKK',   # TKK
    2056:  '2056 SPI',   # SPI
    2057:  '2057 BBU',   # BBU
    2058:  '2058 SKN',   # SKN
    2059:  '2059 CKI',   # CKI
    2060:  '2060 TPN',   # TPN
    2061:  '2061 SBU',   # SBU
    2062:  '2062 BDR',   # BDR
    2063:  '2063 AIM',   # AIM
    2064:  '2064 YPG',   # YPG
    2065:  '2065 KKI',   # KKI
    2066:  '2066 BTG',   # BTG
    2067:  '2067 SDN',   # SDN
    2068:  '2068 MRI',   # MRI
    2069:  '2069 KKB',   # KKB
    2070:  '2070 KTI',   # KTI
    2071:  '2071 KLI',   # KLI
    2072:  '2072 JTA',   # JTA
    2073:  '2073 SRM',   # SRM
    2074:  '2074 SRR',   # SRR
    2075:  '2075 KKR',   # KKR
    2076:  '2076 PJA',   # PJA
    2077:  '2077 BEN',   # BEN
    2078:  '2078 LDU',   # LDU
    2079:  '2079 BPT',   # BPT
    2080:  '2080 KAP',   # KAP
    2081:  '2081 PRI',   # PRI
    2082:  '2082 GMG',   # GMG
    2083:  '2083 TST',   # TST
    2084:  '2084 PLH',   # PLH
    2085:  '2085 SBP',   # SBP
    2086:  '2086 KLA',   # KLA
    2087:  '2087 TDI',   # TDI
    2088:  '2088 CAH',   # CAH
    2089:  '2089 JSN',   # JSN
    2090:  '2090 RAU',   # RAU
    2091:  '2091 SBR',   # SBR
    2092:  '2092 LLG',   # LLG
    2093:  '2093 JLP',   # JLP
    2094:  '2094 TSP',   # TSP
    2095:  '2095 BSI',   # BSI
    2096:  '2096 JMH',   # JMH
    2097:  '2097 PBR',   # PBR
    2098:  '2098 MLB',   # MLB
    2099:  '2099 BTL',   # BTL
    2100:  '2100 SAM',   # SAM
    2101:  '2101 KBU',   # KBU
    2102:  '2102 KBS',   # KBS
    2103:  '2103 SAN',   # SAN
    2104:  '2104 SJY',   # SJY
    2105:  '2105 JJG',   # JJG
    2106:  '2106 TML',   # TML
    2107:  '2107 JIH',   # JIH
    2108:  '2108 BBB',   # BBB
    2109:  '2109 INN',   # INN
    2110:  '2110 SBM',   # SBM
    2111:  '2111 SEA',   # SEA
    2112:  '2112 PJO',   # PJO
    2113:  '2113 IGN',   # IGN
    2114:  '2114 BBI',   # BBI
    2115:  '2115 WMU',   # WMU
    2116:  '2116 GRT',   # GRT
    2117:  '2117 SGB',   # SGB
    2118:  '2118 UYB',   # UYB
    2119:  '2119 KMY',   # KMY
    2120:  '2120 BTA',   # BTA
    2121:  '2121 KRK',   # KRK
    2122:  '2122 TRJ',   # TRJ
    2123:  '2123 SLY',   # SLY
    2124:  '2124 NLI',   # NLI
    2125:  '2125 SSA',   # SSA
    2126:  '2126 SPG',   # SPG
    2127:  '2127 SBH',   # SBH
    2128:  '2128 SSH',   # SSH
    2129:  '2129 TIH',   # TIH
    2130:  '2130 RTU',   # RTU
    2131:  '2131 TJJ',   # TJJ
    2132:  '2132 PCG',   # PCG
    2133:  '2133 SNI',   # SNI
    2134:  '2134 JRT',   # JRT
    2135:  '2135 SGK',   # SGK
    2136:  '2136 STL',   # STL
    2137:  '2137 JTS',   # JTS
    2138:  '2138 JKA',   # JKA
    2139:  '2139 JDK',   # JDK
    2140:  '2140 SAB',   # SAB
    2141:  '2141 SDI',   # SDI
    2142:  '2142 TPI',   # TPI
    2143:  '2143 TCT',   # TCT
    2144:  '2144 NTL',   # NTL
    2145:  '2145 MSI',   # MSI
    2146:  '2146 CCE',   # CCE
    2147:  '2147 KJA',   # KJA
    2148:  '2148 JLT',   # JLT
    2149:  '2149 BDA',   # BDA
    2150:  '2150 BBM',   # BBM
    2151:  '2151 AST',   # AST
    2152:  '2152 TMA',   # TMA
    2153:  '2153 USJ',   # USJ
    2154:  '2154 TMI',   # TMI
    2155:  '2155 TMK',   # TMK
    2156:  '2156 DUA',   # DUA
    2157:  '2157 JSB',   # JSB
    2158:  '2158 PIH',   # PIH
    2159:  '2159 SS2',   # SS2
    2160:  '2160 TSJ',   # TSJ
    2161:  '2161 TCL',   # TCL
    2162:  '2162 TEA',   # TEA
    2163:  '2163 SFN',   # SFN
    2164:  '2164 JTT',   # JALAN TANJUNG TOKONG
    2165:  '2165 JBH',   # JALAN BESERAH
    2166:  '2166 BAM',   # BAGAN AJAM
    2167:  '2167 JPA',   # JALAN PUTERA
    2168:  '2168 STW',   # SITIAWAN
    4000:  '4000 IBU',
}


def format_ibu(branch: Optional[int]) -> str:
    """
    Map numeric IBU branch number to its label string.

    Rules (matching SAS VALUE IBU):
      2001-2168    -> '#### XXX' individual IBU label
      4000         -> '4000 IBU'
      5000-9999    -> '>5000 HOA'
      otherwise    -> ''
    """
    if branch is None:
        return ''
    try:
        b = int(branch)
    except (TypeError, ValueError):
        return ''
    if b in _IBU_MAP:
        return _IBU_MAP[b]
    if 5000 <= b <= 9999:
        return '>5000 HOA'
    return ''


# =============================================================================
# PUBLIC API
# =============================================================================

__all__ = [
    'format_brchcd',
    'format_ibu',
    '_BRCHCD_MAP',
    '_IBU_MAP',
]


# =============================================================================
# SELF-TEST
# =============================================================================
if __name__ == '__main__':
    tests = [
        (format_brchcd,  1,    'KLM/001'),
        (format_brchcd,  10,   'JBA/010'),
        (format_brchcd,  100,  'SAM/100'),
        (format_brchcd,  163,  'SFN/163'),
        (format_brchcd,  168,  'STW/168'),
        (format_brchcd,  2000, 'IBU'),
        (format_brchcd,  3500, 'IBU'),
        (format_brchcd,  4000, 'IBU'),
        (format_brchcd,  5000, 'HOA'),
        (format_brchcd,  9999, 'HOA'),
        (format_brchcd,  169,  ''),      # not defined
        (format_ibu,     2001, '2001 KLM'),
        (format_ibu,     2100, '2100 SAM'),
        (format_ibu,     2168, '2168 STW'),
        (format_ibu,     4000, '4000 IBU'),
        (format_ibu,     5000, '>5000 HOA'),
        (format_ibu,     9999, '>5000 HOA'),
        (format_ibu,     1,    ''),      # not in IBU range
    ]
    all_ok = True
    for fn, val, expected in tests:
        result = fn(val)
        status = 'OK' if result == expected else f'FAIL (got {result!r})'
        if status != 'OK':
            all_ok = False
        print(f"  {fn.__name__}({val!r:>5}) = {result!r:>12}  {status}")
    print(f"\n{'All tests passed.' if all_ok else 'SOME TESTS FAILED.'}")
