# !/usr/bin/env python3
"""
Program : DIBMIS02
Date    : 09.08.02
Purpose : Report on Al-Mudharabah Accounts (Host version of MIS program DIBMIS02).
          Generates TODATE BALANCE and DAILY AVERAGE tabulate reports
            by branch and FD maturity month (MTH) for Islamic Mudharabah FD accounts.
          Runs only on 22nd and month-end dates (REPTFQ='Y').
"""

# Dependency: PBMISFMT.py (format functions used for branch code mapping)
# from PBMISFMT import format_brchcd  # placeholder - see PBMISFMT.py

import os
import duckdb
import polars as pl
from datetime import date, timedelta
import calendar

# ===========================================================================
# PATH CONFIGURATION
# ===========================================================================
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR      = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR     = os.path.join(BASE_DIR, 'output')

REPTDATE_PARQUET      = os.path.join(INPUT_DIR, 'deposit_reptdate.parquet')
MIS_DYIBUF_PARQUET_TMPL = os.path.join(INPUT_DIR, 'mis_dyibuf{reptmon}.parquet')

OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'DIBMIS02.txt')

PAGE_LENGTH = 60
LINE_SIZE   = 132

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===========================================================================
# FORMAT: BRCHCD  (numeric branch -> 3-letter code)
# (Dependency: PBMISFMT)
# ===========================================================================
_BRCHCD_MAP: dict[int, str] = {
    1:'HOE',2:'JSS',3:'JRC',4:'MLK',5:'IMO',6:'PPG',7:'JBU',8:'KTN',9:'JYK',10:'ASR',
    11:'GRN',12:'PPH',13:'KBU',14:'TMH',15:'KPG',16:'NLI',17:'TPN',18:'PJN',19:'DUA',20:'TCL',
    21:'BPT',22:'SMY',23:'KMT',24:'RSH',25:'SAM',26:'SPG',27:'NTL',28:'MUA',29:'JRL',30:'KTU',
    31:'SKC',32:'WSS',33:'KKU',34:'KGR',35:'SSA',36:'SS2',37:'TSA',38:'JKL',39:'KKG',40:'JSB',
    41:'JIH',42:'BMM',43:'BTG',44:'TWU',45:'SRB',46:'APG',47:'SGM',48:'MTK',49:'JLP',50:'MRI',
    51:'SMG',52:'UTM',53:'TMI',54:'BBB',55:'LBN',56:'KJG',57:'SPI',58:'SBU',59:'PKL',60:'BAM',
    61:'KLI',62:'SDK',63:'GMS',64:'PDN',65:'BHU',66:'BDA',67:'CMR',68:'SAT',69:'BKI',70:'PSA',
    71:'BCG',72:'PPR',73:'SPK',74:'SIK',75:'CAH',76:'PRS',77:'PLI',78:'SJA',79:'MSI',80:'MLB',
    81:'SBH',82:'MCG',83:'JBB',84:'PMS',85:'SST',86:'CLN',87:'MSG',88:'KUM',89:'TPI',90:'BTL',
    91:'KUG',92:'KLG',93:'EDU',94:'STP',95:'TIN',96:'SGK',97:'HSL',98:'TCY',
    102:'PRJ',103:'JJG',104:'KKL',105:'KTI',106:'CKI',107:'JLT',108:'BSI',109:'KSR',110:'TJJ',
    111:'AKH',112:'LDO',113:'TML',114:'BBA',115:'KNG',116:'TRI',117:'KKI',118:'TMW',119:'BNV',
    120:'PIH',121:'PRA',122:'SKN',123:'IGN',124:'S14',125:'KJA',126:'PTS',127:'TSM',128:'SGB',
    129:'BSR',130:'PDG',131:'TMG',132:'CKT',133:'PKG',134:'RPG',135:'BSY',136:'TCS',137:'JPP',
    138:'WMU',139:'JRT',140:'CPE',141:'STL',142:'KBD',143:'LDU',144:'KHG',145:'BSD',146:'PSG',
    147:'PNS',148:'PJO',149:'BFT',150:'LMM',151:'SLY',152:'ATR',153:'USJ',154:'BSJ',155:'TTJ',
    156:'TMR',157:'BPJ',158:'SPL',159:'RLU',160:'MTH',161:'DGG',162:'SEA',163:'JKA',164:'KBS',
    165:'TKA',166:'PGG',167:'BBG',168:'KLC',169:'CTD',170:'PJA',171:'JMR',172:'TMJ',173:'SCA',
    174:'BBP',175:'LBG',176:'TPG',177:'JRU',178:'MIN',179:'OUG',180:'KBG',
    182:'JPU',183:'JCL',184:'JPN',185:'KCY',186:'JTZ',
    # 187 commented out in SAS
    188:'PLT',189:'BNH',190:'BTR',191:'KPT',192:'MRD',193:'MKH',194:'SRK',195:'BWK',196:'JHL',
    197:'TNM',198:'TDA',199:'JTH',
    # 200 commented out in SAS
    201:'PDA',202:'RWG',203:'SJM',204:'BTW',205:'SNG',206:'TBM',207:'BCM',208:'JSI',209:'STW',
    210:'TMM',211:'TPD',212:'JMA',213:'JKB',214:'JGA',215:'JKP',216:'SKI',217:'TMB',220:'GHS',
    221:'TSK',222:'TDC',223:'TRJ',224:'JAH',225:'TIH',226:'JPR',227:'KSB',228:'INN',229:'TSJ',
    230:'SSH',231:'BBM',232:'TMD',233:'BEN',234:'SRM',235:'SBM',236:'UYB',237:'KLS',238:'JKT',
    239:'KMY',240:'KAP',241:'DJA',242:'TKK',243:'KKR',244:'GRT',245:'BDR',246:'BGH',247:'BPR',
    248:'JTS',249:'TAI',250:'TEA',251:'KPR',252:'TMA',253:'JTT',254:'KPH',255:'SBP',256:'PBR',
    257:'RAU',258:'JTA',259:'SAN',260:'KDN',261:'GMG',262:'TCT',263:'BTA',264:'JBH',265:'JAI',
    266:'JDK',267:'TDI',268:'BBT',269:'MKA',270:'BPI',273:'LHA',274:'STG',275:'MSL',276:'JAS',
    277:'WSU',278:'JPI',279:'PTJ',280:'KDA',281:'PLT',282:'PTT',283:'PSE',284:'BSP',285:'BMC',
    286:'BIH',287:'SUA',288:'SPT',289:'TEE',290:'TDY',291:'BSL',292:'BMJ',293:'BSA',294:'KKM',
    295:'BKR',296:'BJL',
    701:'IKB',702:'IPJ',703:'IWS',704:'IJK',
}
_BRCHCD_HOE_RANGES = [(3000, 3001), (7000, 7000), (7500, 8000), (8050, 8750)]

def _format_brchcd(branch: int) -> str:
    if branch is None:
        return ''
    for lo, hi in _BRCHCD_HOE_RANGES:
        if lo <= branch <= hi:
            return 'HOE'
    offset = branch - 3000
    if 1 <= offset <= 296 and offset in _BRCHCD_MAP:
        return _BRCHCD_MAP[offset]
    return _BRCHCD_MAP.get(branch, '')


# ===========================================================================
# FORMAT: FDFMT  (1-60 months)
# ===========================================================================
def fdfmt(mth: int) -> str:
    if mth is None or mth == 0:
        return ' '
    if 1 <= mth <= 60:
        return f"{mth} MONTH{'S' if mth > 1 else ''}"
    return ' '


# ===========================================================================
# INTPLAN -> MTH mapping
# ===========================================================================
_INTPLAN_MTH: dict[int, int] = {}

def _build_intplan_map():
    mapping = [
        (1,  [340,448,660,720]),
        (2,  [352,449,661,721]),
        (3,  [341,450,662,722]),
        (4,  [353,451,663,723]),
        (5,  [354,452,664,724]),
        (6,  [342,453,665,725]),
        (7,  [355,454,666,726]),
        (8,  [356,455,667,727]),
        (9,  [343,456,668,728]),
        (10, [357,457,669,729]),
        (11, [358,458,670,730]),
        (12, [344,459,671,731]),
        (13, [588,461,672,732]),
        (14, [589,462,673,733]),
        (15, [345,463,674,734]),
        (16, [590,675]),
        (17, [591,676]),
        (18, [346,464,677,735]),
        (19, [592,678]),
        (20, [593,679]),
        (21, [347,465,680,736]),
        (22, [594,681]),
        (23, [595,682]),
        (24, [348,466,683,737]),
        (25, [596,684]),
        (26, [597,685]),
        (27, [359,686]),
        (28, [598,687]),
        (29, [599,688]),
        (30, [540,580,689]),
        (31, [690]),
        (32, [691]),
        (33, [541,581,692]),
        (34, [693]),
        (35, [694]),
        (36, [349,467,695,738]),
        (37, [696]),
        (38, [697]),
        (39, [542,582,698]),
        (40, [699]),
        (41, [700]),
        (42, [543,583,701]),
        (43, [702]),
        (44, [703]),
        (45, [544,584,704]),
        (46, [705]),
        (47, [706]),
        (48, [350,468,707,739]),
        (49, [708]),
        (50, [709]),
        (51, [545,585,710]),
        (52, [711]),
        (53, [712]),
        (54, [546,586,713]),
        (55, [714]),
        (56, [715]),
        (57, [547,587,716]),
        (58, [717]),
        (59, [718]),
        (60, [351,719,740]),
    ]
    for mth, plans in mapping:
        for p in plans:
            _INTPLAN_MTH[p] = mth

_build_intplan_map()

def map_intplan_to_mth(intplan: int) -> int:
    return _INTPLAN_MTH.get(intplan, 0)


# ===========================================================================
# REPORT HELPERS
# ===========================================================================
def format_comma9(val) -> str:
    if val is None:
        return f"{'0':>9}"
    return f"{int(round(val)):>9,}"

def format_comma14_2(val) -> str:
    if val is None:
        return f"{'0.00':>14}"
    return f"{float(val):>14,.2f}"


def render_fd_tabulate(data: pl.DataFrame, section_title: str,
                       report_titles: list[str], rdate: str) -> list[str]:
    """
    Render tabulate: BRCH x MTH -> FDINO (NO OF A/C), FDINO2 (NO OF RECEIPT), FDI
    Plus an ALL='TOTAL' column across MTH.
    """
    if data.is_empty():
        return []

    agg = (
        data.group_by(['BRCH', 'MTH'])
        .agg([
            pl.col('FDINO').sum().alias('FDINO'),
            pl.col('FDINO2').sum().alias('FDINO2'),
            pl.col('FDI').sum().alias('FDI'),
        ])
        .sort(['BRCH', 'MTH'])
    )

    months_present = sorted(agg['MTH'].unique().to_list())

    # Build pivot
    pivot: dict[str, dict] = {}
    for row in agg.iter_rows(named=True):
        b = row['BRCH']
        pivot.setdefault(b, {})
        pivot[b][row['MTH']] = (row['FDINO'], row['FDINO2'], row['FDI'])

    branches = sorted(pivot.keys())

    lines: list[str] = []
    for t in report_titles:
        lines.append(t.center(LINE_SIZE))
    lines.append(f"AS AT {rdate}".center(LINE_SIZE))
    lines.append('')
    lines.append(section_title.center(LINE_SIZE))
    lines.append('')

    # Header row
    brch_w = 10
    mth_w  = 9 + 1 + 9 + 1 + 14   # FDINO + FDINO2 + FDI
    hdr = f"{'BRANCH NO/CODE':<{brch_w}}"
    for m in months_present:
        hdr += f"{fdfmt(m):^{mth_w}}"
    hdr += f"{'TOTAL':^{mth_w}}"
    lines.append(hdr)

    sub = ' ' * brch_w
    col_hdr = f"{'NO OF A/C':>9} {'NO OF RECEIPT':>13} {' ':>14}"
    for _ in months_present:
        sub += col_hdr
    sub += col_hdr
    lines.append(sub)
    lines.append('-' * LINE_SIZE)

    # Totals accumulators
    tot: dict[int, list] = {m: [0.0, 0.0, 0.0] for m in months_present}
    tot_all = [0.0, 0.0, 0.0]

    for brch in branches:
        line = f"{brch:<{brch_w}}"
        row_all = [0.0, 0.0, 0.0]
        for m in months_present:
            vals = pivot[brch].get(m, (0.0, 0.0, 0.0))
            fdino, fdino2, fdi = vals[0] or 0, vals[1] or 0, vals[2] or 0
            line += f"{format_comma9(fdino)} {format_comma9(fdino2)} {format_comma14_2(fdi)}"
            tot[m][0] += fdino; tot[m][1] += fdino2; tot[m][2] += fdi
            row_all[0] += fdino; row_all[1] += fdino2; row_all[2] += fdi
        line += f"{format_comma9(row_all[0])} {format_comma9(row_all[1])} {format_comma14_2(row_all[2])}"
        tot_all[0] += row_all[0]; tot_all[1] += row_all[1]; tot_all[2] += row_all[2]
        lines.append(line)

    lines.append('-' * LINE_SIZE)
    tline = f"{'TOTAL':<{brch_w}}"
    for m in months_present:
        tline += f"{format_comma9(tot[m][0])} {format_comma9(tot[m][1])} {format_comma14_2(tot[m][2])}"
    tline += f"{format_comma9(tot_all[0])} {format_comma9(tot_all[1])} {format_comma14_2(tot_all[2])}"
    lines.append(tline)

    return lines


# ===========================================================================
# ASA CARRIAGE CONTROL
# ===========================================================================
def with_asa(lines: list[str], page_length: int = PAGE_LENGTH) -> list[str]:
    out = []
    line_in_page = 0
    for line in lines:
        asa = '1' if line_in_page == 0 else ' '
        out.append(asa + line)
        line_in_page += 1
        if line_in_page >= page_length:
            line_in_page = 0
    return out


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    # OPTIONS YEARCUTOFF=1950 NOCENTER NODATE MISSING=0 LINESIZE=132
    con = duckdb.connect()
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate: date = row[0] if isinstance(row[0], date) else row[0].date()
    reptmon  = f"{reptdate.month:02d}"
    rdate    = reptdate.strftime('%d/%m/%y')

    # IF DAY(REPTDATE+1) IN (23, 1) THEN REPTFQ='Y'
    next_day = reptdate + timedelta(days=1)
    reptfq = 'Y' if next_day.day in (23, 1) else 'N'

    # %CHKRPTDT - only execute if REPTFQ = 'Y'
    if reptfq != 'Y':
        print(f"REPTFQ={reptfq}: Report not generated (only runs on 22nd and month-end).")
        return

    # Determine reporting period
    rpdate  = reptdate
    strtdte = date(rpdate.year, rpdate.month, 1)
    sdate   = strtdte
    edate   = rpdate

    mis_parquet = MIS_DYIBUF_PARQUET_TMPL.format(reptmon=reptmon)

    con = duckdb.connect()
    raw = con.execute(f"SELECT * FROM read_parquet('{mis_parquet}')").df()
    con.close()

    df = pl.from_pandas(raw)

    # BRCHCD and BRCH
    df = df.with_columns([
        pl.col('BRANCH').map_elements(_format_brchcd, return_dtype=pl.Utf8).alias('BRCHCD'),
    ])
    df = df.with_columns([
        (pl.col('BRANCH').cast(pl.Utf8).str.zfill(3) + pl.lit('/') + pl.col('BRCHCD')).alias('BRCH')
    ])

    # Map INTPLAN -> MTH
    df = df.with_columns([
        pl.col('INTPLAN').map_elements(map_intplan_to_mth, return_dtype=pl.Int32).alias('MTH')
    ])

    # Fill nulls
    for col in ['FDI', 'FDINO', 'FDINO2']:
        if col in df.columns:
            df = df.with_columns(pl.col(col).fill_null(0))
        else:
            df = df.with_columns(pl.lit(0.0).alias(col))

    if df['REPTDATE'].dtype != pl.Date:
        df = df.with_columns(pl.col('REPTDATE').cast(pl.Date))

    # DYIBUF: reptdate == edate
    df_today  = df.filter(pl.col('REPTDATE') == edate)
    # DYIBUF1: sdate <= reptdate <= edate (and sdate != edate per original)
    df_period = pl.DataFrame()
    if sdate != edate:
        df_period = df.filter(
            (pl.col('REPTDATE') >= sdate) & (pl.col('REPTDATE') <= edate)
        )

    report_titles = [
        'REPORT ID : DIBMIS02',
        'PUBLIC BANK BERHAD - IBU',
        'AL-MUDHARABAH FD ACCOUNT',
    ]

    all_lines: list[str] = []

    # %TDBAL
    if not df_today.is_empty():
        # PROC SORT DATA=DYIBUF OUT=DYIBUF (KEEP=BRCH MTH FDI FDINO FDINO2); BY BRANCH;
        td_lines = render_fd_tabulate(df_today, 'TODATE BALANCE', report_titles, rdate)
        all_lines.extend(td_lines)

    # %AVG - CHK IF AVG DAILY BALANCE DATA AVAILABLE
    n = len(df_period)
    if n > 0:
        # PROC SUMMARY NWAY: sum by BRCH MTH
        sumdf = (
            df_period.group_by(['BRCH', 'MTH'])
            .agg([
                pl.col('FDI').sum(), pl.col('FDINO').sum(), pl.col('FDINO2').sum()
            ])
        )
        days = edate.day
        sumdf = sumdf.with_columns([
            (pl.col('FDI')    / days).alias('FDI'),
            (pl.col('FDINO')  / days).alias('FDINO'),
            (pl.col('FDINO2') / days).alias('FDINO2'),
        ])
        avg_lines = render_fd_tabulate(sumdf, 'DAILY AVERAGE', report_titles, rdate)
        all_lines.extend([''] + avg_lines)

    final_lines = with_asa(all_lines, PAGE_LENGTH)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(final_lines))
        f.write('\n')

    print(f"Report written to: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
