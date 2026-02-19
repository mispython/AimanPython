# !/usr/bin/env python3
"""
Program : DIBMIS01
Purpose : Report on Islamic Savings/Current Accounts (Al-Wadiah)
          Generates TODATE BALANCE and DAILY AVERAGE tabulate reports
            by branch for Islamic savings (SAI), current (CAI), Mudharabah CA (CA96),
            and MBSA (MBS) accounts.
Note    : Revamp on 23-03-09
"""

# Dependency: PBMISFMT.py (format functions used for branch code mapping)
# from PBMISFMT import format_brchcd  # placeholder - see PBMISFMT.py

import sys
import os
import duckdb
import polars as pl
from datetime import date, datetime
import calendar

# ===========================================================================
# PATH CONFIGURATION
# ===========================================================================
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR      = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR     = os.path.join(BASE_DIR, 'output')

# Input parquet files
REPTDATE_PARQUET = os.path.join(INPUT_DIR, 'deposit_reptdate.parquet')
# MIS.DYIBU&REPTMON -> e.g. mis_dyibu01.parquet (month 2-digit zero-padded)
MIS_DYIBU_PARQUET_TMPL = os.path.join(INPUT_DIR, 'mis_dyibu{reptmon}.parquet')

# Output report file
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'DIBMIS01.txt')

# Page layout
PAGE_LENGTH = 60
LINE_SIZE   = 132

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===========================================================================
# FORMAT: BRCHCD  (numeric branch -> 3-letter code)
# (Dependency: PBMISFMT - replicated inline for self-containment)
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
# LOCAL FORMAT: LBLA / LBLB
# ===========================================================================
LBLA = {1: 'SAVINGS', 2: 'CURRENT', 3: 'MUDHARABAH CA', 4: 'MBSA'}
LBLB = {
    1: 'TODATE BALANCE',
    2: 'DAILY AVERAGE',
    3: 'DAILY AVERAGE (GOVT)',
    4: 'DAILY AVERAGE (HOUSING DEVELOPER)',
}


# ===========================================================================
# HELPER: Read reptdate and derive macro variables
# ===========================================================================
def read_reptdate() -> tuple[date, str, str, int]:
    """Return (reptdate, reptmon, rdate, edate_int)."""
    con = duckdb.connect()
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    con.close()
    reptdate: date = row[0] if isinstance(row[0], date) else row[0].date()
    reptmon = f"{reptdate.month:02d}"
    rdate   = reptdate.strftime('%d/%m/%y')   # DDMMYY8. -> DD/MM/YY
    edate   = int(reptdate.strftime('%Y%j'))   # Z5. numeric representation (not used directly)
    return reptdate, reptmon, rdate, reptdate.day


# ===========================================================================
# MACRO TDBAL: Get to-date balance rows
# ===========================================================================
def tdbal(df_today: pl.DataFrame) -> pl.DataFrame:
    """Reshape today's snapshot into long form with LBL1/LBL2 indicators."""
    rows = []
    for row in df_today.iter_rows(named=True):
        brch   = row['BRCH']
        rows.append({'BRCH': brch, 'COUNT': row['SAINO'],  'AMOUNT': row['SAI'],  'LBL1': 1, 'LBL2': 1})
        rows.append({'BRCH': brch, 'COUNT': row['CAINO'],  'AMOUNT': row['CAI'],  'LBL1': 2, 'LBL2': 1})
        rows.append({'BRCH': brch, 'COUNT': row['CAI96'],  'AMOUNT': row['CA96'], 'LBL1': 3, 'LBL2': 1})
        rows.append({'BRCH': brch, 'COUNT': row['MBSNO'],  'AMOUNT': row['MBS'],  'LBL1': 4, 'LBL2': 1})
    return pl.DataFrame(rows)


# ===========================================================================
# MACRO AVGBAL: Compute daily average balance rows
# ===========================================================================
def avgbal(df_period: pl.DataFrame, edate_day: int) -> pl.DataFrame:
    """Sum period rows, divide by number of days in month, reshape to long form."""
    if df_period.is_empty():
        return pl.DataFrame()

    days = edate_day
    sumdf = df_period.select([
        'BRCH',
        pl.col('SAI').sum().alias('SAI'),
        pl.col('CAI').sum().alias('CAI'),
        pl.col('CAIG').sum().alias('CAIG'),
        pl.col('CAIH').sum().alias('CAIH'),
        pl.col('SAINO').sum().alias('SAINO'),
        pl.col('CAINO').sum().alias('CAINO'),
        pl.col('CAIGNO').sum().alias('CAIGNO'),
        pl.col('CAIHNO').sum().alias('CAIHNO'),
        pl.col('CA96').sum().alias('CA96'),
        pl.col('CAI96').sum().alias('CAI96'),
        pl.col('MBS').sum().alias('MBS'),
        pl.col('MBSNO').sum().alias('MBSNO'),
    ]).group_by('BRCH').agg([
        pl.col('SAI').sum(), pl.col('CAI').sum(), pl.col('CAIG').sum(), pl.col('CAIH').sum(),
        pl.col('SAINO').sum(), pl.col('CAINO').sum(), pl.col('CAIGNO').sum(), pl.col('CAIHNO').sum(),
        pl.col('CA96').sum(), pl.col('CAI96').sum(), pl.col('MBS').sum(), pl.col('MBSNO').sum(),
    ])

    # Divide all numeric columns by DAYS
    for col in ['SAI','CAI','CAIG','CAIH','SAINO','CAINO','CAIGNO','CAIHNO','CA96','CAI96','MBS','MBSNO']:
        sumdf = sumdf.with_columns((pl.col(col) / days).alias(col))

    rows = []
    for row in sumdf.iter_rows(named=True):
        brch = row['BRCH']
        rows.append({'BRCH': brch, 'COUNT': row['SAINO'],  'AMOUNT': row['SAI'],  'LBL1': 1, 'LBL2': 2})
        rows.append({'BRCH': brch, 'COUNT': row['CAINO'],  'AMOUNT': row['CAI'],  'LBL1': 2, 'LBL2': 2})
        rows.append({'BRCH': brch, 'COUNT': row['CAIGNO'], 'AMOUNT': row['CAIG'], 'LBL1': 2, 'LBL2': 3})
        rows.append({'BRCH': brch, 'COUNT': row['CAIHNO'], 'AMOUNT': row['CAIH'], 'LBL1': 2, 'LBL2': 4})
        rows.append({'BRCH': brch, 'COUNT': row['CAI96'],  'AMOUNT': row['CA96'], 'LBL1': 3, 'LBL2': 2})
        rows.append({'BRCH': brch, 'COUNT': row['MBSNO'],  'AMOUNT': row['MBS'],  'LBL1': 4, 'LBL2': 2})
    return pl.DataFrame(rows)


# ===========================================================================
# REPORT RENDERING
# ===========================================================================
def format_comma9(val) -> str:
    if val is None:
        return f"{'0':>9}"
    return f"{int(round(val)):>9,}"

def format_comma14_2(val) -> str:
    if val is None:
        return f"{'0.00':>14}"
    return f"{val:>14,.2f}"


def render_tabulate(data: pl.DataFrame, section_label: str,
                    title_lines: list[str], rdate: str) -> list[str]:
    """
    Render a PROC TABULATE-style report section.
    Columns: BRANCH NO/CODE | LBL1 (LBLA) x LBL2 (LBLB) x (NO OF A/C | O/S BALANCE)
    Returns list of output lines (without ASA chars - added by caller).
    """
    if data.is_empty():
        return []

    # Aggregate: group by BRCH, LBL1, LBL2 -> sum COUNT, AMOUNT
    agg = (
        data.group_by(['BRCH', 'LBL1', 'LBL2'])
        .agg([pl.col('COUNT').sum(), pl.col('AMOUNT').sum()])
        .sort(['BRCH', 'LBL1', 'LBL2'])
    )

    # Determine unique LBL1/LBL2 combos present
    combos = sorted(agg.select(['LBL1','LBL2']).unique().rows())

    # Build pivot: brch -> {(lbl1,lbl2): (count, amount)}
    branch_vals: dict[str, dict] = {}
    for row in agg.iter_rows(named=True):
        key = row['BRCH']
        branch_vals.setdefault(key, {})
        branch_vals[key][(row['LBL1'], row['LBL2'])] = (row['COUNT'], row['AMOUNT'])

    # Also compute TOTAL row
    total_vals: dict[tuple, tuple] = {}
    for bvals in branch_vals.values():
        for combo, (cnt, amt) in bvals.items():
            tc, ta = total_vals.get(combo, (0.0, 0.0))
            total_vals[combo] = (tc + (cnt or 0), ta + (amt or 0))

    # --- Header ---
    col_width = 25   # per combo: 9 (count) + 1 + 14 (amount) + 1
    brch_col  = 10

    lines: list[str] = []
    lines.append(section_label)
    lines.append('')

    # Title block
    for t in title_lines:
        lines.append(t.center(LINE_SIZE))
    lines.append(f"AS AT {rdate}".center(LINE_SIZE))
    lines.append('')

    # Column headers: group by LBL1 first
    lbl1_groups: dict[int, list[int]] = {}
    for l1, l2 in combos:
        lbl1_groups.setdefault(l1, []).append(l2)

    # Row: LBL1 labels spanning their columns
    hdr1 = f"{'BRANCH NO/CODE':<{brch_col}}"
    for l1, l2_list in lbl1_groups.items():
        lbl1_text = LBLA.get(l1, str(l1))
        span_width = len(l2_list) * col_width
        hdr1 += f"{lbl1_text:^{span_width}}"
    lines.append(hdr1)

    # Row: LBL2 labels
    hdr2 = ' ' * brch_col
    for l1, l2_list in lbl1_groups.items():
        for l2 in l2_list:
            lbl2_text = LBLB.get(l2, str(l2))
            hdr2 += f"{lbl2_text:^{col_width}}"
    lines.append(hdr2)

    # Row: sub-column headers
    hdr3 = ' ' * brch_col
    for l1, l2_list in lbl1_groups.items():
        for l2 in l2_list:
            hdr3 += f"{'NO OF A/C':>9} {'O/S BALANCE':>14}"
    lines.append(hdr3)
    lines.append('-' * LINE_SIZE)

    # Data rows sorted by BRCH
    for brch in sorted(branch_vals.keys()):
        line = f"{brch:<{brch_col}}"
        bvals = branch_vals[brch]
        for combo in combos:
            cnt, amt = bvals.get(combo, (0.0, 0.0))
            line += f"{format_comma9(cnt)} {format_comma14_2(amt)}"
        lines.append(line)

    lines.append('-' * LINE_SIZE)

    # TOTAL row
    tline = f"{'TOTAL':<{brch_col}}"
    for combo in combos:
        tc, ta = total_vals.get(combo, (0.0, 0.0))
        tline += f"{format_comma9(tc)} {format_comma14_2(ta)}"
    lines.append(tline)

    return lines


# ===========================================================================
# ASA CARRIAGE CONTROL
# ===========================================================================
def with_asa(lines: list[str], page_length: int = PAGE_LENGTH) -> list[str]:
    """Prepend ASA carriage-control characters. '1'=new page, ' '=single space, '0'=double space."""
    out = []
    line_in_page = 0
    first_page = True
    for i, line in enumerate(lines):
        if line_in_page == 0:
            asa = '1' if not first_page else '1'
            first_page = False
        else:
            asa = ' '
        out.append(asa + line)
        line_in_page += 1
        if line_in_page >= page_length:
            line_in_page = 0
    return out


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    # OPTIONS YEARCUTOFF=1950 NOCENTER NODATE MISSING=0
    reptdate, reptmon, rdate, edate_day = read_reptdate()

    mis_parquet = MIS_DYIBU_PARQUET_TMPL.format(reptmon=reptmon)

    con = duckdb.connect()
    raw = con.execute(f"SELECT * FROM read_parquet('{mis_parquet}')").df()
    con.close()

    df = pl.from_pandas(raw)

    # CAI = SUM(CAI, CAIG, CAIH, CA96)
    # CAINO = SUM(CAINO, CAIGNO, CAIHNO, CAI96)
    df = df.with_columns([
        (pl.col('CAI').fill_null(0) + pl.col('CAIG').fill_null(0) +
         pl.col('CAIH').fill_null(0) + pl.col('CA96').fill_null(0)).alias('CAI'),
        (pl.col('CAINO').fill_null(0) + pl.col('CAIGNO').fill_null(0) +
         pl.col('CAIHNO').fill_null(0) + pl.col('CAI96').fill_null(0)).alias('CAINO'),
    ])

    # BRCHCD and BRCH
    df = df.with_columns([
        pl.col('BRANCH').map_elements(_format_brchcd, return_dtype=pl.Utf8).alias('BRCHCD'),
    ])
    df = df.with_columns([
        (pl.col('BRANCH').cast(pl.Utf8).str.zfill(3) + pl.lit('/') + pl.col('BRCHCD')).alias('BRCH')
    ])

    # Fill nulls for numeric columns used
    for col in ['SAI','SAINO','CAI','CAINO','CA96','CAI96','MBS','MBSNO',
                'CAIG','CAIH','CAIGNO','CAIHNO']:
        if col in df.columns:
            df = df.with_columns(pl.col(col).fill_null(0))
        else:
            df = df.with_columns(pl.lit(0.0).alias(col))

    # REPTDATE as date
    if df['REPTDATE'].dtype != pl.Date:
        df = df.with_columns(pl.col('REPTDATE').cast(pl.Date))

    edate = reptdate

    # Split: today (DYIBU) and period (DYIBU1)
    df_today  = df.filter(pl.col('REPTDATE') == edate)
    df_period = df.filter(pl.col('REPTDATE') <= edate)

    # %TDBAL
    long_today = tdbal(df_today)

    # Check N for AVGBAL
    n = len(df_period)

    title_lines = [
        'REPORT ID : DIBMIS01',
        'PUBLIC BANK BERHAD - IBU',
        'AL-WADIAH SAVINGS/CURRENT ACCOUNT',
    ]

    all_output_lines: list[str] = []

    if not long_today.is_empty():
        report_lines = render_tabulate(long_today, 'TODATE BALANCE', title_lines, rdate)
        all_output_lines.extend(report_lines)

    # %AVGBAL
    if n > 0:
        long_avg = avgbal(df_period, edate_day)
        if not long_avg.is_empty():
            # PROC APPEND BASE=DYIBU DATA=DYIBU1
            combined = pl.concat([long_today, long_avg])
            avg_report = render_tabulate(long_avg, 'DAILY AVERAGE', title_lines, rdate)
            all_output_lines.extend([''] + avg_report)

    final_lines = with_asa(all_output_lines, PAGE_LENGTH)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(final_lines))
        f.write('\n')

    print(f"Report written to: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
