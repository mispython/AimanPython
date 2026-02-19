# !/usr/bin/env python3
"""
Program : DIIMISB2
Date    : 31.03.09
Report  : PBB Old Al-Mudharabah FD Account Prior Privatisation.
          Maturity month (MTH) is computed from remaining months between
            maturity date (MATDATE) and reporting date (REPTDATE) using
            a precise fractional months formula (REMMTH macro).
          The FDFMT format uses ranges: LOW-1 = '01 MONTH', 1-2 = '02 MONTHS', etc.
          NOTE: PROC TABULATE in TDBAL/AVG use * FORMAT MTH FDFMT. (commented out in SAS).
          REPTFQ is always 'Y'. Input source: MIS.DYIBUB&REPTMON.
"""

# Dependency: PBMISFMT.py (format functions used for branch code mapping)
# from PBMISFMT import format_brchcd  # placeholder - see PBMISFMT.py

import os
import duckdb
import polars as pl
from datetime import date
import math

# ===========================================================================
# PATH CONFIGURATION
# ===========================================================================
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR      = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR     = os.path.join(BASE_DIR, 'output')

REPTDATE_PARQUET        = os.path.join(INPUT_DIR, 'deposit_reptdate.parquet')
MIS_DYIBUB_PARQUET_TMPL = os.path.join(INPUT_DIR, 'mis_dyibub{reptmon}.parquet')

OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'DIIMISB2.txt')

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
# DCLVAR / REMMTH macro equivalents
# Days per month array (1-indexed), accounting for leap year in February.
# ===========================================================================

def _days_in_month(year: int, month: int) -> int:
    """Return days in a given month, considering leap year for February."""
    if month == 2:
        # Leap year: MOD(year, 4) = 0
        return 29 if year % 4 == 0 else 28
    # Months with 30 days: 4,6,9,11
    if month in (4, 6, 9, 11):
        return 30
    return 31


def compute_remmth(reptdate: date, matdate_int: int) -> float:
    """
    %REMMTH macro equivalent.
    Computes remaining months between matdate and reptdate.
    matdate_int: integer in YYYYMMDD format (from MATDATE field, stored as Z8.).
    Returns fractional months remaining (REMMTH).
    """
    # Parse MATDATE from YYYYMMDD integer
    matdate_str = str(int(matdate_int)).zfill(8)
    try:
        fddate = date(int(matdate_str[:4]), int(matdate_str[4:6]), int(matdate_str[6:8]))
    except ValueError:
        return 0.0

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rpdays_rpmth = _days_in_month(rpyr, rpmth)

    fdyr  = fddate.year
    fdmth = fddate.month
    fdday = fddate.day
    fddays_fdmth = _days_in_month(fdyr, fdmth)

    # If fdday equals end of fd month, map to end of rp month
    if fdday == fddays_fdmth:
        fdday = rpdays_rpmth

    remy = fdyr  - rpyr
    remm = fdmth - rpmth
    remd = fdday - rpday

    remmth = remy * 12 + remm + remd / rpdays_rpmth
    return remmth


def format_fdfmt_range(remmth: float) -> str:
    """
    FDFMT range format (used in DIIMISB2):
    LOW - 1  = '01 MONTH'
    1   - 2  = '02 MONTHS'
    ...
    59  - 60 = '60 MONTHS'
    (exclusive upper bound per SAS range format convention)
    """
    if remmth <= 1:  return '01 MONTH '
    if remmth <= 2:  return '02 MONTHS'
    if remmth <= 3:  return '03 MONTHS'
    if remmth <= 4:  return '04 MONTHS'
    if remmth <= 5:  return '05 MONTHS'
    if remmth <= 6:  return '06 MONTHS'
    if remmth <= 7:  return '07 MONTHS'
    if remmth <= 8:  return '08 MONTHS'
    if remmth <= 9:  return '09 MONTHS'
    if remmth <= 10: return '10 MONTHS'
    if remmth <= 11: return '11 MONTHS'
    if remmth <= 12: return '12 MONTHS'
    if remmth <= 13: return '13 MONTHS'
    if remmth <= 14: return '14 MONTHS'
    if remmth <= 15: return '15 MONTHS'
    if remmth <= 16: return '16 MONTHS'
    if remmth <= 17: return '17 MONTHS'
    if remmth <= 18: return '18 MONTHS'
    if remmth <= 19: return '19 MONTHS'
    if remmth <= 20: return '20 MONTHS'
    if remmth <= 21: return '21 MONTHS'
    if remmth <= 22: return '22 MONTHS'
    if remmth <= 23: return '23 MONTHS'
    if remmth <= 24: return '24 MONTHS'
    if remmth <= 25: return '25 MONTHS'
    if remmth <= 26: return '26 MONTHS'
    if remmth <= 27: return '27 MONTHS'
    if remmth <= 28: return '28 MONTHS'
    if remmth <= 29: return '29 MONTHS'
    if remmth <= 30: return '30 MONTHS'
    if remmth <= 31: return '31 MONTHS'
    if remmth <= 32: return '32 MONTHS'
    if remmth <= 33: return '33 MONTHS'
    if remmth <= 34: return '34 MONTHS'
    if remmth <= 35: return '35 MONTHS'
    if remmth <= 36: return '36 MONTHS'
    if remmth <= 37: return '37 MONTHS'
    if remmth <= 38: return '38 MONTHS'
    if remmth <= 39: return '39 MONTHS'
    if remmth <= 40: return '40 MONTHS'
    if remmth <= 41: return '41 MONTHS'
    if remmth <= 42: return '42 MONTHS'
    if remmth <= 43: return '43 MONTHS'
    if remmth <= 44: return '44 MONTHS'
    if remmth <= 45: return '45 MONTHS'
    if remmth <= 46: return '46 MONTHS'
    if remmth <= 47: return '47 MONTHS'
    if remmth <= 48: return '48 MONTHS'
    if remmth <= 49: return '49 MONTHS'
    if remmth <= 50: return '50 MONTHS'
    if remmth <= 51: return '51 MONTHS'
    if remmth <= 52: return '52 MONTHS'
    if remmth <= 53: return '53 MONTHS'
    if remmth <= 54: return '54 MONTHS'
    if remmth <= 55: return '55 MONTHS'
    if remmth <= 56: return '56 MONTHS'
    if remmth <= 57: return '57 MONTHS'
    if remmth <= 58: return '58 MONTHS'
    if remmth <= 59: return '59 MONTHS'
    return '60 MONTHS'


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
    Render tabulate: BRCH x MTH (string label) -> FDINO, FDINO2, FDI + TOTAL column.
    NOTE: FORMAT MTH FDFMT. is commented out in original SAS TDBAL/AVG,
          so MTH appears as numeric string.
    """
    if data.is_empty():
        return []

    agg = (
        data.group_by(['BRCH', 'MTH'])
        .agg([
            pl.col('FDINO').sum(),
            pl.col('FDINO2').sum(),
            pl.col('FDI').sum(),
        ])
        .sort(['BRCH', 'MTH'])
    )

    months_present = sorted(agg['MTH'].unique().to_list())

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

    brch_w = 10
    col_w  = 9 + 1 + 9 + 1 + 14

    # * FORMAT MTH FDFMT.; (commented out - MTH shown as raw label string)
    hdr = f"{'BRANCH NO/CODE':<{brch_w}}"
    for m in months_present:
        hdr += f"{str(m):^{col_w}}"
    hdr += f"{'TOTAL':^{col_w}}"
    lines.append(hdr)

    sub = ' ' * brch_w
    col_sub = f"{'NO OF A/C':>9} {'NO OF RECEIPT':>13} {' ':>14}"
    for _ in months_present:
        sub += col_sub
    sub += col_sub
    lines.append(sub)
    lines.append('-' * LINE_SIZE)

    tot: dict[str, list] = {m: [0.0, 0.0, 0.0] for m in months_present}
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

    # REPTFQ='Y' always (forced)
    reptfq = 'Y'

    # %CHKRPTDT
    if reptfq != 'Y':
        print("Report not generated.")
        return

    # Determine reporting period
    rpdate  = reptdate
    strtdte = date(rpdate.year, rpdate.month, 1)
    sdate   = strtdte
    edate   = rpdate

    # DATA CX: SDATE = &SDATE; EDATE = &EDATE; RPDATE = &RPDATE;
    # (informational only)

    mis_parquet = MIS_DYIBUB_PARQUET_TMPL.format(reptmon=reptmon)

    con = duckdb.connect()
    raw = con.execute(f"SELECT * FROM read_parquet('{mis_parquet}')").df()
    con.close()

    df = pl.from_pandas(raw)

    # Ensure MATDATE column exists
    if 'MATDATE' not in df.columns:
        df = df.with_columns(pl.lit(0).cast(pl.Int64).alias('MATDATE'))

    # BRCHCD and BRCH
    df = df.with_columns([
        pl.col('BRANCH').map_elements(_format_brchcd, return_dtype=pl.Utf8).alias('BRCHCD'),
    ])
    df = df.with_columns([
        (pl.col('BRANCH').cast(pl.Utf8).str.zfill(3) + pl.lit('/') + pl.col('BRCHCD')).alias('BRCH')
    ])

    if df['REPTDATE'].dtype != pl.Date:
        df = df.with_columns(pl.col('REPTDATE').cast(pl.Date))

    for col in ['FDI', 'FDINO', 'FDINO2']:
        if col in df.columns:
            df = df.with_columns(pl.col(col).fill_null(0))
        else:
            df = df.with_columns(pl.lit(0.0).alias(col))

    # %DCLVAR / %REMMTH: compute MTH = PUT(REMMTH, FDFMT.)
    # MTH is a string label based on fractional months remaining
    def compute_mth_label(row_reptdate, row_matdate) -> str:
        try:
            rd = row_reptdate if isinstance(row_reptdate, date) else row_reptdate.date()
            remmth = compute_remmth(rd, row_matdate)
            return format_fdfmt_range(remmth)
        except Exception:
            return '01 MONTH '

    df = df.with_columns([
        pl.struct(['REPTDATE', 'MATDATE']).map_elements(
            lambda s: compute_mth_label(s['REPTDATE'], s['MATDATE']),
            return_dtype=pl.Utf8
        ).alias('MTH')
    ])

    # DYIBUA: reptdate == edate
    df_today  = df.filter(pl.col('REPTDATE') == edate)
    # DYIBUA1: sdate <= reptdate <= edate
    df_period = df.filter(
        (pl.col('REPTDATE') >= sdate) & (pl.col('REPTDATE') <= edate)
    )

    report_titles = [
        'REPORT ID : DIBMISB2',
        'PUBLIC ISLAMIC BANK BERHAD - IBU',
        'PBB OLD AL-MUDHARABAH FD ACCOUNT PRIOR PRIVATISATION',
    ]

    all_lines: list[str] = []

    # %TDBAL
    if not df_today.is_empty():
        # PROC SORT DATA=DYIBUA OUT=DYIBUA (KEEP=BRCH MTH FDI FDINO FDINO2); BY BRANCH;
        td_lines = render_fd_tabulate(df_today, 'TODATE BALANCE', report_titles, rdate)
        all_lines.extend(td_lines)

    # CHK IF AVG DAILY BALANCE DATA AVAILABLE
    # PROC CONTENTS DATA=DYIBUA1 NOPRINT OUT=NUMOBS(KEEP=NOBS);
    n = len(df_period)

    # %AVG
    if n > 0:
        sumdf = (
            df_period.group_by(['BRCH', 'MTH'])
            .agg([pl.col('FDI').sum(), pl.col('FDINO').sum(), pl.col('FDINO2').sum()])
        )
        days = edate.day
        sumdf = sumdf.with_columns([
            (pl.col('FDI')    / days).alias('FDI'),
            (pl.col('FDINO')  / days).alias('FDINO'),
            (pl.col('FDINO2') / days).alias('FDINO2'),
        ])
        # * FORMAT MTH FDFMT.; (commented out in original SAS AVG)
        avg_lines = render_fd_tabulate(sumdf, 'DAILY AVERAGE', report_titles, rdate)
        all_lines.extend([''] + avg_lines)

    final_lines = with_asa(all_lines, PAGE_LENGTH)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(final_lines))
        f.write('\n')

    print(f"Report written to: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
