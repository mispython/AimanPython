#!/usr/bin/env python3
"""
Program : EIIMRM03.py
Date    : 01.04.08
Report  : FD-BY INDIVIDUAL AND NON-INDIVIDUAL, BY TIME
          TO MATURITY FOR ALCO
          (WEIGHTED AVERAGE COST BY MATURITY PROFILE)
"""

import duckdb
import polars as pl
import math
from datetime import date, datetime
from pathlib import Path

# ============================================================================
# PATHS
# ============================================================================
INPUT_DIR        = Path("input")
OUTPUT_DIR       = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FD_PARQUET       = INPUT_DIR / "fd.parquet"
REPTDATE_PARQUET = INPUT_DIR / "reptdate.parquet"
OUTPUT_FILE      = OUTPUT_DIR / "EIIMRM03.txt"

# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

# REMFMT: continuous REMMTH -> label string (fractional months)
def remfmt(val):
    """
    Maps remaining months value (continuous) to display label.
    Low-0 = '       ', ranges by month bracket up to 60, plus special codes.
    """
    if val is None:
        return '  '
    fval = float(val)
    # Special sentinel values
    special = {
        91: ' 1 MONTH',
        92: ' 3 MONTHS',
        93: ' 6 MONTHS',
        94: ' 9 MONTHS',
        95: '12 MONTHS',
        96: '15 MONTHS',
        97: 'ABOVE 15 MONTHS',
        99: 'OVERDUE FD',
    }
    iv = int(fval)
    if iv in special and fval == iv:
        return special[iv]
    if fval <= 0:
        return '       '
    if fval <= 1:
        return '>  0-1 MTH'
    if fval <= 2:
        return '>  1-2 MTHS'
    if fval <= 3:
        return '>  2-3 MTHS'
    if fval <= 4:
        return '>  3-4 MTHS'
    if fval <= 5:
        return '>  4-5 MTHS'
    if fval <= 6:
        return '>  5-6 MTHS'
    if fval <= 7:
        return '>  6-7 MTHS'
    if fval <= 8:
        return '>  7-8 MTHS'
    if fval <= 9:
        return '>  8-9 MTHS'
    if fval <= 10:
        return '>  9-10 MTHS'
    if fval <= 11:
        return '> 10-11 MTHS'
    if fval <= 12:
        return '> 11-12 MTHS'
    if fval <= 13:
        return '> 12-13 MTHS'
    if fval <= 14:
        return '> 13-14 MTHS'
    if fval <= 15:
        return '> 14-15 MTHS'
    if fval <= 16:
        return '> 15-16 MTHS'
    if fval <= 17:
        return '> 16-17 MTHS'
    if fval <= 18:
        return '> 17-18 MTHS'
    if fval <= 19:
        return '> 18-19 MTHS'
    if fval <= 20:
        return '> 19-20 MTHS'
    if fval <= 21:
        return '> 20-21 MTHS'
    if fval <= 22:
        return '> 21-22 MTHS'
    if fval <= 23:
        return '> 22-23 MTHS'
    if fval <= 24:
        return '> 23-24 MTHS'
    if fval <= 36:
        return '>2-3 YRS'
    if fval <= 48:
        return '>3-4 YRS'
    if fval <= 60:
        return '>4-5 YRS'
    return '  '


# $SUBTTL format
SUBTTL_MAP = {
    'A': 'REMAINING MATURITY',
    'B': 'OVERDUE FD',
    'C': 'NEW FD FOR THE MONTH',
    'D': 'SAVING ACCOUNTS',
    'E': 'NON INTEREST BEARING',
    'F': 'INTEREST BEARING',
    'G': 'HOUSNG DEVELOPER ACC',
    'H': 'PORTION FROM ACE ACC',
}


def subttl_fmt(code):
    return SUBTTL_MAP.get(code, '')


# TERMFMT: INTPLAN -> term in months (for FCY)
TERMFMT_1 = {470, 471, 476, 477, 482, 483, 488, 489, 494, 495, 548, 549, 554, 555}
TERMFMT_3 = {472, 473, 478, 479, 484, 485, 490, 491, 496, 497, 550, 551, 556, 557}
TERMFMT_6 = {474, 475, 480, 481, 486, 487, 492, 493, 498, 499, 552, 553, 558, 559}


def termfmt(intplan):
    if intplan in TERMFMT_1:
        return 1
    if intplan in TERMFMT_3:
        return 3
    if intplan in TERMFMT_6:
        return 6
    return None


# FDPROD format (from PBBDPFMT)
from PBBDPFMT import fdprod_format

# ============================================================================
# DAYS-IN-MONTH HELPERS
# ============================================================================

_LDAY = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def days_in_month(yr, mth):
    if mth == 2:
        return 29 if (yr % 4 == 0) else 28
    return _LDAY[mth]


def calc_remmth(matdt: date, rp_yr: int, rp_mth: int, rp_day: int) -> float:
    """
    Replicate SAS %REMMTH macro.
    Calculates remaining months from report date to maturity date.
    """
    md_yr  = matdt.year
    md_mth = matdt.month
    md_day = matdt.day

    rp_days_in_mth = days_in_month(rp_yr, rp_mth)
    if md_day > rp_days_in_mth:
        md_day = rp_days_in_mth

    remy = md_yr  - rp_yr
    remm = md_mth - rp_mth
    remd = md_day - rp_day
    remmth = remy * 12 + remm + remd / rp_days_in_mth
    return remmth


# ============================================================================
# LOAD REPTDATE
# ============================================================================

def load_reptdate():
    con = duckdb.connect()
    row = con.execute(f"SELECT reptdate FROM read_parquet('{REPTDATE_PARQUET}') LIMIT 1").fetchone()
    reptdate = row[0]
    if isinstance(reptdate, datetime):
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
    reptyrs   = str(reptdate.year)[-2:]
    reptyear  = str(reptdate.year)
    reptmon   = f"{reptdate.month:02d}"
    reptday   = f"{reptdate.day:02d}"
    rdate     = reptdate.strftime("%d/%m/%y")   # DDMMYY8. -> dd/mm/yy
    return reptdate, nowk, reptyrs, reptyear, reptmon, reptday, rdate


# ============================================================================
# PROCESS FD DATA
# NOTE: EIIMRM03 filters to BNMCODE='42132' only (Islamic FD / SPTF),
#       whereas EIIMRM02 processes all FD products.
#       CUSTCD filter also differs: EIIMRM03 uses (77,78,95,96);
#       EIIMRM02 uses (76,77,78,95,96).
# ============================================================================

def process_fd(reptdate: date):
    rp_yr  = reptdate.year
    rp_mth = reptdate.month
    rp_day = reptdate.day

    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{FD_PARQUET}')").pl()

    fd_rows  = []
    td_rows  = []
    fdn_rows = []

    for row in df.iter_rows(named=True):
        intplan = row.get('INTPLAN')
        curbal  = row.get('CURBAL', 0) or 0
        rate    = row.get('RATE', 0) or 0
        openind = row.get('OPENIND', '')
        custcd  = row.get('CUSTCD')
        matdate = row.get('MATDATE')
        intdate = row.get('INTDATE')

        bnmcode = fdprod_format(intplan)

        # EIIMRM03 filters: IF BNMCODE='42132';
        if bnmcode != '42132':
            continue

        if bnmcode == '42630':
            prodtyp = 'FIXED DEPT(FCY)'
            term = termfmt(intplan)
        else:
            prodtyp = 'FIXED DEPT(RM)'
            term = None

        # TYPE — EIIMRM03 uses (77,78,95,96) not (76,77,78,95,96)
        if custcd in (77, 78, 95, 96):
            typ = '  INDIVIDUALS  '
        else:
            typ = 'NON-INDIVIDUALS'

        if openind in ('O', 'D') and curbal > 0:
            # Parse MATDATE (stored as integer YYYYMMDD or date)
            if isinstance(matdate, (int, float)):
                matdate_str = f"{int(matdate):08d}"
                matdt = date(int(matdate_str[:4]), int(matdate_str[4:6]), int(matdate_str[6:8]))
            elif isinstance(matdate, (date, datetime)):
                matdt = matdate if isinstance(matdate, date) else matdate.date()
            else:
                matdt = None

            base = {
                'PRODTYP':  prodtyp,
                'INTDATE':  intdate,
                'INTPLAN':  intplan,
                'OPENIND':  openind,
                'REPTDATE': reptdate,
                'TYPE':     typ,
                'CUSTCD':   custcd,
                'MATDT':    matdt,
                'AMOUNT':   curbal,
            }

            if openind == 'D' or (matdt is not None and matdt < reptdate):
                subtyp = 'SPTF' if bnmcode == '42132' else 'CONVENTIONAL'
                cost   = curbal * rate
                td_rows.append({**base, 'SUBTTL': 'B', 'SUBTYP': subtyp,
                                 'REMMTH': 99, 'TERM': term,
                                 'COST': cost, 'REMM': curbal * 99})
            else:
                remmth_val = calc_remmth(matdt, rp_yr, rp_mth, rp_day)
                subtyp = 'SPTF' if bnmcode == '42132' else 'CONVENTIONAL'
                cost   = curbal * rate
                remm   = curbal * remmth_val
                fd_rows.append({**base, 'SUBTTL': 'A', 'SUBTYP': subtyp,
                                 'REMMTH': remmth_val, 'TERM': term,
                                 'COST': cost, 'REMM': remm})

                # New FD for the month: (TERM - REMMTH) < 1
                if term is not None and (term - remmth_val) < 1:
                    remm2   = remmth_val * curbal
                    # REMMTH = TERM - 0.5 in SAS for FDN output
                    remmth_fdn = (term - 0.5) if term is not None else remmth_val
                    fdn_rows.append({**base, 'SUBTTL': 'C', 'SUBTYP': subtyp,
                                      'REMMTH': remmth_fdn, 'TERM': term,
                                      'COST': cost, 'REMM': remm2})

    def make_df(rows):
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    return make_df(td_rows), make_df(fd_rows), make_df(fdn_rows)


# ============================================================================
# SUMMARY
# ============================================================================

def summarise(df: pl.DataFrame, group_cols: list) -> pl.DataFrame:
    if df.is_empty():
        return df
    existing = [c for c in group_cols if c in df.columns]
    return (
        df.group_by(existing)
          .agg([
              pl.col('AMOUNT').sum(),
              pl.col('COST').sum(),
              pl.col('REMM').sum(),
          ])
    )


# ============================================================================
# BUILD DUMMY ROWS (1-60 months scaffold)
# ============================================================================

def build_dummy(dep: pl.DataFrame) -> pl.DataFrame:
    """
    For each unique (TYPE, PRODTYP, SUBTTL, SUBTYP) combination where SUBTTL in ('A','C'),
    generate rows for REMMTH 1..60, compute REMMTH1.
    """
    filtered = dep.filter(pl.col('SUBTTL').is_in(['A', 'C']))
    if filtered.is_empty():
        return pl.DataFrame()

    keys = filtered.select(['TYPE', 'PRODTYP', 'SUBTTL', 'SUBTYP']).unique()
    dummy_rows = []
    for row in keys.iter_rows(named=True):
        for rm in range(1, 61):
            dummy_rows.append({**row, 'REMMTH': float(rm), 'REMMTH1': remfmt(float(rm))})

    dummy = pl.DataFrame(dummy_rows)
    dummy = dummy.unique(subset=['TYPE', 'PRODTYP', 'SUBTTL', 'SUBTYP', 'REMMTH1'])
    return dummy


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    reptdate, nowk, reptyrs, reptyear, reptmon, reptday, rdate = load_reptdate()

    td_raw, fd_raw, fdn_raw = process_fd(reptdate)

    grp = ['TYPE', 'PRODTYP', 'SUBTYP', 'SUBTTL', 'REMMTH']
    td_sum  = summarise(td_raw,  grp)
    fd_sum  = summarise(fd_raw,  grp)
    fdn_sum = summarise(fdn_raw, grp)

    dep = pl.concat([td_sum, fd_sum, fdn_sum], how='diagonal')

    # Add REMMTH1 label
    dep = dep.with_columns(
        pl.col('REMMTH').map_elements(remfmt, return_dtype=pl.Utf8).alias('REMMTH1')
    )

    # Sort
    dep = dep.sort(['TYPE', 'PRODTYP', 'SUBTTL', 'SUBTYP', 'REMMTH1'])

    # Build dummy scaffold and merge
    dummy = build_dummy(dep)
    if not dummy.is_empty():
        dep = dep.join(
            dummy.select(['TYPE', 'PRODTYP', 'SUBTTL', 'SUBTYP', 'REMMTH1']),
            on=['TYPE', 'PRODTYP', 'SUBTTL', 'SUBTYP', 'REMMTH1'],
            how='outer'
        ).fill_null(0)

    # Compute WACOST, WAREMM, round AMOUNT
    dep = dep.with_columns([
        pl.when((pl.col('SUBTYP').is_in(['SPTF', 'CONVENTIONAL'])) & (pl.col('AMOUNT') != 0))
          .then(pl.col('COST') / pl.col('AMOUNT'))
          .otherwise(None)
          .alias('WACOST'),
        pl.when((pl.col('SUBTYP').is_in(['SPTF', 'CONVENTIONAL'])) & (pl.col('AMOUNT') != 0))
          .then(pl.col('REMM') / pl.col('AMOUNT'))
          .otherwise(None)
          .alias('WAREMM'),
        (pl.col('AMOUNT') / 1000).round(0).alias('AMOUNT'),
    ])

    # SUBTYPE TOTAL (class: TYPE PRODTYP SUBTTL REMMTH1)
    dep_total = (
        dep.group_by(['TYPE', 'PRODTYP', 'SUBTTL', 'REMMTH1'])
           .agg([pl.col('AMOUNT').sum(), pl.col('COST').sum(), pl.col('REMM').sum()])
    )
    dep_total = dep_total.with_columns([
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('COST') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WACOST'),
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('REMM') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WAREMM'),
        pl.lit('TOTAL').alias('SUBTYP'),
    ])

    # Sort dep for TYPE TOTAL step
    dep = dep.sort(['SUBTYP', 'PRODTYP', 'SUBTTL', 'REMMTH1'])

    # TYPE TOTAL (class: SUBTYP PRODTYP SUBTTL REMMTH1)
    dep_tota2 = (
        dep.group_by(['SUBTYP', 'PRODTYP', 'SUBTTL', 'REMMTH1'])
           .agg([pl.col('AMOUNT').sum(), pl.col('COST').sum(), pl.col('REMM').sum()])
    )
    dep_tota2 = dep_tota2.with_columns([
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('COST') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WACOST'),
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('REMM') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WAREMM'),
        pl.lit('TOTAL').alias('TYPE'),
    ])

    depfinal = pl.concat([dep, dep_total, dep_tota2], how='diagonal')

    # SUB-TOTAL (class: PRODTYP SUBTTL SUBTYP TYPE)
    dep_tota2b = (
        depfinal.group_by(['PRODTYP', 'SUBTTL', 'SUBTYP', 'TYPE'])
                .agg([pl.col('AMOUNT').sum(), pl.col('COST').sum(), pl.col('REMM').sum()])
    )
    dep_tota2b = dep_tota2b.with_columns([
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('COST') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WACOST'),
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('REMM') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WAREMM'),
        pl.lit('SUB-TOTAL').alias('REMMTH1'),
    ])

    depfinal = pl.concat([depfinal, dep_tota2b], how='diagonal')
    # Filter out blank TYPE (15 spaces)
    depfinal = depfinal.filter(pl.col('TYPE').fill_null('') != '               ')

    # ========================================================================
    # PRODUCE REPORT
    # ========================================================================
    produce_report(depfinal, rdate)


# ============================================================================
# REPORT GENERATION
# ============================================================================

PAGE_LENGTH = 60


def fmt_num(v, width=12, dec=0):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ' ' * width
    if dec == 0:
        return f"{v:>{width},.0f}"
    return f"{v:>{width},.{dec}f}"


def produce_report(depfinal: pl.DataFrame, rdate: str):
    def page_header(rdate):
        return [
            "1PUBLIC ISLAMIC BANK BERHAD",
            f" TIME TO MATURITY AS AT {rdate}",
            f" RISK MANAGEMENT REPORT : EIIMRM03",
            f" RM DENOMINATION",
            f" ",
        ]

    col_hdr = [
        " " + "-" * 139,
        (
            f" {'DEPOSITS':<65}"
            f"{'CONVENTIONAL':>20}"
            f"{'':>10}"
            f"{'SPTF':>20}"
            f"{'':>10}"
            f"{'TOTAL':>15}"
        ),
        (
            f" {'':<65}"
            f"{'BAL OUSTANDING':>14}{'W.A. COST %':>12}{'REMAINING MATURITY':>12}"
            f"{'BAL OUSTANDING':>14}{'W.A. COST %':>12}{'REMAINING MATURITY':>12}"
            f"{'BAL OUSTANDING':>14}{'W.A. COST %':>12}{'REMAINING MATURITY':>12}"
        ),
        (
            f" {'':<65}"
            f"{'(RM000)':>14}{'':>12}{'':>12}"
            f"{'(RM000)':>14}{'':>12}{'':>12}"
            f"{'(RM000)':>14}{'':>12}{'':>12}"
        ),
    ]

    # Sort for output: PRODTYP, SUBTTL, REMMTH1, SUBTYP, TYPE
    depfinal_sorted = depfinal.sort(
        ['PRODTYP', 'SUBTTL', 'REMMTH1', 'SUBTYP', 'TYPE'],
        nulls_last=True
    )

    # Pivot: (PRODTYP, SUBTTL, REMMTH1, TYPE) -> {SUBTYP: (AMOUNT, WACOST, WAREMM)}
    pivot = {}
    for row in depfinal_sorted.iter_rows(named=True):
        key = (
            row.get('PRODTYP', '') or '',
            row.get('SUBTTL', '') or '',
            row.get('REMMTH1', '') or '',
            row.get('TYPE', '') or '',
        )
        sub = row.get('SUBTYP', '') or ''
        pivot.setdefault(key, {})[sub] = (
            row.get('AMOUNT', 0) or 0,
            row.get('WACOST') or 0,
            row.get('WAREMM') or 0,
        )

    body_lines = []
    body_lines += page_header(rdate)
    body_lines += col_hdr

    prev_prodtyp = None
    prev_subttl  = None

    line_count = len(body_lines)

    for (prodtyp, subttl, remmth1, typ), submap in sorted(pivot.items()):
        # Page break check
        if line_count >= PAGE_LENGTH:
            body_lines += page_header(rdate)
            body_lines += col_hdr
            line_count = len(page_header(rdate)) + len(col_hdr)

        if prodtyp != prev_prodtyp:
            body_lines.append(f" {'':=<139}")
            body_lines.append(f" {prodtyp}")
            prev_prodtyp = prodtyp
            prev_subttl  = None
            line_count  += 2

        subttl_label = subttl_fmt(subttl)
        if subttl != prev_subttl:
            body_lines.append(f"   {subttl_label}")
            prev_subttl = subttl
            line_count += 1

        conv  = submap.get('CONVENTIONAL', (0, 0, 0))
        sptf  = submap.get('SPTF', (0, 0, 0))
        total = submap.get('TOTAL', (0, 0, 0))

        detail_line = (
            f"     {remmth1:<20} {typ:<17}"
            f" {fmt_num(conv[0])} {fmt_num(conv[1], dec=2)} {fmt_num(conv[2], 5, 2)}"
            f" {fmt_num(sptf[0])} {fmt_num(sptf[1], dec=2)} {fmt_num(sptf[2], 5, 2)}"
            f" {fmt_num(total[0])} {fmt_num(total[1], dec=2)} {fmt_num(total[2], 5, 2)}"
        )
        body_lines.append(detail_line)
        line_count += 1

    # Write output with ASA carriage control characters
    # The first line already starts with '1' (new page ASA character)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for ln in body_lines:
            f.write(ln + '\n')

    print(f"Report written to {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
