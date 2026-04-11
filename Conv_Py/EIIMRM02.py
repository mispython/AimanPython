#!/usr/bin/env python3
"""
Program : EIIMRM02.py
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
INPUT_DIR  = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FD_PARQUET    = INPUT_DIR / "fd.parquet"
REPTDATE_PARQUET = INPUT_DIR / "reptdate.parquet"
OUTPUT_FILE   = OUTPUT_DIR / "EIIMRM02.txt"

# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

# REMFMT: integer REMMTH -> label string
def remfmt(val):
    """Maps remaining months integer to display label."""
    if val is None:
        return ''
    if isinstance(val, float):
        iv = int(val)
    else:
        iv = val
    mapping = {
        1:  '  1 MONTH',
        2:  '  2 MONTHS',
        3:  '  3 MONTHS',
        4:  '  4 MONTHS',
        5:  '  5 MONTHS',
        6:  '  6 MONTHS',
        7:  '  7 MONTHS',
        8:  '  8 MONTHS',
        9:  '  9 MONTHS',
        10: ' 10 MONTHS',
        11: ' 11 MONTHS',
        12: ' 12 MONTHS',
        13: ' 13 MONTHS',
        14: ' 14 MONTHS',
        15: ' 15 MONTHS',
        16: ' 16 MONTHS',
        17: ' 17 MONTHS',
        18: ' 18 MONTHS',
        19: ' 19 MONTHS',
        20: ' 20 MONTHS',
        21: ' 21 MONTHS',
        22: ' 22 MONTHS',
        23: ' 23 MONTHS',
        24: ' 24 MONTHS',
        91: ' 1 MONTH',
        92: ' 3 MONTHS',
        93: ' 6 MONTHS',
        94: ' 9 MONTHS',
        95: '12 MONTHS',
        96: '15 MONTHS',
        97: 'ABOVE 15 MONTHS',
        99: 'OVERDUE FD',
    }
    if iv in mapping:
        return mapping[iv]
    fval = float(val)
    if 24 < fval <= 36:
        return '>2-3 YRS  '
    if 36 < fval <= 48:
        return '>3-4 YRS  '
    if 48 < fval <= 60:
        return '>4-5 YRS  '
    return ''


# $SUBTTL format
SUBTTL_MAP = {
    'A': 'ORIGINAL MATURITY',
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

# Days in month arrays (index 1-12), default 31; Apr/Jun/Sep/Nov = 30; Feb = 28 (leap handled dynamically)
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

    # cap maturity day to report-month days
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

    keep_cols = ['PRODTYP', 'SUBTYP', 'SUBTTL', 'REMMTH', 'TERM',
                 'AMOUNT', 'COST', 'MATDT', 'INTDATE', 'INTPLAN',
                 'OPENIND', 'REPTDATE', 'TYPE', 'CUSTCD', 'ORIGIN']

    for row in df.iter_rows(named=True):
        intplan = row.get('INTPLAN')
        curbal  = row.get('CURBAL', 0) or 0
        rate    = row.get('RATE', 0) or 0
        openind = row.get('OPENIND', '')
        custcd  = row.get('CUSTCD')
        matdate = row.get('MATDATE')
        intdate = row.get('INTDATE')

        bnmcode = fdprod_format(intplan)

        if bnmcode == '42630':
            prodtyp = 'FIXED DEPT(FCY)'
            term = termfmt(intplan)
        else:
            prodtyp = 'FIXED DEPT(RM)'
            term = None

        # TYPE
        if custcd in (76, 77, 78, 95, 96):
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
                subttl = 'B'
                remmth = 99
                subtyp = 'SPTF' if bnmcode == '42132' else 'CONVENTIONAL'
                cost   = curbal * rate
                origin = curbal * remmth
                td_rows.append({**base, 'SUBTTL': subttl, 'SUBTYP': subtyp,
                                 'REMMTH': remmth, 'TERM': term,
                                 'COST': cost, 'ORIGIN': origin})
            else:
                remmth_val = calc_remmth(matdt, rp_yr, rp_mth, rp_day)
                remmt1     = remmth_val
                remmth_out = term  # REMMTH = TERM in SAS OUTPUT FD

                subtyp = 'SPTF' if bnmcode == '42132' else 'CONVENTIONAL'
                cost   = curbal * rate
                origin = curbal * (remmth_out if remmth_out is not None else 0)
                fd_rows.append({**base, 'SUBTTL': 'A', 'SUBTYP': subtyp,
                                 'REMMTH': remmth_out, 'TERM': term,
                                 'COST': cost, 'ORIGIN': origin})

                if term is not None and (term - remmt1) < 1:
                    cost2   = curbal * rate
                    origin2 = curbal * (remmth_out if remmth_out is not None else 0)
                    fdn_rows.append({**base, 'SUBTTL': 'C', 'SUBTYP': subtyp,
                                      'REMMTH': remmth_out, 'TERM': term,
                                      'COST': cost2, 'ORIGIN': origin2})

    def make_df(rows):
        if not rows:
            return pl.DataFrame(schema={c: pl.Utf8 for c in keep_cols})
        return pl.DataFrame(rows)

    return make_df(td_rows), make_df(fd_rows), make_df(fdn_rows)


# ============================================================================
# SUMMARY
# ============================================================================

def summarise(df: pl.DataFrame) -> pl.DataFrame:
    """Group by CLASS variables and sum VAR columns."""
    if df.is_empty():
        return df
    group_cols = ['TYPE', 'PRODTYP', 'SUBTTL', 'REMMTH', 'SUBTYP']
    existing = [c for c in group_cols if c in df.columns]
    return (
        df.group_by(existing)
          .agg([
              pl.col('AMOUNT').sum(),
              pl.col('COST').sum(),
              pl.col('ORIGIN').sum(),
          ])
    )


# ============================================================================
# BUILD DUMMY ROWS (1-60 months scaffold)
# ============================================================================

def build_dummy(dep: pl.DataFrame) -> pl.DataFrame:
    """
    For each unique (PRODTYP, SUBTTL, SUBTYP, TYPE) combination where SUBTTL in ('A','C'),
    generate rows for REMMTH 1..60, compute REMMTH1.
    """
    filtered = dep.filter(pl.col('SUBTTL').is_in(['A', 'C']))
    if filtered.is_empty():
        return pl.DataFrame()

    keys = filtered.select(['PRODTYP', 'SUBTTL', 'SUBTYP', 'TYPE']).unique()
    dummy_rows = []
    for row in keys.iter_rows(named=True):
        for rm in range(1, 61):
            dummy_rows.append({**row, 'REMMTH': rm, 'REMMTH1': remfmt(rm)})

    dummy = pl.DataFrame(dummy_rows)
    # NODUPKEYS on (PRODTYP, SUBTTL, SUBTYP, TYPE, REMMTH1)
    dummy = dummy.unique(subset=['PRODTYP', 'SUBTTL', 'SUBTYP', 'TYPE', 'REMMTH1'])
    return dummy


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    reptdate, nowk, reptyrs, reptyear, reptmon, reptday, rdate = load_reptdate()

    td_raw, fd_raw, fdn_raw = process_fd(reptdate)

    # Summarise each
    td_sum  = summarise(td_raw)
    fd_sum  = summarise(fd_raw)
    fdn_sum = summarise(fdn_raw)

    # Combine
    dep = pl.concat([td_sum, fd_sum, fdn_sum], how='diagonal')

    # Add REMMTH1 label
    dep = dep.with_columns(
        pl.col('REMMTH').map_elements(remfmt, return_dtype=pl.Utf8).alias('REMMTH1')
    )

    # Sort
    dep = dep.sort(['PRODTYP', 'SUBTTL', 'SUBTYP', 'TYPE', 'REMMTH1'])

    # Build dummy scaffold and merge (left join)
    dummy = build_dummy(dep)
    if not dummy.is_empty():
        dep = dep.join(
            dummy.select(['PRODTYP', 'SUBTTL', 'SUBTYP', 'TYPE', 'REMMTH1']),
            on=['PRODTYP', 'SUBTTL', 'SUBTYP', 'TYPE', 'REMMTH1'],
            how='outer'
        ).fill_null(0)

    # Compute WACOST, WAORIG, round AMOUNT
    def safe_div(num, denom):
        if denom and denom != 0:
            return num / denom
        return None

    dep = dep.with_columns([
        pl.when((pl.col('SUBTYP').is_in(['SPTF', 'CONVENTIONAL'])) & (pl.col('AMOUNT') != 0))
          .then(pl.col('COST') / pl.col('AMOUNT'))
          .otherwise(None)
          .alias('WACOST'),
        pl.when((pl.col('SUBTYP').is_in(['SPTF', 'CONVENTIONAL'])) & (pl.col('AMOUNT') != 0))
          .then(pl.col('ORIGIN') / pl.col('AMOUNT'))
          .otherwise(None)
          .alias('WAORIG'),
        (pl.col('AMOUNT') / 1000).round(0).alias('AMOUNT'),
    ])

    # SUBTYPE TOTAL (class: TYPE PRODTYP SUBTTL REMMTH1)
    dep_total = (
        dep.group_by(['TYPE', 'PRODTYP', 'SUBTTL', 'REMMTH1'])
           .agg([pl.col('AMOUNT').sum(), pl.col('COST').sum(), pl.col('ORIGIN').sum()])
    )
    dep_total = dep_total.with_columns([
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('COST') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WACOST'),
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('ORIGIN') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WAORIG'),
        pl.lit('TOTAL').alias('SUBTYP'),
    ])

    # Sort dep
    dep = dep.sort(['TYPE', 'PRODTYP', 'SUBTTL', 'REMMTH1'])

    # TYPE TOTAL (class: SUBTYP PRODTYP SUBTTL REMMTH1)
    dep_tota2 = (
        dep.group_by(['SUBTYP', 'PRODTYP', 'SUBTTL', 'REMMTH1'])
           .agg([pl.col('AMOUNT').sum(), pl.col('COST').sum(), pl.col('ORIGIN').sum()])
    )
    dep_tota2 = dep_tota2.with_columns([
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('COST') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WACOST'),
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('ORIGIN') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WAORIG'),
        pl.lit('TOTAL').alias('TYPE'),
    ])

    depfinal = pl.concat([dep, dep_total, dep_tota2], how='diagonal')

    # SUB-TOTAL (class: PRODTYP SUBTTL SUBTYP TYPE)
    dep_tota2b = (
        depfinal.group_by(['PRODTYP', 'SUBTTL', 'SUBTYP', 'TYPE'])
                .agg([pl.col('AMOUNT').sum(), pl.col('COST').sum(), pl.col('ORIGIN').sum()])
    )
    dep_tota2b = dep_tota2b.with_columns([
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('COST') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WACOST'),
        pl.when((pl.col('AMOUNT') * 1000).round(0) != 0)
          .then(pl.col('ORIGIN') / ((pl.col('AMOUNT') * 1000).round(0)))
          .otherwise(None)
          .alias('WAORIG'),
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

def produce_report(depfinal: pl.DataFrame, rdate: str):
    lines = []
    page  = 1

    def header():
        hdr = []
        hdr.append(f"1PUBLIC ISLAMIC BANK BERHAD")
        hdr.append(f" TIME TO MATURITY AS AT {rdate}")
        hdr.append(f" RISK MANAGEMENT REPORT : EIIMRM02")
        hdr.append(f" RM DENOMINATION")
        hdr.append(f" ")
        return hdr

    def col_header():
        ch = []
        ch.append(" " + "-" * 139)
        ch.append(
            f" {'DEPOSITS':<65}"
            f"{'CONVENTIONAL':>20}"
            f"{'':>10}"
            f"{'SPTF':>20}"
            f"{'':>10}"
            f"{'TOTAL':>15}"
        )
        ch.append(
            f" {'':<65}"
            f"{'BAL OUSTANDING':>14}{'W.A. COST %':>12}{'REMAINING MATURITY':>12}"
            f"{'BAL OUSTANDING':>14}{'W.A. COST %':>12}{'REMAINING MATURITY':>12}"
            f"{'BAL OUSTANDING':>14}{'W.A. COST %':>12}{'REMAINING MATURITY':>12}"
        )
        ch.append(
            f" {'':<65}"
            f"{'(RM''000)':>14}{'':>12}{'':>12}"
            f"{'(RM''000)':>14}{'':>12}{'':>12}"
            f"{'(RM''000)':>14}{'':>12}{'':>12}"
        )
        return ch

    # Sort for output
    depfinal_sorted = depfinal.sort(
        ['PRODTYP', 'SUBTTL', 'REMMTH1', 'SUBTYP', 'TYPE'],
        nulls_last=True
    )

    # Build pivot: for each (PRODTYP, SUBTTL, REMMTH1, TYPE) -> {SUBTYP: (AMOUNT, WACOST, WAORIG)}
    # Report mimics PROC TABULATE output with CONDENSE option

    current_page_lines = header()
    current_page_lines += col_header()
    body_lines_on_page = len(current_page_lines)

    def flush_page(pg_lines, page_num):
        lines.extend(pg_lines)

    # Group display
    prev_prodtyp = None
    prev_subttl  = None
    prev_remmth1 = None

    # Pivot the data
    pivot = {}
    for row in depfinal_sorted.iter_rows(named=True):
        key = (
            row.get('PRODTYP', ''),
            row.get('SUBTTL', ''),
            row.get('REMMTH1', ''),
            row.get('TYPE', ''),
        )
        sub = row.get('SUBTYP', '')
        pivot.setdefault(key, {})[sub] = (
            row.get('AMOUNT', 0) or 0,
            row.get('WACOST') or 0,
            row.get('WAORIG') or 0,
        )

    def fmt_num(v, width=12, dec=0):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return ' ' * width
        if dec == 0:
            return f"{v:>{width},.0f}"
        return f"{v:>{width},.{dec}f}"

    report_lines = []
    report_lines += header()
    report_lines += col_header()

    subttls_seen = {}
    prev_prodtyp = None

    for (prodtyp, subttl, remmth1, typ), submap in sorted(pivot.keys().__class__(pivot.items()) if False else pivot.items()):
        if prodtyp != prev_prodtyp:
            report_lines.append(f" {'':=<139}")
            report_lines.append(f" {prodtyp}")
            prev_prodtyp = prodtyp
            prev_subttl = None

        subttl_label = subttl_fmt(subttl)
        if subttl != prev_subttl:
            report_lines.append(f"   {subttl_label}")
            prev_subttl = subttl

        conv  = submap.get('CONVENTIONAL', (0, 0, 0))
        sptf  = submap.get('SPTF', (0, 0, 0))
        total = submap.get('TOTAL', (0, 0, 0))

        report_lines.append(
            f"     {remmth1:<20} {typ:<17}"
            f" {fmt_num(conv[0])} {fmt_num(conv[1], dec=2)} {fmt_num(conv[2], 5, 2)}"
            f" {fmt_num(sptf[0])} {fmt_num(sptf[1], dec=2)} {fmt_num(sptf[2], 5, 2)}"
            f" {fmt_num(total[0])} {fmt_num(total[1], dec=2)} {fmt_num(total[2], 5, 2)}"
        )

    # Write output with ASA carriage control characters
    page_num = 1
    body = []
    for i, ln in enumerate(report_lines):
        if i == 0:
            # First line of first page: '1' = new page
            body.append(ln)
        else:
            body.append(ln)
        if (i + 1) % PAGE_LENGTH == 0 and i + 1 < len(report_lines):
            # New page: insert '1' ASA character at start of next page's first line
            body.append("1" + " " * 10 + "PUBLIC ISLAMIC BANK BERHAD")
            body.append(f" TIME TO MATURITY AS AT {rdate}")
            body.append(f" RISK MANAGEMENT REPORT : EIIMRM02")
            body.append(f" RM DENOMINATION")
            body.append(f" ")
            for hln in col_header():
                body.append(hln)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for ln in body:
            f.write(ln + '\n')

    print(f"Report written to {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
