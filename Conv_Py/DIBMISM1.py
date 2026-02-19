# !/usr/bin/env python3
"""
Program : DIBMISM1
Report  : Additional Report Mudharabah Current Account
ESMR    : 2011-1214
          Generates a PROC TABULATE-style report of average Mudharabah
          CA balances grouped by deposit range (CATEGORY format).
          Runs for all dates (no REPTFQ check).
"""

# Dependency: PBMISFMT.py (format functions used for branch code mapping)
# from PBMISFMT import format_brchcd  # placeholder - see PBMISFMT.py

import os
import duckdb
import polars as pl
from datetime import date

# ===========================================================================
# PATH CONFIGURATION
# ===========================================================================
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR      = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR     = os.path.join(BASE_DIR, 'output')

REPTDATE_PARQUET      = os.path.join(INPUT_DIR, 'deposit_reptdate.parquet')
MIS_DYIBU_PARQUET_TMPL = os.path.join(INPUT_DIR, 'mis_dyibu{reptmon}.parquet')

OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'DIBMISM1.txt')

PAGE_LENGTH = 60
LINE_SIZE   = 132

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ===========================================================================
# FORMAT: CATEGORY  (deposit range -> label)
# LOW - < 5001     = 'UP TO RM5,000'
# 5001 - < 50001   = 'UP TO RM50,000'
# 50001 - < 100001 = 'UP TO RM100,000'
# 100001 - < 500001= 'UP TO RM500,000'
# 500001 - < 2000001= 'UP TO RM2000,000'
# 2000001 - < 3000001= 'UP TO RM3000,000'
# 3000001 - HIGH   = 'ABOVE RM3000,000'
# ===========================================================================

# Ordered category labels for display (PRELOADFMT order)
CATEGORY_ORDER = [
    'UP TO RM5,000',
    'UP TO RM50,000',
    'UP TO RM100,000',
    'UP TO RM500,000',
    'UP TO RM2000,000',
    'UP TO RM3000,000',
    'ABOVE RM3000,000',
]

def format_category(amount) -> str:
    """Map deposit amount to category label."""
    if amount is None:
        return 'UP TO RM5,000'
    v = float(amount)
    if v < 5001:    return 'UP TO RM5,000'
    if v < 50001:   return 'UP TO RM50,000'
    if v < 100001:  return 'UP TO RM100,000'
    if v < 500001:  return 'UP TO RM500,000'
    if v < 2000001: return 'UP TO RM2000,000'
    if v < 3000001: return 'UP TO RM3000,000'
    return              'ABOVE RM3000,000'


# ===========================================================================
# REPORT HELPERS
# ===========================================================================
def format_comma15_2(val) -> str:
    if val is None:
        return f"{'0.00':>15}"
    return f"{float(val):>15,.2f}"


def render_category_tabulate(data: pl.DataFrame, report_titles: list[str],
                              rdate: str) -> list[str]:
    """
    Render PROC TABULATE with PRINTMISS / PRELOADFMT:
    DEPOSIT RANGE | AVERAGE BALANCE
    All 7 categories are always printed (preloaded), even if zero.
    """
    # Aggregate by CATEGORY
    agg_map: dict[str, float] = {cat: 0.0 for cat in CATEGORY_ORDER}
    for row in data.iter_rows(named=True):
        cat = format_category(row['RANGE'])
        agg_map[cat] = agg_map.get(cat, 0.0) + (row['AVGAMT'] or 0.0)

    lines: list[str] = []
    for t in report_titles:
        lines.append(t.center(LINE_SIZE))
    lines.append(f"AS AT {rdate}".center(LINE_SIZE))
    lines.append('')

    # Header
    hdr = f"{'DEPOSIT RANGE':<30} {'AVERAGE BALANCE':>15}"
    lines.append(hdr)
    lines.append('-' * 50)

    for cat in CATEGORY_ORDER:
        val = agg_map.get(cat, 0.0)
        lines.append(f"{cat:<30} {format_comma15_2(val)}")

    lines.append('-' * 50)

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
    # OPTIONS YEARCUTOFF=1950 NOCENTER NODATE MISSING=0

    con = duckdb.connect()
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate: date = row[0] if isinstance(row[0], date) else row[0].date()
    reptmon  = f"{reptdate.month:02d}"
    rdate    = reptdate.strftime('%d/%m/%y')
    reptday  = reptdate.day

    mis_parquet = MIS_DYIBU_PARQUET_TMPL.format(reptmon=reptmon)

    con = duckdb.connect()
    raw = con.execute(f"SELECT * FROM read_parquet('{mis_parquet}')").df()
    con.close()

    df = pl.from_pandas(raw)

    # DATA MUDCA: WHERE CAI96 GT 0
    if 'CAI96' not in df.columns:
        df = df.with_columns(pl.lit(0.0).alias('CAI96'))
    if 'CA96' not in df.columns:
        df = df.with_columns(pl.lit(0.0).alias('CA96'))

    df = df.with_columns([
        pl.col('CAI96').fill_null(0),
        pl.col('CA96').fill_null(0),
    ])

    mudca = df.filter(pl.col('CAI96') > 0)

    # AVGAMT = CA96 / &REPTDAY
    # RANGE  = CA96
    mudca = mudca.with_columns([
        (pl.col('CA96') / reptday).alias('AVGAMT'),
        pl.col('CA96').alias('RANGE'),
    ])

    report_titles = [
        'REPORT ID : DIBMISM1',
        'PUBLIC BANK BERHAD ',
        'MUDHARABAH CURRENT ACCOUNT',
    ]

    all_lines: list[str] = []

    report_lines = render_category_tabulate(mudca, report_titles, rdate)
    all_lines.extend(report_lines)

    final_lines = with_asa(all_lines, PAGE_LENGTH)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(final_lines))
        f.write('\n')

    print(f"Report written to: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
