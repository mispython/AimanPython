#!/usr/bin/env python3
"""
Program : EIBBPBCR.py
Date    : 24.04.2000
Report  : 1. LISTING OF PB PREMIUM CLUB MEMBERS
          2. PROFILE ON PB PREMIUM CLUB
ESMR NO : 2006-1225
"""

import duckdb
import polars as pl
import pyarrow as pa
import sys
import os
from datetime import datetime, date

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR   = os.path.join(BASE_DIR, "input")
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Input parquet paths
REPTDATE_PATH = os.path.join(INPUT_DIR, "deposit_reptdate.parquet")
SAVING_PATH   = os.path.join(INPUT_DIR, "deposit_saving.parquet")
CURRENT_PATH  = os.path.join(INPUT_DIR, "deposit_current.parquet")

# Output report files (ASA carriage-control text reports)
OUTPUT_COLD1  = os.path.join(OUTPUT_DIR, "EIBBPBCR_COLD1.txt")   # Profile by deposit range
OUTPUT_COLD2  = os.path.join(OUTPUT_DIR, "EIBBPBCR_COLD2.txt")   # Profile by product

# ---------------------------------------------------------------------------
# DEPENDENCY: PBBELF – branch-code formatter
# ---------------------------------------------------------------------------
from PBBELF import format_brchcd

# ---------------------------------------------------------------------------
# FORMAT DEFINITIONS  (mirror SAS PROC FORMAT values)
# ---------------------------------------------------------------------------

SAPROD_MAP = {
    200: 'PLUS',
    201: 'STAFF',
    202: 'YAA',
    203: '50PLUS',
    204: 'AL-WADIAH',
    212: 'WISE',
    213: 'PB SAVELINK',
    214: 'MUDHARABAH BESTARI',
    156: 'PB CURRENTLINK',
    157: 'PB CURRENTLINK EXT',
    100: 'PLUS CURRENT',
    102: 'EXT CURRENT',
    150: 'ACE NORMAL',
    151: 'ACE STAFF',
    152: 'ACE EXTERNAL',
    160: 'AL-WADIAH CA',
    162: 'AL-WADIAH EXT',
}

def fmt_saprod(val) -> str:
    return SAPROD_MAP.get(val, 'UNKNOWN')


def fmt_exist(val) -> str:
    """EXIST format: 1 -> NEW, other -> EXISTING"""
    return 'NEW' if val == 1 else 'EXISTING'


def fmt_sapurp(val: str) -> str:
    """$SAPURP format"""
    if val == '1':
        return 'PERSONAL'
    if val == '2':
        return 'JOINT'
    if val in ('5', '6'):
        return 'ON-BEHALF'
    return 'UNKNOWN'


def fmt_race(val: str) -> str:
    """$RACE format"""
    if val == '1':
        return 'MALAY'
    if val == '2':
        return 'CHINESE'
    if val == '3':
        return 'INDIAN'
    return 'OTHERS'


def fmt_balrge(val) -> str:
    """BALRGE range format"""
    if val is None:
        return ''
    v = float(val)
    if v <= 30000:
        return ' RM   30,000 AND BELOW'
    if v <= 50000:
        return '>RM   30,000-RM   50,000'
    if v <= 75000:
        return '>RM   50,000-RM   75,000'
    if v <= 100000:
        return '>RM   75,000-RM  100,000'
    if v <= 250000:
        return '>RM  100,000-RM  250,000'
    if v <= 500000:
        return '>RM  250,000-RM  500,000'
    if v <= 750000:
        return '>RM  500,000-RM  750,000'
    if v <= 1000000:
        return '>RM  750,000-RM1,000,000'
    return '>RM1,000,000 & ABOVE'


# ---------------------------------------------------------------------------
# HELPER: ASA carriage-control line builder
# ---------------------------------------------------------------------------
PAGE_LENGTH = 60  # lines per page (body lines, excluding header)

def asa_line(cc: str, text: str) -> str:
    """Return a line with a leading ASA carriage-control character."""
    return f"{cc}{text}\n"


class ReportWriter:
    """Simple ASA carriage-control report writer."""

    def __init__(self, filepath: str, titles: list[str]):
        self.filepath   = filepath
        self.titles     = titles          # list of title strings (up to 4)
        self.lines: list[str] = []
        self.body_count = 0
        self.page_num   = 1
        self._emit_page_header()

    def _emit_page_header(self):
        # '1' = form-feed / new page
        self.lines.append(asa_line('1', self.titles[0] if len(self.titles) > 0 else ''))
        for t in self.titles[1:]:
            self.lines.append(asa_line(' ', t))
        self.lines.append(asa_line(' ', ''))   # blank line after titles
        self.body_count = 0

    def write(self, cc: str, text: str):
        """Write one body line; handle page breaks automatically."""
        if self.body_count >= PAGE_LENGTH:
            self.page_num += 1
            self._emit_page_header()
        self.lines.append(asa_line(cc, text))
        self.body_count += 1

    def blank(self, n: int = 1):
        for _ in range(n):
            self.write(' ', '')

    def save(self):
        with open(self.filepath, 'w', encoding='utf-8') as f:
            f.writelines(self.lines)


# ---------------------------------------------------------------------------
# READ REPORT DATE
# ---------------------------------------------------------------------------
def get_report_date() -> str:
    con = duckdb.connect()
    row = con.execute(f"SELECT reptdate FROM read_parquet('{REPTDATE_PATH}') LIMIT 1").fetchone()
    con.close()
    if row:
        d = row[0]
        if isinstance(d, (datetime, date)):
            return d.strftime('%d/%m/%Y')
        return str(d)
    return ''


# ---------------------------------------------------------------------------
# DATA PREPARATION
# ---------------------------------------------------------------------------
def prepare_data(rdate: str) -> pl.DataFrame:
    con = duckdb.connect()

    # ---- SAVING dataset ----
    # Keep: BRANCH ACCTNO NAME PURPOSE PRODUCT AVGAMT RACE LEDGBAL EXISTING
    # Filter: USER3 IN ('2','4','7') AND OPENIND NOT IN ('B','C','P','Z')
    # EXISTING=1 if OPENDT >= 18-NOV-1998
    savg_sql = f"""
        SELECT
            BRANCH,
            ACCTNO,
            NAME,
            PURPOSE,
            PRODUCT,
            AVGAMT,
            RACE,
            LEDGBAL,
            CASE
                WHEN TRY_CAST(SUBSTR(PRINTF('%011d', OPENDT), 1, 8) AS INTEGER) >=
                     CAST(strftime('%m%d%Y', '1998-11-18') AS INTEGER)
                THEN 1
                ELSE 0
            END AS EXISTING
        FROM read_parquet('{SAVING_PATH}')
        WHERE USER3 IN ('2','4','7')
          AND OPENIND NOT IN ('B','C','P','Z')
    """

    # ---- CURRENT dataset ----
    # Filter: PRODUCT IN (...) & USER3 IN ('2','4','7') AND OPENIND NOT IN ('B','C','P')
    curn_sql = f"""
        SELECT
            BRANCH,
            ACCTNO,
            NAME,
            PURPOSE,
            PRODUCT,
            AVGAMT,
            RACE,
            LEDGBAL,
            CASE
                WHEN TRY_CAST(SUBSTR(PRINTF('%011d', OPENDT), 1, 8) AS INTEGER) >=
                     CAST(strftime('%m%d%Y', '1998-11-18') AS INTEGER)
                THEN 1
                ELSE 0
            END AS EXISTING
        FROM read_parquet('{CURRENT_PATH}')
        WHERE PRODUCT IN (150,151,152,100,102,156,157,160,162)
          AND USER3 IN ('2','4','7')
          AND OPENIND NOT IN ('B','C','P')
    """

    savg = con.execute(savg_sql).pl()
    curn = con.execute(curn_sql).pl()
    con.close()

    # Combine SAVG + CURN (SAS SET appends rows)
    combined = pl.concat([savg, curn], how="diagonal")

    # Add RANGE and formatted BRCH column, rename BRCH -> BRANCH (drop old BRANCH)
    combined = combined.with_columns([
        pl.col("LEDGBAL").map_elements(fmt_balrge, return_dtype=pl.Utf8).alias("RANGE"),
        pl.struct(["BRANCH"]).map_elements(
            lambda r: f"{r['BRANCH']:03d}/{format_brchcd(r['BRANCH'])}",
            return_dtype=pl.Utf8
        ).alias("BRCH"),
    ]).drop("BRANCH").rename({"BRCH": "BRANCH"})

    # Sort by BRANCH, NAME
    combined = combined.sort(["BRANCH", "NAME"])

    return combined


# ---------------------------------------------------------------------------
# REPORT 1 (commented out in SAS – retained but commented)
# ---------------------------------------------------------------------------
# The SAS code has the two PROC PRINT blocks wrapped in /* ... */
# so they are intentionally suppressed.  They are preserved here as a
# reference but will NOT be executed.
#
# def report_listing(df: pl.DataFrame, rdate: str):
#     """
#     LISTING OF PB PREMIUM CLUB MEMBERS AS AT <rdate>
#     Two sub-reports:
#       (a) YTD AVERAGE BALANCE OF RM30K & ABOVE
#       (b) YTD AVERAGE BALANCE BELOW RM30K
#     (Suppressed in original SAS – kept as placeholder)
#     """
#     pass


# ---------------------------------------------------------------------------
# REPORT 2 – PROFILE ON PB PREMIUM CLUB (COLD1)
# ---------------------------------------------------------------------------
RANGE_ORDER = [
    ' RM   30,000 AND BELOW',
    '>RM   30,000-RM   50,000',
    '>RM   50,000-RM   75,000',
    '>RM   75,000-RM  100,000',
    '>RM  100,000-RM  250,000',
    '>RM  250,000-RM  500,000',
    '>RM  500,000-RM  750,000',
    '>RM  750,000-RM1,000,000',
    '>RM1,000,000 & ABOVE',
]

PURPOSE_LABELS = ['PERSONAL', 'JOINT', 'ON-BEHALF', 'UNKNOWN']
RACE_LABELS    = ['MALAY', 'CHINESE', 'INDIAN', 'OTHERS']


def report_cold1(df: pl.DataFrame, rdate: str):
    """
    PROC TABULATE equivalent – profile by deposit range.
    TABLE RANGE ALL, LEDGBAL*(N PCTN SUM PCTSUM MEAN) N*(PURPOSE RACE)
    """
    title1 = (
        f"REPORT NO : EIBBPBCR"
        f"         PUBLIC BANK BERHAD - PROFILE ON PB PREMIUM CLUB AT AT {rdate}"
    )
    titles = [title1, 'PROGRAM   :', 'BRANCH NO :', '']

    rw = ReportWriter(OUTPUT_COLD1, titles)

    # Derive formatted columns
    work = df.with_columns([
        pl.col("PURPOSE").map_elements(fmt_sapurp, return_dtype=pl.Utf8).alias("PURPOSE_FMT"),
        pl.col("RACE").map_elements(fmt_race,    return_dtype=pl.Utf8).alias("RACE_FMT"),
    ])

    total_n   = len(work)
    total_sum = work["LEDGBAL"].sum() or 0.0

    # ---- header row ----
    col_w = 35
    hdr_left  = f"{'DEPOSIT RANGE':<{col_w}}"
    hdr_cols  = (
        f"{'NO OF ACCOUNT':>13}"
        f"{'%':>8}"
        f"{'O/S BALANCE':>18}"
        f"{'%':>8}"
        f"{'AVERAGE AMOUNT PER A/C':>22}"
    )
    # PURPOSE sub-cols
    purp_hdr = "".join(f"{p:>12}" for p in PURPOSE_LABELS)
    race_hdr = "".join(f"{r:>10}" for r in RACE_LABELS)

    sep = '-' * 120

    rw.write(' ', sep)
    rw.write(' ', f"{hdr_left}{hdr_cols}  {'ACCOUNT TYPE':>48}  {'RACE':>40}")
    rw.write(' ', f"{'':<{col_w}}{'':>69}  {purp_hdr}  {race_hdr}")
    rw.write(' ', sep)

    def row_stats(subset: pl.DataFrame):
        n    = len(subset)
        pct_n  = (n / total_n * 100) if total_n else 0.0
        s    = subset["LEDGBAL"].sum() or 0.0
        pct_s  = (s / total_sum * 100) if total_sum else 0.0
        mean = (s / n) if n else 0.0
        return n, pct_n, s, pct_s, mean

    def purpose_counts(subset: pl.DataFrame) -> list[int]:
        grp = subset.group_by("PURPOSE_FMT").agg(pl.len().alias("cnt"))
        d = dict(zip(grp["PURPOSE_FMT"].to_list(), grp["cnt"].to_list()))
        return [d.get(p, 0) for p in PURPOSE_LABELS]

    def race_counts(subset: pl.DataFrame) -> list[int]:
        grp = subset.group_by("RACE_FMT").agg(pl.len().alias("cnt"))
        d = dict(zip(grp["RACE_FMT"].to_list(), grp["cnt"].to_list()))
        return [d.get(r, 0) for r in RACE_LABELS]

    for rng in RANGE_ORDER:
        subset = work.filter(pl.col("RANGE") == rng)
        if len(subset) == 0:
            continue
        n, pct_n, s, pct_s, mean = row_stats(subset)
        pcnts = purpose_counts(subset)
        rcnts = race_counts(subset)
        purp_str = "".join(f"{c:>12,}" for c in pcnts)
        race_str = "".join(f"{c:>10,}" for c in rcnts)
        line = (
            f"{rng:<{col_w}}"
            f"{n:>13,}"
            f"{pct_n:>8.2f}"
            f"{s:>18,.2f}"
            f"{pct_s:>8.2f}"
            f"{mean:>22,.2f}"
            f"  {purp_str}  {race_str}"
        )
        rw.write(' ', line)

    # TOTAL row
    rw.write(' ', sep)
    n, pct_n, s, pct_s, mean = row_stats(work)
    pcnts = purpose_counts(work)
    rcnts = race_counts(work)
    purp_str = "".join(f"{c:>12,}" for c in pcnts)
    race_str = "".join(f"{c:>10,}" for c in rcnts)
    total_line = (
        f"{'TOTAL':<{col_w}}"
        f"{n:>13,}"
        f"{pct_n:>8.2f}"
        f"{s:>18,.2f}"
        f"{pct_s:>8.2f}"
        f"{mean:>22,.2f}"
        f"  {purp_str}  {race_str}"
    )
    rw.write(' ', total_line)
    rw.write(' ', sep)

    rw.save()
    print(f"[EIBBPBCR] COLD1 report written -> {OUTPUT_COLD1}")


# ---------------------------------------------------------------------------
# REPORT 3 – PROFILE ON PB PREMIUM CLUB BY PRODUCT (COLD2)
# ---------------------------------------------------------------------------
PRODUCT_ORDER = [
    'PLUS', 'STAFF', 'YAA', '50PLUS', 'AL-WADIAH', 'WISE',
    'PB SAVELINK', 'MUDHARABAH BESTARI', 'PB CURRENTLINK', 'PB CURRENTLINK EXT',
    'PLUS CURRENT', 'EXT CURRENT', 'ACE NORMAL', 'ACE STAFF', 'ACE EXTERNAL',
    'AL-WADIAH CA', 'AL-WADIAH EXT', 'UNKNOWN',
]


def report_cold2(df: pl.DataFrame, rdate: str):
    """
    PROC TABULATE equivalent – profile by product & existing/new.
    TABLE PRODUCT ALL, LEDGBAL*(N*(EXISTING) N PCTN SUM*(EXISTING) SUM PCTSUM MEAN)
    """
    title1 = (
        f"REPORT NO : EIBBPBCR        PBB - PROFILE ON PB PREMIUM CLUB"
        f" BY PRODUCT AS AT {rdate}"
    )
    titles = [title1, 'PROGRAM   :', 'BRANCH NO :', '']

    rw = ReportWriter(OUTPUT_COLD2, titles)

    work = df.with_columns([
        pl.col("PRODUCT").map_elements(fmt_saprod,  return_dtype=pl.Utf8).alias("PRODUCT_FMT"),
        pl.col("EXISTING").map_elements(fmt_exist,  return_dtype=pl.Utf8).alias("EXIST_FMT"),
    ])

    total_n   = len(work)
    total_sum = work["LEDGBAL"].sum() or 0.0

    EXIST_CATS = ['EXISTING', 'NEW']
    col_w = 18

    # Header
    sep = '-' * 140
    exist_n_hdr   = "".join(f"{e:>14}" for e in EXIST_CATS)
    exist_s_hdr   = "".join(f"{e:>18}" for e in EXIST_CATS)

    rw.write(' ', sep)
    rw.write(' ', (
        f"{'TYPE OF ACCOUNT':<{col_w}}"
        f"{'NO OF ACCOUNT':>28}"
        f"{'TOTAL NO':>9}"
        f"{'%':>8}"
        f"{'O/S AMOUNT':>36}"
        f"{'TOTAL O/S':>18}"
        f"{'%':>8}"
        f"{'AVG AMT PER A/C':>16}"
    ))
    rw.write(' ', (
        f"{'':>{col_w}}"
        f"{exist_n_hdr}"
        f"{'OF ACCOUNT':>9}"
        f"{'':>8}"
        f"{exist_s_hdr}"
        f"{'AMOUNT':>18}"
        f"{'':>8}"
        f"{'':>16}"
    ))
    rw.write(' ', sep)

    def prod_row(subset: pl.DataFrame):
        n       = len(subset)
        pct_n   = (n / total_n  * 100) if total_n  else 0.0
        s       = subset["LEDGBAL"].sum() or 0.0
        pct_s   = (s / total_sum * 100) if total_sum else 0.0
        mean    = (s / n) if n else 0.0
        # counts by exist category
        grp_n = subset.group_by("EXIST_FMT").agg(pl.len().alias("cnt"))
        nd    = dict(zip(grp_n["EXIST_FMT"].to_list(), grp_n["cnt"].to_list()))
        # sums by exist category
        grp_s = subset.group_by("EXIST_FMT").agg(pl.col("LEDGBAL").sum().alias("sm"))
        sd    = dict(zip(grp_s["EXIST_FMT"].to_list(), grp_s["sm"].to_list()))
        n_cats = [nd.get(e, 0) for e in EXIST_CATS]
        s_cats = [sd.get(e, 0.0) or 0.0 for e in EXIST_CATS]
        return n, pct_n, s, pct_s, mean, n_cats, s_cats

    for prod in PRODUCT_ORDER:
        subset = work.filter(pl.col("PRODUCT_FMT") == prod)
        if len(subset) == 0:
            continue
        n, pct_n, s, pct_s, mean, n_cats, s_cats = prod_row(subset)
        n_str = "".join(f"{c:>14,}" for c in n_cats)
        s_str = "".join(f"{v:>18,.2f}" for v in s_cats)
        line = (
            f"{prod:<{col_w}}"
            f"{n_str}"
            f"{n:>9,}"
            f"{pct_n:>8.2f}"
            f"{s_str}"
            f"{s:>18,.2f}"
            f"{pct_s:>8.2f}"
            f"{mean:>16,.2f}"
        )
        rw.write(' ', line)

    # TOTAL row
    rw.write(' ', sep)
    n, pct_n, s, pct_s, mean, n_cats, s_cats = prod_row(work)
    n_str = "".join(f"{c:>14,}" for c in n_cats)
    s_str = "".join(f"{v:>18,.2f}" for v in s_cats)
    total_line = (
        f"{'TOTAL':<{col_w}}"
        f"{n_str}"
        f"{n:>9,}"
        f"{pct_n:>8.2f}"
        f"{s_str}"
        f"{s:>18,.2f}"
        f"{pct_s:>8.2f}"
        f"{mean:>16,.2f}"
    )
    rw.write(' ', total_line)
    rw.write(' ', sep)

    rw.save()
    print(f"[EIBBPBCR] COLD2 report written -> {OUTPUT_COLD2}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    rdate = get_report_date()
    print(f"[EIBBPBCR] Report date: {rdate}")

    df = prepare_data(rdate)
    print(f"[EIBBPBCR] Records loaded: {len(df)}")

    # Report 1 (LISTING) is intentionally commented out in the original SAS program
    # report_listing(df, rdate)

    report_cold1(df, rdate)
    report_cold2(df, rdate)

    print("[EIBBPBCR] Done.")


if __name__ == "__main__":
    main()
