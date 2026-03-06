#!/usr/bin/env python3
"""
Program : EIFMLN03.py
Date    : 22.05.99
Report  : WEIGHTED AVERAGE LENDING RATE ON OUTSTANDING,
          PRESCRIBED & NON-PRESCRIBED LOANS (RDIR II)
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
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Input parquet paths
REPTDATE_PATH = os.path.join(INPUT_DIR, "bnm_reptdate.parquet")
SDESC_PATH    = os.path.join(INPUT_DIR, "bnm_sdesc.parquet")
# BNM.LOAN{REPTMON}{NOWK} – resolved at runtime from REPTDATE
LOAN_DIR      = INPUT_DIR   # loan parquet files sit in the same input directory

# Output report file (ASA carriage-control text)
OUTPUT_REPORT = os.path.join(OUTPUT_DIR, "EIFMLN03_REPORT.txt")

# ---------------------------------------------------------------------------
# DEPENDENCY: PBBELF – branch-code formatter
# ---------------------------------------------------------------------------
from PBBELF import format_brchcd

# ---------------------------------------------------------------------------
# CONSTANTS  (mirroring SAS OPTIONS / hard-coded values)
# ---------------------------------------------------------------------------
NOWK     = '4'       # CALL SYMPUT('NOWK', PUT('4',$1.)) – hard-coded literal
PAGE_LENGTH = 60     # lines per page (ASA carriage control)

# ---------------------------------------------------------------------------
# ASA REPORT WRITER
# ---------------------------------------------------------------------------

def asa_line(cc: str, text: str) -> str:
    return f"{cc}{text}\n"


class ReportWriter:
    """Simple ASA carriage-control report writer with automatic page breaks."""

    def __init__(self, filepath: str, titles: list[str]):
        self.filepath   = filepath
        self.titles     = titles
        self.lines: list[str] = []
        self.body_count = 0
        self.page_num   = 1
        self._emit_page_header()

    def _emit_page_header(self):
        # '1' = form-feed / new page
        self.lines.append(asa_line('1', self.titles[0] if self.titles else ''))
        for t in self.titles[1:]:
            self.lines.append(asa_line(' ', t))
        self.lines.append(asa_line(' ', ''))   # blank line after titles
        self.body_count = 0

    def write(self, cc: str, text: str):
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
# READ REPORT DATE & DESCRIPTOR
# ---------------------------------------------------------------------------

def get_meta() -> tuple[str, str, str]:
    """
    Returns (reptmon_str, rdate_str, sdesc_str).
    NOWK is hard-coded as '4' per the SAS CALL SYMPUT.
    REPTMON -> zero-padded 2-digit month (e.g. '03').
    RDATE   -> WORDDATX18 equivalent (e.g. '22 May          1999').
    SDESC   -> 26-char padded descriptor string.
    """
    con = duckdb.connect()

    # reptdate
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{REPTDATE_PATH}') LIMIT 1"
    ).fetchone()
    if not row:
        con.close()
        raise RuntimeError("No row found in bnm_reptdate")

    reptdate_val = row[0]
    if isinstance(reptdate_val, (datetime, date)):
        d = reptdate_val if isinstance(reptdate_val, date) else reptdate_val.date()
    else:
        d = datetime.strptime(str(reptdate_val), '%Y-%m-%d').date()

    reptmon = f"{d.month:02d}"

    # WORDDATX18: e.g. "22 May          1999"  (day  month_abbr padded  year)
    month_name = d.strftime('%B')           # full month name
    rdate = f"{d.day} {month_name:<14} {d.year}"   # width ~18

    # sdesc
    row2 = con.execute(
        f"SELECT sdesc FROM read_parquet('{SDESC_PATH}') LIMIT 1"
    ).fetchone()
    sdesc = f"{(row2[0] if row2 else ''):<26}"

    con.close()
    return reptmon, rdate, sdesc.strip()


# ---------------------------------------------------------------------------
# DATA PREPARATION: LOAN dataset
# ---------------------------------------------------------------------------

def prepare_loan(reptmon: str) -> pl.DataFrame:
    """
    Reads BNM.LOAN{REPTMON}{NOWK}, filters on PRODCD='34111' and LOANSTAT=1,
    computes APR and WAMT per loan, then summarises by BRANCH.
    """
    loan_path = os.path.join(LOAN_DIR, f"bnm_loan{reptmon}{NOWK}.parquet")

    con = duckdb.connect()

    # Read raw loan rows
    raw_sql = f"""
        SELECT
            BRANCH,
            BALANCE,
            NOTETERM,
            INTRATE
        FROM read_parquet('{loan_path}')
        WHERE PRODCD = '34111'
          AND LOANSTAT = 1
    """
    loan_raw = con.execute(raw_sql).pl()
    con.close()

    if loan_raw.is_empty():
        return pl.DataFrame(schema={
            "BRANCH":  pl.Int64,
            "BRCH":    pl.Utf8,
            "BALANCE": pl.Float64,
            "WAMT":    pl.Float64,
            "WAVRATE": pl.Float64,
            "TYPE":    pl.Utf8,
        })

    # Compute TERM, TRATE, APR, WAMT
    # TERM  = min(NOTETERM, 12)
    # TRATE = NOTETERM * INTRATE
    # APR   = TRATE * (300*TERM + TRATE) / (NOTETERM*TRATE + 150*TERM*(NOTETERM+1))
    # WAMT  = BALANCE * APR
    # (The commented-out SAS formula includes *12/TERM – kept commented below)
    loan_raw = loan_raw.with_columns([
        pl.when(pl.col("NOTETERM") > 12)
          .then(pl.lit(12))
          .otherwise(pl.col("NOTETERM"))
          .alias("TERM"),
        (pl.col("NOTETERM") * pl.col("INTRATE")).alias("TRATE"),
    ]).with_columns([
        (
            pl.col("TRATE") * (300 * pl.col("TERM") + pl.col("TRATE")) /
            (pl.col("NOTETERM") * pl.col("TRATE") + 150 * pl.col("TERM") * (pl.col("NOTETERM") + 1))
            # SAS commented-out variant:
            # * 12 / pl.col("TERM")
        ).alias("APR"),
    ]).with_columns([
        (pl.col("BALANCE") * pl.col("APR")).alias("WAMT"),
    ])

    # Summarise by BRANCH  (PROC SUMMARY NWAY SUM)
    loan_sum = (
        loan_raw
        .group_by("BRANCH")
        .agg([
            pl.col("BALANCE").sum().alias("BALANCE"),
            pl.col("WAMT").sum().alias("WAMT"),
        ])
        .sort("BRANCH")
    )

    # BRHNO = BRANCH (kept as numeric reference)
    # BRCH  = PUT(BRANCH, BRCHCD.) – branch name via PBBELF format_brchcd
    # WAVRATE = WAMT / BALANCE ; TYPE = 'A'
    loan1 = loan_sum.with_columns([
        pl.col("BRANCH").map_elements(
            lambda b: format_brchcd(int(b)), return_dtype=pl.Utf8
        ).alias("BRCH"),
        (pl.col("WAMT") / pl.col("BALANCE")).alias("WAVRATE"),
        pl.lit("A").alias("TYPE"),
    ])

    return loan1


# ---------------------------------------------------------------------------
# REPORT: WEIGHTED AVERAGE LENDING RATE ON HPD (RDIR II)
# ---------------------------------------------------------------------------

def report_wavg(df: pl.DataFrame, sdesc: str, rdate: str):
    """
    Equivalent to PROC REPORT with BREAK AFTER TYPE including a custom
    COMPUTE block that prints the grand-total summary line.

    Column layout (matching SAS DEFINE widths):
      BRANCH   : FORMAT=7.          -> numeric branch code, right-aligned 7
      BRCH     : PUT(BRANCH,BRCHCD.)-> branch name displayed in BRANCH column
      BALANCE  : FORMAT=COMMA20.2   -> right-aligned 20
      WAMT     : FORMAT=COMMA20.2   -> right-aligned 20
      WAVRATE  : FORMAT=10.8        -> right-aligned 10 (8 decimal places)

    Note: In the SAS DATA step, BRCH = PUT(BRANCH, BRCHCD.) derives the
    branch name using the PBBELF BRCHCD format. PROC REPORT then uses
    BRHNO (the numeric copy) for ORDER and displays BRANCH (the name) in
    the report body. We render BRCH as the visible branch identifier.
    """
    title_line = f"{sdesc}  REPORT AS AT  {rdate}"
    title3     = 'WEIGHTED AVERAGE LENDING RATE ON HPD (RDIR II)'
    titles     = [title_line, '', title3]

    rw = ReportWriter(OUTPUT_REPORT, titles)

    sep = '-' * 80   # 80-char separator (SAS LINE @009 80*'-' -> col 9 + 80 chars)

    # Column headers  (HEADLINE / HEADSKIP equivalent)
    rw.write(' ', ' ' * 8 + f"{'BRANCH':>7}  {'BALANCE':>20}  {'WEIGHTED AMOUNT':>20}  {'WGTED AV.RATE':>10}")
    rw.write(' ', ' ' * 8 + sep)

    # Detail rows – sorted by BRANCH numeric (already sorted from prepare_loan)
    # BRCH (branch name via format_brchcd / PBBELF BRCHCD format) is displayed
    # in the BRANCH column as per SAS:  BRCH = PUT(BRANCH, BRCHCD.)
    for row in df.iter_rows(named=True):
        branch_str  = f"{row['BRCH']:>7}"       # formatted branch name (BRCHCD format)
        balance_str = f"{row['BALANCE']:>20,.2f}"
        wamt_str    = f"{row['WAMT']:>20,.2f}"
        wavrate_str = f"{row['WAVRATE']:>10.8f}"
        rw.write(' ', f"        {branch_str}  {balance_str}  {wamt_str}  {wavrate_str}")

    # COMPUTE AFTER TYPE block (grand total summary)
    total_balance = df["BALANCE"].sum() or 0.0
    total_wamt    = df["WAMT"].sum() or 0.0
    avgrts        = round(total_wamt / total_balance, 8) if total_balance else 0.0

    rw.write(' ', ' ' * 8 + sep)
    # LINE @009 ' ' @014 BALANCE.SUM @036 WAMT.SUM @056 AVGRTS
    # Positional: col 9 = offset 8, col 14 = offset 13, col 36 = offset 35, col 56 = offset 55
    grand_line = (
        f"{' ':8}"                         # @009 = col 9 (0-indexed: 8 spaces)
        f"{'':5}"                           # padding to col 14
        f"{total_balance:>18,.2f}"         # BALANCE.SUM COMMA18.2  (ends ~col 35)
        f"  "
        f"{total_wamt:>18,.2f}"            # WAMT.SUM COMMA18.2     (ends ~col 55)
        f"  "
        f"{avgrts:>10.8f}"                 # AVGRTS COMMA10.8
    )
    rw.write(' ', grand_line)
    rw.write(' ', ' ' * 8 + sep)

    rw.save()
    print(f"[EIFMLN03] Report written -> {OUTPUT_REPORT}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    reptmon, rdate, sdesc = get_meta()
    print(f"[EIFMLN03] reptmon={reptmon}, NOWK={NOWK}, rdate={rdate}, sdesc={sdesc!r}")

    df = prepare_loan(reptmon)
    if df.is_empty():
        print("[EIFMLN03] WARNING: No loan data found. Empty report will be produced.")

    report_wavg(df, sdesc, rdate)
    print("[EIFMLN03] Done.")


if __name__ == "__main__":
    main()
