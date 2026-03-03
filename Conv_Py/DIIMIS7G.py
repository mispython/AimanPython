# !/usr/bin/env python3
"""
Program : DIIMIS7G.py
Date    : 15.08.2014
SMR     : 2014-1832
Report  : ISLAMIC TERM DEPOSIT ACCOUNT (316,393)
"""

import sys
import os
from datetime import date, datetime
import duckdb
import polars as pl

# %INC PGM(PBMISFMT) - import from PBMISFMT module
from PBMISFMT import format_brchcd

# ===========================================================================
# PATH CONFIGURATION
# ===========================================================================
INPUT_DIR   = os.environ.get('INPUT_DIR',  'input')
OUTPUT_DIR  = os.environ.get('OUTPUT_DIR', 'output')

# Input parquet files
REPTDATE_PARQUET = os.path.join(INPUT_DIR, 'REPTDATE.parquet')
# MIS.DYIBUG<MM> parquet - built dynamically from reptmon
# e.g. MIS.DYIBUG06 -> DYIBUG06.parquet

# Output report file
REPORT_FILE = os.path.join(OUTPUT_DIR, 'DIIMIS7G.txt')

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===========================================================================
# LINESIZE / PAGE CONFIGURATION
# OPTIONS LINESIZE=132; page length default 60 lines
# ===========================================================================
LINESIZE   = 132
PAGE_LINES = 60   # default page length

# ===========================================================================
# FORMAT HELPERS
# ===========================================================================

def fmt_fdfmt(mth: int) -> str:
    """FDFMT: numeric month code (1-60) -> '<N> MONTH(S)' label."""
    if 1 <= mth <= 60:
        suffix = 'MONTH' if mth == 1 else 'MONTHS'
        return f'{mth} {suffix}'
    return ' '


def fmt_cdfmt(custcd) -> str:
    """CDFMT: customer code -> RETAIL / CORPORATE."""
    try:
        v = int(custcd)
    except (TypeError, ValueError):
        return 'CORPORATE'
    if v in (77, 78, 95, 96):
        return 'RETAIL'
    return 'CORPORATE'


def fmt_z3(branch) -> str:
    """Zero-pad branch to 3 digits."""
    try:
        return f'{int(branch):03d}'
    except (TypeError, ValueError):
        return '000'


# ===========================================================================
# INTPLAN -> MTH MAPPING  (SELECT/WHEN in SAS DATA step)
# NOTE: DIIMIS7G uses a different, simpler mapping than DIIMIS7O/P/W/V.
#       INTPLAN 780-839 maps sequentially to MTH 1-60 (one-to-one).
# ===========================================================================
_INTPLAN_MTH: dict[int, int] = {}

def _register(mth: int, *plans: int):
    for p in plans:
        _INTPLAN_MTH[p] = mth

_register(1,  780)
_register(2,  781)
_register(3,  782)
_register(4,  783)
_register(5,  784)
_register(6,  785)
_register(7,  786)
_register(8,  787)
_register(9,  788)
_register(10, 789)
_register(11, 790)
_register(12, 791)
_register(13, 792)
_register(14, 793)
_register(15, 794)
_register(16, 795)
_register(17, 796)
_register(18, 797)
_register(19, 798)
_register(20, 799)
_register(21, 800)
_register(22, 801)
_register(23, 802)
_register(24, 803)
_register(25, 804)
_register(26, 805)
_register(27, 806)
_register(28, 807)
_register(29, 808)
_register(30, 809)
_register(31, 810)
_register(32, 811)
_register(33, 812)
_register(34, 813)
_register(35, 814)
_register(36, 815)
_register(37, 816)
_register(38, 817)
_register(39, 818)
_register(40, 819)
_register(41, 820)
_register(42, 821)
_register(43, 822)
_register(44, 823)
_register(45, 824)
_register(46, 825)
_register(47, 826)
_register(48, 827)
_register(49, 828)
_register(50, 829)
_register(51, 830)
_register(52, 831)
_register(53, 832)
_register(54, 833)
_register(55, 834)
_register(56, 835)
_register(57, 836)
_register(58, 837)
_register(59, 838)
_register(60, 839)


def map_intplan_to_mth(intplan) -> int:
    """Map INTPLAN code to month bucket (0 if unrecognised)."""
    try:
        return _INTPLAN_MTH.get(int(intplan), 0)
    except (TypeError, ValueError):
        return 0


# ===========================================================================
# REPORT RENDERING HELPERS
# ===========================================================================

class ReportWriter:
    """Stateful line/page writer that prepends ASA carriage-control characters."""

    ASA_SPACE   = ' '   # single space  (advance 1 line)
    ASA_DOUBLE  = '0'   # double space  (advance 2 lines before printing)
    ASA_NEWPAGE = '1'   # advance to new page

    def __init__(self, fileobj, page_lines: int = PAGE_LINES, linesize: int = LINESIZE):
        self._f          = fileobj
        self._page_lines = page_lines
        self._linesize   = linesize
        self._line_count = 0
        self._page_no    = 0
        self._titles: list[str] = []
        self._pending_new_page  = True   # first page triggers newpage

    def set_titles(self, *titles: str):
        self._titles = list(titles)

    def _write_raw(self, asa: str, text: str):
        line = (asa + text)[:self._linesize + 1]
        self._f.write(line + '\n')

    def _emit_page_header(self):
        self._page_no += 1
        # First title line uses ASA '1' (new page)
        for i, t in enumerate(self._titles):
            asa = self.ASA_NEWPAGE if i == 0 else self.ASA_SPACE
            self._write_raw(asa, t.center(self._linesize))
        # blank line after titles
        self._write_raw(self.ASA_SPACE, '')
        self._line_count = len(self._titles) + 1

    def write_line(self, text: str, double_space: bool = False):
        """Write a content line, emitting page header when needed."""
        if self._pending_new_page or self._line_count >= self._page_lines:
            self._emit_page_header()
            self._pending_new_page = False

        asa = self.ASA_DOUBLE if double_space else self.ASA_SPACE
        self._write_raw(asa, text)
        advance = 2 if double_space else 1
        self._line_count += advance

    def new_page(self):
        self._pending_new_page = True

    def write_blank(self):
        self.write_line('')


# ===========================================================================
# TABULATE RENDERING  (replicates PROC TABULATE output)
# ===========================================================================

def _comma9(v) -> str:
    """Format number as COMMA9 (integer with commas, 9 chars)."""
    try:
        return f'{int(round(float(v))):>9,}'
    except (TypeError, ValueError):
        return f'{"0":>9}'


def _comma14_2(v) -> str:
    """Format number as COMMA14.2 (2 dp with commas, 14 chars)."""
    try:
        return f'{float(v):>14,.2f}'
    except (TypeError, ValueError):
        return f'{"0.00":>14}'


def render_tabulate_old(df: pl.DataFrame, writer: ReportWriter, section_title: str):
    """
    Replicate PROC TABULATE for OLD format (no CUSTCD dimension):
      Rows    : BRCH + TOTAL
      Columns : NO OF A/C | NO OF RECEIPT | FDI by MTH cols + TOTAL
    """
    # Collect all MTH values present (sorted)
    mth_vals   = sorted(df['MTH'].unique().to_list())
    mth_labels = [fmt_fdfmt(m) for m in mth_vals]
    col_w      = 14   # width per MTH FDI column

    # Aggregate by BRCH, MTH
    grp = (
        df.group_by(['BRCH', 'MTH'])
          .agg([
              pl.sum('FDINO').alias('FDINO'),
              pl.sum('FDINO2').alias('FDINO2'),
              pl.sum('FDI').alias('FDI'),
          ])
    )

    brch_vals  = sorted(grp['BRCH'].unique().to_list())
    mth_header = ''.join(f'{lbl:>{col_w}}' for lbl in mth_labels)
    sep        = '-' * LINESIZE

    writer.write_blank()
    writer.write_line(f'  {section_title}')
    writer.write_blank()

    # Header row
    header = (
        f'{"BRANCH NO/CODE":<10}  '
        f'{"NO OF A/C":>9}  '
        f'{"NO OF RECEIPT":>9}  '
        + mth_header
        + f'{"TOTAL":>{col_w}}'
    )
    writer.write_line(header)
    writer.write_line(sep)

    # Data rows
    for brch in brch_vals:
        sub     = grp.filter(pl.col('BRCH') == brch)
        fdino   = sub['FDINO'].sum()
        fdino2  = sub['FDINO2'].sum()
        fdi_map = {row['MTH']: row['FDI'] for row in sub.iter_rows(named=True)}
        fdi_tot = sub['FDI'].sum()
        fdi_cols = ''.join(f'{_comma14_2(fdi_map.get(m, 0)):>{col_w}}' for m in mth_vals)
        line = (
            f'{brch:<10}  '
            f'{_comma9(fdino)}  '
            f'{_comma9(fdino2)}  '
            + fdi_cols
            + f'{_comma14_2(fdi_tot):>{col_w}}'
        )
        writer.write_line(line)

    # TOTAL row
    fdino_t     = grp['FDINO'].sum()
    fdino2_t    = grp['FDINO2'].sum()
    fdi_mth_t   = grp.group_by('MTH').agg(pl.sum('FDI'))
    fdi_mth_map = {r['MTH']: r['FDI'] for r in fdi_mth_t.iter_rows(named=True)}
    fdi_total_t = grp['FDI'].sum()

    fdi_cols_t = ''.join(f'{_comma14_2(fdi_mth_map.get(m, 0)):>{col_w}}' for m in mth_vals)
    total_line = (
        f'{"TOTAL":<10}  '
        f'{_comma9(fdino_t)}  '
        f'{_comma9(fdino2_t)}  '
        + fdi_cols_t
        + f'{_comma14_2(fdi_total_t):>{col_w}}'
    )
    writer.write_line(sep)
    writer.write_line(total_line)
    writer.write_blank()


def render_tabulate_new(df: pl.DataFrame, writer: ReportWriter, section_title: str):
    """
    Replicate PROC TABULATE for NEW format (with CUSTCD dimension):
      Outer rows : CUSTCD (RETAIL / CORPORATE)
      Inner rows : BRCH + TOTAL per CUSTCD
      Columns    : NO OF A/C | NO OF RECEIPT | FDI by MTH cols + TOTAL
    """
    mth_vals   = sorted(df['MTH'].unique().to_list())
    col_w      = 14

    sep = '-' * LINESIZE

    # Add CUSTCD_LABEL
    df = df.with_columns(
        pl.col('CUSTCD').map_elements(fmt_cdfmt, return_dtype=pl.Utf8).alias('CUSTCD_LABEL')
    )

    grp = (
        df.group_by(['CUSTCD_LABEL', 'BRCH', 'MTH'])
          .agg([
              pl.sum('FDINO').alias('FDINO'),
              pl.sum('FDINO2').alias('FDINO2'),
              pl.sum('FDI').alias('FDI'),
          ])
    )

    writer.write_blank()
    writer.write_line(f'  {section_title}')
    writer.write_blank()

    # Header row
    mth_header = ''.join(f'{fmt_fdfmt(m):>{col_w}}' for m in mth_vals)
    header = (
        f'{"BRANCH NO/CODE":<10}  '
        f'{"NO OF A/C":>9}  '
        f'{"NO OF RECEIPT":>9}  '
        + mth_header
        + f'{"TOTAL":>{col_w}}'
    )
    writer.write_line(header)

    custcd_labels = sorted(df['CUSTCD_LABEL'].unique().to_list())

    for cd_label in custcd_labels:
        writer.write_line(sep)
        writer.write_line(f'{cd_label}')
        writer.write_line(sep)

        sub_cd    = grp.filter(pl.col('CUSTCD_LABEL') == cd_label)
        brch_vals = sorted(sub_cd['BRCH'].unique().to_list())

        for brch in brch_vals:
            sub     = sub_cd.filter(pl.col('BRCH') == brch)
            fdino   = sub['FDINO'].sum()
            fdino2  = sub['FDINO2'].sum()
            fdi_map = {row['MTH']: row['FDI'] for row in sub.iter_rows(named=True)}
            fdi_tot = sub['FDI'].sum()
            fdi_cols = ''.join(f'{_comma14_2(fdi_map.get(m, 0)):>{col_w}}' for m in mth_vals)
            line = (
                f'{brch:<10}  '
                f'{_comma9(fdino)}  '
                f'{_comma9(fdino2)}  '
                + fdi_cols
                + f'{_comma14_2(fdi_tot):>{col_w}}'
            )
            writer.write_line(line)

        # TOTAL per CUSTCD
        fdino_t        = sub_cd['FDINO'].sum()
        fdino2_t       = sub_cd['FDINO2'].sum()
        fdi_mth_cd     = sub_cd.group_by('MTH').agg(pl.sum('FDI'))
        fdi_mth_map_cd = {r['MTH']: r['FDI'] for r in fdi_mth_cd.iter_rows(named=True)}
        fdi_total_cd   = sub_cd['FDI'].sum()

        fdi_cols_t = ''.join(f'{_comma14_2(fdi_mth_map_cd.get(m, 0)):>{col_w}}' for m in mth_vals)
        total_line = (
            f'{"TOTAL":<10}  '
            f'{_comma9(fdino_t)}  '
            f'{_comma9(fdino2_t)}  '
            + fdi_cols_t
            + f'{_comma14_2(fdi_total_cd):>{col_w}}'
        )
        writer.write_line(sep)
        writer.write_line(total_line)

    writer.write_blank()


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    # ------------------------------------------------------------------
    # Step 1 : Read REPTDATE  (replaces DATA REPTDATE / SET DEPOSIT.REPTDATE)
    # ------------------------------------------------------------------
    con = duckdb.connect()

    reptdate_row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()

    if reptdate_row is None:
        print('ERROR: REPTDATE table is empty.', file=sys.stderr)
        sys.exit(1)

    reptdate_val = reptdate_row[0]
    # Normalise to Python date
    if isinstance(reptdate_val, datetime):
        reptdate = reptdate_val.date()
    elif isinstance(reptdate_val, date):
        reptdate = reptdate_val
    else:
        reptdate = datetime.strptime(str(reptdate_val), '%Y-%m-%d').date()

    # REPTFQ='Y' (always quarterly flag in original)
    reptfq  = 'Y'
    reptmon = f'{reptdate.month:02d}'   # CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE), Z2.))
    rdate   = reptdate.strftime('%d%m%Y')  # DDMMYY8. -> DDMMYYYY

    # ------------------------------------------------------------------
    # CHKRPTDT: only execute if REPTFQ == 'Y'
    # ------------------------------------------------------------------
    if reptfq != 'Y':
        print('INFO: REPTFQ != Y - report not generated.')
        return

    # ------------------------------------------------------------------
    # Determine reporting period  (DATA _NULL_ / DATA CX equivalent)
    # ------------------------------------------------------------------
    sdate         = date(reptdate.year, reptdate.month, 1)   # first of month
    edate         = reptdate
    rpdate        = reptdate
    days_in_edate = edate.day   # DAY(&EDATE) used for daily average

    # ------------------------------------------------------------------
    # Step 2 : Read MIS.DYIBUG<REPTMON> parquet
    # (unique to DIIMIS7G; uses DYIBUG prefix and different INTPLAN mapping)
    # ------------------------------------------------------------------
    dyibug_parquet = os.path.join(INPUT_DIR, f'DYIBUG{reptmon}.parquet')

    raw_df = con.execute(
        f"SELECT * FROM read_parquet('{dyibug_parquet}')"
    ).pl()

    # ------------------------------------------------------------------
    # DATA DYIBUA / DYIBUA1  (replicate DATA step with SELECT/WHEN)
    # ------------------------------------------------------------------
    def _process_raw(df: pl.DataFrame) -> pl.DataFrame:
        """Add BRCHCD, BRCH, MTH columns."""
        df = df.with_columns([
            pl.col('BRANCH').map_elements(format_brchcd, return_dtype=pl.Utf8).alias('BRCHCD'),
            pl.col('INTPLAN').map_elements(map_intplan_to_mth, return_dtype=pl.Int32).alias('MTH'),
        ])
        df = df.with_columns(
            (
                pl.col('BRANCH').map_elements(fmt_z3, return_dtype=pl.Utf8)
                + pl.lit('/')
                + pl.col('BRCHCD')
            ).alias('BRCH')
        )
        return df

    processed = _process_raw(raw_df)

    # Normalise REPTDATE column to date for comparison
    if 'REPTDATE' in processed.columns:
        if processed['REPTDATE'].dtype != pl.Date:
            processed = processed.with_columns(
                pl.col('REPTDATE').cast(pl.Date)
            )

    # DYIBUA  : only rows where REPTDATE == EDATE (to-date balance)
    dyibua  = processed.filter(pl.col('REPTDATE') == edate)

    # DYIBUA1 : rows where SDATE <= REPTDATE <= EDATE (for daily average)
    dyibua1 = processed.filter(
        (pl.col('REPTDATE') >= sdate) & (pl.col('REPTDATE') <= edate)
    )

    # ------------------------------------------------------------------
    # Write reports
    # ------------------------------------------------------------------
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        writer = ReportWriter(f)

        # ==============================================================
        # OLD REPORT SECTION
        # TITLE1 'REPORT ID : DIIMIS7G (OLD)'
        # ==============================================================
        writer.set_titles(
            'REPORT ID : DIIMIS7G (OLD)',
            'PUBLIC ISLAMIC BANK BERHAD - PIBB',
            'ISLAMIC TERM DEPOSIT ACCOUNT',
            f'(01-JULY-14 ONWARDS) AS AT {rdate}',
        )

        # %TDBAL -> PROC TABULATE on DYIBUA (OLD, no CUSTCD)
        if dyibua.is_empty():
            writer.write_blank()
            writer.write_line('  (NO DATA FOR TODATE BALANCE)')
        else:
            render_tabulate_old(dyibua, writer, 'TODATE BALANCE')

        # %AVG -> daily average on DYIBUA1 (OLD, no CUSTCD)
        # CHK IF AVG DAILY BALANCE DATA AVAILABLE (OLD)
        n_old = dyibua1.height
        if n_old > 0:
            # PROC SUMMARY -> sum by BRCH, MTH then divide by DAYS
            dyibua1o = (
                dyibua1.group_by(['BRCH', 'MTH'])
                       .agg([
                           pl.sum('FDI').alias('FDI'),
                           pl.sum('FDINO').alias('FDINO'),
                           pl.sum('FDINO2').alias('FDINO2'),
                       ])
                       .with_columns([
                           (pl.col('FDI')    / days_in_edate).alias('FDI'),
                           (pl.col('FDINO')  / days_in_edate).alias('FDINO'),
                           (pl.col('FDINO2') / days_in_edate).alias('FDINO2'),
                       ])
            )
            render_tabulate_old(dyibua1o, writer, 'DAILY AVERAGE')

        # ==============================================================
        # NEW REPORT SECTION
        # TITLE1 'REPORT ID : DIIMIS7G (NEW)'
        # ==============================================================
        writer.new_page()
        writer.set_titles(
            'REPORT ID : DIIMIS7G (NEW)',
            'PUBLIC ISLAMIC BANK BERHAD - PIBB',
            'ISLAMIC TERM DEPOSIT ACCOUNT',
            f'(01-JULY-2014 ONWARDS) AS AT {rdate}',
        )

        # %TDBAL1 -> PROC TABULATE on DYIBUA (NEW, with CUSTCD)
        if dyibua.is_empty():
            writer.write_blank()
            writer.write_line('  (NO DATA FOR TODATE BALANCE)')
        else:
            render_tabulate_new(dyibua, writer, 'TODATE BALANCE')

        # %AVG1 -> daily average on DYIBUA1 (NEW, with CUSTCD)
        # CHK IF AVG DAILY BALANCE DATA AVAILABLE (NEW)
        # PROC CONTENTS DATA=DYIBUA1 NOPRINT OUT=NUMOBS(KEEP=NOBS)
        n_new = dyibua1.height
        if n_new > 0:
            dyibua1n = (
                dyibua1.group_by(['BRCH', 'MTH', 'CUSTCD'])
                       .agg([
                           pl.sum('FDI').alias('FDI'),
                           pl.sum('FDINO').alias('FDINO'),
                           pl.sum('FDINO2').alias('FDINO2'),
                       ])
                       .with_columns([
                           (pl.col('FDI')    / days_in_edate).alias('FDI'),
                           (pl.col('FDINO')  / days_in_edate).alias('FDINO'),
                           (pl.col('FDINO2') / days_in_edate).alias('FDINO2'),
                       ])
            )
            render_tabulate_new(dyibua1n, writer, 'DAILY AVERAGE')

    print(f'Report written to: {REPORT_FILE}')


if __name__ == '__main__':
    main()
