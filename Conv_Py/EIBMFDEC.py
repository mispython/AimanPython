# !/usr/bin/env python3
"""
Program: EIBMFDEC
Purpose: Exception FD Rate on Non-SME & Non-Resident Accounts.
         Reads Fixed Deposit data, filters by customer code and account type,
            merges with CIS customer names and Wadiah rate file, then produces
            a fixed-width report with ASA carriage control characters.
         RUN ON 16TH OF EVERY MONTH AFTER EIBWFALE.
         RECFM=FBA, LRECL=150
"""

import duckdb
import polars as pl
from pathlib import Path
import datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = Path("/data/sap")
RBP2_DIR      = Path("/data/rbp2")

# Input paths
FD_REPTDATE_PATH  = BASE_DIR / "pbb/mnifd/reptdate.parquet"       # SAP.PBB.MNIFD(0) - REPTDATE
FD_FD_PATH        = BASE_DIR / "pbb/mnifd/fd.parquet"             # SAP.PBB.MNIFD(0) - FD
BRANCHFL_PATH     = RBP2_DIR / "b033/pbb/branch.parquet"          # SAP.RBP2.B033.PBB.BRANCH
CISFD_PATH        = BASE_DIR / "pbb/crm/cisbext/deposit.parquet"  # SAP.PBB.CRM.CISBEXT - DEPOSIT
RATEFLE_PATH      = RBP2_DIR / "b033/wadiah/rate.txt"             # RBP2.B033.WADIAH.RATE

# Output path
# *SASLIST  DD DSN=SAP.PBB.FD1ME.COLD (commented out in original)
# SASLIST   DD DSN=&&COLD (temp)
OUTPUT_PATH = BASE_DIR / "pbb/fd1me.cold.txt"

# Report layout constants (LRECL=150, RECFM=FBA)
LRECL      = 150
PAGE_LINES = 60  # default page length

# ============================================================================
# INTPLAN MAPPING (373-391 -> remapped values)
# ============================================================================

INTPLAN_MAP = {
    373: 300, 374: 312, 375: 301, 376: 313, 377: 314,
    378: 302, 379: 315, 380: 316, 381: 303, 382: 317,
    383: 318, 384: 304, 385: 305, 386: 306, 387: 307,
    388: 308, 389: 309, 390: 310, 391: 311,
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_numeric(value, width: int, decimals: int) -> str:
    """Format numeric right-justified with fixed decimal places."""
    if value is None:
        return ' ' * width
    try:
        s = f"{float(value):>{width}.{decimals}f}"
        return s[:width] if len(s) > width else s
    except (ValueError, TypeError):
        return ' ' * width


def fmt_integer(value, width: int, zero_padded: bool = False) -> str:
    """Format integer, optionally zero-padded."""
    if value is None:
        return ' ' * width
    try:
        if zero_padded:
            return f"{int(value):0{width}d}"
        return f"{int(value):{width}d}"
    except (ValueError, TypeError):
        return ' ' * width


def fmt_char(value, width: int) -> str:
    """Format character left-justified, padded/truncated to width."""
    if value is None:
        return ' ' * width
    return f"{str(value):<{width}.{width}}"


def fmt_date_ddmmyy10(d) -> str:
    """Format date as DD/MM/YYYY (DDMMYY10.)."""
    if d is None:
        return ' ' * 10
    try:
        if isinstance(d, (int, float)):
            # SAS date: days since 1960-01-01
            d = datetime.date(1960, 1, 1) + datetime.timedelta(days=int(d))
        if hasattr(d, 'date'):
            d = d.date()
        return d.strftime('%d/%m/%Y')
    except Exception:
        return ' ' * 10


def pad_line(content: str, lrecl: int) -> str:
    """Pad/truncate content (no ASA char) to LRECL-1 characters."""
    body = lrecl - 1
    return f"{content:<{body}.{body}}"


def asa_newpage() -> str:
    return '1'


def asa_newline() -> str:
    return ' '


def build_at(line: str, col: int, text: str) -> str:
    """
    Place text at 1-based column position col within the content string.
    Extends line with spaces as needed.
    """
    idx = col - 1
    if len(line) < idx:
        line = line + ' ' * (idx - len(line))
    return line[:idx] + text + line[idx + len(text):]


# ============================================================================
# RATE FILE READER
# ============================================================================

def parse_rate_file(path: Path) -> pl.DataFrame:
    """
    Read fixed-width Wadiah rate file.
    INPUT @04 INTPLAN 3.  @16 RATE 5.3  @21 TDATE 8.
    Columns are 1-based in SAS, so:
      INTPLAN: cols 4-6   (index 3:6)
      RATE:    cols 16-20 (index 15:20), format 5.3 means 5 chars, implied 3 decimals
      TDATE:   cols 21-28 (index 20:28), format 8.
    EDATE = parse TDATE as YYMMDD8.
    """
    records = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')
            if len(line) < 28:
                line = line.ljust(28)
            try:
                intplan_str = line[3:6].strip()
                rate_str    = line[15:20].strip()
                tdate_str   = line[20:28].strip()
                if not intplan_str:
                    continue
                intplan = int(intplan_str)
                rate    = float(rate_str) / 1000.0 if rate_str else None   # 5.3 implied decimals
                # TDATE as YYMMDD8. -> parse
                if tdate_str and len(tdate_str) == 8:
                    # YYMMDD8. with YEARCUTOFF=1990: 2-digit year >= 90 -> 19xx, else 20xx
                    yy = int(tdate_str[0:2])
                    mm = int(tdate_str[2:4])
                    dd = int(tdate_str[4:6])
                    # remaining 2 chars ignored (8-char field, first 6 used)
                    year = (1900 + yy) if yy >= 90 else (2000 + yy)
                    edate = datetime.date(year, mm, dd)
                else:
                    edate = None
                records.append({'INTPLAN': intplan, 'RATE': rate, 'EDATE': edate})
            except (ValueError, IndexError):
                continue
    return pl.DataFrame(records)


# ============================================================================
# DATE CONVERSION UTILITIES
# ============================================================================

def depodte_to_date(depodte_val) -> datetime.date:
    """
    DEPDTE=INPUT(SUBSTR(PUT(DEPODTE,Z11.),1,8),MMDDYY8.)
    DEPODTE is numeric; format as Z11., take first 8 chars, parse as MMDDYY8.
    MMDDYY8. = MMDDYYYY (8 chars)
    """
    try:
        s = f"{int(depodte_val):011d}"[:8]   # zero-pad to 11, take first 8
        mm = int(s[0:2])
        dd = int(s[2:4])
        yy = int(s[4:8])
        # YEARCUTOFF=1990 applied in SAS but MMDDYY8. with 4-digit year is straightforward
        return datetime.date(yy, mm, dd)
    except Exception:
        return None


def matdate_to_date(matdate_val) -> datetime.date:
    """
    MATDTE=INPUT(SUBSTR(PUT(MATDATE,Z8.),1,8),YYMMDD8.)
    MATDATE as Z8., parse first 8 chars as YYMMDD8. (YYYYMMDD effectively with 4-digit year).
    """
    try:
        s = f"{int(matdate_val):08d}"
        # YYMMDD8. with 4-digit year in the string = YYYYMMDD
        yyyy = int(s[0:4])
        mm   = int(s[4:6])
        dd   = int(s[6:8])
        return datetime.date(yyyy, mm, dd)
    except Exception:
        return None


def lmatdate_to_date(lmatdate_val) -> datetime.date:
    """
    LMATDT=INPUT(SUBSTR(PUT(LMATDATE,Z11.),1,8),MMDDYY8.)
    Same logic as DEPODTE.
    """
    return depodte_to_date(lmatdate_val)


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    con = duckdb.connect()

    # ------------------------------------------------------------
    # DATA REPTDATE: SET FD.REPTDATE
    # ------------------------------------------------------------
    reptdate_row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{FD_REPTDATE_PATH}') LIMIT 1"
    ).fetchdf()
    reptdate_val = reptdate_row['REPTDATE'].iloc[0]
    if hasattr(reptdate_val, 'date'):
        reptdate_val = reptdate_val.date()
    reptdt = reptdate_val.strftime('%d/%m/%Y')   # DDMMYY10. -> DD/MM/YYYY

    # ------------------------------------------------------------
    # DATA FD: SET FD.FD with filters
    # ------------------------------------------------------------
    fd_df = con.execute(
        f"SELECT * FROM read_parquet('{FD_FD_PATH}')"
    ).pl()
    con.close()

    # IF (01<=CUSTCD<=63) OR (80<=CUSTCD<=99) OR CUSTCD=75
    fd_df = fd_df.filter(
        ((pl.col('CUSTCD') >= 1)  & (pl.col('CUSTCD') <= 63)) |
        ((pl.col('CUSTCD') >= 80) & (pl.col('CUSTCD') <= 99)) |
        (pl.col('CUSTCD') == 75)
    )

    # IF OPENIND NOT IN ('B','C','P') AND ACCTTYPE IN (300,301,303) AND CURBAL > 0
    fd_df = fd_df.filter(
        (~pl.col('OPENIND').is_in(['B', 'C', 'P'])) &
        (pl.col('ACCTTYPE').is_in([300, 301, 303])) &
        (pl.col('CURBAL') > 0)
    )

    # Compute DEPDTE, MATDTE, LMATDT
    def compute_dates(df: pl.DataFrame) -> pl.DataFrame:
        rows = df.to_dicts()
        depdtes  = []
        matdtes  = []
        lmatdts  = []
        for row in rows:
            depdte  = depodte_to_date(row.get('DEPODTE', 0))
            matdte  = matdate_to_date(row.get('MATDATE', 0))
            lmd_raw = row.get('LMATDATE', 0)
            if lmd_raw and float(lmd_raw) > 0:
                lmatdt = lmatdate_to_date(lmd_raw)
            else:
                lmatdt = depdte
            depdtes.append(depdte)
            matdtes.append(matdte)
            lmatdts.append(lmatdt)
        return df.with_columns([
            pl.Series('DEPDTE', depdtes),
            pl.Series('MATDTE', matdtes),
            pl.Series('LMATDT', lmatdts),
        ])

    fd_df = compute_dates(fd_df)

    # IF PENDINT=. THEN PENDINT=0; IF PENDINT > 0 THEN DELETE
    fd_df = fd_df.with_columns(
        pl.col('PENDINT').fill_null(0)
    ).filter(pl.col('PENDINT') <= 0)

    # OLDPLN=INTPLAN; remap INTPLAN
    fd_df = fd_df.with_columns(
        pl.col('INTPLAN').alias('OLDPLN')
    )
    fd_df = fd_df.with_columns(
        pl.col('INTPLAN').replace(INTPLAN_MAP).alias('INTPLAN')
    )

    # ------------------------------------------------------------
    # DATA CUSTM: SET CISFD.DEPOSIT; IF SECCUST='901'
    # KEEP ACCTNO CUSTNAME
    # PROC SORT NODUPKEY BY ACCTNO
    # ------------------------------------------------------------
    con2 = duckdb.connect()
    custm_df = con2.execute(
        f"SELECT ACCTNO, CUSTNAME FROM read_parquet('{CISFD_PATH}') "
        f"WHERE SECCUST = '901'"
    ).pl()
    con2.close()
    # NODUPKEY: keep first occurrence per ACCTNO
    custm_df = custm_df.unique(subset=['ACCTNO'], keep='first')

    # ------------------------------------------------------------
    # DATA FD: MERGE FD(IN=A) CUSTM; BY ACCTNO; IF A
    # ------------------------------------------------------------
    fd_df = fd_df.join(custm_df, on='ACCTNO', how='left')

    # ------------------------------------------------------------
    # PROC SORT; BY INTPLAN
    # ------------------------------------------------------------
    fd_df = fd_df.sort('INTPLAN')

    # ------------------------------------------------------------
    # DATA RATE: INFILE RATEFLE - read fixed-width rate file
    # PROC SORT DATA=RATE; BY INTPLAN DESCENDING EDATE
    # ------------------------------------------------------------
    rate_df = parse_rate_file(RATEFLE_PATH)
    rate_df = rate_df.sort(['INTPLAN', 'EDATE'], descending=[False, True])

    # ------------------------------------------------------------
    # DATA RATE: BY INTPLAN; FIRST.INTPLAN -> REC=1; REC+1
    # Keep only REC 1-5 per INTPLAN
    # ------------------------------------------------------------
    rate_df = rate_df.with_columns(
        pl.col('INTPLAN').rank(method='dense').over('INTPLAN')
    )
    # Add row number within each INTPLAN group (already sorted desc EDATE)
    rate_df = rate_df.with_columns(
        pl.arange(0, rate_df.height).alias('_row_idx')
    )
    rate_df = rate_df.with_columns(
        (pl.col('_row_idx') - pl.col('_row_idx').min().over('INTPLAN') + 1).alias('REC')
    ).drop('_row_idx')

    # DATA RATEA (KEEP=INTPLAN RATE) RATEB (KEEP=INTPLAN EDATE): IF 1<=REC<=5
    ratea_df = rate_df.filter((pl.col('REC') >= 1) & (pl.col('REC') <= 5)).select(['INTPLAN', 'RATE', 'REC'])
    rateb_df = rate_df.filter((pl.col('REC') >= 1) & (pl.col('REC') <= 5)).select(['INTPLAN', 'EDATE', 'REC'])

    # PROC TRANSPOSE DATA=RATEA OUT=RATEA; BY INTPLAN; VAR RATE
    # Creates COL1..COL5 per INTPLAN
    ratea_pivot = (
        ratea_df
        .with_columns(pl.col('REC').cast(pl.Utf8).map_elements(lambda x: f'COL{x}', return_dtype=pl.Utf8))
        .pivot(index='INTPLAN', on='REC', values='RATE', aggregate_function='first')
    )
    # Ensure COL1-COL5 exist
    for i in range(1, 6):
        col = f'COL{i}'
        if col not in ratea_pivot.columns:
            ratea_pivot = ratea_pivot.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

    # DATA RATEA: IF COL2=. THEN COL2=COL1; etc.; RATE01-RATE05=COL1-COL5
    ratea_pivot = ratea_pivot.with_columns([
        pl.when(pl.col('COL2').is_null()).then(pl.col('COL1')).otherwise(pl.col('COL2')).alias('COL2'),
        pl.when(pl.col('COL3').is_null()).then(pl.col('COL1')).otherwise(pl.col('COL3')).alias('COL3'),
        pl.when(pl.col('COL4').is_null()).then(pl.col('COL1')).otherwise(pl.col('COL4')).alias('COL4'),
        pl.when(pl.col('COL5').is_null()).then(pl.col('COL1')).otherwise(pl.col('COL5')).alias('COL5'),
    ]).rename({
        'COL1': 'RATE01', 'COL2': 'RATE02', 'COL3': 'RATE03',
        'COL4': 'RATE04', 'COL5': 'RATE05',
    })

    # PROC TRANSPOSE DATA=RATEB OUT=RATEB; BY INTPLAN; VAR EDATE
    rateb_pivot = (
        rateb_df
        .with_columns(pl.col('REC').cast(pl.Utf8).map_elements(lambda x: f'COL{x}', return_dtype=pl.Utf8))
        .pivot(index='INTPLAN', on='REC', values='EDATE', aggregate_function='first')
    )
    for i in range(1, 6):
        col = f'COL{i}'
        if col not in rateb_pivot.columns:
            rateb_pivot = rateb_pivot.with_columns(pl.lit(None).alias(col))

    # DATA RATES: MERGE RATEA RATEB; BY INTPLAN; DROP _NAME_
    rates_df = ratea_pivot.join(rateb_pivot, on='INTPLAN', how='outer', suffix='_DATE')

    # PROC SORT BY INTPLAN
    rates_df = rates_df.sort('INTPLAN')

    # ------------------------------------------------------------
    # DATA FD: MERGE RATES FD(IN=A); BY INTPLAN; IF A
    # Determine NR based on LMATDT vs COL1..COL5 date columns
    # IF RATE GE NR
    # INTPLAN=OLDPLN
    # ------------------------------------------------------------
    fd_df = fd_df.join(rates_df, on='INTPLAN', how='left')

    # Compute NR per row
    date_cols = ['COL1_DATE', 'COL2_DATE', 'COL3_DATE', 'COL4_DATE', 'COL5_DATE']
    rate_cols = ['RATE01', 'RATE02', 'RATE03', 'RATE04', 'RATE05']

    def compute_nr(row: dict) -> float:
        lmatdt = row.get('LMATDT')
        for dc, rc in zip(date_cols, rate_cols):
            dc_val = row.get(dc)
            if dc_val is not None and lmatdt is not None and lmatdt > dc_val:
                return row.get(rc)
        # IF NR=. THEN NR=RATE01
        r01 = row.get('RATE01')
        return r01 if r01 is not None else None

    nr_vals = [compute_nr(r) for r in fd_df.to_dicts()]
    fd_df = fd_df.with_columns(pl.Series('NR', nr_vals))

    # IF NR=. THEN NR=RATE01
    fd_df = fd_df.with_columns(
        pl.when(pl.col('NR').is_null()).then(pl.col('RATE01')).otherwise(pl.col('NR')).alias('NR')
    )

    # IF RATE GE NR (keep only rows where RATE >= NR)
    fd_df = fd_df.filter(pl.col('RATE') >= pl.col('NR'))

    # INTPLAN=OLDPLN
    fd_df = fd_df.with_columns(pl.col('OLDPLN').alias('INTPLAN'))

    # PROC SORT; BY BRANCH ACCTNO MATDTE
    fd_df = fd_df.sort(['BRANCH', 'ACCTNO', 'MATDTE'])

    # ------------------------------------------------------------
    # REPORT GENERATION
    # TITLE1-4: '  ' (blank titles)
    # DATA WRITE: FILE PRINT HEADER=NEWPAGE
    # PUT statements with NEWPAGE header routine
    # ------------------------------------------------------------
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    lines      = []
    page_count = 0
    line_count = PAGE_LINES + 1   # force new page on first record

    def emit(asa: str, content: str):
        nonlocal line_count
        padded = pad_line(content, LRECL)
        lines.append(asa + padded)
        if asa != '+':
            line_count += 1

    def emit_newpage(branch_str: str):
        """NEWPAGE header routine."""
        nonlocal page_count, line_count
        page_count += 1
        line_count  = 0

        # PUT @001 'REPORT NO : FDEXNSME' @051 'P U B L I C  B A N K  B H D' @111 'PAGE' @126 ':' PAGECNT
        ln = ' ' * 149
        ln = build_at(ln,   1, 'REPORT NO : FDEXNSME')
        ln = build_at(ln,  51, 'P U B L I C  B A N K  B H D')
        ln = build_at(ln, 111, 'PAGE')
        ln = build_at(ln, 126, f': {page_count}')
        emit(asa_newpage(), ln)

        # PUT @001 'PROGRAM ID: EIBMFDEC' @041 'EXCEPTION FD RATE...' @111 'REPORT DATE:' "&REPTDT"
        ln = ' ' * 149
        ln = build_at(ln,   1, 'PROGRAM ID: EIBMFDEC')
        ln = build_at(ln,  41, 'EXCEPTION FD RATE ON NON-SME & NON-RESIDENT ACCTS ')
        ln = build_at(ln, 111, f'REPORT DATE: {reptdt}')
        emit(asa_newline(), ln)

        # PUT @001 'BRANCH ID : ' @013 BRXH
        ln = ' ' * 149
        ln = build_at(ln,  1, 'BRANCH ID : ')
        ln = build_at(ln, 13, branch_str)
        emit(asa_newline(), ln)

        # PUT @002 '        '  (blank line)
        emit(asa_newline(), '')

        # PUT @002 'ACCOUNT ' @046 'RECEIPT ' @058 'OFFERED       INTR  DEPOSIT     MATURITY'
        ln = ' ' * 149
        ln = build_at(ln,  2, 'ACCOUNT ')
        ln = build_at(ln, 46, 'RECEIPT ')
        ln = build_at(ln, 58, 'OFFERED       INTR  DEPOSIT     MATURITY')
        emit(asa_newline(), ln)

        # PUT @002 'NUMBER  ' @014 'NAME OF CUSTOMER ' @046 'NUMBER' @058 'RATE    TERM  PLAN  DATE        DATE '
        ln = ' ' * 149
        ln = build_at(ln,  2, 'NUMBER  ')
        ln = build_at(ln, 14, 'NAME OF CUSTOMER ')
        ln = build_at(ln, 46, 'NUMBER')
        ln = build_at(ln, 58, 'RATE    TERM  PLAN  DATE        DATE ')
        emit(asa_newline(), ln)

        # PUT @002 '==========' @014 '=============================' @046 '=======' @058 '====    ====  ====  ==========  =========='
        ln = ' ' * 149
        ln = build_at(ln,  2, '==========')
        ln = build_at(ln, 14, '=============================')
        ln = build_at(ln, 46, '=======')
        ln = build_at(ln, 58, '====    ====  ====  ==========  ==========')
        emit(asa_newline(), ln)

        line_count = 7   # header used 7 lines (page line + 6 more)

    records    = fd_df.to_dicts()
    prev_branch = None

    for row in records:
        branch_val = row.get('BRANCH')
        # FORMAT BRXH $3.: BRXH=BRANCH (branch formatted as 3-char string)
        try:
            brxh = f"{int(branch_val):03d}" if branch_val is not None else '   '
        except (ValueError, TypeError):
            brxh = fmt_char(branch_val, 3)

        # IF FIRST.BRANCH THEN PUT _PAGE_
        if branch_val != prev_branch:
            emit_newpage(brxh)
            prev_branch = branch_val
        elif line_count >= PAGE_LINES:
            emit_newpage(brxh)

        # PUT @002 ACCTNO 10. @014 CUSTNAME $30. @042 CDNO 10. @055 RATE 8.3
        #     @065 TERM 4. @072 INTPLAN 3. @078 LMATDT (DDMMYY10.) @090 MATDTE (DDMMYY10.)
        ln = ' ' * 149
        ln = build_at(ln,  2, fmt_integer(row.get('ACCTNO'),   10))
        ln = build_at(ln, 14, fmt_char(row.get('CUSTNAME'),    30))
        ln = build_at(ln, 42, fmt_integer(row.get('CDNO'),     10))
        ln = build_at(ln, 55, fmt_numeric(row.get('RATE'),      8, 3))
        ln = build_at(ln, 65, fmt_integer(row.get('TERM'),      4))
        ln = build_at(ln, 72, fmt_integer(row.get('INTPLAN'),   3))
        ln = build_at(ln, 78, fmt_date_ddmmyy10(row.get('LMATDT')))
        ln = build_at(ln, 90, fmt_date_ddmmyy10(row.get('MATDTE')))
        emit(asa_newline(), ln)

    # Write output
    with open(OUTPUT_PATH, 'w', encoding='utf-8', newline='\n') as f:
        for line in lines:
            f.write(line + '\n')

    print(f"Report written to: {OUTPUT_PATH}")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
