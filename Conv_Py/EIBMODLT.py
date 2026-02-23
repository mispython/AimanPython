# !/usr/bin/env python3
"""
Program Name: EIBMODLT
Purpose: Generate OD (Overdraft) Listing Reports for PBB (Public Bank Berhad) and PIBB (Public Islamic Bank Berhad)
         - Report 1: OD Listing by FISS Purpose Code (all custcodes)
         - Report 2: OD Listing by Construction (Sector 5001-5999) and Real Estate (8310) for Non-Individual Customers
         Outputs text data file (ODLSD/ODLSDI) and RPS report file (ODLST/ODLSTI)
"""

import duckdb
import polars as pl
from datetime import date, datetime
import os

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

# --- PBB Input Paths ---
BANK_REPTDATE_PARQUET    = "input/bank/reptdate.parquet"
BANK_LOAN_PARQUET_PREFIX = "input/bank/loan"          # e.g. loan0101.parquet (LOAN&REPTMON&NOWK)
BANK_CURRENT_PARQUET     = "input/bank/current.parquet"

# --- PIBB Input Paths ---
PIBB_REPTDATE_PARQUET    = "input/pibb/reptdate.parquet"
PIBB_LOAN_PARQUET_PREFIX = "input/pibb/loan"
PIBB_CURRENT_PARQUET     = "input/pibb/current.parquet"

# --- PBB Output Paths ---
BANK_ODLST_TXT  = "output/bank/odlist_rps.txt"        # SAP.BANK.ODLIST.RPS   (136 char)
BANK_ODLSD_TXT  = "output/bank/odlist_text.txt"       # SAP.BANK.ODLIST.TEXT  (80 char)

# --- PIBB Output Paths ---
PIBB_ODLSTI_TXT = "output/pibb/odlist_rps.txt"        # SAP.PIBB.ODLIST.RPS   (136 char)
PIBB_ODLSDI_TXT = "output/pibb/odlist_text.txt"       # SAP.PIBB.ODLIST.TEXT  (80 char)

# Ensure output directories exist
os.makedirs("output/bank", exist_ok=True)
os.makedirs("output/pibb", exist_ok=True)

# Fixed record widths
LINE_WIDTH     = 136
TEXT_WIDTH     = 80
LINECNT_MAX    = 55
LINECNT_START  = 13

# =============================================================================
# FORMAT HELPERS
# =============================================================================

BANK_FMT = {33: 'PBB', 134: 'PFB'}

def fmt_bankno(bankno):
    """PROC FORMAT BANKFMT"""
    return BANK_FMT.get(bankno, str(bankno) if bankno is not None else '')

def fmt_ddmmyy8(d):
    """Format date as DD/MM/YY (DDMMYY8.)"""
    if d is None:
        return ''
    if isinstance(d, (int, float)):
        d = date(1960, 1, 1) + __import__('datetime').timedelta(days=int(d))
    elif isinstance(d, str):
        d = datetime.strptime(d, '%Y-%m-%d').date()
    return d.strftime('%d/%m/%y')

def fmt_comma(val, width, decimals=2):
    """Format numeric as COMMA<width>.<decimals>, right-justified."""
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * width
    formatted = f"{val:,.{decimals}f}"
    return formatted.rjust(width)

def fmt_comma15_2(val):
    return fmt_comma(val, 15, 2)

def fmt_comma18_2(val):
    return fmt_comma(val, 18, 2)

def fmt_comma8_2(val):
    return fmt_comma(val, 8, 2)

def fmt_comma6_2(val):
    return fmt_comma(val, 6, 2)

def fmt_z3(val):
    """Format numeric as Z3. (zero-padded 3 digits)"""
    if val is None:
        return '000'
    return str(int(val)).zfill(3)

def place_at(line_list, col, text):
    """Place text into line_list at 1-indexed column position."""
    col0 = col - 1
    for i, ch in enumerate(str(text)):
        pos = col0 + i
        while len(line_list) <= pos:
            line_list.append(' ')
        line_list[pos] = ch

def make_line(width=LINE_WIDTH):
    return [' '] * width

def finalize_line(line_list, width=LINE_WIDTH):
    s = ''.join(line_list)
    return s.ljust(width)[:width]

def write_blank_p001(f, width=LINE_WIDTH):
    line = make_line(width)
    place_at(line, 1, 'P001')
    f.write(finalize_line(line, width) + '\n')

# =============================================================================
# DETERMINE REPORT DATE VARIABLES FROM REPTDATE
# =============================================================================

def get_report_vars(reptdate_parquet):
    """
    Read REPTDATE, compute NOWK, RDATE, REPTMON.
    OPTIONS YEARCUTOFF=1950: 2-digit years 50-99 -> 1950-1999, 00-49 -> 2000-2049.
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate_val = row[0]
    if isinstance(reptdate_val, (int, float)):
        reptdate = date(1960, 1, 1) + __import__('datetime').timedelta(days=int(reptdate_val))
    elif isinstance(reptdate_val, str):
        reptdate = datetime.strptime(reptdate_val, '%Y-%m-%d').date()
    else:
        reptdate = reptdate_val

    day = reptdate.day
    if day == 8:
        wk = '1'
    elif day == 15:
        wk = '2'
    elif day == 22:
        wk = '3'
    else:
        wk = '4'

    rdate   = reptdate.strftime('%d/%m/%y')
    reptmon = str(reptdate.month).zfill(2)

    return wk, rdate, reptmon

# =============================================================================
# MAKE BRANCH NUMBER STRING (BRNO)
# =============================================================================

def make_brno(branch):
    """Compute BRNO from branch number (SAS SELECT logic)."""
    b = int(branch)
    if b < 10:
        return f"BR0{b}"
    elif b < 100:
        return f"BR{b}"
    else:
        return f"B{b}"

# =============================================================================
# DATA PREPARATION (shared logic for PBB and PIBB)
# =============================================================================

def prepare_data(reptdate_parquet, loan_parquet_prefix, current_parquet):
    """
    Replicate SAS data preparation:
      1. Determine NOWK, RDATE, REPTMON from REPTDATE
      2. Load LOAN (KEEP=ACCTNO SECTORCD FISSPURP) sorted BY ACCTNO
      3. Load CURRENT, filter CURBAL<0 AND CUSTCODE NE 81, compute BALANCE=(-1)*CURBAL -> ODRAFT
      4. Sort ODRAFT BY ACCTNO
      5. MERGE ODRAFT + LOAN BY ACCTNO, keep IF A AND B -> ODRAFT1
      6. ODRAFT2 = ODRAFT1 filtered by CUSTCD/SECTORCD rules
      7. Sort ODRAFT1 BY BRANCH FISSPURP ACCTNO
      8. Sort ODRAFT2 BY BRANCH SECTORCD ACCTNO
    Returns: rdate, odraft1 (polars df), odraft2 (polars df)
    """
    wk, rdate, reptmon = get_report_vars(reptdate_parquet)
    loan_parquet = f"{loan_parquet_prefix}{reptmon}{wk}.parquet"

    con = duckdb.connect()

    # Load LOAN (KEEP=ACCTNO SECTORCD FISSPURP) sorted BY ACCTNO
    loan_df = con.execute(f"""
        SELECT acctno, sectorcd, fisspurp
        FROM read_parquet('{loan_parquet}')
        ORDER BY acctno
    """).pl()

    # Load CURRENT, filter CURBAL<0 AND CUSTCODE NE 81
    current_df = con.execute(f"""
        SELECT *
        FROM read_parquet('{current_parquet}')
        WHERE curbal < 0 AND custcode <> 81
        ORDER BY acctno
    """).pl()

    con.close()

    # BALANCE = (-1)*CURBAL
    odraft = current_df.with_columns(
        (pl.col('curbal') * -1).alias('balance')
    )

    # MERGE ODRAFT + LOAN BY ACCTNO, keep IF A AND B (inner join)
    odraft1 = odraft.join(loan_df, on='acctno', how='inner', suffix='_loan')

    # Resolve overlapping columns from loan side (sectorcd, fisspurp)
    for col in ['sectorcd', 'fisspurp']:
        loan_col = f"{col}_loan"
        if loan_col in odraft1.columns:
            odraft1 = odraft1.with_columns(
                pl.when(pl.col(loan_col).is_not_null())
                  .then(pl.col(loan_col))
                  .otherwise(pl.col(col))
                  .alias(col)
            ).drop(loan_col)

    # ODRAFT2: CUSTCD NOT IN ('77','78','95','96') AND (SECTORCD starts '5' OR SECTORCD='8310')
    odraft2 = odraft1.filter(
        (~pl.col('custcd').cast(pl.Utf8).is_in(['77', '78', '95', '96'])) &
        (
            pl.col('sectorcd').cast(pl.Utf8).str.starts_with('5') |
            (pl.col('sectorcd').cast(pl.Utf8) == '8310')
        )
    )

    # Sort ODRAFT1 BY BRANCH FISSPURP ACCTNO
    odraft1_sorted = odraft1.sort(['branch', 'fisspurp', 'acctno'])

    # Sort ODRAFT2 BY BRANCH SECTORCD ACCTNO
    odraft2_sorted = odraft2.sort(['branch', 'sectorcd', 'acctno'])

    return rdate, odraft1_sorted, odraft2_sorted

# =============================================================================
# WRITE TEXT DATA FILE (ODLSD / ODLSDI) — 80-char fixed
# =============================================================================

def write_odlsd(odraft1: pl.DataFrame, output_path: str):
    """
    Write text data file.
    PUT @01 BRANCH 3. @04 ACCTNO 10. @14 APPRLIMT COMMA15.2 @31 BALANCE COMMA15.2
    Fixed record length 80.
    """
    lines = []
    for row in odraft1.iter_rows(named=True):
        line = make_line(TEXT_WIDTH)

        branch   = str(int(row['branch'])).rjust(3)   if row['branch']   is not None else '   '
        acctno   = str(int(row['acctno'])).rjust(10)  if row['acctno']   is not None else ' ' * 10
        apprlimt = fmt_comma15_2(row.get('apprlimt'))
        balance  = fmt_comma15_2(row['balance'])

        place_at(line, 1,  branch)
        place_at(line, 4,  acctno)
        place_at(line, 14, apprlimt)
        place_at(line, 31, balance)

        lines.append(finalize_line(line, TEXT_WIDTH))

    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        for ln in lines:
            f.write(ln + '\n')

# =============================================================================
# PAGE HEADER WRITERS
# =============================================================================

def write_newpage_fiss(f, pagecnt, branch, rdate, is_first_branch, bank_title, width=LINE_WIDTH):
    """Write new page header for FISS Purpose OD report. Returns updated pagecnt."""
    pagecnt += 1

    # E255 — page eject control
    line = make_line(width)
    place_at(line, 1, 'E255')
    f.write(finalize_line(line, width) + '\n')

    if is_first_branch:
        brno = make_brno(branch)
        line = make_line(width)
        place_at(line, 1,   'P000PBBEDPPBBEDP')
        place_at(line, 133, brno)
        f.write(finalize_line(line, width) + '\n')

    # P000 report title line
    line = make_line(width)
    place_at(line, 1,   'P000REPORT NO :  ODLIST')
    place_at(line, 44,  bank_title)
    place_at(line, 122, f'PAGE NO : {pagecnt}')
    f.write(finalize_line(line, width) + '\n')

    # P001 branch / report description line
    branch_z3 = fmt_z3(branch)
    line = make_line(width)
    place_at(line, 1,  'P001BRANCH    :  ')
    place_at(line, 22, branch_z3)
    place_at(line, 32, f'OD LISTING FOR FISS PURPOSE CODE (FOR ALL CUSTCODES) {rdate}')
    f.write(finalize_line(line, width) + '\n')

    # Two blank P001
    write_blank_p001(f, width)
    write_blank_p001(f, width)

    # Column header row 1
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 62,  'APPROVED')
    place_at(line, 76,  'OUTSTANDING')
    place_at(line, 89,  'FISS PURPOSE')
    place_at(line, 103, 'SECTOR')
    place_at(line, 111, 'CUST')
    place_at(line, 118, 'STATE')
    f.write(finalize_line(line, width) + '\n')

    # Column header row 2
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 10,  'A/C NO.')
    place_at(line, 19,  'NAME OF CUSTOMER')
    place_at(line, 65,  'LIMIT')
    place_at(line, 80,  'BALANCE')
    place_at(line, 95,  'CODE')
    place_at(line, 105, 'CODE')
    place_at(line, 111, 'CODE')
    place_at(line, 119, 'CODE')
    place_at(line, 124, 'FLAT RATE')
    f.write(finalize_line(line, width) + '\n')

    # Column header row 3 (LIMIT1..LIMIT5, RATE1..RATE5)
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 14,  'LIMIT1')
    place_at(line, 31,  'LIMIT2')
    place_at(line, 49,  'LIMIT3')
    place_at(line, 67,  'LIMIT4')
    place_at(line, 85,  'LIMIT5')
    place_at(line, 95,  'RATE1')
    place_at(line, 102, 'RATE2')
    place_at(line, 109, 'RATE3')
    place_at(line, 116, 'RATE4')
    place_at(line, 123, 'RATE5')
    f.write(finalize_line(line, width) + '\n')

    # Column header row 4 (COLL1..COLL5)
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 95,  'COLL1')
    place_at(line, 102, 'COLL2')
    place_at(line, 109, 'COLL3')
    place_at(line, 116, 'COLL4')
    place_at(line, 123, 'COLL5')
    f.write(finalize_line(line, width) + '\n')

    # Separator line
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 5,   '-' * 30)
    place_at(line, 35,  '-' * 30)
    place_at(line, 65,  '-' * 30)
    place_at(line, 95,  '-' * 30)
    place_at(line, 125, '-' * 10)
    f.write(finalize_line(line, width) + '\n')

    # Two blank P001
    write_blank_p001(f, width)
    write_blank_p001(f, width)

    return pagecnt


def write_newpage_sector(f, pagecnt, branch, rdate, is_first_branch, bank_title, width=LINE_WIDTH):
    """Write new page header for Sector/Construction OD report. Returns updated pagecnt."""
    pagecnt += 1

    line = make_line(width)
    place_at(line, 1, 'E255')
    f.write(finalize_line(line, width) + '\n')

    if is_first_branch:
        brno = make_brno(branch)
        line = make_line(width)
        place_at(line, 1,   'P000PBBEDPPBBEDP')
        place_at(line, 133, brno)
        f.write(finalize_line(line, width) + '\n')

    # P000 report title line
    line = make_line(width)
    place_at(line, 1,   'P000REPORT NO :  ODLIST')
    place_at(line, 44,  bank_title)
    place_at(line, 122, f'PAGE NO : {pagecnt}')
    f.write(finalize_line(line, width) + '\n')

    # P001 branch / report description line 1
    branch_z3 = fmt_z3(branch)
    line = make_line(width)
    place_at(line, 1,  'P001BRANCH    :  ')
    place_at(line, 22, branch_z3)
    place_at(line, 36, 'OD LISTING BY CONSTRUCTION (SECTCODE 5001-5999) AND')
    f.write(finalize_line(line, width) + '\n')

    # P001 report description line 2 (PUT @31 "REAL ESTATE...")
    line = make_line(width)
    place_at(line, 31, f'REAL ESTATE (SECTCODE 8310) FOR NON-INDI. CUSTOMER FOR {rdate}')
    f.write(finalize_line(line, width) + '\n')

    # Two blank P001
    write_blank_p001(f, width)
    write_blank_p001(f, width)

    # Column header row 1
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 62,  'APPROVED')
    place_at(line, 76,  'OUTSTANDING')
    place_at(line, 89,  'FISS PURPOSE')
    place_at(line, 103, 'SECTOR')
    place_at(line, 111, 'CUST')
    place_at(line, 118, 'STATE')
    f.write(finalize_line(line, width) + '\n')

    # Column header row 2
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 10,  'A/C NO.')
    place_at(line, 19,  'NAME OF CUSTOMER')
    place_at(line, 65,  'LIMIT')
    place_at(line, 80,  'BALANCE')
    place_at(line, 95,  'CODE')
    place_at(line, 105, 'CODE')
    place_at(line, 111, 'CODE')
    place_at(line, 119, 'CODE')
    place_at(line, 124, 'FLAT RATE')
    f.write(finalize_line(line, width) + '\n')

    # Column header row 3
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 14,  'LIMIT1')
    place_at(line, 31,  'LIMIT2')
    place_at(line, 49,  'LIMIT3')
    place_at(line, 67,  'LIMIT4')
    place_at(line, 85,  'LIMIT5')
    place_at(line, 95,  'RATE1')
    place_at(line, 102, 'RATE2')
    place_at(line, 109, 'RATE3')
    place_at(line, 116, 'RATE4')
    place_at(line, 123, 'RATE5')
    f.write(finalize_line(line, width) + '\n')

    # Column header row 4
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 95,  'COLL1')
    place_at(line, 102, 'COLL2')
    place_at(line, 109, 'COLL3')
    place_at(line, 116, 'COLL4')
    place_at(line, 123, 'COLL5')
    f.write(finalize_line(line, width) + '\n')

    # Separator line
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 5,   '-' * 30)
    place_at(line, 35,  '-' * 30)
    place_at(line, 65,  '-' * 30)
    place_at(line, 95,  '-' * 30)
    place_at(line, 125, '-' * 10)
    f.write(finalize_line(line, width) + '\n')

    # Two blank P001
    write_blank_p001(f, width)
    write_blank_p001(f, width)

    return pagecnt

# =============================================================================
# DATA ROW WRITER (3 lines per record)
# =============================================================================

def write_data_rows(f, row, width=LINE_WIDTH):
    """
    Write 3 PUT lines per data record (matching SAS output).
    Line 1: ACCTNO, NAME, APPRLIMT, BALANCE, FISSPURP, SECTORCD, CUSTCODE, STATE, FLATRATE
    Line 2: LIMIT1..LIMIT5, RATE1..RATE5
    Line 3: COL1..COL5
    """
    # --- Line 1 ---
    line = make_line(width)
    place_at(line, 1, 'P001')

    acctno   = str(int(row['acctno'])).rjust(12) if row['acctno'] is not None else ' ' * 12
    name     = str(row['name'])[:33]             if row['name']   is not None else ''
    apprlimt = fmt_comma18_2(row.get('apprlimt'))
    balance  = fmt_comma15_2(row['balance'])
    fisspurp = str(row['fisspurp'])[:4].rjust(4) if row['fisspurp']  is not None else '    '
    sectorcd = str(row['sectorcd'])[:4].rjust(4) if row['sectorcd']  is not None else '    '
    custcode = str(row['custcode']).rjust(4)      if row.get('custcode') is not None else '    '
    state    = str(row['state'])[:6].rjust(6)    if row['state']    is not None else '      '
    flatrate = fmt_comma8_2(row.get('flatrate'))

    place_at(line, 5,   acctno)
    place_at(line, 19,  name)
    place_at(line, 52,  apprlimt)
    place_at(line, 72,  balance)
    place_at(line, 95,  fisspurp)
    place_at(line, 105, sectorcd)
    place_at(line, 111, custcode)
    place_at(line, 117, state)
    place_at(line, 125, flatrate)
    f.write(finalize_line(line, width) + '\n')

    # --- Line 2 ---
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 5,   fmt_comma15_2(row.get('limit1')))
    place_at(line, 22,  fmt_comma15_2(row.get('limit2')))
    place_at(line, 40,  fmt_comma15_2(row.get('limit3')))
    place_at(line, 58,  fmt_comma15_2(row.get('limit4')))
    place_at(line, 76,  fmt_comma15_2(row.get('limit5')))
    place_at(line, 94,  fmt_comma6_2(row.get('rate1')))
    place_at(line, 101, fmt_comma6_2(row.get('rate2')))
    place_at(line, 108, fmt_comma6_2(row.get('rate3')))
    place_at(line, 115, fmt_comma6_2(row.get('rate4')))
    place_at(line, 122, fmt_comma6_2(row.get('rate5')))
    f.write(finalize_line(line, width) + '\n')

    # --- Line 3 ---
    line = make_line(width)
    place_at(line, 1,   'P001')
    place_at(line, 95,  str(row['col1']) if row.get('col1') is not None else '')
    place_at(line, 102, str(row['col2']) if row.get('col2') is not None else '')
    place_at(line, 109, str(row['col3']) if row.get('col3') is not None else '')
    place_at(line, 116, str(row['col4']) if row.get('col4') is not None else '')
    place_at(line, 123, str(row['col5']) if row.get('col5') is not None else '')
    f.write(finalize_line(line, width) + '\n')

# =============================================================================
# SUBTOTAL AND GRAND TOTAL WRITERS
# =============================================================================

def write_subtotal_fiss(f, fisspurp, bnmamt, width=LINE_WIDTH):
    """Write 4 subtotal lines for FISS Purpose group."""
    line = make_line(width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '---------------')
    f.write(finalize_line(line, width) + '\n')

    line = make_line(width)
    place_at(line, 1,  'P001')
    fisp_str = str(fisspurp) if fisspurp is not None else ''
    place_at(line, 37, f'SUBTOTAL FOR FISS PURPOSE {fisp_str}')
    place_at(line, 72, fmt_comma15_2(bnmamt))
    f.write(finalize_line(line, width) + '\n')

    line = make_line(width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '===============')
    f.write(finalize_line(line, width) + '\n')

    write_blank_p001(f, width)


def write_subtotal_sector(f, sectorcd, bnmamt, width=LINE_WIDTH):
    """Write 4 subtotal lines for Sector Code group."""
    line = make_line(width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '---------------')
    f.write(finalize_line(line, width) + '\n')

    line = make_line(width)
    place_at(line, 1,  'P001')
    sect_str = str(sectorcd) if sectorcd is not None else ''
    place_at(line, 37, f'SUBTOTAL FOR SECTOR {sect_str}')
    place_at(line, 72, fmt_comma15_2(bnmamt))
    f.write(finalize_line(line, width) + '\n')

    line = make_line(width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '===============')
    f.write(finalize_line(line, width) + '\n')

    write_blank_p001(f, width)


def write_grand_total(f, brchamt, width=LINE_WIDTH):
    """Write 4 grand total lines for branch."""
    line = make_line(width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '---------------')
    f.write(finalize_line(line, width) + '\n')

    line = make_line(width)
    place_at(line, 1,  'P001')
    place_at(line, 37, 'GRAND TOTAL ')
    place_at(line, 72, fmt_comma15_2(brchamt))
    f.write(finalize_line(line, width) + '\n')

    line = make_line(width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '===============')
    f.write(finalize_line(line, width) + '\n')

    write_blank_p001(f, width)

# =============================================================================
# WRITE RPS REPORT — FISS PURPOSE (ODRAFT1)
# =============================================================================

def write_odlst_fiss(odraft1: pl.DataFrame, rdate: str, output_path: str,
                     bank_title: str, mode: str = 'w', width: int = LINE_WIDTH):
    """
    Write OD RPS report grouped by BRANCH / FISSPURP / ACCTNO.
    Each record produces 3 output lines. Equivalent to the first DATA _NULL_ block.
    """
    rows = odraft1.to_dicts()
    n = len(rows)

    with open(output_path, mode, encoding='ascii', errors='replace') as f:
        linecnt       = 0
        pagecnt       = 0
        brchamt       = 0.0
        bnmamt        = 0.0
        prev_branch   = None
        prev_fisspurp = None

        for idx, row in enumerate(rows):
            branch   = row['branch']
            fisspurp = row['fisspurp']
            balance  = row['balance'] if row['balance'] is not None else 0.0

            is_first_branch   = (branch != prev_branch)
            is_first_fisspurp = is_first_branch or (fisspurp != prev_fisspurp)

            is_last_branch   = (idx == n - 1) or (rows[idx + 1]['branch'] != branch)
            is_last_fisspurp = (idx == n - 1) or \
                               (rows[idx + 1]['fisspurp'] != fisspurp) or \
                               (rows[idx + 1]['branch'] != branch)

            if is_first_branch:
                pagecnt = 0
                brchamt = 0.0
                pagecnt = write_newpage_fiss(f, pagecnt, branch, rdate,
                                             True, bank_title, width)
                linecnt = LINECNT_START

            if is_first_fisspurp:
                bnmamt = 0.0

            brchamt += balance
            bnmamt  += balance

            write_data_rows(f, row, width)
            linecnt += 3

            if linecnt > LINECNT_MAX:
                pagecnt = write_newpage_fiss(f, pagecnt, branch, rdate,
                                             False, bank_title, width)
                linecnt = LINECNT_START

            if is_last_fisspurp:
                write_subtotal_fiss(f, fisspurp, bnmamt, width)
                linecnt += 4

            if linecnt > LINECNT_MAX:
                pagecnt = write_newpage_fiss(f, pagecnt, branch, rdate,
                                             False, bank_title, width)
                linecnt = LINECNT_START

            if is_last_branch:
                write_grand_total(f, brchamt, width)
                # Note: SAS does not increment LINECNT after LAST.BRANCH grand total

            prev_branch   = branch
            prev_fisspurp = fisspurp

# =============================================================================
# WRITE RPS REPORT — SECTOR CODE (ODRAFT2)
# =============================================================================

def write_odlst_sector(odraft2: pl.DataFrame, rdate: str, output_path: str,
                       bank_title: str, mode: str = 'a', width: int = LINE_WIDTH):
    """
    Write OD RPS report grouped by BRANCH / SECTORCD / ACCTNO.
    Each record produces 3 output lines. Equivalent to the second DATA _NULL_ block (FILE ODLST MOD).
    """
    rows = odraft2.to_dicts()
    n = len(rows)

    with open(output_path, mode, encoding='ascii', errors='replace') as f:
        linecnt        = 0
        pagecnt        = 0
        brchamt        = 0.0
        bnmamt         = 0.0
        prev_branch    = None
        prev_sectorcd  = None

        for idx, row in enumerate(rows):
            branch   = row['branch']
            sectorcd = row['sectorcd']
            balance  = row['balance'] if row['balance'] is not None else 0.0

            is_first_branch   = (branch != prev_branch)
            is_first_sectorcd = is_first_branch or (sectorcd != prev_sectorcd)

            is_last_branch   = (idx == n - 1) or (rows[idx + 1]['branch'] != branch)
            is_last_sectorcd = (idx == n - 1) or \
                               (rows[idx + 1]['sectorcd'] != sectorcd) or \
                               (rows[idx + 1]['branch'] != branch)

            if is_first_branch:
                pagecnt = 0
                brchamt = 0.0
                pagecnt = write_newpage_sector(f, pagecnt, branch, rdate,
                                               True, bank_title, width)
                linecnt = LINECNT_START

            if is_first_sectorcd:
                bnmamt = 0.0

            brchamt += balance
            bnmamt  += balance

            write_data_rows(f, row, width)
            linecnt += 3

            if linecnt > LINECNT_MAX:
                pagecnt = write_newpage_sector(f, pagecnt, branch, rdate,
                                               False, bank_title, width)
                linecnt = LINECNT_START

            if is_last_sectorcd:
                write_subtotal_sector(f, sectorcd, bnmamt, width)
                linecnt += 4

            if linecnt > LINECNT_MAX:
                pagecnt = write_newpage_sector(f, pagecnt, branch, rdate,
                                               False, bank_title, width)
                linecnt = LINECNT_START

            if is_last_branch:
                write_grand_total(f, brchamt, width)
                # Note: SAS does not increment LINECNT after LAST.BRANCH grand total

            prev_branch   = branch
            prev_sectorcd = sectorcd

# =============================================================================
# MAIN: PBB SECTION
# =============================================================================

def run_pbb():
    """
    Process PBB (Public Bank Berhad) OD Listing.
    Equivalent to the first EIBMODLT SAS execution block.
    """
    print("Processing PBB OD Listing...")

    rdate, odraft1, odraft2 = prepare_data(
        BANK_REPTDATE_PARQUET,
        BANK_LOAN_PARQUET_PREFIX,
        BANK_CURRENT_PARQUET
    )

    # Write ODLSD text data file (DATA ODLD block)
    write_odlsd(odraft1, BANK_ODLSD_TXT)
    print(f"  Written: {BANK_ODLSD_TXT}")

    # Write ODLST RPS report - FISS Purpose (FILE ODLST, new)
    bank_title = 'P U B L I C   B A N K   B E R H A D'
    write_odlst_fiss(odraft1, rdate, BANK_ODLST_TXT, bank_title, mode='w')
    print(f"  Written FISS section: {BANK_ODLST_TXT}")

    # Write ODLST RPS report - Sector Code (FILE ODLST MOD, append)
    write_odlst_sector(odraft2, rdate, BANK_ODLST_TXT, bank_title, mode='a')
    print(f"  Appended Sector section: {BANK_ODLST_TXT}")

    # //STEP01 EXEC PGM=IEFBR14 - Delete RBP2.B033.ODLISTR*.RPS datasets (no-op in Python)
    # //STEP02 EXEC PGM=SPLIB136 - Insert control characters for RPS, split by region (no-op in Python)
    # //INFIL1 DD DSN=RMDS.OPC.BANKCODE(KLREGION) - KL Region bank codes (no-op in Python)
    # //INFIL2 DD DSN=RMDS.OPC.BANKCODE(BGREGION) - BG Region bank codes (no-op in Python)
    # //INFIL3 DD DSN=RMDS.OPC.BANKCODE(JBREGION) - JB Region bank codes (no-op in Python)
    # //INFIL4 DD DSN=RMDS.OPC.BANKCODE(SBREGION) - SB Region bank codes (no-op in Python)
    # //INFIL5 DD DSN=RMDS.OPC.BANKCODE(PPREGION) - PP Region bank codes (no-op in Python)
    # //INFIL6 DD DSN=RMDS.OPC.BANKCODE(TTREGION) - TT Region bank codes (no-op in Python)

    print("PBB OD Listing complete.")

# =============================================================================
# MAIN: PIBB SECTION
# =============================================================================

def run_pibb():
    """
    Process PIBB (Public Islamic Bank Berhad) OD Listing.
    Equivalent to the EIBMODLI SAS execution block (FOR PIBB).
    """
    print("Processing PIBB OD Listing...")

    rdate, odraft1, odraft2 = prepare_data(
        PIBB_REPTDATE_PARQUET,
        PIBB_LOAN_PARQUET_PREFIX,
        PIBB_CURRENT_PARQUET
    )

    # Write ODLSDI text data file (DATA ODLD block)
    write_odlsd(odraft1, PIBB_ODLSDI_TXT)
    print(f"  Written: {PIBB_ODLSDI_TXT}")

    # Write ODLSTI RPS report - FISS Purpose (FILE ODLSTI, new)
    pibb_title = 'P U B L I C   I S L A M I C  B A N K   B E R H A D'
    write_odlst_fiss(odraft1, rdate, PIBB_ODLSTI_TXT, pibb_title, mode='w')
    print(f"  Written FISS section: {PIBB_ODLSTI_TXT}")

    # Write ODLSTI RPS report - Sector Code (FILE ODLSTI MOD, append)
    write_odlst_sector(odraft2, rdate, PIBB_ODLSTI_TXT, pibb_title, mode='a')
    print(f"  Appended Sector section: {PIBB_ODLSTI_TXT}")

    # //STEP03 EXEC PGM=IEFBR14 - Delete RBP2.B051.ODLISTR*.RPS datasets (no-op in Python)
    # //STEP04 EXEC PGM=SPLIB136 - Insert control characters for RPS, split by region (no-op in Python)
    # //INFIL1 DD DSN=RMDS.OPC.BANKCODE(KLREGION) - KL Region bank codes (no-op in Python)
    # //INFIL2 DD DSN=RMDS.OPC.BANKCODE(BGREGION) - BG Region bank codes (no-op in Python)
    # //INFIL3 DD DSN=RMDS.OPC.BANKCODE(JBREGION) - JB Region bank codes (no-op in Python)
    # //INFIL4 DD DSN=RMDS.OPC.BANKCODE(SBREGION) - SB Region bank codes (no-op in Python)
    # //INFIL5 DD DSN=RMDS.OPC.BANKCODE(PPREGION) - PP Region bank codes (no-op in Python)
    # //INFIL6 DD DSN=RMDS.OPC.BANKCODE(TTREGION) - TT Region bank codes (no-op in Python)

    print("PIBB OD Listing complete.")

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    run_pbb()
    run_pibb()
