# !/usr/bin/env python3
"""
Program Name: EIBMLNLT
Purpose: Generate Loan Listing Reports for PBB (Public Bank Berhad) and PIBB (Public Islamic Bank Berhad)
         - Report 1: Loan Listing by FISS Purpose Code (all custcodes)
         - Report 2: Loan Listing by Construction (Sector 5001-5999) and Real Estate (8310) for Non-Individual Customers
         Outputs text data file (LNLISD/LNLISX) and RPS report file (LNLIST/LNLISR)
"""

import duckdb
import polars as pl
from datetime import datetime, date
import os

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

# Input paths
BANK_REPTDATE_PARQUET    = "input/bank/reptdate.parquet"
BANK_LOAN_PARQUET_PREFIX = "input/bank/loan"          # e.g. loan0101.parquet (LOAN&REPTMON&NOWK)
BANK_LNNOTE_PARQUET      = "input/bank/lnnote.parquet"
BANK_LNCOMM_PARQUET      = "input/bank/lncomm.parquet"

PIBB_REPTDATE_PARQUET    = "input/pibb/reptdate.parquet"
PIBB_LOAN_PARQUET_PREFIX = "input/pibb/loan"
PIBB_LNNOTE_PARQUET      = "input/pibb/lnnote.parquet"
PIBB_LNCOMM_PARQUET      = "input/pibb/lncomm.parquet"

# Output paths
BANK_LNLISD_TXT  = "output/bank/loanlist_text.txt"     # SAP.BANK.LOANLIST.TEXT  (80 char)
BANK_LNLIST_TXT  = "output/bank/loanlist_rps.txt"      # SAP.BANK.LOANLIST.RPS   (136 char)

PIBB_LNLISX_TXT  = "output/pibb/loanlist_text.txt"     # SAP.PIBB.LOANLIST.TEXT  (80 char)
PIBB_LNLISR_TXT  = "output/pibb/loanlist_rps.txt"      # SAP.PIBB.LOANLIST.RPS   (136 char)

# Ensure output directories exist
os.makedirs("output/bank", exist_ok=True)
os.makedirs("output/pibb", exist_ok=True)

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
        # SAS date (days since 1960-01-01)
        d = date(1960, 1, 1) + __import__('datetime').timedelta(days=int(d))
    return d.strftime('%d/%m/%y')

def fmt_comma15_2(val):
    """Format numeric as COMMA15.2 right-justified in 15 chars"""
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * 15
    formatted = f"{val:,.2f}"
    return formatted.rjust(15)

def fmt_comma5_2(val):
    """Format numeric as COMMA5.2 right-justified in 5 chars"""
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * 5
    formatted = f"{val:,.2f}"
    return formatted.rjust(5)

def fmt_z3(val):
    """Format numeric as Z3. (zero-padded 3 digits)"""
    if val is None:
        return '000'
    return str(int(val)).zfill(3)

def place_at(line_list, col, text):
    """
    Place text into line_list (1-indexed column).
    line_list is a mutable list of characters.
    """
    col0 = col - 1
    for i, ch in enumerate(text):
        pos = col0 + i
        if pos < len(line_list):
            line_list[pos] = ch
        else:
            # extend if needed
            while len(line_list) <= pos:
                line_list.append(' ')
            line_list[pos] = ch

def make_line(width=136):
    return [' '] * width

def finalize_line(line_list, width=136):
    s = ''.join(line_list)
    # Pad or trim to width
    return s.ljust(width)[:width]

# =============================================================================
# DETERMINE REPORT DATE VARIABLES FROM REPTDATE
# =============================================================================

def get_report_vars(reptdate_parquet):
    """
    Read REPTDATE from parquet, compute NOWK, RDATE, REPTMON, REPTYEAR.
    OPTIONS YEARCUTOFF=1950 logic: 2-digit years 50-99 -> 1950-1999, 00-49 -> 2000-2049.
    """
    con = duckdb.connect()
    row = con.execute(f"SELECT reptdate FROM read_parquet('{reptdate_parquet}') LIMIT 1").fetchone()
    con.close()
    reptdate_val = row[0]

    # Convert to Python date
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

    rdate    = reptdate.strftime('%d/%m/%y')
    reptmon  = str(reptdate.month).zfill(2)
    reptyear = str(reptdate.year)

    return wk, rdate, reptmon, reptyear

# =============================================================================
# DATA PREPARATION (shared logic for both PBB and PIBB)
# =============================================================================

def prepare_data(reptdate_parquet, loan_parquet_prefix, lnnote_parquet, lncomm_parquet):
    """
    Replicate the SAS data preparation steps:
      1. Determine report variables from REPTDATE
      2. PROC SORT LNNOTE (KEEP=ACCTNO NOTENO BANKNO STATE) BY ACCTNO NOTENO
      3. PROC SORT LOAN&REPTMON&NOWK BY ACCTNO NOTENO
      4. MERGE LOAN + LNNOTE by ACCTNO NOTENO, keep IF ACCTYPE='LN' -> LNOTE
      5. PROC SORT LNOTE BY ACCTNO COMMNO
      6. PROC SORT LNCOMM BY ACCTNO COMMNO
      7. MERGE LNOTE + LNCOMM by ACCTNO COMMNO, keep if A -> NOTE1
      8. NOTE2 = NOTE1 filtered by CUSTCD and SECTORCD rules
      9. PROC SORT NOTE1 -> LNNOTE1 BY BRANCH FISSPURP CUSTCD ACCTNO
     10. PROC SORT NOTE2 -> LNNOTE2 BY BRANCH SECTORCD CUSTCD ACCTNO
    Returns: wk, rdate, reptmon, reptyear, lnnote1 (polars df), lnnote2 (polars df)
    """
    wk, rdate, reptmon, reptyear = get_report_vars(reptdate_parquet)

    loan_parquet = f"{loan_parquet_prefix}{reptmon}{wk}.parquet"

    con = duckdb.connect()

    # LNNOTE: KEEP=ACCTNO NOTENO BANKNO STATE, sorted BY ACCTNO NOTENO
    lnnote_df = con.execute(f"""
        SELECT acctno, noteno, bankno, state
        FROM read_parquet('{lnnote_parquet}')
        ORDER BY acctno, noteno
    """).pl()

    # LOAN sorted BY ACCTNO NOTENO
    loan_df = con.execute(f"""
        SELECT *
        FROM read_parquet('{loan_parquet}')
        ORDER BY acctno, noteno
    """).pl()

    con.close()

    # MERGE LOAN + LNNOTE by ACCTNO NOTENO (left join from LOAN side, keep IF ACCTYPE='LN')
    # SAS MERGE with IN=A, IN=B, IF ACCTYPE='LN' - keeps rows where at least one dataset has the key
    # but filters to ACCTYPE='LN' from LOAN side
    merged = loan_df.join(lnnote_df, on=['acctno', 'noteno'], how='left', suffix='_note')

    # Resolve BANKNO and STATE: if available from lnnote, use that (right side wins in SAS MERGE for matching vars)
    # In SAS MERGE, the later dataset overwrites. lnnote has bankno and state, so they overwrite loan's versions if present
    if 'bankno_note' in merged.columns:
        merged = merged.with_columns([
            pl.when(pl.col('bankno_note').is_not_null()).then(pl.col('bankno_note')).otherwise(pl.col('bankno')).alias('bankno'),
            pl.col('bankno_note')
        ]).drop('bankno_note')
    if 'state_note' in merged.columns:
        merged = merged.with_columns([
            pl.when(pl.col('state_note').is_not_null()).then(pl.col('state_note')).otherwise(pl.col('state')).alias('state')
        ]).drop('state_note')

    # Filter ACCTYPE = 'LN'
    lnote = merged.filter(pl.col('acctype') == 'LN').select([
        'bankno', 'branch', 'acctno', 'noteno', 'name', 'balance',
        'sectorcd', 'custcd', 'intrate', 'ntbrch', 'commno', 'liabcode',
        'apprlimt', 'fisspurp', 'state'
    ]).sort(['acctno', 'commno'])

    # LNCOMM sorted BY ACCTNO COMMNO
    con2 = duckdb.connect()
    lncomm = con2.execute(f"""
        SELECT * FROM read_parquet('{lncomm_parquet}')
        ORDER BY acctno, commno
    """).pl()
    con2.close()

    # MERGE LNOTE + LNCOMM by ACCTNO COMMNO, keep IF A (keep all from lnote)
    note1 = lnote.join(lncomm, on=['acctno', 'commno'], how='left', suffix='_comm')

    # Resolve overlapping columns from lncomm (SAS: lncomm values overwrite for matching keys)
    for col in ['bankno', 'branch', 'noteno', 'name', 'balance', 'sectorcd', 'custcd',
                'intrate', 'ntbrch', 'liabcode', 'apprlimt', 'fisspurp', 'state']:
        comm_col = f"{col}_comm"
        if comm_col in note1.columns:
            note1 = note1.with_columns(
                pl.when(pl.col(comm_col).is_not_null()).then(pl.col(comm_col)).otherwise(pl.col(col)).alias(col)
            ).drop(comm_col)

    # Select NOTE1 columns
    note1_cols = ['bankno', 'branch', 'acctno', 'noteno', 'name', 'apprlimt', 'balance',
                  'sectorcd', 'custcd', 'state', 'intrate', 'ntbrch', 'commno', 'liabcode',
                  'ccolltrl', 'fisspurp']
    note1 = note1.select([c for c in note1_cols if c in note1.columns])

    # NOTE2: CUSTCD NOT IN ('77','78','95','96') AND (SECTORCD starts with '5' OR SECTORCD='8310')
    note2 = note1.filter(
        (~pl.col('custcd').cast(pl.Utf8).is_in(['77', '78', '95', '96'])) &
        (
            pl.col('sectorcd').cast(pl.Utf8).str.starts_with('5') |
            (pl.col('sectorcd').cast(pl.Utf8) == '8310')
        )
    )

    # LNNOTE1: NOTE1 sorted BY BRANCH FISSPURP CUSTCD ACCTNO
    lnnote1 = note1.sort(['branch', 'fisspurp', 'custcd', 'acctno'])

    # LNNOTE2: NOTE2 sorted BY BRANCH SECTORCD CUSTCD ACCTNO
    lnnote2 = note2.sort(['branch', 'sectorcd', 'custcd', 'acctno'])

    return wk, rdate, reptmon, reptyear, lnnote1, lnnote2

# =============================================================================
# WRITE LNLISD / LNLISX  (text data file, 80-char fixed)
# =============================================================================

def write_lnlisd(lnnote1: pl.DataFrame, output_path: str):
    """
    Write the text data file equivalent to FILE LNLISD/LNLISX.
    Only rows where BALANCE>0 OR APPRLIMT>0.
    PUT @001 BRANCH 3. @004 ACCTNO 10. @014 NOTENO 5.
        @020 APPRLIMT COMMA15.2 @040 BALANCE COMMA15.2
    Fixed record length 80.
    """
    filtered = lnnote1.filter(
        (pl.col('balance') > 0) | (pl.col('apprlimt') > 0)
    )

    lines = []
    for row in filtered.iter_rows(named=True):
        line = [' '] * 80

        branch  = str(int(row['branch'])).rjust(3)[:3] if row['branch'] is not None else '   '
        acctno  = str(int(row['acctno'])).rjust(10)[:10] if row['acctno'] is not None else ' ' * 10
        noteno  = str(int(row['noteno'])).rjust(5)[:5] if row['noteno'] is not None else '     '

        bal = row['balance'] if row['balance'] is not None else 0.0
        appr = row['apprlimt'] if row['apprlimt'] is not None else 0.0

        apprlimt_str = fmt_comma15_2(appr)
        balance_str  = fmt_comma15_2(bal)

        place_at(line, 1,  branch)
        place_at(line, 4,  acctno)
        place_at(line, 14, noteno)
        place_at(line, 20, apprlimt_str)
        place_at(line, 40, balance_str)

        lines.append(finalize_line(line, 80))

    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        for ln in lines:
            f.write(ln + '\n')

# =============================================================================
# RPS REPORT GENERATION HELPERS
# =============================================================================

def make_brno(branch):
    """Compute BRNO from branch number (matching SAS SELECT logic)."""
    b = int(branch)
    if b < 10:
        return f"BR0{b}"
    elif b < 100:
        return f"BR{b}"
    else:
        return f"B{b}"

def write_newpage_fiss(f, pagecnt, branch, bankno, rdate, is_first_branch, bank_title, line_width=136):
    """
    Write a new page header for FISS Purpose report.
    Returns updated pagecnt.
    """
    pagecnt += 1

    # 'E255' line  (ASA carriage control: E = skip to new page equivalent for this RPS system)
    line = make_line(line_width)
    place_at(line, 1, 'E255')
    f.write(finalize_line(line, line_width) + '\n')

    if is_first_branch:
        brno = make_brno(branch)
        line = make_line(line_width)
        place_at(line, 1, 'P000PBBEDPPBBEDP')
        place_at(line, 133, brno)
        f.write(finalize_line(line, line_width) + '\n')

    # P000 REPORT NO / BANK NAME / PAGE NO
    line = make_line(line_width)
    place_at(line, 1,   'P000REPORT NO :  LOANLIST')
    place_at(line, 44,  bank_title)
    place_at(line, 122, f'PAGE NO : {pagecnt}')
    f.write(finalize_line(line, line_width) + '\n')

    # P001 BRANCH line
    bankno_fmt = fmt_bankno(bankno)
    branch_z3  = fmt_z3(branch)
    line = make_line(line_width)
    place_at(line, 1,  'P001BRANCH    :  ')
    place_at(line, 18, bankno_fmt)
    place_at(line, 22, branch_z3)
    place_at(line, 31, f'LOAN LISTING BY FISS PURPOSE CODE (FOR ALL CUSTCODES) {rdate}')
    f.write(finalize_line(line, line_width) + '\n')

    # Two blank P001
    for _ in range(2):
        line = make_line(line_width)
        place_at(line, 1, 'P001')
        f.write(finalize_line(line, line_width) + '\n')

    # Column headers row 1
    line = make_line(line_width)
    place_at(line, 1,   'P001')
    place_at(line, 62,  'APPROVED')
    place_at(line, 76,  'OUTSTANDING')
    place_at(line, 88,  'FISS PURPOSE')
    place_at(line, 101, 'SECTOR')
    place_at(line, 108, 'CUST')
    place_at(line, 114, 'STATE')
    place_at(line, 120, 'INT')
    place_at(line, 126, 'COLL')
    place_at(line, 131, 'COLL')
    f.write(finalize_line(line, line_width) + '\n')

    # Column headers row 2
    line = make_line(line_width)
    place_at(line, 1,   'P001')
    place_at(line, 10,  'A/C NO.')
    place_at(line, 19,  'NOTE NO.')
    place_at(line, 29,  'NAME OF CUSTOMER')
    place_at(line, 65,  'LIMIT')
    place_at(line, 80,  'BALANCE')
    place_at(line, 90,  'CODE')
    place_at(line, 101, 'CODE')
    place_at(line, 108, 'CODE')
    place_at(line, 114, 'CODE')
    place_at(line, 120, 'RATE')
    place_at(line, 126, 'NOTE')
    place_at(line, 131, 'COMM')
    f.write(finalize_line(line, line_width) + '\n')

    # Separator line
    line = make_line(line_width)
    place_at(line, 1,   'P001')
    place_at(line, 5,   '-' * 40)
    place_at(line, 45,  '-' * 40)
    place_at(line, 85,  '-' * 40)
    place_at(line, 125, '-' * 10)
    f.write(finalize_line(line, line_width) + '\n')

    # Two blank P001
    for _ in range(2):
        line = make_line(line_width)
        place_at(line, 1, 'P001')
        f.write(finalize_line(line, line_width) + '\n')

    return pagecnt

def write_newpage_sector(f, pagecnt, branch, bankno, rdate, is_first_branch, bank_title, line_width=136):
    """
    Write a new page header for Sector/Construction report.
    Returns updated pagecnt.
    """
    pagecnt += 1

    line = make_line(line_width)
    place_at(line, 1, 'E255')
    f.write(finalize_line(line, line_width) + '\n')

    if is_first_branch:
        brno = make_brno(branch)
        line = make_line(line_width)
        place_at(line, 1, 'P000PBBEDPPBBEDP')
        place_at(line, 133, brno)
        f.write(finalize_line(line, line_width) + '\n')

    # P000 REPORT NO / BANK NAME / PAGE NO
    line = make_line(line_width)
    place_at(line, 1,   'P000REPORT NO :  LOANLIST')
    place_at(line, 44,  bank_title)
    place_at(line, 122, f'PAGE NO : {pagecnt}')
    f.write(finalize_line(line, line_width) + '\n')

    # P001 BRANCH line
    bankno_fmt = fmt_bankno(bankno)
    branch_z3  = fmt_z3(branch)
    line = make_line(line_width)
    place_at(line, 1,  'P001BRANCH    :  ')
    place_at(line, 18, bankno_fmt)
    place_at(line, 22, branch_z3)
    place_at(line, 36, 'LOAN LISTING BY CONSTRUCTION (SECTCODE 5001-5999) AND')
    f.write(finalize_line(line, line_width) + '\n')

    # Second title line (PUT @30 "REAL ESTATE...")
    line = make_line(line_width)
    place_at(line, 30, f'REAL ESTATE (SECTCODE 8310) FOR NON-INDI. CUSTOMER FOR {rdate}')
    f.write(finalize_line(line, line_width) + '\n')

    # One blank P001
    line = make_line(line_width)
    place_at(line, 1, 'P001')
    f.write(finalize_line(line, line_width) + '\n')

    # Column headers row 1
    line = make_line(line_width)
    place_at(line, 1,   'P001')
    place_at(line, 62,  'APPROVED')
    place_at(line, 76,  'OUTSTANDING')
    place_at(line, 88,  'FISS PURPOSE')
    place_at(line, 101, 'SECTOR')
    place_at(line, 108, 'CUST')
    place_at(line, 114, 'STATE')
    place_at(line, 120, 'INT')
    place_at(line, 126, 'COLL')
    place_at(line, 131, 'COLL')
    f.write(finalize_line(line, line_width) + '\n')

    # Column headers row 2
    line = make_line(line_width)
    place_at(line, 1,   'P001')
    place_at(line, 10,  'A/C NO.')
    place_at(line, 19,  'NOTE NO.')
    place_at(line, 29,  'NAME OF CUSTOMER')
    place_at(line, 65,  'LIMIT')
    place_at(line, 80,  'BALANCE')
    place_at(line, 90,  'CODE')
    place_at(line, 101, 'CODE')
    place_at(line, 108, 'CODE')
    place_at(line, 114, 'CODE')
    place_at(line, 120, 'RATE')
    place_at(line, 126, 'NOTE')
    place_at(line, 131, 'COMM')
    f.write(finalize_line(line, line_width) + '\n')

    # Separator line
    line = make_line(line_width)
    place_at(line, 1,   'P001')
    place_at(line, 5,   '-' * 40)
    place_at(line, 45,  '-' * 40)
    place_at(line, 85,  '-' * 40)
    place_at(line, 125, '-' * 10)
    f.write(finalize_line(line, line_width) + '\n')

    # Two blank P001
    for _ in range(2):
        line = make_line(line_width)
        place_at(line, 1, 'P001')
        f.write(finalize_line(line, line_width) + '\n')

    return pagecnt

def write_data_row(f, row, line_width=136):
    """Write a single data detail row to the RPS report file."""
    line = make_line(line_width)
    place_at(line, 1, 'P001')

    acctno  = str(int(row['acctno'])).rjust(12) if row['acctno'] is not None else ' ' * 12
    noteno  = str(int(row['noteno'])).rjust(8)  if row['noteno'] is not None else ' ' * 8
    name    = str(row['name'])[:26] if row['name'] is not None else ''

    bal  = row['balance']  if row['balance']  is not None else 0.0
    appr = row['apprlimt'] if row['apprlimt'] is not None else 0.0

    # $4.-R = right-justified in 4 chars
    fisspurp = str(row['fisspurp'])[:4].rjust(4) if row['fisspurp'] is not None else '    '
    sectorcd = str(row['sectorcd'])[:4].rjust(4) if row['sectorcd'] is not None else '    '
    custcd   = str(row['custcd']).rjust(4)[:4]   if row['custcd']   is not None else '    '
    state    = str(row['state'])[:3].rjust(3)    if row['state']    is not None else '   '
    liabcode = str(row['liabcode']) if row['liabcode'] is not None else ''
    ccolltrl = str(row['ccolltrl']) if row['ccolltrl'] is not None else ''

    place_at(line, 5,   acctno)
    place_at(line, 19,  noteno)
    place_at(line, 29,  name)
    place_at(line, 55,  fmt_comma15_2(appr))
    place_at(line, 72,  fmt_comma15_2(bal))
    place_at(line, 90,  fisspurp)
    place_at(line, 101, sectorcd)
    place_at(line, 108, custcd)
    place_at(line, 114, state)
    place_at(line, 120, fmt_comma5_2(row['intrate']))
    place_at(line, 126, liabcode)
    place_at(line, 131, ccolltrl)

    f.write(finalize_line(line, line_width) + '\n')

def write_blank_p001(f, line_width=136):
    line = make_line(line_width)
    place_at(line, 1, 'P001')
    f.write(finalize_line(line, line_width) + '\n')

def write_subtotal_fiss(f, fisspurp, bnmamt, line_width=136):
    """Write subtotal lines for FISS Purpose."""
    # Separator
    line = make_line(line_width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '---------------')
    f.write(finalize_line(line, line_width) + '\n')

    # Subtotal
    line = make_line(line_width)
    place_at(line, 1,  'P001')
    fisp_str = str(fisspurp) if fisspurp is not None else ''
    place_at(line, 37, f'SUBTOTAL FOR FISS PURPOSE {fisp_str}')
    place_at(line, 72, fmt_comma15_2(bnmamt))
    f.write(finalize_line(line, line_width) + '\n')

    # Double separator
    line = make_line(line_width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '===============')
    f.write(finalize_line(line, line_width) + '\n')

    write_blank_p001(f, line_width)

def write_subtotal_sector(f, sectorcd, bnmamt, line_width=136):
    """Write subtotal lines for Sector Code."""
    line = make_line(line_width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '---------------')
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1,  'P001')
    sect_str = str(sectorcd) if sectorcd is not None else ''
    place_at(line, 37, f'SUBTOTAL FOR SECTOR {sect_str}')
    place_at(line, 72, fmt_comma15_2(bnmamt))
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '===============')
    f.write(finalize_line(line, line_width) + '\n')

    write_blank_p001(f, line_width)

def write_grand_total(f, brchamt, line_width=136):
    """Write grand total lines for branch."""
    line = make_line(line_width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '---------------')
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1,  'P001')
    place_at(line, 37, 'GRAND TOTAL FOR BRANCH')
    place_at(line, 72, fmt_comma15_2(brchamt))
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1,  'P001')
    place_at(line, 72, '===============')
    f.write(finalize_line(line, line_width) + '\n')

    write_blank_p001(f, line_width)

# =============================================================================
# WRITE RPS REPORT - FISS PURPOSE (LNNOTE1)
# =============================================================================

def write_lnlist_fiss(lnnote1: pl.DataFrame, rdate: str, output_path: str,
                      bank_title: str, mode: str = 'w', line_width: int = 136):
    """
    Write LNLIST RPS report grouped by BRANCH / FISSPURP / CUSTCD / ACCTNO.
    Equivalent to the first DATA _NULL_ block.
    """
    rows = lnnote1.to_dicts()
    n = len(rows)

    open_mode = mode  # 'w' for new, 'a' for MOD

    with open(output_path, open_mode, encoding='ascii', errors='replace') as f:
        linecnt  = 0
        pagecnt  = 0
        brchamt  = 0.0
        bnmamt   = 0.0
        prev_branch   = None
        prev_fisspurp = None

        for idx, row in enumerate(rows):
            branch   = row['branch']
            fisspurp = row['fisspurp']
            balance  = row['balance'] if row['balance'] is not None else 0.0

            is_first_branch   = (branch != prev_branch)
            is_first_fisspurp = (fisspurp != prev_fisspurp) or is_first_branch

            # Determine last flags
            is_last_branch   = (idx == n - 1) or (rows[idx + 1]['branch'] != branch)
            is_last_fisspurp = (idx == n - 1) or (rows[idx + 1]['fisspurp'] != fisspurp) or \
                               (rows[idx + 1]['branch'] != branch)

            if is_first_branch:
                pagecnt  = 0
                brchamt  = 0.0
                bankno   = row['bankno']
                pagecnt  = write_newpage_fiss(f, pagecnt, branch, bankno, rdate,
                                              True, bank_title, line_width)
                linecnt  = 11

            if is_first_fisspurp:
                bnmamt = 0.0

            brchamt += balance
            bnmamt  += balance

            write_data_row(f, row, line_width)
            linecnt += 1

            if linecnt > 55:
                pagecnt = write_newpage_fiss(f, pagecnt, branch, bankno, rdate,
                                             False, bank_title, line_width)
                linecnt = 11

            if is_last_fisspurp:
                write_subtotal_fiss(f, fisspurp, bnmamt, line_width)
                linecnt += 4

            if linecnt > 55:
                pagecnt = write_newpage_fiss(f, pagecnt, branch, bankno, rdate,
                                             False, bank_title, line_width)
                linecnt = 11

            if is_last_branch:
                write_grand_total(f, brchamt, line_width)
                linecnt += 4

            prev_branch   = branch
            prev_fisspurp = fisspurp

# =============================================================================
# WRITE RPS REPORT - SECTOR CODE (LNNOTE2)
# =============================================================================

def write_lnlist_sector(lnnote2: pl.DataFrame, rdate: str, output_path: str,
                        bank_title: str, mode: str = 'a', line_width: int = 136):
    """
    Write LNLIST RPS report grouped by BRANCH / SECTORCD / CUSTCD / ACCTNO.
    Equivalent to the second DATA _NULL_ block (FILE LNLIST MOD).
    """
    rows = lnnote2.to_dicts()
    n = len(rows)

    with open(output_path, mode, encoding='ascii', errors='replace') as f:
        linecnt  = 0
        pagecnt  = 0
        brchamt  = 0.0
        bnmamt   = 0.0
        prev_branch  = None
        prev_sectorcd = None

        for idx, row in enumerate(rows):
            branch   = row['branch']
            sectorcd = row['sectorcd']
            balance  = row['balance'] if row['balance'] is not None else 0.0

            is_first_branch   = (branch != prev_branch)
            is_first_sectorcd = (sectorcd != prev_sectorcd) or is_first_branch

            is_last_branch   = (idx == n - 1) or (rows[idx + 1]['branch'] != branch)
            is_last_sectorcd = (idx == n - 1) or (rows[idx + 1]['sectorcd'] != sectorcd) or \
                               (rows[idx + 1]['branch'] != branch)

            if is_first_branch:
                pagecnt  = 0
                brchamt  = 0.0
                bankno   = row['bankno']
                pagecnt  = write_newpage_sector(f, pagecnt, branch, bankno, rdate,
                                                True, bank_title, line_width)
                linecnt  = 11

            if is_first_sectorcd:
                bnmamt = 0.0

            brchamt += balance
            bnmamt  += balance

            write_data_row(f, row, line_width)
            linecnt += 1

            if linecnt > 55:
                pagecnt = write_newpage_sector(f, pagecnt, branch, bankno, rdate,
                                               False, bank_title, line_width)
                linecnt = 11

            if is_last_sectorcd:
                write_subtotal_sector(f, sectorcd, bnmamt, line_width)
                linecnt += 4

            if linecnt > 55:
                pagecnt = write_newpage_sector(f, pagecnt, branch, bankno, rdate,
                                               False, bank_title, line_width)
                linecnt = 11

            if is_last_branch:
                write_grand_total(f, brchamt, line_width)
                linecnt += 4

            prev_branch   = branch
            prev_sectorcd = sectorcd

# =============================================================================
# MAIN: PBB SECTION
# =============================================================================

def run_pbb():
    """
    Process PBB (Public Bank Berhad) section.
    Equivalent to the first EIBMLNLT SAS execution block.
    """
    print("Processing PBB section...")

    wk, rdate, reptmon, reptyear, lnnote1, lnnote2 = prepare_data(
        BANK_REPTDATE_PARQUET,
        BANK_LOAN_PARQUET_PREFIX,
        BANK_LNNOTE_PARQUET,
        BANK_LNCOMM_PARQUET
    )

    # Write LNLISD text data file
    # DATA LNDATA: SET LNNOTE1; IF BALANCE > 0 OR APPRLIMT > 0; FILE LNLISD;
    write_lnlisd(lnnote1, BANK_LNLISD_TXT)
    print(f"  Written: {BANK_LNLISD_TXT}")

    # Write LNLIST RPS report - FISS Purpose (FILE LNLIST, new)
    bank_title = 'P U B L I C   B A N K   B E R H A D'
    write_lnlist_fiss(lnnote1, rdate, BANK_LNLIST_TXT, bank_title, mode='w')
    print(f"  Written FISS section: {BANK_LNLIST_TXT}")

    # Write LNLIST RPS report - Sector Code (FILE LNLIST MOD, append)
    write_lnlist_sector(lnnote2, rdate, BANK_LNLIST_TXT, bank_title, mode='a')
    print(f"  Appended Sector section: {BANK_LNLIST_TXT}")

    # PROC DATASETS LIB=WORK NOLIST; DELETE NOTE1 LNNOTE1; (in-memory, no-op in Python)

    # //STEP01 EXEC PGM=IEFBR14 - Delete RBP2.B033.LOANLIS*.RPS datasets (no-op in Python)
    # //STEP02 EXEC PGM=SPLIB136 - Insert control characters for RPS, split by region (no-op in Python)
    # //INFIL1 DD DSN=RMDS.OPC.BANKCODE(KLREGION) - KL Region bank codes (no-op in Python)
    # //INFIL2 DD DSN=RMDS.OPC.BANKCODE(BGREGION) - BG Region bank codes (no-op in Python)
    # //INFIL3 DD DSN=RMDS.OPC.BANKCODE(JBREGION) - JB Region bank codes (no-op in Python)
    # //INFIL4 DD DSN=RMDS.OPC.BANKCODE(SBREGION) - SB Region bank codes (no-op in Python)
    # //INFIL5 DD DSN=RMDS.OPC.BANKCODE(PPREGION) - PP Region bank codes (no-op in Python)
    # //INFIL6 DD DSN=RMDS.OPC.BANKCODE(TTREGION) - TT Region bank codes (no-op in Python)

    print("PBB section complete.")

# =============================================================================
# MAIN: PIBB SECTION
# =============================================================================

def run_pibb():
    """
    Process PIBB (Public Islamic Bank Berhad) section.
    Equivalent to the second EIBMLNLT SAS execution block (FOR PIBB).
    """
    print("Processing PIBB section...")

    wk, rdate, reptmon, reptyear, lnnote1, lnnote2 = prepare_data(
        PIBB_REPTDATE_PARQUET,
        PIBB_LOAN_PARQUET_PREFIX,
        PIBB_LNNOTE_PARQUET,
        PIBB_LNCOMM_PARQUET
    )

    # Write LNLISX text data file
    write_lnlisd(lnnote1, PIBB_LNLISX_TXT)
    print(f"  Written: {PIBB_LNLISX_TXT}")

    # Write LNLISR RPS report - FISS Purpose (FILE LNLISR, new)
    pibb_title = 'P U B L I C   I S L A M I C   B A N K   B E R H A D'
    write_lnlist_fiss(lnnote1, rdate, PIBB_LNLISR_TXT, pibb_title, mode='w')
    print(f"  Written FISS section: {PIBB_LNLISR_TXT}")

    # Write LNLISR RPS report - Sector Code (FILE LNLISR MOD, append)
    write_lnlist_sector(lnnote2, rdate, PIBB_LNLISR_TXT, pibb_title, mode='a')
    print(f"  Appended Sector section: {PIBB_LNLISR_TXT}")

    # PROC DATASETS LIB=WORK NOLIST; DELETE NOTE1 LNNOTE1; (in-memory, no-op in Python)

    # //STEP01 EXEC PGM=IEFBR14 - Delete RBP2.B051.LOANLIS*.RPS datasets (no-op in Python)
    # //STEP02 EXEC PGM=SPLIB136 - Insert control characters for RPS, split by region (no-op in Python)
    # //INFIL1 DD DSN=RMDS.OPC.BANKCODE(KLREGION) - KL Region bank codes (no-op in Python)
    # //INFIL2 DD DSN=RMDS.OPC.BANKCODE(BGREGION) - BG Region bank codes (no-op in Python)
    # //INFIL3 DD DSN=RMDS.OPC.BANKCODE(JBREGION) - JB Region bank codes (no-op in Python)
    # //INFIL4 DD DSN=RMDS.OPC.BANKCODE(SBREGION) - SB Region bank codes (no-op in Python)
    # //INFIL5 DD DSN=RMDS.OPC.BANKCODE(PPREGION) - PP Region bank codes (no-op in Python)
    # //INFIL6 DD DSN=RMDS.OPC.BANKCODE(TTREGION) - TT Region bank codes (no-op in Python)

    print("PIBB section complete.")

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    run_pbb()
    run_pibb()
