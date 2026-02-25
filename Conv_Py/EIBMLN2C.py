# !/usr/bin/env python3
"""
Program: EIBMLN2C
Purpose: Loan Listing by Construction (SectCode 5001-5999) and
            Real Estate (SectCode 8310) for Non-Individual Customers.
         Produces reports for both Public Bank Berhad (PBB) and Public Islamic Bank Berhad (PIBB).
         Output is a fixed-width report with ASA carriage control characters.
         RECFM=FBA, LRECL=134, BLKSIZE=13400
"""

import duckdb
import polars as pl
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR = Path("/data/sap")

# PBB paths
PBB_LOAN_PATH    = BASE_DIR / "pbb/mniln/reptdate.parquet"        # SAP.PBB.MNILN(0) - REPTDATE
PBB_LNLC_PATH    = BASE_DIR / "pbb/loanlist/sasdata"              # SAP.PBB.LOANLIST.SASDATA
PBB_OUTPUT_PATH  = BASE_DIR / "pbb/loanlis2.cold.txt"             # SAP.PBB.LOANLIS2.COLD

# PIBB paths
PIBB_LOAN_PATH   = BASE_DIR / "pibb/mniln/reptdate.parquet"       # SAP.PIBB.MNILN(0) - REPTDATE
PIBB_LNLCI_PATH  = BASE_DIR / "pibb/loanlist/sasdata"             # SAP.PIBB.LOANLIST.SASDATA
PIBB_OUTPUT_PATH = BASE_DIR / "pibb/loanlis2.cold.txt"            # SAP.PIBB.LOANLIS2.COLD

# Report layout constants (LRECL=134, RECFM=FBA)
LRECL      = 134
PAGE_LINES = 60  # lines per page (default)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_report_date(loan_parquet: Path) -> tuple:
    """Read REPTDATE from MNILN REPTDATE dataset and return formatted strings."""
    con = duckdb.connect()
    df = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{loan_parquet}') LIMIT 1"
    ).fetchdf()
    con.close()
    reptdate = df['REPTDATE'].iloc[0]
    if hasattr(reptdate, 'date'):
        reptdate = reptdate.date()
    # DDMMYY8. format -> DD/MM/YY
    rdate    = reptdate.strftime('%d/%m/%y')
    reptmon  = reptdate.strftime('%m')
    reptyear = reptdate.strftime('%Y')
    return rdate, reptmon, reptyear


def fmt_numeric(value, width: int, decimals: int) -> str:
    """Format a numeric value right-justified with fixed decimal places."""
    if value is None:
        return ' ' * width
    try:
        formatted = f"{float(value):>{width}.{decimals}f}"
        if len(formatted) > width:
            formatted = formatted[:width]
        return formatted
    except (ValueError, TypeError):
        return ' ' * width


def fmt_integer(value, width: int, zero_padded: bool = False) -> str:
    """Format an integer value, optionally zero-padded (Zn. format)."""
    if value is None:
        return ' ' * width
    try:
        if zero_padded:
            return f"{int(value):0{width}d}"
        return f"{int(value):{width}d}"
    except (ValueError, TypeError):
        return ' ' * width


def fmt_char(value, width: int) -> str:
    """Format a character value left-justified, padded/truncated to width."""
    if value is None:
        return ' ' * width
    s = str(value)
    return f"{s:<{width}.{width}}"


def asa_newpage() -> str:
    """ASA carriage control: '1' = form feed / new page."""
    return '1'


def asa_newline() -> str:
    """ASA carriage control: ' ' = single space (normal new line)."""
    return ' '


def pad_line(content: str, lrecl: int) -> str:
    """Pad or truncate a line body (excluding ASA char) to LRECL-1 characters."""
    body = lrecl - 1
    return f"{content:<{body}.{body}}"


def build_separator_line(col: int, count: int, char: str = '-') -> str:
    """
    Build a separator line with repeated characters starting at
    1-based column position col within the content area (no ASA char).
    """
    prefix = ' ' * (col - 1)
    return prefix + (char * count)


# ============================================================================
# COLUMN LAYOUT
# ============================================================================
# PROC REPORT COLUMN order (BRANCH is NOPRINT):
#   ACCTNO(10.)  NOTENO(5.)  NAME($24.)  APPRLIMT(13.2)  BALANCE(13.2)
#   FISSPURP($4.)  SECTORCD($4.)  CUSTCD($4.)  STATE($2.)
#   INTRATE(5.2)  LIABCODE($4.)  CCOLLTRL($4.)
#
# PBB: APPRLIMT header = 'APPROVED / LIMIT'
# PIBB: APPRLIMT header = 'APPROVE LIMIT'  (single header row, no split)
#
# Column tuple: (col_name, hdr_line1, hdr_line2, width, fmt_type, decimals)
# fmt_type: 'N' = numeric (right-justified), 'C' = character (left-justified)

COLUMNS_PBB = [
    ('ACCTNO',   'ACCOUNT',      'NUMBER',      10, 'N', 0),
    ('NOTENO',   'NOTE',         '',             5,  'N', 0),
    ('NAME',     'CUSTOMER NAME','',            24,  'C', 0),
    ('APPRLIMT', 'APPROVED',     'LIMIT',       13,  'N', 2),
    ('BALANCE',  'OUTSTANDING',  'BALANCE',     13,  'N', 2),
    ('FISSPURP', 'PUR',          'POSE',         4,  'C', 0),
    ('SECTORCD', 'SEC',          'TOR',          4,  'C', 0),
    ('CUSTCD',   'CUST',         'CODE',         4,  'C', 0),
    ('STATE',    'ST',           'CD',           2,  'C', 0),
    ('INTRATE',  'INT',          'RATE',         5,  'N', 2),
    ('LIABCODE', 'COLL',         'NOTE',         4,  'C', 0),
    ('CCOLLTRL', 'COLL',         'COMM',         4,  'C', 0),
]

COLUMNS_PIBB = [
    ('ACCTNO',   'ACCOUNT',      'NUMBER',      10, 'N', 0),
    ('NOTENO',   'NOTE',         '',             5,  'N', 0),
    ('NAME',     'CUSTOMER NAME','',            24,  'C', 0),
    ('APPRLIMT', 'APPROVE LIMIT','',            13,  'N', 2),
    ('BALANCE',  'OUTSTANDING',  'BALANCE',     13,  'N', 2),
    ('FISSPURP', 'PUR',          'POSE',         4,  'C', 0),
    ('SECTORCD', 'SEC',          'TOR',          4,  'C', 0),
    ('CUSTCD',   'CUST',         'CODE',         4,  'C', 0),
    ('STATE',    'ST',           'CD',           2,  'C', 0),
    ('INTRATE',  'INT',          'RATE',         5,  'N', 2),
    ('LIABCODE', 'COLL',         'NOTE',         4,  'C', 0),
    ('CCOLLTRL', 'COLL',         'COMM',         4,  'C', 0),
]

COL_SEP = 1  # spaces between columns


def build_header_rows(columns: list) -> tuple:
    """Build two header rows based on column definitions."""
    row1 = ''
    row2 = ''
    for _, hdr1, hdr2, width, fmt_type, _ in columns:
        if fmt_type == 'N':
            row1 += f"{hdr1:>{width}}" + ' ' * COL_SEP
            row2 += f"{hdr2:>{width}}" + ' ' * COL_SEP
        else:
            row1 += f"{hdr1:<{width}}" + ' ' * COL_SEP
            row2 += f"{hdr2:<{width}}" + ' ' * COL_SEP
    return row1.rstrip(), row2.rstrip()


def format_data_row(row: dict, columns: list) -> str:
    """Format a single data row according to column definitions."""
    line = ''
    for col_name, _, _, width, fmt_type, decimals in columns:
        val = row.get(col_name, None)
        if fmt_type == 'N':
            if decimals > 0:
                cell = fmt_numeric(val, width, decimals)
            else:
                cell = fmt_integer(val, width)
        else:
            cell = fmt_char(val, width)
        line += cell + ' ' * COL_SEP
    return line.rstrip()


# ============================================================================
# REPORT GENERATION - PBB
# ============================================================================

def generate_report_pbb(
    lnnote_df: pl.DataFrame,
    columns: list,
    rdate: str,
    title1: str,
    title2: str,
    title3: str,
    title4: str,
    output_path: Path,
):
    """
    Generate PBB Loan listing report (EIBMLN2C) with ASA carriage control.
    Output: RECFM=FBA, LRECL=134.

    PBB groups/breaks by SECTORCD (DEFINE SECTORCD / ORDER).
    Break compute uses: 'SUBTOTAL FOR SECTOR   ' SECTOR $4.
    Note: SAS source uses SECTOR (not SECTORCD) in the LINE statement for PBB.

    Subtotal/Grand total lines use 1-based column positions:
      @025 = position 25 in content (0-based index 24)
      @063 = position 63 in content (0-based index 62)
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if lnnote_df.is_empty():
        output_path.write_text('')
        return

    # Sort: BY BRANCH then SECTORCD then natural order
    df_sorted = lnnote_df.sort(['BRANCH', 'SECTORCD'])

    hdr_row1, hdr_row2 = build_header_rows(columns)

    lines      = []
    line_count = 0

    POS_25 = 24   # 0-based index for @025
    POS_63 = 62   # 0-based index for @063

    def emit(asa: str, content: str):
        nonlocal line_count
        padded = pad_line(content, LRECL)
        lines.append(asa + padded)
        if asa != '+':
            line_count += 1

    def check_page_break(needed: int = 1):
        if line_count + needed > PAGE_LINES:
            emit_page_header()

    def emit_page_header():
        nonlocal line_count
        emit(asa_newpage(), title1)
        emit(asa_newline(), title2)
        emit(asa_newline(), title3)
        emit(asa_newline(), title4)
        emit(asa_newline(), '')
        emit(asa_newline(), hdr_row1)
        emit(asa_newline(), hdr_row2)
        underline = '-' * len(hdr_row1.rstrip())
        emit(asa_newline(), underline)
        # HEADSKIP
        emit(asa_newline(), '')
        line_count = 9

    def build_compute_line(prefix_text: str, suffix_value: str) -> str:
        line = ' ' * POS_25 + prefix_text
        line = line.ljust(POS_63) + suffix_value
        return line

    emit_page_header()

    branches = df_sorted['BRANCH'].unique(maintain_order=True).to_list()

    for branch in branches:
        branch_df      = df_sorted.filter(pl.col('BRANCH') == branch)
        branch_bal_sum = 0.0

        sectorcds = branch_df['SECTORCD'].unique(maintain_order=True).to_list()

        for sectorcd in sectorcds:
            sc_df      = branch_df.filter(pl.col('SECTORCD') == sectorcd)
            sc_bal_sum = 0.0

            for row in sc_df.to_dicts():
                check_page_break(1)
                emit(asa_newline(), format_data_row(row, columns))
                try:
                    sc_bal_sum += float(row.get('BALANCE', 0) or 0)
                except (ValueError, TypeError):
                    pass

            branch_bal_sum += sc_bal_sum

            # BREAK AFTER SECTORCD compute block
            # LINE @025 51*'-'
            # LINE @025 'SUBTOTAL FOR SECTOR   ' SECTOR $4. @063 BALANCE.SUM 13.2
            # LINE @025 51*'-'
            # Note: SAS source uses variable name SECTOR (not SECTORCD) in LINE stmt
            check_page_break(3)
            emit(asa_newline(), build_separator_line(25, 51))
            sc_str       = fmt_char(sectorcd, 4)
            balance_str  = fmt_numeric(sc_bal_sum, 13, 2)
            subtotal_txt = 'SUBTOTAL FOR SECTOR   ' + sc_str
            emit(asa_newline(), build_compute_line(subtotal_txt, balance_str))
            emit(asa_newline(), build_separator_line(25, 51))

        # BREAK AFTER BRANCH compute block
        # LINE @025 51*'-'
        # LINE @025 'GRAND TOTAL FOR BRANCH   ' BRANCH Z3. @063 BALANCE.SUM 13.2
        # LINE @025 51*'-'
        check_page_break(3)
        emit(asa_newline(), build_separator_line(25, 51))
        branch_str  = fmt_integer(branch, 3, zero_padded=True)
        balance_str = fmt_numeric(branch_bal_sum, 13, 2)
        grand_txt   = 'GRAND TOTAL FOR BRANCH   ' + branch_str
        emit(asa_newline(), build_compute_line(grand_txt, balance_str))
        emit(asa_newline(), build_separator_line(25, 51))

    with open(output_path, 'w', encoding='utf-8', newline='\n') as f:
        for line in lines:
            f.write(line + '\n')

    print(f"Report written to: {output_path}")


# ============================================================================
# REPORT GENERATION - PIBB
# ============================================================================

def generate_report_pibb(
    lnnote_df: pl.DataFrame,
    columns: list,
    rdate: str,
    title1: str,
    title2: str,
    title3: str,
    title4: str,
    output_path: Path,
):
    """
    Generate PIBB Loan listing report (EIBMLN2C) with ASA carriage control.
    Output: RECFM=FBA, LRECL=134.

    PIBB groups/breaks by SECTORCD (DEFINE SECTORCD / ORDER).
    Break compute uses: 'SUBTOTAL FOR SECTOR     ' SECTORCD $4.

    Subtotal/Grand total lines use 1-based column positions:
      @025 = position 25 in content (0-based index 24)
      @063 = position 63 in content (0-based index 62)
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if lnnote_df.is_empty():
        output_path.write_text('')
        return

    # Sort: BY BRANCH then SECTORCD then natural order
    df_sorted = lnnote_df.sort(['BRANCH', 'SECTORCD'])

    hdr_row1, hdr_row2 = build_header_rows(columns)

    lines      = []
    line_count = 0

    POS_25 = 24   # 0-based index for @025
    POS_63 = 62   # 0-based index for @063

    def emit(asa: str, content: str):
        nonlocal line_count
        padded = pad_line(content, LRECL)
        lines.append(asa + padded)
        if asa != '+':
            line_count += 1

    def check_page_break(needed: int = 1):
        if line_count + needed > PAGE_LINES:
            emit_page_header()

    def emit_page_header():
        nonlocal line_count
        emit(asa_newpage(), title1)
        emit(asa_newline(), title2)
        emit(asa_newline(), title3)
        emit(asa_newline(), title4)
        emit(asa_newline(), '')
        emit(asa_newline(), hdr_row1)
        emit(asa_newline(), hdr_row2)
        underline = '-' * len(hdr_row1.rstrip())
        emit(asa_newline(), underline)
        # HEADSKIP
        emit(asa_newline(), '')
        line_count = 9

    def build_compute_line(prefix_text: str, suffix_value: str) -> str:
        line = ' ' * POS_25 + prefix_text
        line = line.ljust(POS_63) + suffix_value
        return line

    emit_page_header()

    branches = df_sorted['BRANCH'].unique(maintain_order=True).to_list()

    for branch in branches:
        branch_df      = df_sorted.filter(pl.col('BRANCH') == branch)
        branch_bal_sum = 0.0

        sectorcds = branch_df['SECTORCD'].unique(maintain_order=True).to_list()

        for sectorcd in sectorcds:
            sc_df      = branch_df.filter(pl.col('SECTORCD') == sectorcd)
            sc_bal_sum = 0.0

            for row in sc_df.to_dicts():
                check_page_break(1)
                emit(asa_newline(), format_data_row(row, columns))
                try:
                    sc_bal_sum += float(row.get('BALANCE', 0) or 0)
                except (ValueError, TypeError):
                    pass

            branch_bal_sum += sc_bal_sum

            # BREAK AFTER SECTORCD compute block
            # LINE @025 51*'-'
            # LINE @025 'SUBTOTAL FOR SECTOR     ' SECTORCD $4. @063 BALANCE.SUM 13.2
            # LINE @025 51*'-'
            check_page_break(3)
            emit(asa_newline(), build_separator_line(25, 51))
            sc_str       = fmt_char(sectorcd, 4)
            balance_str  = fmt_numeric(sc_bal_sum, 13, 2)
            subtotal_txt = 'SUBTOTAL FOR SECTOR     ' + sc_str
            emit(asa_newline(), build_compute_line(subtotal_txt, balance_str))
            emit(asa_newline(), build_separator_line(25, 51))

        # BREAK AFTER BRANCH compute block
        # LINE @025 51*'-'
        # LINE @025 'GRAND TOTAL FOR BRANCH   ' BRANCH Z3. @063 BALANCE.SUM 13.2
        # LINE @025 51*'-'
        check_page_break(3)
        emit(asa_newline(), build_separator_line(25, 51))
        branch_str  = fmt_integer(branch, 3, zero_padded=True)
        balance_str = fmt_numeric(branch_bal_sum, 13, 2)
        grand_txt   = 'GRAND TOTAL FOR BRANCH   ' + branch_str
        emit(asa_newline(), build_compute_line(grand_txt, balance_str))
        emit(asa_newline(), build_separator_line(25, 51))

    with open(output_path, 'w', encoding='utf-8', newline='\n') as f:
        for line in lines:
            f.write(line + '\n')

    print(f"Report written to: {output_path}")


# ============================================================================
# MAIN - PBB
# ============================================================================

def run_pbb():
    """Run Loan listing report for Public Bank Berhad."""
    rdate, reptmon, reptyear = get_report_date(PBB_LOAN_PATH)

    # DATA LNNOTE2: SET LNLC.NOTE2&REPTMON
    note2_path = PBB_LNLC_PATH / f"note2{reptmon}.parquet"
    con = duckdb.connect()
    lnnote2_df = con.execute(
        f"SELECT * FROM read_parquet('{note2_path}')"
    ).pl()
    con.close()

    #
    title1 = 'REPORT NO : LOANLIST                      PUBLIC BANK BERHAD'
    title2 = 'PROGRAM ID: EIBMLN2C'
    title3 = (
        'LOAN LISTING BY CONSTRUCTION (SECTCODE 5001-5999) AND'
        '                                       REPORT DATE: ' + rdate
    )
    title4 = 'REAL ESTATE (SECTCODE 8310) FOR NON-IND.CUSTOMER'

    generate_report_pbb(
        lnnote_df=lnnote2_df,
        columns=COLUMNS_PBB,
        rdate=rdate,
        title1=title1,
        title2=title2,
        title3=title3,
        title4=title4,
        output_path=PBB_OUTPUT_PATH,
    )


# ============================================================================
# MAIN - PIBB
# ============================================================================

def run_pibb():
    """Run Loan listing report for Public Islamic Bank Berhad."""
    rdate, reptmon, reptyear = get_report_date(PIBB_LOAN_PATH)

    # DATA LNNOTE2: SET LNLCI.NOTE2&REPTMON
    note2_path = PIBB_LNLCI_PATH / f"note2{reptmon}.parquet"
    con = duckdb.connect()
    lnnote2_df = con.execute(
        f"SELECT * FROM read_parquet('{note2_path}')"
    ).pl()
    con.close()

    #
    title1 = 'REPORT NO :  LOANLIST          PUBLIC ISLAMIC BANK BERHAD'
    title2 = 'PROGRAM ID:  EIBMLN2C'
    title3 = (
        'LOAN LISTING BY CONSTRUCTION (SECTCODE 5001-5999) AND'
        '                                       REPORT DATE: ' + rdate
    )
    title4 = 'REAL ESTATE (SECTCODE 8310) FOR NON-IND.CUSTOMER'

    generate_report_pibb(
        lnnote_df=lnnote2_df,
        columns=COLUMNS_PIBB,
        rdate=rdate,
        title1=title1,
        title2=title2,
        title3=title3,
        title4=title4,
        output_path=PIBB_OUTPUT_PATH,
    )


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    run_pbb()
    #
    # FOR PIBB
    run_pibb()
    #
