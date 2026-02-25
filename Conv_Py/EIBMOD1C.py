# !/usr/bin/env python3
"""
Program: EIBMOD1C
Purpose: OD Listing by FISS Purpose Code (for all CustCodes)
         Produces reports for both Public Bank Berhad (PBB) and Public Islamic Bank Berhad (PIBB).
         Output is a fixed-width report with ASA carriage control characters.
"""

import duckdb
import polars as pl
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR = Path("/data/sap")

# PBB paths
PBB_DEPOSIT_PATH  = BASE_DIR / "pbb/mnitb/reptdate.parquet"       # SAP.PBB.MNITB(0) - REPTDATE
PBB_ODLC_PATH     = BASE_DIR / "pbb/odlist/sasdata"               # SAP.PBB.ODLIST.SASDATA
PBB_OUTPUT_PATH   = BASE_DIR / "pbb/odlis1.cold.txt"              # SAP.PBB.ODLIS1.COLD

# PIBB paths
PIBB_DEPOSIT_PATH = BASE_DIR / "pibb/mnitb/reptdate.parquet"      # SAP.PIBB.MNITB(0) - REPTDATE
PIBB_ODLCI_PATH   = BASE_DIR / "pibb/odlist/sasdata"              # SAP.PIBB.ODLIST.SASDATA
PIBB_OUTPUT_PATH  = BASE_DIR / "pibb/odlis1.cold.txt"             # SAP.PIBB.ODLIS1.COLD

# Report layout constants (LRECL=134, RECFM=FBA)
LRECL      = 134
PAGE_LINES = 60  # lines per page (default)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_report_date(deposit_parquet: Path):
    """Read REPTDATE from MNITB and return formatted date strings."""
    con = duckdb.connect()
    df = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{deposit_parquet}') LIMIT 1"
    ).fetchdf()
    con.close()
    reptdate = df['REPTDATE'].iloc[0]
    if hasattr(reptdate, 'date'):
        reptdate = reptdate.date()
    # DDMMYY8. format -> DD/MM/YY
    rdate   = reptdate.strftime('%d/%m/%y')
    reptmon = reptdate.strftime('%m')
    return rdate, reptmon


def fmt_numeric(value, width: int, decimals: int) -> str:
    """Format a numeric value as right-justified with fixed decimal places."""
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


def asa_doublespace() -> str:
    """ASA carriage control: '0' = double space."""
    return '0'


def asa_overprint() -> str:
    """ASA carriage control: '+' = overprint (no line feed)."""
    return '+'


def pad_line(content: str, lrecl: int) -> str:
    """Pad or truncate a line (excluding ASA char) to LRECL-1 characters."""
    body = lrecl - 1
    return f"{content:<{body}.{body}}"


def build_separator_line(col: int, count: int, char: str = '-') -> str:
    """Build a line with repeated characters starting at column position (1-based)."""
    # col is 1-based, content area is LRECL-1 wide (excl ASA char)
    prefix = ' ' * (col - 1)
    return prefix + (char * count)


# ============================================================================
# COLUMN LAYOUT
# ============================================================================
# Based on PROC REPORT COLUMN order and FORMAT definitions:
# BRANCH(NOPRINT) ACCTNO(10.) NAME($24.) APPRLIMT(12.2) BALANCE(12.2)
# FISSPURP($4.) SECTORCD($4.) CUSTCODE(4.) STATE($3.) FLATRATE(5.2)
# LIMIT1-5(12.2) RATE1-5(5.2) COL1-5($5.)
#
# PROC REPORT with HEADLINE/HEADSKIP and SPLIT='*'
# Header lines use '*' as line split character for column headers.

# Column definitions: (header_line1, header_line2, data_width, data_format)
# Positions are computed sequentially based on widths.
# SAS PROC REPORT default separation between columns is typically 1 space.

COLUMNS = [
    # (col_name,     hdr1,          hdr2,          width, fmt_type, decimals)
    ('ACCTNO',    'ACCOUNT',     'NUMBER',       10,    'N',  0),
    ('NAME',      'CUSTOMER NAME','',            24,    'C',  0),
    ('APPRLIMT',  'APPROVE LIMIT','',            12,    'N',  2),
    ('BALANCE',   'OUTSTANDING', 'BALANCE',      12,    'N',  2),
    ('FISSPURP',  'PUR',         'POSE',          4,    'C',  0),
    ('SECTORCD',  'SEC',         'TOR',           4,    'C',  0),
    ('CUSTCODE',  'CUST',        'CODE',          4,    'N',  0),
    ('STATE',     'ST',          'CD',            3,    'C',  0),
    ('FLATRATE',  'FLAT',        'RATE',          5,    'N',  2),
    ('LIMIT1',    'LIMIT1',      '',             12,    'N',  2),
    ('LIMIT2',    'LIMIT2',      '',             12,    'N',  2),
    ('LIMIT3',    'LIMIT3',      '',             12,    'N',  2),
    ('LIMIT4',    'LIMIT4',      '',             12,    'N',  2),
    ('LIMIT5',    'LIMIT5',      '',             12,    'N',  2),
    ('RATE1',     'RATE1',       '',              5,    'N',  2),
    ('RATE2',     'RATE2',       '',              5,    'N',  2),
    ('RATE3',     'RATE3',       '',              5,    'N',  2),
    ('RATE4',     'RATE4',       '',              5,    'N',  2),
    ('RATE5',     'RATE5',       '',              5,    'N',  2),
    ('COL1',      'COLL1',       '',              5,    'C',  0),
    ('COL2',      'COLL2',       '',              5,    'C',  0),
    ('COL3',      'COLL3',       '',              5,    'C',  0),
    ('COL4',      'COLL4',       '',              5,    'C',  0),
    ('COL5',      'COLL5',       '',              5,    'C',  0),
]

COL_SEP = 1  # space between columns


def build_header_rows() -> tuple:
    """Build two header rows for the column definitions."""
    row1 = ''
    row2 = ''
    for col_name, hdr1, hdr2, width, fmt_type, _ in COLUMNS:
        if fmt_type == 'N':
            row1 += f"{hdr1:>{width}}" + ' ' * COL_SEP
            row2 += f"{hdr2:>{width}}" + ' ' * COL_SEP
        else:
            row1 += f"{hdr1:<{width}}" + ' ' * COL_SEP
            row2 += f"{hdr2:<{width}}" + ' ' * COL_SEP
    return row1.rstrip(), row2.rstrip()


def format_data_row(row: dict) -> str:
    """Format a single data row according to column definitions."""
    line = ''
    for col_name, hdr1, hdr2, width, fmt_type, decimals in COLUMNS:
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
# REPORT GENERATION
# ============================================================================

def generate_report(
    odraft_df: pl.DataFrame,
    rdate: str,
    title1: str,
    title2: str,
    title3: str,
    title4: str,
    output_path: Path,
):
    """
    Generate the OD listing report with ASA carriage control characters.
    Output is RECFM=FBA, LRECL=134.
    Structure:
      - Titles on first page header
      - HEADSKIP: blank line after header
      - HEADLINE: underline after header
      - BY BRANCH grouping
      - BREAK AFTER FISSPURP: subtotal lines
      - BREAK AFTER BRANCH: grand total lines
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Sort data: BY BRANCH, then FISSPURP (GROUP), then natural order
    if odraft_df.is_empty():
        output_path.write_text('')
        return

    df_sorted = odraft_df.sort(['BRANCH', 'FISSPURP'])
    records = df_sorted.to_dicts()

    hdr_row1, hdr_row2 = build_header_rows()

    lines = []        # list of (asa_char, content_str)
    line_count = 0    # lines used on current page (excluding ASA)
    page_num = 1
    first_page = True

    def emit(asa: str, content: str):
        nonlocal line_count
        padded = pad_line(content, LRECL)
        lines.append(asa + padded)
        if asa != '+':
            line_count += 1

    def check_page_break(needed: int = 1):
        nonlocal line_count, page_num, first_page
        if line_count + needed > PAGE_LINES:
            emit_page_header()

    def emit_page_header():
        nonlocal line_count, page_num, first_page
        # New page
        emit(asa_newpage(), title1)
        emit(asa_newline(), title2)
        emit(asa_newline(), title3)
        emit(asa_newline(), title4)
        emit(asa_newline(), '')
        # Column header row 1
        emit(asa_newline(), hdr_row1)
        # Column header row 2
        emit(asa_newline(), hdr_row2)
        # HEADLINE: underline row
        underline = '-' * len(hdr_row1.rstrip())
        emit(asa_newline(), underline)
        # HEADSKIP: blank line
        emit(asa_newline(), '')
        page_num += 1
        first_page = False

    # Emit first page header
    emit_page_header()

    # Group by BRANCH
    branches = df_sorted['BRANCH'].unique(maintain_order=True).to_list()

    for branch in branches:
        branch_df = df_sorted.filter(pl.col('BRANCH') == branch)
        branch_balance_sum = 0.0

        # Group by FISSPURP within branch
        fisspurps = branch_df['FISSPURP'].unique(maintain_order=True).to_list()

        for fisspurp in fisspurps:
            fp_df = branch_df.filter(pl.col('FISSPURP') == fisspurp)
            fp_balance_sum = 0.0

            for row in fp_df.to_dicts():
                check_page_break(1)
                data_line = format_data_row(row)
                emit(asa_newline(), data_line)
                try:
                    fp_balance_sum += float(row.get('BALANCE', 0) or 0)
                except (ValueError, TypeError):
                    pass

            branch_balance_sum += fp_balance_sum

            # BREAK AFTER FISSPURP compute block
            # LINE @015 52*'-'
            check_page_break(3)
            emit(asa_newline(), build_separator_line(15, 52))
            # LINE @015 'SUBTOTAL FOR FISS PURPOSE   ' FISSPURP $4. @054 BALANCE.SUM 13.2
            fp_str = fmt_char(fisspurp, 4)
            balance_str = fmt_numeric(fp_balance_sum, 13, 2)
            subtotal_content = (
                ' ' * 14
                + f"{'SUBTOTAL FOR FISS PURPOSE   '}{fp_str}"
                + ' ' * (54 - 14 - 28 - 4 - 1)
                + balance_str
            )
            # Column @015 = position 15 (1-based), @054 = position 54
            # Content at @015: 14 spaces prefix
            subtotal_line = ' ' * 14 + 'SUBTOTAL FOR FISS PURPOSE   ' + fp_str
            # Pad to column 54 (1-based in content = index 53)
            content_pos54 = 53  # 0-based index in content (no ASA char)
            subtotal_line = subtotal_line.ljust(content_pos54) + balance_str
            emit(asa_newline(), subtotal_line)
            emit(asa_newline(), build_separator_line(15, 52))

        # BREAK AFTER BRANCH compute block
        # LINE @015 52*'-'
        check_page_break(3)
        emit(asa_newline(), build_separator_line(15, 52))
        # LINE @015 'GRAND TOTAL FOR BRANCH   ' BRANCH Z3. @054 BALANCE.SUM 13.2
        branch_str = fmt_integer(branch, 3, zero_padded=True)
        grand_line = ' ' * 14 + 'GRAND TOTAL FOR BRANCH   ' + branch_str
        grand_line = grand_line.ljust(content_pos54) + fmt_numeric(branch_balance_sum, 13, 2)
        emit(asa_newline(), grand_line)
        emit(asa_newline(), build_separator_line(15, 52))

    # Write output file
    with open(output_path, 'w', encoding='utf-8', newline='\n') as f:
        for line in lines:
            f.write(line + '\n')

    print(f"Report written to: {output_path}")


# ============================================================================
# MAIN - PBB
# ============================================================================

def run_pbb():
    """Run OD listing report for Public Bank Berhad."""
    rdate, reptmon = get_report_date(PBB_DEPOSIT_PATH)

    # DATA ODRAFT1: SET ODLC.ODRAF1&REPTMON
    odraf1_path = PBB_ODLC_PATH / f"odraf1{reptmon}.parquet"
    con = duckdb.connect()
    odraft1_df = con.execute(
        f"SELECT * FROM read_parquet('{odraf1_path}')"
    ).pl()
    con.close()

    #
    title1 = (
        'REPORT NO :  ODLIST                           PUBLIC BANK BERHAD'
    )
    title2 = 'PROGRAM ID:  EIBMOD1C'
    title3 = (
        'OD LISTING BY FISS PURPOSE CODE (FOR ALL CUSTCODES)'
        '                                       REPORT DATE: ' + rdate
    )
    title4 = '**'

    generate_report(
        odraft_df=odraft1_df,
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
    """Run OD listing report for Public Islamic Bank Berhad."""
    rdate, reptmon = get_report_date(PIBB_DEPOSIT_PATH)

    # DATA ODRAFT1: SET ODLCI.ODRAF1&REPTMON
    odraf1_path = PIBB_ODLCI_PATH / f"odraf1{reptmon}.parquet"
    con = duckdb.connect()
    odraft1_df = con.execute(
        f"SELECT * FROM read_parquet('{odraf1_path}')"
    ).pl()
    con.close()

    #
    title1 = (
        'REPORT NO :  ODLIST                         PUBLIC ISLAMIC BANK BERHAD'
    )
    title2 = 'PROGRAM ID:  EIBMOD1C'
    title3 = (
        'OD LISTING BY FISS PURPOSE CODE (FOR ALL CUSTCODES)'
        '                                       REPORT DATE: ' + rdate
    )
    title4 = '**'

    generate_report(
        odraft_df=odraft1_df,
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
