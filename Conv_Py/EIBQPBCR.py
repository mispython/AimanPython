#!/usr/bin/env python3
"""
PROGRAM : EIBQPBCR.py
DATE    : 30.03.99
PURPOSE : TO OFFER PB PREMIUM CLUB AUTOMATIC MEMBERSHIP
REPORT  : LISTING OF ELIGIBLE DEPOSITORS FOR PB PREMIUM CLUB
NOTE    : INPUT ADDR.NAMELST IS OUTPUT FROM PGM EIBQADR2
"""

# %INC PGM(PBBELF)
#
# PBBELF: provides the BRCHCD format — PUT(BRANCH, BRCHCD.) — actively used
#         in the DATA steps of this program to construct the branch display
#         string: PUT(BRANCH,Z3.) || '/' || PUT(BRANCH,BRCHCD.)
#         Imported below as format_brchcd().

from PBBELF import format_brchcd   # BRCHCD format: branch numeric -> 3-letter name

import datetime
import duckdb
import polars as pl
from pathlib import Path

# ---------------------------------------------------------------------------
# Path Configuration
# ---------------------------------------------------------------------------
INPUT_DIR   = Path("input")
OUTPUT_DIR  = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PARQUET_NEW      = INPUT_DIR / "addr_new.parquet"
PARQUET_AUT      = INPUT_DIR / "addr_aut.parquet"
PARQUET_P50      = INPUT_DIR / "addr_p50.parquet"
PARQUET_HL       = INPUT_DIR / "addr_hl.parquet"
PARQUET_HP       = INPUT_DIR / "addr_hp.parquet"
PARQUET_REPTDATE = INPUT_DIR / "addr_reptdate.parquet"

OUTPUT_FILE = OUTPUT_DIR / "EIBQPBCR_report.txt"

# Page layout constants
PAGE_WIDTH  = 132
PAGE_LENGTH = 60   # lines per page (default SAS)

# ---------------------------------------------------------------------------
# PROC FORMAT equivalents
# NOTE: EIBQPBCR's SAPROD includes code 151 ('ACE STAFF'), which is
#       intentionally absent from EIBQPBCM. This matches the respective SAS sources.
# ---------------------------------------------------------------------------
SAPROD_MAP = {
    200: 'PLUS',
    202: 'YAA',
    203: '50PLUS',
    212: 'WISE',
    213: 'PB SAVELINK',
    214: 'MUDHARABAH BESTARI',
    150: 'ACE NORMAL',
    151: 'ACE STAFF',
    152: 'ACE EXTERNAL',
    156: 'PB CURRENTLINK',
    157: 'PB CURRENTLINK EXT',
    204: 'AL-WADIAH SA',
    100: 'PLUS CURRENT',
    102: 'PLUS CA EXT',
    160: 'AL-WADIAH CA',
    162: 'AL-WADIAH CA EXT',
}

SAPURP_MAP = {
    '1': 'PERSONAL',
    '2': 'JOINT',
    '5': 'ON-BEHALF',
    '6': 'ON-BEHALF',
}


def fmt_saprod(val) -> str:
    try:
        return SAPROD_MAP.get(int(val), 'UNKNOWN')
    except (ValueError, TypeError):
        return 'UNKNOWN'


def fmt_sapurp(val) -> str:
    return SAPURP_MAP.get(str(val).strip(), 'UNKNOWN')


def fmt_comma14_2(val) -> str:
    """Format numeric value as COMMA14.2: right-justified, 14 chars, 2 decimal places."""
    try:
        fval = float(val) if val is not None else 0.0
    except (ValueError, TypeError):
        fval = 0.0
    return f"{fval:,.2f}".rjust(14)


def build_branch_string(branch_val) -> str:
    """
    Replicate: PUT(BRANCH,Z3.) || '/' || PUT(BRANCH,BRCHCD.)
    BRCHCD format is sourced from PBBELF via format_brchcd().
    """
    try:
        numeric_part = str(int(branch_val)).zfill(3)
    except (ValueError, TypeError):
        numeric_part = str(branch_val).zfill(3)
    name_part = format_brchcd(int(branch_val))
    return f"{numeric_part}/{name_part}"


# ---------------------------------------------------------------------------
# Read REPTDATE -> &RDATE (WORDDATX18. format)
# ---------------------------------------------------------------------------
def get_report_date() -> str:
    con = duckdb.connect()
    df = con.execute(f"SELECT REPTDATE FROM '{PARQUET_REPTDATE}' LIMIT 1").fetchdf()
    con.close()
    if df.empty:
        return ""
    rdate = df["REPTDATE"].iloc[0]
    if hasattr(rdate, 'strftime'):
        dt = rdate
    else:
        dt = pl.Series([rdate]).cast(pl.Date)[0]
        dt = datetime.date(dt.year, dt.month, dt.day)
    months = ["January", "February", "March", "April", "May", "June",
              "July", "August", "September", "October", "November", "December"]
    # WORDDATX18.: day + month left-padded to 9 + year, total 18 chars
    formatted = f"{dt.day} {months[dt.month - 1]:<9} {dt.year}"
    return formatted[:18]


# ---------------------------------------------------------------------------
# Load a parquet file, compute BRCH, drop BRANCH, rename BRCH -> BRANCH
# ---------------------------------------------------------------------------
def load_and_transform(parquet_path: Path) -> pl.DataFrame:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{parquet_path}'").pl()
    con.close()
    branch_col  = df["BRANCH"].to_list()
    brch_values = [build_branch_string(v) for v in branch_col]
    df = df.drop("BRANCH").with_columns(
        pl.Series("BRANCH", brch_values)
    )
    return df


# ---------------------------------------------------------------------------
# ASA Carriage Control helpers
# ASA codes: '1'=new page, ' '=single space, '0'=double space, '+'=overprint
# ---------------------------------------------------------------------------
def asa_line(cc: str, text: str, width: int = PAGE_WIDTH) -> str:
    """Return a single report line with ASA carriage control prefix."""
    return cc + text[:width]


# ---------------------------------------------------------------------------
# Report generator — PROC PRINT equivalent with ASA carriage control
# ---------------------------------------------------------------------------
def build_report_section(
    df: pl.DataFrame,
    title1: str,
    title2: str,
    title3: str,
    col_specs: list,    # list of (col_name, label, width, align, formatter)
    page_lines: list    # output accumulator
):
    """
    Renders a PROC PRINT-style report section with ASA carriage control.
    col_specs: [ (col_name, label, width, align='l'|'r', formatter_fn | None) ]
    """
    obs_col_width = 6   # SAS PROC PRINT adds an OBS column

    def make_header_lines() -> list:
        hdr = []
        # Title lines — centred over PAGE_WIDTH; '1' = ASA new page
        hdr.append(asa_line('1', title1.center(PAGE_WIDTH)))
        hdr.append(asa_line(' ', title2.center(PAGE_WIDTH)))
        hdr.append(asa_line(' ', title3.center(PAGE_WIDTH)))
        hdr.append(asa_line(' ', ''))   # blank separator line
        # Column header row: OBS + labels
        header_row = 'OBS'.ljust(obs_col_width)
        for (col_name, label, width, align, _) in col_specs:
            if align == 'r':
                header_row += label.rjust(width) + '  '
            else:
                header_row += label.ljust(width) + '  '
        hdr.append(asa_line(' ', header_row))
        hdr.append(asa_line(' ', ''))   # blank line after header
        return hdr

    header = make_header_lines()
    page_lines.extend(header)
    lines_on_page = len(header)

    for obs_idx, row in enumerate(df.iter_rows(named=True), start=1):
        detail = str(obs_idx).ljust(obs_col_width)
        for (col_name, label, width, align, formatter) in col_specs:
            raw_val = row.get(col_name)
            if raw_val is None:
                raw_val = ''
            if formatter:
                display = formatter(raw_val)
            else:
                display = str(raw_val) if raw_val != '' else '0'
            if align == 'r':
                detail += display.rjust(width) + '  '
            else:
                detail += str(display).ljust(width) + '  '
        page_lines.append(asa_line(' ', detail))
        lines_on_page += 1

        # Page break
        if lines_on_page >= PAGE_LENGTH - 1:
            header = make_header_lines()
            page_lines.extend(header)
            lines_on_page = len(header)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    rdate = get_report_date()

    # Load and transform datasets
    # PROC SORT DATA=ADDR.NEW OUT=NAMELST1; BY BRANCH NAME; (then DATA step rebuilds BRANCH)
    # PROC SORT DATA=ADDR.AUT OUT=NAMELST;  BY BRANCH NAME;
    # etc.
    # Each section is sorted BY BRANCH NAME per the individual PROC SORT statements.
    namelst1 = load_and_transform(PARQUET_NEW).sort(["BRANCH", "NAME"])   # ADDR.NEW
    namelst  = load_and_transform(PARQUET_AUT).sort(["BRANCH", "NAME"])   # ADDR.AUT
    namelst3 = load_and_transform(PARQUET_P50).sort(["BRANCH", "NAME"])   # ADDR.P50
    namelst4 = load_and_transform(PARQUET_HL).sort(["BRANCH", "NAME"])    # ADDR.HL
    namelst5 = load_and_transform(PARQUET_HP).sort(["BRANCH", "NAME"])    # ADDR.HP

    report_lines = []

    # ---------------------------------------------------------------------------
    # SECTION 1: NAMELST1 (ADDR.NEW)
    # %LET TTL=LISTING OF DEPOSITORS FOR PB PREMIUM CLUB AS AT;
    # PROC PRINT DATA=NAMELST1 LABEL;
    #   FORMAT PURPOSE $SAPURP. PRODUCT SAPROD. YTDAVAMT COMMA14.2;
    #   VAR BRANCH ACCTNO NAME PURPOSE PRODUCT YTDAVAMT;
    # ---------------------------------------------------------------------------
    ttl = "LISTING OF DEPOSITORS FOR PB PREMIUM CLUB AS AT"
    col_specs_deposit = [
        ("BRANCH",   "BRANCH CODE",             15, 'l', None),
        ("ACCTNO",   "ACCOUNT NO.",              15, 'l', None),
        ("NAME",     "CUSTOMER NAME",            30, 'l', None),
        ("PURPOSE",  "TYPE OF ACCOUNT",          15, 'l', fmt_sapurp),
        ("PRODUCT",  "PRODUCT TYPE",             18, 'l', fmt_saprod),
        ("YTDAVAMT", "YTD AVERAGE BALANCE (RM)", 14, 'r', fmt_comma14_2),
    ]
    build_report_section(
        namelst1,
        title1="REPORT NO : EIBQPBCR",
        title2="PUBLIC BANK BERHAD",
        title3=f"{ttl} {rdate}",
        col_specs=col_specs_deposit,
        page_lines=report_lines
    )

    # ---------------------------------------------------------------------------
    # SECTION 2: NAMELST (ADDR.AUT)
    # %LET TTL=LISTING OF ELIGIBLE DEPOSITORS FOR PB PREMIUM CLUB AS AT;
    # ---------------------------------------------------------------------------
    ttl = "LISTING OF ELIGIBLE DEPOSITORS FOR PB PREMIUM CLUB AS AT"
    build_report_section(
        namelst,
        title1="REPORT NO : EIBQPBCR",
        title2="PUBLIC BANK BERHAD",
        title3=f"{ttl} {rdate}",
        col_specs=col_specs_deposit,
        page_lines=report_lines
    )

    # ---------------------------------------------------------------------------
    # SECTION 3: NAMELST3 (ADDR.P50)
    # %LET TTL=LISTING OF 50 PLUS S/A FOR PB PREMIUM CLUB AS AT;
    # ---------------------------------------------------------------------------
    ttl = "LISTING OF 50 PLUS S/A FOR PB PREMIUM CLUB AS AT"
    build_report_section(
        namelst3,
        title1="REPORT NO : EIBQPBCR",
        title2="PUBLIC BANK BERHAD",
        title3=f"{ttl} {rdate}",
        col_specs=col_specs_deposit,
        page_lines=report_lines
    )

    # ---------------------------------------------------------------------------
    # SECTION 4: NAMELST4 (ADDR.HL) — Housing Loans
    # %LET TTL=LISTING OF HOUSING LOANS FOR PB PREMIUM CLUB AS AT;
    # PROC PRINT DATA=NAMELST4 LABEL;
    #   VAR BRANCH ACCTNO NAME LOANTYPE CUSTCODE NETPROC APVLIMIT;
    # ---------------------------------------------------------------------------
    ttl = "LISTING OF HOUSING LOANS FOR PB PREMIUM CLUB AS AT"
    col_specs_loan = [
        ("BRANCH",   "BRANCH CODE",        15, 'l', None),
        ("ACCTNO",   "ACCOUNT NO.",         15, 'l', None),
        ("NAME",     "CUSTOMER NAME",       30, 'l', None),
        ("LOANTYPE", "LOAN TYPE",           12, 'l', None),
        ("CUSTCODE", "CUSTCODE",            10, 'l', None),
        ("NETPROC",  "NETPROC",             10, 'l', None),
        ("APVLIMIT", "APPROVED LIMIT (RM)", 14, 'r', fmt_comma14_2),
    ]
    build_report_section(
        namelst4,
        title1="REPORT NO : EIBQPBCR",
        title2="PUBLIC BANK BERHAD",
        title3=f"{ttl} {rdate}",
        col_specs=col_specs_loan,
        page_lines=report_lines
    )

    # ---------------------------------------------------------------------------
    # SECTION 5: NAMELST5 (ADDR.HP) — Hire Purchase
    # %LET TTL=LISTING OF HIRE PURCHASE ACCTS FOR PB PREMIUM CLUB AS AT;
    # ---------------------------------------------------------------------------
    ttl = "LISTING OF HIRE PURCHASE ACCTS FOR PB PREMIUM CLUB AS AT"
    build_report_section(
        namelst5,
        title1="REPORT NO : EIBQPBCR",
        title2="PUBLIC BANK BERHAD",
        title3=f"{ttl} {rdate}",
        col_specs=col_specs_loan,
        page_lines=report_lines
    )

    # ---------------------------------------------------------------------------
    # Write report file with ASA carriage control characters
    # Each line: first character = ASA control code, rest = report text
    # ---------------------------------------------------------------------------
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for line in report_lines:
            f.write(line + "\n")

    print(f"Report written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
