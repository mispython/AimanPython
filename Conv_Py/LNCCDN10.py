# !/usr/bin/env python3
"""
Program: LNCCDN10
Function: Details for BORSTAT F/I/R & (S & NOTENO=98010) HP ONLY (A monthly report)
Modify (ESMR): 2006-1048
"""

import polars as pl
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

# Input paths
INPUT_PATH_LOANTEMP = "BNM_LOANTEMP.parquet"
INPUT_PATH_BRANCH = "RBP2_B033_PBB_BRANCH.parquet"
INPUT_PATH_LNNOTE = "SAP_PBB_MNILN_0_LNNOTE.parquet"

# Output path
OUTPUT_PATH_HP_RPS = "SAP_PBB_NPL_HP_RPS.txt"

# Page configuration
LRECL_RPS = 134

# %INC PGM(PBBLNFMT) - HPD products
HPD_PRODUCTS = [380, 381, 700, 705, 720, 725, 128, 130, 131, 132]

# Format mappings (placeholders - from PBBELF)
# %INC PGM(PBBELF)
CACNAME_MAP = {}  # Placeholder
BRCHCD_MAP = {}  # Placeholder


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_asa_line(line_content, carriage_control=' '):
    """Format a line with ASA carriage control character."""
    return f"{carriage_control}{line_content}"


def get_branch_code(branch_num):
    """Get branch abbreviation from branch number."""
    return BRCHCD_MAP.get(branch_num, str(branch_num).zfill(3))


def get_cac_name(branch_num):
    """Get CAC name from branch number."""
    return CACNAME_MAP.get(branch_num, f"CAC-{branch_num:03d}")


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_lnccdn10(rdate):
    """
    Main processing function for LNCCDN10.

    Args:
        rdate: Report date string in DD/MM/YY format
    """
    print("=" * 80)
    print("LNCCDN10 - HP F/I/R/T & Restructured Report")
    print("=" * 80)

    # Load branch data
    print("Loading branch data...")
    branch_df = pl.read_parquet(INPUT_PATH_BRANCH)

    # Load LOANTEMP
    print("Loading LOANTEMP...")
    loantemp_df = pl.read_parquet(INPUT_PATH_LOANTEMP)

    # Create LOANTEM2 with filtering and categorization
    print("Filtering and categorizing data...")

    # Add CENSUS9 field
    loantemp_df = loantemp_df.with_columns([
        pl.col('CENSUS').cast(str).str.slice(-1).alias('CENSUS9')
    ])

    # Filter conditions
    filtered_df = loantemp_df.filter(
        (pl.col('ARREAR') > 4) |
        (pl.col('BORSTAT') == 'R') |
        (pl.col('BORSTAT') == 'I') |
        (pl.col('BORSTAT') == 'F') |
        (pl.col('CENSUS9') == '9')
    )

    # Categorize records
    records = []
    for row in filtered_df.iter_rows(named=True):
        product = row['PRODUCT']
        borstat = row['BORSTAT']
        noteno = row['NOTENO']

        if product in HPD_PRODUCTS and borstat == 'F':
            records.append({**row, 'CAT': 'A', 'TYPE': '(DEFICIT)'})

        if product in HPD_PRODUCTS and borstat == 'I':
            records.append({**row, 'CAT': 'B', 'TYPE': '(IRREGULAR)'})

        if product in HPD_PRODUCTS and borstat == 'R':
            records.append({**row, 'CAT': 'C', 'TYPE': '(REPOSSESSED)'})

        # /*
        # IF PRODUCT IN &HPD AND BORSTAT = 'T' THEN DO;
        #    CAT  = 'D';
        #    TYPE = '(TOTAL LOSS INSURANCE CLAMIN)';
        #    OUTPUT;
        # END;
        # */

        if product in HPD_PRODUCTS and borstat == 'S' and noteno >= 98010:
            records.append({**row, 'CAT': 'E', 'TYPE': 'RESTRUCTURED'})

    if not records:
        print("No records found for LNCCDN10")
        Path(OUTPUT_PATH_HP_RPS).touch()
        return

    report_df = pl.DataFrame(records)

    # Sort by BRANCH
    report_df = report_df.sort('BRANCH')

    # Merge with branch data
    report_df = report_df.join(
        branch_df.select(['BRANCH', 'BRHCODE']),
        on='BRANCH',
        how='left'
    )

    # Merge with LNNOTE to get additional fields
    print("Merging with LNNOTE...")
    lnnote_df = pl.read_parquet(INPUT_PATH_LNNOTE).select([
        'ACCTNO', 'NOTENO', 'ISSUEDT', 'ORGBAL', 'CURBAL'
    ])

    report_df = report_df.join(
        lnnote_df,
        on=['ACCTNO', 'NOTENO'],
        how='left'
    )

    # Calculate ISTLPAID
    report_df = report_df.with_columns([
        ((pl.col('ORGBAL').fill_null(0) - pl.col('CURBAL').fill_null(0))).alias('ISTLPAID')
    ])

    # Add CAC and BRABBR
    report_df = report_df.with_columns([
        pl.col('BRANCH').map_elements(get_cac_name, return_dtype=pl.Utf8).alias('CAC'),
        pl.col('BRANCH').map_elements(get_branch_code, return_dtype=pl.Utf8).alias('BRABBR')
    ])

    # Sort by BRANCH, CAT, ACCTNO
    report_df = report_df.sort(['BRANCH', 'CAT', 'ACCTNO'])

    # Generate report
    print("Generating report...")
    with open(OUTPUT_PATH_HP_RPS, 'w') as f:
        generate_report_output(f, report_df, rdate)

    print(f"Generated: {OUTPUT_PATH_HP_RPS}")
    print("LNCCDN10 processing complete")


def generate_report_output(f, df, rdate):
    """Generate formatted output for LNCCDN10."""

    pagecnt = 0
    linecnt = 0
    totbrbal = 0
    totbrpd = 0
    totbrac = 0
    prev_branch = None
    prev_cat = None
    brbal = 0
    brpd = 0
    brac = 0

    # %MACRO PRNRPT;
    for idx, row in enumerate(df.iter_rows(named=True)):
        branch = row['BRANCH']
        cat = row['CAT']
        cat_type = row['TYPE']
        brabbr = row.get('BRHCODE', str(branch).zfill(3))

        # IF _N_=1
        if idx == 0:
            totbrbal = 0
            totbrpd = 0
            totbrac = 0

        # IF FIRST.BRANCH
        if branch != prev_branch:
            pagecnt = 0
            brbal = 0
            brpd = 0
            brac = 0
            # LINK NEWPAGE
            pagecnt += 1
            linecnt = 7
            write_page_header(f, branch, brabbr, cat_type, rdate, pagecnt)

        # IF FIRST.CAT
        if cat != prev_cat and branch == prev_branch:
            pagecnt = 0
            brbal = 0
            brpd = 0
            brac = 0
            # LINK NEWPAGE
            pagecnt += 1
            linecnt = 7
            write_page_header(f, branch, brabbr, cat_type, rdate, pagecnt)

        # Write detail line
        acctno = str(row['ACCTNO']).ljust(10)
        noteno = f"{row['NOTENO']:>6}" if row['NOTENO'] else ' ' * 6
        name = str(row.get('NAME', '')).ljust(24)[:24]
        product = f"{row['PRODUCT']:3d}" if row['PRODUCT'] else '   '
        issdte = row['ISSDTE'].strftime('%d/%m/%y') if row['ISSDTE'] else '  /  /  '
        bldate = row['BLDATE'].strftime('%d/%m/%y') if row.get('BLDATE') else '  /  /  '
        daydiff = f"{row.get('DAYDIFF', 0):5,d}" if row.get('DAYDIFF') else '     '
        balance = f"{row.get('BALANCE', 0):14,.2f}"
        istlpaid = f"{row.get('ISTLPAID', 0):14,.2f}"

        line = (f"{acctno}{noteno}  {name}  {product}  "
                f"{issdte}  {bldate}  {daydiff}  {balance}  {istlpaid}")

        f.write(format_asa_line(line.ljust(LRECL_RPS - 1)) + '\n')
        linecnt += 1

        brbal += row.get('BALANCE', 0)
        brpd += row.get('ISTLPAID', 0)
        brac += 1
        totbrbal += row.get('BALANCE', 0)
        totbrpd += row.get('ISTLPAID', 0)
        totbrac += 1

        # Check for page break
        if linecnt > 56:
            pagecnt += 1
            linecnt = 7
            write_page_header(f, branch, brabbr, cat_type, rdate, pagecnt)

        # IF LAST.CAT
        is_last_cat = (idx == len(df) - 1 or
                       df.row(idx + 1, named=True)['CAT'] != cat or
                       df.row(idx + 1, named=True)['BRANCH'] != branch)

        if is_last_cat:
            sep_line = '-' * 40 + ' ' * 40 + '-' * 40
            f.write(format_asa_line(sep_line.ljust(LRECL_RPS - 1)) + '\n')

            total_line = (f"CATEGORY TOTAL{'':14}NO OF A/C : {brac:10,d}{'':28}"
                          f"{brbal:14,.2f}  {brpd:14,.2f}")
            f.write(format_asa_line(total_line.ljust(LRECL_RPS - 1)) + '\n')

            f.write(format_asa_line(sep_line.ljust(LRECL_RPS - 1)) + '\n')
            linecnt += 3

            brbal = 0
            brpd = 0
            brac = 0

        # IF LAST.BRANCH
        is_last_branch = (idx == len(df) - 1 or
                          df.row(idx + 1, named=True)['BRANCH'] != branch)

        if is_last_branch:
            f.write(format_asa_line(' ' * (LRECL_RPS - 1)) + '\n')

            total_line = (f"BRANCH TOTAL{'':16}NO OF A/C : {totbrac:10,d}{'':28}"
                          f"{totbrbal:14,.2f}  {totbrpd:14,.2f}")
            f.write(format_asa_line(total_line.ljust(LRECL_RPS - 1)) + '\n')

            sep_line = '=' * 40 + ' ' * 40 + '=' * 40
            f.write(format_asa_line(sep_line.ljust(LRECL_RPS - 1)) + '\n')
            linecnt += 3

            totbrbal = 0
            totbrpd = 0
            totbrac = 0

        prev_branch = branch
        prev_cat = cat


def write_page_header(f, branch, brabbr, cat_type, rdate, pagecnt):
    """
    NEWPAGE: subroutine
    Write page header for LNCCDN10 report.
    """
    # PUT _PAGE_
    line1 = f"PUBLIC   BANK    BERHAD - BRANCH : {branch:3d} ({brabbr}){'':45}PAGE NO.: {pagecnt}"
    f.write(format_asa_line(line1.ljust(LRECL_RPS - 1), '1') + '\n')

    line2 = f"DETAIL OF ACCOUNTS BY BRANCH - {cat_type} AS AT {rdate}"
    f.write(format_asa_line(line2.ljust(LRECL_RPS - 1), ' ') + '\n')

    line3 = "PROGRAM-ID : LNCCDN10"
    f.write(format_asa_line(line3.ljust(LRECL_RPS - 1), ' ') + '\n')

    f.write(format_asa_line(' ' * (LRECL_RPS - 1), ' ') + '\n')

    line5 = f"{'':12}NOTE{'':36}  ISSUE  BILL  DAYS IN"
    f.write(format_asa_line(line5.ljust(LRECL_RPS - 1), ' ') + '\n')

    line6 = (f"ACCTNO{'':6}  NO  NAME{'':34}PRODUCT  DATE  DATE  ARREARS"
             f"{'':15}BALANCE  INSTALMENT PAID")
    f.write(format_asa_line(line6.ljust(LRECL_RPS - 1), ' ') + '\n')

    sep_line = '-' * 40 + ' ' * 40 + '-' * 40
    f.write(format_asa_line(sep_line.ljust(LRECL_RPS - 1), ' ') + '\n')


if __name__ == "__main__":
    # Example usage
    rdate = "22/02/24"  # Replace with actual report date
    process_lnccdn10(rdate)
