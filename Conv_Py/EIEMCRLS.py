#!/usr/bin/env python3
"""
File Name: EIEMCRLS
Report ID: EIQPROM2
Automailing Listing for Reinstatement of Loan
"""

import duckdb
import polars as pl
from datetime import date, timedelta
from pathlib import Path
import hashlib
import base64


# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Input paths
PROMOTE_LOAN_PATH = "/data/input/promote_loan_{month}.parquet"
LN_LNNAME_PATH = "/data/input/ln_lnname.parquet"
LNI_LNNAME_PATH = "/data/input/lni_lnname.parquet"

# Output paths
EMCPBB_PATH = "/data/output/emcpbb.txt"
EMCPBBS_PATH = "/data/output/emcpbbs.txt"
EMLPBB_PATH = "/data/output/emlpbb.txt"
EMLPBBS_PATH = "/data/output/emlpbbs.txt"
EMXPBB_PATH = "/data/output/emxpbb.txt"
EMCPIB_PATH = "/data/output/emcpib.txt"
EMCPIBS_PATH = "/data/output/emcpibs.txt"
EMLPIB_PATH = "/data/output/emlpib.txt"
EMLPIBS_PATH = "/data/output/emlpibs.txt"
EMXPIB_PATH = "/data/output/emxpib.txt"
REPORT_PATH = "/data/output/eiqprom2_report.txt"

# Create output directory if it doesn't exist
Path("/data/output").mkdir(parents=True, exist_ok=True)


# ============================================================================
# DATE CALCULATIONS
# ============================================================================

def calculate_report_dates():
    """Calculate report dates similar to SAS REPTDATE logic"""
    today = date.today()
    # Get first day of current month, then subtract 1 day to get last day of previous month
    first_of_month = today.replace(day=1)
    reptdate = first_of_month - timedelta(days=1)

    reptdt = reptdate.strftime('%d%m%y')  # DDMMYY6
    indxdt = reptdate.strftime('%Y%m%d')  # YYMMDDN8
    rdate = reptdate.strftime('%d/%m/%y')  # DDMMYY8 with slashes
    reptmon = f"{reptdate.month:02d}"  # Z2
    reptyr = str(reptdate.year)  # YEAR4
    mthnam = reptdate.strftime('%b').upper()  # MONNAME3 uppercase

    return {
        'reptdate': reptdate,
        'reptdt': reptdt,
        'indxdt': indxdt,
        'rdate': rdate,
        'reptmon': reptmon,
        'reptyr': reptyr,
        'mthnam': mthnam
    }


dates = calculate_report_dates()
REPTDT = dates['reptdt']
INDXDT = dates['indxdt']
RDATE = dates['rdate']
REPTMON = dates['reptmon']
REPTYR = dates['reptyr']
MTHNAM = dates['mthnam']


# ============================================================================
# ENCRYPTION FUNCTIONS (ID MASKING)
# ============================================================================

def encrypt_id(id_value):
    """
    Encrypt ID similar to %ENCR_ID macro
    Returns masked ID string (24 characters)
    """
    if not id_value or str(id_value).strip() == '':
        return ' ' * 24

    # Create a hash of the ID
    id_str = str(id_value).strip()
    hash_obj = hashlib.sha256(id_str.encode())
    hash_digest = hash_obj.digest()

    # Encode to base64 and take first 24 characters
    encoded = base64.b64encode(hash_digest).decode('ascii')[:24]

    return encoded


# ============================================================================
# DATA PROCESSING
# ============================================================================

print("Starting EIQPROM2 processing...")
print(f"Report Date: {RDATE}")
print(f"Report Month: {REPTMON}")

# Step 1: Load and filter PROMOTE.LOAN data
print("\nStep 1: Loading and filtering PROMOTE.LOAN data...")
promote_path = PROMOTE_LOAN_PATH.format(month=REPTMON)

rlslist = duckdb.query(f"""
    SELECT *
    FROM read_parquet('{promote_path}')
    WHERE REPAID > 100000
    ORDER BY GUAREND, REPAID DESC
""").pl()

# Remove duplicates keeping first record per GUAREND
rlslist = rlslist.unique(subset=['GUAREND'], keep='first')

# Sort by ACCTNO for merge
rlslist = rlslist.sort('ACCTNO')

print(f"  Records in RLSLIST: {len(rlslist)}")


# ============================================================================
# PBB PROCESSING
# ============================================================================

print("\nStep 2: Processing PBB data...")

# Load LN.LNNAME and merge with RLSLIST
pbbname = duckdb.query(f"""
    SELECT a.*
    FROM read_parquet('{LN_LNNAME_PATH}') a
    INNER JOIN rlslist b ON a.ACCTNO = b.ACCTNO
    ORDER BY a.ACCTNO
""").pl()

print(f"  Records in PBBNAME after merge: {len(pbbname)}")

# Add encrypted ID and split into PBBNAME and MAILPBB
pbbname = pbbname.with_columns([
    pl.col('NEWIC').str.strip_chars().alias('ID')
])

pbbname = pbbname.with_columns([
    pl.col('ID').map_elements(encrypt_id, return_dtype=pl.Utf8).alias('MASK_IDS')
])

# Split based on MAILCODE and EMAILADD
mailpbb = pbbname.filter(
    (pl.col('MAILCODE').is_in([' ', '13', '14'])) &
    (pl.col('EMAILADD').str.strip_chars() != '')
)

pbbname = pbbname.filter(
    ~((pl.col('MAILCODE').is_in([' ', '13', '14'])) &
      (pl.col('EMAILADD').str.strip_chars() != ''))
)

print(f"  Records in PBBNAME (non-email): {len(pbbname)}")
print(f"  Records in MAILPBB (email): {len(mailpbb)}")

# Write EMCPBB file
print("\n  Writing EMCPBB file...")
with open(EMCPBB_PATH, 'w') as f:
    rowcnt = 0
    for row in pbbname.iter_rows(named=True):
        rowcnt += 1
        line = (
            f"B"
            f"{REPTDT}"
            f"{str(row['NAMELN1'] or ''):<40.40}"
            f"{str(row['NAMELN2'] or ''):<40.40}"
            f"{str(row['NAMELN3'] or ''):<40.40}"
            f"{str(row['NAMELN4'] or ''):<40.40}"
            f"{str(row['NAMELN5'] or ''):<40.40}"
            f"{row['BRANCH']:07d}"
            f"{row['ACCTNO']:011d}"
            f"{row['MASK_IDS']:<24.24}"
        )
        f.write(line + '\n')

# Write EMCPBBS summary file
with open(EMCPBBS_PATH, 'w') as f:
    line = (
        f"LNRIHLCP"
        f"{' ' * 22}"
        f"{REPTDT}"
        f"{rowcnt:016d}"
    )
    f.write(line + '\n')

print(f"  EMCPBB records written: {rowcnt}")

# Process MAILPBB
if len(mailpbb) > 0:
    print("\n  Processing MAILPBB email statements...")

    mailpbb = mailpbb.with_columns([
        pl.lit(pl.arange(1, len(mailpbb) + 1)).alias('ROWCNT')
    ])

    mailpbb = mailpbb.with_columns([
        (pl.lit("PBB_EMAIL_STMT_RIL_C") +
         pl.col('ROWCNT').cast(pl.Utf8).str.zfill(10) +
         pl.lit("_") +
         pl.lit(INDXDT)).alias('VAR_ID'),
        (pl.lit(MTHNAM) + pl.lit(REPTYR)).alias('STATE_DTE')
    ])

    # Write EMLPBB file
    print("  Writing EMLPBB file...")
    with open(EMLPBB_PATH, 'w') as f:
        rowcnt = 0
        for row in mailpbb.iter_rows(named=True):
            rowcnt += 1
            line = (
                f"B"
                f"{REPTDT}"
                f"{str(row['NAMELN1'] or ''):<40.40}"
                f"{str(row['NAMELN2'] or ''):<40.40}"
                f"{str(row['NAMELN3'] or ''):<40.40}"
                f"{str(row['NAMELN4'] or ''):<40.40}"
                f"{str(row['NAMELN5'] or ''):<40.40}"
                f"{row['BRANCH']:07d}"
                f"{row['ACCTNO']:011d}"
                f"{' '}"
                f"{row['VAR_ID']:<40.40}"
                f"{' '}"
                f"{row['MASK_IDS']:<24.24}"
            )
            f.write(line + '\n')

    # Write EMLPBBS summary file
    with open(EMLPBBS_PATH, 'w') as f:
        line = (
            f"LNRIHLCE"
            f"{' ' * 22}"
            f"{REPTDT}"
            f"{rowcnt:016d}"
        )
        f.write(line + '\n')

    print(f"  EMLPBB records written: {rowcnt}")

    # Write EMXPBB index file
    print("  Writing EMXPBB index file...")
    with open(EMXPBB_PATH, 'w') as f:
        for row in mailpbb.iter_rows(named=True):
            line = (
                f"{row['VAR_ID']:<40.40}"
                f"{str(row['EMAILADD'] or ''):<60.60}"
                f"{row['STATE_DTE']:<7.7}"
                f"{str(row['NAMELN1'] or ''):<40.40}"
                f"{str(row['NEWIC'] or ''):<17.17}"
            )
            f.write(line + '\n')

    print(f"  EMXPBB records written: {len(mailpbb)}")


# ============================================================================
# PIB PROCESSING
# ============================================================================

print("\nStep 3: Processing PIB data...")

# Load LNI.LNNAME and merge with RLSLIST
pibname = duckdb.query(f"""
    SELECT a.*
    FROM read_parquet('{LNI_LNNAME_PATH}') a
    INNER JOIN rlslist b ON a.ACCTNO = b.ACCTNO
    ORDER BY a.ACCTNO
""").pl()

print(f"  Records in PIBNAME after merge: {len(pibname)}")

# Add encrypted ID and split into PIBNAME and MAILPIB
pibname = pibname.with_columns([
    pl.col('NEWIC').str.strip_chars().alias('ID')
])

pibname = pibname.with_columns([
    pl.col('ID').map_elements(encrypt_id, return_dtype=pl.Utf8).alias('MASK_IDS')
])

# Split based on MAILCODE and EMAILADD
mailpib = pibname.filter(
    (pl.col('MAILCODE').is_in([' ', '13', '14'])) &
    (pl.col('EMAILADD').str.strip_chars() != '')
)

pibname = pibname.filter(
    ~((pl.col('MAILCODE').is_in([' ', '13', '14'])) &
      (pl.col('EMAILADD').str.strip_chars() != ''))
)

print(f"  Records in PIBNAME (non-email): {len(pibname)}")
print(f"  Records in MAILPIB (email): {len(mailpib)}")

# Write EMCPIB file
print("\n  Writing EMCPIB file...")
with open(EMCPIB_PATH, 'w') as f:
    rowcnt = 0
    for row in pibname.iter_rows(named=True):
        rowcnt += 1
        line = (
            f"B"
            f"{REPTDT}"
            f"{str(row['NAMELN1'] or ''):<40.40}"
            f"{str(row['NAMELN2'] or ''):<40.40}"
            f"{str(row['NAMELN3'] or ''):<40.40}"
            f"{str(row['NAMELN4'] or ''):<40.40}"
            f"{str(row['NAMELN5'] or ''):<40.40}"
            f"{row['BRANCH']:07d}"
            f"{row['ACCTNO']:011d}"
            f"{row['MASK_IDS']:<24.24}"
        )
        f.write(line + '\n')

# Write EMCPIBS summary file
with open(EMCPIBS_PATH, 'w') as f:
    line = (
        f"LNRIHLIP"
        f"{' ' * 22}"
        f"{REPTDT}"
        f"{rowcnt:016d}"
    )
    f.write(line + '\n')

print(f"  EMCPIB records written: {rowcnt}")

# Process MAILPIB
if len(mailpib) > 0:
    print("\n  Processing MAILPIB email statements...")

    mailpib = mailpib.with_columns([
        pl.lit(pl.arange(1, len(mailpib) + 1)).alias('ROWCNT')
    ])

    mailpib = mailpib.with_columns([
        (pl.lit("PIB_EMAIL_STMT_RIL_C") +
         pl.col('ROWCNT').cast(pl.Utf8).str.zfill(10) +
         pl.lit("_") +
         pl.lit(INDXDT)).alias('VAR_ID'),
        (pl.lit(MTHNAM) + pl.lit(REPTYR)).alias('STATE_DTE')
    ])

    # Write EMLPIB file
    print("  Writing EMLPIB file...")
    with open(EMLPIB_PATH, 'w') as f:
        rowcnt = 0
        for row in mailpib.iter_rows(named=True):
            rowcnt += 1
            line = (
                f"B"
                f"{REPTDT}"
                f"{str(row['NAMELN1'] or ''):<40.40}"
                f"{str(row['NAMELN2'] or ''):<40.40}"
                f"{str(row['NAMELN3'] or ''):<40.40}"
                f"{str(row['NAMELN4'] or ''):<40.40}"
                f"{str(row['NAMELN5'] or ''):<40.40}"
                f"{row['BRANCH']:07d}"
                f"{row['ACCTNO']:011d}"
                f"{' '}"
                f"{row['VAR_ID']:<40.40}"
                f"{' '}"
                f"{row['MASK_IDS']:<24.24}"
            )
            f.write(line + '\n')

    # Write EMLPIBS summary file
    with open(EMLPIBS_PATH, 'w') as f:
        line = (
            f"LNRIHLIPE"
            f"{' ' * 21}"
            f"{REPTDT}"
            f"{rowcnt:016d}"
        )
        f.write(line + '\n')

    print(f"  EMLPIB records written: {rowcnt}")

    # Write EMXPIB index file
    print("  Writing EMXPIB index file...")
    with open(EMXPIB_PATH, 'w') as f:
        for row in mailpib.iter_rows(named=True):
            line = (
                f"{row['VAR_ID']:<40.40}"
                f"{str(row['EMAILADD'] or ''):<60.60}"
                f"{row['STATE_DTE']:<7.7}"
                f"{str(row['NAMELN1'] or ''):<40.40}"
                f"{str(row['NEWIC'] or ''):<17.17}"
            )
            f.write(line + '\n')

    print(f"  EMXPIB records written: {len(mailpib)}")


# ============================================================================
# REPORT GENERATION
# ============================================================================

print("\nStep 4: Generating report...")

# Combine PBBNAME and PIBNAME, add NOEMC flag, and sort
rlslist_report = pl.concat([pbbname, pibname])
rlslist_report = rlslist_report.with_columns([
    pl.lit(1).alias('NOEMC')
])
rlslist_report = rlslist_report.sort(['BRANCH', 'ACCTNO', 'NOTENO'])

print(f"  Total records for report: {len(rlslist_report)}")


def format_branch_code(branch_val):
    """Format branch as 3-digit string"""
    if branch_val is None:
        return '   '
    return f"{int(branch_val):3d}"


def write_report_with_asa():
    """Generate report with ASA carriage control characters"""

    PAGE_LENGTH = 60
    line_count = 0
    page_number = 1

    with open(REPORT_PATH, 'w') as f:

        def write_line(asa_char, content):
            """Write a line with ASA carriage control character"""
            nonlocal line_count
            f.write(f"{asa_char}{content}\n")
            line_count += 1

        def new_page():
            """Start a new page"""
            nonlocal line_count, page_number
            write_line('1', ' ' * 130)  # Form feed
            write_line(' ', f"{'REPORT ID : EIQPROM2':^130}")
            write_line(' ', f"AUTOMAILING LISTING FOR REINSTATEMENT OF LOAN AS AT {RDATE}".center(130))
            write_line(' ', ' ' * 130)
            write_line(' ',
                       f"{'BRANCH':<7} {'BRANCH':<6} {'A/C NO':>10} {'NOTE':>5} {'PRODUCT':<8} {'NAME OF BORROWER/CUSTOMER':<40} {'MAIL CODE':>2}")
            write_line(' ', f"{'CODE':<7} {'':<6} {'':<10} {'NO':>5} {'CODE':<8} {'':<40} {'':<2}")
            write_line(' ', '-' * 130)
            line_count = 7
            page_number += 1

        # Write first page header
        new_page()

        current_branch = None
        branch_count = 0

        for row in rlslist_report.iter_rows(named=True):

            # Check for branch break
            if current_branch is not None and row['BRANCH'] != current_branch:
                # Write branch summary
                write_line(' ', ' ' * 130)
                write_line(' ', '   ' + '-' * 123)
                write_line(' ', f"   NO OF BORROWER/CUSTOMER :{branch_count:8,}")
                write_line(' ', ' ' * 130)
                line_count += 4
                branch_count = 0

            # Check if we need a new page
            if line_count >= PAGE_LENGTH - 5:
                new_page()

            current_branch = row['BRANCH']
            branch_count += 1

            # Format and write detail line
            branch_code = f"{row['BRANCH']:3d}" if row['BRANCH'] is not None else '   '
            brch = format_branch_code(row['BRANCH'])
            acctno = f"{row['ACCTNO']:10d}" if row['ACCTNO'] is not None else ' ' * 10
            noteno = f"{row['NOTENO']:5d}" if row['NOTENO'] is not None else ' ' * 5
            product = f"{str(row['PRODUCT'] or ''):<8.8}"
            nameln1 = f"{str(row['NAMELN1'] or ''):<40.40}"
            mailcode = f"{str(row['MAILCODE'] or ''):>2.2}"

            line = f" {branch_code} {brch} {acctno} {noteno} {product} {nameln1} {mailcode}"
            write_line(' ', line)

        # Write final branch summary
        if branch_count > 0:
            write_line(' ', ' ' * 130)
            write_line(' ', '   ' + '-' * 123)
            write_line(' ', f"   NO OF BORROWER/CUSTOMER :{branch_count:8,}")
            write_line(' ', ' ' * 130)

    print(f"  Report written to: {REPORT_PATH}")
    print(f"  Total pages: {page_number - 1}")


write_report_with_asa()

print("\n" + "=" * 70)
print("EIQPROM2 processing completed successfully!")
print("=" * 70)
