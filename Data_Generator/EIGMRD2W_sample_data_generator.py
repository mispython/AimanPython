"""
Comprehensive Walker Data Generator
Generates both ALW (R1) and ALM (R2) Walker format test data
"""

from pathlib import Path
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
random.seed(42)

# ============================================================================
# CONFIGURATION
# ============================================================================

OUTPUT_DIR = Path("./data/input/walker")
ALW_OUTPUT_FILE = OUTPUT_DIR / "WALALW.txt"
ALM_OUTPUT_FILE = OUTPUT_DIR / "WALALM.txt"

# Ensure directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REFERENCE DATA
# ============================================================================

# Account numbers (realistic bank account format)
ACCOUNT_PREFIXES = ['100', '200', '300', '400', '500', '600', '700', '800']
BRANCH_CODES = ['001', '002', '003', '010', '020', '050', '100']

# Set IDs for R1R115/R2R115 and R1R913/R2R913
SET_IDS = [
    'SET001',
    'SET002',
    'SET003',
    'SETDEF001',
    'SETDEF002',
    'SETPRD001',
    'SETPRD002'
]

# User codes
USER_CODES = ['USR1', 'USR2', 'USR3', 'ADM1', 'SYS1', 'OPS1']


# ============================================================================
# DATA GENERATORS
# ============================================================================

def generate_account_number():
    """Generate realistic account number (22 chars max)"""
    prefix = random.choice(ACCOUNT_PREFIXES)
    branch = random.choice(BRANCH_CODES)
    number = random.randint(1000000, 9999999)
    suffix = random.randint(1, 99)

    return f"{prefix}{branch}{number:07d}{suffix:02d}"


def generate_amount():
    """Generate realistic banking amount"""
    if random.random() < 0.9:
        # Positive amounts
        return abs(random.lognormvariate(10, 2))
    else:
        # Negative amounts
        return -abs(random.lognormvariate(9, 1.5))


def format_amount(amount, width=21):
    """Format amount with commas (COMMA21.2 format)"""
    formatted = f"{amount:,.2f}"
    return formatted.rjust(width)


def get_report_date():
    """Get last day of previous month"""
    today = datetime.now()
    first_of_month = datetime(today.year, today.month, 1)
    reptdate = first_of_month - timedelta(days=1)
    return reptdate


def generate_date_within_range(base_date, days_back=30):
    """Generate a random date within range"""
    days_offset = random.randint(0, days_back)
    return base_date - timedelta(days=days_offset)


# ============================================================================
# ALW WALKER FORMAT WRITERS (R1 series)
# ============================================================================

def write_section_header(f, setid):
    """Write section header (record type '1')"""
    # Format: @2 ID='1' @3 SETID (8 chars)
    line = f" 1{setid:<8}\n"
    f.write(line)


def write_r1gl4000_record(f, acct_no, ackind, glamt):
    """Write R1GL4000 data record"""
    # Format: @2 ID='+' @3 ACCT_NO (22) @29 ACKIND (1) @31 GLAMT (COMMA21.2)
    amt_str = format_amount(glamt, 21)
    line = f" +{acct_no:<22}     {ackind} {amt_str}\n"
    f.write(line)


def write_r1gl4100_record(f, acct_no, effdate, jeamt, ackind):
    """Write R1GL4100 data record"""
    # Format: @2 ID='+' @3 ACCT_NO (22) @30 EFFDATE (DDMMYY8) @47 JEAMT (COMMA21.2) @75 ACKIND (1)
    date_str = effdate.strftime('%d%m%Y')
    amt_str = format_amount(jeamt, 21)
    line = f" +{acct_no:<22}     {date_str}         {amt_str}       {ackind}\n"
    f.write(line)


def write_r1key913_record(f, acct_no):
    """Write R1KEY913 data record"""
    # Format: @2 ID='+' @3 ACCT_NO (22)
    line = f" +{acct_no:<22}\n"
    f.write(line)


def write_r1r115_record(f, set_id, usercd):
    """Write R1R115 data record"""
    # Format: @2 ID='+' @3 SET_ID (12) @17 USERCD (4)
    line = f" +{set_id:<12}  {usercd:<4}\n"
    f.write(line)


def write_r1r913_record(f, acct_no, set_id):
    """Write R1R913 data record"""
    # Format: @2 ID='+' @3 ACCT_NO (22) @28 SET_ID (12)
    line = f" +{acct_no:<22}   {set_id:<12}\n"
    f.write(line)


# ============================================================================
# ALM WALKER FORMAT WRITERS (R2 series)
# ============================================================================

def write_r2gl4000_record(f, acct_no, ackind, glamt):
    """Write R2GL4000 data record"""
    # Format: @2 ID='+' @3 ACCT_NO (22) @29 ACKIND (1) @30 GLAMT (COMMA21.2)
    # NOTE: Different from R1GL4000 - GLAMT starts at position 30, not 31
    amt_str = format_amount(glamt, 21)
    line = f" +{acct_no:<22}     {ackind}{amt_str}\n"
    f.write(line)


def write_r2key913_record(f, acct_no):
    """Write R2KEY913 data record"""
    # Format: @2 ID='+' @3 ACCT_NO (22)
    line = f" +{acct_no:<22}\n"
    f.write(line)


def write_r2r115_record(f, set_id, usercd):
    """Write R2R115 data record"""
    # Format: @2 ID='+' @3 SET_ID (12) @17 USERCD (4)
    line = f" +{set_id:<12}  {usercd:<4}\n"
    f.write(line)


def write_r2r913_record(f, acct_no, set_id):
    """Write R2R913 data record"""
    # Format: @2 ID='+' @3 ACCT_NO (22) @28 SET_ID (12)
    line = f" +{acct_no:<22}   {set_id:<12}\n"
    f.write(line)


# ============================================================================
# ALW FILE GENERATION (R1 series)
# ============================================================================

def generate_alw_file(report_date, num_gl4000=50, num_gl4100=40,
                      num_key913=30, num_r115=10, num_r913=25):
    """Generate Walker ALW file (R1 series)"""

    print("\n" + "=" * 70)
    print("Generating ALW Walker File (R1 series)")
    print("=" * 70)
    print(f"Report Date: {report_date.strftime('%d/%m/%Y')}")
    print(f"Output File: {ALW_OUTPUT_FILE}")

    with open(ALW_OUTPUT_FILE, 'w') as f:
        # Write file header with date
        date_header = f" {report_date.strftime('%d%m%Y')}\n"
        f.write(date_header)

        # Generate R1GL4000 section
        print(f"\nGenerating R1GL4000 section ({num_gl4000} records)...")
        write_section_header(f, 'R1GL4000')

        for i in range(num_gl4000):
            acct_no = generate_account_number()
            ackind = random.choice(['D', 'C', ' '])
            glamt = generate_amount()
            write_r1gl4000_record(f, acct_no, ackind, glamt)

        # Generate R1GL4100 section
        print(f"Generating R1GL4100 section ({num_gl4100} records)...")
        write_section_header(f, 'R1GL4100')

        for i in range(num_gl4100):
            acct_no = generate_account_number()
            effdate = generate_date_within_range(report_date)
            jeamt = generate_amount()
            ackind = random.choice(['D', 'C', ' '])
            write_r1gl4100_record(f, acct_no, effdate, jeamt, ackind)

        # Generate R1KEY913 section
        print(f"Generating R1KEY913 section ({num_key913} records)...")
        write_section_header(f, 'R1KEY913')

        for i in range(num_key913):
            acct_no = generate_account_number()
            write_r1key913_record(f, acct_no)

        # Generate R1R115 section
        print(f"Generating R1R115 section ({num_r115} records)...")
        write_section_header(f, 'R1R115')

        for i in range(num_r115):
            set_id = random.choice(SET_IDS)
            usercd = random.choice(USER_CODES)
            write_r1r115_record(f, set_id, usercd)

        # Generate R1R913 section
        print(f"Generating R1R913 section ({num_r913} records)...")
        write_section_header(f, 'R1R913')

        for i in range(num_r913):
            acct_no = generate_account_number()
            set_id = random.choice(SET_IDS)
            write_r1r913_record(f, acct_no, set_id)

    file_size = ALW_OUTPUT_FILE.stat().st_size
    total_records = num_gl4000 + num_gl4100 + num_key913 + num_r115 + num_r913

    print(f"\n✓ ALW file created: {ALW_OUTPUT_FILE}")
    print(f"  Size: {file_size:,} bytes")
    print(f"  Total records: {total_records}")


# ============================================================================
# ALM FILE GENERATION (R2 series)
# ============================================================================

def generate_alm_file(report_date, num_gl4000=45, num_key913=28,
                      num_r115=8, num_r913=22):
    """Generate Walker ALM file (R2 series)"""

    print("\n" + "=" * 70)
    print("Generating ALM Walker File (R2 series)")
    print("=" * 70)
    print(f"Report Date: {report_date.strftime('%d/%m/%Y')}")
    print(f"Output File: {ALM_OUTPUT_FILE}")

    with open(ALM_OUTPUT_FILE, 'w') as f:
        # Write file header with date
        date_header = f" {report_date.strftime('%d%m%Y')}\n"
        f.write(date_header)

        # Generate R2GL4000 section
        print(f"\nGenerating R2GL4000 section ({num_gl4000} records)...")
        write_section_header(f, 'R2GL4000')

        for i in range(num_gl4000):
            acct_no = generate_account_number()
            ackind = random.choice(['D', 'C', ' '])
            glamt = generate_amount()
            write_r2gl4000_record(f, acct_no, ackind, glamt)

        # Generate R2KEY913 section
        print(f"Generating R2KEY913 section ({num_key913} records)...")
        write_section_header(f, 'R2KEY913')

        for i in range(num_key913):
            acct_no = generate_account_number()
            write_r2key913_record(f, acct_no)

        # Generate R2R115 section
        print(f"Generating R2R115 section ({num_r115} records)...")
        write_section_header(f, 'R2R115')

        for i in range(num_r115):
            set_id = random.choice(SET_IDS)
            usercd = random.choice(USER_CODES)
            write_r2r115_record(f, set_id, usercd)

        # Generate R2R913 section
        print(f"Generating R2R913 section ({num_r913} records)...")
        write_section_header(f, 'R2R913')

        for i in range(num_r913):
            acct_no = generate_account_number()
            set_id = random.choice(SET_IDS)
            write_r2r913_record(f, acct_no, set_id)

        file_size = ALM_OUTPUT_FILE.stat().st_size
        total_records = num_gl4000 + num_key913 + num_r115 + num_r913

        print(f"\n✓ ALM file created: {ALM_OUTPUT_FILE}")
        print(f"  Size: {file_size:,} bytes")
        print(f"  Total records: {total_records}")

        # ============================================================================
        # INSPECTION UTILITY
        # ============================================================================


def inspect_walker_files():
    """Display sample lines from both Walker files"""

    print("=" * 70)
    print("Walker Files Inspector")
    print("=" * 70)

    for name, filepath in [("ALW (R1)", ALW_OUTPUT_FILE), ("ALM (R2)", ALM_OUTPUT_FILE)]:
        print(f"\n{name} File: {filepath}")

        if not filepath.exists():
            print(f"  File not found. Generate it first.")
            continue

        print(f"  Size: {filepath.stat().st_size:,} bytes")
        print("\n  First 20 lines:")
        print("  " + "-" * 66)

        with open(filepath, 'r') as f:
            for i, line in enumerate(f, 1):
                if i <= 20:
                    print(f"  {i:3d}: {repr(line)}")
                else:
                    break

        print("  " + "-" * 66)

        # Count records by type
        type_1_count = 0
        type_plus_count = 0

        with open(filepath, 'r') as f:
            for line in f:
                if len(line) >= 2:
                    if line[1] == '1':
                        type_1_count += 1
                    elif line[1] == '+':
                        type_plus_count += 1

        print(f"\n  Record summary:")
        print(f"    Header records (type '1'): {type_1_count}")
        print(f"    Data records (type '+'):   {type_plus_count}")


# ============================================================================
# MAIN GENERATION
# ============================================================================

def generate_all_walker_files():
    """Generate both ALW and ALM Walker files"""

    print("=" * 70)
    print("COMPREHENSIVE WALKER DATA GENERATOR")
    print("=" * 70)

    report_date = get_report_date()
    print(f"\nReport Date: {report_date.strftime('%d/%m/%Y (%d%m%Y)')}")
    print(f"Output Directory: {OUTPUT_DIR}")

    # Generate ALW file (R1 series)
    generate_alw_file(
        report_date=report_date,
        num_gl4000=50,
        num_gl4100=40,
        num_key913=30,
        num_r115=10,
        num_r913=25
    )

    # Generate ALM file (R2 series)
    generate_alm_file(
        report_date=report_date,
        num_gl4000=45,
        num_key913=28,
        num_r115=8,
        num_r913=22
    )

    print("\n" + "=" * 70)
    print("GENERATION COMPLETE")
    print("=" * 70)
    print(f"\nGenerated files:")
    print(f"  ALW (R1): {ALW_OUTPUT_FILE}")
    print(f"  ALM (R2): {ALM_OUTPUT_FILE}")
    print(f"\nTo inspect files, run:")
    print(f"  python {Path(__file__).name} --inspect")
    print()


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--inspect":
        inspect_walker_files()
    else:
        generate_all_walker_files()
