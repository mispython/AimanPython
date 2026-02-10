"""
For EIGWRD1W / EIGMRD2W / EIGMRGCW
Comprehensive Walker Data Generator
Generates ALW (R1), ALM (R2), and GAY (RG) Walker format test data
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
GAY_OUTPUT_FILE = OUTPUT_DIR / "WALGAY.txt"

# Ensure directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REFERENCE DATA
# ============================================================================

# Account numbers
ACCOUNT_PREFIXES = ['100', '200', '300', '400', '500', '600', '700', '800']
BRANCH_CODES = ['001', '002', '003', '010', '020', '050', '100']

# Set IDs
SET_IDS = ['SET001', 'SET002', 'SET003', 'SETDEF001', 'SETDEF002', 'SETPRD001', 'SETPRD002']

# User codes
USER_CODES = ['USR1', 'USR2', 'USR3', 'ADM1', 'SYS1', 'OPS1']

# Segment values and descriptions for RGR901
SEGMENT_DATA = [
    ('CORP', 'Corporate Banking Division'),
    ('RETAIL', 'Retail Banking Division'),
    ('INV', 'Investment Banking Division'),
    ('TREAS', 'Treasury Division'),
    ('RISK', 'Risk Management Division'),
]


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
        return abs(random.lognormvariate(10, 2))
    else:
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
# COMMON WRITERS
# ============================================================================

def write_section_header(f, setid):
    """Write section header (record type '1')"""
    line = f" 1{setid:<8}\n"
    f.write(line)


def write_gl4000_record(f, acct_no, ackind, glamt):
    """Write GL4000 record (R1/RG format - position 31)"""
    amt_str = format_amount(glamt, 21)
    line = f" +{acct_no:<22}     {ackind} {amt_str}\n"
    f.write(line)


def write_gl4100_record(f, acct_no, effdate, jeamt, ackind):
    """Write GL4100 record"""
    date_str = effdate.strftime('%d%m%Y')
    amt_str = format_amount(jeamt, 21)
    line = f" +{acct_no:<22}     {date_str}          {amt_str}       {ackind}\n"
    f.write(line)


def write_key913_record(f, acct_no):
    """Write KEY913 record"""
    line = f" +{acct_no:<22}\n"
    f.write(line)


def write_r115_record(f, set_id, usercd):
    """Write R115 record"""
    line = f" +{set_id:<12}  {usercd:<4}\n"
    f.write(line)


def write_r913_record(f, acct_no, set_id):
    """Write R913 record"""
    line = f" +{acct_no:<22}   {set_id:<12}\n"
    f.write(line)


def write_r2gl4000_record(f, acct_no, ackind, glamt):
    """Write R2GL4000 record (position 30 - ALM specific)"""
    amt_str = format_amount(glamt, 21)
    line = f" +{acct_no:<22}     {ackind}{amt_str}\n"
    f.write(line)


def write_r901_record(f, seg_val, seg_desc):
    """Write R901/RGR901 record (segment information)"""
    line = f" +{seg_val:<5}   {seg_desc:<40}\n"
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
        f.write(f" {report_date.strftime('%d%m%Y')}\n")

        # R1GL4000 section
        print(f"\n  Generating R1GL4000 ({num_gl4000} records)...")
        write_section_header(f, 'R1GL4000')
        for _ in range(num_gl4000):
            write_gl4000_record(f, generate_account_number(),
                                random.choice(['D', 'C', ' ']), generate_amount())

        # R1GL4100 section
        print(f"  Generating R1GL4100 ({num_gl4100} records)...")
        write_section_header(f, 'R1GL4100')
        for _ in range(num_gl4100):
            write_gl4100_record(f, generate_account_number(),
                                generate_date_within_range(report_date),
                                generate_amount(), random.choice(['D', 'C', ' ']))

        # R1KEY913 section
        print(f"  Generating R1KEY913 ({num_key913} records)...")
        write_section_header(f, 'R1KEY913')
        for _ in range(num_key913):
            write_key913_record(f, generate_account_number())

        # R1R115 section
        print(f"  Generating R1R115 ({num_r115} records)...")
        write_section_header(f, 'R1R115')
        for _ in range(num_r115):
            write_r115_record(f, random.choice(SET_IDS), random.choice(USER_CODES))

        # R1R913 section
        print(f"  Generating R1R913 ({num_r913} records)...")
        write_section_header(f, 'R1R913')
        for _ in range(num_r913):
            write_r913_record(f, generate_account_number(), random.choice(SET_IDS))

    file_size = ALW_OUTPUT_FILE.stat().st_size
    total = num_gl4000 + num_gl4100 + num_key913 + num_r115 + num_r913
    print(f"\n  ✓ Created: {file_size:,} bytes, {total} records")


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
        f.write(f" {report_date.strftime('%d%m%Y')}\n")

        # R2GL4000 section
        print(f"\n  Generating R2GL4000 ({num_gl4000} records)...")
        write_section_header(f, 'R2GL4000')
        for _ in range(num_gl4000):
            write_r2gl4000_record(f, generate_account_number(),
                                  random.choice(['D', 'C', ' ']), generate_amount())

        # R2KEY913 section
        print(f"  Generating R2KEY913 ({num_key913} records)...")
        write_section_header(f, 'R2KEY913')
        for _ in range(num_key913):
            write_key913_record(f, generate_account_number())

        # R2R115 section
        print(f"  Generating R2R115 ({num_r115} records)...")
        write_section_header(f, 'R2R115')
        for _ in range(num_r115):
            write_r115_record(f, random.choice(SET_IDS), random.choice(USER_CODES))

        # R2R913 section
        print(f"  Generating R2R913 ({num_r913} records)...")
        write_section_header(f, 'R2R913')
        for _ in range(num_r913):
            write_r913_record(f, generate_account_number(), random.choice(SET_IDS))

    file_size = ALM_OUTPUT_FILE.stat().st_size
    total = num_gl4000 + num_key913 + num_r115 + num_r913
    print(f"\n  ✓ Created: {file_size:,} bytes, {total} records")


# ============================================================================
# GAY FILE GENERATION (RG series)
# ============================================================================

def generate_gay_file(report_date, num_gl4000=55, num_gl4100=45,
                      num_key913=35, num_r115=12, num_r901=5, num_r913=28):
    """Generate Walker GAY file (RG series - Global Assets and Capital)"""

    print("\n" + "=" * 70)
    print("Generating GAY Walker File (RG series)")
    print("=" * 70)
    print(f"Report Date: {report_date.strftime('%d/%m/%Y')}")
    print(f"Output File: {GAY_OUTPUT_FILE}")

    with open(GAY_OUTPUT_FILE, 'w') as f:
        # Write file header with date
        f.write(f" {report_date.strftime('%d%m%Y')}\n")

        # RGGL4000 section
        print(f"\n  Generating RGGL4000 ({num_gl4000} records)...")
        write_section_header(f, 'RGGL4000')
        for _ in range(num_gl4000):
            write_gl4000_record(f, generate_account_number(),
                                random.choice(['D', 'C', ' ']), generate_amount())

        # RGGL4100 section
        print(f"  Generating RGGL4100 ({num_gl4100} records)...")
        write_section_header(f, 'RGGL4100')
        for _ in range(num_gl4100):
            write_gl4100_record(f, generate_account_number(),
                                generate_date_within_range(report_date),
                                generate_amount(), random.choice(['D', 'C', ' ']))

        # RGKEY913 section
        print(f"  Generating RGKEY913 ({num_key913} records)...")
        write_section_header(f, 'RGKEY913')
        for _ in range(num_key913):
            write_key913_record(f, generate_account_number())

        # RGR115 section
        print(f"  Generating RGR115 ({num_r115} records)...")
        write_section_header(f, 'RGR115')
        for _ in range(num_r115):
            write_r115_record(f, random.choice(SET_IDS), random.choice(USER_CODES))

        # RGR901 section (unique to GAY)
        print(f"  Generating RGR901 ({num_r901} records)...")
        write_section_header(f, 'RGR901')
        for seg_val, seg_desc in SEGMENT_DATA:
            write_r901_record(f, seg_val, seg_desc)

        # RGR913 section
        print(f"  Generating RGR913 ({num_r913} records)...")
        write_section_header(f, 'RGR913')
        for _ in range(num_r913):
            write_r913_record(f, generate_account_number(), random.choice(SET_IDS))

    file_size = GAY_OUTPUT_FILE.stat().st_size
    total = num_gl4000 + num_gl4100 + num_key913 + num_r115 + num_r901 + num_r913
    print(f"\n  ✓ Created: {file_size:,} bytes, {total} records")


# ============================================================================
# INSPECTION UTILITY
# ============================================================================

def inspect_walker_files():
    """Display sample lines from all Walker files"""

    print("=" * 70)
    print("Walker Files Inspector")
    print("=" * 70)

    for name, filepath in [("ALW (R1)", ALW_OUTPUT_FILE),
                           ("ALM (R2)", ALM_OUTPUT_FILE),
                           ("GAY (RG)", GAY_OUTPUT_FILE)]:
        print(f"\n{name} File: {filepath}")

        if not filepath.exists():
            print(f"  File not found.")
            continue

        print(f"  Size: {filepath.stat().st_size:,} bytes")
        print("\n  First 15 lines:")
        print("  " + "-" * 66)

        with open(filepath, 'r') as f:
            for i, line in enumerate(f, 1):
                if i <= 15:
                    print(f"  {i:3d}: {repr(line)}")
                else:
                    break

        print("  " + "-" * 66)

        # Count records
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
        print(f"    Sections (type '1'): {type_1_count}")
        print(f"    Data (type '+'):     {type_plus_count}")


# ============================================================================
# MAIN GENERATION
# ============================================================================

def generate_all_walker_files():
    """Generate all three Walker file types"""

    print("=" * 70)
    print("COMPREHENSIVE WALKER DATA GENERATOR")
    print("=" * 70)
    print(f"\nGenerates: ALW (R1), ALM (R2), GAY (RG)")

    report_date = get_report_date()
    print(f"Report Date: {report_date.strftime('%d/%m/%Y (%d%m%Y)')}")
    print(f"Output Directory: {OUTPUT_DIR}")

    # Generate ALW file
    generate_alw_file(report_date, num_gl4000=50, num_gl4100=40,
                      num_key913=30, num_r115=10, num_r913=25)

    # Generate ALM file
    generate_alm_file(report_date, num_gl4000=45, num_key913=28,
                      num_r115=8, num_r913=22)

    # Generate GAY file
    generate_gay_file(report_date, num_gl4000=55, num_gl4100=45,
                      num_key913=35, num_r115=12, num_r901=5, num_r913=28)

    print("\n" + "=" * 70)
    print("GENERATION COMPLETE")
    print("=" * 70)
    print(f"\nGenerated files:")
    print(f"  ALW (R1 - Allowance):          {ALW_OUTPUT_FILE}")
    print(f"  ALM (R2 - Asset/Liability):    {ALM_OUTPUT_FILE}")
    print(f"  GAY (RG - Global A&C):         {GAY_OUTPUT_FILE}")
    print(f"\nTo inspect files, run:")
    print(f"  python {Path(__file__).name} --inspect")
    print(f"\nTo process files:")
    print(f"  python walker_converter.py      # For ALW")
    print(f"  python alm_walker_converter.py  # For ALM")
    print(f"  python gay_walker_converter.py  # For GAY")
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
