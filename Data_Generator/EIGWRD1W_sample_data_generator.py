"""
Sample Walker Data Generator
Generates realistic Walker ALW format test data
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
OUTPUT_FILE = OUTPUT_DIR / "WALALW.txt"

# Ensure directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REFERENCE DATA
# ============================================================================

# Account numbers (realistic bank account format)
ACCOUNT_PREFIXES = ['100', '200', '300', '400', '500', '600', '700', '800']
BRANCH_CODES = ['001', '002', '003', '010', '020', '050', '100']

# Set IDs for R1R115 and R1R913
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
    """Get appropriate reporting date based on today"""
    today = datetime.now()
    dd = today.day
    mm = today.month
    yy = today.year

    if dd < 8:
        first_of_month = datetime(yy, mm, 1)
        reptdate = first_of_month - timedelta(days=1)
    elif dd < 15:
        reptdate = datetime(yy, mm, 8)
    elif dd < 22:
        reptdate = datetime(yy, mm, 15)
    else:
        reptdate = datetime(yy, mm, 22)

    return reptdate


def generate_date_within_range(base_date, days_back=30):
    """Generate a random date within range"""
    days_offset = random.randint(0, days_back)
    return base_date - timedelta(days=days_offset)


# ============================================================================
# WALKER FORMAT WRITERS
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
    # Positions: 1=space, 2=+, 3-24=ACCT_NO, 25-29=spaces, 30-37=EFFDATE,
    #            38-46=spaces, 47-67=JEAMT, 68-74=spaces, 75=ACKIND
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
    # Positions: 1=space, 2=+, 3-14=SET_ID, 15-16=spaces, 17-20=USERCD
    line = f" +{set_id:<12}  {usercd:<4}\n"
    f.write(line)


def write_r1r913_record(f, acct_no, set_id):
    """Write R1R913 data record"""
    # Format: @2 ID='+' @3 ACCT_NO (22) @28 SET_ID (12)
    # Positions: 1=space, 2=+, 3-24=ACCT_NO, 25-27=spaces, 28-39=SET_ID
    line = f" +{acct_no:<22}   {set_id:<12}\n"
    f.write(line)


# ============================================================================
# MAIN GENERATION
# ============================================================================

def generate_walker_file(num_gl4000=50, num_gl4100=40, num_key913=30,
                         num_r115=10, num_r913=25):
    """Generate complete Walker ALW file"""

    print("=" * 70)
    print("Walker Data Generator")
    print("=" * 70)

    report_date = get_report_date()
    print(f"\nReport Date: {report_date.strftime('%d/%m/%Y')}")
    print(f"Output File: {OUTPUT_FILE}")

    with open(OUTPUT_FILE, 'w') as f:
        # Write file header with date
        # First line: @2 WALKDATE (8 chars) = DDMMYYYY
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

    # Calculate file size
    file_size = OUTPUT_FILE.stat().st_size

    print("\n" + "=" * 70)
    print("Generation Complete!")
    print("=" * 70)
    print(f"File created: {OUTPUT_FILE}")
    print(f"File size:    {file_size:,} bytes")
    print(f"\nRecords generated:")
    print(f"  R1GL4000: {num_gl4000}")
    print(f"  R1GL4100: {num_gl4100}")
    print(f"  R1KEY913: {num_key913}")
    print(f"  R1R115:   {num_r115}")
    print(f"  R1R913:   {num_r913}")
    print(f"  Total:    {num_gl4000 + num_gl4100 + num_key913 + num_r115 + num_r913}")
    print()


# ============================================================================
# INSPECTION UTILITY
# ============================================================================

def inspect_walker_file():
    """Display sample lines from Walker file"""

    print("=" * 70)
    print("Walker File Inspector")
    print("=" * 70)

    if not OUTPUT_FILE.exists():
        print(f"\nFile not found: {OUTPUT_FILE}")
        print("Generate the file first with: python generate_walker_data.py")
        return

    print(f"\nFile: {OUTPUT_FILE}")
    print(f"Size: {OUTPUT_FILE.stat().st_size:,} bytes")
    print("\nFirst 30 lines:")
    print("-" * 70)

    with open(OUTPUT_FILE, 'r') as f:
        for i, line in enumerate(f, 1):
            if i <= 30:
                # Show line with position markers
                print(f"{i:3d}: {repr(line)}")
            else:
                break

    print("-" * 70)

    # Count records by type
    print("\nRecord type summary:")

    type_1_count = 0
    type_plus_count = 0

    with open(OUTPUT_FILE, 'r') as f:
        for line in f:
            if len(line) >= 2:
                if line[1] == '1':
                    type_1_count += 1
                elif line[1] == '+':
                    type_plus_count += 1

    print(f"  Header records (type '1'): {type_1_count}")
    print(f"  Data records (type '+'):   {type_plus_count}")
    print()


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--inspect":
        inspect_walker_file()
    else:
        # Generate with default counts
        generate_walker_file(
            num_gl4000=50,
            num_gl4100=40,
            num_key913=30,
            num_r115=10,
            num_r913=25
        )

        print("To inspect the generated file, run:")
        print("  python generate_walker_data.py --inspect")
        print()
