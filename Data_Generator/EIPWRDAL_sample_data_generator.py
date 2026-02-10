"""
Sample Data Generator for RDAL PBCS Processing
Generates realistic test data in parquet format
"""

import polars as pl
from pathlib import Path
from datetime import datetime
import random

# Set random seed for reproducibility
random.seed(42)

# ============================================================================
# CONFIGURATION
# ============================================================================

OUTPUT_BASE = Path("./data/input")

# Create directory structure
LOAN_DIR = OUTPUT_BASE / "loan"
PBCS_DIR = OUTPUT_BASE / "pbcs"
BNM_DIR = OUTPUT_BASE / "bnm" / "D2025"

LOAN_DIR.mkdir(parents=True, exist_ok=True)
PBCS_DIR.mkdir(parents=True, exist_ok=True)
BNM_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REFERENCE DATA
# ============================================================================

# Item codes for different categories
AL_ITCODES = [
    "4001000000000Y",  # Standard asset
    "4002000000000Y",
    "4003000000000Y",
    "4005000000000Y",
    "4010000000000D",
    "4010000000000I",
    "4010000000000F",
    "4020000000000D",
    "4025000000000Y",
    "4030000000000D",
    "4030000000000I",
    "4040000000000Y",
    "4050000000000D",
    "4060000000000Y",
    "4070000000000Y",
    "4080000000000D",
    "4080000000000I",
    "4090000000000Y",
    "4100000000000D",
    "4110000000000Y",
]

OB_ITCODES = [
    "5001000000000Y",  # Off-balance sheet
    "5002000000000Y",
    "5003000000000D",
    "5003000000000I",
    "5005000000000Y",
    "5010000000000D",
    "5015000000000Y",
    "5020000000000D",
    "5020000000000I",
    "5025000000000Y",
    "5030000000000D",
]

SP_ITCODES = [
    "3070100000000Y",  # Special items (307 prefix)
    "3070200000000D",
    "3070300000000Y",
    "4019000000000Y",  # 40190 prefix
    "4019100000000D",  # 40191 prefix
    "6850100000000Y",  # 685 prefix
    "7850100000000Y",  # 785 prefix
    "SSTS000000000Y",  # SSTS - will be converted to 4017000000000Y
    "8001000000000Y",  # Special processing for 80 prefix
    "8002000000000D",
    "3000000000000Y",  # Position 2 = '0' (SP category)
    "4000000000000Y",
]

# Excluded ranges (should not appear in output)
EXCLUDED_ITCODES = [
    "3022100000000Y",  # 30221-30228 range
    "3022500000000Y",
    "3023100000000Y",  # 30231-30238 range
    "3023500000000Y",
    "3009100000000Y",  # 30091-30098 range
    "3009500000000Y",
    "4015100000000Y",  # 40151-40158 range
    "4015500000000Y",
    "NSSTS00000000Y",  # NSSTS prefix
]

# Special codes
SPECIAL_CODES = [
    "4364008110000Y",  # Should be deleted
    "4966000000000F",  # Should be excluded on days 08/22
    "3400061006120Y",  # Should NOT have absolute value applied
]

# Negative adjustment codes (# suffix)
NEGATIVE_CODES = [
    "4001000000000#",
    "4010000000000#",
    "5001000000000#",
]

# CAG zip codes
CAG_ZIPCODES = [
    2002, 2013, 3039, 3047,
    800003098, 800003114, 800004016, 800004022, 800004029,
    800040050, 800040053, 800050024, 800060024, 800060045,
    800060081, 80060085
]

# Non-CAG zip codes
NON_CAG_ZIPCODES = [
    1000, 1100, 1200, 2000, 3000, 4000, 5000,
    10000, 20000, 30000, 40000, 50000
]

# Loan types
LOAN_TYPES = [
    "HL", "PL", "AL", "BL", "TL", "CL", "ML", "OL",
    "HP", "CC", "OD", "TR", "IL", "SL", "RL"
]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_amount():
    """Generate realistic random amounts"""
    # Most amounts are positive
    if random.random() < 0.9:
        # Log-normal distribution for realistic banking amounts
        return abs(random.lognormvariate(12, 2))
    else:
        # Some negative amounts
        return -abs(random.lognormvariate(10, 1.5))


def generate_loan_balance():
    """Generate realistic loan balances"""
    return abs(random.lognormvariate(11, 1.8))


# ============================================================================
# GENERATE LOAN NOTE DATA
# ============================================================================

def generate_lnnote_data(num_records=1000):
    """Generate sample LNNOTE.parquet file"""

    print("Generating LNNOTE.parquet...")

    data = {
        'PZIPCODE': [],
        'LOANTYPE': [],
        'BALANCE': []
    }

    # Generate CAG records (should appear in output)
    cag_records = num_records // 3
    for _ in range(cag_records):
        data['PZIPCODE'].append(random.choice(CAG_ZIPCODES))
        data['LOANTYPE'].append(random.choice(LOAN_TYPES))
        data['BALANCE'].append(generate_loan_balance())

    # Generate non-CAG records (should not appear in output)
    non_cag_records = num_records - cag_records
    for _ in range(non_cag_records):
        data['PZIPCODE'].append(random.choice(NON_CAG_ZIPCODES))
        data['LOANTYPE'].append(random.choice(LOAN_TYPES))
        data['BALANCE'].append(generate_loan_balance())

    df = pl.DataFrame(data)

    output_path = LOAN_DIR / "LNNOTE.parquet"
    df.write_parquet(output_path)

    print(f"  Created: {output_path}")
    print(f"  Records: {len(df):,}")
    print(f"  CAG records: {cag_records:,}")
    print(f"  Non-CAG records: {non_cag_records:,}")
    print()


# ============================================================================
# GENERATE ALW/CCLW DATA
# ============================================================================

def generate_alw_data(month, week, num_records=500):
    """Generate sample ALW/CCLW data for a specific month and week"""

    # Combine all item codes
    all_itcodes = (AL_ITCODES + OB_ITCODES + SP_ITCODES +
                   EXCLUDED_ITCODES + SPECIAL_CODES + NEGATIVE_CODES)

    data = {
        'ITCODE': [],
        'AMTIND': [],
        'AMOUNT': []
    }

    # Generate records for each item code
    for itcode in all_itcodes:
        # Determine AMTIND from ITCODE (position 14)
        if len(itcode) >= 14:
            last_char = itcode[13]
            if last_char == 'D':
                amtind = 'D'
            elif last_char == 'I':
                amtind = 'I'
            elif last_char == 'F':
                amtind = 'F'
            else:  # Y or #
                # Check position 2
                if itcode[1] == '0':
                    amtind = ' '
                else:
                    amtind = 'D'
        else:
            amtind = 'D'

        data['ITCODE'].append(itcode)
        data['AMTIND'].append(amtind)
        data['AMOUNT'].append(generate_amount())

    # Add some duplicate ITCODES with different amounts (to test aggregation)
    for _ in range(num_records - len(all_itcodes)):
        itcode = random.choice(all_itcodes)

        if len(itcode) >= 14:
            last_char = itcode[13]
            if last_char == 'D':
                amtind = 'D'
            elif last_char == 'I':
                amtind = 'I'
            elif last_char == 'F':
                amtind = 'F'
            else:
                if itcode[1] == '0':
                    amtind = ' '
                else:
                    amtind = 'D'
        else:
            amtind = 'D'

        data['ITCODE'].append(itcode)
        data['AMTIND'].append(amtind)
        data['AMOUNT'].append(generate_amount())

    return pl.DataFrame(data)


def save_alw_files(month, week):
    """Generate and save ALW files for BNM"""

    filename = f"ALW{month:02d}{week}.parquet"
    output_path = BNM_DIR / filename

    print(f"Generating {filename}...")

    df = generate_alw_data(month, week, num_records=500)
    df.write_parquet(output_path)

    print(f"  Created: {output_path}")
    print(f"  Records: {len(df):,}")
    print()


def save_cclw_files(month, week):
    """Generate and save CCLW files for PBCS"""

    filename = f"CCLW{month:02d}{week}.parquet"
    output_path = PBCS_DIR / filename

    print(f"Generating {filename}...")

    df = generate_alw_data(month, week, num_records=300)
    df.write_parquet(output_path)

    print(f"  Created: {output_path}")
    print(f"  Records: {len(df):,}")
    print()


# ============================================================================
# MAIN GENERATION
# ============================================================================

def generate_all_sample_data():
    """Generate all sample data files"""

    print("=" * 70)
    print("RDAL PBCS Sample Data Generator")
    print("=" * 70)
    print()

    # Generate LNNOTE data
    generate_lnnote_data(num_records=1000)

    # Generate for current month (January) all weeks
    current_month = 1  # January

    for week in range(1, 5):
        save_alw_files(current_month, week)
        save_cclw_files(current_month, week)

    # Generate for previous month (December) week 4 only
    prev_month = 12  # December
    save_alw_files(prev_month, 4)
    save_cclw_files(prev_month, 4)

    # Generate for February week 1 (for testing forward month)
    next_month = 2  # February
    save_alw_files(next_month, 1)
    save_cclw_files(next_month, 1)

    print("=" * 70)
    print("Sample Data Generation Complete!")
    print("=" * 70)
    print()
    print("Generated files:")
    print(f"  Loan data: {LOAN_DIR}")
    print(f"  PBCS data: {PBCS_DIR}")
    print(f"  BNM data:  {BNM_DIR}")
    print()
    print("Directory structure:")
    print(f"  {OUTPUT_BASE}/")
    print(f"    loan/")
    print(f"      LNNOTE.parquet")
    print(f"    pbcs/")
    print(f"      CCLW{current_month:02d}1.parquet through CCLW{current_month:02d}4.parquet")
    print(f"      CCLW{prev_month:02d}4.parquet")
    print(f"      CCLW{next_month:02d}1.parquet")
    print(f"    bnm/")
    print(f"      D2025/")
    print(f"        ALW{current_month:02d}1.parquet through ALW{current_month:02d}4.parquet")
    print(f"        ALW{prev_month:02d}4.parquet")
    print(f"        ALW{next_month:02d}1.parquet")
    print()


# ============================================================================
# DATA INSPECTION UTILITY
# ============================================================================

def inspect_generated_data():
    """Inspect the generated data files"""

    print("=" * 70)
    print("Inspecting Generated Data")
    print("=" * 70)
    print()

    # Inspect LNNOTE
    lnnote_path = LOAN_DIR / "LNNOTE.parquet"
    if lnnote_path.exists():
        df = pl.read_parquet(lnnote_path)
        print(f"LNNOTE.parquet:")
        print(f"  Total records: {len(df):,}")
        print(f"  CAG zip codes: {df.filter(pl.col('PZIPCODE').is_in(CAG_ZIPCODES)).height:,}")
        print(f"  Sample data:")
        print(df.head(5))
        print()

    # Inspect ALW01 1
    alw_path = BNM_DIR / "ALW011.parquet"
    if alw_path.exists():
        df = pl.read_parquet(alw_path)
        print(f"ALW011.parquet:")
        print(f"  Total records: {len(df):,}")
        print(f"  Unique ITCODEs: {df.select('ITCODE').n_unique():,}")
        print(f"  AMTIND distribution:")
        print(df.group_by('AMTIND').agg(pl.count()).sort('AMTIND'))
        print(f"  Sample data:")
        print(df.head(5))
        print()

    # Inspect CCLW011
    cclw_path = PBCS_DIR / "CCLW011.parquet"
    if cclw_path.exists():
        df = pl.read_parquet(cclw_path)
        print(f"CCLW011.parquet:")
        print(f"  Total records: {len(df):,}")
        print(f"  Unique ITCODEs: {df.select('ITCODE').n_unique():,}")
        print(f"  Amount statistics:")
        print(df.select('AMOUNT').describe())
        print()


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--inspect":
        inspect_generated_data()
    else:
        generate_all_sample_data()

        print("To inspect the generated data, run:")
        print("  python generate_sample_data.py --inspect")
        print()
