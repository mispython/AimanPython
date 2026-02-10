"""
Sample Data Generator for EIBMMTH1 Processing
Generates realistic test data for all input sources
"""

import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
random.seed(42)

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_PATH = Path("./data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"

# Create directory structure
FISS_DIR = INPUT_PATH / "fiss"
RGAC_DIR = INPUT_PATH / "rgac"
MNILN_DIR = INPUT_PATH / "mniln"
MNIFD_DIR = INPUT_PATH / "mnifd"
MNITB_DIR = INPUT_PATH / "mnitb"
KAPITI_DIR = INPUT_PATH / "kapiti"

for dir in [FISS_DIR, RGAC_DIR, MNILN_DIR, MNIFD_DIR, MNITB_DIR, KAPITI_DIR, OUTPUT_PATH]:
    dir.mkdir(parents=True, exist_ok=True)


# ============================================================================
# DATE UTILITIES
# ============================================================================

def get_report_date():
    """Get last day of previous month"""
    today = datetime.now()
    first_of_month = datetime(today.year, today.month, 1)
    return first_of_month - timedelta(days=1)


def get_lag_date(months_back: int):
    """Get date N months back"""
    report_date = get_report_date()
    year = report_date.year
    month = report_date.month - months_back

    while month <= 0:
        month += 12
        year -= 1

    # Get last day of that month
    if month == 12:
        next_month_first = datetime(year + 1, 1, 1)
    else:
        next_month_first = datetime(year, month + 1, 1)

    return next_month_first - timedelta(days=1)


# ============================================================================
# REFERENCE DATA
# ============================================================================

# Item codes for different categories
AL_ITCODES = [
    "4001000000000Y", "4002000000000Y", "4003000000000Y", "4005000000000Y",
    "4010000000000D", "4010000000000I", "4010000000000F",
    "4020000000000D", "4025000000000Y", "4030000000000D", "4030000000000I",
    "4040000000000Y", "4050000000000D", "4060000000000Y", "4070000000000Y",
    "4080000000000D", "4080000000000I", "4090000000000Y",
    "4100000000000D", "4110000000000Y", "4120000000000Y",
]

OB_ITCODES = [
    "5001000000000Y", "5002000000000Y", "5003000000000D", "5003000000000I",
    "5005000000000Y", "5010000000000D", "5015000000000Y",
    "5020000000000D", "5020000000000I", "5025000000000Y", "5030000000000D",
]

SP_ITCODES = [
    "3070100000000Y", "3070200000000D", "3070300000000Y",
    "4019000000000Y", "4019100000000D",
    "6850100000000Y", "7850100000000Y",
    "8001000000000Y", "8002000000000D", "8005000000000Y",
    "3000000000000Y", "4000000000000Y",
]

LOAN_TYPES = ["HL", "PL", "AL", "BL", "TL", "CL", "ML", "OL", "HP", "CC", "OD", "TR", "IL", "SL", "RL"]
ACCOUNT_TYPES = ["SA", "CA", "FD", "TD", "OD"]
BRANCH_CODES = [f"B{i:04d}" for i in range(1, 101)]
CUSTOMER_TYPES = ["I", "C", "G"]  # Individual, Corporate, Government


# ============================================================================
# AMOUNT GENERATORS
# ============================================================================

def generate_amount():
    """Generate realistic banking amounts"""
    if random.random() < 0.9:
        return abs(random.lognormvariate(12, 2))
    else:
        return -abs(random.lognormvariate(10, 1.5))


def generate_loan_balance():
    """Generate realistic loan balances"""
    return abs(random.lognormvariate(11, 1.8))


def generate_deposit_balance():
    """Generate realistic deposit balances"""
    return abs(random.lognormvariate(10, 2.0))


# ============================================================================
# RDAL1 (ALW) DATA GENERATION
# ============================================================================

def generate_rdal1_data(report_date: datetime, num_records: int = 500):
    """Generate RDAL1 (ALW) data with REPTDATE"""

    print(f"Generating RDAL1 data for {report_date.strftime('%d/%m/%Y')}...")

    all_itcodes = AL_ITCODES + OB_ITCODES + SP_ITCODES

    data = {
        'REPTDATE': [],
        'ITCODE': [],
        'AMTIND': [],
        'AMOUNT': []
    }

    # Generate records for each item code
    for itcode in all_itcodes:
        # Determine AMTIND from ITCODE
        if len(itcode) >= 14:
            last_char = itcode[13]
            if last_char == 'D':
                amtind = 'D'
            elif last_char == 'I':
                amtind = 'I'
            elif last_char == 'F':
                amtind = 'F'
            else:
                amtind = 'D' if itcode[1] != '0' else ' '
        else:
            amtind = 'D'

        data['REPTDATE'].append(report_date)
        data['ITCODE'].append(itcode)
        data['AMTIND'].append(amtind)
        data['AMOUNT'].append(generate_amount())

    # Add duplicates for aggregation testing
    for _ in range(num_records - len(all_itcodes)):
        itcode = random.choice(all_itcodes)
        last_char = itcode[13] if len(itcode) >= 14 else 'Y'

        if last_char == 'D':
            amtind = 'D'
        elif last_char == 'I':
            amtind = 'I'
        elif last_char == 'F':
            amtind = 'F'
        else:
            amtind = 'D' if itcode[1] != '0' else ' '

        data['REPTDATE'].append(report_date)
        data['ITCODE'].append(itcode)
        data['AMTIND'].append(amtind)
        data['AMOUNT'].append(generate_amount())

    df = pl.DataFrame(data)

    output_path = FISS_DIR / "rdal1_text.parquet"
    df.write_parquet(output_path)

    print(f"  Created: {output_path}")
    print(f"  Records: {len(df):,}")
    return df


# ============================================================================
# RGAC (GAY) DATA GENERATION
# ============================================================================

def generate_rgac_data(report_date: datetime, num_records: int = 300):
    """Generate RGAC (GAY) global assets data"""

    print(f"Generating RGAC data for {report_date.strftime('%d/%m/%Y')}...")

    data = {
        'REPTDATE': [],
        'ITCODE': [],
        'AMTIND': [],
        'AMOUNT': []
    }

    # Subset of codes for global reporting
    gay_codes = AL_ITCODES[:10] + SP_ITCODES[:5]

    for itcode in gay_codes:
        last_char = itcode[13] if len(itcode) >= 14 else 'Y'

        if last_char == 'D':
            amtind = 'D'
        elif last_char == 'I':
            amtind = 'I'
        elif last_char == 'F':
            amtind = 'F'
        else:
            amtind = 'D'

        data['REPTDATE'].append(report_date)
        data['ITCODE'].append(itcode)
        data['AMTIND'].append(amtind)
        data['AMOUNT'].append(generate_amount())

    # Add random records
    for _ in range(num_records - len(gay_codes)):
        itcode = random.choice(gay_codes)
        last_char = itcode[13] if len(itcode) >= 14 else 'Y'

        if last_char == 'D':
            amtind = 'D'
        elif last_char == 'I':
            amtind = 'I'
        elif last_char == 'F':
            amtind = 'F'
        else:
            amtind = 'D'

        data['REPTDATE'].append(report_date)
        data['ITCODE'].append(itcode)
        data['AMTIND'].append(amtind)
        data['AMOUNT'].append(generate_amount())

    df = pl.DataFrame(data)

    # Save current and previous period
    output_path = RGAC_DIR / "rgac_text.parquet"
    df.write_parquet(output_path)
    print(f"  Created: {output_path}")

    # Create lag-1 version (previous period)
    prev_date = get_lag_date(1)
    df_prev = df.with_columns(pl.lit(prev_date).alias('REPTDATE'))
    output_path_lag = RGAC_DIR / "RGAC_lag1.parquet"
    df_prev.write_parquet(output_path_lag)
    print(f"  Created: {output_path_lag}")

    print(f"  Records: {len(df):,}")
    return df


# ============================================================================
# LOAN DATA GENERATION
# ============================================================================

def generate_loan_data(report_date: datetime, num_records: int = 1000):
    """Generate loan note (MNILN) data"""

    print(f"Generating Loan data for {report_date.strftime('%d/%m/%Y')}...")

    data = {
        'REPTDATE': [],
        'ACCNO': [],
        'LOANTYPE': [],
        'BALANCE': [],
        'BRANCH': [],
        'CUSTOMER_TYPE': [],
        'INTEREST_RATE': [],
        'PZIPCODE': []
    }

    # CAG zip codes for special processing
    cag_zipcodes = [2002, 2013, 3039, 3047, 800003098, 800003114,
                    800004016, 800004022, 800004029, 800040050,
                    800040053, 800050024, 800060024, 800060045,
                    800060081, 80060085]

    non_cag_zipcodes = [1000, 1100, 2000, 3000, 4000, 5000, 10000, 20000]

    for i in range(num_records):
        # 30% CAG, 70% non-CAG
        if random.random() < 0.3:
            zipcode = random.choice(cag_zipcodes)
        else:
            zipcode = random.choice(non_cag_zipcodes)

        data['REPTDATE'].append(report_date)
        data['ACCNO'].append(f"LN{i:08d}")
        data['LOANTYPE'].append(random.choice(LOAN_TYPES))
        data['BALANCE'].append(generate_loan_balance())
        data['BRANCH'].append(random.choice(BRANCH_CODES))
        data['CUSTOMER_TYPE'].append(random.choice(CUSTOMER_TYPES))
        data['INTEREST_RATE'].append(random.uniform(3.0, 8.0))
        data['PZIPCODE'].append(zipcode)

    df = pl.DataFrame(data)

    # Save current period
    output_path = MNILN_DIR / "MNILN_current.parquet"
    df.write_parquet(output_path)
    print(f"  Created: {output_path}")

    # Create lag-4 version (4 months back)
    lag_date = get_lag_date(4)
    df_lag = df.with_columns(pl.lit(lag_date).alias('REPTDATE'))
    output_path_lag = MNILN_DIR / "MNILN_lag4.parquet"
    df_lag.write_parquet(output_path_lag)
    print(f"  Created: {output_path_lag}")

    print(f"  Records: {len(df):,}")
    return df


# ============================================================================
# FIXED DEPOSIT DATA GENERATION
# ============================================================================

def generate_fd_data(report_date: datetime, num_records: int = 800):
    """Generate fixed deposit (MNIFD) data"""

    print(f"Generating FD data for {report_date.strftime('%d/%m/%Y')}...")

    data = {
        'REPTDATE': [],
        'ACCNO': [],
        'BALANCE': [],
        'BRANCH': [],
        'MATURITY_DATE': [],
        'INTEREST_RATE': [],
        'TERM_MONTHS': []
    }

    for i in range(num_records):
        term = random.choice([1, 3, 6, 12, 24, 36, 60])
        maturity = report_date + timedelta(days=term * 30)

        data['REPTDATE'].append(report_date)
        data['ACCNO'].append(f"FD{i:08d}")
        data['BALANCE'].append(generate_deposit_balance())
        data['BRANCH'].append(random.choice(BRANCH_CODES))
        data['MATURITY_DATE'].append(maturity)
        data['INTEREST_RATE'].append(random.uniform(2.5, 4.5))
        data['TERM_MONTHS'].append(term)

    df = pl.DataFrame(data)

    output_path = MNIFD_DIR / "MNIFD_current.parquet"
    df.write_parquet(output_path)

    print(f"  Created: {output_path}")
    print(f"  Records: {len(df):,}")
    return df


# ============================================================================
# DEPOSIT DATA GENERATION
# ============================================================================

def generate_deposit_data(report_date: datetime, num_records: int = 2000):
    """Generate deposit account (MNITB) data"""

    print(f"Generating Deposit data for {report_date.strftime('%d/%m/%Y')}...")

    data = {
        'REPTDATE': [],
        'ACCNO': [],
        'ACCTYPE': [],
        'BALANCE': [],
        'BRANCH': [],
        'CUSTOMER_TYPE': []
    }

    for i in range(num_records):
        data['REPTDATE'].append(report_date)
        data['ACCNO'].append(f"DEP{i:08d}")
        data['ACCTYPE'].append(random.choice(ACCOUNT_TYPES))
        data['BALANCE'].append(generate_deposit_balance())
        data['BRANCH'].append(random.choice(BRANCH_CODES))
        data['CUSTOMER_TYPE'].append(random.choice(CUSTOMER_TYPES))

    df = pl.DataFrame(data)

    # Save current period
    output_path = MNITB_DIR / "MNITB_current.parquet"
    df.write_parquet(output_path)
    print(f"  Created: {output_path}")

    # Create lag-4 version
    lag_date = get_lag_date(4)
    df_lag = df.with_columns(pl.lit(lag_date).alias('REPTDATE'))
    output_path_lag = MNITB_DIR / "MNITB_lag4.parquet"
    df_lag.write_parquet(output_path_lag)
    print(f"  Created: {output_path_lag}")

    print(f"  Records: {len(df):,}")
    return df


# ============================================================================
# BNM KAPITI TABLES GENERATION
# ============================================================================

def generate_kapiti_tables(report_date: datetime):
    """Generate BNM KAPITI regulatory tables"""

    print(f"Generating KAPITI tables for {report_date.strftime('%d/%m/%Y')}...")

    # KAPITI1 - Capital Adequacy
    data1 = {
        'REPTDATE': [report_date] * 10,
        'ITEM_CODE': [f"K1{i:03d}" for i in range(10)],
        'AMOUNT': [generate_amount() for _ in range(10)],
        'CATEGORY': ['CAP'] * 10
    }
    df1 = pl.DataFrame(data1)
    df1.write_parquet(KAPITI_DIR / "KAPITI1.parquet")
    print(f"  Created: KAPITI1.parquet")

    # KAPITI2 - Liquidity
    data2 = {
        'REPTDATE': [report_date] * 10,
        'ITEM_CODE': [f"K2{i:03d}" for i in range(10)],
        'AMOUNT': [generate_amount() for _ in range(10)],
        'CATEGORY': ['LIQ'] * 10
    }
    df2 = pl.DataFrame(data2)
    df2.write_parquet(KAPITI_DIR / "KAPITI2.parquet")
    print(f"  Created: KAPITI2.parquet")

    # KAPITI3 - Risk Weighted Assets
    data3 = {
        'REPTDATE': [report_date] * 10,
        'ITEM_CODE': [f"K3{i:03d}" for i in range(10)],
        'AMOUNT': [generate_amount() for _ in range(10)],
        'CATEGORY': ['RWA'] * 10
    }
    df3 = pl.DataFrame(data3)
    df3.write_parquet(KAPITI_DIR / "KAPITI3.parquet")
    print(f"  Created: KAPITI3.parquet")

    # KAPITI SASDATA - Combined reference
    all_data = pl.concat([df1, df2, df3])
    all_data.write_parquet(KAPITI_DIR / "KAPITI_SASDATA.parquet")
    print(f"  Created: KAPITI_SASDATA.parquet")


# ============================================================================
# MAIN GENERATION
# ============================================================================

def generate_all_sample_data():
    """Generate all sample data files"""

    print("=" * 70)
    print("EIBMMTH1 Sample Data Generator")
    print("=" * 70)
    print()

    # Get report date
    report_date = get_report_date()
    print(f"Report Date: {report_date.strftime('%d/%m/%Y')}")
    print()

    # Generate all data sources
    generate_rdal1_data(report_date, num_records=500)
    print()

    generate_rgac_data(report_date, num_records=300)
    print()

    generate_loan_data(report_date, num_records=1000)
    print()

    generate_fd_data(report_date, num_records=800)
    print()

    generate_deposit_data(report_date, num_records=2000)
    print()

    generate_kapiti_tables(report_date)
    print()

    print("=" * 70)
    print("Sample Data Generation Complete!")
    print("=" * 70)
    print()
    print("Generated data:")
    print(f"  RDAL1 (ALW):     {FISS_DIR}")
    print(f"  RGAC (GAY):      {RGAC_DIR}")
    print(f"  Loans:           {MNILN_DIR}")
    print(f"  Fixed Deposits:  {MNIFD_DIR}")
    print(f"  Deposits:        {MNITB_DIR}")
    print(f"  KAPITI Tables:   {KAPITI_DIR}")
    print()
    print("Directory structure:")
    print(f"  {INPUT_PATH}/")
    print(f"    fiss/")
    print(f"      rdal1_text.parquet")
    print(f"    rgac/")
    print(f"      rgac_text.parquet")
    print(f"      RGAC_lag1.parquet")
    print(f"    mniln/")
    print(f"      MNILN_current.parquet")
    print(f"      MNILN_lag4.parquet")
    print(f"    mnifd/")
    print(f"      MNIFD_current.parquet")
    print(f"    mnitb/")
    print(f"      MNITB_current.parquet")
    print(f"      MNITB_lag4.parquet")
    print(f"    kapiti/")
    print(f"      KAPITI1.parquet")
    print(f"      KAPITI2.parquet")
    print(f"      KAPITI3.parquet")
    print(f"      KAPITI_SASDATA.parquet")
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

    # Inspect RDAL1
    rdal1_path = FISS_DIR / "rdal1_text.parquet"
    if rdal1_path.exists():
        df = pl.read_parquet(rdal1_path)
        print(f"RDAL1 (ALW):")
        print(f"  Total records: {len(df):,}")
        print(f"  Date range: {df.select('REPTDATE').min().item()} to {df.select('REPTDATE').max().item()}")
        print(f"  Unique ITCODEs: {df.select('ITCODE').n_unique():,}")
        print(f"  Sample data:")
        print(df.head(3))
        print()

    # Inspect Loans
    loan_path = MNILN_DIR / "MNILN_current.parquet"
    if loan_path.exists():
        df = pl.read_parquet(loan_path)
        print(f"Loans (MNILN):")
        print(f"  Total records: {len(df):,}")
        print(f"  Total balance: ${df.select('BALANCE').sum().item():,.2f}")
        print(f"  Loan types: {sorted(df.select('LOANTYPE').unique().to_series().to_list())}")
        print(f"  Sample data:")
        print(df.head(3))
        print()

    # Inspect Deposits
    deposit_path = MNITB_DIR / "MNITB_current.parquet"
    if deposit_path.exists():
        df = pl.read_parquet(deposit_path)
        print(f"Deposits (MNITB):")
        print(f"  Total records: {len(df):,}")
        print(f"  Total balance: ${df.select('BALANCE').sum().item():,.2f}")
        print(f"  Account types: {sorted(df.select('ACCTYPE').unique().to_series().to_list())}")
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
        print("  python generate_eibmmth1_data.py --inspect")
        print()
