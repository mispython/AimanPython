"""
Sample Data Generator for EIBMBAEI Report
Generates realistic test data for BAE Personal Financing-I customer profile report
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import random
import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================
OUTPUT_PATH = Path("/data/input")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Report parameters
REPORT_DATE = datetime(2024, 6, 30)  # June 2024
CURRENT_MONTH = 6
WEEK = 4
REPORT_YEAR_2 = "24"

NUM_ACCOUNTS = 1000  # Number of sample accounts


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_account_number(idx):
    """Generate realistic account number"""
    return f"BAE{idx:08d}"


def generate_birthdate():
    """Generate realistic birthdate"""
    age = random.randint(18, 58)
    birth_year = 2024 - age
    birth_month = random.randint(1, 12)
    birth_day = random.randint(1, 28)
    return f"{birth_day:02d}{birth_month:02d}{birth_year}"


# ============================================================================
# 1. GENERATE REPTDATE (REPORTING DATE)
# ============================================================================

def generate_reptdate():
    """Generate reporting date file"""
    reptdate_df = pl.DataFrame({
        "REPTDATE": [REPORT_DATE]
    })

    output_file = OUTPUT_PATH / "reptdate.parquet"
    reptdate_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return reptdate_df


# ============================================================================
# 2. GENERATE LOAN DATA
# ============================================================================

def generate_loan_data(month, week, num_records=NUM_ACCOUNTS):
    """Generate loan data for Product 135 (BAE Personal Financing-I)"""

    np.random.seed(42)
    random.seed(42)

    # State codes distribution
    state_codes = ['A', 'B', 'C', 'D', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'W']
    state_weights = [0.05, 0.20, 0.05, 0.05, 0.15, 0.05, 0.01, 0.05, 0.05, 0.10, 0.08, 0.02, 0.08, 0.05, 0.06]

    # Tenure options (in months) - add 3 since SAS subtracts 3
    tenure_options = [27, 39, 51, 63, 75, 87, 99, 111, 123, 135, 147, 159, 171, 183, 195, 207, 219, 231, 243]

    accounts = []

    for i in range(num_records):
        acctno = generate_account_number(i)

        # Product is always 135 for BAE
        product = 135

        # State code
        statecd = random.choices(state_codes, weights=state_weights)[0]

        # Financing limit (NETPROC / APPRLIM2)
        # Distribution skewed towards lower amounts
        limit_ranges = [
            (5000, 10000, 0.10),
            (10000, 20000, 0.15),
            (20000, 30000, 0.15),
            (30000, 50000, 0.20),
            (50000, 80000, 0.15),
            (80000, 120000, 0.10),
            (120000, 150000, 0.08),
            (150000, 200000, 0.07)
        ]

        selected_range = random.choices(limit_ranges, weights=[r[2] for r in limit_ranges])[0]
        netproc = round(random.uniform(selected_range[0], selected_range[1]), 2)

        # APPRLIM2 - sometimes higher than NETPROC (especially for > 150K)
        if netproc > 150000:
            apprlim2 = netproc
        else:
            apprlim2 = round(random.uniform(netproc * 0.9, netproc * 1.1), 2)

        # Balance - typically less than approved limit
        balance = round(random.uniform(netproc * 0.3, netproc * 0.95), 2)

        # Tenure
        noteterm = random.choice(tenure_options)

        accounts.append({
            "ACCTNO": acctno,
            "PRODUCT": product,
            "STATECD": statecd,
            "NETPROC": netproc,
            "APPRLIM2": apprlim2,
            "BALANCE": balance,
            "NOTETERM": noteterm,
            "OPENDATE": REPORT_DATE - timedelta(days=random.randint(30, 1095))
        })

    loan_df = pl.DataFrame(accounts)

    output_file = OUTPUT_PATH / f"loan_{month:02d}{week}.parquet"
    loan_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return loan_df


# ============================================================================
# 3. GENERATE CISLN (CUSTOMER INFORMATION)
# ============================================================================

def generate_cisln_data(loan_df):
    """Generate customer demographic data"""

    np.random.seed(100)
    random.seed(100)

    # Race distribution (1=Malay, 2=Chinese, 3=Indian, 4=Other)
    race_dist = ['1', '2', '3', '4']
    race_weights = [0.60, 0.25, 0.10, 0.05]

    # Gender distribution
    gender_dist = ['M', 'F']
    gender_weights = [0.55, 0.45]

    cisln_records = []

    for acctno in loan_df.select("ACCTNO").to_series():
        race = random.choices(race_dist, weights=race_weights)[0]
        gender = random.choices(gender_dist, weights=gender_weights)[0]
        birthdat = generate_birthdate()

        cisln_records.append({
            "ACCTNO": acctno,
            "SECCUST": "901",  # Filter value in original SAS
            "RACE": race,
            "GENDER": gender,
            "BIRTHDAT": birthdat
        })

    cisln_df = pl.DataFrame(cisln_records)

    output_file = OUTPUT_PATH / "cisln_loan.parquet"
    cisln_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return cisln_df


# ============================================================================
# 4. GENERATE ELDS (APPROVAL DATA)
# ============================================================================

def generate_elds_data(month, weeks, year, num_records=200):
    """Generate ELDS approval data for the month"""

    np.random.seed(200)
    random.seed(200)

    # Salary distribution
    salary_ranges = [
        (800, 1000, 0.05),
        (1000, 2000, 0.15),
        (2000, 3000, 0.20),
        (3000, 5000, 0.25),
        (5000, 7000, 0.15),
        (7000, 10000, 0.10),
        (10000, 15000, 0.08),
        (15000, 25000, 0.02)
    ]

    all_records = []

    for week in weeks:
        week_records = num_records // len(weeks)

        for i in range(week_records):
            # Select salary range
            selected_range = random.choices(salary_ranges, weights=[r[2] for r in salary_ranges])[0]
            gincome = round(random.uniform(selected_range[0], selected_range[1]), 2)

            # Amount approved (typically 30-50x monthly income)
            multiplier = random.uniform(30, 50)
            amount = round(gincome * multiplier, 2)

            # Some records might have missing income
            if random.random() < 0.02:
                gincome = None

            all_records.append({
                "PRODUCT": 135,
                "STATUS": "APPROVED",
                "GINCOME": gincome,
                "AMOUNT": amount,
                "APPROVEDATE": REPORT_DATE.replace(day=random.randint(1, 28))
            })

    # Create separate files for each week
    records_per_week = len(all_records) // len(weeks)

    for idx, week in enumerate(weeks):
        start_idx = idx * records_per_week
        end_idx = start_idx + records_per_week if idx < len(weeks) - 1 else len(all_records)

        week_records = all_records[start_idx:end_idx]
        elds_df = pl.DataFrame(week_records)

        output_file = OUTPUT_PATH / f"ieln{month:02d}{week}{year}.parquet"
        elds_df.write_parquet(output_file)
        print(f"✓ Generated: {output_file}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Generate all sample data files"""

    print("=" * 70)
    print("EIBMBAEI Sample Data Generator")
    print("=" * 70)
    print(f"Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
    print(f"Current Month: {CURRENT_MONTH:02d}")
    print(f"Number of Loan Accounts: {NUM_ACCOUNTS}")
    print("=" * 70)
    print()

    # 1. Generate reporting date
    print("Generating reporting date file...")
    reptdate_df = generate_reptdate()
    print()

    # 2. Generate loan data
    print("Generating loan data...")
    loan_df = generate_loan_data(CURRENT_MONTH, WEEK, NUM_ACCOUNTS)
    print()

    # 3. Generate customer demographic data
    print("Generating customer demographic data (CISLN)...")
    cisln_df = generate_cisln_data(loan_df)
    print()

    # 4. Generate ELDS approval data for all weeks
    print("Generating ELDS approval data for all weeks...")
    generate_elds_data(CURRENT_MONTH, ['1', '2', '3', '4'], REPORT_YEAR_2, num_records=200)
    print()

    # Summary statistics
    print("=" * 70)
    print("DATA GENERATION SUMMARY")
    print("=" * 70)
    print(f"Reporting Date Records: {len(reptdate_df)}")
    print(f"Loan Accounts: {len(loan_df)}")
    print(f"Customer Records: {len(cisln_df)}")
    print(f"ELDS Approval Records: ~200 (across 4 weeks)")
    print()

    # Distribution statistics
    print("PRODUCT DISTRIBUTION:")
    product_dist = loan_df.group_by("PRODUCT").agg(pl.len().alias("count"))
    print(product_dist)
    print()

    print("STATE CODE DISTRIBUTION:")
    state_dist = loan_df.group_by("STATECD").agg(pl.len().alias("count")).sort("STATECD")
    print(state_dist)
    print()

    print("BALANCE STATISTICS:")
    balance_stats = loan_df.select([
        pl.col("BALANCE").min().alias("min"),
        pl.col("BALANCE").mean().alias("mean"),
        pl.col("BALANCE").median().alias("median"),
        pl.col("BALANCE").max().alias("max")
    ])
    print(balance_stats)
    print()

    print("FINANCING LIMIT DISTRIBUTION:")
    limit_bins = loan_df.with_columns([
        pl.when(pl.col("NETPROC") <= 20000).then(pl.lit("Up to 20K"))
        .when(pl.col("NETPROC") <= 50000).then(pl.lit("20K-50K"))
        .when(pl.col("NETPROC") <= 100000).then(pl.lit("50K-100K"))
        .when(pl.col("NETPROC") <= 150000).then(pl.lit("100K-150K"))
        .otherwise(pl.lit(">150K"))
        .alias("LIMIT_BIN")
    ]).group_by("LIMIT_BIN").agg(pl.len().alias("count")).sort("LIMIT_BIN")
    print(limit_bins)
    print()

    print("RACE DISTRIBUTION:")
    race_dist = cisln_df.group_by("RACE").agg(pl.len().alias("count")).sort("RACE")
    print(race_dist)
    print()

    print("GENDER DISTRIBUTION:")
    gender_dist = cisln_df.group_by("GENDER").agg(pl.len().alias("count")).sort("GENDER")
    print(gender_dist)
    print()

    print("=" * 70)
    print("✓ All sample data files generated successfully!")
    print(f"✓ Output directory: {OUTPUT_PATH}")
    print("=" * 70)


if __name__ == "__main__":
    main()
