#!/usr/bin/env python3
"""
File Name: EIBWDIRT
BNM Interest Rate Reporting - Part I
Calculates weighted average interest rates for domestic deposits
"""

import duckdb
import polars as pl
from datetime import date
from pathlib import Path
import struct


# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Constants
ACELIMIT = 5000

# # Input paths
# DEPOSIT_REPTDATE_PATH = "/data/input/deposit_reptdate.parquet"
# DEPOSIT_SAVING_PATH = "/data/input/deposit_saving.parquet"
# DEPOSIT_CURRENT_PATH = "/data/input/deposit_current.parquet"
# RATE_FILE_PATH = "/data/input/rate.txt"
#
# # Output paths
# OUTPUT_DIR = "/data/output"
# REPORT_PATH = f"{OUTPUT_DIR}/bnm_interest_rate_report.txt"
#
# # Create output directory
# Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# Input paths
BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"

DEPOSIT_REPTDATE_PATH = INPUT_DIR / "deposit_reptdate.parquet"
DEPOSIT_SAVING_PATH   = INPUT_DIR / "deposit_saving.parquet"
DEPOSIT_CURRENT_PATH  = INPUT_DIR / "deposit_current.parquet"
RATE_FILE_PATH        = INPUT_DIR / "rate.txt"

# Output paths
REPORT_PATH = OUTPUT_DIR / "bnm_interest_rate_report.txt"

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# DATE CALCULATIONS AND WEEK DETERMINATION
# ============================================================================

def get_report_dates():
    """Get report date and calculate week number based on day of month"""
    # Load REPTDATE from deposit.reptdate
    reptdate_df = pl.read_parquet(DEPOSIT_REPTDATE_PATH)
    reptdate = reptdate_df.select('REPTDATE').item(0, 0)

    # Convert to Python date if needed
    if isinstance(reptdate, str):
        from datetime import datetime
        reptdate = datetime.strptime(reptdate, '%Y-%m-%d').date()

    day = reptdate.day

    # Determine week number based on day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    reptyear = str(reptdate.year)
    reptmon = f"{reptdate.month:02d}"
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime('%d/%m/%y')

    return {
        'reptdate': reptdate,
        'nowk': nowk,
        'reptyear': reptyear,
        'reptmon': reptmon,
        'reptday': reptday,
        'rdate': rdate
    }


dates = get_report_dates()
NOWK = dates['nowk']
REPTYEAR = dates['reptyear']
REPTMON = dates['reptmon']
REPTDAY = dates['reptday']
RDATE = dates['rdate']
REPTDATE = dates['reptdate']

print(f"Report Date: {RDATE}")
print(f"Week Number: {NOWK}")
print(f"Report Year: {REPTYEAR}, Month: {REPTMON}")


# ============================================================================
# LOAD AND PROCESS RATE FILE
# ============================================================================

print("\nStep 1: Loading and processing rate file...")

# Read rate file with fixed-width format
rates_data = []
with open(RATE_FILE_PATH, 'r') as f:
    for line in f:
        if len(line) < 27:
            continue

        trancd = line[0:3]
        product_str = line[3:6]
        tier_str = line[6:15]
        rate_str = line[15:20]
        yyyy = line[20:24]
        cc = line[20:22]
        yy = line[22:24]
        mmmm = line[24:26]
        dddd = line[26:28]

        # Parse packed decimal values
        try:
            product = int(product_str)
            tier = int(tier_str)
            rate = float(rate_str) / 10  # Divide by 10 to get decimal representation
        except ValueError:
            continue

        # Filter for TRANCD='215' and specific products
        if trancd == '215' and product in [200, 201, 202, 203, 204, 212, 213, 214, 215, 150, 177]:
            rates_data.append({
                'TRANCD': trancd,
                'PRODUCT': product,
                'TIER': tier,
                'RATE': rate,
                'YYYY': yyyy,
                'CC': cc,
                'YY': yy,
                'MMMM': mmmm,
                'DDDD': dddd
            })

rates = pl.DataFrame(rates_data)
print(f"  Rates loaded: {len(rates)} records")


# ============================================================================
# CALCULATE EFFECTIVE RATES
# ============================================================================

print("\nStep 2: Calculating effective rates...")


def calculate_effective_rate(row):
    """Calculate effective rate based on product type"""
    product = row['PRODUCT']
    rate = row['RATE']
    interest = rate / 100

    if product in [200, 201]:
        # Semi-annual compounding
        payment = 2
        effectr = ((1 + interest / 2) ** 2 - 1) * 100
        erate = round(effectr, 2)
    elif product in [202, 203, 213, 150, 177]:
        # Monthly compounding
        payment = 12
        effectr = ((1 + interest / 12) ** 12 - 1) * 100
        erate = round(effectr, 2)
    elif product == 212:
        # Monthly compounding with 1.05 multiplier
        payment = 12
        effectr = ((1 + (interest * 1.05) / 12) ** 12 - 1) * 100
        erate = round(effectr, 2)
    elif product in [204, 214, 215]:
        # Use rate as-is
        erate = rate
    else:
        erate = 0

    return erate


# Calculate effective rates
rates = rates.with_columns([
    (pl.col('RATE') / 100).alias('INTEREST')
])

rates = rates.with_columns([
    pl.struct(['PRODUCT', 'RATE', 'INTEREST'])
    .map_elements(calculate_effective_rate, return_dtype=pl.Float64)
    .alias('ERATE')
])

# Create EFFDATE for products 212 and 213
rates = rates.with_columns([
    (pl.col('DDDD') + pl.lit('/') + pl.col('MMMM') + pl.lit('/') + pl.col('YY')).alias('EFFDATE')
])

# Split into EFFECT and SARATE
effect = rates.clone()
sarate = rates.filter(pl.col('PRODUCT').is_in([212, 213]))

effect = effect.sort(['PRODUCT', 'TIER'])
print(f"  Effect rates: {len(effect)} records")
print(f"  SA rates: {len(sarate)} records")


# ============================================================================
# LOAD AND PROCESS ACCOUNT DATA
# ============================================================================

print("\nStep 3: Loading and processing account data...")

# Load saving and current accounts
saving = pl.read_parquet(DEPOSIT_SAVING_PATH)
current = pl.read_parquet(DEPOSIT_CURRENT_PATH)

# Combine accounts
accounts = pl.concat([saving, current])

# Filter accounts
accounts = accounts.filter(
    (~pl.col('OPENIND').is_in(['B', 'C', 'P'])) &
    (pl.col('CURBAL') >= 0) &
    (pl.col('PRODUCT').is_in([150, 151, 152, 181, 177, 200, 201, 202, 203, 204, 212, 213, 214, 215]))
)

print(f"  Accounts loaded: {len(accounts)} records")


# ============================================================================
# ASSIGN TIERS TO ACCOUNTS
# ============================================================================

print("\nStep 4: Assigning tiers to accounts...")


def assign_tier(product, curbal):
    """Assign tier based on product and current balance"""

    if product in [150, 151, 152, 181]:
        product = 150
        if curbal > ACELIMIT:
            tier = 999999999
            curbal = curbal - ACELIMIT
        else:
            tier = 5000
            curbal = 0
        return product, tier, curbal

    elif product == 177:
        if curbal < 5000:
            tier = 5000
        else:
            tier = 999999999
        return product, tier, curbal

    elif product in [200, 201]:
        if curbal < 2500:
            tier = 2500
        elif curbal < 5000:
            tier = 5000
        elif curbal < 10000:
            tier = 10000
        elif curbal < 30000:
            tier = 30000
        elif curbal < 50000:
            tier = 50000
        elif curbal < 75000:
            tier = 75000
        else:
            tier = 999999999
        return product, tier, curbal

    elif product == 202:
        if curbal < 500:
            tier = 500
        elif curbal < 2000:
            tier = 2000
        elif curbal < 5000:
            tier = 5000
        elif curbal < 10000:
            tier = 10000
        elif curbal < 30000:
            tier = 30000
        elif curbal < 50000:
            tier = 50000
        elif curbal < 75000:
            tier = 75000
        else:
            tier = 999999999
        return product, tier, curbal

    elif product == 203:
        if curbal < 2000:
            tier = 2000
        elif curbal < 5000:
            tier = 5000
        elif curbal < 10000:
            tier = 10000
        elif curbal < 30000:
            tier = 30000
        elif curbal < 50000:
            tier = 50000
        elif curbal < 75000:
            tier = 75000
        else:
            tier = 999999999
        return product, tier, curbal

    elif product == 204:
        tier = 999999999
        return product, tier, curbal

    elif product == 214:
        if curbal < 1000:
            tier = 1000
        elif curbal < 5000:
            tier = 5000
        elif curbal < 25000:
            tier = 25000
        elif curbal < 50000:
            tier = 50000
        else:
            tier = 999999999
        return product, tier, curbal

    elif product == 212:
        if curbal < 5000:
            tier = 5000
        elif curbal < 10000:
            tier = 10000
        elif curbal < 20000:
            tier = 20000
        elif curbal < 30000:
            tier = 30000
        elif curbal < 50000:
            tier = 50000
        else:
            tier = 999999999
        return product, tier, curbal

    elif product == 213:
        if curbal < 5000:
            tier = 5000
        elif curbal < 10000:
            tier = 10000
        elif curbal < 30000:
            tier = 30000
        elif curbal < 50000:
            tier = 50000
        elif curbal < 75000:
            tier = 75000
        else:
            tier = 999999999
        return product, tier, curbal

    elif product == 215:
        if curbal < 50000:
            tier = 50000
        else:
            tier = 999999999
        return product, tier, curbal

    else:
        return product, 0, 0


# Apply tier assignment
tier_results = accounts.select([
    pl.struct(['PRODUCT', 'CURBAL'])
    .map_elements(lambda x: assign_tier(x['PRODUCT'], x['CURBAL']),
                  return_dtype=pl.Struct([
                      pl.Field('PRODUCT', pl.Int64),
                      pl.Field('TIER', pl.Int64),
                      pl.Field('CURBAL', pl.Float64)
                  ]))
    .alias('tier_info')
]).unnest('tier_info')

# Update accounts with new values
accounts = accounts.with_columns([
    tier_results['PRODUCT'].alias('PRODUCT_NEW'),
    tier_results['TIER'].alias('TIER'),
    tier_results['CURBAL'].alias('CURBAL_NEW')
])

accounts = accounts.drop(['PRODUCT', 'CURBAL']).rename({
    'PRODUCT_NEW': 'PRODUCT',
    'CURBAL_NEW': 'CURBAL'
})

accounts = accounts.sort(['PRODUCT', 'TIER'])


# ============================================================================
# MERGE ACCOUNTS WITH RATES
# ============================================================================

print("\nStep 5: Merging accounts with rates...")

rateacct = accounts.join(
    effect.select(['PRODUCT', 'TIER', 'ERATE', 'RATE']),
    on=['PRODUCT', 'TIER'],
    how='left'
)

print(f"  Merged records: {len(rateacct)}")


# ============================================================================
# CALCULATE WEIGHTED AVERAGE INTEREST RATE
# ============================================================================

print("\nStep 6: Calculating weighted average interest rate...")

# Calculate totals
totint = (rateacct.select((pl.col('ERATE') * pl.col('CURBAL')).sum())).item(0, 0)
totamt = rateacct.select(pl.col('CURBAL').sum()).item(0, 0)

weighted_avg = totint / totamt if totamt > 0 else 0

print(f"  Total Interest: {totint:,.2f}")
print(f"  Total Amount: {totamt:,.2f}")
print(f"  Weighted Average Rate: {weighted_avg:.6f}")

# Create BNM output record
bnm_records = []
bnm_records.append({
    'BNMCODE': '8420300000000Y',
    'AMOUNT': weighted_avg,
    'EFFDATE': None,
    'FLAG': 'E'
})


# ============================================================================
# PROCESS SA RATES (Products 212, 213)
# ============================================================================

print("\nStep 7: Processing SA rates...")

if len(sarate) > 0:
    # Sort and get first by PRODUCT
    sarate = sarate.sort(['PRODUCT', 'RATE', 'EFFDATE'])
    sarate_first = sarate.unique(subset=['PRODUCT'], keep='first')

    # Sort by ERATE for min/max
    sarate_sorted = sarate_first.sort(['ERATE', 'EFFDATE'])

    # Get minimum rate (first record)
    if len(sarate_sorted) > 0:
        min_record = sarate_sorted.row(0, named=True)
        bnm_records.append({
            'BNMCODE': '8420100000000Y',
            'AMOUNT': min_record['ERATE'],
            'EFFDATE': min_record['EFFDATE'],
            'FLAG': 'E'
        })
        print(f"  Min SA Rate: {min_record['ERATE']} (Effective: {min_record['EFFDATE']})")

    # Get maximum rate (last record when sorted descending)
    sarate_sorted_desc = sarate_first.sort(['ERATE', 'EFFDATE'], descending=[True, False])
    if len(sarate_sorted_desc) > 0:
        max_record = sarate_sorted_desc.row(0, named=True)
        bnm_records.append({
            'BNMCODE': '8420200000000Y',
            'AMOUNT': max_record['ERATE'],
            'EFFDATE': max_record['EFFDATE'],
            'FLAG': 'E'
        })
        print(f"  Max SA Rate: {max_record['ERATE']} (Effective: {max_record['EFFDATE']})")


# ============================================================================
# CREATE FINAL BNM DATASET
# ============================================================================

print("\nStep 8: Creating final BNM dataset...")

bnm_output = pl.DataFrame(bnm_records)
bnm_output = bnm_output.rename({'BNMCODE': 'ITCODE'})
bnm_output = bnm_output.sort('ITCODE')

print(f"  Final BNM records: {len(bnm_output)}")


# ============================================================================
# WRITE BINARY OUTPUT FILE
# ============================================================================

# Determine output filename
# output_filename = f"{OUTPUT_DIR}/irwtt{REPTMON}{NOWK}.dat"
output_filename = OUTPUT_DIR / f"irwtt{REPTMON}{NOWK}.dat"

print(f"\nStep 9: Writing binary output to {output_filename}...")

# Write binary file with packed format
with open(output_filename, 'wb') as f:

    for row in bnm_output.iter_rows(named=True):
        # ITCODE: 14 bytes (char)
        itcode = row['ITCODE'].ljust(14)[:14].encode('ascii')

        # AMOUNT: 8 bytes (double)
        amount = row['AMOUNT']

        # EFFDATE: 8 bytes (char) - handle None
        if row['EFFDATE'] is None:
            effdate = ' ' * 8
        else:
            effdate = str(row['EFFDATE']).ljust(8)[:8]
        effdate = effdate.encode('ascii')

        # FLAG: 1 byte (char)
        flag = row['FLAG'].encode('ascii')

        # Write record
        f.write(itcode)
        f.write(struct.pack('d', amount))
        f.write(effdate)
        f.write(flag)

print(f"  Binary file written: {output_filename}")


# ============================================================================
# GENERATE REPORT WITH ASA CARRIAGE CONTROL
# ============================================================================

print("\nStep 10: Generating report...")


def write_report_with_asa():
    """Generate report with ASA carriage control characters"""

    PAGE_LENGTH = 60
    line_count = 0

    with open(REPORT_PATH, 'w') as f:

        def write_line(asa_char, content):
            """Write a line with ASA carriage control character"""
            nonlocal line_count
            f.write(f"{asa_char}{content}\n")
            line_count += 1

        def write_header():
            """Write page header"""
            nonlocal line_count
            write_line('1', ' ' * 80)  # Form feed
            write_line(' ', 'PUBLIC BANK BERHAD'.center(80))
            write_line(' ', 'REPORT ON DOMESTIC INTEREST RATE - PART I'.center(80))
            write_line(' ', f'REPORTING DATE : {RDATE}'.center(80))
            write_line(' ', ' ' * 80)
            write_line(' ', f"{'ITCODE':<20} {'AMOUNT':>15} {'EFFDATE':<10} {'FLAG':<5}")
            write_line(' ', '-' * 80)
            line_count = 7

        # Write header
        write_header()

        # Write data
        for row in bnm_output.iter_rows(named=True):
            # Check if we need a new page
            if line_count >= PAGE_LENGTH - 2:
                write_header()

            itcode = f"{row['ITCODE']:<20}"
            amount = f"{row['AMOUNT']:15.6f}"

            if row['EFFDATE'] is None:
                effdate = f"{' ':<10}"
            else:
                effdate = f"{row['EFFDATE']:<10}"

            flag = f"{row['FLAG']:<5}"

            line = f" {itcode} {amount} {effdate} {flag}"
            write_line(' ', line)

    print(f"  Report written to: {REPORT_PATH}")


write_report_with_asa()

print("\n" + "=" * 70)
print("BNM Interest Rate Reporting completed successfully!")
print("=" * 70)
print(f"\nOutput files:")
print(f"  - Binary data: {output_filename}")
print(f"  - Report: {REPORT_PATH}")
