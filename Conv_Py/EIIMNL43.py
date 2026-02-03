#!/usr/bin/env python3
"""
File Name: EIIMNL43
BNM Liquidity Framework - Final Reporting
Creates behavioral maturity profile reports
"""

import polars as pl
from pathlib import Path


# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Input paths
STORE_FINALSUM_PATH = "/data/input/store_finalsum.parquet"

# Output paths
OUTPUT_DIR = "/data/output"
STORE_NOTERMP1_PATH = f"{OUTPUT_DIR}/store_notermp1.parquet"
STORE_NOTERMP2_PATH = f"{OUTPUT_DIR}/store_notermp2.parquet"
STORE_NOTEFXP1_PATH = f"{OUTPUT_DIR}/store_notefxp1.parquet"
STORE_NOTEFXP2_PATH = f"{OUTPUT_DIR}/store_notefxp2.parquet"

REPORT_RM_PART1_PATH = f"{OUTPUT_DIR}/nlf_rm_part1.txt"
REPORT_RM_PART2_PATH = f"{OUTPUT_DIR}/nlf_rm_part2.txt"

# Create output directory
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


# ============================================================================
# PRODUCT DESCRIPTION MAPPING
# ============================================================================

PRODUCT_MAPPING = {
    # RM - Non-Individuals (09)
    ('9321109', 'A1.01', '- FIXED TERM FINANCING  '): 'NONRMFL',
    ('9521109', 'A1.01', '- FIXED TERM FINANCING  '): 'NONRMFL',
    ('9321209', 'A1.02', '- REVOLVING FINANCING  '): 'NONRMRC',
    ('9521209', 'A1.02', '- REVOLVING FINANCING  '): 'NONRMRC',
    ('9321309', 'A1.03', '- CASH LINE FACILITY'): 'NONRMOD',
    ('9521309', 'A1.03', '- CASH LINE FACILITY'): 'NONRMOD',
    ('9321909', 'A1.04', '- OTHERS'): 'NONRMOT',
    ('9521909', 'A1.04', '- OTHERS'): 'NONRMOT',

    # RM - Individuals (08)
    ('9321408', 'A1.05', '- HOUSING FINANCING'): 'INDRMHL',
    ('9521408', 'A1.05', '- HOUSING FINANCING'): 'INDRMHL',
    # Credit cards commented out in original
    # ('9321508', 'A1.06', '- CREDIT CARDS'): 'INDRMCC',
    # ('9521508', 'A1.06', '- CREDIT CARDS'): 'INDRMCC',
    ('9321308', 'A1.07', '- CASH LINE FACILITY'): 'INDRMOD',
    ('9521308', 'A1.07', '- CASH LINE FACILITY'): 'INDRMOD',
    ('9321908', 'A1.08', '- OTHERS'): 'INDRMOT',
    ('9521908', 'A1.08', '- OTHERS'): 'INDRMOT',

    # RM - Miscellaneous (00)
    ('9322100', 'A1.09', '- CASH HOLDINGS'): 'MISRMCH',
    ('9522100', 'A1.09', '- CASH HOLDINGS'): 'MISRMCH',
    ('9322200', 'A1.10', '- SRR'): 'MISRMSR',
    ('9522200', 'A1.10', '- SRR'): 'MISRMSR',
    ('9322900', 'A1.11', '- OTHER ASSETS'): 'MISRMOT',
    ('9522900', 'A1.11', '- OTHER ASSETS'): 'MISRMOT',

    # FX - Non-Individuals (09)
    ('9421109', 'B1.01', '- FIXED TERM FINANCING  '): 'NONFXFL',
    ('9621109', 'B1.01', '- FIXED TERM FINANCING  '): 'NONFXFL',
    ('9421209', 'B1.02', '- REVOLVING FINANCING  '): 'NONFXRC',
    ('9621209', 'B1.02', '- REVOLVING FINANCING  '): 'NONFXRC',
    ('9421309', 'B1.03', '- CASH LINE FACILITY'): 'NONFXOD',
    ('9621309', 'B1.03', '- CASH LINE FACILITY'): 'NONFXOD',
    ('9421909', 'B1.04', '- OTHERS'): 'NONFXOT',
    ('9621909', 'B1.04', '- OTHERS'): 'NONFXOT',

    # FX - Individuals (08)
    ('9421408', 'B1.05', '- HOUSING FINANCING'): 'INDFXHL',
    ('9621408', 'B1.05', '- HOUSING FINANCING'): 'INDFXHL',
    # Credit cards commented out in original
    # ('9421508', 'B1.06', '- CREDIT CARDS'): 'INDFXCC',
    # ('9621508', 'B1.06', '- CREDIT CARDS'): 'INDFXCC',
    ('9421308', 'B1.07', '- CASH LINE FACILITY'): 'INDFXOD',
    ('9621308', 'B1.07', '- CASH LINE FACILITY'): 'INDFXOD',
    ('9421908', 'B1.08', '- OTHERS'): 'INDFXOT',
    ('9621908', 'B1.08', '- OTHERS'): 'INDFXOT',

    # FX - Miscellaneous (00)
    ('9422100', 'B1.09', '- CASH HOLDINGS'): 'MISFXCH',
    ('9622100', 'B1.09', '- CASH HOLDINGS'): 'MISFXCH',
    ('9422200', 'B1.10', '- SRR'): 'MISFXSR',
    ('9622200', 'B1.10', '- SRR'): 'MISFXSR',
    ('9422900', 'B1.11', '- OTHER ASSETS'): 'MISFXOT',
    ('9622900', 'B1.11', '- OTHER ASSETS'): 'MISFXOT',
}


def get_product_info(prod):
    """Get product description, item code, and item description"""
    for (prod_code, item, item4), desc in PRODUCT_MAPPING.items():
        if prod == prod_code:
            return desc, item, item4
    return None, None, None


# ============================================================================
# LOAD AND PROCESS DATA
# ============================================================================

print("Step 1: Loading FINALSUM data...")

finalsum = pl.read_parquet(STORE_FINALSUM_PATH)
print(f"  Loaded records: {len(finalsum)}")

# Extract PROD (first 7 characters) and INDNON (characters 6-7)
loan = finalsum.with_columns([
    pl.col('BNMCODE').str.slice(0, 7).alias('PROD'),
    pl.col('BNMCODE').str.slice(5, 2).alias('INDNON'),
    (pl.col('AMOUNT') / 1000).round(0).alias('AMOUNT')  # Round to nearest 1000, then divide by 1000
])

print(f"  Processed records: {len(loan)}")


# ============================================================================
# ASSIGN DESCRIPTIONS
# ============================================================================

print("\nStep 2: Assigning product descriptions...")

# Create mapping columns
desc_list = []
for row in loan.iter_rows(named=True):
    prod = row['PROD']
    desc, _, _ = get_product_info(prod)
    desc_list.append(desc)

loan = loan.with_columns([
    pl.Series('DESC', desc_list)
])

# Filter out records without descriptions
loan = loan.filter(pl.col('DESC').is_not_null())
print(f"  Records with descriptions: {len(loan)}")


# ============================================================================
# TRANSPOSE DATA
# ============================================================================

print("\nStep 3: Transposing data by time buckets...")

# The original data has BNMCODE with bucket information in positions 8-9
# We need to pivot so each time bucket becomes a column

# Extract time bucket
loan = loan.with_columns([
    pl.col('BNMCODE').str.slice(7, 2).alias('BUCKET')
])

# Map bucket codes to column names
bucket_map = {
    '01': 'WEEK',
    '02': 'MONTH',
    '03': 'QTR',
    '04': 'HALFYR',
    '05': 'YEAR',
    '06': 'LAST'
}

# Pivot the data
loan_pivot = loan.pivot(
    values='AMOUNT',
    index=['PROD', 'DESC'],
    columns='BUCKET',
    aggregate_function='sum'
).sort(['PROD', 'DESC'])

# Rename columns based on bucket map
for bucket_code, col_name in bucket_map.items():
    if bucket_code in loan_pivot.columns:
        loan_pivot = loan_pivot.rename({bucket_code: col_name})

# Fill missing columns with 0
for col_name in ['WEEK', 'MONTH', 'QTR', 'HALFYR', 'YEAR', 'LAST']:
    if col_name not in loan_pivot.columns:
        loan_pivot = loan_pivot.with_columns([
            pl.lit(0.0).alias(col_name)
        ])

# Ensure column order
loan_pivot = loan_pivot.select([
    'PROD', 'DESC', 'WEEK', 'MONTH', 'QTR', 'HALFYR', 'YEAR', 'LAST'
])

print(f"  Transposed records: {len(loan_pivot)}")


# ============================================================================
# ADD ITEM CODES AND CALCULATE BALANCE
# ============================================================================

print("\nStep 4: Adding item codes and calculating balances...")

# Add ITEM and ITEM4 columns
item_list = []
item4_list = []

for row in loan_pivot.iter_rows(named=True):
    prod = row['PROD']
    _, item, item4 = get_product_info(prod)
    item_list.append(item if item else '')
    item4_list.append(item4 if item4 else '')

store = loan_pivot.with_columns([
    pl.Series('ITEM', item_list),
    pl.Series('ITEM4', item4_list)
])

# Calculate BALANCE (sum of all time buckets)
store = store.with_columns([
    (pl.col('WEEK') + pl.col('MONTH') + pl.col('QTR') +
     pl.col('HALFYR') + pl.col('YEAR') + pl.col('LAST')).alias('BALANCE')
])

# Add INDNON
store = store.with_columns([
    pl.col('PROD').str.slice(5, 2).alias('INDNON')
])

print(f"  Store records: {len(store)}")


# ============================================================================
# CREATE OUTPUT DATASETS
# ============================================================================

print("\nStep 5: Creating output datasets...")

# NOTERMP1: RM Part 1 (93xxx codes)
notermp1 = store.filter(
    (pl.col('PROD').str.slice(0, 2) == '93') &
    (pl.col('ITEM') != '')
).sort('INDNON', descending=True)

notermp1.write_parquet(STORE_NOTERMP1_PATH)
print(f"  NOTERMP1 records: {len(notermp1)} -> {STORE_NOTERMP1_PATH}")

# NOTERMP2: RM Part 2 (95xxx codes)
notermp2 = store.filter(
    (pl.col('PROD').str.slice(0, 2) == '95') &
    (pl.col('ITEM') != '')
).sort('INDNON', descending=True)

notermp2.write_parquet(STORE_NOTERMP2_PATH)
print(f"  NOTERMP2 records: {len(notermp2)} -> {STORE_NOTERMP2_PATH}")

# NOTEFXP1: FX Part 1 (94xxx codes)
notefxp1 = store.filter(
    (pl.col('PROD').str.slice(0, 2) == '94') &
    (pl.col('ITEM') != '')
).sort('INDNON', descending=True)

notefxp1.write_parquet(STORE_NOTEFXP1_PATH)
print(f"  NOTEFXP1 records: {len(notefxp1)} -> {STORE_NOTEFXP1_PATH}")

# NOTEFXP2: FX Part 2 (96xxx codes)
notefxp2 = store.filter(
    (pl.col('PROD').str.slice(0, 2) == '96') &
    (pl.col('ITEM') != '')
).sort('INDNON', descending=True)

notefxp2.write_parquet(STORE_NOTEFXP2_PATH)
print(f"  NOTEFXP2 records: {len(notefxp2)} -> {STORE_NOTEFXP2_PATH}")


# ============================================================================
# GENERATE REPORTS
# ============================================================================

print("\nStep 6: Generating reports...")


def write_nlf_report(filepath, data_df, part_title):
    """
    Write NLF (New Liquidity Framework) report
    Format: Semicolon-delimited CSV-style format
    """

    with open(filepath, 'w') as f:
        # Add ITEM2 and ITEM3 columns
        report_data = []
        for row in data_df.iter_rows(named=True):
            indnon = row['INDNON']

            item2 = 'LOAN :'
            if indnon == '08':
                item3 = 'INDIVIDUALS    '
            elif indnon == '09':
                item3 = 'NON-INDIVIDUALS'
            else:
                item3 = ''

            report_data.append({
                'ITEM': row['ITEM'],
                'ITEM2': item2,
                'ITEM3': item3,
                'ITEM4': row['ITEM4'],
                'WEEK': row['WEEK'],
                'MONTH': row['MONTH'],
                'QTR': row['QTR'],
                'HALFYR': row['HALFYR'],
                'YEAR': row['YEAR'],
                'LAST': row['LAST'],
                'BALANCE': row['BALANCE']
            })

        # Write header (written once)
        f.write(' \n')
        f.write(f'BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - {part_title}\n')
        f.write(' \n')
        f.write('INFLOW(ASSETS)\n')
        f.write('ON BALANCE SHEET\n')
        f.write(
            '    ;    ;    ;    ;UP TO 1 WK ;>1 WK - 1 MTH;>1 MTH - 3 MTHS;>3 MTHS - 6 MTHS;>6 MTHS - 1 YR;>1 YEAR;TOTAL;\n')

        # Write data rows
        for row in report_data:
            # Format numbers: replace 0 with empty string (OPTIONS MISSING=0)
            week = '' if row['WEEK'] == 0 else str(row['WEEK'])
            month = '' if row['MONTH'] == 0 else str(row['MONTH'])
            qtr = '' if row['QTR'] == 0 else str(row['QTR'])
            halfyr = '' if row['HALFYR'] == 0 else str(row['HALFYR'])
            year = '' if row['YEAR'] == 0 else str(row['YEAR'])
            last = '' if row['LAST'] == 0 else str(row['LAST'])
            balance = '' if row['BALANCE'] == 0 else str(row['BALANCE'])

            f.write(f"{row['ITEM']};{row['ITEM2']};{row['ITEM3']};{row['ITEM4']};")
            f.write(f"{week};{month};{qtr};{halfyr};{year};{last};{balance};\n")

    print(f"  Report written to: {filepath}")


# Generate RM Part 1 report
write_nlf_report(REPORT_RM_PART1_PATH, notermp1, 'RINGGIT PART 1-RM')

# Generate RM Part 2 report
write_nlf_report(REPORT_RM_PART2_PATH, notermp2, 'RINGGIT PART 2-RM')


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("BNM Liquidity Framework Reporting completed successfully!")
print("=" * 70)

print(f"\nRecord Counts:")
print(f"  Input (FINALSUM): {len(finalsum)}")
print(f"  After filtering: {len(loan)}")
print(f"  After transpose: {len(loan_pivot)}")
print(f"  RM Part 1 (93xxx): {len(notermp1)}")
print(f"  RM Part 2 (95xxx): {len(notermp2)}")
print(f"  FX Part 1 (94xxx): {len(notefxp1)}")
print(f"  FX Part 2 (96xxx): {len(notefxp2)}")

print(f"\nOutput files:")
print(f"  - NOTERMP1: {STORE_NOTERMP1_PATH}")
print(f"  - NOTERMP2: {STORE_NOTERMP2_PATH}")
print(f"  - NOTEFXP1: {STORE_NOTEFXP1_PATH}")
print(f"  - NOTEFXP2: {STORE_NOTEFXP2_PATH}")
print(f"  - RM Part 1 Report: {REPORT_RM_PART1_PATH}")
print(f"  - RM Part 2 Report: {REPORT_RM_PART2_PATH}")

# Display sample of RM Part 1 data
if len(notermp1) > 0:
    print(f"\nSample RM Part 1 Data (first 5 rows):")
    print(notermp1.head(5))
