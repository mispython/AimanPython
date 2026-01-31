#!usr/bin/env python 3
"""
File Name: EIBDTP5J_EIBDTP50
Top 100 Depositors Report Generator

Produces reports on top 100 largest FD/CA/SA depositors:
1. Individual customers
2. Corporate customers
3. Subsidiaries under top corporate depositors
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys


# ============================================================================
# CONFIGURATION - Define all paths early
# ============================================================================

# Input paths
INPUT_REPTDATE = "data/DEPOSIT/REPTDATE.parquet"
INPUT_CURRENT = "data/DEPOSIT/CURRENT.parquet"
INPUT_FD = "data/DEPOSIT/FD.parquet"
INPUT_SAVING = "data/DEPOSIT/SAVING.parquet"
INPUT_CISCA = "data/CISDP/DEPOSIT.parquet"
INPUT_CISFD = "data/CISFD/DEPOSIT.parquet"
INPUT_COF_MNI_LIST = "data/LIST/COF_MNI_DEPOSITOR_LIST.parquet"
INPUT_TOPDEP_EXCL = "data/LIST/KEEP_TOP_DEP_EXCL_PBB.parquet"

# Format lookup files (PBBDPFMT include file simulation)
FORMAT_CAPROD = "data/formats/CAPROD.parquet"  # PRODUCT -> PRODCD for CA
FORMAT_SAPROD = "data/formats/SAPROD.parquet"  # PRODUCT -> PRODCD for SA

# Output paths
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FD11TEXT = OUTPUT_DIR / "FD11TEXT.txt"  # Individual customers
OUTPUT_FD12TEXT = OUTPUT_DIR / "FD12TEXT.txt"  # Corporate customers
OUTPUT_FD2TEXT = OUTPUT_DIR / "FD2TEXT.txt"  # Subsidiaries

# Report settings
PAGE_LENGTH = 60
ASA_SPACE = ' '
ASA_SKIP = '0'
ASA_NEW_PAGE = '1'


# ============================================================================
# FORMAT MAPPINGS
# ============================================================================

def load_format_mappings():
    """Load format mappings for product codes"""
    caprod_map = {}
    saprod_map = {}

    try:
        con = duckdb.connect()

        if Path(FORMAT_CAPROD).exists():
            caprod_df = con.execute(f"SELECT * FROM '{FORMAT_CAPROD}'").pl()
            caprod_map = dict(zip(caprod_df['PRODUCT'], caprod_df['PRODCD']))

        if Path(FORMAT_SAPROD).exists():
            saprod_df = con.execute(f"SELECT * FROM '{FORMAT_SAPROD}'").pl()
            saprod_map = dict(zip(saprod_df['PRODUCT'], saprod_df['PRODCD']))

    except Exception as e:
        print(f"Warning: Could not load format files: {e}")

    return caprod_map, saprod_map


CAPROD_MAP, SAPROD_MAP = load_format_mappings()


def apply_caprod_format(product):
    """Apply CAPROD format to product code"""
    return CAPROD_MAP.get(product, str(product) if product else 'N')


def apply_saprod_format(product):
    """Apply SAPROD format to product code"""
    return SAPROD_MAP.get(product, str(product) if product else 'N')


# ============================================================================
# STEP 1: Read REPTDATE and create macro variables
# ============================================================================

print("Step 1: Reading REPTDATE...")

con = duckdb.connect()

reptdate_df = con.execute(f"SELECT * FROM '{INPUT_REPTDATE}'").pl()
reptdate_row = reptdate_df.row(0, named=True)
reptdate = reptdate_row['REPTDATE']

# Convert to datetime (SAS date = days since 1960-01-01)
if isinstance(reptdate, (int, float)):
    base_date = datetime(1960, 1, 1)
    reptdate_dt = base_date + timedelta(days=int(reptdate))
else:
    reptdate_dt = reptdate

# Determine week number based on day of month
day_of_month = reptdate_dt.day
if day_of_month == 8:
    NOWK = '1'
elif day_of_month == 15:
    NOWK = '2'
elif day_of_month == 22:
    NOWK = '3'
else:
    NOWK = '4'

REPTYEAR = reptdate_dt.year
REPTMON = f"{reptdate_dt.month:02d}"
REPTDAY = f"{reptdate_dt.day:02d}"
RDATE = reptdate_dt.strftime('%d/%m/%y')

print(f"  Report Date: {RDATE}")
print(f"  Week Number: {NOWK}")


# ============================================================================
# STEP 2: Load and prepare CIS data
# ============================================================================

print("\nStep 2: Loading CIS data...")

# Load CISCA (Current Account CIS)
cisca_query = f"""
SELECT CUSTNO, ACCTNO, CUSTNAME, ICNO, NEWIC, OLDIC, INDORG
FROM '{INPUT_CISCA}'
WHERE SECCUST = '901'
  AND ACCTNO BETWEEN 3000000000 AND 3999999999
"""
cisca_df = con.execute(cisca_query).pl()

# Update ICNO logic
cisca_df = cisca_df.with_columns([
    pl.when(pl.col('NEWIC') != '')
    .then(pl.col('NEWIC'))
    .otherwise(pl.col('CUSTNO'))
    .alias('ICNO')
])

print(f"  CISCA records: {len(cisca_df)}")

# Load CISFD (Fixed Deposit CIS)
cisfd_query = f"""
SELECT CUSTNO, ACCTNO, CUSTNAME, ICNO, NEWIC, OLDIC, INDORG
FROM '{INPUT_CISFD}'
WHERE SECCUST = '901'
  AND ((ACCTNO BETWEEN 1000000000 AND 1999999999)
    OR (ACCTNO BETWEEN 7000000000 AND 7999999999)
    OR (ACCTNO BETWEEN 4000000000 AND 6999999999))
"""
cisfd_df = con.execute(cisfd_query).pl()

# Update ICNO logic
cisfd_df = cisfd_df.with_columns([
    pl.when(pl.col('NEWIC') != '')
    .then(pl.col('NEWIC'))
    .otherwise(pl.col('CUSTNO'))
    .alias('ICNO')
])

print(f"  CISFD records: {len(cisfd_df)}")


# ============================================================================
# STEP 3: Load and process Current Account (CA) data
# ============================================================================

print("\nStep 3: Processing Current Account data...")

ca_df = con.execute(f"SELECT * FROM '{INPUT_CURRENT}' WHERE CURBAL > 0").pl()

# Apply CAPROD format
ca_pd = ca_df.to_pandas()
ca_pd['PRODCD'] = ca_pd['PRODUCT'].apply(apply_caprod_format)
ca_df = pl.from_pandas(ca_pd)

# Filter out PRODCD = 'N'
ca_df = ca_df.filter(pl.col('PRODCD') != 'N')

print(f"  CA records: {len(ca_df)}")

# Merge with CISCA
ca_df = ca_df.join(cisca_df, on='ACCTNO', how='left', suffix='_cis')

# Update CUSTNAME if blank
ca_df = ca_df.with_columns([
    pl.when(pl.col('CUSTNAME').is_null() | (pl.col('CUSTNAME') == '   '))
    .then(pl.col('NAME'))
    .otherwise(pl.col('CUSTNAME'))
    .alias('CUSTNAME')
])

# Add CABAL column
ca_df = ca_df.with_columns([pl.col('CURBAL').alias('CABAL')])

# Split into Individual and Organizational
caind_df = ca_df.filter(pl.col('CUSTCODE').is_in([77, 78, 95, 96]))

caorg_df = ca_df.filter(
    ~pl.col('CUSTCODE').is_in([77, 78, 95, 96]) &
    (pl.col('PURPOSE') != '2') &
    (pl.col('INDORG') == 'O')
)

# Handle JOINT accounts for CAIND
caind_pd = caind_df.to_pandas()
mask_joint = caind_pd['PURPOSE'] == '2'
caind_pd.loc[mask_joint, 'ICNO'] = 'JOINT'
caind_pd.loc[mask_joint, 'CUSTNAME'] = caind_pd.loc[mask_joint, 'NAME']
caind_df = pl.from_pandas(caind_pd).unique(subset=['ACCTNO', 'ICNO', 'CUSTNAME'])

print(f"  CAIND records: {len(caind_df)}")
print(f"  CAORG records: {len(caorg_df)}")


# ============================================================================
# STEP 4: Load and process Fixed Deposit (FD) data
# ============================================================================

print("\nStep 4: Processing Fixed Deposit data...")

fd_df = con.execute(f"SELECT * FROM '{INPUT_FD}' WHERE CURBAL > 0").pl()

print(f"  FD records: {len(fd_df)}")

# Merge with CISFD
fd_df = fd_df.join(cisfd_df, on='ACCTNO', how='left', suffix='_cis')

# Update CUSTNAME if blank
fd_df = fd_df.with_columns([
    pl.when(pl.col('CUSTNAME').is_null() | (pl.col('CUSTNAME') == '   '))
    .then(pl.col('NAME'))
    .otherwise(pl.col('CUSTNAME'))
    .alias('CUSTNAME')
])

# Add FDBAL column
fd_df = fd_df.with_columns([pl.col('CURBAL').alias('FDBAL')])

# Split into Individual and Organizational
fdind_df = fd_df.filter(pl.col('CUSTCODE').is_in([77, 78, 95, 96]))

fdorg_df = fd_df.filter(
    ~pl.col('CUSTCODE').is_in([77, 78, 95, 96]) &
    (pl.col('PURPOSE') != '2') &
    (pl.col('INDORG') == 'O')
)

# Handle JOINT accounts for FDIND
fdind_pd = fdind_df.to_pandas()
mask_joint = fdind_pd['PURPOSE'] == '2'
fdind_pd.loc[mask_joint, 'ICNO'] = 'JOINT'
fdind_pd.loc[mask_joint, 'CUSTNAME'] = fdind_pd.loc[mask_joint, 'NAME']
fdind_df = pl.from_pandas(fdind_pd).unique(subset=['ACCTNO', 'ICNO', 'CUSTNAME'])

print(f"  FDIND records: {len(fdind_df)}")
print(f"  FDORG records: {len(fdorg_df)}")


# ============================================================================
# STEP 5: Load and process Savings Account (SA) data
# ============================================================================

print("\nStep 5: Processing Savings Account data...")

sa_df = con.execute(f"SELECT * FROM '{INPUT_SAVING}' WHERE CURBAL > 0").pl()

# Apply SAPROD format
sa_pd = sa_df.to_pandas()
sa_pd['PRODCD'] = sa_pd['PRODUCT'].apply(apply_saprod_format)
sa_df = pl.from_pandas(sa_pd)

# Filter out PRODCD = 'N'
sa_df = sa_df.filter(pl.col('PRODCD') != 'N')

print(f"  SA records: {len(sa_df)}")

# Merge with CISFD
sa_df = sa_df.join(cisfd_df, on='ACCTNO', how='left', suffix='_cis')

# Update CUSTNAME if blank
sa_df = sa_df.with_columns([
    pl.when(pl.col('CUSTNAME').is_null() | (pl.col('CUSTNAME') == '   '))
    .then(pl.col('NAME'))
    .otherwise(pl.col('CUSTNAME'))
    .alias('CUSTNAME')
])

# Add SABAL column
sa_df = sa_df.with_columns([pl.col('CURBAL').alias('SABAL')])

# Split into Individual and Organizational
saind_df = sa_df.filter(pl.col('CUSTCODE').is_in([77, 78, 95, 96]))

saorg_df = sa_df.filter(
    ~pl.col('CUSTCODE').is_in([77, 78, 95, 96]) &
    (pl.col('PURPOSE') != '2') &
    (pl.col('INDORG') == 'O')
)

# Handle JOINT accounts for SAIND
saind_pd = saind_df.to_pandas()
mask_joint = saind_pd['PURPOSE'] == '2'
saind_pd.loc[mask_joint, 'ICNO'] = 'JOINT'
saind_pd.loc[mask_joint, 'CUSTNAME'] = saind_pd.loc[mask_joint, 'NAME']
saind_df = pl.from_pandas(saind_pd).unique(subset=['ACCTNO', 'ICNO', 'CUSTNAME'])

print(f"  SAIND records: {len(saind_df)}")
print(f"  SAORG records: {len(saorg_df)}")


# ============================================================================
# HELPER FUNCTIONS FOR REPORT GENERATION
# ============================================================================

def prepare_combined_data(fdind, caind, saind, fdorg, caorg, saorg, customer_type='IND'):
    """
    Combine FD, CA, SA data and prepare for reporting
    customer_type: 'IND' for individual, 'ORG' for corporate
    """
    if customer_type == 'IND':
        datasets = [fdind, caind, saind]
    else:
        datasets = [fdorg, caorg, saorg]

    # Ensure all datasets have required columns
    combined_list = []
    for df in datasets:
        if len(df) > 0:
            # Add missing balance columns with 0
            if 'FDBAL' not in df.columns:
                df = df.with_columns(pl.lit(0.0).alias('FDBAL'))
            if 'CABAL' not in df.columns:
                df = df.with_columns(pl.lit(0.0).alias('CABAL'))
            if 'SABAL' not in df.columns:
                df = df.with_columns(pl.lit(0.0).alias('SABAL'))
            combined_list.append(df)

    if not combined_list:
        return pl.DataFrame()

    data1 = pl.concat(combined_list)

    # Replace blank ICNO with 'XX'
    data1 = data1.with_columns([
        pl.when((pl.col('ICNO').is_null()) | (pl.col('ICNO') == '  '))
        .then(pl.lit('XX'))
        .otherwise(pl.col('ICNO'))
        .alias('ICNO')
    ])

    return data1


def generate_top100_summary(data1):
    """Generate top 100 summary by ICNO and CUSTNAME"""
    if len(data1) == 0:
        return pl.DataFrame(), pl.DataFrame()

    # Filter non-blank ICNO
    data1 = data1.filter((pl.col('ICNO') != '') & (pl.col('ICNO').is_not_null()))

    # Group by ICNO and CUSTNAME
    data2 = data1.group_by(['ICNO', 'CUSTNAME']).agg([
        pl.col('CURBAL').sum().alias('CURBAL'),
        pl.col('FDBAL').sum().alias('FDBAL'),
        pl.col('CABAL').sum().alias('CABAL'),
        pl.col('SABAL').sum().alias('SABAL')
    ])

    # Sort by CURBAL descending and take top 100
    data2 = data2.sort('CURBAL', descending=True).head(100)

    # Get list of ICNO/CUSTNAME combinations in top 100
    top100_ids = data2.select(['ICNO', 'CUSTNAME'])

    # Join back to get detailed records
    data3 = data1.join(top100_ids, on=['ICNO', 'CUSTNAME'], how='inner')
    data3 = data3.sort(['ICNO', 'CUSTNAME'])

    return data2, data3


def write_report_header(f, title1, title2):
    """Write report header with ASA carriage control"""
    f.write(f"{ASA_NEW_PAGE}{title1}\n")
    f.write(f"{ASA_SPACE}{title2}\n")
    f.write(f"{ASA_SPACE}\n")


def format_number(value, decimals=2):
    """Format number with commas"""
    if value is None or (isinstance(value, float) and value != value):  # NaN check
        return f"{'0.00':>16}"
    return f"{value:>16,.{decimals}f}"


def write_summary_report(data2, output_file, title1, title2):
    """Write top 100 summary report"""
    with open(output_file, 'w') as f:
        write_report_header(f, title1, title2)

        # Column headers
        header = f"{ASA_SPACE}{'DEPOSITOR':<50} {'TOTAL BALANCE':>16} {'FD BALANCE':>16} {'CA BALANCE':>16} {'SA BALANCE':>16}\n"
        f.write(header)
        f.write(f"{ASA_SPACE}{'-' * 114}\n")

        # Data rows
        data2_pd = data2.to_pandas()
        for idx, row in data2_pd.iterrows():
            custname = str(row['CUSTNAME'])[:50].ljust(50)
            curbal = format_number(row['CURBAL'])
            fdbal = format_number(row['FDBAL'])
            cabal = format_number(row['CABAL'])
            sabal = format_number(row['SABAL'])

            line = f"{ASA_SPACE}{custname}{curbal}{fdbal}{cabal}{sabal}\n"
            f.write(line)


def write_detail_report(data3, output_file, title1, title2, append=False):
    """Write detailed account report"""
    mode = 'a' if append else 'w'

    with open(output_file, mode) as f:
        if not append:
            write_report_header(f, title1, title2)

        data3_pd = data3.to_pandas()

        current_icno = None
        current_custname = None
        group_total = 0

        for idx, row in data3_pd.iterrows():
            icno = row['ICNO']
            custname = row['CUSTNAME']

            # Check if new group
            if current_icno != icno or current_custname != custname:
                # Print previous group total
                if current_icno is not None:
                    f.write(f"{ASA_SPACE}{' ' * 90}{'CURBAL':>10} {format_number(group_total)}\n")
                    f.write(f"{ASA_SPACE}\n")

                # New group
                current_icno = icno
                current_custname = custname
                group_total = 0

                # Print group header
                f.write(f"{ASA_SPACE}ICNO={icno}  CUSTNAME={custname}\n")
                f.write(f"{ASA_SPACE}\n")

                # Column headers
                header = f"{ASA_SPACE}{'BRANCH':>6} {'MNI NO':>12} {'CUSTCD':>6} {'DEPOSITOR':<30} {'CIS NO':>10} {'NEW IC':<15} {'OLD IC':<15} {'CURRENT':>16} {'PRODUCT':>8}\n"
                f.write(header)
                header2 = f"{ASA_SPACE}{'CODE':>6} {' ':>12} {' ':>6} {' ':<30} {' ':>10} {' ':<15} {' ':<15} {'BALANCE':>16} {' ':>8}\n"
                f.write(header2)
                f.write(f"{ASA_SPACE}{'-' * 130}\n")

            # Print detail line
            branch = str(int(row['BRANCH'])) if row['BRANCH'] else ''
            acctno = str(int(row['ACCTNO'])) if row['ACCTNO'] else ''
            custcode = str(int(row['CUSTCODE'])) if row['CUSTCODE'] else ''
            custname_trunc = str(row['CUSTNAME'])[:30].ljust(30)
            custno = str(row['CUSTNO']) if row['CUSTNO'] else ''
            newic = str(row['NEWIC'])[:15].ljust(15) if row['NEWIC'] else ' ' * 15
            oldic = str(row['OLDIC'])[:15].ljust(15) if row['OLDIC'] else ' ' * 15
            curbal = row['CURBAL']
            product = str(int(row['PRODUCT'])) if row['PRODUCT'] else ''

            line = f"{ASA_SPACE}{branch:>6} {acctno:>12} {custcode:>6} {custname_trunc} {custno:>10} {newic} {oldic} {format_number(curbal)} {product:>8}\n"
            f.write(line)

            group_total += curbal

        # Print last group total
        if current_icno is not None:
            f.write(f"{ASA_SPACE}{' ' * 90}{'CURBAL':>10} {format_number(group_total)}\n")


# ============================================================================
# REPORT 1: TOP 100 INDIVIDUAL CUSTOMERS
# ============================================================================

print("\n" + "=" * 80)
print("GENERATING INDIVIDUAL CUSTOMERS REPORT")
print("=" * 80)

data1_ind = prepare_combined_data(fdind_df, caind_df, saind_df, None, None, None, 'IND')
print(f"Combined individual records: {len(data1_ind)}")

data2_ind, data3_ind = generate_top100_summary(data1_ind)
print(f"Top 100 individual depositors: {len(data2_ind)}")
print(f"Detailed individual records: {len(data3_ind)}")

title1 = 'PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTP50'
title2 = f'TOP 100 LARGEST FD/CA/SA INDIVIDUAL CUSTOMERS AS AT {RDATE}'

write_summary_report(data2_ind, OUTPUT_FD11TEXT, title1, title2)
write_detail_report(data3_ind, OUTPUT_FD11TEXT, title1, title2, append=True)

print(f"Individual customers report generated: {OUTPUT_FD11TEXT}")


# ============================================================================
# REPORT 2: TOP 100 CORPORATE CUSTOMERS
# ============================================================================

print("\n" + "=" * 80)
print("GENERATING CORPORATE CUSTOMERS REPORT")
print("=" * 80)

data1_org = prepare_combined_data(None, None, None, fdorg_df, caorg_df, saorg_df, 'ORG')
print(f"Combined corporate records: {len(data1_org)}")

data2_org, data3_org = generate_top100_summary(data1_org)
print(f"Top 100 corporate depositors: {len(data2_org)}")
print(f"Detailed corporate records: {len(data3_org)}")

title1 = 'PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTP50'
title2 = f'TOP 100 LARGEST FD/CA/SA CORPORATE CUSTOMERS AS AT {RDATE}'

write_summary_report(data2_org, OUTPUT_FD12TEXT, title1, title2)
write_detail_report(data3_org, OUTPUT_FD12TEXT, title1, title2, append=True)

print(f"Corporate customers report generated: {OUTPUT_FD12TEXT}")


# ============================================================================
# REPORT 3: SUBSIDIARIES UNDER TOP CORPORATE DEPOSITORS
# ============================================================================

print("\n" + "=" * 80)
print("GENERATING SUBSIDIARIES REPORT")
print("=" * 80)

# Combine all organizational data
subs_all = prepare_combined_data(None, None, None, fdorg_df, caorg_df, saorg_df, 'ORG')

if len(subs_all) > 0:
    # Add DEPID and DEPGRP columns
    subs_all = subs_all.with_columns([
        pl.lit(0).cast(pl.Int64).alias('DEPID'),
        pl.lit('').alias('DEPGRP')
    ])

    # Update ICNO logic
    subs_all = subs_all.with_columns([
        pl.when(pl.col('NEWIC') != '')
        .then(pl.col('NEWIC'))
        .otherwise(pl.col('OLDIC'))
        .alias('ICNO')
    ])

    # Add RMAMT and FCYAMT
    subs_all = subs_all.with_columns([
        pl.when(pl.col('CURCODE') == 'MYR')
        .then(pl.col('CURBAL'))
        .otherwise(0.0)
        .alias('RMAMT'),

        pl.when(pl.col('CURCODE') != 'MYR')
        .then(pl.col('CURBAL'))
        .otherwise(0.0)
        .alias('FCYAMT')
    ])

    print(f"Subsidiaries all records: {len(subs_all)}")

    # Load COF_MNI_DEPOSITOR_LIST
    try:
        cof_mni_df = con.execute(f"SELECT * FROM '{INPUT_COF_MNI_LIST}'").pl()

        # First join: by BUSSREG (renamed to NEWIC)
        cof_mni_idno = cof_mni_df.select(['DEPID', 'DEPGRP', 'BUSSREG']).unique(subset=['BUSSREG'])
        cof_mni_idno = cof_mni_idno.rename({'BUSSREG': 'NEWIC'})

        mni_ic = subs_all.join(cof_mni_idno, on='NEWIC', how='left', suffix='_mni')

        # Update DEPID and DEPGRP from join
        mni_ic = mni_ic.with_columns([
            pl.when(pl.col('DEPID_mni').is_not_null())
            .then(pl.col('DEPID_mni'))
            .otherwise(pl.col('DEPID'))
            .alias('DEPID'),

            pl.when(pl.col('DEPGRP_mni').is_not_null())
            .then(pl.col('DEPGRP_mni'))
            .otherwise(pl.col('DEPGRP'))
            .alias('DEPGRP')
        ])

        # Drop join columns
        if 'DEPID_mni' in mni_ic.columns:
            mni_ic = mni_ic.drop(['DEPID_mni', 'DEPGRP_mni'])

        # Separate matched and unmatched
        mni_ic_matched = mni_ic.filter(pl.col('DEPID') > 0)
        mni_icx = mni_ic.filter(pl.col('DEPID') == 0)

        print(f"  Matched by NEWIC: {len(mni_ic_matched)}")
        print(f"  Unmatched by NEWIC: {len(mni_icx)}")

        # Second join: by CUSTNO
        cof_mni_cust = cof_mni_df.select(['DEPID', 'DEPGRP', 'CUSTNO']).unique(subset=['CUSTNO'])

        mni_cust = mni_icx.join(cof_mni_cust, on='CUSTNO', how='left', suffix='_mni')

        # Update DEPID and DEPGRP from join
        mni_cust = mni_cust.with_columns([
            pl.when(pl.col('DEPID_mni').is_not_null())
            .then(pl.col('DEPID_mni'))
            .otherwise(pl.col('DEPID'))
            .alias('DEPID'),

            pl.when(pl.col('DEPGRP_mni').is_not_null())
            .then(pl.col('DEPGRP_mni'))
            .otherwise(pl.col('DEPGRP'))
            .alias('DEPGRP')
        ])

        # Drop join columns
        if 'DEPID_mni' in mni_cust.columns:
            mni_cust = mni_cust.drop(['DEPID_mni', 'DEPGRP_mni'])

        mni_cust_matched = mni_cust.filter(pl.col('DEPID') > 0)

        print(f"  Matched by CUSTNO: {len(mni_cust_matched)}")

        # Combine all matched records
        mni_all = pl.concat([mni_ic_matched, mni_cust_matched])
        mni_all = mni_all.sort('CUSTNO')

        print(f"  Total MNI matches: {len(mni_all)}")

        # Load exclusion list
        try:
            topdep = con.execute(f"SELECT * FROM '{INPUT_TOPDEP_EXCL}'").pl()
            topdep = topdep.sort('CUSTNO')

            # Anti-join: keep records NOT in exclusion list
            subs_all = mni_all.join(topdep, on='CUSTNO', how='anti')

            print(f"  After exclusions: {len(subs_all)}")

        except Exception as e:
            print(f"  Warning: Could not load exclusion list: {e}")
            subs_all = mni_all

        # Get min and max DEPID
        if len(subs_all) > 0:
            depid_stats = subs_all.select([
                pl.col('DEPID').min().alias('MIN_ID'),
                pl.col('DEPID').max().alias('MAX_ID')
            ])

            MIN_ID = depid_stats['MIN_ID'][0]
            MAX_ID = depid_stats['MAX_ID'][0]

            print(f"  DEPID range: {MIN_ID} to {MAX_ID}")

            # Sort for output
            subs_all = subs_all.sort(['CUSTNO', 'ACCTNO'])

            # Generate subsidiary reports
            with open(OUTPUT_FD2TEXT, 'w') as f:
                for depid in range(MIN_ID, MAX_ID + 1):
                    subs_group = subs_all.filter(pl.col('DEPID') == depid)

                    if len(subs_group) == 0:
                        continue

                    # Get group name
                    group_name = subs_group['DEPGRP'][0]

                    # Write report for this group
                    title1 = 'PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTOP5'
                    title2 = f'GROUP OF COMPANIES UNDER TOP 100 CORP DEPOSITORS @{RDATE}'
                    title3 = f'***** {group_name} *****'

                    if depid == MIN_ID:
                        f.write(f"{ASA_NEW_PAGE}{title1}\n")
                    else:
                        f.write(f"{ASA_NEW_PAGE}{title1}\n")

                    f.write(f"{ASA_SPACE}{title2}\n")
                    f.write(f"{ASA_SPACE}{title3}\n")
                    f.write(f"{ASA_SPACE}\n")

                    # Column headers
                    header = f"{ASA_SPACE}{'BRANCH':>6} {'MNI NO':>12} {'DEPOSITOR':<30} {'CIS NO':>10} {'CUSTCD':>6} {'CURRENT':>16} {'PRODUCT':>8}\n"
                    f.write(header)
                    header2 = f"{ASA_SPACE}{'CODE':>6} {' ':>12} {' ':<30} {' ':>10} {' ':>6} {'BALANCE':>16} {' ':>8}\n"
                    f.write(header2)
                    f.write(f"{ASA_SPACE}{'-' * 100}\n")

                    # Data rows
                    subs_pd = subs_group.to_pandas()
                    current_custno = None
                    group_total = 0

                    for idx, row in subs_pd.iterrows():
                        custno = row['CUSTNO']

                        # Check if new CUSTNO group
                        if current_custno != custno:
                            # Print previous group total
                            if current_custno is not None:
                                f.write(f"{ASA_SPACE}{' ' * 60}{'CURBAL':>10} {format_number(group_total)}\n")
                                f.write(f"{ASA_SPACE}\n")

                            current_custno = custno
                            group_total = 0

                            f.write(f"{ASA_SPACE}CUSTNO={custno}\n")

                        # Print detail line
                        branch = str(int(row['BRANCH'])) if row['BRANCH'] else ''
                        acctno = str(int(row['ACCTNO'])) if row['ACCTNO'] else ''
                        custname = str(row['CUSTNAME'])[:30].ljust(30)
                        custno_str = str(row['CUSTNO']) if row['CUSTNO'] else ''
                        custcode = str(int(row['CUSTCODE'])) if row['CUSTCODE'] else ''
                        curbal = row['CURBAL']
                        product = str(int(row['PRODUCT'])) if row['PRODUCT'] else ''

                        line = f"{ASA_SPACE}{branch:>6} {acctno:>12} {custname} {custno_str:>10} {custcode:>6} {format_number(curbal)} {product:>8}\n"
                        f.write(line)

                        group_total += curbal

                    # Print last group total
                    if current_custno is not None:
                        f.write(f"{ASA_SPACE}{' ' * 60}{'CURBAL':>10} {format_number(group_total)}\n")

            print(f"Subsidiaries report generated: {OUTPUT_FD2TEXT}")
        else:
            print("  No subsidiaries data to report")

    except Exception as e:
        print(f"Warning: Could not process subsidiaries: {e}")
        import traceback

        traceback.print_exc()
else:
    print("  No organizational data for subsidiaries report")


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 80)
print("CONVERSION COMPLETE")
print("=" * 80)
print(f"\nReports generated:")
print(f"  1. Individual Customers: {OUTPUT_FD11TEXT}")
print(f"  2. Corporate Customers: {OUTPUT_FD12TEXT}")
print(f"  3. Subsidiaries: {OUTPUT_FD2TEXT}")
print(f"\nReporting Date: {RDATE}")
