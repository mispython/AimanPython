#!/usr/bin/env python3
"""
File Name: EIBDLIBT
BNM Liquidity Report for Trade Finance
Processes BA (Banker's Acceptance) and TR (Trade) transactions
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import calendar

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# INPUT_DIR = Path("/mnt/user-data/uploads")
# OUTPUT_DIR = Path("/mnt/user-data/outputs")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Input files
BNM1_FILE = INPUT_DIR / "sap_bt_sasdata_daily_0.parquet"
PBA01_FILE = INPUT_DIR / "sap_pbb_pba_daily_0.parquet"
BTDTL_FILE = INPUT_DIR / "sap_bt_sasdata_daily_btdtl.parquet"

# Output file
OUTPUT_FILE = OUTPUT_DIR / "bt.parquet"

# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect()


# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

def get_remfmt(remmth):
    """Format remaining months into BNM codes"""
    if remmth is None:
        return '07'  # MISSING
    elif remmth <= 0.1:
        return '01'  # UP TO 1 WK
    elif remmth <= 1:
        return '02'  # >1 WK - 1 MTH
    elif remmth <= 3:
        return '03'  # >1 MTH - 3 MTHS
    elif remmth <= 6:
        return '04'  # >3 - 6 MTHS
    elif remmth <= 12:
        return '05'  # >6 MTHS - 1 YR
    else:
        return '06'  # > 1 YEAR


# Product format mapping
PRODUCT_HL = [4, 5, 6, 7, 31, 32, 100, 101, 102, 103, 110, 111, 112, 113, 114, 115,
              116, 170, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
              225, 226, 227, 228, 229, 230, 231, 232, 233, 234]
PRODUCT_RC = [350, 910, 925]


def get_prod_type(product):
    """Get product type from product code"""
    if product in PRODUCT_HL:
        return 'HL'
    elif product in PRODUCT_RC:
        return 'RC'
    else:
        return 'FL'


# ============================================================================
# STEP 1: READ REPTDATE AND SET VARIABLES
# ============================================================================
print("Step 1: Reading report date...")

reptdate_df = pl.read_parquet(BNM1_FILE).select(['REPTDATE']).head(1)
reptdate = reptdate_df['REPTDATE'][0]

# Set macro variables
REPTYEAR = f"{reptdate.year % 100:02d}"
REPTMON = f"{reptdate.month:02d}"
REPTDAY = f"{reptdate.day:02d}"
RDATE = reptdate.strftime('%d/%m/%y')

# Determine week number
if reptdate.day == 8:
    NOWK = '1'
elif reptdate.day == 15:
    NOWK = '2'
elif reptdate.day == 22:
    NOWK = '3'
else:
    NOWK = '4'

print(f"Report Date: {reptdate}")

# Update file paths with date suffix
BTDTL_FILE = INPUT_DIR / f"sap_bt_sasdata_daily_btdtl{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
PBA01_FILE = INPUT_DIR / f"sap_pbb_pba_daily_0_pba01{REPTYEAR}{REPTMON}{REPTDAY}.parquet"


# ============================================================================
# STEP 2: CALCULATE RUNOFF DATE
# ============================================================================
print("Step 2: Calculating runoff date...")

rpyr = reptdate.year
rpmth = reptdate.month
rpday = reptdate.day

# Get last day of report month
last_day = calendar.monthrange(rpyr, rpmth)[1]
runoff_date = datetime(rpyr, rpmth, last_day).date()
RUNOFFDT = (runoff_date - datetime(1960, 1, 1).date()).days  # SAS date

print(f"Runoff Date: {runoff_date} (SAS: {RUNOFFDT})")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def calculate_next_bldate(bldate, issdte, payfreq, freq, reptdate):
    """Calculate next billing date"""
    if bldate is None or bldate <= 0:
        return None

    # Convert SAS date to Python date
    base_date = datetime(1960, 1, 1).date()
    bl_date = base_date + timedelta(days=int(bldate))

    if payfreq == '6':
        # Fortnightly - add 14 days
        next_date = bl_date + timedelta(days=14)
    else:
        # Use frequency in months
        issue_date = base_date + timedelta(days=int(issdte)) if issdte and issdte > 0 else bl_date

        mm = bl_date.month + freq
        yy = bl_date.year
        dd = issue_date.day

        if mm > 12:
            mm = mm - 12
            yy = yy + 1

        # Get last day of target month
        last_day = calendar.monthrange(yy, mm)[1]
        if dd > last_day:
            dd = last_day

        next_date = datetime(yy, mm, dd).date()

    # Convert back to SAS date
    return (next_date - base_date).days


def calculate_remmth(matdate, runoff_date, rpyr, rpmth, rpday):
    """Calculate remaining months to maturity"""
    if matdate is None:
        return None

    # Convert to datetime if needed
    if isinstance(matdate, (int, float)):
        base_date = datetime(1960, 1, 1).date()
        matdate = base_date + timedelta(days=int(matdate))

    if isinstance(matdate, datetime):
        matdate = matdate.date()

    # Get days in report month
    rpdays = calendar.monthrange(rpyr, rpmth)[1]

    mdyr = matdate.year
    mdmth = matdate.month
    mdday = matdate.day

    # Adjust day if needed
    if mdday > rpdays:
        mdday = rpdays

    # Calculate difference
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    remmth = remy * 12 + remm + remd / rpdays

    return remmth


# ============================================================================
# STEP 3: PROCESS BTDTL DATA
# ============================================================================
print("Step 3: Processing BTDTL data...")

btdtl_data = pl.read_parquet(BTDTL_FILE).filter(
    (pl.col('ISSDTE') > 0) | (pl.col('EXPRDATE') > 0)
).select(['TRANSREF', 'ISSDTE', 'EXPRDATE', 'PAYAMT'])

# Sort and keep first per TRANSREF (descending ISSDTE)
btdtl_data = btdtl_data.sort(['TRANSREF', 'ISSDTE'], descending=[False, True]).unique(
    subset=['TRANSREF'], keep='first'
)


# ============================================================================
# STEP 4: PROCESS PBA DATA (BANKER'S ACCEPTANCE)
# ============================================================================
print("Step 4: Processing PBA data...")

pba_data = pl.read_parquet(PBA01_FILE)

# Extract TRANSREF (skip first character)
pba_data = pba_data.with_columns([
    pl.col('TRANSREF').str.slice(1, 8).alias('TRANSREF')
])

# Merge PBA with BTDTL
ba_data = pba_data.join(btdtl_data, on='TRANSREF', how='left')


# ============================================================================
# STEP 5: PROCESS BA TRANSACTIONS
# ============================================================================
print("Step 5: Processing BA transactions...")

ba_records = []

for row in ba_data.iter_rows(named=True):
    balance = (row.get('FCVALUE', 0) or 0) - (row.get('UNEARNED', 0) or 0)
    custcd = row.get('CUSTCD', '')
    product = row.get('PRODUCT', 0)
    bldate = row.get('BLDATE', 0)
    issdte = row.get('ISSDTE', 0)
    exprdate = row.get('EXPRDATE', 0)
    payamt = row.get('PAYAMT', 0)

    # Determine customer type
    cust = '08' if custcd in ['77', '78', '95', '96'] else '09'

    # Determine product type
    prod = 'BT'

    # Determine item code
    if custcd in ['77', '78', '95', '96']:
        item = '219'  # Default for BT
    else:
        if prod == 'FL':
            item = '211'
        elif prod == 'RC':
            item = '212'
        else:
            item = '219'

    # Hardcode override
    if product == 100:
        item = '212'

    # Calculate days
    days = 0
    if bldate and bldate > 0:
        days = (reptdate - datetime(1960, 1, 1).date()).days - bldate

    # Initialize variables
    current_balance = balance
    current_bldate = bldate

    # Process maturity profile
    if exprdate and exprdate <= RUNOFFDT:
        remmth = None
    elif exprdate and (exprdate - RUNOFFDT < 8):
        remmth = 0.1
    else:
        # Payment frequency
        payfreq = '3'
        if payfreq == '1':
            freq = 1
        elif payfreq == '2':
            freq = 3
        elif payfreq == '3':
            freq = 6
        elif payfreq == '4':
            freq = 12
        else:
            freq = 6

        # Initialize bldate if needed
        if current_bldate is None or current_bldate <= 0:
            current_bldate = issdte
            # Advance bldate to after reptdate
            reptdate_sas = (reptdate - datetime(1960, 1, 1).date()).days
            while current_bldate and current_bldate <= reptdate_sas:
                current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, freq, reptdate)
                if current_bldate is None:
                    break

        if payamt and payamt < 0:
            payamt = 0

        # Check if we should use expiry date
        if current_bldate and exprdate and (current_bldate > exprdate or current_balance <= (payamt or 0)):
            current_bldate = exprdate

        # Process payment schedule
        while current_bldate and exprdate and current_bldate <= exprdate:
            if current_bldate <= RUNOFFDT:
                remmth = None
            elif current_bldate - RUNOFFDT < 8:
                remmth = 0.1
            else:
                remmth = calculate_remmth(current_bldate, runoff_date, rpyr, rpmth, rpday)

            # Check if we should exit loop
            if remmth and remmth > 1:
                break
            if current_bldate == exprdate:
                break

            # Create output records for payment
            if payamt and payamt > 0 and remmth is not None:
                amount = payamt
                current_balance -= payamt

                # Part 2-RM record
                bnmcode = f"95{item}{cust}{get_remfmt(remmth)}0000Y"
                ba_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

                # Part 1-RM record (NPL if days > 89)
                remmth_npl = 13 if days > 89 else remmth
                bnmcode = f"93{item}{cust}{get_remfmt(remmth_npl)}0000Y"
                ba_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

            # Calculate next bldate
            current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, freq, reptdate)

            if current_bldate and exprdate and (current_bldate > exprdate or current_balance <= (payamt or 0)):
                current_bldate = exprdate

        # Calculate final remmth for remaining balance
        if current_bldate and current_bldate <= RUNOFFDT:
            remmth = None
        elif current_bldate and (current_bldate - RUNOFFDT < 8):
            remmth = 0.1
        else:
            remmth = calculate_remmth(current_bldate if current_bldate else exprdate, runoff_date, rpyr, rpmth, rpday)

    # Output remaining balance
    amount = current_balance
    if amount and amount != 0:
        # Part 2-RM record
        bnmcode = f"95{item}{cust}{get_remfmt(remmth)}0000Y"
        ba_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

        # Part 1-RM record (NPL if days > 89)
        remmth_npl = 13 if days > 89 else remmth
        bnmcode = f"93{item}{cust}{get_remfmt(remmth_npl)}0000Y"
        ba_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

ba_df = pl.DataFrame(ba_records) if ba_records else pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

print(f"BA records created: {len(ba_records)}")


# ============================================================================
# STEP 6: PROCESS TR DATA (TRADE TRANSACTIONS)
# ============================================================================
print("Step 6: Processing TR data...")

tr_data = pl.read_parquet(BTDTL_FILE).filter(
    (~pl.col('LIABCODE').is_in(['BAI', 'BAP', 'BAS', 'BAE'])) &
    (pl.col('DIRCTIND') == 'D')
)

tr_records = []

for row in tr_data.iter_rows(named=True):
    outstand = row.get('OUTSTAND', 0) or 0
    custcd = row.get('CUSTCD', '')
    product = row.get('PRODUCT', 0)
    bldate = row.get('BLDATE', 0)
    issdte = row.get('ISSDTE', 0)
    exprdate = row.get('EXPRDATE', 0)
    payamt = row.get('PAYAMT', 0)

    # Determine customer type
    cust = '08' if custcd in ['77', '78', '95', '96'] else '09'

    # Determine product type
    prod = 'BT'

    # Determine item code
    if custcd in ['77', '78', '95', '96']:
        item = '219'
    else:
        if prod == 'FL':
            item = '211'
        elif prod == 'RC':
            item = '212'
        else:
            item = '219'

    # Hardcode override
    if product == 100:
        item = '212'

    # Calculate days
    days = 0
    if bldate and bldate > 0:
        days = (reptdate - datetime(1960, 1, 1).date()).days - bldate

    # Initialize variables
    current_outstand = outstand
    current_bldate = bldate

    # Process maturity profile
    if exprdate and exprdate <= RUNOFFDT:
        remmth = None
    elif exprdate and (exprdate - RUNOFFDT < 8):
        remmth = 0.1
    else:
        # Payment frequency
        payfreq = '3'
        if payfreq == '1':
            freq = 1
        elif payfreq == '2':
            freq = 3
        elif payfreq == '3':
            freq = 6
        elif payfreq == '4':
            freq = 12
        else:
            freq = 6

        # Initialize bldate if needed
        if current_bldate is None or current_bldate <= 0:
            current_bldate = issdte
            reptdate_sas = (reptdate - datetime(1960, 1, 1).date()).days
            while current_bldate and current_bldate <= reptdate_sas:
                current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, freq, reptdate)
                if current_bldate is None:
                    break

        if payamt and payamt < 0:
            payamt = 0

        if current_bldate and exprdate and (current_bldate > exprdate or current_outstand <= (payamt or 0)):
            current_bldate = exprdate

        # Process payment schedule
        while current_bldate and exprdate and current_bldate <= exprdate:
            if current_bldate <= RUNOFFDT:
                remmth = None
            elif current_bldate - RUNOFFDT < 8:
                remmth = 0.1
            else:
                remmth = calculate_remmth(current_bldate, runoff_date, rpyr, rpmth, rpday)

            if remmth and remmth > 1:
                break
            if current_bldate == exprdate:
                break

            # Create output records for payment
            if payamt and payamt > 0 and remmth is not None:
                amount = payamt
                current_outstand -= payamt

                # Part 2-RM record
                bnmcode = f"95{item}{cust}{get_remfmt(remmth)}0000Y"
                tr_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

                # Part 1-RM record (UP TO 1 WK if days > 89)
                remmth_npl = 0.1 if days > 89 else remmth
                bnmcode = f"93{item}{cust}{get_remfmt(remmth_npl)}0000Y"
                tr_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

            current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, freq, reptdate)

            if current_bldate and exprdate and (current_bldate > exprdate or current_outstand <= (payamt or 0)):
                current_bldate = exprdate

        # Calculate final remmth
        if current_bldate and current_bldate <= RUNOFFDT:
            remmth = None
        elif current_bldate and (current_bldate - RUNOFFDT < 8):
            remmth = 0.1
        else:
            remmth = calculate_remmth(current_bldate if current_bldate else exprdate, runoff_date, rpyr, rpmth, rpday)

    # Output remaining outstand
    amount = current_outstand
    if amount and amount != 0:
        # Part 2-RM record
        bnmcode = f"95{item}{cust}{get_remfmt(remmth)}0000Y"
        tr_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

        # Part 1-RM record
        remmth_npl = 0.1 if days > 89 else remmth
        bnmcode = f"93{item}{cust}{get_remfmt(remmth_npl)}0000Y"
        tr_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

tr_df = pl.DataFrame(tr_records) if tr_records else pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

print(f"TR records created: {len(tr_records)}")


# ============================================================================
# STEP 7: COMBINE AND FILTER
# ============================================================================
print("Step 7: Combining data...")

combine_df = pl.concat([ba_df, tr_df])

# Filter out records with missing remmth (code '07')
print(f"\nRecords with MISSING remmth (code '07'):")
missing_df = combine_df.filter(pl.col('BNMCODE').str.slice(7, 2) == '07')
if len(missing_df) > 0:
    print(f"Count: {len(missing_df)}, Sum: {missing_df['AMOUNT'].sum()}")
else:
    print("Count: 0, Sum: 0")

# Keep only records without missing remmth
note_df = combine_df.filter(pl.col('BNMCODE').str.slice(7, 2) != '07')


# ============================================================================
# STEP 8: SUMMARIZE AND OUTPUT
# ============================================================================
print("\nStep 8: Summarizing data...")

bnm_df = note_df.group_by('BNMCODE').agg([
    pl.col('AMOUNT').sum().alias('AMOUNT')
])

# Sort by BNMCODE
bnm_df = bnm_df.sort('BNMCODE')

# Write output
bnm_df.write_parquet(OUTPUT_FILE)

print(f"\nOutput written to: {OUTPUT_FILE}")
print(f"Total records: {len(bnm_df)}")
print(f"Total amount: {bnm_df['AMOUNT'].sum():.2f}")

# Display summary
print("\nSummary by BNMCODE:")
for row in bnm_df.iter_rows(named=True):
    print(f"  {row['BNMCODE']}: {row['AMOUNT']:,.2f}")

print("\nProcessing complete!")

# Close DuckDB connection
con.close()
