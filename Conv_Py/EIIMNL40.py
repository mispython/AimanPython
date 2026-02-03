#!/usr/bin/env python3
"""
File Name: EIIMNL40
BNM Liquidity Framework - Loan Processing (Simplified)
Processes loan data for maturity profile reporting
Converted from SAS to Python
"""

import duckdb
import polars as pl
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
import calendar


# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Foreign currency product codes
FCY = [800, 801, 802, 803, 804, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860]

# Input paths
LOAN_REPTDATE_PATH = "/data/input/loan_reptdate.parquet"
BNM1_LOAN_PATH = "/data/input/bnm1_loan_{month}{week}.parquet"

# Output paths
OUTPUT_DIR = "/data/output"
BNM_NOTE_PATH = f"{OUTPUT_DIR}/bnm_note.parquet"
REPORT_FCY_PATH = f"{OUTPUT_DIR}/fcy_loans_report.txt"
REPORT_NOTE_PATH = f"{OUTPUT_DIR}/bnm_note_report.txt"

# Create output directory
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_month_last_day(year, month):
    """Get last day of a specific month"""
    return calendar.monthrange(year, month)[1]


def classify_remmth(remmth):
    """Classify remaining months into BNM categories"""
    if remmth < 0.1:
        return '01'  # UP TO 1 WK
    elif remmth < 1:
        return '02'  # >1 WK - 1 MTH
    elif remmth < 3:
        return '03'  # >1 MTH - 3 MTHS
    elif remmth < 6:
        return '04'  # >3 - 6 MTHS
    elif remmth < 12:
        return '05'  # >6 MTHS - 1 YR
    else:
        return '06'  # > 1 YEAR


# ============================================================================
# LIQUIDITY PRODUCT FORMAT MAPPING
# ============================================================================

LIQP_FORMAT_MAP = {
    # Housing Loans
    100: 'HL', 101: 'HL', 102: 'HL', 103: 'HL', 104: 'HL', 105: 'HL',
    106: 'HL', 107: 'HL', 108: 'HL', 109: 'HL', 110: 'HL',
    # Fixed Loans
    200: 'FL', 201: 'FL', 202: 'FL', 203: 'FL', 204: 'FL', 205: 'FL',
    # Revolving Credits
    300: 'RC', 301: 'RC', 302: 'RC',
    # Add more mappings as needed
}


def get_liq_product(product_code):
    """Get liquidity product type"""
    return LIQP_FORMAT_MAP.get(product_code, 'OT')


# ============================================================================
# DATE PROCESSING
# ============================================================================

def get_report_dates():
    """Get report dates and calculate week numbers"""
    # Load LOAN.REPTDATE
    loan_reptdate_df = pl.read_parquet(LOAN_REPTDATE_PATH)
    reptdate = loan_reptdate_df.select('REPTDATE').item(0, 0)

    if isinstance(reptdate, str):
        from datetime import datetime
        reptdate = datetime.strptime(reptdate, '%Y-%m-%d').date()

    day = reptdate.day

    # Determine week number
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
REPTDATE = dates['reptdate']
NOWK = dates['nowk']
REPTYEAR = dates['reptyear']
REPTMON = dates['reptmon']
REPTDAY = dates['reptday']
RDATE = dates['rdate']

print(f"Report Date: {RDATE}")
print(f"Week Number: {NOWK}")
print(f"Report Year: {REPTYEAR}, Month: {REPTMON}")


# ============================================================================
# HELPER FUNCTIONS FOR LOAN PROCESSING
# ============================================================================

def calculate_next_bldate(bldate, issdte, payfreq, freq):
    """Calculate next billing date"""
    if payfreq == '6':
        # Bi-weekly
        return bldate + timedelta(days=14)
    else:
        # Add freq months
        dd = issdte.day
        next_date = bldate + relativedelta(months=freq)

        # Adjust for month-end
        last_day = get_month_last_day(next_date.year, next_date.month)
        if dd > last_day:
            dd = last_day

        return date(next_date.year, next_date.month, dd)


def calculate_remaining_months(matdt, reptdate):
    """Calculate remaining months from report date to maturity date"""
    # Get run-off date components
    rpyr = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    # Get maturity date components
    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    # Adjust day for calculation
    rpdays_month = get_month_last_day(rpyr, rpmth)

    if mdday > rpdays_month:
        mdday = rpdays_month

    # Calculate differences
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    # Calculate remaining months
    remmth = remy * 12 + remm + remd / rpdays_month

    return remmth


def get_payment_frequency_months(payfreq):
    """Convert payment frequency code to months"""
    freq_map = {
        '1': 1,  # Monthly
        '2': 3,  # Quarterly
        '3': 6,  # Semi-annual
        '4': 12,  # Annual
    }
    return freq_map.get(payfreq, 0)


# ============================================================================
# LOAD AND PROCESS LOAN DATA
# ============================================================================

print("\nStep 1: Loading loan data...")

# Load loan data
loan_path = BNM1_LOAN_PATH.format(month=REPTMON, week=NOWK)
loans = pl.read_parquet(loan_path)

print(f"  Loaded loans: {len(loans)} records")


# ============================================================================
# PROCESS LOANS - GENERATE BNM RECORDS
# ============================================================================

print("\nStep 2: Processing loans and generating BNM codes...")

note_records = []

for row in loans.iter_rows(named=True):
    paidind = str(row.get('PAIDIND', ''))
    eir_adj = row.get('EIR_ADJ')

    # Filter: PAIDIND NOT IN ('P','C') OR EIR_ADJ NE .
    if paidind in ['P', 'C'] and eir_adj is None:
        continue

    # Check if Islamic product or specific products
    prodcd = str(row.get('PRODCD', ''))
    product = row.get('PRODUCT', 0)

    if not (prodcd.startswith('34') or product in [225, 226]):
        continue

    # Determine customer category
    custcd = str(row.get('CUSTCD', ''))
    if custcd in ['77', '78', '95', '96']:
        cust = '08'
    else:
        cust = '09'

    acctype = row.get('ACCTYPE', '')
    balance = row.get('BALANCE', 0) or 0

    # Process Overdraft accounts
    if acctype == 'OD':
        remmth = 0.1
        amount = balance

        if prodcd == '34240':
            bnmcode = f"95219{cust}{classify_remmth(remmth)}0000Y"
        else:
            bnmcode = f"95213{cust}{classify_remmth(remmth)}0000Y"

        note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})
        continue

    # Process Loan accounts
    if acctype != 'LN':
        continue

    # Get liquidity product type
    prod = get_liq_product(product)

    # Determine item code
    if custcd in ['77', '78', '95', '96']:
        if prod == 'HL':
            item = '214'
        else:
            item = '219'
    else:
        if prod in ['FL', 'HL']:
            item = '211'
        elif prod == 'RC':
            item = '212'
        else:
            item = '219'

    # Get dates
    bldate = row.get('BLDATE')
    issdte = row.get('ISSDTE')
    exprdate = row.get('EXPRDATE')
    payamt = row.get('PAYAMT', 0) or 0
    payfreq = str(row.get('PAYFREQ', ''))
    loanstat = row.get('LOANSTAT', 0)

    # Convert dates if needed
    if isinstance(bldate, str):
        from datetime import datetime

        bldate = datetime.strptime(bldate, '%Y-%m-%d').date() if bldate else None
    if isinstance(issdte, str):
        from datetime import datetime

        issdte = datetime.strptime(issdte, '%Y-%m-%d').date() if issdte else None
    if isinstance(exprdate, str):
        from datetime import datetime

        exprdate = datetime.strptime(exprdate, '%Y-%m-%d').date() if exprdate else None

    # Calculate days since last billing
    if bldate and bldate > date(1900, 1, 1):
        days = (REPTDATE - bldate).days
    else:
        days = 0

    # Check if expiring within a week
    if exprdate and (exprdate - REPTDATE).days < 8:
        remmth = 0.1
        amount = balance

        if product not in FCY:
            bnmcode = f"95{item}{cust}{classify_remmth(remmth)}0000Y"
            note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

            if days > 89 or loanstat != 1:
                bnmcode = f"93{item}{cust}{classify_remmth(13)}0000Y"
                note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

        # Handle EIR adjustment if present
        if eir_adj is not None:
            amount = eir_adj
            bnmcode = f"95{item}{cust}060000Y"
            note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})
            bnmcode = f"93{item}{cust}060000Y"
            note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

        continue

    # Get payment frequency in months
    freq = get_payment_frequency_months(payfreq)

    # Handle special payment frequencies
    if payfreq in ['5', '9', ' '] or product in [350, 910, 925]:
        bldate = exprdate
    elif not bldate or bldate <= date(1900, 1, 1):
        bldate = issdte if issdte else REPTDATE

        # Advance to next billing date after report date
        while bldate and bldate <= REPTDATE:
            bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)

    # Ensure payment amount is non-negative
    if payamt < 0:
        payamt = 0

    # Adjust bldate if beyond expiry or balance too low
    if bldate and exprdate:
        if bldate > exprdate or balance <= payamt:
            bldate = exprdate

    # Process payment schedule
    temp_balance = balance

    while bldate and exprdate and bldate <= exprdate:
        # Calculate remaining months
        matdt = bldate
        remmth = calculate_remaining_months(matdt, REPTDATE)

        # Check if remaining months > 12 or this is last payment
        if remmth > 12 or bldate == exprdate:
            break

        # Adjust remmth if within a week
        if remmth > 0.1 and (bldate - REPTDATE).days < 8:
            remmth = 0.1

        # Record payment
        amount = payamt
        temp_balance -= payamt

        if product not in FCY:
            bnmcode = f"95{item}{cust}{classify_remmth(remmth)}0000Y"
            note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

            if days > 89 or loanstat != 1:
                remmth_npl = 13
                bnmcode = f"93{item}{cust}{classify_remmth(remmth_npl)}0000Y"
                note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

        # Calculate next billing date
        bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)

        if bldate > exprdate or temp_balance <= payamt:
            bldate = exprdate

    # Final balance
    amount = temp_balance

    if exprdate:
        matdt = exprdate
        remmth = calculate_remaining_months(matdt, REPTDATE)
    else:
        remmth = 13

    if product not in FCY:
        bnmcode = f"95{item}{cust}{classify_remmth(remmth)}0000Y"
        note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

        if days > 89 or loanstat != 1:
            remmth_npl = 13
            bnmcode = f"93{item}{cust}{classify_remmth(remmth_npl)}0000Y"
            note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

    # Handle EIR adjustment if present
    if eir_adj is not None:
        amount = eir_adj
        bnmcode = f"95{item}{cust}060000Y"
        note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})
        bnmcode = f"93{item}{cust}060000Y"
        note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

note = pl.DataFrame(note_records)
print(f"  Generated BNM records: {len(note)}")


# ============================================================================
# PRINT FCY RECORDS (SHOULD BE EMPTY)
# ============================================================================

print("\nStep 3: Checking FCY records...")

fcy_records = note.filter(
    pl.col('BNMCODE').str.slice(0, 2).is_in(['94', '96'])
)

print(f"  FCY records (should be 0): {len(fcy_records)}")


# ============================================================================
# REVOLVING CREDITS SPECIAL PROCESSING
# ============================================================================

print("\nStep 4: Processing revolving credits...")

# Remove 93212 codes
note = note.filter(~pl.col('BNMCODE').str.slice(0, 5).eq('93212'))

# Extract 95212 codes
rccorp = note.filter(pl.col('BNMCODE').str.slice(0, 5).eq('95212'))

# Create 93212 equivalents
if len(rccorp) > 0:
    rccorp1 = rccorp.with_columns([
        (pl.lit('93212') + pl.col('BNMCODE').str.slice(5, 9)).alias('BNMCODE')
    ])

    # Combine back
    note = pl.concat([note, rccorp1])
    print(f"  Added RC corporate records: {len(rccorp1)}")


# ============================================================================
# CREATE DEFAULT RECORDS (ZERO AMOUNTS FOR ALL CATEGORIES)
# ============================================================================

print("\nStep 5: Creating default records...")

default_records = []

for n in range(1, 7):
    # RM accounts
    for prefix in ['93', '95']:
        for item in ['211', '212', '213', '214', '215', '219']:
            for cust in ['08', '09']:
                # Skip certain combinations based on business logic
                if item in ['213', '214', '215', '219'] and cust == '09':
                    continue
                if item in ['211', '212'] and cust == '08':
                    continue

                bnmcode = f"{prefix}{item}{cust}{n:02d}0000Y"
                default_records.append({'BNMCODE': bnmcode, 'AMOUNT': 0.0})

    # FCY accounts (included in defaults even though not used)
    for prefix in ['94', '96']:
        for item in ['211', '212', '213', '214', '215', '219']:
            for cust in ['08', '09']:
                if item in ['213', '214', '215', '219'] and cust == '09':
                    continue
                if item in ['211', '212'] and cust == '08':
                    continue

                bnmcode = f"{prefix}{item}{cust}{n:02d}0000Y"
                default_records.append({'BNMCODE': bnmcode, 'AMOUNT': 0.0})

default = pl.DataFrame(default_records).sort('BNMCODE')
print(f"  Created default records: {len(default)}")

# Merge with actual data (defaults will be overridden by actuals)
note_sorted = note.sort('BNMCODE')
note = default.join(note_sorted, on='BNMCODE', how='left', suffix='_actual').with_columns([
    pl.when(pl.col('AMOUNT_actual').is_not_null())
    .then(pl.col('AMOUNT_actual'))
    .otherwise(pl.col('AMOUNT'))
    .alias('AMOUNT')
]).select(['BNMCODE', 'AMOUNT'])


# ============================================================================
# SUMMARIZE BY BNMCODE
# ============================================================================

print("\nStep 6: Summarizing by BNMCODE...")

bnm_note = note.group_by('BNMCODE').agg([
    pl.col('AMOUNT').sum().alias('AMOUNT')
]).sort('BNMCODE')

print(f"  Final BNM records: {len(bnm_note)}")


# ============================================================================
# SAVE OUTPUT
# ============================================================================

print("\nStep 7: Saving output...")

bnm_note.write_parquet(BNM_NOTE_PATH)
print(f"  Saved to: {BNM_NOTE_PATH}")


# ============================================================================
# GENERATE REPORTS
# ============================================================================

print("\nStep 8: Generating reports...")


def write_report_with_asa(filepath, title, data_df):
    """Generate report with ASA carriage control characters"""

    PAGE_LENGTH = 60
    line_count = 0
    page_number = 1

    with open(filepath, 'w') as f:

        def write_line(asa_char, content):
            """Write a line with ASA carriage control character"""
            nonlocal line_count
            f.write(f"{asa_char}{content}\n")
            line_count += 1

        def write_header():
            """Write page header"""
            nonlocal line_count, page_number
            write_line('1', ' ' * 100)  # Form feed
            write_line(' ', title.center(100))
            write_line(' ', f'{RDATE}'.center(100))
            write_line(' ', f'Page {page_number}'.center(100))
            write_line(' ', ' ' * 100)
            write_line(' ', f"{'BNMCODE':<20} {'AMOUNT':>20}")
            write_line(' ', '-' * 100)
            line_count = 7
            page_number += 1

        # Write header
        write_header()

        # Write data
        for row in data_df.iter_rows(named=True):
            # Check if we need a new page
            if line_count >= PAGE_LENGTH - 3:
                write_header()

            bnmcode = row['BNMCODE']
            amount = row['AMOUNT']

            line = f" {bnmcode:<20} {amount:20,.2f}"
            write_line(' ', line)

        # Write total
        total = data_df.select(pl.col('AMOUNT').sum()).item(0, 0)
        write_line(' ', '-' * 100)
        write_line(' ', f" {'TOTAL':<20} {total:20,.2f}")

    print(f"    Report written to: {filepath}")


# Generate FCY report
write_report_with_asa(REPORT_FCY_PATH, 'FCY', fcy_records)

# Generate full NOTE report
write_report_with_asa(REPORT_NOTE_PATH, 'BNM NOTE', bnm_note)


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("BNM Liquidity Loan Processing completed successfully!")
print("=" * 70)

# Calculate statistics
total_amount = bnm_note.select(pl.col('AMOUNT').sum()).item(0, 0)
performing = bnm_note.filter(pl.col('BNMCODE').str.slice(0, 2).is_in(['95', '96']))
non_performing = bnm_note.filter(pl.col('BNMCODE').str.slice(0, 2).is_in(['93', '94']))

performing_total = performing.select(pl.col('AMOUNT').sum()).item(0, 0)
non_performing_total = non_performing.select(pl.col('AMOUNT').sum()).item(0, 0)

print(f"\nSummary Statistics:")
print(f"  Total amount: {total_amount:,.2f}")
print(f"  Performing (95/96): {performing_total:,.2f}")
print(f"  Non-performing (93/94): {non_performing_total:,.2f}")

print(f"\nRecord Counts:")
print(f"  Total BNM codes: {len(bnm_note)}")
print(f"  Performing codes: {len(performing)}")
print(f"  Non-performing codes: {len(non_performing)}")
print(f"  FCY codes: {len(fcy_records)}")

print(f"\nOutput files:")
print(f"  - BNM NOTE: {BNM_NOTE_PATH}")
print(f"  - FCY Report: {REPORT_FCY_PATH}")
print(f"  - BNM NOTE Report: {REPORT_NOTE_PATH}")
