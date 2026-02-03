#!/usr/bin/env python3
"""
File Name: EIIDNLF0
Report Name: EIBWLIQ2 - BNM Liquidity Framework
Contractual Run-offs as of End of Month
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
LOAN1_REPTDATE_PATH = "/data/input/loan1_reptdate.parquet"
BNM1_LOAN_PATH = "/data/input/bnm1_loan_{month}{day}.parquet"
BNM2_LOAN_PATH = "/data/input/bnm2_loan_{month}{day}.parquet"

# Output paths
OUTPUT_DIR = "/data/output"
BNM_NOTE_PATH = f"{OUTPUT_DIR}/bnm_note.parquet"
REPORT_PATH = f"{OUTPUT_DIR}/eibwliq2_report.txt"

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
    if remmth is None or (isinstance(remmth, float) and pl.Float64.is_nan(remmth)):
        return '07'  # MISSING
    elif remmth < 0.255:
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
    reptyr = str(reptdate.year)[2:4]
    reptmon = f"{reptdate.month:02d}"
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime('%d/%m/%y')

    # Load LOAN1.REPTDATE for weekly data
    loan1_reptdate_df = pl.read_parquet(LOAN1_REPTDATE_PATH)
    wk_reptdate = loan1_reptdate_df.select('REPTDATE').item(0, 0)

    if isinstance(wk_reptdate, str):
        from datetime import datetime
        wk_reptdate = datetime.strptime(wk_reptdate, '%Y-%m-%d').date()

    wk_day = wk_reptdate.day

    if wk_day == 8:
        wknowk = '1'
    elif wk_day == 15:
        wknowk = '2'
    elif wk_day == 22:
        wknowk = '3'
    else:
        wknowk = '4'

    wkmon = f"{wk_reptdate.month:02d}"
    wkday = f"{wk_reptdate.day:02d}"

    # Calculate runoff date (last day of report month)
    runoff_year = reptdate.year
    runoff_month = reptdate.month
    runoff_day = get_month_last_day(runoff_year, runoff_month)
    runoffdt = date(runoff_year, runoff_month, runoff_day)

    return {
        'reptdate': reptdate,
        'nowk': nowk,
        'reptyear': reptyear,
        'reptyr': reptyr,
        'reptmon': reptmon,
        'reptday': reptday,
        'rdate': rdate,
        'wknowk': wknowk,
        'wkmon': wkmon,
        'wkday': wkday,
        'runoffdt': runoffdt
    }


dates = get_report_dates()
REPTDATE = dates['reptdate']
NOWK = dates['nowk']
REPTYEAR = dates['reptyear']
REPTYR = dates['reptyr']
REPTMON = dates['reptmon']
REPTDAY = dates['reptday']
RDATE = dates['rdate']
WKNOWK = dates['wknowk']
WKMON = dates['wkmon']
WKDAY = dates['wkday']
RUNOFFDT = dates['runoffdt']

print(f"Report Date: {RDATE}")
print(f"Week Number: {NOWK}")
print(f"Run-off Date: {RUNOFFDT}")
print(f"Weekly Report Date: {WKMON}/{WKDAY}, Week: {WKNOWK}")


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


def calculate_remaining_months(matdt, runoffdt):
    """Calculate remaining months from runoff date to maturity date"""
    if matdt <= runoffdt:
        return None

    # Calculate year, month, day differences
    years_diff = matdt.year - runoffdt.year
    months_diff = matdt.month - runoffdt.month

    # Adjust day for calculation
    runoff_last_day = get_month_last_day(runoffdt.year, runoffdt.month)
    mat_day = min(matdt.day, runoff_last_day)

    days_diff = mat_day - runoffdt.day

    # Calculate remaining months
    remmth = years_diff * 12 + months_diff + days_diff / runoff_last_day

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
loan_path = BNM1_LOAN_PATH.format(month=REPTMON, day=REPTDAY)
notex = pl.read_parquet(loan_path)

# Filter out paid and closed accounts
notex = notex.filter(~pl.col('PAIDIND').is_in(['P', 'C']))

print(f"  Loaded loans: {len(notex)} records")


# ============================================================================
# PROCESS LOANS - GENERATE BNM RECORDS
# ============================================================================

print("\nStep 2: Processing loans and generating BNM codes...")

note_records = []

for row in notex.iter_rows(named=True):
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

    # Special handling for product 100
    if product == 100:
        item = '212'

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

    # Check if expired
    if exprdate and exprdate <= RUNOFFDT:
        continue  # Skip - already matured

    # Check if expiring within a week
    if exprdate and (exprdate - RUNOFFDT).days < 8:
        remmth = 0.1
        amount = balance

        if days > 89 or loanstat != 1:
            remmth_val = 0.1
        else:
            remmth_val = remmth

        if product not in FCY:
            bnmcode = f"95{item}{cust}{classify_remmth(remmth_val)}0000Y"
            note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

            if days > 89 or loanstat != 1:
                bnmcode = f"93{item}{cust}{classify_remmth(13)}0000Y"
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
        if bldate <= RUNOFFDT:
            remmth = None
        elif (bldate - RUNOFFDT).days < 8:
            remmth = 0.1
        else:
            remmth = calculate_remaining_months(bldate, RUNOFFDT)

        # Check if this is last payment or remaining months > 1
        if remmth is not None and (remmth > 1 or bldate == exprdate):
            break

        # Record payment
        amount = payamt
        temp_balance -= payamt

        if remmth is not None:
            if days > 89 or loanstat != 1:
                remmth_val = 0.1
            else:
                remmth_val = remmth

            if product not in FCY:
                bnmcode = f"95{item}{cust}{classify_remmth(remmth_val)}0000Y"
                note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

                if days > 89 or loanstat != 1:
                    bnmcode = f"93{item}{cust}{classify_remmth(13)}0000Y"
                    note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

        # Calculate next billing date
        bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)

        if bldate > exprdate or temp_balance <= payamt:
            bldate = exprdate

    # Final balance
    amount = temp_balance

    if exprdate and exprdate > RUNOFFDT:
        remmth = calculate_remaining_months(exprdate, RUNOFFDT)
    else:
        remmth = None

    if remmth is not None:
        if days > 89 or loanstat != 1:
            remmth_val = 0.1
        else:
            remmth_val = remmth

        if product not in FCY:
            bnmcode = f"95{item}{cust}{classify_remmth(remmth_val)}0000Y"
            note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

            if days > 89 or loanstat != 1:
                bnmcode = f"93{item}{cust}{classify_remmth(13)}0000Y"
                note_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

note = pl.DataFrame(note_records)
print(f"  Generated BNM records: {len(note)}")


# ============================================================================
# FILTER OUT MISSING REMMTH (CODE 07)
# ============================================================================

print("\nStep 3: Filtering records...")

note = note.filter(pl.col('BNMCODE').str.slice(7, 2) != '07')
print(f"  Records after filtering missing remmth: {len(note)}")


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
# ADD EIR ADJUSTMENTS
# ============================================================================

print("\nStep 5: Adding EIR adjustments...")

# Load weekly loan data
loan_wk_path = BNM2_LOAN_PATH.format(month=WKMON, day=WKDAY)
loan_wk = pl.read_parquet(loan_wk_path)

# Filter for LN accounts with EIR adjustments
loan_wk = loan_wk.filter(
    (pl.col('ACCTYPE') == 'LN') &
    (pl.col('EIR_ADJ').is_not_null())
)

addeir_records = []

for row in loan_wk.iter_rows(named=True):
    custcd = str(row.get('CUSTCD', ''))
    product = row.get('PRODUCT', 0)
    eir_adj = row.get('EIR_ADJ', 0)

    if eir_adj is None:
        continue

    # Determine customer category
    if custcd in ['77', '78', '95', '96']:
        cust = '08'
    else:
        cust = '09'

    # Get liquidity product type
    prod = get_liq_product(product)

    # Determine item code
    if custcd in ['77', '78', '95', '96']:
        if prod == 'HL':
            item = '214'
        else:
            item = '219'
    else:
        if prod == 'FL':
            item = '211'
        elif prod == 'RC':
            item = '212'
        else:
            item = '219'

    # Special handling for product 100
    if product == 100:
        item = '212'

    amount = eir_adj

    # Add to both 95 and 93 with category 06 (>1 year)
    bnmcode = f"95{item}{cust}060000Y"
    addeir_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

    bnmcode = f"93{item}{cust}060000Y"
    addeir_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

if addeir_records:
    addeir = pl.DataFrame(addeir_records)
    note = pl.concat([note, addeir])
    print(f"  Added EIR adjustment records: {len(addeir_records)}")


# ============================================================================
# CREATE DEFAULT RECORDS (ZERO AMOUNTS FOR ALL CATEGORIES)
# ============================================================================

print("\nStep 6: Creating default records...")

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

default = pl.DataFrame(default_records)
print(f"  Created default records: {len(default)}")

# Combine with actual data
note = pl.concat([default, note])


# ============================================================================
# SUMMARIZE BY BNMCODE
# ============================================================================

print("\nStep 7: Summarizing by BNM code...")

bnm_note = note.group_by('BNMCODE').agg([
    pl.col('AMOUNT').sum().alias('AMOUNT')
]).sort('BNMCODE')

print(f"  Final BNM records: {len(bnm_note)}")


# ============================================================================
# SAVE OUTPUT
# ============================================================================

print("\nStep 8: Saving output...")

bnm_note.write_parquet(BNM_NOTE_PATH)
print(f"  Saved to: {BNM_NOTE_PATH}")


# ============================================================================
# GENERATE REPORT
# ============================================================================

print("\nStep 9: Generating report...")


def write_report_with_asa():
    """Generate report with ASA carriage control characters"""

    PAGE_LENGTH = 60
    line_count = 0
    page_number = 1

    with open(REPORT_PATH, 'w') as f:

        def write_line(asa_char, content):
            """Write a line with ASA carriage control character"""
            nonlocal line_count
            f.write(f"{asa_char}{content}\n")
            line_count += 1

        def write_header():
            """Write page header"""
            nonlocal line_count, page_number
            write_line('1', ' ' * 100)  # Form feed
            write_line(' ', 'BNM LIQUIDITY FRAMEWORK'.center(100))
            write_line(' ', 'CONTRACTUAL RUN-OFFS AS OF END OF THE MONTH'.center(100))
            write_line(' ', f'REPORTING DATE: {RDATE}'.center(100))
            write_line(' ', f'Page {page_number}'.center(100))
            write_line(' ', ' ' * 100)
            write_line(' ', f"{'BNMCODE':<20} {'AMOUNT':>20}")
            write_line(' ', '-' * 100)
            line_count = 8
            page_number += 1

        # Write header
        write_header()

        # Write data
        for row in bnm_note.iter_rows(named=True):
            # Check if we need a new page
            if line_count >= PAGE_LENGTH - 2:
                write_header()

            bnmcode = f"{row['BNMCODE']:<20}"
            amount = f"{row['AMOUNT']:20,.2f}"

            line = f" {bnmcode} {amount}"
            write_line(' ', line)

        # Write total
        total_amount = bnm_note.select(pl.col('AMOUNT').sum()).item(0, 0)
        write_line(' ', '-' * 100)
        write_line(' ', f"{'TOTAL':<20} {total_amount:20,.2f}")

    print(f"  Report written to: {REPORT_PATH}")


write_report_with_asa()

print("\n" + "=" * 70)
print("EIBWLIQ2 processing completed successfully!")
print("=" * 70)
print(f"\nOutput files:")
print(f"  - BNM data: {BNM_NOTE_PATH}")
print(f"  - Report: {REPORT_PATH}")
