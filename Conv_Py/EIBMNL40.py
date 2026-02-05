#!/usr/bin/env python3
"""
BNM Loan Maturity Profile Processing
Converts SAS program to Python using DuckDB and Polars
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys


# ==============================================================================
# PATH CONFIGURATION
# ==============================================================================
INPUT_DIR = Path("./input")
OUTPUT_DIR = Path("./output")
LOAN_DIR = INPUT_DIR / "loan"
BNM_DIR = INPUT_DIR / "bnm"
BNM1_DIR = INPUT_DIR / "bnm1"

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==============================================================================
# CONSTANTS AND CONFIGURATIONS
# ==============================================================================
FCY_PRODUCTS = [800, 801, 802, 803, 804, 805, 806, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860]


# REMFMT format mapping
def remfmt_format(value):
    """Convert remaining months to BNM format code"""
    if value <= 0.1:
        return '01'  # UP TO 1 WK
    elif value <= 1:
        return '02'  # >1 WK - 1 MTH
    elif value <= 3:
        return '03'  # >1 MTH - 3 MTHS
    elif value <= 6:
        return '04'  # >3 - 6 MTHS
    elif value <= 12:
        return '05'  # >6 MTHS - 1 YR
    else:
        return '06'  # > 1 YEAR


# LIQPFMT format mapping (from included format file - assumed mapping)
def liqpfmt_format(product):
    """Map product codes to loan types"""
    # This mapping would come from PBBELF format file
    # Assumed common mappings based on typical bank products
    if product in [100, 101, 102, 103, 104, 105]:
        return 'FL'  # Fixed Loan
    elif product in [200, 201, 202, 203, 204, 205]:
        return 'HL'  # Housing Loan
    elif product in [300, 301, 302, 303]:
        return 'RC'  # Revolving Credit
    else:
        return 'OT'  # Other


# ==============================================================================
# HELPER FUNCTIONS - VARIABLE DECLARATION
# ==============================================================================
def initialize_day_arrays():
    """Initialize day arrays for date calculations"""
    d = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]  # D1-D12
    rd = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]  # RD1-RD12
    md = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]  # MD1-MD12
    return d, rd, md


def is_leap_year(year):
    """Check if year is leap year"""
    return year % 4 == 0


def days_in_month(year, month):
    """Get days in month accounting for leap years"""
    days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if month == 2 and is_leap_year(year):
        return 29
    return days[month - 1]


# ==============================================================================
# HELPER FUNCTIONS - NEXT BILLING DATE CALCULATION
# ==============================================================================
def calculate_next_bldate(bldate, issdte, payfreq, freq, lday):
    """Calculate next billing date based on payment frequency"""
    if payfreq == '6':  # Bi-weekly
        dd = bldate.day + 14
        mm = bldate.month
        yy = bldate.year

        # Adjust for leap year
        if mm == 2 and is_leap_year(yy):
            lday[1] = 29
        else:
            lday[1] = 28

        if dd > lday[mm - 1]:
            dd = dd - lday[mm - 1]
            mm += 1
            if mm > 12:
                mm -= 12
                yy += 1
    else:
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year
        if mm > 12:
            mm -= 12
            yy += 1

    # Adjust for leap year
    if mm == 2:
        if is_leap_year(yy):
            lday[1] = 29
        else:
            lday[1] = 28

    # Adjust day if exceeds month length
    if dd > lday[mm - 1]:
        dd = lday[mm - 1]

    return datetime(yy, mm, dd)


# ==============================================================================
# HELPER FUNCTIONS - REMAINING MONTHS CALCULATION
# ==============================================================================
def calculate_remmth(matdt, reptdate, rpdays):
    """Calculate remaining months to maturity"""
    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    rpyr = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    # Adjust for leap year
    if mdmth == 2 and is_leap_year(mdyr):
        rpdays[1] = 29
    else:
        rpdays[1] = 28

    # Adjust day if exceeds month length
    if mdday > rpdays[rpmth - 1]:
        mdday = rpdays[rpmth - 1]

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    remmth = remy * 12 + remm + remd / rpdays[rpmth - 1]

    return remmth


# ==============================================================================
# MAIN DATA PROCESSING
# ==============================================================================
def get_reptdate():
    """Get reporting date and calculate week number"""
    # Read REPTDATE from parquet file
    reptdate_path = BNM_DIR / "reptdate.parquet"

    conn = duckdb.connect()
    reptdate_df = conn.execute(f"""
        SELECT * FROM read_parquet('{reptdate_path}')
    """).pl()

    reptdate = reptdate_df['reptdate'][0]

    # Calculate week number based on day
    day = reptdate.day
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
    rdate = reptdate.strftime('%d%m%Y')

    return reptdate, nowk, reptyear, reptmon, reptday, rdate


def process_loans(reptdate, reptmon, nowk):
    """Process loan data and create NOTE dataset"""

    # Read loan data
    loan_path = BNM1_DIR / f"loan{reptmon}{nowk}.parquet"

    conn = duckdb.connect()
    loan_df = conn.execute(f"""
        SELECT * FROM read_parquet('{loan_path}')
    """).pl()

    # Initialize arrays
    lday, rpdays, mddays = initialize_day_arrays()

    # Get reptdate components
    rpyr = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    # Adjust for leap year
    if is_leap_year(rpyr):
        rpdays[1] = 29

    # Filter conditions
    loan_df = loan_df.filter(
        (~pl.col('paidind').is_in(['P', 'C'])) | (pl.col('eir_adj').is_not_null())
    ).filter(
        (pl.col('prodcd').str.slice(0, 2) == '34') | (pl.col('product').is_in([225, 226]))
    )

    # Process each row
    results = []

    for row in loan_df.iter_rows(named=True):
        # Determine customer type
        if row['custcd'] in ['77', '78', '95', '96']:
            cust = '08'
        else:
            cust = '09'

        # Process overdrafts
        if row['acctype'] == 'OD':
            remmth = 0.1
            amount = row['balance']
            bnmcode = f"95213{cust}{remfmt_format(remmth)}0000Y"

            if row['prodcd'] == '34240':
                bnmcode = f"95219{cust}{remfmt_format(remmth)}0000Y"

            results.append({
                'bnmcode': bnmcode,
                'amount': amount
            })
            continue

        # Process loans only
        if row['acctype'] != 'LN':
            continue

        # Map product to loan type
        prod = liqpfmt_format(row['product'])

        # Determine item code
        if row['custcd'] in ['77', '78', '95', '96']:
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

        # Calculate days since last billing
        if row['bldate'] is not None and row['bldate'] > datetime(1900, 1, 1):
            days = (reptdate - row['bldate']).days
        else:
            days = 0

        # Calculate remaining months
        if (row['exprdate'] - reptdate).days < 8:
            remmth = 0.1
        else:
            # Convert payfreq to months
            if row['payfreq'] == '1':
                freq = 1
            elif row['payfreq'] == '2':
                freq = 3
            elif row['payfreq'] == '3':
                freq = 6
            elif row['payfreq'] == '4':
                freq = 12
            else:
                freq = 0

            # Initialize billing date
            bldate = row['bldate']

            if row['payfreq'] in ['5', '9', ' ', None] or row['product'] in [350, 910, 925]:
                bldate = row['exprdate']
            elif bldate is None or bldate <= datetime(1900, 1, 1):
                bldate = row['issdte']
                # Advance to next billing date after reptdate
                while bldate <= reptdate:
                    bldate = calculate_next_bldate(bldate, row['issdte'], row['payfreq'], freq, lday)

            payamt = row['payamt'] if row['payamt'] is not None and row['payamt'] >= 0 else 0
            balance = row['balance']

            if bldate > row['exprdate'] or balance <= payamt:
                bldate = row['exprdate']

            # Process payment schedule
            while bldate <= row['exprdate']:
                matdt = bldate
                remmth = calculate_remmth(matdt, reptdate, rpdays)

                if remmth > 12 or bldate == row['exprdate']:
                    break

                if remmth > 0.1 and (bldate - reptdate).days < 8:
                    remmth = 0.1

                amount = payamt
                balance = balance - payamt

                # Output for RM (non-FCY products)
                if row['product'] not in FCY_PRODUCTS:
                    # Gross loans outstanding
                    results.append({
                        'bnmcode': f"95{item}{cust}{remfmt_format(remmth)}0000Y",
                        'amount': amount
                    })

                    # Non-performing loans
                    if days > 89 or row['loanstat'] != 1:
                        remmth_npl = 13
                    else:
                        remmth_npl = remmth

                    results.append({
                        'bnmcode': f"93{item}{cust}{remfmt_format(remmth_npl)}0000Y",
                        'amount': amount
                    })

                # Calculate next billing date
                bldate = calculate_next_bldate(bldate, row['issdte'], row['payfreq'], freq, lday)

                if bldate > row['exprdate'] or balance <= payamt:
                    bldate = row['exprdate']

        # Output remaining balance
        amount = balance if 'balance' in locals() else row['balance']

        if row['product'] not in FCY_PRODUCTS:
            # Gross loans outstanding
            results.append({
                'bnmcode': f"95{item}{cust}{remfmt_format(remmth)}0000Y",
                'amount': amount
            })

            # Non-performing loans
            if days > 89 or row['loanstat'] != 1:
                remmth = 13

            results.append({
                'bnmcode': f"93{item}{cust}{remfmt_format(remmth)}0000Y",
                'amount': amount
            })

        # Process EIR adjustment
        if row['eir_adj'] is not None:
            amount = row['eir_adj']

            results.append({
                'bnmcode': f"95{item}{cust}060000Y",
                'amount': amount
            })

            results.append({
                'bnmcode': f"93{item}{cust}060000Y",
                'amount': amount
            })

    # Convert results to Polars DataFrame
    note_df = pl.DataFrame(results)

    return note_df


def process_revolving_credits(note_df):
    """Process revolving credit adjustments"""

    # Remove records with BNMCODE starting with '93212'
    note_df = note_df.filter(~pl.col('bnmcode').str.slice(0, 5).eq('93212'))

    # Extract RC corporate records
    rccorp_df = note_df.filter(pl.col('bnmcode').str.slice(0, 5).eq('95212'))

    # Create new BNMCODE for RC corporate
    rccorp1_df = rccorp_df.with_columns(
        pl.concat_str([
            pl.lit('93212'),
            pl.col('bnmcode').str.slice(5, 9)
        ]).alias('bnmcode')
    )

    # Combine back with original note data
    note_df = pl.concat([note_df, rccorp1_df])

    return note_df


def create_default_records():
    """Create default zero-value records for all combinations"""

    results = []

    for n in range(1, 7):
        codes = [
            # RM codes
            f'93211090{n}0000Y', f'95211090{n}0000Y',
            f'93212090{n}0000Y', f'95212090{n}0000Y',
            f'93213080{n}0000Y', f'95213080{n}0000Y',
            f'93213090{n}0000Y', f'95213090{n}0000Y',
            f'93214080{n}0000Y', f'95214080{n}0000Y',
            f'93215080{n}0000Y', f'95215080{n}0000Y',
            f'93219080{n}0000Y', f'95219080{n}0000Y',
            f'93219090{n}0000Y', f'95219090{n}0000Y',
            # FCY codes
            f'94211090{n}0000Y', f'96211090{n}0000Y',
            f'94212090{n}0000Y', f'96212090{n}0000Y',
            f'94213080{n}0000Y', f'96213080{n}0000Y',
            f'94213090{n}0000Y', f'96213090{n}0000Y',
            f'94214080{n}0000Y', f'96214080{n}0000Y',
            f'94215080{n}0000Y', f'96215080{n}0000Y',
            f'94219080{n}0000Y', f'96219080{n}0000Y',
            f'94219090{n}0000Y', f'96219090{n}0000Y'
        ]

        for code in codes:
            results.append({
                'bnmcode': code,
                'amount': 0.0
            })

    default_df = pl.DataFrame(results)

    return default_df


def summarize_note(note_df):
    """Summarize NOTE data by BNMCODE"""

    summary_df = note_df.group_by('bnmcode').agg(
        pl.col('amount').sum().alias('amount')
    ).sort('bnmcode')

    return summary_df


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
def main():
    """Main execution function"""

    print("BNM Loan Maturity Profile Processing")
    print("=" * 80)

    # Step 1: Get reporting date
    print("\n1. Getting reporting date...")
    reptdate, nowk, reptyear, reptmon, reptday, rdate = get_reptdate()
    print(f"   Reporting Date: {rdate}")
    print(f"   Week Number: {nowk}")

    # Save REPTDATE to output
    reptdate_output = pl.DataFrame({
        'reptdate': [reptdate]
    })
    reptdate_output.write_parquet(OUTPUT_DIR / "reptdate.parquet")

    # Step 2: Process loans
    print("\n2. Processing loan data...")
    note_df = process_loans(reptdate, reptmon, nowk)
    print(f"   Records created: {len(note_df)}")

    # Step 3: Process revolving credits
    print("\n3. Processing revolving credits...")
    note_df = process_revolving_credits(note_df)
    print(f"   Records after RC processing: {len(note_df)}")

    # Step 4: Create default records
    print("\n4. Creating default records...")
    default_df = create_default_records()
    print(f"   Default records created: {len(default_df)}")

    # Step 5: Merge with defaults
    print("\n5. Merging with defaults...")

    # FIX: align schema before concat
    note_df = note_df.with_columns(
        pl.col("amount").cast(pl.Float64)
    )

    # Combine default and note data
    combined_df = pl.concat([default_df, note_df])

    # Step 6: Summarize
    print("\n6. Summarizing by BNMCODE...")
    final_df = summarize_note(combined_df)
    print(f"   Final records: {len(final_df)}")

    # Step 7: Save output
    print("\n7. Saving output...")
    output_path = OUTPUT_DIR / "note.parquet"
    final_df.write_parquet(output_path)
    print(f"   Output saved to: {output_path}")

    # Print sample of output
    print("\n8. Sample output:")
    print(final_df.head(20))

    print("\n" + "=" * 80)
    print("Processing complete!")

    return final_df


if __name__ == "__main__":
    try:
        result_df = main()
    except Exception as e:
        print(f"\nError occurred: {str(e)}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)
