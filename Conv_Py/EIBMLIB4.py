"""
File Name: EIBMLIB4
Loan Maturity Profile Processor
Processes BTRAD loan data and calculates maturity profiles with repayment schedules
"""

import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
import sys
import calendar


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
INPUT_DIR = Path("/path/to/input")
OUTPUT_DIR = Path("/path/to/output")

# Input files
REPTDATE_PATH = INPUT_DIR / "REPTDATE.parquet"
BTRAD_PATH_TEMPLATE = INPUT_DIR / "BTRAD{}{}.parquet"

# Output files
BT_OUTPUT_PATH = OUTPUT_DIR / "BT.parquet"
BT_REPORT_PATH = OUTPUT_DIR / "BT_REPORT.txt"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

def get_rem_format(remmth):
    """
    Convert remaining months to maturity bucket code.

    LOW-0.1 = '01'   # UP TO 1 WK
    0.1-1   = '02'   # >1 WK - 1 MTH
    1-3     = '03'   # >1 MTH - 3 MTHS
    3-6     = '04'   # >3 - 6 MTHS
    6-12    = '05'   # >6 MTHS - 1 YR
    OTHER   = '06'   # > 1 YEAR
    """
    if remmth <= 0.1:
        return '01'
    elif remmth <= 1:
        return '02'
    elif remmth <= 3:
        return '03'
    elif remmth <= 6:
        return '04'
    elif remmth <= 12:
        return '05'
    else:
        return '06'


def get_prod_format(product):
    """
    Classify product type.

    HL = Housing Loan products
    RC = Revolving Credit products
    FL = Other Facility/Loan products
    """
    hl_products = {4, 5, 6, 7, 31, 32, 100, 101, 102, 103, 110, 111, 112, 113, 114, 115,
                   116, 170, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
                   225, 226, 227, 228, 229, 230, 231, 232, 233, 234}
    rc_products = {350, 910, 925}

    if product in hl_products:
        return 'HL'
    elif product in rc_products:
        return 'RC'
    else:
        return 'FL'


# ============================================================================
# DATE HELPER FUNCTIONS
# ============================================================================

def get_days_in_month(year, month):
    """Get number of days in a given month."""
    return calendar.monthrange(year, month)[1]


def calculate_next_bldate(bldate, issdte, payfreq, freq):
    """
    Calculate next billing date based on payment frequency.

    If PAYFREQ = '6' (biweekly), add 14 days.
    Otherwise, add FREQ months from BLDATE, using ISSDTE day.
    """
    if payfreq == '6':
        # Biweekly - add 14 days
        next_date = bldate + timedelta(days=14)
        return next_date
    else:
        # Monthly/quarterly/semi-annual/annual
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year

        # Handle month overflow
        while mm > 12:
            mm -= 12
            yy += 1

        # Adjust day if it exceeds days in month
        max_day = get_days_in_month(yy, mm)
        if dd > max_day:
            dd = max_day

        return datetime(yy, mm, dd).date()


def calculate_remaining_months(matdt, reptdate):
    """
    Calculate remaining months from reptdate to maturity date.

    Returns fractional months considering day differences.
    """
    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    rpyr = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    # Get days in report month
    rpdays_in_month = get_days_in_month(rpyr, rpmth)

    # Adjust maturity day if it exceeds days in report month
    if mdday > rpdays_in_month:
        mdday = rpdays_in_month

    # Calculate year, month, and day differences
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    # Calculate total remaining months
    remmth = remy * 12 + remm + remd / rpdays_in_month

    return remmth


# ============================================================================
# READ REPTDATE
# ============================================================================

def read_reptdate():
    """Read report date and calculate week number."""
    print("Reading REPTDATE...")

    df = pl.read_parquet(REPTDATE_PATH)
    reptdate = df['REPTDATE'][0]

    if isinstance(reptdate, str):
        reptdate = datetime.strptime(reptdate, "%Y-%m-%d").date()

    # Calculate week number based on day of month
    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    reptmon = f"{reptdate.month:02d}"
    reptday = f"{reptdate.day:02d}"
    reptyear = f"{reptdate.year:04d}"
    rdate = reptdate.strftime("%d%m%Y")

    print(f"Report Date: {reptdate} (Week {nowk})")

    return reptdate, nowk, reptmon, reptday, reptyear, rdate


# ============================================================================
# PROCESS LOAN DATA
# ============================================================================

def process_loan_data(reptdate, reptmon, nowk):
    """
    Process BTRAD loan data and generate maturity profile.
    """
    print("Processing loan data...")

    # Read BTRAD data
    btrad_path = Path(str(BTRAD_PATH_TEMPLATE).format(reptmon, nowk))
    df = pl.read_parquet(btrad_path)

    print(f"Loaded {len(df)} records from BTRAD")

    # Filter: SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226)
    df = df.filter(
        (pl.col('PRODCD').str.slice(0, 2) == '34') |
        (pl.col('PRODUCT').is_in([225, 226]))
    )

    print(f"After filtering: {len(df)} records")

    # Process each loan record
    output_records = []

    for row in df.iter_rows(named=True):
        custcd = row.get('CUSTCD', '')
        product = row.get('PRODUCT')
        prodcd = row.get('PRODCD', '')
        balance = row.get('BALANCE', 0.0)
        payamt = row.get('PAYAMT', 0.0)
        bldate_val = row.get('BLDATE')
        issdte_val = row.get('ISSDTE')
        exprdate_val = row.get('EXPRDATE')

        # Determine CUST
        if custcd in ('77', '78', '95', '96'):
            cust = '08'
        else:
            cust = '09'

        # Determine PROD
        prod = 'BT'

        # Classify product type
        prod_type = get_prod_format(product)

        # Determine ITEM
        if custcd in ('77', '78', '95', '96'):
            if prod_type == 'HL':
                item = '214'
            else:
                item = '219'
        else:
            if prod_type == 'FL':
                item = '211'
            elif prod_type == 'RC':
                item = '212'
            else:
                item = '219'

        # Convert dates
        bldate = None
        if bldate_val and bldate_val > 0:
            if isinstance(bldate_val, (int, float)):
                try:
                    bldate = datetime(1960, 1, 1).date() + timedelta(days=int(bldate_val))
                except:
                    bldate = None
            elif hasattr(bldate_val, 'date'):
                bldate = bldate_val.date() if callable(bldate_val.date) else bldate_val
            elif isinstance(bldate_val, datetime):
                bldate = bldate_val.date()
            else:
                bldate = bldate_val

        issdte = None
        if issdte_val:
            if isinstance(issdte_val, (int, float)):
                try:
                    issdte = datetime(1960, 1, 1).date() + timedelta(days=int(issdte_val))
                except:
                    issdte = None
            elif hasattr(issdte_val, 'date'):
                issdte = issdte_val.date() if callable(issdte_val.date) else issdte_val
            elif isinstance(issdte_val, datetime):
                issdte = issdte_val.date()
            else:
                issdte = issdte_val

        exprdate = None
        if exprdate_val:
            if isinstance(exprdate_val, (int, float)):
                try:
                    exprdate = datetime(1960, 1, 1).date() + timedelta(days=int(exprdate_val))
                except:
                    exprdate = None
            elif hasattr(exprdate_val, 'date'):
                exprdate = exprdate_val.date() if callable(exprdate_val.date) else exprdate_val
            elif isinstance(exprdate_val, datetime):
                exprdate = exprdate_val.date()
            else:
                exprdate = exprdate_val

        # Calculate days since last billing
        days = 0
        if bldate:
            days = (reptdate - bldate).days

        # Process repayment schedule
        if not exprdate:
            continue

        # Check if expiry is within 1 week
        if (exprdate - reptdate).days < 8:
            remmth = 0.1

            # Output final amount
            amount = balance
            bnmcode = f"95{item}{cust}{get_rem_format(remmth)}0000Y"
            output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

            if days > 89:
                remmth = 13
            bnmcode = f"93{item}{cust}{get_rem_format(remmth)}0000Y"
            output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})
        else:
            # Process repayment schedule
            payfreq = '3'  # Default to semi-annual

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

            # Set initial bldate
            if product in (350, 910, 925):  # RC products
                bldate = exprdate
            elif not bldate or bldate <= datetime(1960, 1, 1).date():
                bldate = issdte if issdte else reptdate
                # Advance to next billing date after reptdate
                while bldate <= reptdate:
                    bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)

            # Ensure payamt is non-negative
            if payamt < 0:
                payamt = 0

            # Adjust bldate if needed
            if bldate > exprdate or balance <= payamt:
                bldate = exprdate

            # Process payment schedule
            current_balance = balance

            while bldate <= exprdate and current_balance > 0:
                matdt = bldate
                remmth = calculate_remaining_months(matdt, reptdate)

                # If remaining months > 12 or at expiry, output final balance
                if remmth > 12 or bldate == exprdate:
                    amount = current_balance
                    bnmcode = f"95{item}{cust}{get_rem_format(remmth)}0000Y"
                    output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

                    if days > 89:
                        remmth_code = 13
                    else:
                        remmth_code = remmth
                    bnmcode = f"93{item}{cust}{get_rem_format(remmth_code)}0000Y"
                    output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})
                    break

                # Output payment
                amount = min(payamt, current_balance)
                current_balance -= amount

                bnmcode = f"95{item}{cust}{get_rem_format(remmth)}0000Y"
                output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

                if days > 89:
                    remmth_code = 13
                else:
                    remmth_code = remmth
                bnmcode = f"93{item}{cust}{get_rem_format(remmth_code)}0000Y"
                output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

                # Calculate next billing date
                bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)

                # Adjust if exceeds expiry or balance exhausted
                if bldate > exprdate or current_balance <= payamt:
                    bldate = exprdate

    print(f"Generated {len(output_records)} output records")

    # Create output dataframe
    if output_records:
        df_output = pl.DataFrame(output_records)
    else:
        df_output = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    return df_output


# ============================================================================
# AGGREGATE AND OUTPUT
# ============================================================================

def aggregate_output(df_output):
    """Aggregate output by BNMCODE."""
    print("Aggregating output...")

    if len(df_output) == 0:
        print("No records to aggregate")
        return df_output

    # Aggregate by BNMCODE
    df_agg = df_output.group_by('BNMCODE').agg([
        pl.col('AMOUNT').sum()
    ])

    # Sort by BNMCODE
    df_agg = df_agg.sort('BNMCODE')

    print(f"Aggregated to {len(df_agg)} unique BNMCODE records")

    # Calculate total
    total_amount = df_agg['AMOUNT'].sum()
    print(f"Total AMOUNT: {total_amount:,.2f}")

    return df_agg


# ============================================================================
# GENERATE REPORT
# ============================================================================

def generate_report(df_agg):
    """Generate report output similar to PROC PRINT."""
    print("Generating report...")

    with open(BT_REPORT_PATH, 'w') as f:
        # Header with ASA carriage control
        f.write(' ' + '=' * 80 + '\n')
        f.write(' LOAN MATURITY PROFILE REPORT\n')
        f.write(' ' + '=' * 80 + '\n')
        f.write(' \n')
        f.write(f' {"BNMCODE":<20} {"AMOUNT":>20}\n')
        f.write(' ' + '-' * 80 + '\n')

        # Data rows
        total = 0.0
        for row in df_agg.iter_rows(named=True):
            bnmcode = row['BNMCODE']
            amount = row['AMOUNT']
            total += amount
            f.write(f' {bnmcode:<20} {amount:>20,.2f}\n')

        # Total
        f.write(' ' + '-' * 80 + '\n')
        f.write(f' {"TOTAL":<20} {total:>20,.2f}\n')
        f.write(' ' + '=' * 80 + '\n')

    print(f"Report written to {BT_REPORT_PATH}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    try:
        print("=" * 80)
        print("Loan Maturity Profile Processor")
        print("=" * 80)

        # Read reptdate
        reptdate, nowk, reptmon, reptday, reptyear, rdate = read_reptdate()

        # Process loan data
        df_output = process_loan_data(reptdate, reptmon, nowk)

        # Aggregate output
        df_agg = aggregate_output(df_output)

        # Write parquet output
        df_agg.write_parquet(BT_OUTPUT_PATH)
        print(f"Parquet output written to {BT_OUTPUT_PATH}")

        # Generate report
        generate_report(df_agg)

        print("=" * 80)
        print("Processing completed successfully!")
        print("=" * 80)

        return 0

    except Exception as e:
        print(f"\nERROR: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
