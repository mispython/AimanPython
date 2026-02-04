"""
File Name: EIBDNLF0
Liquidity Framework Contractual Run-Offs
Processes loan data and calculates contractual run-offs as of end of month
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys
import calendar


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# INPUT_DIR = Path("/path/to/input")
# OUTPUT_DIR = Path("/path/to/output")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"


# Input files
REPTDATE_BNM_PATH = INPUT_DIR / "REPTDATE_BNM.parquet"
REPTDATE_LOAN_PATH = INPUT_DIR / "REPTDATE_LOAN.parquet"
REPTDATE_LOAN1_PATH = INPUT_DIR / "REPTDATE_LOAN1.parquet"
LOAN_PATH_TEMPLATE = INPUT_DIR / "LOAN{}{}.parquet"

# Output files
NOTE_OUTPUT_PATH = OUTPUT_DIR / "NOTE.parquet"
NOTE_REPORT_PATH = OUTPUT_DIR / "NOTE_REPORT.txt"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# CONSTANTS
# ============================================================================

# Foreign Currency Products
FCY_PRODUCTS = {800, 801, 802, 803, 804, 805, 806, 851, 852, 853, 854, 855, 856,
                857, 858, 859, 860, 807, 808, 809, 810, 811, 812, 813, 814, 815,
                816, 817}

# Product format mapping (LIQPFMT)
HL_PRODUCTS = {4, 5, 6, 7, 31, 32, 100, 101, 102, 103, 110, 111, 112, 113, 114, 115,
               116, 170, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
               225, 226, 227, 228, 229, 230, 231, 232, 233, 234}
RC_PRODUCTS = {350, 910, 925}
FL_PRODUCTS = set()  # Default for other products


# ============================================================================
# FORMAT FUNCTIONS
# ============================================================================

def get_rem_format(remmth):
    """
    Convert remaining months to maturity bucket code.

    LOW-0.255 = '01'   # UP TO 1 WK
    0.255-1   = '02'   # >1 WK - 1 MTH
    1-3       = '03'   # >1 MTH - 3 MTHS
    3-6       = '04'   # >3 - 6 MTHS
    6-12      = '05'   # >6 MTHS - 1 YR
    .         = '07'   # MISSING
    OTHER     = '06'   # > 1 YEAR
    """
    if remmth is None:
        return '07'
    elif remmth <= 0.255:
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


def get_liq_prod_format(product):
    """Classify product type for liquidity reporting."""
    if product in HL_PRODUCTS:
        return 'HL'
    elif product in RC_PRODUCTS:
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
    """Calculate next billing date based on payment frequency."""
    if payfreq == '6':
        # Biweekly - add 14 days
        return bldate + timedelta(days=14)
    else:
        # Use issue date day, add FREQ months
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year

        while mm > 12:
            mm -= 12
            yy += 1

        max_day = get_days_in_month(yy, mm)
        if dd > max_day:
            dd = max_day

        return datetime(yy, mm, dd).date()


def calculate_remaining_months(matdt, runoffdt):
    """Calculate remaining months from runoff date to maturity date."""
    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    rpyr = runoffdt.year
    rpmth = runoffdt.month
    rpday = runoffdt.day

    rpdays_in_month = get_days_in_month(rpyr, rpmth)

    if mdday > rpdays_in_month:
        mdday = rpdays_in_month

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    remmth = remy * 12 + remm + remd / rpdays_in_month

    return remmth


def parse_date(date_val):
    """Parse various date formats to date object."""
    if not date_val or date_val == 0:
        return None

    if isinstance(date_val, (int, float)):
        try:
            return datetime(1960, 1, 1).date() + timedelta(days=int(date_val))
        except:
            return None
    elif hasattr(date_val, 'date'):
        return date_val.date() if callable(date_val.date) else date_val
    elif isinstance(date_val, datetime):
        return date_val.date()
    else:
        return date_val


# ============================================================================
# READ REPTDATE
# ============================================================================

def assert_file_exists(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

def read_reptdate():
    """Read report dates and calculate parameters."""
    print("Reading REPTDATE files...")

    assert_file_exists(REPTDATE_BNM_PATH)

    # Read BNM reptdate
    df_bnm = pl.read_parquet(REPTDATE_BNM_PATH)
    reptdate = df_bnm['REPTDATE'][0]

    if isinstance(reptdate, str):
        reptdate = datetime.strptime(reptdate, "%Y-%m-%d").date()

    # Calculate week number
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

    # Read LOAN1 reptdate for weekly data
    df_loan1 = pl.read_parquet(REPTDATE_LOAN1_PATH)
    wk_reptdate = df_loan1['REPTDATE'][0]

    if isinstance(wk_reptdate, str):
        wk_reptdate = datetime.strptime(wk_reptdate, "%Y-%m-%d").date()

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

    # Calculate run-off date (end of month)
    rpyr = reptdate.year
    rpmth = reptdate.month
    rpdays = get_days_in_month(rpyr, rpmth)
    runoffdt = datetime(rpyr, rpmth, rpdays).date()

    print(f"Report Date: {reptdate} (Week {nowk})")
    print(f"Weekly Report Date: {wk_reptdate} (Week {wknowk})")
    print(f"Run-off Date: {runoffdt}")

    return {
        'reptdate': reptdate,
        'nowk': nowk,
        'reptmon': reptmon,
        'reptday': reptday,
        'reptyear': reptyear,
        'wknowk': wknowk,
        'wkmon': wkmon,
        'wkday': wkday,
        'runoffdt': runoffdt,
    }


# ============================================================================
# PROCESS LOAN DATA
# ============================================================================

def process_loan_data(params):
    """Process loan data and generate maturity profile."""
    print("Processing loan data...")

    reptdate = params['reptdate']
    reptmon = params['reptmon']
    reptday = params['reptday']
    runoffdt = params['runoffdt']

    # Read loan data
    loan_path = Path(str(LOAN_PATH_TEMPLATE).format(reptmon, reptday))
    df = pl.read_parquet(loan_path)

    print(f"Loaded {len(df)} records from LOAN file")

    # Filter: PAIDIND NOT IN ('P','C')
    df = df.filter(~pl.col('PAIDIND').is_in(['P', 'C']))

    print(f"After PAIDIND filter: {len(df)} records")

    # Process each loan record
    output_records = []

    for row in df.iter_rows(named=True):
        prodcd = row.get('PRODCD', '')
        product = row.get('PRODUCT')
        custcd = row.get('CUSTCD', '')
        acctype = row.get('ACCTYPE', '')
        balance = row.get('BALANCE', 0.0)
        payamt = row.get('PAYAMT', 0.0)
        bldate_val = row.get('BLDATE')
        issdte_val = row.get('ISSDTE')
        exprdate_val = row.get('EXPRDATE')
        payfreq = row.get('PAYFREQ', '')
        loanstat = row.get('LOANSTAT')

        # Process OD (Overdraft) accounts
        if acctype == 'OD':
            if custcd in ('77', '78', '95', '96'):
                cust = '08'
            else:
                cust = '09'

            remmth = 0.1
            amount = balance

            if prodcd == '34240':
                bnmcode = f"95219{cust}{get_rem_format(remmth)}0000Y"
            else:
                bnmcode = f"95213{cust}{get_rem_format(remmth)}0000Y"

            output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})
            continue

        # Process LN (Loan) accounts
        if acctype != 'LN':
            continue

        # Filter for specific products
        if not (prodcd.startswith('34') or product in (225, 226)):
            continue

        # Determine CUST
        if custcd in ('77', '78', '95', '96'):
            cust = '08'
        else:
            cust = '09'

        # Classify product
        prod = get_liq_prod_format(product)

        # Determine ITEM
        if custcd in ('77', '78', '95', '96'):
            if prod == 'HL':
                item = '214'
            else:
                item = '219'
        else:
            if prod in ('FL', 'HL'):
                item = '211'
            elif prod == 'RC':
                item = '212'
            else:
                item = '219'

        # Hardcoded exception
        if product == 100:
            item = '212'

        # Parse dates
        bldate = parse_date(bldate_val)
        issdte = parse_date(issdte_val)
        exprdate = parse_date(exprdate_val)

        if not exprdate:
            continue

        # Calculate days since last billing
        days = 0
        if bldate:
            days = (reptdate - bldate).days

        # Check if expiry is before or at runoff date
        if exprdate <= runoffdt:
            remmth = None
        elif (exprdate - runoffdt).days < 8:
            remmth = 0.1
        else:
            # Process repayment schedule
            # Convert PAYFREQ to FREQ
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
            if payfreq in ('5', '9', ' ') or product in RC_PRODUCTS:
                bldate = exprdate
            elif not bldate or bldate <= datetime(1960, 1, 1).date():
                bldate = issdte if issdte else reptdate
                while bldate <= reptdate:
                    bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)

            if payamt < 0:
                payamt = 0

            if bldate > exprdate or balance <= payamt:
                bldate = exprdate

            # Process payment schedule
            current_balance = balance

            while bldate <= exprdate and current_balance > 0:
                # Calculate remmth for this payment
                if bldate <= runoffdt:
                    remmth = None
                elif (bldate - runoffdt).days < 8:
                    remmth = 0.1
                else:
                    matdt = bldate
                    remmth = calculate_remaining_months(matdt, runoffdt)

                # If remmth > 1 or at expiry, output final balance
                if (remmth is not None and remmth > 1) or bldate == exprdate:
                    amount = current_balance

                    # Adjust remmth for NPL
                    remmth_output = remmth
                    if days > 89 or loanstat != 1:
                        remmth_output = 0.1

                    # Output records (skip FCY for now as commented out)
                    if product not in FCY_PRODUCTS:
                        bnmcode = f"95{item}{cust}{get_rem_format(remmth_output)}0000Y"
                        output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

                    # NPL record
                    remmth_npl = 13 if (days > 89 or loanstat != 1) else remmth
                    if product not in FCY_PRODUCTS:
                        bnmcode = f"93{item}{cust}{get_rem_format(remmth_npl)}0000Y"
                        output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

                    break

                # Output payment
                amount = min(payamt, current_balance)
                current_balance -= amount

                # Adjust remmth for NPL
                remmth_output = remmth
                if days > 89 or loanstat != 1:
                    remmth_output = 0.1

                # Output records
                if product not in FCY_PRODUCTS:
                    bnmcode = f"95{item}{cust}{get_rem_format(remmth_output)}0000Y"
                    output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

                # NPL record
                remmth_npl = 13 if (days > 89 or loanstat != 1) else remmth
                if product not in FCY_PRODUCTS:
                    bnmcode = f"93{item}{cust}{get_rem_format(remmth_npl)}0000Y"
                    output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

                # Next billing date
                bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)

                if bldate > exprdate or current_balance <= payamt:
                    bldate = exprdate

            continue

        # Output final balance for loans expiring before runoff
        amount = balance

        remmth_output = remmth
        if days > 89 or loanstat != 1:
            remmth_output = 0.1

        if product not in FCY_PRODUCTS:
            bnmcode = f"95{item}{cust}{get_rem_format(remmth_output)}0000Y"
            output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

        remmth_npl = 13 if (days > 89 or loanstat != 1) else remmth
        if product not in FCY_PRODUCTS:
            bnmcode = f"93{item}{cust}{get_rem_format(remmth_npl)}0000Y"
            output_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

    print(f"Generated {len(output_records)} output records")

    if output_records:
        df_output = pl.DataFrame(output_records)
    else:
        df_output = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    return df_output


# ============================================================================
# ADD EIR ADJUSTMENTS
# ============================================================================

def add_eir_adjustments(params):
    """Add EIR adjustment records."""
    print("Adding EIR adjustments...")

    wkmon = params['wkmon']
    wkday = params['wkday']

    # Read weekly loan data
    loan_path = Path(str(LOAN_PATH_TEMPLATE).format(wkmon, wkday))

    try:
        df = pl.read_parquet(loan_path)
    except:
        print("Weekly loan file not found, skipping EIR adjustments")
        return pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    # Filter for LN accounts with EIR_ADJ
    df = df.filter(
        (pl.col('ACCTYPE') == 'LN') &
        (pl.col('EIR_ADJ').is_not_null())
    )

    eir_records = []

    for row in df.iter_rows(named=True):
        custcd = row.get('CUSTCD', '')
        product = row.get('PRODUCT')
        eir_adj = row.get('EIR_ADJ')

        if eir_adj is None:
            continue

        # Determine CUST
        if custcd in ('77', '78', '95', '96'):
            cust = '08'
        else:
            cust = '09'

        # Classify product
        prod = get_liq_prod_format(product)

        # Determine ITEM
        if custcd in ('77', '78', '95', '96'):
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

        # Hardcoded exception
        if product == 100:
            item = '212'

        # Output EIR records with bucket '06' (> 1 year)
        eir_records.append({'BNMCODE': f"95{item}{cust}060000Y", 'AMOUNT': eir_adj})
        eir_records.append({'BNMCODE': f"93{item}{cust}060000Y", 'AMOUNT': eir_adj})

    if eir_records:
        return pl.DataFrame(eir_records)
    else:
        return pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})


# ============================================================================
# PROCESS REVOLVING CREDITS
# ============================================================================

def process_revolving_credits(df_note):
    """Process revolving credit special rules."""
    print("Processing revolving credits...")

    # Delete 93212 records
    df_note = df_note.filter(
        ~pl.col('BNMCODE').str.slice(0, 5).eq('93212')
    )

    # Find 95212 records
    df_rc = df_note.filter(
        pl.col('BNMCODE').str.slice(0, 5).eq('95212')
    )

    if len(df_rc) > 0:
        # Create corresponding 93212 records
        df_rc = df_rc.with_columns(
            (pl.lit('93212') + pl.col('BNMCODE').str.slice(5, 9)).alias('BNMCODE')
        )

        # Combine
        df_note = pl.concat([df_note, df_rc])

    return df_note


# ============================================================================
# CREATE DEFAULT RECORDS
# ============================================================================

def create_default_records():
    """Create default zero-amount records for all combinations."""
    print("Creating default records...")

    default_records = []

    for n in range(1, 7):
        bucket = f"{n:02d}"

        # RM (Ringgit Malaysia) records
        for prefix in ['93', '95']:
            for item in ['211', '212', '213', '214', '215', '219']:
                for cust in ['08', '09']:
                    # Skip certain combinations
                    if item in ['214', '215'] and cust == '09':
                        continue
                    if item == '213' and cust == '08':
                        continue

                    bnmcode = f"{prefix}{item}{cust}{bucket}0000Y"
                    default_records.append({'BNMCODE': bnmcode, 'AMOUNT': 0.0})

        # FCY (Foreign Currency) records - commented out in SAS but included for completeness
        for prefix in ['94', '96']:
            for item in ['211', '212', '213', '214', '215', '219']:
                for cust in ['08', '09']:
                    if item in ['214', '215'] and cust == '09':
                        continue
                    if item == '213' and cust == '08':
                        continue

                    bnmcode = f"{prefix}{item}{cust}{bucket}0000Y"
                    default_records.append({'BNMCODE': bnmcode, 'AMOUNT': 0.0})

    return pl.DataFrame(default_records)


# ============================================================================
# AGGREGATE AND OUTPUT
# ============================================================================

def aggregate_output(df_note):
    """Aggregate output by BNMCODE."""
    print("Aggregating output...")

    if len(df_note) == 0:
        print("No records to aggregate")
        return df_note

    # Remove records with bucket '07' (missing remmth)
    df_note = df_note.filter(
        ~pl.col('BNMCODE').str.slice(7, 2).eq('07')
    )

    print(f"After removing bucket 07: {len(df_note)} records")

    # Aggregate by BNMCODE
    df_agg = df_note.group_by('BNMCODE').agg([
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
    """Generate report output."""
    print("Generating report...")

    with open(NOTE_REPORT_PATH, 'w') as f:
        # Header with ASA carriage control
        f.write(' ' + '=' * 80 + '\n')
        f.write(' LIQUIDITY FRAMEWORK - CONTRACTUAL RUN-OFFS REPORT\n')
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

    print(f"Report written to {NOTE_REPORT_PATH}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    try:
        print("=" * 80)
        print("Liquidity Framework - Contractual Run-Offs Processor")
        print("=" * 80)

        # Read reptdate
        params = read_reptdate()

        # Process loan data
        df_loan = process_loan_data(params)

        # Add EIR adjustments
        df_eir = add_eir_adjustments(params)

        # Combine loan and EIR data
        df_note = pl.concat([df_loan, df_eir])

        # Process revolving credits
        df_note = process_revolving_credits(df_note)

        # Create default records
        df_default = create_default_records()

        # Combine with defaults
        df_note = pl.concat([df_default, df_note])

        # Aggregate output
        df_agg = aggregate_output(df_note)

        # Write parquet output
        df_agg.write_parquet(NOTE_OUTPUT_PATH)
        print(f"Parquet output written to {NOTE_OUTPUT_PATH}")

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
