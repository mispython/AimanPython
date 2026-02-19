"""
Program: EIBMNRCC
Purpose: Overdrawn Current Accounts (NRCC) Report
         - Reports overdrawn current accounts for customer codes 63 and 68
         - Filters accounts where overdraft balance exceeds approved limit
         - Date range filtering based on REPTDATE from two different sources
"""

import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIGURATION - Define all paths early
# ============================================================================

# Input paths
INPUT_PATH_REPTDATE1 = "LOAN1_REPTDATE.parquet"
INPUT_PATH_REPTDATE2 = "LOAN2_REPTDATE.parquet"
INPUT_PATH_RMCA = "TRBL_RMCA{qtr}{yyd}.parquet"  # Template: e.g., TRBL_RMCAQ124.parquet

# Output path
OUTPUT_PATH_REPORT = "EIBMNRCC_REPORT.txt"

# Page configuration
PAGE_LENGTH = 60
LRECL = 133  # 132 + 1 for ASA carriage control

# Filter criteria
CUSTCODE_FILTER = [63, 68]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_asa_line(line_content, carriage_control=' '):
    """Format a line with ASA carriage control character."""
    return f"{carriage_control}{line_content}"


def get_report_date_info():
    """
    Process REPTDATE from two sources to generate macro variables.

    Returns:
        dict: Macro variables including YYD, QTR, RDATE, XDATE, YDATE
    """
    # Process REPTDATE1
    reptdate1_df = pl.read_parquet(INPUT_PATH_REPTDATE1)
    reptdate1_row = reptdate1_df.row(0, named=True)
    reptdate1 = reptdate1_row['REPTDATE']

    mmd = reptdate1.month

    # Determine quarter
    if mmd in [1, 2, 3]:
        qtr = 'Q1'
    elif mmd in [4, 5, 6]:
        qtr = 'Q2'
    elif mmd in [7, 8, 9]:
        qtr = 'Q3'
    else:  # 10, 11, 12
        qtr = 'Q4'

    # YYD = YEAR2 (2-digit year)
    yyd = f"{str(reptdate1.year)[2:]}"

    # RDATE = DDMMYY8.
    rdate = f"{reptdate1.day:02d}/{reptdate1.month:02d}/{str(reptdate1.year)[2:]}"

    # XDATE = Z5. (YYDDD format - Julian date)
    # Calculate day of year
    day_of_year = reptdate1.timetuple().tm_yday
    xdate = f"{str(reptdate1.year)[2:]}{day_of_year:03d}"

    # Process REPTDATE2
    reptdate2_df = pl.read_parquet(INPUT_PATH_REPTDATE2)
    reptdate2_row = reptdate2_df.row(0, named=True)
    reptdate2 = reptdate2_row['REPTDATE']

    # YDATE = Z5. (YYDDD format - Julian date)
    day_of_year2 = reptdate2.timetuple().tm_yday
    ydate = f"{str(reptdate2.year)[2:]}{day_of_year2:03d}"

    return {
        'YYD': yyd,
        'QTR': qtr,
        'RDATE': rdate,
        'XDATE': xdate,
        'YDATE': ydate,
        'REPTDATE1': reptdate1,
        'REPTDATE2': reptdate2
    }


def parse_julian_date(julian_str):
    """
    Parse Julian date in YYDDD format to datetime.

    Args:
        julian_str: String in format YYDDD (e.g., '24001' for Jan 1, 2024)

    Returns:
        datetime object or None if invalid
    """
    if not julian_str or len(julian_str) != 5:
        return None

    try:
        yy = int(julian_str[0:2])
        ddd = int(julian_str[2:5])

        # Convert 2-digit year to 4-digit (using YEARCUTOFF=1950)
        if yy >= 50:
            yyyy = 1900 + yy
        else:
            yyyy = 2000 + yy

        # Create datetime from year and day of year
        base_date = datetime(yyyy, 1, 1)
        target_date = base_date.replace(year=yyyy, month=1, day=1)
        from datetime import timedelta
        result_date = target_date + timedelta(days=ddd - 1)

        return result_date
    except:
        return None


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_eibmnrcc():
    """Main processing function for EIBMNRCC."""
    print("=" * 80)
    print("EIBMNRCC - Overdrawn Current Accounts (NRCC) Report")
    print("=" * 80)

    # Get report date information
    print("Initializing report parameters...")
    macro_vars = get_report_date_info()
    yyd = macro_vars['YYD']
    qtr = macro_vars['QTR']
    rdate = macro_vars['RDATE']
    xdate = macro_vars['XDATE']
    ydate = macro_vars['YDATE']

    print(f"Report Date: {rdate}")
    print(f"Quarter: {qtr}")
    print(f"Year: {yyd}")
    print(f"XDATE (Julian): {xdate}")
    print(f"YDATE (Julian): {ydate}")
    print()

    con = duckdb.connect()

    try:
        # ====================================================================
        # Load RMCA data
        # ====================================================================
        print("Loading RMCA data...")

        rmca_path = INPUT_PATH_RMCA.format(qtr=qtr, yyd=yyd)

        if not Path(rmca_path).exists():
            print(f"Error: RMCA file not found: {rmca_path}")
            # Create empty report
            with open(OUTPUT_PATH_REPORT, 'w') as f:
                write_empty_report(f, rdate)
            return

        # Load data
        rmca_df = con.execute(f"""
            SELECT * FROM read_parquet('{rmca_path}')
            WHERE CUSTCODE IN ({','.join(map(str, CUSTCODE_FILTER))})
            AND LEDGBAL < 0.01
        """).pl()

        print(f"Loaded {len(rmca_df)} records from RMCA")

        # ====================================================================
        # Process data
        # ====================================================================
        print("Processing data...")

        if len(rmca_df) == 0:
            print("No records found matching criteria")
            with open(OUTPUT_PATH_REPORT, 'w') as f:
                write_empty_report(f, rdate)
            return

        # Filter by TRDATE (date range: YDATE < TRDATE <= XDATE)
        # Convert TRDATE to comparable format
        processed_records = []

        for row in rmca_df.iter_rows(named=True):
            trdate = row.get('TRDATE')

            # Convert TRDATE to Julian format for comparison if it's not already
            if isinstance(trdate, str):
                trdate_julian = trdate
            elif isinstance(trdate, datetime):
                day_of_year = trdate.timetuple().tm_yday
                trdate_julian = f"{str(trdate.year)[2:]}{day_of_year:03d}"
            else:
                continue

            # Check if YDATE < TRDATE <= XDATE
            if ydate < trdate_julian <= xdate:
                # Calculate derived fields
                ledgbal = row.get('LEDGBAL', 0) or 0
                apprlimt = row.get('APPRLIMT', 0) or 0

                # NBALAN = LEDGBAL * -1
                nbalan = ledgbal * -1

                # APPRLIMT = ROUND(APPRLIMT, 0.01)
                apprlimt_rounded = round(apprlimt, 2)

                # OVERDRWN = ROUND(NBALAN - APPRLIMT, 0.01)
                overdrwn = round(nbalan - apprlimt_rounded, 2)

                # Filter: OVERDRWN > 0.00
                if overdrwn > 0.00:
                    processed_records.append({
                        'BRANCH': row.get('BRANCH'),
                        'NAME': row.get('NAME'),
                        'ACCTNO': row.get('ACCTNO'),
                        'CUSTCODE': row.get('CUSTCODE'),
                        'PRODUCT': row.get('PRODUCT'),
                        'TRXDATE': row.get('TRXDATE', ''),
                        'TRDATE': trdate_julian,
                        'LEDGBAL': ledgbal,
                        'APPRLIMT': apprlimt_rounded,
                        'OVERDRWN': overdrwn
                    })

        if not processed_records:
            print("No records found with OVERDRWN > 0.00")
            with open(OUTPUT_PATH_REPORT, 'w') as f:
                write_empty_report(f, rdate)
            return

        # Create final dataframe
        rmca_final_df = pl.DataFrame(processed_records)

        # Sort by BRANCH, ACCTNO, TRDATE
        rmca_final_df = rmca_final_df.sort(['BRANCH', 'ACCTNO', 'TRDATE'])

        print(f"Final records to report: {len(rmca_final_df)}")

        # ====================================================================
        # Generate report
        # ====================================================================
        print("Generating report...")

        with open(OUTPUT_PATH_REPORT, 'w') as f:
            generate_proc_report(f, rmca_final_df, rdate)

        print(f"Generated: {OUTPUT_PATH_REPORT}")
        print("EIBMNRCC processing complete")

    finally:
        con.close()


def write_empty_report(f, rdate):
    """Write empty report with headers only."""
    # Write titles
    f.write(format_asa_line('PUBLIC BANK BERHAD: '.center(LRECL - 1), '1') + '\n')
    f.write(format_asa_line(f'OVERDRAWN CURRENT ACCOUNTS (NRCC) AS AT {rdate}'.center(LRECL - 1), ' ') + '\n')
    f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')
    f.write(format_asa_line('No data found.'.ljust(LRECL - 1), ' ') + '\n')


def generate_proc_report(f, df, rdate):
    """
    Generate PROC REPORT style output.
    Mimics PROC REPORT with NOWD HEADSKIP HEADLINE NOCENTER options.

    PROC REPORT options:
    - NOWD: No report window (batch mode)
    - HEADSKIP: Skip line before column headers
    - HEADLINE: Underline column headers
    - NOCENTER: Left-align report

    Column definitions:
    - BRANCH    /  DISPLAY 'BRH'          FORMAT=3.
    - NAME      /  DISPLAY 'NAME'         FORMAT=$15.
    - ACCTNO    /  DISPLAY 'ACCOUNT NO'   FORMAT=10.
    - CUSTCODE  /  DISPLAY 'CUST/CODE'    FORMAT=4.
    - PRODUCT   /  DISPLAY 'TYP'          FORMAT=3.
    - TRXDATE   /  DISPLAY 'TRX/DATE'     FORMAT=$8.
    - LEDGBAL   /  DISPLAY 'OD BALANCE'   FORMAT=12.2
    - APPRLIMT  /  DISPLAY 'APPRLIMT'     FORMAT=12.2
    - OVERDRWN  /  DISPLAY 'OVERDRWN'     FORMAT=12.2
    """

    # Write titles
    f.write(format_asa_line('PUBLIC BANK BERHAD: '.ljust(LRECL - 1), '1') + '\n')
    f.write(format_asa_line(f'OVERDRAWN CURRENT ACCOUNTS (NRCC) AS AT {rdate}'.ljust(LRECL - 1), ' ') + '\n')

    # HEADSKIP: Skip line before headers
    f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')

    # Write column headers (NOCENTER - left aligned)
    # Column headers with proper spacing
    header_line = (
        f"{'BRH':>3}  "
        f"{'NAME':<15}  "
        f"{'ACCOUNT NO':>10}  "
        f"{'CUST':>4}  "  # Split header: CUST/CODE -> CUST
        f"{'TYP':>3}  "
        f"{'TRX':<8}  "  # Split header: TRX/DATE -> TRX
        f"{'OD BALANCE':>12}  "
        f"{'APPRLIMT':>12}  "
        f"{'OVERDRWN':>12}"
    )
    f.write(format_asa_line(header_line.ljust(LRECL - 1), ' ') + '\n')

    # Second line of headers (for split headers)
    header_line2 = (
        f"{'':>3}  "
        f"{'':>15}  "
        f"{'':>10}  "
        f"{'CODE':>4}  "  # Split header: CUST/CODE -> CODE
        f"{'':>3}  "
        f"{'DATE':<8}  "  # Split header: TRX/DATE -> DATE
        f"{'':>12}  "
        f"{'':>12}  "
        f"{'':>12}"
    )
    f.write(format_asa_line(header_line2.ljust(LRECL - 1), ' ') + '\n')

    # HEADLINE: Underline headers
    underline = (
        f"{'-' * 3}  "
        f"{'-' * 15}  "
        f"{'-' * 10}  "
        f"{'-' * 4}  "
        f"{'-' * 3}  "
        f"{'-' * 8}  "
        f"{'-' * 12}  "
        f"{'-' * 12}  "
        f"{'-' * 12}"
    )
    f.write(format_asa_line(underline.ljust(LRECL - 1), ' ') + '\n')

    # Write data rows
    linecnt = 5  # Already wrote 5 lines (title1, title2, blank, header1, header2, underline)

    for row in df.iter_rows(named=True):
        # Check for page break
        if linecnt > PAGE_LENGTH - 5:
            # New page
            f.write(format_asa_line('PUBLIC BANK BERHAD: '.ljust(LRECL - 1), '1') + '\n')
            f.write(format_asa_line(f'OVERDRAWN CURRENT ACCOUNTS (NRCC) AS AT {rdate}'.ljust(LRECL - 1), ' ') + '\n')
            f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')

            # Re-write headers
            f.write(format_asa_line(header_line.ljust(LRECL - 1), ' ') + '\n')
            f.write(format_asa_line(header_line2.ljust(LRECL - 1), ' ') + '\n')
            f.write(format_asa_line(underline.ljust(LRECL - 1), ' ') + '\n')

            linecnt = 6

        # Format each field according to DEFINE statements
        branch = row.get('BRANCH', 0)
        name = str(row.get('NAME', ''))[:15]
        acctno = row.get('ACCTNO', 0)
        custcode = row.get('CUSTCODE', 0)
        product = row.get('PRODUCT', 0)
        trxdate = str(row.get('TRXDATE', ''))[:8]
        ledgbal = row.get('LEDGBAL', 0.0)
        apprlimt = row.get('APPRLIMT', 0.0)
        overdrwn = row.get('OVERDRWN', 0.0)

        # Format data line
        data_line = (
            f"{branch:>3}  "
            f"{name:<15}  "
            f"{acctno:>10}  "
            f"{custcode:>4}  "
            f"{product:>3}  "
            f"{trxdate:<8}  "
            f"{ledgbal:>12.2f}  "
            f"{apprlimt:>12.2f}  "
            f"{overdrwn:>12.2f}"
        )

        f.write(format_asa_line(data_line.ljust(LRECL - 1), ' ') + '\n')
        linecnt += 1


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    process_eibmnrcc()
