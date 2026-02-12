#!/usr/bin/env python3
"""
Program: FALWPBBP
To process FDWKLY to produce BIC items for RDAL I
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import calendar

# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Define macro variables (these should be set according to your environment)
REPTMON = "202401"  # Example: Report month
NOWK = "01"  # Example: Week number
RDATE = "31012024"  # Report date in DDMMYYYY format

# Define paths
# BASE_PATH = Path("/data")
BASE_PATH = Path(__file__).resolve().parent

BNM_PATH = BASE_PATH / "bnm"
OUTPUT_PATH = BASE_PATH / "output"

# Input files
UMA_FILE = BNM_PATH / "uma.parquet"
FDWKLY_FILE = BNM_PATH / f"fdwkly{REPTMON}{NOWK}.parquet"

# Output file
FALW_PARQUET = BNM_PATH / f"falw{REPTMON}{NOWK}.parquet"

# Ensure output directories exist
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
BNM_PATH.mkdir(parents=True, exist_ok=True)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_date_ddmmyyyy(date_str):
    """Parse date in DDMMYYYY format"""
    day = int(date_str[:2])
    month = int(date_str[2:4])
    year = int(date_str[4:8])
    return date(year, month, day)


def parse_date_yyyymmdd(date_int):
    """Parse date in YYYYMMDD format (as integer)"""
    date_str = str(date_int).zfill(8)
    year = int(date_str[:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])
    return date(year, month, day)


def days_in_month(year, month):
    """Get number of days in a month"""
    return calendar.monthrange(year, month)[1]


def calculate_remaining_months(matdate_int, report_date):
    """
    Calculate remaining months from report date to maturity date
    Similar to SAS REMMTH calculation
    """
    if matdate_int is None or matdate_int == 0:
        return None

    try:
        fd_date = parse_date_yyyymmdd(matdate_int)
    except:
        return None

    fd_year = fd_date.year
    fd_month = fd_date.month
    fd_day = fd_date.day

    rp_year = report_date.year
    rp_month = report_date.month
    rp_day = report_date.day

    # Get days in report month
    rp_days_in_month = days_in_month(rp_year, rp_month)

    # Adjust fd_day if it exceeds days in report month
    if fd_day > rp_days_in_month:
        fd_day = rp_days_in_month

    # Calculate differences
    rem_years = fd_year - rp_year
    rem_months = fd_month - rp_month
    rem_days = fd_day - rp_day

    # Calculate total months with fractional component
    remmth = rem_years * 12 + rem_months + rem_days / rp_days_in_month

    return remmth


def map_bnmcode_custcode(custcode, bic):
    """
    Map customer code to BNMCODE for FD accepted by customer code
    Returns list of BNMCODEs
    """
    results = []

    if custcode == '01':
        results.append(f'{bic}01000000Y')
    elif custcode in ['10', '02', '03', '11', '12', '07', '17', '57', '75']:
        results.append(f'{bic}{custcode}000000Y')
    elif custcode in ['20', '13', '30', '31', '04', '05', '06', '32', '33', '34', '35',
                      '36', '37', '38', '39', '40', '45']:
        results.append(f'{bic}20000000Y')
    elif custcode in ['60', '61', '62', '63', '64', '65', '66', '67', '68', '69',
                      '41', '42', '43', '44', '46', '47', '48', '49', '51',
                      '52', '53', '54', '59']:
        results.append(f'{bic}60000000Y')
    elif custcode in ['70', '71', '72', '73', '74']:
        results.append(f'{bic}{custcode}000000Y')
    elif custcode in ['76', '77', '78']:
        results.append(f'{bic}76000000Y')
    elif custcode == '79':
        results.append(f'{bic}79000000Y')
    elif custcode in ['81', '82', '83', '84']:
        results.append(f'{bic}81000000Y')
    elif custcode in ['85', '86', '87', '88', '89', '90', '91', '92', '95', '96', '98', '99']:
        results.append(f'{bic}85000000Y')

    return results


def map_bnmcode_orgmat(custcode, bic, om):
    """
    Map customer code to BNMCODE for FD by original maturity
    """
    results = []

    if custcode in ['01', '02', '03', '11', '12', '79', '57', '75']:
        results.append(f'{bic}{custcode}{om}0000Y')
    elif custcode in ['20', '13', '17', '30', '31', '04', '05', '06', '32', '33', '34', '35',
                      '36', '37', '38', '39', '40', '45']:
        results.append(f'{bic}20{om}0000Y')
    elif custcode in ['60', '61', '62', '63', '64', '65', '66', '67', '68', '69',
                      '41', '42', '43', '44', '46', '47', '48', '49', '51',
                      '52', '53', '54', '59']:
        results.append(f'{bic}60{om}0000Y')
    elif custcode in ['70', '71', '72', '73', '74']:
        results.append(f'{bic}70{om}0000Y')
    elif custcode in ['76', '77', '78']:
        results.append(f'{bic}76{om}0000Y')
    elif custcode in ['80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91',
                      '92', '95', '96', '98', '99']:
        results.append(f'{bic}80{om}0000Y')

    return results


def map_bnmcode_remmat(custcode, bic, rm):
    """
    Map customer code to BNMCODE for FD by remaining maturity
    """
    results = []

    if custcode in ['01', '02', '11', '12', '79', '03', '57', '75']:
        results.append(f'{bic}{custcode}{rm}0000Y')
    elif custcode in ['20', '13', '17', '30', '31', '04', '05', '06', '32', '33', '34', '35',
                      '36', '37', '38', '39', '40', '45']:
        results.append(f'{bic}20{rm}0000Y')
    elif custcode in ['60', '61', '62', '63', '64', '65', '66', '67', '68', '69',
                      '41', '42', '43', '44', '46', '47', '48', '49', '51',
                      '52', '53', '54', '59']:
        results.append(f'{bic}60{rm}0000Y')
    elif custcode in ['70', '71', '72', '73', '74']:
        results.append(f'{bic}70{rm}0000Y')
    elif custcode in ['76', '77', '78']:
        results.append(f'{bic}76{rm}0000Y')
    elif custcode in ['80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91',
                      '92', '95', '96', '98', '99']:
        results.append(f'{bic}80{rm}0000Y')

    return results


def format_fdorgmt(term):
    """
    Format original maturity based on term
    This simulates the FDORGMT format
    """
    if term is None:
        return '99'

    if term <= 1:
        return '01'
    elif term <= 3:
        return '03'
    elif term <= 6:
        return '06'
    elif term <= 12:
        return '12'
    elif term <= 24:
        return '24'
    elif term <= 36:
        return '36'
    elif term <= 60:
        return '60'
    else:
        return '61'


def format_rmfdorgmt(intplan):
    """
    Format original maturity based on interest plan
    This simulates the RMFDORGMT format for intplan 272-295
    """
    if intplan is None:
        return '99'

    # Mapping for intplan values 272-295
    intplan_map = {
        272: '01', 273: '01', 274: '03', 275: '03',
        276: '06', 277: '06', 278: '12', 279: '12',
        280: '24', 281: '24', 282: '36', 283: '36',
        284: '60', 285: '60', 286: '61', 287: '61',
        288: '01', 289: '01', 290: '03', 291: '03',
        292: '06', 293: '06', 294: '12', 295: '12'
    }

    return intplan_map.get(intplan, '99')


def format_fdrmmt(remmth):
    """
    Format remaining maturity in months
    This simulates the FDRMMT format
    """
    if remmth is None:
        return '99'

    if remmth <= 1:
        return '01'
    elif remmth <= 3:
        return '03'
    elif remmth <= 6:
        return '06'
    elif remmth <= 12:
        return '12'
    elif remmth <= 24:
        return '24'
    elif remmth <= 36:
        return '36'
    elif remmth <= 60:
        return '60'
    else:
        return '61'


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    """Main processing function"""

    conn = duckdb.connect()

    # Parse report date
    report_date = parse_date_ddmmyyyy(RDATE)

    # Initialize list to collect all ALWDEPT dataframes
    all_alwdept = []

    print("Starting FALWPBBP processing...")

    # ========================================================================
    # SECTION 1: RM FD ACCEPTED BY CUSTOMER CODE
    # ========================================================================

    print("Processing RM FD ACCEPTED BY CUSTOMER CODE...")

    print("UMA_FILE path:", UMA_FILE)
    print("Exists?", UMA_FILE.exists())

    # Combine UMA and FDWKLY datasets
    fdwkly = conn.execute(f"""
        SELECT * FROM read_parquet('{UMA_FILE}')
        UNION ALL
        SELECT * FROM read_parquet('{FDWKLY_FILE}')
    """).pl()

    # Aggregate by STATE, BIC, CUSTCODE, AMTIND
    alw = fdwkly.group_by(['state', 'bic', 'custcode', 'amtind']).agg(
        pl.col('curbal').sum().alias('amount')
    )

    # Generate BNMCODE mappings
    alwdept_rows = []
    for row in alw.iter_rows(named=True):
        bic = row['bic']
        custcode = row['custcode']
        amtind = row['amtind']
        amount = row['amount'] if row['amount'] is not None else 0.0

        bnmcodes = map_bnmcode_custcode(custcode, bic)

        for bnmcode in bnmcodes:
            alwdept_rows.append({
                'bnmcode': bnmcode,
                'amount': amount,
                'amtind': amtind
            })

    if alwdept_rows:
        alwdept = pl.DataFrame(alwdept_rows)

        # Aggregate by BNMCODE and AMTIND
        alwdept = alwdept.group_by(['bnmcode', 'amtind']).agg(
            pl.col('amount').sum()
        )

        all_alwdept.append(alwdept)

    # ========================================================================
    # SECTION 2: RM INTEREST PAYABLE NIE TO NON-RESIDENTS
    # ========================================================================

    print("Processing RM INTEREST PAYABLE NIE TO NON-RESIDENTS...")

    print("FDWKLY_FILE path:", FDWKLY_FILE)
    print("Exists?", FDWKLY_FILE.exists())

    # Load FDWKLY for interest payable
    alw = conn.execute(f"""
        SELECT bic, custcode, amtind, SUM(intpay) as amount
        FROM read_parquet('{FDWKLY_FILE}')
        GROUP BY bic, custcode, amtind
    """).pl()

    alwdept_rows = []
    for row in alw.iter_rows(named=True):
        bic = row['bic']
        custcode = row['custcode']
        amtind = row['amtind']
        amount = row['amount']

        bnmcode = None

        if custcode in ['80', '81', '82', '83', '84', '85', '86', '87', '88', '89',
                        '90', '91', '92', '95', '96', '98', '99']:
            if bic in ['42130', '42133']:
                bnmcode = '4911080000000Y'
            elif bic in ['42132', '42199']:
                bnmcode = '4929980000000Y'
        else:
            if bic in ['42130', '42133']:
                bnmcode = '4911050000000Y'
            elif bic in ['42132', '42199']:
                bnmcode = '4929950000000Y'

        if bnmcode:
            alwdept_rows.append({
                'bnmcode': bnmcode,
                'amount': amount,
                'amtind': amtind
            })

    if alwdept_rows:
        alwdept = pl.DataFrame(alwdept_rows)
        all_alwdept.append(alwdept)

    # ========================================================================
    # SECTION 3: RM FD ACCEPTED BY ORIGINAL MATURITY
    # ========================================================================

    print("Processing RM FD ACCEPTED BY ORIGINAL MATURITY...")

    print("FDWKLY_FILE path:", FDWKLY_FILE)
    print("Exists?", FDWKLY_FILE.exists())

    alw = conn.execute(f"""
        SELECT bic, openind, term, intplan, custcode, amtind, SUM(curbal) as amount
        FROM read_parquet('{FDWKLY_FILE}')
        WHERE bic IN ('42130','42132','42133','42199')
        GROUP BY bic, openind, term, intplan, custcode, amtind
    """).pl()

    alwdept_rows = []
    for row in alw.iter_rows(named=True):
        bic = row['bic']
        custcode = row['custcode']
        amtind = row['amtind']
        amount = row['amount']
        intplan = row['intplan']
        term = row['term']

        # Determine original maturity code
        if intplan is not None and 272 <= intplan <= 295:
            om = format_rmfdorgmt(intplan)
        else:
            om = format_fdorgmt(term)

        bnmcodes = map_bnmcode_orgmat(custcode, bic, om)

        for bnmcode in bnmcodes:
            alwdept_rows.append({
                'bnmcode': bnmcode,
                'amount': amount,
                'amtind': amtind
            })

    if alwdept_rows:
        alwdept = pl.DataFrame(alwdept_rows)

        # Aggregate by BNMCODE and AMTIND
        alwdept = alwdept.group_by(['bnmcode', 'amtind']).agg(
            pl.col('amount').sum()
        )

        all_alwdept.append(alwdept)

    # ========================================================================
    # SECTION 4: RM FD ACCEPTED BY REMAINING MATURITY
    # ========================================================================

    print("Processing RM FD ACCEPTED BY REMAINING MATURITY...")

    print("FDWKLY_FILE path:", FDWKLY_FILE)
    print("Exists?", FDWKLY_FILE.exists())

    # Load data and calculate remaining months
    fdwkly_remmat = conn.execute(f"""
        SELECT bic, custcode, matdate, curbal, amtind
        FROM read_parquet('{FDWKLY_FILE}')
        WHERE bic IN ('42130','42132','42133','42199','42630')
    """).pl()

    # Calculate remaining months for each record
    alw_rows = []
    for row in fdwkly_remmat.iter_rows(named=True):
        matdate = row['matdate']
        remmth = calculate_remaining_months(matdate, report_date)

        if remmth is not None:
            alw_rows.append({
                'bic': row['bic'],
                'custcode': row['custcode'],
                'remmth': remmth,
                'curbal': row['curbal'],
                'amtind': row['amtind']
            })

    if alw_rows:
        alw = pl.DataFrame(alw_rows)

        # Aggregate by BIC, CUSTCODE, REMMTH, AMTIND
        alw = alw.group_by(['bic', 'custcode', 'remmth', 'amtind']).agg(
            pl.col('curbal').sum().alias('amount')
        )

        # Generate BNMCODE mappings
        alwdept_rows = []
        for row in alw.iter_rows(named=True):
            bic = row['bic']
            custcode = row['custcode']
            amtind = row['amtind']
            amount = row['amount']
            remmth = row['remmth']

            # Format remaining maturity
            rm = format_fdrmmt(remmth)

            bnmcodes = map_bnmcode_remmat(custcode, bic, rm)

            for bnmcode in bnmcodes:
                alwdept_rows.append({
                    'bnmcode': bnmcode,
                    'amount': amount,
                    'amtind': amtind
                })

        if alwdept_rows:
            alwdept = pl.DataFrame(alwdept_rows)
            all_alwdept.append(alwdept)

    # ========================================================================
    # FINAL CONSOLIDATION
    # ========================================================================

    print("Performing final consolidation...")

    if all_alwdept:
        # Combine all ALWDEPT dataframes
        combined_df = pl.concat(all_alwdept)

        # Final aggregation by BNMCODE and AMTIND
        final_df = combined_df.group_by(['bnmcode', 'amtind']).agg(
            pl.col('amount').sum()
        ).sort(['bnmcode', 'amtind'])

        # Save to parquet
        final_df.write_parquet(FALW_PARQUET)
        print(f"Saved consolidated data to {FALW_PARQUET}")

        # Display summary
        print(f"\nProcessing complete!")
        print(f"Total records: {len(final_df)}")
        print(f"Total amount: {final_df['amount'].sum():,.2f}")

    else:
        print("No data to process")

    conn.close()


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()
