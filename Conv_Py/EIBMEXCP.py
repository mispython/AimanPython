# !/usr/bin/env python3
"""
Program: EIBMEXCP
Date: 13.01.2000
Purpose: Exceptional Report on New Product Codes
         - Reports accounts with unexpected/unknown product codes
         - Reports invalid sector/customer code combinations
         - Generates multiple exception reports for:
           * Saving Accounts
           * Current Accounts
           * Fixed Deposits
           * Fixed Deposit CDs
           * Loan Accounts
           * OD Accounts
           * Sector & Customer Code validations
"""

import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIGURATION - Define all paths early
# ============================================================================

# Input paths
INPUT_PATH_REPTDATE = "SASDATA_REPTDATE.parquet"
INPUT_PATH_SAVING = "DEPOSIT_SAVING.parquet"
INPUT_PATH_CURRENT = "DEPOSIT_CURRENT.parquet"
INPUT_PATH_FD_DEPOSIT = "DEPOSIT_FD.parquet"
INPUT_PATH_FD_CD = "FD_FD.parquet"
INPUT_PATH_LNNOTE = "SAP_PBB_MNILN_0_LNNOTE.parquet"
INPUT_PATH_LOAN_SASDATA = "SASDATA_LOAN{reptmon}{nowk}.parquet"  # Template

# Output paths
OUTPUT_PATH_SAVING = "EIBMEXCP_SAVING.txt"
OUTPUT_PATH_CURRENT = "EIBMEXCP_CURRENT.txt"
OUTPUT_PATH_FD = "EIBMEXCP_FD.txt"
OUTPUT_PATH_FDCD = "EIBMEXCP_FDCD.txt"
OUTPUT_PATH_LOAN = "EIBMEXCP_LOAN.txt"
OUTPUT_PATH_SECTOR_CUST = "EIBMEXCP_SECTOR_CUST.txt"
OUTPUT_PATH_OD = "EIBMEXCP_OD.txt"
OUTPUT_PATH_CUSTCODE = "EIBMEXCP_CUSTCODE.txt"

# Page configuration
PAGE_LENGTH = 60
LRECL = 133  # 132 + 1 for ASA carriage control

# ============================================================================
# PRODUCT CODE FORMATS (PROC FORMAT definitions)
# ============================================================================

# VALUE SAPROD (Saving Account products)
SAPROD_VALID = list(range(200, 205)) + list(range(150, 153)) + [177, 181] + list(range(212, 216))

# VALUE FDPROD (Fixed Deposit products)
FDPROD_VALID = list(range(300, 304))

# VALUE FDPRODCD (Fixed Deposit CD products)
FDPRODCD_VALID = (
        list(range(300, 332)) + list(range(340, 360)) + list(range(360, 367)) +
        list(range(370, 373)) + list(range(373, 385)) + list(range(400, 429)) +
        list(range(448, 470)) + list(range(500, 540)) + list(range(580, 640))
)

# VALUE CAPROD (Current Account products)
CAPROD_VALID = (
        list(range(100, 137)) + list(range(140, 153)) + list(range(154, 156)) +
        list(range(156, 159)) + list(range(160, 166)) + list(range(170, 177)) +
        list(range(180, 183))
)

# VALUE LNPROD (Loan products)
LNPROD_VALID = (
        [4, 5, 6, 7, 10, 15, 20] + list(range(25, 35)) + list(range(60, 64)) +
        list(range(70, 79)) + list(range(100, 104)) + list(range(110, 117)) +
        [117, 118] + list(range(120, 126)) + [127, 129, 135, 136, 170, 180, 181, 182, 194, 195] +
        [200, 201, 204, 205] + list(range(209, 213)) + [214, 215, 219, 220] +
        list(range(225, 235)) + [300, 301, 304, 305, 309, 310, 315, 320, 325, 330, 335, 340] +
        [345, 350, 355] + list(range(356, 362)) + [380, 381, 390, 391] +
        [500, 504, 505, 509, 510] + list(range(515, 529)) + [555, 556, 559, 560, 561] +
        list(range(564, 569)) + [570, 573, 900, 901, 904, 905, 910, 914, 915, 919, 920, 925]
)

# VALUE ODPROD (Overdraft products)
ODPROD_VALID = (
        list(range(100, 129)) + list(range(130, 137)) + list(range(140, 153)) +
        list(range(154, 159)) + list(range(160, 166)) + list(range(170, 174)) + [180]
)

# Sector and Customer Code validation rules
SECTOR_CUSTCODE_RULES = {
    '0131': {'excluded': [0, 76, 77, 78]},
    '0132': {'included': [4, 5, 6, 36, 80, 81, 82, 83, 84, 85, 86, 90, 91, 92, 95, 96, 98, 99]},
    ('0200', '0210', '0410', '0420', '0430'): {
        'excluded': [77, 78, 80, 81, 82, 83, 84, 85, 86, 90, 91, 92, 95, 96, 98, 99]
    },
    ('1000', '2000', '3000', '4000', '5000', '6100', '6200', '6300', '7000'): {
        'excluded': [61, 62, 63, 64, 65, 66, 67, 68, 69, 75, 57, 59, 80, 81, 82, 83, 84, 85, 86,
                     90, 91, 92, 95, 96, 98, 99]
    },
    '8100': {
        'excluded': [1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 15, 17, 20, 30, 31, 32, 33, 34, 35, 36, 37,
                     38, 39, 40, 45, 80, 81, 82, 83, 84, 85, 86, 90, 91, 92, 95, 96, 98, 99]
    },
    '8200': {
        'excluded': [1, 2, 4, 5, 6, 13, 15, 17, 20, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 45,
                     80, 81, 82, 83, 84, 85, 86, 90, 91, 92, 95, 96, 98, 99]
    },
    ('8310', '8320', '8330'): {
        'excluded': [61, 62, 63, 64, 65, 75, 57, 59, 80, 81, 82, 83, 84, 85, 86, 90, 91, 92, 95,
                     96, 98, 99]
    },
    ('8000', '9999'): {
        'excluded': [61, 62, 63, 64, 65, 66, 67, 68, 69, 75, 57, 59, 70, 79, 80, 81, 82, 83, 84,
                     85, 86, 90, 91, 92, 95, 96, 98, 99]
    }
}

# Customer Code to Sector validation (HHH logic)
CUSTCODE_SECTOR_RULES = {
    (2, 3, 11, 12, 13, 17, 30, 31, 32, 33, 34, 35, 4, 5, 6, 37, 38, 39, 46, 47): {
        'invalid_sectors': ['0200', '0410', '0420', '0430', '1000', '2000', '3000', '4000',
                            '5000', '6000', '7000', '8200', '8300', '9000', '9999']
    },
    (40,): {
        'invalid_sectors': ['0200', '0410', '0420', '0430', '1000', '2000', '3000', '4000',
                            '5000', '6000', '7000', '8300', '9000', '9999']
    },
    (79,): {
        'invalid_sectors': ['0200', '0410', '0420', '0430', '1000', '2000', '3000', '4000',
                            '5000', '6000', '7000', '8000']
    },
    (71, 72, 73, 74): {
        'invalid_sectors': ['0200', '0410', '0420', '0430', '1000', '2000', '3000', '4000',
                            '5000', '6000', '7000', '8000', '9200', '9300', '9400', '9500', '9600']
    }
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_asa_line(line_content, carriage_control=' '):
    """Format a line with ASA carriage control character."""
    return f"{carriage_control}{line_content}"


def get_report_date_info():
    """Process REPTDATE to generate macro variables."""
    reptdate_df = pl.read_parquet(INPUT_PATH_REPTDATE)
    reptdate_row = reptdate_df.row(0, named=True)
    reptdate = reptdate_row['REPTDATE']

    day = reptdate.day

    # SELECT(DAY(REPTDATE))
    if day == 8:
        sdd = 1
        wk = '1'
        wk1 = '4'
    elif day == 15:
        sdd = 9
        wk = '2'
        wk1 = '1'
    elif day == 22:
        sdd = 16
        wk = '3'
        wk1 = '2'
    else:
        sdd = 23
        wk = '4'
        wk1 = '3'

    mm = reptdate.month

    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    sdate = datetime(reptdate.year, mm, sdd)
    udate = datetime(reptdate.year, mm, reptdate.day)

    rdate = f"{reptdate.day:02d}/{reptdate.month:02d}/{str(reptdate.year)[2:]}"
    sdate_str = f"{sdate.day:02d}/{sdate.month:02d}/{str(sdate.year)[2:]}"

    return {
        'NOWK': wk,
        'NOWK1': wk1,
        'RDATE': rdate,
        'SDATE': sdate_str,
        'UDATE': udate,
        'REPTMON': f"{mm:02d}",
        'REPTDAY': f"{reptdate.day:02d}",
        'REPTMON1': f"{mm1:02d}",
        'REPTYEAR': f"{reptdate.year:04d}"
    }


def check_sector_custcode(sector, custcode):
    """Check if sector/custcode combination is invalid (FFF logic)."""
    for sector_key, rules in SECTOR_CUSTCODE_RULES.items():
        # Handle both single sector and tuple of sectors
        sectors = sector_key if isinstance(sector_key, tuple) else (sector_key,)

        if sector in sectors:
            if 'excluded' in rules:
                if custcode not in rules['excluded']:
                    return True  # Invalid combination
            elif 'included' in rules:
                if custcode in rules['included']:
                    return True  # Invalid combination
            return False

    return False


def check_custcode_sector(custcode, sector):
    """Check if custcode/sector combination is invalid (HHH logic)."""
    for custcode_key, rules in CUSTCODE_SECTOR_RULES.items():
        if custcode in custcode_key:
            if sector in rules['invalid_sectors']:
                return True  # Invalid combination
            return False

    return False


def write_proc_print_report(f, df, title1, title2, title3, by_var, pageby_var):
    """
    Generate PROC PRINT style report.

    Args:
        f: File handle
        df: Polars DataFrame
        title1, title2, title3: Report titles
        by_var: BY variable for grouping
        pageby_var: PAGEBY variable for page breaks
    """
    if len(df) == 0:
        # Write header even if no data
        f.write(format_asa_line(title1.center(LRECL - 1), '1') + '\n')
        f.write(format_asa_line(title2.center(LRECL - 1), ' ') + '\n')
        f.write(format_asa_line(title3.center(LRECL - 1), ' ') + '\n')
        f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')
        f.write(format_asa_line('No observations found.'.ljust(LRECL - 1), ' ') + '\n')
        return

    # Get column names
    cols = df.columns

    prev_by_value = None
    obs_num = 0

    for idx, row in enumerate(df.iter_rows(named=True)):
        by_value = row[by_var]

        # Check for page break (PAGEBY)
        if by_value != prev_by_value:
            # Write page header
            f.write(format_asa_line(title1.center(LRECL - 1), '1') + '\n')
            f.write(format_asa_line(title2.center(LRECL - 1), ' ') + '\n')
            f.write(format_asa_line(title3.center(LRECL - 1), ' ') + '\n')
            f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')

            # Write BY line
            f.write(format_asa_line(
                f"-------------------------------- {by_var}={by_value} --------------------------------".ljust(
                    LRECL - 1), ' ') + '\n')
            f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')

            # Write column headers
            header_line = f"{'OBS':>6}  " + "  ".join([f"{col:>12}" for col in cols if col != by_var])
            f.write(format_asa_line(header_line.ljust(LRECL - 1), ' ') + '\n')
            f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')

            obs_num = 0

        obs_num += 1

        # Write data line
        data_parts = [f"{obs_num:>6}  "]
        for col in cols:
            if col == by_var:
                continue

            val = row[col]
            if val is None:
                val_str = '.'
            elif isinstance(val, float):
                val_str = f"{val:12.2f}"
            elif isinstance(val, int):
                val_str = f"{val:>12}"
            elif isinstance(val, datetime):
                val_str = f"{val.strftime('%d/%m/%y'):>12}"
            else:
                val_str = f"{str(val):>12}"

            data_parts.append(f"{val_str:>12}  ")

        data_line = ''.join(data_parts)
        f.write(format_asa_line(data_line.ljust(LRECL - 1), ' ') + '\n')

        prev_by_value = by_value


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_eibmexcp():
    """Main processing function for EIBMEXCP."""
    print("=" * 80)
    print("EIBMEXCP - Exceptional Report on Product Codes")
    print("=" * 80)

    # Get report date information
    print("Initializing report parameters...")
    macro_vars = get_report_date_info()
    rdate = macro_vars['RDATE']
    nowk = macro_vars['NOWK']
    reptmon = macro_vars['REPTMON']
    print(f"Report Date: {rdate}")
    print()

    con = duckdb.connect()

    try:
        # ====================================================================
        # AAA - Exceptional SAVING accounts
        # ====================================================================
        print("Processing Saving Accounts exceptions...")

        saving_df = con.execute(f"""
            SELECT BRANCH, ACCTNO, NAME, OPENDT, CURBAL, PRODUCT
            FROM read_parquet('{INPUT_PATH_SAVING}')
            WHERE OPENIND NOT IN ('B', 'C', 'P')
            AND CURBAL >= 0
        """).pl()

        # Filter for unknown products
        aaa_df = saving_df.filter(~pl.col('PRODUCT').is_in(SAPROD_VALID))

        if len(aaa_df) > 0:
            aaa_df = aaa_df.sort(['PRODUCT', 'BRANCH', 'ACCTNO'])

        # Generate report
        with open(OUTPUT_PATH_SAVING, 'w') as f:
            write_proc_print_report(
                f, aaa_df,
                'PUBLIC BANK BERHAD',
                'REPORT ID : EIBMEXCP',
                f'EXCEPTIONAL REPORT ON SAVING ACCOUNTS AS AT : {rdate}',
                'PRODUCT', 'PRODUCT'
            )

        print(f"  Generated: {OUTPUT_PATH_SAVING} ({len(aaa_df)} records)")

        # ====================================================================
        # BBB - Exceptional CURRENT accounts
        # ====================================================================
        print("Processing Current Accounts exceptions...")

        current_df = con.execute(f"""
            SELECT BRANCH, ACCTNO, NAME, OPENDT, CURBAL, PRODUCT
            FROM read_parquet('{INPUT_PATH_CURRENT}')
            WHERE OPENIND NOT IN ('B', 'C', 'P')
        """).pl()

        bbb_df = current_df.filter(~pl.col('PRODUCT').is_in(CAPROD_VALID))

        if len(bbb_df) > 0:
            bbb_df = bbb_df.sort(['PRODUCT', 'BRANCH', 'ACCTNO'])

        with open(OUTPUT_PATH_CURRENT, 'w') as f:
            write_proc_print_report(
                f, bbb_df,
                'PUBLIC BANK BERHAD',
                'REPORT ID : EIBMEXCP',
                f'EXCEPTIONAL REPORT ON CURRENT ACCOUNTS AS AT : {rdate}',
                'PRODUCT', 'PRODUCT'
            )

        print(f"  Generated: {OUTPUT_PATH_CURRENT} ({len(bbb_df)} records)")

        # ====================================================================
        # CCC - Exceptional FIXED DEPOSITS (account level)
        # ====================================================================
        print("Processing Fixed Deposit exceptions...")

        fd_deposit_df = con.execute(f"""
            SELECT BRANCH, ACCTNO, NAME, OPENDT, CURBAL, PRODUCT
            FROM read_parquet('{INPUT_PATH_FD_DEPOSIT}')
            WHERE OPENIND NOT IN ('B', 'C', 'P')
            AND CURBAL >= 0
        """).pl()

        ccc_df = fd_deposit_df.filter(~pl.col('PRODUCT').is_in(FDPROD_VALID))

        if len(ccc_df) > 0:
            ccc_df = ccc_df.sort(['PRODUCT', 'BRANCH', 'ACCTNO'])

        with open(OUTPUT_PATH_FD, 'w') as f:
            write_proc_print_report(
                f, ccc_df,
                'PUBLIC BANK BERHAD',
                'REPORT ID : EIBMEXCP',
                f'EXCEPTIONAL REPORT ON FIXED DEPOSITS AS AT : {rdate}',
                'PRODUCT', 'PRODUCT'
            )

        print(f"  Generated: {OUTPUT_PATH_FD} ({len(ccc_df)} records)")

        # ====================================================================
        # DDD - Exceptional FIXED DEPOSIT CDs
        # ====================================================================
        print("Processing Fixed Deposit CD exceptions...")

        fd_cd_df = con.execute(f"""
            SELECT BRANCH, ACCTNO, CDNO, NAME, ORGDATE, MATDATE, CURBAL, INTPLAN
            FROM read_parquet('{INPUT_PATH_FD_CD}')
            WHERE OPENIND IN ('O', 'D')
        """).pl()

        ddd_df = fd_cd_df.filter(~pl.col('INTPLAN').is_in(FDPRODCD_VALID))

        if len(ddd_df) > 0:
            ddd_df = ddd_df.sort(['INTPLAN', 'BRANCH', 'ACCTNO', 'CDNO'])

        with open(OUTPUT_PATH_FDCD, 'w') as f:
            write_proc_print_report(
                f, ddd_df,
                'PUBLIC BANK BERHAD',
                'REPORT ID : EIBMEXCP',
                f'EXCEPTIONAL REPORT ON FIXED DEPOSITS CD LEVELS AS AT : {rdate}',
                'INTPLAN', 'INTPLAN'
            )

        print(f"  Generated: {OUTPUT_PATH_FDCD} ({len(ddd_df)} records)")

        # ====================================================================
        # EEE - Exceptional LOAN accounts
        # ====================================================================
        print("Processing Loan Account exceptions...")

        loan_df = con.execute(f"""
            SELECT ACCBRCH, ACCTNO, NOTENO, NAME, CURBAL, LOANTYPE, GRANTDT, NOTEMAT
            FROM read_parquet('{INPUT_PATH_LNNOTE}')
            WHERE (REVERSED <> 'Y' OR REVERSED IS NULL)
            AND NOTENO IS NOT NULL
            AND (PAIDIND <> 'P' OR PAIDIND IS NULL)
        """).pl()

        eee_df = loan_df.filter(~pl.col('LOANTYPE').is_in(LNPROD_VALID))

        if len(eee_df) > 0:
            eee_df = eee_df.sort(['LOANTYPE', 'ACCBRCH', 'ACCTNO'])

        with open(OUTPUT_PATH_LOAN, 'w') as f:
            write_proc_print_report(
                f, eee_df,
                'PUBLIC BANK BERHAD',
                'REPORT ID : EIBMEXCP',
                f'EXCEPTIONAL REPORT ON LOANS ACCOUNTS AS AT : {rdate}',
                'LOANTYPE', 'LOANTYPE'
            )

        print(f"  Generated: {OUTPUT_PATH_LOAN} ({len(eee_df)} records)")

        # ====================================================================
        # FFF - Invalid SECTOR & CUSTOMER CODE combinations on LOANS
        # ====================================================================
        print("Processing Sector/Customer Code exceptions...")

        loan_full_df = con.execute(f"""
            SELECT ACCBRCH AS BRANCH, ACCTNO, NOTENO, NAME, CURBAL, LOANTYPE, 
                   GRANTDT, NOTEMAT, CUSTCODE, SECTOR
            FROM read_parquet('{INPUT_PATH_LNNOTE}')
            WHERE (REVERSED <> 'Y' OR REVERSED IS NULL)
            AND NOTENO IS NOT NULL
            AND (PAIDIND <> 'P' OR PAIDIND IS NULL)
        """).pl()

        # Apply sector/custcode validation
        fff_records = []
        for row in loan_full_df.iter_rows(named=True):
            sector = row['SECTOR']
            custcode = row['CUSTCODE']

            if check_sector_custcode(sector, custcode):
                fff_records.append(row)

        fff_df = pl.DataFrame(fff_records) if fff_records else pl.DataFrame()

        if len(fff_df) > 0:
            fff_df = fff_df.sort(['BRANCH', 'LOANTYPE', 'ACCTNO'])

        with open(OUTPUT_PATH_SECTOR_CUST, 'w') as f:
            write_proc_print_report(
                f, fff_df,
                'LIST OF INVALID CODES OF LOANS BY CUSTOMER & BY SECTOR',
                'REPORT ID : EIBMEXCP',
                f'PRESENTLY MAINTAINED IN THE M&I SYSTEM AS AT : {rdate}',
                'BRANCH', 'BRANCH'
            )

        print(f"  Generated: {OUTPUT_PATH_SECTOR_CUST} ({len(fff_df)} records)")

        # ====================================================================
        # GGG - Exceptional OD accounts from SASDATA
        # ====================================================================
        print("Processing OD Account exceptions...")

        loan_sasdata_path = INPUT_PATH_LOAN_SASDATA.format(reptmon=reptmon, nowk=nowk)

        if Path(loan_sasdata_path).exists():
            od_df = con.execute(f"""
                SELECT BRANCH, ACCTNO, NAME, CUSTCODE, SECTORCD, CURBAL, PRODUCT
                FROM read_parquet('{loan_sasdata_path}')
                WHERE ACCTYPE = 'OD'
            """).pl()

            ggg_df = od_df.filter(~pl.col('PRODUCT').is_in(ODPROD_VALID))

            if len(ggg_df) > 0:
                ggg_df = ggg_df.sort(['PRODUCT', 'BRANCH', 'ACCTNO'])
        else:
            print(f"  Warning: SASDATA file not found: {loan_sasdata_path}")
            ggg_df = pl.DataFrame()

        with open(OUTPUT_PATH_OD, 'w') as f:
            write_proc_print_report(
                f, ggg_df,
                'PUBLIC BANK BERHAD',
                'REPORT ID : EIBMEXCP',
                f'EXCEPTIONAL REPORT ON OD ACCOUNTS ON SASDATA AS AT : {rdate}',
                'PRODUCT', 'PRODUCT'
            )

        print(f"  Generated: {OUTPUT_PATH_OD} ({len(ggg_df)} records)")

        # ====================================================================
        # HHH - Invalid CUSTOMER CODE to SECTOR combinations
        # ====================================================================
        print("Processing Customer Code/Sector exceptions...")

        # Apply custcode/sector validation
        hhh_records = []
        for row in loan_full_df.iter_rows(named=True):
            custcode = row['CUSTCODE']
            sector = row['SECTOR']

            if check_custcode_sector(custcode, sector):
                hhh_records.append(row)

        hhh_df = pl.DataFrame(hhh_records) if hhh_records else pl.DataFrame()

        # Rename ACCBRCH to BRANCH (already done in query above)
        # hhh_df already has BRANCH column

        if len(hhh_df) > 0:
            # Need to use ACCBRCH for BY variable
            if 'ACCBRCH' not in hhh_df.columns and 'BRANCH' in hhh_df.columns:
                hhh_df = hhh_df.rename({'BRANCH': 'ACCBRCH'})
            hhh_df = hhh_df.sort(['ACCBRCH', 'LOANTYPE', 'ACCTNO'])

        with open(OUTPUT_PATH_CUSTCODE, 'w') as f:
            if len(hhh_df) > 0:
                write_proc_print_report(
                    f, hhh_df,
                    'PUBLIC BANK BERHAD',
                    'REPORT ID : EIBMEXCP',
                    f'EXCEPTIONAL REPORT ON CUSTCODE ON LOANS ACCTS AS AT : {rdate}',
                    'ACCBRCH', 'ACCBRCH'
                )
            else:
                # Empty report
                f.write(format_asa_line('PUBLIC BANK BERHAD'.center(LRECL - 1), '1') + '\n')
                f.write(format_asa_line('REPORT ID : EIBMEXCP'.center(LRECL - 1), ' ') + '\n')
                f.write(
                    format_asa_line(f'EXCEPTIONAL REPORT ON CUSTCODE ON LOANS ACCTS AS AT : {rdate}'.center(LRECL - 1),
                                    ' ') + '\n')
                f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')
                f.write(format_asa_line('No observations found.'.ljust(LRECL - 1), ' ') + '\n')

        print(f"  Generated: {OUTPUT_PATH_CUSTCODE} ({len(hhh_df)} records)")

        print()
        print("=" * 80)
        print("All EIBMEXCP reports generated successfully")
        print("=" * 80)

    finally:
        con.close()


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    process_eibmexcp()
