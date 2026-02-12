#!/usr/bin/env python3
"""
Program: DALWBP
Report on domestic assets and liabilities - Part I
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Define macro variables (these should be set according to your environment)
REPTMON = "202401"  # Example: Report month
NOWK = "01"  # Example: Week number
SDESC = "'MONTHLY REPORT'"  # Report description
RDATE = "'31-JAN-2024'"  # Report date

# Define CURX macro variable (product list)
CURX = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
    39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56,
    57, 58, 59, 60, 61, 62, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75,
    101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
    116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130,
    131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145,
    146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160,
    161, 162, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175
]

# Define paths
# BASE_PATH = Path("/data")
BASE_PATH = Path(__file__).resolve().parent

BNM_PATH = BASE_PATH / "bnm"
DEPOSIT_PATH = BASE_PATH / "deposit"
UMA_PATH = BASE_PATH / "uma"
OUTPUT_PATH = BASE_PATH / "output"

# Input files
CURN_FILE = BNM_PATH / f"curn{REPTMON}{NOWK}.parquet"
DEPT_FILE = BNM_PATH / f"dept{REPTMON}{NOWK}.parquet"
CURRENT_FILE = DEPOSIT_PATH / "current.parquet"
UMA_HOE_FILE = UMA_PATH / "uma_hoe.parquet"

# Output files
DALW_PARQUET = BNM_PATH / f"dalw{REPTMON}{NOWK}.parquet"
REPORT_FILE = OUTPUT_PATH / f"dalw_report_{REPTMON}{NOWK}.txt"

# Ensure output directory exists
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
BNM_PATH.mkdir(parents=True, exist_ok=True)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_bnmcode_mapping():
    """Create mapping functions for BNMCODE generation"""

    def map_bnmcode_42110(custcd):
        """Map BNMCODE for PRODCD 42110 series"""
        results = []

        if custcd in ['01', '11', '12', '71', '72', '73', '74', '79', '07', '17', '57', '75']:
            results.append(f'42110{custcd}000000Y')
        elif custcd in ['13', '04', '05', '06', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40']:
            results.append('4211020000000Y')
            if custcd == '13':
                results.append(f'42110{custcd}000000Y')
        elif custcd in ['60', '61', '62', '63', '64', '65', '45', '41', '42', '43', '44', '46', '47', '48', '49', '51',
                        '52', '53', '54', '59']:
            results.append('4211060000000Y')
        elif custcd in ['76', '77', '78']:
            results.append('4211076000000Y')
        elif custcd in ['81', '82', '83', '84']:
            results.append('4211081000000Y')
        elif custcd in ['85', '86', '87', '88', '89', '90', '91', '92', '95', '96', '98', '99']:
            results.append('4211085000000Y')

        return results

    def map_bnmcode_42610(custcd):
        """Map BNMCODE for PRODCD 42610"""
        results = []

        if custcd in ['01', '11', '12', '71', '72', '73', '74', '79', '07', '17', '57', '75']:
            results.append(f'42610{custcd}000000Y')
        elif custcd in ['13', '04', '05', '06', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40']:
            results.append('4261020000000Y')
            if custcd == '13':
                results.append(f'42610{custcd}000000Y')
        elif custcd in ['60', '61', '62', '63', '64', '65', '45', '41', '42', '43', '44', '46', '47', '48', '49', '51',
                        '52', '53', '54', '59']:
            results.append('4261060000000Y')
        elif custcd in ['76', '77', '78']:
            results.append('4261076000000Y')
        elif custcd in ['81', '82', '83', '84']:
            results.append('4261081000000Y')
        elif custcd in ['85', '86', '87', '88', '89', '90', '91', '92', '95', '96', '98', '99']:
            results.append('4261085000000Y')

        return results

    def map_bnmcode_42120(custcd):
        """Map BNMCODE for PRODCD 42120/42320"""
        results = []

        if custcd in ['13', '04', '05', '06', '30', '31', '32', '33', '34', '35', '45', '36', '37', '38', '39', '40']:
            results.append('4212020000000Y')
        elif custcd in ['76', '77', '78']:
            results.append('4212076000000Y')
        elif custcd in ['79', '07', '17', '57', '75']:
            results.append(f'42120{custcd}000000Y')
        elif custcd in ['85', '86', '87', '88', '89', '90', '91', '92', '95', '96', '98', '99']:
            results.append('4212085000000Y')
            if custcd in ['86', '87', '88', '89']:
                results.append('4212086000000Y')

        return results

    def map_bnmcode_42199(custcd):
        """Map BNMCODE for PRODCD 42199"""
        results = []

        if custcd in ['04', '05', '06', '13', '20', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
                      '45']:
            results.append('4219920000000Y')
        elif custcd in ['77', '78']:
            results.append('4219976000000Y')
        elif custcd in ['01', '02', '03', '07', '12', '17', '57', '71', '72', '73', '74', '75', '79']:
            results.append(f'42199{custcd}000000Y')
        elif custcd in ['41', '42', '43', '44', '46', '47', '48', '49', '51', '52', '53', '54', '57', '59', '61', '62',
                        '63', '64', '65', '66', '67', '68', '69', '75']:
            results.append('4219960000000Y')
        elif custcd in ['82', '83', '84']:
            results.append('4219981000000Y')
        elif custcd in ['86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99']:
            results.append('4219985000000Y')

        return results

    def map_bnmcode_42699(custcd):
        """Map BNMCODE for PRODCD 42699"""
        results = []

        if custcd in ['04', '05', '06', '13', '20', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
                      '45']:
            results.append('4269920000000Y')
        elif custcd in ['77', '78']:
            results.append('4269976000000Y')
        elif custcd in ['01', '02', '03', '07', '12', '17', '57', '71', '72', '73', '74', '75', '79']:
            results.append(f'42699{custcd}000000Y')
        elif custcd in ['41', '42', '43', '44', '46', '47', '48', '49', '51', '52', '53', '54', '57', '59', '61', '62',
                        '63', '64', '65', '66', '67', '68', '69', '75']:
            results.append('4269960000000Y')
        elif custcd in ['82', '83', '84']:
            results.append('4269981000000Y')
        elif custcd in ['86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99']:
            results.append('4269985000000Y')

        return results

    return {
        'map_42110': map_bnmcode_42110,
        'map_42610': map_bnmcode_42610,
        'map_42120': map_bnmcode_42120,
        'map_42199': map_bnmcode_42199,
        'map_42699': map_bnmcode_42699
    }


def write_asa_report(df, output_file, title_lines, page_length=60):
    """
    Write report with ASA carriage control characters

    ASA control characters:
    ' ' = Single space (advance one line)
    '0' = Double space (advance two lines)
    '-' = Triple space (advance three lines)
    '1' = Form feed (new page)
    '+' = No advance (overprint)
    """

    with open(output_file, 'w') as f:
        lines_on_page = 0

        def write_line(text, control=' '):
            nonlocal lines_on_page
            f.write(f"{control}{text}\n")
            if control == '1':
                lines_on_page = 1
            elif control == '0':
                lines_on_page += 2
            elif control == '-':
                lines_on_page += 3
            else:
                lines_on_page += 1

        def check_page_break(lines_needed=1):
            nonlocal lines_on_page
            if lines_on_page + lines_needed > page_length:
                write_line("", '1')
                for title in title_lines:
                    write_line(title, ' ')
                write_line("", ' ')
                return True
            return False

        # Write initial page with titles
        write_line("", '1')
        for title in title_lines:
            write_line(title, ' ')
        write_line("", ' ')

        # Write header
        header = f"{'BNMCODE':<14}  {'AMTIND':<6}  {'AMOUNT':>25}"
        write_line(header, ' ')
        write_line("-" * len(header), ' ')

        # Write data rows
        for row in df.iter_rows(named=True):
            check_page_break(1)
            bnmcode = row['bnmcode']
            amtind = row['amtind'] if row['amtind'] else ''
            amount = row['amount']

            line = f"{bnmcode:<14}  {amtind:<6}  {amount:>25,.2f}"
            write_line(line, ' ')


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    """Main processing function"""

    conn = duckdb.connect()

    # Initialize empty list to collect all ALWDEPT dataframes
    all_alwdept = []

    # ========================================================================
    # SECTION 1: CURRENT ACCOUNTS (CURN)
    # ========================================================================

    print("Processing CURRENT accounts...")

    print("CURN_FILE path:", CURN_FILE)
    print("Exists?", CURN_FILE.exists())

    alw = conn.execute(f"""
        SELECT custcd, amtind, SUM(curbal) as amount
        FROM read_parquet('{CURN_FILE}')
        WHERE product IN ({','.join(map(str, CURX))})
          AND product NOT IN (63, 163)
        GROUP BY custcd, amtind
    """).pl()

    # Generate BNMCODE mappings for 42110 series
    mappers = create_bnmcode_mapping()

    alwdept_rows = []
    for row in alw.iter_rows(named=True):
        custcd = row['custcd']
        amtind = row['amtind']
        amount = row['amount']

        bnmcodes = mappers['map_42110'](custcd)
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
    # SECTION 2: RM DEPOSIT - BY CUSTOMER CODE
    # ========================================================================

    print("Processing RM DEPOSIT by customer code...")

    print("DEPT_FILE path:", DEPT_FILE)
    print("Exists?", DEPT_FILE.exists())

    # Part 1: From DEPT file
    alw = conn.execute(f"""
        SELECT statecd, prodcd, custcd, amtind, SUM(curbal) as amount
        FROM read_parquet('{DEPT_FILE}')
        WHERE prodcd IN ('42120','42320','42199','42699')
        GROUP BY statecd, prodcd, custcd, amtind
    """).pl()

    # Part 2: From DEPOSIT.CURRENT file (FCY)
    # Note: We need format mappings. Assuming they're defined in lookup tables
    # For this conversion, we'll create the logic inline

    print("CURRENT_FILE path:", CURRENT_FILE)
    print("Exists?", CURRENT_FILE.exists())

    fcy = conn.execute(f"""
        SELECT *
        FROM read_parquet('{CURRENT_FILE}')
        WHERE (product BETWEEN 400 AND 444) OR (product BETWEEN 450 AND 454)
          AND product != 413
    """).pl()

    # Apply format transformations (these would normally come from format catalogs)
    # For simplicity, we'll assume statecd, custcd, prodcd, amtind are derived columns
    # that need to be added based on BRANCH, CUSTCODE, PRODUCT

    # Simplified version - in real scenario, you'd load the format mappings
    if len(fcy) > 0:
        alw426 = conn.execute(f"""
            SELECT statecd, prodcd, custcd, amtind, SUM(curbal) as amount
            FROM fcy
            GROUP BY statecd, prodcd, custcd, amtind
        """).pl()

        alw = pl.concat([alw, alw426])

    # Generate BNMCODE mappings for multiple product codes
    alwdept_rows = []
    for row in alw.iter_rows(named=True):
        prodcd = row['prodcd']
        custcd = row['custcd']
        amtind = row['amtind']
        amount = row['amount']

        bnmcodes = []

        if prodcd == '42610':
            bnmcodes = mappers['map_42610'](custcd)
        elif prodcd in ['42120', '42320']:
            bnmcodes = mappers['map_42120'](custcd)
        elif prodcd == '42199':
            bnmcodes = mappers['map_42199'](custcd)
        elif prodcd == '42699':
            bnmcodes = mappers['map_42699'](custcd)

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
    # SECTION 3: RM VOSTRO ACCOUNTS
    # ========================================================================

    print("Processing RM VOSTRO accounts...")

    print("DEPT_FILE path:", DEPT_FILE)
    print("Exists?", DEPT_FILE.exists())

    alw = conn.execute(f"""
        SELECT custcd, amtind, SUM(curbal) as amount
        FROM read_parquet('{DEPT_FILE}')
        WHERE prodcd = '43110'
        GROUP BY custcd, amtind
    """).pl()

    alwdept_rows = []
    for row in alw.iter_rows(named=True):
        custcd = row['custcd']
        amtind = row['amtind']
        amount = row['amount']

        if custcd in ['02', '81']:
            alwdept_rows.append({
                'bnmcode': f'43110{custcd}000000Y',
                'amount': amount,
                'amtind': amtind
            })

    if alwdept_rows:
        alwdept = pl.DataFrame(alwdept_rows)
        all_alwdept.append(alwdept)

    # ========================================================================
    # SECTION 4: RM INTEREST PAYABLE NIE
    # ========================================================================

    print("Processing RM INTEREST PAYABLE NIE...")

    print("DEPT_FILE path:", DEPT_FILE)
    print("Exists?", DEPT_FILE.exists())

    alw = conn.execute(f"""
        SELECT prodcd, custcd, amtind, SUM(intpaybl) as amount
        FROM read_parquet('{DEPT_FILE}')
        WHERE SUBSTRING(prodcd, 1, 3) IN ('421','423','426')
        GROUP BY prodcd, custcd, amtind
    """).pl()

    alwdept_rows = []
    for row in alw.iter_rows(named=True):
        prodcd = row['prodcd']
        custcd = row['custcd']
        amtind = row['amtind']
        amount = row['amount']

        bnmcode = None

        if custcd in ['80', '81', '82', '83', '84', '85', '86', '87', '88', '89',
                      '90', '91', '92', '95', '96', '98', '99']:
            if prodcd in ['42110', '42120', '42310', '42320']:
                bnmcode = '4911080000000Y'
            elif prodcd == '42610':
                bnmcode = '4961080000000Y'
        else:
            if prodcd in ['42110', '42120', '42310', '42320']:
                bnmcode = '4911050000000Y'
            elif prodcd == '42610':
                bnmcode = '4961050000000Y'

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
    # SECTION 5: UMA HOE UNCLAIMED MONIES
    # ========================================================================

    print("Processing UMA HOE UNCLAIMED MONIES...")

    print("UMA_HOE_FILE path:", UMA_HOE_FILE)
    print("Exists?", UMA_HOE_FILE.exists())

    alw = conn.execute(f"""
        SELECT custcode, amtind, SUM(amount) as amount
        FROM read_parquet('{UMA_HOE_FILE}')
        WHERE (acctno BETWEEN 1000000000 AND 1999999999)
           OR (acctno BETWEEN 7000000000 AND 7999999999)
        GROUP BY custcode, amtind
    """).pl()

    alwdept_rows = []
    for row in alw.iter_rows(named=True):
        custcd = str(row['custcode'])
        amtind = row['amtind']
        amount = row['amount']

        bnmcodes = mappers['map_42199'](custcd)

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

        # Final aggregation
        final_df = combined_df.group_by(['bnmcode', 'amtind']).agg(
            pl.col('amount').sum()
        ).sort(['bnmcode', 'amtind'])

        # Save to parquet
        final_df.write_parquet(DALW_PARQUET)
        print(f"Saved consolidated data to {DALW_PARQUET}")

        # Generate report
        print("Generating report...")

        title_lines = [
            SDESC.strip("'"),
            "REPORT ON DOMESTIC ASSETS AND LIABILITIES PART I - M&I DEPOSIT",
            f"REPORT DATE : {RDATE.strip('\'')}",
            ""
        ]

        write_asa_report(final_df, REPORT_FILE, title_lines)
        print(f"Report written to {REPORT_FILE}")

    else:
        print("No data to process")

    conn.close()
    print("Processing complete!")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()
