#!/usr/bin/env python3
"""
Program: LALWPBBP
Report on domestic assets and liabilities - Part I
Loan Processing Module
"""

import duckdb
import polars as pl
from pathlib import Path

# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Define macro variables (these should be set according to your environment)
REPTMON = "202401"  # Example: Report month
NOWK = "01"  # Example: Week number
SDESC = "'MONTHLY REPORT'"  # Report description
RDATE = "'31-JAN-2024'"  # Report date

# Define paths
# BASE_PATH = Path("/data")
BASE_PATH = Path(__file__).resolve().parent

BNM_PATH = BASE_PATH / "bnm"
OUTPUT_PATH = BASE_PATH / "output"

# Input files
LOAN_FILE = BNM_PATH / f"loan{REPTMON}{NOWK}.parquet"

# Output files
LALW_PARQUET = BNM_PATH / f"lalw{REPTMON}{NOWK}.parquet"
REPORT_FILE = OUTPUT_PATH / f"lalw_report_{REPTMON}{NOWK}.txt"

# Ensure output directories exist
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
BNM_PATH.mkdir(parents=True, exist_ok=True)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

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


def map_bnmcode_rm_loan(custcd):
    """
    Map customer code to BNMCODE for RM loans (341-344 series)
    Returns list of BNMCODEs
    """
    results = []

    if custcd in ['02', '03', '11', '12', '71', '72', '73', '74', '79']:
        results.append(f'34100{custcd}000000Y')
    elif custcd in ['20', '13', '17', '30', '32', '33', '34', '35',
                    '36', '37', '38', '39', '40', '04', '05', '06']:
        results.append('3410020000000Y')
        if custcd in ['13', '17']:
            results.append(f'34100{custcd}000000Y')
    elif custcd in ['41', '42', '43', '44', '46', '47', '48', '49', '51',
                    '52', '53', '54', '60', '61', '62', '63', '64', '65',
                    '59', '75', '57']:
        results.append('3410060000000Y')
    elif custcd in ['76', '77', '78']:
        results.append('3410076000000Y')
    elif custcd in ['81', '82', '83', '84']:
        results.append('3410081000000Y')
    elif custcd in ['85', '86', '87', '88', '89', '90', '91', '92', '95',
                    '96', '98', '99']:
        results.append('3410085000000Y')
        if custcd in ['86', '87', '88', '89']:
            results.append('3410086000000Y')

    return results


def map_bnmcode_fx_loan(custcd):
    """
    Map customer code to BNMCODE for FX loans (346 series)
    Returns list of BNMCODEs
    """
    results = []

    if custcd in ['02', '03', '11', '12', '71', '72', '73', '74', '79']:
        results.append(f'34600{custcd}000000Y')
    elif custcd in ['20', '13', '17', '30', '32', '33', '34', '35',
                    '36', '37', '38', '39', '40', '04', '05', '06']:
        results.append('3460020000000Y')
        if custcd in ['13', '17']:
            results.append(f'34600{custcd}000000Y')
    elif custcd in ['41', '42', '43', '44', '46', '47', '48', '49', '51',
                    '52', '53', '54', '60', '61', '62', '63', '64', '65',
                    '59', '75', '57']:
        results.append('3460060000000Y')
        if custcd in ['41', '42', '43', '61']:
            results.append('3460061000000Y')
        if custcd in ['44', '46', '47', '62']:
            results.append('3460062000000Y')
        if custcd in ['48', '49', '51', '63']:
            results.append('3460063000000Y')
    elif custcd in ['76', '77', '78']:
        results.append('3460076000000Y')
    elif custcd in ['81', '82', '83', '84']:
        results.append('3460081000000Y')
    elif custcd in ['85', '86', '87', '88', '89', '90', '91', '92', '95',
                    '96', '98', '99']:
        results.append('3460085000000Y')
        if custcd in ['86', '87', '88', '89']:
            results.append('3460086000000Y')

    return results


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    """Main processing function"""

    conn = duckdb.connect()

    # Initialize list to collect all ALWLOAN dataframes
    all_alwloan = []

    print("Starting LOAN processing...")

    # ========================================================================
    # LOAD AND FILTER LOAN DATA
    # ========================================================================

    print("Loading loan data...")

    # Filter loan data: exclude paid/closed loans unless they have EIR adjustment
    loan_data = conn.execute(f"""
        SELECT *
        FROM read_parquet('{LOAN_FILE}')
        WHERE paidind NOT IN ('P', 'C') OR eir_adj IS NOT NULL
    """).pl()

    print(f"Loaded {len(loan_data)} loan records after filtering")

    # ========================================================================
    # SECTION 1: RM LOANS - BY CUSTOMER CODE (341-344 series)
    # ========================================================================

    print("Processing RM LOANS by customer code...")

    # Filter for RM loans and aggregate
    alw = conn.execute("""
        SELECT custcd, prodcd, amtind, SUM(bal_aft_eir) as amount
        FROM loan_data
        WHERE SUBSTRING(prodcd, 1, 3) IN ('341','342','343','344')
        GROUP BY custcd, prodcd, amtind
    """).pl()

    # Generate BNMCODE mappings for RM loans
    alwloan_rows = []
    for row in alw.iter_rows(named=True):
        custcd = row['custcd']
        amtind = row['amtind']
        amount = row['amount']

        bnmcodes = map_bnmcode_rm_loan(custcd)

        for bnmcode in bnmcodes:
            alwloan_rows.append({
                'bnmcode': bnmcode,
                'amount': amount,
                'amtind': amtind
            })

    if alwloan_rows:
        alwloan = pl.DataFrame(alwloan_rows)
        all_alwloan.append(alwloan)
        print(f"  Generated {len(alwloan_rows)} RM loan records")

    # ========================================================================
    # SECTION 2: FX LOANS - BY CUSTOMER CODE (346 series)
    # ========================================================================

    print("Processing FX LOANS by customer code...")

    # Filter for FX loans and aggregate
    alw = conn.execute("""
        SELECT custcd, prodcd, amtind, SUM(balance) as amount
        FROM loan_data
        WHERE SUBSTRING(prodcd, 1, 3) IN ('346')
        GROUP BY custcd, prodcd, amtind
    """).pl()

    # Generate BNMCODE mappings for FX loans
    alwloan_rows = []
    for row in alw.iter_rows(named=True):
        custcd = row['custcd']
        amtind = row['amtind']
        amount = row['amount']

        bnmcodes = map_bnmcode_fx_loan(custcd)

        for bnmcode in bnmcodes:
            alwloan_rows.append({
                'bnmcode': bnmcode,
                'amount': amount,
                'amtind': amtind
            })

    if alwloan_rows:
        alwloan = pl.DataFrame(alwloan_rows)
        all_alwloan.append(alwloan)
        print(f"  Generated {len(alwloan_rows)} FX loan records")

    # ========================================================================
    # SECTION 3: GROSS LOAN - BY APPROVED LIMIT
    # ========================================================================

    print("Processing GROSS LOAN by approved limit...")

    # Filter for all loans starting with '34' or product code '54120'
    alw = conn.execute("""
        SELECT prodcd, amtind, SUM(bal_aft_eir) as amount
        FROM loan_data
        WHERE (SUBSTRING(prodcd, 1, 2) = '34' OR prodcd = '54120')
        GROUP BY prodcd, amtind
    """).pl()

    # All records map to the same BNMCODE
    alwloan_rows = []
    for row in alw.iter_rows(named=True):
        alwloan_rows.append({
            'bnmcode': '3051000000000Y',
            'amount': row['amount'],
            'amtind': row['amtind']
        })

    if alwloan_rows:
        alwloan = pl.DataFrame(alwloan_rows)
        all_alwloan.append(alwloan)
        print(f"  Generated {len(alwloan_rows)} gross loan records")

    # ========================================================================
    # SECTION 4: LOANS SOLD TO CAGAMAS BERHAD WITH RECOURSE
    # ========================================================================

    print("Processing LOANS SOLD TO CAGAMAS BERHAD...")

    # Filter for products 225 and 226
    alw = conn.execute("""
        SELECT prodcd, amtind, SUM(bal_aft_eir) as amount
        FROM loan_data
        WHERE product IN (225, 226)
        GROUP BY prodcd, amtind
    """).pl()

    # All records map to the same BNMCODE
    alwloan_rows = []
    for row in alw.iter_rows(named=True):
        alwloan_rows.append({
            'bnmcode': '7511100000000Y',
            'amount': row['amount'],
            'amtind': row['amtind']
        })

    if alwloan_rows:
        alwloan = pl.DataFrame(alwloan_rows)
        all_alwloan.append(alwloan)
        print(f"  Generated {len(alwloan_rows)} Cagamas loan records")

    # ========================================================================
    # FINAL CONSOLIDATION
    # ========================================================================

    print("Performing final consolidation...")

    if all_alwloan:
        # Combine all ALWLOAN dataframes
        combined_df = pl.concat(all_alwloan)

        # Final aggregation by BNMCODE and AMTIND
        final_df = combined_df.group_by(['bnmcode', 'amtind']).agg(
            pl.col('amount').sum()
        ).sort(['bnmcode', 'amtind'])

        # Save to parquet
        final_df.write_parquet(LALW_PARQUET)
        print(f"Saved consolidated data to {LALW_PARQUET}")

        # Display summary statistics
        print(f"\nSummary Statistics:")
        print(f"  Total records: {len(final_df)}")
        print(f"  Total amount: {final_df['amount'].sum():,.2f}")
        print(f"  Unique BNMCODEs: {final_df['bnmcode'].n_unique()}")

        # Generate report
        print("\nGenerating report...")

        title_lines = [
            SDESC.strip("'"),
            "REPORT ON DOMESTIC ASSETS AND LIABILITIES PART I - M&I LOAN",
            f"REPORT DATE : {RDATE.strip('\'')}",
            ""
        ]

        write_asa_report(final_df, REPORT_FILE, title_lines)
        print(f"Report written to {REPORT_FILE}")

    else:
        print("No data to process")

    conn.close()
    print("\nProcessing complete!")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()
