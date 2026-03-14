#!/usr/bin/env python3
"""
Program: NALMPBBP.py
Date: 06/07/12 (Original SAS)
Purpose: RDAL PART I (NSRS KAPITI ITEMS)
         RDIR PART I (NSRS KAPITI ITEMS)

This program processes domestic assets and liabilities Part I data,
focusing on NSRS (Non-Statutory Reserve System) KAPITI items.
"""

import os
import polars as pl
from datetime import datetime
from pathlib import Path

# =========================================================================
# ENVIRONMENT SETUP
# =========================================================================
REPTMON = os.getenv('REPTMON', '')  # Reporting month
NOWK = os.getenv('NOWK', '')  # Week number
AMTIND = os.getenv('AMTIND', "'D'")  # Amount indicator
RDATE = os.getenv('RDATE', '')  # Formatted report date
SDESC = os.getenv('SDESC', '')  # System description

# Path Configuration
UPLOAD_DIR = os.getenv('UPLOAD_DIR', '/mnt/user-data/uploads')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '/mnt/user-data/outputs')

# Input parquet file
K1TBL_FILE = os.path.join(UPLOAD_DIR, f'K1TBL{REPTMON}{NOWK}.parquet')

# Output files
NALM_OUT = os.path.join(OUTPUT_DIR, f'NALM{REPTMON}{NOWK}.parquet')
NALM_FINAL_OUT = os.path.join(OUTPUT_DIR, f'NALM{REPTMON}{NOWK}_FINAL.parquet')
NALM_REPORT = os.path.join(OUTPUT_DIR, f'NALM{REPTMON}{NOWK}_REPORT.txt')


# =========================================================================
# DEPENDENCIES
# =========================================================================
# %INC PGM(KALWPBBF,PBBELF)
#
# KALWPBBF: Contains KALW format definitions (BNMCODE list for filtering)
# - Not directly used in this program's logic, so not imported
#
# PBBELF: Contains EL/ELI format definitions and helper functions
# - format_brchcd(), format_ctype(), etc.
# - Not used in this program's core logic, so not imported

# =========================================================================
# UTILITY FUNCTIONS
# =========================================================================

def calculate_remaining_months(reptdate, maturity_date):
    """
    Calculate remaining months from reptdate to maturity_date.
    Equivalent to SAS %REMMTH macro.

    Args:
        reptdate: Reporting date (datetime)
        maturity_date: Maturity date (datetime)

    Returns:
        float: Remaining months
    """
    if not reptdate or not maturity_date:
        return None

    # Extract date components for reporting date
    rpyr = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    # Days in reporting month
    if rpmth == 2:
        rd_month = 29 if (rpyr % 4 == 0) else 28
    elif rpmth in [4, 6, 9, 11]:
        rd_month = 30
    else:
        rd_month = 31

    # Extract date components for maturity date
    mdyr = maturity_date.year
    mdmth = maturity_date.month
    mdday = maturity_date.day

    # Adjust maturity day if needed
    if mdday > rd_month:
        mdday = rd_month

    # Calculate remaining time
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    remmth = remy * 12 + remm + (remd / rd_month)

    return remmth


# =========================================================================
# PROCESS K1TABL
# =========================================================================

def process_k1tabl():
    """
    Process K1TBL data to generate NALM dataset.
    Implements DP32000 and AF33000 logic sections.
    """
    print("Processing K1TABL...")

    # Read K1TBL
    df = pl.read_parquet(K1TBL_FILE).with_columns([
        pl.col('GWBALC').alias('AMOUNT'),
    ])

    # Extract BRANCH from GWAB (first 4 characters)
    df = df.with_columns([
        pl.col('GWAB').str.slice(0, 4).cast(pl.Int64).alias('BRANCH'),
        pl.lit(AMTIND).alias('AMTIND'),
    ])

    # Get REPTDATE (assumed to be in the dataset or from first row)
    reptdate_val = df.select('REPTDATE').to_series()[0] if 'REPTDATE' in df.columns else None

    if not reptdate_val:
        # If REPTDATE not in dataset, cannot proceed with date calculations
        print("Warning: REPTDATE not found in K1TBL dataset")
        reptdate_val = datetime.now()

    results = []

    # Process each row
    for row in df.iter_rows(named=True):
        bnmcodes = []

        # DP32000 Section: Process deposits and placements
        # Condition: GWCCY = 'MYR' AND GWMVT = 'P' AND GWMVTS = 'M'
        # AND (GWDLP IN ('FDA','FDB','FDL','FDS','LC','LO') OR SUBSTR(GWDLP,2,2) IN ('XI','XT'))
        if (row['GWCCY'] == 'MYR' and row['GWMVT'] == 'P' and row['GWMVTS'] == 'M'):
            if (row['GWDLP'] in ['FDA', 'FDB', 'FDL', 'FDS', 'LC', 'LO'] or
                    (len(row['GWDLP']) >= 3 and row['GWDLP'][1:3] in ['XI', 'XT'])):
                bnmcodes.extend(process_dp32000(row, reptdate_val))

        # AF33000 Section: Process loans and advances
        # Condition: GWDLP IN (' ','LO','LS','LF') OR GWACT = 'CN'
        if row['GWDLP'] in [' ', 'LO', 'LS', 'LF'] or row['GWACT'] == 'CN':
            bnmcodes.extend(process_af33000(row))

        # Output records
        for bnmcode in bnmcodes:
            if bnmcode and bnmcode != ' ':
                results.append({
                    'BRANCH': row['BRANCH'],
                    'BNMCODE': bnmcode,
                    'AMOUNT': row['AMOUNT'],
                    'AMTIND': AMTIND
                })

    # Create result DataFrame
    result_df = pl.DataFrame(results) if results else pl.DataFrame(
        schema={
            'BRANCH': pl.Int64,
            'BNMCODE': pl.Utf8,
            'AMOUNT': pl.Float64,
            'AMTIND': pl.Utf8
        }
    )

    return result_df


def process_dp32000(row, reptdate_val):
    """
    Process DP32000 section logic.
    Handles deposits and placements with maturity > 3 months.

    Args:
        row: Dictionary containing row data
        reptdate_val: Reporting date

    Returns:
        list: List of BNMCODE values to output
    """
    bnmcodes = []

    # Check if GWDLP has 'XI' or 'XT' in positions 2-3
    if len(row['GWDLP']) >= 3 and row['GWDLP'][1:3] in ['XI', 'XT']:
        # Skip if maturity date is missing
        if not row['GWMDT']:
            return bnmcodes

        # Calculate remaining months
        remmth = calculate_remaining_months(reptdate_val, row['GWMDT'])

        if remmth is None:
            return bnmcodes

        # First set of BNMCODE assignments (3250xxx series)
        if row['GWCCY'] == 'MYR' and remmth > 3:
            bnmcode1 = None

            if row['GWCTP'] in ['BC', 'BP']:
                bnmcode1 = '3250001000000F'
            elif row['GWCTP'] == 'BB':
                bnmcode1 = '3250002000000F'
            elif row['GWCTP'] == 'BI':
                bnmcode1 = '3250003000000F'
            elif row['GWCTP'] == 'BM':
                bnmcode1 = '3250012000000F'
            elif row['GWCTP'] == 'BG':
                bnmcode1 = '3250017000000F'
            elif row['GWCTP'] in ['AD', 'BF', 'BH', 'BN', 'BR', 'BS', 'BT', 'BU', 'BV', 'BZ']:
                bnmcode1 = '3250020000000F'
            elif row['GWCTP'] in ['AC', 'CA', 'CB', 'CC', 'CD', 'CF', 'CG', 'DD']:
                bnmcode1 = '3250006000000F'
            elif row['GWCTP'] in ['DA', 'DB', 'DC']:
                bnmcode1 = '3250007000000F'
            elif row['GWCTP'] in ['EA', 'EC', 'EJ']:
                bnmcode1 = '3250007600000F'
            elif row['GWCTP'] == 'FA':
                bnmcode1 = '3250007900000F'
            elif row['GWCTP'] in ['BA', 'BE']:
                bnmcode1 = '3250008100000F'
            elif row['GWCTP'] in ['CE', 'EB', 'GA']:
                bnmcode1 = '3250008500000F'

            if bnmcode1:
                bnmcodes.append(bnmcode1)

        # Second set of BNMCODE assignments (3280xxx series)
        if row['GWCCY'] == 'MYR' and remmth > 3:
            bnmcode2 = None

            if row['GWCTP'] == 'BB':
                bnmcode2 = '3280002000000F'
            elif row['GWCTP'] == 'BI':
                bnmcode2 = '3280003000000F'
            elif row['GWCTP'] == 'BM':
                bnmcode2 = '3280012000000F'
            elif row['GWCTP'] in ['AD', 'BF', 'BH', 'BN', 'BR', 'BS', 'BT', 'BU', 'BV', 'BZ']:
                bnmcode2 = '3280020000000F'
            elif row['GWCTP'] in ['BA', 'BE']:
                bnmcode2 = '3280008100000F'

            if bnmcode2:
                bnmcodes.append(bnmcode2)

    return bnmcodes


def process_af33000(row):
    """
    Process AF33000 section logic.
    Handles loans and placements (LO, LS, LF).

    Args:
        row: Dictionary containing row data

    Returns:
        list: List of BNMCODE values to output
    """
    bnmcodes = []

    # Process LO (Overnight) and LS (Short-term) loans
    if row['GWDLP'] in ['LO', 'LS']:
        # MYR currency
        if row['GWCCY'] == 'MYR' and row['GWMVT'] == 'P' and row['GWMVTS'] == 'M':
            bnmcode = None

            if row['GWCTP'] in ['BC', 'BP']:
                bnmcode = '3314001100000F'
            elif row['GWCTP'] == 'BB':
                bnmcode = '3314002100000F'
            elif row['GWCTP'] == 'BI':
                bnmcode = '3314003100000F'
            elif row['GWCTP'] == 'BM':
                bnmcode = '3314012100000F'
            elif row['GWCTP'] == 'BG':
                bnmcode = '3314017100000F'
            elif row['GWCTP'] in ['AD', 'BF', 'BH', 'BN', 'BR', 'BS', 'BT', 'BU', 'BV', 'BZ']:
                bnmcode = '3314020100000F'
            elif row['GWCTP'] in ['BA', 'BE']:
                bnmcode = '3314081100000F'

            if bnmcode:
                bnmcodes.append(bnmcode)

        # Non-MYR currency
        if row['GWCCY'] != 'MYR' and row['GWMVT'] == 'P' and row['GWMVTS'] == 'M':
            bnmcode = None

            if row['GWCTP'] in ['BC', 'BP']:
                bnmcode = '3364001100000F'
            elif row['GWCTP'] == 'BB':
                bnmcode = '3364002100000F'
            elif row['GWCTP'] == 'BI':
                bnmcode = '3364003100000F'
            elif row['GWCTP'] == 'BJ':
                bnmcode = '3364007100000F'
            elif row['GWCTP'] == 'BM':
                bnmcode = '3364012100000F'
            elif row['GWCTP'] in ['BA', 'BE']:
                bnmcode = '3364081100000F'

            if bnmcode:
                bnmcodes.append(bnmcode)

    # Process LF (Fixed-term) loans
    if row['GWDLP'] == 'LF':
        # MYR currency
        if row['GWCCY'] == 'MYR' and row['GWMVT'] == 'P' and row['GWMVTS'] == 'M':
            bnmcode = None

            if row['GWCTP'] in ['BC', 'BP']:
                bnmcode = '3314001200000F'
            elif row['GWCTP'] == 'BB':
                bnmcode = '3314002200000F'
            elif row['GWCTP'] == 'BI':
                bnmcode = '3314003200000F'
            elif row['GWCTP'] == 'BM':
                bnmcode = '3314012200000F'
            elif row['GWCTP'] == 'BG':
                bnmcode = '3314017200000F'
            elif row['GWCTP'] in ['AD', 'BF', 'BH', 'BN', 'BR', 'BS', 'BT', 'BU', 'BV', 'BZ']:
                bnmcode = '3314020200000F'
            elif row['GWCTP'] in ['BA', 'BE']:
                bnmcode = '3314081200000F'

            if bnmcode:
                bnmcodes.append(bnmcode)

        # Non-MYR currency
        if row['GWCCY'] != 'MYR' and row['GWMVT'] == 'P' and row['GWMVTS'] == 'M':
            bnmcode = None

            if row['GWCTP'] in ['BC', 'BP']:
                bnmcode = '3364001200000F'
            elif row['GWCTP'] == 'BB':
                bnmcode = '3364002200000F'
            elif row['GWCTP'] == 'BI':
                bnmcode = '3364003200000F'
            elif row['GWCTP'] == 'BJ':
                bnmcode = '3364007200000F'
            elif row['GWCTP'] == 'BM':
                bnmcode = '3364012200000F'
            elif row['GWCTP'] in ['BA', 'BE']:
                bnmcode = '3364081200000F'

            if bnmcode:
                bnmcodes.append(bnmcode)

    return bnmcodes


# =========================================================================
# MAIN PROCESSING
# =========================================================================

def main():
    """Main processing function."""
    print("=" * 70)
    print("NALMPBBP - RDAL PART I (NSRS KAPITI ITEMS)")
    print("=" * 70)

    # Process K1TABL
    k1tabl = process_k1tabl()

    # Drop BRANCH column for NALM dataset
    print("\nCreating NALM dataset...")
    nalm = k1tabl.drop('BRANCH')

    # Write intermediate NALM file
    nalm.write_parquet(NALM_OUT)
    print(f"NALM intermediate written to {NALM_OUT}")

    # Final consolidation - summarize by BNMCODE and AMTIND
    print("\nPerforming final consolidation...")
    nalm_final = nalm.group_by(['BNMCODE', 'AMTIND']).agg([
        pl.col('AMOUNT').sum().alias('AMOUNT')
    ])

    # Take absolute value of AMOUNT
    nalm_final = nalm_final.with_columns([
        pl.col('AMOUNT').abs().alias('AMOUNT')
    ])

    # Write final NALM file
    nalm_final.write_parquet(NALM_FINAL_OUT)
    print(f"NALM final written to {NALM_FINAL_OUT}")

    # Generate report
    print("\nGenerating report...")
    generate_report(nalm_final)

    print("\n" + "=" * 70)
    print("Processing complete!")
    print("=" * 70)

    # Print summary statistics
    print(f"\nTotal records: {len(nalm_final)}")
    print(f"Total amount: {nalm_final['AMOUNT'].sum():,.2f}")


def generate_report(df):
    """
    Generate ASA carriage control report.
    Page length: 60 lines per page.

    Args:
        df: Polars DataFrame with final NALM data
    """
    with open(NALM_REPORT, 'w') as f:
        page_num = 1
        line_num = 0
        page_size = 60

        # Write report header
        def write_header():
            nonlocal line_num
            f.write(' ' + SDESC + '\n')
            f.write(' REPORT ON DOMESTIC ASSETS AND LIABILITIES PART I - KAPITI\n')
            f.write(f' REPORT DATE : {RDATE}\n')
            f.write(' \n')
            f.write(' BNMCODE          AMTIND           AMOUNT\n')
            f.write(' ' + '-' * 70 + '\n')
            line_num = 6

        # Write first page header
        write_header()

        # Write data rows
        for row in df.iter_rows(named=True):
            # Check if new page needed
            if line_num >= page_size - 2:
                f.write('1')  # Form feed (ASA carriage control)
                page_num += 1
                line_num = 0
                write_header()

            # Format amount with comma separator
            amount_str = f"{row['AMOUNT']:>30,.2f}"

            # Write data line
            f.write(f" {row['BNMCODE']:<14}   {row['AMTIND']:<1}   {amount_str}\n")
            line_num += 1

    print(f"Report written to {NALM_REPORT}")


# =========================================================================
# EXECUTION
# =========================================================================

if __name__ == '__main__':
    main()
