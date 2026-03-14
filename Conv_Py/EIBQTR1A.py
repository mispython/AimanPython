#!/usr/bin/env python3
"""
Program: EIBQTR1A.py
Date: Original JCL/SAS orchestrator
Purpose: BNM RDAL/RDIR Quarterly Reporting Orchestrator

This orchestrator coordinates the execution of multiple BNM reporting programs
for RDAL (Report on Domestic Assets and Liabilities) and RDIR (Report on
Domestic Interest Rates).

Program execution sequence:
1. Date validation and setup
2. RDAL processing (DALMPBBD, DALWBP, DALMBP)
3. Fixed deposit processing (FALMPBBD, FALWPBBP, FALMPBBP, FALQPBBP)
4. Loan processing (LALWPBBP, LALMPBBP, LALQPBBP)
5. KAPITI processing (KALWPBBP, KALMPBBP, KALMSTOR, NALMPBBP, KALQPBBP)
6. Report generation (EIBRDL1A, EIBRDL2A, EIBRDL3A, etc.)
7. Output file transfer preparation
"""

import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import shutil

# =========================================================================
# ENVIRONMENT SETUP
# =========================================================================

# Path Configuration
UPLOAD_DIR = os.getenv('UPLOAD_DIR', '/mnt/user-data/uploads')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '/mnt/user-data/outputs')
WORK_DIR = '/home/claude'

# Program directory (where child programs are located)
PROGRAM_DIR = OUTPUT_DIR  # Assuming converted programs are in output dir


# =========================================================================
# DATE CALCULATION AND MACRO VARIABLES
# =========================================================================

def calculate_reporting_dates():
    """
    Calculate reporting dates and week numbers.
    Equivalent to the initial DATA REPTDATE step in SAS.

    Returns:
        dict: Dictionary containing all macro variables
    """
    # Get first day of current month minus 1 day (last day of previous month)
    today = datetime.now()
    first_of_month = datetime(today.year, today.month, 1)
    reptdate = first_of_month - timedelta(days=1)

    # Determine week based on day of month
    day = reptdate.day

    if day == 8:
        sdd = 1
        wk = '1'
        wk1 = '4'
        wk2 = None
        wk3 = None
    elif day == 15:
        sdd = 9
        wk = '2'
        wk1 = '1'
        wk2 = None
        wk3 = None
    elif day == 22:
        sdd = 16
        wk = '3'
        wk1 = '2'
        wk2 = None
        wk3 = None
    else:  # Last week of month
        sdd = 23
        wk = '4'
        wk1 = '3'
        wk2 = '2'
        wk3 = '1'

    mm = reptdate.month

    # Calculate MM1 (previous month if WK='1', else current month)
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    # Calculate MM2 (always previous month)
    mm2 = mm - 1
    if mm2 == 0:
        mm2 = 12

    sdate = datetime(reptdate.year, mm, sdd)
    sdesc = 'PUBLIC BANK BERHAD'

    # Build macro variable dictionary
    macros = {
        'NOWK': wk,
        'NOWK1': '1',
        'NOWK2': '2',
        'NOWK3': '3',
        'REPTMON': f'{mm:02d}',
        'REPTMON1': f'{mm1:02d}',
        'REPTMON2': f'{mm2:02d}',
        'REPTYEAR': f'{reptdate.year:04d}',
        'REPTYR': f'{reptdate.year % 100:02d}',
        'REPTDAY': f'{reptdate.day:02d}',
        'RDATE': reptdate.strftime('%d/%m/%Y'),
        'TDATE': reptdate.strftime('%Y%m%d'),
        'SDATE': sdate.strftime('%d/%m/%Y'),
        'SDESC': sdesc,
        'AMTIND': "'D'"
    }

    return macros, reptdate


def read_reptdate_from_file(filename):
    """
    Read REPTDATE from a parquet file.

    Args:
        filename: Path to parquet file

    Returns:
        str: Formatted date string (DD/MM/YYYY) or None if file not found
    """
    try:
        import polars as pl
        df = pl.read_parquet(filename)
        if 'REPTDATE' in df.columns and len(df) > 0:
            reptdate = df.select('REPTDATE').to_series()[0]
            if isinstance(reptdate, datetime):
                return reptdate.strftime('%d/%m/%Y')
            elif isinstance(reptdate, str):
                return reptdate
    except Exception as e:
        print(f"Warning: Could not read {filename}: {e}")

    return None


def read_reptdate_from_text(filename):
    """
    Read REPTDATE from first line of text file (YYMMDD8 format).

    Args:
        filename: Path to text file

    Returns:
        str: Formatted date string (DD/MM/YYYY) or None if file not found
    """
    try:
        with open(filename, 'r') as f:
            first_line = f.readline().strip()
            # Parse YYMMDD8 format (YYYYMMDD)
            if len(first_line) >= 8:
                date_str = first_line[:8]
                reptdate = datetime.strptime(date_str, '%Y%m%d')
                return reptdate.strftime('%d/%m/%Y')
    except Exception as e:
        print(f"Warning: Could not read {filename}: {e}")

    return None


def validate_input_dates(macros):
    """
    Validate that all input files have matching reporting dates.

    Args:
        macros: Dictionary of macro variables

    Returns:
        tuple: (bool, dict) - (validation result, dict of file dates)
    """
    rdate = macros['RDATE']
    reptmon = macros['REPTMON']
    nowk = macros['NOWK']

    # Define input files and their expected dates
    file_dates = {}

    # LOAN file
    loan_file = os.path.join(UPLOAD_DIR, f'LOAN{reptmon}{nowk}.parquet')
    file_dates['LOAN'] = read_reptdate_from_file(loan_file)

    # DEPOSIT file
    deposit_file = os.path.join(UPLOAD_DIR, f'DEPOSIT{reptmon}{nowk}.parquet')
    file_dates['DEPOSIT'] = read_reptdate_from_file(deposit_file)

    # FD file
    fd_file = os.path.join(UPLOAD_DIR, f'FD{reptmon}{nowk}.parquet')
    file_dates['FD'] = read_reptdate_from_file(fd_file)

    # KAPITI1 text file
    kapiti1_file = os.path.join(UPLOAD_DIR, 'kapiti1.txt')
    file_dates['KAPITI1'] = read_reptdate_from_text(kapiti1_file)

    # KAPITI2 text file
    kapiti2_file = os.path.join(UPLOAD_DIR, 'kapiti2.txt')
    file_dates['KAPITI2'] = read_reptdate_from_text(kapiti2_file)

    # KAPITI3 text file
    kapiti3_file = os.path.join(UPLOAD_DIR, 'kapiti3.txt')
    file_dates['KAPITI3'] = read_reptdate_from_text(kapiti3_file)

    # Validate all dates match RDATE
    all_valid = True
    for file_name, file_date in file_dates.items():
        if file_date is None:
            print(f"Warning: Could not read date from {file_name}")
            all_valid = False
        elif file_date != rdate:
            print(f"ERROR: The {file_name} extraction is not dated {rdate} (found: {file_date})")
            all_valid = False

    return all_valid, file_dates


# =========================================================================
# PROGRAM EXECUTION HELPERS
# =========================================================================

def set_environment_variables(macros):
    """
    Set environment variables for child programs.

    Args:
        macros: Dictionary of macro variables
    """
    for key, value in macros.items():
        os.environ[key] = str(value)

    # Also set path variables
    os.environ['UPLOAD_DIR'] = UPLOAD_DIR
    os.environ['OUTPUT_DIR'] = OUTPUT_DIR


def run_program(program_name, description=""):
    """
    Execute a child Python program.

    Args:
        program_name: Name of the program to execute (without .py extension)
        description: Optional description for logging

    Returns:
        bool: True if successful, False otherwise
    """
    program_path = os.path.join(PROGRAM_DIR, f'{program_name}.py')

    print(f"\n{'=' * 70}")
    print(f"Executing: {program_name}")
    if description:
        print(f"Description: {description}")
    print(f"{'=' * 70}")

    if not os.path.exists(program_path):
        print(f"ERROR: Program not found: {program_path}")
        return False

    try:
        result = subprocess.run(
            ['python3', program_path],
            check=True,
            capture_output=True,
            text=True
        )

        # Print output
        if result.stdout:
            print(result.stdout)

        print(f"✓ {program_name} completed successfully")
        return True

    except subprocess.CalledProcessError as e:
        print(f"✗ {program_name} failed with return code {e.returncode}")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr)
        return False
    except Exception as e:
        print(f"✗ {program_name} failed with exception: {e}")
        return False


def cleanup_work_datasets():
    """
    Clean up temporary work datasets.
    Equivalent to PROC DATASETS LIB=WORK KILL NOLIST.
    """
    print("\nCleaning up temporary work files...")
    # In this implementation, we don't create explicit work files
    # But this hook is here for consistency with SAS structure
    pass


def backup_datasets(source_files, backup_dir):
    """
    Backup datasets to archive directory.
    Equivalent to PROC COPY.

    Args:
        source_files: List of source file paths
        backup_dir: Backup directory path
    """
    print(f"\nBacking up datasets to {backup_dir}...")

    os.makedirs(backup_dir, exist_ok=True)

    for source_file in source_files:
        if os.path.exists(source_file):
            filename = os.path.basename(source_file)
            dest_file = os.path.join(backup_dir, filename)
            shutil.copy2(source_file, dest_file)
            print(f"  Copied: {filename}")


def delete_datasets(file_list):
    """
    Delete specified datasets.
    Equivalent to PROC DATASETS DELETE.

    Args:
        file_list: List of file paths to delete
    """
    print("\nDeleting temporary datasets...")

    for file_path in file_list:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"  Deleted: {os.path.basename(file_path)}")


# =========================================================================
# SFTP FILE PREPARATION
# =========================================================================

def prepare_sftp_commands(macros):
    """
    Prepare SFTP command file for file transfer to DRR.

    Args:
        macros: Dictionary of macro variables
    """
    sftp_file = os.path.join(OUTPUT_DIR, 'sftp_commands.txt')

    today = datetime.now()

    # Determine file naming based on day and time
    # If day > 2 OR (day = 2 AND hour > 20), use "EIR.TXT" suffix
    if today.day > 2 or (today.day == 2 and today.hour > 20):
        rdal_name = "RDAL KAPMNI EIR.TXT"
        nsrs_name = "NSRS KAPMNI EIR.TXT"
    else:
        rdal_name = "RDAL KAPMNI.TXT"
        nsrs_name = "NSRS KAPMNI.TXT"

    with open(sftp_file, 'w') as f:
        f.write(f'PUT //SAP.PBB.KAPMNI.RDAL       "{rdal_name}"\n')
        f.write(f'PUT //SAP.PBB.NSRS.KAPMNI.RDAL  "{nsrs_name}"\n')

    print(f"\nSFTP commands prepared: {sftp_file}")
    print(f"  RDAL file: {rdal_name}")
    print(f"  NSRS file: {nsrs_name}")


# =========================================================================
# MAIN PROCESSING ORCHESTRATOR
# =========================================================================

def main():
    """Main orchestrator function."""
    print("=" * 70)
    print("EIBQTR1A - BNM RDAL/RDIR QUARTERLY REPORTING ORCHESTRATOR")
    print("=" * 70)

    # Calculate reporting dates and set up macro variables
    print("\nCalculating reporting dates...")
    macros, reptdate = calculate_reporting_dates()

    print(f"\nReporting Date: {macros['RDATE']}")
    print(f"Reporting Month: {macros['REPTMON']}")
    print(f"Week Number: {macros['NOWK']}")
    print(f"Reporting Year: {macros['REPTYEAR']}")

    # Set environment variables for child programs
    set_environment_variables(macros)

    # Validate input file dates
    print("\n" + "=" * 70)
    print("VALIDATING INPUT FILE DATES")
    print("=" * 70)

    dates_valid, file_dates = validate_input_dates(macros)

    if not dates_valid:
        print("\n" + "!" * 70)
        print("ERROR: Input file date validation failed!")
        print("THE JOB IS NOT DONE !!")
        print("!" * 70)
        sys.exit(77)

    print("\n✓ All input files validated successfully")

    # Begin processing
    print("\n" + "=" * 70)
    print("BEGIN PROCESSING")
    print("=" * 70)

    try:
        # ================================================================
        # RDAL PROCESSING
        # ================================================================
        print("\n" + "-" * 70)
        print("RDAL PROCESSING")
        print("-" * 70)

        if not run_program('DALMPBBD', 'RDAL Master Processing - Domestic'):
            raise Exception("DALMPBBD failed")

        if not run_program('DALWBP', 'RDAL Weekly Processing'):
            raise Exception("DALWBP failed")

        if not run_program('DALMBP', 'RDAL Monthly Processing'):
            raise Exception("DALMBP failed")

        cleanup_work_datasets()

        # Backup deposit-related datasets
        reptmon = macros['REPTMON']
        nowk = macros['NOWK']

        backup_files = [
            os.path.join(OUTPUT_DIR, f'SAVG{reptmon}{nowk}.parquet'),
            os.path.join(OUTPUT_DIR, f'CURN{reptmon}{nowk}.parquet'),
            os.path.join(OUTPUT_DIR, f'DEPT{reptmon}{nowk}.parquet'),
        ]
        backup_dir = os.path.join(OUTPUT_DIR, 'backup')
        backup_datasets(backup_files, backup_dir)

        # Delete backed up datasets from main area
        delete_datasets(backup_files)

        # ================================================================
        # FIXED DEPOSIT PROCESSING
        # ================================================================
        print("\n" + "-" * 70)
        print("FIXED DEPOSIT PROCESSING")
        print("-" * 70)

        if not run_program('FALMPBBD', 'Fixed Deposit Master Processing'):
            raise Exception("FALMPBBD failed")

        if not run_program('FALWPBBP', 'Fixed Deposit Weekly Processing - PBB'):
            raise Exception("FALWPBBP failed")

        if not run_program('FALMPBBP', 'Fixed Deposit Monthly Processing - PBB'):
            raise Exception("FALMPBBP failed")

        if not run_program('FALQPBBP', 'Fixed Deposit Quarterly Processing - PBB'):
            raise Exception("FALQPBBP failed")

        # Backup FD datasets
        fd_backup_files = [
            os.path.join(OUTPUT_DIR, 'FDWKLY.parquet'),
            os.path.join(OUTPUT_DIR, 'FDMTHLY.parquet'),
        ]
        backup_datasets(fd_backup_files, backup_dir)

        # Copy LOAN datasets (simulated - in practice these would come from BNM1 library)
        # This represents: PROC COPY IN=BNM1 OUT=BNM
        print("\nCopying LOAN datasets...")

        cleanup_work_datasets()

        # ================================================================
        # LOAN PROCESSING
        # ================================================================
        print("\n" + "-" * 70)
        print("LOAN PROCESSING")
        print("-" * 70)

        if not run_program('LALWPBBP', 'Loan Weekly Processing - PBB'):
            raise Exception("LALWPBBP failed")

        if not run_program('LALMPBBP', 'Loan Monthly Processing - PBB'):
            raise Exception("LALMPBBP failed")

        if not run_program('LALQPBBP', 'Loan Quarterly Processing - PBB'):
            raise Exception("LALQPBBP failed")

        # Delete LOAN datasets after processing
        loan_delete_files = [
            os.path.join(OUTPUT_DIR, f'LOAN{reptmon}{nowk}.parquet'),
            os.path.join(OUTPUT_DIR, f'ULOAN{reptmon}{nowk}.parquet'),
        ]
        delete_datasets(loan_delete_files)

        # ================================================================
        # KAPITI PROCESSING
        # ================================================================
        print("\n" + "-" * 70)
        print("KAPITI PROCESSING")
        print("-" * 70)

        if not run_program('KALWPBBP', 'KAPITI Weekly Processing - PBB'):
            raise Exception("KALWPBBP failed")

        if not run_program('KALMPBBP', 'KAPITI Monthly Processing - PBB (Part II)'):
            raise Exception("KALMPBBP failed")

        if not run_program('KALMSTOR', 'KAPITI Storage Processing'):
            raise Exception("KALMSTOR failed")

        if not run_program('NALMPBBP', 'NSRS KAPITI Processing (Part I)'):
            raise Exception("NALMPBBP failed")

        if not run_program('KALQPBBP', 'KAPITI Quarterly Processing - PBB'):
            raise Exception("KALQPBBP failed")

        cleanup_work_datasets()

        # ================================================================
        # REPORT GENERATION
        # ================================================================
        print("\n" + "-" * 70)
        print("REPORT GENERATION")
        print("-" * 70)

        if not run_program('EIBRDL1A', 'RDAL Report 1A'):
            raise Exception("EIBRDL1A failed")

        if not run_program('EIBRDL2A', 'RDAL Report 2A'):
            raise Exception("EIBRDL2A failed")

        if not run_program('EIBRDL3A', 'RDAL Report 3A'):
            raise Exception("EIBRDL3A failed")

        if not run_program('KALMLIFE', 'KAPITI Life Insurance Processing'):
            raise Exception("KALMLIFE failed")

        if not run_program('EIBMSAPC', 'Monthly Statistical Analysis Processing'):
            raise Exception("EIBMSAPC failed")

        if not run_program('EIBWRDLA', 'Weekly RDAL Processing'):
            raise Exception("EIBWRDLA failed")

        cleanup_work_datasets()

        # ================================================================
        # SFTP FILE PREPARATION
        # ================================================================
        print("\n" + "-" * 70)
        print("SFTP FILE PREPARATION")
        print("-" * 70)

        prepare_sftp_commands(macros)

        # ================================================================
        # PROCESSING COMPLETE
        # ================================================================
        print("\n" + "=" * 70)
        print("✓ ALL PROCESSING COMPLETED SUCCESSFULLY")
        print("=" * 70)

        print("\nOutput files generated:")
        print(f"  - RDAL KAPMNI report: {OUTPUT_DIR}/RDAL_KAPMNI.txt")
        print(f"  - NSRS KAPMNI report: {OUTPUT_DIR}/NSRS_KAPMNI.txt")
        print(f"  - SFTP commands: {OUTPUT_DIR}/sftp_commands.txt")

        return 0

    except Exception as e:
        print("\n" + "!" * 70)
        print(f"✗ PROCESSING FAILED: {e}")
        print("!" * 70)
        return 1


# =========================================================================
# EXECUTION
# =========================================================================

if __name__ == '__main__':
    sys.exit(main())
