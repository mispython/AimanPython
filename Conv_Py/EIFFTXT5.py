#!/usr/bin/env python3
"""
Program: EIFFTXT5.py
Date: 2016-4547 (ESMR)
Purpose: STREAMLINE HP LOTUS NOTES WRITE-OFF SYSTEM

This program:
1. Reads write-off text files from PIBB and PBB sources
2. Combines them into a single output file
3. Adds file header (FH) and file trailer (FT) records
4. Prepares SFTP commands for file transfer to WebNotes and SAS Data Warehouse
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# =========================================================================
# ENVIRONMENT SETUP
# =========================================================================

# Path Configuration
UPLOAD_DIR = os.getenv('UPLOAD_DIR', '/mnt/user-data/uploads')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '/mnt/user-data/outputs')

# Input files
PIBB_INPUT_FILE = os.path.join(UPLOAD_DIR, 'FTPLNS_WOFFTXT_PIBB.txt')
PBB_INPUT_FILE = os.path.join(UPLOAD_DIR, 'FTPLNS_WOFFTXT_PBB.txt')

# Output files
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'FTPLNS_WOFFTXT_ALL.txt')
SFTP_WEBNOTES = os.path.join(OUTPUT_DIR, 'sftp_webnotes.txt')
SFTP_DRR = os.path.join(OUTPUT_DIR, 'sftp_drr.txt')


# =========================================================================
# MAIN PROCESSING
# =========================================================================

def read_woff_files():
    """
    Read write-off text files from PIBB and PBB sources.
    Each record is 800 characters.

    Returns:
        list: List of 800-character records
    """
    print("Reading write-off input files...")

    records = []

    # Read PIBB file
    if os.path.exists(PIBB_INPUT_FILE):
        print(f"  Reading: {PIBB_INPUT_FILE}")
        with open(PIBB_INPUT_FILE, 'r') as f:
            for line in f:
                # Pad or truncate to exactly 800 characters
                record = line.rstrip('\n\r')
                record = record.ljust(800)[:800]
                records.append(record)
        print(f"    Records read: {len(records)}")
    else:
        print(f"  Warning: {PIBB_INPUT_FILE} not found")

    # Read PBB file
    pibb_count = len(records)
    if os.path.exists(PBB_INPUT_FILE):
        print(f"  Reading: {PBB_INPUT_FILE}")
        with open(PBB_INPUT_FILE, 'r') as f:
            for line in f:
                # Pad or truncate to exactly 800 characters
                record = line.rstrip('\n\r')
                record = record.ljust(800)[:800]
                records.append(record)
        print(f"    Records read: {len(records) - pibb_count}")
    else:
        print(f"  Warning: {PBB_INPUT_FILE} not found")

    print(f"\nTotal records read: {len(records)}")

    return records


def write_output_file(records):
    """
    Write output file with FH header and FT trailer.

    File format:
    - Line 1: FH header with run date (FHYYYYMMDD)
    - Lines 2-n: Data records (800 chars each)
    - Line n+1: FT trailer with record count (FTnnnnnnnn)

    Args:
        records: List of 800-character data records
    """
    print(f"\nWriting output file: {OUTPUT_FILE}")

    # Get run date in YYYYMMDD format
    rundate = datetime.now()
    rundate_str = rundate.strftime('%Y%m%d')

    # Calculate total record count (data records only, not including header/trailer)
    record_count = len(records)

    with open(OUTPUT_FILE, 'w') as f:
        # Write FH header record
        fh_record = f'FH{rundate_str}'.ljust(800)
        f.write(fh_record + '\n')

        # Write all data records
        for record in records:
            f.write(record + '\n')

        # Write FT trailer record with zero-padded count
        ft_record = f'FT{record_count:08d}'.ljust(800)
        f.write(ft_record + '\n')

    print(f"  File header: FH{rundate_str}")
    print(f"  Data records: {record_count}")
    print(f"  File trailer: FT{record_count:08d}")
    print(f"  Total lines written: {record_count + 2}")


def prepare_sftp_webnotes():
    """
    Prepare SFTP command file for transfer to WebNotes (HP Write-Off system).

    The file is named with current date: HPTXTyyyymmdd.TXT
    """
    print(f"\nPreparing SFTP commands for WebNotes: {SFTP_WEBNOTES}")

    rundate = datetime.now()

    # Generate filename with date: HPTXTyyyymmdd.TXT
    # Using %CYYYY.%CMM.%CDD format from JCL (current year/month/day)
    filename = f'HPTXT{rundate.strftime("%Y%m%d")}.TXT'

    with open(SFTP_WEBNOTES, 'w') as f:
        f.write("# SFTP commands for WebNotes transfer\n")
        f.write(f"# Generated: {rundate.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("#\n")
        f.write("# Target: ftpprod@192.168.68.150\n")
        f.write("# Directory: \\HPWOFF\n")
        f.write("#\n")
        f.write("export PASSWD_DSN='OPER.PBB.CONTROL(PAS#LTS)'\n")
        f.write("$coz_bin/cozsftp $ssh_opts -b- ftpprod@192.168.68.150 <<EOB\n")
        f.write("lzopts servercp=$servercp,notrim,overflow=trunc,mode=text\n")
        f.write("lzopts linerule=$lr\n")
        f.write("CD \\HPWOFF\n")
        f.write(f"PUT //SAP.PBB.FTPLNS.WOFFTXT.ALL  {filename}\n")
        f.write("EOB\n")

    print(f"  Target filename: {filename}")
    print(f"  Target server: ftpprod@192.168.68.150")
    print(f"  Target directory: \\HPWOFF")


def prepare_sftp_drr():
    """
    Prepare SFTP command file for transfer to SAS Data Warehouse (DRR).

    The file is named with current date: HPTXTyyyymmdd.TXT
    """
    print(f"\nPreparing SFTP commands for DRR: {SFTP_DRR}")

    rundate = datetime.now()

    # Generate filename with date: HPTXTyyyymmdd.TXT
    filename = f'HPTXT{rundate.strftime("%Y%m%d")}.TXT'

    with open(SFTP_DRR, 'w') as f:
        f.write("# SFTP commands for SAS Data Warehouse (DRR) transfer\n")
        f.write(f"# Generated: {rundate.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("#\n")
        f.write("# Using DRR#SFTP parameter library settings\n")
        f.write("# Directory: HPCC/WRITTEN-OFF\n")
        f.write("#\n")
        f.write("lzopts servercp=$servercp,notrim,overflow=trunc,mode=text\n")
        f.write("lzopts linerule=$lr\n")
        f.write("cd HPCC/WRITTEN-OFF\n")
        f.write(f"PUT //SAP.PBB.FTPLNS.WOFFTXT.ALL  {filename}\n")
        f.write("EOB\n")

    print(f"  Target filename: {filename}")
    print(f"  Target directory: HPCC/WRITTEN-OFF")


# =========================================================================
# MAIN EXECUTION
# =========================================================================

def main():
    """Main processing function."""
    print("=" * 70)
    print("EIFFTXT5 - HP LOTUS NOTES WRITE-OFF TEXT FILE PROCESSOR")
    print("=" * 70)
    print(f"\nRun Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Read input files
        records = read_woff_files()

        if len(records) == 0:
            print("\nWarning: No records read from input files")
            print("Output file will contain only header and trailer")

        # Write output file with header and trailer
        write_output_file(records)

        # Prepare SFTP command files
        prepare_sftp_webnotes()
        prepare_sftp_drr()

        # Summary
        print("\n" + "=" * 70)
        print("✓ PROCESSING COMPLETED SUCCESSFULLY")
        print("=" * 70)

        print("\nOutput files generated:")
        print(f"  1. Combined write-off file: {OUTPUT_FILE}")
        print(f"  2. SFTP commands (WebNotes): {SFTP_WEBNOTES}")
        print(f"  3. SFTP commands (DRR): {SFTP_DRR}")

        print("\nFile statistics:")
        print(f"  Data records: {len(records)}")
        print(f"  Total lines (incl. header/trailer): {len(records) + 2}")
        print(f"  Record length: 800 characters")

        print("\nNext steps:")
        print("  1. Execute SFTP commands to transfer file to WebNotes")
        print("  2. Execute SFTP commands to transfer file to DRR")

        return 0

    except Exception as e:
        print("\n" + "!" * 70)
        print(f"✗ PROCESSING FAILED: {e}")
        print("!" * 70)
        import traceback
        traceback.print_exc()
        return 1


# =========================================================================
# EXECUTION
# =========================================================================

if __name__ == '__main__':
    sys.exit(main())
