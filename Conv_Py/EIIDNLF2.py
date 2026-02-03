#!/usr/bin/env python3
"""
File Name: EIIDNLF2
BNM Liquidity Framework - GL Consolidation
Processes cash holdings and SRR from Walker file
Consolidates all BNM liquidity data
"""

import duckdb
import polars as pl
from datetime import date, timedelta
from pathlib import Path
import calendar


# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Input paths
LOAN_REPTDATE_PATH = "/data/input/loan_reptdate.parquet"
WALK_FILE_PATH = "/data/input/walk.txt"
BNM_NOTE_PATH = "/data/input/bnm_note.parquet"
BNM_CALC_PATH = "/data/input/bnm_calc.parquet"

# Output paths
OUTPUT_DIR = "/data/output"
BNM_GLSET_PATH = f"{OUTPUT_DIR}/bnm_glset.parquet"
BNM_FINAL_PATH = f"{OUTPUT_DIR}/bnm_final.parquet"
BNM_FINALSUM_PATH = f"{OUTPUT_DIR}/bnm_finalsum.parquet"

REPORT_CASH_PATH = f"{OUTPUT_DIR}/cash_holdings_report.txt"
REPORT_SRR_PATH = f"{OUTPUT_DIR}/srr_report.txt"

# Create output directory
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


# ============================================================================
# DATE PROCESSING
# ============================================================================

def get_report_dates():
    """Get report dates and calculate week number"""
    # Load LOAN.REPTDATE
    reptdate_df = pl.read_parquet(LOAN_REPTDATE_PATH)
    reptdate = reptdate_df.select('REPTDATE').item(0, 0)

    if isinstance(reptdate, str):
        from datetime import datetime
        reptdate = datetime.strptime(reptdate, '%Y-%m-%d').date()

    day = reptdate.day

    # Determine week number based on day
    if day == 8:
        wk = '1'
    elif day == 15:
        wk = '2'
    elif day == 22:
        wk = '3'
    else:
        wk = '0'

    # Calculate start of next month
    if reptdate.month == 12:
        sreptdate = date(reptdate.year + 1, 1, 1)
    else:
        sreptdate = date(reptdate.year, reptdate.month + 1, 1)

    # Previous report date (last day of current month)
    prvrptdate = sreptdate - timedelta(days=1)

    # If reptdate is last day of month, set week to 4
    if reptdate == prvrptdate:
        wk = '4'

    nowk = wk
    rdate = reptdate.strftime('%d/%m/%y')
    rdat1 = int(reptdate.strftime('%y%m%d'))  # YYMDD format
    reptmon = f"{reptdate.month:02d}"
    reptday = f"{reptdate.day:02d}"

    return {
        'reptdate': reptdate,
        'nowk': nowk,
        'rdate': rdate,
        'rdat1': rdat1,
        'reptmon': reptmon,
        'reptday': reptday,
        'sreptdate': sreptdate,
        'prvrptdate': prvrptdate
    }


dates = get_report_dates()
REPTDATE = dates['reptdate']
NOWK = dates['nowk']
RDATE = dates['rdate']
RDAT1 = dates['rdat1']
REPTMON = dates['reptmon']
REPTDAY = dates['reptday']
SREPTDATE = dates['sreptdate']
PRVRPTDATE = dates['prvrptdate']

print(f"Report Date: {RDATE}")
print(f"Week Number: {NOWK}")
print(f"RDAT1: {RDAT1}")
print(f"Start of Next Month: {SREPTDATE}")
print(f"Previous Report Date (Month End): {PRVRPTDATE}")


# ============================================================================
# READ WALKER FILE
# ============================================================================

print("\nStep 1: Reading Walker file...")

# Read fixed-width Walker file
walk_records = []

with open(WALK_FILE_PATH, 'r') as f:
    for line_num, line in enumerate(f, 1):
        if line_num < 1:  # FIRSTOBS=1 means start from first line (0-indexed would skip header)
            continue

        if len(line) < 42:
            continue

        # Parse fields based on positions
        # @2   DESC          $19.  (position 2-20, 0-indexed: 1-20)
        # @21  DD              2.   (position 21-22, 0-indexed: 20-22)
        # @24  MM              2.   (position 24-25, 0-indexed: 23-25)
        # @27  YY              2.   (position 27-28, 0-indexed: 26-28)
        # @42  AMOUNT    COMMA20.2  (position 42+, 0-indexed: 41+)

        desc = line[1:20].strip()  # @2 means starting at position 2 (1-indexed), so 0-indexed is 1

        try:
            dd = int(line[20:22].strip())
            mm = int(line[23:25].strip())
            yy = int(line[26:28].strip())
        except (ValueError, IndexError):
            continue

        # Parse amount - remove commas
        amount_str = line[41:].strip().replace(',', '')
        try:
            amount = float(amount_str)
        except ValueError:
            continue

        # Convert 2-digit year to 4-digit year
        if yy >= 50:
            year = 1900 + yy
        else:
            year = 2000 + yy

        # Create date
        try:
            reptdate = date(year, mm, dd)
        except ValueError:
            continue

        # Filter for current report date
        reptdate_int = int(reptdate.strftime('%y%m%d'))
        if reptdate_int == RDAT1:
            walk_records.append({
                'DESC': desc,
                'REPTDATE': reptdate,
                'AMOUNT': amount
            })

walk = pl.DataFrame(walk_records)
print(f"  Walker records loaded for {RDATE}: {len(walk)}")


# ============================================================================
# PROCESS CASH HOLDINGS
# ============================================================================

print("\nStep 2: Processing Cash Holdings...")

# Create default records for all buckets
cash_default_records = []
for n in range(1, 7):
    cash_default_records.append({'BNMCODE': f'93221000{n}0000Y', 'AMOUNT': 0.0})
    cash_default_records.append({'BNMCODE': f'95221000{n}0000Y', 'AMOUNT': 0.0})

cash_default = pl.DataFrame(cash_default_records).sort('BNMCODE')

# Summarize cash holdings (DESC = '39110')
cash_walk = walk.filter(pl.col('DESC') == '39110')

if len(cash_walk) > 0:
    cash_summary = cash_walk.group_by('REPTDATE').agg([
        pl.col('AMOUNT').sum().alias('AMOUNT')
    ])

    cash_amount = cash_summary.select('AMOUNT').item(0, 0)
    print(f"  Cash Holdings amount: {cash_amount:,.2f}")

    # Create BNMCODE records
    cash_records = [
        {'BNMCODE': '9322100010000Y', 'AMOUNT': cash_amount},
        {'BNMCODE': '9522100010000Y', 'AMOUNT': cash_amount}
    ]
    cash_data = pl.DataFrame(cash_records)
else:
    print(f"  No cash holdings found for DESC='39110'")
    cash_data = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

# Merge with defaults
cash = cash_default.join(
    cash_data,
    on='BNMCODE',
    how='left',
    suffix='_data'
).with_columns([
    pl.when(pl.col('AMOUNT_data').is_not_null())
    .then(pl.col('AMOUNT_data'))
    .otherwise(pl.col('AMOUNT'))
    .alias('AMOUNT')
]).select(['BNMCODE', 'AMOUNT']).sort('BNMCODE')

print(f"  Cash Holdings records: {len(cash)}")


# ============================================================================
# PROCESS SRR (Statutory Reserve Requirement)
# ============================================================================

print("\nStep 3: Processing SRR...")

# Create default records for all buckets
srr_default_records = []
for n in range(1, 7):
    srr_default_records.append({'BNMCODE': f'93222000{n}0000Y', 'AMOUNT': 0.0})
    srr_default_records.append({'BNMCODE': f'95222000{n}0000Y', 'AMOUNT': 0.0})

srr_default = pl.DataFrame(srr_default_records).sort('BNMCODE')

# Summarize SRR (DESC = '32110')
srr_walk = walk.filter(pl.col('DESC') == '32110')

if len(srr_walk) > 0:
    srr_summary = srr_walk.group_by('REPTDATE').agg([
        pl.col('AMOUNT').sum().alias('AMOUNT')
    ])

    srr_amount = srr_summary.select('AMOUNT').item(0, 0)
    print(f"  SRR amount: {srr_amount:,.2f}")

    # Create BNMCODE records - bucket 06 (>1 year)
    srr_records = [
        {'BNMCODE': '9322200060000Y', 'AMOUNT': srr_amount},
        {'BNMCODE': '9522200060000Y', 'AMOUNT': srr_amount}
    ]
    srr_data = pl.DataFrame(srr_records)
else:
    print(f"  No SRR found for DESC='32110'")
    srr_data = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

# Merge with defaults
srr = srr_default.join(
    srr_data,
    on='BNMCODE',
    how='left',
    suffix='_data'
).with_columns([
    pl.when(pl.col('AMOUNT_data').is_not_null())
    .then(pl.col('AMOUNT_data'))
    .otherwise(pl.col('AMOUNT'))
    .alias('AMOUNT')
]).select(['BNMCODE', 'AMOUNT']).sort('BNMCODE')

print(f"  SRR records: {len(srr)}")


# ============================================================================
# CREATE GLSET
# ============================================================================

print("\nStep 4: Creating GLSET...")

bnm_glset = pl.concat([cash, srr])
bnm_glset.write_parquet(BNM_GLSET_PATH)
print(f"  GLSET records: {len(bnm_glset)}")
print(f"  Saved to: {BNM_GLSET_PATH}")


# ============================================================================
# CONSOLIDATION
# ============================================================================

print("\nStep 5: Consolidating all BNM data...")

# Load BNM.NOTE and BNM.CALC
bnm_note = pl.read_parquet(BNM_NOTE_PATH)
bnm_calc = pl.read_parquet(BNM_CALC_PATH)

print(f"  BNM NOTE records: {len(bnm_note)}")
print(f"  BNM CALC records: {len(bnm_calc)}")
print(f"  BNM GLSET records: {len(bnm_glset)}")

# Combine all datasets
bnm_final = pl.concat([bnm_note, bnm_calc, bnm_glset]).sort('BNMCODE')

print(f"  BNM FINAL records (before summarizing): {len(bnm_final)}")

# Save FINAL
bnm_final.write_parquet(BNM_FINAL_PATH)
print(f"  Saved to: {BNM_FINAL_PATH}")


# ============================================================================
# SUMMARIZE BY BNMCODE
# ============================================================================

print("\nStep 6: Summarizing by BNMCODE...")

bnm_finalsum = bnm_final.group_by('BNMCODE').agg([
    pl.col('AMOUNT').sum().alias('AMOUNT')
]).sort('BNMCODE')

print(f"  BNM FINALSUM records: {len(bnm_finalsum)}")

# Save FINALSUM
bnm_finalsum.write_parquet(BNM_FINALSUM_PATH)
print(f"  Saved to: {BNM_FINALSUM_PATH}")


# ============================================================================
# GENERATE REPORTS
# ============================================================================

print("\nStep 7: Generating reports...")


def write_report_with_asa(filepath, title, data_df):
    """Write a report with ASA carriage control characters"""

    PAGE_LENGTH = 60
    line_count = 0
    page_number = 1

    with open(filepath, 'w') as f:

        def write_line(asa_char, content):
            """Write a line with ASA carriage control character"""
            nonlocal line_count
            f.write(f"{asa_char}{content}\n")
            line_count += 1

        def write_header():
            """Write page header"""
            nonlocal line_count, page_number
            write_line('1', ' ' * 100)  # Form feed
            write_line(' ', title.center(100))
            write_line(' ', f'{RDATE}'.center(100))
            write_line(' ', ' ' * 100)
            write_line(' ', f"{'BNMCODE':<20} {'AMOUNT':>20}")
            write_line(' ', '-' * 100)
            line_count = 6
            page_number += 1

        # Write header
        write_header()

        # Write data
        for row in data_df.iter_rows(named=True):
            # Check if we need a new page
            if line_count >= PAGE_LENGTH - 3:
                write_header()

            bnmcode = row['BNMCODE']
            amount = row['AMOUNT']

            line = f" {bnmcode:<20} {amount:20,.2f}"
            write_line(' ', line)

        # Write total
        total = data_df.select(pl.col('AMOUNT').sum()).item(0, 0)
        write_line(' ', '-' * 100)
        write_line(' ', f" {'TOTAL':<20} {total:20,.2f}")

    print(f"    Report written to: {filepath}")


# Generate Cash Holdings report
write_report_with_asa(REPORT_CASH_PATH, 'CASH HOLDINGS', cash)

# Generate SRR report
write_report_with_asa(REPORT_SRR_PATH, 'SRR', srr)


# ============================================================================
# SUMMARY STATISTICS
# ============================================================================

print("\n" + "=" * 70)
print("BNM GL Consolidation completed successfully!")
print("=" * 70)

# Calculate totals by category
cash_total = cash.select(pl.col('AMOUNT').sum()).item(0, 0)
srr_total = srr.select(pl.col('AMOUNT').sum()).item(0, 0)
final_total = bnm_finalsum.select(pl.col('AMOUNT').sum()).item(0, 0)

print(f"\nSummary Statistics:")
print(f"  Cash Holdings total: {cash_total:,.2f}")
print(f"  SRR total: {srr_total:,.2f}")
print(f"  GL Set total: {cash_total + srr_total:,.2f}")
print(f"  Final consolidated total: {final_total:,.2f}")

print(f"\nRecord Counts:")
print(f"  BNM NOTE: {len(bnm_note)}")
print(f"  BNM CALC: {len(bnm_calc)}")
print(f"  BNM GLSET: {len(bnm_glset)}")
print(f"  BNM FINAL (before sum): {len(bnm_final)}")
print(f"  BNM FINALSUM (after sum): {len(bnm_finalsum)}")

print(f"\nOutput files:")
print(f"  - GLSET: {BNM_GLSET_PATH}")
print(f"  - FINAL: {BNM_FINAL_PATH}")
print(f"  - FINALSUM: {BNM_FINALSUM_PATH}")
print(f"  - Cash Holdings Report: {REPORT_CASH_PATH}")
print(f"  - SRR Report: {REPORT_SRR_PATH}")
