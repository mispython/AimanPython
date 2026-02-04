#!/usr/bin/env python3
"""
File Name: EIBDNLF2
GL Items and Final Consolidation
Processes GL items from WALK file and consolidates all BNM datasets
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# INPUT_DIR = Path("/path/to/input")
# OUTPUT_DIR = Path("/path/to/output")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"

# Input files
REPTDATE_PATH = INPUT_DIR / "REPTDATE.parquet"
WALK_PATH = INPUT_DIR / "WALK.txt"
NOTE_PATH = INPUT_DIR / "NOTE.parquet"
CALC_PATH = INPUT_DIR / "CALC.parquet"
PBIF_PATH = INPUT_DIR / "PBIF.parquet"
BT_PATH = INPUT_DIR / "BT.parquet"

# Output files
GLSET_OUTPUT_PATH = OUTPUT_DIR / "GLSET.parquet"
FINAL_OUTPUT_PATH = OUTPUT_DIR / "FINAL.parquet"
FINALSUM_OUTPUT_PATH = OUTPUT_DIR / "FINALSUM.parquet"

# Individual GL reports
FS_REPORT_PATH = OUTPUT_DIR / "FS_REPORT.txt"
HP_REPORT_PATH = OUTPUT_DIR / "HP_REPORT.txt"
CASH_REPORT_PATH = OUTPUT_DIR / "CASH_REPORT.txt"
SRR_REPORT_PATH = OUTPUT_DIR / "SRR_REPORT.txt"
OTH_REPORT_PATH = OUTPUT_DIR / "OTH_REPORT.txt"
FCYFL_REPORT_PATH = OUTPUT_DIR / "FCYFL_REPORT.txt"
FCYRC_REPORT_PATH = OUTPUT_DIR / "FCYRC_REPORT.txt"
FCYCASH_REPORT_PATH = OUTPUT_DIR / "FCYCASH_REPORT.txt"
GLSET_REPORT_PATH = OUTPUT_DIR / "GLSET_REPORT.txt"
FINAL_REPORT_PATH = OUTPUT_DIR / "FINAL_REPORT.txt"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# READ REPTDATE
# ============================================================================

def assert_file_exists(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

def read_reptdate():
    """Read report date and calculate week number."""
    print("Reading REPTDATE...")

    assert_file_exists(REPTDATE_PATH)

    df = pl.read_parquet(REPTDATE_PATH)
    reptdate = df['REPTDATE'][0]

    if isinstance(reptdate, str):
        reptdate = datetime.strptime(reptdate, "%Y-%m-%d").date()

    # Calculate week number
    day = reptdate.day
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
        sreptdate = datetime(reptdate.year + 1, 1, 1).date()
    else:
        sreptdate = datetime(reptdate.year, reptdate.month + 1, 1).date()

    # Previous report date = day before start of next month
    prvrptdate = sreptdate - timedelta(days=1)

    # If reptdate is month-end, set week to 4
    if reptdate == prvrptdate:
        wk = '4'

    nowk = wk
    reptmon = f"{reptdate.month:02d}"
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%Y")
    rdat1 = int(reptdate.strftime("%y%m%d"))

    print(f"Report Date: {reptdate} (Week {nowk})")
    print(f"RDAT1: {rdat1}")

    return {
        'reptdate': reptdate,
        'nowk': nowk,
        'reptmon': reptmon,
        'reptday': reptday,
        'rdate': rdate,
        'rdat1': rdat1,
    }


# ============================================================================
# READ WALK FILE
# ============================================================================

def read_walk_file(params):
    """Read WALK file and filter for current report date."""
    print("Reading WALK file...")

    walk_records = []

    with open(WALK_PATH, 'r') as f:
        for line in f:
            if len(line) >= 42:
                desc = line[1:20].strip()
                dd_str = line[20:22].strip()
                mm_str = line[23:25].strip()
                yy_str = line[26:28].strip()
                amount_str = line[41:61].strip()

                try:
                    dd = int(dd_str) if dd_str else 0
                    mm = int(mm_str) if mm_str else 0
                    yy = int(yy_str) if yy_str else 0
                    amount = float(amount_str.replace(',', ''))
                except:
                    continue

                if dd == 0 or mm == 0:
                    continue

                # Construct date
                year = 2000 + yy if yy < 50 else 1900 + yy
                try:
                    reptdate_val = datetime(year, mm, dd).date()
                    reptdate_int = int(reptdate_val.strftime("%y%m%d"))
                except:
                    continue

                if reptdate_int == params['rdat1']:
                    walk_records.append({
                        'DESC': desc,
                        'REPTDATE': reptdate_int,
                        'AMOUNT': amount
                    })

    df_walk = pl.DataFrame(walk_records) if walk_records else pl.DataFrame({
        'DESC': [], 'REPTDATE': [], 'AMOUNT': []
    })

    print(f"WALK records for current date: {len(df_walk)}")

    return df_walk


# ============================================================================
# CREATE DEFAULT RECORDS
# ============================================================================

def create_default_records(prefix_pattern, item):
    """
    Create default zero-amount records for all buckets.

    Args:
        prefix_pattern: Pattern like '9321909' or '9422100'
        item: Last digit or identifier (usually '0' or specific number)

    Returns:
        DataFrame with default records
    """
    default_records = []

    for n in range(1, 7):
        bucket = f"{n:02d}"

        # Regular and NPL records
        if prefix_pattern.startswith('93') or prefix_pattern.startswith('95'):
            bnmcode_npl = f"{prefix_pattern[0:7]}{bucket}0000Y"
            bnmcode_reg = f"95{prefix_pattern[2:7]}{bucket}0000Y"
            if prefix_pattern.startswith('93'):
                bnmcode_npl = f"{prefix_pattern[0:7]}{bucket}0000Y"
                bnmcode_reg = f"95{prefix_pattern[2:7]}{bucket}0000Y"
            else:
                bnmcode_reg = f"{prefix_pattern[0:7]}{bucket}0000Y"
                bnmcode_npl = f"93{prefix_pattern[2:7]}{bucket}0000Y"

            default_records.append({'BNMCODE': bnmcode_npl, 'AMOUNT': 0.0})
            default_records.append({'BNMCODE': bnmcode_reg, 'AMOUNT': 0.0})
        elif prefix_pattern.startswith('94') or prefix_pattern.startswith('96'):
            # FCY codes
            bnmcode_npl = f"94{prefix_pattern[2:7]}{bucket}0000Y"
            bnmcode_reg = f"96{prefix_pattern[2:7]}{bucket}0000Y"

            default_records.append({'BNMCODE': bnmcode_npl, 'AMOUNT': 0.0})
            default_records.append({'BNMCODE': bnmcode_reg, 'AMOUNT': 0.0})

    return pl.DataFrame(default_records).sort('BNMCODE')


# ============================================================================
# PROCESS GL ITEMS
# ============================================================================

def process_floor_stocking(df_walk):
    """Process Floor Stocking (FS) from WALK file."""
    print("\nProcessing Floor Stocking...")

    # Create defaults
    df_default = create_default_records('9321909', '0')

    # Filter and sum
    df_fs = df_walk.filter(pl.col('DESC') == 'ML34170').group_by('REPTDATE').agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_fs) > 0:
        amount = df_fs['AMOUNT'][0]
        fs_records = [
            {'BNMCODE': '932190905' + '0000Y', 'AMOUNT': amount},
            {'BNMCODE': '952190905' + '0000Y', 'AMOUNT': amount},
        ]
        df_fs_out = pl.DataFrame(fs_records)
    else:
        df_fs_out = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    # Merge with defaults
    df_result = df_default.join(df_fs_out, on='BNMCODE', how='left', suffix='_new')
    df_result = df_result.with_columns(
        pl.coalesce([pl.col('AMOUNT_new'), pl.col('AMOUNT')]).alias('AMOUNT')
    ).select(['BNMCODE', 'AMOUNT'])

    generate_report(df_result, FS_REPORT_PATH, 'FLOOR STOCKING')

    return df_result


def process_hire_purchase(df_walk):
    """Process Hire Purchase Agency (HP) from WALK file."""
    print("Processing Hire Purchase...")

    # Create defaults
    df_default = create_default_records('9321109', '0')

    # Filter and sum
    df_hp = df_walk.filter(pl.col('DESC') == 'ML34111').group_by('REPTDATE').agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_hp) > 0:
        amount = df_hp['AMOUNT'][0]
        hp_records = [
            {'BNMCODE': '932110906' + '0000Y', 'AMOUNT': amount},
            {'BNMCODE': '952110906' + '0000Y', 'AMOUNT': amount},
        ]
        df_hp_out = pl.DataFrame(hp_records)
    else:
        df_hp_out = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    # Merge with defaults
    df_result = df_default.join(df_hp_out, on='BNMCODE', how='left', suffix='_new')
    df_result = df_result.with_columns(
        pl.coalesce([pl.col('AMOUNT_new'), pl.col('AMOUNT')]).alias('AMOUNT')
    ).select(['BNMCODE', 'AMOUNT'])

    generate_report(df_result, HP_REPORT_PATH, 'HIRE PURCHASE AGENCY')

    return df_result


def process_cash_holdings(df_walk):
    """Process Cash Holdings from WALK file."""
    print("Processing Cash Holdings...")

    # Filter and sum (no defaults needed for cash)
    df_cash = df_walk.filter(pl.col('DESC') == '39110').group_by('REPTDATE').agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_cash) > 0:
        amount = df_cash['AMOUNT'][0]
        cash_records = [
            {'BNMCODE': '932210001' + '0000Y', 'AMOUNT': amount},
            {'BNMCODE': '952210001' + '0000Y', 'AMOUNT': amount},
        ]
        df_result = pl.DataFrame(cash_records)
    else:
        df_result = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    generate_report(df_result, CASH_REPORT_PATH, 'CASH HOLDINGS')

    return df_result


def process_srr(df_walk):
    """Process Statutory Reserve Requirement (SRR) from WALK file."""
    print("Processing SRR...")

    # Create defaults
    df_default = create_default_records('9322200', '0')

    # Filter and sum
    df_srr = df_walk.filter(pl.col('DESC') == '32110').group_by('REPTDATE').agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_srr) > 0:
        amount = df_srr['AMOUNT'][0]
        srr_records = [
            {'BNMCODE': '932220006' + '0000Y', 'AMOUNT': amount},
            {'BNMCODE': '952220006' + '0000Y', 'AMOUNT': amount},
        ]
        df_srr_out = pl.DataFrame(srr_records)
    else:
        df_srr_out = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    # Merge with defaults
    df_result = df_default.join(df_srr_out, on='BNMCODE', how='left', suffix='_new')
    df_result = df_result.with_columns(
        pl.coalesce([pl.col('AMOUNT_new'), pl.col('AMOUNT')]).alias('AMOUNT')
    ).select(['BNMCODE', 'AMOUNT'])

    generate_report(df_result, SRR_REPORT_PATH, 'SRR')

    return df_result


def process_other_assets(df_walk):
    """Process Other Assets (Shares Held) from WALK file."""
    print("Processing Other Assets...")

    # Filter and sum (no defaults needed)
    df_oth = df_walk.filter(pl.col('DESC') == 'F137010SH').group_by('REPTDATE').agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_oth) > 0:
        amount = df_oth['AMOUNT'][0]
        oth_records = [
            {'BNMCODE': '932290001' + '0000Y', 'AMOUNT': amount},
            {'BNMCODE': '952290001' + '0000Y', 'AMOUNT': amount},
        ]
        df_result = pl.DataFrame(oth_records)
    else:
        df_result = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    generate_report(df_result, OTH_REPORT_PATH, 'OTHER ASSETS')

    return df_result


def process_fcy_fl(df_walk):
    """Process FCY Fixed Loan from WALK file."""
    print("Processing FCY - FL...")

    # Create defaults
    df_default = create_default_records('9421109', '0')

    # Filter and sum
    df_fcyfl = df_walk.filter(
        pl.col('DESC').is_in(['F13460081BCB', 'F13460064FLB'])
    ).group_by('REPTDATE').agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_fcyfl) > 0:
        amount = df_fcyfl['AMOUNT'][0]
        fcyfl_records = [
            {'BNMCODE': '942110906' + '0000Y', 'AMOUNT': amount},
            {'BNMCODE': '962110906' + '0000Y', 'AMOUNT': amount},
        ]
        df_fcyfl_out = pl.DataFrame(fcyfl_records)
    else:
        df_fcyfl_out = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    # Merge with defaults
    df_result = df_default.join(df_fcyfl_out, on='BNMCODE', how='left', suffix='_new')
    df_result = df_result.with_columns(
        pl.coalesce([pl.col('AMOUNT_new'), pl.col('AMOUNT')]).alias('AMOUNT')
    ).select(['BNMCODE', 'AMOUNT'])

    generate_report(df_result, FCYFL_REPORT_PATH, 'FCY - FL')

    return df_result


def process_fcy_rc(df_walk):
    """Process FCY Revolving Credit from WALK file."""
    print("Processing FCY - RC...")

    # Create defaults
    df_default = create_default_records('9421209', '0')

    # Filter and sum
    df_fcyrc = df_walk.filter(pl.col('DESC') == 'F134600RC').group_by('REPTDATE').agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_fcyrc) > 0:
        amount = df_fcyrc['AMOUNT'][0]
        fcyrc_records = [
            {'BNMCODE': '942120906' + '0000Y', 'AMOUNT': amount},
            {'BNMCODE': '962120906' + '0000Y', 'AMOUNT': amount},
        ]
        df_fcyrc_out = pl.DataFrame(fcyrc_records)
    else:
        df_fcyrc_out = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    # Merge with defaults
    df_result = df_default.join(df_fcyrc_out, on='BNMCODE', how='left', suffix='_new')
    df_result = df_result.with_columns(
        pl.coalesce([pl.col('AMOUNT_new'), pl.col('AMOUNT')]).alias('AMOUNT')
    ).select(['BNMCODE', 'AMOUNT'])

    generate_report(df_result, FCYRC_REPORT_PATH, 'FCY - RC')

    return df_result


def process_fcy_cash(df_walk):
    """Process FCY Cash Holdings from WALK file."""
    print("Processing FCY - Cash Holdings...")

    # Create defaults
    df_default = create_default_records('9422100', '0')

    # Filter and sum
    df_fcycash = df_walk.filter(pl.col('DESC') == 'F139610FXNC').group_by('REPTDATE').agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_fcycash) > 0:
        amount = df_fcycash['AMOUNT'][0]
        fcycash_records = [
            {'BNMCODE': '942210001' + '0000Y', 'AMOUNT': amount},
            {'BNMCODE': '962210001' + '0000Y', 'AMOUNT': amount},
        ]
        df_fcycash_out = pl.DataFrame(fcycash_records)
    else:
        df_fcycash_out = pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    # Merge with defaults
    df_result = df_default.join(df_fcycash_out, on='BNMCODE', how='left', suffix='_new')
    df_result = df_result.with_columns(
        pl.coalesce([pl.col('AMOUNT_new'), pl.col('AMOUNT')]).alias('AMOUNT')
    ).select(['BNMCODE', 'AMOUNT'])

    generate_report(df_result, FCYCASH_REPORT_PATH, 'FCY - CASH HOLDINGS')

    return df_result


# ============================================================================
# GENERATE REPORTS
# ============================================================================

def generate_report(df, report_path, title):
    """Generate individual GL item report."""
    with open(report_path, 'w') as f:
        f.write(' ' + '=' * 80 + '\n')
        f.write(f' {title}\n')
        f.write(' ' + '=' * 80 + '\n')
        f.write(' \n')
        f.write(f' {"BNMCODE":<20} {"AMOUNT":>20}\n')
        f.write(' ' + '-' * 80 + '\n')

        total = 0.0
        for row in df.iter_rows(named=True):
            bnmcode = row['BNMCODE']
            amount = row['AMOUNT']
            total += amount
            f.write(f' {bnmcode:<20} {amount:>20,.2f}\n')

        f.write(' ' + '-' * 80 + '\n')
        f.write(f' {"TOTAL":<20} {total:>20,.2f}\n')
        f.write(' ' + '=' * 80 + '\n')


def generate_glset_report(df):
    """Generate GLSET combined report."""
    with open(GLSET_REPORT_PATH, 'w') as f:
        f.write(' ' + '=' * 80 + '\n')
        f.write(' GLSET - ALL GL ITEMS\n')
        f.write(' ' + '=' * 80 + '\n')
        f.write(' \n')
        f.write(f' {"BNMCODE":<20} {"AMOUNT":>20}\n')
        f.write(' ' + '-' * 80 + '\n')

        total = 0.0
        for row in df.iter_rows(named=True):
            bnmcode = row['BNMCODE']
            amount = row['AMOUNT']
            total += amount
            f.write(f' {bnmcode:<20} {amount:>20,.2f}\n')

        f.write(' ' + '-' * 80 + '\n')
        f.write(f' {"TOTAL":<20} {total:>20,.2f}\n')
        f.write(' ' + '=' * 80 + '\n')


def generate_final_report(df):
    """Generate FINAL consolidated report summary."""
    with open(FINAL_REPORT_PATH, 'w') as f:
        f.write(' ' + '=' * 80 + '\n')
        f.write(' FINAL CONSOLIDATED BNM REPORT\n')
        f.write(' ' + '=' * 80 + '\n')
        f.write(' \n')
        f.write(f' {"BNMCODE":<20} {"AMOUNT":>20}\n')
        f.write(' ' + '-' * 80 + '\n')

        # Sort by BNMCODE
        df_sorted = df.sort('BNMCODE')

        total = 0.0
        for row in df_sorted.iter_rows(named=True):
            bnmcode = row['BNMCODE']
            amount = row['AMOUNT']
            total += amount
            f.write(f' {bnmcode:<20} {amount:>20,.2f}\n')

        f.write(' ' + '-' * 80 + '\n')
        f.write(f' {"TOTAL":<20} {total:>20,.2f}\n')
        f.write(' ' + '=' * 80 + '\n')
        f.write(' \n')
        f.write(f' Total unique BNMCODE entries: {len(df_sorted)}\n')
        f.write(' ' + '=' * 80 + '\n')


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    try:
        print("=" * 80)
        print("GL Items Processing and Final Consolidation")
        print("=" * 80)

        # Read reptdate
        params = read_reptdate()

        # Read WALK file
        df_walk = read_walk_file(params)

        # Process all GL items
        df_fs = process_floor_stocking(df_walk)
        df_hp = process_hire_purchase(df_walk)
        df_cash = process_cash_holdings(df_walk)
        df_srr = process_srr(df_walk)
        df_oth = process_other_assets(df_walk)
        df_fcyfl = process_fcy_fl(df_walk)
        df_fcyrc = process_fcy_rc(df_walk)
        df_fcycash = process_fcy_cash(df_walk)

        # Combine all GL items into GLSET
        df_glset = pl.concat([
            df_fs,
            df_hp,
            df_cash,
            df_srr,
            df_oth,
            df_fcyfl,
            df_fcycash,
            df_fcyrc,
        ])

        # Write GLSET
        df_glset.write_parquet(GLSET_OUTPUT_PATH)
        print(f"\nGLSET written to {GLSET_OUTPUT_PATH}")
        print(f"GLSET records: {len(df_glset)}")

        # Generate GLSET report
        generate_glset_report(df_glset)

        # Read other datasets for consolidation
        print("\n" + "=" * 80)
        print("Reading datasets for consolidation...")

        datasets_to_read = []

        # NOTE
        if NOTE_PATH.exists():
            df_note = pl.read_parquet(NOTE_PATH)
            datasets_to_read.append(('NOTE', df_note))
            print(f"NOTE: {len(df_note)} records")

        # CALC
        if CALC_PATH.exists():
            df_calc = pl.read_parquet(CALC_PATH)
            datasets_to_read.append(('CALC', df_calc))
            print(f"CALC: {len(df_calc)} records")

        # PBIF
        if PBIF_PATH.exists():
            df_pbif = pl.read_parquet(PBIF_PATH)
            datasets_to_read.append(('PBIF', df_pbif))
            print(f"PBIF: {len(df_pbif)} records")

        # BT
        if BT_PATH.exists():
            df_bt = pl.read_parquet(BT_PATH)
            datasets_to_read.append(('BT', df_bt))
            print(f"BT: {len(df_bt)} records")

        # GLSET
        datasets_to_read.append(('GLSET', df_glset))
        print(f"GLSET: {len(df_glset)} records")

        # Consolidate all datasets
        print("\n" + "=" * 80)
        print("Consolidating datasets...")

        all_dfs = [df for name, df in datasets_to_read]
        df_final = pl.concat(all_dfs)

        print(f"FINAL total records: {len(df_final)}")

        # Sort by BNMCODE
        df_final = df_final.sort('BNMCODE')

        # Write FINAL
        df_final.write_parquet(FINAL_OUTPUT_PATH)
        print(f"FINAL written to {FINAL_OUTPUT_PATH}")

        # Aggregate by BNMCODE
        df_finalsum = df_final.group_by('BNMCODE').agg([
            pl.col('AMOUNT').sum()
        ]).sort('BNMCODE')

        # Write FINALSUM
        df_finalsum.write_parquet(FINALSUM_OUTPUT_PATH)
        print(f"FINALSUM written to {FINALSUM_OUTPUT_PATH}")
        print(f"FINALSUM unique BNMCODE: {len(df_finalsum)}")

        # Generate final report
        generate_final_report(df_finalsum)

        # Summary statistics
        print("\n" + "=" * 80)
        print("SUMMARY STATISTICS")
        print("=" * 80)

        for name, df in datasets_to_read:
            total = df['AMOUNT'].sum()
            print(f"{name:<15} Records: {len(df):>8,}  Total: {total:>20,.2f}")

        print("-" * 80)
        final_total = df_finalsum['AMOUNT'].sum()
        print(f"{'FINALSUM':<15} Records: {len(df_finalsum):>8,}  Total: {final_total:>20,.2f}")
        print("=" * 80)

        return 0

    except Exception as e:
        print(f"\nERROR: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
