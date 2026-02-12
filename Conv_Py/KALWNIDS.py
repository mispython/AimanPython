#!/usr/bin/env python3
"""
Program: KALWNIDS
Function: Process K3TBL data to calculate prorated interest amounts
"""

import duckdb
import polars as pl
from pathlib import Path

# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Define macro variables (these should be set according to your environment)
REPTMON = "202401"  # Report month
NOWK = "01"  # Week number

# Define paths
BASE_PATH = Path("/data")
BNMK_PATH = BASE_PATH / "bnmk"

# Input file
K3TBL_FILE = BNMK_PATH / f"k3tbl{REPTMON}{NOWK}.parquet"


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_k3tabi():
    """
    Process K3TBL data to calculate prorated interest amounts
    Returns: Polars DataFrame with BNMCODE, AMOUNT, and AMTIND columns
    """

    print("Processing K3TABI data...")

    # Read K3TBL data
    k3tb1 = pl.read_parquet(K3TBL_FILE)
    print(f"Loaded {len(k3tb1)} records from K3TBL")

    # ========================================================================
    # STEP 1: Calculate total UTAMOC by UTSMN
    # ========================================================================

    k3x = k3tb1.group_by('utsmn').agg([
        pl.col('utamoc').sum().alias('ttamoc')
    ])

    print(f"Calculated totals for {len(k3x)} UTSMN groups")

    # ========================================================================
    # STEP 2: Merge totals back and calculate prorated amounts
    # ========================================================================

    # Merge K3X with K3TB1
    k3tabi = k3tb1.join(k3x, on='utsmn', how='left')

    # Calculate UTDPX (prorated UTDPF)
    # UTDPX = (UTAMOC / TTAMOC) * UTDPF
    k3tabi = k3tabi.with_columns([
        ((pl.col('utamoc') / pl.col('ttamoc')) * pl.col('utdpf')).alias('utdpx')
    ])

    # Take absolute value if UTDPX < 0
    k3tabi = k3tabi.with_columns([
        pl.when(pl.col('utdpx') < 0)
        .then(pl.col('utdpx') * -1)
        .otherwise(pl.col('utdpx'))
        .alias('utdpx')
    ])

    # Calculate AMOUNT
    k3tabi = k3tabi.with_columns([
        (pl.col('utamoc') - pl.col('utdpx')).alias('amount')
    ])

    # Filter for UTSMN starting with 'I'
    k3tabi = k3tabi.filter(
        pl.col('utsmn').str.starts_with('I')
    )

    # Add AMTIND
    k3tabi = k3tabi.with_columns([
        pl.lit('I').alias('amtind')
    ])

    print(f"Filtered to {len(k3tabi)} records with UTSMN starting with 'I'")

    # ========================================================================
    # STEP 3: Assign BNMCODE based on UTCTP
    # ========================================================================

    # Define valid UTCTP values
    valid_utctp = [
        'BB', 'BQ', 'BM', 'BN', 'BG', 'BR', 'BF', 'BH', 'BZ',
        'BU', 'AD', 'BT', 'BV', 'BS', 'AC', 'DD', 'CG', 'CA',
        'CB', 'CC', 'CD', 'CF', 'BW', 'BA', 'BE', 'DA',
        'DB', 'DC', 'EC', 'EA', 'FA', 'EB', 'CE', 'GA', 'BJ'
    ]

    # Filter for valid UTCTP
    k3tabi = k3tabi.filter(
        pl.col('utctp').is_in(valid_utctp)
    )

    print(f"Filtered to {len(k3tabi)} records with valid UTCTP")

    # Assign BNMCODE based on UTCTP using conditional logic
    k3tabi = k3tabi.with_columns([
        pl.when(pl.col('utctp') == 'BB')
        .then(pl.lit('4215002000000Y'))
        .when(pl.col('utctp') == 'BJ')
        .then(pl.lit('4215007000000Y'))
        .when(pl.col('utctp') == 'BQ')
        .then(pl.lit('4215011000000Y'))
        .when(pl.col('utctp') == 'BM')
        .then(pl.lit('4215012000000Y'))
        .when(pl.col('utctp') == 'BN')
        .then(pl.lit('4215013000000Y'))
        .when(pl.col('utctp') == 'BG')
        .then(pl.lit('4215017000000Y'))
        .when(pl.col('utctp').is_in(['BR', 'BF', 'BH', 'BZ', 'BU', 'AD', 'BT', 'BV', 'BS']))
        .then(pl.lit('4215020000000Y'))
        .when(pl.col('utctp').is_in(['AC', 'DD', 'CG', 'CA', 'CB', 'CC', 'CD', 'CF']))
        .then(pl.lit('4215060000000Y'))
        .when(pl.col('utctp') == 'DA')
        .then(pl.lit('4215071000000Y'))
        .when(pl.col('utctp') == 'DB')
        .then(pl.lit('4215072000000Y'))
        .when(pl.col('utctp') == 'DC')
        .then(pl.lit('4215074000000Y'))
        .when(pl.col('utctp').is_in(['EC', 'EA']))
        .then(pl.lit('4215076000000Y'))
        .when(pl.col('utctp') == 'FA')
        .then(pl.lit('4215079000000Y'))
        .when(pl.col('utctp').is_in(['BW', 'BA', 'BE']))
        .then(pl.lit('4215081000000Y'))
        .when(pl.col('utctp').is_in(['EB', 'CE', 'GA']))
        .then(pl.lit('4215085000000Y'))
        .otherwise(pl.lit(' '))
        .alias('bnmcode')
    ])

    # Filter out records with blank BNMCODE
    k3tabi = k3tabi.filter(
        pl.col('bnmcode') != ' '
    )

    print(f"Final K3TABI records: {len(k3tabi)}")

    # ========================================================================
    # STEP 4: Select required columns
    # ========================================================================

    k3tabi_final = k3tabi.select(['bnmcode', 'amount', 'amtind'])

    # Display summary statistics
    if len(k3tabi_final) > 0:
        print("\nK3TABI Summary:")
        print(f"Total records: {len(k3tabi_final):,}")
        print(f"Total amount:  {k3tabi_final['amount'].sum():,.2f}")

        # BNMCODE distribution
        bnmcode_dist = k3tabi_final.group_by('bnmcode').agg([
            pl.count().alias('count'),
            pl.col('amount').sum().alias('total_amount')
        ]).sort('bnmcode')

        print("\nBNMCODE Distribution:")
        for row in bnmcode_dist.iter_rows(named=True):
            print(f"  {row['bnmcode']}: {row['count']:>6,} records, "
                  f"Amount: {row['total_amount']:>20,.2f}")
    else:
        print("\nNo K3TABI records generated")

    return k3tabi_final


# ============================================================================
# STANDALONE EXECUTION
# ============================================================================

def main():
    """Main function for standalone execution"""

    print("=" * 70)
    print("K3TABI Processing Module")
    print("=" * 70)

    # Process K3TABI
    k3tabi = process_k3tabi()

    print("\n" + "=" * 70)
    print("Processing Complete")
    print("=" * 70)

    return k3tabi


if __name__ == "__main__":
    k3tabi_result = main()
