#!/usr/bin/env python3
"""
Program  : FALWPBBD
To filter out invalid customer code & product code
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

# Define paths
# BASE_PATH = Path("/data")
BASE_PATH = Path(__file__).resolve().parent

BNM_PATH = BASE_PATH / "bnm"
FD_PATH = BASE_PATH / "fd"
DEPOSIT_PATH = BASE_PATH / "deposit"

# Input files
FD_FILE = FD_PATH / "fd.parquet"
UMA_FILE = DEPOSIT_PATH / "uma.parquet"

# Output files
FDWKLY_OUTPUT = BNM_PATH / f"fdwkly{REPTMON}{NOWK}.parquet"
UMA_OUTPUT = BNM_PATH / "uma.parquet"

# Ensure output directories exist
BNM_PATH.mkdir(parents=True, exist_ok=True)


# ============================================================================
# FORMAT MAPPING FUNCTIONS
# ============================================================================

def format_fdprod(intplan):
    """
    Map INTPLAN to BIC product code (FDPROD format)
    """
    if intplan is None:
        return None

    # RM Fixed Deposit mappings
    fdprod_map = {
        # RM Fixed Deposits
        1: '42130', 2: '42130', 3: '42130', 4: '42130', 5: '42130',
        6: '42130', 7: '42130', 8: '42130', 9: '42130', 10: '42130',
        11: '42130', 12: '42130', 13: '42130', 14: '42130', 15: '42130',
        16: '42130', 17: '42130', 18: '42130', 19: '42130', 20: '42130',
        21: '42130', 22: '42130', 23: '42130', 24: '42130', 25: '42130',
        26: '42130', 27: '42130', 28: '42130', 29: '42130', 30: '42130',

        # FX Fixed Deposits
        272: '42630', 273: '42630', 274: '42630', 275: '42630',
        276: '42630', 277: '42630', 278: '42630', 279: '42630',
        280: '42630', 281: '42630', 282: '42630', 283: '42630',
        284: '42630', 285: '42630', 286: '42630', 287: '42630',
        288: '42630', 289: '42630', 290: '42630', 291: '42630',
        292: '42630', 293: '42630', 294: '42630', 295: '42630',
    }

    return fdprod_map.get(intplan, '42130')


def format_fddenom(intplan):
    """
    Map INTPLAN to currency denomination (FDDENOM format)
    D = Domestic (RM), I = International (FX)
    """
    if intplan is None:
        return 'D'

    # FX intplan codes (272-295) return 'I', all others return 'D'
    if 272 <= intplan <= 295:
        return 'I'
    else:
        return 'D'


def format_statecd(branch):
    """
    Map BRANCH to STATE code (STATECD format)
    """
    if branch is None:
        return None

    # State code mapping based on branch ranges
    # This is a simplified version - actual mapping may be more complex
    branch_str = str(branch).zfill(4)

    # Mapping based on first 2 digits of branch code
    state_map = {
        '01': 'A', '02': 'B', '03': 'C', '04': 'D', '05': 'E',
        '06': 'F', '07': 'G', '08': 'H', '09': 'I', '10': 'J',
        '11': 'K', '12': 'L', '13': 'M', '14': 'N', '15': 'O',
        '16': 'P', '20': 'W', '21': 'X', '22': 'Y', '23': 'Z',
    }

    prefix = branch_str[:2]
    return state_map.get(prefix, 'A')


def format_fdcustcd(custcd):
    """
    Map CUSTCD to standardized customer code for FD (FDCUSTCD format)
    """
    if custcd is None:
        return None

    custcd_str = str(custcd).strip()

    # Direct mapping for known customer codes
    fdcustcd_map = {
        '1': '01', '2': '02', '3': '03', '4': '04', '5': '05',
        '6': '06', '7': '07', '10': '10', '11': '11', '12': '12',
        '13': '13', '17': '17', '20': '20', '30': '30', '31': '31',
        '32': '32', '33': '33', '34': '34', '35': '35', '36': '36',
        '37': '37', '38': '38', '39': '39', '40': '40', '41': '41',
        '42': '42', '43': '43', '44': '44', '45': '45', '46': '46',
        '47': '47', '48': '48', '49': '49', '51': '51', '52': '52',
        '53': '53', '54': '54', '57': '57', '59': '59', '60': '60',
        '61': '61', '62': '62', '63': '63', '64': '64', '65': '65',
        '66': '66', '67': '67', '68': '68', '69': '69', '70': '70',
        '71': '71', '72': '72', '73': '73', '74': '74', '75': '75',
        '76': '76', '77': '77', '78': '78', '79': '79', '80': '80',
        '81': '81', '82': '82', '83': '83', '84': '84', '85': '85',
        '86': '86', '87': '87', '88': '88', '89': '89', '90': '90',
        '91': '91', '92': '92', '95': '95', '96': '96', '98': '98',
        '99': '99',
    }

    # Try direct mapping first
    if custcd_str in fdcustcd_map:
        return fdcustcd_map[custcd_str]

    # Try with leading zeros removed
    custcd_int = str(int(custcd_str)) if custcd_str.isdigit() else custcd_str
    if custcd_int in fdcustcd_map:
        return fdcustcd_map[custcd_int]

    # Return padded to 2 digits if numeric
    if custcd_str.isdigit():
        return custcd_str.zfill(2)

    return custcd_str


def format_ifdcuscd(custcd):
    """
    Map CUSTCD to standardized customer code for IFD (IFDCUSCD format)
    International FD customer codes - similar to FDCUSTCD but may have different rules
    """
    # For this implementation, using same logic as FDCUSTCD
    # Actual format may differ based on business rules
    return format_fdcustcd(custcd)


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    """Main processing function"""

    conn = duckdb.connect()

    print("Starting FALWPBBD processing...")

    # ========================================================================
    # SECTION 1: Process FD.FD file to create BNM.FDWKLY
    # ========================================================================

    print("\nProcessing FD.FD file...")

    # Load FD data
    fd_data = pl.read_parquet(FD_FILE)
    print(f"Loaded {len(fd_data)} records from FD file")

    # Apply transformations
    fd_processed = fd_data.with_columns([
        # Apply BIC format
        pl.col('intplan').map_elements(
            format_fdprod,
            return_dtype=pl.Utf8
        ).alias('bic'),

        # Apply AMTIND format
        pl.col('intplan').map_elements(
            format_fddenom,
            return_dtype=pl.Utf8
        ).alias('amtind'),

        # Apply STATE format
        pl.col('branch').map_elements(
            format_statecd,
            return_dtype=pl.Utf8
        ).alias('state'),
    ])

    # Apply conditional customer code formatting
    def apply_custcode(row):
        """Apply customer code formatting based on BIC"""
        bic = row['bic']
        custcd = row['custcd']

        if bic in ['42130', '42630']:
            return format_fdcustcd(custcd)
        else:
            return format_ifdcuscd(custcd)

    # Create custcode column
    fd_with_custcode = []
    for row in fd_processed.iter_rows(named=True):
        custcode = apply_custcode(row)
        fd_with_custcode.append(custcode)

    fd_processed = fd_processed.with_columns(
        pl.Series('custcode', fd_with_custcode, dtype=pl.Utf8)
    )

    # Handle PURPOSE field for BIC='42630'
    def adjust_purpose(row):
        """Adjust PURPOSE field based on BIC and CUSTCODE"""
        bic = row['bic']
        custcode = row['custcode']
        purpose = row.get('purpose')

        if bic == '42630':
            if custcode in ['77', '78', '95']:
                if purpose not in ['1', '2', '3']:
                    return '1'
            else:
                if purpose not in ['4', '5']:
                    return '4'

        return purpose

    # Apply purpose adjustments if PURPOSE column exists
    if 'purpose' in fd_processed.columns:
        purpose_adjusted = []
        for row in fd_processed.iter_rows(named=True):
            purpose_adjusted.append(adjust_purpose(row))

        fd_processed = fd_processed.with_columns(
            pl.Series('purpose', purpose_adjusted, dtype=pl.Utf8)
        )

    # Override BIC based on ACCTTYPE
    fd_processed = fd_processed.with_columns(
        pl.when(pl.col('accttype').is_in([315, 394]))
        .then(pl.lit('42132'))
        .when(pl.col('accttype').is_in([397, 398]))
        .then(pl.lit('42199'))
        .otherwise(pl.col('bic'))
        .alias('bic')
    )

    # Filter for valid OPENIND values
    fd_processed = fd_processed.filter(
        pl.col('openind').is_in(['O', 'D'])
    )

    # Select required columns for output
    fdwkly = fd_processed.select([
        'branch', 'acctno', 'custcode', 'name', 'amtind', 'accttype',
        'openind', 'curbal', 'bic', 'intpay', 'state', 'term',
        'intplan', 'matdate'
    ])

    # Save FDWKLY output
    fdwkly.write_parquet(FDWKLY_OUTPUT)
    print(f"Saved {len(fdwkly)} records to {FDWKLY_OUTPUT}")

    # ========================================================================
    # SECTION 2: Process DEPOSIT.UMA file to create BNM.UMA
    # ========================================================================

    print("\nProcessing DEPOSIT.UMA file...")

    # Load UMA data
    uma_data = pl.read_parquet(UMA_FILE)
    print(f"Loaded {len(uma_data)} records from UMA file")

    # Rename custcode to custcd if needed
    if 'custcode' in uma_data.columns:
        uma_data = uma_data.rename({'custcode': 'custcd'})

    # Apply transformations
    def process_uma_row(row):
        """Process UMA row to determine custcode and amtind"""
        product = row['product']
        custcd = row['custcd']

        if product == 297:
            custcode = format_fdcustcd(custcd)
            amtind = 'D'
        else:
            custcode = format_ifdcuscd(custcd)
            amtind = 'I'

        return custcode, amtind

    # Process each row
    custcodes = []
    amtinds = []

    for row in uma_data.iter_rows(named=True):
        custcode, amtind = process_uma_row(row)
        custcodes.append(custcode)
        amtinds.append(amtind)

    # Add processed columns
    uma_processed = uma_data.with_columns([
        pl.Series('custcode', custcodes, dtype=pl.Utf8),
        pl.Series('amtind', amtinds, dtype=pl.Utf8),

        # Apply STATE format
        pl.col('branch').map_elements(
            format_statecd,
            return_dtype=pl.Utf8
        ).alias('state'),

        # Set BIC to constant value
        pl.lit('42199').alias('bic')
    ])

    # Filter for valid OPENIND values
    uma_processed = uma_processed.filter(
        pl.col('openind').is_in(['O', 'D'])
    )

    # Select required columns for output
    uma_output = uma_processed.select([
        'branch', 'acctno', 'custcode', 'name', 'amtind',
        'openind', 'curbal', 'bic', 'state'
    ])

    # Save UMA output
    uma_output.write_parquet(UMA_OUTPUT)
    print(f"Saved {len(uma_output)} records to {UMA_OUTPUT}")

    # ========================================================================
    # Summary
    # ========================================================================

    print("\n" + "=" * 70)
    print("Processing Summary:")
    print("=" * 70)
    print(f"FDWKLY records created: {len(fdwkly):>10,}")
    print(f"UMA records created:    {len(uma_output):>10,}")
    print("=" * 70)

    # Display sample statistics
    if len(fdwkly) > 0:
        print("\nFDWKLY BIC Distribution:")
        bic_counts = fdwkly.group_by('bic').agg(pl.count()).sort('bic')
        for row in bic_counts.iter_rows(named=True):
            print(f"  {row['bic']}: {row['count']:>10,}")

    if len(uma_output) > 0:
        print("\nUMA AMTIND Distribution:")
        amtind_counts = uma_output.group_by('amtind').agg(pl.count()).sort('amtind')
        for row in amtind_counts.iter_rows(named=True):
            print(f"  {row['amtind']}: {row['count']:>10,}")

    conn.close()
    print("\nProcessing complete!")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()
