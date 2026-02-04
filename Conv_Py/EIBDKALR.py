"""
File Name: EIBDKALR
BNMK Table Processor
Processes BNMTBL1 and BNMTBL3 files and creates K1TBL and K3TBL parquet outputs
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
INPUT_DIR = Path("/path/to/input")
OUTPUT_DIR = Path("/path/to/output")

BNMTBL1_PATH = INPUT_DIR / "KAPITI1.txt"
BNMTBL3_PATH = INPUT_DIR / "KAPMNI.txt"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# DATE CALCULATIONS (equivalent to REPTDATE DATA step)
# ============================================================================
def calculate_report_period():
    """
    Calculate report date (today - 1), week number, and month.
    Returns: (reptdate, nowk, reptmon)
    """
    reptdate = datetime.now().date() - timedelta(days=1)
    day = reptdate.day

    # Determine week of month
    if 1 <= day <= 8:
        nowk = '1'
    elif 9 <= day <= 15:
        nowk = '2'
    elif 16 <= day <= 22:
        nowk = '3'
    else:
        nowk = '4'

    reptmon = f"{reptdate.month:02d}"

    return reptdate, nowk, reptmon


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def parse_yymmdd8_numeric(value):
    """
    Convert 8-digit numeric date (YYYYMMDD) to date.
    Returns None if value is 0 or invalid.
    """
    if value == 0 or value is None:
        return None
    try:
        date_str = f"{int(value):08d}"
        return datetime.strptime(date_str, "%Y%m%d").date()
    except (ValueError, TypeError):
        return None


def parse_yymmdd8_string(value):
    """
    Parse YYYYMMDD string format to date.
    """
    if not value or value.strip() == '':
        return None
    try:
        return datetime.strptime(value.strip(), "%Y%m%d").date()
    except ValueError:
        return None


def parse_ddmmyy10_string(value):
    """
    Parse DD/MM/YYYY string format to date.
    """
    if not value or value.strip() == '':
        return None
    try:
        return datetime.strptime(value.strip(), "%d/%m/%Y").date()
    except ValueError:
        return None


# ============================================================================
# PROCESS K1TBL (BNMTBL1)
# ============================================================================
def process_k1tbl(reptdate, reptmon, nowk):
    """
    Process BNMTBL1 file to create K1TBL parquet file.
    """
    print(f"Processing K1TBL{reptmon}{nowk}...")

    # Define column specifications
    column_names = [
        'GWAB', 'GWAN', 'GWAS', 'GWAPP', 'GWACS',
        'GWBALA', 'GWBALC', 'GWPAIA', 'GWPAIC', 'GWSHN',
        'GWCTP', 'GWACT', 'GWACD', 'GWSAC', 'GWNANC',
        'GWCNAL', 'GWCCY', 'GWCNAR', 'GWCNAP', 'GWDIAA',
        'GWDIAC', 'GWCIAA', 'GWCIAC', 'GWRATD', 'GWRATC',
        'GWDIPA', 'GWDIPC', 'GWCIPA', 'GWCIPC', 'GWPL1D',
        'GWPL2D', 'GWPL1C', 'GWPL2C', 'GWPALA', 'GWPALC',
        'GWDLP', 'GWDLR', 'GWSDT', 'GWRDT', 'GWRRT',
        'GWPDT', 'GWPRT', 'GWPCM', 'GWMOTC', 'GWMRTC',
        'GWMRT', 'GWMDT', 'GWMCM', 'GWMWM', 'GWMVT',
        'GWMVTS', 'GWSRC', 'GWUC1', 'GWUC2', 'GWC2R',
        'GWAMAP', 'GWEXR', 'GWOPT', 'GWOCY', 'GWCBD'
    ]

    # Read first line to get REPTDATE
    with open(BNMTBL1_PATH, 'r') as f:
        first_line = f.readline().strip()
        if first_line:
            reptdate_from_file = parse_yymmdd8_string(first_line)
            if reptdate_from_file:
                reptdate = reptdate_from_file

    # Read data starting from line 2 using DuckDB
    query = f"""
    SELECT * FROM read_csv(
        '{BNMTBL1_PATH}',
        delim='|',
        header=false,
        skip=1,
        nullstr='',
        columns={{
            'GWAB': 'VARCHAR',
            'GWAN': 'VARCHAR',
            'GWAS': 'VARCHAR',
            'GWAPP': 'VARCHAR',
            'GWACS': 'VARCHAR',
            'GWBALA': 'DOUBLE',
            'GWBALC': 'DOUBLE',
            'GWPAIA': 'DOUBLE',
            'GWPAIC': 'DOUBLE',
            'GWSHN': 'VARCHAR',
            'GWCTP': 'VARCHAR',
            'GWACT': 'VARCHAR',
            'GWACD': 'VARCHAR',
            'GWSAC': 'VARCHAR',
            'GWNANC': 'VARCHAR',
            'GWCNAL': 'VARCHAR',
            'GWCCY': 'VARCHAR',
            'GWCNAR': 'VARCHAR',
            'GWCNAP': 'VARCHAR',
            'GWDIAA': 'DOUBLE',
            'GWDIAC': 'DOUBLE',
            'GWCIAA': 'DOUBLE',
            'GWCIAC': 'DOUBLE',
            'GWRATD': 'DOUBLE',
            'GWRATC': 'DOUBLE',
            'GWDIPA': 'DOUBLE',
            'GWDIPC': 'DOUBLE',
            'GWCIPA': 'DOUBLE',
            'GWCIPC': 'DOUBLE',
            'GWPL1D': 'VARCHAR',
            'GWPL2D': 'VARCHAR',
            'GWPL1C': 'VARCHAR',
            'GWPL2C': 'VARCHAR',
            'GWPALA': 'DOUBLE',
            'GWPALC': 'DOUBLE',
            'GWDLP': 'VARCHAR',
            'GWDLR': 'VARCHAR',
            'GWSDT': 'BIGINT',
            'GWRDT': 'BIGINT',
            'GWRRT': 'BIGINT',
            'GWPDT': 'BIGINT',
            'GWPRT': 'BIGINT',
            'GWPCM': 'BIGINT',
            'GWMOTC': 'VARCHAR',
            'GWMRTC': 'VARCHAR',
            'GWMRT': 'BIGINT',
            'GWMDT': 'BIGINT',
            'GWMCM': 'BIGINT',
            'GWMWM': 'BIGINT',
            'GWMVT': 'VARCHAR',
            'GWMVTS': 'VARCHAR',
            'GWSRC': 'VARCHAR',
            'GWUC1': 'VARCHAR',
            'GWUC2': 'VARCHAR',
            'GWC2R': 'VARCHAR',
            'GWAMAP': 'DOUBLE',
            'GWEXR': 'DOUBLE',
            'GWOPT': 'VARCHAR',
            'GWOCY': 'VARCHAR',
            'GWCBD': 'VARCHAR'
        }}
    )
    """

    df = duckdb.query(query).pl()

    # Add REPTDATE column at the beginning
    df = df.with_columns(pl.lit(reptdate).alias('REPTDATE'))

    # Convert date fields: GWSDT and GWMDT
    df = df.with_columns([
        pl.when(pl.col('GWSDT').is_not_null() & (pl.col('GWSDT') != 0))
        .then(
            pl.col('GWSDT').cast(pl.Utf8).str.zfill(8).str.to_date("%Y%m%d", strict=False)
        )
        .otherwise(None)
        .alias('GWSDT'),

        pl.when(pl.col('GWMDT').is_not_null() & (pl.col('GWMDT') != 0))
        .then(
            pl.col('GWMDT').cast(pl.Utf8).str.zfill(8).str.to_date("%Y%m%d", strict=False)
        )
        .otherwise(None)
        .alias('GWMDT')
    ])

    # Reorder columns with REPTDATE first
    cols = ['REPTDATE'] + [col for col in df.columns if col != 'REPTDATE']
    df = df.select(cols)

    # Write to parquet
    output_path = OUTPUT_DIR / f"K1TBL{reptmon}{nowk}.parquet"
    df.write_parquet(output_path)
    print(f"Written {len(df)} rows to {output_path}")

    return df


# ============================================================================
# PROCESS K3TBL (BNMTBL3)
# ============================================================================
def process_k3tbl(reptdate, reptmon, nowk):
    """
    Process BNMTBL3 file to create K3TBL parquet file.
    """
    print(f"Processing K3TBL{reptmon}{nowk}...")

    # Read first line to get REPTDATE
    with open(BNMTBL3_PATH, 'r') as f:
        first_line = f.readline().strip()
        if first_line:
            reptdate_from_file = parse_yymmdd8_string(first_line)
            if reptdate_from_file:
                reptdate = reptdate_from_file

    # Read data starting from line 2 using DuckDB
    query = f"""
    SELECT * FROM read_csv(
        '{BNMTBL3_PATH}',
        delim='|',
        header=false,
        skip=1,
        nullstr='',
        columns={{
            'UTSTY': 'VARCHAR',
            'UTREF': 'VARCHAR',
            'UTBRNM': 'VARCHAR',
            'UTDLP': 'VARCHAR',
            'UTDLR': 'VARCHAR',
            'UTSMN': 'VARCHAR',
            'UTCUS': 'VARCHAR',
            'UTCLC': 'VARCHAR',
            'UTCTP': 'VARCHAR',
            'UTFCV': 'DOUBLE',
            'UTIDT': 'VARCHAR',
            'UTLCD': 'VARCHAR',
            'UTNCD': 'VARCHAR',
            'UTMDT': 'VARCHAR',
            'UTCBD': 'VARCHAR',
            'UTCPR': 'DOUBLE',
            'UTQDS': 'DOUBLE',
            'UTPCP': 'DOUBLE',
            'UTAMOC': 'DOUBLE',
            'UTDPF': 'DOUBLE',
            'UTAICT': 'DOUBLE',
            'UTAICY': 'DOUBLE',
            'UTAIT': 'DOUBLE',
            'UTDPET': 'DOUBLE',
            'UTDPEY': 'DOUBLE',
            'UTDPE': 'DOUBLE',
            'UTASN': 'BIGINT',
            'UTOSD': 'VARCHAR',
            'UTCA2': 'VARCHAR',
            'UTSAC': 'VARCHAR',
            'UTCNAP': 'VARCHAR',
            'UTCNAR': 'VARCHAR',
            'UTCNAL': 'VARCHAR',
            'UTCCY': 'VARCHAR',
            'UTAMTS': 'DOUBLE',
            'UTMM1': 'VARCHAR'
        }}
    )
    """

    df = duckdb.query(query).pl()

    # Filter: DELETE IF SUBSTR(UTDLP,2,2) IN ('RT','RI')
    df = df.filter(
        ~pl.col('UTDLP').str.slice(1, 2).is_in(['RT', 'RI'])
    )

    # Parse date fields from DD/MM/YYYY format
    df = df.with_columns([
        pl.when(pl.col('UTMDT').is_not_null() & (pl.col('UTMDT') != ''))
        .then(pl.col('UTMDT').str.to_date("%d/%m/%Y", strict=False))
        .otherwise(None)
        .alias('MATDT'),

        pl.when(pl.col('UTOSD').is_not_null() & (pl.col('UTOSD') != ''))
        .then(pl.col('UTOSD').str.to_date("%d/%m/%Y", strict=False))
        .otherwise(None)
        .alias('ISSDT'),

        pl.when(pl.col('UTCBD').is_not_null() & (pl.col('UTCBD') != ''))
        .then(pl.col('UTCBD').str.to_date("%d/%m/%Y", strict=False))
        .otherwise(None)
        .alias('REPTDATE_PARSED'),

        pl.when(pl.col('UTCBD').is_not_null() & (pl.col('UTCBD') != ''))
        .then(pl.col('UTCBD').str.to_date("%d/%m/%Y", strict=False))
        .otherwise(None)
        .alias('DDATE'),

        pl.when(pl.col('UTIDT').is_not_null() & (pl.col('UTIDT') != ''))
        .then(pl.col('UTIDT').str.to_date("%d/%m/%Y", strict=False))
        .otherwise(None)
        .alias('XDATE')
    ])

    # Use REPTDATE_PARSED if available, otherwise use input reptdate
    df = df.with_columns(
        pl.when(pl.col('REPTDATE_PARSED').is_not_null())
        .then(pl.col('REPTDATE_PARSED'))
        .otherwise(pl.lit(reptdate))
        .alias('REPTDATE')
    )

    # IF UTSTY = 'IZD' THEN UTCPR = UTQDS
    df = df.with_columns(
        pl.when(pl.col('UTSTY') == 'IZD')
        .then(pl.col('UTQDS'))
        .otherwise(pl.col('UTCPR'))
        .alias('UTCPR')
    )

    # Filter: IF UTSTY IN ('IFD','ILD','ISD','IZD') THEN IF XDATE > DDATE THEN DELETE
    df = df.filter(
        ~(
                pl.col('UTSTY').is_in(['IFD', 'ILD', 'ISD', 'IZD']) &
                (pl.col('XDATE') > pl.col('DDATE'))
        )
    )

    # Select and order final columns
    final_columns = [
        'REPTDATE', 'UTSTY', 'UTREF', 'UTBRNM', 'UTDLP', 'UTDLR', 'UTSMN',
        'UTCUS', 'UTCLC', 'UTCTP', 'UTFCV', 'UTIDT', 'UTLCD', 'UTNCD',
        'UTMDT', 'UTCBD', 'UTCPR', 'UTQDS', 'UTPCP', 'UTAMOC', 'UTDPF',
        'UTAICT', 'UTAICY', 'UTAIT', 'UTDPET', 'UTDPEY', 'UTDPE', 'UTASN',
        'UTOSD', 'UTCA2', 'UTSAC', 'UTCNAP', 'UTCNAR', 'UTCNAL', 'UTCCY',
        'UTAMTS', 'UTMM1', 'MATDT', 'ISSDT', 'DDATE', 'XDATE'
    ]

    df = df.select(final_columns)

    # Write to parquet
    output_path = OUTPUT_DIR / f"K3TBL{reptmon}{nowk}.parquet"
    df.write_parquet(output_path)
    print(f"Written {len(df)} rows to {output_path}")

    return df


# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    """
    Main execution function.
    """
    try:
        print("=" * 80)
        print("BNMK Table Processor - SAS to Python Conversion")
        print("=" * 80)

        # Calculate report period
        reptdate, nowk, reptmon = calculate_report_period()
        print(f"\nReport Date: {reptdate}")
        print(f"Month: {reptmon}, Week: {nowk}")
        print(f"Output suffix: {reptmon}{nowk}\n")

        # Process K1TBL
        print("-" * 80)
        k1tbl = process_k1tbl(reptdate, reptmon, nowk)
        print(f"K1TBL processing complete: {len(k1tbl)} records")

        # Process K3TBL
        print("-" * 80)
        k3tbl = process_k3tbl(reptdate, reptmon, nowk)
        print(f"K3TBL processing complete: {len(k3tbl)} records")

        print("-" * 80)
        print("\nProcessing completed successfully!")
        print("=" * 80)

        return 0

    except Exception as e:
        print(f"\nERROR: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
