"""
EIAWOF07.py
Processes loan accounts for bad debt write-off exercise
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

BASE_DIR = Path("./")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
TEMP_DIR = BASE_DIR / "temp"

# Create directories
for dir_path in [INPUT_DIR, OUTPUT_DIR, TEMP_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Libraries (SAS libnames)
LOAN_DIR = INPUT_DIR / "loan"
NPL_DIR = INPUT_DIR / "npl"
SASLN_DIR = INPUT_DIR / "sasln"

for dir_path in [LOAN_DIR, NPL_DIR, SASLN_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# =============================================================================
# DATE HANDLING AND MACRO VARIABLES
# =============================================================================

def sas_date_to_datetime(sas_date):
    """Convert SAS numeric date to datetime"""
    if sas_date is None or sas_date <= 0:
        return None
    return datetime(1960, 1, 1) + timedelta(days=int(sas_date))

def datetime_to_sas_date(dt):
    """Convert datetime to SAS numeric date"""
    if dt is None:
        return 0
    reference = datetime(1960, 1, 1)
    return (dt - reference).days

def parse_mmddyyyy(date_str):
    """Parse MMDDYYYY string to date"""
    if not date_str or len(date_str) != 8:
        return None
    try:
        month = int(date_str[0:2])
        day = int(date_str[2:4])
        year = int(date_str[4:8])
        return datetime(year, month, day).date()
    except:
        return None

def setup_date_variables():
    """Calculate report date and create macro variable equivalents"""
    reptdate_df = pl.read_parquet(LOAN_DIR / "REPTDATE.parquet")
    
    # Get REPTDATE from first row
    reptdate_val = reptdate_df.select(pl.first()).item()
    reptdate = sas_date_to_datetime(reptdate_val)
    
    day = reptdate.day
    
    # Determine WK and WK1 based on day
    if day == 8:
        wk = '1'
        wk1 = '4'
    elif day == 15:
        wk = '2'
        wk1 = '1'
    elif day == 22:
        wk = '3'
        wk1 = '2'
    else:
        wk = '4'
        wk1 = '3'
    
    mm = reptdate.month
    mm1 = mm - 1
    if mm1 == 0:
        mm1 = 12
    
    return {
        'REPTDATE': reptdate,
        'REPTDATE_SAS': reptdate_val,
        'WK': wk,
        'WK1': wk1,
        'NOWK': wk,
        'NOWKS': '4',
        'NOWK1': wk1,
        'REPTMON': str(mm).zfill(2),
        'REPTMON1': str(mm1).zfill(2),
        'RDATE': reptdate.strftime('%d%m%Y')
    }

# =============================================================================
# SECURITY TYPE LISTS
# =============================================================================

# HP Loan types (from macro &HPD)
HPD = ['HPLN', 'HPCS', 'HPSV', 'HPCS2', 'HPLN2', 'HPUS', 
       'AUTOL', 'AUTOL2', 'BOB', 'MBIKE', 'MBIKE2', 'HVYEQ']

# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================

def process_npl_data():
    """Process NPL data from IIS, SP2, WOFFHP files"""
    
    # Read input files with deduplication
    iis_df = pl.read_parquet(NPL_DIR / "IIS.parquet").unique(subset=['ACCTNO'])
    sp_df = pl.read_parquet(NPL_DIR / "SP2.parquet").unique(subset=['ACCTNO'])
    woff_df = pl.read_parquet(NPL_DIR / "WOFFHP.parquet")
    
    # Merge SP, IIS, and WOFF
    npl_df = sp_df.join(iis_df, on='ACCTNO', how='inner')
    npl_df = npl_df.join(woff_df, on='ACCTNO', how='inner')
    
    # Round numeric fields
    for col in ['IIS', 'OI', 'SP', 'MARKETVL']:
        if col in npl_df.columns:
            npl_df = npl_df.with_columns([
                pl.col(col).round(2).alias(col)
            ])
    
    # Create BRNO and BRABBR from BRANCH
    if 'BRANCH' in npl_df.columns:
        npl_df = npl_df.with_columns([
            pl.col('BRANCH').str.slice(3, 4).alias('BRNO'),
            pl.col('BRANCH').str.slice(0, 3).alias('BRABBR')
        ])
    
    # Keep required columns
    keep_cols = ['BRNO', 'BRABBR', 'NAME', 'ACCTNO', 'NOTENO', 'IIS', 'OI', 
                 'BORSTAT', 'SP', 'MARKETVL', 'DAYS', 'BRANCH']
    npl_df = npl_df.select([c for c in keep_cols if c in npl_df.columns])
    
    return npl_df

def process_loan_data(npl_df):
    """Process LOAN data from LNNOTE file"""
    
    loan_df = pl.read_parquet(LOAN_DIR / "LNNOTE.parquet")
    
    # Filter by loan type
    loan_df = loan_df.filter(pl.col('LOANTYPE').is_in(HPD))
    
    # Remove duplicates
    loan_df = loan_df.unique(subset=['ACCTNO', 'NOTENO'])
    
    # Merge with NPL
    merged_df = npl_df.join(loan_df, on=['ACCTNO', 'NOTENO'], how='inner')
    
    # Calculate derived fields
    merged_df = merged_df.with_columns([
        pl.col('FEETOTAL').fill_null(0).alias('FEETOTAL'),
        pl.col('NFEEAMT5').fill_null(0).alias('NFEEAMT5'),
        pl.col('FEETOT2').fill_null(0).alias('FEETOT2'),
        pl.col('FEEAMTA').fill_null(0).alias('FEEAMTA'),
        pl.col('FEEAMT5').fill_null(0).alias('FEEAMT5')
    ])
    
    merged_df = merged_df.with_columns([
        (pl.col('FEETOTAL') + pl.col('NFEEAMT5')).alias('POSTAMT'),
        (pl.col('FEEAMT3') - (pl.col('FEETOTAL') + pl.col('NFEEAMT5'))).alias('OTHERAMT'),
        (pl.col('FEETOT2') - pl.col('FEEAMTA') + pl.col('FEEAMT5')).alias('OIFEEAMT')
    ])
    
    # Handle ECSRRSRV
    if 'ECSRRSRV' in merged_df.columns:
        merged_df = merged_df.with_columns([
            pl.when(pl.col('ECSRRSRV') <= 0).then(0).otherwise(pl.col('ECSRRSRV')).alias('ECSRRSRV')
        ])
    
    # Process MATUREDT
    if 'MATUREDT' in merged_df.columns:
        merged_df = merged_df.with_columns([
            pl.when(pl.col('MATUREDT').is_not_null())
            .then(
                pl.col('MATUREDT').cast(pl.Utf8).str.zfill(11).str.slice(0, 8)
                .str.strptime(pl.Date, format='%m%d%Y')
            )
            .otherwise(None).alias('MATUREDT')
        ])
        
        merged_df = merged_df.with_columns([
            pl.col('MATUREDT').dt.strftime('%m/%d/%Y').alias('MATDATE')
        ])
    
    return merged_df

def process_sasln_data(loan_df, npl_df, macro_vars):
    """Process SASLN data for previous balance"""
    
    sasln_file = SASLN_DIR / f"LOAN{macro_vars['REPTMON1']}{macro_vars['NOWKS']}.parquet"
    
    if not sasln_file.exists():
        print(f"Warning: {sasln_file} not found")
        return loan_df
    
    sasln_df = pl.read_parquet(sasln_file).select(['ACCTNO', 'NOTENO', 'CURBAL'])
    sasln_df = sasln_df.unique(subset=['ACCTNO', 'NOTENO'])
    
    # Merge with NPL to get PREVBAL
    merged_df = sasln_df.join(npl_df.select(['ACCTNO', 'NOTENO']), on=['ACCTNO', 'NOTENO'], how='inner')
    merged_df = merged_df.rename({'CURBAL': 'PREVBAL'})
    
    # Merge back to loan data
    result_df = loan_df.join(merged_df, on=['ACCTNO', 'NOTENO'], how='left')
    
    return result_df

def process_woff(loan_df, npl_df):
    """Create final WOFF dataset"""
    
    woff_df = loan_df.with_columns([
        (pl.col('CURBAL') - pl.col('PREVBAL').fill_null(0)).alias('PAYMENT')
    ])
    
    # Merge with NPL to get IIS, OIFEEAMT, SP
    woff_df = woff_df.join(
        npl_df.select(['ACCTNO', 'NOTENO', 'IIS', 'OI', 'SP']), 
        on=['ACCTNO', 'NOTENO'], 
        how='left'
    )
    
    woff_df = woff_df.with_columns([
        (pl.col('IIS').fill_null(0) + pl.col('OIFEEAMT').fill_null(0)).round(2).alias('TOTIIS'),
        ((pl.col('IIS').fill_null(0) + pl.col('OIFEEAMT').fill_null(0) + 
          pl.col('SP').fill_null(0)).round(2)).alias('TOTAL')
    ])
    
    return woff_df

def process_guarantor_data(woff_df):
    """Process guarantor end date from LNNOTE"""
    
    lnnote_df = pl.read_parquet(LOAN_DIR / "LNNOTE.parquet")
    
    # Get latest NOTENO per ACCTNO
    lnnote_df = lnnote_df.sort(['ACCTNO', 'NOTENO'], descending=True)
    lnnote_df = lnnote_df.unique(subset=['ACCTNO']).select(['ACCTNO', 'GUAREND'])
    
    # Merge with WOFF
    result_df = woff_df.join(lnnote_df, on='ACCTNO', how='left')
    
    return result_df

def write_output_files(df):
    """Write WOFF data to text files"""
    
    if df is None or len(df) == 0:
        print("No data to write")
        return
    
    # Fixed-width output (WOFFACCT)
    with open(OUTPUT_DIR / "WOFFACCT.txt", 'w') as f:
        for row in df.rows():
            # Format each field according to positions
            line = (
                f"{str(row.get('BRANCH', '')):<7}"
                f"{str(row.get('NAME', '')):<24}"
                f"{str(row.get('ACCTNO', '')):<10}"
                f"{str(row.get('NOTENO', '')):<5}"
                f"{str(row.get('BORSTAT', '')):<1}"
                f"{float(row.get('IIS', 0)):>16.2f}"
                f"{float(row.get('OIFEEAMT', 0)):>16.2f}"
                f"{float(row.get('TOTIIS', 0)):>16.2f}"
                f"{float(row.get('SP', 0)):>16.2f}"
                f"{float(row.get('TOTAL', 0)):>16.2f}"
                f"{float(row.get('CURBAL', 0)):>16.2f}"
                f"{float(row.get('PREVBAL', 0)):>16.2f}"
                f"{float(row.get('PAYMENT', 0)):>16.2f}"
                f"{float(row.get('ECSRRSRV', 0)):>16.2f}"
                f"{float(row.get('POSTAMT', 0)):>16.2f}"
                f"{float(row.get('OTHERAMT', 0)):>16.2f}"
                f"{str(row.get('MATDATE', '')):<10}"
                f"{int(row.get('LOANTYPE', 0)):>3}"
                f"{float(row.get('INTAMT', 0)):>16.2f}"
                f"{str(row.get('POSTNTRN', '')):<1}"
                f"{float(row.get('MARKETVL', 0)):>16.2f}"
                f"{float(row.get('INTEARN4', 0)):>16.2f}"
                f"{int(row.get('DAYS', 0)):>6}"
                f"{int(row.get('CUSTCODE', 0)):>3}"
                f"{float(row.get('BALANCE', 0)):>16.2f}"
                f"{str(row.get('GUAREND', '')):<12}"
            )
            f.write(line + '\n')
    
    # CSV output (WOFFACC2) with headers
    with open(OUTPUT_DIR / "WOFFACC2.csv", 'w') as f:
        # Write header
        headers = ['BRNO', 'BRABBR', 'NAME', 'ACCTNO', 'NOTENO', 'BORSTAT',
                   'IIS', 'OIFEEAMT', 'TOTIIS', 'SP', 'TOTAL', 'CURBAL',
                   'PREVBAL', 'PAYMENT', 'ECSRRSRV', 'POSTAMT', 'OTHERAMT',
                   'MATDATE', 'LOANTYPE', 'INTAMT', 'POSTNTRN', 'MARKETVL',
                   'INTEARN4', 'DAYS', 'CUSTCODE', 'BALANCE', 'I/C NO./BR NO.']
        f.write(';'.join(headers) + '\n')
        
        # Write data rows
        for row in df.rows():
            values = [
                str(row.get('BRNO', '')),
                str(row.get('BRABBR', '')),
                str(row.get('NAME', '')),
                str(row.get('ACCTNO', '')),
                str(row.get('NOTENO', '')),
                str(row.get('BORSTAT', '')),
                f"{float(row.get('IIS', 0)):.2f}",
                f"{float(row.get('OIFEEAMT', 0)):.2f}",
                f"{float(row.get('TOTIIS', 0)):.2f}",
                f"{float(row.get('SP', 0)):.2f}",
                f"{float(row.get('TOTAL', 0)):.2f}",
                f"{float(row.get('CURBAL', 0)):.2f}",
                f"{float(row.get('PREVBAL', 0)):.2f}",
                f"{float(row.get('PAYMENT', 0)):.2f}",
                f"{float(row.get('ECSRRSRV', 0)):.2f}",
                f"{float(row.get('POSTAMT', 0)):.2f}",
                f"{float(row.get('OTHERAMT', 0)):.2f}",
                str(row.get('MATDATE', '')),
                str(row.get('LOANTYPE', '')),
                f"{float(row.get('INTAMT', 0)):.2f}",
                str(row.get('POSTNTRN', '')),
                f"{float(row.get('MARKETVL', 0)):.2f}",
                f"{float(row.get('INTEARN4', 0)):.2f}",
                str(row.get('DAYS', '')),
                str(row.get('CUSTCODE', '')),
                f"{float(row.get('BALANCE', 0)):.2f}",
                str(row.get('GUAREND', ''))
            ]
            f.write(';'.join(values) + '\n')
    
    print(f"Fixed-width output: {OUTPUT_DIR}/WOFFACCT.txt")
    print(f"CSV output: {OUTPUT_DIR}/WOFFACC2.csv")

def generate_listing(df, macro_vars):
    """Generate PROC PRINT equivalent listing"""
    
    print("\n" + "="*100)
    print("LISTING OF ACCOUNTS FOR BAD DEBT WRITING-OFF EXERCISE")
    print(f"AS AT {macro_vars['RDATE']}")
    print("="*100)
    
    # Select and order columns for display
    display_cols = ['BRANCH', 'NAME', 'ACCTNO', 'NOTENO', 'BORSTAT', 'IIS', 'OI', 
                    'TOTIIS', 'SP', 'TOTAL', 'CURBAL', 'PREVBAL', 'PAYMENT', 
                    'FEEAMT3', 'FEEAMT4', 'MATDATE', 'LOANTYPE', 'INTAMT', 
                    'POSTNTRN', 'MARKETVL', 'INTEARN4', 'DAYS', 'BALANCE']
    
    display_df = df.select([c for c in display_cols if c in df.columns])
    
    # Print with formatting
    with pl.Config(tbl_rows=100, tbl_cols=50, fmt_str_lengths=50):
        print(display_df)
    
    # Calculate and print sums
    sum_cols = ['IIS', 'OI', 'TOTIIS', 'SP', 'TOTAL', 'CURBAL', 
                'PREVBAL', 'PAYMENT', 'FEEAMT3', 'FEEAMT4']
    
    print("\n" + "-"*100)
    print("SUMMARY TOTALS:")
    for col in sum_cols:
        if col in df.columns:
            total = df.select(pl.col(col).sum()).item()
            print(f"  {col}: {total:>16.2f}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    print(f"Starting EIAWOF07 translation at {datetime.now()}")
    
    # Setup date variables
    macro_vars = setup_date_variables()
    print(f"Report Date: {macro_vars['RDATE']}, Week: {macro_vars['WK']}")
    print(f"REPTMON: {macro_vars['REPTMON']}, REPTMON1: {macro_vars['REPTMON1']}")
    
    try:
        print("\n" + "="*60)
        print("Processing NPL data...")
        print("="*60)
        
        # Step 1: Process NPL data
        npl_df = process_npl_data()
        if npl_df is not None:
            print(f"  NPL records: {len(npl_df)}")
        
        # Step 2: Process LOAN data
        print("\n" + "="*60)
        print("Processing LOAN data...")
        print("="*60)
        
        loan_df = process_loan_data(npl_df, macro_vars)
        if loan_df is not None:
            print(f"  LOAN records after merge: {len(loan_df)}")
        
        # Step 3: Process SASLN data
        print("\n" + "="*60)
        print("Processing SASLN data...")
        print("="*60)
        
        loan_df = process_sasln_data(loan_df, npl_df, macro_vars)
        if loan_df is not None:
            print(f"  Records after SASLN merge: {len(loan_df)}")
        
        # Step 4: Create WOFF dataset
        print("\n" + "="*60)
        print("Creating WOFF dataset...")
        print("="*60)
        
        woff_df = process_woff(loan_df, npl_df, macro_vars)
        
        # Step 5: Add guarantor data
        print("\n" + "="*60)
        print("Adding guarantor data...")
        print("="*60)
        
        woff_df = process_guarantor_data(woff_df)
        if woff_df is not None:
            print(f"  Final WOFF records: {len(woff_df)}")
        
        # Step 6: Write output files
        print("\n" + "="*60)
        print("Writing output files...")
        print("="*60)
        
        write_output_files(woff_df, macro_vars)
        
        # Step 7: Generate listing
        print("\n" + "="*60)
        print("Generating listing...")
        print("="*60)
        
        generate_listing(woff_df, macro_vars)
        
        # Step 8: Save WOFF as parquet for reference
        output_file = OUTPUT_DIR / f"WOFF_{macro_vars['RDATE']}.parquet"
        woff_df.write_parquet(output_file)
        print(f"\nWOFF data saved to: {output_file}")
        
        print(f"\nProcessing completed successfully at {datetime.now()}")
        
    except Exception as e:
        print(f"\nError during processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
