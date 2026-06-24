"""
EIBMFDS2 - Monthly Interest Rates Exception Report
Report for FD accounts with total receipts > RM 1.0M (Corporate)
Features nested breaks: ACCOUNT level and NAME level totals
"""

import polars as pl
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'FD': 'data/fd/',
    'FDC': 'data/fdc/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# =============================================================================
# REPORT DATE
# =============================================================================
def get_report_date():
    """Get report date from FD.REPTDATE"""
    try:
        df = pl.read_parquet(f"{PATHS['FD']}REPTDATE.parquet")
        reptdate = df['REPTDATE'][0]
        reptdt = reptdate.strftime('%d/%m/%Y')
        return reptdt, reptdate
    except Exception as e:
        print(f"  Warning: Could not read REPTDATE: {e}")
        today = datetime.now()
        reptdt = today.strftime('%d/%m/%Y')
        return reptdt, today

# =============================================================================
# MAIN REPORT
# =============================================================================
def main():
    print("=" * 60)
    print("EIBMFDS2 - Monthly Interest Rates Exception Report")
    print("=" * 60)
    
    # Get report date
    reptdt, reptdate = get_report_date()
    print(f"\nReport Date: {reptdt}")
    
    # Read FD1MC data
    print("\nReading FD1MC data...")
    try:
        # Read data - exactly as SAS does (FDC.FD1MC)
        df = pl.read_parquet(f"{PATHS['FDC']}FD1MC.parquet")
        
        # Note: SAS code has no filters - all records included
        # The "> RM 1.0 M" in title is descriptive only
        
        # Ensure required columns exist (with defaults if missing)
        required_cols = ['BRANCH', 'NAMEQ', 'ACCTNO', 'MATDTE', 'CDNO', 
                        'CURBAL', 'TERM', 'DEPDTE', 'RATE', 'NR', 
                        'CURBALN', 'CURBALY']
        
        for col in required_cols:
            if col not in df.columns:
                print(f"  Warning: Column {col} not found, adding with default")
                if col in ['CURBAL', 'CURBALN', 'CURBALY', 'RATE', 'NR']:
                    df = df.with_columns([pl.lit(0.0).alias(col)])
                elif col in ['ACCTNO', 'CDNO', 'TERM']:
                    df = df.with_columns([pl.lit(0).alias(col)])
                else:
                    df = df.with_columns([pl.lit('').alias(col)])
        
        # Use existing columns (no renaming needed as SAS uses original names)
        # But ensure DEPID exists (CDNO in SAS)
        if 'CDNO' in df.columns and 'DEPID' not in df.columns:
            df = df.with_columns([pl.col('CDNO').alias('DEPID')])
        
        # Format dates as strings for display (matching SAS $10. format)
        df = df.with_columns([
            pl.col('DEPDTE').dt.strftime('%d/%m/%Y').alias('DEPNDT_STR'),
            pl.col('MATDTE').dt.strftime('%d/%m/%Y').alias('MATNDT_STR')
        ])
        
        # Sort by BRANCH, NAMEQ, ACCTNO, MATDTE
        df = df.sort(['BRANCH', 'NAMEQ', 'ACCTNO', 'MATDTE'])
        
        print(f"  Records: {len(df):,}")
        
        if len(df) == 0:
            print("  No records found")
            return
        
        # Generate report
        print("\nGenerating report...")
        
        report_lines = []
        report_lines.append("PUBLIC BANK BERHAD - REPORT ID: PDR/D/FDRATE3")
        report_lines.append(f"MONTHLY INTEREST RATES EXCEPTION REPORT BY BRANCH @ {reptdt}")
        report_lines.append("FOR FD ACCOUNTS WITH TOTAL RECEIPTS > RM 1.0 M : CORPORATE")
        report_lines.append("")
        
        # Group by BRANCH (BY BRANCH in PROC REPORT)
        branches = df['BRANCH'].unique().sort()
        
        for branch in branches:
            branch_df = df.filter(pl.col('BRANCH') == branch)
            
            report_lines.append(f"Branch: {branch}")
            report_lines.append("-" * 120)
            report_lines.append(
                f"{'NAME OF CUSTOMER':<35} {'BRN':<4} {'ACCOUNT':<10} {'RECEIPT':<7} {'RECEIPT':>12} "
                f"{'TERM':<4} {'DEPOSIT':<12} {'MATURITY':<12} {'OFFERED':>8} {'COUNTER':>8}"
            )
            report_lines.append(
                f"{'':<35} {'':<4} {'NUMBER':<10} {'NUMBER':<7} {'AMOUNT':>12} "
                f"{'':<4} {'DATE':<12} {'DATE':<12} {'RATE':>8} {'RATE':>8}"
            )
            report_lines.append("-" * 120)
            
            # Group by NAMEQ within BRANCH
            names = branch_df['NAMEQ'].unique().sort()
            
            for name in names:
                name_df = branch_df.filter(pl.col('NAMEQ') == name)
                
                # Group by ACCTNO within NAMEQ
                accounts = name_df['ACCTNO'].unique().sort()
                
                for acct in accounts:
                    acct_df = name_df.filter(pl.col('ACCTNO') == acct).sort('MATDTE')
                    
                    # Detail rows for this account
                    for row in acct_df.rows(named=True):
                        report_lines.append(
                            f"{str(row.get('NAMEQ', ''))[:35]:<35} "
                            f"{str(row.get('BRANCH', ''))[:4]:<4} "
                            f"{str(row.get('ACCTNO', ''))[:10]:<10} "
                            f"{str(row.get('DEPID', ''))[:7]:<7} "
                            f"{row.get('CURBAL', 0):>12,.2f} "
                            f"{str(row.get('TERM', ''))[:4]:<4} "
                            f"{row.get('DEPNDT_STR', ''):<12} "
                            f"{row.get('MATNDT_STR', ''):<12} "
                            f"{row.get('RATE', 0):>8,.2f} "
                            f"{row.get('NR', 0):>8,.2f}"
                        )
                    
                    # BREAK AFTER ACCTNO - account level totals
                    acct_total = acct_df['CURBAL'].sum()
                    acct_total_n = acct_df['CURBALN'].sum()
                    acct_total_y = acct_df['CURBALY'].sum()
                    
                    report_lines.append("-" * 120)
                    report_lines.append(
                        f"{'':<45} ACCT TOT =     {acct_total:>18,.2f} "
                        f"C={acct_total_y:>12,.2f} S={acct_total_n:>12,.2f}"
                    )
                    report_lines.append("-" * 120)
                    report_lines.append("")
                
                # BREAK AFTER NAMEQ - name level totals
                name_total = name_df['CURBAL'].sum()
                name_total_n = name_df['CURBALN'].sum()
                name_total_y = name_df['CURBALY'].sum()
                
                report_lines.append("-" * 120)
                report_lines.append(
                    f"{'':<45} NAME TOT =     {name_total:>18,.2f} "
                    f"C={name_total_y:>12,.2f} S={name_total_n:>12,.2f}"
                )
                report_lines.append("-" * 120)
                report_lines.append("")
            
            # Branch separator
            report_lines.append("=" * 120)
            report_lines.append("")
        
        # Write report
        output_file = f"{PATHS['OUTPUT']}FDRATE3_{reptdate.strftime('%Y%m%d')}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            for line in report_lines:
                f.write(f"{line}\n")
        
        print(f"  Report written to: {output_file}")
        print(f"  Total lines: {len(report_lines):,}")
        
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("✓ EIBMFDS2 Complete")

if __name__ == "__main__":
    main()
