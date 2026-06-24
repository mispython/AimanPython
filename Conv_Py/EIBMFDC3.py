"""
EIBMFDC3 - Monthly Interest Rates Exception Report
"""

import polars as pl
from datetime import datetime
from pathlib import Path

PATHS = {
    'FD1M': 'data/fd1m/',
    'FD': 'data/fd/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

def main():
    print("=" * 60)
    print("EIBMFDC3 - Monthly Interest Rates Exception Report")
    print("=" * 60)
    
    # Get report date
    try:
        rept_df = pl.read_parquet(f"{PATHS['FD']}REPTDATE.parquet")
        reptdate = rept_df['REPTDATE'][0]
        reptdt = reptdate.strftime('%d%m%Y')
    except:
        reptdate = datetime.now()
        reptdt = reptdate.strftime('%d%m%Y')
    
    print(f"\nReport Date: {reptdt}")
    
    # Read data - exactly as SAS does (no extra filters)
    try:
        df = pl.read_parquet(f"{PATHS['FD1M']}FD1M.parquet")
        
        # Filter JI='C' only (SAS logic)
        df = df.filter(pl.col('JI') == 'C')
        
        # Rename fields to match SAS program
        df = df.with_columns([
            pl.col('CUSTNAME').alias('NAMEQ'),
            pl.col('CDNO').alias('DEPID'),
            pl.col('TERM').alias('DEPTERM'),
            pl.col('DEPDTE').alias('DEPNDT'),
            pl.col('MATDTE').alias('MATNDT'),
            pl.col('RATE').alias('RATE1')
            # CURBAL, NR, CURBALN, CURBALY assumed to exist in input
        ])
        
        # Sort by BRANCH NAMEQ ACCTNO MATDTE
        df = df.sort(['BRANCH', 'NAMEQ', 'ACCTNO', 'MATDTE'])
        
        print(f"  Records: {len(df):,}")
        
        # Generate report
        print("\nGenerating report...")
        
        report_lines = []
        report_lines.append("PUBLIC BANK BERHAD - REPORT ID: PDR/D/FDRATE3")
        report_lines.append(f"MONTHLY INTEREST RATES EXCEPTION REPORT BY BRANCH @ {reptdt}")
        report_lines.append("FOR FD ACCOUNTS WITH TOTAL RECEIPTS > RM 1.0 M : CORPORATE")
        report_lines.append("")
        
        # Process by BRANCH
        for branch in df['BRANCH'].unique().sort():
            branch_df = df.filter(pl.col('BRANCH') == branch)
            
            report_lines.append(f"Branch: {branch}")
            report_lines.append("-" * 120)
            report_lines.append(
                f"{'NAME OF CUSTOMER':<35} {'ACCOUNT':<10} {'RECEIPT':<9} {'RECEIPT':>12} "
                f"{'TERM':<4} {'DEPOSIT':<12} {'MATURITY':<12} {'OFFERED':>8} {'COUNTER':>8}"
            )
            report_lines.append(
                f"{'':<35} {'NUMBER':<10} {'NUMBER':<9} {'AMOUNT':>12} "
                f"{'':<4} {'DATE':<12} {'DATE':<12} {'RATE':>8} {'RATE':>8}"
            )
            report_lines.append("-" * 120)
            
            # Process by NAMEQ within BRANCH
            for name in branch_df['NAMEQ'].unique().sort():
                name_df = branch_df.filter(pl.col('NAMEQ') == name).sort('ACCTNO')
                
                # Detail rows
                for row in name_df.rows(named=True):
                    depndt = row.get('DEPNDT')
                    depndt_str = depndt.strftime('%d/%m/%Y') if depndt else ''
                    matndt = row.get('MATNDT')
                    matndt_str = matndt.strftime('%d/%m/%Y') if matndt else ''
                    
                    report_lines.append(
                        f"{str(row.get('NAMEQ', ''))[:35]:<35} "
                        f"{str(row.get('ACCTNO', ''))[:10]:<10} "
                        f"{str(row.get('DEPID', ''))[:9]:<9} "
                        f"{row.get('CURBAL', 0):>12,.2f} "
                        f"{str(row.get('DEPTERM', ''))[:4]:<4} "
                        f"{depndt_str:<12} "
                        f"{matndt_str:<12} "
                        f"{row.get('RATE1', 0):>8,.2f} "
                        f"{row.get('NR', 0):>8,.2f}"
                    )
                
                # BREAK AFTER NAMEQ - totals
                report_lines.append("-" * 120)
                report_lines.append(
                    f"{'':<40} NAME TOT = {name_df['CURBAL'].sum():>18,.2f} "
                    f"C={name_df['CURBALY'].sum():>12,.2f} "
                    f"S={name_df['CURBALN'].sum():>12,.2f}"
                )
                report_lines.append("-" * 120)
                report_lines.append("")
            
            report_lines.append("=" * 120)
            report_lines.append("")
        
        # Write output
        out_file = f"{PATHS['OUTPUT']}FDRATE3_{reptdt}.txt"
        with open(out_file, 'w') as f:
            for line in report_lines:
                f.write(f"{line}\n")
        
        print(f"  Report: {out_file}")
        
    except Exception as e:
        print(f"  Error: {e}")
    
    print("\n" + "=" * 60)
    print("✓ EIBMFDC3 Complete")

if __name__ == "__main__":
    main()
