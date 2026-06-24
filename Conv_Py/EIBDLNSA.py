"""
EIBDLNSA - Branch Summary on Daily Outstanding for BAE Personal Financing-I
Compares current vs previous month loan balances for products 135/136.
Includes PROC TABULATE-style output with proper formatting.
"""

import polars as pl
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'MIS': 'data/mis/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# =============================================================================
# REPORT DATE
# =============================================================================
def get_report_vars():
    """Get report date variables from MIS.REPTDATE"""
    df = pl.read_parquet(f"{PATHS['MIS']}REPTDATE.parquet")
    reptdate = df['REPTDATE'][0]
    
    mm = reptdate.month
    # Month rollback logic
    mm1 = mm - 1
    year = reptdate.year
    if mm1 == 0:
        mm1 = 12
        year = year - 1  # Previous year when month is January
    
    nowk = '4'  # Hardcoded in SAS
    dletday = 0  # Not defined in code, default to 0
    
    return {
        'reptmon': f"{mm:02d}",
        'reptmon1': f"{mm1:02d}",
        'reptday': f"{reptdate.day:02d}",
        'reptyear': str(reptdate.year),
        'rdate': reptdate.strftime('%d%m%Y'),
        'nowk': nowk,
        'dletday': f"{dletday:02d}",
        'date_obj': reptdate,
        'prev_year': year
    }

# =============================================================================
# LOAN DATA PROCESSING
# =============================================================================
def main():
    print("=" * 60)
    print("EIBDLNSA - Branch Summary - BAE Personal Financing-I")
    print("=" * 60)
    
    # Get report variables
    rep = get_report_vars()
    print(f"\nReport Date: {rep['rdate']}")
    print(f"Current Month: {rep['reptmon']}, Previous Month: {rep['reptmon1']} (Year: {rep['prev_year']})")
    print(f"NOWK: {rep['nowk']}, DLETDAY: {rep['dletday']}")
    
    # Current month loans
    try:
        curr_file = f"{PATHS['MIS']}LOAN{rep['reptmon']}{rep['nowk']}.parquet"
        curr_df = pl.read_parquet(curr_file)
        curr_df = curr_df.filter(pl.col('PRODUCT').is_in([135, 136]))
        
        # Summarize by BRANCH, PRODUCT
        curr_sum = curr_df.group_by(['BRANCH', 'PRODUCT']).agg([
            pl.len().alias('NOACCT'),
            pl.col('BALANCE').sum().alias('BRLNAMT')
        ])
        print(f"Current month: {len(curr_sum)} branch-product combinations")
    except Exception as e:
        print(f"Error reading current month: {e}")
        curr_sum = pl.DataFrame(schema={'BRANCH': pl.Int64, 'PRODUCT': pl.Int64, 
                                        'NOACCT': pl.Int64, 'BRLNAMT': pl.Float64})
    
    # Previous month loans (may be different year)
    try:
        prev_file = f"{PATHS['MIS']}LOAN{rep['reptmon1']}{rep['nowk']}.parquet"
        prev_df = pl.read_parquet(prev_file)
        prev_df = prev_df.filter(pl.col('PRODUCT').is_in([135, 136]))
        
        prev_sum = prev_df.group_by(['BRANCH', 'PRODUCT']).agg([
            pl.len().alias('PNOACCT'),
            pl.col('BALANCE').sum().alias('PBRLNAMT')
        ])
        print(f"Previous month: {len(prev_sum)} branch-product combinations")
    except Exception as e:
        print(f"Error reading previous month: {e}")
        prev_sum = pl.DataFrame(schema={'BRANCH': pl.Int64, 'PRODUCT': pl.Int64,
                                        'PNOACCT': pl.Int64, 'PBRLNAMT': pl.Float64})
    
    # Merge current and previous
    loan_df = curr_sum.join(prev_sum, on=['BRANCH', 'PRODUCT'], how='outer').fill_null(0)
    
    # Read branch data (fixed-width)
    branch_file = Path("data/input/BRANCHF.txt")
    branches = []
    if branch_file.exists():
        with open(branch_file, 'r') as f:
            for line in f:
                if len(line) >= 42:
                    # SAS: @001 BANK $1. @002 BRANCH 3. @006 ABBREV $3. @012 BRCHNAME $30.
                    branches.append({
                        'BANK': line[0:1].strip(),
                        'BRANCH': int(line[1:4].strip()) if line[1:4].strip().isdigit() else 0,
                        'ABBREV': line[5:8].strip(),
                        'BRCHNAME': line[11:41].strip()
                    })
    branch_df = pl.DataFrame(branches).sort('BRANCH')
    print(f"Branches loaded: {len(branch_df)}")
    
    # Merge with branch info
    final_df = loan_df.join(branch_df, on='BRANCH', how='left')
    final_df = final_df.with_columns([
        (pl.col('BRLNAMT') - pl.col('PBRLNAMT')).alias('VARIANLN')
    ])
    
    # Sort for display
    final_df = final_df.sort(['BRANCH', 'PRODUCT'])
    
    # Generate PROC TABULATE style report
    print("\n" + "=" * 100)
    print("REPORT ID : EIBDLNSA")
    print("PUBLIC BANK BERHAD")
    print("BRANCH SUMM ON DAILY O/S (RM) - BAE PERSONAL FINANCING-I")
    print("BRANCH SUMM ON DAILY O/S (RM) - PLUS BAE PERSONAL FINANCING-I")
    print(f"AS AT {rep['rdate']}")
    print("=" * 100)
    
    # Calculate grand totals
    grand_total = {
        'NOACCT': final_df['NOACCT'].sum(),
        'PNOACCT': final_df['PNOACCT'].sum(),
        'BRLNAMT': final_df['BRLNAMT'].sum(),
        'PBRLNAMT': final_df['PBRLNAMT'].sum()
    }
    
    # Get unique products
    products = sorted(final_df['PRODUCT'].unique().to_list())
    
    # Print header with product columns (TABULATE style)
    header1 = f"{'BRANCH':<21}"
    header2 = f"{'BRANCH':<21}"
    for prod in products:
        header1 += f"{f'PRODUCT {prod}':>52}"
        header2 += f"{'CURR TOT':>12} {'PREV TOT':>12} {'CURR AMOUNT':>15} {'PREV AMOUNT':>15}"
    print(header1)
    print(header2)
    print("-" * (21 + len(products) * 54))
    
    # Print each branch
    for branch in sorted(final_df['BRANCH'].unique().to_list()):
        branch_df = final_df.filter(pl.col('BRANCH') == branch)
        brname = branch_df['BRCHNAME'][0] if len(branch_df) > 0 and branch_df['BRCHNAME'][0] else ''
        
        line = f"{branch:03d} {brname[:17]:<17}"
        
        # Add data for each product
        for prod in products:
            prod_row = branch_df.filter(pl.col('PRODUCT') == prod)
            if len(prod_row) > 0:
                row = prod_row.row(0, named=True)
                line += (f"{row['NOACCT']:>12,.0f} {row['PNOACCT']:>12,.0f} "
                        f"{row['BRLNAMT']:>15,.2f} {row['PBRLNAMT']:>15,.2f}")
            else:
                line += f"{'':>12} {'':>12} {'':>15} {'':>15}"
        
        print(line)
    
    # Grand total line
    print("-" * (21 + len(products) * 54))
    line = f"{'GRAND TOTAL':<21}"
    for prod in products:
        # For grand total, sum across all branches for this product
        prod_total = final_df.filter(pl.col('PRODUCT') == prod).select([
            pl.sum('NOACCT').alias('NOACCT'),
            pl.sum('PNOACCT').alias('PNOACCT'),
            pl.sum('BRLNAMT').alias('BRLNAMT'),
            pl.sum('PBRLNAMT').alias('PBRLNAMT')
        ])
        if len(prod_total) > 0:
            line += (f"{prod_total['NOACCT'][0]:>12,.0f} {prod_total['PNOACCT'][0]:>12,.0f} "
                    f"{prod_total['BRLNAMT'][0]:>15,.2f} {prod_total['PBRLNAMT'][0]:>15,.2f}")
        else:
            line += f"{'':>12} {'':>12} {'':>15} {'':>15}"
    print(line)
    
    # Overall grand total (across all products)
    print("-" * (21 + len(products) * 54))
    print(f"{'OVERALL TOTAL':<21} "
          f"{grand_total['NOACCT']:>12,.0f} {grand_total['PNOACCT']:>12,.0f} "
          f"{grand_total['BRLNAMT']:>15,.2f} {grand_total['PBRLNAMT']:>15,.2f}")
    
    print("\n" + "=" * 60)
    print("✓ EIBDLNSA Complete")

if __name__ == "__main__":
    main()
