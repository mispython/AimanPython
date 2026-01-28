#!/usr/bin/env python3
"""
File Name: EIBDLN1M
Daily Movement in Bank's Loans/OD Accounts Report
Net Increased/(Decreased) of RM1 Million & Above Per Customer
"""

import duckdb
from datetime import datetime, timedelta
from pathlib import Path


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Input paths
INPUT_LOAN_REPTDATE = "input/loan_reptdate.parquet"
INPUT_CISDP_DEPOSIT = "input/cisdp_deposit.parquet"
INPUT_CISLN_LOAN = "input/cisln_loan.parquet"
INPUT_BRANCH_FILE = "input/branch.txt"

# Input loan data paths (will be constructed dynamically based on report date)
INPUT_LOAN_CURRENT = "input/loan_{month}{day}.parquet"  # e.g., loan_0128.parquet
INPUT_LOANX_PREVIOUS = "input/loanx_{month}{day}.parquet"  # e.g., loanx_0127.parquet

# Output path
OUTPUT_REPORT = "/mnt/user-data/outputs/eibdln1m_report.txt"


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect(database=':memory:')


# ============================================================================
# STEP 1: PROCESS REPORT DATE
# ============================================================================
print("Step 1: Processing report date...")

# Read REPTDATE from parquet
reptdate_df = con.execute(f"""
    SELECT * FROM read_parquet('{INPUT_LOAN_REPTDATE}')
""").fetchdf()

reptdate = reptdate_df['REPTDATE'].iloc[0]
reptpdat = reptdate - timedelta(days=1)

# Create macro variables (as Python variables)
RDATE = reptdate.strftime('%d/%m/%y')
REPTDAY = f"{reptdate.day:02d}"
REPTPDAY = f"{reptpdat.day:02d}"
REPTMON = f"{reptdate.month:02d}"
REPTPMON = f"{reptpdat.month:02d}"
REPTYEAR = reptdate.strftime('%y')
REPTPYEA = reptpdat.strftime('%y')

print(f"Report Date: {RDATE}")
print(f"Current: {REPTMON}/{REPTDAY}, Previous: {REPTPMON}/{REPTPDAY}")


# ============================================================================
# STEP 2: CREATE CUSTOMER NAME DATASET (CISNM)
# ============================================================================
print("\nStep 2: Creating customer name dataset...")

# Construct dynamic input paths
current_loan_file = INPUT_LOAN_CURRENT.format(month=REPTMON, day=REPTDAY)
previous_loan_file = INPUT_LOANX_PREVIOUS.format(month=REPTPMON, day=REPTPDAY)

# Read and combine deposit and loan data for customer names
cisnm = con.execute(f"""
    WITH combined AS (
        SELECT ACCTNO, CUSTNAM1 as CUSTNAME, SECCUST
        FROM read_parquet('{INPUT_CISDP_DEPOSIT}')
        
        UNION ALL
        
        SELECT ACCTNO, CUSTNAM1 as CUSTNAME, SECCUST
        FROM read_parquet('{INPUT_CISLN_LOAN}')
    ),
    sorted AS (
        SELECT ACCTNO, CUSTNAME, SECCUST
        FROM combined
        ORDER BY ACCTNO, SECCUST, CUSTNAME
    )
    SELECT DISTINCT ON (ACCTNO) ACCTNO, CUSTNAME, SECCUST
    FROM sorted
    ORDER BY ACCTNO
""").df()

con.register('cisnm', cisnm)
print(f"Customer names loaded: {len(cisnm)} records")


# ============================================================================
# STEP 3: PROCESS CURRENT PERIOD DATA (SASC)
# ============================================================================
print("\nStep 3: Processing current period loan data...")

sasc = con.execute(f"""
    WITH loan_data AS (
        SELECT 
            ACCTNO,
            BRANCH,
            'C' as TDI,
            APPRLIMT,
            APPRLIM2,
            BALANCE,
            ACCTYPE,
            PRODUCT,
            CASE
                WHEN ACCTYPE = 'OD' AND PRODUCT NOT IN (107, 173) THEN 'OD'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN (302, 350, 364, 365, 506, 902, 903, 910, 925, 951) THEN 'RC'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN (128, 130, 131, 132, 380, 381, 700, 705, 720, 725) THEN 'HP'
                WHEN ACCTYPE = 'LN' THEN 'TL'
                ELSE NULL
            END as CATG
        FROM read_parquet('{current_loan_file}')
    ),
    filtered AS (
        SELECT ACCTNO, BRANCH, TDI, APPRLIMT, APPRLIM2, BALANCE, CATG
        FROM loan_data
        WHERE CATG IS NOT NULL
    ),
    aggregated AS (
        SELECT 
            ACCTNO,
            BRANCH,
            TDI,
            MAX(APPRLIMT) as APPRLIMT,
            MAX(APPRLIM2) as APPRLIM2,
            CATG,
            SUM(BALANCE) as ACCBAL,
            SUM(APPRLIMT) as LIMTBAL
        FROM filtered
        GROUP BY ACCTNO, BRANCH, TDI, CATG
    )
    SELECT * FROM aggregated
    ORDER BY BRANCH, ACCTNO
""").df()

con.register('sasc', sasc)
print(f"Current period data loaded: {len(sasc)} records")


# ============================================================================
# STEP 4: PROCESS PREVIOUS PERIOD DATA (SASP)
# ============================================================================
print("\nStep 4: Processing previous period loan data...")

sasp = con.execute(f"""
    WITH loan_data AS (
        SELECT 
            ACCTNO,
            BRANCH,
            'P' as TDI,
            APPRLIMT,
            APPRLIM2,
            BALANCE,
            ACCTYPE,
            PRODUCT,
            CASE
                WHEN ACCTYPE = 'OD' AND PRODUCT NOT IN (107, 173) THEN 'OD'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN (302, 350, 364, 365, 506, 902, 903, 910, 925, 951) THEN 'RC'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN (128, 130, 131, 132, 380, 381, 700, 705, 720, 725) THEN 'HP'
                WHEN ACCTYPE = 'LN' THEN 'TL'
                ELSE NULL
            END as CATG
        FROM read_parquet('{previous_loan_file}')
    ),
    filtered AS (
        SELECT ACCTNO, BRANCH, TDI, APPRLIMT, APPRLIM2, BALANCE, CATG
        FROM loan_data
        WHERE CATG IS NOT NULL
    ),
    aggregated AS (
        SELECT 
            ACCTNO,
            BRANCH,
            TDI,
            MAX(APPRLIMT) as APPRLIMT,
            MAX(APPRLIM2) as APPRLIM2,
            CATG,
            SUM(BALANCE) as PACCBAL,
            SUM(APPRLIMT) as PLIMBAL
        FROM filtered
        GROUP BY ACCTNO, BRANCH, TDI, CATG
    )
    SELECT * FROM aggregated
    ORDER BY BRANCH, ACCTNO
""").df()

con.register('sasp', sasp)
print(f"Previous period data loaded: {len(sasp)} records")


# ============================================================================
# STEP 5: MERGE AND CALCULATE MOVEMENTS
# ============================================================================
print("\nStep 5: Merging periods and calculating movements...")

sasc_merged = con.execute("""
    WITH merged AS (
        SELECT 
            COALESCE(c.ACCTNO, p.ACCTNO) as ACCTNO,
            COALESCE(c.BRANCH, p.BRANCH) as BRANCH,
            COALESCE(c.TDI, p.TDI) as TDI,
            c.APPRLIMT,
            c.APPRLIM2,
            COALESCE(c.CATG, p.CATG) as CATG,
            COALESCE(c.ACCBAL, 0.0) as ACCBAL,
            COALESCE(p.PACCBAL, 0.0) as PACCBAL,
            COALESCE(c.LIMTBAL, p.PLIMBAL, 0) as LIMTBAL,
            COALESCE(p.PLIMBAL, 0) as PLIMBAL
        FROM sasc c
        FULL OUTER JOIN sasp p ON c.BRANCH = p.BRANCH AND c.ACCTNO = p.ACCTNO
    )
    SELECT 
        ACCTNO,
        BRANCH,
        TDI,
        APPRLIMT,
        APPRLIM2,
        CATG,
        ACCBAL,
        PACCBAL,
        CASE WHEN TDI = 'P' THEN PLIMBAL ELSE LIMTBAL END as LIMTBAL,
        (ACCBAL - PACCBAL) as MOVEAMTS,
        ABS(ACCBAL - PACCBAL) as MOVEAMT
    FROM merged
    WHERE ABS(ACCBAL - PACCBAL) >= 1000000
""").df()

con.register('sasc_merged', sasc_merged)
print(f"Records with movements >= RM1M: {len(sasc_merged)}")


# ============================================================================
# STEP 6: READ BRANCH DATA
# ============================================================================
print("\nStep 6: Reading branch data...")

# Read branch file (fixed width format)
branch_data = []
with open(INPUT_BRANCH_FILE, 'r') as f:
    for line in f:
        if len(line.strip()) > 0:
            bank = line[0:1]
            branch = int(line[1:4])
            brname = line[5:8]
            branch_data.append({'BRANCH': branch, 'BANK': bank, 'BRNAME': brname})

branch_df = con.execute("""
    SELECT * FROM branch_data
    ORDER BY BRANCH
""").df()

con.register('branch', branch_df)
print(f"Branch data loaded: {len(branch_df)} records")


# ============================================================================
# STEP 7: MERGE WITH BRANCH AND CUSTOMER DATA
# ============================================================================
print("\nStep 7: Merging with branch and customer data...")

final_data = con.execute("""
    WITH with_branch AS (
        SELECT 
            s.*,
            b.BANK,
            b.BRNAME
        FROM sasc_merged s
        LEFT JOIN branch b ON s.BRANCH = b.BRANCH
    ),
    with_customer AS (
        SELECT 
            w.*,
            c.CUSTNAME,
            c.SECCUST
        FROM with_branch w
        LEFT JOIN cisnm c ON w.ACCTNO = c.ACCTNO
    ),
    with_category AS (
        SELECT 
            *,
            CASE
                WHEN CATG = 'OD' THEN 'OVERDRAFT       '
                WHEN CATG = 'TL' THEN 'TERM LOAN       '
                WHEN CATG = 'HP' THEN 'HIRE PURCHASE   '
                WHEN CATG = 'RC' THEN 'REVOLVING CREDIT'
                ELSE NULL
            END as CATEGORY
        FROM with_customer
    )
    SELECT * 
    FROM with_category
    WHERE CATG IS NOT NULL
    ORDER BY CATEGORY, BRANCH, ACCTNO
""").df()

print(f"Final report data: {len(final_data)} records")


# ============================================================================
# STEP 8: GENERATE REPORT WITH ASA CARRIAGE CONTROL
# ============================================================================
print("\nStep 8: Generating report...")

def format_number(value, width, decimals=0, comma=True):
    """Format number with comma separators and fixed width"""
    if value is None or (hasattr(value, '__iter__') and len(value) == 0):
        return ' ' * width
    
    if decimals > 0:
        formatted = f"{float(value):,.{decimals}f}"
    else:
        formatted = f"{int(value):,}"
    
    return formatted.rjust(width)


def write_report_header(f, category):
    """Write report header with ASA carriage control"""
    # '1' = new page
    f.write('1')
    f.write('  PUBLIC BANK BERHAD - RETAIL BANKING DIVISION\n')
    
    f.write(' ')
    f.write('  REPORT TITLE : EIBDLN1M\n')
    
    f.write(' ')
    f.write(f"  DAILY MOVEMENT IN BANK'S LOANS/OD ACCOUNTS @ {RDATE}\n")
    
    f.write(' ')
    f.write('  NET INCREASED/(DECREASED) OF RM1 MILLION & ABOVE PER CUSTOMER\n')
    
    f.write(' ')
    f.write('  *\n')
    
    f.write(' ')
    f.write(f'  {category}\n')
    
    f.write(' ')
    f.write('  ' + '-' * 131 + '\n')
    
    f.write(' ')
    f.write('  BRH  BRH\n')
    
    f.write(' ')
    f.write('  CODE ABBR CUSTOMER NAME' + ' ' * 30)
    f.write('ACCOUNT NO.   APPROVE LIMIT  CURRENT BALANCE')
    f.write('   PREVIOUS BAL    NET (INC/DEC)\n')
    
    f.write(' ')
    f.write('  ' + '-' * 131 + '\n')


# Create output directory if it doesn't exist
Path(OUTPUT_REPORT).parent.mkdir(parents=True, exist_ok=True)

# Write report
with open(OUTPUT_REPORT, 'w') as f:
    current_category = None
    
    for idx, row in final_data.iterrows():
        # Check if we need a new page for new category
        if row['CATEGORY'] != current_category:
            current_category = row['CATEGORY']
            write_report_header(f, current_category)
        
        # Format and write data line with ASA carriage control
        # ' ' = single space (normal line)
        line = ' '
        
        # Branch code (column 2, 3 chars, zero-padded)
        line += f"  {int(row['BRANCH']):03d}"
        
        # Branch name (column 7, 3 chars)
        line += f"  {str(row['BRNAME'])[:3]}"
        
        # Customer name (column 12, 40 chars)
        custname = str(row['CUSTNAME']) if row['CUSTNAME'] is not None else ''
        line += f"  {custname[:40]:40s}"
        
        # Account number (column 54, 10 chars right-aligned)
        line += f"  {int(row['ACCTNO']):>10d}"
        
        # Approve limit (column 66, 15 chars with commas, no decimals)
        line += f"  {format_number(row['LIMTBAL'], 15, 0)}"
        
        # Current balance (column 83, 15 chars with commas, 2 decimals)
        line += f"  {format_number(row['ACCBAL'], 15, 2)}"
        
        # Previous balance (column 100, 15 chars with commas, 2 decimals)
        line += f"  {format_number(row['PACCBAL'], 15, 2)}"
        
        # Net movement (column 117, 15 chars with commas, 2 decimals)
        line += f"  {format_number(row['MOVEAMTS'], 15, 2)}"
        
        line += '\n'
        f.write(line)

print(f"\nReport generated successfully: {OUTPUT_REPORT}")
print("\nConversion complete!")

# Close DuckDB connection
con.close()
