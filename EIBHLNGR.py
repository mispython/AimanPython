#!/usr/bin/env python3
"""
File Name: EIBHLNGR
Loan Extended to Non-Resident/Foreign Entities
Report on loans according to guarantee or non-guarantee classification
Generates three reports: Foreign Banking Institutions, Non-Foreign Banking Institutions, and Customer List
"""

import duckdb
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Input paths
INPUT_LOAN_REPTDATE = "input/EIBHLNGR/loan_reptdate.parquet"
INPUT_LOAN_DATA = "input/EIBHLNGR/loan_{month}4.parquet"  # e.g., loan_014.parquet
INPUT_COLL_COLLATER = "input/EIBHLNGR/coll_collater.parquet"

# Output paths
OUTPUT_FOREIGN_BKINST = "/mnt/user-data/outputs/lngrfbi_report.txt"
OUTPUT_NON_FOREIGN_BKINST = "/mnt/user-data/outputs/lnngrfbi_report.txt"
OUTPUT_CUSTOMER_LIST = "/mnt/user-data/outputs/custlist_report.txt"

# Report configuration
PAGE_LENGTH = 60
LINES_PER_PAGE = PAGE_LENGTH - 6  # Account for headers and spacing


# ============================================================================
# CUSTOMER TYPE DESCRIPTIONS (PROC FORMAT)
# ============================================================================
CUSTDESC = {
    '80': 'NON-RESIDENTS/FOREIGN ENTITIES',
    '81': 'FOREIGN BANKING INSTITUTIONS',
    '82': 'AFFILIATES ABROAD',
    '83': 'G7 COUNTRIES',
    '84': 'FOREIGN BANK IN OTHER COUNTRIES',
    '85': 'FOREIGN NON-BANK ENTITIES',
    '86': 'FOREIGN BUSINESS ENTERPRISES',
    '87': 'MICRO',
    '88': 'SMALL',
    '89': 'MEDIUM',
    '90': 'FOREIGN GOVERNMENTS',
    '91': 'FOREIGN CENTRAL BANKS',
    '92': 'FOREIGN DIPLOMATIC REPRESENTATION IN MALAYSIA',
    '95': 'FOREIGN INDIVIDUALS',
    '96': 'FOREIGN EMPLOYED/STUDIED IN MALAYSIA',
    '98': 'FOREIGN NON COMMERCIAL INTERNATIONAL ORGANISATION IN MALAYSIA',
    '99': 'FOREIGN OTHER ENTITIES NIE'
}


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
prevhalf = reptdate - relativedelta(months=6)
# Set to end of month for prevhalf
prevhalf = prevhalf.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)

# Create macro variables
REPTMON = f"{reptdate.month:02d}"
REPTMMMYY = reptdate.strftime('%b%y').upper()
REPTYEAR = reptdate.strftime('%y')
RDATE = reptdate.strftime('%d%b%Y').upper()

print(f"Report Date: {RDATE}")
print(f"Report Month: {REPTMON}")
print(f"Report Period: {REPTMMMYY}")
print(f"Previous Half: {prevhalf.strftime('%d%b%Y').upper()}")


# ============================================================================
# STEP 2: LOAD AND FILTER LOAN DATA (LN)
# ============================================================================
print("\nStep 2: Loading and filtering loan data...")

# Construct dynamic input path
loan_file = INPUT_LOAN_DATA.format(month=REPTMON)

ln = con.execute(f"""
    SELECT 
        ACCTNO,
        NOTENO,
        PRODUCT,
        CUSTCD,
        PAIDIND,
        EIR_ADJ,
        BAL_AFT_EIR
    FROM read_parquet('{loan_file}')
    WHERE (PAIDIND NOT IN ('P', 'C') OR EIR_ADJ IS NOT NULL)
      AND CAST(CUSTCD AS INTEGER) >= 80
""").df()

con.register('ln', ln)
print(f"Loan records loaded: {len(ln):,}")


# ============================================================================
# STEP 3: LOAD AND FILTER COLLATERAL/GUARANTEE DATA
# ============================================================================
print("\nStep 3: Loading collateral/guarantee data...")

cfinguar = con.execute(f"""
    SELECT 
        ACCTNO,
        NOTENO,
        CCOLLNO,
        CDOLARV,
        CGUARNAT,
        'Y' as GUARANTEE_IND
    FROM read_parquet('{INPUT_COLL_COLLATER}')
    WHERE CDOLARV > 0
      AND CGUARNAT IN ('01', '02', '03', '04', '05', '06', '07')
""").df()

con.register('cfinguar', cfinguar)
print(f"Guarantee records loaded: {len(cfinguar):,}")


# ============================================================================
# STEP 4: JOIN LOAN AND GUARANTEE DATA (LN_GUAR)
# ============================================================================
print("\nStep 4: Joining loan and guarantee data...")

ln_guar = con.execute("""
    SELECT 
        t1.ACCTNO,
        t1.NOTENO,
        t1.PRODUCT,
        t1.CUSTCD,
        t1.PAIDIND,
        t1.EIR_ADJ,
        t1.BAL_AFT_EIR,
        t2.CCOLLNO,
        t2.CGUARNAT,
        t2.GUARANTEE_IND
    FROM ln t1
    LEFT JOIN cfinguar t2 
        ON t1.ACCTNO = t2.ACCTNO 
        AND t1.NOTENO = t2.NOTENO
""").df()

con.register('ln_guar', ln_guar)
print(f"Joined records: {len(ln_guar):,}")


# ============================================================================
# STEP 5: CREATE LN_OSBAL (DISTINCT RECORDS WITH GUARANTEE INDICATOR)
# ============================================================================
print("\nStep 5: Creating outstanding balance dataset...")

ln_osbal = con.execute("""
    SELECT DISTINCT 
        ACCTNO,
        NOTENO,
        PRODUCT,
        CUSTCD,
        PAIDIND,
        BAL_AFT_EIR,
        CASE 
            WHEN GUARANTEE_IND = 'Y' THEN 'Y'
            ELSE 'N'
        END AS GUARANTEE_IND
    FROM ln_guar
""").df()

con.register('ln_osbal', ln_osbal)
print(f"Outstanding balance records: {len(ln_osbal):,}")


# ============================================================================
# STEP 6: SUMMARIZE BY CUSTOMER CODE AND GUARANTEE INDICATOR
# ============================================================================
print("\nStep 6: Summarizing by customer code and guarantee...")

osbal_custguar = con.execute("""
    SELECT 
        CUSTCD,
        GUARANTEE_IND,
        SUM(BAL_AFT_EIR) as BAL_AFT_EIR
    FROM ln_osbal
    GROUP BY CUSTCD, GUARANTEE_IND
""").df()

con.register('osbal_custguar', osbal_custguar)
print(f"Customer/Guarantee summary records: {len(osbal_custguar):,}")


# ============================================================================
# STEP 7: SPLIT INTO FOREIGN BANKING AND NON-FOREIGN BANKING INSTITUTIONS
# ============================================================================
print("\nStep 7: Splitting into foreign banking and non-foreign banking institutions...")

# Process and split the data
split_data = con.execute("""
    WITH base_data AS (
        SELECT 
            CUSTCD,
            GUARANTEE_IND,
            BAL_AFT_EIR,
            CASE WHEN GUARANTEE_IND = 'Y' THEN BAL_AFT_EIR ELSE 0 END as BAL_GUAR,
            CASE WHEN GUARANTEE_IND = 'N' THEN BAL_AFT_EIR ELSE 0 END as BAL_NON_GUAR,
            CAST(CUSTCD AS INTEGER) as CUSTCD_NUM
        FROM osbal_custguar
    ),
    foreign_bkinst_records AS (
        -- Original records for CUSTCD < 85
        SELECT CUSTCD, BAL_GUAR, BAL_NON_GUAR, BAL_AFT_EIR
        FROM base_data
        WHERE CUSTCD_NUM < 85
        
        UNION ALL
        
        -- Duplicate with CUSTCD = '81'
        SELECT '81' as CUSTCD, BAL_GUAR, BAL_NON_GUAR, BAL_AFT_EIR
        FROM base_data
        WHERE CUSTCD_NUM < 85
    ),
    non_foreign_bkinst_records AS (
        -- Original records for CUSTCD >= 85
        SELECT CUSTCD, BAL_GUAR, BAL_NON_GUAR, BAL_AFT_EIR
        FROM base_data
        WHERE CUSTCD_NUM >= 85
        
        UNION ALL
        
        -- Duplicate with CUSTCD = '85'
        SELECT '85' as CUSTCD, BAL_GUAR, BAL_NON_GUAR, BAL_AFT_EIR
        FROM base_data
        WHERE CUSTCD_NUM >= 85
    )
    SELECT 'FOREIGN' as CATEGORY, * FROM foreign_bkinst_records
    UNION ALL
    SELECT 'NON_FOREIGN' as CATEGORY, * FROM non_foreign_bkinst_records
""").df()

con.register('split_data', split_data)


# ============================================================================
# STEP 8: SUMMARIZE FOREIGN BANKING INSTITUTIONS
# ============================================================================
print("\nStep 8: Summarizing foreign banking institutions...")

foreign_bkinst_sum = con.execute("""
    SELECT 
        CUSTCD,
        SUM(BAL_GUAR) as BAL_GUAR,
        SUM(BAL_NON_GUAR) as BAL_NON_GUAR,
        SUM(BAL_AFT_EIR) as BAL_AFT_EIR
    FROM split_data
    WHERE CATEGORY = 'FOREIGN'
    GROUP BY CUSTCD
    ORDER BY CUSTCD
""").df()

# Add customer type descriptions
foreign_bkinst_sum['CUSTTYPE'] = foreign_bkinst_sum['CUSTCD'].map(CUSTDESC)
print(f"Foreign banking institution summary: {len(foreign_bkinst_sum):,} records")


# ============================================================================
# STEP 9: SUMMARIZE NON-FOREIGN BANKING INSTITUTIONS
# ============================================================================
print("\nStep 9: Summarizing non-foreign banking institutions...")

non_foreign_bkinst_sum = con.execute("""
    SELECT 
        CUSTCD,
        SUM(BAL_GUAR) as BAL_GUAR,
        SUM(BAL_NON_GUAR) as BAL_NON_GUAR,
        SUM(BAL_AFT_EIR) as BAL_AFT_EIR
    FROM split_data
    WHERE CATEGORY = 'NON_FOREIGN'
    GROUP BY CUSTCD
    ORDER BY CUSTCD
""").df()

# Add customer type descriptions
non_foreign_bkinst_sum['CUSTTYPE'] = non_foreign_bkinst_sum['CUSTCD'].map(CUSTDESC)
print(f"Non-foreign banking institution summary: {len(non_foreign_bkinst_sum):,} records")


# ============================================================================
# STEP 10: PREPARE CUSTOMER DETAIL LIST
# ============================================================================
print("\nStep 10: Preparing customer detail list...")

# Add customer type to ln_guar and merge with ln_osbal
cust_osbal = con.execute("""
    SELECT 
        lg.ACCTNO,
        lg.NOTENO,
        lg.PRODUCT,
        lg.CUSTCD,
        lg.PAIDIND,
        lg.CCOLLNO,
        lg.CGUARNAT,
        lo.BAL_AFT_EIR,
        lo.GUARANTEE_IND
    FROM ln_guar lg
    INNER JOIN ln_osbal lo 
        ON lg.ACCTNO = lo.ACCTNO 
        AND lg.NOTENO = lo.NOTENO
    ORDER BY lg.ACCTNO, lg.NOTENO
""").df()

# Add customer type descriptions
cust_osbal['CUSTTYPE'] = cust_osbal['CUSTCD'].map(CUSTDESC)
print(f"Customer detail records: {len(cust_osbal):,}")


# ============================================================================
# REPORT GENERATION FUNCTIONS
# ============================================================================

def write_report_header(f, title1, title2, is_summary=True):
    """Write report header with ASA carriage control"""
    # '1' = new page
    f.write('1')
    f.write(f'{title1:^132}\n')
    
    f.write(' ')
    f.write(f'{title2:^132}\n')
    
    f.write(' ')
    f.write('\n')
    
    if is_summary:
        # Summary report header
        f.write(' ')
        f.write(f'{"CODE":<6}{"TYPE":<50}{"GUARANTEE O/S BALANCE":>22}{"NON GUARANTEE O/S BALANCE":>28}{"TOTAL OUTSTANDING":>22}\n')
        
        f.write(' ')
        f.write('-' * 132 + '\n')
    else:
        # Detail report header
        f.write(' ')
        f.write(f'{"ACCTNO":<12}{"NOTENO":<10}{"PRODUCT":<10}{"BAL_AFT_EIR":>20}{"CUSTFISS":<10}{"PAIDIND":<10}{"CCOLLNO":<12}{"CGUARNAT":<10}{"GUARANTEE_IND":<15}\n')
        
        f.write(' ')
        f.write('-' * 132 + '\n')


def format_number(value, width=18, decimals=2):
    """Format number with decimals and fixed width"""
    if value is None or (hasattr(value, '__iter__') and len(value) == 0):
        formatted = '.' * decimals if decimals > 0 else ''
        return formatted.rjust(width)
    
    if decimals > 0:
        formatted = f"{float(value):.{decimals}f}"
    else:
        formatted = str(int(value))
    
    return formatted.rjust(width)


# ============================================================================
# STEP 11: GENERATE FOREIGN BANKING INSTITUTIONS REPORT
# ============================================================================
print("\nStep 11: Generating foreign banking institutions report...")

Path(OUTPUT_FOREIGN_BKINST).parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_FOREIGN_BKINST, 'w') as f:
    title1 = 'PUBLIC BANK BERHAD'
    title2 = f'LOAN EXTENDED TO NON-RESIDENT/FOREIGN ENTITIES ACCORDING TO GUARANTEE OR NON GUARANTEE @ {REPTMMMYY}'
    
    write_report_header(f, title1, title2, is_summary=True)
    
    for idx, row in foreign_bkinst_sum.iterrows():
        line = ' '  # ASA carriage control
        
        # CODE (4 chars)
        line += f"{str(row['CUSTCD']):<6}"
        
        # TYPE (customer description)
        custtype = str(row['CUSTTYPE']) if row['CUSTTYPE'] is not None else ''
        line += f"{custtype:<50}"
        
        # GUARANTEE O/S BALANCE (18.2 format, right-aligned in 22 chars)
        line += f"{format_number(row['BAL_GUAR'], 22, 2)}"
        
        # NON GUARANTEE O/S BALANCE (18.2 format, right-aligned in 28 chars)
        line += f"{format_number(row['BAL_NON_GUAR'], 28, 2)}"
        
        # TOTAL OUTSTANDING (18.2 format, right-aligned in 22 chars)
        line += f"{format_number(row['BAL_AFT_EIR'], 22, 2)}"
        
        line += '\n'
        f.write(line)

print(f"Foreign banking institutions report: {OUTPUT_FOREIGN_BKINST}")


# ============================================================================
# STEP 12: GENERATE NON-FOREIGN BANKING INSTITUTIONS REPORT
# ============================================================================
print("\nStep 12: Generating non-foreign banking institutions report...")

Path(OUTPUT_NON_FOREIGN_BKINST).parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_NON_FOREIGN_BKINST, 'w') as f:
    title1 = 'PUBLIC BANK BERHAD'
    title2 = f'LOAN EXTENDED TO NON-RESIDENT/FOREIGN ENTITIES ACCORDING TO GUARANTEE OR NON GUARANTEE @ {REPTMMMYY}'
    
    write_report_header(f, title1, title2, is_summary=True)
    
    for idx, row in non_foreign_bkinst_sum.iterrows():
        line = ' '  # ASA carriage control
        
        # CODE (4 chars)
        line += f"{str(row['CUSTCD']):<6}"
        
        # TYPE (customer description)
        custtype = str(row['CUSTTYPE']) if row['CUSTTYPE'] is not None else ''
        line += f"{custtype:<50}"
        
        # GUARANTEE O/S BALANCE (18.2 format, right-aligned in 22 chars)
        line += f"{format_number(row['BAL_GUAR'], 22, 2)}"
        
        # NON GUARANTEE O/S BALANCE (18.2 format, right-aligned in 28 chars)
        line += f"{format_number(row['BAL_NON_GUAR'], 28, 2)}"
        
        # TOTAL OUTSTANDING (18.2 format, right-aligned in 22 chars)
        line += f"{format_number(row['BAL_AFT_EIR'], 22, 2)}"
        
        line += '\n'
        f.write(line)

print(f"Non-foreign banking institutions report: {OUTPUT_NON_FOREIGN_BKINST}")


# ============================================================================
# STEP 13: GENERATE CUSTOMER DETAIL LIST REPORT
# ============================================================================
print("\nStep 13: Generating customer detail list report...")

Path(OUTPUT_CUSTOMER_LIST).parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_CUSTOMER_LIST, 'w') as f:
    title1 = 'PUBLIC BANK BERHAD'
    title2 = f'SUMMARY DETAILS ON LOAN EXTENDED TO NON-RESIDENT/FOREIGN ENTITIES ACCORDING TO GUARANTEE OR NON GUARANTEE @ {REPTMMMYY}'
    
    write_report_header(f, title1, title2, is_summary=False)
    
    for idx, row in cust_osbal.iterrows():
        line = ' '  # ASA carriage control
        
        # ACCTNO (12 chars)
        line += f"{int(row['ACCTNO']) if row['ACCTNO'] is not None else 0:<12}"
        
        # NOTENO (10 chars)
        noteno = str(row['NOTENO']) if row['NOTENO'] is not None else ''
        line += f"{noteno:<10}"
        
        # PRODUCT (10 chars)
        product = str(row['PRODUCT']) if row['PRODUCT'] is not None else ''
        line += f"{product:<10}"
        
        # BAL_AFT_EIR (18.2 format, right-aligned in 20 chars)
        line += f"{format_number(row['BAL_AFT_EIR'], 20, 2)}"
        
        # CUSTFISS (4 chars, 10 char field)
        custcd = str(row['CUSTCD']) if row['CUSTCD'] is not None else ''
        line += f"{custcd:<10}"
        
        # PAIDIND (10 chars)
        paidind = str(row['PAIDIND']) if row['PAIDIND'] is not None else ''
        line += f"{paidind:<10}"
        
        # CCOLLNO (12 chars)
        ccollno = str(row['CCOLLNO']) if row['CCOLLNO'] is not None else ''
        line += f"{ccollno:<12}"
        
        # CGUARNAT (10 chars)
        cguarnat = str(row['CGUARNAT']) if row['CGUARNAT'] is not None else ''
        line += f"{cguarnat:<10}"
        
        # GUARANTEE_IND (15 chars)
        guar_ind = str(row['GUARANTEE_IND']) if row['GUARANTEE_IND'] is not None else ''
        line += f"{guar_ind:<15}"
        
        line += '\n'
        f.write(line)

print(f"Customer detail list report: {OUTPUT_CUSTOMER_LIST}")


# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*70)
print("REPORT GENERATION COMPLETE")
print("="*70)
print(f"\nGenerated Reports:")
print(f"  1. Foreign Banking Institutions: {OUTPUT_FOREIGN_BKINST}")
print(f"  2. Non-Foreign Banking Institutions: {OUTPUT_NON_FOREIGN_BKINST}")
print(f"  3. Customer Detail List: {OUTPUT_CUSTOMER_LIST}")
print("\nConversion complete!")

# Close DuckDB connection
con.close()
