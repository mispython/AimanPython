# SAS to Python Conversion: Top 100 Depositors Report

## Overview

This Python program converts a SAS program that generates three comprehensive reports on the largest depositors across Fixed Deposits (FD), Current Accounts (CA), and Savings Accounts (SA):

1. **Top 100 Individual Customers** (FD11TEXT.txt)
2. **Top 100 Corporate Customers** (FD12TEXT.txt)
3. **Group of Companies under Top 100 Corporate Depositors** (FD2TEXT.txt)

## Report Details

**Program IDs:** EIBDTP50, EIBDTOP5  
**Purpose:** Identify and report on largest depositors for risk management and relationship monitoring

## Requirements

```bash
pip install duckdb polars pyarrow --break-system-packages
```

## Input Files

### 1. Core Data Files

**REPTDATE** (`data/DEPOSIT/REPTDATE.parquet`)
- Reporting date
- Column: `REPTDATE` (SAS date format)

**CURRENT** (`data/DEPOSIT/CURRENT.parquet`)
- Current account balances
- Key columns: `ACCTNO`, `CURBAL`, `PRODUCT`, `CUSTCODE`, `PURPOSE`, `BRANCH`, `NAME`, `CURCODE`

**FD** (`data/DEPOSIT/FD.parquet`)
- Fixed deposit balances
- Key columns: `ACCTNO`, `CURBAL`, `CUSTCODE`, `PURPOSE`, `BRANCH`, `NAME`, `CURCODE`, `PRODUCT`

**SAVING** (`data/DEPOSIT/SAVING.parquet`)
- Savings account balances
- Key columns: `ACCTNO`, `CURBAL`, `PRODUCT`, `CUSTCODE`, `PURPOSE`, `BRANCH`, `NAME`

### 2. CIS (Customer Information System) Files

**CISDP/DEPOSIT** (`data/CISDP/DEPOSIT.parquet`)
- CIS data for current accounts
- Filter: `SECCUST = '901'` and `ACCTNO` between 3000000000 - 3999999999
- Key columns: `CUSTNO`, `ACCTNO`, `CUSTNAME`, `ICNO`, `NEWIC`, `OLDIC`, `INDORG`

**CISFD/DEPOSIT** (`data/CISFD/DEPOSIT.parquet`)
- CIS data for fixed deposits and savings
- Filter: `SECCUST = '901'` and account ranges:
  - 1000000000 - 1999999999
  - 7000000000 - 7999999999
  - 4000000000 - 6999999999
- Key columns: `CUSTNO`, `ACCTNO`, `CUSTNAME`, `ICNO`, `NEWIC`, `OLDIC`, `INDORG`

### 3. Reference Files

**COF_MNI_DEPOSITOR_LIST** (`data/LIST/COF_MNI_DEPOSITOR_LIST.parquet`)
- List of depositor groups (subsidiaries and related companies)
- Key columns: `DEPID`, `DEPGRP`, `BUSSREG`, `CUSTNO`

**KEEP_TOP_DEP_EXCL_PBB** (`data/LIST/KEEP_TOP_DEP_EXCL_PBB.parquet`)
- Exclusion list for subsidiary reporting
- Key column: `CUSTNO`

### 4. Format Files (Optional)

**CAPROD** (`data/formats/CAPROD.parquet`)
- Product code mapping for Current Accounts
- Columns: `PRODUCT`, `PRODCD`

**SAPROD** (`data/formats/SAPROD.parquet`)
- Product code mapping for Savings Accounts
- Columns: `PRODUCT`, `PRODCD`

## Output Files

### 1. FD11TEXT.txt (Individual Customers)
Two-part report:
- Summary: Top 100 individual depositors by total balance
- Details: All accounts for each top 100 depositor

### 2. FD12TEXT.txt (Corporate Customers)
Two-part report:
- Summary: Top 100 corporate depositors by total balance
- Details: All accounts for each top 100 depositor

### 3. FD2TEXT.txt (Subsidiaries)
Detailed report of subsidiary companies grouped under parent depositor groups

## Program Logic

### Week Number Calculation

Based on day of month:
- Day 8: Week 1
- Day 15: Week 2
- Day 22: Week 3
- Other days: Week 4

### Customer Classification

#### Individual vs Corporate
- **Individual:** `CUSTCODE` in (77, 78, 95, 96)
- **Corporate:** `CUSTCODE` not in (77, 78, 95, 96) AND `INDORG = 'O'`

#### Joint Accounts
- Identified by `PURPOSE = '2'`
- For individuals: ICNO set to 'JOINT', CUSTNAME set to NAME

### Data Processing Flow

#### Step 1: Date Processing
- Reads reporting date
- Calculates week number
- Formats date for report headers

#### Step 2: CIS Data Loading
- Loads customer information for CA and FD/SA accounts
- Filters by SECCUST and account ranges
- Updates ICNO (uses NEWIC if available, else CUSTNO)

#### Step 3: Current Account Processing
1. Load CA data where CURBAL > 0
2. Apply CAPROD format mapping
3. Filter out PRODCD = 'N'
4. Merge with CIS data
5. Update blank CUSTNAME with NAME
6. Create CABAL column (copy of CURBAL)
7. Split into:
   - **CAIND:** Individual customers
   - **CAORG:** Corporate customers (exclude PURPOSE='2')
8. Handle JOINT accounts for CAIND

#### Step 4: Fixed Deposit Processing
1. Load FD data where CURBAL > 0
2. Merge with CIS data
3. Update blank CUSTNAME with NAME
4. Create FDBAL column (copy of CURBAL)
5. Split into:
   - **FDIND:** Individual customers
   - **FDORG:** Corporate customers (exclude PURPOSE='2')
6. Handle JOINT accounts for FDIND

#### Step 5: Savings Account Processing
1. Load SA data where CURBAL > 0
2. Apply SAPROD format mapping
3. Filter out PRODCD = 'N'
4. Merge with CIS data
5. Update blank CUSTNAME with NAME
6. Create SABAL column (copy of CURBAL)
7. Split into:
   - **SAIND:** Individual customers
   - **SAORG:** Corporate customers (exclude PURPOSE='2')
8. Handle JOINT accounts for SAIND

#### Step 6: Top 100 Individual Report Generation
1. Combine FDIND, CAIND, SAIND
2. Replace blank ICNO with 'XX'
3. Group by ICNO and CUSTNAME
4. Sum CURBAL, FDBAL, CABAL, SABAL
5. Sort by CURBAL descending
6. Take top 100
7. Generate summary report
8. Join back to detail records
9. Generate detailed account listing

#### Step 7: Top 100 Corporate Report Generation
1. Combine FDORG, CAORG, SAORG
2. Replace blank ICNO with 'XX'
3. Group by ICNO and CUSTNAME
4. Sum CURBAL, FDBAL, CABAL, SABAL
5. Sort by CURBAL descending
6. Take top 100
7. Generate summary report
8. Join back to detail records
9. Generate detailed account listing

#### Step 8: Subsidiaries Report Generation
1. Start with all corporate data (FDORG, CAORG, SAORG)
2. Update ICNO (use NEWIC if available, else OLDIC)
3. Add RMAMT and FCYAMT based on CURCODE
4. **First join:** Match by NEWIC → BUSSREG in COF_MNI_DEPOSITOR_LIST
5. **Second join:** Match remaining by CUSTNO
6. Exclude records in KEEP_TOP_DEP_EXCL_PBB
7. Group by DEPID (depositor group ID)
8. Generate separate report for each DEPID/DEPGRP

### Report Formats

#### Summary Report Format
```
PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTP50
TOP 100 LARGEST FD/CA/SA INDIVIDUAL CUSTOMERS AS AT dd/mm/yy

DEPOSITOR                                          TOTAL BALANCE      FD BALANCE      CA BALANCE      SA BALANCE
------------------------------------------------------------------------------------------------------------------
JOHN DOE                                           12,345,678.90   10,000,000.00    2,000,000.00      345,678.90
JANE SMITH                                         10,234,567.89    9,000,000.00    1,000,000.00      234,567.89
...
```

#### Detail Report Format
```
ICNO=123456789012  CUSTNAME=JOHN DOE

BRANCH     MNI NO CUSTCD DEPOSITOR                      CIS NO     NEW IC          OLD IC          CURRENT BALANCE PRODUCT
CODE                                                                                                      BALANCE
----------------------------------------------------------------------------------------------------------------------------------
   101 3001234567     77 JOHN DOE                      C1234567   123456789012    987654321        10,000,000.00      100
   102 1002345678     77 JOHN DOE                      C1234567   123456789012    987654321         2,345,678.90      200
                                                                                            CURBAL    12,345,678.90
```

#### Subsidiaries Report Format
```
PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTOP5
GROUP OF COMPANIES UNDER TOP 100 CORP DEPOSITORS @dd/mm/yy
***** ABC GROUP OF COMPANIES *****

BRANCH     MNI NO DEPOSITOR                      CIS NO     CUSTCD CURRENT BALANCE PRODUCT
CODE                                                                      BALANCE
----------------------------------------------------------------------------------------------------
CUSTNO=C1000001
   101 3001234567 ABC COMPANY SDN BHD            C1000001       80    5,000,000.00      100
   102 1002345678 ABC COMPANY SDN BHD            C1000001       80    3,000,000.00      200
                                                                      CURBAL     8,000,000.00

CUSTNO=C1000002
   103 3003456789 ABC TRADING SDN BHD            C1000002       80    2,500,000.00      100
                                                                      CURBAL     2,500,000.00
```

## ASA Carriage Control

All reports use ASA (American Standards Association) carriage control:
- **Space (' '):** Single spacing (normal line)
- **'0':** Double spacing
- **'1':** New page
- Page length: 60 lines

## Key Business Rules

### Account Filtering

1. **Current Accounts (CA):**
   - Must have CURBAL > 0
   - PRODCD must not equal 'N'
   - Account range: 3000000000 - 3999999999

2. **Fixed Deposits (FD):**
   - Must have CURBAL > 0
   - Account ranges:
     - 1000000000 - 1999999999
     - 7000000000 - 7999999999
     - 4000000000 - 6999999999

3. **Savings Accounts (SA):**
   - Must have CURBAL > 0
   - PRODCD must not equal 'N'
   - Same account ranges as FD

### Customer Code Classification

- **77, 78, 95, 96:** Individual customers
- **Others:** Corporate customers (if INDORG = 'O')

### Purpose Code Handling

- **PURPOSE = '2':** Joint accounts
  - For individual reports: Special handling (ICNO='JOINT')
  - For corporate reports: Excluded

### ICNO (Identification Number) Logic

Priority order:
1. NEWIC (New IC number)
2. OLDIC (Old IC number)
3. CUSTNO (Customer number)
4. 'XX' (if all are blank)

### Subsidiary Matching Logic

Two-stage matching process:
1. **By Business Registration:** Match NEWIC to BUSSREG
2. **By Customer Number:** Match CUSTNO to CUSTNO

### Balance Calculations

- **CURBAL:** Total current balance (from source tables)
- **FDBAL:** Fixed deposit balance (0 if no FD accounts)
- **CABAL:** Current account balance (0 if no CA accounts)
- **SABAL:** Savings account balance (0 if no SA accounts)
- **Total Balance:** Sum of FDBAL + CABAL + SABAL

## Key Differences from SAS

### 1. Format Mappings
- **SAS:** Uses PUT() function with formats from PBBDPFMT
- **Python:** Uses dictionary lookups from parquet files

### 2. Data Processing
- **SAS:** Multiple DATA steps with BY-group processing
- **Python:** Polars group_by operations and joins

### 3. Deduplication
- **SAS:** PROC SORT with NODUPKEY
- **Python:** Polars unique() method

### 4. Report Generation
- **SAS:** PROC PRINT with format statements
- **Python:** Manual formatting with string operations

### 5. Macro Processing
- **SAS:** %MACRO for repetitive operations
- **Python:** Functions and loops

## Performance Considerations

### Optimizations

1. **Efficient Joins:**
   - DuckDB for initial filtering
   - Polars for joins and aggregations

2. **Memory Management:**
   - Processes each customer type separately
   - Selective column loading

3. **Reduced Sorting:**
   - Only sorts when necessary for output
   - Uses indexed operations where possible

### Data Volume Handling

- Top 100 limitation reduces output size
- Deduplication removes redundant records
- Efficient parquet format for large datasets

## Error Handling

1. **Missing Format Files:**
   - Default values used
   - Warning messages displayed

2. **Missing Reference Files:**
   - Processing continues without subsidiaries
   - Appropriate warnings issued

3. **Empty Data Sets:**
   - Gracefully skips empty groups
   - No errors thrown

4. **Data Type Issues:**
   - Defensive null checking
   - Type conversion with defaults

## Testing Recommendations

### Data Validation

1. **Customer Segmentation:**
   - Verify CUSTCODE filtering (77,78,95,96)
   - Check INDORG = 'O' for corporate
   - Validate PURPOSE = '2' exclusions

2. **Balance Calculations:**
   - Sum of FDBAL + CABAL + SABAL = CURBAL
   - Verify CURBAL > 0 filtering

3. **Account Range Filtering:**
   - Check CA range (3000000000-3999999999)
   - Check FD ranges (3 separate ranges)

### Report Verification

1. **Top 100 Selection:**
   - Confirm descending CURBAL sort
   - Verify exactly 100 or fewer records

2. **Detail Records:**
   - All accounts for top 100 depositors included
   - Grouping by ICNO/CUSTNAME correct

3. **Subsidiaries:**
   - DEPID grouping correct
   - Exclusion list properly applied
   - Parent-subsidiary relationships maintained

### Format Testing

1. **ASA Carriage Control:**
   - Verify space (' ') for normal lines
   - Check '1' for new pages

2. **Number Formatting:**
   - Comma separators correct
   - Decimal places (2) consistent
   - Right-alignment proper

3. **Column Alignment:**
   - Headers align with data
   - Fixed-width fields correct

## Usage

```bash
# Ensure input files exist in correct directories
# Run the conversion
python sas_top_depositors_conversion.py

# Output files will be created in output/ directory:
# - FD11TEXT.txt (Individual customers)
# - FD12TEXT.txt (Corporate customers)
# - FD2TEXT.txt (Subsidiaries)
```

## Maintenance Notes

### Customer Codes
- Individual codes: 77, 78, 95, 96
- Update filter if codes change

### Account Ranges
- CA: 3000000000-3999999999
- FD: 1000000000-1999999999, 7000000000-7999999999, 4000000000-6999999999
- Update queries if ranges change

### Product Code Exclusion
- PRODCD = 'N' excluded for CA and SA
- Update if exclusion logic changes

### Report Count
- Currently limited to top 100
- Change `.head(100)` to adjust

## Troubleshooting

### Issue: Missing depositors in top 100
**Solution:** Check ICNO filtering (must not be blank/null)

### Issue: Wrong balance totals
**Solution:** Verify FDBAL, CABAL, SABAL calculations and summation

### Issue: Subsidiaries not matching
**Solution:** Check NEWIC/OLDIC values and COF_MNI_DEPOSITOR_LIST data

### Issue: Joint accounts misclassified
**Solution:** Verify PURPOSE = '2' handling logic

### Issue: Report formatting incorrect
**Solution:** Check ASA carriage control characters and column widths

## Dependencies

- **Python 3.8+**
- **duckdb:** Parquet file reading and SQL operations
- **polars:** Fast data transformations
- **pandas:** Complex operations (via polars.to_pandas())
- **pyarrow:** Parquet support

## Security Considerations

This program handles sensitive customer financial data. Ensure:
1. Secure storage of parquet files
2. Access control to output reports
3. Encryption at rest and in transit
4. Audit logging of report generation
5. Compliance with data protection regulations
