# SAS to Python Conversion Documentation: EIBDLN1M

## Overview
This document describes the conversion of SAS program EIBDLN1M to Python. The program generates a daily report showing movements in loan and overdraft accounts where the net change is RM1 million or above per customer.

## Program Purpose
**Report Name**: EIBDLN1M  
**Title**: Daily Movement in Bank's Loans/OD Accounts  
**Description**: Net Increased/(Decreased) of RM1 Million & Above Per Customer

## Input Files

### 1. Report Date File
- **Path**: `input/loan_reptdate.parquet`
- **Description**: Contains the report date
- **Columns**: REPTDATE (date)

### 2. Customer Information Files
- **Path**: `input/cisdp_deposit.parquet`
- **Description**: Deposit customer information
- **Columns**: ACCTNO, CUSTNAM1, SECCUST

- **Path**: `input/cisln_loan.parquet`
- **Description**: Loan customer information
- **Columns**: ACCTNO, CUSTNAM1, SECCUST

### 3. Branch File
- **Path**: `input/branch.txt`
- **Description**: Branch information (fixed-width format)
- **Format**:
  - Column 1: BANK (1 char)
  - Columns 2-4: BRANCH (3 digits)
  - Columns 6-8: BRNAME (3 chars)

### 4. Current Period Loan Data
- **Path**: `input/loan_{MMDD}.parquet` (dynamically constructed)
- **Example**: `input/loan_0128.parquet` for January 28
- **Columns**: ACCTNO, BRANCH, ACCTYPE, PRODUCT, APPRLIMT, APPRLIM2, BALANCE

### 5. Previous Period Loan Data
- **Path**: `input/loanx_{MMDD}.parquet` (dynamically constructed)
- **Example**: `input/loanx_0127.parquet` for January 27
- **Columns**: ACCTNO, BRANCH, ACCTYPE, PRODUCT, APPRLIMT, APPRLIM2, BALANCE

## Output File

### Report File
- **Path**: `/mnt/user-data/outputs/eibdln1m_report.txt`
- **Format**: Text file with ASA carriage control characters
- **Page Length**: 60 lines per page (standard)

## Processing Logic

### Step 1: Report Date Processing
1. Read report date from `loan_reptdate.parquet`
2. Calculate previous date (report date - 1 day)
3. Extract date components for dynamic file naming:
   - REPTDAY, REPTPDAY: Day (zero-padded)
   - REPTMON, REPTPMON: Month (zero-padded)
   - REPTYEAR, REPTPYEA: Year (2 digits)
   - RDATE: Formatted date (DD/MM/YY)

### Step 2: Customer Name Dataset
1. Combine deposit and loan customer data
2. Select columns: ACCTNO, CUSTNAME (from CUSTNAM1), SECCUST
3. Remove duplicates, keeping first occurrence per ACCTNO

### Step 3: Current Period Processing
1. Read loan data for current date
2. Categorize accounts:
   - **OD (Overdraft)**: ACCTYPE='OD', excluding products 107, 173
   - **RC (Revolving Credit)**: ACCTYPE='LN', products 302, 350, 364, 365, 506, 902, 903, 910, 925, 951
   - **HP (Hire Purchase)**: ACCTYPE='LN', products 128, 130, 131, 132, 380, 381, 700, 705, 720, 725
   - **TL (Term Loan)**: ACCTYPE='LN', all other products
3. Aggregate by ACCTNO and BRANCH:
   - ACCBAL: Sum of BALANCE
   - LIMTBAL: Sum of APPRLIMT

### Step 4: Previous Period Processing
1. Read loan data for previous date
2. Apply same categorization logic
3. Aggregate by ACCTNO and BRANCH:
   - PACCBAL: Sum of BALANCE
   - PLIMBAL: Sum of APPRLIMT

### Step 5: Movement Calculation
1. Merge current and previous period data
2. Calculate movements:
   - MOVEAMTS = ACCBAL - PACCBAL (signed movement)
   - MOVEAMT = ABS(ACCBAL - PACCBAL) (absolute movement)
3. Filter records where MOVEAMT >= 1,000,000
4. Set LIMTBAL = PLIMBAL when TDI='P' (previous period only)

### Step 6: Enrich with Branch and Customer Data
1. Merge with branch data to get branch names
2. Merge with customer data to get customer names
3. Expand category codes to full descriptions:
   - OD → "OVERDRAFT       "
   - TL → "TERM LOAN       "
   - HP → "HIRE PURCHASE   "
   - RC → "REVOLVING CREDIT"

### Step 7: Report Generation
1. Sort by CATEGORY, BRANCH, ACCTNO
2. Generate report with ASA carriage control:
   - '1' = New page (form feed)
   - ' ' = Single space (normal line)
3. New page header for each category change
4. Format data with proper alignment and number formatting

## Report Layout

### Header (per category)
```
PUBLIC BANK BERHAD - RETAIL BANKING DIVISION
REPORT TITLE : EIBDLN1M
DAILY MOVEMENT IN BANK'S LOANS/OD ACCOUNTS @ {date}
NET INCREASED/(DECREASED) OF RM1 MILLION & ABOVE PER CUSTOMER
*
{CATEGORY}
-------------------------------------------------------------------------------
BRH  BRH
CODE ABBR CUSTOMER NAME                          ACCOUNT NO.   APPROVE LIMIT  CURRENT BALANCE   PREVIOUS BAL    NET (INC/DEC)
-------------------------------------------------------------------------------
```

### Data Line Format
| Column | Width | Format | Description |
|--------|-------|--------|-------------|
| 2 | 3 | Zero-padded | Branch code |
| 7 | 3 | Left-aligned | Branch abbreviation |
| 12 | 40 | Left-aligned | Customer name |
| 54 | 10 | Right-aligned | Account number |
| 66 | 15 | Right-aligned, comma | Approved limit (no decimals) |
| 83 | 15 | Right-aligned, comma | Current balance (2 decimals) |
| 100 | 15 | Right-aligned, comma | Previous balance (2 decimals) |
| 117 | 15 | Right-aligned, comma | Net movement (2 decimals) |

## Key Differences: SAS vs Python

### Technology Stack
- **SAS**: PROC SORT, DATA steps, MERGE statements
- **Python**: DuckDB for SQL operations, native Python for report generation

### Data Processing
- **SAS**: Multi-pass processing with intermediate datasets
- **Python**: Single-pass SQL with CTEs (Common Table Expressions)

### Sorting
- **SAS**: Explicit PROC SORT statements required before merges
- **Python**: DuckDB handles joins without explicit sorting; ORDER BY only for final output

### Merging
- **SAS**: Requires pre-sorted datasets with BY statement
- **Python**: SQL FULL OUTER JOIN handles all cases

### Performance Optimizations
1. Removed unnecessary intermediate sorts
2. Combined multiple operations into single SQL statements
3. Used DuckDB for efficient parquet file processing
4. Minimized data movement between processing steps

## Dependencies

### Python Packages
- `duckdb`: SQL analytics engine for parquet processing
- `datetime`: Date manipulation
- `pathlib`: File path handling

### Installation
```bash
pip install duckdb
```

## Usage

### Basic Execution
```bash
python eibdln1m_converter.py
```

### Prerequisites
1. All input parquet files must exist in `input/` directory
2. Parquet files must have column names matching specifications
3. Branch file must be in fixed-width format
4. Date-specific loan files must exist for report date and previous date

### Expected Console Output
```
Step 1: Processing report date...
Report Date: 28/01/26
Current: 01/28, Previous: 01/27

Step 2: Creating customer name dataset...
Customer names loaded: XXXXX records

Step 3: Processing current period loan data...
Current period data loaded: XXXXX records

Step 4: Processing previous period loan data...
Previous period data loaded: XXXXX records

Step 5: Merging periods and calculating movements...
Records with movements >= RM1M: XXX records

Step 6: Reading branch data...
Branch data loaded: XXX records

Step 7: Merging with branch and customer data...
Final report data: XXX records

Step 8: Generating report...
Report generated successfully: /mnt/user-data/outputs/eibdln1m_report.txt

Conversion complete!
```

## ASA Carriage Control Characters

The report uses ASA (American Standards Association) carriage control:
- **'1'**: Form feed (new page)
- **' '**: Single space (normal line advance)

These characters appear as the first character of each line in the output file and control printer behavior in legacy systems.

## Data Quality Notes

1. **NULL Handling**: Missing values are converted to 0.0 for numeric calculations
2. **Duplicate Accounts**: Only the first customer name is kept per account
3. **Product Filtering**: Specific products are excluded from OD category
4. **Movement Threshold**: Only movements >= RM1,000,000 are reported
5. **Category Assignment**: Accounts without valid categories are excluded

## Maintenance Considerations

1. **Product Code Changes**: Product lists in categorization logic may need updates
2. **File Paths**: Adjust paths in configuration section as needed
3. **Date Format**: Currently assumes DD/MM/YY format; modify if different format needed
4. **Column Positions**: Report column positions follow original SAS specifications
5. **Page Length**: Default 60 lines per page; can be modified if needed

## Testing Recommendations

1. Verify date calculations match expected report dates
2. Compare categorization results with original SAS output
3. Check movement calculations for accuracy
4. Validate report formatting and column alignment
5. Test with various data volumes
6. Verify ASA carriage control characters are correct
7. Confirm number formatting matches original report
