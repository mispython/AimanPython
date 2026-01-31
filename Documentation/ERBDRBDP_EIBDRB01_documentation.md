# SAS to Python Conversion: FCY FD Report Generator

## Overview

This Python program is a direct conversion of a SAS program that generates a daily report on Foreign Currency (FCY) Fixed Deposits and Foreign Company balances.

## Report Details

**Report ID:** EIBDRB01  
**Purpose:** Daily Total Outstanding Balance/Account on FCY FD & Foreign Companies

## Requirements

```bash
pip install duckdb polars pyarrow --break-system-packages
```

## Input Files

The program expects the following parquet files:

1. **REPTDATE** (`data/DEPO/REPTDATE.parquet`)
   - Contains the reporting date
   - Column: `REPTDATE`

2. **FD** (`data/DEPO/FD.parquet`)
   - Fixed deposit data
   - Key columns: `CURCODE`, `CURBAL`, `OPENDT`, `CLOSEDT`, `OPENIND`

3. **WKDTL** (`data/WALK/WKDTLyymmdd.parquet`)
   - Walk-through detail data (foreign companies)
   - Filename includes year (2-digit), month, and day
   - Key columns: `CURR`, `CURBAL`

## Output Files

1. **Store Dataset** (`data/STORE/RB01DPmm.parquet`)
   - Monthly consolidated data
   - Filename includes 2-digit month
   - Updated daily with deduplication by REPTDATE and ID

2. **Report File** (`output/EIBDRB01_report.txt`)
   - Formatted text report with ASA carriage control characters
   - Page length: 60 lines
   - Contains 3-decimal place currency amounts in RM millions

## Program Logic

### Step 1: Date Processing
- Reads reporting date from REPTDATE file
- Extracts year, month, day components
- Converts SAS date (days since 1960-01-01) to Python datetime

### Step 2: FCY Data Processing
- Filters FD data for non-MYR currencies with positive balances
- Converts balances to thousands (division by 1000)
- Parses OPENDT and CLOSEDT from numeric format
- Calculates flags:
  - `OPCLMH`: Opened and closed in same reporting month
  - `OSACCT`: Outstanding account indicator
  - `NOACCT`: Non-zero balance account indicator
- Assigns ID=3 for FD data

### Step 3: WK Data Processing
- Reads walk-through detail file for the specific date
- Negates and converts balances to thousands
- Extracts currency code from CURR field (last 3 characters)
- Assigns ID=2 for WK data

### Step 4: Data Consolidation
- Combines FCY and WK datasets
- Groups by REPTDATE, ID, and CURCODE
- Sums CURBAL, NOACCT, and OSACCT
- Converts to millions (division by 1000) with 3 decimal places

### Step 5: Duplication for Totals
- Creates duplicate records with ID=1 for grand totals
- Maintains original ID values (2 or 3) for subtotals

### Step 6: Total Calculations
- Summarizes by REPTDATE and ID
- Calculates TOTFCYFD (total foreign currency FD)
- Aggregates account counts

### Step 7: Currency Pivot
- Transforms data from long to wide format
- Creates separate columns for each currency:
  - USD, NZD, AUD, GBP, HKD, SGD, EUR, JPY, CAD, CNY, CHF, THB
- Fills missing currencies with 0.000

### Step 8: Data Storage
- **First day of month (day 01):** Creates new store file
- **Other days:** Appends to existing store file and deduplicates
- Deduplication by REPTDATE and ID ensures no duplicate entries

### Step 9: Report Generation
- Reads stored data sorted by REPTDATE and ID
- Generates formatted text report with ASA carriage control
- Format specifications:
  - Carriage control: Space character (' ') for normal lines
  - Column positions match SAS PUT statement @positions
  - 3-decimal formatting for currency amounts
  - Comma-separated thousands (European format: period as separator)
  - Separator lines after ID=3 records

## Report Structure

### Header Section
```
REPORT ID : EIBDRB01
DAILY TOTAL OUTSTANDING BALANCE/ACCOUNT ON FCY FD & FOREIGN COMPANIES
AS AT dd/mm/yy

 DATE                                                      OUTSTANDING AMOUNT (RM'MIL) (3-decimal)                                                                                                                                         TOTAL NO OF O/S ACCT
              USD           NZD           AUD           GBP           HKD           SGD           EUR           JPY           CAD           CNY           CHF           THB           TOT AMT O/S RM            NO OF A/C          NO OF A/C
 (A)+(B)      TOTAL         TOTAL         TOTAL         TOTAL         TOTAL         TOTAL         TOTAL         TOTAL         TOTAL         TOTAL         TOTAL         TOTAL         TOTAL                     (EXCL              (INCL
   (A)        FRGN CO.      FRGN CO.      FRGN CO.      FRGN CO.      FRGN CO.      FRGN CO.      FRGN CO.      FRGN CO.      FRGN CO.      FRGN CO.      FRGN CO.      FRGN CO.      FRGN CO.                  ZERO               ZERO
   (B)        FCY FD        FCY FD        FCY FD        FCY FD        FCY FD        FCY FD        FCY FD        FCY FD        FCY FD        FCY FD        FCY FD        FCY FD        FCY FD                    BALANCE)           BALANCE)
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

### Data Rows
- **ID=1**: Grand total row (shows date)
- **ID=2**: Foreign companies subtotal
- **ID=3**: FCY FD subtotal (followed by separator line)

## Key Differences from SAS

1. **Date Handling:**
   - SAS uses numeric dates (days since 1960-01-01)
   - Python uses datetime objects
   - Conversion handled automatically

2. **Data Processing:**
   - SAS: DATA steps and PROC SQL
   - Python: DuckDB for SQL-like operations, Polars for data manipulation

3. **File Format:**
   - SAS: SAS datasets (.sas7bdat)
   - Python: Parquet files (more efficient, cross-platform)

4. **Report Output:**
   - Both use ASA carriage control characters
   - Python implementation provides exact format match

## ASA Carriage Control Characters

The report uses ASA (American Standards Association) carriage control:
- **Space (' ')**: Single spacing (normal line)
- First character of each line is the control character
- Not visible when printed on compatible printers

## Performance Considerations

1. **Efficient Processing:**
   - DuckDB used for reading parquet files (zero-copy when possible)
   - Polars for fast data transformations
   - Removed unnecessary sort operations where data is already sorted

2. **Memory Management:**
   - Streaming operations where possible
   - Only necessary columns selected early in pipeline

3. **File I/O:**
   - Parquet format for efficient storage and retrieval
   - Append logic minimizes disk operations

## Error Handling

- Missing WKDTL file: Program continues with FCY data only
- Missing currency columns: Automatically filled with 0.000
- Date parsing errors: Returns None and continues processing

## Maintenance Notes

1. Currency codes are hard-coded in the final merge step
2. Page length set to 60 lines for ASA compatibility
3. Number formatting uses European convention (period as thousands separator)
4. All paths defined at the top of the script for easy configuration

## Testing Recommendations

1. Verify date conversion matches SAS date arithmetic
2. Compare output totals with SAS version
3. Check report formatting character-by-character
4. Validate deduplication logic on non-01 days
5. Test with missing WKDTL files

## Usage

```bash
python sas_to_python_conversion.py
```

Ensure input directories exist and contain required parquet files before running.
