# SAS to Python Conversion: FD Withdrawal Summary Report

## Overview

This Python program converts a SAS program that generates daily summary reports on reasons for Fixed Deposit (FD) withdrawals. It produces two separate reports:
1. **RM Fixed Deposit Withdrawals** (MYR currency)
2. **FCY Fixed Deposit Withdrawals** (Foreign Currency)

Each report is segmented by customer type (Individual vs Non-Individual) and account type (PBB vs PIBB).

## Report Details

**Report ID:** EIBDRB01  
**Purpose:** Daily Summary Report on Reasons for FD Withdrawals Over-the-Counter Based on Receipts by Branch

## Requirements

```bash
pip install duckdb polars pyarrow --break-system-packages
```

## Input Files

### 1. REPTDATE (data/DEPOSIT/REPTDATE.parquet)
- Contains the reporting date
- Column: `REPTDATE` (SAS date format)

### 2. FDWDRW (data/MIS/FDWDRWmm.parquet)
Monthly file containing FD withdrawal transactions
- Filename includes 2-digit month (e.g., FDWDRW01.parquet for January)
- **Key Columns:**
  - `REPTDATE`: Reporting date (numeric)
  - `BRANCH`: Branch code
  - `CURCODE`: Currency code (MYR or foreign currency)
  - `PRODCD`: Product code
  - `ACCTTYPE`: Account type ('C' for Corporate, 'I' for Individual)
  - `CUSTCODE`: Customer code (77, 78, 95, 96 indicate individual customers)
  - `RSONCODE`: Reason code (W01-W16)
  - `TRANAMT`: Transaction amount

### 3. Format Files (Optional)

**BRCHCD (data/formats/BRCHCD.parquet)**
- Branch code to abbreviation mapping
- Columns: `BRANCH`, `BRABBR`

**REGNEW (data/formats/REGNEW.parquet)**
- Branch code to region mapping
- Columns: `BRANCH`, `REGION`

If format files are not available, the program uses default values.

## Output Files

### 1. RMWDRAW.txt (output/RMWDRAW.txt)
Report for RM (MYR) Fixed Deposit Withdrawals
- CSV format with semicolon (;) delimiters
- ASA carriage control characters
- Page length: 60 lines

### 2. FCYWDRAW.txt (output/FCYWDRAW.txt)
Report for FCY (Foreign Currency) Fixed Deposit Withdrawals
- Same format as RMWDRAW.txt

## Program Logic

### Customer and Account Segmentation

The program segments withdrawal data into **4 groups** for each currency type:

| Group | Account Type | Customer Type | Description |
|-------|--------------|---------------|-------------|
| 1 | Corporate (C) | Individual (77,78,95,96) | PBB Individual |
| 2 | Individual (I) | Individual (77,78,95,96) | PIBB Individual |
| 3 | Corporate (C) | Non-Individual (others) | PBB Non-Individual |
| 4 | Individual (I) | Non-Individual (others) | PIBB Non-Individual |

### Processing Steps

#### Step 1: Date Processing
- Reads reporting date from REPTDATE file
- Converts SAS date (days since 1960-01-01) to Python datetime
- Extracts year, month components

#### Step 2: Data Loading and Filtering
- Reads FDWDRW file for the reporting month
- Filters by REPTDATE to get current day's data
- Applies branch format mappings (BRCHCD, REGNEW)

#### Step 3: Currency-Specific Grouping

**RM (MYR) Processing:**
- Filters for `CURCODE = 'MYR'`
- Excludes `PRODCD = 394`
- Splits into 4 groups based on ACCTTYPE and CUSTCODE

**FCY Processing:**
- Filters for `CURCODE != 'MYR'`
- No PRODCD exclusion
- Splits into 4 groups based on ACCTTYPE and CUSTCODE

#### Step 4: Reason Code Aggregation

For each group and each branch:
- Counts transactions by reason code (W01-W16)
- Sums transaction amounts by reason code
- **Output columns:**
  - `C1-C16`: Count of transactions for each reason code
  - `A1-A16`: Amount of transactions for each reason code
  - `TOT_CNT`: Total count of all transactions
  - `TOT_AMT`: Total amount of all transactions

#### Step 5: Summary Calculations

For each group:
- **Grand totals:**
  - `RC1-RC16`: Total count across all branches for each reason code
  - `RA1-RA16`: Total amount across all branches for each reason code
  - `GTC`: Grand total count
  - `GTA`: Grand total amount

- **Percentage composition:**
  - `PC1-PC16`: Percentage of count for each reason code
  - `PA1-PA16`: Percentage of amount for each reason code
  - `PGTC`: Sum of all count percentages (should be ~100)
  - `PGTA`: Sum of all amount percentages (should be ~100)

#### Step 6: Report Generation

Generates CSV-formatted reports with semicolon delimiters:

**Report Structure:**
1. Header section (Report ID, Title, Date)
2. Section A: Individual Customers (Groups 1-2)
   - Group 1: PBB Individual
   - Group 2: PIBB Individual
3. Section B: Non-Individual Customers (Groups 3-4)
   - Group 3: PBB Non-Individual
   - Group 4: PIBB Non-Individual

**For each group:**
- Column headers showing reason codes W01-W16
- Detail rows by branch
- Total row with grand totals
- Percentage composition row

## Report Format Details

### CSV Structure

The report uses **semicolon (;)** as delimiter with the following columns:

```
BRCH CODE;BRCH ABBR;REGION;W01 NO.;W01 RM;W02 NO.;W02 RM;...;W16 NO.;W16 RM;TOTAL NO.;TOTAL RM
```

### Data Rows
```
123;BRH1;REGION1;5;1000;3;500;...;2;300;50;25000
```

### Total Row
```
;TOTAL;;120;50000;80;30000;...;25;10000;1500;750000
```

### Percentage Row
```
;% COMPOSITION;;8;7;5;4;...;2;1;100;100
```

## ASA Carriage Control

The report includes ASA (American Standards Association) carriage control characters:
- **Space (' ')**: Single spacing (normal line)
- First character of each line is the control character
- Page length: 60 lines

## Reason Codes (W01-W16)

The program tracks 16 different withdrawal reason codes:
- W01, W02, W03, ..., W16
- Each reason code has:
  - Count of transactions (NO.)
  - Total amount (RM)

## Key Differences from SAS

### 1. Macro Processing
- **SAS:** Uses `%MACRO` for repetitive operations
- **Python:** Uses functions and loops

### 2. Format Mappings
- **SAS:** Uses `PUT()` function with formats from PBBELF
- **Python:** Uses dictionary mappings loaded from parquet files

### 3. Data Aggregation
- **SAS:** Uses DATA steps with FIRST./LAST. logic
- **Python:** Uses pandas groupby operations

### 4. Report Generation
- **SAS:** Uses `PUT` statements with `@` positioning
- **Python:** Uses string formatting with semicolon delimiters

### 5. Array Processing
- **SAS:** Uses array notation and DO loops
- **Python:** Uses loops with f-strings for dynamic column names

## Performance Considerations

### Optimizations
1. **DuckDB Integration:**
   - Efficient parquet file reading
   - SQL-like filtering at source

2. **Polars for Transformations:**
   - Fast filtering and column operations
   - Efficient memory usage

3. **Pandas for Complex Aggregations:**
   - Groupby operations for branch-level summaries
   - Flexible data manipulation

### Memory Management
- Processes data in groups (1-4) rather than all at once
- Uses efficient data structures (dictionaries for mappings)

## Error Handling

1. **Missing Format Files:**
   - Program continues with default values
   - Warning message displayed

2. **Empty Groups:**
   - Skips empty groups in report generation
   - No error thrown

3. **Missing Columns:**
   - Defensive programming for optional fields
   - Default values used when columns missing

## Business Logic Notes

### Customer Code Classification
- **Individual Customers:** CUSTCODE in (77, 78, 95, 96)
- **Non-Individual Customers:** All other CUSTCODEs

### Account Type Classification
- **'C' (Corporate):** Categorized as PBB
- **'I' (Individual):** Categorized as PIBB

### Product Code Exclusion
- **RM only:** Excludes PRODCD = 394
- **FCY:** No product exclusions

### Percentage Calculation
- Count percentage: `(Reason Count / Grand Total Count) × 100`
- Amount percentage: `(Reason Amount / Grand Total Amount) × 100`

## Testing Recommendations

### Data Validation
1. Verify group segmentation matches SAS logic
2. Check that CUSTCODE filtering works correctly
3. Validate PRODCD exclusion for RM only

### Calculation Verification
1. Compare grand totals (GTC, GTA) with SAS output
2. Verify percentage calculations sum to ~100%
3. Check reason code aggregations (C1-C16, A1-A16)

### Format Testing
1. Verify CSV delimiter is semicolon
2. Check ASA carriage control characters
3. Validate column alignment matches SAS

### Edge Cases
1. Test with empty groups
2. Test with missing BRANCH mappings
3. Test with all reason codes present vs. sparse data

## Usage

```bash
# Ensure input files exist
# data/DEPOSIT/REPTDATE.parquet
# data/MIS/FDWDRWmm.parquet (where mm is the month)

# Run the conversion
python sas_fdwdraw_conversion.py

# Output files will be created in output/ directory
```

## Maintenance Notes

1. **Reason Code Count:**
   - Currently hardcoded to 16 (W01-W16)
   - Modify loop ranges if reason codes change

2. **Customer Codes:**
   - Individual codes: 77, 78, 95, 96
   - Update filter if codes change

3. **Format Files:**
   - BRCHCD and REGNEW paths at top of script
   - Easy to update if locations change

4. **Output Format:**
   - Semicolon delimiter is hardcoded
   - Modify `write_line()` if format changes

## Troubleshooting

### Issue: "File not found" error
**Solution:** Check that input parquet files exist in correct directories

### Issue: Missing branch abbreviations
**Solution:** Ensure BRCHCD.parquet format file exists or accept defaults

### Issue: Totals don't match SAS
**Solution:** Verify CUSTCODE and PRODCD filtering logic

### Issue: Report format misaligned
**Solution:** Check semicolon delimiters and ASA control characters

## Output Example

```
 REPORT ID : EIBDRB01
 TITLE : DAILY SUMMARY REPORT ON REASONS FOR FD WITHDRAWALS (PBB&PIBB) OVER-THE-COUNTER BASED ON RECEIPTS BY BRANCH
 REPORTING DATE : 01/02/26
 
 (A) INDIVIDUAL CUSTOMER (CUSTOMER CODE: 77,78,95 AND 96)
 
 (I) PBB
 BRCH;BRCH;REGION;;;;;;;;;;;;;;;BY REASON CODE
 CODE;ABBR;;W01;W01;W02;W02;W03;W03;...;W16;W16;TOTAL;TOTAL
 ;;;NO.;RM;NO.;RM;NO.;RM;...;NO.;RM;NO.;RM
 101;BR01;CENTRAL;5;1000;3;500;2;300;...;1;100;50;5000
 102;BR02;NORTHERN;8;2000;5;1200;3;600;...;2;200;75;8500
 ...
 ;TOTAL;;120;50000;80;30000;50;20000;...;25;5000;1500;250000
 ;% COMPOSITION;;8;20;5;12;3;8;...;2;2;100;100
```

## Dependencies

- **Python 3.8+**
- **duckdb:** Parquet file reading
- **polars:** Data transformations
- **pandas:** Complex aggregations (via polars.to_pandas())
- **pyarrow:** Parquet support
