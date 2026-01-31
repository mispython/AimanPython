# SAS to Python Conversion: Islamic Bank Savings Account Report

## Overview

This Python program converts a SAS program that generates a monthly report on savings account activity for Public Islamic Bank Berhad. The report tracks accounts opened and closed during the month, with cumulative year-to-date tracking.

## Report Details

**Bank:** Public Islamic Bank Berhad  
**Report Title:** Savings Account Opened/Closed for the Month  
**Purpose:** Track monthly and cumulative savings account activity by branch

## Requirements

```bash
pip install duckdb polars pyarrow --break-system-packages
```

## Input Files

### 1. REPTDATE (data/DEPOSIT/REPTDATE.parquet)
- Contains the reporting date
- Column: `REPTDATE` (SAS date format)

### 2. SAVING (data/DEPOSIT/SAVING.parquet)
Main savings account data file

**Key Columns:**
- `BRANCH`: Branch code
- `PRODUCT`: Product code
- `ACCTNO`: Account number
- `OPENDT`: Opening date
- `CLOSEDT`: Closing date
- `OPENIND`: Opening indicator ('O', 'B', 'C', 'P', 'Z')
- `CURBAL`: Current balance
- `CUSTCODE`: Customer code
- `MTDAVBAL`: Month-to-date average balance
- `YTDAVAMT`: Year-to-date average amount
- `OPENMH`: Opened this month
- `CLOSEMH`: Closed this month
- `LASTTRAN`: Last transaction date
- `BDATE`: Birth date

**Product Filter:**
- Products 200-207
- Products 212-216
- Products 220-223
- Product 218

### 3. SAVGF (data/MIS/SAVGFmm.parquet)
Previous month's cumulative file (where mm is previous month number)

**Key Columns:**
- `BRANCH`: Branch code
- `PRODUCT`: Product code
- `OPENCUM`: Cumulative opened accounts
- `CLOSECUM`: Cumulative closed accounts

## Output Files

### 1. SAVGC (data/MIS/SAVGCmm.parquet)
Detailed dataset of accounts closed in current month

**Contains:** All closed account details for archival purposes

### 2. SAVGF (data/MIS/SAVGFmm.parquet)
Summary dataset with cumulative tracking

**Key Columns:**
- `BRANCH`: Branch code
- `PRODUCT`: Product code
- `OPENMH`: Opened this month
- `CLOSEMH`: Closed this month
- `OPENCUM`: Cumulative opened (year-to-date)
- `CLOSECUM`: Cumulative closed (year-to-date)
- `NETCHGMH`: Net change this month (OPENMH - CLOSEMH)
- `NETCHGYR`: Net change year-to-date (OPENCUM - CLOSECUM)
- `BCLOSE`: Closed by bank
- `CCLOSE`: Closed by customer
- `NOACCT`: Number of active accounts
- `CURBAL`: Total outstanding balance

### 3. Report (output/SAVINGS_ACCOUNT_REPORT.txt)
Formatted tabular report with ASA carriage control

**Format:** Fixed-width columns with branch-level summaries and totals

## Program Logic

### Step 1: Date Processing
- Reads reporting date from REPTDATE
- Calculates current month (MM) and previous month (MM1)
- Handles year-end rollover (January → previous month is December)
- Formats date components for file naming and report headers

### Step 2: SAVING Data Loading and Filtering

**Product Filter:**
Includes only Islamic banking savings products:
- 200-207, 212-216, 220-223, 218

**Branch Filter:**
- Excludes branch 227
- Remaps branch 250 → 092

**Account Status Processing:**

1. **OPENIND='Z' handling:**
   - Convert to 'O' (Open)
   - Set CLOSEMH=0

2. **Account retention logic:**
   Keep accounts where:
   - OPENIND='O' (Open), OR
   - OPENIND in ('B','C','P') AND CLOSEMH=1 (Closed this month)

3. **Close type classification:**
   - **BCLOSE** (Closed by Bank): When OPENIND in ('B','P')
   - **CCLOSE** (Closed by Customer): When OPENIND='C'
   - **NOACCT** (Active accounts): When OPENIND not in ('B','C','P')

### Step 3: SAVGC Dataset Creation

Creates detailed dataset of closed accounts:

1. Filters for OPENIND in ('B','C','P') AND CLOSEMH=1
2. Adds YTDAVBAL (from YTDAVAMT)
3. Sets CUSTFISS=0
4. Parses LASTTRAN date (MMDDYY format)
5. Parses BDATE to DOBMNI (MMDDYYYY format)
6. Saves to MIS/SAVGCmm.parquet

### Step 4: Aggregation by Branch and Product

Groups data by BRANCH and PRODUCT:
- Sums: OPENMH, CURBAL, NOACCT, CLOSEMH, BCLOSE, CCLOSE

### Step 5: Cumulative Data Processing

**For months after January (REPTMON > 01):**

1. Load previous month's SAVGF file
2. Extract OPENCUM → OPENCUX, CLOSECUM → CLOSECUX
3. Apply branch remapping:
   - 227 → 81
   - 250 → 092
4. Aggregate previous month by BRANCH and PRODUCT
5. Merge with current month data (outer join)
6. Fill null values with 0
7. Calculate cumulative values:
   - `OPENCUM = OPENMH + OPENCUX`
   - `CLOSECUM = CLOSEMH + CLOSECUX`
   - `NETCHGMH = OPENMH - CLOSEMH`
   - `NETCHGYR = OPENCUM - CLOSECUM`

**For January (REPTMON = 01):**

1. No previous month data
2. Set cumulative values equal to monthly values:
   - `OPENCUM = OPENMH`
   - `CLOSECUM = CLOSEMH`
   - `NETCHGMH = OPENMH - CLOSEMH`
   - `NETCHGYR = OPENMH - CLOSEMH`

### Step 6: SAVGF Dataset Creation

Saves cumulative summary to MIS/SAVGFmm.parquet for use by next month

### Step 7: Report Generation

1. Aggregates data by BRANCH (sums across all products)
2. Calculates grand totals
3. Generates formatted tabular report

### Step 8: Report Formatting

Creates fixed-width columnar report with:
- Title and date header
- Column headers (3 lines)
- Branch detail rows
- Total row
- ASA carriage control characters

## Report Format

```
PUBLIC ISLAMIC BANK BERHAD
SAVINGS ACCOUNT OPENED/CLOSED FOR THE MONTH AS AT dd/mm/yy

BRANCH     CURRENT CUMULATIVE    CURRENT    CLOSED    CLOSED CUMULATIVE     NO.OF   TOTAL (RM)  NET CHANGE  NET CHANGE
             MONTH     OPENED      MONTH        BY        BY     CLOSED      ACCTS          O/S     FOR THE    YEAR TO
            OPENED                CLOSED      BANK  CUSTOMER                                        MONTH        DATE
---------------------------------------------------------------------------------------------------------------------------
      81        100        450        50        20        30        200        350  1,234,567.89         50         250
      92        150        600        75        30        45        300        525  2,345,678.90         75         300
     ...        ...        ...       ...       ...       ...        ...        ...           ...        ...         ...
---------------------------------------------------------------------------------------------------------------------------
   TOTAL        250      1,050       125        50        75        500        875  3,580,246.79        125         550
```

### Column Descriptions

| Column | Description | Format |
|--------|-------------|--------|
| BRANCH | Branch code | Integer, right-aligned |
| CURRENT MONTH OPENED | Accounts opened this month | Integer with period separators |
| CUMULATIVE OPENED | Total accounts opened YTD | Integer with period separators |
| CURRENT MONTH CLOSED | Accounts closed this month | Integer with period separators |
| CLOSED BY BANK | Closed by bank (OPENIND='B' or 'P') | Integer with period separators |
| CLOSED BY CUSTOMER | Closed by customer (OPENIND='C') | Integer with period separators |
| CUMULATIVE CLOSED | Total accounts closed YTD | Integer with period separators |
| NO.OF ACCTS | Number of active accounts | Integer with period separators |
| TOTAL (RM) O/S | Total outstanding balance in RM | Decimal (2 places), period separators |
| NET CHANGE FOR THE MONTH | OPENMH - CLOSEMH | Integer with period separators |
| NET CHANGE YEAR TO DATE | OPENCUM - CLOSECUM | Integer with period separators |

## ASA Carriage Control

The report uses ASA (American Standards Association) carriage control:
- **'1':** New page (first line)
- **' ':** Single spacing (normal lines)
- Page length: 60 lines

## Key Business Rules

### Branch Handling

1. **Exclusions:**
   - Branch 227 completely excluded from input data

2. **Remapping:**
   - Branch 250 → 092 (in input data)
   - Branch 227 → 81 (in previous month data)

### OPENIND Values

| Code | Meaning | Processing |
|------|---------|------------|
| O | Open | Active account, NOACCT=1 |
| Z | (Special) | Converted to 'O', CLOSEMH=0 |
| B | Closed by Bank | BCLOSE=CLOSEMH |
| C | Closed by Customer | CCLOSE=CLOSEMH |
| P | Closed (Permanently?) | BCLOSE=CLOSEMH |

### Date Parsing

1. **LASTTRAN:** 6-digit MMDDYY format
   - Example: 123121 → 12/31/21

2. **BDATE:** 8-digit MMDDYYYY format
   - Example: 01011990 → 01/01/1990

### Number Formatting

1. **Integers:** Period (.) as thousands separator
   - Example: 1000 → 1.000

2. **Decimals:** Comma (,) as decimal separator, period (.) as thousands
   - Example: 1234567.89 → 1.234.567,89

This follows European number formatting convention.

## Key Differences from SAS

### 1. PROC TABULATE Equivalent
- **SAS:** Uses PROC TABULATE with FORMAT and FORMCHAR
- **Python:** Manual table formatting with fixed-width columns

### 2. Date Processing
- **SAS:** Uses INPUT() function with format strings
- **Python:** Uses datetime.strptime() with format strings

### 3. Cumulative Logic
- **SAS:** Uses %MACRO with %IF for conditional processing
- **Python:** Uses if/else statements

### 4. Data Merging
- **SAS:** MERGE statement with BY
- **Python:** Polars join() with outer join type

### 5. Missing Value Handling
- **SAS:** Automatic . to 0 conversion in arithmetic
- **Python:** Explicit fill_null(0) operations

## Performance Considerations

### Optimizations

1. **Efficient Filtering:**
   - DuckDB SQL for initial product and branch filtering
   - Reduces data loaded into memory

2. **Single Aggregation Pass:**
   - Groups by BRANCH/PRODUCT once
   - Reuses for report generation

3. **Selective Column Loading:**
   - SAVGC saves only required columns
   - Reduces file size

### Memory Management

- Processes data in logical steps
- Converts to pandas only when necessary
- Uses Polars for most operations

## Error Handling

1. **Missing Previous Month File:**
   - Treats as first month
   - Sets cumulative = monthly values
   - Warning message displayed

2. **Missing Columns:**
   - Defensive null checking
   - Optional column handling

3. **Date Parsing Errors:**
   - Returns None for invalid dates
   - Continues processing

## Testing Recommendations

### Data Validation

1. **Product Filtering:**
   - Verify only specified products included
   - Check exclusion of other products

2. **Branch Filtering:**
   - Confirm branch 227 excluded
   - Verify branch 250 → 092 remapping

3. **OPENIND Processing:**
   - Validate 'Z' → 'O' conversion
   - Check BCLOSE/CCLOSE assignment

### Calculation Verification

1. **Monthly Values:**
   - NETCHGMH = OPENMH - CLOSEMH
   - BCLOSE + CCLOSE should ≤ CLOSEMH

2. **Cumulative Values:**
   - OPENCUM = OPENMH + OPENCUX (previous)
   - CLOSECUM = CLOSEMH + CLOSECUX (previous)
   - NETCHGYR = OPENCUM - CLOSECUM

3. **Report Totals:**
   - Sum of branches = TOTAL row
   - All columns sum correctly

### Format Testing

1. **Number Formatting:**
   - Integer values use period separators
   - Decimal values use comma separator
   - Right-alignment correct

2. **ASA Carriage Control:**
   - '1' at start for new page
   - ' ' for subsequent lines

## Usage

```bash
# Ensure input files exist
# data/DEPOSIT/REPTDATE.parquet
# data/DEPOSIT/SAVING.parquet
# data/MIS/SAVGFmm.parquet (for months after January)

# Run the conversion
python sas_savings_report_conversion.py

# Output files will be created:
# - data/MIS/SAVGCmm.parquet (closed accounts detail)
# - data/MIS/SAVGFmm.parquet (cumulative summary)
# - output/SAVINGS_ACCOUNT_REPORT.txt (formatted report)
```

## Maintenance Notes

### Product Codes
Current filter includes:
- 200-207, 212-216, 220-223, 218
- Update filter if product range changes

### Branch Mappings
- Exclusion: 227
- Remapping: 250→092, 227→81 (in previous data)
- Update if branch structure changes

### File Naming
- SAVGC and SAVGF use 2-digit month
- Files named SAVGCmm.parquet, SAVGFmm.parquet
- Update template if naming changes

### Cumulative Reset
- Cumulative counters reset each January
- Previous month file not loaded for month 01
- Verify year-end processing

## Troubleshooting

### Issue: Totals don't match previous reports
**Solution:** Check branch remapping and product filtering

### Issue: Missing previous month data
**Solution:** Verify SAVGF file exists for previous month

### Issue: Incorrect cumulative values
**Solution:** Verify OPENCUX/CLOSECUX calculations and merge logic

### Issue: Wrong number format
**Solution:** Check period/comma separator implementation

### Issue: Branch 250 appearing in report
**Solution:** Verify branch remapping 250→092 applied

## Dependencies

- **Python 3.8+**
- **duckdb:** SQL operations on parquet files
- **polars:** Data transformations
- **pandas:** Complex operations (via polars.to_pandas())
- **pyarrow:** Parquet file support

## Month-End Processing Notes

This program is designed for monthly execution:
1. Run after month-end closing
2. Previous month's SAVGF file must exist (except January)
3. Creates cumulative file for use by next month
4. SAVGC provides audit trail of closed accounts
