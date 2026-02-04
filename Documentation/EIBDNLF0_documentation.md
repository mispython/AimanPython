# EIBDNLF0 - SAS to Python Conversion: Liquidity Framework Contractual Run-Offs

## Overview

This Python script converts a SAS program that processes loan data to calculate contractual run-offs as of end of month for the New Liquidity Framework. The program generates maturity profiles for various loan types with specific handling for NPL (Non-Performing Loans) and revolving credits.

## File Structure

- **liquidity_runoff.py**: Main Python script that replicates the SAS program logic

## SAS Program Logic

The original SAS program performs the following operations:

1. **Multiple REPTDATE Processing**: 
   - Reads from BNM, LOAN, and LOAN1 sources
   - Calculates week numbers and run-off date (end of month)
2. **Loan Data Processing**:
   - Filters paid loans (PAIDIND not in 'P','C')
   - Processes OD (Overdraft) accounts separately
   - Filters for specific products (PRODCD='34*' or PRODUCT in (225,226))
   - Calculates repayment schedules based on run-off date (not report date)
3. **Maturity Profiling**:
   - Generates output records for each payment with maturity buckets
   - Creates both regular (95*) and NPL (93*) records
   - Uses run-off date as reference instead of report date
4. **Special Processing**:
   - Revolving credit handling (duplicate 95212 → 93212)
   - EIR adjustment additions
   - Foreign currency exclusion (commented out)
5. **Default Records**: Creates zero-amount records for all code combinations
6. **Aggregation**: Summarizes by BNMCODE and removes missing buckets

## Key Differences from Previous Loan Program

### 1. Run-Off Date vs Report Date
**This program uses END OF MONTH (run-off date) as reference**:
- Run-off date = Last day of report month
- All maturity calculations are from run-off date, not report date
- Example: If report date is 2024-01-15, run-off date is 2024-01-31

### 2. Multiple REPTDATE Sources
- **BNM.REPTDATE**: Main report date
- **LOAN1.REPTDATE**: Weekly report date for EIR adjustments
- Different week calculations for each

### 3. Maturity Bucket Thresholds
Different from previous program:
- **≤ 0.255** = '01' (UP TO 1 WK) - Note: 0.255 not 0.1
- **0.255-1** = '02' (>1 WK - 1 MTH)
- **Missing** = '07' - Explicitly mapped, then filtered out

### 4. Revolving Credit Special Logic
- Deletes all 93212 records initially
- Duplicates 95212 records as 93212
- This ensures RC always has both regular and NPL records

### 5. EIR Adjustments
- Read from separate weekly LOAN file
- Added to bucket '06' (> 1 year)
- Creates both 95* and 93* records

### 6. Foreign Currency Handling
- FCY products defined but output commented out
- Code structure present but disabled

## Format Mappings

### Maturity Bucket (REMFMT)

| Remaining Months | Code | Description |
|-----------------|------|-------------|
| ≤ 0.255 | 01 | Up to 1 week |
| 0.255 - 1 | 02 | > 1 week - 1 month |
| 1 - 3 | 03 | > 1 month - 3 months |
| 3 - 6 | 04 | > 3 - 6 months |
| 6 - 12 | 05 | > 6 months - 1 year |
| Missing (None) | 07 | Missing (filtered out) |
| > 12 | 06 | > 1 year |

### Product Classification (LIQPFMT)

Same as previous program:
- **HL**: Housing Loan products
- **RC**: Revolving Credit (350, 910, 925)
- **FL**: Facility Loan (default)

### BNMCODE Structure

Format: `{PREFIX}{ITEM}{CUST}{REM}0000Y`

**PREFIX**:
- **'95'**: Regular loans (RM - Ringgit Malaysia)
- **'93'**: NPL loans (RM)
- **'96'**: Regular loans (FCY - Foreign Currency) [commented out]
- **'94'**: NPL loans (FCY) [commented out]

**ITEM**:
- **'211'**: FL for non-special customers
- **'212'**: RC for non-special customers
- **'213'**: OD (Overdraft)
- **'214'**: HL for special customers (77, 78, 95, 96)
- **'215'**: (Reserved)
- **'219'**: Other

**CUST**:
- **'08'**: Customer codes 77, 78, 95, 96
- **'09'**: All other customers

**REM**: Maturity bucket (01-06, 07 filtered out)

## Input Files

### REPTDATE_BNM.parquet
Main report date from BNM source:
- **REPTDATE**: Report date

### REPTDATE_LOAN.parquet
Report date from LOAN source (may be redundant):
- **REPTDATE**: Report date

### REPTDATE_LOAN1.parquet
Weekly report date for EIR adjustments:
- **REPTDATE**: Weekly report date

### LOAN{MM}{DD}.parquet
Daily loan data where:
- **MM**: Month (01-12)
- **DD**: Day (01-31)

**Required Columns**:
| Column | Type | Description |
|--------|------|-------------|
| PAIDIND | String | Paid indicator ('P', 'C', or other) |
| ACCTYPE | String | Account type ('LN', 'OD') |
| PRODCD | String | Product code |
| PRODUCT | Integer | Product number |
| CUSTCD | String | Customer code |
| BALANCE | Numeric | Outstanding balance |
| PAYAMT | Numeric | Payment amount |
| BLDATE | Date | Last billing date |
| ISSDTE | Date | Issue date |
| EXPRDATE | Date | Expiry date |
| PAYFREQ | String | Payment frequency |
| LOANSTAT | Integer | Loan status (1 = normal) |
| EIR_ADJ | Numeric | EIR adjustment amount (optional) |

## Output Files

### NOTE.parquet
Aggregated output with columns:
- **BNMCODE**: Classification code
- **AMOUNT**: Aggregated amount

### NOTE_REPORT.txt
Text report with ASA carriage control

## Processing Steps

### 1. Read Multiple REPTDATEs
- BNM REPTDATE → Main report date and week
- LOAN1 REPTDATE → Weekly data for EIR
- Calculate run-off date (end of month)

### 2. Calculate Run-Off Date
```python
Run-off date = Last day of report month
Example: REPTDATE = 2024-01-15 → RUNOFFDT = 2024-01-31
```

### 3. Filter Loan Data
- Exclude PAIDIND in ('P', 'C')
- Process OD accounts separately with REMMTH = 0.1
- Filter LN accounts for PRODCD='34*' or PRODUCT in (225, 226)

### 4. Process Each Loan

**For OD Accounts**:
- Set REMMTH = 0.1
- ITEM = '213' (or '219' if PRODCD='34240')
- Output single record

**For LN Accounts**:

a. **Check Expiry vs Run-Off Date**:
   - If EXPRDATE ≤ RUNOFFDT → REMMTH = None (filtered out)
   - If EXPRDATE - RUNOFFDT < 8 days → REMMTH = 0.1
   - Otherwise → Calculate repayment schedule

b. **Process Repayment Schedule**:
   - Set initial BLDATE
   - Loop through billing dates until EXPRDATE
   - For each payment:
     - Calculate REMMTH from BLDATE to RUNOFFDT
     - If BLDATE ≤ RUNOFFDT → REMMTH = None
     - If BLDATE - RUNOFFDT < 8 → REMMTH = 0.1
     - Otherwise → Calculate remaining months
   - Stop if REMMTH > 1 or at expiry

c. **NPL Classification**:
   - If DAYS > 89 OR LOANSTAT ≠ 1:
     - For output: Set REMMTH = 0.1 (bucket 01)
     - For NPL record: Set REMMTH = 13 (bucket 06)

d. **Generate Records**:
   - Regular record: 95{ITEM}{CUST}{BUCKET}0000Y
   - NPL record: 93{ITEM}{CUST}{BUCKET}0000Y

### 5. Add EIR Adjustments
- Read weekly LOAN file
- For records with EIR_ADJ not null:
  - Create 95{ITEM}{CUST}060000Y with EIR_ADJ amount
  - Create 93{ITEM}{CUST}060000Y with EIR_ADJ amount

### 6. Process Revolving Credits
- Delete all existing 93212* records
- Find all 95212* records
- Create duplicate 93212* records from 95212*

### 7. Create Default Records
- Generate zero-amount records for all combinations:
  - Prefixes: 93, 95 (94, 96 for FCY)
  - Items: 211, 212, 213, 214, 215, 219
  - Customers: 08, 09
  - Buckets: 01-06

### 8. Aggregate and Filter
- Combine defaults with data
- Remove bucket '07' (missing REMMTH)
- Aggregate by BNMCODE
- Sort by BNMCODE

## Key Calculations

### Run-Off Date Calculation
```python
rpyr = REPTDATE.year
rpmth = REPTDATE.month
rpdays = days_in_month(rpyr, rpmth)
RUNOFFDT = datetime(rpyr, rpmth, rpdays).date()
```

### Remaining Months (from Run-Off Date)
```python
REMY = MATDT.year - RUNOFFDT.year
REMM = MATDT.month - RUNOFFDT.month
REMD = MATDT.day - RUNOFFDT.day
REMMTH = REMY * 12 + REMM + REMD / days_in_month(RUNOFFDT)
```

### NPL Logic
```python
If DAYS > 89 OR LOANSTAT ≠ 1:
    For 95* record: Use REMMTH = 0.1 (bucket 01)
    For 93* record: Use REMMTH = 13 (bucket 06)
Else:
    Use calculated REMMTH
```

## Example Processing Flow

**Scenario**:
- REPTDATE: 2024-01-15
- RUNOFFDT: 2024-01-31
- EXPRDATE: 2024-06-30
- BLDATE: 2024-03-15
- BALANCE: 100,000
- PAYAMT: 20,000
- DAYS: 50 (not NPL)
- LOANSTAT: 1 (normal)

**Processing**:
1. BLDATE (2024-03-15) > RUNOFFDT (2024-01-31)
2. Days difference: 43 days (≥ 8)
3. Calculate REMMTH: ~1.4 months
4. Since REMMTH > 1: Output final balance at expiry
5. REMMTH at expiry: ~5 months
6. Output:
   - 95211090500Y: 100,000 (bucket 05)
   - 93211090500Y: 100,000 (bucket 05)

## Dependencies

```
polars>=0.20.0
```

## Installation

```bash
pip install polars
```

## Configuration

Edit the path configuration at the top of `liquidity_runoff.py`:

```python
INPUT_DIR = Path("/path/to/input")
OUTPUT_DIR = Path("/path/to/output")
```

## Usage

```bash
python liquidity_runoff.py
```

## Special Cases

### 1. Overdraft (OD) Accounts
- Always treated as immediate (REMMTH = 0.1)
- Item '213' for normal OD, '219' for PRODCD='34240'

### 2. Revolving Credit (RC)
- Initial 93212 records deleted
- 95212 records duplicated to create 93212
- Ensures both regular and NPL records exist

### 3. Product 100 Exception
Hardcoded to ITEM = '212' regardless of classification

### 4. Foreign Currency (FCY)
Products 800-817 are identified but output is commented out in original SAS

### 5. Missing Maturity (Bucket 07)
Records with REMMTH = None get bucket '07', then filtered out before final output

### 6. EIR Adjustments
Always assigned to bucket '06' (> 1 year)

## Validation Points

1. Verify run-off date = last day of report month
2. Check REMMTH calculations use run-off date, not report date
3. Confirm bucket '07' records are removed
4. Validate revolving credit duplication (95212 → 93212)
5. Verify NPL classification (DAYS > 89 or LOANSTAT ≠ 1)
6. Ensure EIR adjustments in bucket '06'
7. Check default zero records for all combinations
8. Confirm PAIDIND filter excludes 'P' and 'C'
9. Validate OD accounts use REMMTH = 0.1
10. Verify Product 100 uses ITEM = '212'

## Differences from SAS

1. **Date handling**: Python datetime instead of SAS dates
2. **Multiple inputs**: Reads three separate REPTDATE files
3. **Macros**: Python functions replace SAS macros
4. **Run-off logic**: Explicitly calculates end-of-month date
5. **FCY handling**: Commented out but structure preserved
6. **Output**: Generates both parquet and text report

## Notes

- Run-off date is CRITICAL - always end of month, not report date
- Bucket threshold 0.255 (not 0.1 or 0.2) for "up to 1 week"
- PAIDIND filter must exclude both 'P' and 'C'
- Revolving credit logic ensures proper NPL records
- EIR adjustments come from different (weekly) LOAN file
- Default records ensure all code combinations exist with zero amounts
- Bucket '07' is explicitly mapped then filtered out
- LOANSTAT ≠ 1 also triggers NPL classification
- Foreign currency output is disabled (commented)
