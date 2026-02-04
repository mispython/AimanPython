# EIBMLIB4 - SAS to Python Conversion: Loan Maturity Profile Processor

## Overview

This Python script converts a SAS program that processes loan data (BTRAD) and calculates maturity profiles with repayment schedules. The program generates output records for different maturity buckets and loan classifications.

## File Structure

- **loan_maturity_profile.py**: Main Python script that replicates the SAS program logic

## SAS Program Logic

The original SAS program performs the following operations:

1. **REPTDATE Processing**: Reads report date and determines week number based on day of month
2. **Format Definitions**: 
   - REMFMT: Maps remaining months to maturity bucket codes
   - PRDFMT: Classifies products as HL (Housing Loan), RC (Revolving Credit), or FL (Facility Loan)
3. **Macro Definitions**:
   - DCLVAR: Declares arrays for day calculations
   - NXTBLDT: Calculates next billing date based on payment frequency
   - REMMTH: Calculates remaining months to maturity
4. **Loan Processing**:
   - Filters BTRAD data for specific products (PRODCD='34*' or PRODUCT in (225,226))
   - Calculates repayment schedules based on payment frequency
   - Generates output records for each payment with maturity buckets
   - Creates two types of records: 95* (regular) and 93* (NPL if days > 89)
5. **Aggregation**: Summarizes by BNMCODE
6. **Report**: Prints summary with total

## Key Conversion Details

### Week Number Calculation

Based on the day of the month from REPTDATE:
- Day 8 → Week 1
- Day 15 → Week 2
- Day 22 → Week 3
- Other days → Week 4

### Maturity Bucket Mapping (REMFMT)

| Remaining Months | Code | Description |
|-----------------|------|-------------|
| ≤ 0.1 | 01 | Up to 1 week |
| 0.1 - 1 | 02 | > 1 week - 1 month |
| 1 - 3 | 03 | > 1 month - 3 months |
| 3 - 6 | 04 | > 3 - 6 months |
| 6 - 12 | 05 | > 6 months - 1 year |
| > 12 | 06 | > 1 year |

### Product Classification (PRDFMT)

**HL (Housing Loan)**: Products 4, 5, 6, 7, 31, 32, 100-103, 110-116, 170, 200, 201, 204, 205, 209-215, 219, 220, 225-234

**RC (Revolving Credit)**: Products 350, 910, 925

**FL (Facility Loan)**: All other products

### BNMCODE Structure

Format: `{PREFIX}{ITEM}{CUST}{REM}0000Y`

- **PREFIX**: 
  - '95' = Regular loans
  - '93' = NPL (Non-Performing Loans, if days since last billing > 89)
- **ITEM**: Product classification
  - '214' = HL for customer types 77, 78, 95, 96
  - '219' = Other for customer types 77, 78, 95, 96
  - '211' = FL for other customers
  - '212' = RC for other customers
  - '219' = Other for other customers
- **CUST**: Customer type
  - '08' = Customer codes 77, 78, 95, 96
  - '09' = All other customers
- **REM**: Maturity bucket (01-06)

Example: `952140802000Y` = Regular (95) HL loan (214) for special customers (08) with 0.1-1 month maturity (02)

### Payment Frequency Mapping

| PAYFREQ | FREQ | Description |
|---------|------|-------------|
| '1' | 1 | Monthly |
| '2' | 3 | Quarterly |
| '3' | 6 | Semi-annual |
| '4' | 12 | Annual |
| '6' | - | Biweekly (14 days) |

### Repayment Schedule Logic

1. **Expiry within 8 days**: Treat as immediate (REMMTH = 0.1)
2. **Revolving Credit (RC)**: Use expiry date as billing date
3. **Other Loans**:
   - Calculate billing dates from issue date using payment frequency
   - Generate output for each payment until expiry
   - Track remaining balance after each payment
   - Output final balance at expiry or when REMMTH > 12

### NPL Classification

If days since last billing (BLDATE) > 89 days:
- Also generate a '93' prefix record with REMMTH = 13
- This creates NPL classification records

## Input Files

### REPTDATE.parquet
Contains the report date field:
- **REPTDATE**: Report date

### BTRAD{MM}{W}.parquet
Loan transaction data where:
- **MM**: Month (01-12)
- **W**: Week (1-4)

**Required Columns**:
| Column | Type | Description |
|--------|------|-------------|
| PRODCD | String | Product code |
| PRODUCT | Integer | Product number |
| CUSTCD | String | Customer code |
| BALANCE | Numeric | Outstanding balance |
| PAYAMT | Numeric | Payment amount |
| BLDATE | Date | Last billing date |
| ISSDTE | Date | Issue date |
| EXPRDATE | Date | Expiry date |

## Output Files

### BT.parquet
Aggregated output with columns:
- **BNMCODE**: Classification code (format: {PREFIX}{ITEM}{CUST}{REM}0000Y)
- **AMOUNT**: Aggregated amount

### BT_REPORT.txt
Text report with ASA carriage control containing:
- Summary of BNMCODE and AMOUNT
- Total AMOUNT at bottom

**Format**:
```
 ================================================================================
 LOAN MATURITY PROFILE REPORT
 ================================================================================
 
 BNMCODE                          AMOUNT
 --------------------------------------------------------------------------------
 932110801000Y                 1,234,567.89
 952140802000Y                   987,654.32
 ...
 --------------------------------------------------------------------------------
 TOTAL                        12,345,678.90
 ================================================================================
```

## Processing Steps

### 1. Read REPTDATE
- Extract report date
- Calculate week number based on day of month

### 2. Filter Loans
- Select records where PRODCD starts with '34' OR PRODUCT in (225, 226)

### 3. Process Each Loan
For each loan record:

a. **Classify Customer and Product**
   - Determine CUST (08 or 09) based on CUSTCD
   - Classify product type (HL, RC, FL)
   - Determine ITEM code

b. **Calculate Days Since Billing**
   - DAYS = REPTDATE - BLDATE

c. **Check Expiry Status**
   - If expiry within 8 days: Output balance with REMMTH = 0.1
   - Otherwise: Process repayment schedule

d. **Process Repayment Schedule** (if applicable)
   - Set initial billing date
   - For RC products, use expiry date
   - For other loans, calculate from issue date
   - Loop through billing dates until expiry:
     - Calculate remaining months to maturity
     - Output payment amount
     - If REMMTH > 12 or at expiry, output final balance and break
     - Calculate next billing date
     - Reduce balance by payment amount

e. **Generate Output Records**
   - Create '95' prefix record (regular)
   - If DAYS > 89, create '93' prefix record (NPL)

### 4. Aggregate by BNMCODE
- Sum AMOUNT for each unique BNMCODE

### 5. Generate Outputs
- Write aggregated data to parquet
- Generate text report with totals

## Date Calculations

### Next Billing Date (NXTBLDT Macro)

**For Biweekly (PAYFREQ='6')**:
```
Next date = Current BLDATE + 14 days
```

**For Other Frequencies**:
```
Day = ISSDTE day
Month = BLDATE month + FREQ
Year = BLDATE year
Adjust for month overflow (> 12)
Adjust day if exceeds days in month
```

### Remaining Months (REMMTH Macro)

```
REMY = Maturity year - Report year
REMM = Maturity month - Report month
REMD = Maturity day - Report day
REMMTH = REMY * 12 + REMM + REMD / Days_in_report_month
```

## Example Processing Flow

**Loan Record**:
- BALANCE: 120,000
- PAYAMT: 20,000
- ISSDTE: 2023-01-15
- BLDATE: 2024-06-15
- EXPRDATE: 2025-01-15
- REPTDATE: 2024-08-15
- PAYFREQ: '3' (semi-annual)
- CUSTCD: '09'
- PRODUCT: 211 (FL)

**Processing**:
1. ITEM = '211' (FL, non-special customer)
2. CUST = '09'
3. DAYS = 61 (not > 89, so no NPL record)
4. Initial BLDATE = 2024-12-15 (next payment after reptdate)
5. Output records:
   - BLDATE 2024-12-15: REMMTH = 4 months → '952110904000Y', AMOUNT = 20,000
   - Final balance at 2025-01-15: REMMTH = 5 months → '952110905000Y', AMOUNT = 100,000

## Dependencies

```
polars>=0.20.0
python-dateutil>=2.8.0
```

## Installation

```bash
pip install polars python-dateutil
```

## Configuration

Edit the path configuration at the top of `loan_maturity_profile.py`:

```python
INPUT_DIR = Path("/path/to/input")
OUTPUT_DIR = Path("/path/to/output")
```

## Usage

```bash
python loan_maturity_profile.py
```

## Error Handling

The script includes comprehensive error handling:
- File not found errors
- Date conversion errors
- Invalid data handling
- General exceptions with full traceback

## Performance Considerations

- **Efficient iteration**: Processes loans one at a time to handle complex repayment logic
- **Memory efficient**: Collects output records in list before creating dataframe
- **Single aggregation**: One group-by operation at the end
- **Minimal I/O**: Single read and write operations

## Notes

- Week number is determined by specific days: 8, 15, 22, or other
- Default payment frequency is '3' (semi-annual) if not specified
- Revolving credit products (350, 910, 925) use expiry date as billing date
- NPL records (prefix '93') are only generated if DAYS > 89
- For NPL records with REMMTH > 12, REMMTH is set to 13 in the code
- The program assumes SAS date values (days since 1960-01-01)
- Leap years are handled correctly in date calculations
- Days in month adjustments ensure valid dates

## Validation Points

To verify correct conversion:

1. Check week number calculation matches SAS logic
2. Verify product classification (HL, RC, FL)
3. Confirm ITEM codes match customer type and product
4. Validate maturity bucket assignments (01-06)
5. Ensure billing date calculations match payment frequency
6. Verify remaining months calculation
7. Check NPL records only generated when DAYS > 89
8. Confirm aggregation totals match expected values
9. Verify output BNMCODE format: {PREFIX}{ITEM}{CUST}{REM}0000Y

## Differences from SAS

1. **Date handling**: Python uses datetime objects instead of SAS date values
2. **Macros**: Python uses functions instead of SAS macros
3. **Arrays**: Python uses variables and calculations instead of SAS arrays
4. **Looping**: Python uses while loops with explicit conditions
5. **Output**: Generates both parquet (for data) and text (for report) outputs
