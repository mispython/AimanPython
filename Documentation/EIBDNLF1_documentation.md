# EIBDNLF1 - SAS to Python Conversion: Behavioral Run-Offs Calculator

## Overview

This Python script converts a SAS program that calculates behavioral run-offs for various product types using a 48-week rolling minimum methodology. The program processes credit cards (PBCARD), overdrafts (OD), and small/medium credits (SMC) for both corporate and individual customers.

## File Structure

- **behavioral_runoff.py**: Main Python script that replicates the SAS program logic

## SAS Program Logic

The original SAS program performs the following operations:

1. **REPTDATE Processing**: 
   - Determines report date and week number
   - Calculates INSERT flag based on report day
2. **48-Week Methodology**:
   - Maintains historical data for 48 weeks
   - Calculates CURRENT balance (most recent week)
   - Calculates MINIMUM balance (lowest in 48 weeks)
   - Distributes (CURRENT - MINIMUM) / 5 across maturity buckets < 12 months
   - Assigns MINIMUM to bucket ≥ 12 months
3. **Product Processing**:
   - PBCARD: Credit card balances from WALK file
   - ODCORP/ODIND: Overdraft balances from NOTE file
   - SMCCORP/SMCIND: SMC balances from LOAN file
4. **Base Management**:
   - Updates BASE files on insert days (8th, 15th, 22nd, month-end)
   - Creates STORE files (all records ≤ current date)
5. **Output Generation**:
   - Creates BNMCODE records with behavioral run-off amounts
   - Generates tabulation reports

## Key Concepts

### 48-Week Rolling Methodology

This is a **behavioral modeling** approach (vs contractual):

**Purpose**: Estimate how much of the balance will remain (not run off) based on historical behavior

**Calculation**:
```
CURRENT = Most recent week's balance
MINIMUM = Lowest balance in past 48 weeks
STABLE = MINIMUM (assumed to stay long-term)
VOLATILE = CURRENT - MINIMUM (may run off)

For buckets < 12 months: AMOUNT = VOLATILE / 5
For bucket ≥ 12 months:  AMOUNT = STABLE
```

**Example**:
- Current balance: 1,000,000
- Minimum (48 weeks): 600,000
- Stable portion: 600,000
- Volatile portion: 400,000

Distribution:
- Each bucket < 12 months: 400,000 / 5 = 80,000
- Bucket ≥ 12 months: 600,000

### INSERT Flag Logic

Determines when to update the BASE file:

**INSERT = 'Y' when**:
- REPTDAY = '08', '15', or '22' (weekly reports)
- REPTDAY = last day of month AND today's day < 8 (month-end report)

**When INSERT = 'Y'**:
- Delete existing record with same REPTDATE from BASE
- Append new record to BASE
- Create STORE from BASE (all records ≤ REPTDATE)

**When INSERT = 'N'**:
- BASE remains unchanged
- STORE = BASE + new record (temporary)

### RDAT1 Format

Report date in YYMMDD numeric format:
- Example: 2024-01-15 → 240115

## Product Details

### PBCARD (Credit Cards)

**Source**: WALK file, DESC='34200'

**Output**:
- **PBCARD**: NPL records (prefix 93) with behavioral distribution
- **PBCARD2**: Regular records (prefix 95) with basket distribution

**Basket Distribution** (PBCARD2):
| Basket | Percentage | Description |
|--------|-----------|-------------|
| 1 | 15% | Bucket assignment per table |
| 2 | 69% | Bucket assignment per table |
| 3 | 1% | Bucket assignment per table |
| 4 | 3% | Bucket assignment per table |
| 5 | 7% | Bucket assignment per table |
| 6 | 5% | Bucket assignment per table |

**BNMCODE Format**:
- PBCARD: `9321508{BUCKET}0000Y` (NPL)
- PBCARD2: `9521508{BUCKET}0000Y` (Regular)

### ODCORP (Corporate Overdraft)

**Source**: NOTE file, BNMCODE1='9521309'

**Process**:
1. Sum all amounts from NOTE where BNMCODE starts with '9521309'
2. Apply 48-week methodology
3. Output with NPL prefix (93)

**BNMCODE Format**: `9321309{BUCKET}0000Y`

### ODIND (Individual Overdraft)

**Source**: NOTE file, BNMCODE1='9521308'

**Process**:
1. Sum all amounts from NOTE where BNMCODE starts with '9521308'
2. Apply 48-week methodology
3. Output with NPL prefix (93)

**BNMCODE Format**: `9321308{BUCKET}0000Y`

### SMCCORP (Corporate SMC)

**Source**: LOAN file

**Filters**:
- PAIDIND NOT IN ('P', 'C')
- PRODCD starts with '34' OR PRODUCT IN (225, 226)
- ACCTYPE = 'OD'
- PRODUCT NOT IN (151, 152, 181)
- BNMCODE starts with '9521909' (corporate)

**BNMCODE Format**: `9321909{BUCKET}0000Y`

### SMCIND (Individual SMC)

**Source**: LOAN file (same as SMCCORP)

**Filter**: BNMCODE starts with '9521908' (individual)

**BNMCODE Format**: `9321908{BUCKET}0000Y`

## Input Files

### REPTDATE.parquet
Report date information:
- **REPTDATE**: Report date

### WALK.txt
Fixed-width walk file with GL balances:
- Columns 2-20: DESC (description)
- Columns 21-22: DD (day)
- Columns 24-25: MM (month)
- Columns 27-28: YY (year, 2-digit)
- Columns 42-61: AMOUNT (formatted with commas)

### NOTE.parquet
Note file with BNMCODE and amounts:
- **BNMCODE**: Classification code
- **AMOUNT**: Amount

### LOAN{MM}{DD}.parquet
Daily loan file:
- **PAIDIND**: Paid indicator
- **PRODCD**: Product code
- **PRODUCT**: Product number
- **CUSTCD**: Customer code
- **ACCTYPE**: Account type
- **BALANCE**: Balance amount

### TABLE.parquet
Reference table with maturity buckets:
- **REMMTH**: Remaining months
- **BASKET**: Basket number (for PBCARD2)

### BASE_{PROD}.parquet (Persistent)
Historical base files for each product:
- **REPTDATE**: Report date (YYMMDD numeric)
- **AMOUNT**: Amount

### STORE_{PROD}.parquet (Generated)
Filtered store files (all records ≤ current REPTDATE):
- **REPTDATE**: Report date
- **AMOUNT**: Amount

## Output Files

### CALC.parquet
Combined output with all products:
- **BNMCODE**: Classification code
- **AMOUNT**: Calculated behavioral run-off amount

### {PROD}_REPORT.txt
Tabulation reports for each product with ASA carriage control:
- 48-week table summary
- Breakdown by maturity profile
- Total amounts

## Processing Steps

### 1. Read REPTDATE
- Extract report date
- Calculate week number
- Determine INSERT flag
- Calculate RDAT1 (YYMMDD format)

### 2. Process PBCARD
a. Read WALK file, extract DESC='34200' for current date
b. Create new record with REPTDATE and AMOUNT
c. Apply 48-week methodology:
   - Load STORE_CARD (or BASE_CARD + new if INSERT='N')
   - Get 48 most recent weeks
   - Calculate CURBAL (most recent) and MINBAL (minimum)
   - For REMMTH < 12: AMOUNT = (CURBAL - MINBAL) / 5
   - For REMMTH ≥ 12: AMOUNT = MINBAL
d. Create PBCARD output (NPL, prefix 93)
e. Create PBCARD2 output (Regular, prefix 95) with basket %

### 3. Process ODCORP
a. Sum NOTE amounts where BNMCODE1='9521309'
b. Apply 48-week methodology
c. Create output with prefix 93

### 4. Process ODIND
a. Sum NOTE amounts where BNMCODE1='9521308'
b. Apply 48-week methodology
c. Create output with prefix 93

### 5. Process SMCCORP
a. Read LOAN file with filters
b. Sum amounts where BNMCODE starts with '9521909'
c. Apply 48-week methodology
d. Create output with prefix 93

### 6. Process SMCIND
a. Use same LOAN data as SMCCORP
b. Sum amounts where BNMCODE starts with '9521908'
c. Apply 48-week methodology
d. Create output with prefix 93

### 7. Combine and Output
- Combine all product outputs
- Write to CALC.parquet

## Maturity Buckets

### REMFMT (Code Format)
| Remaining Months | Code | Description |
|-----------------|------|-------------|
| ≤ 0.1 | 01 | Up to 1 week |
| 0.1 - 1 | 02 | > 1 week - 1 month |
| 1 - 3 | 03 | > 1 month - 3 months |
| 3 - 6 | 04 | > 3 - 6 months |
| 6 - 12 | 05 | > 6 months - 1 year |
| > 12 | 06 | > 1 year |

### REMFMTB (Label Format)
| Remaining Months | Label |
|-----------------|-------|
| ≤ 0.255 | UP TO 1 WK |
| 0.255 - 1 | >1 WK - 1 MTH |
| 1 - 3 | >1 MTH - 3 MTHS |
| 3 - 6 | >3 - 6 MTHS |
| 6 - 12 | >6 MTHS - 1 YR |
| > 12 | > 1 YEAR |

## Example Calculation

**Scenario**:
- Product: ODCORP
- Current balance: 5,000,000
- 48-week minimum: 3,000,000
- Table has 6 buckets with REMMTH: 0.5, 2, 4, 8, 10, 15

**Calculation**:
```
CURBAL = 5,000,000
MINBAL = 3,000,000
VOLATILE = 5,000,000 - 3,000,000 = 2,000,000

Bucket 1 (REMMTH=0.5 < 12):  2,000,000 / 5 = 400,000
Bucket 2 (REMMTH=2 < 12):    2,000,000 / 5 = 400,000
Bucket 3 (REMMTH=4 < 12):    2,000,000 / 5 = 400,000
Bucket 4 (REMMTH=8 < 12):    2,000,000 / 5 = 400,000
Bucket 5 (REMMTH=10 < 12):   2,000,000 / 5 = 400,000
Bucket 6 (REMMTH=15 ≥ 12):   3,000,000

Total: 5,000,000 ✓
```

## Dependencies

```
polars>=0.20.0
```

## Installation

```bash
pip install polars
```

## Configuration

Edit the path configuration at the top of `behavioral_runoff.py`:

```python
INPUT_DIR = Path("/path/to/input")
OUTPUT_DIR = Path("/path/to/output")
BASE_DIR = Path("/path/to/base")
```

## Usage

```bash
python behavioral_runoff.py
```

## File Persistence

The program maintains persistent BASE files that accumulate historical data:

**BASE_{PROD}.parquet**: Permanent storage
- Updated only when INSERT='Y'
- Accumulates all historical records
- Used to calculate 48-week statistics

**STORE_{PROD}.parquet**: Temporary filtered view
- Always regenerated
- Contains records ≤ current REPTDATE
- Used for 48-week window extraction

## Special Cases

### 1. INSERT Flag
Only updates BASE on specific days (8th, 15th, 22nd, month-end)

### 2. PBCARD Double Output
Creates both NPL (93) and Regular (95) records with different logic

### 3. Basket Distribution
PBCARD2 uses predefined percentages across baskets

### 4. Product Exclusions
SMC excludes products 151, 152, 181

### 5. 48-Week Window
If fewer than 48 weeks available, uses all available data

## Validation Points

1. Verify INSERT flag logic for update days
2. Check 48-week window extraction (most recent 48)
3. Confirm CURBAL = most recent AMOUNT
4. Validate MINBAL = minimum AMOUNT in 48 weeks
5. Verify (CURBAL - MINBAL) / 5 for buckets < 12 months
6. Check MINBAL assigned to buckets ≥ 12 months
7. Confirm RDAT1 format (YYMMDD numeric)
8. Validate PBCARD basket percentages sum to 100%
9. Check product filtering for SMC
10. Verify BNMCODE format for each product

## Differences from SAS

1. **Persistent Storage**: Uses parquet files instead of SAS datasets
2. **Date Format**: RDAT1 stored as integer (YYMMDD)
3. **Macros**: Python functions replace SAS macros
4. **PROC TABULATE**: Custom report generation with ASA control
5. **File Management**: Explicit path handling for BASE/STORE files

## Notes

- The 48-week methodology assumes stable core deposits
- Volatile portion assumed to run off over 5 buckets equally
- INSERT='Y' on 8th, 15th, 22nd ensures weekly updates
- Month-end special handling (INSERT if processed in first week)
- PBCARD has dual output (NPL behavioral + Regular basket)
- All outputs use NPL prefix (93) except PBCARD2 (95)
- BASE files persist across runs for historical tracking
- STORE files regenerated each run for current calculations
