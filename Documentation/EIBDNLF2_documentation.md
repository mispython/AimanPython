# EIBDNLF2 - SAS to Python Conversion: GL Items and Final Consolidation

## Overview

This Python script converts a SAS program that processes General Ledger (GL) items from the WALK file and consolidates all previously generated BNM datasets into a final comprehensive report. This is the final step in the BNM reporting pipeline.

## File Structure

- **gl_consolidation.py**: Main Python script for GL processing and consolidation

## SAS Program Logic

The original SAS program performs the following operations:

1. **REPTDATE Processing**: Determines report date and week number (including month-end detection)
2. **WALK File Reading**: Reads GL balances from fixed-width WALK file
3. **GL Item Processing**: Processes 8 different GL item types:
   - Floor Stocking (FS)
   - Hire Purchase Agency (HP)
   - Cash Holdings (CASH)
   - Statutory Reserve Requirement (SRR)
   - Other Assets/Shares Held (OTH)
   - FCY Fixed Loan (FCYFL)
   - FCY Revolving Credit (FCYRC)
   - FCY Cash Holdings (FCYCASH)
4. **Default Records**: Creates zero-amount records for all maturity buckets
5. **GLSET Creation**: Combines all GL items
6. **Final Consolidation**: Merges NOTE, CALC, PBIF, BT, and GLSET
7. **Final Aggregation**: Sums amounts by BNMCODE

## Week Number Logic

This program uses a slightly different week calculation:

```
Day 8:        Week 1
Day 15:       Week 2
Day 22:       Week 3
Month-end:    Week 4
Other days:   Week 0
```

**Month-end detection**:
- Calculate first day of next month
- Subtract 1 day to get last day of current month
- If REPTDATE equals last day of month, set week to 4

## GL Item Details

### 1. Floor Stocking (FS)

**Source**: DESC = 'ML34170'

**Output Codes**:
- NPL: `9321909{BUCKET}0000Y`
- Regular: `9521909{BUCKET}0000Y`
- Specific assignment: Bucket 05 (6-12 months)

**Default records**: Created for buckets 01-06

### 2. Hire Purchase Agency (HP)

**Source**: DESC = 'ML34111'

**Output Codes**:
- NPL: `9321109{BUCKET}0000Y`
- Regular: `9521109{BUCKET}0000Y`
- Specific assignment: Bucket 06 (> 1 year)

**Default records**: Created for buckets 01-06

### 3. Cash Holdings (CASH)

**Source**: DESC = '39110'

**Output Codes**:
- NPL: `932210001` + `0000Y`
- Regular: `952210001` + `0000Y`
- Specific assignment: Bucket 01 (up to 1 week)

**Default records**: Not created (specific bucket only)

### 4. Statutory Reserve Requirement (SRR)

**Source**: DESC = '32110'

**Output Codes**:
- NPL: `9322200{BUCKET}0000Y`
- Regular: `9522200{BUCKET}0000Y`
- Specific assignment: Bucket 06 (> 1 year)

**Default records**: Created for buckets 01-06

### 5. Other Assets (Shares Held)

**Source**: DESC = 'F137010SH'

**Output Codes**:
- NPL: `932290001` + `0000Y`
- Regular: `952290001` + `0000Y`
- Specific assignment: Bucket 01

**Default records**: Not created (specific bucket only)

### 6. FCY - Fixed Loan (FCYFL)

**Source**: DESC IN ('F13460081BCB', 'F13460064FLB')

**Output Codes**:
- NPL: `9421109{BUCKET}0000Y`
- Regular: `9621109{BUCKET}0000Y`
- Specific assignment: Bucket 06 (> 1 year)

**Default records**: Created for buckets 01-06

**Note**: Uses prefix 94/96 (FCY) instead of 93/95 (RM)

### 7. FCY - Revolving Credit (FCYRC)

**Source**: DESC = 'F134600RC'

**Output Codes**:
- NPL: `9421209{BUCKET}0000Y`
- Regular: `9621209{BUCKET}0000Y`
- Specific assignment: Bucket 06 (> 1 year)

**Default records**: Created for buckets 01-06

### 8. FCY - Cash Holdings (FCYCASH)

**Source**: DESC = 'F139610FXNC'

**Output Codes**:
- NPL: `9422100{BUCKET}0000Y`
- Regular: `9622100{BUCKET}0000Y`
- Specific assignment: Bucket 01

**Default records**: Created for buckets 01-06

## Input Files

### REPTDATE.parquet
Report date information:
- **REPTDATE**: Report date

### WALK.txt
Fixed-width GL walk file:
- Columns 2-20: DESC (GL description/code)
- Columns 21-22: DD (day, 2 digits)
- Columns 24-25: MM (month, 2 digits)
- Columns 27-28: YY (year, 2 digits)
- Columns 42-61: AMOUNT (formatted with commas)

**Example**:
```
 ML34170          010824    1,234,567.89
 39110            010824      987,654.32
```

### NOTE.parquet (Optional)
From previous liquidity run-off processing:
- **BNMCODE**: Classification code
- **AMOUNT**: Amount

### CALC.parquet (Optional)
From behavioral run-off processing:
- **BNMCODE**: Classification code
- **AMOUNT**: Amount

### PBIF.parquet (Optional)
From PBIF processing (not shown in previous programs):
- **BNMCODE**: Classification code
- **AMOUNT**: Amount

### BT.parquet (Optional)
From BT loan processing:
- **BNMCODE**: Classification code
- **AMOUNT**: Amount

## Output Files

### GLSET.parquet
Combined GL items:
- **BNMCODE**: Classification code
- **AMOUNT**: Amount from WALK file

### FINAL.parquet
Consolidated all datasets (unsummed):
- **BNMCODE**: Classification code
- **AMOUNT**: Amount (may have duplicates)

### FINALSUM.parquet
Final aggregated dataset:
- **BNMCODE**: Classification code (unique)
- **AMOUNT**: Sum of all amounts for each BNMCODE

### Individual Reports (with ASA carriage control)
- **FS_REPORT.txt**: Floor Stocking
- **HP_REPORT.txt**: Hire Purchase
- **CASH_REPORT.txt**: Cash Holdings
- **SRR_REPORT.txt**: Statutory Reserve
- **OTH_REPORT.txt**: Other Assets
- **FCYFL_REPORT.txt**: FCY Fixed Loan
- **FCYRC_REPORT.txt**: FCY Revolving Credit
- **FCYCASH_REPORT.txt**: FCY Cash Holdings
- **GLSET_REPORT.txt**: All GL items combined
- **FINAL_REPORT.txt**: Final consolidated summary

## Processing Steps

### 1. Read REPTDATE
- Extract report date
- Calculate week number with month-end detection
- Calculate RDAT1 (YYMMDD numeric format)

### 2. Read WALK File
- Parse fixed-width format
- Filter for records matching RDAT1
- Extract DESC and AMOUNT

### 3. Process Each GL Item
For each of the 8 GL item types:

a. **Create Defaults** (if applicable)
   - Generate zero-amount records for buckets 01-06
   - Both NPL (93/94) and Regular (95/96) prefixes

b. **Filter WALK Data**
   - Select records with matching DESC
   - Sum amounts by REPTDATE

c. **Create Output Records**
   - Assign to specific bucket
   - Create both NPL and Regular records

d. **Merge with Defaults**
   - Left join defaults with actual data
   - Coalesce to prefer actual data over zeros

e. **Generate Report**
   - Individual report for each GL item

### 4. Create GLSET
- Combine all 8 GL item datasets
- Write to GLSET.parquet
- Generate combined GLSET report

### 5. Read Other Datasets
- NOTE (if exists)
- CALC (if exists)
- PBIF (if exists)
- BT (if exists)
- GLSET (always)

### 6. Consolidate FINAL
- Concatenate all datasets
- Sort by BNMCODE
- Write to FINAL.parquet

### 7. Aggregate FINALSUM
- Group by BNMCODE
- Sum AMOUNT
- Sort by BNMCODE
- Write to FINALSUM.parquet
- Generate final summary report

## BNMCODE Patterns

### RM (Ringgit Malaysia) Items
- **93xxxx**: NPL/behavioral items
- **95xxxx**: Regular items

### FCY (Foreign Currency) Items
- **94xxxx**: NPL/behavioral items
- **96xxxx**: Regular items

### Detailed Breakdown

| Prefix | Item | Cust | Description |
|--------|------|------|-------------|
| 9321909 | 219 | 09 | Floor Stocking (NPL) |
| 9521909 | 219 | 09 | Floor Stocking (Regular) |
| 9321109 | 211 | 09 | Hire Purchase (NPL) |
| 9521109 | 211 | 09 | Hire Purchase (Regular) |
| 9322100 | 210 | 00 | Cash Holdings (NPL) |
| 9522100 | 210 | 00 | Cash Holdings (Regular) |
| 9322200 | 220 | 00 | SRR (NPL) |
| 9522200 | 220 | 00 | SRR (Regular) |
| 9322900 | 290 | 00 | Other Assets (NPL) |
| 9522900 | 290 | 00 | Other Assets (Regular) |
| 9421109 | 211 | 09 | FCY FL (NPL) |
| 9621109 | 211 | 09 | FCY FL (Regular) |
| 9421209 | 212 | 09 | FCY RC (NPL) |
| 9621209 | 212 | 09 | FCY RC (Regular) |
| 9422100 | 210 | 00 | FCY Cash (NPL) |
| 9622100 | 210 | 00 | FCY Cash (Regular) |

## Consolidation Pipeline

This program is the final step in a complete BNM reporting pipeline:

```
Step 1: loan_maturity_profile.py  → Produces contractual loan schedules
Step 2: liquidity_runoff.py       → Produces NOTE dataset
Step 3: behavioral_runoff.py      → Produces CALC dataset
Step 4: (Other programs)           → Produce PBIF, BT datasets
Step 5: gl_consolidation.py       → Produces GLSET, FINAL, FINALSUM
```

## Example Processing Flow

**WALK File Extract**:
```
 ML34170          150824    5,000,000.00
 39110            150824    2,000,000.00
 32110            150824    10,000,000.00
```

**Processing**:
1. FS: Amount 5,000,000 → buckets 01-06 (defaults) + bucket 05 (actual)
2. CASH: Amount 2,000,000 → bucket 01 only
3. SRR: Amount 10,000,000 → buckets 01-06 (defaults) + bucket 06 (actual)

**GLSET Output**:
- 12 FS records (6 NPL + 6 Regular)
- 2 CASH records (1 NPL + 1 Regular)
- 12 SRR records (6 NPL + 6 Regular)
- Total: 26 records

## Dependencies

```
polars>=0.20.0
```

## Installation

```bash
pip install polars
```

## Configuration

Edit the path configuration at the top of `gl_consolidation.py`:

```python
INPUT_DIR = Path("/path/to/input")
OUTPUT_DIR = Path("/path/to/output")
```

## Usage

```bash
python gl_consolidation.py
```

## Special Cases

### 1. Month-End Week Detection
Uses next month calculation to determine last day:
```python
next_month_first_day = MDY(month+1, 1, year)
month_end = next_month_first_day - 1 day
if reptdate == month_end: week = 4
```

### 2. Default Record Creation
Some GL items create defaults, others don't:
- **With defaults**: FS, HP, SRR, FCYFL, FCYRC, FCYCASH
- **Without defaults**: CASH, OTH

### 3. FCY Prefix Handling
Foreign currency items use 94/96 instead of 93/95

### 4. Specific Bucket Assignments
Most items assign to one specific bucket:
- FS → 05 (6-12 months)
- HP → 06 (> 1 year)
- CASH → 01 (up to 1 week)
- SRR → 06 (> 1 year)
- OTH → 01 (up to 1 week)
- FCYFL → 06 (> 1 year)
- FCYRC → 06 (> 1 year)
- FCYCASH → 01 (up to 1 week)

### 5. Optional Dataset Handling
Program continues if NOTE, CALC, PBIF, or BT don't exist

## Validation Points

1. Verify month-end week detection logic
2. Check WALK file parsing (fixed-width columns)
3. Confirm DESC matching for each GL item
4. Validate default record creation
5. Verify bucket assignments for each GL item
6. Check NPL/Regular pair creation
7. Confirm FCY prefix (94/96) vs RM prefix (93/95)
8. Validate GLSET contains all 8 GL item types
9. Verify FINAL includes all input datasets
10. Confirm FINALSUM aggregation by BNMCODE

## Differences from SAS

1. **File handling**: Reads optional datasets with existence checks
2. **Default merging**: Uses polars join with coalesce
3. **Report generation**: Custom ASA carriage control formatting
4. **Error handling**: Continues if optional files missing
5. **Summary statistics**: Added detailed summary output

## Notes

- This is the final consolidation step in BNM reporting
- GLSET contains only GL items from WALK file
- FINAL contains all datasets (may have duplicate BNMCODEs)
- FINALSUM is the ultimate aggregated output
- Week 0 indicates non-reporting day
- Week 4 is specifically for month-end
- All GL items create both NPL (93/94) and Regular (95/96) records
- Default records ensure complete maturity profile even with zero amounts
- FCY items use different prefix but same structure as RM items
- The program generates 10 separate reports plus summary statistics
