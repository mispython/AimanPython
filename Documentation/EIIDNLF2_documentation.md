# BNM GL Consolidation - Documentation

## Overview
This program processes General Ledger data from the Walker file to extract cash holdings and Statutory Reserve Requirement (SRR) data, then consolidates all BNM liquidity framework data into final reporting datasets.

## Program Purpose
The program:
1. **Reads Walker GL file** with fixed-width format
2. **Extracts Cash Holdings** (GL code 39110)
3. **Extracts SRR data** (GL code 32110)
4. **Consolidates** with loan data (BNM.NOTE) and OD calculations (BNM.CALC)
5. **Summarizes** final BNM codes for regulatory reporting

## Input Files

### 1. LOAN.REPTDATE (Parquet)
- **Purpose**: Reporting date and week determination
- **Columns**: 
  - REPTDATE: Reporting date
- **Usage**: Calculate week number and report period

### 2. WALK File (Fixed-Width Text)
- **Purpose**: General Ledger data extract
- **Format**: Fixed-width text file
- **Structure**:
  ```
  Position  Length  Field     Type
  2-20      19      DESC      Character (GL code)
  21-22     2       DD        Numeric (day)
  24-25     2       MM        Numeric (month)
  27-28     2       YY        Numeric (2-digit year)
  42+       varies  AMOUNT    Numeric with commas
  ```
- **Key GL Codes**:
  - **39110**: Cash Holdings
  - **32110**: Statutory Reserve Requirement (SRR)

### 3. BNM.NOTE (Parquet)
- **Purpose**: Loan liquidity data from previous processing
- **Columns**: 
  - BNMCODE: 14-character BNM code
  - AMOUNT: Balance amount

### 4. BNM.CALC (Parquet)
- **Purpose**: Overdraft calculations from previous processing
- **Columns**: 
  - BNMCODE: 14-character BNM code
  - AMOUNT: Calculated amount

## Output Files

### Data Files (Parquet)

#### 1. bnm_glset.parquet
- **Purpose**: Combined GL-based liquidity data
- **Contents**: Cash Holdings + SRR
- **Columns**: BNMCODE, AMOUNT

#### 2. bnm_final.parquet
- **Purpose**: Consolidated all BNM data before summarizing
- **Contents**: BNM.NOTE + BNM.CALC + BNM.GLSET
- **Columns**: BNMCODE, AMOUNT
- **Note**: May contain duplicate BNMCODEs

#### 3. bnm_finalsum.parquet
- **Purpose**: Final summarized BNM data
- **Contents**: Grouped by BNMCODE with summed amounts
- **Columns**: BNMCODE, AMOUNT
- **Usage**: Primary output for regulatory reporting

### Report Files (Text with ASA)

#### 1. cash_holdings_report.txt
- **Purpose**: Cash holdings breakdown
- **Format**: ASA carriage control
- **Contents**: All BNMCODE entries for cash (93221xxx, 95221xxx)

#### 2. srr_report.txt
- **Purpose**: SRR breakdown
- **Format**: ASA carriage control
- **Contents**: All BNMCODE entries for SRR (93222xxx, 95222xxx)

## Business Logic

### Week Number Calculation

```
IF DAY(REPTDATE) = 8:
    WK = '1'
ELSE IF DAY(REPTDATE) = 15:
    WK = '2'
ELSE IF DAY(REPTDATE) = 22:
    WK = '3'
ELSE:
    WK = '0'

# Check if month-end
SREPTDATE = First day of next month
PRVRPTDATE = Last day of current month (SREPTDATE - 1 day)

IF REPTDATE = PRVRPTDATE:
    WK = '4'
```

**Examples**:
- Jan 8, 2025 → WK = '1'
- Jan 15, 2025 → WK = '2'
- Jan 22, 2025 → WK = '3'
- Jan 31, 2025 → WK = '4' (month-end)
- Jan 10, 2025 → WK = '0' (other days)

### Walker File Parsing

**Field Extraction**:
```
DESC:   Position 2-20   (19 characters, trimmed)
DD:     Position 21-22  (2 digits)
MM:     Position 24-25  (2 digits)
YY:     Position 27-28  (2 digits)
AMOUNT: Position 42+    (comma-formatted number)
```

**Year Conversion**:
- YY >= 50 → 1900 + YY (e.g., 95 → 1995)
- YY < 50 → 2000 + YY (e.g., 25 → 2025)

**Date Filter**:
- Only records where REPTDATE matches current report date (RDAT1)

**Example Line**:
```
 39110              15 01 25                    1,234,567.89
 ↑                  ↑  ↑  ↑                     ↑
 DESC (pos 2-20)    DD MM YY                    AMOUNT (pos 42+)
```

### Cash Holdings Processing

**Source**: Walker file, DESC = '39110'

**Logic**:
1. Filter Walker records for DESC = '39110'
2. Sum AMOUNT for current REPTDATE
3. Create BNM codes:
   - **9322100010000Y**: Non-performing, bucket 01 (up to 1 week)
   - **9522100010000Y**: Performing, bucket 01 (up to 1 week)

**Rationale**: Cash is highly liquid and available immediately (bucket 01)

**Default Records**: Create zero-amount records for all 12 codes:
- 93221000{1-6}0000Y (non-performing, buckets 01-06)
- 95221000{1-6}0000Y (performing, buckets 01-06)

**Merge Logic**: Actual amounts override zero defaults where present

### SRR Processing

**Source**: Walker file, DESC = '32110'

**Logic**:
1. Filter Walker records for DESC = '32110'
2. Sum AMOUNT for current REPTDATE
3. Create BNM codes:
   - **9322200060000Y**: Non-performing, bucket 06 (>1 year)
   - **9522200060000Y**: Performing, bucket 06 (>1 year)

**Rationale**: SRR is locked at central bank, considered stable long-term (bucket 06)

**Default Records**: Create zero-amount records for all 12 codes:
- 93222000{1-6}0000Y (non-performing, buckets 01-06)
- 95222000{1-6}0000Y (performing, buckets 01-06)

**Merge Logic**: Actual amounts override zero defaults where present

### BNM Code Structure - GL Items

#### Cash Holdings: 93/95 221 00 XX 0000Y
- **93/95**: Performance category (93=non-performing, 95=performing)
- **221**: Item code (Cash holdings)
- **00**: Customer category (not applicable for cash)
- **XX**: Time bucket (01-06)
- **0000Y**: Filler and year indicator

#### SRR: 93/95 222 00 XX 0000Y
- **93/95**: Performance category
- **222**: Item code (SRR)
- **00**: Customer category (not applicable for SRR)
- **XX**: Time bucket (01-06)
- **0000Y**: Filler and year indicator

### Consolidation Process

**Step 1: Create GLSET**
```
GLSET = Cash Holdings + SRR
```

**Step 2: Create FINAL**
```
FINAL = BNM.NOTE + BNM.CALC + GLSET
```

**Components**:
- **BNM.NOTE**: Loan liquidity data
  - FL, HL, RC, OD items
  - Multiple BNMCODEs per product/customer/bucket
  
- **BNM.CALC**: Overdraft calculations
  - ODCORP and ODIND
  - 48-week methodology results
  
- **GLSET**: GL-based items
  - Cash holdings
  - SRR

**Step 3: Summarize to FINALSUM**
```sql
SELECT BNMCODE, SUM(AMOUNT) as AMOUNT
FROM FINAL
GROUP BY BNMCODE
ORDER BY BNMCODE
```

**Purpose**: Combine duplicate BNMCODEs from different sources

## Data Flow

```
LOAN.REPTDATE → Calculate WK and dates
                        ↓
WALK File → Parse → Filter by REPTDATE → Split by DESC
                                              ↓
                                    ├─→ DESC='39110' → Cash Holdings
                                    │         ↓
                                    │   Create defaults (12 codes)
                                    │         ↓
                                    │   Merge actual with defaults
                                    │
                                    └─→ DESC='32110' → SRR
                                              ↓
                                        Create defaults (12 codes)
                                              ↓
                                        Merge actual with defaults
                                              ↓
                                    Cash + SRR = GLSET
                                              ↓
BNM.NOTE ──┐
           ├─→ Concatenate → FINAL → Group by BNMCODE → FINALSUM
BNM.CALC ──┤                              ↓
GLSET ─────┘                         Save outputs
                                          ↓
                                    Generate reports
```

## Example Processing

### Example 1: Cash Holdings

**Walker File Extract**:
```
 39110              15 01 25                    5,234,567.89
```

**Processing**:
1. Parse: DESC='39110', Date=Jan 15, 2025, AMOUNT=5,234,567.89
2. Filter: REPTDATE matches RDAT1
3. Sum: Total = 5,234,567.89
4. Create codes:
   - 9322100010000Y: 5,234,567.89
   - 9522100010000Y: 5,234,567.89

**Default Merge**:
| BNMCODE | Default | Actual | Final |
|---------|---------|--------|-------|
| 9322100010000Y | 0 | 5,234,567.89 | 5,234,567.89 |
| 9322100020000Y | 0 | - | 0 |
| ... | 0 | - | 0 |
| 9522100010000Y | 0 | 5,234,567.89 | 5,234,567.89 |
| 9522100020000Y | 0 | - | 0 |
| ... | 0 | - | 0 |

### Example 2: SRR

**Walker File Extract**:
```
 32110              15 01 25                    2,150,000.00
```

**Processing**:
1. Parse: DESC='32110', Date=Jan 15, 2025, AMOUNT=2,150,000.00
2. Filter: REPTDATE matches RDAT1
3. Sum: Total = 2,150,000.00
4. Create codes:
   - 9322200060000Y: 2,150,000.00
   - 9522200060000Y: 2,150,000.00

**Default Merge**:
| BNMCODE | Default | Actual | Final |
|---------|---------|--------|-------|
| 9322200010000Y | 0 | - | 0 |
| ... | 0 | - | 0 |
| 9322200060000Y | 0 | 2,150,000.00 | 2,150,000.00 |
| 9522200010000Y | 0 | - | 0 |
| ... | 0 | - | 0 |
| 9522200060000Y | 0 | 2,150,000.00 | 2,150,000.00 |

### Example 3: Consolidation

**Input Data**:
- BNM.NOTE: 500 records (loans)
- BNM.CALC: 12 records (OD)
- GLSET: 24 records (cash + SRR)

**FINAL**:
- Total records: 536 (500 + 12 + 24)
- May have duplicate BNMCODEs

**Example duplicates**:
```
BNMCODE          AMOUNT       Source
95211090100000Y  1,000,000    BNM.NOTE
95211090100000Y    500,000    BNM.NOTE (different loan)
```

**FINALSUM**:
```
BNMCODE          AMOUNT       (Sum of duplicates)
95211090100000Y  1,500,000
```

## Report Format

### Cash Holdings Report
```
1                    CASH HOLDINGS                    
                        15/01/25                        

BNMCODE              AMOUNT
----------------------------------------------------------------------------------------------------
 9322100010000Y        5,234,567.89
 9322100020000Y                0.00
 9322100030000Y                0.00
 9322100040000Y                0.00
 9322100050000Y                0.00
 9322100060000Y                0.00
 9522100010000Y        5,234,567.89
 9522100020000Y                0.00
 9522100030000Y                0.00
 9522100040000Y                0.00
 9522100050000Y                0.00
 9522100060000Y                0.00
----------------------------------------------------------------------------------------------------
 TOTAL                10,469,135.78
```

### SRR Report
```
1                         SRR                           
                        15/01/25                        

BNMCODE              AMOUNT
----------------------------------------------------------------------------------------------------
 9322200010000Y                0.00
 9322200020000Y                0.00
 9322200030000Y                0.00
 9322200040000Y                0.00
 9322200050000Y                0.00
 9322200060000Y        2,150,000.00
 9522200010000Y                0.00
 9522200020000Y                0.00
 9522200030000Y                0.00
 9522200040000Y                0.00
 9522200050000Y                0.00
 9522200060000Y        2,150,000.00
----------------------------------------------------------------------------------------------------
 TOTAL                 4,300,000.00
```

## Technical Implementation

### Walker File Parsing

**Fixed-Width Reading**:
```python
with open(WALK_FILE_PATH, 'r') as f:
    for line in f:
        desc = line[1:20].strip()      # Position 2-20 (1-indexed)
        dd = int(line[20:22].strip())   # Position 21-22
        mm = int(line[23:25].strip())   # Position 24-25
        yy = int(line[26:28].strip())   # Position 27-28
        amount_str = line[41:].strip()  # Position 42+
        amount = float(amount_str.replace(',', ''))
```

**Date Conversion**:
```python
# 2-digit year to 4-digit
if yy >= 50:
    year = 1900 + yy
else:
    year = 2000 + yy

reptdate = date(year, mm, dd)
```

### Default Records Strategy

**Purpose**: Ensure complete reporting structure even with missing data

**Implementation**:
```python
# Create all possible codes
default_records = []
for n in range(1, 7):
    default_records.append({'BNMCODE': f'93221000{n}0000Y', 'AMOUNT': 0.0})
    default_records.append({'BNMCODE': f'95221000{n}0000Y', 'AMOUNT': 0.0})

# Merge with actuals (actuals override defaults)
result = defaults.join(actuals, on='BNMCODE', how='left')
```

### RDAT1 Format

**Purpose**: Numeric date for comparison

**Format**: YYMDD (variable length)
- Jan 1-9: YYMMD (5 digits) - e.g., 25011
- Jan 10-31: YYMMDD (6 digits) - e.g., 250115
- Oct 1-9: YYMMD (5 digits) - e.g., 25101
- Oct 10-31: YYMMDD (6 digits) - e.g., 251015

**Implementation**:
```python
rdat1 = int(reptdate.strftime('%y%m%d'))
# Jan 15, 2025 → 250115
# Jan 1, 2025 → 25011 (NOT 250101 due to leading zero removal)
```

## Performance Considerations

1. **Walker File**: Read line-by-line to handle large files
2. **Date Filtering**: Filter early to reduce memory usage
3. **Consolidation**: Use Polars concat for efficient merging
4. **Grouping**: Final summarization uses Polars group_by

## Error Handling

The code handles:
- Missing or malformed Walker file lines
- Invalid dates in Walker file
- Missing GL codes (39110, 32110) - creates zero records
- Empty input files (BNM.NOTE, BNM.CALC)

## Testing Checklist

### Date Processing
- [ ] Week number calculated correctly (8→1, 15→2, 22→3, month-end→4, other→0)
- [ ] SREPTDATE = first day of next month
- [ ] PRVRPTDATE = last day of current month
- [ ] Month-end detection works

### Walker File
- [ ] Fixed-width positions parsed correctly
- [ ] DESC extracted (position 2-20)
- [ ] Date fields parsed (DD, MM, YY)
- [ ] 2-digit year converted to 4-digit
- [ ] AMOUNT parsed with comma removal
- [ ] Filtered to REPTDATE = RDAT1

### Cash Holdings
- [ ] DESC='39110' filtered correctly
- [ ] Amount summed for date
- [ ] Bucket 01 codes created
- [ ] Default records created for all buckets
- [ ] Actual overrides defaults

### SRR
- [ ] DESC='32110' filtered correctly
- [ ] Amount summed for date
- [ ] Bucket 06 codes created
- [ ] Default records created for all buckets
- [ ] Actual overrides defaults

### Consolidation
- [ ] GLSET = Cash + SRR
- [ ] FINAL = NOTE + CALC + GLSET
- [ ] FINALSUM groups by BNMCODE
- [ ] Amounts summed correctly
- [ ] No records lost in merge

### Output Files
- [ ] All parquet files created
- [ ] Reports generated with ASA
- [ ] Totals match

## Configuration

Update paths in script:
```python
LOAN_REPTDATE_PATH = "/data/input/loan_reptdate.parquet"
WALK_FILE_PATH = "/data/input/walk.txt"
BNM_NOTE_PATH = "/data/input/bnm_note.parquet"
BNM_CALC_PATH = "/data/input/bnm_calc.parquet"
OUTPUT_DIR = "/data/output"
```

## Dependencies

```
polars>=0.19.0
pyarrow>=14.0.0
```

## Execution

```bash
python3 bnm_gl_consolidation.py
```

Expected output showing record counts and totals for each component.
