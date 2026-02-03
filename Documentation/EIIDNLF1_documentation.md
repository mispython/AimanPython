# Overdraft Liquidity Calculation - Documentation

## Overview
This program calculates overdraft liquidity metrics for BNM (Bank Negara Malaysia) reporting using a 48-week rolling window methodology to determine stable funding assumptions.

## Program Purpose
The program:
1. **Extracts current overdraft balances** from BNM NOTE data
2. **Maintains 48-week historical data** for trend analysis
3. **Calculates minimum balance** over 48-week period
4. **Spreads volatile portion** across time buckets
5. **Generates BNM codes** for liquidity reporting

## Key Concepts

### 48-Week Methodology
**Purpose**: Determine stable vs volatile portions of overdraft balances

**Logic**:
- Collect last 48 weeks of balance data
- Identify minimum balance over this period (stable portion)
- Calculate difference between current and minimum (volatile portion)
- Spread volatile portion equally across first 5 time buckets
- Assign stable minimum to >1 year bucket

### Product Categories
- **ODCORP**: Overdraft - Corporate (BNMCODE prefix: 9521309 → 9321309)
- **ODIND**: Overdraft - Individual (BNMCODE prefix: 9521308 → 9321308)

## Input Files

### 1. LOAN.REPTDATE (Parquet)
- **Purpose**: Reporting date and week number
- **Columns**:
  - REPTDATE: Reporting date
  - WK: Week number
- **Usage**: Determines reporting period and insertion logic

### 2. BNM.NOTE (Parquet)
- **Purpose**: Source data for overdraft balances
- **Key Columns**:
  - BNMCODE: 14-character BNM code
  - AMOUNT: Balance amount
- **Filter**: Extract BNMCODE1 (first 7 chars) = '9521309' or '9521308'

### 3. BNM.BASE_ODCORP (Parquet)
- **Purpose**: Historical base data for corporate OD
- **Columns**:
  - REPTDATE: Report date (numeric YYMDD format)
  - AMOUNT: Total balance

### 4. BNM.BASE_ODIND (Parquet)
- **Purpose**: Historical base data for individual OD
- **Columns**: Same as BNM.BASE_ODCORP

### 5. BNM.TABLE (Parquet)
- **Purpose**: Template with REMMTH time buckets
- **Columns**:
  - REMMTH: Remaining months (time bucket indicator)
  - Other columns as needed

## Output Files

### Data Files (Parquet)

#### 1. bnm_store_odcorp.parquet
- **Purpose**: ODCORP data up to current report date
- **Contents**: All historical records where REPTDATE <= RDAT1

#### 2. bnm_store_odind.parquet
- **Purpose**: ODIND data up to current report date
- **Contents**: All historical records where REPTDATE <= RDAT1

#### 3. bnm_base_odcorp.parquet (if INSERT='Y')
- **Purpose**: Permanent base with updated record
- **Update**: Replaces existing RDAT1 record if present, adds new

#### 4. bnm_base_odind.parquet (if INSERT='Y')
- **Purpose**: Permanent base with updated record
- **Update**: Replaces existing RDAT1 record if present, adds new

#### 5. bnm_calc.parquet
- **Purpose**: Final BNM codes with amounts
- **Columns**:
  - BNMCODE: 14-character code
  - AMOUNT: Calculated amount
- **Contents**: Combined ODCORP and ODIND records

### Report Files (Text with ASA)

For each product (ODCORP, ODIND):
1. **{product}_48weeks_report.txt**: Last 48 weeks of data
2. **{product}_current_report.txt**: Most recent balance
3. **{product}_minimum_report.txt**: Minimum balance over 48 weeks
4. **{product}_maturity_profile.txt**: Breakdown by time bucket
5. **{product}_data_report.txt**: Final BNM codes and amounts

## Business Logic

### Insertion Logic

**INSERT Flag Determination**:
```
IF REPTDAY in (8, 15, 22):
    INSERT = 'Y'
ELSE IF REPTDAY = last_day_of_month AND today_day < 8:
    INSERT = 'Y'
ELSE:
    INSERT = 'N'
```

**Purpose**: Determine if this is an official reporting date

**Effects**:
- **INSERT = 'Y'**: Update permanent base, create store
- **INSERT = 'N'**: Create temporary store without updating base

### Date Format

**RDAT1**: 5-digit numeric date format YYMDD
- Example: Jan 15, 2025 → 25015 (YY=25, M=01, DD=15)
- Note: This is a compressed format where month is single digit for Jan-Sep

### BNM Code Structure

#### Source Codes (from BNM.NOTE)
- **9521309**: Performing OD - Corporate
- **9521308**: Performing OD - Individual

#### Target Codes (for BNM.CALC)
- **9321309XX0000Y**: Non-performing OD - Corporate, bucket XX
- **9321308XX0000Y**: Non-performing OD - Individual, bucket XX

**Note**: The conversion from 95 (performing) to 93 (non-performing) represents a liquidity run-off assumption for BNM reporting purposes.

### Calculation Methodology

#### Step 1: Extract Current Balance
From BNM.NOTE, sum all amounts where BNMCODE1 matches product code.

**Example ODCORP**:
```sql
SELECT SUM(AMOUNT)
FROM BNM.NOTE
WHERE SUBSTR(BNMCODE, 1, 7) = '9521309'
```

#### Step 2: Update Historical Base
Create new record:
```
REPTDATE = RDAT1
AMOUNT = [sum from step 1]
```

If INSERT = 'Y':
- Remove existing record with REPTDATE = RDAT1 (if any)
- Append new record
- Save to permanent base

#### Step 3: Create 48-Week Store
Filter all records where REPTDATE <= RDAT1

#### Step 4: Calculate Statistics

**Current Balance (CURBAL)**:
- Most recent record (highest REPTDATE)

**Minimum Balance (MINBAL)**:
- Minimum AMOUNT over last 48 weeks

**Example**:
- Week 1: 1,000,000
- Week 2: 950,000
- ...
- Week 48: 1,100,000
- CURBAL = 1,100,000 (most recent)
- MINBAL = 850,000 (minimum over 48 weeks)

#### Step 5: Spread Amounts Across Buckets

For each time bucket (REMMTH) in BNM.TABLE:

```python
IF REMMTH < 12:
    AMOUNT = (CURBAL - MINBAL) / 5
ELSE:
    AMOUNT = MINBAL
```

**Rationale**:
- Volatile portion (CURBAL - MINBAL) assumed to run off within 1 year
- Spread equally over 5 time buckets (01-05)
- Stable minimum assumed to stay >1 year (bucket 06)

**Example Calculation**:
- CURBAL = 1,000,000
- MINBAL = 800,000
- Volatile = 200,000
- Spread = 200,000 / 5 = 40,000 per bucket

| Time Bucket | REMMTH Range | Amount |
|-------------|--------------|---------|
| 01 | <0.1 | 40,000 |
| 02 | 0.1-1 | 40,000 |
| 03 | 1-3 | 40,000 |
| 04 | 3-6 | 40,000 |
| 05 | 6-12 | 40,000 |
| 06 | >=12 | 800,000 |

#### Step 6: Create BNM Codes

For each time bucket, create BNMCODE:
```
BNMCODE = '932130' + CUST + BUCKET + '0000Y'
```

Where:
- CUST: '9' for corporate, '8' for individual
- BUCKET: classify_remmth(REMMTH) → '01' to '06'

**Example ODCORP, bucket 03**:
```
'9321309' + '03' + '0000Y' = '932130903000Y'
```

### Time Bucket Classification

```python
def classify_remmth(remmth):
    if remmth < 0.1:
        return '01'   # UP TO 1 WK
    elif remmth < 1:
        return '02'   # >1 WK - 1 MTH
    elif remmth < 3:
        return '03'   # >1 MTH - 3 MTHS
    elif remmth < 6:
        return '04'   # >3 - 6 MTHS
    elif remmth < 12:
        return '05'   # >6 MTHS - 1 YR
    else:
        return '06'   # > 1 YEAR
```

## Data Flow

```
BNM.NOTE → Filter by BNMCODE1 → Sum AMOUNT → New Record
                                                    ↓
BNM.BASE_[PROD] → Update/Merge → BNM.STORE_[PROD]
                                        ↓
                            Sort DESC by REPTDATE
                                        ↓
                            Take last 48 records
                                        ↓
                            ├─→ Get Current (latest)
                            └─→ Get Minimum (lowest)
                                        ↓
BNM.TABLE → Join with calculated amounts
                    ↓
            Create BNM codes
                    ↓
            ├─→ ODCORP results
            └─→ ODIND results
                    ↓
            Combine → BNM.CALC
```

## Example Scenarios

### Scenario 1: Regular Weekly Update

**Inputs**:
- REPTDATE: 2025-01-15 (day 15)
- WK: 2
- Current ODCORP balance: 5,000,000
- Historical 48-week minimum: 4,200,000

**Processing**:
1. INSERT = 'Y' (day 15 triggers insertion)
2. RDAT1 = 25015
3. Volatile portion = 5,000,000 - 4,200,000 = 800,000
4. Spread = 800,000 / 5 = 160,000

**Results**:
| BNMCODE | AMOUNT |
|---------|---------|
| 932130901000Y | 160,000 |
| 932130902000Y | 160,000 |
| 932130903000Y | 160,000 |
| 932130904000Y | 160,000 |
| 932130905000Y | 160,000 |
| 932130906000Y | 4,200,000 |

### Scenario 2: Month-End Processing

**Inputs**:
- REPTDATE: 2025-01-31 (last day of month)
- Today: 2025-02-05 (day 5 < 8)
- Current ODIND balance: 2,000,000
- Historical minimum: 1,500,000

**Processing**:
1. INSERT = 'Y' (month-end before day 8 of next month)
2. RDAT1 = 25031
3. Volatile = 2,000,000 - 1,500,000 = 500,000
4. Spread = 500,000 / 5 = 100,000

**Results**:
| BNMCODE | AMOUNT |
|---------|---------|
| 932130801000Y | 100,000 |
| 932130802000Y | 100,000 |
| 932130803000Y | 100,000 |
| 932130804000Y | 100,000 |
| 932130805000Y | 100,000 |
| 932130806000Y | 1,500,000 |

### Scenario 3: Non-Insertion Run

**Inputs**:
- REPTDATE: 2025-01-20 (day 20, not 8/15/22)
- Current balance: 3,000,000

**Processing**:
1. INSERT = 'N' (day 20 doesn't trigger)
2. New record created for analysis only
3. Base file NOT updated
4. Store created temporarily with combined data
5. Calculations proceed normally

**Effect**: Reports generated but permanent base unchanged

## Report Formats

### 48 Weeks Table Report
```
                    48 WEEKS TABLE - ODCORP 15/01/25
--------------------------------------------------------------------------------
REPTDATE: 25015 | AMOUNT: 5,000,000.00
REPTDATE: 25008 | AMOUNT: 4,950,000.00
REPTDATE: 24952 | AMOUNT: 4,800,000.00
...
```

### Current Value Report
```
                    CURRENT VALUE - ODCORP 15/01/25
--------------------------------------------------------------------------------
REPTDATE: 25015 | AMOUNT: 5,000,000.00
```

### Minimum Value Report
```
                    MINIMUM VALUE - ODCORP 15/01/25
--------------------------------------------------------------------------------
REPTDATE: 24915 | AMOUNT: 4,200,000.00
```

### Maturity Profile Report
```
                        ODCORP MATURITY PROFILE
              BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE
                              15/01/25
----------------------------------------------------------------------------------------------------
CORE (NON-TRADING) BANKING ACTIVITIES                      AMOUNT
----------------------------------------------------------------------------------------------------
 >1 MTH - 3 MTHS                                          160,000.00
 >1 WK - 1 MTH                                            160,000.00
 >3 - 6 MTHS                                              160,000.00
 >6 MTHS - 1 YR                                           160,000.00
 > 1 YEAR                                               4,200,000.00
 UP TO 1 WK                                               160,000.00
----------------------------------------------------------------------------------------------------
 TOTAL                                                  5,000,000.00
```

### Data Report
```
                            ODCORP - 15/01/25
----------------------------------------------------------------------------------------------------
BNMCODE              AMOUNT
----------------------------------------------------------------------------------------------------
 932130901000Y         160,000.00
 932130902000Y         160,000.00
 932130903000Y         160,000.00
 932130904000Y         160,000.00
 932130905000Y         160,000.00
 932130906000Y       4,200,000.00
----------------------------------------------------------------------------------------------------
 TOTAL               5,000,000.00
```

## Technical Implementation

### Historical Data Management

**Base vs Store**:
- **Base**: Permanent repository of all historical records
- **Store**: Working dataset filtered to relevant date range

**Update Strategy**:
```python
if INSERT == 'Y':
    # Remove duplicate if exists
    base = base.filter(REPTDATE != RDAT1)
    # Add new record
    base = concat([base, new_record])
    # Save updated base
    save(base)
    # Create store from base
    store = base.filter(REPTDATE <= RDAT1)
else:
    # Don't modify base
    # Create temporary store
    store = concat([base, new_record]).filter(REPTDATE <= RDAT1)
```

### 48-Week Window

**Implementation**:
```python
# Sort descending by date
store_sorted = store.sort('REPTDATE', descending=True)

# Take first 48 records
week48 = store_sorted.head(48)
```

**Edge Cases**:
- Less than 48 weeks of data: Use all available
- Exactly 48 weeks: Use all
- More than 48 weeks: Use most recent 48

### RDAT1 Format

**Conversion**:
```python
# From date to RDAT1
reptdate = date(2025, 1, 15)
rdat1 = int(reptdate.strftime('%y%m%d'))  # → 250115
```

**Note**: This creates a 6-digit integer for single-digit months and 5-digit for month 10-12.

## Performance Considerations

1. **Minimal Sorting**: Sort only when required (DESC for current, ASC for minimum)
2. **Efficient Filtering**: Use Polars filter operations
3. **Single Pass Calculations**: Calculate stats in one iteration
4. **Conditional Base Updates**: Only write base when INSERT='Y'

## Error Handling

The code handles:
- Missing base files (creates new empty base)
- No matching BNMCODE records (creates zero-amount record)
- Less than 48 weeks of history (uses available data)

## Testing Checklist

### Data Validation
- [ ] REPTDATE loaded correctly
- [ ] INSERT flag calculated correctly
- [ ] RDAT1 format correct (5 or 6 digits)
- [ ] BNMCODE1 extraction correct (first 7 chars)

### Filtering
- [ ] ODCORP filtered for '9521309'
- [ ] ODIND filtered for '9521308'
- [ ] Store filtered to REPTDATE <= RDAT1

### Calculations
- [ ] Current balance = most recent record
- [ ] Minimum balance = lowest over 48 weeks
- [ ] Volatile portion = Current - Minimum
- [ ] Spread = Volatile / 5
- [ ] Buckets <12 months get spread amount
- [ ] Bucket >=12 months gets minimum

### BNM Codes
- [ ] Format: 9321309XX0000Y or 9321308XX0000Y
- [ ] Bucket codes 01-06 assigned correctly
- [ ] All 6 buckets present for each product

### Base Updates
- [ ] If INSERT='Y', base updated
- [ ] If INSERT='N', base unchanged
- [ ] Duplicate RDAT1 removed before append
- [ ] Store always created correctly

### Reports
- [ ] All 10 reports generated (5 per product)
- [ ] ASA carriage control present
- [ ] Amounts formatted with commas
- [ ] Totals calculated correctly

## Configuration

Update paths in script:
```python
LOAN_REPTDATE_PATH = "/data/input/loan_reptdate.parquet"
BNM_NOTE_PATH = "/data/input/bnm_note.parquet"
BNM_BASE_ODCORP_PATH = "/data/input/bnm_base_odcorp.parquet"
BNM_BASE_ODIND_PATH = "/data/input/bnm_base_odind.parquet"
BNM_TABLE_PATH = "/data/input/bnm_table.parquet"
OUTPUT_DIR = "/data/output"
```

## Dependencies

```
polars>=0.19.0
pyarrow>=14.0.0
```

## Execution

```bash
python3 od_liquidity_calc.py
```

Expected console output with record counts, balances, and file paths.
