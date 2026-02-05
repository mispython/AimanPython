# EIBMNL41: BNM Maturity Profile Calculation - Python Conversion

## Overview
This Python script converts a SAS program that calculates maturity profiles for various banking products using a minimum/maximum balance methodology over a 48-week historical period. Products include:
- **PBCARD**: Credit card balances
- **ODCORP/ODIND**: Overdrafts (Corporate/Individual)
- **SMCCORP/SMCIND**: Share Margin Credit (Corporate/Individual)

## Key Concept: Min/Max Methodology

The program uses a unique calculation approach:

1. **Historical Analysis**: Examines the last 48 weeks of balance data
2. **Current Balance**: Most recent week's balance
3. **Minimum Balance**: Lowest balance across the 48-week period
4. **Distribution**:
   - For maturity buckets < 12 months: `(Current - Minimum) / 5`
   - For maturity buckets ≥ 12 months: `Minimum`

This methodology assumes that:
- The minimum balance represents long-term, stable funds
- The difference between current and minimum represents shorter-term fluctuations
- Short-term fluctuations are distributed equally across 5 buckets

## Requirements
```bash
pip install duckdb polars pyarrow
```

## Input Files

### 1. **`./input/bnm/reptdate.parquet`**
- Contains: `reptdate` (reporting date)

### 2. **`./input/bnm/table.parquet`**
- Reference table with maturity buckets
- Contains: `basket` (optional), `remmth` (remaining months)

### 3. **`./input/bnm/note.parquet`**
- Output from previous loan processing (bnm_note_conversion.py)
- Contains: `bnmcode`, `amount`

### 4. **`./input/bnm1/loan{YYYYMM}{W}.parquet`**
- Loan data for SMC processing
- Contains: Same columns as in bnm_note_conversion.py

### 5. **`./input/walk.txt`**
- Fixed-width format file for PBCARD data
- Format:
  - Position 2-20: Description (must be '34200' for PBCARD)
  - Position 21-22: Day (DD)
  - Position 24-25: Month (MM)
  - Position 27-28: Year (YY, 2-digit)
  - Position 42-61: Amount (comma-formatted decimal)

### 6. **Historical Store Files** (created/updated by program)
- `./input/bnm/base_{PROD}.parquet`: All historical records
- `./input/bnm/store_{PROD}.parquet`: Records up to current reporting date

## Output Files

### 1. **`./output/reptdate.parquet`**
- Reporting date information

### 2. **`./output/calc.parquet`**
- Combined results for all products
- Columns: `bnmcode`, `amount`

### 3. **Updated Store Files** (in BNM_DIR)
- `base_{PROD}.parquet`: Updated with latest week
- `store_{PROD}.parquet`: Filtered up to reporting date

## Processing Flow

### 1. Reporting Date Extraction
```python
# Extract from reptdate.parquet
day = reptdate.day

# Determine week number
if day == 8: week = 1
elif day == 15: week = 2
elif day == 22: week = 3
else: week = 4

# Determine if this is an insert week
# INSERT = 'Y' if day is 8, 15, 22, or last day of month (and today < 8)
```

### 2. PBCARD Processing

**Input**: WALK file with credit card balance

**Steps**:
1. Parse WALK file for DESC='34200' and matching reporting date
2. Extract card amount (CARDAMT)
3. Append to BASE_CARD, create/update STORE_CARD
4. Get last 48 weeks from STORE_CARD
5. Calculate: CURBAL (most recent), MINBAL (minimum of 48 weeks)
6. For each maturity bucket:
   - If REMMTH < 12: `AMOUNT = (CURBAL - MINBAL) / 5`
   - If REMMTH ≥ 12: `AMOUNT = MINBAL`

**Output**:
- **PBCARD**: NPL codes (9321508XX0000Y)
- **PBCARD2**: Gross loan codes (9521508XX0000Y) distributed by basket:
  - Basket 1: 15% of total
  - Basket 2: 69% of total
  - Basket 3: 1% of total
  - Basket 4: 3% of total
  - Basket 5: 7% of total
  - Basket 6: 5% of total

### 3. Overdraft Processing

**ODCORP (Corporate)**: Customer type '09'
**ODIND (Individual)**: Customer type '08'

**Steps**:
1. Extract from NOTE.parquet where BNMCODE starts with '9521309' (corp) or '9521308' (ind)
2. Sum amounts for the customer type
3. Append to BASE_OD{CORP/IND}, update STORE
4. Calculate min/max profile as above
5. Output NPL codes (932130{9/8}XX0000Y)

### 4. SMC (Share Margin Credit) Processing

**SMCCORP (Corporate)**: Customer type '09'
**SMCIND (Individual)**: Customer type '08'

**Steps**:
1. Read loan data for current reporting period
2. Filter for:
   - Not paid (PAIDIND not in 'P', 'C')
   - Product codes starting with '34' or products 225, 226
   - Account type = 'OD'
   - Product code = '34240' (SMC products)
   - Exclude products 151, 152, 181
3. Classify by customer type (staff vs non-staff)
4. Sum amounts by BNMCODE (952190{9/8}XX0000Y)
5. Append to BASE_SMC{CORP/IND}, update STORE
6. Calculate min/max profile
7. Output NPL codes (932190{9/8}XX0000Y)

## BNMCODE Structure

### For this program:
- **Positions 1-2**: 
  - `93`: Non-performing loans (all outputs are NPL in this program)
  - `95`: Gross loans outstanding (PBCARD2 only)
- **Positions 3-5**: Product type
  - `215`: PBCARD (Credit Cards)
  - `213`: Overdrafts
  - `219`: SMC (Other facilities)
- **Positions 6-7**: Customer type
  - `08`: Individual/Staff
  - `09`: Corporate/Non-staff
- **Positions 8-9**: Maturity bucket
  - Same as previous program (01-06)
- **Positions 10-15**: `0000Y`

## Insert Logic

The program maintains historical data differently based on INSERT flag:

**INSERT = 'Y'** (Days 8, 15, 22, or month-end):
- This is an "official" reporting week
- Remove any existing record for this REPTDATE from BASE
- Append new record to BASE
- Update STORE with all records ≤ REPTDATE

**INSERT = 'N'** (Other days):
- This is an "interim" calculation
- Do NOT update BASE
- Create temporary STORE by combining BASE + new record
- Filter STORE to records ≤ REPTDATE

This allows for:
- Regular updates on official reporting days
- Ad-hoc calculations on other days without polluting the base

## Example Calculation

**Scenario**: PBCARD with 48-week history

```
Week 1: 1,000,000
Week 2: 950,000
...
Week 48: 1,100,000
Current (Week 49): 1,200,000

CURBAL = 1,200,000
MINBAL = 950,000 (minimum across all 48 weeks)

Maturity Buckets:
- UP TO 1 WK (REMMTH=0.1):    (1,200,000 - 950,000) / 5 = 50,000
- >1 WK - 1 MTH (REMMTH=0.5):  (1,200,000 - 950,000) / 5 = 50,000
- >1 MTH - 3 MTHS (REMMTH=2):  (1,200,000 - 950,000) / 5 = 50,000
- >3 - 6 MTHS (REMMTH=4.5):    (1,200,000 - 950,000) / 5 = 50,000
- >6 MTHS - 1 YR (REMMTH=9):   (1,200,000 - 950,000) / 5 = 50,000
- > 1 YEAR (REMMTH=13):        950,000
```

## Date Formats

### Julian Date (RDAT1)
- Format: YYDDD (e.g., 24015 = 15th day of 2024)
- Used for: Storing/comparing reptdate values

### Standard Date (RDATE)
- Format: DDMMYYYY (e.g., 15012024)
- Used for: Display purposes

## Key Differences from SAS

1. **PROC TABULATE**: Replaced with custom `print_tabulation()` function
2. **INFILE/INPUT**: Replaced with Python file parsing for fixed-width format
3. **CALL SYMPUT**: Replaced with Python variables and dictionary
4. **Macro variables**: Replaced with function parameters and return values
5. **Julian dates**: Implemented using `strftime('%y%j')`

## Validation Checklist

- [ ] Verify 48-week window is correctly implemented
- [ ] Check min/max calculations match SAS output
- [ ] Validate INSERT='Y' vs INSERT='N' behavior
- [ ] Confirm PBCARD basket percentages (15%, 69%, 1%, 3%, 7%, 5%)
- [ ] Verify customer type classification (08 vs 09)
- [ ] Check WALK file parsing (positions and formats)
- [ ] Validate maturity bucket assignments
- [ ] Confirm BNMCODE generation matches exactly

## Troubleshooting

### Issue: No data in WALK file
**Solution**: Ensure WALK file exists and has records with DESC='34200'

### Issue: Historical data not found
**Solution**: First run will have no historical data; subsequent runs will build history

### Issue: INSERT flag always 'N'
**Solution**: Check that reporting date falls on 8th, 15th, 22nd, or month-end

### Issue: Maturity amounts don't sum to total
**Solution**: This is expected - short-term buckets get 1/5 of difference, long-term gets minimum

## Dependencies Between Programs

This program depends on output from `bnm_note_conversion.py`:
1. Run `bnm_note_conversion.py` first to create `note.parquet`
2. Then run this program to calculate maturity profiles

## Usage

```bash
# First, run the note conversion
python bnm_note_conversion.py

# Then, run the calc conversion
python bnm_calc_conversion.py
```

## File Structure
```
./input/
  bnm/
    reptdate.parquet
    table.parquet
    note.parquet
    base_CARD.parquet (created/updated)
    store_CARD.parquet (created/updated)
    base_ODCORP.parquet (created/updated)
    store_ODCORP.parquet (created/updated)
    base_ODIND.parquet (created/updated)
    store_ODIND.parquet (created/updated)
    base_SMCCORP.parquet (created/updated)
    store_SMCCORP.parquet (created/updated)
    base_SMCIND.parquet (created/updated)
    store_SMCIND.parquet (created/updated)
  bnm1/
    loan{YYYYMM}{W}.parquet
  walk.txt

./output/
  reptdate.parquet
  calc.parquet
```

## Future Enhancements

1. Add command-line arguments for date ranges
2. Create visualization of maturity profiles
3. Add comparison reports (current vs previous week)
4. Implement data validation checks
5. Add audit trail logging
6. Support multiple WALK file formats
7. Optimize for very large historical datasets

## Notes

- The 48-week window is a regulatory requirement for stable funding analysis
- The min/max methodology provides a conservative estimate of maturity distribution
- PBCARD basket percentages should be verified with business rules
- Historical data accumulates over time; consider archival strategy for old data
