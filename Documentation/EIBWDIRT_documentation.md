# EIBWDIRT: BNM Interest Rate Reporting - Python Conversion Documentation

## Overview
This document describes the conversion of the BNM (Bank Negara Malaysia) Interest Rate Reporting SAS program to Python. The program calculates weighted average interest rates for domestic deposits and generates regulatory reports.

## Program Purpose
The program:
1. Loads interest rate tables from fixed-width text files
2. Calculates effective interest rates based on compounding periods
3. Processes deposit accounts and assigns rate tiers
4. Calculates weighted average interest rates
5. Generates BNM regulatory output in binary format
6. Produces management reports with ASA carriage control

## Key Technical Specifications

### Input Files

#### 1. DEPOSIT.REPTDATE (Parquet)
- **Purpose**: Contains the reporting date
- **Columns**: REPTDATE (date field)
- **Usage**: Determines week number and date formatting

#### 2. DEPOSIT.SAVING (Parquet)
- **Purpose**: Savings account data
- **Key Columns**: 
  - OPENIND (account open indicator)
  - CURBAL (current balance)
  - PRODUCT (product code)
  - Other account details

#### 3. DEPOSIT.CURRENT (Parquet)
- **Purpose**: Current account data
- **Columns**: Same as DEPOSIT.SAVING

#### 4. RATE File (Fixed-Width Text)
- **Purpose**: Interest rate table
- **Format**:
  ```
  Position  Length  Field     Type
  1-3       3       TRANCD    Character
  4-6       3       PRODUCT   Packed Decimal (ZD3)
  7-15      9       TIER      Packed Decimal (ZD9)
  16-20     5       RATE      Numeric (5.3 format)
  21-24     4       YYYY      Character
  21-22     2       CC        Character (Century)
  23-24     2       YY        Character (Year)
  25-26     2       MMMM      Character (Month)
  27-28     2       DDDD      Character (Day)
  ```

### Output Files

#### 1. Binary Data File: IRWTT{REPTMON}{NOWK}.dat
- **Format**: Binary file with packed data
- **Structure** (per record, 31 bytes total):
  ```
  Position  Length  Field     Format
  1-14      14      ITCODE    ASCII string
  15-22     8       AMOUNT    Double (8-byte float)
  23-30     8       EFFDATE   ASCII string (or spaces)
  31        1       FLAG      ASCII character ('E')
  ```
- **Purpose**: BNM regulatory submission file

#### 2. Report File: bnm_interest_rate_report.txt
- **Format**: Text file with ASA carriage control
- **Purpose**: Management report
- **Layout**: Headers + detail lines with formatted data

### Constants and Parameters

```python
ACELIMIT = 5000  # Account excess limit for products 150-152, 181
```

### Week Number Determination

Based on day of month from REPTDATE:
- Day 8 → Week 1
- Day 15 → Week 2
- Day 22 → Week 3
- All other days → Week 4

## Data Flow

```
RATE File → Parse → Filter (TRANCD='215') → Calculate Effective Rates → EFFECT & SARATE
                                                                            ↓
DEPOSIT.SAVING + DEPOSIT.CURRENT → Filter → Assign Tiers → Merge with EFFECT
                                                                ↓
                                                         Calculate Weighted Avg
                                                                ↓
                                            SARATE → Min/Max Rates
                                                                ↓
                                                    Combine BNM Records
                                                                ↓
                                                    Sort by ITCODE
                                                                ↓
                                            Write Binary File + Report
```

## Technical Implementation Details

### 1. Date Processing

**Week Number Logic**:
```python
day = reptdate.day
if day == 8:
    nowk = '1'
elif day == 15:
    nowk = '2'
elif day == 22:
    nowk = '3'
else:
    nowk = '4'
```

### 2. Rate File Parsing

The rate file uses fixed-width format with packed decimal fields:
- **PRODUCT (ZD3)**: 3-digit zoned decimal
- **TIER (ZD9)**: 9-digit zoned decimal
- **RATE**: 5.3 format (5 total digits, 3 decimals)

Example parsing:
```python
product = int(line[3:6])      # Position 4-6
tier = int(line[6:15])        # Position 7-15
rate = float(line[15:20]) / 10  # Position 16-20, divide by 10
```

### 3. Effective Rate Calculation

Different products have different compounding periods:

#### Products 200, 201 (Semi-Annual)
```python
interest = rate / 100
effectr = ((1 + interest/2)**2 - 1) * 100
erate = round(effectr, 2)
```

#### Products 202, 203, 213, 150, 177 (Monthly)
```python
interest = rate / 100
effectr = ((1 + interest/12)**12 - 1) * 100
erate = round(effectr, 2)
```

#### Product 212 (Monthly with 1.05 multiplier)
```python
interest = rate / 100
effectr = ((1 + (interest * 1.05)/12)**12 - 1) * 100
erate = round(effectr, 2)
```

#### Products 204, 214, 215 (No compounding)
```python
erate = rate  # Use rate as-is
```

### 4. Account Filtering

Accounts are filtered based on:
- OPENIND NOT IN ('B', 'C', 'P') - Exclude blocked, closed, purged
- CURBAL >= 0 - Non-negative balance
- PRODUCT IN (150, 151, 152, 181, 177, 200, 201, 202, 203, 204, 212, 213, 214, 215)

### 5. Tier Assignment Logic

Each product has specific tier breakpoints:

#### Products 150, 151, 152, 181 → Consolidated to 150
```python
if curbal > ACELIMIT (5000):
    tier = 999999999
    curbal = curbal - ACELIMIT
else:
    tier = 5000
    curbal = 0
```

#### Product 177
```python
if curbal < 5000:
    tier = 5000
else:
    tier = 999999999
```

#### Products 200, 201
```
Balance Range         → Tier
< 2,500              → 2500
2,500 - 4,999        → 5000
5,000 - 9,999        → 10000
10,000 - 29,999      → 30000
30,000 - 49,999      → 50000
50,000 - 74,999      → 75000
>= 75,000            → 999999999
```

#### Product 202
```
Balance Range         → Tier
< 500                → 500
500 - 1,999          → 2000
2,000 - 4,999        → 5000
5,000 - 9,999        → 10000
10,000 - 29,999      → 30000
30,000 - 49,999      → 50000
50,000 - 74,999      → 75000
>= 75,000            → 999999999
```

#### Product 203
```
Balance Range         → Tier
< 2,000              → 2000
2,000 - 4,999        → 5000
5,000 - 9,999        → 10000
10,000 - 29,999      → 30000
30,000 - 49,999      → 50000
50,000 - 74,999      → 75000
>= 75,000            → 999999999
```

#### Product 204
```
All balances         → 999999999
```

#### Product 212
```
Balance Range         → Tier
< 5,000              → 5000
5,000 - 9,999        → 10000
10,000 - 19,999      → 20000
20,000 - 29,999      → 30000
30,000 - 49,999      → 50000
>= 50,000            → 999999999
```

#### Product 213
```
Balance Range         → Tier
< 5,000              → 5000
5,000 - 9,999        → 10000
10,000 - 29,999      → 30000
30,000 - 49,999      → 50000
50,000 - 74,999      → 75000
>= 50,000            → 999999999
```
**Note**: There's a logical issue in the original SAS code where the last condition checks >= 50,000 but should check >= 75,000. The Python code replicates the original logic.

#### Product 214
```
Balance Range         → Tier
< 1,000              → 1000
1,000 - 4,999        → 5000
5,000 - 24,999       → 25000
25,000 - 49,999      → 50000
>= 50,000            → 999999999
```

#### Product 215
```
Balance Range         → Tier
< 50,000             → 50000
>= 50,000            → 999999999
```

### 6. Weighted Average Calculation

```python
totint = Σ(ERATE × CURBAL)
totamt = Σ(CURBAL)
weighted_avg = totint / totamt
```

This is stored as BNM code '8420300000000Y'.

### 7. SA Rate Processing (Products 212, 213)

For special accounts (products 212, 213):
1. Filter rates for these products
2. Sort by PRODUCT, RATE, EFFDATE
3. Keep first record per PRODUCT
4. Sort by ERATE, EFFDATE
5. Extract:
   - **Minimum rate**: First record → Code '8420100000000Y'
   - **Maximum rate**: First record when sorted descending → Code '8420200000000Y'

### 8. BNM Code Structure

Three records are generated:
- **8420100000000Y**: Minimum SA rate (from products 212/213)
- **8420200000000Y**: Maximum SA rate (from products 212/213)
- **8420300000000Y**: Weighted average rate (all products)

### 9. Binary File Format

Record structure (31 bytes):
```
Offset  Length  Field     Type        Description
0       14      ITCODE    char[14]    BNM code (left-aligned, space-padded)
14      8       AMOUNT    double      Interest rate (IEEE 754 double)
22      8       EFFDATE   char[8]     Effective date or spaces
30      1       FLAG      char        'E' flag
```

Example binary write:
```python
itcode = row['ITCODE'].ljust(14)[:14].encode('ascii')
f.write(itcode)
f.write(struct.pack('d', amount))  # 8-byte double
f.write(effdate.encode('ascii'))   # 8-byte string
f.write(flag.encode('ascii'))      # 1-byte char
```

### 10. Report Format

**Headers**:
```
PUBLIC BANK BERHAD
REPORT ON DOMESTIC INTEREST RATE - PART I
REPORTING DATE : 31/01/25

ITCODE               AMOUNT         EFFDATE    FLAG
--------------------------------------------------------------------------------
```

**Detail Lines**:
```
 8420100000000Y        3.150000 01/12/24   E
 8420200000000Y        3.750000 15/12/24   E
 8420300000000Y        3.456789            E
```

**ASA Carriage Control**:
- '1' = Form feed (new page)
- ' ' = Single space (normal line)

## Product Codes Reference

| Code | Product Name | Compounding | Special Notes |
|------|--------------|-------------|---------------|
| 150-152, 181 | Basic Savings | Monthly | Consolidated to 150, ACELIMIT applied |
| 177 | Savings Plus | Monthly | - |
| 200, 201 | Premium Savings | Semi-annual | - |
| 202 | Regular Savings | Monthly | - |
| 203 | Gold Savings | Monthly | - |
| 204 | Fixed Deposit | None | Single tier |
| 212 | Special Account 1 | Monthly | 1.05 multiplier, min/max tracking |
| 213 | Special Account 2 | Monthly | Min/max tracking |
| 214 | Youth Savings | None | - |
| 215 | Senior Savings | None | - |

## Performance Optimizations

1. **DuckDB for Parquet**: Fast columnar data reading
2. **Polars DataFrames**: Efficient memory usage and operations
3. **Minimal Sorting**: Only sort when required for business logic
4. **Vectorized Operations**: Use Polars expressions instead of row-by-row processing
5. **Single-pass Calculations**: Calculate totals in one pass

## Error Handling

The code handles:
- Missing or malformed rate file lines
- Null/empty values in account fields
- Division by zero in weighted average (checks totamt > 0)
- Missing EFFDATE values (writes spaces to binary file)

## Testing Checklist

### Data Validation
- [ ] REPTDATE loaded correctly
- [ ] Week number calculated correctly (8→1, 15→2, 22→3, other→4)
- [ ] Rate file parsed with correct field positions
- [ ] Filtered to TRANCD='215' and valid products
- [ ] Effective rates calculated correctly for each product type
- [ ] Account filtering applied (OPENIND, CURBAL, PRODUCT)

### Tier Assignment
- [ ] Products 150-152, 181 consolidated to 150
- [ ] ACELIMIT applied correctly to product 150
- [ ] All tier breakpoints match specification
- [ ] CURBAL adjusted for product 150 (excess over ACELIMIT)

### Rate Calculations
- [ ] Weighted average = totint / totamt
- [ ] SA rates filtered for products 212, 213
- [ ] Minimum SA rate = first after sorting by ERATE asc
- [ ] Maximum SA rate = first after sorting by ERATE desc

### Output Validation
- [ ] Binary file has correct record length (31 bytes)
- [ ] ITCODE field is 14 bytes, left-aligned, space-padded
- [ ] AMOUNT is 8-byte IEEE 754 double
- [ ] EFFDATE is 8 bytes (date string or spaces)
- [ ] FLAG is 1 byte ('E')
- [ ] Records sorted by ITCODE
- [ ] Three records generated (8420100000000Y, 8420200000000Y, 8420300000000Y)

### Report Validation
- [ ] ASA carriage control characters present
- [ ] Page breaks at 60 lines
- [ ] Headers on each page
- [ ] ITCODE column width 20 characters
- [ ] AMOUNT formatted with 6 decimals
- [ ] EFFDATE column width 10 characters
- [ ] FLAG column width 5 characters

## Configuration

Update these paths in the script:

```python
# Input paths
DEPOSIT_REPTDATE_PATH = "/data/input/deposit_reptdate.parquet"
DEPOSIT_SAVING_PATH = "/data/input/deposit_saving.parquet"
DEPOSIT_CURRENT_PATH = "/data/input/deposit_current.parquet"
RATE_FILE_PATH = "/data/input/rate.txt"

# Output paths
OUTPUT_DIR = "/data/output"
```

## Execution

```bash
python3 bnm_interest_rate.py
```

Expected console output:
```
Report Date: 15/01/25
Week Number: 2
Report Year: 2025, Month: 01

Step 1: Loading and processing rate file...
  Rates loaded: 45 records

Step 2: Calculating effective rates...
  Effect rates: 45 records
  SA rates: 12 records

Step 3: Loading and processing account data...
  Accounts loaded: 125,678 records

Step 4: Assigning tiers to accounts...

Step 5: Merging accounts with rates...
  Merged records: 125,678 records

Step 6: Calculating weighted average interest rate...
  Total Interest: 4,567,890.12
  Total Amount: 1,234,567,890.00
  Weighted Average Rate: 3.456789

Step 7: Processing SA rates...
  Min SA Rate: 3.15 (Effective: 01/12/24)
  Max SA Rate: 3.75 (Effective: 15/12/24)

Step 8: Creating final BNM dataset...
  Final BNM records: 3

Step 9: Writing binary output to /data/output/irwtt0102.dat...
  Binary file written: /data/output/irwtt0102.dat

Step 10: Generating report...
  Report written to: /data/output/bnm_interest_rate_report.txt

======================================================================
BNM Interest Rate Reporting completed successfully!
======================================================================

Output files:
  - Binary data: /data/output/irwtt0102.dat
  - Report: /data/output/bnm_interest_rate_report.txt
```

## Maintenance Notes

1. **ACELIMIT**: Currently set to 5000, adjust if policy changes
2. **Product Codes**: Add new products to filter lists and tier assignment logic
3. **Tier Breakpoints**: Update tier assignment function if thresholds change
4. **BNM Codes**: Fixed codes used for regulatory reporting
5. **Compounding Periods**: Match product specifications

## Dependencies

```
duckdb>=0.9.0
polars>=0.19.0
pyarrow>=14.0.0
```

## Known Issues

1. **Product 213 Tier Logic**: Original SAS code has last condition `>= 50000` which conflicts with prior condition `50000 - 74999 → 75000`. This should likely be `>= 75000`. The Python code replicates the original logic for consistency.

2. **Rate File Format**: Assumes fixed-width format with specific positions. Any deviation in format will cause parsing errors.

## Differences from SAS

1. **Binary Output**: SAS writes to library dataset (BNM.IRWTT), Python writes binary file directly
2. **Sorting**: Python minimizes sorts using efficient filtering
3. **Packed Decimals**: SAS ZD format handled as integer parsing in Python
4. **Null Handling**: Python explicitly handles None values in EFFDATE

## Regulatory Compliance

This program generates data for Bank Negara Malaysia (BNM) regulatory reporting. The output format and codes must match BNM specifications exactly. Any changes should be validated against current regulations.
