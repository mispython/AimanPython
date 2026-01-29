# SAS to Python Conversion Documentation
## Weekly Loan Payment Schedule Extraction (EIBWPSCH)

## Overview
This program extracts weekly loan payment schedules from a binary payment file and prepares them for warehouse upload. The data is segmented by week based on the report date.

## Program Purpose
**Job Name**: EIBWPSCH  
**Purpose**: Extract loan schedule payments for weekly periods  
**Output**: Weekly payment data, print report, and transport file

## Weekly Periods

The program segments data into 4 weekly periods based on the report day:

| Report Day | Week Code | Period Coverage |
|------------|-----------|-----------------|
| 8 | 01 | 1st - 8th of month |
| 15 | 02 | 9th - 15th of month |
| 22 | 03 | 16th - 22nd of month |
| Other | 04 | 23rd - Month End |

## Input Files

### 1. Report Date File
- **Path**: `input/ln_reptdate.parquet`
- **Description**: Contains the report date
- **Columns**:
  - `REPTDATE` (date): The report date

### 2. Loan Note File
- **Path**: `input/ln_lnnote.parquet`
- **Description**: Master list of active loan notes
- **Columns**:
  - `ACCTNO` (int64): Account number
  - `NOTENO` (int32): Note number
  - `PAIDIND` (string): Paid indicator (optional - not filtered in this version)

### 3. Payment Schedule File (Binary)
- **Path**: `input/paysfile.dat`
- **Description**: Binary file with packed decimal format
- **Format**: Fixed-length records with IBM packed decimal fields

#### Payment File Record Layout

The file uses IBM packed decimal (PD) format. Field positions are 1-based:

| Position | Field | Format | Length | Decimals | Description |
|----------|-------|--------|--------|----------|-------------|
| @001 | ACCTNO | PD6. | 6 bytes | 0 | Account number |
| @007 | NOTENO | PD3. | 3 bytes | 0 | Note number |
| @011 | EFFDAT | PD6. | 6 bytes | 0 | Effective date (YYYYMMDD packed) |
| @018 | PAYTYPE | $1. | 1 byte | - | Payment type (character) |
| @019 | NOPAY | PD3. | 3 bytes | 0 | Number of payments |
| @022 | PAYAMT | PD8.2 | 8 bytes | 2 | Payment amount |
| @054 | SCHFREQ | $1. | 1 byte | - | Schedule frequency |
| @061 | PAYMAINTAIN | PD6. | 6 bytes | 0 | Payment maintain date |

## Output Files

### 1. Weekly Payment Data (Parquet)
- **Path**: `/mnt/user-data/outputs/lnpay_week{WK}.parquet`
- **Example**: `lnpay_week01.parquet` for week 1
- **Description**: Processed payment schedules for the week
- **Format**: Parquet file

**Schema:**
- `ACCTNO` (int64): Account number
- `NOTENO` (int32): Note number
- `PAYTYPE` (string): Payment type
- `NOPAY` (int32): Number of payments
- `PAYAMT` (float64): Payment amount
- `SCHFREQ` (string): Schedule frequency
- `PAYDAY` (int32): Original day from effective date
- `EFFDATE` (date): Calculated effective date
- `PAYMAINTAIN` (date): Payment maintenance date

### 2. Print Report
- **Path**: `/mnt/user-data/outputs/lnpay_print_report.txt`
- **Description**: Formatted report showing first 50 records
- **Format**: Text file with ASA carriage control

### 3. Transport File
- **Path**: `/mnt/user-data/outputs/lnpywftp.dat`
- **Description**: Binary transport file for data warehouse
- **Format**: Custom binary format containing parquet data

## Business Logic

### Week Determination

```python
if reptday == 8:
    NOWK = '01'
elif reptday == 15:
    NOWK = '02'
elif reptday == 22:
    NOWK = '03'
else:
    NOWK = '04'
```

### Effective Date Processing

The EFFDAT field contains a packed decimal YYYYMMDD value. Special handling for:

1. **Day 31 or 99**: Month-end adjustment
   - Jan, Mar, May, Jul, Aug, Oct, Dec → 31
   - Feb → 29 (leap year) or 28
   - Apr, Jun, Sep, Nov → 30

2. **Day 30**: February adjustment
   - Feb → 29 (leap year) or 28

3. **Day 29**: February adjustment
   - Feb → 29 (leap year) or 28

Example:
```
Input: EFFDAT = 20260231 (packed decimal)
Output: EFFDATE = 2026-02-28 (not a leap year)

Input: EFFDAT = 20240229 (packed decimal)
Output: EFFDATE = 2024-02-29 (leap year)
```

### Payment Maintain Date Processing

If PAYMAINTAIN > 0, it's converted from packed decimal MMDDYYYY format:

```
Input: PAYMAINTAIN = 01152026 (packed)
Output: PAYMAINTAIN = 2026-01-15
```

### Record Filtering

Records are included if:
1. **In loan note file**: ACCTNO and NOTENO exist in LNNOTE
2. **Active payments**: NOPAY > 0

## Processing Steps

### Step 1: Report Date and Week Determination
1. Read report date from parquet
2. Extract day component
3. Determine week code (01, 02, 03, or 04)
4. Set macro variables: NOWK, REPTMON, REPTYEAR

### Step 2: Read Payment File
1. Open binary payment file
2. Read fixed-length records
3. Unpack IBM packed decimal fields
4. Convert to Python data structures

### Step 3: Process Dates
1. Extract date components from EFFDAT
2. Adjust days for month-end scenarios
3. Create datetime objects
4. Process PAYMAINTAIN if present

### Step 4: Load Loan Notes
1. Read LNNOTE parquet file
2. Extract ACCTNO and NOTENO
3. Prepare for merge

### Step 5: Merge and Filter
1. Left join LNNOTE with payment data
2. Keep only records where ACCTNO/NOTENO match
3. Filter for NOPAY > 0
4. Create final weekly dataset

### Step 6: Save to Parquet
1. Write processed data to parquet file
2. Filename includes week code

### Step 7: Generate Print Report
1. Create formatted text report
2. Show first 50 observations
3. Include ASA carriage control
4. Format dates as DDMMMYYYY

### Step 8: Create Transport File
1. Read parquet data
2. Create binary transport format
3. Write to .dat file

## Packed Decimal Format

### IBM Packed Decimal Structure

Each byte stores two decimal digits (nibbles):
- High nibble: First digit (0-9)
- Low nibble: Second digit (0-9) or sign

Example: Number 12345 as PD6.
```
Bytes: 00 00 00 01 23 45
       00 00 00 01 23 4C (C = positive sign)
```

Sign nibbles:
- `C` (0xC): Positive
- `D` (0xD): Negative
- `F` (0xF): Unsigned (treat as positive)
- `B` (0xB): Negative (alternate)

### Decimal Places

For PD8.2 (8 bytes, 2 decimal places):
- Value stored: 123456
- Actual value: 1234.56

The decimal point is implied, not stored.

## Date Formats

### SAS DATE8. Format
Output format: `DDMMMYYYY`

Examples:
- `15JAN2026`
- `28FEB2024`
- `31DEC2025`

### Packed Decimal Dates

Two formats are used:

1. **YYYYMMDD** (EFFDAT)
   - Example: 20260115 → January 15, 2026

2. **MMDDYYYY** (PAYMAINTAIN)
   - Example: 01152026 → January 15, 2026

## ASA Carriage Control

The print report uses ASA carriage control:
- `'1'`: Form feed (new page)
- `' '`: Single space (normal line)

## Key Differences: SAS vs Python

### Data Processing
- **SAS**: INFILE with INPUT statement, automatic packed decimal handling
- **Python**: Manual unpacking of binary data with custom functions

### Date Handling
- **SAS**: Automatic date functions (MDY, SUBSTR, PUT)
- **Python**: Manual parsing and datetime object creation

### File Formats
- **SAS**: Native SAS dataset with PROC CPORT for transport
- **Python**: Parquet for data, custom binary format for transport

### Merge Logic
- **SAS**: DATA step MERGE with BY statement
- **Python**: DuckDB SQL LEFT JOIN

## Error Handling

The conversion includes:
1. **File not found**: Gracefully handles missing payment file
2. **Invalid dates**: Returns None for unparseable dates
3. **Packed decimal errors**: Returns 0 or None for invalid data
4. **Empty datasets**: Creates proper schema even with no data

## Dependencies

### Python Packages
- `duckdb`: SQL analytics and parquet handling
- `struct`: Binary data packing/unpacking
- `datetime`: Date manipulation
- `calendar`: Calendar functions
- `pathlib`: File path handling

### Installation
```bash
pip install duckdb
```

## Usage

### Basic Execution
```bash
python weekly_loan_payment_schedule.py
```

### Prerequisites
1. Input files must exist:
   - `input/ln_reptdate.parquet`
   - `input/ln_lnnote.parquet`
   - `input/paysfile.dat` (binary format)
2. Payment file must be in IBM packed decimal format
3. Output directory must be writable

### Expected Console Output
```
Step 1: Processing report date and determining week...
Report Date: 2026-01-08
Report Day: 8
Week Number: 01
Report Month: 01
Report Year: 26

Step 2: Reading and processing payment file...
Payment records read: X,XXX

Step 3: Loading loan note data...
Loan note records loaded: X,XXX

Step 4: Merging loan notes with payment data...
Final payment schedule records: X,XXX

Step 5: Saving weekly payment data...
Weekly payment data saved: /mnt/user-data/outputs/lnpay_week01.parquet

Step 6: Generating print report...
Print report saved: /mnt/user-data/outputs/lnpay_print_report.txt

Step 7: Creating transport file...
Transport file created: /mnt/user-data/outputs/lnpywftp.dat
Transport file size: XXX,XXX bytes

======================================================================
WEEKLY LOAN PAYMENT EXTRACTION COMPLETE
======================================================================

Week Extracted: Week 01
Report Period: 2026-01-08
Total Records: X,XXX

Generated Files:
  1. Weekly Payment Data: /mnt/user-data/outputs/lnpay_week01.parquet
  2. Print Report: /mnt/user-data/outputs/lnpay_print_report.txt
  3. Transport File: /mnt/user-data/outputs/lnpywftp.dat

Payment Statistics:
  Total Records: X,XXX
  Unique Accounts: X,XXX
  Total Payment Amount: $X,XXX,XXX.XX
  Average Payment: $X,XXX.XX
  Earliest Payment Date: YYYY-MM-DD
  Latest Payment Date: YYYY-MM-DD

Conversion complete!
```

## Testing Recommendations

1. **Packed Decimal Unpacking**
   - Test with known values
   - Verify sign handling (positive/negative)
   - Check decimal place positioning

2. **Date Processing**
   - Test month-end adjustments (31, 30, 29, 28)
   - Verify leap year calculations
   - Check special values (99)

3. **Week Determination**
   - Test all four week boundaries (8, 15, 22, other)
   - Verify correct week codes assigned

4. **Merge Logic**
   - Confirm only active loans included
   - Verify NOPAY > 0 filter
   - Check record counts

5. **File Formats**
   - Verify parquet schema
   - Check print report formatting
   - Validate transport file structure

## Performance Considerations

1. **Binary File Reading**: Efficient byte-by-byte processing
2. **DuckDB Operations**: Optimized SQL for joins and filtering
3. **Memory Usage**: Processes records in manageable chunks
4. **File I/O**: Minimal disk operations

## Troubleshooting

### Common Issues

1. **Invalid Packed Decimal Data**
   - Check byte alignment in binary file
   - Verify field positions match layout
   - Inspect with hex editor if needed

2. **Date Conversion Errors**
   - Validate YYYYMMDD format in source
   - Check for invalid dates (e.g., Feb 31)
   - Verify leap year calculations

3. **Empty Output**
   - Confirm payment file exists and is readable
   - Check NOPAY > 0 condition
   - Verify LNNOTE has matching records

4. **Wrong Week Code**
   - Check report date day value
   - Verify week determination logic
   - Confirm REPTDATE is correct

## Maintenance Notes

1. **Payment File Format**: If binary format changes, update field positions
2. **Week Boundaries**: Current logic fixed at 8, 15, 22 - adjust if needed
3. **Date Formats**: Modify date processing if format changes
4. **Transport Format**: Custom format may need adaptation for target system
