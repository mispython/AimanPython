# EIGWRD1W: Walker Data Extraction System

## Complete Documentation

---

## Table of Contents
1. [Overview](#overview)
2. [Input File Format](#input-file-format)
3. [Processing Logic](#processing-logic)
4. [Output Files](#output-files)
5. [Date Validation](#date-validation)
6. [Installation & Usage](#installation--usage)
7. [Testing](#testing)
8. [Troubleshooting](#troubleshooting)

---

## Overview

### Purpose
The Walker Data Extraction System reads Walker ALW format files, validates the reporting date, and extracts data into five separate datasets based on record types.

### Key Features
- **Date validation**: Ensures Walker file matches current reporting period
- **Multi-dataset extraction**: Splits single file into 5 typed datasets
- **Fixed-position parsing**: Handles complex fixed-width format
- **Automatic sorting**: Sorts each dataset per specifications

### Processing Schedule
Walker files are processed based on the day of the month:
- **Days 1-7**: Process data as of last day of previous month
- **Days 8-14**: Process data as of the 8th
- **Days 15-21**: Process data as of the 15th
- **Days 22+**: Process data as of the 22nd

---

## Input File Format

### Walker ALW File Structure

The Walker file is a fixed-position text file with two record types:

**Record Type '1'** (Header): Defines the dataset type
```
Position 1:   Space
Position 2:   '1' (record identifier)
Position 3-10: SETID (8 characters)
```

**Record Type '+'** (Data): Contains actual data
```
Position 1:   Space
Position 2:   '+' (record identifier)
Position 3+:  Data fields (varies by SETID)
```

### Dataset Types (SETID)

#### 1. R1GL4000 - General Ledger Data
**Purpose**: GL account balances and amounts

**Data Format**:
```
Position 1:    Space
Position 2:    '+'
Position 3-24: ACCT_NO (22 chars)
Position 29:   ACKIND (1 char) - Account kind (D/C/ )
Position 31-51: GLAMT (21 chars) - Amount in COMMA21.2 format
```

**Example**:
```
 +10000110000000123     D    1,234,567.89
```

#### 2. R1GL4100 - Journal Entry Data
**Purpose**: Journal entries with effective dates

**Data Format**:
```
Position 1:    Space
Position 2:    '+'
Position 3-24:  ACCT_NO (22 chars)
Position 30-37: EFFDATE (8 chars) - DDMMYYYY format
Position 47-67: JEAMT (21 chars) - Journal entry amount
Position 75:    ACKIND (1 char)
```

**Example**:
```
 +10000110000000123     31012025         234,567.89       D
```

#### 3. R1KEY913 - Account Keys
**Purpose**: Account identifiers

**Data Format**:
```
Position 1:    Space
Position 2:    '+'
Position 3-24: ACCT_NO (22 chars)
```

**Example**:
```
 +10000110000000123
```

#### 4. R1R115 - Set User Mapping
**Purpose**: Maps set IDs to user codes

**Data Format**:
```
Position 1:    Space
Position 2:    '+'
Position 3-14:  SET_ID (12 chars)
Position 17-20: USERCD (4 chars)
```

**Example**:
```
 +SET001         USR1
```

#### 5. R1R913 - Account Set Mapping
**Purpose**: Maps accounts to sets

**Data Format**:
```
Position 1:    Space
Position 2:    '+'
Position 3-24:  ACCT_NO (22 chars)
Position 28-39: SET_ID (12 chars)
```

**Example**:
```
 +10000110000000123   SET001
```

### File Header

**First Line**: Contains the reporting date
```
Position 1:   Space
Position 2-9: WALKDATE (8 chars) - DDMMYYYY format
```

**Example**:
```
 31012025
```

### Complete File Example
```
 31012025
 1R1GL4000
 +10000110000000123     D    1,234,567.89
 +20000220000000456     C      567,890.12
 1R1GL4100
 +10000110000000123     31012025         123,456.78       D
 +30000330000000789     30012025         -45,678.90       C
 1R1KEY913
 +10000110000000123
 +20000220000000456
 1R1R115
 +SET001         USR1
 +SET002         USR2
 1R1R913
 +10000110000000123   SET001
 +20000220000000456   SET002
```

---

## Processing Logic

### Phase 1: Date Calculation

**Calculate Current Reporting Date (NDATE)**:
```python
today = datetime.now()
dd = today.day

if dd < 8:
    reptdate = last_day_of_previous_month
elif dd < 15:
    reptdate = datetime(year, month, 8)
elif dd < 22:
    reptdate = datetime(year, month, 15)
else:
    reptdate = datetime(year, month, 22)
```

**Examples**:
| Today's Date | Reporting Date |
|--------------|----------------|
| Feb 5, 2025  | Jan 31, 2025   |
| Feb 10, 2025 | Feb 8, 2025    |
| Feb 18, 2025 | Feb 15, 2025   |
| Feb 25, 2025 | Feb 22, 2025   |

### Phase 2: Extract Walker Date

Read first line of Walker file:
```python
first_line = file.readline()
walkdate = first_line[1:9]  # Position 2-9
reptdate = parse_date(walkdate, 'DDMMYYYY')
```

Save REPTDATE to both libraries:
- `ALWZ/REPTDATE.parquet`
- `ALW/REPTDATE.parquet`

### Phase 3: Date Validation

**Validation Rule**:
```
IF NDATE == RDATE:
    Proceed with conversion
ELSE:
    Print warning and abort
```

**Success Output**:
```
✓ Dates match: 31012025
  Proceeding with conversion...
```

**Failure Output**:
```
✗ Date mismatch!
  Expected: 31012025
  Found:    30012025

WARNING: WALKER SOURCE IS NOT AS AT CURRENT REPORTING DATE

Conversion aborted.
```

### Phase 4: Parse Walker File

**Algorithm**:
```python
setid = None

for each line in file:
    id_char = line[1]  # Position 2
    
    if id_char == '1':
        # Header record - set current SETID
        setid = line[2:10].strip()
    
    elif id_char == '+' and setid:
        # Data record - parse based on SETID
        if setid == 'R1GL4000':
            parse_r1gl4000(line)
        elif setid == 'R1GL4100':
            parse_r1gl4100(line)
        # ... etc
```

**Field Extraction**:

For R1GL4000:
```python
acct_no = line[2:24].strip()      # Position 3-24
ackind = line[28:29]              # Position 29
glamt_str = line[30:52].strip()   # Position 31-51
glamt = float(glamt_str.replace(',', ''))
```

### Phase 5: Sort and Save

Each dataset is sorted according to specifications:

**R1GL4000**: Sort by ACCT_NO, ACKIND, GLAMT
**R1GL4100**: Sort by ACCT_NO, EFFDATE, JEAMT, ACKIND
**R1KEY913**: Sort by ACCT_NO
**R1R115**: Sort by SET_ID
**R1R913**: Sort by ACCT_NO, SET_ID

Then saved as parquet files.

---

## Output Files

### 1. REPTDATE Files

**Location**:
- `output/alwz/REPTDATE.parquet`
- `output/alw/REPTDATE.parquet`

**Schema**:
```
REPTDATE: Date  # Report date extracted from Walker file
```

**Purpose**: Store reporting date for validation in downstream processes

### 2. R1GL4000.parquet

**Location**: `output/alw/R1GL4000.parquet`

**Schema**:
```
ACCT_NO: Utf8      # Account number (22 chars)
ACKIND:  Utf8      # Account kind (1 char)
GLAMT:   Float64   # General ledger amount
```

**Sort Order**: ACCT_NO, ACKIND, GLAMT

**Purpose**: General ledger balances by account

### 3. R1GL4100.parquet

**Location**: `output/alw/R1GL4100.parquet`

**Schema**:
```
ACCT_NO:  Utf8      # Account number
EFFDATE:  Date      # Effective date
JEAMT:    Float64   # Journal entry amount
ACKIND:   Utf8      # Account kind
```

**Sort Order**: ACCT_NO, EFFDATE, JEAMT, ACKIND

**Purpose**: Journal entries with dates

### 4. R1KEY913.parquet

**Location**: `output/alw/R1KEY913.parquet`

**Schema**:
```
ACCT_NO: Utf8      # Account number
```

**Sort Order**: ACCT_NO

**Purpose**: Account identifier list

### 5. R1R115.parquet

**Location**: `output/alw/R1R115.parquet`

**Schema**:
```
SET_ID:  Utf8      # Set identifier (12 chars)
USERCD:  Utf8      # User code (4 chars)
```

**Sort Order**: SET_ID

**Purpose**: Set-to-user mappings

### 6. R1R913.parquet

**Location**: `output/alw/R1R913.parquet`

**Schema**:
```
ACCT_NO: Utf8      # Account number
SET_ID:  Utf8      # Set identifier
```

**Sort Order**: ACCT_NO, SET_ID

**Purpose**: Account-to-set mappings

---

## Date Validation

### Importance

Date validation ensures:
1. Walker data is current for the reporting period
2. Prevents processing stale data
3. Maintains data integrity across systems

### Validation Process

```
Step 1: Calculate expected date (NDATE)
        Based on current day of month

Step 2: Extract actual date (RDATE)
        From Walker file header

Step 3: Compare dates
        IF NDATE == RDATE:
            ✓ PASS - Continue processing
        ELSE:
            ✗ FAIL - Abort with warning
```

### Error Handling

**When validation fails**:
1. Display error message
2. Show expected vs actual dates
3. Exit with code 1
4. Do not create output files

**Example Error**:
```
======================================================================
DATE VALIDATION
======================================================================
✗ Date mismatch!
  Expected: 31012025
  Found:    30012025

WARNING: WALKER SOURCE IS NOT AS AT CURRENT REPORTING DATE

Conversion aborted.
```

---

## Installation & Usage

### Prerequisites

**Python Version**: 3.9+

**Required Libraries**:
```bash
pip install polars>=0.19.0
```

### Directory Setup

```
project/
├── walker_converter.py
├── generate_walker_data.py
├── README_WALKER.md
└── data/
    ├── input/
    │   └── walker/
    │       └── WALALW.txt
    └── output/
        ├── alwz/
        │   └── REPTDATE.parquet
        └── alw/
            ├── REPTDATE.parquet
            ├── R1GL4000.parquet
            ├── R1GL4100.parquet
            ├── R1KEY913.parquet
            ├── R1R115.parquet
            └── R1R913.parquet
```

### Quick Start

#### 1. Generate Sample Data
```bash
python generate_walker_data.py
```

**Output**:
```
======================================================================
Walker Data Generator
======================================================================

Report Date: 31/01/2025
Output File: ./data/input/walker/WALALW.txt

Generating R1GL4000 section (50 records)...
Generating R1GL4100 section (40 records)...
Generating R1KEY913 section (30 records)...
Generating R1R115 section (10 records)...
Generating R1R913 section (25 records)...

======================================================================
Generation Complete!
======================================================================
File created: ./data/input/walker/WALALW.txt
File size:    12,345 bytes

Records generated:
  R1GL4000: 50
  R1GL4100: 40
  R1KEY913: 30
  R1R115:   10
  R1R913:   25
  Total:    155
```

#### 2. Inspect Sample Data
```bash
python generate_walker_data.py --inspect
```

#### 3. Run Converter
```bash
python walker_converter.py
```

**Expected Output**:
```
======================================================================
WALKER DATA EXTRACTION
======================================================================

Current reporting date (NDATE): 31012025
Walker file date (RDATE):       31012025

REPTDATE saved: 31012025
  ALWZ: output/alwz/REPTDATE.parquet
  ALW:  output/alw/REPTDATE.parquet

======================================================================
DATE VALIDATION
======================================================================
✓ Dates match: 31012025
  Proceeding with conversion...

======================================================================
CONVERTING WALKER FILE
======================================================================

Parsing Walker file: data/input/walker/WALALW.txt
Total lines processed: 156
Records extracted:
  R1GL4000: 50
  R1GL4100: 40
  R1KEY913: 30
  R1R115:   10
  R1R913:   25

Sorting and saving datasets...
  R1GL4000: 50 records -> output/alw/R1GL4000.parquet
  R1GL4100: 40 records -> output/alw/R1GL4100.parquet
  R1KEY913: 30 records -> output/alw/R1KEY913.parquet
  R1R115:   10 records -> output/alw/R1R115.parquet
  R1R913:   25 records -> output/alw/R1R913.parquet

======================================================================
CONVERSION COMPLETE
======================================================================

======================================================================
PROCESSING SUMMARY
======================================================================
Report Date:  31012025

Output files generated:
  REPTDATE:  output/alwz/REPTDATE.parquet
  REPTDATE:  output/alw/REPTDATE.parquet
  R1GL4000:  output/alw/R1GL4000.parquet
  R1GL4100:  output/alw/R1GL4100.parquet
  R1KEY913:  output/alw/R1KEY913.parquet
  R1R115:    output/alw/R1R115.parquet
  R1R913:    output/alw/R1R913.parquet
======================================================================
```

### Production Usage

#### Update Paths

Edit `walker_converter.py`:
```python
# Change from:
BASE_PATH = Path("/data")

# To:
BASE_PATH = Path("/production/data")
```

#### Schedule Execution

**Linux/Unix (Cron)**:
```bash
# Run daily at 6 AM
0 6 * * * /usr/bin/python /prod/scripts/walker_converter.py >> /prod/logs/walker.log 2>&1
```

**Windows (Task Scheduler)**:
- Program: `python.exe`
- Arguments: `C:\prod\scripts\walker_converter.py`
- Schedule: Daily, 6:00 AM

---

## Testing

### Unit Tests

#### Test Date Calculation
```python
def test_date_calculation():
    # Mock date: Feb 5, 2025
    date = calculate_current_reporting_date()
    assert date == datetime(2025, 1, 31)
    
    # Mock date: Feb 10, 2025
    date = calculate_current_reporting_date()
    assert date == datetime(2025, 2, 8)
```

#### Test Walker Date Extraction
```python
def test_extract_walker_date():
    # Create test file with date
    with open('test_walker.txt', 'w') as f:
        f.write(' 31012025\n')
    
    date = extract_walker_date('test_walker.txt')
    assert date == datetime(2025, 1, 31)
```

#### Test Field Parsing
```python
def test_parse_r1gl4000():
    line = " +10000110000000123     D    1,234,567.89\n"
    # Parse and verify
    acct_no = line[2:24].strip()
    assert acct_no == "10000110000000123"
    
    ackind = line[28:29]
    assert ackind == "D"
    
    glamt_str = line[30:52].strip()
    glamt = float(glamt_str.replace(',', ''))
    assert glamt == 1234567.89
```

### Integration Tests

#### End-to-End Test
```python
def test_full_pipeline():
    # Generate sample data
    generate_walker_file()
    
    # Run converter
    process()
    
    # Verify outputs
    assert REPTDATE_ALWZ.exists()
    assert REPTDATE_ALW.exists()
    assert R1GL4000.exists()
    assert R1GL4100.exists()
    assert R1KEY913.exists()
    assert R1R115.exists()
    assert R1R913.exists()
    
    # Verify content
    df = pl.read_parquet(R1GL4000)
    assert len(df) > 0
    assert 'ACCT_NO' in df.columns
```

### Validation Tests

#### Date Consistency
```python
def test_date_consistency():
    # Both REPTDATE files should have same date
    df_alwz = pl.read_parquet(REPTDATE_ALWZ)
    df_alw = pl.read_parquet(REPTDATE_ALW)
    
    assert df_alwz['REPTDATE'][0] == df_alw['REPTDATE'][0]
```

#### Sort Order
```python
def test_sort_order():
    df = pl.read_parquet(R1GL4000)
    
    # Verify sorted by ACCT_NO, ACKIND, GLAMT
    sorted_df = df.sort(['ACCT_NO', 'ACKIND', 'GLAMT'])
    assert df.equals(sorted_df)
```

---

## Troubleshooting

### Common Issues

#### Issue 1: Date Mismatch
```
✗ Date mismatch!
  Expected: 31012025
  Found:    30012025
```

**Cause**: Walker file not current for reporting period

**Solution**:
1. Check Walker file date in header
2. Verify file is from correct date
3. Regenerate Walker file if needed
4. Adjust reporting date if appropriate

#### Issue 2: File Not Found
```
ERROR: Could not extract date from Walker file: [Errno 2] No such file or directory
```

**Cause**: Input file missing

**Solution**:
1. Verify file path: `data/input/walker/WALALW.txt`
2. Check file permissions
3. Generate sample data: `python generate_walker_data.py`

#### Issue 3: Parse Error
```
ERROR: Could not parse date from Walker file: invalid literal for int()
```

**Cause**: Invalid date format in file header

**Solution**:
1. Check first line format: ` DDMMYYYY`
2. Verify date is exactly 8 digits
3. Ensure space at position 1

#### Issue 4: Empty Datasets
```
R1GL4000: 0 records (empty)
```

**Cause**: No data for that record type in Walker file

**Solution**:
- This is normal if Walker file doesn't contain that type
- Empty datasets will still be created with correct schema
- Check Walker file contains expected SETID headers

#### Issue 5: Amount Parsing Error
```
ValueError: could not convert string to float
```

**Cause**: Invalid amount format

**Solution**:
1. Check amount field positions match specification
2. Verify COMMA format (e.g., "1,234.56")
3. Handle missing/blank amounts as 0.0

### Debug Mode

Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Validation Checklist

Before processing:
- [ ] Walker file exists at correct path
- [ ] First line contains valid date
- [ ] File contains header records (type '1')
- [ ] File contains data records (type '+')
- [ ] Date matches expected reporting period

After processing:
- [ ] All output files created
- [ ] REPTDATE files contain correct date
- [ ] Record counts match expectations
- [ ] Datasets are properly sorted
- [ ] No parsing errors in log

---

## Appendix

### A. Field Position Reference

Quick reference for all field positions:

**R1GL4000**:
- ACCT_NO: 3-24 (22)
- ACKIND: 29 (1)
- GLAMT: 31-51 (21)

**R1GL4100**:
- ACCT_NO: 3-24 (22)
- EFFDATE: 30-37 (8)
- JEAMT: 47-67 (21)
- ACKIND: 75 (1)

**R1KEY913**:
- ACCT_NO: 3-24 (22)

**R1R115**:
- SET_ID: 3-14 (12)
- USERCD: 17-20 (4)

**R1R913**:
- ACCT_NO: 3-24 (22)
- SET_ID: 28-39 (12)

### B. Sample Walker File

Complete minimal example:
```
 31012025
 1R1GL4000
 +10000110000000001     D    1,000,000.00
 +20000220000000002     C      500,000.00
 1R1GL4100
 +10000110000000001     31012025         100,000.00       D
 1R1KEY913
 +10000110000000001
 1R1R115
 +SET001         USR1
 1R1R913
 +10000110000000001   SET001
```

### C. Data Flow Diagram

```
WALALW.txt
    │
    ├─ Extract REPTDATE (line 1)
    │  └─> REPTDATE.parquet (ALWZ & ALW)
    │
    └─ Parse Data Records
       │
       ├─ R1GL4000 → Sort → R1GL4000.parquet
       ├─ R1GL4100 → Sort → R1GL4100.parquet
       ├─ R1KEY913 → Sort → R1KEY913.parquet
       ├─ R1R115   → Sort → R1R115.parquet
       └─ R1R913   → Sort → R1R913.parquet
```

---

**Document Version**: 1.0  
**Last Updated**: February 10, 2025  
**Program**: Walker Data Extraction (EIGWRD1W)
