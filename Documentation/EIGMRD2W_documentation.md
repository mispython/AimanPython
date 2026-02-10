# EIGMRD2W: Walker Data Extraction System - Comprehensive Documentation

## Complete Guide for ALW (R1) and ALM (R2) Processing

---

## Table of Contents
1. [Overview](#overview)
2. [System Comparison](#system-comparison)
3. [Input File Formats](#input-file-formats)
4. [Processing Logic](#processing-logic)
5. [Output Files](#output-files)
6. [Installation & Usage](#installation--usage)
7. [Testing](#testing)
8. [Troubleshooting](#troubleshooting)

---

## Overview

### Purpose
The Walker Data Extraction System processes two types of Walker format files:
- **ALW (R1 series)**: Allowance data from EIGWRD1W
- **ALM (R2 series)**: Asset/Liability Management data from EIGMRD2W

Both systems validate reporting dates and extract data into typed parquet datasets.

### Key Differences

| Feature | ALW (R1) | ALM (R2) |
|---------|----------|----------|
| **SAS Program** | EIGWRD1W | EIGMRD2W |
| **Input File** | WALALW.txt | WALALM.txt |
| **Date Logic** | Multi-period (days 8,15,22 or last) | Last day of previous month only |
| **Datasets** | 5 datasets (R1GL4000, R1GL4100, R1KEY913, R1R115, R1R913) | 4 datasets (R2GL4000, R2KEY913, R2R115, R2R913) |
| **R1GL4000** | Has R1GL4100 with EFFDATE | No equivalent |
| **Position @31** | GLAMT starts at 31 | GLAMT starts at 30 |
| **Output Libraries** | ALWZ + ALW | ALM only |

---

## System Comparison

### ALW System (R1 Series)

**Date Calculation**:
```python
dd = today.day
if dd < 8:
    reptdate = last_day_of_previous_month
elif dd < 15:
    reptdate = 8th_of_current_month
elif dd < 22:
    reptdate = 15th_of_current_month
else:
    reptdate = 22nd_of_current_month
```

**Datasets**:
1. R1GL4000 - GL balances (ACCT_NO, ACKIND, GLAMT)
2. R1GL4100 - Journal entries (ACCT_NO, EFFDATE, JEAMT, ACKIND)
3. R1KEY913 - Account keys (ACCT_NO)
4. R1R115 - Set-user mapping (SET_ID, USERCD)
5. R1R913 - Account-set mapping (ACCT_NO, SET_ID)

**Output Libraries**:
- ALWZ/REPTDATE.parquet
- ALW/REPTDATE.parquet
- ALW/R1GL4000.parquet
- ALW/R1GL4100.parquet
- ALW/R1KEY913.parquet
- ALW/R1R115.parquet
- ALW/R1R913.parquet

### ALM System (R2 Series)

**Date Calculation**:
```python
# Always last day of previous month
first_of_month = datetime(year, month, 1)
reptdate = first_of_month - timedelta(days=1)
```

**Datasets**:
1. R2GL4000 - GL balances (ACCT_NO, ACKIND, GLAMT)
2. R2KEY913 - Account keys (ACCT_NO)
3. R2R115 - Set-user mapping (SET_ID, USERCD)
4. R2R913 - Account-set mapping (ACCT_NO, SET_ID)

**Output Library**:
- ALM/REPTDATE.parquet
- ALM/R2GL4000.parquet
- ALM/R2KEY913.parquet
- ALM/R2R115.parquet
- ALM/R2R913.parquet

---

## Input File Formats

### Common Structure

Both ALW and ALM files share the same overall structure:

**Record Type '1'** (Header):
```
Position 1:   Space
Position 2:   '1'
Position 3-10: SETID (8 characters)
```

**Record Type '+'** (Data):
```
Position 1:   Space
Position 2:   '+'
Position 3+:  Data fields (varies by SETID)
```

**File Header** (First Line):
```
Position 1:   Space
Position 2-9: WALKDATE (DDMMYYYY)
```

### ALW (R1) Specific Formats

#### R1GL4000
```
Position 3-24:  ACCT_NO (22 chars)
Position 29:    ACKIND (1 char)
Position 31-51: GLAMT (21 chars) ← starts at 31
```

Example:
```
 +10000110000000123     D    1,234,567.89
```

#### R1GL4100 (Unique to ALW)
```
Position 3-24:  ACCT_NO (22 chars)
Position 30-37: EFFDATE (8 chars DDMMYYYY)
Position 47-67: JEAMT (21 chars)
Position 75:    ACKIND (1 char)
```

Example:
```
 +10000110000000123     31012025         123,456.78       D
```

### ALM (R2) Specific Formats

#### R2GL4000
```
Position 3-24:  ACCT_NO (22 chars)
Position 29:    ACKIND (1 char)
Position 30-50: GLAMT (21 chars) ← starts at 30 (different from R1!)
```

Example:
```
 +10000110000000123     D1,234,567.89
```

**Critical Difference**: R2GL4000 has GLAMT starting at position 30, while R1GL4000 has it at position 31 (one position difference).

### Common Formats (R1 & R2)

#### KEY913 Records
```
Position 3-24: ACCT_NO (22 chars)
```

#### R115 Records
```
Position 3-14:  SET_ID (12 chars)
Position 17-20: USERCD (4 chars)
```

#### R913 Records
```
Position 3-24:  ACCT_NO (22 chars)
Position 28-39: SET_ID (12 chars)
```

---

## Processing Logic

### Phase 1: Date Calculation

**ALW (R1)**:
```
Examples for February 2025:
- Feb 5:  Report date = Jan 31 (last day of previous month)
- Feb 10: Report date = Feb 8
- Feb 18: Report date = Feb 15
- Feb 25: Report date = Feb 22
```

**ALM (R2)**:
```
Examples:
- Any day in February 2025: Report date = Jan 31
- Any day in March 2025:    Report date = Feb 28/29
```

### Phase 2: Date Extraction & Validation

Both systems:
1. Read first line of Walker file
2. Extract date from positions 2-9
3. Parse as DDMMYYYY
4. Compare with calculated date
5. Abort if mismatch

### Phase 3: Data Parsing

**Common Algorithm**:
```python
setid = None

for line in file:
    id_char = line[1]
    
    if id_char == '1':
        setid = line[2:10].strip()
    
    elif id_char == '+' and setid:
        if setid == 'R1GL4000' or 'R2GL4000':
            parse_gl4000(line)
        # ... etc
```

### Phase 4: Sorting & Saving

**Sort Specifications**:

| Dataset | Sort Order |
|---------|------------|
| R1/R2GL4000 | ACCT_NO, ACKIND, GLAMT |
| R1GL4100 | ACCT_NO, EFFDATE, JEAMT, ACKIND |
| R1/R2KEY913 | ACCT_NO |
| R1/R2R115 | SET_ID |
| R1/R2R913 | ACCT_NO, SET_ID |

---

## Output Files

### ALW (R1) Outputs

#### 1. REPTDATE (2 copies)
**Locations**:
- `output/alwz/REPTDATE.parquet`
- `output/alw/REPTDATE.parquet`

**Schema**:
```
REPTDATE: Date
```

#### 2. R1GL4000
**Location**: `output/alw/R1GL4000.parquet`

**Schema**:
```
ACCT_NO: Utf8
ACKIND:  Utf8
GLAMT:   Float64
```

**Sort**: ACCT_NO, ACKIND, GLAMT

#### 3. R1GL4100
**Location**: `output/alw/R1GL4100.parquet`

**Schema**:
```
ACCT_NO: Utf8
EFFDATE: Date
JEAMT:   Float64
ACKIND:  Utf8
```

**Sort**: ACCT_NO, EFFDATE, JEAMT, ACKIND

#### 4. R1KEY913
**Location**: `output/alw/R1KEY913.parquet`

**Schema**:
```
ACCT_NO: Utf8
```

**Sort**: ACCT_NO

#### 5. R1R115
**Location**: `output/alw/R1R115.parquet`

**Schema**:
```
SET_ID: Utf8
USERCD: Utf8
```

**Sort**: SET_ID

#### 6. R1R913
**Location**: `output/alw/R1R913.parquet`

**Schema**:
```
ACCT_NO: Utf8
SET_ID:  Utf8
```

**Sort**: ACCT_NO, SET_ID

### ALM (R2) Outputs

#### 1. REPTDATE
**Location**: `output/alm/REPTDATE.parquet`

**Schema**:
```
REPTDATE: Date
```

#### 2-5. R2GL4000, R2KEY913, R2R115, R2R913

Same schemas as R1 equivalents, stored in `output/alm/` directory.

---

## Installation & Usage

### Prerequisites

```bash
pip install polars>=0.19.0
```

### Directory Structure

```
project/
├── walker_converter.py              # ALW (R1) converter
├── alm_walker_converter.py          # ALM (R2) converter
├── generate_walker_comprehensive.py # Generator for both
├── README_WALKER_COMPLETE.md        # This file
└── data/
    ├── input/
    │   └── walker/
    │       ├── WALALW.txt           # ALW input
    │       └── WALALM.txt           # ALM input
    └── output/
        ├── alwz/
        │   └── REPTDATE.parquet
        ├── alw/
        │   ├── REPTDATE.parquet
        │   ├── R1GL4000.parquet
        │   ├── R1GL4100.parquet
        │   ├── R1KEY913.parquet
        │   ├── R1R115.parquet
        │   └── R1R913.parquet
        └── alm/
            ├── REPTDATE.parquet
            ├── R2GL4000.parquet
            ├── R2KEY913.parquet
            ├── R2R115.parquet
            └── R2R913.parquet
```

### Quick Start

#### 1. Generate Sample Data
```bash
python generate_walker_comprehensive.py
```

**Output**:
```
======================================================================
COMPREHENSIVE WALKER DATA GENERATOR
======================================================================

Report Date: 31/01/2025 (31012025)
Output Directory: ./data/input/walker

======================================================================
Generating ALW Walker File (R1 series)
======================================================================
Report Date: 31/01/2025
Output File: ./data/input/walker/WALALW.txt

Generating R1GL4000 section (50 records)...
Generating R1GL4100 section (40 records)...
Generating R1KEY913 section (30 records)...
Generating R1R115 section (10 records)...
Generating R1R913 section (25 records)...

✓ ALW file created: ./data/input/walker/WALALW.txt
  Size: 12,345 bytes
  Total records: 155

======================================================================
Generating ALM Walker File (R2 series)
======================================================================
Report Date: 31/01/2025
Output File: ./data/input/walker/WALALM.txt

Generating R2GL4000 section (45 records)...
Generating R2KEY913 section (28 records)...
Generating R2R115 section (8 records)...
Generating R2R913 section (22 records)...

✓ ALM file created: ./data/input/walker/WALALM.txt
  Size: 9,876 bytes
  Total records: 103

======================================================================
GENERATION COMPLETE
======================================================================

Generated files:
  ALW (R1): ./data/input/walker/WALALW.txt
  ALM (R2): ./data/input/walker/WALALM.txt
```

#### 2. Inspect Sample Data
```bash
python generate_walker_comprehensive.py --inspect
```

#### 3. Process ALW Data
```bash
python walker_converter.py
```

#### 4. Process ALM Data
```bash
python alm_walker_converter.py
```

### Expected Output

**ALW Processing**:
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
```

**ALM Processing**:
```
======================================================================
WALKER ALM DATA EXTRACTION
======================================================================

Current reporting date (NDATE): 31012025
Walker ALM file date (RDATE):   31012025

REPTDATE saved: 31012025
  ALM: output/alm/REPTDATE.parquet

======================================================================
DATE VALIDATION
======================================================================
✓ Dates match: 31012025
  Proceeding with conversion...

======================================================================
CONVERTING WALKER ALM FILE
======================================================================

Parsing Walker ALM file: data/input/walker/WALALM.txt
Total lines processed: 104
Records extracted:
  R2GL4000: 45
  R2KEY913: 28
  R2R115:   8
  R2R913:   22

Sorting and saving datasets...
  R2GL4000: 45 records -> output/alm/R2GL4000.parquet
  R2KEY913: 28 records -> output/alm/R2KEY913.parquet
  R2R115:   8 records -> output/alm/R2R115.parquet
  R2R913:   22 records -> output/alm/R2R913.parquet
```

---

## Testing

### Unit Tests

#### Test Date Calculations

**ALW**:
```python
def test_alw_date_calculation():
    # Feb 5 → Jan 31
    assert calculate_date(datetime(2025, 2, 5)) == datetime(2025, 1, 31)
    
    # Feb 10 → Feb 8
    assert calculate_date(datetime(2025, 2, 10)) == datetime(2025, 2, 8)
```

**ALM**:
```python
def test_alm_date_calculation():
    # Any day in Feb → Jan 31
    assert calculate_date(datetime(2025, 2, 5)) == datetime(2025, 1, 31)
    assert calculate_date(datetime(2025, 2, 25)) == datetime(2025, 1, 31)
```

#### Test Field Parsing

**R1GL4000** (Position 31):
```python
line = " +10000110000000123     D    1,234,567.89\n"
glamt_str = line[30:52].strip()  # Position 31-51
assert float(glamt_str.replace(',', '')) == 1234567.89
```

**R2GL4000** (Position 30):
```python
line = " +10000110000000123     D1,234,567.89\n"
glamt_str = line[29:51].strip()  # Position 30-50
assert float(glamt_str.replace(',', '')) == 1234567.89
```

### Integration Tests

```python
def test_alw_pipeline():
    generate_alw_file()
    process_alw()
    
    # Verify outputs
    assert Path("output/alwz/REPTDATE.parquet").exists()
    assert Path("output/alw/R1GL4000.parquet").exists()
    assert Path("output/alw/R1GL4100.parquet").exists()

def test_alm_pipeline():
    generate_alm_file()
    process_alm()
    
    # Verify outputs
    assert Path("output/alm/REPTDATE.parquet").exists()
    assert Path("output/alm/R2GL4000.parquet").exists()
```

---

## Troubleshooting

### Common Issues

#### Issue 1: Position Mismatch in Amounts

**Symptom**: R2GL4000 amounts parsed incorrectly

**Cause**: Using R1 position (31) instead of R2 position (30)

**Solution**: 
```python
# R1GL4000
glamt_str = line[30:52]  # Position 31

# R2GL4000
glamt_str = line[29:51]  # Position 30
```

#### Issue 2: Wrong Date for ALM

**Symptom**: ALM converter expects Feb 8, but file has Jan 31

**Cause**: Using ALW date logic instead of ALM logic

**Solution**:
- ALW: Multiple periods per month
- ALM: Always last day of previous month

#### Issue 3: Missing R1GL4100 in ALM

**Symptom**: Error looking for R1GL4100 in ALM file

**Cause**: R1GL4100 only exists in ALW, not ALM

**Solution**: ALM only has 4 datasets (no GL4100)

#### Issue 4: REPTDATE in Wrong Location

**Symptom**: Can't find REPTDATE for downstream processing

**Cause**: Looking in wrong library

**Solution**:
- ALW: Check both ALWZ and ALW
- ALM: Check ALM only

### Debugging Tips

1. **Inspect generated files**:
   ```bash
   python generate_walker_comprehensive.py --inspect
   ```

2. **Check first few lines**:
   ```bash
   head -20 data/input/walker/WALALW.txt
   head -20 data/input/walker/WALALM.txt
   ```

3. **Verify field positions**:
   Use a hex editor or Python to check exact positions

4. **Enable verbose logging**:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

---

## Appendix

### A. Field Position Quick Reference

**R1GL4000 vs R2GL4000**:
```
R1GL4000:
Pos 3-24:  ACCT_NO
Pos 29:    ACKIND
Pos 31-51: GLAMT ← Position 31

R2GL4000:
Pos 3-24:  ACCT_NO
Pos 29:    ACKIND
Pos 30-50: GLAMT ← Position 30 (1 char earlier)
```

### B. Complete File Examples

**ALW File**:
```
 31012025
 1R1GL4000
 +10000110000000123     D    1,234,567.89
 1R1GL4100
 +10000110000000123     31012025         123,456.78       D
 1R1KEY913
 +10000110000000123
 1R1R115
 +SET001         USR1
 1R1R913
 +10000110000000123   SET001
```

**ALM File**:
```
 31012025
 1R2GL4000
 +10000110000000123     D1,234,567.89
 1R2KEY913
 +10000110000000123
 1R2R115
 +SET001         USR1
 1R2R913
 +10000110000000123   SET001
```

### C. Processing Workflow

```
ALW (R1):
WALALW.txt → walker_converter.py → {ALWZ, ALW} libraries

ALM (R2):
WALALM.txt → alm_walker_converter.py → ALM library

Both can be generated with:
generate_walker_comprehensive.py
```

---

**Document Version**: 1.0  
**Last Updated**: February 10, 2025  
**Programs**: EIGWRD1W (ALW/R1) and EIGMRD2W (ALM/R2)
