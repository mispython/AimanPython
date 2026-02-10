# EIBMMTH1 - Monthly RDAL Processing System

## Comprehensive Documentation

---

## Table of Contents
1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Input Files](#input-files)
4. [Processing Flow](#processing-flow)
5. [Output Files](#output-files)
6. [Date Validation](#date-validation)
7. [Business Logic](#business-logic)
8. [Installation & Setup](#installation--setup)
9. [Usage](#usage)
10. [Testing](#testing)
11. [Troubleshooting](#troubleshooting)

---

## Overview

### Purpose
The EIBMMTH1 system processes monthly banking regulatory data for Bank Negara Malaysia (BNM) reporting. It consolidates multiple data sources, validates data integrity, and generates standardized RDAL (Regulatory Data Analysis Layer) reports.

### Key Features
- **Multi-source data integration**: Consolidates 10+ data sources
- **Date validation**: Ensures all inputs are dated correctly
- **Error handling**: Aborts processing if validation fails
- **Report generation**: Creates FISS and NSRS format reports
- **FTP automation**: Generates upload scripts for DRR system

### Processing Schedule
- **Frequency**: Monthly
- **Trigger dates**: 8th, 15th, 22nd, or last day of month
- **Reports generated**: Week 1-4 of each month

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    INPUT DATA SOURCES                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │  RDAL1   │  │  RGAC    │  │  LOANS   │  │ DEPOSITS │      │
│  │  (ALW)   │  │  (GAY)   │  │ (MNILN)  │  │ (MNITB)  │      │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘      │
│       │             │              │             │            │
│  ┌────┴─────┐  ┌───┴──────┐  ┌───┴──────┐  ┌──┴───────┐    │
│  │   FD     │  │ KAPITI1  │  │ KAPITI2  │  │ KAPITI3  │    │
│  │ (MNIFD)  │  │  (BNM)   │  │  (BNM)   │  │  (BNM)   │    │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    │
│       │             │              │             │            │
└───────┼─────────────┼──────────────┼─────────────┼────────────┘
        │             │              │             │
        └─────────────┴──────────────┴─────────────┘
                      │
                      ▼
        ┌─────────────────────────────────┐
        │   DATE VALIDATION ENGINE        │
        │   - Verify all dates match      │
        │   - Report discrepancies        │
        │   - Abort if invalid            │
        └─────────────┬───────────────────┘
                      │
                      ▼
        ┌─────────────────────────────────┐
        │   PROCESSING ENGINE             │
        ├─────────────────────────────────┤
        │  1. EIGWRD1W  - RDAL1 Extract   │
        │  2. EIGMRGCW  - RGAC Merge      │
        │  3. WALWPBBP  - Domestic A/L    │
        │  4. EIBRDL1B  - RDAL Step 1     │
        │  5. EIBRDL2B  - RDAL Step 2     │
        │  6. KALMLIFE  - Life Insurance  │
        │  7. EIBWRDLB  - RDAL Write      │
        │  8. PBBRDAL1  - PBB RDAL 1      │
        │  9. PBBRDAL2  - PBB RDAL 2      │
        │ 10. PBBELP    - ELP Processing  │
        │ 11. PBBBRELP  - BR ELP          │
        │ 12. EIGWRDAL  - Write RDAL      │
        │ 13. PBBALP    - ALP Processing  │
        └─────────────┬───────────────────┘
                      │
        ┌─────────────┴───────────────────┐
        │                                 │
        ▼                                 ▼
┌──────────────────┐           ┌─────────────────┐
│  FISS_RDAL.txt   │           │  NSRS_RDAL.txt  │
│                  │           │                 │
│  - AL Section    │           │  - AL Section   │
│  - OB Section    │           │  - OB Section   │
│  - SP Section    │           │  - SP Section   │
└──────────────────┘           └─────────────────┘
        │                                 │
        └─────────────┬───────────────────┘
                      │
                      ▼
              ┌───────────────┐
              │ FTP Commands  │
              │ (DRR Upload)  │
              └───────────────┘
```

---

## Input Files

### 1. RDAL1 (ALW) - Allowance Data
**File**: `rdal1_text.parquet`  
**Location**: `input/fiss/`

**Schema**:
```
REPTDATE  : Date      # Report date
ITCODE    : Utf8      # 14-character item code
AMTIND    : Utf8      # Amount indicator (D/I/F/ )
AMOUNT    : Float64   # Transaction amount
```

**Purpose**: Primary regulatory data extract for domestic assets and liabilities.

**Volume**: ~500-1,000 records per reporting period

### 2. RGAC (GAY) - Global Assets and Capital
**File**: `rgac_text.parquet`  
**Location**: `input/rgac/`

**Schema**: Same as RDAL1

**Purpose**: Global and consolidated assets reporting.

**Additional Files**:
- `RGAC_lag1.parquet`: Previous period data for comparisons

### 3. Loans (MNILN)
**File**: `MNILN_current.parquet`  
**Location**: `input/mniln/`

**Schema**:
```
REPTDATE      : Date      # Report date
ACCNO         : Utf8      # Account number
LOANTYPE      : Utf8      # Loan product type (HL, PL, etc.)
BALANCE       : Float64   # Outstanding balance
BRANCH        : Utf8      # Branch code
CUSTOMER_TYPE : Utf8      # Customer type (I/C/G)
INTEREST_RATE : Float64   # Interest rate
PZIPCODE      : Int64     # Postal/zip code
```

**Purpose**: Loan portfolio data for CAG (Credit Aggregate) processing.

**Additional Files**:
- `MNILN_lag4.parquet`: Data from 4 months ago

**Volume**: ~1,000-10,000 records

### 4. Fixed Deposits (MNIFD)
**File**: `MNIFD_current.parquet`  
**Location**: `input/mnifd/`

**Schema**:
```
REPTDATE      : Date      # Report date
ACCNO         : Utf8      # Account number
BALANCE       : Float64   # Deposit balance
BRANCH        : Utf8      # Branch code
MATURITY_DATE : Date      # Maturity date
INTEREST_RATE : Float64   # Interest rate
TERM_MONTHS   : Int64     # Term in months
```

**Purpose**: Fixed deposit portfolio data.

**Volume**: ~800-5,000 records

### 5. Deposits (MNITB)
**File**: `MNITB_current.parquet`  
**Location**: `input/mnitb/`

**Schema**:
```
REPTDATE      : Date      # Report date
ACCNO         : Utf8      # Account number
ACCTYPE       : Utf8      # Account type (SA/CA/FD/TD/OD)
BALANCE       : Float64   # Account balance
BRANCH        : Utf8      # Branch code
CUSTOMER_TYPE : Utf8      # Customer type
```

**Purpose**: Deposit account portfolio data.

**Additional Files**:
- `MNITB_lag4.parquet`: Data from 4 months ago

**Volume**: ~2,000-20,000 records

### 6. BNM KAPITI Tables
**Files**: 
- `KAPITI1.parquet` - Capital Adequacy
- `KAPITI2.parquet` - Liquidity
- `KAPITI3.parquet` - Risk Weighted Assets
- `KAPITI_SASDATA.parquet` - Combined reference

**Location**: `input/kapiti/`

**Schema**:
```
REPTDATE  : Date      # Report date
ITEM_CODE : Utf8      # Regulatory item code
AMOUNT    : Float64   # Reported amount
CATEGORY  : Utf8      # Category (CAP/LIQ/RWA)
```

**Purpose**: BNM regulatory framework data.

**Volume**: ~10-50 records per table

---

## Processing Flow

### Phase 1: Initialization & Validation

#### Step 1: Calculate Report Dates
```python
dates = calculate_report_dates()
# Returns:
# - reptdate: Last day of previous month
# - nowk: Week number (1-4)
# - reptmon: Report month (01-12)
# - reptyear: Report year
# - rdate: Formatted date (DDMMYYYY)
```

#### Step 2: Validate Input Dates
```python
is_valid, errors, file_dates = validate_input_dates(dates)

# Checks:
# - ALW date matches RDATE
# - GAY date matches RDATE
# - LOAN date matches RDATE
# - DEPOSIT date matches RDATE
# - FD date matches RDATE
# - KAPITI1 date matches RDATE
# - KAPITI2 date matches RDATE
# - KAPITI3 date matches RDATE

# If any mismatch:
# - Log error message
# - Set is_valid = False
# - Abort processing (ABORT 77)
```

**Example Validation Output**:
```
======================================================================
VALIDATING INPUT DATA DATES
======================================================================
✓ ALW         : 31012025
✓ GAY         : 31012025
✓ LOAN        : 31012025
✓ DEPOSIT     : 31012025
✓ FD          : 31012025
✓ KAPITI1     : 31012025
✓ KAPITI2     : 31012025
✗ KAPITI3     : 30012025

ERROR: THE KAPITI3 EXTRACTION IS NOT DATED 31012025 (found: 30012025)
THE JOB IS NOT DONE !!
```

### Phase 2: Data Processing

#### Processing Chain
```
1. EIGWRD1W  → Process RDAL1 (ALW) data
2. EIGMRGCW  → Merge RGAC (GAY) data
3. WALWPBBP  → Generate Domestic A/L Report
4. Cleanup   → Delete work datasets
5. EIBRDL1B  → RDAL processing step 1B
6. EIBRDL2B  → RDAL processing step 2B
7. KALMLIFE  → Life insurance calculations
8. EIBWRDLB  → Write RDAL intermediate
9. PBBRDAL1  → PBB-specific RDAL part 1
10. PBBRDAL2 → PBB-specific RDAL part 2
11. PBBELP   → ELP (External Liabilities) processing
12. PBBBRELP → Branch ELP processing
13. EIGWRDAL → Write final RDAL files
14. PBBALP   → ALP (Asset/Liability Position) processing
```

### Phase 3: Output Generation

#### FISS RDAL Format
```
RDAL{DD}{MM}{YYYY}
AL
{ITCODE};{AMOUNTD};{AMOUNTI};{AMOUNTF}
...
OB
{ITCODE};{AMOUNTD};{AMOUNTI};{AMOUNTF}
...
SP
{ITCODE};{AMOUNTD};{AMOUNTF}
...
```

#### NSRS RDAL Format
Same structure as FISS, but with different scaling rules for codes starting with '80'.

### Phase 4: FTP Script Generation
```
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
lzopts linerule=$lr
cd "FD-BNM REPORTING/PBB/BNM RPTG"
put //SAP.PBB.FISS.RDAL       "RDAL MTH.TXT"
put //SAP.PBB.NSRS.RDAL       "NSRS MTH.TXT"
EOB
```

---

## Output Files

### 1. FISS_RDAL.txt
**Purpose**: Main RDAL report in FISS format  
**Location**: `output/FISS_RDAL.txt`  
**Format**: Text file with semicolon delimiters

**Sections**:
- **AL**: Asset/Liability items
- **OB**: Off-Balance Sheet items
- **SP**: Special items

### 2. NSRS_RDAL.txt
**Purpose**: RDAL report in NSRS format  
**Location**: `output/NSRS_RDAL.txt`  
**Format**: Text file with semicolon delimiters

**Key Differences from FISS**:
- Different scaling for codes starting with '80'
- Handles negative adjustments differently

### 3. WALKER_RDAL.txt
**Purpose**: Walker format RDAL report  
**Location**: `output/WALKER_RDAL.txt`

### 4. ELIAB_TEXT.txt
**Purpose**: External Liabilities report  
**Location**: `output/ELIAB_TEXT.txt`  
**Format**: Fixed-width (134 characters)

### 5. Intermediate Datasets

#### RDAL1_processed.parquet
Processed RDAL1 data after EIGWRD1W

#### RGAC_processed.parquet
Processed RGAC data after EIGMRGCW

#### TEMP.parquet
Temporary working dataset

#### ALWZ.parquet
Intermediate allowance data

---

## Date Validation

### Validation Logic

The system requires **all** input files to be dated with the same report date. This ensures data consistency across all sources.

**Required Matches**:
1. ALW (RDAL1) date = RDATE
2. GAY (RGAC) date = RDATE
3. LOAN date = RDATE
4. DEPOSIT date = RDATE
5. FD date = RDATE
6. KAPITI1 date = RDATE
7. KAPITI2 date = RDATE
8. KAPITI3 date = RDATE

**Validation Process**:
```python
1. Load each input file
2. Extract REPTDATE from first record
3. Compare with calculated RDATE
4. If mismatch:
   - Log error message
   - Add to error list
5. If any errors:
   - Print all errors
   - Abort processing (exit code 77)
```

### Error Messages

**Format**:
```
THE {SOURCE} EXTRACTION IS NOT DATED {EXPECTED} (found: {ACTUAL})
```

**Example**:
```
THE LOAN EXTRACTION IS NOT DATED 31012025 (found: 30012025)
THE FD EXTRACTION IS NOT DATED 31012025 (found: NOT FOUND)
```

### Abort Behavior

When validation fails:
```
THE JOB IS NOT DONE !!
```

In the original SAS:
```sas
DATA A;
   ABORT 77;
```

In Python:
```python
sys.exit(77)  # or continue with warnings in test mode
```

---

## Business Logic

### Date Calculation Rules

**Report Date**: Last day of previous month

**Week Determination**:
| Day of Month | Week | Start Day | Previous Week |
|--------------|------|-----------|---------------|
| 8            | 1    | 1         | 4             |
| 15           | 2    | 9         | 1             |
| 22           | 3    | 16        | 2             |
| 29/30/31     | 4    | 23        | 3             |

**Example**:
- Today: February 10, 2025
- Report Date: January 31, 2025 (last day of previous month)
- Day = 31 → Week = 4

### Include Programs

The original SAS program includes multiple external programs:

1. **EIGWRD1W**: RDAL1 weekly extraction
2. **EIGMRGCW**: RGAC merge and consolidation
3. **WALWPBBP**: Domestic assets/liabilities report
4. **EIBRDL1B**: RDAL step 1B processing
5. **EIBRDL2B**: RDAL step 2B processing
6. **KALMLIFE**: Life insurance calculations
7. **EIBWRDLB**: RDAL write processing
8. **PBBRDAL1**: PBB RDAL part 1
9. **PBBRDAL2**: PBB RDAL part 2
10. **PBBELP**: External liabilities processing
11. **PBBBRELP**: Branch external liabilities
12. **EIGWRDAL**: Final RDAL file writing
13. **PBBALP**: Asset/liability position

**Implementation Note**: In the Python conversion, these are implemented as separate functions. In production, each would contain the actual business logic from the corresponding SAS include file.

### Dataset Cleanup

After processing, the system deletes intermediate datasets:

**Deleted Datasets**:
- `ALWGL{REPTMON}{NOWK}`: General ledger allowances
- `ALWJE{REPTMON}{NOWK}`: Journal entry allowances

**Example**: For January Week 4:
- Delete `ALWGL014`
- Delete `ALWJE014`

---

## Installation & Setup

### Prerequisites

**Python Version**: 3.9 or higher

**Required Libraries**:
```bash
pip install polars>=0.19.0
pip install duckdb>=0.9.0
pip install pyarrow>=14.0.0
```

### Directory Structure Setup

```
project/
├── eibmmth1_converter.py          # Main converter
├── generate_eibmmth1_data.py      # Sample data generator
├── README_EIBMMTH1.md             # This documentation
├── data/
│   ├── input/
│   │   ├── fiss/
│   │   │   └── rdal1_text.parquet
│   │   ├── rgac/
│   │   │   ├── rgac_text.parquet
│   │   │   └── RGAC_lag1.parquet
│   │   ├── mniln/
│   │   │   ├── MNILN_current.parquet
│   │   │   └── MNILN_lag4.parquet
│   │   ├── mnifd/
│   │   │   └── MNIFD_current.parquet
│   │   ├── mnitb/
│   │   │   ├── MNITB_current.parquet
│   │   │   └── MNITB_lag4.parquet
│   │   └── kapiti/
│   │       ├── KAPITI1.parquet
│   │       ├── KAPITI2.parquet
│   │       ├── KAPITI3.parquet
│   │       └── KAPITI_SASDATA.parquet
│   └── output/
│       ├── FISS_RDAL.txt
│       ├── NSRS_RDAL.txt
│       ├── WALKER_RDAL.txt
│       ├── ELIAB_TEXT.txt
│       └── ftp_commands.txt
```

### Path Configuration

Edit `eibmmth1_converter.py` (lines 15-50):

```python
# Update to your paths
BASE_PATH = Path("/production/data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"
```

---

## Usage

### Quick Start

#### 1. Generate Sample Data
```bash
python generate_eibmmth1_data.py
```

**Output**:
```
======================================================================
EIBMMTH1 Sample Data Generator
======================================================================

Report Date: 31/01/2025

Generating RDAL1 data for 31/01/2025...
  Created: ./data/input/fiss/rdal1_text.parquet
  Records: 500

Generating RGAC data for 31/01/2025...
  Created: ./data/input/rgac/rgac_text.parquet
  Created: ./data/input/rgac/RGAC_lag1.parquet
  Records: 300

[... more files ...]

Sample Data Generation Complete!
```

#### 2. Inspect Sample Data
```bash
python generate_eibmmth1_data.py --inspect
```

#### 3. Run Converter
```bash
python eibmmth1_converter.py
```

**Expected Output**:
```
======================================================================
EIBMMTH1 - RDAL Monthly Processing
======================================================================

Report Date: 31012025
Year: 2025, Month: 01, Week: 4
Start Date: 23012025
Description: PUBLIC BANK BERHAD

======================================================================
VALIDATING INPUT DATA DATES
======================================================================
✓ ALW         : 31012025
✓ GAY         : 31012025
✓ LOAN        : 31012025
✓ DEPOSIT     : 31012025
✓ FD          : 31012025
✓ KAPITI1     : 31012025
✓ KAPITI2     : 31012025
✓ KAPITI3     : 31012025

======================================================================
STARTING MAIN REPORT PROCESSING
======================================================================

Executing EIGWRD1W (RDAL1 Processing)...
Loaded WALALW (RDAL1): 500 records
Saved processed ALW data to ./data/output/RDAL1_processed.parquet

[... processing steps ...]

Executing EIGWRDAL (Write RDAL files)...
Generated RDAL output: ./data/output/FISS_RDAL.txt
Generated NSRS output: ./data/output/NSRS_RDAL.txt

======================================================================
REPORT PROCESSING COMPLETE
======================================================================

======================================================================
GENERATING FTP COMMANDS
======================================================================
FTP commands written to: ./data/output/ftp_commands.txt

======================================================================
PROCESSING SUMMARY
======================================================================
Output files generated:
  - RDAL (FISS):  ./data/output/FISS_RDAL.txt
  - RDAL (NSRS):  ./data/output/NSRS_RDAL.txt
  - FTP Script:   ./data/output/ftp_commands.txt
======================================================================
```

### Production Usage

#### 1. Update Paths
```python
BASE_PATH = Path("/prod/bnm/data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"
```

#### 2. Implement Include Programs

Replace placeholder functions with actual business logic:

```python
def execute_eigwrd1w(dates: Dict[str, str]) -> pl.DataFrame:
    """
    Actual EIGWRD1W implementation
    """
    # Load actual RDAL1 data
    df = pl.read_parquet(WALALW_PATH)
    
    # Apply actual business rules
    # ... business logic here ...
    
    # Save processed data
    df.write_parquet(ALW_OUTPUT)
    
    return df
```

#### 3. Schedule Execution

**Linux/Unix (Cron)**:
```bash
# Run on 1st of each month at 2 AM
0 2 1 * * /usr/bin/python /prod/scripts/eibmmth1_converter.py >> /prod/logs/eibmmth1.log 2>&1
```

**Windows (Task Scheduler)**:
- Program: `python.exe`
- Arguments: `C:\prod\scripts\eibmmth1_converter.py`
- Schedule: Monthly, Day 1, 2:00 AM

---

## Testing

### Unit Tests

#### Test Date Calculation
```python
def test_date_calculation():
    # Mock today's date as Feb 10, 2025
    dates = calculate_report_dates()
    assert dates['reptday'] == '31'
    assert dates['reptmon'] == '01'
    assert dates['reptyear'] == '2025'
    assert dates['nowk'] == '4'
```

#### Test Date Validation
```python
def test_date_validation_success():
    dates = {'rdate': '31012025'}
    # All files have matching dates
    is_valid, errors, _ = validate_input_dates(dates)
    assert is_valid == True
    assert len(errors) == 0

def test_date_validation_failure():
    dates = {'rdate': '31012025'}
    # One file has mismatched date
    is_valid, errors, _ = validate_input_dates(dates)
    assert is_valid == False
    assert len(errors) > 0
```

### Integration Tests

#### End-to-End Test
```python
def test_full_pipeline():
    # Generate sample data
    generate_all_sample_data()
    
    # Run converter
    main()
    
    # Verify outputs exist
    assert RDAL_OUTPUT.exists()
    assert NSRS_OUTPUT.exists()
    assert (OUTPUT_PATH / 'ftp_commands.txt').exists()
    
    # Verify content
    with open(RDAL_OUTPUT) as f:
        lines = f.readlines()
        assert lines[0].startswith('RDAL')
        assert 'AL\n' in lines
        assert 'OB\n' in lines
        assert 'SP\n' in lines
```

### Validation Tests

#### Date Consistency Test
```python
def test_all_dates_consistent():
    report_date = get_report_date()
    
    # Check all generated files have same date
    for path in [WALALW_PATH, WALGAY_PATH, LOAN_PATH, ...]:
        df = pl.read_parquet(path)
        file_date = df.select('REPTDATE').head(1).item()
        assert file_date == report_date
```

---

## Troubleshooting

### Common Issues

#### Issue 1: Date Mismatch Error
```
ERROR: THE LOAN EXTRACTION IS NOT DATED 31012025
THE JOB IS NOT DONE !!
```

**Cause**: Input file has incorrect report date

**Solution**:
1. Check file creation date
2. Verify data extraction process
3. Regenerate file with correct date
4. Re-run converter

#### Issue 2: File Not Found
```
Warning: WALALW (RDAL1) not found at /data/input/fiss/rdal1_text.parquet
```

**Cause**: Input file missing or incorrect path

**Solution**:
1. Verify file exists at specified path
2. Check path configuration in code
3. Verify file permissions
4. Generate sample data if testing

#### Issue 3: Empty Output Files
```
RDAL file generated but contains only header
```

**Cause**: No data passed through processing

**Solution**:
1. Check if input files contain data
2. Verify filtering logic not excluding all records
3. Check business rule implementations
4. Review log messages for warnings

#### Issue 4: Invalid REPTDATE Format
```
Error reading ALW: could not parse date
```

**Cause**: REPTDATE column has wrong format

**Solution**:
1. Verify REPTDATE is Date type, not string
2. Convert string dates to Date type:
   ```python
   df = df.with_columns(
       pl.col('REPTDATE').str.strptime(pl.Date, '%d%m%Y')
   )
   ```

### Error Codes

| Code | Meaning | Action |
|------|---------|--------|
| 0    | Success | Normal completion |
| 1    | General error | Check logs for details |
| 77   | Validation failed | Check input file dates |

### Debug Mode

Enable verbose logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Log Analysis

Check logs for:
- Date validation results
- File loading messages
- Processing step completions
- Error messages
- Warning messages

---

## Appendix

### A. Glossary

- **RDAL**: Regulatory Data Analysis Layer
- **FISS**: Financial Information Submission System
- **NSRS**: Non-Standard Reporting Structure
- **BNM**: Bank Negara Malaysia (Central Bank)
- **GAY/RGAC**: Report on Global Assets and Capital
- **ALW**: Allowance
- **CAG**: Credit Aggregate
- **ELP**: External Liabilities Position
- **ALP**: Asset/Liability Position

### B. Item Code Reference

**AL Codes** (Asset/Liability):
- 4xxx: Asset items
- Position 14: D/I/F = Currency type

**OB Codes** (Off-Balance Sheet):
- 5xxx: Off-balance items

**SP Codes** (Special):
- 3xxx: Special categories
- 80xx: Items with special scaling

### C. Contact Information

For issues or questions:
- System Owner: Finance Division, Public Bank Berhad
- Technical Support: IT Department
- BNM Reporting: reporting@bnm.gov.my

### D. Version History

**v1.0** (2025-02-10)
- Initial Python conversion from SAS EIBMMTH1
- Multi-source data validation
- FISS and NSRS RDAL generation
- FTP script automation

---

## Quick Reference

### File Locations
```
Input:  /data/input/{fiss|rgac|mniln|mnifd|mnitb|kapiti}/
Output: /data/output/
```

### Key Commands
```bash
# Generate sample data
python generate_eibmmth1_data.py

# Inspect data
python generate_eibmmth1_data.py --inspect

# Run converter
python eibmmth1_converter.py
```

### Validation Checklist
- [ ] All input files present
- [ ] All dates match RDATE
- [ ] No error messages in validation
- [ ] Output files generated
- [ ] FTP script created

---

**Document Version**: 1.0  
**Last Updated**: February 10, 2025  
**Author**: System Conversion Team
