# EIPWRDAL: RDAL PBCS Data Processing - Python Conversion

## Overview

This Python application converts the SAS program `EIPWRDAL` to Python, processing banking regulatory data for Bank Negara Malaysia (BNM) reporting. It generates RDAL (Regulatory Data Analysis Layer) and NSRS (Non-Standard Reporting Structure) files for PBCS (Public Bank Berhad Corporate System) reporting.

## Purpose

The program processes:
- Asset and Liability data (AL)
- Off-Balance Sheet items (OB)
- Special items (SP)

Output files are formatted for submission to BNM's Data Report Repository (DRR) system.

## System Requirements

### Python Version
- Python 3.9 or higher

### Required Libraries
```bash
pip install polars>=0.19.0
pip install duckdb>=0.9.0
pip install pyarrow>=14.0.0
```

## Directory Structure

```
project/
├── rdal_pbcs_converter.py          # Main processing script
├── generate_sample_data.py          # Sample data generator
├── README.md                        # This file
├── TECHNICAL_DOCUMENTATION.md       # Detailed technical documentation
├── data/
│   ├── input/
│   │   ├── loan/
│   │   │   └── LNNOTE.parquet      # Loan note data
│   │   ├── pbcs/
│   │   │   └── CCLW{MM}{W}.parquet # PBCS data by month/week
│   │   └── bnm/
│   │       └── D{YYYY}/
│   │           └── ALW{MM}{W}.parquet  # BNM data by year/month/week
│   └── output/
│       ├── rdal_pbcs.txt            # RDAL output file
│       ├── nsrs_rdal_pbcs.txt       # NSRS output file
│       └── ftp_commands.txt         # FTP upload commands
```

## Input Files

### 1. LNNOTE.parquet (Loan Note Data)
**Location:** `data/input/loan/LNNOTE.parquet`

**Columns:**
- `PZIPCODE` (Int64): Postal/zip code identifier
- `LOANTYPE` (Utf8): Type of loan product
- `BALANCE` (Float64): Outstanding loan balance

**Purpose:** Used for CAG (Credit Aggregate) processing for specific geographic zones.

### 2. ALW{MM}{W}.parquet (BNM Allowance Data)
**Location:** `data/input/bnm/D{YYYY}/ALW{MM}{W}.parquet`

**Columns:**
- `ITCODE` (Utf8): 14-character item code
- `AMTIND` (Utf8): Amount indicator ('D'=Domestic, 'I'=International, 'F'=Foreign, ' '=No indicator)
- `AMOUNT` (Float64): Transaction amount

**Naming Convention:**
- `{YYYY}`: 4-digit year (e.g., 2025)
- `{MM}`: 2-digit month (01-12)
- `{W}`: Week number (1-4)

**Example:** `ALW021.parquet` = February, Week 1

### 3. CCLW{MM}{W}.parquet (PBCS Data)
**Location:** `data/input/pbcs/CCLW{MM}{W}.parquet`

**Columns:** Same as ALW files
- `ITCODE` (Utf8)
- `AMTIND` (Utf8)
- `AMOUNT` (Float64)

## Output Files

### 1. rdal_pbcs.txt (FISS RDAL Output)
**Format:** Fixed-width text file with semicolon delimiters

**Structure:**
```
RDAL{DD}{MM}{YYYY}          # Header line
AL                           # Asset/Liability section
{ITCODE};{AMOUNTD};{AMOUNTI};{AMOUNTF}
...
OB                           # Off-Balance Sheet section
{ITCODE};{AMOUNTD};{AMOUNTI};{AMOUNTF}
...
SP                           # Special Items section
{ITCODE};{AMOUNTD};{AMOUNTF}
...
```

**Field Descriptions:**
- `ITCODE`: 14-character item code
- `AMOUNTD`: Domestic amount (in thousands)
- `AMOUNTI`: International amount (in thousands)
- `AMOUNTF`: Foreign amount (in thousands)

### 2. nsrs_rdal_pbcs.txt (NSRS Output)
**Format:** Same structure as rdal_pbcs.txt

**Key Differences:**
- Amounts for codes starting with '80' are divided by 1000
- Other amounts are kept as whole numbers (not divided by 1000)
- Handles negative adjustments via '#' to 'Y' conversion

### 3. ftp_commands.txt (SFTP Upload Script)
**Format:** Plain text FTP commands

**Content:**
```
PUT //SAP.PBB.NSRS.RDAL.PBCS nsrs_EAB_PBCS_{DDMMYYYY}.txt
```

## Date Logic

The application calculates reporting dates based on the last day of the previous month:

| Day of Month | Week (WK) | Start Day (SDD) | Previous Week (WK1) |
|--------------|-----------|-----------------|---------------------|
| 8            | 1         | 1               | 4                   |
| 15           | 2         | 9               | 1                   |
| 22           | 3         | 16              | 2                   |
| 29/30/31     | 4         | 23              | 3                   |

**Example:**
- Today: February 10, 2025
- Report Date: January 31, 2025
- Week: 4 (since day 31 > 22)
- Files to process: `ALW014.parquet`, `CCLW014.parquet`

## Item Code (ITCODE) Structure

**Format:** 14 characters (positional significance)

```
Position:  1-5    6-10   11-13  14
Example:   30221  00000  000    Y
           │      │      │      └─ Record type (Y/F/#)
           │      │      └──────── Sub-classification
           │      └─────────────── Category detail
           └────────────────────── Main classification
```

**Position 14 Values:**
- `Y`: Standard record
- `F`: Foreign currency
- `#`: Negative adjustment (converted to 'Y' with negated amount)

**Position 2 (second character):**
- `0`: No amount indicator (AMTIND = ' ')
- Other: Has amount indicator (AMTIND = 'D', 'I', or 'F')

## Data Categorization

### AL (Asset/Liability)
Items that don't fall into OB or SP categories and don't start with '5'

**Exclusions:**
- Codes starting with '307'
- Codes starting with '40190' or '40191'
- Codes starting with 'SSTS'
- Codes starting with '685' or '785'

### OB (Off-Balance Sheet)
Items where ITCODE starts with '5'

### SP (Special Items)
- Items starting with '307'
- Items starting with '40190' or '40191'
- Items starting with '685' or '785'
- Items starting with 'SSTS' (converted to '4017000000000Y')
- Items where position 2 = '0' (regardless of AMTIND)

## Filtering Rules

### Excluded Item Code Ranges:
- `30221` - `30228` (first 5 positions)
- `30231` - `30238` (first 5 positions)
- `30091` - `30098` (first 5 positions)
- `40151` - `40158` (first 5 positions)
- `NSSTS` (first 5 positions)

### Specific Exclusions:
- `4364008110000Y` (deleted explicitly)

### Date-Specific Exclusions:
On days 08 and 22 of the month:
- Exclude `4003000000000Y` if code starts with '68' or '78'

Always exclude:
- `4966000000000F`

## CAG (Credit Aggregate) Processing

### Target Zip Codes:
```
2002, 2013, 3039, 3047,
800003098, 800003114, 800004016, 800004022, 800004029,
800040050, 800040053, 800050024, 800060024, 800060045,
800060081, 80060085
```

### Process:
1. Filter LNNOTE data for specified zip codes
2. Apply LNPROD format to determine product code
3. Apply LNDENOM format to determine amount indicator
4. Set ITCODE to `7511100000000Y`
5. Aggregate by ITCODE and AMTIND

## Amount Calculations

### RDAL File:
- All amounts divided by 1000 and rounded
- AMOUNTD = (AMOUNTD + AMOUNTI + AMOUNTF) for AL section
- AMOUNTD = (AMOUNTD + AMOUNTI) for OB section
- AMOUNTD = (AMOUNTD + AMOUNTF) for SP section

### NSRS File:
- Codes starting with '80': Amount / 1000
- Other codes: Amount (no division)
- All amounts rounded to nearest integer

## Usage

### Basic Execution:
```bash
python rdal_pbcs_converter.py
```

### With Custom Paths:
Modify the path constants at the top of the script:
```python
INPUT_BASE_PATH = Path("/custom/path/to/input")
OUTPUT_BASE_PATH = Path("/custom/path/to/output")
```

### Expected Output:
```
Processing for date: 31012025
Report year: 2025, Month: 01, Week: 4
RDAL file written to: /data/output/rdal_pbcs.txt
NSRS file written to: /data/output/nsrs_rdal_pbcs.txt
FTP commands written to: /data/output/ftp_commands.txt

======================================================================
Processing complete!
======================================================================
Output files:
  - RDAL: /data/output/rdal_pbcs.txt
  - NSRS: /data/output/nsrs_rdal_pbcs.txt
  - SFTP: /data/output/ftp_commands.txt
```

## Generating Sample Data

To generate test data:
```bash
python generate_sample_data.py
```

This creates sample parquet files in the correct directory structure.

## Error Handling

The application handles:
- Missing input files (creates empty DataFrames)
- Invalid date calculations
- Missing columns in input files

**Note:** The application will run even if input files are missing, but will produce empty output sections.

## Performance Considerations

### Optimizations:
- Uses Polars for fast columnar operations
- DuckDB for efficient parquet file processing
- Minimal sorting (only where order is critical)
- Efficient group-by operations with `maintain_order=True`

### Expected Performance:
- Typical dataset (100K rows): < 10 seconds
- Large dataset (1M rows): < 60 seconds

## Troubleshooting

### Issue: "File not found" errors
**Solution:** Ensure input files exist with correct naming convention

### Issue: Incorrect output amounts
**Solution:** Verify AMTIND values in input data ('D', 'I', 'F', or ' ')

### Issue: Missing records in output
**Solution:** Check if ITCODE falls within excluded ranges

### Issue: Date calculation errors
**Solution:** Verify system date is set correctly

## Maintenance

### Adding New Item Codes:
Modify the filtering logic in the data categorization section.

### Changing Amount Calculations:
Update the `write_rdal_file()` or `write_nsrs_file()` functions.

### Modifying Date Logic:
Update the `calculate_report_dates()` function.

## Related Documentation

- `TECHNICAL_DOCUMENTATION.md`: Detailed technical specifications
- `SAS_to_Python_Mapping.md`: SAS-to-Python conversion reference

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review sample data generation
3. Verify input file formats
4. Check log output for specific errors

## Version History

**v1.0** (2025-02-10)
- Initial Python conversion from SAS EIPWRDAL
- Polars-based implementation
- Complete date logic replication
- RDAL and NSRS file generation

## License

Internal use only - Public Bank Berhad
