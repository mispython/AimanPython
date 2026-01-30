# DMMISRI3 Report - SAS to Python Conversion

## Overview

This Python script is a complete conversion of the SAS program for generating the **DMMISRI3** report: "Breakdown of Islamic Savings Account Deposit" for Public Islamic Bank Berhad (IBU).

## Requirements

```bash
pip install duckdb polars pyarrow --break-system-packages
```

## Input Files

The program expects the following Parquet files in the `input/` directory:

1. **reptdate.parquet**: Contains the report date
   - Column: `REPTDATE` (date)

2. **isa{MMWWYY}.parquet**: Islamic Savings Account data
   - Columns: `PRODUCT`, `CURBAL`
   - Filename pattern: `isa` + 2-digit month + 1-digit week + 2-digit year
   - Example: `isa0212024.parquet` (February, Week 1, 2024)

## Output File

- **dmmisri3_report.txt**: Text report with ASA carriage control characters
  - Location: `output/dmmisri3_report.txt`
  - Format: Fixed-width text with ASA carriage control
  - Page length: 60 lines per page

## Key Conversion Details

### 1. Macro Variables
The SAS program uses `CALL SYMPUT` to create macro variables. Python equivalent:
- Extracted into a dictionary returned by `process_reptdate()`
- Used throughout the program via string interpolation

### 2. Format Definitions

#### SDRANGE Format (Deposit Range Classification)
```python
def get_deposit_range(amount):
    # Returns range code 1-11 based on amount
```

| Range Code | Amount Range |
|------------|--------------|
| 1 | < RM100 |
| 2 | RM100 - RM499 |
| 3 | RM500 - RM999 |
| 4 | RM1,000 - RM4,999 |
| 5 | RM5,000 - RM9,999 |
| 6 | RM10,000 - RM49,999 |
| 7 | RM50,000 - RM99,999 |
| 8 | RM100,000 - RM499,999 |
| 9 | RM500,000 - RM999,999 |
| 10 | RM1,000,000 - RM4,999,999 |
| 11 | RM5,000,000 AND ABOVE |

#### SADPRG Format (Deposit Range Labels)
Maps range codes to display strings (see table above).

#### SDNAME Format (Product Names)
- 204: "ISLAMIC SAVINGS ACCOUNT"
- 215: "ISLAMIC SAVINGS ACCOUNT-i"

### 3. Data Processing Steps

#### Step 1: REPTDATE Processing
```sas
/* SAS */
DATA REPTDATE;
  SET BNM.REPTDATE;
  /* Calculate week, extract date components */
RUN;
```

```python
# Python
macro_vars = process_reptdate()
```

#### Step 2: Filter and Classify ISA Data
```sas
/* SAS */
DATA SAVE;
  SET BNM.ISA&REPTMON&NOWK&REPTYEAR;
  IF PRODUCT IN (204,215);
  IF CURBAL GE 0;
  RANGE = INPUT(CURBAL, SDRANGE.);
  DEPRANGE = PUT(RANGE, SADPRG.);
RUN;
```

```python
# Python - using DuckDB
query = """
SELECT PRODUCT, CURBAL,
       get_deposit_range(CURBAL) as RANGE,
       format_deposit_range(get_deposit_range(CURBAL)) as DEPRANGE
FROM read_parquet('{isa_file}')
WHERE PRODUCT IN (204, 215) AND CURBAL >= 0
"""
df = con.execute(query).pl()
```

#### Step 3: Summarize Data
```sas
/* SAS */
PROC SUMMARY DATA=SAVE NWAY;
CLASS DEPRANGE PRODUCT;
VAR CURBAL;
OUTPUT OUT=SDRNGE SUM=CURBAL;
RUN;
```

```python
# Python - using Polars
summary = df.group_by(['DEPRANGE', 'RANGE', 'PRODUCT']).agg([
    pl.count().alias('NOACCT'),
    pl.col('CURBAL').sum().alias('CURBAL')
])
```

#### Step 4: Generate Tabular Report
```sas
/* SAS */
PROC TABULATE DATA=SDRNGE;
  CLASS DEPRANGE PRODUCT;
  VAR NOACCT CURBAL;
  TABLE PRODUCT ALL, DEPRANGE ALL, 
        NOACCT*SUM CURBAL*SUM;
RUN;
```

```python
# Python
report_lines = generate_report(summary_df, macro_vars)
```

### 4. ASA Carriage Control Characters

The output report includes ASA (ANSI) carriage control characters as the first character of each line:

- `'1'`: New page (form feed)
- `' '`: Single spacing
- `'0'`: Double spacing
- `'-'`: Triple spacing

Example:
```
1                                                                   (new page + blank line)
 REPORT ID : DMMISRI3                                               (single space)
 PUBLIC ISLAMIC BANK BERHAD (IBU)                                   (single space)
0                                                                   (double space)
 ------------------------------------------------------------------ (single space)
```

### 5. Report Structure

```
┌─────────────────────────────────────────────────────────────┐
│ REPORT ID : DMMISRI3                                        │
│ PUBLIC ISLAMIC BANK BERHAD (IBU)                            │
│ BREAKDOWN OF ISLAMIC SAVINGS ACCOUNT DEPOSIT                │
│ REPORT AS AT DD/MM/YY                                       │
├─────────────────────────────────────────────────────────────┤
│ ISLAMIC SAVINGS ACCOUNT         |   NO OF A/C |     AMOUNT  │
├─────────────────────────────────────────────────────────────┤
│ DEPOSIT RANGE/LESS THAN RM100   |       1,234 |  $12,345.67 │
│ DEPOSIT RANGE/RM100 - RM499     |       5,678 |  $98,765.43 │
│ ...                                                          │
│ TOTAL                           |      XX,XXX | $XXX,XXX.XX │
├─────────────────────────────────────────────────────────────┤
│ ISLAMIC SAVINGS ACCOUNT-i       |   NO OF A/C |     AMOUNT  │
│ ...                                                          │
├─────────────────────────────────────────────────────────────┤
│ BANK TOTAL                      |   NO OF A/C |     AMOUNT  │
│ DEPOSIT RANGE/LESS THAN RM100   |       X,XXX |  $XX,XXX.XX │
│ ...                                                          │
│ TOTAL                           |      XX,XXX | $XXX,XXX.XX │
└─────────────────────────────────────────────────────────────┘
```

## Usage

```bash
python dmmisri3_report.py
```

### Directory Structure

```
.
├── input/
│   ├── reptdate.parquet
│   └── isa0212024.parquet
├── output/
│   └── dmmisri3_report.txt (generated)
└── dmmisri3_report.py
```

## Configuration

Edit the following variables in the script if needed:

```python
# Paths
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")

# Report settings
PAGE_LENGTH = 60  # lines per page
MISSING_VALUE = 0
```

## Key Features

1. **DuckDB Integration**: Efficient processing of Parquet files
2. **Polars DataFrame**: Fast data manipulation
3. **User-Defined Functions**: DuckDB UDFs for format conversions
4. **ASA Carriage Control**: Proper print formatting for legacy systems
5. **Exact SAS Equivalence**: Output matches SAS PROC TABULATE format

## Differences from SAS

1. **No External Format Libraries**: Format definitions are embedded in the script (instead of `%INC PGM(PBBDPFMT,PBMISFMT)`)
2. **File Format**: Output is always text with ASA control (SAS might vary)
3. **Sorting**: Removed unnecessary sorts for efficiency
4. **Error Handling**: Added comprehensive error handling and logging

## Verification

To verify the conversion:
1. Compare output report format and spacing
2. Verify totals and subtotals match
3. Check deposit range classifications
4. Validate ASA carriage control characters

## Notes

- The week number (NOWK) is calculated based on the day of the month:
  - Week 1: Days 1-8
  - Week 2: Days 9-15
  - Week 3: Days 16-22
  - Week 4: Days 23-31
  
- Products filtered: 204 (Islamic Savings Account) and 215 (Islamic Savings Account-i)

- Only accounts with CURBAL >= 0 are included

## Troubleshooting

**FileNotFoundError**: Ensure input files exist with correct naming convention
**Empty Report**: Check REPTDATE and ISA data contain valid records
**Format Issues**: Verify product codes and amount ranges are correct

## Performance

- DuckDB handles large Parquet files efficiently
- Polars provides fast aggregation
- No unnecessary sorting operations
- Single-pass data processing where possible
