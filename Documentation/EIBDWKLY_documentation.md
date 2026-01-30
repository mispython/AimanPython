# EIBDWKLY - Loan and Deposit Processing with Dual Date Validation

## Overview

This Python script converts the JCL/SAS program **EIBDWKLY** that performs conditional processing with dual date validation for both loan and deposit extractions.

## Program Details

- **Program**: EIBDWKLY
- **ESMR**: 06-1428
- **Dependencies**: Run after EIBDLNEX
- **Purpose**: Validate loan and deposit extraction dates, then execute processing programs

## Requirements

```bash
pip install polars pyarrow --break-system-packages
```

## Input Files

All input files should be in Parquet format in the `input/` directory:

### 1. loan_reptdate.parquet
- **Description**: Report date from loan extraction (LOAN.REPTDATE)
- **Columns**: 
  - `REPTDATE` (date): The loan extraction date
- **Purpose**: Source for macro variables and loan date validation

### 2. deposit_reptdate.parquet
- **Description**: Report date from deposit extraction (DEPOSIT.REPTDATE)
- **Columns**: 
  - `REPTDATE` (date): The deposit extraction date
- **Purpose**: Deposit date validation

## Output Files

All output files are generated in the `output/` directory:

### 1. reptdate.parquet
- **Description**: Copy of loan report date for downstream processing (BNM.REPTDATE)
- **Columns**: Same as input `REPTDATE`

### 2. Processing outputs (if validation passes)
- **lalwpbbd_***: Outputs from LALWPBBD processing
- **lalweirc_***: Outputs from LALWEIRC processing

## Processing Logic

### Dual Date Validation Flow

```
1. Read LOAN.REPTDATE
   ├─ Extract macro variables → RDATE
   └─ Save to BNM.REPTDATE
   ↓
2. Read LOAN.REPTDATE again → LOAN date
   ↓
3. Read DEPOSIT.REPTDATE → DEPOSIT date
   ↓
4. Compare: LOAN == RDATE AND DEPOSIT == RDATE?
   ├─ YES: Both dates match
   │   ├─ Execute LALWPBBD
   │   ├─ Execute LALWEIRC
   │   └─ Exit with code 0 (success)
   │
   └─ NO: One or both dates don't match
       ├─ Print error message(s)
       └─ Exit with code 77 (date mismatch)
```

### Conditional Processing Macro

```sas
/* SAS */
%MACRO PROCESS;
   %IF "&LOAN"="&RDATE" AND "&DEPOSIT"="&RDATE" %THEN %DO;
      %INC PGM(LALWPBBD);
      %INC PGM(LALWEIRC);
   %END;
   %ELSE %DO;
      %IF "&LOAN" NE "&RDATE" %THEN
         %PUT THE LOAN EXTRACTION IS NOT DATED &RDATE;
      %IF "&DEPOSIT" NE "&RDATE" %THEN
         %PUT THE DEPOSIT EXTRACTION IS NOT DATED &RDATE;
      %PUT THE JOB IS NOT DONE !!;
      DATA A; ABORT 77; %END;
   %END;
%MEND;
```

**Python Equivalent:**
```python
loan_date = get_loan_date()
deposit_date = get_deposit_date()
rdate = macro_vars['RDATE']

if loan_date == rdate and deposit_date == rdate:
    execute_lalwpbbd()
    execute_lalweirc()
    sys.exit(0)
else:
    if loan_date != rdate:
        print(f"THE LOAN EXTRACTION IS NOT DATED {rdate}")
    if deposit_date != rdate:
        print(f"THE DEPOSIT EXTRACTION IS NOT DATED {rdate}")
    print("THE JOB IS NOT DONE !!")
    sys.exit(77)
```

## Exit Codes

| Code | Meaning | Description |
|------|---------|-------------|
| 0 | Success | Both dates valid, processing completed |
| 77 | Date Mismatch | LOAN or DEPOSIT date ≠ RDATE, processing aborted |
| 1 | General Error | Exception occurred during processing |

## Macro Variables

Extracted from LOAN.REPTDATE and available for downstream processing:

| Variable | SAS Format | Description | Example |
|----------|------------|-------------|---------|
| NOWK | Z2. | Day of month (zero-padded) | `15` |
| REPTMON | Z2. | Month (zero-padded) | `02` |
| REPTYEAR | YEAR4. | Four-digit year | `2024` |
| REPTYR | YEAR2. | Two-digit year | `24` |
| RDATE | DDMMYY8. | Formatted date | `15/02/2024` |
| SDATE | DDMMYY8. | Same as RDATE | `15/02/2024` |
| TDATE | Z5. | Date timestamp (YMMDD) | `40215` |

## Usage

```bash
python eibdwkly_processing.py
```

### Directory Structure

```
.
├── input/
│   ├── loan_reptdate.parquet
│   └── deposit_reptdate.parquet
├── output/
│   ├── reptdate.parquet (generated)
│   ├── lalwpbbd_*.* (generated if validation passes)
│   └── lalweirc_*.* (generated if validation passes)
├── eibdwkly_processing.py
├── lalwpbbd_processing.py (required for full processing)
└── lalweirc_processing.py (required for full processing)
```

## External Programs

The program executes two external Python scripts:

### 1. LALWPBBD Processing
- **Script**: `lalwpbbd_processing.py`
- **Represents**: `%INC PGM(LALWPBBD)`
- **Purpose**: Daily loan and deposit balance processing
- **Inputs**: Loan and deposit data from input/
- **Outputs**: Balance calculations to output/

### 2. LALWEIRC Processing
- **Script**: `lalweirc_processing.py`
- **Represents**: `%INC PGM(LALWEIRC)`
- **Purpose**: EIR (Effective Interest Rate) calculations
- **Inputs**: EIR data from input/
- **Outputs**: EIR metrics to output/

### Commented Out Programs
The following programs are commented out in the original SAS:
- **LALBDBAL**: SMR2016-1430 (disabled)
- **LALWPBBU**: (disabled)

### Stub Implementations

Stub implementations are provided for both LALWPBBD and LALWEIRC that:
- Print processing messages
- Create dummy output files
- Return success code

**Replace the stubs with actual conversions when available.**

## Key Features

### 1. Dual Date Validation
- Validates BOTH loan and deposit extraction dates
- Requires both to match RDATE for processing to proceed
- Provides specific error messages for each mismatch

### 2. Conditional Execution
- Only processes data if both dates validate
- Prevents downstream errors from stale data
- Clear error messages for each validation failure

### 3. Sequential Program Execution
- Executes LALWPBBD first
- Then executes LALWEIRC
- Stops on first failure

### 4. Macro Variable Generation
- Extracts multiple date formats from single source
- Includes both 2-digit and 4-digit year formats
- Makes dates available for downstream processing

### 5. Clean Exit Codes
- Exit 0: Success (both dates match)
- Exit 77: Date mismatch (matches SAS ABORT 77)
- Exit 1: General error

## Error Scenarios

### Scenario 1: Loan Date Mismatch
```
Date Validation:
  LOAN extraction:    14/02/2024
  DEPOSIT extraction: 15/02/2024
  Expected (RDATE):   15/02/2024

  ✗ LOAN date does NOT match
  ✓ DEPOSIT date matches

✗ Date validation failed
  THE LOAN EXTRACTION IS NOT DATED 15/02/2024
  THE JOB IS NOT DONE !!
  
Exit code: 77
```

### Scenario 2: Deposit Date Mismatch
```
Date Validation:
  LOAN extraction:    15/02/2024
  DEPOSIT extraction: 14/02/2024
  Expected (RDATE):   15/02/2024

  ✓ LOAN date matches
  ✗ DEPOSIT date does NOT match

✗ Date validation failed
  THE DEPOSIT EXTRACTION IS NOT DATED 15/02/2024
  THE JOB IS NOT DONE !!
  
Exit code: 77
```

### Scenario 3: Both Dates Mismatch
```
Date Validation:
  LOAN extraction:    14/02/2024
  DEPOSIT extraction: 14/02/2024
  Expected (RDATE):   15/02/2024

  ✗ LOAN date does NOT match
  ✗ DEPOSIT date does NOT match

✗ Date validation failed
  THE LOAN EXTRACTION IS NOT DATED 15/02/2024
  THE DEPOSIT EXTRACTION IS NOT DATED 15/02/2024
  THE JOB IS NOT DONE !!
  
Exit code: 77
```

### Scenario 4: Success
```
Date Validation:
  LOAN extraction:    15/02/2024
  DEPOSIT extraction: 15/02/2024
  Expected (RDATE):   15/02/2024

  ✓ LOAN date matches
  ✓ DEPOSIT date matches

✓ All dates match - proceeding with processing
✓ LALWPBBD processing completed
✓ LALWEIRC processing completed
✓ Program completed successfully

Exit code: 0
```

## Processing Steps

### Step 1: Process LOAN.REPTDATE (First Pass)
```python
# Read LOAN.REPTDATE
reptdate = df['REPTDATE'][0]

# Extract components and create macro variables
NOWK = f"{reptdate.day:02d}"
REPTMON = f"{reptdate.month:02d}"
REPTYEAR = str(reptdate.year)
REPTYR = str(reptdate.year)[-2:]
RDATE = f"{reptdate.day:02d}/{reptdate.month:02d}/{reptdate.year}"
# etc.

# Save to BNM.REPTDATE
df.write_parquet(REPTDATE_OUTPUT)
```

### Step 2: Get LOAN Date (Second Pass)
```python
# Read LOAN.REPTDATE again
loan_date = get_loan_date()  # Format: DDMMYY8
```

### Step 3: Get DEPOSIT Date
```python
# Read DEPOSIT.REPTDATE
deposit_date = get_deposit_date()  # Format: DDMMYY8
```

### Step 4: Validate Both Dates
```python
if loan_date == rdate and deposit_date == rdate:
    proceed_with_processing()
else:
    abort_with_specific_error_messages()
```

### Step 5: Execute Processing Programs (if validation passes)
```python
subprocess.run(['python3', 'lalwpbbd_processing.py'])
subprocess.run(['python3', 'lalweirc_processing.py'])
```

## Differences from SAS/JCL

| Feature | SAS/JCL | Python |
|---------|---------|--------|
| **File Format** | SAS datasets | Parquet files |
| **Macro Variables** | `%LET` / `CALL SYMPUT` | Dictionary |
| **Dual Validation** | `AND` operator | Python `and` |
| **External Programs** | `%INC PGM()` | `subprocess.run()` |
| **Abort** | `ABORT 77` | `sys.exit(77)` |
| **Multiple Errors** | Multiple `%PUT` | Multiple print statements |

## Date Format Conversions

| SAS Format | Python Equivalent | Example |
|------------|-------------------|---------|
| `Z2.` | `f"{value:02d}"` | `15` |
| `YEAR4.` | `str(year)` | `2024` |
| `YEAR2.` | `str(year)[-2:]` | `24` |
| `DDMMYY8.` | `f"{day:02d}/{month:02d}/{year}"` | `15/02/2024` |
| `Z5.` | `f"{year}{month:02d}{day:02d}"[2:]` | `40215` |

## Comparison with EIBDWKLX

| Feature | EIBDWKLX | EIBDWKLY |
|---------|----------|----------|
| **Data Sources** | LOAN only | LOAN + DEPOSIT |
| **Validation** | Single date | Dual date |
| **Processing** | LALWPBBC + LALWPBBU | LALWPBBD + LALWEIRC |
| **Complexity** | Single validation | AND validation |

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **Exit code 77** | Check both LOAN and DEPOSIT dates match RDATE |
| **LALWPBBD not found** | Ensure `lalwpbbd_processing.py` exists |
| **LALWEIRC not found** | Ensure `lalweirc_processing.py` exists |
| **loan_reptdate missing** | Verify input/loan_reptdate.parquet exists |
| **deposit_reptdate missing** | Verify input/deposit_reptdate.parquet exists |
| **One date missing** | Both date files are required |

## Validation Checklist

After running:
- [ ] Exit code is 0 (success) or 77 (date mismatch)
- [ ] reptdate.parquet created in output/
- [ ] If dates match: LALWPBBD outputs created
- [ ] If dates match: LALWEIRC outputs created
- [ ] If dates don't match: Specific error messages printed
- [ ] Macro variables displayed in console output

## Integration Notes

### Upstream Dependency: EIBDLNEX
This program must run AFTER EIBDLNEX completes successfully. EIBDLNEX is responsible for:
- Extracting loan data (creates LOAN.REPTDATE)
- Extracting deposit data (creates DEPOSIT.REPTDATE)
- Ensuring data consistency

### Downstream Processing
The outputs of this program are typically used by:
- Reporting programs
- Balance reconciliation processes
- Regulatory submission processes
- Data warehouse loads

## Performance Notes

- **Polars**: Fast Parquet I/O
- **No Sorting**: No unnecessary PROC SORT operations
- **Early Exit**: Aborts immediately on date mismatch
- **Sequential Processing**: Programs execute in sequence
- **Subprocess**: Each program runs in separate process

## Example Output

### Successful Run
```
EIBDWKLY - Loan and Deposit Processing with Dual Date Validation
================================================================================
ESMR: 06-1428
Run after: EIBDLNEX
================================================================================

1. Processing REPTDATE and extracting macro variables...

   Macro Variables:
     NOWK (day): 15
     REPTMON (month): 02
     REPTYEAR (year 4-digit): 2024
     REPTYR (year 2-digit): 24
     RDATE (formatted): 15/02/2024
     SDATE (formatted): 15/02/2024
     TDATE (timestamp): 40215

================================================================================
CONDITIONAL PROCESSING
================================================================================

Date Validation:
  LOAN extraction:    15/02/2024
  DEPOSIT extraction: 15/02/2024
  Expected (RDATE):   15/02/2024

  ✓ LOAN date matches
  ✓ DEPOSIT date matches

✓ All dates match - proceeding with processing
--------------------------------------------------------------------------------

1. Executing LALWPBBD processing...
================================================================================
LALWPBBD Processing (Daily Loan and Deposit Balances)
(processing output...)
✓ LALWPBBD processing completed successfully
================================================================================

2. Executing LALWEIRC processing...
================================================================================
LALWEIRC Processing (EIR Calculation)
(processing output...)
✓ LALWEIRC processing completed successfully
================================================================================

✓ All processing completed successfully

================================================================================
PROCESSING SUMMARY
================================================================================

Date Information:
  Week (NOWK): 15
  Month (REPTMON): 02
  Year 4-digit (REPTYEAR): 2024
  Year 2-digit (REPTYR): 24
  Report Date (RDATE): 15/02/2024
  Start Date (SDATE): 15/02/2024
  Timestamp (TDATE): 40215

Generated Files:
  - output/reptdate.parquet (256 bytes)

  Processing outputs:
    - lalwpbbd_processed.txt (45 bytes)
    - lalweirc_processed.txt (42 bytes)

✓ Program completed successfully
```
