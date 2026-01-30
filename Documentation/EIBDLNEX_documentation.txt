# Loan Data Processing with Foreign Exchange Rates - SAS to Python Conversion

## Overview

This Python script converts a SAS program that processes loan note data for both conventional (PBB) and Islamic (PIBB) banking operations, updating product codes, census tracts, and applying foreign exchange rates.

## Requirements

```bash
pip install duckdb polars pyarrow --break-system-packages
```

## Input Files

All input files should be in Parquet format in the `input/` directory:

### 1. Conventional Loan Files (PBB)

#### lnnote.parquet
- **Description**: Main conventional loan note data
- **Key Columns**:
  - `ACCTNO` (integer): Account number
  - `NOTENO` (integer): Note number
  - `LOANTYPE` (integer): Loan type code (to be updated)
  - `CENSUS` (float): Census tract (to be updated)
  - `CURCODE` (string): Currency code
  - `SPOTRATE` (float): Foreign exchange spot rate
  - Other loan-related columns

#### lnwof.parquet
- **Description**: Conventional loan write-off data
- **Key Columns**:
  - `ACCTNO` (integer): Account number
  - `NOTENO` (integer): Note number
  - `WRITE_DOWN_BAL` (float): Write-down balance
  - `PRODUCT` (integer): Product code (source for LOANTYPE update)
  - `CENSUS_TRT` (float): Census tract (source for CENSUS update)
  - `ORICODE` (integer): Original code (alternate source for LOANTYPE)

### 2. Islamic Loan Files (PIBB)

#### ilnnote.parquet
- **Description**: Main Islamic loan note data
- **Columns**: Same structure as `lnnote.parquet`

#### ilnwof.parquet
- **Description**: Islamic loan write-off data
- **Columns**: Same structure as `lnwof.parquet`

### 3. Foreign Exchange Rate File

#### foratebkp.parquet
- **Description**: Foreign exchange rate backup/history
- **Columns**:
  - `CURCODE` (string): Currency code
  - `REPTDATE` (date): Report date
  - `SPOTRATE` (float): Spot exchange rate

### 4. Optional: Date Control File

#### datefile.txt
- **Description**: Control file for conditional processing (OPC logic simulation)
- **Format**: First 8 characters contain date in MMDDYYYY or DDMMYYYY format
- **Purpose**: Program runs only on 8th, 15th, 22nd, or last day of month
- **Note**: Set `ALWAYS_RUN = True` in script to bypass this check

## Output Files

All output files are generated in the `output/` directory:

### 1. lnnote_updated.parquet
- **Description**: Updated conventional loan notes
- **Updates**:
  - `LOANTYPE`: Updated from PRODUCT or ORICODE (if not null/zero)
  - `CENSUS`: Updated from CENSUS_TRT (if not null/zero)
  - `SPOTRATE`: Applied for non-MYR currencies

### 2. ilnnote_updated.parquet
- **Description**: Updated Islamic loan notes
- **Updates**: Same as conventional loans

### 3. fxrate_current.parquet
- **Description**: Current foreign exchange rates (most recent per currency)
- **Columns**: `CURCODE`, `SPOTRATE`

## Processing Logic

### 1. Date Control (OPC Conditional Logic)

```sas
/* SAS OPC Logic */
%OPC COMP=(&OCDATE..EQ.(08,15,22,&OCLASTC))
```

**Python Equivalent:**
```python
# Run on 8th, 15th, 22nd, or last day of month
if day in [8, 15, 22] or day == last_day_of_month:
    process()
```

**Control Variable:**
```python
ALWAYS_RUN = True   # Set to False to enable date checking
```

### 2. FX Rate Processing

```sas
/* SAS */
PROC SORT DATA=FORATE.FORATEBKP OUT=FXRATE;
   BY CURCODE DESCENDING REPTDATE;
   WHERE REPTDATE <= TODAY()-1;
RUN;
PROC SORT DATA=FXRATE NODUPKEY; BY CURCODE; RUN;
```

**Python Equivalent:**
```python
# Filter rates up to yesterday
fxrate_df = fxrate_df.filter(pl.col('REPTDATE') <= yesterday)

# Sort by CURCODE and REPTDATE descending
fxrate_df = fxrate_df.sort(['CURCODE', 'REPTDATE'], descending=[False, True])

# Keep only most recent rate per currency
fxrate_df = fxrate_df.unique(subset=['CURCODE'], keep='first')
```

### 3. Update Logic - LOANTYPE

```sas
/* SAS */
IF PRODUCT NOT IN (.,0) THEN DO;
   LOANTYPE=PRODUCT;
END;
IF ORICODE NOT IN (.,0) THEN DO;
   LOANTYPE=ORICODE;  /* Overwrites PRODUCT if exists */
END;
```

**Python Equivalent:**
```python
# First update from PRODUCT
lnnote = lnnote.with_columns([
    pl.when(
        (pl.col('PRODUCT').is_not_null()) & 
        (pl.col('PRODUCT') != 0)
    )
    .then(pl.col('PRODUCT'))
    .otherwise(pl.col('LOANTYPE'))
    .alias('LOANTYPE')
])

# Then update from ORICODE (takes precedence)
lnnote = lnnote.with_columns([
    pl.when(
        (pl.col('ORICODE').is_not_null()) & 
        (pl.col('ORICODE') != 0)
    )
    .then(pl.col('ORICODE'))
    .otherwise(pl.col('LOANTYPE'))
    .alias('LOANTYPE')
])
```

**Priority Order:**
1. If ORICODE is not null and not 0 → Use ORICODE
2. Else if PRODUCT is not null and not 0 → Use PRODUCT
3. Else → Keep original LOANTYPE

### 4. Update Logic - CENSUS

```sas
/* SAS */
IF CENSUS_TRT NOT IN (.,0) THEN DO;
   CENSUS=CENSUS_TRT;
END;
```

**Python Equivalent:**
```python
lnnote = lnnote.with_columns([
    pl.when(
        (pl.col('CENSUS_TRT').is_not_null()) & 
        (pl.col('CENSUS_TRT') != 0)
    )
    .then(pl.col('CENSUS_TRT'))
    .otherwise(pl.col('CENSUS'))
    .alias('CENSUS')
])
```

### 5. FX Rate Application

```sas
/* SAS */
PROC FORMAT CNTLIN=FOFMT;  /* Create format from FX rates */
RUN;

IF CURCODE NE 'MYR' THEN SPOTRATE = PUT(CURCODE,$FORATE.)*1;
```

**Python Equivalent:**
```python
# Create FX lookup dictionary
fx_lookup = {curcode: spotrate for curcode, spotrate in fxrate_df}

# Apply to non-MYR records
lnnote = lnnote.with_columns([
    pl.when(pl.col('CURCODE') != 'MYR')
    .then(
        pl.col('CURCODE').map_elements(
            lambda x: fx_lookup.get(x),
            return_dtype=pl.Float64
        )
    )
    .otherwise(pl.col('SPOTRATE'))
    .alias('SPOTRATE')
])
```

**Note**: MYR (Malaysian Ringgit) records do NOT get SPOTRATE updated.

## Processing Flow

```
1. Check run date (if ALWAYS_RUN=False)
   ↓ (Skip if not 8th, 15th, 22nd, or last day)
   
2. Process FX rates
   ├─ Read foratebkp.parquet
   ├─ Filter REPTDATE <= yesterday
   ├─ Sort by CURCODE, REPTDATE DESC
   ├─ Keep most recent per currency
   └─ Create lookup dictionary
   
3. Process Conventional Loans (PBB)
   ├─ Read lnnote.parquet
   ├─ Read lnwof.parquet (if exists)
   ├─ Merge on ACCTNO, NOTENO (LEFT JOIN)
   ├─ Update LOANTYPE:
   │  ├─ From PRODUCT (if not null/0)
   │  └─ From ORICODE (if not null/0) - takes precedence
   ├─ Update CENSUS from CENSUS_TRT (if not null/0)
   ├─ Apply SPOTRATE for non-MYR currencies
   └─ Save to lnnote_updated.parquet
   
4. Process Islamic Loans (PIBB)
   ├─ Read ilnnote.parquet
   ├─ Read ilnwof.parquet (if exists)
   ├─ Same update logic as conventional
   └─ Save to ilnnote_updated.parquet
   
5. Generate summary report
```

## Key Features

### 1. Conditional Processing (OPC Logic)
- Simulates SAS OPC (Operations Planning and Control) conditional execution
- Checks if current day is 8th, 15th, 22nd, or last day of month
- Can be overridden with `ALWAYS_RUN = True`

### 2. Multi-Source Updates
- Updates come from write-off tables (LNWOF/ILNWOF)
- Handles missing write-off data gracefully
- Preserves original values if no updates available

### 3. Priority-Based Updates
- ORICODE takes precedence over PRODUCT for LOANTYPE
- Only updates if new value is not null and not 0
- Preserves existing values when update source is empty

### 4. FX Rate Management
- Uses most recent rate per currency (up to yesterday)
- Creates efficient lookup dictionary
- Only applies to non-MYR currencies
- Handles missing currency codes gracefully

### 5. Separate Processing Streams
- Conventional (PBB) and Islamic (PIBB) processed independently
- Same logic applied to both
- Separate output files

## Usage

```bash
python loan_fx_processing.py
```

### Directory Structure

```
.
├── input/
│   ├── lnnote.parquet
│   ├── lnwof.parquet
│   ├── ilnnote.parquet
│   ├── ilnwof.parquet
│   ├── foratebkp.parquet
│   └── datefile.txt (optional)
├── output/
│   ├── lnnote_updated.parquet (generated)
│   ├── ilnnote_updated.parquet (generated)
│   └── fxrate_current.parquet (generated)
└── loan_fx_processing.py
```

## Configuration

Edit these variables in the script:

```python
# Control automatic processing
ALWAYS_RUN = True  # Set to False to enable date checking

# Input paths (can be customized)
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")

# File names (can be customized)
LNNOTE_FILE = INPUT_DIR / "lnnote.parquet"
LNWOF_FILE = INPUT_DIR / "lnwof.parquet"
# etc.
```

## ESMR References

### ESMR 2011-3795
**Purpose**: Update product code census tract for PBB (conventional loans)
- Updates `LOANTYPE` from write-off `PRODUCT` or `ORICODE`
- Updates `CENSUS` from write-off `CENSUS_TRT`

### ESMR 2011-3853
**Purpose**: Update product code census tract in PIBB (Islamic loans)
- Same update logic as conventional loans
- Applies to Islamic banking loan portfolio

## Data Quality Rules

### 1. NULL and Zero Handling
- NULL values are NOT considered valid updates
- Zero values are NOT considered valid updates
- Only non-null, non-zero values trigger updates

### 2. Update Priority
For `LOANTYPE`:
1. **ORICODE** (highest priority) - if not null and not 0
2. **PRODUCT** (medium priority) - if not null and not 0
3. **Original LOANTYPE** (fallback) - if no updates available

### 3. Currency Rules
- **MYR**: SPOTRATE is NOT updated (domestic currency)
- **Non-MYR**: SPOTRATE updated from FX lookup
- **Missing currency**: SPOTRATE remains null if not in lookup

### 4. Merge Rules
- LEFT JOIN: All LNNOTE/ILNNOTE records preserved
- Write-off data is optional (no error if missing)
- Unmatched records keep original values

## Output Summary

The program generates a summary showing:

```
PROCESSING SUMMARY
==================

Conventional Loans (PBB):
  Total records: 123,456
  Non-MYR records: 12,345
  Records with LOANTYPE: 120,000
  Records with CENSUS: 115,000

Islamic Loans (PIBB):
  Total records: 45,678
  Non-MYR records: 4,567
  Records with LOANTYPE: 44,000
  Records with CENSUS: 43,000

Foreign Exchange Rates:
  Currencies loaded: 25
  Sample rates:
    USD: 4.1250
    EUR: 4.5680
    GBP: 5.2340
    SGD: 3.0890
    CNY: 0.5720
```

## Differences from SAS

| Feature                   | SAS              | Python              |
|---------------------------|------------------|---------------------|
| **File Format**           | SAS datasets     | Parquet files       |
| **Conditional Execution** | %OPC macros      | Date check function |
| **FX Format**             | PROC FORMAT      | Dictionary lookup   |
| **Merge**                 | MERGE with IN=   | Polars .join()      |
| **Update Logic**          | IF-THEN-DO       | .with_columns()     |
| **Sorting**               | PROC SORT        | .sort()             |
| **Deduplication**         | NODUPKEY         | .unique()           |
| **Output**                | Library datasets | Parquet files       |

## Troubleshooting

| Issue                    | Solution                                             |
|--------------------------|------------------------------------------------------|
| **FileNotFoundError**    | Verify input files exist with correct names          |
| **No updates applied**   | Check write-off files contain matching ACCTNO/NOTENO |
| **SPOTRATE not updated** | Verify CURCODE exists in FX rate file                |
| **Processing skipped**   | Check date or set ALWAYS_RUN = True                  |
| **Missing FX rates**     | Check foratebkp.parquet has recent dates             |

## Validation Checklist

After running:
- [ ] Output files created in output/ directory
- [ ] Record counts match input (no records lost)
- [ ] LOANTYPE updated where PRODUCT/ORICODE exists
- [ ] CENSUS updated where CENSUS_TRT exists
- [ ] SPOTRATE populated for non-MYR currencies
- [ ] MYR records have null/original SPOTRATE
- [ ] FX rates are most recent (up to yesterday)

## Performance Notes

- **Polars**: Fast aggregations and joins
- **No Unnecessary Sorting**: Removed sorts that don't affect results
- **Efficient Lookups**: Dictionary for FX rates
- **Single-Pass Updates**: All updates in one operation
- **Graceful Degradation**: Continues if optional files missing

## Example Update Scenarios

### Scenario 1: Full Update
```
Before: LOANTYPE=100, CENSUS=1.0, CURCODE='USD'
WOF:    PRODUCT=200, ORICODE=300, CENSUS_TRT=2.5
FX:     USD=4.125

After:  LOANTYPE=300 (from ORICODE - priority)
        CENSUS=2.5 (from CENSUS_TRT)
        SPOTRATE=4.125 (from FX lookup)
```

### Scenario 2: Partial Update
```
Before: LOANTYPE=100, CENSUS=1.0, CURCODE='MYR'
WOF:    PRODUCT=200, ORICODE=0, CENSUS_TRT=0
FX:     (not applicable for MYR)

After:  LOANTYPE=200 (from PRODUCT)
        CENSUS=1.0 (unchanged - CENSUS_TRT is 0)
        SPOTRATE=null (MYR not updated)
```

### Scenario 3: No Updates
```
Before: LOANTYPE=100, CENSUS=1.0, CURCODE='SGD'
WOF:    (no matching record)
FX:     SGD=3.089

After:  LOANTYPE=100 (unchanged)
        CENSUS=1.0 (unchanged)
        SPOTRATE=3.089 (from FX lookup)
```
