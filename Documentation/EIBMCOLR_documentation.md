# EIBMCOLR Report - SAS to Python Conversion

## Overview

This Python script is a complete conversion of the SAS program **EIBMCOLR** for generating comprehensive loan and advances reports by collateral type for Public Bank Berhad.

## Program Details

- **Program**: EIBMCOLR
- **Date**: 23.03.01
- **Purpose**: Bank's Total Loans and Advances by Collaterals
- **Requestor**: Financial Accounting, Finance Division
- **SMR/Others**: A277

## Requirements

```bash
pip install duckdb polars pyarrow python-dateutil --break-system-packages
```

## Input Files

All input files should be in Parquet format in the `input/` directory:

### 1. reptdate.parquet
- **Columns**: `REPTDATE` (date)
- **Purpose**: Report date for macro variable generation

### 2. loan{MMWK}.parquet
- **Naming**: `loan` + 2-digit month + 1-digit week
- **Example**: `loan021.parquet` (February, Week 1)
- **Columns**: 
  - `ACCTNO`, `NOTENO`, `CUSTCODE`, `RISKCD`, `RISKRTE`, `NAME`
  - `PRODUCT`, `PRODCD`, `BRANCH`, `CURBAL`, `INTAMT`, `NETPROC`
  - `LOANSTAT`, `APPVALUE`, `FEEAMT`, `BALANCE`, `CLOSEDTE`, `APPRLIMT`
  - `ISSDTE`, `RLEASAMT`, `APPRLIM2`, `ACCTYPE`, `COL1`, `COL2`
  - `AMTIND`, `LIMIT1`, `LIMIT2`, `FISSPURP`

### 3. branch.parquet
- **Columns**: `BRANCH` (integer), `BRHCODE` (string)
- **Purpose**: Branch information

### 4. totiis{MM}.parquet
- **Naming**: `totiis` + 2-digit month
- **Example**: `totiis02.parquet`
- **Columns**: `ACCTNO`, `NOTENO`, `TOTIIS`
- **Purpose**: Interest in suspense amounts

### 5. eibmcolr_rcat_prev.dat (Optional)
- **Format**: Binary flat file
- **Purpose**: Previous month's data for variance report
- **Structure**:
  - Line 1: `YYYYMMDD`
  - Data lines: Fixed-width (Branch, RiskCat, BNMCode, Balance, TOTIIS, NetBal)
  - Last line: `EOF`

## Output Files

All output files are generated in the `output/` directory:

### 1. eibmcolr_by_branch.txt
- **Type**: Text report with ASA carriage control
- **Content**: Loans by branch and risk category
- **Features**: Page breaks by branch

### 2. eibmcolr_summary_all.txt
- **Type**: Text report with ASA carriage control
- **Content**: Summary for all loans (Conventional + Islamic)

### 3. eibmcolr_summary_conv.txt
- **Type**: Text report with ASA carriage control
- **Content**: Summary for conventional loans only (AMTIND='D')

### 4. eibmcolr_summary_islamic.txt
- **Type**: Text report with ASA carriage control
- **Content**: Summary for Islamic loans only (AMTIND='I')

### 5. eibmcolr_variance.txt
- **Type**: Text report with ASA carriage control
- **Content**: Variance report comparing current vs previous month
- **Note**: Only generated if previous month data exists

### 6. eibmcolr_rcat.dat
- **Type**: Binary flat file
- **Content**: Current month data for next month's variance report
- **Structure**:
  - Line 1: Date (YYYYMMDD)
  - Data: Fixed-width records
  - Last line: EOF

## Key Features

### 1. Week Calculation Logic
Based on report date day:
- Day 8: Week 1, Start Day 1, Previous Week 4
- Day 15: Week 2, Start Day 9, Previous Week 1
- Day 22: Week 3, Start Day 16, Previous Week 2
- Other: Week 4, Start Day 23, Previous Week 3

### 2. Liability Code Extraction
For OD (Overdraft) accounts:
- Extracts from `COL1` and `COL2` fields
- Priority: LIAB1 → LIAB2 → LIAB3 → LIAB4
- Positions: COL1[0:2], COL1[3:5], COL2[0:2], COL2[3:5]

### 3. Collateral Classification

| Liability Code | BNM Code | Risk Category | Notes |
|---------------|----------|---------------|-------|
| 007, 012, 013, 014, 024, 048, 049, 117 | 30307 | 0% | |
| 021 | 30309 | 0% | |
| 017, 026, 029 | 30009/17 | 10% | |
| 006, 016 | 30323 | 20% | |
| 011, 030 | 30325 | 20% | |
| 018, 027 | 30327 | 20% | |
| 003 | 30009/10 | 20% | |
| 025 | 30335 | 20% | |
| 050, 118 | 30341 | 50% or 100% | Depends on FISSPURP |
| 019, 028, 031 | 30351 | 100% | |
| Others | 30359 | 100% | |

**Special Case for 050/118**:
- Risk Category = 50% if FISSPURP in ['0311', '0312', '0313', '0314', '0315', '0316']
- Risk Category = 100% otherwise

### 4. Collateral Value Logic (LN Accounts Only)

**Scenario 3**: When `APPVALUE > 0` and `APPVALUE < BALANCE` and `LIABCOD1 != '050'`:
1. First record: `BALANCE = APPVALUE` (original classification)
2. Second record: `BALANCE = Original BALANCE - APPVALUE`, `BNMCODE = '30359'`, `RISKCAT = 100`

This splits the loan into covered and uncovered portions.

### 5. Net Balance Calculation

```
NETBAL = BALANCE - TOTIIS
```

Where:
- `BALANCE`: Loan balance
- `TOTIIS`: Total Interest In Suspense
- `NETBAL`: Net balance after deducting IIS

### 6. Report Types

#### By Branch Report (PROC PRINT with PAGEBY)
- Groups by BRANCH, then RISKCAT
- Shows BNMCODE details
- Subtotals by RISKCAT
- Totals by BRANCH
- Page break per branch

#### Summary Reports (PROC PRINT)
- All Loans: `_TYPE_ = 6` (grouped by RISKCAT, BNMCODE)
- Conventional: `_TYPE_ = 7` and `AMTIND = 'D'`
- Islamic: `_TYPE_ = 7` and `AMTIND = 'I'`

#### Variance Report
- Compares current month vs previous month
- Shows: Current Balance, Current IIS, Current Net, Variance, Previous Net
- Grouped by RISKCAT and BNMCODE
- Page break per BNMCODE

## Usage

```bash
python eibmcolr_report.py
```

### Directory Structure

```
.
├── input/
│   ├── reptdate.parquet
│   ├── loan021.parquet
│   ├── branch.parquet
│   ├── totiis02.parquet
│   └── eibmcolr_rcat_prev.dat (optional)
├── output/
│   ├── eibmcolr_by_branch.txt
│   ├── eibmcolr_summary_all.txt
│   ├── eibmcolr_summary_conv.txt
│   ├── eibmcolr_summary_islamic.txt
│   ├── eibmcolr_variance.txt (if prev data exists)
│   └── eibmcolr_rcat.dat
└── eibmcolr_report.py
```

## Data Filters

### Excluded Products
- `PRODCD` in ['N', '54120']
- `PRODUCT` in [500, 520]

### Included Accounts
- All accounts except those matching exclusion criteria
- `CURBAL >= 0` (implicitly through data)

## Output Format Details

### ASA Carriage Control Characters
- `'1'`: New page (form feed)
- `' '`: Single spacing
- `'0'`: Double spacing
- `'-'`: Triple spacing

### Column Formatting
- **BNMCODE**: Left-aligned, 12 characters
- **BALANCE**: Right-aligned, comma-formatted, 2 decimals, 22 characters
- **TOTIIS**: Right-aligned, comma-formatted, 2 decimals, 22 characters
- **NETBAL**: Right-aligned, comma-formatted, 2 decimals, 22 characters
- **VARBAL**: Right-aligned, comma-formatted, 2 decimals, 22 characters

### Risk Category Display
- 0 → `  0%`
- 10 → ` 10%`
- 20 → ` 20%`
- 50 → ` 50%`
- 100 → `100%`

## Flat File Format (RCAT)

### Structure
```
Position  Length  Field       Format
----------------------------------------
Record 1: Date Header
1-8       8       Date        YYYYMMDD

Data Records:
1-3       3       BRANCH      Numeric, zero-padded
4-6       3       RISKCAT     Numeric, zero-padded
7-14      8       BNMCODE     Character
15-29     15      BALANCE     Numeric (no decimal point)
46-60     15      TOTIIS      Numeric (no decimal point)
61-75     15      NETBAL      Numeric (no decimal point)

Last Record:
1-3       3       EOF         Literal "EOF"
```

### Example
```
20240215
001000303070000000123456700000000000000000000000123456700
002020303230000000987654300000000000000000000000987654300
EOF
```

## Processing Flow

```
1. Read REPTDATE
   ↓
2. Calculate macro variables (week, month, year)
   ↓
3. Read LOAN file
   ↓
4. Filter products
   ↓
5. Extract liability codes (OD accounts)
   ↓
6. Classify collateral (BNM code + risk category)
   ↓
7. Handle collateral value (split LN records if needed)
   ↓
8. Separate OD and LN accounts
   ↓
9. Merge TOTIIS data (LN accounts only)
   ↓
10. Combine OD and LN
    ↓
11. Calculate NETBAL
    ↓
12. Merge branch information
    ↓
13. Summarize by branch/risk/BNM code
    ↓
14. Generate reports:
    - By Branch
    - All Loans Summary
    - Conventional Summary
    - Islamic Summary
    ↓
15. Write RCAT flat file
    ↓
16. Read previous RCAT file
    ↓
17. Generate variance report
```

## Error Handling

The program includes comprehensive error handling:
- File existence validation
- Missing data handling (fills with 0)
- Optional variance report (skips if no previous data)
- Informative error messages with stack traces

## Performance Optimizations

1. **DuckDB**: Efficient Parquet file reading with SQL filtering
2. **Polars**: Fast aggregations and joins
3. **Streaming**: Processes data in chunks where possible
4. **Reduced Sorting**: Only sorts when necessary for output
5. **Single Pass**: Combines operations where possible

## Notes

### Differences from SAS

1. **File Handling**: Uses Parquet instead of SAS datasets
2. **Flat File**: Binary .dat format for RCAT output
3. **Reports**: All reports use ASA carriage control
4. **Type Column**: `_TYPE_` explicitly created (SAS auto-generates)
5. **Sorting**: Optimized to reduce unnecessary sorts

### Branch File Format

The branch file is read as Parquet with columns:
- `BRANCH`: Integer (e.g., 1, 2, 3)
- `BRHCODE`: String (e.g., 'PBB', 'KLG')

Combined as: `BRBOTH = '001-PBB'`

### TOTIIS Merge

- Only merges with LN accounts (ACCTYPE='LN')
- OD accounts have TOTIIS = 0
- Missing TOTIIS values filled with 0

## Troubleshooting

| Issue | Solution |
|-------|----------|
| FileNotFoundError | Verify input files exist with correct naming |
| Empty reports | Check REPTDATE and LOAN data are valid |
| No variance report | Previous RCAT file must exist |
| Format errors | Verify column names match specification |
| Missing TOTIIS | File is optional, will use 0 if missing |

## Verification Checklist

- [ ] All 6 output files generated
- [ ] Report totals match expectations
- [ ] Risk categories correctly assigned
- [ ] Collateral split logic applied (Scenario 3)
- [ ] Variance calculations accurate
- [ ] RCAT file format correct
- [ ] ASA carriage control present
- [ ] Branch page breaks working
