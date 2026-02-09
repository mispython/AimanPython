# EIIMMEF2 Report - SAS to Python Conversion

## Overview

This project converts the EIIMMEF2 SAS program to Python, generating a Performance Report on Product 428 and 439 for Public Islamic Bank Berhad. The report includes:

1. **MEF Report** - Disbursement, Repayment, and Outstanding balances
2. **Impaired Loans (IL) Report** - Categorized by months in arrears

## Table of Contents

- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Installation](#installation)
- [Data Files](#data-files)
- [Usage](#usage)
- [Report Specifications](#report-specifications)
- [Data Dictionary](#data-dictionary)
- [Business Logic](#business-logic)
- [Output Format](#output-format)
- [Troubleshooting](#troubleshooting)

## Project Structure

```
.
├── EIIMMEF2_report.py          # Main report generation script
├── generate_sample_data.py      # Sample data generator
├── README.md                    # This file
├── /data/
│   ├── /input/                  # Input parquet files
│   │   ├── reptdate.parquet
│   │   ├── loan_024.parquet     # Previous month
│   │   ├── loan_034.parquet     # Current month
│   │   ├── idispaymth03.parquet
│   │   └── icredmsubac0324.parquet
│   └── /output/                 # Generated reports
│       └── PBMEF2.txt
```

## Requirements

### Python Version
- Python 3.8 or higher

### Dependencies

```bash
pip install polars duckdb pyarrow numpy
```

**Package Versions:**
- `polars >= 0.19.0`
- `duckdb >= 0.9.0`
- `pyarrow >= 10.0.0`
- `numpy >= 1.24.0`

## Installation

1. **Clone or download the project files**

2. **Install dependencies:**
   ```bash
   pip install polars duckdb pyarrow numpy
   ```

3. **Create directory structure:**
   ```bash
   mkdir -p /data/input
   mkdir -p /data/output
   ```

4. **Generate sample data (optional):**
   ```bash
   python generate_sample_data.py
   ```

## Data Files

### Input Files

All input files must be in Parquet format and located in `/data/input/`:

| File Name | Description | Key Columns |
|-----------|-------------|-------------|
| `reptdate.parquet` | Reporting date | REPTDATE |
| `loan_MMWK.parquet` | Loan master file | ACCTNO, NOTENO, BRANCH, PRODUCT, CENSUS, BALANCE, PAIDIND, LOANSTAT |
| `idispaymthMM.parquet` | Disbursement/Repayment | ACCTNO, NOTENO, DISBURSE, REPAID |
| `icredmsubacMMYY.parquet` | CCRIS credit data | ACCTNUM, NOTENO, BRANCH, DAYSARR, MTHARR |

**Naming Convention:**
- `MM` = Month (01-12)
- `WK` = Week (typically 4)
- `YY` = Year (2-digit)

### Output File

| File Name | Description | Format |
|-----------|-------------|--------|
| `PBMEF2.txt` | Performance report | Text with ASA carriage control |

## Usage

### Step 1: Generate Sample Data (First Time Only)

```bash
python generate_sample_data.py
```

**Expected Output:**
```
======================================================================
EIIMMEF2 Sample Data Generator
======================================================================
Report Date: 2024-03-31
Current Month: 03
Previous Month: 02
Number of Accounts: 500
======================================================================

Generating reporting date file...
✓ Generated: /data/input/reptdate.parquet

Generating previous month loan data...
✓ Generated: /data/input/loan_024.parquet

Generating current month loan data...
✓ Generated: /data/input/loan_034.parquet

Generating disbursement and payment data...
✓ Generated: /data/input/idispaymth03.parquet

Generating CCRIS credit submission data...
✓ Generated: /data/input/icredmsubac0324.parquet
```

### Step 2: Generate Report

```bash
python EIIMMEF2_report.py
```

**Expected Output:**
```
Report Date: 31/03/24
Current Month: 03
Previous Month: 02

Processing MEF Report (Disbursement, Repayment, Outstanding)...
✓ MEF Report data processed

Processing Impaired Loans (IL) Report...
✓ IL Report data processed

Writing report to: /data/output/PBMEF2.txt
✓ Report generated successfully: /data/output/PBMEF2.txt

======================================================================
EIIMMEF2 Report Generation Complete!
======================================================================
```

### Step 3: View Report

```bash
cat /data/output/PBMEF2.txt
```

Or open in a text editor that supports ASA carriage control characters.

## Report Specifications

### Report Components

#### 1. MEF Report (Main Performance Report)

**Columns:**
- **Type**: DISBURSEMENT, REPAYMENT, OUTSTANDING
- **With CGC Guaranteed**: Number of accounts and amount
- **Without CGC Guaranteed**: Number of accounts and amount
- **Total**: Combined totals

**Classification:**
- **With CGC**: CENSUS in (428.00, 428.02)
- **Without CGC**: CENSUS in (428.01, 428.03) OR PRODUCT = 439

#### 2. Impaired Loans (IL) Report

**Categories by Months in Arrears:**
- **A**: < 3 months
- **B**: 3 to less than 6 months
- **C**: 6 to less than 9 months
- **D**: >= 9 months

**Criteria:**
- LOANSTAT = 3 (Non-performing)
- BALANCE > 0

## Data Dictionary

### Loan File Columns

| Column | Type | Description | Values/Format |
|--------|------|-------------|---------------|
| ACCTNO | String | Account number | Unique identifier |
| NOTENO | String | Note number | Sub-account identifier |
| BRANCH | String | Branch code | e.g., BR001 |
| PRODUCT | Integer | Product code | 428, 439 |
| CENSUS | Float | Census classification | 428.00, 428.01, 428.02, 428.03, 439.00 |
| BALANCE | Float | Outstanding balance | Decimal(15,2) |
| PAIDIND | String | Paid indicator | 'P'=Paid, 'C'=Closed, 'A'=Active |
| LOANSTAT | Integer | Loan status | 1=Performing, 3=Non-performing |
| CUSTNAME | String | Customer name | Text |
| OPENDATE | Date | Account open date | YYYY-MM-DD |

### Disbursement/Repayment File Columns

| Column | Type | Description | Values/Format |
|--------|------|-------------|---------------|
| ACCTNO | String | Account number | Matches loan file |
| NOTENO | String | Note number | Matches loan file |
| DISBURSE | Float | Disbursement amount | Decimal(15,2) |
| REPAID | Float | Repayment amount | Decimal(15,2) |
| TRANSDATE | Date | Transaction date | YYYY-MM-DD |

### CCRIS Credit Data Columns

| Column | Type | Description | Values/Format |
|--------|------|-------------|---------------|
| ACCTNUM | String | Account number | Matches ACCTNO |
| NOTENO | String | Note number | Matches loan file |
| BRANCH | String | Branch code | Matches loan file |
| DAYSARR | Integer | Days in arrears | Days |
| MTHARR | Integer | Months in arrears | Months |
| REPORTDATE | Date | Report date | YYYY-MM-DD |

## Business Logic

### MEF Report Processing

1. **Data Selection:**
   ```
   - Filter loans: PRODUCT IN (428, 439)
   - Filter active loans: PAIDIND NOT IN ('P', 'C') AND BALANCE != 0
   ```

2. **CGC Classification:**
   ```
   WITH CGC:
   - CENSUS IN (428.00, 428.02)
   
   WITHOUT CGC:
   - CENSUS IN (428.01, 428.03) OR PRODUCT = 439
   ```

3. **Transaction Types:**
   ```
   DISBURSEMENT:
   - Records where DISBURSE > 0
   
   REPAYMENT:
   - Records where REPAID > 0
   
   OUTSTANDING:
   - Current BALANCE for all active accounts
   ```

4. **Aggregation:**
   - Sum amounts by transaction type and CGC classification
   - Count number of accounts (NOACC)

### Impaired Loans (IL) Report Processing

1. **Selection Criteria:**
   ```
   - LOANSTAT = 3 (Non-performing)
   - BALANCE > 0
   - PRODUCT IN (428, 439)
   ```

2. **Arrears Categorization:**
   ```
   A: MTHARR < 3
   B: 3 <= MTHARR < 6
   C: 6 <= MTHARR < 9
   D: MTHARR >= 9
   ```

3. **Data Source:**
   - Arrears information from CCRIS credit submission data

### Calculation Examples

**Example 1: Disbursement Count and Amount**
```python
# WITH CGC
NOACCT = Count of accounts with DISBURSE > 0 and CENSUS IN (428.00, 428.02)
AMOUNT = Sum of DISBURSE for these accounts

# WITHOUT CGC
NOACCTX = Count of accounts with DISBURSE > 0 and 
          (CENSUS IN (428.01, 428.03) OR PRODUCT = 439)
AMOUNTX = Sum of DISBURSE for these accounts

# TOTAL
TOTACCT = NOACCT + NOACCTX
TOTAMOUNT = AMOUNT + AMOUNTX
```

**Example 2: Impaired Loans in Category A**
```python
# WITH CGC
NOACCT = Count of accounts where:
         - CENSUS IN (428.00, 428.02)
         - LOANSTAT = 3
         - MTHARR < 3
         - BALANCE > 0
AMOUNT = Sum of BALANCE for these accounts

# WITHOUT CGC
NOACCTX = Count of accounts where:
          - (CENSUS IN (428.01, 428.03) OR PRODUCT = 439)
          - LOANSTAT = 3
          - MTHARR < 3
          - BALANCE > 0
AMOUNTX = Sum of BALANCE for these accounts
```

## Output Format

### ASA Carriage Control Characters

The report uses ASA (American Standards Association) carriage control characters in column 1:

| Character | Meaning | Usage |
|-----------|---------|-------|
| ' ' (space) | Single spacing | Normal line advancement |
| '1' | New page | Start of each report section |
| '0' | Double spacing | (Not used in this report) |
| '-' | Triple spacing | (Not used in this report) |

### Report Layout

```
Column Positions (excluding ASA character):

MEF REPORT:
Position 001-020: Type description
Position 020-028: WITH CGC - No of Accounts
Position 030-045: WITH CGC - Amount
Position 050-058: WITHOUT CGC - No of Accounts
Position 060-075: WITHOUT CGC - Amount
Position 080-088: TOTAL - No of Accounts
Position 090-105: TOTAL - Amount

IL REPORT:
Position 001-029: Category description
Position 030-038: WITH CGC - No of Accounts
Position 040-055: WITH CGC - Amount
Position 060-068: WITHOUT CGC - No of Accounts
Position 070-085: WITHOUT CGC - Amount
Position 090-098: TOTAL - No of Accounts
Position 100-115: TOTAL - Amount
```

### Sample Output

```
1PUBLIC ISLAMIC BANK BERHAD
 PERFORMANCE REPORT ON PRODUCT 428 AND 439 AS AT 31/03/24
 REPORT ID : EIIMMEF2
                           
                           
 MEF                  WITH CGC GUARANTEED          WITHOUT CGC GUARANTEED      TOTAL
                      NO ACCT     AMOUNT           NO ACCT     AMOUNT           NO ACCT     AMOUNT
 DISBURSEMENT              45   2,345,678.90            32   1,234,567.80            77   3,580,246.70
 REPAYMENT                 89   1,456,789.12            67     987,654.32           156   2,444,443.44
 OUTSTANDING              234  45,678,901.23           189  34,567,890.12           423  80,246,791.35
                   
                   
1PUBLIC ISLAMIC BANK BERHAD
 PERFORMANCE REPORT ON PRODUCT 428 AND 439 AS AT 31/03/24
 REPORT ID : EIIMMEF2 (IMPARED LOANS)
                           
                           
 MEF (IL)                     WITH CGC GUARANTEED          WITHOUT CGC GUARANTEED      TOTAL
                              NO ACCT     AMOUNT           NO ACCT     AMOUNT           NO ACCT     AMOUNT
 < 3 MTHS                          12   1,234,567.89             8     876,543.21            20   2,111,111.10
 3 TO LESS THAN 6 MTHS              8     987,654.32             5     654,321.09            13   1,641,975.41
 6 TO LESS THAN 9 MTHS              5     678,901.23             3     432,109.87             8   1,111,011.10
 >= 9 MTHS                          3     456,789.01             2     321,098.76             5     777,887.77
 TOTAL                             28   3,358,912.45            18   2,284,072.93            46   5,642,985.38
```

## Troubleshooting

### Common Issues

#### 1. File Not Found Error

**Error:**
```
FileNotFoundError: [Errno 2] No such file or directory: '/data/input/reptdate.parquet'
```

**Solution:**
- Ensure input directory exists: `mkdir -p /data/input`
- Run sample data generator: `python generate_sample_data.py`
- Verify file names match expected pattern

#### 2. Missing Columns Error

**Error:**
```
polars.exceptions.ColumnNotFoundError: BALANCE
```

**Solution:**
- Verify parquet files have correct column names
- Check data dictionary for required columns
- Regenerate sample data if necessary

#### 3. Empty Report Sections

**Issue:** Report shows zeros for all values

**Solution:**
- Check filtering conditions in loan data (PAIDIND, PRODUCT)
- Verify CENSUS codes match expected values (428.00, 428.01, etc.)
- Ensure BALANCE values are not all zero
- Check that disbursement/repayment data has matching account numbers

#### 4. Incorrect Totals

**Issue:** Totals don't sum correctly

**Solution:**
- Verify CGC classification logic (CENSUS codes)
- Check for null values in amount fields
- Ensure no duplicate records in source data
- Review join conditions between loan and dispay files

#### 5. Date Format Issues

**Error:**
```
ValueError: time data '31/03/2024' does not match format
```

**Solution:**
- Ensure REPTDATE is in YYYY-MM-DD format in parquet file
- Check datetime parsing in script
- Verify date columns are datetime type, not string

### Data Validation Checklist

Before running the report, verify:

- [ ] All required input files exist in `/data/input/`
- [ ] Parquet files can be read without errors
- [ ] PRODUCT column contains values 428 or 439
- [ ] CENSUS codes are 428.00, 428.01, 428.02, 428.03, or 439.00
- [ ] BALANCE values are numeric and not all zero
- [ ] PAIDIND contains valid values ('P', 'C', 'A', etc.)
- [ ] LOANSTAT contains numeric values (1 or 3)
- [ ] ACCTNO and NOTENO are present in all files
- [ ] Date fields are in correct format

### Performance Optimization

For large datasets (>1 million rows):

1. **Use DuckDB filtering:**
   ```python
   # Filter in SQL before loading to Polars
   query = """
       SELECT * FROM read_parquet('file.parquet')
       WHERE PRODUCT IN (428, 439)
       AND BALANCE > 0
   """
   ```

2. **Partition input files:**
   - Split loan files by month
   - Process in batches if memory constrained

3. **Index key columns:**
   - Ensure ACCTNO and NOTENO are sorted in source data
   - This speeds up join operations

## Technical Notes

### ASA Carriage Control

The output file includes ASA (American Standards Association) carriage control characters in the first position of each line. These control printer behavior:

- Legacy mainframe format
- Column 1 reserved for control character
- Data starts at column 2

Modern systems may need conversion for proper printing.

### Null Value Handling

The script replaces null/missing values with zero, matching SAS behavior:

```python
.fill_null(0)
```

This ensures calculations proceed without errors and matches the SAS `MISSING=0` option.

### Floating Point Precision

Amounts are formatted to 2 decimal places:

```python
format_number(value, width=15, decimals=2)
```

This ensures financial values display correctly.

## Maintenance

### Adding New Products

To include additional products:

1. Update product filter in both scripts:
   ```python
   WHERE PRODUCT IN (428, 439, NEW_PRODUCT)
   ```

2. Define CGC classification for new product
3. Update documentation

### Modifying Report Format

To change column positions or widths:

1. Update `format_number()` width parameters
2. Adjust column position strings in output section
3. Test alignment with sample data

### Extending Arrears Categories

To add or modify IL categories:

1. Update the categorization logic:
   ```python
   pl.when(pl.col("MTHARR") < 3).then(pl.lit("A"))
   ```

2. Add new labels to `il_labels` dictionary
3. Update documentation

## Support

For issues or questions:

1. Check troubleshooting section
2. Verify sample data generation works
3. Review SAS source code for business logic
4. Check Polars/DuckDB documentation for syntax

## Version History

**Version 1.0** (2024)
- Initial conversion from SAS
- Full MEF and IL report functionality
- Sample data generator
- Comprehensive documentation

## License

Internal use only - Public Islamic Bank Berhad

---

**Last Updated:** February 2024
**Author:** SAS to Python Conversion Project
**Contact:** IT Department
