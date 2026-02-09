# EIBMBAEI Report - SAS to Python Conversion

## Overview

This project converts the EIBMBAEI SAS program to Python, generating a comprehensive Profile Report on BAE Personal Financing-I Customers for Public Islamic Bank Berhad. The report provides 7 detailed distribution analyses using PROC TABULATE format.

## Table of Contents

- [Report Components](#report-components)
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

## Report Components

The EIBMBAEI report generates **7 distribution analyses**:

1. **Distribution by State** - Geographic distribution across Malaysian states
2. **Distribution by Financing Limit** - Approved limit ranges (RM 10K to >150K)
3. **Distribution by Tenure** - Financing period (24 to 240 months)
4. **Distribution by Race** - Demographic breakdown (Malay, Chinese, Indian, Other)
5. **Distribution by Age** - Age group analysis (18-58 years)
6. **Distribution by Gender** - Gender distribution (Male, Female, N/A)
7. **Distribution by Salary Range** - Income distribution for monthly approvals

Each report shows:
- Number of accounts
- Percentage distribution
- Total amount (RM)
- Percentage of total amount

## Project Structure

```
.
├── EIBMBAEI_report.py               # Main report generation script
├── generate_sample_data_eibmbaei.py # Sample data generator
├── README_EIBMBAEI.md               # This file
├── /data/
│   ├── /input/                      # Input parquet files
│   │   ├── reptdate.parquet
│   │   ├── loan_064.parquet         # Current month loan data
│   │   ├── cisln_loan.parquet       # Customer demographic data
│   │   ├── ieln0614.parquet         # ELDS week 1
│   │   ├── ieln0624.parquet         # ELDS week 2
│   │   ├── ieln0634.parquet         # ELDS week 3
│   │   └── ieln0644.parquet         # ELDS week 4
│   └── /output/                     # Generated reports
│       └── MATURE_BAE.txt
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
   python generate_sample_data_eibmbaei.py
   ```

## Data Files

### Input Files

All input files must be in Parquet format and located in `/data/input/`:

| File Name | Description | Key Columns |
|-----------|-------------|-------------|
| `reptdate.parquet` | Reporting date | REPTDATE |
| `loan_MMWK.parquet` | Loan master file | ACCTNO, PRODUCT, STATECD, NETPROC, APPRLIM2, BALANCE, NOTETERM |
| `cisln_loan.parquet` | Customer demographics | ACCTNO, SECCUST, RACE, GENDER, BIRTHDAT |
| `ielnMMWKYY.parquet` | ELDS approval data | PRODUCT, STATUS, GINCOME, AMOUNT |

**Naming Convention:**
- `MM` = Month (01-12)
- `WK` = Week (1-4)
- `YY` = Year (2-digit)

### Output File

| File Name | Description | Format |
|-----------|-------------|--------|
| `MATURE_BAE.txt` | Profile report | Text with ASA carriage control |

## Usage

### Step 1: Generate Sample Data (First Time Only)

```bash
python generate_sample_data_eibmbaei.py
```

**Expected Output:**
```
======================================================================
EIBMBAEI Sample Data Generator
======================================================================
Report Date: 2024-06-30
Current Month: 06
Number of Loan Accounts: 1000
======================================================================

Generating reporting date file...
✓ Generated: /data/input/reptdate.parquet

Generating loan data...
✓ Generated: /data/input/loan_064.parquet

Generating customer demographic data (CISLN)...
✓ Generated: /data/input/cisln_loan.parquet

Generating ELDS approval data for all weeks...
✓ Generated: /data/input/ieln0614.parquet
✓ Generated: /data/input/ieln0624.parquet
✓ Generated: /data/input/ieln0634.parquet
✓ Generated: /data/input/ieln0644.parquet
```

### Step 2: Generate Report

```bash
python EIBMBAEI_report.py
```

**Expected Output:**
```
Report Date: 30/06/24
Report Month: 07

Loading loan data...
✓ Loaded 1000 loan records

Generating Report 1: Distribution by State...
Generating Report 2: Distribution by Financing Limit...
Generating Report 3: Distribution by Tenure...
Loading customer demographic data...
Generating Report 4: Distribution by Race...
Generating Report 5: Distribution by Age...
Generating Report 6: Distribution by Gender...
Loading ELDS approval data...
✓ All data processed

Writing report to: /data/output/MATURE_BAE.txt
✓ Report generated successfully: /data/output/MATURE_BAE.txt

======================================================================
EIBMBAEI Report Generation Complete!
======================================================================
```

### Step 3: View Report

```bash
cat /data/output/MATURE_BAE.txt
```

Or open in a text editor that supports ASA carriage control characters.

## Report Specifications

### Product Filter
- **Product Code**: 135 (BAE Personal Financing-I)
- All reports focus exclusively on this product

### Report Structure

Each of the 7 reports follows this format:

```
┌────────────────────────────────────────────────────────────────────────────┐
│ PUBLIC ISLAMIC BANK BERHAD: REPORT ID: EIBMBAEI                            │
│ PROFILE ON BAE PERSONAL FINANCING-I CUSTOMERS DD/MM/YY                     │
│ N.  DISTRIBUTION BY [CATEGORY]                                             │
│ COORBRH=IBU                                                                │
│                                                                            │
│ [CATEGORY LABEL]          | NO OF A/C |PERCENTAGE| AMOUNT(RM)    |PERCENTAGE│
│                            |           |   (%)    |              |   (%)    │
│ ────────────────────────────────────────────────────────────────────────── │
│ [Row 1]                    |     XXX   |   XX.XX  |  XXX,XXX.XX  |  XX.XX   │
│ [Row 2]                    |     XXX   |   XX.XX  |  XXX,XXX.XX  |  XX.XX   │
│ ...                                                                        │
│ ────────────────────────────────────────────────────────────────────────── │
│ TOTAL                      |   X,XXX   |  100.00  | X,XXX,XXX.XX | 100.00   │
│ ────────────────────────────────────────────────────────────────────────── │
└────────────────────────────────────────────────────────────────────────────┘
```

## Data Dictionary

### Loan File Columns

| Column | Type | Description | Values/Format |
|--------|------|-------------|---------------|
| ACCTNO | String | Account number | Unique identifier |
| PRODUCT | Integer | Product code | 135 = BAE Personal Financing-I |
| STATECD | String | State code | A-W (see state mapping) |
| NETPROC | Float | Net proceeds | Decimal(15,2) |
| APPRLIM2 | Float | Approved limit 2 | Decimal(15,2) |
| BALANCE | Float | Outstanding balance | Decimal(15,2) |
| NOTETERM | Integer | Note term (months) | 27-243 (actual term + 3) |
| OPENDATE | Date | Account open date | YYYY-MM-DD |

### Customer Demographics (CISLN) Columns

| Column | Type | Description | Values/Format |
|--------|------|-------------|---------------|
| ACCTNO | String | Account number | Matches loan file |
| SECCUST | String | Customer type | '901' = Primary customer |
| RACE | String | Race code | '1'=Malay, '2'=Chinese, '3'=Indian, '4'=Other |
| GENDER | String | Gender | 'M'=Male, 'F'=Female |
| BIRTHDAT | String | Birth date | DDMMYYYY |

### ELDS Approval Data Columns

| Column | Type | Description | Values/Format |
|--------|------|-------------|---------------|
| PRODUCT | Integer | Product code | 135 |
| STATUS | String | Approval status | 'APPROVED' |
| GINCOME | Float | Gross monthly income | Decimal(10,2), nullable |
| AMOUNT | Float | Approved amount | Decimal(15,2) |
| APPROVEDATE | Date | Approval date | YYYY-MM-DD |

## Business Logic

### State Code Mapping

| Code | State Name |
|------|------------|
| A | PERAK |
| B | SELANGOR |
| C | PAHANG |
| D | KELANTAN |
| J | JOHOR |
| K | KEDAH |
| L | WILAYAH LABUAN |
| M | MELAKA |
| N | NEGERI SEMBILAN |
| P | PENANG |
| Q | SARAWAK |
| R | PERLIS |
| S | SABAH |
| T | TERENGGANU |
| W | WILAYAH PERSEKUTUAN |

### Financing Limit Categories (LMTGRP)

Based on the higher of NETPROC or APPRLIM2 (if APPRLIM2 > 150,000):

| Category | Range (RM) |
|----------|------------|
| 01 | Up to 10,000 |
| 02 | > 10,000 - 15,000 |
| 03 | > 15,000 - 20,000 |
| 04 | > 20,000 - 30,000 |
| 05 | > 30,000 - 40,000 |
| 06 | > 40,000 - 50,000 |
| 07 | > 50,000 - 60,000 |
| 08 | > 60,000 - 70,000 |
| 09 | > 70,000 - 80,000 |
| 10 | > 80,000 - 90,000 |
| 11 | > 90,000 - 100,000 |
| 12 | > 100,000 - 110,000 |
| 13 | > 110,000 - 120,000 |
| 14 | > 120,000 - 130,000 |
| 15 | > 130,000 - 140,000 |
| 16 | > 140,000 - 150,000 |
| 17 | > 150,000 |

### Tenure Categories

Original SAS code subtracts 3 from NOTETERM, so actual values are:

| Category | Months |
|----------|--------|
| 01 | 24 |
| 02 | 36 |
| 03 | 48 |
| 04 | 60 |
| 05 | 72 |
| 06 | 84 |
| 07 | 96 |
| 08 | 108 |
| 09 | 120 |
| 10 | 132 |
| 11 | 144 |
| 12 | 156 |
| 13 | 168 |
| 14 | 180 |
| 15 | 192 |
| 16 | 204 |
| 17 | 216 |
| 18 | 228 |
| 19 | 240 |

### Age Group Categories

Calculated as: Report Year - Birth Year

| Category | Age Range |
|----------|-----------|
| 1 | 18 - 30 |
| 2 | 31 - 40 |
| 3 | 41 - 50 |
| 4 | 51 - 55 |
| 5 | 56 - 58 |

### Salary Range Categories (Report 7)

Based on GINCOME from ELDS data:

| Category | Range (RM) |
|----------|------------|
| 01 | Below 1,000 |
| 02 | 1,000 (exactly) |
| 03 | > 1,000 - 1,500 |
| 04 | > 1,500 - 2,000 |
| 05 | > 2,000 - 2,500 |
| 06 | > 2,500 - 3,000 |
| 07 | > 3,000 - 3,500 |
| 08 | > 3,500 - 4,000 |
| 09 | > 4,000 - 4,500 |
| 10 | > 4,500 - 5,000 |
| 11 | > 5,000 - 5,500 |
| 12 | > 5,500 - 6,000 |
| 13 | > 6,000 - 6,500 |
| 14 | > 6,500 - 7,000 |
| 15 | > 7,000 - 7,500 |
| 16 | > 7,500 - 8,000 |
| 17 | > 8,000 - 9,000 |
| 18 | > 9,000 - 10,000 |
| 19 | Above 10,000 |
| 20 | N/A (null values) |

**Note**: Null GINCOME values are set to 99999999 and categorized as "N/A"

## Output Format

### ASA Carriage Control Characters

The report uses ASA (American Standards Association) carriage control characters:

| Character | Meaning | Usage |
|-----------|---------|-------|
| ' ' (space) | Single spacing | Normal line advancement |
| '1' | New page | Start of each report section |

### Sample Output (Report 1 - Distribution by State)

```
1PUBLIC ISLAMIC BANK BERHAD: REPORT ID: EIBMBAEI
 PROFILE ON BAE PERSONAL FINANCING-I CUSTOMERS 30/06/24
 1.  DISTRIBUTION BY STATE
 COORBRH=IBU
 
 STATE                         |          |          |              |                  |              |
                               |          |          |    AMOUNT(RM)    |                  |
                               | NO OF A/C|PERCENTAGE|                  |  PERCENTAGE  |
                               |          |    (%)   |                  |     (%)      |
 ----------------------------------------------------------------------------------------------------
 01. PERAK                     |        45|     4.50|       2,345,678.90|          5.23|
 02. SELANGOR                  |       195|    19.50|      10,234,567.80|         22.81|
 03. PAHANG                    |        48|     4.80|       2,456,789.12|          5.48|
 ...
 ----------------------------------------------------------------------------------------------------
 TOTAL                         |     1,000|   100.00|      44,856,234.56|        100.00|
 ----------------------------------------------------------------------------------------------------
```

## Troubleshooting

### Common Issues

#### 1. File Not Found Error

**Error:**
```
FileNotFoundError: [Errno 2] No such file or directory: '/data/input/loan_064.parquet'
```

**Solution:**
- Ensure input directory exists: `mkdir -p /data/input`
- Run sample data generator: `python generate_sample_data_eibmbaei.py`
- Verify file names match expected pattern (month and week)

#### 2. Missing PRODUCT 135 Data

**Issue:** Report shows "No data found" or empty tables

**Solution:**
- Verify loan data contains PRODUCT = 135
- Check that PRODUCT column is integer type, not string
- Ensure sample data generator ran successfully

#### 3. Missing Demographic Data

**Issue:** Reports 4-6 show zeros or missing data

**Solution:**
- Verify CISLN file exists and contains data
- Check SECCUST filter value is '901'
- Ensure ACCTNO values match between loan and CISLN files
- Verify birthdate format is DDMMYYYY (8 characters)

#### 4. Empty Report 7 (Salary Distribution)

**Issue:** Report 7 shows all zeros

**Solution:**
- Check that ELDS files exist for all 4 weeks
- Verify STATUS = 'APPROVED' filter
- Ensure PRODUCT = 135 in ELDS data
- Check file naming convention: `ielnMMWKYY.parquet`

#### 5. Percentage Calculation Errors

**Issue:** Percentages don't sum to 100% or show unusual values

**Solution:**
- Verify no negative balances or amounts
- Check for duplicate account numbers
- Ensure all required categories are present (use dummy data)
- Review total calculation logic

#### 6. Date Format Issues

**Error:**
```
ValueError: Invalid birthdate format
```

**Solution:**
- Ensure BIRTHDAT is string type in format DDMMYYYY
- Check for null or malformed birth dates
- Verify birth year extraction (characters 5-8)

### Data Validation Checklist

Before running the report, verify:

- [ ] All required input files exist in `/data/input/`
- [ ] PRODUCT = 135 exists in loan data
- [ ] NOTETERM values are realistic (27-243)
- [ ] STATECD contains valid codes (A-W)
- [ ] NETPROC and BALANCE are positive numbers
- [ ] CISLN contains SECCUST = '901' records
- [ ] RACE codes are '1', '2', '3', or '4'
- [ ] GENDER is 'M', 'F', or null
- [ ] BIRTHDAT format is DDMMYYYY (8 chars)
- [ ] ELDS files exist for weeks 1-4
- [ ] ELDS STATUS = 'APPROVED'
- [ ] GINCOME values are reasonable or null

## Technical Notes

### Report Date Logic

The SAS code uses `REPTDAT1 = REPTDATE + 1` for month calculation:

```python
reptdat1 = reptdate + timedelta(days=1)
mm = reptdat1.month
```

This means if REPTDATE is the last day of the month (e.g., 2024-06-30), the report month will be the next month (07).

### NOTETERM Adjustment

Original SAS: `NOTETERM = NOTETERM - 3`

The stored NOTETERM values are 3 months higher than the actual term. For example:
- Stored: 27 → Actual: 24 months
- Stored: 243 → Actual: 240 months

### NETPROC vs APPRLIM2 Logic

```python
IF APPRLIM2 > 150000 THEN NETPROC = APPRLIM2
```

For financing limits above RM 150,000, APPRLIM2 is used instead of NETPROC for categorization.

### Null Income Handling (Report 7)

```python
IF GINCOME = . THEN GINCOME = 99999999
```

Null income values are replaced with a large number (99999999) which categorizes them as "N/A".

### Dummy Data Pattern

To ensure all categories appear in reports (even with zero values), the code creates dummy dataframes with all possible category values:

```python
dum2 = pl.DataFrame({
    "LMTGRP": all_categories,
    "NOACCT": [0] * len(all_categories),
    "BALANCE": [0.0] * len(all_categories)
})
```

This is then merged with actual data using left join.

## Performance Optimization

For large datasets (>100,000 accounts):

1. **Use DuckDB filtering:**
   ```python
   query = """
       SELECT * FROM read_parquet('file.parquet')
       WHERE PRODUCT = 135
   ```
   ```

2. **Partition ELDS files by week:**
   - Process each week separately
   - Combine results at the end

3. **Index on ACCTNO:**
   - Ensures efficient joins between loan and CISLN data

## Maintenance

### Adding New State Codes

To add new states:

1. Update state mapping in the code
2. Add corresponding display label
3. Update documentation

### Modifying Category Ranges

To change financing limit or salary ranges:

1. Update the categorization logic
2. Update dummy data categories
3. Adjust report labels
4. Update documentation

### Customizing Report Format

To modify report column widths or formats:

1. Update `generate_tabulate_report()` function
2. Adjust format strings and column positions
3. Test with sample data

## Support

For issues or questions:

1. Check troubleshooting section
2. Verify sample data generation works correctly
3. Review SAS source code for business logic
4. Check Polars/DuckDB documentation for syntax

## Version History

**Version 1.0** (2024)
- Initial conversion from SAS
- Full 7-report functionality
- PROC TABULATE format replication
- Sample data generator
- Comprehensive documentation

## License

Internal use only - Public Islamic Bank Berhad

---

**Last Updated:** February 2024
**Author:** SAS to Python Conversion Project
**Contact:** IT Department
