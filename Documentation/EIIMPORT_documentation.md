# EIIMPORT Report - SAS to Python Conversion

## Overview

This project converts the EIIMPORT SAS program to Python, generating the Islamic Banking Portfolio Exposure Report for Public Islamic Bank Berhad. The report provides a comprehensive view of investment, deposit, derivatives, and sukuk holdings categorized by Shariah-approved concepts.

## Table of Contents

- [Report Purpose](#report-purpose)
- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Installation](#installation)
- [Data Files](#data-files)
- [Usage](#usage)
- [Report Specifications](#report-specifications)
- [Shariah Concepts](#shariah-concepts)
- [Data Dictionary](#data-dictionary)
- [Business Logic](#business-logic)
- [Output Format](#output-format)
- [Troubleshooting](#troubleshooting)

## Report Purpose

**Frequency**: Quarterly Basis  
**ESMR**: 2014-691  
**Description**: Generate reporting requirements for Islamic Portfolio Exposure

The report categorizes all Islamic banking products according to their underlying Shariah concepts across three main categories:
1. **Savings** - Wadiah-based deposits
2. **Current** - Various Shariah concepts (Wadiah, BBA, Murabahah, etc.)
3. **Term** - Fixed deposits, derivatives, and special arrangements

## Project Structure

```
.
├── EIIMPORT_report.py               # Main report generation script
├── generate_sample_data_eiimport.py # Sample data generator
├── README_EIIMPORT.md               # This file
├── /data/
│   ├── /input/                      # Input parquet files
│   │   ├── reptdate.parquet
│   │   ├── saving.parquet
│   │   ├── current.parquet
│   │   ├── fd.parquet
│   │   ├── iutfxYYMMDD.parquet     # Equity/derivatives
│   │   └── w1alMMWK.parquet        # INI transactions
│   └── /output/                     # Generated reports
│       └── BANK_PORTF.txt
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
   python generate_sample_data_eiimport.py
   ```

## Data Files

### Input Files

All input files must be in Parquet format and located in `/data/input/`:

| File Name | Description | Key Columns |
|-----------|-------------|-------------|
| `reptdate.parquet` | Reporting date | REPTDATE |
| `saving.parquet` | Savings accounts | PRODUCT, OPENIND, CURBAL, INTPAYBL |
| `current.parquet` | Current accounts | PRODUCT, OPENIND, CURBAL, INTPAYBL |
| `fd.parquet` | Fixed deposits | PRODUCT, OPENIND, CURBAL, INTPAYBL |
| `iutfxYYMMDD.parquet` | Equity/derivatives | DEALTYPE, AMTPAY |
| `w1alMMWK.parquet` | INI transactions | SET_ID, AMOUNT |

**Naming Convention:**
- `YY` = Year (2-digit)
- `MM` = Month (01-12)
- `DD` = Day (01-31)
- `WK` = Week (1-4)

### Output File

| File Name | Description | Format |
|-----------|-------------|--------|
| `BANK_PORTF.txt` | Portfolio exposure report | Semicolon-delimited with ASA |

## Usage

### Step 1: Generate Sample Data (First Time Only)

```bash
python generate_sample_data_eiimport.py
```

**Expected Output:**
```
======================================================================
EIIMPORT Sample Data Generator
======================================================================
Report Date: 2024-06-25
Week: 4
======================================================================

Generating reporting date file...
✓ Generated: /data/input/reptdate.parquet

Generating saving account data...
✓ Generated: /data/input/saving.parquet

Generating current account data...
✓ Generated: /data/input/current.parquet

Generating fixed deposit data...
✓ Generated: /data/input/fd.parquet

Generating equity/derivatives data...
✓ Generated: /data/input/iutfx240625.parquet

Generating W1AL (Bai Al Inah) data...
✓ Generated: /data/input/w1al064.parquet
```

### Step 2: Generate Report

```bash
python EIIMPORT_report.py
```

**Expected Output:**
```
Report Date: 25/06/2024
Week: 4
REPTDATE (Z5): 24177

Processing Savings Accounts...
✓ Processed 500 savings accounts

Processing Current Accounts...
✓ Processed 800 current accounts

Processing Fixed Deposits (MGIA)...
✓ Processed 150 MGIA deposits

Processing Commodity Murabahah...
✓ Processed 150 Commodity Murabahah deposits

Processing Equity/Derivatives...
✓ Processed 50 equity/derivative transactions

Processing INI (Bai Al Inah)...
✓ Processed 20 INI transactions

Calculating term total...
Term Total: 123,456,789.12

Building portfolio exposure report...
✓ Generated 25 report lines

Writing report to: /data/output/BANK_PORTF.txt
✓ Report generated successfully: /data/output/BANK_PORTF.txt

======================================================================
EIIMPORT Report Generation Complete!
======================================================================

Summary:
  Savings Total:    RM 45,234,567.89
  Current Total:    RM 234,567,890.12
  Term Total:       RM 123,456,789.12
    - MGIA:         RM 56,789,012.34
    - Commodity:    RM 34,567,890.12
    - Short Term:   RM 23,456,789.01
    - INI:          RM 8,643,097.65
```

### Step 3: View Report

```bash
cat /data/output/BANK_PORTF.txt
```

## Report Specifications

### Report Categories

The report is organized into three main sections:

#### 1. SAVINGS (ID: 1.0)
- **Concept**: Wadiah only
- **Products**: 204, 207, 214, 215
- **Subcategory** (ID: 1.1): Wadiah details

#### 2. CURRENT (ID: 2.0)
- **Concepts**: Multiple (Wadiah, BBA, Murabahah, Bai Al Inah, Qard, Musyarakah)
- **Products**: 32, 33, 60-67, 70, 71, 73, 93-97, 160-169, 182-188, 440-444
- **Subcategories** (ID: 2.1): Details by concept

#### 3. TERM (ID: 3.0)
Sum of four sub-components:

##### A. MGIA (ID: 3.1, Details: 3.11)
- **Concepts**: Mudarabah, Istismar
- **Products**: 302, 396 (Mudarabah), 315, 394 (Istismar)

##### B. COMMODITY MURABAHAH (ID: 3.2, Details: 3.21)
- **Concept**: Commodity Murabahah Tawwarruq
- **Products**: 316, 393

##### C. SHORT TERM DEPOSIT (ID: 3.3, Details: 3.31)
- **Concepts**: Istismar, Mudarabah, Wadiah, Commodity Murabahah Tawarruq
- **Source**: Equity/Derivatives (EQUT file)
- **Deal Types**: BCS, BCI, BCW, BCT

##### D. INI (ID: 3.4, Details: 3.41)
- **Concept**: Bai Al Inah
- **Source**: W1AL file
- **Filter**: SET_ID = 'F142150NIDI'

## Shariah Concepts

### Islamic Banking Concepts Explained

| Concept | Arabic Term | Description | Product Type |
|---------|-------------|-------------|--------------|
| **Wadiah** | وديعة | Safekeeping/custody | Savings, Current |
| **Mudarabah** | مضاربة | Profit-sharing partnership | Fixed Deposits, Derivatives |
| **Istismar** | استثمار | Investment | Fixed Deposits, Derivatives |
| **Bai Bithaman Ajil** | بيع بثمن آجل | Deferred payment sale | Current Accounts |
| **Murabahah** | مرابحة | Cost-plus financing | Current Accounts |
| **Bai Al Inah** | بيع العينة | Sale and buyback | Current, INI |
| **Qard** | قرض | Benevolent loan | Current Accounts |
| **Musyarakah** | مشاركة | Joint venture | Current Accounts |
| **Commodity Murabahah Tawarruq** | تورق | Commodity sale for liquidity | Fixed Deposits, Derivatives |

### Product to Concept Mapping

#### Savings Products
- **204, 207, 214, 215** → Wadiah

#### Current Products
- **60-67, 93, 96-97, 160-165, 182** → Wadiah
- **32, 70, 73, 94-95, 166, 183-185, 187-188** → Bai Bithaman Ajil
- **169** → Murabahah
- **33, 71, 167-168** → Bai Al Inah
- **440-444** → Qard
- **186** → Musyarakah

#### Fixed Deposit Products
- **302, 396** → Mudarabah
- **315, 394** → Istismar
- **316, 393** → Commodity Murabahah Tawwarruq

#### Equity/Derivatives Deal Types
- **BCS** → Istismar
- **BCI** → Mudarabah
- **BCT** → Commodity Murabahah Tawarruq
- **BCW** → Wadiah

## Data Dictionary

### Common Account Fields

| Column | Type | Description | Values/Format |
|--------|------|-------------|---------------|
| ACCTNO | String | Account number | Unique identifier |
| PRODUCT | Integer | Product code | See product mappings |
| OPENIND | String | Open indicator | 'A'=Active, 'B'=Blocked, 'C'=Closed, 'P'=Purged |
| CURBAL | Float | Current balance | Decimal(16,2), >= 0 |
| INTPAYBL | Float | Interest payable | Decimal(16,2) |
| CUSTCODE | String | Customer code | Customer identifier |
| BRANCH | String | Branch code | Branch identifier |
| RACE | String | Race code | '1'-'4' |

### Equity/Derivatives Fields (EQUT)

| Column | Type | Description | Values/Format |
|--------|------|-------------|---------------|
| DEALID | String | Deal identifier | Unique |
| DEALTYPE | String | Deal type | BCS, BCI, BCT, BCW |
| AMTPAY | Float | Amount paid | Decimal(16,2) |
| DEALDATE | Date | Deal date | YYYY-MM-DD |

### W1AL (INI) Fields

| Column | Type | Description | Values/Format |
|--------|------|-------------|---------------|
| SET_ID | String | Set identifier | 'F142150NIDI' for INI |
| TRANS_ID | String | Transaction ID | Unique |
| AMOUNT | Float | Transaction amount | Decimal(16,2) |
| TRANS_DATE | Date | Transaction date | YYYY-MM-DD |

## Business Logic

### Week Determination

Based on the day of the reporting date:

```python
if 1 <= day <= 8:
    week = '1'
    # Special case: Use last day of previous month
elif 9 <= day <= 15:
    week = '2'
elif 16 <= day <= 22:
    week = '3'
else:  # 23-31
    week = '4'
```

### Account Filtering Rules

All account types must meet these criteria:
1. **OPENIND NOT IN ('B', 'C', 'P')** - Exclude blocked, closed, purged
2. **CURBAL >= 0** - Only positive or zero balances

### REPTDATE Z5 Format

The Z5 format represents date as YYDDD:
- YY: 2-digit year
- DDD: Day of year (001-366)

Example: 2024-06-25 → 24177 (177th day of 2024)

### Aggregation Hierarchy

```
Portfolio Total
├── 1. SAVINGS (Total of all Wadiah savings)
│   └── 1.1 WADIAH (Detail)
├── 2. CURRENT (Total of all current concepts)
│   ├── 2.1 WADIAH (Detail)
│   ├── 2.1 BAI BITHAMAN AJIL (Detail)
│   ├── 2.1 MURABAHAH (Detail)
│   ├── 2.1 BAI AL INAH (Detail)
│   ├── 2.1 QARD (Detail)
│   └── 2.1 MUSYARAKAH (Detail)
└── 3. TERM (Sum of MGIA + Commodity + Short Term + INI)
    ├── 3.1 A.MGIA (Total)
    │   ├── 3.11 MUDARABAH (Detail)
    │   └── 3.11 ISTISMAR (Detail)
    ├── 3.2 B.COMMODITY MURABAHAH (Total)
    │   └── 3.21 COMMODITY MURABAHAH TAWWARRUQ (Detail)
    ├── 3.3 C.SHORT TERM DEPOSIT (Total)
    │   ├── 3.31 ISTISMAR (Detail)
    │   ├── 3.31 MUDARABAH (Detail)
    │   ├── 3.31 WADIAH (Detail)
    │   └── 3.31 COMMODITY MURABAHAH TAWARRUQ (Detail)
    └── 3.4 D.INI (Total)
        └── 3.41 BAI AL INAH (Detail)
```

### Term Total Calculation

```python
high_term = total3 + total4 + total5 + total6

Where:
  total3 = MGIA total (Mudarabah + Istismar from FD)
  total4 = Commodity Murabahah total (from FD)
  total5 = Short Term Deposit total (from EQUT)
  total6 = INI total (from W1AL)
```

## Output Format

### File Structure

The output is a semicolon-delimited text file with ASA carriage control:

```
 
 TITLE: REPORTING REQUIREMENTS FOR ISLAMIC BANKING
                                                   PORTFOLIO EXPOSURE 
        (INVESTMENT,DEPOSIT,DERIVATIVES AND SUKUK HOLDING)
                                                          ACCORDING TO PRODUCT AND SHARIAH APPROVED
        AS AT :25/06/2024
 ITEM/CONCEPT
 DEPOSIT/INVESTMENT;RM(AMOUNT);
 WADIAH;45234567.89;
 1.SAVINGS;45234567.89;
 WADIAH;123456789.01;
 BAI BITHAMAN AJIL;56789012.34;
 BAI AL INAH;23456789.01;
 MURABAHAH;12345678.90;
 QARD;5678901.23;
 MUSYARAKAH;3456789.01;
 2.CURRENT;234567890.12;
 MUDARABAH;34567890.12;
 ISTISMAR;22222222.22;
 3.TERM;123456789.12;
 MUDARABAH;34567890.12;
 ISTISMAR;22222222.22;
 A.MGIA;56789012.34;
 COMMODITY MURABAHAH TAWARRUQ;34567890.12;
 B.COMMODITY MURABAHAH;34567890.12;
 ISTISMAR;5678901.23;
 MUDARABAH;8901234.56;
 WADIAH;4567890.12;
 COMMODITY MURABAHAH TAWARRUQ;4567890.10;
 C.SHORT TERM DEPOSIT;23456789.01;
 BAI AL INAH;8643097.65;
 D.INI;8643097.65;
```

### Field Descriptions

- **Line 1**: Blank line with ASA control
- **Lines 2-6**: Report header and title
- **Line 7**: Column header (ITEM/CONCEPT)
- **Line 8**: Column header (DEPOSIT/INVESTMENT;RM(AMOUNT);)
- **Lines 9+**: Data rows in format: `CONCEPT;AMOUNT;`

### ASA Carriage Control

All lines use single spacing (' ') as the carriage control character.

## Troubleshooting

### Common Issues

#### 1. File Not Found Error

**Error:**
```
FileNotFoundError: [Errno 2] No such file or directory: '/data/input/saving.parquet'
```

**Solution:**
- Ensure input directory exists: `mkdir -p /data/input`
- Run sample data generator: `python generate_sample_data_eiimport.py`
- Verify all required files are present

#### 2. Missing EQUT File

**Warning:**
```
⚠ Warning: Equity file not found: /data/input/iutfx240625.parquet
```

**Solution:**
- Check file naming matches `iutfxYYMMDD.parquet` pattern
- Verify YYDDMM values match reporting date
- File may be optional; report will use zero values

#### 3. Missing W1AL File

**Warning:**
```
⚠ Warning: W1AL file not found: /data/input/w1al064.parquet
```

**Solution:**
- Check file naming matches `w1alMMWK.parquet` pattern
- Verify MM (month) and WK (week) values
- File may be optional; report will use zero values

#### 4. Incorrect Week Calculation

**Issue:** Week number doesn't match expectation

**Solution:**
- Review week determination logic based on day of month
- Days 1-8: Week 1 (special handling for previous month)
- Days 9-15: Week 2
- Days 16-22: Week 3
- Days 23-31: Week 4

#### 5. Zero Balances in Report

**Issue:** All amounts show as 0.00

**Solution:**
- Check OPENIND filter (exclude B, C, P)
- Verify CURBAL >= 0 condition
- Ensure PRODUCT codes match expected values
- Review sample data generation for realistic balances

#### 6. Concept Mapping Errors

**Issue:** Products not categorized correctly

**Solution:**
- Verify product-to-concept mapping in code
- Check for typos in product lists
- Ensure all required products are included
- Review Shariah concept definitions

### Data Validation Checklist

Before running the report, verify:

- [ ] All required input files exist in `/data/input/`
- [ ] REPTDATE is valid date format
- [ ] PRODUCT codes are integers
- [ ] OPENIND values are single characters
- [ ] CURBAL values are numeric and >= 0
- [ ] DEALTYPE codes match expected values (BCS, BCI, BCT, BCW)
- [ ] SET_ID = 'F142150NIDI' for INI transactions
- [ ] File naming conventions are correct
- [ ] No duplicate account numbers within same file

## Technical Notes

### REPTDATE Special Handling

When reporting date falls in days 1-8, the system uses the last day of the previous month for certain calculations:

```python
if 1 <= day <= 8:
    reptdate1 = reptdate.replace(day=1) - timedelta(days=1)
```

This is reflected in the month value used for some processing.

### Interest Payable (INTPAYBL)

The field INTPAYBL is aggregated but not displayed in the final report. It's maintained for internal calculations and audit purposes.

### ID Numbering System

The ID field uses decimal notation to represent hierarchy:
- **X.0** = Major category total
- **X.Y** = Sub-category total
- **X.Y1** = Detail line

Examples:
- 1.0 = Savings total
- 1.1 = Wadiah detail under savings
- 3.0 = Term total
- 3.1 = MGIA sub-total
- 3.11 = MGIA detail lines

### Null Handling

The code uses Polars' default null handling:
- Null values in aggregations are treated as 0
- Sum operations ignore nulls
- Missing files result in zero totals with warnings

### Performance Considerations

For large datasets (>1 million accounts):

1. **Use DuckDB filtering:**
   ```python
   query = """
       SELECT * FROM read_parquet('file.parquet')
       WHERE PRODUCT IN (...)
       AND OPENIND NOT IN ('B', 'C', 'P')
   ```

2. **Index on PRODUCT:**
   - Ensures efficient filtering by product code

3. **Batch processing:**
   - Process each account type separately
   - Minimize memory usage

## Maintenance

### Adding New Products

To include new Islamic banking products:

1. Identify the Shariah concept
2. Add product code to appropriate list:
   ```python
   wadiah_products = [60, ..., NEW_PRODUCT]
   ```
3. Update documentation
4. Test with sample data

### Adding New Concepts

To add new Shariah concepts:

1. Create new concept mapping
2. Add to appropriate aggregation section
3. Update report output section
4. Add to documentation

### Modifying Report Format

To change output format:

1. Update header section in output writing
2. Adjust column positions or delimiters
3. Test with various data volumes
4. Update documentation

## Support

For issues or questions:

1. Check troubleshooting section
2. Verify sample data generation works
3. Review SAS source code for business logic
4. Check Polars/DuckDB documentation for syntax

## Version History

**Version 1.0** (2024)
- Initial conversion from SAS
- Full portfolio exposure functionality
- Support for all Shariah concepts
- Sample data generator
- Comprehensive documentation

## License

Internal use only - Public Islamic Bank Berhad

---

**Last Updated:** February 2024  
**ESMR**: 2014-691  
**Frequency**: Quarterly  
**Author:** SAS to Python Conversion Project  
**Contact:** IT Department
