# Table 1 - Statement of Financial Position - SAS to Python Conversion

## Overview

This Python script converts the SAS program that generates **Table 1 - Statement of Financial Position** report for CNY (Chinese Yuan) transactions, showing resident and non-resident breakdowns.

## Requirements

```bash
pip install duckdb polars pyarrow --break-system-packages
```

## Input Files

All input files should be in Parquet format in the `input/` directory:

### 1. reptdate.parquet
- **Columns**: `REPTDATE` (date)
- **Purpose**: Report date for macro variable generation

### 2. btrad{MMWKYY}.parquet
- **Naming**: `btrad` + 2-digit month + 1-digit week + 2-digit year
- **Example**: `btrad0212024.parquet` (February, Week 1, 2024)
- **Columns**:
  - `LIABCODE` (string): Liability code
  - `FORCURR` (string): Foreign currency code
  - `CUSTCD` (integer): Customer code
  - `TRANXMT` (float): Transaction amount
  - `INTAMT` (float): Interest amount
- **Filter**: Only CNY currency with specific liability codes

### 3. fcy{MMWKYY}.parquet
- **Naming**: `fcy` + 2-digit month + 1-digit week + 2-digit year
- **Example**: `fcy0212024.parquet`
- **Columns**:
  - `PRODUCT` (integer): Product code
  - `CURCODE` (string): Currency code
  - `CUSTCD` (integer): Customer code
  - `FORBAL` (float): Foreign balance
- **Filter**: Only CNY currency

## Output File

### table1_financial_position.txt
- **Type**: Text report with ASA carriage control
- **Width**: 220 characters
- **Content**: Statement of Financial Position with multiple sections

## Report Structure

The report displays data in the following format:

```
TABLE 1 - STATEMENT OF FINANCIAL POSITION

|                              |                     | RESIDENT                                                     | NON RESIDENT                  |
|                              |                     |------------------------------------------------------------ |-------------------------------|
|                              |                     |          |          |   NON-BANK ENTITY              | TOTAL(F)  | FOREIGN   | FOREIGN   |
|                              |                     |          |          |                                | =(G)+(H)  | BANKING   | NON-BANK  |
|                              | TOTAL=TOTAL(A)      | TOTAL(A) | BANKING  |                                |           | INSTITUTION(G) | ENTITY (H)  |
| ITEM                         | + TOTAL(F)          | = (B) + (C) | INSTITUTION(B) |                                |           |           |           |
|                              |                     |          |          | TOTAL(C)  | CORPORATE(D) | RETAIL(E)  |           |           |           |
|                              |                     |          |          | = (D) + (E) |            |            |           |           |           |
|------------------------------|---------------------|----------|----------|-----------|------------|------------|-----------|-----------|-----------|
| TOTAL ASSETS                 |                     |          |          |           |            |            |           |           |           |
|                              |                     |          |          |           |            |            |           |           |           |
| - LOANS/FINANCING AND        |                     |          |          |           |            |            |           |           |           |
|   RECEIVABLES                |                     |          |          |           |            |            |           |           |           |
|   (NET OF PROVISION)         |        XXX,XXX.XX   | XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX|
|------------------------------|---------------------|----------|----------|-----------|------------|------------|-----------|-----------|-----------|
| DEPOSITS ACCEPTED            |                     |          |          |           |            |            |           |           |           |
|                              |                     |          |          |           |            |            |           |           |           |
|    - FX FIXED DEPOSITS       |        XXX,XXX.XX   | XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX|
|    - FX DEMAND DEPOSITS      |        XXX,XXX.XX   | XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX|
|------------------------------|---------------------|----------|----------|-----------|------------|------------|-----------|-----------|-----------|
| TOTAL                        |        XXX,XXX.XX   | XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX| XXX,XXX.XX|
|------------------------------|---------------------|----------|----------|-----------|------------|------------|-----------|-----------|-----------|
```

## Data Classification

### Customer Code Groupings

#### 1. Banking Institutions (Resident)
- **Codes**: 1, 2, 3, 12, 7
- **Column**: BNKINSTI (B)

#### 2. Corporate (Resident)
- **Codes**: 20, 17, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 45, 4, 5, 6, 60, 61, 62, 63, 64, 41, 42, 43, 44, 46, 47, 48, 49, 51, 52, 53, 54, 57, 59, 75, 70, 71, 72, 73, 74, 66, 67, 68, 69
- **Column**: CORPORATE (D)

#### 3. Retail (Resident)
- **Codes**: 77, 78, 79
- **Column**: RETAIL (E)

#### 4. Foreign Banking Institutions (Non-Resident)
- **Codes**: 81, 82, 83, 84
- **Column**: FORBNKINSTI (G)

#### 5. Foreign Non-Bank Entity (Non-Resident)
- **Codes**: 85, 86, 87, 88, 89, 90, 91, 92, 95, 96, 98, 99
- **Column**: FORNONBNK (H)

### Category Definitions

#### BTAC / FCYBT - Bills and Trade (CNY)
- **Source**: BTRAD file
- **Filter**: LIABCODE in ('FFS', 'FFU', 'FCS', 'FCU', 'FFL', 'FTL', 'FTI') AND FORCURR = 'CNY'
- **Amount**: TRXAMT = TRANXMT + INTAMT
- **Display**: "TOTAL ASSETS - LOANS/FINANCING AND RECEIVABLES (NET OF PROVISION)"

#### DPAC / FCYFD - Deposits (CNY Fixed)
- **Source**: FCY file
- **Filter**: PRODUCT in (360, 350, 351, 352, 353, 354, 355, 356, 357, 358) AND CURCODE = 'CNY'
- **Amount**: FORBAL
- **Display**: "DEPOSITS ACCEPTED - FX FIXED DEPOSITS"

#### DPAC / FCYDD - Deposits (CNY Demand)
- **Source**: FCY file
- **Filter**: PRODUCT not in FD list AND CURCODE = 'CNY'
- **Amount**: FORBAL
- **Display**: "DEPOSITS ACCEPTED - FX DEMAND DEPOSITS"

## Calculated Fields

### Intermediate Calculations
```python
CORPRETAIL = CORPORATE + RETAIL
TOTAL_A = BNKINSTI + CORPRETAIL  # Resident total
TOTAL_F = FORBNKINSTI + FORNONBNK  # Non-resident total
TOTAL = TOTAL_A + TOTAL_F  # Grand total
```

### Column Relationships
- **TOTAL(C)** = CORPORATE (D) + RETAIL (E)
- **TOTAL(A)** = BANKING INSTITUTION (B) + TOTAL(C)
- **TOTAL(F)** = FOREIGN BANKING INSTITUTION (G) + FOREIGN NON-BANK ENTITY (H)
- **TOTAL** = TOTAL(A) + TOTAL(F)

## Processing Flow

```
1. Read REPTDATE
   ↓
2. Calculate macro variables (week, month, year)
   ↓
3. Read BTRAD file
   ↓
4. Filter: LIABCODE in specific codes AND FORCURR = 'CNY'
   ↓
5. Calculate TRXAMT = TRANXMT + INTAMT
   ↓
6. Classify by customer code → BTAC/FCYBT
   ↓
7. Read FCY file
   ↓
8. Filter: CURCODE = 'CNY'
   ↓
9. Classify by product:
   - FD products (360, 350-358) → DPAC/FCYFD
   - Others → DPAC/FCYDD
   ↓
10. Classify by customer code
    ↓
11. Combine BTRAD and FCY
    ↓
12. Calculate totals:
    - CORPRETAIL = CORPORATE + RETAIL
    - TOTAL_A = BNKINSTI + CORPRETAIL
    - TOTAL_F = FORBNKINSTI + FORNONBNK
    - TOTAL = TOTAL_A + TOTAL_F
    ↓
13. Summarize by CATEGORY and GROUP
    ↓
14. Calculate grand total
    ↓
15. Merge with category master (ensures all categories present)
    ↓
16. Sort by category order (NO field)
    ↓
17. Generate formatted report
```

## Report Sections

### 1. Header Section
- Title: "TABLE 1 - STATEMENT OF FINANCIAL POSITION"
- Column headers with hierarchical structure
- Separators and alignment

### 2. Data Section - Total Assets
- **Category**: BTAC
- **Group**: FCYBT
- **Display**: Loans/Financing and Receivables (Net of Provision)
- **Source**: Bills and Trade transactions in CNY

### 3. Data Section - Deposits Accepted
- **FX Fixed Deposits**
  - Category: DPAC
  - Group: FCYFD
  - Source: Fixed deposit products in CNY

- **FX Demand Deposits**
  - Category: DPAC
  - Group: FCYDD
  - Source: Non-FD products in CNY

### 4. Total Section
- **Category**: TOTAL
- **Group**: TOTAL
- **Display**: Sum of all categories

## Output Format

### Column Widths
- Item description: 29 characters
- Each amount column: 20 characters (right-aligned, comma-formatted, 2 decimals)
- Total width: 220 characters

### Number Formatting
- Format: `XXX,XXX.XX`
- Alignment: Right-aligned within 20-character field
- Null values: Displayed as 0.00

### ASA Carriage Control
- All lines start with space (' ') for single spacing
- No page breaks in this report

## Usage

```bash
python table1_report.py
```

### Directory Structure

```
.
├── input/
│   ├── reptdate.parquet
│   ├── btrad0212024.parquet
│   └── fcy0212024.parquet
├── output/
│   └── table1_financial_position.txt (generated)
└── table1_report.py
```

## Configuration

Edit these variables in the script if needed:

```python
# Paths
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")

# Report settings
PAGE_LENGTH = 60
REPORT_WIDTH = 220

# Customer code classifications
BNKINSTI_CODES = [1, 2, 3, 12, 7]
CORPORATE_CODES = [20, 17, 30, ...]
RETAIL_CODES = [77, 78, 79]
FORBNKINSTI_CODES = [81, 82, 83, 84]
FORNONBNK_CODES = [85, 86, 87, 88, 89, 90, 91, 92, 95, 96, 98, 99]

# Product classifications
FD_PRODUCTS = [360, 350, 351, 352, 353, 354, 355, 356, 357, 358]
```

## Key Features

1. **DuckDB Integration**: Efficient Parquet file processing with SQL filtering
2. **Customer Classification**: Automatic categorization based on customer codes
3. **Product Classification**: Separates fixed deposits from demand deposits
4. **Multi-source Aggregation**: Combines BTRAD and FCY data
5. **Hierarchical Totals**: Calculates subtotals and grand totals
6. **Formatted Output**: Professional table layout with proper alignment
7. **Missing Data Handling**: All categories present even if no data

## Differences from SAS

1. **File Format**: Uses Parquet instead of SAS datasets
2. **Explicit Classification**: Customer codes defined in constants
3. **Simplified Logic**: Direct filtering instead of complex DATA steps
4. **No LIBNAME**: Dynamic year in LIBNAME simulated with file paths
5. **Report Generation**: Custom formatting function instead of DATA _NULL_ with FILE

## Notes

### Currency Filter
The report only includes CNY (Chinese Yuan) transactions. To process other currencies, modify the filters in `process_btrad_data()` and `process_fcy_data()`.

### Liability Codes (BTRAD)
Valid liability codes for BTRAD:
- FFS, FFU, FCS, FCU, FFL, FTL, FTI

### Product Codes (FCY)
Fixed Deposit products:
- 360, 350, 351, 352, 353, 354, 355, 356, 357, 358

All other products in CNY are classified as demand deposits.

### NULL Handling
All NULL values in numeric columns are replaced with 0.00 to ensure proper totals and display.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| FileNotFoundError | Verify input files exist with correct naming |
| Empty report | Check BTRAD and FCY files contain CNY records |
| Incorrect totals | Verify customer code classifications |
| Format issues | Check column widths match specifications |

## Verification

To verify the conversion:
1. Compare column totals between SAS and Python output
2. Verify customer classification for each category
3. Check product classification (FD vs demand)
4. Validate grand total calculations
5. Ensure all categories present even if zero

## Example Output

```
TABLE 1 - STATEMENT OF FINANCIAL POSITION

... (formatted table with actual data)

BTAC     / FCYBT   :        1,234,567.89
DPAC     / FCYFD   :          987,654.32
DPAC     / FCYDD   :          456,789.01
TOTAL    / TOTAL   :        2,679,011.22
```
