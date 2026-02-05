# BNM Report Generation - Python Conversion

## Overview
This Python script converts a SAS program that generates formatted CSV reports for Bank Negara Malaysia (BNM) submission. It takes the FINALSUM dataset and creates four behavioral maturity profile reports:

1. **RM Part 1**: Ringgit Non-Performing Loans (NPL)
2. **RM Part 2**: Ringgit Gross Loans
3. **FX Part 1**: Foreign Currency Non-Performing Loans
4. **FX Part 2**: Foreign Currency Gross Loans

## Requirements
```bash
pip install duckdb polars pyarrow
```

## Input Files

### **`./input/store/finalsum.parquet`**
- Output from bnm_glset_consolidation.py
- Contains: `bnmcode`, `amount`
- This is the consolidated BNM dataset

## Output Files

### CSV Reports (in `./output/`)
1. **`report_rm_part1.csv`** - RM NPL positions
2. **`report_rm_part2.csv`** - RM Gross positions
3. **`report_fx_part1.csv`** - FCY NPL positions
4. **`report_fx_part2.csv`** - FCY Gross positions

### Intermediate Parquet Files (in `./input/store/`)
1. **`notermp1.parquet`** - RM Part 1 data
2. **`notermp2.parquet`** - RM Part 2 data
3. **`notefxp1.parquet`** - FX Part 1 data
4. **`notefxp2.parquet`** - FX Part 2 data

## Report Structure

### CSV Format
Each report is a semicolon-delimited CSV file with the following structure:

```
[Blank line]
BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - [TITLE]
[Blank line]
INFLOW(ASSETS)
ON BALANCE SHEET
;;;;;UP TO 1 WK;>1 WK - 1 MTH;>1 MTH - 3 MTHS;>3 MTHS - 6 MTHS;>6 MTHS - 1 YR;>1 YEAR;TOTAL;
[Data rows]
```

### Columns
| Column | Description | Example |
|--------|-------------|---------|
| ITEM | Item code | A1.01, B1.05 |
| ITEM2 | Category | LOAN : |
| ITEM3 | Customer type | INDIVIDUALS, NON-INDIVIDUALS |
| ITEM4 | Product description | - FIXED TERM LOANS |
| WEEK | Up to 1 week | 1234.0 |
| MONTH | >1 week to 1 month | 5678.0 |
| QTR | >1 month to 3 months | 9012.0 |
| HALFYR | >3 months to 6 months | 3456.0 |
| YEAR | >6 months to 1 year | 7890.0 |
| LAST | >1 year | 12345.0 |
| TOTAL | Sum of all buckets | 30615.0 |

## Product Mapping

### RM (Ringgit Malaysia) Products

#### Part 1 (NPL - Code 93) & Part 2 (Gross - Code 95)

| PROD Code | Item | Description | Customer Type |
|-----------|------|-------------|---------------|
| 9X21109 | A1.01 | FIXED TERM LOANS | Non-Individual |
| 9X21209 | A1.02 | REVOLVING LOANS | Non-Individual |
| 9X21309 | A1.03 | OVERDRAFTS | Non-Individual |
| 9X21909 | A1.04 | OTHERS | Non-Individual |
| 9X21408 | A1.05 | HOUSING LOANS | Individual |
| 9X21508 | A1.06 | CREDIT CARDS | Individual |
| 9X21308 | A1.07 | OVERDRAFTS | Individual |
| 9X21908 | A1.08 | OTHERS | Individual |
| 9X22100 | A1.09 | CASH HOLDINGS | Miscellaneous |
| 9X22200 | A1.10 | SRR | Miscellaneous |
| 9X22900 | A1.11 | OTHER ASSETS | Miscellaneous |

*X = 3 for NPL (Part 1), 5 for Gross (Part 2)*

### FCY (Foreign Currency) Products

#### Part 1 (NPL - Code 94) & Part 2 (Gross - Code 96)

| PROD Code | Item | Description | Customer Type |
|-----------|------|-------------|---------------|
| 9X21109 | B1.01 | FIXED TERM LOANS | Non-Individual |
| 9X21209 | B1.02 | REVOLVING LOANS | Non-Individual |
| 9X21309 | B1.03 | OVERDRAFTS | Non-Individual |
| 9X21909 | B1.04 | OTHERS | Non-Individual |
| 9X21408 | B1.05 | HOUSING LOANS | Individual |
| 9X21508 | B1.06 | CREDIT CARDS | Individual |
| 9X21308 | B1.07 | OVERDRAFTS | Individual |
| 9X21908 | B1.08 | OTHERS | Individual |
| 9X22100 | B1.09 | CASH HOLDINGS | Miscellaneous |
| 9X22200 | B1.10 | SRR | Miscellaneous |
| 9X22900 | B1.11 | OTHER ASSETS | Miscellaneous |

*X = 4 for NPL (Part 1), 6 for Gross (Part 2)*

## Processing Logic

### 1. Data Loading
- Read FINALSUM.parquet
- Extract PROD (first 7 characters of BNMCODE)
- Extract INDNON (positions 6-7 of BNMCODE)

### 2. Amount Rounding
- Divide amounts by 1000 (convert to thousands)
- Round to nearest whole number
- Example: 1,234,567.89 → 1235.0

### 3. Data Transformation
- Transpose from long to wide format
- Convert maturity bucket codes to column names:
  - Bucket 01 → WEEK
  - Bucket 02 → MONTH
  - Bucket 03 → QTR
  - Bucket 04 → HALFYR
  - Bucket 05 → YEAR
  - Bucket 06 → LAST

### 4. Metadata Addition
- **ITEM**: Item code (A1.01-A1.11 for RM, B1.01-B1.11 for FCY)
- **ITEM2**: Always "LOAN :"
- **ITEM3**: "INDIVIDUALS" (code 08) or "NON-INDIVIDUALS" (code 09)
- **ITEM4**: Product description
- **BALANCE**: Sum of all maturity buckets

### 5. Filtering and Sorting
- Filter by report type (93, 94, 95, 96)
- Remove records without ITEM code
- Sort by INDNON descending (Non-individuals first)

### 6. Report Generation
- Write CSV with semicolon delimiter
- Include header section
- Format data rows

## Example Output

### Sample RM Part 1 Report (report_rm_part1.csv)
```
 
BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - RINGGIT PART 1-RM
 
INFLOW(ASSETS)
ON BALANCE SHEET
;    ;    ;    ;    ;UP TO 1 WK ;>1 WK - 1 MTH;>1 MTH - 3 MTHS;>3 MTHS - 6 MTHS;>6 MTHS - 1 YR;>1 YEAR;TOTAL;
A1.01;LOAN :;NON-INDIVIDUALS;- FIXED TERM LOANS  ;0.0;0.0;500.0;1000.0;2000.0;15000.0;18500.0;
A1.02;LOAN :;NON-INDIVIDUALS;- REVOLVING LOANS  ;0.0;0.0;100.0;200.0;300.0;5000.0;5600.0;
A1.03;LOAN :;NON-INDIVIDUALS;- OVERDRAFTS;50.0;100.0;150.0;200.0;250.0;1000.0;1750.0;
...
A1.05;LOAN :;INDIVIDUALS    ;- HOUSING LOANS;0.0;50.0;100.0;200.0;500.0;8000.0;8850.0;
...
```

## Report Differentiation

### Part 1 vs Part 2
- **Part 1 (93/94)**: Non-Performing Loans only
- **Part 2 (95/96)**: Gross Loans Outstanding

### RM vs FCY
- **RM (A-series)**: Malaysian Ringgit denominated
- **FCY (B-series)**: Foreign Currency denominated

## Execution Order

This is the **final program** in the BNM reporting suite:

```bash
# 1. Process loans
python bnm_note_conversion.py

# 2. Calculate maturity profiles
python bnm_calc_conversion.py

# 3. Consolidate all data
python bnm_glset_consolidation.py

# 4. Generate reports (THIS PROGRAM)
python bnm_report_generation.py
```

## Validation Checklist

- [ ] All product codes mapped correctly
- [ ] Amounts rounded to thousands
- [ ] Transposition correct (buckets → columns)
- [ ] BALANCE equals sum of buckets
- [ ] Reports sorted correctly (Non-individuals first)
- [ ] CSV format matches specification
- [ ] All four reports generated
- [ ] Header sections present
- [ ] Semicolon delimiters correct

## Key Differences from SAS

1. **PROC TRANSPOSE**: Replaced with Polars pivot operation
2. **DATA _NULL_ with FILE**: Replaced with Python file writing
3. **PUT statements**: Replaced with formatted string writing
4. **Multiple IF statements**: Replaced with dictionary mapping
5. **SUBSTR function**: Replaced with Polars str.slice

## CSV vs Original SAS Output

The SAS program uses `FILE NLF;` which typically creates a flat file. Our Python version creates CSV files with:
- Semicolon (`;`) delimiters
- Same column structure
- Same header format
- Identical data content

To convert to other formats if needed:
```python
# Read CSV
import polars as pl
df = pl.read_csv('report_rm_part1.csv', separator=';', skip_rows=6)

# Save as Excel
df.write_excel('report_rm_part1.xlsx')

# Save as fixed-width
# (custom formatting required)
```

## Performance Notes

- Efficient pivot operation using Polars
- Dictionary-based mapping (O(1) lookup)
- Minimal sorting (only where required)
- Lazy evaluation where possible

## Customization

### Adding New Product Categories

1. Add to `PRODUCT_MAPPING` dictionary:
```python
('9321XXX', '9521XXX'): {
    'desc': 'NEWPROD',
    'item': 'A1.12',
    'item4': '- NEW PRODUCT'
}
```

2. Program will automatically include in reports

### Changing Report Format

Modify `generate_report()` function:
- Change delimiter (currently `;`)
- Adjust header text
- Modify column formatting
- Add/remove sections

### Changing Amount Rounding

Current: Divide by 1000, round to whole number

To change to millions:
```python
(pl.col('amount') / 1000000).round(2).alias('amount')
```

## Troubleshooting

### Issue: Missing products in report
**Solution**: Check if PROD code exists in PRODUCT_MAPPING

### Issue: Amounts don't sum correctly
**Solution**: Verify all bucket columns are included in BALANCE calculation

### Issue: Wrong customer type sorting
**Solution**: Check INDNON extraction (should be positions 6-7, not 5-6)

### Issue: CSV format incorrect
**Solution**: Verify semicolon delimiters, check for missing headers

## Report Usage

These reports are typically:
1. Submitted to Bank Negara Malaysia
2. Used for regulatory compliance
3. Archived for audit purposes
4. Analyzed for liquidity management

The behavioral maturity profile helps regulators understand:
- Liquidity risk exposure
- Maturity mismatch
- Funding stability
- Asset-liability management

## Future Enhancements

1. Add Excel formatting (colors, borders, fonts)
2. Create summary/cover page
3. Add charts and visualizations
4. Implement report comparison (current vs previous period)
5. Add data quality checks
6. Create PDF version
7. Add email distribution capability

---

This is the final step in the BNM reporting process. The output CSV files are ready for submission or further processing.
