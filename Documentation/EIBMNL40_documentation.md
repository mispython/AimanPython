# EIBMNL40 - BNM Loan Maturity Profile Processing - Python Conversion

## Overview
This Python script converts a SAS program that processes loan data for Bank Negara Malaysia (BNM) reporting. The program calculates maturity profiles for different types of loans and generates summary reports by BNM code.

## Key Features
- Processes loan data to calculate remaining months to maturity
- Handles different loan types: Fixed Loans (FL), Housing Loans (HL), Revolving Credits (RC), and Overdrafts (OD)
- Separates gross loans and non-performing loans
- Groups loans by maturity buckets (up to 1 week, 1 week to 1 month, etc.)
- Processes both RM (Malaysian Ringgit) and FCY (Foreign Currency) products

## Requirements
```bash
pip install duckdb polars pyarrow
```

## Input Files
The script expects the following parquet files:

1. **`./input/bnm/reptdate.parquet`**
   - Contains: `reptdate` (reporting date)

2. **`./input/bnm1/loan{YYYYMM}{W}.parquet`**
   - Contains loan data with columns:
     - `paidind`: Paid indicator
     - `eir_adj`: EIR adjustment amount
     - `prodcd`: Product code
     - `product`: Product number
     - `custcd`: Customer code
     - `acctype`: Account type ('LN' for loan, 'OD' for overdraft)
     - `balance`: Outstanding balance
     - `payamt`: Payment amount
     - `bldate`: Billing date
     - `issdte`: Issue date
     - `exprdate`: Expiry/maturity date
     - `payfreq`: Payment frequency code
     - `loanstat`: Loan status
     - `days`: Days past due (calculated)

## Output Files
- **`./output/reptdate.parquet`**: Reporting date information
- **`./output/note.parquet`**: Summarized BNM codes with amounts

## BNMCODE Structure
The BNMCODE is a 15-character code structured as follows:
- **Positions 1-2**: Report type
  - `93`: Non-performing loans (RM)
  - `94`: Non-performing loans (FCY)
  - `95`: Gross loans outstanding (RM)
  - `96`: Gross loans outstanding (FCY)
- **Positions 3-5**: Loan item type
  - `211`: Fixed/Housing loans (non-staff)
  - `212`: Revolving credits (non-staff)
  - `213`: Overdrafts (non-staff)
  - `214`: Housing loans (staff)
  - `219`: Other loans
- **Positions 6-7**: Customer type
  - `08`: Staff
  - `09`: Non-staff
- **Positions 8-9**: Maturity bucket
  - `01`: Up to 1 week
  - `02`: >1 week to 1 month
  - `03`: >1 month to 3 months
  - `04`: >3 months to 6 months
  - `05`: >6 months to 1 year
  - `06`: >1 year
- **Positions 10-15**: `0000Y` (fixed suffix)

## Processing Logic

### 1. Reporting Date Extraction
- Reads the reporting date from `reptdate.parquet`
- Calculates the week number based on the day of the month:
  - Day 8 → Week 1
  - Day 15 → Week 2
  - Day 22 → Week 3
  - Other → Week 4

### 2. Loan Processing
For each loan:
- **Overdrafts (OD)**: Assigned to shortest maturity bucket (0.1 months)
- **Term Loans (LN)**: 
  - Calculate payment schedule based on payment frequency
  - For each scheduled payment, calculate remaining months
  - Classify into maturity buckets
  - Separate performing vs non-performing loans (>89 days past due)

### 3. Payment Frequency Mapping
- `1`: Monthly (1 month)
- `2`: Quarterly (3 months)
- `3`: Semi-annual (6 months)
- `4`: Annual (12 months)
- `6`: Bi-weekly (14 days)
- `5`, `9`, ` `: Bullet payment at maturity

### 4. Remaining Months Calculation
```
REMMTH = (MATYR - REPTYR) * 12 + (MATMTH - REPTMTH) + (MATDAY - REPTDAY) / DAYS_IN_MONTH
```

### 5. Non-Performing Loan Criteria
Loans are classified as non-performing if:
- Days past due > 89, OR
- Loan status ≠ 1

### 6. Revolving Credit Special Processing
- RC corporate loans (BNMCODE starting with '95212') generate additional NPL entries ('93212...')
- Original '93212' entries are removed and replaced

### 7. Default Records
Creates zero-value records for all possible BNMCODE combinations to ensure complete reporting

### 8. Final Summarization
Groups by BNMCODE and sums amounts

## Key Differences from SAS

1. **Date Handling**: Python uses `datetime` objects instead of SAS date values
2. **Array Indexing**: Python uses 0-based indexing vs SAS 1-based
3. **Null Handling**: Explicit `None` checks in Python vs SAS `.` notation
4. **Data Processing**: Uses Polars DataFrames for efficient columnar operations
5. **Formatting**: Custom Python functions replace SAS PROCs and formats

## Assumptions

1. **Format Files**: The script includes assumed mappings for:
   - `LIQPFMT`: Product to loan type mapping
   - `REMFMT`: Remaining months to bucket code
   
   These should be verified against actual format definitions in:
   - `PBBLNFMT`
   - `PBBELF`
   - `PBBDPFMT`

2. **FCY Products**: List of foreign currency products is hardcoded
   
3. **Product Codes**: Product code mappings may need adjustment based on actual bank product catalog

## Performance Notes

- Uses DuckDB for efficient parquet file reading
- Polars provides fast columnar operations
- Iterates row-by-row for complex loan payment calculations (could be optimized with vectorization for very large datasets)

## Usage

```bash
python bnm_note_conversion.py
```

Ensure input directory structure:
```
./input/
  bnm/
    reptdate.parquet
  bnm1/
    loan{YYYYMM}{W}.parquet
```

Output will be created in:
```
./output/
  reptdate.parquet
  note.parquet
```

## Validation

To validate the conversion:
1. Compare record counts between SAS and Python outputs
2. Verify BNMCODE distribution matches
3. Check sum totals by report type (93, 94, 95, 96)
4. Validate sample calculations manually
5. Cross-check maturity bucket classifications

## Troubleshooting

**Issue**: Missing format definitions
- **Solution**: Update `liqpfmt_format()` function with actual product mappings

**Issue**: Date calculation errors
- **Solution**: Verify leap year handling and month-end adjustments

**Issue**: Memory issues with large datasets
- **Solution**: Process in chunks or use DuckDB for aggregations

## Future Enhancements

1. Vectorize payment schedule calculations for better performance
2. Add data validation and error checking
3. Create detailed audit trail
4. Add logging for troubleshooting
5. Implement parallel processing for multiple reporting periods
6. Add command-line arguments for configurable paths

## Contact
For questions about the conversion or discrepancies, review the original SAS code and consult the format definition files.
