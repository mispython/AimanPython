# EIIMTCOF - SAS to Python Conversion: Islamic Bank LCR Concentration of Funding Report

## Overview

This Python script converts the SAS program that generates the Liquidity Coverage Ratio (LCR) Table 4 - Concentration of Funding report for **Public Islamic Bank Berhad**. This is the Islamic banking variant of the LCR COF report.

## File Structure

- **icof_report.py**: Main Python script that replicates the SAS program logic

## Differences from Conventional Bank Version

This Islamic Bank version differs from the conventional bank version in several key aspects:

### 1. Bank Name
- **Conventional**: "PUBLIC BANK BERHAD"
- **Islamic**: "PUBLIC ISLAMIC BANK BERHAD"

### 2. Format Mappings
Different BIC codes and item classifications:

**COFF1FMT (Total Liabilities):**
- Islamic uses: 95315, 95317, 95312, 95313 (vs conventional's 95311, 95312, 95313)
- Different item codes: 1.01I-1.31II

**COFF5FMT (Wholesale Funding):**
- Islamic: Simpler structure with 5.01-5.05
- Conventional: More detailed with 5.04I-5.04VII subdivisions

**GLCOFFMT:**
- Islamic uses: F142600A (vs conventional's F142600FBI)
- Fewer GL account mappings in Islamic version

### 3. List Files
Different naming convention for customer lists:
- **Islamic**: ICOF_MNI_INTRA_GROUP, ICOF_EQU_INTRA_GROUP, etc.
- **Conventional**: COF_MNI_INTRA_GROUP, COF_EQU_INTRA_GROUP, etc.

### 4. Output Files
- **Islamic**: ICOF_DDMMYY.XLS (with ICOF_ prefix)
- **Conventional**: COF_DDMMYY.XLS (with COF_ prefix)

### 5. SFTP Script
- **Islamic**: References //SAP.PIBB.LCR.COF.TEXT
- **Conventional**: References //SAP.PBB.LCR.COF.TEXT

### 6. No VOSTRO Processing
The Islamic version does NOT include VOSTRO account processing, which is present in the conventional version.

## SAS Program Logic

The program performs the following operations:

1. **REPTDATE Processing**: Reads report date and calculates formatting parameters
2. **Format Definitions**: Defines value formats for categorizing Islamic banking liabilities
3. **Template Reading**: Reads template file with item descriptions
4. **GL Data Processing**: Reads WALK file starting from line 2 (FIRSTOBS=2)
5. **COF Data Processing**:
   - Reads CMM and EQU data
   - Creates TAG 1 (Total Liabilities)
   - Reads customer lists for intra-group and related party identification
   - Creates TAG 2 (Intra Group) and TAG 3 (Related Party)
6. **Retail/Wholesale Breakdown**: Creates COF45 for Islamic funding classification
7. **Summary Creation**: Aggregates data at item, subtotal, and total levels
8. **Report Generation**: Merges template with data and generates formatted output
9. **SFTP Script**: Creates SFTP command file for file transfer

## Format Mappings - Islamic Bank

### COFF1FMT (Total Liabilities)
```
95315 → 1.01I    (Savings Accounts - Wadiah)
95317 → 1.02I    (Savings Accounts - Mudharabah)
95312 → 1.03I    (Current Accounts)
95313 → 1.04I    (Investment Accounts)
95810 → 1.05I    (Deposits from banks)
95820 → 1.06I    (Negotiable instruments)
95830 → 1.07I    (Borrowings)
95840 → 1.08I    (Other deposits)
95850 → 1.10I    (Subordinated debt)
96317 → 1.20II   (Savings - Mudharabah - foreign currency)
96313 → 1.21II   (Investment accounts - foreign currency)
```

### COFF2FMT (Intra Group)
```
95315 → 2.01I    (Intra-group savings - Wadiah)
95317 → 2.02I    (Intra-group savings - Mudharabah)
95312 → 2.03I    (Intra-group current accounts)
95313 → 2.04I    (Intra-group investment accounts)
```

### COFF3FMT (Related Party)
```
95315 → 3.01I    (Related party savings - Wadiah)
95317 → 3.02I    (Related party savings - Mudharabah)
95312 → 3.03I    (Related party current accounts)
95313 → 3.04I    (Related party investment accounts)
```

### COFF4FMT (Retail Funding)
```
95315 → 4.01     (Retail savings - Wadiah)
95317 → 4.02     (Retail savings - Mudharabah)
95312 → 4.03     (Retail current accounts)
95313 → 4.04     (Retail investment accounts)
```

### COFF5FMT (Wholesale Funding)
```
95315 → 5.01     (Wholesale savings - Wadiah)
95317 → 5.02     (Wholesale savings - Mudharabah)
95313 → 5.03     (Wholesale investment accounts)
95810 → 5.05I    (Operational deposits)
95820 → 5.05II   (Non-operational deposits)
95830 → 5.05III  (Borrowings)
95840 → 5.05IV   (Other wholesale)
```

### GLCOFFMT (GL Accounts)
```
F143130      → 1.12I    (Structured deposits)
F144111      → 1.13I    (Repo liabilities)
F147100      → 1.13I    (Securities lending)
F249120BP    → 1.16I    (Bills payable)
F142199C     → 1.17I    (Accrued expenses)
F142199D     → 1.17I    (Accrued expenses)
F142199E     → 1.17I    (Accrued expenses)
F142510FDA   → 1.18I    (Federal deposit insurance)
F142600A     → 1.31II   (Federal funds purchased)
```

## Input Files

### Parquet Files
- **REPTDATE.parquet**: Contains REPTDATE field
- **CMM{MM}.parquet**: Islamic Money Market data
- **EQU{MM}.parquet**: Islamic Equity data
- **ICOF_MNI_INTRA_GROUP.parquet**: Intra-group customer list
- **ICOF_MNI_RELATED_PARTY.parquet**: Related party customer list
- **ICOF_EQU_INTRA_GROUP.parquet**: Equity intra-group list
- **ICOF_EQU_RELATED_PARTY.parquet**: Equity related party list

### Text Files
- **TEMPL.txt**: Template file with item descriptions
- **WALK.txt**: GL walk file (processed from line 2 onwards)

## Output Files

### ICOF_OUTPUT.txt
Pipe-delimited text file with ASA carriage control containing:
- Header: Islamic Bank name, report title, date
- Data rows: Item descriptions and amounts by maturity bucket
- Column headers inserted after section breaks

**Format**:
```
 PUBLIC ISLAMIC BANK BERHAD
 LIQUIDITY COVERAGE RATIO (LCR) TABLE 4 AS AT DD/MM/YYYY
 CONCENTRATION OF FUNDING
 
 {IDESC}|{BUC1}|{BUC2}|{BUC3}|{BUC4}|{BUC5}|{BUC6}|{BUC7}|{AMOUNT}|
```

### SFTP_SCRIPT.txt
```
put //SAP.PIBB.LCR.COF.TEXT  ICOF_DDMMYY.XLS
```

## Key Processing Rules

### 1. GL Bucket Assignment (from WALK file)
- **BUC5**: SET_ID in ('F142199C', 'F142199D')
- **BUC6**: SET_ID in ('F144111', 'F147100')
- **BUC7**: All other GL items

### 2. REM Override
For BICs '95312', '95313', '96313', '9531X', REM is set to '07' (No specific maturity)

### 3. Operational Deposit Classification
If ITEM = '5.03' and ECP = '01', then ITEM becomes '5.04' (Operational)

### 4. WALK File Processing
- Starts from line 2 (FIRSTOBS=2 in SAS)
- Column 62: Space character means negative amount

### 5. TAG Classification
- **TAG 1**: All records (Total Liabilities)
- **TAG 2**: Intra Group (based on INTRACUS, INTRAIC, INTRAEQ lists)
- **TAG 3**: Related Party (based on RELCUS, RELIC, RELEQ lists)

## Bucket Definitions (BUC1-BUC7)

Same as conventional bank version:
- **BUC1**: up to 1 week
- **BUC2**: > 1 week - 1 month
- **BUC3**: > 1 - 3 months
- **BUC4**: > 3 - 6 months
- **BUC5**: > 6 months - 1 year
- **BUC6**: > 1 year
- **BUC7**: No specific maturity

## Dependencies

```
polars>=0.20.0
```

## Installation

```bash
pip install polars
```

## Configuration

Edit the path configuration at the top of `icof_report.py`:

```python
INPUT_DIR = Path("/path/to/input")
OUTPUT_DIR = Path("/path/to/output")
LIST_DIR = Path("/path/to/list")
```

## Usage

```bash
python icof_report.py
```

## Processing Steps

1. Read REPTDATE and calculate parameters (REPTMON, FILDT, RDATE)
2. Read template file with item descriptions
3. Read GL data from WALK file (starting line 2)
4. Read CMM and EQU data for current month
5. Read Islamic customer classification lists (ICOF_* files)
6. Create TAG 1 (Total Liabilities)
7. Create TAG 2 and 3 (Intra Group and Related Party)
8. Apply Islamic format mappings (COFF1-5, GLCOFFMT)
9. Allocate amounts to maturity buckets
10. Create retail/wholesale breakdown (COF45)
11. Combine all data sources
12. Create multi-level summaries
13. Generate report with ASA carriage control
14. Generate SFTP script

## Islamic Banking Considerations

The format mappings reflect Islamic banking principles:
- **Wadiah**: Safekeeping deposits (95315)
- **Mudharabah**: Profit-sharing investment accounts (95317, 96317)
- **Investment Accounts**: General Islamic investment products (95313, 96313)

## Comparison Table: Islamic vs Conventional

| Feature | Islamic Bank | Conventional Bank |
|---------|-------------|-------------------|
| Bank Name | PUBLIC ISLAMIC BANK BERHAD | PUBLIC BANK BERHAD |
| List Files Prefix | ICOF_ | COF_ |
| Output File Prefix | ICOF_ | COF_ |
| BIC Codes | 95315, 95317 (Islamic products) | 95311 (conventional) |
| VOSTRO Processing | No | Yes |
| GL Format (Federal Funds) | F142600A | F142600FBI |
| SFTP Dataset | SAP.PIBB | SAP.PBB |
| Operational Item | 5.04 | 5.03 |

## Validation Points

To verify correct conversion:

1. Verify Islamic product codes (95315, 95317) are correctly mapped
2. Check that VOSTRO processing is NOT included
3. Confirm list files use ICOF_ prefix
4. Validate output file name has ICOF_ prefix
5. Verify bank name is "PUBLIC ISLAMIC BANK BERHAD"
6. Ensure operational deposit logic: 5.03 + ECP='01' → 5.04
7. Check WALK file processing starts from line 2
8. Verify SFTP script references SAP.PIBB (not SAP.PBB)

## Notes

- This version is specifically for Islamic banking operations
- No VOSTRO account processing (unlike conventional version)
- Different BIC codes reflect Islamic banking products
- WALK file processing starts from line 2 (FIRSTOBS=2)
- ASA carriage control character is space (single spacing)
- Delimiter is chr(0x05) (ASCII 5)
- Amounts formatted with comma separators and 2 decimal places
