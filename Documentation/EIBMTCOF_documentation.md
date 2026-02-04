# eibmtcof - SAS to Python Conversion: LCR Concentration of Funding Report

## Overview

This Python script converts the SAS program that generates the Liquidity Coverage Ratio (LCR) Table 4 - Concentration of Funding report for Public Bank Berhad. The output is a text file with ASA carriage control characters and pipe-delimited fields.

## File Structure

- **lcr_cof_report.py**: Main Python script that replicates the SAS program logic

## SAS Program Logic

The original SAS program performs the following operations:

1. **REPTDATE Processing**: Reads report date and calculates formatting parameters
2. **Format Definitions**: Defines value formats for categorizing liabilities (COFF1-5, GLCOFFMT)
3. **Template Reading**: Reads template file with item descriptions
4. **GL Data Processing**: Reads WALK file and applies GL format mappings
5. **COF Data Processing**: 
   - Reads CMM and EQU data
   - Creates TAG 1 (Total Liabilities)
   - Reads customer lists for intra-group and related party identification
   - Creates TAG 2 (Intra Group) and TAG 3 (Related Party)
6. **Retail/Wholesale Breakdown**: Creates COF45 for funding classification
7. **VOSTRO Processing**: Processes VOSTRO accounts for intra-group identification
8. **Summary Creation**: Aggregates data at item, subtotal, and total levels
9. **Report Generation**: Merges template with data and generates formatted output
10. **SFTP Script**: Creates SFTP command file for file transfer

## Key Conversion Details

### Format Mappings

The program uses five main format mappings:

- **COFF1FMT**: Total Liabilities categorization (sections 1.01-1.30)
- **COFF2FMT**: Intra Group categorization (sections 2.01-2.22)
- **COFF3FMT**: Related Party categorization (sections 3.01-3.15)
- **COFF4FMT**: Retail Funding categorization (sections 4.01-4.03)
- **COFF5FMT**: Wholesale Funding categorization (sections 5.01-5.05)
- **GLCOFFMT**: General Ledger account mappings

### Data Flow

```
CMM + EQU Data → COF (TAG=1: Total Liabilities)
                    ↓
          Customer Lists → COF23 (TAG=2: Intra Group, TAG=3: Related Party)
                    ↓
               COF123 (Apply formats, allocate to buckets)
                    ↓
                COF45 (Retail/Wholesale breakdown)
                    ↓
    VOSTRO + GL Data → COF Combined
                    ↓
            Summaries (Item, Subtotal, Total)
                    ↓
      Template Merge → Final Report
```

### Bucket Allocation (BUC1-BUC7)

Amounts are allocated to buckets based on the REM field (maturity buckets):

- **BUC1**: REM = '01' (up to 1 week)
- **BUC2**: REM = '02' (> 1 week - 1 month)
- **BUC3**: REM = '03' (> 1 - 3 months)
- **BUC4**: REM = '04' (> 3 - 6 months)
- **BUC5**: REM = '05' (> 6 months - 1 year)
- **BUC6**: REM = '06' (> 1 year)
- **BUC7**: REM = '07' (No specific maturity)

### TAG Classification

- **TAG = 1**: Total Liabilities (all records)
- **TAG = 2**: Intra Group (based on customer/IC lists)
- **TAG = 3**: Related Party (based on customer/IC lists)

### Special Processing Rules

1. **REM Override**: For BICs '95312', '95313', '96313', '9531X', REM is set to '07'
2. **Operational Deposit**: Item '5.02' with ECP='01' becomes '5.03'
3. **GL Bucket Assignment**:
   - BUC5: SET_ID in ('F142199C', 'F142199D')
   - BUC6: SET_ID in ('F144111RM', 'F141301', 'F147100', 'F144140CAGA', 'F247610')
   - BUC7: All other GL items

### Customer List Processing

The program reads several customer lists to identify special categories:

1. **COF_MNI_INTRA_GROUP**: 
   - BUSSREG → INTRAIC (business registration numbers)
   - CUSTNO → INTRACUS (customer numbers)

2. **COF_MNI_RELATED_PARTY**:
   - CUSTNO → RELCUS (customer numbers)
   - CUSTNO where ICNEW starts with '-' → XRELCUS (exclusions)
   - ICNEW → RELIC (IC numbers)

3. **COF_EQU_INTRA_GROUP**:
   - CUSTNO → INTRAEQ (equity customer numbers)

4. **COF_EQU_RELATED_PARTY**:
   - CUSTNO → RELEQ (equity customer numbers)

## Input Files

### Parquet Files
- **REPTDATE.parquet**: Contains REPTDATE field
- **CMM{MM}.parquet**: Commodity/Money Market data (MM = month in 2-digit format)
- **EQU{MM}.parquet**: Equity data (MM = month in 2-digit format)
- **VOSTRO.parquet**: VOSTRO account data
- **CISINFO.parquet**: Customer information
- **COF_MNI_INTRA_GROUP.parquet**: Intra-group customer list
- **COF_MNI_RELATED_PARTY.parquet**: Related party customer list
- **COF_EQU_INTRA_GROUP.parquet**: Equity intra-group list
- **COF_EQU_RELATED_PARTY.parquet**: Equity related party list

### Text Files
- **TEMPL.txt**: Template file with item descriptions (fixed-width format)
  - Columns 1-8: ITEM code
  - Columns 10-129: IDESC (item description)
- **WALK.txt**: GL walk file (fixed-width format)
  - Columns 2-20: SET_ID (GL account code)
  - Columns 42-61: AMOUNT (formatted with commas)
  - Column 62: SIGN (' ' = negative, other = positive)

## Output Files

### COF_OUTPUT.txt
Pipe-delimited text file with ASA carriage control characters containing:
- Header: Bank name, report title, date
- Data rows: Item descriptions and amounts by maturity bucket
- Column headers inserted after section breaks

**Format**:
```
 PUBLIC BANK BERHAD
 LIQUIDITY COVERAGE RATIO (LCR) TABLE 4 AS AT DD/MM/YYYY
 CONCENTRATION OF FUNDING
 
 {IDESC}|{BUC1}|{BUC2}|{BUC3}|{BUC4}|{BUC5}|{BUC6}|{BUC7}|{AMOUNT}|
 |Deposit Type|up to 1 week|> 1 wk - 1 mth|> 1 - 3 mths|> 3 - 6 mths|> 6 mths -  1 yr|> 1 year|No specific maturity|Total|
```

**ASA Carriage Control**: Each line starts with a space character (single spacing)

### SFTP_SCRIPT.txt
Contains SFTP command for file transfer:
```
put //SAP.PBB.LCR.COF.TEXT  COF_DDMMYY.XLS
```

## Column Specifications

### CMM/EQU Data
| Column | Type | Description |
|--------|------|-------------|
| CMMCODE | String | Combined code: BIC(5) + CUST(2) + REM(2) + ECP(2) |
| CUSTNO | Numeric | Customer number |
| NEWIC | String | New IC/business registration |
| AMOUNT | Numeric | Amount value |

### VOSTRO Data
| Column | Type | Description |
|--------|------|-------------|
| ACCTNO | String | Account number |
| AMOUNT | Numeric | Amount value |

### CISINFO Data
| Column | Type | Description |
|--------|------|-------------|
| ACCTNO | String | Account number |
| CUSTNO | Numeric | Customer number |
| CUSTEQNO | String | Equity customer number |
| NEWIC | String | New IC/business registration |

### Template Data
| Column | Type | Description |
|--------|------|-------------|
| ITEM | String | 8-character item code |
| IDESC | String | 120-character item description |
| RECNO | Integer | Record sequence number |

## Dependencies

```
polars>=0.20.0
duckdb>=0.9.0 (not actively used but imported)
```

## Installation

```bash
pip install polars
```

## Configuration

Edit the path configuration at the top of `lcr_cof_report.py`:

```python
INPUT_DIR = Path("/path/to/input")
OUTPUT_DIR = Path("/path/to/output")
LIST_DIR = Path("/path/to/list")
```

## Usage

```bash
python lcr_cof_report.py
```

## Processing Steps

1. **Read REPTDATE**: Extract report date and calculate formatting parameters
2. **Read Template**: Load item descriptions with sequence numbers
3. **Read GL Data**: Process WALK file and apply format mappings
4. **Read COF Data**: Load CMM and EQU data for current month
5. **Read Customer Lists**: Load all customer classification lists
6. **Create TAG Classifications**: Assign TAG 2 and 3 based on customer lists
7. **Apply Formats**: Map BIC codes to report items using format tables
8. **Allocate Buckets**: Distribute amounts to maturity buckets based on REM
9. **Create Breakdowns**: Generate retail/wholesale funding breakdown (COF45)
10. **Process VOSTRO**: Filter and classify VOSTRO accounts
11. **Combine Data**: Merge all data sources
12. **Create Summaries**: Aggregate at item, subtotal, and total levels
13. **Generate Report**: Merge with template and write formatted output
14. **Generate SFTP Script**: Create file transfer command

## Format Mapping Examples

### COFF1FMT (Total Liabilities)
```
95311 → 1.01I    (Demand Deposits)
95312 → 1.02I    (Savings Deposits)
95313 → 1.03I    (Fixed Deposits)
95810 → 1.04I    (Deposits from banks)
```

### GLCOFFMT (GL Accounts)
```
F143110VLB  → 1.10I    (Variable rate deposits)
F143130     → 1.11I    (Structured deposits)
F144111RM   → 1.12I    (Repo liabilities)
F142600FBI  → 1.27II   (Federal funds purchased)
```

## Summary Levels

1. **COFITEM**: Detail level - one row per ITEM code
2. **COFSUBTOT**: Subtotal level - format: {PART1}.00{PART2}
   - Example: 1.00I (Subtotal for section 1, type I)
3. **COFTOT**: Total level - format: {PART1}.99
   - Example: 1.99 (Grand total for section 1)
4. **COFSPCL**: Special subtotals for prefixes 5.04 and 5.05

## Error Handling

The script includes comprehensive error handling:
- File not found errors
- Data type conversion errors
- Missing column handling
- General exceptions with full traceback

## Performance Considerations

- **Polars**: Used for fast dataframe operations
- **In-memory processing**: All data loaded into memory for speed
- **Efficient aggregations**: Group-by operations optimized
- **Minimal I/O**: Single pass through input files

## Differences from SAS

1. **Data Format**: Uses Parquet for input datasets instead of SAS datasets
2. **Output Format**: Generates text file with same format as SAS
3. **Customer Lists**: Converted to Python sets for efficient lookup
4. **Format Application**: Uses dictionary lookups instead of PROC FORMAT
5. **ASA Carriage Control**: Manually added to each line (space character)

## Notes

- The delimiter character is chr(0x05) (ASCII 5)
- ASA carriage control uses space character for single spacing
- Column headers are inserted after each section header
- Amounts are formatted with comma separators and 2 decimal places
- Empty/None values are displayed as empty strings in the output
- The SFTP script references mainframe dataset names (//SAP.PBB.LCR.COF.TEXT)

## Validation Points

To verify correct conversion:

1. Check total amounts match between SAS and Python outputs
2. Verify bucket allocations (BUC1-BUC7) sum to total AMOUNT
3. Confirm TAG classifications (especially intra-group and related party)
4. Validate format mappings produce correct ITEM codes
5. Ensure subtotals and totals calculate correctly
6. Verify ASA carriage control characters present in output
7. Check delimiter characters (0x05) between fields
