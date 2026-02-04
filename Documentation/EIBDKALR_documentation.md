# EIBDKALR - SAS to Python Conversion: BNMK Table Processor

## Overview

This Python script converts the SAS program that processes BNMTBL1 and BNMTBL3 files to create K1TBL and K3TBL output files. The conversion uses DuckDB for efficient CSV processing and Polars for data manipulation.

## File Structure

- **bnmk_processor.py**: Main Python script that replicates the SAS DATA steps

## SAS Program Logic

The original SAS program consists of:

1. **REPTDATE Calculation**: Determines the report date (today - 1), calculates the week of the month (1-4), and extracts the month in 2-digit format
2. **K1TBL Processing**: Reads BNMTBL1 (KAPITI1) pipe-delimited file and creates K1TBL dataset
3. **K3TBL Processing**: Reads BNMTBL3 (KAPMNI) pipe-delimited file and creates K3TBL dataset

## Key Conversion Details

### Date Calculations

The SAS macro variables `&NOWK` and `&REPTMON` are calculated as follows:
- **REPTDATE**: Today - 1 day
- **NOWK**: Week of month (1: days 1-8, 2: days 9-15, 3: days 16-22, 4: days 23+)
- **REPTMON**: Month in 2-digit zero-padded format (01-12)

### K1TBL Processing (BNMTBL1)

**Input**: Pipe-delimited text file (KAPITI1.txt)
- First line contains REPTDATE in YYYYMMDD format
- Subsequent lines contain 59 fields

**Key Processing**:
- REPTDATE is read from the first line and added to all records
- Date fields GWSDT and GWMDT are converted from numeric YYYYMMDD to date format
- Zero values in date fields are converted to NULL

**Output**: K1TBL{REPTMON}{NOWK}.parquet (e.g., K1TBL011.parquet for January, week 1)

### K3TBL Processing (BNMTBL3)

**Input**: Pipe-delimited text file (KAPMNI.txt)
- First line contains REPTDATE in YYYYMMDD format
- Subsequent lines contain 36 fields (note: includes UTBRNM field which is not in the first DATA step)

**Key Processing**:
1. Filter out records where UTDLP substring (positions 2-3) equals 'RT' or 'RI'
2. Parse date fields from DD/MM/YYYY format:
   - UTMDT → MATDT
   - UTOSD → ISSDT
   - UTCBD → REPTDATE, DDATE
   - UTIDT → XDATE
3. If UTSTY = 'IZD', set UTCPR = UTQDS
4. For security types ('IFD','ILD','ISD','IZD'), delete records where XDATE > DDATE

**Output**: K3TBL{REPTMON}{NOWK}.parquet (e.g., K3TBL011.parquet for January, week 1)

## Column Specifications

### K1TBL Columns (59 fields)

| Column | Type | Width | Description |
|--------|------|-------|-------------|
| REPTDATE | Date | - | Report Date (added from file) |
| GWAB | String | 4 | BRANCH |
| GWAN | String | 6 | BASIC NUMBER |
| GWAS | String | 3 | ACCOUNT SUFFIX |
| GWAPP | String | 2 | - |
| GWACS | String | 1 | - |
| GWBALA | Numeric | 15.2 | BALANCE - ORIGINAL CURRENCY |
| GWBALC | Numeric | 15.2 | BALANCE - LOCAL CURRENCY |
| GWPAIA | Numeric | 15.2 | - |
| GWPAIC | Numeric | 15.2 | - |
| GWSHN | String | 15 | ACCOUNT SHORT NAME |
| GWCTP | String | 2 | CUSTOMER TYPE |
| GWACT | String | 2 | A/C TYPE |
| GWACD | String | 2 | ANALYSIS CODE |
| GWSAC | String | 2 | SUNDRY ANALYSIS CODE |
| GWNANC | String | 5 | - |
| GWCNAL | String | 2 | RESIDENCE COUNTRY |
| GWCCY | String | 3 | CCY |
| GWCNAR | String | 2 | - |
| GWCNAP | String | 2 | - |
| GWDIAA | Numeric | 15.2 | - |
| GWDIAC | Numeric | 15.2 | DR INT ACC - LOCAL CURRENCY |
| GWCIAA | Numeric | 15.2 | - |
| GWCIAC | Numeric | 15.2 | CR INT ACC - LOCAL CURRENCY |
| GWRATD | Numeric | 11.6 | - |
| GWRATC | Numeric | 11.6 | - |
| GWDIPA | Numeric | 17.2 | - |
| GWDIPC | Numeric | 17.2 | - |
| GWCIPA | Numeric | 17.2 | - |
| GWCIPC | Numeric | 17.2 | - |
| GWPL1D | String | 2 | - |
| GWPL2D | String | 2 | - |
| GWPL1C | String | 2 | - |
| GWPL2C | String | 2 | - |
| GWPALA | Numeric | 17.2 | - |
| GWPALC | Numeric | 17.2 | - |
| GWDLP | String | 3 | DEAL TYPE |
| GWDLR | String | 13 | DEAL REFERENCE |
| GWSDT | Date | 8 | START DATE |
| GWRDT | Numeric | 8 | - |
| GWRRT | Numeric | 5 | - |
| GWPDT | Numeric | 8 | - |
| GWPRT | Numeric | 5 | - |
| GWPCM | Numeric | 3 | - |
| GWMOTC | String | 4 | - |
| GWMRTC | String | 4 | - |
| GWMRT | Numeric | 5 | - |
| GWMDT | Date | 8 | MATURITY DATE |
| GWMCM | Numeric | 3 | - |
| GWMWM | Numeric | 3 | - |
| GWMVT | String | 1 | MOVEMENT TYPE |
| GWMVTS | String | 1 | MOVEMENT SUB-TYPE |
| GWSRC | String | 2 | - |
| GWUC1 | String | 3 | - |
| GWUC2 | String | 3 | - |
| GWC2R | String | 3 | CUSTFISS |
| GWAMAP | Numeric | 15.2 | - |
| GWEXR | Numeric | 13.7 | TRANSACTED FX/RM RATES |
| GWOPT | String | 1 | - |
| GWOCY | String | 3 | OTHER CURRENCY MNEMONIC |
| GWCBD | String | - | CURRENT BUSINESS DATE |

### K3TBL Columns (36 fields + derived fields)

| Column | Type | Width | Description |
|--------|------|-------|-------------|
| REPTDATE | Date | - | Report Date (added from file) |
| UTSTY | String | 3 | SECURITY TYPE |
| UTREF | String | 16 | PORTFOLIO REFERENCE |
| UTBRNM | String | 3 | BRANCH |
| UTDLP | String | 3 | DEAL PREFIX |
| UTDLR | String | 13 | DEAL REFERENCE |
| UTSMN | String | 16 | SECURITY MNEMONIC |
| UTCUS | String | 6 | CUSTOMER NAME |
| UTCLC | String | 3 | CUSTOMER LOCATION |
| UTCTP | String | 2 | CUSTOMER TYPE |
| UTFCV | Numeric | 15.2 | FACE VALUE |
| UTIDT | String | 10 | ISSUE DATE |
| UTLCD | String | 10 | LAST COUPON DATE |
| UTNCD | String | 10 | NEXT COUPON DATE |
| UTMDT | String | 10 | MATURITY DATE |
| UTCBD | String | 10 | CURRENT BUSINESS DATE |
| UTCPR | Numeric | 13.7 | COUPON RATE |
| UTQDS | Numeric | 13.7 | DISCOUNT RATE |
| UTPCP | Numeric | 13.7 | DEAL CAPITAL PRICE |
| UTAMOC | Numeric | 15.2 | CURRENT AMOUNT OWNED |
| UTDPF | Numeric | 15.2 | DISCOUNT/PREMIUM UNEARNED |
| UTAICT | Numeric | 15.2 | ACTUAL INT ACCRUED COUPON TO DATE |
| UTAICY | Numeric | 15.2 | ACTUAL INT ACCRUED COUPON TO YESTERDAY |
| UTAIT | Numeric | 15.2 | ACTUAL INT ACCRUED P&L TODAY |
| UTDPET | Numeric | 15.2 | DISCOUNT/PREMIUM UNEARNED TODAY |
| UTDPEY | Numeric | 15.2 | DISCOUNT/PREMIUM UNEARNED YESTERDAY |
| UTDPE | Numeric | 15.2 | DISCOUNT/PREMIUM P&L TODAY |
| UTASN | Numeric | 7 | ASSET NUMBER |
| UTOSD | String | 10 | OWNERSHIP SETTLEMENT DATE |
| UTCA2 | String | 2 | ANALYSIS CODE |
| UTSAC | String | 2 | SUNDRY ANALYSIS CODE |
| UTCNAP | String | 2 | PARENT COUNTRY |
| UTCNAR | String | 2 | RISK COUNTRY |
| UTCNAL | String | 2 | RESIDENCE COUNTRY |
| UTCCY | String | 3 | CURRENCY CODE |
| UTAMTS | Numeric | 15.2 | PURCHASE PROCEEDS |
| UTMM1 | String | 3 | MARKET MARKER 1 |
| MATDT | Date | - | Maturity Date (parsed) |
| ISSDT | Date | - | Issue Date (parsed) |
| DDATE | Date | - | Current Business Date (parsed) |
| XDATE | Date | - | Issue Date (parsed) |

## Dependencies

```
duckdb>=0.9.0
polars>=0.20.0
```

## Installation

```bash
pip install duckdb polars
```

## Configuration

Edit the path configuration at the top of `bnmk_processor.py`:

```python
INPUT_DIR = Path("/path/to/input")
OUTPUT_DIR = Path("/path/to/output")

BNMTBL1_PATH = INPUT_DIR / "KAPITI1.txt"
BNMTBL3_PATH = INPUT_DIR / "KAPMNI.txt"
```

## Usage

```bash
python bnmk_processor.py
```

## Output

The script generates two parquet files:
- `K1TBL{MM}{W}.parquet` - e.g., K1TBL011.parquet (January, Week 1)
- `K3TBL{MM}{W}.parquet` - e.g., K3TBL011.parquet (January, Week 1)

Where:
- MM = 2-digit month (01-12)
- W = week of month (1-4)

## Error Handling

The script includes comprehensive error handling:
- File not found errors
- Date parsing errors
- Data type conversion errors
- General exceptions with full traceback

## Performance Considerations

- **DuckDB**: Used for efficient reading of large CSV files with proper type inference
- **Polars**: Provides fast columnar operations for data transformation
- **No sorting**: Unnecessary sort operations have been removed for efficiency
- **Lazy evaluation**: DuckDB and Polars use lazy evaluation where possible

## Differences from SAS

1. **Output format**: Changed from SAS dataset to Parquet format for better interoperability
2. **Date handling**: Python datetime objects are used instead of SAS date values
3. **Missing values**: Empty strings and NULL are handled consistently
4. **Type safety**: Explicit type definitions ensure data quality

## Notes

- The second DATA step for K3TBL includes the UTBRNM field which is not present in the first DATA step, suggesting this is from a later version of the SAS code
- Date format parsing assumes DD/MM/YYYY for BNMTBL3 dates based on the DDMMYY10. format specification
- The REPTDATE field is retained at the beginning of each record as specified by the RETAIN statement
