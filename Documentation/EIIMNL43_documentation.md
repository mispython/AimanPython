# BNM Liquidity Framework Reporting - Python Conversion

## Overview
This program transforms the consolidated BNM liquidity data (FINALSUM) into formatted reports for regulatory submission, pivoting data by time buckets and categorizing by product types.

## Key Features:
### 1. Complete Python Implementation (`bnm_liquidity_reporting.py`)
- Processes consolidated FINALSUM data
- Assigns product descriptions and item codes
- Pivots data by time buckets (transpose operation)
- Generates semicolon-delimited NLF reports

### 2. Data Transformation:
#### Input Structure:
```
   BNMCODE: 14-character code (e.g., '932110901000Y')
   AMOUNT: Balance amount
```

#### Output Structure (after pivot):
```
   PROD, DESC, WEEK, MONTH, QTR, HALFYR, YEAR, LAST, BALANCE
```

### 3. Product Categories:
#### RM (Ringgit Malaysia) - A1.xx:
- Fixed Term Financing (FL)
- Revolving Financing (RC)
- Cash Line Facility (OD)
- Housing Financing (HL)
- Cash Holdings, SRR, Other Assets

#### FX (Foreign Exchange) - B1.xx:
- Same categories as RM but for foreign currency

#### Customer Types:
- 08: Individuals
- 09: Non-Individuals
- 00: Miscellaneous

### 4. Output Datasets:
#### Four Parquet Files:
- NOTERMP1: RM Part 1 (93xxx - Non-performing RM)
- NOTERMP2: RM Part 2 (95xxx - Performing RM)
- NOTEFXP1: FX Part 1 (94xxx - Non-performing FX)
- NOTEFXP2: FX Part 2 (96xxx - Performing FX)

#### Two NLF Reports (semicolon-delimited):
- nlf_rm_part1.txt: Ringgit Part 1 report
- nlf_rm_part2.txt: Ringgit Part 2 report

### 5. Report Format:
```
   BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - RINGGIT PART 1-RM
   
   INFLOW(ASSETS)
   ON BALANCE SHEET
       ;    ;    ;    ;UP TO 1 WK ;>1 WK - 1 MTH;>1 MTH - 3 MTHS;...
   A1.01;LOAN :;NON-INDIVIDUALS;- FIXED TERM FINANCING  ;1234.5;2345.6;...
   A1.02;LOAN :;NON-INDIVIDUALS;- REVOLVING FINANCING  ;567.8;890.1;...
```

### 6. Key Processing Steps:
- Extract PROD (first 7 chars of BNMCODE)
- Round amounts to nearest 1000 (divide by 1000)
- Assign descriptions based on PROD code
- Pivot by time bucket (chars 8-9 of BNMCODE)
- Calculate total BALANCE across all buckets
- Split into 4 datasets by prefix (93/95/94/96)
- Generate formatted reports with headers

### 7. Special Features:
- MISSING=0: Zero values displayed as empty strings in reports
- Credit Cards: Commented out in original (A1.06, B1.06)
- Rounding: Amounts rounded to thousands for reporting
- Sorting: By INDNON descending (09 before 08)
