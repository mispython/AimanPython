# EIBWLIQ2 - BNM Liquidity Framework Documentation

## Overview
This program processes loan data to generate BNM (Bank Negara Malaysia) Liquidity Framework reports showing contractual run-offs by maturity profile as of end of month.

## Program Purpose
EIBWLIQ2 analyzes loan portfolios and calculates:
1. **Loan repayment schedules** based on payment frequency
2. **Maturity profiles** categorized by time remaining
3. **Customer segmentation** (Financial Institutions vs Others)
4. **Product categorization** (FL, HL, RC, OD, etc.)
5. **Performing vs Non-performing** classifications

## Key Technical Specifications

### Input Files

#### 1. LOAN.REPTDATE (Parquet)
- **Purpose**: Main reporting date
- **Columns**: REPTDATE (date)
- **Usage**: Determines week number and reporting period

#### 2. LOAN1.REPTDATE (Parquet)
- **Purpose**: Weekly reporting date
- **Columns**: REPTDATE (date)
- **Usage**: For EIR adjustment processing

#### 3. BNM1.LOAN{REPTMON}{REPTDAY} (Parquet)
- **Purpose**: Main loan data
- **Key Columns**:
  - PAIDIND: Payment indicator
  - PRODCD: Product code
  - PRODUCT: Product number
  - CUSTCD: Customer code
  - ACCTYPE: Account type (LN, OD)
  - BALANCE: Current balance
  - BLDATE: Last billing date
  - ISSDTE: Issue date
  - EXPRDATE: Expiry date
  - PAYAMT: Payment amount
  - PAYFREQ: Payment frequency
  - LOANSTAT: Loan status

#### 4. BNM2.LOAN{WKMON}{WKDAY} (Parquet)
- **Purpose**: Weekly loan data for EIR adjustments
- **Key Columns**: Same as BNM1.LOAN plus EIR_ADJ

### Output Files

#### 1. bnm_note.parquet
- **Format**: Parquet file
- **Columns**: 
  - BNMCODE (string, 14 characters)
  - AMOUNT (float)
- **Purpose**: Summarized BNM liquidity data

#### 2. eibwliq2_report.txt
- **Format**: Text with ASA carriage control
- **Purpose**: Management report

### Constants

```python
FCY = [800, 801, 802, 803, 804, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860]
# Foreign currency product codes
```

## BNM Code Structure

### Format: XXYYYCCTTFFFFY
- **XX** (2 digits): Report category
  - 93: Non-performing loans (RM)
  - 94: Non-performing loans (FCY)
  - 95: Performing loans (RM)
  - 96: Performing loans (FCY)

- **YYY** (3 digits): Item code
  - 211: Fixed Loans (FL) / Housing Loans (HL) - Others
  - 212: Revolving Credits (RC) - Others
  - 213: Overdrafts (OD) - Others
  - 214: Housing Loans (HL) - Financial Institutions
  - 215: (Reserved)
  - 219: Other loans

- **CC** (2 digits): Customer category
  - 08: Financial Institutions (CUSTCD: 77, 78, 95, 96)
  - 09: Others

- **TT** (2 digits): Time bucket
  - 01: Up to 1 week (0 to 0.255 months)
  - 02: >1 week to 1 month (0.255 to 1 month)
  - 03: >1 month to 3 months (1 to 3 months)
  - 04: >3 months to 6 months (3 to 6 months)
  - 05: >6 months to 1 year (6 to 12 months)
  - 06: >1 year (12+ months)
  - 07: Missing/Invalid

- **FFFF** (4 digits): Filler (0000)

- **Y** (1 digit): Year indicator (Y)

### Examples:
- `95211090100000Y`: Performing FL/HL (Others), Time bucket 01
- `93212080300000Y`: Non-performing RC (FI), Time bucket 03
- `95213090200000Y`: Performing OD (Others), Time bucket 02

## Business Logic

### Week Number Determination
Based on day of month from REPTDATE:
| Day of Month | Week Number |
|--------------|-------------|
| 8 | 1 |
| 15 | 2 |
| 22 | 3 |
| All others | 4 |

### Run-off Date Calculation
- Run-off date = Last day of reporting month
- Example: If REPTDATE = 2025-01-15, RUNOFFDT = 2025-01-31

### Account Filtering
Accounts are processed if:
- PAIDIND NOT IN ('P', 'C') - Not paid off or closed
- PRODCD starts with '34' OR PRODUCT IN (225, 226) - Islamic products

### Product Type Classification

**Liquidity Product Format (LIQPFMT)**:
- **FL**: Fixed Loans
- **HL**: Housing Loans
- **RC**: Revolving Credits
- **OD**: Overdrafts
- **OT**: Others

### Customer Category Classification

| CUSTCD | Category | Code |
|--------|----------|------|
| 77, 78, 95, 96 | Financial Institutions | 08 |
| All others | Others | 09 |

### Item Code Determination

**For Financial Institutions (08)**:
| Product Type | Item Code |
|--------------|-----------|
| HL | 214 |
| Others | 219 |

**For Others (09)**:
| Product Type | Item Code |
|--------------|-----------|
| FL, HL | 211 |
| RC | 212 |
| OD | 213 |
| Others | 219 |

**Special Case**: Product 100 → Item 212 (ABBA HL Financing)

### Overdraft Processing

For ACCTYPE = 'OD':
- Remaining months (REMMTH) = 0.1 (bucket 01)
- Amount = Current balance
- Item = 213 (or 219 if PRODCD = '34240')

### Loan Processing Algorithm

#### Payment Frequency Conversion
| PAYFREQ Code | Months | Description |
|--------------|--------|-------------|
| 1 | 1 | Monthly |
| 2 | 3 | Quarterly |
| 3 | 6 | Semi-annual |
| 4 | 12 | Annual |
| 6 | 14 days | Bi-weekly |
| 5, 9, blank | Bullet | Single payment at maturity |

#### Special Products (Bullet Payment)
Products 350, 910, 925 → Treat as bullet payment (single maturity date)

#### Billing Date Calculation

**Initial Billing Date**:
1. If PAYFREQ in ('5', '9', ' ') or PRODUCT in (350, 910, 925):
   - BLDATE = EXPRDATE
2. Else if BLDATE <= 0:
   - BLDATE = ISSDTE
   - Advance to next billing after REPTDATE

**Next Billing Date**:
- If PAYFREQ = '6': Add 14 days
- Else: Add FREQ months, adjust for month-end

#### Remaining Months Calculation

```python
remmth = (MATDT_YEAR - RUNOFFDT_YEAR) * 12 +
         (MATDT_MONTH - RUNOFFDT_MONTH) +
         (MATDT_DAY - RUNOFFDT_DAY) / RUNOFFDT_MONTH_DAYS
```

**Time Bucket Classification**:
| Remaining Months | Bucket | Code |
|------------------|--------|------|
| < 0.255 | Up to 1 week | 01 |
| 0.255 - 1 | >1 week - 1 month | 02 |
| 1 - 3 | >1 - 3 months | 03 |
| 3 - 6 | >3 - 6 months | 04 |
| 6 - 12 | >6 months - 1 year | 05 |
| >= 12 | >1 year | 06 |
| Missing | Missing | 07 |

### Loan Status Classification

**Performing vs Non-performing**:
- **Non-performing** if:
  - DAYS > 89 (more than 90 days since last billing), OR
  - LOANSTAT != 1

**Dual Recording**:
For each payment/balance:
1. Record under 95/96 (Performing) or 93/94 (Non-performing) based on status
2. If non-performing, set REMMTH = 0.1 (bucket 01) for immediate category
3. Also record under 93/94 with REMMTH = 13 (bucket 06 for >1 year)

### Payment Schedule Processing

```
FOR each billing date from current to expiry:
    IF bldate <= runoffdt:
        Skip (already past)
    ELSE IF bldate - runoffdt < 8 days:
        remmth = 0.1
    ELSE:
        Calculate remaining months
    
    IF remmth > 1 OR bldate = exprdate:
        Break (process final balance)
    
    Record payment amount in appropriate bucket
    Reduce balance by payment amount
    Calculate next billing date
END FOR

Record final balance at expiry date
```

### Revolving Credits Special Processing

**RC Corporate Accounts**:
1. Remove all '93212' codes (non-performing RC)
2. Find all '95212' codes (performing RC - Others)
3. Create equivalent '93212' codes from '95212' codes
4. Add back to dataset

This ensures RC accounts are reported in both performing and non-performing categories.

### EIR Adjustment Processing

From weekly loan data (BNM2.LOAN):
- For accounts with EIR_ADJ not null:
  - Add amount to both 95xxx and 93xxx codes
  - Always use bucket 06 (>1 year)

### Default Records

Create zero-amount records for all valid combinations to ensure complete reporting structure:
- Categories: 93, 95 (RM only; 94, 96 for FCY excluded in this version)
- Items: 211, 212, 213, 214, 215, 219
- Customers: 08, 09
- Time buckets: 01-06

Exclusions:
- Items 213, 214, 215, 219 with customer 09
- Items 211, 212 with customer 08

## Data Flow

```
LOAN.REPTDATE → Calculate dates and week numbers
                                ↓
BNM1.LOAN → Filter (not paid/closed, Islamic products)
                                ↓
                    Process by account type
                    ├─→ OD: Calculate immediate runoff
                    └─→ LN: Calculate payment schedule
                                ↓
                        Generate BNM records
                        - Determine item code
                        - Calculate remaining months
                        - Classify time bucket
                        - Create performing (95/96) records
                        - Create non-performing (93/94) if applicable
                                ↓
                        Filter missing buckets (07)
                                ↓
                    Process RC special handling
                                ↓
BNM2.LOAN → Add EIR adjustments (weekly data)
                                ↓
                        Create default records
                                ↓
                    Combine all records
                                ↓
                Group by BNMCODE and sum amounts
                                ↓
        Save to Parquet + Generate Report
```

## Technical Implementation

### Billing Date Advancement Logic

```python
def calculate_next_bldate(bldate, issdte, payfreq, freq):
    if payfreq == '6':
        # Bi-weekly: add 14 days
        return bldate + timedelta(days=14)
    else:
        # Add freq months
        dd = issdte.day
        next_date = bldate + relativedelta(months=freq)
        
        # Adjust for month-end
        last_day = get_month_last_day(next_date.year, next_date.month)
        if dd > last_day:
            dd = last_day
        
        return date(next_date.year, next_date.month, dd)
```

### Remaining Months Calculation

```python
def calculate_remaining_months(matdt, runoffdt):
    years_diff = matdt.year - runoffdt.year
    months_diff = matdt.month - runoffdt.month
    
    runoff_last_day = get_month_last_day(runoffdt.year, runoffdt.month)
    mat_day = min(matdt.day, runoff_last_day)
    days_diff = mat_day - runoffdt.day
    
    remmth = years_diff * 12 + months_diff + days_diff / runoff_last_day
    return remmth
```

### Time Bucket Classification

```python
def classify_remmth(remmth):
    if remmth is None:
        return '07'  # MISSING
    elif remmth < 0.255:
        return '01'  # UP TO 1 WK
    elif remmth < 1:
        return '02'  # >1 WK - 1 MTH
    elif remmth < 3:
        return '03'  # >1 MTH - 3 MTHS
    elif remmth < 6:
        return '04'  # >3 - 6 MTHS
    elif remmth < 12:
        return '05'  # >6 MTHS - 1 YR
    else:
        return '06'  # > 1 YEAR
```

## Example Processing Scenarios

### Scenario 1: Monthly Housing Loan (Performing)
- Product: 101 (HL)
- CUSTCD: 01 (Others)
- PAYFREQ: 1 (Monthly)
- BALANCE: 100,000
- PAYAMT: 1,000
- BLDATE: 2025-02-15
- EXPRDATE: 2027-01-15
- REPTDATE: 2025-01-15
- RUNOFFDT: 2025-01-31
- DAYS: 0 (current)
- LOANSTAT: 1 (performing)

**Processing**:
1. Item code: 211 (FL/HL - Others)
2. First payment on 2025-02-15:
   - Remaining months = 0.5
   - Bucket: 02 (>1 week - 1 month)
   - Code: 95211090200000Y, Amount: 1,000
3. Continue for each monthly payment
4. Final balance at expiry:
   - Remaining months = 23.5
   - Bucket: 06 (>1 year)
   - Code: 95211090600000Y, Amount: remaining balance

### Scenario 2: Overdraft (Non-performing)
- Product: 300 (OD)
- ACCTYPE: OD
- CUSTCD: 05 (Others)
- BALANCE: 50,000
- DAYS: 100 (>90 days overdue)

**Processing**:
1. Item code: 213 (OD - Others)
2. REMMTH: 0.1
3. Bucket: 01
4. Code: 95213090100000Y, Amount: 50,000 (performing category)
5. Since DAYS > 89, also non-performing:
   - Code: 93213090100000Y, Amount: 50,000 (immediate)
   - Code: 93213090600000Y, Amount: 50,000 (>1 year)

### Scenario 3: RC Corporate Account
- Product: 301 (RC)
- CUSTCD: 77 (Financial Institution)
- ACCTYPE: LN
- BALANCE: 200,000
- Status: Performing

**Processing**:
1. Item code: 219 (Other - FI)
2. Calculate payment schedule
3. Generate 95212 codes
4. In RC processing step:
   - Create equivalent 93212 codes
5. Final output includes both 95212xxx and 93212xxx

## Report Format

```
                        BNM LIQUIDITY FRAMEWORK
             CONTRACTUAL RUN-OFFS AS OF END OF THE MONTH
                      REPORTING DATE: 15/01/25
                             Page 1

BNMCODE              AMOUNT
--------------------------------------------------------------------------------
 93211090100000Y           0.00
 93211090200000Y     125,450.50
 93211090300000Y     234,567.80
 ...
 95211090100000Y   1,234,567.89
 95211090200000Y   2,345,678.90
 ...
--------------------------------------------------------------------------------
TOTAL                     12,345,678.90
```

## Performance Optimizations

1. **Iterative Processing**: Process loans one by one to handle complex payment schedules
2. **Date Calculations**: Use Python datetime for accurate date arithmetic
3. **Early Filtering**: Filter accounts early to reduce processing volume
4. **Batch Aggregation**: Group by BNMCODE at end for summary

## Error Handling

The code handles:
- Missing or invalid dates
- Negative payment amounts (set to 0)
- Billing dates beyond expiry (adjust to expiry)
- Invalid payment frequencies
- Missing EIR adjustments

## Testing Checklist

### Data Validation
- [ ] REPTDATE loaded correctly
- [ ] Week number calculated (8→1, 15→2, 22→3, other→4)
- [ ] Run-off date = last day of report month
- [ ] Accounts filtered for Islamic products only
- [ ] Customer categories assigned correctly (FI vs Others)

### Product Classification
- [ ] Item codes assigned correctly
- [ ] Product 100 → Item 212
- [ ] OD accounts → Item 213 or 219
- [ ] HL for FI → Item 214

### Payment Schedule
- [ ] Billing dates calculated correctly
- [ ] Payment frequency converted to months
- [ ] Bi-weekly payments (PAYFREQ=6) work
- [ ] Bullet payments (PAYFREQ=5,9,' ') work
- [ ] Month-end adjustments correct

### Time Buckets
- [ ] Remaining months calculated correctly
- [ ] Time bucket classification accurate
- [ ] Bucket 07 (missing) filtered out

### Performing/Non-performing
- [ ] DAYS > 89 → non-performing
- [ ] LOANSTAT != 1 → non-performing
- [ ] Non-performing creates both immediate and >1 year records
- [ ] Both 95/93 codes generated appropriately

### RC Processing
- [ ] 93212 codes removed initially
- [ ] 95212 codes identified
- [ ] Equivalent 93212 codes created

### EIR Adjustments
- [ ] Weekly data loaded
- [ ] EIR_ADJ amounts added
- [ ] Both 95 and 93 codes created with bucket 06

### Default Records
- [ ] All valid combinations created
- [ ] Exclusions applied correctly

### Summary
- [ ] Grouped by BNMCODE
- [ ] Amounts summed correctly
- [ ] Output sorted by BNMCODE

## Configuration

Update paths in the script:
```python
LOAN_REPTDATE_PATH = "/data/input/loan_reptdate.parquet"
LOAN1_REPTDATE_PATH = "/data/input/loan1_reptdate.parquet"
BNM1_LOAN_PATH = "/data/input/bnm1_loan_{month}{day}.parquet"
BNM2_LOAN_PATH = "/data/input/bnm2_loan_{month}{day}.parquet"
OUTPUT_DIR = "/data/output"
```

## Dependencies

```
duckdb>=0.9.0
polars>=0.19.0
pyarrow>=14.0.0
python-dateutil>=2.8.0
```

## Execution

```bash
python3 eibwliq2.py
```

Expected output structure with record counts and processing steps clearly indicated.
