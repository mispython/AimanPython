# BNM Liquidity Loans - Python Conversion

## Overview
This is a simplified version of the BNM liquidity framework loan processing program (compared to EIBWLIQ2). It processes loan data directly from the report date without complex run-off date calculations.

## Key Features:
### 1. Complete Python Implementation (bnm_liquidity_loans.py)
- Processes loan repayment schedules
- Calculates maturity profiles by time buckets
- Handles performing vs non-performing classification
- Excludes FCY (foreign currency) products in output


### 2. Main Differences from EIBWLIQ2:
- Simpler Date Logic: Uses REPTDATE directly instead of calculating RUNOFFDT
- Same Payment Processing: Identical payment schedule logic
- FCY Exclusion: Foreign currency products excluded from final output
- Integrated EIR: EIR adjustments processed inline with loans


### 3. Processing Logic:
#### Overdrafts (OD):
- Immediate classification (bucket 01)
- Item 213 (Others) or 219 (product 34240)

#### Loans (LN):
- Calculate payment schedules
- Assign time buckets based on remaining months
- Non-performing if DAYS > 89 or LOANSTAT ≠ 1
- EIR adjustments added to bucket 06

#### Revolving Credits (RC):
- Special dual recording in both 95212 and 93212 codes


### 4. Key Calculations:
#### Remaining Months:

```
remmth = (years × 12) + months + (days / month_days)
```

#### Time Buckets:
- 01: < 0.1 months (< 1 week)
- 02: 0.1-1 month
- 03: 1-3 months
- 04: 3-6 months
- 05: 6-12 months
- 06: > 12 months


### 5. Output Files:
- bnm_note.parquet: Final BNM codes with amounts
- fcy_loans_report.txt: FCY records (should be empty)
- bnm_note_report.txt: Full BNM NOTE listing


### Differences from Full EIBWLIQ2:
| Feature | EIBWLIQ2 | This Program |
| ------- | -------- | ------------ |
| Run-off Date | Calculated (month-end) | Uses REPTDATE directly |
| Date Filtering | Checks against RUNOFFDT | Checks against REPTDATEFCY |
| FCY Products | Processed but commented out | Excluded entirely |
| EIR Processing | Separate step from weekly data | Integrated in main loop |
| Complexity | High (run-off methodology) | Medium (direct processing) |

This simplified version is suitable when run-off date calculations aren't required and processing can be done directly from the report date.
