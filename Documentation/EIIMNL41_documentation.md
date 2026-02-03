# OD Liquidity Calculation (BNM Version) - Python Conversion

## Overview
This is a variant of the OD liquidity calculation program that uses BNM.REPTDATE instead of LOAN.REPTDATE as the source for the report date. Otherwise, it follows the exact same 48-week rolling window methodology.

## Key Features:
### 1. Complete Python Implementation (`od_liquidity_calc_bnm.py`)
- Identical logic to the previous OD liquidity program
- Uses BNM.REPTDATE as date source
- 48-week rolling window methodology
- Processes Corporate and Individual overdrafts

### 2. Main Difference from Previous Version:
- Date Source: BNM.REPTDATE instead of LOAN.REPTDATE
- Input Path: Different source parquet file for report date
- All other logic: Identical (calculations, methodology, outputs)

### 3. Processing Summary:
- Extracts ODCORP (9521309) and ODIND (9521308) balances
- Maintains 48-week historical window
- Calculates current and minimum balances
- Spreads volatile portion across 5 buckets
- Assigns stable minimum to bucket 06

### 4. Output Files:
- bnm_calc.parquet: Combined ODCORP and ODIND results
- bnm_store_{product}.parquet: Historical data
- bnm_base_{product}.parquet: Updated base (if INSERT='Y')
- 14 Reports: 7 per product (48-week, current, minimum, table, maturity profile, data, store)
