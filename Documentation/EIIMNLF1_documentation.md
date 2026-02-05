# EIIMNLF1: BNM Overdraft Maturity Profile - Simplified Version

## Overview
This is a **simplified version** of the BNM maturity profile calculation that processes **only Overdraft (OD) products** for Corporate and Individual customers. This is based on the JCL job EIIMNLF1.

Unlike the full version (`bnm_calc_conversion.py`) which processes PBCARD, OD, and SMC, this version focuses solely on overdrafts.

## Key Differences from Full Version

| Feature | Full Version | Simplified Version (This) |
|---------|--------------|---------------------------|
| PBCARD | ✅ Processed | ❌ Not included |
| SMC | ✅ Processed | ❌ Not included |
| Overdrafts | ✅ Processed | ✅ Processed (only) |
| WALK file | ✅ Required | ❌ Not required |
| Products | 5 (PBCARD + 4 OD/SMC) | 2 (ODCORP + ODIND) |

## Requirements
```bash
pip install duckdb polars pyarrow
```

## Input Files

### 1. **`./input/loan/reptdate.parquet`**
- Contains: `reptdate` (reporting date)

### 2. **`./input/bnm/table.parquet`**
- Reference table with maturity buckets
- Contains: `remmth` (remaining months)

### 3. **`./input/bnm/note.parquet`**
- Output from bnm_note_conversion.py
- Contains: `bnmcode`, `amount`

### 4. **Historical Store Files** (created/updated by program)
- `./input/bnm/base_ODCORP.parquet`: All historical records (Corporate)
- `./input/bnm/store_ODCORP.parquet`: Records up to current date (Corporate)
- `./input/bnm/base_ODIND.parquet`: All historical records (Individual)
- `./input/bnm/store_ODIND.parquet`: Records up to current date (Individual)

## Output Files

### 1. **`./output/reptdate.parquet`**
- Reporting date information

### 2. **`./output/calc.parquet`**
- Combined results for ODCORP and ODIND
- Columns: `bnmcode`, `amount`

### 3. **`./input/bnm/calc.parquet`**
- Same as output/calc.parquet (for downstream processing)

### 4. **Updated Store Files** (in BNM_DIR)
- `base_ODCORP.parquet`: Updated with latest week
- `store_ODCORP.parquet`: Filtered up to reporting date
- `base_ODIND.parquet`: Updated with latest week
- `store_ODIND.parquet`: Filtered up to reporting date

## Processing Flow

### 1. Extract from NOTE
```python
# Corporate OD: Extract records with BNMCODE starting with '9521309'
# Individual OD: Extract records with BNMCODE starting with '9521308'
```

### 2. Sum Amounts
```python
total_amount = sum(amounts for matching records)
```

### 3. Append to Historical Data
```python
if INSERT == 'Y':
    # Official reporting week
    # Remove old record for this date, add new
    # Update BASE file
else:
    # Interim calculation
    # Don't update BASE, create temp STORE
```

### 4. Calculate 48-Week Min/Max
```python
current_balance = most_recent_week_amount
minimum_balance = min(last_48_weeks)
```

### 5. Distribute Across Maturity Buckets
```python
for each bucket:
    if remmth < 12 months:
        amount = (current - minimum) / 5
    else:  # >= 12 months
        amount = minimum
```

### 6. Generate NPL BNMCODEs
```python
# Corporate: 93213090X0000Y (where X = bucket 01-06)
# Individual: 93213080X0000Y (where X = bucket 01-06)
```

## BNMCODE Structure

### Overdrafts Output
- **ODCORP (Corporate)**: `9321309XX0000Y`
  - `93`: NPL (Non-Performing Loans)
  - `213`: Overdrafts
  - `09`: Non-Individual (Corporate)
  - `XX`: Maturity bucket (01-06)
  
- **ODIND (Individual)**: `9321308XX0000Y`
  - `93`: NPL
  - `213`: Overdrafts
  - `08`: Individual
  - `XX`: Maturity bucket (01-06)

## Example Calculation

### Corporate OD
```
From NOTE.parquet:
  9521309010000Y: 100,000
  9521309020000Y: 150,000
  9521309030000Y: 200,000
  9521309040000Y: 250,000
  9521309050000Y: 300,000
  9521309060000Y: 500,000
  
Total = 1,500,000

Historical 48 weeks:
  Week 1 (current): 1,500,000
  Week 2: 1,450,000
  ...
  Week 48: 1,200,000
  
Minimum = 1,200,000

Distribution:
  Buckets 01-05 (< 12 months): (1,500,000 - 1,200,000) / 5 = 60,000 each
  Bucket 06 (>= 12 months): 1,200,000

Output:
  93213090100Y: 60,000
  93213090200Y: 60,000
  93213090300Y: 60,000
  93213090400Y: 60,000
  93213090500Y: 60,000
  93213090600Y: 1,200,000
```

## Insert Week Logic

Same as full version:

**INSERT = 'Y'** (Days 8, 15, 22, or month-end):
- Official reporting week
- Update BASE file (remove old, add new)
- Create STORE from updated BASE

**INSERT = 'N'** (Other days):
- Interim calculation
- Don't update BASE
- Create temp STORE (BASE + new record)

## Comparison with Full Version

### This Simplified Version:
✅ Faster execution (fewer products)
✅ Simpler to understand
✅ No WALK file dependency
✅ Suitable when only OD data needed

### Full Version (bnm_calc_conversion.py):
✅ Complete product coverage
✅ Includes PBCARD (credit cards)
✅ Includes SMC (share margin credit)
✅ Required for complete BNM submission

## When to Use This Version

Use this **simplified version** when:
- You only need OD maturity profiles
- Testing/validation of OD logic
- PBCARD and SMC are handled elsewhere
- Quick turnaround for OD-only reports

Use the **full version** when:
- Complete BNM submission required
- All products must be included
- Production reporting

## Execution Order

```bash
# Step 1: Run NOTE conversion
python bnm_note_conversion.py

# Step 2: Run this simplified OD calculation
python bnm_od_calc_simplified.py
```

Or for complete processing:

```bash
# Use the full version instead
python bnm_calc_conversion.py
```

## Validation

Compare outputs:
```python
# Simplified version output
od_simple = pl.read_parquet('output/calc.parquet')

# Full version output (OD records only)
calc_full = pl.read_parquet('output/calc_full.parquet')
od_full = calc_full.filter(
    pl.col('bnmcode').str.slice(0, 5).is_in(['93213'])
)

# Should match
assert od_simple.equals(od_full)
```

## Key Files Breakdown

| File | Purpose |
|------|---------|
| reptdate.parquet | Reporting date |
| table.parquet | Maturity bucket reference |
| note.parquet | Input (from previous step) |
| base_ODCORP.parquet | Corporate OD history (all time) |
| store_ODCORP.parquet | Corporate OD history (up to date) |
| base_ODIND.parquet | Individual OD history (all time) |
| store_ODIND.parquet | Individual OD history (up to date) |
| calc.parquet | Output (NPL maturity profile) |

## Troubleshooting

### Issue: No OD data in NOTE
**Solution**: Verify bnm_note_conversion.py ran successfully and created OD records

### Issue: Historical data missing
**Solution**: First run creates empty history; subsequent runs build it

### Issue: Amounts don't match expected
**Solution**: Check 48-week window calculation and min/max logic

## Performance

Typical execution time:
- With historical data: ~10-30 seconds
- First run (no history): ~5-10 seconds

## Migration Note

This simplified version is extracted from the JCL job EIIMNLF1 which appears to be a focused OD-only processing job, likely used for:
- Quick OD reporting
- OD-specific analysis
- Testing OD calculations
- Interim OD reports

For production BNM submission, use the complete suite including all products.

## Related Programs

1. **bnm_note_conversion.py** - Creates NOTE.parquet (prerequisite)
2. **bnm_calc_conversion.py** - Full version with all products
3. **bnm_glset_consolidation.py** - Final consolidation
4. **bnm_report_generation.py** - Report generation

---

This is a **focused, simplified version** for OD-only processing. For complete BNM reporting, use the full program suite.
