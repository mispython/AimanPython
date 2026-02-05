# EIBMNL42: BNM GL Set and Final Consolidation - Python Conversion

## Overview
This Python script converts a SAS program that:
1. Processes WALK file data for various General Ledger (GL) accounts
2. Creates GLSET dataset with all GL-based positions
3. Consolidates all BNM outputs into a final comprehensive dataset

This is typically the **final step** in the BNM reporting process, combining outputs from all previous programs.

## Requirements
```bash
pip install duckdb polars pyarrow
```

## Input Files

### 1. **`./input/bnm/reptdate.parquet`**
- Contains: `reptdate` (reporting date)

### 2. **`./input/walk.txt`**
- Fixed-width format file with GL account balances
- Format:
  - Position 2-20: Description/GL code
  - Position 21-22: Day (DD)
  - Position 24-25: Month (MM)
  - Position 27-28: Year (YY, 2-digit)
  - Position 42-61: Amount (comma-formatted decimal)

### 3. **Previous BNM Outputs** (for consolidation)
Located in `./input/bnm/`:
- `note.parquet` - From bnm_note_conversion.py
- `calc.parquet` - From bnm_calc_conversion.py
- `pbif.parquet` - From other BNM program (if exists)
- `bt.parquet` - From other BNM program (if exists)

## Output Files

### 1. **`./output/glset.parquet`**
- Combined GL account positions
- Columns: `bnmcode`, `amount`

### 2. **`./output/final.parquet`**
- All BNM records (NOTE + CALC + PBIF + BT + GLSET)
- Before summarization

### 3. **`./output/finalsum.parquet`**
- Final summarized dataset
- Aggregated by BNMCODE
- **This is the primary deliverable for BNM reporting**

## GL Accounts Processed

### RM (Malaysian Ringgit) Accounts

| Product | GL Code | BNMCODE Pattern | Bucket | Description |
|---------|---------|-----------------|--------|-------------|
| **Floor Stocking** | ML34170 | 93219090X / 95219090X | 05 | >6 months to 1 year |
| **Hire Purchase** | ML34111 | 93211090X / 95211090X | 06 | >1 year |
| **Cash Holdings** | 39110 | 93221000X / 95221000X | 01 | Up to 1 week |
| **SRR** | 32110 | 93222000X / 95222000X | 06 | >1 year |
| **Other Assets** | F137010SH | 93229000X / 95229000X | 01 | Up to 1 week |

### FCY (Foreign Currency) Accounts

| Product | GL Code | BNMCODE Pattern | Bucket | Description |
|---------|---------|-----------------|--------|-------------|
| **FCY Fixed Loan** | F13460081BCB, F13460064FLB | 94211090X / 96211090X | 06 | >1 year |
| **FCY RC** | F134600RC | 94212090X / 96212090X | 06 | >1 year |
| **FCY Cash** | F139610FXNC | 94221000X / 96221000X | 01 | Up to 1 week |

## BNMCODE Structure for GL Accounts

### Pattern: XXYYZZZBBSSSS
- **XX (Positions 1-2)**: Report type
  - `93`: Non-performing loans (RM)
  - `94`: Non-performing loans (FCY)
  - `95`: Gross loans outstanding (RM)
  - `96`: Gross loans outstanding (FCY)
- **YY (Positions 3-4)**: Asset class
  - `21`: Loans and advances
  - `22`: Other assets/reserves
  - `23`: (varies)
- **ZZZ (Positions 5-7)**: Product type
  - `109`: Hire purchase
  - `130`: Overdrafts
  - `190`: Other facilities
  - `210`: Cash
  - `220`: Reserves
  - `290`: Other assets
- **BB (Positions 8-9)**: Maturity bucket
  - `01`: Up to 1 week
  - `02`: >1 week to 1 month
  - `03`: >1 month to 3 months
  - `04`: >3 months to 6 months
  - `05`: >6 months to 1 year
  - `06`: >1 year
- **SSSS (Positions 10-15)**: Suffix (always `0000Y`)

## Processing Logic

### Week Number Calculation
```python
day = reptdate.day

if day == 8: week = 1
elif day == 15: week = 2
elif day == 22: week = 3
elif day == last_day_of_month: week = 4
else: week = 0
```

### For Each GL Account:

1. **Create Default Records**
   - Generate zero-value records for all 6 maturity buckets
   - Both NPL (93/94) and Gross (95/96) versions

2. **Filter WALK File**
   - Extract records matching the GL code/description
   - Match reporting date

3. **Sum Amounts**
   - Aggregate all matching records

4. **Assign to Bucket**
   - Based on product type:
     - Cash, Other Assets → Bucket 01 (shortest maturity)
     - Floor Stocking → Bucket 05 (6-12 months)
     - Hire Purchase, SRR, Long-term loans → Bucket 06 (>1 year)

5. **Merge with Defaults**
   - Overlay actual amounts onto default records
   - Ensures all buckets are represented

### Consolidation Logic

1. **Collect All Outputs**
   - NOTE (loan-based calculations)
   - CALC (maturity profiles)
   - PBIF (if exists)
   - BT (if exists)
   - GLSET (GL accounts)

2. **Concatenate**
   - Combine all records into FINAL dataset

3. **Summarize**
   - Group by BNMCODE
   - Sum amounts
   - Create FINALSUM dataset

## Example Processing

### Floor Stocking
```
WALK file entry:
  DESC: ML34170
  Date: 15/01/24
  Amount: 1,250,000.00

Output:
  93219090100Y: 0.00
  93219090200Y: 0.00
  93219090300Y: 0.00
  93219090400Y: 0.00
  93219090500Y: 1,250,000.00  ← Actual amount
  93219090600Y: 0.00
  95219090100Y: 0.00
  95219090200Y: 0.00
  95219090300Y: 0.00
  95219090400Y: 0.00
  95219090500Y: 1,250,000.00  ← Actual amount
  95219090600Y: 0.00
```

### FCY Cash Holdings
```
WALK file entries:
  DESC: F139610FXNC
  Date: 15/01/24
  Amount: 500,000.00

Output:
  94221000100Y: 500,000.00  ← Bucket 01 (shortest)
  94221000200Y: 0.00
  94221000300Y: 0.00
  94221000400Y: 0.00
  94221000500Y: 0.00
  94221000600Y: 0.00
  96221000100Y: 500,000.00  ← Bucket 01
  96221000200Y: 0.00
  ...
```

## Program Execution Order

This is typically the **final program** in the BNM reporting suite:

```bash
# 1. Process loan data
python bnm_note_conversion.py

# 2. Calculate maturity profiles
python bnm_calc_conversion.py

# 3. Process GL accounts and consolidate (THIS PROGRAM)
python bnm_glset_consolidation.py
```

## Output Files Purpose

### GLSET
- GL-based positions only
- Used for GL reconciliation
- Intermediate file

### FINAL
- All records before summarization
- Detailed audit trail
- Shows contribution from each source

### FINALSUM
- **Primary deliverable**
- One record per BNMCODE
- Ready for BNM submission
- Used for regulatory reporting

## Validation Checklist

- [ ] All GL codes found in WALK file
- [ ] Amounts match GL balances
- [ ] Default records created for all products
- [ ] Bucket assignments correct
- [ ] GLSET totals reconcile to GL
- [ ] FINAL includes all source datasets
- [ ] FINALSUM has no duplicate BNMCODEs
- [ ] Sum of FINALSUM matches sum of FINAL

## WALK File Format

### Example Lines
```
 39110              150124           12,345,678.90
 32110              150124            5,000,000.00
 ML34170            150124            1,250,000.00
 ML34111            150124              987,654.32
 F137010SH          150124              123,456.78
 F13460081BCB       150124              500,000.00
 F13460064FLB       150124              750,000.00
 F134600RC          150124              250,000.00
 F139610FXNC        150124              100,000.00
```

### Position Map
```
Position:  1         11        21   24   27        42
           |         |         |    |    |         |
           " 39110              15 01 24           12,345,678.90"
           └─────────┘          └──┘└──┘└──┘       └────────────┘
             DESC (19 chars)    DD  MM  YY         AMOUNT
```

## Common Issues & Solutions

### Issue: GL code not found in WALK file
**Solution**: Check GL code spelling; ensure WALK file is current

### Issue: Amounts don't match GL system
**Solution**: Verify WALK file extraction date matches reporting date

### Issue: Missing consolidation inputs
**Solution**: Ensure all prerequisite programs have been run successfully

### Issue: Duplicate BNMCODEs in FINALSUM
**Solution**: Check for multiple sources generating same BNMCODE; investigate data quality

### Issue: WALK file parsing errors
**Solution**: Verify fixed-width positions; check for encoding issues

## Key Differences from SAS

1. **MERGE Statement**: Python uses left join with coalesce for defaults
2. **PROC SUMMARY**: Replaced with Polars group_by + agg
3. **Fixed-width INPUT**: Custom Python parsing function
4. **Data concatenation**: Uses Polars concat instead of SET
5. **Sorting**: Only done when required for merging

## Performance Notes

- WALK file parsing is efficient for files <10MB
- For very large WALK files, consider chunked reading
- DuckDB used for reading parquet files efficiently
- Consolidation uses lazy evaluation where possible

## Customization Points

### Adding New GL Accounts
1. Add new processing function (copy template from existing)
2. Update GL code filter
3. Specify appropriate BNMCODE prefix and bucket
4. Add to main() execution flow
5. Include in GLSET concatenation

### Template for New GL Account
```python
def process_new_account(walk_df):
    """Process New Account from WALK file"""
    print("\nProcessing New Account...")
    
    # Create defaults if needed
    default_df = create_default_records('93XXXXXX', '95XXXXXX')
    
    # Filter WALK file
    data = walk_df.filter(pl.col('desc') == 'GL_CODE')
    
    if len(data) == 0:
        return default_df
    
    total_amount = data['amount'].sum()
    
    # Create records for specific bucket
    records = pl.DataFrame([
        {'bnmcode': '93XXXXXXBB00Y', 'amount': total_amount},
        {'bnmcode': '95XXXXXXBB00Y', 'amount': total_amount}
    ])
    
    # Merge with defaults
    result_df = default_df.join(...)
    
    return result_df
```

## Reporting Period Logic

The week number affects which data is used:
- **Week 1** (Day 8): First weekly reporting
- **Week 2** (Day 15): Second weekly reporting
- **Week 3** (Day 22): Third weekly reporting
- **Week 4** (Last day of month): Month-end reporting

Some GL balances may differ by week due to:
- Intra-month transactions
- Accruals
- Timing differences

## Testing

### Quick Test
```bash
python generate_test_data_glset.py
python bnm_glset_consolidation.py
```

### Validation Test
1. Compare GL totals to source system
2. Verify bucket distributions make sense
3. Check FINALSUM totals against individual programs
4. Reconcile to previous period

## Dependencies

This program depends on outputs from:
1. **bnm_note_conversion.py** → note.parquet
2. **bnm_calc_conversion.py** → calc.parquet
3. Other BNM programs (if applicable) → pbif.parquet, bt.parquet

## Future Enhancements

1. Add data quality checks (negative amounts, outliers)
2. Create comparison reports vs prior period
3. Add GL reconciliation report
4. Implement automated WALK file validation
5. Generate submission-ready formatted output
6. Add data lineage tracking

## Support

For questions about:
- **GL codes**: Consult chart of accounts
- **BNMCODE structure**: See BNM reporting guidelines
- **Bucket assignments**: Review regulatory requirements
- **WALK file format**: Check source system documentation

---

This is the final consolidation step in the BNM reporting process. The FINALSUM output is ready for submission to Bank Negara Malaysia.
