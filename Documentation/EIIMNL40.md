"BNM Liquidity Loans - Python Conversion"
""Overview""
This is a simplified version of the BNM liquidity framework loan processing program (compared to EIBWLIQ2). It processes loan data directly from the report date without complex run-off date calculations.

""Key Features:""

"""1. Complete Python Implementation (bnm_liquidity_loans.py)""
Processes loan repayment schedules
Calculates maturity profiles by time buckets
Handles performing vs non-performing classification
Excludes FCY (foreign currency) products in output


"""2. Main Differences from EIBWLIQ2:"""
Simpler Date Logic: Uses REPTDATE directly instead of calculating RUNOFFDT
Same Payment Processing: Identical payment schedule logic
FCY Exclusion: Foreign currency products excluded from final output
Integrated EIR: EIR adjustments processed inline with loans


Processing Logic:
Overdrafts (OD):

Immediate classification (bucket 01)
Item 213 (Others) or 219 (product 34240)

Loans (LN):

Calculate payment schedules
Assign time buckets based on remaining months
Non-performing if DAYS > 89 or LOANSTAT ≠ 1
EIR adjustments added to bucket 06

Revolving Credits (RC):

Special dual recording in both 95212 and 93212 codes


Key Calculations:
Remaining Months:

python   remmth = (years × 12) + months + (days / month_days)
Time Buckets:

01: < 0.1 months (< 1 week)
02: 0.1-1 month
03: 1-3 months
04: 3-6 months
05: 6-12 months
06: > 12 months


Output Files:

bnm_note.parquet: Final BNM codes with amounts
fcy_loans_report.txt: FCY records (should be empty)
bnm_note_report.txt: Full BNM NOTE listing



Differences from Full EIBWLIQ2:
FeatureEIBWLIQ2This ProgramRun-off DateCalculated (month-end)Uses REPTDATE directlyDate FilteringChecks against RUNOFFDTChecks against REPTDATEFCY ProductsProcessed but commented outExcluded entirelyEIR ProcessingSeparate step from weekly dataIntegrated in main loopComplexityHigh (run-off methodology)Medium (direct processing)
This simplified version is suitable when run-off date calculations aren't required and processing can be done directly from the report date.
Summary of All 6 Conversions
I've now successfully converted all 6 SAS programs to Python:

✅ EIQPROM2 - Loan reinstatement mailing
✅ BNM Interest Rate - Rate reporting with compounding
✅ EIBWLIQ2 - Full liquidity framework with run-off dates
✅ OD Liquidity - 48-week rolling window methodology
✅ GL Consolidation - Walker file processing and consolidation
✅ BNM Liquidity Loans - Simplified loan liquidity processing
