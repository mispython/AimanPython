# !/usr/bin/env python3
"""
Program: EIFMNPLP - NPL Report Generation Job
Job: JOB02042
Purpose: Main orchestration program that calls all NPL report generation programs
         in sequence, mirroring the original JCL structure
"""

import sys
from datetime import datetime
from pathlib import Path

# Import all subordinate programs
import LNCCD006
import LNCCDN10
import LNCCDR10
import LNCCDQ10
import LNCCDW10
import EIFMNPP1
import EIFMNPP2
import EIFMNPP3
import EIFMNPP4
import EIFMNPP5
import EIFMNPP6
import EIFMNPP7

# ============================================================================
# CONFIGURATION
# ============================================================================

# Input paths
INPUT_PATH_REPTDATE = "SAP_PBB_MNILN_0_REPTDATE.parquet"

# Output file paths
OUTPUT_PATH_IIS_RPS = "SAP_PBB_NPL_IIS_RPS.txt"
OUTPUT_PATH_SP1_RPS = "SAP_PBB_NPL_SP1_RPS.txt"
OUTPUT_PATH_SP2_RPS = "SAP_PBB_NPL_SP2_RPS.txt"
OUTPUT_PATH_ARREARS_RPS = "SAP_PBB_NPL_ARREARS_RPS.txt"
OUTPUT_PATH_WOFFHP_RPS = "SAP_PBB_NPL_WOFFHP_RPS.txt"
OUTPUT_PATH_HP_RPS = "SAP_PBB_NPL_HP_RPS.txt"

# CAC format outputs
OUTPUT_PATH_IIS_CAC = "SAP_PBB_NPL_IIS_CAC.txt"
OUTPUT_PATH_SP1_CAC = "SAP_PBB_NPL_SP1_CAC.txt"
OUTPUT_PATH_SP2_CAC = "SAP_PBB_NPL_SP2_CAC.txt"
OUTPUT_PATH_ARREARS_CAC = "SAP_PBB_NPL_ARREARS_CAC.txt"
OUTPUT_PATH_WOFFHP_CAC = "SAP_PBB_NPL_WOFFHP_CAC.txt"
OUTPUT_PATH_HP_CAC = "SAP_PBB_NPL_HP_CAC.txt"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_report_date_info():
    """
    Process REPTDATE to generate macro variables.
    Equivalent to the DATA REPTDATE step in original SAS.
    """
    import polars as pl

    reptdate_df = pl.read_parquet(INPUT_PATH_REPTDATE)
    reptdate_row = reptdate_df.row(0, named=True)
    reptdate = reptdate_row['REPTDATE']

    day = reptdate.day

    # Determine week (SELECT(DAY(REPTDATE)))
    if day == 8:
        wk = '1'
    elif day == 15:
        wk = '2'
    elif day == 22:
        wk = '3'
    else:
        wk = '4'

    # Calculate previous month for LNCCDQ10
    if reptdate.month == 1:
        pmth = 12
        pyear = reptdate.year - 1
    else:
        pmth = reptdate.month - 1
        pyear = reptdate.year

    from datetime import datetime
    preptdte = datetime(pyear, pmth, 1)

    # Format dates
    # CALL SYMPUT('RDATE', PUT(REPTDATE, DDMMYY8.))
    rdate = f"{reptdate.day:02d}/{reptdate.month:02d}/{str(reptdate.year)[2:]}"

    return {
        'NOWK': wk,
        'RDATE': rdate,
        'REPTYEAR': f"{reptdate.year:04d}",
        'REPTMON': f"{reptdate.month:02d}",
        'REPTDAY': f"{reptdate.day:02d}",
        'REPTDATE': reptdate,
        'PREPTDTE': preptdte
    }


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function that orchestrates all steps.
    This mirrors the original JCL job structure.
    """
    print("\n" + "=" * 80)
    print("EIFMNPLP - NPL REPORT GENERATION JOB")
    print("JOB02042")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        # ====================================================================
        # Get report date information
        # ====================================================================
        print("Initializing report parameters...")
        macro_vars = get_report_date_info()
        rdate = macro_vars['RDATE']
        preptdte = macro_vars['PREPTDTE']
        print(f"Report Date: {rdate}")
        print(f"Report Week: {macro_vars['NOWK']}")
        print()

        # ====================================================================
        # STEP 1: LNCCD006 - Generate base LOANTEMP dataset
        # ====================================================================
        # /* %INC PGM(LNCCD006);
        # PROC DATASETS LIB=WORK KILL NOLIST; RUN; */

        # NOTE: LNCCD006 is commented out in original, but we include it
        # as it generates the LOANTEMP dataset needed by subsequent programs
        print("=" * 80)
        print("STEP 1: LNCCD006 - Generate LOANTEMP Dataset")
        print("=" * 80)
        LNCCD006.process_lnccd006(rdate)
        print()

        # ====================================================================
        # STEP 2: LNCCDN10 - Loans in NPL Report by CAC
        # ====================================================================
        # /**  LOANS IN NPL REPORT - BY CAC **/
        # %INC PGM(LNCCDN10);
        # PROC DATASETS LIB=WORK KILL NOLIST; RUN;

        print("=" * 80)
        print("STEP 2: LNCCDN10 - Loans in NPL Report by CAC")
        print("=" * 80)
        LNCCDN10.process_lnccdn10(rdate)
        print()

        # ====================================================================
        # STEP 3: LNCCDR10 - Monthly Detail Listing for NPL Accounts
        # ====================================================================
        # /**  MONTHLY DETAIL LISTING FOR NPL ACCOUNTS      **/
        # /* %INC PGM(LNCCDR10);
        # PROC DATASETS LIB=WORK KILL NOLIST; RUN;  */

        # NOTE: This step is commented out in the original
        print("=" * 80)
        print("STEP 3: LNCCDR10 - (Commented out in original)")
        print("=" * 80)
        LNCCDR10.process_lnccdr10(rdate)
        print()

        # ====================================================================
        # STEP 4: LNCCDQ10 - Monthly Detail Listing for NPL 3 months & above
        # ====================================================================
        # /* MTHLY DETAIL LISTING FOR NPL 3 MONTHS & ABOVE FOR ALL ACCT */
        # %INC PGM(LNCCDQ10);
        # PROC DATASETS LIB=WORK KILL NOLIST; RUN;

        print("=" * 80)
        print("STEP 4: LNCCDQ10 - NPL 3 Months & Above")
        print("=" * 80)
        LNCCDQ10.process_lnccdq10(rdate, preptdte)
        print()

        # ====================================================================
        # STEP 5: LNCCDW10 - Monthly Detail Listing for Written Off HP
        # ====================================================================
        # /* MTHLY DETAIL LISTING FOR WRITTEN OFF HP 983 & 993 ACCT */
        # %INC PGM(LNCCDW10);
        # PROC DATASETS LIB=WORK KILL NOLIST; RUN;

        print("=" * 80)
        print("STEP 5: LNCCDW10 - Written Off HP Accounts")
        print("=" * 80)
        LNCCDW10.process_lnccdw10(rdate)
        print()

        # ====================================================================
        # STEP 6: EIFMNPP1 - Print IIS, SP1, SP2 into RPS format
        # ====================================================================
        # //*********************************************************************
        # //*  PROGRAM TO PRINT THE IIS, SP1, SP2 INTO A TEXT FILE FOR RPS USE
        # //*  (SMR A264)
        # //*********************************************************************

        print("=" * 80)
        print("STEP 6: EIFMNPP1 - Generate IIS, SP1, SP2 Reports")
        print("=" * 80)
        EIFMNPP1.process_eifmnpp1(rdate)
        print()

        # ====================================================================
        # STEPS 7-12: EIFMNPP2-7 - Convert RPS to CAC format
        # ====================================================================
        # //**********************************************************************
        # //*  TO PRINT IIS/SP1/SP2/ARREARS/WOFFHP/HP INTO RPS FORMAT
        # //**********************************************************************

        print("=" * 80)
        print("STEPS 7-12: Converting RPS to CAC Format")
        print("=" * 80)

        # STEP 7: EIFMNPP2 - Convert IIS to CAC
        print("  STEP 7: EIFMNPP2 - IIS to CAC...")
        EIFMNPP2.process_eifmnpp2(OUTPUT_PATH_IIS_RPS, OUTPUT_PATH_IIS_CAC)

        # STEP 8: EIFMNPP3 - Convert SP1 to CAC
        print("  STEP 8: EIFMNPP3 - SP1 to CAC...")
        EIFMNPP3.process_eifmnpp3(OUTPUT_PATH_SP1_RPS, OUTPUT_PATH_SP1_CAC)

        # STEP 9: EIFMNPP4 - Convert SP2 to CAC
        print("  STEP 9: EIFMNPP4 - SP2 to CAC...")
        EIFMNPP4.process_eifmnpp4(OUTPUT_PATH_SP2_RPS, OUTPUT_PATH_SP2_CAC)

        # STEP 10: EIFMNPP5 - Convert ARREARS to CAC
        print("  STEP 10: EIFMNPP5 - ARREARS to CAC...")
        EIFMNPP5.process_eifmnpp5(OUTPUT_PATH_ARREARS_RPS, OUTPUT_PATH_ARREARS_CAC)

        # STEP 11: EIFMNPP6 - Convert WOFFHP to CAC
        print("  STEP 11: EIFMNPP6 - WOFFHP to CAC...")
        EIFMNPP6.process_eifmnpp6(OUTPUT_PATH_WOFFHP_RPS, OUTPUT_PATH_WOFFHP_CAC)

        # STEP 12: EIFMNPP7 - Convert HP to CAC
        print("  STEP 12: EIFMNPP7 - HP to CAC...")
        EIFMNPP7.process_eifmnpp7(OUTPUT_PATH_HP_RPS, OUTPUT_PATH_HP_CAC)

        print()

        # ====================================================================
        # Job completion
        # ====================================================================
        print("=" * 80)
        print("JOB COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        print("Final Output Files Generated:")
        print(f"  RPS Format:")
        print(f"    - {OUTPUT_PATH_ARREARS_RPS}")
        print(f"    - {OUTPUT_PATH_WOFFHP_RPS}")
        print(f"    - {OUTPUT_PATH_HP_RPS}")
        print(f"    - {OUTPUT_PATH_IIS_RPS}")
        print(f"    - {OUTPUT_PATH_SP1_RPS}")
        print(f"    - {OUTPUT_PATH_SP2_RPS}")
        print(f"  CAC Format:")
        print(f"    - {OUTPUT_PATH_IIS_CAC}")
        print(f"    - {OUTPUT_PATH_SP1_CAC}")
        print(f"    - {OUTPUT_PATH_SP2_CAC}")
        print(f"    - {OUTPUT_PATH_ARREARS_CAC}")
        print(f"    - {OUTPUT_PATH_WOFFHP_CAC}")
        print(f"    - {OUTPUT_PATH_HP_CAC}")
        print()

    except Exception as e:
        print("\n" + "=" * 80)
        print("JOB FAILED")
        print("=" * 80)
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
