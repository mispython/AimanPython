#!/usr/bin/env python3
"""
Program  : EIBDRBDP.py
ESMR     : 2010-2431 (AAB)
Purpose  : Reasons for RM FD & FCY FD Withdrawals Over-the-Counter.
           Orchestration wrapper that replicates the JCL job steps:

           Step 1 – DELETE (IEFBR14):
             Removes any pre-existing output files before the run:
               SAP.PBB.EIBDRB01  -> output/EIBDRB01_report.txt
               SAP.PBB.EIBDRB2A  -> output/RMWDRAW
               SAP.PBB.EIBDRB2B  -> output/FCYWDRAW

           Step 2 – EIBDRB01 (SAS609):
             Daily Total Outstanding Balance/Account on FCY FD.
             Libraries:
               DEPO  -> data/deposit      (SAP.PBB.MNITB.DAILY(0))
               WALK  -> data/walk         (SAP.PBB.DAILY.WALKER(0))
               MISFD -> data/pibb/fcyfd   (SAP.PBB.FCYFD)
               STORE -> data/store        (SAP.PBB.DP.SASDATA)
             Output:
               output/EIBDRB01_report.txt (LRECL=300, RECFM=FB)

           Step 3 – EIBDRB02 (SAS609):
             Daily FD Withdrawals Over-the-Counter by Branch.
             Libraries:
               DEPOSIT -> data/deposit    (SAP.PBB.MNITB.DAILY(0))
               MIS     -> data/mis        (SAP.PBB.DP.SASDATA)
             Outputs:
               output/RMWDRAW   (LRECL=360, RECFM=FB) – RM withdrawals
               output/FCYWDRAW  (LRECL=360, RECFM=FB) – FCY withdrawals

           Dependencies:
             EIBDRB01  - FCY FD outstanding balance/account report
             EIBDRB02  - FD withdrawal reasons report by branch
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import sys
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path(".")
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Output file paths
# SAP.PBB.EIBDRB01  (LRECL=300, RECFM=FB)
OUTPUT_EIBDRB01 = OUTPUT_DIR / "EIBDRB01_report.txt"
# SAP.PBB.EIBDRB2A  (LRECL=360, RECFM=FB) – RM withdrawals
OUTPUT_RMWDRAW  = OUTPUT_DIR / "RMWDRAW"
# SAP.PBB.EIBDRB2B  (LRECL=360, RECFM=FB) – FCY withdrawals
OUTPUT_FCYWDRAW = OUTPUT_DIR / "FCYWDRAW"

# ============================================================================
# DEPENDENCY IMPORTS
# ============================================================================
from EIBDRB01 import main as run_eibdrb01
from EIBDRB02 import main as run_eibdrb02


# ============================================================================
# STEP 1: DELETE pre-existing output files
# //DELETE EXEC PGM=IEFBR14
# //DD01 DD DSN=SAP.PBB.EIBDRB01, DISP=(MOD,DELETE,DELETE)
# //DD02 DD DSN=SAP.PBB.EIBDRB2A, DISP=(MOD,DELETE,DELETE)
# //DD03 DD DSN=SAP.PBB.EIBDRB2B, DISP=(MOD,DELETE,DELETE)
# ============================================================================
def delete_outputs() -> None:
    """
    Remove pre-existing output files before the run, replicating the
    IEFBR14 DELETE step (DISP=(MOD,DELETE,DELETE)).
    """
    for path in (OUTPUT_EIBDRB01, OUTPUT_RMWDRAW, OUTPUT_FCYWDRAW):
        if path.exists():
            path.unlink()
            print(f"Deleted: {path}")
        else:
            print(f"Not found (skipped): {path}")


# ============================================================================
# STEP 2: EIBDRB01
# //EIBDRB01 EXEC SAS609
# //SYSIN DD DSN=SAP.BNM.PROGRAM(EIBDRB01),DISP=SHR
# ============================================================================
def step_eibdrb01() -> None:
    """Execute the EIBDRB01 program (FCY FD outstanding balance report)."""
    print("=" * 60)
    print("STEP EIBDRB01: Daily Total Outstanding Balance/Account on FCY FD")
    print("=" * 60)
    run_eibdrb01()
    print("STEP EIBDRB01 completed.")


# ============================================================================
# STEP 3: EIBDRB02
# //EIBDRB02 EXEC SAS609
# //SYSIN DD DSN=SAP.BNM.PROGRAM(EIBDRB02),DISP=SHR
# ============================================================================
def step_eibdrb02() -> None:
    """Execute the EIBDRB02 program (FD withdrawal reasons report by branch)."""
    print("=" * 60)
    print("STEP EIBDRB02: Daily FD Withdrawals Over-the-Counter by Branch")
    print("=" * 60)
    run_eibdrb02()
    print("STEP EIBDRB02 completed.")


# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    print("EIBDRBDP started.")

    # Step 1: DELETE pre-existing outputs (IEFBR14)
    print("-" * 60)
    print("STEP DELETE: Removing pre-existing output files...")
    delete_outputs()
    print("STEP DELETE completed.")

    # Step 2: EIBDRB01
    step_eibdrb01()

    # Step 3: EIBDRB02
    step_eibdrb02()

    print("=" * 60)
    print("EIBDRBDP completed successfully.")
    print(f"  EIBDRB01 report : {OUTPUT_EIBDRB01}")
    print(f"  RMWDRAW  report : {OUTPUT_RMWDRAW}")
    print(f"  FCYWDRAW report : {OUTPUT_FCYWDRAW}")


if __name__ == "__main__":
    main()
