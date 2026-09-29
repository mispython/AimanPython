#!/usr/bin/env python3
"""
Program : EIBMRBDP.py
Purpose : Monthly branch deposit and branch performance report for BEC paper preparation, plus reasons
          for RM FD and FCY FD over-the-counter withdrawals (JCL job EIBMRBDP, ESMR 2010-1642 / 2010-2431).

The JCL is a driver only, so this file holds no report logic. Each EXEC step is a separate program,
run in JCL order by importing its module (module-level execution, like %INC PGM(...)).

    DELETE   (IEFBR14)  -> remove the previous output files (SAP.PBB.EIBMRB01 ... EIBMRB09)
    EIBMRB01 -> EIBMRB01.txt  Daily total outstanding balance/account on FCY FD
    EIBMRB02 -> EIBMRB02.txt  Monthly SA/CA/FD account outstanding amount
    EIBMRB03 -> EIBMRB03.txt  Listing of new CA opened for the month
    EIBMRB04 -> EIBMRB04.txt  Listing of new FD opened for the month
    EIBMRB05 -> EIBMRB05.txt  Month-end report by branch for FCY FD and FCY CA
    EIBMRB06 -> EIBMRB06.txt  Listing of new FCY FD opened for the month
    EIBMRB07 -> EIBMRB07.txt  Listing of new FCY CA opened for the month
    EIBMRB08 -> EIBMRB8A.txt / EIBMRB8B.txt  Monthly FD receipts withdrawals by account balance (RM / FCY)
    EIBMRB09 -> EIBMRB09.txt  Monthly FD receipts withdrawals by product types

Every input/output path is declared inside the respective program file.
"""

from pathlib import Path

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
OUTPUT_DIR = BASE_DIR / "output" / "EIBMRBDP"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# STEP DELETE (IEFBR14): DD01-DD10 delete the output datasets
# ============================================================================
OUTPUT_FILES = [
    "EIBMRB01.txt", "EIBMRB02.txt", "EIBMRB03.txt", "EIBMRB04.txt", "EIBMRB05.txt",
    "EIBMRB06.txt", "EIBMRB07.txt", "EIBMRB8A.txt", "EIBMRB8B.txt", "EIBMRB09.txt",
]

print("Step DELETE: removing previous output files...")
for name in OUTPUT_FILES:
    (OUTPUT_DIR / name).unlink(missing_ok=True)

# ============================================================================
# REPORT STEPS (run in JCL order; each import executes the program)
# ============================================================================
print("\nStep EIBMRB01: Daily total outstanding balance/account on FCY FD")
import EIBMRB01  # noqa: E402,F401

print("\nStep EIBMRB02: Monthly SA/CA/FD account outstanding amount")
import EIBMRB02  # noqa: E402,F401

print("\nStep EIBMRB03: Listing of new CA opened for the month")
import EIBMRB03  # noqa: E402,F401

print("\nStep EIBMRB04: Listing of new FD opened for the month")
import EIBMRB04  # noqa: E402,F401

print("\nStep EIBMRB05: Month-end report by branch for FCY FD and FCY CA")
import EIBMRB05  # noqa: E402,F401

print("\nStep EIBMRB06: Listing of new FCY FD opened for the month")
import EIBMRB06  # noqa: E402,F401

print("\nStep EIBMRB07: Listing of new FCY CA opened for the month")
import EIBMRB07  # noqa: E402,F401

print("\nStep EIBMRB08: Monthly FD receipts withdrawals by account balance")
import EIBMRB08  # noqa: E402,F401

print("\nStep EIBMRB09: Monthly FD receipts withdrawals by product types")
import EIBMRB09  # noqa: E402,F401

print("\nEIBMRBDP complete.")
