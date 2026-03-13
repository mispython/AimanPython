#!/usr/bin/env python3
"""
Program : EIBQRPTS.py
Purpose : JCL job orchestrator for the EIBQRPTS job stream.
          Coordinates sequential execution of dependent Python modules.

Original JCL job:  EIBQRPTS
Job class:         A
Message class:     X

Output destinations defined in original JCL:
  PRINT1 – CLASS=K  The Manager, 14th Floor, Credit Review & Rehabilitation
  PRINT2 – CLASS=R  MS.RUBIAH, 26th Floor, Statistics, Finance Division
  PRINT3 – CLASS=R  MR. LOO KAM TOH, 26th Floor, Treasury Accounting

Commented-out recipients in original JCL (inactive):
  *PRINT8 – MS LIM POH LAN, 21st Floor, Retail Banking-Consumer Bkg
"""

from __future__ import annotations

import importlib
import sys
import traceback
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Dataset mappings (JCL DD statements → local paths)
# Active DD assignments carried into executed steps:
#   DEPOSIT  → input/MNILIMT.parquet   (SAP.PBB.MNILIMT(0))
#   LOAN     → input/MNITB.parquet     (SAP.PBB.MNITB(0))
#   ODEXLIST → output/ODEXLIST.txt     (SAP.PBB.ODEXLIST.COLD, RECFM=FBA LRECL=140)
# ---------------------------------------------------------------------------
DD_DEPOSIT  = INPUT_DIR  / "MNILIMT.parquet"
DD_LOAN     = INPUT_DIR  / "MNITB.parquet"
DD_ODEXLIST = OUTPUT_DIR / "ODEXLIST.txt"

# ---------------------------------------------------------------------------
# Step definitions
# Each entry:  (step_label, module_name, active)
#
# Inactive steps are those commented out (//*) in the original JCL.
# They are listed here for completeness but will not be executed.
#
# Active steps in this job:
#   STEP1  – EIBQODLS  (SYSIN=SAP.BNM.PROGRAM(EIBQODLS))
#
# Commented-out / inactive steps (preserved from JCL comments):
#   EIBQLNRT – BANKING INSTITUTIONS EXPOSURE TO MOVEMENT IN INTEREST RATE RISK
#              STOP EFF FR. JAN 05 (REFER TO E-SMR 2005-39)
#   EIBQIRER – AGEING REPORT ON INTEREST RATE EXPOSURE REPORT
#              STOP EFF FR. JAN 05 (REFER TO E-SMR 2005-39)
#   DMMISR12 – REPORT ON WISE ACCTS BY STATE AND AGE RANGE
#              (commented out in JCL)
# ---------------------------------------------------------------------------
STEPS: list[tuple[str, str, bool]] = [
    # (step_label,   module_name,  active)
    ("EIBQODLS",    "EIBQODLS",   True),   # active: SYSIN=EIBQODLS

    # *SAS609 EXEC ... SYSIN=EIBQLNRT
    # STOP EFF FR. JAN 05 (REFER TO E-SMR 2005-39)
    ("EIBQLNRT",    "EIBQLNRT",   False),

    # *EIFQIRER EXEC ... SYSIN=EIBQIRER
    # AGEING REPORT ON INTEREST RATE EXPOSURE REPORT
    # STOP EFF FR. JAN 05 (REFER TO E-SMR 2005-39)
    ("EIBQIRER",    "EIBQIRER",   False),

    # *DMMISR12 EXEC SAS609 ... SYSIN=DMMISR12
    # REPORT ON WISE ACCTS BY STATE AND AGE RANGE
    ("DMMISR12",    "DMMISR12",   False),
]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_step(module_name: str) -> None:
    """Dynamically import and execute a converted program's main() function."""
    mod = importlib.import_module(module_name)
    if not hasattr(mod, "main"):
        raise AttributeError(f"Module '{module_name}' does not expose a main() function.")
    mod.main()


def main() -> None:
    print("=" * 70)
    print(f"JOB  : EIBQRPTS")
    print(f"CLASS: A   MSGCLASS: X")
    print("=" * 70)

    # DELETE step – equivalent of IEFBR14 to remove/reset ODEXLIST output
    # DD1 DSN=SAP.PBB.ODEXLIST.COLD DISP=(MOD,DELETE,DELETE)
    if DD_ODEXLIST.exists():
        DD_ODEXLIST.unlink()
        print(f"DELETE: removed existing {DD_ODEXLIST}")

    rc_overall = 0

    for step_label, module_name, active in STEPS:
        if not active:
            print(f"\nSTEP {step_label:<12} [SKIPPED – commented out in JCL]")
            continue

        print(f"\nSTEP {step_label:<12} [EXECUTING module: {module_name}]")
        try:
            run_step(module_name)
            print(f"STEP {step_label:<12} RC=0000")
        except Exception:
            print(f"STEP {step_label:<12} ABEND – exception follows:")
            traceback.print_exc()
            rc_overall = 8
            # JCL default: job continues unless COND check stops it
            # No explicit COND= parameters in this JCL, so continue.

    print("\n" + "=" * 70)
    if rc_overall == 0:
        print("JOB EIBQRPTS  ENDED NORMALLY")
    else:
        print(f"JOB EIBQRPTS  ENDED WITH ERRORS  (highest RC={rc_overall})")
    print("=" * 70)

    sys.exit(rc_overall)


if __name__ == "__main__":
    main()
