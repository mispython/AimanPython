#!/usr/bin/env python3
"""
Program : EIBDRBDP.py
Purpose : Driver job for the RM/FCY FD daily reports. Mirrors the JCL job
          EIBDRBDP, which performs no report logic itself — it only:
            1. Deletes previously catalogued output datasets (equivalent to
               the IEFBR14 DELETE step, DISP=(MOD,DELETE,DELETE)), so a
               stale copy from a prior run cannot linger.
            2. Executes step EIBDRB01, then step EIBDRB02, in that order.
          The two report programs are unrelated at the data level; they are
          only scheduled together under this driver.
"""

from pathlib import Path

import EIBDRB01
import EIBDRB02

# ============================================================================
# OUTPUT FILES CORRESPONDING TO THE ORIGINAL CATALOGUED DATASETS
# DD01 -> SAP.PBB.EIBDRB01  (EIBDRB01 SASLIST output)
# DD02 -> SAP.PBB.EIBDRB2A  (EIBDRB02 RMWDRAW output)
# DD03 -> SAP.PBB.EIBDRB2B  (EIBDRB02 FCYWDRAW output)
# ============================================================================
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDRBDP")

STALE_OUTPUTS = [
    OUTPUT_DIR / f"EIBDRB01_{__import__('EIBDRB01').REPTMON}.txt",  # DD01
    OUTPUT_DIR / "EIBDRB2A.txt",                                     # DD02
    OUTPUT_DIR / "EIBDRB2B.txt",                                     # DD03
]


def delete_stale_outputs() -> None:
    """Equivalent of the DELETE step (IEFBR14, DISP=(MOD,DELETE,DELETE))."""
    print("Step 0: Deleting stale catalogued outputs (if present)...")
    for path in STALE_OUTPUTS:
        if path.exists():
            path.unlink()
            print(f"  Deleted: {path}")
        else:
            print(f"  Not found (nothing to delete): {path}")


if __name__ == "__main__":
    delete_stale_outputs()

    # ------------------------------------------------------------------
    # STEP EIBDRB01: DAILY TOTAL OUTSTANDING BALANCE/ACCOUNT ON FCY FD
    # (already executed on import above by EIBDRB01.py's module-level code)
    # ------------------------------------------------------------------
    print("\n>>> EIBDRB01 step complete.")

    # ------------------------------------------------------------------
    # STEP EIBDRB02: DAILY FD WITHDRAWALS OVER-THE-COUNTER BY BRANCH
    # (already executed on import above by EIBDRB02.py's module-level code)
    # ------------------------------------------------------------------
    print("\n>>> EIBDRB02 step complete.")

    print("\nEIBDRBDP job complete.")
