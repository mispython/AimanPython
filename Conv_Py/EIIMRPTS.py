#!/usr/bin/env python3
"""
Program : EIIMRPTS.py
Purpose : JCL driver -- EIIMRPTS job.
          Orchestrates deletion of prior output datasets, (re)allocation of
          fresh output datasets, and sequential execution of the following
          batch report/reconciliation programs, in the same order as the
          original MVS JCL EXEC steps:

              1. EIIMRM01  - Deposits, By Time To Maturity For ALCO (Islamic)
              2. EIIMRM02  - FD by Individual/Non-Individual, Time To Maturity - Part 1
              3. EIIMRM03  - FD by Individual/Non-Individual, Time To Maturity - Part 2
              4. EIIMRM04  - Loans & Advances, By Time To Maturity For ALCO
              5. EIIWSTAF  - Weekly listing for staff new/paid loan
              6. EIIMLN03  - Weighted Average Lending Rate on Loan (RDIR II)
              7. EIFMLN03  - Weighted Average Lending Rate on HPD (RDIR II)

JCL DELETE step (PGM=IEFBR14) physically deletes and recatalogs 8 output
datasets before the CREATE step reallocates them fresh (DISP=(MOD,DELETE,
DELETE) followed by DISP=(NEW,CATLG,DELETE)). This is replicated below by
removing each corresponding output file up front, independent of each
program's own OUTPUT_FILE constant, so this driver can perform cleanup
BEFORE any program module is imported/executed.

JOB-level COND=(4,LT): a step is bypassed if the return code of ANY
previously-executed step is greater than 4. Since the converted Python
programs raise exceptions on failure rather than returning MVS-style
condition codes, this is emulated here: a step failure sets RC=8 and halts
the remaining chain; success keeps RC=0 and the chain continues.

TYPRUN=SCAN on the JOB card means the original JCL was submitted for a
syntax scan only (no steps actually executed) -- preserved here only as a
comment, since a Python driver script has no equivalent "scan-only"
submission mode.

DD statements PRINT1 / PRINT9 (SYSOUT class R, with room/building/dept
distribution info for physical report routing) and the commented-out
PRINTS step (IEBGENER copy of SAP.PIBB.EIBWSTAF to a second print
destination via *.PRINT9) configure mainframe print/output-class routing,
not data transformation -- they have no Python equivalent and are omitted
here (kept as documentation only).

//SASLIST DD DSN=SAP.PIBB.EIIWSTAF(+1) under EIIWSTAF is a GDG "+1" (new
generation) allocation; generation handling is the responsibility of
EIIWSTAF.py itself and is not re-implemented in this driver.
"""

import sys
from pathlib import Path

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# ============================================================================
# JCL DELETE STEP (PGM=IEFBR14) equivalent
# DD1-DD8 -> SAP.PIBB.EIIMRM01.TEXT / EIIMRM02.TEXT / EIIMRM03.TEXT /
#            EIIMRM04.TEXT / EIBWSTAF / EIIMLN03 / M4LOAN / EIFMLN03
# Paths below mirror each program's own OUTPUT_FILE definition but are kept
# independent (not imported) so cleanup can happen before any program
# module is loaded.
# ============================================================================
_DELETE_TARGETS = [
    BASE_DIR / "output" / "EIIMRPTS" / "EIIMRM01.txt",   # DD1 / CRT01
    BASE_DIR / "output" / "EIIMRPTS" / "EIIMRM02.txt",   # DD2 / CRT02
    BASE_DIR / "output" / "EIIMRPTS" / "EIIMRM03.txt",   # DD3 / CRT03
    BASE_DIR / "output" / "EIIMRPTS" / "EIIMRM04.txt",   # DD4 / CRT04
    BASE_DIR / "output" / "EIIMRPTS" / "EIIWSTAF.txt",   # DD5 / CRT05 (GDG +1)
    BASE_DIR / "output" / "EIIMRPTS" / "EIIMLN03.txt",   # DD6 / CRT06
    BASE_DIR / "output" / "EIIMRPTS" / "M4LOAN.txt",     # DD7 / CRT07
    BASE_DIR / "output" / "EIIMRPTS" / "EIFMLN03.txt",   # DD8 / CRT08
]


def _delete_step() -> None:
    print("Step DELETE: removing prior output datasets (IEFBR14 equivalent)...")
    for target in _DELETE_TARGETS:
        if target.exists():
            target.unlink()
            print(f"  Deleted   : {target}")
        else:
            print(f"  Not present (skip): {target}")


# ============================================================================
# JCL CREATE STEP (PGM=IEFBR14) equivalent
# CRT01-04: RECFM=FB LRECL=256 BLKSIZE=25600
# CRT05   : RECFM=FB LRECL=133 BLKSIZE=0
# CRT06   : RECFM=FB LRECL=133 BLKSIZE=0
# CRT07   : RECFM=FB LRECL=50  BLKSIZE=0
# CRT08   : RECFM=FB LRECL=133 BLKSIZE=0
# DCB attributes govern physical record layout on MVS and have no direct
# Python equivalent; each downstream program's own report-writer already
# honours the equivalent fixed-width layout. This step therefore only
# guarantees the output directories exist ahead of time.
# ============================================================================
_CREATE_DIRS = [
    BASE_DIR / "output" / "EIIMRPTS",
]


def _create_step() -> None:
    print("Step CREATE: allocating output directories (IEFBR14 equivalent)...")
    for d in _CREATE_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        print(f"  Ensured   : {d}")


# ============================================================================
# STEP EXECUTION CHAIN
# Each JCL EXEC step is run as a module import (module-level execution on
# import), per project convention for JCL driver jobs.
# ============================================================================
_STEPS = [
    ("EIIMRM01", "Deposits, By Time To Maturity For ALCO (Islamic book)"),
    ("EIIMRM02", "FD - Individual/Non-Individual, Time To Maturity - Part 1"),
    ("EIIMRM03", "FD - Individual/Non-Individual, Time To Maturity - Part 2"),
    ("EIIMRM04", "Loans & Advances, By Time To Maturity For ALCO"),
    ("EIIWSTAF", "Weekly listing for staff new/paid loan"),
    ("EIIMLN03", "Weighted Average Lending Rate on Loan (RDIR II)"),
    ("EIFMLN03", "Weighted Average Lending Rate on HPD (RDIR II)"),
]


def _run_steps() -> int:
    """COND=(4,LT) emulation: once RC exceeds 4, all remaining steps are
    bypassed rather than executed, mirroring the JOB-card condition test
    applied against every prior step's return code."""
    rc = 0
    for step_name, purpose in _STEPS:
        if rc > 4:
            print(f"Step {step_name} SKIPPED (COND=(4,LT): prior RC={rc} > 4).")
            continue
        print(f"\n=== EXEC {step_name} : {purpose} ===")
        try:
            __import__(step_name)
            print(f"=== {step_name} completed (RC=0) ===")
        except Exception as exc:
            rc = 8
            print(f"=== {step_name} FAILED (RC=8): {exc} ===", file=sys.stderr)
    return rc


def main() -> int:
    # TYPRUN=SCAN on the original JOB card meant syntax-scan only, no steps
    # actually executed -- documentation-only note; this driver always runs.
    _delete_step()
    _create_step()
    rc = _run_steps()
    print(f"\nEIIMRPTS job complete. Final RC={rc}")
    return 0 if rc == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
