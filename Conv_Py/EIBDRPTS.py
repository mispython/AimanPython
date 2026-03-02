#!/usr/bin/env python3
"""
Program : EIBDRPTS
Function: Daily EIS/MIS Report Orchestration Job
          Equivalent of JCL job EIBDRPTS (JOB22447)

          Orchestrates the sequential execution of all daily deposit and
            MIS report programs in the order defined by the JCL job steps.
          Handles pre-run cleanup of output files (equivalent to the
            IEFBR14 DELETE step) and invokes each program's main() function.

          JCL Output destinations (PRINT1, PRINT2, PRINT3, PRINT9, PRINT10)
            are mapped to the configured OUTPUT_DIR. File routing/distribution
            is handled by the downstream delivery process.

Job steps (in execution order):
  DELETE   – Pre-run cleanup of prior output files
  EIBDFD02 – Daily FD Movement (Placement/Withdrawal RM100K & above)
  EIBDFD2B – Daily FD Movement (Placement/Withdrawal RM1M & above)
  EIBDDPMV – Daily Savings & ACE Deposits Movements by Range
  EIBDLOAN – Daily Term Loans Summarised by Branch (commented out in JCL)
  DMMISR01 – Number of Accounts & Total Deposits / Savings (via DMMISRX1)
  DMMISR02 – Movement of Savings/Demand Deposits >= RM1M
  DMMISR22 – Movement of Conventional SA >= RM1M
  DMMISR42 – Movement of Conventional CA >= RM1M
  DMMISR52 – Movement of Demand Deposits >= RM1M (Range Summary)
  DMMISR09 – Movement of OD >= RM1M
  DMMISR11 – Movements in ACE Accounts Balance
  DMMISR13 – Movements in Savings (PMMISR13 / PMMISR23)
  DMMISR62 – Movement of Islamic SA >= RM50K   (JCL step: DMMISR12)
  DMMISR32 – Movement of Islamic CA >= RM1M
  DMMIPB03 – Profile on PB Bright Star Savings (Product 208)
  DMMIPB06 – Profile on PB Bright Star Savings (Product 208, variant)
"""

import logging
import os
import sys
import traceback
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR   = os.environ.get("BASE_DIR",   "/data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================================
# LOGGING SETUP  (equivalent to JCL MSGCLASS=X / SYSLOG)
# ============================================================================

logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt  = "%Y-%m-%d %H:%M:%S",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(OUTPUT_DIR, "EIBDRPTS.log"), mode="w"),
    ],
)
log = logging.getLogger("EIBDRPTS")

# ============================================================================
# OUTPUT FILES TO CLEAN UP BEFORE EXECUTION
# (Equivalent to the JCL DELETE / IEFBR14 step – MOD,DELETE,DELETE on each DD)
#
#   SAP.PBB.EIBDFD1M.A  → EIBDFD1MA.txt   (RPTFILE  – Conventional detail)
#   SAP.PBB.EIBDFD1M.B  → EIBDFD1MS.txt   (SUMFILE  – Conventional summary)
#   SAP.PBB.DMMIPB03    → DMMIPB03.txt
#   SAP.PBB.DMMIPB06    → DMMIPB06.txt
#   SAP.PBB.DMMISR01    → DMMISR01.txt
#   SAP.PBB.DMMISR02    → DMMISR02.txt
#   SAP.PBB.DMMISR22    → DMMISR22.txt
#   SAP.PBB.DMMISR42    → DMMISR42.txt
#   SAP.PBB.DMMISR52    → DMMISR52.txt
#   SAP.PBB.DMMISR11    → DMMISR11.txt
#   SAP.PBB.DMMISR13    → PMMISR13.txt
#   SAP.PBB.DMMISR23    → PMMISR23.txt
#   SAP.PBB.EIBDDPMV    → EIBDDPMV.txt
#   SAP.PBB.EIBDFD02    → EIBDFD02.txt
#   SAP.PIBB.EIBDFD1M.A → EIBDFD1MI.txt   (RPTFILI  – Islamic detail)
#   SAP.PIBB.EIIDFD1M.S → EIBDFD1MIS.txt  (ISUMFILE – Islamic summary)
#   SAP.PBB.DMMISR09    → DMMISR09.txt
#   SAP.PBB.DMMISR32    → DMMISR32.txt
#   SAP.PBB.DMMISR12    → DMMISR62.txt    (SAS step DMMISR12 runs DMMISR62)
#   SAP.PBB.DMMISRX1.DMMISFTP → DMMISR01.txt (CRT01 – FTP dataset)
# ============================================================================

DELETE_FILES = [
    "EIBDFD1MA.txt",
    "EIBDFD1MS.txt",
    "DMMIPB03.txt",
    "DMMIPB06.txt",
    "DMMISR01.txt",
    "DMMISR02.txt",
    "DMMISR22.txt",
    "DMMISR42.txt",
    "DMMISR52.txt",
    "DMMISR11.txt",
    "PMMISR13.txt",
    "PMMISR23.txt",
    "EIBDDPMV.txt",
    "EIBDFD02.txt",
    "EIBDFD1MI.txt",
    "EIBDFD1MIS.txt",
    "DMMISR09.txt",
    "DMMISR32.txt",
    "DMMISR62.txt",
    "EIBDRPTS.log",   # re-created fresh each run
]


def delete_prior_outputs() -> None:
    """
    DELETE step – remove prior output files before re-generating.
    Equivalent to JCL IEFBR14 step with DISP=(MOD,DELETE,DELETE).
    """
    log.info("DELETE step – removing prior output files")
    for fname in DELETE_FILES:
        fpath = Path(OUTPUT_DIR) / fname
        if fpath.exists():
            try:
                fpath.unlink()
                log.info("  Deleted: %s", fpath)
            except OSError as exc:
                log.warning("  Could not delete %s: %s", fpath, exc)
        else:
            log.debug("  Not found (skipped): %s", fpath)


# ============================================================================
# JOB STEP RUNNER
# ============================================================================

def run_step(step_name: str, module_name: str, commented_out: bool = False) -> bool:
    """
    Import the named module and call its main() function.
    Returns True on success, False on failure.
    If commented_out=True the step is logged as skipped (mirrors JCL //* lines).
    """
    if commented_out:
        log.info("STEP %-12s  SKIPPED (commented out in JCL)", step_name)
        return True

    log.info("STEP %-12s  STARTING  (module: %s)", step_name, module_name)
    try:
        # Dynamic import – each program is a standalone module
        import importlib
        mod = importlib.import_module(module_name)
        mod.main()
        log.info("STEP %-12s  COMPLETED SUCCESSFULLY", step_name)
        return True
    except Exception:
        log.error("STEP %-12s  FAILED\n%s", step_name, traceback.format_exc())
        return False


# ============================================================================
# MAIN ORCHESTRATION  (mirrors JCL execution order)
# ============================================================================

def main() -> None:
    log.info("=" * 70)
    log.info("JOB  EIBDRPTS  –  Daily EIS/MIS Report Orchestration")
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # DELETE step – pre-run cleanup
    # ------------------------------------------------------------------
    delete_prior_outputs()

    results: dict[str, bool] = {}

    # ------------------------------------------------------------------
    # EIBDFD02 – Daily FD Movement (Placement/Withdrawal RM100K & above)
    # //EIBDFD02  EXEC SAS609
    # ------------------------------------------------------------------
    results["EIBDFD02"]  = run_step("EIBDFD02",  "EIBDFD02")

    # ------------------------------------------------------------------
    # EIBDFD2B – Daily FD Movement (Placement/Withdrawal RM1M & above)
    # //EIBDFD2B  EXEC SAS609
    # ------------------------------------------------------------------
    results["EIBDFD2B"]  = run_step("EIBDFD2B",  "EIBDFD2B")

    # ------------------------------------------------------------------
    # EIBDDPMV – Daily Savings & ACE Deposits Movements by Range
    # //EIBDDPMV  EXEC SAS609
    # ------------------------------------------------------------------
    results["EIBDDPMV"]  = run_step("EIBDDPMV",  "EIBDDPMV")

    # ------------------------------------------------------------------
    # EIBDLOAN – Daily Term Loans Summarised by Branch
    # //*EIBDLOAN  EXEC SAS609   <– commented out in JCL
    # ------------------------------------------------------------------
    results["EIBDLOAN"]  = run_step("EIBDLOAN",  "EIBDLOAN", commented_out=True)

    # ------------------------------------------------------------------
    # DMMISR01 – Number of Accounts & Total Deposits/Savings
    # //DMMISR01 EXEC SAS609  (SYSIN = DMMISRX1)
    # ------------------------------------------------------------------
    results["DMMISR01"]  = run_step("DMMISR01",  "DMMISRX1")

    # ------------------------------------------------------------------
    # DMMISR02 – Movement of Savings/Demand Deposits >= RM1M
    # //DMMISR02 EXEC SAS609
    # ------------------------------------------------------------------
    results["DMMISR02"]  = run_step("DMMISR02",  "DMMISR02")

    # ------------------------------------------------------------------
    # DMMISR22 – Movement of Conventional SA >= RM1M
    # //DMMISR22 EXEC SAS609
    # ------------------------------------------------------------------
    results["DMMISR22"]  = run_step("DMMISR22",  "DMMISR22")

    # ------------------------------------------------------------------
    # DMMISR42 – Movement of Conventional CA >= RM1M
    # //DMMISR42 EXEC SAS609
    # ------------------------------------------------------------------
    results["DMMISR42"]  = run_step("DMMISR42",  "DMMISR42")

    # ------------------------------------------------------------------
    # DMMISR52 – Summary of Demand Deposit Movement by Range
    # //DMMISR52 EXEC SAS609
    # ------------------------------------------------------------------
    results["DMMISR52"]  = run_step("DMMISR52",  "DMMISR52")

    # ------------------------------------------------------------------
    # DMMISR09 – Movement of OD >= RM1M
    # //DMMISR09 EXEC SAS609
    # ------------------------------------------------------------------
    results["DMMISR09"]  = run_step("DMMISR09",  "DMMISR09")

    # ------------------------------------------------------------------
    # DMMISR11 – Movements in ACE Accounts Balance
    # //DMMISR11 EXEC SAS609
    # ------------------------------------------------------------------
    results["DMMISR11"]  = run_step("DMMISR11",  "DMMISR11")

    # ------------------------------------------------------------------
    # DMMISR13 – Movements in Savings (produces PMMISR13 + PMMISR23)
    # //DMMISR13 EXEC SAS609
    # ------------------------------------------------------------------
    results["DMMISR13"]  = run_step("DMMISR13",  "DMMISR13")

    # ------------------------------------------------------------------
    # DMMISR12 (Islamic SA >= RM50K)
    # //DMMISR12 EXEC SAS609  (SYSIN = DMMISR62)
    # The JCL job step is named DMMISR12 but runs program DMMISR62.
    # ------------------------------------------------------------------
    results["DMMISR12"]  = run_step("DMMISR12",  "DMMISR62")

    # ------------------------------------------------------------------
    # DMMISR32 – Movement of Islamic CA >= RM1M
    # //DMMISR32 EXEC SAS609
    # ------------------------------------------------------------------
    results["DMMISR32"]  = run_step("DMMISR32",  "DMMISR32")

    # ------------------------------------------------------------------
    # DMMIPB03 – Profile on PB Bright Star Savings (Product 208)
    # //DMMIPB03 EXEC SAS609
    # ------------------------------------------------------------------
    results["DMMIPB03"]  = run_step("DMMIPB03",  "DMMIPB03")

    # ------------------------------------------------------------------
    # DMMIPB06 – Profile on PB Bright Star Savings (Product 208, variant)
    # //DMMIPB06 EXEC SAS609
    # ------------------------------------------------------------------
    results["DMMIPB06"]  = run_step("DMMIPB06",  "DMMIPB06")

    # ------------------------------------------------------------------
    # Job completion summary
    # ------------------------------------------------------------------
    log.info("=" * 70)
    log.info("JOB EIBDRPTS  –  STEP SUMMARY")
    log.info("=" * 70)

    failed_steps = [step for step, ok in results.items() if not ok]
    for step, ok in results.items():
        status = "OK     " if ok else "FAILED "
        log.info("  %-14s  %s", step, status)

    log.info("=" * 70)
    if failed_steps:
        log.error("JOB EIBDRPTS  ENDED WITH FAILURES: %s", ", ".join(failed_steps))
        sys.exit(1)
    else:
        log.info("JOB EIBDRPTS  ENDED NORMALLY")
        sys.exit(0)


if __name__ == "__main__":
    main()
