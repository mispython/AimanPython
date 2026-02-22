# !/usr/bin/env python3
"""
 PROGRAM : EIFMNPL0  (JCL: EIIMNPL0)
 PURPOSE : Job orchestrator — sequences execution of NPL report programs:
             EIFMNP03  Movements of Interest in Suspense
             EIFMNP06  Movements of Specific Provision (Depreciated PP)
             EIFMNP07  Statistics on Asset Quality - Movements in NPL

 JOB     : EIIMNPL0
 NOTIFY  : &SYSUID
 USER    : OPCC

 OUTPUT LABEL (original JCL):
   NAME     : THE MANAGER
   ROOM     : 22TH FLOOR
   BUILDING : MENARA PBB
   DEPT     : HP CREDIT CONTROL DEPARTMENT
   ADDRESS  : MENARA PUBLIC BANK, 146 JALAN AMPANG, 50450 KUALA LUMPUR
   DEST     : S1.LOCAL

 MODIFY  : ESMR 2004-720 & 2004-579
"""

import sys
import logging
import shutil
import traceback
from datetime import datetime
from pathlib import Path

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR    = Path("/data/npl")
INPUT_DIR   = BASE_DIR / "parquet"
REPORT_DIR  = BASE_DIR / "reports"
LOG_DIR     = BASE_DIR / "logs"

# Output text report files  (equivalent to JCL SASLIST DD datasets)
# EIFMNP03  ->  SAP.PIBB.IIS.TEXT  (LRECL=133, RECFM=VB)
IIS_TEXT    = REPORT_DIR / "EIFMNP03_report.txt"

# EIFMNP05  ->  commented out in JCL (SYSOUT only, no persistent dataset)
# SP1_TEXT  = REPORT_DIR / "EIFMNP05_report.txt"   # discontinued per ESMR 2009-1486

# EIFMNP06  ->  SAP.PIBB.SP.TEXT   (LRECL=133, RECFM=VB)
SP_TEXT     = REPORT_DIR / "EIFMNP06_report.txt"

# EIFMNP07  ->  SAP.PIBB.AQ.TEXT   (LRECL=133, RECFM=VB)
AQ_TEXT     = REPORT_DIR / "EIFMNP07_report.txt"

# Intermediate datasets deleted at job start (equivalent to DELETE step)
# SAP.PIBB.IIS.TEXT  -> IIS_TEXT
# SAP.PIBB.SP.TEXT   -> SP_TEXT
# SAP.PIBB.AQ.TEXT   -> AQ_TEXT
DELETE_ON_START = [IIS_TEXT, SP_TEXT, AQ_TEXT]

# =============================================================================
# LOGGING SETUP
# =============================================================================
LOG_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)
INPUT_DIR.mkdir(parents=True, exist_ok=True)

_log_file = LOG_DIR / f"EIIMNPL0_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
# IMPORT STEP MODULES
# Each module exposes a main() entry point that runs the full step end-to-end.
# Import paths assume all four .py files reside in the same directory as this
# orchestrator.  Adjust sys.path if the layout differs.
# =============================================================================
def _import_step(module_name: str):
    """Dynamically import a sibling step module."""
    import importlib
    step_dir = Path(__file__).resolve().parent
    if str(step_dir) not in sys.path:
        sys.path.insert(0, str(step_dir))
    return importlib.import_module(module_name)


# =============================================================================
# DELETE STEP  (equivalent to //DELETE EXEC PGM=IEFBR14)
# Removes prior-run output datasets before re-creating them, mirroring the
# DISP=(MOD,DELETE,DELETE) behaviour of the JCL DELETE step.
# =============================================================================
def step_delete() -> None:
    """
    //DELETE   EXEC PGM=IEFBR14
    //DD01     DD DSN=SAP.PIBB.IIS.TEXT, DISP=(MOD,DELETE,DELETE)
    //DD02     DD DSN=SAP.PIBB.SP.TEXT,  DISP=(MOD,DELETE,DELETE)
    //DD03     DD DSN=SAP.PIBB.AQ.TEXT,  DISP=(MOD,DELETE,DELETE)
    """
    log.info("=== DELETE STEP: removing prior output files ===")
    for path in DELETE_ON_START:
        if path.exists():
            path.unlink()
            log.info("  Deleted: %s", path)
        else:
            log.info("  Not found (skip): %s", path)
    log.info("DELETE STEP complete.")


# =============================================================================
# STEP EIFMNP03
# //EIFMNP03 EXEC SAS609,REGION=6M,WORK='120000,8000'
# //NPL      DD DSN=SAP.PIBB.NPL.HP.SASDATA,DISP=OLD
# //SASLIST  DD DSN=SAP.PIBB.IIS.TEXT,DISP=(NEW,CATLG,DELETE),
# //            DCB=(LRECL=133,RECFM=VB,BLKSIZE=0)
# //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIIMNP03),DISP=SHR
# =============================================================================
def step_eifmnp03() -> None:
    """
    Run EIFMNP03 — Movements of Interest in Suspense.
    Output report written to IIS_TEXT (equivalent to SAP.PIBB.IIS.TEXT).
    """
    log.info("=== STEP EIFMNP03: Movements of Interest in Suspense ===")
    mod = _import_step("EIIMNP03")
    mod.main()
    log.info("STEP EIFMNP03 complete.  Report: %s", IIS_TEXT)


# =============================================================================
# STEP EIFMNP04  -- DISCONTINUED
# //* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS
# //* EIFMNP04 EXEC SAS609,REGION=6M,WORK='120000,8000'
# //* PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
# //* NPL      DD DSN=SAP.PBB.NPL.HP.SASDATA,DISP=OLD
# //* SASLIST  DD SYSOUT=(,),OUTPUT=(*.LABEL)
# //* SYSIN    DD DSN=SAP.BNM.PROGRAM(EIFMNP04),DISP=SHR
# =============================================================================


# =============================================================================
# STEP EIFMNP05  -- DISCONTINUED per ESMR 2009-1486 TSY4
# //* SMR 2009-1486 TSY4.
# //*EIFMNP05 EXEC SAS609,REGION=6M,WORK='120000,8000'
# //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
# //*NPL      DD DSN=SAP.PIBB.NPL.HP.SASDATA,DISP=OLD
# //*SASLIST  DD SYSOUT=(,),OUTPUT=(*.LABEL)
# //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIIMNP05),DISP=SHR
# =============================================================================


# =============================================================================
# STEP EIFMNP06
# //EIFMNP06 EXEC SAS609,REGION=6M,WORK='120000,8000'
# //NPL      DD DSN=SAP.PIBB.NPL.HP.SASDATA,DISP=OLD
# //SASLIST  DD DSN=SAP.PIBB.SP.TEXT,DISP=(NEW,CATLG,DELETE),
# //            DCB=(LRECL=133,RECFM=VB,BLKSIZE=0)
# //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIIMNP06),DISP=SHR
# =============================================================================
def step_eifmnp06() -> None:
    """
    Run EIFMNP06 — Movements of Specific Provision
                   (Depreciated Purchase Price for Unscheduled Goods).
    Output report written to SP_TEXT (equivalent to SAP.PIBB.SP.TEXT).
    Depends on IIS dataset produced by EIFMNP03.
    """
    log.info("=== STEP EIFMNP06: Movements of Specific Provision (SP2) ===")
    mod = _import_step("EIIMNP06")
    mod.main()
    log.info("STEP EIFMNP06 complete.  Report: %s", SP_TEXT)


# =============================================================================
# STEP EIFMNP07
# //EIFMNP07 EXEC SAS609,REGION=6M,WORK='120000,8000'
# //NPL      DD DSN=SAP.PIBB.NPL.HP.SASDATA,DISP=OLD
# //SASLIST  DD DSN=SAP.PIBB.AQ.TEXT,DISP=(NEW,CATLG,DELETE),
# //            DCB=(LRECL=133,RECFM=VB,BLKSIZE=0)
# //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIIMNP07),DISP=SHR
# =============================================================================
def step_eifmnp07() -> None:
    """
    Run EIFMNP07 — Statistics on Asset Quality - Movements in NPL.
    Output report written to AQ_TEXT (equivalent to SAP.PIBB.AQ.TEXT).
    """
    log.info("=== STEP EIFMNP07: Statistics on Asset Quality - Movements in NPL ===")
    mod = _import_step("EIIMNP07")
    mod.main()
    log.info("STEP EIFMNP07 complete.  Report: %s", AQ_TEXT)


# =============================================================================
# JOB CONDITION CHECK  (equivalent to COND=(0,LT) on JOB card)
# The JCL COND=(0,LT) means: skip subsequent step if 0 < return code from
# any prior step, i.e. abort if any step returns non-zero.
# Here we raise an exception on failure, which halts the job.
# =============================================================================
def run_step(step_name: str, step_fn) -> None:
    """
    Execute a single job step.  Any uncaught exception is logged and re-raised,
    which stops the job — equivalent to COND=(0,LT) abort behaviour.
    """
    log.info("--- Starting step: %s ---", step_name)
    start = datetime.now()
    try:
        step_fn()
        elapsed = (datetime.now() - start).total_seconds()
        log.info("--- Step %s finished successfully in %.1fs ---", step_name, elapsed)
    except Exception:
        elapsed = (datetime.now() - start).total_seconds()
        log.error(
            "--- Step %s FAILED after %.1fs ---\n%s",
            step_name, elapsed, traceback.format_exc()
        )
        # COND=(0,LT): a non-zero return code causes remaining steps to be skipped.
        raise SystemExit(8)


# =============================================================================
# MAIN JOB SEQUENCE
# =============================================================================
def main() -> None:
    """
    //EIIMNPL0  JOB MISEIS,EIFMNPL0,COND=(0,LT),CLASS=A,MSGCLASS=X
    Execute all active job steps in order.
    """
    job_start = datetime.now()
    log.info("=" * 72)
    log.info("JOB EIIMNPL0 started at %s", job_start.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 72)

    # //DELETE step — purge prior output datasets before re-creation
    run_step("DELETE",    step_delete)

    # //EIFMNP03 step — IIS report (SAP.PIBB.IIS.TEXT)
    run_step("EIFMNP03",  step_eifmnp03)

    # //EIFMNP04 step — DISCONTINUED AS PER LETTER DATED 26/08/03 FR STATISTICS
    # run_step("EIFMNP04", step_eifmnp04)

    # //EIFMNP05 step — DISCONTINUED per ESMR 2009-1486 TSY4
    # run_step("EIFMNP05", step_eifmnp05)

    # //EIFMNP06 step — SP report (SAP.PIBB.SP.TEXT); must follow EIFMNP03
    run_step("EIFMNP06",  step_eifmnp06)

    # //EIFMNP07 step — AQ report (SAP.PIBB.AQ.TEXT)
    run_step("EIFMNP07",  step_eifmnp07)

    elapsed_total = (datetime.now() - job_start).total_seconds()
    log.info("=" * 72)
    log.info(
        "JOB EIIMNPL0 completed successfully in %.1fs.  Log: %s",
        elapsed_total, _log_file
    )
    log.info("Output reports:")
    log.info("  IIS  : %s", IIS_TEXT)
    log.info("  SP   : %s", SP_TEXT)
    log.info("  AQ   : %s", AQ_TEXT)
    log.info("=" * 72)


if __name__ == "__main__":
    main()
