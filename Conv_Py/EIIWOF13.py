#!/usr/bin/env python3
"""
Program  : EIIWOF13.py
Purpose  : Multi-step job orchestrator — replicates the EIIWOF13 JCL job stream.
           Runs EIIMNP03, EIIMNP06, and EIIMNP07 as three sequential SAS steps,
           each using the same NPL library (SAP.PIBB.NPL.HP.SASDATA.WOFF).

           JCL structure:
             //EIFMNP03 EXEC SAS609 -> //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP03)
             //EIFMNP06 EXEC SAS609 -> //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP06)
             //EIFMNP07 EXEC SAS609 -> //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP07)

           Note: EIIWOF13 consolidates the three programs that EIIWOF04/05/06 run
           individually. EIIWOF13 is the batch-combined version.
           The *SASLIST lines are commented out in the JCL (*SASLIST DD SYSOUT=...),
           meaning report output is suppressed for all three steps.

           Library mapping (all three steps):
             NPL = SAP.PIBB.NPL.HP.SASDATA.WOFF  (DISP=OLD — write target)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import sys
import logging
from pathlib import Path

# ============================================================================
# CHILD PROGRAM IMPORTS
# //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP03) -> import and call EIIMNP03.main()
# //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP06) -> import and call EIIMNP06.main()
# //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP07) -> import and call EIIMNP07.main()
# ============================================================================
import EIIMNP03
import EIIMNP06
import EIIMNP07

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path(".")
NPL_DIR  = BASE_DIR / "data" / "pibb" / "npl_woff"   # NPL: SAP.PIBB.NPL.HP.SASDATA.WOFF

# ============================================================================
# STEP RUNNERS
# Each replicates one //EXEC SAS609 step from the JCL.
# COND=(0,LT): steps run unconditionally — only a negative RC (impossible)
# would skip them; in practice all steps always execute.
# ============================================================================

def run_eiimnp03() -> bool:
    """
    //EIFMNP03 EXEC SAS609
    //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIIMNP03),DISP=SHR
    *SASLIST suppressed (commented out in JCL).
    """
    logger.info("Step 1 — Running EIIMNP03...")
    try:
        EIIMNP03.main()
        logger.info("EIIMNP03 completed.")
        return True
    except Exception as e:
        logger.error(f"EIIMNP03 failed: {e}")
        return False


def run_eiimnp06() -> bool:
    """
    //EIFMNP06 EXEC SAS609
    //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIIMNP06),DISP=SHR
    *SASLIST suppressed (commented out in JCL).
    """
    logger.info("Step 2 — Running EIIMNP06...")
    try:
        EIIMNP06.main()
        logger.info("EIIMNP06 completed.")
        return True
    except Exception as e:
        logger.error(f"EIIMNP06 failed: {e}")
        return False


def run_eiimnp07() -> bool:
    """
    //EIFMNP07 EXEC SAS609
    //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIIMNP07),DISP=SHR
    *SASLIST suppressed (commented out in JCL).
    """
    logger.info("Step 3 — Running EIIMNP07...")
    try:
        EIIMNP07.main()
        logger.info("EIIMNP07 completed.")
        return True
    except Exception as e:
        logger.error(f"EIIMNP07 failed: {e}")
        return False

# ============================================================================
# MAIN
# ============================================================================
def main():
    """
    Orchestrates the full EIIWOF13 JCL job stream.

    Execution order (three independent EXEC SAS609 steps):
      1. EIIMNP03  (//EIFMNP03 EXEC step)
      2. EIIMNP06  (//EIFMNP06 EXEC step)
      3. EIIMNP07  (//EIFMNP07 EXEC step)

    Each step uses the same NPL library. COND=(0,LT) means all steps run
    unconditionally — a negative return code is never produced by SAS.
    Any step failure exits with RC=8 to signal the job as failed.
    """
    logger.info("=" * 60)
    logger.info("EIIWOF13 job started")
    logger.info("=" * 60)

    # Step 1: EIIMNP03
    if not run_eiimnp03():
        logger.error("Step 1 (EIIMNP03) failed — aborting.")
        sys.exit(8)

    # Step 2: EIIMNP06
    if not run_eiimnp06():
        logger.error("Step 2 (EIIMNP06) failed — aborting.")
        sys.exit(8)

    # Step 3: EIIMNP07
    if not run_eiimnp07():
        logger.error("Step 3 (EIIMNP07) failed — aborting.")
        sys.exit(8)

    logger.info("=" * 60)
    logger.info("EIIWOF13 job completed successfully.")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
