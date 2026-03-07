#!/usr/bin/env python3
"""
Program  : EIIWOF06.py
Purpose  : Job orchestrator — replicates the EIIWOF06 JCL job stream.
           Updates NPL hardcode data by executing program EIIMNP07.

           JCL note:
             //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP07),DISP=SHR
             The SYSIN program EIIMNP07 contains all the SAS logic;
             there is no inline SAS in this JCL. EIIMNP07 is imported
             and executed as the sole processing step.

           Library mapping:
             NPL    = SAP.PIBB.NPL.HP.SASDATA.WOFF  (DISP=OLD — write target)
             SASLIST= SYSOUT (report output, routed to printer label LABEL)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import sys
import logging
from pathlib import Path

# ============================================================================
# CHILD PROGRAM IMPORTS
# //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP07) -> import and call EIIMNP07.main()
# ============================================================================
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
# MAIN
# ============================================================================
def main():
    """
    //EIIWOF06 EXEC SAS609 job step.
    Executes EIIMNP07 (//SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP07)).
    COND=(0,LT): step runs unconditionally (only skipped on negative RC,
    which is not possible — effectively always runs).
    """
    logger.info("=" * 60)
    logger.info("EIIWOF06 job started")
    logger.info("=" * 60)

    # //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP07)
    logger.info("Running EIIMNP07...")
    try:
        EIIMNP07.main()
        logger.info("EIIMNP07 completed.")
    except Exception as e:
        logger.error(f"EIIMNP07 failed with exception: {e}")
        sys.exit(8)

    logger.info("EIIWOF06 job completed successfully.")


if __name__ == '__main__':
    main()
