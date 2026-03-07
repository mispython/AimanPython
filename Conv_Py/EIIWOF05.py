#!/usr/bin/env python3
"""
Program  : EIIWOF05.py
Purpose  : Job orchestrator — replicates the EIIWOF05 JCL job stream.
           Updates NPL hardcode data by executing program EIIMNP06.

           JCL note:
             //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP06),DISP=SHR
             The SYSIN program EIIMNP06 contains all the SAS logic;
             there is no inline SAS in this JCL. EIIMNP06 is imported
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
# //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP06) -> import and call EIIMNP06.main()
# ============================================================================
import EIIMNP06

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
    //EIIWOF05 EXEC SAS609 job step.
    Executes EIIMNP06 (//SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP06)).
    COND=(0,LT): step runs unconditionally (only skipped on negative RC,
    which is not possible — effectively always runs).
    """
    logger.info("=" * 60)
    logger.info("EIIWOF05 job started")
    logger.info("=" * 60)

    # //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP06)
    logger.info("Running EIIMNP06...")
    try:
        EIIMNP06.main()
        logger.info("EIIMNP06 completed.")
    except Exception as e:
        logger.error(f"EIIMNP06 failed with exception: {e}")
        sys.exit(8)

    logger.info("EIIWOF05 job completed successfully.")


if __name__ == '__main__':
    main()
