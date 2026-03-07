#!/usr/bin/env python3
"""
Program  : EIIWOF04.py
Purpose  : Job orchestrator — replicates the EIIWOF04 JCL job stream.
           Updates NPL hardcode data by executing program EIIMNP03.

           JCL note:
             //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP03),DISP=SHR
             The SYSIN program EIIMNP03 contains all the SAS logic;
             there is no inline SAS in this JCL. EIIMNP03 is imported
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
# //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP03) -> import and call EIIMNP03.main()
# ============================================================================
import EIIMNP03

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
    //EIIWOF04 EXEC SAS609 job step.
    Executes EIIMNP03 (//SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP03)).
    COND=(0,LT): step runs unconditionally (only skipped on negative RC,
    which is not possible — effectively always runs).
    """
    logger.info("=" * 60)
    logger.info("EIIWOF04 job started")
    logger.info("=" * 60)

    # //SYSIN DD DSN=SAP.BNM.PROGRAM(EIIMNP03)
    logger.info("Running EIIMNP03...")
    try:
        EIIMNP03.main()
        logger.info("EIIMNP03 completed.")
    except Exception as e:
        logger.error(f"EIIMNP03 failed with exception: {e}")
        sys.exit(8)

    logger.info("EIIWOF04 job completed successfully.")


if __name__ == '__main__':
    main()
