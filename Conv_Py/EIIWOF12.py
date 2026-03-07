#!/usr/bin/env python3
"""
Program  : EIIWOF12.py
Purpose  : Job orchestrator — replicates the EIIWOF12 JCL job stream.
           Executes EIFMNP02 using daily loan data and NPL HP write-off datasets.

           JCL note:
             //SYSIN DD DSN=SAP.BNM.PROGRAM(EIFMNP02),DISP=SHR
             The SYSIN program EIFMNP02 contains all the SAS logic;
             there is no inline SAS in this JCL. EIFMNP02 is imported
             and executed as the sole processing step.

           Library mapping:
             LOAN  = SAP.PIBB.MNILN.DAILY(0)           (daily loan data, DISP=SHR)
             NPL6  = SAP.PIBB.NPL.HP.SASDATA.WOFF       (DISP=OLD — write target)
             SORTWK01-04 = temporary sort work areas (handled by Python sort natively)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import sys
import logging
from pathlib import Path

# ============================================================================
# CHILD PROGRAM IMPORTS
# //SYSIN DD DSN=SAP.BNM.PROGRAM(EIFMNP02) -> import and call EIFMNP02.main()
# ============================================================================
import EIFMNP02

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
BASE_DIR  = Path(".")
LOAN_DIR  = BASE_DIR / "data" / "pibb" / "loan_daily"   # LOAN: SAP.PIBB.MNILN.DAILY(0)
NPL6_DIR  = BASE_DIR / "data" / "pibb" / "npl_woff"     # NPL6: SAP.PIBB.NPL.HP.SASDATA.WOFF

# SORTWK01-04: temporary sort work areas — not required in Python;
# polars/duckdb sort operations handle memory natively.

# ============================================================================
# MAIN
# ============================================================================
def main():
    """
    //EIIWOF12 EXEC SAS609 job step.
    Executes EIFMNP02 (//SYSIN DD DSN=SAP.BNM.PROGRAM(EIFMNP02)).
    COND=(0,LT): step runs unconditionally (only skipped on negative RC,
    which is not possible — effectively always runs).
    """
    logger.info("=" * 60)
    logger.info("EIIWOF12 job started")
    logger.info("=" * 60)

    # //SYSIN DD DSN=SAP.BNM.PROGRAM(EIFMNP02)
    logger.info("Running EIFMNP02...")
    try:
        EIFMNP02.main()
        logger.info("EIFMNP02 completed.")
    except Exception as e:
        logger.error(f"EIFMNP02 failed with exception: {e}")
        sys.exit(8)

    logger.info("EIIWOF12 job completed successfully.")


if __name__ == '__main__':
    main()
