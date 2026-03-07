#!/usr/bin/env python3
"""
Program  : EIBWLIQP.py
Purpose  : Job orchestrator — replicates the EIBWLIQP JCL job stream.

           Invokes the following programs in sequence:

           Step 1 — EIBWLIQ1 (SAS step):
             Initialises BNM.REPTDATE from DEPOSIT.REPTDATE,
             then sequentially runs:
               - DALWPBBD  : PBB deposit manipulation (savings/current accounts)
               - EIBWLIQ1  : New Liquidity Framework (behavioural + contractual, full)
               - EIBWLIQ2  : New Liquidity Framework (contractual run-offs, end-of-month)
               - EIBWLQP2  : New Liquidity Framework Part 2-RM loans (contractual)

           Step 2 — EIBFODFR (separate SAS step, independent library context):
               - EIBFODFR  : OD Balance by Individuals and Corporates for
                             Liquidity Framework

           JCL notes replicated:
             //DELETE step clears SAP.PBB.ODFR.REPT.WEEK before EIBFODFR runs.
             SDESC is set here as 'PUBLIC BANK BERHAD' and passed to child programs.
             EIBFODFR runs with its own separate library context (BNM = SAP.PBB.SASDATA,
             DEPOSIT = SAP.PBB.MNITB) independent of the first step.
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import sys
import logging
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb

# ============================================================================
# CHILD PROGRAM IMPORTS
# %INC PGM(DALWPBBD)  -> import and call DALWPBBD.main()
# %INC PGM(EIBWLIQ1)  -> import and call EIBWLIQ1.main()
# %INC PGM(EIBWLIQ2)  -> import and call EIBWLIQ2.main()
# %INC PGM(EIBWLQP2)  -> import and call EIBWLQP2.main()
# %INC PGM(EIBFODFR)  -> import and call EIBFODFR (module-level execution)
# ============================================================================
import DALWPBBD
import EIBWLIQ1
import EIBWLIQ2
import EIBWLQP2
import EIBFODFR

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
# Step 1 — EIBWLIQ1 job step library paths
BASE_DIR     = Path(".")
DEPOSIT_DIR  = BASE_DIR / "data" / "deposit"   # DEPOSIT libref: SAP.PBB.MNITB(0)
BNM_DIR      = BASE_DIR / "data" / "bnm"       # BNM libref: SAP.TEMP.TEMP (shared work area)
BNM1_DIR     = BASE_DIR / "data" / "bnm1"      # BNM1 libref: SAP.PBB.SASDATA
BNMK_DIR     = BASE_DIR / "data" / "bnmk"      # BNMK libref: SAP.PBB.KAPITI.SASDATA
FD_DIR       = BASE_DIR / "data" / "fd"        # FD libref: SAP.PBB.MNIFD(0)
LOAN_DIR     = BASE_DIR / "data" / "loan"      # LOAN libref: SAP.PBB.MNILN(0)
OUTPUT_DIR   = BASE_DIR / "output"

# Step 2 — EIBFODFR job step library paths (separate JCL step — independent context)
# //BNM     DD DSN=SAP.PBB.SASDATA  (different from Step 1 BNM = SAP.TEMP.TEMP)
# //DEPOSIT DD DSN=SAP.PBB.MNITB(0) (same physical dataset, separate step allocation)
EIBFODFR_BNM_DIR     = BASE_DIR / "data" / "bnm"      # SAP.PBB.SASDATA
EIBFODFR_DEPOSIT_DIR = BASE_DIR / "data" / "deposit"  # SAP.PBB.MNITB(0)
EIBFODFR_OUTPUT_DIR  = OUTPUT_DIR

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# SDESC — institution description set in JCL orchestrator
# SDESC='PUBLIC BANK BERHAD' is assigned here and passed to child programs.
# In SAS: CALL SYMPUT('SDESC',PUT(SDESC,$26.))
# ============================================================================
SDESC = 'PUBLIC BANK BERHAD'

# ============================================================================
# HELPER: initialise BNM.REPTDATE from DEPOSIT.REPTDATE
# Replicates:
#   DATA BNM.REPTDATE;
#      SET DEPOSIT.REPTDATE;
#      SELECT(DAY(REPTDATE)); ... END;
#      SDESC='PUBLIC BANK BERHAD';
#      CALL SYMPUT('REPTYEAR',...);
#      CALL SYMPUT('REPTMON',...);
#      ...
# This writes a reptdate.parquet to BNM_DIR so all child programs can read it.
# ============================================================================
def initialise_reptdate() -> dict:
    """
    DATA BNM.REPTDATE; SET DEPOSIT.REPTDATE;
    Copies REPTDATE from DEPOSIT library to BNM library, adds SDESC,
    and derives macro variables NOWK, REPTYEAR, REPTMON, REPTDAY, RDATE, SDESC.

    Returns a dict of the derived macro variables.
    """
    deposit_reptdate_file = DEPOSIT_DIR / "reptdate.parquet"
    bnm_reptdate_file     = BNM_DIR / "reptdate.parquet"

    BNM_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    df  = con.execute(
        f"SELECT * FROM read_parquet('{deposit_reptdate_file}') LIMIT 1"
    ).pl()

    # Read REPTDATE value
    raw = df["REPTDATE"][0]
    if isinstance(raw, (date, datetime)):
        reptdate = raw.date() if isinstance(raw, datetime) else raw
    else:
        reptdate = date.fromisoformat(str(raw)[:10])

    # SELECT(DAY(REPTDATE)): derive NOWK
    day = reptdate.day
    if   day == 8:  nowk = '1'
    elif day == 15: nowk = '2'
    elif day == 22: nowk = '3'
    else:           nowk = '4'

    # Derive macro variables
    # SDESC='PUBLIC BANK BERHAD'
    reptyear = str(reptdate.year)
    reptmon  = str(reptdate.month).zfill(2)
    reptday  = str(reptdate.day).zfill(2)
    rdate    = reptdate.strftime('%d/%m/%y')   # DDMMYY8. approximation
    sdesc    = SDESC                            # 'PUBLIC BANK BERHAD'

    # Write BNM.REPTDATE (so child programs reading BNM/reptdate.parquet work correctly)
    import polars as pl
    bnm_df = df.with_columns(
        pl.lit(sdesc).alias("SDESC")
    )
    bnm_df.write_parquet(str(bnm_reptdate_file))
    logger.info(f"BNM.REPTDATE written to {bnm_reptdate_file}")

    macros = {
        'reptdate': reptdate,
        'nowk':     nowk,
        'reptyear': reptyear,
        'reptmon':  reptmon,
        'reptday':  reptday,
        'rdate':    rdate,
        'sdesc':    sdesc,
    }
    logger.info(
        f"Derived macros: REPTMON={reptmon}, NOWK={nowk}, "
        f"REPTYEAR={reptyear}, RDATE={rdate}, SDESC={sdesc}"
    )
    return macros

# ============================================================================
# STEP 1 — EIBWLIQ1 job step
# Runs: DALWPBBD -> EIBWLIQ1 -> EIBWLIQ2 -> EIBWLQP2
# All share the same BNM.REPTDATE initialised above.
# ============================================================================
def run_step1(macros: dict) -> bool:
    """
    //EIBWLIQ1 EXEC SAS609 job step.
    Sequentially executes %INC PGM(DALWPBBD,EIBWLIQ1,EIBWLIQ2,EIBWLQP2).
    Returns True on success, False if any step fails (COND=(4,LT) equivalent).
    """
    reptmon = macros['reptmon']
    nowk    = macros['nowk']

    # ---- %INC PGM(DALWPBBD) ----
    logger.info("Step 1.1 — Running DALWPBBD...")
    try:
        result = DALWPBBD.main(reptmon=reptmon, nowk=nowk)
        if result.get('status') == 'ERROR':
            logger.error(f"DALWPBBD returned ERROR: {result}")
            return False
        logger.info(f"DALWPBBD completed: {result}")
    except Exception as e:
        logger.error(f"DALWPBBD failed with exception: {e}")
        return False

    # ---- %INC PGM(EIBWLIQ1) ----
    logger.info("Step 1.2 — Running EIBWLIQ1...")
    try:
        EIBWLIQ1.main()
        logger.info("EIBWLIQ1 completed.")
    except Exception as e:
        logger.error(f"EIBWLIQ1 failed with exception: {e}")
        return False

    # ---- %INC PGM(EIBWLIQ2) ----
    logger.info("Step 1.3 — Running EIBWLIQ2...")
    try:
        EIBWLIQ2.main()
        logger.info("EIBWLIQ2 completed.")
    except Exception as e:
        logger.error(f"EIBWLIQ2 failed with exception: {e}")
        return False

    # ---- %INC PGM(EIBWLQP2) ----
    logger.info("Step 1.4 — Running EIBWLQP2...")
    try:
        EIBWLQP2.main()
        logger.info("EIBWLQP2 completed.")
    except Exception as e:
        logger.error(f"EIBWLQP2 failed with exception: {e}")
        return False

    return True

# ============================================================================
# STEP 2 — EIBFODFR job step (separate SAS execution context)
# //DELETE step: clears previous ODFR.REPT.WEEK output before running.
# EIBFODFR reads from its own BNM (SAP.PBB.SASDATA) and DEPOSIT libraries.
# ============================================================================
def run_step2() -> bool:
    """
    //EIBFODFR EXEC SAS609 job step.
    Executes %INC PGM(EIBFODFR).

    The //DELETE step that precedes this in the JCL removes the previous
    SAP.PBB.ODFR.REPT.WEEK dataset. In Python, the output file is simply
    overwritten on each run (equivalent behaviour).

    Returns True on success, False on failure.
    """
    # //DELETE EXEC PGM=IEFBR14 equivalent:
    # The ODFR report output file is overwritten by EIBFODFR on each run.
    # No explicit pre-deletion needed — open(..., 'w') truncates automatically.
    logger.info("Step 2 — Running EIBFODFR (separate job step)...")
    try:
        # EIBFODFR is a module-level script; its logic runs on import in the
        # original conversion. Re-executing via its main block is equivalent
        # to %INC PGM(EIBFODFR) within a fresh SAS session.
        # The module uses its own path constants (DEPOSIT_DIR, BNM_DIR, OUTPUT_DIR)
        # which correspond to the separate DDname allocations in the EIBFODFR step.
        import importlib
        importlib.reload(EIBFODFR)
        logger.info("EIBFODFR completed.")
    except Exception as e:
        logger.error(f"EIBFODFR failed with exception: {e}")
        return False

    return True

# ============================================================================
# MAIN — orchestrates the full EIBWLIQP job stream
# COND=(4,LT): subsequent steps are skipped if a prior step returns RC >= 4.
# ============================================================================
def main():
    """
    Orchestrates the full EIBWLIQP JCL job stream.

    Execution order:
      1. Initialise BNM.REPTDATE from DEPOSIT.REPTDATE  (DATA BNM.REPTDATE step)
      2. Run DALWPBBD, EIBWLIQ1, EIBWLIQ2, EIBWLQP2    (//EIBWLIQ1 EXEC step)
      3. Run EIBFODFR                                    (//EIBFODFR EXEC step)

    COND=(4,LT) is replicated by aborting subsequent steps on any failure.
    """
    logger.info("=" * 60)
    logger.info("EIBWLIQP job started")
    logger.info("=" * 60)

    # ---- Initialise BNM.REPTDATE from DEPOSIT.REPTDATE ----
    logger.info("Initialising BNM.REPTDATE from DEPOSIT.REPTDATE...")
    try:
        macros = initialise_reptdate()
    except Exception as e:
        logger.error(f"REPTDATE initialisation failed: {e}")
        sys.exit(8)

    # ---- Step 1: EIBWLIQ1 job step ----
    # COND=(4,LT): abort if step 1 fails
    step1_ok = run_step1(macros)
    if not step1_ok:
        logger.error(
            "Step 1 (EIBWLIQ1 job step) failed — "
            "COND=(4,LT): skipping subsequent steps."
        )
        sys.exit(8)

    # ---- Step 2: EIBFODFR job step ----
    step2_ok = run_step2()
    if not step2_ok:
        logger.error("Step 2 (EIBFODFR job step) failed.")
        sys.exit(8)

    logger.info("=" * 60)
    logger.info("EIBWLIQP job completed successfully.")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
