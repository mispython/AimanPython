#!/usr/bin/env python3
"""
Program  : EIIWLIQP.py
Purpose  : Job orchestrator — replicates the EIIWLIQP JCL job stream.
           Islamic Bank (PIBB) equivalent of EIBWLIQP.

           Invokes the following programs in sequence:

           Step 1 — EIBWLIQ1 (SAS step):
             Initialises BNM.REPTDATE from DEPOSIT.REPTDATE (PIBB source),
             then sequentially runs:
               - DALWPBBD  : PBB deposit manipulation (savings/current accounts)
               - EIBWLIQ1  : New Liquidity Framework (behavioural + contractual, full)
               - EIBWLIQ2  : New Liquidity Framework (contractual run-offs, end-of-month)
               - EIBWLQP2  : New Liquidity Framework Part 2-RM loans (contractual)

           Step 2 — EIBFODFR (separate SAS step, independent library context):
               - EIIFODFR  : OD Balance by Individuals and Corporates for
                             Liquidity Framework (Islamic Bank variant)

           Key differences from EIBWLIQP:
             - SDESC = 'PUBLIC ISLAMIC BANK BERHAD' (not 'PUBLIC BANK BERHAD')
             - All data sources reference PIBB datasets (SAP.PIBB.*)
             - Step 2 calls EIIFODFR (not EIBFODFR)
             - //DELETE clears SAP.PIBB.ODFR.REPT.WEEK

           JCL notes replicated:
             //DELETE step clears SAP.PIBB.ODFR.REPT.WEEK before EIIFODFR runs.
             SDESC is set here as 'PUBLIC ISLAMIC BANK BERHAD' and passed to child programs.
             EIIFODFR runs with its own separate library context (BNM = SAP.PIBB.SASDATA,
             DEPOSIT = SAP.PIBB.MNITB) independent of the first step.
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
# %INC PGM(EIIFODFR)  -> import and reload EIIFODFR (Islamic variant of EIBFODFR)
# ============================================================================
import DALWPBBD
import EIBWLIQ1
import EIBWLIQ2
import EIBWLQP2
import EIIFODFR

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
# Step 1 — EIBWLIQ1 job step library paths (PIBB datasets: SAP.PIBB.*)
BASE_DIR     = Path(".")
DEPOSIT_DIR  = BASE_DIR / "data" / "pibb" / "deposit"   # DEPOSIT: SAP.PIBB.MNITB(0)
BNM_DIR      = BASE_DIR / "data" / "pibb" / "bnm"       # BNM: &&TEMP (temporary work area)
BNM1_DIR     = BASE_DIR / "data" / "pibb" / "bnm1"      # BNM1: SAP.PIBB.SASDATA
BNMK_DIR     = BASE_DIR / "data" / "pibb" / "bnmk"      # BNMK: SAP.PIBB.KAPITI.SASDATA
FD_DIR       = BASE_DIR / "data" / "pibb" / "fd"        # FD: SAP.PIBB.MNIFD(0)
LOAN_DIR     = BASE_DIR / "data" / "pibb" / "loan"      # LOAN: SAP.PIBB.MNILN(0)
OUTPUT_DIR   = BASE_DIR / "output" / "pibb"

# Step 2 — EIIFODFR job step library paths (separate JCL step — independent context)
# //BNM     DD DSN=SAP.PIBB.SASDATA  (separate step, different from &&TEMP in Step 1)
# //DEPOSIT DD DSN=SAP.PIBB.MNITB(0)
EIIFODFR_BNM_DIR     = BASE_DIR / "data" / "pibb" / "bnm"      # SAP.PIBB.SASDATA
EIIFODFR_DEPOSIT_DIR = BASE_DIR / "data" / "pibb" / "deposit"  # SAP.PIBB.MNITB(0)
EIIFODFR_OUTPUT_DIR  = OUTPUT_DIR

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# SDESC — institution description set in JCL orchestrator
# SDESC='PUBLIC ISLAMIC BANK BERHAD' (differs from EIBWLIQP)
# In SAS: CALL SYMPUT('SDESC',PUT(SDESC,$26.))
# ============================================================================
SDESC = 'PUBLIC ISLAMIC BANK BERHAD'

# ============================================================================
# HELPER: initialise BNM.REPTDATE from DEPOSIT.REPTDATE (PIBB source)
# Replicates:
#   DATA BNM.REPTDATE;
#      SET DEPOSIT.REPTDATE;
#      SELECT(DAY(REPTDATE)); ... END;
#      SDESC='PUBLIC ISLAMIC BANK BERHAD';
#      CALL SYMPUT('REPTYEAR',...);
#      CALL SYMPUT('REPTMON',...);
#      ...
# Writes reptdate.parquet to BNM_DIR so all child programs can read it.
# ============================================================================
def initialise_reptdate() -> dict:
    """
    DATA BNM.REPTDATE; SET DEPOSIT.REPTDATE;
    Copies REPTDATE from PIBB DEPOSIT library to BNM temporary library,
    adds SDESC, and derives macro variables NOWK, REPTYEAR, REPTMON, REPTDAY,
    RDATE, SDESC.

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
    # SDESC='PUBLIC ISLAMIC BANK BERHAD'
    reptyear = str(reptdate.year)
    reptmon  = str(reptdate.month).zfill(2)
    reptday  = str(reptdate.day).zfill(2)
    rdate    = reptdate.strftime('%d/%m/%y')   # DDMMYY8. approximation
    sdesc    = SDESC                            # 'PUBLIC ISLAMIC BANK BERHAD'

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
# STEP 1 — EIBWLIQ1 job step (PIBB data sources)
# Runs: DALWPBBD -> EIBWLIQ1 -> EIBWLIQ2 -> EIBWLQP2
# All share the same BNM.REPTDATE initialised above from PIBB DEPOSIT.
# ============================================================================
def run_step1(macros: dict) -> bool:
    """
    //EIBWLIQ1 EXEC SAS609 job step (PIBB variant).
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
# STEP 2 — EIIFODFR job step (separate SAS execution context, PIBB variant)
# //DELETE step: clears SAP.PIBB.ODFR.REPT.WEEK before EIIFODFR runs.
# EIIFODFR reads from its own BNM (SAP.PIBB.SASDATA) and DEPOSIT libraries.
# ============================================================================
def run_step2() -> bool:
    """
    //EIBFODFR EXEC SAS609 job step (PIBB variant — calls EIIFODFR).
    Executes %INC PGM(EIIFODFR).

    The //DELETE step that precedes this in the JCL removes the previous
    SAP.PIBB.ODFR.REPT.WEEK dataset. In Python, the output file is simply
    overwritten on each run (equivalent behaviour).

    Returns True on success, False on failure.
    """
    # //DELETE EXEC PGM=IEFBR14 equivalent:
    # The ODFR report output file is overwritten by EIIFODFR on each run.
    # No explicit pre-deletion needed — open(..., 'w') truncates automatically.
    logger.info("Step 2 — Running EIIFODFR (separate job step, PIBB variant)...")
    try:
        # EIIFODFR is a module-level script; its logic runs on import in the
        # original conversion. Re-executing via importlib.reload() is equivalent
        # to %INC PGM(EIIFODFR) within a fresh SAS session, ensuring the module
        # re-executes with the correct PIBB library context.
        import importlib
        importlib.reload(EIIFODFR)
        logger.info("EIIFODFR completed.")
    except Exception as e:
        logger.error(f"EIIFODFR failed with exception: {e}")
        return False

    return True

# ============================================================================
# MAIN — orchestrates the full EIIWLIQP job stream
# COND=(4,LT): subsequent steps are skipped if a prior step returns RC >= 4.
# ============================================================================
def main():
    """
    Orchestrates the full EIIWLIQP JCL job stream (Islamic Bank variant).

    Execution order:
      1. Initialise BNM.REPTDATE from PIBB DEPOSIT.REPTDATE   (DATA BNM.REPTDATE step)
      2. Run DALWPBBD, EIBWLIQ1, EIBWLIQ2, EIBWLQP2           (//EIBWLIQ1 EXEC step)
      3. Run EIIFODFR                                           (//EIBFODFR EXEC step)

    COND=(4,LT) is replicated by aborting subsequent steps on any failure.
    """
    logger.info("=" * 60)
    logger.info("EIIWLIQP job started (PUBLIC ISLAMIC BANK BERHAD)")
    logger.info("=" * 60)

    # ---- Initialise BNM.REPTDATE from PIBB DEPOSIT.REPTDATE ----
    logger.info("Initialising BNM.REPTDATE from PIBB DEPOSIT.REPTDATE...")
    try:
        macros = initialise_reptdate()
    except Exception as e:
        logger.error(f"REPTDATE initialisation failed: {e}")
        sys.exit(8)

    # ---- Step 1: EIBWLIQ1 job step (PIBB) ----
    # COND=(4,LT): abort if step 1 fails
    step1_ok = run_step1(macros)
    if not step1_ok:
        logger.error(
            "Step 1 (EIBWLIQ1 job step) failed — "
            "COND=(4,LT): skipping subsequent steps."
        )
        sys.exit(8)

    # ---- Step 2: EIIFODFR job step (PIBB) ----
    step2_ok = run_step2()
    if not step2_ok:
        logger.error("Step 2 (EIIFODFR job step) failed.")
        sys.exit(8)

    logger.info("=" * 60)
    logger.info("EIIWLIQP job completed successfully.")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
