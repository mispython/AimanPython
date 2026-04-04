#!/usr/bin/env python3
"""
Program : EIISMELQ.py
Remarks : JCL ORCHESTRATOR – NEW LIQUIDITY FRAMEWORK FOR SME (PIBB)
          (TO RUN AS MONTH END)
          Equivalent to JCL job EIISMELQ which:
            1. Deletes/recreates SAP.PIBB.SMELN.MNILIQ
            2. Sets SDESC = 'PUBLIC ISLAMIC BANK BERHAD' and other macro variables
            3. Calls %INC PGM(EISMELQE) to produce the SME liquidity report
            4. FTP step to PBB DataWarehouse server is commented out in original JCL
"""

import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

# ============================================================================
# PATH SETUP
# ============================================================================

BASE_DIR    = Path(os.environ.get("BASE_DIR",    "/data"))
# PIBB-specific library paths (mapped from JCL DD statements)
# //DEPOSIT  DD DSN=SAP.PIBB.MNITB(0)
# //LOAN     DD DSN=SAP.PIBB.MNILN(0)
# //BNM1     DD DSN=SAP.PIBB.SASDATA
# //MNILQ    DD DSN=SAP.PIBB.SMELN.MNILIQ
DEPOSIT_DIR = Path(os.environ.get("DEPOSIT_DIR", BASE_DIR / "PIBB" / "MNITB"))
LOAN_DIR    = Path(os.environ.get("LOAN_DIR",    BASE_DIR / "PIBB" / "MNILN"))
BNM1_DIR    = Path(os.environ.get("BNM1_DIR",    BASE_DIR / "PIBB" / "SASDATA"))
BNM_DIR     = Path(os.environ.get("BNM_DIR",     BASE_DIR / "BNM"))
OUTPUT_DIR  = Path(os.environ.get("OUTPUT_DIR",  BASE_DIR / "output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Output file: SAP.PIBB.SMELN.MNILIQ → MNILQ_PIBB.txt
MNILQ_PATH  = OUTPUT_DIR / "MNILQ_PIBB.txt"

# ============================================================================
# STEP: DELETE  (IEFBR14 DISP=(MOD,DELETE,DELETE))
# Equivalent to deleting SAP.PIBB.SMELN.MNILIQ before the run.
# ============================================================================

def step_delete() -> None:
    """Remove the output file if it exists (mirrors IEFBR14 DELETE step)."""
    if MNILQ_PATH.exists():
        MNILQ_PATH.unlink()
        print(f"Deleted existing output: {MNILQ_PATH}")


# ============================================================================
# STEP: CREATE  (IEFBR14 DISP=(NEW,CATLG,DELETE))
# The PIBB JCL has an explicit CREATE step (absent in PBB JCL).
# In Python this is a no-op because the file is created on first write;
# the DELETE step above is sufficient to reset the output.
# ============================================================================

def step_create() -> None:
    """
    Mirrors IEFBR14 CREATE step in PIBB JCL
    (//CREATE EXEC PGM=IEFBR14 / DISP=(NEW,CATLG,DELETE)).
    No action required; file creation happens at write time.
    """
    pass


# ============================================================================
# READ REPTDATE FROM DEPOSIT LIBRARY
# (DATA BNM.REPTDATE; SET DEPOSIT.REPTDATE; ...)
# ============================================================================

def load_reptdate_from_deposit() -> dict:
    """
    Read REPTDATE from DEPOSIT.REPTDATE parquet (SAP.PIBB.MNITB(0)) and
    derive macro variables including SDESC for PIBB.
    """
    reptdate_parquet = DEPOSIT_DIR / "REPTDATE.parquet"
    conn = duckdb.connect()
    row = conn.execute(
        f"SELECT reptdate FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchone()
    conn.close()

    if row is None:
        raise RuntimeError("REPTDATE table is empty")

    reptdate: date = row[0]

    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    return {
        "reptdate":  reptdate,
        "nowk":      nowk,
        "reptyear":  str(reptdate.year),
        "reptmon":   f"{reptdate.month:02d}",
        "reptday":   f"{reptdate.day:02d}",
        "rdate":     reptdate.strftime("%d/%m/%Y"),
        # SDESC='PUBLIC ISLAMIC BANK BERHAD' (padded to $26. in SAS → stored as-is here)
        "sdesc":     "PUBLIC ISLAMIC BANK BERHAD",
    }


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    # ── STEP DELETE ──────────────────────────────────────────────────────────
    step_delete()

    # ── STEP CREATE (PIBB only) ───────────────────────────────────────────────
    step_create()

    # ── Read REPTDATE and set macro-equivalent environment variables ──────────
    rd = load_reptdate_from_deposit()

    # Propagate as environment variables for EISMELQE (mirrors CALL SYMPUT)
    os.environ["REPTMON"]  = rd["reptmon"]
    os.environ["NOWK"]     = rd["nowk"]
    os.environ["RDATE"]    = rd["rdate"]
    os.environ["SDESC"]    = rd["sdesc"]
    os.environ["BNM1_DIR"] = str(BNM1_DIR)
    os.environ["BNM_DIR"]  = str(BNM_DIR)
    os.environ["OUTPUT_DIR"] = str(OUTPUT_DIR)

    # Override the output path env var so EISMELQE writes to PIBB-specific file
    os.environ["MNILQ_FILENAME"] = MNILQ_PATH.name

    print(f"REPTDATE : {rd['reptdate']}")
    print(f"REPTMON  : {rd['reptmon']}")
    print(f"NOWK     : {rd['nowk']}")
    print(f"RDATE    : {rd['rdate']}")
    print(f"SDESC    : {rd['sdesc']}")

    # ── %INC PGM(EISMELQE) – invoke the shared processing program ───────────
    # Mirrors JCL step EIISMELQ EXEC SAS609 ... %INC PGM(EISMELQE)
    result = subprocess.run(
        [sys.executable, "EISMELQE.py"],
        env=os.environ.copy(),
    )
    rc = result.returncode
    if rc >= 4:
        print(f"EISMELQE returned RC={rc} – aborting.", file=sys.stderr)
        return rc

    print(f"EISMELQE completed with RC={rc}")

    # ── FTP step ─────────────────────────────────────────────────────────────
    # The FTP step in the original PIBB JCL is fully commented out:
    #   //*RUNSFTP  EXEC COZBATCH
    #   //*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(CSASSFTP)
    #   //*PUT //SAP.PIBB.SMELN.MNILIQ SMEPIBB.TXT
    # No FTP action is performed for PIBB.

    return 0


if __name__ == "__main__":
    sys.exit(main())
