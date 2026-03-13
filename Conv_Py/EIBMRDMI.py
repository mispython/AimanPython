#!/usr/bin/env python3
"""
Program : EIBMRDMI.py
Purpose : RDAL FOR ISLAMIC ITEMS — Orchestrator

Resolves the reporting date and derived macro variables from the LOAN.REPTDATE
dataset, then dispatches to each sub-program in the correct sequence:
    1. DEPRDALM   — always
    2. LOANRDAL   — always
    3. KALWPBBI   — always
    4. KALMPBBI   — always
    5. FALQPBBI   — quarterly only (REPTMON in 03, 06, 09, 12)
    6. LALQPBBI   — quarterly only
    7. KALQPBBI   — quarterly only

Original JCL DD allocations (dataset names retained as comments):
    //LOAN     DD DSN=SAP.PBB.MNILN(0)       -> input/LOAN/
    //DEPOSIT  DD DSN=SAP.PBB.DEPOSIT(0)     -> input/DEPOSIT/
    //BNMTBL1  DD DSN=SAP.PBB.KAPITI1(0)     -> input/BNMTBL1.dat
    //BNMTBL2  DD DSN=SAP.PBB.KAPITI2(0)     -> input/BNMTBL2.dat
    //BNMTBL3  DD DSN=SAP.PBB.KAPITI3(0)     -> input/BNMTBL3.dat
    //BNMK     DD DSN=SAP.PBB.KAPITI.SASDATA -> input/BNMK/
    //PGM      DD DSN=SAP.BNM.PROGRAM        -> (Python modules, resolved via import)

OPTIONS: YEARCUTOFF=1950, LS=132, PS=60, NOCENTER

Dependencies:
    - PBBDPFMT  : Format definitions for deposit products   (%INC PGM(PBBDPFMT))
    - PBBLNFMT  : Format definitions for loan products      (%INC PGM(PBBLNFMT))
    - DEPRDALM  : Deposit RDAL processing                   (%INC PGM(DEPRDALM))
    - LOANRDAL  : Loan RDAL processing                      (%INC PGM(LOANRDAL))
    - KALWPBBI  : Islamic KAPITI Part I                     (%INC PGM(KALWPBBI))
    - KALMPBBI  : Islamic KAPITI Part II                    (%INC PGM(KALMPBBI))
    - FALQPBBI  : Islamic FD remaining maturity report      (%INC PGM(FALQPBBI))  [quarterly]
    - LALQPBBI  : Islamic KAPITI Part III M&I Loan report   (%INC PGM(LALQPBBI)) [quarterly]
    - KALQPBBI  : Islamic KAPITI Part III deposits          (%INC PGM(KALQPBBI)) [quarterly]
"""

from __future__ import annotations

import calendar
import importlib
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Ensure all sub-programs in the same directory are importable
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# ---------------------------------------------------------------------------
# Format library imports
# SAS: %INC PGM(PBBDPFMT, PBBLNFMT);
# Loaded at orchestrator level to guarantee availability before sub-programs run.
# ---------------------------------------------------------------------------
from PBBDPFMT import (  # noqa: F401
    fdrmmt_format,
    fdorgmt_format,
)
from PBBLNFMT import (  # noqa: F401
    MORE_PLAN,
    MORE_ISLAM,
    HP_ALL,
    HP_ACTIVE,
    AITAB,
    HOME_ISLAMIC,
    HOME_CONVENTIONAL,
    SWIFT_ISLAMIC,
    SWIFT_CONVENTIONAL,
    FCY_PRODUCTS,
)

# ---------------------------------------------------------------------------
# SAS date helpers
# ---------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_python(sas_days: int) -> date:
    """Convert SAS date (days since 1960-01-01) to Python date."""
    return SAS_EPOCH + timedelta(days=int(sas_days))


def python_date_to_sas(d: date) -> int:
    """Convert Python date to SAS date integer."""
    return (d - SAS_EPOCH).days


# ---------------------------------------------------------------------------
# DATA REPTDATE — derive all macro variables from the reporting date record
# ---------------------------------------------------------------------------
def resolve_reptdate(loan_reptdate_parquet: Path) -> dict:
    """
    DATA REPTDATE (KEEP=REPTDATE);
    SET LOAN.REPTDATE;

    Determines week number, start day, month/year values, and all
    CALL SYMPUT macro variable equivalents from the single REPTDATE record.

    Returns a dict of all macro variable equivalents:
        REPTDATE        Python date object
        SDATE           Python date object (start of reporting window)
        NOWK            Week number string '1'-'4'
        NOWK1           Prior week string
        NOWK2           Two-weeks-prior string (only for week 4, else None)
        NOWK3           Three-weeks-prior string (only for week 4, else None)
        REPTMON         Zero-padded month string e.g. '09'
        REPTMON1        Prior month string (adjusted at week 1)
        REPTYEAR        Four-digit year string e.g. '2005'
        REPTDAY         Zero-padded day string e.g. '30'
        RDATE           DDMMYY8. string e.g. '30/09/05'  (for most sub-programs)
        RDATE_DDMMYYYY  DDMMYYYY string e.g. '30092005'  (for FALQPBBI env var)
        SDATE_STR       DDMMYY8. string for start date
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{loan_reptdate_parquet}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError(f"No REPTDATE record found in {loan_reptdate_parquet}")

    reptdate: date = sas_date_to_python(int(row[0]))

    day  = reptdate.day
    mm   = reptdate.month
    year = reptdate.year

    # SELECT(DAY(REPTDATE))
    wk2: str | None = None
    wk3: str | None = None

    if day == 8:
        sdd = 1;  wk = "1"; wk1 = "4"
    elif day == 15:
        sdd = 9;  wk = "2"; wk1 = "1"
    elif day == 22:
        sdd = 16; wk = "3"; wk1 = "2"
    else:  # OTHERWISE — end-of-month
        sdd = 23; wk = "4"; wk1 = "3"
        wk2 = "2"; wk3 = "1"

    # MM1 — prior month; roll back to December if month is January
    if wk == "1":
        mm1 = mm - 1 if mm > 1 else 12
    else:
        mm1 = mm

    # SDATE = MDY(MM, SDD, YEAR(REPTDATE)) — clamp SDD to valid day
    last_day_of_mm = calendar.monthrange(year, mm)[1]
    sdate = date(year, mm, min(sdd, last_day_of_mm))

    return {
        "REPTDATE":       reptdate,
        "SDATE":          sdate,
        "NOWK":           wk,
        "NOWK1":          wk1,
        "NOWK2":          wk2,
        "NOWK3":          wk3,
        "REPTMON":        f"{mm:02d}",
        "REPTMON1":       f"{mm1:02d}",
        "REPTYEAR":       str(year),
        "REPTDAY":        f"{day:02d}",
        "RDATE":          reptdate.strftime("%d/%m/%y"),     # DD/MM/YY  (DDMMYY8.)
        "RDATE_DDMMYYYY": reptdate.strftime("%d%m%Y"),       # DDMMYYYY  (FALQPBBI env var)
        "SDATE_STR":      sdate.strftime("%d/%m/%y"),
    }


# ---------------------------------------------------------------------------
# Sub-program dispatch functions
# ---------------------------------------------------------------------------

def _run_deprdalm(macros: dict) -> None:
    """
    %INC PGM(DEPRDALM);

    DEPRDALM uses module-level variables REPTMON, NOWK, RDATE (string).
    BNM_DIR is defined inside the module as Path("data/BNM"); the sub-program
    constructs parquet paths as BNM_DIR / f"{name}{REPTMON}{NOWK}.parquet".
    """
    import DEPRDALM
    DEPRDALM.REPTMON = macros["REPTMON"]
    DEPRDALM.NOWK    = macros["NOWK"]
    DEPRDALM.RDATE   = macros["RDATE"]
    DEPRDALM.main()


def _run_loanrdal(macros: dict) -> None:
    """
    %INC PGM(LOANRDAL);

    LOANRDAL uses module-level variables REPTMON, NOWK, RDATE (string).
    BNM_DIR is defined inside the module as Path("data/BNM"); the sub-program
    constructs parquet paths as BNM_DIR / f"{name}{REPTMON}{NOWK}.parquet".
    """
    import LOANRDAL
    LOANRDAL.REPTMON = macros["REPTMON"]
    LOANRDAL.NOWK    = macros["NOWK"]
    LOANRDAL.RDATE   = macros["RDATE"]
    LOANRDAL.main()


def _run_kalwpbbi(macros: dict) -> None:
    """
    %INC PGM(KALWPBBI);

    KALWPBBI uses module-level variables REPTMON, NOWK, RDATE (string DD/MM/YY).
    """
    import KALWPBBI
    KALWPBBI.REPTMON = macros["REPTMON"]
    KALWPBBI.NOWK    = macros["NOWK"]
    KALWPBBI.RDATE   = macros["RDATE"]
    KALWPBBI.main()


def _run_kalmpbbi(macros: dict) -> None:
    """
    %INC PGM(KALMPBBI);

    KALMPBBI uses module-level variables REPTMON, NOWK, and RDATE as a
    SAS date integer (days since 1960-01-01), not a string.
    """
    import KALMPBBI
    KALMPBBI.REPTMON = macros["REPTMON"]
    KALMPBBI.NOWK    = macros["NOWK"]
    KALMPBBI.RDATE   = python_date_to_sas(macros["REPTDATE"])
    KALMPBBI.main()


def _run_falqpbbi(macros: dict) -> None:
    """
    %INC PGM(FALQPBBI);

    FALQPBBI does NOT use module-level variables for RDATE. Instead it reads
        RDATE at module-import time from os.environ['RDATE'] in DDMMYYYY format
        (e.g. '30092005'). Because the parse runs at import time (module-level code),
        the env var must be set and the module reloaded so the correct date is used.
    """
    os.environ["RDATE"] = macros["RDATE_DDMMYYYY"]
    import FALQPBBI
    importlib.reload(FALQPBBI)
    FALQPBBI.main()


def _run_lalqpbbi(macros: dict) -> None:
    """
    %INC PGM(LALQPBBI);

    LALQPBBI uses module-level variables REPTMON, NOWK, RDATE (string DD/MM/YY).
    """
    import LALQPBBI
    LALQPBBI.REPTMON = macros["REPTMON"]
    LALQPBBI.NOWK    = macros["NOWK"]
    LALQPBBI.RDATE   = macros["RDATE"]
    LALQPBBI.main()


def _run_kalqpbbi(macros: dict) -> None:
    """
    %INC PGM(KALQPBBI);

    KALQPBBI uses module-level variables REPTMON, NOWK, RDATE (string DD/MM/YY).
    """
    import KALQPBBI
    KALQPBBI.REPTMON = macros["REPTMON"]
    KALQPBBI.NOWK    = macros["NOWK"]
    KALQPBBI.RDATE   = macros["RDATE"]
    KALQPBBI.main()


# ---------------------------------------------------------------------------
# %MACRO PROCESS — main dispatch sequence
# ---------------------------------------------------------------------------
def process(macros: dict) -> None:
    """
    %MACRO PROCESS;
        %INC PGM(DEPRDALM);
        %INC PGM(LOANRDAL);
        %INC PGM(KALWPBBI);
        %INC PGM(KALMPBBI);
        %IF "&REPTMON" EQ "03" OR
            "&REPTMON" EQ "06" OR
            "&REPTMON" EQ "09" OR
            "&REPTMON" EQ "12" %THEN %DO;
             %INC PGM(FALQPBBI);
             %INC PGM(LALQPBBI);
             %INC PGM(KALQPBBI);
        %END;
    %MEND;
    """
    reptmon = macros["REPTMON"]

    # Always-run programs
    _run_deprdalm(macros)
    _run_loanrdal(macros)
    _run_kalwpbbi(macros)
    _run_kalmpbbi(macros)

    # Quarterly-only programs (REPTMON in 03, 06, 09, 12)
    if reptmon in ("03", "06", "09", "12"):
        _run_falqpbbi(macros)
        _run_lalqpbbi(macros)
        _run_kalqpbbi(macros)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def main() -> None:
    # LOAN.REPTDATE parquet — equivalent of //LOAN DD DSN=SAP.PBB.MNILN(0)
    loan_reptdate_parquet = INPUT_DIR / "LOAN" / "REPTDATE.parquet"

    # Resolve all macro variables from the REPTDATE record
    macros = resolve_reptdate(loan_reptdate_parquet)

    print(f"[EIBMRDMI] REPTDATE  : {macros['REPTDATE'].isoformat()}")
    print(f"[EIBMRDMI] REPTMON   : {macros['REPTMON']}")
    print(f"[EIBMRDMI] NOWK      : {macros['NOWK']}")
    print(f"[EIBMRDMI] RDATE     : {macros['RDATE']}")
    print(f"[EIBMRDMI] REPTYEAR  : {macros['REPTYEAR']}")
    print(f"[EIBMRDMI] Quarterly : {macros['REPTMON'] in ('03', '06', '09', '12')}")

    # LIBNAME BNM "SAP.PBB.D&REPTYEAR"
    # All BNM-library parquet files expected under input/BNM/<REPTYEAR>/
    bnm_lib_dir = INPUT_DIR / "BNM" / macros["REPTYEAR"]
    bnm_lib_dir.mkdir(parents=True, exist_ok=True)

    # Execute %PROCESS macro equivalent
    process(macros)

    print("[EIBMRDMI] Processing complete.")


if __name__ == "__main__":
    main()
