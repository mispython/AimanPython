#!/usr/bin/env python3
"""
Program : EIBQP124.py
Purpose : Quarterly batch orchestrator for Domestic Assets and Liabilities
          report (L124). Derives reporting period macro variables from
          LOAN.REPTDATE, then sequentially invokes:
            LALWP124  - Part I   (M&I Loan summary -> LALW)
            LALMP124  - Part II  (Loan aggregation -> LALM)
            LALQP124  - Part III (State/sector/rate -> LALQ)
            P124DL2A  - LALM -> ALMKM -> append to ALWKM
            P124DL3A  - LALQ -> ALQKM -> append to ALWKM
            P124RDAL  - Merge BIC reference + ALW, produce RDAL output

  Equivalent SAS JCL: EIBQP124
  SMR 2007-0925. RUN AFTER EIBWWKLY QUARTERLY.

Dependencies:
  - LALWP124.py  : Part I  – M&I Loan  (%INC PGM(LALWP124))
  - LALMP124.py  : Part II – Loan agg  (%INC PGM(LALMP124))
  - LALQP124.py  : Part III – State    (%INC PGM(LALQP124))
  - P124DL2A.py  : LALM -> ALWKM       (%INC PGM(P124DL2A))
  - P124DL3A.py  : LALQ -> ALWKM       (%INC PGM(P124DL3A))
  - P124RDAL.py  : BIC merge + output  (%INC PGM(P124RDLA))
"""

import os
import duckdb
import polars as pl
from datetime import date, datetime
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = Path(os.environ.get("BASE_DIR", "/data/sap"))

# LOAN DD  -> SAP.PBB.MNILN(0)   current generation
# LMLOAN DD -> SAP.PBB.MNILN(-4) previous generation (4 weeks back)
LOAN_DIR      = BASE_DIR / "pbb" / "mniln"
REPTDATE_PATH = LOAN_DIR / "reptdate.parquet"

# BNM DD   -> SAP.PBB.P124
BNM_PATH      = BASE_DIR / "pbb" / "p124"

# BNM1 library -> SAP.PBB.SASDATA
BNM1_PATH     = BASE_DIR / "pbb" / "sasdata"

# RDALKM output -> SAP.PBB.KAPMNI.RDAL124 (FB LRECL=80)
RDALKM_PATH   = BASE_DIR / "pbb" / "kapmni" / "rdal124.txt"

# Ensure output directories exist
BNM_PATH.mkdir(parents=True, exist_ok=True)
BNM1_PATH.mkdir(parents=True, exist_ok=True)
RDALKM_PATH.parent.mkdir(parents=True, exist_ok=True)

# ============================================================================
# STEP 1 : DERIVE MACRO VARIABLES FROM LOAN.REPTDATE
#
# DATA REPTDATE; SET LOAN.REPTDATE;
#   SELECT(DAY(REPTDATE));
#     WHEN (8)  DO; SDD = 1;  WK = '1'; WK1 = '4'; END;
#     WHEN(15)  DO; SDD = 9;  WK = '2'; WK1 = '1'; END;
#     WHEN(22)  DO; SDD = 16; WK = '3'; WK1 = '2'; END;
#     OTHERWISE DO; SDD = 23; WK = '4'; WK1 = '3';
#                             WK2= '2'; WK3 = '1'; END;
#   END;
#   ...
#   CALL SYMPUT('NOWK', ...);
#   CALL SYMPUT('REPTMON', ...);
#   ...
# ============================================================================

def derive_macro_vars(reptdate_path: Path) -> dict:
    """
    Read LOAN.REPTDATE, derive all CALL SYMPUT macro variables, and return
    them as a dictionary.  Equivalent to the DATA REPTDATE step in SAS.

    OPTIONS YEARCUTOFF=1950 applies (2-digit years >= 50 -> 19xx, else 20xx).
    """
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{reptdate_path}')").pl()
    con.close()

    # Take first row
    row      = df.row(0, named=True)
    reptdate = row["REPTDATE"]

    # Normalise to date object
    if isinstance(reptdate, datetime):
        reptdate = reptdate.date()
    elif isinstance(reptdate, (int, float)):
        # SAS numeric date (days since 1960-01-01)
        from datetime import timedelta
        reptdate = date(1960, 1, 1) + timedelta(days=int(reptdate))

    day = reptdate.day
    mm  = reptdate.month
    yr  = reptdate.year

    # SELECT(DAY(REPTDATE))
    if day == 8:
        sdd = 1;  wk = '1'; wk1 = '4'; wk2 = '';  wk3 = ''
    elif day == 15:
        sdd = 9;  wk = '2'; wk1 = '1'; wk2 = '';  wk3 = ''
    elif day == 22:
        sdd = 16; wk = '3'; wk1 = '2'; wk2 = '';  wk3 = ''
    else:                                            # OTHERWISE (end-of-month)
        sdd = 23; wk = '4'; wk1 = '3'; wk2 = '2'; wk3 = '1'

    # MM1 – previous month when WK='1', else same month
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    # MM2 – always previous month
    mm2 = mm - 1
    if mm2 == 0:
        mm2 = 12

    # SDATE = MDY(MM, SDD, YEAR(REPTDATE))
    sdate = date(yr, mm, sdd)

    # CALL SYMPUT equivalents
    reptmon  = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptmon2 = f"{mm2:02d}"
    reptyear = str(yr)
    reptday  = f"{day:02d}"
    rdate    = reptdate.strftime("%d%m%Y")   # DDMMYY8. -> DDMMYYYY (8 chars)
    sdate_s  = sdate.strftime("%d%m%Y")

    return {
        "NOWK":     wk,
        "NOWK1":    wk1,
        "NOWK2":    wk2,
        "NOWK3":    wk3,
        "REPTMON":  reptmon,
        "REPTMON1": reptmon1,
        "REPTMON2": reptmon2,
        "REPTYEAR": reptyear,
        "REPTDAY":  reptday,
        "RDATE":    rdate,
        "SDATE":    sdate_s,
    }


# ============================================================================
# STEP 2 : INJECT MACRO VARIABLES INTO ENVIRONMENT
# (Equivalent to CALL SYMPUT and subsequent macro resolution in SAS)
# ============================================================================

def set_env(macro_vars: dict) -> None:
    """
    Write all derived macro variables into os.environ so that the dependent
    programs (which read them via os.environ.get) pick them up correctly.
    """
    for key, val in macro_vars.items():
        if val:                        # skip empty strings (e.g. WK2/WK3 when not week 4)
            os.environ[key] = str(val)

    # Propagate library paths expected by dependent programs
    os.environ.setdefault("BASE_DIR",  str(BASE_DIR))
    os.environ.setdefault("BNM_DIR",   str(BNM_PATH))
    os.environ.setdefault("BNM1_DIR",  str(BNM1_PATH))


# ============================================================================
# STEP 3 : SEQUENTIAL EXECUTION OF DEPENDENT PROGRAMS
#
# %INC PGM(LALWP124);
# %INC PGM(LALMP124);
# %INC PGM(LALQP124);
# %INC PGM(P124DL2A);
# %INC PGM(P124DL3A);
# %INC PGM(P124RDLA);   <- note: source spells it P124RDLA; Python module is P124RDAL
# ============================================================================

def run_lalwp124() -> None:
    """
    %INC PGM(LALWP124)
    Part I – M&I Loan summary. Produces BNM.LALW{REPTMON}{NOWK}.
    """
    # Dependency: LALWP124.py
    from LALWP124 import main as lalwp124_main
    lalwp124_main()


def run_lalmp124() -> None:
    """
    %INC PGM(LALMP124)
    Part II – Full loan aggregation across NPL, customer, sector, collateral,
    approved limit, maturity, purpose, disbursement/repayment etc.
    Produces BNM.LALM{REPTMON}{NOWK}.
    """
    # Dependency: LALMP124.py
    from LALMP124 import main as lalmp124_main
    lalmp124_main()


def run_lalqp124() -> None:
    """
    %INC PGM(LALQP124)
    Part III – State/sector and fixed/floating rate aggregation.
    Produces BNM.LALQ{REPTMON}{NOWK}.
    """
    # Dependency: LALQP124.py
    from LALQP124 import main as lalqp124_main
    lalqp124_main()


def run_p124dl2a() -> None:
    """
    %INC PGM(P124DL2A)
    Summarise LALM -> ALMKM -> append to ALWKM, delete ALMKM.
    """
    # Dependency: P124DL2A.py
    # P124DL2A executes at import / module level (no main() wrapper in source).
    # Wrapping the import inside a function isolates its execution to this call.
    import importlib, sys
    mod_name = "P124DL2A"
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])
    else:
        importlib.import_module(mod_name)


def run_p124dl3a() -> None:
    """
    %INC PGM(P124DL3A)
    Rename LALQ BNMCODE -> ITCODE -> ALQKM -> append to ALWKM, delete ALQKM.
    """
    # Dependency: P124DL3A.py
    import importlib, sys
    mod_name = "P124DL3A"
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])
    else:
        importlib.import_module(mod_name)


def run_p124rdal() -> None:
    """
    %INC PGM(P124RDLA)   <- SAS source spells the member name P124RDLA;
                            the Python equivalent module is P124RDAL.py.
    Merge BIC reference codes with ALW summary, filter Cagamas loans,
    split into AL/OB/SP sections, write semicolon-delimited RDAL output.
    """
    # Dependency: P124RDAL.py
    from P124RDAL import main as p124rdal_main
    p124rdal_main()


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    # ------------------------------------------------------------------
    # OPTIONS SORTDEV=3390 YEARCUTOFF=1950 NOCENTER;
    # (YEARCUTOFF=1950 handled in derive_macro_vars; others are SAS-only)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # DATA REPTDATE: SET LOAN.REPTDATE; ... CALL SYMPUT(...)
    # Derive all reporting period variables.
    # ------------------------------------------------------------------
    macro_vars = derive_macro_vars(REPTDATE_PATH)

    # ------------------------------------------------------------------
    # LIBNAME BNM1 "SAP.PBB.SASDATA" DISP=SHR;
    # Inject macro vars + library paths into the process environment so
    # every dependent program resolves them via os.environ.get.
    # ------------------------------------------------------------------
    set_env(macro_vars)

    print("EIBQP124: macro variables derived:")
    for k, v in macro_vars.items():
        if v:
            print(f"  {k} = {v!r}")

    # ------------------------------------------------------------------
    # %INC PGM(LALWP124)
    # ------------------------------------------------------------------
    print("\nEIBQP124: running LALWP124 ...")
    run_lalwp124()

    # ------------------------------------------------------------------
    # %INC PGM(LALMP124)
    # ------------------------------------------------------------------
    print("\nEIBQP124: running LALMP124 ...")
    run_lalmp124()

    # ------------------------------------------------------------------
    # %INC PGM(LALQP124)
    # ------------------------------------------------------------------
    print("\nEIBQP124: running LALQP124 ...")
    run_lalqp124()

    # ------------------------------------------------------------------
    # %INC PGM(P124DL2A)
    # ------------------------------------------------------------------
    print("\nEIBQP124: running P124DL2A ...")
    run_p124dl2a()

    # ------------------------------------------------------------------
    # %INC PGM(P124DL3A)
    # ------------------------------------------------------------------
    print("\nEIBQP124: running P124DL3A ...")
    run_p124dl3a()

    # ------------------------------------------------------------------
    # %INC PGM(P124RDLA)   (module name: P124RDAL)
    # ------------------------------------------------------------------
    print("\nEIBQP124: running P124RDAL ...")
    run_p124rdal()

    print("\nEIBQP124 complete.")


if __name__ == "__main__":
    main()
