# !/usr/bin/env python3
"""
Program  : EIBWP124
Purpose  : Weekly run (after EIBWWKLY) for PIBB - Report on Domestic Assets
           and Liabilities Part I (M&I Loan / Cagamas L124).
           - Derives REPTDATE week/month variables for PIBB MNILN.
           - Runs LALWP124 to produce BNM.LALW{REPTMON}{NOWK}.
           - Copies BNMX.ALW{REPTMON}{NOWK} to BNM.ALW{REPTMON}{NOWK}.
           - Runs P124RDAL to produce the RDAL semicolon-delimited output.

           SMR 2007-0925. RUN AFTER EIBWWKLY.

Dependency: LALWP124 - produces BNM.LALW{REPTMON}{NOWK} from L124/UL124 data.
            P124RDAL - merges BIC codes with ALW data and writes RDAL output.
"""

import duckdb
import polars as pl
from pathlib import Path
import datetime
import shutil

# ---------------------------------------------------------------------------
# Dependency: LALWP124 (%INC PGM(LALWP124))
# ---------------------------------------------------------------------------
from LALWP124 import main as run_lalwp124

# ---------------------------------------------------------------------------
# Dependency: P124RDAL (%INC PGM(P124RDAL))
# ---------------------------------------------------------------------------
from P124RDAL import main as run_p124rdal

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR  = Path("/data/sap")

# PIBB MNILN - current generation (0) and prior (-4) reptdate/lnnote
PIBB_LOAN_PATH      = BASE_DIR / "pibb/mniln/gen0/reptdate.parquet"   # SAP.PIBB.MNILN(0)  - REPTDATE
PIBB_LMLOAN_PATH    = BASE_DIR / "pibb/mniln/gen4/reptdate.parquet"   # SAP.PIBB.MNILN(-4) - REPTDATE

# BNM output library  (SAP.PBB.P124)
BNM_PATH            = BASE_DIR / "pbb/p124"                           # BNM library (DD BNM)

# BNMX library - source of ALW data keyed by REPTYEAR
# SAP.PIBB.D&REPTYEAR  -> resolved at runtime once REPTYEAR is known
# BNMX is set dynamically below based on REPTYEAR.

# BNM1 library - PIBB sasdata (SAP.PIBB.SASDATA)
BNM1_PATH           = BASE_DIR / "pibb/sasdata"                       # SAP.PIBB.SASDATA

# RDAL output (SAP.PBB.FISS.RDAL124)
# RDAL_OUTPUT_PATH is managed by P124RDAL; defined here for reference.
RDAL_OUTPUT_PATH    = BASE_DIR / "pbb/fiss/rdal124.txt"               # SAP.PBB.FISS.RDAL124

# Program library (SAP.BNM.PROGRAM) - used by %INC PGM(); handled via imports above.
# PGM_PATH          = BASE_DIR / "bnm/program"

# ============================================================================
# HELPER: Derive all date/week macro variables from REPTDATE
# ============================================================================

def get_date_variables(reptdate_parquet: Path) -> dict:
    """
    Replicate SAS DATA REPTDATE step with SELECT(DAY(REPTDATE)) logic.
    Returns a dict of all macro variable equivalents:
      NOWK, NOWK1, NOWK2, NOWK3,
      REPTMON, REPTMON1, REPTMON2,
      REPTYEAR, REPTDAY, RDATE, SDATE
    """
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchdf()
    con.close()

    reptdate = df['REPTDATE'].iloc[0]
    if hasattr(reptdate, 'date'):
        reptdate = reptdate.date()

    day  = reptdate.day
    mm   = reptdate.month
    yyyy = reptdate.year

    # SELECT(DAY(REPTDATE))
    if day == 8:
        sdd  = 1
        wk   = '1';  wk1 = '4'
        wk2  = None; wk3 = None
    elif day == 15:
        sdd  = 9
        wk   = '2';  wk1 = '1'
        wk2  = None; wk3 = None
    elif day == 22:
        sdd  = 16
        wk   = '3';  wk1 = '2'
        wk2  = None; wk3 = None
    else:
        sdd  = 23
        wk   = '4';  wk1 = '3'
        wk2  = '2';  wk3 = '1'

    # MM1: prior month for WK='1', else current month
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    # MM2: prior month (for all weeks)
    mm2 = mm - 1
    if mm2 == 0:
        mm2 = 12

    # SDATE = MDY(MM, SDD, YEAR(REPTDATE))
    sdate = datetime.date(yyyy, mm, sdd)

    return {
        'NOWK':     wk,
        'NOWK1':    wk1,
        'NOWK2':    wk2,
        'NOWK3':    wk3,
        'REPTMON':  f"{mm:02d}",
        'REPTMON1': f"{mm1:02d}",
        'REPTMON2': f"{mm2:02d}",
        'REPTYEAR': str(yyyy),
        'REPTDAY':  f"{day:02d}",
        'RDATE':    reptdate.strftime('%d/%m/%y'),   # DDMMYY8.
        'SDATE':    sdate.strftime('%d/%m/%y'),      # DDMMYY8.
        '_reptdate_obj': reptdate,
    }


# ============================================================================
# MAIN
# ============================================================================

def main():
    # -----------------------------------------------------------------------
    # DATA REPTDATE: SET LOAN.REPTDATE (SAP.PIBB.MNILN(0))
    # Derive all week/month/year macro variables
    # -----------------------------------------------------------------------
    dvars = get_date_variables(PIBB_LOAN_PATH)

    nowk     = dvars['NOWK']
    nowk1    = dvars['NOWK1']
    nowk2    = dvars['NOWK2']
    nowk3    = dvars['NOWK3']
    reptmon  = dvars['REPTMON']
    reptmon1 = dvars['REPTMON1']
    reptmon2 = dvars['REPTMON2']
    reptyear = dvars['REPTYEAR']
    reptday  = dvars['REPTDAY']
    rdate    = dvars['RDATE']
    sdate    = dvars['SDATE']

    print(f"REPTMON={reptmon}, NOWK={nowk}, REPTYEAR={reptyear}, RDATE={rdate}, SDATE={sdate}")

    # -----------------------------------------------------------------------
    # LIBNAME BNM1 "SAP.PIBB.SASDATA"
    # LIBNAME BNMX "SAP.PIBB.D&REPTYEAR"
    # Resolve BNMX path dynamically using REPTYEAR
    # -----------------------------------------------------------------------
    # LIBNAME BNM1 "SAP.PIBB.SASDATA" DISP=SHR
    bnm1_path = BNM1_PATH

    # LIBNAME BNMX "SAP.PIBB.D&REPTYEAR" DISP=SHR
    bnmx_path = BASE_DIR / f"pibb/d{reptyear}"

    # -----------------------------------------------------------------------
    # %INC PGM(LALWP124)
    # Runs the LALWP124 dependency which internally:
    #   - runs L124PBBD to produce BNM.L124&REPTMON&NOWK and BNM.UL124&REPTMON&NOWK
    #   - summarises by CUSTCD, PRODCD and Cagamas, appends to BNM.LALW&REPTMON&NOWK
    # -----------------------------------------------------------------------
    run_lalwp124()

    # -----------------------------------------------------------------------
    # DATA BNM.ALW&REPTMON&NOWK;
    #   SET BNMX.ALW&REPTMON&NOWK;
    # Copy ALW parquet from BNMX library into BNM library
    # -----------------------------------------------------------------------
    bnmx_alw_path = bnmx_path / f"alw{reptmon}{nowk}.parquet"
    bnm_alw_path  = BNM_PATH  / f"alw{reptmon}{nowk}.parquet"

    BNM_PATH.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    alw_df = con.execute(
        f"SELECT * FROM read_parquet('{bnmx_alw_path}')"
    ).pl()
    con.close()

    alw_df.write_parquet(bnm_alw_path)
    print(f"ALW copied from {bnmx_alw_path} to {bnm_alw_path}  ({len(alw_df)} rows)")

    # -----------------------------------------------------------------------
    # %INC PGM(P124RDAL)
    # Runs the P124RDAL dependency which:
    #   - loads BIC codes (weekly or monthly)
    #   - merges with BNM.ALW&REPTMON&NOWK
    #   - appends Cagamas loan data
    #   - splits into AL / OB / SP sections
    #   - writes semicolon-delimited RDAL output file
    # -----------------------------------------------------------------------
    run_p124rdal()


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
