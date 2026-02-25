# !/usr/bin/env python3
"""
Program: L124PBBD
Purpose: Filter LOAN and ULOAN datasets for PRODUCT IN (124, 145),
            assign PRODCD='34120' and AMTIND='I', then write output
            parquet datasets BNM.L124{REPTMON}{NOWK} and BNM.UL124{REPTMON}{NOWK}.

Dependency: Requires REPTMON and NOWK derived from REPTDATE in the upstream
                loan master file (e.g. as produced by EIBXLNLC or similar program).
            LOAN{REPTMON}{NOWK} and ULOAN{REPTMON}{NOWK} must exist in BNM1 path.
"""

import duckdb
import polars as pl
from pathlib import Path
import datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR  = Path("/data/sap")

# Input paths
BNM1_PATH = BASE_DIR / "pbb/bnm1"          # SAP source: BNM1 library
                                            # contains LOAN{MM}{WK} and ULOAN{MM}{WK}

# REPTDATE source (to derive REPTMON and NOWK)
REPTDATE_PATH = BASE_DIR / "pbb/mniln/reptdate.parquet"   # SAP.PBB.MNILN(0) - REPTDATE

# Output path
BNM_PATH  = BASE_DIR / "pbb/bnm"           # SAP target: BNM library

# ============================================================================
# HELPER: Derive REPTMON and NOWK from REPTDATE
# ============================================================================

def get_reptmon_nowk(reptdate_parquet: Path) -> tuple:
    """
    Read REPTDATE and derive REPTMON (ZZ2.) and NOWK week code.
    SELECT(DAY(REPTDATE)):
      WHEN(8)  -> WK='1'
      WHEN(15) -> WK='2'
      WHEN(22) -> WK='3'
      OTHERWISE -> WK='4'
    """
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchdf()
    con.close()

    reptdate = df['REPTDATE'].iloc[0]
    if hasattr(reptdate, 'date'):
        reptdate = reptdate.date()

    reptmon = reptdate.strftime('%m')   # Z2. -> zero-padded 2-digit month
    day     = reptdate.day

    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    return reptmon, nowk

# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    reptmon, nowk = get_reptmon_nowk(REPTDATE_PATH)

    # Input file paths
    loan_path  = BNM1_PATH / f"loan{reptmon}{nowk}.parquet"    # BNM1.LOAN&REPTMON&NOWK
    uloan_path = BNM1_PATH / f"uloan{reptmon}{nowk}.parquet"   # BNM1.ULOAN&REPTMON&NOWK

    # Output file paths
    BNM_PATH.mkdir(parents=True, exist_ok=True)
    l124_out  = BNM_PATH / f"l124{reptmon}{nowk}.parquet"      # BNM.L124&REPTMON&NOWK
    ul124_out = BNM_PATH / f"ul124{reptmon}{nowk}.parquet"     # BNM.UL124&REPTMON&NOWK

    con = duckdb.connect()

    # ----------------------------------------------------------------
    # DATA BNM.L124&REPTMON&NOWK;
    #   SET BNM1.LOAN&REPTMON&NOWK;
    #   IF PRODUCT IN (124,145);
    #   PRODCD='34120';
    #   AMTIND='I';
    # ----------------------------------------------------------------
    l124_df = con.execute(
        f"SELECT * FROM read_parquet('{loan_path}') WHERE PRODUCT IN (124, 145)"
    ).pl()

    l124_df = l124_df.with_columns([
        pl.lit('34120').alias('PRODCD'),
        pl.lit('I').alias('AMTIND'),
    ])

    l124_df.write_parquet(l124_out)
    print(f"L124 written to: {l124_out}  ({len(l124_df)} rows)")

    # ----------------------------------------------------------------
    # DATA BNM.UL124&REPTMON&NOWK;
    #   SET BNM1.ULOAN&REPTMON&NOWK;
    #   IF PRODUCT IN (124,145);
    #   PRODCD='34120';
    #   AMTIND='I';
    # ----------------------------------------------------------------
    ul124_df = con.execute(
        f"SELECT * FROM read_parquet('{uloan_path}') WHERE PRODUCT IN (124, 145)"
    ).pl()

    ul124_df = ul124_df.with_columns([
        pl.lit('34120').alias('PRODCD'),
        pl.lit('I').alias('AMTIND'),
    ])

    ul124_df.write_parquet(ul124_out)
    print(f"UL124 written to: {ul124_out}  ({len(ul124_df)} rows)")

    con.close()


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
