#!/usr/bin/env python3
"""
Program  : RDALPBIF.py
Purpose  : Load PBIF client data, filter and enrich, then compute
           next billing date (MATDTE) for commitment maturity profiling.

           Reads  : PBIF.CLIEN<REPTYEAR><REPTMON><REPTDAY>  (parquet)
                    REPTDATE                                  (parquet, from caller)

           Produces: PBIF dataset (parquet) — enriched with MATDTE, PRODCD,
                     FISSPURP, AMTIND, CUSTFISS, BALANCE, UNDRAWN.

           Note   : This module is included (%INC PGM(RDALPBIF)) by a calling
                    program. It expects REPTDATE (date) and the following macro
                    variables to be passed in: REPTYEAR, REPTMON, REPTDAY.

           Dependencies: PBBLNFMT (included in SAS header via %INC PGM(PBBLNFMT))
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import logging
from datetime import date
from pathlib import Path
from typing import Optional

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# Note: %INC PGM(PBBLNFMT) — format functions used upstream; no direct calls
# required here except for any format lookups added by implementer.
# ============================================================================
# from PBBLNFMT import ...

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path(".")
DATA_DIR   = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PBIF_DATA_DIR = DATA_DIR / "pbif"      # PBIF.CLIEN<REPTYEAR><REPTMON><REPTDAY>
REPTDATE_PATH = DATA_DIR / "bnm" / "reptdate.parquet"

# ============================================================================
# CONSTANTS — calendar day counts (matches %DCLVAR RETAIN D1-D12 31 D4 D6 D9 D11 30)
# ============================================================================
_BASE_LDAY = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def lday_array(year: int) -> list[int]:
    """Return days-in-month array for given year, leap-year aware."""
    d = _BASE_LDAY.copy()
    if year % 4 == 0:
        d[1] = 29
    return d

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ============================================================================
# HELPER: %NXTBLDT macro (RDALPBIF variant)
# Advance MATDTE by FREQ months from DAY(MATDTE).
# ============================================================================
def next_matdte(matdte: date, freq: int) -> date:
    """
    %MACRO NXTBLDT (RDALPBIF version):
      DD = DAY(MATDTE)
      MM = MONTH(MATDTE) + FREQ
      YY = YEAR(MATDTE)
      IF MM > 12: MM -= 12; YY += 1
      End-of-month capping applied.
    Note: uses MATDTE.day (not ISSDTE) unlike the EIIBTLIQ variant.
    """
    dd = matdte.day
    mm = matdte.month + freq
    yy = matdte.year

    if mm > 12:
        mm -= 12
        yy += 1

    lday = lday_array(yy)
    if mm == 2 and yy % 4 == 0:
        lday[1] = 29
    if dd > lday[mm - 1]:
        dd = lday[mm - 1]

    try:
        return date(yy, mm, dd)
    except ValueError:
        return date(yy, mm, lday[mm - 1])


# ============================================================================
# CUSTFISS remapping (mirrors IF/ELSE IF chain in SAS)
# ============================================================================
def remap_custfiss(custfiss: int) -> int:
    if custfiss in (41, 42, 43, 66):   return 41
    if custfiss in (44, 47, 67):        return 44
    if custfiss in (46,):               return 46
    if custfiss in (48, 49, 51, 68):    return 48
    if custfiss in (52, 53, 54, 69):    return 52
    return custfiss


# ============================================================================
# STEP 1: Load and filter PBIF.CLIEN<REPTYEAR><REPTMON><REPTDAY>
# ============================================================================
def load_pbif(reptyear: str, reptmon: str, reptday: str) -> pl.DataFrame:
    """
    DATA PBIF; SET PBIF.CLIEN&REPTYEAR&REPTMON&REPTDAY;
      IF ENTITY='PBBH';
      IF FIU=0.00 AND PRMTHFIU=0.00 THEN DELETE;
      APPRLIMX = INLIMIT;
      PRODCD   = '30591';
      FISSPURP = '0470';
      AMTIND   = 'D';
      CUSTFISS remapping;
      CUSTCX   = CUSTFISS;
      BALANCE  = SUM(FIU, PRMTHFIU);
      UNDRAWN  = INLIMIT - BALANCE;
    PROC SORT; BY CLIENTNO;
    """
    pbif_path = PBIF_DATA_DIR / f"CLIEN{reptyear}{reptmon}{reptday}.parquet"
    df = pl.read_parquet(pbif_path)

    # Filter ENTITY='PBBH' and non-zero FIU or PRMTHFIU
    df = df.filter(
        (pl.col("ENTITY") == "PBBH") &
        ~((pl.col("FIU") == 0.0) & (pl.col("PRMTHFIU") == 0.0))
    )

    result_rows = []
    for r in df.to_dicts():
        fiu      = float(r.get("FIU",      0) or 0)
        prmthfiu = float(r.get("PRMTHFIU", 0) or 0)
        inlimit  = float(r.get("INLIMIT",  0) or 0)
        custcd   = int(r.get("CUSTCD",     0) or 0)

        custfiss = remap_custfiss(custcd)
        balance  = fiu + prmthfiu
        undrawn  = inlimit - balance

        r.update({
            "APPRLIMX": inlimit,
            "PRODCD":   "30591",
            "FISSPURP": "0470",
            "AMTIND":   "D",
            "CUSTFISS": custfiss,
            "CUSTCX":   custfiss,
            "BALANCE":  balance,
            "UNDRAWN":  undrawn,
        })
        result_rows.append(r)

    out = pl.DataFrame(result_rows) if result_rows else pl.DataFrame()
    return out.sort("CLIENTNO") if not out.is_empty() else out


# ============================================================================
# PROC PRINT equivalent (for diagnostic output to log)
# ============================================================================
def print_pbif(df: pl.DataFrame) -> None:
    """
    PROC PRINT; VAR BRANCH CLIENTNO BALANCE CUSTCX FISSPURP INLIMIT UNDRAWN SECTORCD ACCTNO;
    SUM BALANCE UNDRAWN;
    Outputs summary to log.
    """
    cols = ["BRANCH", "CLIENTNO", "BALANCE", "CUSTCX", "FISSPURP",
            "INLIMIT", "UNDRAWN", "SECTORCD", "ACCTNO"]
    available = [c for c in cols if c in df.columns]
    log.info("PBIF print (rows=%d):", len(df))
    log.info("  SUM BALANCE : %.2f", float(df["BALANCE"].sum()) if "BALANCE" in df.columns else 0)
    log.info("  SUM UNDRAWN : %.2f", float(df["UNDRAWN"].sum()) if "UNDRAWN" in df.columns else 0)


# ============================================================================
# STEP 2: Compute MATDTE via %NXTBLDT advance loop
# ============================================================================
def compute_matdte(df: pl.DataFrame, reptdate: date) -> pl.DataFrame:
    """
    DATA PBIF (DROP CUSTCD):
      %DCLVAR — initialise day arrays from REPTDATE.
      FREQ=6 (default); IF INLIMIT < 1000000.00 THEN FREQ=12.
      MATDTE = REPTDATE.
      IF STDATES > 0: MATDTE = STDATES; advance while MATDTE <= REPTDATE.
    PROC SORT NODUPKEY; BY CLIENTNO MATDTE;
    """
    result_rows = []
    for r in df.to_dicts():
        inlimit = float(r.get("INLIMIT",  0) or 0)
        stdates = r.get("STDATES")           # start date field

        # FREQ: 12 for small limits, 6 for large
        freq = 12 if inlimit < 1_000_000.0 else 6

        matdte = reptdate

        if stdates and stdates > date(1960, 1, 1):
            matdte = stdates
            # DO WHILE (MATDTE <= REPTDATE): %NXTBLDT
            safety = 0
            while matdte <= reptdate:
                matdte = next_matdte(matdte, freq)
                safety += 1
                if safety > 1000:
                    break   # guard against infinite loop

        # DROP CUSTCD
        r.pop("CUSTCD", None)
        r["MATDTE"] = matdte
        result_rows.append(r)

    out = pl.DataFrame(result_rows) if result_rows else pl.DataFrame()
    if not out.is_empty():
        out = out.unique(subset=["CLIENTNO", "MATDTE"], keep="first")\
                 .sort(["CLIENTNO", "MATDTE"])
    return out


# ============================================================================
# MAIN
# ============================================================================
def main(reptdate: Optional[date] = None,
         reptyear: Optional[str]  = None,
         reptmon:  Optional[str]  = None,
         reptday:  Optional[str]  = None) -> pl.DataFrame:
    """
    Entry point for RDALPBIF — called by parent program.
    If date parameters are not supplied, reads from REPTDATE_PATH.
    Returns enriched PBIF dataframe.
    """
    log.info("RDALPBIF started.")

    if reptdate is None:
        rd       = pl.read_parquet(REPTDATE_PATH).row(0, named=True)
        reptdate = rd["REPTDATE"]
        reptyear = reptdate.strftime("%y")     # YEAR2. — two-digit year
        reptmon  = f"{reptdate.month:02d}"
        reptday  = f"{reptdate.day:02d}"

    # ----------------------------------------------------------------
    # Load and filter PBIF
    # ----------------------------------------------------------------
    pbif = load_pbif(reptyear, reptmon, reptday)
    log.info("PBIF rows after filter: %d", len(pbif))

    # PROC PRINT diagnostic
    print_pbif(pbif)

    # ----------------------------------------------------------------
    # Compute MATDTE
    # ----------------------------------------------------------------
    pbif = compute_matdte(pbif, reptdate)
    log.info("PBIF rows after MATDTE computation: %d", len(pbif))

    # ----------------------------------------------------------------
    # Write output parquet
    # ----------------------------------------------------------------
    out_path = OUTPUT_DIR / f"PBIF{reptyear}{reptmon}{reptday}.parquet"
    pbif.write_parquet(out_path)
    log.info("PBIF written: %s", out_path)

    log.info("RDALPBIF completed.")
    return pbif


if __name__ == "__main__":
    main()
