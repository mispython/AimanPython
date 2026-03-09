#!/usr/bin/env python3
"""
Program  : LALBDBAL.py
Purpose  : Update BAL_AFT_EIR, BALANCE, BALMNI to WRITE_DOWN_BAL.
           ESMR 2011-3878, 2011-3795, 2011-3853, 2011-3893

           Reads  : BNM.LOAN<REPTMON><NOWK>  (parquet)
           Updates: BNM.LOAN<REPTMON><NOWK>  (parquet, in-place)

           Product ranges triggering write-down:
             OD: PRODUCT IN (30,31,32,33,34)
             LN: PRODUCT IN (600–609, 631–637, 650–699)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import logging
from datetime import date
from pathlib import Path

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import polars as pl

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path(".")
DATA_DIR   = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
REPTDATE_PATH = DATA_DIR / "bnm" / "reptdate.parquet"

# ============================================================================
# CONSTANTS
# ============================================================================
# OD products requiring write-down
OD_WRITEDOWN_PRODUCTS = {30, 31, 32, 33, 34}

# LN products requiring write-down (600–609, 631–637, 650–699)
LN_WRITEDOWN_PRODUCTS = (
    set(range(600, 610)) |
    set(range(631, 638)) |
    set(range(650, 700))
)

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
# MAIN PROCESSING FUNCTION
# ============================================================================
def apply_writedown(df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA BNM.LOAN&REPTMON&NOWK:
      Preserve original values as ORIBAL_AFT_EIR, ORIBALANCE, ORIBALMNI.
      For OD accounts with PRODUCT IN (30,31,32,33,34):
        set BAL_AFT_EIR = BALANCE = BALMNI = WRITE_DOWN_BAL
      For LN accounts with PRODUCT IN (600–609, 631–637, 650–699):
        set BAL_AFT_EIR = BALANCE = BALMNI = WRITE_DOWN_BAL
    """
    # Preserve originals
    df = df.with_columns([
        pl.col("BAL_AFT_EIR").alias("ORIBAL_AFT_EIR"),
        pl.col("BALANCE").alias("ORIBALANCE"),
        pl.col("BALMNI").alias("ORIBALMNI"),
    ])

    # OD write-down: PRODUCT IN (30,31,32,33,34) AND ACCTYPE='OD'
    od_mask = (
        pl.col("PRODUCT").is_in(list(OD_WRITEDOWN_PRODUCTS)) &
        (pl.col("ACCTYPE") == "OD")
    )

    # LN write-down: PRODUCT IN (600:609, 631:637, 650:699) AND ACCTYPE='LN'
    ln_mask = (
        pl.col("PRODUCT").is_in(list(LN_WRITEDOWN_PRODUCTS)) &
        (pl.col("ACCTYPE") == "LN")
    )

    writedown_mask = od_mask | ln_mask

    df = df.with_columns([
        pl.when(writedown_mask)
          .then(pl.col("WRITE_DOWN_BAL"))
          .otherwise(pl.col("BAL_AFT_EIR"))
          .alias("BAL_AFT_EIR"),
        pl.when(writedown_mask)
          .then(pl.col("WRITE_DOWN_BAL"))
          .otherwise(pl.col("BALANCE"))
          .alias("BALANCE"),
        pl.when(writedown_mask)
          .then(pl.col("WRITE_DOWN_BAL"))
          .otherwise(pl.col("BALMNI"))
          .alias("BALMNI"),
    ])

    return df


# ============================================================================
# MAIN
# ============================================================================
def main(reptmon: str = None, nowk: str = None) -> None:
    log.info("LALBDBAL started.")

    loan_path = OUTPUT_DIR / f"LOAN{reptmon}{nowk}.parquet"

    df = pl.read_parquet(loan_path)
    log.info("LOAN rows loaded: %d", len(df))

    df = apply_writedown(df)

    df.write_parquet(loan_path)
    log.info("LOAN written (write-down applied): %s", loan_path)

    log.info("LALBDBAL completed.")


if __name__ == "__main__":
    rd   = pl.read_parquet(REPTDATE_PATH).row(0, named=True)
    rdt  = rd["REPTDATE"]
    main(reptmon=f"{rdt.month:02d}", nowk=f"{rdt.day:02d}")
