#!/usr/bin/env python3
"""
Program  : LALWPBBU.py
Purpose  : Enrich BNM.ULOAN with COSTCTR via a cascading lookup chain,
           then summarise utilised OD balances and append to BNM.OD<REPTMON>.

           Reads  : BNM.ULOAN<REPTMON><NOWK>   (parquet)
                    BNM.LOAN<REPTMON><NOWK>     (parquet)
                    DEPOSIT.CURRENT             (parquet)
                    LOAN.LNNOTE                 (parquet)
                    LOAN.LNACCT                 (parquet)
           Updates: BNM.ULOAN<REPTMON><NOWK>    (parquet, COMPRESS=YES equiv.)
           Appends: BNM.OD<REPTMON>             (parquet)

           COSTCTR lookup cascade for ULOAN:
             UL1 (CA accts 3xxx): COSTCTR from DEPOSIT.CURRENT
             UL2 (others):
               → NOTE1 (LNNOTE dedup by ACCTNO, COSTCTR>0)  → UL5
               → NOTE  (LNNOTE by ACCTNO+COMMNO, COSTCTR>0) → UL4
               → LNACCT (by ACCTNO, COSTCTR>0)              → UL3
               → UL2   (no COSTCTR found — COSTCTR dropped)
           Final ULOAN = UL1 + UL2 + UL3 + UL4 + UL5

           OD summary:
             Filter BNM.LOAN: ACCTYPE='OD', BRANCH<998, valid product,
             WRITE_DOWN_BAL override for products 30–34, BALANCE>0.
             PROC SUMMARY by REPTDATE+PRODCD → SUM ODTOT/ODINDV/ODCORP.
             PROC APPEND to BNM.OD<REPTMON>.
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
BASE_DIR      = Path(".")
DATA_DIR      = BASE_DIR / "data"
OUTPUT_DIR    = BASE_DIR / "output"
REPTDATE_PATH = DATA_DIR / "bnm"     / "reptdate.parquet"
DEPOSIT_CURRENT_PATH = DATA_DIR / "deposit" / "current.parquet"
LNNOTE_PATH   = DATA_DIR / "loan"    / "lnnote.parquet"
LNACCT_PATH   = DATA_DIR / "loan"    / "lnacct.parquet"

# ============================================================================
# CONSTANTS
# ============================================================================
# Products excluded from OD summary
OD_EXCL_PRODUCTS = {
    104, 107, 126, 127, 128, 129, 130,
    475, 476, 477, 478, 479, 473, 474,
    136, 139, 140, 141, 142, 143, 144, 145,
    146, 147, 148, 149, 171, 172, 173, 549, 550,
}

# OD products requiring WRITE_DOWN_BAL override in summary
OD_WRITEDOWN_PRODUCTS = {30, 31, 32, 33, 34}

# Individual customer codes
INDV_CUSTCDS = {'77', '78', '95', '96'}

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
# STEP 1: COSTCTR enrichment cascade for BNM.ULOAN
# ============================================================================
def enrich_uloan_costctr(uloan_df: pl.DataFrame) -> pl.DataFrame:
    """
    Cascading COSTCTR lookup for ULOAN records:

      UL1 (3000000000 < ACCTNO < 3999999999):
        COSTCTR from DEPOSIT.CURRENT by ACCTNO.

      UL2 (all others, DROP COSTCTR initially):
        Step 1 — match NOTE1 (LNNOTE dedup by ACCTNO, COSTCTR>0):
          COSTCTR found → UL5; not found → remain UL2
        Step 2 — match NOTE (LNNOTE by ACCTNO+COMMNO, COSTCTR>0):
          COSTCTR found → UL4; not found → remain UL2
        Step 3 — match LNACCT (by ACCTNO, COSTCTR>0):
          COSTCTR found → UL3; not found → remain UL2 (COSTCTR dropped)

      Final: SET UL1 UL2 UL3 UL4 UL5
    """
    # Split UL1 / UL2
    ul1 = uloan_df.filter(
        (pl.col("ACCTNO") > 3_000_000_000) & (pl.col("ACCTNO") < 3_999_999_999)
    ).drop("COSTCTR", strict=False)

    ul2 = uloan_df.filter(
        ~((pl.col("ACCTNO") > 3_000_000_000) & (pl.col("ACCTNO") < 3_999_999_999))
    ).drop("COSTCTR", strict=False)

    # ---- UL1: COSTCTR from DEPOSIT.CURRENT ----
    dep_costctr = (
        pl.read_parquet(DEPOSIT_CURRENT_PATH)
          .select(["ACCTNO", "COSTCTR"])
          .sort("ACCTNO")
    )
    ul1 = ul1.sort("ACCTNO").join(dep_costctr, on="ACCTNO", how="left")

    # ---- NOTE1: LNNOTE deduplicated by ACCTNO (COSTCTR = ACCOSTCT, COSTCTR > 0) ----
    lnnote_raw = pl.read_parquet(LNNOTE_PATH)
    note = (
        lnnote_raw.with_columns(
            pl.col("ACCOSTCT").alias("COSTCTR")
        )
        .filter(pl.col("COSTCTR") > 0)
        .select(["ACCTNO", "COMMNO", "COSTCTR"])
    )
    note1 = (
        note.drop("COMMNO")
            .unique(subset=["ACCTNO"], keep="first")
            .sort("ACCTNO")
    )

    # UL2 Step 1: merge with NOTE1 by ACCTNO
    ul2_sorted = ul2.sort("ACCTNO")
    ul2_merged1 = ul2_sorted.join(note1, on="ACCTNO", how="left")
    ul5 = ul2_merged1.filter(pl.col("COSTCTR").is_not_null() & (pl.col("COSTCTR") > 0))
    ul2 = ul2_merged1.filter(~(pl.col("COSTCTR").is_not_null() & (pl.col("COSTCTR") > 0)))\
                     .drop("COSTCTR", strict=False)

    # UL2 Step 2: merge with NOTE by ACCTNO+COMMNO
    note_sorted = note.sort(["ACCTNO", "COMMNO"])
    ul2_sorted2 = ul2.sort(["ACCTNO", "COMMNO"])
    ul2_merged2 = ul2_sorted2.join(note_sorted, on=["ACCTNO", "COMMNO"], how="left")
    ul4 = ul2_merged2.filter(pl.col("COSTCTR").is_not_null() & (pl.col("COSTCTR") > 0))
    ul2 = ul2_merged2.filter(~(pl.col("COSTCTR").is_not_null() & (pl.col("COSTCTR") > 0)))\
                     .drop("COSTCTR", strict=False)

    # UL2 Step 3: merge with LNACCT by ACCTNO (COSTCTR > 0)
    lnacct = (
        pl.read_parquet(LNACCT_PATH)
          .filter(pl.col("COSTCTR") > 0)
          .select(["ACCTNO", "COSTCTR"])
          .sort("ACCTNO")
    )
    ul2_sorted3 = ul2.sort("ACCTNO")
    ul2_merged3 = lnacct.join(ul2_sorted3, on="ACCTNO", how="right", suffix="_LN")
    # IF A (LNACCT) matches → UL3, else UL2 (COSTCTR dropped)
    ul3 = ul2_merged3.filter(pl.col("COSTCTR").is_not_null() & (pl.col("COSTCTR") > 0))
    ul2 = ul2_merged3.filter(~(pl.col("COSTCTR").is_not_null() & (pl.col("COSTCTR") > 0)))\
                     .drop("COSTCTR", strict=False)

    # ---- Combine: SET UL1 UL2 UL3 UL4 UL5 ----
    parts = [df for df in [ul1, ul2, ul3, ul4, ul5] if not df.is_empty()]
    if not parts:
        return pl.DataFrame()
    return pl.concat(parts, how="diagonal")


# ============================================================================
# STEP 2: OD balance summary and append to BNM.OD<REPTMON>
# ============================================================================
def summarise_od(loan_df: pl.DataFrame, tdate: int, reptmon: str) -> None:
    """
    Filter BNM.LOAN for utilised OD accounts (ACCTYPE='OD', BRANCH<998).
    Apply WRITE_DOWN_BAL for products 30–34.
    Exclude specific products and keep only BALANCE > 0.
    Classify ODINDV / ODCORP by CUSTCD.
    PROC SUMMARY by REPTDATE + PRODCD → SUM ODTOT / ODINDV / ODCORP.
    PROC APPEND to BNM.OD<REPTMON> (parquet, append mode).
    """
    od = loan_df.filter(
        (pl.col("ACCTYPE") == "OD") & (pl.col("BRANCH") < 998)
    )

    # WRITE_DOWN_BAL override for products 30–34
    od = od.with_columns(
        pl.when(pl.col("PRODUCT").is_in(list(OD_WRITEDOWN_PRODUCTS)))
          .then(pl.col("WRITE_DOWN_BAL"))
          .otherwise(pl.col("BALANCE"))
          .alias("BALANCE")
    )

    # Filter: product NOT in exclusion list AND BALANCE > 0
    od = od.filter(
        ~pl.col("PRODUCT").is_in(list(OD_EXCL_PRODUCTS)) &
        (pl.col("BALANCE") > 0)
    )

    # REPTDATE = &TDATE (SAS integer date as integer)
    od = od.with_columns(pl.lit(tdate).alias("REPTDATE"))

    # ODTOT, ODINDV, ODCORP
    od = od.with_columns([
        pl.col("BALANCE").alias("ODTOT"),
        pl.when(pl.col("CUSTCD").is_in(list(INDV_CUSTCDS)))
          .then(pl.col("BALANCE"))
          .otherwise(pl.lit(0.0))
          .alias("ODINDV"),
        pl.when(~pl.col("CUSTCD").is_in(list(INDV_CUSTCDS)))
          .then(pl.col("BALANCE"))
          .otherwise(pl.lit(0.0))
          .alias("ODCORP"),
    ])

    # PROC SUMMARY NWAY by REPTDATE + PRODCD
    od_summary = (
        od.group_by(["REPTDATE", "PRODCD"])
          .agg([
              pl.col("ODTOT").sum(),
              pl.col("ODINDV").sum(),
              pl.col("ODCORP").sum(),
          ])
          .sort(["REPTDATE", "PRODCD"])
    )
    log.info("OD summary rows: %d", len(od_summary))

    # PROC APPEND: append to BNM.OD<REPTMON>
    od_base_path = OUTPUT_DIR / f"OD{reptmon}.parquet"
    if od_base_path.exists():
        existing = pl.read_parquet(od_base_path)
        combined = pl.concat([existing, od_summary], how="diagonal")
    else:
        combined = od_summary
    combined.write_parquet(od_base_path)
    log.info("OD summary appended to: %s", od_base_path)


# ============================================================================
# MAIN
# ============================================================================
def main(reptmon: str = None, nowk: str = None, tdate: int = None) -> None:
    log.info("LALWPBBU started.")

    uloan_path = OUTPUT_DIR / f"ULOAN{reptmon}{nowk}.parquet"
    loan_path  = OUTPUT_DIR / f"LOAN{reptmon}{nowk}.parquet"

    # ----------------------------------------------------------------
    # Load BNM.ULOAN
    # ----------------------------------------------------------------
    uloan_df = pl.read_parquet(uloan_path)
    log.info("ULOAN rows loaded: %d", len(uloan_df))

    # ----------------------------------------------------------------
    # COSTCTR enrichment cascade
    # ----------------------------------------------------------------
    uloan_df = enrich_uloan_costctr(uloan_df)
    log.info("ULOAN rows after COSTCTR enrichment: %d", len(uloan_df))

    # ----------------------------------------------------------------
    # Write BNM.ULOAN (COMPRESS=YES → zstd)
    # ----------------------------------------------------------------
    uloan_df.write_parquet(uloan_path, compression="zstd")
    log.info("ULOAN written: %s", uloan_path)

    # ----------------------------------------------------------------
    # OD summary → PROC APPEND to BNM.OD<REPTMON>
    # ----------------------------------------------------------------
    loan_df = pl.read_parquet(loan_path)
    log.info("LOAN rows loaded: %d", len(loan_df))

    summarise_od(loan_df, tdate, reptmon)

    log.info("LALWPBBU completed.")


if __name__ == "__main__":
    rd      = pl.read_parquet(REPTDATE_PATH).row(0, named=True)
    rdt     = rd["REPTDATE"]
    reptmon = f"{rdt.month:02d}"
    nowk    = f"{rdt.day:02d}"
    # SAS date integer: days since 01-JAN-1960
    tdate   = (rdt - date(1960, 1, 1)).days

    main(reptmon=reptmon, nowk=nowk, tdate=tdate)
