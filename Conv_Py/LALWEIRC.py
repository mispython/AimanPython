#!/usr/bin/env python3
"""
Program  : LALWEIRC.py
Purpose  : Merge EIR adjustments into BNM.LOAN, compute BAL_AFT_EIR,
           then apply WRITE_DOWN_BAL overrides.

           Reads  : EIR.EIR<REPTMON><REPTYR>   (parquet)
                    LOAN.LNNOTE                 (parquet, KEEP subset)
                    BNM.LOAN<REPTMON><NOWK>     (parquet)
           Updates: BNM.LOAN<REPTMON><NOWK>     (parquet, COMPRESS=YES equiv.)

           Logic:
             BAL_AFT_EIR = BALANCE + EIR_ADJ
             If EIR_ADJ not null and PAIDIND in ('P','C'): BAL_AFT_EIR = EIR_ADJ
             If EIR_ADJ not null and APPRLIM2 null: APPRLIM2 = APPREIR (from EIR)
             EIRIND=1 when EIR record exists without matching LOAN, or PAIDIND='P'/'C'
               → REMAINMH=0, REMMTH=0, REMAINMT='51'
           Then apply WRITE_DOWN_BAL overrides:
             OD: PRODUCT IN (30–34)
             LN: PRODUCT IN (600–699)
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
REPTDATE_PATH = DATA_DIR / "bnm"  / "reptdate.parquet"
LNNOTE_PATH   = DATA_DIR / "loan" / "lnnote.parquet"

# EIR input path — parameterised by REPTMON + REPTYR at runtime
# EIR.EIR<REPTMON><REPTYR>  e.g. EIR0124.parquet
EIR_DATA_DIR  = DATA_DIR / "eir"

# ============================================================================
# CONSTANTS
# ============================================================================
# OD write-down products
OD_WRITEDOWN_PRODUCTS = {30, 31, 32, 33, 34}

# LN write-down products (600–699) — note: broader range than LALBDBAL
LN_WRITEDOWN_PRODUCTS = set(range(600, 700))

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
# STEP 1: Load and prepare EIR dataset
# PROC SORT DATA=EIR.EIR&REPTMON&REPTYR OUT=EIR; BY ACCTNO NOTENO;
# DATA EIR: MERGE EIR + LOAN.LNNOTE (PAIDIND, NTINDEX, DNBFISME); IF B;
# ============================================================================
def load_eir(reptmon: str, reptyr: str) -> pl.DataFrame:
    """
    Load EIR.<REPTMON><REPTYR>, merge with LOAN.LNNOTE to attach
    PAIDIND, NTINDEX, DNBFISME. Retain all EIR rows (IF B).
    DNBFI_ORI = DNBFISME.
    """
    eir_path = EIR_DATA_DIR / f"EIR{reptmon}{reptyr}.parquet"
    eir_df   = pl.read_parquet(eir_path).sort(["ACCTNO", "NOTENO"])

    lnnote_df = (
        pl.read_parquet(LNNOTE_PATH)
          .select(["ACCTNO", "NOTENO", "PAIDIND", "NTINDEX", "DNBFISME"])
          .sort(["ACCTNO", "NOTENO"])
    )

    # Left merge: keep all EIR rows (IF B — EIR is the primary)
    merged = eir_df.join(lnnote_df, on=["ACCTNO", "NOTENO"], how="left", suffix="_LN")

    # DNBFI_ORI = DNBFISME
    merged = merged.with_columns(
        pl.col("DNBFISME").alias("DNBFI_ORI")
    )

    return merged


# ============================================================================
# STEP 2: Merge EIR into BNM.LOAN, compute BAL_AFT_EIR, EIRIND, REMAINMT
# DATA BNM.LOAN(DROP=APPREIR):
#   MERGE EIR(IN=B RENAME=(APPRLIM2=APPREIR)) BNM.LOAN(IN=A);
#   BY ACCTNO NOTENO;
# ============================================================================
def merge_eir_into_loan(loan_df: pl.DataFrame, eir_df: pl.DataFrame) -> pl.DataFrame:
    """
    Full outer merge of EIR (renamed APPRLIM2→APPREIR) with BNM.LOAN by ACCTNO+NOTENO.
    Computes BAL_AFT_EIR, EIRIND, and resets REMAINMH/REMMTH/REMAINMT for EIRIND=1.
    """
    # Rename EIR.APPRLIM2 → APPREIR before merge
    eir_renamed = eir_df.rename({"APPRLIM2": "APPREIR"}) if "APPRLIM2" in eir_df.columns \
                  else eir_df.with_columns(pl.lit(None).cast(pl.Float64).alias("APPREIR"))

    # Full outer join — need both IN=A and IN=B flags
    merged = loan_df.join(
        eir_renamed, on=["ACCTNO", "NOTENO"], how="full", suffix="_EIR"
    )

    result_rows = []
    for r in merged.to_dicts():
        in_a = r.get("BALANCE") is not None           # present in LOAN (IN=A)
        in_b = r.get("EIR_ADJ") is not None           # present in EIR  (IN=B)

        if not in_b:
            continue   # IF B — only keep rows where EIR record exists

        balance  = float(r.get("BALANCE",  0) or 0)
        eir_adj  = r.get("EIR_ADJ")
        paidind  = str(r.get("PAIDIND", "") or "")
        apprlim2 = r.get("APPRLIM2")
        appreir  = r.get("APPREIR")

        # BAL_AFT_EIR = BALANCE + EIR_ADJ
        bal_aft_eir = balance + (float(eir_adj) if eir_adj is not None else 0.0)

        # If EIR_ADJ not null and PAIDIND in ('P','C'): BAL_AFT_EIR = EIR_ADJ
        if eir_adj is not None and paidind in ("P", "C"):
            bal_aft_eir = float(eir_adj)

        # If EIR_ADJ not null and APPRLIM2 null: APPRLIM2 = APPREIR
        if eir_adj is not None and (apprlim2 is None):
            apprlim2 = appreir

        # EIRIND flag
        eirind = 0
        if in_b and not in_a:
            eirind = 1
        if in_b and in_a and paidind in ("P", "C"):
            eirind = 1

        # EIRIND=1 → reset remaining maturity
        remainmh = r.get("REMAINMH", 0) or 0
        remmth   = r.get("REMMTH",   0) or 0
        remainmt = str(r.get("REMAINMT", "") or "")

        if eirind == 1:
            remainmh = 0
            remmth   = 0
            remainmt = "51"

        r.update({
            "BAL_AFT_EIR": bal_aft_eir,
            "APPRLIM2":    apprlim2,
            "EIRIND":      eirind,
            "REMAINMH":    remainmh,
            "REMMTH":      remmth,
            "REMAINMT":    remainmt,
        })
        # Drop APPREIR (mirrors DROP=APPREIR)
        r.pop("APPREIR", None)
        result_rows.append(r)

    return pl.DataFrame(result_rows) if result_rows else pl.DataFrame()


# ============================================================================
# STEP 3: Apply WRITE_DOWN_BAL overrides
# DATA BNM.LOAN(COMPRESS=YES):
#   Preserve ORI* fields; override BAL_AFT_EIR/BALANCE/BALMNI for
#   OD products (30–34) and LN products (600–699).
# ============================================================================
def apply_writedown(df: pl.DataFrame) -> pl.DataFrame:
    """
    Preserve originals then apply WRITE_DOWN_BAL overrides.
    OD: PRODUCT IN (30,31,32,33,34)
    LN: PRODUCT IN (600–699)  ← note: full range 600–699 (broader than LALBDBAL)
    """
    df = df.with_columns([
        pl.col("BAL_AFT_EIR").alias("ORIBAL_AFT_EIR"),
        pl.col("BALANCE").alias("ORIBALANCE"),
        pl.col("BALMNI").alias("ORIBALMNI"),
    ])

    od_mask = (
        pl.col("PRODUCT").is_in(list(OD_WRITEDOWN_PRODUCTS)) &
        (pl.col("ACCTYPE") == "OD")
    )
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
def main(reptmon: str = None, nowk: str = None, reptyr: str = None) -> None:
    log.info("LALWEIRC started.")

    loan_path = OUTPUT_DIR / f"LOAN{reptmon}{nowk}.parquet"

    # ----------------------------------------------------------------
    # Load EIR and merge with LNNOTE
    # ----------------------------------------------------------------
    eir_df = load_eir(reptmon, reptyr)
    log.info("EIR rows loaded: %d", len(eir_df))

    # ----------------------------------------------------------------
    # Load BNM.LOAN
    # ----------------------------------------------------------------
    loan_df = pl.read_parquet(loan_path).sort(["ACCTNO", "NOTENO"])
    log.info("LOAN rows loaded: %d", len(loan_df))

    # ----------------------------------------------------------------
    # Merge EIR → LOAN, compute BAL_AFT_EIR / EIRIND
    # ----------------------------------------------------------------
    loan_df = merge_eir_into_loan(loan_df, eir_df)
    log.info("LOAN rows after EIR merge: %d", len(loan_df))

    # ----------------------------------------------------------------
    # Apply WRITE_DOWN_BAL overrides
    # ----------------------------------------------------------------
    loan_df = apply_writedown(loan_df)

    # ----------------------------------------------------------------
    # Write output (COMPRESS=YES equivalent — parquet is compressed)
    # ----------------------------------------------------------------
    loan_df.write_parquet(loan_path, compression="zstd")
    log.info("LOAN written (EIR + write-down applied): %s", loan_path)

    log.info("LALWEIRC completed.")


if __name__ == "__main__":
    rd      = pl.read_parquet(REPTDATE_PATH).row(0, named=True)
    rdt     = rd["REPTDATE"]
    reptmon = f"{rdt.month:02d}"
    nowk    = f"{rdt.day:02d}"
    reptyr  = str(rdt.year)

    main(reptmon=reptmon, nowk=nowk, reptyr=reptyr)
