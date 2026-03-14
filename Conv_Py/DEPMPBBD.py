#!/usr/bin/env python3
"""
Program : DEPMPBBD.py
Purpose : To prepare MIS data files — Deposit
Date    : 26/05/97

Sections:
  1. SDRNGE  — Saving deposit by branch, race, product, range, age
  2. SDPAGE  — Saving deposit by statecd, branch, product, age
  3. DDRNGE  — Demand deposit by purpose, race, custcd, range, avgrnge
  4. DPPOSN  — Deposit position (saving + current) as at the month
  5. DPACCT  — Accounts opened/closed in the month (saving + current)
  6. SDMVNT  — Saving deposit movement >= 1 million (increases + decreases)
  7. DDMVNT  — Current account movement >= 1 million (absolute change)
  8. CAPUR   — Current account by branch, purpose, deptype, product
  9. MTHACE  — ACE (Automated Cash Enhancement) by branch

Input libraries (parquet):
  BNM.SAVG&REPTMON&NOWK       -> input/BNM/SAVG<REPTMON><NOWK>.parquet
  BNM.CURN&REPTMON&NOWK       -> input/BNM/CURN<REPTMON><NOWK>.parquet
  DEPOSIT.SAVING               -> input/DEPOSIT/SAVING.parquet
  DEPOSIT.CURRENT              -> input/DEPOSIT/CURRENT.parquet
  LMDEPT.SAVING                -> input/LMDEPT/SAVING.parquet   (last month)
  LMDEPT.CURRENT               -> input/LMDEPT/CURRENT.parquet  (last month)

Output library (parquet, append-semantics):
  MIS.SDRNGE<REPTMON>          -> output/MIS/SDRNGE<REPTMON>.parquet
  MIS.SDPAGE<REPTMON>          -> output/MIS/SDPAGE<REPTMON>.parquet
  MIS.DDRNGE<REPTMON>          -> output/MIS/DDRNGE<REPTMON>.parquet
  MIS.DPPOSN<REPTMON>          -> output/MIS/DPPOSN<REPTMON>.parquet
  MIS.DPACCT<REPTMON>          -> output/MIS/DPACCT<REPTMON>.parquet
  MIS.SDMVNT<REPTMON>          -> output/MIS/SDMVNT<REPTMON>.parquet
  MIS.DDMVNT<REPTMON>          -> output/MIS/DDMVNT<REPTMON>.parquet
  MIS.CAPUR<REPTMON>           -> output/MIS/CAPUR<REPTMON>.parquet
  MIS.MTHACE<REPTMON>          -> output/MIS/MTHACE<REPTMON>.parquet

Note: PROC APPEND semantics — each section overwrites the MIS target
        (matching 'PROC DATASETS DELETE' then 'PROC APPEND' pattern).
      Since DELETE precedes each PROC APPEND in the SAS source, the
        Python equivalent simply writes (overwrites) each output file.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Path / macro setup
# ---------------------------------------------------------------------------
BASE_DIR    = Path(__file__).resolve().parent
INPUT_DIR   = BASE_DIR / "input"
OUTPUT_DIR  = BASE_DIR / "output"
MIS_DIR     = OUTPUT_DIR / "MIS"

for d in [INPUT_DIR, MIS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Runtime macro variables — set by orchestrator or overridden here
REPTMON: str = "09"   # e.g. "09"
NOWK:    str = "4"    # e.g. "4"
RDATE:   str = "30/09/05"  # DDMMYY8. format (used in LNMHPBBD sibling; not needed here)

# Derived input paths
BNM_DIR     = INPUT_DIR / "BNM"
DEPOSIT_DIR = INPUT_DIR / "DEPOSIT"
LMDEPT_DIR  = INPUT_DIR / "LMDEPT"


def _savg_path() -> Path:
    return BNM_DIR / f"SAVG{REPTMON}{NOWK}.parquet"

def _curn_path() -> Path:
    return BNM_DIR / f"CURN{REPTMON}{NOWK}.parquet"

def _mis_path(name: str) -> Path:
    return MIS_DIR / f"{name}{REPTMON}.parquet"

def _read(path: Path) -> pl.DataFrame:
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    return df

def _write(df: pl.DataFrame, path: Path) -> None:
    """Write (overwrite) a parquet MIS output file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    print(f"  -> {path}  ({df.height} rows)")


# ---------------------------------------------------------------------------
# Helper: PROC SUMMARY NWAY equivalent
#   Groups by all CLASS vars, aggregates VAR columns.
#   _FREQ_ is renamed to NOACCT (or similar) via `freq_name`.
# ---------------------------------------------------------------------------
def _summary_nway(
    df: pl.DataFrame,
    class_cols: list[str],
    var_exprs: dict,           # {output_col: polars_expr}
    freq_name: str | None = "NOACCT",
) -> pl.DataFrame:
    """
    PROC SUMMARY DATA=... NWAY;
    CLASS <class_cols>;
    VAR   ...;
    OUTPUT OUT=... (RENAME=(_FREQ_=<freq_name>) DROP=_TYPE_) <aggrs>;

    var_exprs is a dict mapping output column name -> polars aggregation expr.
    freq_name=None means DROP=_FREQ_ (no count column emitted).
    """
    agg_exprs = list(var_exprs.values())
    if freq_name is not None:
        agg_exprs = [pl.len().alias(freq_name)] + agg_exprs

    # Rename keys to match output column names
    named_exprs = (
        [pl.len().alias(freq_name)] if freq_name else []
    ) + [
        expr.alias(col) for col, expr in var_exprs.items()
    ]

    return df.group_by(class_cols).agg(named_exprs)


# ---------------------------------------------------------------------------
# 1. SDRNGE — Saving deposit by BRANCH RACE PRODCD PRODUCT RANGE AGE ACCYTD R2NGE
# ---------------------------------------------------------------------------
def run_sdrnge() -> None:
    """
    PROC SUMMARY DATA=BNM.SAVG&REPTMON&NOWK NWAY;
    CLASS BRANCH RACE PRODCD PRODUCT RANGE AGE ACCYTD R2NGE;
    VAR CURBAL;
    OUTPUT OUT=SDRNGE (RENAME=(_FREQ_=NOACCT) DROP=_TYPE_) SUM=CURBAL;

    PROC APPEND DATA=SDRNGE BASE=MIS.SDRNGE&REPTMON;
    """
    print("[SDRNGE] Saving deposit by branch/race/product/range/age")
    df = _read(_savg_path())
    class_cols = ["BRANCH", "RACE", "PRODCD", "PRODUCT", "RANGE", "AGE", "ACCYTD", "R2NGE"]
    out = _summary_nway(
        df, class_cols,
        {"CURBAL": pl.col("CURBAL").sum()},
        freq_name="NOACCT",
    )
    _write(out, _mis_path("SDRNGE"))


# ---------------------------------------------------------------------------
# 2. SDPAGE — Saving deposit by STATECD BRANCH PRODUCT AGE
# ---------------------------------------------------------------------------
def run_sdpage() -> None:
    """
    PROC SUMMARY DATA=BNM.SAVG&REPTMON&NOWK NWAY;
    CLASS STATECD BRANCH PRODUCT AGE;
    VAR CURBAL;
    OUTPUT OUT=SDPAGE (RENAME=(_FREQ_=NOACCT) DROP=_TYPE_) SUM=CURBAL;

    PROC APPEND DATA=SDPAGE BASE=MIS.SDPAGE&REPTMON;
    """
    print("[SDPAGE] Saving deposit by statecd/branch/product/age")
    df = _read(_savg_path())
    class_cols = ["STATECD", "BRANCH", "PRODUCT", "AGE"]
    out = _summary_nway(
        df, class_cols,
        {"CURBAL": pl.col("CURBAL").sum()},
        freq_name="NOACCT",
    )
    _write(out, _mis_path("SDPAGE"))


# ---------------------------------------------------------------------------
# 3. DDRNGE — Demand deposit by PURPOSE RACE CUSTCD AVGRNGE RANGE PRODUCT
# ---------------------------------------------------------------------------
def run_ddrnge() -> None:
    """
    PROC SUMMARY DATA=BNM.CURN&REPTMON&NOWK NWAY;
    CLASS PURPOSE RACE CUSTCD AVGRNGE RANGE PRODUCT;
    VAR AVGAMT CURBAL ACCYTD;
    OUTPUT OUT=DDRNGE (DROP=_FREQ_ _TYPE_)
           N(AVGAMT)=AVGACCT SUM(AVGAMT)=AVGAMT
                             SUM(ACCYTD)=ACCYTD
           N(CURBAL)=NOACCT  SUM(CURBAL)=CURBAL;

    Note: _FREQ_ is dropped (no freq_name). Two separate N() counts are
    needed (AVGAMT -> AVGACCT, CURBAL -> NOACCT). Polars count_not_null
    is used to replicate SAS N() which counts non-missing values.
    """
    print("[DDRNGE] Demand deposit by purpose/race/custcd/range/avgrnge")
    df = _read(_curn_path())
    class_cols = ["PURPOSE", "RACE", "CUSTCD", "AVGRNGE", "RANGE", "PRODUCT"]
    out = df.group_by(class_cols).agg([
        pl.col("AVGAMT").count().alias("AVGACCT"),
        pl.col("AVGAMT").sum().alias("AVGAMT"),
        pl.col("ACCYTD").sum().alias("ACCYTD"),
        pl.col("CURBAL").count().alias("NOACCT"),
        pl.col("CURBAL").sum().alias("CURBAL"),
    ])
    _write(out, _mis_path("DDRNGE"))


# ---------------------------------------------------------------------------
# 4. DPPOSN — Deposit position (saving + current appended)
# ---------------------------------------------------------------------------
def run_dpposn() -> None:
    """
    Two PROC SUMMARYs (SAVG then CURN), both appended to MIS.DPPOSN&REPTMON.
    CLASS BRANCH DEPTYPE PRODCD PRODUCT; VAR CURBAL;

    SAS PROC APPEND appends; since the first block does DELETE then APPEND,
    and the second block does only APPEND, we union both summaries into one
    output file.
    """
    print("[DPPOSN] Deposit position (saving + current)")
    class_cols = ["BRANCH", "DEPTYPE", "PRODCD", "PRODUCT"]

    savg = _read(_savg_path())
    savg_out = _summary_nway(
        savg, class_cols,
        {"CURBAL": pl.col("CURBAL").sum()},
        freq_name="NOACCT",
    )

    curn = _read(_curn_path())
    curn_out = _summary_nway(
        curn, class_cols,
        {"CURBAL": pl.col("CURBAL").sum()},
        freq_name="NOACCT",
    )

    out = pl.concat([savg_out, curn_out], how="diagonal")
    _write(out, _mis_path("DPPOSN"))


# ---------------------------------------------------------------------------
# 5. DPACCT — Accounts opened/closed (saving + current appended)
# ---------------------------------------------------------------------------
def run_dpacct() -> None:
    """
    Two PROC SUMMARYs: DEPOSIT.SAVING then DEPOSIT.CURRENT.
    CLASS BRANCH DEPTYPE PRODUCT; VAR OPENMH CLOSEMH;
    OUTPUT: RENAME(_FREQ_=NOACCT) DROP=_TYPE_, SUM=OPENMH CLOSEMH.
    """
    print("[DPACCT] Accounts opened/closed in the month")
    class_cols = ["BRANCH", "DEPTYPE", "PRODUCT"]

    saving  = _read(DEPOSIT_DIR / "SAVING.parquet")
    sav_out = _summary_nway(
        saving, class_cols,
        {
            "OPENMH":  pl.col("OPENMH").sum(),
            "CLOSEMH": pl.col("CLOSEMH").sum(),
        },
        freq_name="NOACCT",
    )

    current  = _read(DEPOSIT_DIR / "CURRENT.parquet")
    curr_out = _summary_nway(
        current, class_cols,
        {
            "OPENMH":  pl.col("OPENMH").sum(),
            "CLOSEMH": pl.col("CLOSEMH").sum(),
        },
        freq_name="NOACCT",
    )

    out = pl.concat([sav_out, curr_out], how="diagonal")
    _write(out, _mis_path("DPACCT"))


# ---------------------------------------------------------------------------
# 6. SDMVNT — Saving deposit movement >= 1 million
# ---------------------------------------------------------------------------
def run_sdmvnt() -> None:
    """
    Section A: Increases >= 1M
        THISMTH = DEPOSIT.SAVING WHERE CURBAL >= 1,000,000  (keyed on ACCTNO)
        SDMOVEMT = LMDEPT.SAVING joined to THISMTH
                   WHERE (NEWBAL - OLDBAL) >= 1,000,000

    Section B: Decreases >= 1M
        LASTMTH = LMDEPT.SAVING WHERE CURBAL >= 1,000,000  (keyed on ACCTNO)
        SDMOVEMT = DEPOSIT.SAVING joined to LASTMTH
                   WHERE (OLDBAL - NEWBAL) >= 1,000,000

    Both are appended to MIS.SDMVNT&REPTMON.

    The SAS SET ... KEY= /UNIQUE join is reproduced as an inner join
    (IF _IORC_ EQ 0 means a matching record was found).
    """
    print("[SDMVNT] Saving deposit movement >= 1 million")

    # ----- Section A: Increases -----
    dep_saving = _read(DEPOSIT_DIR / "SAVING.parquet") \
        .select(["ACCTNO", "BRANCH", "NAME", "CURBAL"]) \
        .filter(pl.col("CURBAL") >= 1_000_000) \
        .rename({"CURBAL": "NEWBAL"})

    lm_saving  = _read(LMDEPT_DIR / "SAVING.parquet") \
        .select(["ACCTNO", "BRANCH", "CURBAL"]) \
        .rename({"CURBAL": "OLDBAL"})

    # SET LMDEPT.SAVING ... SET THISMTH KEY=ACCTNO/UNIQUE -> inner join
    inc = lm_saving.join(dep_saving, on="ACCTNO", how="inner", suffix="_new") \
        .filter((pl.col("NEWBAL") - pl.col("OLDBAL")) >= 1_000_000)

    # Resolve BRANCH column (left side has BRANCH from LMDEPT, right side has BRANCH_new)
    inc = inc.select(["ACCTNO",
                      pl.col("BRANCH"),
                      "NAME", "OLDBAL", "NEWBAL"])

    # ----- Section B: Decreases -----
    lm_saving2 = _read(LMDEPT_DIR / "SAVING.parquet") \
        .select(["ACCTNO", "BRANCH", "CURBAL"]) \
        .filter(pl.col("CURBAL") >= 1_000_000) \
        .rename({"CURBAL": "OLDBAL"})

    dep_saving2 = _read(DEPOSIT_DIR / "SAVING.parquet") \
        .select(["ACCTNO", "BRANCH", "NAME", "CURBAL"]) \
        .rename({"CURBAL": "NEWBAL"})

    dec = dep_saving2.join(lm_saving2, on="ACCTNO", how="inner", suffix="_old") \
        .filter((pl.col("OLDBAL") - pl.col("NEWBAL")) >= 1_000_000)

    dec = dec.select(["ACCTNO",
                      pl.col("BRANCH"),
                      "NAME", "OLDBAL", "NEWBAL"])

    out = pl.concat([inc, dec], how="diagonal")
    _write(out, _mis_path("SDMVNT"))


# ---------------------------------------------------------------------------
# 7. DDMVNT — Current account movement >= 1 million (absolute change)
# ---------------------------------------------------------------------------
def run_ddmvnt() -> None:
    """
    PROC SORT LMDEPT.CURRENT -> LASTMTH (RENAME CURBAL=OLDBAL)
    PROC SORT DEPOSIT.CURRENT -> THISMTH (RENAME CURBAL=NEWBAL)
    DATA DDMOVEMT;
      MERGE LASTMTH THISMTH; BY ACCTNO;
      IF ABS(NEWBAL - OLDBAL) >= 1,000,000;

    Full outer merge (MERGE without IN= conditions keeps all ACCTNO).
    """
    print("[DDMVNT] Current account movement >= 1 million")

    lastmth = _read(LMDEPT_DIR / "CURRENT.parquet") \
        .select(["ACCTNO", "BRANCH", "CURBAL"]) \
        .rename({"CURBAL": "OLDBAL"})

    thismth = _read(DEPOSIT_DIR / "CURRENT.parquet") \
        .select(["ACCTNO", "BRANCH", "NAME", "CURBAL"]) \
        .rename({"CURBAL": "NEWBAL"})

    # MERGE ... BY ACCTNO (full outer)
    merged = lastmth.join(thismth, on="ACCTNO", how="full", coalesce=True, suffix="_new")

    # ABS(SUM(NEWBAL, (-1)*OLDBAL)) GE 1,000,000
    merged = merged.with_columns([
        pl.col("NEWBAL").fill_null(0.0),
        pl.col("OLDBAL").fill_null(0.0),
    ]).filter(
        (pl.col("NEWBAL") - pl.col("OLDBAL")).abs() >= 1_000_000
    )

    # Resolve BRANCH: prefer THISMTH branch (BRANCH_new), fall back to LASTMTH branch
    if "BRANCH_new" in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col("BRANCH_new").is_not_null())
            .then(pl.col("BRANCH_new"))
            .otherwise(pl.col("BRANCH"))
            .alias("BRANCH")
        ).drop("BRANCH_new")

    _write(merged, _mis_path("DDMVNT"))


# ---------------------------------------------------------------------------
# 8. CAPUR — Current account by BRANCH PURPOSE DEPTYPE PRODUCT
# ---------------------------------------------------------------------------
def run_capur() -> None:
    """
    PROC SUMMARY DATA=BNM.CURN&REPTMON&NOWK NWAY;
    CLASS BRANCH PURPOSE DEPTYPE PRODUCT;
    VAR CURBAL;
    OUTPUT OUT=CAPUR (RENAME=(_FREQ_=NOACCT) DROP=_TYPE_) SUM=CURBAL;

    PROC APPEND DATA=CAPUR BASE=MIS.CAPUR&REPTMON;
    """
    print("[CAPUR] Current account by branch/purpose/deptype/product")
    df = _read(_curn_path())
    class_cols = ["BRANCH", "PURPOSE", "DEPTYPE", "PRODUCT"]
    out = _summary_nway(
        df, class_cols,
        {"CURBAL": pl.col("CURBAL").sum()},
        freq_name="NOACCT",
    )
    _write(out, _mis_path("CAPUR"))


# ---------------------------------------------------------------------------
# 9. MTHACE — ACE by branch (Automated Cash Enhancement)
# ---------------------------------------------------------------------------
def run_mthace() -> None:
    """
    DATA MIS.MTHACE&REPTMON;
      KEEP ACCTNO BRANCH SABAL CABAL CURBAL;
      SET BNM.CURN&REPTMON&NOWK;
      IF PRODUCT IN (150,151,152,181);
        IF CABAL=0 THEN CABAL=CURBAL;

    PROC SUMMARY DATA=MIS.MTHACE&REPTMON NWAY;
    CLASS BRANCH;
    VAR SABAL CABAL;
    OUTPUT OUT=MIS.MTHACE&REPTMON SUM=;

    Note: The commented-out PROC SORT blocks for SAVG/CURN are left
    commented out as in the original SAS source.
    """
    print("[MTHACE] ACE by branch")
    df = _read(_curn_path())

    # Filter to ACE products and adjust CABAL
    mthace = (
        df
        .select(["ACCTNO", "BRANCH", "SABAL", "CABAL", "CURBAL"])
        .filter(pl.col("PRODUCT").is_in([150, 151, 152, 181]))
        .with_columns(
            pl.when(pl.col("CABAL") == 0)
            .then(pl.col("CURBAL"))
            .otherwise(pl.col("CABAL"))
            .alias("CABAL")
        )
    )

    # PROC SUMMARY NWAY CLASS BRANCH VAR SABAL CABAL; OUTPUT OUT=... SUM=;
    out = mthace.group_by("BRANCH").agg([
        pl.col("SABAL").sum(),
        pl.col("CABAL").sum(),
    ])

    _write(out, _mis_path("MTHACE"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print(f"[DEPMPBBD] REPTMON={REPTMON}  NOWK={NOWK}")
    run_sdrnge()
    run_sdpage()
    run_ddrnge()
    run_dpposn()
    run_dpacct()
    run_sdmvnt()
    run_ddmvnt()
    run_capur()
    run_mthace()
    print("[DEPMPBBD] Done.")


if __name__ == "__main__":
    main()
