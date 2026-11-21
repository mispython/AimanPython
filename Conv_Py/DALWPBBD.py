#!/usr/bin/env python3
"""
Program : DALWPBBD.py
Function: PBB deposit manipulation extracted from SAP.PBB.MNITB
          (SAVING and CURRENT accounts) — invoked by weekly BNM
          reporting jobs (e.g. EIIWREXL) via %INC PGM(DALWPBBD).

Original : DALWPBBD (invoked by job EIBWEEK1-3)

Dependency (format library):
    %INC PGM(PBBDPFMT);  -> already converted as PBBDPFMT.py
    from PBBDPFMT import (statecd_format, saprod_format, sadenom_format,
                           sacustcd_format, caprod_format, cadenom_format,
                           ddcustcd_format, ACE_PRODUCTS)

============================================================================
PHYSICAL INPUT DATASETS USED BY THIS PROGRAM  (all .sas7bdat, cached to
Parquet on first)
============================================================================
1. DEPOSIT.SAVING  (SAP.PBB.MNITB savings extract)
   File     : sa<mmwyy>.sas7bdat  (weekly, e.g. sa07226 — filename
              convention assumed per project convention; unconfirmed)
   Path     : INPUT_SAVING_DIR, resolved via input_date.get_latest_file()
   Used in  : Step 2 - build BNM_SAVG (CUSTCD/STATECD/PRODCD/AMTIND
              formats applied; filtered OPENIND NOT IN ('B','C','P')
              AND CURBAL >= 0)

2. DEPOSIT.CURRENT  (SAP.PBB.MNITB current extract)
   File     : ca<mmwyy>.sas7bdat  (weekly — filename convention assumed;
              unconfirmed)
   Path     : INPUT_CURRENT_DIR, resolved via input_date.get_latest_file()
   Used in  : Step 3 - build BNM_CURN / BNM_FCY (STATECD/PRODCD/AMTIND/
              CUSTCD formats + ACE / FCY-range branching)

3. CISDP.DEPOSIT  (CIS deposit customer-number extension — fixed
   filename, no date token)
   File     : CISDP_deposit.sas7bdat
   Path     : INPUT_CISDP_DIR
   Used in  : Step 3 - left join onto BNM_FCY rows (BY ACCTNO, IF A) to
              attach CUSTNO before appending FCY rows onto BNM_CURN

------------------------------------------------------------------------
NON-FILE / DERIVED / TEMPORARY OUTPUTS PRODUCED BY THIS PROGRAM
------------------------------------------------------------------------
This module is designed to be IMPORTED (module-level execution, mirroring
SAS %INC PGM(DALWPBBD)) by downstream programs such as EIIWREXL.py. It
produces the following in-memory/cached artefacts for their consumption —
none of these are physical mainframe inputs, they are built here:

- BNM_SAVG  (module-level polars DataFrame) : SAP.PBB.MNITB savings, BNM
  library equivalent of BNM.SAVG&REPTMON&NOWK
- BNM_CURN  (module-level polars DataFrame) : SAP.PBB.MNITB current +
  appended FCY rows, equivalent of BNM.CURN&REPTMON&NOWK
- BNM_DEPT  (module-level polars DataFrame) : branch-level PROC SUMMARY
  rollup of SAVG + CURN, equivalent of BNM.DEPT&REPTMON&NOWK
- SAVG<REPTMON><NOWK>.parquet / CURN<REPTMON><NOWK>.parquet /
  DEPT<REPTMON><NOWK>.parquet, written under OUTPUT_CACHE_DIR — persisted
  copies of the three DataFrames above so other programs in the same
  pipeline can read them via read_parquet() without re-running this module.
"""

import gc
from pathlib import Path

import duckdb
import polars as pl

from PBBDPFMT import (
    statecd_format,
    saprod_format,
    sadenom_format,
    sacustcd_format,
    caprod_format,
    cadenom_format,
    ddcustcd_format,
    ACE_PRODUCTS,
)

# ============================================================================
# STEP 1: RESOLVE & CACHE INPUT FILES  (see module docstring items 1-3)
# ============================================================================
def build_savg_curn_dept(
    saving_cache: Path,
    current_cache: Path,
    cisdp_cache: Path,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Builds BNM_SAVG / BNM_CURN / BNM_DEPT (equivalent of BNM.SAVG&REPTMON&NOWK,
    BNM.CURN&REPTMON&NOWK, BNM.DEPT&REPTMON&NOWK) from PRE-CACHED parquet
    files. DALWPBBD no longer resolves, dates, or converts its own inputs —
    every physical input path is owned and supplied by the calling job
    (e.g. EIBDLIQP.py), since this program is shared across several jobs
    whose input file dates differ.

    Returns (BNM_SAVG, BNM_CURN, BNM_DEPT) only. This program has no
    physical output of its own (BNM is a temporary library in the SAS job);
    any persistence is the caller's responsibility.
    """
    SAVING_SAS_CACHE  = saving_cache
    CURRENT_SAS_CACHE = current_cache
    CISDP_SAS_CACHE   = cisdp_cache

    # ========================================================================
    # STEP 2: BUILD BNM.SAVG&REPTMON&NOWK  (input: item 1 — DEPOSIT.SAVING)
    # DATA BNM.SAVG&REPTMON&NOWK &SAVG2;
    #   SET DEPOSIT.SAVING &SAVG1;
    #   IF OPENIND NOT IN ('B','C','P') AND CURBAL GE 0;
    #   CUSTCD=PUT(CUSTCODE, SACUSTCD.); STATECD=PUT(BRANCH, STATECD.);
    #   PRODCD=PUT(PRODUCT, SAPROD.);    AMTIND=PUT(PRODUCT, SADENOM.);
    # ============================================================================
    print("\nStep 2: Building BNM_SAVG...")

    con = duckdb.connect(database=":memory:")
    _savg_raw = con.execute(f"""
        SELECT
            CAST(BRANCH   AS INTEGER) AS BRANCH,
            CAST(PRODUCT  AS INTEGER) AS PRODUCT,
            CAST(CUSTCODE AS INTEGER) AS CUSTCODE,
            CAST(NAME     AS VARCHAR) AS NAME,
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(CURBAL   AS DOUBLE)  AS CURBAL,
            CAST(INTPAYBL AS DOUBLE)  AS INTPAYBL,
            CAST(COSTCTR  AS VARCHAR) AS COSTCTR,
            CAST(DNBFISME AS VARCHAR) AS DNBFISME,
            CAST(CURCODE  AS VARCHAR) AS CURCODE
        FROM read_parquet('{SAVING_SAS_CACHE.as_posix()}')
        WHERE OPENIND NOT IN ('B','C','P') AND CURBAL >= 0
    """).pl()
    con.close()

    con = duckdb.connect(database=":memory:")

    BNM_SAVG = _savg_raw.with_columns([
        pl.col("CUSTCODE").map_elements(sacustcd_format, return_dtype=pl.Utf8).alias("CUSTCD"),
        pl.col("BRANCH").map_elements(statecd_format, return_dtype=pl.Utf8).alias("STATECD"),
        pl.col("PRODUCT").map_elements(saprod_format, return_dtype=pl.Utf8).alias("PRODCD"),
        pl.col("PRODUCT").map_elements(sadenom_format, return_dtype=pl.Utf8).alias("AMTIND"),
    ]).select([
        "BRANCH", "PRODUCT", "CUSTCD", "STATECD", "PRODCD", "NAME", "ACCTNO",
        "CURBAL", "INTPAYBL", "AMTIND", "COSTCTR", "DNBFISME", "CURCODE",
    ])

    del _savg_raw
    gc.collect()
    print(f"  BNM_SAVG rows: {len(BNM_SAVG):,}")


    # ============================================================================
    # STEP 3: BUILD BNM.CURN&REPTMON&NOWK / BNM.FCY&REPTMON&NOWK
    # (inputs: item 2 — DEPOSIT.CURRENT; item 3 — CISDP.DEPOSIT)
    # ============================================================================
    print("\nStep 3: Building BNM_CURN / BNM_FCY...")

    con = duckdb.connect(database=":memory:")
    _curn_raw = con.execute(f"""
        SELECT
            CAST(BRANCH     AS INTEGER) AS BRANCH,
            CAST(PRODUCT    AS INTEGER) AS PRODUCT,
            CAST(CUSTCODE   AS INTEGER) AS CUSTCODE,
            CAST(NAME       AS VARCHAR) AS NAME,
            CAST(ACCTNO     AS BIGINT)  AS ACCTNO,
            CAST(CURBAL     AS DOUBLE)  AS CURBAL,
            CAST(INTPAYBL   AS DOUBLE)  AS INTPAYBL,
            CAST(ODINTACC   AS VARCHAR) AS ODINTACC,
            CAST(COSTCTR    AS VARCHAR) AS COSTCTR,
            CAST(SECTOR     AS INTEGER) AS SECTOR,
            CAST(DNBFISME   AS VARCHAR) AS DNBFISME,
            CAST(CURCODE    AS VARCHAR) AS CURCODE,
            CAST(INTRATE    AS DOUBLE)  AS INTRATE,
            CAST(BILLERIND  AS VARCHAR) AS BILLERIND
        FROM read_parquet('{CURRENT_SAS_CACHE.as_posix()}')
        WHERE OPENIND NOT IN ('B','C','P') AND CURBAL >= 0
    """).pl()
    con.close()

    _curn_raw = _curn_raw.with_columns([
        pl.col("BRANCH").map_elements(statecd_format, return_dtype=pl.Utf8).alias("STATECD"),
        pl.col("PRODUCT").map_elements(caprod_format, return_dtype=pl.Utf8).alias("PRODCD"),
        pl.col("PRODUCT").map_elements(cadenom_format, return_dtype=pl.Utf8).alias("AMTIND"),
    ])

    # SELECT(PRODUCT): 104 -> '02', 105 -> '81', OTHERWISE PUT(CUSTCODE, DDCUSTCD.)
    _curn_raw = _curn_raw.with_columns(
        pl.when(pl.col("PRODUCT") == 104).then(pl.lit("02"))
        .when(pl.col("PRODUCT") == 105).then(pl.lit("81"))
        .otherwise(pl.col("CUSTCODE").map_elements(ddcustcd_format, return_dtype=pl.Utf8))
        .alias("CUSTCD")
    )

    is_ace = pl.col("PRODUCT").is_in(list(ACE_PRODUCTS))
    is_fcy_range = (
        ((pl.col("PRODUCT") >= 400) & (pl.col("PRODUCT") <= 444))
        | pl.col("PRODUCT").is_in([450, 451, 452, 453, 454])
    )

    # ACE branch: INTPAYBL forced to 0; PRODCD/AMTIND recomputed (same formula,
    # same result as the initial computation above — replicated for fidelity).
    _ace_rows = _curn_raw.filter(is_ace).with_columns([
        pl.lit(0.0).alias("INTPAYBL"),
        pl.col("PRODUCT").map_elements(caprod_format, return_dtype=pl.Utf8).alias("PRODCD"),
        pl.col("PRODUCT").map_elements(cadenom_format, return_dtype=pl.Utf8).alias("AMTIND"),
    ])

    # FCY branch: SECTOR reclassification based on CUSTCD residency grouping.
    _fcy_rows = _curn_raw.filter(~is_ace & is_fcy_range)
    _is_resident = pl.col("CUSTCD").is_in(["77", "78", "95"])
    _fcy_rows = _fcy_rows.with_columns(
        pl.when(_is_resident & pl.col("SECTOR").is_in([4, 5])).then(1)
        .when(_is_resident & ~pl.col("SECTOR").is_in([1, 2, 3, 4, 5])).then(1)
        .when(~_is_resident & pl.col("SECTOR").is_in([1, 2, 3])).then(4)
        .when(~_is_resident & ~pl.col("SECTOR").is_in([1, 2, 3, 4, 5])).then(4)
        .otherwise(pl.col("SECTOR"))
        .alias("SECTOR")
    )

    # Remaining rows: neither ACE nor FCY range -> straight to CURN.
    _plain_rows = _curn_raw.filter(~is_ace & ~is_fcy_range)

    _curn_out_cols = [
        "BRANCH", "PRODUCT", "CUSTCD", "STATECD", "PRODCD", "NAME", "ACCTNO",
        "CURBAL", "INTPAYBL", "AMTIND", "ODINTACC", "COSTCTR", "SECTOR",
        "DNBFISME", "CURCODE", "INTRATE", "BILLERIND",
    ]

    BNM_CURN_BASE = pl.concat(
        [_ace_rows.select(_curn_out_cols), _plain_rows.select(_curn_out_cols)]
    )
    BNM_FCY = _fcy_rows.select(_curn_out_cols + ["CUSTCODE"])

    del _curn_raw, _ace_rows, _plain_rows
    gc.collect()

    # ----------------------------------------------------------------------------
    # PROC SORT DATA=CISDP.DEPOSIT OUT=CISDP(KEEP=ACCTNO CUSTNO); BY ACCTNO;
    # MERGE BNM.FCY(IN=A) CISDP; BY ACCTNO; IF A;   -- left join FCY -> CISDP
    # PROC APPEND FCY(DROP=CUSTNO) BASE=CURN        -- append FCY rows to CURN
    # ----------------------------------------------------------------------------
    print("  Joining FCY with CISDP and appending onto CURN...")

    con = duckdb.connect(database=":memory:")
    cisdp = con.execute(f"""
        SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(CUSTNO AS VARCHAR) AS CUSTNO
        FROM read_parquet('{CISDP_SAS_CACHE.as_posix()}')
    """).pl()
    con.close()

    BNM_FCY = BNM_FCY.join(cisdp, on="ACCTNO", how="left")  # IF A -> keep all FCY rows
    BNM_FCY_FOR_APPEND = BNM_FCY.select(_curn_out_cols)     # DROP=CUSTNO

    BNM_CURN = pl.concat([BNM_CURN_BASE, BNM_FCY_FOR_APPEND])

    del BNM_CURN_BASE, BNM_FCY_FOR_APPEND, cisdp
    gc.collect()
    print(f"  BNM_CURN rows: {len(BNM_CURN):,}  (incl. FCY appended)")


    # ============================================================================
    # STEP 4: BUILD BNM.DEPT&REPTMON&NOWK  (branch-level summary; not a
    # physical input — built from BNM_SAVG/BNM_CURN produced in Steps 2-3)
    # PROC DATASETS ... DELETE DEPT&REPTMON&NOWK;   -- fresh rebuild every run
    # PROC SUMMARY SAVG NWAY CLASS BRANCH STATECD PRODCD CUSTCD AMTIND -> DEPT
    # PROC SUMMARY CURN NWAY MISSING CLASS BRANCH STATECD PRODCD CUSTCD SECTOR
    #              AMTIND -> DEPT (FORCE append, differing class columns)
    # ============================================================================
    print("\nStep 4: Building BNM_DEPT (branch-level summary)...")

    _dept_savg = (
        BNM_SAVG.group_by(["BRANCH", "STATECD", "PRODCD", "CUSTCD", "AMTIND"])
        .agg([pl.col("CURBAL").sum(), pl.col("INTPAYBL").sum()])
    )

    _dept_curn = (
        BNM_CURN.group_by(["BRANCH", "STATECD", "PRODCD", "CUSTCD", "SECTOR", "AMTIND"])
        .agg([pl.col("CURBAL").sum(), pl.col("INTPAYBL").sum()])
    )

    # PROC APPEND FORCE: differing columns (SECTOR only in CURN summary) are
    # unioned; SAVG summary rows get SECTOR = null.
    BNM_DEPT = pl.concat([_dept_savg, _dept_curn], how="diagonal_relaxed")

    del _dept_savg, _dept_curn
    gc.collect()
    print(f"  BNM_DEPT rows: {len(BNM_DEPT):,}")


    # ============================================================================
    # STEP 5: WRITE PARQUET CACHE  (temporary artefacts for this program's own
    # and downstream programs' use — see module docstring "NON-FILE / DERIVED /
    # TEMPORARY OUTPUTS")
    # ============================================================================
    # BNM library is &&TEMP in the SAS job: nothing is persisted here.
    print("  BNM_SAVG / BNM_CURN / BNM_DEPT built in memory.")
    print("==================================================")
    return BNM_SAVG, BNM_CURN, BNM_DEPT
