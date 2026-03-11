#!/usr/bin/env python3
"""
Program : LALQP124.py (converted from X_LALQP124)
Purpose : Report on Domestic Assets and Liabilities - Part III
          Builds BNM.LALQ&REPTMON&NOWK by aggregating loan data
          across state/purpose, state/sector, and fixed/floating rate
          dimensions, then appending each block into the target LALQ
          parquet dataset.

Dependencies:
  - PBBLNFMT.py : format functions for loan processing.
                  %INC PGM(PBBLNFMT) in the original SAS makes all formats
                  globally available. The specific PBBLNFMT formats
                  (APPRLIMT., LOANSIZE. etc.) are not directly called in
                  this program's DATA steps; the %INC is preserved
                  structurally via this import.
  - SECTMAP.py  : sector code mapping / rollup.
                  process_sectmap(df) is the single entry-point equivalent
                  to %INC PGM(SECTMAP) in the SAS source.
"""

import os
import duckdb
import polars as pl

# ── Dependency imports from PBBLNFMT ─────────────────────────────────────────
# %INC PGM(PBBLNFMT) is present in the original SAS source.
# No specific PBBLNFMT format functions are directly called in this program's
# DATA steps; the %INC is preserved structurally via this import.
# format_apprlimt, format_loansize, format_lnormt, format_lnrmmt, format_riskcd
# are listed in __all__ within PBBLNFMT and are imported here for completeness
# consistent with the SAS %INC semantic.
from PBBLNFMT import (
    format_apprlimt,
    format_loansize,
    format_lnormt,
    format_lnrmmt,
    format_riskcd,
)

# ── Dependency: SECTMAP exposes a single entry-point function ─────────────────
# process_sectmap(df) is the equivalent of %INC PGM(SECTMAP) in SAS.
# Returns the combined ALM + ALMA dataset with SECTORCD normalised and
# expanded through the full hierarchy rollup pipeline.
from SECTMAP import process_sectmap


# ── Path configuration ────────────────────────────────────────────────────────
BASE_DIR   = os.environ.get("BASE_DIR", "/data")
BNM_DIR    = os.path.join(BASE_DIR, "bnm")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

REPTMON  = os.environ.get("REPTMON", "")   # e.g. "202401"
NOWK     = os.environ.get("NOWK",    "")   # e.g. "01"

# Input parquet
LOAN_PARQUET = os.path.join(BNM_DIR, f"LOAN{REPTMON}{NOWK}.parquet")

# Output parquet (BNM.LALQ&REPTMON&NOWK)
LALQ_PARQUET = os.path.join(BNM_DIR, f"LALQ{REPTMON}{NOWK}.parquet")

os.makedirs(BNM_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()


# ── Helper: apply SECTMAP pipeline to a DataFrame with SECTORCD column ────────
def apply_sectmap(df: pl.DataFrame) -> pl.DataFrame:
    """
    Equivalent of %INC PGM(SECTMAP) in the SAS source.
    Delegates to process_sectmap() from SECTMAP.py, which runs the full
    normalisation → expansion → rollup pipeline (inlining $NEWSECT./$VALIDSE.
    format logic) and returns the combined ALM + ALMA dataset with SECTORCD
    updated.  Intermediate helper columns added by SECTMAP are dropped.
    """
    df = process_sectmap(df)
    for col in ("SECTA", "SECVALID", "SECTCD"):
        if col in df.columns:
            df = df.drop(col)
    return df


# ── Helper: append rows to LALQ parquet ──────────────────────────────────────
def append_to_lalq(new_df: pl.DataFrame) -> None:
    """Append new_df (BNMCODE, AMTIND, AMOUNT) to LALQ parquet."""
    cols   = ["BNMCODE", "AMTIND", "AMOUNT"]
    schema = {"BNMCODE": pl.Utf8, "AMTIND": pl.Utf8, "AMOUNT": pl.Float64}

    if new_df.is_empty():
        return

    new_df = new_df.select(cols).cast({
        "BNMCODE": pl.Utf8,
        "AMTIND":  pl.Utf8,
        "AMOUNT":  pl.Float64,
    })

    if os.path.exists(LALQ_PARQUET):
        existing = con.execute(
            f"SELECT BNMCODE, AMTIND, AMOUNT FROM read_parquet('{LALQ_PARQUET}')"
        ).pl()
        combined = pl.concat([existing, new_df], how="diagonal")
    else:
        combined = new_df

    combined.write_parquet(LALQ_PARQUET)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 : LOAN – BY STATE CODE AND BY PURPOSE CODE
# ═══════════════════════════════════════════════════════════════════════════════

def section_loan_by_state_purpose():
    """
    PROC SUMMARY NWAY by FISSPURP, STATECD, AMTIND where PRODCD[:2]='34'.
    DATA ALQ1: remap FISSPURP IN ('0220','0230','0210','0211','0212') → '0200',
    append to ALQ.
    BNMCODE = '340000000' || FISSPURP || STATECD  (no trailing 'Y' – SAS
    concatenation ends with STATECD directly, not a literal 'Y').
    """
    alq = con.execute(f"""
        SELECT FISSPURP, STATECD, AMTIND, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
        GROUP BY FISSPURP, STATECD, AMTIND
    """).pl()

    # DATA ALQ1: remap FISSPURP to '0200' for those codes, output only remapped rows
    alq1_rows = []
    for row in alq.iter_rows(named=True):
        if (row["FISSPURP"] or "").strip() in ("0220","0230","0210","0211","0212"):
            alq1_rows.append({**row, "FISSPURP": "0200"})

    alq1 = pl.DataFrame(alq1_rows) if alq1_rows else pl.DataFrame(schema=alq.schema)

    # DATA ALQ: SET ALQ ALQ1  (append remapped rows to original)
    alq_full = pl.concat([alq, alq1], how="diagonal")

    # DATA ALQLOAN: BNMCODE = '340000000' || FISSPURP || STATECD; OUTPUT
    rows = []
    for row in alq_full.iter_rows(named=True):
        fisspurp = (row["FISSPURP"] or "").strip()
        statecd  = (row["STATECD"]  or "").strip()
        amtind   = row["AMTIND"] or ""
        amount   = row["AMOUNT"]
        rows.append({
            "BNMCODE": f"340000000{fisspurp}{statecd}",
            "AMTIND":  amtind,
            "AMOUNT":  amount,
        })

    if rows:
        append_to_lalq(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 : LOAN – BY STATE CODE AND BY SECTOR CODE
# ═══════════════════════════════════════════════════════════════════════════════

def section_loan_by_state_sector():
    """
    PROC SUMMARY NWAY by SECTORCD, STATECD, AMTIND where PRODCD[:2]='34'
    AND SECTORCD NE '0410'.
    DATA ALM: SET ALQ; %INC PGM(SECTMAP); DATA ALQ: SET ALM.
    IF SECTORCD NE '9700' → BNMCODE = '340000000' || SECTORCD || STATECD.
    """
    alq = con.execute(f"""
        SELECT SECTORCD, STATECD, AMTIND, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
          AND SECTORCD <> '0410'
        GROUP BY SECTORCD, STATECD, AMTIND
    """).pl()

    # DATA ALM: SET ALQ; %INC PGM(SECTMAP); DATA ALQ: SET ALM
    # SECTMAP is applied on the ALQ dataset (renamed ALM in SAS for the
    # %INC scope, then back to ALQ – pure aliasing, not a structural change).
    alq = apply_sectmap(alq)

    # DATA ALQLOAN: IF SECTORCD NE '9700' → BNMCODE; OUTPUT
    rows = []
    for row in alq.iter_rows(named=True):
        sectorcd = (row["SECTORCD"] or "").strip()
        statecd  = (row["STATECD"]  or "").strip()
        amtind   = row["AMTIND"] or ""
        amount   = row["AMOUNT"]
        # Note: no trailing 'Y' – SAS literal ends with STATECD directly
        if sectorcd != "9700":
            rows.append({
                "BNMCODE": f"340000000{sectorcd}{statecd}",
                "AMTIND":  amtind,
                "AMOUNT":  amount,
            })

    if rows:
        append_to_lalq(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 : FIXED AND FLOATING RATE LOANS
# ═══════════════════════════════════════════════════════════════════════════════

def section_fixed_floating_rate():
    """
    PROC SUMMARY NWAY MISSING by PRODCD, PRODUCT, USURYIDX, AMTIND
    where PRODCD[:2]='34' OR PRODCD[:2]='54'; VAR BALANCE.

    Note: The original SAS DATA step references NTINDEX in the BIC assignment
    (IF NTINDEX=030 THEN BIC='30595') rather than USURYIDX.  The PROC SUMMARY
    CLASS uses USURYIDX; both columns are selected so the DATA step logic
    operates on NTINDEX exactly as the SAS source specifies.

    Note: PROC APPEND is commented out in the original SAS source:
      (* PROC APPEND DATA=ALQLOAN BASE=BNM.LALQ&REPTMON&NOWK; RUN; )
    Accordingly, this section produces and consolidates ALQLOAN but does
    NOT append to LALQ, matching the original behaviour exactly.
    """
    alq = con.execute(f"""
        SELECT PRODCD, PRODUCT, USURYIDX, NTINDEX, AMTIND, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) IN ('34', '54')
        GROUP BY PRODCD, PRODUCT, USURYIDX, NTINDEX, AMTIND
    """).pl()

    rows = []
    for row in alq.iter_rows(named=True):
        amtind  = row["AMTIND"] or ""
        ntindex = row.get("NTINDEX") or 0
        amount  = row["AMOUNT"]

        bic = "30591"
        if ntindex == 30:   # IF NTINDEX=030
            bic = "30595"

        # IF BIC ^= ' ' – BIC is always non-blank, so always output
        rows.append({
            "BNMCODE": f"{bic}00000000Y",
            "AMTIND":  amtind,
            "AMOUNT":  amount,
        })

    if not rows:
        return

    alqloan = pl.DataFrame(rows)

    # PROC SUMMARY NWAY by BNMCODE, AMTIND; SUM AMOUNT (consolidation)
    alqloan = (
        alqloan
        .group_by(["BNMCODE", "AMTIND"])
        .agg(pl.col("AMOUNT").sum())
    )

    # * PROC APPEND DATA=ALQLOAN BASE=BNM.LALQ&REPTMON&NOWK; RUN;
    # Commented out in original – result NOT appended to LALQ.
    _ = alqloan   # computed but not written, matching original SAS behaviour


# ═══════════════════════════════════════════════════════════════════════════════
# FINAL CONSOLIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def final_consolidation():
    """
    PROC SUMMARY NWAY by BNMCODE, AMTIND; VAR AMOUNT; SUM → overwrite LALQ.
    Equivalent to DROP=_TYPE_ _FREQ_ in the OUTPUT dataset.
    """
    if not os.path.exists(LALQ_PARQUET):
        return

    final = con.execute(f"""
        SELECT BNMCODE, AMTIND, SUM(AMOUNT) AS AMOUNT
        FROM read_parquet('{LALQ_PARQUET}')
        GROUP BY BNMCODE, AMTIND
    """).pl()

    final.write_parquet(LALQ_PARQUET)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # %INC PGM(PBBLNFMT) – formats imported at module level above
    # %INC PGM(SECTMAP)  – process_sectmap imported at module level above

    section_loan_by_state_purpose()
    section_loan_by_state_sector()
    section_fixed_floating_rate()   # PROC APPEND commented out in original
    final_consolidation()

    print(f"LALQP124 complete. BNM.LALQ written to: {LALQ_PARQUET}")


if __name__ == "__main__":
    main()
