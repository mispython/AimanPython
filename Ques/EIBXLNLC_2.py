#!/usr/bin/env python3
"""
Program : EIBXLNLC.py
Purpose : Loan data preparation - merges LNNOTE, LNCOMM, and LOAN datasets
          to produce NOTE1 (all loans by FISSPURP) and NOTE2 (construction/
          real-estate loans for non-individual customers) for both PBB and
          PIBB. Runs at the same frequency as EIBXODLC.py (right after it
          in scheduling):
            - 16th of month -> report date = 15th  (NOWK='2')
            - 1st of month  -> report date = last day of prior month (NOWK='4')

Memory note:
    LNNOTE / LOAN inputs can exceed 10GB combined. Following the pattern used
    in EIBDLN1M.py, every .sas7bdat input is first streamed in chunks into a
    Parquet cache (schema locked on the first chunk, PyArrow ParquetWriter),
    and all merges/filters are then pushed down into DuckDB SQL run directly
    against the cached Parquet files. Only the final, already-filtered NOTE1
    / NOTE2 result sets are ever materialised as in-memory Polars DataFrames.
"""

from __future__ import annotations

import os
import gc
from pathlib import Path
from typing import Dict

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from input_date import get_latest_file

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Production Path
# BASE_DIR = Path("/dwh")
# INPUT_DIR = Path("/sas/ln/dwh/enrichment")
# OUTPUT_DIR = BASE_DIR / "output" / "EIBXLNLC"

BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR / "input" / "prod" / "EIBXLNLC"
OUTPUT_DIR = BASE_DIR / "output" / "EIBXLNLC"
CACHE_DIR  = BASE_DIR / "input" / "prod" / "EIBXLNLC" / "cache"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------------
# Inputs:
#   1. enrh_ln_note.sas7bdat  - shared LNNOTE file for both PBB and PIBB;
#                                split by ENTITY_CD: 'PIBB' -> PIBB, else -> PBB
#                                Columns: ACCTNO NOTENO BANKNO STATE NAME
#                                         NTBRCH COMMNO LIABCODE ENTITY_CD
#   2. enrh_ln_comm.sas7bdat  - shared LNCOMM file for both PBB and PIBB
#                                Columns: ACCTNO COMMNO CCOLLTRL
#   3. lnXXXXX.sas7bdat       - PBB loan extract  (SAP.PBB.SASDATA)
#                                Columns: ACCTNO NOTENO COMMNO BRANCH BALANCE
#                                         SECTORCD CUSTCD INTRATE APPRLIMT
#                                         FISSPURP LIABCODE
#   4. ilnXXXXX.sas7bdat      - PIBB loan extract (SAP.PIBB.SASDATA)
#                                Same columns as PBB loan extract
#
#   NOTE: The original SAS DATA LNOTE step filters "IF ACCTYPE = 'LN'".
#   ACCTYPE is not present in the loan Parquet source used by this pipeline
#   (same situation as EIBDLN1M's ACCTYPE/LNTYPE note), so - consistent with
#   the prior conversion of this program - that filter is not applied here.
# ----------------------------------------------------------------------------
LNNOTE_PATH: Path = INPUT_DIR / "enrh_ln_note.sas7bdat"
LNCOMM_PATH: Path = INPUT_DIR / "enrh_ln_comm.sas7bdat"

# Production Path
# PBB_CONFIG: Dict[str, Path] = {
#     "loan_dir"  : get_latest_file(BASE_DIR / "ln_ln", "ln"),
#     "output_dir": OUTPUT_DIR / "PBB",
# }
# PIBB_CONFIG: Dict[str, Path] = {
#     "loan_dir"  : get_latest_file(BASE_DIR / "iln_ln", "iln"),
#     "output_dir": OUTPUT_DIR / "PIBB",
# }

# Testing Path
PBB_CONFIG: Dict[str, Path] = {
    "loan_dir"  : get_latest_file(BASE_DIR / "input/prod/EIBXODLC", "ln"),
    "output_dir": OUTPUT_DIR / "PBB",
}
PIBB_CONFIG: Dict[str, Path] = {
    "loan_dir"  : get_latest_file(BASE_DIR / "input/prod/EIBXODLC", "iln"),
    "output_dir": OUTPUT_DIR / "PIBB",
}

# ============================================================================
# PROC FORMAT (informational - not used in output columns)
# ============================================================================
# PROC FORMAT;
#    VALUE BANKFMT 33='PBB'
#                 134='PFB';
# RUN;
BANKFMT = {33: "PBB", 134: "PFB"}

# ============================================================================
# REPORT DATE DERIVATION
# ============================================================================
# DATA _NULL_;
#    SET LOAN.REPTDATE;
#    SELECT(DAY(REPTDATE)) ... CALL SYMPUT('NOWK', ...) CALL SYMPUT('RDATE', ...)
#    CALL SYMPUT('REPTMON', ...) CALL SYMPUT('REPTYEAR', ...)
# RUN;
#
# REPTMON / NOWK are obtained from REPTDATE.get_reptdate_values().
# RDATE and REPTYEAR are not consumed downstream so are not carried forward.

# ============================================================================
# CHUNK SIZE FOR STREAMING LARGE .sas7bdat FILES
# ============================================================================
CHUNK_ROWS = 500_000
ROW_LIMIT  = int(os.environ.get("EIBXLNLC_ROW_LIMIT", 0))   # 0 = no limit (test mode via env)


# ============================================================================
# HELPER: CACHE STAMP  (skip re-conversion if .sas7bdat hasn't changed)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


# ============================================================================
# HELPER: STREAM .sas7bdat -> PARQUET  (memory-efficient chunked conversion)
# ============================================================================
def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a large .sas7bdat to Parquet in streaming chunks.

    Column names are upper-cased on read so downstream DuckDB SQL can rely
    on a consistent naming convention regardless of the source file.
    """
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer    = None
    schema    = None
    total     = 0
    rows_read = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if ROW_LIMIT and rows_read >= ROW_LIMIT:
            break
        if ROW_LIMIT:
            chunk = chunk.iloc[: ROW_LIMIT - rows_read]
        rows_read += len(chunk)

        chunk.columns = [c.upper() for c in chunk.columns]
        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if schema is None:
            # Lock schema on first chunk
            schema = table.schema
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        else:
            # Cast subsequent chunks to match the locked schema
            cast_arrays = []
            for field in schema:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        print(f"  [{tag}] WARNING: Cannot cast '{field.name}' "
                              f"from {col.type} to {field.type}: {e} - filling nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)

        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows cached.")


def _ensure_cached(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert *sas_path* to Parquet at *cache_path* only if the cache is stale."""
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
        return
    sas_to_parquet(sas_path, cache_path, tag)


# ============================================================================
# ROW LIMIT NOTE
# ============================================================================
# ROW_LIMIT (env var EIBXLNLC_ROW_LIMIT) caps rows read from EVERY SAS input
# during chunked conversion - used for fast local testing. Leave unset or 0
# for full production runs.


# ============================================================================
# CORE PROCESSING  (per bank: PBB / PIBB)
# ============================================================================
def process_bank(
    bank_name    : str,
    config       : Dict[str, Path],
    lnnote_cache : Path,
    lncomm_cache : Path,
    entity_filter: str,
    reptmon      : str,
) -> None:
    loan_path  = config["loan_dir"]
    output_dir = config["output_dir"]

    if not loan_path.exists():
        raise FileNotFoundError(f"[{bank_name}] Missing LOAN file: {loan_path}")

    loan_cache = CACHE_DIR / f"{bank_name.lower()}_{loan_path.stem}.parquet"
    _ensure_cached(loan_path, loan_cache, f"{bank_name} LOAN")

    con = duckdb.connect(database=":memory:")

    # ------------------------------------------------------------------
    # DATA LNOTE:
    #   MERGE LOAN(IN=A) LNNOTE(IN=B); BY ACCTNO NOTENO;
    #   KEEP: BANKNO BRANCH ACCTNO NOTENO NAME BALANCE SECTORCD CUSTCD
    #         INTRATE NTBRCH COMMNO LIABCODE APPRLIMT FISSPURP STATE
    #
    # ENTITY_CD splits the shared LNNOTE file into PBB / PIBB subsets.
    # COMMNO / LIABCODE exist on both sides - LNNOTE (last dataset) wins,
    # falling back to LOAN's value only when LNNOTE's is missing.
    # Filtering, casting, and the join all happen inside DuckDB so the full
    # (10GB+) LOAN / LNNOTE contents never load into Python memory at once -
    # only the projected/joined result does.
    # ------------------------------------------------------------------
    lnote_sql = f"""
        WITH lnnote_bank AS (
            SELECT
                CAST(ACCTNO AS BIGINT)  AS ACCTNO,
                CAST(NOTENO AS BIGINT)  AS NOTENO,
                BANKNO, STATE, NAME, NTBRCH,
                CAST(COMMNO AS BIGINT)  AS COMMNO,
                LIABCODE
            FROM read_parquet('{lnnote_cache}')
            WHERE ACCTNO IS NOT NULL AND NOTENO IS NOT NULL
              AND {entity_filter}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, NOTENO ORDER BY ACCTNO) = 1
        ),
        loan_bank AS (
            SELECT
                CAST(ACCTNO AS BIGINT)  AS ACCTNO,
                CAST(NOTENO AS BIGINT)  AS NOTENO,
                CAST(COMMNO AS BIGINT)  AS COMMNO,
                CAST(BRANCH AS BIGINT)  AS BRANCH,
                BALANCE,
                CAST(SECTORCD AS VARCHAR) AS SECTORCD,
                CAST(CUSTCD   AS VARCHAR) AS CUSTCD,
                INTRATE, APPRLIMT, FISSPURP, LIABCODE
            FROM read_parquet('{loan_cache}')
            WHERE NOTENO IS NOT NULL AND NOTENO <> 0
        )
        SELECT
            n.BANKNO,
            l.BRANCH,
            l.ACCTNO,
            l.NOTENO,
            n.NAME,
            l.BALANCE,
            l.SECTORCD,
            l.CUSTCD,
            l.INTRATE,
            n.NTBRCH,
            COALESCE(n.COMMNO, l.COMMNO)     AS COMMNO,
            COALESCE(n.LIABCODE, l.LIABCODE) AS LIABCODE,
            l.APPRLIMT,
            l.FISSPURP,
            n.STATE
        FROM loan_bank l
        LEFT JOIN lnnote_bank n
          ON l.ACCTNO = n.ACCTNO AND l.NOTENO = n.NOTENO
    """
    con.execute(f"CREATE TEMP TABLE lnote AS {lnote_sql}")

    # ------------------------------------------------------------------
    # DATA NOTE1:
    #   MERGE LNOTE(IN=A) LNCOMM(IN=B); BY ACCTNO COMMNO; IF A;
    # (LEFT JOIN from lnote already enforces "IF A")
    # ------------------------------------------------------------------
    note1_sql = f"""
        WITH lncomm AS (
            SELECT
                CAST(ACCTNO AS BIGINT) AS ACCTNO,
                CAST(COMMNO AS BIGINT) AS COMMNO,
                CCOLLTRL
            FROM read_parquet('{lncomm_cache}')
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, COMMNO ORDER BY ACCTNO) = 1
        )
        SELECT
            o.BANKNO, o.BRANCH, o.ACCTNO, o.NOTENO, o.NAME, o.APPRLIMT,
            o.BALANCE, o.SECTORCD, o.CUSTCD, o.STATE, o.INTRATE, o.NTBRCH,
            o.COMMNO, o.LIABCODE, c.CCOLLTRL, o.FISSPURP
        FROM lnote o
        LEFT JOIN lncomm c
          ON o.ACCTNO = c.ACCTNO AND o.COMMNO = c.COMMNO
    """
    note1_df = con.execute(note1_sql).pl()
    con.execute("DROP TABLE lnote")
    con.close()
    gc.collect()

    # ------------------------------------------------------------------
    # DATA NOTE2:
    #   SET NOTE1;
    #   IF CUSTCD NOT IN ('77','78','95','96') AND
    #      (SUBSTR(SECTORCD,1,1) = '5' OR SECTORCD = '8310') THEN OUTPUT;
    # ------------------------------------------------------------------
    note2_df = note1_df.filter(
        (~pl.col("CUSTCD").is_in(["77", "78", "95", "96"]))
        & ((pl.col("SECTORCD").str.slice(0, 1) == "5") | (pl.col("SECTORCD") == "8310"))
    )

    # ------------------------------------------------------------------
    # PROC SORT DATA=NOTE1 OUT=LNLC(I).NOTE1&REPTMON;  BY BRANCH FISSPURP CUSTCD ACCTNO;
    # PROC SORT DATA=NOTE2 OUT=LNLC(I).NOTE2&REPTMON;  BY BRANCH SECTORCD CUSTCD ACCTNO;
    # ------------------------------------------------------------------
    note1_sorted = note1_df.sort(["BRANCH", "FISSPURP", "CUSTCD", "ACCTNO"])
    note2_sorted = note2_df.sort(["BRANCH", "SECTORCD", "CUSTCD", "ACCTNO"])

    del note1_df, note2_df
    gc.collect()

    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = "LNLC" if bank_name == "PBB" else "LNLCI"

    note1_out = output_dir / f"{prefix}_NOTE1_{reptmon}.parquet"
    note2_out = output_dir / f"{prefix}_NOTE2_{reptmon}.parquet"

    note1_sorted.write_parquet(note1_out)
    note2_sorted.write_parquet(note2_out)

    print(f"\n[{bank_name}] REPTMON={reptmon}")
    print(f"[{bank_name}] NOTE1 rows : {len(note1_sorted):,}")
    print(f"[{bank_name}] NOTE2 rows : {len(note2_sorted):,}")
    print(f"[{bank_name}] Output -> {note1_out}")
    print(f"[{bank_name}] Output -> {note2_out}")
    print(note1_sorted.head())
    print(note2_sorted.head())

    del note1_sorted, note2_sorted
    gc.collect()


# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    rv      = get_reptdate_values()
    reptmon = rv.reptmon  # zero-padded month e.g. '05'
    nowk    = rv.nowk     # week bucket       e.g. '2' or '4'

    print(f"Report Date : {rv.reptdate}  (REPTMON={reptmon}, NOWK={nowk})")
    if ROW_LIMIT:
        print(f"Test mode: reading at most {ROW_LIMIT:,} rows from each SAS input")

    if not LNNOTE_PATH.exists():
        raise FileNotFoundError(f"Missing shared LNNOTE file: {LNNOTE_PATH}")
    if not LNCOMM_PATH.exists():
        raise FileNotFoundError(f"Missing shared LNCOMM file: {LNCOMM_PATH}")

    # Shared inputs are cached to Parquet once - both banks then query the
    # same cache file via DuckDB, filtered by ENTITY_CD / bank as needed.
    lnnote_cache = CACHE_DIR / "enrh_ln_note.parquet"
    lncomm_cache = CACHE_DIR / "enrh_ln_comm.parquet"

    _ensure_cached(LNNOTE_PATH, lnnote_cache, "LNNOTE")
    _ensure_cached(LNCOMM_PATH, lncomm_cache, "LNCOMM")

    # PBB
    process_bank(
        "PBB", PBB_CONFIG,
        lnnote_cache=lnnote_cache,
        lncomm_cache=lncomm_cache,
        entity_filter="ENTITY_CD IS DISTINCT FROM 'PIBB'",
        reptmon=reptmon,
    )

    # PIBB
    process_bank(
        "PIBB", PIBB_CONFIG,
        lnnote_cache=lnnote_cache,
        lncomm_cache=lncomm_cache,
        entity_filter="ENTITY_CD = 'PIBB'",
        reptmon=reptmon,
    )


if __name__ == "__main__":
    main()
