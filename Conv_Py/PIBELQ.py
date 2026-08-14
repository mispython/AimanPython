#!/usr/bin/env python3
"""
Program : PIBELQ.py
Purpose : Macro-equivalent module converted from PIBELQ (split from PBBELP).
          Builds and prints the detail daily Eligible Liabilities (EL)
          items for DAY A - DAY H (prtel) and DAY I (prteli).

Called by EIIWKAPE.py via:
    from PIBELQ import build_elw1, prtel, prteli

============================================================================
PHYSICAL INPUT DATASETS USED BY THIS MODULE  (all .sas7bdat, cached to
Parquet on first read per EIBDLN1M.py's chunked-conversion pattern)
============================================================================
1. BNMK REP2   (SAS libref BNMK -> SAP.PIBB.DKAPITI.SASDATA)
   File     : rep2<REPTMON><NOWK>.sas7bdat
   Path     : INPUT_BNMK_REP2_DIR
   Used in  : prtel()  -> builds REP6 (RM MARKETABLE SECURITIES total,
              BNMCODE forced to '4017100000000Y', filtered by ELDAY)
              prteli() -> builds REP7 (same logic, DAYI)

2. BNMK TBL1   (SAS libref BNMK -> SAP.PIBB.DKAPITI.SASDATA)
   File     : tbl1<REPTMON><NOWK>.sas7bdat
   Path     : INPUT_BNMK_TBL1_DIR
   Used in  : prtel() / prteli() -> PMM dataset, filtered by ELDAY

3. BNM ELW     (SAS libref BNM  -> SAP.PIBB.D&TOYYYY)
   File     : elw<REPTMON><NOWK>.sas7bdat
   Path     : INPUT_BNM_ELW_DIR
   Used in  : build_elw1() -> primary ELW1 source (remap/split logic for
              BNMCODEs 32199xx, 4411xxx/4414xxx/etc., 4019000000000Y)

4. BNMB ELW    (SAS libref BNMB -> SAP.PBB.D&TOYYYY)
   File     : elw<REPTMON><NOWK>.sas7bdat
   Path     : INPUT_BNMB_ELW_DIR
   Used in  : build_elw1() -> ELW2 override rows (BNMCODE='4929980000000Y'
              AND BRANCH > 3000), unioned onto ELW1

------------------------------------------------------------------------
NON-FILE INPUT: EL / ELI ITEM CATALOGUE
------------------------------------------------------------------------
DATA ELITEM in the original SAS is built from `PROC SORT DATA=EL` (in
%PRTEL, days A-H) and `PROC SORT DATA=ELI` (in %PRTELI, day I). These are
NOT physical BNM datasets in this job's JCL -- their column layout
(BNMCODE, SIGN, FMTNAME, TYPE, IDX, DESC) is exactly PBBELF's
EL_DEFINITIONS / ELI_DEFINITIONS tables, so they are sourced directly
from the PBBELF format-library module rather than from a file:
    from PBBELF import EL_DEFINITIONS, ELI_DEFINITIONS

Dependency note (format functions):
    %INC PGM(PBBELF); loads the PBBELF format library into the SAS
    session. Aside from the EL_DEFINITIONS/ELI_DEFINITIONS data used
    above, no PUT(var, <PBBELF-format>.) call (format_brchcd,
    format_regnew, format_ctype, format_cacbrch, etc.) appears anywhere
    in this program body, so those format functions are NOT imported.
"""

import gc
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from PBBELF import EL_DEFINITIONS, ELI_DEFINITIONS

# ============================================================================
# PATH CONFIGURATION (each physical input kept independent)
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_BNMK_REP2_DIR = BASE_DIR / "input" / "prod" / "EIIWKAPE" / "bnmk_rep2"
INPUT_BNMK_TBL1_DIR = BASE_DIR / "input" / "prod" / "EIIWKAPE" / "bnmk_tbl1"
INPUT_BNM_ELW_DIR   = BASE_DIR / "input" / "prod" / "EIIWKAPE" / "bnm_elw"
INPUT_BNMB_ELW_DIR  = BASE_DIR / "input" / "prod" / "EIIWKAPE" / "bnmb_elw"

# Parquet cache directory (shared with EIIWKAPE.py — same physical
# BNMK REP2 dataset is read by both programs for the same REPTMON/NOWK)
CACHE_DIR = BASE_DIR / "cache" / "EIIWKAPE"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000

# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET
# (identical pattern to EIBDLN1M.py: freshness check via mtime, PyArrow
# ParquetWriter with schema locked on first chunk)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer, schema, total = None, None, 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if schema is None:
            schema = table.schema
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        else:
            cast_arrays = []
            for field in schema:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        print(f"  [{tag}] WARNING: cannot cast '{field.name}' "
                              f"from {col.type} to {field.type}: {e} — filling nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done — {total:,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    """Resolve <stem>.parquet cache under CACHE_DIR, converting if stale."""
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh — skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# ASA REPORT LINE HELPERS
# ============================================================================
def _new_buf(width: int = 132) -> list:
    return [" "] * width


def _put(buf: list, col: int, text: str) -> None:
    """SAS @col PUT text equivalent (col is 1-based)."""
    start = col - 1
    for i, ch in enumerate(str(text)):
        if 0 <= start + i < len(buf):
            buf[start + i] = ch


def _line(buf: list, asa: str = " ") -> str:
    return asa + "".join(buf)


def _fmt_comma(value, width: int, decimals: int = 2) -> str:
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    s = f"{v:,.{decimals}f}"
    return s.rjust(width)[:width]


def _title_lines(*titles: str) -> list[str]:
    """TITLE1..TITLEn -> first line ASA='1' (new page), rest ASA=' '."""
    lines = []
    for i, t in enumerate(titles):
        buf = _new_buf()
        _put(buf, 1, t)
        lines.append(_line(buf, "1" if i == 0 else " "))
    return lines


# ============================================================================
# EL / ELI CATALOGUE (from PBBELF, not a file — see module docstring)
# ============================================================================
def _el_catalogue(definitions: list) -> pl.DataFrame:
    """PROC SORT DATA=EL|ELI (DROP=TYPE) OUT=ELITEM; BY BNMCODE;"""
    df = pl.DataFrame(definitions).rename({
        "bnmcode": "BNMCODE", "sign": "SIGN", "fmtname": "FMTNAME",
        "idx": "IDX", "desc": "DESC",
    }).drop("type")   # DROP=TYPE
    return df.sort("BNMCODE")


# ============================================================================
# BUILD ELW1  (shared by prtel() per-day filter and prteli())
# DATA ELW1; SET BNM.ELW&REPTMON&NOWK; ... ; DATA ELW2; SET BNMB.ELW...;
# ============================================================================
def build_elw1(reptmon: str, nowk: str, sdesc: str) -> pl.DataFrame:
    print("  Loading BNM ELW / BNMB ELW (build_elw1)...")

    elw1_sas = INPUT_BNM_ELW_DIR / f"elw{reptmon}{nowk}.sas7bdat"
    elw1_cache = _load_cached(elw1_sas, "BNM_ELW")

    elw2_sas = INPUT_BNMB_ELW_DIR / f"elw{reptmon}{nowk}.sas7bdat"
    elw2_cache = _load_cached(elw2_sas, "BNMB_ELW")

    con = duckdb.connect(database=":memory:")
    elw2 = con.execute(f"""
        SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(BRANCH AS INTEGER) BRANCH,
               CAST(ELDAY AS VARCHAR) ELDAY, CAST(AMOUNT AS DOUBLE) AMOUNT
        FROM read_parquet('{elw2_cache.as_posix()}')
        WHERE BNMCODE = '4929980000000Y' AND BRANCH > 3000
    """).pl()

    elw1_raw = con.execute(f"""
        SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(BRANCH AS INTEGER) BRANCH,
               CAST(ELDAY AS VARCHAR) ELDAY, CAST(AMOUNT AS DOUBLE) AMOUNT
        FROM read_parquet('{elw1_cache.as_posix()}')
    """).pl()
    con.close()

    elw1_raw = elw1_raw.with_columns(pl.col("AMOUNT").abs())

    rows = []
    for row in elw1_raw.iter_rows(named=True):
        bnmcode = row["BNMCODE"]
        amount = row["AMOUNT"]

        # IF "&SDESC"='PUBLIC BANK BERHAD' THEN ... (only applies to PBB run)
        if sdesc == "PUBLIC BANK BERHAD" and bnmcode == "4929980000000Y":
            amount = 0.00

        if bnmcode in ("3219902000000Y", "3219903000000Y", "3219912000000Y"):
            rows.append({**row, "BNMCODE": "3219910000000Y", "AMOUNT": amount})
        elif bnmcode in (
            "4411100000000Y", "4414000000000Y", "4411010000000Y",
            "4411015000000Y", "4411080000000Y", "4411200000000Y",
            "4411300000000Y", "4411900000000Y", "4412000000000Y",
            "4413000000000Y", "4429900000000Y",
        ):
            rows.append({**row, "BNMCODE": bnmcode, "AMOUNT": amount})
            rows.append({**row, "BNMCODE": "4410000000000Y", "AMOUNT": amount})
        elif bnmcode == "4019000000000Y":
            rows.append({**row, "BNMCODE": "4019100000000Y", "AMOUNT": amount})
            rows.append({**row, "BNMCODE": "4019000000000Y", "AMOUNT": 0.00})
        else:
            rows.append({**row, "BNMCODE": bnmcode, "AMOUNT": amount})

    elw1 = pl.DataFrame(rows, schema=elw1_raw.schema) if rows else elw1_raw.clear()
    elw1 = pl.concat([elw1, elw2.select(elw1.columns)], how="vertical")
    return elw1


def _elw_for_day(elw1: pl.DataFrame, day_code: str) -> pl.DataFrame:
    """PROC SORT DATA=ELW1(WHERE=(ELDAY="&I")) OUT=ELW (KEEP=BNMCODE AMOUNT)."""
    return (
        elw1.filter(pl.col("ELDAY") == day_code)
        .select(["BNMCODE", "AMOUNT"])
        .sort("BNMCODE")
    )


# ============================================================================
# MERGE WITH EL/ELI CATALOGUE + BUILD ELWT (SRR row) + FINAL RENDER
# ============================================================================
def _finalize_elw(elw_stack: pl.DataFrame, elitem: pl.DataFrame,
                   drop_4019: bool = True) -> pl.DataFrame:
    """
    DATA ELW; MERGE ELW(IN=A) ELITEM(IN=B); BY BNMCODE; IF B;
    ... DESC override for cagamas, FMTNAME=IDX||'-'||FMTNAME,
    AMOUNX/TOTALX SIGN adjustment, drop 4019000000000Y/4019100000000Y.

    drop_4019: prtel() always drops these two codes; prteli() only drops
    them WHEN WK NE &NOWK  (i.e. when NOWK is not '4') — controlled by
    the caller.
    """
    summed = elw_stack.group_by("BNMCODE").agg(pl.col("AMOUNT").sum().alias("AMOUNT"))

    merged = elitem.join(summed, on="BNMCODE", how="left").with_columns(
        pl.col("AMOUNT").fill_null(0.0)
    )
    merged = merged.with_columns(pl.col("AMOUNT").alias("TOTAL"))

    # IF IDX NE ' '; FMTNAME=IDX||'-'||FMTNAME;
    merged = merged.filter(pl.col("IDX") != " ")
    merged = merged.with_columns(
        (pl.col("IDX") + "-" + pl.col("FMTNAME")).alias("FMTNAME")
    )

    # Cagamas special description
    merged = merged.with_columns([
        pl.when(pl.col("BNMCODE") == "4314017000000Y")
        .then(
            pl.lit("O/W RM IBB FROM CAGAMAS ")
            + pl.col("AMOUNT").map_elements(lambda v: _fmt_comma(v, 14, 2).strip(), return_dtype=pl.Utf8)
        )
        .otherwise(pl.col("DESC"))
        .alias("DESC"),
        pl.when(pl.col("BNMCODE") == "4314017000000Y").then(0.0).otherwise(pl.col("AMOUNT")).alias("AMOUNT"),
        pl.when(pl.col("BNMCODE") == "4314017000000Y").then(0.0).otherwise(pl.col("TOTAL")).alias("TOTAL"),
    ])

    merged = merged.with_columns([
        pl.col("AMOUNT").alias("AMOUNX"),
        pl.col("TOTAL").alias("TOTALX"),
    ])
    neg = pl.col("SIGN") == "-"
    merged = merged.with_columns(
        pl.when(neg).then(-1 * pl.col("AMOUNT")).otherwise(pl.col("AMOUNX")).alias("AMOUNX")
    )
    merged = merged.with_columns(
        pl.when(neg).then(pl.col("AMOUNX")).otherwise(pl.col("TOTALX")).alias("TOTALX")
    )

    merged = merged.with_columns(
        pl.when(pl.col("BNMCODE") == "4017100000000Y")
        .then(pl.lit("TOTAL RM MARKETABLE SECURITIES"))
        .otherwise(pl.col("DESC"))
        .alias("DESC")
    )

    if drop_4019:
        merged = merged.filter(~pl.col("BNMCODE").is_in(["4019000000000Y", "4019100000000Y"]))
    return merged


def _build_elwt(elw_final: pl.DataFrame) -> pl.DataFrame:
    """DATA ELWT; SET ELW; BNMCODE='4013000000000Y'; ... PROC SUMMARY NWAY."""
    elwt = elw_final.with_columns([
        pl.lit("4013000000000Y").alias("BNMCODE"),
        pl.lit("E-ELSRR").alias("FMTNAME"),
        pl.lit("+").alias("SIGN"),
        pl.lit("ELIGIBLE LIABILITIES FOR SRR NEXT MONTH").alias("DESC"),
    ])
    bd_flag = pl.col("IDX").is_in(["B", "D"])
    elwt = elwt.with_columns(
        pl.when(bd_flag).then(-1 * pl.col("AMOUNT")).otherwise(pl.col("AMOUNX")).alias("AMOUNX")
    )
    elwt = elwt.with_columns(
        pl.when(bd_flag).then(pl.col("AMOUNX")).otherwise(pl.col("TOTALX")).alias("TOTALX")
    )

    grouped = (
        elwt.group_by(["BNMCODE", "FMTNAME", "DESC", "SIGN"])
        .agg([
            pl.col("AMOUNT").sum().alias("AMOUNT"),
            pl.col("TOTAL").sum().alias("TOTAL"),
            pl.col("AMOUNX").sum().alias("AMOUNX"),
            pl.col("TOTALX").sum().alias("TOTALX"),
        ])
    )
    # DATA ELWT; SET ELWT; AMOUNT=AMOUNX; TOTAL=TOTALX;
    grouped = grouped.with_columns([
        pl.col("AMOUNX").alias("AMOUNT"),
        pl.col("TOTALX").alias("TOTAL"),
    ])
    return grouped


# ============================================================================
# PROC REPORT RENDERING
# ============================================================================
def _render_report_rmel(df: pl.DataFrame) -> list[str]:
    """
    PROC REPORT WHERE=(FMTNAME IN ('A-RMEL','B-RMEA')) with BREAK AFTER
    FMTNAME / COMPUTE producing a dashed subtotal line per FMTNAME group.
    """
    lines = []
    subset = df.filter(pl.col("FMTNAME").is_in(["A-RMEL", "B-RMEA"])).sort(
        ["FMTNAME", "SIGN", "BNMCODE"]
    )
    if subset.is_empty():
        return lines

    for fmtname in subset["FMTNAME"].unique(maintain_order=True).sort().to_list():
        grp = subset.filter(pl.col("FMTNAME") == fmtname)
        amounx_sum = 0.0
        totalx_sum = 0.0
        for row in grp.iter_rows(named=True):
            buf = _new_buf()
            _put(buf, 1, str(row["FMTNAME"] or ""))
            _put(buf, 10, str(row["BNMCODE"] or ""))
            _put(buf, 26, str(row["DESC"] or "")[:40])
            _put(buf, 68, str(row["SIGN"] or ""))
            _put(buf, 74, _fmt_comma(row["AMOUNT"], 22, 2))
            _put(buf, 98, _fmt_comma(row["TOTAL"], 22, 2))
            lines.append(_line(buf))
            amounx_sum += row["AMOUNX"] or 0.0
            totalx_sum += row["TOTALX"] or 0.0

        buf = _new_buf()
        _put(buf, 3, "-" * 119)
        lines.append(_line(buf))

        buf = _new_buf()
        _put(buf, 12, f"TOTAL FOR {fmtname:<7s}")
        _put(buf, 74, _fmt_comma(amounx_sum, 24, 2))
        _put(buf, 98, _fmt_comma(totalx_sum, 24, 2))
        lines.append(_line(buf))

        buf = _new_buf()
        _put(buf, 74, "-" * 24)
        _put(buf, 98, "-" * 24)
        lines.append(_line(buf))

    return lines


def _render_report_rest(df: pl.DataFrame) -> list[str]:
    """PROC REPORT WHERE=(FMTNAME NOT IN ('A-RMEL','B-RMEA'))."""
    lines = []
    subset = df.filter(~pl.col("FMTNAME").is_in(["A-RMEL", "B-RMEA"])).sort(
        ["FMTNAME", "SIGN", "BNMCODE"]
    )
    for row in subset.iter_rows(named=True):
        buf = _new_buf()
        _put(buf, 1, str(row["FMTNAME"] or ""))
        _put(buf, 10, str(row["BNMCODE"] or ""))
        _put(buf, 26, str(row["DESC"] or "")[:40])
        _put(buf, 68, str(row["SIGN"] or ""))
        _put(buf, 74, _fmt_comma(row["AMOUNT"], 22, 2))
        _put(buf, 98, _fmt_comma(row["TOTAL"], 22, 2))
        lines.append(_line(buf))
    return lines


# ============================================================================
# PRTEL(I)  -- I in DAYA..DAYH
# ============================================================================
def prtel(day_code: str, *, reptmon: str, nowk: str, sdesc: str, rdate: str,
          gold_df: pl.DataFrame, rep4_df: pl.DataFrame, elw1: pl.DataFrame) -> list[str]:
    print(f"  [PIBELQ] prtel({day_code})...")

    # DATA ELG; SET ELG.GOLD&REPTMON&NOWK; IF ELDAY="&I";
    # GOLD is a single-row work dataset built inline in EIIWKAPE.py — not a
    # physical file — passed in as gold_df.
    elg = gold_df.filter(pl.col("ELDAY") == day_code)

    # DATA PMM; SET BNMK.TBL1&REPTMON&NOWK; IF ELDAY="&I";
    tbl1_sas = INPUT_BNMK_TBL1_DIR / f"tbl1{reptmon}{nowk}.sas7bdat"
    tbl1_cache = _load_cached(tbl1_sas, "BNMK_TBL1")

    # DATA REP6; SET BNMK.REP2&REPTMON&NOWK REP4; ... IF ELDAY="&I";
    rep2_sas = INPUT_BNMK_REP2_DIR / f"rep2{reptmon}{nowk}.sas7bdat"
    rep2_cache = _load_cached(rep2_sas, "BNMK_REP2")

    con = duckdb.connect(database=":memory:")
    pmm = con.execute(f"""
        SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(ELDAY AS VARCHAR) ELDAY,
               CAST(AMOUNT AS DOUBLE) AMOUNT
        FROM read_parquet('{tbl1_cache.as_posix()}')
        WHERE ELDAY = '{day_code}'
    """).pl()

    rep2_base = con.execute(f"""
        SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(UTSTY AS VARCHAR) UTSTY,
               CAST(UTREF AS VARCHAR) UTREF, CAST(ELDAY AS VARCHAR) ELDAY,
               CAST(AMOUNT AS DOUBLE) AMOUNT, CAST(NETAMT AS DOUBLE) NETAMT,
               CAST(COSTDED AS DOUBLE) COSTDED
        FROM read_parquet('{rep2_cache.as_posix()}')
    """).pl()
    con.close()

    rep6 = pl.concat([rep2_base, rep4_df.select(rep2_base.columns)], how="vertical")
    rep6 = rep6.with_columns(
        pl.when(pl.col("BNMCODE") == "3250000000000Y")
        .then(pl.col("NETAMT"))
        .otherwise(pl.col("AMOUNT"))
        .alias("AMOUNT")
    )
    rep6 = rep6.with_columns(pl.lit("4017100000000Y").alias("BNMCODE"))
    rep6 = rep6.filter(pl.col("ELDAY") == day_code).select(["BNMCODE", "AMOUNT"])

    elw = _elw_for_day(elw1, day_code)

    elw_stack = pl.concat(
        [
            elw.select(["BNMCODE", "AMOUNT"]),
            rep6.select(["BNMCODE", "AMOUNT"]),
            pmm.select(["BNMCODE", "AMOUNT"]),
            elg.select(["BNMCODE", "AMOUNT"]),
        ],
        how="vertical",
    )

    elitem = _el_catalogue(EL_DEFINITIONS)   # PROC SORT DATA=EL ...
    elw_final = _finalize_elw(elw_stack, elitem, drop_4019=True)
    elwt = _build_elwt(elw_final)
    combined = pl.concat([elwt.select(elw_final.columns), elw_final], how="vertical")

    lines = _title_lines(
        sdesc,
        f"DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR : {day_code}",
        f"REPORT DATE : {rdate}",
        "",
    )
    lines.extend(_render_report_rmel(combined))
    lines.extend(_render_report_rest(combined))
    return lines


# ============================================================================
# PRTELI(I)  -- I = DAYI
# ============================================================================
def prteli(day_code: str, *, reptmon: str, nowk: str, rdate: str,
           gold_df: pl.DataFrame, rep4_df: pl.DataFrame, elw1: pl.DataFrame) -> list[str]:
    print(f"  [PIBELQ] prteli({day_code})...")

    rep2_sas = INPUT_BNMK_REP2_DIR / f"rep2{reptmon}{nowk}.sas7bdat"
    rep2_cache = _load_cached(rep2_sas, "BNMK_REP2")

    tbl1_sas = INPUT_BNMK_TBL1_DIR / f"tbl1{reptmon}{nowk}.sas7bdat"
    tbl1_cache = _load_cached(tbl1_sas, "BNMK_TBL1")

    con = duckdb.connect(database=":memory:")
    rep2_base = con.execute(f"""
        SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(UTSTY AS VARCHAR) UTSTY,
               CAST(UTREF AS VARCHAR) UTREF, CAST(ELDAY AS VARCHAR) ELDAY,
               CAST(AMOUNT AS DOUBLE) AMOUNT, CAST(NETAMT AS DOUBLE) NETAMT,
               CAST(COSTDED AS DOUBLE) COSTDED
        FROM read_parquet('{rep2_cache.as_posix()}')
    """).pl()

    pmm = con.execute(f"""
        SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(ELDAY AS VARCHAR) ELDAY,
               CAST(AMOUNT AS DOUBLE) AMOUNT
        FROM read_parquet('{tbl1_cache.as_posix()}')
        WHERE ELDAY = '{day_code}'
    """).pl()
    con.close()

    # DATA REP7; SET BNMK.REP2&REPTMON&NOWK REP4; ... IF ELDAY="&I";
    rep7 = pl.concat([rep2_base, rep4_df.select(rep2_base.columns)], how="vertical")
    rep7 = rep7.with_columns(
        pl.when(pl.col("BNMCODE") == "3250000000000Y")
        .then(pl.col("NETAMT"))
        .otherwise(pl.col("AMOUNT"))
        .alias("AMOUNT")
    )
    rep7 = rep7.with_columns(pl.lit("4017100000000Y").alias("BNMCODE"))
    rep7 = rep7.filter(pl.col("ELDAY") == day_code)
    # PROC SUMMARY NWAY CLASS BNMCODE VAR AMOUNT
    rep7 = rep7.group_by("BNMCODE").agg(pl.col("AMOUNT").sum().alias("AMOUNT"))

    elw = _elw_for_day(elw1, day_code)
    elg = gold_df.filter(pl.col("ELDAY") == day_code)

    elw_stack = pl.concat(
        [
            elw.select(["BNMCODE", "AMOUNT"]),
            rep7.select(["BNMCODE", "AMOUNT"]),
            elg.select(["BNMCODE", "AMOUNT"]),
            pmm.select(["BNMCODE", "AMOUNT"]),
        ],
        how="vertical",
    )

    # WK='4' (literal); IF WK NE "&NOWK" THEN drop 4019000000000Y/4019100000000Y
    drop_4019 = (nowk != "4")

    elitem = _el_catalogue(ELI_DEFINITIONS)   # PROC SORT DATA=ELI ...
    elw_final = _finalize_elw(elw_stack, elitem, drop_4019=drop_4019)
    elwt = _build_elwt(elw_final)
    combined = pl.concat([elwt.select(elw_final.columns), elw_final], how="vertical")

    lines = _title_lines(
        f"DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR : {day_code}",
        f"REPORT DATE : {rdate}",
        "",
    )
    lines.extend(_render_report_rmel(combined))
    lines.extend(_render_report_rest(combined))
    return lines
