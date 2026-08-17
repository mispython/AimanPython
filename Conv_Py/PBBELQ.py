#!/usr/bin/env python3
"""
Program : PBBELQ.py
Purpose : Macro-equivalent module converted from PBBELQ (split from PBBELP).
          Builds and prints the detail daily Eligible Liabilities (EL)
          items for DAY A - DAY H (prtel) and DAY I (prteli), for Public
          Bank Berhad (PBB).

Called by EIBWKAPE.py via:
    from PBBELQ import build_elw1, prtel, prteli

Structural note (vs. PIBELQ):
    PBBELQ's DATA REP6/REP7 steps ("SET REP2 REP4") reuse the REP2/REP4
    datasets already built by the CALLING program (EIBWKAPE.py) at the
    point %INC PGM(PBBELQ) fires -- they do NOT re-read BNMK.REP2 fresh
    from disk (unlike PIBELQ, which reads BNMK.REP2&REPTMON&NOWK itself).
    prtel()/prteli() therefore take rep2_df/rep4_df as parameters.

    ELG.GOLD&REPTMON&NOWK is a REAL physical dataset in this program's
    JCL (SAP.PBB.GOLD.SASDATA, DISP=SHR) -- unlike EIIWKAPE, there is no
    inline seed step for it, so it is read here as a physical input.

============================================================================
PHYSICAL INPUT DATASETS USED BY THIS MODULE  (all .sas7bdat, cached to
Parquet on first read per EIBDLN1M.py's chunked-conversion pattern)
============================================================================
1. BNMK TBL1   (SAS libref BNMK -> SAP.PBB.DKAPITI.SASDATA)
   File     : tbl1<REPTMON><NOWK>.sas7bdat
   Path     : INPUT_BNMK_TBL1_DIR
   Used in  : prtel() / prteli() -> part of PMM dataset, filtered by ELDAY

2. BNMK DCI    (SAS libref BNMK -> SAP.PBB.DKAPITI.SASDATA)
   File     : dci<REPTMON><NOWK>.sas7bdat (REPTDATS column dropped)
   Path     : INPUT_BNMK_DCI_DIR
   Used in  : prtel() / prteli() -> part of PMM dataset, filtered by ELDAY
              (union order differs: TBL1 then DCI in prtel; DCI then TBL1
              in prteli -- exactly mirroring the two DATA PMM steps below)

3. BNM ELW     (SAS libref BNM  -> SAP.PBB.D&REPTYEAR)
   File     : elw<REPTMON><NOWK>.sas7bdat
   Path     : INPUT_BNM_ELW_DIR
   Used in  : build_elw1() -> primary/only ELW1 source (remap/split logic
              for BNMCODEs 32199xx, 4411100/4414000/4413000, 4019000000000Y;
              rows with BNMCODE='4929980000000Y' AND BRANCH>3000 are
              deleted -- no BNMB union exists in this program, unlike
              PIBELQ)

4. ELG GOLD    (SAS libref ELG  -> SAP.PBB.GOLD.SASDATA)
   File     : gold<REPTMON><NOWK>.sas7bdat
   Path     : INPUT_ELG_GOLD_DIR
   Used in  : prtel() / prteli() -> ELG dataset, filtered by ELDAY. Unlike
              EIIWKAPE (where GOLD is built inline and passed in), this is
              a genuine physical input resolved by this module itself.

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
STG_FIR  = Path("/stgsrcsys/host/uat/AII/KAPE")

INPUT_BNMK_TBL1_DIR = STG_FIR / "BNMK"              # bnmk_tbl1
INPUT_BNMK_DCI_DIR  = STG_FIR / "BNMK"              # bnmk_dci
INPUT_BNM_ELW_DIR   = STG_FIR / "BNM"               # bnm_elw
INPUT_ELG_GOLD_DIR  = STG_FIR / "ELG"               # elg_gold

# Parquet cache directory (shared with EIBWKAPE.py — same BNMK-family
# datasets may be reused across programs for the same REPTMON/NOWK)
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBWKAPE"
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

    # Read the whole SAS file into a pandas DataFrame
    try:
        df = pd.read_sas(sas_path, encoding="latin1")
    except Exception as e:
        raise RuntimeError(f"Failed to read SAS file {sas_path}: {e}")

    # Convert to PyArrow Table (this preserves the schema even if df is empty)
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Write to Parquet
    writer = pq.ParquetWriter(cache_path, table.schema, compression="snappy")
    writer.write_table(table)
    writer.close()

    print(f"  [{tag}] Done — {len(df):,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    """Resolve <parent>_<stem>.parquet cache under CACHE_DIR, converting if stale."""
    # Include directory name to avoid collisions (e.g., bnm_elw081.parquet vs bnmb_elw081.parquet)
    cache_name = f"{sas_path.parent.name}_{sas_path.stem}.parquet"
    cache_path = CACHE_DIR / cache_name
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
    """TITLE1..TITLEn -> all lines get ASA=' ' (space)."""
    lines = []
    for t in titles:
        buf = _new_buf()
        _put(buf, 1, t)
        lines.append(_line(buf, " "))   # always space, no '1'
    return lines


def _get_day_title_lines(include_bank: bool, day_code: str, rdate: str, sdesc: str = None) -> list[str]:
    """
    Return the title lines for a day's report (no column header).
    include_bank: True for DAYA-DAYH, False for DAYI.
    Includes a blank line after the report date.
    """
    titles = []
    if include_bank and sdesc:
        titles.append(sdesc)
    titles.append(f"DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR : {day_code}")
    titles.append(f"REPORT DATE : {rdate}")
    titles.append("")  # blank line after report date
    return _title_lines(*titles)

def _get_full_header(include_bank: bool, day_code: str, rdate: str, sdesc: str = None) -> list[str]:
    """
    Return the complete header: title lines + column header + dashed line.
    """
    title_lines = _get_day_title_lines(include_bank, day_code, rdate, sdesc)
    return title_lines + _render_table_header()


def _render_table_header() -> list[str]:
    """Emulate the PROC REPORT column headers and the dashed line."""
    lines = []
    buf = _new_buf()
    # Matches the SAS REPORT header exactly:
    # "  FMTNAME  BNMCODE         DESC                                      SIGN                  AMOUNT                   TOTAL"
    _put(buf, 1, "FMTNAME  BNMCODE         DESC                                      SIGN                  AMOUNT                   TOTAL")
    lines.append(_line(buf, " "))

    buf = _new_buf()
    _put(buf, 1, "-" * 119)   # 119 dashes starting at column 3 (two spaces before)
    lines.append(_line(buf, " "))
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
# BUILD ELW1
# DATA ELW1; SET BNM.ELW&REPTMON&NOWK; AMOUNT=ABS(AMOUNT);
#   IF BNMCODE='4929980000000Y' AND BRANCH > 3000 THEN DELETE;
#   ... remap/split logic (BNXCODE-based) ...
# No ELW2/BNMB union exists in this program (unlike PIBELQ).
# ============================================================================
def build_elw1(reptmon: str, nowk: str) -> pl.DataFrame:
    print("  Loading BNM ELW (build_elw1)...")

    elw1_sas = INPUT_BNM_ELW_DIR / f"elw{reptmon}{nowk}.sas7bdat"
    elw1_cache = _load_cached(elw1_sas, "BNM_ELW")

    con = duckdb.connect(database=":memory:")
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
        branch = row["BRANCH"]
        amount = row["AMOUNT"]

        # IF BNMCODE='4929980000000Y' AND BRANCH > 3000 THEN DELETE;
        if bnmcode == "4929980000000Y" and (branch or 0) > 3000:
            continue

        if bnmcode in ("3219902000000Y", "3219903000000Y", "3219912000000Y"):
            rows.append({**row, "BNMCODE": "3219910000000Y", "AMOUNT": amount})
        elif bnmcode in ("4411100000000Y", "4414000000000Y", "4413000000000Y"):
            # BNXCODE=BNMCODE;
            # IF BNXCODE='4411100000000Y' THEN BNMCODE='4411100000000Y'; (no-op)
            # IF BNXCODE='4414000000000Y' THEN BNMCODE='4414000000000Y'; (no-op)
            # IF BNXCODE^='4413000000000Y' THEN OUTPUT;
            # BNMCODE='4410000000000Y'; OUTPUT;
            if bnmcode != "4413000000000Y":
                rows.append({**row, "BNMCODE": bnmcode, "AMOUNT": amount})
            rows.append({**row, "BNMCODE": "4410000000000Y", "AMOUNT": amount})
        elif bnmcode == "4019000000000Y":
            rows.append({**row, "BNMCODE": "4019100000000Y", "AMOUNT": amount})
            rows.append({**row, "BNMCODE": "4019000000000Y", "AMOUNT": 0.00})
        else:
            rows.append({**row, "BNMCODE": bnmcode, "AMOUNT": amount})

    elw1 = pl.DataFrame(rows, schema=elw1_raw.schema) if rows else elw1_raw.clear()
    return elw1


def _elw_for_day(elw1: pl.DataFrame, day_code: str) -> pl.DataFrame:
    """PROC SORT DATA=ELW1(WHERE=(ELDAY="&I")) OUT=ELW (KEEP=BNMCODE AMOUNT)."""
    return (
        elw1.filter(pl.col("ELDAY") == day_code)
        .select(["BNMCODE", "AMOUNT"])
        .sort("BNMCODE")
    )


def _load_gold(reptmon: str, nowk: str, day_code: str) -> pl.DataFrame:
    """DATA ELG; SET ELG.GOLD&REPTMON&NOWK; IF ELDAY="&I"; -- physical input."""
    gold_sas = INPUT_ELG_GOLD_DIR / f"gold{reptmon}{nowk}.sas7bdat"
    gold_cache = _load_cached(gold_sas, "ELG_GOLD")

    con = duckdb.connect(database=":memory:")
    gold = con.execute(f"""
        SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(ELDAY AS VARCHAR) ELDAY,
               CAST(AMOUNT AS DOUBLE) AMOUNT
        FROM read_parquet('{gold_cache.as_posix()}')
        WHERE ELDAY = '{day_code}'
    """).pl()
    con.close()
    return gold


# ============================================================================
# MERGE WITH EL/ELI CATALOGUE + BUILD ELWT (SRR row) + FINAL RENDER
# (identical logic to PIBELQ — the PROC REPORT/merge section of PBBELQ and
# PIBELQ is byte-for-byte the same in the original SAS)
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
    merged = merged.filter(pl.col("IDX").str.strip_chars() != "")
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
        first_row = True
        for row in grp.iter_rows(named=True):
            buf = _new_buf()
            if first_row:
                _put(buf, 1, str(row["FMTNAME"] or ""))
                first_row = False
            _put(buf, 10, str(row["BNMCODE"] or ""))
            _put(buf, 26, str(row["DESC"] or "")[:40])
            _put(buf, 68, str(row["SIGN"] or ""))
            _put(buf, 74, _fmt_comma(row["AMOUNT"], 22, 2))
            _put(buf, 98, _fmt_comma(row["TOTAL"], 22, 2))
            lines.append(_line(buf))
            amounx_sum += row["AMOUNX"] or 0.0
            totalx_sum += row["TOTALX"] or 0.0

        # Dashed line after each group (separator)
        buf = _new_buf()
        _put(buf, 1, "-" * 119)
        lines.append(_line(buf))

        # Total line
        buf = _new_buf()
        _put(buf, 10, f"TOTAL FOR {fmtname:<7s}")
        _put(buf, 72, _fmt_comma(amounx_sum, 24, 2))
        _put(buf, 96, _fmt_comma(totalx_sum, 24, 2))
        lines.append(_line(buf))

        # Dashed line under total
        buf = _new_buf()
        _put(buf, 72, "-" * 24)
        _put(buf, 96, "-" * 24)
        lines.append(_line(buf))

    return lines


def _render_report_rest(df: pl.DataFrame) -> list[str]:
    """PROC REPORT WHERE=(FMTNAME NOT IN ('A-RMEL','B-RMEA'))."""
    lines = []
    subset = df.filter(~pl.col("FMTNAME").is_in(["A-RMEL", "B-RMEA"])).sort(
        ["FMTNAME", "SIGN", "BNMCODE"]
    )
    current_fmt = None
    for row in subset.iter_rows(named=True):
        buf = _new_buf()
        # Print FMTNAME only when it changes (SAS GROUP behavior)
        if row["FMTNAME"] != current_fmt:
            current_fmt = row["FMTNAME"]
            _put(buf, 1, str(row["FMTNAME"] or ""))
        # otherwise leave blank
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
          rep2_df: pl.DataFrame, rep4_df: pl.DataFrame, elw1: pl.DataFrame) -> list[str]:
    print(f"  [PBBELQ] prtel({day_code})...")

    # DATA ELG; SET ELG.GOLD&REPTMON&NOWK; IF ELDAY="&I";
    elg = _load_gold(reptmon, nowk, day_code)

    # DATA PMM; SET BNMK.TBL1&REPTMON&NOWK BNMK.DCI&REPTMON&NOWK(DROP=REPTDATS);
    #           IF ELDAY="&I";
    tbl1_sas = INPUT_BNMK_TBL1_DIR / f"tbl1{reptmon}{nowk}.sas7bdat"
    tbl1_cache = _load_cached(tbl1_sas, "BNMK_TBL1")

    dci_sas = INPUT_BNMK_DCI_DIR / f"dci{reptmon}{nowk}.sas7bdat"
    dci_cache = _load_cached(dci_sas, "BNMK_DCI")

    con = duckdb.connect(database=":memory:")
    tbl1_df = con.execute(f"""
        SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(ELDAY AS VARCHAR) ELDAY,
               CAST(AMOUNT AS DOUBLE) AMOUNT
        FROM read_parquet('{tbl1_cache.as_posix()}')
        WHERE ELDAY = '{day_code}'
    """).pl()

    # (DROP=REPTDATS) -- REPTDATS is simply not selected below
    dci_df = con.execute(f"""
        SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(ELDAY AS VARCHAR) ELDAY,
               CAST(AMOUNT AS DOUBLE) AMOUNT
        FROM read_parquet('{dci_cache.as_posix()}')
        WHERE ELDAY = '{day_code}'
    """).pl()
    con.close()

    pmm = pl.concat([tbl1_df, dci_df], how="vertical")   # TBL1 then DCI (matches PMM order)

    # DATA REP6; SET REP2 REP4; IF BNMCODE='3250000000000Y' THEN AMOUNT=NETAMT;
    # BNMCODE='4017100000000Y'; IF ELDAY="&I";
    rep6 = pl.concat([rep2_df, rep4_df.select(rep2_df.columns)], how="vertical")
    rep6 = rep6.with_columns(
        pl.when(pl.col("BNMCODE") == "3250000000000Y")
        .then(pl.col("NETAMT"))
        .otherwise(pl.col("AMOUNT"))
        .alias("AMOUNT")
    )
    rep6 = rep6.with_columns(pl.lit("4017100000000Y").alias("BNMCODE"))
    rep6 = rep6.filter(pl.col("ELDAY") == day_code).select(["BNMCODE", "AMOUNT"])

    elw = _elw_for_day(elw1, day_code)

    # DATA ELW; SET ELW REP6 PMM ELG;
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
    common_cols = ["BNMCODE", "FMTNAME", "DESC", "SIGN", "AMOUNT", "TOTAL", "AMOUNX", "TOTALX"]
    elwt = elwt.select(common_cols)
    elw_final = elw_final.select(common_cols)
    combined = pl.concat([elwt, elw_final], how="vertical")

    # Build the full header (titles + column header + dashed line) for this day
    full_header = _get_full_header(True, day_code, rdate, sdesc)

    # Render the A-RMEL and B-RMEA groups (includes subtotal lines)
    rmel_lines = _render_report_rmel(combined)

    # Render the remaining groups
    rest_lines = _render_report_rest(combined)

    # Assemble output
    lines = full_header + rmel_lines
    if rest_lines:
        # Add only the title lines (no column header) before the rest groups
        title_only = _get_day_title_lines(True, day_code, rdate, sdesc)
        # Add a blank line between the subtotal and the repeated title? Actually the subtotal already has short dashes,
        # and the original has a blank line after the subtotal and before the repeated title.
        # We'll add an extra blank line.
        lines += [""] + title_only + rest_lines
    else:
        lines += rest_lines
    return lines


# ============================================================================
# PRTELI(I)  -- I = DAYI
# ============================================================================
def prteli(day_code: str, *, reptmon: str, nowk: str, rdate: str,
           rep2_df: pl.DataFrame, rep4_df: pl.DataFrame, elw1: pl.DataFrame) -> list[str]:
    print(f"  [PBBELQ] prteli({day_code})...")

    # DATA REP7; SET REP2 REP4; IF BNMCODE='3250000000000Y' THEN AMOUNT=NETAMT;
    # BNMCODE='4017100000000Y'; IF ELDAY="&I";
    rep7 = pl.concat([rep2_df, rep4_df.select(rep2_df.columns)], how="vertical")
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

    # DATA PMM; SET BNMK.DCI&REPTMON&NOWK(DROP=REPTDATS) BNMK.TBL1&REPTMON&NOWK;
    #           IF ELDAY="&I";  -- note DCI-then-TBL1 order (swapped vs prtel)
    dci_sas = INPUT_BNMK_DCI_DIR / f"dci{reptmon}{nowk}.sas7bdat"
    dci_cache = _load_cached(dci_sas, "BNMK_DCI")

    tbl1_sas = INPUT_BNMK_TBL1_DIR / f"tbl1{reptmon}{nowk}.sas7bdat"
    tbl1_cache = _load_cached(tbl1_sas, "BNMK_TBL1")

    con = duckdb.connect(database=":memory:")
    dci_df = con.execute(f"""
        SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(ELDAY AS VARCHAR) ELDAY,
               CAST(AMOUNT AS DOUBLE) AMOUNT
        FROM read_parquet('{dci_cache.as_posix()}')
        WHERE ELDAY = '{day_code}'
    """).pl()

    tbl1_df = con.execute(f"""
        SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(ELDAY AS VARCHAR) ELDAY,
               CAST(AMOUNT AS DOUBLE) AMOUNT
        FROM read_parquet('{tbl1_cache.as_posix()}')
        WHERE ELDAY = '{day_code}'
    """).pl()
    con.close()

    pmm = pl.concat([dci_df, tbl1_df], how="vertical")

    # DATA ELG; SET ELG.GOLD&REPTMON&NOWK; IF ELDAY="&I";
    elg = _load_gold(reptmon, nowk, day_code)

    # DATA ELW; SET ELW REP7 ELG PMM;
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
    common_cols = ["BNMCODE", "FMTNAME", "DESC", "SIGN", "AMOUNT", "TOTAL", "AMOUNX", "TOTALX"]
    elwt = elwt.select(common_cols)
    elw_final = elw_final.select(common_cols)
    combined = pl.concat([elwt, elw_final], how="vertical")

    # Build the full header for DAYI (no bank name)
    full_header = _get_full_header(False, day_code, rdate)   # no bank name

    rmel_lines = _render_report_rmel(combined)
    rest_lines = _render_report_rest(combined)

    lines = full_header + rmel_lines
    if rest_lines:
        title_only = _get_day_title_lines(False, day_code, rdate)
        lines += [""] + title_only + rest_lines
    else:
        lines += rest_lines
    return lines
