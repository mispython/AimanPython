#!/usr/bin/env python3
"""
Program : EIBMEPC2.py
Purpose : Monthly BNM loan-repayment-by-customer reports for PBB and PIBB.
          Consolidates repayment channels (CASH, HOUSE/LOCAL CHEQUE, ATM,
          INTERBANK, RENTAS, CDM, CDT) from deposit/loan payment files, CTCS
          cheque data, EFT loan transactions and weekly RENTAS files, tags
          each record with a loan product (HL/VL/PL/CC/OT) and writes:
            - PBB  transaction listing        (SAP.PBB.EIBMEPC2.TRXLIST.TEXT)
            - PIBB transaction listing        (SAP.PIBB.EIBMEPC2.TRXLIST.TEXT)
            - CDM  transaction listing        (SAP.EIBMEPC2.CDMLIST.TEXT)
            - PBB  summary                    (SAP.PBB.EIBMEPC2.SUM.TEXT)
            - PIBB summary                    (SAP.PIBB.EIBMEPC2.SUM.TEXT)
            - CDM  summary                    (SAP.EIBMEPC2.CDMSUM.TEXT)

Dependency / notes:
    JCL //DELETE step (IEFBR14) is pure dataset housekeeping: no Python
    equivalent; output files are simply rewritten on each run.
    JCL //BNM and //IBNM DDs (SAP.PBB.EPCUWH.MONTH(+1) / SAP.PIBB...) are SAS
    libraries that only held work tables (BNM.xxx / IBNM.xxx); here they are
    in-memory Polars frames, so no physical file is produced.
    JCL //PGM DD (SAP.BNM.PROGRAM) is not referenced by any statement of this
    program.
    LOAN.REPTDATE is replaced by get_reptdate() (no reptdate.parquet exists).
    All output files are RECFM=FB / LRECL=1000 reports without ASA control
    characters, so each record is padded to 1000 bytes and no ASA byte is added.
"""

import gc
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================================
# STEP 1: REPORT DATE  (replaces DATA REPTDATE; SET LOAN.REPTDATE)
# ============================================================================
def get_reptdate(run_date: Optional[date] = None) -> date:
    """Monthly job (JCL: 'TO RUN MONTHLY'): the report date is the last day of
    the month preceding the run date."""
    run_date = run_date or date.today()
    return run_date.replace(day=1) - timedelta(days=1)


print("Step 1: Deriving report date...")
REPTDATE = get_reptdate()

# CALL SYMPUT tokens
REPTMON  = f"{REPTDATE.month:02d}"          # PUT(MONTH(REPTDATE),Z2.)
REPTYEAR = REPTDATE.strftime("%y")          # PUT(REPTDATE,YEAR2.)
REPTMON1 = f"{REPTMON}01"                   # week-1 member suffix
REPTMON2 = f"{REPTMON}02"
REPTMON3 = f"{REPTMON}03"
REPTMON4 = f"{REPTMON}04"
REPORTDT = REPTDATE.strftime("%d/%m/%Y")    # PUT(REPTDATE,DDMMYY10.)
# RDATE (SAS date number), RDATE1 (first day of month, from SREPTDATE) and
# PREPTYEAR (derived from PRVRPTDATE, a variable that does not exist in
# LOAN.REPTDATE) are SYMPUT'd in the original but never referenced again in
# the program body -- dead; kept for documentation parity only.
RDATE    = (REPTDATE - date(1960, 1, 1)).days
RDATE1   = (REPTDATE.replace(day=1) - date(1960, 1, 1)).days

print(f"  REPTDATE: {REPORTDT}   REPTYEAR/MON: {REPTYEAR}/{REPTMON}")

# OPTIONS YEARCUTOFF=1930 NOCENTER NODATE;  (SAS session options, no equivalent)

# ============================================================================
# PATH CONFIGURATION  (every physical input / output declared independently)
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII/EIBMEPC2")
STG_DIR2 = Path("/stgsrcsys/host/uat/AII/MNILN")

# --- Inputs (.sas7bdat) -----------------------------------------------------
# //DPLP DD  SAP.PBB.EPCU.LOANPYMT  -> member DPLP&REPTMON
INPUT_DPLP_FILE = STG_DIR / "DPLP" / f"dplp{REPTMON}.sas7bdat"

# //DPCC DD  SAP.PBB.EPCU.CRDTPYMT  -> member DPCC&REPTMON
INPUT_DPCC_FILE = STG_DIR / "DPCC" / f"dpcc{REPTMON}.sas7bdat"

# //LNLP1 DD SAP.PBB.CRM.LNTRNSAC(0) -> members LNTRAX&REPTYEAR&REPTMON1..4
INPUT_LNLP_FILES = [
    STG_DIR / "LNLP1" / f"lntrax{REPTYEAR}{tok}.sas7bdat"
    for tok in (REPTMON1, REPTMON2, REPTMON3, REPTMON4)
]

# //DPTRN DD SAP.PBB.CRM.DPTRNSAC(0) -> members DPBTRAN&REPTYEAR&REPTMON1..4
INPUT_DPTRN_FILES = [
    STG_DIR / "DPTRN" / f"dpbtran{REPTYEAR}{tok}.sas7bdat"
    for tok in (REPTMON1, REPTMON2, REPTMON3, REPTMON4)
]

# //CTCS DD  SAP.PBB.EPCU.CTCS -> member CTCS&REPTMON
INPUT_CTCS_FILE = STG_DIR / "CTCS" / f"ctcs{REPTMON}.sas7bdat"

# //LOAN DD  SAP.PBB.MNILN(0)  -> LNNOTE   (REPTDATE member not needed)
INPUT_LOAN_LNNOTE_FILE  = STG_DIR2 / "PBB"  / "lnnote.sas7bdat"

# //ILOAN DD SAP.PIBB.MNILN(0) -> LNNOTE
INPUT_ILOAN_LNNOTE_FILE = STG_DIR2 / "PIBB" / "ilnnote.sas7bdat"

# //BNMW1..4 DD SAP.PBB.EPCUWH.WEEKLY(-3..0)  -> RENCC, RENLN
INPUT_BNMW1_RENCC_FILE = STG_DIR / "BNMW1" / "rencc.sas7bdat"
INPUT_BNMW2_RENCC_FILE = STG_DIR / "BNMW2" / "rencc.sas7bdat"
INPUT_BNMW3_RENCC_FILE = STG_DIR / "BNMW3" / "rencc.sas7bdat"
INPUT_BNMW4_RENCC_FILE = STG_DIR / "BNMW4" / "rencc.sas7bdat"

INPUT_BNMW1_RENLN_FILE = STG_DIR / "BNMW1" / "renln.sas7bdat"
INPUT_BNMW2_RENLN_FILE = STG_DIR / "BNMW2" / "renln.sas7bdat"
INPUT_BNMW3_RENLN_FILE = STG_DIR / "BNMW3" / "renln.sas7bdat"
INPUT_BNMW4_RENLN_FILE = STG_DIR / "BNMW4" / "renln.sas7bdat"

# //IBNMW1..4 DD SAP.PIBB.EPCUWH.WEEKLY(-3..0) -> RENLN
INPUT_IBNMW1_RENLN_FILE = STG_DIR / "IBNMW1" / "renln.sas7bdat"
INPUT_IBNMW2_RENLN_FILE = STG_DIR / "IBNMW2" / "renln.sas7bdat"
INPUT_IBNMW3_RENLN_FILE = STG_DIR / "IBNMW3" / "renln.sas7bdat"
INPUT_IBNMW4_RENLN_FILE = STG_DIR / "IBNMW4" / "renln.sas7bdat"

# --- Parquet cache / work ---------------------------------------------------
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMEPC2"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
WORK_DIR = BASE_DIR / "input" / "work" / "EIBMEPC2"
TMP_DIR  = WORK_DIR / "tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

# --- Outputs (fixed dataset names, no date component) -----------------------
OUTPUT_DIR = BASE_DIR / "output" / "EIBMEPC2"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PBB_TRX_FILE  = OUTPUT_DIR / "EIBMEPC2_PBB_TRXLIST.txt"    # //TRX
OUTPUT_PIBB_TRX_FILE = OUTPUT_DIR / "EIBMEPC2_PIBB_TRXLIST.txt"   # //ITRX
OUTPUT_CDM_TRX_FILE  = OUTPUT_DIR / "EIBMEPC2_CDMLIST.txt"        # //CDM
OUTPUT_PBB_SUM_FILE  = OUTPUT_DIR / "EIBMEPC2_PBB_SUM.txt"        # //SUM
OUTPUT_PIBB_SUM_FILE = OUTPUT_DIR / "EIBMEPC2_PIBB_SUM.txt"       # //ISUM
OUTPUT_CDM_SUM_FILE  = OUTPUT_DIR / "EIBMEPC2_CDMSUM.txt"         # //CDMSUM

CHUNK_ROWS  = 500_000
LRECL       = 1000            # DCB=(LRECL=1000,RECFM=FB)
WRITE_BATCH = 500_000

# ============================================================================
# CACHE: .sas7bdat -> PARQUET
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _build_schema(df: "pd.DataFrame") -> pa.Schema:
    fields = []
    for col, dtype in df.dtypes.items():
        if dtype == "object":
            pa_type = pa.string()
        elif pd.api.types.is_integer_dtype(dtype):
            pa_type = pa.int64()
        elif pd.api.types.is_float_dtype(dtype):
            pa_type = pa.float64()
        else:
            pa_type = pa.from_numpy_dtype(dtype)
        fields.append(pa.field(col, pa_type))
    return pa.schema(fields)


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if schema is None:
            schema = _build_schema(chunk)
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer is not None:
        writer.close()
    else:
        # 0-row source: chunked reader yielded nothing; write an empty parquet.
        empty = pd.read_sas(sas_path, encoding="latin1")
        schema = _build_schema(empty)
        pq.write_table(pa.Table.from_pandas(empty, schema=schema, preserve_index=False),
                       cache_path)
    print(f"  [{tag}] Done - {total:,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    # Parent folder name is part of the cache name: several inputs share the
    # same member name (e.g. RENCC / RENLN / LNNOTE) in different libraries.
    cache_path = CACHE_DIR / f"{sas_path.parent.name}__{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


print("\nStep 2: Caching input SAS datasets to Parquet...")
DPLP_CACHE  = _load_cached(INPUT_DPLP_FILE, "DPLP")
DPCC_CACHE  = _load_cached(INPUT_DPCC_FILE, "DPCC")
LNLP_CACHES = [_load_cached(p, f"LNLP{i}") for i, p in enumerate(INPUT_LNLP_FILES, 1)]
DPTRN_CACHES = [_load_cached(p, f"DPTRN{i}") for i, p in enumerate(INPUT_DPTRN_FILES, 1)]
CTCS_CACHE  = _load_cached(INPUT_CTCS_FILE, "CTCS")
LOAN_LNNOTE_CACHE  = _load_cached(INPUT_LOAN_LNNOTE_FILE, "LOAN.LNNOTE")
ILOAN_LNNOTE_CACHE = _load_cached(INPUT_ILOAN_LNNOTE_FILE, "ILOAN.LNNOTE")
RENCC_CACHES = [
    _load_cached(p, f"BNMW{i}.RENCC") for i, p in enumerate(
        (INPUT_BNMW1_RENCC_FILE, INPUT_BNMW2_RENCC_FILE,
         INPUT_BNMW3_RENCC_FILE, INPUT_BNMW4_RENCC_FILE), 1)
]
RENLN_CACHES = [
    _load_cached(p, f"BNMW{i}.RENLN") for i, p in enumerate(
        (INPUT_BNMW1_RENLN_FILE, INPUT_BNMW2_RENLN_FILE,
         INPUT_BNMW3_RENLN_FILE, INPUT_BNMW4_RENLN_FILE), 1)
]
IRENLN_CACHES = [
    _load_cached(p, f"IBNMW{i}.RENLN") for i, p in enumerate(
        (INPUT_IBNMW1_RENLN_FILE, INPUT_IBNMW2_RENLN_FILE,
         INPUT_IBNMW3_RENLN_FILE, INPUT_IBNMW4_RENLN_FILE), 1)
]

# ============================================================================
# DUCKDB (reads Parquet cache) + SAS-SEMANTIC HELPERS
# ============================================================================
con = duckdb.connect()
con.execute("SET memory_limit='16GB'")          # about 50% of server RAM
con.execute(f"SET temp_directory='{TMP_DIR.as_posix()}'")
con.execute("SET threads=4")
con.execute("SET preserve_insertion_order=true")


def _read(cache_path: Path, columns: str = "*", where: Optional[str] = None) -> pl.DataFrame:
    sql = f"SELECT {columns} FROM read_parquet('{cache_path.as_posix()}')"
    if where:
        sql += f" WHERE {where}"
    return con.execute(sql).pl()


def _concat(frames: list) -> pl.DataFrame:
    """SET a b c; -- stack datasets in order."""
    return pl.concat(frames, how="diagonal_relaxed")


def _c(col: str) -> pl.Expr:
    """SAS character semantics: missing -> blank, trailing blanks ignored."""
    return pl.col(col).cast(pl.Utf8).fill_null("").str.strip_chars_end()


def _isin(col: str, values) -> pl.Expr:
    """Numeric IN (...) with SAS semantics (missing never matches)."""
    return pl.col(col).cast(pl.Float64).is_in([float(v) for v in values]).fill_null(False)


def _ssum(col: str) -> pl.Expr:
    """PROC SUMMARY SUM=: all-missing group sums to missing, not 0."""
    return (pl.when(pl.col(col).is_not_null().any())
              .then(pl.col(col).sum())
              .otherwise(None)
              .alias(col))


def _round_cents(col: str) -> pl.Expr:
    """ROUND(x,0.01) with SAS-like fuzz (half away from zero)."""
    y = pl.col(col).cast(pl.Float64) * 100
    return ((y + y.sign() * y.abs() * 1e-12).round(0) / 100).alias(col)


def _join(left: pl.DataFrame, right: pl.DataFrame, on: list) -> pl.DataFrame:
    """Left join where missing keys match each other (as in SAS BY matching)."""
    try:
        return left.join(right, on=on, how="left", nulls_equal=True)
    except TypeError:
        return left.join(right, on=on, how="left", join_nulls=True)


def _align_key_types(a: pl.DataFrame, b: pl.DataFrame, keys: list):
    for k in keys:
        ta, tb = a.schema[k], b.schema[k]
        if ta != tb:
            target = pl.Float64 if (ta.is_numeric() and tb.is_numeric()) else pl.Utf8
            a = a.with_columns(pl.col(k).cast(target))
            b = b.with_columns(pl.col(k).cast(target))
    return a, b


def sas_merge(a: pl.DataFrame, b: pl.DataFrame, keys: list, inner: bool = False) -> pl.DataFrame:
    """Emulates `MERGE a(IN=A) b(IN=B); BY keys; IF A;` (inner=False) or
    `IF A & B;` (inner=True), including the PROC SORT BY that precedes it.
      - inputs are stably sorted by `keys` (missing first);
      - every BY group present in A yields max(m, n) rows (m = rows from A,
        n = rows from B); a dataset that runs out keeps its last values and
        its IN= flag stays 1 for the rest of the group;
      - variables present in both are taken from B while B still contributes
        a fresh observation, otherwise A's own value stays;
      - groups absent from B give missing B-only variables (IN=B = 0);
      - groups absent from A are dropped (IF A).
    """
    a, b = _align_key_types(a, b, keys)
    a = a.sort(keys, maintain_order=True)
    b = b.sort(keys, maintain_order=True)
    a_cols = list(a.columns)
    b_cols = [c for c in b.columns if c not in keys]
    common = [c for c in b_cols if c in a_cols]
    b_only = [c for c in b_cols if c not in a_cols]

    seq = pl.int_range(pl.len(), dtype=pl.Int64).over(keys) + 1
    a = a.with_columns(seq.alias("_ia"))
    b = b.with_columns(seq.alias("_ib"))

    cnt_a = a.group_by(keys, maintain_order=True).agg(pl.len().cast(pl.Int64).alias("_m"))
    cnt_b = b.group_by(keys, maintain_order=True).agg(pl.len().cast(pl.Int64).alias("_n"))
    grid = _join(cnt_a, cnt_b, keys).with_columns(pl.col("_n").fill_null(0))
    if inner:
        grid = grid.filter(pl.col("_n") > 0)            # IF A & B: B must exist in the group
    grid = (grid.with_columns(pl.int_ranges(1, pl.max_horizontal("_m", "_n") + 1).alias("_i"))
                .explode("_i")
                .with_columns(pl.min_horizontal("_i", "_m").alias("_ia"),     # A row (last one retained)
                              pl.min_horizontal("_i", "_n").alias("_ib")))    # B row (last one retained)

    b = b.rename({c: f"{c}__b" for c in b_cols})
    out = _join(grid, a, keys + ["_ia"])
    out = _join(out, b, keys + ["_ib"])
    fresh_b = pl.col("_i") <= pl.col("_n")
    out = out.with_columns(
        [pl.when(fresh_b).then(pl.col(f"{c}__b")).otherwise(pl.col(c)).alias(c) for c in common]
        + [pl.col(f"{c}__b").alias(c) for c in b_only])
    out = out.sort(keys + ["_i"], maintain_order=True)
    return out.select(a_cols + b_only)


# ============================================================================
# FORMATS  (PROC FORMAT)
# ============================================================================
_DESC_RANGES = [
    ("HL", [(4, 7), (60, 62), (70, 70), (200, 299), (600, 600), (911, 911)]),
    ("VL", [(15, 15), (20, 20), (71, 71), (72, 72), (380, 380), (381, 381),
            (700, 700), (705, 705), (720, 720), (725, 725)]),
    ("PL", [(25, 34), (73, 79), (303, 303), (306, 308), (311, 311), (313, 313),
            (320, 340), (355, 358), (364, 364), (365, 365), (367, 367),
            (369, 369), (391, 391)]),
]
_IDESC_RANGES = [
    ("HL", [(102, 102), (105, 106), (110, 119), (124, 124), (139, 139), (142, 142),
            (145, 145), (150, 152), (156, 156), (170, 170), (175, 175), (400, 400),
            (409, 410), (412, 415), (423, 423), (466, 466), (469, 469), (650, 651),
            (664, 664)]),
    ("VL", [(103, 104), (107, 108), (128, 128), (130, 132), (196, 196)]),
    ("PL", [(122, 122), (135, 138), (194, 195), (419, 420), (422, 422), (424, 424),
            (426, 426), (464, 465), (468, 468), (652, 653), (668, 669), (672, 675)]),
]
SEQ_LABELS = {1: "CASH", 2: "HOUSE CHEQUE", 3: "LOCAL CHEQUE", 4: "ATM",
              6: "RENTAS", 7: "CDM", 8: "CDT"}          # 5 has no label in SEQFMT
PDESC_LABELS = {"HL": "HOUSING LOANS", "VL": "VEHICLE LOANS", "PL": "PERSONAL LOANS",
                "CC": "CREDIT AND CHARGE CARDS",
                "MS": "OTHER TYPES OF LOANS", "OT": "OTHER TYPES OF LOANS"}


def _put_loan_fmt(col: str, ranges: list) -> pl.Expr:
    """PUT(LOANTYPE, DESC./IDESC.): missing -> 'MS', unmatched -> 'OT'."""
    x = pl.col(col).cast(pl.Float64)
    expr = pl.when(x.is_null()).then(pl.lit("MS"))
    for label, spans in ranges:
        cond = None
        for lo, hi in spans:
            c = x.is_between(lo, hi)
            cond = c if cond is None else (cond | c)
        expr = expr.when(cond).then(pl.lit(label))
    return expr.otherwise(pl.lit("OT"))


def _split_entity(df: pl.DataFrame):
    """Route rows to PBB (DESC.) or PIBB (IDESC.) by COSTCTR; set PROD."""
    cc = pl.col("COSTCTR").cast(pl.Float64).fill_null(0)
    islamic = ((cc > 3000) & (cc < 3999)) | cc.is_in([4043.0, 4048.0])
    df = df.with_columns(
        islamic.alias("_isl"),
        pl.when(islamic).then(_put_loan_fmt("LOANTYPE", _IDESC_RANGES))
          .otherwise(_put_loan_fmt("LOANTYPE", _DESC_RANGES)).alias("PROD"))
    pbb = df.filter(~pl.col("_isl")).drop("_isl")
    pibb = df.filter(pl.col("_isl")).drop("_isl")
    return pbb, pibb


# ============================================================================
# STEP 3: LNNOTE  (SET LOAN.LNNOTE ILOAN.LNNOTE; KEEP=...)
# ============================================================================
print("\nStep 3: Building LNNOTE...")
_LNNOTE_COLS = "ACCBRCH, ACCTNO, NOTENO, LOANTYPE, COSTCTR"
lnnote = _concat([_read(LOAN_LNNOTE_CACHE, _LNNOTE_COLS),
                  _read(ILOAN_LNNOTE_CACHE, _LNNOTE_COLS)])
print(f"  LNNOTE rows: {lnnote.height:,}")

# ============================================================================
# STEP 4: CASH - LN   (BNM.DPLP, BNM.LNLP -> OTCLN)
# ============================================================================
print("\nStep 4: CASH - LN...")
# dplp = _read(DPLP_CACHE)                                       # BNM.DPLP
dplp = _read(DPLP_CACHE, where=f"TRANDT < {RDATE}")            # TEMP TEST
lnlp = _concat([_read(p) for p in LNLP_CACHES])                # BNM.LNLP
lnlp = lnlp.with_columns(pl.col("REPTDATE").alias("TRANDT"))   # TRANDT=REPTDATE (data column)

otcln = sas_merge(lnlp, dplp, ["ACCTNO", "TRANDT", "TRANAMT"], inner=True)
otcln = sas_merge(otcln, lnnote, ["ACCTNO", "NOTENO"])
otcln = otcln.with_columns(pl.lit("CASH").alias("ITEM"), pl.lit(1).alias("SEQ"))
otcln_pbb, otcln_pibb = _split_entity(otcln)
del dplp, otcln
print(f"  OTCLN: PBB {otcln_pbb.height:,}  PIBB {otcln_pibb.height:,}")

# ============================================================================
# STEP 5: CASH - CC   (BNM.OTCCC)
# ============================================================================
print("\nStep 5: CASH - CC...")
# otccc = _read(DPCC_CACHE).with_columns(
otccc = _read(DPCC_CACHE, where=f"TRANDT < {RDATE}").with_columns(   # TEMP TEST
    pl.lit("CC").alias("PROD"), pl.lit("CASH").alias("ITEM"), pl.lit(1).alias("SEQ"))
print(f"  OTCCC: {otccc.height:,}")

# ============================================================================
# STEP 6: CTCS
# ============================================================================
print("\nStep 6: CTCS...")
ctcs = _read(CTCS_CACHE).filter(_c("IND") != "OC")      # exclude overseas cheque
ctcs = sas_merge(ctcs, lnnote, ["ACCTNO", "NOTENO"])
_prodind, _ind = _c("PRODIND"), _c("IND")
ctcs = ctcs.with_columns(
    _round_cents("TRANAMT"),
    pl.when((_prodind == "O") & (_ind == "HC")).then(pl.lit("HOUSE CHEQUE"))
      .when((_prodind == "O") & (_ind == "LC")).then(pl.lit("LOCAL CHEQUE"))
      .when(_prodind == "C").then(pl.lit("CDM")).alias("ITEM"),
    pl.when((_prodind == "O") & (_ind == "HC")).then(pl.lit(2))
      .when((_prodind == "O") & (_ind == "LC")).then(pl.lit(3))
      .when(_prodind == "C").then(pl.lit(7)).alias("SEQ"))

# DPTRX: TRANCODE=876 debit transactions, renamed for the CTCS match
dptrx = _concat([
    _read(p, "REPTDATE AS TRANDT, TRANAMT, CHQNO AS CHEQNO, ACCTNO AS DEBACCT",
          "TRANCODE = 876")
    for p in DPTRN_CACHES])
dptrx = dptrx.with_columns(_round_cents("TRANAMT"))
_DPTRX_KEYS = ["TRANDT", "TRANAMT", "CHEQNO", "DEBACCT"]
dptrx = (dptrx.unique(subset=_DPTRX_KEYS, keep="first", maintain_order=True)   # NODUPKEYS
              .sort(_DPTRX_KEYS, maintain_order=True))
ctcs = sas_merge(ctcs, dptrx, ["TRANDT", "TRANAMT", "CHEQNO"])
ctcs_pbb, ctcs_pibb = _split_entity(ctcs)
del ctcs, dptrx
print(f"  CTCS: PBB {ctcs_pbb.height:,}  PIBB {ctcs_pibb.height:,}")

# ============================================================================
# STEP 7: ELECTRONIC FUND TRANSFER
# ============================================================================
print("\nStep 7: Electronic fund transfer...")
_tc_ok = _isin("TRANCODE", (614, 668))
eft_all = lnlp.with_columns(
    pl.when(_tc_ok & _isin("CHANNEL", (501, 502))).then(pl.lit("ATM"))
      .when(_tc_ok & _isin("CHANNEL", (521, 522))).then(pl.lit("INTERBANK"))
      .when(_tc_ok & _isin("CHANNEL", (513, 514))).then(pl.lit("CDT")).alias("ITEM"),
    pl.when(_tc_ok & _isin("CHANNEL", (501, 502))).then(pl.lit(4))
      .when(_tc_ok & _isin("CHANNEL", (521, 522))).then(pl.lit(5))
      .when(_tc_ok & _isin("CHANNEL", (513, 514))).then(pl.lit(8)).alias("SEQ"),
).filter(pl.col("ITEM").is_not_null())                          # EFT614N668
del lnlp

_EFT_KEYS = ["ACCTNO", "NOTENO", "USERID", "CHANNEL"]
eft614 = eft_all.filter(pl.col("TRANCODE").cast(pl.Float64) == 614)
# PROC SUMMARY NWAY: _TYPE_ / _FREQ_ are never referenced downstream -> omitted.
eftamt = eft_all.group_by(_EFT_KEYS, maintain_order=True).agg(_ssum("TRANAMT"))
eft = sas_merge(eft614, eftamt, _EFT_KEYS)
eft = sas_merge(eft, lnnote, ["ACCTNO", "NOTENO"])
eft_pbb, eft_pibb = _split_entity(eft)
del eft_all, eft614, eftamt, eft
print(f"  EFT: PBB {eft_pbb.height:,}  PIBB {eft_pibb.height:,}")

# ============================================================================
# STEP 8: RENTAS
# ============================================================================
print("\nStep 8: RENTAS...")
rencc = _concat([_read(p) for p in RENCC_CACHES])                       # BNM.RENCC
renln = _concat([_read(p) for p in RENLN_CACHES + IRENLN_CACHES])       # BNM.RENLN
renln = sas_merge(renln, lnnote.drop("LOANTYPE"), ["ACCTNO", "NOTENO"])  # LNNOTE(DROP=LOANTYPE)
renln_pbb, renln_pibb = _split_entity(renln)
del renln, lnnote
print(f"  RENCC: {rencc.height:,}  RENLN: PBB {renln_pbb.height:,}  PIBB {renln_pibb.height:,}")

# ============================================================================
# STEP 9: CONSOLIDATION  (BNM.TRANX2 / IBNM.TRANX2, sorted BY SEQ)
# ============================================================================
print("\nStep 9: Consolidation...")


def _build_tranx2(frames: list, entity: str) -> pl.DataFrame:
    df = _concat(frames)
    # *WHERE PROD NOT EQ 'OT';
    df = df.with_columns(
        (pl.col("TRANAMT").cast(pl.Float64) / 1000).alias("TRANAMT1"),
        pl.lit(entity).alias("ENTITY"),
        pl.col("PROD").cast(pl.Utf8).replace(PDESC_LABELS).alias("PDESC"),
        pl.col("SEQ").cast(pl.Int64).cast(pl.Utf8)
          .replace({str(k): v for k, v in SEQ_LABELS.items()})
          .fill_null(".").alias("SEQFMT"))
    return df.sort("SEQ", maintain_order=True)


bnm_tranx2 = _build_tranx2(
    [ctcs_pbb, renln_pbb, rencc, otcln_pbb, otccc, eft_pbb], "PBB")
ibnm_tranx2 = _build_tranx2(
    [ctcs_pibb, renln_pibb, otcln_pibb, eft_pibb], "PIBB")
del ctcs_pbb, renln_pbb, rencc, otcln_pbb, otccc, eft_pbb
del ctcs_pibb, renln_pibb, otcln_pibb, eft_pibb
print(f"  BNM.TRANX2: {bnm_tranx2.height:,}   IBNM.TRANX2: {ibnm_tranx2.height:,}")

# ============================================================================
# OUTPUT HELPERS  (SAS list-output PUT semantics)
# ============================================================================
def _best12(x: float) -> str:
    """Scalar BEST12. (left-aligned, as written by list-output PUT)."""
    if x is None or x != x:
        return "."
    if x == 0:
        return "0"
    ax = abs(x)
    if ax >= 1e12 or ax < 1e-5:
        return f"{x:.5E}"
    sign = 1 if x < 0 else 0
    d = max(12 - sign - len(str(int(ax))) - 1, 0)
    s = f"{x:.{d}f}"
    while len(s) > 12 and d > 0:
        d -= 1
        s = f"{x:.{d}f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _num_expr(col: str) -> pl.Expr:
    """Vectorised BEST12. for a numeric column (missing -> '.')."""
    x = pl.col(col).cast(pl.Float64)
    missing = x.is_null() | x.is_nan()
    is_int = (x == x.floor()) & (x.abs() < 1e11)
    fast = (x.round(2) == x) & (x.abs() < 1e9)       # shortest repr fits 12 chars
    slow = pl.when(~missing & ~is_int & ~fast).then(x).otherwise(None)
    return (pl.when(missing).then(pl.lit("."))
              .when(is_int).then(x.cast(pl.Int64).cast(pl.Utf8))
              .when(fast).then(x.cast(pl.Utf8))
              .otherwise(slow.map_elements(_best12, return_dtype=pl.Utf8)))


def _date_expr(col: str, dtype) -> pl.Expr:
    """DDMMYY10. for a SAS date (days since 1960-01-01) or a temporal column."""
    if dtype == pl.Date or isinstance(dtype, pl.Datetime):
        d = pl.col(col).cast(pl.Date)
    else:
        d = pl.lit(date(1960, 1, 1)) + pl.duration(days=pl.col(col).cast(pl.Int64))
    return pl.when(pl.col(col).is_null()).then(pl.lit(".")).otherwise(d.dt.strftime("%d/%m/%Y"))


def _field_expr(schema: dict, col: str) -> pl.Expr:
    """Default-format list output of one variable, by its type."""
    if col not in schema or schema[col] == pl.Null:
        return pl.lit(".")
    dtype = schema[col]
    if col == "TRANDT" or dtype == pl.Date or isinstance(dtype, pl.Datetime):
        return _date_expr(col, dtype)
    if dtype == pl.Utf8:
        # blank character value is written as one blank
        return pl.when(_c(col) == "").then(pl.lit(" ")).otherwise(_c(col))
    if col == "ACCTNO":
        # account numbers print in full (no E-notation), e.g. 16-digit card numbers
        x = pl.col(col).cast(pl.Float64)
        return (pl.when(x.is_null() | x.is_nan()).then(pl.lit("."))
                  .otherwise(x.round(0).cast(pl.Int64).cast(pl.Utf8)))
    return _num_expr(col)


def _brch_expr(schema: dict) -> pl.Expr:
    """BRCH = PUT(ACCBRCH,Z3.)  (missing -> '.')"""
    if "ACCBRCH" not in schema:
        return pl.lit(".")
    b = pl.col("ACCBRCH").cast(pl.Int64)
    return pl.when(b.is_null()).then(pl.lit(".")).otherwise(b.cast(pl.Utf8).str.zfill(3))


def _write_records(path: Path, head: list, body: Optional[pl.Series]) -> None:
    """Write RECFM=FB / LRECL=1000 text records (padded, no ASA byte)."""
    with open(path, "w", encoding="latin1", newline="") as fh:
        for line in head:
            fh.write(line.ljust(LRECL)[:LRECL] + "\n")
        if body is not None:
            for start in range(0, len(body), WRITE_BATCH):
                part = body.slice(start, WRITE_BATCH)
                fh.write("\n".join(part.to_list()) + "\n")
    print(f"  Written {path}")


def _write_listing(df: pl.DataFrame, path: Path, title: str, columns: list, labels: list) -> None:
    """DATA _NULL_ transaction listing; header only when there is >= 1 obs."""
    if df.height == 0:
        _write_records(path, [], None)
        return
    schema = df.schema
    parts = [_brch_expr(schema) if c == "BRCH" else _field_expr(schema, c) for c in columns]
    body = (df.select((pl.concat_str(parts, separator=" ;") + pl.lit(" ;"))
                      .str.pad_end(LRECL, " ").alias("L"))["L"])
    head = [title, " ", ";".join(labels) + ";"]
    _write_records(path, head, body)


# ============================================================================
# STEP 10: TRANSACTION LISTINGS
# ============================================================================
print("\nStep 10: Writing transaction listings...")
_LIST_LABELS = ["BRANCH", "PAYMENT DATE", "ACCOUNT NUMBER", "TYPE OF LOAN",
                "TYPE OF TRANSACTIONS", "CHEQUE NUMBER", "ACCOUNT DEBITED",
                "VALUE OF TRANSACTIONS (RM)", "TRANSACTION CODE"]
_LIST_COLS = ["BRCH", "TRANDT", "ACCTNO", "PDESC", "ITEM", "CHEQNO",
              "DEBACCT", "TRANAMT", "TRANCD"]

# *WHERE PROD NOT IN ('MS','OT');
_write_listing(bnm_tranx2, OUTPUT_PBB_TRX_FILE,
               f"PBB - TRANSACTION LISTING FOR REPORT ID: EIBMEPC2 AS AT {REPORTDT}",
               _LIST_COLS, _LIST_LABELS)
# *WHERE PROD NOT IN ('MS','OT');
_write_listing(ibnm_tranx2, OUTPUT_PIBB_TRX_FILE,
               f"PIBB - TRANSACTION LISTING FOR REPORT ID: EIBMEPC2 AS AT {REPORTDT}",
               _LIST_COLS, _LIST_LABELS)

# CDM listing: WHERE ITEM EQ 'CDM' on BNM.TRANX2 followed by IBNM.TRANX2
cdm_rows = _concat([bnm_tranx2.filter(_c("ITEM") == "CDM"),
                    ibnm_tranx2.filter(_c("ITEM") == "CDM")])
_write_listing(
    cdm_rows, OUTPUT_CDM_TRX_FILE,
    f"PBB & PIBB - TRANSACTION LISTING FOR CDM OF EIBMEPC2 AS AT {REPORTDT}",
    ["BRCH", "TRANDT", "ENTITY", "ACCTNO", "PDESC", "IND", "CHEQNO",
     "DEBACCT", "TRANAMT", "TRANCD"],
    ["BRANCH", "PAYMENT DATE", "ENTITY", "ACCOUNT NUMBER", "TYPE OF LOAN",
     "TYPE OF TRANSACTIONS", "CHEQUE NUMBER", "ACCOUNT DEBITED",
     "VALUE OF TRANSACTIONS (RM)", "TRANSACTION CODE"])
# DATA CDMTRANX2 (SET SUM ISUM; WHERE ITEM EQ 'CDM') is the same row set as
# cdm_rows and is never read again in the SAS program -- dead; not materialised.

# ============================================================================
# STEP 11: SUMMARY REPORTS (PBB / PIBB)
# ============================================================================
def _fmt_scalar(v) -> str:
    return "." if v is None else _best12(float(v))


def _seq_label(seq) -> str:
    """TRANX=PUT(SEQ,SEQFMT.); missing SEQ -> 'TOTAL'."""
    if seq is None:
        return "TOTAL"
    s = int(seq)
    return SEQ_LABELS.get(s, str(s))


def _summary_metrics(cats: list) -> list:
    cols = []
    for key, _ in cats:
        cols += [f"{key}_NUM", f"{key}_AMT"]
    return cols + ["CNUM", "TRANAMT1"]


def _summary_rows(tranx2: pl.DataFrame, cats: list) -> list:
    """SUM/ISUM data step + PROC SUMMARY (CLASS SEQ -> SUM1, none -> SUM2)."""
    flags = []
    for key, prods in cats:
        cond = pl.col("PROD").is_in(list(prods))
        flags.append(pl.when(cond).then(pl.col("TRANAMT1")).alias(f"{key}_AMT"))
        flags.append(pl.when(cond).then(pl.lit(1)).alias(f"{key}_NUM"))
    flags.append(pl.lit(1).alias("CNUM"))
    df = tranx2.with_columns(flags)
    metrics = _summary_metrics(cats)
    # *WHERE PROD NOT IN ('MS','OT');
    sum1 = (df.filter(pl.col("SEQ").is_not_null())          # CLASS drops missing SEQ
              .group_by("SEQ").agg([_ssum(c) for c in metrics]).sort("SEQ"))
    sum2 = df.select([_ssum(c) for c in metrics])
    rows = [(_seq_label(r["SEQ"]), [r[c] for c in metrics]) for r in sum1.iter_rows(named=True)]
    rows += [("TOTAL", [r[c] for c in metrics]) for r in sum2.iter_rows(named=True)]
    return rows


def _write_summary(path: Path, head: list, rows: list) -> None:
    lines = list(head)
    for label, values in rows:
        lines.append(";".join([label] + [_fmt_scalar(v) for v in values]))
    _write_records(path, lines, pl.Series([ln.ljust(LRECL)[:LRECL] for ln in lines[len(head):]],
                                          dtype=pl.Utf8) if len(lines) > len(head) else None)


def _write_summary_file(path: Path, head: list, rows: list) -> None:
    body = [" ;".join([label] + [_fmt_scalar(v) for v in values]) for label, values in rows]
    _write_records(path, head,
                   pl.Series([ln.ljust(LRECL)[:LRECL] for ln in body], dtype=pl.Utf8)
                   if body else None)


def _pairs(a: str, b: str, n: int) -> list:
    return [a, b] * n


print("\nStep 11: Writing PBB / PIBB summaries...")
_PBB_CATS = [("CC", ("CC",)), ("HL", ("HL",)), ("VL", ("VL",)),
             ("PL", ("PL",)), ("OT", ("OT", "MS"))]
_PIBB_CATS = [("HL", ("HL",)), ("VL", ("VL",)), ("PL", ("PL",)), ("OT", ("OT", "MS"))]

_pbb_head = [
    "PUBLIC BANK BERHAD",
    "REPORT ID: EIBMEPC2",
    f"LOAN REPAYMENT BY CUSTOMER AS AT {REPORTDT}",
    " ",
    ";".join([" ", "CREDIT AND CHARGE CARDS", " ", "HOUSING LOANS", " ", "VEHICLE LOANS",
              " ", "PERSONAL LOANS", " ", "OTHER TYPES OF LOANS", " ", "TOTAL", " "]),
    ";".join([" "] + _pairs("NUMBER OF", "VALUE OF", 6)),
    ";".join(_pairs("TRANSACTIONS", "TRANSACTION", 6) + ["TRANSACTIONS"]),
    ";".join([" "] + _pairs("(UNIT)", "(RM'000)", 6)),
]
_write_summary_file(OUTPUT_PBB_SUM_FILE, _pbb_head, _summary_rows(bnm_tranx2, _PBB_CATS))

_pibb_head = [
    "PUBLIC ISLAMIC BANK BERHAD",
    "REPORT ID: EIBMEPC2",
    f"LOAN REPAYMENT BY CUSTOMER AS AT {REPORTDT}",
    " ",
    ";".join([" ", "HOUSING LOANS", " ", "VEHICLE LOANS", " ", "PERSONAL LOANS",
              " ", "OTHER TYPES OF LOANS", " ", "TOTAL", " "]),
    ";".join([" "] + _pairs("NUMBER OF", "VALUE OF", 5)),
    ";".join(_pairs("TRANSACTIONS", "TRANSACTION", 5) + ["TRANSACTIONS"]),
    ";".join([" "] + _pairs("(UNIT)", "(RM'000)", 5)),
]
_write_summary_file(OUTPUT_PIBB_SUM_FILE, _pibb_head, _summary_rows(ibnm_tranx2, _PIBB_CATS))

# ============================================================================
# STEP 12: CDM SUMMARY  (CDMSUM: by product, HC/LC split by entity)
# ============================================================================
print("\nStep 12: Writing CDM summary...")
_ind = _c("IND")
_ent = _c("ENTITY")
_cdm_defs = [
    ("HC", _ind == "HC"), ("LC", _ind == "LC"),
    ("CHC", (_ind == "HC") & (_ent == "PBB")), ("CLC", (_ind == "LC") & (_ent == "PBB")),
    ("IHC", (_ind == "HC") & (_ent == "PIBB")), ("ILC", (_ind == "LC") & (_ent == "PIBB")),
]
_cdm_flags = []
for _k, _cond in _cdm_defs:
    _cdm_flags.append(pl.when(_cond).then(pl.col("TRANAMT1")).alias(f"{_k}_AMT"))
    _cdm_flags.append(pl.when(_cond).then(pl.lit(1)).alias(f"{_k}_NUM"))
cdm = cdm_rows.with_columns(_cdm_flags)

_cdm_metrics = ["HC_AMT", "LC_AMT", "HC_NUM", "LC_NUM", "CHC_AMT", "CLC_AMT", "CHC_NUM",
                "CLC_NUM", "IHC_AMT", "ILC_AMT", "IHC_NUM", "ILC_NUM"]
# CLASS PROD with FORMAT $PDESC.: groups are formed on the formatted value
# (MS and OT collapse into 'OTHER TYPES OF LOANS'), ordered by internal PROD.
# *WHERE PROD NOT IN ('MS','OT');
cdm1 = (cdm.filter(_c("PROD") != "")
           .group_by("PDESC")
           .agg([_ssum(c) for c in _cdm_metrics] + [pl.col("PROD").min().alias("_ord")])
           .sort("_ord"))
cdm2 = cdm.select([_ssum(c) for c in _cdm_metrics])

_cdm_out = ["CHC_NUM", "CHC_AMT", "CLC_NUM", "CLC_AMT", "IHC_NUM", "IHC_AMT",
            "ILC_NUM", "ILC_AMT", "HC_NUM", "HC_AMT", "LC_NUM", "LC_AMT"]
_cdm_rows = [(r["PDESC"] or "TOTAL", [r[c] for c in _cdm_out]) for r in cdm1.iter_rows(named=True)]
_cdm_rows += [("TOTAL", [r[c] for c in _cdm_out]) for r in cdm2.iter_rows(named=True)]

_cdm_head = [
    "PUBLIC BANK BERHAD & PUBLIC ISLAMIC BANK BERHAD",
    "REPORT ID: EIBMEPC2",
    f"LOAN REPAYMENT BY CUSTOMER BY CDM AS AT {REPORTDT}",
    " ",
    ";".join([" ", "PBB", " ", " ", " ", "PIBB", " ", " ", " ", "TOTAL", " ", " ", " "]),
    ";".join([" ", "HOUSE CHEQUE", " ", "LOCAL CHEQUE", " ", "HOUSE CHEQUE", " ",
              "LOCAL CHEQUE", " ", "HOUSE CHEQUE", " ", "LOCAL CHEQUE", " "]),
    ";".join([" "] + _pairs("NUMBER OF", "VALUE OF", 6)),
    ";".join(["TYPE OF LOAN", "TRANSACTION"] + _pairs("TRANSACTIONS", "TRANSACTION", 5)
             + ["TRANSACTIONS"]),
    ";".join([" "] + _pairs("(UNIT)", "(RM'000)", 6)),
]
_write_summary_file(OUTPUT_CDM_SUM_FILE, _cdm_head, _cdm_rows)

# ============================================================================
# FTP STEPS (JCL) - not part of the SAS data processing
# ============================================================================
# //*RUNSFTP  EXEC COZBATCH            (commented out in the JCL: FTP TO SAS DATAWAREHOUSE)
# //*CD TextFile/Remittance/BPP
# //*PUT //SAP.PBB.EIBMEPC2.TRXLIST.TEXT    MEPC2_PBB_TRX@%OMM.%OYY..TXT
# //*PUT //SAP.PIBB.EIBMEPC2.TRXLIST.TEXT   MEPC2_PIBB_TRX@%OMM.%OYY..TXT
# //*PUT //SAP.EIBMEPC2.CDMLIST.TEXT        MEPC2_CDM_TRX@%OMM.%OYY..TXT
# //*PUT //SAP.PBB.EIBMEPC2.SUM.TEXT        MEPC2_PBB_SUM@%OMM.%OYY..TXT
# //*PUT //SAP.PIBB.EIBMEPC2.SUM.TEXT       MEPC2_PIBB_SUM@%OMM.%OYY..TXT
# //*PUT //SAP.EIBMEPC2.CDMSUM.TEXT         MEPC2_CDM_SUM@%OMM.%OYY..TXT
#
# //RUNSFTP  EXEC COZBATCH               (live JCL step: FTP TO DATA REPORT REPOSITORY, DRR)
# CD "BOD-BPP/BNM - LOANS REPAYMENT CDM REPORT"
#   PUT SAP.EIBMEPC2.CDMSUM.TEXT       -> MEPC2_CDM_SUM@<MM><YY>.TXT
# CD "/BOD-BPP/BNM - LOANS REPAYMENT CDM TRANSACTION LISTING"
#   PUT SAP.EIBMEPC2.CDMLIST.TEXT      -> MEPC2_CDM_TRX@<MM><YY>.TXT
# CD "/BOD-BPP/BNM - PBB LOAN REPAYMENT REPORT"
#   PUT SAP.PBB.EIBMEPC2.SUM.TEXT      -> MEPC2_PBB_SUM@<MM><YY>.TXT
# CD "/BOD-BPP/BNM - PBB LOAN REPAYMENT TRANSACTION LISTING"
#   PUT SAP.PBB.EIBMEPC2.TRXLIST.TEXT  -> MEPC2_PBB_TRX@<MM><YY>.TXT
# CD "/BOD-BPP/BNM - PIBB LOAN REPAYMENT REPORT"
#   PUT SAP.PIBB.EIBMEPC2.SUM.TEXT     -> MEPC2_PIBB_SUM@<MM><YY>.TXT
# CD "/BOD-BPP/BNM - PIBB LOAN REPAYMENT TRANSACTION LISTING"
#   PUT SAP.PIBB.EIBMEPC2.TRXLIST.TEXT -> MEPC2_PIBB_TRX@<MM><YY>.TXT

con.close()
print("\nEIBMEPC2 complete.")
