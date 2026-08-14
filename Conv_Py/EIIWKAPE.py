#!/usr/bin/env python3
"""
Program : EIIWKAPE.py
Purpose : Weekly (Daily) KAPITI Stock Report - Specified & Non-Specified
          RENTAS Securities from Trading Book, for Public Islamic Bank
          Berhad (PIBB). Produces the variance report between KAPITI and
          WALKER, and the Rev Repo report, then dispatches PIBELQ's
          per-day detail Eligible Liabilities report (DAY A - DAY I).

Dependency:
    %INC PGM(PIBELQ);  -> converted separately as PIBELQ.py
    from PIBELQ import build_elw1, prtel, prteli

Dependency note (PBBELF):
    PIBELQ.py (not this program) uses PBBELF.EL_DEFINITIONS /
    ELI_DEFINITIONS as the EL/ELI item catalogue. This program does not
    reference PBBELF directly, so it is not imported here.

============================================================================
PHYSICAL INPUT DATASETS USED BY THIS PROGRAM  (all .sas7bdat, cached to
Parquet on first read per EIBDLN1M.py's chunked-conversion pattern)
============================================================================
1. BNMK REP4X  (SAS libref BNMK -> SAP.PIBB.DKAPITI.SASDATA)
   File     : rep4x<REPTYEAR><REPTMON><WK>.sas7bdat
   Path     : INPUT_BNMK_REP4X_DIR
   Used in  : Step 5 - build REP4 (BNMCODE remap 3723...->3523...,
              filtered UTREF IN ('DLG','IDLG'), UTSTY NOT IN ('BMN','CB1'))

2. BNMK REP2   (SAS libref BNMK -> SAP.PIBB.DKAPITI.SASDATA)
   File     : rep2<REPTMON><WK>.sas7bdat
   Path     : INPUT_BNMK_REP2_DIR
   Used in  : Step 6 - build REP2 (union with REP4, BNMCODG derivation) ->
              feeds the RENTAS securities report (Step 7)
              Step 9 - raw REP0 (Rev Repo at purchase proceeds report),
              read independently from the *unmodified* dataset (SAS SET
              BNMK.REP2&REPTMON&WK directly, not the reworked work.REP2)
              NOTE: this is the SAME physical dataset that PIBELQ.py reads
              as rep2<REPTMON><NOWK> since NOWK == WK in this program.

3. BNMS ELSCD  (SAS libref BNMS -> SAP.PIBB.RDAL1)
   File     : elscd<REPTMON><WK>.sas7bdat
   Path     : INPUT_BNMS_ELSCD_DIR
   Used in  : Step 8 - WALW variance source (union with BNM ELW)

4. BNM ELW     (SAS libref BNM  -> SAP.PIBB.D&TOYYYY)
   File     : elw<REPTMON><WK>.sas7bdat
   Path     : INPUT_BNM_ELW_DIR
   Used in  : Step 8 - WALW variance source (union with BNMS ELSCD)

------------------------------------------------------------------------
NON-FILE / DERIVED INPUTS
------------------------------------------------------------------------
- LOAN.REPTDATE and BNMK.REPTDATE: no reptdate.parquet/.sas7bdat exists.
  Both are derived from REPTDATE.py's get_reptdate_values() (see Steps 1-2).
- ELG.GOLD&REPTMON&NOWK: a single-row work dataset built inline by this
  program (Step 4) — not a physical file — and passed to PIBELQ.prtel /
  prteli as gold_df.
"""

import gc
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from PIBELQ import build_elw1, prtel, prteli

# NOTE: %INC PGM stated in JCL, but PBB.PROGRAM library holds SAS source
# code (compiled macros), not data - it has no python equivalent to import.

# ============================================================================
# PATH CONFIGURATION (each physical input kept independent)
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_BNMK_REP4X_DIR = BASE_DIR / "input" / "prod" / "EIIWKAPE" / "bnmk_rep4x"
INPUT_BNMK_REP2_DIR  = BASE_DIR / "input" / "prod" / "EIIWKAPE" / "bnmk_rep2"
INPUT_BNMS_ELSCD_DIR = BASE_DIR / "input" / "prod" / "EIIWKAPE" / "bnms_elscd"
INPUT_BNM_ELW_DIR    = BASE_DIR / "input" / "prod" / "EIIWKAPE" / "bnm_elw"

OUTPUT_DIR      = BASE_DIR / "output" / "EIIWKAPE"
OUTPUT_NSRS_DIR = BASE_DIR / "output" / "EIIWKAPE" / "nsrs"

# Parquet cache directory — shared with PIBELQ.py (same BNMK REP2 dataset
# is read by both programs for the same REPTMON/NOWK, so caching once
# here avoids a duplicate conversion when PIBELQ.py runs in-process)
CACHE_DIR = BASE_DIR / "cache" / "EIIWKAPE"

for _d in (OUTPUT_DIR, OUTPUT_NSRS_DIR, CACHE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000

# ============================================================================
# SFTP CONFIGURATION
# ============================================================================
# RUNSFTP step uploads the report to "FD-BNM REPORTING/PIBB/BNM RPTG" on the
# Data Report Repository (DRR) host. Following project convention, paramiko
# is used with credentials resolved via EDW_TRANSFORMATION.get_sftp_info().
# HOST_DESC key for the DRR host is not confirmed against
# ctl_dwh_sftp_info.sas7bdat yet, so the actual transfer call is left as a
# documented placeholder below (see Step 13).
# from EDW_TRANSFORMATION import get_sftp_info
SFTP_REMOTE_DIR = "FD-BNM REPORTING/PIBB/BNM RPTG"

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
# STEP 1: REPORT DATE  (LOAN.REPTDATE equivalent — no physical file, see
# module docstring "NON-FILE / DERIVED INPUTS")
# DATA REPTDAT1; SET LOAN.REPTDATE; SDESC='PUBLIC ISLAMIC BANK BERHAD'; ...
# ============================================================================
print("Step 1: Deriving report date (REPTDAT1)...")

_reptdat1 = get_reptdate_values(year_format="%Y")
SDESC   = "PUBLIC ISLAMIC BANK BERHAD"
RDATE   = _reptdat1.reptdate.strftime("%d/%m/%y")   # DDMMYY8.
RYEAR   = _reptdat1.reptdate.strftime("%Y")          # YEAR4.
MTHNAM  = _reptdat1.reptdate.strftime("%B").upper()  # MONNAME.

print(f"  SDESC  : {SDESC}")
print(f"  RDATE  : {RDATE}")
print(f"  RYEAR  : {RYEAR}")
print(f"  MTHNAM : {MTHNAM}")

# ============================================================================
# STEP 2: WEEK / MONTH DERIVATION  (BNMK.REPTDATE equivalent — no
# physical file, see module docstring)
# DATA REPTDATE; SET BNMK.REPTDATE; MM=MONTH(SXDATE);
#   custom week buckets (1-8='4', 9-15='1', 16-22='2', else='3');
#   IF WK='4' THEN roll back one month (and one year if January).
# ============================================================================
print("\nStep 2: Deriving week/month bucket (REPTDATE)...")

SXDATE = _reptdat1.reptdate
_day = SXDATE.day
if 1 <= _day <= 8:
    WK = "4"
elif 9 <= _day <= 15:
    WK = "1"
elif 16 <= _day <= 22:
    WK = "2"
else:
    WK = "3"

MM = SXDATE.month
if WK == "4":
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
    MM = MM1
    if MM == 12:
        # SXDATE = MDY(1,1,YEAR(SXDATE)) - 1  ->  31-Dec of prior year
        SXDATE = date(SXDATE.year, 1, 1) - timedelta(days=1)

NOWK     = WK                       # CALL SYMPUT('NOWK', WK) -- identical to WK
REPTMON  = f"{MM:02d}"              # Z2.
RPDATE   = SXDATE.strftime("%d/%m/%y")
REPTYEAR = SXDATE.strftime("%Y")    # YEAR4.

print(f"  WK / NOWK : {WK}")
print(f"  REPTMON   : {REPTMON}")
print(f"  REPTYEAR  : {REPTYEAR}")
print(f"  RPDATE    : {RPDATE}")

# ============================================================================
# STEP 3: OUTPUT FILE NAMES
# SASLIST DSN=SAP.PIBB.EIIWKAPD (catalogued, no date suffix in local name)
# ============================================================================
OUTPUT_FILE      = OUTPUT_DIR / "EIIWKAPD.txt"
OUTPUT_NSRS_FILE = OUTPUT_NSRS_DIR / "EIIWKAPD.txt"

# PUT //SAP.PIBB.EIIWKAPD  EIIWKAPD_MTH.TXT   (if NOWK='4')
# PUT //SAP.PIBB.EIIWKAPD  EIIWKAPD_WK&NOWK..TXT  (otherwise)
SFTP_REMOTE_NAME = "EIIWKAPD_MTH.TXT" if NOWK == "4" else f"EIIWKAPD_WK{NOWK}.TXT"

print(f"  Output file        : {OUTPUT_FILE.name}")
print(f"  NSRS copy          : {OUTPUT_NSRS_FILE.name}")
print(f"  SFTP remote name   : {SFTP_REMOTE_NAME}")

# ============================================================================
# ASA REPORT HELPERS
# ============================================================================
def _new_buf(width: int = 132) -> list:
    return [" "] * width


def _put(buf: list, col: int, text: str) -> None:
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
    lines = []
    for i, t in enumerate(titles):
        buf = _new_buf()
        _put(buf, 1, t)
        lines.append(_line(buf, "1" if i == 0 else " "))
    return lines


def _render_pivot_report(
    df: pl.DataFrame,
    title_lines: list[str],
    row_col: str,
    all_label: str,
    class_col: str,
    value_specs: list[tuple],
    rts: int,
) -> list[str]:
    """
    Generic emulation of PROC TABULATE: rows = distinct row_col values plus
    an ALL/grand-total row, columns = distinct class_col values, each
    showing one or more summed value columns (COMMA-formatted).
    """
    lines = list(title_lines)

    class_vals = sorted(df[class_col].drop_nulls().unique().to_list())
    hdr = _new_buf()
    pos = rts + 1
    col_starts = {}
    for cv in class_vals:
        for (col, label, width, dec) in value_specs:
            col_starts[(cv, col)] = pos
            seg = cv if len(value_specs) == 1 else f"{cv[:width - len(label) - 1]} {label}"
            _put(hdr, pos, seg[:width].rjust(width))
            pos += width
    lines.append(_line(hdr))
    lines.append(" " + "-" * (pos - 2))

    grand = {}
    row_vals = sorted(df[row_col].drop_nulls().unique().to_list())
    for rv in row_vals:
        buf = _new_buf()
        _put(buf, 1, str(rv)[:rts])
        sub = df.filter(pl.col(row_col) == rv)
        for cv in class_vals:
            cell = sub.filter(pl.col(class_col) == cv)
            for (col, label, width, dec) in value_specs:
                val = float(cell[col].sum()) if len(cell) else 0.0
                grand[(cv, col)] = grand.get((cv, col), 0.0) + val
                _put(buf, col_starts[(cv, col)], _fmt_comma(val, width, dec))
        lines.append(_line(buf))

    buf = _new_buf()
    _put(buf, 1, all_label[:rts])
    for cv in class_vals:
        for (col, label, width, dec) in value_specs:
            _put(buf, col_starts[(cv, col)], _fmt_comma(grand.get((cv, col), 0.0), width, dec))
    lines.append(_line(buf))
    return lines


# ============================================================================
# STEP 4: ELG.GOLD&REPTMON&NOWK  (work dataset, single seed row — not a
# physical file, see module docstring)
# DATA ELG.GOLD&REPTMON&NOWK; ELDAY='DAYI'; BNMCODE='4929995000000Y';
#      AMOUNT=0.00;
# ============================================================================
print("\nStep 4: Building GOLD seed dataset...")

gold_df = pl.DataFrame({
    "ELDAY": ["DAYI"],
    "BNMCODE": ["4929995000000Y"],
    "AMOUNT": [0.00],
})

# ============================================================================
# STEP 5: REP4  (INPUT: BNMK REP4X — see module docstring item 1)
# ============================================================================
print("\nStep 5: Building REP4...")

rep4_sas = INPUT_BNMK_REP4X_DIR / f"rep4x{REPTYEAR}{REPTMON}{WK}.sas7bdat"
rep4_cache = _load_cached(rep4_sas, "BNMK_REP4X")

con = duckdb.connect(database=":memory:")
rep4 = con.execute(f"""
    SELECT
        CASE WHEN BNMCODE = '3723000000000Y' THEN '3523000000000Y' ELSE BNMCODE END AS BNMCODE,
        CAST(UTSTY   AS VARCHAR) AS UTSTY,
        CAST(UTREF   AS VARCHAR) AS UTREF,
        CAST(ELDAY   AS VARCHAR) AS ELDAY,
        CAST(AMOUNT  AS DOUBLE)  AS AMOUNT,
        CAST(NETAMT  AS DOUBLE)  AS NETAMT,
        CAST(COSTDED AS DOUBLE)  AS COSTDED
    FROM read_parquet('{rep4_cache.as_posix()}')
    WHERE UTREF IN ('DLG','IDLG')
      AND UTSTY NOT IN ('BMN','CB1')
""").pl()
con.close()
print(f"  REP4 rows: {len(rep4):,}")

# ============================================================================
# STEP 6: REP2  (INPUT: BNMK REP2 — see module docstring item 2)
# DATA REP2; SET BNMK.REP2&REPTMON&WK REP4; remap codes; build BNMCODG;
# PROC SORT BY BNMCODG omitted -- the pivot renderer groups by row_col
# directly, so a pre-sort is unnecessary.
# ============================================================================
print("\nStep 6: Building REP2 (union with REP4, remap, BNMCODG)...")

rep2_sas = INPUT_BNMK_REP2_DIR / f"rep2{REPTMON}{WK}.sas7bdat"
rep2_cache = _load_cached(rep2_sas, "BNMK_REP2")

con = duckdb.connect(database=":memory:")
rep2_base = con.execute(f"""
    SELECT
        CAST(BNMCODE AS VARCHAR) AS BNMCODE,
        CAST(UTSTY   AS VARCHAR) AS UTSTY,
        CAST(UTREF   AS VARCHAR) AS UTREF,
        CAST(ELDAY   AS VARCHAR) AS ELDAY,
        CAST(AMOUNT  AS DOUBLE)  AS AMOUNT,
        CAST(NETAMT  AS DOUBLE)  AS NETAMT,
        CAST(COSTDED AS DOUBLE)  AS COSTDED
    FROM read_parquet('{rep2_cache.as_posix()}')
""").pl()
con.close()

rep2 = pl.concat([rep2_base, rep4.select(rep2_base.columns)], how="vertical")

_is_repo = pl.col("BNMCODE") == "3250000000000Y"
rep2 = rep2.with_columns([
    pl.when(_is_repo).then(pl.lit("REV")).otherwise(pl.col("UTSTY")).alias("UTSTY"),
    pl.when(_is_repo).then(pl.lit("REPO ")).otherwise(pl.col("UTREF")).alias("UTREF"),
    pl.when(_is_repo).then(pl.col("NETAMT")).otherwise(pl.col("AMOUNT")).alias("AMOUNT"),
])
rep2 = rep2.with_columns(
    pl.when(pl.col("BNMCODE") == "3752000000000Y")
    .then(pl.lit("3552000000000Y"))
    .otherwise(pl.col("BNMCODE"))
    .alias("BNMCODE")
)
rep2 = rep2.with_columns(
    (pl.col("BNMCODE") + "-" + pl.col("UTSTY") + " " + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
)
print(f"  REP2 rows: {len(rep2):,}")

# ============================================================================
# STEP 7: RENTAS SECURITIES REPORT  (PROC TABULATE #1)
# ============================================================================
print("\nStep 7: Rendering RENTAS securities report...")

title1 = _title_lines(
    f"PUBLIC BANK BERHAD -REPORT DATE {RDATE}",
    "SPECIFIED & NON-SPECIFIED RENTAS SECURITIES FROM TRADING BOOK",
    f"(DAILY KAPITI STOCK REPORT) WEEK {WK} {MTHNAM} {RYEAR}",
)
report1_lines = _render_pivot_report(
    rep2, title1,
    row_col="BNMCODG", all_label="TOTAL RM MARKETABLE SECURITIES",
    class_col="ELDAY", value_specs=[("AMOUNT", "", 16, 2)], rts=30,
)

# ============================================================================
# STEP 8: VARIANCE REPORT  (INPUT: BNMS ELSCD + BNM ELW — see module
# docstring items 3 & 4; PROC SUMMARY REPOV / WALW, MERGE, TABULATE #2)
# ============================================================================
print("\nStep 8: Building variance report (KAPITI vs WALKER)...")

repov = rep2.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("AMOUNT"))

elscd_sas = INPUT_BNMS_ELSCD_DIR / f"elscd{REPTMON}{WK}.sas7bdat"
elscd_cache = _load_cached(elscd_sas, "BNMS_ELSCD")

elw_sas = INPUT_BNM_ELW_DIR / f"elw{REPTMON}{WK}.sas7bdat"
elw_cache = _load_cached(elw_sas, "BNM_ELW")

con = duckdb.connect(database=":memory:")
walw_base = con.execute(f"""
    SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(ELDAY AS VARCHAR) ELDAY,
           CAST(AMOUNT AS DOUBLE) AMOUNT
    FROM read_parquet('{elscd_cache.as_posix()}')
    UNION ALL
    SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(ELDAY AS VARCHAR) ELDAY,
           CAST(AMOUNT AS DOUBLE) AMOUNT
    FROM read_parquet('{elw_cache.as_posix()}')
""").pl()
con.close()

walw = walw_base.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("WALWAMT"))

# MERGE REPOV(IN=A) WALW(IN=B); BY BNMCODE ELDAY; IF A;  -> left join, keep REPOV
variance_df = repov.join(walw, on=["BNMCODE", "ELDAY"], how="left").with_columns(
    pl.col("WALWAMT").fill_null(0.0)
).with_columns(
    (pl.col("AMOUNT") - pl.col("WALWAMT")).alias("VARIANC")
)

title2 = _title_lines("VARIANCE BETWEEN KAPITI AND WALKER")
report2_lines = _render_pivot_report(
    variance_df, title2,
    row_col="BNMCODE", all_label="TOTAL ",
    class_col="ELDAY",
    value_specs=[
        ("AMOUNT", "KAPITI", 16, 2),
        ("WALWAMT", "WALKER", 16, 2),
        ("VARIANC", "VARIANCE", 16, 2),
    ],
    rts=34,
)

# ============================================================================
# STEP 9: REV REPO AT PURCHASE PROCEEDS REPORT  (REP0, TABULATE #3)
# DATA REP0; SET BNMK.REP2&REPTMON&WK;  -- raw physical REP2 (same input
# as Step 6 item 2), read directly rather than from the reworked work.REP2,
# since SAS re-reads the physical dataset here instead of reusing work.REP2.
# ============================================================================
print("\nStep 9: Building Rev Repo report...")

rep0 = rep2_base.filter(pl.col("BNMCODE") == "3250000000000Y").with_columns(
    (pl.col("BNMCODE") + "-" + pl.col("UTSTY") + " " + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
)

title3 = _title_lines("REV REPO AT PURCHASE PROCEEDS")
report3_lines = _render_pivot_report(
    rep0, title3,
    row_col="BNMCODG", all_label="TOTAL ",
    class_col="ELDAY",
    value_specs=[
        ("AMOUNT", "AMOUNT", 16, 2),
        ("COSTDED", "(-) PURC PROC.", 16, 2),
        ("NETAMT", "MARKET SEC ", 16, 2),
    ],
    rts=30,
)

del rep2_base, repov, walw_base, walw
gc.collect()

# ============================================================================
# STEP 10: PIBELQ DAILY EL DETAIL REPORTS  (%PRTEL DAYA..DAYH, %PRTELI DAYI)
# Consumes: BNMK REP2 / BNMK TBL1 / BNM ELW / BNMB ELW — all documented
# in PIBELQ.py's own module docstring, kept as separate physical inputs.
# ============================================================================
print("\nStep 10: Rendering PIBELQ daily EL detail reports...")

elw1 = build_elw1(REPTMON, NOWK, SDESC)

pibelq_lines: list[str] = []
for day_code in ("DAYA", "DAYB", "DAYC", "DAYD", "DAYE", "DAYF", "DAYG", "DAYH"):
    pibelq_lines.extend(
        prtel(
            day_code,
            reptmon=REPTMON, nowk=NOWK, sdesc=SDESC, rdate=RDATE,
            gold_df=gold_df, rep4_df=rep4, elw1=elw1,
        )
    )

pibelq_lines.extend(
    prteli(
        "DAYI",
        reptmon=REPTMON, nowk=NOWK, rdate=RDATE,
        gold_df=gold_df, rep4_df=rep4, elw1=elw1,
    )
)

del elw1, gold_df, rep4, rep2, rep0, variance_df
gc.collect()

# ============================================================================
# STEP 11: WRITE OUTPUT (SASLIST, RECFM=FB LRECL=133, ASA carriage control)
# ============================================================================
print("\nStep 11: Writing SASLIST output...")

all_lines = report1_lines + report2_lines + report3_lines + pibelq_lines

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in all_lines:
        fh.write(ln[:133].ljust(133) + "\n")

print(f"  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(all_lines):,}")

# ============================================================================
# STEP 12: COPY OUTPUT FOR NSRS  (PROC IEBGENER copy of the SASLIST output)
# ============================================================================
print("\nStep 12: Copying output for NSRS...")

with open(OUTPUT_FILE, "rb") as src, open(OUTPUT_NSRS_FILE, "wb") as dst:
    dst.write(src.read())

print(f"  NSRS copy written : {OUTPUT_NSRS_FILE}")

# ============================================================================
# STEP 13: SFTP THE REPORT TO THE DATA REPORT REPOSITORY (DRR)
# RUNSFTP step -- lzopts servercp=..., cd "FD-BNM REPORTING/PIBB/BNM RPTG"
# ============================================================================
print("\nStep 13: Transferring output via SFTP...")

# HOST_DESC lookup key against ctl_dwh_sftp_info.sas7bdat is not yet
# confirmed for this DRR destination, so the transfer is documented here
# rather than executed silently against an unverified host entry.
#
# from EDW_TRANSFORMATION import get_sftp_info
# import paramiko
#
# sftp_info = get_sftp_info("DRR")  # HOST_DESC placeholder -- confirm key
# transport = paramiko.Transport((sftp_info.host, sftp_info.port))
# transport.connect(username=sftp_info.username, password=sftp_info.password)
# sftp = paramiko.SFTPClient.from_transport(transport)
# sftp.chdir(SFTP_REMOTE_DIR)
# sftp.put(str(OUTPUT_FILE), SFTP_REMOTE_NAME)
# sftp.close()
# transport.close()

print(f"  Remote dir  : {SFTP_REMOTE_DIR}")
print(f"  Remote name : {SFTP_REMOTE_NAME}")
print("  (SFTP transfer call left commented -- HOST_DESC key unconfirmed)")

print("\nEIIWKAPE complete.")
