#!/usr/bin/env python3
"""
Program : EIBMRB01.py
Purpose : Daily total outstanding balance / number of accounts on FCY FD (Report ID EIBMRB01).

Dependency:
    No %INC in the SAS source, so no format module is imported.

Physical inputs (each cached to Parquet independently):
    //MISFD  DD SAP.PBB.FCYFD  -> MISFD.FCYFD&REPTMON            (INPUT_MISFD_FILE)
    //MIS2FD DD SAP.PBB.FCFD   -> MIS2FD.FCYFD&REPTMON&NOWK&YY   (INPUT_MIS2FD_FILE)
    Both file names are fully derived from report-date tokens, so they are built
    directly and input_date.get_latest_file() is not used.
    DEPO.REPTDATE is not read; the report date comes from REPTDATE.py.

Output:
    //SASLIST DD SAP.PBB.EIBMRB01 (LRECL=133, RECFM=FB). Fixed name, no date token,
    so output_date.py is not used. RECFM=FB means no ASA byte; a form feed marks the page start.
"""

import calendar
import gc
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR = Path("/stgsrcsys/host/uat/AII")

INPUT_MISFD_DIR  = STG_DIR / "EIBMRBDP"     # //MISFD  DD SAP.PBB.FCYFD
INPUT_MIS2FD_DIR = STG_DIR / "EIBMRBDP"     # //MIS2FD DD SAP.PBB.FCFD

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMRBDP"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIBMRBDP"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "EIBMRB01.txt"               # //SASLIST DD SAP.PBB.EIBMRB01

CHUNK_ROWS   = 500_000
FF           = "\f"
MISSING_CHAR = "0"                                      # OPTIONS MISSING=0
CELL         = 16                                       # FORMAT=COMMA16. column width
DATE_W       = 14                                       # DATE (row title) column width
LINESIZE     = 133                                      # LRECL=133, every line padded to this width
PAGESIZE     = 60                                       # lines per page
PANEL_COLS   = (LINESIZE - DATE_W - 2) // (CELL + 1)    # 6 data columns fit per panel
ROWS_CONT    = (PAGESIZE - 12 + 1) // 2                 # 24 date rows on a page that ends with "(Continued)"
ROWS_LAST    = (PAGESIZE - 10 + 1) // 2                 # 25 date rows on the final page
LABEL        = "OUTSTANDING AMOUNT (RM 000)"
CONTINUED    = "(Continued)"


# ============================================================================
# STEP 1: REPORT DATE  (DATA REPTDATE)
# ============================================================================
def derive_report_context() -> dict:
    """Macro-variable equivalents of the REPTDATE step (NOWK: exact day 8/15/22, else 4)."""

    # reptdate = get_reptdate_values(year_format="%Y").reptdate

    # DEBUG - UAT override
    reptdate = date(2026, 9, 30)

    return {
        "reptdate": reptdate,
        "nowk": {8: "1", 15: "2", 22: "3"}.get(reptdate.day, "4"),
        "reptyear": reptdate.strftime("%Y"),
        "reptyear2": reptdate.strftime("%y"),
        "reptmon": reptdate.strftime("%m"),
        "rdate": reptdate.strftime("%d/%m/%y"),
        "reptmth": calendar.month_name[reptdate.month].upper(),
    }


print("Step 1: Deriving report date...")
CTX = derive_report_context()
REPTYEAR_I = int(CTX["reptyear"])
REPTMON_I = int(CTX["reptmon"])
print(f"  RDATE: {CTX['rdate']}  NOWK: {CTX['nowk']}")

# INPUT_MISFD_FILE = INPUT_MISFD_DIR / f"fcyfd{CTX['reptmon']}.sas7bdat"
# INPUT_MIS2FD_FILE = INPUT_MIS2FD_DIR / f"fcyfd{CTX['reptmon']}{CTX['nowk']}{CTX['reptyear2']}.sas7bdat"

INPUT_MISFD_FILE  = INPUT_MISFD_DIR  / f"fcyfd09.sas7bdat"
INPUT_MIS2FD_FILE = INPUT_MIS2FD_DIR / f"fcyfd09426.sas7bdat"


# ============================================================================
# HELPERS: CACHE (.sas7bdat -> Parquet), DATES, FORMATTING
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return cache_path.exists() and cache_path.stat().st_mtime >= sas_path.stat().st_mtime


def _sas_type_to_arrow(dtype) -> pa.DataType:
    if dtype == "object":
        return pa.string()
    if pd.api.types.is_integer_dtype(dtype):
        return pa.int64()
    if pd.api.types.is_float_dtype(dtype):
        return pa.float64()
    return pa.from_numpy_dtype(dtype)


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0
    for chunk in pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS):
        if schema is None:
            schema = pa.schema([pa.field(c, _sas_type_to_arrow(t)) for c, t in chunk.dtypes.items()])
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()
    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    # Cache name = immediate parent folder + file stem, e.g. CISBEXT_DP_deposit.parquet,
    # so datasets with the same file name in different subfolders never share a cache.
    cache_path = CACHE_DIR / f"{sas_path.parent.name}_{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


def _read_pq(cache_path: Path, columns: str, where: str) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(
            f"SELECT {columns} FROM read_parquet('{cache_path.as_posix()}') WHERE {where}"
        ).pl()
    finally:
        con.close()


def _sas_date(df: pl.DataFrame, col: str) -> pl.Expr:
    """SAS date value (days since 1960-01-01, or already a date/datetime) -> Date."""
    dtype = df.schema[col]
    if dtype == pl.Date:
        return pl.col(col)
    if isinstance(dtype, pl.Datetime):
        return pl.col(col).dt.date()
    days = pl.duration(days=pl.col(col).cast(pl.Float64).cast(pl.Int64))
    return (pl.lit(date(1960, 1, 1)) + days).cast(pl.Date)


def _in_report_month(col: str) -> pl.Expr:
    return (pl.col(col).dt.year() == REPTYEAR_I) & (pl.col(col).dt.month() == REPTMON_I)


def _comma(value, width: int, decimals: int = 0) -> str:
    """COMMAw.d (round half away from zero)."""
    if value is None:
        return MISSING_CHAR.rjust(width)
    quant = Decimal(1).scaleb(-decimals)
    text = f"{Decimal(repr(float(value))).quantize(quant, rounding=ROUND_HALF_UP):,.{decimals}f}"
    return text.rjust(width)


def _center(text: str, width: int) -> str:
    text = text[:width]
    pad = width - len(text)
    return " " * (pad // 2) + text + " " * (pad - pad // 2)


# ============================================================================
# STEP 2: CACHE INPUTS
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
MISFD_CACHE  = _load_cached(INPUT_MISFD_FILE, "MISFD")
MIS2FD_CACHE = _load_cached(INPUT_MIS2FD_FILE, "MIS2FD")

# ============================================================================
# STEP 3: DATA FCY / DATA FCYFD / MERGE / FILTER
# ============================================================================
print("\nStep 3: Building FCYFD...")

# DATA FCY: KEEP ACCTNO CUSTCODE OPENDT. CUSTCODE is never used afterwards, so only
# ACCTNO and OPENDT are loaded. SAS MERGE (last dataset wins) -> keep last row per ACCTNO.
fcy = _read_pq(MIS2FD_CACHE, "CAST(ACCTNO AS BIGINT) AS ACCTNO, OPENDT AS OPENDT_FCY", "TRUE")
fcy = (
    fcy.with_columns(_sas_date(fcy, "OPENDT_FCY").alias("OPENDT_FCY"), pl.lit(True).alias("IN_FCY"))
    .unique(subset="ACCTNO", keep="last", maintain_order=True)
)

# DATA FCYFD: SET MISFD.FCYFD&REPTMON; WHERE CURCODE NE 'MYR' AND CURBAL GT 0
names = pq.read_schema(MISFD_CACHE).names
select_cols = ["CAST(ACCTNO AS BIGINT) AS ACCTNO", "CAST(CURBAL AS DOUBLE) AS CURBAL", "TRIM(CURCODE) AS CURCODE"]
select_cols += [c for c in ("OPENDT", "CLOSEDAT", "REPTDATE") if c in names]
fcyfd = _read_pq(
    MISFD_CACHE,
    ", ".join(select_cols),
    "COALESCE(TRIM(CURCODE), '') <> 'MYR' AND CURBAL > 0",
)

if "OPENDT" in fcyfd.columns:
    fcyfd = fcyfd.with_columns(_sas_date(fcyfd, "OPENDT").alias("OPENDT"))
else:
    fcyfd = fcyfd.with_columns(pl.lit(None, dtype=pl.Date).alias("OPENDT"))

# MERGE FCYFD(IN=A) FCY; BY ACCTNO; IF A;  -> matched rows take FCY's OPENDT
fcyfd = fcyfd.join(fcy, on="ACCTNO", how="left").with_columns(
    pl.when(pl.col("IN_FCY").fill_null(False))
    .then(pl.col("OPENDT_FCY"))
    .otherwise(pl.col("OPENDT"))
    .alias("OPENDT"),
    (pl.col("CURBAL") / 1000).alias("CURBAL"),
)

# IF opened in report month AND (YEAR(CLOSEDAT)=YEAR(OPENDT)) & (MONTH(CLOSEDAT)=MONTH(OPENDT)) THEN DELETE;
# The SAS source tests CLOSEDAT (not CLOSEDT). It is preserved as written: when the column is
# absent from the dataset SAS treats it as missing, which never equals YEAR(OPENDT), so nothing is deleted.
if "CLOSEDAT" in fcyfd.columns:
    closed = _sas_date(fcyfd, "CLOSEDAT")
    same_month = (closed.dt.year() == pl.col("OPENDT").dt.year()) & (
        closed.dt.month() == pl.col("OPENDT").dt.month()
    )
    fcyfd = fcyfd.filter(~((_in_report_month("OPENDT") & same_month).fill_null(False)))

if "REPTDATE" in fcyfd.columns:
    fcyfd = fcyfd.with_columns(_sas_date(fcyfd, "REPTDATE").alias("REPTDATE"))
else:
    fcyfd = fcyfd.with_columns(pl.lit(CTX["reptdate"]).cast(pl.Date).alias("REPTDATE"))

print(f"  FCYFD rows: {len(fcyfd):,}")

# ============================================================================
# STEP 4: PROC TABULATE  (REPTDATE x CURCODE, total, number of accounts)
# ============================================================================
print("\nStep 4: Rendering report...")

# CLASS variables with missing values are dropped by PROC TABULATE.
summary = (
    fcyfd.filter(pl.col("CURCODE").is_not_null() & (pl.col("CURCODE") != "") & pl.col("REPTDATE").is_not_null())
    .group_by(["REPTDATE", "CURCODE"])
    .agg(pl.col("CURBAL").sum(), pl.len().alias("N"))
)


def _pad(text: str) -> str:
    return text.ljust(LINESIZE)


def _rule(ncols: int) -> str:
    return "|" + "-" * DATE_W + "+" + "+".join(["-" * CELL] * ncols) + "|"


def _render_report(summ: pl.DataFrame) -> list:
    """PROC TABULATE layout: panels of PANEL_COLS columns, paged by PAGESIZE, 133-wide lines."""
    currencies = sorted(summ["CURCODE"].unique().to_list())
    dates = sorted(summ["REPTDATE"].unique().to_list())
    ncur = len(currencies)

    bal = {(r, c): v for r, c, v in summ.select("REPTDATE", "CURCODE", "CURBAL").iter_rows()}
    tot = {r: v for r, v in summ.group_by("REPTDATE").agg(pl.col("CURBAL").sum()).iter_rows()}
    cnt = {r: v for r, v in summ.group_by("REPTDATE").agg(pl.col("N").sum()).iter_rows()}

    headers = currencies + ["TOT AMT O/S RM", "NO OF ACCT"]
    rows = {
        d: [_comma(bal.get((d, c)), CELL) for c in currencies]
        + [_comma(tot[d], CELL), str(int(cnt[d])).rjust(CELL)]      # NO OF ACCT: F=16. (no comma)
        for d in dates
    }

    panels = [range(s, min(s + PANEL_COLS, len(headers))) for s in range(0, len(headers), PANEL_COLS)]
    titles = ["REPORT ID : EIBMRB01", "DAILY TOTAL OUTSTANDING BALANCE/ACCOUNT ON FCY FD", f"AS AT {CTX['rdate']}"]

    lines = []
    page_no = 0
    for pi, cols in enumerate(panels):
        last_panel = pi == len(panels) - 1
        k = len(cols)
        c_in = max(0, min(cols.stop, ncur) - cols.start)       # currency columns in this panel
        span = c_in * CELL + c_in - 1
        blanks = "".join(" " * CELL + "|" for _ in range(k - c_in))
        dash_blanks = "".join(" " * CELL + "|" for _ in range(k - c_in))
        width = 1 + DATE_W + 1 + k * CELL + (k - 1) + 1

        pos = 0
        while pos < len(dates):
            remaining = len(dates) - pos
            if last_panel and remaining <= ROWS_LAST:
                take, continued = remaining, False
            else:
                take, continued = min(ROWS_CONT, remaining), True
            chunk = dates[pos:pos + take]
            pos += take

            lines.append((FF if page_no else "") + _pad(titles[0]))
            lines.append(_pad(titles[1]))
            lines.append(_pad(titles[2]))
            lines.append(_pad(""))
            lines.append(_pad("-" * width))
            lines.append(_pad(
                "|" + "DATE".ljust(DATE_W) + "|"
                + (_center(LABEL, span) + "|" if c_in else "") + blanks
            ))
            lines.append(_pad(
                "|" + " " * DATE_W + "|"
                + ("-" * span + "|" if c_in else "") + dash_blanks
            ))
            lines.append(_pad(
                "|" + " " * DATE_W + "|" + "|".join(_center(headers[i], CELL) for i in cols) + "|"
            ))
            lines.append(_pad(_rule(k)))
            for n, d in enumerate(chunk):
                if n:
                    lines.append(_pad(_rule(k)))
                lines.append(_pad(
                    "|" + d.strftime("%d/%m/%y").ljust(DATE_W) + "|"
                    + "|".join(rows[d][i] for i in cols) + "|"
                ))
            lines.append(_pad("-" * width))
            if continued:
                lines.append(_pad(""))
                lines.append(_pad(CONTINUED))
            page_no += 1
    return lines


report_lines = _render_report(summary) if not summary.is_empty() else []

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")
print("\nEIBMRB01 complete.")
