#!/usr/bin/env python3
"""
Program : DMMISRI3.py
Purpose : Breakdown Of Islamic Savings Account Deposit (PIBB, IBU)
          Converted from SAS job DMMISRI3.

Dependency:
    %INC PGM(PBBDPFMT,PBMISFMT);
    -> from PBBDPFMT import sdrange_format
       (SDRANGE. invalue format -- used to bucket CURBAL into the
        upper-bound RANGE value consumed by SADPRG.)
       PBBDPFMT is otherwise NOT used any further in this program -- none
       of its other formats (FDPROD, CAPROD, CADENOM, etc.) are invoked
       anywhere in the SAS source body, so only sdrange_format is imported.
    -> from PBMISFMT import format_sadprg, format_sdname
       (SADPRG.  -- deposit-range display label applied to RANGE)
       (SDNAME.  -- FORMAT PRODUCT SDNAME. in PROC TABULATE, also used to
        resolve the BOX=_PAGE_ page-corner label for each PRODUCT page.)

============================================================================
PHYSICAL INPUT DATASET  (cached to Parquet using the same chunked
sas7bdat -> Parquet -> cache pattern as EIIMRM01.py / EIBDLN1M.py)
============================================================================
1. BNM.ISA&REPTMON&NOWK&REPTYEAR  (JCL //BNM DD DSN=SAP.PIBB.SADATAWH)
   The SAS dataset name is built entirely from macro variables that are
   all deterministically derivable from REPTDATE.py (REPTMON, NOWK,
   REPTYEAR) -- there is no non-deterministic component, so the physical
   filename below is constructed directly and get_latest_file() from
   input_date.py is intentionally NOT used (per project convention for
   deterministic filenames).
   File : INPUT_ISA_FILE -> isa<REPTMON><NOWK><REPTYEAR>.sas7bdat
          (physical lowercase filename assumed; adjust prefix if the
          actual staging convention differs)
   Cols used : PRODUCT, CURBAL

============================================================================
OUTPUT
============================================================================
SASLIST DD SYSOUT=(,),OUTPUT=(*.PRINT7) -- a printer listing (PROC TABULATE
report), not a catalogued dataset with a date token in its name, so the
Python output uses a fixed filename (output_date.py is not applicable here).

This is a printed report, so per project convention the output carries ASA
carriage-control characters (RECFM=FBA equivalent): the first character of
every physical line is the ASA control byte ('1' = skip to new page,
' ' = single space then print). PAGESIZE is not specified in the SAS
source, so the project default of 60 lines/page is assumed.

OPTIONS NOCENTER -- titles are left-justified (column 1), not centred.
"""

import gc
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from PBBDPFMT import sdrange_format
from PBMISFMT import format_sadprg, format_sdname

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_ISA_DIR = STG_DIR / "from_dwh"

CACHE_DIR = BASE_DIR / "input" / "cache" / "DMMISRI3"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "DMMISRI3"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "DMMISRI3.txt"

CHUNK_ROWS = 500_000
PAGE_SIZE  = 60          # lines per page (not specified in SAS -> default)

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet -- derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values()          # year_format="%y" -> YEAR2.
REPTYEAR = reptdate_values.reptyear              # 2-digit year
REPTMON  = reptdate_values.reptmon               # zero-padded month (Z2.)
REPTDAY  = reptdate_values.reptday               # zero-padded day   (Z2.)
# NOWK: SAS SELECT/WHEN uses 1<=day<=8 -> '1', 9<=day<=15 -> '2',
# 16<=day<=22 -> '3', OTHERWISE -> '4'. This is IDENTICAL to the range
# logic already implemented in REPTDATE.get_reptdate_values(), so NOWK is
# taken directly from it (no local recomputation needed, unlike programs
# where the SAS source used a different day-matching rule).
NOWK = reptdate_values.nowk
RDATE = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"        # PUT(REPTDATE, DDMMYY8.)

print(f"  RDATE   : {RDATE}")
print(f"  REPTMON : {REPTMON}  NOWK: {NOWK}  REPTYEAR: {REPTYEAR}")

# Deterministic physical input filename (see module docstring).
# INPUT_ISA_FILE = INPUT_ISA_DIR / f"isa{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
INPUT_ISA_FILE = INPUT_ISA_DIR / f"isa08426.sas7bdat"
print(f"  Input file  : {INPUT_ISA_FILE.name}")
print(f"  Output file : {OUTPUT_FILE.name}")

# ============================================================================
# TITLES  (OPTIONS NOCENTER -- left-justified, not centred)
# ============================================================================
TITLE1 = "REPORT ID : DMMISRI3"
TITLE2 = "PUBLIC ISLAMIC BANK BERHAD (IBU)"
TITLE3 = "BREAKDOWN OF ISLAMIC SAVINGS ACCOUNT DEPOSIT"
TITLE4 = f"REPORT AS AT {RDATE}"
TITLE_BLOCK = [TITLE1, TITLE2, TITLE3, TITLE4]

# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET  (EIIMRM01.py pattern)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if schema is None:
            fields = []
            for col, dtype in chunk.dtypes.items():
                if dtype == 'object':
                    pa_type = pa.string()
                elif pd.api.types.is_integer_dtype(dtype):
                    pa_type = pa.int64()
                elif pd.api.types.is_float_dtype(dtype):
                    pa_type = pa.float64()
                else:
                    pa_type = pa.from_numpy_dtype(dtype)
                fields.append(pa.field(col, pa_type))
            schema = pa.schema(fields)
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
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# STEP 2: CACHE INPUT SAS FILE TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS dataset to Parquet...")
ISA_CACHE = _load_cached(INPUT_ISA_FILE, "ISA")

# ============================================================================
# STEP 3: DATA SAVE;  SET BNM.ISA...; IF PRODUCT IN (204,215); IF CURBAL GE 0;
#         RANGE = INPUT(CURBAL, SDRANGE.); DEPRANGE = PUT(RANGE, SADPRG.);
# ============================================================================
print("\nStep 3: Building SAVE (PRODUCT in (204,215), CURBAL >= 0)...")

con = duckdb.connect(database=":memory:")
save_raw = con.execute(f"""
    SELECT
        CAST(PRODUCT AS INTEGER) AS PRODUCT,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL
    FROM read_parquet('{ISA_CACHE.as_posix()}')
    WHERE CAST(PRODUCT AS INTEGER) IN (204, 215)
      AND CAST(CURBAL AS DOUBLE) >= 0
""").pl()
con.close()

print(f"  SAVE rows: {len(save_raw):,}")

save_rows = []
for r in save_raw.iter_rows(named=True):
    curbal = r["CURBAL"]
    range_val = sdrange_format(curbal)         # INPUT(CURBAL, SDRANGE.)
    deprange = format_sadprg(range_val)        # PUT(RANGE, SADPRG.)
    save_rows.append({
        "PRODUCT": r["PRODUCT"],
        "CURBAL": curbal,
        "RANGE": range_val,
        "DEPRANGE": deprange,
    })

del save_raw
gc.collect()

# ============================================================================
# STEP 4: PROC SUMMARY NWAY; CLASS DEPRANGE PRODUCT; VAR CURBAL;
#         OUTPUT OUT=SDRNGE (RENAME=(_FREQ_=NOACCT)) SUM=CURBAL;
# ============================================================================
print("\nStep 4: Summarising by DEPRANGE x PRODUCT (NWAY)...")

_summary = {}
for r in save_rows:
    key = (r["DEPRANGE"], r["PRODUCT"])
    g = _summary.setdefault(key, {"NOACCT": 0, "CURBAL": 0.0})
    g["NOACCT"] += 1
    g["CURBAL"] += r["CURBAL"]

summary_rows = [
    {"DEPRANGE": dep, "PRODUCT": prod, "NOACCT": v["NOACCT"], "CURBAL": v["CURBAL"]}
    for (dep, prod), v in _summary.items()
]
print(f"  SDRNGE summary rows: {len(summary_rows):,}")

# ============================================================================
# ALL 48 SDRANGE / SADPRG BANDS  (for PROC TABULATE PRINTMISS)
# ------------------------------------------------------------------------
# PRINTMISS forces every DEPRANGE band to print on every PRODUCT page even
# when it has zero matching rows. This list of upper-bound bucket values
# MUST mirror the breakpoints list inside PBBDPFMT.sdrange_format() exactly
# -- it is duplicated here (rather than introspected) purely to enumerate
# the full set of 48 bands in ascending order for report rendering.
# ============================================================================
_ALL_RANGE_BREAKPOINTS = [
    5, 10, 50, 100, 500, 1000, 1500, 2000, 2500, 3000,
    3500, 4000, 4500, 5000, 6000, 7000, 8000, 9000, 10000,
    15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000,
    55000, 60000, 65000, 70000, 75000, 80000, 85000, 90000,
    95000, 100000, 150000, 200000, 300000, 500000,
    1000000, 2000000, 3000000, 4000000, 5000000, 10000000,
    10000001,   # overflow bucket (band 48)
]
ALL_RANGE_LABELS = [format_sadprg(float(bp)) for bp in _ALL_RANGE_BREAKPOINTS]

# ============================================================================
# STEP 5: PROC TABULATE  (PAGE=PRODUCT ALL='BANK TOTAL',
#         ROW=DEPRANGE ALL='TOTAL', COL=NOACCT CURBAL)
# BOX=_PAGE_ RTS=35 PRINTMISS CONDENSE NOSEPS MISSING
# ============================================================================
print("\nStep 5: Rendering PROC TABULATE report...")

LABEL_WIDTH   = 35   # RTS=35
NOACCT_WIDTH  = 12   # COMMA12.
AMOUNT_WIDTH  = 20   # DOLLAR20.2

PRODUCT_PAGES = [204, 215, None]   # None represents the ALL='BANK TOTAL' page


def _comma_int(value: int, width: int) -> str:
    return f"{int(value):,d}".rjust(width)


def _dollar(value: float, width: int, decimals: int = 2) -> str:
    v = float(value)
    neg = v < 0
    v = abs(v)
    s = f"${v:,.{decimals}f}"
    if neg:
        s = "-" + s
    return s.rjust(width)


def _center(text: str, width: int) -> str:
    text = text[:width]
    pad = width - len(text)
    left = pad // 2
    right = pad - left
    return " " * left + text + " " * right


def _box_label(product) -> str:
    """BOX=_PAGE_ -- shows the current page's formatted class value."""
    if product is None:
        return "BANK TOTAL"          # ALL='BANK TOTAL' literal label
    return format_sdname(product)    # FORMAT PRODUCT SDNAME.


def _band_sums(rows, product) -> dict:
    sums = {}
    for r in rows:
        if product is not None and r["PRODUCT"] != product:
            continue
        d = sums.setdefault(r["DEPRANGE"], {"NOACCT": 0, "CURBAL": 0.0})
        d["NOACCT"] += r["NOACCT"]
        d["CURBAL"] += r["CURBAL"]
    return sums


def _header_lines(box_label: str) -> list:
    div = "-" * LABEL_WIDTH + "+" + "-" * NOACCT_WIDTH + "+" + "-" * AMOUNT_WIDTH
    hdr = (
        box_label.ljust(LABEL_WIDTH)[:LABEL_WIDTH] + "|"
        + _center("NO OF A/C", NOACCT_WIDTH) + "|"
        + _center("AMOUNT", AMOUNT_WIDTH)
    )
    return [div, hdr, div]


def _data_row(label: str, noacct: int, curbal: float) -> str:
    return (
        label.ljust(LABEL_WIDTH)[:LABEL_WIDTH] + "|"
        + _comma_int(noacct, NOACCT_WIDTH) + "|"
        + _dollar(curbal, AMOUNT_WIDTH)
    )


def _render_page(product) -> list:
    """Returns a list of (asa_char, text) tuples for one PRODUCT page,
    including internal PAGESIZE=60 pagination if the band count ever
    exceeds one physical page."""
    sums = _band_sums(summary_rows, product)
    box_label = _box_label(product)

    lines: list = []
    page_line_count = {"n": 0}

    def _emit_page_start():
        lines.append(("1", TITLE1))
        for t in TITLE_BLOCK[1:]:
            lines.append((" ", t))
        lines.append((" ", ""))
        for h in _header_lines(box_label):
            lines.append((" ", h))
        page_line_count["n"] = 4 + 1 + len(_header_lines(box_label))

    _emit_page_start()

    grand_noacct = 0
    grand_curbal = 0.0

    for label in ALL_RANGE_LABELS:
        if page_line_count["n"] >= PAGE_SIZE:
            _emit_page_start()

        band = sums.get(label, {"NOACCT": 0, "CURBAL": 0.0})
        grand_noacct += band["NOACCT"]
        grand_curbal += band["CURBAL"]

        lines.append((" ", _data_row(label, band["NOACCT"], band["CURBAL"])))
        page_line_count["n"] += 1

    if page_line_count["n"] >= PAGE_SIZE:
        _emit_page_start()

    lines.append((" ", _data_row("TOTAL", grand_noacct, grand_curbal)))
    lines.append((" ", "-" * LABEL_WIDTH + "+" + "-" * NOACCT_WIDTH + "+" + "-" * AMOUNT_WIDTH))

    return lines


report_lines: list = []
for product in PRODUCT_PAGES:
    report_lines.extend(_render_page(product))

print(f"  Total report lines: {len(report_lines):,}")

# ============================================================================
# STEP 6: WRITE OUTPUT  (RECFM=FBA -- ASA carriage-control byte + text)
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for asa, text in report_lines:
        fh.write(asa + text + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")

print("\nDMMISRI3 complete.")
