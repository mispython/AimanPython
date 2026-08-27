#!/usr/bin/env python3
"""
Program : EIFMLN03.py
Purpose : Weighted Average Lending Rate on HPD (RDIR II)

Dependency:
    %INC PGM(PBBELF); -> from PBBELF import format_brchcd
    BRCHCD.  is the only PUT() format referenced in this program body
    ("BRCH=PUT(BRANCH,BRCHCD.);"). All other PBBELF definitions
    (EL_DEFINITIONS, CACBRCH_MAP, REGIOFF_MAP, REGNEW_MAP, CTYPE_MAP,
    BRCHRVR_MAP, branch-list helpers, etc.) have no traceable PUT()/direct
    call anywhere in this program, so they are intentionally NOT imported.
    NOTE: BRCH itself is computed in the SAS DATA LOAN step but is never
    referenced again afterwards (not in the CLASS/VAR list of PROC SUMMARY,
    not in the PROC REPORT COLUMN list) -- it is a dead variable in the
    original SAS. It is still computed here for logic fidelity, but has no
    effect on any output.

============================================================================
PHYSICAL INPUT DATASETS (each cached to Parquet independently, using the
same chunked sas7bdat -> Parquet -> cache pattern as EIBDLN1M.py /
EIIMLN03.py)
============================================================================
1. BNM.SDESC              (JCL //BNM DD DSN=SAP.PIBB.SASDATA)
   File : INPUT_SDESC_FILE -> sdesc.sas7bdat
   Cols used : SDESC
   Used : DATA DESC; SET BNM.SDESC; CALL SYMPUT('SDESC',PUT(SDESC,$26.));
          No BY / no RUN before the next DATA step -- SAS loops over every
          row of SDESC, overwriting the SDESC macro variable each time, so
          the FINAL value is the LAST observation's SDESC value. Reproduced
          here by taking the last row.

2. BNM.LOAN&REPTMON&NOWK  (JCL //BNM DD DSN=SAP.PIBB.SASDATA)
   File : INPUT_LOAN_FILE -> constructed directly as
          loan{REPTMON}{NOWK}.sas7bdat. NOWK is a LITERAL constant '4'
          in this program (CALL SYMPUT('NOWK',PUT('4',$1.));) -- it is
          NOT derived from the report day (unlike EIIMLN03's exact-day
          NOWK). Because the resulting filename is still fully
          deterministic from REPTMON + the fixed NOWK, input_date.py's
          get_latest_file() is NOT used here, per project convention for
          deterministic month/week-suffixed inputs.
   Cols used : PRODCD, LOANSTAT, NOTETERM, INTRATE, BALANCE, BRANCH

REPTDATE / RDATE derivation differs from REPTDATE.py's default NOWK/RDATE:
  - NOWK is the hardcoded literal '4' (SAS: PUT('4',$1.)).
  - REPTMON = PUT(MONTH(REPTDATE),Z2.) -- same shape as REPTDATE.py.
  - RDATE = PUT(REPTDATE,WORDDATX18.) -- a "DD MonthName YYYY" word-date
    format, left-justified/padded to 18 characters (SAS word-date formats
    behave as character-like output). This is DIFFERENT from REPTDATE.py's
    default RDATE (DDMMYY8-style) and is therefore computed locally with a
    dedicated helper, reusing only get_reptdate_values() for the base
    REPTDATE value (TODAY()-1).

============================================================================
OUTPUTS
============================================================================
1. //SASLIST DD DSN=SAP.PIBB.EIFMLN03, DCB=(RECFM=FB,LRECL=133,BLKSIZE=0)
   -> OUTPUT_REPORT_FILE (EIFMLN03.txt). Fixed catalogued (GDG-style) name,
      no date token -> output_date.py NOT applicable. RECFM=FB (not FBA)
      => NO ASA carriage control byte, per project convention (page breaks
      are embedded as literal form-feed characters instead, matching
      EIIMLN03.py's convention).
"""

import gc
import math
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from PBBELF import format_brchcd

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_SDESC_DIR = STG_DIR / "sasdata"
INPUT_LOAN_DIR  = STG_DIR / "sasdata"

INPUT_SDESC_FILE = INPUT_SDESC_DIR / "sdesc.sas7bdat"
# INPUT_LOAN_FILE is built below once REPTMON / NOWK are known.

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIFMLN03"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIFMLN03"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_REPORT_FILE = OUTPUT_DIR / "EIFMLN03.txt"

CHUNK_ROWS = 500_000
PAGE_SIZE  = 60      # lines per page (not specified in SAS -> default)
LINE_WIDTH = 133     # DCB LRECL for SASLIST

# ============================================================================
# STEP 1: DATA REPTDATE; SET BNM.REPTDATE;
#         CALL SYMPUT('NOWK',PUT('4',$1.));
#         CALL SYMPUT('REPTMON',PUT(MONTH(REPTDATE),Z2.));
#         CALL SYMPUT('RDATE',PUT(REPTDATE,WORDDATX18.));
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values()
reptdate = reptdate_values.reptdate

# NOWK is a LITERAL constant in this program (not day-derived) -- preserved
# verbatim from the SAS source.
NOWK = "4"
REPTMON = reptdate.strftime("%m")   # PUT(MONTH(REPTDATE),Z2.)


def _worddatx18(d) -> str:
    """WORDDATX18. -- 'DD MonthName YYYY' word-date, character-like output
    left-justified/padded to a total field width of 18 (SAS word-date
    formats are treated as character output, not right-justified numerics).
    """
    text = f"{d.day:d} {d.strftime('%B').upper()} {d.year:d}"
    if len(text) > 18:
        text = text[:18]
    return text.ljust(18)


RDATE = _worddatx18(reptdate)

INPUT_LOAN_FILE = INPUT_LOAN_DIR / f"loan{REPTMON}{NOWK}.sas7bdat"

print(f"  REPTMON : {REPTMON}   NOWK : {NOWK}")
print(f"  RDATE   : '{RDATE}'")
print(f"  LOAN input file : {INPUT_LOAN_FILE.name}")


# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET  (EIBDLN1M.py pattern)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    first_chunk = None
    try:
        first_chunk = next(reader)
    except StopIteration:
        empty_df = pd.read_sas(sas_path, encoding="latin1")
        if len(empty_df.columns) == 0:
            pq.write_table(pa.Table.from_pandas(pd.DataFrame()), cache_path)
        else:
            fields = []
            for col, dtype in empty_df.dtypes.items():
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
            empty_table = pa.Table.from_pandas(empty_df, schema=schema, preserve_index=False)
            pq.write_table(empty_table, cache_path)
        print(f"  [{tag}] Done - 0 rows cached (empty file).")
        return

    writer = None
    schema = None
    total = 0
    chunks = [first_chunk] + list(reader)

    for chunk in chunks:
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
# STEP 2: CACHE INPUT SAS FILES TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
SDESC_CACHE = _load_cached(INPUT_SDESC_FILE, "SDESC")
LOAN_CACHE  = _load_cached(INPUT_LOAN_FILE, "LOAN")

# ============================================================================
# STEP 3: DATA DESC; SET BNM.SDESC; CALL SYMPUT('SDESC',PUT(SDESC,$26.));
# ============================================================================
print("\nStep 3: Deriving &SDESC (last observation of BNM.SDESC)...")

con = duckdb.connect(database=":memory:")
sdesc_df = con.execute(f"""
    SELECT CAST(SDESC AS VARCHAR) AS SDESC
    FROM read_parquet('{SDESC_CACHE.as_posix()}')
""").pl()
con.close()

_sdesc_raw = sdesc_df["SDESC"][-1] if len(sdesc_df) > 0 else ""
SDESC = (_sdesc_raw or "")[:26].ljust(26)
print(f"  SDESC: '{SDESC}'")

# ============================================================================
# STEP 4: DATA LOAN; SET BNM.LOAN&REPTMON&NOWK;
#   IF PRODCD='34111' AND LOANSTAT=1;
#   IF NOTETERM > 12 THEN TERM=12; ELSE TERM=NOTETERM;
#   TRATE = NOTETERM*INTRATE;
#   APR = TRATE*(300*TERM+TRATE)/((NOTETERM*TRATE)+(150*TERM*(NOTETERM+1)));
#   WAMT = BALANCE*APR;
#   BRHNO=BRANCH; BRCH=PUT(BRANCH,BRCHCD.);
# ============================================================================
print("\nStep 4: Loading LOAN dataset and computing weighted amount...")

con = duckdb.connect(database=":memory:")
loan_raw = con.execute(f"""
    SELECT
        CAST(PRODCD   AS VARCHAR) AS PRODCD,
        CAST(LOANSTAT AS DOUBLE)  AS LOANSTAT,
        CAST(NOTETERM AS DOUBLE)  AS NOTETERM,
        CAST(INTRATE  AS DOUBLE)  AS INTRATE,
        CAST(BALANCE  AS DOUBLE)  AS BALANCE,
        CAST(BRANCH   AS INTEGER) AS BRANCH
    FROM read_parquet('{LOAN_CACHE.as_posix()}')
    WHERE PRODCD = '34111' AND LOANSTAT = 1
""").pl()
con.close()

print(f"  LOAN rows after subsetting IF: {len(loan_raw):,}")


def _process_loan_row(row: dict) -> dict:
    noteterm = row["NOTETERM"]
    intrate  = row["INTRATE"]
    balance  = row["BALANCE"]
    branch   = row["BRANCH"]

    if noteterm is None:
        term = None
    else:
        term = 12.0 if noteterm > 12 else float(noteterm)

    trate = None if (noteterm is None or intrate is None) else noteterm * intrate

    apr = None
    if trate is not None and term is not None and noteterm is not None:
        denom = (noteterm * trate) + (150 * term * (noteterm + 1))
        if denom:
            apr = trate * (300 * term + trate) / denom

    wamt = (balance * apr) if (balance is not None and apr is not None) else None

    brhno = branch
    # BRCH: dead variable in the original SAS -- computed here for logic
    # fidelity only; it is never referenced by PROC SUMMARY or PROC REPORT
    # below.
    _brch = format_brchcd(branch) if branch is not None else ""  # noqa: F841

    return {"BRANCH": branch, "BALANCE": balance, "WAMT": wamt, "BRHNO": brhno}


loan_final_rows = []
for r in loan_raw.iter_rows(named=True):
    loan_final_rows.append(_process_loan_row(r))

del loan_raw
gc.collect()

print(f"  LOAN (final) rows: {len(loan_final_rows):,}")


# ============================================================================
# STEP 5: PROC SUMMARY helper (NWAY, default MISSING-exclusion, SUM ignores
# missing contributions, matching PROC SUMMARY's SUM statistic semantics)
# ============================================================================
def _class_summary(rows, class_fields, sum_fields):
    filtered = [r for r in rows if all(r.get(f) is not None for f in class_fields)]
    groups = {}
    for r in filtered:
        key = tuple(r[f] for f in class_fields)
        g = groups.setdefault(key, {f: None for f in sum_fields})
        for f in sum_fields:
            v = r.get(f)
            if v is not None:
                g[f] = (g[f] or 0.0) + v
    out = []
    for key, sums in groups.items():
        rec = dict(zip(class_fields, key))
        rec.update(sums)
        out.append(rec)
    return out


# ============================================================================
# STEP 6: PROC SUMMARY DATA=LOAN NWAY; CLASS BRANCH; VAR BALANCE WAMT;
#         OUTPUT OUT=LOAN1(DROP=_TYPE_ _FREQ_) SUM=;
#   DATA LOAN1; KEEP BRANCH BALANCE WAMT WAVRATE TYPE; SET LOAN1;
#         WAVRATE = WAMT/BALANCE; TYPE='A';
# ============================================================================
print("\nStep 6: Summarizing BALANCE/WAMT by BRANCH...")

loan1_rows = _class_summary(loan_final_rows, ["BRANCH"], ["BALANCE", "WAMT"])
for r in loan1_rows:
    balance = r.get("BALANCE")
    wamt = r.get("WAMT")
    r["WAVRATE"] = (wamt / balance) if (balance is not None and balance != 0 and wamt is not None) else None
    r["TYPE"] = "A"

# PROC SUMMARY with a single CLASS variable outputs groups in ascending
# sorted order of that variable -- replicate that ordering explicitly.
loan1_rows.sort(key=lambda x: x["BRANCH"])

print(f"  Branch groups: {len(loan1_rows):,}")

# ============================================================================
# STEP 7: REPORT RENDERING  (PROC REPORT emulation)
#
# COLUMN TYPE BRANCH BALANCE WAMT WAVRATE AVGRTS;
# TYPE and AVGRTS are NOPRINT. Column field layout derived directly from the
# absolute-column LINE statements in the COMPUTE AFTER TYPE block (which
# anchor exactly against the BALANCE/WAMT/WAVRATE column boundaries):
#   margin(2) BRANCH(7) gap(2) BALANCE(20) gap(2) WAMT(20) gap(2) WAVRATE(15)
#   -> BRANCH  cols 3-9      BALANCE cols 12-31
#      WAMT    cols 34-53    WAVRATE cols 56-70
# which matches "LINE @009 80*'-'", "@014 BALANCE.SUM COMMA18.2",
# "@036 WAMT.SUM COMMA18.2", "@056 AVGRTS COMMA10.8" precisely.
#
# Since TYPE is a single constant value ('A') for the whole dataset,
# BREAK AFTER TYPE / COMPUTE AFTER TYPE fires exactly once, after ALL data
# rows, producing the report's single grand-total bar. That 3-line block
# (dash / values / dash) is never split across a page break.
# ============================================================================
print("\nStep 7: Rendering PROC REPORT...")

FF = "\f"

MARGIN = 2
GAP = 2
COL_BRANCH_W  = 7
COL_BALANCE_W = 20
COL_WAMT_W    = 20
COL_WAVRATE_W = 15

HDR_BRANCH  = "BRANCH"
HDR_BALANCE = "BALANCE"
HDR_WAMT    = "WEIGHTED AMOUNT"
HDR_WAVRATE = "WGTED AV.RATE  "

TITLE1 = f"{SDESC} REPORT AS AT {RDATE}"
TITLE3 = "WEIGHTED AVERAGE LENDING RATE ON HPD (RDIR II)"
# TITLE2 was never assigned in the SAS source (only TITLE/TITLE1 and
# TITLE3 are set) -- SAS still reserves that title line as blank.
TITLES = [TITLE1, "", TITLE3]

HEADER_LINE = (
    " " * MARGIN
    + HDR_BRANCH.center(COL_BRANCH_W)
    + " " * GAP
    + HDR_BALANCE.center(COL_BALANCE_W)
    + " " * GAP
    + HDR_WAMT.center(COL_WAMT_W)
    + " " * GAP
    + HDR_WAVRATE
)

HEADLINE_DASH = (
    " " * MARGIN
    + "-" * COL_BRANCH_W
    + " " * GAP
    + "-" * COL_BALANCE_W
    + " " * GAP
    + "-" * COL_WAMT_W
    + " " * GAP
    + "-" * COL_WAVRATE_W
)


def _fmt_int(value, width):
    """FORMAT=7. -- plain zero/blank-on-missing integer, right-justified.
    OPTIONS MISSING=0 => a missing value prints as a single '0' character
    right-justified in the field."""
    if value is None:
        return "0".rjust(width)
    return str(int(value)).rjust(width)


def _fmt_comma(value, width, decimals):
    """COMMAw.d -- thousands-separated, right-justified. Missing -> '0'
    right-justified (OPTIONS MISSING=0)."""
    if value is None:
        return "0".rjust(width)
    v = float(value)
    s = f"{v:,.{decimals}f}"
    if len(s) > width:
        s = f"{v:.{decimals}f}"
    if len(s) > width:
        s = s[-width:]
    return s.rjust(width)


def _fmt_plain(value, width, decimals):
    """w.d -- fixed decimal, NO thousands separator, right-justified.
    Missing -> '0' right-justified (OPTIONS MISSING=0)."""
    if value is None:
        return "0".rjust(width)
    v = float(value)
    s = f"{v:.{decimals}f}"
    if len(s) > width:
        s = s[-width:]
    return s.rjust(width)


def _sas_round_unit(x, unit):
    """SAS ROUND(number, round-unit) -- round to nearest multiple of
    round-unit, halves away from zero."""
    if x is None:
        return None
    if x >= 0:
        return math.floor(x / unit + 0.5) * unit
    return math.ceil(x / unit - 0.5) * unit


def _data_line(row) -> str:
    return (
        " " * MARGIN
        + _fmt_int(row.get("BRANCH"), COL_BRANCH_W)
        + " " * GAP
        + _fmt_comma(row.get("BALANCE"), COL_BALANCE_W, 2)
        + " " * GAP
        + _fmt_comma(row.get("WAMT"), COL_WAMT_W, 2)
        + " " * GAP
        + _fmt_plain(row.get("WAVRATE"), 10, 8).rjust(COL_WAVRATE_W)
    )


def _line_at(width, segments):
    """Build a fixed-width line, placing each (col, text) segment starting
    at its 1-indexed absolute column -- replicates SAS LINE @n semantics."""
    buf = [" "] * width
    for col, text in segments:
        start = col - 1
        for i, ch in enumerate(text):
            pos = start + i
            if 0 <= pos < width:
                buf[pos] = ch
    return "".join(buf)


def _render_report(rows, titles):
    output: list = []
    lines_on_page = [0]

    def _new_page():
        block = []
        block.append(FF + titles[0])
        for t in titles[1:]:
            block.append(t)
        block.append("")
        block.append(HEADER_LINE)
        block.append(HEADLINE_DASH)
        block.append("")  # HEADSKIP
        output.extend(block)
        lines_on_page[0] = len(block)

    _new_page()

    for r in rows:
        if lines_on_page[0] >= PAGE_SIZE:
            _new_page()
        output.append(_data_line(r))
        lines_on_page[0] += 1

    # COMPUTE AFTER TYPE (fires once -- TYPE is a single constant value)
    grand_balance = 0.0
    grand_wamt = 0.0
    for r in rows:
        b = r.get("BALANCE")
        w = r.get("WAMT")
        if b is not None:
            grand_balance += b
        if w is not None:
            grand_wamt += w

    avgrts = None
    if grand_balance:
        avgrts = _sas_round_unit(grand_wamt / grand_balance, 0.00000001)

    dash_line = _line_at(LINE_WIDTH, [(9, "-" * 80)])
    sum_line = _line_at(
        LINE_WIDTH,
        [
            (9, " "),
            (14, _fmt_comma(grand_balance, 18, 2)),
            (36, _fmt_comma(grand_wamt, 18, 2)),
            (56, _fmt_comma(avgrts, 10, 8)),
        ],
    )

    break_block = [dash_line, sum_line, dash_line]
    if lines_on_page[0] + len(break_block) > PAGE_SIZE:
        _new_page()
    output.extend(break_block)

    return output


report_lines = _render_report(loan1_rows, TITLES)

with open(OUTPUT_REPORT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln.ljust(LINE_WIDTH) + "\n")

print(f"  Report written : {OUTPUT_REPORT_FILE}")
print(f"  Report lines   : {len(report_lines):,}")

print("\nEIFMLN03 complete.")
