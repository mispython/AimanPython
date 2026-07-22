#!/usr/bin/env python3
"""
Program : EIBMDPBR.py
Purpose : Development of Wealth Management Centres -
          Deposit Range Profile by Selected Branches Report
          (SMR 2006-1400. SELECTED BRANCHES.)
"""

import gc
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
# from output_date import build_output_file
#
# input_date.py IS used for SAVING, CURRENT and FD: their physical filenames
# encode an MMWYY date (e.g. sa07226.sas7bdat = month 07, week 2, year 26),
# so get_latest_file() resolves the newest file per prefix ('sa','ca','fd').
#
# input_date.py is NOT used for CISDP / CISSA: the JCL source DSNs
# (SAP.PBB.CISBEXT.DP / SAP.PBB.CRM.CISBEXT) carry no date component in their
# names, so fixed physical filenames are used instead (mirrors the
# CISLN_loan.sas7bdat / CISDP_deposit.sas7bdat fixed-name pattern in
# EIBDLN1M.py).
#
# output_date.py is NOT used: the output DSN SAP.PBB.PROFILE.DPBR carries no
# date component either, so the output filename is fixed ("DPBR.txt").

# ============================================================================
# DEPENDENCY: format libraries
# Original SAS: %INC PGM(PBBLNFMT,PBBDPFMT,PBMISFMT);
# PBBLNFMT and PBBDPFMT are included at session level only - neither defines
# a BRCHCD format (checked: PBBLNFMT has STATECD/STATEPOST; PBBDPFMT has its
# own STATECD and BRANCHCD-to-full-name, not BRCHCD), and no other explicit
# PUT(var,fmt.) call against their formats appears in this program body, so
# they are NOT imported here (comment only).
# The BRCHCD. format IS explicitly invoked via PUT(BRANCH,BRCHCD.) below.
# Confirmed source: PBMISFMT.format_brchcd() (numeric branch -> 3-letter code).
# ============================================================================
from PBMISFMT import format_brchcd as get_brchcd_format

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_DIR = BASE_DIR / "input" / "prod" / "EIBMDPBR"

# Fixed physical input files (no date component in source DSNs)
INPUT_CISDP_FILE = INPUT_DIR / "cisdp_deposit.sas7bdat"   # CISDP.DEPOSIT
INPUT_CISSA_FILE = INPUT_DIR / "cissa_deposit.sas7bdat"   # CISSA.DEPOSIT (CRM external)

# SAVING / CURRENT / FD filenames encode an MMWYY date (e.g. sa07226.sas7bdat)
# and are resolved to the latest file per prefix via input_date.get_latest_file()
INPUT_DEP_DIR      = INPUT_DIR
SAVING_PREFIX      = "sa"   # DEP.SAVING  -> sa{MM}{W}{YY}.sas7bdat
CURRENT_PREFIX     = "ca"   # DEP.CURRENT -> ca{MM}{W}{YY}.sas7bdat
FD_PREFIX          = "fd"   # DEP.FD      -> fd{MM}{W}{YY}.sas7bdat

# Parquet cache directory (co-located with source, same convention as EIBDLN1M)
CACHE_DIR = INPUT_DIR

OUTPUT_DIR  = BASE_DIR / "output" / "EIBMDPBR"
OUTPUT_FILE = OUTPUT_DIR / "DPBR.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# CHUNK SIZE FOR STREAMING LARGE .sas7bdat FILES
# ============================================================================
CHUNK_ROWS = 500_000

# ============================================================================
# REPORT PAGE CONFIGURATION
# ============================================================================
PAGE_SIZE    = 60   # lines per page (SAS default)
HEADER_LINES = 7     # title(3) + blank + column header + headline + headskip
CONTENT_WIDTH = 132  # LRECL=133 minus 1 ASA carriage-control byte

# ============================================================================
# STEP 0: DELETE OLD OUTPUT FILE  (DELETE EXEC PGM=IEFBR14 equivalent)
# ============================================================================
print("Step 0: Removing stale output file (if any)...")
if OUTPUT_FILE.exists():
    OUTPUT_FILE.unlink()

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# DATA DATES; SET DEP.REPTDATE;
#   CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY8.));
#   CALL SYMPUT('REPTDAY',PUT(DAY(REPTDATE),Z2.));
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values()
reptdate = reptdate_values.reptdate

RDATE   = reptdate.strftime("%d/%m/%y")   # DDMMYY8. equivalent
REPTDAY = reptdate.strftime("%d")          # Z2. equivalent

print(f"  Report date : {RDATE}")
print(f"  Output file : {OUTPUT_FILE.name}")

# ============================================================================
# PROC FORMAT  VALUE DEPRAN  (defined locally - fully specified in this
# SAS program, so implemented directly rather than treated as a dependency)
# ============================================================================
_DEPRAN_BANDS = [
    (0,          50_000,      " 0.              BELOW RM 50,000 "),
    (50_000,     100_000,     " 1. RM  50,000 - BELOW RM100,000 "),
    (100_000,    200_000,     " 2. RM 100,000 - BELOW RM200,000 "),
    (200_000,    300_000,     " 3. RM 200,000 - BELOW RM300,000 "),
    (300_000,    400_000,     " 4. RM 300,000 - BELOW RM400,000 "),
    (400_000,    500_000,     " 5. RM 400,000 - BELOW RM500,000 "),
    (500_000,    600_000,     " 6. RM 500,000 - BELOW RM600,000 "),
    (600_000,    700_000,     " 7. RM 600,000 - BELOW RM700,000 "),
    (700_000,    800_000,     " 8. RM 700,000 - BELOW RM800,000 "),
    (800_000,    900_000,     " 9. RM 800,000 - BELOW RM900,000 "),
    (900_000,    1_000_000,   "10. RM 900,000 - BELOW RM 1 MILL "),
    (1_000_000,  2_000_000,   "11. RM  1 MILL - BELOW RM 2 MILL"),
    (2_000_000,  3_000_000,   "12. RM  2 MILL - BELOW RM 3 MILL"),
    (3_000_000,  4_000_000,   "13. RM  3 MILL - BELOW RM 4 MILL"),
    (4_000_000,  5_000_000,   "14. RM  4 MILL - BELOW RM 5 MILL"),
    (5_000_000,  6_000_000,   "15. RM  5 MILL - BELOW RM 6 MILL"),
    (6_000_000,  7_000_000,   "16. RM  6 MILL - BELOW RM 7 MILL"),
    (7_000_000,  8_000_000,   "17. RM  7 MILL - BELOW RM 8 MILL"),
    (8_000_000,  9_000_000,   "18. RM  8 MILL - BELOW RM 9 MILL"),
    (9_000_000,  10_000_000,  "19. RM  9 MILL - BELOW RM10 MILL"),
]
_DEPRAN_HIGH = "20. RM 10 MILL & ABOVE           "


def get_deposit_range(balance: float) -> str:
    """Equivalent of PUT(BALANCE,DEPRAN.)."""
    bal = balance if balance is not None else 0.0
    for low, high, label in _DEPRAN_BANDS:
        if low <= bal < high:
            return label
    if bal < 0:
        # LOW - <50000 covers negative balances too
        return _DEPRAN_BANDS[0][2]
    return _DEPRAN_HIGH


# ============================================================================
# %LET BRH=(...)  — defined in the original SAS but never referenced anywhere
# else in the program (the actual branch filter in DATA DPBR uses a separate,
# larger literal list). Dead macro variable — intentionally not translated.
# ============================================================================

STFSA = (151, 181, 200, 201, 215)

STFCA = (
    50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65,
    101, 103, 106, 151, 158, 164, 180, 181, 182,
)

SELECTED_BRANCHES = (
    2, 3, 4, 5, 6, 7, 8, 9, 13, 15, 18, 19, 20, 24, 25,
    26, 28, 33, 36, 37, 38, 40, 41, 42, 44, 45, 54, 56, 57, 58,
    60, 66, 68, 78, 79, 90, 94, 110, 123, 129, 130, 135, 136, 145,
    153, 157, 168, 179, 183, 185, 187, 198, 200, 207, 216,
)

# ============================================================================
# HELPER: CACHE STAMP  (skip re-conversion if .sas7bdat hasn't changed)
# (pattern from EIBDLN1M.py)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


# ============================================================================
# HELPER: STREAM .sas7bdat → PARQUET  (pattern from EIBDLN1M.py)
# ============================================================================
def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

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
                        print(f"  [{tag}] WARNING: Cannot cast '{field.name}' "
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


# ============================================================================
# STEP 2: RESOLVE LATEST SAVING / CURRENT / FD FILES, THEN CACHE TO PARQUET
# Filenames encode MMWYY (e.g. sa07226.sas7bdat = month 07, week 2, year 26).
# ============================================================================
print("\nStep 2: Resolving latest SAVING / CURRENT / FD files...")

saving_path  = get_latest_file(INPUT_DEP_DIR, prefix=SAVING_PREFIX)
current_path = get_latest_file(INPUT_DEP_DIR, prefix=CURRENT_PREFIX)
fd_path      = get_latest_file(INPUT_DEP_DIR, prefix=FD_PREFIX)

print(f"  SAVING  : {saving_path.name}")
print(f"  CURRENT : {current_path.name}")
print(f"  FD      : {fd_path.name}")

print("\nStep 2b: Caching SAS files to Parquet (if needed)...")

CISDP_CACHE   = CACHE_DIR / "cisdp_deposit.parquet"
CISSA_CACHE   = CACHE_DIR / "cissa_deposit.parquet"
SAVING_CACHE  = CACHE_DIR / f"{saving_path.stem}.parquet"
CURRENT_CACHE = CACHE_DIR / f"{current_path.stem}.parquet"
FD_CACHE      = CACHE_DIR / f"{fd_path.stem}.parquet"

_sources = (
    (INPUT_CISDP_FILE, CISDP_CACHE,   "CISDP"),
    (INPUT_CISSA_FILE, CISSA_CACHE,   "CISSA"),
    (saving_path,      SAVING_CACHE,  "SAVING"),
    (current_path,     CURRENT_CACHE, "CURRENT"),
    (fd_path,          FD_CACHE,      "FD"),
)

for sas_path, cache_path, tag in _sources:
    if not _cache_is_fresh(sas_path, cache_path):
        sas_to_parquet(sas_path, cache_path, tag)
    else:
        print(f"  [{tag}] Cache fresh — skipping conversion.")

# ============================================================================
# STEP 3: BUILD CISDP  (customer-number lookup)
# DATA CISDP; KEEP ACCTNO CUSTNO; SET CISDP.DEPOSIT CISSA.DEPOSIT;
# PROC SORT NODUPKEY; BY CUSTNO ACCTNO;
# PROC SORT; BY ACCTNO;  (resort not needed — join does not require presort)
# ============================================================================
print("\nStep 3: Building CISDP (customer number lookup)...")

con = duckdb.connect(database=":memory:")

cisdp = con.execute(f"""
    WITH cisdp_src AS (
        SELECT
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            CAST(CUSTNO AS BIGINT) AS CUSTNO,
            0 AS SRC,
            ROW_NUMBER() OVER () AS SEQ
        FROM read_parquet('{CISDP_CACHE}')
    ),
    cissa_src AS (
        SELECT
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            CAST(CUSTNO AS BIGINT) AS CUSTNO,
            1 AS SRC,
            ROW_NUMBER() OVER () AS SEQ
        FROM read_parquet('{CISSA_CACHE}')
    ),
    combined AS (
        SELECT ACCTNO, CUSTNO, SRC, SEQ FROM cisdp_src
        UNION ALL
        SELECT ACCTNO, CUSTNO, SRC, SEQ FROM cissa_src
    ),
    ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY CUSTNO, ACCTNO
                   ORDER BY SRC, SEQ
               ) AS rn
        FROM combined
    )
    SELECT ACCTNO, CUSTNO
    FROM ranked
    WHERE rn = 1
""").pl()

con.close()
gc.collect()
print(f"  CISDP rows: {len(cisdp):,}")

# ============================================================================
# STEP 4: BUILD SA / CA / FD  (per-source filtering)
# DATA SA;  SET DEP.SAVING;  IF PRODUCT IN &STFSA THEN DELETE;
# DATA CA;  SET DEP.CURRENT; IF PRODUCT IN &STFCA THEN DELETE;
# DATA FD;  SET DEP.FD;      IF (INTPLAN range) THEN DELETE;
# ============================================================================
print("\nStep 4: Filtering SA / CA / FD sources...")

con = duckdb.connect(database=":memory:")

stfsa_list = ",".join(str(v) for v in STFSA)
stfca_list = ",".join(str(v) for v in STFCA)

sa = con.execute(f"""
    SELECT
        CAST(ACCTNO  AS BIGINT)  AS ACCTNO,
        CAST(BRANCH  AS INTEGER) AS BRANCH,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL,
        CAST(OPENIND AS VARCHAR) AS OPENIND
    FROM read_parquet('{SAVING_CACHE}')
    WHERE CAST(PRODUCT AS INTEGER) NOT IN ({stfsa_list})
""").pl()

ca = con.execute(f"""
    SELECT
        CAST(ACCTNO  AS BIGINT)  AS ACCTNO,
        CAST(BRANCH  AS INTEGER) AS BRANCH,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL,
        CAST(OPENIND AS VARCHAR) AS OPENIND
    FROM read_parquet('{CURRENT_CACHE}')
    WHERE CAST(PRODUCT AS INTEGER) NOT IN ({stfca_list})
""").pl()

fd = con.execute(f"""
    SELECT
        CAST(ACCTNO  AS BIGINT)  AS ACCTNO,
        CAST(BRANCH  AS INTEGER) AS BRANCH,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL,
        CAST(OPENIND AS VARCHAR) AS OPENIND
    FROM read_parquet('{FD_CACHE}')
    WHERE NOT (
        (CAST(INTPLAN AS INTEGER) BETWEEN 400 AND 428) OR
        (CAST(INTPLAN AS INTEGER) BETWEEN 448 AND 469) OR
        (CAST(INTPLAN AS INTEGER) BETWEEN 600 AND 639) OR
        (CAST(INTPLAN AS INTEGER) BETWEEN 720 AND 740) OR
        (CAST(INTPLAN AS INTEGER) BETWEEN 470 AND 499) OR
        (CAST(INTPLAN AS INTEGER) BETWEEN 548 AND 573)
    )
""").pl()

con.close()
gc.collect()
print(f"  SA rows: {len(sa):,}  CA rows: {len(ca):,}  FD rows: {len(fd):,}")

# ============================================================================
# STEP 5: BUILD DPBR  (union SA/CA/FD, branch & OPENIND filter, BRCHCD format)
# DATA DPBR;
#   KEEP ACCTNO CUSTNO CURBAL BRCHCD;
#   SET SA CA FD;
#   IF BRANCH IN (...selected branches...);
#   IF OPENIND NOT IN ('B','C','P');
#   BRCHCD=PUT(BRANCH,BRCHCD.);
# ============================================================================
print("\nStep 5: Building DPBR (union + branch/OPENIND filter)...")

branch_list = ",".join(str(v) for v in SELECTED_BRANCHES)

con = duckdb.connect(database=":memory:")
con.register("sa_df", sa.to_pandas())
con.register("ca_df", ca.to_pandas())
con.register("fd_df", fd.to_pandas())

dpbr_raw = con.execute(f"""
    WITH combined AS (
        SELECT ACCTNO, BRANCH, CURBAL, OPENIND FROM sa_df
        UNION ALL
        SELECT ACCTNO, BRANCH, CURBAL, OPENIND FROM ca_df
        UNION ALL
        SELECT ACCTNO, BRANCH, CURBAL, OPENIND FROM fd_df
    )
    SELECT ACCTNO, BRANCH, CURBAL
    FROM combined
    WHERE BRANCH IN ({branch_list})
      AND COALESCE(OPENIND, '') NOT IN ('B', 'C', 'P')
""").pl()

con.close()
del sa, ca, fd
gc.collect()

dpbr_raw = dpbr_raw.with_columns(
    pl.col("BRANCH")
    .map_elements(get_brchcd_format, return_dtype=pl.Utf8)
    .alias("BRCHCD")
)

print(f"  DPBR raw rows: {len(dpbr_raw):,}")

# ============================================================================
# STEP 6: MERGE WITH CISDP  (MERGE DPBR(IN=A) CISDP; BY ACCTNO; IF A)
# Left join keeping all DPBR rows regardless of CISDP match.
#
# NOTE: CISDP was deduplicated on (CUSTNO, ACCTNO), not on ACCTNO alone, so
# duplicate ACCTNO keys with different CUSTNO can still remain (e.g. joint
# accounts). SAS MERGE BY ACCTNO with duplicate keys on the CISDP side would
# multiply output rows for that ACCTNO, which is not the intended report
# semantics here. Take the first CUSTNO per ACCTNO before joining to avoid
# unintended row inflation (Polars/DuckDB joins produce a full cross-product
# on duplicate keys, unlike SAS's paired-duplicate merge behaviour).
# ============================================================================
print("\nStep 6: Merging DPBR with CISDP (customer number)...")

cisdp_unique = (
    cisdp.with_row_index("SEQ")
    .sort("SEQ")
    .unique(subset=["ACCTNO"], keep="first")
    .select(["ACCTNO", "CUSTNO"])
)

dpbr = dpbr_raw.join(cisdp_unique, on="ACCTNO", how="left")

del dpbr_raw, cisdp, cisdp_unique
gc.collect()

# ============================================================================
# STEP 7: ACCUMULATE BALANCE PER BRCHCD+CUSTNO  (SAS BY-group running total)
# PROC SORT; BY BRCHCD CUSTNO;
# DATA DPBR;
#   SET DPBR; BY BRCHCD CUSTNO;
#   IF FIRST.BRCHCD OR FIRST.CUSTNO THEN BALANCE=0;
#   BALANCE+CURBAL;
#   IF LAST.BRCHCD OR LAST.CUSTNO THEN DO;
#       DRANGE=PUT(BALANCE,DEPRAN.); ACCT=1; OUTPUT;
#   END;
#
# A running total reset at the start of every (BRCHCD, CUSTNO) group and
# emitted only at group end is equivalent to a direct SUM(CURBAL) grouped by
# (BRCHCD, CUSTNO). Missing CUSTNO values group together per BRCHCD, matching
# SAS's treatment of missing BY values.
# ============================================================================
print("\nStep 7: Aggregating balance per BRCHCD + customer...")

per_customer = (
    dpbr.group_by(["BRCHCD", "CUSTNO"])
    .agg(pl.col("CURBAL").sum().alias("BALANCE"))
)

per_customer = per_customer.with_columns(
    pl.col("BALANCE")
    .map_elements(get_deposit_range, return_dtype=pl.Utf8)
    .alias("DRANGE")
)

del dpbr
gc.collect()
print(f"  Customer-level groups: {len(per_customer):,}")

# ============================================================================
# STEP 8: PROC SUMMARY NWAY  (sum BALANCE, count customers per BRCHCD+DRANGE)
# PROC SUMMARY DATA=DPBR NWAY; CLASS BRCHCD DRANGE; VAR BALANCE ACCT;
# OUTPUT OUT=DPBR (DROP=_TYPE_ _FREQ_) SUM=;
# PROC SORT; BY BRCHCD DRANGE;
# ============================================================================
print("\nStep 8: Summarising by BRCHCD + DRANGE...")

summary = (
    per_customer.group_by(["BRCHCD", "DRANGE"])
    .agg(
        pl.col("BALANCE").sum().alias("BALANCE"),
        pl.len().alias("ACCT"),
    )
    .sort(["BRCHCD", "DRANGE"])
)

del per_customer
gc.collect()
print(f"  Summary rows: {len(summary):,}")

# ============================================================================
# STEP 9: GENERATE REPORT  (ASA carriage control, LRECL=133, PAGE_SIZE=60)
# PROC REPORT DATA=DPBR NOWD HEADSKIP HEADLINE SPLIT='*';
# COLUMN BRCHCD DRANGE ACCT BALANCE;
# BREAK AFTER BRCHCD/; COMPUTE AFTER BRCHCD; ... ENDCOMP;
# ============================================================================
print("\nStep 9: Generating report...")

# Column layout derived from the explicit @ positions used in the COMPUTE
# block (@010 TOTAL label, @045 ACCT.SUM, @058 BALANCE.SUM):
#   BRCHCD  : cols  1- 5  (width 5,  left-justified)
#   DRANGE  : cols  8-42  (width 35, left-justified)
#   ACCT    : cols 45-52  (width 8,  right-justified)
#   BALANCE : cols 58-75  (width 18, right-justified)
COL_BRCHCD  = (0, 5)
COL_DRANGE  = (7, 35)
COL_ACCT    = (44, 8)
COL_BALANCE = (57, 18)


def _fmt_comma(value, width: int, decimals: int = 0) -> str:
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    s = f"{v:,.{decimals}f}" if decimals > 0 else f"{int(round(v)):,}"
    return s.rjust(width)[:width]


def _place(buf: list, start: int, width: int, text: str, right_just: bool = False) -> None:
    text = text[:width]
    text = text.rjust(width) if right_just else text.ljust(width)
    buf[start:start + width] = list(text)


def _new_buffer() -> list:
    return [" "] * CONTENT_WIDTH


def _build_header_lines() -> list[str]:
    """Titles + column headers + HEADLINE + HEADSKIP (7 content lines)."""
    lines = []

    lines.append(("1", "PUBLIC BANK BERHAD"))
    lines.append((" ", "DEVELOPMENT OF WEALTH MANAGEMENT CENTRES "))
    lines.append((" ", f"DEPOSIT RANGE PROFILE BY SELECTED BRANCHES- {RDATE}"))
    lines.append((" ", ""))  # blank line before column headers

    hdr_buf = _new_buffer()
    _place(hdr_buf, *COL_BRCHCD, "BRH  CODE")
    _place(hdr_buf, *COL_DRANGE, "DEPOSIT RANGE ")
    _place(hdr_buf, COL_ACCT[0], COL_ACCT[1], "NO. OF  CUSTOMER", right_just=True)
    _place(hdr_buf, COL_BALANCE[0], COL_BALANCE[1], "OUTSTANDING AMOUNT", right_just=True)
    lines.append((" ", "".join(hdr_buf).rstrip()))

    lines.append((" ", "-" * 75))   # HEADLINE
    lines.append((" ", ""))         # HEADSKIP

    return [f"{asa}{content}".ljust(CONTENT_WIDTH + 1) for asa, content in lines]


def _build_detail_line(row: dict) -> str:
    buf = _new_buffer()
    _place(buf, *COL_BRCHCD, str(row["BRCHCD"] or ""))
    _place(buf, *COL_DRANGE, str(row["DRANGE"] or ""))
    _place(buf, COL_ACCT[0], COL_ACCT[1], _fmt_comma(row["ACCT"], COL_ACCT[1], 0), right_just=True)
    _place(buf, COL_BALANCE[0], COL_BALANCE[1], _fmt_comma(row["BALANCE"], COL_BALANCE[1], 2), right_just=True)
    return " " + "".join(buf)


def _build_break_lines(total_acct, total_balance) -> list[str]:
    """BREAK AFTER BRCHCD / COMPUTE AFTER BRCHCD block."""
    lines = []

    buf1 = _new_buffer()
    _place(buf1, 9, 65, "-" * 65)
    lines.append(" " + "".join(buf1))

    buf2 = _new_buffer()
    _place(buf2, 9, 9, "TOTAL    ")
    _place(buf2, 44, 10, _fmt_comma(total_acct, 10, 0), right_just=True)
    _place(buf2, 57, 17, _fmt_comma(total_balance, 17, 2), right_just=True)
    lines.append(" " + "".join(buf2))

    buf3 = _new_buffer()
    _place(buf3, 9, 65, "-" * 65)
    lines.append(" " + "".join(buf3))

    return lines


output_lines: list[str] = []
lines_on_page = 0
current_brh = None
group_acct_total = 0
group_balance_total = 0.0

rows = summary.to_dicts()

for i, row in enumerate(rows):
    brh = row["BRCHCD"]

    new_group = brh != current_brh
    if new_group and current_brh is not None:
        # Emit BREAK AFTER BRCHCD subtotal for the group just finished
        break_lines = _build_break_lines(group_acct_total, group_balance_total)
        if lines_on_page + len(break_lines) > PAGE_SIZE:
            output_lines.extend(_build_header_lines())
            lines_on_page = HEADER_LINES
        output_lines.extend(break_lines)
        lines_on_page += len(break_lines)
        group_acct_total = 0
        group_balance_total = 0.0

    if new_group or not output_lines:
        output_lines.extend(_build_header_lines())
        lines_on_page = HEADER_LINES
        current_brh = brh
    elif lines_on_page >= PAGE_SIZE:
        output_lines.extend(_build_header_lines())
        lines_on_page = HEADER_LINES

    output_lines.append(_build_detail_line(row))
    lines_on_page += 1

    group_acct_total += row["ACCT"] or 0
    group_balance_total += row["BALANCE"] or 0.0

    # Final row of the entire dataset: emit closing subtotal
    if i == len(rows) - 1:
        break_lines = _build_break_lines(group_acct_total, group_balance_total)
        if lines_on_page + len(break_lines) > PAGE_SIZE:
            output_lines.extend(_build_header_lines())
            lines_on_page = HEADER_LINES
        output_lines.extend(break_lines)
        lines_on_page += len(break_lines)

# ============================================================================
# WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(ln.ljust(CONTENT_WIDTH + 1) + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")

del summary
gc.collect()

print("\nEIBMDPBR complete.")
