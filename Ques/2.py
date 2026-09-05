#!/usr/bin/env python3
"""
Program : EIBMNPL1
Purpose : Total Overdue Loans report (Loans / HP / O/D / Loans & O/D),
          plus the "AGEING OF ALL OVERDUE OD & TERM LOANS" BY-BRANCH table.

Dependency:
    %INC PGM(PBBLNFMT,PBBELF);
      - PBBELF  : format_brchcd() (PUT(BRANCH,BRCHCD.)) is used repeatedly
        in the SAS body -> imported and used below.
      - PBBLNFMT: included at session level but NO PUT(x,<PBBLNFMT format>.)
        call appears anywhere in EIBMNPL1/EIBMNPL2's body, so per project
        convention this is a comment-only reference, never imported live.
        # from PBBLNFMT import ...   (NOT USED -- no live format call)

    %INC PGM1(EIBMNPL2);  -- continuation program, kept in its own file.
        EIBMNPL2.py imports shared state (paths, report-date values, helper
        functions) FROM this module. The Parquet cache paths specifically
        (LOAN_CACHE / OVERDFT_CACHE) are module-level globals that only
        become valid AFTER main() has converted the source files -- see
        the CACHING STRATEGY note below.

============================================================================
CACHING STRATEGY: CONVERT -> USE -> DELETE  (no persistent Parquet cache)
============================================================================
Earlier revisions of this program family (see EIIMRM01.py / EIBDLN1M.py)
kept a persistent Parquet cache under CACHE_DIR, freshness-checked by
comparing file mtimes, so a re-run with an unchanged source file could skip
re-conversion entirely.

Per explicit instruction for THIS program, that persistent cache is removed
in favour of a "convert -> use -> delete" pattern to avoid unbounded disk
usage on svdwh004:
  1. Each .sas7bdat input is converted ONCE per run into a Parquet file
     under a per-run temporary directory (tempfile.mkdtemp()).
  2. That temp Parquet file is queried via DuckDB as many times as needed
     within the run (both PBB and PIBB entity passes share the same
     converted file -- ENTITY_CD is filtered per-query, not per-file).
  3. On completion (success OR failure, via try/finally) the temp Parquet
     files and their directory are deleted, leaving no residual cache on
     disk between runs.

Trade-off (documented, not silently accepted): every run now re-pays the
full pd.read_sas() conversion cost for both inputs, even if the underlying
.sas7bdat files have not changed since the previous run. This is the
correct trade for this program given the stated disk-space constraint;
programs that run very frequently against slowly-changing sources would
be better served by the persistent-cache pattern instead.

============================================================================
PHYSICAL INPUT DATASETS
============================================================================
1. LOAN dataset   (JCL //BNM DD DSN=SAP.<PBB|PIBB>.SASDATA, member
   LOAN&REPTMON&NOWK). NOWK is HARD-CODED to '4' by
   CALL SYMPUT('NOWK',PUT('4',$1.)) in the original REPTDATE step, so the
   member name is fully derivable from REPTMON alone -- input_date.py's
   get_latest_file() is therefore NOT used (per project convention).
   File : INPUT_LOAN_FILE -> loan<REPTMON>4_d19.sas7bdat
   Cols used : ACCTNO, BRANCH, BALANCE, PRODUCT, ACCTYPE, NOTENO, BLDATE,
               RISKRTE, APPRLIMT, NAME, CUSTCD, SECTORCD, COLLCD, STATECD,
               SECURE, OLDNOTEDAYARR, ENTITY_CD.
   The original JCL runs this program once for PBB and once for PIBB using
   separate SASDATA librefs; here both partitions live in one physical
   LOAN dataset distinguished by ENTITY_CD ('PBB' / 'PIBB'), per project
   instruction, and both are produced in a single run (loop over ENTITY)
   sharing the SAME converted temp Parquet file.

2. OD.OVERDFT dataset (JCL //OD DD DSN=SAP.<PBB|PIBB>.MNILIMT(0), member
   OVERDFT). Fixed catalogued name (no date token).
   File : INPUT_OVERDFT_FILE -> overdft_d19.sas7bdat
   Cols used : ACCTNO, EXCESSDT, TODDATE, RISKCODE, ENTITY_CD.

============================================================================
OUTPUTS (per ENTITY in {PBB, PIBB})
============================================================================
A. TEMP  DD DSN=SAP.<ENTITY>.ODTLLIST.TEXT, RECFM=FB  (NO ASA control byte)
   -> <ENTITY>_ODTLLIST_TEXT.txt
   Plain BRANCH x RISKRATE crosstab of BALANCE then RISKBAL (N and SUM),
   FORMCHAR=' ' (no box-drawing chars at all), LINESIZE=256.

B. ODTLLIST DD DSN=SAP.<ENTITY>.ODTLLIST.COLD, RECFM=FBA, LRECL=136
   (ASA carriage control) -> <ENTITY>_ODTLLIST_COLD.txt
   This file is opened here (EIBMNPL1's ageing table) and then CONTINUED
   (append, no PRINTTO...NEW) by EIBMNPL2's PROC PRINT detail listings.
   LINESIZE=132, PAGESIZE=60 (not specified in source -> project default).

The %TBLS macro's 4x2 PROC TABULATE displays (detail risk-rating breakdown
and ID1/ID2/ID3-grouped summary, run for LOAN1/LOAN2/LOAN3/LOAN4) are never
redirected via PROC PRINTTO in the original SAS (that redirection happens
only AFTER %TBLS runs) and SASLIST DD is commented out in the JCL, so none
of those 8 displays are captured to any catalogued dataset in the original
job. They are still computed here for logical completeness and printed to
the terminal (equivalent to the uncaptured default SAS listing), but are
NOT written to an output file, matching the original job's behaviour.
"""

import gc
import shutil
import tempfile
from pathlib import Path
from datetime import date

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

INPUT_LOAN_DIR    = STG_DIR / "sasdata"
INPUT_OVERDFT_DIR = STG_DIR / "sasdata"

INPUT_OVERDFT_FILE = INPUT_OVERDFT_DIR / "overdft_d19.sas7bdat"

OUTPUT_DIR = BASE_DIR / "output" / "EIBMNPL"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000
PAGE_SIZE  = 60     # lines per page (not specified in SAS -> default)
LINESIZE_TEMP     = 256
LINESIZE_ODTLLIST = 132

ENTITIES = ("PBB", "PIBB")

# Module-level cache-path placeholders. These are set to real temp Parquet
# paths by _convert_inputs() at the start of main(), and cleared back to
# None by _cleanup_inputs() in the finally block. EIBMNPL2.py imports these
# names directly ("from EIBMNPL1 import LOAN_CACHE, OVERDFT_CACHE"); the
# deferred "import EIBMNPL2" inside main() (below) only happens AFTER
# _convert_inputs() has run, so EIBMNPL2 always sees the real paths, never
# the None placeholders.
LOAN_CACHE: Path = None
OVERDFT_CACHE: Path = None

# ============================================================================
# STEP 1: REPORT DATE (derive from REPTDATE.py -- no reptdate.parquet)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%y")
REPTDATE = reptdate_values.reptdate
REPTMON  = reptdate_values.reptmon           # PUT(MONTH(REPTDATE),Z2.)
RDATE    = REPTDATE.strftime("%d/%m/%y")     # PUT(REPTDATE,DDMMYY8.)

# NOWK is hard-coded in the original REPTDATE DATA step:
#   CALL SYMPUT('NOWK',PUT('4',$1.));
# i.e. NOWK is ALWAYS '4' regardless of the actual report date -- preserved
# verbatim (not corrected to the range-based NOWK used elsewhere).
NOWK = "4"

INPUT_LOAN_FILE = INPUT_LOAN_DIR / f"loan{REPTMON}{NOWK}_d19.sas7bdat"

print(f"  RDATE        : {RDATE}")
print(f"  REPTMON/NOWK : {REPTMON}/{NOWK}")
print(f"  LOAN input   : {INPUT_LOAN_FILE.name}")
print(f"  OVERDFT input: {INPUT_OVERDFT_FILE.name}")

TBL_LABELS = {1: "(LOANS)", 2: "(HP)", 3: "(O/D)", 4: "(LOANS & O/D)"}

# ============================================================================
# LOCAL PROC FORMAT EQUIVALENTS
# ============================================================================
_RISK_LABELS = {
    0: "(0-7 DAYS)", 1: "(8-30 DAYS)", 2: "(31-59 DAYS)", 3: "(60-89 DAYS)",
    4: "(90-121 DAYS)", 5: "(122-151 DAYS)", 6: "(152-182 DAYS)",
    7: "(183-213 DAYS)", 8: "(214-243 DAYS)", 9: "(244-273 DAYS)",
    10: "(274-364 DAYS)", 11: "(365 - 547 DAYS)", 12: "(548 - 729 DAYS)",
    13: "(730 - 1094 DAYS)", 14: "(1095 & ABOVE DAYS)",
}


def format_risk(code) -> str:
    """VALUE RISK. -- only values 0-14 are defined."""
    return _RISK_LABELS.get(code, "")


_ID1F = {1: "OVERDUE (IMTH - BAD) (30 DAYS & ABOVE)"}
_ID2F = {1: "DELINQUENT (SS1- BAD) (90 DAYS & ABOVE)"}
_ID3F = {1: "NON PERFORMING LOANS (SS2- BAD) (183 DAYS & ABOVE)"}


def format_id1f(code) -> str:
    return _ID1F.get(code, "")


def format_id2f(code) -> str:
    return _ID2F.get(code, "")


def format_id3f(code) -> str:
    return _ID3F.get(code, "")


# ============================================================================
# RISKRATE CASCADE  (shared SELECT block used for LOAN1/LOAN2 and LOAN3;
# also imported by EIBMNPL2 for its own O/D DAYS derivation)
# ============================================================================
def risk_rate_from_days(days) -> int:
    """SELECT; WHEN(DAYS>1094) 14; ... OTHERWISE 0; END;
    SAS numeric missing sorts as -infinity, so a missing DAYS falls through
    every WHEN and lands on OTHERWISE (0) -- replicated by treating None as
    smaller than any comparison threshold."""
    if days is None:
        return 0
    if days > 1094:
        return 14
    if days > 729:
        return 13
    if days > 547:
        return 12
    if days > 364:
        return 11
    if days > 273:
        return 10
    if days > 243:
        return 9
    if days > 213:
        return 8
    if days > 182:
        return 7
    if days > 151:
        return 6
    if days > 121:
        return 5
    if days > 89:
        return 4
    if days > 59:
        return 3
    if days > 30:
        return 2
    if days > 7:
        return 1
    return 0


# ============================================================================
# EXCESSDT / TODDATE / BLDATE PARSING  (LOAN3 here; also used by EIBMNPL2's
# LOAN2/O-D detail build, hence exposed as public module-level functions)
# ============================================================================
def _z11(value) -> str:
    """PUT(value,Z11.) -- zero-padded 11-digit numeric string."""
    return f"{int(value):011d}"


def parse_excessdt(excessdt) -> date:
    """EXCMONTH=SUBSTR(...,1,2); EXCDAY=SUBSTR(...,3,2); EXCYEAR=SUBSTR(...,5,4);
    EXCDATE=MDY(EXCMONTH,EXCDAY,EXCYEAR). Offsets preserved verbatim from
    the SAS source (against the Z11.-padded string) even though this reuses
    leading zero-pad digits -- this is a legacy quirk, not corrected here."""
    s = _z11(excessdt)
    return date(int(s[4:8]), int(s[0:2]), int(s[2:4]))


def parse_toddate(toddate) -> date:
    """TODDAY=INPUT(SUBSTR(...,3,2),2.); TODMONTH=INPUT(SUBSTR(...,1,2),2.);
    TODYEAR=INPUT(SUBSTR(...,5,4),4.); TODDT=MDY(TODMONTH,TODDAY,TODYEAR)."""
    s = _z11(toddate)
    return date(int(s[4:8]), int(s[0:2]), int(s[2:4]))


def _bldate_from_z11_mmddyy8(raw_value) -> date:
    """BLDATE=INPUT(SUBSTR(PUT(raw,Z11.),1,8),MMDDYY8.) -- parses the first
    8 chars of the Z11.-padded 11-digit string as MMDDYY (2-digit year,
    YEARCUTOFF=1950). Preserved exactly as written in the SAS source."""
    s = _z11(raw_value)[0:8]
    mm, dd, yy = int(s[0:2]), int(s[2:4]), int(s[4:6])
    year = 1900 + yy if yy >= 50 else 2000 + yy
    return date(year, mm, dd)


def compute_bldate(excessdt, toddate):
    """IF EXCESSDT NE 0 AND TODDATE NE 0 THEN DO;
         IF EXCDATE<=TODDT THEN BLDATE=...(EXCESSDT);
         IF EXCDATE> TODDT THEN BLDATE=...(TODDATE);
       END;
       ELSE IF EXCESSDT>0 THEN BLDATE=...(EXCESSDT);
       ELSE IF TODDATE>0  THEN BLDATE=...(TODDATE);"""
    bldate = None
    if excessdt != 0 and toddate != 0:
        excdate = parse_excessdt(excessdt)
        toddt = parse_toddate(toddate)
        if excdate <= toddt:
            bldate = _bldate_from_z11_mmddyy8(excessdt)
        if excdate > toddt:
            bldate = _bldate_from_z11_mmddyy8(toddate)
    elif excessdt is not None and excessdt > 0:
        bldate = _bldate_from_z11_mmddyy8(excessdt)
    elif toddate is not None and toddate > 0:
        bldate = _bldate_from_z11_mmddyy8(toddate)
    return bldate


# ============================================================================
# CONVERT -> USE -> DELETE : one-shot Parquet conversion, no persistent cache
# ============================================================================
def _sas_to_parquet(sas_path: Path, parquet_path: Path, tag: str) -> None:
    """Streams .sas7bdat -> Parquet in chunks (same conversion logic as the
    persistent-cache pattern in EIIMRM01.py), but is ALWAYS executed --
    there is no mtime/freshness check here, since no prior-run artifact is
    ever kept around to compare against."""
    print(f"  [{tag}] Converting {sas_path.name} -> {parquet_path.name} "
          f"(temp, convert-use-delete) ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if schema is None:
            fields = []
            for col, dtype in chunk.dtypes.items():
                if dtype == "object":
                    pa_type = pa.string()
                elif pd.api.types.is_integer_dtype(dtype):
                    pa_type = pa.int64()
                elif pd.api.types.is_float_dtype(dtype):
                    pa_type = pa.float64()
                else:
                    pa_type = pa.from_numpy_dtype(dtype)
                fields.append(pa.field(col, pa_type))
            schema = pa.schema(fields)
            writer = pq.ParquetWriter(parquet_path, schema, compression="snappy")

        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows converted (temporary).")


def _convert_inputs(temp_dir: Path) -> None:
    """Converts both physical inputs to Parquet files inside the per-run
    temp_dir and stores their paths in the module-level LOAN_CACHE /
    OVERDFT_CACHE globals so every helper function below (and EIBMNPL2,
    once imported) can query them via DuckDB for the remainder of the run."""
    global LOAN_CACHE, OVERDFT_CACHE

    print("\nStep 2: Converting input SAS datasets to temporary Parquet "
          "(convert -> use -> delete; no persistent cache)...")

    LOAN_CACHE = temp_dir / f"{INPUT_LOAN_FILE.stem}.parquet"
    _sas_to_parquet(INPUT_LOAN_FILE, LOAN_CACHE, "LOAN")

    OVERDFT_CACHE = temp_dir / f"{INPUT_OVERDFT_FILE.stem}.parquet"
    _sas_to_parquet(INPUT_OVERDFT_FILE, OVERDFT_CACHE, "OVERDFT")


def _cleanup_inputs(temp_dir: Path) -> None:
    """Deletes the temporary Parquet files (and the temp directory itself)
    once every entity pass has finished reading from them, and resets the
    module-level cache-path globals so no stale Path lingers for anything
    that might run afterwards in the same process."""
    global LOAN_CACHE, OVERDFT_CACHE

    print("\nCleanup: Deleting temporary Parquet files (convert-use-delete)...")
    shutil.rmtree(temp_dir, ignore_errors=True)
    LOAN_CACHE = None
    OVERDFT_CACHE = None
    print(f"  Removed temp directory: {temp_dir}")


# ============================================================================
# ASA / PAGINATION HELPERS  (also used by EIBMNPL2 for the same COLD output)
# ============================================================================
class AsaWriter:
    """Accumulates ASA-controlled lines with PAGESIZE=60 pagination.
    Control byte is fused as the first character of each line (per project
    convention -- '1' for new page is fused onto the first title line, not
    emitted standalone)."""

    def __init__(self, page_size: int = PAGE_SIZE):
        self.page_size = page_size
        self.lines: list = []
        self.lines_on_page = 0

    def new_page(self, title_lines: list) -> None:
        if not title_lines:
            self.lines.append("1")
            self.lines_on_page = 1
            return
        first, *rest = title_lines
        self.lines.append("1" + first)
        for t in rest:
            self.lines.append(" " + t)
        self.lines_on_page = len(title_lines)

    def add(self, text: str) -> None:
        self.lines.append(" " + text)
        self.lines_on_page += 1

    def ensure_space(self, needed: int, title_lines: list) -> None:
        if self.lines_on_page + needed > self.page_size:
            self.new_page(title_lines)

    def write(self, path: Path) -> None:
        with open(path, "w", encoding="latin1") as fh:
            for ln in self.lines:
                fh.write(ln + "\n")


def comma(value, width: int, decimals: int = 0) -> str:
    """COMMAw.d -- MISSING=0 semantics (a missing numeric prints as 0.)."""
    v = 0.0 if value is None else float(value)
    s = f"{v:,.{decimals}f}"
    if len(s) > width:
        s = f"{v:.{decimals}f}"
    if len(s) > width:
        s = s[-width:]
    return s.rjust(width)


def center(text: str, width: int) -> str:
    text = text[:width]
    pad = width - len(text)
    left = pad // 2
    return " " * left + text + " " * (pad - left)


# ============================================================================
# STEP 3: BUILD LOAN1 / LOAN2  ("DATA LOAN1 LOAN2" step)
# ============================================================================
def _build_loan1_loan2(entity: str):
    print(f"\nStep 3 [{entity}]: Building LOAN1 / LOAN2 (LN facilities)...")
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(BRANCH   AS INTEGER) AS BRANCH,
            CAST(BALANCE  AS DOUBLE)  AS BALANCE,
            CAST(PRODUCT  AS INTEGER) AS PRODUCT,
            CAST(ACCTYPE  AS VARCHAR) AS ACCTYPE,
            CAST(NOTENO   AS INTEGER) AS NOTENO,
            CAST(BLDATE   AS DATE)    AS BLDATE,
            CAST(RISKRTE  AS INTEGER) AS RISKRTE
        FROM read_parquet('{LOAN_CACHE.as_posix()}')
        WHERE ENTITY_CD = '{entity}'
          AND NOTENO < 90000
          AND ACCTYPE = 'LN'
          AND BRANCH IS NOT NULL
          AND BALANCE >= 1.00
          AND PRODUCT NOT IN (517, 500)
    """).pl()
    con.close()

    loan1_rows, loan2_rows = [], []
    for r in raw.iter_rows(named=True):
        riskrte = r["RISKRTE"]
        riskbal = r["BALANCE"] if riskrte in (1, 2, 3, 4) else None

        bldate = r["BLDATE"]
        days = (REPTDATE - bldate).days if bldate is not None else None

        riskrate = risk_rate_from_days(days)
        branch = format_brchcd(r["BRANCH"])

        row = {
            "BRANCH": branch, "BALANCE": r["BALANCE"], "RISKRATE": riskrate,
            "RISKRTE": riskrte, "RISKBAL": riskbal, "ACCTNO": r["ACCTNO"],
            "DAYS": days,
        }
        if r["PRODUCT"] in (380, 381):
            loan2_rows.append(row)
        else:
            loan1_rows.append(row)

    loan1_rows.sort(key=lambda x: (x["BRANCH"], x["RISKRATE"]))
    loan2_rows.sort(key=lambda x: (x["BRANCH"], x["RISKRATE"]))
    print(f"  LOAN1 rows: {len(loan1_rows):,}   LOAN2 rows: {len(loan2_rows):,}")
    return loan1_rows, loan2_rows


def _pad_all_riskrates(rows: list) -> list:
    """DATA DUMMY; ... IF FIRST.BRANCH THEN DO RISKRATE=0 TO 14; OUTPUT; END;
    DATA LOANn; MERGE LOANn DUMMY; BY BRANCH RISKRATE;
    Ensures every BRANCH x RISKRATE(0-14) combination exists so the ageing
    crosstab shows a (zero) cell rather than an absent one."""
    existing = {(r["BRANCH"], r["RISKRATE"]) for r in rows}
    branches = sorted({r["BRANCH"] for r in rows})
    padded = list(rows)
    for branch in branches:
        for rr in range(0, 15):
            if (branch, rr) not in existing:
                padded.append({
                    "BRANCH": branch, "BALANCE": None, "RISKRATE": rr,
                    "RISKRTE": None, "RISKBAL": None, "ACCTNO": None,
                    "DAYS": None,
                })
    padded.sort(key=lambda x: (x["BRANCH"], x["RISKRATE"]))
    return padded


# ============================================================================
# STEP 4: BUILD LOAN3  (O/D loans)
# ============================================================================
def _build_loan3(entity: str) -> list:
    print(f"\nStep 4 [{entity}]: Building LOAN3 (O/D facilities)...")
    con = duckdb.connect(database=":memory:")
    od_base = con.execute(f"""
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(BRANCH   AS INTEGER) AS BRANCH,
            CAST(BALANCE  AS DOUBLE)  AS BALANCE,
            CAST(PRODUCT  AS INTEGER) AS PRODUCT
        FROM read_parquet('{LOAN_CACHE.as_posix()}')
        WHERE ENTITY_CD = '{entity}'
          AND ACCTYPE = 'OD'
          AND (APPRLIMT >= 0 OR BALANCE < 0)
          AND (ACCTNO <= 3900000000 OR ACCTNO > 3999999999)
        ORDER BY ACCTNO
    """).pl()

    od_ref = con.execute(f"""
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(EXCESSDT AS BIGINT)  AS EXCESSDT,
            CAST(TODDATE  AS BIGINT)  AS TODDATE,
            CAST(RISKCODE AS VARCHAR) AS RISKCODE
        FROM read_parquet('{OVERDFT_CACHE.as_posix()}')
        WHERE ENTITY_CD = '{entity}'
          AND (EXCESSDT > 0 OR TODDATE > 0)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) = 1
    """).pl()
    con.close()

    od_ref_map = {r["ACCTNO"]: r for r in od_ref.iter_rows(named=True)}

    loan3_rows = []
    for r in od_base.iter_rows(named=True):
        if r["BRANCH"] is None or r["PRODUCT"] in (517, 500):
            continue
        od = od_ref_map.get(r["ACCTNO"])
        if od is None:
            continue   # MERGE ... BY ACCTNO; IF AA -> only matched rows kept

        riskcode = od["RISKCODE"]
        try:
            riskrte = int(riskcode)
        except (TypeError, ValueError):
            riskrte = None
        riskbal = r["BALANCE"] if riskcode in ("1", "2", "3", "4") else None

        bldate = compute_bldate(od["EXCESSDT"], od["TODDATE"])
        days = (REPTDATE - bldate).days + 1 if bldate is not None else None
        riskrate = risk_rate_from_days(days)

        loan3_rows.append({
            "BRANCH": format_brchcd(r["BRANCH"]), "BALANCE": r["BALANCE"],
            "RISKRATE": riskrate, "RISKRTE": riskrte, "RISKCODE": riskcode,
            "RISKBAL": riskbal, "ACCTNO": r["ACCTNO"], "DAYS": days,
        })

    loan3_rows.sort(key=lambda x: (x["BRANCH"], x["RISKRATE"]))
    print(f"  LOAN3 rows: {len(loan3_rows):,}")
    return loan3_rows


# ============================================================================
# STEP 5: %TBLS MACRO EQUIVALENT (aggregation only -- displays not captured
# to any output file in the original job; printed to terminal for parity).
# ============================================================================
def _group_counts(rows: list, where):
    """PROC SUMMARY NWAY; CLASS BRANCH; VAR BALANCE; WHERE <where>;
    OUTPUT OUT=... (RENAME=(_FREQ_=..NO BALANCE=..AMT)) SUM=;"""
    agg: dict = {}
    for r in rows:
        if r["BALANCE"] is None or not where(r):
            continue
        b = r["BRANCH"]
        no, amt = agg.get(b, (0, 0.0))
        agg[b] = (no + 1, amt + r["BALANCE"])
    return agg


def _run_tbls_iteration(i: int, rows: list) -> None:
    """One %TBLS %DO loop iteration for LOAN&I (I=1..4)."""
    label = TBL_LABELS[i]
    print(f"\n  --- %TBLS iteration {i} {label} (console only, not captured "
          f"to any catalogued output in the original job) ---")

    tla  = _group_counts(rows, lambda r: True)
    sum1 = _group_counts(rows, lambda r: (r["RISKRATE"] or 0) > 0)
    sum2 = _group_counts(rows, lambda r: (r["RISKRATE"] or 0) > 3)
    sum3 = _group_counts(rows, lambda r: (r["RISKRATE"] or 0) > 6)

    print(f"  TITLE1: TOTAL OVERDUE LOANS AS AT {RDATE} {label}")
    print(f"  TITLE2: FREQUENCY : MONTHLY")
    print(f"  {'BRANCH':<8}{'TLNO':>8}{'TLAMT':>16}{'S1NO':>8}{'S1AMT':>16}"
          f"{'S2NO':>8}{'S2AMT':>16}{'S3NO':>8}{'S3AMT':>16}")
    for b in sorted(tla):
        tlno, tlamt = tla.get(b, (0, 0.0))
        s1no, s1amt = sum1.get(b, (0, 0.0))
        s2no, s2amt = sum2.get(b, (0, 0.0))
        s3no, s3amt = sum3.get(b, (0, 0.0))
        print(f"  {b:<8}{tlno:>8}{tlamt:>16,.2f}{s1no:>8}{s1amt:>16,.2f}"
              f"{s2no:>8}{s2amt:>16,.2f}{s3no:>8}{s3amt:>16,.2f}")

    # ID1/ID2/ID3 grouped summary (RETAIN ID1 ID2 ID3 1) -- ID columns are
    # always 1, so the format labels are constant for every branch row.
    print(f"  {'BRANCH':<8}{'ID1':<42}{'ID2':<42}{'ID3':<42}")
    for b in sorted(tla):
        print(f"  {b:<8}{format_id1f(1):<42}{format_id2f(1):<42}{format_id3f(1):<42}")


def _run_tbls(loan1, loan2, loan3, loan4) -> None:
    print("\nStep 5: %TBLS macro (LOAN1/LOAN2/LOAN3/LOAN4 summaries)...")
    for i, rows in ((1, loan1), (2, loan2), (3, loan3), (4, loan4)):
        _run_tbls_iteration(i, rows)


# ============================================================================
# STEP 6: TEMP OUTPUT (ODTLLIST.TEXT) -- FORMCHAR=' ', no ASA, no box chars
# ============================================================================
def _write_temp_output(loan4_padded: list, out_path: Path) -> None:
    """PROC PRINTTO PRINT=TEMP NEW; OPTION LINESIZE=256;
    Two PROC TABULATE calls (BALANCE, then RISKBAL), FORMCHAR blank,
    NOSEPS, TITLE1/TITLE2 blank, RTS=5 CONDENSE.
    RECFM=FB -> no ASA control byte; plain fixed-width numeric columns."""
    print(f"\nStep 6: Writing TEMP (ODTLLIST.TEXT, no ASA) -> {out_path.name}")

    branches = sorted({r["BRANCH"] for r in loan4_padded})
    riskrates = list(range(0, 15))

    def _crosstab(field: str) -> dict:
        agg: dict = {}
        for r in loan4_padded:
            val = r[field]
            key = (r["BRANCH"], r["RISKRATE"])
            n, s = agg.get(key, (0, None))
            if val is not None:
                n += 1
                s = (s or 0.0) + val
            agg[key] = (n, s)
        return agg

    lines = []
    for field, width_n, width_s in (("BALANCE", 5, 12), ("RISKBAL", 5, 12)):
        cross = _crosstab(field)
        lines.append("")  # TITLE1/TITLE2 blank
        lines.append("")
        for b in branches:
            parts = [b.ljust(5)]
            for rr in riskrates:
                n, s = cross.get((b, rr), (0, None))
                parts.append(str(n).rjust(width_n))
                parts.append(comma(s, width_s, 0))
            lines.append(" ".join(parts))

    with open(out_path, "w", encoding="latin1") as fh:
        for ln in lines:
            fh.write(ln + "\n")


# ============================================================================
# STEP 7: ODTLLIST OUTPUT (ODTLLIST.COLD) -- ASA control, LRECL=136
# ============================================================================
def _ageing_title_block() -> list:
    return ["AGEING OF ALL OVERDUE OD & TERM LOANS", f"AS AT {RDATE}"]


def _render_ageing_table(asa: AsaWriter, loan4_padded: list) -> None:
    """PROC TABULATE DATA=LOAN4 MISSING; FORMAT RISKRATE RISK.; BY BRANCH;
    CLASS BRANCH RISKRATE; VAR BALANCE RISKBAL;
    TABLE RISKRATE=' ' ALL='TOTAL', (BALANCE=... RISKBAL=...)*(N SUM)
    / BOX=' ' RTS=30 CONDENSE;
    BY BRANCH -> one table (new page) per branch, columns: BALANCE(N,SUM),
    RISKBAL(N,SUM)."""
    label_w = 30
    branches = sorted({r["BRANCH"] for r in loan4_padded})
    by_branch: dict = {}
    for r in loan4_padded:
        by_branch.setdefault(r["BRANCH"], []).append(r)

    header_line1 = (" " * label_w + "|" + center("O/S LOANS IN ARREARS (RMM)", 27) +
                     "|" + center("O/S LOANS CLASSIFIED AS NPL(RMM) 2,3,4", 27))
    header_line2 = (" " * label_w + "|" + center("NO.", 8) + center("AMOUNT", 19) +
                     "|" + center("NO.", 8) + center("AMOUNT", 19))

    for branch in branches:
        rows = by_branch[branch]
        by_rr = {}
        for r in rows:
            key = r["RISKRATE"]
            n_bal, s_bal, n_rb, s_rb = by_rr.get(key, (0, 0.0, 0, 0.0))
            if r["BALANCE"] is not None:
                n_bal += 1
                s_bal += r["BALANCE"]
            if r["RISKBAL"] is not None:
                n_rb += 1
                s_rb += r["RISKBAL"]
            by_rr[key] = (n_bal, s_bal, n_rb, s_rb)

        title_lines = _ageing_title_block()
        asa.new_page(title_lines)
        asa.add(header_line1)
        asa.add(header_line2)

        tot_n_bal = tot_s_bal = tot_n_rb = tot_s_rb = 0
        for rr in range(0, 15):
            n_bal, s_bal, n_rb, s_rb = by_rr.get(rr, (0, 0.0, 0, 0.0))
            tot_n_bal += n_bal
            tot_s_bal += s_bal
            tot_n_rb += n_rb
            tot_s_rb += s_rb
            label = format_risk(rr).ljust(label_w)[:label_w]
            asa.ensure_space(1, title_lines)
            asa.add(f"{label}|{comma(n_bal, 6)}  {comma(s_bal, 18, 2)}"
                    f"|{comma(n_rb, 6)}  {comma(s_rb, 18, 2)}")

        asa.ensure_space(1, title_lines)
        asa.add(f"{'TOTAL'.ljust(label_w)}|{comma(tot_n_bal, 6)}  {comma(tot_s_bal, 18, 2)}"
                f"|{comma(tot_n_rb, 6)}  {comma(tot_s_rb, 18, 2)}")


# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    # Per-run scratch directory for the "convert -> use -> delete" pattern.
    # Created fresh every run, guaranteed removed in the finally block below
    # regardless of success or failure, so no Parquet residue accumulates.
    temp_dir = Path(tempfile.mkdtemp(prefix="EIBMNPL_"))
    print(f"\nUsing temporary scratch directory: {temp_dir}")

    try:
        _convert_inputs(temp_dir)

        # Deferred import: EIBMNPL2 imports LOAN_CACHE / OVERDFT_CACHE FROM
        # this module. Deferring the import to here (AFTER _convert_inputs
        # has set those globals to real temp Parquet paths) guarantees
        # EIBMNPL2 never sees the None placeholders.
        import EIBMNPL2

        for entity in ENTITIES:
            print(f"\n{'='*70}\nProcessing entity: {entity}\n{'='*70}")

            loan1, loan2 = _build_loan1_loan2(entity)
            loan1_p = _pad_all_riskrates(loan1)
            loan2_p = _pad_all_riskrates(loan2)
            loan3 = _build_loan3(entity)
            loan3_p = _pad_all_riskrates(loan3)
            loan4_p = loan1_p + loan2_p + loan3_p   # SET LOAN1 LOAN2 LOAN3;

            _run_tbls(loan1_p, loan2_p, loan3_p, loan4_p)

            temp_out = OUTPUT_DIR / f"{entity}_ODTLLIST_TEXT.txt"
            _write_temp_output(loan4_p, temp_out)
            print(f"  Output written : {temp_out}")

            loan4_sorted = sorted(loan4_p, key=lambda x: x["BRANCH"])
            asa = AsaWriter(page_size=PAGE_SIZE)
            _render_ageing_table(asa, loan4_sorted)

            # %INC PGM1(EIBMNPL2); -- continue building the SAME
            # ODTLLIST.COLD output (append, no PRINTTO...NEW) with the
            # loans/O-D detail prints, reading from the SAME temp Parquet
            # files converted above (no re-conversion per entity).
            EIBMNPL2.run(entity, asa)

            cold_out = OUTPUT_DIR / f"{entity}_ODTLLIST_COLD.txt"
            asa.write(cold_out)
            print(f"  Output written : {cold_out}")
            print(f"  Total lines    : {len(asa.lines):,}")

            print("\n--- Console preview (ageing + detail titles) ---")
            for ln in asa.lines[:20]:
                print(ln)

            del loan1, loan2, loan3, loan1_p, loan2_p, loan3_p, loan4_p, loan4_sorted
            gc.collect()

        print("\nEIBMNPL1 / EIBMNPL2 complete.")

    finally:
        # Guaranteed cleanup: temp Parquet files are deleted whether the run
        # succeeded or raised partway through, leaving no cache behind.
        _cleanup_inputs(temp_dir)


if __name__ == "__main__":
    main()
