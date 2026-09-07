#!/usr/bin/env python3
print("===== SCRIPT START =====")
"""
Program : EIBMNPL1.py
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

    %INC PGM1(EIBMNPL2);  -- continuation program, kept in its own file
        EIBMNPL2.py imports shared state (paths, caches, REPTDATE values,
        helper functions) FROM this module ("from EIBMNPL1 import ..."),
        matching the "%INC continues in the same session" semantics. Since
        this combined run loops over both PBB and PIBB in one Python
        process (per project instruction to merge the two JCL variants),
        the plain "import EIBMNPL2 -> executes once" driver pattern is
        replaced by explicit function calls into EIBMNPL2, made once per
        entity inside the loop below (see main()).

============================================================================
PHYSICAL INPUT DATASETS (independent cache, sas7bdat -> parquet
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
   LOAN parquet distinguished by ENTITY_CD ('PBB' / 'PIBB'), per project
   instruction, and both are produced in a single run (loop over ENTITY).

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
from pathlib import Path
from datetime import date, timedelta
from typing import Optional

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

INPUT_LOAN_PBB_DIR  = STG_DIR / "EIBMNPL"
INPUT_LOAN_PIBB_DIR = STG_DIR / "EIBMNPL"
INPUT_OVERDFT_DIR   = STG_DIR / "EIBMNPL"

INPUT_OVERDFT_FILE = INPUT_OVERDFT_DIR / "intg_dp_acct_overdft_d31.sas7bdat"

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMNPL"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIBMNPL"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000
PAGE_SIZE  = 60     # lines per page (not specified in SAS -> default)
LINESIZE_TEMP     = 256
LINESIZE_ODTLLIST = 132

ENTITIES = ("PBB", "PIBB")

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

# INPUT_LOAN_PBB_FILE  = INPUT_LOAN_PBB_DIR  / f"loan{REPTMON}{NOWK}.sas7bdat"
# INPUT_LOAN_PIBB_FILE = INPUT_LOAN_PIBB_DIR / f"iloan{REPTMON}{NOWK}.sas7bdat"

INPUT_LOAN_PBB_FILE  = INPUT_LOAN_PBB_DIR  / f"loan084.sas7bdat"
INPUT_LOAN_PIBB_FILE = INPUT_LOAN_PIBB_DIR / f"iloan084.sas7bdat"

print(f"  RDATE           : {RDATE}")
print(f"  REPTMON/NOWK    : {REPTMON}/{NOWK}")
print(f"  PBB LOAN input  : {INPUT_LOAN_PBB_FILE.name}")
print(f"  PIBB LOAN input : {INPUT_LOAN_PIBB_FILE.name}")
print(f"  OVERDFT input   : {INPUT_OVERDFT_FILE.name}")

TBL_LABELS = {1: "(LOANS)", 2: "(HP)", 3: "(O/D)", 4: "(LOANS & O/D)"}

# ============================================================================
# %TBLS COLUMN CHUNKING (RISKRATE horizontal groups per LINESIZE=132 page)
# ============================================================================
# TABLE ... RISKRATE=' '*(N*F=COMMA6./7. SUM*F=COMMA14.) columns are wider
# than the plain-BEST TEMP-output table, so fewer RISKRATE groups fit per
# line before PROC TABULATE wraps horizontally. Break points observed from
# the actual SAS output: [0:6), [6:13), [13:15) RISKRATE columns per chunk.
TBLS_CHUNK_BOUNDARIES = [6, 13, 15]   # RISKRATE cols: [0:6), [6:13), [13:15)


def _tbls_chunks(riskrates: list) -> list:
    """Splits RISKRATE columns (0-14) into the 3 horizontal chunks that
    PROC TABULATE's page-wrap produces for this specific table."""
    chunks = []
    start = 0
    for end in TBLS_CHUNK_BOUNDARIES:
        chunks.append(riskrates[start:end])
        start = end
    return chunks

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
def _parse_date_from_sas_date(value):
    if value is None or value == 0:
        return None
    try:
        return date(1960, 1, 1) + timedelta(days=int(value))
    except (TypeError, ValueError):
        return None

def parse_excessdt(excessdt):
    return _parse_date_from_sas_date(excessdt)

def parse_toddate(toddate):
    return _parse_date_from_sas_date(toddate)

def _bldate_from_sas_date(raw_value):
    return _parse_date_from_sas_date(raw_value)

# Update compute_bldate to use the new function
def compute_bldate(excessdt, toddate):
    bldate = None
    if excessdt != 0 and toddate != 0:
        excdate = parse_excessdt(excessdt)
        toddt = parse_toddate(toddate)
        if excdate is not None and toddt is not None:
            if excdate <= toddt:
                bldate = _bldate_from_sas_date(excessdt)
            else:
                bldate = _bldate_from_sas_date(toddate)
    elif excessdt is not None and excessdt != 0:
        bldate = _bldate_from_sas_date(excessdt)
    elif toddate is not None and toddate != 0:
        bldate = _bldate_from_sas_date(toddate)
    return bldate


# ============================================================================
# CACHE STAMP + STREAM .sas7bdat -> PARQUET
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


print("\nStep 2: Caching input SAS datasets to Parquet...")
LOAN_PBB_CACHE  = _load_cached(INPUT_LOAN_PBB_FILE, "LOAN_PBB")
LOAN_PIBB_CACHE = _load_cached(INPUT_LOAN_PIBB_FILE, "LOAN_PIBB")
OVERDFT_CACHE   = _load_cached(INPUT_OVERDFT_FILE, "OVERDFT")


def _loan_cache_for(entity: str) -> Path:
    return LOAN_PBB_CACHE if entity == "PBB" else LOAN_PIBB_CACHE

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


def _num_nocomma(value, width: int, decimals: int = 2) -> str:
    """Plain numeric PUT with no COMMA format — SAS default numeric-missing
    prints blank, not 0."""
    if value is None:
        return " " * width
    return f"{float(value):.{decimals}f}".rjust(width)


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
            (DATE '1960-01-01' + CAST(BLDATE AS INTEGER)) AS BLDATE,
            CAST(RISKRTE  AS INTEGER) AS RISKRTE
        FROM read_parquet('{_loan_cache_for(entity).as_posix()}')
        WHERE NOTENO < 90000
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
        FROM read_parquet('{_loan_cache_for(entity).as_posix()}')
        WHERE ACCTYPE = 'OD'
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
TEMP_LABEL_WIDTH = 5     # RTS=5
TEMP_N_WIDTH     = 5     # F=5. on N
TEMP_SUM_WIDTH   = 12    # unformatted SUM -> default BESTw. width
TEMP_SEP_WIDTH   = 1     # one blank column preceding every cell

TEMP_PAGE_LEN     = 60
TEMP_TOP_MARGIN   = 4    # blank lines before the header line
TEMP_BOTTOM_BLANK = 2    # blank lines before "(Continued)"
TEMP_DATA_ROWS_PER_PAGE = (
    TEMP_PAGE_LEN - TEMP_TOP_MARGIN - 1  # header line
    - 1                                   # blank after header
    - TEMP_BOTTOM_BLANK - 1               # "(Continued)" line
)  # = 51

# Explicit horizontal-split points, in flat cell-index terms, where the
# flat cell sequence is [N0, SUM0, N1, SUM1, ..., N14, SUM14] (30 cells,
# indices 0-29). A break value B means "start a new chunk before cell B".
#
# BALANCE splits cleanly between whole RISKRATE groups: groups 0-12
# together, then 13-14 -- one break, after group 12's SUM cell (index 25),
# i.e. before index 26.
TEMP_BALANCE_BREAKS = [26]

# RISKBAL splits mid-group at column 6: groups 0-5 plus N6 in chunk 1;
# SUM6 plus groups 7-12 in chunk 2; groups 13-14 in chunk 3.
TEMP_RISKBAL_BREAKS = [13, 26]


def _best_format(value, width: int = TEMP_SUM_WIDTH) -> str:
    """Emulates BESTw. for an unformatted TABULATE SUM cell: no comma
    grouping; decimals step down 2 -> 1 -> 0 until the value fits exactly
    in `width` characters. MISSING=0 -> None renders as '0'."""
    if value is None:
        return "0".rjust(width)
    v = float(value)
    sign = "-" if v < 0 else ""
    v_abs = abs(v)
    for decimals in (2, 1, 0):
        s = f"{sign}{v_abs:.{decimals}f}"
        if len(s) <= width:
            return s.rjust(width)
    s = f"{sign}{v_abs:.0f}"
    return s[-width:].rjust(width)


def _n_format(value, width: int = TEMP_N_WIDTH) -> str:
    v = 0 if value is None else int(value)
    s = str(v)
    return (s[-width:] if len(s) > width else s).rjust(width)


def _build_cell_layout(riskrates: list) -> list:
    """Flat list of (riskrate, kind, width) cells in print order:
    N0, SUM0, N1, SUM1, ... A chunk boundary can fall between a group's
    N cell and its SUM cell, so cells are tracked individually rather
    than as N/SUM pairs."""
    cells = []
    for rr in riskrates:
        cells.append((rr, "N", TEMP_N_WIDTH))
        cells.append((rr, "SUM", TEMP_SUM_WIDTH))
    return cells


def _split_by_breaks(cells: list, breaks: list) -> list:
    """Splits `cells` into chunks at the given flat cell-index breakpoints."""
    chunks, start = [], 0
    for b in breaks:
        chunks.append(cells[start:b])
        start = b
    chunks.append(cells[start:])
    return [c for c in chunks if c]


def _build_chunk_line(chunk: list, label_width: int, cross: dict, branch=None) -> str:
    """Builds either the header line (branch=None) or one data row for
    `branch`, for a single horizontal chunk. Consecutive cells sharing
    the same RISKRATE are grouped so the header label is centered over
    however much of that group (N only, SUM only, or both) is present
    in this chunk -- this is what lets column 6's label appear once at
    the end of one chunk and again at the start of the next."""
    groups = []
    for cell in chunk:
        rr = cell[0]
        if groups and groups[-1][0] == rr:
            groups[-1][1].append(cell)
        else:
            groups.append((rr, [cell]))

    if branch is None:
        out = " " * label_width
        for rr, group_cells in groups:
            total_width = sum(TEMP_SEP_WIDTH + w for (_, _, w) in group_cells)
            out += str(rr).center(total_width)
        return out

    out = branch.ljust(label_width)
    for rr, group_cells in groups:
        n, s = cross.get((branch, rr), (0, None))
        for (_, kind, width) in group_cells:
            value = _n_format(n, width) if kind == "N" else _best_format(s, width)
            out += " " * TEMP_SEP_WIDTH + value
    return out


def _render_table_pages(branches: list, cross: dict, riskrates: list,
                         breaks: list, is_last_table: bool) -> list:
    """Renders one full TABULATE table across all its horizontal chunks
    (per `breaks`) and vertical (60-line) pages. '(Continued)' prints
    after every page except the very last page of the very last chunk
    of the very last table in this output file."""
    cells = _build_cell_layout(riskrates)
    chunks = _split_by_breaks(cells, breaks)

    lines = []
    for chunk_idx, chunk in enumerate(chunks):
        header = _build_chunk_line(chunk, TEMP_LABEL_WIDTH, cross, branch=None)
        is_last_chunk = chunk_idx == len(chunks) - 1

        idx = 0
        n_branches = len(branches)
        while True:
            page_branches = branches[idx: idx + TEMP_DATA_ROWS_PER_PAGE]
            lines.extend([""] * TEMP_TOP_MARGIN)
            lines.append(header)
            lines.append("")
            for b in page_branches:
                lines.append(_build_chunk_line(chunk, TEMP_LABEL_WIDTH, cross, branch=b))

            idx += TEMP_DATA_ROWS_PER_PAGE
            more_branches_remain = idx < n_branches
            is_last_page_overall = (
                is_last_table and is_last_chunk and not more_branches_remain
            )

            if not is_last_page_overall:
                lines.extend([""] * TEMP_BOTTOM_BLANK)
                lines.append("(Continued)")

            if not more_branches_remain:
                break

    return lines


def _write_temp_output(loan4_padded: list, out_path: Path) -> None:
    """PROC PRINTTO PRINT=TEMP NEW; OPTION LINESIZE=256;
    Two PROC TABULATE calls (BALANCE, then RISKBAL), FORMCHAR blank,
    NOSEPS, TITLE1/TITLE2 blank, RTS=5 CONDENSE. RECFM=FB -> no ASA byte;
    pagination is blank-line based only (no '1' carriage-control byte).
    BALANCE (primary var) wraps once at column 12/13; RISKBAL (secondary
    var) wraps twice, splitting mid-group at column 6 and again at 12/13."""
    print(f"\nStep 6: Writing TEMP (ODTLLIST.TEXT, no ASA) -> {out_path.name}")

    riskrates = list(range(0, 15))
    branches = sorted({r["BRANCH"] for r in loan4_padded})

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

    all_lines = []
    table_specs = [
        ("BALANCE", TEMP_BALANCE_BREAKS),
        ("RISKBAL", TEMP_RISKBAL_BREAKS),
    ]
    for i, (field, breaks) in enumerate(table_specs):
        cross = _crosstab(field)
        is_last_table = (i == len(table_specs) - 1)
        all_lines.extend(_render_table_pages(branches, cross, riskrates, breaks, is_last_table))

    with open(out_path, "w", encoding="latin1") as fh:
        for ln in all_lines:
            fh.write(ln + "\n")


# ============================================================================
# STEP 7: ODTLLIST OUTPUT (ODTLLIST.COLD) -- ASA control, LRECL=136
# ============================================================================
AGE_LABEL_W = 28
AGE_NO_W    = 6
AGE_AMT_W   = 18
AGE_GROUP_W = AGE_NO_W + 1 + AGE_AMT_W                       # 25
AGE_BORDER_W = AGE_LABEL_W + 4 * 1 + 2 * AGE_NO_W + 2 * AGE_AMT_W + 2  # 82

AGE_GROUP1_LINE1 = "O/S LOANS IN ARREARS"
AGE_GROUP1_LINE2 = "(RMM)"
AGE_GROUP2_LINE1 = "O/S LOANS CLASSIFIED AS"
AGE_GROUP2_LINE2 = "NPL(RMM) 2,3,4"


def _ageing_title_block(branch: str) -> list:
    """TITLE1/TITLE2 plus the default BY-line block PROC TABULATE prints
    for 'BY BRANCH;' (blank, 'BRANCH=xxx', blank) before the table."""
    return [
        "AGEING OF ALL OVERDUE OD & TERM LOANS",
        f"AS AT {RDATE}",
        "",
        f"BRANCH={branch}",
        "",
    ]


def _ageing_count(value, width: int = AGE_NO_W) -> str:
    """N statistic -- a count, never missing; always COMMA-formatted."""
    v = 0 if value is None else int(value)
    return f"{v:,}".rjust(width)


def _ageing_amount(value, n_count, width: int = AGE_AMT_W) -> str:
    """SUM statistic under OPTIONS MISSING=0: a cell with zero
    contributing observations is genuinely missing, not a computed
    zero, and prints as a bare '0' with no decimals/commas -- the
    MISSING= substitution bypasses the numeric FORMAT entirely. A cell
    with at least one contributing observation is always a real number
    and gets the normal COMMA18.2 treatment."""
    if not n_count:
        return "0".rjust(width)
    return f"{float(value):,.2f}".rjust(width)


def _age_row(label: str, n_bal, s_bal, n_rb, s_rb) -> str:
    return (
        "|" + label.ljust(AGE_LABEL_W)[:AGE_LABEL_W]
        + "|" + _ageing_count(n_bal)
        + "|" + _ageing_amount(s_bal, n_bal)
        + "|" + _ageing_count(n_rb)
        + "|" + _ageing_amount(s_rb, n_rb)
        + "|"
    )


def _age_divider_full() -> str:
    return (
        "|" + "-" * AGE_LABEL_W
        + "+" + "-" * AGE_NO_W
        + "+" + "-" * AGE_AMT_W
        + "+" + "-" * AGE_NO_W
        + "+" + "-" * AGE_AMT_W
        + "|"
    )


def _age_border() -> str:
    return "-" * AGE_BORDER_W


def _render_ageing_table(asa: AsaWriter, loan4_padded: list) -> None:
    """PROC TABULATE DATA=LOAN4 MISSING; FORMAT RISKRATE RISK.; BY BRANCH;
    CLASS BRANCH RISKRATE; VAR BALANCE RISKBAL;
    TABLE RISKRATE=' ' ALL='TOTAL', (BALANCE=... RISKBAL=...)*(N SUM)
    / BOX=' ' RTS=30 CONDENSE;
    PROC TABULATE starts a new page for every BY-group by default, so
    each branch gets its own full-page box-drawn table -- there is no
    mid-branch pagination or multi-branch packing to manage, since the
    fixed 15-bucket + TOTAL layout (~44 lines) always fits well under
    PAGESIZE=60."""
    branches = sorted({r["BRANCH"] for r in loan4_padded})
    by_branch: dict = {}
    for r in loan4_padded:
        by_branch.setdefault(r["BRANCH"], []).append(r)

    for branch in branches:
        rows = by_branch[branch]
        by_rr: dict = {}
        for r in rows:
            key = r["RISKRATE"]
            n_bal, s_bal, n_rb, s_rb = by_rr.get(key, (0, None, 0, None))
            if r["BALANCE"] is not None:
                n_bal += 1
                s_bal = (s_bal or 0.0) + r["BALANCE"]
            if r["RISKBAL"] is not None:
                n_rb += 1
                s_rb = (s_rb or 0.0) + r["RISKBAL"]
            by_rr[key] = (n_bal, s_bal, n_rb, s_rb)

        asa.new_page(_ageing_title_block(branch))

        asa.add(_age_border())
        asa.add(
            "|" + " " * AGE_LABEL_W
            + "|" + center(AGE_GROUP1_LINE1, AGE_GROUP_W)
            + "|" + center(AGE_GROUP2_LINE1, AGE_GROUP_W)
            + "|"
        )
        asa.add(
            "|" + " " * AGE_LABEL_W
            + "|" + center(AGE_GROUP1_LINE2, AGE_GROUP_W)
            + "|" + center(AGE_GROUP2_LINE2, AGE_GROUP_W)
            + "|"
        )
        asa.add(
            "|" + " " * AGE_LABEL_W
            + "|" + "-" * AGE_GROUP_W
            + "+" + "-" * AGE_GROUP_W
            + "|"
        )
        asa.add(
            "|" + " " * AGE_LABEL_W
            + "|" + center("NO.", AGE_NO_W)
            + "|" + center("AMOUNT", AGE_AMT_W)
            + "|" + center("NO.", AGE_NO_W)
            + "|" + center("AMOUNT", AGE_AMT_W)
            + "|"
        )
        asa.add(_age_divider_full())

        tot_n_bal = tot_n_rb = 0
        tot_s_bal = tot_s_rb = None
        for rr in range(0, 15):
            n_bal, s_bal, n_rb, s_rb = by_rr.get(rr, (0, None, 0, None))
            tot_n_bal += n_bal
            tot_n_rb += n_rb
            if s_bal is not None:
                tot_s_bal = (tot_s_bal or 0.0) + s_bal
            if s_rb is not None:
                tot_s_rb = (tot_s_rb or 0.0) + s_rb

            asa.add(_age_row(format_risk(rr), n_bal, s_bal, n_rb, s_rb))
            asa.add(_age_divider_full())

        asa.add(_age_row("TOTAL", tot_n_bal, tot_s_bal, tot_n_rb, tot_s_rb))
        asa.add(_age_border())


# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    # Deferred import: EIBMNPL2 imports state (REPTDATE, RDATE, LOAN_CACHE,
    # OVERDFT_CACHE, format_brchcd, parse_*, compute_bldate, AsaWriter, etc.)
    # FROM this module, so this module's top-level code must have already
    # finished executing before EIBMNPL2 is imported -- deferring the import
    # to inside main() avoids any circular-import issue.
    import EIBMNPL2

    for entity in ENTITIES:
        print(f"\n{'='*70}\nProcessing entity: {entity}\n{'='*70}")

        loan1, loan2 = _build_loan1_loan2(entity)
        loan1_p = _pad_all_riskrates(loan1)
        loan2_p = _pad_all_riskrates(loan2)
        loan3 = _build_loan3(entity)
        loan3_p = _pad_all_riskrates(loan3)
        loan4_p = loan1_p + loan2_p + loan3_p   # DATA LOAN4; SET LOAN1 LOAN2 LOAN3;

        # _run_tbls(loan1_p, loan2_p, loan3_p, loan4_p)

        with open(OUTPUT_DIR / f"{entity}_TBLS.log", "w") as f:
            import sys
            sys.stdout = f
            _run_tbls(loan1_p, loan2_p, loan3_p, loan4_p)
            sys.stdout = sys.__stdout__

        temp_out = OUTPUT_DIR / f"{entity}_ODTLLIST_TEXT.txt"
        _write_temp_output(loan4_p, temp_out)
        print(f"  Output written : {temp_out}")

        loan4_sorted = sorted(loan4_p, key=lambda x: x["BRANCH"])
        asa = AsaWriter(page_size=PAGE_SIZE)
        _render_ageing_table(asa, loan4_sorted)

        # %INC PGM1(EIBMNPL2); -- continue building the SAME ODTLLIST.COLD
        # output (append, no PRINTTO...NEW) with the loans/O-D detail prints.
        EIBMNPL2.run(entity, asa)

        cold_out = OUTPUT_DIR / f"{entity}_ODTLLIST_COLD.txt"
        asa.write(cold_out)
        print(f"  Output written : {cold_out}")
        print(f"  Total lines    : {len(asa.lines):,}")

        # print("\n--- Console preview (ageing + detail titles) ---")
        # for ln in asa.lines[:20]:
        #     print(ln)

        del loan1, loan2, loan3, loan1_p, loan2_p, loan3_p, loan4_p, loan4_sorted
        gc.collect()

    print("\nEIBMNPL1 / EIBMNPL2 complete.")


if __name__ == "__main__":
    main()
