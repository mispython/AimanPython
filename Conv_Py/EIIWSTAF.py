#!/usr/bin/env python3
"""
Program : EIIWSTAF.py
Purpose : Weekly Listing For Staff New Loan And Paid Loan (PBB + PIBB
           staff loan accounts, cost-centre based staff-loan population).

Dependency:
    No %INC PGM(...) statements are present anywhere in the original SAS
    source (EIIWSTAF) -- there is therefore no external format-library or
    macro-program dependency to import for this conversion.

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently, using the
same chunked sas7bdat -> Parquet -> cache pattern as EIBDLN1M.py / EIIMRM01.py)
============================================================================
1. MNILN.LNNOTE   (JCL //MNILN  DD DSN=SAP.PBB.MNILN(0))
   File : INPUT_LNNOTE_PBB_FILE  -> mnln_lnnote_pbb.sas7bdat
   Used : PROC SORT ... WHERE (LOANTYPE<=61 OR LOANTYPE IN(...)) AND
          COSTCTR=8044 (PBB staff cost centre).

2. MNILN.LNCOMM   (same //MNILN DD, member LNCOMM)
   File : INPUT_LNCOMM_PBB_FILE  -> mnln_lncomm_pbb.sas7bdat
   Used : merged onto LNNOTE (PBB) by ACCTNO/COMMNO for APPRLIMT sourcing.

3. IMNILN.LNNOTE  (JCL //IMNILN DD DSN=SAP.PIBB.MNILN(0))
   File : INPUT_LNNOTE_PIBB_FILE -> imnln_lnnote_pibb.sas7bdat
   Used : same as (1) but COSTCTR BETWEEN 3000 AND 3999 (PIBB staff range).

4. IMNILN.LNCOMM  (same //IMNILN DD, member LNCOMM)
   File : INPUT_LNCOMM_PIBB_FILE -> imnln_lncomm_pibb.sas7bdat

5. PAY.LNPAY&NOWK   (JCL //PAY  DD DSN=SAP.PBB.LNPAYSCH.WEEK, member LNPAY<wk>)
   File : INPUT_LNPAY_PBB_FILE  -> lnpay<NOWK>.sas7bdat
   NOWK is fully deterministic from REPTDATE (exact-day match, see Step 1),
   so the filename is constructed directly -- input_date.get_latest_file()
   is intentionally NOT used here (per project convention for deterministic
   filenames).

6. IPAY.ILNPAY&NOWK (JCL //IPAY DD DSN=SAP.PIBB.LNPAYSCH.WEEK, member ILNPAY<wk>)
   File : INPUT_LNPAY_PIBB_FILE -> ilnpay<NOWK>.sas7bdat

7. LNHIST.ISBASE  (JCL LIBNAME LNHIST "SAP.PBB.SGM.SASDATA", DISP=OLD)
   File : INPUT_ISBASE_FILE -> lnhist_isbase.sas7bdat
   This is a PERSISTENT history table of already-reported released/migrated
   loan notes (ACCTNO, NOTENO only). It is both READ (as HIST, to exclude
   previously-reported accounts) and WRITTEN to (PROC APPEND DATA=LNRELS1
   BASE=LNHIST.ISBASE) every run. In this Python conversion the persistent
   store is modelled as the cached ISBASE Parquet file itself: new
   ACCTNO/NOTENO keys are appended onto ISBASE_CACHE at the end of the run
   so subsequent runs' HIST dedup will exclude them. The underlying
   .sas7bdat source file is not rewritten by this program.

============================================================================
OUTPUT
============================================================================
//SASLIST DD DSN=SAP.PIBB.EIIWSTAF(+1), DISP=(NEW,CATLG,DELETE),
          DCB=(RECFM=FB,LRECL=133,BLKSIZE=0)
This is a GDG-style catalogued listing dataset with NO date token in the
generation name (the "+1" is a GDG generation number, not a calendar date),
so per project convention output_date.py / build_output_file() is NOT used
here -- the Python output uses a fixed filename.

RECFM=FB (NOT FBA) means -- per project convention -- this report carries
NO ASA carriage-control byte, matching the precedent already established in
EIIMRM01.py. Page breaks (PAGESIZE=60, explicitly set via
`OPTIONS LS=132 PS=60 NOCENTER;` in the SAS source) are marked with a plain
form-feed character rather than an ASA '1' byte. All four PROC REPORT steps
in the SAS source (LNSETTLE / LNRPT1A / LNRPT1B / LNRPT1C) write to this
same physical SASLIST listing destination, in sequence.
"""

import gc
from pathlib import Path
from datetime import date, timedelta

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
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_LNNOTE_PBB_DIR  = STG_DIR / "sasdata"
INPUT_LNCOMM_PBB_DIR  = STG_DIR / "sasdata"
INPUT_LNNOTE_PIBB_DIR = STG_DIR / "sasdata"
INPUT_LNCOMM_PIBB_DIR = STG_DIR / "sasdata"
INPUT_LNPAY_PBB_DIR   = STG_DIR / "sasdata"
INPUT_LNPAY_PIBB_DIR  = STG_DIR / "sasdata"
INPUT_ISBASE_DIR      = STG_DIR / "sasdata"

INPUT_LNNOTE_PBB_FILE  = INPUT_LNNOTE_PBB_DIR  / "mnln_lnnote_pbb.sas7bdat"     # MNILN.LNNOTE
INPUT_LNCOMM_PBB_FILE  = INPUT_LNCOMM_PBB_DIR  / "mnln_lncomm_pbb.sas7bdat"     # MNILN.LNCOMM
INPUT_LNNOTE_PIBB_FILE = INPUT_LNNOTE_PIBB_DIR / "imnln_lnnote_pibb.sas7bdat"   # IMNILN.LNNOTE
INPUT_LNCOMM_PIBB_FILE = INPUT_LNCOMM_PIBB_DIR / "imnln_lncomm_pibb.sas7bdat"   # IMNILN.LNCOMM
INPUT_ISBASE_FILE      = INPUT_ISBASE_DIR / "lnhist_isbase.sas7bdat"            # LNHIST.ISBASE

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIWSTAF"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR  = BASE_DIR / "output" / "EIIWSTAF"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "EIIWSTAF.txt"

CHUNK_ROWS = 500_000
PAGE_SIZE  = 60     # OPTIONS PS=60 (explicit in SAS source)
LINE_SIZE  = 132    # OPTIONS LS=132 (explicit in SAS source)
FF = "\f"

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet -- derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate     # SAS: SET MNILN.REPTDATE (this run's REPTDATE)

# NOWK/SDD/STRDAY in the original SAS SELECT(DAY(REPTDATE)) block use an
# EXACT-day match (8/15/22/else 23), which differs from REPTDATE.py's
# range-based NOWK -- re-derived locally, same pattern as EIIMRM01.py.
_day = reptdate.day
if _day == 8:
    STRDAY, NOWK = 1, "01"
elif _day == 15:
    STRDAY, NOWK = 9, "02"
elif _day == 22:
    STRDAY, NOWK = 16, "03"
else:
    STRDAY, NOWK = 23, "04"

RPYR, RPMTH = reptdate.year, reptdate.month
PDATE = date(RPYR, RPMTH, 1)                 # MDY(MMP,01,YYP)
EDATE = reptdate                              # PUT(REPTDATE,Z5.) -- used only for date-range comparisons

# SDATE = MDY(MMP,SDD,YYP) is SYMPUT'd in the original SAS but &SDATE is
# never referenced anywhere else in the program body -- dead value, kept
# only for documentation parity.
SDATE = date(RPYR, RPMTH, STRDAY)  # noqa: F841 (intentionally unused, mirrors SAS dead code)

# PREVDATE / REPTMM / REPTYY: the original SAS `IF WK='4' THEN ... ELSE ...`
# branch compares the 2-character WK ('01'/'02'/'03'/'04') against the
# 1-character literal '4', which never matches -- the ELSE branch therefore
# ALWAYS fires regardless of the actual week, so REPTMM/REPTYY are always
# derived from PREVDATE even in week 4. Both macro variables are, in turn,
# never referenced anywhere else in the program -- this whole branch is
# dead code, preserved here only for documentation parity with the SAS
# source (same treatment EIIMRM01.py gives its own dead NOWK derivation).
PREVDATE = PDATE - timedelta(days=1)
REPTMM = f"{PREVDATE.month:02d}"   # noqa: F841 (dead, mirrors SAS)
REPTYY = PREVDATE.strftime("%y")   # noqa: F841 (dead, mirrors SAS)
REPTMON = f"{RPMTH:02d}"           # noqa: F841 (SYMPUT'd but never referenced again -- dead, mirrors SAS)

REPTDAY = reptdate.day
REPTMTH = RPMTH
REPTYEAR = RPYR
RDATE = reptdate.strftime("%d/%m/%Y")   # PUT(REPTDATE, DDMMYY10.)

print(f"  RDATE        : {RDATE}")
print(f"  NOWK (week)  : {NOWK}   STRDAY: {STRDAY}   REPTDAY: {REPTDAY}")
print(f"  PDATE..EDATE : {PDATE} .. {EDATE}")
print(f"  Output file  : {OUTPUT_FILE.name}")

# Weekly input filenames depend on NOWK, which is only known after Step 1 --
# constructed directly (deterministic), per project convention.
INPUT_LNPAY_PBB_FILE  = INPUT_LNPAY_PBB_DIR  / f"lnpay{NOWK}.sas7bdat"    # PAY.LNPAY&NOWK
INPUT_LNPAY_PIBB_FILE = INPUT_LNPAY_PIBB_DIR / f"ilnpay{NOWK}.sas7bdat"   # IPAY.ILNPAY&NOWK

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


# ============================================================================
# STEP 2: CACHE INPUT SAS FILES TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
LNNOTE_PBB_CACHE  = _load_cached(INPUT_LNNOTE_PBB_FILE, "LNNOTE_PBB")
LNCOMM_PBB_CACHE  = _load_cached(INPUT_LNCOMM_PBB_FILE, "LNCOMM_PBB")
LNNOTE_PIBB_CACHE = _load_cached(INPUT_LNNOTE_PIBB_FILE, "LNNOTE_PIBB")
LNCOMM_PIBB_CACHE = _load_cached(INPUT_LNCOMM_PIBB_FILE, "LNCOMM_PIBB")
LNPAY_PBB_CACHE   = _load_cached(INPUT_LNPAY_PBB_FILE, "LNPAY_PBB")
LNPAY_PIBB_CACHE  = _load_cached(INPUT_LNPAY_PIBB_FILE, "LNPAY_PIBB")
ISBASE_CACHE      = _load_cached(INPUT_ISBASE_FILE, "ISBASE")

# ============================================================================
# GENERIC HELPER: SAS `MERGE a(IN=x) b(IN=y); BY <keys>;` EMULATION
# ============================================================================
def _group_consecutive(rows, key_fields):
    """Group PRE-SORTED rows into consecutive equal-key chunks (SAS BY-group
    processing requires both merge inputs to already be sorted by the BY
    variables)."""
    groups = []
    cur_key = None
    cur_chunk = []
    for r in rows:
        k = tuple(r.get(f) for f in key_fields)
        if cur_chunk and k != cur_key:
            groups.append((cur_key, cur_chunk))
            cur_chunk = []
        cur_key = k
        cur_chunk.append(r)
    if cur_chunk:
        groups.append((cur_key, cur_chunk))
    return groups


def _sas_merge_retain(left_rows, right_rows, key_fields):
    """
    Emulates `MERGE left(IN=a) right(IN=b); BY <key_fields>;`, including
    SAS's documented (and easily-overlooked) behaviour for BY-groups with
    non-unique BY values on either side: within a shared BY-group, the two
    datasets are stepped through in lock-step, one new source observation
    per DATA-step iteration from whichever side still has unread rows for
    the group; once a side's rows for the group are exhausted, its non-BY
    columns are simply RETAINED (not reset to missing) for the rest of the
    group -- and, since only an actual MERGE read can change a PDV column,
    that retention persists globally across BY-groups where a side
    contributes zero observations, not just within one group (project
    convention: "SAS MERGE last-dataset-wins semantics ... replicate by
    explicitly tracking which dataset's columns win").
    BY-key columns themselves always come from the current group's key
    (SAS handles BY variables specially; they never go stale).
    Returns combined dict rows tagged with '_IN_LEFT' / '_IN_RIGHT'.
    """
    left_groups = _group_consecutive(left_rows, key_fields)
    right_groups = _group_consecutive(right_rows, key_fields)
    li = ri = 0
    retained_left: dict = {}
    retained_right: dict = {}
    out = []

    while li < len(left_groups) or ri < len(right_groups):
        lkey = left_groups[li][0] if li < len(left_groups) else None
        rkey = right_groups[ri][0] if ri < len(right_groups) else None

        if lkey is not None and (rkey is None or lkey < rkey):
            key, lrows, rrows = lkey, left_groups[li][1], []
            li += 1
        elif rkey is not None and (lkey is None or rkey < lkey):
            key, lrows, rrows = rkey, [], right_groups[ri][1]
            ri += 1
        else:
            key, lrows, rrows = lkey, left_groups[li][1], right_groups[ri][1]
            li += 1
            ri += 1

        n = max(len(lrows), len(rrows))
        in_left = len(lrows) > 0
        in_right = len(rrows) > 0
        for j in range(n):
            if j < len(lrows):
                retained_left = lrows[j]
            if j < len(rrows):
                retained_right = rrows[j]
            combined = {**retained_left, **retained_right}
            for i, f in enumerate(key_fields):
                combined[f] = key[i]
            combined["_IN_LEFT"] = in_left
            combined["_IN_RIGHT"] = in_right
            out.append(combined)
    return out


# ============================================================================
# STEP 3/4: BUILD LNNOTE (PBB) AND ILNNOTE (PIBB)
# DATA LNNOTE; MERGE LNNOTE(IN=A) LNCOMM; BY ACCTNO COMMNO; IF A; ...
# ============================================================================
def _build_lnnote(tag: str, note_cache: Path, comm_cache: Path, costctr_sql: str) -> list:
    con = duckdb.connect(database=":memory:")
    note_pl = con.execute(f"""
        SELECT *
        FROM read_parquet('{note_cache.as_posix()}')
        WHERE (LOANTYPE IS NULL OR LOANTYPE <= 61 OR LOANTYPE IN (100,102,103,104,105))
          AND {costctr_sql}
        ORDER BY ACCTNO, COMMNO
    """).pl()
    comm_pl = con.execute(f"""
        SELECT * FROM read_parquet('{comm_cache.as_posix()}')
        ORDER BY ACCTNO, COMMNO
    """).pl()
    con.close()

    left = note_pl.to_dicts()
    right = comm_pl.to_dicts()
    merged = _sas_merge_retain(left, right, key_fields=("ACCTNO", "COMMNO"))

    out_rows = []
    for r in merged:
        if not r["_IN_LEFT"]:
            continue  # IF A;
        commno = r.get("COMMNO") or 0
        if commno > 0:
            apprlimt = r.get("CORGAMT") if r.get("REVOVLI") == "N" else r.get("CCURAMT")
        else:
            apprlimt = r.get("ORGBAL")
        r["APPRLIMT"] = apprlimt
        out_rows.append(r)
    print(f"  [{tag}] LNNOTE rows after merge with LNCOMM: {len(out_rows):,}")
    return out_rows


print("\nStep 3: Building LNNOTE (PBB, COSTCTR=8044)...")
lnnote_pbb_rows = _build_lnnote("PBB", LNNOTE_PBB_CACHE, LNCOMM_PBB_CACHE, "COSTCTR = 8044")

print("\nStep 4: Building ILNNOTE (PIBB, 3000<=COSTCTR<=3999)...")
lnnote_pibb_rows = _build_lnnote("PIBB", LNNOTE_PIBB_CACHE, LNCOMM_PIBB_CACHE,
                                  "COSTCTR BETWEEN 3000 AND 3999")

# ============================================================================
# STEP 5: DATA LOAN &INTGRVAR;  SET LNNOTE ILNNOTE;  ...
# ============================================================================
print("\nStep 5: Building LOAN (date parsing, PAYEFF, FULRELDTE, NOOFAC)...")


def _decode_z11_mmddyyyy(raw):
    """MDY(INPUT(SUBSTR(PUT(val,Z11.),1,2),2.), SUBSTR(...,3,2), SUBSTR(...,5,4))
    -- the raw numeric value's first 8 (of 11 zero-padded) digits encode
    MM(2) DD(2) YYYY(4); any trailing digits (positions 9-11) are unused by
    this derivation, exactly as in the original SAS."""
    if raw is None:
        return None
    s = f"{int(raw):011d}"
    mm, dd, yyyy = int(s[0:2]), int(s[2:4]), int(s[4:8])
    return date(yyyy, mm, dd)


def _decode_payeff(payeffdt):
    """PAYEFF = SUBSTR(Z11.,10,2)||'/'||SUBSTR(Z11.,8,2)||'/'||SUBSTR(Z11.,3,2)
    -- built from fixed character positions of the Z11.-padded PAYEFFDT
    value; positions are replicated verbatim without inferring a semantic
    date meaning for them (INPUT(...,$2.) on an already-character SUBSTR
    result is just a character read, i.e. a no-op)."""
    if payeffdt is None:
        return None
    s = f"{int(payeffdt):011d}"
    return f"{s[9:11]}/{s[7:9]}/{s[2:4]}"


def _decode_fulreldte(freleas):
    """IF FRELEAS NOT IN (.,0) THEN FULRELDTE = INPUT(SUBSTR(Z11.,1,8),MMDDYY8.);
    MMDDYY8. reads an unpunctuated MM(2)DD(2)YYYY(4) 8-character field."""
    if freleas is None or freleas == 0:
        return None
    s = f"{int(freleas):011d}"
    mm, dd, yyyy = int(s[0:2]), int(s[2:4]), int(s[4:8])
    return date(yyyy, mm, dd)


loan_rows = []
for r in (lnnote_pbb_rows + lnnote_pibb_rows):
    # &INTGRVAR KEEP list (BLDATE appears twice in the original SAS macro
    # variable -- harmless duplicate, KEEP= just lists variable names).
    loan_rows.append({
        "LOANTYPE": r.get("LOANTYPE"), "NTBRCH": r.get("NTBRCH"), "ORGTYPE": r.get("ORGTYPE"),
        "ACCTNO": r.get("ACCTNO"), "CURBAL": r.get("CURBAL"), "NOTENO": r.get("NOTENO"),
        "NAME": r.get("NAME"), "APPRLIMT": r.get("APPRLIMT"),
        "ISSDTE": _decode_z11_mmddyyyy(r.get("ISSUEDT")),
        "PAIDIND": r.get("PAIDIND"), "BLDATE": r.get("BLDATE"), "BILPAY": r.get("BILPAY"),
        "PAYAMT": r.get("PAYAMT"), "INTRATE": r.get("INTRATE"), "STAFFNO": r.get("STAFFNO"),
        "PAYEFF": _decode_payeff(r.get("PAYEFFDT")), "ORGBAL": r.get("ORGBAL"),
        "LASTTRAN": _decode_z11_mmddyyyy(r.get("LASTTRAN")),
        "LSTTRNAM": r.get("LSTTRNAM"), "LSTTRNCD": r.get("LSTTRNCD"), "NOOFAC": 1,
        "RESTIND": r.get("RESTIND"), "FLAG1": r.get("FLAG1"),
        "FULRELDTE": _decode_fulreldte(r.get("FRELEAS")),
    })

# PROC SORT DATA=LOAN OUT=LOAN; BY ACCTNO NOTENO;
loan_rows.sort(key=lambda r: (r["ACCTNO"], r["NOTENO"]))
print(f"  LOAN rows: {len(loan_rows):,}")

# ============================================================================
# STEP 6: LNSETTLE  -- RPT: SETTLED A/C FOR THE WEEK
# ============================================================================
print("\nStep 6: Building LNSETTLE (settled accounts for the week)...")

lnsettle_rows = []
for r in loan_rows:
    lasttran = r["LASTTRAN"]
    if (r.get("PAIDIND") in ("P", "C") and lasttran is not None
            and STRDAY <= lasttran.day <= REPTDAY
            and lasttran.month == REPTMTH
            and lasttran.year == REPTYEAR):
        row = dict(r)
        row["SETTDT"] = row.pop("LASTTRAN")
        row["SETTAMT"] = row.pop("LSTTRNAM")
        row["SETTCD"] = row.pop("LSTTRNCD")
        row["LSTRNDSC"] = "LAST TRANCODE EQ 652" if r.get("LSTTRNCD") == 652 else "LAST TRANCODE NE 652"
        lnsettle_rows.append(row)

lnsettle_for_report = sorted(lnsettle_rows, key=lambda r: (r["LSTRNDSC"], r["LOANTYPE"], r["NTBRCH"]))
print(f"  LNSETTLE rows: {len(lnsettle_for_report):,}")

# ============================================================================
# REPORT RENDERING HELPERS  (shared column-buffer helpers, per project
# convention: fixed-width ASA output uses _put_buf/_place/_line helpers)
# ============================================================================
def _new_line(width):
    return [" "] * width


def _place(buf, col, text):
    for i, ch in enumerate(str(text)):
        pos = col - 1 + i
        if 0 <= pos < len(buf):
            buf[pos] = ch


def _line_str(buf):
    return "".join(buf)


def _center(text, width):
    text = str(text)[:width]
    pad = width - len(text)
    left = pad // 2
    return " " * left + text + " " * (pad - left)


def _fmt_num(value, width, decimals):
    """COMMAw.d with comma-drop-on-overflow; a genuinely absent cell (no
    contributing rows) renders as a bare '0'."""
    if value is None:
        return "0".rjust(width)
    v = float(value)
    s = f"{v:,.{decimals}f}"
    if len(s) > width:
        s = f"{v:.{decimals}f}"
    if len(s) > width:
        s = s[-width:]
    return s.rjust(width)


def _fmt_plain_int(value, width):
    if value is None:
        return "".rjust(width)
    return str(int(value)).rjust(width)


# ============================================================================
# STEP 7: RENDER LNSETTLE REPORT
# PROC REPORT DATA=LNSETTLE ...; COLUMN LSTRNDSC LOANTYPE NTBRCH ORGTYPE
#   STAFFNO ACCTNO NOTENO NAME APPRLIMT SETTDT SETTAMT PAYAMT SETTCD NOOFAC;
# Column start positions below are calibrated to the explicit @col anchors
# given in the SAS source's LINE statements (@016, @028, @070, @096, @110)
# using the declared column WIDTH/FORMAT widths with 2-space gaps between
# columns (one gap of 4 immediately before APPRLIMT, matching the source's
# @070 anchor exactly). Header/data-row positions are algorithmically
# derived; break/total-line positions are taken verbatim from the source.
# ============================================================================
SETTLE_COLS = [
    ("LOANTYPE", 1, 3), ("NTBRCH", 6, 3), ("ORGTYPE", 11, 3), ("STAFFNO", 16, 5),
    ("ACCTNO", 23, 10), ("NOTENO", 35, 5), ("NAME", 42, 24), ("APPRLIMT", 70, 14),
    ("SETTDT", 86, 8), ("SETTAMT", 96, 12), ("PAYAMT", 110, 12), ("SETTCD", 124, 3),
]
SETTLE_WIDTH = 128
SETTLE_HEADER = {
    "LOANTYPE": ["FAC"], "NTBRCH": ["BRH", "CDE"], "ORGTYPE": ["ORG", "TYP"],
    "STAFFNO": ["STAFF", "NUM"], "ACCTNO": ["A/C NO"], "NOTENO": ["NOTE", "NO"],
    "NAME": ["NAME"], "APPRLIMT": ["APPROVED", "LIMIT"], "SETTDT": ["PAID", "DATE"],
    "SETTAMT": ["SETTLEMENT", "AMOUNT"], "PAYAMT": ["MONTHLY", "REPAYMENT"],
    "SETTCD": ["LST", "TRN", "CDE"],
}


def _settle_title_lines():
    return [
        "REPORT ID : EIIWSTAF (PIBB)",
        f"WEEKLY REPORT FOR STAFF PAID LOAN LIST AS AT {RDATE}",
        "",
    ]


def _render_settle_report(rows):
    lines = []
    if not rows:
        return lines

    state = {"first_page": True, "lines_on_page": 0}

    def _emit_header(new_page):
        block = []
        if new_page:
            if not state["first_page"]:
                block.append(FF)
            state["first_page"] = False
            block.extend(_settle_title_lines())
        header_h = max(len(v) for v in SETTLE_HEADER.values())
        bufs = [_new_line(SETTLE_WIDTH) for _ in range(header_h)]
        for col, start, width in SETTLE_COLS:
            seg = SETTLE_HEADER[col]
            for i in range(header_h):
                _place(bufs[i], start, _center(seg[i] if i < len(seg) else "", width))
        block.extend(_line_str(b) for b in bufs)
        block.append(_line_str(_new_line(SETTLE_WIDTH)))  # HEADSKIP
        lines.extend(block)
        state["lines_on_page"] = 0

    _emit_header(new_page=True)

    def _zero_acc():
        return {"NOOFAC": 0, "APPRLIMT": 0.0, "SETTAMT": 0.0, "PAYAMT": 0.0}

    acc_ntbrch, acc_loantype, acc_lstrndsc, acc_grand = (_zero_acc() for _ in range(4))

    def _accum(acc, r):
        acc["NOOFAC"] += r.get("NOOFAC") or 0
        acc["APPRLIMT"] += r.get("APPRLIMT") or 0.0
        acc["SETTAMT"] += r.get("SETTAMT") or 0.0
        acc["PAYAMT"] += r.get("PAYAMT") or 0.0

    def _emit_ntbrch_break():
        buf = _new_line(SETTLE_WIDTH)
        _place(buf, 8, "-" * 121)
        lines.append(_line_str(buf))
        buf = _new_line(SETTLE_WIDTH)
        _place(buf, 16, "NO OF A/C :")
        _place(buf, 28, _fmt_num(acc_ntbrch["NOOFAC"], 8, 0))
        _place(buf, 70, _fmt_num(acc_ntbrch["APPRLIMT"], 14, 2))
        _place(buf, 96, _fmt_num(acc_ntbrch["SETTAMT"], 12, 2))
        _place(buf, 110, _fmt_num(acc_ntbrch["PAYAMT"], 12, 2))
        lines.append(_line_str(buf))
        lines.append(_line_str(_new_line(SETTLE_WIDTH)))

    def _emit_loantype_break():
        lines.append("-" * SETTLE_WIDTH)
        buf = _new_line(SETTLE_WIDTH)
        _place(buf, 1, "FAC TOTAL")
        _place(buf, 16, "NO OF A/C :")
        _place(buf, 28, _fmt_num(acc_loantype["NOOFAC"], 8, 0))
        _place(buf, 70, _fmt_num(acc_loantype["APPRLIMT"], 14, 2))
        _place(buf, 96, _fmt_num(acc_loantype["SETTAMT"], 12, 2))
        _place(buf, 110, _fmt_num(acc_loantype["PAYAMT"], 12, 2))
        lines.append(_line_str(buf))
        lines.append("-" * SETTLE_WIDTH)
        lines.append(_line_str(_new_line(SETTLE_WIDTH)))

    def _emit_lstrndsc_break():
        lines.append("=" * SETTLE_WIDTH)
        buf = _new_line(SETTLE_WIDTH)
        _place(buf, 1, "SUB TOTAL")
        _place(buf, 16, "NO OF A/C :")
        _place(buf, 28, _fmt_num(acc_lstrndsc["NOOFAC"], 8, 0))
        _place(buf, 70, _fmt_num(acc_lstrndsc["APPRLIMT"], 14, 2))
        _place(buf, 96, _fmt_num(acc_lstrndsc["SETTAMT"], 12, 2))
        _place(buf, 110, _fmt_num(acc_lstrndsc["PAYAMT"], 12, 2))
        lines.append(_line_str(buf))
        lines.append("=" * SETTLE_WIDTH)
        lines.append(_line_str(_new_line(SETTLE_WIDTH)))

    prev_lstrndsc = prev_loantype = prev_ntbrch = None

    for r in rows:
        lstrndsc, loantype, ntbrch = r.get("LSTRNDSC"), r.get("LOANTYPE"), r.get("NTBRCH")

        if prev_ntbrch is not None and (lstrndsc, loantype, ntbrch) != (prev_lstrndsc, prev_loantype, prev_ntbrch):
            _emit_ntbrch_break()
            acc_ntbrch = _zero_acc()
        if prev_loantype is not None and (lstrndsc, loantype) != (prev_lstrndsc, prev_loantype):
            _emit_loantype_break()
            acc_loantype = _zero_acc()
        if prev_lstrndsc is not None and lstrndsc != prev_lstrndsc:
            _emit_lstrndsc_break()
            acc_lstrndsc = _zero_acc()

        if state["lines_on_page"] >= PAGE_SIZE:
            _emit_header(new_page=True)
            prev_lstrndsc = prev_loantype = prev_ntbrch = None

        show_loantype = loantype != prev_loantype
        show_ntbrch = show_loantype or (ntbrch != prev_ntbrch)

        buf = _new_line(SETTLE_WIDTH)
        if show_loantype:
            _place(buf, 1, _fmt_plain_int(loantype, 3))
        if show_ntbrch:
            _place(buf, 6, _fmt_plain_int(ntbrch, 3))
        _place(buf, 11, str(r.get("ORGTYPE") or "")[:3])
        _place(buf, 16, _fmt_plain_int(r.get("STAFFNO"), 5))
        _place(buf, 23, _fmt_plain_int(r.get("ACCTNO"), 10))
        _place(buf, 35, _fmt_plain_int(r.get("NOTENO"), 5))
        _place(buf, 42, str(r.get("NAME") or "")[:24])
        _place(buf, 70, _fmt_num(r.get("APPRLIMT"), 14, 2))
        settdt = r.get("SETTDT")
        _place(buf, 86, settdt.strftime("%d/%m/%y") if settdt else "")
        _place(buf, 96, _fmt_num(r.get("SETTAMT"), 12, 2))
        _place(buf, 110, _fmt_num(r.get("PAYAMT"), 12, 2))
        _place(buf, 124, _fmt_plain_int(r.get("SETTCD"), 3))
        lines.append(_line_str(buf))
        state["lines_on_page"] += 1

        _accum(acc_ntbrch, r); _accum(acc_loantype, r); _accum(acc_lstrndsc, r); _accum(acc_grand, r)
        prev_lstrndsc, prev_loantype, prev_ntbrch = lstrndsc, loantype, ntbrch

    _emit_ntbrch_break()
    _emit_loantype_break()
    _emit_lstrndsc_break()

    buf = _new_line(SETTLE_WIDTH)
    _place(buf, 1, "GRAND TOTAL ")
    _place(buf, 16, "NO OF A/C : ")
    _place(buf, 28, _fmt_num(acc_grand["NOOFAC"], 8, 0))
    _place(buf, 70, _fmt_num(acc_grand["APPRLIMT"], 14, 2))
    _place(buf, 96, _fmt_num(acc_grand["SETTAMT"], 12, 2))
    _place(buf, 110, _fmt_num(acc_grand["PAYAMT"], 12, 2))
    lines.append(_line_str(buf))
    lines.append("=" * SETTLE_WIDTH)
    return lines


print("\nStep 7: Rendering LNSETTLE (PAID LOAN LIST) report...")
settle_report_lines = _render_settle_report(lnsettle_for_report)

# ============================================================================
# STEP 8: HIST (ISBASE dedup) + LNRELES / LNRELS1
# ============================================================================
print("\nStep 8: Building LNRELES / LNRELS1 (release/migration accounts)...")

con = duckdb.connect(database=":memory:")
isbase_pl = con.execute(f"SELECT * FROM read_parquet('{ISBASE_CACHE.as_posix()}')").pl()
con.close()

# PROC SORT DATA=LNHIST.ISBASE OUT=HIST NODUPKEY; BY ACCTNO NOTENO;
hist_pl = isbase_pl.sort(["ACCTNO", "NOTENO"]).unique(subset=["ACCTNO", "NOTENO"], keep="first")
hist_rows = hist_pl.to_dicts()
print(f"  HIST (deduped ISBASE) rows: {len(hist_rows):,}")

merged_hist = _sas_merge_retain(loan_rows, hist_rows, key_fields=("ACCTNO", "NOTENO"))

lnreles_rows = []
for r in merged_hist:
    if not (r["_IN_LEFT"] and not r["_IN_RIGHT"]):
        continue  # IF A AND NOT B;
    nmn = None
    issdte = r.get("ISSDTE")
    fulreldte = r.get("FULRELDTE")
    if (fulreldte is not None and PDATE <= fulreldte <= EDATE
            and r.get("FLAG1") == "M" and r.get("RESTIND") == "M"):
        nmn = "Y"
        issdte = fulreldte
    if not ((issdte is not None and PDATE <= issdte <= EDATE) or nmn == "Y"):
        continue
    # commented-out alternative filter in the original SAS (day/month/year
    # range check on ISSDTE) preserved as dead code below:
    # IF DAY(ISSDTE) GE &STRDAY AND DAY(ISSDTE) LE &REPTDAY
    # IF     MONTH(ISSDTE) EQ &REPTMTH
    #    AND YEAR(ISSDTE) EQ &REPTYEAR;
    row = dict(r)
    row["ISSDTE"] = issdte
    row["NMN"] = nmn
    row["NWI"] = "Y"
    lnreles_rows.append(row)

print(f"  LNRELES rows: {len(lnreles_rows):,}")

# PROC APPEND DATA=LNRELS1 BASE=LNHIST.ISBASE;  (LNRELS1 KEEP=ACCTNO NOTENO)
lnrels1_rows = [{"ACCTNO": r["ACCTNO"], "NOTENO": r["NOTENO"]} for r in lnreles_rows]


def _append_isbase(cache_path: Path, existing_pl: pl.DataFrame, new_rows: list) -> None:
    """Models PROC APPEND DATA=LNRELS1 BASE=LNHIST.ISBASE by appending the
    newly-released ACCTNO/NOTENO keys onto the cached ISBASE Parquet, so
    future runs' HIST dedup will exclude accounts already reported as
    released. The underlying isbase.sas7bdat source is not itself
    rewritten by this Python program."""
    if not new_rows:
        print("  PROC APPEND equivalent: no new LNRELS1 rows to append.")
        return
    existing = existing_pl.select(["ACCTNO", "NOTENO"]) if existing_pl.height else pl.DataFrame(
        {"ACCTNO": [], "NOTENO": []}
    )
    combined = pl.concat([existing, pl.DataFrame(new_rows)])
    combined.write_parquet(cache_path)
    print(f"  PROC APPEND equivalent: appended {len(new_rows):,} rows to "
          f"{cache_path.name} (total rows now {combined.height:,}).")


_append_isbase(ISBASE_CACHE, isbase_pl, lnrels1_rows)

# PROC SORT DATA=LNRELES OUT=LNRELES; BY ACCTNO; RUN;
# PROC SORT DATA=LNSETTLE OUT=LNSETTLE; BY ACCTNO; RUN;
lnreles_by_acct = sorted(lnreles_rows, key=lambda r: r["ACCTNO"])
lnsettle_by_acct = sorted(lnsettle_rows, key=lambda r: r["ACCTNO"])

# ============================================================================
# STEP 9: DATA LNRPT1A LNRPT1B; MERGE LNSETTLE(IN=B) LNRELES(IN=A); BY ACCTNO;
# ============================================================================
print("\nStep 9: Splitting into LNRPT1A (new loan) / LNRPT1B (migration)...")

merged_acct = _sas_merge_retain(lnsettle_by_acct, lnreles_by_acct, key_fields=("ACCTNO",))

lnrpt1a_rows, lnrpt1b_rows = [], []
for r in merged_acct:
    in_b = r["_IN_LEFT"]   # LNSETTLE's IN=B
    in_a = r["_IN_RIGHT"]  # LNRELES's IN=A

    if in_a and not in_b:
        if r.get("NMN") == "Y":
            lnrpt1b_rows.append(dict(r))
        else:
            lnrpt1a_rows.append(dict(r))

    if in_a and in_b:
        orgbal, settamt = r.get("ORGBAL"), r.get("SETTAMT")
        if (orgbal == settamt) or (r.get("NMN") == "Y"):
            lnrpt1b_rows.append(dict(r))
        if orgbal == settamt:
            continue  # DELETE; -- skips the NWI/PAIDIND check below
        if r.get("NWI") == "Y" and r.get("PAIDIND") != "P":
            lnrpt1a_rows.append(dict(r))

print(f"  LNRPT1A rows: {len(lnrpt1a_rows):,}   LNRPT1B rows: {len(lnrpt1b_rows):,}")

# ============================================================================
# STEP 10: DATA LNPAY; SET PAY.LNPAY&NOWK IPAY.ILNPAY&NOWK; WHERE PAYAMT NE 0;
# ============================================================================
print("\nStep 10: Building LNPAY (weekly payment schedule)...")

con = duckdb.connect(database=":memory:")
# WHERE PAYAMT NE 0 -- SAS missing PAYAMT is NOT EQUAL to 0, so missing
# rows are KEPT; COALESCE guards this SAS missing-value semantic (project
# convention).
lnpay_pbb = con.execute(f"""
    SELECT ACCTNO, NOTENO, EFFDATE, PAYAMT
    FROM read_parquet('{LNPAY_PBB_CACHE.as_posix()}')
    WHERE COALESCE(PAYAMT, -1) != 0
""").pl().to_dicts()
lnpay_pibb = con.execute(f"""
    SELECT ACCTNO, NOTENO, EFFDATE, PAYAMT
    FROM read_parquet('{LNPAY_PIBB_CACHE.as_posix()}')
    WHERE COALESCE(PAYAMT, -1) != 0
""").pl().to_dicts()
con.close()

lnpay_rows = []
for r in (lnpay_pbb + lnpay_pibb):
    effdate = r.get("EFFDATE")
    # PAYEFFDD is a constant 99; PAYEFF = LEFT(COMPBL(PAYEFFDD||'/'||PAYEFFMM||'/'||PAYEFFYY))
    # -- with a constant 2-digit PAYEFFDD and no embedded blanks, LEFT/COMPBL
    # reduce to a plain "99/MM/YY" string.
    payeff = f"99/{effdate.month:02d}/{effdate.strftime('%y')}" if effdate else None
    lnpay_rows.append({"ACCTNO": r.get("ACCTNO"), "NOTENO": r.get("NOTENO"),
                        "PAYEFF": payeff, "PAYAMT": r.get("PAYAMT")})

# PROC SORT DATA=LNPAY; BY ACCTNO NOTENO PAYEFF;
# PROC SORT DATA=LNPAY NODUPKEY; BY ACCTNO NOTENO;
lnpay_rows.sort(key=lambda r: (r["ACCTNO"], r["NOTENO"], r["PAYEFF"] or ""))
seen_keys = set()
lnpay_dedup = []
for r in lnpay_rows:
    k = (r["ACCTNO"], r["NOTENO"])
    if k in seen_keys:
        continue
    seen_keys.add(k)
    lnpay_dedup.append(r)
print(f"  LNPAY rows (deduped): {len(lnpay_dedup):,}")

# ============================================================================
# STEP 11: DATA LNRPT1B LNRPT1C; MERGE LNRPT1B(IN=A) LNPAY; BY ACCTNO NOTENO; IF A;
# PAYEFF/PAYAMT come from LNPAY when matched (listed second -> overwrites);
# when unmatched they are RETAINED from whatever LNPAY row was last read
# globally -- a genuine SAS MERGE quirk, preserved via _sas_merge_retain.
# ============================================================================
print("\nStep 11: Splitting LNRPT1B into LNRPT1B (migration) / LNRPT1C (full release)...")

lnrpt1b_sorted = sorted(lnrpt1b_rows, key=lambda r: (r["ACCTNO"], r["NOTENO"]))
merged_paysched = _sas_merge_retain(lnrpt1b_sorted, lnpay_dedup, key_fields=("ACCTNO", "NOTENO"))

lnrpt1b_final, lnrpt1c_rows = [], []
for r in merged_paysched:
    if not r["_IN_LEFT"]:
        continue  # IF A;
    if r.get("NMN") == "Y":
        lnrpt1c_rows.append(r)
    else:
        lnrpt1b_final.append(r)

print(f"  LNRPT1B (final) rows: {len(lnrpt1b_final):,}   LNRPT1C rows: {len(lnrpt1c_rows):,}")

# ============================================================================
# STEP 12: RENDER LNRPT1A / LNRPT1B / LNRPT1C  (shared column layout)
# COLUMN LOANTYPE NTBRCH ORGTYPE STAFFNO ACCTNO NOTENO NAME APPRLIMT ISSDTE
#        PAYEFF PAYAMT INTRATE NOOFAC;
# Column positions calibrated to the explicit @col anchors in the SAS
# source (@014, @026, @073, @110) the same way as the LNSETTLE report.
# ============================================================================
NEWLOAN_COLS = [
    ("LOANTYPE", 1, 3), ("NTBRCH", 6, 4), ("ORGTYPE", 12, 4), ("STAFFNO", 18, 6),
    ("ACCTNO", 26, 10), ("NOTENO", 38, 5), ("NAME", 45, 24), ("APPRLIMT", 73, 14),
    ("ISSDTE", 89, 8), ("PAYEFF", 99, 9), ("PAYAMT", 110, 14), ("INTRATE", 126, 5),
]
NEWLOAN_WIDTH = 130
NEWLOAN_HEADER = {
    "LOANTYPE": ["FAC"], "NTBRCH": ["BR", "CODE"], "ORGTYPE": ["ORG.", "TYPE"],
    "STAFFNO": ["EMP.NO"], "ACCTNO": ["A/C NO"], "NOTENO": ["NOTE", "NO"],
    "NAME": ["NAME"], "APPRLIMT": ["APPROVED", "LIMIT"], "ISSDTE": ["ISSUE", "DATE"],
    "PAYEFF": ["PAYMENT", "EFF. DATE"], "PAYAMT": ["PAYMENT", "AMOUNT"],
    "INTRATE": ["INT.", "RATE"],
}


def _newloan_title_lines(title2_text):
    return [
        "REPORT ID : EIIWSTAF (PIBB)",
        f"WEEKLY REPORT FOR STAFF {title2_text} LOAN LIST AS AT {RDATE}",
        "",
    ]


def _render_newloan_report(rows, title2_text):
    lines = []
    if not rows:
        return lines

    state = {"first_page": True, "lines_on_page": 0}

    def _emit_header(new_page):
        block = []
        if new_page:
            if not state["first_page"]:
                block.append(FF)
            state["first_page"] = False
            block.extend(_newloan_title_lines(title2_text))
        header_h = max(len(v) for v in NEWLOAN_HEADER.values())
        bufs = [_new_line(NEWLOAN_WIDTH) for _ in range(header_h)]
        for col, start, width in NEWLOAN_COLS:
            seg = NEWLOAN_HEADER[col]
            for i in range(header_h):
                _place(bufs[i], start, _center(seg[i] if i < len(seg) else "", width))
        block.extend(_line_str(b) for b in bufs)
        block.append(_line_str(_new_line(NEWLOAN_WIDTH)))  # HEADSKIP
        lines.extend(block)
        state["lines_on_page"] = 0

    _emit_header(new_page=True)

    def _zero_acc():
        return {"NOOFAC": 0, "APPRLIMT": 0.0, "PAYAMT": 0.0}

    acc_ntbrch, acc_loantype, acc_grand = (_zero_acc() for _ in range(3))

    def _accum(acc, r):
        acc["NOOFAC"] += r.get("NOOFAC") or 0
        acc["APPRLIMT"] += r.get("APPRLIMT") or 0.0
        acc["PAYAMT"] += r.get("PAYAMT") or 0.0

    def _emit_ntbrch_break():
        buf = _new_line(NEWLOAN_WIDTH)
        _place(buf, 8, "-" * 123)
        lines.append(_line_str(buf))
        buf = _new_line(NEWLOAN_WIDTH)
        _place(buf, 14, "NO OF A/C :")
        _place(buf, 26, _fmt_num(acc_ntbrch["NOOFAC"], 8, 0))
        _place(buf, 73, _fmt_num(acc_ntbrch["APPRLIMT"], 14, 2))
        _place(buf, 110, _fmt_num(acc_ntbrch["PAYAMT"], 14, 2))
        lines.append(_line_str(buf))

    def _emit_loantype_break():
        lines.append("-" * NEWLOAN_WIDTH)
        buf = _new_line(NEWLOAN_WIDTH)
        _place(buf, 1, "SUB TOTAL")
        _place(buf, 14, "NO OF A/C :")
        _place(buf, 26, _fmt_num(acc_loantype["NOOFAC"], 8, 0))
        _place(buf, 73, _fmt_num(acc_loantype["APPRLIMT"], 14, 2))
        _place(buf, 110, _fmt_num(acc_loantype["PAYAMT"], 14, 2))
        lines.append(_line_str(buf))
        lines.append("-" * NEWLOAN_WIDTH)

    prev_loantype = prev_ntbrch = None

    for r in rows:
        loantype, ntbrch = r.get("LOANTYPE"), r.get("NTBRCH")

        if prev_ntbrch is not None and (loantype, ntbrch) != (prev_loantype, prev_ntbrch):
            _emit_ntbrch_break()
            acc_ntbrch = _zero_acc()
        if prev_loantype is not None and loantype != prev_loantype:
            _emit_loantype_break()
            acc_loantype = _zero_acc()

        if state["lines_on_page"] >= PAGE_SIZE:
            _emit_header(new_page=True)
            prev_loantype = prev_ntbrch = None

        show_loantype = loantype != prev_loantype
        show_ntbrch = show_loantype or (ntbrch != prev_ntbrch)

        buf = _new_line(NEWLOAN_WIDTH)
        if show_loantype:
            _place(buf, 1, _fmt_plain_int(loantype, 3))
        if show_ntbrch:
            _place(buf, 6, _fmt_plain_int(ntbrch, 3).rjust(4))
        _place(buf, 12, str(r.get("ORGTYPE") or "")[:4])
        _place(buf, 18, _fmt_plain_int(r.get("STAFFNO"), 6))
        _place(buf, 26, _fmt_plain_int(r.get("ACCTNO"), 10))
        _place(buf, 38, _fmt_plain_int(r.get("NOTENO"), 5))
        _place(buf, 45, str(r.get("NAME") or "")[:24])
        _place(buf, 73, _fmt_num(r.get("APPRLIMT"), 14, 2))
        issdte = r.get("ISSDTE")
        _place(buf, 89, issdte.strftime("%d/%m/%y") if issdte else "")
        _place(buf, 99, str(r.get("PAYEFF") or "")[:9])
        _place(buf, 110, _fmt_num(r.get("PAYAMT"), 14, 2))
        intrate = r.get("INTRATE")
        _place(buf, 126, f"{intrate:.2f}" if intrate is not None else "")
        lines.append(_line_str(buf))
        state["lines_on_page"] += 1

        _accum(acc_ntbrch, r); _accum(acc_loantype, r); _accum(acc_grand, r)
        prev_loantype, prev_ntbrch = loantype, ntbrch

    _emit_ntbrch_break()
    _emit_loantype_break()

    lines.append("-" * NEWLOAN_WIDTH)
    buf = _new_line(NEWLOAN_WIDTH)
    _place(buf, 1, "GRAND TOTAL ")
    _place(buf, 14, "NO OF A/C : ")
    _place(buf, 26, _fmt_num(acc_grand["NOOFAC"], 8, 0))
    _place(buf, 73, _fmt_num(acc_grand["APPRLIMT"], 14, 2))
    _place(buf, 110, _fmt_num(acc_grand["PAYAMT"], 14, 2))
    lines.append(_line_str(buf))
    lines.append("-" * NEWLOAN_WIDTH)
    return lines


print("\nStep 12: Rendering LNRPT1A / LNRPT1B / LNRPT1C reports...")
# PROC SORT ... BY LOANTYPE NTBRCH; for each of the three report sources
lnrpt1a_sorted = sorted(lnrpt1a_rows, key=lambda r: (r["LOANTYPE"], r["NTBRCH"]))
lnrpt1b_sorted_final = sorted(lnrpt1b_final, key=lambda r: (r["LOANTYPE"], r["NTBRCH"]))
lnrpt1c_sorted = sorted(lnrpt1c_rows, key=lambda r: (r["LOANTYPE"], r["NTBRCH"]))

newloan_reports = [
    _render_newloan_report(lnrpt1a_sorted, "NEW"),
    _render_newloan_report(lnrpt1b_sorted_final, "MIGRATION"),
    _render_newloan_report(lnrpt1c_sorted, "FULL RELEASE"),
]

# ============================================================================
# STEP 13: WRITE OUTPUT (all four reports feed the same SASLIST listing)
# ============================================================================
print("\nStep 13: Writing output...")

report_lines = list(settle_report_lines)
for rep in newloan_reports:
    if rep:
        report_lines.append(FF)
        report_lines.extend(rep)

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")
print("\nEIIWSTAF complete.")
