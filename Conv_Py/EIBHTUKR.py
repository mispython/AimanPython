#!/usr/bin/env python3
"""
Program : EIBHTUKR.py
Purpose : CGC/TUK Loans (Product 521/522/523/528) - Half-Yearly Interest
          Accrued/Paid Extraction and Outstanding Loans Reports.

Dependency:
    %INC PGM(PBBELF);
        PBBELF.py IS available in this project, but none of its exported
        lookups/formats (format_brchcd, CACBRCH_MAP, REGIOFF_MAP, CTYPE_MAP,
        etc.) are actually referenced anywhere in the EIBHTUKR SAS source
        body -- the %INC is a dead/no-op format-library include in the
        original program. It is therefore intentionally NOT imported here
        (it would have zero effect on this program's output).
    %INC PGM(EIFPCFMT); /* SHARING THE FORMAT $STATE. */
        -> from EIFPCFMT import state_format
        Used for `FORMAT ... STATE $STATE.;` in report -1 and report -5
        (PROC PRINT of CDRLNTUK / TUK2).

============================================================================
PHYSICAL INPUT DATASETS
============================================================================
1. BRHFILE  (JCL DD DSN=RBP2.B033.PBB.BRANCH)
   Fixed-width TEXT flat file (NOT sas7bdat) -> read directly, line-sliced;
   no Parquet caching applied (project convention: only sas7bdat inputs are
   parquet-cached).
   File   : INPUT_BRHFILE -> branch.txt
   Layout (1-indexed, per `INFILE BRHFILE LRECL=80; INPUT @02 BRANCH 3.
           @06 BRH $3. @12 BRHNAME $25. @45 STATE $1.;`)

2. RAW.LNNOTE  (JCL DD RAW DSN=SAP.PBB.MNILN(0), member LNNOTE)
   File   : INPUT_LNNOTE_FILE -> lnnote.sas7bdat  (Parquet-cached)
   Cols used : ACCTNO, NOTENO, LOANTYPE
   Filtered to LOANTYPE IN (521,522,523,528).

3. LOAN.LOAN&REPTMON&NOWK  (JCL DD LOAN DSN=SAP.PBB.SASDATA)
   SAS member name is built dynamically at runtime from REPTMON (report
   month, '06' or '12') and NOWK (report week digit '1'-'4', derived from
   day-of-month of REPTDATE) -- resolved below as loan_<reptmon><nowk>.
   File   : INPUT_LOAN_FILE (resolved at runtime) (Parquet-cached)
   Cols used : BRANCH ACCTNO NAME PRODUCT HSTINT BALANCE NOTENO CGCREF
               CUSTIDNO APPRLIMT BILTOT BLDATE HSTPRIN ACCTYIND
   Filtered to PRODUCT IN (521,522,523,528).

//TEMP DD DSN=SAP.CDR.TEMP is a SAS *library* reference (libref TEMP) used
to hold intermediate/permanent working datasets (TEMP.CDRLNTUK, TEMP.CDRTUK)
-- it is NOT a physical report/flat file (its DCB RECFM=FS LRECL=27648 is
just the SAS library page format). These are kept purely as in-memory
Python structures here, never serialised to disk.

============================================================================
OUTPUT
============================================================================
//SASLIST DD SYSOUT=(,),OUTPUT=(*.PRINT1)  -- the printed report stream,
made up of 6 sequential report sections (PROC PRINT x3, PROC TABULATE x2,
one PUT-based custom report). RECFM is a print/SYSOUT stream, so the output
file below carries an ASA carriage-control byte per project convention.
PAGESIZE=65, LINESIZE=135 (OPTIONS PS=65 LS=135 in the original SYSIN).
File : OUTPUT_FILE -> EIBHTUKR.txt
"""

import gc
from pathlib import Path
from datetime import date, timedelta

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from EIFPCFMT import state_format

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_BRHFILE_DIR = STG_DIR                      # /sasdata/rawdata/lookup
INPUT_LNNOTE_DIR  = STG_DIR / "MNILN"
INPUT_LOAN_DIR    = STG_DIR / "mth_bnm"

INPUT_BRHFILE      = INPUT_BRHFILE_DIR / "LKP_BRANCH"
INPUT_LNNOTE_FILE  = INPUT_LNNOTE_DIR / "enrh_ln_note_m08.sas7bdat"
# INPUT_LOAN_FILE is resolved below once REPTMON / NOWK are known.

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBHTUKR"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000
PAGE_SIZE  = 65    # OPTIONS PS=65
LINE_SIZE  = 135   # OPTIONS LS=135

# ============================================================================
# STEP 1: DATA REPTDATE;  (report date derivation - no reptdate.parquet)
# ============================================================================
print("Step 1: Deriving report date...")


def _z(n: int, width: int) -> str:
    return str(n).zfill(width)


_today = date.today()
_first_of_month = date(_today.year, _today.month, 1)
REPTDATE = _first_of_month - timedelta(days=1)   # 1st of this month - 1 day

MTH = REPTDATE.month
LASTMTH = 6 if MTH == 12 else 12 if MTH == 6 else None

_day = REPTDATE.day
if _day == 8:
    SDD, WK, WK1 = 1, "1", "4"
elif _day == 15:
    SDD, WK, WK1 = 9, "2", "1"
elif _day == 22:
    SDD, WK, WK1 = 16, "3", "2"
else:
    SDD, WK, WK1 = 23, "4", "3"

NOWK     = WK
RDATE    = REPTDATE.strftime("%d/%m/%Y")            # PUT(REPTDATE,DDMMYY10.)
REPTMON  = _z(MTH, 2)                               # PUT(MTH,Z2.)
LREPTMON = _z(LASTMTH, 2) if LASTMTH else "  "      # PUT(LASTMTH,Z2.)
RYEAR    = _z(REPTDATE.year, 4)                     # PUT(YEAR(REPTDATE),Z4.)

if REPTMON == "06":
    FULLDATE = "1 JANUARY TO 30 JUNE "
elif REPTMON == "12":
    FULLDATE = "1 JULY TO 31 DECEMBER "
else:
    FULLDATE = ""

# INPUT_LOAN_FILE = INPUT_LOAN_DIR / f"loan{REPTMON}{NOWK}.sas7bdat"
INPUT_LOAN_FILE = INPUT_LOAN_DIR / f"loan084.sas7bdat"  # -> NEED TO CHANGE. Current is PIBB dataset. Columns not available

# Generate time stamp
report_date = date.today() - timedelta(days=1)
ts = report_date.strftime("%y%m%d")

OUTPUT_DIR  = BASE_DIR / "output" / "EIBHTUKR"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / f"EIBHTUKR_{ts}.txt"

print(f"  RDATE       : {RDATE}")
print(f"  REPTMON/NOWK: {REPTMON}/{NOWK}   RYEAR: {RYEAR}   FULLDATE: {FULLDATE.strip()}")
print(f"  LOAN input  : {INPUT_LOAN_FILE.name}")
print(f"  Output file : {OUTPUT_FILE.name}")

# ============================================================================
# PROC FORMAT EQUIVALENTS (local to this program)
# ============================================================================
_TUK_MAP = {
    521: "TUK 1-CODE 521",
    522: "TUK 2-CODE 522",
    523: "TUK 3-CODE 523",
    528: "TUK 4-CODE 528",
}


def tuk_format(code):
    """VALUE TUK. No OTHER clause -> unmatched codes return None."""
    return _TUK_MAP.get(code)


_COL_MAP = {
    "A": "TOTAL INTEREST",
    "B": "INT. TO CGC (2/6)",
    "C": "INT. TO CGC (1/6)",
    "D": "INT. TO INSTITUTION APPT BY CGC (4/6)",
}


def col_format(code):
    return _COL_MAP.get(code)


def _sas_round(x: float, scale: float = 0.01) -> float:
    """SAS ROUND(x, scale): round to nearest multiple of scale, halves away
    from zero."""
    if x is None:
        return None
    q = x / scale
    if q >= 0:
        q = int(q + 0.5)
    else:
        q = -int(-q + 0.5)
    return q * scale


def _fmt_comma(value, width: int, decimals: int) -> str:
    """COMMAw.d, MISSING='0' (OPTIONS MISSING='0'): a missing numeric
    prints as '0' (not blank), matching the SAS OPTIONS MISSING='0' set in
    this program. Commas are dropped if the value would overflow the
    field width, matching COMMAw. overflow behaviour."""
    if value is None:
        v = 0.0
    else:
        v = float(value)
    s = f"{v:,.{decimals}f}"
    if len(s) > width:
        s = f"{v:.{decimals}f}"
    if len(s) > width:
        s = "*" * width
    return s.rjust(width)


def _fmt_z(value, width: int) -> str:
    if value is None:
        value = 0
    return str(int(value)).zfill(width)


def _fmt_int(value, width: int) -> str:
    if value is None:
        value = 0
    return str(int(value)).rjust(width)


# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET (EIIMRM01.py pattern)
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
# STEP 2: CACHE INPUT SAS FILES TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
LNNOTE_CACHE = _load_cached(INPUT_LNNOTE_FILE, "LNNOTE")
LOAN_CACHE   = _load_cached(INPUT_LOAN_FILE, "LOAN")

# ============================================================================
# STEP 3: DATA BRHDATA;  (fixed-width flat file, read directly - no Parquet)
# ============================================================================
print("\nStep 3: Reading BRHDATA (branch reference flat file)...")

brhdata_rows = []
if INPUT_BRHFILE.exists():
    with open(INPUT_BRHFILE, "r", encoding="latin1") as fh:
        for line in fh:
            line = line.rstrip("\n").ljust(80)
            branch_s = line[1:4].strip()        # @02 BRANCH 3.
            brh      = line[5:8].strip()        # @06 BRH $3.
            brhname  = line[11:36].strip()      # @12 BRHNAME $25.
            state_c  = line[44:45].strip()      # @45 STATE $1.
            if not branch_s:
                continue
            brhdata_rows.append({
                "BRANCH": int(branch_s), "BRH": brh,
                "BRHNAME": brhname, "STATE": state_c,
            })
else:
    print(f"  WARNING: {INPUT_BRHFILE} not found - BRHDATA is empty.")

brhdata_by_branch = {r["BRANCH"]: r for r in brhdata_rows}
print(f"  BRHDATA rows: {len(brhdata_rows):,}")

# ============================================================================
# STEP 4: DATA RAW;  SET RAW.LNNOTE; IF LOANTYPE IN (521,522,523,528);
# ============================================================================
print("\nStep 4: Building RAW from RAW.LNNOTE...")

con = duckdb.connect(database=":memory:")
raw_pl = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(NOTENO   AS BIGINT)  AS NOTENO,
        CAST(LOANTYPE AS INTEGER) AS LOANTYPE
    FROM read_parquet('{LNNOTE_CACHE.as_posix()}')
    WHERE CAST(LOANTYPE AS INTEGER) IN (521,522,523,528)
""").pl()
con.close()

# PROC SORT DATA=RAW; BY ACCTNO NOTENO;
raw_by_key = {}
for r in raw_pl.iter_rows(named=True):
    raw_by_key[(r["ACCTNO"], r["NOTENO"])] = r["LOANTYPE"]

print(f"  RAW rows: {len(raw_pl):,}")
del raw_pl
gc.collect()

# ============================================================================
# STEP 5: DATA PROCESS;  SET REPTDATE LOAN.LOAN&REPTMON&NOWK;
#         IF _N_=1 THEN TODATE=REPTDATE; RETAIN TODATE;
#         IF PRODUCT IN (521,522,523,528);
# ============================================================================
print("\nStep 5: Building PROCESS from LOAN.LOAN&REPTMON&NOWK...")

con = duckdb.connect(database=":memory:")
process_pl = con.execute(f"""
    SELECT
        CAST(BRANCH   AS INTEGER) AS BRANCH,
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(NAME     AS VARCHAR) AS NAME,
        CAST(PRODUCT  AS INTEGER) AS PRODUCT,
        CAST(HSTINT   AS DOUBLE)  AS HSTINT,
        CAST(BALANCE  AS DOUBLE)  AS BALANCE,
        CAST(NOTENO   AS BIGINT)  AS NOTENO,
        CAST(CGCREF   AS VARCHAR) AS CGCREF,
        CAST(CUSTIDNO AS VARCHAR) AS CUSTIDNO,
        CAST(APPRLIMT AS DOUBLE)  AS APPRLIMT,
        CAST(BILTOT   AS DOUBLE)  AS BILTOT,
        CAST(BLDATE   AS DATE)    AS BLDATE,
        CAST(HSTPRIN  AS DOUBLE)  AS HSTPRIN,
        CAST(ACCTYIND AS INTEGER) AS ACCTYIND
    FROM read_parquet('{LOAN_CACHE.as_posix()}')
    WHERE CAST(PRODUCT AS INTEGER) IN (521,522,523,528)
""").pl()
con.close()

# RETAIN TODATE = REPTDATE (carried forward from the REPTDATE dataset's
# single row across every subsequent LOAN observation).
process_rows = []
for r in process_pl.iter_rows(named=True):
    rec = dict(r)
    rec["TODATE"] = REPTDATE
    process_rows.append(rec)

print(f"  PROCESS rows: {len(process_rows):,}")
del process_pl
gc.collect()

# ============================================================================
# STEP 6: DATA SELECTED;  MERGE RAW(IN=A) PROCESS(IN=B); BY ACCTNO NOTENO;
#         IF B;
# ============================================================================
print("\nStep 6: Building SELECTED (PROCESS left-joined to RAW)...")

selected_rows = []
for r in process_rows:
    rec = dict(r)
    rec["LOANTYPE"] = raw_by_key.get((r["ACCTNO"], r["NOTENO"]))
    selected_rows.append(rec)

print(f"  SELECTED rows: {len(selected_rows):,}")

# ============================================================================
# STEP 7: DATA TEMP.CDRLNTUK &TUK;  SET REPTDATE SELECTED;
#         IF PRODUCT IN (521,522,523,528);  ARREARNO CALC.
# ============================================================================
print("\nStep 7: Building TEMP.CDRLNTUK (ARREARNO calc)...")

TUK_KEEP = (
    "BRANCH", "ACCTNO", "NAME", "PRODUCT", "HSTINT", "BALANCE", "NOTENO",
    "CGCREF", "CUSTIDNO", "APPRLIMT", "BILTOT", "ARREARNO", "BLDATE",
    "TODATE", "HSTPRIN", "ACCTYIND",
)

cdrlntuk_temp = []
for r in selected_rows:
    if r["PRODUCT"] not in (521, 522, 523, 528):
        continue
    biltot = r["BILTOT"] or 0.0
    arrearno = 0
    if biltot > 0:
        days = (r["TODATE"] - r["BLDATE"]).days
        raw_arrear = days / 14
        if raw_arrear % 1 != 0:
            arrearno = int(days // 14) + 1
        else:
            arrearno = int(days // 14)
        # NOTE: IF ARREARNO=2.30 OR 2.01, ARREARNO IS 3 INSTALLMENTS
    rec = {k: r.get(k) for k in TUK_KEEP if k != "ARREARNO"}
    rec["ARREARNO"] = arrearno
    cdrlntuk_temp.append(rec)

# PROC SORT; BY BRANCH;  DATA TEMP.CDRLNTUK; MERGE TEMP.CDRLNTUK(IN=A)
# BRHDATA; BY BRANCH; IF A THEN OUTPUT;  (left join, attach BRH/BRHNAME/STATE)
for r in cdrlntuk_temp:
    b = brhdata_by_branch.get(r["BRANCH"])
    r["BRH"]     = b["BRH"] if b else None
    r["BRHNAME"] = b["BRHNAME"] if b else None
    r["STATE"]   = b["STATE"] if b else None

print(f"  TEMP.CDRLNTUK rows: {len(cdrlntuk_temp):,}")

# ============================================================================
# STEP 8: PROC SORT; BY STATE BRANCH CGCREF;
#         DATA CDRLNTUK; SET TEMP.CDRLNTUK; BY STATE BRANCH;
#         IF FIRST.STATE OR FIRST.BRANCH THEN NUM=0; NUM+1;
#         BRNAME=PUT(BRANCH,Z3.)||' ('||TRIM(BRHNAME)||')';
# ============================================================================
print("\nStep 8: Building CDRLNTUK (NUM/BRNAME per BRANCH group)...")

cdrlntuk_temp.sort(key=lambda r: (r["STATE"] or "", r["BRANCH"] or 0, r["CGCREF"] or ""))

cdrlntuk_rows = []
_num = 0
_prev_state = _prev_branch = object()
for r in cdrlntuk_temp:
    rec = dict(r)
    if rec["STATE"] != _prev_state or rec["BRANCH"] != _prev_branch:
        _num = 0
    _num += 1
    _prev_state, _prev_branch = rec["STATE"], rec["BRANCH"]
    rec["NUM"] = _num
    rec["BRNAME"] = f"{_fmt_z(rec['BRANCH'], 3)} ({(rec['BRHNAME'] or '').strip()})"
    cdrlntuk_rows.append(rec)

# PROC SORT DATA=CDRLNTUK; BY STATE BRNAME;
cdrlntuk_rows.sort(key=lambda r: (r["STATE"] or "", r["BRNAME"] or ""))

# PROC SORT DATA=TEMP.CDRLNTUK; BY ACCTNO;  (order irrelevant to downstream
# logic since TEMP.CDRTUK below is re-derived by BRANCH/PRODUCT again)
cdrlntuk_temp_by_acct = sorted(cdrlntuk_temp, key=lambda r: r["ACCTNO"] or 0)

print(f"  CDRLNTUK rows: {len(cdrlntuk_rows):,}")

# ============================================================================
# STEP 9: DATA TEMP.CDRTUK;  SET TEMP.CDRLNTUK;
#         PROC SORT; BY BRANCH;
#         DATA TEMP.CDRTUK; MERGE TEMP.CDRTUK(IN=A) BRHDATA; BY BRANCH;
#         IF A THEN OUTPUT;
#
# NOTE: TEMP.CDRLNTUK already carries BRH/BRHNAME/STATE from the identical
# BRHDATA merge in Step 7, so this second re-merge by BRANCH is idempotent
# (same key, same source table -> same resulting values). TEMP.CDRTUK is
# therefore built directly from TEMP.CDRLNTUK (via cdrlntuk_temp_by_acct)
# without repeating the no-op merge.
# ============================================================================
cdrtuk_source = [dict(r) for r in cdrlntuk_temp_by_acct]

# ============================================================================
# STEP 10: DATA CDRTUK&REPTMON;  SET TEMP.CDRTUK;
#          COL='A'/'B'/'C'/'D' expansion with ACCRBAL/ACCRTOT/INTBAL/INTTOT.
# ============================================================================
print("\nStep 10: Building CDRTUK<reptmon> (COL A/B/C/D expansion)...")

cdrtuk_rows = []
for r in cdrtuk_source:
    hstint = r["HSTINT"] or 0.0
    base = {"BRH": r["BRH"], "BRHNAME": r["BRHNAME"], "PRODUCT": r["PRODUCT"],
            "NAME": r["NAME"]}

    # COL='A': ACCRTOT/INTTOT are computed-only vars (not RETAINed and not
    # from SET), so they reset to missing every DATA-step iteration -> None
    # here, exactly as in the original SAS.
    cdrtuk_rows.append({**base, "COL": "A", "ACCRBAL": hstint, "ACCRTOT": None,
                         "INTBAL": hstint, "INTTOT": None})

    if r["PRODUCT"] in (521, 523, 528):
        val = _sas_round((2 / 6) * hstint)
        cdrtuk_rows.append({**base, "COL": "B", "ACCRBAL": val, "ACCRTOT": val,
                             "INTBAL": val, "INTTOT": val})
    elif r["PRODUCT"] == 522:
        val_c = _sas_round((1 / 6) * hstint)
        cdrtuk_rows.append({**base, "COL": "C", "ACCRBAL": val_c, "ACCRTOT": val_c,
                             "INTBAL": val_c, "INTTOT": val_c})
        val_d = _sas_round((4 / 6) * hstint)
        # ACCRTOT/INTTOT reset to 0 then left unassigned for COL='D' -> 0
        cdrtuk_rows.append({**base, "COL": "D", "ACCRBAL": val_d, "ACCRTOT": 0.0,
                             "INTBAL": val_d, "INTTOT": 0.0})

print(f"  CDRTUK<reptmon> rows: {len(cdrtuk_rows):,}")

# ============================================================================
# ASA REPORT WRITER
# ============================================================================
report_lines: list = []


def _emit(text: str, ctl: str = " "):
    """ctl: ' '=single space, '0'=skip one line first, '1'=new page,
    '+'=overprint (same line as previous)."""
    report_lines.append(ctl + text)


def _emit_blank(n: int = 1):
    for _ in range(n):
        report_lines.append(" ")


def _center(text: str, width: int) -> str:
    text = text[:width]
    pad = width - len(text)
    left = pad // 2
    return " " * left + text + " " * (pad - left)


# ============================================================================
# STEP 11: REPORT -1  PROC PRINT DATA=CDRLNTUK BY STATE BRNAME
#          (ID NUM; VAR BRH ACCTNO NAME CUSTIDNO CGCREF APPRLIMT BALANCE
#           ARREARNO BILTOT; SUM BALANCE; SUMBY BRNAME; PAGEBY BRNAME;)
# ============================================================================
print("\nStep 11: Rendering report -1 (CDRLNTUK outstanding loans)...")

_R1_LABELS = {
    "NUM": ["NO."], "BRH": ["BRANCH ABBR"], "ACCTNO": ["ACCTNO"], "NAME": ["NAME"],
    "CUSTIDNO": ["IC OR BR NO"], "CGCREF": ["CGC REFERENCE NO"],
    "APPRLIMT": ["LIMIT APPROVED", "BY CGC"], "BALANCE": ["BALANCE"],
    "ARREARNO": ["NO OF", "INSTALMENTS IN", "ARREARS"],
    "BILTOT": ["AMOUNT IN", "ARREARS"],
}
_R1_VARS = ["BRH", "ACCTNO", "NAME", "CUSTIDNO", "CGCREF", "APPRLIMT",
            "BALANCE", "ARREARNO", "BILTOT"]


def _r1_fmt(var, val):
    if var == "BALANCE":
        return _fmt_comma(val, 15, 2)
    if var == "BILTOT":
        return _fmt_comma(val, 8, 2)
    if var == "APPRLIMT":
        return _fmt_comma(val, 15, 2)
    if var == "ARREARNO":
        return _fmt_int(val, 5)
    return "" if val is None else str(val)


def _r1_title_block():
    return [
        "PROGRAM ID : EIBHTUKR - 1",
        "TABUNG USAHAWAN KECIL (TUK) ON OUTSTANDING LOAN",
        f"REPORTING DATE : {FULLDATE}{RYEAR}",
        "",
    ]


def _render_report1(rows):
    # BY STATE BRNAME groups, PAGEBY BRNAME -> new page for each BRNAME group.
    groups = {}
    order = []
    for r in rows:
        key = (r["STATE"] or "", r["BRNAME"] or "")
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(r)

    grand_total = 0.0
    for state, brname in order:
        grp = groups[(state, brname)]

        # Column widths computed per BY-group (PROC PRINT without UNIFORM
        # computes widths per BY-group, not globally).
        widths = {}
        for v in ["NUM"] + _R1_VARS:
            label_w = max(len(x) for x in _R1_LABELS.get(v, [v]))
            if v == "NUM":
                data_w = max(len(_fmt_int(g["NUM"], 1)) for g in grp)
            else:
                data_w = max(len(_r1_fmt(v, g.get(v))) for g in grp)
            widths[v] = max(label_w, data_w) + 2

        report_lines.append("\f")
        report_lines.extend(_r1_title_block())
        _emit(f"STATE={state_format(state) or state}   BRNAME={brname}")
        _emit_blank()

        header_h = max(len(_R1_LABELS.get(v, [v])) for v in ["NUM"] + _R1_VARS)
        for line_idx in range(header_h):
            parts = []
            for v in ["NUM"] + _R1_VARS:
                lbls = _R1_LABELS.get(v, [v])
                txt = lbls[line_idx] if line_idx < len(lbls) else ""
                parts.append(txt.center(widths[v]))
            _emit("".join(parts))
        _emit("".join("-" * widths[v] for v in ["NUM"] + _R1_VARS))

        subtotal = 0.0
        for g in grp:
            parts = [_fmt_int(g["NUM"], widths["NUM"])]
            for v in _R1_VARS:
                parts.append(_r1_fmt(v, g.get(v)).rjust(widths[v]) if v not in ("NAME",)
                             else (g.get(v) or "").ljust(widths[v]))
            _emit("".join(parts))
            subtotal += g.get("BALANCE") or 0.0

        # SUMBY BRNAME subtotal row
        sum_row = "".join(
            (_fmt_comma(subtotal, widths["BALANCE"], 2) if v == "BALANCE"
             else " " * widths[v])
            for v in ["NUM"] + _R1_VARS
        )
        _emit(sum_row)
        grand_total += subtotal

    # Overall grand total (PROC PRINT always shows the overall total of a
    # SUM variable at the very end of the report).
    _emit_blank()
    _emit(f"GRAND TOTAL BALANCE: {_fmt_comma(grand_total, 18, 2)}")


_render_report1(cdrlntuk_rows)

# ============================================================================
# STEP 12: REPORT -2 / REPORT -3  PROC TABULATE (ACCRBAL/ACCRTOT, INTBAL/INTTOT)
# ============================================================================
print("\nStep 12: Rendering reports -2 and -3 (TABULATE interest accrued/paid)...")


def _render_tabulate(rows, row_key, row_label_fn, sum_field, tot_field,
                      title2, title3, rts, box, sum_width, sum_dec,
                      tot_width, tot_dec):
    """
    Emulates:
      TABLE <row_key>=' ' ALL='TOTAL',
            PRODUCT=' '*COL=' '*<sum_field>=' ' <tot_field>
            /RTS=<rts> BOX=<box>;

    NOTE: The original FORMAT statement ("FORMAT <sum_field> <tot_field>
    PRODUCT TUK. COL $COL.;") groups <sum_field>/<tot_field>/PRODUCT under
    the TUK. format by SAS's variable-list-before-format parsing rule.
    Since <sum_field>/<tot_field> values never match TUK.'s discrete
    521/522/523/528 codes, TUK. never actually matches them and the
    PROC-level default `FORMAT=COMMA12.2` is what is visibly rendered for
    them -- preserved here by simply using COMMA12.2/whatever width is
    passed in for the numeric cells.
    """
    report_lines.append("\f")
    report_lines.append("PROGRAM ID : EIBHTUKR - " + ("2" if sum_field == "ACCRBAL" else "3"))
    report_lines.append(title2)
    report_lines.append(f"REPORTING DATE : {FULLDATE}{RYEAR}")
    report_lines.append("")

    # Build (row_key_val, PRODUCT, COL) -> sum(sum_field); and row_key_val -> sum(tot_field)
    cell_sum = {}
    row_tot = {}
    row_vals = []
    seen_rows = set()
    col_groups = {}   # PRODUCT -> ordered list of COL letters seen

    for r in rows:
        rk = row_label_fn(r)
        if rk not in seen_rows:
            seen_rows.add(rk)
            row_vals.append(rk)
        prod, col = r["PRODUCT"], r["COL"]
        col_groups.setdefault(prod, [])
        if col not in col_groups[prod]:
            col_groups[prod].append(col)
        key = (rk, prod, col)
        v = r.get(sum_field)
        if v is not None:
            cell_sum[key] = (cell_sum.get(key) or 0.0) + v
        tv = r.get(tot_field)
        if tv is not None:
            row_tot[rk] = (row_tot.get(rk) or 0.0) + tv

    products = sorted(col_groups.keys())
    row_vals = sorted(row_vals) + ["TOTAL"]

    label_w = rts
    col_w = max(sum_width, 8)

    for prod in products:
        prod_label = tuk_format(prod) or str(prod)
        for c in col_groups[prod]:
            pass
    # Header
    header1 = box.ljust(label_w)
    header2 = " " * label_w
    for prod in products:
        prod_label = tuk_format(prod) or str(prod)
        n = len(col_groups[prod])
        header1 += _center(prod_label, col_w * n)
        for c in col_groups[prod]:
            header2 += _center(col_format(c) or c, col_w)
    header1 += " " * tot_width
    header2 += _center(tot_field, tot_width)
    report_lines.append(header1)
    report_lines.append(header2)
    report_lines.append("-" * len(header2))

    for rk in row_vals:
        line = rk.ljust(label_w)
        if rk == "TOTAL":
            for prod in products:
                for c in col_groups[prod]:
                    tot = sum(cell_sum.get((rv, prod, c), 0.0) for rv in row_vals if rv != "TOTAL")
                    line += _fmt_comma(tot, col_w, sum_dec)
            grand_tot = sum(v for k, v in row_tot.items())
            line += _fmt_comma(grand_tot, tot_width, tot_dec)
        else:
            for prod in products:
                for c in col_groups[prod]:
                    line += _fmt_comma(cell_sum.get((rk, prod, c)), col_w, sum_dec)
            line += _fmt_comma(row_tot.get(rk), tot_width, tot_dec)
        report_lines.append(line)


_render_tabulate(
    cdrtuk_rows, "BRH", lambda r: r["BRH"] or "",
    "ACCRBAL", "ACCRTOT",
    title2="HALF-YEARLY EXTRACTION OF INTEREST ACCRUED FOR TUK 1/2/3/4",
    title3="", rts=10, box="BRANCH", sum_width=12, sum_dec=2,
    tot_width=12, tot_dec=2,
)

_render_tabulate(
    cdrtuk_rows, "BRHNAME", lambda r: (r["BRHNAME"] or "").strip(),
    "INTBAL", "INTTOT",
    title2="HALF-YEARLY EXTRACTION OF INTEREST PAID BY CUSTOMER FOR TUK 1/2/3/4",
    title3="", rts=30, box="BRANCH", sum_width=15, sum_dec=2,
    tot_width=12, tot_dec=2,
)

# ============================================================================
# STEP 13: REPORT (WHERE NAME=' ')  PROC PRINT DATA=CDRTUK&REPTMON WHERE NAME=' '
# ============================================================================
print("\nStep 13: Rendering 'accounts without names' listing...")

report_lines.append("\f")
report_lines.append("LIST OF ACCOUNTS WITHOUT NAMES")
report_lines.append(f"REPORTING DATE : {FULLDATE}{RYEAR}")
report_lines.append("")
blank_name_rows = [r for r in cdrtuk_rows if not (r.get("NAME") or "").strip()]
if blank_name_rows:
    hdr = "BRH".ljust(6) + "BRHNAME".ljust(27) + "PRODUCT".ljust(9) + "COL".ljust(5) + \
          "ACCRBAL".rjust(14) + "ACCRTOT".rjust(14) + "INTBAL".rjust(14) + "INTTOT".rjust(14)
    report_lines.append(hdr)
    for r in blank_name_rows:
        report_lines.append(
            (r.get("BRH") or "").ljust(6) + (r.get("BRHNAME") or "").ljust(27) +
            str(r.get("PRODUCT") or "").ljust(9) + (r.get("COL") or "").ljust(5) +
            _fmt_comma(r.get("ACCRBAL"), 14, 2) + _fmt_comma(r.get("ACCRTOT"), 14, 2) +
            _fmt_comma(r.get("INTBAL"), 14, 2) + _fmt_comma(r.get("INTTOT"), 14, 2)
        )
else:
    report_lines.append("(no observations)")

# ============================================================================
# STEP 14: DATA TUK134 TUK2;  SET CDRLNTUK;
# ============================================================================
print("\nStep 14: Building TUK134 / TUK2 (CLASSIF/INTERB/INTERCGC/INTERAG)...")

tuk134_rows, tuk2_rows = [], []
for r in cdrlntuk_rows:
    hstint = r.get("HSTINT") or 0.0
    acctyind = r.get("ACCTYIND")
    if r["PRODUCT"] in (521, 523, 528):
        classif = "FOR THE PUBLIC BANK ACCOUNTS ONLY"
        interb = _sas_round((4 / 6) * hstint)
        intercgc = _sas_round((2 / 6) * hstint)
        if acctyind == 700:
            classif = "FOR FORMER HHB ACCOUNTS ONLY"
        if acctyind == 800:
            classif = "FOR FORMER ADF ACCOUNTS ONLY"
        rec = dict(r)
        rec.update({"CLASSIF": classif, "INTERB": interb, "INTERCGC": intercgc})
        tuk134_rows.append(rec)
    elif r["PRODUCT"] == 522:
        classif = "FOR THE PUBLIC BANK ACCOUNTS ONLY"
        interb = _sas_round((1 / 6) * hstint)
        intercgc = _sas_round((1 / 6) * hstint)
        interag = _sas_round((4 / 6) * hstint)
        if acctyind == 700:
            classif = "FOR FORMER HHB ACCOUNTS ONLY"
        if acctyind == 800:
            classif = "FOR FORMER ADF ACCOUNTS ONLY"
        rec = dict(r)
        rec.update({"CLASSIF": classif, "INTERB": interb, "INTERCGC": intercgc,
                    "INTERAG": interag})
        tuk2_rows.append(rec)

# PROC SORT DATA=TUK134; BY CLASSIF STATE BRNAME;
tuk134_rows.sort(key=lambda r: (r["CLASSIF"], r["STATE"] or "", r["BRNAME"] or ""))
print(f"  TUK134 rows: {len(tuk134_rows):,}   TUK2 rows: {len(tuk2_rows):,}")

# ============================================================================
# STEP 15: REPORT -4  DATA WRITE;  (PUT-based custom report, FILE PRINT
#          HEADER=NEWPAGE, per-BRNAME/STATE/overall subtotal accumulators)
# ============================================================================
print("\nStep 15: Rendering report -4 (TUK134 custom PUT report)...")

_page_cnt = 0


def _newpage(classif, state, brname, brh, dt):
    global _page_cnt
    _page_cnt += 1
    report_lines.append("\f")
    report_lines.append(
        f"REPORT NAME : EIBHTUKR - 4"
        + " " * 20 + "P U B L I C   B A N K   B E R H A D"
        + " " * 10 + f"PAGE NO : {_page_cnt}"
    )
    report_lines.append("")
    report_lines.append("TABUNG USAHAWAN KECIL (TUK) LOAN SCHEME - PACKAGE 1,3,OR 4")
    report_lines.append(classif)
    report_lines.append(
        "PROFIT EARNED FOR THE HALF YEARLY REPAYMENT FOR "
        + f"PERIOD ENDED : {FULLDATE}{RYEAR}"
        + f"  DATE    : {dt.strftime('%d/%m/%y')}"
    )
    report_lines.append("")
    report_lines.append(f"STATE : {state_format(state) or state}  BRANCH CODE : {brname}  BRANCH ABBR : {brh}")
    report_lines.append("")
    report_lines.append(
        "CGC REF" + " " * 46 + "  LOAN    " + "OUSTANDING" +
        "REPAYMENT COLLECTED(RM)" + " " * 1 + "INCOME SHARING IN (RM)"
    )
    report_lines.append(
        "NO." + "NUMBER  ".rjust(4) + "ACCOUNT NO".rjust(15) + "NAME OF BORROWER".rjust(26) +
        "AMOUNT(RM)".rjust(26) + "BALANCE(RM)".rjust(12) + "PRINCIPAL".rjust(12) +
        "INTEREST6%".rjust(12) + "BANK(X 4/6)".rjust(12) + "CGC (X 2/6)".rjust(12)
    )
    report_lines.append(
        "___" + " " * 4 + "_______".rjust(11) + "__________".rjust(15) +
        "________________".rjust(26) + "__________".rjust(26) + "___________".rjust(12) +
        "_________".rjust(12) + "___________".rjust(12) + "___________".rjust(12) +
        "___________".rjust(12)
    )
    report_lines.append("")


def _render_report4(rows):
    if not rows:
        return
    dt = date.today()
    cnt = sappr = sbal = shstprin = sintpdyt = sinterb = sintercg = 0.0
    bappr = bbal = bhstprin = bintpdyt = binterb = bintercg = 0.0
    tappr = tbal = thstprin = tintpdyt = tinterb = tintercg = 0.0

    prev_state = prev_brname = prev_classif = object()
    n = len(rows)
    for i, r in enumerate(rows):
        classif, state, brname, brh = r["CLASSIF"], r["STATE"], r["BRNAME"], r["BRH"]
        first_brname = (brname != prev_brname) or (state != prev_state) or (classif != prev_classif)
        first_state = (state != prev_state) or (classif != prev_classif)

        if first_brname:
            cnt = sappr = sbal = shstprin = sintpdyt = sinterb = sintercg = 0.0
            if i != 0:
                pass  # PUT _PAGE_ handled by _newpage below
        if first_state:
            bappr = bbal = bhstprin = bintpdyt = binterb = bintercg = 0.0

        if first_brname:
            _newpage(classif, state, brname, brh, dt)

        cnt += 1
        balance = r.get("BALANCE") or 0.0
        apprlimt = r.get("APPRLIMT") or 0.0
        hstprin = r.get("HSTPRIN") or 0.0
        hstint = r.get("HSTINT") or 0.0
        interb = r.get("INTERB") or 0.0
        intercgc = r.get("INTERCGC") or 0.0

        sbal += balance; sappr += apprlimt; shstprin += hstprin
        sintpdyt += hstint; sinterb += interb; sintercg += intercgc
        bbal += balance; bappr += apprlimt; bhstprin += hstprin
        bintpdyt += hstint; binterb += interb; bintercg += intercgc
        tbal += balance; tappr += apprlimt; thstprin += hstprin
        tintpdyt += hstint; tinterb += interb; tintercg += intercgc

        report_lines.append(
            _fmt_int(cnt, 2) + " " +
            (r.get("CGCREF") or "").ljust(15) + " " +
            _fmt_int(r.get("ACCTNO"), 10) + " " +
            (r.get("NAME") or "").ljust(24) + " " +
            _fmt_comma(apprlimt, 10, 2) + " " +
            _fmt_comma(balance, 10, 2) + " " +
            _fmt_comma(hstprin, 10, 2) + " " +
            _fmt_comma(hstint, 10, 2) + " " +
            _fmt_comma(interb, 10, 2) + " " +
            _fmt_comma(intercgc, 10, 2)
        )

        is_last_brname = (i == n - 1) or (
            rows[i + 1]["BRNAME"] != brname or rows[i + 1]["STATE"] != state
            or rows[i + 1]["CLASSIF"] != classif
        )
        is_last_state = (i == n - 1) or (
            rows[i + 1]["STATE"] != state or rows[i + 1]["CLASSIF"] != classif
        )

        if is_last_brname:
            report_lines.append("")
            report_lines.append("-" * 130)
            report_lines.append(
                " " * 56 + _fmt_comma(sappr, 12, 2) + _fmt_comma(sbal, 12, 2) +
                _fmt_comma(shstprin, 12, 2) + _fmt_comma(sintpdyt, 12, 2) +
                _fmt_comma(sinterb, 12, 2) + _fmt_comma(sintercg, 12, 2)
            )
            report_lines.append("-" * 130)

        if is_last_state:
            report_lines.append("")
            report_lines.append("+" * 130)
            report_lines.append(
                " " * 54 + _fmt_comma(bappr, 13, 2) + _fmt_comma(bhstprin, 12, 2) +
                _fmt_comma(binterb, 12, 2)
            )
            report_lines.append(
                " " * 67 + _fmt_comma(bbal, 13, 2) + _fmt_comma(bintpdyt, 12, 2) +
                _fmt_comma(bintercg, 12, 2)
            )
            report_lines.append("+" * 130)

        if i == n - 1:
            report_lines.append("")
            report_lines.append("=" * 130)
            report_lines.append(
                " " * 54 + _fmt_comma(tappr, 13, 2) + _fmt_comma(thstprin, 12, 2) +
                _fmt_comma(tinterb, 12, 2)
            )
            report_lines.append(
                " " * 67 + _fmt_comma(tbal, 13, 2) + _fmt_comma(tintpdyt, 12, 2) +
                _fmt_comma(tintercg, 12, 2)
            )
            report_lines.append("=" * 130)

        prev_state, prev_brname, prev_classif = state, brname, classif


_render_report4(tuk134_rows)

# ============================================================================
# STEP 16: REPORT -5  PROC SORT DATA=TUK2; BY CLASSIF STATE BRNAME BRH;
#          PROC PRINT DATA=TUK2 SPLIT='*';
# ============================================================================
print("\nStep 16: Rendering report -5 (TUK2 package 2 listing)...")

tuk2_rows.sort(key=lambda r: (r["CLASSIF"], r["STATE"] or "", r["BRNAME"] or "", r["BRH"] or ""))

_R5_LABELS = {
    "ACCTNO": ["ACCOUNT NO"], "NAME": ["NAME"], "CGCREF": ["CGC REFERENCE NO"],
    "APPRLIMT": ["LIMIT", "APPROVED", "BY CGC"], "BALANCE": ["BALANCE"],
    "HSTPRIN": ["REPAYMENT", "COLLECTED", "FOR PRINCIPAL", "IN(RM)"],
    "HSTINT": ["REPAYMENT", "COLLECTED", "FOR INTEREST", "6% (RM)"],
    "INTERB": ["INCOME", "SHARING", "-------", "BANK (RM)", "(1/6 %)"],
    "INTERCGC": ["INCOME", "SHARING", "-------", " CGC (RM)", "(1/6 %)"],
    "INTERAG": ["INCOME", "SHARING", "-------", "AGENT(RM)", "(4/6 %)"],
}
_R5_VARS = ["ACCTNO", "NAME", "CGCREF", "APPRLIMT", "BALANCE", "HSTPRIN",
            "HSTINT", "INTERB", "INTERCGC", "INTERAG"]
_R5_NUM_W = {"BALANCE": 9, "APPRLIMT": 9, "HSTPRIN": 9, "HSTINT": 9,
             "INTERB": 9, "INTERCGC": 9, "INTERAG": 9}


def _r5_fmt(v, val):
    if v in _R5_NUM_W:
        return _fmt_comma(val, _R5_NUM_W[v], 2)
    return "" if val is None else str(val)


def _render_report5(rows):
    groups = {}
    order = []
    for r in rows:
        key = (r["STATE"] or "", r["BRNAME"] or "", r["BRH"] or "")
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(r)

    for state, brname, brh in order:
        grp = groups[(state, brname, brh)]

        widths = {}
        for v in _R5_VARS:
            label_w = max(len(x) for x in _R5_LABELS.get(v, [v]))
            data_w = max(len(_r5_fmt(v, g.get(v))) for g in grp)
            widths[v] = max(label_w, data_w) + 2

        report_lines.append("\f")
        report_lines.append("REPORT NAME : EIBHTUKR - 5")
        report_lines.append("TUK LOAN SCHEME - PACKAGE 2")
        report_lines.append(
            f"PROFIT EARNED FOR THE HALF YEARLY REPAYMENT FOR PERIOD ENDED : "
            f"{FULLDATE}{RYEAR}"
        )
        report_lines.append("")
        report_lines.append(f"STATE={state_format(state) or state}  BRNAME={brname}  BRH={brh}")
        report_lines.append("")

        header_h = max(len(_R5_LABELS.get(v, [v])) for v in _R5_VARS)
        for line_idx in range(header_h):
            parts = []
            for v in _R5_VARS:
                lbls = _R5_LABELS.get(v, [v])
                txt = lbls[line_idx] if line_idx < len(lbls) else ""
                parts.append(txt.center(widths[v]))
            report_lines.append("".join(parts))
        report_lines.append("".join("-" * widths[v] for v in _R5_VARS))

        sums = {v: 0.0 for v in _R5_NUM_W}
        for g in grp:
            parts = []
            for v in _R5_VARS:
                txt = _r5_fmt(v, g.get(v))
                parts.append(txt.ljust(widths[v]) if v == "NAME" else txt.rjust(widths[v]))
            report_lines.append("".join(parts))
            for v in sums:
                sums[v] += g.get(v) or 0.0

        sum_line = "".join(
            (_fmt_comma(sums[v], widths[v], 2) if v in sums else " " * widths[v])
            for v in _R5_VARS
        )
        report_lines.append(sum_line)


_render_report5(tuk2_rows)

# ============================================================================
# STEP 17: WRITE OUTPUT (ASA carriage-control text)
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        if ln == "\f":
            fh.write("1" + "\n")
        else:
            fh.write(" " + ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")
print("\nEIBHTUKR complete.")
