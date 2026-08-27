#!/usr/bin/env python3
"""
Program : EIIMLN03.py
Purpose : Weighted Average Lending Rate on Loan (RDIR II)

Dependency:
    %INC PGM(PBBLNFMT); -> from PBBLNFMT import format_ln03fmt
    LN03FMT.   is used directly ("LOANTYP = PUT(PRODUCT,LN03FMT.);").
    All other PBBLNFMT formats/macro lists (LNPROD., LNDENOM., LNRATE.,
    product lists, etc.) have no traceable PUT()/direct call anywhere in
    this program body, so they are intentionally NOT imported here.

============================================================================
PHYSICAL INPUT DATASETS (each cached to Parquet independently, using the
same chunked sas7bdat -> Parquet -> cache pattern as EIBDLN1M.py /
EIIMRM01.py)
============================================================================
1. BNM.SDESC          (JCL //BNM DD DSN=SAP.PIBB.SASDATA)
   File : INPUT_SDESC_FILE -> sdesc.sas7bdat
   Cols used : SDESC
   Used : DATA DESC; SET BNM.SDESC; CALL SYMPUT('SDESC', PUT(SDESC,$26.));
          No BY / no RUN before the next DATA step -- SAS loops over every
          row of SDESC, overwriting the SDESC macro variable each time, so
          the FINAL value is the LAST observation's SDESC value. Reproduced
          here by taking the last row.

2. ODGP3.GP3          (JCL //ODGP3 DD DSN=SAP.PIBB.MNILIMT(0))
   File : INPUT_GP3_FILE -> gp3.sas7bdat
   Cols used : ACCTNO, RISKCODE
   Used : DATA GP3; SET ODGP3.GP3; RISKRTE=INPUT(RISKCODE,1.);
          IF RISKRTE < 1 THEN LOANSTAT = 1; -- derives a per-account
          LOANSTAT flag later merged (BY ACCTNO, KEEP=ACCTNO LOANSTAT)
          into the main loan dataset.

3. BNM.LOAN&REPTMON&NOWK  (JCL //BNM DD DSN=SAP.PIBB.SASDATA)
   File : INPUT_LOAN_FILE -> constructed directly as
          loan{REPTMON}{NOWK}.sas7bdat (fully deterministic from REPTMON +
          NOWK, so input_date.get_latest_file() is NOT used here, per
          project convention for month/week-suffixed deterministic inputs).
   Cols used : ACCTNO, PRODUCT, PRODCD, ACCTYPE, BRANCH, INTRATE, BALANCE,
               SPREAD, CENSUS

NOWK here is derived by EXACT-DAY matching (day=8/15/22, else 4) -- this is
DIFFERENT from REPTDATE.py's range-based NOWK and is genuinely used (unlike
in EIIMRM01.py where an identically-derived NOWK was dead code), because it
drives the input dataset name. It is therefore computed locally here rather
than sourced from REPTDATE.get_reptdate_values().nowk.

============================================================================
OUTPUTS
============================================================================
1. //SASLIST DD DSN=SAP.PIBB.EIIMLN03, DCB=(RECFM=FB,LRECL=133,BLKSIZE=0)
   -> OUTPUT_REPORT_FILE (EIIMLN03.txt). Fixed catalogued (GDG-style) name,
      no date token -> output_date.py NOT applicable (no date component in
      the original dataset name). RECFM=FB (not FBA) => NO ASA carriage
      control byte, per project convention.

2. //M4LOAN  DD DSN=SAP.PIBB.M4LOAN,  DCB=(RECFM=FB,LRECL=50, BLKSIZE=0)
   -> OUTPUT_M4LOAN_FILE (M4LOAN.txt). Fixed catalogued name, no date token
      -> output_date.py NOT applicable. All fields are Z-format
      zero-padded decimal digits (no packed-decimal / COMP-3 fields), so
      this stays a plain .txt flat file, not a binary .dat file.
"""

import gc
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from PBBLNFMT_AII import format_ln03fmt

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_SDESC_DIR = STG_DIR / "sasdata"
INPUT_GP3_DIR   = STG_DIR / "sasdata"
INPUT_LOAN_DIR  = STG_DIR / "sasdata"

INPUT_SDESC_FILE = INPUT_SDESC_DIR / "sdesc.sas7bdat"
INPUT_GP3_FILE   = INPUT_GP3_DIR / "gp3.sas7bdat"
# INPUT_LOAN_FILE is built below once REPTMON / NOWK are known.

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIMLN03"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIIMLN03"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_REPORT_FILE = OUTPUT_DIR / "EIIMLN03.txt"
OUTPUT_M4LOAN_FILE = OUTPUT_DIR / "M4LOAN.txt"

CHUNK_ROWS = 500_000
PAGE_SIZE  = 60      # lines per page (not specified in SAS -> default)
LINE_WIDTH = 133     # DCB LRECL for SASLIST
M4LOAN_LRECL = 50    # DCB LRECL for M4LOAN

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet -- derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate

# NOWK here uses EXACT-DAY matching (day=8/15/22 else 4), which differs from
# REPTDATE.py's range-based NOWK. Unlike EIIMRM01.py (where an identical
# derivation was dead code), this NOWK IS used to build the input dataset
# name below, so it is computed locally to preserve the SAS source exactly.
_day = reptdate.day
NOWK = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"

REPTYEAR = reptdate.strftime("%Y")   # PUT(REPTDATE,YEAR4.)
REPTMON  = reptdate.strftime("%m")   # PUT(MONTH(REPTDATE),Z2.)
REPTDAY  = reptdate.strftime("%d")   # PUT(DAY(REPTDATE),Z2.)
RDATE    = reptdate.strftime("%d/%m/%y")   # PUT(REPTDATE,DDMMYY8.)

# INPUT_LOAN_FILE = INPUT_LOAN_DIR / f"loan{REPTMON}{NOWK}.sas7bdat"
INPUT_LOAN_FILE = INPUT_LOAN_DIR / f"iloan083.sas7bdat"

print(f"  REPTYEAR/MON/DAY : {REPTYEAR}/{REPTMON}/{REPTDAY}   NOWK: {NOWK}")
print(f"  RDATE            : {RDATE}")
print(f"  LOAN input file  : {INPUT_LOAN_FILE.name}")

# ============================================================================
# LOCAL PROC FORMAT: $LNFMT  (local to this program, NOT part of PBBLNFMT)
# ============================================================================
_LNFMT_LABELS = {
    "P1": "PRESCRIBED RATE (HOUSING LOANS)",
    "P2": "PRESCRIBED RATE (BNM FUNDED LOANS)",
    "P3": "NON-PRESCRIBED RATE (HOUSING LOANS)",
    "P4": "NON-PRESCRIBED RATE (OTHER LOANS)",
}


def _lnfmt_label(code):
    return _LNFMT_LABELS.get(code, code if code is not None else "")


def _sas_round(x: float) -> float:
    """SAS ROUND() with no scale argument: round to nearest integer,
    halves away from zero."""
    if x >= 0:
        return float(int(x + 0.5))
    return float(-int(-x + 0.5))


def _input_1_numeric(text):
    """INPUT(RISKCODE,1.) -- reads only the FIRST character of RISKCODE
    (informat width 1) and converts it to a number."""
    if text is None:
        return None
    s = str(text)
    if len(s) == 0:
        return None
    try:
        return float(s[0])
    except ValueError:
        return None


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
GP3_CACHE   = _load_cached(INPUT_GP3_FILE, "GP3")
LOAN_CACHE  = _load_cached(INPUT_LOAN_FILE, "LOAN")

# ============================================================================
# STEP 3: DATA DESC; SET BNM.SDESC; CALL SYMPUT('SDESC', PUT(SDESC,$26.));
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
# STEP 4: DATA GP3; SET ODGP3.GP3; RISKRTE=INPUT(RISKCODE,1.);
#         IF RISKRTE < 1 THEN LOANSTAT = 1;
# ============================================================================
print("\nStep 4: Building GP3 (RISKRTE / LOANSTAT)...")

con = duckdb.connect(database=":memory:")
gp3_raw = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(RISKCODE AS VARCHAR) AS RISKCODE
    FROM read_parquet('{GP3_CACHE.as_posix()}')
""").pl()
con.close()

# Only ACCTNO/LOANSTAT survive the later MERGE (KEEP=ACCTNO LOANSTAT), so we
# only need a dict of ACCTNO -> LOANSTAT (present only where LOANSTAT=1;
# absent = missing, matching SAS's un-assigned/ELSE-less behaviour).
gp3_loanstat = {}
for r in gp3_raw.iter_rows(named=True):
    riskrte = _input_1_numeric(r["RISKCODE"])
    if riskrte is not None and riskrte < 1:
        gp3_loanstat[r["ACCTNO"]] = 1

print(f"  GP3 rows: {len(gp3_raw):,}   Accounts flagged LOANSTAT=1: {len(gp3_loanstat):,}")

# ============================================================================
# STEP 5: DATA LOAN; SET BNM.LOAN&REPTMON&NOWK;
#         IF PRODUCT IN (124,145) AND PRODCD='54120' THEN DELETE;
# ============================================================================
print("\nStep 5: Loading base LOAN dataset...")

con = duckdb.connect(database=":memory:")
loan_raw = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(PRODUCT  AS INTEGER) AS PRODUCT,
        CAST(PRODCD   AS VARCHAR) AS PRODCD,
        CAST(ACCTYPE  AS VARCHAR) AS ACCTYPE,
        CAST(BRANCH   AS INTEGER) AS BRANCH,
        CAST(INTRATE  AS DOUBLE)  AS INTRATE,
        CAST(BALANCE  AS DOUBLE)  AS BALANCE,
        CAST(SPREAD   AS DOUBLE)  AS SPREAD,
        CAST(CENSUS   AS DOUBLE)  AS CENSUS
    FROM read_parquet('{LOAN_CACHE.as_posix()}')
    WHERE NOT (PRODUCT IN (124,145) AND PRODCD = '54120')
""").pl()
con.close()

print(f"  LOAN rows after initial filter: {len(loan_raw):,}")

# ============================================================================
# STEP 6: DATA LOAN (KEEP=LOANTYP PRODCD LOANSTAT INTRATE BALANCE BRHNO);
#         MERGE LOAN(IN=A) GP3(KEEP=ACCTNO LOANSTAT); BY ACCTNO; IF A;
#         (per-row business logic, dangling-RISKRTE quirk preserved)
# ============================================================================
print("\nStep 6: Applying per-row merge/branch logic...")

_LN_DELETE_SINGLES = {668, 669, 670, 672, 673, 674, 675, 690, 671, 676, 677}
_LN_DELETE_RANGE1  = set(range(691, 696))   # 691:695
_LN_DELETE_RANGE2  = set(range(851, 861))   # 851:860
_LN_DELETE_SET = _LN_DELETE_SINGLES | _LN_DELETE_RANGE1 | _LN_DELETE_RANGE2

_OD_P3_PRODUCTS = {120, 137, 138, 154, 155, 192, 193, 194, 195}
_OD_P2_PRODUCTS = {73, 187, 188, 47, 48, 49, 17, 14}
_CENSUS_169_SET = {169.01, 169.02, 169.03, 169.04}


def _process_loan_row(row: dict) -> dict:
    acctno   = row["ACCTNO"]
    product  = row["PRODUCT"]
    prodcd   = row["PRODCD"]
    accttype = row["ACCTYPE"]
    branch   = row["BRANCH"]
    intrate  = row["INTRATE"]
    balance  = row["BALANCE"]
    spread   = row.get("SPREAD")
    census   = row.get("CENSUS")

    loanstat = gp3_loanstat.get(acctno)   # from GP3 merge (None = missing)
    brhno = branch

    if prodcd == "34111":
        return None  # IF PRODCD='34111' THEN DELETE;

    if not (isinstance(prodcd, str) and prodcd[:2] == "34"):
        return None  # IF SUBSTR(PRODCD,1,2) = '34';  (subsetting IF)

    loantyp = None

    if accttype == "OD":
        loantyp = "P4"
        if product in (93, 162):
            return None  # IF PRODUCT IN (93,162) THEN DELETE;
        if product == 119:
            loantyp = "P1"
        elif product in _OD_P3_PRODUCTS:
            loantyp = "P3"
        elif product in _OD_P2_PRODUCTS:
            loantyp = "P2"
        # RISKRTE quirk: GP3 is merged with KEEP=ACCTNO LOANSTAT only, so
        # RISKRTE itself never enters this MERGE step's PDV -- it is an
        # uninitialised (missing) variable here. In SAS a missing numeric
        # value compares as less than any real number, so
        # "IF RISKRTE < 1 THEN LOANSTAT = 1;" is UNCONDITIONALLY TRUE for
        # every OD account, forcing LOANSTAT=1 regardless of what the GP3
        # merge produced. This looks like an original SAS bug and is
        # preserved verbatim.
        loanstat = 1

    if accttype == "LN":
        if product in (225, 226):
            if (intrate is not None and intrate <= 9) or (spread is not None and spread <= 1.75):
                loantyp = "P1"
            else:
                loantyp = "P3"
        else:
            loantyp = format_ln03fmt(product)
        if product in _LN_DELETE_SET:
            return None
        if product == 169 and census in _CENSUS_169_SET:
            loantyp = "P2"

    if loantyp == "SL":
        return None  # IF LOANTYP ^= 'SL';  (subsetting IF)

    return {
        "LOANTYP": loantyp, "PRODCD": prodcd, "LOANSTAT": loanstat,
        "INTRATE": intrate, "BALANCE": balance, "BRHNO": brhno,
    }


loan_final_rows = []
for r in loan_raw.iter_rows(named=True):
    out = _process_loan_row(r)
    if out is not None:
        loan_final_rows.append(out)

del loan_raw, gp3_raw
gc.collect()

print(f"  LOAN (final) rows: {len(loan_final_rows):,}")


# ============================================================================
# STEP 7: PROC SUMMARY helper (NWAY, default MISSING-exclusion, SUM ignores
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
# STEP 8: REPORT #1
#   PROC SUMMARY DATA=LOAN NWAY; CLASS LOANTYP INTRATE; VAR BALANCE;
#   OUTPUT OUT=ALM SUM=; WHERE LOANSTAT = 1;
#   DATA ALM; SET ALM; PRODUCT = INTRATE*BALANCE;
# ============================================================================
print("\nStep 8: Building report #1 (LOANSTAT=1 accounts)...")

alm1_rows = _class_summary(
    [r for r in loan_final_rows if r["LOANSTAT"] == 1],
    ["LOANTYP", "INTRATE"], ["BALANCE"],
)
for r in alm1_rows:
    r["PRODUCT"] = (
        r["INTRATE"] * r["BALANCE"]
        if r["INTRATE"] is not None and r["BALANCE"] is not None else None
    )

# ============================================================================
# STEP 9: REPORT #2
#   PROC SUMMARY DATA=LOAN NWAY; CLASS LOANTYP INTRATE; VAR BALANCE;
#   OUTPUT OUT=ALM SUM=; WHERE LOANTYP IN ('P1','P2');
#   DATA ALM; SET ALM; PRODUCT = INTRATE*BALANCE;
# ============================================================================
print("Step 9: Building report #2 (prescribed rates P1/P2)...")

alm2_rows = _class_summary(
    [r for r in loan_final_rows if r["LOANTYP"] in ("P1", "P2")],
    ["LOANTYP", "INTRATE"], ["BALANCE"],
)
for r in alm2_rows:
    r["PRODUCT"] = (
        r["INTRATE"] * r["BALANCE"]
        if r["INTRATE"] is not None and r["BALANCE"] is not None else None
    )

# ============================================================================
# STEP 10: REPORT #3
#   PROC SUMMARY DATA=ALM NWAY; CLASS INTRATE; VAR BALANCE PRODUCT;
#   OUTPUT OUT=ALM(DROP=_FREQ_ _TYPE_) SUM=;
# ============================================================================
print("Step 10: Building report #3 (collapsed by INTRATE)...")

alm3_rows = _class_summary(alm2_rows, ["INTRATE"], ["BALANCE", "PRODUCT"])

# ============================================================================
# STEP 11: REPORT RENDERING  (PROC PRINT emulation)
# ============================================================================
print("\nStep 11: Rendering PROC PRINT reports...")

FF = "\f"
OBS_WIDTH = 6
INTRATE_WIDTH = 9
AMT_WIDTH = 18   # COMMA18.2

_COL_WIDTH = {"Obs": OBS_WIDTH, "INTRATE": INTRATE_WIDTH, "BALANCE": AMT_WIDTH, "PRODUCT": AMT_WIDTH}


def _fmt_comma(value, width=AMT_WIDTH, decimals=2):
    """COMMAw.d format. OPTIONS MISSING=0 makes SAS substitute a single
    '0' character (right-justified) for a missing numeric value instead of
    the default '.'; a genuine computed zero still prints fully formatted."""
    if value is None:
        return "0".rjust(width)
    v = float(value)
    s = f"{v:,.{decimals}f}"
    if len(s) > width:
        s = f"{v:.{decimals}f}"
    if len(s) > width:
        s = s[-width:]
    return s.rjust(width)


def _fmt_best(value, width=INTRATE_WIDTH):
    """Default SAS BESTw. display for INTRATE (no explicit FORMAT given)."""
    if value is None:
        return "0".rjust(width)
    v = float(value)
    s = str(int(v)) if v == int(v) else f"{v:.6g}"
    if len(s) > width:
        s = f"{v:.2f}"
    if len(s) > width:
        s = s[:width]
    return s.rjust(width)


def _fmt_value(field, value):
    if field in ("BALANCE", "PRODUCT"):
        return _fmt_comma(value)
    if field == "INTRATE":
        return _fmt_best(value)
    return str(value) if value is not None else ""


def _title_lines(titles):
    return [t for t in titles if t]


def _render_proc_print(rows, by_field, value_fields, sum_fields, titles, lnfmt_label=None):
    """
    Emulates:
        PROC PRINT;
           FORMAT LOANTYP $LNFMT. BALANCE PRODUCT COMMA18.2;
           [BY <by_field>; PAGEBY <by_field>; SUMBY <by_field>;]
           SUM <sum_fields>;
           TITLE1-4 ...;

    - Obs continuously numbered across the whole listing (not reset per
      BY group), matching default PROC PRINT behaviour.
    - PAGEBY starts a brand-new page (titles + column headers reprinted)
      at the first row of every BY group; without a BY statement, a
      single grand-total SUM line closes the listing instead.
    - No ASA carriage control -- SASLIST is RECFM=FB (not FBA); a plain
      form-feed character marks page breaks, per project convention.
    """
    output = []
    header_cols = ["Obs"] + value_fields

    def _header_line():
        return "  ".join(h.rjust(_COL_WIDTH[h]) for h in header_cols)

    lines_on_page = [0]

    def _new_page(by_label):
        block = [FF]
        block.extend(_title_lines(titles))
        block.append("")
        if by_label is not None:
            block.append(by_label)
            block.append("")
        block.append(_header_line())
        output.extend(block)
        lines_on_page[0] = len(block)

    if by_field:
        groups = sorted({r[by_field] for r in rows if r.get(by_field) is not None})
    else:
        groups = [None]

    obs = 0
    for grp in groups:
        grp_rows = rows if by_field is None else [r for r in rows if r.get(by_field) == grp]
        by_label = None
        if by_field:
            label_val = lnfmt_label(grp) if lnfmt_label else grp
            by_label = f"{by_field}={label_val}"

        _new_page(by_label)  # PAGEBY: new page for every group (incl. first)

        for r in grp_rows:
            if lines_on_page[0] >= PAGE_SIZE:
                _new_page(by_label)
            obs += 1
            cells = [str(obs).rjust(OBS_WIDTH)]
            for f in value_fields:
                cells.append(_fmt_value(f, r.get(f)))
            output.append("  ".join(cells))
            lines_on_page[0] += 1

        if lines_on_page[0] >= PAGE_SIZE:
            _new_page(by_label)

        sum_cells = [" " * OBS_WIDTH]
        for f in value_fields:
            if f in sum_fields:
                total = sum(r.get(f) or 0.0 for r in grp_rows if r.get(f) is not None)
                sum_cells.append(_fmt_value(f, total))
            else:
                sum_cells.append(" " * _COL_WIDTH[f])
        output.append("  ".join(sum_cells))
        lines_on_page[0] += 1

    return output


_TITLE1 = "REPORT ID : EIIMLN03"
titles_report1 = [
    _TITLE1, SDESC,
    f"WEIGHTED AVERAGE LENDING RATE AS AT {RDATE}",
    "",
]
titles_report23 = [
    _TITLE1, SDESC,
    f"WEIGHTED AVERAGE LENDING RATE (PRESCRIBED) AS AT {RDATE}",
    "(INCLUDES ACCOUNTS WITH PENALTY RATES & UNDER LITIGATION)",
]

report_lines = []
report_lines += _render_proc_print(
    alm1_rows, "LOANTYP", ["INTRATE", "BALANCE", "PRODUCT"], ["BALANCE", "PRODUCT"],
    titles_report1, lnfmt_label=_lnfmt_label,
)
report_lines += _render_proc_print(
    alm2_rows, "LOANTYP", ["INTRATE", "BALANCE", "PRODUCT"], ["BALANCE", "PRODUCT"],
    titles_report23, lnfmt_label=_lnfmt_label,
)
report_lines += _render_proc_print(
    alm3_rows, None, ["INTRATE", "BALANCE", "PRODUCT"], ["BALANCE", "PRODUCT"],
    titles_report23,
)

with open(OUTPUT_REPORT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln.ljust(LINE_WIDTH) + "\n")

print(f"  Report written : {OUTPUT_REPORT_FILE}")
print(f"  Report lines   : {len(report_lines):,}")

# ============================================================================
# STEP 12: FLAT FILE M4LOAN
#   ALM  (SRS TYPE 1): CLASS INTRATE; VAR BALANCE; WHERE LOANSTAT=1 &
#                       LOANTYP IN ('P1','P2');
#   ALM1 (SRS TYPE 4): CLASS INTRATE; VAR BALANCE; WHERE LOANSTAT=1 &
#                       PRODCD NE '34111';
#   LOAN (SRS TYPE 9): CLASS BRHNO INTRATE; VAR BALANCE; (no WHERE filter)
#   DATA _NULL_; FILE M4LOAN; SET ALM(IN=A) ALM1(IN=B) LOAN END=EOF; ...
# ============================================================================
print("\nStep 12: Building M4LOAN flat file...")

alm_srs1 = _class_summary(
    [r for r in loan_final_rows if r["LOANSTAT"] == 1 and r["LOANTYP"] in ("P1", "P2")],
    ["INTRATE"], ["BALANCE"],
)
alm1_srs4 = _class_summary(
    [r for r in loan_final_rows if r["LOANSTAT"] == 1 and r["PRODCD"] != "34111"],
    ["INTRATE"], ["BALANCE"],
)
loan_srs9 = _class_summary(
    loan_final_rows, ["BRHNO", "INTRATE"], ["BALANCE"],
)


def _z_pad(value, width):
    """Zw. format: zero-padded numeric, right-justified. OPTIONS MISSING=0
    substitutes a single '0' character right-justified for a missing
    numeric value instead of zero-padding the whole field."""
    if value is None:
        return "0".rjust(width)
    return str(int(value)).rjust(width, "0")


m4loan_lines = [f"{REPTYEAR}{REPTMON}{REPTDAY}".ljust(M4LOAN_LRECL)]

combined = (
    [("A", r) for r in alm_srs1]
    + [("B", r) for r in alm1_srs4]
    + [("C", r) for r in loan_srs9]
)

for idx, (tag, r) in enumerate(combined):
    intrate_val = None if r["INTRATE"] is None else _sas_round(r["INTRATE"] * 100)
    balance_val = None if r["BALANCE"] is None else _sas_round(r["BALANCE"] * 100)

    if tag == "A":
        body = "001" + "M4 0000" + "01" + _z_pad(intrate_val, 4) + _z_pad(balance_val, 15)
    elif tag == "B":
        body = "001" + "M4 0000" + "04" + _z_pad(intrate_val, 4) + _z_pad(balance_val, 15)
    else:
        body = (
            _z_pad(r.get("BRHNO"), 3) + "M4 0000" + "09"
            + _z_pad(intrate_val, 4) + _z_pad(balance_val, 15)
        )

    m4loan_lines.append(body.ljust(M4LOAN_LRECL))

    if idx == len(combined) - 1:
        m4loan_lines.append("EOF".ljust(M4LOAN_LRECL))

with open(OUTPUT_M4LOAN_FILE, "w", encoding="latin1") as fh:
    for ln in m4loan_lines:
        fh.write(ln + "\n")

print(f"  M4LOAN written : {OUTPUT_M4LOAN_FILE}")
print(f"  M4LOAN lines   : {len(m4loan_lines):,}")

print("\nEIIMLN03 complete.")
