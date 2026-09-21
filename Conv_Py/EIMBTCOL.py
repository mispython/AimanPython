#!/usr/bin/env python3
"""
Program : EIMBTCOL.py
Purpose : Bank's Total Loans And Advances By Collaterals.
          Requested by Financial Accounting, Finance Division.
          Two reports: (1) collateral breakdown by branch/risk category/
          BNM code, (2) summary report for all loans (trade bills).

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
1. btmast<REPTMON><NOWK>.sas7bdat
   (JCL //BNM DD DSN=SAP.BT.SASDATA)
   SAS ref  : BNM.BTMAST&REPTMON&NOWK
   Filename is deterministic from REPTMON + NOWK (exact-day NOWK, live
   here since it drives this filename -- see Step 1).
   Used   : PROC SORT ... OUT=BTRADE (KEEP=ACCTNO BRANCH ICURBAL DBALANCE
            SECTORCD) NODUPKEYS; BY ACCTNO;
            WHERE SUBACCT='OV' AND CUSTCD NE ' ' AND BRANCH NE 0;
   Cols used: ACCTNO, SUBACCT, CUSTCD, BRANCH, ICURBAL, DBALANCE, SECTORCD.

2. collater.sas7bdat
   (JCL //BTCOLL DD DSN=SAP.PBB.MNICOL(0))
   SAS ref  : BTCOLL.COLLATER
   Fixed-name catalogued member (no date token) -> static physical
   filename, same pattern as EIWBTR1C's COLLATER input. Unlike EIWBTR1C,
   there is NO account-number range WHERE filter applied here.
   Cols used: ACCTNO, CCLASSC (-> LIABCODE), CDOLARV.
   Deduplicated by ACCTNO (PROC SORT ... OUT=BTCOLL NODUPKEYS).

------------------------------------------------------------------------
INPUTS DECLARED IN JCL BUT NOT USED -- documented, intentionally NOT
loaded, per project convention for dead/orphaned physical inputs:
------------------------------------------------------------------------
- BNM.BTRAD&REPTMON&NOWK (JCL //BNM DD, same DSN as BTMAST):
      PROC SORT DATA=BNM.BTRAD&REPTMON&NOWK OUT=BTRADE; BY ACCTNO;
  This creates dataset BTRADE, but the VERY NEXT statement,
      PROC SORT DATA=BNM.BTMAST&REPTMON&NOWK OUT=BTRADE (...) NODUPKEYS;
  overwrites BTRADE (same dataset name) before it is ever referenced.
  BTRAD's sorted output therefore has zero effect on the final report
  and is never actually consumed -- it is not imported/cached here,
  exactly as BNM.SAPROD is treated as dead in EIIMRM01.py.
- //BRANCH DD DSN=SAP.RBP2.B033.PBB.BRANCH: declared in the JCL step but
  never SET/MERGEd (no "BRANCH." dataset reference) anywhere in the SAS
  program body -- an unreferenced JCL DD, not loaded.

============================================================================
OUTPUT
============================================================================
//SASLIST DD DSN=SAP.PBB.EIMBTCOL.TEXT, DISP=MOD (target dataset created
fresh by the preceding //CREATE IEFBR14 step), DCB=(RECFM=FB,LRECL=150,
BLKSIZE=27000).
RECFM=FB (NOT FBA) -> per project convention, no ASA carriage-control
byte; plain fixed-width text, form-feed page breaks.
Fixed-name catalogued dataset (no date token) -> static output filename.
OPTIONS PS=65 is explicit in this program -> PAGE_SIZE=65 (not the
60-line default used when the SAS source is silent on PAGESIZE).

Two PROC PRINT reports are written, in order:
  1. TOTAL TRADE BILLS BY COLLATERAL AS AT <date> / BY BRANCHES
     (DATA=LNBR, PAGEBY BRANCH, SUMBY RISKCAT, ID BNMCODE, VAR XBALANCE)
  2. SUMMARY REPORT FOR ALL LOANS (TRADE BILLS)
     (DATA=LN WHERE _TYPE_=6, SUMBY RISKCAT, ID RISKCAT, VAR BNMCODE
     XBALANCE)

NOTE on rendering: PROC PRINT's exact character-level column spacing and
BY/SUMBY box conventions cannot be reproduced byte-for-byte without a
reference listing to compare against (same limitation documented in
EIWBTR1C.py for PROC TABULATE), so a structured, fixed-width PROC
PRINT-style renderer is used instead: BY-line group headers, ID/VAR
columns using the program's own COMMA20.2 / RISK. formats, dashed
SUMBY subtotal lines, and PAGEBY-driven page breaks with title repeats.
"""

import gc
from pathlib import Path
from datetime import date, timedelta

import duckdb
import polars as pl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# Each physical input gets its own directory/path variable, per project
# convention, for clear traceability.
INPUT_BTMAST_DIR   = STG_DIR / "from_dwh"
INPUT_COLLATER_DIR = STG_DIR / "MNICOL"

CACHE_DIR = BASE_DIR / "input" / "cache" / "BTRD"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000
PAGE_SIZE  = 65          # OPTIONS PS=65 (explicit in the SAS source)
LINE_SIZE  = 150         # matches LRECL=150 on the //SASLIST DD
FF = "\f"
PAGE_NUM_COL  = 132         # page number right-edge on title line

# Report 2 column layout (total = 44 chars)
R2_ID_TOTAL   = 12          # RISK value right-justified in 6 + 6 trailing
R2_ID_VALUE   = 6           # risk label right-justified in this
R2_ID_TRAIL   = 6           # trailing spaces after value
R2_BNM_WIDTH  = 12          # BNMCODE column, left-justified
R2_NUM_WIDTH  = 20          # BALANCE, right-justified

# ============================================================================
# STEP 1: REPORT DATE  (DATA REPTDATE; SET BNM.REPTDATE; ...)
# No reptdate.parquet exists -- REPTDATE.py is the source of REPTDATE
# itself; the SELECT(DAY(REPTDATE)) derivation logic below is
# reimplemented locally exactly as written in the SAS DATA step.
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate

_day = reptdate.day
# SELECT(DAY(REPTDATE)):
#   WHEN(8)  DO; SDD=1;  WK='1'; WK1='4'; END;
#   WHEN(15) DO; SDD=9;  WK='2'; WK1='1'; END;
#   WHEN(22) DO; SDD=16; WK='3'; WK1='2'; END;
#   OTHERWISE DO; SDD=23; WK='4'; WK1='3'; END;
# SDD and WK1 are computed in the SAS source but their values are never
# referenced again anywhere in the program (WK1 is never even passed to
# CALL SYMPUT) -- dead computations, omitted here beyond this note.
WK = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"
NOWK = WK   # CALL SYMPUT('NOWK', PUT(WK,$1.));

MM = reptdate.month
# IF WK='1' THEN DO; MM1=MM-1; IF MM1=0 THEN MM1=12; END; ELSE MM1=MM;
# CALL SYMPUT('REPTMON1', PUT(MM1,Z2.)); -- REPTMON1 is produced but the
# macro variable &REPTMON1 is never referenced anywhere else in the SAS
# body (only &REPTMON and &NOWK drive the BTMAST filename/titles) -- MM1/
# REPTMON1 is therefore dead code and is not computed further here.

REPTMON  = f"{MM:02d}"                        # PUT(MONTH(REPTDATE),Z2.)
RDATE    = reptdate.strftime("%d/%m/%y")      # PUT(REPTDATE,DDMMYY8.)

# Generate time stamp
report_date = date.today() - timedelta(days=1)
ts = report_date.strftime("%y%m%d")

OUTPUT_DIR  = BASE_DIR / "output" / "BTRD"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / f"EIMBTCOL_{ts}.txt"   # fixed-name catalogued dataset, no date token

print(f"  RDATE        : {RDATE}")
print(f"  REPTMON/NOWK : {REPTMON}/{NOWK}")
print(f"  Output file  : {OUTPUT_FILE.name}")

# ============================================================================
# INPUT FILE NAMES
# ============================================================================
# INPUT_BTMAST_FILE   = INPUT_BTMAST_DIR / f"btmast{REPTMON}{NOWK}.sas7bdat"
# INPUT_COLLATER_FILE = INPUT_COLLATER_DIR / "collater.sas7bdat"   # fixed name, no date token

INPUT_BTMAST_FILE   = INPUT_BTMAST_DIR / f"btmast08426.sas7bdat"
INPUT_COLLATER_FILE = INPUT_COLLATER_DIR / "collater.sas7bdat"   # fixed name, no date token

# INPUT_BTRAD_FILE = STG_DIR / "from_dwh" / f"btrad{REPTMON}{NOWK}.sas7bdat"
# -- NOT loaded: see module docstring ("INPUTS DECLARED IN JCL BUT NOT
# USED") -- BNM.BTRAD&REPTMON&NOWK's PROC SORT output is overwritten by
# the very next statement before it is ever referenced.

print(f"  Input BTMAST    : {INPUT_BTMAST_FILE}")
print(f"  Input COLLATER  : {INPUT_COLLATER_FILE}")

# ============================================================================
# PROC FORMAT EQUIVALENT: VALUE RISK
# ============================================================================
_RISK_LABELS = {0: "  0%", 10: " 10%", 20: " 20%", 50: " 50%", 100: "100%"}


def risk_format(riskcat) -> str:
    if riskcat is None:
        return "    "
    return _RISK_LABELS.get(riskcat, str(riskcat))


# ============================================================================
# SAS-MISSING-AWARE ARITHMETIC HELPERS
# ============================================================================
def _sas_sum(*vals):
    """SAS SUM(): ignores missing; returns missing only if every argument
    is missing."""
    present = [v for v in vals if v is not None]
    return sum(present) if present else None


def _sas_div(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b


def _sas_mul(a, b):
    if a is None or b is None:
        return None
    return a * b


def _is_blank(x) -> bool:
    return x is None or str(x).strip() == ""


# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET
# (identical pattern to EIIMRM01.py / EIWBTR1C.py / EIBDLN1M.py)
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
BTMAST_CACHE   = _load_cached(INPUT_BTMAST_FILE, "BTMAST")
COLLATER_CACHE = _load_cached(INPUT_COLLATER_FILE, "COLLATER")

# ============================================================================
# STEP 3: PROC SORT DATA=BNM.BTMAST&REPTMON&NOWK OUT=BTRADE (KEEP=...)
#         NODUPKEYS; BY ACCTNO; WHERE SUBACCT='OV' AND CUSTCD NE ' '
#         AND BRANCH NE 0;
# ============================================================================
print("\nStep 3: Building BTRADE from BTMAST (filtered, deduped)...")

con = duckdb.connect(database=":memory:")
btmast_raw = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(BRANCH   AS DOUBLE)  AS BRANCH,
        CAST(ICURBAL  AS DOUBLE)  AS ICURBAL,
        CAST(DBALANCE AS DOUBLE)  AS DBALANCE,
        CAST(SECTORCD AS VARCHAR) AS SECTORCD
    FROM read_parquet('{BTMAST_CACHE.as_posix()}')
    WHERE TRIM(CAST(SUBACCT AS VARCHAR)) = 'OV'
      AND COALESCE(TRIM(CAST(CUSTCD AS VARCHAR)), '') <> ''
      AND COALESCE(CAST(BRANCH AS DOUBLE), 0) <> 0
""").pl()
con.close()

btrade_dedup = btmast_raw.sort("ACCTNO").unique(subset=["ACCTNO"], keep="first")
print(f"  BTRADE rows (deduped): {len(btrade_dedup):,}")

# ============================================================================
# STEP 4: DATA COLLATER; SET BTCOLL.COLLATER (RENAME=(ACCTNO=ACCTNO2));
#         ACCTNO = ACCTNO2;
#         PROC SORT DATA=COLLATER OUT=BTCOLL NODUPKEYS; BY ACCTNO;
#         (No account-number range WHERE filter in this program, unlike
#          EIWBTR1C's equivalent step.)
# ============================================================================
print("\nStep 4: Building BTCOLL from COLLATER (deduped by ACCTNO)...")

con = duckdb.connect(database=":memory:")
collater_raw = con.execute(f"""
    SELECT
        CAST(t.ACCTNO  AS BIGINT)  AS ACCTNO,
        CAST(t.CCLASSC AS VARCHAR) AS LIABCODE,
        CAST(t.CDOLARV AS DOUBLE)  AS CDOLARV
    FROM read_parquet('{COLLATER_CACHE.as_posix()}') AS t
""").pl()
con.close()

btcoll = collater_raw.sort("ACCTNO").unique(subset=["ACCTNO"], keep="first")
print(f"  BTCOLL rows (deduped): {len(btcoll):,}")

# ============================================================================
# STEP 5: DATA BTRADE; MERGE BTRADE(IN=A)
#              BTCOLL(KEEP=ACCTNO CCLASSC CDOLARV RENAME=(CCLASSC=LIABCODE));
#         BY ACCTNO; IF A;
# ============================================================================
print("\nStep 5: Building BTRADE (BTRADE LEFT JOIN BTCOLL)...")

btrade = btrade_dedup.join(btcoll, on="ACCTNO", how="left")
print(f"  BTRADE (merged) rows: {len(btrade):,}")

del btmast_raw, collater_raw, btrade_dedup, btcoll
gc.collect()

# ============================================================================
# STEP 6: DATA LOAN  (BNMCODE/RISKCAT classification + collateral split)
# ============================================================================
print("\nStep 6: Building LOAN (BNMCODE/RISKCAT classification, collateral split)...")


def _classify_liabcode(liabcode, sectorcd):
    """SELECT (LIABCODE); ... reproduced as an ordered chain matching the
    original WHEN clauses exactly (sets are disjoint, so order has no
    behavioural effect beyond mirroring the source for readability)."""
    code = "" if liabcode is None else str(liabcode).strip()
    if code in {"007", "012", "013", "014", "024", "048", "049", "117"}:
        return "30307", 0
    if code == "021":
        return "30309", 0
    if code in {"017", "026", "029"}:
        return "30009/17", 10
    if code in {"006", "016"}:
        return "30323", 20
    if code in {"011", "030"}:
        return "30325", 20
    if code in {"018", "027"}:
        return "30327", 20
    if code == "003":
        return "30009/10", 20
    if code == "025":
        return "30335", 20
    if code in {"050", "118"}:
        sector = "" if sectorcd is None else str(sectorcd).strip()
        riskcat = 50 if sector in {"0311", "0312", "0313", "0314", "0315", "0316"} else 100
        return "30341", riskcat
    if code in {"019", "028", "031"}:
        return "30351", 100
    return "30359", 100   # OTHERWISE


def _build_loan(rows) -> list:
    """SET BTRADE; AMTIND='D'; SELECT(LIABCODE) ...
    TOTBAL=SUM(ICURBAL,DBALANCE); COLVAL=(DBALANCE/TOTBAL)*CDOLARV;
    IF (COLVAL>0) & (DBALANCE>COLVAL) THEN DO;   -- 2-way OUTPUT split
       VBAL=DBALANCE; XBALANCE=COLVAL; OUTPUT;
       XBALANCE=VBAL-COLVAL; BNMCODE='30359'; RISKCAT=100; OUTPUT;
    END;
    ELSE DO; XBALANCE=DBALANCE; OUTPUT; END;"""
    out = []
    for r in rows:
        branch   = r["BRANCH"]
        icurbal  = r["ICURBAL"]
        dbalance = r["DBALANCE"]
        cdolarv  = r["CDOLARV"]

        bnmcode, riskcat = _classify_liabcode(r["LIABCODE"], r["SECTORCD"])
        totbal = _sas_sum(icurbal, dbalance)
        colval = _sas_mul(_sas_div(dbalance, totbal), cdolarv)

        if colval is not None and colval > 0 and dbalance is not None and dbalance > colval:
            vbal = dbalance
            out.append({"BRANCH": branch, "RISKCAT": riskcat, "BNMCODE": bnmcode,
                        "AMTIND": "D", "XBALANCE": colval})
            out.append({"BRANCH": branch, "RISKCAT": 100, "BNMCODE": "30359",
                        "AMTIND": "D", "XBALANCE": vbal - colval})
        else:
            out.append({"BRANCH": branch, "RISKCAT": riskcat, "BNMCODE": bnmcode,
                        "AMTIND": "D", "XBALANCE": dbalance})
    return out


loan_rows = _build_loan(btrade.iter_rows(named=True))
print(f"  LOAN rows: {len(loan_rows):,}")

# PROC SORT DATA=LOAN; BY BRANCH; is not reproduced: it is immediately
# followed only by PROC SUMMARY (an order-independent aggregation) and
# by a PROC PRINT whose BY BRANCH operates on the already class-sorted
# LNBR summary output, so this sort has no observable effect on the
# final report and is omitted per project convention (unnecessary sorts
# removed for efficiency).

del btrade
gc.collect()

# ============================================================================
# STEP 7: PROC SUMMARY DATA=LOAN NWAY; CLASS BRANCH RISKCAT BNMCODE;
#         VAR XBALANCE; OUTPUT OUT=LNBR SUM=;
# ============================================================================
print("\nStep 7: Summarising LOAN -> LNBR (SUM XBALANCE by BRANCH/RISKCAT/BNMCODE)...")


def _group_sum(rows, key_fields, sum_field="XBALANCE"):
    """PROC SUMMARY NWAY; SUM statistic ignores missing values; a group
    stays missing only if every contributing value is missing."""
    groups = {}
    for r in rows:
        key = tuple(r.get(f) for f in key_fields)
        g = groups.setdefault(key, None)
        v = r.get(sum_field)
        if v is not None:
            groups[key] = (g or 0.0) + v
        elif key not in groups:
            groups[key] = None
    out = []
    for key, total in groups.items():
        rec = dict(zip(key_fields, key))
        rec[sum_field] = total
        out.append(rec)
    return out


lnbr_rows = _group_sum(loan_rows, ["BRANCH", "RISKCAT", "BNMCODE"])
lnbr_rows.sort(key=lambda r: (r["BRANCH"], r["RISKCAT"], r["BNMCODE"]))
print(f"  LNBR rows: {len(lnbr_rows):,}")

# ============================================================================
# STEP 8: PROC SUMMARY DATA=LOAN; CLASS RISKCAT BNMCODE AMTIND;
#         VAR XBALANCE; OUTPUT OUT=LN SUM=;   ... WHERE _TYPE_=6;
# ============================================================================
print("\nStep 8: Summarising LOAN -> LN, selecting _TYPE_=6 (RISKCAT x BNMCODE)...")

# With CLASS RISKCAT BNMCODE AMTIND (bit weights 4/2/1 left-to-right),
# _TYPE_=6 (binary 110) means RISKCAT and BNMCODE are broken out while
# AMTIND is collapsed (summed over). AMTIND is always the constant 'D'
# in this program, so collapsing it changes nothing numerically -- the
# _TYPE_=6 subset is therefore reproduced directly as a group by
# (RISKCAT, BNMCODE), equivalent to the SAS WHERE _TYPE_=6 filter.
ln_rows = _group_sum(loan_rows, ["RISKCAT", "BNMCODE"])
ln_rows.sort(key=lambda r: (r["RISKCAT"], r["BNMCODE"]))
print(f"  LN rows (_TYPE_=6 equivalent): {len(ln_rows):,}")

del loan_rows
gc.collect()

# ============================================================================
# STEP 9: REPORT RENDERING  (PROC PRINT emulation)
# ============================================================================
print("\nStep 9: Rendering reports...")

ID_WIDTH      = 11          # report 1: BNMCODE ID column (was 12)
NUM_WIDTH     = 20          # COMMA20.2


def _fmt_comma(value, width=NUM_WIDTH, decimals=2) -> str:
    """COMMAw.d. Absent cell (None) prints blank (PROC PRINT default)."""
    if value is None:
        return " " * width
    v = float(value)
    if abs(v) < 0.5 * 10 ** -decimals:
        v = 0.0
    s = f"{v:,.{decimals}f}"
    if len(s) > width:
        s = s[-width:]
    return s.rjust(width)


def _pad(line: str) -> str:
    """Pad every written line to LRECL=150 (RECFM=FB)."""
    return line.ljust(LINE_SIZE)


def _title_line(title: str, page_num: int) -> str:
    """TITLE1 line: text + right-aligned page number ending at
    PAGE_NUM_COL, then padded to LINE_SIZE."""
    return _pad(title + str(page_num).rjust(PAGE_NUM_COL - len(title)))


def _page_header(title4: str, page_num: int, first_page: bool = False) -> list:
    title_line = _title_line("REPORT ID: EIMBTCOL", page_num)
    if not first_page:
        title_line = FF + title_line      # FF embedded at start of line 1
    return [
        title_line,
        _pad("PUBLIC BANK BERHAD"),
        _pad(f"TOTAL TRADE BILLS BY COLLATERAL AS AT : {RDATE}"),
        _pad(title4),
        _pad(""),
    ]


def _r1_header() -> str:
    return _pad("BNMCODE".ljust(ID_WIDTH) + "BALANCE (RM)".rjust(NUM_WIDTH))


def _r1_rule() -> str:
    # "-------" under BNMCODE (7 dashes) + 4 spaces + 20 dashes
    return _pad("-" * 7 + " " * (ID_WIDTH - 7) + "-" * NUM_WIDTH)


def _r1_grand_rule() -> str:
    return _pad(" " * ID_WIDTH + "=" * NUM_WIDTH)


def _r1_row(bnmcode, amount) -> str:
    return _pad(str(bnmcode).ljust(ID_WIDTH) + _fmt_comma(amount))


def _r1_subtotal(label: str, amount) -> str:
    return _pad(label.ljust(ID_WIDTH) + _fmt_comma(amount))


def _r1_grand_total(amount) -> str:
    return _pad(" " * ID_WIDTH + _fmt_comma(amount))


def render_report1(rows: list) -> tuple:
    """PROC PRINT DATA=LNBR; VAR XBALANCE; ID BNMCODE;
       BY BRANCH RISKCAT; SUMBY RISKCAT; PAGEBY BRANCH; SUM XBALANCE;
       (SUMBY/SUM lines suppressed when the owning BY group has <= 1 obs.)"""
    output: list = []
    page_num = 1
    first_page = True

    def _group_sum(group):
        s = 0.0
        for r in group:
            if r["XBALANCE"] is not None:
                s += r["XBALANCE"]
        return s

    branches = sorted({r["BRANCH"] for r in rows}, key=lambda b: int(float(b)))

    for branch in branches:
        # ---- new page for this branch (PAGEBY BRANCH) ----
        output.extend(_page_header("BY BRANCHES", page_num, first_page))
        first_page = False
        page_num += 1

        branch_rows = [r for r in rows if r["BRANCH"] == branch]
        riskcats = sorted({r["RISKCAT"] for r in branch_rows})

        for r_idx, riskcat in enumerate(riskcats):
            group_rows = [r for r in branch_rows if r["RISKCAT"] == riskcat]

            # combined BY-line
            output.append(_pad(
                f"BRANCH={int(float(branch))} "
                f"RISK CATEGORY={risk_format(riskcat)}"
            ))
            output.append(_pad(""))
            output.append(_r1_header())
            output.append(_pad(""))

            for r in group_rows:
                output.append(_r1_row(r["BNMCODE"], r["XBALANCE"]))

            # SUMBY RISKCAT (only when riskcat group has > 1 obs)
            if len(group_rows) > 1:
                output.append(_r1_rule())
                output.append(_r1_subtotal("RISKCAT", _group_sum(group_rows)))

            # 2 blank lines before next riskcat group inside the branch
            if r_idx < len(riskcats) - 1:
                output.append(_pad(""))
                output.append(_pad(""))

        # SUM (BRANCH subtotal) — only when branch has > 1 obs
        if len(branch_rows) > 1:
            last_group = [r for r in branch_rows if r["RISKCAT"] == riskcats[-1]]
            if len(last_group) <= 1:        # last riskcat didn't emit a rule
                output.append(_r1_rule())
            output.append(_r1_subtotal("BRANCH", _group_sum(branch_rows)))

    # final report total (=====), appended after the last branch
    output.append(_r1_grand_rule())
    output.append(_r1_grand_total(
        sum(r["XBALANCE"] for r in rows if r["XBALANCE"] is not None)
    ))

    return output, page_num


def _r2_header_line1() -> str:
    return _pad("RISK".rjust(R2_ID_VALUE))                       # "  RISK"


def _r2_header_line2() -> str:
    return _pad(
        "CATEGORY".ljust(R2_ID_TOTAL)
        + "BNMCODE".ljust(R2_BNM_WIDTH)
        + "BALANCE (RM)".rjust(R2_NUM_WIDTH)
    )


def _r2_rule() -> str:
    # 8 dashes + 16 spaces + 20 dashes = 44
    return _pad("-" * 8 + " " * (R2_ID_TOTAL + R2_BNM_WIDTH - 8) + "-" * R2_NUM_WIDTH)


def _r2_grand_rule() -> str:
    return _pad(" " * (R2_ID_TOTAL + R2_BNM_WIDTH) + "=" * R2_NUM_WIDTH)


def _r2_row(risk_label: str, bnmcode, amount, first_in_group: bool) -> str:
    id_part = (risk_label.rjust(R2_ID_VALUE) + " " * R2_ID_TRAIL) \
              if first_in_group else " " * R2_ID_TOTAL
    return _pad(id_part + str(bnmcode).ljust(R2_BNM_WIDTH)
                + _fmt_comma(amount).rjust(R2_NUM_WIDTH))


def _r2_subtotal_row(risk_label: str, amount) -> str:
    return _pad(
        risk_label.rjust(R2_ID_VALUE) + " " * R2_ID_TRAIL
        + " " * R2_BNM_WIDTH
        + _fmt_comma(amount).rjust(R2_NUM_WIDTH)
    )


def _r2_grand_total_row(amount) -> str:
    return _pad(" " * (R2_ID_TOTAL + R2_BNM_WIDTH)
                + _fmt_comma(amount).rjust(R2_NUM_WIDTH))


def render_report2(rows: list, start_page: int) -> list:
    """PROC PRINT DATA=LN; VAR BNMCODE XBALANCE; ID RISKCAT; BY RISKCAT;
       SUMBY RISKCAT; SUM XBALANCE; WHERE _TYPE_=6;"""
    output: list = []
    output.extend(_page_header(
        "SUMMARY REPORT FOR ALL LOANS (TRADE BILLS)", start_page, first_page=False
    ))
    output.append(_r2_header_line1())
    output.append(_r2_header_line2())
    output.append(_pad(""))

    riskcats = sorted({r["RISKCAT"] for r in rows})
    grand_total = 0.0

    for riskcat in riskcats:
        group_rows = [r for r in rows if r["RISKCAT"] == riskcat]
        label = risk_format(riskcat)

        group_sum = 0.0
        for i, r in enumerate(group_rows):
            output.append(_r2_row(label, r["BNMCODE"], r["XBALANCE"], i == 0))
            if r["XBALANCE"] is not None:
                group_sum += r["XBALANCE"]

        output.append(_r2_rule())
        output.append(_r2_subtotal_row(label, group_sum))
        output.append(_pad(""))

        grand_total += group_sum

    output.append(_r2_grand_rule())
    output.append(_r2_grand_total_row(grand_total))
    return output


r1_lines, next_page = render_report1(lnbr_rows)
r2_lines = render_report2(ln_rows, start_page=next_page - 1)   # same number as last R1 page
report_lines = r1_lines + r2_lines

# ============================================================================
# STEP 10: WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")

print("\nEIMBTCOL complete.")
