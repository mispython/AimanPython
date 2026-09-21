#!/usr/bin/env python3
"""
Program : EIMBTLIQ.py
Purpose : New Liquidity Framework - Contractual Run-Off For Trade Bills.
          Breakdown by behavioural maturity profile (Part 1-RM: loans
          using repayment/billing-date amortisation).

Dependency:
    %INC PGM(PBBLNFMT,PBBDPFMT);
    Neither PBBLNFMT nor PBBDPFMT format/list is ever applied via a direct
    PUT(var,format.) call anywhere in this program's body. PROD is
    hardcoded to the literal 'BT' (not derived from any PBBLNFMT product
    format), and PRODCD/PRODUCT/CUSTCD are tested directly with
    SUBSTR()/IN() rather than through a format lookup. Per project
    convention, a session-level %INC with no direct call in the program
    body is documented only, never imported as a live dependency:
        # from PBBLNFMT import ...   -- NOT USED, no PUT(...,fmt.) call in this program
        # from PBBDPFMT import ...   -- NOT USED, no PUT(...,fmt.) call in this program

============================================================================
PHYSICAL INPUT DATASETS  (cached to Parquet independently)
============================================================================
1. btrad<REPTMON><NOWK>.sas7bdat
   (JCL //BNM1 DD DSN=SAP.BT.SASDATA)
   SAS ref : BNM1.BTRAD&REPTMON&NOWK
   Filename is deterministic from REPTMON + NOWK (exact-day NOWK -- see
   Step 1; live here, drives this filename -- same pattern as EIWBTR1C's
   BTMAST input, and unlike EIMBTCOL where the equivalent BTRAD input was
   dead code).
   Cols used (resolved from the KEEP=/SET usage in DATA NOTE below):
       ACCTNO, PRODCD, PRODUCT, CUSTCD, ISSDTE, EXPRDATE, BALANCE,
       PAYAMT, LOANSTAT.

============================================================================
OUTPUT
============================================================================
//SASLIST DD DSN=SAP.PBB.NLFMBT.TEXT, DISP=(NEW,CATLG,DELETE),
          DCB=(RECFM=FB,LRECL=80,BLKSIZE=6320)
RECFM=FB (NOT FBA) -> per project convention, no ASA carriage-control
byte; plain fixed-width text, form-feed page breaks. OPTIONS NONUMBER ->
no page-number suffix on the title line (unlike EIMBTCOL / EIWBTR1C,
which both used NUMBER). PAGESIZE is not specified in the SAS source ->
default 60 lines/page. LRECL=80 tightly bounds each physical output
line, so the single-statistic ITEM x REMMTH crosstab is horizontally
paginated into '(Continued)' column-group segments, following the same
established PROC TABULATE chunk-wrap renderer pattern used in
EIIMRM01.py / EIWBTR1C.py. Fixed-name catalogued dataset (no date token
in the DSN) -> a static base filename is used, with the same date-stamp
suffix convention.

NOTE on rendering: PROC TABULATE's exact character-level box drawing
cannot be reproduced byte-for-byte without a reference listing (same
limitation documented in EIWBTR1C.py / EIMBTCOL.py), so the same
structured, bordered crosstab renderer approach is reused here.
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

# from PBBLNFMT import ...   -- NOT USED, no PUT(...,fmt.) call in this program body
# from PBBDPFMT import ...   -- NOT USED, no PUT(...,fmt.) call in this program body

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_BTRAD_DIR = STG_DIR / "from_dwh"

CACHE_DIR = BASE_DIR / "input" / "cache" / "BTRD"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "BTRD"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS  = 500_000
PAGE_SIZE   = 60      # lines per page (not specified in SAS -> default)
LINE_SIZE   = 80      # matches LRECL=80 on the //SASLIST DD
LABEL_WIDTH = 45      # RTS=45
NUM_WIDTH   = 20      # COMMA20.2
FF = "\f"

# Safety cap for the %NXTBLDT billing-date advance loops below: if ISSDTE
# is genuinely missing, the original SAS macro would also loop forever
# (a missing BLDATE always compares as "<= REPTDATE"/"<= EXPRDATE" since
# SAS missing sorts as LOW), so this cap only guards the Python process
# against a hang on such bad data -- it has no effect for any row with a
# valid ISSDTE/EXPRDATE.
_MAX_NXTBLDT_ITER = 2000

# ============================================================================
# STEP 1: REPORT DATE + RUN-OFF DATE
# (DATA REPTDATE -- this SELECT(DAY(REPTDATE)) derivation appears twice,
#  identically, in the SAS source with no intervening change, so it is
#  computed once here.
#  DATA _NULL_ (determine run-off date) -- RUNOFFDT = last calendar day
#  of the report month.)
# ============================================================================
print("Step 1: Deriving report date and run-off date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate

_day = reptdate.day
NOWK = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"

REPTYEAR = reptdate.strftime("%Y")          # PUT(REPTDATE,YEAR4.)
REPTMON  = reptdate.strftime("%m")          # PUT(MONTH(REPTDATE),Z2.)
REPTDAY  = reptdate.strftime("%d")          # PUT(DAY(REPTDATE),Z2.)
RDATE    = reptdate.strftime("%d/%m/%y")    # PUT(REPTDATE,DDMMYY8.)

# %DCLVAR's RD1-RD12 (RPDAYS) / D1-D12 (LDAY) arrays -- days-per-month for
# the report year. MD1-MD12 (MDDAYS) is also RETAINed/declared by the
# original DCLVAR macro but is never referenced by any live logic in
# %REMMTH or %NXTBLDT (only assigned to, inside %REMMTH's dead MD2 line
# below) -- a dead declaration, omitted here, same as the equivalent dead
# array documented in EIIMRM01.py / EIWBTR1C.py.
RD_DAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
if reptdate.year % 4 == 0:
    RD_DAYS[1] = 29

# RUNOFFDT = MDY(RPMTH,RPDAYS(RPMTH),RPYR); -- last day of the report month
RUNOFFDT = date(reptdate.year, reptdate.month, RD_DAYS[reptdate.month - 1])

# Generate time stamp
report_date = date.today() - timedelta(days=1)
ts = report_date.strftime("%y%m%d")

OUTPUT_FILE = OUTPUT_DIR / f"EIMBTLIQ_{ts}.txt"

print(f"  RDATE        : {RDATE}")
print(f"  REPTMON/NOWK : {REPTMON}/{NOWK}")
print(f"  RUNOFFDT     : {RUNOFFDT.isoformat()}")
print(f"  Output file  : {OUTPUT_FILE.name}")

# ============================================================================
# INPUT FILE NAME  (deterministic, built directly from date tokens)
# ============================================================================
INPUT_BTRAD_FILE = INPUT_BTRAD_DIR / f"btrad{REPTMON}{NOWK}.sas7bdat"
INPUT_BTRAD_FILE = INPUT_BTRAD_DIR / f"btrad08426.sas7bdat"
print(f"  Input BTRAD  : {INPUT_BTRAD_FILE}")

# ============================================================================
# PROC FORMAT EQUIVALENTS (local to this program)
# ============================================================================
REMFMT_LABELS = ["UP TO 1 WK", ">1 WK - 1 MTH", ">1 MTH"]


def remfmt_format(value):
    """PROC FORMAT VALUE REMFMT. -- LOW-0.255 is listed first, so a value
    exactly on that boundary matches this earlier range; SAS numeric
    missing sorts as LOW and therefore also falls into the
    'UP TO 1 WK' bucket."""
    if value is None or value <= 0.255:
        return "UP TO 1 WK"
    if value <= 1:
        return ">1 WK - 1 MTH"
    return ">1 MTH"


# VALUE $ITEMF. -- full label table as declared in the SAS source. Many
# codes here (A1.12 onward) are never produced by this program's own
# ITEM-classification logic in DATA NOTE below (this excerpt only ever
# computes 'A1.01','A1.02','A1.04','A1.05','A1.08','A1.08A'), but the
# full format is kept for fidelity with the SAS format library.
ITEMF_MAP = {
    'A1.01':  'A1.01  LOANS: CORP - FIXED TERM LOANS',
    'A1.02':  'A1.02  LOANS: CORP - REVOLVING LOANS',
    'A1.03':  'A1.03  LOANS: CORP - OVERDRAFTS',
    'A1.04':  'A1.04  LOANS: CORP - OTHERS',
    'A1.05':  'A1.05  LOANS: IND  - HOUSING LOANS',
    'A1.07':  'A1.07  LOANS: IND  - OVERDRAFTS',
    'A1.08':  'A1.08  LOANS: IND  - OTHERS',
    'A1.08A': 'A1.08A LOANS: IND  - REVOLVING LOANS',
    'A1.12':  'A1.12  DEPOSITS: CORP - FIXED',
    'A1.13':  'A1.13  DEPOSITS: CORP - SAVINGS',
    'A1.14':  'A1.14  DEPOSITS: CORP - CURRENT',
    'A1.15':  'A1.15  DEPOSITS: IND  - FIXED',
    'A1.16':  'A1.16  DEPOSITS: IND  - SAVINGS',
    'A1.17':  'A1.17  DEPOSITS: IND  - CURRENT',
    'A1.25':  'A1.25  UNDRAWN OD FACILITIES GIVEN',
    'A1.28':  'A1.28  UNDRAWN PORTION OF OTHER C/F GIVEN',
    'A2.01':  'A2.01  INTERBANK LENDING/DEPOSITS',
    'A2.02':  'A2.02  REVERSE REPO',
    'A2.03':  'A2.03  DEBT SEC: GOVT PP/BNM BILLS/CAG',
    'A2.04':  'A2.04  DECT SEC: FIN INST PAPERS',
    'A2.05':  'A2.05  DEBT SEC: TRADE PAPERS',
    'A2.06':  'A2.06  CORP DEBT: GOVT-GUARANTEED',
    'A2.08':  'A2.08  CORP DEBT: NON-GUARANTEED',
    'A2.09':  'A2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'A2.14':  'A2.14  INTERBANK BORROWINGS/DEPOSITS',
    'A2.15':  'A2.15  INTERBANK REPOS',
    'A2.16':  'A2.16  NON-INTERBANK REPOS',
    'A2.17':  'A2.17  NIDS ISSUED',
    'A2.18':  'A2.18  BAS PAYABLE',
    'A2.19':  'A2.19  FX EXCHG CONTRACTS PAYABLE',
    'B1.12':  'B1.12  DEPOSITS: CORP - FIXED',
    'B1.15':  'B1.15  DEPOSITS: IND  - FIXED',
    'B2.01':  'B2.01  INTERBANK LENDING/DEPOSITS',
    'B2.09':  'B2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'B2.14':  'B2.14  INTERBANK BORROWINGS/DEPOSITS',
    'B2.19':  'B2.19  FX EXCHG CONTRACTS PAYABLE',
}


def itemf_format(code):
    """PUT(ITEM,$ITEMF.); -- the format has no OTHER clause, so an
    unmatched code is returned unchanged (same no-OTHER-clause semantics
    already documented for $FISSTYPE/$FISSGROUP in PBBLNFMT.py)."""
    return ITEMF_MAP.get(code, code)


# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET
# (identical pattern to EIIMRM01.py / EIWBTR1C.py / EIMBTCOL.py)
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
# STEP 2: CACHE INPUT SAS FILE TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS dataset to Parquet...")
BTRAD_CACHE = _load_cached(INPUT_BTRAD_FILE, "BTRAD")

# ============================================================================
# STEP 3: LOAD BTRAD  (PROC SORT DATA=BNM1.BTRAD... OUT=NOTEX; BY ACCTNO;
#         is NOT reproduced: DATA NOTE below processes every row
#         independently -- the only order-sensitive construct is the
#         _N_=1 check used solely to read the (constant) REPTDATE value,
#         which is unaffected by row order -- and the final PROC SUMMARY
#         aggregation is itself order-independent, so this sort has no
#         observable effect on the report and is omitted per project
#         convention (unnecessary sorts removed for efficiency).)
# ============================================================================
print("\nStep 3: Loading BTRAD...")

con = duckdb.connect(database=":memory:")
btrad_raw = con.execute(f"""
    SELECT
        CAST(t.ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(t.PRODCD   AS VARCHAR) AS PRODCD,
        CAST(t.PRODUCT  AS INTEGER) AS PRODUCT,
        CAST(t.CUSTCD   AS VARCHAR) AS CUSTCD,
        (DATE '1960-01-01' + CAST(t.ISSDTE   AS INTEGER)) AS ISSDTE,
        (DATE '1960-01-01' + CAST(t.EXPRDATE AS INTEGER)) AS EXPRDATE,
        CAST(t.BALANCE  AS DOUBLE)  AS BALANCE,
        CAST(t.PAYAMT   AS DOUBLE)  AS PAYAMT,
        CAST(t.LOANSTAT AS INTEGER) AS LOANSTAT
    FROM read_parquet('{BTRAD_CACHE.as_posix()}') AS t
""").pl()
con.close()
print(f"  BTRAD rows: {len(btrad_raw):,}")

# ============================================================================
# STEP 4: DATA NOTE (KEEP=PART ITEM REMMTH AMOUNT)
# ============================================================================
print("\nStep 4: Building NOTE (ITEM classification + maturity amortisation)...")


def _nxtbldt(bldate: date, issdte, payfreq: str, freq: int, lday: list) -> date:
    """%MACRO NXTBLDT; -- PAYFREQ is hardcoded to '3' at every call site
    in DATA NOTE below (this program only ever computes FREQ=6 months),
    so the PAYFREQ='6' branch is structurally unreachable in this
    program; preserved verbatim as dead code per project convention for
    unreachable SAS branches. lday (D1-D12 / LDAY array) is mutated in
    place and RETAINed across every row and every call, exactly as the
    SAS RETAIN semantics require."""
    if payfreq == '6':
        dd = bldate.day + 14
        mm = bldate.month
        yy = bldate.year
        if mm == 2:
            lday[1] = 29 if yy % 4 == 0 else 28
        if dd > lday[mm - 1]:
            dd -= lday[mm - 1]
            mm += 1
            if mm > 12:
                mm -= 12
                yy += 1
    else:
        dd = issdte.day if issdte is not None else bldate.day
        mm = bldate.month + freq
        yy = bldate.year
        if mm > 12:
            mm -= 12
            yy += 1

    if mm == 2:
        lday[1] = 29 if yy % 4 == 0 else 28
    if dd > lday[mm - 1]:
        dd = lday[mm - 1]
    return date(yy, mm, dd)


def _remmth(matdt: date, rpyr: int, rpmth: int, rpday: int, rd_days: list) -> float:
    """%MACRO REMMTH; -- MD2 (the MDDAYS array's leap-year override) is
    assigned in the original macro but MDDAYS is never referenced
    elsewhere (dead, same as the equivalent dead MD1-MD12 array noted
    above), so it is omitted here beyond this note."""
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    days_in_rpmth = rd_days[rpmth - 1]
    if mdday > days_in_rpmth:
        mdday = days_in_rpmth
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / days_in_rpmth


def _needs_clamp(bldate: date, exprdate: date, balance, payamt) -> bool:
    """IF BLDATE > EXPRDATE | BALANCE <= PAYAMT THEN BLDATE = EXPRDATE;
    A missing BALANCE sorts as LOW in SAS, so 'BALANCE <= PAYAMT' is TRUE
    whenever BALANCE is missing."""
    return bldate > exprdate or balance is None or balance <= payamt


def _build_note_rows(rows_iter, reptdate_dt: date, runoffdt: date, rd_days: list) -> list:
    """SET NOTEX; BLDATE = EXPRDATE; ... See module docstring / inline
    comments for the transcription of the SAS logic: PROD hardcode
    (dead SELECT branches), ITEM classification, the DAYS-past-due
    computation, the three-way REMMTH bucket test against RUNOFFDT, and
    the periodic-installment DO WHILE amortisation loop."""
    rpyr, rpmth, rpday = runoffdt.year, runoffdt.month, runoffdt.day
    lday = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]   # D1-D12 (RETAIN)

    rows = []
    for r in rows_iter:
        prodcd   = r["PRODCD"]
        product  = r["PRODUCT"]
        custcd   = r["CUSTCD"]
        issdte   = r["ISSDTE"]
        exprdate = r["EXPRDATE"]
        balance  = r["BALANCE"]
        payamt   = r["PAYAMT"]
        loanstat = r["LOANSTAT"]

        bldate = exprdate

        # IF SUBSTR(PRODCD,1,2)='34' OR PRODUCT IN (225,226); (subsetting IF)
        code2 = "" if prodcd is None else str(prodcd)[:2]
        if not (code2 == "34" or product in (225, 226)):
            continue

        # PROD = 'BT'; -- hardcoded literal, so the SELECT(PROD) below can
        # only ever reach its OTHERWISE branch; the 'HL'/'RC'/'FL' WHEN
        # clauses are structurally unreachable and are preserved verbatim
        # as dead code, matching project convention.
        prod = 'BT'
        cust = "" if custcd is None else str(custcd).strip()
        if cust in ("77", "78", "95", "96"):
            if prod == 'HL':
                item = 'A1.05'
            elif prod == 'RC':
                item = 'A1.08A'
            else:
                item = 'A1.08'
        else:
            if prod == 'FL':
                item = 'A1.01'
            elif prod == 'RC':
                item = 'A1.02'
            else:
                item = 'A1.04'

        if product == 100:
            item = 'A1.05'   # HARDCODE BY MAZNI

        days = None
        if bldate is not None:
            days = (reptdate_dt - bldate).days

        remmth = None

        if exprdate is None or exprdate <= runoffdt:
            remmth = None
        elif (exprdate - runoffdt).days < 8:
            remmth = 0.1
        else:
            payfreq = '3'
            freq = 6   # SELECT(PAYFREQ): WHEN('3') FREQ=6; -- only reachable branch

            if product in (350, 910, 925):
                bldate = exprdate
            elif bldate is None:
                # Structurally unreachable in this program: EXPRDATE (and
                # therefore BLDATE) is guaranteed non-missing at this
                # point, since the branch above already requires
                # EXPRDATE > RUNOFFDT. Preserved verbatim per the SAS
                # source's ELSE IF BLDATE<=0 structure.
                bldate = issdte
                _iter = 0
                while bldate is not None and bldate <= reptdate_dt and _iter < _MAX_NXTBLDT_ITER:
                    bldate = _nxtbldt(bldate, issdte, payfreq, freq, lday)
                    _iter += 1

            if payamt is None or payamt < 0:
                payamt = 0.0

            if _needs_clamp(bldate, exprdate, balance, payamt):
                bldate = exprdate

            _iter = 0
            while bldate <= exprdate and _iter < _MAX_NXTBLDT_ITER:
                if bldate <= runoffdt:
                    remmth = None
                elif (bldate - runoffdt).days < 8:
                    remmth = 0.1
                else:
                    remmth = _remmth(bldate, rpyr, rpmth, rpday, rd_days)

                if (remmth is not None and remmth > 1) or bldate == exprdate:
                    break

                amount = payamt
                balance = balance - payamt
                if (days is not None and days > 89) or (loanstat != 1):
                    remmth = 13.0
                rows.append({"PART": "1-RM", "ITEM": item, "REMMTH": remmth, "AMOUNT": amount})

                bldate = _nxtbldt(bldate, issdte, payfreq, freq, lday)
                if _needs_clamp(bldate, exprdate, balance, payamt):
                    bldate = exprdate
                _iter += 1

        amount = balance
        if days is not None and days > 89:
            remmth = 13.0
        rows.append({"PART": "1-RM", "ITEM": item, "REMMTH": remmth, "AMOUNT": amount})

    return rows


note_rows = _build_note_rows(btrad_raw.iter_rows(named=True), reptdate, RUNOFFDT, RD_DAYS)
print(f"  NOTE rows (pre-summary): {len(note_rows):,}")

del btrad_raw
gc.collect()

# ============================================================================
# STEP 5: PROC SUMMARY DATA=NOTE NWAY; CLASS PART ITEM REMMTH; VAR AMOUNT;
#         OUTPUT OUT=NOTE(DROP=_TYPE_ _FREQ_) SUM=;
# ============================================================================
print("\nStep 5: Summarising NOTE (SUM AMOUNT by PART/ITEM/REMMTH)...")


def _group_sum(rows, key_fields, sum_field="AMOUNT"):
    """PROC SUMMARY NWAY; SUM statistic ignores missing values; a group
    stays missing only if every contributing value is missing."""
    groups = {}
    for r in rows:
        key = tuple(r.get(f) for f in key_fields)
        v = r.get(sum_field)
        if v is not None:
            groups[key] = (groups.get(key) or 0.0) + v
        elif key not in groups:
            groups[key] = None
    out = []
    for key, total in groups.items():
        rec = dict(zip(key_fields, key))
        rec[sum_field] = total
        out.append(rec)
    return out


note_summary = _group_sum(note_rows, ["PART", "ITEM", "REMMTH"])
print(f"  NOTE summary rows: {len(note_summary):,}")

del note_rows
gc.collect()

# WHERE PART = '1-RM'; -- PART is always the literal '1-RM' in this
# program, so this filter is retained for fidelity but is a no-op.
note_1rm = [r for r in note_summary if r["PART"] == "1-RM"]

# ============================================================================
# STEP 6: REPORT RENDERING  (PROC TABULATE emulation)
# ============================================================================
print("\nStep 6: Rendering report...")


def _center(text: str, width: int) -> str:
    text = text[:width]
    pad = width - len(text)
    left = pad // 2
    right = pad - left
    return " " * left + text + " " * right


def _dashes(width: int) -> str:
    return "-" * width


def _fmt_comma20(value) -> str:
    """F=COMMA20.2 with OPTIONS MISSING=0: a genuinely absent cell (no
    contributing rows -> None) renders as a bare '0', a real computed
    value renders fully comma/decimal-formatted."""
    if value is None:
        return "0".rjust(NUM_WIDTH)
    v = float(value)
    if abs(v) < 0.005:
        v = 0.0
    s = f"{v:,.2f}"
    if len(s) > NUM_WIDTH:
        s = f"{v:.2f}"
    if len(s) > NUM_WIDTH:
        s = s[-NUM_WIDTH:]
    return s.rjust(NUM_WIDTH)


def _title_block() -> list:
    return [
        "PUBLIC BANK BERHAD",
        f"NEW LIQUIDITY FRAMEWORK AS AT {RDATE}",
        "(CONTRACTUAL RUN-OFF FOR TRADE BILLS)",
        "BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE (PART 1-RM)",
        "",
    ]


def _render_tabulate(rows: list, box_label: str) -> list:
    """
    Emulates: TABLE ITEM=' ',(REMMTH=' ' ALL='TOTAL')*
                     (SUM=' '*AMOUNT=' '*F=COMMA20.2)
              / BOX='CORE (NON-TRADING) BANKING ACTIVITIES' RTS=45 CONDENSE;

    REMMTH is grouped here by its formatted REMFMT. bucket, matching how
    PROC TABULATE re-aggregates a CLASS variable under its display format
    even though the preceding PROC SUMMARY (NWAY) grouped by the raw,
    un-formatted REMMTH value.

    Horizontal pagination: LRECL=80 cannot fit RTS=45 plus even one full
    COMMA20.2 column group side by side without wrapping, so column
    groups are split into '(Continued)' chunks exactly as in the
    established PROC TABULATE renderer (EIIMRM01.py / EIWBTR1C.py).
    Vertical pagination: PAGE_SIZE (60, default) triggers a new page with
    repeated titles/headers.
    """
    col_keys = list(REMFMT_LABELS)

    cell = {}
    for r in rows:
        bucket = remfmt_format(r["REMMTH"])
        key = (r["ITEM"], bucket)
        cell[key] = (cell.get(key) or 0.0) + (r["AMOUNT"] or 0.0)

    row_vals = sorted({r["ITEM"] for r in rows})

    row_totals = {}
    for item in row_vals:
        tot = None
        for bucket in col_keys:
            v = cell.get((item, bucket))
            if v is not None:
                tot = (tot or 0.0) + v
        row_totals[item] = tot

    all_cols = col_keys + ["TOTAL"]
    max_cols_per_chunk = max(1, (LINE_SIZE - (LABEL_WIDTH + 2)) // (NUM_WIDTH + 1))
    chunks = [
        all_cols[i:i + max_cols_per_chunk]
        for i in range(0, len(all_cols), max_cols_per_chunk)
    ]

    output: list = []

    for chunk_idx, cols in enumerate(chunks):
        n = len(cols)
        total_width = LABEL_WIDTH + 2 + n * (NUM_WIDTH + 1)
        state = {"lines_on_page": 0}

        def _emit_page(with_titles: bool):
            block = []
            if with_titles:
                block.append(FF)
                block.extend(_title_block())
            block.append(_dashes(total_width))
            header_cells = [_center(c, NUM_WIDTH) for c in cols]
            block.append("|" + " " * LABEL_WIDTH + "|" + "|".join(header_cells) + "|")
            block.append(
                "|" + box_label.ljust(LABEL_WIDTH)[:LABEL_WIDTH] + "+"
                + "+".join(_dashes(NUM_WIDTH) for _ in cols) + "|"
            )
            output.extend(block)
            state["lines_on_page"] = len(block)

        _emit_page(with_titles=(chunk_idx == 0))

        for item in row_vals:
            if state["lines_on_page"] >= PAGE_SIZE:
                _emit_page(with_titles=True)

            label = itemf_format(item).ljust(LABEL_WIDTH)[:LABEL_WIDTH]
            row_cells = []
            for c in cols:
                v = row_totals[item] if c == "TOTAL" else cell.get((item, c))
                row_cells.append(_fmt_comma20(v))
            output.append("|" + label + "|" + "|".join(row_cells) + "|")
            state["lines_on_page"] += 1

        output.append(_dashes(total_width))

        if chunk_idx < len(chunks) - 1:
            output.append("")
            output.append("(Continued)")
            output.append("")

    return output


report_lines = _render_tabulate(note_1rm, "CORE (NON-TRADING) BANKING ACTIVITIES")

# ============================================================================
# STEP 7: WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")

print("\nEIMBTLIQ complete.")
