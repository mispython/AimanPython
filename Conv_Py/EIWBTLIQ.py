#!/usr/bin/env python3
"""
Program : EIWBTLIQ.py
Purpose : New Liquidity Framework - Contractual Run-Off For Trade Bills
          (weekly). Produces BNMCODE-coded maturity profile of trade bills
          (BTRAD) and the undrawn portion of trade bill facilities (BTMAST).

Relationship to EIMBTLIQ:
    Same skeleton (trade-bill filter, %DCLVAR / %NXTBLDT / %REMMTH macros,
    repayment-date amortisation loop) but a different program: BNMCODE-coded
    output with a 6-bucket REMFMT, 346-product split ('96'/'94' vs '95'/'93'),
    USD/SGD amounts, an undrawn-portion step from BTMAST, PROC PRINT instead
    of PROC TABULATE, remaining months measured from REPTDATE (not a
    month-end run-off date) and BLDATE read from the input (not EXPRDATE).

Dependency:
    %INC PGM(PBBLNFMT,PBBELF);
    Neither PBBLNFMT nor PBBELF format is ever applied via a direct
    PUT(var,format.) call in this program's body. PROD is hardcoded to the
    literal 'BT'; PRODCD/PRODUCT/CUSTCD are tested directly with SUBSTR()/IN()
    rather than through a format lookup. PRDFMT is declared locally in
    PROC FORMAT but is never applied either. Per project convention, a
    session-level %INC with no direct call in the program body is documented
    only, never imported as a live dependency:
        # from PBBLNFMT import ...   -- NOT USED, no PUT(...,fmt.) call in this program
        # from PBBELF import ...     -- NOT USED, no PUT(...,fmt.) call in this program

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
1. btrad<REPTMON><NOWK>.sas7bdat
   (JCL //BNM1 DD DSN=SAP.BT.SASDATA)  SAS ref: BNM1.BTRAD&REPTMON&NOWK
   Cols used: PRODCD, PRODUCT, CUSTCD, ISSDTE, BLDATE, EXPRDATE, BALANCE,
              PAYAMT, FORCURR.
2. btmast<REPTMON><NOWK>.sas7bdat
   (JCL //BNM1 DD DSN=SAP.BT.SASDATA)  SAS ref: BNM1.BTMAST&REPTMON&NOWK
   Cols used: SUBACCT, CUSTCD, DCURBAL, EXPRDATE, DUNDRAWN.
   (ACCTNO is only used by the SAS PROC SORT ... BY ACCTNO, which has no
    effect on the report and is not reproduced.)

============================================================================
OUTPUT
============================================================================
//SASLIST DD DSN=SAP.PBB.NLFBT.TEXT, DISP=(NEW,CATLG,DELETE),
          DCB=(RECFM=FB,LRECL=80,BLKSIZE=6320)
RECFM=FB (NOT FBA) -> per project convention no ASA carriage-control byte;
plain fixed-width 80-char text with form-feed page breaks. Two PROC PRINT
listings are written to the same file: (1) the trade-bill maturity profile
(default title), (2) the undrawn portion (TITLE1). OPTIONS NOCENTER leaves
DATE and NUMBER at their defaults, so each page carries the run date/time
and a page number (continuous across both listings). PAGESIZE is not
specified -> default 60 lines/page. The JCL IEFBR14 delete step is handled
by overwriting the output file.
"""

import gc
from pathlib import Path
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values

# from PBBLNFMT import ...   -- NOT USED, no PUT(...,fmt.) call in this program body
# from PBBELF import ...     -- NOT USED, no PUT(...,fmt.) call in this program body

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# Physical inputs (each with its own directory)
INPUT_BTRAD_DIR  = STG_DIR / "BTRD"      # //BNM1 DD -> BTRAD&REPTMON&NOWK
INPUT_BTMAST_DIR = STG_DIR / "BTRD"      # //BNM1 DD -> BTMAST&REPTMON&NOWK

# Parquet caches (each with its own directory)
CACHE_BTRAD_DIR  = BASE_DIR / "input" / "cache" / "BTRD"
CACHE_BTMAST_DIR = BASE_DIR / "input" / "cache" / "BTRD"
CACHE_BTRAD_DIR.mkdir(parents=True, exist_ok=True)
CACHE_BTMAST_DIR.mkdir(parents=True, exist_ok=True)

# Output
OUTPUT_DIR = BASE_DIR / "output" / "BTRD"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000
PAGE_SIZE  = 60       # lines per page (not specified in SAS -> default)
LINE_SIZE  = 80       # matches LRECL=80 on the //SASLIST DD
COL_SEP    = "  "     # PROC PRINT column spacing
DEFAULT_TITLE = "SAS" # default title printed when no TITLE statement is active
FF = "\f"

# Safety cap for the %NXTBLDT billing-date advance loops: a missing ISSDTE
# would make the original SAS macro loop forever; the cap only guards the
# Python process on such bad data and has no effect for valid rows.
_MAX_NXTBLDT_ITER = 2000

_SAS_EPOCH = date(1960, 1, 1)

# ============================================================================
# STEP 1: REPORT DATE
# (DATA REPTDATE -- SELECT(DAY(REPTDATE)) -> NOWK; REPTYEAR/REPTMON/REPTDAY/
#  RDATE macro variables.)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate

_day = reptdate.day
NOWK = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"

REPTYEAR = reptdate.strftime("%Y")          # PUT(REPTDATE,YEAR4.)
REPTMON  = reptdate.strftime("%m")          # PUT(MONTH(REPTDATE),Z2.)
REPTDAY  = reptdate.strftime("%d")          # PUT(DAY(REPTDATE),Z2.)
RDATE    = reptdate.strftime("%d/%m/%y")    # PUT(REPTDATE,DDMMYY8.)

# %DCLVAR RD1-RD12 (RPDAYS): days-per-month of the report year (RD2=29 when
# MOD(RPYR,4)=0, set at _N_=1). MD1-MD12 (MDDAYS) is declared and assigned
# inside %REMMTH but never referenced by any live logic -> dead, omitted.
RD_DAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
if reptdate.year % 4 == 0:
    RD_DAYS[1] = 29

# Run timestamp for the DATE / NUMBER page header (SAS defaults)
_run_dt = datetime.now()
RUN_STAMP = (
    f"{_run_dt:%H:%M} {_run_dt:%A}, {_run_dt:%B} {_run_dt.day}, {_run_dt.year}"
)

# Generate time stamp
report_date = date.today() - timedelta(days=1)
ts = report_date.strftime("%y%m%d")

OUTPUT_FILE = OUTPUT_DIR / f"EIWBTLIQ_{ts}.txt"

# ============================================================================
# INPUT FILE NAMES  (deterministic, built directly from date tokens)
# ============================================================================
INPUT_BTRAD_FILE  = INPUT_BTRAD_DIR  / f"btrad{REPTMON}{NOWK}.sas7bdat"
INPUT_BTMAST_FILE = INPUT_BTMAST_DIR / f"btmast{REPTMON}{NOWK}.sas7bdat"

print(f"  RDATE        : {RDATE}")
print(f"  REPTMON/NOWK : {REPTMON}/{NOWK}")
print(f"  Input BTRAD  : {INPUT_BTRAD_FILE}")
print(f"  Input BTMAST : {INPUT_BTMAST_FILE}")
print(f"  Output file  : {OUTPUT_FILE}")


# ============================================================================
# PROC FORMAT EQUIVALENTS (local to this program)
# ============================================================================
def remfmt_format(value) -> str:
    """PROC FORMAT VALUE REMFMT. -- overlapping boundaries resolve to the
    earlier-listed range; a SAS missing numeric sorts as LOW -> '01'."""
    if value is None or value <= 0.1:
        return "01"      # UP TO 1 WK
    if value <= 1:
        return "02"      # >1 WK - 1 MTH
    if value <= 3:
        return "03"      # >1 MTH - 3 MTHS
    if value <= 6:
        return "04"      # >3 - 6 MTHS
    if value <= 12:
        return "05"      # >6 MTHS - 1 YR
    return "06"          # > 1 YEAR


_PRDFMT_HL = {
    4, 5, 6, 7, 31, 32, 100, 101, 102, 103, 110, 111, 112, 113, 114, 115,
    116, 170, 200, 201, 204, 205, 209, 210,
    211, 212, 214, 215, 219, 220, 225, 226,
    227, 228, 229, 230, 231, 232, 233, 234,
}
_PRDFMT_RC = {350, 910, 925}


def prdfmt_format(value) -> str:
    """PROC FORMAT VALUE PRDFMT. -- declared in the SAS source but never
    applied (PROD is hardcoded to 'BT'); kept for fidelity only."""
    if value in _PRDFMT_HL:
        return "HL"
    if value in _PRDFMT_RC:
        return "RC"
    return "FL"


# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET
# (identical pattern to EIIMRM01.py / EIMBTLIQ.py)
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


def _load_cached(sas_path: Path, cache_dir: Path, tag: str) -> Path:
    cache_path = cache_dir / f"{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# STEP 2: CACHE INPUT SAS FILES TO PARQUET (each input independently)
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
BTRAD_CACHE  = _load_cached(INPUT_BTRAD_FILE,  CACHE_BTRAD_DIR,  "BTRAD")
BTMAST_CACHE = _load_cached(INPUT_BTMAST_FILE, CACHE_BTMAST_DIR, "BTMAST")

# ============================================================================
# STEP 3: LOAD BTRAD
# ============================================================================
print("\nStep 3: Loading BTRAD...")

con = duckdb.connect(database=":memory:")
btrad_raw = con.execute(f"""
    SELECT
        CAST(t.PRODCD   AS VARCHAR) AS PRODCD,
        CAST(t.PRODUCT  AS INTEGER) AS PRODUCT,
        CAST(t.CUSTCD   AS VARCHAR) AS CUSTCD,
        CAST(t.ISSDTE   AS BIGINT)  AS ISSDTE,
        CAST(t.BLDATE   AS BIGINT)  AS BLDATE,
        CAST(t.EXPRDATE AS BIGINT)  AS EXPRDATE,
        CAST(t.BALANCE  AS DOUBLE)  AS BALANCE,
        CAST(t.PAYAMT   AS DOUBLE)  AS PAYAMT,
        CAST(t.FORCURR  AS VARCHAR) AS FORCURR
    FROM read_parquet('{BTRAD_CACHE.as_posix()}') AS t
""").pl()
print(f"  BTRAD rows: {len(btrad_raw):,}")

# ============================================================================
# STEP 4: DATA NOTE (KEEP=BNMCODE AMOUNT AMTUSD AMTSGD)
# ============================================================================
print("\nStep 4: Building NOTE (BNMCODE classification + maturity amortisation)...")


def _sasdate(n):
    """SAS date number (days since 1960-01-01) -> date; missing -> None."""
    if n is None or n != n:
        return None
    return _SAS_EPOCH + timedelta(days=int(n))


def _nxtbldt(bldate, issdte, payfreq: str, freq: int, lday: list):
    """%MACRO NXTBLDT; -- PAYFREQ is hardcoded to '3' at every call site in
    DATA NOTE (FREQ=6 months only), so the PAYFREQ='6' branch is
    structurally unreachable; preserved verbatim as dead code. lday
    (D1-D12 / LDAY array) is mutated in place and RETAINed across every row
    and every call, exactly as the SAS RETAIN semantics require. A missing
    BLDATE/ISSDTE yields a missing result (None), as in SAS."""
    if bldate is None:
        return None
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
        if issdte is None:
            return None
        dd = issdte.day
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
    """%MACRO REMMTH; -- MD2 (MDDAYS leap-year override) is assigned in the
    original macro but never referenced elsewhere (dead), so omitted."""
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    days_in_rpmth = rd_days[rpmth - 1]
    if mdday > days_in_rpmth:
        mdday = days_in_rpmth
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / days_in_rpmth


def _needs_clamp(bldate, exprdate, balance, payamt) -> bool:
    """IF BLDATE > EXPRDATE | BALANCE <= PAYAMT THEN BLDATE = EXPRDATE;
    A missing BALANCE sorts as LOW in SAS, so 'BALANCE <= PAYAMT' is TRUE
    whenever BALANCE is missing."""
    return (
        (bldate is not None and bldate > exprdate)
        or balance is None
        or balance <= payamt
    )


def _build_note_rows(rows_iter, reptdate_dt: date, rd_days: list) -> list:
    """DATA NOTE (KEEP=BNMCODE AMOUNT AMTUSD AMTSGD); transcription of the
    SAS logic: trade-bill subsetting IF, CUST/ITEM classification (PROD is
    the literal 'BT'), DAYS past due, the REMMTH bucket tests against
    REPTDATE, the periodic-installment DO WHILE loop and the closing
    balance outputs. AMTUSD/AMTSGD are only assigned in the closing block
    (346 products), so loop outputs carry them as missing."""
    rpyr, rpmth, rpday = reptdate_dt.year, reptdate_dt.month, reptdate_dt.day
    lday = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]   # D1-D12 (RETAIN)

    out = []

    def _emit(prefix, item, cust, remmth, amount, amtusd, amtsgd):
        out.append([
            prefix + item + cust + remfmt_format(remmth) + '0000Y',
            amount, amtusd, amtsgd,
        ])

    for r in rows_iter:
        prodcd   = r["PRODCD"]
        product  = r["PRODUCT"]
        custcd   = r["CUSTCD"]
        issdte   = _sasdate(r["ISSDTE"])
        exprdate = _sasdate(r["EXPRDATE"])
        balance  = r["BALANCE"]
        payamt   = r["PAYAMT"]
        forcurr  = r["FORCURR"]
        bl_raw   = r["BLDATE"]

        # IF SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226);
        code2 = "" if prodcd is None else str(prodcd)[:2]
        if not (code2 == "34" or product in (225, 226)):
            continue
        is_346 = ("" if prodcd is None else str(prodcd)[:3]) == "346"

        cust_cd = "" if custcd is None else str(custcd).strip()
        in_cust = cust_cd in ("77", "78", "95", "96")
        cust = "08" if in_cust else "09"

        # PROD = 'BT'; -- hardcoded literal, so the SELECT(PROD) below can
        # only reach its OTHERWISE branch; the 'HL'/'FL'/'RC' WHEN clauses
        # are structurally unreachable and preserved verbatim as dead code.
        prod = 'BT'
        if in_cust:
            if prod == 'HL':
                item = '214'
            else:
                item = '219'
        else:
            if prod == 'FL':
                item = '211'
            elif prod == 'RC':
                item = '212'
            else:
                item = '219'

        # BLDATE is an input column; IF BLDATE > 0 THEN DAYS = ...
        # (value <= 0 or missing means "no billing date" -> None)
        bldate = _sasdate(bl_raw) if (bl_raw is not None and bl_raw == bl_raw and bl_raw > 0) else None
        days = (reptdate_dt - bldate).days if bldate is not None else None

        remmth = None

        # IF EXPRDATE - REPTDATE < 8 THEN REMMTH = 0.1;  (missing EXPRDATE
        # makes the difference missing, which is < 8 in SAS)
        if exprdate is None or (exprdate - reptdate_dt).days < 8:
            remmth = 0.1
        else:
            payfreq = '3'
            freq = 6   # SELECT(PAYFREQ): WHEN('3') FREQ=6; -- only reachable branch

            if product in (350, 910, 925):
                bldate = exprdate
            elif bldate is None:           # ELSE IF BLDATE <= 0
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
            while bldate is not None and bldate <= exprdate and _iter < _MAX_NXTBLDT_ITER:
                remmth = _remmth(bldate, rpyr, rpmth, rpday, rd_days)
                if remmth > 12 or bldate == exprdate:
                    break

                if is_346:
                    amount = payamt
                    balance = balance - payamt
                    _emit('96', item, cust, remmth, amount, None, None)
                    if days is not None and days > 89:
                        remmth = 13.0
                    _emit('94', item, cust, remmth, amount, None, None)
                else:
                    amount = payamt
                    balance = balance - payamt
                    _emit('95', item, cust, remmth, amount, None, None)
                    if days is not None and days > 89:
                        remmth = 13.0
                    _emit('93', item, cust, remmth, amount, None, None)

                bldate = _nxtbldt(bldate, issdte, payfreq, freq, lday)
                if _needs_clamp(bldate, exprdate, balance, payamt):
                    bldate = exprdate
                _iter += 1

        # Closing balance outputs
        if is_346:
            amount = balance
            amtusd = balance if (forcurr is not None and str(forcurr).strip() == 'USD') else None
            amtsgd = balance if (forcurr is not None and str(forcurr).strip() == 'SGD') else None
            _emit('96', item, cust, remmth, amount, amtusd, amtsgd)
            if days is not None and days > 89:
                remmth = 13.0
            _emit('94', item, cust, remmth, amount, amtusd, amtsgd)
        else:
            amount = balance
            _emit('95', item, cust, remmth, amount, None, None)
            if days is not None and days > 89:
                remmth = 13.0
            _emit('93', item, cust, remmth, amount, None, None)

    return out


note_rows = _build_note_rows(btrad_raw.iter_rows(named=True), reptdate, RD_DAYS)
print(f"  NOTE rows (pre-summary): {len(note_rows):,}")

del btrad_raw
gc.collect()

# ============================================================================
# STEP 5: PROC SUMMARY DATA=NOTE NWAY; CLASS BNMCODE; VAR AMOUNT AMTUSD AMTSGD;
#         OUTPUT OUT=NOTE(DROP=_TYPE_ _FREQ_) SUM=;
# ============================================================================
print("\nStep 5: Summarising NOTE (SUM by BNMCODE)...")


def _group_sum(rows, n_vars):
    """PROC SUMMARY NWAY on BNMCODE (col 0); SUM ignores missing values and
    a group stays missing only if every contributing value is missing.
    Output is ordered by the class variable, as PROC SUMMARY does."""
    groups = {}
    for r in rows:
        key = r[0]
        acc = groups.get(key)
        if acc is None:
            acc = [None] * n_vars
            groups[key] = acc
        for i in range(n_vars):
            v = r[i + 1]
            if v is not None:
                acc[i] = (acc[i] or 0.0) + v
    return [[k] + groups[k] for k in sorted(groups)]


note_summary = _group_sum(note_rows, 3)
del note_rows
gc.collect()

# DATA NOTE; SET NOTE; IF AMTUSD = . THEN AMTUSD = 0; IF AMTSGD = . THEN AMTSGD = 0;
note_print = [
    {
        "BNMCODE": r[0],
        "AMOUNT":  r[1],
        "AMTUSD":  0.0 if r[2] is None else r[2],
        "AMTSGD":  0.0 if r[3] is None else r[3],
    }
    for r in note_summary
]
print(f"  NOTE summary rows: {len(note_print):,}")

# ============================================================================
# STEP 6: UNDRAWN PORTION
# (PROC SORT DATA=BNM1.BTMAST&REPTMON&NOWK OUT=ALM; BY ACCTNO; is NOT
#  reproduced: DATA UNOTE processes every row independently -- the BY
#  statement is never used via FIRST./LAST. and the following PROC SUMMARY
#  is order-independent -- so the sort has no observable effect.)
# ============================================================================
print("\nStep 6: Building UNOTE (undrawn portion)...")

btmast_raw = con.execute(f"""
    SELECT
        CAST(t.EXPRDATE AS BIGINT) AS EXPRDATE,
        CAST(t.DUNDRAWN AS DOUBLE) AS DUNDRAWN
    FROM read_parquet('{BTMAST_CACHE.as_posix()}') AS t
    WHERE TRIM(CAST(t.SUBACCT AS VARCHAR)) = 'OV'
      AND t.CUSTCD IS NOT NULL
      AND TRIM(CAST(t.CUSTCD AS VARCHAR)) <> ''
      AND t.DCURBAL IS NOT NULL
      AND NOT isnan(CAST(t.DCURBAL AS DOUBLE))
""").pl()
con.close()
print(f"  BTMAST (OV) rows: {len(btmast_raw):,}")


def _build_unote_rows(rows_iter, reptdate_dt: date, rd_days: list) -> list:
    """DATA UNOTE (KEEP=BNMCODE AMOUNT); RENAME DUNDRAWN=AMOUNT;"""
    rpyr, rpmth, rpday = reptdate_dt.year, reptdate_dt.month, reptdate_dt.day
    out = []
    for r in rows_iter:
        # * IF SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226);
        matdt = _sasdate(r["EXPRDATE"])
        item = '429'
        if matdt is None or (matdt - reptdate_dt).days < 8:
            remmth = 0.1
        else:
            remmth = _remmth(matdt, rpyr, rpmth, rpday, rd_days)
        bnmcode = '95' + item + '00' + remfmt_format(remmth) + '0000Y'
        out.append([bnmcode, r["DUNDRAWN"]])
    return out


unote_rows = _build_unote_rows(btmast_raw.iter_rows(named=True), reptdate, RD_DAYS)
del btmast_raw
gc.collect()

# PROC SUMMARY DATA=UNOTE NWAY; CLASS BNMCODE; VAR AMOUNT; OUTPUT OUT=NOTE SUM=;
unote_summary = _group_sum(unote_rows, 1)
del unote_rows
gc.collect()
unote_print = [{"BNMCODE": r[0], "AMOUNT": r[1]} for r in unote_summary]
print(f"  UNOTE summary rows: {len(unote_print):,}")

# ============================================================================
# STEP 7: REPORT RENDERING  (PROC PRINT emulation)
# ============================================================================


def _pad80(s: str) -> str:
    """LRECL=80 on //SASLIST: every physical line is exactly 80 chars."""
    return s[:LINE_SIZE].ljust(LINE_SIZE)


def _best12(value, width: int = 12) -> str:
    """Default PROC PRINT numeric format (BEST12.): shortest representation
    that fits the width; missing -> '.'."""
    if value is None:
        return "."
    v = float(value)
    if v == 0:
        return "0"
    if v == int(v) and len(str(int(v))) <= width:
        return str(int(v))
    neg = v < 0
    a = abs(v)
    int_digits = len(str(int(a))) if a >= 1 else 1
    dec = width - (1 if neg else 0) - int_digits - 1
    if dec >= 1:
        s = f"{v:.{dec}f}"
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        if len(s) <= width:
            return s
    else:
        s = f"{v:.0f}"
        if len(s) <= width:
            return s
    for k in range(width - 5, -1, -1):
        s = f"{v:.{k}E}"
        mant, exp = s.split("E")
        if "." in mant:
            mant = mant.rstrip("0").rstrip(".")
        s = f"{mant}E{int(exp)}"
        if len(s) <= width:
            return s
    return s


def _page_head(title: str, page_no: int) -> list:
    """Title line (left-aligned under NOCENTER) with the DATE/NUMBER stamp
    right-aligned on the same line, followed by one blank line."""
    stamp = f"{RUN_STAMP}{str(page_no).rjust(4)}"
    gap = max(1, LINE_SIZE - len(title) - len(stamp))
    return [title + " " * gap + stamp, ""]


def _render_proc_print(rows: list, columns: list, sum_cols: list,
                       title: str, page_no: int, out: list) -> int:
    """Emulates PROC PRINT (no ID/VAR -> OBS + every variable) with a SUM
    statement. Column width = max(header, widest formatted value); numeric
    headers/values right-aligned, character left-aligned. A new page repeats
    the title/header every PAGE_SIZE lines. Returns the last page number."""
    if not rows:
        return page_no

    nobs = len(rows)
    cells = [
        {n: (_best12(r[n]) if k == "N" else str(r[n])) for n, k in columns}
        for r in rows
    ]
    totals = {}
    for n in sum_cols:
        vals = [r[n] for r in rows if r[n] is not None]
        totals[n] = sum(vals) if vals else None

    widths = {}
    for n, k in columns:
        w = len(n)
        for c in cells:
            w = max(w, len(c[n]))
        if n in totals:
            w = max(w, len(_best12(totals[n])))
        widths[n] = w
    obs_w = max(len("OBS"), len(str(nobs)))

    def _cell(text, kind, w):
        return text.ljust(w) if kind == "C" else text.rjust(w)

    header = "OBS".rjust(obs_w) + COL_SEP + COL_SEP.join(
        _cell(n, k, widths[n]) for n, k in columns
    )
    eq_line = " " * obs_w + COL_SEP + COL_SEP.join(
        ("=" * widths[n]) if n in totals else " " * widths[n] for n, k in columns
    )
    tot_line = " " * obs_w + COL_SEP + COL_SEP.join(
        _cell(_best12(totals[n]), k, widths[n]) if n in totals else " " * widths[n]
        for n, k in columns
    )

    def _start_page() -> int:
        nonlocal page_no
        page_no += 1
        if out:
            out.append(FF)           # form feed before every page but the first
        out.extend(_page_head(title, page_no))
        out.append(header)
        out.append("")
        return 4                     # title, blank, header, blank

    used = _start_page()
    for i, c in enumerate(cells, start=1):
        if used >= PAGE_SIZE:
            used = _start_page()
        out.append(
            str(i).rjust(obs_w) + COL_SEP + COL_SEP.join(
                _cell(c[n], k, widths[n]) for n, k in columns
            )
        )
        used += 1

    if sum_cols:
        if used + 2 > PAGE_SIZE:
            used = _start_page()
        out.append(eq_line)
        out.append(tot_line)
    return page_no


print("\nStep 7: Rendering report...")

report_lines: list = []
_page = 0

# PROC PRINT; SUM AMOUNT AMTUSD AMTSGD;   (no TITLE statement active yet)
_page = _render_proc_print(
    note_print,
    [("BNMCODE", "C"), ("AMOUNT", "N"), ("AMTUSD", "N"), ("AMTSGD", "N")],
    ["AMOUNT", "AMTUSD", "AMTSGD"],
    DEFAULT_TITLE,
    _page,
    report_lines,
)

# TITLE1 'UNDRAWN PORTION FOR TRADE BILLS AS AT '&RDATE;
# PROC PRINT; SUM AMOUNT;
_page = _render_proc_print(
    unote_print,
    [("BNMCODE", "C"), ("AMOUNT", "N")],
    ["AMOUNT"],
    f"UNDRAWN PORTION FOR TRADE BILLS AS AT {RDATE}",
    _page,
    report_lines,
)

# ============================================================================
# STEP 8: WRITE OUTPUT  (JCL IEFBR14 delete + DISP=(NEW,CATLG,DELETE) ->
#         any previous file is replaced)
# ============================================================================
if OUTPUT_FILE.exists():
    OUTPUT_FILE.unlink()

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(_pad80(ln) + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")

print("\nEIWBTLIQ complete.")
