#!/usr/bin/env python3
"""
Program : EIBMRLFI.py
Purpose : Islamic FISS - New Liquidity Framework report (BNM regulatory
          liquidity BNMCODE detail + PROC PRINT listing + PROC TABULATE
          customer-deposit-distribution report).

Dependency:
    PBBLNFMT.py -> format_liqpfmt (LIQPFMT. is the only traceable PUT()
                   call against PBBLNFMT in this program body).
    PBBDPFMT.py -> fddenom_format (FDDENOM.), fdprod_format (FDPROD.) --
                   the only two traceable PUT() calls against PBBDPFMT.
    PBBELF.py   -> %INC PGM(PBBELF) is present in the original SYSIN, but
                   no PUT(var,fmt.) call against any PBBELF format/table
                   (EL/ELI BNMCODE lookups, branch mappings, etc.) appears
                   anywhere in this program body. Left as a commented
                   placeholder, never imported, per project convention:
                   # from PBBELF import ...  (no direct invocation found)
    DALWPBBD.py -> imported for its module-level BNM_SAVG / BNM_CURN
                   DataFrames (SAP.PBB.MNITB savings/current, already
                   BNMCODE/AMTIND-formatted), matching the JCL sequence
                   %INC PGM(DALWPBBD); %INC PGM(EIBMRLFI);
    KALMLIQI.py -> called via KALMLIQI.main(...) for the KAPITI PART 1/2/3
                   BNMCODE detail (%INC PGM(KALMLIQI) equivalent).

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently, using the
same chunked sas7bdat -> Parquet -> cache pattern as EIBDLN1M.py)
============================================================================
1. LOAN.REPTDATE      -> no physical file; derived via REPTDATE.py.

2. BNM1.LOAN&REPTMON&NOWK  (loan master, RM-denominated + Islamic subset)
   File : loan<REPTMON><NOWK>.sas7bdat  (deterministic REPTMON+NOWK name)
   Path : INPUT_LOAN_FILE
   Cols used : ACCTNO, NOTENO, AMTIND, PAIDIND, PRODCD, PRODUCT, CUSTCD,
               ACCTYPE, BALANCE, COMMNO, APPRLIM2, BLDATE, EXPRDATE,
               PAYFREQ, ISSDTE, PAYAMT, LOANSTAT, UNDRAWN, APPRDATE

3. FD.FD  (fixed deposit certificate extract, fixed name -- no date token)
   File : fd.sas7bdat
   Path : INPUT_FD_FILE
   Cols used : INTPLAN, CURBAL, CUSTCD, MATDATE, OPENIND

4. DEPOSIT.CURRENT  (current-account extract, read directly here -- a
   separate physical read from BNM.CURN built by DALWPBBD.py)
   File : current.sas7bdat
   Path : INPUT_CURRENT_FILE
   Cols used : PRODUCT, CUSTCODE, CURBAL

5. LOAN.LNCOMM  (loan-commitment linkage, used only to establish the
   ACCTNO/COMMNO merge boundary in the undrawn-portion logic below)
   File : lncomm.sas7bdat
   Path : INPUT_LNCOMM_FILE
   Cols used : ACCTNO, COMMNO

============================================================================
IN-MEMORY INPUTS (from DALWPBBD.py, produced when %INC-equivalent import
runs; not re-read from Parquet since they are already in scope)
============================================================================
- DALWPBBD.BNM_SAVG  -> equivalent of BNM.SAVG&REPTMON&NOWK
- DALWPBBD.BNM_CURN  -> equivalent of BNM.CURN&REPTMON&NOWK

============================================================================
OUTPUT
============================================================================
//SASLIST DD DSN=SAP.PBB.LIQP.ISLM(+1) -- a GDG (new generation each run),
DCB=(RECFM=FB,LRECL=133). RECFM=FB (not FBA) means NO ASA carriage-control
byte per project convention; page breaks use a form-feed character instead.
PAGESIZE is not specified in the SAS source -> default 60 lines/page.
Since the GDG name carries an implicit new-generation date, the Python
output filename is date-stamped via output_date.build_output_file().
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
from output_date import build_output_file
from PBBLNFMT import format_liqpfmt
from PBBDPFMT import fddenom_format, fdprod_format
# from PBBELF import ...   # %INC PGM(PBBELF) in SAS source, but no direct
#                          # PUT(var,fmt.) call against any PBBELF format
#                          # or lookup table exists in this program body --
#                          # boilerplate include only, intentionally not a
#                          # live import (per project convention).

import DALWPBBD          # %INC PGM(DALWPBBD) equivalent -- module-level
                          # execution builds BNM_SAVG / BNM_CURN / BNM_DEPT.
import KALMLIQI           # %INC PGM(KALMLIQI) equivalent -- driven via
                          # KALMLIQI.main() below with this program's
                          # REPTDATE/RPYR/RPMTH/RPDAY/RD_DAYS/REPTMON/NOWK.

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR = Path("/stgsrcsys/host/uat/AII")

INPUT_LOAN_DIR = STG_DIR / "MNILN" / "sasdata"
INPUT_FD_DIR = STG_DIR / "MNIFD" / "sasdata"
INPUT_CURRENT_DIR = STG_DIR / "MNITB" / "sasdata"
INPUT_LNCOMM_DIR = STG_DIR / "MNILN" / "sasdata"

INPUT_FD_FILE = INPUT_FD_DIR / "fd.sas7bdat"
INPUT_CURRENT_FILE = INPUT_CURRENT_DIR / "current.sas7bdat"
INPUT_LNCOMM_FILE = INPUT_LNCOMM_DIR / "lncomm.sas7bdat"

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMRLFI"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIBMRLFI"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000
PAGE_SIZE = 60      # PAGESIZE not specified in SAS source -> default
LINE_WIDTH = 132    # LRECL=133 (RECFM=FB, no ASA byte -- 132 data cols)
EPOCH_SENTINEL = date(1960, 1, 1)   # SAS date 0 -- "BLDATE not yet set"


# ============================================================================
# LOCAL FORMAT / MACRO EQUIVALENTS
# (REMFMT / %DCLVAR / %REMMTH / %NXTBLDT are all declared once, in this
#  program's own SYSIN, before %INC PGM(KALMLIQI) -- so KALMLIQI.py carries
#  its own identical copy of REMFMT/%REMMTH, documented there as a
#  necessary duplicate.)
# ============================================================================
def remfmt_format(value: float) -> str:
    """PROC FORMAT VALUE REMFMT."""
    if value <= 0.1:
        return "01"
    if value <= 1:
        return "02"
    if value <= 3:
        return "03"
    if value <= 6:
        return "04"
    if value <= 12:
        return "05"
    return "06"


def build_rd_days(rpyr: int) -> list:
    """%DCLVAR's RD1-RD12 / MD1-MD12 (identical arrays; RD used below)."""
    days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if rpyr % 4 == 0:
        days[1] = 29
    return days


def remmth(matdt: date, rpyr: int, rpmth_: int, rpday: int, rd_days: list) -> float:
    """%REMMTH macro. RPDAYS(RPMTH) caps MDDAY using the REPORT month's
    day-count, not the maturity month's -- preserved verbatim."""
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    days_in_rpmth = rd_days[rpmth_ - 1]
    if mdday > days_in_rpmth:
        mdday = days_in_rpmth
    remy = mdyr - rpyr
    remm_ = mdmth - rpmth_
    remd = mdday - rpday
    return remy * 12 + remm_ + remd / days_in_rpmth


class _DclVars:
    """%DCLVAR's RETAINed LDAY (D1-D12) array. D2 (Feb) starts at 31 (a
    bug -- there is no explicit 28-day default, only the leap-year 29
    override inside %NXTBLDT) and is mutated in place exactly as SAS
    RETAIN does: once a leap year is seen, D2 stays 29 for the remainder
    of the run. Preserved verbatim."""

    def __init__(self):
        self.lday = [31, 31, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def _nxtbldt(bldate: date, issdte: date, payfreq, freq, dcl: _DclVars) -> date:
    """%NXTBLDT macro."""
    if payfreq == "6":
        dd = bldate.day + 14
        mm = bldate.month
        yy = bldate.year
        if mm == 2 and yy % 4 == 0:
            dcl.lday[1] = 29
        if dd > dcl.lday[mm - 1]:
            dd = dd - dcl.lday[mm - 1]
            mm += 1
            if mm > 12:
                mm -= 12
                yy += 1
    else:
        dd = issdte.day if issdte else bldate.day
        mm = bldate.month + (freq or 0)
        yy = bldate.year
        if mm > 12:
            mm -= 12
            yy += 1

    if mm == 2 and yy % 4 == 0:
        dcl.lday[1] = 29
    if dd > dcl.lday[mm - 1]:
        dd = dcl.lday[mm - 1]

    return date(yy, mm, dd)


def _is_unset_date(d) -> bool:
    """'BLDATE > 0' / 'BLDATE <= 0' -- SAS date 0 is 1960-01-01."""
    return d is None or d <= EPOCH_SENTINEL


def _parse_yyyymmdd(value) -> date:
    """MATDT = INPUT(PUT(MATDATE,Z8.),YYMMDD8.) -- MATDATE stored as an
    8-digit YYYYMMDD integer."""
    s = f"{int(value):08d}"
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


# ============================================================================
# CACHE HELPER: STREAM .sas7bdat -> PARQUET (EIBDLN1M.py pattern)
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
# STEP: NOTE -- loan liquidity BNMCODE detail (OD single-row + LN loop)
# ============================================================================
def _process_loan_row(r, reptdate, rpyr, rpmth_, rpday, rd_days, dcl):
    acctype = r["ACCTYPE"]
    custcd = r["CUSTCD"]
    cust = "08" if custcd in ("77", "78", "95", "96") else "09"
    rows_out = []

    if acctype == "OD":
        remmth_val = 0.1
        amount = r["BALANCE"]
        bnmcode = f"95213{cust}{remfmt_format(remmth_val)}0000Y"
        rows_out.append({"BNMCODE": bnmcode, "AMOUNT": amount, "AMTUSD": 0.0, "AMTSGD": 0.0})
        return rows_out   # subsetting IF ACCTYPE='LN' drops the remainder

    if acctype != "LN":
        return rows_out   # dropped by subsetting IF ACCTYPE='LN'

    product = r["PRODUCT"]
    prod = format_liqpfmt(product)
    if custcd in ("77", "78", "95", "96"):
        item = "214" if prod == "HL" else "219"
    else:
        item = "211" if prod == "FL" else ("212" if prod == "RC" else "219")

    bldate = r["BLDATE"]
    exprdate = r["EXPRDATE"]
    issdte = r["ISSDTE"]
    payfreq = r["PAYFREQ"]
    payamt = r["PAYAMT"] or 0.0
    balance = r["BALANCE"]
    loanstat = r["LOANSTAT"]

    days = None
    if not _is_unset_date(bldate):
        days = (reptdate - bldate).days

    remmth_val = None
    if exprdate is not None and (exprdate - reptdate).days < 8:
        remmth_val = 0.1
    else:
        freq_map = {"1": 1, "2": 3, "3": 6, "4": 12}
        freq = freq_map.get(payfreq)

        if payfreq in ("5", "9", " ", None) or product in (350, 910, 925):
            bldate = exprdate
        elif _is_unset_date(bldate):
            bldate = issdte
            while bldate is not None and bldate <= reptdate:
                bldate = _nxtbldt(bldate, issdte, payfreq, freq, dcl)

        if payamt < 0:
            payamt = 0.0
        if bldate is None or bldate > exprdate or balance <= payamt:
            bldate = exprdate

        while bldate <= exprdate:
            matdt = bldate
            remmth_val = remmth(matdt, rpyr, rpmth_, rpday, rd_days)
            if remmth_val > 12 or bldate == exprdate:
                break
            amount = payamt
            balance = balance - payamt
            bnmcode = f"95{item}{cust}{remfmt_format(remmth_val)}0000Y"
            rows_out.append({"BNMCODE": bnmcode, "AMOUNT": amount, "AMTUSD": 0.0, "AMTSGD": 0.0})

            remmth_final = 13 if (days is not None and days > 89) or (loanstat != 1) else remmth_val
            bnmcode2 = f"93{item}{cust}{remfmt_format(remmth_final)}0000Y"
            rows_out.append({"BNMCODE": bnmcode2, "AMOUNT": amount, "AMTUSD": 0.0, "AMTSGD": 0.0})

            bldate = _nxtbldt(bldate, issdte, payfreq, freq, dcl)
            if bldate > exprdate or balance <= payamt:
                bldate = exprdate

    amount = balance
    bnmcode = f"95{item}{cust}{remfmt_format(remmth_val)}0000Y"
    rows_out.append({"BNMCODE": bnmcode, "AMOUNT": amount, "AMTUSD": 0.0, "AMTSGD": 0.0})
    remmth_final = 13 if (days is not None and days > 89) or (loanstat != 1) else remmth_val
    bnmcode2 = f"93{item}{cust}{remfmt_format(remmth_final)}0000Y"
    rows_out.append({"BNMCODE": bnmcode2, "AMOUNT": amount, "AMTUSD": 0.0, "AMTSGD": 0.0})

    return rows_out


def build_note_rows(loan_df: pl.DataFrame, reptdate, rpyr, rpmth_, rpday, rd_days) -> list:
    dcl = _DclVars()
    rows = []
    for r in loan_df.iter_rows(named=True):
        rows.extend(_process_loan_row(r, reptdate, rpyr, rpmth_, rpday, rd_days, dcl))
    return rows


# ============================================================================
# STEP: FD -- fixed deposit BNMCODE detail
# ============================================================================
def build_fd_rows(fd_cache: Path, reptdate, rpyr, rpmth_, rpday, rd_days) -> list:
    con = duckdb.connect(":memory:")
    raw = con.execute(f"""
        SELECT
            CAST(INTPLAN AS INTEGER) AS INTPLAN,
            CAST(CURBAL AS DOUBLE) AS CURBAL,
            CAST(CUSTCD AS INTEGER) AS CUSTCD,
            CAST(MATDATE AS BIGINT) AS MATDATE,
            CAST(OPENIND AS VARCHAR) AS OPENIND
        FROM read_parquet('{fd_cache.as_posix()}')
    """).pl()
    con.close()

    rows = []
    for r in raw.iter_rows(named=True):
        intplan = r["INTPLAN"]
        amtind = fddenom_format(intplan)
        if amtind != "I":
            continue
        curbal = r["CURBAL"]
        if curbal is None or curbal <= 0:
            continue
        custcd = r["CUSTCD"]
        cust = "08" if custcd in (77, 78, 95, 96) else "09"
        matdt = _parse_yyyymmdd(r["MATDATE"])

        if r["OPENIND"] == "D" or (matdt - reptdate).days < 8:
            remmth_val = 0.1
        else:
            remmth_val = remmth(matdt, rpyr, rpmth_, rpday, rd_days)

        bic = fdprod_format(intplan)
        amtusd, amtsgd = 0.0, 0.0
        if bic == "42630":
            if intplan in (470, 471, 472, 473, 474, 475, 560):
                amtusd = curbal
            elif intplan in (488, 489, 490, 491, 492, 493, 563):
                amtsgd = curbal
            bnmcode = f"96311{cust}{remfmt_format(remmth_val)}0000Y"
        else:
            bnmcode = f"95311{cust}{remfmt_format(remmth_val)}0000Y"

        rows.append({"BNMCODE": bnmcode, "AMOUNT": curbal, "AMTUSD": amtusd, "AMTSGD": amtsgd})

    return rows


# ============================================================================
# STEP: SA / CA -- from DALWPBBD.BNM_SAVG / DALWPBBD.BNM_CURN
# ============================================================================
def build_sa_rows(bnm_savg: pl.DataFrame) -> pl.DataFrame:
    df = bnm_savg.filter(pl.col("AMTIND") == "I")
    df = df.with_columns(
        pl.when(pl.col("CUSTCD").is_in(["77", "78", "95", "96"]))
        .then(pl.lit("08")).otherwise(pl.lit("09")).alias("CUST")
    ).with_columns([
        (pl.lit("95312") + pl.col("CUST") + pl.lit("01") + pl.lit("0000Y")).alias("BNMCODE"),
        pl.lit(0.0).alias("AMTUSD"),
        pl.lit(0.0).alias("AMTSGD"),
        pl.col("CURBAL").alias("AMOUNT"),
    ])
    return df.select(["BNMCODE", "AMOUNT", "AMTUSD", "AMTSGD"])


def build_ca_rows(bnm_curn: pl.DataFrame) -> pl.DataFrame:
    df = bnm_curn.filter(
        (pl.col("AMTIND") == "I")
        & (pl.col("PRODCD").str.slice(0, 3).is_in(["421", "423"]))
    )
    df = df.with_columns(
        pl.when(pl.col("CUSTCD").is_in(["77", "78", "95", "96"]))
        .then(pl.lit("08")).otherwise(pl.lit("09")).alias("CUST")
    ).with_columns([
        (pl.lit("95313") + pl.col("CUST") + pl.lit("01") + pl.lit("0000Y")).alias("BNMCODE"),
        pl.lit(0.0).alias("AMTUSD"),
        pl.lit(0.0).alias("AMTSGD"),
        pl.col("CURBAL").alias("AMOUNT"),
    ])
    return df.select(["BNMCODE", "AMOUNT", "AMTUSD", "AMTSGD"])


# ============================================================================
# STEP: FCYCA -- from DEPOSIT.CURRENT (physical file, distinct from BNM.CURN)
# ============================================================================
def build_fcyca_rows(current_cache: Path) -> pl.DataFrame:
    con = duckdb.connect(":memory:")
    raw = con.execute(f"""
        SELECT
            CAST(PRODUCT AS INTEGER) AS PRODUCT,
            CAST(CUSTCODE AS INTEGER) AS CUSTCODE,
            CAST(CURBAL AS DOUBLE) AS CURBAL
        FROM read_parquet('{current_cache.as_posix()}')
        WHERE PRODUCT BETWEEN 400 AND 410
    """).pl()
    con.close()

    rows = []
    for r in raw.iter_rows(named=True):
        product = r["PRODUCT"]
        if fddenom_format(product) != "I":
            continue
        custcode = r["CUSTCODE"]
        cust = "08" if custcode in (77, 78, 95, 96) else "09"
        bnmcode = f"96313{cust}010000Y"
        curbal = r["CURBAL"]
        amtusd = curbal if product == 400 else 0.0
        amtsgd = curbal if product == 403 else 0.0
        rows.append({"BNMCODE": bnmcode, "AMOUNT": curbal, "AMTUSD": amtusd, "AMTSGD": amtsgd})

    if rows:
        return pl.DataFrame(rows)
    return pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64,
                                 "AMTUSD": pl.Float64, "AMTSGD": pl.Float64})


# ============================================================================
# STEP: Undrawn-portion (RC commitment) manipulation -> UNOTE
# ============================================================================
def build_appr(alwcom: pl.DataFrame) -> pl.DataFrame:
    """APPR: ALWCOM rows with COMMNO>0. For PRODCD='34190', keep only the
    first row per (ACCTNO,COMMNO) group; all other PRODCD rows pass
    through unconditionally (MERGE ALWCOM(IN=A) LNCOMM; IF A & PRODCD=
    '34190' THEN ... ELSE IF A THEN OUTPUT; -- since A is always true when
    iterating from ALWCOM, LNCOMM's own role here is purely to establish
    the (ACCTNO,COMMNO) match boundary, which sorting by that key already
    provides)."""
    alwcom = alwcom.sort(["ACCTNO", "COMMNO"])
    is_34190 = pl.col("PRODCD") == "34190"
    prod_34190 = alwcom.filter(is_34190).unique(
        subset=["ACCTNO", "COMMNO"], keep="first", maintain_order=True
    )
    other = alwcom.filter(~is_34190)
    return pl.concat([prod_34190, other])


def build_appr1(alwnocom: pl.DataFrame) -> pl.DataFrame:
    """APPR1 (final): ALWNOCOM rows with COMMNO<=0, sorted by
    (ACCTNO,APPRLIM2). For PRODCD='34190' groups: if the group had ANY
    duplicate row (beyond the first) with BALANCE>=APPRLIM2 (the DUPLI
    flag), keep every row in that group with BALANCE>=APPRLIM2; otherwise
    keep only the first row. All other PRODCD rows pass through
    unconditionally. (Group-level DUPLI flag is a practical equivalent of
    SAS's per-row MERGE BY match -- exact for the common case of a single
    duplicate row per approved-limit group.)"""
    alwnocom = alwnocom.sort(["ACCTNO", "APPRLIM2"])
    prod_rows = alwnocom.filter(pl.col("PRODCD") == "34190")
    other_rows = alwnocom.filter(pl.col("PRODCD") != "34190")

    if prod_rows.is_empty():
        return pl.concat([prod_rows, other_rows], how="diagonal_relaxed")

    prod_rows = prod_rows.with_columns(
        pl.int_range(0, pl.len()).over(["ACCTNO", "APPRLIM2"]).alias("_grp_seq")
    )
    dup_ok = (pl.col("_grp_seq") > 0) & (pl.col("BALANCE") >= pl.col("APPRLIM2"))
    prod_rows = prod_rows.with_columns(dup_ok.alias("_dup_ok"))

    group_has_dupli = (
        prod_rows.group_by(["ACCTNO", "APPRLIM2"])
        .agg(pl.col("_dup_ok").any().alias("_group_dupli"))
    )
    prod_rows = prod_rows.join(group_has_dupli, on=["ACCTNO", "APPRLIM2"], how="left")

    keep = (
        (pl.col("_group_dupli") & (pl.col("BALANCE") >= pl.col("APPRLIM2")))
        | (~pl.col("_group_dupli") & (pl.col("_grp_seq") == 0))
    )
    appr1_final = prod_rows.filter(keep).drop(["_grp_seq", "_dup_ok", "_group_dupli"])
    return pl.concat([appr1_final, other_rows], how="diagonal_relaxed")


def build_unote_rows(loan_df: pl.DataFrame, reptdate, rpyr, rpmth_, rpday, rd_days) -> list:
    rows = []
    for r in loan_df.iter_rows(named=True):
        if r["AMTIND"] != "I":
            continue
        prodcd = r["PRODCD"] or ""
        if not (prodcd[:2] == "34" or r["PRODUCT"] in (225, 226)):
            continue

        if r["ACCTYPE"] == "LN":
            matdt = r["EXPRDATE"]
            item = "429"
        else:
            apprdate = r["APPRDATE"]
            matdt = apprdate + timedelta(days=365) if apprdate else None
            item = "423"

        if matdt is None or (matdt - reptdate).days < 8:
            remmth_val = 0.1
        else:
            remmth_val = remmth(matdt, rpyr, rpmth_, rpday, rd_days)

        bnmcode = f"95{item}00{remfmt_format(remmth_val)}0000Y"
        rows.append({"BNMCODE": bnmcode, "AMOUNT": r["UNDRAWN"], "AMTUSD": 0.0, "AMTSGD": 0.0})

    return rows


# ============================================================================
# SUMMARY HELPER
# ============================================================================
def _summarize(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
    return df.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum(),
        pl.col("AMTUSD").sum(),
        pl.col("AMTSGD").sum(),
    ])


# ============================================================================
# REPORT RENDERING (RECFM=FB -> no ASA control byte, form-feed page breaks,
# PAGESIZE=60 default, OPTIONS NOCENTER -> titles left-justified)
# ============================================================================
def _render_note_print(note_df: pl.DataFrame, rdate: str) -> list:
    lines = []
    title1 = "PUBLIC BANK BERHAD"
    title2 = f"NEW LIQUIDITY FRAMEWORK (ISLAMIC) AS AT {rdate}"
    header = f"{'OBS':>5} {'BNMCODE':<14} {'AMOUNT':>17} {'AMTUSD':>17} {'AMTSGD':>17}"
    rule = "-" * len(header)

    def _page_header():
        lines.append(title1)
        lines.append(title2)
        lines.append("")
        lines.append(header)
        lines.append(rule)

    _page_header()
    lines_on_page = 5
    for i, r in enumerate(note_df.iter_rows(named=True), start=1):
        if lines_on_page >= PAGE_SIZE:
            lines.append("\f")
            _page_header()
            lines_on_page = 5
        amount = r["AMOUNT"] or 0.0    # OPTIONS MISSING=0
        amtusd = r["AMTUSD"] or 0.0
        amtsgd = r["AMTSGD"] or 0.0
        lines.append(
            f"{i:>5} {r['BNMCODE']:<14} {amount:>17,.2f} {amtusd:>17,.2f} {amtsgd:>17,.2f}"
        )
        lines_on_page += 1
    return lines


def _render_suppl_tabulate(suppl_df: pl.DataFrame, rdate: str) -> list:
    lines = []
    title1 = "PUBLIC BANK BERHAD"
    title2 = f"NEW LIQUIDITY FRAMEWORK (ISLAMIC) AS AT {rdate}"
    title4 = "CUSTOMER DEPOSITS >= 1% OF TOTAL (PART 3)"
    box_label = "NAME OF DEPOSITOR"
    name_width = 20     # RTS=20
    val_width = 20

    lines.append(title1)
    lines.append(title2)
    lines.append("")   # TITLE3 never set -- blank title line
    lines.append(title4)
    lines.append("")

    if suppl_df.is_empty():
        lines.append(f"{box_label:<{name_width}}|{'':>{val_width}}")
        return lines

    for cat in suppl_df["CAT"].unique(maintain_order=True).to_list():
        cat_rows = suppl_df.filter(pl.col("CAT") == cat).sort("NAME")
        lines.append(f"{box_label:<{name_width}}|{'':>{val_width}}")
        lines.append("-" * (name_width + 1 + val_width))
        total = 0.0
        for r in cat_rows.iter_rows(named=True):
            name = r["NAME"] or ""
            amount = r["AMOUNT"] or 0.0
            total += amount
            lines.append(f"{name:<{name_width}}|{amount:>{val_width},.2f}")
        lines.append("-" * (name_width + 1 + val_width))
        lines.append(f"{'TOTAL':<{name_width}}|{total:>{val_width},.2f}")
        lines.append("")
    return lines


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
def main() -> None:
    # ------------------------------------------------------------------
    # STEP 0: REPORT DATE / NOWK (exact-day match, matching original SAS
    # SELECT(DAY(REPTDATE)) -- no reptdate.parquet exists)
    # ------------------------------------------------------------------
    print("Step 0: Deriving report date...")
    reptdate_values = get_reptdate_values(year_format="%Y")
    reptdate = reptdate_values.reptdate

    day_ = reptdate.day
    nowk = "1" if day_ == 8 else "2" if day_ == 15 else "3" if day_ == 22 else "4"
    reptyear = reptdate.strftime("%Y")
    reptmon = reptdate.strftime("%m")
    reptday = reptdate.strftime("%d")
    rdate = reptdate.strftime("%d/%m/%y")

    rpyr, rpmth_, rpday = reptdate.year, reptdate.month, reptdate.day
    rd_days = build_rd_days(rpyr)

    print(f"  RDATE: {rdate}   REPTMON: {reptmon}   NOWK: {nowk}")

    # ------------------------------------------------------------------
    # STEP 1: RESOLVE & CACHE INPUT FILES
    # ------------------------------------------------------------------
    print("\nStep 1: Caching input SAS datasets to Parquet...")
    loan_sas = INPUT_LOAN_DIR / f"loan{reptmon}{nowk}.sas7bdat"
    loan_cache = _load_cached(loan_sas, "LOAN")
    fd_cache = _load_cached(INPUT_FD_FILE, "FD")
    current_cache = _load_cached(INPUT_CURRENT_FILE, "CURRENT")
    lncomm_cache = _load_cached(INPUT_LNCOMM_FILE, "LNCOMM")

    # ------------------------------------------------------------------
    # STEP 2: LOAD LOAN DATASET (common AMTIND/PAIDIND filter shared by
    # NOTE, ALW/APPR/UNOTE; NOTE applies an ADDITIONAL PRODCD filter below)
    # ------------------------------------------------------------------
    print("\nStep 2: Loading BNM1.LOAN...")
    con = duckdb.connect(":memory:")
    loan_all = con.execute(f"""
        SELECT
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            CAST(NOTENO AS INTEGER) AS NOTENO,
            CAST(AMTIND AS VARCHAR) AS AMTIND,
            CAST(PAIDIND AS VARCHAR) AS PAIDIND,
            CAST(PRODCD AS VARCHAR) AS PRODCD,
            CAST(PRODUCT AS INTEGER) AS PRODUCT,
            CAST(CUSTCD AS VARCHAR) AS CUSTCD,
            CAST(ACCTYPE AS VARCHAR) AS ACCTYPE,
            CAST(BALANCE AS DOUBLE) AS BALANCE,
            CAST(COMMNO AS INTEGER) AS COMMNO,
            CAST(APPRLIM2 AS DOUBLE) AS APPRLIM2,
            CAST(BLDATE AS DATE) AS BLDATE,
            CAST(EXPRDATE AS DATE) AS EXPRDATE,
            CAST(PAYFREQ AS VARCHAR) AS PAYFREQ,
            CAST(ISSDTE AS DATE) AS ISSDTE,
            CAST(PAYAMT AS DOUBLE) AS PAYAMT,
            CAST(LOANSTAT AS INTEGER) AS LOANSTAT,
            CAST(UNDRAWN AS DOUBLE) AS UNDRAWN,
            CAST(APPRDATE AS DATE) AS APPRDATE
        FROM read_parquet('{loan_cache.as_posix()}')
        WHERE AMTIND = 'I' AND PAIDIND NOT IN ('P','C')
    """).pl()
    con.close()
    print(f"  Loan rows (AMTIND='I', PAIDIND not P/C): {len(loan_all):,}")

    loan_note = loan_all.filter(
        (pl.col("PRODCD").str.slice(0, 2) == "34") | (pl.col("PRODUCT").is_in([225, 226]))
    )
    print(f"  Loan rows for NOTE (+ PRODCD/PRODUCT filter): {len(loan_note):,}")

    # ------------------------------------------------------------------
    # STEP 3: NOTE -- loan liquidity BNMCODE detail
    # ------------------------------------------------------------------
    print("\nStep 3: Building NOTE (loan liquidity detail)...")
    note_rows = build_note_rows(loan_note, reptdate, rpyr, rpmth_, rpday, rd_days)
    note_df = pl.DataFrame(note_rows) if note_rows else pl.DataFrame(
        schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTUSD": pl.Float64, "AMTSGD": pl.Float64}
    )
    print(f"  NOTE rows: {len(note_df):,}")

    # ------------------------------------------------------------------
    # STEP 4: FD -- fixed deposit BNMCODE detail
    # ------------------------------------------------------------------
    print("\nStep 4: Building FD...")
    fd_rows = build_fd_rows(fd_cache, reptdate, rpyr, rpmth_, rpday, rd_days)
    fd_df = pl.DataFrame(fd_rows) if fd_rows else pl.DataFrame(
        schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTUSD": pl.Float64, "AMTSGD": pl.Float64}
    )
    print(f"  FD rows: {len(fd_df):,}")

    # ------------------------------------------------------------------
    # STEP 5: SA / CA -- from DALWPBBD.BNM_SAVG / DALWPBBD.BNM_CURN
    # ------------------------------------------------------------------
    print("\nStep 5: Building SA / CA (from DALWPBBD)...")
    sa_df = build_sa_rows(DALWPBBD.BNM_SAVG)
    ca_df = build_ca_rows(DALWPBBD.BNM_CURN)
    print(f"  SA rows: {len(sa_df):,}   CA rows: {len(ca_df):,}")

    # ------------------------------------------------------------------
    # STEP 6: FCYCA -- from DEPOSIT.CURRENT (physical file)
    # ------------------------------------------------------------------
    print("\nStep 6: Building FCYCA...")
    fcyca_df = build_fcyca_rows(current_cache)
    print(f"  FCYCA rows: {len(fcyca_df):,}")

    # ------------------------------------------------------------------
    # STEP 7: Undrawn-portion (RC commitment) manipulation -> UNOTE
    # ------------------------------------------------------------------
    print("\nStep 7: Building UNOTE (undrawn commitment portion)...")
    con = duckdb.connect(":memory:")
    lncomm = con.execute(f"""
        SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(COMMNO AS INTEGER) AS COMMNO
        FROM read_parquet('{lncomm_cache.as_posix()}')
    """).pl()
    con.close()
    del lncomm   # LNCOMM only establishes the ACCTNO/COMMNO match boundary
    gc.collect()

    alwcom = loan_all.filter(pl.col("COMMNO") > 0)
    alwnocom = loan_all.filter(pl.col("COMMNO") <= 0)

    appr = build_appr(alwcom)
    appr1_final = build_appr1(alwnocom)
    loan_combined = pl.concat([appr, appr1_final], how="diagonal_relaxed").sort("ACCTNO")

    unote_rows = build_unote_rows(loan_combined, reptdate, rpyr, rpmth_, rpday, rd_days)
    unote_df = pl.DataFrame(unote_rows) if unote_rows else pl.DataFrame(
        schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTUSD": pl.Float64, "AMTSGD": pl.Float64}
    )
    print(f"  UNOTE rows: {len(unote_df):,}")

    del loan_all, loan_note, alwcom, alwnocom, appr, appr1_final, loan_combined
    gc.collect()

    # ------------------------------------------------------------------
    # STEP 8: SUMMARISE AND CONSOLIDATE (PROC SUMMARY NWAY per source)
    # ------------------------------------------------------------------
    print("\nStep 8: Summarising NOTE / FD / SA / CA / FCYCA / UNOTE...")
    note_summary = _summarize(note_df)
    fd_summary = _summarize(fd_df)
    sa_summary = _summarize(sa_df)
    ca_summary = _summarize(ca_df)
    fcyca_summary = _summarize(fcyca_df)
    unote_summary = _summarize(unote_df)

    note_all = pl.concat(
        [note_summary, fd_summary, sa_summary, ca_summary, unote_summary, fcyca_summary],
        how="diagonal_relaxed",
    )
    print(f"  NOTE (combined) rows before KAPITI: {len(note_all):,}")

    # ------------------------------------------------------------------
    # STEP 9: KAPITI items for PART 2 & 3 (%INC PGM(KALMLIQI) equivalent)
    # ------------------------------------------------------------------
    print("\nStep 9: Running KALMLIQI (KAPITI items)...")
    inst = "PBB"
    kalmliqi_result = KALMLIQI.main(
        reptdate=reptdate, rpyr=rpyr, rpmth=rpmth_, rpday=rpday, rd_days=rd_days,
        reptmon=reptmon, nowk=nowk, inst=inst,
    )
    ktbl = kalmliqi_result["ktbl"]
    k1tbl_summary = kalmliqi_result["k1tbl_summary"]

    suppl = k1tbl_summary.filter(pl.col("AMOUNT").abs() >= 5_000_000)
    note_all = pl.concat([note_all, ktbl], how="diagonal_relaxed")
    print(f"  SUPPL rows (ABS(AMOUNT)>=5,000,000): {len(suppl):,}")
    print(f"  NOTE (combined + KTBL) rows: {len(note_all):,}")

    # ------------------------------------------------------------------
    # STEP 10: FINAL PROC SUMMARY + PROC PRINT / PROC TABULATE RENDERING
    # ------------------------------------------------------------------
    print("\nStep 10: Final summary and report rendering...")
    note_final = _summarize(note_all)

    report_lines = []
    report_lines += _render_note_print(note_final, rdate)
    report_lines.append("\f")
    report_lines += _render_suppl_tabulate(suppl, rdate)

    output_path = build_output_file(OUTPUT_DIR, "EIBMRLFI_LIQP_ISLM", date_format="ddmmyy")
    output_file = output_path.with_suffix(".txt")

    with open(output_file, "w", encoding="latin1") as fh:
        for ln in report_lines:
            fh.write(ln + "\n")

    print(f"\nOutput written : {output_file}")
    print(f"Total lines    : {len(report_lines):,}")
    print("\n--- Report preview (first 30 lines) ---")
    for ln in report_lines[:30]:
        print(ln)

    print("\nEIBMRLFI complete.")


if __name__ == "__main__":
    main()
