# !/usr/bin/env python3
"""
Program  : EIFFTXT3.py
Purpose  : ESMR 2009-16 (SEND FILE TO LNS) - 3RD EXTRACTION
           Generates a fixed-width write-off text file for BAD DEBT
           WRITING-OFF EXERCISE for PBB (Domestic) HP accounts.
           RIND = 'D'  (Domestic indicator)
           Sources : NPL.IIS, NPL.SP2, LOAN.LNNOTE, CISNAME.LOAN,
                     LOAN.LIAB, SASLN.LOAN{REPTMON1}{NOWKS}, NPL.LIST
           Outputs : WOFFTEXT (SAP.PBB.FTPLNS.WOFFTXT, LRECL=750)
                     WOFFTEX1 (SAP.PBB.WOFFTXT.DUM,    LRECL=750)
                     NPL1.WOFFTXT (permanent SAS dataset)

OPTION NOCENTER
%INC PGM(PBBLNFMT)
"""

import os
import math
from datetime import date, datetime

import duckdb
import polars as pl

from PBBLNFMT import HP_ALL, format_mthpass

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR    = r"C:/data"
NPL_DIR     = os.path.join(BASE_DIR, "npl")         # NPL   – SAP.PBB.NPL.HP.SASDATA.DAILY
NPL1_DIR    = os.path.join(BASE_DIR, "npl1")        # NPL1  – SAP.PBB.NPL.HP.SASDATA
SASLN_DIR   = os.path.join(BASE_DIR, "sasln")       # SASLN – SAP.PBB.SASDATA
LOAN_DIR    = os.path.join(BASE_DIR, "loan")        # LOAN  – SAP.PBB.MNILN.DAILY
CISNAME_DIR = os.path.join(BASE_DIR, "cisname")     # CISNAME – SAP.PBB.CISBEXT.LN
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")

# //WOFFTEXT DD DSN=SAP.PBB.FTPLNS.WOFFTXT  (final fixed-width output)
# //WOFFTEX1 DD DSN=SAP.PBB.WOFFTXT.DUM     (intermediate dump)
WOFFTEXT_PATH = os.path.join(OUTPUT_DIR, "EIFFTXT3_WOFFTEXT.txt")
WOFFTEX1_PATH = os.path.join(OUTPUT_DIR, "EIFFTXT3_WOFFTEX1.txt")
NPL1_WOFFTXT  = os.path.join(NPL1_DIR,  "WOFFTXT.parquet")
PRINT_PATH    = os.path.join(OUTPUT_DIR, "EIFFTXT3_PRINT.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(NPL1_DIR,   exist_ok=True)

# RIND = 'D'  (Domestic – PBB)
RIND = "D"

# &HPD macro – HP product set
HPD = set(HP_ALL)

con = duckdb.connect()


# ─────────────────────────────────────────────
# DATA REPTDATE (KEEP=REPTDATE)
#   SET LOAN.REPTDATE
#   SELECT(DAY(REPTDATE)):
#     WHEN (8)  WK='1'; WK1='4'
#     WHEN(15)  WK='2'; WK1='1'
#     WHEN(22)  WK='3'; WK1='2'
#     OTHERWISE WK='4'; WK1='3'
#   MM  = MONTH(REPTDATE)
#   MM1 = MM-1;  IF MM1=0 THEN MM1=12
#   CALL SYMPUT('NOWKS',  '4')
#   CALL SYMPUT('REPTMON1', PUT(MM1,Z2.))
#   CALL SYMPUT('RDATE',   PUT(REPTDATE,DDMMYY8.))
# ─────────────────────────────────────────────
_rdate_path = os.path.join(LOAN_DIR, "REPTDATE.parquet")
_rdate_raw  = con.execute(f"SELECT REPTDATE FROM '{_rdate_path}'").pl()
_rdate_val  = _rdate_raw["REPTDATE"][0]
if isinstance(_rdate_val, int):
    _rdate_val = date.fromordinal(_rdate_val)

_day = _rdate_val.day
if   _day == 8:   NOWK, NOWK1 = "1", "4"
elif _day == 15:  NOWK, NOWK1 = "2", "1"
elif _day == 22:  NOWK, NOWK1 = "3", "2"
else:             NOWK, NOWK1 = "4", "3"

NOWKS    = "4"
MM       = _rdate_val.month
MM1      = MM - 1 if MM > 1 else 12
REPTMON  = f"{MM:02d}"
REPTMON1 = f"{MM1:02d}"
RDATE    = _rdate_val.strftime("%d/%m/%y")      # DDMMYY8. → 'DD/MM/YY'


# ─────────────────────────────────────────────
# Date helpers
# SAS stores some dates as packed integers: PUT(x,Z11.) → 11-char
# zero-padded string → SUBSTR(,1,8) → 'MMDDYYYY' → INPUT(,MMDDYY8.)
# PUT(date, MMDDYY10.) → 'MM/DD/YYYY'
# ─────────────────────────────────────────────
def _z11_substr8(val) -> str | None:
    """PUT(val,Z11.) → SUBSTR(1,8) → 'MMDDYYYY' string."""
    if val is None or val == 0:
        return None
    try:
        return f"{int(val):011d}"[:8]
    except (TypeError, ValueError):
        return None

def _parse_mmddyyyy(val) -> date | None:
    """INPUT(SUBSTR(PUT(val,Z11.),1,8), MMDDYY8.) → Python date."""
    s = _z11_substr8(val)
    if s is None:
        return None
    try:
        return datetime.strptime(s, "%m%d%Y").date()
    except ValueError:
        return None

def _fmt_mmddyy10(val) -> str:
    """PUT(date, MMDDYY10.) → 'MM/DD/YYYY'.  OPTIONS MISSING=' ' → 10 spaces."""
    if val is None:
        return " " * 10
    if isinstance(val, (int, float)):
        val = _parse_mmddyyyy(val)
    if isinstance(val, str):
        return f"{val:<10}"[:10]    # already formatted (MATDATE / LASTTRA1)
    if isinstance(val, date):
        return val.strftime("%m/%d/%Y")
    return " " * 10


# ─────────────────────────────────────────────
# PROC SORT DATA=NPL.IIS  OUT=IIS  NODUPKEYS; BY ACCTNO
# PROC SORT DATA=NPL.SP2  OUT=SP   NODUPKEYS; BY ACCTNO
# ─────────────────────────────────────────────
iis_df = con.execute(f"""
    SELECT * FROM '{os.path.join(NPL_DIR,"IIS.parquet")}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) = 1
""").pl()

sp_df = con.execute(f"""
    SELECT * FROM '{os.path.join(NPL_DIR,"SP2.parquet")}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) = 1
""").pl()


# ─────────────────────────────────────────────
# DATA NPL
#   KEEP BRNO BRABBR NAME ACCTNO NOTENO IIS OI TOTIIS BORSTAT
#        SP MARKETVL DAYS BRANCH
#   MERGE SP IIS; BY ACCTNO
#   * IIS    = ROUND(IIS,.01)      ← commented out in SAS
#   * OI     = ROUND(OI,.01)       ← commented out in SAS
#   * SP     = ROUND(SP,.01)       ← commented out in SAS
#   * TOTIIS = ROUND(IIS+OI,.01)   ← commented out in SAS
#   * TOTAL  = ROUND(TOTIIS+SP,.01)← commented out in SAS
#   MARKETVL = ROUND(MARKETVL,.01)
#   BRNO   = SUBSTR(BRANCH,4,4)    ← Python [3:7]  (1-based pos 4, len 4)
#   BRABBR = SUBSTR(BRANCH,1,3)    ← Python [0:3]  (1-based pos 1, len 3)
# ─────────────────────────────────────────────
npl_df = sp_df.join(iis_df, on="ACCTNO", how="full", suffix="_IIS")

_mktv = "MARKETVL" if "MARKETVL" in npl_df.columns else "MARKETVL_IIS"
_br   = "BRANCH"   if "BRANCH"   in npl_df.columns else "BRANCH_IIS"

npl_df = npl_df.with_columns([
    pl.col(_mktv).fill_null(0.0).round(2).alias("MARKETVL"),
    pl.col(_br).str.slice(3, 4).alias("BRNO"),    # SUBSTR(BRANCH,4,4)
    pl.col(_br).str.slice(0, 3).alias("BRABBR"),  # SUBSTR(BRANCH,1,3)
    pl.col(_br).alias("BRANCH"),
])

_npl_keep = ["BRNO", "BRABBR", "NAME", "ACCTNO", "NOTENO",
             "IIS", "OI", "TOTIIS", "BORSTAT", "SP", "MARKETVL", "DAYS", "BRANCH"]
npl_df = npl_df.select([c for c in _npl_keep if c in npl_df.columns])


# ─────────────────────────────────────────────
# PROC SORT DATA=LOAN.LNNOTE OUT=LOAN NODUPKEYS
#   WHERE LOANTYPE IN &HPD; BY ACCTNO NOTENO
# ─────────────────────────────────────────────
lnnote_df = con.execute(f"""
    SELECT * FROM '{os.path.join(LOAN_DIR,"LNNOTE.parquet")}'
    WHERE LOANTYPE IN ({','.join(str(p) for p in HPD)})
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, NOTENO
                               ORDER BY ACCTNO, NOTENO) = 1
    ORDER BY ACCTNO, NOTENO
""").pl()


# ─────────────────────────────────────────────
# DATA LOAN
#   KEEP ACCTNO NOTENO FEEAMT3 FEEAMT4 CURBAL MATDATE LOANTYPE INTAMT
#        POSTNTRN ECSRRSRV MATUREDT INTEARN4 POSTAMT OIFEEAMT OTHERAMT
#        DAYS CUSTCODE LASTTRAN LSTTRNCD LASTTRA1 MTHPDUE BALANCE
#        GUAREND ISSXDTE CRRGRADE NETPROC MARGINFI NOTETERM PAYAMT
#        DOBMNI LOANTYPE ECSRIND COLLYEAR COLLDESC BILPAID
#   MERGE NPL(IN=AA) LOAN; BY ACCTNO NOTENO; IF AA
# ─────────────────────────────────────────────
loan_df = npl_df.join(lnnote_df, on=["ACCTNO", "NOTENO"], how="inner", suffix="_LN")

def _compute(row: dict) -> dict:
    feetotal  = float(row.get("FEETOTAL")  or 0.0)
    nfeeamt5  = float(row.get("NFEEAMT5")  or 0.0)
    feeamt3   = float(row.get("FEEAMT3")   or 0.0)
    feetot2   = float(row.get("FEETOT2")   or 0.0)
    feeamta   = float(row.get("FEEAMTA")   or 0.0)
    feeamt5   = float(row.get("FEEAMT5")   or 0.0)
    ecsrrsrv  = float(row.get("ECSRRSRV")  or 0.0)
    maturedt  = row.get("MATUREDT")
    lasttran  = row.get("LASTTRAN")
    days_v    = float(row.get("DAYS")      or 0.0)
    netproc   = float(row.get("NETPROC")   or 0.0)
    appvalue  = float(row.get("APPVALUE")  or 0.0)
    birthdt   = row.get("BIRTHDT")
    orgbal    = float(row.get("ORGBAL")    or 0.0)
    curbal    = float(row.get("CURBAL")    or 0.0)
    payamt_v  = float(row.get("PAYAMT")    or 0.0)
    score2    = str(row.get("SCORE2")    or "").strip()
    contrtype = str(row.get("CONTRTYPE") or "").strip()

    # POSTAMT  = SUM(FEETOTAL, NFEEAMT5)
    postamt  = feetotal + nfeeamt5
    # OTHERAMT = SUM(FEEAMT3, (-1)*POSTAMT)
    otheramt = feeamt3 - postamt
    # OIFEEAMT = SUM(FEETOT2, (-1)*FEEAMTA, FEEAMT5)
    # * OIFEEAMT = FEETOT2;  ← commented out in SAS
    oifeeamt = feetot2 - feeamta + feeamt5
    # IF ECSRRSRV LE 0 THEN ECSRRSRV = 0
    ecsrrsrv = max(ecsrrsrv, 0.0)

    # IF MATUREDT NE . THEN DO
    #   MATDATE  = PUT(INPUT(SUBSTR(PUT(MATUREDT,Z11.),1,8),MMDDYY8.),MMDDYY10.)
    #   MATUREDT = INPUT(SUBSTR(PUT(MATUREDT,Z11.),1,8),MMDDYY8.)
    matdate_str  = ""
    maturedt_out = None
    if maturedt and maturedt not in (None, 0):
        maturedt_out = _parse_mmddyyyy(maturedt)
        matdate_str  = _fmt_mmddyy10(maturedt_out)

    # IF LASTTRAN NE . THEN
    #   LASTTRA1 = PUT(INPUT(SUBSTR(PUT(LASTTRAN,Z11.),1,8),MMDDYY8.),MMDDYY10.)
    lasttra1 = ""
    if lasttran and lasttran not in (None, 0):
        lasttra1 = _fmt_mmddyy10(_parse_mmddyyyy(lasttran))

    # MTHPDUE = PUT(DAYS, MTHPASS.)
    # IF MTHPDUE=24 THEN MTHPDUE=INT((DAYS/365)*12)
    mthpdue_s = format_mthpass(int(days_v))
    mthpdue   = int(mthpdue_s)
    if mthpdue == 24:
        mthpdue = int((days_v / 365.0) * 12)

    # FORMAT CRRGRADE $5.
    # CRRGRADE = TRIM(SCORE2) || TRIM(CONTRTYPE)
    crrgrade = (score2 + contrtype)[:5]

    # MARGINFI = ROUND(NETPROC/APPVALUE, .01)
    marginfi = round(netproc / appvalue, 2) if appvalue != 0.0 else 0.0

    # IF BIRTHDT NOT IN (.,0) THEN
    #   DOBMNI = INPUT(SUBSTR(PUT(BIRTHDT,Z11.),1,8), MMDDYY8.)
    dobmni = None
    if birthdt and birthdt not in (None, 0):
        dobmni = _parse_mmddyyyy(birthdt)

    # IF ECSRRSRV > 0 THEN ECSRIND = 'Y'; ELSE ECSRIND = 'N';
    ecsrind = "Y" if ecsrrsrv > 0.0 else "N"

    # BILPAID = INT(SUM(ORGBAL,-1*CURBAL)/PAYAMT)
    bilpaid = int((orgbal - curbal) / payamt_v) if payamt_v != 0.0 else 0

    return {
        "POSTAMT":   postamt,
        "OTHERAMT":  otheramt,
        "OIFEEAMT":  oifeeamt,
        "ECSRRSRV":  ecsrrsrv,
        "MATDATE":   matdate_str,
        "MATUREDT":  maturedt_out,
        "LASTTRA1":  lasttra1,
        "MTHPDUE":   mthpdue,
        "CRRGRADE":  crrgrade,
        "MARGINFI":  marginfi,
        "DOBMNI":    dobmni,
        "ECSRIND":   ecsrind,
        "BILPAID":   bilpaid,
    }

_computed = [_compute(r) for r in loan_df.iter_rows(named=True)]

for _col in ["POSTAMT", "OTHERAMT", "OIFEEAMT", "ECSRRSRV",
             "MATDATE", "LASTTRA1", "MTHPDUE", "CRRGRADE",
             "MARGINFI", "ECSRIND", "BILPAID"]:
    loan_df = loan_df.with_columns(
        pl.Series(_col, [c[_col] for c in _computed])
    )
loan_df = loan_df.with_columns(
    pl.Series("DOBMNI", [c["DOBMNI"] for c in _computed], dtype=pl.Date)
)

# LOAN KEEP list (EIFFTXT3)
_loan_keep = [
    "ACCTNO", "NOTENO", "FEEAMT3", "FEEAMT4", "CURBAL", "MATDATE",
    "LOANTYPE", "INTAMT", "POSTNTRN", "ECSRRSRV", "INTEARN4",
    "POSTAMT", "OIFEEAMT", "OTHERAMT", "DAYS", "CUSTCODE",
    "LASTTRAN", "LSTTRNCD", "LASTTRA1", "MTHPDUE", "BALANCE",
    "GUAREND", "ISSXDTE", "CRRGRADE", "NETPROC", "MARGINFI",
    "NOTETERM", "PAYAMT", "DOBMNI", "ECSRIND", "COLLYEAR",
    "COLLDESC", "BILPAID",
]
loan_df = loan_df.select([c for c in _loan_keep if c in loan_df.columns])


# ─────────────────────────────────────────────
# PROC SORT DATA=CISNAME.LOAN OUT=CNAME(KEEP=ACCTNO CUSTNAM1) NODUPKEY
#   WHERE SECCUST EQ '901'; BY ACCTNO
# ─────────────────────────────────────────────
cname_df = con.execute(f"""
    SELECT ACCTNO, CUSTNAM1
    FROM '{os.path.join(CISNAME_DIR,"LOAN.parquet")}'
    WHERE SECCUST = '901'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) = 1
    ORDER BY ACCTNO
""").pl()


# ─────────────────────────────────────────────
# PROC SORT DATA=LOAN.LIAB OUT=LIAB; BY LIABACCT
# DATA LIAB
#   MERGE LIAB(IN=A) CNAME(RENAME=(ACCTNO=LIABACCT CUSTNAM1=GNAME))
#   BY LIABACCT; IF A
#   IF GNAME='' THEN GNAME=LIABNAME
# PROC SORT; BY ACCTNO NOTENO
# PROC TRANSPOSE DATA=LIAB OUT=LIAB(DROP=_NAME_) PREFIX=GUARNAM
#   BY ACCTNO NOTENO; VAR GNAME
# ─────────────────────────────────────────────
liab_df = con.execute(f"""
    SELECT * FROM '{os.path.join(LOAN_DIR,"LIAB.parquet")}' ORDER BY LIABACCT
""").pl()

liab_df = liab_df.join(
    cname_df.rename({"ACCTNO": "LIABACCT", "CUSTNAM1": "GNAME"}),
    on="LIABACCT",
    how="left",
)

_liabname = "LIABNAME" if "LIABNAME" in liab_df.columns else None
_gnames   = []
for _row in liab_df.iter_rows(named=True):
    _g = str(_row.get("GNAME") or "").strip()
    if _g == "" and _liabname:
        _g = str(_row.get("LIABNAME") or "").strip()
    _gnames.append(_g)
liab_df = liab_df.with_columns(pl.Series("GNAME", _gnames))

liab_df = liab_df.sort(["ACCTNO", "NOTENO"])

# PROC TRANSPOSE → GUARNAM1 / GUARNAM2
guarnam_df = (
    liab_df
    .group_by(["ACCTNO", "NOTENO"], maintain_order=True)
    .agg(pl.col("GNAME").alias("_GN"))
    .with_columns([
        pl.col("_GN").list.get(0).fill_null("").alias("GUARNAM1"),
        pl.col("_GN").list.get(1).fill_null("").alias("GUARNAM2"),
    ])
    .drop("_GN")
)


# ─────────────────────────────────────────────
# PROC SORT DATA=SASLN.LOAN&REPTMON1&NOWKS
#   OUT=SASLN(KEEP=ACCTNO NOTENO CURBAL); BY ACCTNO NOTENO
# DATA SASLN
#   KEEP ACCTNO NOTENO PREVBAL GUARNAM1 GUARNAM2
#   MERGE SASLN NPL(IN=AA) LIAB; BY ACCTNO NOTENO; IF AA
#   PREVBAL = CURBAL
# ─────────────────────────────────────────────
sasln_path = os.path.join(SASLN_DIR, f"LOAN{REPTMON1}{NOWKS}.parquet")
sasln_df = con.execute(f"""
    SELECT ACCTNO, NOTENO, CURBAL FROM '{sasln_path}'
    ORDER BY ACCTNO, NOTENO
""").pl()

sasln_df = (
    sasln_df
    .join(npl_df.select(["ACCTNO", "NOTENO"]), on=["ACCTNO", "NOTENO"], how="inner")
    .join(guarnam_df,                           on=["ACCTNO", "NOTENO"], how="left")
    .rename({"CURBAL": "PREVBAL"})              # PREVBAL = CURBAL
)
_sln_keep = ["ACCTNO", "NOTENO", "PREVBAL", "GUARNAM1", "GUARNAM2"]
sasln_df = sasln_df.select([c for c in _sln_keep if c in sasln_df.columns])


# ─────────────────────────────────────────────
# DATA WOFF
#   MERGE SASLN LOAN NPL; BY ACCTNO
#   PAYMENT = CURBAL - PREVBAL
#   * TOTIIS = ROUND(IIS+OIFEEAMT,.01)  ← commented out
#   * TOTAL  = ROUND(TOTIIS+SP,.01)     ← commented out
#   TOTAL  = TOTIIS + SP
#   RIND = 'D'
# ─────────────────────────────────────────────
woff_df = (
    sasln_df
    .join(loan_df, on="ACCTNO", how="left", suffix="_LN")
    .join(npl_df,  on="ACCTNO", how="left", suffix="_NPL")
)

_curbal  = "CURBAL"  if "CURBAL"  in woff_df.columns else "CURBAL_LN"
_totiis  = "TOTIIS"  if "TOTIIS"  in woff_df.columns else "TOTIIS_NPL"
_sp      = "SP"      if "SP"      in woff_df.columns else "SP_NPL"

woff_df = woff_df.with_columns([
    (pl.col(_curbal).fill_null(0.0) - pl.col("PREVBAL").fill_null(0.0))
        .alias("PAYMENT"),
    (pl.col(_totiis).fill_null(0.0) + pl.col(_sp).fill_null(0.0))
        .alias("TOTAL"),
    pl.lit(RIND).alias("RIND"),
])


# ─────────────────────────────────────────────
# PROC SORT DATA=NPL.LIST OUT=LIST; BY ACCTNO
# DATA WOFF
#   MERGE WOFF(IN=A) LIST(IN=B KEEP=ACCTNO); BY ACCTNO; IF A & B
# ─────────────────────────────────────────────
list_df = con.execute(f"""
    SELECT ACCTNO FROM '{os.path.join(NPL_DIR,"LIST.parquet")}' ORDER BY ACCTNO
""").pl()
woff_df = woff_df.join(list_df, on="ACCTNO", how="inner")


# ─────────────────────────────────────────────
# DATA WOFF
#   MERGE WOFF(IN=A DROP=NAME) CNAME(RENAME=(CUSTNAM1=NAME))
#   BY ACCTNO; IF A
# ─────────────────────────────────────────────
if "NAME" in woff_df.columns:
    woff_df = woff_df.drop("NAME")
woff_df = woff_df.join(
    cname_df.rename({"CUSTNAM1": "NAME"}),
    on="ACCTNO",
    how="left",
)


# ─────────────────────────────────────────────
# Fixed-width output helpers
# LRECL=750   OPTIONS MISSING=' '
# ─────────────────────────────────────────────
LRECL = 750

def _buf() -> list:
    return [" "] * LRECL

def _place(text: str, col: int, buf: list) -> None:
    """Put text at 1-based column col."""
    for i, ch in enumerate(str(text)):
        pos = col - 1 + i
        if 0 <= pos < LRECL:
            buf[pos] = ch

def _render(buf: list) -> str:
    return "".join(buf)

def _fc(val, width: int) -> str:
    """$CHARw. – left-justified, padded/truncated. Missing → spaces."""
    return f"{str(val or ''):<{width}}"[:width]

def _fn(val, width: int, dec: int = 0) -> str:
    """Numeric format w.d – right-justified. Missing → spaces (OPTIONS MISSING=' ')."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return " " * width
    try:
        s = f"{float(val):.{dec}f}"
        return f"{s:>{width}}"[:width]
    except (TypeError, ValueError):
        return " " * width

def _fd(val) -> str:
    """MMDDYY10. → 'MM/DD/YYYY' (10 chars). Missing → 10 spaces."""
    return _fmt_mmddyy10(val)


# ─────────────────────────────────────────────
# Build WOFFTEX1 record (intermediate dump)
# @133 TOTAL  ← /* commented out */ in SAS
# @374 CAP    ← /* commented out */ in SAS
# ─────────────────────────────────────────────
def _rec_wofftex1(row: dict) -> str:
    b = _buf()
    _place(_fc(row.get("BRANCH"),    7),     1,  b)
    _place(_fc(row.get("NAME"),     40),     9,  b)
    _place(_fn(row.get("ACCTNO"),   10),    50,  b)
    _place(_fn(row.get("NOTENO"),    5),    61,  b)
    _place(_fc(row.get("BORSTAT"),   1),    67,  b)
    _place(_fn(row.get("IIS"),      16, 2), 69,  b)
    _place(_fn(row.get("OI"),       16, 2), 85,  b)
    _place(_fn(row.get("TOTIIS"),   16, 2), 101, b)
    _place(_fn(row.get("SP"),       16, 2), 117, b)
    # /* @133 TOTAL    16.2 */   ← commented out
    _place(_fn(row.get("CURBAL"),   16, 2), 149, b)
    _place(_fn(row.get("PREVBAL"),  16, 2), 165, b)
    _place(_fn(row.get("PAYMENT"),  16, 2), 181, b)
    _place(_fn(row.get("ECSRRSRV"), 16, 2), 197, b)
    _place(_fn(row.get("POSTAMT"),  16, 2), 213, b)
    _place(_fn(row.get("OTHERAMT"), 16, 2), 229, b)
    _place(_fc(row.get("MATDATE"),  10),   245,  b)
    _place(_fn(row.get("LOANTYPE"),  3),   255,  b)
    _place(_fn(row.get("INTAMT"),   16, 2), 258, b)
    _place(_fc(row.get("POSTNTRN"),  1),   274,  b)
    _place(_fn(row.get("MARKETVL"), 16, 2), 278, b)
    _place(_fn(row.get("INTEARN4"), 16, 2), 294, b)
    _place(_fn(row.get("DAYS"),      6),   310,  b)
    _place(_fn(row.get("CUSTCODE"),  3),   317,  b)
    _place(_fc(row.get("RIND"),      1),   321,  b)
    _place(_fn(row.get("OIFEEAMT"), 16, 2), 322, b)
    _place(_fc(row.get("LASTTRA1"), 10),   339,  b)
    _place(_fn(row.get("LSTTRNCD"),  3),   350,  b)
    _place(_fn(row.get("MTHPDUE"),   2),   354,  b)
    _place(_fn(row.get("BALANCE"),  16, 2), 357, b)
    # /* @374 CAP      16.2 */   ← commented out
    _place(_fc(row.get("GUAREND"),  20),   408,  b)
    _place(_fc(row.get("GUARNAM1"), 40),   429,  b)
    _place(_fc(row.get("GUARNAM2"), 40),   470,  b)
    _place(_fd(row.get("ISSXDTE")),        511,  b)   # ISSXDTE MMDDYY10.
    _place(_fn(row.get("NETPROC"),  16, 2), 522, b)
    _place(_fc(row.get("COLLDESC"), 70),   539,  b)
    _place(_fn(row.get("COLLYEAR"),  4),   610,  b)
    _place(_fn(row.get("BILPAID"),   3),   615,  b)
    _place(_fc(row.get("CRRGRADE"),  5),   619,  b)
    _place(_fn(row.get("MARGINFI"), 16, 2), 625, b)
    _place(_fn(row.get("NOTETERM"),  3),   642,  b)
    _place(_fn(row.get("PAYAMT"),   16, 2), 646, b)
    _place(_fd(row.get("DOBMNI")),         663,  b)   # DOBMNI  MMDDYY10.
    _place(_fc(row.get("ECSRIND"),   1),   674,  b)
    return _render(b)


# ─────────────────────────────────────────────
# OPTIONS MISSING=' '
# DATA WOFF – SET WOFF; FILE WOFFTEX1; PUT ...
# ─────────────────────────────────────────────
with open(WOFFTEX1_PATH, "w", encoding="utf-8") as _f1:
    for _row in woff_df.iter_rows(named=True):
        _f1.write(_rec_wofftex1(_row) + "\n")


# ─────────────────────────────────────────────
# DATA TEXT
#   INFILE WOFFTEX1
#   INPUT (identical layout; @133 TOTAL and @374 CAP skipped)
#   SP    = SUM(BALANCE,-1*TOTIIS)
#   TOTAL = TOTIIS + SP
# ─────────────────────────────────────────────
def _read_wofftex1(line: str) -> dict:
    line = line.ljust(LRECL)

    def _gn(col: int, w: int) -> float:
        s = line[col-1:col-1+w].strip()
        return float(s) if s else 0.0

    def _gc(col: int, w: int) -> str:
        return line[col-1:col-1+w]

    rec = {
        "BRANCH":   _gc(1,  7),
        "NAME":     _gc(9,  40),
        "ACCTNO":   _gn(50, 10),
        "NOTENO":   _gn(61, 5),
        "BORSTAT":  _gc(67, 1),
        "IIS":      _gn(69, 16),
        "OI":       _gn(85, 16),
        "TOTIIS":   _gn(101, 16),
        "SP":       _gn(117, 16),
        # /* @133 TOTAL */  skipped
        "CURBAL":   _gn(149, 16),
        "PREVBAL":  _gn(165, 16),
        "PAYMENT":  _gn(181, 16),
        "ECSRRSRV": _gn(197, 16),
        "POSTAMT":  _gn(213, 16),
        "OTHERAMT": _gn(229, 16),
        "MATDATE":  _gc(245, 10),
        "LOANTYPE": _gn(255, 3),
        "INTAMT":   _gn(258, 16),
        "POSTNTRN": _gc(274, 1),
        "MARKETVL": _gn(278, 16),
        "INTEARN4": _gn(294, 16),
        "DAYS":     _gn(310, 6),
        "CUSTCODE": _gn(317, 3),
        "RIND":     _gc(321, 1),
        "OIFEEAMT": _gn(322, 16),
        "LASTTRA1": _gc(339, 10),
        "LSTTRNCD": _gn(350, 3),
        "MTHPDUE":  _gn(354, 2),
        "BALANCE":  _gn(357, 16),
        # /* @374 CAP */  skipped
        "GUAREND":  _gc(408, 20),
        "GUARNAM1": _gc(429, 40),
        "GUARNAM2": _gc(470, 40),
        "ISSXDTE":  _gc(511, 10),
        "NETPROC":  _gn(522, 16),
        "COLLDESC": _gc(539, 70),
        "COLLYEAR": _gn(610, 4),
        "BILPAID":  _gn(615, 3),
        "CRRGRADE": _gc(619, 5),
        "MARGINFI": _gn(625, 16),
        "NOTETERM": _gn(642, 3),
        "PAYAMT":   _gn(646, 16),
        "DOBMNI":   _gc(663, 10),
        "ECSRIND":  _gc(674, 1),
    }
    # SP    = SUM(BALANCE, -1*TOTIIS)
    rec["SP"]    = rec["BALANCE"] - rec["TOTIIS"]
    # TOTAL = TOTIIS + SP
    rec["TOTAL"] = rec["TOTIIS"] + rec["SP"]
    return rec

text_rows: list[dict] = []
with open(WOFFTEX1_PATH, "r", encoding="utf-8") as _f1:
    for _raw in _f1:
        text_rows.append(_read_wofftex1(_raw.rstrip("\n")))


# ─────────────────────────────────────────────
# Build WOFFTEXT record (final output)
# @133 TOTAL   – active
# @374 CAP     – active  (CAP=0)
# @391 LATECHG – active  (LATECHG=OI)
# ─────────────────────────────────────────────
def _rec_wofftext(row: dict) -> str:
    cap     = 0.0
    latechg = float(row.get("OI") or 0.0)      # LATECHG = OI

    b = _buf()
    _place(_fc(row.get("BRANCH"),    7),     1,  b)
    _place(_fc(row.get("NAME"),     40),     9,  b)
    _place(_fn(row.get("ACCTNO"),   10),    50,  b)
    _place(_fn(row.get("NOTENO"),    5),    61,  b)
    _place(_fc(row.get("BORSTAT"),   1),    67,  b)
    _place(_fn(row.get("IIS"),      16, 2), 69,  b)
    _place(_fn(row.get("OI"),       16, 2), 85,  b)
    _place(_fn(row.get("TOTIIS"),   16, 2), 101, b)
    _place(_fn(row.get("SP"),       16, 2), 117, b)
    _place(_fn(row.get("TOTAL"),    16, 2), 133, b)   # @133 TOTAL   – active
    _place(_fn(row.get("CURBAL"),   16, 2), 149, b)
    _place(_fn(row.get("PREVBAL"),  16, 2), 165, b)
    _place(_fn(row.get("PAYMENT"),  16, 2), 181, b)
    _place(_fn(row.get("ECSRRSRV"), 16, 2), 197, b)
    _place(_fn(row.get("POSTAMT"),  16, 2), 213, b)
    _place(_fn(row.get("OTHERAMT"), 16, 2), 229, b)
    _place(_fc(row.get("MATDATE"),  10),   245,  b)
    _place(_fn(row.get("LOANTYPE"),  3),   255,  b)
    _place(_fn(row.get("INTAMT"),   16, 2), 258, b)
    _place(_fc(row.get("POSTNTRN"),  1),   274,  b)
    _place(_fn(row.get("MARKETVL"), 16, 2), 278, b)
    _place(_fn(row.get("INTEARN4"), 16, 2), 294, b)
    _place(_fn(row.get("DAYS"),      6),   310,  b)
    _place(_fn(row.get("CUSTCODE"),  3),   317,  b)
    _place(_fc(row.get("RIND"),      1),   321,  b)
    _place(_fn(row.get("OIFEEAMT"), 16, 2), 322, b)
    _place(_fc(row.get("LASTTRA1"), 10),   339,  b)
    _place(_fn(row.get("LSTTRNCD"),  3),   350,  b)
    _place(_fn(row.get("MTHPDUE"),   2),   354,  b)
    _place(_fn(row.get("BALANCE"),  16, 2), 357, b)
    _place(_fn(cap,                 16, 2), 374, b)   # @374 CAP=0   – active
    _place(_fn(latechg,             16, 2), 391, b)   # @391 LATECHG – active
    _place(_fc(row.get("GUAREND"),  20),   408,  b)
    _place(_fc(row.get("GUARNAM1"), 40),   429,  b)
    _place(_fc(row.get("GUARNAM2"), 40),   470,  b)
    _place(_fd(row.get("ISSXDTE")),        511,  b)   # ISSXDTE MMDDYY10.
    _place(_fn(row.get("NETPROC"),  16, 2), 522, b)
    _place(_fc(row.get("COLLDESC"), 70),   539,  b)
    _place(_fn(row.get("COLLYEAR"),  4),   610,  b)
    _place(_fn(row.get("BILPAID"),   3),   615,  b)
    _place(_fc(row.get("CRRGRADE"),  5),   619,  b)
    _place(_fn(row.get("MARGINFI"), 16, 2), 625, b)
    _place(_fn(row.get("NOTETERM"),  3),   642,  b)
    _place(_fn(row.get("PAYAMT"),   16, 2), 646, b)
    _place(_fd(row.get("DOBMNI")),         663,  b)   # DOBMNI  MMDDYY10.
    _place(_fc(row.get("ECSRIND"),   1),   674,  b)
    return _render(b)


# ─────────────────────────────────────────────
# DATA WOFF NPL1.WOFFTXT
#   SET TEXT; CAP=0; LATECHG=OI
#   FILE WOFFTEXT; PUT ...
# ─────────────────────────────────────────────
final_rows: list[dict] = []
with open(WOFFTEXT_PATH, "w", encoding="utf-8") as _fout:
    for _row in text_rows:
        _fout.write(_rec_wofftext(_row) + "\n")
        final_rows.append(_row)

# Save NPL1.WOFFTXT parquet
if final_rows:
    pl.DataFrame(final_rows).write_parquet(NPL1_WOFFTXT)


# ─────────────────────────────────────────────
# PROC PRINT DATA=WOFF
#   VAR BRANCH NAME ACCTNO NOTENO BORSTAT IIS OI TOTIIS SP TOTAL CURBAL
#       PREVBAL PAYMENT MATDATE LOANTYPE INTAMT POSTNTRN
#       MARKETVL INTEARN4 DAYS LASTTRA1 LSTTRNCD MTHPDUE BALANCE
#   SUM IIS OI TOTIIS SP TOTAL CURBAL PREVBAL PAYMENT BALANCE
#   TITLE1 'LISTING OF ACCOUNTS FOR BAD DEBT WRITING-OFF EXERCISE'
#   TITLE2 'AS AT ' "&RDATE"
# ─────────────────────────────────────────────
_PVAR = ["BRANCH", "NAME", "ACCTNO", "NOTENO", "BORSTAT",
         "IIS", "OI", "TOTIIS", "SP", "TOTAL", "CURBAL",
         "PREVBAL", "PAYMENT", "MATDATE", "LOANTYPE", "INTAMT",
         "POSTNTRN", "MARKETVL", "INTEARN4", "DAYS",
         "LASTTRA1", "LSTTRNCD", "MTHPDUE", "BALANCE"]
_SVAR = {"IIS", "OI", "TOTIIS", "SP", "TOTAL",
         "CURBAL", "PREVBAL", "PAYMENT", "BALANCE"}
_NVAR = {"ACCTNO", "NOTENO", "IIS", "OI", "TOTIIS", "SP", "TOTAL",
         "CURBAL", "PREVBAL", "PAYMENT", "LOANTYPE", "INTAMT",
         "MARKETVL", "INTEARN4", "DAYS", "LSTTRNCD", "MTHPDUE", "BALANCE"}
_CW = 16

with open(PRINT_PATH, "w", encoding="utf-8") as _fp:
    _fp.write("LISTING OF ACCOUNTS FOR BAD DEBT WRITING-OFF EXERCISE\n")
    _fp.write(f"AS AT {RDATE}\n")
    _fp.write("-" * (_CW * len(_PVAR)) + "\n")
    _fp.write("".join(f"{v:>{_CW}}" if v in _NVAR else f"{v:<{_CW}}"
                      for v in _PVAR) + "\n")
    _fp.write("-" * (_CW * len(_PVAR)) + "\n")

    _sums = {v: 0.0 for v in _SVAR}
    for _row in final_rows:
        _parts = []
        for v in _PVAR:
            _val = _row.get(v)
            if v in _NVAR:
                _fv = float(_val or 0.0)
                if v in _SVAR:
                    _sums[v] += _fv
                _parts.append(f"{_fv:>{_CW}.2f}")
            else:
                _parts.append(f"{str(_val or ''):<{_CW}}")
        _fp.write("".join(_parts) + "\n")

    _fp.write("-" * (_CW * len(_PVAR)) + "\n")
    _fp.write("".join(f"{_sums[v]:>{_CW}.2f}" if v in _SVAR else " " * _CW
                      for v in _PVAR) + "\n")

print(f"WOFFTEXT      : {WOFFTEXT_PATH}  ({len(final_rows)} records)")
print(f"NPL1.WOFFTXT  : {NPL1_WOFFTXT}")
print(f"Print listing : {PRINT_PATH}")
