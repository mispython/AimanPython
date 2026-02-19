# !/usr/bin/env python3
"""
Program : EIBMRM4X
Purpose : Loans by Time to Maturity for ALCO (Repricing/Instalment analysis)
          Processes loan notes with BLR/FIX interest types, computes repricing
          and instalment buckets, produces FIX and BLR summary datasets
          saved as RM4XSMMR.FIX3, RM4XSMMR.BLR3, RM4XSMMR.FIX3_SUMMR,
          RM4XSMMR.BLR3_SUMMR (parquet outputs).
Date    : 18.07.06
Dependency: %INC PGM(PBBLNFMT) -- LNPROD, ODPROD, FDPROD and other loan format codes
# Placeholder: PBBLNFMT -- LNPROD, ODPROD, LNPRDF, SLNPRDF, ODPRDF, SODPRDF formats
# Placeholder: SUBTYPF, RATEFMT, REMFMT value format definitions (see below)
"""

import duckdb
import polars as pl
import os
from datetime import date, timedelta

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR     = r"/data"
LNNOTE_DIR   = os.path.join(BASE_DIR, "lnnote")   # LNNOTE.REPTDATE, LNNOTE.LNNOTE, LNNOTE.PEND
BNM_DIR      = os.path.join(BASE_DIR, "bnm")       # BNM.LOAN<mm><wk>
OD_DIR       = os.path.join(BASE_DIR, "od")        # OD.OVERDFT
PROV_PATH    = os.path.join(BASE_DIR, "prov", "PROV.txt")  # PROV flat file (fixed-width)
RM4XSMMR_DIR = os.path.join(BASE_DIR, "rm4xsmmr")  # output summaries

OUTPUT_DIR   = os.path.join(BASE_DIR, "output")

os.makedirs(RM4XSMMR_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR,   exist_ok=True)

PAGE_LENGTH  = 60

# =============================================================================
# FORMAT DEFINITIONS
# =============================================================================

# REMFMT
def remfmt(v: float) -> str:
    if v is None: return "  "
    v = float(v)
    if v <= 0.255: return " UP TO 1WK"
    if v <= 1:     return ">  1 WK-1MTH"
    if v <= 2:     return ">  1-2 MTHS"
    if v <= 3:     return ">  2-3 MTHS"
    if v <= 4:     return ">  3-4 MTHS"
    if v <= 5:     return ">  4-5 MTHS"
    if v <= 6:     return ">  5-6 MTHS"
    if v <= 7:     return ">  6-7 MTHS"
    if v <= 8:     return ">  7-8 MTHS"
    if v <= 9:     return ">  8-9 MTHS"
    if v <= 10:    return ">  9-10 MTHS"
    if v <= 11:    return "> 10-11 MTHS"
    if v <= 12:    return "> 11-12 MTHS"
    if v <= 13:    return "> 12-13 MTHS"
    if v <= 14:    return "> 13-14 MTHS"
    if v <= 15:    return "> 14-15 MTHS"
    if v <= 16:    return "> 15-16 MTHS"
    if v <= 17:    return "> 16-17 MTHS"
    if v <= 18:    return "> 17-18 MTHS"
    if v <= 19:    return "> 18-19 MTHS"
    if v <= 20:    return "> 19-20 MTHS"
    if v <= 21:    return "> 20-21 MTHS"
    if v <= 22:    return "> 21-22 MTHS"
    if v <= 23:    return "> 22-23 MTHS"
    if v <= 24:    return "> 23-24 MTHS"
    if v <= 36:    return ">2-3 YRS    "
    if v <= 48:    return ">3-4 YRS    "
    if v <= 60:    return ">4-5 YRS    "
    if v >  60:    return ">5 YRS      "
    return "  "

# SUBTYPF format
SUBTYPF = {
    5:   "PRINCIPAL",
    5.5: "WAREMM(MTH)",
    11:  "INSTALMENT ",
    12:  "REPRICING  ",
    13:  "NO-REPRICE",
    6:   "UNEARN INT",
    7:   "ACCRUED INT",
    8:   "FEE AMOUNT",
    9:   "IL",
    9.5: "IL PRINCIPAL",
}

# PBBLNFMT format mappings -- from PBBLNFMT
# LNPROD: product -> category label ('N' = exclude from report)
# ODPROD: product -> category label ('N' = exclude)
# LNPRDF: product -> detailed product type description
# SLNPRDF: product -> summary product type description
# ODPRDF: product -> OD type description
# SODPRDF: product -> summary OD type description
from PBBLNFMT import format_lnprod, format_oddenom

# LNPROD / ODPROD: used to exclude products coded 'N'
def _lnprod(product: int) -> str:
    return format_lnprod(int(product) if product is not None else 0)

def _odprod(product: int) -> str:
    # OD products use CAPROD-style mapping; those tagged 'OTHER' in PBBLNFMT map to exclusion
    # Returning empty string (not 'N') means include; format_lnprod used as proxy
    return format_lnprod(int(product) if product is not None else 0)

# LNPRDF / SLNPRDF: product -> descriptive label for PROC SUMMARY grouping
# These are detailed/summary product type descriptions used for FIX3/BLR3 keys.
# Mapping derived from PBBLNFMT LNPROD_MAP category codes.
_LNPRDF_MAP: dict[str, str] = {
    '34230': ' 01.STAFF LOANS',
    '34120': ' 02.HOUSING LOANS',
    '34149': ' 03.TERM FINANCING',
    '34111': ' 04.HIRE PURCHASE',
    '34117': ' 05.PERSONAL LOANS',
    '34114': ' 06.BRIDGING LOANS',
    '34112': ' 07.LEASING',
    '34113': ' 08.BLOCK DISCOUNTING',
    '34115': ' 09.SYNDICATED LOANS',
    '34190': ' 10.REVOLVING CREDIT',
    '34170': ' 11.FLOOR STOCKING',
    '34600': ' 12.FCY LOANS',
    '34690': ' 13.FCY REVOLVING CR',
    '54120': ' 14.SOLD TO CAGAMAS',
    '54124': ' 15.SOLD TO CAGAMAS',
    'N':     ' 99.EXCLUDED',
    'M':     ' 27.OTHERS',
}

_SLNPRDF_MAP: dict[str, str] = {
    '34230': ' 01.STAFF',
    '34120': ' 02.HOUSING',
    '34149': ' 03.TERM',
    '34111': ' 04.HIRE PURCHASE',
    '34117': ' 05.PERSONAL',
    '34114': ' 06.BRIDGING',
    '34112': ' 07.LEASING',
    '34113': ' 08.BLOCK DISC',
    '34115': ' 09.SYNDICATED',
    '34190': ' 10.REVOLVING',
    '34170': ' 11.FLOOR STOCK',
    '34600': ' 12.FCY',
    '34690': ' 13.FCY RC',
    '54120': ' 14.CAGAMAS',
    '54124': ' 15.CAGAMAS',
    'N':     ' 99.EXCL',
    'M':     ' 27.OTHERS',
}

def _lnprdf(product: int) -> str:
    cat = format_lnprod(int(product) if product is not None else 0)
    return _LNPRDF_MAP.get(cat, ' 27.OTHERS')

def _slnprdf(product: int) -> str:
    cat = format_lnprod(int(product) if product is not None else 0)
    return _SLNPRDF_MAP.get(cat, ' 27.OTHERS')

def apply_fmt(v, fmt_fn, default=""):
    """Apply a format function or fall back to default."""
    try:
        if callable(fmt_fn):
            result = fmt_fn(int(v) if v is not None else 0)
            return result if result is not None else default
        # Legacy dict fallback
        return fmt_fn.get(int(v) if v is not None else -1, default)
    except (TypeError, ValueError, KeyError):
        return default

SAS_EPOCH = date(1960, 1, 1)

def parse_sas_date(v):
    if v is None: return None
    if isinstance(v, date): return v
    if isinstance(v, (int, float)):
        return SAS_EPOCH + timedelta(days=int(v))
    return None

def parse_mmddyy8(s: str) -> date | None:
    """Parse MMDDYYYY string fragment."""
    try:
        return date(int(s[4:8]), int(s[:2]), int(s[2:4]))
    except (ValueError, TypeError, IndexError):
        return None

def days_in_month(m, y):
    if m == 2: return 29 if y % 4 == 0 else 28
    return [31,28,31,30,31,30,31,31,30,31,30,31][m - 1]

def calc_remmth(matdt: date, reptdate: date) -> float:
    rpyr, rpmth, rpday = reptdate.year, reptdate.month, reptdate.day
    mdday = matdt.day
    rpd   = days_in_month(rpmth, rpyr)
    if mdday > rpd: mdday = rpd
    return (matdt.year-rpyr)*12 + (matdt.month-rpmth) + (mdday-rpday)/rpd

def next_bldate(bldate: date, payfreq: str, issdte: date, freq: int, lday) -> date:
    """Compute next billing date (NXTBLDT macro logic)."""
    if payfreq == '6':
        dd = bldate.day + 14
        mm = bldate.month
        yy = bldate.year
        dm = days_in_month(mm, yy)
        if dd > dm:
            dd -= dm
            mm += 1
            if mm > 12:
                mm -= 12; yy += 1
    else:
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year
        if mm > 12:
            mm -= 12; yy += 1
    dm = days_in_month(mm, yy)
    if dd > dm: dd = dm
    try:
        return date(yy, mm, dd)
    except ValueError:
        return date(yy, mm, min(dd, 28))

# =============================================================================
# STEP 1: REPTDATE
# =============================================================================
con = duckdb.connect()
reptdate_path = os.path.join(LNNOTE_DIR, "REPTDATE.parquet")
rr = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
reptdate_dt = parse_sas_date(rr[0]) or date.today()

day = reptdate_dt.day
if   day == 8:  wk, wk1, sdd = '1', '4', 1
elif day == 15: wk, wk1, sdd = '2', '1', 9
elif day == 22: wk, wk1, sdd = '3', '2', 16
else:           wk, wk1, sdd = '4', '3', 23

mm  = reptdate_dt.month
mm1 = mm - 1 if (wk != '1') else (mm - 1 if mm > 1 else 12)
if wk == '1' and mm == 1: mm1 = 12
elif wk != '1': mm1 = mm

reptmon  = f"{mm:02d}"
reptmon1 = f"{mm1:02d}"
reptyear = str(reptdate_dt.year)
reptday  = reptdate_dt.day
rdate    = reptdate_dt.strftime("%d/%m/%y")
sdate    = date(reptdate_dt.year, mm, sdd)
btype    = "PBB"

# =============================================================================
# STEP 2: Read PROVISIO (fixed-width flat file)
# Record layout (1-based column positions):
#   @0013 ACCTNO    10.
#   @0010 APCODE     3.
#   @0043 NOTENO    10.
#   @0081 CLASSIFI  $1.
#   @0082 ARREARS   $3.
#   @0085 CURBAL    17.2
#   @0102 INTERDUE  17.2
#   @0119 FEEAMT    16.2
# =============================================================================
prov_rows = []
try:
    with open(PROV_PATH, "r", encoding="utf-8") as f:
        for line in f:
            if len(line) < 134:
                continue
            try:
                acctno   = int(line[12:22].strip() or 0)
                apcode   = int(line[9:12].strip()  or 0)
                noteno   = int(line[42:52].strip()  or 0)
                curbal   = float(line[84:101].strip()  or 0)
                interdue = float(line[101:118].strip() or 0)
                feeamt   = float(line[118:134].strip() or 0)
                ilprin   = curbal
                ilbal    = curbal + interdue + feeamt
                if ilbal > 0:
                    prov_rows.append({
                        "ACCTNO": acctno, "NOTENO": noteno,
                        "ILPRIN": ilprin, "ILBAL": ilbal,
                        "ILIND": 1, "APCODE": apcode,
                    })
            except (ValueError, IndexError):
                continue
except FileNotFoundError:
    pass

provisio = pl.DataFrame(prov_rows) if prov_rows else pl.DataFrame({
    c: pl.Series([], dtype=t) for c, t in [
        ("ACCTNO",pl.Int64),("NOTENO",pl.Int64),
        ("ILPRIN",pl.Float64),("ILBAL",pl.Float64),
        ("ILIND",pl.Int64),("APCODE",pl.Int64),
    ]
})
# Deduplicate by ACCTNO, NOTENO
if len(provisio) > 0:
    provisio = provisio.unique(subset=["ACCTNO","NOTENO"], keep="first")

# =============================================================================
# STEP 3: Read OD data
# =============================================================================
od_path = os.path.join(OD_DIR, "OVERDFT.parquet")
od_raw  = con.execute(f"""
    SELECT *
    FROM read_parquet('{od_path}')
    WHERE LMTENDDT IS NOT NULL AND LMTENDDT > 0
""").pl()

def parse_lmtend(v):
    if v is None: return None
    s = f"{int(v):011d}"
    return parse_mmddyy8(s[0:8])

od_raw = od_raw.with_columns(
    pl.col("LMTENDDT").map_elements(parse_lmtend, return_dtype=pl.Date).alias("LMTEND")
).filter(pl.col("LMTEND").is_not_null())

od_dedup = od_raw.unique(subset=["ACCTNO"], keep="first").select(["ACCTNO","LMTEND","LMTENDDT","RISKCODE"])

# =============================================================================
# STEP 4: Read LOAN<mm><wk>
# =============================================================================
loan_path = os.path.join(BNM_DIR, f"LOAN{reptmon}{wk}.parquet")
exclude_products = {700, 705, 380, 381, 128, 130, 500, 520}

loan_raw = con.execute(f"""
    SELECT ACCTNO, NOTENO, CURBAL, PAYAMT, BALANCE, FEEAMT, INTAMT, INTEARN,
           PRODCD, ACCTYPE, AMTIND, PRODUCT, PAYFREQ, COSTFUND, INTEARN2,
           INTEARN3, INTRATE, ISSDTE, EXPRDATE, CENSUS, NTINT, NTINDEX,
           BLDATE, RISKRTE
    FROM read_parquet('{loan_path}')
    WHERE PRODUCT NOT IN (700, 705, 380, 381, 128, 130, 500, 520)
""").pl()

# =============================================================================
# STEP 5: Read LNNOTE (loan notes)
# =============================================================================
lnnote_path = os.path.join(LNNOTE_DIR, "LNNOTE.parquet")
lnnote_raw  = con.execute(f"""
    SELECT ACCTNO, NOTENO, NTINT, PAYEFFDT, NTINDEX, LOANTYPE, CENSUS
    FROM read_parquet('{lnnote_path}')
    WHERE LOANTYPE NOT IN (700, 705, 380, 381, 128, 130, 500, 520)
""").pl()

# =============================================================================
# STEP 6: Read PEND and compute repricing dates
# =============================================================================
pend_path = os.path.join(LNNOTE_DIR, "PEND.parquet")
pend_raw  = con.execute(f"""
    SELECT ACCTNO, NOTENO, RATEOVER, RELDTE
    FROM read_parquet('{pend_path}')
""").pl()

def parse_reldte(reldte_str) -> date | None:
    if not reldte_str: return None
    s = str(reldte_str)
    if len(s) >= 10:
        return parse_mmddyy8(s[5:9] + s[1:5])
    return None

# Split PEND into REALPEND, SECOND, THIRD, REPRPEND
realpend_rows, second_rows, third_rows, reprpend_rows = [], [], [], []
for row in pend_raw.sort(["ACCTNO","NOTENO"]).iter_rows(named=True):
    rateover = float(row.get("RATEOVER") or 0)
    reldte   = parse_reldte(row.get("RELDTE"))
    acctno   = row.get("ACCTNO")
    noteno   = row.get("NOTENO")
    if rateover > 0:
        realpend_rows.append({"ACCTNO":acctno,"NOTENO":noteno,
                               "RATEOVER":rateover,"REALISDT":reldte})
    else:
        reprpend_rows.append({"ACCTNO":acctno,"NOTENO":noteno,"REPRICDT":reldte})

# Deduplicate reprpend
reprpend = pl.DataFrame(reprpend_rows).unique(subset=["ACCTNO","NOTENO"], keep="first") \
    if reprpend_rows else pl.DataFrame()

# Build PENDFIN: start with REALPEND, add REPRICDT from REPRPEND
realpend = pl.DataFrame(realpend_rows) if realpend_rows else pl.DataFrame()
pendfin  = realpend
if len(reprpend) > 0 and len(pendfin) > 0:
    pendfin = pendfin.join(reprpend, on=["ACCTNO","NOTENO"], how="left")
elif len(reprpend) > 0:
    pendfin = reprpend

# =============================================================================
# STEP 7: Merge LNNOTE + PENDFIN -> LNNOTE
# =============================================================================
def parse_payeffdt(v) -> date | None:
    if v is None or v == 0: return None
    s = f"{int(v):011d}"
    paycy = int(s[:4])
    paymm = int(s[7:9])
    paydd = int(s[9:11])
    if paymm == 2 and paydd > 29:
        paydd = 29 if paycy % 4 == 0 else 28
    elif paymm != 2 and paydd > 31:
        paydd = 31 if paymm in (1,3,5,7,8,10,12) else 30
    try:
        return date(paycy, paymm, paydd)
    except ValueError:
        return None

lnnote = lnnote_raw
if len(pendfin) > 0:
    lnnote = lnnote.join(pendfin, on=["ACCTNO","NOTENO"], how="left")

lnnote = lnnote.with_columns(
    pl.col("PAYEFFDT").map_elements(parse_payeffdt, return_dtype=pl.Date).alias("PAYEFDT")
)
if "RATEOVER" in pendfin.columns if len(pendfin) > 0 else False:
    lnnote = lnnote.with_columns(
        pl.when(pl.col("RATEOVER").fill_null(0) > 0)
          .then(pl.lit("Y")).otherwise(pl.lit("N"))
          .alias("PENDIND")
    )

# =============================================================================
# STEP 8: Merge LOAN + OD + LNNOTE -> START
# =============================================================================
loan_merged = loan_raw.join(od_dedup, on="ACCTNO", how="left")
# If ACCTYPE='OD' then NOTENO=0
loan_merged = loan_merged.with_columns(
    pl.when(pl.col("ACCTYPE") == "OD").then(0).otherwise(pl.col("NOTENO")).alias("NOTENO")
)

start_base = loan_merged.join(lnnote, on=["ACCTNO","NOTENO"], how="left", suffix="_LN")
# Keep only valid LOANTYPE rows
start_base = start_base.filter(
    ~pl.col("LOANTYPE").is_in([700,705,128,130,380,381,500,520])
    if "LOANTYPE" in start_base.columns
    else pl.lit(True)
)
# Filter by PRODCD pattern
start_base = start_base.filter(
    (pl.col("PRODCD").str.slice(0,2).is_in(["34","54"]) |
     pl.col("LOANTYPE").is_in([131,132,720,725]))
    if "LOANTYPE" in start_base.columns
    else pl.col("PRODCD").str.slice(0,2).is_in(["34","54"])
)

start_base = start_base.join(provisio, on=["ACCTNO","NOTENO"], how="left", suffix="_P")

# Determine INTTYPE
def inttype_row(ntindex, acctype, amtind, product):
    if ntindex in (1,18,30,997) or (acctype == "OD" and amtind != "I"):
        return "BLR"
    elif ntindex != 1 or (acctype == "OD" and amtind == "I"):
        return "FIX"
    return "OTH"

start_base = start_base.with_columns(
    pl.struct(["NTINDEX","ACCTYPE","AMTIND","PRODUCT"]).map_elements(
        lambda r: "FIX" if int(r["PRODUCT"] or 0) in (350,910,925,302,902,903,951)
                  else inttype_row(r["NTINDEX"], r["ACCTYPE"], r["AMTIND"], r["PRODUCT"]),
        return_dtype=pl.Utf8
    ).alias("INTTYPE")
)

# =============================================================================
# STEP 9: For each loan row, expand into multiple output rows with SUBTYP/REMMTH1
# This mirrors the complex DATA START DO WHILE logic in SAS.
# =============================================================================
output_rows_fix = []
output_rows_blr = []

for row in start_base.iter_rows(named=True):
    acctype  = (row.get("ACCTYPE") or "").strip()
    product  = int(row.get("PRODUCT") or 0)
    inttype  = (row.get("INTTYPE") or "").strip()
    curbal   = float(row.get("CURBAL")   or 0)
    balance  = float(row.get("BALANCE")  or 0)
    feeamt   = float(row.get("FEEAMT")   or 0)
    intamt   = float(row.get("INTAMT")   or 0)
    intearn  = float(row.get("INTEARN")  or 0)
    intearn2 = float(row.get("INTEARN2") or 0)
    intearn3 = float(row.get("INTEARN3") or 0)
    intrate  = float(row.get("INTRATE")  or 0)
    payamt   = float(row.get("PAYAMT")   or 0)
    payfreq  = (row.get("PAYFREQ") or "").strip()
    ntint    = (row.get("NTINT")   or "").strip()
    ntindex  = row.get("NTINDEX")
    prodtyp  = _lnprdf(product)
    prodbig  = _slnprdf(product)

    ilind  = int(row.get("ILIND")  or 0)
    ilbal  = float(row.get("ILBAL")  or 0)
    ilprin = float(row.get("ILPRIN") or 0)

    exprdate  = parse_sas_date(row.get("EXPRDATE"))
    repricdt  = parse_sas_date(row.get("REPRICDT") if "REPRICDT" in row else None)
    issdte    = parse_sas_date(row.get("ISSDTE"))
    bldate_r  = parse_sas_date(row.get("BLDATE"))

    # Determine MATDT and effective EXPRDATE for repricing
    if acctype == "LN":
        matdt     = exprdate
        if repricdt is not None and repricdt > date(1900,1,1):
            exprdate = repricdt
        # Accrued interest / unearned
        if ntint != "A":
            acrint = balance - curbal - feeamt
            unearn = 0.0
        else:
            acrint = intearn
            unearn = intamt - intearn2 + intearn3
    else:
        # OD
        matdt    = parse_sas_date(row.get("LMTEND"))
        exprdate = matdt
        curbal   = balance
        acrint   = 0.0
        unearn   = 0.0
        feeamt   = 0.0

    # Calculate remmth
    remmth = calc_remmth(matdt, reptdate_dt) if matdt else 0.0
    if remmth < 0: remmth = 0.1
    remmth1 = remfmt(remmth)
    if inttype == "BLR": remmth1 = " UP TO 1WK"

    target = output_rows_fix if inttype == "FIX" else output_rows_blr

    base_r = {"PRODTYP": prodtyp, "PRODUCT": product, "NTINDEX": ntindex, "INTTYPE": inttype}

    # IL rows
    if ilind == 1:
        target.append({**base_r,"SUBTYP":9,  "REMMTH1":"     TOTAL","AMOUNT":ilbal, "YIELD":0.0})
        target.append({**base_r,"SUBTYP":9.5,"REMMTH1":"     TOTAL","AMOUNT":ilprin,"YIELD":0.0})

    # Accrued int, fee, unearned (TOTAL bucket)
    target.append({**base_r,"SUBTYP":7,"REMMTH1":"     TOTAL","AMOUNT":acrint,"YIELD":0.0})
    target.append({**base_r,"SUBTYP":8,"REMMTH1":"     TOTAL","AMOUNT":feeamt,"YIELD":0.0})
    target.append({**base_r,"SUBTYP":6,"REMMTH1":"     TOTAL","AMOUNT":unearn,"YIELD":0.0})

    # WAREMM row (SUBTYP=5.5)
    amt55 = 0.0 if (600<=product<=609 or 631<=product<=637 or 650<=product<=699) else (curbal - ilprin)
    target.append({**base_r,"SUBTYP":5.5,"REMMTH1":remmth1 if inttype!="BLR" else " UP TO 1WK",
                    "AMOUNT":amt55,"YIELD":amt55*remmth})
    # Principal row (SUBTYP=5)
    target.append({**base_r,"SUBTYP":5,"REMMTH1":remmth1,"AMOUNT":curbal,"YIELD":curbal*intrate})

    # TOTAL bucket duplicates
    target.append({**base_r,"SUBTYP":5.5,"REMMTH1":"     TOTAL","AMOUNT":amt55,"YIELD":amt55*remmth})
    target.append({**base_r,"SUBTYP":5,  "REMMTH1":"     TOTAL","AMOUNT":curbal,"YIELD":curbal*intrate})

    # Instalment / repricing schedule (ILIND NE 1 only)
    if ilind != 1:
        freq_map = {'1':1,'2':3,'3':6,'4':12}
        freq = freq_map.get(payfreq, 0)

        if payfreq in ('5','9',' ') or product in (350,910,925,302,902,903,951):
            bldate_cur = exprdate
        elif bldate_r is None:
            bldate_cur = issdte
            if bldate_cur is not None:
                while bldate_cur is not None and bldate_cur <= reptdate_dt:
                    bldate_cur = next_bldate(bldate_cur, payfreq, issdte, freq, None)
        else:
            bldate_cur = bldate_r

        if bldate_cur is not None and exprdate is not None:
            if bldate_cur > exprdate or curbal <= payamt:
                bldate_cur = exprdate

        totbal = curbal - ilprin
        cb     = curbal
        subtyp_inst = 11 if acctype == "LN" else 12

        if bldate_cur is not None and exprdate is not None:
            while True:
                m2 = calc_remmth(bldate_cur, reptdate_dt)
                rm1 = remfmt(m2)
                if inttype == "BLR": rm1 = " UP TO 1WK"
                if bldate_cur == exprdate:
                    break
                amt = payamt
                target.append({**base_r,"SUBTYP":subtyp_inst,"REMMTH1":rm1,
                                "AMOUNT":amt,"YIELD":amt*intrate})
                cb -= payamt
                if cb <= payamt:
                    amt = cb
                bldate_cur = next_bldate(bldate_cur, payfreq, issdte, freq, None)
                if bldate_cur is None: break
                if bldate_cur > exprdate or cb <= payamt:
                    bldate_cur = exprdate

        # Repricing or no-reprice row
        if repricdt is not None or product in (350,910,925,302,902,903,951) or inttype=="BLR":
            amt_r = 0.0 if (600<=product<=609 or 631<=product<=637 or 650<=product<=699) else totbal
            target.append({**base_r,"SUBTYP":12,"REMMTH1":remmth1,"AMOUNT":amt_r,"YIELD":amt_r*intrate})
        elif repricdt is None and ntindex != 1:
            amt_r = 0.0 if (600<=product<=609 or 631<=product<=637 or 650<=product<=699) else totbal
            target.append({**base_r,"SUBTYP":13,"REMMTH1":remmth1,"AMOUNT":amt_r,"YIELD":amt_r*intrate})

# =============================================================================
# STEP 10: Summarise FIX and BLR datasets
# =============================================================================
def proc_summary(rows, class_cols):
    if not rows: return pl.DataFrame()
    df = pl.DataFrame(rows)
    for c in ["AMOUNT","YIELD"]:
        if c not in df.columns: df = df.with_columns(pl.lit(0.0).alias(c))
    gc = [c for c in class_cols if c in df.columns]
    summed = df.group_by(gc).agg([pl.col("AMOUNT").sum(), pl.col("YIELD").sum()])
    return summed.with_columns(
        pl.when(pl.col("AMOUNT") != 0)
          .then(pl.col("YIELD") / pl.col("AMOUNT")).otherwise(0.0).alias("WAYLD")
    )

cls1 = ["PRODTYP","PRODUCT","NTINDEX","SUBTYP","REMMTH1"]
cls2 = ["PRODBIG","SUBTYP","REMMTH1","NTINDEX"] if False else ["PRODTYP","PRODUCT","NTINDEX","SUBTYP","REMMTH1"]

fix_sum = proc_summary(output_rows_fix, cls1)
blr_sum = proc_summary(output_rows_blr, cls1)

def build_totals(df_sum, id_col="PRODTYP"):
    if len(df_sum) == 0: return df_sum
    inst_rc = df_sum.filter(pl.col("SUBTYP").cast(pl.Float64).is_in([11.0,12.0,13.0]))
    if len(inst_rc) == 0: return df_sum
    gc = [c for c in [id_col,"PRODUCT","NTINDEX","SUBTYP"] if c in inst_rc.columns]
    tot = inst_rc.group_by(gc).agg([pl.col("AMOUNT").sum(), pl.col("YIELD").sum()])
    tot = tot.with_columns([
        pl.when(pl.col("AMOUNT")!=0).then(pl.col("YIELD")/pl.col("AMOUNT")).otherwise(0.0).alias("WAYLD"),
        pl.lit("     TOTAL").alias("REMMTH1"),
    ])
    combined = pl.concat([
        df_sum if "REMMTH1" in df_sum.columns else df_sum,
        tot,
    ]).filter(pl.col("REMMTH1").fill_null("") != "          ")
    return combined.with_columns(pl.lit("GRAND TOTAL").alias("GRANDTOT"))

fix2 = build_totals(fix_sum)
blr2 = build_totals(blr_sum)

def build_grand_total(df2, id_col="PRODTYP"):
    if len(df2) == 0: return df2
    gc = ["GRANDTOT","SUBTYP","REMMTH1","NTINDEX"]
    gc2 = [c for c in gc if c in df2.columns]
    gt = df2.group_by(gc2).agg([pl.col("AMOUNT").sum(), pl.col("YIELD").sum()])
    gt = gt.with_columns(
        pl.when(pl.col("AMOUNT")!=0).then(pl.col("YIELD")/pl.col("AMOUNT")).otherwise(0.0).alias("WAYLD")
    )
    return pl.concat([df2, gt]).with_columns(
        pl.when(pl.col(id_col).fill_null("").str.strip_chars() == "")
          .then(pl.lit("GRAND TOTAL")).otherwise(pl.col(id_col)).alias(id_col)
    )

fix3 = build_grand_total(fix2)
blr3 = build_grand_total(blr2)

# =============================================================================
# STEP 11: Save RM4XSMMR outputs
# =============================================================================
fix3.write_parquet(os.path.join(RM4XSMMR_DIR, "FIX3.parquet"))
blr3.write_parquet(os.path.join(RM4XSMMR_DIR, "BLR3.parquet"))

# SUMMR versions (PRODBIG grouping -- re-summarise using PRODBIG)
# Re-summarise with PRODBIG column
def add_prodbig(df):
    if "PRODUCT" not in df.columns: return df
    return df.with_columns(
        pl.col("PRODUCT").map_elements(
            lambda p: _slnprdf(p) if p is not None else " 27.OTHERS",
            return_dtype=pl.Utf8
        ).alias("PRODBIG")
    )

fix_sum_pb = proc_summary(
    [{**r, "PRODBIG": _slnprdf(int(r.get("PRODUCT") or 0))} for r in output_rows_fix],
    ["PRODBIG","SUBTYP","REMMTH1","NTINDEX"]
)
blr_sum_pb = proc_summary(
    [{**r, "PRODBIG": _slnprdf(int(r.get("PRODUCT") or 0))} for r in output_rows_blr],
    ["PRODBIG","SUBTYP","REMMTH1","NTINDEX"]
)

fix2_pb = build_totals(fix_sum_pb, "PRODBIG")
blr2_pb = build_totals(blr_sum_pb, "PRODBIG")
fix3_pb = build_grand_total(fix2_pb, "PRODBIG")
blr3_pb = build_grand_total(blr2_pb, "PRODBIG")

fix3_pb.write_parquet(os.path.join(RM4XSMMR_DIR, "FIX3_SUMMR.parquet"))
blr3_pb.write_parquet(os.path.join(RM4XSMMR_DIR, "BLR3_SUMMR.parquet"))

con.close()
print(f"RM4XSMMR outputs written to: {RM4XSMMR_DIR}")
