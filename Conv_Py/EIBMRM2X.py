# !/usr/bin/env python3
"""
Program : EIBMRM2X
Purpose : FD by Individual and Non-Individual, by Time to Maturity for ALCO
          (Weighted Average Cost by Maturity Profile) -- RM Denomination
          Uses ORIGINAL MATURITY (REMMTH = TERM) for SUBTTL='A'.
          Includes TYPE (Individuals / Non-Individuals) cross-tabulation.
Date    : 18.07.06
Dependency: %INC PGM(PBBDPFMT) -- FDPROD, TERMFMT formats
# Placeholder: PBBDPFMT FDPROD format -- maps INTPLAN to BIC code
# Placeholder: TERMFMT numeric format: 470/471/... -> 1, 472/... -> 3, 474/... -> 6
"""

import duckdb
import polars as pl
import os
from datetime import date
from collections import defaultdict

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR   = r"/data"
FD_DIR     = os.path.join(BASE_DIR, "fd")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EIBMRM2X.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH = 60

# =============================================================================
# FORMAT DEFINITIONS
# =============================================================================

def remfmt(v: float) -> str:
    """REMFMT for EIBMRM2X -- UP TO 1 WK, month-by-month up to 24, then year buckets."""
    if v is None: return ""
    v = float(v)
    if v <= 0.255: return "   UP TO 1 WK"
    if v <= 1:     return "  >  1 WK - 1 MTH"
    # Exact month labels 2-24
    for m in range(2, 25):
        if v == m: return f"  {m:2d} MONTHS"
    if v <= 36:  return ">2-3 YRS  "
    if v <= 48:  return ">3-4 YRS  "
    if v <= 60:  return ">4-5 YRS  "
    if v == 91:  return " 1 MONTH"
    if v == 92:  return " 3 MONTHS"
    if v == 93:  return " 6 MONTHS"
    if v == 94:  return " 9 MONTHS"
    if v == 95:  return "12 MONTHS"
    if v == 96:  return "15 MONTHS"
    if v == 97:  return "ABOVE 15 MONTHS"
    if v == 99:  return "OVERDUE FD"
    return ""

SUBTTL_FMT = {
    'A': 'ORIGINAL MATURITY',
    'B': 'OVERDUE FD',
    'C': 'NEW FD FOR THE MONTH',
    'D': 'SAVING ACCOUNTS',
    'E': 'NON INTEREST BEARING',
    'F': 'INTEREST BEARING',
    'G': 'HOUSNG DEVELOPER ACC',
    'H': 'PORTION FROM ACE ACC',
}

TERMFMT_ONES  = {470,471,476,477,482,483,488,489,494,495,548,549,554,555}
TERMFMT_THREE = {472,473,478,479,484,485,490,491,496,497,550,551,556,557}
TERMFMT_SIX   = {474,475,480,481,486,487,492,493,498,499,552,553,558,559}

def apply_termfmt(intplan: int) -> float:
    if intplan in TERMFMT_ONES:  return 1.0
    if intplan in TERMFMT_THREE: return 3.0
    if intplan in TERMFMT_SIX:   return 6.0
    return 0.0

# FDPROD format: INTPLAN -> BNM BIC code string -- from PBBDPFMT
from PBBDPFMT import FDProductFormat

def apply_fdprod(v) -> str:
    return FDProductFormat.format(int(v) if v is not None else None)

SAS_EPOCH = date(1960, 1, 1)

def parse_sas_date(v):
    if v is None: return None
    if isinstance(v, date): return v
    if isinstance(v, (int, float)):
        import datetime as _dt
        return SAS_EPOCH + _dt.timedelta(days=int(v))
    return None

def parse_yymmdd8(val):
    if val is None: return None
    s = f"{int(val):08d}"
    try: return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except: return None

def days_in_month(m, y):
    if m == 2: return 29 if y % 4 == 0 else 28
    return [31,28,31,30,31,30,31,31,30,31,30,31][m - 1]

def calc_remmth(matdt: date, reptdate: date) -> float:
    rpyr, rpmth, rpday = reptdate.year, reptdate.month, reptdate.day
    mdday = matdt.day
    rpd   = days_in_month(rpmth, rpyr)
    if mdday > rpd: mdday = rpd
    return (matdt.year - rpyr)*12 + (matdt.month - rpmth) + (mdday - rpday)/rpd

def is_sptf(intplan):
    return (340<=intplan<=359 or 448<=intplan<=459 or
            461<=intplan<=469 or 580<=intplan<=599 or 660<=intplan<=740)

def fmt_comma12(v) -> str:
    if v is None: return f"{'0':>12}"
    return f"{round(float(v)):>12,}"

def fmt_comma12_2(v) -> str:
    if v is None: return f"{'0.00':>12}"
    return f"{float(v):>12,.2f}"

def fmt_comma5_2(v) -> str:
    if v is None: return f"{'0.00':>5}"
    return f"{float(v):>5,.2f}"

class ReportWriter:
    def __init__(self, fp, pl=60): self.filepath,self.page_length,self._lines = fp,pl,[]
    def put(self, cc, text): self._lines.append((cc, text))
    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            pc = 0
            for cc, text in self._lines:
                if cc == '1':
                    if pc > 0:
                        while pc % self.page_length != 0: f.write(" \n"); pc += 1
                    f.write(f"1{text}\n"); pc += 1
                else:
                    f.write(f"{cc}{text}\n"); pc += 1

# =============================================================================
# STEP 1: REPTDATE
# =============================================================================
con = duckdb.connect()
reptdate_path = os.path.join(FD_DIR, "REPTDATE.parquet")
rr = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
reptdate_dt = parse_sas_date(rr[0]) or date.today()
rdate = reptdate_dt.strftime("%d/%m/%y")

sdesc_path = os.path.join(FD_DIR, "SDESC.parquet")
try:
    sr = con.execute(f"SELECT SDESC FROM read_parquet('{sdesc_path}') LIMIT 1").fetchone()
    sdesc = (sr[0] or "")[:36] if sr else ""
except Exception:
    sdesc = ""

# =============================================================================
# STEP 2: Expand FD rows  (ORIGINAL MATURITY = TERM for SUBTTL='A')
# =============================================================================
fd_path = os.path.join(FD_DIR, "FD.parquet")
fd_raw  = con.execute(f"SELECT * FROM read_parquet('{fd_path}')").pl()

FD_rows, TD_rows, FDN_rows = [], [], []
for row in fd_raw.iter_rows(named=True):
    intplan = int(row.get("INTPLAN") or 0)
    bnmcode = apply_fdprod(intplan)
    is_fcy  = (bnmcode == "42630")
    prodtyp = "FIXED DEPT(FCY)" if is_fcy else "FIXED DEPT(RM)"
    term    = apply_termfmt(intplan) if is_fcy else float(row.get("TERM") or 0)

    custcd  = int(row.get("CUSTCD") or 0)
    type_   = "  INDIVIDUALS  " if custcd in (76,77,78,95,96) else "NON-INDIVIDUALS"
    openind = (row.get("OPENIND") or "").strip()
    curbal  = float(row.get("CURBAL") or 0)
    rate    = float(row.get("RATE")   or 0)
    matdt   = parse_yymmdd8(row.get("MATDATE"))
    subtyp  = "SPTF" if is_sptf(intplan) else "CONVENTIONAL"

    if not (openind == "O" or (openind == "D" and curbal > 0)):
        continue

    base = {"TYPE":type_,"PRODTYP":prodtyp,"SUBTYP":subtyp,"TERM":term}
    if openind == "D" or (matdt is not None and matdt < reptdate_dt):
        TD_rows.append({**base,"SUBTTL":"B","REMMTH":99.0,
                         "AMOUNT":curbal,"COST":curbal*rate,"ORIGIN":curbal*99.0})
    else:
        remmt1 = calc_remmth(matdt, reptdate_dt) if matdt else 0.0
        # SUBTTL='A': REMMTH = TERM (original maturity)
        FD_rows.append({**base,"SUBTTL":"A","REMMTH":term,
                         "AMOUNT":curbal,"COST":curbal*rate,"ORIGIN":curbal*term})
        if (term - remmt1) < 1:
            rc = term - 0.5
            if   rc<=1: rc=91
            elif rc<=3: rc=92
            elif rc<=6: rc=93
            elif rc<=9: rc=94
            elif rc<=12:rc=95
            elif rc<=15:rc=96
            else:       rc=97
            FDN_rows.append({**base,"SUBTTL":"C","REMMTH":rc,
                              "AMOUNT":curbal,"COST":curbal*rate,"ORIGIN":curbal*rc})

# Dummy
TD_rows.append({"TYPE":"  INDIVIDUALS  ","PRODTYP":"FIXED DEPT(FCY)","SUBTYP":"CONVENTIONAL",
                 "SUBTTL":"B","REMMTH":99.0,"AMOUNT":0.0,"COST":0.0,"ORIGIN":0.0,"TERM":0.0})

def summ(rows, gcols=None):
    if gcols is None:
        gcols = ["TYPE","PRODTYP","SUBTYP","SUBTTL","REMMTH"]
    if not rows:
        return pl.DataFrame()
    df = pl.DataFrame(rows)
    for c in ["AMOUNT","COST","ORIGIN"]:
        if c not in df.columns: df = df.with_columns(pl.lit(0.0).alias(c))
    df = df.with_columns(
        pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8).alias("REMMTH_LBL")
    )
    gc = [c for c in gcols if c in df.columns] + ["REMMTH_LBL"]
    return df.group_by(gc).agg([
        pl.col("AMOUNT").sum(), pl.col("COST").sum(), pl.col("ORIGIN").sum()
    ])

td_sum  = summ(TD_rows)
fd_sum  = summ(FD_rows)
fdn_sum = summ(FDN_rows)

def combine_all(*dfs):
    cols = ["TYPE","PRODTYP","SUBTYP","SUBTTL","REMMTH_LBL","AMOUNT","COST","ORIGIN"]
    parts = []
    for df in dfs:
        if df is None or len(df) == 0: continue
        for c in cols:
            if c not in df.columns: df = df.with_columns(pl.lit(None).alias(c))
        parts.append(df.select(cols))
    return pl.concat(parts) if parts else pl.DataFrame()

dep = combine_all(td_sum, fd_sum, fdn_sum)

if len(dep) > 0:
    dep = dep.sort(["PRODTYP","SUBTTL","SUBTYP","TYPE","REMMTH_LBL"])

    # Generate WACOST, WAORIG, AMOUNT/1000
    dep = dep.with_columns([
        pl.when(pl.col("SUBTYP").is_in(["SPTF","CONVENTIONAL"]) & (pl.col("AMOUNT") != 0))
          .then(pl.col("COST") / pl.col("AMOUNT")).otherwise(0.0).alias("WACOST"),
        pl.when(pl.col("AMOUNT") != 0)
          .then(pl.col("ORIGIN") / pl.col("AMOUNT")).otherwise(0.0).alias("WAORIG"),
        (pl.col("AMOUNT") / 1000).round(0).alias("AMOUNT"),
    ])

    # SUBTYPE TOTAL (by TYPE, PRODTYP, SUBTTL, REMMTH1)
    deptotal = (
        dep
        .group_by(["TYPE","PRODTYP","SUBTTL","REMMTH_LBL"])
        .agg([pl.col("AMOUNT").sum(), pl.col("COST").sum(), pl.col("ORIGIN").sum()])
        .with_columns([
            (pl.col("COST") / (pl.col("AMOUNT")*1000).clip(lower_bound=0.0001)).alias("WACOST"),
            (pl.col("ORIGIN") / (pl.col("AMOUNT")*1000).clip(lower_bound=0.0001)).alias("WAORIG"),
            pl.lit("TOTAL").alias("SUBTYP"),
        ])
    )

    # TYPE TOTAL (by SUBTYP, PRODTYP, SUBTTL, REMMTH1)
    deptota2 = (
        dep
        .group_by(["SUBTYP","PRODTYP","SUBTTL","REMMTH_LBL"])
        .agg([pl.col("AMOUNT").sum(), pl.col("COST").sum(), pl.col("ORIGIN").sum()])
        .with_columns([
            (pl.col("COST") / (pl.col("AMOUNT")*1000).clip(lower_bound=0.0001)).alias("WACOST"),
            (pl.col("ORIGIN") / (pl.col("AMOUNT")*1000).clip(lower_bound=0.0001)).alias("WAORIG"),
            pl.lit("TOTAL").alias("TYPE"),
        ])
    )

    # SUB-TOTAL rows (REMMTH1='SUB-TOTAL')
    deptota3_cols = ["PRODTYP","SUBTTL","SUBTYP","TYPE","AMOUNT","COST","ORIGIN"]
    deptota3 = (
        pl.concat([
            dep.select([c for c in deptota3_cols if c in dep.columns]),
            deptotal.select([c for c in deptota3_cols if c in deptotal.columns]),
            deptota2.select([c for c in deptota3_cols if c in deptota2.columns]),
        ])
        .group_by(["PRODTYP","SUBTTL","SUBTYP","TYPE"])
        .agg([pl.col("AMOUNT").sum(), pl.col("COST").sum(), pl.col("ORIGIN").sum()])
        .with_columns([
            (pl.col("COST") / (pl.col("AMOUNT")*1000).clip(lower_bound=0.0001)).alias("WACOST"),
            (pl.col("ORIGIN") / (pl.col("AMOUNT")*1000).clip(lower_bound=0.0001)).alias("WAORIG"),
            pl.lit("SUB-TOTAL").alias("REMMTH_LBL"),
        ])
        .filter(pl.col("TYPE") != "               ")
    )

# =============================================================================
# STEP 3: Write report
# =============================================================================
rpt = ReportWriter(OUTPUT_FILE, PAGE_LENGTH)
rpt.put('1', 'PUBLIC BANK BERHAD')
rpt.put(' ', f"TIME TO MATURITY AS AT {rdate}")
rpt.put(' ', "RISK MANAGEMENT REPORT : EIBMRM02")
rpt.put(' ', "RM DENOMINATION")
rpt.put(' ', "")

# Header: SUBTYP x TYPE cross-tab
hdr1 = (
    f"{'DEPOSITS':<65}"
    f"{'SPTF':^32}"
    f"{'CONVENTIONAL':^32}"
    f"{'TOTAL':^32}"
)
hdr2 = (
    f"{'':65}"
    + ("".join([
        f"{'INDIVIDUALS':>12}{'NON-IND':>12}{'TOTAL':>8}"
    ] * 3))
)
hdr3 = (
    f"{'':65}"
    + ("".join([f"{'BAL(RM000)':>12}{'WA.COST%':>12}{'WAORIG':>8}" ] * 3))
)
rpt.put(' ', hdr1)
rpt.put(' ', hdr2)
rpt.put(' ', hdr3)
rpt.put(' ', "-" * 130)

if len(dep) > 0:
    # Build pivot: key=(PRODTYP,SUBTTL,REMMTH_LBL), col=(SUBTYP,TYPE)
    all_rows_for_pivot = pl.concat([dep, deptotal, deptota2, deptota3])
    all_rows_for_pivot = all_rows_for_pivot.filter(pl.col("TYPE").fill_null("") != "               ")

    pivot: dict = defaultdict(lambda: defaultdict(lambda: {"AMOUNT":0.0,"WACOST":0.0,"WAORIG":0.0}))
    for row in all_rows_for_pivot.iter_rows(named=True):
        key = (row.get("PRODTYP",""), row.get("SUBTTL",""), row.get("REMMTH_LBL",""))
        col = (row.get("SUBTYP","").strip(), row.get("TYPE","").strip())
        pivot[key][col]["AMOUNT"] = float(row.get("AMOUNT") or 0)
        pivot[key][col]["WACOST"] = float(row.get("WACOST") or 0)
        pivot[key][col]["WAORIG"] = float(row.get("WAORIG") or 0)

    for key in sorted(pivot.keys()):
        prodtyp, subttl, remmth_l = key
        lbl = f"{prodtyp:<20}{SUBTTL_FMT.get(subttl,subttl):<25}{remmth_l:<20}"
        p   = pivot[key]
        line = (
            lbl
            + fmt_comma12(p[("SPTF","INDIVIDUALS")]["AMOUNT"])
            + fmt_comma12_2(p[("SPTF","NON-INDIVIDUALS")]["WACOST"])
            + fmt_comma5_2(p[("SPTF","TOTAL")]["WAORIG"])
            + fmt_comma12(p[("CONVENTIONAL","INDIVIDUALS")]["AMOUNT"])
            + fmt_comma12_2(p[("CONVENTIONAL","NON-INDIVIDUALS")]["WACOST"])
            + fmt_comma5_2(p[("CONVENTIONAL","TOTAL")]["WAORIG"])
            + fmt_comma12(p[("TOTAL","INDIVIDUALS")]["AMOUNT"])
            + fmt_comma12_2(p[("TOTAL","NON-INDIVIDUALS")]["WACOST"])
            + fmt_comma5_2(p[("TOTAL","TOTAL")]["WAORIG"])
        )
        rpt.put(' ', line)

rpt.write()
con.close()
print(f"Report written to: {OUTPUT_FILE}")
