# !/usr/bin/env python3
"""
Program : EIBMRM1X
Purpose : Deposits by Time to Maturity for ALCO
          (Weighted Average Cost by Maturity Profile) -- RM Denomination
          Processes FD, Savings, and Current accounts with detailed
            maturity bucketing, sub-type and total aggregations.
          Produces two PROC TABULATE reports: SA/CA and FD sections.
Date    : 18.07.06
Dependency: %INC PGM(PBBDPFMT)
# Placeholder: PBBDPFMT -- FDPROD, SAPROD, CAPROD, ODPROD, LNPROD formats
# Placeholder: TERMFMT numeric format: 470/471/476... -> 1, 472/473... -> 3, 474/475... -> 6
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
BNM_DIR    = os.path.join(BASE_DIR, "bnm")    # BNM.REPTDATE, BNM.SAVING, BNM.FD, BNM.CURRENT
FD_DIR     = os.path.join(BASE_DIR, "fd")     # FD.FD (same as BNM.FD for this program)
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EIBMRM1X.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH = 60

# =============================================================================
# FORMAT DEFINITIONS
# =============================================================================

def remfmt(v: float) -> str:
    if v is None: return "  "
    v = float(v)
    if v <= 0:    return "       "
    if v <= 1:    return ">  0-1 MTH"
    if v <= 2:    return ">  1-2 MTHS"
    if v <= 3:    return ">  2-3 MTHS"
    if v <= 4:    return ">  3-4 MTHS"
    if v <= 5:    return ">  4-5 MTHS"
    if v <= 6:    return ">  5-6 MTHS"
    if v <= 7:    return ">  6-7 MTHS"
    if v <= 8:    return ">  7-8 MTHS"
    if v <= 9:    return ">  8-9 MTHS"
    if v <= 10:   return ">  9-10 MTHS"
    if v <= 11:   return "> 10-11 MTHS"
    if v <= 12:   return "> 11-12 MTHS"
    if v <= 13:   return "> 12-13 MTHS"
    if v <= 14:   return "> 13-14 MTHS"
    if v <= 15:   return "> 14-15 MTHS"
    if v <= 16:   return "> 15-16 MTHS"
    if v <= 17:   return "> 16-17 MTHS"
    if v <= 18:   return "> 17-18 MTHS"
    if v <= 19:   return "> 18-19 MTHS"
    if v <= 20:   return "> 19-20 MTHS"
    if v <= 21:   return "> 20-21 MTHS"
    if v <= 22:   return "> 21-22 MTHS"
    if v <= 23:   return "> 22-23 MTHS"
    if v <= 24:   return "> 23-24 MTHS"
    if v <= 36:   return ">2-3 YRS"
    if v <= 48:   return ">3-4 YRS"
    if v <= 60:   return ">4-5 YRS"
    if v == 91:   return " 1 MONTH"
    if v == 92:   return " 3 MONTHS"
    if v == 93:   return " 6 MONTHS"
    if v == 94:   return " 9 MONTHS"
    if v == 95:   return "12 MONTHS"
    if v == 96:   return "15 MONTHS"
    if v == 97:   return "ABOVE 15 MONTHS"
    if v == 99:   return "OVERDUE FD"
    return "  "

SUBTTL_FMT = {
    'A':  'REMAINING MATURITY',
    'B':  'OVERDUE FD',
    'C':  'NEW FD FOR THE MONTH',
    'D1': 'SAVING ACCOUNTS  ',
    'D2': 'WADIAH SAVING A/C',
    'E1': 'NORMAL CURRENT A/C',
    'E2': 'WADIAH CURRENT A/C',
    'E3': 'FCY CURRENT A/C',
    'E4': 'OD A/C',
    'F1': 'INT-BEAR. GOV.  ACCT',
    'F2': 'INT-BEAR. HSING ACCT',
    'F3': 'ACE < 5K            ',
    'F4': 'ACE > 5K            ',
    'F5': 'VOSTRO LOCAL        ',
    'F6': 'VOSTRO FOREIGN      ',
    'F7': 'PB SHARE LINK       ',
    'H':  'PORTION FROM ACE ACC',
    'I':  'SUB-TOTAL',
}

# TERMFMT: INTPLAN -> term in months
TERMFMT_ONES  = {470,471,476,477,482,483,488,489,494,495,548,549,554,555}
TERMFMT_THREE = {472,473,478,479,484,485,490,491,496,497,550,551,556,557}
TERMFMT_SIX   = {474,475,480,481,486,487,492,493,498,499,552,553,558,559}

def apply_termfmt(intplan: int) -> float:
    if intplan in TERMFMT_ONES:  return 1.0
    if intplan in TERMFMT_THREE: return 3.0
    if intplan in TERMFMT_SIX:   return 6.0
    return 0.0

# FDPROD / SAPROD / CAPROD format mappings -- from PBBDPFMT
from PBBDPFMT import FDProductFormat, SAProductFormat, CAProductFormat

def apply_fdprod(v) -> str:
    return FDProductFormat.format(int(v) if v is not None else None)

def apply_saprod(v) -> str:
    return SAProductFormat.format(int(v) if v is not None else None)

def apply_caprod(v) -> str:
    result = CAProductFormat.format(int(v) if v is not None else None)
    return result if result else "N"

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

def days_in_month(month, year):
    if month == 2: return 29 if year % 4 == 0 else 28
    return [31,28,31,30,31,30,31,31,30,31,30,31][month - 1]

def calc_remmth(matdt: date, reptdate: date) -> float:
    rpyr, rpmth, rpday = reptdate.year, reptdate.month, reptdate.day
    mdday = matdt.day
    rpd   = days_in_month(rpmth, rpyr)
    if mdday > rpd: mdday = rpd
    return (matdt.year - rpyr)*12 + (matdt.month - rpmth) + (mdday - rpday)/rpd

def is_sptf(intplan: int) -> bool:
    return (
        340 <= intplan <= 359 or 448 <= intplan <= 459 or
        461 <= intplan <= 469 or 580 <= intplan <= 599 or
        660 <= intplan <= 740
    )

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
    def __init__(self, filepath, page_length=60):
        self.filepath, self.page_length, self._lines = filepath, page_length, []
    def put(self, cc, text): self._lines.append((cc, text))
    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            pc = 0
            for cc, text in self._lines:
                if cc == '1':
                    if pc > 0:
                        while pc % self.page_length != 0:
                            f.write(" \n"); pc += 1
                    f.write(f"1{text}\n"); pc += 1
                else:
                    f.write(f"{cc}{text}\n"); pc += 1

# =============================================================================
# STEP 1: REPTDATE
# =============================================================================
con = duckdb.connect()
reptdate_path = os.path.join(BNM_DIR, "REPTDATE.parquet")
rr = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
reptdate_dt = parse_sas_date(rr[0]) or date.today()

day = reptdate_dt.day
nowk = '1' if day==8 else '2' if day==15 else '3' if day==22 else '4'
rdate = reptdate_dt.strftime("%d/%m/%y")

# =============================================================================
# STEP 2: Process FIXED DEPOSITS -> FD_rows, TD_rows, FDN_rows
# =============================================================================
fd_path = os.path.join(FD_DIR, "FD.parquet")
fd_raw  = con.execute(f"SELECT * FROM read_parquet('{fd_path}')").pl()

FD_rows, TD_rows, FDN_rows = [], [], []

for row in fd_raw.iter_rows(named=True):
    intplan  = int(row.get("INTPLAN") or 0)
    bnmcode  = apply_fdprod(intplan)
    is_fcy   = (bnmcode == "42630")
    prodtyp  = "FIXED DEPT(FCY)" if is_fcy else "FIXED DEPT(RM)"
    term     = apply_termfmt(intplan) if is_fcy else float(row.get("TERM") or 0)

    openind  = (row.get("OPENIND") or "").strip()
    curbal   = float(row.get("CURBAL") or 0)
    rate     = float(row.get("RATE")   or 0)
    matdt    = parse_yymmdd8(row.get("MATDATE"))

    subtyp   = "SPTF" if is_sptf(intplan) else "CONVENTIONAL"

    if not (openind == "O" or (openind == "D" and curbal > 0)):
        continue

    base = {"PRODTYP": prodtyp, "SUBTYP": subtyp, "TERM": term}

    if openind == "D" or (matdt is not None and matdt < reptdate_dt):
        remmth = 99.0
        cost   = curbal * rate
        TD_rows.append({**base, "SUBTTL":"B", "REMMTH":remmth,
                         "AMOUNT":curbal, "COST":cost, "REMM":curbal*remmth})
    else:
        remmth = calc_remmth(matdt, reptdate_dt) if matdt else 0.0
        cost   = curbal * rate
        remm   = curbal * remmth
        FD_rows.append({**base, "SUBTTL":"A", "REMMTH":remmth,
                         "AMOUNT":curbal, "COST":cost, "REMM":remm})
        if (term - remmth) < 1:
            rc = term - 0.5
            if   rc <= 1:  rc = 91
            elif rc <= 3:  rc = 92
            elif rc <= 6:  rc = 93
            elif rc <= 9:  rc = 94
            elif rc <= 12: rc = 95
            elif rc <= 15: rc = 96
            else:          rc = 97
            FDN_rows.append({**base, "SUBTTL":"C", "REMMTH":rc,
                              "AMOUNT":curbal, "COST":cost, "REMM":remmth*curbal})

# Dummy row for FCY
TD_rows.append({
    "PRODTYP":"FIXED DEPT(FCY)","SUBTYP":"CONVENTIONAL","SUBTTL":"B",
    "REMMTH":99.0,"AMOUNT":0.0,"COST":0.0,"REMM":0.0,"TERM":0.0
})

# =============================================================================
# STEP 3: Process SAVINGS
# =============================================================================
sa_path = os.path.join(BNM_DIR, "SAVING.parquet")
sa_raw  = con.execute(f"SELECT * FROM read_parquet('{sa_path}')").pl()
SA_rows = []
for row in sa_raw.iter_rows(named=True):
    product = int(row.get("PRODUCT") or 0)
    openind = (row.get("OPENIND") or "").strip()
    if openind in ("B","C","P"): continue
    curbal  = float(row.get("CURBAL")   or 0)
    intrate = float(row.get("INTRATE")  or 0)
    if curbal < 0: continue
    if product in (204, 214, 215):
        subtyp = "SPTF"
        subttl = "D2"
    else:
        subtyp = "CONVENTIONAL"
        subttl = "D1"
    SA_rows.append({
        "PRODTYP":"SAVINGS DEPOSIT","SUBTYP":subtyp,"SUBTTL":subttl,
        "REMMTH":0.0,"AMOUNT":curbal,"COST":curbal*intrate,"REMM":0.0
    })

# =============================================================================
# STEP 4: Process CURRENT accounts -> CA, CAG, CAS
# =============================================================================
ca_path = os.path.join(BNM_DIR, "CURRENT.parquet")
ca_raw  = con.execute(f"SELECT * FROM read_parquet('{ca_path}')").pl()
CA_rows, CAG_rows, CAS_rows = [], [], []

for row in ca_raw.iter_rows(named=True):
    product = int(row.get("PRODUCT") or 0)
    openind = (row.get("OPENIND") or "").strip()
    if openind in ("B","C","P"): continue
    bnmcode = apply_caprod(product)
    if bnmcode == "N": continue
    curbal  = float(row.get("CURBAL")  or 0)
    intrate = float(row.get("INTRATE") or 0)

    base = {"PRODTYP":"DEMAND DEPOSIT","SUBTYP":"CONVENTIONAL","REMMTH":0.0}

    if curbal > 0:
        if product in (101,103,161,163):
            subtyp  = "CONVENTIONAL" if product in (101,103) else "SPTF"
            subttl  = "F1" if product in (101,161) else "F2"
            CAG_rows.append({**base,"SUBTYP":subtyp,"SUBTTL":subttl,
                              "AMOUNT":curbal,"COST":curbal*intrate,"REMM":0.0})
        elif product in (150,151,152,181):
            if curbal <= 5000:
                CA_rows.append({**base,"SUBTTL":"F3","AMOUNT":curbal,"COST":0.0,"REMM":0.0})
            else:
                CAS_rows.append({"PRODTYP":"SAVINGS DEPOSIT","SUBTYP":"CONVENTIONAL",
                                  "SUBTTL":"H","REMMTH":0.0,"AMOUNT":curbal-5000,
                                  "COST":(curbal-5000)*intrate,"REMM":0.0})
                CA_rows.append({**base,"SUBTTL":"F4","AMOUNT":5000.0,"COST":0.0,"REMM":0.0})
        elif product in (60,61,62,63,64,160,162,164,165,166,182):
            CA_rows.append({**base,"SUBTYP":"SPTF","SUBTTL":"E2",
                             "AMOUNT":curbal,"COST":curbal*intrate,"REMM":0.0})
        elif 400 <= product <= 410:
            CA_rows.append({**base,"SUBTTL":"E3",
                             "AMOUNT":curbal,"COST":curbal*intrate,"REMM":0.0})
        elif product in (104,105,177,189,190,178):
            subttl = "F5" if product==104 else "F6" if product==105 else "F7"
            CA_rows.append({**base,"SUBTTL":subttl,
                             "AMOUNT":curbal,"COST":curbal*intrate,"REMM":0.0})
        elif product not in (101,104,105,107,113,150,151,152,178,189,190):
            CA_rows.append({**base,"SUBTTL":"E1",
                             "AMOUNT":curbal,"COST":curbal*intrate,"REMM":0.0})
    elif curbal <= 0:
        CA_rows.append({**base,"SUBTTL":"E4","AMOUNT":curbal,"COST":curbal*intrate,"REMM":0.0})

# =============================================================================
# STEP 5: PROC SUMMARY for each dataset
# =============================================================================
def summarize(rows, group_cols=None):
    if not group_cols:
        group_cols = ["PRODTYP","SUBTYP","SUBTTL","REMMTH"]
    if not rows:
        return pl.DataFrame({c: pl.Series([], dtype=pl.Utf8) for c in group_cols} |
                            {"AMOUNT": pl.Series([], dtype=pl.Float64),
                             "COST":   pl.Series([], dtype=pl.Float64),
                             "REMM":   pl.Series([], dtype=pl.Float64)})
    df = pl.DataFrame(rows)
    for c in ["AMOUNT","COST","REMM"]:
        if c not in df.columns:
            df = df.with_columns(pl.lit(0.0).alias(c))
    df = df.with_columns(
        pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8).alias("REMMTH_LBL")
    )
    gc = [c for c in group_cols if c in df.columns] + ["REMMTH_LBL"]
    return df.group_by(gc).agg([
        pl.col("AMOUNT").sum(), pl.col("COST").sum(), pl.col("REMM").sum()
    ])

td_sum  = summarize(TD_rows)
fd_sum  = summarize(FD_rows)
fdn_sum = summarize(FDN_rows)
sa_sum  = summarize(SA_rows)
ca_sum  = summarize(CA_rows)
cag_sum = summarize(CAG_rows)
cas_sum = summarize(CAS_rows)

# =============================================================================
# STEP 6: Combine and compute WACOST/WAREMM/AMOUNT
# =============================================================================
def mk_dep_row(df):
    cols = ["PRODTYP","SUBTYP","SUBTTL","REMMTH_LBL","AMOUNT","COST","REMM"]
    for c in cols:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))
    return df.select(cols)

fd_combined = pl.concat([mk_dep_row(td_sum), mk_dep_row(fd_sum), mk_dep_row(fdn_sum)])
dep_all     = pl.concat([mk_dep_row(fd_combined),
                         mk_dep_row(sa_sum), mk_dep_row(ca_sum),
                         mk_dep_row(cag_sum), mk_dep_row(cas_sum)])

dep_all = dep_all.with_columns([
    pl.when(pl.col("SUBTYP").is_in(["SPTF","CONVENTIONAL"]) & (pl.col("AMOUNT") != 0))
      .then(pl.col("COST") / pl.col("AMOUNT")).otherwise(0.0).alias("WACOST"),
    pl.when(pl.col("AMOUNT") != 0)
      .then(pl.col("REMM") / pl.col("AMOUNT")).otherwise(0.0).alias("WAREMM"),
    (pl.col("AMOUNT") / 1000).round(0).alias("AMOUNT"),
    pl.when(pl.col("SUBTTL").is_in(["E1","E2","E3"]))
      .then(pl.lit("CA NON-INT BEARING"))
      .when(pl.col("SUBTTL").is_in(["F1","F2","F3","F4","F5","F6","F7"]))
      .then(pl.lit("CA INT BEARING"))
      .otherwise(pl.col("PRODTYP"))
      .alias("PRODTYP"),
])

# =============================================================================
# STEP 7: Write reports
# =============================================================================
rpt = ReportWriter(OUTPUT_FILE, PAGE_LENGTH)
rpt.put('1', 'PUBLIC BANK BERHAD')
rpt.put(' ', f"TIME TO MATURITY AS AT {rdate}")
rpt.put(' ', "RISK MANAGEMENT REPORT : EIBMRM01")
rpt.put(' ', "RM DENOMINATION")
rpt.put(' ', "")

# Column headers
hdr = (
    f"{'DEPOSITS':<65}"
    f"{'SPTF':^12}{'':>12}{'':>5}"
    f"{'CONVENTIONAL':^12}{'':>12}{'':>5}"
    f"{'TOTAL':^12}{'':>12}{'':>5}"
)
sub_hdr = (
    f"{'':65}"
    f"{'BAL(RM000)':>12}{'WA.COST%':>12}{'REM.MTH':>5}"
    f"{'BAL(RM000)':>12}{'WA.COST%':>12}{'REM.MTH':>5}"
    f"{'BAL(RM000)':>12}{'WA.COST%':>12}{'REM.MTH':>5}"
)
rpt.put(' ', hdr)
rpt.put(' ', sub_hdr)
rpt.put(' ', "-" * 110)

# Filter SA/CA (non-FD) rows
dep_saca = dep_all.filter(~pl.col("PRODTYP").is_in(["FIXED DEPT(RM)","FIXED DEPT(FCY)"]))
dep_saca_sorted = dep_saca.sort(["PRODTYP","SUBTTL","SUBTYP","REMMTH_LBL"])

def write_tabulate_section(rpt, data):
    pivot: dict = defaultdict(lambda: defaultdict(lambda: {"AMOUNT":0.0,"WACOST":0.0,"WAREMM":0.0}))
    for row in data.iter_rows(named=True):
        key = (row.get("PRODTYP",""), row.get("SUBTTL",""), row.get("REMMTH_LBL",""))
        col = row.get("SUBTYP","")
        pivot[key][col]["AMOUNT"] = float(row.get("AMOUNT") or 0)
        pivot[key][col]["WACOST"] = float(row.get("WACOST") or 0)
        pivot[key][col]["WAREMM"] = float(row.get("WAREMM") or 0)
    for key in sorted(pivot.keys()):
        prodtyp, subttl, remmth_l = key
        lbl = f"{prodtyp:<20}{SUBTTL_FMT.get(subttl,subttl):<25}{remmth_l:<20}"
        p   = pivot[key]
        line = (
            lbl
            + fmt_comma12(p["SPTF"]["AMOUNT"])
            + fmt_comma12_2(p["SPTF"]["WACOST"])
            + fmt_comma5_2(p["SPTF"]["WAREMM"])
            + fmt_comma12(p["CONVENTIONAL"]["AMOUNT"])
            + fmt_comma12_2(p["CONVENTIONAL"]["WACOST"])
            + fmt_comma5_2(p["CONVENTIONAL"]["WAREMM"])
        )
        rpt.put(' ', line)

write_tabulate_section(rpt, dep_saca_sorted)

# FD section (separate page)
rpt.put('1', 'PUBLIC BANK BERHAD')
rpt.put(' ', f"TIME TO MATURITY AS AT {rdate}")
rpt.put(' ', "RISK MANAGEMENT REPORT : EIBMRM01 (FD)")
rpt.put(' ', "RM DENOMINATION")
rpt.put(' ', "")
rpt.put(' ', hdr)
rpt.put(' ', sub_hdr)
rpt.put(' ', "-" * 110)

dep_fd = dep_all.filter(pl.col("PRODTYP").is_in(["FIXED DEPT(RM)","FIXED DEPT(FCY)"]))
write_tabulate_section(rpt, dep_fd.sort(["PRODTYP","SUBTTL","SUBTYP","REMMTH_LBL"]))

rpt.write()
con.close()
print(f"Report written to: {OUTPUT_FILE}")
