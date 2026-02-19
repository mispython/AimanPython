# !/usr/bin/env python3
"""
Program : EIBMFC03
Purpose : FD by Individual and Non-Individual, by Time to Maturity for Foreign Currencies
          (Weighted Average Cost by Maturity Profile)
          Uses REMAINING MATURITY (actual REMMTH) for SUBTTL='A' unlike FC02.
Date    : 21.10.05
Dependency: %INC PGM(PBBDPFMT) -- FDPROD format
# Placeholder: PBBDPFMT FDPROD format -- maps INTPLAN to BIC code (e.g. '42630' = FCY FD)
"""

import duckdb
import polars as pl
import os
from datetime import date
from collections import defaultdict

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR    = r"/data"
FD_DIR      = os.path.join(BASE_DIR, "fd")
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")

OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EIBMFC03.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH = 60

# =============================================================================
# FORMAT DEFINITIONS  (identical to FC02 REMFMT)
# =============================================================================
def remfmt(v: float) -> str:
    if v is None: return "       "
    v = float(v)
    if v <= 0:   return "       "
    if v <= 1:   return ">0-1 MTH"
    if v <= 2:   return ">1-2 MTHS"
    if v <= 3:   return ">2-3 MTHS"
    if v <= 4:   return ">3-4 MTHS"
    if v <= 5:   return ">4-5 MTHS"
    if v <= 6:   return ">5-6 MTHS"
    if v <= 7:   return ">6-7 MTHS"
    if v <= 8:   return ">7-8 MTHS"
    if v <= 9:   return ">8-9 MTHS"
    if v <= 10:  return ">9-10 MTHS"
    if v <= 11:  return ">10-11 MTHS"
    if v <= 12:  return ">11-12 MTHS"
    if v <= 24:  return ">1-2 YRS"
    if v <= 36:  return ">2-3 YRS"
    if v <= 48:  return ">3-4 YRS"
    if v <= 60:  return ">4-5 YRS"
    if v == 91:  return " 1 MONTH"
    if v == 92:  return " 3 MONTHS"
    if v == 93:  return " 6 MONTHS"
    if v == 94:  return " 9 MONTHS"
    if v == 95:  return "12 MONTHS"
    if v == 96:  return "15 MONTHS"
    if v == 97:  return "ABOVE 15 MONTHS"
    if v == 99:  return "OVERDUE FD"
    return ">5 YRS"

SUBTTL_FMT = {
    'A': 'REMAINING MATURITY',
    'B': 'OVERDUE FD',
    'C': 'NEW FD FOR THE MONTH',
    'D': 'SAVING ACCOUNTS',
    'E': 'NON INTEREST BEARING',
    'F': 'INTEREST BEARING',
    'G': 'HOUSNG DEVELOPER ACC',
    'Z': 'TOTAL               ',
}

# FDPROD format: INTPLAN -> BNM BIC code string
# Source: PBBDPFMT FDProductFormat -- BIC '42630' identifies FCY FD accounts
from PBBDPFMT import FDProductFormat

def apply_fdprod(intplan: int) -> str:
    if intplan is None:
        return FDProductFormat.format(None)
    return FDProductFormat.format(int(intplan))

def days_in_month(month: int, year: int) -> int:
    if month == 2:
        return 29 if year % 4 == 0 else 28
    return [31,28,31,30,31,30,31,31,30,31,30,31][month - 1]

def calc_remmth(matdt: date, reptdate: date) -> float:
    rpyr, rpmth, rpday = reptdate.year, reptdate.month, reptdate.day
    mdday = matdt.day
    rpdays_rpmth = days_in_month(rpmth, rpyr)
    if mdday > rpdays_rpmth:
        mdday = rpdays_rpmth
    remy = matdt.year - rpyr
    remm = matdt.month - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / rpdays_rpmth

def parse_yymmdd8(val) -> date | None:
    if val is None: return None
    s = f"{int(val):08d}"
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except (ValueError, TypeError):
        return None

def fmt_comma12(v) -> str:
    if v is None: return f"{'0':>12}"
    return f"{round(float(v)):>12,}"

def fmt_comma12_2(v) -> str:
    if v is None: return f"{'0.00':>12}"
    return f"{float(v):>12,.2f}"

class ReportWriter:
    def __init__(self, filepath, page_length=60):
        self.filepath, self.page_length, self._lines = filepath, page_length, []
    def put(self, cc, text):
        self._lines.append((cc, text))
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
reptdate_path = os.path.join(FD_DIR, "REPTDATE.parquet")
rr = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
reptdate = rr[0]
if isinstance(reptdate, (int, float)):
    import datetime as _dt
    reptdate = date(1960, 1, 1) + _dt.timedelta(days=int(reptdate))
reptdate_dt = reptdate if isinstance(reptdate, date) else reptdate.date()
rdate = reptdate_dt.strftime("%d/%m/%y")

sdesc_path = os.path.join(FD_DIR, "SDESC.parquet")
try:
    sr = con.execute(f"SELECT SDESC FROM read_parquet('{sdesc_path}') LIMIT 1").fetchone()
    sdesc = (sr[0] or "")[:36] if sr else ""
except Exception:
    sdesc = ""

# =============================================================================
# STEP 2: Expand FD rows  (REMAINING MATURITY for SUBTTL='A')
# =============================================================================
fd_path = os.path.join(FD_DIR, "FD.parquet")
fd_raw  = con.execute(f"SELECT * FROM read_parquet('{fd_path}')").pl()

output_rows = []
for row in fd_raw.iter_rows(named=True):
    intplan = row.get("INTPLAN") or 0
    bic     = apply_fdprod(int(intplan))
    if bic != "42630":
        continue

    custcd  = int(row.get("CUSTCD") or 0)
    type_   = "  INDIVIDUALS  " if custcd in (77, 78, 95, 96) else "NON-INDIVIDUALS"
    subtyp  = "USD" if (470 <= intplan <= 475 or intplan == 560) else "OTH"

    openind = (row.get("OPENIND") or "").strip()
    curbal  = float(row.get("CURBAL") or 0)
    rate    = float(row.get("RATE")   or 0)
    term    = float(row.get("TERM")   or 0)
    matdt   = parse_yymmdd8(row.get("MATDATE"))

    if openind == "O" or (openind == "D" and curbal > 0):
        if openind == "D" or (matdt is not None and matdt < reptdate_dt):
            output_rows.append({
                "TYPE": type_, "PRODTYP": "FIXED DEPOSIT", "SUBTYP": subtyp,
                "SUBTTL": "B", "REMMTH": 99.0,
                "AMOUNT": curbal, "COST": curbal * rate,
            })
        else:
            # SUBTTL='A': use actual REMMTH (remaining, not original)
            remmth = calc_remmth(matdt, reptdate_dt) if matdt else 0.0
            output_rows.append({
                "TYPE": type_, "PRODTYP": "FIXED DEPOSIT", "SUBTYP": subtyp,
                "SUBTTL": "A", "REMMTH": remmth,
                "AMOUNT": curbal, "COST": curbal * rate,
            })
            # NEW FD: IF (TERM - REMMTH) < 1
            if (term - remmth) < 1:
                rc = term - 0.5
                if   rc <= 1:  rc = 91
                elif rc <= 3:  rc = 92
                elif rc <= 6:  rc = 93
                elif rc <= 9:  rc = 94
                elif rc <= 12: rc = 95
                elif rc <= 15: rc = 96
                else:          rc = 97
                output_rows.append({
                    "TYPE": type_, "PRODTYP": "FIXED DEPOSIT", "SUBTYP": subtyp,
                    "SUBTTL": "C", "REMMTH": rc,
                    "AMOUNT": curbal, "COST": curbal * rate,
                })

fd_exp = pl.DataFrame(output_rows) if output_rows else pl.DataFrame({
    c: pl.Series([], dtype=t) for c, t in [
        ("TYPE", pl.Utf8), ("PRODTYP", pl.Utf8), ("SUBTYP", pl.Utf8),
        ("SUBTTL", pl.Utf8), ("REMMTH", pl.Float64),
        ("AMOUNT", pl.Float64), ("COST", pl.Float64),
    ]
})

# =============================================================================
# STEP 3: PROC SUMMARY -> FD1
# =============================================================================
fd1 = (
    fd_exp
    .with_columns(pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8).alias("REMMTH_LBL"))
    .group_by(["TYPE", "PRODTYP", "SUBTYP", "SUBTTL", "REMMTH_LBL"])
    .agg([pl.col("AMOUNT").sum(), pl.col("COST").sum()])
)

# =============================================================================
# STEP 4: FD2 totals; SUBTTL='Z', PRODTYP='TOTAL'
# =============================================================================
fd2 = (
    fd_exp
    .group_by(["TYPE", "PRODTYP", "SUBTYP"])
    .agg([pl.col("AMOUNT").sum(), pl.col("COST").sum()])
    .with_columns([
        pl.lit("Z").alias("SUBTTL"),
        pl.lit("TOTAL").alias("PRODTYP"),
        pl.lit("TOTAL").alias("REMMTH_LBL"),
    ])
)

# =============================================================================
# STEP 5: Combine, WACOST, AMOUNT/1000
# =============================================================================
def combine_dep(d1, d2):
    cols = ["TYPE","PRODTYP","SUBTYP","SUBTTL","REMMTH_LBL","AMOUNT","COST"]
    def pad(df):
        for c in cols:
            if c not in df.columns:
                df = df.with_columns(pl.lit(None).alias(c))
        return df.select(cols)
    return pl.concat([pad(d1), pad(d2)]).with_columns([
        pl.when(pl.col("AMOUNT") != 0)
          .then(pl.col("COST") / pl.col("AMOUNT"))
          .otherwise(0.0)
          .alias("WACOST"),
        (pl.col("AMOUNT") / 1000).round(0).alias("AMOUNT"),
    ])

dep = combine_dep(fd1, fd2)

# =============================================================================
# STEP 6: Write report  (same layout as EIBMFC02 -- SUBTYP x TYPE cross-tab)
# =============================================================================
rpt = ReportWriter(OUTPUT_FILE, PAGE_LENGTH)
rpt.put('1', sdesc)
rpt.put(' ', f"TIME TO MATURITY AS AT {rdate}")
rpt.put(' ', "RISK MANAGEMENT REPORT : EIBMFC03")
rpt.put(' ', "RM DENOMINATION FOR FOREIGN CURRENCIES")
rpt.put(' ', "")

hdr1 = (
    f"{'FIXED DEPOSIT':<65}"
    f"{'USD':^28}"
    f"{'OTH':^28}"
    f"{'TOTAL':^28}"
)
hdr2 = (
    f"{'':65}"
    f"{'INDIVIDUALS':^14}{'NON-INDIVIDUALS':^14}"
    f"{'INDIVIDUALS':^14}{'NON-INDIVIDUALS':^14}"
    f"{'INDIVIDUALS':^14}{'NON-INDIVIDUALS':^14}"
)
hdr3 = f"{'':65}" + ("".join([f"{'BAL(RM000)':>12}{'WA.COST%':>12}" for _ in range(3)]))
rpt.put(' ', hdr1)
rpt.put(' ', hdr2)
rpt.put(' ', hdr3)
rpt.put(' ', "-" * 137)

# Pivot
pivot: dict = defaultdict(lambda: defaultdict(lambda: {"AMOUNT": 0.0, "WACOST": 0.0}))
for row in dep.iter_rows(named=True):
    key = (row.get("PRODTYP",""), row.get("SUBTTL",""), row.get("REMMTH_LBL",""))
    col = (row.get("SUBTYP",""), row.get("TYPE","").strip())
    pivot[key][col]["AMOUNT"] = float(row.get("AMOUNT") or 0)
    pivot[key][col]["WACOST"] = float(row.get("WACOST") or 0)

for key in sorted(pivot.keys()):
    prodtyp, subttl, remmth_l = key
    subttl_lbl = SUBTTL_FMT.get(subttl, subttl)
    lbl  = f"{prodtyp:<20}{subttl_lbl:<25}{remmth_l:<20}"
    p    = pivot[key]
    line = (
        lbl
        + fmt_comma12(p[("USD","INDIVIDUALS")]["AMOUNT"])
        + fmt_comma12_2(p[("USD","INDIVIDUALS")]["WACOST"])
        + fmt_comma12(p[("USD","NON-INDIVIDUALS")]["AMOUNT"])
        + fmt_comma12_2(p[("USD","NON-INDIVIDUALS")]["WACOST"])
        + fmt_comma12(p[("OTH","INDIVIDUALS")]["AMOUNT"])
        + fmt_comma12_2(p[("OTH","INDIVIDUALS")]["WACOST"])
        + fmt_comma12(p[("OTH","NON-INDIVIDUALS")]["AMOUNT"])
        + fmt_comma12_2(p[("OTH","NON-INDIVIDUALS")]["WACOST"])
    )
    rpt.put(' ', line)

rpt.write()
con.close()
print(f"Report written to: {OUTPUT_FILE}")
