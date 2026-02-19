# !/usr/bin/env python3
"""
Program : EIBMFC01
Purpose : Time to Maturity for FCY Fixed Deposits Report
          (Weighted Average Cost by Maturity Profile)
          Reads FD data, computes remaining months to maturity,
            summarizes by product type/subtype/subtotal/remmth and
            produces a PROC TABULATE-style report.
Date    : 21.10.05
Dependency: %INC PGM(PBBDPFMT) -- FDPROD format (INTPLAN -> BIC code mapping)
# Placeholder: PBBDPFMT FDPROD format -- maps INTPLAN to BIC code (e.g. '42630' = FCY FD)
"""

import duckdb
import polars as pl
import os
from datetime import date
import math

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR   = r"/data"
FD_DIR     = os.path.join(BASE_DIR, "fd")       # FD.REPTDATE, FD.FD
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EIBMFC01.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH = 60

# =============================================================================
# FORMAT DEFINITIONS
# =============================================================================

# REMFMT: remaining months -> label
def remfmt(remmth: float) -> str:
    if remmth is None:
        return ""
    v = float(remmth)
    if v <= 0.255:    return " UP TO 1 WK"
    if v <= 1:        return ">  1 WK - 1 MTH"
    if v <= 2:        return ">1-2 MTHS"
    if v <= 3:        return ">2-3 MTHS"
    if v <= 4:        return ">3-4 MTHS"
    if v <= 5:        return ">4-5 MTHS"
    if v <= 6:        return ">5-6 MTHS"
    if v <= 7:        return ">6-7 MTHS"
    if v <= 8:        return ">7-8 MTHS"
    if v <= 9:        return ">8-9 MTHS"
    if v <= 10:       return ">9-10 MTHS"
    if v <= 11:       return ">10-11 MTHS"
    if v <= 12:       return ">11-12 MTHS"
    if v <= 24:       return ">1-2 YRS"
    if v <= 36:       return ">2-3 YRS"
    if v <= 48:       return ">3-4 YRS"
    if v <= 60:       return ">4-5 YRS"
    if v == 91:       return " 1 MONTH"
    if v == 92:       return " 3 MONTHS"
    if v == 93:       return " 6 MONTHS"
    if v == 94:       return " 9 MONTHS"
    if v == 95:       return "12 MONTHS"
    if v == 96:       return "15 MONTHS"
    if v == 97:       return "ABOVE 15 MONTHS"
    if v == 99:       return "OVERDUE FD"
    return ">5 YRS"

# SUBTTL format
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

# FDPROD format: INTPLAN (product code) -> BNM BIC code string
# Source: PBBDPFMT FDProductFormat -- BIC '42630' identifies FCY FD accounts
from PBBDPFMT import FDProductFormat

def apply_fdprod(intplan: int) -> str:
    if intplan is None:
        return FDProductFormat.format(None)
    return FDProductFormat.format(int(intplan))

# Days per month (standard; leap year RD2/MD2 updated at runtime)
DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

def is_leap(year: int) -> bool:
    return year % 4 == 0

def days_in_month(month: int, year: int) -> int:
    if month == 2:
        return 29 if is_leap(year) else 28
    return DAYS_IN_MONTH[month - 1]

def calc_remmth(matdt: date, reptdate: date) -> float:
    """Calculate remaining months (REMMTH macro logic)."""
    if matdt is None:
        return 0.0
    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    mdyr  = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    rpdays_rpmth = days_in_month(rpmth, rpyr)
    if mdday > rpdays_rpmth:
        mdday = rpdays_rpmth

    remy  = mdyr  - rpyr
    remm  = mdmth - rpmth
    remd  = mdday - rpday
    return remy * 12 + remm + remd / rpdays_rpmth

# =============================================================================
# HELPERS
# =============================================================================
def fmt_comma12(v) -> str:
    if v is None:
        return f"{'0':>12}"
    return f"{round(float(v)):>12,}"

def fmt_comma12_2(v) -> str:
    if v is None:
        return f"{'0.00':>12}"
    return f"{float(v):>12,.2f}"

class ReportWriter:
    def __init__(self, filepath: str, page_length: int = 60):
        self.filepath    = filepath
        self.page_length = page_length
        self._lines      = []

    def put(self, cc: str, text: str):
        self._lines.append((cc, text))

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            page_count = 0
            for (cc, text) in self._lines:
                if cc == '1':
                    if page_count > 0:
                        while page_count % self.page_length != 0:
                            f.write(" \n")
                            page_count += 1
                    f.write(f"1{text}\n")
                    page_count += 1
                else:
                    f.write(f"{cc}{text}\n")
                    page_count += 1

# =============================================================================
# STEP 1: Read REPTDATE
# =============================================================================
con = duckdb.connect()
reptdate_path = os.path.join(FD_DIR, "REPTDATE.parquet")
reptdate_row  = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
reptdate = reptdate_row[0]
if isinstance(reptdate, (int, float)):
    import datetime as _dt
    reptdate = date(1960, 1, 1) + _dt.timedelta(days=int(reptdate))
reptdate_dt = reptdate if isinstance(reptdate, date) else reptdate.date()

day = reptdate_dt.day
if   day == 8:  nowk = '1'
elif day == 15: nowk = '2'
elif day == 22: nowk = '3'
else:           nowk = '4'

reptmon  = f"{reptdate_dt.month:02d}"
rdate    = reptdate_dt.strftime("%d/%m/%y")

# SDESC -- used in TITLE1; read from FD.REPTDATE or placeholder
sdesc_path = os.path.join(FD_DIR, "SDESC.parquet")
try:
    sdesc_row = con.execute(f"SELECT SDESC FROM read_parquet('{sdesc_path}') LIMIT 1").fetchone()
    sdesc = (sdesc_row[0] or "")[:36] if sdesc_row else ""
except Exception:
    sdesc = ""

# =============================================================================
# STEP 2: Read FD data, filter BIC='42630', compute REMMTH, expand rows
# =============================================================================
fd_path = os.path.join(FD_DIR, "FD.parquet")
fd_raw  = con.execute(f"SELECT * FROM read_parquet('{fd_path}')").pl()

SAS_EPOCH = date(1960, 1, 1)

def parse_sas_date(v) -> date | None:
    if v is None:
        return None
    if isinstance(v, date):
        return v
    if isinstance(v, (int, float)):
        import datetime as _dt
        return SAS_EPOCH + _dt.timedelta(days=int(v))
    return None

def parse_yymmdd8(val) -> date | None:
    """Parse integer YYYYMMDD (from PUT(MATDATE,Z8.) -> INPUT(...,YYMMDD8.))"""
    if val is None:
        return None
    s = f"{int(val):08d}"
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except (ValueError, TypeError):
        return None

output_rows = []

for row in fd_raw.iter_rows(named=True):
    intplan = row.get("INTPLAN") or 0
    bic     = apply_fdprod(int(intplan))

    # IF BIC='42630'
    if bic != "42630":
        continue

    # SUBTYP
    subtyp = "USD" if (470 <= intplan <= 475 or intplan == 560) else "OTH"

    openind = (row.get("OPENIND") or "").strip()
    curbal  = float(row.get("CURBAL") or 0)
    rate    = float(row.get("RATE")   or 0)
    term    = float(row.get("TERM")   or 0)

    matdate_raw = row.get("MATDATE")
    matdt = parse_yymmdd8(matdate_raw)

    if openind == "O" or (openind == "D" and curbal > 0):
        if openind == "D" or (matdt is not None and matdt < reptdate_dt):
            # OVERDUE / D
            remmth = 99.0
            subttl = "B"
            cost   = curbal * rate
            output_rows.append({
                "PRODTYP": "FIXED DEPOSIT",
                "SUBTYP":  subtyp,
                "SUBTTL":  subttl,
                "REMMTH":  remmth,
                "AMOUNT":  curbal,
                "COST":    cost,
            })
        else:
            # REMAINING MATURITY
            remmth = calc_remmth(matdt, reptdate_dt) if matdt else 0.0
            subttl = "A"
            cost   = curbal * rate
            output_rows.append({
                "PRODTYP": "FIXED DEPOSIT",
                "SUBTYP":  subtyp,
                "SUBTTL":  subttl,
                "REMMTH":  remmth,
                "AMOUNT":  curbal,
                "COST":    cost,
            })
            # NEW FD FOR THE MONTH: IF (TERM - REMMTH) < 1
            if (term - remmth) < 1:
                subttl_c  = "C"
                remmth_c  = term - 0.5
                if   remmth_c <= 1:  remmth_c = 91
                elif remmth_c <= 3:  remmth_c = 92
                elif remmth_c <= 6:  remmth_c = 93
                elif remmth_c <= 9:  remmth_c = 94
                elif remmth_c <= 12: remmth_c = 95
                elif remmth_c <= 15: remmth_c = 96
                else:                remmth_c = 97
                output_rows.append({
                    "PRODTYP": "FIXED DEPOSIT",
                    "SUBTYP":  subtyp,
                    "SUBTTL":  subttl_c,
                    "REMMTH":  remmth_c,
                    "AMOUNT":  curbal,
                    "COST":    cost,
                })

fd_expanded = pl.DataFrame(output_rows) if output_rows else pl.DataFrame({
    "PRODTYP": pl.Series([], dtype=pl.Utf8),
    "SUBTYP":  pl.Series([], dtype=pl.Utf8),
    "SUBTTL":  pl.Series([], dtype=pl.Utf8),
    "REMMTH":  pl.Series([], dtype=pl.Float64),
    "AMOUNT":  pl.Series([], dtype=pl.Float64),
    "COST":    pl.Series([], dtype=pl.Float64),
})

# =============================================================================
# STEP 3: PROC SUMMARY by PRODTYP, SUBTYP, SUBTTL, REMMTH -> FD1
# =============================================================================
fd1 = (
    fd_expanded
    .with_columns(pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8).alias("REMMTH_LBL"))
    .group_by(["PRODTYP", "SUBTYP", "SUBTTL", "REMMTH_LBL"])
    .agg([pl.col("AMOUNT").sum(), pl.col("COST").sum()])
)

# =============================================================================
# STEP 4: PROC SUMMARY by PRODTYP, SUBTYP only -> FD2 (totals); SUBTTL='Z'
# =============================================================================
fd2 = (
    fd_expanded
    .group_by(["PRODTYP", "SUBTYP"])
    .agg([pl.col("AMOUNT").sum(), pl.col("COST").sum()])
    .with_columns([
        pl.lit(-1.0).alias("REMMTH"),
        pl.lit("Z").alias("SUBTTL"),
        pl.lit("TOTAL").alias("PRODTYP"),
        pl.lit("TOTAL").alias("REMMTH_LBL"),
    ])
)

# =============================================================================
# STEP 5: Combine FD1 + FD2 -> DEP; compute WACOST, AMOUNT in 000s
# =============================================================================
def combine_and_compute(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    all_cols = ["PRODTYP", "SUBTYP", "SUBTTL", "REMMTH_LBL", "AMOUNT", "COST"]
    def pad(df):
        for c in all_cols:
            if c not in df.columns:
                df = df.with_columns(pl.lit(None).alias(c))
        return df.select(all_cols)
    combined = pl.concat([pad(df1), pad(df2)])
    return combined.with_columns([
        pl.when(pl.col("AMOUNT") != 0)
          .then(pl.col("COST") / pl.col("AMOUNT"))
          .otherwise(0.0)
          .alias("WACOST"),
        (pl.col("AMOUNT") / 1000).round(0).alias("AMOUNT"),
    ])

dep = combine_and_compute(fd1, fd2)

# =============================================================================
# STEP 6: Write PROC TABULATE-style report
# =============================================================================
rpt = ReportWriter(OUTPUT_FILE, PAGE_LENGTH)

rpt.put('1', sdesc)
rpt.put(' ', f"TIME TO MATURITY AS AT {rdate}")
rpt.put(' ', "RISK MANAGEMENT REPORT : EIBMFC01")
rpt.put(' ', "RM DENOMINATION FOR FOREIGN CURRENCIES")
rpt.put(' ', "")

# Table header: FIXED DEPOSIT | SUBTYP columns (USD / OTH)
#               BAL OUTSTANDING (RM'000) | W.A. COST %
col_hdr = (
    f"{'FIXED DEPOSIT':<65}"
    f"{'USD':>20}"
    f"{'':>14}"
    f"{'OTH':>20}"
    f"{'':>14}"
)
rpt.put(' ', col_hdr)
sub_hdr = (
    f"{'':65}"
    f"{'BAL OUSTANDING (RM000)':>20}"
    f"{'W.A. COST %':>14}"
    f"{'BAL OUSTANDING (RM000)':>20}"
    f"{'W.A. COST %':>14}"
)
rpt.put(' ', sub_hdr)
rpt.put(' ', "-" * 113)

# Group by PRODTYP, SUBTTL, REMMTH_LBL; pivot SUBTYP
subtypes = ["USD", "OTH"]
dep_sorted = dep.sort(["PRODTYP", "SUBTTL", "REMMTH_LBL"])

prev_prodtyp = None
prev_subttl  = None
for row in dep_sorted.iter_rows(named=True):
    prodtyp  = row.get("PRODTYP") or ""
    subttl   = SUBTTL_FMT.get(row.get("SUBTTL") or "", row.get("SUBTTL") or "")
    remmth_l = row.get("REMMTH_LBL") or ""
    subtyp   = row.get("SUBTYP") or ""
    amount   = row.get("AMOUNT") or 0.0
    wacost   = row.get("WACOST") or 0.0

    lbl = f"{prodtyp:<20}{subttl:<25}{remmth_l:<20}"
    if subtyp == "USD":
        line = f"{lbl}{fmt_comma12(amount)}{fmt_comma12_2(wacost)}"
    else:
        line = f"{'':65}{'':12}{'':12}{fmt_comma12(amount)}{fmt_comma12_2(wacost)}"
    rpt.put(' ', line)

rpt.write()
con.close()
print(f"Report written to: {OUTPUT_FILE}")
