# !/usr/bin/env python3
"""
Program : EIMALIMT.py
Purpose : Gross Loan Approved Limit for OD, RC, HP and Other Loans
          Reads loan and branch data, classifies products into OD/RC/HP/LN,
            summarizes approved limits and produces 4 separate PROC REPORT-style
            reports (EILIMTOD, EILIMTRC, EILIMTHP, EILIMTLN).
Created : 31 DEC 2004
Modify  : ESMR07-1607
"""

import duckdb
import polars as pl
import os
from datetime import date

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR     = r"/data"
LOAN_DIR     = os.path.join(BASE_DIR, "loan")        # LOAN.REPTDATE, LOAN.LOAN<mm><wk>, LOAN.ULOAN<mm><wk>, LOAN.SDESC
BRHFILE_PATH = os.path.join(BASE_DIR, "brhfile", "BRHFILE.txt")
OUTPUT_DIR   = os.path.join(BASE_DIR, "output")

OUTPUT_FILE  = os.path.join(OUTPUT_DIR, "EIMALIMT.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH  = 60

# =============================================================================
# PRODUCT CLASSIFICATION SETS
# =============================================================================
OD_PRODUCTS  = {50,51,52,53,54,55,56,57,58,59,62,63,64,65}
OD2_PRODUCTS = {87,90,91,100,101,102,103,106,108,109,112,114,115,116,117,118,
                119,120,122,123,124,125,137,138,139,152,153,154,155,156,159,
                167,170,176,179,180,181,191,192,193,194,195,196,197,198}
RC_PRODUCTS  = {910}
RC2_PRODUCTS = {350,302,364,902}
HP_PRODUCTS  = {380,381,700,705,720}
LN_PRODUCTS  = {900,901,904,905,906,907,909,914,915}
LN2_PRODUCTS = {
    4,5,6,7,15,20,25,26,27,29,30,31,32,34,70,71,72,78,147,148,173,174,
    200,201,204,205,209,210,211,212,214,215,219,220,227,228,230,231,232,
    233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,300,
    301,304,305,309,310,320,325,330,335,345,355,143,356,357,358,359,361,
    362,363,391,504,505,509,510,515,517,518,519,73,521,524,525,527,531,
    532,533,556,559,560,565,566,567,568,569,570,573,574,925,999,60,61,
    177,178
}

DELETE_PRODUCTS = {150, 151}

# =============================================================================
# HELPERS
# =============================================================================
def fmt_comma9(v) -> str:
    if v is None: return f"{'0':>9}"
    return f"{int(v):>9,}"

def fmt_comma20_2(v) -> str:
    if v is None: return f"{'0.00':>20}"
    return f"{float(v):>20,.2f}"

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
reptdate_path = os.path.join(LOAN_DIR, "REPTDATE.parquet")
rr = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
reptdate = rr[0]
if isinstance(reptdate, (int, float)):
    import datetime as _dt
    reptdate = date(1960,1,1) + _dt.timedelta(days=int(reptdate))
reptdate_dt = reptdate if isinstance(reptdate, date) else reptdate.date()

day = reptdate_dt.day
wk  = '1' if day==8 else '2' if day==15 else '3' if day==22 else '4'
reptmon = f"{reptdate_dt.month:02d}"
repdate = reptdate_dt.strftime("%d/%m/%y")

# =============================================================================
# STEP 2: SDESC
# =============================================================================
sdesc_path = os.path.join(LOAN_DIR, "SDESC.parquet")
try:
    sr = con.execute(f"SELECT SDESC FROM read_parquet('{sdesc_path}') LIMIT 1").fetchone()
    sdesc = (sr[0] or "")[:36] if sr else ""
except Exception:
    sdesc = ""

# =============================================================================
# STEP 3: BRHDATA (fixed-width: @2 BRANCH 3., @6 BRHCODE $3.)
# =============================================================================
brh_rows = []
with open(BRHFILE_PATH, "r", encoding="utf-8") as f:
    for line in f:
        if len(line) >= 8:
            try:
                branch  = int(line[1:4].strip())
                brhcode = line[5:8].strip()
                brh_rows.append({"BRANCH": branch, "BRHCODE": brhcode})
            except ValueError:
                continue
brhdata = pl.DataFrame(brh_rows)

# =============================================================================
# STEP 4: Load LN data from LOAN<mm><wk> and ULOAN<mm><wk>
#         ULOAN has APPRLIMT renamed to APPRLIM2
# =============================================================================
loan_path  = os.path.join(LOAN_DIR, f"LOAN{reptmon}{wk}.parquet")
uloan_path = os.path.join(LOAN_DIR, f"ULOAN{reptmon}{wk}.parquet")

ln_main = con.execute(f"""
    SELECT ACCTNO, NOTENO, PRODCD, PRODUCT, BRANCH, APPRLIM2
    FROM read_parquet('{loan_path}')
    WHERE PRODUCT NOT IN (150, 151)
      AND (SUBSTR(PRODCD, 1, 2) = '34' OR PRODCD = '54120')
      AND PAIDIND <> 'P'
""").pl()

try:
    ln_u = con.execute(f"""
        SELECT ACCTNO, NOTENO, PRODCD, PRODUCT, BRANCH,
               APPRLIMT AS APPRLIM2
        FROM read_parquet('{uloan_path}')
        WHERE PRODUCT NOT IN (150, 151)
          AND (SUBSTR(PRODCD, 1, 2) = '34' OR PRODCD = '54120')
          AND PAIDIND <> 'P'
    """).pl()
    ln_all = pl.concat([ln_main, ln_u])
except Exception:
    ln_all = ln_main

# =============================================================================
# STEP 5: Merge LN + BRHDATA by BRANCH
# =============================================================================
ln_merged = ln_all.join(brhdata, on="BRANCH", how="left")
ln_merged = ln_merged.with_columns(
    pl.col("BRHCODE").fill_null("XXX")
)

# =============================================================================
# STEP 6: Classify into OD, RC, HP, LN buckets
# =============================================================================
od_rows, od2_rows = [], []
rc_rows, rc2_rows = [], []
hp_rows = []
ln_rows, ln2_rows = [], []

for row in ln_merged.iter_rows(named=True):
    product  = int(row.get("PRODUCT") or 0)
    apprlim2 = float(row.get("APPRLIM2") or 0)
    base     = {
        "PROD":     "",
        "BRHCODE":  row.get("BRHCODE") or "XXX",
        "APPRLIM2": apprlim2,
        "APPRLIM3": 0.0,
    }

    if product in OD_PRODUCTS:
        od_rows.append({**base, "PROD":"OD", "APPRLIM3": apprlim2})
    elif product in OD2_PRODUCTS:
        od2_rows.append({**base, "PROD":"OD", "APPRLIM3": 0.0})

    if product in RC_PRODUCTS:
        rc_rows.append({**base, "PROD":"RC", "APPRLIM3": apprlim2})
    elif product in RC2_PRODUCTS:
        rc2_rows.append({**base, "PROD":"RC", "APPRLIM3": 0.0})

    if product in HP_PRODUCTS:
        hp_rows.append({**base, "PROD":"HP", "APPRLIM3": apprlim2})

    if product in LN_PRODUCTS:
        ln_rows.append({**base, "PROD":"LN", "APPRLIM3": apprlim2})
    elif product in LN2_PRODUCTS:
        ln2_rows.append({**base, "PROD":"LN", "APPRLIM3": 0.0})

def make_df(rows):
    if not rows:
        return pl.DataFrame({
            "PROD":pl.Series([],dtype=pl.Utf8),
            "BRHCODE":pl.Series([],dtype=pl.Utf8),
            "APPRLIM2":pl.Series([],dtype=pl.Float64),
            "APPRLIM3":pl.Series([],dtype=pl.Float64),
        })
    return pl.DataFrame(rows)

od_df = pl.concat([make_df(od_rows), make_df(od2_rows)])
rc_df = pl.concat([make_df(rc_rows), make_df(rc2_rows)])
hp_df = make_df(hp_rows)
ln_df = pl.concat([make_df(ln_rows), make_df(ln2_rows)])

# =============================================================================
# STEP 7: PROC SUMMARY by PROD, BRHCODE; COUNT -> NOACCT
# =============================================================================
def summarize_loans(df):
    if len(df) == 0:
        return pl.DataFrame({
            "PROD":pl.Series([],dtype=pl.Utf8),
            "BRHCODE":pl.Series([],dtype=pl.Utf8),
            "NOACCT":pl.Series([],dtype=pl.Int64),
            "APPRLIM2":pl.Series([],dtype=pl.Float64),
            "APPRLIM3":pl.Series([],dtype=pl.Float64),
        })
    return (
        df.group_by(["PROD","BRHCODE"])
          .agg([
              pl.len().alias("NOACCT"),
              pl.col("APPRLIM2").sum(),
              pl.col("APPRLIM3").sum(),
          ])
          .sort(["PROD","BRHCODE"])
    )

od_sum = summarize_loans(od_df)
rc_sum = summarize_loans(rc_df)
hp_sum = summarize_loans(hp_df)
ln_sum = summarize_loans(ln_df)

# =============================================================================
# STEP 8: Write PROC REPORT-style sections
# =============================================================================
rpt = ReportWriter(OUTPUT_FILE, PAGE_LENGTH)

def write_report_section(rpt, df_sum, report_id, title3):
    """Write one PROC REPORT block with BREAK AFTER PROD and RBREAK AFTER."""
    rpt.put('1', sdesc)
    rpt.put(' ', f"REPORT ID : {report_id}")
    rpt.put(' ', title3)
    rpt.put(' ', "")

    hdr = (
        f"{'PROD':<8}  {'BRHCODE':<7}  {'NO OF A/C':>9}  "
        f"{'APPROVED LIMIT':>20}  {'O/W CORP APP.LIMIT':>20}"
    )
    rpt.put(' ', hdr)
    rpt.put(' ', "=" * len(hdr))

    if len(df_sum) == 0:
        rpt.put(' ', "(NO DATA)")
        return

    grand_noacct = 0
    grand_lim2   = 0.0
    grand_lim3   = 0.0

    current_prod = None
    prod_noacct  = 0
    prod_lim2    = 0.0
    prod_lim3    = 0.0

    def flush_prod(rpt, prod, pn, pl2, pl3):
        rpt.put(' ', "")
        rpt.put(' ', "=" * len(hdr))
        rpt.put(' ', (
            f"{prod:<8}  {'':7}  {fmt_comma9(pn)}  "
            f"{fmt_comma20_2(pl2)}  {fmt_comma20_2(pl3)}"
        ))
        rpt.put(' ', "=" * len(hdr))

    for row in df_sum.iter_rows(named=True):
        prod    = (row.get("PROD")    or "")
        brhcode = (row.get("BRHCODE") or "")
        noacct  = int(row.get("NOACCT") or 0)
        lim2    = float(row.get("APPRLIM2") or 0)
        lim3    = float(row.get("APPRLIM3") or 0)

        if current_prod is not None and prod != current_prod:
            flush_prod(rpt, current_prod, prod_noacct, prod_lim2, prod_lim3)
            grand_noacct += prod_noacct
            grand_lim2   += prod_lim2
            grand_lim3   += prod_lim3
            prod_noacct  = 0
            prod_lim2    = 0.0
            prod_lim3    = 0.0

        current_prod  = prod
        prod_noacct  += noacct
        prod_lim2    += lim2
        prod_lim3    += lim3

        line = (
            f"{prod:<8}  {brhcode:<7}  {fmt_comma9(noacct)}  "
            f"{fmt_comma20_2(lim2)}  {fmt_comma20_2(lim3)}"
        )
        rpt.put(' ', line)

    if current_prod is not None:
        flush_prod(rpt, current_prod, prod_noacct, prod_lim2, prod_lim3)
        grand_noacct += prod_noacct
        grand_lim2   += prod_lim2
        grand_lim3   += prod_lim3

    # RBREAK AFTER (grand total)
    rpt.put(' ', "=" * len(hdr))
    rpt.put(' ', (
        f"{'TOTAL':<8}  {'':7}  {fmt_comma9(grand_noacct)}  "
        f"{fmt_comma20_2(grand_lim2)}  {fmt_comma20_2(grand_lim3)}"
    ))
    rpt.put(' ', "=" * len(hdr))

write_report_section(rpt, od_sum, "EILIMTOD",
    f"LOAN APPROVED LIMIT AS AT {repdate}")
write_report_section(rpt, rc_sum, "EILIMTRC",
    f"LOAN APPROVED LIMIT AS AT {repdate}")
write_report_section(rpt, hp_sum, "EILIMTHP",
    f"LOAN APPROVED LIMIT AS AT {repdate}")
write_report_section(rpt, ln_sum, "EILIMTLN",
    f"LOAN APPROVED LIMIT AS AT {repdate}")

rpt.write()
con.close()
print(f"Report written to: {OUTPUT_FILE}")
