# !/usr/bin/env python3
"""
Program : EIBM619L
Purpose : Number of Loan Accounts by Product Report
          Reads loan data for a specific week/month, merges with branch file,
            summarizes by branch/category/product and produces a
          PROC REPORT-style output with sub-totals and branch totals.
Dependency: %INC PGM(BRANCHCD) -- branch code format definitions
BRANCHCD: branch code (BRCHCD) is read directly from BRHFILE flat file (@6 BRCHCD $3.)
          Numeric branch -> 'XXX/NNN' lookups available via X_BRANCHCD.format_brchcd()
AMTIND: read directly from LOAN parquet column ('D'=CONVENTIONAL, 'I'=ISLAMIC)
"""

import duckdb
import polars as pl
import os
from datetime import date

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR     = r"/data"
LOAN_DIR     = os.path.join(BASE_DIR, "loan")        # LOAN.REPTDATE, LOAN.LOAN<mm><wk>, LOAN.SDESC
BRHFILE_PATH = os.path.join(BASE_DIR, "brhfile", "BRHFILE.txt")
OUTPUT_DIR   = os.path.join(BASE_DIR, "output")

OUTPUT_FILE  = os.path.join(OUTPUT_DIR, "EIBM619L.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH  = 60

# =============================================================================
# HELPERS
# =============================================================================
def fmt_comma16(v) -> str:
    if v is None:
        return f"{'0':>16}"
    return f"{int(v):>16,}"

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
# STEP 1: Read REPTDATE, derive week/month macros
# =============================================================================
con = duckdb.connect()
reptdate_path = os.path.join(LOAN_DIR, "REPTDATE.parquet")
reptdate_row  = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
reptdate = reptdate_row[0]
if isinstance(reptdate, (int, float)):
    import datetime as _dt
    reptdate = date(1960, 1, 1) + _dt.timedelta(days=int(reptdate))
reptdate_dt = reptdate if isinstance(reptdate, date) else reptdate.date()

day = reptdate_dt.day
if   day == 8:  wk, sdd = '1', 1
elif day == 15: wk, sdd = '2', 9
elif day == 22: wk, sdd = '3', 16
else:           wk, sdd = '4', 23

reptmon = f"{reptdate_dt.month:02d}"
rdate   = reptdate_dt.strftime("%d/%m/%y")

# =============================================================================
# STEP 2: Read SDESC
# =============================================================================
sdesc_path = os.path.join(LOAN_DIR, "SDESC.parquet")
sdesc_row  = con.execute(f"SELECT SDESC FROM read_parquet('{sdesc_path}') LIMIT 1").fetchone()
sdesc      = (sdesc_row[0] or "")[:36] if sdesc_row else ""

# =============================================================================
# STEP 3: Read BRHFILE (fixed-width: @2 BRANCH 3., @6 BRCHCD $3.)
# =============================================================================
brhfile_rows = []
with open(BRHFILE_PATH, "r", encoding="utf-8") as f:
    for line in f:
        if len(line) >= 8:
            try:
                branch = int(line[1:4].strip())
                brchcd = line[5:8].strip()
                brhfile_rows.append({"BRANCH": branch, "BRCHCD": brchcd})
            except ValueError:
                continue

brhfile = pl.DataFrame(brhfile_rows)

# =============================================================================
# STEP 4: Load LOAN<mm><wk> -- filter PAIDIND != 'P'
# =============================================================================
loan_path = os.path.join(LOAN_DIR, f"LOAN{reptmon}{wk}.parquet")
p619 = con.execute(f"""
    SELECT BRANCH, AMTIND, PRODUCT, CURBAL
    FROM read_parquet('{loan_path}')
    WHERE PAIDIND <> 'P'
    ORDER BY BRANCH, AMTIND, PRODUCT
""").pl()

# =============================================================================
# STEP 5: PROC SUMMARY by BRANCH, AMTIND, PRODUCT -- SUM CURBAL, COUNT
# =============================================================================
p619o = (
    p619
    .with_columns(pl.col("CURBAL").cast(pl.Float64))
    .group_by(["BRANCH", "AMTIND", "PRODUCT"])
    .agg([
        pl.col("CURBAL").sum().alias("CURBAL"),
        pl.len().alias("_FREQ_"),
    ])
)

# =============================================================================
# STEP 6: Merge BRHFILE + P619O; assign CATEGORY and BRCX
# =============================================================================
p619_merged = (
    p619o
    .join(brhfile, on="BRANCH", how="left")
    .with_columns([
        pl.lit("ISLAMIC     ").alias("CATEGORY"),
        pl.col("_FREQ_").alias("ACCTS"),
    ])
    .with_columns(
        pl.when(pl.col("AMTIND") == "D")
          .then(pl.lit("CONVENTIONAL"))
          .otherwise(pl.col("CATEGORY"))
          .alias("CATEGORY")
    )
    .with_columns(
        (pl.col("BRANCH").cast(pl.Utf8).str.zfill(3) + pl.lit("-") +
         pl.col("BRCHCD").fill_null("")).alias("BRCX")
    )
    .sort(["BRCX", "CATEGORY", "PRODUCT"])
)

# =============================================================================
# STEP 7: Produce PROC REPORT-style output
# =============================================================================
rpt = ReportWriter(OUTPUT_FILE, PAGE_LENGTH)

# Titles
rpt.put('1', f"{sdesc}      REPORT ID: EIBM619L")
rpt.put(' ', f"NUMBER OF LOANS ACCOUNTS BY PRODUCT @ {rdate}")
rpt.put(' ', "")

# Column header (HEADSKIP HEADLINE)
hdr = f"{'BR-CODE':<8}  {'CATEGORY':<12}  {'PRODUCT':>7}  {'NUMBER OF ACCTS':>12}"
rpt.put(' ', hdr)
rpt.put(' ', "-" * len(hdr))

current_brcx     = None
current_category = None
cat_accts_sum    = 0
brcx_accts_sum   = 0

def flush_category(rpt, accts_sum):
    rpt.put(' ', " " * 22 + "-" * 35)
    rpt.put(' ', " " * 22 + f"{'SUB-TOTAL ':<10}{fmt_comma16(accts_sum)}")

def flush_brcx(rpt, accts_sum):
    rpt.put(' ', " " * 22 + f"{'TOTAL BY BRANCH ':<16}{fmt_comma16(accts_sum)}")
    rpt.put(' ', " " * 22 + "-" * 35)

for row in p619_merged.iter_rows(named=True):
    brcx     = row["BRCX"]     or ""
    category = row["CATEGORY"] or ""
    product  = row["PRODUCT"]  or 0
    accts    = int(row["ACCTS"] or 0)

    if current_category is not None and (category != current_category or brcx != current_brcx):
        flush_category(rpt, cat_accts_sum)
        cat_accts_sum = 0

    if current_brcx is not None and brcx != current_brcx:
        flush_brcx(rpt, brcx_accts_sum)
        brcx_accts_sum = 0

    current_brcx     = brcx
    current_category = category
    cat_accts_sum   += accts
    brcx_accts_sum  += accts

    line = f"{brcx:<8}  {category:<12}  {product:>7}  {fmt_comma16(accts)}"
    rpt.put(' ', line)

# Final breaks
if current_category is not None:
    flush_category(rpt, cat_accts_sum)
if current_brcx is not None:
    flush_brcx(rpt, brcx_accts_sum)

rpt.write()
con.close()
print(f"Report written to: {OUTPUT_FILE}")
