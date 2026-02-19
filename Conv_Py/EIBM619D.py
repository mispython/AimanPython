# !/usr/bin/env python3
"""
Program : EIBM619D
Purpose : Number of Deposit Accounts by Product Report
          Reads SA, FD, CA deposit data, merges with branch file,
            summarizes by branch/category/product and produces
            a PROC REPORT-style output with sub-totals and branch totals.
Dependency: %INC PGM(BRANCHCD,PBBDPFMT) -- format definitions referenced below
BRANCHCD: branch code (BRCHCD) is read directly from BRHFILE flat file (@6 BRCHCD $3.)
          Numeric branch -> 'XXX/NNN' lookups available via X_BRANCHCD.format_brchcd()
PBBDPFMT: SADenomFormat, FDDenomFormat, CADenomFormat imported below
"""

import duckdb
import polars as pl
import os
from datetime import date

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR     = r"/data"
BNM_DIR      = os.path.join(BASE_DIR, "bnm")        # BNM.REPTDATE, BNM.SAVING, BNM.FD, BNM.CURRENT
LOAN_DIR     = os.path.join(BASE_DIR, "loan")        # LOAN.SDESC
BRHFILE_PATH = os.path.join(BASE_DIR, "brhfile", "BRHFILE.txt")  # branch flat file
OUTPUT_DIR   = os.path.join(BASE_DIR, "output")

OUTPUT_FILE  = os.path.join(OUTPUT_DIR, "EIBM619D.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH  = 60

# =============================================================================
# FORMAT DEFINITIONS (SADENOM, FDDENOM, CADENOM) -- from PBBDPFMT
# Dependency: PBBDPFMT -- map PRODUCT -> denomination indicator
# 'D' = CONVENTIONAL, 'I' = ISLAMIC
# =============================================================================
from PBBDPFMT import SADenomFormat, FDDenomFormat, CADenomFormat

def apply_denom(product: int, denom_fn) -> str:
    """Apply a denomination format function; default 'D' if product is None."""
    if product is None:
        return 'D'
    return denom_fn(product)

# Wrap class .format() as callables expected by apply_denom
def _sadenom(p): return SADenomFormat.format(p)
def _fddenom(p): return FDDenomFormat.format(p)
def _cadenom(p): return CADenomFormat.format(p)

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
# STEP 1: Read REPTDATE
# =============================================================================
con = duckdb.connect()
reptdate_path = os.path.join(BNM_DIR, "REPTDATE.parquet")
reptdate_row  = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
reptdate = reptdate_row[0]
if isinstance(reptdate, (int, float)):
    import datetime as _dt
    reptdate = date(1960, 1, 1) + _dt.timedelta(days=int(reptdate))
reptdate_dt = reptdate if isinstance(reptdate, date) else reptdate.date()

reptmon = f"{reptdate_dt.month:02d}"
rdate   = reptdate_dt.strftime("%d/%m/%y")

# =============================================================================
# STEP 2: Read SDESC
# =============================================================================
sdesc_path = os.path.join(LOAN_DIR, "SDESC.parquet")
sdesc_row  = con.execute(f"SELECT SDESC FROM read_parquet('{sdesc_path}') LIMIT 1").fetchone()
sdesc      = (sdesc_row[0] or "")[:36] if sdesc_row else ""

# =============================================================================
# STEP 3: Read BRHFILE (fixed-width text: @2 BRANCH 3., @6 BRCHCD $3.)
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
# STEP 4: Read SA, FD, CA; assign AMTIND via denomination format
# =============================================================================
sa_path = os.path.join(BNM_DIR, "SAVING.parquet")
fd_path = os.path.join(BNM_DIR, "FD.parquet")
ca_path = os.path.join(BNM_DIR, "CURRENT.parquet")

sa_raw = con.execute(f"SELECT * FROM read_parquet('{sa_path}')").pl()
fd_raw = con.execute(f"SELECT * FROM read_parquet('{fd_path}')").pl()
ca_raw = con.execute(f"SELECT * FROM read_parquet('{ca_path}')").pl()

def add_amtind(df: pl.DataFrame, denom_fn) -> pl.DataFrame:
    products = df["PRODUCT"].to_list()
    amtind   = [apply_denom(p, denom_fn) for p in products]
    return df.with_columns(pl.Series("AMTIND", amtind))

sa_raw = add_amtind(sa_raw, _sadenom)
fd_raw = add_amtind(fd_raw, _fddenom)
ca_raw = add_amtind(ca_raw, _cadenom)

# =============================================================================
# STEP 5: Union SA + FD + CA -> DEP; filter out closed (B,C,P)
# =============================================================================
common_cols = ["BRANCH", "PRODUCT", "AMTIND", "CURBAL", "OPENIND"]

def safe_select(df, cols):
    existing = [c for c in cols if c in df.columns]
    missing  = [c for c in cols if c not in df.columns]
    result   = df.select(existing)
    for c in missing:
        result = result.with_columns(pl.lit(None).alias(c))
    return result.select(cols)

dep = pl.concat([
    safe_select(sa_raw, common_cols),
    safe_select(fd_raw, common_cols),
    safe_select(ca_raw, common_cols),
]).filter(~pl.col("OPENIND").is_in(["B", "C", "P"]))

# =============================================================================
# STEP 6: PROC SUMMARY by BRANCH, AMTIND, PRODUCT -- SUM CURBAL, COUNT -> ACCTS
# =============================================================================
p619o = (
    dep
    .with_columns(pl.col("CURBAL").cast(pl.Float64))
    .group_by(["BRANCH", "AMTIND", "PRODUCT"])
    .agg([
        pl.col("CURBAL").sum().alias("CURBAL"),
        pl.len().alias("_FREQ_"),
    ])
)

# =============================================================================
# STEP 7: Merge BRHFILE + P619O (keep P619O rows)
# =============================================================================
p619 = (
    p619o
    .join(brhfile, on="BRANCH", how="left")
    .with_columns([
        pl.lit("ISLAMIC     ").alias("CATEGORY"),
        (pl.col("_FREQ_")).alias("ACCTS"),
    ])
    .with_columns(
        pl.when(pl.col("AMTIND") == "D")
          .then(pl.lit("CONVENTIONAL"))
          .otherwise(pl.col("CATEGORY"))
          .alias("CATEGORY")
    )
    .with_columns(
        (pl.col("BRANCH").cast(pl.Utf8).str.zfill(3) + pl.lit("-") + pl.col("BRCHCD").fill_null("")).alias("BRCX")
    )
    .sort(["BRCX", "CATEGORY", "PRODUCT"])
)

# =============================================================================
# STEP 8: Produce PROC REPORT-style output
# =============================================================================
rpt = ReportWriter(OUTPUT_FILE, PAGE_LENGTH)

# Titles
rpt.put('1', f"{sdesc}  REPORT ID: EIBM619D")
rpt.put(' ', f"NUMBER OF DEPOSIT ACCOUNTS BY PRODUCT @ {rdate}")
rpt.put(' ', "")

# Column headers (HEADSKIP HEADLINE)
hdr = f"{'BR-CODE':<8}  {'CATEGORY':<12}  {'PRODUCT':>7}  {'NUMBER OF ACCTS':>12}"
rpt.put(' ', hdr)
rpt.put(' ', "-" * len(hdr))

current_brcx     = None
current_category = None
cat_accts_sum    = 0
brcx_accts_sum   = 0

def flush_category(rpt, category, accts_sum):
    rpt.put(' ', " " * 22 + "-" * 35)
    rpt.put(' ', " " * 22 + f"{'SUB-TOTAL ':<10}{fmt_comma16(accts_sum)}")

def flush_brcx(rpt, brcx, accts_sum):
    rpt.put(' ', " " * 22 + f"{'TOTAL BY BRANCH ':<16}{fmt_comma16(accts_sum)}")
    rpt.put(' ', " " * 22 + "-" * 35)

for row in p619.iter_rows(named=True):
    brcx     = row["BRCX"]     or ""
    category = row["CATEGORY"] or ""
    product  = row["PRODUCT"]  or 0
    accts    = int(row["ACCTS"] or 0)

    # Category break
    if current_category is not None and (category != current_category or brcx != current_brcx):
        flush_category(rpt, current_category, cat_accts_sum)
        cat_accts_sum = 0

    # BRCX break
    if current_brcx is not None and brcx != current_brcx:
        flush_brcx(rpt, current_brcx, brcx_accts_sum)
        brcx_accts_sum = 0

    current_brcx     = brcx
    current_category = category
    cat_accts_sum   += accts
    brcx_accts_sum  += accts

    line = f"{brcx:<8}  {category:<12}  {product:>7}  {fmt_comma16(accts)}"
    rpt.put(' ', line)

# Final breaks
if current_category is not None:
    flush_category(rpt, current_category, cat_accts_sum)
if current_brcx is not None:
    flush_brcx(rpt, current_brcx, brcx_accts_sum)

rpt.write()
con.close()
print(f"Report written to: {OUTPUT_FILE}")
