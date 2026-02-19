# !/usr/bin/env python3
"""
Program : EIBMDPCA
Purpose : Current Account Opened/Closed for the Month Report
          Reads current account data, applies product/branch filters,
            computes cumulative opened/closed and net change year-to-date,
            saves MIS datasets, and produces a PROC TABULATE-style report.
"""

import duckdb
import polars as pl
import os
from datetime import date

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR    = r"/data"
DEPOSIT_DIR = os.path.join(BASE_DIR, "deposit")   # DEPOSIT.REPTDATE, DEPOSIT.CURRENT
MIS_DIR     = os.path.join(BASE_DIR, "mis")        # MIS.CAO<mm>, MIS.CURR1<mm>
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")

OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EIBMDPCA.txt")

os.makedirs(MIS_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH = 60

# Product filter sets
PRODUCTS_INCLUDE_EXACT = {85, 86, 90, 91, 106, 170, 135, 137, 138, 68, 69}
def product_in_range(p: int) -> bool:
    return (
        p in PRODUCTS_INCLUDE_EXACT or
        50  <= p <= 65  or
        100 <= p <= 103 or
        108 <= p <= 125 or
        150 <= p <= 167 or
        174 <= p <= 182 or
        191 <= p <= 198
    )

# =============================================================================
# HELPERS
# =============================================================================
def fmt_comma10(v) -> str:
    if v is None:
        return f"{'0':>10}"
    return f"{float(v):>10,.0f}"

def fmt_comma17_2(v) -> str:
    if v is None:
        return f"{'0.00':>17}"
    return f"{float(v):>17,.2f}"

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
reptdate_path = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")
reptdate_row  = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
reptdate = reptdate_row[0]
if isinstance(reptdate, (int, float)):
    import datetime as _dt
    reptdate = date(1960, 1, 1) + _dt.timedelta(days=int(reptdate))
reptdate_dt = reptdate if isinstance(reptdate, date) else reptdate.date()

mm       = reptdate_dt.month
mm1      = mm - 1 if mm > 1 else 12
rdate    = reptdate_dt.strftime("%d/%m/%y")
ryear    = str(reptdate_dt.year)
rmonth   = f"{mm:02d}"
reptmon  = f"{mm:02d}"
reptmon1 = f"{mm1:02d}"
rday     = reptdate_dt.day

# =============================================================================
# STEP 2: Load CURRENT data, apply transformations
# =============================================================================
current_path = os.path.join(DEPOSIT_DIR, "CURRENT.parquet")
curr_raw = con.execute(f"SELECT * FROM read_parquet('{current_path}')").pl()

# IF BRANCH=250 THEN BRANCH=092
# /* IF BRANCH=996 THEN BRANCH=168; */  (commented out in original)
curr_raw = curr_raw.with_columns(
    pl.when(pl.col("BRANCH") == 250).then(92).otherwise(pl.col("BRANCH")).alias("BRANCH")
)

# Product filter
curr_raw = curr_raw.filter(
    pl.col("PRODUCT").map_elements(product_in_range, return_dtype=pl.Boolean)
)

# IF OPENMH=1 AND CLOSEMH=1 THEN DELETE
curr_raw = curr_raw.filter(~((pl.col("OPENMH") == 1) & (pl.col("CLOSEMH") == 1)))

# IF OPENIND='Z' THEN OPENIND='O'
curr_raw = curr_raw.with_columns(
    pl.when(pl.col("OPENIND") == "Z").then(pl.lit("O")).otherwise(pl.col("OPENIND")).alias("OPENIND")
)

# Keep rows where OPENIND='O' OR (OPENIND IN ('B','C','P') AND CLOSEMH=1)
curr_raw = curr_raw.filter(
    (pl.col("OPENIND") == "O") |
    (pl.col("OPENIND").is_in(["B", "C", "P"]) & (pl.col("CLOSEMH") == 1))
)

# Compute NOACCT, CCLOSE, BCLOSE
curr_raw = curr_raw.with_columns([
    pl.lit(0).alias("NOACCT"),
    pl.lit(0).alias("CCLOSE"),
    pl.lit(0).alias("BCLOSE"),
])

curr_raw = curr_raw.with_columns([
    pl.when(pl.col("OPENIND").is_in(["B", "C", "P"]))
      .then(pl.when(pl.col("OPENIND") == "C").then(pl.col("CLOSEMH")).otherwise(0))
      .otherwise(0)
      .alias("CCLOSE"),
    pl.when(pl.col("OPENIND").is_in(["B", "C", "P"]))
      .then(pl.when(pl.col("OPENIND") != "C").then(pl.col("CLOSEMH")).otherwise(0))
      .otherwise(0)
      .alias("BCLOSE"),
    pl.when(~pl.col("OPENIND").is_in(["B", "C", "P"]))
      .then(pl.lit(1))
      .otherwise(pl.lit(0))
      .alias("NOACCT"),
    pl.when(~pl.col("OPENIND").is_in(["B", "C", "P"]))
      .then(pl.lit(0))
      .otherwise(pl.col("CLOSEMH"))
      .alias("CLOSEMH"),
])

# =============================================================================
# STEP 3: CAO -- OPENMH=1 & CURBAL > 0; PROC SUMMARY by BRANCH
# =============================================================================
cao = curr_raw.filter((pl.col("OPENMH") == 1) & (pl.col("CURBAL") > 0))
cao_summary = (
    cao
    .with_columns([pl.col(c).cast(pl.Float64) for c in ["OPENMH", "CURBAL"]])
    .group_by("BRANCH")
    .agg([pl.col("OPENMH").sum(), pl.col("CURBAL").sum()])
)
cao_output_path = os.path.join(MIS_DIR, f"CAO{reptmon}.parquet")
cao_summary.write_parquet(cao_output_path)

# =============================================================================
# STEP 4: PROC SUMMARY CURR by BRANCH, PRODUCT
# =============================================================================
sum_cols = ["OPENMH", "CLOSEMH", "NOACCT", "CURBAL", "BCLOSE", "CCLOSE"]
curr_summary = (
    curr_raw
    .with_columns([pl.col(c).cast(pl.Float64) for c in sum_cols])
    .group_by(["BRANCH", "PRODUCT"])
    .agg([pl.col(c).sum() for c in sum_cols])
)

# =============================================================================
# STEP 5: MACRO PROCESS -- cumulative opened/closed
# =============================================================================
if mm > 1:
    # %IF &REPTMON > 01
    prev_curr_path = os.path.join(MIS_DIR, f"CURR1{reptmon1}.parquet")
    currp_raw = con.execute(f"SELECT * FROM read_parquet('{prev_curr_path}')").pl()
    currp = currp_raw.rename({
        "OPENCUM":  "OPENCUX",
        "CLOSECUM": "CLOSECUX",
    })
    currp = currp.with_columns(
        pl.when(pl.col("BRANCH") == 250).then(92).otherwise(pl.col("BRANCH")).alias("BRANCH")
    )
    currp_sum = (
        currp
        .with_columns([pl.col(c).cast(pl.Float64) for c in ["OPENCUX", "CLOSECUX"]
                       if c in currp.columns])
        .group_by(["BRANCH", "PRODUCT"])
        .agg([pl.col(c).sum() for c in ["OPENCUX", "CLOSECUX"] if c in currp.columns])
    )
    curr_merged = curr_summary.join(currp_sum, on=["BRANCH", "PRODUCT"], how="left")
    null_fill   = {c: 0.0 for c in ["CLOSECUX", "OPENCUX", "OPENMH", "CLOSEMH",
                                     "CCLOSE", "BCLOSE", "NOACCT", "CURBAL"]}
    curr_merged = curr_merged.with_columns([
        pl.col(c).fill_null(0.0) for c in null_fill
    ])
    curr_merged = curr_merged.with_columns([
        (pl.col("OPENMH")  + pl.col("OPENCUX")).alias("OPENCUM"),
        (pl.col("CLOSEMH") + pl.col("CLOSECUX")).alias("CLOSECUM"),
        (pl.col("OPENMH")  - pl.col("CLOSEMH")).alias("NETCHGMH"),
    ])
    curr_merged = curr_merged.with_columns(
        (pl.col("OPENCUM") - pl.col("CLOSECUM")).alias("NETCHGYR")
    )
else:
    # %ELSE
    curr_merged = curr_summary.with_columns(
        pl.when(pl.col("BRANCH") == 250).then(92).otherwise(pl.col("BRANCH")).alias("BRANCH")
    )
    curr_merged = curr_merged.with_columns([
        pl.col("OPENMH").alias("OPENCUM"),
        pl.col("CLOSEMH").alias("CLOSECUM"),
        (pl.col("OPENMH") - pl.col("CLOSEMH")).alias("NETCHGMH"),
    ])
    curr_merged = curr_merged.with_columns(
        (pl.col("OPENCUM") - pl.col("CLOSECUM")).alias("NETCHGYR")
    )

# =============================================================================
# STEP 6: Save MIS.CURR1<reptmon>
# =============================================================================
curr1_path = os.path.join(MIS_DIR, f"CURR1{reptmon}.parquet")
curr_merged.write_parquet(curr1_path)

# =============================================================================
# STEP 7: PROC TABULATE -- aggregate by BRANCH for report
# =============================================================================
report_cols = ["OPENMH", "CLOSEMH", "NOACCT", "CURBAL", "CLOSECUM", "OPENCUM",
               "NETCHGMH", "NETCHGYR", "BCLOSE", "CCLOSE"]
existing_rc = [c for c in report_cols if c in curr_merged.columns]

branch_summary = (
    curr_merged
    .with_columns([pl.col(c).cast(pl.Float64).fill_null(0.0) for c in existing_rc])
    .group_by("BRANCH")
    .agg([pl.col(c).sum() for c in existing_rc])
    .sort("BRANCH")
)

totals = branch_summary.select([pl.col(c).sum().alias(c) for c in existing_rc])

# =============================================================================
# STEP 8: Write report
# =============================================================================
rpt = ReportWriter(OUTPUT_FILE, PAGE_LENGTH)

rpt.put('1', 'PUBLIC BANK BERHAD')
rpt.put(' ', f"CURRENT ACCOUNT OPENED/CLOSED FOR THE MONTH AS AT {rdate}")
rpt.put(' ', "")

# Header row 1
hdr = (
    f"{'BRANCH':<10}"
    f"{'CURRENT MONTH OPENED':>10}"
    f"{'CUMULATIVE OPENED':>10}"
    f"{'CURRENT MONTH CLOSED':>10}"
    f"{'CLOSED BY BANK':>10}"
    f"{'CLOSED BY CUSTOMER':>10}"
    f"{'CUMULATIVE CLOSED':>10}"
    f"{'NO. OF ACCTS':>10}"
    f"{'TOTAL(RM) O/S':>17}"
    f"{'NET CHANGE FOR THE MONTH':>10}"
    f"{'NET CHANGE YEAR TO DATE':>10}"
)
rpt.put(' ', hdr)
rpt.put(' ', "-" * len(hdr))

def get_val(row, col):
    return row.get(col) or 0.0

for row in branch_summary.iter_rows(named=True):
    branch = row.get("BRANCH") or 0
    line = (
        f"{branch:<10}"
        f"{fmt_comma10(get_val(row,'OPENMH'))}"
        f"{fmt_comma10(get_val(row,'OPENCUM'))}"
        f"{fmt_comma10(get_val(row,'CLOSEMH'))}"
        f"{fmt_comma10(get_val(row,'BCLOSE'))}"
        f"{fmt_comma10(get_val(row,'CCLOSE'))}"
        f"{fmt_comma10(get_val(row,'CLOSECUM'))}"
        f"{fmt_comma10(get_val(row,'NOACCT'))}"
        f"{fmt_comma17_2(get_val(row,'CURBAL'))}"
        f"{fmt_comma10(get_val(row,'NETCHGMH'))}"
        f"{fmt_comma10(get_val(row,'NETCHGYR'))}"
    )
    rpt.put(' ', line)

# TOTAL row
rpt.put(' ', "-" * len(hdr))
tot = totals.row(0, named=True)
total_line = (
    f"{'TOTAL':<10}"
    f"{fmt_comma10(get_val(tot,'OPENMH'))}"
    f"{fmt_comma10(get_val(tot,'OPENCUM'))}"
    f"{fmt_comma10(get_val(tot,'CLOSEMH'))}"
    f"{fmt_comma10(get_val(tot,'BCLOSE'))}"
    f"{fmt_comma10(get_val(tot,'CCLOSE'))}"
    f"{fmt_comma10(get_val(tot,'CLOSECUM'))}"
    f"{fmt_comma10(get_val(tot,'NOACCT'))}"
    f"{fmt_comma17_2(get_val(tot,'CURBAL'))}"
    f"{fmt_comma10(get_val(tot,'NETCHGMH'))}"
    f"{fmt_comma10(get_val(tot,'NETCHGYR'))}"
)
rpt.put(' ', total_line)

rpt.write()
con.close()
print(f"Report written to: {OUTPUT_FILE}")
