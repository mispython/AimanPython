# !/usr/bin/env python3
"""
PROGRAM : EIBMLN01.py
DATE    : 07.04.99
REPORT  : UNDRAWN TERM LOAN/OVERDRAFT FOR BRANCHES UPDATE
NOTE    : EXCLUDE - VOSTRO 104,105 ('33110')
          N/A - NORMAL HL SOLD TO CAGAMAS 225,226 ('54120')
          OTHER LOANS - RC 350,910,925 ('34190')
                      - STAFF LOANS 4-33 ('34230')
"""

import duckdb
import polars as pl
from datetime import date
import math
import os

# ─── PATH CONFIGURATION ───────────────────────────────────────────────────────
BASE_DIR       = r"C:/data"
PARQUET_DIR    = os.path.join(BASE_DIR, "parquet")
OUTPUT_DIR     = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

REPTDATE_FILE  = os.path.join(PARQUET_DIR, "reptdate.parquet")
SDESC_FILE     = os.path.join(PARQUET_DIR, "sdesc.parquet")
LNCOMM_FILE    = os.path.join(PARQUET_DIR, "lncomm.parquet")
OUTPUT_FILE    = os.path.join(OUTPUT_DIR,  "EIBMLN01.txt")

PAGE_LENGTH = 60  # lines per page (ASA default)

# ─── MACRO VARIABLES FROM REPTDATE ────────────────────────────────────────────
con = duckdb.connect()

reptdate_row = con.execute(f"SELECT reptdate FROM '{REPTDATE_FILE}' LIMIT 1").fetchone()
reptdate: date = reptdate_row[0]

day = reptdate.day
if   day == 8:  NOWK = "1"
elif day == 15: NOWK = "2"
elif day == 22: NOWK = "3"
else:           NOWK = "4"

REPTMON = f"{reptdate.month:02d}"

# WORDDATX18 equivalent: e.g. "07 April 1999"
month_names = ["JANUARY","FEBRUARY","MARCH","APRIL","MAY","JUNE",
               "JULY","AUGUST","SEPTEMBER","OCTOBER","NOVEMBER","DECEMBER"]
RDATE = f"{reptdate.day:02d} {month_names[reptdate.month-1]} {reptdate.year}"

# SDESC
sdesc_row = con.execute(f"SELECT sdesc FROM '{SDESC_FILE}' LIMIT 1").fetchone()
SDESC = f"{sdesc_row[0]:<26}" if sdesc_row else " " * 26

# ─── DYNAMIC FILE PATHS ───────────────────────────────────────────────────────
LOAN_FILE  = os.path.join(PARQUET_DIR, f"loan{REPTMON}{NOWK}.parquet")
ULOAN_FILE = os.path.join(PARQUET_DIR, f"uloan{REPTMON}{NOWK}.parquet")

# ─── LOAD DATA ────────────────────────────────────────────────────────────────
loan_df  = con.execute(f"SELECT * FROM '{LOAN_FILE}' ORDER BY acctno, noteno").pl()
lncomm_df = con.execute(f"SELECT * FROM '{LNCOMM_FILE}' ORDER BY acctno, commno").pl()

# ─── MANIPULATION FOR RC ──────────────────────────────────────────────────────

# ALWCOM: records with commno > 0
alwcom = loan_df.filter(pl.col("commno") > 0).sort(["acctno", "commno"])

# Merge ALWCOM with LNCOMM (left join)
appr = alwcom.join(lncomm_df, on=["acctno", "commno"], how="left", suffix="_lncomm")

# Keep first per acctno/commno for prodcd='34190', else keep all A records
def filter_appr(df: pl.DataFrame) -> pl.DataFrame:
    results = []
    for (acctno, commno), grp in df.group_by(["acctno", "commno"], maintain_order=True):
        grp = grp.with_row_index("_row")
        if grp["prodcd"][0] == "34190":
            results.append(grp.head(1).drop("_row"))
        else:
            results.append(grp.drop("_row"))
    return pl.concat(results) if results else df.clear()

appr = filter_appr(appr)

# ALWNOCOM: commno <= 0
alwnocom = loan_df.filter(pl.col("commno") <= 0).sort(["acctno", "apprlim2"])

# Split into APPR1 and DUP for prodcd='34190'
appr1_rows = []
dup_rows   = []

for (acctno,), grp in alwnocom.group_by(["acctno"], maintain_order=True):
    seen_apprlim2 = set()
    for row in grp.iter_rows(named=True):
        if row["prodcd"] == "34190":
            key = (acctno, row["apprlim2"])
            if key not in seen_apprlim2:
                seen_apprlim2.add(key)
                appr1_rows.append(row)
            else:
                dup_rows.append({**row, "dupli": 1})
        # non-34190 rows not output here (handled below via APPR1 merge)

appr1_34190 = pl.DataFrame(appr1_rows) if appr1_rows else alwnocom.clear()
dup_df      = pl.DataFrame(dup_rows)   if dup_rows   else alwnocom.clear().with_columns(pl.lit(None).cast(pl.Int8).alias("dupli"))

# DUPLI: dup where balance >= apprlim2
dupli = (dup_df.filter(pl.col("balance") >= pl.col("apprlim2"))
               .sort(["acctno", "apprlim2"])
               .select(["acctno", "apprlim2", "dupli"]))

# Rebuild APPR1 by merging ALWNOCOM with DUPLI
appr1_merged = alwnocom.join(dupli, on=["acctno", "apprlim2"], how="left", suffix="_dup")

appr1_final_rows = []
for row in appr1_merged.iter_rows(named=True):
    if row["prodcd"] == "34190":
        if row.get("dupli") == 1:
            if row["balance"] >= row["apprlim2"]:
                appr1_final_rows.append(row)
        else:
            # first acctno or first apprlim2 — already handled by left join uniqueness
            # We need to deduplicate first-per (acctno, apprlim2) for non-dupli rows
            appr1_final_rows.append(row)
    else:
        appr1_final_rows.append(row)

appr1_final = pl.DataFrame(appr1_final_rows) if appr1_final_rows else alwnocom.clear()

# Deduplicate non-dupli 34190 rows to first per (acctno, apprlim2)
mask_34190_nondupli = (appr1_final["prodcd"] == "34190") & (appr1_final.get_column("dupli").is_null() if "dupli" in appr1_final.columns else pl.Series([True]*len(appr1_final)))
# simpler: re-filter
non_34190 = appr1_final.filter(pl.col("prodcd") != "34190")
yes_34190 = appr1_final.filter(pl.col("prodcd") == "34190")
yes_34190_dedup = yes_34190.unique(subset=["acctno", "apprlim2"], keep="first", maintain_order=True)
appr1_final = pl.concat([non_34190, yes_34190_dedup])

# Combine APPR and APPR1
loan_combined = pl.concat([appr, appr1_final], how="diagonal").sort("acctno")

# Load ULOAN
uloan_df = con.execute(f"SELECT * FROM '{ULOAN_FILE}' ORDER BY acctno").pl()

# ─── BUILD LOAN DATASET ───────────────────────────────────────────────────────
combined = pl.concat([loan_combined, uloan_df], how="diagonal").sort("acctno")

# Determine REMMTH, UOD, UTL, UOL
def build_loan(df: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for row in df.iter_rows(named=True):
        prodcd  = str(row.get("prodcd") or "")
        acctype = str(row.get("acctype") or "")
        origmt  = str(row.get("origmt") or "")
        undrawn = row.get("undrawn") or 0.0

        # Filter: prodcd starts with '34' OR prodcd == '54120'
        if not (prodcd[:2] == "34" or prodcd == "54120"):
            continue

        if acctype == "OD":
            remmth = 1
        else:
            # * IF EXPRDATE - REPTDATE < 366 THEN REMMTH = 1;
            if origmt < "20":
                remmth = 1
            else:
                remmth = 13

        uod = uol = utl = None
        if acctype == "OD":
            uod = undrawn
        elif prodcd in ("34190", "34230", "54120"):
            uol = undrawn
        else:
            utl = undrawn

        rows.append({
            "branch": row.get("branch"),
            "remmth": remmth,
            "utl":    utl,
            "uod":    uod,
            "uol":    uol,
        })
    return pl.DataFrame(rows)

loan = build_loan(combined)

# ─── SUMMARY ──────────────────────────────────────────────────────────────────
summary = (
    loan.group_by(["remmth", "branch"])
        .agg([
            pl.col("utl").sum().alias("utl"),
            pl.col("uod").sum().alias("uod"),
            pl.col("uol").sum().alias("uol"),
        ])
        .sort(["remmth", "branch"])
)

# Summary total by branch
summary_branch = (
    loan.group_by(["branch"])
        .agg([
            pl.col("utl").sum().alias("utl"),
            pl.col("uod").sum().alias("uod"),
            pl.col("uol").sum().alias("uol"),
        ])
        .sort("branch")
)

# ─── REPORT HELPERS ───────────────────────────────────────────────────────────
def fmt_num(v) -> str:
    """Format number as COMMA17.2 (17 wide including sign, 2 decimal)"""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        v = 0.0
    return f"{v:>17,.2f}"

def print_table(rows: list[dict], title3: str, lines: list[str], line_no: list[int]):
    """Append a PROC PRINT style table to lines list with ASA carriage control."""
    PAGE_LEN = PAGE_LENGTH

    def emit(asa: str, text: str):
        lines.append(f"{asa}{text}")
        if asa == "1":
            line_no[0] = 1
        else:
            line_no[0] += 1

    def need_new_page():
        return line_no[0] >= PAGE_LEN

    # Header block
    def print_headers():
        emit("1", f"{SDESC.strip()}")
        emit(" ", f"UNDRAWN TERM LOAN AND OVERDRAFT AS AT {RDATE}")
        emit(" ", title3)
        emit(" ", "")
        # Column header
        emit(" ", f"{'BRANCH':<10}{'UNDRAWN TERM LOAN':>17}{'UNDRAWN OVERDRAFT':>17}{'UNDRAWN LOAN OTHERS':>20}")
        emit(" ", "-" * 64)

    print_headers()

    utl_tot = uod_tot = uol_tot = 0.0

    for row in rows:
        if need_new_page():
            print_headers()
        branch = str(row.get("branch") or "")
        utl    = row.get("utl") or 0.0
        uod    = row.get("uod") or 0.0
        uol    = row.get("uol") or 0.0
        utl_tot += utl; uod_tot += uod; uol_tot += uol
        emit(" ", f"{branch:<10}{fmt_num(utl)}{fmt_num(uod)}{fmt_num(uol)}")

    # SUM line
    emit(" ", "-" * 64)
    emit(" ", f"{'':10}{fmt_num(utl_tot)}{fmt_num(uod_tot)}{fmt_num(uol_tot)}")
    emit(" ", "")

# ─── GENERATE REPORT ──────────────────────────────────────────────────────────
lines    = []
line_no  = [0]

# Section 1: REMMTH = 1  (Original Maturity of up to 1 year)
rows1 = summary.filter(pl.col("remmth") == 1).sort("branch").to_dicts()
print_table(rows1, "ORIGINAL MATURITY OF UP TO 1 YEAR", lines, line_no)

# Section 2: REMMTH != 1 (Original Maturity of over 1 year)
rows2 = summary.filter(pl.col("remmth") != 1).sort("branch").to_dicts()
print_table(rows2, "ORIGINAL MATURITY OF OVER 1 YEAR", lines, line_no)

# Section 3: Total by branch
rows3 = summary_branch.to_dicts()
print_table(rows3, "TOTAL BY BRANCH", lines, line_no)

# ─── WRITE OUTPUT ─────────────────────────────────────────────────────────────
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")

print(f"Report written to: {OUTPUT_FILE}")
