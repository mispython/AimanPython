# !/usr/bin/env python3
"""
PROGRAM : EIBMLN04.py
REPORT  : UNDRAWN TL/OD/OTHERS BY SECTOR
"""

import duckdb
import polars as pl
from datetime import date
import math
import os

# ─── PATH CONFIGURATION ───────────────────────────────────────────────────────
BASE_DIR      = r"C:/data"
PARQUET_DIR   = os.path.join(BASE_DIR, "parquet")
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

REPTDATE_FILE = os.path.join(PARQUET_DIR, "reptdate.parquet")
LNCOMM_FILE   = os.path.join(PARQUET_DIR, "lncomm.parquet")
OUTPUT_FILE   = os.path.join(OUTPUT_DIR,  "EIBMLN04.txt")

PAGE_LENGTH = 60  # lines per page (ASA default)

# ─── FORMAT TABLES ────────────────────────────────────────────────────────────
# VALUE $SECTA
def fmt_secta(code: str) -> str:
    """Map sector code to SECTA group."""
    if not code:
        return "OTHER"
    c = code.strip()
    if "0110" <= c <= "0139": return "0100"  # SECURITIES
    if "0210" <= c <= "0230": return "0200"  # TRANSPORT VEHICLES
    if ("0311" <= c <= "0316") or ("0321" <= c <= "0329"): return "0300"  # LANDED PROPERTY
    if c == "0410": return "0410"  # PERSONAL USES
    if c == "0420": return "0420"  # CREDIT CARD
    if c == "0430": return "0430"  # CONSUMER DURABLES
    if "1100" <= c <= "1400": return "1000"  # AGRI,HUNTG,FORESTRY,FISHG
    if "2100" <= c <= "2909": return "2000"  # MINING & QUARRYING
    if "3100" <= c <= "3909": return "3000"  # MANUFACTURING
    if "4100" <= c <= "4200": return "4000"  # ELEC, GAS & WATER SUPPLY
    if "5001" <= c <= "5009": return "5000"  # CONSTRUCTION
    if "6100" <= c <= "6320": return "6000"  # WHOLESALE & RETAIL TRADE, RESTAURANTS & HOTELS
    if "7100" <= c <= "7220": return "7000"  # TRANSPORT, STORAGE & COMM
    if "8100" <= c <= "8330": return "8000"  # FINANCING, INSURANCE, REAL ESTATE, BUSINESS SERVICES
    if "9100" <= c <= "9600": return "9000"  # COMMUNITY, SOCIAL & PERSONAL SERVICES
    if c == "9999": return "9999"  # SECTORS NEC
    return "OTHER"

# VALUE $SECTB
def fmt_sectb(code: str) -> str:
    """Map sector code to SECTB group."""
    if not code:
        return " "
    c = code.strip()
    if "0311" <= c <= "0316": return "0310"  # RESIDENTIAL PROPERTY
    if "0321" <= c <= "0329": return "0320"  # NON-RESIDENTIAL PROPERTY
    if c == "5003": return "5003"  # CONSTR - IND BLDG & FACTORY
    if c == "5004": return "5004"  # CONSTR - INFRASTRUCTURE
    if c == "5005": return "5005"  # CONSTR - COMMERCIAL COMPLEX
    if c == "5006": return "5006"  # CONSTR - RESIDENTIAL
    if "6110" <= c <= "6150": return "6100"  # WHOLESALE TRADE
    if "6210" <= c <= "6250": return "6200"  # RETAIL TRADE
    if "6310" <= c <= "6320": return "6300"  # RESTAURANTS & HOTELS
    if c == "8100": return "8100"  # FINANCIAL SERVICES
    if c == "8200": return "8200"  # INSURANCE
    if c == "8310": return "8310"  # REAL ESTATE
    if "8321" <= c <= "8329": return "8320"  # BUSINESS SERVICES
    if c == "8330": return "8330"  # EQUIPMENT RENTAL
    return " "

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

month_names = ["JANUARY","FEBRUARY","MARCH","APRIL","MAY","JUNE",
               "JULY","AUGUST","SEPTEMBER","OCTOBER","NOVEMBER","DECEMBER"]
RDATE = f"{reptdate.day:02d} {month_names[reptdate.month-1]} {reptdate.year}"

# ─── DYNAMIC FILE PATHS ───────────────────────────────────────────────────────
LOAN_FILE  = os.path.join(PARQUET_DIR, f"loan{REPTMON}{NOWK}.parquet")
ULOAN_FILE = os.path.join(PARQUET_DIR, f"uloan{REPTMON}{NOWK}.parquet")

# ─── LOAD DATA ────────────────────────────────────────────────────────────────
loan_df   = con.execute(f"SELECT * FROM '{LOAN_FILE}' ORDER BY acctno, noteno").pl()
lncomm_df = con.execute(f"SELECT * FROM '{LNCOMM_FILE}' ORDER BY acctno, commno").pl()

# ─── MANIPULATION FOR RC ──────────────────────────────────────────────────────

# ALWCOM: commno > 0
alwcom = loan_df.filter(pl.col("commno") > 0).sort(["acctno", "commno"])

# Merge with LNCOMM
appr = alwcom.join(lncomm_df, on=["acctno", "commno"], how="left", suffix="_lncomm")

def filter_appr(df: pl.DataFrame) -> pl.DataFrame:
    results = []
    for (acctno, commno), grp in df.group_by(["acctno", "commno"], maintain_order=True):
        if grp["prodcd"][0] == "34190":
            results.append(grp.head(1))
        else:
            results.append(grp)
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
            appr1_final_rows.append(row)
    else:
        appr1_final_rows.append(row)

appr1_final = pl.DataFrame(appr1_final_rows) if appr1_final_rows else alwnocom.clear()

# Deduplicate non-dupli 34190 rows
non_34190   = appr1_final.filter(pl.col("prodcd") != "34190")
yes_34190   = appr1_final.filter(pl.col("prodcd") == "34190")
yes_34190_d = yes_34190.unique(subset=["acctno", "apprlim2"], keep="first", maintain_order=True)
appr1_final = pl.concat([non_34190, yes_34190_d])

# Combine APPR and APPR1
loan_combined = pl.concat([appr, appr1_final], how="diagonal").sort("acctno")

# Load ULOAN
uloan_df = con.execute(f"SELECT * FROM '{ULOAN_FILE}' ORDER BY acctno").pl()

# ─── BUILD LOAN DATASET ───────────────────────────────────────────────────────
combined = pl.concat([loan_combined, uloan_df], how="diagonal").sort("acctno")

def build_loan(df: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for row in df.iter_rows(named=True):
        prodcd   = str(row.get("prodcd")  or "")
        acctype  = str(row.get("acctype") or "")
        origmt   = str(row.get("origmt")  or "")
        undrawn  = row.get("undrawn") or 0.0
        fisspurp = str(row.get("fisspurp") or "")

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

        # SECTCD = FISSPURP
        sectcd = fisspurp
        # * SECTCD = '0000'; OUTPUT;
        # * SECTCD = PUT(SECTORCD,$SECTA.); OUTPUT;
        # * SECTCD = PUT(SECTORCD,$SECTB.);

        if sectcd.strip() != "":
            rows.append({
                "sectcd":  sectcd,
                "utl":     utl,
                "uod":     uod,
                "uol":     uol,
            })
    return pl.DataFrame(rows)

loan = build_loan(combined)

# ─── SUMMARY BY SECTCD ────────────────────────────────────────────────────────
summary = (
    loan.group_by(["sectcd"])
        .agg([
            pl.col("utl").sum().alias("utl"),
            pl.col("uod").sum().alias("uod"),
            pl.col("uol").sum().alias("uol"),
        ])
        .sort("sectcd")
)

# Derive SECTOR = '56200' || SECTCD || 'Y'
summary = summary.with_columns(
    (pl.lit("56200") + pl.col("sectcd") + pl.lit("Y")).alias("sector")
)

# ─── REPORT HELPERS ───────────────────────────────────────────────────────────
def fmt_num(v) -> str:
    """Format as COMMA20.2 (20 wide, 2 decimal)"""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        v = 0.0
    return f"{v:>20,.2f}"

def print_report(rows: list[dict], lines: list[str]):
    line_no = [0]

    def emit(asa: str, text: str):
        lines.append(f"{asa}{text}")
        if asa == "1":
            line_no[0] = 1
        else:
            line_no[0] += 1

    def print_headers():
        emit("1", "REPORT ID: EIBMLN04")
        emit(" ", "PUBLIC BANK BERHAD")
        emit(" ", f"UNDRAWN TL/OD/OTHERS BY SECTOR AS AT {RDATE}")
        emit(" ", "")
        emit(" ", f"{'SECTOR':<11}{'UNDRAWN TERM LOAN':>20}{'UNDRAWN OVERDRAFT':>20}{'UNDRAWN LOAN OTHERS':>20}")
        emit(" ", "-" * 71)

    print_headers()

    utl_tot = uod_tot = uol_tot = 0.0

    for row in rows:
        if line_no[0] >= PAGE_LENGTH:
            print_headers()
        sector = str(row.get("sector") or "")
        utl    = row.get("utl") or 0.0
        uod    = row.get("uod") or 0.0
        uol    = row.get("uol") or 0.0
        utl_tot += utl; uod_tot += uod; uol_tot += uol
        emit(" ", f"{sector:<11}{fmt_num(utl)}{fmt_num(uod)}{fmt_num(uol)}")

    # SUM line
    emit(" ", "-" * 71)
    emit(" ", f"{'':11}{fmt_num(utl_tot)}{fmt_num(uod_tot)}{fmt_num(uol_tot)}")

# ─── GENERATE REPORT ──────────────────────────────────────────────────────────
lines = []
print_report(summary.to_dicts(), lines)

# ─── WRITE OUTPUT ─────────────────────────────────────────────────────────────
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")

print(f"Report written to: {OUTPUT_FILE}")
