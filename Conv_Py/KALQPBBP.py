#!/usr/bin/env python3
"""
Program : KALQPBBP
Date    : 11/12/96
Purpose : RDAL PART III (KAPITI ITEMS)
          Deposit from Non-Resident (Remaining Maturity)
          Appends results to BNM.KALQ{REPTMON}{NOWK} dataset

%INC PGM(KALMPBBF);
Placeholder for KALMPBBF dependency (format definitions including KREMMTH format)
KALMPBBF defines custom formats used throughout this program
"""

import duckdb
import polars as pl
from datetime import datetime, date
import os

import KALMPBBF

# ─────────────────────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────────────────────
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR        = os.path.join(BASE_DIR, "input")
OUTPUT_DIR       = os.path.join(BASE_DIR, "output")

# Macro variables (equivalent to &REPTMON, &NOWK, &RDATE, &AMTIND)
REPTMON  = "202412"   # e.g. YYYYMM
NOWK     = "01"       # week number or run identifier
RDATE    = "31122024" # DDMMYYYY
AMTIND   = "I"

K1TBL_PARQUET  = os.path.join(INPUT_DIR, f"K1TBL{REPTMON}{NOWK}.parquet")
# BNM.KALQ output (appended dataset stored as parquet)
KALQ_PARQUET   = os.path.join(OUTPUT_DIR, f"KALQ{REPTMON}{NOWK}.parquet")
OUTPUT_PRINT   = os.path.join(OUTPUT_DIR, f"X_KALQPBBP_{REPTMON}{NOWK}.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# CONSTANTS / HELPERS
# ─────────────────────────────────────────────────────────────
PAGE_LENGTH = 60  # lines per page (ASA carriage control)


def month_days(year: int) -> list:
    """Return list of days per month (1-indexed, index 0 unused)."""
    leap = (year % 4 == 0)
    return [0, 31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def compute_remmth(nddate: date, rpyr: int, rpmth: int, rpday: int) -> float:
    """
    Equivalent to %REMMTH macro (uses ND-prefixed variables for this program).
    Compute remaining months from report date to nddate.
    """
    rpdays = month_days(rpyr)
    nddays = month_days(nddate.year)

    ndyr  = nddate.year
    ndmth = nddate.month
    ndday = nddate.day

    # If nd day equals end of nd month, set to rpday
    if ndday == nddays[ndmth]:
        ndday = rpday

    remy = ndyr  - rpyr
    remm = ndmth - rpmth
    remd = ndday - rpday
    remmth = remy * 12 + remm + remd / rpdays[rpmth]
    return remmth


def kremmth_format(remmth: float) -> str:
    """
    Equivalent to KREMMTH. format (defined in KALMPBBF).
    Maps remaining months to a 2-character code string.
    Standard ALM remaining maturity bands:
      < 0       -> '51'  (demand / on-call)
       0  -  1  -> '01'
       1  -  3  -> '03'
       3  -  6  -> '06'
       6  - 12  -> '12'
      12  - 24  -> '24'
      24  - 36  -> '36'
      36  - 60  -> '60'
      60  - 84  -> '84'
      84  +     -> '99'
    """
    if remmth < 0:
        return '51'
    elif remmth <= 1:
        return '01'
    elif remmth <= 3:
        return '03'
    elif remmth <= 6:
        return '06'
    elif remmth <= 12:
        return '12'
    elif remmth <= 24:
        return '24'
    elif remmth <= 36:
        return '36'
    elif remmth <= 60:
        return '60'
    elif remmth <= 84:
        return '84'
    else:
        return '99'


# ─────────────────────────────────────────────────────────────
# PARSE REPORT DATE  (DDMMYYYY)
# ─────────────────────────────────────────────────────────────
reptdate = datetime.strptime(RDATE, "%d%m%Y").date()
rpyr  = reptdate.year
rpmth = reptdate.month
rpday = reptdate.day

# ─────────────────────────────────────────────────────────────
# PROC DATASETS: DELETE BNM.KALQ{REPTMON}{NOWK}
# (i.e. remove the output parquet if it already exists)
# ─────────────────────────────────────────────────────────────
if os.path.exists(KALQ_PARQUET):
    os.remove(KALQ_PARQUET)

# ─────────────────────────────────────────────────────────────
# READ INPUT  K1TBL{REPTMON}{NOWK}
# Rename GWBALC -> AMOUNT
# ─────────────────────────────────────────────────────────────
con = duckdb.connect()
df_raw = con.execute(
    f"SELECT *, GWBALC AS AMOUNT FROM read_parquet('{K1TBL_PARQUET}')"
).pl()
con.close()

# ─────────────────────────────────────────────────────────────
# DATA K1TABL  (equivalent to DATA step with conditional logic)
# KEEP=BNMCODE AMOUNT AMTIND
# ─────────────────────────────────────────────────────────────
# %DCLVAR:
#   RETAIN RD1-RD12 ND1-ND12 31 RD2 ND2 28 RD4 RD6 RD9 RD11
#          ND4 ND6 ND9 ND11 30 RPYR RPMTH RPDAY;
#   ARRAY RPDAYS RD1-RD12;
#   ARRAY NDDAYS ND1-ND12;

records = []

for row in df_raw.iter_rows(named=True):
    gwmvt  = str(row.get("GWMVT",  "") or "")
    gwmvts = str(row.get("GWMVTS", "") or "")

    if not (gwmvt == 'P' and gwmvts == 'M'):
        continue

    gwdlp  = str(row.get("GWDLP",  "") or "")
    gwnanc = str(row.get("GWNANC", "") or "")
    gwact  = str(row.get("GWACT",  "") or "")
    gwsac  = str(row.get("GWSAC",  "") or "")
    gwan   = str(row.get("GWAN",   "") or "")
    gwcnap = str(row.get("GWCNAP", "") or "")
    gwcnal = str(row.get("GWCNAL", "") or "")
    gwctp  = str(row.get("GWCTP",  "") or "")
    gwmdt  = row.get("GWMDT")
    amount = row.get("AMOUNT", 0) or 0

    # Determine BIC
    bic = ""
    if gwdlp in ('FDA', 'FDB', 'FDL', 'FDS'):
        bic = '42000'
    elif gwdlp in ('BF', 'BO'):
        bic = '48000'

    if bic == '':
        continue

    # Compute remaining maturity
    nddate_val = gwmdt
    if nddate_val is None:
        continue
    try:
        if isinstance(nddate_val, (int, float)):
            nddate = datetime.strptime(str(int(nddate_val)).zfill(8), "%Y%m%d").date()
        elif isinstance(nddate_val, str):
            nddate = datetime.strptime(nddate_val[:10], "%Y-%m-%d").date()
        else:
            nddate = nddate_val  # already a date object
    except (ValueError, TypeError):
        continue

    remmth = compute_remmth(nddate, rpyr, rpmth, rpday)

    # Determine RM code
    if gwnanc == '00001' and gwact == 'CV':
        rm = '51'
    else:
        rm = kremmth_format(remmth)

    # Condition 1: GWSAC='UO' AND GWACT='CV' AND SUBSTR(GWAN,1,1)='0'
    #              AND (GWCNAP='MY' OR GWAN='000418')
    if (gwsac == 'UO' and gwact == 'CV'
            and len(gwan) >= 1 and gwan[0] == '0'
            and (gwcnap == 'MY' or gwan == '000418')):
        bnmcode = (bic + '82' + rm + '0000Y')[:14]
        records.append({"BNMCODE": bnmcode, "AMOUNT": amount, "AMTIND": AMTIND})

    # Condition 2: NOT('BA' <= GWCTP <= 'BZ') AND GWCNAL^='MY' AND GWSAC='UF'
    elif (not ('BA' <= gwctp <= 'BZ')
          and gwcnal != 'MY'
          and gwsac == 'UF'):
        bnmcode = (bic + '85' + rm + '0000Y')[:14]
        records.append({"BNMCODE": bnmcode, "AMOUNT": amount, "AMTIND": AMTIND})

    # Remaining IF BNMCODE=' ' block (neither of above conditions matched)
    else:
        # IF GWCNAL^='MY'
        if gwcnal != 'MY':
            if gwcnap in ('CA', 'FR', 'IT', 'DE', 'JP', 'US', 'GB'):
                bnmcode = (bic + '83' + rm + '0000Y')[:14]
                records.append({"BNMCODE": bnmcode, "AMOUNT": amount, "AMTIND": AMTIND})
            else:
                bnmcode = (bic + '84' + rm + '0000Y')[:14]
                records.append({"BNMCODE": bnmcode, "AMOUNT": amount, "AMTIND": AMTIND})

k1tabl = pl.DataFrame(
    records,
    schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTIND": pl.Utf8},
)

# ─────────────────────────────────────────────────────────────
# PROC SUMMARY DATA=K1TABL NWAY
# CLASS BNMCODE AMTIND; VAR AMOUNT; OUTPUT OUT=K1TABL SUM=AMOUNT;
# ─────────────────────────────────────────────────────────────
if not k1tabl.is_empty():
    k1tabl = (
        k1tabl.group_by(["BNMCODE", "AMTIND"])
        .agg(pl.col("AMOUNT").sum())
    )

# PROC PRINT (inline print after PROC SUMMARY)
def format_proc_print(df: pl.DataFrame) -> str:
    lines = []
    lines.append("1")  # ASA new page
    hdr = f" {'OBS':<6} {'BNMCODE':<14}  {'AMTIND':<6}  {'AMOUNT':>20}"
    lines.append(f" {hdr}")
    lines.append(f" {'-' * len(hdr)}")
    for i, row in enumerate(df.iter_rows(named=True), 1):
        lines.append(
            f"  {i:<6} {row['BNMCODE']:<14}  {row['AMTIND']:<6}  {row['AMOUNT']:>20,.2f}"
        )
    return "\n".join(lines)

print_output = format_proc_print(k1tabl)

# ─────────────────────────────────────────────────────────────
# PROC APPEND DATA=K1TABL BASE=BNM.KALQ{REPTMON}{NOWK}
# ─────────────────────────────────────────────────────────────
if os.path.exists(KALQ_PARQUET):
    con = duckdb.connect()
    existing = con.execute(f"SELECT * FROM read_parquet('{KALQ_PARQUET}')").pl()
    con.close()
    combined = pl.concat([existing, k1tabl], how="diagonal")
else:
    combined = k1tabl

combined.write_parquet(KALQ_PARQUET)

# PROC DATASETS LIB=WORK NOLIST; DELETE K1TABL; (handled by scope / GC)

# ─────────────────────────────────────────────────────────────
# WRITE PRINT OUTPUT
# ─────────────────────────────────────────────────────────────
with open(OUTPUT_PRINT, "w", encoding="utf-8") as f:
    f.write(print_output)
    f.write("\n")

print(f"Output report written to : {OUTPUT_PRINT}")
print(f"Appended dataset written : {KALQ_PARQUET}")
