# !/usr/bin/env python3
"""
Program : FALQPBBP
Purpose : RM FD ACCEPTED BY REMAINING MATURITY
          Generates reports on External Liabilities filtered by BNMCODE prefix

%INC PGM(KALMPBBF);
Placeholder for KALMPBBF dependency (format definitions including KREMMTH format)
KALMPBBF defines custom formats such as KREMMTH used to map remaining months to codes
"""

import duckdb
import polars as pl
from datetime import datetime, date
import math
import os

import KALMPBBF

# ─────────────────────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR       = os.path.join(BASE_DIR, "input")
OUTPUT_DIR      = os.path.join(BASE_DIR, "output")

FDMTHLY_PARQUET = os.path.join(INPUT_DIR, "FDMTHLY.parquet")
OUTPUT_FILE     = os.path.join(OUTPUT_DIR, "X_FALQPBBP.txt")

# Report date in DDMMYYYY format (equivalent to &RDATE macro variable)
RDATE = "31122024"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# CONSTANTS / HELPERS
# ─────────────────────────────────────────────────────────────
PAGE_LENGTH = 60  # lines per page (ASA carriage control)

# Days in each month (1-indexed, index 0 unused)
# RD1-RD12 retained: most months=31, RD2=28, RD4/RD6/RD9/RD11=30
def month_days(year: int) -> list:
    """Return list of days per month (1-indexed, index 0 unused)."""
    leap = (year % 4 == 0)
    return [0, 31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def rp_month_days(rpyr: int) -> list:
    return month_days(rpyr)


def fd_month_days(fdyr: int) -> list:
    return month_days(fdyr)


def compute_remmth(fddate: date, rpyr: int, rpmth: int, rpday: int) -> float:
    """
    Equivalent to %REMMTH macro:
    Compute remaining months from report date to fddate.
    Returns REMMTH value (can be fractional).
    """
    rpdays = rp_month_days(rpyr)
    fddays = fd_month_days(fddate.year)

    fdyr  = fddate.year
    fdmth = fddate.month
    fdday = fddate.day

    # If fd day equals end of fd month, set to rpday
    if fdday == fddays[fdmth]:
        fdday = rpday

    remy = fdyr  - rpyr
    remm = fdmth - rpmth
    remd = fdday - rpday
    remmth = remy * 12 + remm + remd / rpdays[rpmth]
    return remmth


def kremmth_format(remmth: float) -> str:
    """
    Equivalent to KREMMTH. format in SAS (defined in KALMPBBF).
    Maps remaining months to a 2-character code string.
    Typical BNM KREMMTH mapping (standard ALM remaining maturity bands):
      -1        -> '51'  (demand / on-call)
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
# PARSE REPORT DATE
# ─────────────────────────────────────────────────────────────
# RDATE is DDMMYYYY
reptdate = datetime.strptime(RDATE, "%d%m%Y").date()
rpyr  = reptdate.year
rpmth = reptdate.month
rpday = reptdate.day


# ─────────────────────────────────────────────────────────────
# STEP 1: READ FDMTHLY AND COMPUTE REMMTH (DATA ALM)
#   KEEP=BIC CUSTCODE REMMTH CURBAL
# ─────────────────────────────────────────────────────────────
con = duckdb.connect()
df_raw = con.execute(f"SELECT * FROM read_parquet('{FDMTHLY_PARQUET}')").pl()
con.close()

records = []
for row in df_raw.iter_rows(named=True):
    openind = row.get("OPENIND", "")
    bic     = str(row.get("BIC", "") or "")
    custcode= str(row.get("CUSTCODE", "") or "")
    curbal  = row.get("CURBAL", 0) or 0

    if openind == 'O':
        matdate_val = row.get("MATDATE")
        # MATDATE is numeric YYYYMMDD (stored as Z8 in SAS)
        if matdate_val is not None:
            matdate_str = str(int(matdate_val)).zfill(8)
            try:
                fddate = datetime.strptime(matdate_str, "%Y%m%d").date()
                remmth = compute_remmth(fddate, rpyr, rpmth, rpday)
            except ValueError:
                continue
        else:
            continue
    elif openind == 'D':
        remmth = -1.0
    else:
        continue

    records.append({
        "BIC":      bic,
        "CUSTCODE": custcode,
        "REMMTH":   remmth,
        "CURBAL":   curbal,
    })

alm = pl.DataFrame(records)

# ─────────────────────────────────────────────────────────────
# STEP 2: PROC SUMMARY - SUM CURBAL BY BIC CUSTCODE REMMTH
# ─────────────────────────────────────────────────────────────
alm = (
    alm.group_by(["BIC", "CUSTCODE", "REMMTH"])
    .agg(pl.col("CURBAL").sum().alias("AMOUNT"))
)

# ─────────────────────────────────────────────────────────────
# STEP 3: DATA ALMDEPT - map to BNMCODE
#   KEEP=BNMCODE AMOUNT CUSTCODE REMMTH
# ─────────────────────────────────────────────────────────────
dept_records = []
for row in alm.iter_rows(named=True):
    bic      = row["BIC"]
    custcode = row["CUSTCODE"]
    remmth   = row["REMMTH"]
    amount   = row["AMOUNT"]

    rm = kremmth_format(remmth)

    if custcode in ('81', '82', '83', '84'):
        bnmcode = (bic + custcode + rm + '0000Y')[:14]
        dept_records.append({"BNMCODE": bnmcode, "AMOUNT": amount,
                              "CUSTCODE": custcode, "REMMTH": remmth})

    elif custcode in ('85', '86', '90', '91', '92', '95', '96', '98', '99'):
        bnmcode = (bic + '85' + rm + '0000Y')[:14]
        dept_records.append({"BNMCODE": bnmcode, "AMOUNT": amount,
                              "CUSTCODE": custcode, "REMMTH": remmth})
    # OTHERWISE: no output

almdept = pl.DataFrame(dept_records, schema={
    "BNMCODE":  pl.Utf8,
    "AMOUNT":   pl.Float64,
    "CUSTCODE": pl.Utf8,
    "REMMTH":   pl.Float64,
})


# ─────────────────────────────────────────────────────────────
# HELPER: PROC SUMMARY + PROC PRINT report block
# ─────────────────────────────────────────────────────────────
def summarise_and_report(df: pl.DataFrame, prefix: str,
                         title1: str, title2: str, title3: str = "") -> str:
    """
    Filter BNMCODE by prefix, summarise AMOUNT by BNMCODE,
    produce a PROC PRINT style report string with ASA carriage control.
    """
    filtered = df.filter(pl.col("BNMCODE").str.starts_with(prefix))

    if filtered.is_empty():
        summarised = pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64})
    else:
        summarised = (
            filtered.group_by("BNMCODE")
            .agg(pl.col("AMOUNT").sum())
            .sort("BNMCODE")
        )

    total = summarised["AMOUNT"].sum() if not summarised.is_empty() else 0.0

    lines = []
    # ASA: '1' = new page
    lines.append(f"1{title1}")
    lines.append(f" {title2}")
    if title3:
        lines.append(f" {title3}")
    lines.append(f" ")

    # Header
    hdr = f" {'OBS':<6} {'BNMCODE':<14}  {'AMOUNT':>20}"
    lines.append(f" {hdr}")
    lines.append(f" {'-'*len(hdr)}")

    for i, row in enumerate(summarised.iter_rows(named=True), 1):
        amount_fmt = f"{row['AMOUNT']:>20,.2f}"
        lines.append(f"  {i:<6} {row['BNMCODE']:<14}  {amount_fmt}")

    # Sum line
    lines.append(f" {'-'*len(hdr)}")
    total_fmt = f"{total:>20,.2f}"
    lines.append(f"  {'':6} {'':14}  {total_fmt}")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# REPORT 1: prefix '42130'
# TITLE1 'SPECIAL PURPOSE ITEMS (QUARTERLY): EXTERNAL LIABILITIES'
# TITLE2 'AS AT ' &RDATE
# TITLE3 'CODE 81 & 85 FOR 42130-80-XX-0000Y'
# ─────────────────────────────────────────────────────────────
report1 = summarise_and_report(
    almdept,
    prefix="42130",
    title1="SPECIAL PURPOSE ITEMS (QUARTERLY): EXTERNAL LIABILITIES",
    title2=f"AS AT {RDATE}",
    title3="CODE 81 & 85 FOR 42130-80-XX-0000Y",
)

# ─────────────────────────────────────────────────────────────
# REPORT 2: prefix '42132'
# TITLE3 'CODE 81 & 85 FOR 42132-80-XX-0000Y'
# ─────────────────────────────────────────────────────────────
report2 = summarise_and_report(
    almdept,
    prefix="42132",
    title1="SPECIAL PURPOSE ITEMS (QUARTERLY): EXTERNAL LIABILITIES",
    title2=f"AS AT {RDATE}",
    title3="CODE 81 & 85 FOR 42132-80-XX-0000Y",
)

# ─────────────────────────────────────────────────────────────
# REPORT 3: prefix '42630'
# TITLE1 'REPORT ON EXTERNAL LIABILITIES FOR FCY FD FROM FNBE (85)'
# TITLE2 'AS AT ' &RDATE
# ─────────────────────────────────────────────────────────────
report3 = summarise_and_report(
    almdept,
    prefix="42630",
    title1="REPORT ON EXTERNAL LIABILITIES FOR FCY FD FROM FNBE (85)",
    title2=f"AS AT {RDATE}",
)

# ─────────────────────────────────────────────────────────────
# WRITE OUTPUT
# ─────────────────────────────────────────────────────────────
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(report1)
    f.write("\n")
    f.write(report2)
    f.write("\n")
    f.write(report3)
    f.write("\n")

print(f"Output written to: {OUTPUT_FILE}")
