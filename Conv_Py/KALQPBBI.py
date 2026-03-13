#!/usr/bin/env python3
"""
Program : KALQPBBI.py (ISLAMIC)
Report  : DOMESTIC ASSETS AND LIABILITIES (KAPITI) - PART III

- Reads Islamic FX deposit records from BNMK.K1TBL, computes remaining
    maturity buckets using the KREMMTH format (from KALMPBBF), and assigns
    BNM codes for non-resident deposits by country and customer type.
- Produces a summarised output and a PROC PRINT equivalent report.

Dependencies:
    - KALMPBBF : Provides format_kremmth (SAS: KREMMTH.)
                 Note: format_orimat (ORIMAT.) is also available in KALMPBBF
                       but is not used in this program.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# SAS: %INC PGM(KALMPBBF);
# format_kremmth maps remaining months to BNM bucket codes (e.g. '51','52',...'60')
# format_orimat is available in KALMPBBF but not called in this program.
from KALMPBBF import format_kremmth

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Macro variable equivalents
# &REPTMON — reporting month string e.g. "200509"
# &NOWK    — week suffix string e.g. ""
# &RDATE   — reporting date string in DDMMYY8. format e.g. "30/09/05"
REPTMON: str = "200509"
NOWK:    str = ""
RDATE:   str = "30/09/05"   # DDMMYY8. format (DD/MM/YY)

# Input / output paths
K1TBL_PARQUET = INPUT_DIR  / f"K1TBL{REPTMON}{NOWK}.parquet"
K1TABL_OUTPUT = OUTPUT_DIR / f"K1TABL{REPTMON}{NOWK}.parquet"
REPORT_OUTPUT = OUTPUT_DIR / f"KALQPBBI_{REPTMON}{NOWK}.txt"

# Page layout for ASA report
PAGE_LENGTH = 60

# G7 country codes used in BNM classification
_G7_COUNTRIES = {"CA", "FR", "IT", "DE", "JP", "US", "GB"}


# ---------------------------------------------------------------------------
# SAS date helpers
# ---------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_python(sas_days: Optional[int]) -> Optional[date]:
    """Convert SAS date (days since 1960-01-01) to Python date."""
    if sas_days is None:
        return None
    return SAS_EPOCH + timedelta(days=int(sas_days))


def parse_ddmmyy8(rdate_str: str) -> date:
    """
    Parse reporting date from DDMMYY8. format (e.g. '30/09/05').
    SAS INPUT("&RDATE", DDMMYY8.) interprets 2-digit year with SAS century-window rules:
    years 00-99 map to 1960-2059 (default SAS YEARCUTOFF=1900 + 60 = 1960).
    """
    from datetime import datetime
    # Allow both '30/09/05' and '30/09/2005'
    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%d%m%y", "%d%m%Y"):
        try:
            return datetime.strptime(rdate_str.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse RDATE: '{rdate_str}'")


# ---------------------------------------------------------------------------
# MACRO DCLVAR — days-per-month arrays (RPDAYS and NDDAYS)
# ---------------------------------------------------------------------------
_BASE_DAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]  # Jan-Dec


def _make_days_arr(year: int) -> list[int]:
    """Return days-per-month array for the given year (leap-year aware)."""
    arr = _BASE_DAYS[:]
    if year % 4 == 0:
        arr[1] = 29
    return arr


# ---------------------------------------------------------------------------
# MACRO REMMTH — compute fractional remaining months
# ---------------------------------------------------------------------------
def calc_remmth(
    nddate: date,
    reptdate: date,
    rpdays_arr: list[int],
) -> float:
    """
    Python equivalent of %REMMTH macro (KALQPBBI variant).

    Key difference vs KALMPBBI: instead of capping NDDAY to RPDAYS(RPMTH),
    this macro sets NDDAY = RPDAYS(RPMTH) when NDDAY equals the last day of
    the maturity month (IF NDDAY = NDDAYS(NDMTH) THEN NDDAY=RPDAYS(RPMTH)).

    REMMTH = REMY*12 + REMM + REMD / RPDAYS(RPMTH)
    """
    ndyr  = nddate.year
    ndmth = nddate.month
    ndday = nddate.day

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    # NDDAYS — days-per-month for maturity year
    nddays_arr = _make_days_arr(ndyr)

    # IF NDDAY = NDDAYS(NDMTH) THEN NDDAY = RPDAYS(RPMTH)
    rp_mth_days = rpdays_arr[rpmth - 1]
    nd_mth_days = nddays_arr[ndmth - 1]
    if ndday == nd_mth_days:
        ndday = rp_mth_days

    remy = ndyr  - rpyr
    remm = ndmth - rpmth
    remd = ndday - rpday

    return remy * 12 + remm + remd / rp_mth_days


# ---------------------------------------------------------------------------
# DATA K1TABL — deposit from non-resident, remaining maturity
# ---------------------------------------------------------------------------
def build_k1tabl(k1tbl_path: Path, reptdate: date) -> pl.DataFrame:
    """
    DATA K1TABL (KEEP=BNMCODE AMOUNT AMTIND);
    SET BNMK.K1TBL&REPTMON&NOWK (RENAME=(GWBALC=AMOUNT));

    Filters:
        - BRANCH > 2000
        - GWMVT = 'P' AND GWMVTS = 'M'
        - GWDLP IN ('FDA','FDB','FDL','FDS') -> BIC='42000'
          OR GWDLP IN ('BF','BO')            -> BIC='48000'
    Assigns BNMCODE based on customer type, account type, and country.

    BNM code structure: BIC || CTRY_CODE || RM || '0000Y'
        where RM = KREMMTH bucket of remaining months to maturity.

    Special rule:
        - GWNANC='00001' AND GWACT='CV' -> RM='51' (override: overdue bucket)
    """
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{k1tbl_path}')").pl()
    con.close()

    # RENAME GWBALC -> AMOUNT
    if "GWBALC" in df.columns:
        df = df.rename({"GWBALC": "AMOUNT"})

    rpdays_arr = _make_days_arr(reptdate.year)

    output_rows: list[dict] = []

    for row in df.iter_rows(named=True):
        # BRANCH = INPUT(SUBSTR(GWAB,1,4), 4.)
        gwab = (row.get("GWAB") or "").ljust(4)
        try:
            branch = int(gwab[:4].strip())
        except ValueError:
            branch = 0

        # IF BRANCH > 2000
        if branch <= 2000:
            continue

        # IF GWMVT EQ 'P' AND GWMVTS EQ 'M'
        gwmvt  = (row.get("GWMVT")  or "").strip()
        gwmvts = (row.get("GWMVTS") or "").strip()
        if gwmvt != "P" or gwmvts != "M":
            continue

        gwdlp = (row.get("GWDLP") or "").strip()
        bic   = " "

        # IF GWDLP IN ('FDA','FDB','FDL','FDS') THEN BIC='42000';
        if gwdlp in ("FDA", "FDB", "FDL", "FDS"):
            bic = "42000"
        # ELSE IF GWDLP IN ('BF','BO') THEN BIC='48000';
        elif gwdlp in ("BF", "BO"):
            bic = "48000"

        # IF BIC ^= ' '
        if bic == " ":
            continue

        # Compute remaining maturity
        gwmdt_sas = row.get("GWMDT")
        if gwmdt_sas is None:
            continue
        nddate = sas_date_to_python(int(gwmdt_sas))

        remmth_val = calc_remmth(nddate, reptdate, rpdays_arr)

        gwnanc = (row.get("GWNANC") or "").strip()
        gwact  = (row.get("GWACT")  or "").strip()
        gwsac  = (row.get("GWSAC")  or "").strip()
        gwan   = (row.get("GWAN")   or "").ljust(1)
        gwcnap = (row.get("GWCNAP") or "").strip()
        gwcnal = (row.get("GWCNAL") or "").strip()
        gwctp  = (row.get("GWCTP")  or "").strip()
        amount = row.get("AMOUNT") or 0.0
        amtind = "I"

        # IF GWNANC='00001' AND GWACT='CV' THEN RM='51';
        # ELSE RM=PUT(REMMTH, KREMMTH.);
        if gwnanc == "00001" and gwact == "CV":
            rm = "51"
        else:
            rm = format_kremmth(remmth_val)

        bnmcode = " "

        # Condition 1:
        # IF GWSAC='UO' AND GWACT='CV' AND SUBSTR(GWAN,1,1)='0' AND
        #    (GWCNAP='MY' OR GWAN='000418')
        if (gwsac == "UO" and gwact == "CV"
                and gwan[:1] == "0"
                and (gwcnap == "MY" or gwan == "000418")):
            bnmcode = bic + "82" + rm + "0000Y"
            output_rows.append({
                "BNMCODE": bnmcode,
                "AMOUNT":  amount,
                "AMTIND":  amtind,
            })

        # Condition 2 (ELSE IF):
        elif (not ("BA" <= gwctp <= "BZ")
              and gwcnal != "MY"
              and gwsac == "UF"):
            bnmcode = bic + "85" + rm + "0000Y"
            output_rows.append({
                "BNMCODE": bnmcode,
                "AMOUNT":  amount,
                "AMTIND":  amtind,
            })

        # IF BNMCODE=' ' — fallback country-based classification
        if bnmcode == " ":
            # IF GWCNAL^='MY'
            if gwcnal != "MY":
                # SELECT(GWCNAP)
                if gwcnap in _G7_COUNTRIES:
                    bnmcode = bic + "83" + rm + "0000Y"
                    output_rows.append({
                        "BNMCODE": bnmcode,
                        "AMOUNT":  amount,
                        "AMTIND":  amtind,
                    })
                else:
                    bnmcode = bic + "84" + rm + "0000Y"
                    output_rows.append({
                        "BNMCODE": bnmcode,
                        "AMOUNT":  amount,
                        "AMTIND":  amtind,
                    })

    if not output_rows:
        return pl.DataFrame({
            "BNMCODE": pl.Series([], dtype=pl.Utf8),
            "AMOUNT":  pl.Series([], dtype=pl.Float64),
            "AMTIND":  pl.Series([], dtype=pl.Utf8),
        })

    return pl.DataFrame(output_rows)


# ---------------------------------------------------------------------------
# PROC SUMMARY — summarise by BNMCODE, AMTIND
# ---------------------------------------------------------------------------
def summarise(df: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SUMMARY DATA=K1TABL NWAY;
    CLASS BNMCODE AMTIND;
    VAR AMOUNT;
    OUTPUT OUT=K1TABL SUM=AMOUNT;
    """
    return (
        df
        .group_by(["BNMCODE", "AMTIND"])
        .agg(pl.col("AMOUNT").sum().alias("AMOUNT"))
        .sort(["BNMCODE", "AMTIND"])
    )


# ---------------------------------------------------------------------------
# Report writer — PROC PRINT equivalent with ASA carriage control
# ---------------------------------------------------------------------------
def write_report(df: pl.DataFrame, reptdate: date, output_path: Path) -> None:
    """
    OPTIONS NOCENTER NONUMBER NODATE;
    TITLE1 'PUBLIC BANK BERHAD';
    TITLE2 'ISLAMIC DOMESTIC ASSETS AND LIABILITIES (KAPITI) - PART III';
    TITLE3 'REPORT DATE : ' &RDATE;
    PROC PRINT DATA=K1TABL;
    FORMAT AMOUNT COMMA30.2;

    ASA carriage control: '1' = new page, ' ' = single space.
    Page length: 60 lines.
    """
    title1 = "PUBLIC BANK BERHAD"
    title2 = "ISLAMIC DOMESTIC ASSETS AND LIABILITIES (KAPITI) - PART III"
    title3 = f"REPORT DATE : {reptdate.strftime('%d/%m/%y')}"

    header_lines = [
        f" {title1}",
        f" {title2}",
        f" {title3}",
        " ",
        f" {'OBS':<6} {'BNMCODE':<16} {'AMTIND':<8} {'AMOUNT':>30}",
        " " + "-" * 64,
    ]

    HEADER_ROWS = len(header_lines)
    DATA_ROWS_PER_PAGE = PAGE_LENGTH - HEADER_ROWS

    lines: list[str] = []
    obs = 0
    for row in df.iter_rows(named=True):
        obs += 1
        bnmcode    = (row.get("BNMCODE") or "").ljust(14)
        amtind     = (row.get("AMTIND")  or "").ljust(1)
        amount     = row.get("AMOUNT") or 0.0
        # FORMAT AMOUNT COMMA30.2
        amount_str = f"{amount:>30,.2f}"
        lines.append(f" {obs:<6} {bnmcode:<16} {amtind:<8} {amount_str}")

    with open(output_path, "w", encoding="ascii", errors="replace") as f:
        i = 0
        total = len(lines)

        while True:
            # Write page header; first line gets ASA '1' (form feed)
            for h_idx, hline in enumerate(header_lines):
                asa = "1" if h_idx == 0 else " "
                f.write(asa + hline + "\n")

            # Write data lines for this page
            page_slice = lines[i:i + DATA_ROWS_PER_PAGE]
            for dline in page_slice:
                f.write(" " + dline + "\n")

            i += DATA_ROWS_PER_PAGE
            if i >= total:
                break


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # Parse reporting date from &RDATE macro variable (DDMMYY8. format)
    # SAS: REPTDATE = INPUT("&RDATE", DDMMYY8.);
    reptdate = parse_ddmmyy8(RDATE)

    # Build K1TABL from BNMK.K1TBL parquet
    k1tabl = build_k1tabl(K1TBL_PARQUET, reptdate)

    # PROC SUMMARY
    k1tabl_summary = summarise(k1tabl)

    # Persist output
    k1tabl_summary.write_parquet(K1TABL_OUTPUT)
    print(f"Written: {K1TABL_OUTPUT}  ({k1tabl_summary.height} rows)")

    # PROC PRINT report
    write_report(k1tabl_summary, reptdate, REPORT_OUTPUT)
    print(f"Report : {REPORT_OUTPUT}")


if __name__ == "__main__":
    main()
