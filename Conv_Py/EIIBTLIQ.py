#!/usr/bin/env python3
"""
Program  : EIIBTLIQ.py
Purpose  : IBT loan maturity profile breakdown (Part 1 & 2 — RM).
           Produces SASLIST report of BNM BNMCODE/AMOUNT buckets for
           IBT trade bills (utilised + undrawn), then FTPs result.

           Reads  : BNM1.REPTDATE                 (parquet)
                    BNM1.IBTRAD<REPTMON><NOWK>    (parquet)
                    BNM1.IBTMAST<REPTMON><NOWK>   (parquet, undrawn/OV)

           Writes : output/EIIBTLIQ.txt           — SASLIST report (LRECL=80)

           Dependencies: PBBLNFMT, PBBELF (included in SAS source;
                         format functions used upstream in BNM1 datasets)

           FTP stub: uploads EIIBTLIQ.txt to
                     DRR: "FD-BNM REPORTING/PIBB/BNM RPTG"
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import logging
from datetime import date
from pathlib import Path
from typing import Optional

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# Note: %INC PGM(PBBLNFMT,PBBELF) appears in the SAS header.
#
# PBBLNFMT: BNM1.IBTRAD is already formatted upstream. The PRDFMT value map
#           (VALUE PRDFMT ... HL/RC/FL) is defined locally in the SAS program itself
#           and is NOT from PBBLNFMT — it is reproduced as prdfmt() below.
#
# PBBELF: defines EL/ELI BNMCODE tables for deposit reporting. This program
#           only constructs BNMCODE strings inline using string concatenation.
#           No PBBELF lookup functions are called here.
# ============================================================================

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path(".")
DATA_DIR   = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BNM1_DATA_DIR = DATA_DIR / "bnm1"
SASLIST_PATH  = OUTPUT_DIR / "EIIBTLIQ.txt"

# ============================================================================
# CONSTANTS — calendar day counts per month (31/30/28 defaults; Feb adjusted)
# Mirrors: RETAIN D1-D12 31 D4 D6 D9 D11 30 RD1-RD12 MD1-MD12 31 RD2 MD2 28
# ============================================================================
_BASE_LDAY = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

# REMFMT: remaining months → bucket code
# LOW–0.1='01', 0.1–1='02', 1–3='03', 3–6='04', 6–12='05', OTHER='06'
def remfmt(months: float) -> str:
    if months <= 0.1:    return '01'   # UP TO 1 WK
    elif months <= 1:    return '02'   # >1 WK – 1 MTH
    elif months <= 3:    return '03'   # >1 MTH – 3 MTHS
    elif months <= 6:    return '04'   # >3 – 6 MTHS
    elif months <= 12:   return '05'   # >6 MTHS – 1 YR
    else:                return '06'   # > 1 YEAR

# PRDFMT: product → category
_HL_PRODUCTS = {
    4, 5, 6, 7, 31, 32, 100, 101, 102, 103, 110, 111, 112, 113, 114, 115,
    116, 170, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
    225, 226, 227, 228, 229, 230, 231, 232, 233, 234
}
_RC_PRODUCTS = {350, 910, 925}

def prdfmt(product: int) -> str:
    if product in _HL_PRODUCTS: return 'HL'
    if product in _RC_PRODUCTS: return 'RC'
    return 'FL'

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ============================================================================
# HELPER: leap-year aware days-in-month array (D1–D12)
# ============================================================================
def lday_array(year: int) -> list[int]:
    d = _BASE_LDAY.copy()
    if year % 4 == 0:
        d[1] = 29
    return d


# ============================================================================
# HELPER: derive week code from day-of-month (SELECT WHEN logic)
# ============================================================================
def derive_nowk(day: int) -> str:
    if   day == 8:  return '1'
    elif day == 15: return '2'
    elif day == 22: return '3'
    else:           return '4'


# ============================================================================
# HELPER: %REMMTH macro — remaining months from REPTDATE to MATDT
# ============================================================================
def calc_remmth(matdt: date, reptdate: date) -> float:
    """
    %MACRO REMMTH:
      REMY  = MDYR  - RPYR
      REMM  = MDMTH - RPMTH
      REMD  = MDDAY - RPDAY  (capped to days-in-month of reptdate month)
      REMMTH = REMY*12 + REMM + REMD/RPDAYS(RPMTH)
    """
    rpyr   = reptdate.year
    rpmth  = reptdate.month
    rpday  = reptdate.day

    mdyr   = matdt.year
    mdmth  = matdt.month
    mdday  = matdt.day

    # RPDAYS array — days in reptdate month (leap-year aware)
    rpdays = lday_array(rpyr)
    rpdays_mth = rpdays[rpmth - 1]

    # MDDAYS array — cap MDDAY to days in matdt month
    mddays = lday_array(mdyr)
    if mdday > mddays[mdmth - 1]:
        mdday = mddays[mdmth - 1]
    # Cap MDDAY to RPDAYS of reptdate month
    if mdday > rpdays_mth:
        mdday = rpdays_mth

    remy  = mdyr  - rpyr
    remm  = mdmth - rpmth
    remd  = mdday - rpday

    return remy * 12 + remm + remd / rpdays_mth


# ============================================================================
# HELPER: %NXTBLDT macro — advance BLDATE by payment frequency
# (EIIBTLIQ variant: PAYFREQ='6' → biweekly; otherwise monthly intervals)
# ============================================================================
def next_bldate(bldate: date, issdte: date, payfreq: str, freq: int) -> date:
    """
    %MACRO NXTBLDT (EIIBTLIQ version):
      IF PAYFREQ='6': DD = DAY(BLDATE)+14, advance by fortnight.
      ELSE: DD=DAY(ISSDTE), MM=MONTH(BLDATE)+FREQ, advance monthly.
    End-of-month capping applied.
    """
    from datetime import date as _date
    import calendar

    lday = lday_array(bldate.year)

    if payfreq == '6':
        dd = bldate.day + 14
        mm = bldate.month
        yy = bldate.year
        # Leap-year Feb adjustment
        lday = lday_array(yy)
        if dd > lday[mm - 1]:
            dd -= lday[mm - 1]
            mm += 1
            if mm > 12:
                mm -= 12; yy += 1
    else:
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year
        if mm > 12:
            mm -= 12; yy += 1

    # Feb leap-year re-check
    lday = lday_array(yy)
    if dd > lday[mm - 1]:
        dd = lday[mm - 1]
    try:
        return _date(yy, mm, dd)
    except ValueError:
        return _date(yy, mm, lday[mm - 1])


# ============================================================================
# STEP 1: Load BNM1.REPTDATE
# ============================================================================
def load_reptdate() -> tuple[date, str, str, str, str]:
    """
    DATA REPTDATE; SET BNM1.REPTDATE;
    SELECT(DAY): NOWK=1/2/3/4.
    Returns (reptdate, nowk, reptmon, reptday, rdate).
    """
    df       = pl.read_parquet(BNM1_DATA_DIR / "reptdate.parquet")
    reptdate: date = df["REPTDATE"][0]

    nowk    = derive_nowk(reptdate.day)
    reptmon = f"{reptdate.month:02d}"
    reptday = f"{reptdate.day:02d}"
    rdate   = reptdate.strftime("%d/%m/%Y")

    log.info("REPTDATE=%s NOWK=%s REPTMON=%s RDATE=%s",
             reptdate, nowk, reptmon, rdate)
    return reptdate, nowk, reptmon, reptday, rdate


# ============================================================================
# STEP 2: Build NOTE — maturity cashflow buckets from BNM1.IBTRAD
# ============================================================================
def build_note(ibtrad_df: pl.DataFrame, reptdate: date) -> pl.DataFrame:
    """
    DATA NOTE (KEEP=BNMCODE AMOUNT):
      Filter PRODCD starts with '34' OR PRODUCT IN (225,226).
      CUST: '08' for retail (CUSTCD 77/78/95/96), else '09'.
      PROD: prdfmt(PRODUCT).
      ITEM based on CUST/PROD.
      Payment schedule loop — generate BNMCODE and AMOUNT per cashflow bucket.
    """
    rpyr   = reptdate.year
    rpmth  = reptdate.month
    rpday  = reptdate.day

    rows = []
    for r in ibtrad_df.to_dicts():
        prodcd  = str(r.get("PRODCD",  "") or "")
        product = int(r.get("PRODUCT", 0) or 0)

        if not (prodcd[:2] == "34" or product in (225, 226)):
            continue

        custcd   = str(r.get("CUSTCD",   "") or "")
        balance  = float(r.get("BALANCE", 0) or 0)
        payamt   = float(r.get("PAYAMT",  0) or 0)
        exprdate = r.get("EXPRDATE")
        bldate   = r.get("BLDATE")
        issdte   = r.get("ISSDTE")

        if payamt < 0:
            payamt = 0

        cust = "08" if custcd in ("77", "78", "95", "96") else "09"
        prod = prdfmt(product)

        # ITEM selection
        if custcd in ("77", "78", "95", "96"):
            item = "214" if prod == "HL" else "219"
        else:
            if prod == "FL":  item = "211"
            elif prod == "RC": item = "212"
            else:              item = "219"

        # DAYS — days past since last bill date
        days = 0
        if bldate and bldate > date(1960, 1, 1):
            days = (reptdate - bldate).days

        # EXPRDATE guard
        if not exprdate or exprdate <= date(1960, 1, 1):
            continue

        # Near-maturity: < 8 days remaining
        if (exprdate - reptdate).days < 8:
            remmth = 0.1
            # Final output
            bnmcode = f"95{item}{cust}{remfmt(remmth)}0000Y"
            rows.append({"BNMCODE": bnmcode, "AMOUNT": balance})
            if days > 89: remmth = 13
            bnmcode = f"93{item}{cust}{remfmt(remmth)}0000Y"
            rows.append({"BNMCODE": bnmcode, "AMOUNT": balance})
            continue

        # PAYFREQ hardcoded to '3' in EIIBTLIQ (PAYFREQ='3' → FREQ=6 months)
        payfreq = "3"
        freq    = 6   # WHEN('3') FREQ=6

        # RC/OV: BLDATE = EXPRDATE directly
        if product in (350, 910, 925):
            bldate = exprdate
        elif not bldate or bldate <= date(1960, 1, 1):
            bldate = issdte if issdte else reptdate
            while bldate <= reptdate:
                bldate = next_bldate(bldate, issdte or reptdate, payfreq, freq)

        if bldate > exprdate or balance <= payamt:
            bldate = exprdate

        # Amortisation loop
        bal_remain = balance
        while bldate <= exprdate:
            matdt  = bldate
            remmth = calc_remmth(matdt, reptdate)

            if remmth > 12 or bldate == exprdate:
                break

            amount      = payamt
            bal_remain -= payamt

            bnmcode = f"95{item}{cust}{remfmt(remmth)}0000Y"
            rows.append({"BNMCODE": bnmcode, "AMOUNT": amount})

            rm_out = 13 if days > 89 else remmth
            bnmcode = f"93{item}{cust}{remfmt(rm_out)}0000Y"
            rows.append({"BNMCODE": bnmcode, "AMOUNT": amount})

            bldate = next_bldate(bldate, issdte or reptdate, payfreq, freq)
            if bldate > exprdate or bal_remain <= payamt:
                bldate = exprdate

        # Final balance bucket
        remmth  = calc_remmth(exprdate, reptdate)
        bnmcode = f"95{item}{cust}{remfmt(remmth)}0000Y"
        rows.append({"BNMCODE": bnmcode, "AMOUNT": bal_remain})
        rm_out  = 13 if days > 89 else remmth
        bnmcode = f"93{item}{cust}{remfmt(rm_out)}0000Y"
        rows.append({"BNMCODE": bnmcode, "AMOUNT": bal_remain})

    if not rows:
        return pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64})

    df = pl.DataFrame(rows)
    return (
        df.group_by("BNMCODE")
          .agg(pl.col("AMOUNT").sum())
          .sort("BNMCODE")
    )


# ============================================================================
# STEP 3: Build UNOTE — undrawn portion from BNM1.IBTMAST (SUBACCT='OV')
# ============================================================================
def build_unote(ibtmast_df: pl.DataFrame, reptdate: date) -> pl.DataFrame:
    """
    PROC SORT DATA=BNM1.IBTMAST WHERE SUBACCT='OV' AND CUSTCD NE ' ' AND DCURBAL NE .;
    DATA UNOTE (KEEP=BNMCODE AMOUNT):
      MATDT = EXPRDATE;
      ITEM='429'; REMMTH from %REMMTH macro.
      BNMCODE='95'||ITEM||'00'||REMFMT.||'0000Y';
      RENAME DUNDRAWN=AMOUNT.
    """
    df = ibtmast_df.filter(
        (pl.col("SUBACCT") == "OV") &
        (pl.col("CUSTCD") != " ") &
        pl.col("DCURBAL").is_not_null()
    ).sort("ACCTNO")

    rows = []
    for r in df.to_dicts():
        exprdate  = r.get("EXPRDATE")
        dundrawn  = float(r.get("DUNDRAWN", 0) or 0)

        if not exprdate or exprdate <= date(1960, 1, 1):
            continue

        item = "429"
        matdt = exprdate

        if (matdt - reptdate).days < 8:
            remmth = 0.1
        else:
            remmth = calc_remmth(matdt, reptdate)

        bnmcode = f"95{item}00{remfmt(remmth)}0000Y"
        rows.append({"BNMCODE": bnmcode, "AMOUNT": dundrawn})

    if not rows:
        return pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64})

    df_out = pl.DataFrame(rows)
    return (
        df_out.group_by("BNMCODE")
              .agg(pl.col("AMOUNT").sum())
              .sort("BNMCODE")
    )


# ============================================================================
# STEP 4: Write SASLIST report (LRECL=80)
# ============================================================================
def write_saslist(note: pl.DataFrame, unote: pl.DataFrame,
                  rdate: str, path: Path) -> None:
    """
    PROC PRINT; SUM AMOUNT; — produces tabular listing with sum.
    Writes to SASLIST (LRECL=80, RECFM=FB).
    Title for UNOTE: 'UNDRAWN PORTION FOR TRADE BILLS AS AT &RDATE'.
    """
    lines = []

    def format_section(title: str, df: pl.DataFrame) -> list[str]:
        sec = [title, "", f"{'BNMCODE':<20}  {'AMOUNT':>20}", "-" * 44]
        total = 0.0
        for r in df.sort("BNMCODE").to_dicts():
            bnmcode = str(r.get("BNMCODE", "") or "")
            amount  = float(r.get("AMOUNT",  0) or 0)
            total  += amount
            sec.append(f"{bnmcode:<20}  {amount:>20.2f}")
        sec.append("-" * 44)
        sec.append(f"{'SUM':<20}  {total:>20.2f}")
        sec.append("")
        return sec

    lines += format_section("MATURITY PROFILE — UTILISED (NOTE)", note)
    lines += format_section(
        f"UNDRAWN PORTION FOR TRADE BILLS AS AT {rdate}", unote
    )

    # Pad to LRECL=80
    with open(path, "w", encoding="latin-1") as f:
        for line in lines:
            f.write(line[:80].ljust(80) + "\n")
    log.info("SASLIST written: %s", path)


# ============================================================================
# FTP STUB — upload EIIBTLIQ.txt to DRR
# ============================================================================
def ftp_output(path: Path) -> None:
    """
    //RUNSFTP EXEC COZBATCH
    cd "FD-BNM REPORTING/PIBB/BNM RPTG"
    PUT //SAP.PIBB.NLFBT.TEXT  EIIBTLIQ.TXT

    Requires paramiko + DRR credentials. Stub only — enable when configured.
    """
    log.warning(
        "FTP stub: upload %s to DRR 'FD-BNM REPORTING/PIBB/BNM RPTG' as EIIBTLIQ.TXT"
        " — configure paramiko credentials to activate.", path
    )
    # import paramiko
    # ...


# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    log.info("EIIBTLIQ started.")

    # ----------------------------------------------------------------
    # Load BNM1.REPTDATE
    # ----------------------------------------------------------------
    reptdate, nowk, reptmon, reptday, rdate = load_reptdate()

    # ----------------------------------------------------------------
    # Load BNM1.IBTRAD
    # ----------------------------------------------------------------
    ibtrad_path = BNM1_DATA_DIR / f"IBTRAD{reptmon}{nowk}.parquet"
    ibtrad_df   = pl.read_parquet(ibtrad_path)
    log.info("IBTRAD rows: %d", len(ibtrad_df))

    # ----------------------------------------------------------------
    # Build NOTE (maturity cashflow buckets)
    # ----------------------------------------------------------------
    note = build_note(ibtrad_df, reptdate)
    log.info("NOTE buckets: %d", len(note))

    # ----------------------------------------------------------------
    # Load BNM1.IBTMAST (undrawn OV accounts)
    # ----------------------------------------------------------------
    ibtmast_path = BNM1_DATA_DIR / f"IBTMAST{reptmon}{nowk}.parquet"
    ibtmast_df   = pl.read_parquet(ibtmast_path)
    log.info("IBTMAST rows: %d", len(ibtmast_df))

    # ----------------------------------------------------------------
    # Build UNOTE (undrawn portion)
    # ----------------------------------------------------------------
    unote = build_unote(ibtmast_df, reptdate)
    log.info("UNOTE buckets: %d", len(unote))

    # ----------------------------------------------------------------
    # Write SASLIST report
    # ----------------------------------------------------------------
    write_saslist(note, unote, rdate, SASLIST_PATH)

    # ----------------------------------------------------------------
    # FTP stub
    # ----------------------------------------------------------------
    ftp_output(SASLIST_PATH)

    log.info("EIIBTLIQ completed.")


if __name__ == "__main__":
    main()
