#!/usr/bin/env python3
"""
Program  : EIBMFAC4.py
Purpose  : Daily PBIF for NLF.
           Reads REPTDATE, determines if quarter-end (REPTQ), loads and enriches
            PBIF data via RDALPBIF (non quarter-end) or RDLMPBIF (quarter-end),
            computes remaining months to maturity, assigns BNMCODE, summarises
            by BNMCODE, and writes output to BNM.PBIF parquet.

           Dependencies:
             PBBELF   -> (format definitions - EL_DEFINITIONS, etc.)
             PBBDPFMT -> (product format definitions)
             RDALPBIF -> load_pbif, compute_matdte (non quarter-end path)
             RDLMPBIF -> main as rdlmpbif_main    (quarter-end path)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# Note: %INC PGM(PBBELF) and %INC PGM(PBBDPFMT) appear in the SAS source as suite-wide boilerplate.
#       Neither PBBELF nor PBBDPFMT formats are directly invoked in EIBDFACT itself
#       — all formats used here (REMFMT, %DCLVAR, %NXTBLDT, %REMMTH) are defined locally.
#       PBBELF/PBBDPFMT are consumed by RDALPBIF and RDLMPBIF in their own respective modules,
#       so no import is needed here.
#
# RDALPBIF: non quarter-end path
from RDALPBIF import load_pbif as rdalpbif_load, compute_matdte as rdalpbif_matdte

# RDLMPBIF: quarter-end path
from RDLMPBIF import main as rdlmpbif_main

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR       = Path(".")
DATA_DIR       = BASE_DIR / "data"
OUTPUT_DIR     = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_PATH  = DATA_DIR / "bnm" / "reptdate.parquet"   # PBIF.REPTDATE
BNM_OUTPUT     = OUTPUT_DIR / "bnm_pbif.parquet"          # BNM.PBIF output

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
# FORMAT: REMFMT
# Replicate PROC FORMAT VALUE REMFMT bucketing on remaining months (REMMTH).
#
#   LOW-0.1 = '01'   /*  UP TO 1 WK       */
#   0.1-1   = '02'   /*  >1 WK - 1 MTH    */
#   1-3     = '03'   /*  >1 MTH - 3 MTHS  */
#   3-6     = '04'   /*  >3 - 6 MTHS      */
#   6-12    = '05'   /*  >6 MTHS - 1 YR   */
#   OTHER   = '06';  /*  > 1 YEAR         */
# ============================================================================

def remfmt(remmth: float) -> str:
    """Apply REMFMT format to remaining months value."""
    if remmth <= 0.1:
        return '01'
    elif remmth <= 1:
        return '02'
    elif remmth <= 3:
        return '03'
    elif remmth <= 6:
        return '04'
    elif remmth <= 12:
        return '05'
    else:
        return '06'


# ============================================================================
# DAYS-IN-MONTH ARRAYS  (%MACRO DCLVAR)
# RETAIN D1-D12 31 D4 D6 D9 D11 30
# RETAIN RD1-RD12 31 RD2 28 RD4 RD6 RD9 RD11 30
# ============================================================================

def days_in_month(mm: int, yy: int) -> int:
    """Return days in month mm of year yy (replicates LDAY array + leap logic)."""
    if mm == 2:
        return 29 if (yy % 4 == 0) else 28
    if mm in (4, 6, 9, 11):
        return 30
    return 31


# ============================================================================
# %MACRO NXTBLDT: advance MATDTE by FREQ months, clamping day to month end
# ============================================================================

def next_billing_date(matdte: date, freq: int) -> date:
    """
    Replicate %MACRO NXTBLDT:
      DD = DAY(MATDTE)
      MM = MONTH(MATDTE) + FREQ
      If MM > 12: MM -= 12, YY += 1
      If DD > LDAY(MM): DD = LDAY(MM)
      MATDTE = MDY(MM, DD, YY)
    """
    dd = matdte.day
    mm = matdte.month + freq
    yy = matdte.year
    if mm > 12:
        mm -= 12
        yy += 1
    max_dd = days_in_month(mm, yy)
    if dd > max_dd:
        dd = max_dd
    return date(yy, mm, dd)


# ============================================================================
# %MACRO REMMTH: compute remaining months from REPTDATE to MATDTE
# ============================================================================

def calc_remmth(matdte: date, rpyr: int, rpmth: int, rpday: int) -> float:
    """
    Replicate %MACRO REMMTH:
      MDYR  = YEAR(MATDTE)
      MDMTH = MONTH(MATDTE)
      MDDAY = DAY(MATDTE)
      If MDDAY > RPDAYS(RPMTH): MDDAY = RPDAYS(RPMTH)
      REMY = MDYR - RPYR
      REMM = MDMTH - RPMTH
      REMD = MDDAY - RPDAY
      REMMTH = REMY*12 + REMM + REMD/RPDAYS(RPMTH)
    """
    mdyr  = matdte.year
    mdmth = matdte.month
    mdday = matdte.day

    rpdays_rpmth = days_in_month(rpmth, rpyr)
    if mdday > rpdays_rpmth:
        mdday = rpdays_rpmth

    remy  = mdyr  - rpyr
    remm  = mdmth - rpmth
    remd  = mdday - rpday

    remmth = remy * 12 + remm + remd / rpdays_rpmth
    return remmth


# ============================================================================
# STEP: GET REPTDATE
# DATA REPTDATE; SET PBIF.REPTDATE;
#   REPTQ='N'; IF DAY(REPTDATE+1)=1 THEN REPTQ='Y';
#   SELECT(DAY(REPTDATE)):
#     WHEN(8)  -> NOWK='1'
#     WHEN(15) -> NOWK='2'
#     WHEN(22) -> NOWK='3'
#     OTHERWISE-> NOWK='4'
# ============================================================================

def load_reptdate() -> dict:
    """
    Load REPTDATE from parquet and derive macro variables:
      REPTQ, NOWK, RDATX, REPTYR, REPTYEAR, REPTMON, REPTDAY, RDATE, MDATE
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT * FROM read_parquet('{REPTDATE_PATH}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate: date = row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))

    # REPTQ: if next day is 1st of month -> quarter-end flag
    reptq = 'Y' if (reptdate + timedelta(days=1)).day == 1 else 'N'

    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    # RDATX: SAS date integer (days since 01-Jan-1960) formatted Z5.
    sas_epoch = date(1960, 1, 1)
    rdatx_int = (reptdate - sas_epoch).days
    rdatx     = str(rdatx_int).zfill(5)

    # MDATE: same as RDATX (PUT(REPTDATE,Z5.))
    mdate_str = rdatx

    return {
        "reptdate":  reptdate,
        "reptq":     reptq,
        "nowk":      nowk,
        "rdatx":     rdatx,
        "rdatx_int": rdatx_int,
        "reptyr":    reptdate.year,
        "reptyear":  reptdate.strftime("%y"),         # YEAR2. two-digit
        "reptmon":   f"{reptdate.month:02d}",
        "reptday":   f"{reptdate.day:02d}",
        "rdate":     reptdate.strftime("%d%m%Y"),     # DDMMYY8.
        "mdate":     mdate_str,
    }


# ============================================================================
# DATA PBIF (main enrichment step post RDAL):
#   RPYR / RPMTH / RPDAY from macro vars
#   IF MOD(RPYR,4)=0 THEN RD2=29
#   DAYS = MATDTE - &RDATX (SAS date integer)
#   IF DAYS < 8 THEN REMMTH = 0.1
#   ELSE %REMMTH
#   IF CUSTCX IN ('77','78','95','96') THEN CUST='08'; ELSE CUST='09'
#   AMOUNT = BALANCE
#   BNMCODE = '95211' || CUST || PUT(REMMTH,REMFMT.) || '0000Y'; OUTPUT
#   BNMCODE = '93211' || CUST || PUT(REMMTH,REMFMT.) || '0000Y'; OUTPUT
# ============================================================================

def enrich_pbif(pbif: pl.DataFrame, env: dict) -> pl.DataFrame:
    """
    Apply the main DATA PBIF enrichment step to produce two BNMCODE output rows
    per input row (prefixes '95211' and '93211').
    """
    rpyr      = env["reptyr"]
    rpmth     = int(env["reptmon"])
    rpday     = int(env["reptday"])
    rdatx_int = env["rdatx_int"]

    sas_epoch = date(1960, 1, 1)

    output_rows = []
    for r in pbif.to_dicts():
        matdte = r.get("MATDTE")
        if matdte is None:
            continue

        if isinstance(matdte, (int, float)):
            matdte = sas_epoch + timedelta(days=int(matdte))

        # DAYS = MATDTE - &RDATX (SAS numeric date difference)
        matdte_sas = (matdte - sas_epoch).days
        days = matdte_sas - rdatx_int

        # REMMTH derivation
        if days < 8:
            remmth = 0.1
        else:
            remmth = calc_remmth(matdte, rpyr, rpmth, rpday)

        # CUST derivation
        custcx = str(r.get("CUSTCX", "") or "").strip()
        cust = '08' if custcx in ('77', '78', '95', '96') else '09'

        amount = float(r.get("BALANCE", 0) or 0)
        rem_code = remfmt(remmth)

        # OUTPUT row 1: BNMCODE prefix '95211'
        row1 = dict(r)
        row1["BNMCODE"] = f"95211{cust}{rem_code}0000Y"
        row1["AMOUNT"]  = amount
        output_rows.append(row1)

        # OUTPUT row 2: BNMCODE prefix '93211'
        row2 = dict(r)
        row2["BNMCODE"] = f"93211{cust}{rem_code}0000Y"
        row2["AMOUNT"]  = amount
        output_rows.append(row2)

    return pl.DataFrame(output_rows) if output_rows else pl.DataFrame()


# ============================================================================
# PROC SUMMARY NWAY: CLASS BNMCODE; VAR AMOUNT; OUTPUT SUM=
# ============================================================================

def summarise_by_bnmcode(pbif: pl.DataFrame) -> pl.DataFrame:
    """
    Replicate PROC SUMMARY DATA=PBIF NWAY; CLASS BNMCODE; VAR AMOUNT; OUTPUT SUM=
    Returns dataframe with BNMCODE and summed AMOUNT, sorted by BNMCODE.
    """
    return (
        pbif.group_by("BNMCODE")
            .agg(pl.col("AMOUNT").sum())
            .sort("BNMCODE")
    )


# ============================================================================
# PROC PRINT: print BNM.PBIF summary to log
# ============================================================================

def proc_print(df: pl.DataFrame) -> None:
    """Replicate PROC PRINT DATA=BNM.PBIF; RUN;"""
    log.info("BNM.PBIF PROC PRINT (rows=%d):", len(df))
    log.info("  %-20s  %20s", "BNMCODE", "AMOUNT")
    log.info("  %s", "-" * 44)
    for row in df.iter_rows(named=True):
        log.info("  %-20s  %20.2f", row.get("BNMCODE", ""), float(row.get("AMOUNT", 0) or 0))


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    """
    Main entry point replicating EIBDFACT SAS program logic:
      1. Load REPTDATE and derive macro variables.
      2. Call RDALPBIF (non quarter-end) or RDLMPBIF (quarter-end).
      3. Enrich PBIF with REMMTH, CUST, BNMCODE, AMOUNT (two rows per record).
      4. Summarise by BNMCODE.
      5. Write output to BNM.PBIF parquet and print.
    """
    log.info("EIBMFAC4 started.")

    # ----------------------------------------------------------------
    # GET REPTDATE and macro vars
    # ----------------------------------------------------------------
    env = load_reptdate()
    reptdate = env["reptdate"]
    reptq    = env["reptq"]

    log.info("REPTDATE=%s  REPTQ=%s  NOWK=%s", reptdate, reptq, env["nowk"])

    # ----------------------------------------------------------------
    # %RDAL: conditionally include RDALPBIF or RDLMPBIF
    # %IF "&REPTQ" NE "Y" %THEN %DO; %INC PGM(RDALPBIF); %END;
    #                     %ELSE %DO; %INC PGM(RDLMPBIF); %END;
    # ----------------------------------------------------------------
    if reptq != 'Y':
        # Non quarter-end: RDALPBIF path
        log.info("Non quarter-end path: RDALPBIF")
        pbif = rdalpbif_load(
            reptyear=env["reptyear"],
            reptmon=env["reptmon"],
            reptday=env["reptday"],
        )
        pbif = rdalpbif_matdte(pbif, reptdate)
    else:
        # Quarter-end: RDLMPBIF path
        log.info("Quarter-end path: RDLMPBIF")
        pbif = rdlmpbif_main(
            reptdate=reptdate,
            reptyear=env["reptyear"],
            reptmon=env["reptmon"],
            reptday=env["reptday"],
        )

    log.info("PBIF rows loaded: %d", len(pbif))

    # ----------------------------------------------------------------
    # DATA PBIF: enrich with REMMTH, CUST, BNMCODE, AMOUNT
    # ----------------------------------------------------------------
    pbif_enriched = enrich_pbif(pbif, env)
    log.info("PBIF enriched rows (2x per record): %d", len(pbif_enriched))

    # ----------------------------------------------------------------
    # PROC SUMMARY NWAY: CLASS BNMCODE; VAR AMOUNT; OUTPUT SUM=
    # ----------------------------------------------------------------
    bnm_pbif = summarise_by_bnmcode(pbif_enriched)
    log.info("BNM.PBIF summarised rows: %d", len(bnm_pbif))

    # ----------------------------------------------------------------
    # Write BNM.PBIF output (DROP=_TYPE_ _FREQ_ already excluded by groupby)
    # ----------------------------------------------------------------
    bnm_pbif.write_parquet(BNM_OUTPUT)
    log.info("BNM.PBIF written: %s", BNM_OUTPUT)

    # ----------------------------------------------------------------
    # PROC PRINT DATA=BNM.PBIF; RUN;
    # ----------------------------------------------------------------
    proc_print(bnm_pbif)

    log.info("EIBMFAC4 completed.")


if __name__ == "__main__":
    main()
