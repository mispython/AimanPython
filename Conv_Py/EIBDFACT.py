#!/usr/bin/env python3
"""
Program  : EIBDFACT.py
Purpose  : Daily PBIF for NLF — maturity profile breakdown (RM).
           Loads PBIF client data, dispatches to RDALPBIF or RDLMPBIF
           based on whether REPTDATE is a quarter-end day (REPTQ),
           computes BNMCODE/AMOUNT buckets, and writes BNM.PBIF output.

           Reads  : PBIF.REPTDATE              (parquet)
                    PBIF.CLIEN<YY><MM><DD>     (parquet, via RDALPBIF/RDLMPBIF)
                    MECHRG(0)                  (fixed-width text, via RDLMPBIF)

           Writes : output/BNM_PBIF.parquet    — BNMCODE/AMOUNT summary
                    output/eibdfact_print.txt   — PROC PRINT equivalent

           Dependencies:
             PBBELF    — EL/ELI BNMCODE definitions (imported but not directly
                         used here; BNMCODE strings are constructed inline)
             PBBDPFMT  — deposit product formats (imported but not directly
                         called; product classification is upstream in PBIF data)
             RDALPBIF  — invoked when REPTQ='N' (non-quarter-end)
             RDLMPBIF  — invoked when REPTQ='Y' (quarter-end / last day of month)
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
# Note: %INC PGM(PBBELF,PBBDPFMT) appears in the SAS header.
#
# PBBELF:   defines EL/ELI BNMCODE tables for deposit eligibility reporting.
#           This program constructs BNMCODE strings inline ('95211'||CUST||...)
#           using the REMFMT bucket code — no PBBELF lookup is called here.
#
# PBBDPFMT: deposit product format mappings (SADENOM, SAPROD, etc.).
#           The PBIF dataset is already enriched upstream (PRODCD='30591'
#           fixed). No PBBDPFMT format functions are called in this program.
#
# RDALPBIF: called when REPTQ='N'. Returns enriched PBIF pl.DataFrame.
# RDLMPBIF: called when REPTQ='Y'. Returns enriched PBIF pl.DataFrame.
# ============================================================================
from RDALPBIF import main as rdalpbif_main
from RDLMPBIF import main as rdlmpbif_main

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR       = Path(".")
DATA_DIR       = BASE_DIR / "data"
OUTPUT_DIR     = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_PATH  = DATA_DIR / "pbif" / "reptdate.parquet"   # PBIF.REPTDATE
BNM_PBIF_PATH  = OUTPUT_DIR / "BNM_PBIF.parquet"          # BNM.PBIF output
PRINT_PATH     = OUTPUT_DIR / "eibdfact_print.txt"         # PROC PRINT output

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
# REMFMT: remaining months → 2-char BNM bucket code
# (PROC FORMAT VALUE REMFMT defined locally in EIBDFACT SAS source)
# LOW–0.1='01', 0.1–1='02', 1–3='03', 3–6='04', 6–12='05', OTHER='06'
# ============================================================================
def remfmt(months: float) -> str:
    if months <= 0.1:   return '01'   # UP TO 1 WK
    elif months <= 1:   return '02'   # >1 WK – 1 MTH
    elif months <= 3:   return '03'   # >1 MTH – 3 MTHS
    elif months <= 6:   return '04'   # >3 – 6 MTHS
    elif months <= 12:  return '05'   # >6 MTHS – 1 YR
    else:               return '06'   # > 1 YEAR


# ============================================================================
# %REMMTH macro — remaining months from REPTDATE to MATDTE
# ============================================================================
def calc_remmth(matdte: date, reptdate: date) -> float:
    """
    %MACRO REMMTH:
      MDYR  = YEAR(MATDTE); MDMTH = MONTH(MATDTE); MDDAY = DAY(MATDTE)
      Feb leap-year cap on MDDAY.
      IF MDDAY > RPDAYS(RPMTH) THEN MDDAY = RPDAYS(RPMTH)
      REMY = MDYR - RPYR;  REMM = MDMTH - RPMTH;  REMD = MDDAY - RPDAY
      REMMTH = REMY*12 + REMM + REMD / RPDAYS(RPMTH)
    """
    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    # Days in reptdate's month (RPDAYS array)
    rpdays_mth = _days_in_month(rpmth, rpyr)

    mdyr  = matdte.year
    mdmth = matdte.month
    mdday = matdte.day

    # Cap MDDAY to days in matdte's Feb if applicable
    if mdmth == 2:
        feb_days = 29 if mdyr % 4 == 0 else 28
        if mdday > feb_days:
            mdday = feb_days

    # Cap MDDAY to RPDAYS of reptdate month
    if mdday > rpdays_mth:
        mdday = rpdays_mth

    remy = mdyr  - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    return remy * 12 + remm + remd / rpdays_mth


def _days_in_month(mm: int, yy: int) -> int:
    if mm == 2:
        return 29 if yy % 4 == 0 else 28
    if mm in (4, 6, 9, 11):
        return 30
    return 31


# ============================================================================
# STEP 1: Load PBIF.REPTDATE — derive macro variables
# ============================================================================
def load_reptdate() -> tuple[date, str, str, str, str, str, str, str, str]:
    """
    DATA REPTDATE; SET PBIF.REPTDATE;
      REPTQ='N'; IF DAY(REPTDATE+1)=1 THEN REPTQ='Y';  ← last day of month
      SELECT(DAY(REPTDATE)): NOWK 1/2/3/4.
      CALL SYMPUT: REPTQ, RDATX (Z5.), REPTYR (YEAR4.), REPTYEAR (YEAR2.),
                   REPTMON (Z2.), REPTDAY (Z2.), RDATE (DDMMYY8.), MDATE (Z5.).
    Returns (reptdate, reptq, nowk, rdatx, reptyr, reptyear, reptmon, reptday, rdate).
    """
    from datetime import timedelta

    df       = pl.read_parquet(REPTDATE_PATH)
    reptdate: date = df["REPTDATE"][0]

    # REPTQ: 'Y' if next day is the 1st (i.e., reptdate is last day of month)
    next_day = reptdate + timedelta(days=1)
    reptq    = 'Y' if next_day.day == 1 else 'N'

    # NOWK: SELECT(DAY(REPTDATE))
    day = reptdate.day
    if   day == 8:  nowk = '1'
    elif day == 15: nowk = '2'
    elif day == 22: nowk = '3'
    else:           nowk = '4'

    # Macro variable equivalents
    rdatx    = str(int(reptdate.strftime("%y%m%d"))).zfill(5)  # Z5. of SAS date numeric
    reptyr   = str(reptdate.year)                               # YEAR4.
    reptyear = reptdate.strftime("%y")                          # YEAR2.
    reptmon  = f"{reptdate.month:02d}"                          # Z2.
    reptday  = f"{reptdate.day:02d}"                            # Z2.
    rdate    = reptdate.strftime("%d/%m/%Y")                    # DDMMYY8.

    log.info("REPTDATE=%s REPTQ=%s NOWK=%s REPTYR=%s REPTMON=%s REPTDAY=%s",
             reptdate, reptq, nowk, reptyr, reptmon, reptday)

    return reptdate, reptq, nowk, rdatx, reptyr, reptyear, reptmon, reptday, rdate


# ============================================================================
# STEP 2: Dispatch to RDALPBIF or RDLMPBIF based on REPTQ
# ============================================================================
def load_pbif(reptdate: date, reptq: str,
              reptyear: str, reptmon: str, reptday: str) -> pl.DataFrame:
    """
    %MACRO RDAL:
      %IF "&REPTQ" NE "Y" %THEN %DO; %INC PGM(RDALPBIF); %END;
                          %ELSE %DO; %INC PGM(RDLMPBIF); %END;
    %RDAL;
    Calls the appropriate sub-program and returns the enriched PBIF DataFrame.
    """
    if reptq != 'Y':
        log.info("REPTQ='N' — dispatching to RDALPBIF.")
        pbif = rdalpbif_main(
            reptdate=reptdate,
            reptyear=reptyear,
            reptmon=reptmon,
            reptday=reptday,
        )
    else:
        log.info("REPTQ='Y' — dispatching to RDLMPBIF.")
        pbif = rdlmpbif_main(
            reptdate=reptdate,
            reptyear=reptyear,
            reptmon=reptmon,
            reptday=reptday,
            mdate=reptdate,
        )
    log.info("PBIF rows from sub-program: %d", len(pbif))
    return pbif


# ============================================================================
# STEP 3: DATA PBIF — compute REMMTH, CUST, BNMCODE; output two rows per record
#   RPYR=&REPTYR; RPMTH=&REPTMON; RPDAY=&REPTDAY;
#   IF MOD(RPYR,4)=0 THEN RD2=29;
#   DAYS = MATDTE - &RDATX;
#   IF DAYS < 8 THEN REMMTH=0.1; ELSE %REMMTH;
#   IF CUSTCX IN ('77','78','95','96') THEN CUST='08'; ELSE CUST='09';
#   AMOUNT = BALANCE;
#   BNMCODE='95211'||CUST||PUT(REMMTH,REMFMT.)||'0000Y'; OUTPUT;
#   BNMCODE='93211'||CUST||PUT(REMMTH,REMFMT.)||'0000Y'; OUTPUT;
# ============================================================================
def build_bnmcode_rows(pbif: pl.DataFrame,
                       reptdate: date,
                       rdatx_sas: int) -> pl.DataFrame:
    """
    Compute REMMTH and generate two BNMCODE output rows per PBIF record.
    rdatx_sas: SAS date integer equivalent of REPTDATE (days since 01-Jan-1960).
    """
    rows = []
    for r in pbif.to_dicts():
        matdte  = r.get("MATDTE")
        balance = float(r.get("BALANCE", 0) or 0)
        custcx  = str(r.get("CUSTCX",   "") or "")

        if matdte is None:
            continue

        # DAYS = MATDTE (as SAS integer) - RDATX
        # Replicated as calendar day difference — same arithmetic result
        if isinstance(matdte, date):
            days = (matdte - reptdate).days
        else:
            days = 0

        # REMMTH
        if days < 8:
            remmth = 0.1
        else:
            remmth = calc_remmth(matdte, reptdate)

        # CUST
        cust = '08' if custcx in ('77', '78', '95', '96') else '09'

        amount  = balance
        bucket  = remfmt(remmth)

        # Two output rows per input record
        rows.append({"BNMCODE": f"95211{cust}{bucket}0000Y", "AMOUNT": amount})
        rows.append({"BNMCODE": f"93211{cust}{bucket}0000Y", "AMOUNT": amount})

    if not rows:
        return pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64})
    return pl.DataFrame(rows)


# ============================================================================
# STEP 4: PROC SUMMARY NWAY — CLASS BNMCODE; VAR AMOUNT; SUM=
#         → BNM.PBIF (DROP=_TYPE_ _FREQ_)
# ============================================================================
def summarise_pbif(df: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SUMMARY DATA=PBIF NWAY;
      CLASS BNMCODE; VAR AMOUNT;
      OUTPUT OUT=BNM.PBIF(DROP=_TYPE_ _FREQ_) SUM=;
    """
    return (
        df.group_by("BNMCODE")
          .agg(pl.col("AMOUNT").sum())
          .sort("BNMCODE")
    )


# ============================================================================
# STEP 5: PROC PRINT DATA=BNM.PBIF
# ============================================================================
def write_proc_print(df: pl.DataFrame, path: Path) -> None:
    """PROC PRINT DATA=BNM.PBIF — writes tabular listing with sum."""
    lines = []
    lines.append(f"{'BNMCODE':<20}  {'AMOUNT':>20}")
    lines.append("-" * 44)

    total = 0.0
    for r in df.sort("BNMCODE").to_dicts():
        bnmcode = str(r.get("BNMCODE", "") or "")
        amount  = float(r.get("AMOUNT",  0) or 0)
        total  += amount
        lines.append(f"{bnmcode:<20}  {amount:>20.2f}")

    lines.append("-" * 44)
    lines.append(f"{'SUM':<20}  {total:>20.2f}")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    log.info("PROC PRINT written: %s", path)


# ============================================================================
# HELPER: SAS date integer for REPTDATE (days since 01-Jan-1960)
# ============================================================================
def to_sas_date(d: date) -> int:
    from datetime import date as _date
    sas_epoch = _date(1960, 1, 1)
    return (d - sas_epoch).days


# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    log.info("EIBDFACT started.")

    # ----------------------------------------------------------------
    # STEP 1: Load REPTDATE
    # ----------------------------------------------------------------
    reptdate, reptq, nowk, rdatx, reptyr, reptyear, reptmon, reptday, rdate = (
        load_reptdate()
    )
    rdatx_sas = to_sas_date(reptdate)   # RDATX as SAS integer (for DAYS arithmetic)

    # ----------------------------------------------------------------
    # STEP 2: Dispatch to RDALPBIF / RDLMPBIF
    # ----------------------------------------------------------------
    pbif = load_pbif(reptdate, reptq, reptyear, reptmon, reptday)

    # ----------------------------------------------------------------
    # STEP 3: Compute BNMCODE rows
    # ----------------------------------------------------------------
    pbif_coded = build_bnmcode_rows(pbif, reptdate, rdatx_sas)
    log.info("BNMCODE rows before summary: %d", len(pbif_coded))

    # ----------------------------------------------------------------
    # STEP 4: PROC SUMMARY → BNM.PBIF
    # ----------------------------------------------------------------
    bnm_pbif = summarise_pbif(pbif_coded)
    log.info("BNM.PBIF buckets: %d", len(bnm_pbif))

    # ----------------------------------------------------------------
    # STEP 5: Write output
    # ----------------------------------------------------------------
    bnm_pbif.write_parquet(BNM_PBIF_PATH)
    log.info("BNM.PBIF written: %s", BNM_PBIF_PATH)

    write_proc_print(bnm_pbif, PRINT_PATH)

    log.info("EIBDFACT completed.")


if __name__ == "__main__":
    main()
