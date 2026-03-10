#!/usr/bin/env python3
"""
Program  : EIBMLIB4.py
Purpose  : Daily BT (Bills of Trade) NLF processing.
           Reads REPTDATE from BNM1.REPTDATE, loads BNM1.BTRADmmww trade data,
            filters to product code '34xx' or product IN (225,226),
            iterates repayment schedule to assign BNMCODE per maturity bucket,
            summarises by BNMCODE, and writes output to BNM.BT parquet.

           Dependencies:
             %INC PGM(PBBLNFMT) -> loan format definitions.
               PBBLNFMT is included as suite-wide boilerplate in the SAS source.
               None of its format functions are directly called in EIBMLIBT;
               they are consumed by downstream suite programs. No import needed.
             %INC PGM(PBBELF)   -> EL/branch format definitions.
               Same reasoning as PBBLNFMT — included for session availability
               but not directly invoked here. No import needed.
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import logging
from datetime import date, timedelta
from pathlib import Path

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb
import polars as pl

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR      = Path(".")
DATA_DIR      = BASE_DIR / "data"
OUTPUT_DIR    = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# BNM1.REPTDATE
REPTDATE_PATH = DATA_DIR / "bnm1" / "reptdate.parquet"

# BNM1.BTRADmmww  -- resolved at runtime using REPTMON + NOWK
# e.g. data/bnm1/btrad0314.parquet  (REPTMON=03, NOWK=4)
BTRAD_PATTERN = DATA_DIR / "bnm1"

# BNM.BT output
BNM_OUTPUT    = OUTPUT_DIR / "bnm_bt.parquet"

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
# VALUE REMFMT
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
# FORMAT: PRDFMT
# VALUE PRDFMT
#   4,5,6,7,31,32,100,101,102,103,110,111,112,113,114,115,
#   116,170,200,201,204,205,209,210,
#   211,212,214,215,219,220,225,226,
#   227,228,229,230,231,232,233,234 = 'HL'
#   350,910,925 = 'RC'
#   OTHER = 'FL'
# ============================================================================

_PRDFMT_HL = frozenset([
    4, 5, 6, 7, 31, 32, 100, 101, 102, 103, 110, 111, 112, 113, 114, 115,
    116, 170, 200, 201, 204, 205, 209, 210,
    211, 212, 214, 215, 219, 220, 225, 226,
    227, 228, 229, 230, 231, 232, 233, 234,
])
_PRDFMT_RC = frozenset([350, 910, 925])


def prdfmt(product: int) -> str:
    """Apply PRDFMT format: 'HL', 'RC', or 'FL'."""
    if product in _PRDFMT_HL:
        return 'HL'
    if product in _PRDFMT_RC:
        return 'RC'
    return 'FL'


# ============================================================================
# %MACRO DCLVAR: days-in-month helpers
# RETAIN D1-D12 31, D4 D6 D9 D11 30
# ============================================================================

def days_in_month(mm: int, yy: int) -> int:
    """Return days in month mm of year yy (replicates LDAY/RPDAYS arrays)."""
    if mm == 2:
        return 29 if (yy % 4 == 0) else 28
    if mm in (4, 6, 9, 11):
        return 30
    return 31


# ============================================================================
# %MACRO NXTBLDT: advance BLDATE by one payment period
#
#   IF PAYFREQ = '6' THEN DO;
#     DD = DAY(BLDATE) + 14   (bi-weekly)
#     MM = MONTH(BLDATE)
#     YY = YEAR(BLDATE)
#     If DD > LDAY(MM): overflow into next month
#   END
#   ELSE DO;
#     DD = DAY(ISSDTE)         (anchor to issue day)
#     MM = MONTH(BLDATE) + FREQ
#     YY = YEAR(BLDATE)
#     If MM > 12: roll year
#   END
#   Clamp DD to LDAY(MM)
#   BLDATE = MDY(MM,DD,YY)
# ============================================================================

def next_billing_date(bldate: date, payfreq: str, freq: int, issdte: date) -> date:
    """
    Replicate %MACRO NXTBLDT.

    payfreq='6' -> bi-weekly (+14 days), else monthly advance by freq months
    anchoring day to issue date day.
    """
    if payfreq == '6':
        dd = bldate.day + 14
        mm = bldate.month
        yy = bldate.year
        max_dd = days_in_month(mm, yy)
        if dd > max_dd:
            dd = dd - max_dd
            mm += 1
            if mm > 12:
                mm -= 12
                yy += 1
    else:
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year
        if mm > 12:
            mm -= 12
            yy += 1

    max_dd = days_in_month(mm, yy)
    if dd > max_dd:
        dd = max_dd
    return date(yy, mm, dd)


# ============================================================================
# %MACRO REMMTH: compute remaining months from REPTDATE to MATDT
#
#   MDYR  = YEAR(MATDT)
#   MDMTH = MONTH(MATDT)
#   MDDAY = DAY(MATDT)
#   If MDDAY > RPDAYS(RPMTH): MDDAY = RPDAYS(RPMTH)
#   REMY  = MDYR - RPYR
#   REMM  = MDMTH - RPMTH
#   REMD  = MDDAY - RPDAY
#   REMMTH = REMY*12 + REMM + REMD/RPDAYS(RPMTH)
# ============================================================================

def calc_remmth(matdt: date, rpyr: int, rpmth: int, rpday: int) -> float:
    """Replicate %MACRO REMMTH."""
    mdyr  = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    rpdays_rpmth = days_in_month(rpmth, rpyr)
    if mdday > rpdays_rpmth:
        mdday = rpdays_rpmth

    remy  = mdyr  - rpyr
    remm  = mdmth - rpmth
    remd  = mdday - rpday
    return remy * 12 + remm + remd / rpdays_rpmth


# ============================================================================
# SAS date helper
# ============================================================================

_SAS_EPOCH = date(1960, 1, 1)


def sas_to_date(val) -> date:
    """Convert SAS numeric date (days since 1960-01-01) or date object to date."""
    if isinstance(val, date):
        return val
    if val is None or val != val:     # NaN guard
        return None
    return _SAS_EPOCH + timedelta(days=int(val))


# ============================================================================
# STEP: GET REPTDATE
# DATA REPTDATE; SET BNM1.REPTDATE;
# ============================================================================

def load_reptdate() -> dict:
    """
    Load REPTDATE from parquet and derive macro variables:
      NOWK, REPTYEAR, REPTMON, REPTDAY, RDATE
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT * FROM read_parquet('{REPTDATE_PATH}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate = sas_to_date(row[0])

    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    return {
        "reptdate":  reptdate,
        "nowk":      nowk,
        "reptyear":  str(reptdate.year),
        "reptmon":   f"{reptdate.month:02d}",
        "reptday":   f"{reptdate.day:02d}",
        "rdate":     reptdate.strftime("%d%m%Y"),   # DDMMYY8.
        "rpyr":      reptdate.year,
        "rpmth":     reptdate.month,
        "rpday":     reptdate.day,
    }


# ============================================================================
# STEP: Load BNM1.BTRADmmww
# SET BNM1.BTRAD&REPTMON&NOWK
# e.g. BTRAD0314 for March, week 4
# ============================================================================

def load_btrad(reptmon: str, nowk: str) -> pl.DataFrame:
    """Load the BNM1.BTRADmmww parquet file."""
    filename = f"btrad{reptmon}{nowk}.parquet"
    path = BTRAD_PATTERN / filename
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    log.info("Loaded %s: %d rows", filename, len(df))
    return df


# ============================================================================
# PAYFREQ -> FREQ mapping
# SELECT (PAYFREQ):
#   WHEN ('1') FREQ = 1
#   WHEN ('2') FREQ = 3
#   WHEN ('3') FREQ = 6
#   WHEN ('4') FREQ = 12
# Note: In the DATA step PAYFREQ is hard-coded to '3', so FREQ = 6 always.
# The SELECT is retained for generality and fidelity to the SAS source.
# ============================================================================

_FREQ_MAP = {'1': 1, '2': 3, '3': 6, '4': 12}


def payfreq_to_freq(payfreq: str) -> int:
    """Map PAYFREQ code to integer month frequency."""
    return _FREQ_MAP.get(str(payfreq), 0)


# ============================================================================
# MAIN DATA STEP: DATA NOTE (KEEP=BNMCODE AMOUNT)
#
# Filters:
#   SUBSTR(PRODCD,1,2) = '34'  OR  PRODUCT IN (225,226)
#
# Logic per row:
#   CUSTCD -> CUST ('08' individual, '09' other)
#   PROD   = 'BT' (hardcoded; PRDFMT applied to PRODUCT for ITEM derivation)
#   ITEM   derived from CUST + PRDFMT(PRODUCT):
#     individual (08): HL -> '214', else -> '219'
#     other      (09): FL -> '211', RC -> '212', else -> '219'
#   DAYS   = REPTDATE - BLDATE
#   IF EXPRDATE - REPTDATE < 8: REMMTH = 0.1 (near-maturity shortcut)
#   ELSE: iterate repayment schedule outputting one row pair per instalment
#   Two BNMCODE rows per output point: '95'||ITEM||CUST||REMFMT||'0000Y'
#                                      '93'||ITEM||CUST||REMFMT||'0000Y'
#   For '93' row: if DAYS > 89, override REMMTH = 13 -> '06' bucket
# ============================================================================

def process_btrad(df: pl.DataFrame, env: dict) -> pl.DataFrame:
    """
    Replicate the DATA NOTE step producing (BNMCODE, AMOUNT) rows.
    PAYFREQ is hardcoded to '3' (FREQ=6) per the SAS DATA step.
    """
    reptdate = env["reptdate"]
    rpyr     = env["rpyr"]
    rpmth    = env["rpmth"]
    rpday    = env["rpday"]

    output_rows = []

    for r in df.to_dicts():
        prodcd  = str(r.get("PRODCD", "") or "").strip()
        product = int(r.get("PRODUCT", 0) or 0)

        # IF SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226)
        if not (prodcd[:2] == '34' or product in (225, 226)):
            continue

        custcd = str(r.get("CUSTCD", "") or "").strip()
        cust   = '08' if custcd in ('77', '78', '95', '96') else '09'

        # PROD = 'BT' (hardcoded); PRDFMT applied to PRODUCT for ITEM
        prod_type = prdfmt(product)

        if cust == '08':
            item = '214' if prod_type == 'HL' else '219'
        else:
            if prod_type == 'FL':
                item = '211'
            elif prod_type == 'RC':
                item = '212'
            else:
                item = '219'

        # Date fields
        bldate_raw   = r.get("BLDATE")
        exprdate_raw = r.get("EXPRDATE")
        issdte_raw   = r.get("ISSDTE")
        payamt_raw   = r.get("PAYAMT")

        bldate   = sas_to_date(bldate_raw)
        exprdate = sas_to_date(exprdate_raw)
        issdte   = sas_to_date(issdte_raw)

        payamt  = float(payamt_raw or 0)
        if payamt < 0:
            payamt = 0

        balance = float(r.get("BALANCE", 0) or 0)

        # DAYS = REPTDATE - BLDATE  (only if BLDATE > 0)
        days = (reptdate - bldate).days if bldate and bldate > date(1960, 1, 1) else 0

        # PAYFREQ hardcoded to '3' in DATA step -> FREQ = 6
        payfreq = '3'
        freq    = payfreq_to_freq(payfreq)

        # Near-maturity shortcut
        if exprdate and (exprdate - reptdate).days < 8:
            remmth = 0.1
            # Output final pair only (falls through to post-loop output below)
            amount = balance
            _emit_pair(output_rows, item, cust, remmth, amount, days)
            continue

        # ----------------------------------------------------------------
        # Advance BLDATE to first future billing date after REPTDATE
        # ----------------------------------------------------------------
        # RC / OD / products 350,910,925: use EXPRDATE directly
        if product in (350, 910, 925):
            bldate = exprdate
        elif not bldate or bldate <= date(1960, 1, 1):
            # BLDATE <= 0: start from ISSDTE, advance until > REPTDATE
            bldate = issdte
            if bldate:
                while bldate <= reptdate:
                    bldate = next_billing_date(bldate, payfreq, freq, issdte)

        if bldate and bldate > exprdate or balance <= payamt:
            bldate = exprdate

        # ----------------------------------------------------------------
        # Iterate repayment schedule
        # DO WHILE (BLDATE <= EXPRDATE)
        # ----------------------------------------------------------------
        if bldate and exprdate:
            while bldate <= exprdate:
                matdt  = bldate
                remmth = calc_remmth(matdt, rpyr, rpmth, rpday)

                # LEAVE if > 12 months remaining or at final date
                if remmth > 12 or bldate == exprdate:
                    break

                amount  = payamt
                balance = balance - payamt
                _emit_pair(output_rows, item, cust, remmth, amount, days)

                # Advance to next billing date
                bldate = next_billing_date(bldate, payfreq, freq, issdte)
                if bldate > exprdate or balance <= payamt:
                    bldate = exprdate

        # ----------------------------------------------------------------
        # Final output: residual balance at last MATDT
        # ----------------------------------------------------------------
        amount = balance
        _emit_pair(output_rows, item, cust, remmth, amount, days)

    if not output_rows:
        return pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64})

    return pl.DataFrame(output_rows)


def _emit_pair(
    output_rows: list,
    item: str,
    cust: str,
    remmth: float,
    amount: float,
    days: int,
) -> None:
    """
    Emit the two BNMCODE output rows for a given maturity point.

    BNMCODE = '95'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y'  -> row 1
    IF DAYS > 89 THEN REMMTH = 13                              -> overrides bucket for row 2
    BNMCODE = '93'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y'  -> row 2
    """
    rem_code = remfmt(remmth)

    # Row 1: '95' prefix
    output_rows.append({
        "BNMCODE": f"95{item}{cust}{rem_code}0000Y",
        "AMOUNT":  amount,
    })

    # Row 2: '93' prefix — if DAYS > 89 override REMMTH to 13 (-> '06' bucket)
    remmth_93 = 13.0 if days > 89 else remmth
    rem_code_93 = remfmt(remmth_93)
    output_rows.append({
        "BNMCODE": f"93{item}{cust}{rem_code_93}0000Y",
        "AMOUNT":  amount,
    })


# ============================================================================
# PROC SUMMARY NWAY: CLASS BNMCODE; VAR AMOUNT; OUTPUT SUM=
# ============================================================================

def summarise_by_bnmcode(note: pl.DataFrame) -> pl.DataFrame:
    """
    Replicate PROC SUMMARY DATA=NOTE NWAY; CLASS BNMCODE; VAR AMOUNT; OUTPUT SUM=
    Drops _TYPE_ and _FREQ_ (not produced by group_by).
    """
    return (
        note.group_by("BNMCODE")
            .agg(pl.col("AMOUNT").sum())
            .sort("BNMCODE")
    )


# ============================================================================
# PROC PRINT: SUM AMOUNT
# ============================================================================

def proc_print(df: pl.DataFrame) -> None:
    """Replicate PROC PRINT; SUM AMOUNT;"""
    log.info("BNM.BT PROC PRINT (rows=%d):", len(df))
    log.info("  %-22s  %20s", "BNMCODE", "AMOUNT")
    log.info("  %s", "-" * 46)
    total = 0.0
    for row in df.iter_rows(named=True):
        amt = float(row.get("AMOUNT", 0) or 0)
        total += amt
        log.info("  %-22s  %20.2f", row.get("BNMCODE", ""), amt)
    log.info("  %s", "-" * 46)
    log.info("  %-22s  %20.2f", "TOTAL", total)


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    """
    Main entry point replicating EIBMLIBT SAS program logic:
      1. Load REPTDATE and derive macro variables.
      2. Load BNM1.BTRADmmww trade data.
      3. Filter to PRODCD starting '34' or PRODUCT IN (225,226).
      4. Process repayment schedule to assign BNMCODE per maturity bucket.
      5. Summarise NOTE by BNMCODE (PROC SUMMARY NWAY SUM).
      6. Write BNM.BT output parquet and print.
    """
    log.info("EIBMLIB4 started.")

    # ----------------------------------------------------------------
    # GET REPTDATE
    # ----------------------------------------------------------------
    env = load_reptdate()
    log.info(
        "REPTDATE=%s  REPTMON=%s  NOWK=%s",
        env["reptdate"], env["reptmon"], env["nowk"],
    )

    # ----------------------------------------------------------------
    # Load BNM1.BTRADmmww
    # ----------------------------------------------------------------
    btrad = load_btrad(env["reptmon"], env["nowk"])

    # ----------------------------------------------------------------
    # DATA NOTE (KEEP=BNMCODE AMOUNT)
    # ----------------------------------------------------------------
    note = process_btrad(btrad, env)
    log.info("NOTE rows produced: %d", len(note))

    # ----------------------------------------------------------------
    # PROC SUMMARY NWAY: CLASS BNMCODE; VAR AMOUNT; OUTPUT SUM=
    # ----------------------------------------------------------------
    bnm_bt = summarise_by_bnmcode(note)
    log.info("BNM.BT summarised rows: %d", len(bnm_bt))

    # ----------------------------------------------------------------
    # Write BNM.BT output
    # ----------------------------------------------------------------
    bnm_bt.write_parquet(BNM_OUTPUT)
    log.info("BNM.BT written: %s", BNM_OUTPUT)

    # ----------------------------------------------------------------
    # PROC PRINT; SUM AMOUNT;
    # ----------------------------------------------------------------
    proc_print(bnm_bt)

    log.info("EIBMLIB4 completed.")


if __name__ == "__main__":
    main()
