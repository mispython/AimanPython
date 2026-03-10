#!/usr/bin/env python3
"""
Program  : EIIMNL40.py
Purpose  : INLF Report - Monthly-End Format (EIBMRLFM).
           Reads REPTDATE from LOAN.REPTDATE, loads loan data from
            BNM1.LOANmmww (month + week number), computes maturity run-off
            schedule per loan using REPTDATE as the base date, assigns BNMCODE
            per maturity bucket, adds inline EIR_ADJ rows, adjusts revolving
            credit, injects DEFAULT zero-amount rows, and summarises by BNMCODE
            into BNM.NOTE output parquet.

           Dependencies:
             %INC PGM(PBBLNFMT) ->
               LIQPFMT format is used via PUT(PRODUCT, LIQPFMT.) to classify
               products as 'FL', 'HL', 'RC'. LIQPFMT is not present in the
               attached PBBLNFMT.py conversion; a local approximation is
               implemented below (same as EIIDNLF0). All other PBBLNFMT
               formats are not directly called here.
             %INC PGM(PBBELF) ->
               Included as suite-wide boilerplate. No formats from PBBELF
               are directly invoked in this program. No import needed.
             %INC PGM(PBBDPFMT) ->
               Included as suite-wide boilerplate. No formats from PBBDPFMT
               are directly invoked in this program. No import needed.
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

# LOAN.REPTDATE
LOAN_REPTDATE_PATH = DATA_DIR / "loan" / "reptdate.parquet"

# BNM1.LOANmmww -- resolved at runtime using REPTMON + NOWK
BNM1_DIR = DATA_DIR / "bnm1"

# BNM.REPTDATE (written then read back within same run)
BNM_REPTDATE_PATH = OUTPUT_DIR / "bnm_reptdate.parquet"

# BNM.NOTE final output
BNM_NOTE_OUTPUT = OUTPUT_DIR / "bnm_note.parquet"

# %LET FCY products
FCY_PRODUCTS = frozenset([
    800, 801, 802, 803, 804,
    851, 852, 853, 854, 855, 856, 857, 858, 859, 860,
])

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
# SAS date helpers
# ============================================================================

_SAS_EPOCH = date(1960, 1, 1)


def sas_to_date(val) -> date | None:
    """Convert SAS numeric date (days since 1960-01-01) or date object to date."""
    if val is None:
        return None
    if isinstance(val, date):
        return val
    try:
        if float(val) != float(val):   # NaN
            return None
        return _SAS_EPOCH + timedelta(days=int(val))
    except (TypeError, ValueError):
        return None


# ============================================================================
# FORMAT: REMFMT
# VALUE REMFMT
#   LOW-0.1 = '01'   /*  UP TO 1 WK       */
#   0.1-1   = '02'   /*  >1 WK - 1 MTH    */
#   1-3     = '03'   /*  >1 MTH - 3 MTHS  */
#   3-6     = '04'   /*  >3 - 6 MTHS      */
#   6-12    = '05'   /*  >6 MTHS - 1 YR   */
#   OTHER   = '06';  /*  > 1 YEAR         */
# Note: threshold is 0.1 (differs from EIIDNLF0 which uses 0.255).
# No missing ('07') bucket in this program.
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
# FORMAT: LIQPFMT  (approximated - same definition as EIIDNLF0)
# Used via PUT(PRODUCT, LIQPFMT.) to classify products as 'FL', 'HL', 'RC'.
# LIQPFMT is absent from the attached PBBLNFMT.py; implemented locally.
# ============================================================================

_LIQPFMT_HL = frozenset([
    110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
    139, 140, 141, 142, 147, 173, 225, 226,
    400, 409, 410, 412, 413, 414, 415, 423, 431, 432, 433, 440,
    466, 472, 473, 474, 479, 484, 486, 489, 494,
    600, 638, 650, 651, 664, 677, 911,
    *range(200, 249), *range(250, 261),
])

_LIQPFMT_RC = frozenset([
    146, 184, 192, 195, 196, 302, 350, 351, 364, 365,
    495, 506, 604, 605, 634, 641, 660, 685, 689,
    802, 803, 806, 808, 810, 812, 814, 817, 818,
    856, 857, 858, 859, 860,
    902, 903, 910, 917, 925, 951,
])


def liqpfmt(product: int) -> str:
    """
    Approximate LIQPFMT format: classify product as 'HL', 'RC', or 'FL'.
    See module docstring for sourcing notes.
    """
    if product in _LIQPFMT_HL:
        return 'HL'
    if product in _LIQPFMT_RC:
        return 'RC'
    return 'FL'


# ============================================================================
# %MACRO DCLVAR: days-in-month helper
# ============================================================================

def days_in_month(mm: int, yy: int) -> int:
    """Return days in month mm of year yy (replicates LDAY/RPDAYS arrays)."""
    if mm == 2:
        return 29 if (yy % 4 == 0) else 28
    if mm in (4, 6, 9, 11):
        return 30
    return 31


# ============================================================================
# %MACRO NXTBLDT
# ============================================================================

def next_billing_date(bldate: date, payfreq: str, freq: int, issdte: date) -> date:
    """
    Replicate %MACRO NXTBLDT.
    payfreq='6' -> bi-weekly (+14 days), else monthly advance anchored to issdte.day.
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
        dd = issdte.day if issdte else bldate.day
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
# Base date is REPTDATE (rpyr/rpmth/rpday), unlike EIIDNLF0 which uses RUNOFFDT.
# ============================================================================

def calc_remmth(matdt: date, rpyr: int, rpmth: int, rpday: int) -> float:
    """Replicate %MACRO REMMTH."""
    mdyr  = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    rpdays_rpmth = days_in_month(rpmth, rpyr)
    if mdday > rpdays_rpmth:
        mdday = rpdays_rpmth

    remy = mdyr  - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / rpdays_rpmth


# ============================================================================
# PAYFREQ -> FREQ mapping
# ============================================================================

_FREQ_MAP = {'1': 1, '2': 3, '3': 6, '4': 12}


def payfreq_to_freq(payfreq: str) -> int:
    return _FREQ_MAP.get(str(payfreq).strip(), 0)


# ============================================================================
# ITEM derivation
# IF CUSTCD IN ('77','78','95','96') THEN CUST='08'; ELSE CUST='09'
# individual (08): HL -> '214', else -> '219'
# other      (09): FL/HL -> '211', RC -> '212', else -> '219'
# Note: No PRODUCT=100 override in this program (differs from EIIDNLF0).
# ============================================================================

def derive_item(prod_type: str, cust: str) -> str:
    """Derive ITEM code from product classification and customer type."""
    if cust == '08':
        return '214' if prod_type == 'HL' else '219'
    else:
        if prod_type in ('FL', 'HL'):
            return '211'
        elif prod_type == 'RC':
            return '212'
        return '219'


# ============================================================================
# STEP: GET REPTDATE
# DATA BNM.REPTDATE; SET LOAN.REPTDATE;
# ============================================================================

def load_reptdate() -> dict:
    """Load REPTDATE from LOAN.REPTDATE parquet and derive macro variables."""
    con = duckdb.connect()
    row = con.execute(
        f"SELECT * FROM read_parquet('{LOAN_REPTDATE_PATH}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate = sas_to_date(row[0])
    day  = reptdate.day
    nowk = '1' if day == 8 else '2' if day == 15 else '3' if day == 22 else '4'

    return {
        "reptdate":  reptdate,
        "nowk":      nowk,
        "reptyear":  str(reptdate.year),
        "reptmon":   f"{reptdate.month:02d}",
        "reptday":   f"{reptdate.day:02d}",
        "rdate":     reptdate.strftime("%d%m%Y"),
        "rpyr":      reptdate.year,
        "rpmth":     reptdate.month,
        "rpday":     reptdate.day,
    }


# ============================================================================
# MAIN DATA STEP: DATA NOTE (KEEP=BNMCODE AMOUNT)
#
# Key differences from EIIDNLF0:
#   - Filter: PAIDIND NOT IN ('P','C') OR EIR_ADJ NE . (OR logic)
#   - Base date for %REMMTH: REPTDATE (not RUNOFFDT)
#   - Near-maturity: EXPRDATE - REPTDATE < 8 (not EXPRDATE - RUNOFFDT)
#   - Inner LEAVE: REMMTH > 12 (not > 1)
#   - Extra clamp: IF REMMTH > 0.1 AND (BLDATE-REPTDATE) < 8 -> REMMTH = 0.1
#   - EIR_ADJ emitted inline after the main loan logic
#   - No PRODUCT=100 ITEM override
# ============================================================================

def process_loans(df: pl.DataFrame, env: dict) -> pl.DataFrame:
    """
    Replicate DATA NOTE step producing (BNMCODE, AMOUNT) rows.
    Base date is REPTDATE for all remaining-month calculations.
    EIR_ADJ rows are emitted inline at the end of each record's processing.
    """
    reptdate = env["reptdate"]
    rpyr     = env["rpyr"]
    rpmth    = env["rpmth"]
    rpday    = env["rpday"]

    output_rows = []

    for r in df.to_dicts():
        paidind = str(r.get("PAIDIND", "") or "").strip()
        eir_adj = r.get("EIR_ADJ")
        eir_is_missing = (eir_adj is None or
                          (isinstance(eir_adj, float) and eir_adj != eir_adj))

        # IF PAIDIND NOT IN ('P','C') OR EIR_ADJ NE .
        if paidind in ('P', 'C') and eir_is_missing:
            continue

        prodcd  = str(r.get("PRODCD", "") or "").strip()
        product = int(r.get("PRODUCT", 0) or 0)

        # IF SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226)
        if not (prodcd[:2] == '34' or product in (225, 226)):
            continue

        custcd = str(r.get("CUSTCD", "") or "").strip()
        cust   = '08' if custcd in ('77', '78', '95', '96') else '09'

        acctype  = str(r.get("ACCTYPE", "") or "").strip()
        bldate   = sas_to_date(r.get("BLDATE"))
        exprdate = sas_to_date(r.get("EXPRDATE"))
        issdte   = sas_to_date(r.get("ISSDTE"))
        payamt   = float(r.get("PAYAMT", 0) or 0)
        balance  = float(r.get("BALANCE", 0) or 0)
        payfreq  = str(r.get("PAYFREQ", "") or "").strip()
        loanstat = r.get("LOANSTAT")

        # DAYS = REPTDATE - BLDATE (if BLDATE > 0)
        days = (reptdate - bldate).days if bldate and bldate > date(1960, 1, 1) else 0

        # ----------------------------------------------------------------
        # OD accounts: fixed bucket, single output row (no '93' pair here)
        # IF ACCTYPE = 'OD'
        # ----------------------------------------------------------------
        if acctype == 'OD':
            remmth = 0.1
            amount = balance
            bnmcode = f"95213{cust}{remfmt(remmth)}0000Y"
            if prodcd == '34240':
                bnmcode = f"95219{cust}{remfmt(remmth)}0000Y"
            output_rows.append({"BNMCODE": bnmcode, "AMOUNT": amount})
            # OD records: EIR_ADJ emitted after OD output if applicable
            if not eir_is_missing:
                prod_type = liqpfmt(product)
                item_eir  = derive_item(prod_type, cust)
                output_rows.append({"BNMCODE": f"95{item_eir}{cust}060000Y", "AMOUNT": float(eir_adj)})
                output_rows.append({"BNMCODE": f"93{item_eir}{cust}060000Y", "AMOUNT": float(eir_adj)})
            continue

        # ----------------------------------------------------------------
        # LN accounts only
        # IF ACCTYPE = 'LN'
        # ----------------------------------------------------------------
        if acctype != 'LN':
            continue

        prod_type = liqpfmt(product)
        item      = derive_item(prod_type, cust)

        if payamt < 0:
            payamt = 0

        remmth = None   # initialise

        # ----------------------------------------------------------------
        # Near or past maturity (base: REPTDATE)
        # ----------------------------------------------------------------
        if exprdate and (exprdate - reptdate).days < 8:
            remmth = 0.1

        else:
            freq = payfreq_to_freq(payfreq)

            # Advance BLDATE to first future billing date
            if payfreq in ('5', '9', ' ', '') or product in (350, 910, 925):
                bldate = exprdate
            elif not bldate or bldate <= date(1960, 1, 1):
                bldate = issdte
                if bldate:
                    while bldate <= reptdate:
                        bldate = next_billing_date(bldate, payfreq, freq, issdte)

            if bldate and exprdate and (bldate > exprdate or balance <= payamt):
                bldate = exprdate

            # ----------------------------------------------------------------
            # DO WHILE (BLDATE <= EXPRDATE) - iterate instalment schedule
            # LEAVE: REMMTH > 12 OR BLDATE = EXPRDATE
            # ----------------------------------------------------------------
            if bldate and exprdate:
                while bldate <= exprdate:
                    matdt  = bldate
                    remmth = calc_remmth(matdt, rpyr, rpmth, rpday)

                    # LEAVE if > 12 months remaining or at final expiry date
                    if remmth > 12 or bldate == exprdate:
                        break

                    # Extra near-maturity clamp inside loop:
                    # IF REMMTH > 0.1 AND (BLDATE-REPTDATE) < 8 THEN REMMTH=0.1
                    if remmth > 0.1 and (bldate - reptdate).days < 8:
                        remmth = 0.1

                    amount  = payamt
                    balance = balance - payamt

                    # RM output (FCY commented out in SAS)
                    if product not in FCY_PRODUCTS:
                        output_rows.append({
                            "BNMCODE": f"95{item}{cust}{remfmt(remmth)}0000Y",
                            "AMOUNT":  amount,
                        })
                    #   /*  IF PRODUCT IN &FCY THEN DO;
                    #          BNMCODE = '94'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
                    #       OUTPUT;
                    #   END; */

                    remmth_93 = 13.0 if (days > 89 or (loanstat is not None and loanstat != 1)) else remmth

                    if product not in FCY_PRODUCTS:
                        output_rows.append({
                            "BNMCODE": f"93{item}{cust}{remfmt(remmth_93)}0000Y",
                            "AMOUNT":  amount,
                        })
                    #   /*  IF PRODUCT IN &FCY THEN DO;
                    #          BNMCODE = '96'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
                    #       OUTPUT;
                    #   END; */

                    # Advance billing date
                    bldate = next_billing_date(bldate, payfreq, freq, issdte)
                    if bldate and exprdate and (bldate > exprdate or balance <= payamt):
                        bldate = exprdate

        # ----------------------------------------------------------------
        # Final residual balance output
        # AMOUNT = BALANCE
        # ----------------------------------------------------------------
        amount = balance

        if product not in FCY_PRODUCTS:
            output_rows.append({
                "BNMCODE": f"95{item}{cust}{remfmt(remmth)}0000Y",
                "AMOUNT":  amount,
            })
        #   /* IF PRODUCT IN &FCY THEN DO;
        #         BNMCODE = '94'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
        #      OUTPUT;
        #   END; */

        remmth_93_final = 13.0 if (days > 89 or (loanstat is not None and loanstat != 1)) else remmth

        if product not in FCY_PRODUCTS:
            output_rows.append({
                "BNMCODE": f"93{item}{cust}{remfmt(remmth_93_final)}0000Y",
                "AMOUNT":  amount,
            })
        #   /* IF PRODUCT IN &FCY THEN DO;
        #         BNMCODE = '96'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
        #      OUTPUT;
        #   END; */

        # ----------------------------------------------------------------
        # EIR_ADJ inline output (after main loan rows)
        # IF EIR_ADJ NE . THEN DO;
        # ----------------------------------------------------------------
        if not eir_is_missing:
            output_rows.append({"BNMCODE": f"95{item}{cust}060000Y", "AMOUNT": float(eir_adj)})
            output_rows.append({"BNMCODE": f"93{item}{cust}060000Y", "AMOUNT": float(eir_adj)})

    if not output_rows:
        return pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64})
    return pl.DataFrame(output_rows)


# ============================================================================
# PROC PRINT DATA=NOTE WHERE SUBSTR(BNMCODE,1,2) IN ('94','96')
# FCY rows are commented out in the original SAS DATA step; expected empty.
# ============================================================================

def print_fcy(note: pl.DataFrame) -> None:
    """Print FCY rows (prefix 94 or 96) - expected empty per commented-out SAS."""
    fcy_rows = note.filter(pl.col("BNMCODE").str.slice(0, 2).is_in(["94", "96"]))
    log.info("FCY PROC PRINT (rows=%d) - expected 0 (FCY output commented in SAS):", len(fcy_rows))
    for row in fcy_rows.iter_rows(named=True):
        log.info("  %s  %.2f", row["BNMCODE"], float(row["AMOUNT"] or 0))


# ============================================================================
# REVOLVING CREDIT adjustments (identical logic to EIIDNLF0):
#   1. Delete all '93212' rows from NOTE.
#   2. For each '95212' row create a matching '93212' row.
#   3. Append back to NOTE.
# ============================================================================

def adjust_revolving_credit(note: pl.DataFrame) -> pl.DataFrame:
    """
    Replicate RC adjustment steps:
      1. DELETE rows where BNMCODE starts '93212'
      2. For each '95212' row create '93212' counterpart
         (BNMCODE = '93212' || SUBSTR(BNMCODE1, 6, 9))
      3. Append back to NOTE
    """
    # Step 1: drop existing '93212' rows
    note = note.filter(~pl.col("BNMCODE").str.starts_with("93212"))

    # Step 2: extract '95212' rows and create '93212' counterparts
    rccorp = note.filter(pl.col("BNMCODE").str.starts_with("95212"))
    rccorp1 = rccorp.with_columns(
        pl.concat_str([
            pl.lit("93212"),
            pl.col("BNMCODE").str.slice(5, 9),   # SUBSTR(BNMCODE1,6,9) -> 0-indexed 5
        ]).alias("BNMCODE")
    )

    # Step 3: append
    return pl.concat([note, rccorp1])


# ============================================================================
# DEFAULTING: generate zero-amount seed rows for all expected BNMCODE buckets
# DATA DEFAULT: DO WHILE (N < 7); N = 1 to 6
# ============================================================================

def build_defaults() -> pl.DataFrame:
    """
    Replicate DATA DEFAULT step.
    Emits AMOUNT=0 rows for every expected RM and FCY BNMCODE prefix x bucket (1-6).
    """
    rm_prefixes = [
        '93211090', '95211090',
        '93212090', '95212090',
        '93213080', '95213080',
        '93213090', '95213090',
        '93214080', '95214080',
        '93215080', '95215080',
        '93219080', '95219080',
        '93219090', '95219090',
    ]
    fcy_prefixes = [
        '94211090', '96211090',
        '94212090', '96212090',
        '94213080', '96213080',
        '94213090', '96213090',
        '94214080', '96214080',
        '94215080', '96215080',
        '94219080', '96219080',
        '94219090', '96219090',
    ]

    rows = []
    for n in range(1, 7):   # N = 1 to 6 (DO WHILE N < 7, N+1)
        for pfx in rm_prefixes + fcy_prefixes:
            rows.append({"BNMCODE": f"{pfx}{n}0000Y", "AMOUNT": 0.0})

    return pl.DataFrame(rows).sort("BNMCODE")


# ============================================================================
# DEFAULT merge: MERGE DEFAULT NOTE BY BNMCODE
# Unlike EIIDNLF0 (SET concat), this uses a left-biased merge to mirror
# SAS MERGE BY which keeps all keys from both datasets, summing is done
# later by PROC SUMMARY. The MERGE here just unions all rows by BNMCODE key.
# Since PROC SUMMARY SUM= follows immediately, behaviour is equivalent to
# a SET concat (all rows combined then summed).
# ============================================================================

def merge_default_note(default_df: pl.DataFrame, note: pl.DataFrame) -> pl.DataFrame:
    """
    Replicate DATA NOTE; MERGE DEFAULT NOTE; BY BNMCODE.
    SAS MERGE BY with no conditional logic simply unions all rows from both
    datasets (interleaved by BNMCODE). PROC SUMMARY SUM= follows, making
    the merge equivalent to a concat for summation purposes.
    """
    return pl.concat([default_df, note]).sort("BNMCODE")


# ============================================================================
# PROC SUMMARY NWAY: CLASS BNMCODE; VAR AMOUNT; OUTPUT SUM=
# ============================================================================

def summarise_by_bnmcode(note: pl.DataFrame) -> pl.DataFrame:
    """Replicate PROC SUMMARY DATA=NOTE NWAY; CLASS BNMCODE; VAR AMOUNT; OUTPUT SUM="""
    return (
        note.group_by("BNMCODE")
            .agg(pl.col("AMOUNT").sum())
            .sort("BNMCODE")
    )


# ============================================================================
# PROC PRINT DATA=BNM.NOTE
# ============================================================================

def proc_print(df: pl.DataFrame) -> None:
    """Replicate PROC PRINT DATA=BNM.NOTE; RUN;"""
    log.info("BNM.NOTE PROC PRINT (rows=%d):", len(df))
    log.info("  %-22s  %20s", "BNMCODE", "AMOUNT")
    log.info("  %s", "-" * 46)
    for row in df.iter_rows(named=True):
        log.info("  %-22s  %20.2f", row["BNMCODE"], float(row["AMOUNT"] or 0))


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    """
    Main entry point replicating EIIMNLF0 SAS program logic:
      1.  Load REPTDATE (LOAN.REPTDATE) -> BNM.REPTDATE
      2.  Load BNM1.LOANmmww (REPTMON + NOWK)
      3.  Process loan run-off schedule -> NOTE (BNMCODE, AMOUNT)
          including inline EIR_ADJ rows
      4.  PROC PRINT FCY rows (expected empty)
      5.  Adjust revolving credit (93212 / 95212)
      6.  Build DEFAULT zero-amount rows
      7.  MERGE DEFAULT + NOTE by BNMCODE
      8.  PROC SUMMARY NWAY -> BNM.NOTE
      9.  PROC PRINT BNM.NOTE
    """
    log.info("EIIMNL40 started.")

    # ----------------------------------------------------------------
    # 1. Load REPTDATE
    # ----------------------------------------------------------------
    env = load_reptdate()
    reptdate = env["reptdate"]
    log.info("REPTDATE=%s  REPTMON=%s  NOWK=%s",
             reptdate, env["reptmon"], env["nowk"])

    # Write BNM.REPTDATE (DATA BNM.REPTDATE; SET LOAN.REPTDATE)
    pl.DataFrame({"REPTDATE": [reptdate]}).write_parquet(BNM_REPTDATE_PATH)
    log.info("BNM.REPTDATE written: %s", BNM_REPTDATE_PATH)

    # ----------------------------------------------------------------
    # 2. Load BNM1.LOANmmww
    # SET BNM1.LOAN&REPTMON&NOWK
    # ----------------------------------------------------------------
    loan_file = BNM1_DIR / f"loan{env['reptmon']}{env['nowk']}.parquet"
    con = duckdb.connect()
    loan_df = con.execute(f"SELECT * FROM read_parquet('{loan_file}')").pl()
    con.close()
    log.info("BNM1.LOAN loaded: %d rows from %s", len(loan_df), loan_file.name)

    # ----------------------------------------------------------------
    # 3. DATA NOTE: process loan run-off schedule (includes EIR_ADJ inline)
    # ----------------------------------------------------------------
    note = process_loans(loan_df, env)
    log.info("NOTE rows produced: %d", len(note))

    # ----------------------------------------------------------------
    # 4. PROC PRINT FCY (expected empty)
    # ----------------------------------------------------------------
    print_fcy(note)

    # ----------------------------------------------------------------
    # 5. Revolving credit adjustment
    # ----------------------------------------------------------------
    note = adjust_revolving_credit(note)
    log.info("NOTE rows after RC adjustment: %d", len(note))

    # ----------------------------------------------------------------
    # 6 & 7. Build DEFAULT rows and merge
    # PROC SORT DATA=DEFAULT; BY BNMCODE
    # PROC SORT DATA=NOTE;    BY BNMCODE
    # DATA NOTE; MERGE DEFAULT NOTE; BY BNMCODE
    # ----------------------------------------------------------------
    default_df = build_defaults()
    note       = merge_default_note(default_df, note)
    log.info("NOTE rows after DEFAULT merge: %d", len(note))

    # ----------------------------------------------------------------
    # 8. PROC SUMMARY NWAY -> BNM.NOTE
    # ----------------------------------------------------------------
    bnm_note = summarise_by_bnmcode(note)
    log.info("BNM.NOTE summarised rows: %d", len(bnm_note))

    bnm_note.write_parquet(BNM_NOTE_OUTPUT)
    log.info("BNM.NOTE written: %s", BNM_NOTE_OUTPUT)

    # ----------------------------------------------------------------
    # 9. PROC PRINT DATA=BNM.NOTE
    # ----------------------------------------------------------------
    proc_print(bnm_note)

    log.info("EIIMNL40 completed.")


if __name__ == "__main__":
    main()
