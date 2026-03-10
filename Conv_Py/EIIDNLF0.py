#!/usr/bin/env python3
"""
Program  : EIIDNLF0.py
Purpose  : NLF Report - Contractual Run-offs as of End of Month (Run-Off Format).
           Reads REPTDATE from LOAN.REPTDATE and LOAN1.REPTDATE, loads loan data
            from BNM1.LOANmmdd, computes maturity run-off schedule per loan,
            assigns BNMCODE per maturity bucket, adds EIR adjustments, injects
            DEFAULT zero-amount rows for all expected BNMCODE combinations, and
            summarises by BNMCODE into BNM.NOTE output parquet.

           Dependencies:
             %INC PGM(PBBLNFMT) ->
               LIQPFMT format is used via PUT(PRODUCT, LIQPFMT.) to classify
               products as 'FL', 'HL', 'RC', 'OD' etc.
               LIQPFMT is NOT present in the attached PBBLNFMT.py conversion.
               It is likely defined in a separate SAS format library in the
               original suite. A local approximation (liqpfmt()) is implemented
               below based on contextual usage in this program.
               All other PBBLNFMT formats (format_lndenom, format_lnprod, etc.)
               are not directly called here.
             %INC PGM(PBBDPFMT) ->
               Included as suite-wide boilerplate. No formats from PBBDPFMT
               are directly invoked in this program. No import needed.
             %INC PGM(PBBELF) ->
               Included as suite-wide boilerplate. No formats from PBBELF
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
BASE_DIR       = Path(".")
DATA_DIR       = BASE_DIR / "data"
OUTPUT_DIR     = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# LOAN.REPTDATE  (primary reporting date)
LOAN_REPTDATE_PATH  = DATA_DIR / "loan"  / "reptdate.parquet"
# LOAN1.REPTDATE (weekly date for EIR_ADJ lookup)
LOAN1_REPTDATE_PATH = DATA_DIR / "loan1" / "reptdate.parquet"

# BNM1.LOANmmdd  -- resolved at runtime using REPTMON + REPTDAY
BNM1_DIR = DATA_DIR / "bnm1"

# BNM2.LOANmmww  -- resolved at runtime using WKMON + WKNOWK
BNM2_DIR = DATA_DIR / "bnm2"

# BNM.REPTDATE output (also used within same run)
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
#   LOW-0.255 = '01'   /*  UP TO 1 WK       */
#   0.255-1   = '02'   /*  >1 WK - 1 MTH    */
#   1-3       = '03'   /*  >1 MTH - 3 MTHS  */
#   3-6       = '04'   /*  >3 - 6 MTHS      */
#   6-12      = '05'   /*  >6 MTHS - 1 YR   */
#   .         = '07'   /*  MISSING          */
#   OTHER     = '06';  /*  > 1 YEAR         */
# Note: threshold changed to 0.255 (vs 0.1 in EIBMFAC4/EIBMLIB4).
# ============================================================================

def remfmt(remmth) -> str:
    """Apply REMFMT format to remaining months value."""
    if remmth is None or (isinstance(remmth, float) and remmth != remmth):
        return '07'   # missing -> '07'
    if remmth <= 0.255:
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
# FORMAT: LIQPFMT  (approximated from contextual usage)
# Used as PUT(PRODUCT, LIQPFMT.) to classify products for ITEM derivation.
# LIQPFMT is not present in PBBLNFMT.py; defined here based on the SAS
# program's branching logic which expects values: 'HL', 'FL', 'RC', 'OD'.
#
# Mapping derived from PBBLNFMT.PRDFMT-equivalent and standard product ranges:
#   Housing Loans (HL): 110-119,139-142,147,173,200-248,400,409,410,412-415,
#                       423,431-433,440,466,472-474,479,484,486,489,494,
#                       600,638,650,651,664,677,911 and Islamic HL
#   Revolving Credit (RC): 146,184,192,195,196,302,350,351,364,365,506,495,
#                          604,605,634,641,660,685,689,802-818,856-860,
#                          902,903,910,917,925,951
#   Floor/Leasing/Other -> 'FL'
# The SAS suite uses '34' prefix PRODCD as the primary filter; LIQPFMT
# provides a secondary classification. Products 225,226 are treated as HL
# per the filter condition and PRDFMT patterns in PBBLNFMT.
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
# %MACRO REMMTH: compute remaining months from RUNOFFDT to MATDT
# ============================================================================

def calc_remmth(matdt: date, rpyr: int, rpmth: int, rpday: int) -> float:
    """Replicate %MACRO REMMTH (base date is RUNOFFDT, not REPTDATE here)."""
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
# STEP: GET REPTDATE  (primary + weekly)
# ============================================================================

def load_reptdate(path: Path) -> dict:
    """Load a REPTDATE parquet and derive week/month/day macro vars."""
    con = duckdb.connect()
    row = con.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 1").fetchone()
    con.close()

    reptdate = sas_to_date(row[0])
    day = reptdate.day
    nowk = '1' if day == 8 else '2' if day == 15 else '3' if day == 22 else '4'

    return {
        "reptdate":  reptdate,
        "nowk":      nowk,
        "reptyear":  str(reptdate.year),
        "reptyr":    reptdate.strftime("%y"),
        "reptmon":   f"{reptdate.month:02d}",
        "reptday":   f"{reptdate.day:02d}",
        "rdate":     reptdate.strftime("%d%m%Y"),
        "rpyr":      reptdate.year,
        "rpmth":     reptdate.month,
        "rpday":     reptdate.day,
    }


# ============================================================================
# STEP: DETERMINE RUNOFFDT
# RUNOFFDT = MDY(RPMTH, RPDAYS(RPMTH), RPYR)  -> last day of reporting month
# ============================================================================

def compute_runoffdt(reptdate: date) -> date:
    """
    Replicate DATA _NULL_ RUNOFFDT computation:
      RUNOFFDT = MDY(RPMTH, RPDAYS(RPMTH), RPYR) = last day of reporting month
    """
    rpyr  = reptdate.year
    rpmth = reptdate.month
    last_day = days_in_month(rpmth, rpyr)
    return date(rpyr, rpmth, last_day)


# ============================================================================
# PAYFREQ -> FREQ mapping
# ============================================================================

_FREQ_MAP = {'1': 1, '2': 3, '3': 6, '4': 12}


def payfreq_to_freq(payfreq: str) -> int:
    return _FREQ_MAP.get(str(payfreq).strip(), 0)


# ============================================================================
# ITEM derivation helper
# IF CUSTCD IN ('77','78','95','96') THEN CUST = '08'; ELSE CUST = '09'
# individual (08): HL -> '214', else -> '219'
# other      (09): FL/HL -> '211', RC -> '212', else -> '219'
# PRODUCT = 100 -> ITEM = '212' (hardcoded - ABBA HL Financing)
# ============================================================================

def derive_item(prod_type: str, cust: str, product: int) -> str:
    """Derive ITEM code from product classification, customer type, and product."""
    if cust == '08':
        item = '214' if prod_type == 'HL' else '219'
    else:
        if prod_type in ('FL', 'HL'):
            item = '211'
        elif prod_type == 'RC':
            item = '212'
        else:
            item = '219'

    # HARDCORD BY MAZNI - ABBA HL FINANCING
    if product == 100:
        item = '212'

    return item


# ============================================================================
# STEP: DATA NOTEX / DATA NOTE
# Process loan data to produce (BNMCODE, AMOUNT) rows
# ============================================================================

def process_loans(df: pl.DataFrame, env: dict, runoffdt: date) -> pl.DataFrame:
    """
    Replicate DATA NOTEX + DATA NOTE steps.

    Filters: PAIDIND NOT IN ('P','C'), PRODCD starts '34' or PRODUCT IN (225,226).
    Computes BNMCODE per maturity schedule bucket for each loan account.
    FCY product output rows are commented out in the SAS source - preserved as
    comments below; only RM rows are emitted.
    """
    reptdate = env["reptdate"]

    # RUNOFFDT components for %REMMTH base (note: SAS uses RUNOFFDT not REPTDATE here)
    ro_yr  = runoffdt.year
    ro_mth = runoffdt.month
    ro_day = runoffdt.day

    # DAYS base is still REPTDATE (DAYS = REPTDATE - BLDATE per SAS)
    output_rows = []

    for r in df.to_dicts():
        paidind = str(r.get("PAIDIND", "") or "").strip()
        if paidind in ('P', 'C'):
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
        eir_adj  = r.get("EIR_ADJ")

        # DAYS = REPTDATE - BLDATE (if BLDATE > 0)
        days = (reptdate - bldate).days if bldate and bldate > date(1960, 1, 1) else 0

        # ----------------------------------------------------------------
        # OD accounts: fixed REMMTH = 0.1, single output pair
        # IF ACCTYPE = 'OD'
        # ----------------------------------------------------------------
        if acctype == 'OD':
            remmth = 0.1
            amount = balance
            bnmcode = f"95213{cust}{remfmt(remmth)}0000Y"
            # IF PRODCD='34240' override
            if prodcd == '34240':
                bnmcode = f"95219{cust}{remfmt(remmth)}0000Y"
            output_rows.append({"BNMCODE": bnmcode, "AMOUNT": amount})
            continue

        # ----------------------------------------------------------------
        # LN accounts only beyond this point
        # IF ACCTYPE = 'LN'
        # ----------------------------------------------------------------
        if acctype != 'LN':
            continue

        prod_type = liqpfmt(product)
        item = derive_item(prod_type, cust, product)

        if payamt < 0:
            payamt = 0

        # ----------------------------------------------------------------
        # Near or past maturity checks (using RUNOFFDT as base)
        # ----------------------------------------------------------------
        remmth = None   # SAS missing (.)

        if exprdate and exprdate <= runoffdt:
            remmth = None   # REMMTH = .  (past maturity relative to run-off date)

        elif exprdate and (exprdate - runoffdt).days < 8:
            remmth = 0.1

        else:
            # ----------------------------------------------------------------
            # Advance BLDATE to first future billing date
            # PAYFREQ IN ('5','9',' ') OR PRODUCT IN (350,910,925) -> use EXPRDATE
            # ----------------------------------------------------------------
            freq = payfreq_to_freq(payfreq)

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
            # ----------------------------------------------------------------
            if bldate and exprdate:
                while bldate <= exprdate:
                    # Determine REMMTH for this billing date
                    if bldate <= runoffdt:
                        remmth = None       # REMMTH = .
                    elif (bldate - runoffdt).days < 8:
                        remmth = 0.1
                    else:
                        matdt  = bldate
                        remmth = calc_remmth(matdt, ro_yr, ro_mth, ro_day)

                    # LEAVE if > 1 month remaining or at final expiry date
                    if (remmth is not None and remmth > 1) or bldate == exprdate:
                        break

                    amount  = payamt
                    balance = balance - payamt

                    # Overrides for impaired / delinquent loans
                    remmth_95 = remmth
                    if days > 89 or (loanstat is not None and loanstat != 1):
                        remmth_95 = 0.1

                    # RM output rows (FCY rows are commented out in SAS source)
                    if product not in FCY_PRODUCTS:
                        output_rows.append({
                            "BNMCODE": f"95{item}{cust}{remfmt(remmth_95)}0000Y",
                            "AMOUNT":  amount,
                        })
                    # else:
                    #   FCY rows omitted - commented out in original SAS:
                    #   * IF PROD='FL' THEN ITEM= 211;
                    #   * IF PROD='RC' THEN ITEM= 212;
                    #   * BNMCODE = '96'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
                    #   * OUTPUT;

                    remmth_93 = 13.0 if (days > 89 or (loanstat is not None and loanstat != 1)) else remmth

                    if product not in FCY_PRODUCTS:
                        output_rows.append({
                            "BNMCODE": f"93{item}{cust}{remfmt(remmth_93)}0000Y",
                            "AMOUNT":  amount,
                        })
                    # else:
                    #   * BNMCODE = '94'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
                    #   * OUTPUT;

                    # Advance billing date
                    bldate = next_billing_date(bldate, payfreq, freq, issdte)
                    if bldate and exprdate and (bldate > exprdate or balance <= payamt):
                        bldate = exprdate

        # ----------------------------------------------------------------
        # Final output: residual balance
        # ----------------------------------------------------------------
        amount = balance
        remmth_95 = 0.1 if (days > 89 or (loanstat is not None and loanstat != 1)) else remmth
        remmth_93 = 13.0 if (days > 89 or (loanstat is not None and loanstat != 1)) else remmth

        if product not in FCY_PRODUCTS:
            output_rows.append({
                "BNMCODE": f"95{item}{cust}{remfmt(remmth_95)}0000Y",
                "AMOUNT":  amount,
            })
            output_rows.append({
                "BNMCODE": f"93{item}{cust}{remfmt(remmth_93)}0000Y",
                "AMOUNT":  amount,
            })
        # else:
        #   * BNMCODE = '96'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
        #   * OUTPUT;
        #   * BNMCODE = '94'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
        #   * OUTPUT;

    if not output_rows:
        return pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64})
    return pl.DataFrame(output_rows)


# ============================================================================
# PROC PRINT DATA=NOTE WHERE SUBSTR(BNMCODE,1,2) IN ('94','96'); TITLE 'FCY'
# FCY rows are commented out in the original SAS DATA step, so this print
# will always produce an empty result. Retained for fidelity.
# ============================================================================

def print_fcy(note: pl.DataFrame) -> None:
    """Print FCY rows (prefix 94 or 96) - expected empty per commented-out SAS."""
    fcy_rows = note.filter(pl.col("BNMCODE").str.slice(0, 2).is_in(["94", "96"]))
    log.info("FCY PROC PRINT (rows=%d) - expected 0 (FCY output commented in SAS):", len(fcy_rows))
    for row in fcy_rows.iter_rows(named=True):
        log.info("  %s  %.2f", row["BNMCODE"], float(row["AMOUNT"] or 0))


# ============================================================================
# Drop rows where SUBSTR(BNMCODE,8,2) = '07'  (missing REMMTH bucket)
# DATA NOTE; SET NOTE; IF SUBSTR(BNMCODE,8,2) NE '07';
# ============================================================================

def drop_missing_remmth(note: pl.DataFrame) -> pl.DataFrame:
    """Remove rows where BNMCODE positions 8-9 (0-indexed 7-8) equal '07'."""
    return note.filter(pl.col("BNMCODE").str.slice(7, 2) != "07")


# ============================================================================
# REVOLVING CREDIT adjustments:
#   Delete all '93212' rows from NOTE.
#   For each '95212' row create a matching '93212' row.
# ============================================================================

def adjust_revolving_credit(note: pl.DataFrame) -> pl.DataFrame:
    """
    Replicate RC adjustment steps:
      1. DELETE rows where BNMCODE starts '93212'
      2. For each '95212' row, create a '93212' counterpart
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
# ADD IN EIR_ADJ
# DATA ADDEIR: reads BNM2.LOANmmww, filters LN accounts, emits EIR_ADJ rows
# BNMCODE = '95'||ITEM||CUST||'060000Y'  and  '93' variant
# ============================================================================

def build_eir_adj(path: Path) -> pl.DataFrame:
    """
    Replicate DATA ADDEIR step.
    Loads BNM2.LOANmmww, filters ACCTYPE='LN', emits two BNMCODE rows per
    account where EIR_ADJ is not missing, using fixed bucket '06' (>1 year).
    """
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()

    output_rows = []
    for r in df.to_dicts():
        if str(r.get("ACCTYPE", "") or "").strip() != 'LN':
            continue

        eir_adj = r.get("EIR_ADJ")
        if eir_adj is None or (isinstance(eir_adj, float) and eir_adj != eir_adj):
            continue

        custcd    = str(r.get("CUSTCD", "") or "").strip()
        cust      = '08' if custcd in ('77', '78', '95', '96') else '09'
        product   = int(r.get("PRODUCT", 0) or 0)
        prod_type = liqpfmt(product)
        item      = derive_item(prod_type, cust, product)
        amount    = float(eir_adj)

        output_rows.append({"BNMCODE": f"95{item}{cust}060000Y", "AMOUNT": amount})
        output_rows.append({"BNMCODE": f"93{item}{cust}060000Y", "AMOUNT": amount})

    if not output_rows:
        return pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64})
    return pl.DataFrame(output_rows)


# ============================================================================
# DEFAULTING: generate zero-amount rows for all expected BNMCODE combinations
# DATA DEFAULT: DO WHILE (N < 7); N = 1 to 6; N+1
# Ensures all buckets exist in the PROC SUMMARY output even if no data.
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
    Main entry point replicating EIIDNLF0 SAS program logic:
      1.  Load primary REPTDATE (LOAN.REPTDATE) -> BNM.REPTDATE
      2.  Load weekly REPTDATE (LOAN1.REPTDATE) -> WKMON, WKNOWK
      3.  Compute RUNOFFDT (last day of reporting month)
      4.  Load BNM1.LOANmmdd, filter PAIDIND, sort by ACCTNO
      5.  Process loan run-off schedule -> NOTE (BNMCODE, AMOUNT)
      6.  PROC PRINT FCY rows (expected empty)
      7.  Drop missing REMMTH rows (bucket '07')
      8.  Adjust revolving credit (93212 / 95212)
      9.  Add EIR_ADJ rows from BNM2.LOANmmww
      10. Build DEFAULT zero-amount rows
      11. Merge DEFAULT + NOTE
      12. PROC SUMMARY NWAY -> BNM.NOTE
      13. PROC PRINT BNM.NOTE
    """
    log.info("EIIDNLF0 started.")

    # ----------------------------------------------------------------
    # 1. Load primary REPTDATE
    # ----------------------------------------------------------------
    env = load_reptdate(LOAN_REPTDATE_PATH)
    reptdate = env["reptdate"]
    log.info("Primary REPTDATE=%s  REPTMON=%s  REPTDAY=%s  NOWK=%s",
             reptdate, env["reptmon"], env["reptday"], env["nowk"])

    # Write BNM.REPTDATE (DATA BNM.REPTDATE; SET LOAN.REPTDATE)
    bnm_reptdate_df = pl.DataFrame({"REPTDATE": [reptdate]})
    bnm_reptdate_df.write_parquet(BNM_REPTDATE_PATH)
    log.info("BNM.REPTDATE written: %s", BNM_REPTDATE_PATH)

    # ----------------------------------------------------------------
    # 2. Load weekly REPTDATE (LOAN1.REPTDATE)
    # ----------------------------------------------------------------
    wk_env = load_reptdate(LOAN1_REPTDATE_PATH)
    wkmon  = wk_env["reptmon"]
    wknowk = wk_env["nowk"]
    log.info("Weekly REPTDATE=%s  WKMON=%s  WKNOWK=%s",
             wk_env["reptdate"], wkmon, wknowk)

    # ----------------------------------------------------------------
    # 3. Compute RUNOFFDT
    # ----------------------------------------------------------------
    runoffdt = compute_runoffdt(reptdate)
    log.info("RUNOFFDT=%s", runoffdt)

    # ----------------------------------------------------------------
    # 4. Load BNM1.LOANmmdd and filter PAIDIND NOT IN ('P','C')
    # DATA NOTEX; SET BNM1.LOAN&REPTMON&REPTDAY; IF PAIDIND NOT IN ('P','C')
    # PROC SORT; BY ACCTNO
    # ----------------------------------------------------------------
    loan_file = BNM1_DIR / f"loan{env['reptmon']}{env['reptday']}.parquet"
    con = duckdb.connect()
    notex = con.execute(f"SELECT * FROM read_parquet('{loan_file}')").pl()
    con.close()
    notex = notex.filter(~pl.col("PAIDIND").is_in(["P", "C"])).sort("ACCTNO")
    log.info("NOTEX rows (after PAIDIND filter): %d", len(notex))

    # ----------------------------------------------------------------
    # 5. DATA NOTE: process loan run-off schedule
    # ----------------------------------------------------------------
    note = process_loans(notex, env, runoffdt)
    log.info("NOTE rows produced: %d", len(note))

    # ----------------------------------------------------------------
    # 6. PROC PRINT FCY
    # ----------------------------------------------------------------
    print_fcy(note)

    # ----------------------------------------------------------------
    # 7. Drop missing REMMTH rows (SUBSTR(BNMCODE,8,2) NE '07')
    # ----------------------------------------------------------------
    note = drop_missing_remmth(note)
    log.info("NOTE rows after drop missing REMMTH: %d", len(note))

    # ----------------------------------------------------------------
    # 8. Revolving credit adjustment
    # ----------------------------------------------------------------
    note = adjust_revolving_credit(note)
    log.info("NOTE rows after RC adjustment: %d", len(note))

    # ----------------------------------------------------------------
    # 9. Add EIR_ADJ from BNM2.LOANmmww
    # ----------------------------------------------------------------
    eir_file = BNM2_DIR / f"loan{wkmon}{wknowk}.parquet"
    addeir   = build_eir_adj(eir_file)
    note     = pl.concat([note, addeir])
    log.info("NOTE rows after EIR_ADJ: %d", len(note))

    # ----------------------------------------------------------------
    # 10 & 11. Build DEFAULT rows and merge
    # PROC SORT DATA=DEFAULT; BY BNMCODE
    # PROC SORT DATA=NOTE;    BY BNMCODE
    # DATA NOTE; SET DEFAULT NOTE; BY BNMCODE
    # ----------------------------------------------------------------
    default_df = build_defaults()
    note       = pl.concat([default_df, note]).sort("BNMCODE")
    log.info("NOTE rows after DEFAULT merge: %d", len(note))

    # ----------------------------------------------------------------
    # 12. PROC SUMMARY NWAY -> BNM.NOTE
    # ----------------------------------------------------------------
    bnm_note = summarise_by_bnmcode(note)
    log.info("BNM.NOTE summarised rows: %d", len(bnm_note))

    bnm_note.write_parquet(BNM_NOTE_OUTPUT)
    log.info("BNM.NOTE written: %s", BNM_NOTE_OUTPUT)

    # ----------------------------------------------------------------
    # 13. PROC PRINT DATA=BNM.NOTE
    # ----------------------------------------------------------------
    proc_print(bnm_note)

    log.info("EIIDNLF0 completed.")


if __name__ == "__main__":
    main()
