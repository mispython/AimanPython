#!/usr/bin/env python3
"""
Program  : P124RDAL.py
Purpose  : Report on Domestic Assets and Liabilities - Part I (Cagamas/L124).
           - Loads BIC reference codes (weekly or monthly depending on NOWK).
           - Merges with BNM.ALW summary data.
           - Filters and appends Cagamas loan data (LOANTYPE 124/145).
           - Splits result into AL (assets/loans), OB (off-balance), SP (special).
           - Writes semicolon-delimited output text file RDAL with sections
             AL, OB, SP each prefixed by section header and PHEAD on first line.

Dependency: PBBLNFMT  - format/mapping functions
            PBBWRDLF  - weekly  ITCODE reference list -> PBBRDAL.parquet
            PBBMRDLF  - monthly ITCODE reference list -> PBBRDAL.parquet
"""

import math
import polars as pl
import duckdb
from pathlib import Path

# ---------------------------------------------------------------------------
# Dependency: PBBLNFMT (%INC PGM(PBBLNFMT))
# Note: format_lnprod and format_lndenom are imported as part of the standard
#       %INC PGM(PBBLNFMT) inclusion, but they are not directly called within this
#       program. They are available in the session for reference consistency.
# ---------------------------------------------------------------------------
from PBBLNFMT import (
    format_lnprod,
    format_lndenom,
)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR         = Path("/data/sap")

# BNM library path
BNM_PATH         = BASE_DIR / "pbb/bnm"                        # BNM library

# LOAN.LNNOTE
LOAN_PATH        = BASE_DIR / "pbb/mniln/lnnote.parquet"

# PBBRDAL reference output (produced by PBBWRDLF or PBBMRDLF at import time)
PBBRDAL_PATH     = Path(__file__).resolve().parent / "output" / "PBBRDAL.parquet"

# RDAL output text file (semicolon-delimited)
RDAL_OUTPUT_PATH = BASE_DIR / "pbb/rdal/rdal.txt"

# ============================================================================
# GLOBAL MACRO VARIABLE EQUIVALENTS
# These correspond to SAS global macro variables (&REPTMON, &NOWK, &REPTDAY,
# &REPTYEAR) that are pre-defined in the SAS session environment before this
# program runs. They are not derived inside this program.
# OPTIONS YEARCUTOFF=1950 applies (2-digit years >= 50 -> 19xx, else 20xx).
# ============================================================================

REPTMON  = '03'    # Reporting month  (Z2. format, e.g. '03' for March)
NOWK     = '4'     # Week number within month ('1', '2', '3', '4')
REPTDAY  = '31'    # Reporting day    (Z2. format)
REPTYEAR = '2026'  # Reporting year   (YEAR4. format)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def round_div1000(value) -> int:
    """
    Equivalent to SAS ROUND(AMOUNT/1000).
    Uses standard rounding (round half away from zero), not banker's rounding.
    """
    if value is None:
        return 0
    return int(math.floor(float(value) / 1000.0 + 0.5))


# ============================================================================
# %MACRO MRGBIC
# ============================================================================

def macro_mrgbic(pbbrdal_df: pl.DataFrame, alw_df: pl.DataFrame) -> pl.DataFrame:
    """
    %MACRO MRGBIC:
    DATA PBBRDAL1:
      - Derive AMTIND: IF SUBSTR(ITCODE,2,1)='0' THEN AMTIND=' ' ELSE AMTIND='D'
      - Set AMOUNT=0
    MERGE ALW (RENAME AMOUNT->AMT1, IN=A)
          PBBRDAL1 (RENAME AMOUNT->AMT2, IN=B)
    BY ITCODE AMTIND;
      - A AND B     -> AMOUNT=AMT1
      - NOT A AND B -> AMOUNT=AMT2
      - A AND NOT B -> AMOUNT=AMT1
    Remove unwanted ITCODE ranges.
    """

    # DATA PBBRDAL1
    pbbrdal1 = pbbrdal_df.with_columns([
        pl.when(pl.col('ITCODE').str.slice(1, 1) == '0')
          .then(pl.lit(' '))
          .otherwise(pl.lit('D'))
          .alias('AMTIND'),
        pl.lit(0.0).alias('AMOUNT'),
    ])

    # PROC SORT DATA=PBBRDAL1; BY ITCODE AMTIND
    pbbrdal1 = pbbrdal1.sort(['ITCODE', 'AMTIND'])

    # PROC SORT DATA=BNM.ALW&REPTMON&NOWK OUT=ALW; BY ITCODE AMTIND
    alw_df = alw_df.sort(['ITCODE', 'AMTIND'])

    # MERGE ALW (RENAME=(AMOUNT=AMT1) IN=A) PBBRDAL1 (RENAME=(AMOUNT=AMT2) IN=B)
    # BY ITCODE AMTIND
    merged = alw_df.rename({'AMOUNT': 'AMT1'}).join(
        pbbrdal1.rename({'AMOUNT': 'AMT2'}),
        on=['ITCODE', 'AMTIND'],
        how='full',
        suffix='_R',
    )

    # IF A AND B / NOT A AND B / A AND NOT B -> resolve AMOUNT
    merged = merged.with_columns(
        pl.when(pl.col('AMT1').is_not_null())
          .then(pl.col('AMT1'))
          .otherwise(pl.col('AMT2'))
          .alias('AMOUNT')
    )

    # Drop merge helper columns
    drop_cols = [c for c in merged.columns if c in ('AMT1', 'AMT2')]
    merged = merged.drop(drop_cols)

    # REMOVE UNWANTED ITEMS:
    # IF NOT ('30221' <= SUBSTR(ITCODE,1,5) <= '30228') &
    #    NOT ('30231' <= SUBSTR(ITCODE,1,5) <= '30238') &
    #    NOT ('30091' <= SUBSTR(ITCODE,1,5) <= '30098') &
    #    NOT ('40151' <= SUBSTR(ITCODE,1,5) <= '40158') &
    #    SUBSTR(ITCODE,1,5) NOT IN ('NSSTS');
    rdal = merged.filter(
        ~(
            ((pl.col('ITCODE').str.slice(0, 5) >= '30221') & (pl.col('ITCODE').str.slice(0, 5) <= '30228')) |
            ((pl.col('ITCODE').str.slice(0, 5) >= '30231') & (pl.col('ITCODE').str.slice(0, 5) <= '30238')) |
            ((pl.col('ITCODE').str.slice(0, 5) >= '30091') & (pl.col('ITCODE').str.slice(0, 5) <= '30098')) |
            ((pl.col('ITCODE').str.slice(0, 5) >= '40151') & (pl.col('ITCODE').str.slice(0, 5) <= '40158')) |
            (pl.col('ITCODE').str.slice(0, 5) == 'NSSTS')
        )
    )

    return rdal


# ============================================================================
# MAIN
# ============================================================================

def main():
    # -----------------------------------------------------------------------
    # %GET_BICS:
    # %IF "&NOWK" EQ "4" %THEN %MONTHLY; %ELSE %WEEKLY;
    # Importing the module executes its module-level code, which writes
    # PBBRDAL.parquet — equivalent to %INC PGM(PBBWRDLF/PBBMRDLF) in SAS.
    # -----------------------------------------------------------------------
    if NOWK == '4':
        # %MONTHLY -> %INC PGM(PBBMRDLF)
        import PBBMRDLF  # noqa: F401
    else:
        # %WEEKLY -> %INC PGM(PBBWRDLF)
        import PBBWRDLF  # noqa: F401

    # Load PBBRDAL reference (produced by import above)
    pbbrdal_df = pl.read_parquet(PBBRDAL_PATH)

    # Load BNM.ALW&REPTMON&NOWK
    alw_path = BNM_PATH / f"alw{REPTMON}{NOWK}.parquet"
    con = duckdb.connect()
    alw_df = con.execute(f"SELECT * FROM read_parquet('{alw_path}')").pl()
    con.close()

    # %MRGBIC
    rdal_df = macro_mrgbic(pbbrdal_df, alw_df)

    # -----------------------------------------------------------------------
    # DATA CAG: SET LOAN.LNNOTE
    # IF LOANTYPE IN (124,145)
    # PRODCD='34120'; AMTIND='I'
    # IF PZIPCODE IN (...); ITCODE='7511100000000Y'
    # -----------------------------------------------------------------------
    con2 = duckdb.connect()
    loan_df = con2.execute(f"SELECT * FROM read_parquet('{LOAN_PATH}')").pl()
    con2.close()

    pzipcode_list = [
        2002, 2013, 3039, 3047, 800003098, 800003114,
        800004016, 800004022, 800004029, 800040050,
        800040053, 800050024, 800060024, 800060045,
        800060081, 80060085,
    ]

    cag_df = (
        loan_df
        .filter(pl.col('LOANTYPE').is_in([124, 145]))
        .with_columns([
            pl.lit('34120').alias('PRODCD'),
            pl.lit('I').alias('AMTIND'),
        ])
        .filter(pl.col('PZIPCODE').is_in(pzipcode_list))
        .with_columns(pl.lit('7511100000000Y').alias('ITCODE'))
    )

    # PROC SUMMARY DATA=CAG NWAY;
    # CLASS ITCODE AMTIND; VAR BALANCE;
    # OUTPUT OUT=CAG (DROP=_FREQ_ _TYPE_) SUM=AMOUNT;
    cag_summary = (
        cag_df
        .group_by(['ITCODE', 'AMTIND'])
        .agg(pl.col('BALANCE').sum().alias('AMOUNT'))
    )

    # DATA RDAL: SET RDAL CAG;
    # IF SUBSTR(ITCODE,1,3) IN ('331','421','426','431') THEN DELETE;
    rdal_df = pl.concat([rdal_df, cag_summary], how='diagonal')
    rdal_df = rdal_df.filter(
        ~pl.col('ITCODE').str.slice(0, 3).is_in(['331', '421', '426', '431'])
    )

    # PROC SORT DATA=RDAL; BY ITCODE AMTIND
    rdal_df = rdal_df.sort(['ITCODE', 'AMTIND'])

    # -----------------------------------------------------------------------
    # DATA AL OB SP: SET RDAL
    # Split rows into AL, OB, SP based on AMTIND and ITCODE prefix rules
    # -----------------------------------------------------------------------
    al_rows = []
    ob_rows = []
    sp_rows = []

    for row in rdal_df.to_dicts():
        itcode = str(row.get('ITCODE', '') or '')
        amtind = str(row.get('AMTIND', '') or '')

        it1   = itcode[0:1]    # SUBSTR(ITCODE,1,1)
        it3   = itcode[0:3]    # SUBSTR(ITCODE,1,3)
        it4   = itcode[0:4]    # SUBSTR(ITCODE,1,4)
        it5   = itcode[0:5]    # SUBSTR(ITCODE,1,5)
        it2_1 = itcode[1:2]    # SUBSTR(ITCODE,2,1)

        # IF AMTIND ^= ' ' THEN DO;
        if amtind != ' ':
            # IF SUBSTR(ITCODE,1,3) IN ('307') THEN OUTPUT SP;
            if it3 == '307':
                sp_rows.append(row)
            # ELSE IF SUBSTR(ITCODE,1,5) IN ('40190') THEN OUTPUT SP;
            elif it5 == '40190':
                sp_rows.append(row)
            # ELSE IF SUBSTR(ITCODE,1,4) = 'SSTS' THEN DO;
            #    ITCODE = '4017000000000Y'; OUTPUT SP;
            elif it4 == 'SSTS':
                new_row = dict(row)
                new_row['ITCODE'] = '4017000000000Y'
                sp_rows.append(new_row)
            # ELSE IF SUBSTR(ITCODE,1,1) ^= '5' THEN DO;
            elif it1 != '5':
                # IF SUBSTR(ITCODE,1,3) IN ('685','785') THEN OUTPUT SP;
                if it3 in ('685', '785'):
                    sp_rows.append(row)
                # ELSE OUTPUT AL;
                else:
                    al_rows.append(row)
            # ELSE OUTPUT OB;
            else:
                ob_rows.append(row)
        # ELSE IF SUBSTR(ITCODE,2,1)='0' THEN OUTPUT SP;
        elif it2_1 == '0':
            sp_rows.append(row)

    al_df = pl.DataFrame(al_rows) if al_rows else pl.DataFrame(schema=rdal_df.schema)
    ob_df = pl.DataFrame(ob_rows) if ob_rows else pl.DataFrame(schema=rdal_df.schema)
    sp_df = pl.DataFrame(sp_rows) if sp_rows else pl.DataFrame(schema=rdal_df.schema)

    # PROC SORT DATA=SP OUT=SP; BY ITCODE
    sp_df = sp_df.sort('ITCODE')

    # -----------------------------------------------------------------------
    # Write RDAL output text file
    # FILE RDAL    -> new file for AL section
    # FILE RDAL MOD -> append for OB and SP sections
    # -----------------------------------------------------------------------
    RDAL_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # PHEAD='RDAL'||"&REPTDAY"||"&REPTMON"||"&REPTYEAR"
    phead = f"RDAL{REPTDAY}{REPTMON}{REPTYEAR}"

    with open(RDAL_OUTPUT_PATH, 'w', encoding='utf-8', newline='\n') as f:

        # -------------------------------------------------------------------
        # DATA _NULL_: SET AL; BY ITCODE AMTIND
        # FILE RDAL (new)
        # IF _N_=1: PUT @1 PHEAD; PUT @1 'AL'; AMOUNTD=0; AMOUNTI=0;
        # RETAIN AMOUNTD AMOUNTI;
        # AMOUNT=ROUND(AMOUNT/1000);
        # IF AMTIND='D' THEN AMOUNTD+AMOUNT;
        # ELSE IF AMTIND='I' THEN AMOUNTI+AMOUNT;
        # IF LAST.ITCODE: AMOUNTD=AMOUNTD+AMOUNTI;
        #    PUT @1 ITCODE +(-1) ';' AMOUNTD +(-1) ';' AMOUNTI;
        #    AMOUNTD=0; AMOUNTI=0;
        # -------------------------------------------------------------------
        al_sorted = al_df.sort(['ITCODE', 'AMTIND'])
        al_recs   = al_sorted.to_dicts()

        first_al    = True
        amountd     = 0
        amounti     = 0
        prev_itcode = None

        for row in al_recs:
            itcode = str(row.get('ITCODE', '') or '')
            amtind = str(row.get('AMTIND', '') or '')
            amount = float(row.get('AMOUNT', 0) or 0)

            if first_al:
                f.write(phead + '\n')
                f.write('AL\n')
                amountd  = 0
                amounti  = 0
                first_al = False

            # Flush on LAST.ITCODE (group change)
            if prev_itcode is not None and itcode != prev_itcode:
                amountd = amountd + amounti
                f.write(f"{prev_itcode};{amountd};{amounti}\n")
                amountd = 0
                amounti = 0

            # AMOUNT=ROUND(AMOUNT/1000)
            amt_rounded = round_div1000(amount)
            if amtind == 'D':
                amountd += amt_rounded
            elif amtind == 'I':
                amounti += amt_rounded

            prev_itcode = itcode

        # Flush final ITCODE group for AL
        if prev_itcode is not None:
            amountd = amountd + amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # -------------------------------------------------------------------
        # DATA _NULL_: SET OB; BY ITCODE AMTIND
        # FILE RDAL MOD
        # IF _N_=1: PUT @1 'OB'; AMOUNTD=0; AMOUNTI=0;
        # RETAIN AMOUNTD AMOUNTI;
        # IF AMTIND='D' THEN AMOUNTD+ROUND(AMOUNT/1000);
        # ELSE IF AMTIND='I' THEN AMOUNTI+ROUND(AMOUNT/1000);
        # IF LAST.ITCODE: AMOUNTD=AMOUNTD+AMOUNTI;
        #    PUT @1 ITCODE +(-1) ';' AMOUNTD +(-1) ';' AMOUNTI;
        #    AMOUNTD=0; AMOUNTI=0;
        # -------------------------------------------------------------------
        ob_sorted = ob_df.sort(['ITCODE', 'AMTIND'])
        ob_recs   = ob_sorted.to_dicts()

        first_ob    = True
        amountd     = 0
        amounti     = 0
        prev_itcode = None

        for row in ob_recs:
            itcode = str(row.get('ITCODE', '') or '')
            amtind = str(row.get('AMTIND', '') or '')
            amount = float(row.get('AMOUNT', 0) or 0)

            if first_ob:
                f.write('OB\n')
                amountd  = 0
                amounti  = 0
                first_ob = False

            if prev_itcode is not None and itcode != prev_itcode:
                amountd = amountd + amounti
                f.write(f"{prev_itcode};{amountd};{amounti}\n")
                amountd = 0
                amounti = 0

            if amtind == 'D':
                amountd += round_div1000(amount)
            elif amtind == 'I':
                amounti += round_div1000(amount)

            prev_itcode = itcode

        # Flush final ITCODE group for OB
        if prev_itcode is not None:
            amountd = amountd + amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # -------------------------------------------------------------------
        # PROC SORT DATA=SP OUT=SP; BY ITCODE  (already sorted above)
        #
        # DATA _NULL_: SET SP; BY ITCODE
        # FILE RDAL MOD
        # IF _N_=1: PUT @1 'SP'; AMOUNTD=0;
        # AMOUNTD+AMOUNT;
        # IF LAST.ITCODE: AMOUNTD=ROUND(AMOUNTD/1000);
        #    PUT @1 ITCODE +(-1) ';' AMOUNTD +(-1);
        #    AMOUNTD=0;
        # -------------------------------------------------------------------
        sp_recs     = sp_df.to_dicts()
        first_sp    = True
        amountd     = 0.0
        prev_itcode = None

        for row in sp_recs:
            itcode = str(row.get('ITCODE', '') or '')
            amount = float(row.get('AMOUNT', 0) or 0)

            if first_sp:
                f.write('SP\n')
                amountd  = 0.0
                first_sp = False

            if prev_itcode is not None and itcode != prev_itcode:
                # AMOUNTD=ROUND(AMOUNTD/1000) on LAST.ITCODE
                f.write(f"{prev_itcode};{round_div1000(amountd)}\n")
                amountd = 0.0

            # AMOUNTD+AMOUNT (accumulate raw, round only on LAST.ITCODE)
            amountd += amount
            prev_itcode = itcode

        # Flush final ITCODE group for SP
        if prev_itcode is not None:
            f.write(f"{prev_itcode};{round_div1000(amountd)}\n")

    print(f"RDAL output written to: {RDAL_OUTPUT_PATH}")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
