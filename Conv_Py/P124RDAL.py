#!/usr/bin/env python3
"""
Program  : P124RDAL
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
            L124PBBD  - produces BNM.L124/UL124 (via LALWP124 upstream)
"""

import polars as pl
import duckdb
from pathlib import Path
import datetime

# ---------------------------------------------------------------------------
# Dependency: PBBLNFMT  (%INC PGM(PBBLNFMT))
# ---------------------------------------------------------------------------
from PBBLNFMT import (
    format_lnprod,
    format_lndenom,
)

# ---------------------------------------------------------------------------
# Dependency: PBBWRDLF  (%MACRO WEEKLY -> %INC PGM(PBBWRDLF))
# Produces output/PBBRDAL.parquet from weekly ITCODE list.
# ---------------------------------------------------------------------------
from PBBWRDLF import main as run_pbbwrdlf

# ---------------------------------------------------------------------------
# Dependency: PBBMRDLF  (%MACRO MONTHLY -> %INC PGM(PBBMRDLF))
# Produces output/PBBRDAL.parquet from monthly ITCODE list.
# ---------------------------------------------------------------------------
from PBBMRDLF import main as run_pbbmrdlf

# ---------------------------------------------------------------------------
# Dependency: L124PBBD - REPTDATE_PATH and get_reptmon_nowk
# ---------------------------------------------------------------------------
from L124PBBD import get_reptmon_nowk, REPTDATE_PATH

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = Path("/data/sap")

# BNM library paths
BNM_PATH      = BASE_DIR / "pbb/bnm"                           # BNM library
LOAN_PATH     = BASE_DIR / "pbb/mniln/lnnote.parquet"          # LOAN.LNNOTE

# PBBRDAL reference output (produced by PBBWRDLF or PBBMRDLF)
PBBRDAL_PATH  = Path(__file__).resolve().parent / "output" / "PBBRDAL.parquet"

# RDAL output text file (semicolon-delimited)
RDAL_OUTPUT_PATH = BASE_DIR / "pbb/rdal/rdal.txt"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_reptdate_info(reptdate_parquet: Path) -> tuple:
    """
    Read REPTDATE and return (reptmon, nowk, reptday, reptyear, rdate_str).
    YEARCUTOFF=1950 applies (2-digit years >= 50 -> 19xx, else 20xx).
    """
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchdf()
    con.close()

    reptdate = df['REPTDATE'].iloc[0]
    if hasattr(reptdate, 'date'):
        reptdate = reptdate.date()

    day      = reptdate.day
    reptmon  = reptdate.strftime('%m')          # Z2.
    reptyear = reptdate.strftime('%Y')          # YEAR4.
    reptday  = reptdate.strftime('%d')          # Z2.
    rdate    = reptdate.strftime('%d/%m/%y')    # DDMMYY8.

    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    return reptmon, nowk, reptday, reptyear, rdate


def round_div1000(value) -> int:
    """ROUND(AMOUNT/1000) equivalent - Python round() uses banker's rounding; use standard."""
    if value is None:
        return 0
    import math
    return int(math.floor(float(value) / 1000.0 + 0.5))


# ============================================================================
# %MACRO MRGBIC
# ============================================================================

def macro_mrgbic(
    pbbrdal_df: pl.DataFrame,
    alw_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    %MACRO MRGBIC:
    1. Derive AMTIND from ITCODE[1] (0-based index 1 = SAS SUBSTR(ITCODE,2,1)).
    2. Set AMOUNT=0 for the reference frame.
    3. Merge ALW (left) with PBBRDAL1 (right) by ITCODE, AMTIND.
    4. Apply AMOUNT resolution rules.
    5. Remove unwanted ITCODE ranges.
    """

    # DATA PBBRDAL1: IF SUBSTR(ITCODE,2,1)='0' THEN AMTIND=' ' ELSE AMTIND='D'
    # AMOUNT=0
    pbbrdal1 = pbbrdal_df.with_columns([
        pl.when(pl.col('ITCODE').str.slice(1, 1) == '0')
          .then(pl.lit(' '))
          .otherwise(pl.lit('D'))
          .alias('AMTIND'),
        pl.lit(0.0).alias('AMOUNT'),
    ])

    # PROC SORT: BY ITCODE AMTIND (sort both sides for merge)
    pbbrdal1 = pbbrdal1.sort(['ITCODE', 'AMTIND'])
    alw_df   = alw_df.sort(['ITCODE', 'AMTIND'])

    # MERGE ALW (RENAME AMOUNT->AMT1, IN=A) PBBRDAL1 (RENAME AMOUNT->AMT2, IN=B)
    # BY ITCODE AMTIND
    # Full outer join, then apply rules:
    #   A AND B     -> AMOUNT = AMT1
    #   NOT A AND B -> AMOUNT = AMT2
    #   A AND NOT B -> AMOUNT = AMT1
    merged = alw_df.rename({'AMOUNT': 'AMT1'}).join(
        pbbrdal1.rename({'AMOUNT': 'AMT2'}),
        on=['ITCODE', 'AMTIND'],
        how='outer',
        suffix='_R',
    )

    # Resolve AMOUNT: A AND B or A AND NOT B -> AMT1; NOT A AND B -> AMT2
    merged = merged.with_columns(
        pl.when(pl.col('AMT1').is_not_null())
          .then(pl.col('AMT1'))
          .otherwise(pl.col('AMT2'))
          .alias('AMOUNT')
    )

    # Keep only rows that exist in either dataset (all rows after outer join)
    # Drop helper columns
    drop_cols = [c for c in merged.columns if c in ('AMT1', 'AMT2')]
    merged = merged.drop(drop_cols)

    # REMOVE UNWANTED ITEMS:
    # NOT ('30221' <= SUBSTR(ITCODE,1,5) <= '30228')
    # NOT ('30231' <= SUBSTR(ITCODE,1,5) <= '30238')
    # NOT ('30091' <= SUBSTR(ITCODE,1,5) <= '30098')
    # NOT ('40151' <= SUBSTR(ITCODE,1,5) <= '40158')
    # SUBSTR(ITCODE,1,5) NOT IN ('NSSTS')
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
    # Derive date variables
    # -----------------------------------------------------------------------
    reptmon, nowk, reptday, reptyear, rdate = get_reptdate_info(REPTDATE_PATH)

    # -----------------------------------------------------------------------
    # %GET_BICS: IF NOWK=4 -> MONTHLY else WEEKLY, then MRGBIC
    # -----------------------------------------------------------------------
    if nowk == '4':
        # %MONTHLY -> %INC PGM(PBBMRDLF)
        run_pbbmrdlf()
    else:
        # %WEEKLY  -> %INC PGM(PBBWRDLF)
        run_pbbwrdlf()

    # Load PBBRDAL reference (produced above)
    pbbrdal_df = pl.read_parquet(PBBRDAL_PATH)

    # Load BNM.ALW&REPTMON&NOWK
    alw_path = BNM_PATH / f"alw{reptmon}{nowk}.parquet"
    con      = duckdb.connect()
    alw_df   = con.execute(f"SELECT * FROM read_parquet('{alw_path}')").pl()
    con.close()

    # %MRGBIC
    rdal_df = macro_mrgbic(pbbrdal_df, alw_df)

    # -----------------------------------------------------------------------
    # DATA CAG: SET LOAN.LNNOTE
    # IF LOANTYPE IN (124,145); PRODCD='34120'; AMTIND='I'
    # IF PZIPCODE IN (...); ITCODE='7511100000000Y'
    # -----------------------------------------------------------------------
    con2    = duckdb.connect()
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

    # PROC SUMMARY DATA=CAG NWAY; CLASS ITCODE AMTIND; VAR BALANCE;
    # OUTPUT OUT=CAG (DROP=_FREQ_ _TYPE_) SUM=AMOUNT;
    cag_summary = (
        cag_df
        .group_by(['ITCODE', 'AMTIND'])
        .agg(pl.col('BALANCE').sum().alias('AMOUNT'))
    )

    # DATA RDAL: SET RDAL CAG;
    # IF SUBSTR(ITCODE,1,3) IN ('331','421','426','431') THEN DELETE
    rdal_df = pl.concat([rdal_df, cag_summary], how='diagonal')
    rdal_df = rdal_df.filter(
        ~pl.col('ITCODE').str.slice(0, 3).is_in(['331', '421', '426', '431'])
    )

    # PROC SORT DATA=RDAL; BY ITCODE AMTIND
    rdal_df = rdal_df.sort(['ITCODE', 'AMTIND'])

    # -----------------------------------------------------------------------
    # DATA AL OB SP: SET RDAL
    # Split into three datasets based on AMTIND and ITCODE prefix rules
    # -----------------------------------------------------------------------
    al_rows = []
    ob_rows = []
    sp_rows = []

    for row in rdal_df.to_dicts():
        itcode = str(row.get('ITCODE', '') or '')
        amtind = str(row.get('AMTIND', '') or '')
        amount = float(row.get('AMOUNT', 0) or 0)

        it1   = itcode[0:1]    # SUBSTR(ITCODE,1,1)
        it3   = itcode[0:3]    # SUBSTR(ITCODE,1,3)
        it4   = itcode[0:4]    # SUBSTR(ITCODE,1,4)
        it5   = itcode[0:5]    # SUBSTR(ITCODE,1,5)
        it2_1 = itcode[1:2]    # SUBSTR(ITCODE,2,1)

        if amtind != ' ':
            if it3 in ('307',):
                sp_rows.append(row)
            elif it5 == '40190':
                sp_rows.append(row)
            elif it4 == 'SSTS':
                # ITCODE = '4017000000000Y'
                new_row = dict(row)
                new_row['ITCODE'] = '4017000000000Y'
                sp_rows.append(new_row)
            elif it1 != '5':
                if it3 in ('685', '785'):
                    sp_rows.append(row)
                else:
                    al_rows.append(row)
            else:
                ob_rows.append(row)
        elif it2_1 == '0':
            sp_rows.append(row)

    al_df = pl.DataFrame(al_rows) if al_rows else pl.DataFrame(schema=rdal_df.schema)
    ob_df = pl.DataFrame(ob_rows) if ob_rows else pl.DataFrame(schema=rdal_df.schema)
    sp_df = pl.DataFrame(sp_rows) if sp_rows else pl.DataFrame(schema=rdal_df.schema)

    # PROC SORT DATA=SP OUT=SP; BY ITCODE
    sp_df = sp_df.sort('ITCODE')

    # -----------------------------------------------------------------------
    # Write RDAL output text file
    # FILE RDAL (new) then MOD for OB and SP sections
    # -----------------------------------------------------------------------
    RDAL_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    phead = f"RDAL{reptday}{reptmon}{reptyear}"

    with open(RDAL_OUTPUT_PATH, 'w', encoding='utf-8', newline='\n') as f:

        # -------------------------------------------------------------------
        # DATA _NULL_: SET AL; BY ITCODE AMTIND
        # FILE RDAL (new file)
        # PUT @1 PHEAD  (on _N_=1)
        # PUT @1 'AL'   (on _N_=1)
        # Accumulate AMOUNTD / AMOUNTI per ITCODE, write on LAST.ITCODE
        # -------------------------------------------------------------------
        al_sorted = al_df.sort(['ITCODE', 'AMTIND'])
        al_recs   = al_sorted.to_dicts()

        first_al = True
        amountd  = 0
        amounti  = 0
        prev_itcode = None

        for i, row in enumerate(al_recs):
            itcode = str(row.get('ITCODE', '') or '')
            amtind = str(row.get('AMTIND', '') or '')
            amount = float(row.get('AMOUNT', 0) or 0)

            if first_al:
                # PUT @1 PHEAD
                f.write(phead + '\n')
                # PUT @1 'AL'
                f.write('AL\n')
                amountd  = 0
                amounti  = 0
                first_al = False

            # Flush previous ITCODE group on change
            if prev_itcode is not None and itcode != prev_itcode:
                amountd = amountd + amounti
                # PUT @1 ITCODE +(-1) ';' AMOUNTD +(-1) ';' AMOUNTI
                f.write(f"{prev_itcode};{amountd};{amounti}\n")
                amountd = 0
                amounti = 0

            # RETAIN AMOUNTD AMOUNTI; accumulate
            amt_rounded = round_div1000(amount)
            if amtind == 'D':
                amountd += amt_rounded
            elif amtind == 'I':
                amounti += amt_rounded

            prev_itcode = itcode

        # Flush last group for AL
        if prev_itcode is not None:
            amountd = amountd + amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # -------------------------------------------------------------------
        # DATA _NULL_: SET OB; BY ITCODE AMTIND
        # FILE RDAL MOD
        # PUT @1 'OB' (on _N_=1)
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

        if prev_itcode is not None:
            amountd = amountd + amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # -------------------------------------------------------------------
        # DATA _NULL_: SET SP; BY ITCODE  (already sorted)
        # FILE RDAL MOD
        # PUT @1 'SP' (on _N_=1)
        # Accumulate AMOUNTD (all amounts combined), write on LAST.ITCODE
        # -------------------------------------------------------------------
        sp_recs  = sp_df.to_dicts()
        first_sp = True
        amountd  = 0
        prev_itcode = None

        for row in sp_recs:
            itcode = str(row.get('ITCODE', '') or '')
            amount = float(row.get('AMOUNT', 0) or 0)

            if first_sp:
                f.write('SP\n')
                amountd  = 0
                first_sp = False

            if prev_itcode is not None and itcode != prev_itcode:
                amountd = round_div1000(amountd)
                # PUT @1 ITCODE +(-1) ';' AMOUNTD +(-1)
                f.write(f"{prev_itcode};{amountd}\n")
                amountd = 0

            # AMOUNTD+AMOUNT (accumulate raw, round only on LAST.ITCODE)
            amountd += amount
            prev_itcode = itcode

        if prev_itcode is not None:
            amountd = round_div1000(amountd)
            f.write(f"{prev_itcode};{amountd}\n")

    print(f"RDAL output written to: {RDAL_OUTPUT_PATH}")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
