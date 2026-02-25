# !/usr/bin/env python3
"""
Program  : EIIWRDAL
Purpose  : Weekly/Monthly BIC merge and RDAL/NSRS output generation for PIBB.
           ESMR: 06-1485
           - Loads BIC reference codes (weekly or monthly depending on NOWK).
           - Merges with BNM.ALW summary data (AMTIND forced to 'I').
           - Appends Cagamas LNNOTE data filtered by PZIPCODE and COSTCTR.
           - Splits result into AL, OB, SP sections.
           - Writes two semicolon-delimited output files:
               RDAL : amounts divided by 1000 (rounded)
               NSRS : amounts rounded as-is (ITCODE starting '80' divided by 1000)
           Each file has sections: header PHEAD, 'AL', 'OB', 'SP'.

Dependency: PBBLNFMT  - format/mapping functions
            PBBWRDLF  - weekly  ITCODE reference list -> PBBRDAL.parquet
            PBBMRDLF  - monthly ITCODE reference list -> PBBRDAL.parquet
"""

import polars as pl
import duckdb
from pathlib import Path
import datetime
import math

# ---------------------------------------------------------------------------
# Dependency: PBBLNFMT (%INC PGM(PBBLNFMT))
# ---------------------------------------------------------------------------
from PBBLNFMT import format_lnprod, format_lndenom

# ---------------------------------------------------------------------------
# Dependency: PBBWRDLF (%MACRO WEEKLY -> %INC PGM(PBBWRDLF))
# ---------------------------------------------------------------------------
from PBBWRDLF import main as run_pbbwrdlf

# ---------------------------------------------------------------------------
# Dependency: PBBMRDLF (%MACRO MONTHLY -> %INC PGM(PBBMRDLF))
# ---------------------------------------------------------------------------
from PBBMRDLF import main as run_pbbmrdlf

# ---------------------------------------------------------------------------
# Dependency: Derive REPTMON, NOWK, REPTDAY, REPTYEAR from upstream REPTDATE
# ---------------------------------------------------------------------------
from L124PBBD import get_reptmon_nowk, REPTDATE_PATH

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = Path("/data/sap")

# BNM library - contains ALW parquet files
BNM_PATH      = BASE_DIR / "pbb/bnm"

# LOAN.LNNOTE - PIBB loan note data
LOAN_LNNOTE_PATH = BASE_DIR / "pibb/mniln/lnnote.parquet"          # LOAN.LNNOTE

# PBBRDAL reference output (produced by PBBWRDLF or PBBMRDLF)
PBBRDAL_PATH  = Path(__file__).resolve().parent / "output" / "PBBRDAL.parquet"

# Output files
RDAL_OUTPUT_PATH = BASE_DIR / "pibb/rdal/rdal.txt"                 # FILE RDAL
NSRS_OUTPUT_PATH = BASE_DIR / "pibb/rdal/nsrs.txt"                 # FILE NSRS

# ============================================================================
# HELPER: read REPTDATE variables
# ============================================================================

def get_all_date_vars(reptdate_parquet: Path) -> dict:
    """Read REPTDATE and derive REPTMON, NOWK, REPTDAY, REPTYEAR."""
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchdf()
    con.close()

    reptdate = df['REPTDATE'].iloc[0]
    if hasattr(reptdate, 'date'):
        reptdate = reptdate.date()

    day      = reptdate.day
    reptmon  = reptdate.strftime('%m')
    reptyear = reptdate.strftime('%Y')
    reptday  = f"{day:02d}"

    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    return {
        'reptmon':  reptmon,
        'nowk':     nowk,
        'reptday':  reptday,
        'reptyear': reptyear,
    }


# ============================================================================
# HELPER: standard rounding (0.5 rounds up)
# ============================================================================

def sas_round(value) -> int:
    """ROUND() equivalent matching SAS: 0.5 rounds away from zero."""
    if value is None:
        return 0
    return int(math.floor(float(value) + 0.5))


def round_div1000(value) -> int:
    """ROUND(AMOUNT/1000)."""
    if value is None:
        return 0
    return sas_round(float(value) / 1000.0)


# ============================================================================
# %MACRO MRGBIC  (EIIWRDAL variant: AMTIND always 'I', no AMTIND derivation)
# ============================================================================

def macro_mrgbic(
    pbbrdal_df: pl.DataFrame,
    alw_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    %MACRO MRGBIC (EIIWRDAL version):
    DATA PBBRDAL1: SET PBBRDAL; AMTIND='I'; AMOUNT=0;
    PROC SORT PBBRDAL1 BY ITCODE AMTIND;
    PROC SORT ALW BY ITCODE AMTIND;
    MERGE ALW(RENAME AMOUNT->AMT1, IN=A) PBBRDAL1(RENAME AMOUNT->AMT2, IN=B)
      BY ITCODE AMTIND;
      A AND B     -> AMOUNT=AMT1
      NOT A AND B -> AMOUNT=AMT2
      A AND NOT B -> AMOUNT=AMT1
    Remove unwanted ITCODE ranges.
    """
    # DATA PBBRDAL1: AMTIND='I'; AMOUNT=0
    pbbrdal1 = pbbrdal_df.with_columns([
        pl.lit('I').alias('AMTIND'),
        pl.lit(0.0).alias('AMOUNT'),
    ])

    pbbrdal1 = pbbrdal1.sort(['ITCODE', 'AMTIND'])
    alw_df   = alw_df.sort(['ITCODE', 'AMTIND'])

    # Full outer join by ITCODE, AMTIND
    merged = alw_df.rename({'AMOUNT': 'AMT1'}).join(
        pbbrdal1.rename({'AMOUNT': 'AMT2'}),
        on=['ITCODE', 'AMTIND'],
        how='outer',
        suffix='_R',
    )

    # AMOUNT resolution
    merged = merged.with_columns(
        pl.when(pl.col('AMT1').is_not_null())
          .then(pl.col('AMT1'))
          .otherwise(pl.col('AMT2'))
          .alias('AMOUNT')
    )

    drop_cols = [c for c in merged.columns if c in ('AMT1', 'AMT2')]
    merged    = merged.drop(drop_cols)

    # REMOVE UNWANTED ITEMS
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
# AL / OB / SP SPLIT  (EIIWRDAL variant)
# ============================================================================

def split_al_ob_sp(rdal_df: pl.DataFrame) -> tuple:
    """
    DATA AL OB SP: SET RDAL;

    Special handling for ITCODE='4314020000000Y':
      AMOUNT=ABS(AMOUNT); IF AMTIND='D' THEN DELETE.

    Routing rules (same as P124RDAL but with extra ITCODE '40191' -> SP):
      AMTIND != ' ':
        ITCODE[0:3] IN ('307')        -> SP
        ITCODE[0:5] IN ('40190')      -> SP
        ITCODE[0:5] IN ('40191')      -> SP   [EIIWRDAL addition]
        ITCODE[0:4] = 'SSTS'          -> ITCODE='4017000000000Y', SP
        ITCODE[0:1] != '5':
          ITCODE[0:3] IN ('685','785')-> SP
          else                        -> AL
        else                          -> OB
      AMTIND == ' ':
        ITCODE[1:2] = '0'             -> SP
    """
    al_rows = []
    ob_rows = []
    sp_rows = []

    # Apply special ITCODE='4314020000000Y' rule first
    rows = rdal_df.to_dicts()
    filtered = []
    for row in rows:
        itcode = str(row.get('ITCODE', '') or '')
        amtind = str(row.get('AMTIND', '') or '')
        amount = float(row.get('AMOUNT', 0) or 0)

        if itcode == '4314020000000Y':
            amount = abs(amount)
            row['AMOUNT'] = amount
            if amtind == 'D':
                continue    # DELETE
        filtered.append(row)

    for row in filtered:
        itcode = str(row.get('ITCODE', '') or '')
        amtind = str(row.get('AMTIND', '') or '')

        it1  = itcode[0:1]
        it3  = itcode[0:3]
        it4  = itcode[0:4]
        it5  = itcode[0:5]
        it2_1 = itcode[1:2]

        if amtind != ' ':
            if it3 == '307':
                sp_rows.append(row)
            elif it5 == '40190':
                sp_rows.append(row)
            elif it5 == '40191':
                sp_rows.append(row)
            elif it4 == 'SSTS':
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

    schema = rdal_df.schema
    al_df  = pl.DataFrame(al_rows, schema=schema) if al_rows else pl.DataFrame(schema=schema)
    ob_df  = pl.DataFrame(ob_rows, schema=schema) if ob_rows else pl.DataFrame(schema=schema)
    sp_df  = pl.DataFrame(sp_rows, schema=schema) if sp_rows else pl.DataFrame(schema=schema)

    # PROC SORT DATA=SP OUT=SP; BY ITCODE
    sp_df = sp_df.sort('ITCODE')

    return al_df, ob_df, sp_df


# ============================================================================
# RDAL FILE WRITER
# ============================================================================

def write_rdal_file(
    al_df: pl.DataFrame,
    ob_df: pl.DataFrame,
    sp_df: pl.DataFrame,
    reptday: str,
    reptmon: str,
    reptyear: str,
    output_path: Path,
):
    """
    Write RDAL output file (FILE RDAL).
    AL section: AMOUNT=ROUND(AMOUNT/1000); AMOUNTI accumulates all amounts.
                AMOUNTD=AMOUNTI on LAST.ITCODE.
    PROCEED filter: IF &REPTDAY IN ('08','22') AND ITCODE='4003000000000Y'
                    AND SUBSTR(ITCODE,1,2) IN ('68','78') THEN PROCEED='N'
                    (Note: ITCODE='4003000000000Y' and ITCODE[0:2] IN ('68','78')
                     is always mutually exclusive, so PROCEED is always 'Y' in practice.)
    OB section: AMOUNTI accumulates ROUND(AMOUNT/1000); AMOUNTD=AMOUNTI on LAST.ITCODE.
    SP section: AMOUNTI accumulates AMOUNT; ROUND(AMOUNTI/1000) on LAST.ITCODE.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    phead = f"RDAL{reptday}{reptmon}{reptyear}"

    al_sorted = al_df.sort(['ITCODE', 'AMTIND'])
    ob_sorted = ob_df.sort(['ITCODE', 'AMTIND'])

    with open(output_path, 'w', encoding='utf-8', newline='\n') as f:

        # ----------------------------------------------------------------
        # AL section
        # ----------------------------------------------------------------
        first_al    = True
        amounti     = 0
        amountd     = 0
        prev_itcode = None

        for row in al_sorted.to_dicts():
            itcode = str(row.get('ITCODE', '') or '')
            amtind = str(row.get('AMTIND', '') or '')
            amount = float(row.get('AMOUNT', 0) or 0)

            if first_al:
                f.write(phead + '\n')
                f.write('AL\n')
                amounti  = 0
                amountd  = 0
                first_al = False

            if prev_itcode is not None and itcode != prev_itcode:
                amountd = amounti
                f.write(f"{prev_itcode};{amountd};{amounti}\n")
                amounti = 0
                amountd = 0

            # PROCEED logic
            proceed = 'Y'
            if reptday in ('08', '22'):
                # ITCODE='4003000000000Y' AND SUBSTR(ITCODE,1,2) IN ('68','78')
                # is contradictory (4003... starts with '40', not '68'/'78')
                # so PROCEED is always 'Y' - preserved as-is from SAS source
                if itcode == '4003000000000Y' and itcode[0:2] in ('68', '78'):
                    proceed = 'N'

            if proceed == 'Y':
                amt_rounded = round_div1000(amount)
                amounti    += amt_rounded

            prev_itcode = itcode

        if prev_itcode is not None and first_al is False:
            amountd = amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # ----------------------------------------------------------------
        # OB section
        # ----------------------------------------------------------------
        first_ob    = True
        amounti     = 0
        amountd     = 0
        prev_itcode = None

        for row in ob_sorted.to_dicts():
            itcode = str(row.get('ITCODE', '') or '')
            amount = float(row.get('AMOUNT', 0) or 0)

            if first_ob:
                f.write('OB\n')
                amounti  = 0
                amountd  = 0
                first_ob = False

            if prev_itcode is not None and itcode != prev_itcode:
                amountd = amounti
                f.write(f"{prev_itcode};{amountd};{amounti}\n")
                amounti = 0
                amountd = 0

            amounti += round_div1000(amount)
            prev_itcode = itcode

        if prev_itcode is not None and first_ob is False:
            amountd = amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # ----------------------------------------------------------------
        # SP section
        # ----------------------------------------------------------------
        first_sp    = True
        amounti     = 0.0
        prev_itcode = None

        for row in sp_df.to_dicts():
            itcode = str(row.get('ITCODE', '') or '')
            amount = float(row.get('AMOUNT', 0) or 0)

            if first_sp:
                f.write('SP\n')
                amounti  = 0.0
                first_sp = False

            if prev_itcode is not None and itcode != prev_itcode:
                amounti_out = sas_round(amounti / 1000.0)
                f.write(f"{prev_itcode};{amounti_out}\n")
                amounti = 0.0

            amounti    += amount
            prev_itcode = itcode

        if prev_itcode is not None and first_sp is False:
            amounti_out = sas_round(amounti / 1000.0)
            f.write(f"{prev_itcode};{amounti_out}\n")


# ============================================================================
# NSRS FILE WRITER
# ============================================================================

def write_nsrs_file(
    al_df: pl.DataFrame,
    ob_df: pl.DataFrame,
    sp_df: pl.DataFrame,
    reptday: str,
    reptmon: str,
    reptyear: str,
    output_path: Path,
):
    """
    Write NSRS output file (FILE NSRS).
    AL section: AMOUNT=ROUND(AMOUNT); IF ITCODE[0:2]='80' THEN AMOUNT=ROUND(AMOUNT/1000)
                AMOUNTI accumulates; AMOUNTD=AMOUNTI on LAST.ITCODE.
    OB section: AMOUNTI+=ROUND(AMOUNT); IF ITCODE[0:2]='80' THEN /1000 correction.
                AMOUNTD=AMOUNTI on LAST.ITCODE.
    SP section: AMOUNTI+=AMOUNT; ROUND(AMOUNTI) on LAST.ITCODE;
                IF ITCODE[0:2]='80' THEN ROUND(AMOUNT/1000) override.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    phead = f"RDAL{reptday}{reptmon}{reptyear}"

    al_sorted = al_df.sort(['ITCODE', 'AMTIND'])
    ob_sorted = ob_df.sort(['ITCODE', 'AMTIND'])

    with open(output_path, 'w', encoding='utf-8', newline='\n') as f:

        # ----------------------------------------------------------------
        # AL section
        # ----------------------------------------------------------------
        first_al    = True
        amounti     = 0
        amountd     = 0
        prev_itcode = None

        for row in al_sorted.to_dicts():
            itcode = str(row.get('ITCODE', '') or '')
            amount = float(row.get('AMOUNT', 0) or 0)

            if first_al:
                f.write(phead + '\n')
                f.write('AL\n')
                amounti  = 0
                amountd  = 0
                first_al = False

            if prev_itcode is not None and itcode != prev_itcode:
                amountd = amounti
                f.write(f"{prev_itcode};{amountd};{amounti}\n")
                amounti = 0
                amountd = 0

            # PROCEED logic (same as RDAL)
            proceed = 'Y'
            if reptday in ('08', '22'):
                if itcode == '4003000000000Y' and itcode[0:2] in ('68', '78'):
                    proceed = 'N'

            if proceed == 'Y':
                # AMOUNT=ROUND(AMOUNT)
                amt = sas_round(amount)
                # IF SUBSTR(ITCODE,1,2) IN ('80') THEN AMOUNT=ROUND(AMOUNT/1000)
                if itcode[0:2] == '80':
                    amt = round_div1000(amount)
                amounti += amt

            prev_itcode = itcode

        if prev_itcode is not None and first_al is False:
            amountd = amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # ----------------------------------------------------------------
        # OB section
        # ----------------------------------------------------------------
        first_ob    = True
        amounti     = 0
        amountd     = 0
        prev_itcode = None

        for row in ob_sorted.to_dicts():
            itcode = str(row.get('ITCODE', '') or '')
            amount = float(row.get('AMOUNT', 0) or 0)

            if first_ob:
                f.write('OB\n')
                amounti  = 0
                amountd  = 0
                first_ob = False

            if prev_itcode is not None and itcode != prev_itcode:
                amountd = amounti
                f.write(f"{prev_itcode};{amountd};{amounti}\n")
                amounti = 0
                amountd = 0

            # RETAIN AMOUNTI; AMOUNTI+=ROUND(AMOUNT)
            amt = sas_round(amount)
            # IF SUBSTR(ITCODE,1,2) IN ('80') correction
            if itcode[0:2] == '80':
                amount_adj = round_div1000(amount)
                # SAS: AMOUNTI+ROUND(AMOUNT) first, then IF '80' -> /1000 override
                # Per SAS source: AMOUNTI+ROUND(AMOUNT); then IF SUBSTR... AMOUNT=ROUND(AMOUNT/1000)
                # The reassignment of AMOUNT after accumulation does not affect AMOUNTI in SAS
                # so we accumulate sas_round(amount) regardless:
                amt = sas_round(amount)
            amounti += amt
            prev_itcode = itcode

        if prev_itcode is not None and first_ob is False:
            amountd = amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # ----------------------------------------------------------------
        # SP section
        # ----------------------------------------------------------------
        first_sp    = True
        amounti     = 0.0
        prev_itcode = None

        for row in sp_df.to_dicts():
            itcode = str(row.get('ITCODE', '') or '')
            amount = float(row.get('AMOUNT', 0) or 0)

            if first_sp:
                f.write('SP\n')
                amounti  = 0.0
                first_sp = False

            if prev_itcode is not None and itcode != prev_itcode:
                amounti_out = sas_round(amounti)
                # IF SUBSTR(ITCODE,1,2) IN ('80') THEN AMOUNT=ROUND(AMOUNT/1000)
                # Note: in SAS this reassigns AMOUNT (not AMOUNTI) after rounding AMOUNTI,
                # so the output value is ROUND(AMOUNTI) unchanged by the '80' branch.
                f.write(f"{prev_itcode};{amounti_out}\n")
                amounti = 0.0

            amounti    += amount
            prev_itcode = itcode

        if prev_itcode is not None and first_sp is False:
            amounti_out = sas_round(amounti)
            f.write(f"{prev_itcode};{amounti_out}\n")


# ============================================================================
# MAIN
# ============================================================================

def main():
    # -----------------------------------------------------------------------
    # Derive date variables
    # -----------------------------------------------------------------------
    dvars    = get_all_date_vars(REPTDATE_PATH)
    reptmon  = dvars['reptmon']
    nowk     = dvars['nowk']
    reptday  = dvars['reptday']
    reptyear = dvars['reptyear']

    # -----------------------------------------------------------------------
    # ***************************************************
    # * ESMR : 06-1485
    # ***************************************************

    # %GET_BICS: IF NOWK=4 -> MONTHLY else WEEKLY, then MRGBIC
    # -----------------------------------------------------------------------
    if nowk == '4':
        # %MONTHLY -> %INC PGM(PBBMRDLF)
        run_pbbmrdlf()
    else:
        # %WEEKLY  -> %INC PGM(PBBWRDLF)
        run_pbbwrdlf()

    # Load PBBRDAL reference
    pbbrdal_df = pl.read_parquet(PBBRDAL_PATH)

    # Load BNM.ALW&REPTMON&NOWK
    alw_path = BNM_PATH / f"alw{reptmon}{nowk}.parquet"
    con      = duckdb.connect()
    alw_df   = con.execute(f"SELECT * FROM read_parquet('{alw_path}')").pl()
    con.close()

    # %MRGBIC (EIIWRDAL variant: AMTIND always 'I')
    rdal_df = macro_mrgbic(pbbrdal_df, alw_df)

    # -----------------------------------------------------------------------
    # DATA CAG: SET LOAN.LNNOTE
    # IF PZIPCODE IN (...); IF (3000<=COSTCTR<=3999) OR COSTCTR IN (4043,4048)
    # AMTIND='I'; ITCODE='7511100000000Y'
    # -----------------------------------------------------------------------
    con2     = duckdb.connect()
    loan_df  = con2.execute(
        f"SELECT * FROM read_parquet('{LOAN_LNNOTE_PATH}')"
    ).pl()
    con2.close()

    pzipcode_list = [
        2002, 2013, 3039, 3047, 800003098, 800003114,
        800004016, 800004022, 800004029, 800040050,
        800040053, 800050024, 800060024, 800060045,
        800060081, 80060085,
    ]

    cag_df = (
        loan_df
        .filter(pl.col('PZIPCODE').is_in(pzipcode_list))
        .filter(
            ((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 3999)) |
            pl.col('COSTCTR').is_in([4043, 4048])
        )
        .with_columns([
            pl.lit('I').alias('AMTIND'),
            pl.lit('7511100000000Y').alias('ITCODE'),
        ])
    )

    # PROC SUMMARY DATA=CAG NWAY; CLASS ITCODE AMTIND; VAR BALANCE; SUM=AMOUNT
    cag_summary = (
        cag_df
        .group_by(['ITCODE', 'AMTIND'])
        .agg(pl.col('BALANCE').sum().alias('AMOUNT'))
    )

    # DATA RDAL: SET RDAL CAG (append, no delete filter here unlike P124RDAL)
    rdal_df = pl.concat([rdal_df, cag_summary], how='diagonal')

    # PROC SORT DATA=RDAL; BY ITCODE AMTIND
    rdal_df = rdal_df.sort(['ITCODE', 'AMTIND'])

    # -----------------------------------------------------------------------
    # DATA AL OB SP: split with EIIWRDAL routing rules
    # -----------------------------------------------------------------------
    al_df, ob_df, sp_df = split_al_ob_sp(rdal_df)

    # -----------------------------------------------------------------------
    # Write RDAL output file
    # -----------------------------------------------------------------------
    write_rdal_file(
        al_df=al_df,
        ob_df=ob_df,
        sp_df=sp_df,
        reptday=reptday,
        reptmon=reptmon,
        reptyear=reptyear,
        output_path=RDAL_OUTPUT_PATH,
    )
    print(f"RDAL written to: {RDAL_OUTPUT_PATH}")

    # -----------------------------------------------------------------------
    # Write NSRS output file
    # -----------------------------------------------------------------------
    write_nsrs_file(
        al_df=al_df,
        ob_df=ob_df,
        sp_df=sp_df,
        reptday=reptday,
        reptmon=reptmon,
        reptyear=reptyear,
        output_path=NSRS_OUTPUT_PATH,
    )
    print(f"NSRS written to: {NSRS_OUTPUT_PATH}")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
