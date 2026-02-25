# !/usr/bin/env python3
"""
Program: EIIMSTLN
Purpose: Weekly Extraction of Islamic Staff Financing for Public Bank Berhad (PIBB).
         Reads 4 weeks of LNNOTE data, filters by loan types and branches,
            then produces PROC TABULATE-style summary reports for:
                - Islamic Staff House Financing   (LOANTYPE=102, NTBRCH=29)
                - Islamic Staff Renovation Financing (LOANTYPE=105, NTBRCH=29)
                - Islamic Staff Car Financing     (LOANTYPE=103, NTBRCH=811)
                - Islamic Staff Motorcycle Financing (LOANTYPE=104, NTBRCH=811)
         TO BE RUN AFTER 22ND EVERY MONTH.
         SMR 2015-665
         Output: SAP.PIBB.STLN.TEXT(+1), RECFM=FB, LRECL=150
"""

import duckdb
import polars as pl
from pathlib import Path
import datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR = Path("/data/sap")
PGM_DIR  = Path("/data/sap/bnm/program")   # SAP.BNM.PROGRAM

# PIBB MNILN inputs: 4 generations (0,-1,-2,-3)
LN1_REPTDATE_PATH  = BASE_DIR / "pibb/mniln/gen0/reptdate.parquet"   # SAP.PIBB.MNILN(0)
LN1_LNNOTE_PATH    = BASE_DIR / "pibb/mniln/gen0/lnnote.parquet"
LN2_REPTDATE_PATH  = BASE_DIR / "pibb/mniln/gen1/reptdate.parquet"   # SAP.PIBB.MNILN(-1)
LN2_LNNOTE_PATH    = BASE_DIR / "pibb/mniln/gen1/lnnote.parquet"
LN3_REPTDATE_PATH  = BASE_DIR / "pibb/mniln/gen2/reptdate.parquet"   # SAP.PIBB.MNILN(-2)
LN3_LNNOTE_PATH    = BASE_DIR / "pibb/mniln/gen2/lnnote.parquet"
LN4_REPTDATE_PATH  = BASE_DIR / "pibb/mniln/gen3/reptdate.parquet"   # SAP.PIBB.MNILN(-3)
LN4_LNNOTE_PATH    = BASE_DIR / "pibb/mniln/gen3/lnnote.parquet"

# Output
OUTPUT_PATH = BASE_DIR / "pibb/stln/text.txt"    # SAP.PIBB.STLN.TEXT(+1)

# Report layout constants (LRECL=150, RECFM=FB - no ASA char)
LRECL      = 150
PAGE_LINES = 60

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_reptdate_z5(reptdate_parquet: Path):
    """
    Read REPTDATE and return as Z5. format (5-digit right-padded zero integer).
    SAS Z5. format on a date = internal SAS date number as 5-digit zero-padded int.
    SAS date origin: 1960-01-01.
    """
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchdf()
    con.close()
    val = df['REPTDATE'].iloc[0]
    if hasattr(val, 'date'):
        val = val.date()
    # SAS internal date = days since 1960-01-01
    sas_date = (val - datetime.date(1960, 1, 1)).days
    return sas_date, val


def fmt_ddmmyy8(d) -> str:
    """Format date as DD/MM/YY (DDMMYY8.)."""
    if d is None:
        return ' ' * 8
    try:
        if hasattr(d, 'date'):
            d = d.date()
        return d.strftime('%d/%m/%y')
    except Exception:
        return ' ' * 8


def fmt_comma18_2(value) -> str:
    """Format numeric with comma thousands separator, 18 wide, 2 decimal."""
    if value is None:
        return ' ' * 18
    try:
        return f"{float(value):>18,.2f}"
    except (ValueError, TypeError):
        return ' ' * 18


def pad_line_fb(content: str, lrecl: int) -> str:
    """Pad/truncate content to LRECL (RECFM=FB - no ASA char)."""
    return f"{content:<{lrecl}.{lrecl}}"


def load_lnnote(lnnote_path: Path, reptdate_sas: int) -> pl.DataFrame:
    """
    Load LNNOTE parquet and apply common filters.
    KEEP: NTBRCH ACCTNO BALANCE INTRATE LOANTYPE REPTDATE ACCOSTCT POFFICER
    Filter: LOANTYPE IN (102,103,104,105)
            NTBRCH IN (29,811)
            NOT(3000000000 < ACCTNO < 3999999999)
    REPTDATE = &RDATEn (SAS date integer, assigned as constant)
    """
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT NTBRCH, ACCTNO, BALANCE, INTRATE, LOANTYPE, ACCOSTCT, POFFICER "
        f"FROM read_parquet('{lnnote_path}')"
    ).pl()
    con.close()

    df = df.filter(
        pl.col('LOANTYPE').is_in([102, 103, 104, 105]) &
        pl.col('NTBRCH').is_in([29, 811]) &
        ~((pl.col('ACCTNO') > 3_000_000_000) & (pl.col('ACCTNO') < 3_999_999_999))
    )
    # REPTDATE = &RDATEn (assigned as the SAS integer date, stored as int)
    df = df.with_columns(pl.lit(reptdate_sas).alias('REPTDATE'))
    return df


# ============================================================================
# TABULATE REPORT BUILDER
# ============================================================================

def build_tabulate_report(
    ln_df: pl.DataFrame,
    title1: str,
    title2: str,
    title3: str,
    title4: str,
    where_loantype: list,
    where_ntbrch: int,
    where_intrates: list,
    box_label: str,
    lines_out: list,
    rdate: str,
):
    """
    Emulate PROC TABULATE output for Islamic Staff Financing reports.

    TABLE: REPTDATE=''*(INTRATE ALL='SUB-TOTAL') ALL='TOTAL',
           BALANCE='AMOUNT(RM)'*SUM=' '*F=COMMA18.2
    / BOX=<box_label> RTS=25 CONDENSE

    Output is written to lines_out list (RECFM=FB, no ASA char).
    """
    # Apply WHERE filters
    filt = ln_df.filter(pl.col('LOANTYPE').is_in(where_loantype) & (pl.col('NTBRCH') == where_ntbrch))
    if where_intrates:
        filt = filt.filter(pl.col('INTRATE').is_in(where_intrates))

    def emit(content: str):
        lines_out.append(pad_line_fb(content, LRECL))

    # Titles
    emit(title1)
    emit(title2)
    emit(title3)
    emit(title4)
    emit('')

    # Build aggregation: group by REPTDATE, INTRATE
    if filt.is_empty():
        emit(f"{'':25}{box_label}")
        emit('')
        return

    # Get unique sorted REPTDATE values (as SAS integers, format as DDMMYY8.)
    reptdates = sorted(filt['REPTDATE'].unique().to_list())

    # Column header: BOX RTS=25 then AMOUNT(RM) SUM
    rts = 25
    col_width = 20  # COMMA18.2 field + padding

    # Header row 1
    header1 = f"{box_label:<{rts}}{'AMOUNT(RM)':>{col_width}}"
    emit(header1)

    # Separator
    sep = '-' * (rts + col_width)
    emit(sep)

    grand_total = 0.0

    for rdt in reptdates:
        rdt_df    = filt.filter(pl.col('REPTDATE') == rdt)
        rdt_label = fmt_ddmmyy8(
            datetime.date(1960, 1, 1) + datetime.timedelta(days=int(rdt))
        )

        intrates = sorted(rdt_df['INTRATE'].unique().to_list())
        sub_total = 0.0

        for ir in intrates:
            ir_df = rdt_df.filter(pl.col('INTRATE') == ir)
            bal_sum = ir_df['BALANCE'].sum()
            sub_total += bal_sum if bal_sum else 0.0
            label = f"  {rdt_label}  {ir}"
            emit(f"{label:<{rts}}{fmt_comma18_2(bal_sum):>{col_width}}")

        # ALL='SUB-TOTAL' per REPTDATE
        emit(f"{'  ' + rdt_label + '  SUB-TOTAL':<{rts}}{fmt_comma18_2(sub_total):>{col_width}}")
        emit(sep)
        grand_total += sub_total

    # ALL='TOTAL'
    emit(f"{'TOTAL':<{rts}}{fmt_comma18_2(grand_total):>{col_width}}")
    emit(sep)
    emit('')


# ============================================================================
# MAIN
# ============================================================================

def main():
    # ------------------------------------------------------------
    # DATA REPTDATE: SET LN1.REPTDATE
    # CALL SYMPUT('REPTYEAR',...) CALL SYMPUT('REPTMON',...)
    # CALL SYMPUT('REPTDAY',...) CALL SYMPUT('RDATE',...)
    # ------------------------------------------------------------
    _, reptdate1 = get_reptdate_z5(LN1_REPTDATE_PATH)
    reptyear = reptdate1.strftime('%y')    # YEAR2.
    reptmon  = reptdate1.strftime('%m')    # Z2.
    reptday  = reptdate1.strftime('%d')    # Z2.
    rdate    = reptdate1.strftime('%d/%m/%y')  # DDMMYY8.

    # DATA _NULL_: RDATE1..RDATE4 as Z5. (SAS internal date integers)
    rdate1_sas, _ = get_reptdate_z5(LN1_REPTDATE_PATH)
    rdate2_sas, _ = get_reptdate_z5(LN2_REPTDATE_PATH)
    rdate3_sas, _ = get_reptdate_z5(LN3_REPTDATE_PATH)
    rdate4_sas, _ = get_reptdate_z5(LN4_REPTDATE_PATH)

    # ------------------------------------------------------------
    # DATA LN1..LN4: SET LNn.LNNOTE with filters; REPTDATE=&RDATEn
    # ------------------------------------------------------------
    ln1_df = load_lnnote(LN1_LNNOTE_PATH, rdate1_sas)
    ln2_df = load_lnnote(LN2_LNNOTE_PATH, rdate2_sas)
    ln3_df = load_lnnote(LN3_LNNOTE_PATH, rdate3_sas)
    ln4_df = load_lnnote(LN4_LNNOTE_PATH, rdate4_sas)

    # DATA LN: SET LN1 LN2 LN3 LN4
    ln_df = pl.concat([ln1_df, ln2_df, ln3_df, ln4_df])

    # ------------------------------------------------------------
    # Produce output lines
    # ------------------------------------------------------------
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines_out = []

    def emit(content: str):
        lines_out.append(pad_line_fb(content, LRECL))

    # DATA _NULL_: IF TRN=0 THEN write "NO RECORDS" header
    trn = len(ln_df)
    if trn == 0:
        emit('REPORT ID: EIIMSTLN')
        emit('PUBLIC BANK BERHAD')
        emit('WEEKLY EXTRACTION OF ISLAMIC STAFF FINANCING')
        emit(f'AS AT {rdate}')
        emit(' ')
        emit(' /**************************/')
        emit(' /*                        */')
        emit(' /*       NO RECORDS       */')
        emit(' /*                        */')
        emit(' /**************************/')
        emit(' ')

    # ------------------------------------------------------------
    # *** ISLAMIC STAFF HOUSE FINANCING ***
    # WHERE LOANTYPE IN (102) AND NTBRCH EQ 29 AND INTRATE IN (0,1,1.5,2)
    # ------------------------------------------------------------
    build_tabulate_report(
        ln_df=ln_df,
        title1='REPORT ID: EIIMSTLN',
        title2='PUBLIC BANK BERHAD',
        title3='WEEKLY EXTRACTION OF ISLAMIC STAFF HOUSE FINANCING',
        title4=f'AS AT {rdate}',
        where_loantype=[102],
        where_ntbrch=29,
        where_intrates=[0, 1, 1.5, 2],
        box_label='BRANCH = 29',
        lines_out=lines_out,
        rdate=rdate,
    )

    # ------------------------------------------------------------
    # *** ISLAMIC STAFF RENOVATION FINANCING ***
    # WHERE LOANTYPE IN (105) AND NTBRCH EQ 29
    # ------------------------------------------------------------
    build_tabulate_report(
        ln_df=ln_df,
        title1='REPORT ID: EIIMSTLN',
        title2='PUBLIC BANK BERHAD',
        title3='WEEKLY EXTRACTION OF ISLAMIC STAFF RENOVATION FINANCING',
        title4=f'AS AT {rdate}',
        where_loantype=[105],
        where_ntbrch=29,
        where_intrates=[],    # no INTRATE filter
        box_label='BRANCH = 29',
        lines_out=lines_out,
        rdate=rdate,
    )

    # ------------------------------------------------------------
    # *** ISLAMIC STAFF CAR FINANCING ***
    # WHERE LOANTYPE IN (103) AND NTBRCH EQ 811
    # ------------------------------------------------------------
    build_tabulate_report(
        ln_df=ln_df,
        title1='REPORT ID: EIIMSTLN',
        title2='PUBLIC BANK BERHAD',
        title3='WEEKLY EXTRACTION OF ISLAMIC STAFF CAR FINANCING',
        title4=f'AS AT {rdate}',
        where_loantype=[103],
        where_ntbrch=811,
        where_intrates=[],
        box_label='BRANCH = 811',
        lines_out=lines_out,
        rdate=rdate,
    )

    # ------------------------------------------------------------
    # *** ISLAMIC STAFF MOTORCYCLE FINANCING ***
    # WHERE LOANTYPE IN (104) AND NTBRCH EQ 811
    # ------------------------------------------------------------
    build_tabulate_report(
        ln_df=ln_df,
        title1='REPORT ID: EIIMSTLN',
        title2='PUBLIC BANK BERHAD',
        title3='WEEKLY EXTRACTION OF ISLAMIC STAFF MOTORCYCLE FINANCING',
        title4=f'AS AT {rdate}',
        where_loantype=[104],
        where_ntbrch=811,
        where_intrates=[],
        box_label='BRANCH = 811',
        lines_out=lines_out,
        rdate=rdate,
    )

    # Write output (RECFM=FB - no ASA carriage control)
    with open(OUTPUT_PATH, 'w', encoding='utf-8', newline='\n') as f:
        for line in lines_out:
            f.write(line + '\n')

    print(f"Report written to: {OUTPUT_PATH}")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
