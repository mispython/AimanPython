# !/usr/bin/env python3
"""
Program  : PIBBRELP  (original header: PBBBRELP)
Date     : 09/05/97
Purpose  : Branch Eligible Liabilities Report.
           - Loads EL definitions from PBBELF.
           - Derives FIRST/SECOND week range from REPTDAY.
           - Runs three variants: BRCHEL (consolidated), CONVEL (conventional),
             ISLEL (islamic), each followed by TOTALEL to compute EL totals.
           - Produces a fixed-width report with ASA carriage control characters
             showing Branch Eligible Liabilities per REPTDATE and average EL.
           Output: RECFM=FBA, LRECL=133 (default SAS PROC REPORT width), 60 lines/page.
"""

import duckdb
import polars as pl
from pathlib import Path
import datetime

# ---------------------------------------------------------------------------
# Dependency: PBBELF (%INC PGM(PBBELF))
# EL_DEFINITIONS contains bnmcode/sign/fmtname mappings.
# ---------------------------------------------------------------------------
from PBBELF import (
    EL_DEFINITIONS,
    BRCHCD_MAP,
    format_brchcd,
)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR  = Path("/data/sap")

# BNM library - contains MELW and WELW parquet files
BNM_PATH  = BASE_DIR / "pbb/bnm"          # SAP.PBB.BNM (BNM library)

# Macro variables injected from upstream (e.g. EIBWWKLY).
# At runtime these should be set via environment or a shared config.
# Here we read directly from a known REPTDATE parquet.
REPTDATE_PATH = BASE_DIR / "pbb/mniln/reptdate.parquet"

# Output report file
OUTPUT_PATH   = BASE_DIR / "pibb/relp/pibbrelp.txt"

# Report layout
LRECL      = 133
PAGE_LINES = 60

# SDESC placeholder (set by upstream job; default for standalone run)
SDESC = 'PUBLIC ISLAMIC BANK BERHAD'

# ============================================================================
# EL LOOKUP TABLE  (from PBBELF.EL_DEFINITIONS)
# ============================================================================

def build_el_lookup() -> dict:
    """Build {bnmcode: {sign, fmtname}} from EL_DEFINITIONS."""
    return {
        row['bnmcode']: {'sign': row['sign'], 'fmtname': row['fmtname']}
        for row in EL_DEFINITIONS
        if row.get('bnmcode') and row.get('fmtname')
    }

EL_LOOKUP = build_el_lookup()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_reptday_and_vars(reptdate_parquet: Path) -> dict:
    """
    Read REPTDATE parquet and derive REPTDAY, REPTMON, REPTYEAR, RDATE,
    plus FIRST/SECOND week index variables.
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
    reptmon  = reptdate.strftime('%m')
    reptyear = reptdate.strftime('%Y')
    reptday  = f"{day:02d}"
    rdate    = reptdate.strftime('%d/%m/%y')    # DDMMYY8.

    # DATA _NULL_: SELECT(&REPTDAY) -> FIRST, SECOND
    if day == 8:
        first, second = 1, 1
    elif day == 15:
        first, second = 1, 2
    elif day == 22:
        first, second = 2, 3
    else:
        first, second = 3, 4

    return {
        'reptdate': reptdate,
        'reptday':  reptday,
        'reptmon':  reptmon,
        'reptyear': reptyear,
        'rdate':    rdate,
        'first':    first,
        'second':   second,
    }


def load_melw(reptmon: str, i: int, exclude_branches: bool) -> pl.DataFrame:
    """
    PROC SUMMARY DATA=BNM.MELW&REPTMON&I NWAY;
    WHERE BRANCH NOT IN (996,997,998)  [only when exclude_branches=True]
    CLASS BRANCH REPTDATE BNMCODE; VAR AMOUNT; SUM=AMOUNT
    Then remap BNMCODE '4939980*' -> '4929980000000Y' and re-summarise.
    """
    path = BNM_PATH / f"melw{reptmon}{i}.parquet"
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()

    if exclude_branches:
        df = df.filter(~pl.col('BRANCH').is_in([996, 997, 998]))

    # First PROC SUMMARY
    df = (
        df.group_by(['BRANCH', 'REPTDATE', 'BNMCODE'])
          .agg(pl.col('AMOUNT').sum())
    )

    # DATA: remap BNMCODE starting with '4939980' -> '4929980000000Y'
    df = df.with_columns(
        pl.when(pl.col('BNMCODE').str.slice(0, 7) == '4939980')
          .then(pl.lit('4929980000000Y'))
          .otherwise(pl.col('BNMCODE'))
          .alias('BNMCODE')
    )

    # Second PROC SUMMARY after remap
    df = (
        df.group_by(['BRANCH', 'REPTDATE', 'BNMCODE'])
          .agg(pl.col('AMOUNT').sum())
    )

    return df


def load_welw(reptmon: str, i: int, exclude_branches: bool) -> pl.DataFrame:
    """
    PROC SUMMARY DATA=BNM.WELW&REPTMON&I NWAY;
    WHERE BRANCH NOT IN (996,997,998)  [only when exclude_branches=True]
    CLASS BRANCH REPTDATE BNMCODE; VAR AMOUNT; SUM=AMOUNT
    """
    path = BNM_PATH / f"welw{reptmon}{i}.parquet"
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()

    if exclude_branches:
        df = df.filter(~pl.col('BRANCH').is_in([996, 997, 998]))

    df = (
        df.group_by(['BRANCH', 'REPTDATE', 'BNMCODE'])
          .agg(pl.col('AMOUNT').sum())
    )
    return df


def combine_elw(welw_df: pl.DataFrame, melw_df: pl.DataFrame) -> pl.DataFrame:
    """
    COMBINE THE EL ITEMS FROM WALKER AND M&I:
    MERGE WELW (IN=A RENAME AMOUNT->WISAMT)
          MELW (IN=B RENAME AMOUNT->MNIAMT)
    BY BRANCH REPTDATE BNMCODE;

    Rules:
      A AND B:
        AMOUNT = WISAMT
        IF BNMCODE[0:7] IN ('4911080','4929980') THEN AMOUNT = WISAMT + MNIAMT
      A AND NOT B:  AMOUNT = WISAMT
      B AND NOT A AND BNMCODE != '4218000000000Y': AMOUNT = MNIAMT
      ELSE: AMOUNT = WISAMT  (covers B AND NOT A AND BNMCODE='4218000000000Y')
    """
    merged = welw_df.rename({'AMOUNT': 'WISAMT'}).join(
        melw_df.rename({'AMOUNT': 'MNIAMT'}),
        on=['BRANCH', 'REPTDATE', 'BNMCODE'],
        how='outer',
        suffix='_M',
    )

    special_bnm = ['4911080', '4929980']

    merged = merged.with_columns(
        pl.when(
            pl.col('WISAMT').is_not_null() & pl.col('MNIAMT').is_not_null() &
            pl.col('BNMCODE').str.slice(0, 7).is_in(special_bnm)
        ).then(pl.col('WISAMT') + pl.col('MNIAMT'))
        .when(
            pl.col('WISAMT').is_not_null() & pl.col('MNIAMT').is_not_null()
        ).then(pl.col('WISAMT'))
        .when(
            pl.col('WISAMT').is_not_null() & pl.col('MNIAMT').is_null()
        ).then(pl.col('WISAMT'))
        .when(
            pl.col('WISAMT').is_null() & pl.col('MNIAMT').is_not_null() &
            (pl.col('BNMCODE') != '4218000000000Y')
        ).then(pl.col('MNIAMT'))
        .otherwise(pl.col('WISAMT'))
        .alias('AMOUNT')
    ).select(['BRANCH', 'REPTDATE', 'BNMCODE', 'AMOUNT'])

    return merged


def get_elw_for_range(
    reptmon: str,
    first: int,
    second: int,
    exclude_branches: bool,
) -> pl.DataFrame:
    """
    %DO I=&FIRST %TO &SECOND: load and combine MELW/WELW for each week index,
    then SET ELW&FIRST ELW&SECOND (union).
    """
    parts = []
    for i in range(first, second + 1):
        melw = load_melw(reptmon, i, exclude_branches)
        welw = load_welw(reptmon, i, exclude_branches)
        elwi = combine_elw(welw, melw)
        parts.append(elwi)

    return pl.concat(parts, how='diagonal') if parts else pl.DataFrame()


# ============================================================================
# %MACRO TOTALEL
# ============================================================================

def macro_totalel(elw_df: pl.DataFrame) -> pl.DataFrame:
    """
    %MACRO TOTALEL:
    1. Join ELW with EL lookup table on BNMCODE to get SIGN and FMTNAME.
    2. Sort BY BRANCH REPTDATE FMTNAME.
    3. Count distinct REPTDATE values (NUMDAY).
    4. Per BRANCH/REPTDATE group: accumulate FXEL, FXEA, RMEL, RMEA by FMTNAME and SIGN.
    5. Compute EL = RMEL + FXEL - RMEA - MIN(FXEL, FXEA).
    6. AVGEL = EL / NUMDAY.
    Returns BRCHEL: (REPTDATE, BRANCH, EL, AVGEL).
    """
    # Build EL reference DataFrame from EL_LOOKUP
    el_ref_rows = [
        {'BNMCODE': k, 'SIGN': v['sign'], 'FMTNAME': v['fmtname']}
        for k, v in EL_LOOKUP.items()
        if v.get('fmtname') in ('FXEL', 'FXEA', 'RMEL', 'RMEA')
    ]
    el_ref_df = pl.DataFrame(el_ref_rows)

    # PROC SQL: CREATE TABLE ELW AS SELECT ... FROM ELW T1, EL T2 WHERE BNMCODE matches
    elw_joined = elw_df.join(el_ref_df, on='BNMCODE', how='inner')

    # Sort BY BRANCH REPTDATE FMTNAME
    elw_joined = elw_joined.sort(['BRANCH', 'REPTDATE', 'FMTNAME'])

    # NUMDAY = count distinct REPTDATE values
    numday = elw_joined['REPTDATE'].n_unique()
    if numday == 0:
        numday = 1

    # Aggregate: per BRANCH/REPTDATE/FMTNAME, sum signed amounts
    def signed_sum(df: pl.DataFrame, fmtname: str) -> pl.DataFrame:
        sub = df.filter(pl.col('FMTNAME') == fmtname).with_columns(
            pl.when(pl.col('SIGN') == '+')
              .then(pl.col('AMOUNT'))
              .otherwise(-pl.col('AMOUNT'))
              .alias('SIGNED')
        )
        return (
            sub.group_by(['BRANCH', 'REPTDATE'])
               .agg(pl.col('SIGNED').sum().alias(fmtname))
        )

    fxel_df = signed_sum(elw_joined, 'FXEL')
    fxea_df = signed_sum(elw_joined, 'FXEA')
    rmel_df = signed_sum(elw_joined, 'RMEL')
    rmea_df = signed_sum(elw_joined, 'RMEA')

    # Get all BRANCH/REPTDATE combinations present in elw_joined
    keys_df = (
        elw_joined.select(['BRANCH', 'REPTDATE'])
                  .unique()
    )

    # Join all four components
    result = (
        keys_df
        .join(fxel_df, on=['BRANCH', 'REPTDATE'], how='left')
        .join(fxea_df, on=['BRANCH', 'REPTDATE'], how='left')
        .join(rmel_df, on=['BRANCH', 'REPTDATE'], how='left')
        .join(rmea_df, on=['BRANCH', 'REPTDATE'], how='left')
        .fill_null(0.0)
    )

    # EL = SUM(RMEL, FXEL, (-1)*RMEA, (-1)*MIN(FXEL, FXEA))
    result = result.with_columns([
        (
            pl.col('RMEL') + pl.col('FXEL') - pl.col('RMEA') -
            pl.min_horizontal(['FXEL', 'FXEA'])
        ).alias('EL')
    ]).with_columns(
        (pl.col('EL') / numday).alias('AVGEL')
    ).select(['REPTDATE', 'BRANCH', 'EL', 'AVGEL'])

    return result


# ============================================================================
# %MACRO REPORT  (ASA report writer)
# ============================================================================

def fmt_comma18_2(value) -> str:
    """Format numeric as COMMA18.2 right-justified."""
    if value is None:
        return ' ' * 18
    try:
        return f"{float(value):>18,.2f}"
    except (ValueError, TypeError):
        return ' ' * 18


def fmt_ddmmyy8(d) -> str:
    """Format date as DD/MM/YY (DDMMYY8.)."""
    if d is None:
        return ' ' * 8
    try:
        if isinstance(d, (int, float)):
            d = datetime.date(1960, 1, 1) + datetime.timedelta(days=int(d))
        if hasattr(d, 'date'):
            d = d.date()
        return d.strftime('%d/%m/%y')
    except Exception:
        return ' ' * 8


def pad_line(content: str, lrecl: int) -> str:
    """Pad content (no ASA char) to LRECL-1."""
    body = lrecl - 1
    return f"{content:<{body}.{body}}"


def asa_newpage() -> str:
    return '1'


def asa_newline() -> str:
    return ' '


def macro_report(
    brchel_df: pl.DataFrame,
    rpt: str,
    rdate: str,
    tbl_labels: dict,
    output_path: Path,
    append: bool,
):
    """
    %MACRO REPORT:
    PROC REPORT DATA=BRCHEL NOWD HEADLINE SPLIT='*'
      COLUMN BRANCH REPTDATE,EL EL=TOTAL AVGEL
      DEFINE BRANCH   / GROUP FORMAT=BRCHCD. ORDER=INTERNAL 'BRH' ID
      DEFINE REPTDATE / ACROSS FORMAT=DDMMYY8. ' '
      DEFINE EL       / ANALYSIS SUM FORMAT=COMMA18.2 ' '
      DEFINE TOTAL    / ANALYSIS SUM FORMAT=COMMA18.2 'TOTAL EL'
      DEFINE AVGEL    / ANALYSIS SUM FORMAT=COMMA18.2 'AVERAGE EL'
      RBREAK AFTER / OL DUL SUMMARIZE

    Produces ASA-controlled fixed-width report appended or new to output_path.
    """
    tbl_label = tbl_labels.get(rpt, '')

    if brchel_df.is_empty():
        return

    # Distinct REPTDATE columns (sorted) - used as ACROSS variable
    reptdates = sorted(brchel_df['REPTDATE'].unique().to_list())
    reptdate_hdrs = [fmt_ddmmyy8(d) for d in reptdates]

    # Column widths: BRH(4) + each REPTDATE EL(18) + TOTAL EL(18) + AVERAGE EL(18)
    brh_width   = 10
    el_width    = 18
    col_sep     = 2

    # Build header rows
    # Row 1: blank for BRH, then date labels for each REPTDATE, then 'TOTAL EL', 'AVERAGE EL'
    def build_col_header_row1() -> str:
        line = f"{'BRH':<{brh_width}}"
        for hdr in reptdate_hdrs:
            line += ' ' * col_sep + f"{hdr:>{el_width}}"
        line += ' ' * col_sep + f"{'TOTAL EL':>{el_width}}"
        line += ' ' * col_sep + f"{'AVERAGE EL':>{el_width}}"
        return line

    def build_underline() -> str:
        line = '=' * brh_width
        for _ in reptdate_hdrs:
            line += ' ' * col_sep + '=' * el_width
        line += ' ' * col_sep + '=' * el_width
        line += ' ' * col_sep + '=' * el_width
        return line

    hdr_row = build_col_header_row1()
    uline   = build_underline()

    lines      = []
    line_count = PAGE_LINES + 1   # force new page at start

    def emit(asa: str, content: str):
        nonlocal line_count
        lines.append(asa + pad_line(content, LRECL))
        if asa != '+':
            line_count += 1

    def emit_page_header():
        nonlocal line_count
        emit(asa_newpage(), SDESC)
        emit(asa_newline(), f"BRANCH ELIGIBLE LIABILITIES {tbl_label}")
        emit(asa_newline(), f"REPORT DATE : {rdate}")
        emit(asa_newline(), 'REPORT ID : PIBBRELP')
        emit(asa_newline(), '')
        # HEADLINE
        emit(asa_newline(), hdr_row)
        emit(asa_newline(), uline)
        line_count = 7

    def check_page():
        if line_count + 1 > PAGE_LINES:
            emit_page_header()

    # Sort: BY BRANCH (internal order)
    brchel_sorted = brchel_df.sort(['BRANCH', 'REPTDATE'])
    branches      = brchel_sorted['BRANCH'].unique(maintain_order=False).sort().to_list()

    # Grand totals accumulators
    grand_el_by_date = {d: 0.0 for d in reptdates}
    grand_total_el   = 0.0
    grand_avgel      = 0.0

    emit_page_header()

    for branch in branches:
        brch_df  = brchel_sorted.filter(pl.col('BRANCH') == branch)
        brch_lbl = format_brchcd(branch)

        # Per-branch EL per REPTDATE
        el_by_date = {}
        total_el   = 0.0
        avg_el_sum = 0.0

        for row in brch_df.to_dicts():
            rd   = row['REPTDATE']
            el   = float(row['EL']    or 0)
            avgel = float(row['AVGEL'] or 0)
            el_by_date[rd] = el
            total_el  += el
            avg_el_sum += avgel

        check_page()
        line = f"{brch_lbl:<{brh_width}}"
        for rd in reptdates:
            el_val = el_by_date.get(rd, 0.0)
            line  += ' ' * col_sep + fmt_comma18_2(el_val)
            grand_el_by_date[rd] = grand_el_by_date.get(rd, 0.0) + el_val
        line += ' ' * col_sep + fmt_comma18_2(total_el)
        line += ' ' * col_sep + fmt_comma18_2(avg_el_sum)
        emit(asa_newline(), line)

        grand_total_el += total_el
        grand_avgel    += avg_el_sum

    # RBREAK AFTER / OL DUL SUMMARIZE
    # OL = overline (separator before), DUL = double underline (separator after)
    sep = '=' * len(uline.rstrip())
    check_page()
    emit(asa_newline(), sep)
    # Summary row (grand totals)
    grand_line = f"{'':>{brh_width}}"
    for rd in reptdates:
        grand_line += ' ' * col_sep + fmt_comma18_2(grand_el_by_date.get(rd, 0.0))
    grand_line += ' ' * col_sep + fmt_comma18_2(grand_total_el)
    grand_line += ' ' * col_sep + fmt_comma18_2(grand_avgel)
    emit(asa_newline(), grand_line)
    emit(asa_newline(), sep)
    emit(asa_newline(), sep)

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    mode = 'a' if append else 'w'
    with open(output_path, mode, encoding='utf-8', newline='\n') as f:
        for ln in lines:
            f.write(ln + '\n')


# ============================================================================
# MAIN
# ============================================================================

def main():
    # -----------------------------------------------------------------------
    # Read REPTDATE variables
    # -----------------------------------------------------------------------
    dvars    = get_reptday_and_vars(REPTDATE_PATH)
    reptmon  = dvars['reptmon']
    rdate    = dvars['rdate']
    first    = dvars['first']
    second   = dvars['second']

    # %LET TBL1=(CONVENTIONAL+ISLAMIC)
    # %LET TBL2=(CONVENTIONAL)
    # %LET TBL3=(ISLAMIC)
    tbl_labels = {
        '1': '(CONVENTIONAL+ISLAMIC)',
        '2': '(CONVENTIONAL)',
        '3': '(ISLAMIC)',
    }

    # OPTIONS NOCENTER NONUMBER NODATE MISSING=0

    # -----------------------------------------------------------------------
    # CONSOLIDATED EL REPORT (%BRCHEL -> %TOTALEL)
    # BRCHEL: exclude branches 996/997/998, week range FIRST..SECOND
    # -----------------------------------------------------------------------
    # DATA _NULL_: CALL SYMPUT('RPT','1')
    rpt = '1'
    elw_brchel  = get_elw_for_range(reptmon, first, second, exclude_branches=True)
    brchel_cons = macro_totalel(elw_brchel)

    # -----------------------------------------------------------------------
    # CONVENTIONAL EL REPORT (%CONVEL -> %TOTALEL)
    # CONVEL: same logic as BRCHEL (exclude 996/997/998)
    # -----------------------------------------------------------------------
    # DATA _NULL_: CALL SYMPUT('RPT','2')
    rpt = '2'
    elw_convel  = get_elw_for_range(reptmon, first, second, exclude_branches=True)
    brchel_conv = macro_totalel(elw_convel)

    # -----------------------------------------------------------------------
    # ISLAMIC EL REPORT (%ISLEL -> %TOTALEL)
    # ISLEL: does NOT exclude branches (no WHERE clause)
    # -----------------------------------------------------------------------
    # DATA _NULL_: CALL SYMPUT('RPT','3')
    rpt = '3'
    elw_islel   = get_elw_for_range(reptmon, first, second, exclude_branches=False)
    brchel_isl  = macro_totalel(elw_islel)

    # -----------------------------------------------------------------------
    # %REPORT: only called after %ISLEL in SAS (RPT='3' at time of REPORT call)
    # The REPORT macro uses &&TBL&RPT which resolves to &TBL3=(ISLAMIC) at call time.
    # However all three BRCHEL datasets are written sequentially to the same output.
    # We emit all three sections to the output file in order.
    # -----------------------------------------------------------------------

    # PROC PRINTTO PRINT=ELIAB -> output file
    # Write consolidated (RPT=1)
    macro_report(
        brchel_df=brchel_cons,
        rpt='1',
        rdate=rdate,
        tbl_labels=tbl_labels,
        output_path=OUTPUT_PATH,
        append=False,
    )
    # Write conventional (RPT=2)
    macro_report(
        brchel_df=brchel_conv,
        rpt='2',
        rdate=rdate,
        tbl_labels=tbl_labels,
        output_path=OUTPUT_PATH,
        append=True,
    )
    # Write islamic (RPT=3) - this is the %REPORT call in SAS (last %TOTALEL)
    macro_report(
        brchel_df=brchel_isl,
        rpt='3',
        rdate=rdate,
        tbl_labels=tbl_labels,
        output_path=OUTPUT_PATH,
        append=True,
    )

    print(f"Report written to: {OUTPUT_PATH}")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
