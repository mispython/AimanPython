# !/usr/bin/env python3
"""
Program  : LALWP124
Purpose  : Report on Domestic Assets and Liabilities - Part I (M&I Loan).
           Filters L124/UL124 loan data, summarises by customer code,
           approved limit, and Cagamas sold loans, then appends results
           to BNM.LALW{REPTMON}{NOWK} parquet output.

Dependency: PBBLNFMT  - format/mapping functions (format_lnprod, etc.)
            L124PBBD  - produces BNM.L124{REPTMON}{NOWK} and
                        BNM.UL124{REPTMON}{NOWK} from BNM1 source data.
"""

import duckdb
import polars as pl
from pathlib import Path

# ---------------------------------------------------------------------------
# Dependency: PBBLNFMT  (%INC PGM(PBBLNFMT))
# ---------------------------------------------------------------------------
from PBBLNFMT import (
    format_lnprod,
    format_lncustcd,
    format_lndenom,
    format_apprlimt,
)

# ---------------------------------------------------------------------------
# Dependency: L124PBBD  (%INC PGM(L124PBBD))
# Produces BNM.L124&REPTMON&NOWK and BNM.UL124&REPTMON&NOWK.
# Call main() to ensure the output files exist before this program runs.
# ---------------------------------------------------------------------------
from L124PBBD import main as run_l124pbbd, get_reptmon_nowk, REPTDATE_PATH

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR  = Path("/data/sap")

# REPTDATE source (inherited from L124PBBD via shared REPTDATE_PATH)
# REPTDATE_PATH = BASE_DIR / "pbb/mniln/reptdate.parquet"  # defined in L124PBBD

# BNM library - input (produced by L124PBBD) and output
BNM_PATH  = BASE_DIR / "pbb/bnm"           # SAP target: BNM library

# ============================================================================
# BNMCODE MAPPING LOGIC
# ============================================================================

# SELECT(CUSTCD) WHEN blocks mapped to Python lookup rules
# Returns list of BNMCODE strings to output (some custcds emit 2 rows)

def get_bnmcodes_for_custcd(custcd: str) -> list:
    """
    Replicate SAS SELECT(CUSTCD) logic.
    Returns a list of BNMCODE strings (one or two entries per custcd).
    """
    grp1 = {'02', '03', '11', '12', '71', '72', '73', '74', '79'}
    grp2 = {'20', '13', '17', '30', '32', '33', '34', '35',
             '36', '37', '38', '39', '40', '04', '05', '06'}
    grp3 = {'41', '42', '43', '44', '46', '47', '48', '49', '51',
             '52', '53', '54', '60', '61', '62', '63', '64', '65',
             '59', '75', '57'}
    grp4 = {'76', '77', '78'}
    grp5 = {'81', '82', '83', '84'}
    grp6 = {'85', '86', '90', '91', '92', '95', '96', '98', '99'}

    codes = []

    if custcd in grp1:
        # BNMCODE='34100'||CUSTCD||'000000Y'
        codes.append(f'34100{custcd}000000Y')

    elif custcd in grp2:
        # BNMCODE='3410020000000Y'
        codes.append('3410020000000Y')
        # IF CUSTCD IN ('13','17') THEN ALSO output specific code
        if custcd in ('13', '17'):
            codes.append(f'34100{custcd}000000Y')

    elif custcd in grp3:
        codes.append('3410060000000Y')

    elif custcd in grp4:
        codes.append('3410076000000Y')

    elif custcd in grp5:
        codes.append('3410081000000Y')

    elif custcd in grp6:
        codes.append('3410085000000Y')

    # OTHERWISE: no output (empty list)
    return codes


# ============================================================================
# HELPER: PROC APPEND equivalent
# ============================================================================

def append_to_parquet(new_df: pl.DataFrame, target_path: Path) -> None:
    """
    PROC APPEND DATA=new_df BASE=target_path
    Reads existing parquet (if any), concatenates new_df, writes back.
    """
    if target_path.exists():
        existing_df = pl.read_parquet(target_path)
        combined_df = pl.concat([existing_df, new_df], how='diagonal')
    else:
        combined_df = new_df
    combined_df.write_parquet(target_path)


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    # -----------------------------------------------------------------------
    # Derive REPTMON and NOWK (shared with L124PBBD)
    # -----------------------------------------------------------------------
    reptmon, nowk = get_reptmon_nowk(REPTDATE_PATH)

    # -----------------------------------------------------------------------
    # %INC PGM(L124PBBD) - run dependency to produce L124 / UL124 outputs
    # -----------------------------------------------------------------------
    run_l124pbbd()

    # -----------------------------------------------------------------------
    # PROC DATASETS LIB=BNM NOLIST;
    #   DELETE LALW&REPTMON&NOWK LALM&REPTMON&NOWK LALQ&REPTMON&NOWK;
    # Remove any pre-existing output files for this run period
    # -----------------------------------------------------------------------
    lalw_path = BNM_PATH / f"lalw{reptmon}{nowk}.parquet"
    lalm_path = BNM_PATH / f"lalm{reptmon}{nowk}.parquet"
    lalq_path = BNM_PATH / f"lalq{reptmon}{nowk}.parquet"

    for p in (lalw_path, lalm_path, lalq_path):
        if p.exists():
            p.unlink()

    # -----------------------------------------------------------------------
    # DATA LOAN&REPTMON&NOWK: SET BNM.L124&REPTMON&NOWK
    # DATA ULOAN&REPTMON&NOWK: SET BNM.UL124&REPTMON&NOWK
    # -----------------------------------------------------------------------
    l124_path  = BNM_PATH / f"l124{reptmon}{nowk}.parquet"
    ul124_path = BNM_PATH / f"ul124{reptmon}{nowk}.parquet"

    con = duckdb.connect()

    loan_df  = con.execute(f"SELECT * FROM read_parquet('{l124_path}')").pl()
    uloan_df = con.execute(f"SELECT * FROM read_parquet('{ul124_path}')").pl()

    con.close()

    # -----------------------------------------------------------------------
    # SECTION 1: RM LOANS - BY CUSTOMER CODE
    #
    # PROC SUMMARY DATA=LOAN NWAY;
    # CLASS CUSTCD PRODCD AMTIND;
    # VAR BALANCE;
    # WHERE SUBSTR(PRODCD,1,3) IN ('341','342','343','344');
    # OUTPUT OUT=ALW SUM=AMOUNT;
    # -----------------------------------------------------------------------
    alw1_df = (
        loan_df
        .filter(pl.col('PRODCD').str.slice(0, 3).is_in(['341', '342', '343', '344']))
        .group_by(['CUSTCD', 'PRODCD', 'AMTIND'])
        .agg(pl.col('BALANCE').sum().alias('AMOUNT'))
    )

    # DATA ALWLOAN: SELECT(CUSTCD) -> expand to BNMCODE rows
    alwloan1_rows = []
    for row in alw1_df.to_dicts():
        custcd  = str(row.get('CUSTCD', '')).strip()
        amtind  = row.get('AMTIND')
        amount  = row.get('AMOUNT')
        for bnmcode in get_bnmcodes_for_custcd(custcd):
            alwloan1_rows.append({
                'BNMCODE': bnmcode,
                'AMTIND':  amtind,
                'AMOUNT':  amount,
            })

    if alwloan1_rows:
        alwloan1_df = pl.DataFrame(alwloan1_rows, schema={
            'BNMCODE': pl.Utf8,
            'AMTIND':  pl.Utf8,
            'AMOUNT':  pl.Float64,
        })
    else:
        alwloan1_df = pl.DataFrame(schema={
            'BNMCODE': pl.Utf8,
            'AMTIND':  pl.Utf8,
            'AMOUNT':  pl.Float64,
        })

    # PROC APPEND DATA=ALWLOAN BASE=BNM.LALW&REPTMON&NOWK
    BNM_PATH.mkdir(parents=True, exist_ok=True)
    append_to_parquet(alwloan1_df, lalw_path)
    # PROC DATASETS LIB=WORK NOLIST; DELETE ALW ALWLOAN (implicit in Python)

    # -----------------------------------------------------------------------
    # SECTION 2: GROSS LOAN - BY APPROVED LIMIT
    #
    # PROC SUMMARY DATA=LOAN NWAY;
    # CLASS PRODCD AMTIND;
    # VAR BALANCE;
    # WHERE SUBSTR(PRODCD,1,2) EQ '34' OR PRODCD EQ '54120';
    # OUTPUT OUT=ALW SUM=AMOUNT;
    # -----------------------------------------------------------------------
    alw2_df = (
        loan_df
        .filter(
            (pl.col('PRODCD').str.slice(0, 2) == '34') |
            (pl.col('PRODCD') == '54120')
        )
        .group_by(['PRODCD', 'AMTIND'])
        .agg(pl.col('BALANCE').sum().alias('AMOUNT'))
    )

    # DATA ALWLOAN: BNMCODE='3051000000000Y'
    alwloan2_df = alw2_df.with_columns(
        pl.lit('3051000000000Y').alias('BNMCODE')
    ).select(['BNMCODE', 'AMTIND', 'AMOUNT'])

    # PROC APPEND DATA=ALWLOAN BASE=BNM.LALW&REPTMON&NOWK
    append_to_parquet(alwloan2_df, lalw_path)
    # PROC DATASETS LIB=WORK NOLIST; DELETE ALW ALWLOAN

    # -----------------------------------------------------------------------
    # SECTION 3: LOANS SOLD TO CAGAMAS BERHAD WITH RECOURSE
    #
    # PROC SUMMARY DATA=LOAN NWAY;
    # VAR BALANCE;
    # CLASS PRODCD AMTIND;
    # WHERE PRODUCT IN (124,145);
    # OUTPUT OUT=ALW SUM=AMOUNT;
    # -----------------------------------------------------------------------
    alw3_df = (
        loan_df
        .filter(pl.col('PRODUCT').is_in([124, 145]))
        .group_by(['PRODCD', 'AMTIND'])
        .agg(pl.col('BALANCE').sum().alias('AMOUNT'))
    )

    # DATA ALWLOAN: BNMCODE='7511100000000Y'
    alwloan3_df = alw3_df.with_columns(
        pl.lit('7511100000000Y').alias('BNMCODE')
    ).select(['BNMCODE', 'AMTIND', 'AMOUNT'])

    # PROC APPEND DATA=ALWLOAN BASE=BNM.LALW&REPTMON&NOWK
    append_to_parquet(alwloan3_df, lalw_path)
    # PROC DATASETS LIB=WORK NOLIST; DELETE ALW ALWLOAN

    # -----------------------------------------------------------------------
    # FINAL CONSOLIDATION
    #
    # PROC SUMMARY DATA=BNM.LALW&REPTMON&NOWK NWAY;
    # CLASS BNMCODE AMTIND;
    # VAR AMOUNT;
    # OUTPUT OUT=BNM.LALW&REPTMON&NOWK (DROP=_TYPE_ _FREQ_) SUM=AMOUNT;
    # -----------------------------------------------------------------------
    lalw_df = pl.read_parquet(lalw_path)

    lalw_final = (
        lalw_df
        .group_by(['BNMCODE', 'AMTIND'])
        .agg(pl.col('AMOUNT').sum())
    )

    lalw_final.write_parquet(lalw_path)
    print(f"LALW written to: {lalw_path}  ({len(lalw_final)} rows)")

    # -----------------------------------------------------------------------
    # /*
    # OPTIONS NOCENTER NODATE NONUMBER;
    # TITLE1 'PUBLIC BANK BERHAD';
    # TITLE2 'REPORT ON DOMESTIC ASSETS AND LIABILITIES PART I - M&I LOAN';
    # TITLE3 'REPORT DATE : ' &RDATE;
    # TITLE4;
    # PROC PRINT DATA=BNM.LALW&REPTMON&NOWK;
    # FORMAT AMOUNT COMMA25.2;
    # RUN;
    # */


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
