# !/usr/bin/env python3
"""
Program: EIBXLNLC
Purpose: Loan data preparation - merges LNNOTE, LOAN, and LNCOMM datasets
            to produce NOTE1 (all loans by FISSPURP) and NOTE2 (construction/
            real estate loans for non-individual customers) for both PBB and PIBB.
         Output datasets are saved as parquet files consumed by EIBMLN1C/EIBMLN2C.
"""

import duckdb
import polars as pl
from pathlib import Path
import datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR = Path("/data/sap")

# PBB input paths
PBB_LNNOTE_REPTDATE_PATH = BASE_DIR / "pbb/mniln/reptdate.parquet"    # SAP.PBB.MNILN(0) - REPTDATE
PBB_LNNOTE_PATH          = BASE_DIR / "pbb/mniln/lnnote.parquet"      # SAP.PBB.MNILN(0) - LNNOTE
PBB_LOAN_BASE_PATH       = BASE_DIR / "pbb/sasdata"                   # SAP.PBB.SASDATA  - LOAN{MM}{WK}
PBB_LNCOMM_PATH          = BASE_DIR / "pbb/mniln/lncomm.parquet"      # SAP.PBB.MNILN(0) - LNCOMM
PBB_LNLC_PATH            = BASE_DIR / "pbb/loanlist/sasdata"          # SAP.PBB.LOANLIST.SASDATA (output)

# PIBB input paths
PIBB_LNNOTE_REPTDATE_PATH = BASE_DIR / "pibb/mniln/reptdate.parquet"  # SAP.PIBB.MNILN(0) - REPTDATE
PIBB_LNNOTE_PATH          = BASE_DIR / "pibb/mniln/lnnote.parquet"    # SAP.PIBB.MNILN(0) - LNNOTE
PIBB_LOAN_BASE_PATH       = BASE_DIR / "pibb/sasdata"                 # SAP.PIBB.SASDATA  - LOAN{MM}{WK}
PIBB_LNCOMM_PATH          = BASE_DIR / "pibb/mniln/lncomm.parquet"    # SAP.PIBB.MNILN(0) - LNCOMM
PIBB_LNLCI_PATH           = BASE_DIR / "pibb/loanlist/sasdata"        # SAP.PIBB.LOANLIST.SASDATA (output)

# ============================================================================
# BANK FORMAT MAP (PROC FORMAT VALUE BANKFMT)
# ============================================================================
# VALUE BANKFMT 33='PBB' 134='PFB'
BANKFMT = {33: 'PBB', 134: 'PFB'}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_week_code(reptdate) -> tuple:
    """
    Determine NOWK and REPTMON/REPTYEAR from REPTDATE.
    SELECT(DAY(REPTDATE)):
      WHEN(8)  -> WK='1'
      WHEN(15) -> WK='2'
      WHEN(22) -> WK='3'
      OTHERWISE -> WK='4'
    """
    if hasattr(reptdate, 'date'):
        reptdate = reptdate.date()
    day      = reptdate.day
    reptmon  = reptdate.strftime('%m')
    reptyear = reptdate.strftime('%Y')
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'
    return nowk, reptmon, reptyear


def get_report_date(reptdate_parquet: Path) -> tuple:
    """Read REPTDATE and derive week/month/year codes."""
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchdf()
    con.close()
    reptdate = df['REPTDATE'].iloc[0]
    return get_week_code(reptdate)


# ============================================================================
# CORE PROCESSING FUNCTION (shared by PBB and PIBB)
# ============================================================================

def process_loan_data(
    lnnote_reptdate_path: Path,
    lnnote_path: Path,
    loan_base_path: Path,
    lncomm_path: Path,
    output_path: Path,
    entity_label: str,
):
    """
    Shared processing logic for both PBB and PIBB.
    Produces NOTE1&REPTMON and NOTE2&REPTMON parquet output files.
    """
    # ----------------------------------------------------------------
    # DATA _NULL_: determine NOWK, REPTMON, REPTYEAR
    # ----------------------------------------------------------------
    nowk, reptmon, reptyear = get_report_date(lnnote_reptdate_path)
    # rdate = reptdate.strftime('%d/%m/%y')  # DDMMYY8. (not used in output here)

    print(f"[{entity_label}] REPTMON={reptmon}, NOWK={nowk}, REPTYEAR={reptyear}")

    # ----------------------------------------------------------------
    # PROC SORT DATA=LNNOTE.LNNOTE (KEEP=ACCTNO NOTENO BANKNO STATE)
    # OUT=LNNOTE; BY ACCTNO NOTENO
    # ----------------------------------------------------------------
    con = duckdb.connect()
    lnnote_df = con.execute(
        f"SELECT ACCTNO, NOTENO, BANKNO, STATE "
        f"FROM read_parquet('{lnnote_path}') "
        f"ORDER BY ACCTNO, NOTENO"
    ).pl()

    # ----------------------------------------------------------------
    # PROC SORT DATA=LOAN.LOAN&REPTMON&NOWK OUT=LOAN; BY ACCTNO NOTENO
    # ----------------------------------------------------------------
    loan_path = loan_base_path / f"loan{reptmon}{nowk}.parquet"
    loan_df = con.execute(
        f"SELECT * FROM read_parquet('{loan_path}') ORDER BY ACCTNO, NOTENO"
    ).pl()
    con.close()

    # ----------------------------------------------------------------
    # DATA LNOTE: MERGE LOAN(IN=A) LNNOTE(IN=B); BY ACCTNO NOTENO
    # IF ACCTYPE = 'LN'
    # KEEP: BANKNO BRANCH ACCTNO NOTENO NAME BALANCE SECTORCD CUSTCD
    #       INTRATE NTBRCH COMMNO LIABCODE APPRLIMT FISSPURP STATE
    # ----------------------------------------------------------------
    lnote_df = loan_df.join(lnnote_df, on=['ACCTNO', 'NOTENO'], how='left', suffix='_NOTE')

    lnote_df = lnote_df.filter(pl.col('ACCTYPE') == 'LN')

    keep_lnote = [
        'BANKNO', 'BRANCH', 'ACCTNO', 'NOTENO', 'NAME', 'BALANCE',
        'SECTORCD', 'CUSTCD', 'INTRATE', 'NTBRCH', 'COMMNO', 'LIABCODE',
        'APPRLIMT', 'FISSPURP', 'STATE',
    ]
    # Use _NOTE suffix columns for BANKNO/STATE from lnnote if they exist
    for col in keep_lnote:
        note_col = col + '_NOTE'
        if note_col in lnote_df.columns and col not in lnote_df.columns:
            lnote_df = lnote_df.rename({note_col: col})
    # Drop any remaining _NOTE suffix columns
    drop_cols = [c for c in lnote_df.columns if c.endswith('_NOTE')]
    lnote_df  = lnote_df.drop(drop_cols)
    lnote_df  = lnote_df.select([c for c in keep_lnote if c in lnote_df.columns])

    # PROC SORT DATA=LNOTE; BY ACCTNO COMMNO
    lnote_df = lnote_df.sort(['ACCTNO', 'COMMNO'])

    # ----------------------------------------------------------------
    # PROC SORT DATA=LNNOTE.LNCOMM OUT=LNCOMM; BY ACCTNO COMMNO
    # ----------------------------------------------------------------
    con2     = duckdb.connect()
    lncomm_df = con2.execute(
        f"SELECT * FROM read_parquet('{lncomm_path}') ORDER BY ACCTNO, COMMNO"
    ).pl()
    con2.close()

    # ----------------------------------------------------------------
    # DATA NOTE1: MERGE LNOTE(IN=A) LNCOMM(IN=B); BY ACCTNO COMMNO; IF A
    # KEEP: BANKNO BRANCH ACCTNO NOTENO NAME APPRLIMT BALANCE SECTORCD
    #       CUSTCD STATE INTRATE NTBRCH COMMNO LIABCODE CCOLLTRL FISSPURP
    # ----------------------------------------------------------------
    note1_df = lnote_df.join(lncomm_df, on=['ACCTNO', 'COMMNO'], how='left', suffix='_COMM')

    keep_note1 = [
        'BANKNO', 'BRANCH', 'ACCTNO', 'NOTENO', 'NAME', 'APPRLIMT', 'BALANCE',
        'SECTORCD', 'CUSTCD', 'STATE', 'INTRATE', 'NTBRCH', 'COMMNO',
        'LIABCODE', 'CCOLLTRL', 'FISSPURP',
    ]
    drop_cols = [c for c in note1_df.columns if c.endswith('_COMM')]
    note1_df  = note1_df.drop(drop_cols)
    note1_df  = note1_df.select([c for c in keep_note1 if c in note1_df.columns])

    # ----------------------------------------------------------------
    # DATA NOTE2: SET NOTE1
    # IF CUSTCD NOT IN ('77','78','95','96') AND
    #    (SUBSTR(SECTORCD,1,1) = '5' OR SECTORCD = '8310')
    # ----------------------------------------------------------------
    note2_df = note1_df.filter(
        (~pl.col('CUSTCD').cast(pl.Utf8).is_in(['77', '78', '95', '96'])) &
        (
            (pl.col('SECTORCD').cast(pl.Utf8).str.slice(0, 1) == '5') |
            (pl.col('SECTORCD').cast(pl.Utf8) == '8310')
        )
    )

    # PROC DATASETS LIB=WORK NOLIST; DELETE LNOTE LNCOMM (implicit - not needed in Python)

    # ----------------------------------------------------------------
    # PROC SORT DATA=NOTE1 OUT=LNLC.NOTE1&REPTMON; BY BRANCH FISSPURP CUSTCD ACCTNO
    # PROC SORT DATA=NOTE2 OUT=LNLC.NOTE2&REPTMON; BY BRANCH SECTORCD CUSTCD ACCTNO
    # ----------------------------------------------------------------
    output_path.mkdir(parents=True, exist_ok=True)

    note1_sorted = note1_df.sort(['BRANCH', 'FISSPURP', 'CUSTCD', 'ACCTNO'])
    note2_sorted = note2_df.sort(['BRANCH', 'SECTORCD', 'CUSTCD', 'ACCTNO'])

    note1_out = output_path / f"note1{reptmon}.parquet"
    note2_out = output_path / f"note2{reptmon}.parquet"

    note1_sorted.write_parquet(note1_out)
    note2_sorted.write_parquet(note2_out)

    print(f"[{entity_label}] NOTE1 written to: {note1_out}  ({len(note1_sorted)} rows)")
    print(f"[{entity_label}] NOTE2 written to: {note2_out}  ({len(note2_sorted)} rows)")


# ============================================================================
# MAIN
# ============================================================================

def main():
    # PBB processing
    process_loan_data(
        lnnote_reptdate_path=PBB_LNNOTE_REPTDATE_PATH,
        lnnote_path=PBB_LNNOTE_PATH,
        loan_base_path=PBB_LOAN_BASE_PATH,
        lncomm_path=PBB_LNCOMM_PATH,
        output_path=PBB_LNLC_PATH,
        entity_label='PBB',
    )

    #
    # FOR PIBB
    process_loan_data(
        lnnote_reptdate_path=PIBB_LNNOTE_REPTDATE_PATH,
        lnnote_path=PIBB_LNNOTE_PATH,
        loan_base_path=PIBB_LOAN_BASE_PATH,
        lncomm_path=PIBB_LNCOMM_PATH,
        output_path=PIBB_LNLCI_PATH,
        entity_label='PIBB',
    )


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
