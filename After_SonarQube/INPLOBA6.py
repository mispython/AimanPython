#!/usr/bin/env python3
"""
Program: INPLOBA6.py
Purpose: Consolidate NPLX IIS/SP1/SP2/AQ parquet inputs into the NPL.NPLOBAL
            text output for the PIBB flow.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl


# =============================================================================
# PATH SETUP
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input" / "pibb"
OUTPUT_DIR = BASE_DIR / "output" / "pibb"

NPLX_IIS_FILE = INPUT_DIR / "nplx_iis.parquet"
NPLX_SP1_FILE = INPUT_DIR / "nplx_sp1.parquet"
NPLX_SP2_FILE = INPUT_DIR / "nplx_sp2.parquet"
NPLX_AQ_FILE = INPUT_DIR / "nplx_aq.parquet"

NPL_NPLOBAL_FILE = OUTPUT_DIR / "npl_nplobal.txt"
NPL_WAQ_FILE = OUTPUT_DIR / "npl_waq.txt"
NPL_WIIS_FILE = OUTPUT_DIR / "npl_wiis.txt"
NPL_WSP2_FILE = OUTPUT_DIR / "npl_wsp2.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# COLUMN DEFINITIONS
# =============================================================================
IIS_COLUMNS = [
    "BRANCH",
    "NTBRCH",
    "ACCTNO",
    "NOTENO",
    "NAME",
    "BORSTAT",
    "DAYS",
    "IIS",
    "OI",
    "UHC",
    "SP",
]
SP_COLUMNS = ["ACCTNO", "NOTENO", "SP"]
AQ_COLUMNS = ["ACCTNO", "NOTENO", "NPL", "CURBAL", "LOANTYPE"]
OUTPUT_COLUMNS = [
    "BRANCH",
    "NTBRCH",
    "ACCTNO",
    "NOTENO",
    "NAME",
    "BORSTAT",
    "DAYS",
    "IISP",
    "OIP",
    "LOANSTAT",
    "UHCP",
    "SPP2",
    "SPP1",
    "NETBALP",
    "CURBALP",
    "LOANTYPE",
    "EXIST",
]


# =============================================================================
# SAS TRACEABILITY PLACEHOLDERS
# =============================================================================
# *------------------------------------------------*
# *  EIFMNP03  (TO KEEP EXTRA UHC)                 *
# *------------------------------------------------*;
# PROC SORT DATA=NPLX.IIS; BY ACCTNO NOTENO; RUN;
# PROC SORT DATA=NPLX.SP2; BY ACCTNO NOTENO; RUN;
# *;
# DATA NPL.NPLOBAL;
#    KEEP BRANCH NTBRCH ACCTNO NOTENO NAME BORSTAT DAYS IIS
#         OI LOANSTAT UHC SP;
#    LENGTH LOANSTAT 2;
#    RETAIN LOANSTAT 3;
#    MERGE NPLX.IIS NPLX.SP2;
#    BY ACCTNO NOTENO;
#    RENAME IIS=IISP OI=OIP UHC=UHCP SP=SPP2;
# RUN;
#
# /*
# DATA HCOBAL;
#    INFILE NPLTXT;
#    INPUT @001 NTBRCH    3.
#          @005 ACCTNO   10.
#          @016 NOTENO    5.
#          @022 IISP     16.2
#          @039 OIP      16.2
#          @056 SPP2     16.2
#          ;
# RUN;
#
# PROC SORT; BY ACCTNO NOTENO; RUN;
# *------------------------------------------------*
# *  EIFMNP04                                      *
# *------------------------------------------------*;
# PROC SORT DATA=NPLX.SP OUT=SP (KEEP=ACCTNO NOTENO SP RENAME=SP=SPP);
#    BY ACCTNO NOTENO;
# *;
# */
#
# *------------------------------------------------*
# *  EIFMNP05                                      *
# *------------------------------------------------*;
# PROC SORT DATA=NPLX.SP1 OUT=SP1 (KEEP=ACCTNO NOTENO SP RENAME=SP=SPP1);
#    BY ACCTNO NOTENO;
# *;
#
# *------------------------------------------------*
# *  EIFMNP06                                      *
# *------------------------------------------------*;
# PROC SORT DATA=NPLX.SP2 OUT=SP2 (KEEP=ACCTNO NOTENO SP RENAME=SP=SPP2);
#    BY ACCTNO NOTENO;
# *;
#
# *------------------------------------------------*
# *  EIFMNP07 TO KEEP EXTRA LOANTYPE               *
# *------------------------------------------------*;
# DATA AQ;
#    KEEP ACCTNO NOTENO NPL CURBAL LOANTYPE;
#    ARRAY VBL NPL CURBALP;
#    SET NPLX.AQ;
#    * CURBALP = NPL + UHC - OI;   /* NPL = CURBAL - UHC + OI    */
#    * CURBALP = CURBAL;           /* <= THIS WAY ALSO SAME      */
#    DO OVER VBL;
#       IF VBL = . THEN VBL = 0;
#    END;
#    RENAME NPL=NETBALP CURBAL=CURBALP;
# PROC SORT;
#    BY ACCTNO NOTENO;
# *;
#
# *------------------------------------------------*
# *  COMBINE ALL INTO NPL.NPLOBAL                  *
# *------------------------------------------------*;
# DATA ALL;
#    MERGE SP1 SP2 AQ; BY ACCTNO NOTENO;
#    IF SPP1=.        THEN SPP1=0.00;
#    IF NETBALP=.     THEN NETBALP=0.00;
#    IF CURBALP=.     THEN CURBALP=0.00;
#    LOANTYPE=700;
# RUN;
# *;
# DATA NPL.NPLOBAL;
#    MERGE ALL NPL.NPLOBAL;
#    BY ACCTNO NOTENO;
#    EXIST = 'Y';
#    IF NTBRCH GT 0 AND LOANTYPE GT 0;
# RUN;
# PROC SORT DATA=NPL.NPLOBAL NODUP; BY ACCTNO; RUN;
# *;
# PROC SQL;
#    DELETE * FROM NPL.WAQ;
#    DELETE * FROM NPL.WIIS;
#    DELETE * FROM NPL.WSP2;
# QUIT;


# =============================================================================
# HELPERS
# =============================================================================
def read_parquet(con: duckdb.DuckDBPyConnection, path: Path, columns: list[str]) -> pl.DataFrame:
    """Load a parquet file with a stable schema; return an empty frame if missing."""
    if not path.exists():
        return pl.DataFrame(schema={column: pl.Null for column in columns})

    frame = con.execute("SELECT * FROM read_parquet(?)", [str(path)]).pl()
    missing_columns = [column for column in columns if column not in frame.columns]
    if missing_columns:
        frame = frame.with_columns(pl.lit(None).alias(column) for column in missing_columns)
    return frame.select(columns)


def write_txt(df: pl.DataFrame, path: Path) -> None:
    """Write a text output with header row."""
    if df.width == 0:
        path.write_text("", encoding="utf-8")
        return
    df.write_csv(path, separator="|", include_header=True, null_value="")


def prepare_base(con: duckdb.DuckDBPyConnection, iis_df: pl.DataFrame, sp2_df: pl.DataFrame) -> pl.DataFrame:
    """Equivalent of DATA NPL.NPLOBAL merge from NPLX.IIS and NPLX.SP2."""
    con.register("iis_df", iis_df)
    con.register("sp2_df", sp2_df)
    return con.execute(
        """
        SELECT
            iis_df.BRANCH,
            iis_df.NTBRCH,
            COALESCE(iis_df.ACCTNO, sp2_df.ACCTNO) AS ACCTNO,
            COALESCE(iis_df.NOTENO, sp2_df.NOTENO) AS NOTENO,
            iis_df.NAME,
            iis_df.BORSTAT,
            iis_df.DAYS,
            iis_df.IIS AS IISP,
            iis_df.OI AS OIP,
            CAST(3 AS BIGINT) AS LOANSTAT,
            iis_df.UHC AS UHCP,
            iis_df.SP AS SPP2
        FROM iis_df
        FULL OUTER JOIN sp2_df
          ON iis_df.ACCTNO = sp2_df.ACCTNO
         AND iis_df.NOTENO = sp2_df.NOTENO
        """
    ).pl()


def prepare_aq(aq_df: pl.DataFrame) -> pl.DataFrame:
    """Equivalent of DATA AQ with null normalization and rename."""
    return (
        aq_df.with_columns(
            pl.col("NPL").fill_null(0.0),
            pl.col("CURBAL").fill_null(0.0),
        )
        .rename({"NPL": "NETBALP", "CURBAL": "CURBALP"})
        .select(["ACCTNO", "NOTENO", "NETBALP", "CURBALP", "LOANTYPE"])
    )


def prepare_all(
        con: duckdb.DuckDBPyConnection,
        sp1_df: pl.DataFrame,
        sp2_df: pl.DataFrame,
        aq_df: pl.DataFrame,
) -> pl.DataFrame:
    """Equivalent of DATA ALL; MERGE SP1 SP2 AQ; BY ACCTNO NOTENO;"""
    con.register("sp1_df", sp1_df)
    con.register("sp2_df", sp2_df)
    con.register("aq_df", aq_df)
    return con.execute(
        """
        WITH sp12 AS (
            SELECT
                COALESCE(sp1_df.ACCTNO, sp2_df.ACCTNO) AS ACCTNO,
                COALESCE(sp1_df.NOTENO, sp2_df.NOTENO) AS NOTENO,
                sp1_df.SP AS SPP1,
                sp2_df.SP AS SPP2
            FROM sp1_df
            FULL OUTER JOIN sp2_df
              ON sp1_df.ACCTNO = sp2_df.ACCTNO
             AND sp1_df.NOTENO = sp2_df.NOTENO
        )
        SELECT
            COALESCE(sp12.ACCTNO, aq_df.ACCTNO) AS ACCTNO,
            COALESCE(sp12.NOTENO, aq_df.NOTENO) AS NOTENO,
            COALESCE(sp12.SPP1, 0.00) AS SPP1,
            sp12.SPP2,
            COALESCE(aq_df.NETBALP, 0.00) AS NETBALP,
            COALESCE(aq_df.CURBALP, 0.00) AS CURBALP,
            CAST(700 AS BIGINT) AS LOANTYPE
        FROM sp12
        FULL OUTER JOIN aq_df
          ON sp12.ACCTNO = aq_df.ACCTNO
         AND sp12.NOTENO = aq_df.NOTENO
        """
    ).pl()


def finalize_output(
    con: duckdb.DuckDBPyConnection,
    base_df: pl.DataFrame,
    all_df: pl.DataFrame,
) -> pl.DataFrame:
    """Equivalent of final DATA NPL.NPLOBAL and PROC SORT NODUP BY ACCTNO."""
    con.register("base_df", base_df)
    con.register("all_df", all_df)
    result = con.execute(
        """
        SELECT
            base_df.BRANCH,
            base_df.NTBRCH,
            COALESCE(all_df.ACCTNO, base_df.ACCTNO) AS ACCTNO,
            COALESCE(all_df.NOTENO, base_df.NOTENO) AS NOTENO,
            base_df.NAME,
            base_df.BORSTAT,
            base_df.DAYS,
            base_df.IISP,
            base_df.OIP,
            base_df.LOANSTAT,
            base_df.UHCP,
            base_df.SPP2,
            all_df.SPP1,
            all_df.NETBALP,
            all_df.CURBALP,
            all_df.LOANTYPE,
            'Y' AS EXIST
        FROM all_df
        FULL OUTER JOIN base_df
          ON all_df.ACCTNO = base_df.ACCTNO
         AND all_df.NOTENO = base_df.NOTENO
        WHERE COALESCE(base_df.NTBRCH, 0) > 0
          AND COALESCE(all_df.LOANTYPE, 0) > 0
        ORDER BY COALESCE(all_df.ACCTNO, base_df.ACCTNO),
                 COALESCE(all_df.NOTENO, base_df.NOTENO)
        """
    ).pl()

    result = result.unique(subset=["ACCTNO"], keep="first", maintain_order=True)
    missing_columns = [column for column in OUTPUT_COLUMNS if column not in result.columns]
    if missing_columns:
        result = result.with_columns(pl.lit(None).alias(column) for column in missing_columns)
    return result.select(OUTPUT_COLUMNS)


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    with duckdb.connect() as con:
        iis_df = read_parquet(con, NPLX_IIS_FILE, IIS_COLUMNS)
        sp1_df = read_parquet(con, NPLX_SP1_FILE, SP_COLUMNS)
        sp2_df = read_parquet(con, NPLX_SP2_FILE, SP_COLUMNS)
        aq_df = read_parquet(con, NPLX_AQ_FILE, AQ_COLUMNS)

        nplobal_base = prepare_base(con, iis_df, sp2_df)
        aq_prepared = prepare_aq(aq_df)
        all_df = prepare_all(con, sp1_df, sp2_df, aq_prepared)
        nplobal_final = finalize_output(con, nplobal_base, all_df)

    write_txt(nplobal_final, NPL_NPLOBAL_FILE)
    write_txt(pl.DataFrame({"_EMPTY": []}).select([]), NPL_WAQ_FILE)
    write_txt(pl.DataFrame({"_EMPTY": []}).select([]), NPL_WIIS_FILE)
    write_txt(pl.DataFrame({"_EMPTY": []}).select([]), NPL_WSP2_FILE)


if __name__ == "__main__":
    main()
