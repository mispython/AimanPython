#!/usr/bin/env python3
"""
Program : RDALPBIF.py
Purpose : Build the PBIF liquidity dataset for the NORMAL (non month-end)
          reporting cycle. Converted from Ori_SAS/RDALPBIF, %INC'd by
          EIBMAPBL when REPTQ = 'N' (tomorrow is NOT the 1st of a month).

Dependency note:
    The original SAS source has "%INC PGM(PBBLNFMT);" at the top, but no
    PUT(var, fmt.) style call against any PBBLNFMT format exists anywhere
    in this program's body. Per conversion policy the include is kept only
    as a comment/placeholder below and is NOT wired up as a live import,
    because none of PBBLNFMT's format functions are actually exercised here.
"""

# from PBBLNFMT import ...   # placeholder only -- no PBBLNFMT format is
                              # actually referenced anywhere in RDALPBIF.

from datetime import date, datetime
from typing import Optional

import duckdb
import polars as pl

# ============================================================================
# CUSTFISS / CUSTCX RECODE
# IF CUSTFISS IN (41,42,43,66) THEN CUSTFISS=41; ELSE
# IF CUSTFISS IN (44,47,67)    THEN CUSTFISS=44; ELSE
# IF CUSTFISS IN (46)          THEN CUSTFISS=46; ELSE
# IF CUSTFISS IN (48,49,51,68) THEN CUSTFISS=48; ELSE
# IF CUSTFISS IN (52,53,54,69) THEN CUSTFISS=52;
# ============================================================================
_CUSTFISS_MAP = {
    "41": "41", "42": "41", "43": "41", "66": "41",
    "44": "44", "47": "44", "67": "44",
    "46": "46",
    "48": "48", "49": "48", "51": "48", "68": "48",
    "52": "52", "53": "52", "54": "52", "69": "52",
}


def _map_custfiss(custcd: Optional[str]) -> str:
    """CUSTFISS=CUSTCD; recode chain; CUSTCX=CUSTFISS;"""
    code = "" if custcd is None else str(custcd).strip()
    return _CUSTFISS_MAP.get(code, code)


# ============================================================================
# %NXTBLDT MACRO — advance MATDTE by FREQ months, capping day-of-month
# LDAY array (D1-D12): 31 default, D4/D6/D9/D11=30, D2 leap-year aware.
# ============================================================================
def _days_in_month(month: int, year: int) -> int:
    days = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
            7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    if month == 2 and year % 4 == 0:   # SAS simple leap rule (YEARCUTOFF=1950)
        return 29
    return days[month]


def _next_bldate(matdte: date, freq: int) -> date:
    dd = matdte.day
    mm = matdte.month + freq
    yy = matdte.year
    if mm > 12:
        mm -= 12
        yy += 1
    last_day = _days_in_month(mm, yy)
    if dd > last_day:
        dd = last_day
    return date(yy, mm, dd)


def build_pbif(client_cache_path: str, reptdate: date) -> pl.DataFrame:
    """
    Convert RDALPBIF into a callable producing the final PBIF dataset.

    Args:
        client_cache_path: path to the cached Parquet version of
                            PBIF.CLIEN&REPTYEAR&REPTMON&REPTDAY (.sas7bdat).
        reptdate: date object equivalent to REPTDATE (from REPTDATE.py).

    Returns:
        Final PBIF Polars DataFrame, equivalent to
        "PROC SORT DATA=PBIF OUT=PBIF NODUPKEY; BY CLIENTNO MATDTE;"
    """
    # ------------------------------------------------------------------
    # DATA PBIF; SET PBIF.CLIEN...; IF ENTITY='PBBH';
    # ------------------------------------------------------------------
    con = duckdb.connect(database=":memory:")
    base = con.execute(f"""
        SELECT
            CAST(ENTITY   AS VARCHAR) AS ENTITY,
            CAST(BRANCH   AS INTEGER) AS BRANCH,
            CAST(CLIENTNO AS VARCHAR) AS CLIENTNO,
            CAST(ACCTNO   AS VARCHAR) AS ACCTNO,
            CAST(INLIMIT  AS DOUBLE)  AS INLIMIT,
            CAST(FIU      AS DOUBLE)  AS FIU,
            CAST(PRMTHFIU AS DOUBLE)  AS PRMTHFIU,
            CAST(CUSTCD   AS VARCHAR) AS CUSTCD,
            CAST(SECTORCD AS VARCHAR) AS SECTORCD,
            CAST(DATE '1960-01-01' + (CAST(STDATES AS INTEGER) * INTERVAL '1' DAY) AS DATE) AS STDATES
        FROM read_parquet('{client_cache_path}')
        WHERE ENTITY = 'PBBH'
    """).pl()
    con.close()

    if base.is_empty():
        return base

    # APPRLIMX=INLIMIT; PRODCD='30591'; FISSPURP='0470'; AMTIND='D';
    base = base.with_columns([
        pl.col("INLIMIT").alias("APPRLIMX"),
        pl.lit("30591").alias("PRODCD"),
        pl.lit("0470").alias("FISSPURP"),
        pl.lit("D").alias("AMTIND"),
    ])

    # CUSTFISS=CUSTCD; recode chain; CUSTCX=CUSTFISS;
    base = base.with_columns(
        pl.col("CUSTCD").map_elements(_map_custfiss, return_dtype=pl.Utf8).alias("CUSTCX")
    )

    # BALANCE=SUM(FIU,PRMTHFIU); UNDRAWN=(INLIMIT-BALANCE);
    base = base.with_columns(
        (pl.col("FIU").fill_null(0.0) + pl.col("PRMTHFIU").fill_null(0.0)).alias("BALANCE")
    ).with_columns(
        (pl.col("INLIMIT") - pl.col("BALANCE")).alias("UNDRAWN")
    )

    # PROC SORT; BY CLIENTNO;  -- redundant: final PROC SORT NODUPKEY below
    # re-sorts BY CLIENTNO MATDTE anyway, so the intermediate sort is skipped.

    # PROC PRINT; VAR BRANCH CLIENTNO BALANCE CUSTCX FISSPURP INLIMIT
    #             UNDRAWN SECTORCD ACCTNO; SUM BALANCE UNDRAWN;
    print("\n[RDALPBIF] PBIF listing (equivalent PROC PRINT):")
    print(base.select(["BRANCH", "CLIENTNO", "BALANCE", "CUSTCX", "FISSPURP",
                        "INLIMIT", "UNDRAWN", "SECTORCD", "ACCTNO"]))
    print(f"  SUM BALANCE = {base['BALANCE'].sum():,.2f}   "
          f"SUM UNDRAWN = {base['UNDRAWN'].sum():,.2f}")

    # ------------------------------------------------------------------
    # DATA PBIF; DROP CUSTCD; %DCLVAR FORMAT CUSTCX $2.; SET PBIF;
    # FREQ=6; IF INLIMIT<1000000 THEN FREQ=12;
    # MATDTE=REPTDATE; IF STDATES>0 THEN DO MATDTE=STDATES;
    #    DO WHILE (MATDTE<=REPTDATE); %NXTBLDT END; END;
    # ------------------------------------------------------------------
    rows = base.drop("CUSTCD").to_dicts()
    out_rows = []
    for row in rows:
        freq = 12 if row["INLIMIT"] < 1_000_000.00 else 6
        matdte = reptdate
        # stdates = row.get("STDATES")
        # if stdates is not None and stdates > date(1960, 1, 1):
        #     matdte = stdates
        #     while matdte <= reptdate:
        #         matdte = _next_bldate(matdte, freq)
        stdates = row.get("STDATES")
        if stdates is not None:
            # Convert to date if it's a datetime
            if isinstance(stdates, datetime):
                stdates = stdates.date()
            if stdates > date(1960, 1, 1):
                matdte = stdates
                while matdte <= reptdate:
                    matdte = _next_bldate(matdte, freq)
        row["FREQ"] = freq
        row["MATDTE"] = matdte
        out_rows.append(row)

    result = pl.DataFrame(out_rows)

    # PROC SORT DATA=PBIF OUT=PBIF NODUPKEY; BY CLIENTNO MATDTE;
    result = result.sort(["CLIENTNO", "MATDTE"]).unique(
        subset=["CLIENTNO", "MATDTE"], keep="first", maintain_order=True
    )
    return result
