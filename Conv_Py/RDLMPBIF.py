#!/usr/bin/env python3
"""
Program : RDLMPBIF.py
Purpose : Build the PBIF liquidity dataset for the MONTH-END reporting
          cycle. Converted from Ori_SAS/RDLMPBIF, %INC'd by EIBMAPBL when
          REPTQ = 'Y' (tomorrow IS the 1st of a month).

Dependency note:
    The original SAS source has "%INC PGM(PBBLNFMT);" at the top, but no
    PUT(var, fmt.) style call against any PBBLNFMT format exists anywhere
    in this program's body. Per conversion policy the include is kept only
    as a comment/placeholder below and is NOT wired up as a live import,
    because none of PBBLNFMT's format functions are actually exercised here.
"""

# from PBBLNFMT import ...   # placeholder only -- no PBBLNFMT format is
                              # actually referenced anywhere in RDLMPBIF.

from datetime import date, datetime
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# ============================================================================
# CUSTFISS / CUSTCX RECODE  (character version: quotes used in this program)
# IF CUSTFISS IN ('41','42','43','66')  THEN CUSTFISS='41';  ELSE
# IF CUSTFISS IN ('44','47','67')       THEN CUSTFISS='44';  ELSE
# IF CUSTFISS IN ('46')                 THEN CUSTFISS='46';  ELSE
# IF CUSTFISS IN ('48','49','51','68')  THEN CUSTFISS='48';  ELSE
# IF CUSTFISS IN ('52','53','54','69')  THEN CUSTFISS='52';
# ============================================================================
_CUSTFISS_MAP = {
    "41": "41", "42": "41", "43": "41", "66": "41",
    "44": "44", "47": "44", "67": "44",
    "46": "46",
    "48": "48", "49": "48", "51": "48", "68": "48",
    "52": "52", "53": "52", "54": "52", "69": "52",
}


def _map_custfiss(custcd: Optional[str]) -> str:
    code = "" if custcd is None else str(custcd).strip()
    return _CUSTFISS_MAP.get(code, code)


# ============================================================================
# %NXTBLDT MACRO — identical logic to RDALPBIF (each SAS program carries its
# own copy of the macro text, so it is re-implemented locally here as well).
# ============================================================================
def _days_in_month(month: int, year: int) -> int:
    days = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
            7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    if month == 2 and year % 4 == 0:
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


# ============================================================================
# MECHRG — fixed-width raw text file (NOT a .sas7bdat)
# INPUT @001 CLIENTNO $9. @010 PDATE YYMMDD8. @020 UVAL1 12.2
#       @034 UVAL2 12.2  @048 UVAL3 12.2
# ============================================================================
def _parse_implied_decimal(raw: str, decimals: int) -> float:
    """SAS w.d informat: if the text has a decimal point, use it as-is;
    otherwise treat the trailing `decimals` digits as implied decimals."""
    raw = raw.strip()
    if not raw:
        return 0.0
    if "." in raw:
        return float(raw)
    sign = -1.0 if raw.startswith("-") else 1.0
    digits = raw.lstrip("+-").lstrip("0") or "0"
    value = int(digits) / (10 ** decimals)
    return sign * value


def _read_mechrg(mechrg_path: Path, reptdate: date) -> pl.DataFrame:
    """
    DATA MECHRG; INFILE MECHRG; INPUT ...; INTVAL=SUM(UVAL1,UVAL2,UVAL3);
    IF PDATE=&MDATE;
    PROC SUMMARY DATA=MECHRG NWAY; CLASS CLIENTNO; VAR INTVAL;
    OUTPUT OUT=MECHRG(DROP=_FREQ_ _TYPE_) SUM=;
    """
    rows = []
    with open(mechrg_path, "rb") as fh:
        for raw in fh:
            line = raw.rstrip(b"\r\n").decode("latin1")
            if len(line) < 59:
                continue
            clientno = line[0:9].strip()
            pdate_str = line[9:17].strip()
            try:
                pdate = datetime.strptime(pdate_str, "%Y%m%d").date()
            except ValueError:
                continue
            uval1 = _parse_implied_decimal(line[19:31], 2)
            uval2 = _parse_implied_decimal(line[33:45], 2)
            uval3 = _parse_implied_decimal(line[47:59], 2)
            intval = uval1 + uval2 + uval3

            if pdate != reptdate:      # IF PDATE=&MDATE;
                continue

            rows.append({"CLIENTNO": clientno, "INTVAL": intval})

    if not rows:
        return pl.DataFrame({"CLIENTNO": [], "INTVAL": []},
                             schema={"CLIENTNO": pl.Utf8, "INTVAL": pl.Float64})

    mechrg = pl.DataFrame(rows)
    # PROC SUMMARY NWAY CLASS CLIENTNO VAR INTVAL SUM=
    mechrg = mechrg.group_by("CLIENTNO").agg(pl.col("INTVAL").sum())
    return mechrg


def build_pbif(client_cache_path: str, mechrg_path: Path, reptdate: date) -> pl.DataFrame:
    """
    Convert RDLMPBIF into a callable producing the final PBIF dataset.

    Args:
        client_cache_path: path to the cached Parquet version of
                            PBIF.CLIEN&REPTYEAR&REPTMON&REPTDAY (.sas7bdat).
        mechrg_path: path to the fixed-width MECHRG raw text file.
        reptdate: date object equivalent to REPTDATE (from REPTDATE.py).

    Returns:
        Final PBIF Polars DataFrame, equivalent to
        "PROC SORT DATA=PBIF OUT=PBIF NODUPKEY; BY CLIENTNO MATDTE;"
    """
    # ------------------------------------------------------------------
    # DATA PBIF; FORMAT CUSTFISS $2.; SET PBIF.CLIEN...; IF ENTITY='PBBH';
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
            CAST(STDATES  AS DATE)    AS STDATES
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

    # ------------------------------------------------------------------
    # DATA MECHRG; ... ; PROC SUMMARY -> MECHRG(CLIENTNO, INTVAL sum)
    # ------------------------------------------------------------------
    mechrg = _read_mechrg(mechrg_path, reptdate)
    print(f"[RDLMPBIF] MECHRG rows matched to REPTDATE ({reptdate}): {len(mechrg):,}")

    # ------------------------------------------------------------------
    # DATA PBIF; MERGE PBIF(IN=A) MECHRG; BY CLIENTNO; IF A;
    # IF FIU=0.00 AND PRMTHFIU=0.00 THEN DELETE;
    # IF INTVAL=. THEN INTVAL=0.00;
    # FIU=SUM(FIU,INTVAL,PRMTHFIU); BALANCE=FIU;
    # UFIU=0; DISBURSE=0; REPAID=0; ROLLOVER=0;
    # IF BALANCE<0 THEN BALANCE=0;
    # IF FIU<0 THEN UFIU=FIU;
    # IF PRMTHFIU<0 THEN PRMTHFIU=0;
    # IF BALANCE>=0 THEN DO;
    #    IF BALANCE>PRMTHFIU THEN DISBURSE=BALANCE-PRMTHFIU;
    #                        ELSE REPAID=PRMTHFIU-BALANCE;
    # END;
    # UNDRAWN=(INLIMIT-BALANCE);
    # IF FIU=0.00 THEN DELETE;
    # ------------------------------------------------------------------
    merged = base.join(mechrg, on="CLIENTNO", how="left")

    # IF FIU=0.00 AND PRMTHFIU=0.00 THEN DELETE;  (pre-merge FIU/PRMTHFIU)
    merged = merged.filter(~((pl.col("FIU") == 0.0) & (pl.col("PRMTHFIU") == 0.0)))

    merged = merged.with_columns(
        pl.col("INTVAL").fill_null(0.0)
    ).with_columns(
        (pl.col("FIU").fill_null(0.0) + pl.col("INTVAL") + pl.col("PRMTHFIU").fill_null(0.0)).alias("FIU")
    ).with_columns([
        pl.col("FIU").alias("BALANCE"),
        pl.lit(0.0).alias("UFIU"),
        pl.lit(0.0).alias("DISBURSE"),
        pl.lit(0.0).alias("REPAID"),
        pl.lit(0.0).alias("ROLLOVER"),
    ])

    merged = merged.with_columns(
        pl.when(pl.col("BALANCE") < 0.0).then(0.0).otherwise(pl.col("BALANCE")).alias("BALANCE")
    ).with_columns(
        pl.when(pl.col("FIU") < 0.0).then(pl.col("FIU")).otherwise(pl.col("UFIU")).alias("UFIU")
    ).with_columns(
        pl.when(pl.col("PRMTHFIU") < 0.0).then(0.0).otherwise(pl.col("PRMTHFIU")).alias("PRMTHFIU")
    )

    merged = merged.with_columns([
        pl.when((pl.col("BALANCE") >= 0.0) & (pl.col("BALANCE") > pl.col("PRMTHFIU")))
          .then(pl.col("BALANCE") - pl.col("PRMTHFIU")).otherwise(pl.col("DISBURSE")).alias("DISBURSE"),
        pl.when((pl.col("BALANCE") >= 0.0) & (pl.col("BALANCE") <= pl.col("PRMTHFIU")))
          .then(pl.col("PRMTHFIU") - pl.col("BALANCE")).otherwise(pl.col("REPAID")).alias("REPAID"),
    ]).with_columns(
        (pl.col("INLIMIT") - pl.col("BALANCE")).alias("UNDRAWN")
    )

    # IF FIU=0.00 THEN DELETE;
    merged = merged.filter(pl.col("FIU") != 0.0)

    # PROC PRINT; VAR BRANCH CLIENTNO BALANCE CUSTCX FISSPURP INLIMIT UNDRAWN
    #             SECTORCD DISBURSE REPAID FIU ACCTNO PRMTHFIU UFIU INTVAL;
    # SUM BALANCE REPAID DISBURSE UNDRAWN FIU PRMTHFIU UFIU INTVAL;
    print("\n[RDLMPBIF] PBIF listing (equivalent PROC PRINT):")
    print_cols = ["BRANCH", "CLIENTNO", "BALANCE", "CUSTCX", "FISSPURP", "INLIMIT",
                  "UNDRAWN", "SECTORCD", "DISBURSE", "REPAID", "FIU", "ACCTNO",
                  "PRMTHFIU", "UFIU", "INTVAL"]
    print(merged.select(print_cols))
    for col in ["BALANCE", "REPAID", "DISBURSE", "UNDRAWN", "FIU", "PRMTHFIU", "UFIU", "INTVAL"]:
        print(f"  SUM {col} = {merged[col].sum():,.2f}")

    # ------------------------------------------------------------------
    # DATA PBIF; DROP CUSTCD; %DCLVAR FORMAT CUSTCX $2.; SET PBIF;
    # FREQ=6; IF INLIMIT<1000000 THEN FREQ=12;
    # MATDTE=REPTDATE; IF STDATES>0 THEN DO MATDTE=STDATES;
    #    DO WHILE (MATDTE<=REPTDATE); %NXTBLDT END; END;
    # ------------------------------------------------------------------
    rows = merged.drop("CUSTCD").to_dicts()
    out_rows = []
    for row in rows:
        freq = 12 if row["INLIMIT"] < 1_000_000.00 else 6
        matdte = reptdate
        stdates = row.get("STDATES")
        if stdates is not None and stdates > date(1960, 1, 1):
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
