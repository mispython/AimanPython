#!/usr/bin/env python3
"""
Program : LNMHPBBD.py
Purpose : To prepare MIS data files — Loans / Overdrafts
Date    : 23/05/97

Sections:
  1. LNPOSN  — Loan/OD position as at the month
  2. LNSECT  — Loan by sector code (for Credit Division)
  3. HLCOST  — Housing loans for houses below RM 100K
  4. LMLOAN  — Last-month-end loan balance dataset (intermediate BNM lib file)
  5. LNMVNT  — Fixed loans major movement >= 1 million

Input libraries (parquet):
  BNM.LOAN&REPTMON&NOWK        -> input/BNM/LOAN<REPTMON><NOWK>.parquet
  BNM.ULOAN&REPTMON&NOWK       -> input/BNM/ULOAN<REPTMON><NOWK>.parquet
  BNM.LMLOAN&REPTMON           -> input/BNM/LMLOAN<REPTMON>.parquet   (produced then deleted)
  LMLOAN.LNNOTE                -> input/LMLOAN/LNNOTE.parquet

Output library (parquet):
  MIS.LNPOSN<REPTMON>          -> output/MIS/LNPOSN<REPTMON>.parquet
  MIS.LNSECT<REPTMON>          -> output/MIS/LNSECT<REPTMON>.parquet
  MIS.HLCOST<REPTMON>          -> output/MIS/HLCOST<REPTMON>.parquet
  MIS.LNMVNT<REPTMON>          -> output/MIS/LNMVNT<REPTMON>.parquet
  BNM.LMLOAN<REPTMON>          -> output/BNM/LMLOAN<REPTMON>.parquet  (intermediate)

Note: PROC APPEND with preceding DELETE is reproduced as overwrite.
      BNM.LMLOAN is written to output/BNM, used within the run, then deleted at end.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Path / macro setup
# ---------------------------------------------------------------------------
BASE_DIR    = Path(__file__).resolve().parent
INPUT_DIR   = BASE_DIR / "input"
OUTPUT_DIR  = BASE_DIR / "output"
MIS_DIR     = OUTPUT_DIR / "MIS"
BNM_OUT_DIR = OUTPUT_DIR / "BNM"

for d in [INPUT_DIR, MIS_DIR, BNM_OUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Runtime macro variables — set by orchestrator or overridden here
REPTMON: str = "09"       # e.g. "09"
NOWK:    str = "4"        # e.g. "4"
RDATE:   str = "30/09/05" # DDMMYY8. format — used as upper bound for HLCOST date filter

# Derived input paths
BNM_DIR    = INPUT_DIR / "BNM"
LMLOAN_DIR = INPUT_DIR / "LMLOAN"

# Housing loan products for HLCOST section
# %LET PROD=(209,210,211,212)
HLCOST_PRODUCTS = {209, 210, 211, 212}

# Lower bound for HLCOST date filter: '01APR96'D
APPRDATE_LOW = date(1996, 4, 1)

# ---------------------------------------------------------------------------
# SAS date helpers
# ---------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_python(sas_days: int) -> date:
    return SAS_EPOCH + timedelta(days=int(sas_days))


def parse_ddmmyy8(s: str) -> date:
    """
    Parse DDMMYY8. string (e.g. '30/09/05' or '300905') to Python date.
    YEARCUTOFF=1950: 50-99 -> 1950-1999, 00-49 -> 2000-2049.
    """
    s = s.replace("/", "").replace("-", "").strip()
    dd   = int(s[0:2])
    mm   = int(s[2:4])
    yy   = int(s[4:6]) if len(s) == 6 else int(s[4:8])
    if isinstance(yy, int) and yy < 100:
        yyyy = (1900 + yy) if yy >= 50 else (2000 + yy)
    else:
        yyyy = yy
    return date(yyyy, mm, dd)


def _read(path: Path) -> pl.DataFrame:
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    return df


def _write(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    print(f"  -> {path}  ({df.height} rows)")


def _mis_path(name: str) -> Path:
    return MIS_DIR / f"{name}{REPTMON}.parquet"

def _bnm_out_path(name: str) -> Path:
    return BNM_OUT_DIR / f"{name}{REPTMON}.parquet"

def _loan_path() -> Path:
    return BNM_DIR / f"LOAN{REPTMON}{NOWK}.parquet"

def _uloan_path() -> Path:
    return BNM_DIR / f"ULOAN{REPTMON}{NOWK}.parquet"

def _lmloan_path() -> Path:
    return _bnm_out_path("LMLOAN")


# ---------------------------------------------------------------------------
# 1. LNPOSN — Loan/OD position as at the month
# ---------------------------------------------------------------------------
def run_lnposn(loanc: pl.DataFrame) -> None:
    """
    DATA LOANC; SET BNM.LOAN&REPTMON&NOWK;  (pass-through, used throughout)

    PROC SUMMARY DATA=BNM.LOAN&REPTMON&NOWK NWAY;
    CLASS BRANCH ACCTYPE PRODCD PRODUCT CUSTCD;
    VAR ACCTNO APPRLIMT BALANCE;
    OUTPUT OUT=LNPOSN (DROP=_TYPE_ RENAME=(_FREQ_=NOLOAN))
           N(ACCTNO)=NOACCT  SUM(APPRLIMT)=APPRLIMT  SUM(BALANCE)=BALANCE;

    PROC APPEND DATA=LNPOSN BASE=MIS.LNPOSN&REPTMON;
    """
    print("[LNPOSN] Loan/OD position")
    class_cols = ["BRANCH", "ACCTYPE", "PRODCD", "PRODUCT", "CUSTCD"]
    out = loanc.group_by(class_cols).agg([
        pl.len().alias("NOLOAN"),
        pl.col("ACCTNO").count().alias("NOACCT"),
        pl.col("APPRLIMT").sum(),
        pl.col("BALANCE").sum(),
    ])
    _write(out, _mis_path("LNPOSN"))


# ---------------------------------------------------------------------------
# 2. LNSECT — Loan by sector code
# ---------------------------------------------------------------------------
def run_lnsect(loanc: pl.DataFrame) -> None:
    """
    PROC SUMMARY DATA=LOANC NWAY;
    CLASS BRANCH ACCTYPE PRODCD PRODUCT STATECD SECTORCD;
    VAR ACCTNO APPRLIMT BALANCE;
    OUTPUT OUT=LNSECT (DROP=_TYPE_ RENAME=(_FREQ_=NOLOAN))
           N(ACCTNO)=NOACCT  SUM(APPRLIMT)=APPRLIMT  SUM(BALANCE)=BALANCE;

    PROC APPEND DATA=LNSECT BASE=MIS.LNSECT&REPTMON;
    """
    print("[LNSECT] Loan by sector code")
    class_cols = ["BRANCH", "ACCTYPE", "PRODCD", "PRODUCT", "STATECD", "SECTORCD"]
    out = loanc.group_by(class_cols).agg([
        pl.len().alias("NOLOAN"),
        pl.col("ACCTNO").count().alias("NOACCT"),
        pl.col("APPRLIMT").sum(),
        pl.col("BALANCE").sum(),
    ])
    _write(out, _mis_path("LNSECT"))


# ---------------------------------------------------------------------------
# 3. HLCOST — Housing loans for houses below RM100K
# ---------------------------------------------------------------------------
def run_hlcost(loanc: pl.DataFrame) -> None:
    """
    %LET PROD=(209,210,211,212);
    Date filter: '01APR96'D LE APPRDATE LE INPUT("&RDATE", DDMMYY8.)

    Two summaries:
      a. LOANC (BNM.LOAN) filtered by PROD and date
      b. BNM.ULOAN filtered by PROD and date
    Both appended to MIS.HLCOST&REPTMON.

    PROC SUMMARY NWAY; CLASS BRANCH; VAR APPRLIMT;
    OUTPUT OUT=HLCOST (DROP=_TYPE_ RENAME=(_FREQ_=NOLOAN)) SUM=APPRLIMT;
    """
    print("[HLCOST] Housing loans for houses below RM100K")

    rdate_py  = parse_ddmmyy8(RDATE)
    low_sas   = (APPRDATE_LOW - SAS_EPOCH).days
    high_sas  = (rdate_py - SAS_EPOCH).days

    def _hlcost_summary(df: pl.DataFrame) -> pl.DataFrame:
        return (
            df
            .filter(
                pl.col("PRODUCT").is_in(list(HLCOST_PRODUCTS)) &
                pl.col("APPRDATE").is_between(low_sas, high_sas)
            )
            .group_by("BRANCH")
            .agg([
                pl.len().alias("NOLOAN"),
                pl.col("APPRLIMT").sum(),
            ])
        )

    loan_hl  = _hlcost_summary(loanc)
    uloan    = _read(_uloan_path())
    uloan_hl = _hlcost_summary(uloan)

    out = pl.concat([loan_hl, uloan_hl], how="diagonal")
    _write(out, _mis_path("HLCOST"))


# ---------------------------------------------------------------------------
# 4. LMLOAN — Build last-month-end loan balance from LMLOAN.LNNOTE
# ---------------------------------------------------------------------------
def run_lmloan() -> pl.DataFrame:
    """
    DATA NOTEC; SET LMLOAN.LNNOTE;

    PROC SORT DATA=NOTEC (KEEP=ACCTNO NAME BALANCE NOTENO PAIDIND
                               NTBRCH ACCBRCH
                               WHERE=(NOTENO NE . AND PAIDIND NE 'P'))
              OUT=LMNOTE (DROP=NOTENO PAIDIND);
    BY ACCTNO;

    DATA LMNOTE (KEEP=ACCTNO NAME BRANCH BALANCE NOLOAN ACCTYPE);
      LENGTH NAME $24;
      SET LMNOTE (RENAME=(NTBRCH=BRANCH BALANCE=CURBAL));
      IF BRANCH=0 THEN BRANCH=ACCBRCH;
      BY ACCTNO;
      IF FIRST.ACCTNO THEN DO; BALANCE=0; NOLOAN=0; END;
      ACCTYPE='LN';
      BALANCE+CURBAL;   <- cumulative sum within ACCTNO group
      NOLOAN+1;          <- cumulative count within ACCTNO group
      IF LAST.ACCTNO;    <- keep only the last (fully accumulated) row per ACCTNO

    PROC APPEND DATA=LMNOTE BASE=BNM.LMLOAN&REPTMON;

    Returns the LMNOTE DataFrame for use in LNMVNT.
    """
    print("[LMLOAN] Building last-month-end loan balance from LNNOTE")
    notec = _read(LMLOAN_DIR / "LNNOTE.parquet")

    # Filter: NOTENO NE . AND PAIDIND NE 'P'
    notec = notec.filter(
        pl.col("NOTENO").is_not_null() &
        (pl.col("PAIDIND").cast(pl.Utf8).str.strip_chars() != "P")
    ).select(["ACCTNO", "NAME", "BALANCE", "NTBRCH", "ACCBRCH"])

    # RENAME: NTBRCH=BRANCH, BALANCE=CURBAL
    notec = notec.rename({"NTBRCH": "BRANCH", "BALANCE": "CURBAL"})

    # IF BRANCH=0 THEN BRANCH=ACCBRCH
    notec = notec.with_columns(
        pl.when(pl.col("BRANCH") == 0)
        .then(pl.col("ACCBRCH"))
        .otherwise(pl.col("BRANCH"))
        .alias("BRANCH")
    ).sort("ACCTNO")

    # BY ACCTNO: cumulative BALANCE and NOLOAN count; keep LAST.ACCTNO
    lmnote = (
        notec
        .group_by("ACCTNO")
        .agg([
            pl.col("NAME").last().str.slice(0, 24),       # LENGTH NAME $24; last name
            pl.col("BRANCH").last(),                       # LAST.ACCTNO branch
            pl.col("CURBAL").sum().alias("BALANCE"),       # BALANCE + CURBAL (retain sum)
            pl.len().alias("NOLOAN"),                      # NOLOAN + 1 (count)
            pl.lit("LN").alias("ACCTYPE"),
        ])
    ).select(["ACCTNO", "NAME", "BRANCH", "BALANCE", "NOLOAN", "ACCTYPE"])

    _write(lmnote, _lmloan_path())
    return lmnote


# ---------------------------------------------------------------------------
# 5. LNMVNT — Fixed loans major movement >= 1 million
# ---------------------------------------------------------------------------
def run_lnmvnt(loanc: pl.DataFrame, lmloan: pl.DataFrame) -> None:
    """
    DATA LOANP; SET BNM.LMLOAN&REPTMON;

    PROC SORT DATA=LOANP (WHERE=(ACCTYPE='LN'))
              OUT=LASTMTH (RENAME=(NAME=OLDNAME BALANCE=OLDBAL) DROP=ACCTYPE);
    BY BRANCH ACCTNO;

    PROC SORT DATA=LOANC (WHERE=(ACCTYPE='LN'))
              OUT=THISMTH (RENAME=(NAME=NEWNAME BALANCE=NEWBAL) DROP=ACCTYPE);
    BY BRANCH ACCTNO;

    DATA LNMOVEMT (KEEP=BRANCH ACCTNO NAME OLDBAL NEWBAL);
      MERGE LASTMTH(IN=A) THISMTH(IN=B);
      BY BRANCH ACCTNO;
      IF A AND NOT B THEN NAME=OLDNAME;
      ELSE NAME=NEWNAME;
      IF ABS(NEWBAL - OLDBAL) GE 1,000,000;

    PROC APPEND DATA=LNMOVEMT BASE=MIS.LNMVNT&REPTMON;
    """
    print("[LNMVNT] Fixed loans major movement >= 1 million")

    lastmth = (
        lmloan
        .filter(pl.col("ACCTYPE").cast(pl.Utf8).str.strip_chars() == "LN")
        .select(["BRANCH", "ACCTNO", "NAME", "BALANCE"])
        .rename({"NAME": "OLDNAME", "BALANCE": "OLDBAL"})
        .sort(["BRANCH", "ACCTNO"])
    )

    thismth = (
        loanc
        .filter(pl.col("ACCTYPE").cast(pl.Utf8).str.strip_chars() == "LN")
        .select(["BRANCH", "ACCTNO", "NAME", "BALANCE"])
        .rename({"NAME": "NEWNAME", "BALANCE": "NEWBAL"})
        .sort(["BRANCH", "ACCTNO"])
    )

    # MERGE ... BY BRANCH ACCTNO (full outer)
    merged = lastmth.join(
        thismth, on=["BRANCH", "ACCTNO"], how="full", coalesce=True, suffix="_new"
    )

    # IF A AND NOT B THEN NAME=OLDNAME; ELSE NAME=NEWNAME;
    merged = merged.with_columns([
        pl.col("NEWBAL").fill_null(0.0),
        pl.col("OLDBAL").fill_null(0.0),
        pl.when(pl.col("NEWNAME").is_null())
        .then(pl.col("OLDNAME"))
        .otherwise(pl.col("NEWNAME"))
        .alias("NAME"),
    ])

    # IF ABS(SUM(NEWBAL, (-1)*OLDBAL)) GE 1,000,000
    merged = merged.filter(
        (pl.col("NEWBAL") - pl.col("OLDBAL")).abs() >= 1_000_000
    ).select(["BRANCH", "ACCTNO", "NAME", "OLDBAL", "NEWBAL"])

    _write(merged, _mis_path("LNMVNT"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print(f"[LNMHPBBD] REPTMON={REPTMON}  NOWK={NOWK}  RDATE={RDATE}")

    # Load BNM.LOAN once; used in multiple sections as LOANC
    loanc = _read(_loan_path())
    print(f"  LOANC rows: {loanc.height}")

    # 1. Loan/OD position
    run_lnposn(loanc)

    # 2. Loan by sector code
    run_lnsect(loanc)

    # 3. Housing loans below RM100K
    run_hlcost(loanc)

    # 4. Build BNM.LMLOAN from LMLOAN.LNNOTE
    lmloan = run_lmloan()

    # 5. Loan movement >= 1 million
    run_lnmvnt(loanc, lmloan)

    # PROC DATASETS LIB=BNM NOLIST; DELETE LMLOAN&REPTMON;
    lmloan_file = _lmloan_path()
    if lmloan_file.exists():
        lmloan_file.unlink()
        print(f"  Deleted intermediate: {lmloan_file}")

    print("[LNMHPBBD] Done.")


if __name__ == "__main__":
    main()
