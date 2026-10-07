#!/usr/bin/env python3
"""
Program : KAMLIQX.py
Purpose : FX purchase/sale BNM-code derivation fragment for the New
          Liquidity Framework (originally %INC PGM(KAMLIQX) inside
          KALMLIQ). Reads BNMK.K1TBL&REPTMON&NOWK a second time
          (independently of KALMLIQ's own K1TBL step) and derives the
          57100/57400/57600 BNM codes -> final K1TBX (PART ITEM AMOUNT
          AMTUSD AMTSGD MATDT AMTHKD).

          Designed to be imported by KALMLIQ.py, mirroring %INC
          semantics. Owns no physical path of its own -- the pre-cached
          BNMK.K1TBL&REPTMON&NOWK parquet path is supplied by the caller
          (ultimately declared in EIBMLIQP.py).
"""
from pathlib import Path

import duckdb
import polars as pl

FX_DLP = {"FXS", "FXO", "FXF", "SF1", "SF2", "TS1", "TS2", "FBP", "FF1", "FF2"}

_KEEP_COLS = {
    "PART": pl.Utf8, "ITEM": pl.Utf8, "AMOUNT": pl.Float64,
    "AMTUSD": pl.Float64, "AMTSGD": pl.Float64, "MATDT": pl.Utf8, "AMTHKD": pl.Float64,
}


def _select_57100(gwdlp, gwccy, gwctp, gwcnal, gwsac):
    """WHEN('FXS') / WHEN('FBP') / WHEN('FXO','FXF') / WHEN('SF1'...'FF2')
    inside the GWOCY='MYR' AND GWMVT='P' AND GWMVTS='P' branch."""
    explicit = {"BC", "BB", "BI", "BM", "BA", "BE"}

    def otherwise():
        code = None
        if not ("BA" <= gwctp <= "BZ") and gwcnal == "MY" and gwsac != "UF":
            code = "57100"
        if gwsac == "UF":
            code = "57100"
        return code

    if gwccy == "MYR":
        return None
    if gwdlp == "FBP":
        return "57100"
    if gwdlp == "FXS" or gwdlp in ("FXO", "FXF") or gwdlp in ("SF1", "SF2", "TS1", "TS2", "FF1", "FF2"):
        return "57100" if gwctp in explicit else otherwise()
    return None


def _select_57400(gwdlp, gwccy, gwctp, gwcnal, gwsac):
    """Same shape as _select_57100 but for GWMVTS='S' -> the 57400 family.
    Note: no WHEN('FBP') in this SELECT (matches SAS source)."""
    explicit = {"BC", "BB", "BI", "BM", "CE", "BA", "BE"}

    def otherwise(check_ce=False):
        code = None
        if not ("BA" <= gwctp <= "BZ") and gwcnal == "MY" and gwsac != "UF":
            code = "57400"
        if check_ce and gwctp == "CE":
            code = "57400"
        if gwsac == "UF":
            code = "57400"
        return code

    if gwccy == "MYR":
        return None
    if gwdlp == "FXS":
        return "57400" if gwctp in explicit else otherwise()
    if gwdlp in ("FXO", "FXF"):
        return "57400" if gwctp in explicit else otherwise(check_ce=True)
    if gwdlp in ("SF1", "SF2", "TS1", "TS2", "FF1", "FF2"):
        return "57400" if gwctp in explicit else otherwise()
    return None


def _select_57600(gwccy, gwocy, gwmvt, gwmvts, gwctp, gwdlp):
    """K1TBX2 block: cross-currency (neither leg MYR), GWMVT='P' AND
    GWMVTS='P' AND GWCTP<>'BW'."""
    if not (gwccy != "MYR" and gwocy != "MYR" and gwmvt == "P" and gwmvts == "P" and gwctp != "BW"):
        return None
    if gwdlp == "FXS":
        return "57600"
    if gwdlp in ("FXO", "FXF"):
        return "57600"
    if gwdlp in ("SF2", "FF1", "FF2"):
        return "57600"
    if gwdlp in ("SF1", "TS1", "TS2"):
        return "57600"
    return None


def _base_ok(gwmvt, gwocy, gwccy, gwdlp) -> bool:
    if gwmvt != "P":
        return False
    if gwocy == "XAU" or gwccy == "XAU":
        return False
    if gwdlp not in FX_DLP:
        return False
    return True


def build_k1tbx(k1tbl_cache: Path) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT
            CAST(GWCCY  AS VARCHAR) AS GWCCY,
            CAST(GWEXR  AS DOUBLE)  AS GWEXR,
            CAST(GWBALA AS DOUBLE)  AS GWBALA,
            CAST(GWBALC AS DOUBLE)  AS GWBALC,
            CAST(GWMVT  AS VARCHAR) AS GWMVT,
            CAST(GWMVTS AS VARCHAR) AS GWMVTS,
            CAST(GWOCY  AS VARCHAR) AS GWOCY,
            CAST(GWCTP  AS VARCHAR) AS GWCTP,
            CAST(GWCNAL AS VARCHAR) AS GWCNAL,
            CAST(GWSAC  AS VARCHAR) AS GWSAC,
            CAST(GWDLP  AS VARCHAR) AS GWDLP,
            CAST(GWMDT  AS VARCHAR) AS MATDT
        FROM read_parquet('{k1tbl_cache.as_posix()}')
    """).pl()
    con.close()

    rows1, rows2 = [], []
    for r in raw.iter_rows(named=True):
        gwccy, gwmvt, gwmvts = r["GWCCY"], r["GWMVT"], r["GWMVTS"]
        gwocy, gwctp = r["GWOCY"], r["GWCTP"] or ""
        gwcnal, gwsac, gwdlp = r["GWCNAL"] or "", r["GWSAC"] or "", r["GWDLP"] or ""

        # AMOUNT=GWBALA*GWEXR: both branches of the original JPY/ELSE IF are
        # identical -- dead branching, preserved as a single computation.
        amount = (r["GWBALA"] or 0.0) * (r["GWEXR"] or 0.0)

        if not _base_ok(gwmvt, gwocy, gwccy, gwdlp):
            continue

        # ---- K1TBX1: delete XAT, MYR purchase-side 57100/57400 mapping ----
        if gwocy != "XAT" and gwccy != "XAT":
            bnmcode = None
            if gwocy == "MYR" and gwmvt == "P" and gwmvts == "P":
                bnmcode = _select_57100(gwdlp, gwccy, gwctp, gwcnal, gwsac)
            if gwocy == "MYR" and gwmvt == "P" and gwmvts == "S":
                bnmcode = _select_57400(gwdlp, gwccy, gwctp, gwcnal, gwsac)
            if bnmcode is not None:
                rows1.append({"GWCCY": gwccy, "GWOCY": gwocy, "MATDT": r["MATDT"],
                               "AMOUNT": amount, "BNMCODE": bnmcode})

        # ---- K1TBX2: cross-currency 57600 bucket (from base K1TBX) --------
        bnmcode2 = _select_57600(gwccy, gwocy, gwmvt, gwmvts, gwctp, gwdlp)
        if bnmcode2 is not None:
            rows2.append({"GWCCY": gwccy, "GWOCY": gwocy, "MATDT": r["MATDT"],
                           "AMOUNT": r["GWBALC"], "BNMCODE": bnmcode2})

    final_rows = []
    for r in rows1 + rows2:
        raw_amt = r["AMOUNT"] or 0.0
        amount = abs(raw_amt) if raw_amt < 0 else raw_amt
        amthkd = 0.0
        amtusd = amount if r["GWCCY"] == "USD" else 0.0
        amtsgd = amount if r["GWCCY"] == "SGD" else 0.0
        bnmcode, matdt = r["BNMCODE"], r["MATDT"]

        if bnmcode == "57100":
            final_rows.append({"PART": "96", "ITEM": "711", "AMOUNT": amount,
                                "AMTUSD": amtusd, "AMTSGD": amtsgd, "MATDT": matdt, "AMTHKD": amthkd})
            final_rows.append({"PART": "95", "ITEM": "911", "AMOUNT": amount,
                                "AMTUSD": 0.0, "AMTSGD": 0.0, "MATDT": matdt, "AMTHKD": amthkd})
        elif bnmcode == "57400":
            final_rows.append({"PART": "96", "ITEM": "911", "AMOUNT": amount,
                                "AMTUSD": amtusd, "AMTSGD": amtsgd, "MATDT": matdt, "AMTHKD": amthkd})
            final_rows.append({"PART": "95", "ITEM": "711", "AMOUNT": amount,
                                "AMTUSD": 0.0, "AMTSGD": 0.0, "MATDT": matdt, "AMTHKD": amthkd})
        elif bnmcode == "57600":
            final_rows.append({"PART": "96", "ITEM": "711", "AMOUNT": amount,
                                "AMTUSD": amtusd, "AMTSGD": amtsgd, "MATDT": matdt, "AMTHKD": amthkd})
            amtusd2 = amount if r["GWOCY"] == "USD" else 0.0
            amtsgd2 = amount if r["GWOCY"] == "SGD" else 0.0
            final_rows.append({"PART": "96", "ITEM": "911", "AMOUNT": amount,
                                "AMTUSD": amtusd2, "AMTSGD": amtsgd2, "MATDT": matdt, "AMTHKD": amthkd})

    return pl.DataFrame(final_rows, schema=_KEEP_COLS) if final_rows else pl.DataFrame(schema=_KEEP_COLS)
