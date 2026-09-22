#!/usr/bin/env python3
"""
Program : KALMLIQ.py
Purpose : New Liquidity Framework (Kapiti items) -- pure contractual
          maturity profile breakdown (Part 2) and distribution profile of
          customer deposits (Part 3). Originally %INC PGM(KALMLIQ) inside
          EIBMRLFM.

          Depends on:
            %INC PGM(KAMLIQX)  -> KAMLIQX.build_k1tbx()
            %INC PGM(KALMLIQ4) -> KALMLIQ4.build_k3tbl3() (result unused
                                   downstream -- see KALMLIQ4 docstring)

          Designed to be imported by EIBMRLFM.py, mirroring %INC
          semantics. KALMLIQ resolves, dates, or converts none of its own
          inputs -- every physical path and REPTDATE/RPYR/RPMTH/RPDAY/
          RD_DAYS context is owned and supplied by the calling job
          (ultimately EIBMLIQP.py).
"""
from pathlib import Path
from datetime import date

import duckdb
import polars as pl

from KAMLIQX import build_k1tbx
from KALMLIQ4 import build_k3tbl3

_KTBL_SCHEMA = {"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTUSD": pl.Float64, "AMTSGD": pl.Float64}
_DIST_SCHEMA = {"CAT": pl.Utf8, "NAME": pl.Utf8, "AMOUNT": pl.Float64}


def _remfmt(remmth: float) -> str:
    """PROC FORMAT VALUE REMFMT (numeric BNMCODE suffix bucket) -- distinct
    from EIIMRM01's report-label REMFMT of the same name."""
    if remmth <= 0.1:
        return "01"
    if remmth <= 1:
        return "02"
    if remmth <= 3:
        return "03"
    if remmth <= 6:
        return "04"
    if remmth <= 12:
        return "05"
    return "06"


def _parse_date(s) -> date | None:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    y, m, d = s.split("-")[:3]
    return date(int(y), int(m), int(d[:2]))


def _build_k1tbl(k1tbl_cache: Path) -> pl.DataFrame:
    """DATA K1TBL (KEEP=PART ITEM MATDT AMOUNT AMTUSD AMTSGD ISSDT GWCCY
    GWSHN GWC2R GWDLP GWDLR); SET BNMK.K1TBL&REPTMON&NOWK ..."""
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT
            CAST(GWCCY  AS VARCHAR) AS GWCCY,
            CAST(GWMVT  AS VARCHAR) AS GWMVT,
            CAST(GWMVTS AS VARCHAR) AS GWMVTS,
            CAST(GWOCY  AS VARCHAR) AS GWOCY,
            CAST(GWCTP  AS VARCHAR) AS GWCTP,
            CAST(GWDLP  AS VARCHAR) AS GWDLP,
            CAST(GWSHN  AS VARCHAR) AS GWSHN,
            CAST(GWC2R  AS VARCHAR) AS GWC2R,
            CAST(GWDLR  AS VARCHAR) AS GWDLR,
            CAST(GWMDT  AS VARCHAR) AS MATDT,
            CAST(GWSDT  AS VARCHAR) AS ISSDT,
            CAST(GWBALC AS DOUBLE)  AS AMOUNT
        FROM read_parquet('{k1tbl_cache.as_posix()}')
        WHERE GWMVT = 'P' AND GWOCY NOT IN ('XAU','XAT') AND GWCCY NOT IN ('XAU','XAT')
    """).pl()
    con.close()

    ROW1_BCXX = {"LO", "LC", "LF", "LS", "LOI", "LSI", "LSC", "LSW", "FDA", "FDB", "FDS", "FDL", "LOC", "LOW"}
    ROW2_BCXX = {"BO", "BF", "BOI", "BFI", "BSC", "BSW", "BOC", "BOW"}
    RM_BCXX_MI = {"LO", "LC", "LS", "LF", "LOI", "LSI", "LSC", "LOC", "FDA", "FDB", "FDS", "FDL", "LOW", "LSW"}
    RM_BCXX_BC = {"BC", "BF", "BO", "BSC", "BOW", "BSW"}

    out = []
    for r in raw.iter_rows(named=True):
        gwccy, gwmvts, gwdlp = r["GWCCY"], r["GWMVTS"], r["GWDLP"] or ""
        gwctp, gwshn = r["GWCTP"] or "", r["GWSHN"] or ""
        base = {"MATDT": r["MATDT"], "AMOUNT": r["AMOUNT"], "ISSDT": r["ISSDT"], "GWCCY": gwccy,
                "GWSHN": gwshn, "GWC2R": r["GWC2R"], "GWDLP": gwdlp, "GWDLR": r["GWDLR"]}

        if gwccy == "MYR":
            part = "95"
            amtusd = amtsgd = 0.0
            if gwmvts == "M":
                if gwdlp in ("BCD", "BCI", "BCS", "BCQ", "BCT", "BCW", "BQD"):
                    out.append({**base, "PART": part, "ITEM": "830", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                if gwctp[:1] == "B":
                    if gwdlp in ROW1_BCXX:
                        out.append({**base, "PART": part, "ITEM": "610", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                    elif gwdlp in ROW2_BCXX:
                        out.append({**base, "PART": part, "ITEM": "810", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                dlp2 = gwdlp[1:3]
                if dlp2 in ("MI", "MT"):
                    out.append({**base, "PART": part, "ITEM": "820", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                elif dlp2 in ("XI", "XT"):
                    out.append({**base, "PART": part, "ITEM": "620", "AMTUSD": amtusd, "AMTSGD": amtsgd})
            # commented-out FXS/FXO/.../MVTS branch in original SAS -- dead code, no-op
        else:
            part = "96"
            amtusd = r["AMOUNT"] if gwccy == "USD" else 0.0
            amtsgd = r["AMOUNT"] if gwccy == "SGD" else 0.0
            if gwmvts == "M" and gwctp[:1] == "B" and gwctp != "BW":
                if gwdlp in RM_BCXX_MI:
                    out.append({**base, "PART": part, "ITEM": "610", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                elif gwdlp in RM_BCXX_BC:
                    if gwshn[:6] != "FCY-FD":
                        out.append({**base, "PART": part, "ITEM": "810", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                elif gwdlp == "BOC":
                    out.append({**base, "PART": part, "ITEM": "810", "AMTUSD": amtusd, "AMTSGD": amtsgd})
            # commented-out FXS/.../GWACT NOT IN (RV,RW) branch -- dead code, no-op
    schema = {"MATDT": pl.Utf8, "AMOUNT": pl.Float64, "ISSDT": pl.Utf8, "GWCCY": pl.Utf8, "GWSHN": pl.Utf8,
              "GWC2R": pl.Utf8, "GWDLP": pl.Utf8, "GWDLR": pl.Utf8, "PART": pl.Utf8, "ITEM": pl.Utf8,
              "AMTUSD": pl.Float64, "AMTSGD": pl.Float64}
    return pl.DataFrame(out, schema=schema) if out else pl.DataFrame(schema=schema)


def _build_k3tbl(k3tbl_cache: Path, inst: str) -> pl.DataFrame:
    """DATA K3TBL (KEEP=PART ITEM MATDT AMOUNT AMTUSD AMTSGD ISSDT UTCCY
    UTCUS UTCTP UTSTY UTDLR UTDLP); RETAIN PART '95'; SET BNMK.K3TBL..."""
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT
            CAST(MATDT  AS VARCHAR) AS MATDT,
            CAST(UTAMOC AS DOUBLE)  AS UTAMOC,
            CAST(UTDPF  AS DOUBLE)  AS UTDPF,
            CAST(UTSTY  AS VARCHAR) AS UTSTY,
            CAST(UTCCY  AS VARCHAR) AS UTCCY,
            CAST(UTCUS  AS VARCHAR) AS UTCUS,
            CAST(UTCTP  AS VARCHAR) AS UTCTP,
            CAST(UTDLR  AS VARCHAR) AS UTDLR,
            CAST(UTDLP  AS VARCHAR) AS UTDLP,
            CAST(UTREF  AS VARCHAR) AS UTREF,
            CAST(UTAICT AS DOUBLE)  AS UTAICT,
            CAST(UTPCP  AS DOUBLE)  AS UTPCP,
            CAST(UTDPEY AS DOUBLE)  AS UTDPEY,
            CAST(UTDPE  AS DOUBLE)  AS UTDPE,
            CAST(UTAICY AS DOUBLE)  AS UTAICY,
            CAST(UTAIT  AS DOUBLE)  AS UTAIT,
            CAST(UTMM1  AS VARCHAR) AS UTMM1,
            CAST(GWSDT  AS VARCHAR) AS ISSDT
        FROM read_parquet('{k3tbl_cache.as_posix()}')
    """).pl()
    con.close()

    CB_SET = {"CB1", "CB2", "CF1", "CF2", "CNT", "MGS", "MTB", "BNB", "BNN", "ITB", "SAC",
              "BMN", "BMC", "BMF", "SCD", "SCM", "CMB", "MGI", "SMC"}
    I_CB_SET = {"CB1", "CB2", "CF1", "CF2", "CNT", "MGI", "ITB", "SAC", "BMN", "BMC", "BMF",
                "SCD", "SCM", "MGS", "MTB", "BNB", "BNN", "CMB", "SMC"}

    out = []
    for r in raw.iter_rows(named=True):
        utsty, utref, utdlp = r["UTSTY"] or "", r["UTREF"] or "", r["UTDLP"] or ""
        amount = (r["UTAMOC"] or 0.0) - (r["UTDPF"] or 0.0)
        if utsty == "IDC":
            amount = (r["UTAMOC"] or 0.0) + (r["UTDPF"] or 0.0)
        if inst == "PBB":
            amtusd = amount if r["UTCCY"] == "USD" else 0.0
            amtsgd = amount if r["UTCCY"] == "SGD" else 0.0
        else:
            amtusd, amtsgd = 0.0, 0.0

        base = {"PART": "95", "MATDT": r["MATDT"], "ISSDT": r["ISSDT"], "UTCCY": r["UTCCY"],
                "UTCUS": r["UTCUS"], "UTCTP": r["UTCTP"], "UTSTY": utsty, "UTDLR": r["UTDLR"], "UTDLP": utdlp}

        item, amt = None, amount
        if utref in ("INV", "DRI", "DLG", "AFSLIQ", "AFSBOND", "IAFSLIQ", "AFS", "IAFS"):
            if utsty in CB_SET:
                item = "631"
                if inst == "PBB":
                    amt = amount + (r["UTAICT"] or 0.0)
            elif utsty == "SDC":
                item = "632"
                if inst == "PBB":
                    amt = (r["UTAMOC"] or 0.0) * ((r["UTPCP"] or 0.0) / 100) + (r["UTDPEY"] or 0.0) + (r["UTDPE"] or 0.0)
            elif utsty == "LDC":
                item = "632"
                if inst == "PBB":
                    amt = amount + (r["UTAICT"] or 0.0)
            elif utsty in ("SLD", "SSD"):
                item = "632"
                if inst == "PBB":
                    amt = (r["UTAMOC"] or 0.0) * ((r["UTPCP"] or 0.0) / 100) + (r["UTAICY"] or 0.0) + (r["UTAIT"] or 0.0)
            elif utsty in ("SFD", "SZD"):
                item = "632"
                if inst == "PBB":
                    amt = amount + (r["UTAICT"] or 0.0)
            elif utsty == "SBA":
                if utdlp not in ("MOS", "MSS"):
                    item = "633"
            elif utsty in ("ISB", "DHB", "KHA", "PNB"):
                item = "636"
            elif utsty == "IDS":
                item = "635"
            elif utsty == "DBD":
                item = "634"
            elif utsty in ("DMB", "GRL", "MTL", "RUL"):
                item = "635"
            elif utsty == "PBA":
                if utdlp in ("MOS", "MSS"):
                    item = "850"
        elif utref in ("PFD", "PLD", "PSD", "PZD", "PDC"):
            if utsty in ("IFD", "ILD", "ISD", "IZD", "IDC", "IDP", "IZP"):
                item = "840"
        elif utref in ("IINV", "IDRI", "IDLG"):
            if utsty == "SBA" and utdlp == "IOP":
                item = "633"
            elif utsty in ("SDC", "LDC"):
                item = "632"
            elif utsty in I_CB_SET:
                item = "631"
                if inst == "PBB":
                    amt = amount + (r["UTAICT"] or 0.0)
            elif utsty in ("ISB", "IDS", "IBZ", "ICN"):
                if r["UTMM1"] == "GGB":
                    item = "636"
                elif r["UTMM1"] == "NGB":
                    item = "635"
                amt = amount + (r["UTAICT"] or 0.0)
            elif utsty in ("DHB", "KHA"):
                item = "636"
            elif utsty == "DBD":
                item = "634"

        if item is not None:
            out.append({**base, "ITEM": item, "AMOUNT": amt, "AMTUSD": amtusd, "AMTSGD": amtsgd})

        # IF UTSTY IN ('SIP') THEN OUTPUT -- unconditional, independent of the chain above
        if utsty == "SIP":
            out.append({**base, "ITEM": "610", "AMOUNT": amount, "AMTUSD": amtusd, "AMTSGD": amtsgd})

    schema = {"PART": pl.Utf8, "MATDT": pl.Utf8, "ISSDT": pl.Utf8, "UTCCY": pl.Utf8, "UTCUS": pl.Utf8,
              "UTCTP": pl.Utf8, "UTSTY": pl.Utf8, "UTDLR": pl.Utf8, "UTDLP": pl.Utf8, "ITEM": pl.Utf8,
              "AMOUNT": pl.Float64, "AMTUSD": pl.Float64, "AMTSGD": pl.Float64}
    return pl.DataFrame(out, schema=schema) if out else pl.DataFrame(schema=schema)


def build_kalmliq(
    k1tbl_cache: Path, k3tbl_cache: Path, reptdate: date,
    rpyr: int, rpmth: int, rpday: int, rd_days: list, inst: str = "PBB",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Returns (ktbl, dist_summary):
      ktbl          -- BNMCODE/AMOUNT/AMTUSD/AMTSGD, equivalent of
                       KTBL/KTBLALL (Part 2 + duplicated Part 1 rows).
      dist_summary  -- CAT/NAME/AMOUNT, equivalent of the final
                       PROC SUMMARY'd K1TBL EIBMRLFM reads as SUPPL.
    """
    k1tbl = _build_k1tbl(k1tbl_cache)
    k1tbx = build_k1tbx(k1tbl_cache)          # %INC PGM(KAMLIQX)
    k3tbl = _build_k3tbl(k3tbl_cache, inst)
    _ = build_k3tbl3(k3tbl_cache, reptdate)   # %INC PGM(KALMLIQ4) -- unused downstream

    def _calc_remmth(matdt: date) -> float:
        days_in_rpmth = rd_days[rpmth - 1]
        mdday = min(matdt.day, days_in_rpmth)
        remy, remm = matdt.year - rpyr, matdt.month - rpmth
        remd = mdday - rpday
        return remy * 12 + remm + remd / days_in_rpmth

    ktbl_rows = []
    for src in (k1tbl, k3tbl, k1tbx):
        for r in src.iter_rows(named=True):
            if not r.get("ITEM"):
                continue
            matdt = _parse_date(r.get("MATDT"))
            # ORI30D is computed in the SAS source but never referenced
            # downstream in KALMLIQ or EIBMRLFM -- omitted here.
            if matdt is not None and (matdt - reptdate).days < 8:
                remmth = 0.1
            elif matdt is not None:
                remmth = _calc_remmth(matdt)
            else:
                remmth = 0.1
            amtusd = r.get("AMTUSD") or 0.0
            amtsgd = r.get("AMTSGD") or 0.0
            bnmcode = f"{r['PART']}{r['ITEM']}00{_remfmt(remmth)}0000Y"
            ktbl_rows.append({"BNMCODE": bnmcode, "AMOUNT": r["AMOUNT"], "AMTUSD": amtusd, "AMTSGD": amtsgd})
            alt = "93" if r["PART"] == "95" else "94"
            ktbl_rows.append({"BNMCODE": alt + bnmcode[2:], "AMOUNT": r["AMOUNT"], "AMTUSD": amtusd, "AMTSGD": amtsgd})

    ktbl = pl.DataFrame(ktbl_rows, schema=_KTBL_SCHEMA) if ktbl_rows else pl.DataFrame(schema=_KTBL_SCHEMA)

    # ---- DISTRIBUTION PROFILE OF CUSTOMER DEPOSITS (PART 3) ----
    con = duckdb.connect(database=":memory:")
    non_interbank_repos = con.execute(f"""
        SELECT GWSHN AS NAME, GWBALC AS AMOUNT
        FROM read_parquet('{k1tbl_cache.as_posix()}')
        WHERE GWCCY = 'MYR' AND GWMVT = 'P' AND GWMVTS = 'M'
          AND SUBSTR(GWCTP,1,1) <> 'B' AND SUBSTR(GWDLP,2,2) IN ('MI','MT')
    """).pl().with_columns(pl.lit("NON-INTERBANK REPOS").alias("CAT"))
    non_interbank_nids = con.execute(f"""
        SELECT (UTCUS || UTCLC) AS NAME, (UTAMOC - UTDPF) AS AMOUNT
        FROM read_parquet('{k3tbl_cache.as_posix()}')
        WHERE SUBSTR(UTCTP,1,1) <> 'B'
          AND UTREF IN ('PFD','PLD','PSD','PZD','PDC')
          AND UTSTY IN ('IFD','ILD','ISD','IZD','IDC','IDP','IZP')
    """).pl().with_columns(pl.lit("NON-INTERBANK NIDS").alias("CAT"))
    con.close()

    dist = pl.concat([non_interbank_repos, non_interbank_nids], how="diagonal_relaxed")
    dist_summary = dist.group_by(["CAT", "NAME"]).agg(pl.col("AMOUNT").sum()) if len(dist) else pl.DataFrame(schema=_DIST_SCHEMA)

    return ktbl, dist_summary
