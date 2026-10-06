#!/usr/bin/env python3
"""
Program : KALMLIQ.py
Purpose : New Liquidity Framework (Kapiti items) -- Python port of
          %INC PGM(KALMLIQ). Reads BNMK.K1TBL<MON><NOWK> and
          BNMK.K3TBL<MON><NOWK> sas7bdat files directly (matching the
          SAS %INC source), and returns the in-memory KTBLALL frame
          equivalent plus the distribution-profile summary.
"""
from pathlib import Path
from datetime import date
from typing import Optional

import duckdb
import polars as pl
import pyreadstat


# ---------------------------------------------------------------------
# SAS7BDAT READER
# ---------------------------------------------------------------------
# def _read_sas_kapiti(path: Path) -> pl.DataFrame:
#     """Read a KAPITI sas7bdat file. Preserves uppercase SAS column names,
#     since the downstream logic refers to GW*/UT* fields in uppercase."""
#     df_pd, _ = pyreadstat.read_sas7bdat(str(path))
#     return pl.from_pandas(df_pd)

def _read_sas_kapiti(path) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    df = con.execute(f"SELECT * FROM read_parquet('{Path(path).as_posix()}')").pl()
    con.close()
    return df


def _remfmt(remmth: float) -> str:
    if remmth <= 0.1:  return "01"
    if remmth <= 1:    return "02"
    if remmth <= 3:    return "03"
    if remmth <= 6:    return "04"
    if remmth <= 12:   return "05"
    return "06"


def _parse_date(s) -> Optional[date]:
    if s is None:
        return None
    if isinstance(s, date):
        return s
    s = str(s).strip()
    if not s:
        return None
    y, m, d = s.split("-")[:3]
    return date(int(y), int(m), int(d[:2]))


# =========================================================================
# K1TBL
# =========================================================================
def _build_k1tbl(k1tbl_path: Path) -> pl.DataFrame:
    raw = _read_sas_kapiti(k1tbl_path)

    raw = raw.filter(
        (pl.col("GWMVT") == "P") &
        (~pl.col("GWOCY").fill_null("").is_in(["XAU", "XAT"])) &
        (~pl.col("GWCCY").fill_null("").is_in(["XAU", "XAT"]))
    )

    raw = raw.with_columns([
        pl.col("GWMDT").cast(pl.Utf8, strict=False).alias("MATDT"),
        pl.col("GWSDT").cast(pl.Utf8, strict=False).alias("ISSDT"),
        pl.col("GWBALC").cast(pl.Float64, strict=False).alias("AMOUNT"),
    ])

    ROW1_BCXX = {"LO","LC","LF","LS","LOI","LSI","LSC","LSW","FDA","FDB","FDS","FDL","LOC","LOW"}
    ROW2_BCXX = {"BO","BF","BOI","BFI","BSC","BSW","BOC","BOW"}
    RM_BCXX_MI = {"LO","LC","LS","LF","LOI","LSI","LSC","LOC","FDA","FDB","FDS","FDL","LOW","LSW"}
    RM_BCXX_BC = {"BC","BF","BO","BSC","BOW","BSW"}

    out = []
    for r in raw.iter_rows(named=True):
        gwccy  = r.get("GWCCY")
        gwmvts = r.get("GWMVTS")
        gwdlp  = r.get("GWDLP") or ""
        gwctp  = r.get("GWCTP") or ""
        gwshn  = r.get("GWSHN") or ""

        base = {
            "MATDT":  r.get("MATDT"),
            "AMOUNT": r.get("AMOUNT"),
            "ISSDT":  r.get("ISSDT"),
            "GWCCY":  gwccy,
            "GWSHN":  gwshn,
            "GWC2R":  r.get("GWC2R"),
            "GWDLP":  gwdlp,
            "GWDLR":  r.get("GWDLR"),
        }

        if gwccy == "MYR":
            part = "95"
            amtusd = amtsgd = 0.0
            if gwmvts == "M":
                if gwdlp in ("BCD","BCI","BCS","BCQ","BCT","BCW","BQD"):
                    out.append({**base, "PART": part, "ITEM": "830", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                if gwctp[:1] == "B":
                    if gwdlp in ROW1_BCXX:
                        out.append({**base, "PART": part, "ITEM": "610", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                    elif gwdlp in ROW2_BCXX:
                        out.append({**base, "PART": part, "ITEM": "810", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                dlp2 = gwdlp[1:3]
                if dlp2 in ("MI","MT"):
                    out.append({**base, "PART": part, "ITEM": "820", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                elif dlp2 in ("XI","XT"):
                    out.append({**base, "PART": part, "ITEM": "620", "AMTUSD": amtusd, "AMTSGD": amtsgd})
        else:
            part = "96"
            amtusd = r.get("AMOUNT") if gwccy == "USD" else 0.0
            amtsgd = r.get("AMOUNT") if gwccy == "SGD" else 0.0
            if gwmvts == "M" and gwctp[:1] == "B" and gwctp != "BW":
                if gwdlp in RM_BCXX_MI:
                    out.append({**base, "PART": part, "ITEM": "610", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                elif gwdlp in RM_BCXX_BC:
                    if gwshn[:6] != "FCY-FD":
                        out.append({**base, "PART": part, "ITEM": "810", "AMTUSD": amtusd, "AMTSGD": amtsgd})
                elif gwdlp == "BOC":
                    out.append({**base, "PART": part, "ITEM": "810", "AMTUSD": amtusd, "AMTSGD": amtsgd})

    schema = {"MATDT": pl.Utf8, "AMOUNT": pl.Float64, "ISSDT": pl.Utf8, "GWCCY": pl.Utf8,
              "GWSHN": pl.Utf8, "GWC2R": pl.Utf8, "GWDLP": pl.Utf8, "GWDLR": pl.Utf8,
              "PART": pl.Utf8, "ITEM": pl.Utf8, "AMTUSD": pl.Float64, "AMTSGD": pl.Float64}
    return pl.DataFrame(out, schema=schema) if out else pl.DataFrame(schema=schema)


# =========================================================================
# K3TBL
# =========================================================================
def _build_k3tbl(k3tbl_path: Path, inst: str) -> pl.DataFrame:
    raw = _read_sas_kapiti(k3tbl_path)

    for c in ["UTAMOC","UTDPF","UTAICT","UTPCP","UTDPEY","UTDPE","UTAICY","UTAIT"]:
        if c in raw.columns:
            raw = raw.with_columns(pl.col(c).cast(pl.Float64, strict=False))
    raw = raw.with_columns([
        pl.col("MATDT").cast(pl.Utf8, strict=False).alias("MATDT"),
        pl.col("ISSDT").cast(pl.Utf8, strict=False).alias("ISSDT"),
    ])

    CB_SET = {"CB1","CB2","CF1","CF2","CNT","MGS","MTB","BNB","BNN","ITB","SAC",
              "BMN","BMC","BMF","SCD","SCM","CMB","MGI","SMC"}
    I_CB_SET = {"CB1","CB2","CF1","CF2","CNT","MGI","ITB","SAC","BMN","BMC","BMF",
                "SCD","SCM","MGS","MTB","BNB","BNN","CMB","SMC"}

    out = []
    for r in raw.iter_rows(named=True):
        utsty = r.get("UTSTY") or ""
        utref = r.get("UTREF") or ""
        utdlp = r.get("UTDLP") or ""
        amount = (r.get("UTAMOC") or 0.0) - (r.get("UTDPF") or 0.0)
        if utsty == "IDC":
            amount = (r.get("UTAMOC") or 0.0) + (r.get("UTDPF") or 0.0)

        if inst == "PBB":
            amtusd = amount if r.get("UTCCY") == "USD" else 0.0
            amtsgd = amount if r.get("UTCCY") == "SGD" else 0.0
        else:
            amtusd, amtsgd = 0.0, 0.0

        base = {"PART": "95", "MATDT": r.get("MATDT"), "ISSDT": r.get("ISSDT"),
                "UTCCY": r.get("UTCCY"), "UTCUS": r.get("UTCUS"), "UTCTP": r.get("UTCTP"),
                "UTSTY": utsty, "UTDLR": r.get("UTDLR"), "UTDLP": utdlp}

        item, amt = None, amount
        if utref in ("INV","DRI","DLG","AFSLIQ","AFSBOND","IAFSLIQ","AFS","IAFS"):
            if utsty in CB_SET:
                item = "631"
                if inst == "PBB": amt = amount + (r.get("UTAICT") or 0.0)
            elif utsty == "SDC":
                item = "632"
                if inst == "PBB":
                    amt = (r.get("UTAMOC") or 0.0) * ((r.get("UTPCP") or 0.0)/100) \
                          + (r.get("UTDPEY") or 0.0) + (r.get("UTDPE") or 0.0)
            elif utsty == "LDC":
                item = "632"
                if inst == "PBB": amt = amount + (r.get("UTAICT") or 0.0)
            elif utsty in ("SLD","SSD"):
                item = "632"
                if inst == "PBB":
                    amt = (r.get("UTAMOC") or 0.0)*((r.get("UTPCP") or 0.0)/100) \
                          + (r.get("UTAICY") or 0.0) + (r.get("UTAIT") or 0.0)
            elif utsty in ("SFD","SZD"):
                item = "632"
                if inst == "PBB": amt = amount + (r.get("UTAICT") or 0.0)
            elif utsty == "SBA":
                if utdlp not in ("MOS","MSS"): item = "633"
            elif utsty in ("ISB","DHB","KHA","PNB"): item = "636"
            elif utsty == "IDS": item = "635"
            elif utsty == "DBD": item = "634"
            elif utsty in ("DMB","GRL","MTL","RUL"): item = "635"
            elif utsty == "PBA":
                if utdlp in ("MOS","MSS"): item = "850"
        elif utref in ("PFD","PLD","PSD","PZD","PDC"):
            if utsty in ("IFD","ILD","ISD","IZD","IDC","IDP","IZP"): item = "840"
        elif utref in ("IINV","IDRI","IDLG"):
            if utsty == "SBA" and utdlp == "IOP": item = "633"
            elif utsty in ("SDC","LDC"): item = "632"
            elif utsty in I_CB_SET:
                item = "631"
                if inst == "PBB": amt = amount + (r.get("UTAICT") or 0.0)
            elif utsty in ("ISB","IDS","IBZ","ICN"):
                pass
            elif utsty in ("DHB","KHA"): item = "636"
            elif utsty == "DBD": item = "634"

        if item is not None:
            out.append({**base, "ITEM": item, "AMOUNT": amt, "AMTUSD": amtusd, "AMTSGD": amtsgd})

        if utsty == "SIP":
            out.append({**base, "ITEM": "610", "AMOUNT": amount, "AMTUSD": amtusd, "AMTSGD": amtsgd})

    schema = {"PART": pl.Utf8, "MATDT": pl.Utf8, "ISSDT": pl.Utf8, "UTCCY": pl.Utf8,
              "UTCUS": pl.Utf8, "UTCTP": pl.Utf8, "UTSTY": pl.Utf8, "UTDLR": pl.Utf8,
              "UTDLP": pl.Utf8, "ITEM": pl.Utf8, "AMOUNT": pl.Float64,
              "AMTUSD": pl.Float64, "AMTSGD": pl.Float64}
    return pl.DataFrame(out, schema=schema) if out else pl.DataFrame(schema=schema)


# =========================================================================
# KTBLALL BUILDER
# =========================================================================
def build_kalmliq(
    k1tbl_path: Path,
    k3tbl_path: Path,
    reptdate: date,
    rpyr: int, rpmth: int, rpday: int, rd_days: list,
    inst: str = "PBB",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    k1tbl = _build_k1tbl(k1tbl_path)
    k3tbl = _build_k3tbl(k3tbl_path, inst)

    def _calc_remmth(matdt: date) -> float:
        days_in_rpmth = rd_days[rpmth - 1]
        mdday = min(matdt.day, days_in_rpmth)
        remy = matdt.year - rpyr
        remm = matdt.month - rpmth
        remd = mdday - rpday
        return remy*12 + remm + remd/days_in_rpmth

    ktbl_rows = []
    for src in (k1tbl, k3tbl):
        for r in src.iter_rows(named=True):
            if not r.get("ITEM"):
                continue
            matdt = _parse_date(r.get("MATDT"))
            if matdt is not None and (matdt - reptdate).days < 8:
                remmth = 0.1
            elif matdt is not None:
                remmth = _calc_remmth(matdt)
            else:
                remmth = 0.1
            amtusd = r.get("AMTUSD") or 0.0
            amtsgd = r.get("AMTSGD") or 0.0
            bnmcode = f"{r['PART']}{r['ITEM']}00{_remfmt(remmth)}0000Y"
            ktbl_rows.append({"BNMCODE": bnmcode, "AMOUNT": r["AMOUNT"],
                              "AMTUSD": amtusd, "AMTSGD": amtsgd})
            alt = "93" if r["PART"] == "95" else "94"
            ktbl_rows.append({"BNMCODE": alt + bnmcode[2:], "AMOUNT": r["AMOUNT"],
                              "AMTUSD": amtusd, "AMTSGD": amtsgd})

    schema = {"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64,
              "AMTUSD": pl.Float64, "AMTSGD": pl.Float64}
    ktbl = pl.DataFrame(ktbl_rows, schema=schema) if ktbl_rows else pl.DataFrame(schema=schema)

    raw_k1 = _read_sas_kapiti(k1tbl_path)
    raw_k3 = _read_sas_kapiti(k3tbl_path)

    non_interbank_repos = (
        raw_k1
        .filter(
            (pl.col("GWCCY") == "MYR") & (pl.col("GWMVT") == "P") & (pl.col("GWMVTS") == "M") &
            (pl.col("GWCTP").str.slice(0,1) != "B") &
            (pl.col("GWDLP").str.slice(1,2).is_in(["MI","MT"]))
        )
        .select([pl.col("GWSHN").alias("NAME"),
                 pl.col("GWBALC").cast(pl.Float64).alias("AMOUNT")])
        .with_columns(pl.lit("NON-INTERBANK REPOS").alias("CAT"))
    )

    non_interbank_nids = (
        raw_k3
        .filter(
            (pl.col("UTCTP").str.slice(0,1) != "B") &
            (pl.col("UTREF").is_in(["PFD","PLD","PSD","PZD","PDC"])) &
            (pl.col("UTSTY").is_in(["IFD","ILD","ISD","IZD","IDC","IDP","IZP"]))
        )
        .select([(pl.col("UTCUS").cast(pl.Utf8) + pl.col("UTCLC").cast(pl.Utf8)).alias("NAME"),
                 (pl.col("UTAMOC").cast(pl.Float64) - pl.col("UTDPF").cast(pl.Float64)).alias("AMOUNT")])
        .with_columns(pl.lit("NON-INTERBANK NIDS").alias("CAT"))
    )

    dist = pl.concat([non_interbank_repos, non_interbank_nids], how="diagonal_relaxed")
    dist_summary = (dist.group_by(["CAT","NAME"]).agg(pl.col("AMOUNT").sum())
                    if len(dist) else
                    pl.DataFrame(schema={"CAT": pl.Utf8, "NAME": pl.Utf8, "AMOUNT": pl.Float64}))

    return ktbl, dist_summary
