#!/usr/bin/env python3
"""
Program : KALMLIQ.py
Purpose : New Liquidity Framework (Kapiti items) -- Python port of
          %INC PGM(KALMLIQ). Reads BNMK.k1tbl<MON><NOWK> and
          BNMK.k3tbl<MON><NOWK> sas7bdat files directly, and returns the
          in-memory KTBLALL frame (with all source fields preserved so the
          caller can perform the PBBELF DATA ALLEQU rebuild) plus the
          distribution-profile summary.
"""
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Optional

import duckdb
import polars as pl
import pyreadstat


# ---------------------------------------------------------------------
# SAS7BDAT READER (KAPITI: preserves uppercase column names)
# ---------------------------------------------------------------------
# def _read_sas_kapiti(path: Path) -> pl.DataFrame:
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
    """Robust MATDT parser -- handles Python date/datetime, SAS numeric
    date (days since 1960-01-01), ISO strings, YYYYMMDD, DD/MM/YYYY,
    DD-Mon-YYYY, YYYY/MM/DD."""
    if s is None:
        return None
    if isinstance(s, datetime):
        return s.date()
    if isinstance(s, date):
        return s
    if isinstance(s, (int, float)):
        try:
            return date(1960, 1, 1) + timedelta(days=int(s))
        except Exception:
            return None
    t = str(s).strip()
    if not t or t.lower() in ("nan", "nat", "none", "null", ""):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d",
                "%d/%m/%Y", "%d-%b-%Y", "%d-%b-%y", "%Y/%m/%d", "%d%m%Y"):
        try:
            return datetime.strptime(t[:19], fmt).date()
        except ValueError:
            continue
    try:
        import pandas as pd
        return pd.to_datetime(t).date()
    except Exception:
        return None


# =========================================================================
# K1TBL
# =========================================================================
def _build_k1tbl(k1tbl_path: Path) -> pl.DataFrame:
    print(f"    [_build_k1tbl] reading {k1tbl_path}")
    raw = _read_sas_kapiti(k1tbl_path)
    print(f"    [_build_k1tbl] columns: {raw.columns}")
    print(f"    [_build_k1tbl] rows: {len(raw)}")

    if "GWMDT" in raw.columns:
        try:
            print(f"    [_build_k1tbl] GWMDT sample: {raw['GWMDT'].head(3).to_list()}")
            print(f"    [_build_k1tbl] GWMDT dtype: {raw['GWMDT'].dtype}")
        except Exception as e:
            print(f"    [_build_k1tbl] GWMDT diag failed: {e}")

    raw = raw.filter(
        (pl.col("GWMVT") == "P") &
        (~pl.col("GWOCY").cast(pl.Utf8, strict=False).fill_null("").is_in(["XAU", "XAT"])) &
        (~pl.col("GWCCY").cast(pl.Utf8, strict=False).fill_null("").is_in(["XAU", "XAT"]))
    )
    print(f"    [_build_k1tbl] after filter GWMVT='P': {len(raw)}")

    raw = raw.with_columns([
        pl.col("GWMDT").alias("MATDT"),
        pl.col("GWSDT").alias("ISSDT"),
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
        gwdlp  = (r.get("GWDLP") or "").strip() if isinstance(r.get("GWDLP"), str) else ""
        gwctp  = (r.get("GWCTP") or "").strip() if isinstance(r.get("GWCTP"), str) else ""
        gwshn  = (r.get("GWSHN") or "").strip() if isinstance(r.get("GWSHN"), str) else ""

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
    if out:
        df = pl.DataFrame(out)
        for c, t in schema.items():
            if c not in df.columns:
                df = df.with_columns(pl.lit(None).cast(t).alias(c))
            else:
                df = df.with_columns(pl.col(c).cast(t, strict=False))
        df = df.select(list(schema.keys()))
    else:
        df = pl.DataFrame(schema=schema)
    print(f"    [_build_k1tbl] emitted rows: {len(df)}")
    return df


# =========================================================================
# K3TBL
# =========================================================================
def _build_k3tbl(k3tbl_path: Path, inst: str) -> pl.DataFrame:
    print(f"    [_build_k3tbl] reading {k3tbl_path}")
    raw = _read_sas_kapiti(k3tbl_path)
    print(f"    [_build_k3tbl] columns: {raw.columns}")
    print(f"    [_build_k3tbl] rows: {len(raw)}")

    if "MATDT" in raw.columns:
        try:
            print(f"    [_build_k3tbl] MATDT sample: {raw['MATDT'].head(3).to_list()}")
            print(f"    [_build_k3tbl] MATDT dtype: {raw['MATDT'].dtype}")
        except Exception as e:
            print(f"    [_build_k3tbl] MATDT diag failed: {e}")

    for c in ["UTAMOC","UTDPF","UTAICT","UTPCP","UTDPEY","UTDPE","UTAICY","UTAIT"]:
        if c in raw.columns:
            raw = raw.with_columns(pl.col(c).cast(pl.Float64, strict=False))

    CB_SET = {"CB1","CB2","CF1","CF2","CNT","MGS","MTB","BNB","BNN","ITB","SAC",
              "BMN","BMC","BMF","SCD","SCM","CMB","MGI","SMC"}
    I_CB_SET = {"CB1","CB2","CF1","CF2","CNT","MGI","ITB","SAC","BMN","BMC","BMF",
                "SCD","SCM","MGS","MTB","BNB","BNN","CMB","SMC"}

    out = []
    for r in raw.iter_rows(named=True):
        utsty = (r.get("UTSTY") or "").strip() if isinstance(r.get("UTSTY"), str) else ""
        utref = (r.get("UTREF") or "").strip() if isinstance(r.get("UTREF"), str) else ""
        utdlp = (r.get("UTDLP") or "").strip() if isinstance(r.get("UTDLP"), str) else ""

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
    if out:
        df = pl.DataFrame(out)
        for c, t in schema.items():
            if c not in df.columns:
                df = df.with_columns(pl.lit(None).cast(t).alias(c))
            else:
                df = df.with_columns(pl.col(c).cast(t, strict=False))
        df = df.select(list(schema.keys()))
    else:
        df = pl.DataFrame(schema=schema)
    print(f"    [_build_k3tbl] emitted rows: {len(df)}")
    return df


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
    """
    Returns (ktbl, dist_summary):
      ktbl         -- KTBLALL equivalent, with all source fields preserved
                      so the caller can perform the PBBELF DATA ALLEQU merge
                      with UTSAS and rebuild BNMCODE.
      dist_summary -- distribution-profile summary (CAT, NAME, AMOUNT).
    """
    k1tbl = _build_k1tbl(k1tbl_path)
    k3tbl = _build_k3tbl(k3tbl_path, inst)

    def _calc_remmth(matdt: date) -> float:
        days_in_rpmth = rd_days[rpmth - 1]
        mdday = min(matdt.day, days_in_rpmth)
        remy = matdt.year - rpyr
        remm = matdt.month - rpmth
        remd = mdday - rpday
        return remy*12 + remm + remd/days_in_rpmth

    # Schema for the aggregated KTBLALL frame
    ktbl_schema = {
        "TBL":     pl.Utf8,
        "PART":    pl.Utf8,
        "ITEM":    pl.Utf8,
        "MATDT":   pl.Utf8,
        "ISSDT":   pl.Utf8,
        "AMOUNT":  pl.Float64,
        "AMTUSD":  pl.Float64,
        "AMTSGD":  pl.Float64,
        "BNMCODE": pl.Utf8,
        "GWCCY":   pl.Utf8,
        "GWSHN":   pl.Utf8,
        "GWC2R":   pl.Utf8,
        "GWDLP":   pl.Utf8,
        "GWDLR":   pl.Utf8,
        "UTCCY":   pl.Utf8,
        "UTCUS":   pl.Utf8,
        "UTCTP":   pl.Utf8,
        "UTSTY":   pl.Utf8,
        "UTDLR":   pl.Utf8,
        "UTDLP":   pl.Utf8,
    }

    ktbl_rows = []
    parse_fail = 0
    for src_label, src in (("K1TBL", k1tbl), ("K3TBL", k3tbl)):
        for r in src.iter_rows(named=True):
            if not r.get("ITEM"):
                continue
            matdt = _parse_date(r.get("MATDT"))
            if matdt is None:
                parse_fail += 1
                remmth = 0.1
            elif (matdt - reptdate).days < 8:
                remmth = 0.1
            else:
                remmth = _calc_remmth(matdt)
            amtusd = r.get("AMTUSD") or 0.0
            amtsgd = r.get("AMTSGD") or 0.0
            bnmcode = f"{r['PART']}{r['ITEM']}00{_remfmt(remmth)}0000Y"

            base_row = {
                "TBL":     "1" if src_label == "K1TBL" else "3",
                "PART":    r["PART"],
                "ITEM":    r["ITEM"],
                "MATDT":   r.get("MATDT"),
                "ISSDT":   r.get("ISSDT"),
                "AMOUNT":  r["AMOUNT"],
                "AMTUSD":  amtusd,
                "AMTSGD":  amtsgd,
                "BNMCODE": bnmcode,
                "GWCCY":   r.get("GWCCY"),
                "GWSHN":   r.get("GWSHN"),
                "GWC2R":   r.get("GWC2R"),
                "GWDLP":   r.get("GWDLP"),
                "GWDLR":   r.get("GWDLR"),
                "UTCCY":   r.get("UTCCY"),
                "UTCUS":   r.get("UTCUS"),
                "UTCTP":   r.get("UTCTP"),
                "UTSTY":   r.get("UTSTY"),
                "UTDLR":   r.get("UTDLR"),
                "UTDLP":   r.get("UTDLP"),
            }
            ktbl_rows.append(base_row)
            alt = dict(base_row)
            alt_bnm = ("93" if r["PART"] == "95" else "94") + bnmcode[2:]
            alt["BNMCODE"] = alt_bnm
            ktbl_rows.append(alt)

    if parse_fail:
        print(f"    [build_kalmliq] WARNING: {parse_fail} MATDT values unparseable (defaulted to 0.1)")

    # ---- FIX: build with explicit schema; do not rely on inference ----
    if ktbl_rows:
        df = pl.DataFrame(ktbl_rows, infer_schema_length=None)
        for c, t in ktbl_schema.items():
            if c not in df.columns:
                df = df.with_columns(pl.lit(None).cast(t).alias(c))
            else:
                df = df.with_columns(pl.col(c).cast(t, strict=False))
        df = df.select(list(ktbl_schema.keys()))
    else:
        df = pl.DataFrame(schema=ktbl_schema)
    ktbl = df
    print(f"    [build_kalmliq] ktbl rows: {len(ktbl)}")

    # ---- Distribution profile ----
    raw_k1 = _read_sas_kapiti(k1tbl_path)
    raw_k3 = _read_sas_kapiti(k3tbl_path)

    dist_schema = {"CAT": pl.Utf8, "NAME": pl.Utf8, "AMOUNT": pl.Float64}

    try:
        non_interbank_repos = (
            raw_k1
            .filter(
                (pl.col("GWCCY") == "MYR") & (pl.col("GWMVT") == "P") & (pl.col("GWMVTS") == "M") &
                (pl.col("GWCTP").cast(pl.Utf8, strict=False).str.slice(0,1) != "B") &
                (pl.col("GWDLP").cast(pl.Utf8, strict=False).str.slice(1,2).is_in(["MI","MT"]))
            )
            .select([pl.col("GWSHN").cast(pl.Utf8, strict=False).alias("NAME"),
                     pl.col("GWBALC").cast(pl.Float64, strict=False).alias("AMOUNT")])
            .with_columns(pl.lit("NON-INTERBANK REPOS").cast(pl.Utf8).alias("CAT"))
        )
    except Exception as e:
        print(f"    [build_kalmliq] non_interbank_repos filter warning: {e}")
        non_interbank_repos = pl.DataFrame(schema=dist_schema)

    try:
        non_interbank_nids = (
            raw_k3
            .filter(
                (pl.col("UTCTP").cast(pl.Utf8, strict=False).str.slice(0,1) != "B") &
                (pl.col("UTREF").cast(pl.Utf8, strict=False).is_in(["PFD","PLD","PSD","PZD","PDC"])) &
                (pl.col("UTSTY").cast(pl.Utf8, strict=False).is_in(["IFD","ILD","ISD","IZD","IDC","IDP","IZP"]))
            )
            .select([(pl.col("UTCUS").cast(pl.Utf8, strict=False) + pl.col("UTCLC").cast(pl.Utf8, strict=False)).alias("NAME"),
                     (pl.col("UTAMOC").cast(pl.Float64, strict=False) - pl.col("UTDPF").cast(pl.Float64, strict=False)).alias("AMOUNT")])
            .with_columns(pl.lit("NON-INTERBANK NIDS").cast(pl.Utf8).alias("CAT"))
        )
    except Exception as e:
        print(f"    [build_kalmliq] non_interbank_nids filter warning: {e}")
        non_interbank_nids = pl.DataFrame(schema=dist_schema)

    if len(non_interbank_repos) and len(non_interbank_nids):
        dist = pl.concat([non_interbank_repos, non_interbank_nids], how="diagonal_relaxed")
    elif len(non_interbank_repos):
        dist = non_interbank_repos
    elif len(non_interbank_nids):
        dist = non_interbank_nids
    else:
        dist = pl.DataFrame(schema=dist_schema)

    dist_summary = (dist.group_by(["CAT","NAME"]).agg(pl.col("AMOUNT").sum())
                    if len(dist) else
                    pl.DataFrame(schema=dist_schema))

    return ktbl, dist_summary
