#!/usr/bin/env python3
"""
Program : KALMLIQI.py
Purpose : KAPITI-sourced liquidity items for the BNM New Liquidity
          Framework report (PART 1/2 BNMCODE detail + PART 3 customer-
          deposit distribution-profile summary). Originally %INC'd by
          EIBMRLFI after EIBMRLFI has derived REPTDATE/RPYR/RPMTH/RPDAY/
          REPTMON/NOWK and declared the REMFMT format plus the %DCLVAR /
          %REMMTH macros used below. Since Python modules do not share a
          %INC'd compile-time scope the way SAS does, this module is
          called as KALMLIQI.main(...) with that context passed in as
          parameters, and the REMFMT/REMMTH logic is replicated locally.

Dependency:
    EIBMRLFI.py -> supplies the REPTDATE/RPYR/RPMTH/RPDAY/RD_DAYS/REPTMON/
                   NOWK/INST context this module is driven with, and is
                   the program whose SYSIN originally declared REMFMT /
                   %REMMTH (replicated locally here, see remfmt_format()
                   and remmth() below).
    DALWPBBD.py -> no dataset from DALWPBBD is read directly by this
                   program. It is listed here only because KALMLIQI runs
                   inside the same EIBMLIQI job stream where DALWPBBD's
                   BNM.SAVG/BNM.CURN are also produced; this program only
                   ever reads the two KAPITI (BNMK) sources below.

============================================================================
PHYSICAL INPUT DATASETS (each cached to Parquet independently)
============================================================================
1. BNMK.K1TBL&REPTMON&NOWK  (KAPITI treasury/GW deal extract)
   File : k1tbl<REPTMON><NOWK>.sas7bdat
   Path : built from INPUT_K1TBL_DIR + REPTMON + NOWK
   Cols used : GWAB, GWOCY, GWCCY, GWMVT, GWMVTS, GWDLP, GWCTP, GWSHN,
               GWACT, GWMDT (->MATDT), GWBALC (->AMOUNT)
   The filename carries REPTMON+NOWK deterministically (not a "latest
   file" style name), so the path is built directly rather than via
   input_date.get_latest_file().

2. BNMK.K3TBL&REPTMON&NOWK  (KAPITI money-market / NIDS extract)
   File : k3tbl<REPTMON><NOWK>.sas7bdat
   Path : built from INPUT_K3TBL_DIR + REPTMON + NOWK
   Cols used : UTREF, UTSTY, UTCCY, UTAMOC, UTDPF, UTDLP, UTAICT, UTCTP,
               UTCUS, UTCLC
   Filename carries REPTMON+NOWK -- built directly, same reasoning.

============================================================================
OUTPUTS (in-memory only -- KALMLIQI has no DD-level output of its own)
============================================================================
main() returns a dict:
  - 'ktbl'          : BNMCODE / AMOUNT / AMTUSD / AMTSGD (two rows per
                       source row -- the PART-substituted duplicate row is
                       preserved verbatim, see build_ktbl()). Consumed by
                       EIBMRLFI as "SET NOTE KTBL;".
  - 'k1tbl_summary' : CAT / NAME / AMOUNT (PART 3 distribution profile,
                       PROC SUMMARY NWAY CLASS CAT NAME). Consumed by
                       EIBMRLFI as SUPPL = WHERE ABS(AMOUNT) >= 5,000,000.
"""

import gc
from pathlib import Path
from datetime import date

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR = Path("/stgsrcsys/host/uat/AII")

INPUT_K1TBL_DIR = STG_DIR / "sasdata"
INPUT_K3TBL_DIR = STG_DIR / "sasdata"

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMLIQI"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000


# ============================================================================
# LOCAL FORMAT / MACRO EQUIVALENTS
# (REMFMT is a PROC FORMAT declared in EIBMRLFI's SYSIN, before
#  %INC PGM(KALMLIQI); %DCLVAR / %REMMTH are macros declared there too.
#  Replicated verbatim here since Python modules do not share a %INC'd
#  compile-time scope the way SAS does.)
# ============================================================================
def remfmt_format(value: float) -> str:
    """PROC FORMAT VALUE REMFMT. (LOW-0.1='01', 0.1-1='02', 1-3='03',
    3-6='04', 6-12='05', OTHER='06'). Ascending <= checks give the same
    first-match-wins boundary resolution SAS uses for overlapping ranges."""
    if value <= 0.1:
        return "01"
    if value <= 1:
        return "02"
    if value <= 3:
        return "03"
    if value <= 6:
        return "04"
    if value <= 12:
        return "05"
    return "06"


def build_rd_days(rpyr: int) -> list:
    """%DCLVAR's RD1-RD12 (days-per-month for the report year)."""
    days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if rpyr % 4 == 0:
        days[1] = 29
    return days


def remmth(matdt: date, rpyr: int, rpmth: int, rpday: int, rd_days: list) -> float:
    """%REMMTH macro. NOTE: RPDAYS(RPMTH) caps MDDAY using the REPORT
    month's day-count, not the maturity month's -- preserved verbatim,
    matching EIIMRM01.py's identical quirk."""
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    days_in_rpmth = rd_days[rpmth - 1]
    if mdday > days_in_rpmth:
        mdday = days_in_rpmth
    remy = mdyr - rpyr
    remm_ = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm_ + remd / days_in_rpmth


# ============================================================================
# CACHE HELPER: STREAM .sas7bdat -> PARQUET (EIBDLN1M.py pattern)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if schema is None:
            fields = []
            for col, dtype in chunk.dtypes.items():
                if dtype == "object":
                    pa_type = pa.string()
                elif pd.api.types.is_integer_dtype(dtype):
                    pa_type = pa.int64()
                elif pd.api.types.is_float_dtype(dtype):
                    pa_type = pa.float64()
                else:
                    pa_type = pa.from_numpy_dtype(dtype)
                fields.append(pa.field(col, pa_type))
            schema = pa.schema(fields)
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")

        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# STEP A: K1TBL detail (PART 1/2 BNMCODE) -- non-interbank REPO/GW deals
# ============================================================================
def _build_k1tbl_detail(k1_cache: Path) -> list:
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT
            CAST(GWAB   AS DOUBLE)  AS GWAB,
            CAST(GWOCY  AS VARCHAR) AS GWOCY,
            CAST(GWCCY  AS VARCHAR) AS GWCCY,
            CAST(GWMVT  AS VARCHAR) AS GWMVT,
            CAST(GWMVTS AS VARCHAR) AS GWMVTS,
            CAST(GWDLP  AS VARCHAR) AS GWDLP,
            CAST(GWCTP  AS VARCHAR) AS GWCTP,
            CAST(GWSHN  AS VARCHAR) AS GWSHN,
            CAST(GWACT  AS VARCHAR) AS GWACT,
            CAST(GWMDT  AS DATE)    AS MATDT,
            CAST(GWBALC AS DOUBLE)  AS AMOUNT
        FROM read_parquet('{k1_cache.as_posix()}')
    """).pl()
    con.close()

    rows = []
    for r in raw.iter_rows(named=True):
        if r["GWAB"] is None or r["GWAB"] <= 2000:
            continue
        if r["GWOCY"] == "XAU" or r["GWCCY"] == "XAU":
            continue
        if r["GWMVT"] != "P":
            continue

        matdt, amount = r["MATDT"], r["AMOUNT"]
        gwdlp = r["GWDLP"]
        gwctp = r["GWCTP"] or ""
        gwshn = r["GWSHN"] or ""
        gwact = r["GWACT"]
        gwmvts = r["GWMVTS"]
        ctp1 = gwctp[:1]
        dlp2 = (gwdlp or "")[1:3]

        if r["GWCCY"] == "MYR":
            part, amtusd, amtsgd = "95", 0.0, 0.0

            if gwmvts == "M":
                # Three INDEPENDENT checks -- a single row can OUTPUT
                # 0, 1, 2, or 3 times, exactly as the SAS source does.
                if gwdlp in ("BCD", "BCI"):
                    rows.append({"PART": part, "ITEM": "830", "MATDT": matdt,
                                 "AMOUNT": amount, "AMTUSD": amtusd, "AMTSGD": amtsgd})

                if ctp1 == "B":
                    if gwdlp in ("LO", "LC", "LF", "LS", "LOI", "LSI", "LSC",
                                 "FDA", "FDB", "FDS", "FDL"):
                        rows.append({"PART": part, "ITEM": "610", "MATDT": matdt,
                                     "AMOUNT": amount, "AMTUSD": amtusd, "AMTSGD": amtsgd})
                    elif gwdlp in ("BO", "BF", "BOI", "BFI", "BSC"):
                        rows.append({"PART": part, "ITEM": "810", "MATDT": matdt,
                                     "AMOUNT": amount, "AMTUSD": amtusd, "AMTSGD": amtsgd})

                if dlp2 in ("MI", "MT"):
                    item = "820" if ctp1 == "B" else "830"
                    rows.append({"PART": part, "ITEM": item, "MATDT": matdt,
                                 "AMOUNT": amount, "AMTUSD": amtusd, "AMTSGD": amtsgd})
                elif dlp2 in ("XI", "XT"):
                    rows.append({"PART": part, "ITEM": "620", "MATDT": matdt,
                                 "AMOUNT": amount, "AMTUSD": amtusd, "AMTSGD": amtsgd})

            elif gwdlp in ("FXS", "FXO", "FXF", "TS1", "TS2", "SF1", "SF2", "FF1", "FF2"):
                # OUTPUT here is UNCONDITIONAL in the SAS source, even when
                # GWMVTS is neither 'P' nor 'S' (ITEM then stays missing) --
                # preserved; downstream IF ITEM^=' ' drops such rows.
                item = "711" if gwmvts == "P" else ("911" if gwmvts == "S" else None)
                rows.append({"PART": part, "ITEM": item, "MATDT": matdt,
                             "AMOUNT": amount, "AMTUSD": amtusd, "AMTSGD": amtsgd})

        else:
            part = "96"
            amtusd = amount if r["GWCCY"] == "USD" else 0.0
            amtsgd = amount if r["GWCCY"] == "SGD" else 0.0

            if gwmvts == "M":
                if ctp1 == "B":
                    if gwdlp in ("LO", "LC", "LS", "LF", "LOI", "LSI", "LSC",
                                 "FDA", "FDB", "FDS", "FDL"):
                        rows.append({"PART": part, "ITEM": "610", "MATDT": matdt,
                                     "AMOUNT": amount, "AMTUSD": amtusd, "AMTSGD": amtsgd})
                    elif gwdlp in ("BC", "BF", "BO", "BSC"):
                        if gwshn[:6] != "FCY-FD":
                            rows.append({"PART": part, "ITEM": "810", "MATDT": matdt,
                                         "AMOUNT": amount, "AMTUSD": amtusd, "AMTSGD": amtsgd})

            elif gwdlp in ("FXS", "FXO", "FXF", "TS1", "TS2", "SF1", "SF2", "FF1", "FF2") \
                    and gwact not in ("RV", "RW"):
                item = "711" if gwmvts == "P" else ("911" if gwmvts == "S" else None)
                rows.append({"PART": part, "ITEM": item, "MATDT": matdt,
                             "AMOUNT": amount, "AMTUSD": amtusd, "AMTSGD": amtsgd})

    return rows


# ============================================================================
# STEP B: K3TBL detail (PART 1/2 BNMCODE) -- money-market / NIDS
# ============================================================================
def _build_k3tbl_detail(k3_cache: Path, inst: str) -> list:
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT
            CAST(UTREF  AS VARCHAR) AS UTREF,
            CAST(UTSTY  AS VARCHAR) AS UTSTY,
            CAST(UTCCY  AS VARCHAR) AS UTCCY,
            CAST(UTAMOC AS DOUBLE)  AS UTAMOC,
            CAST(UTDPF  AS DOUBLE)  AS UTDPF,
            CAST(UTDLP  AS VARCHAR) AS UTDLP,
            CAST(UTAICT AS DOUBLE)  AS UTAICT,
            CAST(UTCTP  AS VARCHAR) AS UTCTP,
            CAST(UTCUS  AS VARCHAR) AS UTCUS,
            CAST(UTCLC  AS VARCHAR) AS UTCLC
        FROM read_parquet('{k3_cache.as_posix()}')
    """).pl()
    con.close()

    rows = []
    part = "95"  # RETAIN PART '95'
    for r in raw.iter_rows(named=True):
        utref = r["UTREF"] or ""
        if utref[:1] != "I" or utref[:4] == "  ":
            continue

        utsty = r["UTSTY"]
        utamoc = r["UTAMOC"] or 0.0
        utdpf = r["UTDPF"] or 0.0
        amount = utamoc - utdpf
        if utsty == "IDC":
            amount = utamoc + utdpf

        if inst == "PBB":
            amtusd = amount if r["UTCCY"] == "USD" else 0.0
            amtsgd = amount if r["UTCCY"] == "SGD" else 0.0
        else:
            amtusd, amtsgd = 0.0, 0.0

        utdlp = r["UTDLP"]
        utaict = r["UTAICT"] or 0.0
        # MATDT is KEPT by the K3TBL DATA step but never assigned anywhere
        # in it -- it stays SAS-missing for every K3TBL row, which forces
        # REMMTH=0.1 for these rows downstream in build_ktbl(). Preserved.
        matdt = None

        def _emit(item, amt=amount):
            rows.append({"PART": part, "ITEM": item, "MATDT": matdt,
                         "AMOUNT": amt, "AMTUSD": amtusd, "AMTSGD": amtsgd})

        if utref in ("INV", "DRI", "DLG"):
            if utsty in ("CB1", "CB2", "CF1", "CF2", "CNT", "MGS", "MTB", "BNB", "BNN",
                         "SCD", "BMN", "BMC", "CMB", "MGI", "SMC", "BMF"):
                amt = amount + utaict if inst == "PBB" else amount
                _emit("631", amt)
            elif utsty in ("SLD", "SSD", "SFD", "SZD"):
                amt = amount + utaict if inst == "PBB" else amount
                _emit("632", amt)
            elif utsty == "SBA":
                if utdlp not in ("MOS", "MSS"):
                    _emit("633")
            elif utsty in ("ISB", "DHB", "KHA", "PNB"):
                _emit("636")
            elif utsty == "SIP":
                _emit("610")
            elif utsty == "DBD":
                # NOTE: 'DBD' also appears in the WHEN list below ('DMB',
                # 'DBD','GRL','MTL','RUL'); SAS SELECT/WHEN stops at the
                # first match, so that later WHEN clause is unreachable
                # for UTSTY='DBD' -- preserved via if/elif ordering.
                _emit("634")
            elif utsty in ("DMB", "DBD", "GRL", "MTL", "RUL"):
                _emit("635")
            elif utsty == "PBA":
                if utdlp in ("MOS", "MSS"):
                    _emit("850")

        elif utref in ("PFD", "PLD", "PSD", "PZD", "PDC"):
            if utsty in ("IFD", "ILD", "ISD", "IZD", "IDC"):
                _emit("840")

        elif utref in ("IINV", "IDRI", "IDLG"):
            if utsty == "SBA" and utdlp == "IOP":
                _emit("633")
            elif utsty in ("CB1", "CB2", "CF1", "CF2", "CNT", "MGI",
                           "BMN", "BMC", "BMF", "SCD",
                           "MGS", "MTB", "BNB", "BNN", "CMB", "SMC"):
                amt = amount + utaict if inst == "PBB" else amount
                _emit("631", amt)
            elif utsty in ("ISB", "DHB", "KHA"):
                _emit("636")
            elif utsty == "DBD":
                _emit("634")
            elif utsty == "SIP":
                _emit("610")

    return rows


# ============================================================================
# STEP C: KTBL -- PART 1/2 BNMCODE (K1TBL detail + K3TBL detail combined)
# ============================================================================
def build_ktbl(k1_rows: list, k3_rows: list, reptdate: date, rpyr: int, rpmth: int,
               rpday: int, rd_days: list) -> pl.DataFrame:
    out = []
    for r in k1_rows + k3_rows:
        item = r.get("ITEM")
        if item is None or item == "" or item == " ":
            continue

        matdt = r["MATDT"]
        if matdt is None or (matdt - reptdate).days < 8:
            remmth_val = 0.1
        else:
            remmth_val = remmth(matdt, rpyr, rpmth, rpday, rd_days)

        part = r["PART"]
        bnmcode = f"{part}{item}00{remfmt_format(remmth_val)}0000Y"
        out.append({"BNMCODE": bnmcode, "AMOUNT": r["AMOUNT"],
                    "AMTUSD": r["AMTUSD"], "AMTSGD": r["AMTSGD"]})

        # Duplicate row with PART substituted in BNMCODE: 95->93, else->94
        alt_prefix = "93" if part == "95" else "94"
        bnmcode2 = alt_prefix + bnmcode[2:]
        out.append({"BNMCODE": bnmcode2, "AMOUNT": r["AMOUNT"],
                    "AMTUSD": r["AMTUSD"], "AMTSGD": r["AMTSGD"]})

    if out:
        return pl.DataFrame(out)
    return pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64,
                                 "AMTUSD": pl.Float64, "AMTSGD": pl.Float64})


# ============================================================================
# STEP D: PART 3 distribution profile (rebuilt K1TBL / K3TBL, CAT/NAME/AMOUNT)
# ============================================================================
def build_distribution_profile(k1_cache: Path, k3_cache: Path) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    repos = con.execute(f"""
        SELECT
            'NON-INTERBANK REPOS' AS CAT,
            CAST(GWSHN AS VARCHAR) AS NAME,
            CAST(GWBALC AS DOUBLE) AS AMOUNT
        FROM read_parquet('{k1_cache.as_posix()}')
        WHERE GWCCY = 'MYR' AND GWMVT = 'P' AND GWMVTS = 'M'
          AND SUBSTR(GWCTP,1,1) != 'B'
          AND SUBSTR(GWDLP,2,2) IN ('MI','MT')
    """).pl()

    nids = con.execute(f"""
        SELECT
            'NON-INTERBANK NIDS' AS CAT,
            CAST(UTCUS AS VARCHAR) || CAST(UTCLC AS VARCHAR) AS NAME,
            CASE WHEN UTSTY = 'IDC'
                 THEN CAST(UTAMOC AS DOUBLE) + CAST(UTDPF AS DOUBLE)
                 ELSE CAST(UTAMOC AS DOUBLE) - CAST(UTDPF AS DOUBLE) END AS AMOUNT
        FROM read_parquet('{k3_cache.as_posix()}')
        WHERE SUBSTR(UTCTP,1,1) != 'B'
          AND UTREF IN ('PFD','PLD','PSD','PZD','PDC')
          AND UTSTY IN ('IFD','ILD','ISD','IZD','IDC')
    """).pl()
    con.close()

    combined = pl.concat([repos, nids])
    return combined.group_by(["CAT", "NAME"]).agg(pl.col("AMOUNT").sum())


# ============================================================================
# MAIN ENTRY POINT (called by EIBMRLFI.py after %INC-equivalent import)
# ============================================================================
def main(reptdate: date, rpyr: int, rpmth: int, rpday: int, rd_days: list,
         reptmon: str, nowk: str, inst: str = "PBB") -> dict:
    print("KALMLIQI: Caching KAPITI input datasets to Parquet...")
    # k1_sas = INPUT_K1TBL_DIR / f"k1tbl{reptmon}{nowk}.sas7bdat"       # Prod file
    # k3_sas = INPUT_K3TBL_DIR / f"k3tbl{reptmon}{nowk}.sas7bdat"       # Prod file
    k1_sas = INPUT_K1TBL_DIR / f"k1tbl091.sas7bdat"         # Test file
    k3_sas = INPUT_K3TBL_DIR / f"k3tbl091.sas7bdat"         # Test file
    k1_cache = _load_cached(k1_sas, "K1TBL")
    k3_cache = _load_cached(k3_sas, "K3TBL")

    print("KALMLIQI: Building K1TBL / K3TBL detail (PART 1/2)...")
    k1_rows = _build_k1tbl_detail(k1_cache)
    k3_rows = _build_k3tbl_detail(k3_cache, inst)
    print(f"  K1TBL detail rows: {len(k1_rows):,}   K3TBL detail rows: {len(k3_rows):,}")

    ktbl = build_ktbl(k1_rows, k3_rows, reptdate, rpyr, rpmth, rpday, rd_days)
    print(f"  KTBL rows (incl. PART-substituted duplicates): {len(ktbl):,}")

    print("KALMLIQI: Building distribution profile (PART 3)...")
    k1tbl_summary = build_distribution_profile(k1_cache, k3_cache)
    print(f"  K1TBL (distribution-profile) rows: {len(k1tbl_summary):,}")

    return {"ktbl": ktbl, "k1tbl_summary": k1tbl_summary}


if __name__ == "__main__":
    _reptdate_values = get_reptdate_values(year_format="%Y")
    _reptdate = _reptdate_values.reptdate
    _day = _reptdate.day
    _nowk = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"
    _reptmon = f"{_reptdate.month:02d}"
    _rd_days = build_rd_days(_reptdate.year)

    _result = main(
        reptdate=_reptdate, rpyr=_reptdate.year, rpmth=_reptdate.month,
        rpday=_reptdate.day, rd_days=_rd_days, reptmon=_reptmon, nowk=_nowk,
    )
    print("\nKTBL sample:")
    print(_result["ktbl"].head(10))
    print("\nK1TBL (distribution profile) sample:")
    print(_result["k1tbl_summary"].head(10))
