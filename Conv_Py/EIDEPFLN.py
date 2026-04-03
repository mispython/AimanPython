from pathlib import Path
from datetime import date
import polars as pl

# ---------- I/O roots ----------
BASE_IN  = Path("input_parquet")
BASE_OUT = Path("output_parquet")
BASE_OUT.mkdir(parents=True, exist_ok=True)

def write_tbl(df: pl.DataFrame, lib: str, name: str):
    outdir = BASE_OUT / lib
    outdir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(outdir / f"{name}.parquet")

# SAS @pos fixed-width slicer (1-based)
def fw_slice(expr: pl.Expr, pos1: int, width: int) -> pl.Expr:
    return expr.str.slice(pos1 - 1, width)

# EPF.REPTDATE
today_py = date.today()
EPF_REPTDATE = pl.DataFrame({"REPTDATE": [today_py]})
write_tbl(EPF_REPTDATE, "EPF", "REPTDATE")

# &REPTDAY as SAS Z2. (01-31)
REPTDAY = f"{today_py.day:02d}"

FVAL_RAW = pl.read_parquet(BASE_IN / "FVAL" / "FVAL.parquet")

EPFVAL = (
    FVAL_RAW
    .with_row_count(name="__rownum", offset=1)  # to emulate _N_ starting at 1
    .with_columns([
        fw_slice(pl.col("RAW"), 1,   2).str.strip().cast(pl.Int64, strict=False).alias("RECORDTY"),
        fw_slice(pl.col("RAW"), 3,  40).alias("MEMNAME1"),
        fw_slice(pl.col("RAW"), 43, 40).alias("MEMNAME2"),
        fw_slice(pl.col("RAW"), 83, 20).alias("ICNO"),
        fw_slice(pl.col("RAW"),103, 19).str.strip().cast(pl.Int64, strict=False).alias("EPFNO"),
        fw_slice(pl.col("RAW"),122, 19).str.strip().cast(pl.Int64, strict=False).alias("WITHTRAN"),
        fw_slice(pl.col("RAW"),141, 20).alias("ACCTNO20"),
        fw_slice(pl.col("RAW"),141, 10).str.strip().cast(pl.Int64, strict=False).alias("ACCTNO"),
        fw_slice(pl.col("RAW"),151,  5).str.strip().cast(pl.Int64, strict=False).alias("NOTENO"),
        fw_slice(pl.col("RAW"),161, 30).alias("LNREFNO20"),
        fw_slice(pl.col("RAW"),191, 10).alias("WITHSCHEME"),
        fw_slice(pl.col("RAW"),201, 20).alias("OLDICNO"),
        fw_slice(pl.col("RAW"),221, 20).alias("OTHICNO"),
        fw_slice(pl.col("RAW"),241,  2).alias("REJCODE"),
        pl.col("__rownum").alias("SEQID"),
    ])
    .drop("__rownum")
)

# NODUPKEY BY ACCTNO NOTENO (keep first)
EPFVAL = EPFVAL.sort(["ACCTNO", "NOTENO"]).unique(subset=["ACCTNO", "NOTENO"], keep="first")

# Write both work table and the daily libref table like SAS
write_tbl(EPFVAL, "EPF", f"EPFVAL{REPTDAY}")
write_tbl(EPFVAL, "", "EPFVAL")  # optional "work-like" copy (root)

CISLN_LOAN = pl.read_parquet(BASE_IN / "CISLN" / "LOAN.parquet")
CISINFO = (
    CISLN_LOAN
    .select(["CUSTNAME","ACCTNO","NEWIC","NEWICIND","OLDIC","SECCUST"])
    .sort(["ACCTNO","SECCUST"])
)
write_tbl(CISINFO, "", "CISINFO")

TEMP = (
    EPFVAL.join(
        CISINFO,
        on="ACCTNO",
        how="left",
    )
    .select([
        "ACCTNO20","ACCTNO","NOTENO","SEQID",
        "CUSTNAME","NEWIC","NEWICIND","OLDIC","SECCUST"
    ])
)
write_tbl(TEMP, "", "TEMP")

TEMP2 = TEMP.with_columns(
    pl.when(pl.col("NEWICIND").is_in(["PP","PL","ML"]))
      .then(pl.col("NEWIC"))
      .otherwise(pl.lit(None))
      .alias("OTHERID")
)

EPFCIS = TEMP2.filter(pl.col("SECCUST") == "901").sort(["ACCTNO","NOTENO"])
SECCIS  = TEMP2.filter(pl.col("SECCUST") != "901").sort(["ACCTNO","NOTENO"])

write_tbl(EPFCIS, "", "EPFCIS")
write_tbl(SECCIS,  "", "SECCIS")

# assign per-group row number (1,2,...) by (ACCTNO, NOTENO)
SECCIS_K = SECCIS.select(["ACCTNO","NOTENO","CUSTNAME","NEWIC","OLDIC","OTHERID"])
SECCIS_K = (
    SECCIS_K
    .group_by(["ACCTNO","NOTENO"])
    .agg([
        pl.all(),  # collect columns
        pl.len().alias("__len")
    ])
    .explode(["CUSTNAME","NEWIC","OLDIC","OTHERID"])  # undo collect
    .with_columns([
        pl.int_range(1, pl.col("__len")+1).over(["ACCTNO","NOTENO"]).alias("__seq")
    ])
    .drop("__len")
)

SECCIS1 = (
    SECCIS_K.filter(pl.col("__seq") == 1)
    .rename({"CUSTNAME":"CUSTNAME2","NEWIC":"NEWIC2","OLDIC":"OLDIC2","OTHERID":"OTHERID2"})
    .sort(["ACCTNO","NOTENO"])
    .drop("__seq")
)

SECCIS2 = (
    SECCIS_K.filter(pl.col("__seq") == 2)
    .rename({"CUSTNAME":"CUSTNAME3","NEWIC":"NEWIC3","OLDIC":"OLDIC3","OTHERID":"OTHERID3"})
    .sort(["ACCTNO","NOTENO"])
    .drop("__seq")
)

write_tbl(SECCIS1, "", "SECCIS1")
write_tbl(SECCIS2, "", "SECCIS2")

EPFCIS_M = (
    EPFCIS
    .join(SECCIS1, on=["ACCTNO","NOTENO"], how="left")
    .join(SECCIS2, on=["ACCTNO","NOTENO"], how="left")
    .sort(["ACCTNO","NOTENO"])
)
write_tbl(EPFCIS_M, "", "EPFCIS_M")

LN_LNNOTE  = pl.read_parquet(BASE_IN / "LN"  / "LNNOTE.parquet").select(["ACCTNO","NOTENO","VINNO","ISSXDTE"])
ILN_LNNOTE = pl.read_parquet(BASE_IN / "ILN" / "LNNOTE.parquet").select(["ACCTNO","NOTENO","VINNO","ISSXDTE"])

ELDSSAS = (
    pl.concat([LN_LNNOTE, ILN_LNNOTE], how="vertical_relaxed")
    .rename({"VINNO":"AANO"})
    .sort("AANO")
)

ELDS = (
    pl.read_parquet(BASE_IN / "ELDS" / "ELBNMAX.parquet")
    .select(["AANO","APVDTE1","AMOUNT","APPAMTSC","SPADT"])
    .sort("AANO")
    .unique(subset=["AANO"], keep="first")   # NODUPKEY BY AANO
)

write_tbl(ELDSSAS, "", "ELDSSAS")
write_tbl(ELDS,    "", "ELDS")

ELDSSAS2 = (
    ELDSSAS.join(ELDS, on="AANO", how="left")
    .with_columns([
        pl.when(pl.col("APVDTE1").is_null() | (pl.col("APVDTE1") == 0))
          .then(pl.col("ISSXDTE"))
          .otherwise(pl.col("APVDTE1"))
          .alias("APVDTE1"),
        pl.when(pl.col("SPADT").is_null() | (pl.col("SPADT") == 0))
          .then(pl.col("APVDTE1"))
          .otherwise(pl.col("SPADT"))
          .alias("SPADT"),
        pl.when(~(pl.col("APPAMTSC").is_null() | (pl.col("APPAMTSC") == 0)))
          .then(pl.col("APPAMTSC"))
          .otherwise(pl.col("AMOUNT"))
          .alias("AMOUNT"),
    ])
    .sort(["ACCTNO","NOTENO"])
)

write_tbl(ELDSSAS2, "", "ELDSSAS2")

EPFCIS_FINAL = (
    EPFCIS_M.join(ELDSSAS2, on=["ACCTNO","NOTENO"], how="left")
    .with_columns(
        pl.when(pl.col("AMOUNT").is_null() | (pl.col("AMOUNT") == 0))
          .then(pl.lit(0))
          .otherwise(pl.col("AMOUNT"))
          .alias("AMOUNT")
    )
    .sort("SEQID")
)

write_tbl(EPFCIS_FINAL, "EPF", f"EPFOUT{REPTDAY}")   # final daily output (Parquet)

# Optional: create the *text* file in addition to Parquet (to mirror SAS PUT)
out_txt = BASE_OUT / "EPF" / f"EPFOUT{REPTDAY}.txt"

def fmt_yymmdd8(val):
    # Assuming APVDTE1/SPADT are integers like SAS date or already yyyymmdd numbers.
    # If they’re SAS dates (days since 1960), convert first. Here we assume they’re YYYYMMDD ints.
    if val is None:
        return "00000000"
    s = str(int(val))
    return s.zfill(8)[:8]

with out_txt.open("w", encoding="utf-8", newline="") as f:
    for row in EPFCIS_FINAL.iter_rows(named=True):
        line = (
            f"{str(row.get('ACCTNO20','') or ''):<20}"        # @001 $20.
            f"{str(row.get('CUSTNAME','') or ''):<80}"        # @021 $80.
            f"{str(row.get('NEWIC','') or ''):<15}"           # @101 $15.
            f"{str(row.get('OLDIC','') or ''):<15}"           # @116 $15.
            f"{str(row.get('OTHERID','') or ''):<20}"         # @131 $20.
            f"{str(row.get('CUSTNAME2','') or ''):<80}"       # @151 $80.
            f"{str(row.get('NEWIC2','') or ''):<15}"          # @231 $15.
            f"{str(row.get('OLDIC2','') or ''):<15}"          # @246 $15.
            f"{str(row.get('OTHERID2','') or ''):<20}"        # @261 $20.
            f"{str(row.get('CUSTNAME3','') or ''):<80}"       # @281 $80.
            f"{str(row.get('NEWIC3','') or ''):<15}"          # @361 $15.
            f"{str(row.get('OLDIC3','') or ''):<15}"          # @376 $15.
            f"{str(row.get('OTHERID3','') or ''):<20}"        # @391 $20.
            f"{fmt_yymmdd8(row.get('APVDTE1'))}"              # @411 YYMMDDN8.
            f"{fmt_yymmdd8(row.get('SPADT'))}"                # @419 YYMMDDN8.
            f"{format(row.get('AMOUNT',0) or 0, '17.2')}"     # @427 17.2
        )
        f.write(line + "\n")
