
from pathlib import Path
import polars as pl

# ---------------- paths ----------------
BASE_IN  = Path("input_parquet")
BASE_OUT = Path("output_parquet")
(BASE_OUT / "DEPOSIT").mkdir(parents=True, exist_ok=True)

PATHS = {
    "DPTRBL1":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT01.parquet",
    "DPTRBL2":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT02.parquet",
    "DPTRBL3":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT03.parquet",
    "DPTRBL4":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT04.parquet",
    "DPTRBL5":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT05.parquet",
    "DPTRBL6":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT06.parquet",
    "DPTRBL7":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT07.parquet",
    "DPTRBL8":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT08.parquet",
    "DPTRBL9":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT09.parquet",
    "DPTRBL10": BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT10.parquet",
    "DPTRBORG": BASE_IN / "BNMCTR" / "CISWEEK" / "ORG" / "OTH.parquet",
    "DPTRB999": BASE_IN / "BNMCTR" / "CISWEEK" / "ORG" / "SA9.parquet",
}

def rd(name: str) -> pl.DataFrame:
    return pl.read_parquet(PATHS[name])

# ---------------- helpers (keep SAS names/logic) ----------------
def load_dep(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply SAS ICNO rule:
      IF ICNOX='  ' OR SUBSTR(ICNOX,4,5) in ('99999','DUPLI') THEN ICNO=CUSTNO; ELSE ICNO=ICNOX;
    Then sort by ACCTNO ICNO (for print parity).
    """
    return (
        df.with_columns([
            pl.when(
                (pl.col("ICNOX") == "  ") |
                (pl.col("ICNOX").str.slice(3, 5).is_in(["99999", "DUPLI"]))
            ).then(pl.col("CUSTNO")).otherwise(pl.col("ICNOX")).alias("ICNO")
        ])
        .sort(["ACCTNO", "ICNO"])
    )

def make_trans(df_dep: pl.DataFrame, n: int, join_val: int) -> pl.DataFrame:
    """
    Emulates:
      - PROC TRANSPOSE of ICNO → COL1..COLn
      - PROC TRANSPOSE of NAME → COLN1..COLNn (explicit rename to that width)
      - MERGE with deduped DEPn BY ACCTNO
      - Build KEY (COMPRESS of COLi joined by ',')
      - Build KEYNAME (COMPBL of COLNi joined by ',')
      - JOIN = n
    """
    # Wide ICNO
    icno_wide = (
        df_dep.group_by("ACCTNO")
        .agg([pl.col("ICNO").nth(i).alias(f"COL{i+1}") for i in range(n)])
    )
    # Wide NAME
    name_wide = (
        df_dep.group_by("ACCTNO")
        .agg([pl.col("NAME").nth(i).alias(f"COLN{i+1}") for i in range(n)])
    )
    trans = icno_wide.join(name_wide, on="ACCTNO", how="inner")

    # Dedup like NODUPKEYS
    base = df_dep.unique(subset=["ACCTNO"], keep="first")

    trans = trans.join(base, on="ACCTNO", how="left")

    icno_cols = [f"COL{i+1}" for i in range(n)]
    name_cols = [f"COLN{i+1}" for i in range(n)]
    return (
        trans.with_columns([
            pl.concat_str(
                *[pl.col(c).fill_null("").cast(pl.Utf8).str.replace_all(" ", "") for c in icno_cols],
                separator=","
            ).alias("KEY"),
            pl.concat_str(
                *[pl.col(c).fill_null("").cast(pl.Utf8).str.replace_all(r"\s+", " ") for c in name_cols],
                separator=","
            ).alias("KEYNAME"),
            pl.lit(join_val).alias("JOIN")
        ])
    )

def make_single(df: pl.DataFrame, join_val: int) -> pl.DataFrame:
    """
    For TRANS01 / TRANORG / TRAN999:
      - derive ICNO
      - KEY = COMPRESS(ICNO)
      - KEYNAME = COMPBL(NAME)
      - JOIN = join_val
      - NODUPKEYS BY ACCTNO
    """
    df2 = load_dep(df)
    df2 = df2.with_columns([
        pl.col("ICNO").fill_null("").cast(pl.Utf8).str.replace_all(" ", "").alias("KEY"),
        pl.col("NAME").fill_null("").cast(pl.Utf8).str.replace_all(r"\s+", " ").alias("KEYNAME"),
        pl.lit(join_val).alias("JOIN"),
    ])
    return df2.unique(subset=["ACCTNO"], keep="first")

# ---------------- pipeline ----------------
# Load and prepare each DEPn
DEP2  = load_dep(rd("DPTRBL2"))
DEP3  = load_dep(rd("DPTRBL3"))
DEP4  = load_dep(rd("DPTRBL4"))
DEP5  = load_dep(rd("DPTRBL5"))
DEP6  = load_dep(rd("DPTRBL6"))
DEP7  = load_dep(rd("DPTRBL7"))
DEP8  = load_dep(rd("DPTRBL8"))
DEP9  = load_dep(rd("DPTRBL9"))   # DEP9 is read/printed in SAS but not included in final SET
DEP10 = load_dep(rd("DPTRBL10"))  # DEP10 is read/printed in SAS but not included in final SET

# Build TRANS02..TRANS08 exactly to stated widths
TRANS02 = make_trans(DEP2,  2, join_val=2)
TRANS03 = make_trans(DEP3,  3, join_val=3)
TRANS04 = make_trans(DEP4,  4, join_val=4)
TRANS05 = make_trans(DEP5,  5, join_val=5)
TRANS06 = make_trans(DEP6,  6, join_val=6)
TRANS07 = make_trans(DEP7,  7, join_val=7)
TRANS08 = make_trans(DEP8,  8, join_val=8)

# Single-party & org streams
TRANS01 = make_single(rd("DPTRBL1"), join_val=1)
TRANORG = make_single(rd("DPTRBORG"), join_val=0)
TRAN999 = make_single(rd("DPTRB999"), join_val=1)

# Final SET (SAS drops _NAME_, which we never created during wide ops)
combined = pl.concat(
    [TRANS02, TRANS03, TRANS04, TRANS05, TRANS06, TRANS07, TRANS08, TRANS01, TRANORG, TRAN999],
    how="vertical_relaxed"
)

# NODUPKEYS BY ACCTNO and sort
CISDEPWK = combined.unique(subset=["ACCTNO"], keep="first").sort("ACCTNO")

# Write Parquet (DEPOSIT.CISDEPWK)
out_path = BASE_OUT / "DEPOSIT" / "CISDEPWK.parquet"
CISDEPWK.write_parquet(out_path)

print(f"Wrote: {out_path}")
