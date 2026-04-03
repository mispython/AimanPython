# !/usr/bin/env python3
"""
Program  : EIDEPFLN
ESMR     : 2016-4001 (AAB)
Purpose  : Auto-map over the borrower's credit and collateral info upon
           receipt of the batch file from EPF to reduce the outstanding
           balance with the bank under the E-Withdrawal HL Scheme.
"""

from pathlib import Path
from datetime import date, timedelta
import polars as pl

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
BASE_FLAT = Path("input_flat")      # fixed-width .txt flat file inputs
BASE_IN   = Path("input_parquet")   # SAS dataset → Parquet inputs
BASE_OUT  = Path("output_parquet")  # Parquet output root
TXT_OUT   = Path("output_txt")      # fixed-width text file outputs

EPF_DIR  = BASE_OUT / "EPF"
TXT_OUT.mkdir(parents=True, exist_ok=True)
EPF_DIR.mkdir(parents=True, exist_ok=True)

FVAL_TXT = BASE_FLAT / "FVAL" / "FVAL.txt"


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def write_tbl(df: pl.DataFrame, lib: str, name: str) -> None:
    outdir = BASE_OUT / lib
    outdir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(outdir / f"{name}.parquet")


def sas_date_to_yyyymmdd(val) -> str:
    """
    Convert a SAS date integer (days since 1960-01-01) to YYYYMMDD string.
    Mirrors SAS YYMMDDN8. format (4-digit year, no separator, 8 chars).
    Returns '        ' (8 spaces) for null/zero values, matching SAS missing.
    """
    if val is None:
        return "        "
    ival = int(val)
    if ival == 0:
        return "        "
    d = date(1960, 1, 1) + timedelta(days=ival)
    return d.strftime("%Y%m%d")


# --------------------------------------------------------------------------
# DATA EPF.REPTDATE;
#    REPTDATE = TODAY();
#    CALL SYMPUT('REPTDAY', PUT(DAY(REPTDATE), Z2.));
# --------------------------------------------------------------------------
today_py = date.today()
EPF_REPTDATE = pl.DataFrame({"REPTDATE": [today_py]})
write_tbl(EPF_REPTDATE, "EPF", "REPTDATE")

# &REPTDAY as SAS Z2. (01-31)
REPTDAY = f"{today_py.day:02d}"


# --------------------------------------------------------------------------
# DATA EPFVAL  EPF.EPFVAL&REPTDAY;
#   INFILE FVAL;
#   INPUT @001  RECORDTY          2.
#         @003  MEMNAME1         $40.
#         @043  MEMNAME2         $40.
#         @083  ICNO             $20.
#         @103  EPFNO             19.
#         @122  WITHTRAN          19.
#         @141  ACCTNO20         $20.
#         @141  ACCTNO            10.
#         @151  NOTENO             5.
#         @161  LNREFNO20        $30.
#         @191  WITHSCHEME       $10.
#         @201  OLDICNO          $20.
#         @221  OTHICNO          $20.
#         @241  REJCODE           $2.
#         ;
#   SEQID = _N_;
#
# FVAL is a mainframe flat file — read as fixed-width .txt.
# All positions below are 1-based (SAS @pos → Python [pos-1 : pos-1+width]).
# --------------------------------------------------------------------------
raw_lines = FVAL_TXT.read_bytes().decode("latin-1").splitlines()

records = []
for seqid, line in enumerate(raw_lines, start=1):
    rec = line.ljust(242)   # minimum width to reach @241 + 2 chars = 242

    def _num(s):
        s = s.strip()
        return int(s) if s.lstrip("-").isdigit() else None

    records.append({
        "RECORDTY":   _num(rec[0:2]),           # @001  2.
        "MEMNAME1":   rec[2:42],                # @003  $40.
        "MEMNAME2":   rec[42:82],               # @043  $40.
        "ICNO":       rec[82:102],              # @083  $20.
        "EPFNO":      _num(rec[102:121]),       # @103  19.
        "WITHTRAN":   _num(rec[121:140]),       # @122  19.
        "ACCTNO20":   rec[140:160],             # @141  $20.
        "ACCTNO":     _num(rec[140:150]),       # @141  10.
        "NOTENO":     _num(rec[150:155]),       # @151  5.
        "LNREFNO20":  rec[160:190],             # @161  $30.
        "WITHSCHEME": rec[190:200],             # @191  $10.
        "OLDICNO":    rec[200:220],             # @201  $20.
        "OTHICNO":    rec[220:240],             # @221  $20.
        "REJCODE":    rec[240:242],             # @241  $2.
        "SEQID":      seqid,                    # _N_
    })

EPFVAL = pl.DataFrame(
    records,
    schema={
        "RECORDTY":   pl.Int64,
        "MEMNAME1":   pl.Utf8,
        "MEMNAME2":   pl.Utf8,
        "ICNO":       pl.Utf8,
        "EPFNO":      pl.Int64,
        "WITHTRAN":   pl.Int64,
        "ACCTNO20":   pl.Utf8,
        "ACCTNO":     pl.Int64,
        "NOTENO":     pl.Int64,
        "LNREFNO20":  pl.Utf8,
        "WITHSCHEME": pl.Utf8,
        "OLDICNO":    pl.Utf8,
        "OTHICNO":    pl.Utf8,
        "REJCODE":    pl.Utf8,
        "SEQID":      pl.Int64,
    },
)

# PROC SORT DATA=EPFVAL NODUPKEY; BY ACCTNO NOTENO;
EPFVAL = (
    EPFVAL
    .sort(["ACCTNO", "NOTENO"])
    .unique(subset=["ACCTNO", "NOTENO"], keep="first")
)

write_tbl(EPFVAL, "EPF", f"EPFVAL{REPTDAY}")


# --------------------------------------------------------------------------
# %LET CVAR=(KEEP=CUSTNAME ACCTNO NEWIC NEWICIND OLDIC SECCUST);
# PROC SORT DATA=CISLN.LOAN OUT=CISINFO &CVAR; BY ACCTNO SECCUST;
# --------------------------------------------------------------------------
CISINFO = (
    pl.read_parquet(BASE_IN / "CISLN" / "LOAN.parquet")
    .select(["CUSTNAME", "ACCTNO", "NEWIC", "NEWICIND", "OLDIC", "SECCUST"])
    # no pre-sort needed; Polars join does not require sorted inputs
)


# --------------------------------------------------------------------------
# PROC SQL;
#   CREATE TABLE TEMP AS
#   SELECT T1.ACCTNO20, T1.ACCTNO, T1.NOTENO, T1.SEQID, T2.CUSTNAME,
#          T2.NEWIC, T2.NEWICIND, T2.OLDIC, T2.SECCUST
#   FROM EPFVAL T1 LEFT JOIN CISINFO T2 ON (T1.ACCTNO=T2.ACCTNO);
# --------------------------------------------------------------------------
TEMP = (
    EPFVAL
    .join(CISINFO, on="ACCTNO", how="left")
    .select([
        "ACCTNO20", "ACCTNO", "NOTENO", "SEQID",
        "CUSTNAME", "NEWIC", "NEWICIND", "OLDIC", "SECCUST",
    ])
)


# --------------------------------------------------------------------------
# DATA EPFCIS SECCIS;
#   SET TEMP;
#   IF NEWICIND IN ('PP','PL','ML') THEN OTHERID = NEWIC;
#   IF SECCUST = '901' THEN OUTPUT EPFCIS;
#   ELSE                    OUTPUT SECCIS;
# --------------------------------------------------------------------------
TEMP = TEMP.with_columns(
    pl.when(pl.col("NEWICIND").is_in(["PP", "PL", "ML"]))
      .then(pl.col("NEWIC"))
      .otherwise(pl.lit(None))
      .alias("OTHERID")
)

EPFCIS = TEMP.filter(pl.col("SECCUST") == "901")
SECCIS  = TEMP.filter(pl.col("SECCUST") != "901")


# --------------------------------------------------------------------------
# DATA SECCIS1 / SECCIS2
#   SET SECCIS(KEEP=ACCTNO NOTENO CUSTNAME NEWIC OLDIC OTHERID);
#   BY ACCTNO NOTENO;
#   RETAIN SEQNO; IF FIRST.ACCTNO OR FIRST.NOTENO THEN SEQNO=1; ELSE SEQNO+1;
#   IF SEQNO <= 2;
#   IF FIRST.ACCTNO OR FIRST.NOTENO THEN OUTPUT SECCIS1;
#   ELSE OUTPUT SECCIS2;
#
# Within each (ACCTNO, NOTENO) group, EPF requires up to 2 secondary
# borrowers.  Row 1 → SECCIS1 (renamed _2 suffix), Row 2 → SECCIS2 (_3).
# --------------------------------------------------------------------------
SECCIS_K = (
    SECCIS
    .select(["ACCTNO", "NOTENO", "CUSTNAME", "NEWIC", "OLDIC", "OTHERID"])
    .with_columns(
        pl.int_range(pl.len(), dtype=pl.Int64)
          .over(["ACCTNO", "NOTENO"])
          .alias("__seq")   # 0-based rank within group → seq 0=first, 1=second
    )
    .filter(pl.col("__seq") <= 1)   # keep only rows 0 and 1 (up to 2 per group)
)

SECCIS1 = (
    SECCIS_K
    .filter(pl.col("__seq") == 0)
    .drop("__seq")
    .rename({"CUSTNAME": "CUSTNAME2", "NEWIC": "NEWIC2",
             "OLDIC": "OLDIC2", "OTHERID": "OTHERID2"})
)

SECCIS2 = (
    SECCIS_K
    .filter(pl.col("__seq") == 1)
    .drop("__seq")
    .rename({"CUSTNAME": "CUSTNAME3", "NEWIC": "NEWIC3",
             "OLDIC": "OLDIC3", "OTHERID": "OTHERID3"})
)


# --------------------------------------------------------------------------
# DATA EPFCIS;
#   MERGE EPFCIS(IN=A) SECCIS1 SECCIS2;
#   BY ACCTNO NOTENO;
#   IF A;
# --------------------------------------------------------------------------
EPFCIS = (
    EPFCIS
    .join(SECCIS1, on=["ACCTNO", "NOTENO"], how="left")
    .join(SECCIS2, on=["ACCTNO", "NOTENO"], how="left")
)


# --------------------------------------------------------------------------
# DATA ELDSSAS;
#   SET LN.LNNOTE(KEEP=ACCTNO NOTENO VINNO ISSXDTE)
#       ILN.LNNOTE(KEEP=ACCTNO NOTENO VINNO ISSXDTE);
#   RENAME VINNO=AANO;
# --------------------------------------------------------------------------
LN_LNNOTE  = (
    pl.read_parquet(BASE_IN / "LN" / "LNNOTE.parquet")
    .select(["ACCTNO", "NOTENO", "VINNO", "ISSXDTE"])
)
ILN_LNNOTE = (
    pl.read_parquet(BASE_IN / "ILN" / "LNNOTE.parquet")
    .select(["ACCTNO", "NOTENO", "VINNO", "ISSXDTE"])
)

ELDSSAS = (
    pl.concat([LN_LNNOTE, ILN_LNNOTE], how="vertical_relaxed")
    .rename({"VINNO": "AANO"})
)


# --------------------------------------------------------------------------
# PROC SORT DATA=ELDS.ELBNMAX(KEEP=AANO APVDTE1 AMOUNT APPAMTSC SPADT)
#           OUT=ELDS NODUPKEY; BY AANO;
# --------------------------------------------------------------------------
ELDS = (
    pl.read_parquet(BASE_IN / "ELDS" / "ELBNMAX.parquet")
    .select(["AANO", "APVDTE1", "AMOUNT", "APPAMTSC", "SPADT"])
    .sort("AANO")
    .unique(subset=["AANO"], keep="first")
)


# --------------------------------------------------------------------------
# DATA ELDSSAS;
#   MERGE ELDSSAS(IN=A) ELDS;
#   BY AANO;
#   IF A;
#   IF APVDTE1 IN (.,0) THEN APVDTE1 = ISSXDTE;
#   IF SPADT   IN (.,0) THEN SPADT   = APVDTE1;   <- uses updated APVDTE1
#   IF APPAMTSC NOT IN (.,0) THEN AMOUNT = APPAMTSC;
#
# NOTE: APVDTE1 fallback must be applied BEFORE the SPADT fallback because
# SPADT uses the already-updated value of APVDTE1.  Two sequential
# with_columns() calls are required to preserve this dependency.
# --------------------------------------------------------------------------
ELDSSAS = (
    ELDSSAS
    .join(ELDS, on="AANO", how="left")
    # Step 1: update APVDTE1 first
    .with_columns(
        pl.when(pl.col("APVDTE1").is_null() | (pl.col("APVDTE1") == 0))
          .then(pl.col("ISSXDTE"))
          .otherwise(pl.col("APVDTE1"))
          .alias("APVDTE1")
    )
    # Step 2: SPADT fallback uses the updated APVDTE1 from step 1
    .with_columns(
        pl.when(pl.col("SPADT").is_null() | (pl.col("SPADT") == 0))
          .then(pl.col("APVDTE1"))
          .otherwise(pl.col("SPADT"))
          .alias("SPADT")
    )
    # Step 3: AMOUNT override
    .with_columns(
        pl.when(
            ~(pl.col("APPAMTSC").is_null() | (pl.col("APPAMTSC") == 0))
        )
          .then(pl.col("APPAMTSC"))
          .otherwise(pl.col("AMOUNT"))
          .alias("AMOUNT")
    )
)


# --------------------------------------------------------------------------
# DATA EPF.EPFOUT&REPTDAY;
#   MERGE EPFCIS(IN=A) ELDSSAS;
#   BY ACCTNO NOTENO;
#   IF A;
#   IF AMOUNT IN (.,0) THEN AMOUNT = 0;
# --------------------------------------------------------------------------
EPFCIS_FINAL = (
    EPFCIS
    .join(ELDSSAS.select(["ACCTNO", "NOTENO", "AANO",
                          "APVDTE1", "SPADT", "AMOUNT"]),
          on=["ACCTNO", "NOTENO"], how="left")
    .with_columns(
        pl.when(pl.col("AMOUNT").is_null() | (pl.col("AMOUNT") == 0))
          .then(pl.lit(0.0))
          .otherwise(pl.col("AMOUNT"))
          .alias("AMOUNT")
    )
    .sort("SEQID")
)

write_tbl(EPFCIS_FINAL, "EPF", f"EPFOUT{REPTDAY}")


# --------------------------------------------------------------------------
# FILE OUTPUT / PUT — fixed-width text output
#
# PUT @001  ACCTNO20       $20.     → positions   1-20   (20 chars)
#     @021  CUSTNAME       $80.     → positions  21-100  (80 chars)
#     @101  NEWIC          $15.     → positions 101-115  (15 chars)
#     @116  OLDIC          $15.     → positions 116-130  (15 chars)
#     @131  OTHERID        $20.     → positions 131-150  (20 chars)
#     @151  CUSTNAME2      $80.     → positions 151-230  (80 chars)
#     @231  NEWIC2         $15.     → positions 231-245  (15 chars)
#     @246  OLDIC2         $15.     → positions 246-260  (15 chars)
#     @261  OTHERID2       $20.     → positions 261-280  (20 chars)
#     @281  CUSTNAME3      $80.     → positions 281-360  (80 chars)
#     @361  NEWIC3         $15.     → positions 361-375  (15 chars)
#     @376  OLDIC3         $15.     → positions 376-390  (15 chars)
#     @391  OTHERID3       $20.     → positions 391-410  (20 chars)
#     @411  APVDTE1   YYMMDDN8.    → positions 411-418  (8 chars, YYYYMMDD)
#     @419  SPADT     YYMMDDN8.    → positions 419-426  (8 chars, YYYYMMDD)
#     @427  AMOUNT          17.2   → positions 427-443  (17 chars, 2 dp)
#
# YYMMDDN8. = YYYYMMDD (4-digit year, no separator, 8 chars).
# APVDTE1/SPADT are SAS date integers (days since 1960-01-01).
# --------------------------------------------------------------------------
out_txt = TXT_OUT / f"EPFOUT{REPTDAY}.txt"

with out_txt.open("w", encoding="utf-8", newline="") as f:
    for row in EPFCIS_FINAL.iter_rows(named=True):

        def _s(key, width):
            return str(row.get(key) or "").ljust(width)[:width]

        line = (
            _s("ACCTNO20",  20)                        # @001  $20.
            + _s("CUSTNAME",   80)                     # @021  $80.
            + _s("NEWIC",      15)                     # @101  $15.
            + _s("OLDIC",      15)                     # @116  $15.
            + _s("OTHERID",    20)                     # @131  $20.
            + _s("CUSTNAME2",  80)                     # @151  $80.
            + _s("NEWIC2",     15)                     # @231  $15.
            + _s("OLDIC2",     15)                     # @246  $15.
            + _s("OTHERID2",   20)                     # @261  $20.
            + _s("CUSTNAME3",  80)                     # @281  $80.
            + _s("NEWIC3",     15)                     # @361  $15.
            + _s("OLDIC3",     15)                     # @376  $15.
            + _s("OTHERID3",   20)                     # @391  $20.
            + sas_date_to_yyyymmdd(row.get("APVDTE1")) # @411  YYMMDDN8.
            + sas_date_to_yyyymmdd(row.get("SPADT"))   # @419  YYMMDDN8.
            + f"{row.get('AMOUNT', 0) or 0:17.2f}"    # @427  17.2
        )
        f.write(line + "\n")
