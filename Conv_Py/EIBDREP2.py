from __future__ import annotations
from pathlib import Path
from datetime import date, timedelta
from calendar import monthrange
import polars as pl

# =====================================================
# I/O roots (adjust as needed)
# =====================================================
BASE_IN  = Path("input_parquet")
BASE_OUT = Path("output_parquet")
(BASE_OUT / "COMBO").mkdir(parents=True, exist_ok=True)

# Monthly sources: expect REPOLN<MM>.parquet inside these
LOAN_DIR  = BASE_IN / "LOAN"   # current month repo (REPOLN<MM>.parquet)
LOANP_DIR = BASE_IN / "LOANP"  # previous gen
LOANQ_DIR = BASE_IN / "LOANQ"  # two-gens back

# =====================================================
# Helpers (SAS date compatibility and formats)
# =====================================================
SAS_EPOCH = date(1960, 1, 1)

def _sas_days(d: date) -> int:
    return (d - SAS_EPOCH).days

def _yymmdd6_from_sas_days(sas_days: int) -> str:
    # Convert SAS numeric date -> YYMMDD (6 chars) string
    dt = SAS_EPOCH + timedelta(days=int(sas_days))
    return f"{dt.year % 100:02d}{dt.month:02d}{dt.day:02d}"

def _shift_month_end(d: date, k: int) -> date:
    # Equivalent to INTNX('MONTH', d, k, 'E'): last day of the month after shifting k months
    y = d.year + ((d.month - 1 + k) // 12)
    m = (d.month - 1 + k) % 12 + 1
    last_day = monthrange(y, m)[1]
    return date(y, m, last_day)

def _to_sas_days_expr(col: str) -> pl.Expr:
    # Normalize a date/int column to SAS-days int
    return (
        pl.when(pl.col(col).is_dtype(pl.Date))
          .then((pl.col(col).cast(pl.Date) - pl.lit(SAS_EPOCH)).dt.days())
          .otherwise(pl.col(col).cast(pl.Int64))
    )

# =====================================================
# 1) OPTIONS + REPTDATE + "macros"
# =====================================================
T_REPTDATE = pl.read_parquet(BASE_IN / "LN" / "REPTDATE.parquet")
REPTDATE_DT = T_REPTDATE.select(pl.col("REPTDATE").cast(pl.Date)).to_series().item()

REPTYEAR = f"{REPTDATE_DT.year % 100:02d}"
REPTMON  = f"{REPTDATE_DT.month:02d}"
REPTDAY  = f"{REPTDATE_DT.day:02d}"
RDATE    = f"{_sas_days(REPTDATE_DT):05d}"

# Previous months using end-of-month anchor (only month numbers are used for filenames)
REPTMON2 = f"{_shift_month_end(REPTDATE_DT, -1).month:02d}"
REPTMON3 = f"{_shift_month_end(REPTDATE_DT, -2).month:02d}"

# =====================================================
# 2) REPODATE from LNTXT (already Parquet). Build REPTDATE and filter deletions
# =====================================================
# If you mirror SAS DD path, use:
# LNTXT = pl.read_parquet(BASE_IN / "RBP2" / "B033" / "EXT" / "LN5SC" / "INPMIS.parquet")
LNTXT = pl.read_parquet(BASE_IN / "LNTXT.parquet")

# Expect columns: ACCTNO, NOTENO, PROJDT, REPODT, NOTETYPE, STATUS, REMARK
req_cols = ["ACCTNO","NOTENO","PROJDT","REPODT","NOTETYPE","STATUS","REMARK"]
missing = [c for c in req_cols if c not in LNTXT.columns]
if missing:
    raise ValueError(f"LNTXT missing columns: {missing}")

# REPODT parsing as in SAS: PUT(Z11.) then substr → MM, DD, YYYY
def _split_repodt(n) -> tuple[int|None,int|None,int|None]:
    if n is None:
        return (None, None, None)
    try:
        s = f"{int(n):011d}"
        mm = int(s[0:2])
        dd = int(s[2:4])
        yy = int(s[4:8])
        return (yy, mm, dd)
    except Exception:
        return (None, None, None)

REPODATE = (
    LNTXT
    .with_columns([
        pl.struct([pl.col("REPODT")]).map_elements(lambda st: _split_repodt(st["REPODT"])).alias("_YMD"),
    ])
    .with_columns([
        pl.col("_YMD").map_elements(lambda t: t[0]).alias("YY"),
        pl.col("_YMD").map_elements(lambda t: t[1]).alias("MM"),
        pl.col("_YMD").map_elements(lambda t: t[2]).alias("DD"),
    ])
    .with_columns([
        pl.when((pl.col("YY").is_not_null()) & (pl.col("MM").is_not_null()) & (pl.col("DD").is_not_null()))
          .then(pl.date(pl.col("YY"), pl.col("MM"), pl.col("DD")))
          .otherwise(None)
          .alias("_REPTDATE_DT"),
    ])
    .with_columns([
        (pl.col("_REPTDATE_DT") - pl.duration(days=1)).alias("_REPTDATE_MINUS1"),
        ((pl.col("STATUS") == pl.lit("N")) & (pl.col("REMARK") == pl.lit("LOAN ACCOUNT NOT FOUND"))).alias("_DROP"),
    ])
    .filter(~pl.col("_DROP"))
    .with_columns([
        pl.col("_REPTDATE_MINUS1").map_elements(lambda d: None if d is None else _sas_days(d),
                                                return_dtype=pl.Int64).alias("REPTDATE"),
    ])
    .drop(["_YMD","YY","MM","DD","_REPTDATE_DT","_REPTDATE_MINUS1","_DROP"])
    .with_columns([
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("NOTENO").cast(pl.Int64),
    ])
    .select(["ACCTNO","NOTENO","PROJDT","REPODT","NOTETYPE","STATUS","REMARK","REPTDATE"])
    .sort(["ACCTNO","NOTENO","REPTDATE"])
)

# Process flag equivalent to NOBS>0
PROCESS = REPODATE.height > 0

if PROCESS:
    # =================================================
    # 3) PROC MEANS MIN/MAX on REPTDATE (SAS-days int)
    # =================================================
    agg = REPODATE.select([
        pl.min("REPTDATE").alias("STDATE"),
        pl.max("REPTDATE").alias("LTDATE")
    ]).row(0)
    STDATE, LTDATE = agg

    # =================================================
    # 4) Read monthly files for current and previous two months, filter by date window
    # =================================================
    def _read_month(path_dir: Path, mm: str) -> pl.DataFrame:
        p = path_dir / f"REPOLN{mm}.parquet"
        if p.exists():
            return pl.read_parquet(p)
        return pl.DataFrame([])

    REPO = pl.concat([
        _read_month(LOAN_DIR,  REPTMON),
        _read_month(LOANP_DIR, REPTMON2),
        _read_month(LOANQ_DIR, REPTMON3),
    ], how="vertical_relaxed") if True else pl.DataFrame([])

    if REPO.height == 0:
        OUT = pl.DataFrame([])
    else:
        # Normalize keys and REPTDATE to SAS-days for correct filtering/join
        REPO = (
            REPO.with_columns([
                _to_sas_days_expr("REPTDATE").alias("REPTDATE"),
                pl.col("ACCTNO").cast(pl.Int64),
                pl.col("NOTENO").cast(pl.Int64),
            ])
            .filter(
                (pl.col("REPTDATE") >= pl.lit(int(STDATE))) &
                (pl.col("REPTDATE") <= pl.lit(int(LTDATE)))
            )
            .sort(["ACCTNO","NOTENO","REPTDATE"])
        )

        # =================================================
        # 5) Merge (inner) with REPODATE by ACCTNO NOTENO REPTDATE
        # =================================================
        ALL = REPO.join(REPODATE, on=["ACCTNO","NOTENO","REPTDATE"], how="inner")

        # Guards for required columns that are used later
        need_cols = [
            "LOANTYPE","CURBAL","INTEARN4","REBATE","APPVALUE","ORGBAL","NETPROC",
            "ECSRRSRV","FEETOT2","PAYAMT","TOTBILL","TOTNPAID","OVERINT","PAYOFF","BILLPAY"
        ]
        miss2 = [c for c in need_cols if c not in ALL.columns]
        if miss2:
            raise ValueError(f"REPO missing required columns for computation: {miss2}")

        # =================================================
        # 6) Compute derived metrics (mirrors SAS data step)
        # =================================================
        grp1 = {380,381,700,705,993,996,128,130,983}
        grp2 = {720,725,131,132}

        ALL = (
            ALL
            .with_columns([
                pl.when(pl.col("LOANTYPE").is_in(list(grp1)))
                  .then(pl.col("CURBAL") - (pl.col("INTEARN4") + pl.col("REBATE")))
                  .when(pl.col("LOANTYPE").is_in(list(grp2)))
                  .then(pl.col("CURBAL"))
                  .otherwise(None)
                  .alias("PRINOUT"),

                (pl.col("INTEARN4") + pl.col("REBATE")).alias("REBATES"),
                (pl.col("APPVALUE") + (pl.col("ORGBAL") - pl.col("NETPROC"))).alias("TTAMTPAY"),
                ((pl.col("APPVALUE") - pl.col("NETPROC")) + (pl.col("ORGBAL") - pl.col("CURBAL"))).alias("LESSDEPO"),
            ])
        ).with_columns([
            pl.when(pl.col("LOANTYPE").is_in([130,381,705]))
              .then(pl.col("LESSDEPO") + pl.col("ECSRRSRV"))
              .otherwise(pl.col("LESSDEPO")).alias("LESSDEPO"),

            (pl.col("REBATE") + pl.col("INTEARN4")).alias("LESSSTAT"),
            pl.col("FEETOT2").alias("ADDARRS"),
        ])

        # Multiply by 100 (SAS does this before PUT)
        mult_cols = [
            "CURBAL","APPVALUE","NETPROC","ORGBAL","REBATE","INTEARN4","PAYAMT","TOTBILL",
            "PRINOUT","REBATES","TOTNPAID","FEETOT2","TTAMTPAY","LESSDEPO","LESSSTAT","ADDARRS",
            "OVERINT","PAYOFF"
        ]
        ALL = ALL.with_columns([(pl.col(c) * pl.lit(100)).alias(c) for c in mult_cols])

        # ZERO = 0 and YYMMDD6 string for output readability
        ALL = ALL.with_columns([
            pl.lit(0).alias("ZERO"),
            pl.col("REPTDATE").map_elements(_yymmdd6_from_sas_days, return_dtype=pl.Utf8).alias("REPTDATE_YYMMDD6"),
            # Truncate/pad REMARK to 65 chars like SAS $65.
            pl.col("REMARK").cast(pl.Utf8).fill_null("").str.slice(0, 65).alias("REMARK"),
        ])

        # =================================================
        # 7) Select output columns in the same order as SAS PUT
        # =================================================
        OUT = ALL.select([
            "ZERO",
            "ACCTNO",
            "NOTENO",
            "PROJDT",
            "REPODT",
            "NOTETYPE",
            "STATUS",
            "REMARK",
            "PAYOFF",
            "APPVALUE",
            "NETPROC",
            "ORGBAL",
            "REBATE",
            "INTEARN4",
            "PAYAMT",
            "LOANTYPE",
            "BILLPAY",
            "TOTBILL",
            "PRINOUT",
            "REBATES",
            "TOTNPAID",
            "FEETOT2",
            "TTAMTPAY",
            "LESSDEPO",
            "LESSSTAT",
            "ADDARRS",
            "OVERINT",
            "REPTDATE",          # SAS numeric date (days since 1960)
            "REPTDATE_YYMMDD6",  # Friendly YYMMDD6 string (kept as extra)
        ])
    # =================================================
    # 8) Write Parquet (COMBO equivalent) — only if non-empty
    # =================================================
    out_name = f"LN5SC_OUTMIS_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
    out_path = BASE_OUT / "COMBO" / out_name
    if 'OUT' in locals() and OUT.height > 0:
        OUT.write_parquet(out_path)
        print(f"Written: {out_path}")
    else:
        print("Merged output is empty; nothing written.")
else:
    print("No REPODATE records to process (PROCESS = N). Nothing written.")
