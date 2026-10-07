#!/usr/bin/env python3
"""
Program : EIIMRLFM.py
Purpose : New Liquidity Framework (FISS submission, PIBB) -- deposit/loan
          maturity profile, undrawn commitments, NID, KAPITI items,
          distribution profile, and top-100 FD+CA depositor reports.
          Originally %INC PGM(EIIMRLFM) inside EIIMLIQP.

          Differences vs EIBMRLFM.py (PBB), all taken from the SAS source:
            - FCY product list ends at 814 (no 815-817).
            - GLPROD format has the PIBB product set (e.g. 017-019 -> 3313,
              061 -> CI01, extra CI06 members; no 077/078/131/132/...).
            - Loan schedule comes from PAY.ILNPAY (cache supplied by caller).
            - FD: ACCTTYPE ^= 398; BNMCODE chain adds 95317 / 95999.  The
              block "IF BIC='42630' THEN ... BNMCODE='96311'" is a SEPARATE
              IF from the chain that follows, so the chain's final ELSE
              (95311) overwrites it -- only the AMT<CCY> columns survive.
              This is reproduced exactly.
            - FCY current: PRODUCT 450-454 included; currency split is
              (400,440,450)=USD, 403=SGD, 406=HKD, (402,442,452)=AUD.
            - No VOSTRO, no DCI / DCIW (no FORATE / DCIWH / DCIWTB).
            - Top-100 CA exclusion list is 400-410 (no 411).
          PBBELF is %INC'd in the SAS source but no PUT(var,fmt.) call from
          it appears in this program's body -- not imported.

          LCR.* side datasets (LCR.FD, LCR.SA, LCR.CA, LCR.FCYCA, LCR.NID,
          LCR.K1TBL, LCR.K3TBL) are passive SAS outputs not consumed by any
          later step in this job; they are not materialised.

          K3FEI (KALMLIFE) is merged into the BNMCODE-keyed KTBL combination
          exactly as in EIBMRLFM.py.

          Designed to be imported by EIIMLIQP.py, mirroring %INC semantics.
          Owns no physical path -- every cache path and the REPTDATE context
          are supplied by the calling job.
"""
import math
from datetime import date
from pathlib import Path

import duckdb
import polars as pl

from PBBLNFMT_AII import format_liqpfmt
from PBBDPFMT_AII import fdprod_format, ddcustcd_format
from KALMLIQ import build_kalmliq
from KALMLIFE import build_k3fei

# ============================================================================
# CONSTANTS
# ============================================================================
_SAS_EPOCH = date(1960, 1, 1)

# %LET FCY=(800..806,851..860,807..814)
FCY_PRODUCTS = set(range(800, 815)) | set(range(851, 861))

_IND_CODES = [77.0, 78.0, 95.0, 96.0]
_IND_STR = ["77", "78", "95", "96"]
_CIS_KEEP = ["CUSTNO", "ACCTNO", "CUSTNAME", "ICNO", "NEWIC", "OLDIC", "INDORG"]
_PAYFREQ_MONTHS = {"1": 1, "2": 3, "3": 6, "4": 12}
_CCY_COLS = ("USD", "SGD", "HKD", "AUD")
_RC_CODES = ["34190", "34690"]
_MAX_ROLL = 1000        # safety cap: billing-date roll-forward steps
_MAX_ROUNDS = 2000      # safety cap: instalment schedule rounds

_BNMCODE_SCHEMA = {"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTUSD": pl.Float64,
                   "AMTSGD": pl.Float64, "AMTHKD": pl.Float64, "AMTAUD": pl.Float64}

# VALUE GLPROD (PIBB) -- only membership (vs OTHER='C999') is used downstream
_GLPROD_GROUPS = {
    "3301": [117], "3302": [110], "3303": [108], "3304": [118], "3305": [157, 102],
    "3306": [101], "3307": [121],
    "3308": [194, 195, 155, 192, 137, 154, 119, 120, 138, 193],
    "3309": [116], "3311": [114, 85, 86],
    "3313": [87, 88, 89, 91, 179, 174, 175, 100, 156, 198, 90, 93, 180, 197, 17, 18, 19],
    "3314": [123, 176, 196], "3315": [112], "3316": [115], "3317": [111],
    "3318": [113, 135, 189, 177, 190, 178], "3319": [122], "3320": [109],
    "3322": [165, 124, 191], "3323": [159, 125], "3324": [150, 181], "3325": [151],
    "3326": [152], "3327": [170], "3328": [153],
    "3330": [182, 183, 160, 166, 167, 168, 169], "3331": [161], "3332": [162], "3334": [164],
    "7101": [106, 158], "C001": [50], "C002": [51], "C006": [55], "C007": [56],
    "C008": [65, 57], "C009": [58], "CI01": [60, 61],
    "CI06": [64, 66, 67, 68, 69, 70, 71, 73, 74, 81, 82, 83, 84, 92, 94, 95, 96, 97,
             133, 134, 184, 185, 186, 187, 188, 20, 21, 22, 23, 24, 25, 75, 76,
             46, 47, 48, 49, 45, 13, 14, 15, 16, 5, 6, 7, 8],
    "HDA0": [53, 63, 103, 163],   # EXCLUDE FROM RDAL
}
GLPROD_MAP = {prod: code for code, prods in _GLPROD_GROUPS.items() for prod in prods}


# ============================================================================
# DATE / FORMAT HELPERS
# ============================================================================
def _remfmt(remmth: float) -> str:
    """VALUE REMFMT (scalar)."""
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


def _remfmt_expr(rem: pl.Expr) -> pl.Expr:
    return (pl.when(rem <= 0.1).then(pl.lit("01"))
            .when(rem <= 1).then(pl.lit("02"))
            .when(rem <= 3).then(pl.lit("03"))
            .when(rem <= 6).then(pl.lit("04"))
            .when(rem <= 12).then(pl.lit("05"))
            .otherwise(pl.lit("06")))


def _dim_expr(y: pl.Expr, m: pl.Expr) -> pl.Expr:
    """Days in month (same leap rule as the SAS macros: year % 4 == 0)."""
    return (pl.when(m == 2).then(pl.when(y % 4 == 0).then(29).otherwise(28))
            .when(m.is_in([4, 6, 9, 11])).then(30)
            .otherwise(31)).cast(pl.Int64)


def _remmth_expr(ctx: dict, d: pl.Expr) -> pl.Expr:
    """Vectorised %REMMTH (months part)."""
    dim = ctx["rd_days"][ctx["rpmth"] - 1]
    mdday = pl.min_horizontal(d.dt.day().cast(pl.Int64), pl.lit(dim, dtype=pl.Int64))
    return ((d.dt.year().cast(pl.Int64) - ctx["rpyr"]) * 12
            + (d.dt.month().cast(pl.Int64) - ctx["rpmth"])
            + (mdday - ctx["rpday"]) / dim)


def _days_to(d: pl.Expr, ctx: dict) -> pl.Expr:
    """MATDT - REPTDATE in days (null when MATDT is missing)."""
    return (d - pl.lit(ctx["reptdate"])).dt.total_days()


def _add_days(d: pl.Expr, n) -> pl.Expr:
    return (d.cast(pl.Int32) + n).cast(pl.Date)


def _sas_date_sql(col: str) -> str:
    """SAS numeric date -> DATE (DuckDB)."""
    return (f"CASE WHEN {col} IS NULL OR ISNAN({col}) THEN NULL "
            f"ELSE DATE '1960-01-01' + CAST(FLOOR({col}) AS INTEGER) END AS {col}")


def _fmt_expr(df: pl.DataFrame, src: str, fn) -> pl.Expr:
    """PUT(src, fmt.) through a python format function, evaluated once per distinct value."""
    mapping = {v: fn(v) for v in df[src].unique().to_list() if v is not None}
    try:
        null_value = fn(None)
    except Exception:
        null_value = None
    if not mapping:
        return pl.lit(null_value, dtype=pl.Utf8)
    return (pl.col(src).replace_strict(mapping, default=None, return_dtype=pl.Utf8)
            .fill_null(null_value))


def _nxt_date(df: pl.DataFrame, cur: str = "CUR") -> pl.Series:
    """Vectorised %NXTBLDT (PAYFREQ '6' = +14 days, otherwise +FREQ months)."""
    c = pl.col(cur)
    cy = c.dt.year().cast(pl.Int64)
    cm = c.dt.month().cast(pl.Int64)
    cd = c.dt.day().cast(pl.Int64)
    fortnight = pl.col("PAYFREQ") == "6"
    step = pl.col("PAYFREQ").replace_strict(_PAYFREQ_MONTHS, default=0, return_dtype=pl.Int64)
    f_d = cd + 14
    f_roll = f_d > _dim_expr(cy, cm)
    mm0 = (pl.when(fortnight)
           .then(pl.when(f_roll).then(cm + 1).otherwise(cm))
           .otherwise(cm + step))
    yy = pl.when(mm0 > 12).then(cy + 1).otherwise(cy)
    mm = pl.when(mm0 > 12).then(mm0 - 12).otherwise(mm0)
    payday = pl.col("PAYDAY").cast(pl.Int64, strict=False)
    dd_std = (pl.when(payday.is_not_null())
              .then(pl.when(payday == 99).then(_dim_expr(yy, mm)).otherwise(payday))
              .otherwise(cd))
    dd0 = (pl.when(fortnight)
           .then(pl.when(f_roll).then(f_d - _dim_expr(cy, cm)).otherwise(f_d))
           .otherwise(dd_std))
    dd = pl.min_horizontal(dd0, _dim_expr(yy, mm))
    return df.select(pl.date(yy, mm, dd).alias("NXT")).to_series()


def _remmth(ctx: dict, matdt: date):
    """%REMMTH macro (scalar)."""
    days_in_rpmth = ctx["rd_days"][ctx["rpmth"] - 1]
    mdday = min(matdt.day, days_in_rpmth)
    remy, remm = matdt.year - ctx["rpyr"], matdt.month - ctx["rpmth"]
    remd = mdday - ctx["rpday"]
    return remy * 12 + remm + remd / days_in_rpmth


# ============================================================================
# AGGREGATION HELPERS
# ============================================================================
def _summarize(df: pl.DataFrame) -> pl.DataFrame:
    """PROC SUMMARY NWAY CLASS BNMCODE SUM."""
    if df.is_empty():
        return pl.DataFrame(schema=_BNMCODE_SCHEMA)
    return df.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum(), pl.col("AMTUSD").sum(), pl.col("AMTSGD").sum(),
        pl.col("AMTHKD").sum(), pl.col("AMTAUD").sum(),
    ])


def _zero_amts() -> list:
    return [pl.lit(0.0).alias(f"AMT{c}") for c in _CCY_COLS]


def _emit_agg(df: pl.DataFrame, kind: str, amt: str, rem: str) -> pl.DataFrame:
    """kind 'A' -> 95 (LCY) / 94 (FCY);  kind 'B' -> 93 (LCY) / 96 (FCY).
    Currency columns follow SAS: filled when PRODUCT IN (800:899)."""
    lcy, fcy = ("95", "94") if kind == "A" else ("93", "96")
    fc, c8 = pl.col("IS_FCY"), pl.col("IS_8XX")
    out = df.select([
        pl.concat_str([
            pl.when(fc).then(pl.lit(fcy)).otherwise(pl.lit(lcy)),
            pl.col("ITEM"), pl.col("CUST"), _remfmt_expr(pl.col(rem)), pl.lit("0000Y"),
        ]).alias("BNMCODE"),
        pl.col(amt).alias("AMOUNT"),
        *[pl.when(c8 & (pl.col("CCY") == c)).then(pl.col(amt)).otherwise(0.0).alias(f"AMT{c}")
          for c in _CCY_COLS],
    ])
    return _summarize(out)


def _emit_pair(df: pl.DataFrame, amt: str, rem: str) -> list:
    """Rows 95/94 with REMMTH, then 93/96 with REMMTH=13 when COND (DAYS>89, LOANSTAT^=1, IMLOAN='Y')."""
    df = df.with_columns(
        pl.col(rem).alias("REM_A"),
        pl.when(pl.col("COND")).then(13.0).otherwise(pl.col(rem)).alias("REM_B"),
    )
    return [_emit_agg(df, "A", amt, "REM_A"), _emit_agg(df, "B", amt, "REM_B")]


def _code_rows(df: pl.DataFrame, codes: tuple, rem: str, amount: str = "AMOUNT") -> pl.DataFrame:
    """One output row per code: <code><REMFMT>0000Y."""
    return pl.concat([
        df.select([
            pl.concat_str([pl.lit(code), _remfmt_expr(pl.col(rem)), pl.lit("0000Y")]).alias("BNMCODE"),
            pl.col(amount).alias("AMOUNT"), *_zero_amts(),
        ]) for code in codes
    ], how="vertical")


# ============================================================================
# NOTE (loans) -- BREAKDOWN BY MATURITY PROFILE (PART 1 & 2 - RM)
# ============================================================================
def _amortise(loop: pl.DataFrame, ctx: dict, parts: list) -> None:
    """Instalment schedule for all loans at once (replaces the per-row SAS DO WHILE loop)."""
    rept = pl.lit(ctx["reptdate"])
    epoch = pl.lit(_SAS_EPOCH)
    loop = loop.with_columns(
        (pl.col("PAYFREQ").is_null() | pl.col("PAYFREQ").is_in(["5", "9", " "])
         | pl.col("PRODUCT").is_in([350, 910, 925])).fill_null(False).alias("FSKIP")
    ).with_columns(
        # BLDATE <= 0 (SAS) -> missing or on/before 01JAN1960
        (~pl.col("FSKIP") & (pl.col("BLDATE").is_null() | (pl.col("BLDATE") <= epoch))
         ).fill_null(False).alias("ROLL")
    ).with_columns(
        pl.when(pl.col("FSKIP")).then(pl.col("EXPRDATE"))
        .when(pl.col("ROLL")).then(pl.col("ISSDTE"))
        .otherwise(pl.col("BLDATE")).alias("CUR")
    )

    # BLDATE = ISSDTE; DO WHILE (BLDATE <= REPTDATE); %NXTBLDT; END;
    rolling = loop.filter(pl.col("ROLL"))
    if not rolling.is_empty():
        iss = pl.col("ISSDTE")
        iss_y = iss.dt.year().cast(pl.Int64)
        iss_m = iss.dt.month().cast(pl.Int64)
        iss_d = iss.dt.day().cast(pl.Int64)
        fortnight = pl.col("PAYFREQ") == "6"
        step_m = pl.col("PAYFREQ").replace_strict(_PAYFREQ_MONTHS, default=0, return_dtype=pl.Int64)
        months_gap = ((rept.dt.year().cast(pl.Int64) - iss_y) * 12
                      + (rept.dt.month().cast(pl.Int64) - iss_m))

        # Fortnightly: +14 days is exact, jump straight to the first date > REPTDATE.
        k_fn = pl.max_horizontal(pl.lit(0, dtype=pl.Int64), (rept - iss).dt.total_days() // 14 + 1)
        fn_cur = _add_days(iss, 14 * k_fn)

        # Monthly with no PAYDAY and no month-end clamping risk (day <= 28): jump close, step the rest.
        jumpable = (~fortnight & pl.col("PAYDAY").is_null() & (iss_d <= 28) & (step_m > 0)).fill_null(False)
        k_jump = pl.max_horizontal(pl.lit(0, dtype=pl.Int64), (months_gap // step_m.clip(lower_bound=1)) - 3)
        total_m = iss_y * 12 + (iss_m - 1) + k_jump * step_m
        jump_cur = pl.date(total_m // 12, (total_m % 12) + 1, iss_d)

        rolling = rolling.with_columns(
            pl.when(fortnight).then(fn_cur)
            .when(jumpable).then(jump_cur)
            .otherwise(iss).alias("CUR")
        )

        # Exact sequential stepping for whatever is still <= REPTDATE.
        done, pending = [], rolling
        for _ in range(_MAX_ROLL):
            still = (pl.col("CUR") <= rept).fill_null(False)
            done.append(pending.filter(~still))
            pending = pending.filter(still)
            if pending.is_empty():
                break
            pending = pending.with_columns(_nxt_date(pending).alias("CUR"))
        # Rows that could not advance past REPTDATE -> missing (falls back to EXPRDATE below).
        if not pending.is_empty():
            done.append(pending.with_columns(pl.lit(None, dtype=pl.Date).alias("CUR")))
        rolling = pl.concat(done, how="vertical")

    loop = pl.concat([loop.filter(~pl.col("ROLL")), rolling], how="vertical")

    loop = loop.with_columns(
        pl.when(pl.col("PAYAMT") < 0).then(0.0).otherwise(pl.col("PAYAMT")).alias("PAYAMT")
    ).with_columns(
        pl.when(pl.col("CUR").is_null() | (pl.col("CUR") > pl.col("EXPRDATE"))
                | (pl.col("BALANCE") <= pl.col("PAYAMT")))
        .then(pl.col("EXPRDATE")).otherwise(pl.col("CUR")).alias("CUR"),
        pl.col("BALANCE").alias("BAL"),
    )

    active = loop
    for _ in range(_MAX_ROUNDS):
        if active.is_empty():
            break
        active = active.with_columns(_remmth_expr(ctx, pl.col("CUR")).alias("REMM"))
        is_last = (pl.col("REMM") > 12) | (pl.col("CUR") == pl.col("EXPRDATE"))

        last = active.filter(is_last)                       # LEAVE -> residual balance
        if not last.is_empty():
            parts.extend(_emit_pair(last, "BAL", "REMM"))

        cont = active.filter(~is_last)
        if cont.is_empty():
            active = cont
            break
        cont = cont.with_columns(
            pl.when((pl.col("REMM") > 0.1) & ((pl.col("CUR") - rept).dt.total_days() < 8))
            .then(0.1).otherwise(pl.col("REMM")).alias("REM_I"))
        parts.extend(_emit_pair(cont, "PAYAMT", "REM_I"))   # instalment amount

        cont = cont.with_columns((pl.col("BAL") - pl.col("PAYAMT")).alias("BAL"))
        cont = cont.with_columns(_nxt_date(cont).alias("NXT"))
        active = cont.with_columns(
            pl.when((pl.col("NXT") > pl.col("EXPRDATE"))
                    | (pl.col("BAL") <= pl.col("PAYAMT"))
                    | (pl.col("NXT") <= pl.col("CUR")))      # date did not advance -> stop at EXPRDATE
            .then(pl.col("EXPRDATE")).otherwise(pl.col("NXT")).alias("CUR")
        ).drop("NXT")

    if not active.is_empty():                               # safety net only
        print(f"  [warn] {active.height:,} loans hit _MAX_ROUNDS; finalised at residual balance")
        active = active.with_columns(_remmth_expr(ctx, pl.col("CUR")).alias("REMM"))
        parts.extend(_emit_pair(active, "BAL", "REMM"))


def _note_to_rows(note: pl.DataFrame, ctx: dict) -> pl.DataFrame:
    rept = pl.lit(ctx["reptdate"])
    ind = pl.col("CUSTCD").cast(pl.Float64, strict=False).is_in(_IND_CODES).fill_null(False)
    num = lambda c: pl.col(c).cast(pl.Float64, strict=False).fill_nan(None).fill_null(0.0)

    for opt in ("PAYDAY", "DAYS"):          # optional columns of the source datasets
        if opt not in note.columns:
            note = note.with_columns(pl.lit(None, dtype=pl.Float64).alias(opt))

    # IF BLDATE > 0 THEN DAYS = REPTDATE - BLDATE;  (else DAYS keeps the dataset value)
    days = (pl.when(pl.col("BLDATE") > pl.lit(_SAS_EPOCH))
            .then((rept - pl.col("BLDATE")).dt.total_days().cast(pl.Float64))
            .otherwise(pl.col("DAYS").cast(pl.Float64, strict=False)))

    df = note.select(["PRODUCT", "CUSTCD", "ACCTYPE", "BALANCE", "PAYAMT", "BLDATE", "ISSDTE",
                      "EXPRDATE", "LOANSTAT", "IMLOAN", "PAYFREQ", "CCY", "EIR_ADJ",
                      "PAYDAY", "DAYS"]).with_columns(
        pl.col("PRODUCT").cast(pl.Int64, strict=False),
        ind.alias("IS_IND"),
        pl.when(ind).then(pl.lit("08")).otherwise(pl.lit("09")).alias("CUST"),
        num("BALANCE").alias("BALANCE"),
        num("PAYAMT").alias("PAYAMT"),
        pl.col("PAYFREQ").cast(pl.Utf8),
        pl.col("EIR_ADJ").cast(pl.Float64, strict=False).fill_nan(None).alias("EIR"),
        ((days > 89).fill_null(False)
         | pl.col("LOANSTAT").cast(pl.Float64, strict=False).ne_missing(1.0)
         | (pl.col("IMLOAN") == "Y").fill_null(False)).alias("COND"),
    ).with_columns(
        pl.col("PRODUCT").is_in(sorted(FCY_PRODUCTS)).fill_null(False).alias("IS_FCY"),
        pl.col("PRODUCT").is_between(800, 899).fill_null(False).alias("IS_8XX"),
    )

    parts = []

    # ---- OD : 95213{cust}010000Y -------------------------------------------
    od = df.filter(pl.col("ACCTYPE") == "OD")
    parts.append(_summarize(od.select([
        pl.concat_str([pl.lit("95213"), pl.col("CUST"), pl.lit("010000Y")]).alias("BNMCODE"),
        pl.col("BALANCE").alias("AMOUNT"), *_zero_amts(),
    ])))

    # ---- LN -----------------------------------------------------------------
    ln = df.filter(pl.col("ACCTYPE") == "LN")
    prods = [p for p in ln["PRODUCT"].unique().to_list() if p is not None]
    prod_map = pl.DataFrame({"PRODUCT": prods, "PROD": [format_liqpfmt(p) for p in prods]},
                            schema={"PRODUCT": pl.Int64, "PROD": pl.Utf8})
    ln = ln.join(prod_map, on="PRODUCT", how="left").with_columns(
        pl.when(pl.col("IS_IND"))
        .then(pl.when(pl.col("PROD") == "HL").then(pl.lit("214")).otherwise(pl.lit("219")))
        .otherwise(pl.when(pl.col("PROD").is_in(["FL", "HL"])).then(pl.lit("211"))
                   .when(pl.col("PROD") == "RC").then(pl.lit("212"))
                   .otherwise(pl.lit("219"))).alias("ITEM")
    )

    # EIR adjustment rows (only when EIR_ADJ is present)
    eir = ln.filter(pl.col("EIR").is_not_null())
    for pfx in ("95", "93"):
        parts.append(_summarize(eir.select([
            pl.concat_str([pl.lit(pfx), pl.col("ITEM"), pl.col("CUST"), pl.lit("060000Y")]).alias("BNMCODE"),
            pl.col("EIR").alias("AMOUNT"), *_zero_amts(),
        ])))

    # No schedule needed: no expiry date, or expiring in < 8 days -> remmth = 0.1
    simple_mask = pl.col("EXPRDATE").is_null() | ((pl.col("EXPRDATE") - rept).dt.total_days() < 8)
    simple = ln.filter(simple_mask).with_columns(pl.lit(0.1).alias("REM0"))
    parts.extend(_emit_pair(simple, "BALANCE", "REM0"))

    # Instalment schedule
    _amortise(ln.filter(~simple_mask), ctx, parts)

    return _summarize(pl.concat(parts, how="vertical"))


def _build_note(bnm1_loan_cache, lncomm_cache, provsub_txt_path, lnpay_cache, ctx) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    loan_all = con.execute(f"""
        SELECT * REPLACE (
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            CAST(NOTENO AS BIGINT) AS NOTENO,
            CAST(COMMNO AS BIGINT) AS COMMNO,
            {_sas_date_sql('BLDATE')},
            {_sas_date_sql('ISSDTE')},
            {_sas_date_sql('EXPRDATE')},
            {_sas_date_sql('APPRDATE')}
        )
        FROM read_parquet('{bnm1_loan_cache.as_posix()}')
        WHERE (SUBSTR(CAST(PRODCD AS VARCHAR),1,2) = '34' OR PRODUCT IN (225, 226))
          AND CAST(ACCTYPE AS VARCHAR) IN ('OD','LN')
    """).pl()
    # IF PAIDIND NOT IN ('P','C') OR EIR_ADJ NE .
    loan_all = loan_all.filter(
        ~pl.col("PAIDIND").cast(pl.Utf8).is_in(["P", "C"]).fill_null(False)
        | pl.col("EIR_ADJ").cast(pl.Float64, strict=False).fill_nan(None).is_not_null()
    )

    rcloan = loan_all.filter(pl.col("PRODCD").is_in(_RC_CODES)).select(["ACCTNO", "COMMNO", "NOTENO"])
    lncomm = con.execute(f"""
        SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO,
               CAST(COMMNO AS BIGINT) AS COMMNO,
               TRY_STRPTIME(
                   SUBSTR(LPAD(CAST(CAST(EXPIREDT AS BIGINT) AS VARCHAR), 11, '0'), 1, 8),
                   '%m%d%Y'
               )::DATE AS EXPRDATE
        FROM read_parquet('{lncomm_cache.as_posix()}')
    """).pl().sort(["ACCTNO", "COMMNO"], maintain_order=True).unique(
        subset=["ACCTNO", "COMMNO"], keep="first", maintain_order=True)

    rcnote = (
        lncomm.join(rcloan, on=["ACCTNO", "COMMNO"], how="inner")
        .sort(["ACCTNO", "COMMNO"], maintain_order=True)
        .unique(subset=["ACCTNO", "NOTENO"], keep="first", maintain_order=True)
        .select(["ACCTNO", "NOTENO", pl.col("EXPRDATE").alias("EXPRDATE_RC"), pl.lit(True).alias("RCFLAG")])
    )

    # PROVSUB flat file: FIRSTOBS=2, ACCTNO @1 10., NOTENO @12 5., IMLOAN @18 $1.
    provsub_rows = []
    with open(provsub_txt_path, "r", encoding="latin1") as fh:
        for line in fh.readlines()[1:]:
            line = line.rstrip("\n")
            acctno_s, noteno_s, imloan = line[0:10].strip(), line[11:16].strip(), line[17:18].strip()
            if imloan == "Y" and acctno_s.isdigit() and noteno_s.isdigit():
                provsub_rows.append({"ACCTNO": int(acctno_s), "NOTENO": int(noteno_s), "IMLOAN": imloan})
    provsub_schema = {"ACCTNO": pl.Int64, "NOTENO": pl.Int64, "IMLOAN": pl.Utf8}
    provsub = (pl.DataFrame(provsub_rows, schema=provsub_schema) if provsub_rows
               else pl.DataFrame(schema=provsub_schema)).unique(subset=["ACCTNO", "NOTENO"], keep="first")

    # MERGE LOAN(IN=A) RCNOTE(IN=B) PROVSUB; BY ACCTNO NOTENO; IF A;
    note = (
        loan_all
        .join(rcnote, on=["ACCTNO", "NOTENO"], how="left")
        .with_columns(
            pl.when(pl.col("RCFLAG").fill_null(False)).then(pl.col("EXPRDATE_RC"))
            .otherwise(pl.col("EXPRDATE")).alias("EXPRDATE"))
        .drop(["EXPRDATE_RC", "RCFLAG"])
    )
    if "IMLOAN" in note.columns:
        note = note.drop("IMLOAN")
    note = note.join(provsub, on=["ACCTNO", "NOTENO"], how="left")

    pay_raw = con.execute(f"""
        SELECT * REPLACE (
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            CAST(NOTENO AS BIGINT) AS NOTENO,
            {_sas_date_sql('EFFDATE')}
        )
        FROM read_parquet('{lnpay_cache.as_posix()}')
    """).pl()
    con.close()

    tdate = ctx["reptdate"]
    pay_keep = ["ACCTNO", "NOTENO", "PAYAMT"] + (["PAYDAY"] if "PAYDAY" in pay_raw.columns else [])
    # Latest effective schedule row per ACCTNO/NOTENO/PAYAMT (sorted, then NODUPKEY)
    pay = pay_raw.with_columns([
        pl.when(pl.col("EFFDATE") <= tdate).then(1).otherwise(0).alias("SORT_IND"),
        pl.when(pl.col("EFFDATE") <= tdate).then(pl.col("EFFDATE").cast(pl.Int64))
          .otherwise(-pl.col("EFFDATE").cast(pl.Int64)).alias("MANI_EFFDATE"),
    ]).sort(
        ["ACCTNO", "NOTENO", "PAYAMT", "SORT_IND", "MANI_EFFDATE"],
        descending=[False, False, False, True, True], maintain_order=True,
    ).unique(subset=["ACCTNO", "NOTENO", "PAYAMT"], keep="first", maintain_order=True).select(pay_keep)

    if "PAYDAY" in pay.columns and "PAYDAY" in note.columns:
        note = note.drop("PAYDAY")
    note = note.join(pay, on=["ACCTNO", "NOTENO", "PAYAMT"], how="left")
    if "FORATE" in note.columns:
        note = note.with_columns(
            pl.when(pl.col("PRODUCT").is_between(800, 899))
            .then(pl.col("PAYAMT").cast(pl.Float64, strict=False) * pl.col("FORATE").cast(pl.Float64, strict=False))
            .otherwise(pl.col("PAYAMT").cast(pl.Float64, strict=False))
            .alias("PAYAMT")
        )
    return _note_to_rows(note, ctx)


# ============================================================================
# FIXED DEPOSITS / SAVINGS / CURRENT / FCY CURRENT
# ============================================================================
def _build_fd(fd_fd_cache, ctx) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    fd = con.execute(f"""
        SELECT * REPLACE (
            CASE WHEN MATDATE IS NULL OR ISNAN(MATDATE) THEN NULL
                 ELSE TRY_STRPTIME(CAST(CAST(MATDATE AS BIGINT) AS VARCHAR), '%Y%m%d')::DATE END AS MATDATE
        )
        FROM read_parquet('{fd_fd_cache.as_posix()}') WHERE ACCTTYPE <> 398 AND CURBAL > 0
    """).pl()
    con.close()
    if fd.is_empty():
        return pl.DataFrame(schema=_BNMCODE_SCHEMA)

    ind = pl.col("CUSTCD").cast(pl.Float64, strict=False).is_in(_IND_CODES).fill_null(False)
    short = ((pl.col("OPENIND") == "D").fill_null(False) | pl.col("MATDATE").is_null()
             | (_days_to(pl.col("MATDATE"), ctx) < 8).fill_null(False))
    fd = fd.with_columns(
        _fmt_expr(fd, "INTPLAN", fdprod_format).alias("BIC"),
        pl.when(ind).then(pl.lit("08")).otherwise(pl.lit("09")).alias("CUST"),
        pl.when(short).then(0.1).otherwise(_remmth_expr(ctx, pl.col("MATDATE"))).alias("REM"),
        pl.col("CURBAL").fill_null(0.0).alias("AMOUNT"),
    )

    # IF BIC='42630' THEN (AMT<CCY> + BNMCODE='96311...') -- the BNMCODE is then overwritten by
    # the separate IF-chain below, so only the currency amounts of that block survive.
    bic = pl.col("BIC")
    acct_in = pl.col("ACCTTYPE").cast(pl.Int64, strict=False).is_in([302, 315, 394, 396, 313, 314]).fill_null(False)
    prefix = (pl.when((bic == "42133") | acct_in).then(pl.lit("95317"))
              .when(bic == "42132").then(pl.lit("95315"))
              .when(bic == "49999").then(pl.lit("95999"))
              .otherwise(pl.lit("95311")))
    return fd.select([
        pl.concat_str([prefix, pl.col("CUST"), _remfmt_expr(pl.col("REM")), pl.lit("0000Y")]).alias("BNMCODE"),
        pl.col("AMOUNT"),
        *[pl.when((bic == "42630") & (pl.col("CURCODE") == c)).then(pl.col("AMOUNT")).otherwise(0.0)
          .alias(f"AMT{c}") for c in _CCY_COLS],
    ])


def _build_sa(bnm_savg_cache) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    sa = con.execute(f"SELECT * FROM read_parquet('{bnm_savg_cache.as_posix()}')").pl()
    con.close()
    if "PRODCD" in sa.columns:                       # IF PRODCD NE 'N' THEN OUTPUT SA
        sa = sa.filter(pl.col("PRODCD").ne_missing("N"))
    return sa.with_columns(
        pl.when(pl.col("CUSTCD").is_in(_IND_STR)).then(pl.lit("08")).otherwise(pl.lit("09")).alias("CUST")
    ).select([
        (pl.lit("95312") + pl.col("CUST") + pl.lit("010000Y")).alias("BNMCODE"),
        pl.col("CURBAL").alias("AMOUNT"), *_zero_amts(),
    ])


def _build_ca(bnm_curn_cache) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    ca = con.execute(f"SELECT * FROM read_parquet('{bnm_curn_cache.as_posix()}')").pl()
    con.close()
    # GLPROX=PUT(PRODUCT,GLPROD.); IF GLPROX NE 'C999';  IF SUBSTR(PRODCD,1,3) IN ('421','423');
    ca = ca.filter(
        pl.col("PRODUCT").cast(pl.Int64, strict=False).is_in(list(GLPROD_MAP)).fill_null(False)
        & pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 3).is_in(["421", "423"]).fill_null(False)
    )
    return ca.select([
        pl.concat_str([
            pl.lit("95313"),
            pl.when(pl.col("CUSTCD").is_in(_IND_STR)).then(pl.lit("08")).otherwise(pl.lit("09")),
            pl.lit("010000Y"),
        ]).alias("BNMCODE"),
        pl.col("CURBAL").alias("AMOUNT"), *_zero_amts(),
    ])


def _build_fcyca(deposit_current_cache) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT * FROM read_parquet('{deposit_current_cache.as_posix()}')
        WHERE (PRODUCT BETWEEN 400 AND 444 OR PRODUCT BETWEEN 450 AND 454) AND PRODUCT <> 413
    """).pl()
    con.close()
    if raw.is_empty():
        return pl.DataFrame(schema=_BNMCODE_SCHEMA)
    raw = raw.with_columns(
        _fmt_expr(raw, "CUSTCODE", ddcustcd_format).alias("CUSTCD2"),
        pl.col("PRODUCT").cast(pl.Int64, strict=False).alias("PRODUCT"),
        pl.col("CURBAL").fill_null(0.0).alias("CURBAL"),
    )
    prod = pl.col("PRODUCT")
    return raw.select([
        pl.concat_str([
            pl.lit("96313"),
            pl.when(pl.col("CUSTCD2").is_in(_IND_STR)).then(pl.lit("08")).otherwise(pl.lit("09")),
            pl.lit("010000Y"),
        ]).alias("BNMCODE"),
        pl.col("CURBAL").alias("AMOUNT"),
        pl.when(prod.is_in([400, 440, 450])).then(pl.col("CURBAL")).otherwise(0.0).alias("AMTUSD"),
        pl.when(prod == 403).then(pl.col("CURBAL")).otherwise(0.0).alias("AMTSGD"),
        pl.when(prod == 406).then(pl.col("CURBAL")).otherwise(0.0).alias("AMTHKD"),
        pl.when(prod.is_in([402, 442, 452])).then(pl.col("CURBAL")).otherwise(0.0).alias("AMTAUD"),
    ])


# ============================================================================
# UNDRAWN PORTION (RC facilities)
# ============================================================================
def _build_undrawn(bnm1_loan_cache, bnm1_uloan_cache, ctx) -> pl.DataFrame:
    """LNCOMM is merged into ALWCOM only by its keys (ACCTNO, COMMNO) and none of its other
    columns are used by UNOTE, so it is not read here."""
    con = duckdb.connect(database=":memory:")
    loan_all = con.execute(f"""
        SELECT * REPLACE (
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            CAST(NOTENO AS BIGINT) AS NOTENO,
            CAST(COMMNO AS BIGINT) AS COMMNO,
            {_sas_date_sql('BLDATE')},
            {_sas_date_sql('ISSDTE')},
            {_sas_date_sql('EXPRDATE')},
            {_sas_date_sql('APPRDATE')}
        )
        FROM read_parquet('{bnm1_loan_cache.as_posix()}')
        WHERE COALESCE(CAST(PAIDIND AS VARCHAR), '') NOT IN ('P','C')
    """).pl()
    uloan = con.execute(f"""
        SELECT * REPLACE (
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            {_sas_date_sql('ISSDTE')},
            {_sas_date_sql('EXPRDATE')},
            {_sas_date_sql('APPRDATE')}
        )
        FROM read_parquet('{bnm1_uloan_cache.as_posix()}')
        WHERE NOT (ACCTNO BETWEEN 3000000000 AND 3999999999 AND PRODUCT IN (151,152,181) AND ACCTYPE = 'OD')
    """).pl()
    con.close()

    rc = pl.col("PRODCD").is_in(_RC_CODES).fill_null(False)
    alw = (loan_all.filter(~(pl.col("PRODUCT").is_in([151, 152, 181]) & (pl.col("ACCTYPE") == "OD")).fill_null(False))
           .sort(["ACCTNO", "NOTENO"], maintain_order=True))

    # DATA APPR: COMMNO > 0 -- RC keeps the first note per ACCTNO/COMMNO
    alwcom = alw.filter(pl.col("COMMNO") > 0)
    appr = pl.concat([
        alwcom.filter(rc).unique(subset=["ACCTNO", "COMMNO"], keep="first", maintain_order=True),
        alwcom.filter(~rc),
    ], how="diagonal_relaxed")

    # DATA APPR1: COMMNO <= 0 -- RC: first row per ACCTNO/APPRLIM2, plus (when a later duplicate
    # has BALANCE >= APPRLIM2) every row of that key with BALANCE >= APPRLIM2.
    alwnocom = alw.filter((pl.col("COMMNO") <= 0) | pl.col("COMMNO").is_null())
    nocom_rc = alwnocom.filter(rc).with_columns(
        pl.int_range(pl.len()).over(["ACCTNO", "APPRLIM2"]).alias("_RN"))
    flagged = (nocom_rc.filter((pl.col("_RN") > 0) & (pl.col("BALANCE") >= pl.col("APPRLIM2")))
               .select(["ACCTNO", "APPRLIM2"]).unique().with_columns(pl.lit(1).alias("DUPLI")))
    nocom_rc = nocom_rc.join(flagged, on=["ACCTNO", "APPRLIM2"], how="left")
    appr1_rc = nocom_rc.filter(
        ((pl.col("DUPLI") == 1) & (pl.col("BALANCE") >= pl.col("APPRLIM2")))
        | (pl.col("DUPLI").is_null() & (pl.col("_RN") == 0))
    ).drop(["_RN", "DUPLI"])
    appr1 = pl.concat([appr1_rc, alwnocom.filter(~rc)], how="diagonal_relaxed")

    # DATA UNOTE: SET LOAN ULOAN  (row order is irrelevant -- results are summed)
    df = pl.concat([appr, appr1, uloan], how="diagonal_relaxed").with_columns(
        pl.col("PRODUCT").cast(pl.Int64, strict=False),
        pl.col("PRODCD").cast(pl.Utf8),
    ).filter(
        (pl.col("PRODCD").str.slice(0, 2) == "34") | pl.col("PRODUCT").is_in([225, 226])
    )
    if df.is_empty():
        return pl.DataFrame(schema=_BNMCODE_SCHEMA)

    is_ln = pl.col("ACCTYPE") == "LN"
    matdt = pl.when(is_ln).then(pl.col("EXPRDATE")).otherwise(_add_days(pl.col("APPRDATE"), 365))
    item = (pl.when(pl.col("PRODCD") == "34240").then(pl.lit("429"))
            .when(is_ln).then(pl.when(rc).then(pl.lit("424")).otherwise(pl.lit("429")))
            .otherwise(pl.lit("423")))
    days_col = (pl.col("DAYS").cast(pl.Float64, strict=False) if "DAYS" in df.columns
                else pl.lit(None, dtype=pl.Float64))
    imloan = (pl.col("IMLOAN") == "Y").fill_null(False) if "IMLOAN" in df.columns else pl.lit(False)
    loanstat = (pl.col("LOANSTAT").cast(pl.Float64, strict=False).ne_missing(1.0)
                if "LOANSTAT" in df.columns else pl.lit(True))

    df = df.with_columns(matdt.alias("MATDT"), item.alias("ITEM")).with_columns(
        pl.when(pl.col("MATDT").is_null() | (_days_to(pl.col("MATDT"), ctx) < 8))
        .then(0.1).otherwise(_remmth_expr(ctx, pl.col("MATDT"))).alias("REM"),
        pl.col("UNDRAWN").cast(pl.Float64, strict=False).fill_null(0.0).alias("UNDRAWN"),
        pl.col("PRODUCT").is_in(sorted(FCY_PRODUCTS)).fill_null(False).alias("IS_FCY"),
        pl.lit("00").alias("CUST"),
        pl.lit(False).alias("IS_8XX"),
        pl.lit(None, dtype=pl.Utf8).alias("CCY"),
        ((days_col > 89).fill_null(False) | loanstat | imloan).alias("COND"),
    )
    return _summarize(pl.concat(_emit_pair(df, "UNDRAWN", "REM"), how="vertical"))


# ============================================================================
# NID
# ============================================================================
def _build_nid(nid_rnid_cache, ctx) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT * REPLACE (
            {_sas_date_sql('MATDT')},
            {_sas_date_sql('STARTDT')}
        )
        FROM read_parquet('{nid_rnid_cache.as_posix()}') WHERE NIDSTAT = 'N' AND CURBAL > 0
    """).pl()
    con.close()
    rept = ctx["reptdate"]
    raw = raw.filter((pl.col("MATDT") > rept) & (pl.col("STARTDT") <= rept))
    if raw.is_empty():
        return pl.DataFrame(schema=_BNMCODE_SCHEMA)
    raw = raw.with_columns(
        pl.when(_days_to(pl.col("MATDT"), ctx) < 8).then(0.1)
        .otherwise(_remmth_expr(ctx, pl.col("MATDT"))).alias("REM"),
        pl.col("CURBAL").fill_null(0.0).alias("AMOUNT"),
    )
    return _code_rows(raw, ("9384000", "9584000"), "REM")


# ============================================================================
# FISS / NSRS TEXT OUTPUT
# ============================================================================
def _write_fiss_nsrs(note_final, ctx, fiss_path, nsrs_path):
    def _emit(path, divisor):
        with open(path, "w", encoding="latin1") as fh:
            fh.write(f"RLFM{ctx['reptday']}{ctx['reptmon']}{ctx['reptyear']}\n")
            for r in note_final.iter_rows(named=True):
                def _p(v):
                    v = 0.0 if v is None else v
                    # SAS ROUND: half away from zero. Python round: banker's.
                    return int(math.floor(abs(v) / divisor + 0.5))
                fh.write(f"{r['BNMCODE']:<14};{_p(r['AMOUNT'])};{_p(r['AMTUSD'])};{_p(r['AMTSGD'])};"
                         f"{_p(r['AMTHKD'])};{_p(r['AMTAUD'])}\n")
    _emit(fiss_path, 1000)
    _emit(nsrs_path, 1)


def _write_suppl_report(dist_summary, rdate, output_path):
    if dist_summary.is_empty():
        suppl = dist_summary
    else:
        suppl = dist_summary.filter(pl.col("AMOUNT").abs() >= 5_000_000).sort(["CAT", "NAME"])
    lines = ["\f", "PUBLIC BANK BERHAD", f"NEW LIQUIDITY FRAMEWORK AS AT {rdate}", "",
             "CUSTOMER DEPOSITS >= 1% OF TOTAL (PART 3)", ""]
    grand_total, current_cat = 0.0, None
    for r in suppl.iter_rows(named=True):
        if r["CAT"] != current_cat:
            current_cat = r["CAT"]
            lines.append(current_cat)
        amount = r["AMOUNT"] or 0.0
        lines.append(f"  {str(r['NAME'])[:24]:<24}{amount:>20,.2f}")
        grand_total += amount
    lines.append(f"{'TOTAL':<26}{grand_total:>20,.2f}")
    with open(output_path, "w", encoding="latin1") as fh:
        fh.write("\n".join(lines) + "\n")


# ============================================================================
# TOP 100 FD+CA INDIVIDUAL/CORPORATE CUSTOMERS
# ============================================================================
def _build_top100(cisln_deposit_cache, cisdp_deposit_cache, deposit_current_cache, deposit_fd_cache):
    con = duckdb.connect(database=":memory:")
    cisca = con.execute(f"""
        SELECT * REPLACE (CAST(ACCTNO AS BIGINT) AS ACCTNO),
               COALESCE(NULLIF(NEWIC,''), OLDIC) AS ICNO
        FROM read_parquet('{cisln_deposit_cache.as_posix()}')
        WHERE ACCTNO BETWEEN 3000000000 AND 3999999999
    """).pl().select(_CIS_KEEP)
    cisfd = con.execute(f"""
        SELECT * REPLACE (CAST(ACCTNO AS BIGINT) AS ACCTNO),
               COALESCE(NULLIF(NEWIC,''), OLDIC) AS ICNO
        FROM read_parquet('{cisdp_deposit_cache.as_posix()}')
        WHERE (ACCTNO BETWEEN 1000000000 AND 1999999999) OR (ACCTNO BETWEEN 7000000000 AND 7999999999)
    """).pl().select(_CIS_KEEP)
    ca = con.execute(f"""
        SELECT * FROM read_parquet('{deposit_current_cache.as_posix()}') WHERE CURBAL > 0
    """).pl().with_columns([
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("CURBAL").alias("CABAL"),
    ])
    fd = con.execute(f"""
        SELECT * FROM read_parquet('{deposit_fd_cache.as_posix()}') WHERE CURBAL > 0
    """).pl().with_columns([
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("CURBAL").alias("FDBAL"),
    ])
    con.close()

    ca_excl = {400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410}      # PIBB: no 411
    fd_excl = {350, 351, 352, 353, 354, 355, 356, 357}

    cis_cols = [c for c in _CIS_KEEP if c != "ACCTNO"]

    # SAS: MERGE CA(IN=A) CISCA  -> keep every CA row; CIS (later dataset) wins on shared columns
    ca_j = (
        ca.drop([c for c in cis_cols if c in ca.columns])
        .join(cisca, on="ACCTNO", how="left")
        .filter(
            pl.col("PURPOSE").ne_missing("2")
            & ~pl.col("PRODUCT").is_in(ca_excl).fill_null(False)
        )
    )

    # SAS: MERGE CISFD FD(IN=A)  -> keep every FD row; FD (later dataset) wins on shared columns
    fd_j = (
        fd.join(cisfd.drop([c for c in cis_cols if c in fd.columns]), on="ACCTNO", how="left")
        .filter(
            pl.col("PURPOSE").ne_missing("2")
            & ~pl.col("ACCTTYPE").is_in(fd_excl).fill_null(False)
        )
        .with_columns(pl.col("ACCTTYPE").cast(pl.Int64, strict=False).alias("PRODUCT"))
    )

    # CA table uses CUSTCODE; FD table uses CUSTCD (the FD parquet has no CUSTCODE column)
    ca_is_ind = pl.col("CUSTCODE").cast(pl.Float64, strict=False).is_in(_IND_CODES).fill_null(False)
    fd_is_ind = pl.col("CUSTCD").cast(pl.Float64, strict=False).is_in(_IND_CODES).fill_null(False)

    ca_ind = ca_j.filter(ca_is_ind)
    ca_org = ca_j.filter(~ca_is_ind & (pl.col("INDORG") == "O"))
    fd_ind = fd_j.filter(fd_is_ind)
    fd_org = fd_j.filter(~fd_is_ind & (pl.col("INDORG") == "O"))

    def _shape(df: pl.DataFrame, is_fd: bool) -> pl.DataFrame:
        """Project df to the report columns with matching dtypes, adding the FDBAL / CABAL side
        that is missing on the other dataset."""
        def col_or_null(src, name, dtype):
            if src in df.columns:
                return pl.col(src).cast(dtype, strict=False).alias(name)
            return pl.lit(None, dtype=dtype).alias(name)

        product_src = "ACCTTYPE" if is_fd else "PRODUCT"
        out = df.select([
            col_or_null("BRANCH",    "BRANCH",   pl.Int64),
            col_or_null("ACCTNO",    "ACCTNO",   pl.Int64),
            col_or_null("CUSTNAME",  "CUSTNAME", pl.Utf8),
            col_or_null("CUSTNO",    "CUSTNO",   pl.Int64),
            col_or_null("NEWIC",     "NEWIC",    pl.Utf8),
            col_or_null("OLDIC",     "OLDIC",    pl.Utf8),
            col_or_null("CURBAL",    "CURBAL",   pl.Float64),
            col_or_null(product_src, "PRODUCT",  pl.Int64),
            col_or_null("ICNO",      "ICNO",     pl.Utf8),
        ])
        out = out.with_columns(
            (pl.col("CURBAL") if is_fd else pl.lit(0.0)).alias("FDBAL"),
            (pl.lit(0.0) if is_fd else pl.col("CURBAL")).alias("CABAL"),
            pl.lit(0 if is_fd else 1).cast(pl.Int8).alias("SRC"),   # FD first, then CA
        )
        return out.select([
            "BRANCH", "ACCTNO", "CUSTNAME", "CUSTNO", "NEWIC", "OLDIC",
            "CURBAL", "PRODUCT", "ICNO", "FDBAL", "CABAL", "SRC",
        ])

    def _ebcdic(col: str) -> pl.Expr:
        return pl.col(col).map_elements(
            lambda s: (s or "").ljust(60).encode("cp037").hex(), return_dtype=pl.Utf8)

    def _top100(fd_part, ca_part, corp_excl=False):
        fd_shaped = _shape(fd_part, is_fd=True)
        ca_shaped = _shape(ca_part, is_fd=False)
        data1 = pl.concat([fd_shaped, ca_shaped], how="vertical_relaxed").with_columns(
            pl.when(pl.col("ICNO").is_null() | (pl.col("ICNO") == ""))
            .then(pl.lit("XX"))
            .otherwise(pl.col("ICNO")).alias("ICNO")
        )
        if corp_excl:
            data1 = data1.filter(~(
                pl.col("ACCTNO").is_between(1590000000, 1599999999)
                | pl.col("ACCTNO").is_between(1689999999, 1699999999)
                | pl.col("ACCTNO").is_between(1789999999, 1799999999)
            ))
        data1 = data1.filter(pl.col("ICNO") != "")

        # One line per MNI NO per customer: sum the FD receipts of that account.
        # ICNO + CUSTNAME are in the key so joint holders are NOT added together.
        data1 = data1.group_by(["SRC", "ACCTNO", "ICNO", "CUSTNAME"], maintain_order=True).agg(
            [pl.col(c).first() for c in ("BRANCH", "CUSTNO", "NEWIC", "OLDIC", "PRODUCT")]
            + [pl.col(c).sum() for c in ("CURBAL", "FDBAL", "CABAL")]
        )

        summary = (
            data1.group_by(["ICNO", "CUSTNAME"], maintain_order=True)
            .agg([pl.col("CURBAL").sum(), pl.col("FDBAL").sum(), pl.col("CABAL").sum()])
            .with_columns(_ebcdic("ICNO").alias("_K1"), _ebcdic("CUSTNAME").alias("_K2"))
            .sort(["CURBAL", "_K1", "_K2"], descending=[True, False, False], maintain_order=True)
            .drop(["_K1", "_K2"])
            .head(100)
        )
        keys = summary.select(["ICNO", "CUSTNAME"])
        detail = (
            data1.join(keys, on=["ICNO", "CUSTNAME"], how="inner")
            .with_columns(_ebcdic("ICNO").alias("_K1"), _ebcdic("CUSTNAME").alias("_K2"))
            .sort(["_K1", "_K2", "SRC", "ACCTNO"], maintain_order=True)
            .drop(["_K1", "_K2"])
        )
        return summary, detail

    ind_sum, ind_det = _top100(fd_ind, ca_ind)
    org_sum, org_det = _top100(fd_org, ca_org, corp_excl=True)
    return ind_sum, org_sum, ind_det, org_det


# ============================================================================
# TOP 100 REPORT WRITER  (PROC PRINT look-alike, LRECL=320, PS=60, CRLF)
# ============================================================================
_T_LS, _T_PS, _T_OBSW, _T_MAXW = 320, 60, 8, 128   # linesize, pagesize, Obs col width, max row width
_T_SEP = "-" * 16


def _fnum(v) -> str:
    """SAS COMMA16.2: keep the commas when the value fits in 16 characters, otherwise drop them."""
    x = float(v or 0.0)
    s = f"{x:,.2f}"
    return s if len(s) <= 16 else f"{x:.2f}"


def _ctr(s: str, w: int, up: bool = False) -> str:
    pad = w - len(s)
    left = (pad + 1) // 2 if up else pad // 2
    return " " * left + s + " " * (pad - left)


def _fctr(txt: str, w: int, m: int) -> str:
    """Right-align inside a field of the widest value (m), field centred (round up) in column w."""
    start = (w - m + 1) // 2
    return " " * start + txt.rjust(m) + " " * (w - start - m)


def _wrap(label: str, w: int) -> list:
    lines, cur = [], ""
    for wd in label.split():
        if not cur:
            cur = wd
        elif len(cur) + 1 + len(wd) <= w:
            cur += " " + wd
        else:
            lines.append(cur)
            cur = wd
    return lines + [cur]


def _cols(rows: list) -> dict:
    """Column widths of one BY group (empty list -> minimum widths)."""
    mx = lambda k, lo: max([lo] + [len(str(r[k])) for r in rows])
    w = {"b": mx("BRANCH", 6), "a": mx("ACCTNO", 10 if rows else 6), "n": mx("CUSTNAME", 9),
         "c": mx("CUSTNO", 6), "i": mx("NEWIC", 6),
         "o": max([len(r["OLDIC"]) for r in rows] + [0]) or 3, "m": 16, "p": 7,
         "mb": max([len(str(r["BRANCH"])) for r in rows] + [3]),
         "mc": max([len(str(r["CUSTNO"])) for r in rows] + [0 if rows else 6]),
         "mp": max([len(str(r["PRODUCT"])) for r in rows] + [0 if rows else 3])}
    tot = _T_OBSW + w["b"] + w["a"] + w["n"] + w["c"] + w["i"] + w["o"] + w["m"] + w["p"]
    g = 4
    while g > 1 and tot + 7 * g > _T_MAXW:
        g -= 1
    w["g"] = g
    return w


def _mcol(w: dict) -> int:
    """Start offset of the CURBAL column."""
    return _T_OBSW + 7 * w["g"] + w["b"] + w["a"] + w["n"] + w["c"] + w["i"] + w["o"]


def _hdr(rows: list, w: dict, sums: bool) -> list:
    dat_m = max([len(_fnum(r["CURBAL"])) for r in rows] + [0])
    nal = "r" if sums else "cn"
    spec = [("BRANCH CODE", "b", "cn"), ("MNI NO", "a", nal), ("DEPOSITOR", "n", "c"), ("CIS NO", "c", nal),
            ("NEW IC", "i", "c"), ("OLD IC", "o", "c"), ("CURRENT BALANCE", "m", "r"), ("PRODUCT", "p", "cn")]
    labs = [_wrap(lbl, max(max(len(x) for x in lbl.split()), dat_m if k == "m" else w[k])) for lbl, k, _ in spec]
    nl = max(len(x) for x in labs)
    out = []
    for ln in range(nl):
        s = "Obs".rjust(_T_OBSW) if ln == nl - 1 else " " * _T_OBSW
        for (lbl, k, al), L in zip(spec, labs):
            t = ([""] * (nl - len(L)) + L)[ln]
            cell = t.rjust(w[k]) if al == "r" else _ctr(t, w[k], up=(al == "cn"))
            s += " " * w["g"] + cell
        out.append(s.rstrip())
    return out


def _row(obs: int, r: dict, w: dict) -> str:
    g = " " * w["g"]
    return (str(obs).rjust(_T_OBSW) + g + _fctr(str(r["BRANCH"]), w["b"], w["mb"]) + g
            + str(r["ACCTNO"]).rjust(w["a"]) + g + _ctr(r["CUSTNAME"], w["n"]) + g
            + _fctr(str(r["CUSTNO"]), w["c"], w["mc"]) + g + _ctr(r["NEWIC"], w["i"]) + g
            + _ctr(r["OLDIC"], w["o"]) + g + _fnum(r["CURBAL"]).rjust(w["m"]) + g
            + _fctr(str(r["PRODUCT"]), w["p"], w["mp"])).rstrip()


def _sumln(label: str, val, mc: int) -> str:
    return label.rjust(_T_OBSW) + " " * (mc - _T_OBSW) + _fnum(val).rjust(16)


def _dash(mc: int) -> str:
    return "--------".rjust(_T_OBSW) + " " * (mc - _T_OBSW) + _T_SEP


def _summary_lines(summary: pl.DataFrame, title: str) -> list:
    rows = summary.to_dicts()
    out, cap = [], _T_PS - 4
    zero = lambda v: "0  " if not v else _fnum(v)
    for p0 in range(0, len(rows), cap):
        page = rows[p0:p0 + cap]
        wn = max([9] + [len(str(r["CUSTNAME"] or "")[:40]) for r in page])
        out += [title, "", f"{'Obs':<3}    {'DEPOSITOR':<{wn}}    {'TOTAL BALANCE':>16}    {'FD BALANCE':>16}    {'CA BALANCE':>16}", ""]
        for k, r in enumerate(page, start=p0 + 1):
            out.append(f"{k:>3}    {str(r['CUSTNAME'] or '')[:40]:<{wn}}    {_fnum(r['CURBAL']):>16}"
                       f"    {zero(r['FDBAL']):>16}    {zero(r['CABAL']):>16}")
    return out


def _detail_lines(detail: pl.DataFrame, title: str) -> list:
    rows = []
    for r in detail.to_dicts():
        rows.append({"ICNO": r["ICNO"] or "", "CUSTNAME": r["CUSTNAME"] or "", "BRANCH": r["BRANCH"] or 0,
                     "ACCTNO": r["ACCTNO"] or 0, "CUSTNO": r["CUSTNO"] or 0, "NEWIC": r["NEWIC"] or "",
                     "OLDIC": r["OLDIC"] or "", "CURBAL": r["CURBAL"] or 0.0, "PRODUCT": r["PRODUCT"] or 0})
    groups = []
    for r in rows:
        k = (r["ICNO"], r["CUSTNAME"])
        if not groups or groups[-1][0] != k:
            groups.append((k, []))
        groups[-1][1].append(r)

    out, st, obs = [], {"u": 0}, 0

    def emit(lines):
        out.extend(lines)
        st["u"] += len(lines)

    def newpage():
        out.extend([title, ""])
        st["u"] = 2

    newpage()
    for gi, (k, grp) in enumerate(groups):
        n, w = len(grp), _cols(grp)
        mc, tot = _mcol(w), sum(r["CURBAL"] for r in grp)
        by = f"ICNO={k[0]} DEPOSITOR={k[1]}"

        def head(cont, sums):
            return [by] + (["(continued)"] if cont else []) + [""] + _hdr(grp, w, sums) + [""]

        def minhead():
            wm = _cols([])
            return [by, "(continued)", ""] + _hdr([], wm, True) + [""], _mcol(wm)

        gap = 0 if gi == 0 else 2
        if gi and st["u"] + gap + len(head(False, False)) + 1 > _T_PS - 1:
            newpage()
            gap = 0
        idx, cont = 0, False
        while idx < n:
            hl = len(head(cont, False))
            space = _T_PS - 1 - st["u"] - gap - hl
            if space < 1:
                newpage()
                gap, cont = 0, True
                continue
            take = min(space, n - idx)
            sums = idx + take == n and n > 1 and st["u"] + gap + hl + take + 2 <= _T_PS
            emit([""] * gap + head(cont, sums))
            gap = 0
            por = grp[idx:idx + take]
            wp = dict(w, mb=max(len(str(r["BRANCH"])) for r in por), mp=max(len(str(r["PRODUCT"])) for r in por))
            for r in por:
                obs += 1
                emit([_row(obs, r, wp)])
            idx += take
            if idx < n:
                newpage()
                cont = True
        if n > 1:
            mcur = mc
            if st["u"] + 2 > _T_PS:
                newpage()
                hl_, mcur = minhead()
                emit(hl_)
            emit([_dash(mcur), _sumln("CUSTNAME", tot, mcur)])
            if st["u"] + 2 > _T_PS:
                full = st["u"] >= _T_PS
                newpage()
                if full:
                    hl_, mcur = minhead()
                    emit(hl_)
                    emit([_dash(mcur)])
                else:
                    emit(head(True, True))
                    mcur = mc
            emit([_sumln("ICNO", tot, mcur)])
    mc = _mcol(_cols(groups[-1][1]))
    grand = sum(r["CURBAL"] for r in rows)
    out += [" " * mc + "=" * 16, " " * mc + _fnum(grand).rjust(16)]
    return out


def _write_top100_report(summary, detail, title, rdate, output_path):
    ttl = f"{title} AS AT {rdate}"
    lines = _summary_lines(summary, ttl)
    if detail is not None and detail.height > 0:
        lines += _detail_lines(detail, ttl)
    with open(output_path, "w", encoding="latin1", newline="") as fh:
        fh.write("".join(f"{ln:<{_T_LS}}\r\n" for ln in lines))


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
def run_eiimrlfm(
    bnm1_loan_cache: Path, bnm1_uloan_cache: Path, lncomm_cache: Path, provsub_txt_path: Path, lnpay_cache: Path,
    fd_fd_cache: Path, bnm_savg_cache: Path, bnm_curn_cache: Path, deposit_current_cache: Path, deposit_fd_cache: Path,
    nid_rnid_cache: Path, k1tbl_cache: Path, k3tbl_cache: Path, cisln_deposit_cache: Path, cisdp_deposit_cache: Path,
    ctx: dict, output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    inst = "PBB"     # %LET INST = 'PBB';  (as in the SAS source)

    print("  Building NOTE (loans)...")
    note = _build_note(bnm1_loan_cache, lncomm_cache, provsub_txt_path, lnpay_cache, ctx)
    print("  Building FD / SA / CA / FCYCA...")
    fd = _build_fd(fd_fd_cache, ctx)
    sa = _build_sa(bnm_savg_cache)
    ca = _build_ca(bnm_curn_cache)
    fcyca = _build_fcyca(deposit_current_cache)
    print("  Building UNOTE (undrawn portion)...")
    unote = _build_undrawn(bnm1_loan_cache, bnm1_uloan_cache, ctx)
    print("  Building NID...")
    nid = _build_nid(nid_rnid_cache, ctx)

    print("  Building KAPITI items (KALMLIQ + KALMLIFE)...")
    ktbl, dist_summary = build_kalmliq(k1tbl_cache, k3tbl_cache, ctx["reptdate"], ctx["rpyr"], ctx["rpmth"],
                                       ctx["rpday"], ctx["rd_days"], inst=inst)
    ktbl = ktbl.with_columns([pl.lit(0.0).alias("AMTHKD"), pl.lit(0.0).alias("AMTAUD")])
    k3fei = build_k3fei(k3tbl_cache, ctx["reptmon"], ctx["reptyear"])
    k3fei_norm = k3fei.rename({"ITCODE": "BNMCODE"}).with_columns([
        pl.lit(0.0).alias("AMTUSD"), pl.lit(0.0).alias("AMTSGD"),
        pl.lit(0.0).alias("AMTHKD"), pl.lit(0.0).alias("AMTAUD"),
    ]).select(["BNMCODE", "AMOUNT", "AMTUSD", "AMTSGD", "AMTHKD", "AMTAUD"])

    print("  Consolidating and summarising...")
    combined = pl.concat([
        _summarize(note), _summarize(fd), _summarize(sa), _summarize(ca),
        _summarize(unote), _summarize(fcyca), _summarize(nid),
        _summarize(ktbl), k3fei_norm,
    ], how="diagonal_relaxed")
    note_final = (
        combined
        .group_by("BNMCODE")
        .agg([
            pl.col("AMOUNT").sum(), pl.col("AMTUSD").sum(), pl.col("AMTSGD").sum(),
            pl.col("AMTHKD").sum(), pl.col("AMTAUD").sum(),
        ])
        .sort("BNMCODE")
    )

    print("  Writing FISS / NSRS...")
    fiss_path, nsrs_path = output_dir / "FISS.txt", output_dir / "NSRS.txt"
    _write_fiss_nsrs(note_final, ctx, fiss_path, nsrs_path)

    print("  Writing NLF distribution (SUPPL) report...")
    suppl_path = output_dir / "NLF_SUPPL.txt"
    _write_suppl_report(dist_summary, ctx["rdate"], suppl_path)

    print("  Building TOP 100 individual/corporate reports...")
    top_ind, top_org, det_ind, det_org = _build_top100(
        cisln_deposit_cache, cisdp_deposit_cache, deposit_current_cache, deposit_fd_cache
    )
    # FD11TEXT / FD12TEXT are temporary datasets (&&INDV / &&CORP) in the JCL.
    fd11_path, fd12_path = output_dir / "INDV.txt", output_dir / "CORP.txt"
    _write_top100_report(top_ind, det_ind, "TOP 100 LARGEST FD+CA INDIVIDUAL CUSTOMERS", ctx["rdate"], fd11_path)
    _write_top100_report(top_org, det_org, "TOP 100 LARGEST FD+CA CORPORATE CUSTOMERS", ctx["rdate"], fd12_path)

    print("EIIMRLFM complete. Outputs:")
    for p in (fiss_path, nsrs_path, suppl_path, fd11_path, fd12_path):
        print(f"  - {p}")
