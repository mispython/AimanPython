#!/usr/bin/env python3
"""
Program : EIBMRLFM.py
Purpose : New Liquidity Framework (FISS submission) -- deposit/loan
          maturity profile, undrawn commitments, DCI/NID, KAPITI items,
          distribution profile, and top-100 FD+CA depositor reports.
          Originally %INC PGM(EIBMRLFM) inside EIBMLIQP.

          Dependencies:
            PBBLNFMT.format_liqpfmt      -- PUT(PRODUCT,LIQPFMT.)
            PBBDPFMT.fdprod_format       -- PUT(INTPLAN,FDPROD.)
            PBBDPFMT.ddcustcd_format     -- PUT(CUSTCODE,DDCUSTCD.)
            KALMLIQ.build_kalmliq        -- %INC PGM(KALMLIQ)
            KALMLIFE.build_k3fei         -- %INC PGM(KALMLIFE)
          PBBELF is %INC'd in the SAS source but no PUT(var,fmt.) call
          from it appears in this program's body -- kept as comment only.

          K3FEI's only documented downstream use (per KALMLIFE.py's
          docstring) is in EIBPTH1A's SP dataset, not shown being merged
          anywhere in this program's visible SAS body. Since EIBMRLFM
          does %INC PGM(KALMLIFE), K3FEI is built here and merged
          defensively into the BNMCODE-keyed KTBL combination as the best
          available reading of the source -- flagged for verification.

          Designed to be imported by EIBMLIQP.py, mirroring %INC
          semantics. Owns no physical path of its own -- every cache path
          and REPTDATE context are supplied by the calling job.
"""
from datetime import date, timedelta
from pathlib import Path

import duckdb
import polars as pl
import time as _t

from PBBLNFMT_AII import format_liqpfmt
from PBBDPFMT_AII import fdprod_format, ddcustcd_format
from KALMLIQ import build_kalmliq
from KALMLIFE import build_k3fei

FCY_PRODUCTS = {800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 815, 816, 817,
                851, 852, 853, 854, 855, 856, 857, 858, 859, 860}

_LEAP_DAYS_CACHE = {}


def _leap_days(year: int):
    d = _LEAP_DAYS_CACHE.get(year)
    if d is None:
        d = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        if year % 4 == 0:
            d[1] = 29
        _LEAP_DAYS_CACHE[year] = d
    return d


_BNMCODE_SCHEMA = {"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTUSD": pl.Float64,
                   "AMTSGD": pl.Float64, "AMTHKD": pl.Float64, "AMTAUD": pl.Float64}

GLPROD_MAP = {
    117: "3301", 110: "3302", 108: "3303", 118: "3304", 157: "3305", 102: "3305", 101: "3306",
    121: "3307", 194: "3308", 195: "3308", 155: "3308", 192: "3308", 137: "3308", 154: "3308",
    119: "3308", 120: "3308", 138: "3308", 193: "3308", 116: "3309", 114: "3311", 85: "3311",
    86: "3311", 87: "3313", 88: "3313", 89: "3313", 91: "3313", 179: "3313", 174: "3313",
    175: "3313", 100: "3313", 156: "3313", 198: "3313", 90: "3313", 93: "3313", 180: "3313",
    197: "3313", 123: "3314", 176: "3314", 196: "3314", 112: "3315", 115: "3316", 111: "3317",
    113: "3318", 135: "3318", 189: "3318", 177: "3318", 190: "3318", 178: "3318", 122: "3319",
    109: "3320", 165: "3322", 124: "3322", 191: "3322", 159: "3323", 125: "3323", 150: "3324",
    181: "3324", 151: "3325", 152: "3326", 170: "3327", 153: "3328", 182: "3330", 183: "3330",
    160: "3330", 166: "3330", 167: "3330", 168: "3330", 169: "3330", 161: "3331", 162: "3332",
    164: "3334", 106: "7101", 158: "7101", 50: "C001", 51: "C002", 55: "C006", 56: "C007",
    65: "C008", 57: "C008", 58: "C009", 60: "CI01", 64: "CI06", 66: "CI06", 67: "CI06",
    68: "CI06", 69: "CI06", 70: "CI06", 71: "CI06", 77: "CI06", 78: "CI06", 81: "CI06",
    82: "CI06", 83: "CI06", 84: "CI06", 94: "CI06", 95: "CI06", 96: "CI06", 97: "CI06",
    131: "CI06", 132: "CI06", 133: "CI06", 134: "CI06", 184: "CI06", 40: "CI06", 41: "CI06",
    35: "CI06", 36: "CI06", 37: "CI06", 38: "CI06", 39: "CI06", 42: "CI06", 43: "CI06",
    26: "CI06", 27: "CI06", 3: "CI06", 4: "CI06", 9: "CI06", 10: "CI06", 11: "CI06", 12: "CI06",
    53: "HDA0", 63: "HDA0", 103: "HDA0", 163: "HDA0",
}


def _glprod(product) -> str:
    return GLPROD_MAP.get(product, "C999")


def _remfmt(remmth: float) -> str:
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


def _remmth(ctx: dict, matdt: date):
    """%REMMTH macro."""
    rd_days = ctx["rd_days"]
    days_in_rpmth = rd_days[ctx["rpmth"] - 1]
    mdday = min(matdt.day, days_in_rpmth)
    remy, remm = matdt.year - ctx["rpyr"], matdt.month - ctx["rpmth"]
    remd = mdday - ctx["rpday"]
    remmth = remy * 12 + remm + remd / days_in_rpmth
    rem30d = (matdt - ctx["reptdate"]).days / 30
    return remmth, rem30d


# def _nxtbldt(bldate: date, payfreq, payday) -> date:
#     """%NXTBLDT macro."""
#     def leap_days(year):
#         d = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
#         if year % 4 == 0:
#             d[1] = 29
#         return d

#     if payfreq == "6":
#         d_ = leap_days(bldate.year)
#         dd, mm, yy = bldate.day + 14, bldate.month, bldate.year
#         if dd > d_[mm - 1]:
#             dd -= d_[mm - 1]
#             mm += 1
#             if mm > 12:
#                 mm -= 12
#                 yy += 1
#     else:
#         freq = {"1": 1, "2": 3, "3": 6, "4": 12}.get(payfreq, 0)
#         mm, yy = bldate.month + freq, bldate.year
#         if mm > 12:
#             mm -= 12
#             yy += 1
#         if payday is not None:
#             d_tmp = leap_days(yy)
#             dd = d_tmp[mm - 1] if payday == 99 else payday
#         else:
#             dd = bldate.day
#     d_final = leap_days(yy)
#     if dd > d_final[mm - 1]:
#         dd = d_final[mm - 1]
#     return date(yy, mm, dd)

def _nxtbldt(bldate: date, payfreq, payday) -> date:
    """%NXTBLDT macro."""
    def leap_days(year):
        d = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        if year % 4 == 0:
            d[1] = 29
        return d

    if payfreq == "6":
        d_ = leap_days(bldate.year)
        dd, mm, yy = bldate.day + 14, bldate.month, bldate.year
        if dd > d_[mm - 1]:
            dd -= d_[mm - 1]
            mm += 1
            if mm > 12:
                mm -= 12
                yy += 1
    else:
        freq = {"1": 1, "2": 3, "3": 6, "4": 12}.get(payfreq, 0)
        mm, yy = bldate.month + freq, bldate.year
        if mm > 12:
            mm -= 12
            yy += 1
        if payday is not None:
            d_tmp = _leap_days(yy)
            dd = d_tmp[mm - 1] if payday == 99 else payday
        else:
            dd = bldate.day
    d_final = _leap_days(yy)
    if dd > d_final[mm - 1]:
        dd = d_final[mm - 1]
    return date(yy, mm, dd)


def _summarize(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema=_BNMCODE_SCHEMA)
    return df.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum(), pl.col("AMTUSD").sum(), pl.col("AMTSGD").sum(),
        pl.col("AMTHKD").sum(), pl.col("AMTAUD").sum(),
    ])


# ============================================================================
# NOTE (loans) -- BREAKDOWN BY MATURITY PROFILE (PART 1 & 2 - RM)  [VECTORISED]
# ============================================================================
_MAX_ROUNDS = 5000
_IND_CODES = [77.0, 78.0, 95.0, 96.0]
_PAYFREQ_MONTHS = {"1": 1, "2": 3, "3": 6, "4": 12}
_CCY_COLS = ("USD", "SGD", "HKD", "AUD")


def _remfmt_expr(rem: pl.Expr) -> pl.Expr:
    return (pl.when(rem <= 0.1).then(pl.lit("01"))
            .when(rem <= 1).then(pl.lit("02"))
            .when(rem <= 3).then(pl.lit("03"))
            .when(rem <= 6).then(pl.lit("04"))
            .when(rem <= 12).then(pl.lit("05"))
            .otherwise(pl.lit("06")))


def _dim_expr(y: pl.Expr, m: pl.Expr) -> pl.Expr:
    """Days in month (same leap rule as _leap_days: year % 4 == 0)."""
    return (pl.when(m == 2).then(pl.when(y % 4 == 0).then(29).otherwise(28))
            .when(m.is_in([4, 6, 9, 11])).then(30)
            .otherwise(31)).cast(pl.Int64)


def _remmth_expr(ctx: dict, d: pl.Expr) -> pl.Expr:
    """Vectorised %REMMTH (months part only)."""
    dim = ctx["rd_days"][ctx["rpmth"] - 1]
    mdday = pl.min_horizontal(d.dt.day().cast(pl.Int64), pl.lit(dim, dtype=pl.Int64))
    return ((d.dt.year().cast(pl.Int64) - ctx["rpyr"]) * 12
            + (d.dt.month().cast(pl.Int64) - ctx["rpmth"])
            + (mdday - ctx["rpday"]) / dim)


def _nxt_date(df: pl.DataFrame, cur: str = "CUR") -> pl.Series:
    """Vectorised %NXTBLDT (PAYDAY is always missing in this program)."""
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
    dd0 = (pl.when(fortnight)
           .then(pl.when(f_roll).then(f_d - _dim_expr(cy, cm)).otherwise(f_d))
           .otherwise(cd))
    dd = pl.min_horizontal(dd0, _dim_expr(yy, mm))
    return df.select(pl.date(yy, mm, dd).alias("NXT"))["NXT"]


def _emit_agg(df: pl.DataFrame, kind: str, amt: str, rem: str) -> pl.DataFrame:
    """kind 'A' -> 95 (LCY) / 94 (FCY);  kind 'B' -> 93 (LCY) / 96 (FCY)."""
    lcy, fcy = ("95", "94") if kind == "A" else ("93", "96")
    fc = pl.col("IS_FCY")
    out = df.select([
        pl.concat_str([
            pl.when(fc).then(pl.lit(fcy)).otherwise(pl.lit(lcy)),
            pl.col("ITEM"), pl.col("CUST"), _remfmt_expr(pl.col(rem)), pl.lit("0000Y"),
        ]).alias("BNMCODE"),
        pl.col(amt).alias("AMOUNT"),
        *[pl.when(fc & (pl.col("CCY") == c)).then(pl.col(amt)).otherwise(0.0).alias(f"AMT{c}")
          for c in _CCY_COLS],
    ])
    return _summarize(out)


def _emit_pair(df: pl.DataFrame, amt: str, rem: str) -> list:
    df = df.with_columns(
        pl.col(rem).alias("REM_A"),
        pl.when(pl.col("COND")).then(13.0).otherwise(pl.col(rem)).alias("REM_B"),
    )
    return [_emit_agg(df, "A", amt, "REM_A"), _emit_agg(df, "B", amt, "REM_B")]


def _amortise(loop: pl.DataFrame, ctx: dict, parts: list) -> None:
    """Instalment schedule for all loans at once (replaces the per-row while loop)."""
    rept = pl.lit(ctx["reptdate"])
    loop = loop.with_columns(
        (pl.col("PAYFREQ").is_null() | pl.col("PAYFREQ").is_in(["5", "9", " "])
         | pl.col("PRODUCT").is_in([350, 910, 925])).fill_null(False).alias("FSKIP")
    ).with_columns(
        (~pl.col("FSKIP") & (pl.col("BLDATE").is_null()
                             | (pl.col("BLDATE") <= pl.lit(date(1900, 1, 1))))).fill_null(False).alias("ROLL")
    ).with_columns(
        pl.when(pl.col("FSKIP")).then(pl.col("EXPRDATE"))
        .when(pl.col("ROLL")).then(pl.col("ISSDTE"))
        .otherwise(pl.col("BLDATE")).alias("CUR")
    )

    # bldate = issdte, then roll forward while <= reptdate (only unfinished rows are re-processed)
    pending = loop.filter(pl.col("ROLL"))
    finished = []
    for i in range(_MAX_ROUNDS):
        is_due = pl.col("CUR").is_not_null() & (pl.col("CUR") <= rept)
        fin = pending.filter(~is_due)
        if not fin.is_empty():
            finished.append(fin)
        pending = pending.filter(is_due)
        if pending.is_empty():
            break
        pending = pending.with_columns(_nxt_date(pending).alias("NXT")).with_columns(
            pl.when(pl.col("NXT") > pl.col("CUR")).then(pl.col("NXT"))
            .otherwise(pl.lit(None, dtype=pl.Date)).alias("CUR")      # cannot advance -> EXPRDATE fallback
        ).drop("NXT")
        print(f"  [roll] round {i+1}: {pending.height:,} loans still rolling")
    if not pending.is_empty():
        finished.append(pending.with_columns(pl.lit(None, dtype=pl.Date).alias("CUR")))
    rolled = pl.concat(finished, how="vertical") if finished else loop.clear()
    loop = pl.concat([loop.filter(~pl.col("ROLL")), rolled], how="vertical")

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

        last = active.filter(is_last)                       # loop break -> residual balance
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
                    | (pl.col("NXT") <= pl.col("CUR")))             # date did not advance -> stop at EXPRDATE
            .then(pl.col("EXPRDATE")).otherwise(pl.col("NXT")).alias("CUR")
        ).drop("NXT")
        print(f"  [amortise] round {_+1}: {active.height:,} loans still active")

    if not active.is_empty():                               # safety net only
        print(f"  [warn] {active.height:,} loans hit _MAX_ROUNDS; finalised at residual balance")
        active = active.with_columns(_remmth_expr(ctx, pl.col("CUR")).alias("REMM"))
        parts.extend(_emit_pair(active, "BAL", "REMM"))


def _note_to_rows(note: pl.DataFrame, ctx: dict) -> pl.DataFrame:
    rept = pl.lit(ctx["reptdate"])
    ind = pl.col("CUSTCD").cast(pl.Float64, strict=False).is_in(_IND_CODES).fill_null(False)
    num = lambda c: pl.col(c).cast(pl.Float64, strict=False).fill_nan(None).fill_null(0.0)

    df = note.select(["PRODUCT", "CUSTCD", "ACCTYPE", "BALANCE", "PAYAMT", "BLDATE", "ISSDTE",
                      "EXPRDATE", "LOANSTAT", "IMLOAN", "PAYFREQ", "CCY", "EIR_ADJ"]).with_columns(
        pl.col("PRODUCT").cast(pl.Int64, strict=False),
        ind.alias("IS_IND"),
        pl.when(ind).then(pl.lit("08")).otherwise(pl.lit("09")).alias("CUST"),
        num("BALANCE").alias("BALANCE"),
        num("PAYAMT").alias("PAYAMT"),
        pl.col("PAYFREQ").cast(pl.Utf8),
        pl.col("EIR_ADJ").cast(pl.Float64, strict=False).fill_nan(None).alias("EIR"),
        (((rept - pl.col("BLDATE")).dt.total_days() > 89).fill_null(False)
         | pl.col("LOANSTAT").ne_missing(1)
         | (pl.col("IMLOAN") == "Y").fill_null(False)).alias("COND"),
    ).with_columns(
        pl.col("PRODUCT").is_in(sorted(FCY_PRODUCTS)).fill_null(False).alias("IS_FCY"),
    )

    parts = []

    # ---- OD : 95213{cust}010000Y -------------------------------------------
    od = df.filter(pl.col("ACCTYPE") == "OD")
    parts.append(_summarize(od.select([
        pl.concat_str([pl.lit("95213"), pl.col("CUST"), pl.lit("010000Y")]).alias("BNMCODE"),
        pl.col("BALANCE").alias("AMOUNT"),
        *[pl.lit(0.0).alias(f"AMT{c}") for c in _CCY_COLS],
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
            pl.col("EIR").alias("AMOUNT"),
            *[pl.lit(0.0).alias(f"AMT{c}") for c in _CCY_COLS],
        ])))

    # No schedule needed: no expiry date, or expiring in < 8 days -> remmth = 0.1
    simple_mask = pl.col("EXPRDATE").is_null() | ((pl.col("EXPRDATE") - rept).dt.total_days() < 8)
    simple = ln.filter(simple_mask).with_columns(pl.lit(0.1).alias("REM0"))
    parts.extend(_emit_pair(simple, "BALANCE", "REM0"))

    # Instalment schedule
    _amortise(ln.filter(~simple_mask), ctx, parts)

    return _summarize(pl.concat(parts, how="vertical"))


def _build_note(bnm1_loan_cache, lncomm_cache, provsub_txt_path, lnpay_cache, ctx) -> pl.DataFrame:
    t0 = _t.time()
    con = duckdb.connect(database=":memory:")
    loan_all = con.execute(f"""
        SELECT * REPLACE (
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            CAST(NOTENO AS BIGINT) AS NOTENO,
            CAST(COMMNO AS BIGINT) AS COMMNO,
            CASE WHEN BLDATE   IS NULL OR ISNAN(BLDATE)   THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(BLDATE)   AS INTEGER) END AS BLDATE,
            CASE WHEN ISSDTE   IS NULL OR ISNAN(ISSDTE)   THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(ISSDTE)   AS INTEGER) END AS ISSDTE,
            CASE WHEN EXPRDATE IS NULL OR ISNAN(EXPRDATE) THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(EXPRDATE) AS INTEGER) END AS EXPRDATE,
            CASE WHEN APPRDATE IS NULL OR ISNAN(APPRDATE) THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(APPRDATE) AS INTEGER) END AS APPRDATE
        )
        FROM read_parquet('{bnm1_loan_cache.as_posix()}')
        WHERE (COALESCE(CAST(PAIDIND AS VARCHAR),'') NOT IN ('P','C') OR EIR_ADJ IS NOT NULL)
          AND (SUBSTR(CAST(PRODCD AS VARCHAR),1,2) = '34' OR PRODUCT IN (225, 226))
          AND CAST(ACCTYPE AS VARCHAR) IN ('OD','LN')
    """).pl()
    print(f"[timing] loan read: {_t.time()-t0:.1f}s rows={loan_all.height:,}")

    rcloan = loan_all.filter(pl.col("PRODCD").is_in(["34190", "34690"]))
    lncomm = con.execute(f"""
        SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO,
               CAST(COMMNO AS BIGINT) AS COMMNO,
               TRY_STRPTIME(
                   SUBSTR(LPAD(CAST(CAST(EXPIREDT AS BIGINT) AS VARCHAR), 11, '0'), 1, 8),
                   '%m%d%Y'
               )::DATE AS EXPRDATE
        FROM read_parquet('{lncomm_cache.as_posix()}')
    """).pl().unique(subset=["ACCTNO", "COMMNO"], keep="first", maintain_order=True).sort(["ACCTNO", "COMMNO"])

    rcnote = (
        lncomm.join(rcloan.select(["ACCTNO", "COMMNO", "NOTENO"]), on=["ACCTNO", "COMMNO"], how="inner")
        .select(["ACCTNO", "NOTENO", "EXPRDATE"])
        .unique(subset=["ACCTNO", "NOTENO"], keep="first", maintain_order=True)
    )

    # PROVSUB is a flat .txt file (FIRSTOBS=2, fixed columns)
    provsub_rows = []
    with open(provsub_txt_path, "r", encoding="latin1") as fh:
        for line in fh.readlines()[1:]:
            line = line.rstrip("\n")
            acctno_s, noteno_s, imloan = line[0:10].strip(), line[11:16].strip(), line[17:18].strip()
            if imloan == "Y" and acctno_s and noteno_s:
                provsub_rows.append({"ACCTNO": int(acctno_s), "NOTENO": int(noteno_s), "IMLOAN": imloan})
    provsub_schema = {"ACCTNO": pl.Int64, "NOTENO": pl.Int64, "IMLOAN": pl.Utf8}
    provsub = (pl.DataFrame(provsub_rows, schema=provsub_schema) if provsub_rows
               else pl.DataFrame(schema=provsub_schema)).unique(subset=["ACCTNO", "NOTENO"], keep="first")

    note = (
        loan_all
        .join(rcnote, on=["ACCTNO", "NOTENO"], how="left", suffix="_rc")
        .with_columns(pl.coalesce([pl.col("EXPRDATE_rc"), pl.col("EXPRDATE")]).alias("EXPRDATE"))
        .drop("EXPRDATE_rc")
        .join(provsub, on=["ACCTNO", "NOTENO"], how="left")
    )

    pay_raw = con.execute(f"""
        SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(NOTENO AS BIGINT) AS NOTENO, PAYAMT,
               CASE WHEN EFFDATE IS NULL OR ISNAN(EFFDATE) THEN NULL
                    ELSE DATE '1960-01-01' + CAST(FLOOR(EFFDATE) AS INTEGER) END AS EFFDATE
        FROM read_parquet('{lnpay_cache.as_posix()}')
    """).pl()
    con.close()
    tdate = ctx["reptdate"]
    pay = pay_raw.with_columns([
        pl.when(pl.col("EFFDATE") <= tdate).then(1).otherwise(0).alias("SORT_IND"),
        pl.when(pl.col("EFFDATE") <= tdate).then(pl.col("EFFDATE").cast(pl.Int64))
          .otherwise(-pl.col("EFFDATE").cast(pl.Int64)).alias("MANI_EFFDATE"),
    ]).sort(
        ["ACCTNO", "NOTENO", "PAYAMT", "SORT_IND", "MANI_EFFDATE"],
        descending=[False, False, False, True, True],
    ).unique(subset=["ACCTNO", "NOTENO", "PAYAMT"], keep="first").select(["ACCTNO", "NOTENO", "PAYAMT"])

    note = note.join(pay, on=["ACCTNO", "NOTENO", "PAYAMT"], how="left")
    if "FORATE" in note.columns:
        note = note.with_columns(
            pl.when(pl.col("PRODUCT").is_between(800, 899))
            .then(pl.col("PAYAMT").cast(pl.Float64, strict=False) * pl.col("FORATE").cast(pl.Float64, strict=False))
            .otherwise(pl.col("PAYAMT").cast(pl.Float64, strict=False))
            .alias("PAYAMT")
        )
    print(f"[timing] prep joins: {_t.time()-t0:.1f}s")

    result = _note_to_rows(note, ctx)
    print(f"[timing] NOTE total: {_t.time()-t0:.1f}s  codes={result.height:,}")
    return result


# ============================================================================
# FIXED DEPOSITS / SAVINGS / CURRENT / VOSTRO / FCY CURRENT
# ============================================================================
def _build_fd(fd_fd_cache, ctx) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    fd = con.execute(f"""
        SELECT * REPLACE (
            CASE WHEN MATDATE IS NULL OR ISNAN(MATDATE) THEN NULL
                 ELSE STRPTIME(CAST(CAST(MATDATE AS BIGINT) AS VARCHAR), '%Y%m%d')::DATE END AS MATDATE
        )
        FROM read_parquet('{fd_fd_cache.as_posix()}') WHERE ACCTTYPE <> 397 AND CURBAL > 0
    """).pl()
    con.close()
    rows = []
    for r in fd.iter_rows(named=True):
        cust = "08" if r.get("CUSTCD") in (77, 78, 95, 96) else "09"
        matdt, openind = r.get("MATDATE"), r.get("OPENIND")
        if openind == "D" or (matdt is not None and (matdt - ctx["reptdate"]).days < 8):
            remmth = 0.1
        else:
            remmth, _ = _remmth(ctx, matdt)
        bic = fdprod_format(r.get("INTPLAN"))
        curbal, curcode = r.get("CURBAL") or 0.0, r.get("CURCODE")
        amtusd = amtsgd = amthkd = amtaud = 0.0
        if bic == "42630":
            if curcode == "USD":
                amtusd = curbal
            elif curcode == "SGD":
                amtsgd = curbal
            elif curcode == "HKD":
                amthkd = curbal
            elif curcode == "AUD":
                amtaud = curbal
            bnmcode = f"96311{cust}{_remfmt(remmth)}0000Y"
        elif bic == "42132":
            bnmcode = f"95315{cust}{_remfmt(remmth)}0000Y"
        else:
            bnmcode = f"95311{cust}{_remfmt(remmth)}0000Y"
        if r.get("ACCTTYPE") in (315, 394):
            bnmcode = f"95315{cust}{_remfmt(remmth)}0000Y"
        rows.append({"BNMCODE": bnmcode, "AMOUNT": curbal, "AMTUSD": amtusd, "AMTSGD": amtsgd, "AMTHKD": amthkd, "AMTAUD": amtaud})
    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


def _build_sa(bnm_savg_cache) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    sa = con.execute(f"SELECT * FROM read_parquet('{bnm_savg_cache.as_posix()}')").pl()
    con.close()
    return sa.with_columns(
        pl.when(pl.col("CUSTCD").is_in(["77", "78", "95", "96"])).then(pl.lit("08")).otherwise(pl.lit("09")).alias("CUST")
    ).select([
        (pl.lit("95312") + pl.col("CUST") + pl.lit("010000Y")).alias("BNMCODE"),
        pl.col("CURBAL").alias("AMOUNT"),
        pl.lit(0.0).alias("AMTUSD"), pl.lit(0.0).alias("AMTSGD"),
        pl.lit(0.0).alias("AMTHKD"), pl.lit(0.0).alias("AMTAUD"),
    ])


def _build_ca(bnm_curn_cache) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    ca_raw = con.execute(f"SELECT * FROM read_parquet('{bnm_curn_cache.as_posix()}')").pl()
    con.close()
    rows = []
    for r in ca_raw.iter_rows(named=True):
        if _glprod(r.get("PRODUCT")) == "C999":
            continue
        if str(r.get("PRODCD") or "")[:3] not in ("421", "423"):
            continue
        cust = "08" if r.get("CUSTCD") in ("77", "78", "95", "96") else "09"
        rows.append({"BNMCODE": f"95313{cust}010000Y", "AMOUNT": r.get("CURBAL"),
                     "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


def _build_vostro(deposit_current_cache) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    vostro = con.execute(f"""
        SELECT BRANCH, ACCTNO, CURBAL AS AMOUNT, CURCODE, CUSTCD, PRODUCT
        FROM read_parquet('{deposit_current_cache.as_posix()}') WHERE PRODUCT IN (104, 105, 147)
    """).pl()
    con.close()
    return vostro


def _build_fcyca(deposit_current_cache) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT * FROM read_parquet('{deposit_current_cache.as_posix()}')
        WHERE PRODUCT BETWEEN 400 AND 444 AND PRODUCT <> 413
    """).pl()
    con.close()
    rows = []
    for r in raw.iter_rows(named=True):
        custcd = ddcustcd_format(r.get("CUSTCODE"))
        cust = "08" if custcd in ("77", "78", "95", "96") else "09"
        product, curbal = r.get("PRODUCT"), r.get("CURBAL") or 0.0
        rows.append({
            "BNMCODE": f"96313{cust}010000Y", "AMOUNT": curbal,
            "AMTUSD": curbal if product in (400, 420, 440) else 0.0,
            "AMTSGD": curbal if product in (403, 423) else 0.0,
            "AMTHKD": curbal if product in (406, 426) else 0.0,
            "AMTAUD": curbal if product in (402, 422, 442) else 0.0,
        })
    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


# ============================================================================
# UNDRAWN PORTION (RC facilities)
# ============================================================================
def _build_undrawn(bnm1_loan_cache, bnm1_uloan_cache, lncomm_cache, ctx) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    loan_all = con.execute(f"""
        SELECT * REPLACE (
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            CAST(NOTENO AS BIGINT) AS NOTENO,
            CAST(COMMNO AS BIGINT) AS COMMNO,
            CASE WHEN BLDATE   IS NULL OR ISNAN(BLDATE)   THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(BLDATE)   AS INTEGER) END AS BLDATE,
            CASE WHEN ISSDTE   IS NULL OR ISNAN(ISSDTE)   THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(ISSDTE)   AS INTEGER) END AS ISSDTE,
            CASE WHEN EXPRDATE IS NULL OR ISNAN(EXPRDATE) THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(EXPRDATE) AS INTEGER) END AS EXPRDATE,
            CASE WHEN APPRDATE IS NULL OR ISNAN(APPRDATE) THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(APPRDATE) AS INTEGER) END AS APPRDATE
        )
        FROM read_parquet('{bnm1_loan_cache.as_posix()}') WHERE PAIDIND NOT IN ('P','C')
    """).pl()
    lncomm = con.execute(f"""
        SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO,
               CAST(COMMNO AS BIGINT) AS COMMNO
        FROM read_parquet('{lncomm_cache.as_posix()}')
    """).pl().sort(["ACCTNO", "COMMNO"])
    uloan = con.execute(f"""
        SELECT * REPLACE (
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            CASE WHEN ISSDTE   IS NULL OR ISNAN(ISSDTE)   THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(ISSDTE)   AS INTEGER) END AS ISSDTE,
            CASE WHEN EXPRDATE IS NULL OR ISNAN(EXPRDATE) THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(EXPRDATE) AS INTEGER) END AS EXPRDATE,
            CASE WHEN APPRDATE IS NULL OR ISNAN(APPRDATE) THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(APPRDATE) AS INTEGER) END AS APPRDATE
        )
        FROM read_parquet('{bnm1_uloan_cache.as_posix()}')
        WHERE NOT (ACCTNO BETWEEN 3000000000 AND 3999999999 AND PRODUCT IN (151,152,181) AND ACCTYPE = 'OD')
    """).pl().sort(["ACCTNO"])
    con.close()

    alw = loan_all.filter(~((pl.col("PRODUCT").is_in([151, 152, 181])) & (pl.col("ACCTYPE") == "OD"))).sort(["ACCTNO", "NOTENO"])
    alwcom = alw.filter(pl.col("COMMNO") > 0).sort(["ACCTNO", "COMMNO"])
    rc_mask_com = pl.col("PRODCD").is_in(["34190", "34690"])
    appr = pl.concat([
        alwcom.filter(rc_mask_com).unique(subset=["ACCTNO", "COMMNO"], keep="first"),
        alwcom.filter(~rc_mask_com),
    ], how="diagonal_relaxed")

    alwnocom = alw.filter(pl.col("COMMNO") <= 0).sort(["ACCTNO", "APPRLIM2"])
    rc_mask = pl.col("PRODCD").is_in(["34190", "34690"])
    alwnocom_rc, alwnocom_other = alwnocom.filter(rc_mask), alwnocom.filter(~rc_mask)
    appr1_rc = alwnocom_rc.unique(subset=["ACCTNO", "APPRLIM2"], keep="first")
    dup_keys = alwnocom_rc.join(appr1_rc.select(["ACCTNO", "APPRLIM2"]), on=["ACCTNO", "APPRLIM2"], how="anti")
    dupli = dup_keys.filter(pl.col("BALANCE") >= pl.col("APPRLIM2"))
    appr1_final = pl.concat([appr1_rc, dupli, alwnocom_other], how="diagonal_relaxed")

    combined = pl.concat([appr, appr1_final], how="diagonal_relaxed").sort("ACCTNO")
    combined = pl.concat([combined, uloan], how="diagonal_relaxed").sort("ACCTNO")

    rows = []
    for r in combined.iter_rows(named=True):
        prodcd, product = str(r.get("PRODCD") or ""), r.get("PRODUCT")
        if not (prodcd[:2] == "34" or product in (225, 226)):
            continue
        acctype, exprdate, apprdate = r.get("ACCTYPE"), r.get("EXPRDATE"), r.get("APPRDATE")
        if acctype == "LN":
            matdt, item = exprdate, ("424" if prodcd in ("34190", "34690") else "429")
        else:
            matdt, item = (apprdate + timedelta(days=365) if apprdate else None), "423"
        if prodcd == "34240":
            item = "429"
        if matdt is not None and (matdt - ctx["reptdate"]).days < 8:
            remmth = 0.1
        elif matdt is not None:
            remmth, _ = _remmth(ctx, matdt)
        else:
            remmth = 0.1
        undrawn = r.get("UNDRAWN") or 0.0
        is_fcy = product in FCY_PRODUCTS
        rows.append({"BNMCODE": f"{'94' if is_fcy else '95'}{item}00{_remfmt(remmth)}0000Y", "AMOUNT": undrawn,
                     "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
        bldate, loanstat, imloan = r.get("BLDATE"), r.get("LOANSTAT"), r.get("IMLOAN")
        # days = (ctx["reptdate"] - bldate).days if bldate else None
        days = (ctx["reptdate"] - bldate).days if bldate is not None else None
        remmth13 = 13 if (days is not None and days > 89) or loanstat != 1 or imloan == "Y" else remmth
        rows.append({"BNMCODE": f"{'96' if is_fcy else '93'}{item}00{_remfmt(remmth13)}0000Y", "AMOUNT": undrawn,
                     "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


# ============================================================================
# DUAL CURRENCY INVESTMENT (DCI) / NID
# ============================================================================
def _build_dci(dciwh_dci_cache, forate_cache, foratebkp_cache, ctx) -> pl.DataFrame:
    # con = duckdb.connect(database=":memory:")
    # fdate_row = con.execute(f"SELECT REPTDATE FROM read_parquet('{forate_cache.as_posix()}') LIMIT 1").pl()
    # fdate = fdate_row["REPTDATE"][0] if len(fdate_row) else None
    con = duckdb.connect(database=":memory:")
    fdate_row = con.execute(f"""
        SELECT DATE '1960-01-01' + CAST(FLOOR(REPTDATE) AS INTEGER) AS REPTDATE
        FROM read_parquet('{forate_cache.as_posix()}') LIMIT 1
    """).pl()
    fdate = fdate_row["REPTDATE"][0] if len(fdate_row) else None
    if fdate is not None and fdate <= ctx["reptdate"]:
        fcy = con.execute(f"SELECT * FROM read_parquet('{forate_cache.as_posix()}') ORDER BY CURCODE").pl()
    else:
        # fcy = con.execute(f"""
        #     SELECT * FROM read_parquet('{foratebkp_cache.as_posix()}')
        #     WHERE REPTDATE <= DATE '{ctx["reptdate"].isoformat()}'
        #     QUALIFY ROW_NUMBER() OVER (PARTITION BY CURCODE ORDER BY REPTDATE DESC) = 1
        # """).pl()
        fcy = con.execute(f"""
            SELECT * REPLACE (
                DATE '1960-01-01' + CAST(FLOOR(REPTDATE) AS INTEGER) AS REPTDATE
            )
            FROM read_parquet('{foratebkp_cache.as_posix()}')
            WHERE (DATE '1960-01-01' + CAST(FLOOR(REPTDATE) AS INTEGER))
                  <= DATE '{ctx["reptdate"].isoformat()}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CURCODE
                ORDER BY (DATE '1960-01-01' + CAST(FLOOR(REPTDATE) AS INTEGER)) DESC
            ) = 1
        """).pl()
    dci_raw = con.execute(f"""
        SELECT * REPLACE (
            CASE WHEN MATDT IS NULL OR ISNAN(MATDT) THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(MATDT) AS INTEGER) END AS MATDT,
            CASE WHEN STARTDT IS NULL OR ISNAN(STARTDT) THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(STARTDT) AS INTEGER) END AS STARTDT
        )
        FROM read_parquet('{dciwh_dci_cache.as_posix()}') WHERE SPOTRT IS NULL OR TRUE
    """).pl()
    con.close()
    fcy_rate = {r["CURCODE"]: r["SPOTRATE"] for r in fcy.iter_rows(named=True)}

    rows = []
    for r in dci_raw.iter_rows(named=True):
        matdt, startdt = r.get("MATDT"), r.get("STARTDT")
        if not (matdt is not None and startdt is not None and matdt > ctx["reptdate"] and startdt <= ctx["reptdate"]):
            continue
        if (matdt - ctx["reptdate"]).days < 8:
            remmth = 0.1
        else:
            remmth, _ = _remmth(ctx, matdt)
        invcurr, invamt = r.get("INVCURR"), r.get("INVAMT") or 0.0
        if invcurr == "MYR":
            amount = invamt
            for code in ("9332900", "9532900"):
                rows.append({"BNMCODE": f"{code}{_remfmt(remmth)}0000Y", "AMOUNT": amount,
                             "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
        else:
            spotrt = fcy_rate.get(invcurr, 0.0)
            invamt2 = round(invamt) if invcurr == "JPY" else round(invamt, 2)
            amount = invamt2 * spotrt
            amtusd, amtsgd = (amount if invcurr == "USD" else 0.0), (amount if invcurr == "SGD" else 0.0)
            amthkd, amtaud = (amount if invcurr == "HKD" else 0.0), (amount if invcurr == "AUD" else 0.0)
            for code in ("9432900", "9632900"):
                rows.append({"BNMCODE": f"{code}{_remfmt(remmth)}0000Y", "AMOUNT": amount,
                             "AMTUSD": amtusd, "AMTSGD": amtsgd, "AMTHKD": amthkd, "AMTAUD": amtaud})
    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


def _build_dciw(bnmk_dciwtb_cache, ctx) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT * REPLACE (
            CASE WHEN DCMTYD IS NULL OR ISNAN(DCMTYD) THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(DCMTYD) AS INTEGER) END AS DCMTYD
        )
        FROM read_parquet('{bnmk_dciwtb_cache.as_posix()}')
        WHERE DCDLP = 'DCI' AND DCBSI = 'S' AND DCTRNT = 'I'
    """).pl()
    con.close()
    rows = []
    for r in raw.iter_rows(named=True):
        matdt = r.get("DCMTYD")
        if (matdt - ctx["reptdate"]).days < 8:
            remmth = 0.1
        else:
            remmth, _ = _remmth(ctx, matdt)
        dcbccy, dcbamt, c8spt = r.get("DCBCCY"), r.get("DCBAMT") or 0.0, r.get("C8SPT") or 0.0
        if dcbccy == "MYR":
            amount = dcbamt
            for code in ("9392100", "9592100", "9472200", "9672200"):
                rows.append({"BNMCODE": f"{code}{_remfmt(remmth)}0000Y", "AMOUNT": amount,
                             "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
        else:
            dcbamt2 = round(dcbamt) if dcbccy == "JPY" else round(dcbamt, 2)
            amount = dcbamt2 * c8spt
            amtusd, amtsgd = (amount if dcbccy == "USD" else 0.0), (amount if dcbccy == "SGD" else 0.0)
            amthkd, amtaud = (amount if dcbccy == "HKD" else 0.0), (amount if dcbccy == "AUD" else 0.0)
            for code in ("9492200", "9692200", "9372100", "9572100"):
                rows.append({"BNMCODE": f"{code}{_remfmt(remmth)}0000Y", "AMOUNT": amount,
                             "AMTUSD": amtusd, "AMTSGD": amtsgd, "AMTHKD": amthkd, "AMTAUD": amtaud})
    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


def _build_nid(nid_rnid_cache, ctx) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT * REPLACE (
            CASE WHEN MATDT IS NULL OR ISNAN(MATDT) THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(MATDT) AS INTEGER) END AS MATDT,
            CASE WHEN STARTDT IS NULL OR ISNAN(STARTDT) THEN NULL
                 ELSE DATE '1960-01-01' + CAST(FLOOR(STARTDT) AS INTEGER) END AS STARTDT
        )
        FROM read_parquet('{nid_rnid_cache.as_posix()}') WHERE NIDSTAT = 'N' AND CURBAL > 0
    """).pl()
    con.close()
    rows = []
    for r in raw.iter_rows(named=True):
        matdt, startdt = r.get("MATDT"), r.get("STARTDT")
        if not (matdt is not None and startdt is not None and matdt > ctx["reptdate"] and startdt <= ctx["reptdate"]):
            continue
        remmth = 0.1 if (matdt - ctx["reptdate"]).days < 8 else _remmth(ctx, matdt)[0]
        amount = r.get("CURBAL") or 0.0
        for code in ("9384000", "9584000"):
            rows.append({"BNMCODE": f"{code}{_remfmt(remmth)}0000Y", "AMOUNT": amount,
                         "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


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
                    return int(round(abs(v) / divisor))
                fh.write(f"{r['BNMCODE']:<14};{_p(r['AMOUNT'])};{_p(r['AMTUSD'])};{_p(r['AMTSGD'])};{_p(r['AMTHKD'])};{_p(r['AMTAUD'])}\n")
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
    """).pl()
    cisfd = con.execute(f"""
        SELECT * REPLACE (CAST(ACCTNO AS BIGINT) AS ACCTNO),
               COALESCE(NULLIF(NEWIC,''), OLDIC) AS ICNO
        FROM read_parquet('{cisdp_deposit_cache.as_posix()}')
        WHERE (ACCTNO BETWEEN 1000000000 AND 1999999999) OR (ACCTNO BETWEEN 7000000000 AND 7999999999)
    """).pl()
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

    ca_excl, fd_excl = {400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411}, {350, 351, 352, 353, 354, 355, 356, 357}
    ca_j = ca.join(cisca, on="ACCTNO", how="inner").filter((pl.col("PURPOSE") != "2") & (~pl.col("PRODUCT").is_in(ca_excl)))
    ca_ind = ca_j.filter(pl.col("CUSTCODE").is_in([77, 78, 95, 96]))
    ca_org = ca_j.filter((~pl.col("CUSTCODE").is_in([77, 78, 95, 96])) & (pl.col("INDORG") == "O"))
    fd_j = cisfd.join(fd, on="ACCTNO", how="inner").filter((pl.col("PURPOSE") != "2") & (~pl.col("ACCTTYPE").is_in(fd_excl)))
    fd_ind = fd_j.filter(pl.col("CUSTCD").is_in([77.0, 78.0, 95.0, 96.0]))
    fd_org = fd_j.filter((~pl.col("CUSTCD").is_in([77.0, 78.0, 95.0, 96.0])) & (pl.col("INDORG") == "O"))


    def _top100(fd_part, ca_part, corp_excl=False):
        if "CABAL" not in fd_part.columns:
            fd_part = fd_part.with_columns(pl.lit(0.0).alias("CABAL"))
        if "FDBAL" not in ca_part.columns:
            ca_part = ca_part.with_columns(pl.lit(0.0).alias("FDBAL"))
        common = [c for c in fd_part.columns if c in ca_part.columns]
        data1 = pl.concat([fd_part.select(common), ca_part.select(common)]).with_columns(
            pl.when(pl.col("ICNO").is_null() | (pl.col("ICNO") == "")).then(pl.lit("XX")).otherwise(pl.col("ICNO")).alias("ICNO")
        )
        if corp_excl:
            data1 = data1.filter(~(
                pl.col("ACCTNO").is_between(1590000000, 1599999999)
                | pl.col("ACCTNO").is_between(1689999999, 1699999999)
                | pl.col("ACCTNO").is_between(1789999999, 1799999999)
            ))
        return (
            data1.filter(pl.col("ICNO") != "")
            .group_by(["ICNO", "CUSTNAME"])
            .agg([pl.col("CURBAL").sum(), pl.col("FDBAL").sum(), pl.col("CABAL").sum()])
            .sort("CURBAL", descending=True)
            .head(100)
        )

    return _top100(fd_ind, ca_ind), _top100(fd_org, ca_org, corp_excl=True)


def _write_top100_report(summary, title, rdate, output_path):
    lines = [f"{title} AS AT {rdate}", ""]
    tot_cur = tot_fd = tot_ca = 0.0
    for r in summary.iter_rows(named=True):
        curbal, fdbal, cabal = r.get("CURBAL") or 0.0, r.get("FDBAL") or 0.0, r.get("CABAL") or 0.0
        lines.append(f"{str(r['CUSTNAME'])[:30]:<30}{curbal:>18,.2f}{fdbal:>18,.2f}{cabal:>18,.2f}")
        tot_cur, tot_fd, tot_ca = tot_cur + curbal, tot_fd + fdbal, tot_ca + cabal
    lines.append(f"{'TOTAL':<30}{tot_cur:>18,.2f}{tot_fd:>18,.2f}{tot_ca:>18,.2f}")
    with open(output_path, "w", encoding="latin1") as fh:
        fh.write("\n".join(lines) + "\n")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
def run_eibmrlfm(
    bnm1_loan_cache: Path, bnm1_uloan_cache: Path, lncomm_cache: Path, provsub_txt_path: Path, lnpay_cache: Path,
    fd_fd_cache: Path, bnm_savg_cache: Path, bnm_curn_cache: Path, deposit_current_cache: Path, deposit_fd_cache: Path,
    forate_cache: Path, foratebkp_cache: Path, dciwh_dci_cache: Path, bnmk_dciwtb_cache: Path, nid_rnid_cache: Path,
    k1tbl_cache: Path, k3tbl_cache: Path, cisln_deposit_cache: Path, cisdp_deposit_cache: Path,
    ctx: dict, output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    inst = "PBB"

    print("  Building NOTE (loans)...")
    note = _build_note(bnm1_loan_cache, lncomm_cache, provsub_txt_path, lnpay_cache, ctx)
    print("  Building FD / SA / CA / VOSTRO / FCYCA...")
    fd = _build_fd(fd_fd_cache, ctx)
    sa = _build_sa(bnm_savg_cache)
    ca = _build_ca(bnm_curn_cache)
    _vostro = _build_vostro(bnm_curn_cache)  # LCR.VOSTRO -- passive output, no BNMCODE role downstream
    fcyca = _build_fcyca(deposit_current_cache)
    print("  Building UNOTE (undrawn portion)...")
    unote = _build_undrawn(bnm1_loan_cache, bnm1_uloan_cache, lncomm_cache, ctx)
    print("  Building DCI / DCIW / NID...")
    dci = pl.concat([_build_dci(dciwh_dci_cache, forate_cache, foratebkp_cache, ctx),
                      _build_dciw(bnmk_dciwtb_cache, ctx)], how="diagonal_relaxed")
    nid = _build_nid(nid_rnid_cache, ctx)

    print("  Building KAPITI items (KALMLIQ + KALMLIFE)...")
    ktbl, dist_summary = build_kalmliq(k1tbl_cache, k3tbl_cache, ctx["reptdate"], ctx["rpyr"], ctx["rpmth"], ctx["rpday"], ctx["rd_days"], inst=inst)
    ktbl = ktbl.with_columns([pl.lit(0.0).alias("AMTHKD"), pl.lit(0.0).alias("AMTAUD")])
    k3fei = build_k3fei(k3tbl_cache, ctx["reptmon"], ctx["reptyear"])
    k3fei_norm = k3fei.rename({"ITCODE": "BNMCODE"}).with_columns([
        pl.lit(0.0).alias("AMTUSD"), pl.lit(0.0).alias("AMTSGD"),
        pl.lit(0.0).alias("AMTHKD"), pl.lit(0.0).alias("AMTAUD"),
    ]).select(["BNMCODE", "AMOUNT", "AMTUSD", "AMTSGD", "AMTHKD", "AMTAUD"])

    print("  Consolidating and summarising...")
    combined = pl.concat([
        _summarize(note), _summarize(fd), _summarize(sa), _summarize(ca),
        _summarize(fcyca), _summarize(unote), _summarize(dci), _summarize(nid),
        _summarize(ktbl), k3fei_norm,
    ], how="diagonal_relaxed")
    note_final = (
        combined
        .group_by("BNMCODE")
        .agg([
            pl.col("AMOUNT").sum(),pl.col("AMTUSD").sum(),pl.col("AMTSGD").sum(),
            pl.col("AMTHKD").sum(),pl.col("AMTAUD").sum(),
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
    top_ind, top_org = _build_top100(cisln_deposit_cache, cisdp_deposit_cache, deposit_current_cache, deposit_fd_cache)
    fd11_path, fd12_path = output_dir / "INDTOP50.txt", output_dir / "CORTOP50.txt"
    _write_top100_report(top_ind, "TOP 100 LARGEST FD+CA INDIVIDUAL CUSTOMERS", ctx["rdate"], fd11_path)
    _write_top100_report(top_org, "TOP 100 LARGEST FD+CA CORPORATE CUSTOMERS", ctx["rdate"], fd12_path)

    print("EIBMRLFM complete. Outputs:")
    for p in (fiss_path, nsrs_path, suppl_path, fd11_path, fd12_path):
        print(f"  - {p}")
