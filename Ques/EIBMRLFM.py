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

from PBBLNFMT import format_liqpfmt
from PBBDPFMT import fdprod_format, ddcustcd_format
from KALMLIQ import build_kalmliq
from KALMLIFE import build_k3fei

FCY_PRODUCTS = {800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 815, 816, 817,
                851, 852, 853, 854, 855, 856, 857, 858, 859, 860}

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
            d_tmp = leap_days(yy)
            dd = d_tmp[mm - 1] if payday == 99 else payday
        else:
            dd = bldate.day
    d_final = leap_days(yy)
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
# NOTE (loans) -- BREAKDOWN BY MATURITY PROFILE (PART 1 & 2 - RM)
# ============================================================================
def _build_note(bnm1_loan_cache, lncomm_cache, provsub_txt_path, lnpay_cache, ctx) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    loan_all = con.execute(f"SELECT * FROM read_parquet('{bnm1_loan_cache.as_posix()}')").pl()
    rcloan = loan_all.filter(pl.col("PRODCD").is_in(["34190", "34690"])).sort(["ACCTNO", "COMMNO"])
    lncomm = con.execute(f"""
        SELECT ACCTNO, COMMNO, EXPIREDT AS EXPRDATE
        FROM read_parquet('{lncomm_cache.as_posix()}')
    """).pl().unique(subset=["ACCTNO", "COMMNO"], keep="first").sort(["ACCTNO", "COMMNO"])
    con.close()

    rcnote = (
        lncomm.join(rcloan.select(["ACCTNO", "COMMNO", "NOTENO"]), on=["ACCTNO", "COMMNO"], how="inner")
        .select(["ACCTNO", "NOTENO", "EXPRDATE"])
        .unique(subset=["ACCTNO", "NOTENO"], keep="first")
        .sort(["ACCTNO", "NOTENO"])
    )

    loan_sorted = loan_all.sort(["ACCTNO", "NOTENO"])

    # PROVSUB is a flat .txt file (FIRSTOBS=2, fixed columns) -- kept as .txt,
    # read via byte-offset slicing per project convention (@col 1-based -> 0-based).
    provsub_rows = []
    with open(provsub_txt_path, "r", encoding="latin1") as fh:
        for line in fh.readlines()[1:]:
            line = line.rstrip("\n")
            acctno_s, noteno_s, imloan = line[0:10].strip(), line[11:16].strip(), line[17:18].strip()
            if imloan == "Y" and acctno_s and noteno_s:
                provsub_rows.append({"ACCTNO": int(acctno_s), "NOTENO": int(noteno_s), "IMLOAN": imloan})
    provsub_schema = {"ACCTNO": pl.Int64, "NOTENO": pl.Int64, "IMLOAN": pl.Utf8}
    provsub = (pl.DataFrame(provsub_rows, schema=provsub_schema) if provsub_rows else pl.DataFrame(schema=provsub_schema)) \
        .unique(subset=["ACCTNO", "NOTENO"], keep="first")

    note = loan_sorted.join(rcnote, on=["ACCTNO", "NOTENO"], how="left").join(provsub, on=["ACCTNO", "NOTENO"], how="left")

    con = duckdb.connect(database=":memory:")
    pay_raw = con.execute(f"SELECT * FROM read_parquet('{lnpay_cache.as_posix()}')").pl()
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
            .then(pl.col("PAYAMT") * pl.col("FORATE")).otherwise(pl.col("PAYAMT")).alias("PAYAMT")
        )

    rows = []
    for r in note.iter_rows(named=True):
        paidind, eir_adj = r.get("PAIDIND"), r.get("EIR_ADJ")
        if not (paidind not in ("P", "C") or eir_adj is not None):
            continue
        prodcd, product = str(r.get("PRODCD") or ""), r.get("PRODUCT")
        if not (prodcd[:2] == "34" or product in (225, 226)):
            continue
        custcd = str(r.get("CUSTCD") or "")
        cust = "08" if custcd in ("77", "78", "95", "96") else "09"
        is_fcy = product in FCY_PRODUCTS
        acctype = r.get("ACCTYPE")

        if acctype == "OD":
            remmth = 0.1
            amount = r.get("BALANCE") or 0.0
            rows.append({"BNMCODE": f"95213{cust}{_remfmt(remmth)}0000Y", "AMOUNT": amount,
                         "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
            continue
        if acctype != "LN":
            continue

        prod = format_liqpfmt(product)
        if custcd in ("77", "78", "95", "96"):
            item = "214" if prod == "HL" else "219"
        else:
            item = "211" if prod in ("FL", "HL") else "212" if prod == "RC" else "219"

        bldate, exprdate = r.get("BLDATE"), r.get("EXPRDATE")
        loanstat, imloan = r.get("LOANSTAT"), r.get("IMLOAN")
        balance, payamt = r.get("BALANCE") or 0.0, r.get("PAYAMT") or 0.0
        payfreq, payday, issdte, currency = r.get("PAYFREQ"), r.get("PAYDAY"), r.get("ISSDTE"), r.get("CCY")
        days = (ctx["reptdate"] - bldate).days if bldate else None

        def _by_ccy(amt):
            m = {"AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0}
            if is_fcy:
                if currency == "USD":
                    m["AMTUSD"] = amt
                elif currency == "HKD":
                    m["AMTHKD"] = amt
                elif currency == "AUD":
                    m["AMTAUD"] = amt
                elif currency == "SGD":
                    m["AMTSGD"] = amt
            return m

        def _emit(prefix, amt, remmth_):
            rows.append({"BNMCODE": f"{prefix}{item}{cust}{_remfmt(remmth_)}0000Y", "AMOUNT": amt, **_by_ccy(amt)})

        remmth = 0.1
        if exprdate is not None and (exprdate - ctx["reptdate"]).days < 8:
            remmth = 0.1
        elif exprdate is not None:
            if payfreq in ("5", "9", " ", None) or product in (350, 910, 925):
                bldate = exprdate
            elif bldate is None or bldate <= date(1900, 1, 1):
                bldate = issdte
                while bldate is not None and bldate <= ctx["reptdate"]:
                    bldate = _nxtbldt(bldate, payfreq, payday)
            if payamt < 0:
                payamt = 0.0
            if bldate is None or bldate > exprdate or balance <= payamt:
                bldate = exprdate

            while bldate is not None and bldate <= exprdate:
                matdt = bldate
                remmth, _ = _remmth(ctx, matdt)
                if remmth > 12 or bldate == exprdate:
                    break
                if remmth > 0.1 and (bldate - ctx["reptdate"]).days < 8:
                    remmth = 0.1
                amount = payamt
                balance -= payamt
                _emit("94" if is_fcy else "95", amount, remmth)
                remmth13 = 13 if (days is not None and days > 89) or loanstat != 1 or imloan == "Y" else remmth
                _emit("96" if is_fcy else "93", amount, remmth13)
                bldate = _nxtbldt(bldate, payfreq, payday)
                if bldate > exprdate or balance <= amount:
                    bldate = exprdate

        amount = balance
        _emit("94" if is_fcy else "95", amount, remmth)
        remmth13 = 13 if (days is not None and days > 89) or loanstat != 1 or imloan == "Y" else remmth
        _emit("96" if is_fcy else "93", amount, remmth13)

        if eir_adj is not None:
            rows.append({"BNMCODE": f"95{item}{cust}060000Y", "AMOUNT": eir_adj, "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
            rows.append({"BNMCODE": f"93{item}{cust}060000Y", "AMOUNT": eir_adj, "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})

    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


# ============================================================================
# FIXED DEPOSITS / SAVINGS / CURRENT / VOSTRO / FCY CURRENT
# ============================================================================
def _build_fd(fd_fd_cache, ctx) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    fd = con.execute(f"""
        SELECT * FROM read_parquet('{fd_fd_cache.as_posix()}') WHERE ACCTTYPE <> 397 AND CURBAL > 0
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
        SELECT * FROM read_parquet('{bnm1_loan_cache.as_posix()}') WHERE PAIDIND NOT IN ('P','C')
    """).pl()
    lncomm = con.execute(f"SELECT ACCTNO, COMMNO FROM read_parquet('{lncomm_cache.as_posix()}')").pl().sort(["ACCTNO", "COMMNO"])
    uloan = con.execute(f"""
        SELECT * FROM read_parquet('{bnm1_uloan_cache.as_posix()}')
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
        days = (ctx["reptdate"] - bldate).days if bldate else None
        remmth13 = 13 if (days is not None and days > 89) or loanstat != 1 or imloan == "Y" else remmth
        rows.append({"BNMCODE": f"{'96' if is_fcy else '93'}{item}00{_remfmt(remmth13)}0000Y", "AMOUNT": undrawn,
                     "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


# ============================================================================
# DUAL CURRENCY INVESTMENT (DCI) / NID
# ============================================================================
def _build_dci(dciwh_dci_cache, forate_cache, foratebkp_cache, ctx) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    fdate_row = con.execute(f"SELECT REPTDATE FROM read_parquet('{forate_cache.as_posix()}') LIMIT 1").pl()
    fdate = fdate_row["REPTDATE"][0] if len(fdate_row) else None
    if fdate is not None and fdate <= ctx["reptdate"]:
        fcy = con.execute(f"SELECT * FROM read_parquet('{forate_cache.as_posix()}') ORDER BY CURCODE").pl()
    else:
        fcy = con.execute(f"""
            SELECT * FROM read_parquet('{foratebkp_cache.as_posix()}')
            WHERE REPTDATE <= DATE '{ctx["reptdate"].isoformat()}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY CURCODE ORDER BY REPTDATE DESC) = 1
        """).pl()
    dci_raw = con.execute(f"""
        SELECT * FROM read_parquet('{dciwh_dci_cache.as_posix()}') WHERE SPOTRT IS NULL OR TRUE
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
        SELECT * FROM read_parquet('{bnmk_dciwtb_cache.as_posix()}')
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
        SELECT * FROM read_parquet('{nid_rnid_cache.as_posix()}') WHERE NIDSTAT = 'N' AND CURBAL > 0
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
        SELECT *, COALESCE(NULLIF(NEWIC,''), OLDIC) AS ICNO
        FROM read_parquet('{cisln_deposit_cache.as_posix()}')
        WHERE ACCTNO BETWEEN 3000000000 AND 3999999999
    """).pl()
    cisfd = con.execute(f"""
        SELECT *, COALESCE(NULLIF(NEWIC,''), OLDIC) AS ICNO
        FROM read_parquet('{cisdp_deposit_cache.as_posix()}')
        WHERE (ACCTNO BETWEEN 1000000000 AND 1999999999) OR (ACCTNO BETWEEN 7000000000 AND 7999999999)
    """).pl()
    ca = con.execute(f"SELECT *, CURBAL AS CABAL FROM read_parquet('{deposit_current_cache.as_posix()}') WHERE CURBAL > 0").pl()
    fd = con.execute(f"SELECT *, CURBAL AS FDBAL FROM read_parquet('{deposit_fd_cache.as_posix()}') WHERE CURBAL > 0").pl()
    con.close()

    ca_excl, fd_excl = {400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411}, {350, 351, 352, 353, 354, 355, 356, 357}
    ca_j = ca.join(cisca, on="ACCTNO", how="inner").filter((pl.col("PURPOSE") != "2") & (~pl.col("PRODUCT").is_in(ca_excl)))
    ca_ind = ca_j.filter(pl.col("CUSTCODE").is_in([77, 78, 95, 96]))
    ca_org = ca_j.filter((~pl.col("CUSTCODE").is_in([77, 78, 95, 96])) & (pl.col("INDORG") == "O"))
    fd_j = cisfd.join(fd, on="ACCTNO", how="inner").filter((pl.col("PURPOSE") != "2") & (~pl.col("PRODUCT").is_in(fd_excl)))
    fd_ind = fd_j.filter(pl.col("CUSTCODE").is_in([77, 78, 95, 96]))
    fd_org = fd_j.filter((~pl.col("CUSTCODE").is_in([77, 78, 95, 96])) & (pl.col("INDORG") == "O"))

    def _top100(fd_part, ca_part, corp_excl=False):
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
    _vostro = _build_vostro(deposit_current_cache)  # LCR.VOSTRO -- passive output, no BNMCODE role downstream
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
    note_final = combined.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum(), pl.col("AMTUSD").sum(), pl.col("AMTSGD").sum(),
        pl.col("AMTHKD").sum(), pl.col("AMTAUD").sum(),
    ])

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
