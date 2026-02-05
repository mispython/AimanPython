#!/usr/bin/env python3
"""
File Name: EIBMNLF0

Assumptions:
- Source SAS datasets are available as parquet files.
- Column names in parquet match the SAS variable names used by the program.
- Final output is a text-friendly table, written as parquet and .txt.
"""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import math

import duckdb
import polars as pl


# =============================================================================
# PATHS AND CONFIGURATION (declared early per requirement)
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"

REPTDATE_PATH = INPUT_DIR / "LOAN_REPTDATE.parquet"
LOAN_TEMPLATE = INPUT_DIR / "BNM1_LOAN_{reptmon}{nowk}.parquet"

OUTPUT_NOTE_PARQUET = OUTPUT_DIR / "BNM_NOTE.parquet"
OUTPUT_NOTE_TXT = OUTPUT_DIR / "BNM_NOTE.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# CONSTANTS
# =============================================================================
FCY_PRODUCTS = {
    800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814,
    851, 852, 853, 854, 855, 856, 857, 858, 859, 860,
}

HL_PRODUCTS = {
    4, 5, 6, 7, 31, 32, 100, 101, 102, 103, 110, 111, 112, 113, 114, 115,
    116, 170, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
    225, 226, 227, 228, 229, 230, 231, 232, 233, 234,
}
RC_PRODUCTS = {350, 910, 925}


@dataclass(frozen=True)
class ReportContext:
    reptdate: date
    reptyr: int
    reptmon: str
    nowk: str


def _to_date(v: object) -> date:
    if isinstance(v, date):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return datetime.fromisoformat(v).date()
    raise ValueError(f"Unsupported date value: {v!r}")


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


def _liq_prod(product: int | None) -> str:
    if product in HL_PRODUCTS:
        return "HL"
    if product in RC_PRODUCTS:
        return "RC"
    return "FL"


def _is_missing(x: object) -> bool:
    if x is None:
        return True
    if isinstance(x, float):
        return math.isnan(x)
    return False


def read_context() -> ReportContext:
    print("BASE_DIR =", BASE_DIR)
    print("REPTDATE_PATH =", REPTDATE_PATH)
    print("Exists?", REPTDATE_PATH.exists(), "\n")

    rept = pl.read_parquet(REPTDATE_PATH).select("REPTDATE").item(0, 0)
    reptdate = _to_date(rept)
    if reptdate.day == 8:
        nowk = "1"
    elif reptdate.day == 15:
        nowk = "2"
    elif reptdate.day == 22:
        nowk = "3"
    else:
        nowk = "4"
    return ReportContext(
        reptdate=reptdate,
        reptyr=reptdate.year,
        reptmon=f"{reptdate.month:02d}",
        nowk=nowk,
    )


def _next_bldate(bldate: date, issdte: date, payfreq: str, freq: int) -> date:
    if payfreq == "6":
        return bldate + timedelta(days=14)

    dd = issdte.day
    mm = bldate.month + freq
    yy = bldate.year
    while mm > 12:
        mm -= 12
        yy += 1
    dd = min(dd, monthrange(yy, mm)[1])
    return date(yy, mm, dd)


def _remmth(matdt: date, reptdate: date) -> float:
    rpyr = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rpdays = monthrange(rpyr, rpmth)[1]

    mdyr = matdt.year
    mdmth = matdt.month
    mdday = min(matdt.day, rpdays)

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / rpdays


def _default_codes() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    stems = [
        "93211090", "95211090", "93212090", "95212090", "93213080", "95213080",
        "93213090", "95213090", "93214080", "95214080", "93215080", "95215080",
        "93219080", "95219080", "93219090", "95219090",
        "94211090", "96211090", "94212090", "96212090", "94213080", "96213080",
        "94213090", "96213090", "94214080", "96214080", "94215080", "96215080",
        "94219080", "96219080", "94219090", "96219090",
    ]
    for n in range(1, 7):
        suffix = f"{n}0000Y"
        for stem in stems:
            rows.append({"BNMCODE": f"{stem}{suffix}", "AMOUNT": 0.0})
    return rows


def process() -> pl.DataFrame:
    ctx = read_context()
    loan_path = LOAN_TEMPLATE.format(reptmon=ctx.reptmon, nowk=ctx.nowk)

    conn = duckdb.connect()
    query = f"""
        SELECT *
        FROM read_parquet('{loan_path.as_posix()}')
        WHERE (PAIDIND NOT IN ('P','C') OR EIR_ADJ IS NOT NULL)
          AND (substr(PRODCD, 1, 2) = '34' OR PRODUCT IN (225,226))
    """
    loan_df = conn.execute(query).pl()

    reptdate = ctx.reptdate
    note_rows: list[dict[str, object]] = []

    for r in loan_df.iter_rows(named=True):
        cust = "08" if str(r.get("CUSTCD", "")) in {"77", "78", "95", "96"} else "09"
        acctype = str(r.get("ACCTYPE", "") or "")

        balance = float(r.get("BALANCE") or 0.0)
        eir_adj = r.get("EIR_ADJ")
        product = r.get("PRODUCT")
        prodcd = str(r.get("PRODCD", "") or "")
        loanstat = int(r.get("LOANSTAT") or 0)
        payamt = float(r.get("PAYAMT") or 0.0)
        payfreq = str(r.get("PAYFREQ", "") or "")

        bldate = r.get("BLDATE")
        issdte = r.get("ISSDTE")
        exprdate = r.get("EXPRDATE")

        bldate = _to_date(bldate) if not _is_missing(bldate) else None
        issdte = _to_date(issdte) if not _is_missing(issdte) else None
        exprdate = _to_date(exprdate) if not _is_missing(exprdate) else None
        if exprdate is None or issdte is None:
            continue

        if acctype == "OD":
            rem = 0.1
            item = "213"
            if prodcd == "34240":
                item = "219"
            note_rows.append({"BNMCODE": f"952{item}{cust}{_remfmt(rem)}0000Y", "AMOUNT": balance})
            continue

        if acctype != "LN":
            continue

        prod = _liq_prod(product)
        if cust == "08":
            item = "214" if prod == "HL" else "219"
        else:
            if prod in {"FL", "HL"}:
                item = "211"
            elif prod == "RC":
                item = "212"
            else:
                item = "219"

        days = (reptdate - bldate).days if bldate else 0

        if (exprdate - reptdate).days < 8:
            rem = 0.1
        else:
            freq = {"1": 1, "2": 3, "3": 6, "4": 12}.get(payfreq, 0)

            if payfreq in {"5", "9", " "} or product in {350, 910, 925}:
                bldate = exprdate
            elif bldate is None:
                bldate = issdte
                while bldate <= reptdate:
                    bldate = _next_bldate(bldate, issdte, payfreq, freq)

            if payamt < 0:
                payamt = 0.0
            if bldate > exprdate or balance <= payamt:
                bldate = exprdate

            while bldate <= exprdate:
                rem = _remmth(bldate, reptdate)
                if rem > 12 or bldate == exprdate:
                    break
                if rem > 0.1 and (bldate - reptdate).days < 8:
                    rem = 0.1

                amount = payamt
                balance -= payamt
                if product not in FCY_PRODUCTS:
                    note_rows.append({"BNMCODE": f"95{item}{cust}{_remfmt(rem)}0000Y", "AMOUNT": amount})

                rem2 = 13 if (days > 89 or loanstat != 1) else rem
                if product not in FCY_PRODUCTS:
                    note_rows.append({"BNMCODE": f"93{item}{cust}{_remfmt(rem2)}0000Y", "AMOUNT": amount})

                bldate = _next_bldate(bldate, issdte, payfreq, freq)
                if bldate > exprdate or balance <= payamt:
                    bldate = exprdate

        if product not in FCY_PRODUCTS:
            note_rows.append({"BNMCODE": f"95{item}{cust}{_remfmt(rem)}0000Y", "AMOUNT": balance})

        rem2 = 13 if (days > 89 or loanstat != 1) else rem
        if product not in FCY_PRODUCTS:
            note_rows.append({"BNMCODE": f"93{item}{cust}{_remfmt(rem2)}0000Y", "AMOUNT": balance})

        if not _is_missing(eir_adj):
            eir_val = float(eir_adj)
            note_rows.append({"BNMCODE": f"95{item}{cust}060000Y", "AMOUNT": eir_val})
            note_rows.append({"BNMCODE": f"93{item}{cust}060000Y", "AMOUNT": eir_val})

    note = pl.DataFrame(note_rows) if note_rows else pl.DataFrame({"BNMCODE": [], "AMOUNT": []}, schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64})

    # Revolving credit adjustment
    note = note.filter(pl.col("BNMCODE").str.slice(0, 5) != "93212")
    rccorp = note.filter(pl.col("BNMCODE").str.slice(0, 5) == "95212")
    rccorp1 = rccorp.with_columns(pl.concat_str([pl.lit("93212"), pl.col("BNMCODE").str.slice(5, 9)]).alias("BNMCODE")).select("BNMCODE", "AMOUNT")
    note = pl.concat([note, rccorp1], how="vertical")

    default_df = pl.DataFrame(_default_codes())
    merged_like_sas = pl.concat([default_df, note], how="vertical")

    out = (
        merged_like_sas
        .group_by("BNMCODE")
        .agg(pl.col("AMOUNT").sum().alias("AMOUNT"))
        .sort("BNMCODE")
    )
    return out


def write_outputs(df: pl.DataFrame) -> None:
    df.write_parquet(OUTPUT_NOTE_PARQUET)
    with OUTPUT_NOTE_TXT.open("w", encoding="utf-8") as f:
        for row in df.iter_rows(named=True):
            f.write(f"{row['BNMCODE']}{row['AMOUNT']:16.2f}\n")


if __name__ == "__main__":
    result = process()
    write_outputs(result)
    print(f"Wrote {len(result)} rows to {OUTPUT_NOTE_PARQUET} and {OUTPUT_NOTE_TXT}")
