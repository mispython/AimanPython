#!/usr/bin/env python3
"""
Program: KALWPIBP
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
import calendar

import duckdb
import polars as pl

# =============================================================================
# PATHS / RUNTIME MACROS (configure these first)
# =============================================================================
BASE_PATH = Path(__file__).resolve().parent
BNMK_PATH = BASE_PATH / "bnmk"
BNM_PATH = BASE_PATH / "bnm"
NID_PATH = BASE_PATH / "nid"

REPTMON = "202401"
NOWK = "01"
REPTDAY = "20240131"
RDATE = "31/01/2024"
SDESC = "REPORT ON DOMESTIC ASSETS AND LIABILITIES PART I - KAPITI"
AMTIND_VALUE = "I"
PAGE_LENGTH = 60

K1_INPUT = BNMK_PATH / f"k1tbl{REPTMON}{NOWK}.parquet"
K3_INPUT = BNMK_PATH / f"k3tbl{REPTMON}{NOWK}.parquet"
RNID_INPUT = NID_PATH / f"rnid{REPTDAY}.parquet"
KALW_OUTPUT = BNM_PATH / f"kalw{REPTMON}{NOWK}.parquet"
REPORT_OUTPUT = BNM_PATH / f"kalw{REPTMON}{NOWK}_report.txt"

BNM_PATH.mkdir(parents=True, exist_ok=True)


# =============================================================================
# Helpers
# =============================================================================
def sval(v: object) -> str:
    if v is None:
        return ""
    return str(v).strip()


def countmon(gwsdt: date | datetime | None, gwmdt: date | datetime | None) -> int:
    if gwsdt is None or gwmdt is None:
        return 0
    if isinstance(gwsdt, datetime):
        gwsdt = gwsdt.date()
    if isinstance(gwmdt, datetime):
        gwmdt = gwmdt.date()
    if gwmdt < gwsdt:
        return 0

    folmonth = gwsdt
    nummonth = 0
    while gwmdt > folmonth:
        nummonth += 1
        nextday = gwsdt.day
        nextmon = folmonth.month + 1
        nextyear = folmonth.year
        if nextmon > 12:
            nextmon -= 12
            nextyear += 1

        if nextday in (29, 30, 31) and nextmon == 2:
            folmonth = date(nextyear, 3, 1) - timedelta(days=1)
        elif nextday == 31 and nextmon in (4, 6, 9, 11):
            folmonth = date(nextyear, nextmon, 30)
        elif nextday == 30 and gwsdt.month in (4, 6, 9, 11) and nextmon in (1, 3, 5, 7, 8, 10, 12):
            folmonth = date(nextyear, nextmon, 31)
        else:
            day = min(nextday, calendar.monthrange(nextyear, nextmon)[1])
            folmonth = date(nextyear, nextmon, day)
    return nummonth


def output_rec(out: list[dict], code: str, amount: float, amtind: str = AMTIND_VALUE) -> None:
    if code.strip():
        out.append({"bnmcode": code, "amount": float(amount or 0), "amtind": amtind})


def process_dp32000(r: dict, out: list[dict], amount: float) -> None:
    gwdlp, gwctp = sval(r.get("gwdlp")), sval(r.get("gwctp"))
    gwccy, gwmvt, gwmvts, gwcnal = sval(r.get("gwccy")), sval(r.get("gwmvt")), sval(r.get("gwmvts")), sval(r.get("gwcnal"))
    code = ""
    if gwdlp in {"FDA", "FDB", "FDL", "FDS"}:
        if not ("BA" <= gwctp <= "BZ") and gwcnal == "MY" and gwccy == "MYR" and gwmvt == "P" and gwmvts == "M":
            code = "3213020000000Y"
        else:
            code = {"BB": "3213002000000Y", "BQ": "3213011000000Y", "BM": "3213012000000Y"}.get(gwctp, "")
    if gwdlp[1:3] in {"XI", "XT"}:
        if not ("BA" <= gwctp <= "BZ") and gwcnal == "MY" and gwccy == "MYR" and gwmvt == "P" and gwmvts == "M":
            code = "3250020000000Y"
        else:
            if gwctp == "BN":
                output_rec(out, "3250013000000Y", amount)
                code = "3250020000000Y"
            elif gwctp == "BG":
                output_rec(out, "3250017000000Y", amount)
                code = "3250020000000Y"
            else:
                code = {
                    "BB": "3250002000000Y", "BI": "3250003000000Y", "BQ": "3250011000000Y", "BM": "3250012000000Y",
                    "BA": "3250081000000Y", "BW": "3250081000000Y", "BE": "3250081000000Y"
                }.get(gwctp, code)
    if gwdlp == "LC" and gwctp == "BN":
        code = "3214013000000Y"
    output_rec(out, code, amount)


def process_af33000(r: dict, out: list[dict], amount: float) -> None:
    gwdlp, gwccy, gwmvt, gwmvts, gwctp = sval(r.get("gwdlp")), sval(r.get("gwccy")), sval(r.get("gwmvt")), sval(r.get("gwmvts")), sval(r.get("gwctp"))
    code = ""
    if gwdlp in {"LO", "LS", "LF", "LOW", "LSW", "LSC", "LOC", "LOI", "LSI"}:
        if gwccy == "MYR" and gwmvt == "P" and gwmvts == "M":
            code = {"BC": "3314001000000Y", "BB": "3314002000000Y", "BI": "3314003000000Y", "BQ": "3314011000000Y", "BM": "3314012000000Y", "BN": "3314020000000Y", "BG": "3314017000000Y"}.get(gwctp, "")
            if gwctp in {"BA", "BW"}:
                nm = countmon(r.get("gwsdt"), r.get("gwmdt"))
                if gwdlp in {"LO", "LS"} and nm <= 12:
                    code = "3314081100000Y"
                if gwdlp == "LF" and nm > 12:
                    code = "3314081200000Y"
        if gwccy != "MYR" and gwmvt == "P" and gwmvts == "M":
            code = {"BC": "3364001000000Y", "BB": "3364002000000Y", "BI": "3364003000000Y", "BM": "3364012000000Y", "BJ": "3364007000000Y"}.get(gwctp, code)
            if gwctp in {"BA", "BW", "BE"} and gwdlp == "LF" and countmon(r.get("gwsdt"), r.get("gwmdt")) > 12:
                code = "3364081200000Y"
    output_rec(out, code, amount)


def process_ln34000(r: dict, out: list[dict], amount: float) -> None:
    gwdlp, gwccy, gwmvt, gwmvts, gwctp, gwsac, gwcnal = [sval(r.get(c)) for c in ("gwdlp", "gwccy", "gwmvt", "gwmvts", "gwctp", "gwsac", "gwcnal")]
    code = ""
    if gwdlp in {"LO", "LS", "LF"} and gwccy != "MYR" and gwmvt == "P" and gwmvts == "M" and not ("BA" <= gwctp <= "BZ") and gwsac != "UF" and gwcnal == "MY":
        code = "3460015000000Y"
    if gwdlp in {"LO", "LS", "LF"} and gwccy != "MYR" and gwmvt == "P" and gwmvts == "M" and gwsac == "UF":
        code = "3460085000000Y"
    output_rec(out, code, amount)


def process_da42160(r: dict, out: list[dict], amount: float) -> None:
    gwctp, gwc2r = sval(r.get("gwctp")), sval(r.get("gwc2r"))
    code = "4216060000000Y"
    if gwc2r == "57": code = "4216057000000Y"
    elif gwc2r == "75": code = "4216075000000Y"
    elif gwctp in {"BP", "BC"}: code = "4216001000000Y"
    elif gwctp == "BB": code = "4216002000000Y"
    elif gwctp == "BI": code = "4216003000000Y"
    elif gwctp == "BJ": code = "4216007000000Y"
    elif gwctp == "BQ": code = "4216011000000Y"
    elif gwctp == "BM": code = "4216012000000Y"
    elif gwctp == "BN": code = "4216013000000Y"
    elif gwctp == "BG": code = "4216017000000Y"
    elif gwctp in {"BR", "BF", "BH", "BZ", "BU", "AD", "BT", "BV", "BS"}: code = "4216020000000Y"
    elif gwctp == "DA": code = "4216071000000Y"
    elif gwctp == "DB": code = "4216072000000Y"
    elif gwctp == "DC": code = "4216074000000Y"
    elif gwctp in {"EA", "EC"}: code = "4216076000000Y"
    elif gwctp == "FA": code = "4216079000000Y"
    elif gwctp in {"BW", "BA", "BE"}: code = "4216081000000Y"
    elif gwctp in {"CE", "EB", "GA"}: code = "4216085000000Y"
    output_rec(out, code, amount)


def process_da42600(r: dict, out: list[dict], amount: float) -> None:
    gwdlp, gwact, gwctp, gwc2r, gwcnal, gwsac, gwccy = [sval(r.get(c)) for c in ("gwdlp", "gwact", "gwctp", "gwc2r", "gwcnal", "gwsac", "gwccy")]
    gwmvt, gwmvts, gwshn, gwan, gwas = sval(r.get("gwmvt")), sval(r.get("gwmvts")), sval(r.get("gwshn")), sval(r.get("gwan")), sval(r.get("gwas"))
    code = ""
    if gwdlp == "" and gwact == "CV" and not ("BA" <= gwctp <= "BZ") and gwc2r not in {"57", "75"} and gwcnal == "MY" and gwsac != "UF" and gwccy != "MYR":
        code = "4261060000000Y"
    if gwdlp in {"FDA", "FDB", "FDL", "FDS", "BO", "BF"} and gwccy != "MYR" and not ("BA" <= gwctp <= "BZ") and gwc2r not in {"57", "75"} and gwcnal == "MY" and gwmvt == "P" and gwmvts == "M" and gwsac != "UF":
        code = "4263060000000Y"
    if gwdlp in {"FDA", "FDB", "FDL", "FDS", "BO", "BF"} and gwccy != "MYR" and gwctp == "BM" and gwcnal == "MY" and gwmvt == "P" and gwmvts == "M" and gwshn[:3] == "FCY":
        code = "4263012000000Y"
    if gwdlp == "" and gwact == "CV" and gwcnal != "MY" and gwsac == "UO" and gwccy != "MYR" and gwan != "000612" and gwas != "344":
        code = "4263081000000Y"
    output_rec(out, code, amount)


def process_ml49000(r: dict, out: list[dict], amount: float) -> None:
    gwccy, gwsac, gwcnal, gwmvt, gwmvts = [sval(r.get(c)) for c in ("gwccy", "gwsac", "gwcnal", "gwmvt", "gwmvts")]
    gwciac = float(r.get("gwciac") or 0)
    code, amt = "", amount
    if ((gwccy == "MYR" and gwsac == "UO" and gwciac != 0 and gwmvt == "P" and gwmvts == "M") or
        (gwccy == "MYR" and gwcnal != "MY" and gwciac != 0 and gwmvt == "P" and gwmvts == "M")):
        code, amt = "4911080000000Y", gwciac
    if gwccy != "MYR" and gwsac != "UO" and gwciac != 0 and gwcnal == "MY" and gwmvt == "P" and gwmvts == "M":
        code, amt = "4961050000000Y", gwciac
    if (gwsac == "UO" or gwcnal != "MY") and gwccy != "MYR" and gwciac != 0 and gwmvt == "P" and gwmvts == "M":
        code, amt = "4961080000000Y", gwciac
    output_rec(out, code, amt)


def fx_code(prefix: str, gwctp: str, gwcnal: str) -> str:
    core = {"BC": "001", "BB": "002", "BI": "003", "BJ": "007", "BM": "012"}
    if gwctp in core:
        return f"{prefix}{core[gwctp]}000000Y"
    if gwctp in {"BA", "BE"}:
        return f"{prefix}081000000Y"
    if gwctp not in {"BA", "BB", "BC", "BE", "BI", "BJ", "BM", "BW"}:
        return f"{prefix}{'015' if gwcnal == 'MY' else '085'}000000Y"
    return ""


def process_fx57000(r: dict, out: list[dict]) -> None:
    amount = float(r.get("gwbala") or 0) * float(r.get("gwexr") or 0)
    gwocy, gwmvt, gwmvts, gwdlp, gwccy, gwctp, gwcnal = [sval(r.get(c)) for c in ("gwocy", "gwmvt", "gwmvts", "gwdlp", "gwccy", "gwctp", "gwcnal")]
    code = ""
    if gwocy == "MYR" and gwmvt == "P" and gwmvts == "P" and gwccy != "MYR":
        if gwdlp == "FXS": code = fx_code("5711", gwctp, gwcnal)
        elif gwdlp in {"FXO", "FXF", "FBP"}: code = fx_code("5712", gwctp, gwcnal)
        elif gwdlp in {"SF1", "SF2", "TS1", "TS2", "FF1", "FF2"}: code = fx_code("5713", gwctp, gwcnal)
    if gwocy == "MYR" and gwmvt == "P" and gwmvts == "S" and gwccy != "MYR":
        if gwdlp == "FXS": code = fx_code("5741", gwctp, gwcnal)
        elif gwdlp in {"FXO", "FXF", "FBP"}: code = fx_code("5742", gwctp, gwcnal)
        elif gwdlp in {"SF1", "SF2", "TS1", "TS2", "FF1", "FF2"}: code = fx_code("5743", gwctp, gwcnal)
    output_rec(out, code, amount)


def process_k1(df: pl.DataFrame) -> pl.DataFrame:
    out: list[dict] = []
    for r in df.iter_rows(named=True):
        amount = float(r.get("gwbalc") or 0)
        gwdlp, gwccy, gwmvt, gwmvts, gwact, gwocy = [sval(r.get(c)) for c in ("gwdlp", "gwccy", "gwmvt", "gwmvts", "gwact", "gwocy")]

        if gwccy == "MYR" and gwmvt == "P" and gwmvts == "M" and (gwdlp in {"FDA", "FDB", "FDL", "FDS", "LC", "LO"} or gwdlp[1:3] in {"XI", "XT"}):
            process_dp32000(r, out, amount)
        if gwccy == "MYR" and gwmvt == "P" and gwmvts == "M" and gwdlp in {"BCS", "BCT", "BCW", "BQD"}:
            pass
        if gwdlp in {"", "LO", "LS", "LF", "LOW", "LSW", "LSC", "LOC", "LOI", "LSI"} or gwact == "CN":
            process_af33000(r, out, amount)
            process_ln34000(r, out, amount)

        process_da42160(r, out, amount) if (gwdlp[1:3] in {"MI", "MT"} and gwccy == "MYR" and gwmvt == "P" and gwmvts == "M") else None
        if gwdlp in {"FDA", "FDB", "FDS", "FDL", "BO", "BF", ""}:
            process_da42600(r, out, amount)
        if gwdlp in {"BF", "BOW", "BSW", "BOI", "BFI", "BSC", "BOC"}:
            pass
        process_ml49000(r, out, amount)
        if gwocy == "XAU":
            continue
        if gwdlp in {"FXS", "FXO", "FXF", "SF1", "SF2", "TS1", "TS2", "FBP", "FF1", "FF2"}:
            process_fx57000(r, out)
    return pl.DataFrame(out) if out else pl.DataFrame(schema={"bnmcode": pl.Utf8, "amount": pl.Float64, "amtind": pl.Utf8})


def process_k3(df: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict] = []
    for r in df.iter_rows(named=True):
        amount = float(r.get("utamoc") or 0) - float(r.get("utdpf") or 0) + float(r.get("utaict") or 0)
        code = ""
        if sval(r.get("utsty")) in {"IFD", "ILD", "ISD", "IZD", "IDC", "IDP"} and sval(r.get("utref")) in {"PFD", "PLD", "PSD", "PZD", "PDC"}:
            utctp = sval(r.get("utctp"))
            if utctp in {"BB", "BQ"}: code = "4215002000000Y"
            elif utctp == "BI": code = "4215003000000Y"
            elif utctp == "BJ": code = "4215007000000Y"
            elif utctp == "BM": code = "4215012000000Y"
            elif utctp in {"BR", "BF", "BH", "BZ", "BU", "AD", "BT", "BV", "BS", "BN"}: code = "4215020000000Y"
            elif utctp == "BN": code = "4215013000000Y"
            elif utctp == "BG": code = "4215017000000Y"
            elif utctp == "DA": code = "4215071000000Y"
            elif utctp == "DB": code = "4215072000000Y"
            elif utctp == "DC": code = "4215074000000Y"
            elif utctp in {"EC", "EA"}: code = "4215076000000Y"
            elif utctp == "FA": code = "4215079000000Y"
            elif utctp in {"BA", "BW", "BE"}: code = "4215081000000Y"
            elif utctp in {"EB", "CE", "GA"}: code = "4215085000000Y"
            elif utctp in {"AC", "DD", "CG", "CF", "CA", "CB", "CC", "CD"} or not ("BA" <= utctp <= "BZ"):
                code = "4215060000000Y"
        output_rec(rows, code, amount)
    return pl.DataFrame(rows) if rows else pl.DataFrame(schema={"bnmcode": pl.Utf8, "amount": pl.Float64, "amtind": pl.Utf8})


def process_nid(df: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict] = []
    for r in df.filter((pl.col("nidstat") == "N") & (pl.col("curbal") > 0)).iter_rows(named=True):
        cust = int(r.get("custcd") or -1)
        amt = float(r.get("curbal") or 0)
        if cust in {2, 3, 12, 7, 17, 57, 71, 72, 73, 74, 75, 79}:
            output_rec(rows, f"42150{cust:02d}000000Y", amt)
        if 4 <= cust <= 6 or cust in {13, 20, 45} or 30 <= cust <= 40:
            output_rec(rows, "4215020000000Y", amt)
        if 41 <= cust <= 44 or 46 <= cust <= 49 or 51 <= cust <= 54 or cust == 59 or 61 <= cust <= 69:
            output_rec(rows, "4215060000000Y", amt)
        if cust in {77, 78}:
            output_rec(rows, "4215076000000Y", amt)
        if 82 <= cust <= 84:
            output_rec(rows, "4215081000000Y", amt)
        if 86 <= cust <= 99:
            output_rec(rows, "4215085000000Y", amt)
    return pl.DataFrame(rows) if rows else pl.DataFrame(schema={"bnmcode": pl.Utf8, "amount": pl.Float64, "amtind": pl.Utf8})


def write_report(df: pl.DataFrame, output: Path) -> None:
    lines = [
        f"{SDESC}",
        "REPORT ON DOMESTIC ASSETS AND LIABILITIES PART I - KAPITI",
        f"REPORT DATE : {RDATE}",
        "",
        f"{'BNMCODE':<14} {'AMTIND':<6} {'AMOUNT':>30}",
    ]
    for r in df.iter_rows(named=True):
        lines.append(f"{r['bnmcode']:<14} {r['amtind']:<6} {r['amount']:>30,.2f}")

    with output.open("w", encoding="utf-8") as f:
        line_no = 0
        for i, ln in enumerate(lines):
            ctrl = "1" if line_no == 0 else " "
            f.write(f"{ctrl}{ln}\n")
            line_no += 1
            if line_no >= PAGE_LENGTH and i != len(lines) - 1:
                line_no = 0


def main() -> None:
    con = duckdb.connect()
    k1 = con.execute(f"SELECT * FROM read_parquet('{K1_INPUT.as_posix()}')").pl() if K1_INPUT.exists() else pl.DataFrame()
    k3 = con.execute(f"SELECT * FROM read_parquet('{K3_INPUT.as_posix()}')").pl() if K3_INPUT.exists() else pl.DataFrame()
    nid = con.execute(f"SELECT * FROM read_parquet('{RNID_INPUT.as_posix()}')").pl() if RNID_INPUT.exists() else pl.DataFrame()

    k1_out = process_k1(k1) if k1.height else pl.DataFrame(schema={"bnmcode": pl.Utf8, "amount": pl.Float64, "amtind": pl.Utf8})
    k3_out = process_k3(k3) if k3.height else pl.DataFrame(schema={"bnmcode": pl.Utf8, "amount": pl.Float64, "amtind": pl.Utf8})
    nid_out = process_nid(nid) if nid.height else pl.DataFrame(schema={"bnmcode": pl.Utf8, "amount": pl.Float64, "amtind": pl.Utf8})

    new_data = pl.concat([k1_out, k3_out, nid_out], how="vertical") if (k1_out.height + k3_out.height + nid_out.height) else pl.DataFrame(schema={"bnmcode": pl.Utf8, "amount": pl.Float64, "amtind": pl.Utf8})
    base_data = pl.read_parquet(KALW_OUTPUT) if KALW_OUTPUT.exists() else pl.DataFrame(schema={"bnmcode": pl.Utf8, "amount": pl.Float64, "amtind": pl.Utf8})

    appended = pl.concat([base_data, new_data], how="vertical") if base_data.height or new_data.height else new_data
    final_df = appended.group_by(["bnmcode", "amtind"]).agg(pl.col("amount").sum()).with_columns(pl.col("amount").abs())

    final_df.write_parquet(KALW_OUTPUT)
    write_report(final_df.sort(["bnmcode", "amtind"]), REPORT_OUTPUT)


if __name__ == "__main__":
    main()
