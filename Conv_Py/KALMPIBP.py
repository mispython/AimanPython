#!/usr/bin/env python3
"""
Program: KALMPIBP
Purpose: Python conversion of SAS program KALMPIBP (RDAL Part II - Kapiti Items).

Inputs are expected as parquet files with column names matching SAS variables.
Outputs:
  1) Consolidated parquet table: kalm{REPTMON}{NOWK}.parquet
  2) Text report with ASA carriage control (page length 60)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb
import polars as pl


# =============================================================================
# CONFIGURATION
# =============================================================================


@dataclass(frozen=True)
class Config:
    reptmon: str = "202401"
    nowk: str = "01"
    reptday: str = "20240131"
    tdate: str = "2024-01-31"
    rdate: str = "31JAN2024"
    sdesc: str = "KALMPIBP"

    base_path: Path = Path("/data")

    @property
    def bnmk_path(self) -> Path:
        return self.base_path / "bnmk"

    @property
    def bnm_path(self) -> Path:
        return self.base_path / "bnm"

    @property
    def nid_path(self) -> Path:
        return self.base_path / "nid"

    @property
    def output_path(self) -> Path:
        return self.base_path / "output"

    @property
    def k1_file(self) -> Path:
        return self.bnmk_path / f"k1tbl{self.reptmon}{self.nowk}.parquet"

    @property
    def k2_file(self) -> Path:
        return self.bnmk_path / f"k2tbl{self.reptmon}{self.nowk}.parquet"

    @property
    def k3_file(self) -> Path:
        return self.bnmk_path / f"k3tbl{self.reptmon}{self.nowk}.parquet"

    @property
    def nid_file(self) -> Path:
        return self.nid_path / f"rnid{self.reptday}.parquet"

    @property
    def kalm_file(self) -> Path:
        return self.bnm_path / f"kalm{self.reptmon}{self.nowk}.parquet"

    @property
    def report_file(self) -> Path:
        return self.output_path / f"kalm{self.reptmon}{self.nowk}_report.txt"


# =============================================================================
# HELPERS
# =============================================================================


def parse_sas_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d%b%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def load_parquet_duckdb(conn: duckdb.DuckDBPyConnection, path: Path) -> pl.DataFrame:
    return pl.from_arrow(conn.execute("SELECT * FROM read_parquet(?)", [str(path)]).arrow())


def days_in_month(year: int, month: int) -> int:
    if month == 2:
        return 29 if year % 4 == 0 else 28
    return 30 if month in (4, 6, 9, 11) else 31


def calc_remmth(matdt: date, refdt: date) -> float:
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    rpyr, rpmth, rpday = refdt.year, refdt.month, refdt.day
    rpdays = days_in_month(rpyr, rpmth)
    if mdday > rpdays:
        mdday = rpdays
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + (remd / rpdays)


def format_fdrmmt(remmth: float | None) -> str:
    if remmth is None:
        return "99"
    if remmth <= 1:
        return "01"
    if remmth <= 3:
        return "03"
    if remmth <= 6:
        return "06"
    if remmth <= 12:
        return "12"
    if remmth <= 24:
        return "24"
    if remmth <= 36:
        return "36"
    if remmth <= 60:
        return "60"
    return "61"


def format_fdorgmt(remmth: float | None) -> str:
    return format_fdrmmt(remmth)



def append_rows(rows: list[dict], bnmcode: str, amount: float, amtind: str) -> None:
    if bnmcode and bnmcode.strip():
        rows.append({"BNMCODE": bnmcode, "AMOUNT": float(amount), "AMTIND": amtind})


# =============================================================================
# DATA STEP CONVERSIONS
# =============================================================================


def process_k1tbl(k1: pl.DataFrame, amtind: str) -> pl.DataFrame:
    rows: list[dict] = []
    for rec in k1.iter_rows(named=True):
        if rec.get("GWOCY") == "XAU" or rec.get("GWCCY") == "XAU":
            continue
        if not (
            rec.get("GWCCY") != "MYR"
            and rec.get("GWOCY") != "MYR"
            and rec.get("GWMVT") == "P"
            and rec.get("GWMVTS") == "P"
            and rec.get("GWCTP") != "BW"
        ):
            continue

        gwdlp = rec.get("GWDLP")
        bnmcode = ""
        if gwdlp == "FXS":
            bnmcode = "5761000000000Y"
        elif gwdlp in ("FXO", "FXF", "FBP"):
            bnmcode = "5762000000000Y"
        elif gwdlp in ("SF2", "FF1", "FF2"):
            bnmcode = "5763100000000Y"
        elif gwdlp in ("SF1", "TS1", "TS2"):
            bnmcode = "5763200000000Y"

        amount = abs(float(rec.get("AMOUNT") or 0.0))
        if bnmcode:
            append_rows(rows, bnmcode, amount, amtind)
            if bnmcode in {"5761000000000Y", "5763100000000Y", "5763200000000Y"}:
                append_rows(rows, "5760000000000Y", amount, amtind)

    return pl.DataFrame(rows, schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTIND": pl.Utf8})


def map_fx_bnmcode(prefix: str, osdlp: str, gfctp: str, gfcnal: str) -> str:
    if osdlp == "FXS":
        known = {
            "BC": f"{prefix}0101000000Y",
            "BB": f"{prefix}0102000000Y",
            "BI": f"{prefix}0103000000Y",
            "BJ": f"{prefix}0107000000F",
            "BM": f"{prefix}0112000000Y",
        }
        if gfctp in known:
            return known[gfctp]
        if gfctp in ("BA", "BE"):
            return f"{prefix}0181000000Y"
        if gfctp not in ("BA", "BB", "BC", "BE", "BI", "BJ", "BM", "BW"):
            if gfcnal == "MY":
                return f"{prefix}0115000000Y"
            if gfcnal not in ("", " ", "MY"):
                return f"{prefix}0185000000Y"
        return ""

    if osdlp in ("FXF", "FXO", "FBP"):
        known = {
            "BC": f"{prefix}0201000000Y",
            "BB": f"{prefix}0202000000Y",
            "BI": f"{prefix}0203000000Y",
            "BJ": f"{prefix}0207000000F",
            "BM": f"{prefix}0212000000Y",
        }
        if gfctp in known:
            return known[gfctp]
        if gfctp in ("BA", "BE"):
            return f"{prefix}0281000000Y"
        if gfctp not in ("BA", "BB", "BC", "BE", "BI", "BJ", "BM", "BW"):
            if gfcnal == "MY":
                return f"{prefix}0215000000Y"
            if gfcnal not in ("", " ", "MY"):
                return f"{prefix}0285000000Y"
        return ""

    if osdlp in ("TS1", "SF1", "FF1", "TS2", "SF2", "FF2"):
        known = {
            "BC": f"{prefix}0301000000Y",
            "BB": f"{prefix}0302000000Y",
            "BI": f"{prefix}0303000000Y",
            "BJ": f"{prefix}0307000000F",
            "BM": f"{prefix}0312000000Y",
        }
        if gfctp in known:
            return known[gfctp]
        if gfctp in ("BA", "BE"):
            return f"{prefix}0381000000Y"
        if gfctp not in ("BA", "BB", "BC", "BE", "BI", "BJ", "BM", "BW"):
            if gfcnal == "MY":
                return f"{prefix}0315000000Y"
            if gfcnal not in ("", " ", "MY"):
                return f"{prefix}0385000000Y"
    return ""


def process_k2tbl(k2: pl.DataFrame, amtind: str) -> pl.DataFrame:
    rows: list[dict] = []
    for rec in k2.iter_rows(named=True):
        oxpccy = rec.get("OXPCCY")
        oxsccy = rec.get("OXSCCY")
        ommvt = rec.get("OMMVT")
        ommvts = rec.get("OMMVTS")
        gfctp = (rec.get("GFCTP") or "").strip()
        gfcnal = (rec.get("GFCNAL") or "").strip()
        osdlp = rec.get("OSDLP")

        if oxpccy != "MYR" and oxsccy == "MYR" and ommvt == "P" and ommvts == "P":
            bnmcode = map_fx_bnmcode("685", osdlp, gfctp, gfcnal)
            amount = float(rec.get("ORINWR") or 0.0) * float(rec.get("OXEXR") or 0.0)
            append_rows(rows, bnmcode, amount, amtind)

        if oxsccy != "MYR" and oxpccy == "MYR" and ommvt == "P" and ommvts == "S":
            bnmcode = map_fx_bnmcode("785", osdlp, gfctp, gfcnal)
            amount = float(rec.get("ORINWP") or 0.0) * float(rec.get("OXEXR") or 0.0)
            append_rows(rows, bnmcode, amount, amtind)

    return pl.DataFrame(rows, schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTIND": pl.Utf8})


def process_k3tbl1(k3: pl.DataFrame, amtind: str) -> pl.DataFrame:
    rows: list[dict] = []
    for rec in k3.iter_rows(named=True):
        if rec.get("UTSTY") not in ("IFD", "ILD", "ISD", "IZD", "IDC", "IDP"):
            continue
        if rec.get("UTREF") not in ("PFD", "PLD", "PSD", "PZD", "PDC"):
            continue

        matdt = parse_sas_date(rec.get("MATDT"))
        reptdate = parse_sas_date(rec.get("REPTDATE"))
        if matdt is None or reptdate is None or not (matdt > reptdate):
            continue

        remmth = calc_remmth(matdt, reptdate)
        remmt = format_fdrmmt(remmth)
        amount = float(rec.get("UTAMOC") or 0.0) - float(rec.get("UTDPF") or 0.0) + float(rec.get("UTAICT") or 0.0)
        append_rows(rows, f"4215000{remmt}0000Y", amount, amtind)

    return pl.DataFrame(rows, schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTIND": pl.Utf8})


def process_k3tbl2(k3: pl.DataFrame, amtind: str) -> pl.DataFrame:
    rows: list[dict] = []
    for rec in k3.iter_rows(named=True):
        if rec.get("UTSTY") not in ("IFD", "ILD", "ISD", "IZD", "IDC", "IDP"):
            continue
        if rec.get("UTREF") not in ("PFD", "PLD", "PSD", "PZD", "PDC"):
            continue

        matdt = parse_sas_date(rec.get("MATDT"))
        issdt = parse_sas_date(rec.get("ISSDT")) or parse_sas_date(rec.get("REPTDATE"))
        if matdt is None or issdt is None:
            continue

        remmth = calc_remmth(matdt, issdt)
        origmt = format_fdorgmt(remmth)
        amount = float(rec.get("UTAMOC") or 0.0) - float(rec.get("UTDPF") or 0.0) + float(rec.get("UTAICT") or 0.0)
        append_rows(rows, f"4215000{origmt}0000Y", amount, amtind)

    return pl.DataFrame(rows, schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTIND": pl.Utf8})


def process_nidtbl(nid: pl.DataFrame, amtind: str, tdate: date) -> pl.DataFrame:
    rows: list[dict] = []
    for rec in nid.iter_rows(named=True):
        if rec.get("NIDSTAT") != "N":
            continue
        curbal = float(rec.get("CURBAL") or 0.0)
        if curbal <= 0:
            continue

        matdt = parse_sas_date(rec.get("MATDT"))
        startdt = parse_sas_date(rec.get("STARTDT"))
        if matdt is None:
            continue

        if matdt > tdate:
            remmth = calc_remmth(matdt, tdate)
            remmt = format_fdrmmt(remmth)
            append_rows(rows, f"4215000{remmt}0000Y", curbal, amtind)

        if startdt is not None and matdt >= startdt:
            origmt = format_fdorgmt(calc_remmth(matdt, startdt))
            if origmt in ("12", "13"):
                origmt = "14"
            append_rows(rows, f"4215000{origmt}0000Y", curbal, amtind)

    return pl.DataFrame(rows, schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTIND": pl.Utf8})


# =============================================================================
# OUTPUT
# =============================================================================


def write_asa_report(df: pl.DataFrame, cfg: Config) -> None:
    cfg.output_path.mkdir(parents=True, exist_ok=True)
    lines_per_page = 60

    rows = df.sort(["BNMCODE", "AMTIND"]).iter_rows(named=True)

    with cfg.report_file.open("w", encoding="utf-8") as f:
        line_no = 0

        def emit(control: str, text: str) -> None:
            nonlocal line_no
            f.write(f"{control}{text}\n")
            line_no = 1 if control == "1" else line_no + 1

        emit("1", cfg.sdesc)
        emit(" ", "REPORT ON DOMESTIC ASSETS AND LIABILITIES PART II- KAPITI")
        emit(" ", f"REPORT DATE : {cfg.rdate}")
        emit(" ", "")
        emit(" ", f"{'BNMCODE':<14} {'AMTIND':<6} {'AMOUNT':>30}")

        for row in rows:
            if line_no >= lines_per_page:
                emit("1", cfg.sdesc)
                emit(" ", "REPORT ON DOMESTIC ASSETS AND LIABILITIES PART II- KAPITI")
                emit(" ", f"REPORT DATE : {cfg.rdate}")
                emit(" ", "")
                emit(" ", f"{'BNMCODE':<14} {'AMTIND':<6} {'AMOUNT':>30}")

            emit(" ", f"{row['BNMCODE']:<14} {row['AMTIND']:<6} {row['AMOUNT']:>30,.2f}")


def main() -> None:
    cfg = Config()
    amtind = "I"
    tdate = parse_sas_date(cfg.tdate)
    if tdate is None:
        raise ValueError(f"Invalid tdate: {cfg.tdate}")

    cfg.bnm_path.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()

    k1 = load_parquet_duckdb(conn, cfg.k1_file)
    k2 = load_parquet_duckdb(conn, cfg.k2_file)
    k3 = load_parquet_duckdb(conn, cfg.k3_file)
    nid = load_parquet_duckdb(conn, cfg.nid_file)

    if "GWBALC" in k1.columns and "AMOUNT" not in k1.columns:
        k1 = k1.rename({"GWBALC": "AMOUNT"})

    k1tabl = process_k1tbl(k1, amtind)
    k2tabl = process_k2tbl(k2, amtind)
    k3tabl = pl.concat([process_k3tbl1(k3, amtind), process_k3tbl2(k3, amtind)], how="vertical")
    nidtbl = process_nidtbl(nid, amtind, tdate)

    kalm = pl.concat([k1tabl, k2tabl, k3tabl, nidtbl], how="vertical")

    if kalm.height == 0:
        final = pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMTIND": pl.Utf8, "AMOUNT": pl.Float64})
    else:
        final = (
            kalm.group_by(["BNMCODE", "AMTIND"])
            .agg(pl.col("AMOUNT").sum())
            .select(["BNMCODE", "AMTIND", "AMOUNT"])
            .sort(["BNMCODE", "AMTIND"])
        )

    final.write_parquet(cfg.kalm_file)
    write_asa_report(final, cfg)

    print(f"Output parquet: {cfg.kalm_file}")
    print(f"Output report : {cfg.report_file}")
    print(f"Rows produced : {final.height}")


if __name__ == "__main__":
    main()
