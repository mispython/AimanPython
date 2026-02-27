#!/usr/bin/env python3
"""
Program : EIIMTOP5
Purpose : Generate Top 50 FD+CA Individual and Corporate depositors, and
            PB subsidiaries report, using parquet inputs and text report outputs
            with ASA carriage-control characters.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

# =============================================================================
# PATH SETUP (defined early as requested)
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Inputs (assumed parquet already converted with SAS column names)
DEPOSIT_REPTDATE_PARQUET = INPUT_DIR / "DEPOSIT_REPTDATE.parquet"
DEPOSIT_CURRENT_PARQUET = INPUT_DIR / "DEPOSIT_CURRENT.parquet"
DEPOSIT_FD_PARQUET = INPUT_DIR / "DEPOSIT_FD.parquet"
CISCA_DEPOSIT_PARQUET = INPUT_DIR / "CISCA_DEPOSIT.parquet"
CISFD_DEPOSIT_PARQUET = INPUT_DIR / "CISFD_DEPOSIT.parquet"

# Outputs from PRINTTO DD statements
FD11TEXT_OUT = OUTPUT_DIR / "SAP.PIBB.INDTOP50.TEXT.txt"
FD12TEXT_OUT = OUTPUT_DIR / "SAP.PIBB.CORTOP50.TEXT.txt"
FDSTEXT_OUT = OUTPUT_DIR / "SAP.PIBB.SUBTOP50.TEXT.txt"

PAGE_LENGTH = 60  # default page length per instruction
LINE_SIZE = 132

CA_EXCL_PRODUCTS = [400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410]
FD_EXCL_PRODUCTS = [350, 351, 352, 353, 354, 355, 356, 357]
SUBS_CUSTNOS = [53227, 169990, 170108, 3562038, 3721354]


class AsaReportWriter:
    """Write text report with ASA carriage control characters."""

    def __init__(self, out_path: Path, page_length: int = PAGE_LENGTH):
        self.out_path = out_path
        self.page_length = page_length
        self.line_no = 0
        self.page_no = 0
        self.lines: list[str] = []

    def _write_raw(self, asa: str, text: str) -> None:
        self.lines.append(f"{asa}{text[:LINE_SIZE-1]}".rstrip() + "\n")

    def new_page(self, title_lines: Iterable[str]) -> None:
        self.page_no += 1
        self.line_no = 0
        self._write_raw("1", "")
        for idx, line in enumerate(title_lines):
            self._write_raw(" ", line)
            self.line_no += 1
        self._write_raw(" ", f"Page {self.page_no}")
        self.line_no += 1
        self._write_raw(" ", "")
        self.line_no += 1

    def line(self, text: str = "") -> None:
        if self.line_no >= self.page_length - 1:
            self.new_page([])
        self._write_raw(" ", text)
        self.line_no += 1

    def save(self) -> None:
        self.out_path.write_text("".join(self.lines), encoding="utf-8")


def sas_to_date(v) -> date:
    if isinstance(v, date):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, int):
        return date(1960, 1, 1) + timedelta(days=v)
    s = str(v)
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unrecognized REPTDATE value: {v}")


def read_reptdate(con: duckdb.DuckDBPyConnection) -> date:
    df = con.execute(f"SELECT REPTDATE FROM read_parquet('{DEPOSIT_REPTDATE_PARQUET}') LIMIT 1").pl()
    if df.height == 0:
        raise RuntimeError("DEPOSIT.REPTDATE has no rows")
    return sas_to_date(df[0, "REPTDATE"])


def build_cisca_cisfd(con: duckdb.DuckDBPyConnection) -> tuple[pl.DataFrame, pl.DataFrame]:
    all_cis = con.execute(
        f"""
        SELECT CUSTNO, ACCTNO, CUSTNAME, NEWIC, OLDIC, INDORG,
               CASE WHEN COALESCE(NEWIC,'') <> '' THEN NEWIC ELSE OLDIC END AS ICNO
        FROM read_parquet('{CISCA_DEPOSIT_PARQUET}')
        UNION ALL
        SELECT CUSTNO, ACCTNO, CUSTNAME, NEWIC, OLDIC, INDORG,
               CASE WHEN COALESCE(NEWIC,'') <> '' THEN NEWIC ELSE OLDIC END AS ICNO
        FROM read_parquet('{CISFD_DEPOSIT_PARQUET}')
        """
    ).pl()

    cisca = all_cis.filter(pl.col("ACCTNO").is_between(3_000_000_000, 3_999_999_999))
    cisfd = all_cis.filter(
        pl.col("ACCTNO").is_between(1_000_000_000, 1_999_999_999)
        | pl.col("ACCTNO").is_between(7_000_000_000, 7_999_999_999)
    )
    return cisca, cisfd


def build_ca_fd(con: duckdb.DuckDBPyConnection) -> tuple[pl.DataFrame, pl.DataFrame]:
    all_dep = con.execute(
        f"""
        SELECT * FROM read_parquet('{DEPOSIT_CURRENT_PARQUET}')
        UNION ALL
        SELECT * FROM read_parquet('{DEPOSIT_FD_PARQUET}')
        """
    ).pl()

    all_dep = all_dep.filter(pl.col("CURBAL") > 0)
    ca = all_dep.filter(pl.col("ACCTNO").is_between(3_000_000_000, 3_999_999_999))
    fd = all_dep.filter(
        pl.col("ACCTNO").is_between(1_000_000_000, 1_999_999_999)
        | pl.col("ACCTNO").is_between(7_000_000_000, 7_999_999_999)
    )
    return ca, fd


def build_split_sets(ca: pl.DataFrame, fd: pl.DataFrame, cisca: pl.DataFrame, cisfd: pl.DataFrame):
    caj = ca.join(cisca, on="ACCTNO", how="left")
    caj = caj.filter(
        (pl.col("PURPOSE") != "2")
        & (~pl.col("PRODUCT").is_in(CA_EXCL_PRODUCTS))
    ).with_columns(pl.col("CURBAL").alias("CABAL"))

    caind = caj.filter(pl.col("CUSTCODE").is_in([77, 78, 95, 96]))
    caorg = caj.filter(~pl.col("CUSTCODE").is_in([77, 78, 95, 96]) & (pl.col("INDORG") == "O"))

    fdj = cisfd.join(fd, on="ACCTNO", how="left")
    fdj = fdj.filter(
        pl.col("CURBAL").is_not_null()
        & (pl.col("PURPOSE") != "2")
        & (~pl.col("PRODUCT").is_in(FD_EXCL_PRODUCTS))
    ).with_columns(pl.col("CURBAL").alias("FDBAL"))

    fdind = fdj.filter(pl.col("CUSTCODE").is_in([77, 78, 95, 96]))
    fdorg = fdj.filter(~pl.col("CUSTCODE").is_in([77, 78, 95, 96]) & (pl.col("INDORG") == "O"))
    return caind, caorg, fdind, fdorg


def fmt_amt(v) -> str:
    if v is None:
        return ""
    return f"{float(v):,.2f}"


def prnrec(data1: pl.DataFrame, title: str, out_path: Path) -> None:
    d1 = data1.filter(pl.col("ICNO").is_not_null() & (pl.col("ICNO") != "")).sort(["ICNO", "CUSTNAME"])
    d2 = d1.group_by(["ICNO", "CUSTNAME"], maintain_order=True).agg(
        [
            pl.col("CURBAL").sum().alias("CURBAL"),
            pl.col("FDBAL").sum().alias("FDBAL"),
            pl.col("CABAL").sum().alias("CABAL"),
        ]
    ).sort("CURBAL", descending=True).head(50)

    # SAS: data3 = rows from data1 matched to top50 ICNO/CUSTNAME
    keys = d2.select(["ICNO", "CUSTNAME"])
    d3 = d1.join(keys, on=["ICNO", "CUSTNAME"], how="inner").sort(["ICNO", "CUSTNAME"])

    writer = AsaReportWriter(out_path)
    writer.new_page([
        "PUBLIC ISLAMIC BANK BERHAD (EIIMBTOP5)",
        title,
    ])

    writer.line("TOP 50 SUMMARY")
    writer.line(f"{'DEPOSITOR':40} {'TOTAL BALANCE':>18} {'FD BALANCE':>18} {'CA BALANCE':>18}")
    writer.line("-" * 98)
    for row in d2.iter_rows(named=True):
        writer.line(
            f"{str(row.get('CUSTNAME',''))[:40]:40} "
            f"{fmt_amt(row.get('CURBAL')):>18} "
            f"{fmt_amt(row.get('FDBAL')):>18} "
            f"{fmt_amt(row.get('CABAL')):>18}"
        )

    writer.line("")
    writer.line("DETAIL")
    writer.line(
        f"{'ICNO':15} {'BRANCH':>6} {'MNI NO':>12} {'DEPOSITOR':30} {'CIS NO':>10} {'CUR BAL':>16} {'PRODUCT':>8} {'COSTCTR':>8}"
    )
    writer.line("-" * 120)

    current_key = None
    subtotal = 0.0
    for row in d3.iter_rows(named=True):
        key = (row.get("ICNO"), row.get("CUSTNAME"))
        bal = float(row.get("CURBAL") or 0)
        if current_key is not None and key != current_key:
            writer.line(f"{'':15} {'':6} {'':12} {'SUBTOTAL':30} {'':10} {fmt_amt(subtotal):>16}")
            writer.line("")
            subtotal = 0.0
        current_key = key
        subtotal += bal
        writer.line(
            f"{str(row.get('ICNO',''))[:15]:15} "
            f"{str(row.get('BRANCH',''))[:6]:>6} "
            f"{str(row.get('ACCTNO',''))[:12]:>12} "
            f"{str(row.get('CUSTNAME',''))[:30]:30} "
            f"{str(row.get('CUSTNO',''))[:10]:>10} "
            f"{fmt_amt(row.get('CURBAL')):>16} "
            f"{str(row.get('PRODUCT',''))[:8]:>8} "
            f"{str(row.get('COSTCTR',''))[:8]:>8}"
        )

    if current_key is not None:
        writer.line(f"{'':15} {'':6} {'':12} {'SUBTOTAL':30} {'':10} {fmt_amt(subtotal):>16}")

    writer.save()


def write_subs_report(data1: pl.DataFrame, rdate: str, out_path: Path) -> None:
    d = data1.filter(pl.col("CUSTNO").is_in(SUBS_CUSTNOS)).sort(["CUSTNO", "ACCTNO"])

    writer = AsaReportWriter(out_path)
    writer.new_page([
        "PUBLIC ISLAMIC BANK BERHAD (EIIMBTOP5)",
        f"PB SUBSIDIARIES UNDER TOP 50 CORP DEPOSITORS @ {rdate}",
    ])
    writer.line(
        f"{'CUSTNO':>10} {'BRANCH':>6} {'MNI NO':>12} {'DEPOSITOR':30} {'CUSTCD':>6} {'CUR BAL':>16} {'PRODUCT':>8} {'COSTCTR':>8}"
    )
    writer.line("-" * 110)

    current_cust = None
    subtotal = 0.0
    for row in d.iter_rows(named=True):
        custno = row.get("CUSTNO")
        bal = float(row.get("CURBAL") or 0)
        if current_cust is not None and custno != current_cust:
            writer.line(f"{'':10} {'':6} {'':12} {'SUBTOTAL':30} {'':6} {fmt_amt(subtotal):>16}")
            writer.line("")
            subtotal = 0.0
        current_cust = custno
        subtotal += bal
        writer.line(
            f"{str(custno)[:10]:>10} "
            f"{str(row.get('BRANCH',''))[:6]:>6} "
            f"{str(row.get('ACCTNO',''))[:12]:>12} "
            f"{str(row.get('CUSTNAME',''))[:30]:30} "
            f"{str(row.get('CUSTCODE',''))[:6]:>6} "
            f"{fmt_amt(row.get('CURBAL')):>16} "
            f"{str(row.get('PRODUCT',''))[:8]:>8} "
            f"{str(row.get('COSTCTR',''))[:8]:>8}"
        )

    if current_cust is not None:
        writer.line(f"{'':10} {'':6} {'':12} {'SUBTOTAL':30} {'':6} {fmt_amt(subtotal):>16}")

    writer.save()


def main() -> None:
    con = duckdb.connect(database=":memory:")
    reptdate = read_reptdate(con)
    rdate = reptdate.strftime("%d/%m/%y")

    cisca, cisfd = build_cisca_cisfd(con)
    ca, fd = build_ca_fd(con)
    caind, caorg, fdind, fdorg = build_split_sets(ca, fd, cisca, cisfd)

    data1_ind = pl.concat([fdind, caind], how="diagonal_relaxed").with_columns(
        pl.when(pl.col("ICNO") == "  ").then(pl.lit("XX")).otherwise(pl.col("ICNO")).alias("ICNO")
    )
    prnrec(data1_ind, f"TOP 50 LARGEST FD+CA INDIVIDUAL CUSTOMERS AS AT {rdate}", FD11TEXT_OUT)

    data1_corp = pl.concat([fdorg, caorg], how="diagonal_relaxed").with_columns(
        pl.when(pl.col("ICNO") == "  ").then(pl.lit("XX")).otherwise(pl.col("ICNO")).alias("ICNO")
    )
    prnrec(data1_corp, f"TOP 50 LARGEST FD+CA CORPORATE CUSTOMERS AS AT {rdate}", FD12TEXT_OUT)

    data_sub = pl.concat([fdorg, caorg], how="diagonal_relaxed")
    write_subs_report(data_sub, rdate, FDSTEXT_OUT)

    con.close()


if __name__ == "__main__":
    main()
