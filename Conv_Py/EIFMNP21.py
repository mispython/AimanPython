#!/usr/bin/env python3
"""
Program: EIFMNP21
Purpose: Convert SAS PROC TABULATE reports to flat text output.
         Repts to flat file for downloading by accounts, PFB.

Outputs one report text file with ASA carriage control characters:
1) Movements of Interest in Suspense
2) Movements of Specific Provision (based on purchase price less depreciation)
3) Movements of Specific Provision (based on depreciated PP for unscheduled goods)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Paths and runtime constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(".")
INPUT_REPTDATE_PARQUET = BASE_DIR / "NPL.REPTDATE.parquet"
INPUT_IIS_PARQUET = BASE_DIR / "NPL.IIS.parquet"
INPUT_SP1_PARQUET = BASE_DIR / "NPL.SP1.parquet"
INPUT_SP2_PARQUET = BASE_DIR / "NPL.SP2.parquet"
OUTPUT_REPORT_TXT = BASE_DIR / "OUTFL.txt"

PAGE_LENGTH = 60
TABLE_SUFFIX = "(EXISTING AND CURRENT)"
TITLE_PREFIX = "PUBLIC BANK - (NPL FROM 6 MONTHS & ABOVE)"


@dataclass(frozen=True)
class Metric:
    name: str
    label: str
    width: int = 15
    decimals: int = 2


def to_worddatx18(value: object) -> str:
    """Approximate SAS WORDDATX18. output (e.g., March 31, 1998)."""
    if isinstance(value, datetime):
        dt = value.date()
    elif isinstance(value, date):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value).date()
        except ValueError:
            dt = datetime.strptime(value[:10], "%Y-%m-%d").date()
    else:
        raise ValueError(f"Unsupported REPTDATE type: {type(value)}")
    return dt.strftime("%B %d, %Y")


def fetch_reptdate(conn: duckdb.DuckDBPyConnection) -> str:
    reptdate_df = conn.execute(
        "SELECT REPTDATE FROM read_parquet(?) LIMIT 1",
        [str(INPUT_REPTDATE_PARQUET)],
    ).pl()
    if reptdate_df.height == 0:
        raise ValueError("NPL.REPTDATE.parquet is empty.")
    return to_worddatx18(reptdate_df[0, "REPTDATE"])


def load_parquet(conn: duckdb.DuckDBPyConnection, path: Path) -> pl.DataFrame:
    return conn.execute("SELECT * FROM read_parquet(?)", [str(path)]).pl()


def aggregate(df: pl.DataFrame, keys: list[str], metrics: Iterable[str]) -> pl.DataFrame:
    agg_exprs: list[pl.Expr] = [pl.len().alias("N")]
    for m in metrics:
        agg_exprs.append(pl.col(m).cast(pl.Float64, strict=False).fill_null(0.0).sum().alias(m))
    return df.group_by(keys).agg(agg_exprs)


def fmt_number(val: object, width: int = 15, decimals: int = 2) -> str:
    num = 0.0 if val is None else float(val)
    return f"{num:>{width},.{decimals}f}"


def fmt_count(val: object, width: int = 10) -> str:
    cnt = 0 if val is None else int(val)
    return f"{cnt:>{width},d}"


class AsaReportWriter:
    def __init__(self, path: Path, page_length: int = PAGE_LENGTH) -> None:
        self.path = path
        self.page_length = page_length
        self.page_line = 0
        self._fh = path.open("w", encoding="utf-8", newline="\n")

    def close(self) -> None:
        self._fh.close()

    def _write_raw(self, cc: str, text: str = "") -> None:
        self._fh.write(f"{cc}{text}\n")

    def new_page(self, title2: str, title3: str | None = None) -> None:
        self._write_raw("1", TITLE_PREFIX)
        self._write_raw(" ", title2)
        if title3:
            self._write_raw(" ", title3)
            self._write_raw(" ")
            self.page_line = 4
        else:
            self._write_raw(" ")
            self.page_line = 3

    def write_line(self, text: str = "") -> None:
        self._write_raw(" ", text)
        self.page_line += 1

    def ensure_space(self, lines_needed: int, title2: str, title3: str | None = None) -> None:
        if self.page_line + lines_needed > self.page_length:
            self.new_page(title2=title2, title3=title3)


def write_table_header(writer: AsaReportWriter, row_hdr: str, metrics: list[Metric]) -> None:
    left_width = 35
    hdr1 = f"{row_hdr:<{left_width}}{'NO OF ACCOUNT':>13}"
    hdr2 = f"{'':<{left_width}}{'(N)':>13}"
    for m in metrics:
        hdr1 += f"{m.label[:m.width]:>{m.width}}"
        hdr2 += f"{'':>{m.width}}"
    writer.write_line(hdr1)
    writer.write_line(hdr2)
    writer.write_line("-" * len(hdr1))


def write_t1(
    writer: AsaReportWriter,
    df: pl.DataFrame,
    reptdate_label: str,
) -> None:
    title2 = f"MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING {reptdate_label} {TABLE_SUFFIX}"
    metrics = [
        Metric("CURBAL", "CURRENT BAL(A)"),
        Metric("UHC", "UHC(B)"),
        Metric("NETBAL", "NET BAL(C)"),
        Metric("IISP", "OPEN IIS(D)"),
        Metric("SUSPEND", "SUSP(E)"),
        Metric("RECOVER", "RECOV(F)"),
        Metric("RECC", "REVERS(G)"),
        Metric("IISPW", "W/O(H)"),
        Metric("IIS", "IIS(I)"),
        Metric("OIP", "OPEN OI(J)"),
        Metric("OISUSP", "OI SUSP(K)"),
        Metric("OIRECV", "OI REC(L)"),
        Metric("OIRECC", "OI REV(M)"),
        Metric("OIW", "OI W/O(N)"),
        Metric("OI", "OI(O)"),
        Metric("TOTIIS", "TOTAL(I+O)"),
    ]

    grouped = aggregate(df, ["LOANTYP", "BRANCH"], [m.name for m in metrics]).sort(["LOANTYP", "BRANCH"])
    total = aggregate(df, [], [m.name for m in metrics])

    writer.new_page(title2=title2)
    write_table_header(writer, "LOAN TYPE / BRANCH", metrics)

    current_lt = None
    for row in grouped.iter_rows(named=True):
        loantyp = str(row["LOANTYP"])
        if loantyp != current_lt:
            writer.ensure_space(3, title2)
            writer.write_line("")
            writer.write_line(f"{loantyp}")
            current_lt = loantyp
        line = f"  {str(row['BRANCH']):<33}{fmt_count(row['N'])}"
        for m in metrics:
            line += fmt_number(row[m.name], width=m.width, decimals=m.decimals)
        writer.write_line(line)

    grand = total.row(0, named=True)
    writer.write_line("=" * 40)
    line = f"{'TOTAL':<35}{fmt_count(grand['N'])}"
    for m in metrics:
        line += fmt_number(grand[m.name], width=m.width, decimals=m.decimals)
    writer.write_line(line)


def write_t2_or_t3(
    writer: AsaReportWriter,
    df: pl.DataFrame,
    reptdate_label: str,
    opening_metric: str,
    title3: str,
    include_branch_only_view: bool,
) -> None:
    title2 = f"MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING {reptdate_label} {TABLE_SUFFIX}"
    metrics = [
        Metric("CURBAL", "CURRENT BAL(A)"),
        Metric("UHC", "UHC(B)"),
        Metric("NETBAL", "NET BAL(C)"),
        Metric("IIS", "IIS(E)"),
        Metric("OSPRIN", "OSPRIN(F)"),
        Metric("MARKETVL", "MARKET VL(G)"),
        Metric("NETEXP", "NET EXP(H)"),
        Metric(opening_metric, "OPEN(I)"),
        Metric("SPPL", "P&L(J)"),
        Metric("RECOVER", "RECOV(K)"),
        Metric("SPPW", "W/O(L)"),
        Metric("SP", "CLOSING"),
    ]

    key_view = aggregate(df, ["LOANTYP", "RISK", "BRANCH"], [m.name for m in metrics]).sort(
        ["LOANTYP", "RISK", "BRANCH"]
    )
    risk_sub = aggregate(df, ["LOANTYP", "RISK"], [m.name for m in metrics]).sort(["LOANTYP", "RISK"])
    lt_total = aggregate(df, ["LOANTYP"], [m.name for m in metrics]).sort(["LOANTYP"])
    grand = aggregate(df, [], [m.name for m in metrics]).row(0, named=True)

    writer.new_page(title2=title2, title3=title3)
    write_table_header(writer, "LOAN TYPE / RISK / BRANCH", metrics)

    current_lt = None
    current_risk = None
    for row in key_view.iter_rows(named=True):
        loantyp = str(row["LOANTYP"])
        risk = str(row["RISK"])
        branch = str(row["BRANCH"])

        if loantyp != current_lt:
            writer.ensure_space(3, title2, title3)
            writer.write_line("")
            writer.write_line(loantyp)
            current_lt = loantyp
            current_risk = None

        if risk != current_risk:
            writer.write_line(f"  {risk}")
            current_risk = risk

        line = f"    {branch:<31}{fmt_count(row['N'])}"
        for m in metrics:
            line += fmt_number(row[m.name], width=m.width, decimals=m.decimals)
        writer.write_line(line)

    writer.write_line("-" * 40)
    for row in risk_sub.iter_rows(named=True):
        line = f"SUB-TOTAL {str(row['LOANTYP'])}/{str(row['RISK']):<22}{fmt_count(row['N'])}"
        for m in metrics:
            line += fmt_number(row[m.name], width=m.width, decimals=m.decimals)
        writer.write_line(line)

    for row in lt_total.iter_rows(named=True):
        line = f"TOTAL {str(row['LOANTYP']):<29}{fmt_count(row['N'])}"
        for m in metrics:
            line += fmt_number(row[m.name], width=m.width, decimals=m.decimals)
        writer.write_line(line)

    line = f"{'GRAND TOTAL':<35}{fmt_count(grand['N'])}"
    for m in metrics:
        line += fmt_number(grand[m.name], width=m.width, decimals=m.decimals)
    writer.write_line(line)

    if include_branch_only_view:
        branch_view = aggregate(df, ["LOANTYP", "BRANCH"], [m.name for m in metrics]).sort(["LOANTYP", "BRANCH"])
        writer.new_page(title2=title2, title3=title3)
        write_table_header(writer, "LOAN TYPE / BRANCH", metrics)

        current_lt = None
        for row in branch_view.iter_rows(named=True):
            loantyp = str(row["LOANTYP"])
            if loantyp != current_lt:
                writer.write_line("")
                writer.write_line(loantyp)
                current_lt = loantyp
            line = f"  {str(row['BRANCH']):<33}{fmt_count(row['N'])}"
            for m in metrics:
                line += fmt_number(row[m.name], width=m.width, decimals=m.decimals)
            writer.write_line(line)

        line = f"{'TOTAL':<35}{fmt_count(grand['N'])}"
        for m in metrics:
            line += fmt_number(grand[m.name], width=m.width, decimals=m.decimals)
        writer.write_line(line)


def main() -> None:
    conn = duckdb.connect()
    reptdate_label = fetch_reptdate(conn)

    iis = load_parquet(conn, INPUT_IIS_PARQUET)
    sp1 = load_parquet(conn, INPUT_SP1_PARQUET)
    sp2 = load_parquet(conn, INPUT_SP2_PARQUET)

    writer = AsaReportWriter(OUTPUT_REPORT_TXT, page_length=PAGE_LENGTH)
    try:
        write_t1(writer, iis, reptdate_label)
        write_t2_or_t3(
            writer,
            sp1,
            reptdate_label,
            opening_metric="SPP1",
            title3="(BASED ON PURCHASE PRICE LESS DEPRECIATION)",
            include_branch_only_view=False,
        )
        write_t2_or_t3(
            writer,
            sp2,
            reptdate_label,
            opening_metric="SPP2",
            title3="(BASED ON DEPRECIATED PP FOR UNSCHEDULED GOODS)",
            include_branch_only_view=True,
        )
    finally:
        writer.close()


if __name__ == "__main__":
    main()
