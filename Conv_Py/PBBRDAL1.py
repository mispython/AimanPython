#!/usr/bin/env python3
"""
Program: PBBRDAL1
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb
import polars as pl


@dataclass(frozen=True)
class Paths:
    input_dir: Path
    output_dir: Path

    def walw_file(self, reptmon: str, nowk: str) -> Path:
        return self.input_dir / f"WALW{reptmon}{nowk}.parquet"

    def other_files(self, reptmon: str, nowk: str) -> list[Path]:
        return [
            self.input_dir / f"DALW{reptmon}{nowk}.parquet",
            self.input_dir / f"FALW{reptmon}{nowk}.parquet",
            self.input_dir / f"LALW{reptmon}{nowk}.parquet",
            self.input_dir / f"KALW{reptmon}{nowk}.parquet",
        ]

    def alw_txt(self, reptmon: str, nowk: str) -> Path:
        return self.output_dir / f"ALW{reptmon}{nowk}.txt"

    def alwlg_txt(self, reptmon: str, nowk: str) -> Path:
        return self.output_dir / f"ALWLG{reptmon}{nowk}.txt"

    def report_txt(self, reptmon: str, nowk: str) -> Path:
        return self.output_dir / f"PBBRDAL1_REPORT_{reptmon}{nowk}.txt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run X_PBBRDAL1 conversion")

    # parser.add_argument("--reptmon", required=True)
    # parser.add_argument("--nowk", required=True)

    # # If want to run with arguments, use these lines
    # parser.add_argument("--reptmon", required=True, help="Reporting month (MM)")
    # parser.add_argument("--nowk", required=True, help="Reporting week (1-4)")

    # If want to run without arguments, use these lines
    parser.add_argument("--reptmon", default="01")
    parser.add_argument("--nowk", default="1")

    parser.add_argument("--sdesc", default="PUBLIC BANK BERHAD")
    parser.add_argument("--rdate", default=date.today().strftime("%d/%m/%Y"))
    parser.add_argument("--input-dir", default="./data/input")
    parser.add_argument("--output-dir", default="./data/output")
    return parser.parse_args()


def load_sources(conn: duckdb.DuckDBPyConnection, paths: Paths, reptmon: str, nowk: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    other_inputs = [p for p in paths.other_files(reptmon, nowk) if p.exists()]
    if not other_inputs:
        other = pl.DataFrame({"BNMCODE": [], "AMTIND": [], "AMOUNT": []}, schema={"BNMCODE": pl.Utf8, "AMTIND": pl.Utf8, "AMOUNT": pl.Float64})
    else:
        union_sql = " UNION ALL ".join(f"SELECT BNMCODE, AMTIND, AMOUNT FROM read_parquet('{p.as_posix()}')" for p in other_inputs)
        other = conn.execute(
            f"""
            SELECT BNMCODE, AMTIND, SUM(AMOUNT) AS AMOUNT
            FROM ({union_sql})
            GROUP BY BNMCODE, AMTIND
            """
        ).pl()

    walw_path = paths.walw_file(reptmon, nowk)
    if walw_path.exists():
        walw = conn.execute(
            f"SELECT BNMCODE, AMTIND, AMOUNT FROM read_parquet('{walw_path.as_posix()}')"
        ).pl()
    else:
        walw = pl.DataFrame({"BNMCODE": [], "AMTIND": [], "AMOUNT": []}, schema={"BNMCODE": pl.Utf8, "AMTIND": pl.Utf8, "AMOUNT": pl.Float64})

    return walw, expand_other_rows(other)


def expand_other_rows(other: pl.DataFrame) -> pl.DataFrame:
    al100 = {"42110", "42120", "42130", "42131", "42132", "42150", "42160", "42170", "42180", "42199", "42510", "42520", "42599", "42190"}
    al500 = {"42510", "42520", "42599"}
    al600 = {"42610", "42620", "42630", "42631", "42632", "42660", "42699"}

    out = [other]

    out.append(
        other.filter(
            pl.col("BNMCODE").str.slice(0, 5).is_in(al100)
            & pl.col("BNMCODE").str.slice(5, 2).is_in(["57", "75"])
            & (pl.col("BNMCODE").str.slice(5, 2) == "75")
        ).with_columns(pl.lit("4210075000000Y").alias("BNMCODE"))
    )
    out.append(
        other.filter(
            pl.col("BNMCODE").str.slice(0, 5).is_in(al100)
            & pl.col("BNMCODE").str.slice(5, 2).is_in(["57", "75"])
            & (pl.col("BNMCODE").str.slice(5, 2) == "57")
        ).with_columns(pl.lit("4210057000000Y").alias("BNMCODE"))
    )

    out.append(
        other.filter(
            pl.col("BNMCODE").str.slice(0, 5).is_in(al500)
            & pl.col("BNMCODE").str.slice(5, 2).is_in(["01", "71"])
            & (pl.col("BNMCODE").str.slice(5, 2) == "75")
        ).with_columns(pl.lit("4250001000000Y").alias("BNMCODE"))
    )
    out.append(
        other.filter(
            pl.col("BNMCODE").str.slice(0, 5).is_in(al500)
            & pl.col("BNMCODE").str.slice(5, 2).is_in(["01", "71"])
            & (pl.col("BNMCODE").str.slice(5, 2) == "57")
        ).with_columns(pl.lit("4250071000000Y").alias("BNMCODE"))
    )

    out.append(
        other.filter(
            pl.col("BNMCODE").str.slice(0, 5).is_in(al600)
            & pl.col("BNMCODE").str.slice(5, 2).is_in(["57", "75"])
            & (pl.col("BNMCODE").str.slice(5, 2) == "75")
        ).with_columns(pl.lit("4260075000000Y").alias("BNMCODE"))
    )
    out.append(
        other.filter(
            pl.col("BNMCODE").str.slice(0, 5).is_in(al600)
            & pl.col("BNMCODE").str.slice(5, 2).is_in(["57", "75"])
            & (pl.col("BNMCODE").str.slice(5, 2) == "57")
        ).with_columns(pl.lit("4260057000000Y").alias("BNMCODE"))
    )

    return pl.concat(out)


def build_outputs(walw: pl.DataFrame, other: pl.DataFrame, nowk: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    merged = walw.rename({"AMOUNT": "WISAMT"}).join(
        other.rename({"AMOUNT": "OTHAMT"}),
        on=["BNMCODE", "AMTIND"],
        how="full",
        coalesce=True,
    ).with_columns(
        pl.col("BNMCODE").cast(pl.Utf8),
        pl.col("AMTIND").cast(pl.Utf8),
        pl.lit("Y").alias("WEEKLY"),
    )

    alw_rows: list[dict] = []
    alwlg_rows: list[dict] = []
    summary_rows: list[dict] = []

    monthly_filter = {"35", "36", "37", "59"}
    skip_prefix3 = {"821", "414", "411", "391", "392", "491", "492"}

    for row in merged.iter_rows(named=True):
        bn = row["BNMCODE"] or ""
        ai = row["AMTIND"] if row["AMTIND"] is not None else ""
        wis = float(row["WISAMT"]) if row["WISAMT"] is not None else None
        oth = float(row["OTHAMT"]) if row["OTHAMT"] is not None else None
        a = wis is not None
        b = oth is not None
        weekly = "Y"

        def emit_alw(code: str, amount: float, amtind: str) -> None:
            alw_rows.append({"ITCODE": code, "AMOUNT": amount, "AMTIND": amtind})

        prefix2, prefix3, prefix4, prefix5, prefix7 = bn[:2], bn[:3], bn[:4], bn[:5], bn[:7]
        if prefix2 not in monthly_filter and prefix3 not in skip_prefix3:
            if prefix2 == "30":
                ai = " "
            if a and b:
                amount = oth
                if prefix7 == "4219976":
                    amount = wis + oth
                emit_alw(bn, amount, ai)
                alwlg_rows.append({"ITCODE": bn, "WISAMT": wis, "OTHAMT": oth, "AMTIND": ai})
            elif a and not b:
                emit_alw(bn, wis, ai)
            elif b and not a:
                emit_alw(bn, oth, ai)
            continue

        if nowk == "4":
            weekly = "N"

        amount = None
        if a and b:
            amount = oth
            if prefix7 in {"4911050", "4911080", "4929950"}:
                amount = wis + oth
        elif a and not b:
            amount = wis
        elif b and not a:
            amount = oth

        if amount is None:
            continue

        if prefix5 in {"42190", "42191", "33140", "43140"} and weekly == "Y":
            emit_alw(bn, amount, ai)
        if prefix2 == "37" and weekly == "Y":
            emit_alw(bn, amount, ai)
        if prefix3 in {"391", "392"}:
            summary_rows.append({"ITCODE": bn, "AMOUNT": amount, "AMTIND": ai})
            if prefix4 == "3911":
                emit_alw(bn, amount, ai)
            if weekly == "Y":
                emit_alw("3910000000000Y", amount, ai)
        if prefix4 in {"4110", "4111", "4140"} and weekly == "Y":
            emit_alw("4100000000000Y", amount, ai)
        if prefix3 in {"491", "492"}:
            summary_rows.append({"ITCODE": bn, "AMOUNT": amount, "AMTIND": ai})
            if prefix7 in {"4911050", "4911080", "4912050", "4912080", "4929950", "4929980", "4929000"}:
                emit_alw(bn, amount, ai)
            if weekly == "Y":
                emit_alw("4910000000000Y", amount, ai)

    alw = pl.DataFrame(alw_rows, schema={"ITCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTIND": pl.Utf8})
    alwlg = pl.DataFrame(alwlg_rows, schema={"ITCODE": pl.Utf8, "WISAMT": pl.Float64, "OTHAMT": pl.Float64, "AMTIND": pl.Utf8})
    summary = pl.DataFrame(summary_rows, schema={"ITCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTIND": pl.Utf8})
    return alw, alwlg, summary


def asa_report(alwlg: pl.DataFrame, sdesc: str, rdate: str, page_len: int = 60) -> str:
    header = [
        f"{sdesc}",
        "REPORT ON DOMESTIC ASSETS AND LIABILITIES - PART I",
        "COMPARISON REPORT BETWEEN WALKER AND OTHER SOURCES",
        f"REPORT DATE : {rdate}",
        "",
        f"{'BNM CODE':<16}{'WALKER AMOUNT':>25}{'AMOUNT FROM OTHER SOURCES':>30}",
    ]
    lines: list[str] = []
    line_in_page = 0

    def write_header(first: bool) -> None:
        nonlocal line_in_page
        ctrl = "1" if first else "1"
        lines.append(ctrl + header[0])
        for h in header[1:]:
            lines.append(" " + h)
        line_in_page = len(header)

    write_header(first=True)

    for row in alwlg.iter_rows(named=True):
        if line_in_page >= page_len:
            write_header(first=False)
        lines.append(
            " " + f"{row['ITCODE']:<16}{row['WISAMT']:>25,.2f}{row['OTHAMT']:>30,.2f}"
        )
        line_in_page += 1

    return "\n".join(lines) + "\n"


def save_outputs(paths: Paths, reptmon: str, nowk: str, alw: pl.DataFrame, alwlg: pl.DataFrame, summary: pl.DataFrame, sdesc: str, rdate: str) -> None:
    paths.output_dir.mkdir(parents=True, exist_ok=True)

    alw.write_csv(paths.alw_txt(reptmon, nowk), separator="|", include_header=True)
    alwlg.write_csv(paths.alwlg_txt(reptmon, nowk), separator="|", include_header=True)

    summary_391 = summary.filter(pl.col("ITCODE").str.slice(0, 3).is_in(["391", "392"]) & (pl.col("AMTIND") != " "))
    summary_491 = summary.filter(pl.col("ITCODE").str.slice(0, 3).is_in(["491", "492"]) & (pl.col("AMTIND") != " "))

    total_391 = float(summary_391.select(pl.col("AMOUNT").sum()).item() or 0.0)
    total_491 = float(summary_491.select(pl.col("AMOUNT").sum()).item() or 0.0)

    report = asa_report(alwlg, sdesc=sdesc, rdate=rdate)
    report += f" 39100 TOTAL : {total_391:,.2f}\n"
    report += f" 49100 TOTAL : {total_491:,.2f}\n"

    paths.report_txt(reptmon, nowk).write_text(report, encoding="utf-8")


def main() -> None:
    args = parse_args()
    reptmon = args.reptmon.zfill(2)
    nowk = str(args.nowk)

    paths = Paths(input_dir=Path(args.input_dir), output_dir=Path(args.output_dir))
    conn = duckdb.connect()
    walw, other = load_sources(conn, paths, reptmon, nowk)
    alw, alwlg, summary = build_outputs(walw, other, nowk)
    save_outputs(paths, reptmon, nowk, alw, alwlg, summary, args.sdesc, args.rdate)

    print(f"Generated: {paths.alw_txt(reptmon, nowk)}")
    print(f"Generated: {paths.alwlg_txt(reptmon, nowk)}")
    print(f"Generated: {paths.report_txt(reptmon, nowk)}")


if __name__ == "__main__":
    main()

