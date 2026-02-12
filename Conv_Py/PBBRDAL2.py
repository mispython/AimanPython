#!/usr/bin/env python3
"""
Program: PBBRDAL2

Assumptions:
- Input datasets are parquet files with SAS-compatible column names.
- Output datasets are serialized as text files.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import duckdb
import polars as pl


@dataclass(frozen=True)
class Paths:
    input_dir: Path
    output_dir: Path

    def dalm(self, reptmon: str, nowk: str) -> Path:
        return self.input_dir / f"DALM{reptmon}{nowk}.parquet"

    def falm(self, reptmon: str, nowk: str) -> Path:
        return self.input_dir / f"FALM{reptmon}{nowk}.parquet"

    def kalm(self, reptmon: str, nowk: str) -> Path:
        return self.input_dir / f"KALM{reptmon}{nowk}.parquet"

    def nalm(self, reptmon: str, nowk: str) -> Path:
        return self.input_dir / f"NALM{reptmon}{nowk}.parquet"

    def lalm(self, reptmon: str, nowk: str) -> Path:
        return self.input_dir / f"LALM{reptmon}{nowk}.parquet"

    def walw(self, reptmon: str, nowk: str) -> Path:
        return self.input_dir / f"WALW{reptmon}{nowk}.parquet"

    def alw(self, reptmon: str, nowk: str) -> Path:
        return self.input_dir / f"ALW{reptmon}{nowk}.parquet"

    def alm_txt(self, reptmon: str) -> Path:
        return self.output_dir / f"ALM{reptmon}.txt"

    def almlg_txt(self, reptmon: str) -> Path:
        return self.output_dir / f"ALMLG{reptmon}.txt"

    def alw_updated_txt(self, reptmon: str, nowk: str) -> Path:
        return self.output_dir / f"ALW{reptmon}{nowk}.txt"


REQUIRED_COLUMNS = ("BNMCODE", "AMTIND", "AMOUNT")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PBBRDAL2")
    parser.add_argument("--reptmon", default="01", help="Reporting month (MM)")
    parser.add_argument("--nowk", default="1", help="Reporting week")
    parser.add_argument("--input-dir", default="./data/input", help="Directory containing parquet input files")
    parser.add_argument("--output-dir", default="./data/output", help="Directory for generated text outputs")
    parser.add_argument("--delimiter", default="|", help="Field delimiter for text output")
    parser.add_argument("--include-header", action="store_true", help="Include header row in output text files")
    return parser.parse_args()


def assert_inputs_exist(paths: list[Path]) -> None:
    missing = [path.as_posix() for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required input parquet file(s): {', '.join(missing)}")


def load_grouped_other(conn: duckdb.DuckDBPyConnection, source_files: list[Path]) -> pl.DataFrame:
    union_sql = " UNION ALL ".join(
        f"SELECT BNMCODE, AMTIND, AMOUNT FROM read_parquet('{path.as_posix()}')" for path in source_files
    )
    return conn.execute(
        f"""
        SELECT BNMCODE, AMTIND, SUM(AMOUNT) AS AMOUNT
        FROM ({union_sql})
        GROUP BY BNMCODE, AMTIND
        """
    ).pl()


def load_table(conn: duckdb.DuckDBPyConnection, path: Path) -> pl.DataFrame:
    return conn.execute(
        f"SELECT {', '.join(REQUIRED_COLUMNS)} FROM read_parquet('{path.as_posix()}')"
    ).pl()


def load_alw(conn: duckdb.DuckDBPyConnection, path: Path) -> pl.DataFrame:
    return conn.execute(
        f"SELECT ITCODE, AMTIND, AMOUNT FROM read_parquet('{path.as_posix()}')"
    ).pl()


def build_alm_and_almlg(walw: pl.DataFrame, other: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    merged = walw.rename({"AMOUNT": "WISAMT"}).join(
        other.rename({"AMOUNT": "OTHAMT"}),
        on=["BNMCODE", "AMTIND"],
        how="full",
        coalesce=True,
    )

    alm_rows: list[dict[str, object]] = []
    almlg_rows: list[dict[str, object]] = []

    for row in merged.iter_rows(named=True):
        itcode = row["BNMCODE"]
        amtind = row["AMTIND"]
        wisamt = row["WISAMT"]
        othamt = row["OTHAMT"]

        in_walw = wisamt is not None
        in_other = othamt is not None

        if in_walw and in_other:
            alm_rows.append({"ITCODE": itcode, "AMOUNT": othamt, "AMTIND": amtind})
            almlg_rows.append({"ITCODE": itcode, "WISAMT": wisamt, "OTHAMT": othamt, "AMTIND": amtind})
        elif in_walw:
            alm_rows.append({"ITCODE": itcode, "AMOUNT": wisamt, "AMTIND": amtind})
        elif in_other:
            alm_rows.append({"ITCODE": itcode, "AMOUNT": othamt, "AMTIND": amtind})

    alm = pl.DataFrame(alm_rows, schema={"ITCODE": pl.Utf8, "AMOUNT": pl.Float64, "AMTIND": pl.Utf8})
    almlg = pl.DataFrame(almlg_rows, schema={"ITCODE": pl.Utf8, "WISAMT": pl.Float64, "OTHAMT": pl.Float64, "AMTIND": pl.Utf8})
    return alm, almlg


def build_alm_append_rows(alw: pl.DataFrame, alm: pl.DataFrame) -> pl.DataFrame:
    return alm.join(
        alw.select(["ITCODE", "AMTIND"]),
        on=["ITCODE", "AMTIND"],
        how="anti",
    ).filter(
        pl.col("ITCODE").is_not_null() & (pl.col("ITCODE").str.slice(0, 2) != "37")
    )


def save_txt(df: pl.DataFrame, path: Path, delimiter: str, include_header: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(path, separator=delimiter, include_header=include_header)


def run(paths: Paths, reptmon: str, nowk: str, delimiter: str, include_header: bool) -> None:
    source_files = [
        paths.dalm(reptmon, nowk),
        paths.falm(reptmon, nowk),
        paths.kalm(reptmon, nowk),
        paths.nalm(reptmon, nowk),
        paths.lalm(reptmon, nowk),
        paths.walw(reptmon, nowk),
        paths.alw(reptmon, nowk),
    ]
    assert_inputs_exist(source_files)

    conn = duckdb.connect()
    try:
        other = load_grouped_other(conn, source_files[:5])
        walw = load_table(conn, paths.walw(reptmon, nowk))
        alw = load_alw(conn, paths.alw(reptmon, nowk))

        alm, almlg = build_alm_and_almlg(walw=walw, other=other)
        alm_append = build_alm_append_rows(alw=alw, alm=alm)
        alw_updated = pl.concat([alw, alm_append], how="vertical")

        save_txt(alm, paths.alm_txt(reptmon), delimiter=delimiter, include_header=include_header)
        save_txt(almlg, paths.almlg_txt(reptmon), delimiter=delimiter, include_header=include_header)
        save_txt(alw_updated, paths.alw_updated_txt(reptmon, nowk), delimiter=delimiter, include_header=include_header)
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    reptmon = args.reptmon.zfill(2)
    nowk = str(args.nowk)

    paths = Paths(input_dir=Path(args.input_dir), output_dir=Path(args.output_dir))
    run(paths=paths, reptmon=reptmon, nowk=nowk, delimiter=args.delimiter, include_header=args.include_header)

    print(f"Generated: {paths.alm_txt(reptmon)}")
    print(f"Generated: {paths.almlg_txt(reptmon)}")
    print(f"Generated: {paths.alw_updated_txt(reptmon, nowk)}")


if __name__ == "__main__":
    main()
