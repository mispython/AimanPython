#!/usr/bin/env python3
"""
Program: EIBWRDLB

Reads pre-converted parquet datasets and writes RDAL output text:

- Header line: RDAL<REPTDAY><REPTMON><REPTYEAR>
- Section markers: AL / OB / SP
- AL and OB records: ITCODE;TOTAL_D_OR_D_PLUS_I;TOTAL_I
- SP records: ITCODE;TOTAL
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import math
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl


@dataclass(frozen=True)
class Paths:
    """Filesystem paths used by the program."""

    base_dir: Path
    input_bnm_dir: Path
    input_aux_dir: Path
    output_dir: Path
    alwwk_path: Path
    k3fei_path: Path
    rdalwk_output_path: Path


@dataclass(frozen=True)
class RuntimeParams:
    """Equivalent SAS macro variables."""

    reptday: str
    reptmon: str
    reptyear: str


def parse_args() -> argparse.Namespace:
    base_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="EIBWRDLB program")
    parser.add_argument("--base-dir", type=Path, default=base_dir)
    parser.add_argument("--input-bnm-dir", type=Path, default=base_dir / "data" / "bnm")
    parser.add_argument("--input-aux-dir", type=Path, default=base_dir / "output")
    parser.add_argument("--output-dir", type=Path, default=base_dir / "output")
    parser.add_argument("--alwwk-file", default="ALWWK.parquet")
    parser.add_argument("--k3fei-file", default="K3FEI.parquet")
    parser.add_argument("--output-file", default="RDALWK.txt")
    parser.add_argument("--reptday", required=True)
    parser.add_argument("--reptmon", required=True)
    parser.add_argument("--reptyear", required=True)
    return parser.parse_args()


def build_paths(args: argparse.Namespace) -> Paths:
    paths = Paths(
        base_dir=args.base_dir,
        input_bnm_dir=args.input_bnm_dir,
        input_aux_dir=args.input_aux_dir,
        output_dir=args.output_dir,
        alwwk_path=args.input_bnm_dir / args.alwwk_file,
        k3fei_path=args.input_aux_dir / args.k3fei_file,
        rdalwk_output_path=args.output_dir / args.output_file,
    )
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    return paths


def load_parquet(con: duckdb.DuckDBPyConnection, path: Path) -> pl.DataFrame:
    return con.execute("SELECT * FROM read_parquet(?)", [str(path)]).pl()


def sas_round(value: float) -> int:
    """Replicate SAS ROUND(..., 1) behavior (half away from zero)."""
    if value >= 0:
        return int(math.floor(value + 0.5))
    return int(math.ceil(value - 0.5))


def filter_rdalwk_weekly(df: pl.DataFrame) -> pl.DataFrame:
    prefix5 = pl.col("ITCODE").str.slice(0, 5)
    unwanted = (
        prefix5.is_between("30221", "30228")
        | prefix5.is_between("30231", "30238")
        | prefix5.is_between("30091", "30098")
        | prefix5.is_between("40151", "40158")
    )
    return df.filter(~unwanted)


def split_al_ob_sp(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    amtind = pl.col("AMTIND").fill_null(" ")
    non_blank_amtind = amtind != " "

    sp_mask = (
        (non_blank_amtind & pl.col("ITCODE").str.slice(0, 3).is_in(["307"]))
        | (non_blank_amtind & pl.col("ITCODE").str.slice(0, 5).is_in(["40190"]))
        | (
            non_blank_amtind
            & (pl.col("ITCODE").str.slice(0, 1) != "5")
            & pl.col("ITCODE").str.slice(0, 3).is_in(["685", "785"])
        )
        | ((amtind == " ") & (pl.col("ITCODE").str.slice(1, 1) == "0"))
    )

    ob_mask = non_blank_amtind & (pl.col("ITCODE").str.slice(0, 1) == "5")

    al_df = df.filter(~sp_mask & ~ob_mask & non_blank_amtind)
    ob_df = df.filter(ob_mask)
    sp_df = df.filter(sp_mask)
    return al_df, ob_df, sp_df


def group_al_ob(df: pl.DataFrame, reptday: str) -> list[str]:
    lines: list[tuple[str, str, int]] = []
    for row in df.iter_rows(named=True):
        itcode = row["ITCODE"]
        amtind = row["AMTIND"]
        amount = sas_round(float(row["AMOUNT"]) / 1000)

        proceed = True
        if reptday in {"08", "22"}:
            if itcode == "4003000000000Y" and itcode[:2] in {"68", "78"}:
                proceed = False
        if not proceed:
            continue

        lines.append((itcode, amtind, amount))

    # BY ITCODE AMTIND with retained sums and LAST.ITCODE behavior
    output: list[str] = []
    cur_itcode = None
    amountd = 0
    amounti = 0

    for itcode, amtind, amount in lines:
        if cur_itcode is None:
            cur_itcode = itcode
        if itcode != cur_itcode:
            amountd += amounti
            output.append(f"{cur_itcode};{amountd};{amounti}")
            cur_itcode = itcode
            amountd = 0
            amounti = 0

        if amtind == "D":
            amountd += amount
        elif amtind == "I":
            amounti += amount

    if cur_itcode is not None:
        amountd += amounti
        output.append(f"{cur_itcode};{amountd};{amounti}")

    return output


def group_sp(sp_df: pl.DataFrame, k3fei_df: pl.DataFrame) -> list[str]:
    merged = pl.concat([sp_df.select(["ITCODE", "AMOUNT"]), k3fei_df.select(["ITCODE", "AMOUNT"])])
    merged = merged.sort("ITCODE")

    output: list[str] = []
    cur_itcode = None
    amount_sum = 0.0

    for row in merged.iter_rows(named=True):
        itcode = row["ITCODE"]
        amount = float(row["AMOUNT"]) if row["AMOUNT"] is not None else 0.0

        if cur_itcode is None:
            cur_itcode = itcode
        if itcode != cur_itcode:
            output.append(f"{cur_itcode};{sas_round(amount_sum / 1000)}")
            cur_itcode = itcode
            amount_sum = 0.0

        amount_sum += amount

    if cur_itcode is not None:
        output.append(f"{cur_itcode};{sas_round(amount_sum / 1000)}")

    return output


def write_output(path: Path, phead: str, al_lines: Iterable[str], ob_lines: Iterable[str], sp_lines: Iterable[str]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as f:
        f.write(f"{phead}\n")
        f.write("AL\n")
        for line in al_lines:
            f.write(f"{line}\n")

        f.write("OB\n")
        for line in ob_lines:
            f.write(f"{line}\n")

        f.write("SP\n")
        for line in sp_lines:
            f.write(f"{line}\n")


def main() -> None:
    args = parse_args()
    paths = build_paths(args)
    params = RuntimeParams(reptday=args.reptday, reptmon=args.reptmon, reptyear=args.reptyear)

    con = duckdb.connect()
    try:
        rdalwk_raw = load_parquet(con, paths.alwwk_path)
        k3fei_df = load_parquet(con, paths.k3fei_path)
    finally:
        con.close()

    rdalwk_filtered = filter_rdalwk_weekly(rdalwk_raw).sort(["ITCODE", "AMTIND"])
    al_df, ob_df, sp_df = split_al_ob_sp(rdalwk_filtered)

    al_lines = group_al_ob(al_df, params.reptday)
    ob_lines = group_al_ob(ob_df, params.reptday)
    sp_lines = group_sp(sp_df, k3fei_df)

    phead = f"RDAL{params.reptday}{params.reptmon}{params.reptyear}"
    write_output(paths.rdalwk_output_path, phead, al_lines, ob_lines, sp_lines)

    print(f"Wrote output: {paths.rdalwk_output_path}")
    print(f"AL records: {len(al_lines)}")
    print(f"OB records: {len(ob_lines)}")
    print(f"SP records: {len(sp_lines)}")


if __name__ == "__main__":
    main()
