#!/usr/bin/env python3
"""
Program: PBBBRELP

Assumptions from migration brief:
- Input MELW/WELW datasets are already available as parquet files.
- Columns follow SAS names (BRANCH, REPTDATE, BNMCODE, AMOUNT).
- Output is a text REPORT with ASA carriage control characters.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

# ============================================================
# Path and environment setup (defined early, per requirement)
# ============================================================
BASE_DIR = Path(__file__).resolve().parents[1]
# PBBELF_DIR = BASE_DIR / "PBBELF"
INPUT_PARQUET_DIR = BASE_DIR / "data" / "input" / "parquet"
OUTPUT_DIR = BASE_DIR / "data" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# PBBELF_SOURCE = PBBELF_DIR / "PBBELF"
PBBELF_SOURCE = BASE_DIR / "PBBELF"
OUTPUT_REPORT_FILE = OUTPUT_DIR / "ELIAB.txt"

PAGE_LENGTH = 60


# ============================================================
# Helpers
# ============================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert X_PBBBRELP logic to Python")

    # parser.add_argument("--reptday", type=int, required=True)
    # parser.add_argument("--reptmon", type=str, required=True)

    # # If want to run with arguments, use these lines
    # parser.add_argument("--reptday", type=int, required=True, help="Report day (&REPTDAY)")
    # parser.add_argument("--reptmon", type=str, required=True, help="Report month token (&REPTMON), e.g. 2401")

    # If want to run without arguments, use these lines
    parser.add_argument("--reptday", type=int, default=31)
    parser.add_argument("--reptmon", type=str, default="2401")

    parser.add_argument("--sdesc", type=str, default="PUBLIC BANK BERHAD", help="&SDESC title value")
    parser.add_argument("--rdate", type=str, default=datetime.today().strftime("%d/%m/%Y"), help="&RDATE title value")
    parser.add_argument("--output", type=Path, default=OUTPUT_REPORT_FILE, help="Output report file")
    return parser.parse_args()


def parse_sas_eli_mapping(path: Path) -> pl.DataFrame:
    """Parse DATA ELI cards from X_PBBELF into columns BNMCODE, SIGN, FMTNAME."""
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()

    in_eli = False
    in_cards = False
    rows: list[tuple[str, str, str]] = []

    for raw in lines:
        line = raw.rstrip("\n")
        upper = line.strip().upper()

        if upper.startswith("DATA ELI;"):
            in_eli = True
            continue

        if in_eli and upper == "CARDS;":
            in_cards = True
            continue

        if in_eli and in_cards and upper == ";":
            break

        if not (in_eli and in_cards):
            continue

        body = line.split("/*", 1)[0].strip()
        if not body:
            continue

        parts = body.split()
        if len(parts) < 3:
            continue

        bnmcode, sign, fmtname = parts[0], parts[1], parts[2]
        if len(bnmcode) == 14 and sign in {"+", "-"} and fmtname in {"FXEL", "FXEA", "RMEL", "RMEA"}:
            rows.append((bnmcode, sign, fmtname))

    if not rows:
        raise ValueError(f"No ELI mapping rows parsed from {path}")

    return pl.DataFrame(rows, schema=["BNMCODE", "SIGN", "FMTNAME"], orient="row")


def day_range(reptday: int) -> tuple[int, int]:
    return (1, 2) if reptday == 15 else (3, 4)


def branch_where_clause(report_kind: str) -> str:
    if report_kind == "BRCHEL":
        return "BRANCH NOT IN (996, 997, 998, 701)"
    if report_kind == "CONVEL":
        return "((BRANCH < 3000 AND BRANCH NOT IN (996, 997, 998, 701)) OR (BRANCH > 3999 AND BRANCH NOT IN (4043, 4048)))"
    if report_kind == "ISLEL":
        return "((BRANCH BETWEEN 3000 AND 3999) OR BRANCH IN (4043, 4048))"
    raise ValueError(f"Unsupported report kind: {report_kind}")


def build_elw_for_day(conn: duckdb.DuckDBPyConnection, reptmon: str, day_i: int, where_sql: str) -> pl.DataFrame:
    melw_path = INPUT_PARQUET_DIR / f"MELW{reptmon}{day_i}.parquet"
    welw_path = INPUT_PARQUET_DIR / f"WELW{reptmon}{day_i}.parquet"

    query = f"""
    WITH melw0 AS (
        SELECT BRANCH, REPTDATE, BNMCODE, SUM(AMOUNT) AS AMOUNT
        FROM parquet_scan('{melw_path.as_posix()}')
        WHERE {where_sql}
        GROUP BY BRANCH, REPTDATE, BNMCODE
    ),
    melw AS (
        SELECT
            BRANCH,
            REPTDATE,
            CASE WHEN SUBSTR(BNMCODE, 1, 7) = '4939980' THEN '4929980000000Y' ELSE BNMCODE END AS BNMCODE,
            SUM(AMOUNT) AS MNIAMT
        FROM melw0
        GROUP BY BRANCH, REPTDATE, 3
    ),
    welw AS (
        SELECT BRANCH, REPTDATE, BNMCODE, SUM(AMOUNT) AS WISAMT
        FROM parquet_scan('{welw_path.as_posix()}')
        WHERE {where_sql}
        GROUP BY BRANCH, REPTDATE, BNMCODE
    )
    SELECT
        COALESCE(w.BRANCH, m.BRANCH) AS BRANCH,
        COALESCE(w.REPTDATE, m.REPTDATE) AS REPTDATE,
        COALESCE(w.BNMCODE, m.BNMCODE) AS BNMCODE,
        CASE
            WHEN w.BRANCH IS NOT NULL AND m.BRANCH IS NOT NULL THEN
                CASE
                    WHEN SUBSTR(COALESCE(w.BNMCODE, m.BNMCODE), 1, 7) IN ('4911080', '4929980') THEN COALESCE(w.WISAMT, 0) + COALESCE(m.MNIAMT, 0)
                    ELSE w.WISAMT
                END
            WHEN w.BRANCH IS NOT NULL AND m.BRANCH IS NULL THEN w.WISAMT
            WHEN m.BRANCH IS NOT NULL AND w.BRANCH IS NULL AND COALESCE(w.BNMCODE, m.BNMCODE) <> '4218000000000Y' THEN m.MNIAMT
            ELSE w.WISAMT
        END AS AMOUNT
    FROM welw w
    FULL OUTER JOIN melw m
      ON w.BRANCH = m.BRANCH
     AND w.REPTDATE = m.REPTDATE
     AND w.BNMCODE = m.BNMCODE
    """

    return pl.from_pandas(conn.execute(query).fetch_df())


def compute_total_el(elw: pl.DataFrame, eli: pl.DataFrame) -> pl.DataFrame:
    if elw.is_empty():
        return pl.DataFrame(schema={"REPTDATE": pl.Utf8, "BRANCH": pl.Int64, "EL": pl.Float64, "AVGEL": pl.Float64})

    joined = elw.join(eli, on="BNMCODE", how="inner")
    numday = joined.select(pl.col("REPTDATE").n_unique()).item() or 1

    agg = (
        joined.group_by(["BRANCH", "REPTDATE"]).agg(
            [
                pl.when((pl.col("FMTNAME") == "FXEL") & (pl.col("SIGN") == "+")).then(pl.col("AMOUNT")).otherwise(0.0).sum().alias("FXEL_PLUS"),
                pl.when((pl.col("FMTNAME") == "FXEL") & (pl.col("SIGN") == "-")).then(pl.col("AMOUNT")).otherwise(0.0).sum().alias("FXEL_MINUS"),
                pl.when((pl.col("FMTNAME") == "FXEA") & (pl.col("SIGN") == "+")).then(pl.col("AMOUNT")).otherwise(0.0).sum().alias("FXEA_PLUS"),
                pl.when((pl.col("FMTNAME") == "FXEA") & (pl.col("SIGN") == "-")).then(pl.col("AMOUNT")).otherwise(0.0).sum().alias("FXEA_MINUS"),
                pl.when((pl.col("FMTNAME") == "RMEL") & (pl.col("SIGN") == "+")).then(pl.col("AMOUNT")).otherwise(0.0).sum().alias("RMEL_PLUS"),
                pl.when((pl.col("FMTNAME") == "RMEL") & (pl.col("SIGN") == "-")).then(pl.col("AMOUNT")).otherwise(0.0).sum().alias("RMEL_MINUS"),
                pl.when((pl.col("FMTNAME") == "RMEA") & (pl.col("SIGN") == "+")).then(pl.col("AMOUNT")).otherwise(0.0).sum().alias("RMEA_PLUS"),
                pl.when((pl.col("FMTNAME") == "RMEA") & (pl.col("SIGN") == "-")).then(pl.col("AMOUNT")).otherwise(0.0).sum().alias("RMEA_MINUS"),
            ]
        )
        .with_columns(
            [
                (pl.col("FXEL_PLUS") - pl.col("FXEL_MINUS")).alias("FXEL"),
                (pl.col("FXEA_PLUS") - pl.col("FXEA_MINUS")).alias("FXEA"),
                (pl.col("RMEL_PLUS") - pl.col("RMEL_MINUS")).alias("RMEL"),
                (pl.col("RMEA_PLUS") - pl.col("RMEA_MINUS")).alias("RMEA"),
            ]
        )
        .with_columns(
            (pl.col("RMEL") + pl.col("FXEL") - pl.col("RMEA") - pl.min_horizontal("FXEL", "FXEA")).alias("EL")
        )
        .with_columns((pl.col("EL") / float(numday)).alias("AVGEL"))
        .select(["REPTDATE", "BRANCH", "EL", "AVGEL"])
        .sort(["BRANCH", "REPTDATE"])
    )
    return agg


class AsaReportWriter:
    def __init__(self, out_path: Path, page_length: int = PAGE_LENGTH) -> None:
        self.out_path = out_path
        self.page_length = page_length
        self._line_count = 0

    def _write_line(self, fh, text: str, force_new_page: bool = False) -> None:
        if force_new_page or self._line_count == 0 or self._line_count >= self.page_length:
            ctrl = "1"
            self._line_count = 1
        else:
            ctrl = " "
            self._line_count += 1
        fh.write(f"{ctrl}{text}\n")

    def write_report_section(self, fh, brchel: pl.DataFrame, sdesc: str, rdate: str, tbl_label: str) -> None:
        self._write_line(fh, sdesc, force_new_page=True)
        self._write_line(fh, f"BRANCH ELIGIBLE LIABILITIES {tbl_label}")
        self._write_line(fh, f"REPORT DATE : {rdate}")
        self._write_line(fh, "REPORT ID : PBBBRELP")
        self._write_line(fh, "")

        if brchel.is_empty():
            self._write_line(fh, "No data.")
            return

        rept_dates = sorted(brchel.get_column("REPTDATE").unique().to_list())
        piv = brchel.pivot(index="BRANCH", on="REPTDATE", values="EL", aggregate_function="sum").sort("BRANCH")
        avg_by_branch = brchel.group_by("BRANCH").agg(pl.col("AVGEL").sum().alias("AVGEL")).sort("BRANCH")
        piv = piv.join(avg_by_branch, on="BRANCH", how="left")

        title_cols = ["BRH"] + rept_dates + ["TOTAL EL", "AVERAGE EL"]
        header = f"{title_cols[0]:>6}" + "".join(f"{str(c):>18}" for c in title_cols[1:])
        self._write_line(fh, header)
        self._write_line(fh, "-" * len(header))

        numeric_date_cols: list[str] = [d for d in rept_dates if d in piv.columns]
        total_per_date = {d: float(brchel.filter(pl.col("REPTDATE") == d).select(pl.col("EL").sum()).item() or 0.0) for d in rept_dates}

        for row in piv.iter_rows(named=True):
            branch = int(row["BRANCH"])
            values = [f"{branch:>6}"]
            row_total = 0.0
            for d in rept_dates:
                v = float(row.get(d) or 0.0)
                row_total += v
                values.append(f"{v:18,.2f}")
            avg_el = float(row.get("AVGEL") or 0.0)
            values.append(f"{row_total:18,.2f}")
            values.append(f"{avg_el:18,.2f}")
            self._write_line(fh, "".join(values))

        grand_total = float(sum(total_per_date.values()))
        grand_avg = float(brchel.select(pl.col("AVGEL").sum()).item() or 0.0)

        total_values = [f"{'TOTAL':>6}"]
        for d in rept_dates:
            total_values.append(f"{total_per_date[d]:18,.2f}")
        total_values.append(f"{grand_total:18,.2f}")
        total_values.append(f"{grand_avg:18,.2f}")
        self._write_line(fh, "-" * len(header))
        self._write_line(fh, "".join(total_values))


def run_report_kind(conn: duckdb.DuckDBPyConnection, reptmon: str, first_day: int, second_day: int, eli: pl.DataFrame, report_kind: str) -> pl.DataFrame:
    where_sql = branch_where_clause(report_kind)
    elw_frames: list[pl.DataFrame] = []

    for i in range(first_day, second_day + 1):
        day_df = build_elw_for_day(conn=conn, reptmon=reptmon, day_i=i, where_sql=where_sql)
        if not day_df.is_empty():
            elw_frames.append(day_df)

    if not elw_frames:
        return pl.DataFrame(schema={"REPTDATE": pl.Utf8, "BRANCH": pl.Int64, "EL": pl.Float64, "AVGEL": pl.Float64})

    elw = pl.concat(elw_frames, how="vertical")
    return compute_total_el(elw=elw, eli=eli)


def main() -> None:
    args = parse_args()
    first_day, second_day = day_range(args.reptday)
    eli_map = parse_sas_eli_mapping(PBBELF_SOURCE)

    report_labels = {
        "BRCHEL": "(CONVENTIONAL+ISLAMIC)",
        "CONVEL": "(CONVENTIONAL)",
        "ISLEL": "(ISLAMIC)",
    }

    with duckdb.connect(database=":memory:") as conn:
        sections = {
            kind: run_report_kind(
                conn=conn,
                reptmon=args.reptmon,
                first_day=first_day,
                second_day=second_day,
                eli=eli_map,
                report_kind=kind,
            )
            for kind in ("BRCHEL", "CONVEL", "ISLEL")
        }

    writer = AsaReportWriter(args.output)
    with args.output.open("w", encoding="utf-8") as fh:
        for kind in ("BRCHEL", "CONVEL", "ISLEL"):
            writer.write_report_section(
                fh=fh,
                brchel=sections[kind],
                sdesc=args.sdesc,
                rdate=args.rdate,
                tbl_label=report_labels[kind],
            )

    print(f"Generated report: {args.output}")


if __name__ == "__main__":
    main()
