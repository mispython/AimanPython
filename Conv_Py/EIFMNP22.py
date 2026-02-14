#!/usr/bin/env python3
"""
Program: EIFMNP22
Purpose: Reports of outstanding balance for PC/FEE receivable.
        1. By risk and branch
        2. All HP loans
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import argparse

import duckdb
import polars as pl


@dataclass(frozen=True)
class Paths:
    npl_reptdate: Path
    loan_lnnote: Path
    feeplan: Path
    npl_loan_dir: Path
    brchcd_format: Path
    output_report: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert SAS EIFMNP22 to Python")
    parser.add_argument("--npl-reptdate", default="NPL.REPTDATE.parquet")
    parser.add_argument("--loan-lnnote", default="LOAN.LNNOTE.parquet")
    parser.add_argument("--feeplan", default="FEEFILE.parquet")
    parser.add_argument("--npl-loan-dir", default=".")
    parser.add_argument("--brchcd-format", default="data/formats/BRCHCD.parquet")
    parser.add_argument("--output-report", default="OUTFL.txt")
    return parser.parse_args()


def worddatx18(dt_value: date | datetime) -> str:
    dt = dt_value.date() if isinstance(dt_value, datetime) else dt_value
    return f"{dt.strftime('%B')} {dt.day}, {dt.year}"


def load_brchcd_map(con: duckdb.DuckDBPyConnection, path: Path) -> dict[int, str]:
    if not path.exists():
        return {}
    df = con.execute(
        "SELECT * FROM read_parquet(?)",
        [str(path)],
    ).pl()
    key_col = "BRANCH" if "BRANCH" in df.columns else df.columns[0]
    val_col = "BRABBR" if "BRABBR" in df.columns else df.columns[1]
    mapping: dict[int, str] = {}
    for row in df.select([key_col, val_col]).iter_rows(named=True):
        try:
            mapping[int(row[key_col])] = "" if row[val_col] is None else str(row[val_col]).strip()
        except (TypeError, ValueError):
            continue
    return mapping


def loantyp_fmt(loantype: int | None) -> str:
    if loantype in (110, 115, 983):
        return "HPD AITAB"
    if loantype in (700, 705, 993):
        return "HPD CONVENTIONAL"
    if loantype is not None and 200 <= loantype <= 299:
        return "HOUSING LOANS"
    if loantype is not None and (300 <= loantype <= 499 or 504 <= loantype <= 550 or 900 <= loantype <= 980):
        return "FIXED LOANS"
    return "OTHERS"


def risk_fmt(days: int | None) -> str:
    d = int(days or 0)
    if d > 364:
        return "BAD"
    if d > 273:
        return "DOUBTFUL"
    if d > 182:
        return "SUBSTANDARD 2"
    return "SUBSTANDARD-1"


def build_branch(ntbrch: int | None, brchcd_map: dict[int, str]) -> str:
    if ntbrch is None:
        return " 000"
    branch_no = int(ntbrch)
    return f"{brchcd_map.get(branch_no, '')} {branch_no:03d}".rstrip()


def paginate_and_write(lines: list[str], out_path: Path, page_len: int = 60) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        line_count = 0
        for line in lines:
            cc = "1" if line_count == 0 or line_count >= page_len else " "
            if cc == "1" and line_count >= page_len:
                line_count = 0
            f.write(f"{cc}{line}\n")
            line_count += 1


def build_summary_table(loan_df: pl.DataFrame) -> list[str]:
    risk_order = {"SUBSTANDARD-1": 1, "SUBSTANDARD 2": 2, "DOUBTFUL": 3, "BAD": 4}

    detail = (
        loan_df.group_by(["LOANTYP", "RISK", "BRANCH"])
        .agg(
            pl.sum("FEEAMT3").alias("FEEAMT3"),
            pl.sum("FEEAMT4").alias("FEEAMT4"),
        )
        .with_columns(pl.col("RISK").replace_strict(risk_order, default=9).alias("_risk_ord"))
        .sort(["LOANTYP", "_risk_ord", "BRANCH"])
    )

    risk_totals = (
        detail.group_by(["LOANTYP", "RISK", "_risk_ord"])
        .agg(pl.sum("FEEAMT3").alias("FEEAMT3"), pl.sum("FEEAMT4").alias("FEEAMT4"))
        .sort(["LOANTYP", "_risk_ord"])
    )

    loantyp_totals = (
        detail.group_by(["LOANTYP"])
        .agg(pl.sum("FEEAMT3").alias("FEEAMT3"), pl.sum("FEEAMT4").alias("FEEAMT4"))
        .sort("LOANTYP")
    )

    grand_fee3 = float(loan_df["FEEAMT3"].sum() or 0)
    grand_fee4 = float(loan_df["FEEAMT4"].sum() or 0)

    lines = [
        "PUBLIC BANK BERHAD",
        "OUTSTANDING BALANCE FOR PC/FEE RECEIVABLE",
        "(NPL FROM 6 MONTHS & ABOVE)",
        "",
        f"{'RISK        BRANCH':<29}{'PC/FEE REC':>20}{'CURR FEES':>20}",
        "-" * 69,
    ]

    for loantyp in loantyp_totals["LOANTYP"].to_list():
        lines.append(f"{loantyp}")
        lt_detail = detail.filter(pl.col("LOANTYP") == loantyp)
        lt_risk_total = risk_totals.filter(pl.col("LOANTYP") == loantyp)
        for risk in lt_risk_total["RISK"].to_list():
            risk_rows = lt_detail.filter(pl.col("RISK") == risk)
            for row in risk_rows.iter_rows(named=True):
                lines.append(f"{risk:<14} {row['BRANCH']:<14}{float(row['FEEAMT3'] or 0):>20,.2f}{float(row['FEEAMT4'] or 0):>20,.2f}")
            tot = lt_risk_total.filter(pl.col("RISK") == risk).row(0, named=True)
            lines.append(f"{'':<14} {'SUB-TOTAL':<14}{float(tot['FEEAMT3'] or 0):>20,.2f}{float(tot['FEEAMT4'] or 0):>20,.2f}")
        lt_tot = loantyp_totals.filter(pl.col("LOANTYP") == loantyp).row(0, named=True)
        lines.append(f"{'':<14} {'TOTAL FEE':<14}{float(lt_tot['FEEAMT3'] or 0):>20,.2f}{float(lt_tot['FEEAMT4'] or 0):>20,.2f}")
        lines.append("")

    lines.append("=" * 69)
    lines.append(f"{'GRAND TOTAL':<28}{grand_fee3:>20,.2f}{grand_fee4:>20,.2f}")
    lines.append("")
    return lines


def build_all_hp_loans_table(lnnote_df: pl.DataFrame) -> list[str]:
    filtered = lnnote_df.filter(
        (pl.col("REVERSED") != "Y")
        & pl.col("NOTENO").is_not_null()
        & pl.col("NTBRCH").is_not_null()
        & pl.col("LOANTYPE").is_in([110, 115, 700, 705, 709, 710, 750, 752, 760, 770, 775, 799])
    )

    fee3 = float(filtered["FEEAMT3"].sum() or 0)
    fee4 = float(filtered["FEEAMT4"].sum() or 0)

    return [
        "(ALL HP LOANS)",
        f"{'PC/FEE RECEIVABLE':<30}{fee3:>20,.2f}",
        f"{'AMT ACCESS CURR':<30}{fee4:>20,.2f}",
    ]


def main() -> None:
    args = parse_args()
    paths = Paths(
        npl_reptdate=Path(args.npl_reptdate),
        loan_lnnote=Path(args.loan_lnnote),
        feeplan=Path(args.feeplan),
        npl_loan_dir=Path(args.npl_loan_dir),
        brchcd_format=Path(args.brchcd_format),
        output_report=Path(args.output_report),
    )

    con = duckdb.connect()
    brchcd_map = load_brchcd_map(con, paths.brchcd_format)

    reptdate_df = con.execute("SELECT REPTDATE FROM read_parquet(?) LIMIT 1", [str(paths.npl_reptdate)]).pl()
    if reptdate_df.is_empty():
        raise ValueError("NPL.REPTDATE has no rows")

    reptdate_val = reptdate_df["REPTDATE"][0]
    if isinstance(reptdate_val, datetime):
        reptdate = reptdate_val.date()
    else:
        reptdate = reptdate_val

    reptmon = f"{reptdate.month:02d}"
    rdate = worddatx18(reptdate)

    lnnote = con.execute(
        """
        SELECT *
        FROM read_parquet(?)
        WHERE REVERSED <> 'Y'
          AND NOTENO IS NOT NULL
          AND NTBRCH IS NOT NULL
          AND LOANTYPE IN (110,115,700,705)
        """,
        [str(paths.loan_lnnote)],
    ).pl()

    feeplan = con.execute(
        """
        SELECT ACCTNO, NOTENO, LOANTYPE, FEEPLN, FEEAMT4, FEEAMT
        FROM read_parquet(?)
        WHERE LOANTYPE IN (110,115,700,705)
          AND FEEPLN IN ('DC')
        """,
        [str(paths.feeplan)],
    ).pl()

    feepo = feeplan.group_by(["ACCTNO", "NOTENO"]).agg(
        pl.sum("FEEAMT").alias("FEEAMT3A"),
        pl.sum("FEEAMT4").alias("FEEAMT4A"),
    )

    lnnote = lnnote.join(feepo, on=["ACCTNO", "NOTENO"], how="left").with_columns(
        (pl.coalesce([pl.col("FEEAMT3"), pl.lit(0.0)]) + pl.coalesce([pl.col("FEEAMT3A"), pl.lit(0.0)])).alias("FEEAMT3"),
        (pl.coalesce([pl.col("FEEAMT4"), pl.lit(0.0)]) + pl.coalesce([pl.col("FEEAMT4A"), pl.lit(0.0)])).alias("FEEAMT4"),
    )

    npl_loan_file = paths.npl_loan_dir / f"NPL.LOAN{reptmon}.parquet"
    loan = con.execute(
        """
        SELECT LOANTYPE, DAYS, NTBRCH,
               COALESCE(FEEAMT3, 0) AS FEEAMT3,
               COALESCE(FEEAMT4, 0) AS FEEAMT4
        FROM read_parquet(?)
        WHERE LOANTYPE IN (110,115,700,705)
        """,
        [str(npl_loan_file)],
    ).pl()

    loan = loan.with_columns(
        pl.col("LOANTYPE").map_elements(loantyp_fmt, return_dtype=pl.Utf8).alias("LOANTYP"),
        pl.col("DAYS").map_elements(risk_fmt, return_dtype=pl.Utf8).alias("RISK"),
        pl.col("NTBRCH").map_elements(lambda x: build_branch(x, brchcd_map), return_dtype=pl.Utf8).alias("BRANCH"),
    ).select(["LOANTYP", "RISK", "BRANCH", "FEEAMT3", "FEEAMT4"])

    lines: list[str] = [
        f"OUTSTANDING BALANCE FOR PC/FEE RECEIVABLE AS AT {rdate}",
        "",
    ]
    lines.extend(build_summary_table(loan))
    lines.append("")
    lines.extend(build_all_hp_loans_table(lnnote))

    paginate_and_write(lines, paths.output_report, page_len=60)


if __name__ == "__main__":
    main()
