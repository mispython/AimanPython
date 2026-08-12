
# THIS JOB IMPLEMENT BY PAS, KINDLY CONFIRM WITH PROGRAMMER


#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from pbbdpfmt import BRANCHCD


DEFAULT_BASE = Path("/dwh/btrade")
DEFAULT_OUTPUT_DIR = Path("/dwh/ftp/drr")
CONTROL = Path("/sasdata/dwh/control/ctl_dwh_sftp_info.sas7bdat")
BATCH = Path("/sasdata/dwh/logs/EIBMBTMY.txt")
FTP_LOG = Path("/sasdata/dwh/logs/EIBMBTMY_LOG.txt")

REQUIRED_COLUMNS = ["LEVELS", "D", "ACCTNO", "RETAILID", "BRANCH", "APPRLIMT"]


def parse_report_date(value: str | None) -> date:
    if not value:
        first_day_this_month = date.today().replace(day=1)
        return first_day_this_month - timedelta(days=1)

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass

    raise ValueError("Invalid report date. Use YYYY-MM-DD, DD/MM/YYYY, or DD/MM/YY.")


def sas_date(value: date) -> str:
    return value.strftime("%d/%m/%y")


def read_input(path: Path) -> pd.DataFrame:
    df = pd.read_sas(path, format="sas7bdat", encoding="utf-8")
    df.columns = [col.upper().strip() for col in df.columns]

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            "Missing required columns: "
            + ", ".join(missing)
            + "\nAvailable columns: "
            + ", ".join(df.columns)
        )

    return df[REQUIRED_COLUMNS].copy()


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df["ACCTNO"] = df["ACCTNO"].fillna("").astype(str).str.strip()
    df["RETAILID"] = df["RETAILID"].fillna("").astype(str).str.strip()

    df["LEVELS"] = pd.to_numeric(df["LEVELS"], errors="coerce")
    df["D"] = pd.to_numeric(df["D"], errors="coerce")
    df["BRANCH"] = pd.to_numeric(df["BRANCH"], errors="coerce")
    df["APPRLIMT"] = pd.to_numeric(df["APPRLIMT"], errors="coerce")

    return df


def build_report(df: pd.DataFrame) -> pd.DataFrame:
    approve = df[
        (df["LEVELS"] == 1)
        & (df["D"] == 1)
        & (df["ACCTNO"].str[:2] == "25")
    ].copy()

    approve["CATEGORY"] = ""
    approve.loc[approve["RETAILID"] == "C", "CATEGORY"] = "CORPORATE"
    approve.loc[approve["RETAILID"] == "R", "CATEGORY"] = "RETAIL"

    prt = (
        approve.groupby(["BRANCH", "CATEGORY"], dropna=False)
        .agg(
            ACCT=("APPRLIMT", "size"),
            APPRLIMT=("APPRLIMT", "sum"),
        )
        .reset_index()
    )

    prt = prt[prt["BRANCH"] > 0].copy()
    prt["BRANCH"] = prt["BRANCH"].astype(int)
    prt["BRHNAME"] = prt["BRANCH"].map(BRANCHCD).fillna("")
    prt["BRANCHX"] = prt["BRANCH"].astype(str).str.zfill(3) + "-" + prt["BRHNAME"]

    totals = (
        prt.groupby("BRANCHX", dropna=False)
        .agg(
            TOTAL_ACCTS=("ACCT", "sum"),
            TOTAL_LIMITS=("APPRLIMT", "sum"),
        )
        .reset_index()
    )

    categories = prt.pivot_table(
        index="BRANCHX",
        columns="CATEGORY",
        values=["ACCT", "APPRLIMT"],
        aggfunc="sum",
        fill_value=0,
        dropna=False,
    )

    categories.columns = [
        f"{category}_{measure}" for measure, category in categories.columns
    ]

    report = totals.merge(categories.reset_index(), on="BRANCHX", how="left")
    return report.sort_values("BRANCHX")


def num(row: pd.Series, col: str) -> float:
    if col not in row.index or pd.isna(row[col]):
        return 0.0
    return float(row[col])


def fmt_count(value: float, width: int = 8) -> str:
    return f"{'.':>{width}}" if value == 0 else f"{int(value):>{width},}"


def fmt_amt(value: float, width: int = 20) -> str:
    return f"{'.':>{width}}" if value == 0 else f"{value:>{width},.2f}"


def write_saslist(report: pd.DataFrame, output: Path, report_date: date) -> None:
    title1 = "PUBLIC BANK BERHAD. REPORT-ID: EIBMBTMY"
    title2 = f"MONTHLY TRADE BILLS BY APPROVE LIMIT & NO.OF ACCTS {sas_date(report_date)}"

    rows = []
    totals = {
        "total_acct": 0.0,
        "total_limit": 0.0,
        "blank_acct": 0.0,
        "blank_limit": 0.0,
        "corp_acct": 0.0,
        "corp_limit": 0.0,
        "retail_acct": 0.0,
        "retail_limit": 0.0,
    }

    for _, row in report.iterrows():
        item = {
            "branch": str(row["BRANCHX"])[:28],
            "total_acct": num(row, "TOTAL_ACCTS"),
            "total_limit": num(row, "TOTAL_LIMITS"),
            "blank_acct": num(row, "_ACCT"),
            "blank_limit": num(row, "_APPRLIMT"),
            "corp_acct": num(row, "CORPORATE_ACCT"),
            "corp_limit": num(row, "CORPORATE_APPRLIMT"),
            "retail_acct": num(row, "RETAIL_ACCT"),
            "retail_limit": num(row, "RETAIL_APPRLIMT"),
        }

        rows.append(item)

        for key in totals:
            totals[key] += item[key]

    with output.open("w", encoding="utf-8") as f:
        f.write(f"{title1}\n{title2}\n\n")
        f.write("-" * 152 + "\n")
        f.write(
            f"|{'BRANCH':<28}|{'TOTAL':^31}|{'':^29}|"
            f"{'CORPORATE':^29}|{'RETAIL':^29}|\n"
        )
        f.write(
            f"|{'':<28}|{'ACCTS':^8}|{'TOTAL LIMITS(RM)':^22}|"
            f"{'ACCTS':^8}|{'LIMITS(RM)':^20}|"
            f"{'ACCTS':^8}|{'LIMITS(RM)':^20}|"
            f"{'ACCTS':^8}|{'LIMITS(RM)':^20}|\n"
        )
        f.write("-" * 152 + "\n")

        for row in rows:
            f.write(
                f"|{row['branch']:<28}"
                f"|{fmt_count(row['total_acct'])}"
                f"|{fmt_amt(row['total_limit'], 22)}"
                f"|{fmt_count(row['blank_acct'])}"
                f"|{fmt_amt(row['blank_limit'])}"
                f"|{fmt_count(row['corp_acct'])}"
                f"|{fmt_amt(row['corp_limit'])}"
                f"|{fmt_count(row['retail_acct'])}"
                f"|{fmt_amt(row['retail_limit'])}|\n"
            )

        f.write(
            f"|{'TOTAL':<28}"
            f"|{fmt_count(totals['total_acct'])}"
            f"|{fmt_amt(totals['total_limit'], 22)}"
            f"|{fmt_count(totals['blank_acct'])}"
            f"|{fmt_amt(totals['blank_limit'])}"
            f"|{fmt_count(totals['corp_acct'])}"
            f"|{fmt_amt(totals['corp_limit'])}"
            f"|{fmt_count(totals['retail_acct'])}"
            f"|{fmt_amt(totals['retail_limit'])}|\n"
        )
        f.write("-" * 152 + "\n")



def upload_to_drr(local_file: Path) -> None:
    BATCH.parent.mkdir(parents=True, exist_ok=True)
    BATCH.write_bytes(
        f'cd "/FD-MA/LOAN"\r\nput "{local_file.resolve()}" "BTLIMIT.TXT"\r\n'.encode()
    )

    ctl = pd.read_sas(CONTROL, encoding="utf-8")
    ctl.columns = ctl.columns.str.upper()
    rows = ctl[ctl["HOST_DESC"].fillna("").str.contains(
        "Data Report Repository (DRR)", regex=False
    )]
    if len(rows) != 1:
        raise RuntimeError(f"Expected one DRR row; found {len(rows)}")

    row = rows.iloc[0]
    clean = lambda value: "".join(str(value).split())
    command = ["psftp", "-pw", clean(row["SFTP_PW"]),
               f'{clean(row["SFTP_ID"])}@{clean(row["HOST_IP"])}',
               "-b", str(BATCH), "-be"]
    with FTP_LOG.open("w", encoding="utf-8") as log:
        subprocess.run(command, input="y\n", text=True, stdout=log,
                       stderr=subprocess.STDOUT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="EIBMBTMY SAS7BDAT conversion")
    parser.add_argument("--input", type=Path, default=None)
    parser.add_argument("--report-date", type=str, default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    report_date = parse_report_date(args.report_date)
    report_week = {8: 1, 15: 2, 22: 3}.get(report_date.day, 4)
    input_path = (
        args.input
        or DEFAULT_BASE / f"btmast{report_date:%m}{report_week}{report_date:%y}.sas7bdat"
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)

    df = clean_data(read_input(input_path))
    report = build_report(df)

    report_csv = args.output_dir / "PBB_BT_LIMIT_REPORT_TABULATE.csv"
    saslist = args.output_dir / "BTLIMIT.TXT"

    report.to_csv(report_csv, index=False)
    write_saslist(report, saslist, report_date)
    upload_to_drr(saslist)

    print(f"Input       : {input_path}")
    print(f"Rows input  : {len(df):,}")
    print(f"Report CSV  : {report_csv}")
    print(f"SASLIST     : {saslist}")


if __name__ == "__main__":
    main()









