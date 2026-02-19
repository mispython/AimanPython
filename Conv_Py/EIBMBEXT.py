# !/usr/bin/env python3
"""
Program: EIBMBEXT
Purpose: Convert SAS report "Balances of External Accounts by Branch" into Python.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

# Calling PBBDPFMT.py for product format definitions
from PBBDPFMT import CAProductFormat, FDProductFormat, ProductLists, SAProductFormat


# /***************************************************************/
# /*   OWNER : FINANCE DIVISION, PBB                             */
# /*   REPORT ON BALANCES OF EXTERNAL ACCOUNTS BY BRANCH ON      */
# /*   MONTHLY BASIS. SOURCES : SAVINGS, CURRENT, OD, FD & LOANS */
# /***************************************************************/

# OPTIONS SORTDEV=3390 YEARCUTOFF=1950 LS=132 PS=60 NOCENTER;
LINE_SIZE = 132
PAGE_SIZE = 60

# PATHS (defined early as requested)
BASE_PATH = Path("/data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"
OUTPUT_FILE = OUTPUT_PATH / "EIBMBEXT.txt"

DEPOSIT_SAVING_FILE = INPUT_PATH / "deposit_saving.parquet"
DEPOSIT_CURRENT_FILE = INPUT_PATH / "deposit_current.parquet"
FD_FILE = INPUT_PATH / "fd_fd.parquet"
# BNM1.LOAN&REPTMON&NOWK
LOAN_FILE_PATTERN = INPUT_PATH / "bnm1_loan{reptmon}{nowk}.parquet"

OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# %LET CUSTCDE=('80','81','82','83','84','85','86','87','88','89',
#               '90','91','92','93','94','95','96','98','99');
CUSTCDE = {
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "93", "94", "95", "96", "98", "99",
}

# %INC PGM(PBBDPFMT);
# Use the migrated PBBDPFMT format definitions directly.
ACE_EXCLUDE: set[int] = set(ProductLists.ACE_PRODUCTS)


def sas_report_date(today_value: date | None = None) -> tuple[date, str, str, str, str, str]:
    """Replicate DATA REPTDATE and CALL SYMPUT logic from SAS."""
    today_value = today_value or date.today()
    first_of_month = date(today_value.year, today_value.month, 1)
    reptdate = first_of_month - timedelta(days=1)

    if reptdate.day == 8:
        wk = "1"
    elif reptdate.day == 15:
        wk = "2"
    elif reptdate.day == 22:
        wk = "3"
    else:
        wk = "4"

    reptmon = f"{reptdate.month:02d}"
    reptyear = f"{reptdate.year:04d}"
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d/%m/%y")
    return reptdate, wk, reptmon, reptyear, reptday, rdate


def to_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def comma15_2(val: float | int | None) -> str:
    num = 0.0 if val is None else float(val)
    return f"{num:15,.2f}"


def asa_write_report(path: Path, lines: Iterable[tuple[str, str]]) -> None:
    """Write a report using ASA carriage control characters."""
    with path.open("w", encoding="utf-8", newline="\n") as f:
        line_no = 0
        for cc, text in lines:
            if line_no and line_no % PAGE_SIZE == 0 and cc == " ":
                cc = "1"
            f.write(f"{cc}{text}\n")
            line_no += 1


def tabulate_branch_prodcd(df: pl.DataFrame) -> list[tuple[str, str]]:
    prod_order = ["42110", "42120", "42130", "42132", "43110"]

    pt = (
        df.group_by(["BRANCH", "PRODCD"])
        .agg(pl.col("CURBAL").sum().alias("CURBAL"))
    )

    lines: list[tuple[str, str]] = []
    lines.append(("1", "REPORT ID : EIBMBEXT"))
    lines.append((" ", "PUBLIC BANK BERHAD, FINANCE DIVISION (BOP)"))
    lines.append((" ", f"BALANCES OF EXTERNAL ACCOUNT BY BRANCH AS AT {RDATE}"))
    lines.append((" ", "REPORT ON SUMMARY TOTAL BY BRANCH"))
    lines.append((" ", ""))

    header = f"{'BRANCH':<8}{'42110-85':>15}{'42120-85':>15}{'42130-81+85':>15}{'42132-81+85':>15}{'43110-02':>15}{'TOTAL':>15}"
    lines.append((" ", header))
    lines.append((" ", "-" * len(header)))

    branches = sorted(pt.select("BRANCH").unique().to_series().to_list())
    grand = {p: 0.0 for p in prod_order}

    for branch in branches:
        row_vals: dict[str, float] = {p: 0.0 for p in prod_order}
        tmp = pt.filter(pl.col("BRANCH") == branch)
        for r in tmp.iter_rows(named=True):
            p = str(r["PRODCD"])
            if p in row_vals:
                row_vals[p] = float(r["CURBAL"] or 0.0)
                grand[p] += row_vals[p]

        total = sum(row_vals.values())
        line = (
            f"{str(branch):<8}"
            f"{comma15_2(row_vals['42110'])}"
            f"{comma15_2(row_vals['42120'])}"
            f"{comma15_2(row_vals['42130'])}"
            f"{comma15_2(row_vals['42132'])}"
            f"{comma15_2(row_vals['43110'])}"
            f"{comma15_2(total)}"
        )
        lines.append((" ", line))

    grand_total = sum(grand.values())
    lines.append((" ", "-" * len(header)))
    lines.append((" ", f"{'TOTAL':<8}{comma15_2(grand['42110'])}{comma15_2(grand['42120'])}{comma15_2(grand['42130'])}{comma15_2(grand['42132'])}{comma15_2(grand['43110'])}{comma15_2(grand_total)}"))
    return lines


def tabulate_summary_bop1(bop1: pl.DataFrame) -> list[tuple[str, str]]:
    s = bop1.select(
        pl.col("BALANCE").sum().alias("BALANCE"),
        pl.col("FLOATSAV").sum().alias("FLOATSAV"),
        pl.col("FLTCA").sum().alias("FLTCA"),
        pl.col("LEDGBAL").sum().alias("LEDGBAL"),
    )
    totals = s.row(0, named=True)

    lines: list[tuple[str, str]] = []
    lines.append(("1", "REPORT ID : EIBMBEXT"))
    lines.append((" ", "PUBLIC BANK BERHAD, FINANCE DIVISION (BOP)"))
    lines.append((" ", f"BALANCES OF EXTERNAL ACCOUNT BY BRANCH AS AT {RDATE}"))
    lines.append((" ", "REPORT ON SUMMARY OF OD, FLOAT CHEQUES & LEDGER BALANCE"))
    lines.append((" ", ""))

    hdr = f"{'BRANCH':<8}{'OD BALANCE':>15}{'FLOAT CHEQUE (SA)':>20}{'FLOAT CHEQUE (CA)':>20}{'LEDGER BALANCE':>20}"
    lines.append((" ", hdr))
    lines.append((" ", "-" * len(hdr)))

    for r in bop1.sort("BRANCH").iter_rows(named=True):
        lines.append((" ",
            f"{str(r['BRANCH']):<8}"
            f"{comma15_2(r.get('BALANCE'))}"
            f"{comma15_2(r.get('FLOATSAV'))}"
            f"{comma15_2(r.get('FLTCA'))}"
            f"{comma15_2(r.get('LEDGBAL'))}"
        ))

    lines.append((" ", "-" * len(hdr)))
    lines.append((" ",
        f"{'TOTAL':<8}"
        f"{comma15_2(totals['BALANCE'])}"
        f"{comma15_2(totals['FLOATSAV'])}"
        f"{comma15_2(totals['FLTCA'])}"
        f"{comma15_2(totals['LEDGBAL'])}"
    ))
    return lines


if __name__ == "__main__":
    reptdate, NOWK, REPTMON, REPTYEAR, REPTDAY, RDATE = sas_report_date()

    con = duckdb.connect()

    # /***************************************************************/
    # /*   SAVINGS DEPOSIT                                           */
    # /***************************************************************/
    saving = con.execute(
        f"""
        SELECT BRANCH, ACCTNO, NAME, PRODUCT, CUSTCODE, CURBAL, LEDGBAL, (LEDGBAL - CURBAL) AS FLOATSAV,
               OPENIND
        FROM read_parquet('{DEPOSIT_SAVING_FILE.as_posix()}')
        WHERE OPENIND NOT IN ('B','C','P')
          AND CURBAL >= 0
          AND CUSTCODE IN ({','.join([repr(x) for x in sorted(CUSTCDE)])})
        """
    ).pl()

    saving = saving.filter(~pl.col("PRODUCT").cast(pl.Int64).is_in(sorted(ACE_EXCLUDE)))
    saving = saving.with_columns(
        pl.col("PRODUCT")
        .map_elements(lambda x: SAProductFormat.format(to_int(x)), return_dtype=pl.Utf8)
        .alias("PRODCD")
    ).with_columns(
        pl.when(pl.col("PRODCD").is_in(["42120", "42320"]))
        .then(pl.lit("42120"))
        .otherwise(pl.col("PRODCD"))
        .alias("PRODCD")
    ).select(["BRANCH", "ACCTNO", "NAME", "PRODCD", "CUSTCODE", "CURBAL", "LEDGBAL", "FLOATSAV"])

    # /***************************************************************/
    # /*   CURRENT & VOSTRO                                          */
    # /***************************************************************/
    current_src = con.execute(
        f"""
        SELECT BRANCH, ACCTNO, NAME, PRODUCT, CUSTCODE, CURBAL, LEDGBAL, OPENIND
        FROM read_parquet('{DEPOSIT_CURRENT_FILE.as_posix()}')
        WHERE OPENIND NOT IN ('B','C','P')
          AND CUSTCODE IN ({','.join([repr(x) for x in sorted(CUSTCDE)])})
        """
    ).pl().with_columns(
        pl.col("PRODUCT")
        .map_elements(lambda x: CAProductFormat.format(to_int(x)), return_dtype=pl.Utf8)
        .alias("PRODCD")
    )

    current = (
        current_src
        .filter(pl.col("CURBAL") >= 0)
        .with_columns((pl.col("LEDGBAL") - pl.col("CURBAL")).alias("FLOATCUR"))
        .filter(pl.col("PRODCD").is_in(["42110", "42310"]))
        .with_columns(pl.lit("42110").alias("PRODCD"))
        .select(["BRANCH", "ACCTNO", "NAME", "PRODCD", "CUSTCODE", "CURBAL", "FLOATCUR", "LEDGBAL"])
    )

    vostro = (
        current_src
        .filter((pl.col("CURBAL") >= 0) & (pl.col("PRODCD") == "43110"))
        .with_columns((pl.col("LEDGBAL") - pl.col("CURBAL")).alias("FLOATCUR"))
        .select(["BRANCH", "ACCTNO", "NAME", "PRODCD", "CUSTCODE", "CURBAL", "FLOATCUR", "LEDGBAL"])
    )

    odaccts = (
        current_src
        .filter(pl.col("CURBAL") < 0)
        .with_columns((pl.col("LEDGBAL") - pl.col("CURBAL")).alias("FLOATOD"))
        .select(["BRANCH", "ACCTNO", "NAME", "PRODCD", "CUSTCODE", "CURBAL", "FLOATOD", "LEDGBAL"])
    )

    # /***************************************************************/
    # /*   FIXED DEPOSITS                                            */
    # /***************************************************************/
    fd = con.execute(
        f"""
        SELECT BRANCH, ACCTNO, NAME, INTPLAN, CUSTCD, CURBAL, OPENIND
        FROM read_parquet('{FD_FILE.as_posix()}')
        WHERE OPENIND IN ('D','O')
          AND CUSTCD IN ({','.join([repr(x) for x in sorted(CUSTCDE)])})
        """
    ).pl().with_columns([
        pl.lit(0.0).alias("LEDGBAL"),
        pl.col("INTPLAN")
        .map_elements(lambda x: FDProductFormat.format(to_int(x)), return_dtype=pl.Utf8)
        .alias("BIC"),
    ]).rename({"CUSTCD": "CUSTCODE", "BIC": "PRODCD"}).select(
        ["BRANCH", "ACCTNO", "NAME", "PRODCD", "CUSTCODE", "CURBAL", "LEDGBAL"]
    )

    # DATA BOP&REPTMON;
    bop_mon = pl.concat([
        current.select(["BRANCH", "ACCTNO", "NAME", "PRODCD", "CUSTCODE", "CURBAL", "LEDGBAL"]),
        vostro.select(["BRANCH", "ACCTNO", "NAME", "PRODCD", "CUSTCODE", "CURBAL", "LEDGBAL"]),
        saving.select(["BRANCH", "ACCTNO", "NAME", "PRODCD", "CUSTCODE", "CURBAL", "LEDGBAL"]),
        fd.select(["BRANCH", "ACCTNO", "NAME", "PRODCD", "CUSTCODE", "CURBAL", "LEDGBAL"]),
    ], how="vertical_relaxed")

    report1 = tabulate_branch_prodcd(bop_mon)

    # /***************************************************************/
    # /*   OVERDRAFT FROM LOANS DATASET                              */
    # /***************************************************************/
    loan_file = Path(str(LOAN_FILE_PATTERN).format(reptmon=REPTMON, nowk=NOWK))
    od = con.execute(
        f"""
        SELECT *
        FROM read_parquet('{loan_file.as_posix()}')
        WHERE ACCTYPE = 'OD'
          AND CUSTCD IN ({','.join([repr(x) for x in sorted(CUSTCDE)])})
        """
    ).pl()

    # DATA ODFLT; MERGE OD(IN=A) ODACCTS(IN=B); BY ACCTNO;
    odflt = od.join(odaccts, on="ACCTNO", how="inner", suffix="_ODACCTS")

    # PROC SUMMARY DATA=ODFLT NWAY; CLASS BRANCH; VAR BALANCE FLOATOD; OUTPUT OUT=OD SUM=;
    od_sum = odflt.group_by("BRANCH").agg([
        pl.col("BALANCE").sum().alias("BALANCE"),
        pl.col("FLOATOD").sum().alias("FLOATOD"),
    ])

    # DATA BOP&REPTMON; SET CURRENT VOSTRO SAVING;
    bop_mon2 = pl.concat([
        current.select(["BRANCH", "FLOATCUR", "LEDGBAL", "CURBAL"]).with_columns(pl.lit(0.0).alias("FLOATSAV")),
        vostro.select(["BRANCH", "FLOATCUR", "LEDGBAL", "CURBAL"]).with_columns(pl.lit(0.0).alias("FLOATSAV")),
        saving.select(["BRANCH", "LEDGBAL", "CURBAL", "FLOATSAV"]).with_columns(pl.lit(0.0).alias("FLOATCUR")),
    ], how="vertical_relaxed")

    bop_mon2 = bop_mon2.group_by("BRANCH").agg([
        pl.col("FLOATSAV").sum().alias("FLOATSAV"),
        pl.col("FLOATCUR").sum().alias("FLOATCUR"),
        pl.col("LEDGBAL").sum().alias("LEDGBAL"),
        pl.col("CURBAL").sum().alias("CURBAL"),
    ])

    # DATA BOP; MERGE BOP&REPTMON(IN=A) OD(IN=B); BY BRANCH;
    bop = bop_mon2.join(od_sum, on="BRANCH", how="full", coalesce=True).with_columns([
        pl.col("BALANCE").fill_null(0.0),
        pl.col("FLOATOD").fill_null(0.0),
        pl.col("CURBAL").fill_null(0.0),
        pl.col("LEDGBAL").fill_null(0.0),
        pl.col("FLOATSAV").fill_null(0.0),
        pl.col("FLOATCUR").fill_null(0.0),
    ])

    # DATA BOP1; FLTCA = FLOATCUR + FLOATOD;
    bop1 = bop.with_columns((pl.col("FLOATCUR") + pl.col("FLOATOD")).alias("FLTCA"))

    report2 = tabulate_summary_bop1(bop1)

    final_lines = report1 + [(" ", "")] + report2
    asa_write_report(OUTPUT_FILE, final_lines)
    print(f"Generated report: {OUTPUT_FILE}")
