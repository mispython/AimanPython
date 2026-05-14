# =============================================================================
# Program Name : EIQBNMR1.py
# Purpose      : PBB - Breakdown of Loan by Operating Division
#                Generates BNM report summarising corporate and SME loan
#                balances across LN, OD, and BT account types.
# =============================================================================

import duckdb
import polars as pl
from pathlib import Path

from REPTDATE import get_reptdate_values

# =============================================================================
# PATHS
# =============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
LN_PATH    = BASE_DIR / "input/uat/ln05126"
BT_PATH    = BASE_DIR / "input/uat/btrad04426"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# REPORT DATE DERIVATION  (equivalent of DATA REPTDATE step)
# =============================================================================
reptdate_values = get_reptdate_values(year_format="%Y")

reptdate = reptdate_values.reptdate
REPTYEAR = reptdate_values.reptyear   # 4-digit year for this program's file names
REPTMON  = reptdate_values.reptmon    # zero-padded month (Z2.)
REPTDAY  = reptdate_values.reptday    # zero-padded day   (Z2.)
REPTDT   = reptdate_values.reptdt     # raw SAS date integer equivalent (used for filter)
RDATE    = reptdate_values.rdate      # date object used in DATA ECP step
NOWK     = reptdate_values.nowk       # zero-padded 1-digit week number (Z1.)

rdate_str = reptdate.strftime("%d%m%y")

# =============================================================================
# MACRO VARIABLE LISTS
# =============================================================================
UNWANTED_LN = [
    110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
    128, 130, 131, 132, 135, 136, 138, 139, 140, 141, 142, 199,
    315, 320, 325, 330, 340, 355, 380, 381, 500, 520,
    700, 705, 720, 725,
]

UNWANTED_OD = [
    107, 126, 127, 128, 129, 130, 131, 132, 133, 134,
    135, 136, 140, 141, 142, 143, 144, 145, 146, 147, 148,
    149, 150, 171, 172, 173, 549, 550,
]

CORP_CUSTCD = [
    '4',  '5',  '6',  '13', '17', '20', '30', '31', '32',
    '33', '34', '35', '37', '38', '39', '40', '45',
    '57', '59', '61', '62', '63', '64', '71', '72',
    '73', '74', '75', '82', '83', '84', '86', '90',
    '91', '92', '98',
]

SME_CUSTCD = [
    '41', '42', '43', '44', '46', '47', '48', '49',
    '51', '52', '53', '54', '66', '67', '68', '69',
]

# =============================================================================
# HELPER: read a parquet file via DuckDB → Polars DataFrame
# =============================================================================
def _read_parquet(path: Path) -> pl.DataFrame:
    """Return a Polars DataFrame from *path*; empty DataFrame on missing file."""
    if not path.exists():
        return pl.DataFrame()
    return duckdb.query(f"SELECT * FROM read_parquet('{path}')").pl()


# =============================================================================
# STEP 1 – Read source files
#   ln05126    : LOAN<mm><wk>, LNWOD<mm><wk>, LNWOF<mm><wk>
#   btrad04426 : BTRAD<mm><wk>
# =============================================================================
def _load_loan_data() -> pl.DataFrame:
    frames = [
        _read_parquet(LN_PATH / f"LOAN{REPTMON}{NOWK}.parquet"),
        _read_parquet(LN_PATH / f"LNWOD{REPTMON}{NOWK}.parquet"),
        _read_parquet(LN_PATH / f"LNWOF{REPTMON}{NOWK}.parquet"),
    ]
    non_empty = [df for df in frames if not df.is_empty()]
    if not non_empty:
        return pl.DataFrame()
    return pl.concat(non_empty)


def _load_bt_data() -> pl.DataFrame:
    bt_df = _read_parquet(BT_PATH / f"BTRAD{REPTMON}{NOWK}.parquet")
    if bt_df.is_empty():
        return pl.DataFrame()

    # Rename ACCTNO → ACCTNO1, then add ACCTYPE = "BT"
    bt_df = (
        bt_df
        .rename({"ACCTNO": "ACCTNO1"})
        .with_columns(
            pl.col("ACCTNO1").alias("ACCTNO"),
            pl.lit("BT").alias("ACCTYPE"),
        )
    )
    return bt_df


# =============================================================================
# STEP 2 – Filter and segment LN / OD / BT accounts
# =============================================================================
def _filter_ln(all_loan: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    ln = all_loan.filter(
        (pl.col("ACCTYPE") == "LN")
        & (~pl.col("PRODUCT").is_in(UNWANTED_LN))
        & ~(
            ((pl.col("PRODUCT") >= 200) & (pl.col("PRODUCT") <= 299))
            | ((pl.col("PRODUCT") >= 981) & (pl.col("PRODUCT") <= 996))
        )
    )
    return (
        ln.filter(pl.col("CUSTCD").is_in(CORP_CUSTCD)),
        ln.filter(pl.col("CUSTCD").is_in(SME_CUSTCD)),
    )


def _filter_od(all_loan: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    od = all_loan.filter(
        (pl.col("ACCTYPE") == "OD")
        & (~pl.col("PRODUCT").is_in(UNWANTED_OD))
    )
    return (
        od.filter(pl.col("CUSTCD").is_in(CORP_CUSTCD)),
        od.filter(pl.col("CUSTCD").is_in(SME_CUSTCD)),
    )


def _filter_bt(bt_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    return (
        bt_df.filter(pl.col("CUSTCD").is_in(CORP_CUSTCD)),
        bt_df.filter(pl.col("CUSTCD").is_in(SME_CUSTCD)),
    )


# =============================================================================
# STEP 3 – Tag, combine and summarise
# =============================================================================
def _tag_and_concat(frames: list[pl.DataFrame], categ: str) -> pl.DataFrame:
    non_empty = [df.with_columns(pl.lit(categ).alias("CATEG")) for df in frames if not df.is_empty()]
    if not non_empty:
        return pl.DataFrame()
    return pl.concat(non_empty)


def _summarise(tagged: pl.DataFrame) -> pl.DataFrame:
    return (
        tagged
        .group_by(["CATEG", "ACCTYPE"])
        .agg(pl.sum("BALANCE").alias("BALANCE"))
        .sort(["CATEG", "ACCTYPE"])
    )


# =============================================================================
# STEP 4 – Format and write report  (ASA carriage-control characters)
# =============================================================================
PAGE_LENGTH = 60

_ASA_NEWLINE  = " "   # advance 1 line
_ASA_DBL_SKIP = "0"   # advance 2 lines (blank line before)
_ASA_NEW_PAGE = "1"   # advance to top of next page


def _asa(ctl: str, text: str) -> str:
    return f"{ctl}{text}"


def _page_header(lines: list[str], rdate: str) -> None:
    lines.append(_asa(_ASA_NEW_PAGE, f"REPORT ID : EIQBNMR1"))
    lines.append(_asa(_ASA_NEWLINE,  f"PBB - BREAKDOWN OF LOAN BY OPERATING DIVISION {rdate}"))
    lines.append(_asa(_ASA_DBL_SKIP, "=" * 59))
    lines.append(_asa(_ASA_NEWLINE,  f"{'LOAN TYPE':<20} {'A/C TYPE':<10} {'BALANCE':>20}"))
    lines.append(_asa(_ASA_NEWLINE,  "-" * 52))


def _build_report(summary_df: pl.DataFrame, rdate: str) -> list[str]:
    lines: list[str] = []
    _page_header(lines, rdate)
    body_lines = 5
    total_all  = 0.0

    categories = summary_df["CATEG"].unique(maintain_order=True).to_list()

    for category in categories:
        cat_df    = summary_df.filter(pl.col("CATEG") == category)
        cat_total = 0.0

        for row in cat_df.iter_rows(named=True):
            if body_lines >= PAGE_LENGTH - 4:
                _page_header(lines, rdate)
                body_lines = 5

            lines.append(_asa(_ASA_NEWLINE,
                f"{row['CATEG']:<20} {row['ACCTYPE']:<10} {row['BALANCE']:>20,.2f}"))
            body_lines += 1
            cat_total  += row["BALANCE"]

        lines.append(_asa(_ASA_NEWLINE, f"{' ' * 30}{'TOTAL:':<10} {cat_total:>20,.2f}"))
        lines.append(_asa(_ASA_NEWLINE, "-" * 52))
        body_lines += 2
        total_all  += cat_total

    lines.append(_asa(_ASA_NEWLINE, f"{' ' * 30}{'GRAND TOTAL:':<10} {total_all:>20,.2f}"))
    lines.append(_asa(_ASA_NEWLINE, "=" * 52))
    return lines


def _write_report(lines: list[str], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


# =============================================================================
# MAIN
# =============================================================================
def eiqbnmr1() -> None:
    all_loan = _load_loan_data()
    bt_df    = _load_bt_data()

    if all_loan.is_empty() and bt_df.is_empty():
        print("EIQBNMR1: No input data found – report not generated.")
        return

    lncorp, lnsme = _filter_ln(all_loan) if not all_loan.is_empty() else (pl.DataFrame(), pl.DataFrame())
    odcorp, odsme = _filter_od(all_loan) if not all_loan.is_empty() else (pl.DataFrame(), pl.DataFrame())
    btcorp, btsme = _filter_bt(bt_df)    if not bt_df.is_empty()    else (pl.DataFrame(), pl.DataFrame())

    corp_df = _tag_and_concat([lncorp, odcorp, btcorp], "CORPORATE LOANS")
    sme_df  = _tag_and_concat([lnsme,  odsme,  btsme],  "SME LOANS")

    all_frames = [df for df in [corp_df, sme_df] if not df.is_empty()]
    if not all_frames:
        print("EIQBNMR1: No qualifying accounts found – report not generated.")
        return

    summary_df   = _summarise(pl.concat(all_frames))
    report_lines = _build_report(summary_df, rdate_str)

    out_path = OUTPUT_DIR / f"EIQBNMR1_{REPTYEAR}{REPTMON}{REPTDAY}.txt"
    _write_report(report_lines, out_path)

    print(f"EIQBNMR1 completed – report written to {out_path}")


if __name__ == "__main__":
    eiqbnmr1()
