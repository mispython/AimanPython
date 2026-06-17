#!/usr/bin/env python3
"""
Program: EIQBNMR1.py
"""

from pathlib import Path
import duckdb
import pandas as pd
import polars as pl

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# # Testing Path
# BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
# INPUT_DIR  = BASE_DIR / "input" / "uat"
# OUTPUT_DIR = BASE_DIR / "output" / "EIQBNMR1"

# Production Path
INPUT_DIR  = Path("/dwh")
OUTPUT_DIR = Path("/host/mis/output/report") / "EIQBNMR1"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# These are the only input files for this Python version of EIQBNMR1.
SAS_PATH   = get_latest_file(INPUT_DIR / "ln_ln", "ln")         # File name example "ln05126.sas7bdat"
BTSAS_PATH = get_latest_file(INPUT_DIR / "btrade", "btrad")     # File name example "btrad04426.sas7bdat"
# SAS_PATH   = get_latest_file(INPUT_DIR, "ln")         # File name example "ln05126.sas7bdat"
# BTSAS_PATH = get_latest_file(INPUT_DIR, "btrad")     # File name example "btrad04426.sas7bdat"

# REPORT DATE DERIVATION  (shared equivalent of SAS DATA REPTDATE step)
reptdate_values = get_reptdate_values(year_format="%Y")

REPTDATE = reptdate_values.reptdate
REPTYEAR = reptdate_values.reptyear
REPTMON  = reptdate_values.reptmon
REPTDAY  = reptdate_values.reptday
NOWK     = reptdate_values.nowk
RDATE    = REPTDATE.strftime("%d/%m/%y")

REPORT_FILE  = OUTPUT_DIR / f"EIQBNMR1_{REPTYEAR}{REPTMON}{REPTDAY}_report.txt"
SUMMARY_FILE = OUTPUT_DIR / f"EIQBNMR1_{REPTYEAR}{REPTMON}{REPTDAY}_summary.csv"

# SAS macro-list equivalents
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

CORP_CUSTCD = {
    "4", "5", "6", "13", "17", "20", "30", "31", "32",
    "33", "34", "35", "37", "38", "39", "40", "45",
    "57", "59", "61", "62", "63", "64", "71", "72",
    "73", "74", "75", "82", "83", "84", "86", "90",
    "91", "92", "98",
}

SME_CUSTCD = {
    "41", "42", "43", "44", "46", "47", "48", "49",
    "51", "52", "53", "54", "66", "67", "68", "69",
}


# REQUIRED_LOAN_COLUMNS = {"ACCTYPE", "PRODUCT", "CUSTCD", "BALANCE"}
# REQUIRED_BT_COLUMNS = {"ACCTNO", "CUSTCD", "BALANCE"}
REQUIRED_LOAN_COLUMNS = {"ACCTNO", "PRODUCT", "CUSTCODE", "BALANCE"}
REQUIRED_BT_COLUMNS = {"ACCTNO", "CUSTCODE", "BALANCE"}

def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read one .sas7bdat file and return a Polars DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")

    # >>>>>>>>>> Uncomment this -> For production <<<<<<<<<<
    pandas_df = pd.read_sas(
        path,
        format="sas7bdat",
        encoding="latin1",
    )

    # # >>>>>>>>>> Uncomment this -> For testing purposes <<<<<<<<<<
    # reader = pd.read_sas(
    #     path,
    #     format="sas7bdat",
    #     encoding="latin1",
    #     chunksize = 10000          
    # )
    # pandas_df = next(reader)

    pandas_df.columns = [
        str(column).upper().strip()
        for column in pandas_df.columns
    ]
    
    # For testing purposes
    print("\nDEBUG COLUMN NAMES:")
    # print(pandas_df.columns.tolist())
    print(pandas_df.head(10))

    return pl.from_pandas(pandas_df)


def _require_columns(df: pl.DataFrame, required: set[str], source: Path) -> None:
    """Fail early with a clear message if the SAS file lacks needed columns."""
    missing = sorted(required.difference(df.columns))
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"{source} is missing required column(s): {missing_text}")


def _normalise_common_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Normalise source values used by the report filters and summary."""
    return df.with_columns(
        # pl.col("CUSTCD").cast(pl.Utf8).str.strip_chars().alias("CUSTCD"),
        pl.col("CUSTCODE")
        .cast(pl.Int64, strict=False)
        .cast(pl.Utf8)
        .str.strip_chars()
        .alias("CUSTCODE"),
        pl.col("BALANCE").cast(pl.Float64).fill_null(0.0).alias("BALANCE"),
    )


def _load_loan_data() -> pl.DataFrame:
    """Load LN/OD rows from the single loan SAS dataset."""

    loan_df = _read_sas7bdat(SAS_PATH)

    _require_columns(loan_df, REQUIRED_LOAN_COLUMNS, SAS_PATH)

    return _normalise_common_columns(loan_df).with_columns(

        # Convert ACCTNO safely
        pl.col("ACCTNO").cast(pl.Int64, strict=False).alias("ACCTNO"),

        # PRODUCT cleanup
        pl.col("PRODUCT").cast(pl.Int64, strict=False).alias("PRODUCT"),

        # Derive ACCTYPE from ACCTNO
        pl.when(
            (pl.col("ACCTNO") >= 3000000000)
            & (pl.col("ACCTNO") <= 3999999999)
        )
        .then(pl.lit("OD"))
        .otherwise(pl.lit("LN"))
        .alias("ACCTYPE")
        
    )



def _load_bt_data() -> pl.DataFrame:
    """Load BT rows from the single bills/trust receipts SAS dataset."""
    bt_df = _read_sas7bdat(BTSAS_PATH)
    _require_columns(bt_df, REQUIRED_BT_COLUMNS, BTSAS_PATH)

    # SAS does: SET BTSAS.BTRAD...(RENAME=ACCTNO=ACCTNO1); ACCTNO=ACCTNO1;
    # Here we keep ACCTNO and add the account type needed for the report.
    return _normalise_common_columns(bt_df).with_columns(
        pl.lit("BT").alias("ACCTYPE"),
    )


def _split_by_customer_type(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return corporate and SME subsets using the SAS CUSTCD lists."""
    return(
        # df.filter(pl.col("CUSTCD").is_in(CORP_CUSTCD)),
        # df.filter(pl.col("CUSTCD").is_in(SME_CUSTCD)),
        df.filter(pl.col("CUSTCODE").is_in(CORP_CUSTCD)),
        df.filter(pl.col("CUSTCODE").is_in(SME_CUSTCD)),
    )


def _filter_ln(all_loan: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    ln_df = all_loan.filter(
        (pl.col("ACCTYPE") == "LN")
        & (~pl.col("PRODUCT").is_in(UNWANTED_LN))
        & ~(
            pl.col("PRODUCT").is_between(200, 299, closed="both")
            | pl.col("PRODUCT").is_between(981, 996, closed="both")
        )
    )
    return _split_by_customer_type(ln_df)


def _filter_od(all_loan: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    od_df = all_loan.filter(
        (pl.col("ACCTYPE") == "OD")
        & (~pl.col("PRODUCT").is_in(UNWANTED_OD))
    )
    return _split_by_customer_type(od_df)


def _filter_bt(bt_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    return _split_by_customer_type(bt_df)


def _tag_for_summary(frames: list[pl.DataFrame], category: str) -> pl.DataFrame:
    """Keep only summary columns and add the report category."""
    summary_frames = []
    for frame in frames:
        if not frame.is_empty():
            summary_frames.append(
                frame.select("ACCTYPE", "BALANCE").with_columns(
                    pl.lit(category).alias("CATEG")
                )
            )

    if not summary_frames:
        return pl.DataFrame(schema={"ACCTYPE": pl.Utf8, "BALANCE": pl.Float64, "CATEG": pl.Utf8})
    return pl.concat(summary_frames)


def _summarise(tagged: pl.DataFrame) -> pl.DataFrame:
    return (
        tagged.group_by(["CATEG", "ACCTYPE"])
        .agg(pl.sum("BALANCE").alias("BALANCE"))
        .sort(["CATEG", "ACCTYPE"])
    )


# ============================================================================
# REPORT COLUMN WIDTHS
# ============================================================================

LOAN_W    = 20
ACTYPE_W  = 10
BALANCE_W = 36

REPORT_W  = LOAN_W + ACTYPE_W + BALANCE_W + 2
TOTAL_LABEL_W = LOAN_W + ACTYPE_W + 1

def _build_report(summary_df: pl.DataFrame) -> list[str]:
    lines = [
        "REPORT ID : EIQBNMR1",
        f"PBB - BREAKDOWN OF LOAN BY OPERATING DIVISION {RDATE}",
        f"Report date: {REPTDATE.isoformat()}  Week: {NOWK}  Month: {REPTMON}",
        "=" * REPORT_W,
        f"{'LOAN TYPE':<{LOAN_W}} "
        f"{'A/C TYPE':<{ACTYPE_W}} "
        f"{'BALANCE':>{BALANCE_W}}",
        "-" * REPORT_W,
    ]

    grand_total = 0.0
    for category in summary_df["CATEG"].unique(maintain_order=True).to_list():
        category_df = summary_df.filter(pl.col("CATEG") == category)
        category_total = 0.0

        for row in category_df.iter_rows(named=True):
            balance = row["BALANCE"] or 0.0
            lines.append(
                f"{row['CATEG']:<{LOAN_W}} "
                f"{row['ACCTYPE']:<{ACTYPE_W}} "
                f"{balance:>{BALANCE_W},.2f}"
            )
            category_total += balance

        lines.append(
            f"{'TOTAL:':>{TOTAL_LABEL_W + 16}} "
            f"{category_total:>20,.2f}"
        )
        lines.append("-" * 66)
        grand_total += category_total

    lines.append(
        f"{'GRAND TOTAL:':>{TOTAL_LABEL_W + 16}} "
        f"{grand_total:>20,.2f}"
    )
    lines.append("=" * REPORT_W)
    return lines


def _write_report(lines: list[str]) -> None:
    with open(REPORT_FILE, "w", encoding="utf-8") as report:
        report.write("\n".join(lines) + "\n")


def eiqbnmr1() -> None:
    """Run the EIQBNMR1 report from the two required SAS input files."""
    loan_df = _load_loan_data()

    # DEBUGGING
    print("\n========== LOAN DEBUG ==========\n")
    print(
        loan_df.select(
            ["ACCTNO", "ACCTYPE", "CUSTCODE", "PRODUCT", "BALANCE"]
        ).head(20)
    )
    bt_df = _load_bt_data()

    lncorp_df, lnsme_df = _filter_ln(loan_df)
    odcorp_df, odsme_df = _filter_od(loan_df)
    btcorp_df, btsme_df = _filter_bt(bt_df)

    corp_df = _tag_for_summary([lncorp_df, odcorp_df, btcorp_df], "CORPORATE LOANS")
    sme_df = _tag_for_summary([lnsme_df, odsme_df, btsme_df], "SME LOANS")

    report_source = pl.concat([corp_df, sme_df])
    if report_source.is_empty():
        print("EIQBNMR1: No qualifying LN, OD, or BT accounts found; no report generated.")
        return

    summary_df = _summarise(report_source)
    summary_df.write_csv(SUMMARY_FILE)
    _write_report(_build_report(summary_df))

    print(f"EIQBNMR1 completed for report date {REPTDATE.isoformat()}.")
    print(f"Report written: {REPORT_FILE}")
    print(f"Summary written: {SUMMARY_FILE}")

    # To show data - For testing purposes only
    print("\n ========== PREVIEW ========== \n")
    with open(REPORT_FILE, "r", encoding="utf-8") as report:
        print(report.read())
    
    print("\n ========== PREVIEW ========== \n")
    print(pl.read_csv(SUMMARY_FILE))

if __name__ == "__main__":
    eiqbnmr1()
    print("[EIQBNMR1] Program completed successfully.")


# Balance / TOTAL / GRAND TOTAL append to be +16 row to the right
