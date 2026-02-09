#!/usr/bin/env python3
"""
File Name: EIBMSCRA
Summary report on staff participation under SCR by product with exception reporting.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl


# ============================================================================
# Configuration and Path Setup
# ============================================================================

INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = INPUT_DIR / "reptdate.parquet"
BRHFI_FILE = INPUT_DIR / "brhfi.parquet"
HRBR_FILE = INPUT_DIR / "hrbr.parquet"
HRHO_FILE = INPUT_DIR / "hrho.parquet"
CARDFILE_FILE = INPUT_DIR / "cardfile.parquet"
DEP_FILE = INPUT_DIR / "dep.parquet"
ELDS_FILE = INPUT_DIR / "elds.parquet"

SRSBR_OUTPUT = OUTPUT_DIR / "srsbr.parquet"
SRSHO_OUTPUT = OUTPUT_DIR / "srsho.parquet"
REPORT_OUTPUT = OUTPUT_DIR / "eibmscra_report.txt"

PAGE_LENGTH = 60


# ============================================================================
# Helper Functions
# ============================================================================


def load_parquet(path: Path, columns: list[str]) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")
    con = duckdb.connect()
    cols = ", ".join(columns)
    df = con.execute(
        f"SELECT {cols} FROM read_parquet(?)",
        [str(path)],
    ).pl()
    con.close()
    return df


def format_amount(value: float | int | None, width: int = 18) -> str:
    if value is None:
        return " " * width
    return f"{value:>{width},.2f}"


def format_count(value: float | int | None, width: int = 10) -> str:
    if value is None:
        return " " * width
    return f"{int(value):>{width},}"


class AsaReportWriter:
    def __init__(self, path: Path, page_length: int = 60) -> None:
        self.path = path
        self.page_length = page_length
        self.line_count = 0
        self.file = open(path, "w")

    def close(self) -> None:
        self.file.close()

    def write_line(self, content: str = "", asa: str = " ") -> None:
        self.file.write(f"{asa}{content}\n")
        self.line_count += 1

    def new_page(self) -> None:
        self.line_count = 0
        self.write_line(" " * 120, asa="1")

    def ensure_space(self, lines_needed: int) -> None:
        if self.line_count + lines_needed > self.page_length:
            self.new_page()


# ============================================================================
# Load Reference Data
# ============================================================================

reptdate_df = load_parquet(REPTDATE_FILE, ["REPTDATE"])
if len(reptdate_df) == 0:
    raise ValueError("REPTDATE file is empty")
reptdate = reptdate_df["REPTDATE"][0]
RDATE = reptdate.strftime("%d/%m/%y")

brh = load_parquet(BRHFI_FILE, ["BRANCH", "BRCHCD"]).with_columns(
    [pl.col("BRANCH").cast(pl.Int64), pl.col("BRCHCD").cast(pl.Utf8)]
)

hrbr = load_parquet(HRBR_FILE, ["STAFF", "BRANCH", "BRCHCD"]).with_columns(
    [
        pl.col("STAFF").cast(pl.Int64),
        pl.col("BRANCH").cast(pl.Int64),
        pl.col("BRCHCD").cast(pl.Utf8),
    ]
)
hrbr = hrbr.filter((pl.col("BRANCH") >= 2) & (pl.col("BRANCH") <= 267))

hrho = load_parquet(HRHO_FILE, ["STAFF", "HOE"]).with_columns(
    [pl.col("STAFF").cast(pl.Int64), pl.col("HOE").cast(pl.Utf8)]
)


# ============================================================================
# UCARD: Card Accounts
# ============================================================================

card = load_parquet(CARDFILE_FILE, ["ACCTNO", "OPNMM", "OPNYR", "BRANC", "SECO"])
card = card.with_columns(
    [
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("OPNMM").cast(pl.Int64),
        pl.col("OPNYR").cast(pl.Int64),
        pl.col("SECO").cast(pl.Utf8),
    ]
)

typec_expr = (
    pl.col("ACCTNO").cast(pl.Utf8).str.zfill(11).str.slice(0, 5).cast(pl.Int64)
)
staff8_expr = pl.col("SECO").cast(pl.Int64, strict=False)

card = card.with_columns([typec_expr.alias("TYPEC"), staff8_expr.alias("STAFF8")])

valid_typec = [3301, 3302, 3305, 3306, 3308, 3309, 5503, 5504]
card = card.filter(
    pl.col("TYPEC").is_in(valid_typec)
    & (((pl.col("OPNYR") == 6) & (pl.col("OPNMM") > 8)) | (pl.col("OPNYR") >= 7))
    & (pl.col("SECO").str.len_chars().is_between(2, 5))
    & (pl.col("STAFF8") > 1)
    & (pl.col("STAFF8") < 99999)
)

ucard = card.with_columns(
    [
        pl.when(pl.col("TYPEC") == 3309).then(1).otherwise(0).alias("C1CNT"),
        pl.when(pl.col("TYPEC") == 3309).then("X").otherwise("C").alias("NCORE"),
        pl.when(pl.col("TYPEC") == 3309).then(0).otherwise(1).alias("C2CNT"),
        pl.lit(0).alias("C3CNT"),
        pl.lit(0.0).alias("C1BAL"),
        pl.lit(0.0).alias("C2BAL"),
        pl.lit(0.0).alias("C3BAL"),
        pl.lit("CARD           ").alias("PRODUCT"),
        pl.col("STAFF8").alias("STAFF"),
    ]
).select([
    "STAFF",
    "PRODUCT",
    "NCORE",
    "C1CNT",
    "C2CNT",
    "C3CNT",
    "C1BAL",
    "C2BAL",
    "C3BAL",
])

ucbr = ucard.join(hrbr, on="STAFF", how="inner")
ucexc = ucard.join(hrbr, on="STAFF", how="anti")
ucho = ucexc.join(hrho, on="STAFF", how="inner")


# ============================================================================
# DEP: Deposit Accounts
# ============================================================================

dep = load_parquet(
    DEP_FILE,
    [
        "ACCTNO",
        "PRIMOFF",
        "SECNOFF",
        "XCORE",
        "AVGBAL",
        "YTDBALS",
        "NUMMTH",
        "BRANCH",
    ],
).with_columns(
    [
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("PRIMOFF").cast(pl.Int64),
        pl.col("SECNOFF").cast(pl.Int64),
        pl.col("XCORE").cast(pl.Utf8),
        pl.col("YTDBALS").cast(pl.Float64),
        pl.col("NUMMTH").cast(pl.Int64),
        pl.col("BRANCH").cast(pl.Int64),
    ]
)

dep = dep.with_columns(
    [
        pl.when((pl.col("ACCTNO") >= 1000000000) & (pl.col("ACCTNO") <= 1999999999))
        .then("FIXED DEPOSITS    ")
        .when((pl.col("ACCTNO") >= 7000000000) & (pl.col("ACCTNO") <= 7999999999))
        .then("FIXED DEPOSITS    ")
        .when((pl.col("ACCTNO") >= 3000000000) & (pl.col("ACCTNO") <= 3999999999))
        .then("CURRENT ACCOUNT   ")
        .when((pl.col("ACCTNO") >= 4000000000) & (pl.col("ACCTNO") <= 4999999999))
        .then("SAVING ACCOUNT    ")
        .when((pl.col("ACCTNO") >= 6000000000) & (pl.col("ACCTNO") <= 6999999999))
        .then("SAVING ACCOUNT    ")
        .otherwise(None)
        .alias("PRODUCT"),
        pl.when(pl.col("NUMMTH") != 0)
        .then((pl.col("YTDBALS") / pl.col("NUMMTH")).round(0))
        .otherwise(None)
        .alias("YTDBAL"),
        pl.lit("DEPO").alias("TAG"),
    ]
)

dep = dep.join(brh, on="BRANCH", how="left")

dpc1 = (
    dep.with_columns(pl.col("PRIMOFF").alias("STAFF"))
    .filter(~pl.col("STAFF").is_in([0]))
    .filter(pl.col("XCORE") == " ")
    .with_columns(
        [
            pl.lit("X").alias("NCORE"),
            pl.lit("CAT.1         ").alias("CATG"),
            pl.lit(1).alias("C1CNT"),
            pl.col("YTDBAL").alias("C1BAL"),
            pl.lit(0).alias("C2CNT"),
            pl.lit(0).alias("C3CNT"),
            pl.lit(0.0).alias("C2BAL"),
            pl.lit(0.0).alias("C3BAL"),
        ]
    )
)

staff_choice = pl.when(pl.col("SECNOFF").is_in([0]) | pl.col("SECNOFF").is_null())
staff_choice = staff_choice.then(pl.col("PRIMOFF")).otherwise(pl.col("SECNOFF"))

dpc2 = (
    dep.with_columns(staff_choice.alias("STAFF"))
    .filter(~pl.col("STAFF").is_in([0]))
    .filter(pl.col("XCORE").is_in(["C", "N"]))
    .with_columns(
        [
            pl.when(pl.col("XCORE") == "C")
            .then("CAT.2 CORE    ")
            .otherwise("CAT.2 NON-CORE")
            .alias("CATG"),
            pl.when(pl.col("XCORE") == "C").then("C").otherwise("N").alias("NCORE"),
            pl.when(pl.col("XCORE") == "C").then(1).otherwise(0).alias("C2CNT"),
            pl.when(pl.col("XCORE") == "N").then(1).otherwise(0).alias("C3CNT"),
            pl.when(pl.col("XCORE") == "C").then(pl.col("YTDBAL")).otherwise(None).alias("C2BAL"),
            pl.when(pl.col("XCORE") == "N").then(pl.col("YTDBAL")).otherwise(None).alias("C3BAL"),
            pl.lit(0).alias("C1CNT"),
            pl.lit(0.0).alias("C1BAL"),
            pl.lit(0.0).alias("C2BAL"),
        ]
    )
)


# ============================================================================
# ELDS: Account Level SCR
# ============================================================================

elds = load_parquet(
    ELDS_FILE,
    ["AANUM", "STAFX1", "STAFX2", "STAFX3", "PRODUCT", "YTDBAL"],
).with_columns(
    [
        pl.col("AANUM").cast(pl.Utf8),
        pl.col("STAFX1").cast(pl.Utf8),
        pl.col("STAFX2").cast(pl.Utf8),
        pl.col("STAFX3").cast(pl.Utf8),
        pl.col("PRODUCT").cast(pl.Utf8),
        pl.col("YTDBAL").cast(pl.Float64),
    ]
)

elds = elds.filter(pl.col("STAFX1") != "*****")
elds = elds.with_columns(
    [
        pl.lit(None).alias("NCORE"),
        pl.lit("ELDS").alias("TAG"),
        pl.col("AANUM").str.slice(0, 3).alias("BRCHCD"),
    ]
)

elds = elds.with_columns(
    pl.when(pl.col("STAFX1").is_between("00001", "99999"))
    .then("X")
    .otherwise(pl.col("NCORE"))
    .alias("NCORE")
)
elds = elds.with_columns(
    pl.when(pl.col("STAFX2").is_between("00001", "99999"))
    .then("C")
    .otherwise(pl.col("NCORE"))
    .alias("NCORE")
)
elds = elds.with_columns(
    pl.when(pl.col("STAFX3").is_between("00001", "99999"))
    .then("N")
    .otherwise(pl.col("NCORE"))
    .alias("NCORE")
)

elds = elds.join(brh, on="BRCHCD", how="left")

elds = elds.with_columns(
    [
        pl.when(pl.col("NCORE") == "X")
        .then(pl.col("STAFX1").cast(pl.Int64))
        .when(pl.col("NCORE") == "C")
        .then(pl.col("STAFX2").cast(pl.Int64))
        .when(pl.col("NCORE") == "N")
        .then(pl.col("STAFX3").cast(pl.Int64))
        .otherwise(None)
        .alias("STAFF"),
        pl.when(pl.col("NCORE") == "X").then("CAT.1         ")
        .when(pl.col("NCORE") == "C").then("CAT.2 CORE    ")
        .when(pl.col("NCORE") == "N").then("CAT.2 NON-CORE")
        .otherwise(None)
        .alias("CATG"),
        pl.when(pl.col("NCORE") == "X").then(1).otherwise(0).alias("C1CNT"),
        pl.when(pl.col("NCORE") == "C").then(1).otherwise(0).alias("C2CNT"),
        pl.when(pl.col("NCORE") == "N").then(1).otherwise(0).alias("C3CNT"),
        pl.when(pl.col("NCORE") == "X").then(pl.col("YTDBAL")).otherwise(0.0).alias("C1BAL"),
        pl.when(pl.col("NCORE") == "C").then(pl.col("YTDBAL")).otherwise(0.0).alias("C2BAL"),
        pl.when(pl.col("NCORE") == "N").then(pl.col("YTDBAL")).otherwise(0.0).alias("C3BAL"),
    ]
)


# ============================================================================
# Combine Records and Merge with HRHO
# ============================================================================

srs = pl.concat([elds, dpc1, dpc2], how="diagonal")

srsho = srs.join(hrho, on="STAFF", how="inner")
srs_no_ho = srs.join(hrho, on="STAFF", how="anti")
srsbr = srs_no_ho.filter(pl.col("BRANCH") > 0)
srsexc = srs_no_ho.filter(~(pl.col("BRANCH") > 0))

srsbr = pl.concat([srsbr, ucbr], how="diagonal")
srsho = pl.concat([srsho, ucho], how="diagonal")


# ============================================================================
# Summaries for Output
# ============================================================================

srsbr_summary = srsbr.group_by(["BRCHCD", "STAFF", "PRODUCT", "NCORE"]).agg(
    [
        pl.col("C1CNT").sum().alias("C1CNT"),
        pl.col("C2CNT").sum().alias("C2CNT"),
        pl.col("C3CNT").sum().alias("C3CNT"),
        pl.col("C1BAL").sum().alias("C1BAL"),
        pl.col("C2BAL").sum().alias("C2BAL"),
        pl.col("C3BAL").sum().alias("C3BAL"),
    ]
)

srsho_summary = srsho.group_by(["HOE", "STAFF", "PRODUCT", "NCORE"]).agg(
    [
        pl.col("C1CNT").sum().alias("C1CNT"),
        pl.col("C2CNT").sum().alias("C2CNT"),
        pl.col("C3CNT").sum().alias("C3CNT"),
        pl.col("C1BAL").sum().alias("C1BAL"),
        pl.col("C2BAL").sum().alias("C2BAL"),
        pl.col("C3BAL").sum().alias("C3BAL"),
    ]
)

srsbr_summary.write_parquet(SRSBR_OUTPUT)
srsho_summary.write_parquet(SRSHO_OUTPUT)

srss = pl.concat([srsho_summary, srsbr_summary], how="diagonal")
srss = srss.group_by(["STAFF", "PRODUCT", "NCORE"]).agg(
    [
        pl.col("C1CNT").sum().alias("C1CNT"),
        pl.col("C2CNT").sum().alias("C2CNT"),
        pl.col("C3CNT").sum().alias("C3CNT"),
        pl.col("C1BAL").sum().alias("C1BAL"),
        pl.col("C2BAL").sum().alias("C2BAL"),
        pl.col("C3BAL").sum().alias("C3BAL"),
    ]
)

srss = srss.with_columns(
    [
        pl.when(pl.col("NCORE") == "X").then(1).otherwise(0).alias("S1CNT"),
        pl.when(pl.col("NCORE") == "C").then(1).otherwise(0).alias("S2CNT"),
        pl.when(pl.col("NCORE") == "N").then(1).otherwise(0).alias("S3CNT"),
        pl.col("C1BAL").fill_null(0.0),
        pl.col("C2BAL").fill_null(0.0),
        pl.col("C3BAL").fill_null(0.0),
        pl.col("C1CNT").fill_null(0),
        pl.col("C2CNT").fill_null(0),
        pl.col("C3CNT").fill_null(0),
    ]
)

srsp = srss.group_by(["PRODUCT"]).agg(
    [
        pl.col("S1CNT").sum().alias("S1CNT"),
        pl.col("C1CNT").sum().alias("C1CNT"),
        pl.col("C1BAL").sum().alias("C1BAL"),
        pl.col("S2CNT").sum().alias("S2CNT"),
        pl.col("C2CNT").sum().alias("C2CNT"),
        pl.col("C2BAL").sum().alias("C2BAL"),
        pl.col("S3CNT").sum().alias("S3CNT"),
        pl.col("C3CNT").sum().alias("C3CNT"),
        pl.col("C3BAL").sum().alias("C3BAL"),
    ]
)


# ============================================================================
# Generate Report with ASA Carriage Control
# ============================================================================

report = AsaReportWriter(REPORT_OUTPUT, page_length=PAGE_LENGTH)

try:
    # Exception report section
    report.new_page()
    report.write_line("EXCEPTION REPORT : UMMATCHED STAFF ID AGAINST HR FILE")
    report.write_line("")

    elds_exc = srsexc.filter(pl.col("TAG") == "ELDS")
    report.write_line("ELDS EXCEPTIONS")
    report.write_line(f"{'AANUM':<15}{'STAFF':>8}{'PRODUCT':<20}{'YTDBAL':>18}")
    report.write_line("-" * 65)
    for row in elds_exc.iter_rows(named=True):
        report.ensure_space(1)
        report.write_line(
            f"{row.get('AANUM', ''):<15}{row.get('STAFF', ''):>8}"
            f"{row.get('PRODUCT', ''):<20}{format_amount(row.get('YTDBAL'))}"
        )

    report.write_line("")

    depo_exc = srsexc.filter(pl.col("TAG") == "DEPO")
    report.write_line("DEPOSIT EXCEPTIONS")
    report.write_line(f"{'ACCTNO':<15}{'STAFF':>8}{'PRODUCT':<20}{'YTDBAL':>18}")
    report.write_line("-" * 65)
    for row in depo_exc.iter_rows(named=True):
        report.ensure_space(1)
        report.write_line(
            f"{row.get('ACCTNO', ''):<15}{row.get('STAFF', ''):>8}"
            f"{row.get('PRODUCT', ''):<20}{format_amount(row.get('YTDBAL'))}"
        )

    # Summary report section
    report.new_page()
    report.write_line("SUMMARY REPORT ON STAFF PARTICIPATION UNDER SCR BY PRODUCT")
    report.write_line(f"REPORT DATE: {RDATE}")
    report.write_line("")
    header = (
        f"{'PRODUCT':<20}"
        f"{'CAT.1 #STAFF':>12}{'CAT.1 #ACCTS':>14}{'CAT.1 YTD BAL':>18}"
        f"{'CAT.2 #STAFF CORE':>18}{'CAT.2 #ACCTS CORE':>18}{'CAT.2 YTDBAL CORE':>20}"
        f"{'CAT.2 #STAFF NON-CORE':>22}{'CAT.2 #ACCTS NON-CORE':>22}"
        f"{'CAT.2 YTDBAL NON-CORE':>24}"
    )
    report.write_line(header)
    report.write_line("-" * len(header))

    totals = {
        "S1CNT": 0,
        "C1CNT": 0,
        "C1BAL": 0.0,
        "S2CNT": 0,
        "C2CNT": 0,
        "C2BAL": 0.0,
        "S3CNT": 0,
        "C3CNT": 0,
        "C3BAL": 0.0,
    }

    for row in srsp.iter_rows(named=True):
        report.ensure_space(1)
        report.write_line(
            f"{row.get('PRODUCT', ''):<20}"
            f"{format_count(row.get('S1CNT'), 12)}"
            f"{format_count(row.get('C1CNT'), 14)}"
            f"{format_amount(row.get('C1BAL'), 18)}"
            f"{format_count(row.get('S2CNT'), 18)}"
            f"{format_count(row.get('C2CNT'), 18)}"
            f"{format_amount(row.get('C2BAL'), 20)}"
            f"{format_count(row.get('S3CNT'), 22)}"
            f"{format_count(row.get('C3CNT'), 22)}"
            f"{format_amount(row.get('C3BAL'), 24)}"
        )

        totals["S1CNT"] += row.get("S1CNT", 0) or 0
        totals["C1CNT"] += row.get("C1CNT", 0) or 0
        totals["C1BAL"] += row.get("C1BAL", 0.0) or 0.0
        totals["S2CNT"] += row.get("S2CNT", 0) or 0
        totals["C2CNT"] += row.get("C2CNT", 0) or 0
        totals["C2BAL"] += row.get("C2BAL", 0.0) or 0.0
        totals["S3CNT"] += row.get("S3CNT", 0) or 0
        totals["C3CNT"] += row.get("C3CNT", 0) or 0
        totals["C3BAL"] += row.get("C3BAL", 0.0) or 0.0

    report.write_line("-" * len(header))
    report.write_line(
        f"{'TOTAL':<20}"
        f"{format_count(totals['S1CNT'], 12)}"
        f"{format_count(totals['C1CNT'], 14)}"
        f"{format_amount(totals['C1BAL'], 18)}"
        f"{format_count(totals['S2CNT'], 18)}"
        f"{format_count(totals['C2CNT'], 18)}"
        f"{format_amount(totals['C2BAL'], 20)}"
        f"{format_count(totals['S3CNT'], 22)}"
        f"{format_count(totals['C3CNT'], 22)}"
        f"{format_amount(totals['C3BAL'], 24)}"
    )
finally:
    report.close()

print(f"SRSBR summary written to: {SRSBR_OUTPUT}")
print(f"SRSHO summary written to: {SRSHO_OUTPUT}")
print(f"Report written to: {REPORT_OUTPUT}")
