# !/usr/bin/env python3
"""
Program Name: EIBAABBA.py
"""

from datetime import datetime, timedelta
from pathlib import Path

import polars as pl


SAS_BASE_DATE = datetime(1960, 1, 1)
COLLATER_GROUPS = {
    "29": {"001", "006", "007", "014", "016", "024", "025", "026", "046", "048", "049"},
    "70": {
        "000",
        "011",
        "012",
        "013",
        "017",
        "018",
        "019",
        "021",
        "027",
        "028",
        "029",
        "030",
        "031",
        "105",
        "106",
    },
    "90": {
        "002",
        "003",
        "041",
        "042",
        "043",
        "058",
        "059",
        "067",
        "068",
        "069",
        "070",
        "071",
        "072",
        "078",
        "079",
        "084",
        "107",
    },
    "30": {"004", "005"},
    "10": {
        "032",
        "033",
        "034",
        "035",
        "036",
        "037",
        "038",
        "039",
        "040",
        "044",
        "050",
        "051",
        "052",
        "053",
        "054",
        "055",
        "056",
        "057",
        "060",
        "061",
        "062",
    },
    "40": {"065", "066", "075", "076", "082", "083", "093", "094", "095", "096", "097", "098", "101", "102", "103", "104"},
    "60": {"063", "064", "073", "074", "080", "081"},
    "50": {"010", "085", "086", "087", "088", "089", "090", "091", "092"},
    "00": {"009", "022", "023"},
    "21": {"008"},
    "22": {"045", "047"},
    "23": {"015"},
    "80": {"020"},
    "81": {"108", "109"},
    "99": {"077"},
}
CCLASSC_TO_COLLATER = {
    cclassc: collater
    for collater, cclassc_values in COLLATER_GROUPS.items()
    for cclassc in cclassc_values
}
MTHARR_THRESHOLDS = [
    (698, 23),
    (668, 22),
    (638, 21),
    (608, 20),
    (577, 19),
    (547, 18),
    (516, 17),
    (486, 16),
    (456, 15),
    (424, 14),
    (394, 13),
    (364, 12),
    (333, 11),
    (303, 10),
    (273, 9),
    (243, 8),
    (213, 7),
    (182, 6),
    (151, 5),
    (121, 4),
    (89, 3),
    (59, 2),
    (30, 1),
]


def read_dataset(path: Path, file_name: str) -> pl.DataFrame:
    try:
        return pl.read_parquet(path / file_name)
    except (TypeError, ValueError, FileNotFoundError):
        return pl.DataFrame()


def compute_reporting_context(reptdate: datetime) -> dict:
    week_map = {8: ("1", 1), 15: ("2", 9), 22: ("3", 16)}
    week, sdd = week_map.get(reptdate.day, ("4", 23))
    return {
        "week": week,
        "sdd": sdd,
        "month": f"{reptdate.month:02d}",
        "year": str(reptdate.year),
        "sdate": f"{reptdate.day:02d}{reptdate.month:02d}",
    }


def to_datetime(value):
    if value in (None, 0):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return SAS_BASE_DATE + timedelta(days=int(value))
    return datetime.strptime(str(value).zfill(8)[:8], "%m%d%Y")


def calculate_age(birthdt, snapshot_date: datetime) -> int:
    try:
        bdate = to_datetime(birthdt)
        if bdate is None:
            return 0
        return round((snapshot_date - bdate).days / 365)
    except (TypeError, ValueError):
        return 0


def calculate_mtharr(bldate, snapshot_date: datetime) -> int:
    try:
        bldate_dt = to_datetime(bldate)
        if bldate_dt is None:
            return 0

        days = (snapshot_date - bldate_dt).days + 1
        if days > 729:
            return int((days / 365) * 12)

        for threshold, result in MTHARR_THRESHOLDS:
            if days > threshold:
                return result
        return 0
    except (TypeError, ValueError):
        return 0


def map_collater(cclassc) -> str | None:
    if cclassc in (None, ""):
        return None
    return CCLASSC_TO_COLLATER.get(str(cclassc).zfill(3))


def process_abba_data(mniln_path: Path, snapshot_date: datetime) -> pl.DataFrame:
    abba_df = read_dataset(mniln_path, "LNNOTE.parquet")
    if abba_df.is_empty():
        return abba_df

    return (
        abba_df.filter(
            (pl.col("PAIDIND") != "P")
            & (((pl.col("LOANTYPE") >= 110) & (pl.col("LOANTYPE") <= 119)) | ((pl.col("LOANTYPE") >= 139) & (pl.col("LOANTYPE") <= 140)))
            & (pl.col("RISKRATE").is_in([2, 3, 4]))
        )
       .with_columns(
            pl.col("BIRTHDT").map_elements(lambda x: calculate_age(x, snapshot_date), return_dtype=pl.Int64).alias("AGE"),
            pl.col("PENDBRH").alias("BRANCH"),
            pl.col("COLLDESC").str.slice(0, 34).alias("COLLD"),
        )
        .select(
            [
                "ACCTNO",
                "NOTENO",
                "SECTOR",
                "BRANCH",
                "STATE",
                "RISKRATE",
                "BILLCNT",
                "LOANTYPE",
                "AGE",
                "COLLD",
                "PAYAMT",
            ]
        )
        .sort(["ACCTNO", "NOTENO"])
    )


def merge_sasb_data(abba_df: pl.DataFrame, sasd_path: Path, month: str, week: str, snapshot_date: datetime) -> pl.DataFrame:
    sasb_df = read_dataset(sasd_path, f"LOAN{month}{week}.parquet")
    if sasb_df.is_empty():
        return abba_df.with_columns(pl.lit(None).alias("BALANCE"), pl.lit(None).alias("MTHARR"), pl.lit(None).alias("OVERDUE"))

    sasb_df = (
        sasb_df.with_columns(
            pl.col("BLDATE").map_elements(lambda x: calculate_mtharr(x, snapshot_date), return_dtype=pl.Int64).alias("MTHARR")
        )
        .select(["ACCTNO", "NOTENO", "BALANCE", "MTHARR"])
        .sort(["ACCTNO", "NOTENO"])
    )
    return abba_df.join(sasb_df, on=["ACCTNO", "NOTENO"], how="left").with_columns((pl.col("PAYAMT") * pl.col("MTHARR")).alias("OVERDUE"))


def merge_customer_data(abba_df: pl.DataFrame, cisln_path: Path) -> pl.DataFrame:
    cisln_df = read_dataset(cisln_path, "LOAN.parquet")
    if cisln_df.is_empty():
        return abba_df.with_columns(
            pl.lit("").alias("CUSTNAME"),
            pl.lit("").alias("GENDER"),
            pl.lit("").alias("OCCUPAT"),
            pl.lit("").alias("ADDRLN1"),
            pl.lit("").alias("ADDRLN2"),
            pl.lit("").alias("ADDRLN3"),
            pl.lit("").alias("ADDRLN4"),
            pl.lit("").alias("ADDRLN5"),
        )

    cisln_df = cisln_df.select(
        [
            "ACCTNO",
            "CUSTNAME",
            "GENDER",
            "OCCUPAT",
            "ADDRLN1",
            "ADDRLN2",
            "ADDRLN3",
            "ADDRLN4",
            "ADDRLN5",
        ]
    ).sort("ACCTNO")
    return abba_df.join(cisln_df, on="ACCTNO", how="left")


def merge_collateral_data(abba_df: pl.DataFrame, coll_path: Path) -> pl.DataFrame:
    coll_df = read_dataset(coll_path, "COLLATER.parquet")
    if coll_df.is_empty():
        return abba_df

    coll_df = (
        coll_df.with_columns(pl.col("CCLASSC").map_elements(map_collater, return_dtype=pl.Utf8).alias("COLLATER"))
        .select(["ACCTNO", "NOTENO", "COLLATER"])
        .rename({"COLLATER": "COLLD"})
        .sort(["ACCTNO", "NOTENO"])
    )
    return (
        abba_df.join(
            coll_df.select(["ACCTNO", "NOTENO", "COLLD"]),
            on=["ACCTNO", "NOTENO"],
            how="left",
            suffix="_coll",
        )
        .with_columns(pl.coalesce(pl.col("COLLD_coll"), pl.col("COLLD")).alias("COLLD"))
        .drop("COLLD_coll")
    )


def finalize_output(abba_df: pl.DataFrame) -> pl.DataFrame:
    return abba_df.unique(subset=["ACCTNO", "NOTENO"], keep="first").sort(["BRANCH", "ACCTNO", "NOTENO"])


def eibaabba():
    base = Path.cwd()
    mniln_path = base / "MNILN"
    sasd_path = base / "SASD"
    cisln_path = base / "CISLN"
    coll_path = base / "COLL"

    reptdate_df = read_dataset(mniln_path, "REPTDATE.parquet")
    if reptdate_df.is_empty():
        print("No REPTDATE data found")
        return

    reptdate = reptdate_df["REPTDATE"][0]
    context = compute_reporting_context(reptdate)
    snapshot_date = datetime.strptime(context["sdate"] + context["year"][-2:], "%d%m%y")

    print("EIBAABBA - Account Analysis Report")
    print(f"Date: {context['sdate']}, Week: {context['week']}, SDD: {context['sdd']}")

    abba_df = process_abba_data(mniln_path, snapshot_date)
    if abba_df.is_empty():
        print("No LNNOTE data found")
        return

    abba_df = merge_sasb_data(abba_df, sasd_path, context["month"], context["week"], snapshot_date)
    abba_df = merge_customer_data(abba_df, cisln_path)
    abba_df = merge_collateral_data(abba_df, coll_path)
    abba_df = finalize_output(abba_df)

    generate_abba_output(abba_df, base / "ABBALST.csv")
    print(f"Processing complete. Records: {len(abba_df)}")


def generate_abba_output(df: pl.DataFrame, output_path: Path):
    if df.is_empty():
        return
    
    output_columns = [
        "ACCTNO",
        "NOTENO",
        "BRANCH",
        "LOANTYPE",
        "SECTOR",
        "STATE",
        "RISKRATE",
        "COLLD",
        "OVERDUE",
        "BALANCE",
        "MTHARR",
        "BILLCNT",
        "AGE",
        "GENDER",
        "OCCUPAT",
        "CUSTNAME",
        "ADDRLN1",
        "ADDRLN2",
        "ADDRLN3",
        "ADDRLN4",
        "ADDRLN5",
    ]
    output_df = df.select([column for column in output_columns if column in df.columns])
    output_df.write_csv(output_path, separator=";")
    
    print(f"Output file created: {output_path}")
    if len(output_df) > 0:
        print("\nFirst 3 records:")
        for row in output_df.head(3).iter_rows(named=True):
            print(
                f"  ACCTNO: {row.get('ACCTNO', '')}, "
                f"Customer: {row.get('CUSTNAME', '')[:20]}, "
                f"Balance: {row.get('BALANCE', 0):,.2f}"
            )


if __name__ == "__main__":
    eibaabba()
