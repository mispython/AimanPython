#!/usr/bin/env python3
"""
Program : DMMISRX1
Function: Bank's Total Deposits (RM '000)
          Report ID : DMMISR01
          Merges daily position data (MIS, MISI, MISFX) and produces
            a tabular report of deposit balances by date.
"""

import duckdb
import polars as pl
import os
from datetime import datetime
from typing import Optional

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = os.environ.get("BASE_DIR",     "/data")
DEPOSIT_DIR   = os.path.join(BASE_DIR, "deposit")
MIS_DIR       = os.path.join(BASE_DIR, "mis")       # SAP.PBB.MIS.D<YEAR>
MISI_DIR      = os.path.join(BASE_DIR, "misi")      # SAP.PIBB.MIS.D<YEAR>
MISFX_DIR     = os.path.join(BASE_DIR, "misfx")     # SAP.PBB.MIS.FX
TEMP_DIR      = os.path.join(BASE_DIR, "temp")
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Input files
REPTDATE_FILE = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")

# Output
REPORT_FILE   = os.path.join(OUTPUT_DIR, "DMMISR01.txt")
# TEMP.DMMISR<REPTDAY> – intermediate snapshot (parquet)

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "

PAGE_LINES  = 60

os.makedirs(TEMP_DIR,   exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma(value: Optional[float], width: int = 15) -> str:
    """COMMA15. – right-aligned, comma-separated integer (no decimals)."""
    if value is None:
        return " " * width
    s = f"{int(value):,}"
    return s.rjust(width)


# ============================================================================
# STEP 1 – DERIVE REPORT DATE
# ============================================================================

def derive_report_date() -> dict:
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{REPTDATE_FILE}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError("REPTDATE file is empty.")

    val = row[0]
    if isinstance(val, (int, float)):
        reptdate = datetime.strptime(str(int(val)), "%Y%m%d")
    elif isinstance(val, datetime):
        reptdate = val
    else:
        reptdate = datetime.strptime(str(val)[:10], "%Y-%m-%d")

    return {
        "reptdate" : reptdate,
        "reptmon"  : reptdate.strftime("%m"),
        "reptyear" : reptdate.strftime("%Y"),
        "rdate"    : reptdate.strftime("%d/%m/%Y"),
        "reptday"  : reptdate.strftime("%d"),
        "rdatx"    : int(reptdate.strftime("%j")),   # Z5 Julian day
    }


# ============================================================================
# STEP 2 – LOAD SOURCE DATASETS
# ============================================================================

def load_sources(ctx: dict) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Load MIS.DYPOSN<MM>, MIS.DYPOSX<MM>, MISFX.NLF<MM>.
    Files are named with the two-digit month suffix.
    """
    mon = ctx["reptmon"]

    dyposn_file = os.path.join(MIS_DIR,   f"DYPOSN{mon}.parquet")
    dyposx_file = os.path.join(MIS_DIR,   f"DYPOSX{mon}.parquet")
    nlf_file    = os.path.join(MISFX_DIR, f"NLF{mon}.parquet")

    con = duckdb.connect()

    # MISP: DYPOSN – keep REPTDATE, ACECA, ACESA only
    misp = con.execute(
        f"SELECT REPTDATE, ACECA, ACESA FROM read_parquet('{dyposn_file}')"
    ).pl().sort("REPTDATE")

    # MIS1: DYPOSX – drop ACECA
    mis1_cols_raw = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{dyposx_file}')"
    ).fetchall()
    mis1_cols = [r[0] for r in mis1_cols_raw if r[0].upper() != "ACECA"]
    mis1 = con.execute(
        f"SELECT {', '.join(mis1_cols)} FROM read_parquet('{dyposx_file}')"
    ).pl().sort("REPTDATE")

    # MISNLF: NLF
    misnlf = con.execute(
        f"SELECT * FROM read_parquet('{nlf_file}')"
    ).pl().sort("REPTDATE")

    con.close()
    return misp, mis1, misnlf


# ============================================================================
# STEP 3 – MERGE & DERIVE COMPUTED COLUMNS
# ============================================================================

CUTOFF_DATE = datetime(2010, 6, 14)


def _round_k(col: str) -> pl.Expr:
    """INT(ROUND(col, 1000) / 1000) – round to nearest 1000 then integer-divide."""
    return (pl.col(col).round(0).map_elements(
        lambda v: int(round(v / 1000) * 1000 / 1000) if v is not None else 0,
        return_dtype=pl.Int64
    )).alias(col)


def build_dmmisr(misp: pl.DataFrame,
                 mis1: pl.DataFrame,
                 misnlf: pl.DataFrame,
                 ctx: dict) -> pl.DataFrame:
    """
    Full outer merge on REPTDATE, filter > 14JUN10,
    apply INT(ROUND(x,1000)/1000) to all numeric balance columns,
    derive totals and RDATEX.
    """
    # Merge all three on REPTDATE
    df = (
        misp
        .join(mis1,   on="REPTDATE", how="outer", suffix="_MIS1")
        .join(misnlf, on="REPTDATE", how="outer", suffix="_NLF")
    )

    # Filter: REPTDATE > 14JUN2010
    def to_dt(val) -> datetime:
        if isinstance(val, (int, float)):
            return datetime.strptime(str(int(val)), "%Y%m%d")
        if isinstance(val, datetime):
            return val
        return datetime.strptime(str(val)[:10], "%Y-%m-%d")

    df = df.with_columns(
        pl.col("REPTDATE")
          .map_elements(to_dt, return_dtype=pl.Datetime)
          .alias("REPTDATE_DT")
    )
    df = df.filter(pl.col("REPTDATE_DT") > CUTOFF_DATE)

    # Helper: round-to-1000 then divide by 1000 (integer result)
    def rk(v) -> int:
        if v is None:
            return 0
        return int(round(float(v) / 1000) * 1)  # INT(ROUND(v,1000)/1000)

    # Apply to all balance columns that exist in the frame
    balance_cols = [
        "TOTSAVG", "TOTSAVGI", "TOTDMND", "TOTDMNDI",
        "TOTFD",   "TOTFDI",
        "P068BAL", "P066BAL",
        "P395BAL", "P396BAL",
        "X068BALI","X068BALC",
        "X066BALI","X066BALC",
        "X395BALI","X395BALC",
        "X396BALI","X396BALC",
        "ACECA",   "ACESA",
    ]

    for col in balance_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).fill_null(0)
                  .map_elements(rk, return_dtype=pl.Int64)
                  .alias(col)
            )
        else:
            df = df.with_columns(pl.lit(0).cast(pl.Int64).alias(col))

    # Derived totals
    df = df.with_columns([
        (pl.col("TOTSAVG")  + pl.col("TOTSAVGI")).alias("TOTSAX"),
        (pl.col("TOTDMND")  + pl.col("TOTDMNDI")).alias("TOTCAX"),
        (pl.col("TOTFD")    + pl.col("TOTFDI")  ).alias("TOTFDX"),
        (pl.col("P068BAL")  + pl.col("P066BAL") ).alias("TOTP06X"),
        (pl.col("P395BAL")  + pl.col("P396BAL") ).alias("TOTP39X"),
        (pl.col("ACECA")    + pl.col("ACESA")   ).alias("TOTACEX"),
        pl.lit(ctx["rdatx"]).alias("RDATEX"),
    ])

    return df.sort("REPTDATE")


# ============================================================================
# STEP 4 – WRITE REPORT (PROC REPORT equivalent)
# ============================================================================

# Column definitions: (name, header, width)
REPORT_COLS = [
    ("REPTDATE_DT", "DATE",            10),
    ("TOTSAVG",     "SA-C",            15),
    ("TOTSAVGI",    "SA-I",            15),
    ("TOTSAX",      "TOTAL SA",        15),
    ("TOTDMND",     "CA-C",            15),
    ("TOTDMNDI",    "CA-I",            15),
    ("TOTCAX",      "TOTAL CA",        15),
    ("P068BAL",     "DFI&FI CA-C",     15),
    ("P066BAL",     "DFI&FI CA-I",     15),
    ("TOTP06X",     "TOTAL DFI&FI CA", 15),
    ("TOTFD",       "FD-C",            15),
    ("TOTFDI",      "FD-I",            15),
    ("TOTFDX",      "TOTAL FD",        15),
    ("P395BAL",     "DFI&FI FD-C",     15),
    ("P396BAL",     "DFI&FI FD-I",     15),
    ("TOTP39X",     "TOTAL DFI&FI FD", 15),
    ("X068BALI",    "CA-C INDV",       15),
    ("X068BALC",    "CA-C NON-INDV",   15),
    ("X066BALI",    "CA-I INDV",       15),
    ("X066BALC",    "CA-I NON-INDV",   15),
    ("X395BALI",    "FD-C INDV",       15),
    ("X395BALC",    "FD-C NON-INDV",   15),
    ("X396BALI",    "FD-I INDV",       15),
    ("X396BALC",    "FD-I NON-INDV",   15),
    ("ACECA",       "ACE-CA",          15),
    ("ACESA",       "ACE-SA",          15),
    ("TOTACEX",     "TOTAL ACE",       15),
]


def _fmt_cell(col: str, val, width: int) -> str:
    if col == "REPTDATE_DT":
        if val is None:
            return " " * width
        if isinstance(val, datetime):
            return val.strftime("%d/%m/%Y").rjust(width)
        return str(val)[:width].rjust(width)
    # All other columns are integer (COMMA15.)
    return fmt_comma(val, width)


def write_report(dmmisr: pl.DataFrame, rdate: str, reptday: str,
                 ctx: dict) -> None:
    lines: list[str] = []

    def title_block(asa: str = ASA_NEWPAGE) -> list[str]:
        return [
            f"{asa}REPORT ID : DMMISR01",
            f"{ASA_NEWLINE}PUBLIC BANK BERHAD",
            f"{ASA_NEWLINE}SALES ADMINISTRATION & SUPPORT",
            f"{ASA_NEWLINE}BANK'S TOTAL DEPOSITS (RM '000)",
            f"{ASA_NEWLINE}AS AT {rdate}",
            f"{ASA_NEWLINE}",
        ]

    def header_row() -> list[str]:
        hdr = " ".join(h.center(w)[:w] for _, h, w in REPORT_COLS)
        sep = " ".join("-" * w       for _, _, w in REPORT_COLS)
        return [
            f"{ASA_NEWLINE}{hdr}",
            f"{ASA_NEWLINE}{sep}",
        ]

    lines.extend(title_block())
    lines.extend(header_row())
    line_count = len(lines)

    records = dmmisr.to_dicts()
    for row in records:
        if line_count >= PAGE_LINES:
            lines.extend(title_block(ASA_NEWPAGE))
            lines.extend(header_row())
            line_count = len(lines)

        cells = " ".join(
            _fmt_cell(col, row.get(col), w)
            for col, _, w in REPORT_COLS
        )
        lines.append(f"{ASA_NEWLINE}{cells}")
        line_count += 1

    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {REPORT_FILE}")

    # Persist TEMP.DMMISR<REPTDAY> snapshot
    temp_out = os.path.join(TEMP_DIR, f"DMMISR{reptday}.parquet")
    dmmisr.write_parquet(temp_out)
    print(f"Temp snapshot : {temp_out}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("DMMISRX1 – Bank's Total Deposits (RM '000) starting...")

    ctx   = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    misp, mis1, misnlf = load_sources(ctx)

    dmmisr = build_dmmisr(misp, mis1, misnlf, ctx)

    write_report(dmmisr, ctx["rdate"], ctx["reptday"], ctx)

    print("DMMISRX1 – Done.")


if __name__ == "__main__":
    main()
