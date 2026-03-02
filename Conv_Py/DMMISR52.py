#!/usr/bin/env python3
"""
Program : DMMISR52
Function: Summary of Movement in Bank's Demand Deposit by Range
          (Excluding ACE & Al-Wadiah Current Accounts)
          Net Increase/Decrease by Range
          Report ID : DMMISR52
"""

import duckdb
import polars as pl
import os
from datetime import datetime
from typing import Optional

# PBBDPFMT and PBMISFMT included for consistency with %INC directive
# (no specific functions needed beyond date utilities)
# from PBBDPFMT import ...
# from PBMISFMT import ...

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = os.environ.get("BASE_DIR",  "/data")
DEPOSIT_DIR   = os.path.join(BASE_DIR, "deposit")
MIS_DIR       = os.path.join(BASE_DIR, "mis")        # SAP.PBB.MIS.D<YEAR>
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Input files
REPTDATE_FILE = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")  # DEPOSIT.REPTDATE

# Output report
REPORT_FILE   = os.path.join(OUTPUT_DIR, "DMMISR52.txt")

# MOVERANG format buckets: (upper_bound_inclusive, label)
# SAS VALUE format maps a discrete numeric value to a label.
# The RANGE column in DYDDCR holds one of these bucket keys.
MOVERANG_BUCKETS: dict[int, str] = {
    300_000:    "1)  BELOW RM300K              ",
    500_000:    "2)  RM300K TO BELOW RM500K    ",
    1_000_000:  "3)  RM500K TO BELOW RM1 MIL   ",
    1_500_000:  "4)  RM1 MIL TO BELOW RM1.5 MIL",
    2_000_000:  "5)  RM1.5 MIL TO BELOW RM2 MIL",
    3_000_000:  "6)  RM2 MIL TO BELOW RM3 MIL  ",
    4_000_000:  "7)  RM3 MIL TO BELOW RM4 MIL  ",
    5_000_000:  "8)  RM4 MIL TO BELOW RM5 MIL  ",
    10_000_000: "9)  RM5 MIL TO BELOW RM10 MIL ",
    10_000_001: "10) ABOVE RM10 MIL            ",
}

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "

PAGE_LINES  = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma15_2(value: Optional[float], width: int = 15) -> str:
    """COMMA15.2 right-aligned."""
    if value is None:
        return " " * width
    s = f"{value:,.2f}"
    return s.rjust(width)


# ============================================================================
# STEP 1 – DERIVE REPORT DATE FROM DEPOSIT.REPTDATE
# ============================================================================

def _parse_reptdate(val) -> datetime:
    if isinstance(val, (int, float)):
        return datetime.strptime(str(int(val)), "%Y%m%d")
    if isinstance(val, datetime):
        return val
    return datetime.strptime(str(val)[:10], "%Y-%m-%d")


def _parse_z5(val) -> Optional[int]:
    """Normalise REPTDATE to Z5 Julian integer."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        s = str(int(val))
        if len(s) == 8:
            try:
                return int(datetime.strptime(s, "%Y%m%d").strftime("%j"))
            except ValueError:
                pass
        return int(val)
    if isinstance(val, datetime):
        return int(val.strftime("%j"))
    try:
        return int(datetime.strptime(str(val)[:10], "%Y-%m-%d").strftime("%j"))
    except ValueError:
        return None


def derive_report_date() -> dict:
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{REPTDATE_FILE}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError("REPTDATE file is empty.")

    reptdate = _parse_reptdate(row[0])

    return {
        "reptdate" : reptdate,
        "reptmon"  : reptdate.strftime("%m"),
        "reptyear" : reptdate.strftime("%Y"),
        "rdate"    : reptdate.strftime("%d/%m/%Y"),
        "zdate"    : int(reptdate.strftime("%j")),    # Z5 Julian
    }


# ============================================================================
# STEP 2 – LOAD MIS.DYDDCR<MM> FILTERED TO REPTDATE = ZDATE
# ============================================================================

def load_dyddcr(ctx: dict) -> pl.DataFrame:
    """
    Load MIS.DYDDCR<MM>, filter REPTDATE = ZDATE.
    """
    path = os.path.join(MIS_DIR, f"DYDDCR{ctx['reptmon']}.parquet")
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()

    df = df.with_columns(
        pl.col("REPTDATE")
          .map_elements(_parse_z5, return_dtype=pl.Int64)
          .alias("REPTDATE_Z5")
    )

    return df.filter(pl.col("REPTDATE_Z5") == ctx["zdate"])


# ============================================================================
# STEP 3 – WRITE REPORT  (PROC PRINT equivalent with ID RANGE)
# ============================================================================

LABEL_W  = 35   # width for RANGE / NET INCREASE/DECREASE column
DATA_W   = 15   # width for MOVEMENT / TOTAL MOVEMENT column


def write_report(dyddcr: pl.DataFrame, ctx: dict) -> None:
    lines: list[str] = []
    rdate = ctx["rdate"]

    # Title block
    lines.append(f"{ASA_NEWPAGE}REPORT ID : DMMISR52")
    lines.append(f"{ASA_NEWLINE}PUBLIC BANK BERHAD")
    lines.append(f"{ASA_NEWLINE}PRODUCT DEVELOPMENT & MARKETING")
    lines.append(
        f"{ASA_NEWLINE}SUMMARY OF MOVEMENT IN BANK'S DEMAND DEPOSIT AS AT {rdate}"
    )
    lines.append(
        f"{ASA_NEWLINE}(EXCLUDING ACE & AL-WADIAH CURRENT ACCOUNTS)"
    )
    lines.append(f"{ASA_NEWLINE}NET INCREASE/DECREASE BY RANGE")
    lines.append(f"{ASA_NEWLINE}")

    # Column headers (ID=RANGE labelled 'NET INCREASE/DECREASE', VAR=MOVEMENT labelled 'TOTAL MOVEMENT')
    sep = f"{'-' * LABEL_W}-+-{'-' * DATA_W}"
    lines.append(
        f"{ASA_NEWLINE}{'NET INCREASE/DECREASE':<{LABEL_W}} | {'TOTAL MOVEMENT':>{DATA_W}}"
    )
    lines.append(f"{ASA_NEWLINE}{sep}")

    total = 0.0

    if dyddcr.height == 0:
        lines.append(f"{ASA_NEWLINE}  (NO RECORDS)")
    else:
        # Sort by RANGE value to match SAS ID ordering
        for row in dyddcr.sort("RANGE").to_dicts():
            rng      = int(row.get("RANGE") or 0)
            movement = float(row.get("MOVEMENT") or 0.0)

            # Apply MOVERANG format label (direct key lookup; RANGE stores the
            # bucket key value as defined in the SAS VALUE statement)
            label = MOVERANG_BUCKETS.get(rng, f"{rng:<{LABEL_W}}")

            total += movement
            lines.append(
                f"{ASA_NEWLINE}{label:<{LABEL_W}} | {fmt_comma15_2(movement, DATA_W)}"
            )

    # SUM MOVEMENT footer
    lines.append(f"{ASA_NEWLINE}{sep}")
    lines.append(
        f"{ASA_NEWLINE}{'':>{LABEL_W}} | {fmt_comma15_2(total, DATA_W)}"
    )
    lines.append(f"{ASA_NEWLINE}{sep}")

    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {REPORT_FILE}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("DMMISR52 – Summary of Demand Deposit Movement by Range starting...")

    ctx    = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    dyddcr = load_dyddcr(ctx)
    write_report(dyddcr, ctx)

    print("DMMISR52 – Done.")


if __name__ == "__main__":
    main()
