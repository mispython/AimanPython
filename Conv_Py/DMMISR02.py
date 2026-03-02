#!/usr/bin/env python3
"""
Program : DMMISR02
Function: Movement in Bank's Saving & Demand Deposits
          Report ID : DMMISR02
          Produces:
            - Section 1: Saving Deposits Movement (Conventional & Islamic)
            - Section 2: Demand Deposits Credit Movement (Non-SPTF & SPTF)
            - Section 3: Summary of Movement by Range (DYDDCR)
"""

import duckdb
import polars as pl
import os
from datetime import datetime, timedelta
from typing import Optional

# Import format definitions from PBBDPFMT and PBMISFMT
from PBBDPFMT import CAProductFormat
from PBMISFMT import format_brchcd

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = os.environ.get("BASE_DIR",  "/data")
MIS_DIR       = os.path.join(BASE_DIR, "mis")       # SAP.PBB.MIS.D<YEAR>
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

REPORT_FILE   = os.path.join(OUTPUT_DIR, "DMMISR02.txt")

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "

PAGE_LINES  = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================================
# PRODUCT SETS
# ============================================================================

SA_ISLAMIC_PRODUCTS  = {204, 207, 214, 215}
ACE_PRODUCTS         = {150, 151, 152, 181}
SPTF_PRODUCTS_LOW    = set(range(60, 65))    # 60–64
SPTF_PRODUCTS_HIGH   = set(range(160, 166))  # 160–165
SPTF_PRODUCTS        = SPTF_PRODUCTS_LOW | SPTF_PRODUCTS_HIGH

# MOVERANG format buckets (upper-bound inclusive)
MOVERANG_BUCKETS = [
    (300_000,    "1)  BELOW RM300K              "),
    (500_000,    "2)  RM300K TO BELOW RM500K    "),
    (1_000_000,  "3)  RM500K TO BELOW RM1 MIL   "),
    (1_500_000,  "4)  RM1 MIL TO BELOW RM1.5 MIL"),
    (2_000_000,  "5)  RM1.5 MIL TO BELOW RM2 MIL"),
    (3_000_000,  "6)  RM2 MIL TO BELOW RM3 MIL  "),
    (4_000_000,  "7)  RM3 MIL TO BELOW RM4 MIL  "),
    (5_000_000,  "8)  RM4 MIL TO BELOW RM5 MIL  "),
    (10_000_000, "9)  RM5 MIL TO BELOW RM10 MIL "),
    (10_000_001, "10) ABOVE RM10 MIL            "),
]

# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma(value: Optional[float], width: int = 15, dec: int = 2) -> str:
    if value is None:
        return " " * width
    s = f"{value:,.{dec}f}"
    return s.rjust(width)


def fmt_14_1(value: Optional[float], width: int = 14) -> str:
    """14.1 format – right-aligned one decimal."""
    if value is None:
        return " " * width
    s = f"{value:,.1f}"
    return s.rjust(width)


def fmt_10_2(value: Optional[float], width: int = 10) -> str:
    if value is None:
        return " " * width
    s = f"{value:.2f}"
    return s.rjust(width)


# ============================================================================
# STEP 1 – DERIVE REPORT DATE  (TODAY() - 1)
# ============================================================================

def derive_report_date() -> dict:
    reptdate = datetime.today() - timedelta(days=1)
    return {
        "reptdate" : reptdate,
        "reptmon"  : reptdate.strftime("%m"),
        "reptyear" : reptdate.strftime("%Y"),
        "rdate"    : reptdate.strftime("%d/%m/%Y"),
        "zdate"    : int(reptdate.strftime("%j")),    # Z5 Julian
    }


# ============================================================================
# STEP 2 – LOAD MIS.DYMVNT<MM>
# ============================================================================

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


def load_dymvnt(ctx: dict) -> pl.DataFrame:
    path = os.path.join(MIS_DIR, f"DYMVNT{ctx['reptmon']}.parquet")
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    df = df.with_columns(
        pl.col("REPTDATE")
          .map_elements(_parse_z5, return_dtype=pl.Int64)
          .alias("REPTDATE_Z5")
    )
    return df


def load_dyddcr(ctx: dict) -> pl.DataFrame:
    path = os.path.join(MIS_DIR, f"DYDDCR{ctx['reptmon']}.parquet")
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    df = df.with_columns(
        pl.col("REPTDATE")
          .map_elements(_parse_z5, return_dtype=pl.Int64)
          .alias("REPTDATE_Z5")
    )
    return df


# ============================================================================
# SECTION 1 – SAVING DEPOSITS  (%MACRO SAVING)
# ============================================================================

def build_sdmvnt(dymvnt: pl.DataFrame, zdate: int) -> pl.DataFrame:
    """
    Filter DEPTYPE='S' and REPTDATE=ZDATE.
    Compute MOVEMENT, BRCH, SATYPE.
    Sort DESCENDING MOVEMENT, ACCTNO, SATYPE.
    """
    df = dymvnt.filter(
        (pl.col("DEPTYPE") == "S") & (pl.col("REPTDATE_Z5") == zdate)
    )
    df = df.with_columns([
        (pl.col("CREDIT") - pl.col("DEBIT")).round(2).alias("MOVEMENT"),
        pl.col("BRANCH")
          .map_elements(lambda b: format_brchcd(int(b) if b is not None else None),
                        return_dtype=pl.Utf8)
          .alias("BRCH"),
        pl.when(pl.col("PRODUCT").is_in(list(SA_ISLAMIC_PRODUCTS)))
          .then(pl.lit("ISLM"))
          .otherwise(pl.lit("CONV"))
          .alias("SATYPE"),
    ])
    return df.sort(["MOVEMENT", "ACCTNO", "SATYPE"], descending=[True, False, False])


# ============================================================================
# SECTION 2 – DEMAND DEPOSITS  (%MACRO CURRENT)
# ============================================================================

def build_crmove(dymvnt: pl.DataFrame, zdate: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Filter DEPTYPE in ('D','N'), REPTDATE=ZDATE, ABS(CREDIT-DEBIT) >= 1M,
    exclude ACE products, apply CAPROD format (exclude OTHER),
    compute MOVEMENT / PREBAL, filter CURBAL >= 0 and ABS(MOVEMENT) >= 1M.
    Split into CRMVSPTF (SPTF products) and CRMOVE (non-SPTF).
    """
    df = dymvnt.filter(
        (pl.col("DEPTYPE").is_in(["D", "N"])) &
        (pl.col("REPTDATE_Z5") == zdate) &
        ((pl.col("CREDIT") - pl.col("DEBIT")).abs() >= 1_000_000) &
        (~pl.col("PRODUCT").cast(pl.Int64).is_in(list(ACE_PRODUCTS)))
    )

    def caprod(product) -> str:
        return CAProductFormat.format(int(product) if product is not None else None)

    df = df.with_columns([
        pl.col("BRANCH")
          .map_elements(lambda b: format_brchcd(int(b) if b is not None else None),
                        return_dtype=pl.Utf8)
          .alias("BRCH"),
        pl.col("PRODUCT")
          .map_elements(caprod, return_dtype=pl.Utf8)
          .alias("DDTYPE"),
    ])

    # Exclude DDTYPE = 'OTHER'
    df = df.filter(pl.col("DDTYPE") != "OTHER")

    # Compute raw MOVEMENT and PREBAL
    df = df.with_columns([
        (pl.col("CREDIT") - pl.col("DEBIT")).alias("RAW_MOVEMENT"),
        (pl.col("CURBAL") - (pl.col("CREDIT") - pl.col("DEBIT"))).alias("PREBAL"),
    ])

    # Apply balance adjustment: only CURBAL >= 0
    df = df.filter(pl.col("CURBAL") >= 0)

    df = df.with_columns(
        pl.when(pl.col("PREBAL") < 0)
          .then(pl.col("CURBAL"))                          # MOVEMENT = 0 + CURBAL
          .otherwise(pl.col("CURBAL") - pl.col("PREBAL")) # MOVEMENT = CURBAL - PREBAL
          .alias("MOVEMENT")
    )

    # Filter ABS(MOVEMENT) >= 1M
    df = df.filter(pl.col("MOVEMENT").abs() >= 1_000_000)

    # Split SPTF vs non-SPTF
    crmvsptf = (
        df.filter(pl.col("PRODUCT").cast(pl.Int64).is_in(list(SPTF_PRODUCTS)))
          .sort(["MOVEMENT", "ACCTNO"], descending=[True, False])
    )
    crmove = (
        df.filter(~pl.col("PRODUCT").cast(pl.Int64).is_in(list(SPTF_PRODUCTS)))
          .sort(["MOVEMENT", "ACCTNO"], descending=[True, False])
    )

    return crmove, crmvsptf


# ============================================================================
# REPORT RENDERING HELPERS
# ============================================================================

def _sep(widths: list[int], char: str = "-") -> str:
    return " ".join(char * w for w in widths)


def _rbreak(widths: list[int],
            summary_cells: list[str],
            lines: list[str]) -> None:
    """Append RBREAK AFTER / DUL OL SUMMARIZE lines."""
    lines.append(f"{ASA_NEWLINE}{_sep(widths, '-')}")
    lines.append(f"{ASA_NEWLINE}{_sep(widths, '=')}")
    lines.append(f"{ASA_NEWLINE}{' '.join(summary_cells)}")
    lines.append(f"{ASA_NEWLINE}{_sep(widths, '=')}")


# ============================================================================
# WRITE SAVING DEPOSITS SECTION
# ============================================================================

SA_COLS  = [("BRANCH",6),("BRCH",6),("NAME",25),("ACCTNO",20),("MOVEMT",14)]
SA_HDRS  = [("BRANCH",""),("/CODE ",""),("NAME OF CUSTOMER",""),
            ("ACCOUNT NUMBER",""),("INCREASE/","DECREASE/(RM THOUSAND)")]


def _sa_header(lines: list[str]) -> None:
    widths = [c[1] for c in SA_COLS]
    h1 = " ".join(SA_HDRS[i][0].center(widths[i])[:widths[i]] for i in range(len(SA_COLS)))
    h2 = " ".join(SA_HDRS[i][1].center(widths[i])[:widths[i]] for i in range(len(SA_COLS)))
    lines += [f"{ASA_NEWLINE}{h1}", f"{ASA_NEWLINE}{h2}",
              f"{ASA_NEWLINE}{_sep(widths)}"]


def write_saving_section(sdmvnt: pl.DataFrame,
                         satype: str,
                         rdate: str,
                         lines: list[str]) -> None:
    sub = sdmvnt.filter(pl.col("SATYPE") == satype)
    tag = "(CONVENTIONAL)" if satype == "CONV" else "(ISLAMIC)"
    widths = [c[1] for c in SA_COLS]

    lines.append(f"{ASA_NEWPAGE}REPORT ID: DMMISR02")
    lines.append(f"{ASA_NEWLINE}PUBLIC BANK BERHAD ")
    lines.append(f"{ASA_NEWLINE}PRODUCT DEVELOPMENT & MARKETING")
    lines.append(f"{ASA_NEWLINE}MOVEMENT IN BANK'S SAVING DEPOSITS AS AT {rdate}")
    lines.append(f"{ASA_NEWLINE}NET INCREASE/DECREASE OF RM 50 THOUSAND & ABOVE PER CUSTOMER")
    lines.append(f"{ASA_NEWLINE}{tag}")
    lines.append(f"{ASA_NEWLINE}")
    _sa_header(lines)

    if sub.height == 0:
        lines.append(f"{ASA_NEWLINE}")
        lines.append(f"{ASA_NEWLINE}     ****************************************************")
        lines.append(f"{ASA_NEWLINE}     NO CUSTOMER WITH MOVEMENT OF 50 THOUSAND AND ABOVE")
        lines.append(f"{ASA_NEWLINE}     AT {rdate}")
        lines.append(f"{ASA_NEWLINE}     ***************************************************")
        return

    tot_mov = 0.0
    line_count = 8
    for row in sub.to_dicts():
        if line_count >= PAGE_LINES:
            _sa_header(lines); line_count = 3
        movement = float(row.get("MOVEMENT") or 0.0)
        movemt   = movement * 0.001
        tot_mov += movemt
        cells = [
            str(int(row.get("BRANCH") or 0)).rjust(6),
            str(row.get("BRCH") or "").ljust(6)[:6],
            str(row.get("NAME") or "").ljust(25)[:25],
            str(int(row.get("ACCTNO") or 0)).rjust(20),
            fmt_14_1(movemt, 14),
        ]
        lines.append(f"{ASA_NEWLINE}{' '.join(cells)}")
        line_count += 1

    summary = [" " * (widths[0]+1+widths[1]+1+widths[2]+1+widths[3]),
               fmt_14_1(tot_mov, 14)]
    _rbreak(widths, summary, lines)


# ============================================================================
# WRITE DEMAND DEPOSITS SECTION
# ============================================================================

DD_COLS = [("BRANCH",6),("BRCH",6),("NAME",25),("ACCTNO",20),
           ("PREBAL",15),("CURBAL",15),("MOVEMT",10)]
DD_HDRS = [("BRANCH",""),("/CODE ",""),("NAME OF CUSTOMER",""),
           ("ACCOUNT NUMBER",""),("PREVIOUS BALANCE",""),
           ("CURRENT BALANCE",""),("INCREASE/","DECREASE/(RM MILLION)")]


def _dd_header(lines: list[str]) -> None:
    widths = [c[1] for c in DD_COLS]
    h1 = " ".join(DD_HDRS[i][0].center(widths[i])[:widths[i]] for i in range(len(DD_COLS)))
    h2 = " ".join(DD_HDRS[i][1].center(widths[i])[:widths[i]] for i in range(len(DD_COLS)))
    lines += [f"{ASA_NEWLINE}{h1}", f"{ASA_NEWLINE}{h2}",
              f"{ASA_NEWLINE}{_sep([c[1] for c in DD_COLS])}"]


def write_dd_section(df: pl.DataFrame,
                     title4: str,
                     empty_msg: str,
                     rdate: str,
                     lines: list[str]) -> None:
    widths = [c[1] for c in DD_COLS]

    lines.append(f"{ASA_NEWPAGE}REPORT ID: DMMISR02")
    lines.append(f"{ASA_NEWLINE}PUBLIC BANK BERHAD ")
    lines.append(f"{ASA_NEWLINE}PRODUCT DEVELOPMENT & MARKETING")
    lines.append(f"{ASA_NEWLINE}{title4} {rdate}")
    lines.append(f"{ASA_NEWLINE}NET INCREASE/DECREASE OF RM 1 MILLION & ABOVE PER CUSTOMER")
    lines.append(f"{ASA_NEWLINE}")
    _dd_header(lines)

    if df.height == 0:
        lines.append(f"{ASA_NEWLINE}")
        lines.append(f"{ASA_NEWLINE}     *******************************************************")
        lines.append(f"{ASA_NEWLINE}     {empty_msg}")
        lines.append(f"{ASA_NEWLINE}     AT {rdate}")
        lines.append(f"{ASA_NEWLINE}     *******************************************************")
        return

    tot_pre = tot_cur = tot_mov = 0.0
    line_count = 8
    for row in df.to_dicts():
        if line_count >= PAGE_LINES:
            _dd_header(lines); line_count = 3
        prebal   = float(row.get("PREBAL")   or 0.0)
        curbal   = float(row.get("CURBAL")   or 0.0)
        movement = float(row.get("MOVEMENT") or 0.0)
        movemt   = movement * 0.000001
        tot_pre += prebal; tot_cur += curbal; tot_mov += movemt
        cells = [
            str(int(row.get("BRANCH") or 0)).rjust(6),
            str(row.get("BRCH") or "").ljust(6)[:6],
            str(row.get("NAME") or "").ljust(25)[:25],
            str(int(row.get("ACCTNO") or 0)).rjust(20),
            fmt_comma(prebal, 15, 2),
            fmt_comma(curbal, 15, 2),
            fmt_10_2(movemt, 10),
        ]
        lines.append(f"{ASA_NEWLINE}{' '.join(cells)}")
        line_count += 1

    blank = " " * (widths[0]+1+widths[1]+1+widths[2]+1+widths[3])
    summary = [blank, fmt_comma(tot_pre,15,2),
               fmt_comma(tot_cur,15,2), fmt_10_2(tot_mov,10)]
    _rbreak(widths, summary, lines)


# ============================================================================
# SECTION 3 – DEMAND DEPOSIT SUMMARY BY RANGE (DYDDCR / MOVERANG)
# ============================================================================

def write_range_summary(dyddcr: pl.DataFrame,
                        zdate: int,
                        rdate: str,
                        lines: list[str]) -> None:
    """PROC PRINT DATA=MIS.DYDDCR equivalent, filtered to REPTDATE=ZDATE."""
    df = dyddcr.filter(pl.col("REPTDATE_Z5") == zdate)

    lines.append(f"{ASA_NEWPAGE}REPORT ID: DMMISR02")
    lines.append(f"{ASA_NEWLINE}PUBLIC BANK BERHAD ")
    lines.append(f"{ASA_NEWLINE}PRODUCT DEVELOPMENT & MARKETING")
    lines.append(f"{ASA_NEWLINE}SUMMARY OF MOVEMENT IN BANK'S DEMAND DEPOSIT AS AT {rdate}")
    lines.append(f"{ASA_NEWLINE}(EXCLUDING ACE & AL-WADIAH CURRENT ACCOUNTS)")
    lines.append(f"{ASA_NEWLINE}NET INCREASE/DECREASE BY RANGE")
    lines.append(f"{ASA_NEWLINE}")

    LBL_W  = 35
    DATA_W = 15
    sep    = f"{'-'*LBL_W}-+-{'-'*DATA_W}"
    lines.append(f"{ASA_NEWLINE}{'NET INCREASE/DECREASE':<{LBL_W}} | {'TOTAL MOVEMENT':>{DATA_W}}")
    lines.append(f"{ASA_NEWLINE}{sep}")

    total = 0.0
    if df.height == 0:
        lines.append(f"{ASA_NEWLINE}  (NO RECORDS)")
    else:
        for row in df.sort("RANGE").to_dicts():
            rng      = int(row.get("RANGE") or 0)
            movement = float(row.get("MOVEMENT") or 0.0)
            # Apply MOVERANG label
            label = next((lbl for thresh, lbl in MOVERANG_BUCKETS if rng <= thresh),
                         MOVERANG_BUCKETS[-1][1])
            total += movement
            lines.append(f"{ASA_NEWLINE}{label:<{LBL_W}} | {fmt_comma(movement, DATA_W, 2)}")

    lines.append(f"{ASA_NEWLINE}{sep}")
    lines.append(f"{ASA_NEWLINE}{'TOTAL':<{LBL_W}} | {fmt_comma(total, DATA_W, 2)}")
    lines.append(f"{ASA_NEWLINE}{sep}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("DMMISR02 – Movement in Saving & Demand Deposits starting...")

    ctx   = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    dymvnt  = load_dymvnt(ctx)
    dyddcr  = load_dyddcr(ctx)
    sdmvnt  = build_sdmvnt(dymvnt, ctx["zdate"])
    crmove, crmvsptf = build_crmove(dymvnt, ctx["zdate"])

    lines: list[str] = []

    # Section 1a – Saving Conventional
    write_saving_section(sdmvnt, "CONV", ctx["rdate"], lines)

    # Section 1b – Saving Islamic
    write_saving_section(sdmvnt, "ISLM", ctx["rdate"], lines)

    # Section 2a – Demand Deposits Credit Movement (non-SPTF)
    write_dd_section(
        crmove,
        "CREDIT MOVEMENT IN BANK'S DEMAND DEPOSITS AS AT",
        "NO CUSTOMER WITH CREDIT MOVEMENT OF 1 MILLION AND ABOVE",
        ctx["rdate"], lines,
    )

    # Section 2b – SPTF Demand Deposits
    write_dd_section(
        crmvsptf,
        "SPTF CREDIT MOVEMENT IN BANK'S DEMAND DEPOSITS AS AT",
        "NO CUSTOMER WITH SPTF CREDIT MOVEMENT OF 1 MILLION AND ABOVE",
        ctx["rdate"], lines,
    )

    # Section 3 – Range Summary
    write_range_summary(dyddcr, ctx["zdate"], ctx["rdate"], lines)

    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {REPORT_FILE}")

    print("DMMISR02 – Done.")


if __name__ == "__main__":
    main()
