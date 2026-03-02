#!/usr/bin/env python3
"""
Program : DMMISR22
Function: Movement in Bank's Saving Deposits
          Daily Net Increase/Decrease of RM 50 Thousand & Above per Account (Conventional)
          Report ID : DMMISR22
          Enriches with CIS customer name and address data.
"""

import duckdb
import polars as pl
import os
from datetime import datetime, timedelta
from typing import Optional

# Import format definitions from PBBDPFMT and PBMISFMT
from PBBDPFMT import SAProductFormat
from PBMISFMT import format_brchcd

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = os.environ.get("BASE_DIR",  "/data")
MIS_DIR       = os.path.join(BASE_DIR, "mis")        # SAP.PBB.MIS.D<YEAR>
CIS_DIR       = os.path.join(BASE_DIR, "cis")        # SAP.PBB.CIDATAWH.DAILY
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

REPORT_FILE   = os.path.join(OUTPUT_DIR, "DMMISR22.txt")

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "

PAGE_LINES  = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================================
# PRODUCT EXCLUSION: Islamic SA products (excluded from conventional report)
# ============================================================================

SA_ISLAMIC_PRODUCTS = {204, 207, 214, 215}

# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma(value: Optional[float], width: int = 14, dec: int = 2) -> str:
    if value is None:
        return " " * width
    s = f"{value:,.{dec}f}"
    return s.rjust(width)


def fmt_comma14_1(value: Optional[float], width: int = 14) -> str:
    """COMMA14.1 – right-aligned one decimal."""
    if value is None:
        return " " * width
    s = f"{value:,.1f}"
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
        "zdate"    : int(reptdate.strftime("%j")),       # Z5
        "reptdt6"  : reptdate.strftime("%y%m%d"),        # YYMMDDN6
    }


# ============================================================================
# STEP 2 – LOAD & FILTER MIS.DYMVNT<MM>
# ============================================================================

def _parse_z5(val) -> Optional[int]:
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


def build_sdmvnt(ctx: dict) -> pl.DataFrame:
    """
    Load MIS.DYMVNT<MM>, filter DEPTYPE='S' and REPTDATE=ZDATE.
    Exclude Islamic products.
    Apply SAPROD format, exclude PRODCD='N'.
    Compute MOVEMENT, PREBAL, BRCH.
    Sort by ACCTNO (for CRM merge), then re-sort DESCENDING MOVEMENT / ACCTNO.
    """
    path = os.path.join(MIS_DIR, f"DYMVNT{ctx['reptmon']}.parquet")
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()

    df = df.with_columns(
        pl.col("REPTDATE")
          .map_elements(_parse_z5, return_dtype=pl.Int64)
          .alias("REPTDATE_Z5")
    )

    df = df.filter(
        (pl.col("DEPTYPE") == "S") &
        (pl.col("REPTDATE_Z5") == ctx["zdate"]) &
        (~pl.col("PRODUCT").cast(pl.Int64).is_in(list(SA_ISLAMIC_PRODUCTS)))
    )

    # SAPROD format – exclude 'N'
    def saprod(product) -> str:
        return SAProductFormat.format(int(product) if product is not None else None)

    df = df.with_columns(
        pl.col("PRODUCT")
          .map_elements(saprod, return_dtype=pl.Utf8)
          .alias("PRODCD")
    )
    df = df.filter(pl.col("PRODCD") != "N")

    # Compute MOVEMENT and PREBAL
    df = df.with_columns([
        (pl.col("CREDIT") - pl.col("DEBIT")).round(2).alias("MOVEMENT"),
    ])
    df = df.with_columns(
        (pl.col("CURBAL") - pl.col("MOVEMENT")).alias("PREBAL")
    )

    # BRCH from PBMISFMT
    df = df.with_columns(
        pl.col("BRANCH")
          .map_elements(lambda b: format_brchcd(int(b) if b is not None else None),
                        return_dtype=pl.Utf8)
          .alias("BRCH")
    )

    return df.sort("ACCTNO")


# ============================================================================
# STEP 3 – LOAD CIS.CISR1SA<REPTDT6>
# ============================================================================

def load_cname(ctx: dict) -> pl.DataFrame:
    cis_file = os.path.join(CIS_DIR, f"CISR1SA{ctx['reptdt6']}.parquet")
    if not os.path.exists(cis_file):
        return pl.DataFrame({
            "ACCTNO"  : pl.Series([], dtype=pl.Int64),
            "CUSTNAME": pl.Series([], dtype=pl.Utf8),
            "CUSTNO"  : pl.Series([], dtype=pl.Utf8),
            "MNIADDL1": pl.Series([], dtype=pl.Utf8),
            "MNIADDL2": pl.Series([], dtype=pl.Utf8),
        })

    con = duckdb.connect()
    cname = con.execute(f"""
        SELECT ACCTNO, CUSTNAME, CUSTNO, MNIADDL1, MNIADDL2
        FROM   read_parquet('{cis_file}')
        WHERE  SECCUST = '901'
    """).pl()
    con.close()
    return cname.unique(subset=["ACCTNO"], keep="first").sort("ACCTNO")


# ============================================================================
# STEP 4 – MERGE CRM + ADD COUNT / MOVEMT, SORT
# ============================================================================

def enrich_sdmvnt(sdmvnt: pl.DataFrame, cname: pl.DataFrame) -> pl.DataFrame:
    """
    Left-join CRM (MERGE CNAME SDMVNT(IN=A); BY ACCTNO; IF A).
    Compute MOVEMT = ROUND(MOVEMENT * 0.001, .1).
    Sort DESC MOVEMENT / ACCTNO, add COUNT.
    """
    sdmvnt = sdmvnt.join(cname, on="ACCTNO", how="left")

    sdmvnt = sdmvnt.with_columns(
        ((pl.col("MOVEMENT") * 0.001).round(1)).alias("MOVEMT")
    )

    sdmvnt = sdmvnt.sort(["MOVEMENT", "ACCTNO"], descending=[True, False])
    sdmvnt = sdmvnt.with_row_index(name="COUNT", offset=1)

    return sdmvnt


# ============================================================================
# STEP 5 – WRITE REPORT  (PROC REPORT equivalent)
# ============================================================================

# Column definitions: (name, width)
COL_DEFS = [
    ("COUNT",    10),
    ("BRANCH",    6),
    ("ACCTNO",   10),
    ("BRCH",      6),
    ("MNIADDL1", 40),
    ("MOVEMT",   14),
    ("CUSTNAME", 50),
    ("MNIADDL2", 40),
    ("CUSTNO",    8),
    ("CURBAL",   14),
    ("PREBAL",   14),
    ("PRODUCT",   7),
    ("CUSTCODE",  8),
]

HEADERS = [
    ("NO.",                              ""),
    ("BRANCH CODE",                      ""),
    ("ACCOUNT",                          "NO."),
    ("BRANCH ABBR ",                     ""),
    ("NAME1 OF CUSTOMER (FR. A/C LVL)", ""),
    ("DAILY NET INC./(DEC.)",           "(RM'000)"),
    ("NAME OF CUSTOMER (FR. CIS LVL)", ""),
    ("NAME2 OF CUSTOMER (FR. A/C LVL)",""),
    ("CUSTOMER CIS NO.",                ""),
    ("CURRENT DAY",                     "BALANCE (RM)"),
    ("PREVIOUS DAY",                    "BALANCE (RM)"),
    ("PRODUCT CODE",                    ""),
    ("CUSTOMER CODE",                   ""),
]


def _widths() -> list[int]:
    return [c[1] for c in COL_DEFS]


def _header_lines() -> list[str]:
    widths = _widths()
    h1 = " ".join(HEADERS[i][0].center(widths[i])[:widths[i]]
                  for i in range(len(COL_DEFS)))
    h2 = " ".join(HEADERS[i][1].center(widths[i])[:widths[i]]
                  for i in range(len(COL_DEFS)))
    sep = " ".join("-" * w for w in widths)
    return [f"{ASA_NEWLINE}{h1}", f"{ASA_NEWLINE}{h2}", f"{ASA_NEWLINE}{sep}"]


def _cell(col: str, val, width: int) -> str:
    if col == "COUNT":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "BRANCH":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "ACCTNO":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "BRCH":
        return str(val or "").ljust(width)[:width]
    if col == "MNIADDL1":
        return str(val or "").ljust(width)[:width]
    if col == "MOVEMT":
        return fmt_comma14_1(float(val) if val is not None else None, width)
    if col == "CUSTNAME":
        return str(val or "").ljust(width)[:width]
    if col == "MNIADDL2":
        return str(val or "").ljust(width)[:width]
    if col == "CUSTNO":
        return str(val or "").rjust(width)
    if col in ("CURBAL", "PREBAL"):
        return fmt_comma(float(val) if val is not None else None, width, 2)
    if col == "PRODUCT":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "CUSTCODE":
        return str(val or "").rjust(width)
    return str(val or "").rjust(width)


def write_report(sdmvnt: pl.DataFrame, rdate: str) -> None:
    lines: list[str] = []
    widths = _widths()

    lines.append(f"{ASA_NEWPAGE}REPORT ID: DMMISR22")
    lines.append(f"{ASA_NEWLINE}MOVEMENT IN BANK'S SAVING DEPOSITS AS AT {rdate}")
    lines.append(
        f"{ASA_NEWLINE}DAILY NET INCREASE/DECREASE OF RM 50 THOUSAND & ABOVE PER "
        "ACCOUNT (CONVENTIONAL)"
    )
    lines.append(f"{ASA_NEWLINE}")

    if sdmvnt.height == 0:
        lines.append(f"{ASA_NEWLINE}")
        lines.append(f"{ASA_NEWLINE}")
        lines.append(f"{ASA_NEWLINE}     ****************************************************")
        lines.append(f"{ASA_NEWLINE}     NO CUSTOMER WITH MOVEMENT OF 50 THOUSAND AND ABOVE")
        lines.append(f"{ASA_NEWLINE}     AT {rdate}")
        lines.append(f"{ASA_NEWLINE}     ***************************************************")
        with open(REPORT_FILE, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        print(f"Report written (empty): {REPORT_FILE}")
        return

    lines.extend(_header_lines())
    line_count = 7

    # Running totals for RBREAK AFTER SUMMARIZE
    # (MOVEMT and CURBAL/PREBAL are the numeric summary columns)
    tot_movemt = tot_curbal = tot_prebal = 0.0

    for row in sdmvnt.to_dicts():
        if line_count >= PAGE_LINES:
            lines.extend(_header_lines())
            line_count = 3

        tot_movemt += float(row.get("MOVEMT") or 0.0)
        tot_curbal += float(row.get("CURBAL") or 0.0)
        tot_prebal += float(row.get("PREBAL") or 0.0)

        detail = " ".join(_cell(col, row.get(col), width) for col, width in COL_DEFS)
        lines.append(f"{ASA_NEWLINE}{detail}")
        line_count += 1

    # RBREAK AFTER / PAGE DUL OL SUMMARIZE
    sep_sgl = " ".join("-" * w for w in widths)
    sep_dbl = " ".join("=" * w for w in widths)

    sum_row: dict = {col: None for col, _ in COL_DEFS}
    sum_row["MOVEMT"] = tot_movemt
    sum_row["CURBAL"] = tot_curbal
    sum_row["PREBAL"] = tot_prebal

    summary = " ".join(_cell(col, sum_row.get(col), width) for col, width in COL_DEFS)
    lines.append(f"{ASA_NEWLINE}{sep_sgl}")
    lines.append(f"{ASA_NEWLINE}{sep_dbl}")
    lines.append(f"{ASA_NEWLINE}{summary}")
    lines.append(f"{ASA_NEWLINE}{sep_dbl}")

    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {REPORT_FILE}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("DMMISR22 – Saving Deposits Movement (Conventional) starting...")

    ctx     = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    sdmvnt  = build_sdmvnt(ctx)
    cname   = load_cname(ctx)
    sdmvnt  = enrich_sdmvnt(sdmvnt, cname)

    write_report(sdmvnt, ctx["rdate"])

    print("DMMISR22 – Done.")


if __name__ == "__main__":
    main()
