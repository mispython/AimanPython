#!/usr/bin/env python3
"""
Program : EIIDTOP5.py
Function: Produce Reports - Top 100 Depositors (Daily RM & FCY)
            for Public Islamic Bank Berhad.
          Produces three report sections:
            FD11TEXT – Top 100 Largest FD/CA/SA Individual Customers
            FD12TEXT – Top 100 Largest FD/CA/SA Corporate Customers
            FD2TEXT  – Group of Companies Under Top 100 Corp Depositors
                       (Subsidiaries / MNI-linked Depositors)
"""

import duckdb
import polars as pl
import os
from datetime import datetime
from typing import Optional

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = os.environ.get("BASE_DIR",   "/data")
DEPOSIT_DIR   = os.path.join(BASE_DIR, "deposit")    # DEPOSIT library
CISDP_DIR     = os.path.join(BASE_DIR, "cisdp")      # CISDP library (CISDP.DEPOSIT)
CISFD_DIR     = os.path.join(BASE_DIR, "cisfd")      # CISFD library (CISFD.DEPOSIT)
LIST_DIR      = os.path.join(BASE_DIR, "list")        # LIST library
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Input files
REPTDATE_FILE            = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")
DEPOSIT_CURRENT_FILE     = os.path.join(DEPOSIT_DIR, "CURRENT.parquet")    # DEPOSIT.CURRENT
DEPOSIT_FD_FILE          = os.path.join(DEPOSIT_DIR, "FD.parquet")         # DEPOSIT.FD
DEPOSIT_SAVING_FILE      = os.path.join(DEPOSIT_DIR, "SAVING.parquet")     # DEPOSIT.SAVING
CISDP_DEPOSIT_FILE       = os.path.join(CISDP_DIR,   "DEPOSIT.parquet")    # CISDP.DEPOSIT  (CA)
CISFD_DEPOSIT_FILE       = os.path.join(CISFD_DIR,   "DEPOSIT.parquet")    # CISFD.DEPOSIT  (FD/SA)
MNI_DEPOSITOR_LIST_FILE  = os.path.join(LIST_DIR,
                            "ICOF_MNI_DEPOSITOR_LIST.parquet")              # LIST.ICOF_MNI_DEPOSITOR_LIST
KEEP_TOP_DEP_EXCL_FILE   = os.path.join(LIST_DIR,
                            "KEEP_TOP_DEP_EXCL_PIBB.parquet")               # LIST.KEEP_TOP_DEP_EXCL_PIBB

# Output report files
FD11TEXT_FILE = os.path.join(OUTPUT_DIR, "FD11TEXT.txt")   # Individual customers
FD12TEXT_FILE = os.path.join(OUTPUT_DIR, "FD12TEXT.txt")   # Corporate customers
FD2TEXT_FILE  = os.path.join(OUTPUT_DIR, "FD2TEXT.txt")    # Subsidiaries

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "

# Individual CUSTCODE set
INDIVIDUAL_CUSTCODES = {77, 78, 95, 96}

# CA account range
CA_ACCTNO_LO = 3_000_000_000
CA_ACCTNO_HI = 3_999_999_999

# FD/SA account ranges
FDSA_ACCTNO_RANGES = [
    (1_000_000_000, 1_999_999_999),
    (7_000_000_000, 7_999_999_999),
    (4_000_000_000, 6_999_999_999),
]

PAGE_LINES = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma16_2(value: Optional[float], width: int = 16) -> str:
    """COMMA16.2 right-aligned."""
    if value is None or value != value:  # also catches NaN
        return " " * width
    s = f"{value:,.2f}"
    return s.rjust(width)


def _parse_reptdate(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, (int, float)):
        return datetime.strptime(str(int(val)), "%Y%m%d")
    return datetime.strptime(str(val)[:10], "%Y-%m-%d")


# ============================================================================
# STEP 1 – DERIVE REPORT DATE
# ============================================================================

def derive_report_date() -> dict:
    """
    Read DEPOSIT.REPTDATE.
    Derive NOWK (week indicator), REPTYEAR, REPTMON, REPTDAY, RDATE.
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{REPTDATE_FILE}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError("REPTDATE file is empty.")

    reptdate = _parse_reptdate(row[0])
    day      = reptdate.day

    # SELECT(DAY(REPTDATE)) WHEN 8→'1' WHEN 15→'2' WHEN 22→'3' OTHERWISE→'4'
    if   day == 8:  nowk = "1"
    elif day == 15: nowk = "2"
    elif day == 22: nowk = "3"
    else:           nowk = "4"

    return {
        "reptdate" : reptdate,
        "nowk"     : nowk,
        "reptyear" : reptdate.strftime("%Y"),
        "reptmon"  : reptdate.strftime("%m"),
        "reptday"  : reptdate.strftime("%d"),
        "rdate"    : reptdate.strftime("%d/%m/%Y"),
    }


# ============================================================================
# STEP 2 – LOAD CIS DATA (CISCA / CISFD)
# ============================================================================

def _load_cis(path: str, acct_ranges: list[tuple[int, int]]) -> pl.DataFrame:
    """
    Load a CIS DEPOSIT parquet, filter by account ranges and SECCUST='901'.
    Derive ICNO = NEWIC if not blank, else OLDIC.
    Keep: CUSTNO ACCTNO CUSTNAME ICNO NEWIC OLDIC INDORG.
    """
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()

    # Build account range filter
    mask = pl.lit(False)
    for lo, hi in acct_ranges:
        mask = mask | ((pl.col("ACCTNO") >= lo) & (pl.col("ACCTNO") <= hi))
    df = df.filter(mask & (pl.col("SECCUST") == "901"))

    # ICNO = NEWIC if not blank else OLDIC
    df = df.with_columns(
        pl.when(pl.col("NEWIC").is_not_null() & (pl.col("NEWIC").str.strip_chars() != ""))
          .then(pl.col("NEWIC"))
          .otherwise(pl.col("OLDIC"))
          .alias("ICNO")
    )

    keep = [c for c in ["CUSTNO", "ACCTNO", "CUSTNAME", "ICNO", "NEWIC", "OLDIC", "INDORG"]
            if c in df.columns]
    return df.select(keep)


def load_cisca() -> pl.DataFrame:
    return _load_cis(CISDP_DEPOSIT_FILE,
                     [(CA_ACCTNO_LO, CA_ACCTNO_HI)])


def load_cisfd() -> pl.DataFrame:
    return _load_cis(CISFD_DEPOSIT_FILE, FDSA_ACCTNO_RANGES)


# ============================================================================
# STEP 3 – LOAD DEPOSIT BALANCES (CA / FD / SA)
# ============================================================================

def _load_deposit(path: str, bal_alias: str) -> pl.DataFrame:
    """Load a deposit file, filter CURBAL > 0, alias CURBAL to bal_alias."""
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    df  = df.filter(pl.col("CURBAL") > 0)
    return df.with_columns(pl.col("CURBAL").alias(bal_alias))


# ============================================================================
# STEP 4 – MERGE CIS + DEPOSIT AND SPLIT IND / ORG
# ============================================================================

def _merge_split(deposit_df: pl.DataFrame,
                 cis_df:     pl.DataFrame,
                 bal_col:    str,
                 in_left:    bool = True
                 ) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Merge deposit(IN=A) with CIS by ACCTNO.
    Keep rows where A (deposit side present).
    Exclude PURPOSE='2' and blank CUSTNAME.
    Split into IND (CUSTCODE in {77,78,95,96}) and ORG (INDORG='O').
    """
    if in_left:
        # MERGE CA(IN=A) CISCA → left join, deposit is base
        merged = deposit_df.join(cis_df, on="ACCTNO", how="left", suffix="_CIS")
    else:
        # MERGE CISFD FD(IN=A) → left join, deposit is base (but outer to keep CIS cols)
        merged = deposit_df.join(cis_df, on="ACCTNO", how="left", suffix="_CIS")

    # Resolve duplicate columns (CIS preferred for CIS fields)
    for col in ("CUSTNO", "CUSTNAME", "ICNO", "NEWIC", "OLDIC", "INDORG", "CUSTCODE"):
        col_cis = f"{col}_CIS"
        if col_cis in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(col).is_null())
                  .then(pl.col(col_cis))
                  .otherwise(pl.col(col))
                  .alias(col)
            ).drop(col_cis)

    # IF A AND PURPOSE NE '2'
    if "PURPOSE" in merged.columns:
        merged = merged.filter(pl.col("PURPOSE") != "2")

    # IF CUSTNAME='  ' THEN DELETE
    merged = merged.filter(
        pl.col("CUSTNAME").is_not_null() &
        (pl.col("CUSTNAME").str.strip_chars() != "")
    )

    # Ensure bal_col exists from the alias step
    if bal_col not in merged.columns and "CURBAL" in merged.columns:
        merged = merged.with_columns(pl.col("CURBAL").alias(bal_col))

    # Fill missing balance columns with 0
    for bc in ("FDBAL", "CABAL", "SABAL"):
        if bc not in merged.columns:
            merged = merged.with_columns(pl.lit(0.0).alias(bc))

    ind = merged.filter(
        pl.col("CUSTCODE").cast(pl.Int64, strict=False).is_in(list(INDIVIDUAL_CUSTCODES))
    )
    org = merged.filter(
        ~pl.col("CUSTCODE").cast(pl.Int64, strict=False).is_in(list(INDIVIDUAL_CUSTCODES)) &
        (pl.col("INDORG") == "O")
    )

    return ind, org


# ============================================================================
# STEP 5 – %MACRO PRNREC EQUIVALENT
# ============================================================================

def _prnrec(data1: pl.DataFrame,
            title1: str,
            title2: str,
            output_file: str) -> pl.DataFrame:
    """
    Equivalent of %PRNREC macro:
      1. Filter ICNO not blank, sort by ICNO/CUSTNAME/CUSTCODE.
      2. PROC SUMMARY: sum CURBAL/FDBAL/CABAL/SABAL by ICNO/CUSTNAME/CUSTCODE.
      3. Sort DESC CURBAL, keep top 100 (OBS=100).
      4. Write summary PROC PRINT (DATA2).
      5. Inner join back to DATA1 to get detail rows (DATA3).
      6. Write detail PROC PRINT grouped by ICNO/CUSTNAME.
    Returns DATA2 (summary top-100 frame) for downstream use.
    """
    lines: list[str] = []

    # ── Filter ICNO not blank ────────────────────────────────────────────────
    data1 = data1.filter(
        pl.col("ICNO").is_not_null() & (pl.col("ICNO").str.strip_chars() != "")
    ).sort(["ICNO", "CUSTNAME", "CUSTCODE"])

    # ── PROC SUMMARY: sum balances by key ────────────────────────────────────
    agg_cols = [c for c in ("CURBAL", "FDBAL", "CABAL", "SABAL") if c in data1.columns]
    for c in ("FDBAL", "CABAL", "SABAL"):
        if c not in data1.columns:
            data1 = data1.with_columns(pl.lit(0.0).alias(c))

    data2 = (
        data1.group_by(["ICNO", "CUSTNAME", "CUSTCODE"])
             .agg([pl.col(c).sum() for c in ("CURBAL", "FDBAL", "CABAL", "SABAL")])
             .sort("CURBAL", descending=True)
             .head(100)
    )

    # ── Write summary report (DATA2 / PROC PRINT) ───────────────────────────
    _write_summary_report(data2, title1, title2, lines)

    # ── Inner join DATA1 × DATA2 to get per-account detail (DATA3) ───────────
    key_cols   = ["ICNO", "CUSTNAME", "CUSTCODE"]
    data2_keys = data2.select(key_cols).unique()
    data3      = data1.join(data2_keys, on=key_cols, how="inner")\
                      .sort(["ICNO", "CUSTNAME", "CUSTCODE"])

    # ── Write detail report (DATA3 / PROC PRINT BY ICNO CUSTNAME) ───────────
    _write_detail_report(data3, lines)

    with open(output_file, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {output_file}")

    return data2


# ============================================================================
# REPORT WRITERS
# ============================================================================

# --- Summary report (DATA2) ---

SUM_COLS  = [("CUSTNAME",25), ("CURBAL",16), ("FDBAL",16), ("CABAL",16), ("SABAL",16)]
SUM_HDRS  = [
    ("DEPOSITOR",   ""),
    ("TOTAL BALANCE",""),
    ("FD BALANCE",  ""),
    ("CA BALANCE",  ""),
    ("SA BALANCE",  ""),
]


def _write_summary_report(data2: pl.DataFrame,
                           title1: str,
                           title2: str,
                           lines: list[str]) -> None:
    widths = [c[1] for c in SUM_COLS]
    h1  = " ".join(SUM_HDRS[i][0].center(widths[i])[:widths[i]] for i in range(len(SUM_COLS)))
    h2  = " ".join(SUM_HDRS[i][1].center(widths[i])[:widths[i]] for i in range(len(SUM_COLS)))
    sep = " ".join("-" * w for w in widths)
    dbl = " ".join("=" * w for w in widths)

    lines.append(f"{ASA_NEWPAGE}{title1}")
    lines.append(f"{ASA_NEWLINE}{title2}")
    lines.append(f"{ASA_NEWLINE}")
    lines.append(f"{ASA_NEWLINE}{h1}")
    lines.append(f"{ASA_NEWLINE}{h2}")
    lines.append(f"{ASA_NEWLINE}{sep}")

    line_count = 6
    for row in data2.to_dicts():
        if line_count >= PAGE_LINES:
            lines.append(f"{ASA_NEWPAGE}{h1}")
            lines.append(f"{ASA_NEWLINE}{h2}")
            lines.append(f"{ASA_NEWLINE}{sep}")
            line_count = 3
        cells = [
            str(row.get("CUSTNAME") or "").ljust(25)[:25],
            fmt_comma16_2(row.get("CURBAL")),
            fmt_comma16_2(row.get("FDBAL")),
            fmt_comma16_2(row.get("CABAL")),
            fmt_comma16_2(row.get("SABAL")),
        ]
        lines.append(f"{ASA_NEWLINE}{' '.join(cells)}")
        line_count += 1

    lines.append(f"{ASA_NEWLINE}{sep}")
    lines.append(f"{ASA_NEWLINE}{dbl}")


# --- Detail report (DATA3) ---

# BRANCH ACCTNO CUSTCODE CUSTNAME CUSTNO NEWIC OLDIC CURBAL PRODUCT
DET_COLS = [
    ("BRANCH",   6),
    ("ACCTNO",  20),
    ("CUSTCODE", 6),
    ("CUSTNAME",25),
    ("CUSTNO",   8),
    ("NEWIC",   14),
    ("OLDIC",   14),
    ("CURBAL",  16),
    ("PRODUCT",  7),
]
DET_HDRS = [
    ("BRANCH",    "CODE"),
    ("MNI NO",    ""),
    ("CUSTCD",    ""),
    ("DEPOSITOR", ""),
    ("CIS NO",    ""),
    ("NEW IC",    ""),
    ("OLD IC",    ""),
    ("CURRENT",   "BALANCE"),
    ("PRODUCT",   ""),
]


def _det_header() -> list[str]:
    widths = [c[1] for c in DET_COLS]
    h1  = " ".join(DET_HDRS[i][0].center(widths[i])[:widths[i]] for i in range(len(DET_COLS)))
    h2  = " ".join(DET_HDRS[i][1].center(widths[i])[:widths[i]] for i in range(len(DET_COLS)))
    sep = " ".join("-" * w for w in widths)
    return [f"{ASA_NEWLINE}{h1}", f"{ASA_NEWLINE}{h2}", f"{ASA_NEWLINE}{sep}"]


def _det_cell(col: str, val, width: int) -> str:
    if col == "BRANCH":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "ACCTNO":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "CUSTCODE":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "CUSTNAME":
        return str(val or "").ljust(width)[:width]
    if col == "CUSTNO":
        return str(val or "").rjust(width)
    if col in ("NEWIC", "OLDIC"):
        return str(val or "").ljust(width)[:width]
    if col == "CURBAL":
        return fmt_comma16_2(float(val) if val is not None else None, width)
    if col == "PRODUCT":
        return str(int(val) if val is not None else "").rjust(width)
    return str(val or "").rjust(width)


def _write_detail_report(data3: pl.DataFrame, lines: list[str]) -> None:
    """
    PROC PRINT DATA3 with BY ICNO CUSTNAME:
    Print a by-group header, then all accounts in group, then SUM CURBAL.
    """
    widths     = [c[1] for c in DET_COLS]
    sep        = " ".join("-" * w for w in widths)
    dbl        = " ".join("=" * w for w in widths)
    curbal_idx = next(i for i, (c, _) in enumerate(DET_COLS) if c == "CURBAL")

    groups = data3.partition_by(["ICNO", "CUSTNAME"], maintain_order=True)
    first_group = True

    for grp in groups:
        if grp.height == 0:
            continue
        grp_row   = grp.row(0, named=True)
        icno      = grp_row.get("ICNO", "")
        custname  = grp_row.get("CUSTNAME", "")

        # BY group break header
        if first_group:
            lines.extend(_det_header())
            first_group = False
        lines.append(f"{ASA_NEWLINE}--- ICNO: {icno}  DEPOSITOR: {custname} ---")
        lines.append(f"{ASA_NEWLINE}{sep}")

        grp_total = 0.0
        for row in grp.to_dicts():
            cells = [_det_cell(col, row.get(col), width) for col, width in DET_COLS]
            lines.append(f"{ASA_NEWLINE}{' '.join(cells)}")
            grp_total += float(row.get("CURBAL") or 0.0)

        # SUM CURBAL per BY group
        blank_w = sum(w for _, w in DET_COLS[:curbal_idx]) + curbal_idx
        sum_row = f"{' ' * blank_w} {fmt_comma16_2(grp_total, widths[curbal_idx])}"
        lines.append(f"{ASA_NEWLINE}{sep}")
        lines.append(f"{ASA_NEWLINE}{sum_row}")
        lines.append(f"{ASA_NEWLINE}{dbl}")


# ============================================================================
# SUBSIDIARIES SECTION  (FD2TEXT – %MACRO PRNSUB)
# ============================================================================

def build_subs_all(fdorg: pl.DataFrame,
                   caorg: pl.DataFrame,
                   saorg: pl.DataFrame) -> pl.DataFrame:
    """
    DATA SUBS_ALL – combine FDORG CAORG SAORG corporate frames.
    Re-derive ICNO, split CURBAL into RMAMT / FCYAMT by CURCODE.
    """
    subs_all = pl.concat(
        [df for df in (fdorg, caorg, saorg) if df.height > 0],
        how="diagonal"
    )

    # Re-derive ICNO
    subs_all = subs_all.with_columns(
        pl.when(
            pl.col("NEWIC").is_not_null() & (pl.col("NEWIC").str.strip_chars() != "")
        )
        .then(pl.col("NEWIC"))
        .otherwise(pl.col("OLDIC"))
        .alias("ICNO")
    )

    # RMAMT / FCYAMT
    if "CURCODE" in subs_all.columns:
        subs_all = subs_all.with_columns([
            pl.when(pl.col("CURCODE") == "MYR")
              .then(pl.col("CURBAL"))
              .otherwise(pl.lit(None))
              .alias("RMAMT"),
            pl.when(pl.col("CURCODE") != "MYR")
              .then(pl.col("CURBAL"))
              .otherwise(pl.lit(None))
              .alias("FCYAMT"),
        ])
    else:
        subs_all = subs_all.with_columns([
            pl.col("CURBAL").alias("RMAMT"),
            pl.lit(None).cast(pl.Float64).alias("FCYAMT"),
        ])

    # Ensure DEPID / DEPGRP columns exist (filled after MNI merge)
    if "DEPID"  not in subs_all.columns:
        subs_all = subs_all.with_columns(pl.lit(None).cast(pl.Int64).alias("DEPID"))
    if "DEPGRP" not in subs_all.columns:
        subs_all = subs_all.with_columns(pl.lit(None).cast(pl.Utf8).alias("DEPGRP"))

    return subs_all


def load_cof_mni_by_bussreg() -> pl.DataFrame:
    """
    LIST.ICOF_MNI_DEPOSITOR_LIST deduplicated by BUSSREG.
    Keeps DEPID DEPGRP BUSSREG.
    """
    if not os.path.exists(MNI_DEPOSITOR_LIST_FILE):
        return pl.DataFrame({
            "BUSSREG": pl.Series([], dtype=pl.Utf8),
            "DEPID"  : pl.Series([], dtype=pl.Int64),
            "DEPGRP" : pl.Series([], dtype=pl.Utf8),
        })
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT DEPID, DEPGRP, BUSSREG FROM read_parquet('{MNI_DEPOSITOR_LIST_FILE}')"
    ).pl()
    con.close()
    return df.unique(subset=["BUSSREG"], keep="first").select(["DEPID", "DEPGRP", "BUSSREG"])


def load_cof_mni_by_custno() -> pl.DataFrame:
    """
    LIST.ICOF_MNI_DEPOSITOR_LIST deduplicated by CUSTNO.
    Keeps DEPID DEPGRP CUSTNO.
    """
    if not os.path.exists(MNI_DEPOSITOR_LIST_FILE):
        return pl.DataFrame({
            "CUSTNO": pl.Series([], dtype=pl.Int64),
            "DEPID" : pl.Series([], dtype=pl.Int64),
            "DEPGRP": pl.Series([], dtype=pl.Utf8),
        })
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT DEPID, DEPGRP, CUSTNO FROM read_parquet('{MNI_DEPOSITOR_LIST_FILE}')"
    ).pl()
    con.close()
    return df.unique(subset=["CUSTNO"], keep="first").select(["DEPID", "DEPGRP", "CUSTNO"])


def load_keep_top_dep_excl() -> pl.DataFrame:
    """LIST.KEEP_TOP_DEP_EXCL_PIBB – exclusion list by CUSTNO."""
    if not os.path.exists(KEEP_TOP_DEP_EXCL_FILE):
        return pl.DataFrame({"CUSTNO": pl.Series([], dtype=pl.Int64)})
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{KEEP_TOP_DEP_EXCL_FILE}')").pl()
    con.close()
    return df.sort("CUSTNO")


def build_subs_linked(subs_all: pl.DataFrame) -> pl.DataFrame:
    """
    Two-pass MNI matching (by NEWIC/BUSSREG then by CUSTNO),
    apply exclusion list, and combine into final SUBS_ALL for subsidiary report.

    Pass 1: merge SUBS_ALL (sorted by NEWIC) with COF_MNI_IDNO (by BUSSREG=NEWIC)
            → MNI_IC (DEPID > 0), MNI_ICX (DEPID = 0 / null)
    Pass 2: merge MNI_ICX (sorted by CUSTNO) with COF_MNI_CUST (by CUSTNO)
            → MNI_CUST (DEPID > 0), MNI_CUSTX (dropped)
    Combine MNI_IC + MNI_CUST → MNI_ALL
    Exclude rows present in KEEP_TOP_DEP_EXCL_PIBB.
    """
    cof_mni_ic   = load_cof_mni_by_bussreg()   # keyed by BUSSREG
    cof_mni_cust = load_cof_mni_by_custno()     # keyed by CUSTNO
    topdep       = load_keep_top_dep_excl()

    # Pass 1 – match by NEWIC = BUSSREG
    subs_sorted_ic = subs_all.sort("NEWIC")
    merged1 = subs_sorted_ic.join(
        cof_mni_ic.rename({"BUSSREG": "NEWIC"}),
        on="NEWIC", how="left", suffix="_MNI"
    )
    # Resolve DEPID/DEPGRP from merge
    for col in ("DEPID", "DEPGRP"):
        col_mni = f"{col}_MNI"
        if col_mni in merged1.columns:
            merged1 = merged1.with_columns(
                pl.when(
                    pl.col(col).is_null() |
                    (pl.col(col).cast(pl.Utf8) == "null")
                )
                .then(pl.col(col_mni))
                .otherwise(pl.col(col))
                .alias(col)
            ).drop(col_mni)

    mni_ic  = merged1.filter(
        pl.col("DEPID").is_not_null() & (pl.col("DEPID").cast(pl.Int64) > 0)
    )
    mni_icx = merged1.filter(
        pl.col("DEPID").is_null() | (pl.col("DEPID").cast(pl.Int64) <= 0)
    ).drop([c for c in ("DEPID", "DEPGRP") if c in merged1.columns])

    # Pass 2 – match unresolved rows by CUSTNO
    mni_icx_sorted = mni_icx.sort("CUSTNO")
    merged2 = mni_icx_sorted.join(
        cof_mni_cust, on="CUSTNO", how="left", suffix="_MNI"
    )
    for col in ("DEPID", "DEPGRP"):
        col_mni = f"{col}_MNI"
        if col_mni in merged2.columns:
            merged2 = merged2.rename({col_mni: col})

    mni_cust = merged2.filter(
        pl.col("DEPID").is_not_null() & (pl.col("DEPID").cast(pl.Int64) > 0)
    )

    # Combine MNI_IC + MNI_CUST
    mni_all = pl.concat([mni_ic, mni_cust], how="diagonal").sort("CUSTNO")

    # Exclude rows in KEEP_TOP_DEP_EXCL_PIBB (anti-join)
    if topdep.height > 0:
        mni_all = mni_all.join(topdep.select("CUSTNO"), on="CUSTNO", how="anti")

    return mni_all.sort(["CUSTNO", "ACCTNO"])


def write_subs_report(subs_all: pl.DataFrame, rdate: str) -> None:
    """
    %MACRO PRNSUB equivalent:
    For each DEPID (from MIN to MAX), print a page for the group.
    """
    lines: list[str] = []

    if subs_all.height == 0:
        lines.append(f"{ASA_NEWPAGE}PUBLIC ISLAMIC BANK BERHAD        PROGRAM-ID: EIIDTOP5")
        lines.append(f"{ASA_NEWLINE}(NO SUBSIDIARY DATA AVAILABLE)")
        with open(FD2TEXT_FILE, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        print(f"Report written (empty): {FD2TEXT_FILE}")
        return

    # Determine min/max DEPID
    depid_col = subs_all.get_column("DEPID").cast(pl.Int64, strict=False)
    min_id    = int(depid_col.min())
    max_id    = int(depid_col.max())

    widths     = [c[1] for c in DET_COLS]
    sep        = " ".join("-" * w for w in widths)
    dbl        = " ".join("=" * w for w in widths)
    curbal_idx = next(i for i, (c, _) in enumerate(DET_COLS) if c == "CURBAL")

    for depid in range(min_id, max_id + 1):
        subs = subs_all.filter(pl.col("DEPID").cast(pl.Int64, strict=False) == depid)
        if subs.height == 0:
            continue

        group = str(subs.row(0, named=True).get("DEPGRP") or "")

        lines.append(f"{ASA_NEWPAGE}PUBLIC ISLAMIC BANK BERHAD        PROGRAM-ID: EIIDTOP5")
        lines.append(
            f"{ASA_NEWLINE}GROUP OF COMPANIES UNDER TOP 100 CORP DEPOSITORS @ {rdate}"
        )
        lines.append(f"{ASA_NEWLINE}***** {group} *****")
        lines.append(f"{ASA_NEWLINE}")
        lines.extend(_det_header())

        # Print BY CUSTNO groups within this DEPID
        custno_groups = subs.partition_by(["CUSTNO"], maintain_order=True)
        for cgrp in custno_groups:
            if cgrp.height == 0:
                continue
            grp_total = 0.0
            for row in cgrp.to_dicts():
                cells = [_det_cell(col, row.get(col), width) for col, width in DET_COLS]
                lines.append(f"{ASA_NEWLINE}{' '.join(cells)}")
                grp_total += float(row.get("CURBAL") or 0.0)

            # SUM CURBAL per CUSTNO
            blank_w = sum(w for _, w in DET_COLS[:curbal_idx]) + curbal_idx
            sum_row = f"{' ' * blank_w} {fmt_comma16_2(grp_total, widths[curbal_idx])}"
            lines.append(f"{ASA_NEWLINE}{sep}")
            lines.append(f"{ASA_NEWLINE}{sum_row}")
            lines.append(f"{ASA_NEWLINE}{dbl}")

    with open(FD2TEXT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {FD2TEXT_FILE}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("EIIDTOP5 – Top 100 Depositors starting...")

    ctx = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    # Load CIS data
    cisca = load_cisca()
    cisfd = load_cisfd()

    # Load deposit balances
    ca_raw = _load_deposit(DEPOSIT_CURRENT_FILE, "CABAL")
    fd_raw = _load_deposit(DEPOSIT_FD_FILE,      "FDBAL")
    sa_raw = _load_deposit(DEPOSIT_SAVING_FILE,  "SABAL")

    # Merge + split
    caind, caorg = _merge_split(ca_raw, cisca, "CABAL", in_left=True)
    fdind, fdorg = _merge_split(fd_raw, cisfd, "FDBAL", in_left=False)
    saind, saorg = _merge_split(sa_raw, cisfd, "SABAL", in_left=False)

    # Set CURBAL correctly for combined frames
    # CURBAL used in PRNREC is the deposit balance
    for df in (caind, caorg, fdind, fdorg, saind, saorg):
        if "CURBAL" not in df.columns:
            # Locate first available balance column as CURBAL proxy
            for bc in ("CABAL", "FDBAL", "SABAL"):
                if bc in df.columns:
                    df = df.with_columns(pl.col(bc).alias("CURBAL"))
                    break

    # ── FD11TEXT – Top 100 Individual Customers ──────────────────────────────
    data1_ind = pl.concat(
        [df for df in (fdind, caind, saind) if df.height > 0], how="diagonal"
    )
    # IF ICNO='  ' THEN ICNO='XX'
    if "ICNO" in data1_ind.columns:
        data1_ind = data1_ind.with_columns(
            pl.when(
                pl.col("ICNO").is_null() | (pl.col("ICNO").str.strip_chars() == "")
            )
            .then(pl.lit("XX"))
            .otherwise(pl.col("ICNO"))
            .alias("ICNO")
        )

    print("  Writing FD11TEXT – Top 100 Individual Customers...")
    _prnrec(
        data1_ind,
        title1 = "PUBLIC ISLAMIC BANK BERHAD",
        title2 = f"TOP 100 LARGEST FD/CA/SA INDIVIDUAL CUSTOMERS AS AT {ctx['rdate']}",
        output_file = FD11TEXT_FILE,
    )

    # ── FD12TEXT – Top 100 Corporate Customers ───────────────────────────────
    data1_org = pl.concat(
        [df for df in (fdorg, caorg, saorg) if df.height > 0], how="diagonal"
    )
    if "ICNO" in data1_org.columns:
        data1_org = data1_org.with_columns(
            pl.when(
                pl.col("ICNO").is_null() | (pl.col("ICNO").str.strip_chars() == "")
            )
            .then(pl.lit("XX"))
            .otherwise(pl.col("ICNO"))
            .alias("ICNO")
        )

    print("  Writing FD12TEXT – Top 100 Corporate Customers...")
    _prnrec(
        data1_org,
        title1 = "PUBLIC ISLAMIC BANK BERHAD",
        title2 = f"TOP 100 LARGEST FD/CA/SA CORPORATE CUSTOMERS AS AT {ctx['rdate']}",
        output_file = FD12TEXT_FILE,
    )

    # ── FD2TEXT – Subsidiaries (MNI-linked Corporate Depositors) ─────────────
    print("  Building Subsidiaries dataset...")
    subs_all = build_subs_all(fdorg, caorg, saorg)
    subs_all = build_subs_linked(subs_all)

    print("  Writing FD2TEXT – Subsidiaries by Group...")
    write_subs_report(subs_all, ctx["rdate"])

    print("EIIDTOP5 – Done.")


if __name__ == "__main__":
    main()
