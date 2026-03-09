#!/usr/bin/env python3
"""
Program  : EIBDTP50.py
Purpose  : Produce reports - Top 50 Depositor (Daily RM & FCY).
           Generates three reports:
             FD11TEXT  - Top 100 Largest FD/CA/SA Individual Customers
             FD12TEXT  - Top 100 Largest FD/CA/SA Corporate Customers
             FD2TEXT   - Group of Companies Under Top 100 Corp Depositors

           Dependency: PBBDPFMT (product mapping and format definitions)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import sys
import logging
from pathlib import Path
from datetime import date
from typing import Optional

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# ============================================================================
from PBBDPFMT import CAProductFormat, SAProductFormat

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path(".")
DATA_DIR   = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input parquet paths
REPTDATE_PATH             = DATA_DIR / "deposit"  / "reptdate.parquet"
DEPOSIT_CURRENT_PATH      = DATA_DIR / "deposit"  / "current.parquet"
DEPOSIT_FD_PATH           = DATA_DIR / "deposit"  / "fd.parquet"
DEPOSIT_SAVING_PATH       = DATA_DIR / "deposit"  / "saving.parquet"
CISDP_DEPOSIT_PATH        = DATA_DIR / "cisdp"    / "deposit.parquet"
CISFD_DEPOSIT_PATH        = DATA_DIR / "cisfd"    / "deposit.parquet"
COF_MNI_DEPOSITOR_PATH    = DATA_DIR / "list"     / "cof_mni_depositor_list.parquet"
KEEP_TOP_DEP_EXCL_PBB_PATH= DATA_DIR / "list"     / "keep_top_dep_excl_pbb.parquet"

# Output report paths (plain-text, ASA carriage control, LRECL=133)
FD11TEXT_PATH = OUTPUT_DIR / "TOP50I.TXT"   # Individual customers
FD12TEXT_PATH = OUTPUT_DIR / "TOP50C.TXT"   # Corporate customers
FD2TEXT_PATH  = OUTPUT_DIR / "TOP50S.TXT"   # Subsidiaries customers

# ============================================================================
# CONSTANTS
# ============================================================================
PAGE_WIDTH   = 133
PAGE_LINES   = 60
LINES_HEADER = 5   # title lines + blank lines before data

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ============================================================================
# REPORT DATE DERIVATION
# Mirrors: DATA REPTDATE; SET DEPOSIT.REPTDATE; SELECT(DAY(REPTDATE));
# ============================================================================
def derive_report_date() -> tuple[str, str, str, str, str]:
    """
    Read REPTDATE from deposit parquet and derive macro variables:
      NOWK     - week number within month (1-4)
      REPTYEAR - 4-digit year
      REPTMON  - 2-digit zero-padded month
      REPTDAY  - 2-digit zero-padded day
      RDATE    - formatted date as DD/MM/YYYY
    Returns (nowk, reptyear, reptmon, reptday, rdate).
    """
    df = pl.read_parquet(REPTDATE_PATH)
    reptdate: date = df["REPTDATE"][0]

    day = reptdate.day
    if   day == 8:  nowk = "1"
    elif day == 15: nowk = "2"
    elif day == 22: nowk = "3"
    else:           nowk = "4"

    reptyear = str(reptdate.year)
    reptmon  = f"{reptdate.month:02d}"
    reptday  = f"{reptdate.day:02d}"
    rdate    = reptdate.strftime("%d/%m/%Y")

    return nowk, reptyear, reptmon, reptday, rdate


# ============================================================================
# FORMAT APPLICATION HELPERS
# ============================================================================
def apply_caprod(product: Optional[int]) -> str:
    return CAProductFormat.format(product)

def apply_saprod(product: Optional[int]) -> str:
    return SAProductFormat.format(product)


# ============================================================================
# DATA PREPARATION
# ============================================================================

def load_cisca() -> pl.DataFrame:
    """
    DATA CISCA: Filter CIS deposit records for CA accounts.
      SECCUST='901', ACCTNO 3000000000–3999999999
      ICNO derived from NEWIC or CUSTNO.
    """
    df = pl.read_parquet(CISDP_DEPOSIT_PATH)
    df = df.filter(
        (pl.col("SECCUST") == "901") &
        (pl.col("ACCTNO").is_between(3_000_000_000, 3_999_999_999))
    )
    df = df.with_columns(
        pl.when(pl.col("NEWIC") != "")
          .then(pl.col("NEWIC"))
          .otherwise(pl.col("CUSTNO"))
          .alias("ICNO")
    )
    return df.select(["CUSTNO", "ACCTNO", "CUSTNAME", "ICNO", "NEWIC", "OLDIC", "INDORG"])


def load_cisfd() -> pl.DataFrame:
    """
    DATA CISFD: Filter CIS deposit records for FD/SA accounts.
      SECCUST='901', ACCTNO in ranges 1xxx, 4xxx–6xxx, 7xxx.
      ICNO derived from NEWIC or CUSTNO.
    """
    df = pl.read_parquet(CISFD_DEPOSIT_PATH)
    df = df.filter(
        (pl.col("SECCUST") == "901") &
        (
            pl.col("ACCTNO").is_between(1_000_000_000, 1_999_999_999) |
            pl.col("ACCTNO").is_between(7_000_000_000, 7_999_999_999) |
            pl.col("ACCTNO").is_between(4_000_000_000, 6_999_999_999)
        )
    )
    df = df.with_columns(
        pl.when(pl.col("NEWIC") != "")
          .then(pl.col("NEWIC"))
          .otherwise(pl.col("CUSTNO"))
          .alias("ICNO")
    )
    return df.select(["CUSTNO", "ACCTNO", "CUSTNAME", "ICNO", "NEWIC", "OLDIC", "INDORG"])


def load_ca() -> pl.DataFrame:
    """
    DATA CA: Current accounts with positive balance and valid PRODCD.
    """
    df = pl.read_parquet(DEPOSIT_CURRENT_PATH)
    df = df.with_columns(
        pl.col("PRODUCT").map_elements(apply_caprod, return_dtype=pl.Utf8).alias("PRODCD")
    )
    return df.filter((pl.col("CURBAL") > 0) & (pl.col("PRODCD") != "N"))


def load_fd() -> pl.DataFrame:
    """
    DATA FD: Fixed deposits with positive balance.
    """
    df = pl.read_parquet(DEPOSIT_FD_PATH)
    return df.filter(pl.col("CURBAL") > 0)


def load_sa() -> pl.DataFrame:
    """
    DATA SA: Savings accounts with positive balance and valid PRODCD.
    """
    df = pl.read_parquet(DEPOSIT_SAVING_PATH)
    df = df.with_columns(
        pl.col("PRODUCT").map_elements(apply_saprod, return_dtype=pl.Utf8).alias("PRODCD")
    )
    return df.filter((pl.col("CURBAL") > 0) & (pl.col("PRODCD") != "N"))


def merge_and_split(base: pl.DataFrame, cis: pl.DataFrame,
                    bal_col: str, ind_custcodes: list[int]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Merge base deposit data with CIS lookup by ACCTNO.
    Resolve CUSTNAME from CIS where blank.
    Split into IND (individual) and ORG (corporate) datasets.
    Mirrors DATA xIND / xORG steps with CUSTCODE and PURPOSE filters.

    Returns (ind_df, org_df)
    """
    # Merge: base LEFT JOIN cis on ACCTNO
    merged = base.join(cis, on="ACCTNO", how="left", suffix="_CIS")

    # Resolve CUSTNAME: if blank use NAME from base
    if "NAME" in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col("CUSTNAME").str.strip_chars() == "")
              .then(pl.col("NAME"))
              .otherwise(pl.col("CUSTNAME"))
              .alias("CUSTNAME")
        )

    merged = merged.with_columns(pl.col("CURBAL").alias(bal_col))

    # IND: CUSTCODE IN (77,78,95,96)
    ind = merged.filter(pl.col("CUSTCODE").is_in(ind_custcodes))

    # ORG: exclude PURPOSE='2', keep INDORG='O'
    org = merged.filter(
        ~pl.col("CUSTCODE").is_in(ind_custcodes) &
        (pl.col("PURPOSE") != "2") &
        (pl.col("INDORG") == "O")
    )

    # For IND: PURPOSE='2' → set ICNO='JOINT', CUSTNAME=NAME
    if "NAME" in ind.columns:
        ind = ind.with_columns(
            pl.when(pl.col("PURPOSE") == "2")
              .then(pl.lit("JOINT"))
              .otherwise(pl.col("ICNO"))
              .alias("ICNO"),
            pl.when(pl.col("PURPOSE") == "2")
              .then(pl.col("NAME"))
              .otherwise(pl.col("CUSTNAME"))
              .alias("CUSTNAME")
        )

    # NODUPKEY: BY ACCTNO ICNO CUSTNAME
    ind = ind.unique(subset=["ACCTNO", "ICNO", "CUSTNAME"], keep="first")

    return ind, org


# ============================================================================
# ASA CARRIAGE CONTROL REPORT WRITER
# ============================================================================

class ReportWriter:
    """
    Writes fixed-width ASA carriage-control reports (LRECL=133).
    ASA codes: ' ' = single space, '1' = new page, '0' = double space, '+' = overprint.
    """

    def __init__(self, path: Path, title1: str, title2: str, page_lines: int = PAGE_LINES):
        self.path       = path
        self.title1     = title1
        self.title2     = title2
        self.page_lines = page_lines
        self._lines: list[str] = []
        self._cur_line  = 0
        self._page      = 0

    def _format_line(self, asa: str, text: str) -> str:
        content = (text or "")[:PAGE_WIDTH - 1]
        return f"{asa}{content:<{PAGE_WIDTH - 1}}\n"

    def new_page(self):
        self._page    += 1
        self._cur_line = 0
        asa = "1" if self._page > 1 else "1"
        self._lines.append(self._format_line(asa, self.title1))
        self._lines.append(self._format_line(" ", self.title2))
        self._lines.append(self._format_line(" ", ""))
        self._cur_line += 3

    def write_line(self, text: str, asa: str = " "):
        if self._cur_line == 0 or self._cur_line >= self.page_lines - 2:
            self.new_page()
        self._lines.append(self._format_line(asa, text))
        self._cur_line += 1

    def write_separator(self, char: str = "-"):
        self.write_line(char * (PAGE_WIDTH - 1))

    def flush(self):
        with self.path.open("w", encoding="utf-8") as f:
            f.writelines(self._lines)
        log.info("Report written: %s (%d pages)", self.path, self._page)


# ============================================================================
# PRNREC MACRO — Summary + Detail print for IND/ORG groups
# ============================================================================

def prnrec(data1: pl.DataFrame, writer: ReportWriter) -> None:
    """
    Mirrors %PRNREC macro:
      1. Filter ICNO non-blank.
      2. Summarise CURBAL/FDBAL/CABAL/SABAL by ICNO+CUSTNAME.
      3. Sort descending CURBAL, keep top 100.
      4. Print summary table.
      5. Merge back for detail rows and print by ICNO+CUSTNAME groups.
    """
    # Ensure balance columns exist
    for col in ("FDBAL", "CABAL", "SABAL"):
        if col not in data1.columns:
            data1 = data1.with_columns(pl.lit(0.0).alias(col))

    # Filter non-blank ICNO
    data1 = data1.filter(pl.col("ICNO").str.strip_chars() != "")

    # Summarise by ICNO + CUSTNAME
    data2 = (
        data1.group_by(["ICNO", "CUSTNAME"])
             .agg([
                 pl.col("CURBAL").sum(),
                 pl.col("FDBAL").sum(),
                 pl.col("CABAL").sum(),
                 pl.col("SABAL").sum(),
             ])
             .sort("CURBAL", descending=True)
             .head(100)
    )

    # ---- Summary Print ----
    hdr = (
        f"{'DEPOSITOR':<40} {'TOTAL BALANCE':>16} {'FD BALANCE':>16} "
        f"{'CA BALANCE':>16} {'SA BALANCE':>16}"
    )
    writer.write_line(hdr)
    writer.write_separator()

    for row in data2.iter_rows(named=True):
        line = (
            f"{str(row['CUSTNAME']):<40} {row['CURBAL']:>16,.2f} {row['FDBAL']:>16,.2f} "
            f"{row['CABAL']:>16,.2f} {row['SABAL']:>16,.2f}"
        )
        writer.write_line(line)

    # Total line
    total_curbal = data2["CURBAL"].sum()
    writer.write_separator()
    writer.write_line(f"{'TOTAL':<40} {total_curbal:>16,.2f}", asa="0")
    writer.write_line("")

    # ---- Detail Print (by ICNO+CUSTNAME, filtered to top-100 set) ----
    top_keys = data2.select(["ICNO", "CUSTNAME"])
    data3 = data1.join(top_keys, on=["ICNO", "CUSTNAME"], how="inner")
    data3 = data3.sort(["ICNO", "CUSTNAME"])

    detail_hdr = (
        f"{'BRANCH CODE':>11} {'MNI NO':>12} {'CUSTCD':>6} {'DEPOSITOR':<30} "
        f"{'CIS NO':>10} {'NEW IC':>15} {'OLD IC':>15} {'CURRENT BALANCE':>16} {'PRODUCT':>7}"
    )

    grp_key = None
    grp_total = 0.0

    for row in data3.iter_rows(named=True):
        cur_key = (row.get("ICNO", ""), row.get("CUSTNAME", ""))
        if cur_key != grp_key:
            if grp_key is not None:
                writer.write_separator("=")
                writer.write_line(
                    f"{'':>11} {'':>12} {'':>6} {'SUBTOTAL':<30} {'':>10} {'':>15} {'':>15} "
                    f"{grp_total:>16,.2f}"
                )
                writer.write_line("")
            writer.write_line(f"  ICNO: {cur_key[0]}   DEPOSITOR: {cur_key[1]}", asa="1")
            writer.write_line(detail_hdr)
            writer.write_separator()
            grp_key   = cur_key
            grp_total = 0.0

        branch   = row.get("BRANCH",   "")
        acctno   = row.get("ACCTNO",   "")
        custcode = row.get("CUSTCODE", "")
        custname = row.get("CUSTNAME", "")
        custno   = row.get("CUSTNO",   "")
        newic    = row.get("NEWIC",    "")
        oldic    = row.get("OLDIC",    "")
        curbal   = row.get("CURBAL",   0.0) or 0.0
        product  = row.get("PRODUCT",  "")
        grp_total += curbal

        line = (
            f"{str(branch):>11} {str(acctno):>12} {str(custcode):>6} {str(custname):<30} "
            f"{str(custno):>10} {str(newic):>15} {str(oldic):>15} {curbal:>16,.2f} {str(product):>7}"
        )
        writer.write_line(line)

    # Final group total
    if grp_key is not None:
        writer.write_separator("=")
        writer.write_line(
            f"{'':>11} {'':>12} {'':>6} {'SUBTOTAL':<30} {'':>10} {'':>15} {'':>15} "
            f"{grp_total:>16,.2f}"
        )


# ============================================================================
# SUBSIDIARIES SECTION
# ============================================================================

def build_subs_all(fdorg: pl.DataFrame, caorg: pl.DataFrame, saorg: pl.DataFrame) -> pl.DataFrame:
    """
    DATA SUBS_ALL: Combine corporate FD/CA/SA, derive ICNO, RMAMT/FCYAMT.
    """
    combined = pl.concat([fdorg, caorg, saorg], how="diagonal")

    # Derive ICNO from NEWIC or OLDIC
    combined = combined.with_columns(
        pl.when(pl.col("NEWIC").str.strip_chars() != "")
          .then(pl.col("NEWIC"))
          .otherwise(pl.col("OLDIC"))
          .alias("ICNO")
    )

    # RMAMT / FCYAMT split by currency
    combined = combined.with_columns([
        pl.when(pl.col("CURCODE") == "MYR")
          .then(pl.col("CURBAL"))
          .otherwise(pl.lit(0.0))
          .alias("RMAMT"),
        pl.when(pl.col("CURCODE") != "MYR")
          .then(pl.col("CURBAL"))
          .otherwise(pl.lit(0.0))
          .alias("FCYAMT"),
        pl.lit(None).cast(pl.Int64).alias("DEPID"),
        pl.lit(None).cast(pl.Utf8).alias("DEPGRP"),
    ])

    return combined


def match_depositor_list(subs_all: pl.DataFrame) -> pl.DataFrame:
    """
    Mirrors the PROC SORT / DATA MNI_IC / MNI_CUST merge steps.
    Matches subs against COF_MNI_DEPOSITOR_LIST by NEWIC (BUSSREG) then CUSTNO.
    Excludes records found in KEEP_TOP_DEP_EXCL_PBB (anti-join).
    Returns final SUBS_ALL with DEPID and DEPGRP populated.
    """
    cof_raw = pl.read_parquet(COF_MNI_DEPOSITOR_PATH)

    # Deduplicate by BUSSREG → COF_MNI_IDNO
    cof_idno = (
        cof_raw.sort("BUSSREG")
               .unique(subset=["BUSSREG"], keep="first")
               .select(["DEPID", "DEPGRP", "BUSSREG"])
    )

    # Deduplicate by CUSTNO → COF_MNI_CUST
    cof_cust = (
        cof_raw.sort("CUSTNO")
               .unique(subset=["CUSTNO"], keep="first")
               .select(["DEPID", "DEPGRP", "CUSTNO"])
    )

    # Sort subs by NEWIC
    subs = subs_all.sort("NEWIC")

    # Merge by NEWIC = BUSSREG
    merged_ic = subs.join(
        cof_idno.rename({"BUSSREG": "NEWIC"}),
        on="NEWIC", how="left", suffix="_COF"
    )
    # Overwrite DEPID/DEPGRP where matched
    merged_ic = merged_ic.with_columns([
        pl.coalesce(["DEPID_COF", "DEPID"]).alias("DEPID"),
        pl.coalesce(["DEPGRP_COF", "DEPGRP"]).alias("DEPGRP"),
    ]).drop(["DEPID_COF", "DEPGRP_COF"])

    mni_ic  = merged_ic.filter(pl.col("DEPID").is_not_null() & (pl.col("DEPID") > 0))
    mni_icx = merged_ic.filter(~(pl.col("DEPID").is_not_null() & (pl.col("DEPID") > 0)))

    # Merge unmatched by CUSTNO
    mni_icx_sorted = mni_icx.sort("CUSTNO")
    merged_cust = mni_icx_sorted.join(cof_cust, on="CUSTNO", how="left", suffix="_COF")
    merged_cust = merged_cust.with_columns([
        pl.coalesce(["DEPID_COF", "DEPID"]).alias("DEPID"),
        pl.coalesce(["DEPGRP_COF", "DEPGRP"]).alias("DEPGRP"),
    ]).drop(["DEPID_COF", "DEPGRP_COF"])

    mni_cust  = merged_cust.filter(pl.col("DEPID").is_not_null() & (pl.col("DEPID") > 0))

    # Combine IC + CUST matched
    mni_all = pl.concat([mni_ic, mni_cust], how="diagonal").sort("CUSTNO")

    # Anti-join with KEEP_TOP_DEP_EXCL_PBB
    topdep = pl.read_parquet(KEEP_TOP_DEP_EXCL_PBB_PATH).sort("CUSTNO")
    result = mni_all.join(topdep.select(["CUSTNO"]), on="CUSTNO", how="anti")

    return result.sort(["CUSTNO", "ACCTNO"])


def print_subsidiaries(subs_all: pl.DataFrame, writer: ReportWriter, rdate: str) -> None:
    """
    Mirrors %PRNSUB macro:
      Iterate over each DEPID group, print accounts grouped by CUSTNO.
    """
    if subs_all.is_empty():
        log.warning("SUBS_ALL is empty — no subsidiary report to print.")
        return

    dep_ids = (
        subs_all.select("DEPID")
                .filter(pl.col("DEPID").is_not_null())
                .unique()
                .sort("DEPID")["DEPID"]
                .to_list()
    )

    detail_hdr = (
        f"{'BRANCH CODE':>11} {'MNI NO':>12} {'DEPOSITOR':<30} "
        f"{'CIS NO':>10} {'CUSTCD':>6} {'CURRENT BALANCE':>16} {'PRODUCT':>7}"
    )

    for dep_id in dep_ids:
        subs = subs_all.filter(pl.col("DEPID") == dep_id)
        if subs.is_empty():
            continue

        group = subs["DEPGRP"][0] or ""
        writer.title1 = "PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTOP5"
        writer.title2 = f"GROUP OF COMPANIES UNDER TOP 100 CORP DEPOSITORS @ {rdate}"
        writer.new_page()
        writer.write_line(f"***** {group} *****")
        writer.write_line("")

        cust_groups = subs.partition_by("CUSTNO", maintain_order=True)
        for grp_df in cust_groups:
            custno    = grp_df["CUSTNO"][0]
            grp_total = 0.0
            writer.write_line(f"  CUSTNO: {custno}")
            writer.write_line(detail_hdr)
            writer.write_separator()

            for row in grp_df.iter_rows(named=True):
                branch   = row.get("BRANCH",   "")
                acctno   = row.get("ACCTNO",   "")
                custname = row.get("CUSTNAME", "")
                custno_r = row.get("CUSTNO",   "")
                custcode = row.get("CUSTCODE", "")
                curbal   = row.get("CURBAL",   0.0) or 0.0
                product  = row.get("PRODUCT",  "")
                grp_total += curbal

                line = (
                    f"{str(branch):>11} {str(acctno):>12} {str(custname):<30} "
                    f"{str(custno_r):>10} {str(custcode):>6} {curbal:>16,.2f} {str(product):>7}"
                )
                writer.write_line(line)

            writer.write_separator("=")
            writer.write_line(
                f"{'':>11} {'':>12} {'SUBTOTAL':<30} {'':>10} {'':>6} {grp_total:>16,.2f}"
            )
            writer.write_line("")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    log.info("EIBDTP50 started.")

    # ----------------------------------------------------------------
    # Derive report date macro variables
    # ----------------------------------------------------------------
    nowk, reptyear, reptmon, reptday, rdate = derive_report_date()
    log.info("Report date: %s (week %s)", rdate, nowk)

    # ----------------------------------------------------------------
    # %INC PGM(PBBDPFMT) — product format functions imported above
    # ----------------------------------------------------------------

    # ----------------------------------------------------------------
    # Load and prepare datasets
    # ----------------------------------------------------------------
    cisca = load_cisca()
    cisfd = load_cisfd()
    ca    = load_ca()
    fd    = load_fd()
    sa    = load_sa()

    ind_custcodes = [77, 78, 95, 96]

    # CA merge + split
    caind, caorg = merge_and_split(ca, cisca, "CABAL", ind_custcodes)

    # FD merge + split
    fdind, fdorg = merge_and_split(fd, cisfd, "FDBAL", ind_custcodes)

    # SA merge + split (uses CISFD as the CIS source)
    saind, saorg = merge_and_split(sa, cisfd, "SABAL", ind_custcodes)

    # ----------------------------------------------------------------
    # FD+CA+SA INDIVIDUAL CUSTOMERS  → FD11TEXT
    # ----------------------------------------------------------------
    data1_ind = pl.concat([fdind, caind, saind], how="diagonal")
    data1_ind = data1_ind.with_columns(
        pl.when(pl.col("ICNO").str.strip_chars() == "")
          .then(pl.lit("XX"))
          .otherwise(pl.col("ICNO"))
          .alias("ICNO")
    )

    writer_ind = ReportWriter(
        FD11TEXT_PATH,
        title1="PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTP50",
        title2=f"TOP 100 LARGEST FD/CA/SA INDIVIDUAL CUSTOMERS AS AT {rdate}",
    )
    prnrec(data1_ind, writer_ind)
    writer_ind.flush()

    # ----------------------------------------------------------------
    # FD+CA+SA CORPORATE CUSTOMERS  → FD12TEXT
    # ----------------------------------------------------------------
    data1_org = pl.concat([fdorg, caorg, saorg], how="diagonal")
    data1_org = data1_org.with_columns(
        pl.when(pl.col("ICNO").str.strip_chars() == "")
          .then(pl.lit("XX"))
          .otherwise(pl.col("ICNO"))
          .alias("ICNO")
    )

    writer_org = ReportWriter(
        FD12TEXT_PATH,
        title1="PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTP50",
        title2=f"TOP 100 LARGEST FD/CA/SA CORPORATE CUSTOMERS AS AT {rdate}",
    )
    prnrec(data1_org, writer_org)
    writer_org.flush()

    # ----------------------------------------------------------------
    # FD/CA/SA SUBSIDIARIES CUSTOMERS  → FD2TEXT
    # ----------------------------------------------------------------
    subs_all = build_subs_all(fdorg, caorg, saorg)
    subs_all = match_depositor_list(subs_all)

    writer_subs = ReportWriter(
        FD2TEXT_PATH,
        title1="PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTOP5",
        title2=f"GROUP OF COMPANIES UNDER TOP 100 CORP DEPOSITORS @ {rdate}",
    )
    print_subsidiaries(subs_all, writer_subs, rdate)
    writer_subs.flush()

    log.info("EIBDTP50 completed.")


if __name__ == "__main__":
    main()
