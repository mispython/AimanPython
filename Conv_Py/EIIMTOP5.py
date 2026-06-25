#!/usr/bin/env python3
"""
Program : EIIMTOP5
Purpose : Generate Top 50 FD+CA Individual and Corporate depositors, and
          PB Subsidiaries report for Public Islamic Bank Berhad (PIBB),
          using .sas7bdat inputs and text report outputs with ASA
          carriage-control characters.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

import pandas as pd
import polars as pl

from REPTDATE import get_reptdate_values
from input_date import get_latest_file

# =============================================================================
# PATH SETUP
# =============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIMTOP5")
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input .sas7bdat files resolved at runtime via get_latest_file()
# Filename pattern: prefix + MMWNN  (MM=month, W=week digit, NN=year, 5 chars total)
# e.g. ifd06326, ica06326, cisr1ca06326, cisr1fd06326
IFD_PREFIX    = "ifd"       # PIBB FD deposit data
ICA_PREFIX    = "ica"       # PIBB CA deposit data
CISR1CA_PREFIX = "cisr1ca"  # CISCA customer info
CISR1FD_PREFIX = "cisr1fd"  # CISFD customer info

# Output text files (ASA carriage-control, fixed LRECL=320)
FD11TEXT_OUT = OUTPUT_DIR / "SAP.PIBB.INDTOP50.TEXT.txt"   # Individual top 50
FD12TEXT_OUT = OUTPUT_DIR / "SAP.PIBB.CORTOP50.TEXT.txt"   # Corporate top 50
FDSTEXT_OUT  = OUTPUT_DIR / "SAP.PIBB.SUBTOP50.TEXT.txt"   # PB Subsidiaries

# =============================================================================
# CONSTANTS
# =============================================================================
PAGE_LENGTH = 60          # OPTIONS PS=60 (default)
LINE_SIZE   = 132         # OPTIONS LS=132 (default)

# OPTIONS NOCENTER NODATE NONUMBER MISSING=0 (handled in report writer)

# PRODUCT exclusion lists
CA_EXCL_PRODUCTS = {400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410}
FD_EXCL_PRODUCTS = {350, 351, 352, 353, 354, 355, 356, 357}

# PB Subsidiaries CUSTNO filter
# DATA DATA1; IF CUSTNO IN (53227,169990,170108,3562038,3721354);
SUBS_CUSTNOS = {53227, 169990, 170108, 3562038, 3721354}

# ACCTNO range definitions
CA_RANGE   = (3_000_000_000, 3_999_999_999)
FD_RANGE_1 = (1_000_000_000, 1_999_999_999)
FD_RANGE_2 = (7_000_000_000, 7_999_999_999)

# CUSTCODE values that identify individual customers
# IF CUSTCODE IN (77,78,95,96) THEN OUTPUT ...IND
IND_CUSTCODES = {77, 78, 95, 96}


# =============================================================================
# UTILITY: SAS7BDAT READER
# =============================================================================
def _read_sas(path: Path) -> pl.DataFrame:
    """Read a .sas7bdat file via pandas then convert to Polars (latin1 encoding)."""
    pdf = pd.read_sas(str(path), encoding="latin1")
    pdf.columns = [c.upper() for c in pdf.columns]
    return pl.from_pandas(pdf)


def _resolve_input(prefix: str) -> Path:
    """Use get_latest_file() to locate the most recent .sas7bdat for a given prefix."""
    filename = get_latest_file(str(INPUT_DIR), prefix)
    return INPUT_DIR / filename


# =============================================================================
# UTILITY: FORMAT HELPERS
# =============================================================================
def _fmt_amt(v) -> str:
    """COMMA16.2 format — missing treated as 0 (OPTIONS MISSING=0)."""
    if v is None:
        return f"{0:>16,.2f}"
    try:
        return f"{float(v):>16,.2f}"
    except (TypeError, ValueError):
        return f"{0:>16,.2f}"


# =============================================================================
# ASA REPORT WRITER
# =============================================================================
class AsaReportWriter:
    """
    Write fixed-width text reports with ASA carriage-control characters.
    RECFM=FB, LRECL=320, first character is ASA:
      '1' = form-feed (new page)
      ' ' = single space (normal line)
      '0' = double space
    """

    def __init__(self, out_path: Path, page_length: int = PAGE_LENGTH):
        self.out_path    = out_path
        self.page_length = page_length
        self.page_no     = 0
        self.line_no     = 0
        self.lines: list[str] = []

    def _raw(self, asa: str, text: str) -> None:
        record = f"{asa}{text}"
        # Pad/truncate to LRECL=320 (1 ASA + 319 content)
        record = record[:320].ljust(320)
        self.lines.append(record + "\n")

    def _emit(self, asa: str, text: str) -> None:
        self._raw(asa, text)
        self.line_no += 1

    def new_page(self, title_lines: Iterable[str]) -> None:
        """Emit form-feed and reprint title/header block."""
        self.page_no += 1
        self.line_no  = 0
        self._raw("1", "")          # form-feed; does not count against line_no
        for tline in title_lines:
            self._emit(" ", tline)

    def _ensure_space(self, needed: int = 1) -> None:
        """Page-break if there is not enough room for `needed` more lines."""
        if self.line_no + needed >= self.page_length:
            self.new_page(self._title_cache)

    def set_title_cache(self, title_lines: list[str]) -> None:
        """Store title lines so page-overflow can reprint them."""
        self._title_cache = title_lines

    def line(self, text: str = "", double_space: bool = False) -> None:
        asa = "0" if double_space else " "
        self._ensure_space(1)
        self._emit(asa, text)

    def blank(self) -> None:
        self._ensure_space(1)
        self._emit(" ", "")

    def save(self) -> None:
        self.out_path.write_text("".join(self.lines), encoding="latin1")
        print(f"  Written: {self.out_path}")


# =============================================================================
# DATA PREPARATION
# =============================================================================
def _build_cis(cis_df: pl.DataFrame) -> pl.DataFrame:
    """
    KEEP CUSTNO ACCTNO CUSTNAME ICNO NEWIC OLDIC INDORG;
    IF NEWIC NE '' THEN ICNO = NEWIC; ELSE ICNO = OLDIC;
    """
    keep_cols = {"CUSTNO", "ACCTNO", "CUSTNAME", "NEWIC", "OLDIC", "INDORG"}
    available = [c for c in keep_cols if c in cis_df.columns]
    df = cis_df.select(available)
    # Derive ICNO
    if "NEWIC" in df.columns and "OLDIC" in df.columns:
        df = df.with_columns(
            pl.when(
                pl.col("NEWIC").is_not_null() & (pl.col("NEWIC").cast(pl.Utf8).str.strip_chars() != "")
            )
            .then(pl.col("NEWIC").cast(pl.Utf8))
            .otherwise(pl.col("OLDIC").cast(pl.Utf8))
            .alias("ICNO")
        )
    else:
        df = df.with_columns(pl.lit("").alias("ICNO"))
    return df


def build_cisca_cisfd() -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    DATA CISCA CISFD;
       SET CISCA.DEPOSIT CISFD.DEPOSIT;
       IF NEWIC NE '' THEN ICNO = NEWIC; ELSE ICNO = OLDIC;
       IF (3000000000<=ACCTNO<=3999999999) THEN OUTPUT CISCA;
       IF (1000000000<=ACCTNO<=1999999999) THEN OUTPUT CISFD;
       IF (7000000000<=ACCTNO<=7999999999) THEN OUTPUT CISFD;
    """
    cisca_raw = _read_sas(_resolve_input(CISR1CA_PREFIX))
    cisfd_raw = _read_sas(_resolve_input(CISR1FD_PREFIX))

    # SET (union) both CIS files, then split by ACCTNO range
    combined = pl.concat([_build_cis(cisca_raw), _build_cis(cisfd_raw)], how="diagonal_relaxed")

    acctno = pl.col("ACCTNO").cast(pl.Int64, strict=False)
    cisca = combined.filter(acctno.is_between(*CA_RANGE))
    cisfd = combined.filter(
        acctno.is_between(*FD_RANGE_1) | acctno.is_between(*FD_RANGE_2)
    )
    return cisca, cisfd


def build_ca_fd() -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    DATA CA FD;
       SET DEPOSIT.CURRENT DEPOSIT.FD;
       IF CURBAL > 0;
       IF (3000000000<=ACCTNO<=3999999999) THEN OUTPUT CA;
       IF (1000000000<=ACCTNO<=1999999999) THEN OUTPUT FD;
       IF (7000000000<=ACCTNO<=7999999999) THEN OUTPUT FD;
    """
    ica_df = _read_sas(_resolve_input(ICA_PREFIX))   # DEPOSIT.CURRENT → ica
    ifd_df = _read_sas(_resolve_input(IFD_PREFIX))   # DEPOSIT.FD      → ifd

    combined = pl.concat([ica_df, ifd_df], how="diagonal_relaxed")
    combined = combined.filter(pl.col("CURBAL").cast(pl.Float64, strict=False) > 0)

    acctno = pl.col("ACCTNO").cast(pl.Int64, strict=False)
    ca = combined.filter(acctno.is_between(*CA_RANGE))
    fd = combined.filter(
        acctno.is_between(*FD_RANGE_1) | acctno.is_between(*FD_RANGE_2)
    )
    return ca, fd


def build_split_sets(
    ca: pl.DataFrame,
    fd: pl.DataFrame,
    cisca: pl.DataFrame,
    cisfd: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    MERGE CA + CISCA → CAIND / CAORG
    MERGE CISFD + FD → FDIND / FDORG

    SAS MERGE last-dataset-wins applies; left join preserves IN=A semantics.
    CABAL = CURBAL  /  FDBAL = CURBAL (aliases set after merge)
    IF CUSTCODE IN (77,78,95,96) → IND; ELSE IF INDORG='O' → ORG
    """
    # --- CA branch ---
    # PROC SORT DATA=CISCA; BY ACCTNO;
    # PROC SORT DATA=CA; BY ACCTNO;
    # MERGE CA(IN=A) CISCA; BY ACCTNO; IF A AND ...
    caj = ca.join(cisca, on="ACCTNO", how="left")
    caj = caj.filter(
        (pl.col("PURPOSE").cast(pl.Utf8, strict=False) != "2")
        & (~pl.col("PRODUCT").cast(pl.Int64, strict=False).is_in(list(CA_EXCL_PRODUCTS)))
    ).with_columns(
        pl.col("CURBAL").cast(pl.Float64, strict=False).alias("CABAL"),
        pl.lit(None).cast(pl.Float64).alias("FDBAL"),   # not from CA branch
    )

    custcode_ca = pl.col("CUSTCODE").cast(pl.Int64, strict=False)
    caind = caj.filter(custcode_ca.is_in(list(IND_CUSTCODES)))
    caorg = caj.filter(
        ~custcode_ca.is_in(list(IND_CUSTCODES))
        & (pl.col("INDORG").cast(pl.Utf8, strict=False) == "O")
    )

    # --- FD branch ---
    # PROC SORT DATA=CISFD; BY ACCTNO;
    # PROC SORT DATA=FD; BY ACCTNO;
    # MERGE CISFD FD(IN=A); BY ACCTNO; IF A AND ...
    fdj = cisfd.join(fd, on="ACCTNO", how="inner")   # IN=A on FD side → inner join
    fdj = fdj.filter(
        (pl.col("PURPOSE").cast(pl.Utf8, strict=False) != "2")
        & (~pl.col("PRODUCT").cast(pl.Int64, strict=False).is_in(list(FD_EXCL_PRODUCTS)))
    ).with_columns(
        pl.col("CURBAL").cast(pl.Float64, strict=False).alias("FDBAL"),
        pl.lit(None).cast(pl.Float64).alias("CABAL"),   # not from FD branch
    )

    custcode_fd = pl.col("CUSTCODE").cast(pl.Int64, strict=False)
    fdind = fdj.filter(custcode_fd.is_in(list(IND_CUSTCODES)))
    fdorg = fdj.filter(
        ~custcode_fd.is_in(list(IND_CUSTCODES))
        & (pl.col("INDORG").cast(pl.Utf8, strict=False) == "O")
    )

    return caind, caorg, fdind, fdorg


# =============================================================================
# %MACRO PRNREC — TOP 50 SUMMARY + DETAIL REPORT
# =============================================================================
def _build_summary_header(title: str, rdate: str) -> list[str]:
    return [
        "PUBLIC ISLAMIC BANK BERHAD (EIIMBTOP5)",
        f"NEW LIQUIDITY FRAMEWORK AS AT {rdate}",
        title,
    ]


def _print_summary_table(writer: AsaReportWriter, d2: pl.DataFrame) -> None:
    """
    PROC PRINT DATA=DATA2 (summary — top 50 aggregated rows)
    LABEL CUSTNAME='DEPOSITOR' CURBAL='TOTAL BALANCE' FDBAL='FD BALANCE' CABAL='CA BALANCE'
    FORMAT CURBAL FDBAL CABAL COMMA16.2;
    """
    hdr = (
        f"{'DEPOSITOR':<40}  {'TOTAL BALANCE':>16}  {'FD BALANCE':>16}  {'CA BALANCE':>16}"
    )
    sep = "-" * len(hdr)
    writer.line(hdr)
    writer.line(sep)
    for row in d2.iter_rows(named=True):
        writer.line(
            f"{str(row.get('CUSTNAME') or '')[:40]:<40}  "
            f"{_fmt_amt(row.get('CURBAL'))}  "
            f"{_fmt_amt(row.get('FDBAL'))}  "
            f"{_fmt_amt(row.get('CABAL'))}"
        )
    writer.blank()


def _print_detail_table(writer: AsaReportWriter, d3: pl.DataFrame) -> None:
    """
    PROC PRINT DATA=DATA3 BY ICNO CUSTNAME; SUM CURBAL;
    LABEL BRANCH='BRANCH CODE' ACCTNO='MNI NO' CUSTNAME='DEPOSITOR'
          CUSTNO='CIS NO' NEWIC='NEW IC' OLDIC='OLD IC'
          CURBAL='CURRENT BALANCE' PRODUCT='PRODUCT' COSTCTR='COST CENTRE'
    VAR BRANCH ACCTNO CUSTNAME CUSTNO NEWIC OLDIC CURBAL PRODUCT COSTCTR;
    """
    hdr = (
        f"{'BRANCH':>6}  {'MNI NO':>12}  {'DEPOSITOR':<30}  "
        f"{'CIS NO':>10}  {'NEW IC':<15}  {'OLD IC':<15}  "
        f"{'CURRENT BALANCE':>16}  {'PRODUCT':>7}  {'COST CENTRE':>10}"
    )
    sep = "-" * len(hdr)
    writer.line(hdr)
    writer.line(sep)

    current_key = None
    subtotal    = 0.0

    for row in d3.iter_rows(named=True):
        key = (
            str(row.get("ICNO") or "").strip(),
            str(row.get("CUSTNAME") or "").strip(),
        )
        bal = float(row.get("CURBAL") or 0)

        if current_key is not None and key != current_key:
            # SUM line per BY group
            writer.line(
                f"{'':>6}  {'':>12}  {'':30}  "
                f"{'':>10}  {'':15}  {'':15}  "
                f"{_fmt_amt(subtotal)}  {'':>7}  {'':>10}"
            )
            writer.blank()
            subtotal = 0.0

        current_key = key
        subtotal   += bal

        writer.line(
            f"{str(row.get('BRANCH') or '')[:6]:>6}  "
            f"{str(row.get('ACCTNO') or '')[:12]:>12}  "
            f"{str(row.get('CUSTNAME') or '')[:30]:<30}  "
            f"{str(row.get('CUSTNO') or '')[:10]:>10}  "
            f"{str(row.get('NEWIC') or '')[:15]:<15}  "
            f"{str(row.get('OLDIC') or '')[:15]:<15}  "
            f"{_fmt_amt(row.get('CURBAL'))}  "
            f"{str(row.get('PRODUCT') or '')[:7]:>7}  "
            f"{str(row.get('COSTCTR') or '')[:10]:>10}"
        )

    # Final BY-group SUM line
    if current_key is not None:
        writer.line(
            f"{'':>6}  {'':>12}  {'':30}  "
            f"{'':>10}  {'':15}  {'':15}  "
            f"{_fmt_amt(subtotal)}  {'':>7}  {'':>10}"
        )


def prnrec(data1: pl.DataFrame, title: str, rdate: str, out_path: Path) -> None:
    """
    %MACRO PRNREC
      PROC SORT DATA=DATA1; WHERE ICNO NE ''; BY ICNO CUSTNAME;
      PROC SUMMARY ... SUM= → DATA2 top50
      PROC PRINT DATA=DATA2 (summary)
      PROC SORT DATA=DATA2 OUT=DATA2(KEEP=ICNO CUSTNAME CUSTNO); BY ICNO CUSTNAME;
      DATA DATA3 = MERGE DATA1(IN=A) DATA2(IN=B); BY ICNO CUSTNAME; IF A AND B;
      PROC PRINT DATA=DATA3 BY ICNO CUSTNAME; SUM CURBAL;
    """
    # WHERE ICNO NE '' / sort by ICNO CUSTNAME
    d1 = data1.filter(
        pl.col("ICNO").is_not_null()
        & (pl.col("ICNO").cast(pl.Utf8, strict=False).str.strip_chars() != "")
    ).sort(["ICNO", "CUSTNAME"])

    # PROC SUMMARY → top 50 by descending CURBAL
    d2 = (
        d1.group_by(["ICNO", "CUSTNAME"], maintain_order=False)
        .agg([
            pl.col("CURBAL").cast(pl.Float64, strict=False).sum().alias("CURBAL"),
            pl.col("FDBAL").cast(pl.Float64, strict=False).sum().alias("FDBAL"),
            pl.col("CABAL").cast(pl.Float64, strict=False).sum().alias("CABAL"),
        ])
        .sort("CURBAL", descending=True)
        .head(50)
        .sort(["ICNO", "CUSTNAME"])
    )

    # DATA DATA3 — inner join d1 back to top-50 keys
    keys = d2.select(["ICNO", "CUSTNAME"])
    d3   = d1.join(keys, on=["ICNO", "CUSTNAME"], how="inner").sort(["ICNO", "CUSTNAME"])

    title_lines = _build_summary_header(title, rdate)
    writer = AsaReportWriter(out_path)
    writer.set_title_cache(title_lines)
    writer.new_page(title_lines)

    _print_summary_table(writer, d2)
    _print_detail_table(writer, d3)

    writer.save()


# =============================================================================
# PB SUBSIDIARIES REPORT
# =============================================================================
def _print_subs_detail(writer: AsaReportWriter, d: pl.DataFrame) -> None:
    """
    PROC PRINT DATA=DATA1 BY CUSTNO; SUM CURBAL;
    LABEL BRANCH='BRANCH CODE' ACCTNO='MNI NO' CUSTNAME='DEPOSITOR'
          CUSTNO='CIS NO' CUSTCODE='CUSTCD'
          CURBAL='CURRENT BALANCE' PRODUCT='PRODUCT' COSTCTR='COST CENTRE'
    VAR BRANCH ACCTNO CUSTNAME CUSTNO CUSTCODE CURBAL PRODUCT COSTCTR;
    """
    hdr = (
        f"{'BRANCH':>6}  {'MNI NO':>12}  {'DEPOSITOR':<30}  "
        f"{'CIS NO':>10}  {'CUSTCD':>6}  "
        f"{'CURRENT BALANCE':>16}  {'PRODUCT':>7}  {'COST CENTRE':>10}"
    )
    sep = "-" * len(hdr)
    writer.line(hdr)
    writer.line(sep)

    current_cust = None
    subtotal     = 0.0

    for row in d.iter_rows(named=True):
        custno = row.get("CUSTNO")
        bal    = float(row.get("CURBAL") or 0)

        if current_cust is not None and custno != current_cust:
            writer.line(
                f"{'':>6}  {'':>12}  {'':30}  "
                f"{'':>10}  {'':>6}  "
                f"{_fmt_amt(subtotal)}  {'':>7}  {'':>10}"
            )
            writer.blank()
            subtotal = 0.0

        current_cust = custno
        subtotal    += bal

        writer.line(
            f"{str(row.get('BRANCH') or '')[:6]:>6}  "
            f"{str(row.get('ACCTNO') or '')[:12]:>12}  "
            f"{str(row.get('CUSTNAME') or '')[:30]:<30}  "
            f"{str(row.get('CUSTNO') or '')[:10]:>10}  "
            f"{str(row.get('CUSTCODE') or '')[:6]:>6}  "
            f"{_fmt_amt(row.get('CURBAL'))}  "
            f"{str(row.get('PRODUCT') or '')[:7]:>7}  "
            f"{str(row.get('COSTCTR') or '')[:10]:>10}"
        )

    if current_cust is not None:
        writer.line(
            f"{'':>6}  {'':>12}  {'':30}  "
            f"{'':>10}  {'':>6}  "
            f"{_fmt_amt(subtotal)}  {'':>7}  {'':>10}"
        )


def write_subs_report(data_sub: pl.DataFrame, rdate: str, out_path: Path) -> None:
    """
    DATA DATA1;
       SET FDORG CAORG;
       IF CUSTNO IN (53227,169990,170108,3562038,3721354);
    PROC SORT; BY CUSTNO ACCTNO;
    PROC PRINTTO PRINT=FDSTEXT NEW;
    TITLE 'PB SUBSIDIARIES UNDER TOP 50 CORP DEPOSITORS @ ' &RDATE;
    """
    d = data_sub.filter(
        pl.col("CUSTNO").cast(pl.Int64, strict=False).is_in(list(SUBS_CUSTNOS))
    ).sort(["CUSTNO", "ACCTNO"])

    title_lines = [
        "PUBLIC ISLAMIC BANK BERHAD (EIIMBTOP5)",
        f"PB SUBSIDIARIES UNDER TOP 50 CORP DEPOSITORS @ {rdate}",
    ]
    writer = AsaReportWriter(out_path)
    writer.set_title_cache(title_lines)
    writer.new_page(title_lines)

    _print_subs_detail(writer, d)
    writer.save()


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    # --- REPTDATE ---
    # DATA BNM.REPTDATE; SET DEPOSIT.REPTDATE;
    # CALL SYMPUT('RDATE', PUT(REPTDATE, DDMMYY8.));
    # CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE), Z2.));
    # CALL SYMPUT('REPTDAY', PUT(DAY(REPTDATE), Z2.));
    # CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR4.));
    # CALL SYMPUT('NOWK', ...)  — week-of-month bucket for input filename key
    reptdate_values = get_reptdate_values()
    rdate   = reptdate_values["RDATE"]     # DD/MM/YY  (DDMMYY8.)
    reptmon = reptdate_values["REPTMON"]   # ZZ2 month
    # reptday / reptyear / nowk available from reptdate_values if needed elsewhere

    print(f"Report Date : {rdate}  (REPTMON={reptmon})")

    # --- Load CIS data ---
    print("Reading CIS files ...")
    cisca, cisfd = build_cisca_cisfd()

    # --- Load deposit data ---
    print("Reading deposit files ...")
    ca, fd = build_ca_fd()

    # --- Build split datasets ---
    caind, caorg, fdind, fdorg = build_split_sets(ca, fd, cisca, cisfd)

    # *** FD+CA INDIVIDUAL CUSTOMERS ***
    # DATA DATA1; SET FDIND CAIND;
    # IF ICNO='  ' THEN ICNO='XX';
    data1_ind = pl.concat([fdind, caind], how="diagonal_relaxed").with_columns(
        pl.when(pl.col("ICNO").cast(pl.Utf8, strict=False).str.strip_chars() == "")
        .then(pl.lit("XX"))
        .otherwise(pl.col("ICNO").cast(pl.Utf8, strict=False))
        .alias("ICNO")
    )
    # PROC PRINTTO PRINT=FD11TEXT NEW;
    # TITLE 'TOP 50 LARGEST FD+CA INDIVIDUAL CUSTOMERS AS AT ' &RDATE;
    title_ind = f"TOP 50 LARGEST FD+CA INDIVIDUAL CUSTOMERS AS AT {rdate}"
    print(f"\nGenerating: {FD11TEXT_OUT.name}")
    prnrec(data1_ind, title_ind, rdate, FD11TEXT_OUT)

    # *** FD+CA CORPORATE CUSTOMERS ***
    # DATA DATA1; SET FDORG CAORG;
    # IF ICNO='  ' THEN ICNO='XX';
    data1_corp = pl.concat([fdorg, caorg], how="diagonal_relaxed").with_columns(
        pl.when(pl.col("ICNO").cast(pl.Utf8, strict=False).str.strip_chars() == "")
        .then(pl.lit("XX"))
        .otherwise(pl.col("ICNO").cast(pl.Utf8, strict=False))
        .alias("ICNO")
    )
    # PROC PRINTTO PRINT=FD12TEXT NEW;
    # TITLE 'TOP 50 LARGEST FD+CA CORPORATE CUSTOMERS AS AT ' &RDATE;
    title_corp = f"TOP 50 LARGEST FD+CA CORPORATE CUSTOMERS AS AT {rdate}"
    print(f"Generating: {FD12TEXT_OUT.name}")
    prnrec(data1_corp, title_corp, rdate, FD12TEXT_OUT)

    # *** PB SUBSIDIARIES ***
    # DATA DATA1; SET FDORG CAORG;
    # IF CUSTNO IN (53227,169990,170108,3562038,3721354);
    data_sub = pl.concat([fdorg, caorg], how="diagonal_relaxed")
    print(f"Generating: {FDSTEXT_OUT.name}")
    write_subs_report(data_sub, rdate, FDSTEXT_OUT)

    # --- Summary results ---
    print("\n=== Results Summary ===")
    print(f"  INDIVIDUAL top-50 source rows : {data1_ind.height}")
    print(f"  CORPORATE  top-50 source rows : {data1_corp.height}")
    print(f"  SUBSIDIARIES filtered rows    : {data_sub.filter(pl.col('CUSTNO').cast(pl.Int64, strict=False).is_in(list(SUBS_CUSTNOS))).height}")
    print(f"\nOutput directory: {OUTPUT_DIR}")
    print(f"  {FD11TEXT_OUT}")
    print(f"  {FD12TEXT_OUT}")
    print(f"  {FDSTEXT_OUT}")


if __name__ == "__main__":
    main()
