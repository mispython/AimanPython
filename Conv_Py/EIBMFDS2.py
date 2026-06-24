#!/usr/bin/env python3
"""
Program : EIBMFDS2.py
Purpose : Corporate Report by Branch. After EIBMRPTS.
          Monthly Interest Rates Exception Report by Branch for FD Accounts
          with Total Receipts > RM 1.0 M : Corporate.
          Generates report grouped by BRANCH, then by NAMEQ, then by ACCTNO,
          with subtotals after each ACCTNO group and each NAMEQ group.
"""

import pandas as pd
import polars as pl
from pathlib import Path

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Testing Path
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR / "input" / "prod"
OUTPUT_DIR = BASE_DIR / "output" / "EIBMFDS2"

# # Production Path
# INPUT_DIR  = Path("/dwh")
# OUTPUT_DIR = Path("/host/mis/output/report")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input path - FDC.FD1MC (fixed deposits corporate master)
# File name example: fd1mc.sas7bdat
INPUT_FDC = INPUT_DIR / "fd1mc.sas7bdat"
# INPUT_FDC = get_latest_file(INPUT_DIR / "fdc", "fd1mc")

# Output path
OUTPUT_REPORT = build_output_file(OUTPUT_DIR, "PBB_FDRATE3_REPORT").with_suffix(".txt")
# Output example: PBB_FDRATE3_REPORT_180526.txt

# Report configuration
PAGE_SIZE = 60   # Default PS=60 (OPTIONS PS= not explicitly set in SAS source)

# ============================================================================
# REPORT DATE  (from REPTDATE module — no reptdate.parquet file is read)
# ============================================================================
reptdate_values = get_reptdate_values()
REPTDATE    = reptdate_values.reptdate
REPTYEAR    = reptdate_values.reptyear
REPTMON     = reptdate_values.reptmon
REPTDAY     = reptdate_values.reptday
NOWK        = reptdate_values.nowk

# SAS: PUT(REPTDATE, DDMMYY10.) -> DD/MM/YYYY
REPTDT = REPTDATE.strftime("%d/%m/%Y")

# ============================================================================
# INPUT FILE EXISTENCE CHECK — fail fast before any processing
# ============================================================================
_REQUIRED_INPUTS = {
    "FD1MC Corporate FD Master": INPUT_FDC,
}

_missing = [
    f"  [{label}] {path}"
    for label, path in _REQUIRED_INPUTS.items()
    if not path.exists()
]
if _missing:
    raise FileNotFoundError(
        "The following required input files are missing:\n" + "\n".join(_missing)
    )


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read one .sas7bdat file and return a Polars DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")

    pandas_df = pd.read_sas(
        path,
        format="sas7bdat",
        encoding="latin1",
    )

    pandas_df.columns = [
        str(col).upper().strip()
        for col in pandas_df.columns
    ]

    return pl.from_pandas(pandas_df)


def _safe_float(value) -> float:
    """Return float safely, defaulting to 0.0 for None/NaN."""
    try:
        if value is None:
            return 0.0
        f = float(value)
        return 0.0 if f != f else f   # guard NaN
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value) -> int:
    """Return int safely, defaulting to 0 for None/NaN."""
    try:
        if value is None:
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _safe_text(value, length: int) -> str:
    """Return string safely, truncated to length, defaulting to '' for None."""
    if value is None:
        return ""
    return str(value)[:length]


def _safe_str(value) -> str:
    """Return decoded string from bytes or str."""
    if isinstance(value, bytes):
        return value.decode("latin1", errors="replace").strip()
    if value is None:
        return ""
    return str(value).strip()


def _format_acctno(value) -> str:
    """Format ACCTNO as 10-digit integer string (SAS FORMAT=10.)."""
    return f"{_safe_int(value):>10}"


def _format_comma12_2(value) -> str:
    """Format as COMMA12.2 (right-aligned 12 chars with commas, 2 decimals)."""
    return f"{_safe_float(value):>12,.2f}"


def _format_comma18_2(value) -> str:
    """Format as COMMA18.2 (right-aligned 18 chars with commas, 2 decimals)."""
    return f"{_safe_float(value):>18,.2f}"


def _format_comma7_2(value) -> str:
    """Format as COMMA7.2 (right-aligned 7 chars with commas, 2 decimals)."""
    return f"{_safe_float(value):>7,.2f}"


def _get_report_titles(reptdt: str) -> list:
    """Return the three title lines used at the top of every page.

    SAS equivalent:
        TITLE1 'PUBLIC BANK BERHAD-REPORT ID: PDR/D/FDRATE3';
        TITLE2 'MONTHLY INTEREST RATES EXCEPTION REPORT BY BRANCH @ ' &REPTDT;
        TITLE3 'FOR FD ACCOUNTS WITH TOTAL RECEIPTS > RM 1.0 M : CORPORATE';
    """
    return [
        "PUBLIC BANK BERHAD-REPORT ID: PDR/D/FDRATE3",
        f"MONTHLY INTEREST RATES EXCEPTION REPORT BY BRANCH @ {reptdt}",
        "FOR FD ACCOUNTS WITH TOTAL RECEIPTS > RM 1.0 M : CORPORATE",
    ]


def _build_title_lines(titles: list, branch: str) -> list:
    """Build page title block.  ASA '1' triggers form-feed on the first line."""
    return [
        f"1  {titles[0]}\n",
        f"   {titles[1]}\n",
        f"   {titles[2]}\n",
        "\n",
        f"   BRANCH: {branch}\n",
        "\n",
    ]


def _build_header_lines() -> list:
    """Build column header lines.

    SAS PROC REPORT COLUMN order:
        NAMEQ BRN ACCTNO DEPID CURBAL DEPTERM DEPNDT MATNDT RATE1 NR CURBALN CURBALY

    HEADSKIP produces one blank line between header and first data row.
    HEADLINE draws a line under the column headers.
    SPLIT='*' — column labels split on '*' into two header lines.
    """
    hdr1 = (
        f"{'NAME OF CUSTOMER':<35}"
        f"{'BRN':<5}"
        f"{'ACCOUNT':>10}"
        f"{'RECEIPT':>8}"
        f"{'RECEIPT':>13}"
        f"{'':>5}"
        f"{'DEPOSIT':>11}"
        f"{'MATURITY':>11}"
        f"{'OFFERED':>8}"
        f"{'COUNTER':>8}"
    )
    hdr2 = (
        f"{'':35}"
        f"{'':5}"
        f"{'NUMBER':>10}"
        f"{'NUMBER':>8}"
        f"{'AMOUNT':>13}"
        f"{'TERM':>5}"
        f"{'DATE':>11}"
        f"{'DATE':>11}"
        f"{'RATE':>8}"
        f"{'RATE':>8}"
    )
    separator = "-" * len(hdr2)
    return [
        f"   {hdr1}\n",
        f"   {hdr2}\n",
        f"   {separator}\n",
        "\n",
    ]


def _build_detail_line(row: dict) -> str:
    """Build one data detail line for a single receipt row.

    SAS PROC REPORT formats used:
        NAMEQ   $35.   NAME OF CUSTOMER (35 chars)
        BRN     $3.    branch (3 chars)
        ACCTNO  10.    account number
        DEPID   7.     receipt number
        CURBAL  COMMA12.2
        DEPTERM 2.
        DEPNDT  $10.
        MATNDT  $10.
        RATE1   COMMA7.2
        NR      COMMA7.2
        CURBALN / CURBALY are NOPRINT (not shown on detail lines)
    """
    return (
        f"   {_safe_str(row.get('NAMEQ')):<35}"
        f"{_safe_str(row.get('BRN')):<5}"
        f"{_format_acctno(row.get('ACCTNO'))}"
        f"{_safe_int(row.get('DEPID')):>8}"
        f"{_format_comma12_2(row.get('CURBAL')):>13}"
        f"{_safe_int(row.get('DEPTERM')):>5}"
        f"  {_safe_str(row.get('DEPNDT')):<10}"
        f"  {_safe_str(row.get('MATNDT')):<10}"
        f"{_format_comma7_2(row.get('RATE1'))}"
        f"{_format_comma7_2(row.get('NR'))}\n"
    )


def _build_acctno_subtotal_lines(
    curbal_sum: float,
    curbaly_sum: float,
    curbaln_sum: float,
) -> list:
    """Build the BREAK AFTER ACCTNO subtotal block.

    SAS equivalent:
        COMPUTE AFTER ACCTNO;
        LINE @045 79*'-';
        LINE @045 'ACCT TOT =     ' CURBAL.SUM  COMMA18.2
        LINE @085 'C='              CURBALY.SUM COMMA12.2
        LINE @104 'S='              CURBALN.SUM COMMA12.2;
        LINE @045 79*'-';
        ENDCOMP;

    Column positions (1-based SAS @n):
        @045 = col 45 (indent 44 spaces)
        @085 = col 85 (after ACCT TOT label+value)
        @104 = col 104
        79*'-' = 79 dashes starting at col 45  -> total line length = 44+79 = 123
    """
    dash_line   = " " * 44 + "-" * 79
    # 'ACCT TOT =     ' is 16 chars; COMMA18.2 value occupies 18 chars
    #   => text starts @045, value ends around @078; 'C=' at @085, 'S=' at @104
    acct_label  = "ACCT TOT =     "
    c_label     = "C="
    s_label     = "S="

    data_line = (
        " " * 44
        + f"{acct_label}{_format_comma18_2(curbal_sum)}"
        + f"  {c_label}{_format_comma12_2(curbaly_sum)}"
        + f"  {s_label}{_format_comma12_2(curbaln_sum)}"
    )

    return [
        f"{dash_line}\n",
        f"{data_line}\n",
        f"{dash_line}\n",
    ]


def _build_nameq_subtotal_lines(
    curbal_sum: float,
    curbaly_sum: float,
    curbaln_sum: float,
) -> list:
    """Build the BREAK AFTER NAMEQ subtotal block.

    SAS equivalent:
        COMPUTE AFTER NAMEQ;
        LINE @045 79*'-';
        LINE @045 'NAME TOT =     ' CURBAL.SUM  COMMA18.2
        LINE @085 'C='              CURBALY.SUM COMMA12.2
        LINE @104 'S='              CURBALN.SUM COMMA12.2;
        LINE @045 79*'-';
        ENDCOMP;
    """
    dash_line  = " " * 44 + "-" * 79
    name_label = "NAME TOT =     "
    c_label    = "C="
    s_label    = "S="

    data_line = (
        " " * 44
        + f"{name_label}{_format_comma18_2(curbal_sum)}"
        + f"  {c_label}{_format_comma12_2(curbaly_sum)}"
        + f"  {s_label}{_format_comma12_2(curbaln_sum)}"
    )

    return [
        f"{dash_line}\n",
        f"{data_line}\n",
        f"{dash_line}\n",
    ]


# ============================================================================
# PAGE WRITER
# ============================================================================

class PageWriter:
    """Stateful page-writer that tracks lines used and emits form-feeds."""

    def __init__(self, file, page_size: int, title_lines: list, header_lines: list):
        self._file        = file
        self._page_size   = page_size
        self._title_lines = title_lines
        self._header_lines = header_lines
        self._lines_used  = 0
        self._first_page  = True

    def _start_new_page(self) -> None:
        """Emit title + header block; prepend form-feed after the first page."""
        block = self._title_lines + self._header_lines
        if self._first_page:
            # ASA '1' already embedded in _title_lines[0]
            for line in block:
                self._file.write(line)
            self._first_page = False
        else:
            # Replace leading ASA character with form-feed on subsequent pages.
            first_line = "\f" + block[0][1:]
            self._file.write(first_line)
            for line in block[1:]:
                self._file.write(line)
        self._lines_used = len(block)

    def update_titles(self, title_lines: list, header_lines: list) -> None:
        """Update title/header block (called when BRANCH changes)."""
        self._title_lines  = title_lines
        self._header_lines = header_lines
        self._lines_used   = 0
        self._first_page   = True

    def write_lines(self, lines: list) -> None:
        """Write a block of lines, starting a new page if necessary."""
        if self._lines_used == 0:
            self._start_new_page()

        for line in lines:
            if self._lines_used >= self._page_size:
                self._start_new_page()
            self._file.write(line)
            self._lines_used += 1

    def write_block(self, lines: list) -> None:
        """Write a block that must not be split across pages."""
        if self._lines_used == 0:
            self._start_new_page()

        remaining = self._page_size - self._lines_used
        if remaining < len(lines):
            self._start_new_page()

        for line in lines:
            self._file.write(line)
            self._lines_used += 1


# ============================================================================
# MAIN REPORT WRITER
# ============================================================================

def _write_report_file(
    mgrrat: pl.DataFrame,
    output_file: Path,
    titles: list,
    report_date: str,
) -> None:
    """Write the full report replicating PROC REPORT BY BRANCH output.

    Sort order (SAS PROC SORT): BRANCH NAMEQ ACCTNO MATDTE
    Grouping:
        BY BRANCH  -> new page per branch
        NAMEQ      -> GROUP column (show once per name group)
        ACCTNO     -> GROUP column (subtotal after each ACCTNO)
        NAMEQ      -> subtotal after each NAMEQ group
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # SAS PROC SORT: BY BRANCH NAMEQ ACCTNO MATDTE
    mgrrat_sorted = mgrrat.sort(["BRANCH", "NAMEQ", "ACCTNO", "MATDTE"])
    rows = mgrrat_sorted.to_dicts()

    header_lines = _build_header_lines()

    with open(output_file, "w", encoding="utf-8") as report_file:

        prev_branch = None
        prev_nameq  = None
        prev_acctno = None
        writer: PageWriter = None

        # Accumulators
        acct_curbal  = 0.0
        acct_curbaly = 0.0
        acct_curbaln = 0.0
        name_curbal  = 0.0
        name_curbaly = 0.0
        name_curbaln = 0.0

        for i, row in enumerate(rows):
            branch  = _safe_str(row.get("BRANCH"))
            nameq   = _safe_str(row.get("NAMEQ"))
            acctno  = _safe_int(row.get("ACCTNO"))
            curbal  = _safe_float(row.get("CURBAL"))
            curbaly = _safe_float(row.get("CURBALY"))
            curbaln = _safe_float(row.get("CURBALN"))

            # ── BRANCH BREAK ─────────────────────────────────────────────────
            if branch != prev_branch:
                # Flush any open ACCTNO / NAMEQ subtotals
                if prev_acctno is not None:
                    writer.write_block(
                        _build_acctno_subtotal_lines(acct_curbal, acct_curbaly, acct_curbaln)
                    )
                    name_curbal  += acct_curbal
                    name_curbaly += acct_curbaly
                    name_curbaln += acct_curbaln
                if prev_nameq is not None:
                    writer.write_block(
                        _build_nameq_subtotal_lines(name_curbal, name_curbaly, name_curbaln)
                    )

                # Start a fresh writer for the new branch
                branch_title_lines = _build_title_lines(titles, branch)
                if writer is None:
                    writer = PageWriter(
                        report_file, PAGE_SIZE, branch_title_lines, header_lines
                    )
                else:
                    writer.update_titles(branch_title_lines, header_lines)

                prev_branch  = branch
                prev_nameq   = None
                prev_acctno  = None
                acct_curbal  = 0.0
                acct_curbaly = 0.0
                acct_curbaln = 0.0
                name_curbal  = 0.0
                name_curbaly = 0.0
                name_curbaln = 0.0

            # ── NAMEQ BREAK ──────────────────────────────────────────────────
            if nameq != prev_nameq:
                if prev_acctno is not None:
                    writer.write_block(
                        _build_acctno_subtotal_lines(acct_curbal, acct_curbaly, acct_curbaln)
                    )
                    name_curbal  += acct_curbal
                    name_curbaly += acct_curbaly
                    name_curbaln += acct_curbaln
                if prev_nameq is not None:
                    writer.write_block(
                        _build_nameq_subtotal_lines(name_curbal, name_curbaly, name_curbaln)
                    )
                prev_nameq   = nameq
                prev_acctno  = None
                acct_curbal  = 0.0
                acct_curbaly = 0.0
                acct_curbaln = 0.0
                name_curbal  = 0.0
                name_curbaly = 0.0
                name_curbaln = 0.0

            # ── ACCTNO BREAK ─────────────────────────────────────────────────
            if acctno != prev_acctno:
                if prev_acctno is not None:
                    writer.write_block(
                        _build_acctno_subtotal_lines(acct_curbal, acct_curbaly, acct_curbaln)
                    )
                    name_curbal  += acct_curbal
                    name_curbaly += acct_curbaly
                    name_curbaln += acct_curbaln
                prev_acctno  = acctno
                acct_curbal  = 0.0
                acct_curbaly = 0.0
                acct_curbaln = 0.0

            # ── DETAIL LINE ──────────────────────────────────────────────────
            writer.write_lines([_build_detail_line(row)])

            # Accumulate for subtotals
            acct_curbal  += curbal
            acct_curbaly += curbaly
            acct_curbaln += curbaln

        # ── FLUSH FINAL SUBTOTALS ────────────────────────────────────────────
        if prev_acctno is not None and writer is not None:
            writer.write_block(
                _build_acctno_subtotal_lines(acct_curbal, acct_curbaly, acct_curbaln)
            )
            name_curbal  += acct_curbal
            name_curbaly += acct_curbaly
            name_curbaln += acct_curbaln
        if prev_nameq is not None and writer is not None:
            writer.write_block(
                _build_nameq_subtotal_lines(name_curbal, name_curbaly, name_curbaln)
            )


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main() -> None:
    print("=" * 70)
    print("EIBMFDS2 — MONTHLY FD INTEREST RATES EXCEPTION REPORT (CORPORATE)")
    print("=" * 70)
    print(f"\nReport Date : {REPTDT}")
    print(f"Input  File : {INPUT_FDC}")
    print(f"Output File : {OUTPUT_REPORT}")

    # ── STEP 1: Load FD corporate master ────────────────────────────────────
    print("\nStep 1: Loading fd1mc.sas7bdat ...")
    mgrrat = _read_sas7bdat(INPUT_FDC)
    print(f"  Records loaded: {len(mgrrat):,}")

    # SAS DATA MGRRAT; SET FDC.FD1MC; (no filter applied in source)
    # Required columns check
    required_cols = {
        "BRANCH", "NAMEQ", "BRN", "ACCTNO", "DEPID",
        "CURBAL", "DEPTERM", "DEPNDT", "MATNDT",
        "RATE1", "NR", "CURBALN", "CURBALY", "MATDTE",
    }
    missing_cols = required_cols - set(mgrrat.columns)
    if missing_cols:
        raise ValueError(
            f"fd1mc.sas7bdat is missing required column(s): {', '.join(sorted(missing_cols))}"
        )

    # ── STEP 2: Generate report ──────────────────────────────────────────────
    print("\nStep 2: Generating report ...")
    titles = _get_report_titles(REPTDT)
    _write_report_file(mgrrat, OUTPUT_REPORT, titles, REPTDT)
    print(f"  Report saved: {OUTPUT_REPORT}")

    # ── STEP 3: Preview output ───────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"PREVIEW: {OUTPUT_REPORT.name}")
    print(f"{'=' * 70}\n")
    with open(OUTPUT_REPORT, "r", encoding="utf-8") as f:
        print(f.read())
    print(f"{'=' * 70}")
    print("REPORT GENERATION COMPLETE")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
