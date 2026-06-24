#!/usr/bin/env python3
"""
Program : EIBMFDC3.py
Purpose : Report on FD > 1M (Corporate); By Branch Name; Month-End. Cold.
          Monthly Interest Rates Exception Report by Branch for FD Accounts
          with Total Receipts > RM 1.0 M : Corporate.
          Generates report grouped by BRANCH, then by NAMEQ,
          with subtotals after each NAMEQ group.
          Input filtered to JI='C' (Corporate accounts only).
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
OUTPUT_DIR = BASE_DIR / "output" / "EIBMFDC3"

# # Production Path
# INPUT_DIR  = Path("/dwh")
# OUTPUT_DIR = Path("/host/mis/output/report")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input path - FD1M.FD1M (fixed deposits master)
# File name example: fd1m.sas7bdat
INPUT_FD1M = INPUT_DIR / "fd1m.sas7bdat"
# INPUT_FD1M = get_latest_file(INPUT_DIR / "fd1m", "fd1m")

# Output path
OUTPUT_REPORT = build_output_file(OUTPUT_DIR, "PBB_FDRATE3C_REPORT").with_suffix(".txt")
# Output example: PBB_FDRATE3C_REPORT_180526.txt

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

# SAS: PUT(REPTDATE, DDMMYY8.) -> DD/MM/YY  (8-char with slashes, 2-digit year)
REPTDT = REPTDATE.strftime("%d/%m/%y")

# ============================================================================
# INPUT FILE EXISTENCE CHECK — fail fast before any processing
# ============================================================================
_REQUIRED_INPUTS = {
    "FD1M Fixed Deposits Master": INPUT_FD1M,
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


def _format_branch(value) -> str:
    """Format BRANCH as a whole-number string, stripping Float64 decimal suffix.

    .sas7bdat stores numeric fields as Float64, so BRANCH=2 reads as 2.0.
    Cast via int to produce '2' instead of '2.0'.
    """
    if value is None:
        return ""
    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return str(value).strip()


def _load_mgrrat(path: Path) -> pl.DataFrame:
    """Load fd1m.sas7bdat, filter to JI='C', and apply column renames.

    SAS equivalent:
        DATA MGRRAT;
            SET FD1M.FD1M;
            IF  JI='C';
            NAMEQ  = CUSTNAME;
            DEPID  = CDNO;
            DEPTERM= TERM;
            DEPNDT = DEPDTE;
            MATNDT = MATDTE;
            RATE1  = RATE;

    Column renames map source columns to their display aliases used throughout
    the report. All renames are applied before sort and report generation.
    """
    df = _read_sas7bdat(path)

    # Filter: IF JI='C'
    df = df.filter(pl.col("JI") == "C")

    # Column renames
    rename_map = {
        "CUSTNAME": "NAMEQ",
        "CDNO":     "DEPID",
        "TERM":     "DEPTERM",
        "DEPDTE":   "DEPNDT",
        "MATDTE":   "MATNDT",
        "RATE":     "RATE1",
    }
    existing_renames = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(existing_renames)

    return df


def _get_report_titles(reptdt: str) -> list:
    """Return the three title lines used at the top of every page.

    SAS equivalent:
        TITLE  'PUBLIC BANK BERHAD-REPORT ID: PDR/D/FDRATE3';
        TITLE2 'MONTHLY INTEREST RATES EXCEPTION REPORT BY BRANCH @ ' &REPTDT;
        TITLE3 'FOR FD ACCOUNTS WITH TOTAL RECEIPTS > RM 1.0 M : CORPORATE';
    """
    return [
        "PUBLIC BANK BERHAD-REPORT ID: PDR/D/FDRATE3",
        f"MONTHLY INTEREST RATES EXCEPTION REPORT BY BRANCH @ {reptdt}",
        "FOR FD ACCOUNTS WITH TOTAL RECEIPTS > RM 1.0 M : CORPORATE",
    ]


def _build_title_lines(titles: list, branch) -> list:
    """Build page title block.  ASA '1' triggers form-feed on the first line."""
    return [
        f"1  {titles[0]}\n",
        f"   {titles[1]}\n",
        f"   {titles[2]}\n",
        "\n",
        f"   BRANCH: {_format_branch(branch)}\n",
        "\n",
    ]


def _build_header_lines() -> list:
    """Build column header lines.

    SAS PROC REPORT COLUMN order:
        NAMEQ ACCTNO DEPID CURBAL DEPTERM DEPNDT MATNDT RATE1 NR CURBALN CURBALY

    HEADSKIP produces one blank line between header and first data row.
    HEADLINE draws a line under the column headers.
    SPLIT='*' — column labels split on '*' into two header lines.
    """
    hdr1 = (
        f"{'NAME OF CUSTOMER':<35}"
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
        ACCTNO  10.    account number
        DEPID   9.     receipt number (CDNO)
        CURBAL  COMMA12.2
        DEPTERM 2.     (TERM)
        DEPNDT  DDMMYY10.  deposit date (DEPDTE)
        MATNDT  DDMMYY10.  maturity date (MATDTE)
        RATE1   COMMA7.2   (RATE)
        NR      COMMA7.2
        CURBALN / CURBALY are NOPRINT (not shown on detail lines)
    """
    return (
        f"   {_safe_str(row.get('NAMEQ')):<35}"
        f"{_format_acctno(row.get('ACCTNO'))}"
        f"{_safe_int(row.get('DEPID')):>9}"
        f"{_format_comma12_2(row.get('CURBAL')):>13}"
        f"{_safe_int(row.get('DEPTERM')):>5}"
        f"  {_safe_str(row.get('DEPNDT')):<10}"
        f"  {_safe_str(row.get('MATNDT')):<10}"
        f"{_format_comma7_2(row.get('RATE1'))}"
        f"{_format_comma7_2(row.get('NR'))}\n"
    )


def _build_nameq_subtotal_lines(
    curbal_sum: float,
    curbaly_sum: float,
    curbaln_sum: float,
) -> list:
    """Build the BREAK AFTER NAMEQ subtotal block.

    SAS equivalent:
        COMPUTE AFTER NAMEQ;
        LINE @040 81*'-';
        LINE @040 'NAME TOT ='
             @057  CURBAL.SUM  COMMA18.2
             @082 'C='         CURBALY.SUM COMMA12.2
             @104 'S='         CURBALN.SUM COMMA12.2;
        LINE @040 81*'-';
        ENDCOMP;

    Column positions (1-based SAS @n):
        @040 = col 40 (indent 39 spaces)
        @057 = col 57 — COMMA18.2 value (18 chars) starts here
        @082 = col 82 — 'C=' label
        @104 = col 104 — 'S=' label
        81*'-' = 81 dashes starting at col 40
    """
    indent     = " " * 39
    dash_line  = indent + "-" * 81
    name_label = "NAME TOT ="

    # @040 'NAME TOT =' -> label is 10 chars, value at @057 means 7 spaces gap
    # @057 COMMA18.2 -> 18-char value
    # @082 'C=' COMMA12.2 -> 2-char label + 12-char value
    # @104 'S=' COMMA12.2 -> 2-char label + 12-char value
    data_line = (
        indent
        + f"{name_label}"
        + f"{' ' * 7}{_format_comma18_2(curbal_sum)}"
        + f"{'C='}{_format_comma12_2(curbaly_sum)}"
        + f"{'S='}{_format_comma12_2(curbaln_sum)}"
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
        self._file         = file
        self._page_size    = page_size
        self._title_lines  = title_lines
        self._header_lines = header_lines
        self._lines_used   = 0
        self._first_page   = True

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
) -> None:
    """Write the full report replicating PROC REPORT BY BRANCH output.

    Sort order (SAS PROC SORT): BRANCH NAMEQ ACCTNO MATNDT
    Grouping:
        BY BRANCH  -> new page per branch
        NAMEQ      -> ORDER column (show once per name group); subtotal after each NAMEQ
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # SAS PROC SORT: BY BRANCH NAMEQ ACCTNO MATDTE (MATNDT after rename)
    mgrrat_sorted = mgrrat.sort(["BRANCH", "NAMEQ", "ACCTNO", "MATNDT"])
    rows = mgrrat_sorted.to_dicts()

    header_lines = _build_header_lines()

    with open(output_file, "w", encoding="utf-8") as report_file:

        prev_branch = None
        prev_nameq  = None
        writer: PageWriter = None

        # Accumulators
        name_curbal  = 0.0
        name_curbaly = 0.0
        name_curbaln = 0.0

        for row in rows:
            branch  = _format_branch(row.get("BRANCH"))
            nameq   = _safe_str(row.get("NAMEQ"))
            curbal  = _safe_float(row.get("CURBAL"))
            curbaly = _safe_float(row.get("CURBALY"))
            curbaln = _safe_float(row.get("CURBALN"))

            # ── BRANCH BREAK ─────────────────────────────────────────────────
            if branch != prev_branch:
                # Flush any open NAMEQ subtotal
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
                name_curbal  = 0.0
                name_curbaly = 0.0
                name_curbaln = 0.0

            # ── NAMEQ BREAK ──────────────────────────────────────────────────
            if nameq != prev_nameq:
                if prev_nameq is not None:
                    writer.write_block(
                        _build_nameq_subtotal_lines(name_curbal, name_curbaly, name_curbaln)
                    )
                prev_nameq   = nameq
                name_curbal  = 0.0
                name_curbaly = 0.0
                name_curbaln = 0.0

            # ── DETAIL LINE ──────────────────────────────────────────────────
            writer.write_lines([_build_detail_line(row)])

            # Accumulate for subtotals
            name_curbal  += curbal
            name_curbaly += curbaly
            name_curbaln += curbaln

        # ── FLUSH FINAL NAMEQ SUBTOTAL ───────────────────────────────────────
        if prev_nameq is not None and writer is not None:
            writer.write_block(
                _build_nameq_subtotal_lines(name_curbal, name_curbaly, name_curbaln)
            )


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main() -> None:
    print("=" * 70)
    print("EIBMFDC3 — MONTHLY FD INTEREST RATES EXCEPTION REPORT (CORPORATE)")
    print("=" * 70)
    print(f"\nReport Date : {REPTDT}")
    print(f"Input  File : {INPUT_FD1M}")
    print(f"Output File : {OUTPUT_REPORT}")

    # ── STEP 1: Load and prepare FD master ──────────────────────────────────
    print("\nStep 1: Loading fd1m.sas7bdat and filtering JI='C' ...")
    mgrrat = _load_mgrrat(INPUT_FD1M)
    print(f"  Records after filter: {len(mgrrat):,}")

    # Required columns check (after rename)
    required_cols = {
        "BRANCH", "NAMEQ", "ACCTNO", "DEPID",
        "CURBAL", "DEPTERM", "DEPNDT", "MATNDT",
        "RATE1", "NR", "CURBALN", "CURBALY",
    }
    missing_cols = required_cols - set(mgrrat.columns)
    if missing_cols:
        raise ValueError(
            f"fd1m.sas7bdat is missing required column(s): {', '.join(sorted(missing_cols))}"
        )

    # ── STEP 2: Generate report ──────────────────────────────────────────────
    print("\nStep 2: Generating report ...")
    titles = _get_report_titles(REPTDT)
    _write_report_file(mgrrat, OUTPUT_REPORT, titles)
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
