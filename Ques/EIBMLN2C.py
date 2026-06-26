#!/usr/bin/env python3
"""
Program : EIBMLN2C.py
Purpose : Loan Listing by Construction (SectCode 5001-5999) and
          Real Estate (SectCode 8310) for Non-Individual Customers.
          Produces reports for both Public Bank Berhad (PBB) and Public Islamic Bank Berhad (PIBB).
          Inputs sourced from EIBXLNLC.py outputs (NOTE2 parquet files, biweekly schedule).
          Output is a fixed-width report with ASA carriage control characters.
          RECFM=FBA, LRECL=134, BLKSIZE=13400
"""

from pathlib import Path

import duckdb
import polars as pl

from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# PBB paths  (EIBXLNLC output -> EIBMLN2C input)
PBB_INPUT_DIR   = BASE_DIR / "output/EIBXLNLC/PBB"                 # SAP.PBB.LOANLIST.SASDATA  (NOTE2&REPTMON)
PBB_OUTPUT_PATH = BASE_DIR / "output/EIBMLN2C/PBB_LOANLIS2.txt"    # SAP.PBB.LOANLIS2.COLD

# PIBB paths  (EIBXLNLC output -> EIBMLN2C input)
PIBB_INPUT_DIR   = BASE_DIR / "output/EIBXLNLC/PIBB"               # SAP.PIBB.LOANLIST.SASDATA (NOTE2&REPTMON)
PIBB_OUTPUT_PATH = BASE_DIR / "output/EIBMLN2C/PIBB_LOANLIS2.txt"  # SAP.PIBB.LOANLIS2.COLD

# Output directory auto-creation
PBB_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
PIBB_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT LAYOUT CONSTANTS
# ============================================================================

PAGE_SIZE = 60   # lines per page (default when not specified in OPTIONS)
LRECL     = 134  # RECFM=FBA, LRECL=134

# Subtotal block written after each SECTORCD group (3 lines):
#   dashes line (1) + subtotal line (1) + dashes line (1)
SECTORCD_SUBTOTAL_LINES = 3

# Grand-total block written after each BRANCH (3 lines):
#   dashes (1) + grand total (1) + dashes (1)
BRANCH_GRANDTOTAL_LINES = 3

# ============================================================================
# REPORT DATE
# ============================================================================

reptdate_values = get_reptdate_values()
REPTDATE    = reptdate_values.reptdate
REPTMON     = reptdate_values.reptmon
REPORT_DATE = REPTDATE.strftime('%d/%m/%y')   # DDMMYY8. -> DD/MM/YY

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _safe_float(value) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def _safe_int(value) -> int:
    try:
        return int(value) if value is not None else 0
    except (ValueError, TypeError):
        return 0


def _safe_text(value, length: int) -> str:
    if value is None:
        return ''
    return str(value).strip()[:length]


def _fmt_branch_leading_zeroes(value) -> str:
    """Format branch as zero-padded 3-digit string (Z3. format)."""
    if value is None:
        return '   '
    text = str(value).strip()
    if text.endswith('.0'):
        text = text[:-2]
    digits = ''.join(ch for ch in text if ch.isdigit())
    if digits:
        return digits.zfill(3)[-3:]
    return text[:3].zfill(3)


def _fmt_branch(value) -> str:
    """Format branch without leading zeroes (display label)."""
    if value is None:
        return '   '
    text = str(value).strip()
    if text.endswith('.0'):
        text = text[:-2]
    digits = ''.join(ch for ch in text if ch.isdigit())
    if digits:
        return str(int(digits))
    return text


# ============================================================================
# REPORT TITLE & HEADER BUILDERS
# ============================================================================

def _get_report_titles(is_islamic: bool) -> tuple:
    if is_islamic:
        return (
            'REPORT NO :  LOANLIST          PUBLIC ISLAMIC BANK BERHAD',
            'PROGRAM ID:  EIBMLN2C',
        )
    return (
        'REPORT NO : LOANLIST                      PUBLIC BANK BERHAD',
        'PROGRAM ID: EIBMLN2C',
    )


def _build_title_lines(title1: str, title2: str, report_date: str, branch: str, is_islamic: bool) -> list:
    """Build page title block including branch header line."""
    # TITLE4 differs between PBB and PIBB
    # PBB  SAS source: 'REAL ESTATE (SECTCODE 8310) FOR NON-IND.CUSTOMER  (no closing quote — truncated in source)
    # PIBB SAS source: 'REAL ESTATE (SECTCODE 8310) FOR NON-IND.CUSTOMER';
    title4 = 'REAL ESTATE (SECTCODE 8310) FOR NON-IND.CUSTOMER'
    return [
        f"1{title1}\n",
        f" {title2}\n",
        f" LOAN LISTING BY CONSTRUCTION (SECTCODE 5001-5999) AND"
        f"{' ' * 57}REPORT DATE: {report_date}\n",
        f" {title4}\n",
        f"\n",
        f" BRANCH={branch}\n",
        f"\n",
    ]


def _build_header_lines(is_islamic: bool) -> list:
    """
    Build column header rows (two-line split header) mirroring PROC REPORT SPLIT='*'.
    PBB  DEFINE APPRLIMT: 'APPROVED*LIMIT'   -> two-line header.
    PIBB DEFINE APPRLIMT: 'APPROVE LIMIT'    -> single-line header (PIBB source has no SPLIT).
    """
    if is_islamic:
        apprlimt_h1 = 'APPROVE LIMIT'
        apprlimt_h2 = ''
    else:
        apprlimt_h1 = 'APPROVED'
        apprlimt_h2 = 'LIMIT'

    hdr1 = (
        f"   {'ACCOUNT':>10}  {'':>5}  {'':>24}"
        f"  {apprlimt_h1:>13}"
        f"  {'OUTSTANDING':>13}"
        f"  {'PUR':<4}  {'SEC':<4}  {'CUST':<4}"
        f"  {'ST':<2}  {'INT':>5}  {'COLL':<4}  {'COLL':<4}"
    )
    hdr2 = (
        f"   {'NUMBER':>10}  {'NOTE':>5}  {'CUSTOMER NAME':<24}"
        f"  {apprlimt_h2:>13}"
        f"  {'BALANCE':>13}"
        f"  {'POSE':<4}  {'TOR':<4}  {'CODE':<4}"
        f"  {'CD':<2}  {'RATE':>5}  {'NOTE':<4}  {'COMM':<4}"
    )
    underline = '   ' + '-' * (len(hdr2.rstrip()) - 3)
    return [
        f"{hdr1}\n",
        f"{hdr2}\n",
        f"{underline}\n",
        f"\n",   # HEADSKIP
    ]


# ============================================================================
# DETAIL LINE BUILDER
# ============================================================================

def _build_detail_line(row: dict, show_sectorcd: bool = True) -> str:
    """
    Format one data row.
    SECTORCD is shown on the first row of each group (or first on a new page);
    subsequent rows within the same group leave the SECTORCD cell blank —
    matching EIBMOD1C's show-first-only behaviour (DEFINE SECTORCD / ORDER).
    """
    sectorcd_val = _safe_text(row.get('SECTORCD'), 4) if show_sectorcd else ''

    # CUSTCD: cast float->int->str to avoid "78.0" artefact
    custcd_raw = row.get('CUSTCD')
    try:
        custcd_val = str(int(float(custcd_raw))) if custcd_raw is not None else ''
    except (ValueError, TypeError):
        custcd_val = _safe_text(custcd_raw, 4)

    return (
        f"   {_safe_int(row.get('ACCTNO')):>10}"
        f"  {_safe_int(row.get('NOTENO')):>5}"
        f"  {_safe_text(row.get('NAME'), 24):<24}"
        f"  {_safe_float(row.get('APPRLIMT')):>13.2f}"
        f"  {_safe_float(row.get('BALANCE')):>13.2f}"
        f"  {_safe_text(row.get('FISSPURP'), 4):<4}"
        f"  {sectorcd_val:<4}"
        f"  {custcd_val:<4}"
        f"  {_safe_text(row.get('STATE'), 2):<2}"
        f"  {_safe_float(row.get('INTRATE')):>5.2f}"
        f"  {_safe_text(row.get('LIABCODE'), 4):<4}"
        f"  {_safe_text(row.get('CCOLLTRL'), 4):<4}\n"
    )


# ============================================================================
# SUBTOTAL & GRAND-TOTAL LINE BUILDERS
# ============================================================================

def _write_sectorcd_subtotal(report_file, sectorcd: str, balance_sum: float, is_islamic: bool) -> None:
    """
    Write SECTORCD-level subtotal block (3 lines).

    SAS equivalent (COMPUTE AFTER SECTORCD):
      PBB:
        LINE @025 51*'-';
        LINE @025 'SUBTOTAL FOR SECTOR   '  SECTOR $4.  @063 BALANCE.SUM  13.2;
        LINE @025 51*'-';
      PIBB:
        LINE @025 51*'-';
        LINE @025 'SUBTOTAL FOR SECTOR     '  SECTORCD $4.  @063  BALANCE.SUM  13.2;
        LINE @025 51*'-';

    Note: PBB LINE statement references variable name SECTOR (not SECTORCD),
          giving 3 trailing spaces in the label.
          PIBB references SECTORCD, giving 5 trailing spaces in the label.
    """
    sep_line = ' ' * 24 + '-' * 51
    report_file.write(sep_line + '\n')
    sc_str      = f"{_safe_text(sectorcd, 4):<4}"
    balance_str = f"{balance_sum:>13.2f}"
    if is_islamic:
        # PIBB: 'SUBTOTAL FOR SECTOR     ' (5 spaces after SECTOR)
        subtotal_txt = f"{' ' * 24}{'SUBTOTAL FOR SECTOR     '}{sc_str}"
    else:
        # PBB: 'SUBTOTAL FOR SECTOR   ' (3 spaces after SECTOR)
        subtotal_txt = f"{' ' * 24}{'SUBTOTAL FOR SECTOR   '}{sc_str}"
    # @063 = position 63 in content (1-based); pad subtotal_txt to col 62 then append balance
    line = subtotal_txt.ljust(62) + balance_str
    report_file.write(line + '\n')
    report_file.write(sep_line + '\n')


def _write_branch_grandtotal(report_file, branch, balance_sum: float) -> None:
    """
    Write BRANCH-level grand total block (3 lines).

    SAS equivalent (COMPUTE AFTER BRANCH):
        LINE @025 51*'-';
        LINE @025 'GRAND TOTAL FOR BRANCH   '  BRANCH Z3.  @063 BALANCE.SUM  13.2;
        LINE @025 51*'-';
    """
    sep_line    = ' ' * 24 + '-' * 51
    branch_str  = _fmt_branch_leading_zeroes(branch)
    balance_str = f"{balance_sum:>13.2f}"
    report_file.write(sep_line + '\n')
    grand_txt = f"{' ' * 24}{'GRAND TOTAL FOR BRANCH   '}{branch_str}"
    line = grand_txt.ljust(62) + balance_str
    report_file.write(line + '\n')
    report_file.write(sep_line + '\n')


# ============================================================================
# REPORT FILE WRITER
# ============================================================================

def _write_report_file(
    lnnote_df: pl.DataFrame,
    output_file: Path,
    is_islamic: bool,
    report_date: str,
) -> None:
    """
    Write the full loan listing report for one bank entity.

    Report structure (mirrors SAS PROC REPORT):
      - BY BRANCH grouping: page header printed per branch, branch label shown
      - SECTORCD GROUP (DEFINE SECTORCD / ORDER): SECTORCD shown on first row of group only;
        if a page break occurs, SECTORCD re-shown on first row of that page
      - BREAK AFTER SECTORCD: subtotal block (3 lines)
      - BREAK AFTER BRANCH:   grand total block (3 lines)
    """
    title1, title2 = _get_report_titles(is_islamic)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Sort: BY BRANCH then SECTORCD (preserving PROC REPORT BY/ORDER behaviour)
    df_sorted = lnnote_df.sort(['BRANCH', 'SECTORCD'])
    records   = df_sorted.to_dicts()

    if not records:
        output_file.write_text('')
        return

    with open(output_file, 'w', encoding='utf-8') as report_file:
        add_form_feed = False

        # Group by BRANCH
        branches = df_sorted['BRANCH'].unique(maintain_order=True).to_list()

        for branch in branches:
            branch_records = [r for r in records if r['BRANCH'] == branch]
            branch_str     = _fmt_branch(branch)

            title_lines  = _build_title_lines(title1, title2, report_date, branch_str, is_islamic)
            header_lines = _build_header_lines(is_islamic)

            fixed_header_lines = len(title_lines) + len(header_lines)

            # Pre-compute per-SECTORCD balance sums
            sc_balance_sums = {}
            sc_order        = []
            for r in branch_records:
                sc = r.get('SECTORCD')
                if sc not in sc_balance_sums:
                    sc_balance_sums[sc] = 0.0
                    sc_order.append(sc)
                sc_balance_sums[sc] += _safe_float(r.get('BALANCE'))
            branch_balance_sum = sum(sc_balance_sums.values())

            # Build flat list of row entries with SECTORCD boundary metadata
            flat_rows  = []
            total_sc   = len(sc_order)
            for sc_idx, sc_code in enumerate(sc_order):
                sc_records = [r for r in branch_records if r.get('SECTORCD') == sc_code]
                n          = len(sc_records)
                is_last_sc = (sc_idx == total_sc - 1)
                for row_idx, row in enumerate(sc_records):
                    is_last_in_sc     = (row_idx == n - 1)
                    is_last_in_branch = is_last_sc and is_last_in_sc
                    flat_rows.append({
                        'row'              : row,
                        'sectorcd_code'    : sc_code,
                        'is_last_in_sc'    : is_last_in_sc,
                        'sc_balance'       : sc_balance_sums[sc_code],
                        'is_last_in_branch': is_last_in_branch,
                    })

            total_rows = len(flat_rows)
            row_idx    = 0

            while row_idx < total_rows:
                # ── Simulate page fill to find chunk boundary ─────────────────
                page_lines = fixed_header_lines
                chunk_end  = row_idx

                while chunk_end < total_rows:
                    entry        = flat_rows[chunk_end]
                    lines_needed = 1   # detail row itself
                    if entry['is_last_in_sc']:
                        lines_needed += SECTORCD_SUBTOTAL_LINES
                    if entry['is_last_in_branch']:
                        lines_needed += BRANCH_GRANDTOTAL_LINES

                    if page_lines + lines_needed > PAGE_SIZE:
                        break

                    page_lines += lines_needed
                    chunk_end  += 1

                # Force at least one row per page to avoid infinite loop
                if chunk_end == row_idx:
                    chunk_end = row_idx + 1

                chunk = flat_rows[row_idx:chunk_end]

                # ── Page header ───────────────────────────────────────────────
                if add_form_feed:
                    report_file.write('\f')
                for line in title_lines:
                    report_file.write(line)
                for line in header_lines:
                    report_file.write(line)
                add_form_feed = True

                first_row_on_page = True
                prev_sectorcd     = None

                for entry in chunk:
                    row          = entry['row']
                    sectorcd_code = entry['sectorcd_code']

                    # Show SECTORCD on first row of page OR first row of new group
                    show_sc = first_row_on_page or (sectorcd_code != prev_sectorcd)

                    report_file.write(_build_detail_line(row, show_sectorcd=show_sc))
                    first_row_on_page = False
                    prev_sectorcd     = sectorcd_code

                    if entry['is_last_in_sc']:
                        _write_sectorcd_subtotal(
                            report_file, str(sectorcd_code), entry['sc_balance'], is_islamic
                        )

                    if entry['is_last_in_branch']:
                        _write_branch_grandtotal(
                            report_file, branch, branch_balance_sum
                        )

                row_idx = chunk_end


# ============================================================================
# REPORT GENERATOR (main entry per bank)
# ============================================================================

def generate_loan_listing_report(
    input_dir: Path,
    note2_filename: str,
    output_file: Path,
    is_islamic: bool,
) -> bool:
    """Generate Loan Listing report for one bank entity.

    Args:
        input_dir      : Directory containing the NOTE2 parquet file
        note2_filename : Parquet filename (e.g. LNLC_NOTE2_05.parquet)
        output_file    : Path to output .txt report
        is_islamic     : True for PIBB, False for PBB
    Returns:
        True if successful, False otherwise.
    """
    bank_label = 'Public Islamic Bank Berhad (PIBB)' if is_islamic else 'Public Bank Berhad (PBB)'
    print(f"\n{'=' * 70}")
    print(f"Generating Loan Listing Report — {bank_label}")
    print(f"{'=' * 70}")
    print(f"Report Date : {REPORT_DATE}")

    note2_path = input_dir / note2_filename
    print(f"Input       : {note2_path}")

    try:
        # DATA LNNOTE2: SET LNLC.NOTE2&REPTMON  /  LNLCI.NOTE2&REPTMON
        con = duckdb.connect()
        lnnote2_df = con.execute(
            f"SELECT * FROM read_parquet('{note2_path}')"
        ).pl()
        con.close()

        print(f"Rows loaded : {len(lnnote2_df):,}")

        _write_report_file(lnnote2_df, output_file, is_islamic, REPORT_DATE)

        print(f"Report saved: {output_file}")

        print(f"\n{'=' * 20} PREVIEW: {output_file.name} {'=' * 20}\n")
        with open(output_file, 'r', encoding='utf-8') as f:
            print(f.read())
        print(f"{'=' * 20} END PREVIEW {'=' * 20}\n")

        return True

    except Exception as exc:
        print(f"\n[ERROR] Report generation failed for {output_file.name}: {type(exc).__name__}: {exc}")
        return False


# ============================================================================
# MAIN EXECUTION
# ============================================================================

print('=' * 70)
print('LOAN LISTING BY CONSTRUCTION & REAL ESTATE — REPORT GENERATION')
print('=' * 70)
print(f"Report Date : {REPTDATE}  RDATE={REPORT_DATE}  REPTMON={REPTMON}")

results = {}

# ============================================================================
# PART 1: PUBLIC BANK BERHAD (PBB)
# DATA LNNOTE2: SET LNLC.NOTE2&REPTMON
# ============================================================================
results['PBB'] = generate_loan_listing_report(
    input_dir      = PBB_INPUT_DIR,
    note2_filename = f"LNLC_NOTE2_{REPTMON}.parquet",
    output_file    = PBB_OUTPUT_PATH,
    is_islamic     = False,
)

# ============================================================================
# FOR PIBB
# DATA LNNOTE2: SET LNLCI.NOTE2&REPTMON
# ============================================================================
results['PIBB'] = generate_loan_listing_report(
    input_dir      = PIBB_INPUT_DIR,
    note2_filename = f"LNLCI_NOTE2_{REPTMON}.parquet",
    output_file    = PIBB_OUTPUT_PATH,
    is_islamic     = True,
)

# ============================================================================
# SUMMARY
# ============================================================================

print('\n' + '=' * 70)
print('GENERATED REPORTS:')
print('=' * 70)

if results['PBB']:
    print(f"  1. Public Bank Loan Listing (Construction/RE)          : {PBB_OUTPUT_PATH}")
else:
    print(f"  1. Public Bank Loan Listing (Construction/RE)          : [FAILED]")

if results['PIBB']:
    print(f"  2. Public Islamic Bank Loan Listing (Construction/RE)  : {PIBB_OUTPUT_PATH}")
else:
    print(f"  2. Public Islamic Bank Loan Listing (Construction/RE)  : [FAILED]")

if all(results.values()):
    print('\nREPORT GENERATION COMPLETE')
else:
    print('\nREPORT GENERATION COMPLETED WITH ERRORS — review output above.')
