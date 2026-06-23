#!/usr/bin/env python3
"""
Program : EIBMLN1C.py
Purpose : Loan Listing by FISS Purpose Code (for all CustCodes)
          Produces reports for both Public Bank Berhad (PBB) and Public Islamic Bank Berhad (PIBB).
          Inputs sourced from EIBXLNLC.py outputs (NOTE1 parquet files, biweekly schedule).
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

# PBB paths  (EIBXLNLC output -> EIBMLN1C input)
PBB_INPUT_DIR   = BASE_DIR / "output/EIBXLNLC/PBB"                 # SAP.PBB.LOANLIST.SASDATA  (NOTE1&REPTMON)
PBB_OUTPUT_PATH = BASE_DIR / "output/EIBMLN1C/pbb_loanlis1.txt"    # SAP.PBB.LOANLIS1.COLD

# PIBB paths  (EIBXLNLC output -> EIBMLN1C input)
PIBB_INPUT_DIR   = BASE_DIR / "output/EIBXLNLC/PIBB"               # SAP.PIBB.LOANLIST.SASDATA (NOTE1&REPTMON)
PIBB_OUTPUT_PATH = BASE_DIR / "output/EIBMLN1C/pibb_loanlis1.txt"  # SAP.PIBB.LOANLIS1.COLD

# Output directory auto-creation
PBB_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
PIBB_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT LAYOUT CONSTANTS
# ============================================================================

PAGE_SIZE = 60   # lines per page (default when not specified in OPTIONS)
LRECL     = 134  # RECFM=FBA, LRECL=134

# Subtotal block written after each FISSPURP group (3 lines):
#   dashes line (1) + subtotal line (1) + dashes line (1)
FISSPURP_SUBTOTAL_LINES = 3

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
            'PROGRAM ID:  EIBMLN1C',
        )
    return (
        'REPORT NO :  LOANLIST                         PUBLIC BANK BERHAD',
        'PROGRAM ID:  EIBMLN1C',
    )


def _build_title_lines(title1: str, title2: str, report_date: str, branch: str, is_islamic: bool) -> list:
    """Build page title block including branch header line."""
    title4 = '**' if is_islamic else '..'
    return [
        f"1{title1}\n",
        f" {title2}\n",
        f" LOAN LISTING BY FISS PURPOSE CODE (FOR ALL CUSTCODES)"
        f"                                      REPORT DATE: {report_date}\n",
        f" {title4}\n",
        f"\n",
        f" BRANCH={branch}\n",
        f"\n",
    ]


def _build_header_lines(is_islamic: bool) -> list:
    """
    Build column header rows (two-line split header) mirroring PROC REPORT SPLIT='*'.
    PBB  uses 'APPROVED / LIMIT'  for APPRLIMT.
    PIBB uses 'APPROVE LIMIT / ' (single-line label, blank second line).
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

def _build_detail_line(row: dict, show_fisspurp: bool = True) -> str:
    """
    Format one data row.
    FISSPURP is shown on the first row of each group (or first on a new page);
    subsequent rows within the same group leave the FISSPURP cell blank —
    matching EIBMOD1C's show-first-only behaviour.
    """
    fisspurp_val = _safe_text(row.get('FISSPURP'), 4) if show_fisspurp else ''

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
        f"  {fisspurp_val:<4}"
        f"  {_safe_text(row.get('SECTORCD'), 4):<4}"
        f"  {custcd_val:<4}"
        f"  {_safe_text(row.get('STATE'), 2):<2}"
        f"  {_safe_float(row.get('INTRATE')):>5.2f}"
        f"  {_safe_text(row.get('LIABCODE'), 4):<4}"
        f"  {_safe_text(row.get('CCOLLTRL'), 4):<4}\n"
    )


# ============================================================================
# SUBTOTAL & GRAND-TOTAL LINE BUILDERS
# ============================================================================

def _write_fisspurp_subtotal(report_file, fisspurp: str, balance_sum: float) -> None:
    """
    Write FISSPURP-level subtotal block (3 lines).

    SAS equivalent (COMPUTE AFTER FISSPURP):
        LINE @025 51*'-';
        LINE @025 'SUBTOTAL FOR FISS PURPOSE   '  FISSPURP $4.  @063 BALANCE.SUM  13.2;
        LINE @025 51*'-';
    """
    sep_line = ' ' * 24 + '-' * 51
    report_file.write(sep_line + '\n')
    fp_str       = f"{_safe_text(fisspurp, 4):<4}"
    balance_str  = f"{balance_sum:>13.2f}"
    subtotal_txt = f"{' ' * 24}{'SUBTOTAL FOR FISS PURPOSE   '}{fp_str}"
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
    lnnote_df,
    output_file: Path,
    is_islamic: bool,
    report_date: str,
) -> None:
    """
    Write the full loan listing report for one bank entity.

    Report structure (mirrors SAS PROC REPORT):
      - BY BRANCH grouping: page header printed per branch, branch label shown
      - FISSPURP GROUP: FISSPURP shown on first row of group only;
        if a page break occurs, FISSPURP re-shown on first row of that page
      - BREAK AFTER FISSPURP: subtotal block (3 lines)
      - BREAK AFTER BRANCH:   grand total block (3 lines)
    """
    title1, title2 = _get_report_titles(is_islamic)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Convert Polars DataFrame to list of dicts for row iteration
    # Sort: BY BRANCH then FISSPURP (preserving PROC REPORT BY/ORDER behaviour)
    df_sorted = lnnote_df.sort(['BRANCH', 'FISSPURP'])
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

            # Pre-compute per-FISSPURP balance sums
            fp_balance_sums = {}
            fp_order        = []
            for r in branch_records:
                fp = r.get('FISSPURP')
                if fp not in fp_balance_sums:
                    fp_balance_sums[fp] = 0.0
                    fp_order.append(fp)
                fp_balance_sums[fp] += _safe_float(r.get('BALANCE'))
            branch_balance_sum = sum(fp_balance_sums.values())

            # Build flat list of row entries with FISSPURP boundary metadata
            flat_rows      = []
            total_fp       = len(fp_order)
            for fp_idx, fp_code in enumerate(fp_order):
                fp_records = [r for r in branch_records if r.get('FISSPURP') == fp_code]
                n          = len(fp_records)
                is_last_fp = (fp_idx == total_fp - 1)
                for row_idx, row in enumerate(fp_records):
                    is_last_in_fp     = (row_idx == n - 1)
                    is_last_in_branch = is_last_fp and is_last_in_fp
                    flat_rows.append({
                        'row'              : row,
                        'fisspurp_code'    : fp_code,
                        'is_last_in_fp'    : is_last_in_fp,
                        'fp_balance'       : fp_balance_sums[fp_code],
                        'is_last_in_branch': is_last_in_branch,
                    })

            total_rows = len(flat_rows)
            row_idx    = 0

            while row_idx < total_rows:
                # ── Simulate page fill to find chunk boundary ─────────────────
                page_lines = fixed_header_lines
                chunk_end  = row_idx

                while chunk_end < total_rows:
                    entry         = flat_rows[chunk_end]
                    lines_needed  = 1   # detail row itself
                    if entry['is_last_in_fp']:
                        lines_needed += FISSPURP_SUBTOTAL_LINES
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
                prev_fisspurp     = None

                for entry in chunk:
                    row           = entry['row']
                    fisspurp_code = entry['fisspurp_code']

                    # Show FISSPURP on first row of page OR first row of new group
                    show_fp = first_row_on_page or (fisspurp_code != prev_fisspurp)

                    report_file.write(_build_detail_line(row, show_fisspurp=show_fp))
                    first_row_on_page = False
                    prev_fisspurp     = fisspurp_code

                    if entry['is_last_in_fp']:
                        _write_fisspurp_subtotal(
                            report_file, str(fisspurp_code), entry['fp_balance']
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
    note1_filename: str,
    output_file: Path,
    is_islamic: bool,
) -> bool:
    """Generate Loan Listing report for one bank entity.

    Args:
        input_dir      : Directory containing the NOTE1 parquet file
        note1_filename : Parquet filename (e.g. LNLC_NOTE1_05.parquet)
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

    note1_path = input_dir / note1_filename
    print(f"Input       : {note1_path}")

    try:
        # DATA LNNOTE1: SET LNLC.NOTE1&REPTMON  /  LNLCI.NOTE1&REPTMON
        con = duckdb.connect()
        lnnote1_df = con.execute(
            f"SELECT * FROM read_parquet('{note1_path}')"
        ).pl()
        con.close()

        print(f"Rows loaded : {len(lnnote1_df):,}")

        _write_report_file(lnnote1_df, output_file, is_islamic, REPORT_DATE)

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
print('LOAN LISTING BY FISS PURPOSE CODE — REPORT GENERATION')
print('=' * 70)
print(f"Report Date : {REPTDATE}  RDATE={REPORT_DATE}  REPTMON={REPTMON}")

results = {}

# ============================================================================
# PART 1: PUBLIC BANK BERHAD (PBB)
# DATA LNNOTE1: SET LNLC.NOTE1&REPTMON
# ============================================================================
results['PBB'] = generate_loan_listing_report(
    input_dir      = PBB_INPUT_DIR,
    note1_filename = f"LNLC_NOTE1_{REPTMON}.parquet",
    output_file    = PBB_OUTPUT_PATH,
    is_islamic     = False,
)

# ============================================================================
# FOR PIBB
# DATA LNNOTE1: SET LNLCI.NOTE1&REPTMON
# ============================================================================
results['PIBB'] = generate_loan_listing_report(
    input_dir      = PIBB_INPUT_DIR,
    note1_filename = f"LNLCI_NOTE1_{REPTMON}.parquet",
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
    print(f"  1. Public Bank Loan Listing          : {PBB_OUTPUT_PATH}")
else:
    print(f"  1. Public Bank Loan Listing          : [FAILED]")

if results['PIBB']:
    print(f"  2. Public Islamic Bank Loan Listing  : {PIBB_OUTPUT_PATH}")
else:
    print(f"  2. Public Islamic Bank Loan Listing  : [FAILED]")

if all(results.values()):
    print('\nREPORT GENERATION COMPLETE')
else:
    print('\nREPORT GENERATION COMPLETED WITH ERRORS — review output above.')
