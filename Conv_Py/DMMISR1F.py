#!/usr/bin/env python3
"""
Program : DMMISR1F.py
Purpose : PROC REPORT - Bank's Total Foreign Currency Deposits (RM) report.

          Reads the monthly cumulative FCY-deposit dataset produced by
          EIBDDEPF.py (Python/parquet equivalent of SAS MIS.DYFCY&REPTMON)
          and prints one detail line per REPTDATE, with ASA carriage control.

Original SAS:
    TITLE1 'REPORT ID : DMMISR1F';
    TITLE2 'PUBLIC BANK BERHAD';
    TITLE3 'SALES ADMINISTRATION & SUPPORT';
    TITLE4 "BANK'S TOTAL FOREIGN CURRENCY DEPOSITS (RM)";
    TITLE5 'AS AT ' &XDATE;
    PROC REPORT DATA=MIS.DYFCY&REPTMON NOWD HEADLINE SPLIT='*';
        ... GROUP REPTDATE, COMPUTED RT* columns = ROUND(<base>.SUM,1) ...

    Since the source dataset already holds exactly one row per REPTDATE
    (aggregated upstream by EIBDDEPF), each PROC REPORT GROUP is a
    single-row group, so ROUND(<base>.SUM,1) is simply ROUND(<base>,1) —
    applied here defensively (idempotent, base values are already rounded).

    OPTIONS MISSING=0 -> missing numeric values print as 0 (not blank).
    OPTIONS NOCENTER NODATE NONUMBER -> titles left-justified, no auto
    page/date footer line is produced.

Output file: fixed name (no date suffix), mirroring the JCL DD SASLIST
DSN=SAP.PBB.DMMISR1F.DAILY, which is deleted and recreated (NEW,CATLG) on
every run. output_date.py is therefore intentionally NOT used here since
the output filename carries no date component.
"""

from pathlib import Path

import polars as pl

from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# Monthly cumulative dataset produced by EIBDDEPF.py
MONTHLY_DIR = BASE_DIR / "output" / "EIBDDEPF"

# Report output
OUTPUT_DIR = BASE_DIR / "output" / "DMMISR1F"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "DMMISR1F_DAILY.txt"   # fixed name (DAILY dataset equivalent)

# ============================================================================
# REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate

REPTYEAR = reptdate.strftime("%Y")
REPTMON  = reptdate.strftime("%m")
XDATE    = reptdate.strftime("%d/%m/%Y")     # TITLE5 'AS AT ' &XDATE

print(f"  Report date  : {XDATE}")

MONTHLY_FILE = MONTHLY_DIR / REPTYEAR / f"DYFCY{REPTMON}.parquet"

# ============================================================================
# REPORT LAYOUT CONFIGURATION
# ============================================================================
PAGE_SIZE = 60     # lines per page (SAS default)
LINE_GAP = 2        # spaces between report columns

# (base_column, display_width, header_lines)
# Widths follow PROC REPORT FORMAT=COMMA17. (17) / COMMA14. (14).
COLUMNS = [
    ("TOTFDFY",  17, ["FCY FD", "BALANCE", "(EXCL.FI)"]),
    ("TOTFCFY",  17, ["FCY FD", "FOREIGN", "COMPANIES"]),
    ("TOTFYFD",  14, ["FCY FD", "TOTAL", ""]),
    ("TOFFDFIC", 14, ["FCY FD-C FI", "", ""]),
    ("TOTCAFY",  14, ["FCY CA-C", "BALANCE", "(EXCL.FI)"]),
    ("TOTCAFYI", 14, ["FCY CA-I", "BALANCE", "(EXCL.FI)"]),
    ("TOTFYCA",  14, ["FCY CA", "TOTAL", ""]),
    ("TOTFCY",   14, ["TOTAL", "FCY", "BALANCE"]),
    ("TOFFDIDC", 14, ["FCY FD-C", "INDV", ""]),
    ("TOFFDNDC", 14, ["FCY FD-C", "NON-INDV", ""]),
    ("TOFCAFIC", 14, ["FCY CA-C FI", "", ""]),
    ("TOFCAFII", 14, ["FCY CA-I FI", "", ""]),
    ("TOTFCAFI", 14, ["TOTAL", "FCY CA FI", ""]),
    ("TOTFCYFI", 14, ["TOTAL", "FCY FI", ""]),
    ("TOFCAIDC", 14, ["FCY CA-C", "INDV", ""]),
    ("TOFCANDC", 14, ["FCY CA-C", "NON-INDV", ""]),
    ("TOFCAIDI", 14, ["FCY CA-I", "INDV", ""]),
    ("TOFCANDI", 14, ["FCY CA-I", "NON-INDV", ""]),
    ("TOTFCAID", 14, ["TOTAL", "FCY CA INDV", ""]),
    ("TOTFCAND", 14, ["TOTAL", "FCY CA NON-INDV", ""]),
]
DATE_WIDTH = 8   # DDMMYY8.
DATE_HEADER = ["DATE", "", ""]

# ============================================================================
# LOAD MONTHLY CUMULATIVE DATA
# ============================================================================
print("\nStep 2: Reading monthly cumulative dataset...")

if not MONTHLY_FILE.exists():
    raise FileNotFoundError(
        f"Monthly cumulative dataset not found: {MONTHLY_FILE} "
        "(run EIBDDEPF.py first)"
    )

report_df = pl.read_parquet(MONTHLY_FILE).sort("REPTDATE")
print(f"  Rows to report: {len(report_df):,}")

# ============================================================================
# FORMATTING HELPERS
# ============================================================================
def _fmt_comma(value, width: int) -> str:
    """COMMA<width>. with OPTIONS MISSING=0 -> missing prints as 0."""
    v = 0.0 if value is None else float(value)
    s = f"{int(round(v)):,}"
    return s.rjust(width)


def _fmt_date(d) -> str:
    return d.strftime("%d/%m/%y")   # DDMMYY8.


def _center(text: str, width: int) -> str:
    return text.center(width)[:width] if text else " " * width

# ============================================================================
# HEADER / TITLE BLOCK  (ASA: '1' = new page, ' ' = same page)
# ============================================================================
TITLE_LINES = [
    "REPORT ID : DMMISR1F",
    "PUBLIC BANK BERHAD",
    "SALES ADMINISTRATION & SUPPORT",
    "BANK'S TOTAL FOREIGN CURRENCY DEPOSITS (RM)",
    f"AS AT {XDATE}",
]


def _build_header_block() -> list[str]:
    """Title lines + 3-line multi-part column headers + separator."""
    lines = list(TITLE_LINES)
    lines.append("")

    col_headers = [DATE_HEADER] + [h for _, _, h in COLUMNS]
    col_widths  = [DATE_WIDTH] + [w for _, w, _ in COLUMNS]

    for line_idx in range(3):
        parts = [
            _center(headers[line_idx], width)
            for headers, width in zip(col_headers, col_widths)
        ]
        lines.append((" " * LINE_GAP).join(parts))

    total_width = sum(col_widths) + LINE_GAP * (len(col_widths) - 1)
    lines.append("-" * total_width)
    return lines   # 5 titles + 1 blank + 3 header lines + 1 separator = 10 lines


HEADER_LINES = len(_build_header_block())

# ============================================================================
# STEP 3: GENERATE REPORT
# ============================================================================
print("\nStep 3: Generating report...")

output_lines: list[str] = []
lines_on_page = 0
first_block = True


def _emit_header(new_page: bool) -> None:
    global lines_on_page
    block = _build_header_block()
    asa_first = "1" if new_page else " "
    output_lines.append(asa_first + block[0])
    for ln in block[1:]:
        output_lines.append(" " + ln)
    lines_on_page = HEADER_LINES


for row in report_df.iter_rows(named=True):
    if first_block:
        _emit_header(new_page=True)
        first_block = False
    elif lines_on_page >= PAGE_SIZE:
        _emit_header(new_page=True)

    date_str = _fmt_date(row["REPTDATE"])
    parts = [date_str.rjust(DATE_WIDTH)]
    for col_name, width, _ in COLUMNS:
        parts.append(_fmt_comma(row.get(col_name), width))

    detail_line = (" " * LINE_GAP).join(parts)
    output_lines.append(" " + detail_line)   # ASA ' ' = single space
    lines_on_page += 1

# ============================================================================
# WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")

print("\nReport contents (terminal echo):")
for ln in output_lines:
    print(ln)

print("\nDMMISR1F complete.")
