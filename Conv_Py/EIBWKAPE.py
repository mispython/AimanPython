#!/usr/bin/env python3
"""
Program : EIBWKAPE.py
Purpose : Weekly (Daily) KAPITI Stock Report - Specified & Non-Specified
          RENTAS Securities from Trading Book, for Public Bank Berhad
          (PBB). Produces the variance report between KAPITI and WALKER,
          and the Rev Repo report, then dispatches PBBELQ's per-day
          detail Eligible Liabilities report (DAY A - DAY I).

Dependency:
    %INC PGM(PBBELQ);  -> converted separately as PBBELQ.py
    from PBBELQ import prtel, prteli

Dependency note (PBBELF):
    PBBELQ.py (not this program) uses PBBELF.EL_DEFINITIONS /
    ELI_DEFINITIONS as the EL/ELI item catalogue. This program does not
    reference PBBELF directly, so it is not imported here.

Structural note (vs. EIIWKAPE/PIBELQ):
    In this program, %INC PGM(PBBELQ) is textually included -- and its
    %PRTEL/%PRTELI macro calls fire -- BEFORE the calling program
    rebuilds REP2 with BNMCODG (that rebuild happens AFTER the %INC in
    the original JCL). PBBELQ's own DATA REP6/REP7 steps therefore read
    "SET REP2 REP4" against the *pre-BNMCODG*, filtered REP2/REP4
    already built earlier in this program -- they do NOT re-read
    BNMK.REP2 fresh from disk (unlike PIBELQ, which does re-read it).
    Consequently prtel()/prteli() here take rep2_df/rep4_df as
    parameters instead of resolving their own REP2 path.

============================================================================
PHYSICAL INPUT DATASETS USED BY THIS PROGRAM  (all .sas7bdat, cached to
Parquet on first read per EIBDLN1M.py's chunked-conversion pattern)
============================================================================
1. BNMK REP2   (SAS libref BNMK -> SAP.PBB.DKAPITI.SASDATA)
   File     : rep2<REPTMON><WK>.sas7bdat
   Path     : INPUT_BNMK_REP2_DIR
   Used in  : Step 5 - build REP2 (filtered: UTSTY IN (CB1,CF1,CNT,SAC,
              SMC,ISB) AND UTREF NOT IN (DLG,IDLG) -> DELETE)

2. BNMK REP4   (SAS libref BNMK -> SAP.PBB.DKAPITI.SASDATA)
   File     : rep4<REPTMON><WK>.sas7bdat
   Path     : INPUT_BNMK_REP4_DIR
   Used in  : Step 6 - build REP4 (same UTSTY/UTREF filter as REP2; no
              BNMCODE remap, no REPTYEAR in filename -- unlike EIIWKAPE's
              rep4x file)

3. BNM ELW     (SAS libref BNM  -> SAP.PBB.D&REPTYEAR, via LIBNAME)
   File     : elw<REPTMON><WK>.sas7bdat
   Path     : INPUT_BNM_ELW_DIR
   Used in  : Step 10 - WALW variance source (BNMCODE remap only; no
              BNMS union -- this program's JCL has no BNMS DD)

------------------------------------------------------------------------
NON-FILE / DERIVED INPUTS
------------------------------------------------------------------------
- LOAN.REPTDATE and BNMK.REPTDATE: no reptdate.parquet/.sas7bdat exists.
  Both are derived from REPTDATE.py's get_reptdate_values() (see Steps
  1-2).
- LIBNAME BNM "SAP.PBB.D&REPTYEAR": this dynamically re-points the BNM
  library based on the derived REPTYEAR. Following EIIWKAPE.py's
  convention, INPUT_BNM_ELW_DIR is kept as a static path rather than
  re-derived per REPTYEAR.

------------------------------------------------------------------------
NOT REFERENCED BY THIS PROGRAM (vs. EIIWKAPE/PIBELQ)
------------------------------------------------------------------------
- BNMS.ELSCD is NOT used here (no BNMS DD in this program's JCL).
- ELG.GOLD&REPTMON&NOWK is a REAL physical dataset (SAP.PBB.GOLD.SASDATA,
  DISP=SHR) in this program -- unlike EIIWKAPE, there is no inline
  "DATA ELG.GOLD&REPTMON&NOWK; ELDAY=...;" seed step. PBBELQ.py resolves
  and reads this file itself (see its module docstring).
"""

import gc
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from PBBELQ_AII import prtel, prteli

# NOTE: %INC PGM stated in JCL, but PBB.PROGRAM library holds SAS source
# code (compiled macros), not data - it has no python equivalent to import.

# ============================================================================
# PATH CONFIGURATION (each physical input kept independent)
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII/KAPE")

# # PROD Input
# INPUT_BNMK_REP2_DIR = STG_DIR / "EIB" / "BNMK"                  # bnmk_rep2
# INPUT_BNMK_REP4_DIR = STG_DIR / "EIB" / "BNMK"                  # bnmk_rep4
# INPUT_BNM_ELW_DIR   = STG_DIR / "EII" / "BNM"                   # bnm_elw

# UAT Input
INPUT_BNMK_REP2_DIR = STG_DIR / "EIB/BNMK" / "rep2081.sas7bdat"
INPUT_BNMK_REP4_DIR = STG_DIR / "EIB/BNMK" / "rep4081.sas7bdat"
INPUT_BNM_ELW_DIR   = STG_DIR / "EII/BNM"  / "elw081.sas7bdat"

OUTPUT_DIR      = BASE_DIR / "output" / "EIBWKAPE"
OUTPUT_NSRS_DIR = BASE_DIR / "output" / "EIBWKAPE" / "nsrs"

# Parquet cache directory — shared with PBBELQ.py (same BNMK REP2 /
# BNMK REP4 datasets are read/reused by both, so caching once here
# avoids a duplicate conversion when PBBELQ.py runs in-process)
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBWKAPE"

for _d in (OUTPUT_DIR, OUTPUT_NSRS_DIR, CACHE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# GLOBAL CONFIGURATION
# ============================================================================
CHUNK_ROWS = 500_000
PAGE_SIZE = 60

# ============================================================================
# SFTP CONFIGURATION
# ============================================================================
# RUNSFTP step uploads the report to "FD-BNM REPORTING/PBB/BNM RPTG" on the
# Data Report Repository (DRR) host. Following project convention, paramiko
# is used with credentials resolved via EDW_TRANSFORMATION.get_sftp_info().
# HOST_DESC key for the DRR host is not confirmed against
# ctl_dwh_sftp_info.sas7bdat yet, so the actual transfer call is left as a
# documented placeholder below (see Step 14).
# from EDW_TRANSFORMATION import get_sftp_info
SFTP_REMOTE_DIR = "FD-BNM REPORTING/PBB/BNM RPTG"

# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET
# (identical pattern to EIBDLN1M.py: freshness check via mtime, PyArrow
# ParquetWriter with schema locked on first chunk)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")

    # Read the whole SAS file into a pandas DataFrame
    try:
        df = pd.read_sas(sas_path, encoding="latin1")
    except Exception as e:
        raise RuntimeError(f"Failed to read SAS file {sas_path}: {e}")

    # Convert to PyArrow Table (this preserves the schema even if df is empty)
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Write to Parquet
    writer = pq.ParquetWriter(cache_path, table.schema, compression="snappy")
    writer.write_table(table)
    writer.close()

    print(f"  [{tag}] Done — {len(df):,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    """Resolve <parent>_<stem>.parquet cache under CACHE_DIR, converting if stale."""
    # Include directory name to avoid collisions (e.g., bnm_elw081.parquet vs bnmb_elw081.parquet)
    cache_name = f"{sas_path.parent.name}_{sas_path.stem}.parquet"
    cache_path = CACHE_DIR / cache_name
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh — skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# STEP 1: REPORT DATE  (LOAN.REPTDATE equivalent — no physical file, see
# module docstring "NON-FILE / DERIVED INPUTS")
# DATA REPTDAT1; SET LOAN.REPTDATE; SDESC='PUBLIC BANK BERHAD'; ...
#      CALL SYMPUT('MTHEND',PUT(DAY(REPTDATE),Z2.));
# ============================================================================
print("Step 1: Deriving report date (REPTDAT1)...")

_reptdat1 = get_reptdate_values(year_format="%Y")
SDESC   = "PUBLIC BANK BERHAD"
RDATE   = _reptdat1.reptdate.strftime("%d/%m/%y")   # DDMMYY8.
RYEAR   = _reptdat1.reptdate.strftime("%Y")          # YEAR4.
MTHNAM  = _reptdat1.reptdate.strftime("%B").upper()  # MONNAME.
MTHEND  = f"{_reptdat1.reptdate.day:02d}"            # Z2.

print(f"  SDESC  : {SDESC}")
print(f"  RDATE  : {RDATE}")
print(f"  RYEAR  : {RYEAR}")
print(f"  MTHNAM : {MTHNAM}")
print(f"  MTHEND : {MTHEND}")

# ============================================================================
# STEP 2: WEEK / MONTH DERIVATION  (BNMK.REPTDATE equivalent — no
# physical file, see module docstring)
# DATA REPTDATE; SET BNMK.REPTDATE; MM=MONTH(SXDATE);
#   custom week buckets (1-8='4', 9-15='1', 16-22='2', else='3');
#   IF WK='4' THEN roll back one month (and one year if January).
# ============================================================================
print("\nStep 2: Deriving week/month bucket (REPTDATE)...")

SXDATE = _reptdat1.reptdate
_day = SXDATE.day
if 1 <= _day <= 8:
    WK = "4"
elif 9 <= _day <= 15:
    WK = "1"
elif 16 <= _day <= 22:
    WK = "2"
else:
    WK = "3"

MM = SXDATE.month
if WK == "4":
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
    MM = MM1
    if MM == 12:
        # SXDATE = MDY(1,1,YEAR(SXDATE)) - 1  ->  31-Dec of prior year
        SXDATE = date(SXDATE.year, 1, 1) - timedelta(days=1)

NOWK     = WK                       # CALL SYMPUT('NOWK', WK) -- identical to WK
REPTMON  = f"{MM:02d}"              # Z2.
RPDATE   = SXDATE.strftime("%d/%m/%y")
REPTYEAR = SXDATE.strftime("%Y")    # YEAR4.

print(f"  WK / NOWK : {WK}")
print(f"  REPTMON   : {REPTMON}")
print(f"  REPTYEAR  : {REPTYEAR}")
print(f"  RPDATE    : {RPDATE}")

# LIBNAME BNM "SAP.PBB.D&REPTYEAR" DISP=SHR;
# Following EIIWKAPE.py's convention, INPUT_BNM_ELW_DIR is kept static
# rather than re-derived per REPTYEAR (see module docstring).

# ============================================================================
# STEP 3: OUTPUT FILE NAMES
# SASLIST DSN=SAP.PBB.EIBWKAPD.TEXT (catalogued, no date suffix in local name)
# ============================================================================
OUTPUT_FILE      = OUTPUT_DIR / "EIBWKAPD.txt"
OUTPUT_NSRS_FILE = OUTPUT_NSRS_DIR / "EIBWKAPD.txt"

# PUT //SAP.PBB.EIBWKAPD.TEXT  EIBWKAPD_MTH.TXT   (if NOWK='4')
# PUT //SAP.PBB.EIBWKAPD.TEXT  EIBWKAPD_WK&NOWK..TXT  (otherwise)
SFTP_REMOTE_NAME = "EIBWKAPD_MTH.TXT" if NOWK == "4" else f"EIBWKAPD_WK{NOWK}.TXT"

print(f"  Output file        : {OUTPUT_FILE.name}")
print(f"  NSRS copy          : {OUTPUT_NSRS_FILE.name}")
print(f"  SFTP remote name   : {SFTP_REMOTE_NAME}")

# ============================================================================
# ASA REPORT HELPERS
# ============================================================================
def _new_buf(width: int = 132) -> list:
    return [" "] * width


def _put(buf: list, col: int, text: str) -> None:
    start = col - 1
    for i, ch in enumerate(str(text)):
        if 0 <= start + i < len(buf):
            buf[start + i] = ch


def _line(buf: list, asa: str = " ") -> str:
    return asa + "".join(buf)


def _fmt_comma(value, width: int, decimals: int = 2) -> str:
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    s = f"{v:,.{decimals}f}"
    return s.rjust(width)[:width]


def _title_lines(*titles: str) -> list[str]:
    lines = []
    for i, t in enumerate(titles):
        buf = _new_buf()
        _put(buf, 1, t)
        # lines.append(_line(buf, "1" if i == 0 else " "))
        lines.append(_line(buf, " "))
    return lines


# def _render_pivot_report(
#     df: pl.DataFrame,
#     title_lines: list[str],
#     row_col: str,
#     all_label: str,
#     class_col: str,
#     value_specs: list[tuple],
#     rts: int,
# ) -> list[str]:
#     """
#     Generic emulation of PROC TABULATE: rows = distinct row_col values plus
#     an ALL/grand-total row, columns = distinct class_col values, each
#     showing one or more summed value columns (COMMA-formatted).
#     """
#     # If there is no data, return nothing (no titles, no headers)
#     if df.is_empty():
#         return []

#     lines = list(title_lines)

#     class_vals = sorted(df[class_col].drop_nulls().unique().to_list())
#     hdr = _new_buf()
#     pos = rts + 1
#     col_starts = {}
#     for cv in class_vals:
#         for (col, label, width, dec) in value_specs:
#             col_starts[(cv, col)] = pos
#             seg = cv if len(value_specs) == 1 else f"{cv[:width - len(label) - 1]} {label}"
#             _put(hdr, pos, seg[:width].rjust(width))
#             pos += width
#     lines.append(_line(hdr))
#     lines.append(" " + "-" * (pos - 2))

#     grand = {}
#     row_vals = sorted(df[row_col].drop_nulls().unique().to_list())
#     for rv in row_vals:
#         buf = _new_buf()
#         _put(buf, 1, str(rv)[:rts])
#         sub = df.filter(pl.col(row_col) == rv)
#         for cv in class_vals:
#             cell = sub.filter(pl.col(class_col) == cv)
#             for (col, label, width, dec) in value_specs:
#                 val = float(cell[col].sum()) if len(cell) else 0.0
#                 grand[(cv, col)] = grand.get((cv, col), 0.0) + val
#                 _put(buf, col_starts[(cv, col)], _fmt_comma(val, width, dec))
#         lines.append(_line(buf))

#     buf = _new_buf()
#     _put(buf, 1, all_label[:rts])
#     for cv in class_vals:
#         for (col, label, width, dec) in value_specs:
#             _put(buf, col_starts[(cv, col)], _fmt_comma(grand.get((cv, col), 0.0), width, dec))
#     lines.append(_line(buf))
#     return lines


def _fmt_num_fit(value, width: int, decimals: int = 2) -> str:
    """Right-justify a number to *width*; drop the comma separator (then
    reduce decimals) if the value would otherwise overflow the column,
    so the '|' column border is never pushed out of place."""
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    s = f"{v:,.{decimals}f}"
    if len(s) <= width:
        return s.rjust(width)
    s2 = f"{v:.{decimals}f}"
    if len(s2) <= width:
        return s2.rjust(width)
    for d in range(decimals - 1, -1, -1):
        s3 = f"{v:.{d}f}"
        if len(s3) <= width:
            return s3.rjust(width)
    return "*" * width   # value genuinely does not fit even at 0 decimals


def _render_pivot_table(
    df: pl.DataFrame,
    base_titles: list[str],
    row_col: str,
    all_label: str,
    class_col: str,
    value_specs: list[tuple],   # (col_name, sublabel, width, decimals)
    rts: int,
    row_label: str = "BNMCODE",
) -> list[str]:
    """
    Fully-bordered emulation of PROC TABULATE. Rows = distinct row_col
    values plus an ALL/grand-total row; columns = distinct class_col
    values, each showing one or more summed value columns.

    Paginates at 132 print columns, splitting a class (ELDAY) group
    across pages when it doesn't fit -- matching SAS's own column-fill
    behaviour -- and reprints the titles (prefixed "(Continued)") on
    every subsequent page. label_width = rts - 2 and value columns are
    always 16 wide, matching the original SAS TABULATE output exactly.
    """
    if df.is_empty():
        return []

    class_vals = sorted(df[class_col].drop_nulls().unique().to_list())
    row_vals = sorted(df[row_col].drop_nulls().unique().to_list())
    if not class_vals or not row_vals:
        return []

    value_width = value_specs[0][2]
    label_width = max(rts - 2, 10)

    # Flatten (class, spec) pairs in column order, then fill pages with
    # as many 16-wide columns as fit in 132 -- may split a class group.
    flat_cols = [(cv, spec) for cv in class_vals for spec in value_specs]
    max_area = 132 - label_width - 3
    cols_per_page = max(1, (max_area + 1) // (value_width + 1))
    pages = [flat_cols[i:i + cols_per_page] for i in range(0, len(flat_cols), cols_per_page)]

    sums = (
        df.group_by([row_col, class_col])
        .agg([pl.col(c).sum().alias(c) for (c, _, _, _) in value_specs])
    )
    grand = (
        df.group_by(class_col)
        .agg([pl.col(c).sum().alias(c) for (c, _, _, _) in value_specs])
    )
    sum_lookup = {(r[row_col], r[class_col]): r for r in sums.iter_rows(named=True)}
    grand_lookup = {r[class_col]: r for r in grand.iter_rows(named=True)}

    def cell(row_val, cv, col_name):
        rec = sum_lookup.get((row_val, cv))
        return rec[col_name] if rec else 0.0

    def grand_cell(cv, col_name):
        rec = grand_lookup.get(cv)
        return rec[col_name] if rec else 0.0

    out: list[str] = []

    for page_num, page_cols in enumerate(pages, start=1):
        n = len(page_cols)
        value_area_width = n * value_width + (n - 1)
        total_width = label_width + value_area_width + 3
        blank_label = " " * label_width

        if page_num == 1:
            out.extend(_title_lines(*base_titles))
        else:
            out.append(" ")   # blank filler line between continuation pages
            out.extend(_title_lines("(Continued)", *base_titles))

        border = " " + "-" * total_width
        out.append(border)

        # ELDAY spanning header + its dashed separator (single span, no '+')
        out.append(f" |{blank_label}|{class_col.center(value_area_width)}|")
        out.append(f" |{blank_label}|{'-' * value_area_width}|")

        # Class (day) header row -- one centered label per contiguous run
        class_header_parts = []
        i = 0
        while i < n:
            cv = page_cols[i][0]
            j = i
            while j < n and page_cols[j][0] == cv:
                j += 1
            span_w = (j - i) * value_width + (j - i - 1)
            class_header_parts.append(str(cv).center(span_w))
            i = j
        out.append(f" |{blank_label}|" + "|".join(class_header_parts) + "|")

        sep_plus = "-" * label_width + "+" + "+".join("-" * value_width for _ in range(n))
        out.append(f" |{sep_plus}|")

        # Sub-column header row (KAPITI/WALKER/... or blank)
        sub_parts = [spec[1].center(value_width) for (_, spec) in page_cols]
        out.append(f" |{row_label.ljust(label_width)}|" + "|".join(sub_parts) + "|")
        out.append(f" |{sep_plus}|")

        # Data rows
        for rv in row_vals:
            vals = [
                _fmt_num_fit(cell(rv, cv, spec[0]), value_width, spec[3])
                for (cv, spec) in page_cols
            ]
            out.append(f" |{str(rv).ljust(label_width)[:label_width]}|" + "|".join(vals) + "|")
            out.append(f" |{sep_plus}|")

        # Grand total row(s)
        label_txt = all_label.rstrip()
        if len(label_txt) <= label_width:
            vals = [
                _fmt_num_fit(grand_cell(cv, spec[0]), value_width, spec[3])
                for (cv, spec) in page_cols
            ]
            out.append(f" |{label_txt.ljust(label_width)}|" + "|".join(vals) + "|")
        else:
            words = label_txt.split()
            first_line, second_line = " ".join(words[:-1]), words[-1]
            blank_vals = "|".join(" " * value_width for _ in range(n))
            out.append(f" |{first_line.ljust(label_width)[:label_width]}|{blank_vals}|")
            vals = [
                _fmt_num_fit(grand_cell(cv, spec[0]), value_width, spec[3])
                for (cv, spec) in page_cols
            ]
            out.append(f" |{second_line.ljust(label_width)[:label_width]}|" + "|".join(vals) + "|")

        out.append(border)   # bottom border, identical to top

    return out


def _paginate_simple(lines: list[str], page_size: int, header_lines: list[str]) -> list[str]:
    """
    Paginate a list of lines where only the header lines (titles + column headers)
    need to be repeated on every page. No group reprinting.
    Ensures that the total number of lines per page NEVER exceeds page_size.
    """
    if not lines:
        return []

    # Separate header from data
    header_count = len(header_lines)
    data_lines = lines[header_count:]
    if not data_lines:
        return lines

    result = []
    page_buffer = []
    # Start the first page with the header lines
    page_buffer = header_lines.copy()
    # We will iterate over data lines and add them to page_buffer
    for line in data_lines:
        # Check if adding this line would exceed page_size
        if len(page_buffer) + 1 > page_size:
            # Flush current page
            result.extend(page_buffer)
            # Start a new page with the header and this line
            page_buffer = header_lines.copy()
            page_buffer.append(line)
        else:
            page_buffer.append(line)
    # Flush the last page
    if page_buffer:
        result.extend(page_buffer)
    return result


def _paginate_with_groups(lines: list[str], page_size: int, header_lines: list[str]) -> list[str]:
    """
    Paginate a list of lines that contains groups (FMTNAME printed only on first row).
    On each new page, reprint the header lines, and if the group continues,
    reprint the FMTNAME for that group at the top of the new page.
    Strictly enforces page_size lines per page.
    """
    if not lines:
        return []

    header_count = len(header_lines)
    data_lines = lines[header_count:]
    if not data_lines:
        return lines

    # Pre-scan data_lines to find group starts (lines with non-space at column 1)
    group_starts = []   # list of (index, fmtname)
    current_fmt = None
    for idx, line in enumerate(data_lines):
        if len(line) > 0 and line[0] != ' ':
            current_fmt = line[:7].strip()
            group_starts.append((idx, current_fmt))

    result = []
    page_buffer = header_lines.copy()
    # We'll build pages line by line, remembering the current group for reprint
    current_group_fmt = None
    # We need to know if we are at the start of a page to reprint group if needed
    # We'll keep a flag that indicates we are at the beginning of a page (after header)
    at_page_start = True

    for idx, line in enumerate(data_lines):
        # Check if this line is a group start
        is_group_start = (len(line) > 0 and line[0] != ' ')
        if is_group_start:
            current_group_fmt = line[:7].strip()

        # Check if adding this line would exceed page_size
        if len(page_buffer) + 1 > page_size:
            # Flush current page
            result.extend(page_buffer)
            # Start a new page with header
            page_buffer = header_lines.copy()
            at_page_start = True
            # If this line is NOT a group start and we have a current group, we need to reprint the group header
            if not is_group_start and current_group_fmt is not None:
                # Create a group header line
                buf = _new_buf()
                _put(buf, 1, current_group_fmt)
                group_line = _line(buf, " ")
                # Add the group header to the new page (it counts as one line)
                page_buffer.append(group_line)
            # Now add the current line
            page_buffer.append(line)
            at_page_start = False
        else:
            page_buffer.append(line)
            at_page_start = False

    # Flush last page
    if page_buffer:
        result.extend(page_buffer)
    return result


# ============================================================================
# STEP 4: BUILD REP2 (filtered)
# DATA REP2; SET BNMK.REP2&REPTMON&WK;
#   IF UTSTY IN ('CB1','CF1','CNT','SAC','SMC','ISB') THEN DO;
#       IF UTREF NOT IN ('DLG','IDLG') THEN DELETE;
#   END;
# ============================================================================
print("\nStep 4: Building REP2 (filtered)...")

# # PROD Input
# rep2_sas = INPUT_BNMK_REP2_DIR / f"rep2{REPTMON}{WK}.sas7bdat"

# UAT Input
rep2_sas = INPUT_BNMK_REP2_DIR
rep2_cache = _load_cached(rep2_sas, "BNMK_REP2")

con = duckdb.connect(database=":memory:")
rep2_filtered = con.execute(f"""
    SELECT
        CAST(BNMCODE AS VARCHAR) AS BNMCODE,
        CAST(UTSTY   AS VARCHAR) AS UTSTY,
        CAST(UTREF   AS VARCHAR) AS UTREF,
        CAST(ELDAY   AS VARCHAR) AS ELDAY,
        CAST(AMOUNT  AS DOUBLE)  AS AMOUNT,
        CAST(NETAMT  AS DOUBLE)  AS NETAMT,
        CAST(COSTDED AS DOUBLE)  AS COSTDED
    FROM read_parquet('{rep2_cache.as_posix()}')
    WHERE NOT (
        UTSTY IN ('CB1','CF1','CNT','SAC','SMC','ISB')
        AND UTREF NOT IN ('DLG','IDLG')
    )
""").pl()
con.close()
print(f"  REP2 (filtered) rows: {len(rep2_filtered):,}")

# ============================================================================
# STEP 5: BUILD REP4 (filtered)
# DATA REP4; SET BNMK.REP4&REPTMON&WK;
#   IF UTSTY IN ('CB1','CF1','CNT','SAC','SMC','ISB') THEN DO;
#       IF UTREF NOT IN ('DLG','IDLG') THEN DELETE;
#   END;
# ============================================================================
print("\nStep 5: Building REP4 (filtered)...")

# # PROD Input
# rep4_sas = INPUT_BNMK_REP4_DIR / f"rep4{REPTMON}{WK}.sas7bdat"

# UAT Input
rep4_sas = INPUT_BNMK_REP4_DIR
rep4_cache = _load_cached(rep4_sas, "BNMK_REP4")

con = duckdb.connect(database=":memory:")
rep4_raw = con.execute(f"""
    SELECT
        CAST(BNMCODE AS VARCHAR) AS BNMCODE,
        CAST(UTSTY   AS VARCHAR) AS UTSTY,
        CAST(UTREF   AS VARCHAR) AS UTREF,
        CAST(ELDAY   AS VARCHAR) AS ELDAY,
        CAST(AMOUNT  AS DOUBLE)  AS AMOUNT
    FROM read_parquet('{rep4_cache.as_posix()}')
    WHERE NOT (
        UTSTY IN ('CB1','CF1','CNT','SAC','SMC','ISB')
        AND UTREF NOT IN ('DLG','IDLG')
    )
""").pl()
con.close()

# REP4 lacks NETAMT/COSTDED — add as null so it matches REP2's schema for
# concatenation inside PBBELQ.prtel/prteli (SET REP2 REP4).
rep4_filtered = rep4_raw.with_columns([
    pl.lit(None).cast(pl.Float64).alias("NETAMT"),
    pl.lit(None).cast(pl.Float64).alias("COSTDED"),
])
print(f"  REP4 (filtered) rows: {len(rep4_filtered):,}")

# ============================================================================
# STEP 6: PBBELQ DAILY EL DETAIL REPORTS  (%PRTEL DAYA..DAYH, %PRTELI DAYI)
# Called here (BEFORE REP2 is rebuilt with BNMCODG below) to mirror the
# textual %INC PGM(PBBELQ) position in the original JCL -- see module
# docstring "Structural note". Consumes: BNMK TBL1 / BNMK DCI / BNM ELW /
# ELG GOLD -- all documented in PBBELQ.py's own module docstring.
# ============================================================================
print("\nStep 6: Rendering PBBELQ daily EL detail reports...")

elw1 = None  # loaded lazily inside PBBELQ on first call via build_elw1()

from PBBELQ_AII import build_elw1 as _pbbelq_build_elw1
elw1 = _pbbelq_build_elw1(REPTMON, NOWK)

pbbelq_lines: list[str] = []
for day_code in ("DAYA", "DAYB", "DAYC", "DAYD", "DAYE", "DAYF", "DAYG", "DAYH"):
    pbbelq_lines.extend(
        prtel(
            day_code,
            reptmon=REPTMON, nowk=NOWK, sdesc=SDESC, rdate=RDATE,
            rep2_df=rep2_filtered, rep4_df=rep4_filtered, elw1=elw1,
        )
    )

pbbelq_lines.extend(
    prteli(
        "DAYI",
        reptmon=REPTMON, nowk=NOWK, rdate=RDATE,
        rep2_df=rep2_filtered, rep4_df=rep4_filtered, elw1=elw1,
    )
)

del elw1
gc.collect()

# ============================================================================
# STEP 7: REBUILD REP2  (union with REP4, remap, BNMCODG)
# DATA REP2; SET REP2 REP4;
#   IF BNMCODE='3250000000000Y' THEN DO; UTSTY='REV'; UTREF='REPO ';
#      AMOUNT=NETAMT; END;
#   IF BNMCODE='3752000000000Y' THEN BNMCODE='3552000000000Y';
#   BNMCODG=BNMCODE||'-'||UTSTY||' '||SUBSTR(UTREF,1,5);
# PROC SORT BY BNMCODG omitted -- the pivot renderer groups by row_col
# directly, so a pre-sort is unnecessary.
# ============================================================================
print("\nStep 7: Rebuilding REP2 (union with REP4, remap, BNMCODG)...")

rep2 = pl.concat([rep2_filtered, rep4_filtered.select(rep2_filtered.columns)], how="vertical")

_is_repo = pl.col("BNMCODE") == "3250000000000Y"
rep2 = rep2.with_columns([
    pl.when(_is_repo).then(pl.lit("REV")).otherwise(pl.col("UTSTY")).alias("UTSTY"),
    pl.when(_is_repo).then(pl.lit("REPO ")).otherwise(pl.col("UTREF")).alias("UTREF"),
    pl.when(_is_repo).then(pl.col("NETAMT")).otherwise(pl.col("AMOUNT")).alias("AMOUNT"),
])
rep2 = rep2.with_columns(
    pl.when(pl.col("BNMCODE") == "3752000000000Y")
    .then(pl.lit("3552000000000Y"))
    .otherwise(pl.col("BNMCODE"))
    .alias("BNMCODE")
)
rep2 = rep2.with_columns(
    (pl.col("BNMCODE") + "-" + pl.col("UTSTY") + " " + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
)
print(f"  REP2 rows: {len(rep2):,}")

# ============================================================================
# STEP 8: RENTAS SECURITIES REPORT  (PROC TABULATE #1)
# ============================================================================
# print("\nStep 8: Rendering RENTAS securities report...")

# title1 = _title_lines(
#     f"PUBLIC BANK BERHAD -REPORT DATE {RDATE}",
#     "SPECIFIED & NON-SPECIFIED RENTAS SECURITIES FROM TRADING BOOK",
#     f"(DAILY KAPITI STOCK REPORT) WEEK {WK} {MTHNAM} {RYEAR}",
# )
# # report1_lines = _render_pivot_report(
# #     rep2, title1,
# #     row_col="BNMCODG", all_label="TOTAL RM MARKETABLE SECURITIES",
# #     class_col="ELDAY", value_specs=[("AMOUNT", "", 16, 2)], rts=30,
# # )

# # Get sorted ELDAY values from rep2
# elday_values = sorted(rep2["ELDAY"].drop_nulls().unique().to_list())

# # Split into two pages if more than 6 days (original SAS behaviour)
# MAX_DAYS_PER_PAGE = 6
# if len(elday_values) > MAX_DAYS_PER_PAGE:
#     first_elday = elday_values[:MAX_DAYS_PER_PAGE]
#     second_elday = elday_values[MAX_DAYS_PER_PAGE:]

#     # First part: DAYA–DAYF
#     report1_lines = _render_pivot_report(
#         rep2, title1,
#         row_col="BNMCODG",
#         all_label="TOTAL RM MARKETABLE SECURITIES",
#         class_col="ELDAY",
#         value_specs=[("AMOUNT", "", 16, 2)],
#         rts=30,
#         class_vals=first_elday,
#         page_num=1
#     )

#     # Second part: DAYG–DAYI (with continuation header)
#     cont_title = _title_lines(
#         "(Continued)",
#         "PUBLIC BANK BERHAD -REPORT DATE " + RDATE,
#         "SPECIFIED & NON-SPECIFIED RENTAS SECURITIES FROM TRADING BOOK",
#         "(DAILY KAPITI STOCK REPORT) WEEK " + WK + " " + MTHNAM + " " + RYEAR,
#     )
#     report1_lines += _render_pivot_report(
#         rep2, cont_title,
#         row_col="BNMCODG", all_label="TOTAL RM MARKETABLE SECURITIES",
#         class_col="ELDAY", value_specs=[("AMOUNT", "", 16, 2)],
#         rts=30, class_vals=second_elday, page_num=2
#     )
# else:
#     report1_lines = _render_pivot_report(
#         rep2, title1,
#         row_col="BNMCODG", all_label="TOTAL RM MARKETABLE SECURITIES",
#         class_col="ELDAY", value_specs=[("AMOUNT", "", 16, 2)],
#         rts=30, class_vals=elday_values, page_num=1
#     )

print("\nStep 8: Rendering RENTAS securities report...")
report1_lines = _render_pivot_table(
    rep2,
    base_titles=[
        f"PUBLIC BANK BERHAD -REPORT DATE {RDATE}",
        "SPECIFIED & NON-SPECIFIED RENTAS SECURITIES FROM TRADING BOOK",
        f"(DAILY KAPITI STOCK REPORT) WEEK {WK} {MTHNAM} {RYEAR}",
    ],
    row_col="BNMCODG", all_label="TOTAL RM MARKETABLE SECURITIES",
    class_col="ELDAY", value_specs=[("AMOUNT", "", 16, 2)], rts=30,
)

# ============================================================================
# STEP 9: VARIANCE REPORT  (INPUT: BNM ELW only — no BNMS in this
# program's JCL; PROC SUMMARY REPOV / WALW, MERGE, TABULATE #2)
# DATA WALW; SET BNM.ELW&REPTMON&WK;
#   IF BNMCODE='3250001000000Y' THEN BNMCODE='3250000000000Y';
#   OUTPUT;
#   IF BNMCODE='3551000000000Y' THEN DO; BNMCODE='3552000000000Y'; OUTPUT; END;
# ============================================================================
print("\nStep 9: Building variance report (KAPITI vs WALKER)...")

repov = rep2.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("AMOUNT"))

# # PROD Input
# elw_wk_sas = INPUT_BNM_ELW_DIR / f"elw{REPTMON}{WK}.sas7bdat"

# UAT Input
elw_wk_sas = INPUT_BNM_ELW_DIR
elw_wk_cache = _load_cached(elw_wk_sas, "BNM_ELW_WK")

con = duckdb.connect(database=":memory:")
elw_wk_raw = con.execute(f"""
    SELECT CAST(BNMCODE AS VARCHAR) BNMCODE, CAST(ELDAY AS VARCHAR) ELDAY,
           CAST(AMOUNT AS DOUBLE) AMOUNT
    FROM read_parquet('{elw_wk_cache.as_posix()}')
""").pl()
con.close()

# IF BNMCODE='3250001000000Y' THEN BNMCODE='3250000000000Y'; OUTPUT;
walw_base = elw_wk_raw.with_columns(
    pl.when(pl.col("BNMCODE") == "3250001000000Y")
    .then(pl.lit("3250000000000Y"))
    .otherwise(pl.col("BNMCODE"))
    .alias("BNMCODE")
)
# IF BNMCODE='3551000000000Y' THEN DO; BNMCODE='3552000000000Y'; OUTPUT; END;
# (extra row, keyed off the ORIGINAL bnmcode, in addition to the base OUTPUT above)
walw_extra = elw_wk_raw.filter(pl.col("BNMCODE") == "3551000000000Y").with_columns(
    pl.lit("3552000000000Y").alias("BNMCODE")
)
walw_raw = pl.concat([walw_base, walw_extra], how="vertical")

walw = walw_raw.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum().alias("WALWAMT"))

# MERGE REPOV(IN=A) WALW(IN=B); BY BNMCODE ELDAY; IF A;  -> left join, keep REPOV
variance_df = repov.join(walw, on=["BNMCODE", "ELDAY"], how="left").with_columns(
    pl.col("WALWAMT").fill_null(0.0)
).with_columns(
    (pl.col("AMOUNT") - pl.col("WALWAMT")).alias("VARIANC")
)

# title2 = _title_lines("VARIANCE BETWEEN KAPITI AND WALKER")
# # report2_lines = _render_pivot_report(
# #     variance_df, title2,
# #     row_col="BNMCODE", all_label="TOTAL ",
# #     class_col="ELDAY",
# #     value_specs=[
# #         ("AMOUNT", "KAPITI", 16, 2),
# #         ("WALWAMT", "WALKER", 16, 2),
# #         ("VARIANC", "VARIANCE", 16, 2),
# #     ],
# #     rts=34,
# # )

# report2_lines = _render_pivot_report(
#     variance_df, title2,
#     row_col="BNMCODE",
#     all_label="TOTAL ",
#     class_col="ELDAY",
#     value_specs=[
#         ("AMOUNT", "KAPITI", 16, 2),
#         ("WALWAMT", "WALKER", 16, 2),
#         ("VARIANC", "VARIANCE", 16, 2),
#     ],
#     rts=34,
#     class_vals=elday_values,
#     page_num=1
# )

report2_lines = _render_pivot_table(
    variance_df, ["VARIANCE BETWEEN KAPITI AND WALKER"],
    row_col="BNMCODE", all_label="TOTAL ",
    class_col="ELDAY",
    value_specs=[
        ("AMOUNT", "KAPITI", 16, 2),
        ("WALWAMT", "WALKER", 16, 2),
        ("VARIANC", "VARIANCE", 16, 2),
    ],
    rts=34,
)

# ============================================================================
# STEP 10: REV REPO AT PURCHASE PROCEEDS REPORT  (REP0, TABULATE #3)
# DATA REP2; SET BNMK.REP2&REPTMON&WK; IF UTSTY IN (...) THEN DO
#     IF UTREF NOT IN (...) THEN DELETE; END;
# DATA REP0; SET REP2; IF BNMCODE='3250000000000Y'; BNMCODG=...
# The re-read of BNMK.REP2&REPTMON&WK with the identical filter reproduces
# the same rows already held in rep2_filtered, so it is reused directly
# rather than re-querying the source file.
# ============================================================================
print("\nStep 10: Building Rev Repo report...")

rep0 = rep2_filtered.filter(pl.col("BNMCODE") == "3250000000000Y").with_columns(
    (pl.col("BNMCODE") + "-" + pl.col("UTSTY") + " " + pl.col("UTREF").str.slice(0, 5)).alias("BNMCODG")
)

# title3 = _title_lines("REV REPO AT PURCHASE PROCEEDS")
# # report3_lines = _render_pivot_report(
# #     rep0, title3,
# #     row_col="BNMCODG", all_label="TOTAL ",
# #     class_col="ELDAY",
# #     value_specs=[
# #         ("AMOUNT", "AMOUNT", 16, 2),
# #         ("COSTDED", "(-) PURC PROC.", 16, 2),
# #         ("NETAMT", "MARKET SEC ", 16, 2),
# #     ],
# #     rts=30,
# # )

# report3_lines = _render_pivot_report(
#     rep0, title3,
#     row_col="BNMCODG", all_label="TOTAL ",
#     class_col="ELDAY",
#     value_specs=[
#         ("AMOUNT", "AMOUNT", 16, 2),
#         ("COSTDED", "(-) PURC PROC.", 16, 2),
#         ("NETAMT", "MARKET SEC ", 16, 2),
#     ],
#     rts=30,
#     class_vals=elday_values,
#     page_num=1
# )

report3_lines = _render_pivot_table(
    rep0, ["REV REPO AT PURCHASE PROCEEDS"],
    row_col="BNMCODG", all_label="TOTAL ",
    class_col="ELDAY",
    value_specs=[
        ("AMOUNT", "AMOUNT", 16, 2),
        ("COSTDED", "(-) PURC PROC.", 16, 2),
        ("NETAMT", "MARKET SEC ", 16, 2),
    ],
    rts=30,
)


del repov, elw_wk_raw, walw_base, walw_extra, walw_raw, walw
gc.collect()

# ============================================================================
# STEP 11: PAGINATE (only if needed) AND WRITE OUTPUT
# ============================================================================
print("\nStep 11: Paginating and writing output...")

def extract_header_lines(block_lines: list[str]) -> list[str]:
    """Extract the header lines (titles, column header, dashed line)."""
    header = []
    for line in block_lines:
        header.append(line)
        if "---" in line:  # dashed line indicates end of header
            break
    return header

def paginate_block(block: list[str], page_size: int, with_groups: bool = False) -> list[str]:
    """
    Paginate a single block of lines (either pivot or detail).
    If the block fits on one page, return it unchanged (faster).
    """
    if not block:
        return []
    header = extract_header_lines(block)
    header_count = len(header)
    total_lines = len(block)
    # If the whole block fits on one page, no need to paginate
    if total_lines <= page_size:
        return block

    data_lines = block[header_count:]
    if not data_lines:
        return block

    if with_groups:
        # Use group-aware pagination
        return _paginate_with_groups(block, page_size, header)
    else:
        return _paginate_simple(block, page_size, header)

# # Paginate pivot reports (no groups)
# paginated_reports = []
# for report_lines in [report1_lines, report2_lines, report3_lines]:
#     if report_lines:
#         paginated_reports.extend(paginate_block(report_lines, PAGE_SIZE, with_groups=False))

paginated_reports = report1_lines + report2_lines + report3_lines

# Split pbbelq_lines into day blocks, including the bank name
detail_positions = [
    idx for idx, line in enumerate(pbbelq_lines)
    if "DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR : DAY" in line
]

day_blocks = []
for k, pos in enumerate(detail_positions):
    block_start = pos
    if pos > 0 and pbbelq_lines[pos - 1].strip().startswith("PUBLIC BANK BERHAD"):
        block_start = pos - 1

    if k + 1 < len(detail_positions):
        next_pos = detail_positions[k + 1]
        if next_pos > 0 and pbbelq_lines[next_pos - 1].strip().startswith("PUBLIC BANK BERHAD"):
            block_end = next_pos - 1
        else:
            block_end = next_pos
    else:
        block_end = len(pbbelq_lines)

    day_blocks.append(pbbelq_lines[block_start:block_end])

# Paginate each day block with group reprinting
paginated_pbbelq = []
for block in day_blocks:
    paginated_pbbelq.extend(paginate_block(block, PAGE_SIZE, with_groups=True))

# # Combine all reports
# all_lines = paginated_reports + paginated_pbbelq

# Combine all reports: EL details first, then RENTAS, then Variance
all_lines = paginated_pbbelq + paginated_reports

# Write output
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in all_lines:
        fh.write(ln[:133].ljust(133) + "\n")

print(f"  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(all_lines):,}")

# ============================================================================
# STEP 12: COPY OUTPUT FOR NSRS  (PROC IEBGENER copy of the SASLIST output)
# ============================================================================
print("\nStep 12: Copying output for NSRS...")

with open(OUTPUT_FILE, "rb") as src, open(OUTPUT_NSRS_FILE, "wb") as dst:
    dst.write(src.read())

print(f"  NSRS copy written : {OUTPUT_NSRS_FILE}")

# ============================================================================
# STEP 13: SFTP THE REPORT TO THE DATA REPORT REPOSITORY (DRR)
# RUNSFTP step -- lzopts servercp=..., cd "FD-BNM REPORTING/PBB/BNM RPTG"
# ============================================================================
print("\nStep 13: Transferring output via SFTP...")

# HOST_DESC lookup key against ctl_dwh_sftp_info.sas7bdat is not yet
# confirmed for this DRR destination, so the transfer is documented here
# rather than executed silently against an unverified host entry.
#
# from EDW_TRANSFORMATION import get_sftp_info
# import paramiko
#
# sftp_info = get_sftp_info("DRR")  # HOST_DESC placeholder -- confirm key
# transport = paramiko.Transport((sftp_info.host, sftp_info.port))
# transport.connect(username=sftp_info.username, password=sftp_info.password)
# sftp = paramiko.SFTPClient.from_transport(transport)
# sftp.chdir(SFTP_REMOTE_DIR)
# sftp.put(str(OUTPUT_FILE), SFTP_REMOTE_NAME)
# sftp.close()
# transport.close()

print(f"  Remote dir  : {SFTP_REMOTE_DIR}")
print(f"  Remote name : {SFTP_REMOTE_NAME}")
print("  (SFTP transfer call left commented -- HOST_DESC key unconfirmed)")

del rep2, rep2_filtered, rep4_filtered, rep0, variance_df, pbbelq_lines
gc.collect()

print("\nEIBWKAPE complete.")
