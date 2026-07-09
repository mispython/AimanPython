#!/usr/bin/env python3
"""
Program  : EIWIBT1C
Purpose  : Undrawn Trade Bills by Collaterals with Remaining Maturity (PIBB)
Converted from SAS/JCL to Python.

Source references (SAS DDs):
  BNM       -> SAP.IBT.SASDATA            (unused directly; REPTDATE replaced by REPTDATE.py)
  BNMDAILY  -> SAP.IBT.SASDATA.DAILY(0)    -> IBTDTL detail file (ibtdtl*.sas7bdat)
  BTCOLL    -> SAP.PIBB.MNICOL(0)          -> NOT referenced by any DATA/PROC step in the
                                               original source; kept unused here as well
                                               (FLAG-01: verify with business/data team
                                               whether this DD was meant to be used).
  PGM       -> SAP.BNM.PROGRAM             -> library housing the %INC'd PBBLNFMT member;
                                               replaced by a direct Python import.
  EIWIBT1C  -> SAP.PIBB.EIWIBT1C.TEXT      -> output text report (RECFM=FB, LRECL=150)

Dependency: %INC PGM(PBBLNFMT)
  Only $BTPROD. and $BTPRODI. formats are referenced by this program
  (format_btprod / format_btprodi). No other PBBLNFMT format/product-list
  is used here, so only those two functions are imported.

Schema assumptions (FLAG-01, unverified - original copybook/DDL not provided):
  IBTDTL  (detail) : BRANCH, ACCTNO, TRANSREF, LIABCODE, DIRCTIND, OUTSTAND,
                      ISSDTE, EXPRDATE
  IBTMAST (master) : ACCTNO, SUBACCT, LIABCODE
                      (assumed NOT to carry BRANCH/PRODCD/OUTSTAND - those are
                      supplied purely from the BTRADIA summary on merge, matching
                      SAS's last-dataset-wins MERGE semantics.)

PROC TABULATE semantics (Report 1):
  TABLE (BRANCH ALL='GRAND TOTAL'), (BNMCODE=' ' ALL='TOTAL'),
        (TYP=' ' ALL='TOTAL')*OUTSTAND=' '*SUM=' ' / BOX='BNMCODE' RTS=8;
  Three comma-separated dimensions => PAGE = BRANCH, ROW = BNMCODE, COLUMN = TYP.
  Each page only contains the BNMCODE/TYP combinations that actually occur in
  the data for that BRANCH (no invented zero rows/columns), plus a final
  'GRAND TOTAL' page (the BRANCH ALL= level) summed across all branches.

FLAG-01 (unverifiable without an actual SAS run): exact column position of the
OPTIONS NUMBER page-number digit on TITLE1. The source has no OPTIONS
LINESIZE=, so LINESIZE=132 (the SAS default) is assumed and the digit is
right-justified to end at column 132 of the printed line, before the 150-byte
LRECL padding is applied.
"""

from pathlib import Path
from datetime import date, datetime
from typing import Optional

import pandas as pd
import polars as pl

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from PBBLNFMT import format_btprod, format_btprodi

# ============================================================
# PATH CONFIGURATION
# ============================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR = BASE_DIR / "input" / "prod"
OUTPUT_DIR = BASE_DIR / "output" / "EIWIBT1C"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

IBTMAST_PREFIX = "ibtmast"   # ibtmastXXXXX.sas7bdat -> MM W YY
IBTDTL_PREFIX = "ibtdtl"     # ibtdtlXXXXXX.sas7bdat  -> YY MM DD

PAGE_LENGTH = 60
LRECL = 150
LINESIZE = 132          # FLAG-01: assumed default (no OPTIONS LINESIZE= in source)
LABEL_WIDTH = 6         # BOX='BNMCODE' row-label column width (observed)
VALUE_WIDTH = 13        # matches FORMAT=13.2


# ============================================================
# LOCAL PROC FORMAT EQUIVALENTS (program-specific, not in PBBLNFMT)
# ============================================================
def format_typfmt(typ: int) -> str:
    """VALUE TYPFMT"""
    mapping = {
        1: 'PBB OWN FIXED DEPOSIT, 30308 (0%)',
        2: 'DISCOUNT HOUSE/CAGAMAS, 30314 (10%)',
        3: 'FIN INST FIXED DEPOSIT/GUARANTEE, 30332 (20%)',
        4: 'STATUTORY BODIES, 30336 (20%)',
        5: 'FIRST CHARGE, 30342 (50%)',
        6: 'SHARES/UNIT TRUSTS, 30360 (100%)',
        7: 'OTHERS, 30360 (100%)',
    }
    return mapping.get(typ, '')


def format_remfmt(months: float) -> str:
    """VALUE REMFMT (detailed remaining-maturity buckets)"""
    if months <= 0.255:
        return 'UP TO 1 WK     '
    elif months <= 1:
        return '>1 WK - 1 MTH  '
    elif months <= 3:
        return '>1 MTH - 3 MTHS'
    elif months <= 6:
        return '>3 - 6 MTHS    '
    elif months <= 12:
        return '>6 MTHS - 1 YR '
    return '>1 YEAR        '


def format_remfmts(months: float) -> str:
    """VALUE REMFMTS (yearly remaining-maturity bucket)"""
    return '<1 YEAR        ' if months <= 12 else '>1 YEAR        '


REMFMT_ORDER = ['UP TO 1 WK', '>1 WK - 1 MTH', '>1 MTH - 3 MTHS',
                '>3 - 6 MTHS', '>6 MTHS - 1 YR', '>1 YEAR']
REMFMTS_ORDER = ['<1 YEAR', '>1 YEAR']
TYPFMT_ORDER = [1, 2, 3, 4, 5, 6, 7]


# ============================================================
# %COLL MACRO EQUIVALENT
# ============================================================
def classify_collateral(liabcode: str) -> int:
    liabcode = (liabcode or '').strip()
    if liabcode in ('007', '012', '013', '014', '021', '024', '048', '049'):
        return 1
    elif liabcode in ('017', '026', '029'):
        return 2
    elif liabcode in ('006', '011', '016', '030', '018', '027', '003'):
        return 3
    elif liabcode == '025':
        return 4
    elif liabcode == '050':
        return 5
    elif liabcode in ('015', '008', '042'):
        return 6
    return 7


# ============================================================
# PRODCD RESOLUTION (DIRCTIND-driven $BTPROD./$BTPRODI. lookup)
# ============================================================
def resolve_prodcd(liabcode: str, dirctind: str) -> Optional[str]:
    liabcode = (liabcode or '').strip()
    dirctind = (dirctind or '').strip()
    if liabcode != '':
        if dirctind == 'D':
            return format_btprod(liabcode)
        elif dirctind == 'I':
            return format_btprodi(liabcode)
    return None


def compute_origmt(issdte, exprdate) -> str:
    """ORIGMT = '20' default, '10' if EXPRDATE - ISSDTE < 366 days."""
    if issdte is None or exprdate is None:
        return '20'
    issdte = _as_date(issdte)
    exprdate = _as_date(exprdate)
    days = (exprdate - issdte).days
    return '10' if days < 366 else '20'


def _as_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


# ============================================================
# %DCLVAR / %REMMTH MACRO EQUIVALENT
# ============================================================
def days_in_report_month(rptyear: int, rptmonth: int) -> int:
    """RD1-RD12 retain 31, RD4/RD6/RD9/RD11=30, RD2=28 (29 if RPYR leap)."""
    if rptmonth == 2:
        return 29 if rptyear % 4 == 0 else 28
    if rptmonth in (4, 6, 9, 11):
        return 30
    return 31


def compute_remaining_maturity(matdt: Optional[date], rptdate: date) -> float:
    """Replicates %REMMTH. REMMTH = REMY*12 + REMM + REMD/RPDAYS(RPMTH)."""
    if matdt is None:
        return 0.0

    rpyr = rptdate.year
    rpmth = rptdate.month
    rpday = rptdate.day
    rpdays_rpmth = days_in_report_month(rpyr, rpmth)

    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    # NOTE (FLAG-01): the original SAS computes MD2 (leap check on MDYR) here,
    # but never actually references the MDDAYS array afterwards - only
    # RPDAYS(RPMTH) is used below. Preserved as a documented no-op for fidelity.
    # if mdmth == 2:
    #     _md2 = 29 if mdyr % 4 == 0 else 28  # unused, mirrors SAS dead code

    if mdday > rpdays_rpmth:
        mdday = rpdays_rpmth

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / rpdays_rpmth


# ============================================================
# INPUT LOADERS
# ============================================================
def load_ibtdtl() -> pl.DataFrame:
    latest_file = get_latest_file(INPUT_DIR, prefix=IBTDTL_PREFIX)
    df = pd.read_sas(latest_file, encoding='latin1')
    return pl.from_pandas(df)


def load_ibtmast() -> pl.DataFrame:
    latest_file = get_latest_file(INPUT_DIR, prefix=IBTMAST_PREFIX)
    df = pd.read_sas(latest_file, encoding='latin1')
    return pl.from_pandas(df)


# ============================================================
# DATA STEP EQUIVALENTS
# ============================================================
def build_btradi(ibtdtl: pl.DataFrame) -> pl.DataFrame:
    """DATA BTRADI (both steps): PRODCD assignment + ORIGMT derivation.
    BRANCH is cast to Int64 here so it never renders with a trailing '.0'
    (pd.read_sas returns all SAS numerics as Float64)."""
    df = ibtdtl.with_columns([
        pl.col('LIABCODE').cast(pl.Utf8).str.strip_chars(),
        pl.col('DIRCTIND').cast(pl.Utf8).str.strip_chars(),
        pl.col('BRANCH').cast(pl.Int64),
    ])

    prodcd_vals: list[Optional[str]] = []
    origmt_vals: list[str] = []
    for row in df.iter_rows(named=True):
        prodcd_vals.append(resolve_prodcd(row['LIABCODE'], row['DIRCTIND']))
        origmt_vals.append(compute_origmt(row.get('ISSDTE'), row.get('EXPRDATE')))

    return df.with_columns([
        pl.Series('PRODCD', prodcd_vals),
        pl.Series('ORIGMT', origmt_vals),
    ])


def build_btradia(btradi: pl.DataFrame) -> pl.DataFrame:
    """PROC SUMMARY NWAY MISSING CLASS BRANCH ACCTNO PRODCD VAR OUTSTAND SUM=."""
    return (
        btradi
        .group_by(['BRANCH', 'ACCTNO', 'PRODCD', 'LIABCODE'])
        .agg(pl.col('OUTSTAND').sum().alias('OUTSTAND'))
    )


def build_btradm(ibtmast: pl.DataFrame) -> pl.DataFrame:
    """PROC SORT ... OUT=BTRADM NODUPKEYS BY ACCTNO WHERE SUBACCT='OV'."""
    return (
        ibtmast
        .with_columns([
            pl.col('SUBACCT').cast(pl.Utf8).str.strip_chars(),
        ])
        .filter(pl.col('SUBACCT') == 'OV')
        .sort('ACCTNO')
        .unique(subset=['ACCTNO'], keep='first')
    )


def build_btrade(btradm: pl.DataFrame, btradia: pl.DataFrame) -> pl.DataFrame:
    """DATA BTRADE: MERGE BTRADM(IN=A) BTRADIA; BY ACCTNO; IF A;
    Left join keeping all master rows; BTRADIA supplies BRANCH/PRODCD/OUTSTAND
    (last-dataset-wins semantics, since BTRADM carries none of these columns)."""
    return btradm.join(btradia, on='ACCTNO', how='left')


def build_loan(btrade: pl.DataFrame) -> pl.DataFrame:
    """DATA LOAN: %COLL; BNMCODE = PRODCD;
    PROC SORT DATA=LOAN; BY BRANCH; WHERE BNMCODE NE ' ';
    PROC SORT DATA=LOAN; BY ACCTNO;
    The two PROC SORTs (no OUT=) resort LOAN in place; the WHERE on the first
    one permanently drops blank-BNMCODE rows before Report 1 is built."""
    typ_vals = [classify_collateral(v) for v in btrade['LIABCODE'].to_list()]
    loan = btrade.with_columns([
        pl.Series('TYP', typ_vals),
        pl.col('PRODCD').alias('BNMCODE'),
    ])
    return loan.filter(
        pl.col('BNMCODE').is_not_null() & (pl.col('BNMCODE').str.strip_chars() != '')
    )


def build_loan2(btradi: pl.DataFrame, rptdate: date) -> pl.DataFrame:
    """DATA LOAN2(KEEP=BRANCH BNMCODE TYP REMMTH OUTSTAND LIABCODE
                        ISSDTE EXPRDATE ACCTNO);
    NOTE (FLAG-01): TYP is listed in the original KEEP= but is never assigned
    in this SAS DATA step (only DATA LOAN computes TYP via %COLL). This is
    preserved as a missing/None column for structural fidelity; it has no
    effect since Reports 2 and 3 only use BNMCODE and REMMTH."""
    remmth_vals = [
        compute_remaining_maturity(_as_date(row.get('EXPRDATE')), rptdate)
        for row in btradi.iter_rows(named=True)
    ]

    df = btradi.with_columns([
        pl.col('PRODCD').alias('BNMCODE'),
        pl.Series('REMMTH', remmth_vals),
        pl.lit(None).alias('TYP'),
    ])

    df = df.filter(
        pl.col('BNMCODE').is_not_null() & (pl.col('BNMCODE').str.strip_chars() != '')
    )
    return df.select(['BRANCH', 'BNMCODE', 'TYP', 'REMMTH', 'OUTSTAND',
                       'LIABCODE', 'ISSDTE', 'EXPRDATE', 'ACCTNO'])


# ============================================================
# TABULATE-STYLE BOX RENDERING
# ============================================================
def _center(text: str, width: int) -> str:
    if len(text) >= width:
        return text[:width]
    pad = width - len(text)
    left = pad // 2
    right = pad - left
    return ' ' * left + text + ' ' * right


def _wrap_words(label: str, width: int) -> tuple[str, str]:
    """Split a label onto (at most) two physical lines within `width`,
    breaking on the last word boundary that fits; hard-hyphenates a single
    over-long word when there is no boundary to break on."""
    label = label.strip()
    if len(label) <= width:
        return '', label
    words = label.split()
    if len(words) == 1:
        cut = width - 1
        return label[:cut] + '-', label[cut:]
    line1_words: list[str] = []
    cur_len = 0
    for w in words:
        added_len = len(w) if not line1_words else cur_len + 1 + len(w)
        if added_len <= width:
            line1_words.append(w)
            cur_len = added_len
        else:
            break
    remaining = words[len(line1_words):]
    if not remaining:
        remaining = [line1_words.pop()]
    return ' '.join(line1_words), ' '.join(remaining)


def wrap_col_header(label: str, width: int = VALUE_WIDTH) -> tuple[str, str]:
    """Data-column header: both physical lines centered within `width`."""
    line1, line2 = _wrap_words(label, width)
    return (_center(line1, width), _center(line2, width))


def wrap_row_header(label: str, width: int = LABEL_WIDTH) -> tuple[str, str]:
    """BOX= row header: both physical lines left-justified within `width`."""
    line1, line2 = _wrap_words(label, width)
    return (line1.ljust(width), line2.ljust(width))


def fmt_cell(value: Optional[float], missing: bool) -> str:
    """Cells with no underlying observations print as a bare right-justified
    '0' (no decimals); cells with real (even zero-valued) data print with
    the FORMAT=13.2 decimal formatting."""
    if missing:
        return '0'.rjust(VALUE_WIDTH)
    return f"{value:{VALUE_WIDTH}.2f}"


def render_box(row_box_label: str, col_labels: list[str],
               row_data: list[tuple[str, list[tuple[float, bool]]]],
               total_row: list[tuple[float, bool]]) -> list[str]:
    """Renders one BOX='...' RTS=n PROC TABULATE crosstab, including the
    trailing TOTAL column and TOTAL row, with top/bottom borders."""
    widths = [LABEL_WIDTH] + [VALUE_WIDTH] * (len(col_labels) + 1)
    total_width = sum(widths) + len(widths) + 1

    header1_label, header2_label = wrap_row_header(row_box_label)
    col_headers = [wrap_col_header(c) for c in col_labels] + [wrap_col_header('TOTAL')]

    lines: list[str] = ['-' * total_width]
    lines.append('|' + '|'.join([header1_label] + [h[0] for h in col_headers]) + '|')
    lines.append('|' + '|'.join([header2_label] + [h[1] for h in col_headers]) + '|')
    lines.append('|' + '+'.join('-' * w for w in widths) + '|')

    for label, cells in row_data:
        row_label_text = label.ljust(LABEL_WIDTH)[:LABEL_WIDTH]
        cell_text = '|'.join(fmt_cell(v, m) for v, m in cells)
        lines.append('|' + row_label_text + '|' + cell_text + '|')

    total_label_text = 'TOTAL'.ljust(LABEL_WIDTH)
    total_cell_text = '|'.join(fmt_cell(v, m) for v, m in total_row)
    lines.append('|' + total_label_text + '|' + total_cell_text + '|')
    lines.append('-' * total_width)

    return lines


# ============================================================
# REPORT 1: PAGE=BRANCH, ROW=BNMCODE, COLUMN=TYP
# ============================================================
def _crosstab(pdf: 'pd.DataFrame', row_col: str, col_col: str,
              col_order: list) -> tuple[list[str], list[tuple[str, list]], list]:
    """Groups pdf by (row_col, col_col) summing OUTSTAND, keeping NaN for
    combinations that never occur (used to render a bare '0' vs '0.00')."""
    present_cols = [c for c in col_order if c in pdf['__COL__'].unique()]
    pivot = pdf.pivot_table(index='__ROW__', columns='__COL__',
                             values='OUTSTAND', aggfunc='sum', fill_value=None)

    row_labels = sorted(pivot.index.tolist(), key=lambda x: str(x))
    row_data = []
    for row_label in row_labels:
        cells = []
        for c in present_cols:
            val = pivot.loc[row_label, c] if c in pivot.columns else None
            missing = val is None or pd.isna(val)
            cells.append((0.0 if missing else float(val), missing))
        row_total = sum(v for v, m in cells)
        cells.append((row_total, False))
        row_data.append((str(row_label), cells))

    total_row = []
    for c in present_cols:
        col_sum = pivot[c].sum() if c in pivot.columns else 0.0
        total_row.append((float(col_sum), False))
    grand_total = sum(v for v, _ in total_row)
    total_row.append((grand_total, False))

    return present_cols, row_data, total_row


def build_report1_page(loan_pdf: 'pd.DataFrame') -> list[str]:
    """One BOX='BNMCODE' RTS=8 crosstab (rows=BNMCODE, cols=TYP) for the
    subset of LOAN belonging to a single page (one BRANCH, or ALL for the
    GRAND TOTAL page)."""
    pdf = loan_pdf.copy()
    pdf['__ROW__'] = pdf['BNMCODE']
    pdf['__COL__'] = pdf['TYP']

    present_typs, row_data, total_row = _crosstab(pdf, 'BNMCODE', 'TYP', TYPFMT_ORDER)
    col_labels = [format_typfmt(t) for t in present_typs]
    return render_box('BNMCODE', col_labels, row_data, total_row)


def build_report1(loan: pl.DataFrame) -> list[tuple[str, str]]:
    loan_pdf = loan.select(['BRANCH', 'BNMCODE', 'TYP', 'OUTSTAND']).to_pandas()
    loan_pdf['BNMCODE'] = loan_pdf['BNMCODE'].astype(str).str.strip()

    branches = sorted(loan_pdf['BRANCH'].unique().tolist())

    lines: list[tuple[str, str]] = []
    for branch in branches:
        subset = loan_pdf[loan_pdf['BRANCH'] == branch]
        box_lines = build_report1_page(subset)
        lines.extend(_page(' REPORT ON COLLATERAL', f'BRANCH {branch}', box_lines))

    grand_box_lines = build_report1_page(loan_pdf)
    lines.extend(_page(' REPORT ON COLLATERAL', 'GRAND TOTAL', grand_box_lines))

    return lines


# ============================================================
# REPORT 2 / 3: ROW=BNMCODE, COLUMN=REMMTH BUCKET
# ============================================================
def build_bucketed_report(loan2: pl.DataFrame, bucket_fn, bucket_order: list[str],
                           title3: str) -> list[tuple[str, str]]:
    pdf = loan2.select(['BNMCODE', 'REMMTH', 'OUTSTAND']).to_pandas()
    pdf['BNMCODE'] = pdf['BNMCODE'].astype(str).str.strip()
    pdf['__ROW__'] = pdf['BNMCODE']
    pdf['__COL__'] = pdf['REMMTH'].apply(bucket_fn).astype(str).str.strip()

    _, row_data, total_row = _crosstab(pdf, 'BNMCODE', 'REMMTH', bucket_order)
    box_lines = render_box('BNMCODE', bucket_order, row_data, total_row)

    body = [title3.strip(), ''] + box_lines
    return _page(title3, None, body, is_prebuilt_body=True)


# ============================================================
# PAGE / TITLE ASSEMBLY (ASA carriage control)
# ============================================================
_page_counter = {'n': 0}
_rdate_str = {'v': ''}


def _title1(page_num: int) -> str:
    num_str = str(page_num)
    left = 'REPORT ID: EIWIBT1C'
    return left.ljust(LINESIZE - len(num_str)) + num_str


def _page(title3: str, page_header: Optional[str], body_lines: list[str],
          is_prebuilt_body: bool = False) -> list[tuple[str, str]]:
    """Emits one ASA new-page ('1') block: TITLE1/2/3, blank line, optional
    page-dimension header (e.g. 'BRANCH 3002' / 'GRAND TOTAL'), then body."""
    _page_counter['n'] += 1
    lines: list[tuple[str, str]] = [
        ('1', _title1(_page_counter['n'])),
        (' ', f'PUBLIC BANK BERHAD            DATE : {_rdate_str["v"]}'),
        (' ', title3),
        (' ', ''),
    ]
    if page_header is not None:
        lines.append((' ', page_header))
    for line in body_lines:
        lines.append((' ', line))
    return lines


# ============================================================
# OUTPUT WRITER (ASA carriage control)
# ============================================================
def write_report(output_path: Path, all_lines: list[tuple[str, str]]) -> None:
    with open(output_path, 'w', encoding='latin1', newline='') as f:
        for ctrl, text in all_lines:
            line = f"{ctrl}{text}"
            line = line[:LRECL] if len(line) > LRECL else line.ljust(LRECL)
            f.write(line + '\n')


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    reptdate_values = get_reptdate_values()
    rptdate = reptdate_values.reptdate
    _rdate_str['v'] = rptdate.strftime('%d/%m/%y')

    ibtdtl = load_ibtdtl()
    ibtmast = load_ibtmast()

    print("\n===== IBTDTL Columns =====")
    print(ibtdtl.columns)
    print("\n===== IBTMAST Columns =====")
    print(ibtmast.columns)

    btradi = build_btradi(ibtdtl)
    btradia = build_btradia(btradi)
    btradm = build_btradm(ibtmast)
    btrade = build_btrade(btradm, btradia)
    loan = build_loan(btrade)
    loan2 = build_loan2(btradi, rptdate)

    report1_lines = build_report1(loan)
    report2_lines = build_bucketed_report(
        loan2, format_remfmt, REMFMT_ORDER, ' REPORT ON REMAINING MATURITY')
    report3_lines = build_bucketed_report(
        loan2, format_remfmts, REMFMTS_ORDER, ' REPORT ON REMAINING MATURITY')

    all_lines = report1_lines + report2_lines + report3_lines

    # Static filename: original SAP.PIBB.EIWIBT1C.TEXT dataset name carries no
    # date component, so output_date.py's date-suffix builder is not used here.
    output_path = OUTPUT_DIR / "EIWIBT1C.txt"
    write_report(output_path, all_lines)

    print(f"Output written to: {output_path}")
    print()
    for _, text in all_lines:
        print(text)


if __name__ == "__main__":
    main()
