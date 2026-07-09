#!/usr/bin/env python3
"""
Program  : EIWIBT1C.py
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
"""

from pathlib import Path
from datetime import date, datetime, timedelta
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
INPUT_DIR = BASE_DIR / "input" / "prod" / "btrade"
OUTPUT_DIR = BASE_DIR / "output" / "EIWIBT1C"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

IBTMAST_PREFIX = "ibtmast"   # ibtmastXXXXX.sas7bdat -> MM W YY
IBTDTL_PREFIX = "ibtdtl"     # ibtdtlXXXXXX.sas7bdat  -> YY MM DD

PAGE_LENGTH = 60
LRECL = 150
ROW_WIDTH = 8          # RTS=8
VALUE_WIDTH = 13       # FORMAT=13.2
SAS_EPOCH = date(1960, 1, 1)


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
    if issdte is None or exprdate is None:
        return '20'
    days = (exprdate - issdte).days
    return '10' if days < 366 else '20'


def _as_date(value) -> Optional[date]:
    """Convert a SAS-derived date value to a Python date.

    IMPORTANT: pd.read_sas() does NOT always auto-convert SAS date columns to
    datetime - a column can still surface as a raw numeric "days since
    1960-01-01" SAS date value. Feeding such an int/float straight into
    pd.Timestamp() misinterprets it as nanoseconds since 1970-01-01, which
    silently collapses every date to ~epoch and breaks all downstream
    maturity-bucket logic. Raw numerics are therefore explicitly decoded
    against the SAS epoch (1960-01-01) here.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        if value != value:  # NaN check
            return None
        return SAS_EPOCH + timedelta(days=int(value))
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
    """DATA BTRADI (both steps): PRODCD assignment + ORIGMT derivation."""
    df = ibtdtl.with_columns([
        pl.col('LIABCODE').cast(pl.Utf8).str.strip_chars(),
        pl.col('DIRCTIND').cast(pl.Utf8).str.strip_chars(),
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
    (last-dataset-wins semantics, since BTRADM carries none of these columns).

    ACCTNO is cast to a common type on both sides before the join: pd.read_sas()
    returns numerics as float64, but BTRADM and BTRADIA are built through
    different pipelines (SORT/NODUPKEYS vs GROUP BY), so an explicit, matching
    cast is required to avoid silent join-key mismatches that would leave
    PRODCD/BRANCH/OUTSTAND null for every row.
    """
    a = btradm.with_columns(pl.col('ACCTNO').cast(pl.Float64))
    b = btradia.with_columns(pl.col('ACCTNO').cast(pl.Float64))
    return a.join(b, on='ACCTNO', how='left')


def build_loan(btrade: pl.DataFrame) -> pl.DataFrame:
    """DATA LOAN: %COLL; BNMCODE = PRODCD;"""
    typ_vals = [classify_collateral(v) for v in btrade['LIABCODE'].to_list()]
    return btrade.with_columns([
        pl.Series('TYP', typ_vals),
        pl.col('PRODCD').alias('BNMCODE'),
    ])


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
# REPORT RENDERING (ASA carriage control, PROC TABULATE-style crosstabs)
# ============================================================
def fmt_value(v: Optional[float]) -> str:
    """FORMAT=13.2, but PROC TABULATE renders a bucket with no underlying
    data as a plain zero (no decimals), not '0.00'."""
    if v is None:
        v = 0.0
    if v == 0:
        return '0'.rjust(VALUE_WIDTH)
    return f"{v:{VALUE_WIDTH}.2f}"


def title_block(title3: str, rdate_str: str, page_no: int) -> list[tuple[str, str]]:
    title = f"REPORT ID: EIWIBT1C{page_no:>113}"
    return [
        ('', title),
        ('', f'PUBLIC BANK BERHAD            DATE : {rdate_str}'),
        ('', title3),
        ('', ''),
    ]


def paginate(title3: str, rdate_str: str, body_lines: list[str],
             page_length: int = PAGE_LENGTH,
             start_page: int = 1) -> tuple[list[tuple[str, str]], int]:
    """Paginate report lines with page-numbered headings.

    Returns (rendered_lines, next_page) so callers can chain page numbers
    continuously across multiple reports in a single run instead of every
    report restarting at page 1.
    """
    header_len = len(title_block(title3, rdate_str, start_page))
    lines_per_page = max(page_length - header_len, 1)

    out: list[tuple[str, str]] = []
    page_no = start_page
    for i, line in enumerate(body_lines):
        if i % lines_per_page == 0:
            out.extend(title_block(title3, rdate_str, page_no))
            page_no += 1
        out.append(('', line))
    if not body_lines:
        out.extend(title_block(title3, rdate_str, page_no))
        page_no += 1
    return out, page_no


def split_header_label(label: str, width: int = VALUE_WIDTH) -> tuple[str, str]:
    """Wrap a column heading across two lines the way PROC TABULATE does:
    the label sits entirely on the second header line if it fits within the
    column width; otherwise the last word is pushed onto the second line and
    the remainder stays on the first line."""
    label = label.strip()
    if len(label) <= width:
        return '', label
    if ' ' in label:
        first, last = label.rsplit(' ', 1)
        first = first.strip()
        if len(first) <= width and len(last) <= width:
            return first, last
    return label[:width], label[width:2 * width].strip()


def build_report1(loan_df: pl.DataFrame, rdate_str: str,
                   start_page: int = 1) -> tuple[list[tuple[str, str]], int]:
    """Render the collateral report as a single PROC TABULATE crosstab
    (BNMCODE down the rows, collateral type/TOTAL across the columns),
    summarized as one GRAND TOTAL table across all branches.
    Missing detail rows from the master/detail merge are excluded so
    unmatched master accounts do not produce all-zero rows.
    """
    pdf = loan_df.select(['BNMCODE', 'TYP', 'OUTSTAND']).to_pandas()
    pdf = pdf[pdf['OUTSTAND'].notna()]
    pdf['BNMCODE'] = pdf['BNMCODE'].fillna('').astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    pdf = pdf[pdf['BNMCODE'] != '']
    pdf['TYP_LABEL'] = pdf['TYP'].apply(format_typfmt)

    title3 = ' REPORT ON COLLATERAL'
    pivot = pdf.pivot_table(index='BNMCODE', columns='TYP_LABEL',
                             values='OUTSTAND', aggfunc='sum', fill_value=0.0)
    typ_labels = list(pivot.columns)
    if not typ_labels:
        return paginate(title3, rdate_str, [], start_page=start_page)

    header1 = '|BNMCO-' + ''.join(f'|{label.strip()[:VALUE_WIDTH].center(VALUE_WIDTH)}' for label in typ_labels) + '|' + ''.center(VALUE_WIDTH) + '|'
    header2 = '|DE    ' + ''.join(f'|{"(100%)".center(VALUE_WIDTH)}' for _ in typ_labels) + '|' + 'TOTAL'.center(VALUE_WIDTH) + '|'
    sep = '|------' + '+-------------' * (len(typ_labels) + 1) + '|'
    border = '-' * len(header1)

    body: list[str] = ['GRAND TOTAL', border, header1, header2, sep]

    totals = {label: 0.0 for label in typ_labels}
    for bnmcode, rowvals in pivot.sort_index().iterrows():
        cells = [float(rowvals[label]) for label in typ_labels]
        for label, value in zip(typ_labels, cells):
            totals[label] += value
        body.append(f'|{str(bnmcode).ljust(6)[:6]}' + ''.join(f'|{fmt_value(v)}' for v in cells) + f'|{fmt_value(sum(cells))}|')

    total_values = [totals[label] for label in typ_labels]
    body.append('|TOTAL ' + ''.join(f'|{fmt_value(v)}' for v in total_values) + f'|{fmt_value(sum(total_values))}|')
    body.append(border)

    return paginate(title3, rdate_str, body, start_page=start_page)


def build_two_way_report(loan2_df: pl.DataFrame, bucket_fn, bucket_labels: list[str],
                          title3: str, rdate_str: str,
                          start_page: int = 1) -> tuple[list[tuple[str, str]], int]:
    """PROC TABULATE: CLASS BNMCODE REMMTH; VAR OUTSTAND;
    TABLE (BNMCODE=' ' ALL='TOTAL'),(REMMTH=' ' ALL='TOTAL')*OUTSTAND=' '*SUM=' '
          / BOX='BNMCODE' RTS=8;
    Rendered in the same box-table style as Report 1: a bordered grid with a
    two-line wrapped column heading (only shown when a heading needs wrapping)."""
    pdf = loan2_df.select(['BNMCODE', 'REMMTH', 'OUTSTAND']).to_pandas()
    pdf['BNMCODE'] = pdf['BNMCODE'].fillna('').astype(str).str.strip()
    pdf['__BUCKET__'] = pdf['REMMTH'].apply(bucket_fn)

    pivot = pdf.pivot_table(index='BNMCODE', columns='__BUCKET__',
                             values='OUTSTAND', aggfunc='sum', fill_value=0.0)
    for b in bucket_labels:
        if b not in pivot.columns:
            pivot[b] = 0.0
    pivot = pivot[bucket_labels]
    pivot['TOTAL'] = pivot.sum(axis=1)
    grand_total = pivot.sum(axis=0)

    col_labels = bucket_labels + ['TOTAL']
    header1_parts: list[str] = []
    header2_parts: list[str] = []
    needs_split = False
    for label in col_labels:
        h1, h2 = split_header_label(label)
        if h1:
            needs_split = True
        header1_parts.append(h1.center(VALUE_WIDTH))
        header2_parts.append(h2.center(VALUE_WIDTH))

    if needs_split:
        header_lines = [
            '|BNMCO-' + ''.join(f'|{h}' for h in header1_parts) + '|',
            '|DE    ' + ''.join(f'|{h}' for h in header2_parts) + '|',
        ]
    else:
        header_lines = ['|BNMCO-' + ''.join(f'|{h}' for h in header2_parts) + '|']

    sep = '|------' + '+-------------' * len(col_labels) + '|'
    border = '-' * len(header_lines[0])

    body: list[str] = [border, *header_lines, sep]
    for bnmcode, rowvals in pivot.sort_index().iterrows():
        cells = [float(rowvals[c]) for c in col_labels]
        row_label = str(bnmcode).ljust(6)[:6]
        body.append(f'|{row_label}' + ''.join(f'|{fmt_value(v)}' for v in cells) + '|')

    total_cells = [float(grand_total[c]) for c in col_labels]
    body.append('|TOTAL ' + ''.join(f'|{fmt_value(v)}' for v in total_cells) + '|')
    body.append(border)

    return paginate(title3, rdate_str, body, start_page=start_page)


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
    rdate_str = rptdate.strftime('%d/%m/%y')

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

    # Page numbers are threaded across all three reports so pagination is
    # continuous for the run, instead of every report restarting at page 1.
    report1_lines, next_page = build_report1(loan, rdate_str, start_page=1)
    report2_lines, next_page = build_two_way_report(
        loan2, format_remfmt,
        ['UP TO 1 WK     ', '>1 WK - 1 MTH  ', '>1 MTH - 3 MTHS',
         '>3 - 6 MTHS    ', '>6 MTHS - 1 YR ', '>1 YEAR        '],
        ' REPORT ON REMAINING MATURITY', rdate_str, start_page=next_page,
    )
    report3_lines, next_page = build_two_way_report(
        loan2, format_remfmts,
        ['<1 YEAR        ', '>1 YEAR        '],
        ' REPORT ON REMAINING MATURITY', rdate_str, start_page=next_page,
    )

    all_lines = report1_lines + report2_lines + report3_lines

    # Static filename: original SAP.PIBB.EIWIBT1C.TEXT dataset name carries no
    # date component, so output_date.py's date-suffix builder is not used here.
    output_path = OUTPUT_DIR / "EIWIBT1C.txt"
    write_report(output_path, all_lines)

    print(f"Output written to: {output_path}")
    print()
    for ctrl, text in all_lines:
        print(text)

if __name__ == "__main__":
    main()
