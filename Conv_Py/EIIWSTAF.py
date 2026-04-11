#!/usr/bin/env python3
"""
Program : EIIWSTAF.py
Purpose : WEEKLY LISTING FOR STAFF NEW LOAN AND PAID LOAN
"""

import duckdb
import polars as pl
from datetime import date, datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta

# ============================================================================
# PATHS
# ============================================================================
INPUT_DIR          = Path("input")
OUTPUT_DIR         = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_PARQUET   = INPUT_DIR / "reptdate.parquet"
LNNOTE_PARQUET     = INPUT_DIR / "mniln_lnnote.parquet"
LNCOMM_PARQUET     = INPUT_DIR / "mniln_lncomm.parquet"
ILNNOTE_PARQUET    = INPUT_DIR / "imniln_lnnote.parquet"
ILNCOMM_PARQUET    = INPUT_DIR / "imniln_lncomm.parquet"
ISBASE_PARQUET     = INPUT_DIR / "lnhist_isbase.parquet"
# LNPAY and ILNPAY are keyed by week number (NOWK); resolved after REPTDATE is loaded
LNPAY_PARQUET_TPL  = INPUT_DIR / "pay_lnpay{nowk}.parquet"
ILNPAY_PARQUET_TPL = INPUT_DIR / "ipay_ilnpay{nowk}.parquet"

OUTPUT_SETTLE      = OUTPUT_DIR / "EIIWSTAF_paid_loan.txt"
OUTPUT_NEW_LOAN    = OUTPUT_DIR / "EIIWSTAF_new_loan.txt"
OUTPUT_MIGRATION   = OUTPUT_DIR / "EIIWSTAF_migration_loan.txt"
OUTPUT_FULLREL     = OUTPUT_DIR / "EIIWSTAF_full_release_loan.txt"

# ============================================================================
# HELPERS
# ============================================================================

PAGE_LENGTH = 60
LINE_WIDTH  = 132


def fmt_comma(val, width=14, dec=2):
    """Format a number with comma separators."""
    if val is None:
        return ' ' * width
    try:
        if dec == 0:
            s = f"{val:,.0f}"
        else:
            s = f"{val:,.{dec}f}"
        return s.rjust(width)
    except (TypeError, ValueError):
        return ' ' * width


def fmt_ddmmyy8(d):
    """Format a date as DD/MM/YY."""
    if d is None:
        return ' ' * 8
    if isinstance(d, datetime):
        d = d.date()
    return d.strftime("%d/%m/%y")


def parse_packed_date_z11(val):
    """
    Parse SAS Z11 integer-encoded date (MMDDYYYY format packed as integer).
    SAS stores dates like 01152025 for Jan 15 2025 in Z11 format.
    LASTTRAN / ISSUEDT: MMDDYYYY padded to 11 digits.
    """
    if val is None or val == 0:
        return None
    try:
        s = f"{int(val):011d}"
        mm = int(s[0:2])
        dd = int(s[2:4])
        yyyy = int(s[4:8])
        return date(yyyy, mm, dd)
    except Exception:
        return None


def parse_payeffdt_z11(val):
    """
    Parse PAYEFFDT in Z11 format; reconstruct as DD/MM/YY string.
    SAS: PAYEFF = (SUBSTR(Z11,10,2)||'/'||SUBSTR(Z11,8,2)||'/'||SUBSTR(Z11,3,2))
    Positions (1-based): chars 10-11 = day, 8-9 = month, 3-4 = year (2-digit)
    """
    if val is None or val == 0:
        return None
    try:
        s = f"{int(val):011d}"
        dd  = s[9:11]   # positions 10-11 (0-based: 9-10)
        mm  = s[7:9]    # positions 8-9  (0-based: 7-8)
        yy  = s[2:4]    # positions 3-4  (0-based: 2-3)
        return f"{dd}/{mm}/{yy}"
    except Exception:
        return None


def parse_freleas_mmddyyyy(val):
    """
    Parse FRELEAS using MMDDYY8 format (first 8 chars of Z11).
    """
    if val is None or val == 0:
        return None
    try:
        s = f"{int(val):011d}"[:8]
        mm = int(s[0:2])
        dd = int(s[2:4])
        yyyy = int(s[4:8])
        return date(yyyy, mm, dd)
    except Exception:
        return None


# ============================================================================
# LOAD REPTDATE
# ============================================================================

def load_reptdate():
    con = duckdb.connect()
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    reptdate = row[0]
    if isinstance(reptdate, datetime):
        reptdate = reptdate.date()

    day = reptdate.day
    if day == 8:
        sdd = 1;  wk = '01'
    elif day == 15:
        sdd = 9;  wk = '02'
    elif day == 22:
        sdd = 16; wk = '03'
    else:
        sdd = 23; wk = '04'

    mmp   = reptdate.month
    yyp   = reptdate.year
    pdate = date(yyp, mmp, 1)
    sdate = date(yyp, mmp, sdd)

    # PREVDATE = first day of current month - 1 = last day of previous month
    prevdate = pdate - relativedelta(days=1)

    rdate    = reptdate.strftime("%d/%m/%Y")    # DDMMYY10. -> dd/mm/yyyy
    reptmon  = f"{reptdate.month:02d}"
    reptday  = reptdate.day
    reptmth  = reptdate.month
    reptyear = reptdate.year

    # REPTMM/REPTYY depends on week
    if wk == '04':
        reptmm = f"{reptdate.month:02d}"
        reptyy = str(reptdate.year)[-2:]
    else:
        reptmm = f"{prevdate.month:02d}"
        reptyy = str(prevdate.year)[-2:]

    nowk = wk   # '01'..'04'

    return {
        'reptdate': reptdate,
        'prevdate': prevdate,
        'pdate':    pdate,
        'sdate':    sdate,
        'edate':    reptdate,
        'nowk':     nowk,
        'strday':   sdd,
        'rdate':    rdate,
        'reptmon':  reptmon,
        'reptday':  reptday,
        'reptmth':  reptmth,
        'reptyear': reptyear,
        'reptmm':   reptmm,
        'reptyy':   reptyy,
    }


# ============================================================================
# LOAD AND MERGE LNNOTE + LNCOMM
# ============================================================================

LOANTYPE_FILTER = set(list(range(0, 62)) + [100, 102, 103, 104, 105])


def load_lnnote_merged(lnnote_path: Path, lncomm_path: Path, costctr_filter) -> pl.DataFrame:
    """
    Load LNNOTE filtered by LOANTYPE and COSTCTR, merge with LNCOMM,
    then compute APPRLIMT.
    """
    con = duckdb.connect()

    # Build loantype IN clause
    lt_list = ','.join(str(x) for x in sorted(LOANTYPE_FILTER))

    if costctr_filter == 'eq8044':
        costctr_clause = "AND COSTCTR = 8044"
    else:
        # 3000 <= COSTCTR <= 3999
        costctr_clause = "AND COSTCTR BETWEEN 3000 AND 3999"

    lnnote = con.execute(f"""
        SELECT * FROM read_parquet('{lnnote_path}')
        WHERE (LOANTYPE <= 61 OR LOANTYPE IN ({lt_list}))
          {costctr_clause}
        ORDER BY ACCTNO, COMMNO
    """).pl()

    lncomm = con.execute(f"""
        SELECT * FROM read_parquet('{lncomm_path}')
        ORDER BY ACCTNO, COMMNO
    """).pl()

    # Merge (left join): LNNOTE left join LNCOMM on ACCTNO, COMMNO
    merged = lnnote.join(lncomm, on=['ACCTNO', 'COMMNO'], how='left', suffix='_COMM')

    # Compute APPRLIMT:
    # IF COMMNO > 0 THEN
    #    IF REVOVLI = 'N' THEN APPRLIMT = CORGAMT ELSE APPRLIMT = CCURAMT
    # ELSE APPRLIMT = ORGBAL
    def compute_apprlimt(row):
        commno  = row.get('COMMNO', 0) or 0
        revovli = row.get('REVOVLI', '') or ''
        if commno > 0:
            if revovli == 'N':
                return row.get('CORGAMT', 0) or 0
            else:
                return row.get('CCURAMT', 0) or 0
        else:
            return row.get('ORGBAL', 0) or 0

    apprlimt_vals = [compute_apprlimt(r) for r in merged.iter_rows(named=True)]
    merged = merged.with_columns(pl.Series('APPRLIMT', apprlimt_vals))
    return merged


# ============================================================================
# BUILD LOAN DATASET
# ============================================================================

INTGRVAR_KEEP = [
    'LOANTYPE', 'NTBRCH', 'ORGTYPE', 'ACCTNO', 'CURBAL',
    'NOTENO', 'NAME', 'APPRLIMT', 'ISSDTE', 'PAIDIND', 'BLDATE',
    'BILPAY', 'PAYAMT', 'INTRATE', 'STAFFNO', 'PAYEFF',
    'ORGBAL', 'LASTTRAN', 'LSTTRNAM', 'LSTTRNCD', 'NOOFAC',
    'RESTIND', 'FLAG1', 'FULRELDTE',
]


def build_loan(lnnote: pl.DataFrame, ilnnote: pl.DataFrame) -> pl.DataFrame:
    combined = pl.concat([lnnote, ilnnote], how='diagonal')

    rows = []
    for r in combined.iter_rows(named=True):
        r = dict(r)

        # Parse LASTTRAN: Z11 MMDDYYYY
        r['LASTTRAN'] = parse_packed_date_z11(r.get('LASTTRAN'))

        # Parse ISSDTE from ISSUEDT: Z11 MMDDYYYY
        r['ISSDTE'] = parse_packed_date_z11(r.get('ISSUEDT'))

        # Parse PAYEFF from PAYEFFDT: Z11, positions 10-11/8-9/3-4
        r['PAYEFF'] = parse_payeffdt_z11(r.get('PAYEFFDT'))

        # Parse FULRELDTE from FRELEAS using MMDDYY8 (first 8 of Z11)
        freleas = r.get('FRELEAS')
        if freleas and freleas not in (None, 0):
            r['FULRELDTE'] = parse_freleas_mmddyyyy(freleas)
        else:
            r['FULRELDTE'] = None

        r['NOOFAC'] = 1

        # Keep only INTGRVAR columns (add missing as None)
        out = {col: r.get(col) for col in INTGRVAR_KEEP}
        rows.append(out)

    loan = pl.DataFrame(rows)
    loan = loan.sort(['ACCTNO', 'NOTENO'])
    return loan


# ============================================================================
# RPT 1 - SETTLED A/C FOR THE WEEK
# ============================================================================

def build_lnsettle(loan: pl.DataFrame, ctx: dict) -> pl.DataFrame:
    strday  = ctx['strday']
    reptday = ctx['reptday']
    reptmth = ctx['reptmth']
    reptyear= ctx['reptyear']

    rows = []
    for r in loan.iter_rows(named=True):
        paidind   = r.get('PAIDIND', '') or ''
        lasttran  = r.get('LASTTRAN')
        if paidind not in ('P', 'C'):
            continue
        if lasttran is None:
            continue
        if isinstance(lasttran, datetime):
            lasttran = lasttran.date()
        if not (
            lasttran.day >= strday and
            lasttran.day <= reptday and
            lasttran.month == reptmth and
            lasttran.year == reptyear
        ):
            continue

        r = dict(r)
        lsttrncd = r.get('LSTTRNCD')
        r['SETTDT']  = lasttran
        r['SETTAMT'] = r.get('LSTTRNAM')
        r['SETTCD']  = lsttrncd
        r['LSTRNDSC'] = 'LAST TRANCODE EQ 652' if lsttrncd == 652 else 'LAST TRANCODE NE 652'
        rows.append(r)

    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(rows)
    df = df.sort(['LSTRNDSC', 'LOANTYPE', 'NTBRCH'])
    return df


# ============================================================================
# RPT 2 - RELEASE/MIGRATION A/C FOR THE WEEK
# ============================================================================

def build_lnreles(loan: pl.DataFrame, hist: pl.DataFrame, ctx: dict):
    """
    Merge LOAN (not in HIST) and apply RELEASE/MIGRATION filters.
    Returns (lnreles, lnrels1) where lnrels1 is ACCTNO/NOTENO for PROC APPEND.
    """
    pdate = ctx['pdate']
    edate = ctx['edate']

    # Anti-join: LOAN where (ACCTNO, NOTENO) not in HIST
    hist_keys = set(
        zip(hist['ACCTNO'].to_list(), hist['NOTENO'].to_list())
    )

    reles_rows = []
    rels1_rows = []

    for r in loan.iter_rows(named=True):
        acctno = r.get('ACCTNO')
        noteno = r.get('NOTENO')
        if (acctno, noteno) in hist_keys:
            continue

        r = dict(r)
        fulreldte = r.get('FULRELDTE')
        flag1     = r.get('FLAG1', '') or ''
        restind   = r.get('RESTIND', '') or ''
        issdte    = r.get('ISSDTE')
        nmn       = None

        if isinstance(fulreldte, datetime):
            fulreldte = fulreldte.date()
        if isinstance(issdte, datetime):
            issdte = issdte.date()

        if fulreldte and pdate <= fulreldte <= edate and flag1 == 'M' and restind == 'M':
            nmn    = 'Y'
            issdte = fulreldte
            r['ISSDTE'] = issdte

        r['NMN'] = nmn

        # Filter condition
        if issdte and (pdate <= issdte <= edate):
            pass
        elif nmn == 'Y':
            pass
        else:
            continue

        r['NWI'] = 'Y'
        reles_rows.append(r)
        rels1_rows.append({'ACCTNO': acctno, 'NOTENO': noteno})

    lnreles = pl.DataFrame(reles_rows) if reles_rows else pl.DataFrame()
    lnrels1 = pl.DataFrame(rels1_rows) if rels1_rows else pl.DataFrame()
    return lnreles, lnrels1


# ============================================================================
# SPLIT INTO LNRPT1A / LNRPT1B
# ============================================================================

def split_rpt1(lnsettle: pl.DataFrame, lnreles: pl.DataFrame):
    """
    Merge LNSETTLE and LNRELES by ACCTNO, apply split logic.
    Returns (lnrpt1a, lnrpt1b).
    """
    if lnreles.is_empty():
        return pl.DataFrame(), pl.DataFrame()

    # Sort both by ACCTNO
    settle_sorted = lnsettle.sort('ACCTNO') if not lnsettle.is_empty() else lnsettle
    reles_sorted  = lnreles.sort('ACCTNO')

    # Build settle lookup by ACCTNO
    settle_map = {}
    if not settle_sorted.is_empty():
        for r in settle_sorted.iter_rows(named=True):
            settle_map[r['ACCTNO']] = r

    rpt1a_rows = []
    rpt1b_rows = []

    for r in reles_sorted.iter_rows(named=True):
        acctno  = r.get('ACCTNO')
        nmn     = r.get('NMN')
        nwi     = r.get('NWI', 'Y')
        paidind = r.get('PAIDIND', '') or ''
        orgbal  = r.get('ORGBAL', 0) or 0
        in_b    = acctno in settle_map

        if not in_b:
            # A and NOT B
            if nmn == 'Y':
                rpt1b_rows.append(r)
            else:
                rpt1a_rows.append(r)
        else:
            # A and B
            sr = settle_map[acctno]
            settamt = sr.get('SETTAMT', 0) or 0
            if orgbal == settamt or nmn == 'Y':
                rpt1b_rows.append(r)
            if orgbal == settamt:
                # DELETE — skip rpt1a
                continue
            if nwi == 'Y' and paidind != 'P':
                rpt1a_rows.append(r)

    lnrpt1a = pl.DataFrame(rpt1a_rows) if rpt1a_rows else pl.DataFrame()
    lnrpt1b = pl.DataFrame(rpt1b_rows) if rpt1b_rows else pl.DataFrame()
    return lnrpt1a, lnrpt1b


# ============================================================================
# LOAD LNPAY
# ============================================================================

def load_lnpay(nowk: str) -> pl.DataFrame:
    con = duckdb.connect()
    lnpay_path  = Path(str(LNPAY_PARQUET_TPL).replace('{nowk}', nowk))
    ilnpay_path = Path(str(ILNPAY_PARQUET_TPL).replace('{nowk}', nowk))

    dfs = []
    for p in [lnpay_path, ilnpay_path]:
        if p.exists():
            df = con.execute(f"""
                SELECT ACCTNO, NOTENO, EFFDATE, PAYAMT
                FROM read_parquet('{p}')
                WHERE PAYAMT <> 0
            """).pl()
            dfs.append(df)

    if not dfs:
        return pl.DataFrame(schema={'ACCTNO': pl.Int64, 'NOTENO': pl.Int64,
                                     'PAYEFF': pl.Utf8, 'PAYAMT': pl.Float64})

    lnpay = pl.concat(dfs, how='diagonal')

    # Compute PAYEFF: PAYEFFDD=99 / PAYEFFMM / PAYEFFYY
    def make_payeff(effdate):
        if effdate is None:
            return None
        if isinstance(effdate, datetime):
            effdate = effdate.date()
        mm = f"{effdate.month:02d}"
        yy = str(effdate.year)[-2:]
        # PAYEFFDD = 99 (literal, not the actual day)
        return f"99/{mm}/{yy}"

    payeff_vals = [make_payeff(r) for r in lnpay['EFFDATE'].to_list()]
    lnpay = lnpay.with_columns(pl.Series('PAYEFF', payeff_vals))
    lnpay = lnpay.drop('EFFDATE')

    # Sort by ACCTNO, NOTENO, PAYEFF then NODUPKEY by ACCTNO, NOTENO
    lnpay = lnpay.sort(['ACCTNO', 'NOTENO', 'PAYEFF'])
    lnpay = lnpay.unique(subset=['ACCTNO', 'NOTENO'], keep='first')
    return lnpay


# ============================================================================
# MERGE LNRPT1B WITH LNPAY -> split into LNRPT1B and LNRPT1C
# ============================================================================

def merge_rpt1b_lnpay(lnrpt1b: pl.DataFrame, lnpay: pl.DataFrame):
    """
    Merge LNRPT1B with LNPAY by ACCTNO, NOTENO.
    If NMN='Y' -> LNRPT1C, else -> LNRPT1B.
    """
    if lnrpt1b.is_empty():
        return pl.DataFrame(), pl.DataFrame()

    lnrpt1b_sorted = lnrpt1b.sort(['ACCTNO', 'NOTENO'])

    # Build lnpay lookup
    pay_map = {}
    if not lnpay.is_empty():
        for r in lnpay.iter_rows(named=True):
            pay_map[(r['ACCTNO'], r['NOTENO'])] = r

    rpt1b_rows = []
    rpt1c_rows = []

    for r in lnrpt1b_sorted.iter_rows(named=True):
        r = dict(r)
        key = (r.get('ACCTNO'), r.get('NOTENO'))
        pay = pay_map.get(key)
        if pay:
            r['PAYEFF'] = pay.get('PAYEFF', r.get('PAYEFF'))
            r['PAYAMT'] = pay.get('PAYAMT', r.get('PAYAMT'))

        if r.get('NMN') == 'Y':
            rpt1c_rows.append(r)
        else:
            rpt1b_rows.append(r)

    lnrpt1b_out = pl.DataFrame(rpt1b_rows) if rpt1b_rows else pl.DataFrame()
    lnrpt1c_out = pl.DataFrame(rpt1c_rows) if rpt1c_rows else pl.DataFrame()
    return lnrpt1b_out, lnrpt1c_out


# ============================================================================
# REPORT WRITER — PAID LOAN (LNSETTLE)
# ============================================================================

def write_settle_report(df: pl.DataFrame, rdate: str, out_path: Path):
    """
    PROC REPORT for LNSETTLE:
    Groups: LSTRNDSC > LOANTYPE > NTBRCH
    Columns: LSTRNDSC(noprint) LOANTYPE NTBRCH ORGTYPE STAFFNO ACCTNO NOTENO
             NAME APPRLIMT SETTDT SETTAMT PAYAMT SETTCD NOOFAC(noprint)
    """
    lines = []
    page_line = [0]   # mutable counter

    def new_page(first=False):
        asa = '1' if first else '1'
        lines.append(asa + 'REPORT ID : EIIWSTAF (PIBB)')
        lines.append(' WEEKLY REPORT FOR STAFF PAID LOAN LIST AS AT ' + rdate)
        lines.append(' ')
        # Column header
        hdr = (
            f" {'FAC':>3} {'BRH':>3} {'ORG':>3} {'STAFF':>5} {'A/C NO':>10} "
            f"{'NOTE':>5} {'NAME':<24} {'APPROVED LIMIT':>14} "
            f"{'PAID DATE':>8} {'SETTLEMENT AMOUNT':>12} {'MONTHLY REPAYMENT':>12} {'LST':>3}"
        )
        lines.append(hdr)
        lines.append(' ' + '-' * 128)
        page_line[0] = 6

    def check_page():
        if page_line[0] >= PAGE_LENGTH:
            new_page()

    new_page(first=True)

    if df.is_empty():
        with open(out_path, 'w') as f:
            f.write('\n'.join(lines))
        return

    df_sorted = df.sort(['LSTRNDSC', 'LOANTYPE', 'NTBRCH'])

    prev_lstrndsc = None
    prev_loantype = None
    prev_ntbrch   = None

    # Accumulators
    def zero():
        return {'noofac': 0, 'apprlimt': 0.0, 'settamt': 0.0, 'payamt': 0.0}

    tot_ntbrch   = zero()
    tot_loantype = zero()
    tot_lstrndsc = zero()
    tot_grand    = zero()

    def acc(t, r):
        t['noofac']  += r.get('NOOFAC', 0) or 0
        t['apprlimt']+= r.get('APPRLIMT', 0) or 0
        t['settamt'] += r.get('SETTAMT', 0) or 0
        t['payamt']  += r.get('PAYAMT', 0) or 0

    def print_ntbrch_break(t, ntbrch):
        lines.append(' ' + ' ' * 7 + '-' * 121)
        lines.append(
            f"        NO OF A/C :{fmt_comma(t['noofac'],8,0)}"
            f"{' ' * (70-26-8-10)}"
            f"{fmt_comma(t['apprlimt'],14,2)}"
            f"{' ' * (96-70-14)}"
            f"{fmt_comma(t['settamt'],12,2)}"
            f"{' ' * (110-96-12)}"
            f"{fmt_comma(t['payamt'],12,2)}"
        )
        lines.append(' ')
        page_line[0] += 3

    def print_loantype_break(t):
        lines.append(' ' + '-' * 128)
        lines.append(
            f"FAC TOTAL        NO OF A/C :{fmt_comma(t['noofac'],8,0)}"
            f"{' ' * (70-26-8-10)}"
            f"{fmt_comma(t['apprlimt'],14,2)}"
            f"{' ' * (96-70-14)}"
            f"{fmt_comma(t['settamt'],12,2)}"
            f"{' ' * (110-96-12)}"
            f"{fmt_comma(t['payamt'],12,2)}"
        )
        lines.append(' ' + '-' * 128)
        lines.append(' ')
        page_line[0] += 4

    def print_lstrndsc_break(t):
        lines.append(' ' + '=' * 128)
        lines.append(
            f"SUB TOTAL        NO OF A/C :{fmt_comma(t['noofac'],8,0)}"
            f"{' ' * (70-26-8-10)}"
            f"{fmt_comma(t['apprlimt'],14,2)}"
            f"{' ' * (96-70-14)}"
            f"{fmt_comma(t['settamt'],12,2)}"
            f"{' ' * (110-96-12)}"
            f"{fmt_comma(t['payamt'],12,2)}"
        )
        lines.append(' ' + '=' * 128)
        lines.append(' ')
        page_line[0] += 4

    for r in df_sorted.iter_rows(named=True):
        lstrndsc = r.get('LSTRNDSC', '')
        loantype = r.get('LOANTYPE')
        ntbrch   = r.get('NTBRCH')

        # Group breaks (before row)
        if prev_lstrndsc is not None and lstrndsc != prev_lstrndsc:
            print_ntbrch_break(tot_ntbrch, prev_ntbrch)
            print_loantype_break(tot_loantype)
            print_lstrndsc_break(tot_lstrndsc)
            tot_ntbrch = zero(); tot_loantype = zero(); tot_lstrndsc = zero()
        elif prev_loantype is not None and loantype != prev_loantype:
            print_ntbrch_break(tot_ntbrch, prev_ntbrch)
            print_loantype_break(tot_loantype)
            tot_ntbrch = zero(); tot_loantype = zero()
        elif prev_ntbrch is not None and ntbrch != prev_ntbrch:
            print_ntbrch_break(tot_ntbrch, prev_ntbrch)
            tot_ntbrch = zero()

        prev_lstrndsc = lstrndsc
        prev_loantype = loantype
        prev_ntbrch   = ntbrch

        acc(tot_ntbrch, r)
        acc(tot_loantype, r)
        acc(tot_lstrndsc, r)
        acc(tot_grand, r)

        check_page()
        settdt = r.get('SETTDT')
        detail = (
            f" {r.get('LOANTYPE', ''):>3} "
            f"{r.get('NTBRCH', ''):>3} "
            f"{r.get('ORGTYPE', ''):>3} "
            f"{r.get('STAFFNO', ''):>5} "
            f"{r.get('ACCTNO', ''):>10} "
            f"{r.get('NOTENO', ''):>5} "
            f"{str(r.get('NAME', '') or ''):<24} "
            f"{fmt_comma(r.get('APPRLIMT'),14,2)} "
            f"{fmt_ddmmyy8(settdt):>8} "
            f"{fmt_comma(r.get('SETTAMT'),12,2)} "
            f"{fmt_comma(r.get('PAYAMT'),12,2)} "
            f"{r.get('SETTCD', ''):>3}"
        )
        lines.append(detail)
        page_line[0] += 1

    # Final breaks
    if prev_ntbrch is not None:
        print_ntbrch_break(tot_ntbrch, prev_ntbrch)
    if prev_loantype is not None:
        print_loantype_break(tot_loantype)
    if prev_lstrndsc is not None:
        print_lstrndsc_break(tot_lstrndsc)

    # Grand total
    lines.append(' ')
    lines.append(
        f"GRAND TOTAL      NO OF A/C : {fmt_comma(tot_grand['noofac'],8,0)}"
        f"{' ' * (70-28-8-10)}"
        f"{fmt_comma(tot_grand['apprlimt'],14,2)}"
        f"{' ' * (96-70-14)}"
        f"{fmt_comma(tot_grand['settamt'],12,2)}"
        f"{' ' * (110-96-12)}"
        f"{fmt_comma(tot_grand['payamt'],12,2)}"
    )
    lines.append(' ' + '=' * 128)

    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    print(f"Report written to {out_path}")


# ============================================================================
# REPORT WRITER — NEW / MIGRATION / FULL RELEASE LOANS
# ============================================================================

def write_loan_report(df: pl.DataFrame, rdate: str, title2: str, out_path: Path):
    """
    Generic PROC REPORT for LNRPT1A / LNRPT1B / LNRPT1C:
    Groups: LOANTYPE > NTBRCH
    Columns: LOANTYPE NTBRCH ORGTYPE STAFFNO ACCTNO NOTENO NAME
             APPRLIMT ISSDTE PAYEFF PAYAMT INTRATE NOOFAC(noprint)
    """
    lines = []
    page_line = [0]

    def new_page(first=False):
        asa = '1'
        lines.append(asa + 'REPORT ID : EIIWSTAF (PIBB)')
        lines.append(' ' + title2)
        lines.append(' ')
        hdr = (
            f" {'FAC':>3} {'BR':>4} {'ORG.':>4} {'EMP.NO':>6} {'A/C NO':>10} "
            f"{'NOTE':>5} {'NAME':<24} {'APPROVED LIMIT':>14} "
            f"{'ISSUE DATE':>8} {'PAYMENT EFF. DATE':>9} {'PAYMENT AMOUNT':>14} {'INT.':>5}"
        )
        lines.append(hdr)
        lines.append(' ' + '-' * 130)
        page_line[0] = 6

    def check_page():
        if page_line[0] >= PAGE_LENGTH:
            new_page()

    new_page(first=True)

    if df.is_empty():
        with open(out_path, 'w') as f:
            f.write('\n'.join(lines))
        return

    df_sorted = df.sort(['LOANTYPE', 'NTBRCH'])

    prev_loantype = None
    prev_ntbrch   = None

    def zero():
        return {'noofac': 0, 'apprlimt': 0.0, 'payamt': 0.0}

    tot_ntbrch   = zero()
    tot_loantype = zero()
    tot_grand    = zero()

    def acc(t, r):
        t['noofac']  += r.get('NOOFAC', 0) or 0
        t['apprlimt']+= r.get('APPRLIMT', 0) or 0
        t['payamt']  += r.get('PAYAMT', 0) or 0

    def print_ntbrch_break(t):
        lines.append(' ' + ' ' * 7 + '-' * 123)
        lines.append(
            f"              NO OF A/C :{fmt_comma(t['noofac'],8,0)}"
            f"{' '*45}"
            f"{fmt_comma(t['apprlimt'],14,2)}"
            f"{' '*23}"
            f"{fmt_comma(t['payamt'],14,2)}"
        )
        lines.append(' ')
        page_line[0] += 3

    def print_loantype_break(t):
        lines.append(' ' + '-' * 130)
        lines.append(
            f"SUB TOTAL     NO OF A/C :{fmt_comma(t['noofac'],8,0)}"
            f"{' '*45}"
            f"{fmt_comma(t['apprlimt'],14,2)}"
            f"{' '*23}"
            f"{fmt_comma(t['payamt'],14,2)}"
        )
        lines.append(' ' + '-' * 130)
        lines.append(' ')
        page_line[0] += 4

    for r in df_sorted.iter_rows(named=True):
        loantype = r.get('LOANTYPE')
        ntbrch   = r.get('NTBRCH')

        if prev_loantype is not None and loantype != prev_loantype:
            print_ntbrch_break(tot_ntbrch)
            print_loantype_break(tot_loantype)
            tot_ntbrch = zero(); tot_loantype = zero()
        elif prev_ntbrch is not None and ntbrch != prev_ntbrch:
            print_ntbrch_break(tot_ntbrch)
            tot_ntbrch = zero()

        prev_loantype = loantype
        prev_ntbrch   = ntbrch

        acc(tot_ntbrch, r)
        acc(tot_loantype, r)
        acc(tot_grand, r)

        check_page()
        issdte  = r.get('ISSDTE')
        intrate = r.get('INTRATE', 0) or 0
        detail  = (
            f" {r.get('LOANTYPE', ''):>3} "
            f"{r.get('NTBRCH', ''):>4} "
            f"{r.get('ORGTYPE', ''):>4} "
            f"{r.get('STAFFNO', ''):>6} "
            f"{r.get('ACCTNO', ''):>10} "
            f"{r.get('NOTENO', ''):>5} "
            f"{str(r.get('NAME', '') or ''):<24} "
            f"{fmt_comma(r.get('APPRLIMT'),14,2)} "
            f"{fmt_ddmmyy8(issdte):>8} "
            f"{str(r.get('PAYEFF', '') or ''):>9} "
            f"{fmt_comma(r.get('PAYAMT'),14,2)} "
            f"{intrate:>5.2f}"
        )
        lines.append(detail)
        page_line[0] += 1

    # Final breaks
    if prev_ntbrch is not None:
        print_ntbrch_break(tot_ntbrch)
    if prev_loantype is not None:
        print_loantype_break(tot_loantype)

    # Grand total
    lines.append(' ')
    lines.append(' ' + '-' * 130)
    lines.append(
        f"GRAND TOTAL   NO OF A/C : {fmt_comma(tot_grand['noofac'],8,0)}"
        f"{' '*45}"
        f"{fmt_comma(tot_grand['apprlimt'],14,2)}"
        f"{' '*23}"
        f"{fmt_comma(tot_grand['payamt'],14,2)}"
    )
    lines.append(' ' + '-' * 130)

    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    print(f"Report written to {out_path}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    ctx = load_reptdate()
    rdate   = ctx['rdate']
    nowk    = ctx['nowk']
    pdate   = ctx['pdate']
    edate   = ctx['edate']

    # Load and merge LNNOTE (MNILN, COSTCTR=8044)
    lnnote  = load_lnnote_merged(LNNOTE_PARQUET, LNCOMM_PARQUET, 'eq8044')

    # Load and merge ILNNOTE (IMNILN, 3000<=COSTCTR<=3999)
    ilnnote = load_lnnote_merged(ILNNOTE_PARQUET, ILNCOMM_PARQUET, 'range3000_3999')

    # Build combined LOAN dataset
    loan = build_loan(lnnote, ilnnote)

    # Load LNHIST.ISBASE (historical base for anti-join)
    con = duckdb.connect()
    hist = con.execute(
        f"SELECT ACCTNO, NOTENO FROM read_parquet('{ISBASE_PARQUET}') ORDER BY ACCTNO, NOTENO"
    ).pl()
    hist = hist.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

    # RPT - Settled A/C for the week
    lnsettle = build_lnsettle(loan, ctx)
    write_settle_report(lnsettle, rdate, OUTPUT_SETTLE)

    # RPT - Release/Migration A/C for the week
    lnreles, lnrels1 = build_lnreles(loan, hist, ctx)

    # PROC APPEND: lnrels1 -> LNHIST.ISBASE (append new ACCTNO/NOTENO to isbase)
    # Note: In the Python context this would update the parquet/database;
    # here we simulate by writing out the appended result.
    if not lnrels1.is_empty():
        existing_cols = hist.columns
        new_rows = lnrels1.select([c for c in existing_cols if c in lnrels1.columns])
        updated_hist = pl.concat([hist, new_rows], how='diagonal').unique(
            subset=['ACCTNO', 'NOTENO'], keep='first'
        )
        updated_hist.write_parquet(ISBASE_PARQUET)

    # Split LNRELES vs LNSETTLE -> LNRPT1A, LNRPT1B
    lnrpt1a, lnrpt1b = split_rpt1(lnsettle, lnreles)

    # Load LNPAY
    lnpay = load_lnpay(nowk)

    # Merge LNRPT1B with LNPAY -> LNRPT1B (migration), LNRPT1C (full release)
    lnrpt1b, lnrpt1c = merge_rpt1b_lnpay(lnrpt1b, lnpay)

    # RPT - New Loan List
    write_loan_report(
        lnrpt1a, rdate,
        f'WEEKLY REPORT FOR STAFF NEW LOAN LIST AS AT {rdate}',
        OUTPUT_NEW_LOAN
    )

    # RPT - Migration Loan List
    write_loan_report(
        lnrpt1b, rdate,
        f'WEEKLY REPORT FOR STAFF MIGRATION LOAN LIST AS AT {rdate}',
        OUTPUT_MIGRATION
    )

    # RPT - Full Release Loan List
    write_loan_report(
        lnrpt1c, rdate,
        f'WEEKLY REPORT FOR STAFF FULL RELEASE LOAN LIST AS AT {rdate}',
        OUTPUT_FULLREL
    )


if __name__ == '__main__':
    main()
