# !/usr/bin/env python3
"""
Program : DIIMISA4
Date    : 31.03.09
SMR     : 2008-1076
Report  : NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT
          PERIOD 16-SEP-08 ONWARDS
"""

# OPTIONS YEARCUTOFF=1950 NOCENTER NODATE MISSING=0 LINESIZE=132

import os
import duckdb
import polars as pl
from datetime import date

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR  = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

REPTDATE_PARQUET        = os.path.join(INPUT_DIR, 'deposit_reptdate.parquet')
# SET MIS.DYIBUY&REPTMON
MIS_DYIBUY_PARQUET_TMPL = os.path.join(INPUT_DIR, 'mis_dyibuy{reptmon}.parquet')

OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'DIIMISA4.txt')

PAGE_LENGTH = 60
LINE_SIZE   = 132

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# %INC PGM(PBMISFMT)
# ---------------------------------------------------------------------------
from PBMISFMT import format_brchcd  # noqa: E402

# ---------------------------------------------------------------------------
# %MACRO DCLVAR
#   RETAIN RD1-RD12 FD1-FD12 31 RD2 FD2 28 RD4 RD6 RD9 RD11 FD4 FD6 FD9 FD11 30
#          RPYR RPMTH RPDAY;
#   ARRAY RPDAYS RD1-RD12;
#   ARRAY FDDAYS FD1-FD12;
# ---------------------------------------------------------------------------
def _rpdays(year: int, month: int) -> int:
    """RPDAYS array — days in reporting month."""
    if month == 2:
        return 29 if year % 4 == 0 else 28
    if month in (4, 6, 9, 11):
        return 30
    return 31


def _fddays(year: int, month: int) -> int:
    """FDDAYS array — days in maturity month."""
    if month == 2:
        return 29 if year % 4 == 0 else 28
    if month in (4, 6, 9, 11):
        return 30
    return 31


# ---------------------------------------------------------------------------
# %MACRO REMMTH
# ---------------------------------------------------------------------------
def compute_remmth(reptdate: date, matdate_int: int | None) -> float:
    """
    %MACRO REMMTH — fractional months remaining until maturity.
    FDDATE = INPUT(PUT(MATDATE,Z8.),YYMMDD8.);
    """
    if not matdate_int:
        return 0.0
    s = str(int(matdate_int)).zfill(8)
    try:
        fddate = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except ValueError:
        return 0.0

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rpdays_rpmth = _rpdays(rpyr, rpmth)

    fdyr  = fddate.year
    fdmth = fddate.month
    fdday = fddate.day
    fddays_fdmth = _fddays(fdyr, fdmth)

    # IF FDDAY = FDDAYS(FDMTH) THEN FDDAY=RPDAYS(RPMTH);
    if fdday == fddays_fdmth:
        fdday = rpdays_rpmth

    remy   = fdyr  - rpyr
    remm   = fdmth - rpmth
    remd   = fdday - rpday
    remmth = remy * 12 + remm + remd / rpdays_rpmth
    return remmth


# ---------------------------------------------------------------------------
# PROC FORMAT; VALUE FDFMT
#   LOW - 1  = '01 MONTH '  ...  59 - 60 = '60 MONTHS'
# ---------------------------------------------------------------------------
_FDFMT_BOUNDS = [
    (1,  '01 MONTH '),
    (2,  '02 MONTHS'), (3,  '03 MONTHS'), (4,  '04 MONTHS'), (5,  '05 MONTHS'),
    (6,  '06 MONTHS'), (7,  '07 MONTHS'), (8,  '08 MONTHS'), (9,  '09 MONTHS'),
    (10, '10 MONTHS'), (11, '11 MONTHS'), (12, '12 MONTHS'), (13, '13 MONTHS'),
    (14, '14 MONTHS'), (15, '15 MONTHS'), (16, '16 MONTHS'), (17, '17 MONTHS'),
    (18, '18 MONTHS'), (19, '19 MONTHS'), (20, '20 MONTHS'), (21, '21 MONTHS'),
    (22, '22 MONTHS'), (23, '23 MONTHS'), (24, '24 MONTHS'), (25, '25 MONTHS'),
    (26, '26 MONTHS'), (27, '27 MONTHS'), (28, '28 MONTHS'), (29, '29 MONTHS'),
    (30, '30 MONTHS'), (31, '31 MONTHS'), (32, '32 MONTHS'), (33, '33 MONTHS'),
    (34, '34 MONTHS'), (35, '35 MONTHS'), (36, '36 MONTHS'), (37, '37 MONTHS'),
    (38, '38 MONTHS'), (39, '39 MONTHS'), (40, '40 MONTHS'), (41, '41 MONTHS'),
    (42, '42 MONTHS'), (43, '43 MONTHS'), (44, '44 MONTHS'), (45, '45 MONTHS'),
    (46, '46 MONTHS'), (47, '47 MONTHS'), (48, '48 MONTHS'), (49, '49 MONTHS'),
    (50, '50 MONTHS'), (51, '51 MONTHS'), (52, '52 MONTHS'), (53, '53 MONTHS'),
    (54, '54 MONTHS'), (55, '55 MONTHS'), (56, '56 MONTHS'), (57, '57 MONTHS'),
    (58, '58 MONTHS'), (59, '59 MONTHS'), (60, '60 MONTHS'),
]


def format_fdfmt(remmth: float) -> str:
    """VALUE FDFMT — range format (SAS exclusive-high convention)."""
    for upper, label in _FDFMT_BOUNDS:
        if remmth <= upper:
            return label
    return '60 MONTHS'


# ---------------------------------------------------------------------------
# Numeric format helpers
# ---------------------------------------------------------------------------
def fmt_comma9(v) -> str:
    """F=COMMA9."""
    return f"{int(round(v or 0)):>9,}"


def fmt_comma14_2(v) -> str:
    """F=COMMA14.2"""
    return f"{float(v or 0):>14,.2f}"


# ---------------------------------------------------------------------------
# ASA carriage control
# ---------------------------------------------------------------------------
def apply_asa(lines: list[str], page_length: int = PAGE_LENGTH) -> list[str]:
    """Prepend '1' (new page) or ' ' (single space) per ASA convention."""
    out: list[str] = []
    pos = 0
    for line in lines:
        out.append(('1' if pos == 0 else ' ') + line)
        pos += 1
        if pos >= page_length:
            pos = 0
    return out


# ---------------------------------------------------------------------------
# %MACRO TDBAL / %MACRO AVG — PROC TABULATE renderer
#
# PROC TABULATE DATA=DYIBUA NOSEPS;
#    * FORMAT MTH FDFMT.;   <-- commented out in original SAS
#    CLASS BRCH MTH;
#    VAR FDI FDINO FDINO2;
#    TABLE BRCH=' ' ALL='TOTAL',
#          SUM='<section>'*(FDINO='NO OF A/C'*F=COMMA9.
#                           FDINO2='NO OF RECEIPT'*F=COMMA9.
#                           FDI=' '*F=COMMA14.2*(MTH=' ' ALL='TOTAL'))
#    / BOX='BRANCH NO/CODE' RTS=10;
# ---------------------------------------------------------------------------
def render_tabulate(df: pl.DataFrame, section_label: str,
                    title_lines: list[str]) -> list[str]:
    """
    Render PROC TABULATE equivalent.
    NOTE: * FORMAT MTH FDFMT.; is commented out — MTH shown as raw string.
    """
    if df.is_empty():
        return []

    agg = (
        df.group_by(['BRCH', 'MTH'])
        .agg([
            pl.col('FDINO').sum().alias('FDINO'),
            pl.col('FDINO2').sum().alias('FDINO2'),
            pl.col('FDI').sum().alias('FDI'),
        ])
    )

    mths     = sorted(agg['MTH'].unique().to_list())
    branches = sorted(agg['BRCH'].unique().to_list())

    lookup: dict[str, dict[str, tuple]] = {}
    for row in agg.iter_rows(named=True):
        lookup.setdefault(row['BRCH'], {})[row['MTH']] = (
            row['FDINO'] or 0.0,
            row['FDINO2'] or 0.0,
            row['FDI'] or 0.0,
        )

    out: list[str] = []

    for t in title_lines:
        out.append(t)

    rts   = 10
    col_w = 9 + 1 + 9 + 1 + 14

    n_cols = len(mths) + 1
    span_w = n_cols * col_w + (n_cols - 1)
    out.append(' ' * rts + section_label.center(span_w))

    h2 = 'BRANCH NO/CODE'.ljust(rts)
    for m in mths:
        h2 += m.center(col_w) + ' '
    h2 += 'TOTAL'.center(col_w)
    out.append(h2)

    h3  = ' ' * rts
    sub = 'NO OF A/C'.rjust(9) + ' ' + 'NO OF RECEIPT'.rjust(9) + ' ' + ' ' * 14
    for _ in range(n_cols):
        h3 += sub + ' '
    out.append(h3.rstrip())

    out.append('-' * LINE_SIZE)

    tot_mth: dict[str, list[float]] = {m: [0.0, 0.0, 0.0] for m in mths}
    tot_all: list[float]            = [0.0, 0.0, 0.0]

    for brch in branches:
        bdata = lookup.get(brch, {})
        line  = brch.ljust(rts)
        r_tot = [0.0, 0.0, 0.0]

        for m in mths:
            fi, fi2, fdi = bdata.get(m, (0.0, 0.0, 0.0))
            line += fmt_comma9(fi) + ' ' + fmt_comma9(fi2) + ' ' + fmt_comma14_2(fdi) + ' '
            tot_mth[m][0] += fi;  tot_mth[m][1] += fi2;  tot_mth[m][2] += fdi
            r_tot[0] += fi;       r_tot[1] += fi2;        r_tot[2] += fdi

        line += fmt_comma9(r_tot[0]) + ' ' + fmt_comma9(r_tot[1]) + ' ' + fmt_comma14_2(r_tot[2])
        tot_all[0] += r_tot[0]; tot_all[1] += r_tot[1]; tot_all[2] += r_tot[2]
        out.append(line)

    out.append('-' * LINE_SIZE)
    tline = 'TOTAL'.ljust(rts)
    for m in mths:
        tline += fmt_comma9(tot_mth[m][0]) + ' ' + fmt_comma9(tot_mth[m][1]) + ' ' + fmt_comma14_2(tot_mth[m][2]) + ' '
    tline += fmt_comma9(tot_all[0]) + ' ' + fmt_comma9(tot_all[1]) + ' ' + fmt_comma14_2(tot_all[2])
    out.append(tline)

    return out


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    # DATA REPTDATE (KEEP=REPTDATE); SET DEPOSIT.REPTDATE;
    con = duckdb.connect()
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate: date = row[0] if isinstance(row[0], date) else row[0].date()

    # CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE),Z2.));
    reptmon = f"{reptdate.month:02d}"
    # REPTFQ='Y';
    reptfq  = 'Y'
    # CALL SYMPUT('RDATE', PUT(REPTDATE,DDMMYY8.));
    rdate   = reptdate.strftime('%d/%m/%y')

    # %MACRO CHKRPTDT — %IF "&REPTFQ" EQ "Y" %THEN %DO;
    if reptfq != 'Y':
        return

    # -------------------------------------------------------------------------
    # %MACRO DIBMIS02 — MAIN PROGRAM
    # %INC PGM(PBMISFMT);
    # -------------------------------------------------------------------------

    # DATA _NULL_: DETERMINE REPORTING PERIOD
    rpdate  = reptdate
    strtdte = date(rpdate.year, rpdate.month, 1)
    sdate   = strtdte
    edate   = rpdate

    # DATA CX; SDATE=&SDATE; EDATE=&EDATE; RPDATE=&RPDATE;
    # (informational — values held in Python variables)

    # DATA DYIBUA DYIBUA1; SET MIS.DYIBUY&REPTMON;
    mis_path = MIS_DYIBUY_PARQUET_TMPL.format(reptmon=reptmon)
    con  = duckdb.connect()
    raw  = con.execute(f"SELECT * FROM read_parquet('{mis_path}')").df()
    con.close()

    df = pl.from_pandas(raw)

    for col in ('FDI', 'FDINO', 'FDINO2'):
        if col not in df.columns:
            df = df.with_columns(pl.lit(0.0).alias(col))
        else:
            df = df.with_columns(pl.col(col).cast(pl.Float64).fill_null(0.0))

    if 'MATDATE' not in df.columns:
        df = df.with_columns(pl.lit(0).cast(pl.Int64).alias('MATDATE'))
    else:
        df = df.with_columns(pl.col('MATDATE').cast(pl.Int64).fill_null(0))

    if df['REPTDATE'].dtype != pl.Date:
        df = df.with_columns(pl.col('REPTDATE').cast(pl.Date))

    # BRCHCD = PUT(BRANCH,BRCHCD.);
    # BRCH   = PUT(BRANCH,Z3.)||'/'||BRCHCD;
    df = df.with_columns(
        pl.col('BRANCH').map_elements(format_brchcd, return_dtype=pl.Utf8).alias('BRCHCD')
    ).with_columns(
        (pl.col('BRANCH').cast(pl.Utf8).str.zfill(3) + pl.lit('/') + pl.col('BRCHCD')).alias('BRCH')
    )

    # FDDATE = INPUT(PUT(MATDATE,Z8.),YYMMDD8.);
    # %REMMTH;  MTH=PUT(REMMTH,FDFMT.);
    def _mth(s: dict) -> str:
        rd = s['REPTDATE'] if isinstance(s['REPTDATE'], date) else s['REPTDATE'].date()
        return format_fdfmt(compute_remmth(rd, s['MATDATE']))

    df = df.with_columns(
        pl.struct(['REPTDATE', 'MATDATE']).map_elements(_mth, return_dtype=pl.Utf8).alias('MTH')
    )

    # IF REPTDATE = &EDATE THEN OUTPUT DYIBUA;
    df_dyibua  = df.filter(pl.col('REPTDATE') == edate)
    # DATA _NULL_; SET DYIBUA1 (OBS=1) NOBS=N; CALL SYMPUT('N',PUT(N,8.));
    # IF &SDATE <= REPTDATE <= &EDATE THEN OUTPUT DYIBUA1;
    df_dyibua1 = df.filter((pl.col('REPTDATE') >= sdate) & (pl.col('REPTDATE') <= edate))

    # TITLE1 'REPORT ID : DIBMISA4';
    # TITLE2 'PUBLIC ISLAMIC BANK BERHAD - IBU';
    # TITLE3 'NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION';
    # TITLE4 '(16-SEP-08 ONWARDS) AS AT ' &RDATE;
    title_lines = [
        'REPORT ID : DIBMISA4',
        'PUBLIC ISLAMIC BANK BERHAD - IBU',
        'NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION',
        f'(16-SEP-08 ONWARDS) AS AT {rdate}',
        '',
    ]

    all_lines: list[str] = []

    # %TDBAL
    # PROC SORT DATA=DYIBUA OUT=DYIBUA (KEEP=BRCH MTH FDI FDINO FDINO2); BY BRANCH;
    if not df_dyibua.is_empty():
        all_lines.extend(render_tabulate(df_dyibua, 'TODATE BALANCE', title_lines))

    # DATA _NULL_; SET DYIBUA1 (OBS=1) NOBS=N; CALL SYMPUT('N',PUT(N,8.));
    n = len(df_dyibua1)

    # %AVG — %IF "&N" NE "0" %THEN %DO;
    if n != 0:
        # PROC SUMMARY DATA=DYIBUA1 NWAY; CLASS BRCH MTH; OUTPUT OUT=DYIBUA1 SUM=;
        sumdf = (
            df_dyibua1.group_by(['BRCH', 'MTH'])
            .agg([
                pl.col('FDI').sum().alias('FDI'),
                pl.col('FDINO').sum().alias('FDINO'),
                pl.col('FDINO2').sum().alias('FDINO2'),
            ])
        )
        # DATA DYIBUA1: IF _N_=1 THEN DAYS=DAY(&EDATE); VBL=VBL/DAYS;
        days  = edate.day
        sumdf = sumdf.with_columns([
            (pl.col('FDI')    / days).alias('FDI'),
            (pl.col('FDINO')  / days).alias('FDINO'),
            (pl.col('FDINO2') / days).alias('FDINO2'),
        ])
        # PROC TABULATE DATA=DYIBUA1 NOSEPS; * FORMAT MTH FDFMT.;
        all_lines.extend(render_tabulate(sumdf, 'DAILY AVERAGE', title_lines))

    final = apply_asa(all_lines, PAGE_LENGTH)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as fh:
        fh.write('\n'.join(final) + '\n')

    print(f"Report written: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
