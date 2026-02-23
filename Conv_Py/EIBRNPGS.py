# !/usr/bin/env python3
"""
Program Name : EIBRNPGS
Purpose      : Public Bank Berhad - NPGS (National Programmes Guarantee Scheme)
               CGC (Credit Guarantee Corporation) Submission Reports.
               Reads MNILN.REPTDATE for report period variables, combines
               DPNPGS / LNNPGS / BTNPGS parquet source files, filters and
               processes each Schedule code, and writes two output files per
               schedule:
                 <SCH>T — semicolon-delimited fixed-column TEXT file
                 <SCH>R — ASA carriage-control REPORT file

               Schedules produced:
                 70, 72, 51, 53, 63, 64, 65, 85, 81, 83,
                 S2, S3, H4, H5, H6, H7, F5, F6,
                 1Z, 3Z, 2Z, 4Z, 5S, 6S,
                 1H, 3H, 2H, 4H

               %INC PGM(NPGSRPT) is replaced by an import from npgsrpt.py.
"""

import os
from datetime import date, timedelta
from typing import Callable, Optional

import duckdb
import polars as pl

# %INC PGM(NPGSRPT) — imported from standalone module
from NPGSRPT import npgs_report

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

MNILN_REPTDATE_PARQUET = "input/mniln/reptdate.parquet"

NPGS_DP_PREFIX = "input/npgs/dpnpgs"   # dpnpgs<MM>.parquet
NPGS_LN_PREFIX = "input/npgs/lnnpgs"   # lnnpgs<MM>.parquet
NPGS_BT_PREFIX = "input/npgs/btnpgs"   # btnpgs<MM>.parquet

OUTPUT_DIR = "output/eibrnpgs"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def _out(code: str) -> tuple[str, str]:
    """Return (text_path, report_path) for a given schedule code."""
    tag = code.lower()
    return (
        os.path.join(OUTPUT_DIR, f"sc{tag}_text.txt"),
        os.path.join(OUTPUT_DIR, f"sc{tag}_rept.txt"),
    )


# Mapping of schedule code → (text file path, report file path)
OUTPUT_FILES: dict[str, tuple[str, str]] = {
    '70': _out('70'), '72': _out('72'), '51': _out('51'), '53': _out('53'),
    '63': _out('63'), '64': _out('64'), '65': _out('65'), '85': _out('85'),
    '81': _out('81'), '83': _out('83'), 'S2': _out('s2'), 'S3': _out('s3'),
    'H4': _out('h4'), 'H5': _out('h5'), 'H6': _out('h6'), 'H7': _out('h7'),
    'F5': _out('f5'), 'F6': _out('f6'), '1Z': _out('1z'), '3Z': _out('3z'),
    '2Z': _out('2z'), '4Z': _out('4z'), '5S': _out('5s'), '6S': _out('6s'),
    '1H': _out('1h'), '3H': _out('3h'), '2H': _out('2h'), '4H': _out('4h'),
}

# =============================================================================
# DATE / UTILITY HELPERS
# =============================================================================

def _sas_date_to_pydate(val) -> Optional[date]:
    """Convert SAS date integer (days since 1960-01-01) to Python date."""
    if val is None or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, date):
        return val
    return None


def _fmt_ddmmyy10(val) -> str:
    """Format date as DD/MM/YYYY (SAS DDMMYY10. format)."""
    if val is None:
        return '          '
    if isinstance(val, (int, float)):
        val = _sas_date_to_pydate(val)
    if val is None:
        return '          '
    return val.strftime('%d/%m/%Y')


def _fmt_numeric(val, width: int, decimals: int) -> str:
    """Right-justify numeric to <width> with <decimals> decimal places."""
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * width
    v = float(val)
    s = f"{v:{width}.{decimals}f}" if decimals > 0 else f"{int(round(v)):{width}d}"
    return s[-width:] if len(s) > width else s


def _fmt_comma20_2(val) -> str:
    """Format as SAS COMMA20.2 (thousand-separated, 2 decimal places)."""
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * 20
    v   = float(val)
    s   = f"{abs(v):,.2f}"
    s   = ('-' + s) if v < 0 else s
    return s.rjust(20)


def _coalesce_s(val, default: str = '') -> str:
    return str(val).strip() if val is not None else default


def _coalesce_i(val, default: int = 0) -> int:
    if val is None or (isinstance(val, float) and val != val):
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

# =============================================================================
# REPORT DATE VARIABLES
# =============================================================================

def _get_report_vars() -> dict:
    """
    DATA REPTDATE (KEEP=REPTDATE):
      SET MNILN.REPTDATE;
      MM  = MONTH(REPTDATE);
      MM1 = MM - 1;
      IF MM1 = 0 THEN MM1 = 12;
      CALL SYMPUT('REPTMON',  PUT(MM,  Z2.));
      CALL SYMPUT('REPTMON1', PUT(MM1, Z2.));
      CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR4.));
      CALL SYMPUT('REPTDAY',  PUT(DAY(REPTDATE), Z2.));
      CALL SYMPUT('RDATE',    PUT(REPTDATE, DDMMYY8.));
      CALL SYMPUT('NDATE',    PUT(REPTDATE, Z5.));
    """
    con      = duckdb.connect()
    row      = con.execute(
        f"SELECT reptdate FROM read_parquet('{MNILN_REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate  = _sas_date_to_pydate(row[0])
    mm        = reptdate.month
    mm1       = mm - 1 if mm > 1 else 12
    ndate_int = (reptdate - date(1960, 1, 1)).days

    return {
        'reptdate':  reptdate,
        'reptmon':   str(mm).zfill(2),
        'reptmon1':  str(mm1).zfill(2),
        'reptyear':  str(reptdate.year),
        'reptday':   str(reptdate.day).zfill(2),
        'rdate':     reptdate.strftime('%d/%m/%y'),    # DDMMYY8. → DD/MM/YY
        'ndate':     str(ndate_int).zfill(5),
    }

# =============================================================================
# PARQUET LOADER
# =============================================================================

def _load_parquet(path: str) -> pl.DataFrame:
    """Load a parquet file via DuckDB; returns empty DataFrame if not found."""
    if not os.path.exists(path):
        return pl.DataFrame()
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    return df

# =============================================================================
# COMMON NPGS FILTER
# =============================================================================

def _apply_common_filters(
    df:                           pl.DataFrame,
    cvar12_lt3_blank:             bool = True,
    cvar12_blank_to_ap:           bool = False,
    cvar12_else_npl:              bool = False,
    cvar12_else_npf:              bool = False,
    cvar13_blank_if_cvar12_blank: bool = False,
) -> pl.DataFrame:
    """
    Apply filters common to all NPGS schedule DATA steps:

        IF NATGUAR='06' AND CINSTCL='18';
        CVARXX = '          ';
        IF CVAR11 < 3 THEN CVAR12='   ';

    Optional per-schedule CVAR12 / CVAR13 rules (controlled by flags):
        cvar12_blank_to_ap           IF CVAR12=' '    THEN CVAR12='AP'
                                     (SCH S2, S3, H4, H5)
        cvar12_else_npl              ELSE CVAR12='NPL'
                                     (SCH 1Z, 3Z, 5S, 6S, 1H, 3H)
        cvar12_else_npf              ELSE CVAR12='NPF'
                                     (SCH F6, 2Z, 4Z, 2H, 4H)
        cvar13_blank_if_cvar12_blank IF CVAR12='   '  THEN CVAR13='          '
                                     (SCH H6, H7)
    """
    if df.is_empty():
        return df

    # IF NATGUAR='06' AND CINSTCL='18'
    if 'natguar' in df.columns and 'cinstcl' in df.columns:
        df = df.filter(
            (pl.col('natguar').cast(pl.Utf8).str.strip_chars() == '06') &
            (pl.col('cinstcl').cast(pl.Utf8).str.strip_chars() == '18')
        )
    if df.is_empty():
        return df

    # CVARXX = '          '
    df = df.with_columns(pl.lit('          ').alias('cvarxx'))

    # Row-level CVAR12 / CVAR13 logic (requires scalar access)
    rows = df.to_dicts()
    for row in rows:
        cvar11 = _coalesce_i(row.get('cvar11'))

        if cvar12_lt3_blank:
            if cvar11 < 3:
                row['cvar12'] = '   '
            else:
                # ELSE rules apply only when CVAR11 >= 3
                if cvar12_else_npl:
                    row['cvar12'] = 'NPL'
                elif cvar12_else_npf:
                    row['cvar12'] = 'NPF'

        # IF CVAR12=' ' THEN CVAR12='AP'  (evaluated after the <3 check)
        if cvar12_blank_to_ap and _coalesce_s(row.get('cvar12')) in (' ', ''):
            row['cvar12'] = 'AP'

        # IF CVAR12='   ' THEN CVAR13='          '
        if cvar13_blank_if_cvar12_blank and _coalesce_s(row.get('cvar12')) in ('   ', ''):
            row['cvar13'] = '          '

    return pl.from_dicts(rows) if rows else pl.DataFrame()

# =============================================================================
# TEXT FILE WRITERS
# =============================================================================

def _write_text_standard(df: pl.DataFrame, filepath: str) -> None:
    """
    Write standard fixed-column semicolon-delimited TEXT file used by most
    schedules (70, 72, 51, 53, 63, 64, 65, 85, 81, 83, S2, S3, H4, H5, H6,
    H7, 1Z, 3Z, 2Z, 4Z, 5S, 6S):

        PUT @001 CVAR01   10.       ';'
            @012 CVAR02   $2.       ';'
            @015 CVAR03   $15.      ';'
            @031 CVAR04   $50.      ';'
            @082 CVAR05   DDMMYY10. ';'
            @093 CVARXX   $10.
            @103 CVAR06   10.       ';'
            @114 CVAR07   $2.       ';'
            @117 CVAR08   13.2      ';'
            @131 CVAR09   13.2      ';'
            @145 CVAR10   13.2      ';'
            @159 CVAR11   5.        ';'
            @165 CVAR12   $3.       ';'
            @169 CVAR13   $10.      ';'
            @180 CVAR14   $4.       ';'
            @185 CVAR15   $5.       ';'
    """
    if df.is_empty():
        open(filepath, 'w').close()
        return

    with open(filepath, 'w', encoding='utf-8', newline='\n') as fh:
        for row in df.iter_rows(named=True):
            buf = bytearray(b' ' * 190)

            def ps(pos: int, val, w: int) -> None:
                s = _coalesce_s(val)[:w].ljust(w)
                buf[pos-1:pos-1+w] = s.encode('utf-8', errors='replace')[:w]

            def pn(pos: int, val, w: int, d: int) -> None:
                s = _fmt_numeric(val, w, d)
                buf[pos-1:pos-1+w] = s.encode('utf-8', errors='replace')[:w]

            def pd(pos: int, val) -> None:
                s = _fmt_ddmmyy10(val).ljust(10)
                buf[pos-1:pos-1+10] = s.encode('utf-8', errors='replace')[:10]

            def sc(pos: int) -> None:
                buf[pos-1:pos] = b';'

            pn(1,   row.get('cvar01'), 10, 0); sc(11)
            ps(12,  row.get('cvar02'),  2);    sc(14)
            ps(15,  row.get('cvar03'), 15);    sc(30)
            ps(31,  row.get('cvar04'), 50);    sc(81)
            pd(82,  row.get('cvar05'));         sc(92)
            ps(93,  row.get('cvarxx'), 10)
            pn(103, row.get('cvar06'), 10, 0); sc(113)
            ps(114, row.get('cvar07'),  2);    sc(116)
            pn(117, row.get('cvar08'), 13, 2); sc(130)
            pn(131, row.get('cvar09'), 13, 2); sc(144)
            pn(145, row.get('cvar10'), 13, 2); sc(158)
            pn(159, row.get('cvar11'),  5, 0); sc(164)
            ps(165, row.get('cvar12'),  3);    sc(168)
            ps(169, row.get('cvar13'), 10);    sc(179)
            ps(180, row.get('cvar14'),  4);    sc(184)
            ps(185, row.get('cvar15'),  5)

            fh.write(buf.decode('utf-8', errors='replace').rstrip() + '\n')


def _write_text_f5_f6(df: pl.DataFrame, filepath: str) -> None:
    """
    Write fixed-column TEXT file for SCH=F5 and SCH=F6.
    Identical to the standard layout except CVAR09 and CVAR10 are SWAPPED:

        @131 CVAR10  13.2  ';'   <- CVAR10 written at column @131
        @145 CVAR09  13.2  ';'   <- CVAR09 written at column @145
    """
    if df.is_empty():
        open(filepath, 'w').close()
        return

    with open(filepath, 'w', encoding='utf-8', newline='\n') as fh:
        for row in df.iter_rows(named=True):
            buf = bytearray(b' ' * 190)

            def ps(pos: int, val, w: int) -> None:
                s = _coalesce_s(val)[:w].ljust(w)
                buf[pos-1:pos-1+w] = s.encode('utf-8', errors='replace')[:w]

            def pn(pos: int, val, w: int, d: int) -> None:
                s = _fmt_numeric(val, w, d)
                buf[pos-1:pos-1+w] = s.encode('utf-8', errors='replace')[:w]

            def pd(pos: int, val) -> None:
                s = _fmt_ddmmyy10(val).ljust(10)
                buf[pos-1:pos-1+10] = s.encode('utf-8', errors='replace')[:10]

            def sc(pos: int) -> None:
                buf[pos-1:pos] = b';'

            pn(1,   row.get('cvar01'), 10, 0); sc(11)
            ps(12,  row.get('cvar02'),  2);    sc(14)
            ps(15,  row.get('cvar03'), 15);    sc(30)
            ps(31,  row.get('cvar04'), 50);    sc(81)
            pd(82,  row.get('cvar05'));         sc(92)
            ps(93,  row.get('cvarxx'), 10)
            pn(103, row.get('cvar06'), 10, 0); sc(113)
            ps(114, row.get('cvar07'),  2);    sc(116)
            pn(117, row.get('cvar08'), 13, 2); sc(130)
            # F5/F6: CVAR10 at @131, CVAR09 at @145 (swapped vs standard)
            pn(131, row.get('cvar10'), 13, 2); sc(144)
            pn(145, row.get('cvar09'), 13, 2); sc(158)
            pn(159, row.get('cvar11'),  5, 0); sc(164)
            ps(165, row.get('cvar12'),  3);    sc(168)
            ps(169, row.get('cvar13'), 10);    sc(179)
            ps(180, row.get('cvar14'),  4);    sc(184)
            ps(185, row.get('cvar15'),  5)

            fh.write(buf.decode('utf-8', errors='replace').rstrip() + '\n')


def _write_text_dsd(df: pl.DataFrame, filepath: str) -> None:
    """
    Write DSD DLM=';' TEXT file for SCH=1H, 2H, 3H, 4H:

        FORMAT CVAR05 DDMMYY10. CVAR08 CVAR09 CVAR10 COMMA20.2;
        FILE ... DLM=';' DSD;
        PUT @1 CVAR01 CVAR02 ... CVAR15;

    Fields are semicolon-separated; any field containing ';' is double-quoted
    (DSD behaviour). CVAR08/09/10 use COMMA20.2 formatting.
    """
    if df.is_empty():
        open(filepath, 'w').close()
        return

    def _dsd(val, is_date: bool = False,
             is_amount: bool = False, is_num: bool = False) -> str:
        if is_date:
            s = _fmt_ddmmyy10(val)
        elif is_amount:
            s = _fmt_comma20_2(val)
        elif is_num:
            s = _fmt_numeric(val, 10, 0)
        else:
            s = _coalesce_s(val)
        # DSD: double-quote fields that contain the delimiter
        if ';' in s:
            s = '"' + s.replace('"', '""') + '"'
        return s

    with open(filepath, 'w', encoding='utf-8', newline='\n') as fh:
        for row in df.iter_rows(named=True):
            fields = [
                _dsd(row.get('cvar01'), is_num=True),
                _dsd(row.get('cvar02')),
                _dsd(row.get('cvar03')),
                _dsd(row.get('cvar04')),
                _dsd(row.get('cvar05'), is_date=True),
                _dsd(row.get('cvar06'), is_num=True),
                _dsd(row.get('cvar07')),
                _dsd(row.get('cvar08'), is_amount=True),
                _dsd(row.get('cvar09'), is_amount=True),
                _dsd(row.get('cvar10'), is_amount=True),
                _dsd(row.get('cvar11'), is_num=True),
                _dsd(row.get('cvar12')),
                _dsd(row.get('cvar13')),
                _dsd(row.get('cvar14')),
                _dsd(row.get('cvar15')),
            ]
            fh.write(';'.join(fields) + '\n')

# =============================================================================
# SCHEDULE PROCESSOR
# =============================================================================

def _process_schedule(
    df:              pl.DataFrame,
    sch_code:        str,
    rdate:           str,
    text_writer:     Callable[[pl.DataFrame, str], None],
    bank_name:       str           = 'PUBLIC BANK BERHAD',
    cvar07_override: Optional[str] = None,
) -> None:
    """
    Common per-schedule processing block equivalent to:

        (optional) CVAR07 = '<override>';
        PROC SORT; BY CVAR01 CVAR06;
        DATA SC<xx>T; SET NPGS; FILE SC<xx>T; PUT ...; <- text_writer()
        PROC PRINTTO PRINT=SC<xx>R;
        TITLE1 '<bank_name>';
        TITLE2 'DETAIL OF ACCTS (SCH=<sch_code>) FOR SUBMISSION TO CGC @ &RDATE';
        %INC PGM(NPGSRPT);                             <- npgs_report()
    """
    text_path, report_path = OUTPUT_FILES[sch_code]

    if df.is_empty():
        open(text_path,   'w').close()
        open(report_path, 'w').close()
        return

    # Optionally override CVAR07 (e.g. 'TL', 'TF', 'FL')
    if cvar07_override is not None:
        df = df.with_columns(pl.lit(cvar07_override).alias('cvar07'))

    # PROC SORT BY CVAR01 CVAR06
    sort_cols = [c for c in ('cvar01', 'cvar06') if c in df.columns]
    if sort_cols:
        df = df.sort(sort_cols)

    # DATA SC<xx>T — write semicolon-delimited text file
    text_writer(df, text_path)

    # PROC PRINTTO + TITLE1/TITLE2 + %INC PGM(NPGSRPT) — write ASA report
    npgs_report(
        df          = df,
        report_path = report_path,
        title1      = bank_name,
        title2      = (
            f"DETAIL OF ACCTS (SCH={sch_code}) FOR SUBMISSION TO CGC @ {rdate}"
        ),
    )

# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("EIBRNPGS: Starting NPGS CGC submission report processing...")

    rv      = _get_report_vars()
    reptmon = rv['reptmon']
    rdate   = rv['rdate']
    print(f"  Report date : {rv['reptdate']}  MM={reptmon}  YEAR={rv['reptyear']}")

    # Load NPGS source parquet files once for reuse across all schedules
    dp_df = _load_parquet(f"{NPGS_DP_PREFIX}{reptmon}.parquet")
    ln_df = _load_parquet(f"{NPGS_LN_PREFIX}{reptmon}.parquet")
    bt_df = _load_parquet(f"{NPGS_BT_PREFIX}{reptmon}.parquet")

    # -------------------------------------------------------------------------
    # DATA NPGS.NPLA:
    #   SET NPGS.DPNPGS&REPTMON NPGS.LNNPGS&REPTMON NPGS.BTNPGS&REPTMON;
    #   IF  CVAR13 NE '         ';
    #   NDATE  = CVAR13;
    #   STATUS = CVAR12;
    #   KEEP CVAR01 CVAR06 STATUS NDATE;
    # (NPGS.NPLA is an internal SAS dataset; no text/report output required)
    # -------------------------------------------------------------------------

    # =========================================================================
    # SCH=70
    # =========================================================================
    # DATA DP70:
    #   SET NPGS.DPNPGS&REPTMON;
    #   IF  CVAR02 IN ('70','71','XX','YY');
    #   IF  CVAR05 > '01JAN07'D;
    #   IF  CVAR01 IN (70000162,70323600,70327400,70355700,
    #                  70342100,70344500,70351440,70366400,70372400)
    #       THEN DELETE;
    # DATA LN70:
    #   SET NPGS.LNNPGS&REPTMON;
    #   IF  CVAR02 IN ('70','71','XX','YY');
    #   IF  CVAR05 > '01JAN07'D;
    # DATA NPGS:
    #   SET DP70 LN70;
    #   IF  NATGUAR='06' AND CINSTCL='18';
    #   IF  CVAR02='XX' THEN CVAR02='  ';
    #   IF  CVAR02='YY' THEN CVAR02='  ';
    #   CVARXX='          ';
    #   IF  CVAR11 < 3  THEN CVAR12='   ';

    _excl_70 = {70000162, 70323600, 70327400, 70355700,
                70342100, 70344500, 70351440, 70366400, 70372400}
    _dt_jan07 = (date(2007, 1, 1) - date(1960, 1, 1)).days   # 17168

    dp70 = pl.DataFrame()
    if not dp_df.is_empty():
        dp70 = dp_df.filter(
            pl.col('cvar02').cast(pl.Utf8).str.strip_chars().is_in(['70','71','XX','YY'])
        )
        if 'cvar05' in dp70.columns:
            dp70 = dp70.filter(pl.col('cvar05').cast(pl.Float64) > _dt_jan07)
        if 'cvar01' in dp70.columns:
            dp70 = dp70.filter(
                ~pl.col('cvar01').cast(pl.Int64).is_in(list(_excl_70))
            )

    ln70 = pl.DataFrame()
    if not ln_df.is_empty():
        ln70 = ln_df.filter(
            pl.col('cvar02').cast(pl.Utf8).str.strip_chars().is_in(['70','71','XX','YY'])
        )
        if 'cvar05' in ln70.columns:
            ln70 = ln70.filter(pl.col('cvar05').cast(pl.Float64) > _dt_jan07)

    npgs70_frames = [f for f in (dp70, ln70) if not f.is_empty()]
    npgs70 = pl.concat(npgs70_frames, how='diagonal') if npgs70_frames else pl.DataFrame()
    if not npgs70.is_empty():
        # IF CVAR02='XX' THEN CVAR02='  ';  IF CVAR02='YY' THEN CVAR02='  ';
        npgs70 = npgs70.with_columns(
            pl.when(pl.col('cvar02').cast(pl.Utf8).str.strip_chars().is_in(['XX','YY']))
              .then(pl.lit('  '))
              .otherwise(pl.col('cvar02'))
              .alias('cvar02')
        )
        npgs70 = _apply_common_filters(npgs70)

    _process_schedule(npgs70, '70', rdate, _write_text_standard)
    print("  SCH=70 done")

    # =========================================================================
    # SCH=72
    # =========================================================================
    # DATA DP72:
    #   SET NPGS.DPNPGS&REPTMON;
    #   IF  CVAR02='72';
    #   IF  CVAR05 GT 17167;
    #   IF  CVAR06='3131612218' THEN DELETE;
    # DATA XX72:
    #   SET NPGS.LNNPGS&REPTMON NPGS.BTNPGS&REPTMON;
    #   IF  CVAR02='72';
    # DATA NPGS:
    #   SET DP72 XX72;
    #   IF  NATGUAR='06' AND CINSTCL='18';
    #   CVARXX='          ';
    #   IF  CVAR11 < 3  THEN CVAR12='   ';

    dp72 = pl.DataFrame()
    if not dp_df.is_empty():
        dp72 = dp_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '72')
        if 'cvar05' in dp72.columns:
            dp72 = dp72.filter(pl.col('cvar05').cast(pl.Float64) > 17167)
        if 'cvar06' in dp72.columns:
            dp72 = dp72.filter(
                pl.col('cvar06').cast(pl.Utf8).str.strip_chars() != '3131612218'
            )

    xx72_frames = [
        f.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '72')
        for f in (ln_df, bt_df) if not f.is_empty()
    ]
    xx72_frames = [f for f in xx72_frames if not f.is_empty()]
    xx72 = pl.concat(xx72_frames, how='diagonal') if xx72_frames else pl.DataFrame()

    npgs72_frames = [f for f in (dp72, xx72) if not f.is_empty()]
    npgs72 = pl.concat(npgs72_frames, how='diagonal') if npgs72_frames else pl.DataFrame()
    if not npgs72.is_empty():
        npgs72 = _apply_common_filters(npgs72)

    _process_schedule(npgs72, '72', rdate, _write_text_standard)
    print("  SCH=72 done")

    # =========================================================================
    # SCH=51
    # =========================================================================
    # DATA NPGS:
    #   SET NPGS.BTNPGS&REPTMON NPGS.LNNPGS&REPTMON NPGS.DPNPGS&REPTMON;
    #   IF  CVAR02='51';
    #   IF  NATGUAR='06' AND CINSTCL='18';
    #   CVARXX='          ';
    #   IF  CVAR11 < 3  THEN CVAR12='   ';

    npgs51_frames = [
        f.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '51')
        for f in (bt_df, ln_df, dp_df) if not f.is_empty()
    ]
    npgs51_frames = [f for f in npgs51_frames if not f.is_empty()]
    npgs51 = pl.concat(npgs51_frames, how='diagonal') if npgs51_frames else pl.DataFrame()
    if not npgs51.is_empty():
        npgs51 = _apply_common_filters(npgs51)

    _process_schedule(npgs51, '51', rdate, _write_text_standard)
    print("  SCH=51 done")

    # =========================================================================
    # SCH=53
    # =========================================================================
    # DATA NPGS:
    #   SET NPGS.BTNPGS&REPTMON NPGS.LNNPGS&REPTMON NPGS.DPNPGS&REPTMON;
    #   IF  CVAR02='53';

    npgs53_frames = [
        f.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '53')
        for f in (bt_df, ln_df, dp_df) if not f.is_empty()
    ]
    npgs53_frames = [f for f in npgs53_frames if not f.is_empty()]
    npgs53 = pl.concat(npgs53_frames, how='diagonal') if npgs53_frames else pl.DataFrame()
    if not npgs53.is_empty():
        npgs53 = _apply_common_filters(npgs53)

    _process_schedule(npgs53, '53', rdate, _write_text_standard)
    print("  SCH=53 done")

    # =========================================================================
    # SCH=63 — LN only
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; IF CVAR02='63';

    npgs63 = pl.DataFrame()
    if not ln_df.is_empty():
        npgs63 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '63')
        if not npgs63.is_empty():
            npgs63 = _apply_common_filters(npgs63)

    _process_schedule(npgs63, '63', rdate, _write_text_standard)
    print("  SCH=63 done")

    # =========================================================================
    # SCH=64 — LN only
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; IF CVAR02='64';

    npgs64 = pl.DataFrame()
    if not ln_df.is_empty():
        npgs64 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '64')
        if not npgs64.is_empty():
            npgs64 = _apply_common_filters(npgs64)

    _process_schedule(npgs64, '64', rdate, _write_text_standard)
    print("  SCH=64 done")

    # =========================================================================
    # SCH=65
    # =========================================================================
    # DATA NPGS:
    #   SET NPGS.DPNPGS&REPTMON NPGS.LNNPGS&REPTMON NPGS.BTNPGS&REPTMON;
    #   IF  CVAR02='65';

    npgs65_frames = [
        f.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '65')
        for f in (dp_df, ln_df, bt_df) if not f.is_empty()
    ]
    npgs65_frames = [f for f in npgs65_frames if not f.is_empty()]
    npgs65 = pl.concat(npgs65_frames, how='diagonal') if npgs65_frames else pl.DataFrame()
    if not npgs65.is_empty():
        npgs65 = _apply_common_filters(npgs65)

    _process_schedule(npgs65, '65', rdate, _write_text_standard)
    print("  SCH=65 done")

    # =========================================================================
    # SCH=85
    # =========================================================================
    # DATA NPGS:
    #   SET NPGS.DPNPGS&REPTMON NPGS.LNNPGS&REPTMON NPGS.BTNPGS&REPTMON;
    #   IF  CVAR02='85';

    npgs85_frames = [
        f.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '85')
        for f in (dp_df, ln_df, bt_df) if not f.is_empty()
    ]
    npgs85_frames = [f for f in npgs85_frames if not f.is_empty()]
    npgs85 = pl.concat(npgs85_frames, how='diagonal') if npgs85_frames else pl.DataFrame()
    if not npgs85.is_empty():
        npgs85 = _apply_common_filters(npgs85)

    _process_schedule(npgs85, '85', rdate, _write_text_standard)
    print("  SCH=85 done")

    # =========================================================================
    # SCH=81 — LN only
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; IF CVAR02='81';

    npgs81 = pl.DataFrame()
    if not ln_df.is_empty():
        npgs81 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '81')
        if not npgs81.is_empty():
            npgs81 = _apply_common_filters(npgs81)

    _process_schedule(npgs81, '81', rdate, _write_text_standard)
    print("  SCH=81 done")

    # =========================================================================
    # SCH=83 — LN only
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; IF CVAR02='83';

    npgs83 = pl.DataFrame()
    if not ln_df.is_empty():
        npgs83 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '83')
        if not npgs83.is_empty():
            npgs83 = _apply_common_filters(npgs83)

    _process_schedule(npgs83, '83', rdate, _write_text_standard)
    print("  SCH=83 done")

    # =========================================================================
    # SCH=S2 — LN only
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; IF CVAR02='S2';
    #   IF  NATGUAR='06' AND CINSTCL='18';
    #   CVARXX='          ';
    #   IF  CVAR11 < 3   THEN CVAR12='   ';
    #   IF  CVAR12 = ' ' THEN CVAR12='AP';
    # TITLE1 'PUBLIC BANK BERHAD';

    npgsS2 = pl.DataFrame()
    if not ln_df.is_empty():
        npgsS2 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == 'S2')
        if not npgsS2.is_empty():
            npgsS2 = _apply_common_filters(npgsS2, cvar12_blank_to_ap=True)

    _process_schedule(npgsS2, 'S2', rdate, _write_text_standard,
                      bank_name='PUBLIC BANK BERHAD')
    print("  SCH=S2 done")

    # =========================================================================
    # SCH=S3 — LN only
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; IF CVAR02='S3';
    #   same CVAR12 rules as S2
    # TITLE1 'PUBLIC ISLAMIC BANK BERHAD';

    npgsS3 = pl.DataFrame()
    if not ln_df.is_empty():
        npgsS3 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == 'S3')
        if not npgsS3.is_empty():
            npgsS3 = _apply_common_filters(npgsS3, cvar12_blank_to_ap=True)

    _process_schedule(npgsS3, 'S3', rdate, _write_text_standard,
                      bank_name='PUBLIC ISLAMIC BANK BERHAD')
    print("  SCH=S3 done")

    # =========================================================================
    # /* ESMR 2018-248 */
    # SCH=H4 — LN only; CVAR07='TL'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='TL'; IF CVAR02='H4';
    #   IF  NATGUAR='06' AND CINSTCL='18';
    #   CVARXX='          ';
    #   IF  CVAR11 < 3   THEN CVAR12='   ';
    #   IF  CVAR12 = ' ' THEN CVAR12='AP';
    # TITLE1 'PUBLIC BANK BERHAD';

    npgsH4 = pl.DataFrame()
    if not ln_df.is_empty():
        npgsH4 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == 'H4')
        if not npgsH4.is_empty():
            npgsH4 = _apply_common_filters(npgsH4, cvar12_blank_to_ap=True)

    _process_schedule(npgsH4, 'H4', rdate, _write_text_standard,
                      bank_name='PUBLIC BANK BERHAD',
                      cvar07_override='TL')
    print("  SCH=H4 done")

    # =========================================================================
    # SCH=H5 — LN only; CVAR07='TF'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='TF'; IF CVAR02='H5';
    #   same CVAR12 rules as H4
    # TITLE1 'PUBLIC ISLAMIC BANK BERHAD';

    npgsH5 = pl.DataFrame()
    if not ln_df.is_empty():
        npgsH5 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == 'H5')
        if not npgsH5.is_empty():
            npgsH5 = _apply_common_filters(npgsH5, cvar12_blank_to_ap=True)

    _process_schedule(npgsH5, 'H5', rdate, _write_text_standard,
                      bank_name='PUBLIC ISLAMIC BANK BERHAD',
                      cvar07_override='TF')
    print("  SCH=H5 done")

    # =========================================================================
    # /* SMR 2020-1197 */
    # SCH=H6 — LN only; CVAR07='FL'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='FL'; IF CVAR02='H6';
    #   IF  NATGUAR='06' AND CINSTCL='18';
    #   CVARXX='          ';
    #   IF  CVAR11 < 3   THEN CVAR12='   ';
    #   IF CVAR12='   '  THEN CVAR13='          ';
    # TITLE1 'PUBLIC BANK BERHAD';

    npgsH6 = pl.DataFrame()
    if not ln_df.is_empty():
        npgsH6 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == 'H6')
        if not npgsH6.is_empty():
            npgsH6 = _apply_common_filters(npgsH6,
                                            cvar13_blank_if_cvar12_blank=True)

    _process_schedule(npgsH6, 'H6', rdate, _write_text_standard,
                      bank_name='PUBLIC BANK BERHAD',
                      cvar07_override='FL')
    print("  SCH=H6 done")

    # =========================================================================
    # SCH=H7 — LN only; CVAR07='TF'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='TF'; IF CVAR02='H7';
    #   IF  NATGUAR='06' AND CINSTCL='18';
    #   CVARXX='          ';
    #   IF  CVAR11 < 3   THEN CVAR12='   ';
    #   IF  CVAR12='   ' THEN CVAR13='          ';
    # TITLE1 'PUBLIC ISLAMIC BANK BERHAD';

    npgsH7 = pl.DataFrame()
    if not ln_df.is_empty():
        npgsH7 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == 'H7')
        if not npgsH7.is_empty():
            npgsH7 = _apply_common_filters(npgsH7,
                                            cvar13_blank_if_cvar12_blank=True)

    _process_schedule(npgsH7, 'H7', rdate, _write_text_standard,
                      bank_name='PUBLIC ISLAMIC BANK BERHAD',
                      cvar07_override='TF')
    print("  SCH=H7 done")

    # =========================================================================
    # /* SMR 2020-2839 */
    # SCH=F5 — LN only; CVAR07='FL'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='FL'; IF CVAR02='F5';
    #   IF  NATGUAR='06' AND CINSTCL='18';
    #   CVARXX='          ';
    #   IF  CVAR11 < 3   THEN CVAR12='   ';
    # (no ELSE rule for CVAR12 on F5)
    # Text output uses swapped CVAR09/CVAR10 column positions.
    # TITLE1 'PUBLIC BANK BERHAD';

    npgsF5 = pl.DataFrame()
    if not ln_df.is_empty():
        npgsF5 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == 'F5')
        if not npgsF5.is_empty():
            npgsF5 = _apply_common_filters(npgsF5)

    _process_schedule(npgsF5, 'F5', rdate, _write_text_f5_f6,
                      bank_name='PUBLIC BANK BERHAD',
                      cvar07_override='FL')
    print("  SCH=F5 done")

    # =========================================================================
    # SCH=F6 — LN only; CVAR07='TF'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='TF'; IF CVAR02='F6';
    #   IF  NATGUAR='06' AND CINSTCL='18';
    #   CVARXX='          ';
    #   IF  CVAR11 < 3   THEN CVAR12='   ';
    #                    ELSE CVAR12='NPF';
    # Text output uses swapped CVAR09/CVAR10 column positions.
    # TITLE1 'PUBLIC ISLAMIC BANK BERHAD';

    npgsF6 = pl.DataFrame()
    if not ln_df.is_empty():
        npgsF6 = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == 'F6')
        if not npgsF6.is_empty():
            npgsF6 = _apply_common_filters(npgsF6, cvar12_else_npf=True)

    _process_schedule(npgsF6, 'F6', rdate, _write_text_f5_f6,
                      bank_name='PUBLIC ISLAMIC BANK BERHAD',
                      cvar07_override='TF')
    print("  SCH=F6 done")

    # =========================================================================
    # /* SMR 2021-2790 */
    # SCH=1Z — LN only; CVAR07='FL'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='FL'; IF CVAR02='1Z';
    #   IF  NATGUAR='06' AND CINSTCL='18';
    #   CVARXX='          ';
    #   IF  CVAR11 < 3   THEN CVAR12='   ';
    #                    ELSE CVAR12='NPL';
    # TITLE1 'PUBLIC BANK BERHAD';

    npgs1Z = pl.DataFrame()
    if not ln_df.is_empty():
        npgs1Z = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '1Z')
        if not npgs1Z.is_empty():
            npgs1Z = _apply_common_filters(npgs1Z, cvar12_else_npl=True)

    _process_schedule(npgs1Z, '1Z', rdate, _write_text_standard,
                      bank_name='PUBLIC BANK BERHAD',
                      cvar07_override='FL')
    print("  SCH=1Z done")

    # =========================================================================
    # SCH=3Z — LN only; CVAR07='FL'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='FL'; IF CVAR02='3Z';
    #   ELSE CVAR12='NPL';
    # TITLE1 'PUBLIC BANK BERHAD';

    npgs3Z = pl.DataFrame()
    if not ln_df.is_empty():
        npgs3Z = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '3Z')
        if not npgs3Z.is_empty():
            npgs3Z = _apply_common_filters(npgs3Z, cvar12_else_npl=True)

    _process_schedule(npgs3Z, '3Z', rdate, _write_text_standard,
                      bank_name='PUBLIC BANK BERHAD',
                      cvar07_override='FL')
    print("  SCH=3Z done")

    # =========================================================================
    # SCH=2Z — LN only; CVAR07='TF'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='TF'; IF CVAR02='2Z';
    #   ELSE CVAR12='NPF';
    # TITLE1 'PUBLIC ISLAMIC BANK BERHAD';

    npgs2Z = pl.DataFrame()
    if not ln_df.is_empty():
        npgs2Z = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '2Z')
        if not npgs2Z.is_empty():
            npgs2Z = _apply_common_filters(npgs2Z, cvar12_else_npf=True)

    _process_schedule(npgs2Z, '2Z', rdate, _write_text_standard,
                      bank_name='PUBLIC ISLAMIC BANK BERHAD',
                      cvar07_override='TF')
    print("  SCH=2Z done")

    # =========================================================================
    # SCH=4Z — LN only; CVAR07='TF'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='TF'; IF CVAR02='4Z';
    #   ELSE CVAR12='NPF';
    # TITLE1 'PUBLIC ISLAMIC BANK BERHAD';

    npgs4Z = pl.DataFrame()
    if not ln_df.is_empty():
        npgs4Z = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '4Z')
        if not npgs4Z.is_empty():
            npgs4Z = _apply_common_filters(npgs4Z, cvar12_else_npf=True)

    _process_schedule(npgs4Z, '4Z', rdate, _write_text_standard,
                      bank_name='PUBLIC ISLAMIC BANK BERHAD',
                      cvar07_override='TF')
    print("  SCH=4Z done")

    # =========================================================================
    # SCH=5S — LN only; CVAR07='FL'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='FL'; IF CVAR02='5S';
    #   ELSE CVAR12='NPL';
    # TITLE1 'PUBLIC BANK BERHAD';

    npgs5S = pl.DataFrame()
    if not ln_df.is_empty():
        npgs5S = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '5S')
        if not npgs5S.is_empty():
            npgs5S = _apply_common_filters(npgs5S, cvar12_else_npl=True)

    _process_schedule(npgs5S, '5S', rdate, _write_text_standard,
                      bank_name='PUBLIC BANK BERHAD',
                      cvar07_override='FL')
    print("  SCH=5S done")

    # =========================================================================
    # SCH=6S — LN only; CVAR07='TF'
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='TF'; IF CVAR02='6S';
    #   ELSE CVAR12='NPL';
    # TITLE1 'PUBLIC ISLAMIC BANK BERHAD';

    npgs6S = pl.DataFrame()
    if not ln_df.is_empty():
        npgs6S = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '6S')
        if not npgs6S.is_empty():
            npgs6S = _apply_common_filters(npgs6S, cvar12_else_npl=True)

    _process_schedule(npgs6S, '6S', rdate, _write_text_standard,
                      bank_name='PUBLIC ISLAMIC BANK BERHAD',
                      cvar07_override='TF')
    print("  SCH=6S done")

    # =========================================================================
    # /* SMR 2022-1594 */
    # SCH=1H — LN only; CVAR07='FL'; DSD text output
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='FL'; IF CVAR02='1H';
    #   ELSE CVAR12='NPL';
    #   FORMAT CVAR05 DDMMYY10. CVAR08 CVAR09 CVAR10 COMMA20.2;
    #   FILE SC1HT DLM=';' DSD;
    # TITLE1 'PUBLIC BANK BERHAD';

    npgs1H = pl.DataFrame()
    if not ln_df.is_empty():
        npgs1H = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '1H')
        if not npgs1H.is_empty():
            npgs1H = _apply_common_filters(npgs1H, cvar12_else_npl=True)

    _process_schedule(npgs1H, '1H', rdate, _write_text_dsd,
                      bank_name='PUBLIC BANK BERHAD',
                      cvar07_override='FL')
    print("  SCH=1H done")

    # =========================================================================
    # SCH=3H — LN only; CVAR07='FL'; DSD text output
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='FL'; IF CVAR02='3H';
    #   ELSE CVAR12='NPL';
    #   FILE SC3HT DSD DLM=';';
    # TITLE1 'PUBLIC BANK BERHAD';

    npgs3H = pl.DataFrame()
    if not ln_df.is_empty():
        npgs3H = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '3H')
        if not npgs3H.is_empty():
            npgs3H = _apply_common_filters(npgs3H, cvar12_else_npl=True)

    _process_schedule(npgs3H, '3H', rdate, _write_text_dsd,
                      bank_name='PUBLIC BANK BERHAD',
                      cvar07_override='FL')
    print("  SCH=3H done")

    # =========================================================================
    # SCH=2H — LN only; CVAR07='TL'; DSD text output
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='TL'; IF CVAR02='2H';
    #   ELSE CVAR12='NPF';
    #   FILE SC2HT DSD DLM=';';
    # TITLE1 'PUBLIC ISLAMIC BANK BERHAD';

    npgs2H = pl.DataFrame()
    if not ln_df.is_empty():
        npgs2H = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '2H')
        if not npgs2H.is_empty():
            npgs2H = _apply_common_filters(npgs2H, cvar12_else_npf=True)

    _process_schedule(npgs2H, '2H', rdate, _write_text_dsd,
                      bank_name='PUBLIC ISLAMIC BANK BERHAD',
                      cvar07_override='TL')
    print("  SCH=2H done")

    # =========================================================================
    # SCH=4H — LN only; CVAR07='TL'; DSD text output
    # =========================================================================
    # DATA NPGS: SET NPGS.LNNPGS&REPTMON; CVAR07='TL'; IF CVAR02='4H';
    #   ELSE CVAR12='NPF';
    #   FILE SC4HT DSD DLM=';';
    # TITLE1 'PUBLIC ISLAMIC BANK BERHAD';

    npgs4H = pl.DataFrame()
    if not ln_df.is_empty():
        npgs4H = ln_df.filter(pl.col('cvar02').cast(pl.Utf8).str.strip_chars() == '4H')
        if not npgs4H.is_empty():
            npgs4H = _apply_common_filters(npgs4H, cvar12_else_npf=True)

    _process_schedule(npgs4H, '4H', rdate, _write_text_dsd,
                      bank_name='PUBLIC ISLAMIC BANK BERHAD',
                      cvar07_override='TL')
    print("  SCH=4H done")

    print("EIBRNPGS: Processing complete.")
    print(f"  All outputs written to: {OUTPUT_DIR}/")


if __name__ == '__main__':
    main()
