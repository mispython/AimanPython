# !/usr/bin/env python3
"""
Program Name: EIBMCOSR
Purpose: Generate Transaction Volume & Cost Analysis Report for Public Bank Berhad (PBB)
         - Processes cost rates, stock banking, deposit collections, ESMR, hardware/datacomm costs
         - Outputs CSV text files (COSTXT01, COSTXT02, COSTEXP) and formatted print report
         - Identifies missing account names and unknown transaction type exceptions
"""

import duckdb
import polars as pl
from datetime import date, datetime
import os

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

# Input parquet paths (COST library)
COST_REPTDATE_PARQUET  = "input/cost/reptdate.parquet"
COST_NAME_PARQUET      = "input/cost/name.parquet"
COST_ESMR_PARQUET      = "input/cost/esmr.parquet"
COST_OTHCOST_PARQUET   = "input/cost/othcost.parquet"

# Monthly data parquet prefix patterns (e.g. cost/rate01.parquet .. rate12.parquet)
COST_RATE_PREFIX       = "input/cost/rate"        # rate01..rate12
COST_STBK_PREFIX       = "input/cost/stbk"        # stbk01..stbk12
COST_DPCOL_PREFIX      = "input/cost/dpcol"       # dpcol01..dpcol12
COST_DPECP_PREFIX      = "input/cost/dpecp"       # dpecp01..dpecp12
COST_DPDDS_PREFIX      = "input/cost/dpdds"       # dpdds01..dpdds12
COST_DPMISC_PREFIX     = "input/cost/dpmisc"      # dpmisc01..dpmisc12
COST_DPBAL_PREFIX      = "input/cost/dpbal"       # dpbal01..dpbal12

# Output paths
COSTXT01_TXT  = "output/cost01_text.txt"           # SAP.PBB.COST01.TEXT
COSTXT02_TXT  = "output/cost02_text.txt"           # SAP.PBB.COST02.TEXT  (SASLIST / PRINT report)
COSTEXP_TXT   = "output/cost_except_text.txt"      # SAP.PBB.COST.EXCEPT.TEXT

os.makedirs("output", exist_ok=True)

# =============================================================================
# FORMAT / UTILITY HELPERS
# =============================================================================

def sas_date_to_pydate(val):
    """Convert SAS date integer (days since 1960-01-01) or string to Python date."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + __import__('datetime').timedelta(days=int(val))
    if isinstance(val, str):
        return datetime.strptime(val, '%Y-%m-%d').date()
    return val

def parse_ddmmyy8(s):
    """Parse DD/MM/YY string (DDMMYY8. format) to Python date (YEARCUTOFF=1950)."""
    if not s:
        return None
    d, m, y2 = int(s[0:2]), int(s[3:5]), int(s[6:8])
    year = (1900 + y2) if y2 >= 50 else (2000 + y2)
    return date(year, m, d)

def fmt_ddmmyy8(d):
    """Format Python date as DD/MM/YY."""
    if d is None:
        return ''
    return d.strftime('%d/%m/%y')

def fmt_comma(val, width, decimals=2):
    """Format numeric as COMMA<width>.<decimals>, right-justified."""
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * width
    try:
        formatted = f"{float(val):,.{decimals}f}"
    except (TypeError, ValueError):
        return ' ' * width
    return formatted.rjust(width)

def fmt_comma9(val):
    return fmt_comma(val, 9, 0)

def fmt_comma14_2(val):
    return fmt_comma(val, 14, 2)

def fmt_comma15_2(val):
    return fmt_comma(val, 15, 2)

def fmt_4_2(val):
    """Format as 4.2 (width 4, 2 decimals)."""
    if val is None:
        return '    '
    return f"{float(val):.2f}".rjust(4)

def place_at(line_list, col, text):
    """Place text into line_list at 1-indexed column."""
    col0 = col - 1
    for i, ch in enumerate(str(text)):
        pos = col0 + i
        while len(line_list) <= pos:
            line_list.append(' ')
        line_list[pos] = ch

def make_line(width=132):
    return [' '] * width

def finalize_line(line_list, width=132):
    return ''.join(line_list).ljust(width)[:width]

def coalesce(val, default=0.0):
    if val is None or (isinstance(val, float) and val != val):
        return default
    return val

# =============================================================================
# REPORT DATE VARIABLES
# =============================================================================

def get_report_vars(reptdate_parquet):
    """
    Read REPTDATE, compute all macro variables.
    Returns dict with: noofday, reptday, reptmon, reptyear, rdate, sdate, smon, emon
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate = sas_date_to_pydate(row[0])

    # SRPTDATE logic
    # *** SRPTDATE = MDY(MM1,1,YY1); -- commented in original
    # Active: SRPTDATE = MDY(MONTH(REPTDATE),1,YEAR(REPTDATE))
    srptdate = date(reptdate.year, reptdate.month, 1)

    noofday  = reptdate.day
    reptday  = str(reptdate.day).zfill(2)
    reptmon  = str(reptdate.month).zfill(2)
    reptyear = str(reptdate.year)
    rdate    = fmt_ddmmyy8(reptdate)
    sdate    = fmt_ddmmyy8(srptdate)
    smon     = srptdate.month
    emon     = reptdate.month

    return {
        'noofday':  noofday,
        'reptday':  reptday,
        'reptmon':  reptmon,
        'reptyear': reptyear,
        'rdate':    rdate,
        'sdate':    sdate,
        'smon':     smon,
        'emon':     emon,
        'reptdate': reptdate,
        'srptdate': srptdate,
    }

# =============================================================================
# MONTHLY DATA LOADER (replaces %MACRO loops with PROC APPEND)
# =============================================================================

def load_monthly_parquets(prefix: str, smon: int, emon: int) -> pl.DataFrame:
    """
    Load and union monthly parquet files from smon to emon.
    Equivalent to the %DO I=&SMON %TO &EMON macro loops with PROC APPEND.
    """
    frames = []
    con = duckdb.connect()
    for i in range(smon, emon + 1):
        month_str = str(i).zfill(2)
        path = f"{prefix}{month_str}.parquet"
        if os.path.exists(path):
            df = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
            frames.append(df)
    con.close()
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how='diagonal')

# =============================================================================
# %COSTRT — Load and sort RATE data
# =============================================================================

def load_rate(smon: int, emon: int) -> pl.DataFrame:
    """Load all rate monthly parquets, sort BY TRANDT."""
    rate = load_monthly_parquets(COST_RATE_PREFIX, smon, emon)
    if rate.is_empty():
        return rate
    return rate.sort('trandt')

# =============================================================================
# %PROCESS — Main transaction processing
# =============================================================================

def process_main(rv: dict, rate: pl.DataFrame) -> tuple:
    """
    Implements %PROCESS macro:
    - Load STBK, DPCOL, DPECP, DPDDS, DPMISC
    - Build TOTSUM, compute costs per SVTYPE
    - Merge with NAME
    - PROC MEANS by NMABBR
    Returns: totsum (pl.DataFrame), missname (pl.DataFrame), except_df (pl.DataFrame)
    """
    smon, emon = rv['smon'], rv['emon']
    noofday    = rv['noofday']
    reptyear   = int(rv['reptyear'])

    # *** 1. STOCK BANKING ***
    stbk  = load_monthly_parquets(COST_STBK_PREFIX,  smon, emon)
    # *** 2. DEPOSIT COLLECTION ***
    dpcol = load_monthly_parquets(COST_DPCOL_PREFIX, smon, emon)
    # *** 3. DEPOSIT ECP/FDS ***
    dpecp = load_monthly_parquets(COST_DPECP_PREFIX, smon, emon)
    # *** 4. DEPOSIT DDS ***
    dpdds = load_monthly_parquets(COST_DPDDS_PREFIX, smon, emon)
    # *** 5. DEPOSIT MISC ***
    dpmisc = load_monthly_parquets(COST_DPMISC_PREFIX, smon, emon)

    # DATA TOTSUM: SET STBK DPCOL DPECP DPDDS DPMISC;
    totsum = pl.concat([stbk, dpcol, dpecp, dpdds, dpmisc], how='diagonal')
    totsum = totsum.sort('trandt')

    # DATA EXCEPT: rows where SVTYPE not in the known list
    known_svtypes = {'FDSIBG','FDSIBK','ECP','DDS','EBK','TEL','ATM','OTC','ESI',
                     'PREATM','PRESMS','FPXCOL','FPXPYM','STM','PREEBK','MTN',
                     'MIS','PAG','CDT','CDM','MBK'}
    except_df = totsum.filter(~pl.col('svtype').is_in(list(known_svtypes)))

    # MERGE TOTSUM + RATE BY TRANDT (left join, keep IF A)
    rate_sel = rate.select([c for c in rate.columns])
    totsum = totsum.join(rate_sel, on='trandt', how='left', suffix='_rate')
    # Resolve rate columns that may overlap
    for col in rate_sel.columns:
        rate_col = f"{col}_rate"
        if rate_col in totsum.columns:
            totsum = totsum.with_columns(
                pl.when(pl.col(rate_col).is_not_null())
                  .then(pl.col(rate_col))
                  .otherwise(pl.col(col))
                  .alias(col)
            ).drop(rate_col)

    # Compute CNT/COS columns per SVTYPE using a row-wise approach
    cnt_cos_cols = [
        'cntfds','cosfds','cntecp','cosecp','cntdds','cosdds','cntebk','cosebk',
        'cnttel','costel','cntatm','cosatm','cntotc','cosotc','cntesi','cosesi',
        'cntmisc','cosmisc','cntfpxco','cosfpxco','cntfpxpy','cosfpxpy',
        'cntpag','cospag','cntcdt','coscdt','cntcdm','coscdm','cntmbk','cosmbk'
    ]
    # Initialize all cnt/cos columns to null
    for c in cnt_cos_cols:
        if c not in totsum.columns:
            totsum = totsum.with_columns(pl.lit(None).cast(pl.Float64).alias(c))

    # Use map_rows to apply the SELECT(SVTYPE) logic
    def apply_svtype(rows):
        results = []
        for row in rows:
            r = dict(zip(totsum.columns, row))
            svtype   = r.get('svtype', '')
            count    = coalesce(r.get('count', 0), 0.0)
            miscrate = coalesce(r.get('miscrate', 0.0))
            debrate  = coalesce(r.get('debrate',  0.0))
            pberate  = coalesce(r.get('pberate',  0.0))
            telerate = coalesce(r.get('telerate', 0.0))
            atmrate  = coalesce(r.get('atmrate',  0.0))
            otcrate  = coalesce(r.get('otcrate',  0.0))

            out = {c: None for c in cnt_cos_cols}
            if   svtype in ('FDSIBG','FDSIBK'):
                out['cntfds']   = count; out['cosfds']   = count * miscrate
            elif svtype == 'ECP':
                out['cntecp']   = count; out['cosecp']   = count * miscrate
            elif svtype == 'DDS':
                out['cntdds']   = count; out['cosdds']   = count * debrate
            elif svtype == 'EBK':
                out['cntebk']   = count; out['cosebk']   = count * pberate
            elif svtype == 'TEL':
                out['cnttel']   = count; out['costel']   = count * telerate
            elif svtype == 'ATM':
                out['cntatm']   = count; out['cosatm']   = count * atmrate
            elif svtype == 'OTC':
                out['cntotc']   = count; out['cosotc']   = count * otcrate
            elif svtype == 'ESI':
                out['cntesi']   = count; out['cosesi']   = count * miscrate
            elif svtype in ('PREATM','PRESMS','PREEBK','STM','MTN','MIS'):
                out['cntmisc']  = count; out['cosmisc']  = count * miscrate
            elif svtype == 'FPXCOL':
                out['cntfpxco'] = count; out['cosfpxco'] = count * miscrate
            elif svtype == 'FPXPYM':
                out['cntfpxpy'] = count; out['cosfpxpy'] = count * miscrate
            elif svtype == 'PAG':
                out['cntpag']   = count; out['cospag']   = count * miscrate
            elif svtype == 'CDT':
                out['cntcdt']   = count; out['coscdt']   = count * miscrate
            elif svtype == 'CDM':
                out['cntcdm']   = count; out['coscdm']   = count * miscrate
            elif svtype == 'MBK':
                out['cntmbk']   = count; out['cosmbk']   = count * miscrate

            results.append(tuple(out[c] for c in cnt_cos_cols))
        return results

    # Apply row-by-row (using with_columns with map_elements per column is complex;
    # use to_dicts approach for correctness)
    rows_data = totsum.to_dicts()
    cnt_cos_values = {c: [] for c in cnt_cos_cols}
    for row in rows_data:
        svtype   = row.get('svtype', '')
        count    = coalesce(row.get('count', 0), 0.0)
        miscrate = coalesce(row.get('miscrate', 0.0))
        debrate  = coalesce(row.get('debrate',  0.0))
        pberate  = coalesce(row.get('pberate',  0.0))
        telerate = coalesce(row.get('telerate', 0.0))
        atmrate  = coalesce(row.get('atmrate',  0.0))
        otcrate  = coalesce(row.get('otcrate',  0.0))

        out = {c: None for c in cnt_cos_cols}
        if   svtype in ('FDSIBG','FDSIBK'):
            out['cntfds']   = count; out['cosfds']   = count * miscrate
        elif svtype == 'ECP':
            out['cntecp']   = count; out['cosecp']   = count * miscrate
        elif svtype == 'DDS':
            out['cntdds']   = count; out['cosdds']   = count * debrate
        elif svtype == 'EBK':
            out['cntebk']   = count; out['cosebk']   = count * pberate
        elif svtype == 'TEL':
            out['cnttel']   = count; out['costel']   = count * telerate
        elif svtype == 'ATM':
            out['cntatm']   = count; out['cosatm']   = count * atmrate
        elif svtype == 'OTC':
            out['cntotc']   = count; out['cosotc']   = count * otcrate
        elif svtype == 'ESI':
            out['cntesi']   = count; out['cosesi']   = count * miscrate
        elif svtype in ('PREATM','PRESMS','PREEBK','STM','MTN','MIS'):
            out['cntmisc']  = count; out['cosmisc']  = count * miscrate
        elif svtype == 'FPXCOL':
            out['cntfpxco'] = count; out['cosfpxco'] = count * miscrate
        elif svtype == 'FPXPYM':
            out['cntfpxpy'] = count; out['cosfpxpy'] = count * miscrate
        elif svtype == 'PAG':
            out['cntpag']   = count; out['cospag']   = count * miscrate
        elif svtype == 'CDT':
            out['cntcdt']   = count; out['coscdt']   = count * miscrate
        elif svtype == 'CDM':
            out['cntcdm']   = count; out['coscdm']   = count * miscrate
        elif svtype == 'MBK':
            out['cntmbk']   = count; out['cosmbk']   = count * miscrate
        for c in cnt_cos_cols:
            cnt_cos_values[c].append(out[c])

    for c in cnt_cos_cols:
        totsum = totsum.with_columns(pl.Series(c, cnt_cos_values[c], dtype=pl.Float64))

    # TOTFEE = SUM(FEEAMT1, FEEAMT2)
    totsum = totsum.with_columns(
        (pl.col('feeamt1').fill_null(0.0) + pl.col('feeamt2').fill_null(0.0)).alias('totfee')
    )

    # PROC SORT DATA=COST.NAME OUT=NAME NODUPKEYS; BY ACCTNO;
    con = duckdb.connect()
    name_df = con.execute(
        f"SELECT * FROM read_parquet('{COST_NAME_PARQUET}') ORDER BY acctno"
    ).pl().unique(subset=['acctno'], keep='first').sort('acctno')
    con.close()

    # Sort TOTSUM BY ACCTNO
    totsum = totsum.sort('acctno')

    # *** A/C NOT FOUND IN CONTROL FILE (ESMR) ***
    missname = totsum.join(name_df, on='acctno', how='anti')

    # MERGE TOTSUM + NAME BY ACCTNO, keep IF A
    totsum = totsum.join(name_df, on='acctno', how='left', suffix='_name')
    for col in ['nmabbr', 'custname']:
        name_col = f"{col}_name"
        if name_col in totsum.columns:
            totsum = totsum.with_columns(
                pl.when(pl.col(name_col).is_not_null())
                  .then(pl.col(name_col))
                  .otherwise(pl.col(col) if col in totsum.columns else pl.lit(None))
                  .alias(col)
            ).drop(name_col)

    # PROC MEANS BY NMABBR SUM -> aggregate all cnt/cos + totfee columns
    agg_cols = cnt_cos_cols + ['totfee']
    totsum = totsum.sort('nmabbr').group_by('nmabbr', maintain_order=True).agg(
        [pl.col(c).sum().alias(c) for c in agg_cols if c in totsum.columns] +
        [pl.col('custname').first().alias('custname')]
    )

    return totsum, missname, except_df

# =============================================================================
# %ESMRDTL — ESMR detail processing
# =============================================================================

def process_esmr(rv: dict, rate: pl.DataFrame) -> tuple:
    """
    Implements %ESMRDTL macro.
    Returns: esmr2 (pl.DataFrame), missname_esmr (pl.DataFrame)
    """
    smon    = rv['smon']
    emon    = rv['emon']
    sdate_d = parse_ddmmyy8(rv['sdate'])

    con = duckdb.connect()
    esmr_raw = con.execute(f"SELECT * FROM read_parquet('{COST_ESMR_PARQUET}')").pl()
    con.close()

    # Convert expdt
    if 'expdt' in esmr_raw.columns:
        esmr_raw = esmr_raw.with_columns(
            pl.col('expdt').map_elements(
                lambda v: sas_date_to_pydate(v) if v is not None else None,
                return_dtype=pl.Date
            )
        )
    if 'trandt' in esmr_raw.columns:
        esmr_raw = esmr_raw.with_columns(
            pl.col('trandt').map_elements(
                lambda v: sas_date_to_pydate(v) if v is not None else None,
                return_dtype=pl.Date
            )
        )

    # IF EXPDT GE SDATE
    esmr_raw = esmr_raw.filter(pl.col('expdt') >= sdate_d)

    # MERGE ESMR + RATE BY TRANDT (left join)
    rate_conv = rate.with_columns(
        pl.col('trandt').map_elements(
            lambda v: sas_date_to_pydate(v) if v is not None else None,
            return_dtype=pl.Date
        )
    ) if rate.schema.get('trandt') != pl.Date else rate

    esmr_merged = esmr_raw.sort('trandt').join(rate_conv.sort('trandt'), on='trandt', how='left', suffix='_rate')
    for col in rate_conv.columns:
        rate_col = f"{col}_rate"
        if rate_col in esmr_merged.columns:
            esmr_merged = esmr_merged.with_columns(
                pl.when(pl.col(rate_col).is_not_null())
                  .then(pl.col(rate_col))
                  .otherwise(pl.col(col) if col in esmr_merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(rate_col)

    # THISDATE = SDATE; MTHCOUNT = months between EXPDT and THISDATE + 1
    # COSESMR = (MANDAY * MANRATE) / 60
    sdate_y, sdate_m = sdate_d.year, sdate_d.month

    def compute_mthcount(expdt):
        if expdt is None:
            return None
        return (expdt.year * 12 + expdt.month) - (sdate_y * 12 + sdate_m) + 1

    esmr_merged = esmr_merged.with_columns([
        pl.col('expdt').map_elements(compute_mthcount, return_dtype=pl.Int64).alias('mthcount'),
        ((pl.col('manday') * pl.col('manrate')) / 60.0).alias('cosesmr')
    ])

    # %DO I = &SMON %TO &EMON: filter MTHCOUNT GE I and append
    frames = []
    for i in range(smon, emon + 1):
        subset = esmr_merged.filter(pl.col('mthcount') >= i)
        frames.append(subset)
    esmr2 = pl.concat(frames, how='diagonal') if frames else pl.DataFrame()

    # PROC SORT COST.NAME NODUPKEYS BY ACCTNO
    con2 = duckdb.connect()
    name_df = con2.execute(
        f"SELECT * FROM read_parquet('{COST_NAME_PARQUET}') ORDER BY acctno"
    ).pl().unique(subset=['acctno'], keep='first').sort('acctno')
    con2.close()

    esmr2 = esmr2.sort('acctno')

    # *** A/C NOT FOUND IN CONTROL FILE (ESMR) ***
    missacc = esmr2.join(name_df, on='acctno', how='anti')

    # Keep only A AND B (inner join)
    esmr2 = esmr2.join(name_df, on='acctno', how='inner', suffix='_name')
    for col in ['nmabbr', 'custname']:
        name_col = f"{col}_name"
        if name_col in esmr2.columns:
            esmr2 = esmr2.with_columns(
                pl.when(pl.col(name_col).is_not_null())
                  .then(pl.col(name_col))
                  .otherwise(pl.col(col) if col in esmr2.columns else pl.lit(None))
                  .alias(col)
            ).drop(name_col)

    # PROC MEANS BY NMABBR SUM COSESMR
    if not esmr2.is_empty() and 'nmabbr' in esmr2.columns:
        esmr2 = esmr2.sort('nmabbr').group_by('nmabbr', maintain_order=True).agg(
            pl.col('cosesmr').sum()
        )

    return esmr2, missacc

# =============================================================================
# %OTHERDTL — Other cost processing (hardware + datacomm)
# =============================================================================

def process_othercost(rv: dict) -> tuple:
    """
    Implements %OTHERDTL macro.
    Returns: othcost2 (pl.DataFrame), missname_oth (pl.DataFrame)
    """
    smon    = rv['smon']
    emon    = rv['emon']
    sdate_d = parse_ddmmyy8(rv['sdate'])
    rdate_d = parse_ddmmyy8(rv['rdate'])

    con = duckdb.connect()
    othcost_raw = con.execute(f"SELECT * FROM read_parquet('{COST_OTHCOST_PARQUET}')").pl()
    con.close()

    # Convert dates
    for dcol in ['expdt', 'trandt']:
        if dcol in othcost_raw.columns:
            othcost_raw = othcost_raw.with_columns(
                pl.col(dcol).map_elements(
                    lambda v: sas_date_to_pydate(v) if v is not None else None,
                    return_dtype=pl.Date
                )
            )

    # Filter: CTYPE='DC' OR (CTYPE='HW' AND EXPDT GE SDATE AND TRANDT LE RDATE)
    othcost = othcost_raw.filter(
        (pl.col('ctype') == 'DC') |
        (
            (pl.col('ctype') == 'HW') &
            (pl.col('expdt')  >= sdate_d) &
            (pl.col('trandt') <= rdate_d)
        )
    )

    sdate_y, sdate_m = sdate_d.year, sdate_d.month

    def compute_mthcount(expdt):
        if expdt is None:
            return None
        return (expdt.year * 12 + expdt.month) - (sdate_y * 12 + sdate_m) + 1

    rows = othcost.to_dicts()
    mthcounts, hwares, datacomms = [], [], []
    for row in rows:
        ctype   = row.get('ctype','')
        oricost = coalesce(row.get('oricost', 0.0))
        expdt   = row.get('expdt')
        if ctype == 'HW':
            mc = compute_mthcount(expdt)
            mthcounts.append(mc)
            hwares.append(oricost / 60.0)
            datacomms.append(None)
        elif ctype == 'DC':
            mthcounts.append(None)
            hwares.append(None)
            datacomms.append(oricost / 12.0)
        else:
            mthcounts.append(None)
            hwares.append(None)
            datacomms.append(None)

    othcost = othcost.with_columns([
        pl.Series('mthcount', mthcounts, dtype=pl.Int64),
        pl.Series('hware',    hwares,    dtype=pl.Float64),
        pl.Series('datacomm', datacomms, dtype=pl.Float64),
    ])

    # %DO I = &SMON %TO &EMON: filter MTHCOUNT GE I OR CTYPE='DC'
    frames = []
    for i in range(smon, emon + 1):
        subset = othcost.filter(
            (pl.col('mthcount') >= i) | (pl.col('ctype') == 'DC')
        )
        frames.append(subset)
    othcost2 = pl.concat(frames, how='diagonal') if frames else pl.DataFrame()

    # NAME lookup
    con2 = duckdb.connect()
    name_df = con2.execute(
        f"SELECT * FROM read_parquet('{COST_NAME_PARQUET}') ORDER BY acctno"
    ).pl().unique(subset=['acctno'], keep='first').sort('acctno')
    con2.close()

    othcost2 = othcost2.sort('acctno')

    # *** A/C NOT FOUND IN CONTROL FILE (ESMR) ***
    missacc = othcost2.join(name_df, on='acctno', how='anti')

    # A AND B
    othcost2 = othcost2.join(name_df, on='acctno', how='inner', suffix='_name')
    for col in ['nmabbr', 'custname']:
        name_col = f"{col}_name"
        if name_col in othcost2.columns:
            othcost2 = othcost2.with_columns(
                pl.when(pl.col(name_col).is_not_null())
                  .then(pl.col(name_col))
                  .otherwise(pl.col(col) if col in othcost2.columns else pl.lit(None))
                  .alias(col)
            ).drop(name_col)

    # PROC MEANS BY NMABBR SUM HWARE DATACOMM
    if not othcost2.is_empty() and 'nmabbr' in othcost2.columns:
        agg_exprs = []
        for c in ['hware', 'datacomm']:
            if c in othcost2.columns:
                agg_exprs.append(pl.col(c).sum())
        if agg_exprs:
            othcost2 = othcost2.sort('nmabbr').group_by('nmabbr', maintain_order=True).agg(agg_exprs)

    return othcost2, missacc

# =============================================================================
# %DEPBAL — Float amount calculation
# =============================================================================

def process_depbal(rv: dict, rate: pl.DataFrame) -> tuple:
    """
    Implements %DEPBAL macro.
    Returns: dpbal (pl.DataFrame), missname_bal (pl.DataFrame)
    """
    smon     = rv['smon']
    emon     = rv['emon']
    noofday  = rv['noofday']
    reptyear = int(rv['reptyear'])

    dpbal = load_monthly_parquets(COST_DPBAL_PREFIX, smon, emon)
    if dpbal.is_empty():
        return pl.DataFrame(), pl.DataFrame()

    dpbal = dpbal.sort('trandt')

    # MERGE DPBAL + RATE (KEEP=TRANDT FLOAT) BY TRANDT
    rate_float = rate.select(['trandt', 'float']) if 'float' in rate.columns else rate.select(['trandt'])
    dpbal = dpbal.join(rate_float, on='trandt', how='left', suffix='_rate')
    if 'float_rate' in dpbal.columns:
        dpbal = dpbal.with_columns(
            pl.when(pl.col('float_rate').is_not_null())
              .then(pl.col('float_rate'))
              .otherwise(pl.col('float') if 'float' in dpbal.columns else pl.lit(None))
              .alias('float')
        ).drop('float_rate')

    # Leap year check
    dayyr = 366 if (reptyear % 4 == 0) else 365

    # FLOAMT calculation
    def calc_floamt(row):
        avgbal   = coalesce(row.get('avgbal', 0.0))
        float_r  = coalesce(row.get('float',  0.0))
        intrstpd = coalesce(row.get('intrstpd', 0.0))
        if avgbal > 0:
            return ((avgbal * (float_r / 100)) * (noofday / dayyr)) + (-1 * intrstpd)
        return 0.0

    rows   = dpbal.to_dicts()
    floams = [calc_floamt(r) for r in rows]
    dpbal  = dpbal.with_columns(pl.Series('floamt', floams, dtype=pl.Float64))

    # NAME lookup
    con = duckdb.connect()
    name_df = con.execute(
        f"SELECT * FROM read_parquet('{COST_NAME_PARQUET}') ORDER BY acctno"
    ).pl().unique(subset=['acctno'], keep='first').sort('acctno')
    con.close()

    dpbal = dpbal.sort('acctno')

    # *** A/C NOT FOUND IN CONTROL FILE (ESMR) ***
    missacc = dpbal.join(name_df, on='acctno', how='anti')

    # A AND B
    dpbal = dpbal.join(name_df, on='acctno', how='inner', suffix='_name')
    for col in ['nmabbr', 'custname']:
        name_col = f"{col}_name"
        if name_col in dpbal.columns:
            dpbal = dpbal.with_columns(
                pl.when(pl.col(name_col).is_not_null())
                  .then(pl.col(name_col))
                  .otherwise(pl.col(col) if col in dpbal.columns else pl.lit(None))
                  .alias(col)
            ).drop(name_col)

    # PROC MEANS BY NMABBR SUM FLOAMT
    if not dpbal.is_empty() and 'nmabbr' in dpbal.columns:
        dpbal = dpbal.sort('nmabbr').group_by('nmabbr', maintain_order=True).agg(
            pl.col('floamt').sum()
        )

    return dpbal, missacc

# =============================================================================
# GRAND TOTAL MERGE AND FINALIZATION
# =============================================================================

def build_grand_total(totsum: pl.DataFrame, esmr2: pl.DataFrame,
                      othcost2: pl.DataFrame, dpbal: pl.DataFrame,
                      rv: dict, rate: pl.DataFrame) -> pl.DataFrame:
    """
    Implements the GRAND TOTAL DATA step and final null-to-zero replacements.
    """
    rdate_d = parse_ddmmyy8(rv['rdate'])

    # PROC SORT NAME NODUPKEYS BY NMABBR
    con = duckdb.connect()
    name_df = con.execute(
        f"SELECT custname, nmabbr FROM read_parquet('{COST_NAME_PARQUET}') ORDER BY nmabbr"
    ).pl().unique(subset=['nmabbr'], keep='first').sort('nmabbr')
    con.close()

    # Prepare join frames (select only needed columns)
    esmr_join    = esmr2.select(['nmabbr','cosesmr'])    if not esmr2.is_empty()    and 'cosesmr'  in esmr2.columns    else pl.DataFrame(schema={'nmabbr': pl.Utf8})
    dpbal_join   = dpbal.select(['nmabbr','floamt'])     if not dpbal.is_empty()    and 'floamt'   in dpbal.columns    else pl.DataFrame(schema={'nmabbr': pl.Utf8})
    othcost_cols = ['nmabbr'] + [c for c in ['hware','datacomm'] if c in othcost2.columns]
    othcost_join = othcost2.select(othcost_cols)         if not othcost2.is_empty() else pl.DataFrame(schema={'nmabbr': pl.Utf8})

    # MERGE TOTSUM + ESMR2 + OTHCOST2 + DPBAL + NAME BY NMABBR (IF A OR B)
    merged = totsum.join(esmr_join,    on='nmabbr', how='outer', suffix='_esmr')
    merged = merged.join(othcost_join, on='nmabbr', how='left',  suffix='_oth')
    merged = merged.join(dpbal_join,   on='nmabbr', how='left',  suffix='_bal')
    merged = merged.join(name_df,      on='nmabbr', how='left',  suffix='_name')

    # Resolve cosesmr
    if 'cosesmr_esmr' in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col('cosesmr_esmr').is_not_null())
              .then(pl.col('cosesmr_esmr'))
              .otherwise(pl.col('cosesmr') if 'cosesmr' in merged.columns else pl.lit(None))
              .alias('cosesmr')
        ).drop('cosesmr_esmr')

    # Resolve custname
    if 'custname_name' in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col('custname_name').is_not_null())
              .then(pl.col('custname_name'))
              .otherwise(pl.col('custname') if 'custname' in merged.columns else pl.lit(None))
              .alias('custname')
        ).drop('custname_name')

    # Compute TODIRECT, TOCNT, TOCOST, PROFIT
    def safe_sum(*cols):
        return sum(coalesce(c) for c in cols)

    rows  = merged.to_dicts()
    todirect_l, tocnt_l, tocost_l, profit_l = [], [], [], []

    for row in rows:
        hware    = coalesce(row.get('hware'))
        datacomm = coalesce(row.get('datacomm'))
        cosesmr  = coalesce(row.get('cosesmr'))
        todirect = safe_sum(hware, datacomm, cosesmr)

        tocnt = safe_sum(
            row.get('cntotc'), row.get('cntatm'), row.get('cntebk'),
            row.get('cnttel'), row.get('cntdds'), row.get('cntecp'),
            row.get('cntfds'), row.get('cntesi'), row.get('cntfpxco'),
            row.get('cosfpxpy'), row.get('cntmisc'),
            row.get('cntpag'), row.get('cntcdt'), row.get('cntcdm'), row.get('cntmbk')
        )
        tocost = safe_sum(
            row.get('cosotc'), row.get('cosatm'), row.get('cosebk'),
            row.get('costel'), row.get('cosdds'), row.get('cosecp'),
            row.get('cosfds'), row.get('cosesi'), row.get('cosfpxco'),
            row.get('cosfpxpy'), row.get('cosmisc'),
            row.get('cospag'), row.get('coscdt'), row.get('coscdm'), row.get('cosmbk')
        )
        totfee  = coalesce(row.get('totfee'))
        floamt  = coalesce(row.get('floamt'))
        profit  = safe_sum(totfee, floamt) + (-1.0 * safe_sum(todirect, tocost))

        todirect_l.append(todirect)
        tocnt_l.append(tocnt)
        tocost_l.append(tocost)
        profit_l.append(profit)

    merged = merged.with_columns([
        pl.Series('todirect', todirect_l, dtype=pl.Float64),
        pl.Series('tocnt',    tocnt_l,    dtype=pl.Float64),
        pl.Series('tocost',   tocost_l,   dtype=pl.Float64),
        pl.Series('profit',   profit_l,   dtype=pl.Float64),
    ])

    # TRANDT = RDATE
    rdate_int = (rdate_d - date(1960, 1, 1)).days
    merged = merged.with_columns(pl.lit(rdate_d).alias('trandt'))

    # MERGE TOTSUM + RATE BY TRANDT (left join)
    rate_conv = rate
    if rate_conv.schema.get('trandt') != pl.Date:
        rate_conv = rate_conv.with_columns(
            pl.col('trandt').map_elements(
                lambda v: sas_date_to_pydate(v) if v is not None else None,
                return_dtype=pl.Date
            )
        )
    merged = merged.join(rate_conv, on='trandt', how='left', suffix='_rate')
    for col in rate_conv.columns:
        rate_col = f"{col}_rate"
        if rate_col in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(rate_col).is_not_null())
                  .then(pl.col(rate_col))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(rate_col)

    # Replace nulls with 0 for all numeric report columns
    zero_cols = [
        'cntotc','cntatm','cntebk','cnttel','cntdds','cntecp','cntfds','cntesi',
        'cntpag','cntcdt','cntcdm','cntmbk','cntfpxco','cntfpxpy','cntmisc',
        'cosotc','cosatm','cosebk','costel','cosdds','cosecp','cosfds','cosesi',
        'cospag','coscdt','coscdm','cosmbk','cosfpxco','cosfpxpy','cosmisc',
        'totfee','todirect','tocnt','tocost','profit','floamt','hware','datacomm','cosesmr'
    ]
    for c in zero_cols:
        if c in merged.columns:
            merged = merged.with_columns(pl.col(c).fill_null(0.0))

    return merged.sort('custname')

# =============================================================================
# OUTPUT: COSTXT01 — CSV text file
# =============================================================================

def write_costxt01(totsum: pl.DataFrame, output_path: str):
    """
    DATA _NULL_: SET TOTSUM; FILE COSTXT01;
    Writes semicolon-delimited file with header and data rows.
    """
    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        # Header row 1
        f.write(';'
                ';SETUP/DIRECT COST'
                ';' ';' ';'
                ';TOTAL OPERATING'
                ';INCOME(RM)'
                ';' ';'
                ';PROFIT/'
                ';\n')
        # Header row 2
        f.write('CORPORATE NAME'
                ';TOTAL TXN VOL'
                ';H/W & SYS /SW'
                ';DATA COMM'
                ';PROGRAMMING'
                ';TOTAL COST'
                ';COST(RM)'
                ';TOTAL'
                ';FLOAT'
                ';LOSS(RM)'
                ';\n')
        # Data rows
        for row in totsum.iter_rows(named=True):
            custname = str(row.get('custname','') or '')
            tocnt    = str(int(coalesce(row.get('tocnt'), 0)))
            hware    = f"{coalesce(row.get('hware')):15.2f}"
            datacomm = f"{coalesce(row.get('datacomm')):15.2f}"
            cosesmr  = f"{coalesce(row.get('cosesmr')):15.2f}"
            todirect = f"{coalesce(row.get('todirect')):15.2f}"
            tocost   = f"{coalesce(row.get('tocost')):15.2f}"
            totfee   = f"{coalesce(row.get('totfee')):15.2f}"
            floamt   = f"{coalesce(row.get('floamt')):15.2f}"
            profit   = f"{coalesce(row.get('profit')):15.2f}"
            f.write(f"{custname};{tocnt};{hware};{datacomm};{cosesmr};"
                    f"{todirect};{tocost};{totfee};{floamt};{profit};\n")

# =============================================================================
# OUTPUT: PRINT REPORT (COSTXT02 / SASLIST) — %PRNRPT1
# =============================================================================

PAGE_WIDTH  = 132
LINECNT_MAX = 56
LINECNT_LAST_MAX = 50
LINECNT_INIT = 9

def write_print_report(totsum: pl.DataFrame, rv: dict, output_path: str):
    """
    DATA _NULL_: SET TOTSUM END=LAST; FILE PRINT; %PRNRPT1;
    Writes fixed-width formatted report with ASA carriage control (via PUT _PAGE_).
    """
    rows   = totsum.to_dicts()
    n      = len(rows)
    sdate  = rv['sdate']
    rdate  = rv['rdate']

    pagecnt = 0
    linecnt = 0

    # Running totals
    totals = {
        'tcntotc':0,'tcntatm':0,'tcntebk':0,'tcnttel':0,'tcntdds':0,
        'tcntecp':0,'tcntfds':0,'tcntesi':0,'tcntfpxco':0,'tcntfpxpy':0,
        'tcntpag':0,'tcntcdt':0,'tcntcdm':0,'tcntmisc':0,'ttocnt':0,'tcntmbk':0,
        'tcosotc':0.0,'tcosatm':0.0,'tcosebk':0.0,'tcostel':0.0,'tcosdds':0.0,
        'tcosecp':0.0,'tcosfds':0.0,'tcosesi':0.0,'tcosfpxco':0.0,'tcosfpxpy':0.0,
        'tcospag':0.0,'tcoscdt':0.0,'tcoscdm':0.0,'tcosmisc':0.0,'ttocost':0.0,'tcosmbk':0.0,
    }

    def newpage(f, rate_row):
        nonlocal pagecnt, linecnt
        # PUT _PAGE_ — ASA carriage control '1' for new page
        f.write('1' + '\n')
        pagecnt += 1
        linecnt  = LINECNT_INIT

        otcrate  = coalesce(rate_row.get('otcrate'))  if rate_row else 0.0
        atmrate  = coalesce(rate_row.get('atmrate'))  if rate_row else 0.0
        pberate  = coalesce(rate_row.get('pberate'))  if rate_row else 0.0
        telerate = coalesce(rate_row.get('telerate')) if rate_row else 0.0
        debrate  = coalesce(rate_row.get('debrate'))  if rate_row else 0.0
        miscrate = coalesce(rate_row.get('miscrate')) if rate_row else 0.0

        line = make_line(PAGE_WIDTH)
        place_at(line, 1,   f'PUBLIC BANK BERHAD - TRANSACTION VOLUMN & COST ANALYSIS '
                            f'REPORT FROM {sdate} TO {rdate}')
        place_at(line, 120, f'PAGE NO.: {pagecnt}')
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        line = make_line(PAGE_WIDTH)
        place_at(line, 34,  f'OTC ({fmt_4_2(otcrate)})')
        place_at(line, 59,  f'ATM-GIRO ({fmt_4_2(atmrate)})')
        place_at(line, 84,  f'PB EBANK ({fmt_4_2(pberate)})')
        place_at(line, 109, f'TELEBANKING ({fmt_4_2(telerate)})')
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        line = make_line(PAGE_WIDTH)
        place_at(line, 34,  f'DIRECT DEBIT ({fmt_4_2(debrate)})')
        place_at(line, 59,  f'ECP ({fmt_4_2(miscrate)})')
        place_at(line, 84,  f'FDS ({fmt_4_2(miscrate)})')
        place_at(line, 109, f'ESI ({fmt_4_2(miscrate)})')
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        line = make_line(PAGE_WIDTH)
        place_at(line, 34,  f'PAG ({fmt_4_2(miscrate)})')
        place_at(line, 59,  f'CDT ({fmt_4_2(miscrate)})')
        place_at(line, 84,  f'CDM ({fmt_4_2(miscrate)})')
        place_at(line, 109, f'FPX COLLECTION ({fmt_4_2(miscrate)})')
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        line = make_line(PAGE_WIDTH)
        place_at(line, 34,  f'FPX PAYMENT ({fmt_4_2(miscrate)})')
        place_at(line, 59,  f'MISC ({fmt_4_2(miscrate)})')
        place_at(line, 84,  f'MBK ({fmt_4_2(miscrate)})')
        place_at(line, 109, 'TOTAL ')
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # Separator
        line = make_line(PAGE_WIDTH)
        place_at(line, 1,   '-' * 45)
        place_at(line, 46,  '-' * 45)
        place_at(line, 91,  '-' * 40)
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # Column labels
        line = make_line(PAGE_WIDTH)
        place_at(line, 1,   'CORPORATE NAME')
        place_at(line, 34,  'TRX VOL')
        place_at(line, 52,  'COST')
        place_at(line, 59,  'TRX VOL')
        place_at(line, 77,  'COST')
        place_at(line, 84,  'TRX VOL')
        place_at(line, 102, 'COST')
        place_at(line, 109, 'TRX VOL')
        place_at(line, 127, 'COST')
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # Separator
        line = make_line(PAGE_WIDTH)
        place_at(line, 1,   '-' * 45)
        place_at(line, 46,  '-' * 45)
        place_at(line, 91,  '-' * 40)
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

    def write_data_block(f, row):
        nonlocal linecnt

        # PUT block: 5 lines per customer (4 data lines + 1 blank)
        # Line 1: CUSTNAME + OTC + ATM + EBK + TEL
        line = make_line(PAGE_WIDTH)
        place_at(line, 1,   str(row.get('custname') or '')[:30].ljust(30))
        place_at(line, 32,  fmt_comma9(row.get('cntotc')))
        place_at(line, 42,  fmt_comma14_2(row.get('cosotc')))
        place_at(line, 57,  fmt_comma9(row.get('cntatm')))
        place_at(line, 67,  fmt_comma14_2(row.get('cosatm')))
        place_at(line, 82,  fmt_comma9(row.get('cntebk')))
        place_at(line, 92,  fmt_comma14_2(row.get('cosebk')))
        place_at(line, 107, fmt_comma9(row.get('cnttel')))
        place_at(line, 117, fmt_comma14_2(row.get('costel')))
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # Line 2: DDS + ECP + FDS + ESI
        line = make_line(PAGE_WIDTH)
        place_at(line, 32,  fmt_comma9(row.get('cntdds')))
        place_at(line, 42,  fmt_comma14_2(row.get('cosdds')))
        place_at(line, 57,  fmt_comma9(row.get('cntecp')))
        place_at(line, 67,  fmt_comma14_2(row.get('cosecp')))
        place_at(line, 82,  fmt_comma9(row.get('cntfds')))
        place_at(line, 92,  fmt_comma14_2(row.get('cosfds')))
        place_at(line, 107, fmt_comma9(row.get('cntesi')))
        place_at(line, 117, fmt_comma14_2(row.get('cosesi')))
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # Line 3: PAG + CDT + CDM + FPXCOL
        line = make_line(PAGE_WIDTH)
        place_at(line, 32,  fmt_comma9(row.get('cntpag')))
        place_at(line, 42,  fmt_comma14_2(row.get('cospag')))
        place_at(line, 57,  fmt_comma9(row.get('cntcdt')))
        place_at(line, 67,  fmt_comma14_2(row.get('coscdt')))
        place_at(line, 82,  fmt_comma9(row.get('cntcdm')))
        place_at(line, 92,  fmt_comma14_2(row.get('coscdm')))
        place_at(line, 107, fmt_comma9(row.get('cntfpxco')))
        place_at(line, 117, fmt_comma14_2(row.get('cosfpxco')))
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # Line 4: FPXPYM + MISC + MBK + TOTAL
        line = make_line(PAGE_WIDTH)
        place_at(line, 32,  fmt_comma9(row.get('cntfpxpy')))
        place_at(line, 42,  fmt_comma14_2(row.get('cosfpxpy')))
        place_at(line, 57,  fmt_comma9(row.get('cntmisc')))
        place_at(line, 67,  fmt_comma14_2(row.get('cosmisc')))
        place_at(line, 82,  fmt_comma9(row.get('cntmbk')))
        place_at(line, 92,  fmt_comma14_2(row.get('cosmbk')))
        place_at(line, 107, fmt_comma9(row.get('tocnt')))
        place_at(line, 117, fmt_comma14_2(row.get('tocost')))
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # Line 5: blank line (/@001 in SAS)
        f.write(' ' + '\n')

        linecnt += 5

    def write_grand_total_block(f, t):
        # Separator
        line = make_line(PAGE_WIDTH)
        place_at(line, 1,   '-' * 45)
        place_at(line, 46,  '-' * 45)
        place_at(line, 91,  '-' * 40)
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # GRAND TOTAL line 1
        line = make_line(PAGE_WIDTH)
        place_at(line, 1,   'GRAND TOTAL')
        place_at(line, 32,  fmt_comma9(t['tcntotc']))
        place_at(line, 42,  fmt_comma14_2(t['tcosotc']))
        place_at(line, 57,  fmt_comma9(t['tcntatm']))
        place_at(line, 67,  fmt_comma14_2(t['tcosatm']))
        place_at(line, 82,  fmt_comma9(t['tcntebk']))
        place_at(line, 92,  fmt_comma14_2(t['tcosebk']))
        place_at(line, 107, fmt_comma9(t['tcnttel']))
        place_at(line, 117, fmt_comma14_2(t['tcostel']))
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # Line 2
        line = make_line(PAGE_WIDTH)
        place_at(line, 32,  fmt_comma9(t['tcntdds']))
        place_at(line, 42,  fmt_comma14_2(t['tcosdds']))
        place_at(line, 57,  fmt_comma9(t['tcntecp']))
        place_at(line, 67,  fmt_comma14_2(t['tcosecp']))
        place_at(line, 82,  fmt_comma9(t['tcntfds']))
        place_at(line, 92,  fmt_comma14_2(t['tcosfds']))
        place_at(line, 107, fmt_comma9(t['tcntesi']))
        place_at(line, 117, fmt_comma14_2(t['tcosesi']))
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # Line 3
        line = make_line(PAGE_WIDTH)
        place_at(line, 32,  fmt_comma9(t['tcntpag']))
        place_at(line, 42,  fmt_comma14_2(t['tcospag']))
        place_at(line, 57,  fmt_comma9(t['tcntcdt']))
        place_at(line, 67,  fmt_comma14_2(t['tcoscdt']))
        place_at(line, 82,  fmt_comma9(t['tcntcdm']))
        place_at(line, 92,  fmt_comma14_2(t['tcoscdm']))
        place_at(line, 107, fmt_comma9(t['tcntfpxco']))
        place_at(line, 117, fmt_comma14_2(t['tcosfpxco']))
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # Line 4
        line = make_line(PAGE_WIDTH)
        place_at(line, 32,  fmt_comma9(t['tcntfpxpy']))
        place_at(line, 42,  fmt_comma14_2(t['tcosfpxpy']))
        place_at(line, 57,  fmt_comma9(t['tcntmisc']))
        place_at(line, 67,  fmt_comma14_2(t['tcosmisc']))
        place_at(line, 82,  fmt_comma9(t['tcntmbk']))
        place_at(line, 92,  fmt_comma14_2(t['tcosmbk']))
        place_at(line, 107, fmt_comma9(t['ttocnt']))
        place_at(line, 117, fmt_comma14_2(t['ttocost']))
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

        # Closing separator
        line = make_line(PAGE_WIDTH)
        place_at(line, 1,   '-' * 45)
        place_at(line, 46,  '-' * 45)
        place_at(line, 91,  '-' * 40)
        f.write(' ' + finalize_line(line, PAGE_WIDTH) + '\n')

    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        rate_row = rows[0] if rows else {}

        for idx, row in enumerate(rows):
            is_last = (idx == n - 1)

            if idx == 0:
                newpage(f, rate_row)

            write_data_block(f, row)

            # Accumulate totals
            totals['tcntotc']   += int(coalesce(row.get('cntotc'),   0))
            totals['tcntatm']   += int(coalesce(row.get('cntatm'),   0))
            totals['tcntebk']   += int(coalesce(row.get('cntebk'),   0))
            totals['tcnttel']   += int(coalesce(row.get('cnttel'),   0))
            totals['tcntdds']   += int(coalesce(row.get('cntdds'),   0))
            totals['tcntecp']   += int(coalesce(row.get('cntecp'),   0))
            totals['tcntfds']   += int(coalesce(row.get('cntfds'),   0))
            totals['tcntesi']   += int(coalesce(row.get('cntesi'),   0))
            totals['tcntpag']   += int(coalesce(row.get('cntpag'),   0))
            totals['tcntcdt']   += int(coalesce(row.get('cntcdt'),   0))
            totals['tcntcdm']   += int(coalesce(row.get('cntcdm'),   0))
            totals['tcntmbk']   += int(coalesce(row.get('cntmbk'),   0))
            totals['tcntfpxco'] += int(coalesce(row.get('cntfpxco'), 0))
            totals['tcntfpxpy'] += int(coalesce(row.get('cntfpxpy'), 0))
            totals['tcntmisc']  += int(coalesce(row.get('cntmisc'),  0))
            totals['ttocnt']    += int(coalesce(row.get('tocnt'),    0))

            totals['tcosotc']   += coalesce(row.get('cosotc'))
            totals['tcosatm']   += coalesce(row.get('cosatm'))
            totals['tcosebk']   += coalesce(row.get('cosebk'))
            totals['tcostel']   += coalesce(row.get('costel'))
            totals['tcosdds']   += coalesce(row.get('cosdds'))
            totals['tcosecp']   += coalesce(row.get('cosecp'))
            totals['tcosfds']   += coalesce(row.get('cosfds'))
            totals['tcosesi']   += coalesce(row.get('cosesi'))
            totals['tcospag']   += coalesce(row.get('cospag'))
            totals['tcoscdt']   += coalesce(row.get('coscdt'))
            totals['tcoscdm']   += coalesce(row.get('coscdm'))
            totals['tcosmbk']   += coalesce(row.get('cosmbk'))
            totals['tcosfpxco'] += coalesce(row.get('cosfpxco'))
            totals['tcosfpxpy'] += coalesce(row.get('cosfpxpy'))
            totals['tcosmisc']  += coalesce(row.get('cosmisc'))
            totals['ttocost']   += coalesce(row.get('tocost'))

            if linecnt > LINECNT_MAX:
                newpage(f, rate_row)

            if is_last:
                if linecnt > LINECNT_LAST_MAX:
                    newpage(f, rate_row)
                write_grand_total_block(f, totals)

# =============================================================================
# OUTPUT: COSTEXP — Exception report (PROC PRINT equivalent)
# =============================================================================

def write_costexp(except_df: pl.DataFrame, rv: dict, output_path: str):
    """
    PROC PRINT DATA=EXCEPT equivalent.
    Outputs exception records with VAR ACCTNO SVTYPE AMOUNT FEEAMT1 FEEAMT2 TRANDT.
    Includes ASA carriage control and titles.
    """
    sdate = rv['sdate']
    rdate = rv['rdate']

    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        # Title lines
        f.write('1' + '\n')
        f.write(' ' + 'PUBLIC  BANK  BERHAD' + '\n')
        f.write(' ' + f'EXCEPTION REPORTS ON UNKNOW TRANSACTION TYPE FROM {sdate} TO {rdate}' + '\n')
        f.write(' ' + '\n')

        # Column header
        hdr = (f"{'OBS':<6} {'ACCTNO':<12} {'SVTYPE':<10} {'AMOUNT':>15} "
               f"{'FEEAMT1':>15} {'FEEAMT2':>15} {'TRANDT':<12}")
        f.write(' ' + hdr + '\n')
        f.write(' ' + '-' * len(hdr) + '\n')

        for obs, row in enumerate(except_df.iter_rows(named=True), start=1):
            acctno  = str(row.get('acctno','')  or '').strip()
            svtype  = str(row.get('svtype','')  or '').strip()
            amount  = f"{coalesce(row.get('amount')):15.2f}"
            feeamt1 = f"{coalesce(row.get('feeamt1')):15.2f}"
            feeamt2 = f"{coalesce(row.get('feeamt2')):15.2f}"
            trandt  = row.get('trandt')
            if trandt is not None:
                td = sas_date_to_pydate(trandt)
                trandt_str = td.strftime('%d%b%Y').upper() if td else str(trandt)
            else:
                trandt_str = ''
            line = (f"{obs:<6} {acctno:<12} {svtype:<10} {amount} "
                    f"{feeamt1} {feeamt2} {trandt_str:<12}")
            f.write(' ' + line + '\n')

# =============================================================================
# OUTPUT: MISSING ACCOUNT NAMES (PROC PRINT equivalent)
# =============================================================================

def write_missname_report(missname: pl.DataFrame, output_path: str):
    """
    PROC PRINT DATA=MISSNAME VAR ACCTNO equivalent.
    Appended to COSTXT02 report file (SASLIST output).
    Titles: 'PUBLIC BANK BERHAD' / 'MISSSING ACCOUNT NAME FROM PARAMETER (ESMR)'
    """
    # Deduplicate by ACCTNO
    if not missname.is_empty() and 'acctno' in missname.columns:
        missname = missname.unique(subset=['acctno']).sort('acctno')

    with open(output_path, 'a', encoding='ascii', errors='replace') as f:
        f.write('1' + '\n')
        f.write(' ' + 'PUBLIC  BANK  BERHAD' + '\n')
        f.write(' ' + 'MISSSING ACCOUNT NAME FROM PARAMETER (ESMR)' + '\n')
        f.write(' ' + '\n')
        f.write(' ' + f"{'OBS':<6} {'ACCTNO':<15}" + '\n')
        f.write(' ' + '-' * 22 + '\n')
        if not missname.is_empty():
            for obs, row in enumerate(missname.iter_rows(named=True), start=1):
                acctno = str(row.get('acctno','') or '').strip()
                f.write(' ' + f"{obs:<6} {acctno:<15}" + '\n')

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("EIBMCOSR: Starting cost analysis processing...")

    # Read report date variables
    rv = get_report_vars(COST_REPTDATE_PARQUET)
    print(f"  Report date: {rv['rdate']}, Period: {rv['sdate']} to {rv['rdate']}, "
          f"Months: {rv['smon']} to {rv['emon']}")

    # *** COST RATE ***
    rate = load_rate(rv['smon'], rv['emon'])
    print(f"  Loaded rate data: {len(rate)} rows")

    # *** MAIN PROCESS ***
    totsum, missname, except_df = process_main(rv, rate)
    print(f"  TOTSUM (after PROCESS): {len(totsum)} rows")

    # *** ESMR ***
    esmr2, missname_esmr = process_esmr(rv, rate)
    print(f"  ESMR2: {len(esmr2)} rows")

    # DATA MISSNAME: SET MISSACC MISSNAME (append)
    if not missname_esmr.is_empty():
        missname = pl.concat([missname_esmr, missname], how='diagonal')

    # *** OTHER COST ***
    othcost2, missname_oth = process_othercost(rv)
    print(f"  OTHCOST2: {len(othcost2)} rows")

    if not missname_oth.is_empty():
        missname = pl.concat([missname_oth, missname], how='diagonal')

    # *** TO CALCULATE FLOAT AMT ***
    dpbal, missname_bal = process_depbal(rv, rate)
    print(f"  DPBAL: {len(dpbal)} rows")

    if not missname_bal.is_empty():
        missname = pl.concat([missname_bal, missname], how='diagonal')

    # *** GRAND TOTAL ***
    totsum_final = build_grand_total(totsum, esmr2, othcost2, dpbal, rv, rate)
    print(f"  TOTSUM final: {len(totsum_final)} rows")

    # *** OUTPUT TEXT FILE (COSTXT01) ***
    write_costxt01(totsum_final, COSTXT01_TXT)
    print(f"  Written: {COSTXT01_TXT}")

    # *** REPORT FORMAT (SASLIST / COSTXT02) ***
    write_print_report(totsum_final, rv, COSTXT02_TXT)
    print(f"  Written report: {COSTXT02_TXT}")

    # *** MISSING OF A/C NUMBER ***
    # PROC SORT DATA=MISSNAME NODUPKEY; BY ACCTNO;
    # PROC PRINT DATA=MISSNAME VAR ACCTNO (appended to SASLIST output)
    write_missname_report(missname, COSTXT02_TXT)
    print(f"  Appended missing names to: {COSTXT02_TXT}")

    # *** EXCEPTION OF TRANSACTION TYPE ***
    # PROC PRINTTO PRINT=COSTEXP NEW; PROC PRINT DATA=EXCEPT
    write_costexp(except_df, rv, COSTEXP_TXT)
    print(f"  Written exception report: {COSTEXP_TXT}")

    print("EIBMCOSR: Processing complete.")


if __name__ == '__main__':
    main()
