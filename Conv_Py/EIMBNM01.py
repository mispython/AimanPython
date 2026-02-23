# !/usr/bin/env python3
"""
Program Name: EIMBNM01
Purpose: Public Bank Berhad - Monthly Loan Summary Reports (M&I)
         Generates multiple PROC PRINT reports covering:
         - All Loans (disbursement, repayment, outstanding) incl. Factoring
         - Retail Loans breakdown (Personal, Staff, OD, Corp, Factoring)
         - Commercial Retail Loans (individual vs non-individual)
         - SME Loans (DBE, FBE, DNBFI sub-categories)
         - Bank Trade (Bills) summary
         - Sector/sub-sector breakdown tabulations for:
             Factoring, M&I Commercial Retail, Retail Bills, Combined
         - Total Commercial Retail by product type

ESMR: 06-1485
ESMR: 2009-0744
ESMR: 2013-813 (JKA)
ESMR: 2015-606 (TBC)
ESMR: 2016-678 (NSA)
"""

import os
import calendar
from datetime import date, timedelta
from typing import Optional

import duckdb
import polars as pl

# %INC PGM(PBBLNFMT)
from PBBLNFMT import format_lnprod, format_lndenom  # (placeholder)

# %INC PGM(RDL2PBIF) — inline via imported function
from RDL2PBIF import build_pbif, format_fisstype, format_fissgroup

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

BNM_LOAN_PREFIX       = "input/bnm/loan"          # loan<MM><WK>.parquet
BNM_LNWOF_PREFIX      = "input/bnm/lnwof"         # lnwof<MM><WK>.parquet
BNM_LNWOD_PREFIX      = "input/bnm/lnwod"         # lnwod<MM><WK>.parquet
SASD_LOAN_PREFIX      = "input/sasd/loan"          # loan<MM>.parquet
DISPAY_PREFIX         = "input/dispay/dispaymth"   # dispaymth<MM>.parquet
LOAN_LNCOMM_PARQUET   = "input/loan/lncomm.parquet"
FEE_LNFEE_PREFIX      = "input/fee/lnfee"          # lnfee<MM><WK>.parquet
BTBNM_BTRAD_PREFIX    = "input/btbnm/btrad"        # btrad<MM><WK>.parquet

OUTPUT_DIR            = "output/eimbnm01"
REPORT_TXT            = os.path.join(OUTPUT_DIR, "eimbnm01_report.txt")
MFRS_MAST_BR_PARQUET  = "output/mfrs/mast_br.parquet"
MFRS_ALM_CR_PARQUET   = "output/mfrs/alm_cr.parquet"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs("output/mfrs", exist_ok=True)

# =============================================================================
# PRODUCT / CUSTOMER CODE MACRO CONSTANTS
# =============================================================================

ODCORP = {50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,31}

ODRTLA = {68,69,85,86,87,88,89,90,91,100,101,102,103,106,108,109,
          110,111,112,113,114,115,116,117,118,119,120,121,122,123,
          124,125,135,137,138,150,151,152,153,154,155,156,157,158,
          159,170,174,175,176,179,180,181,189,191,192,193,194,195,
          196,197,198,190,30,34,81,82,83,84,77,78}

ODRTLB = {177,178,34,133,134,77,78}

ODFISS = {'0311','0312','0313','0314','0315','0316'}

OTRTLA = {303,306,307,325,330,340,354,355,391,610,611,308,311,367,313,369}

OTRTLB = {4,5,6,7,15,20,25,26,27,28,29,30,31,32,33,34,
          60,61,62,63,70,71,72,73,74,75,76,77,78,79}

FLCORP = {180,181,182,183,193,800,801,802,803,804,818,
          900,901,902,903,904,905,906,907,908,912,922,
          184,909,910,914,915,916,918,919,920,925,950,951,
          631,632,633,634,635,636,637,639,640,641,816,817,
          805,806,807,808,809,810,811,812,813,814,913,917}

HLCORP = {638,911}

DBE_CUSTCDS  = {'41','42','43','44','46','47','48','49','51','52','53','54'}
FBE_CUSTCDS  = {'87','88','89'}
DNBFI_VALS   = {'1','2','3'}
SME_CUSTCDS  = DBE_CUSTCDS | FBE_CUSTCDS
INDIV_CUSTCDS = {'77','78','95','96'}

# =============================================================================
# ASA CARRIAGE CONTROL / REPORT WRITER
# =============================================================================

PAGE_LENGTH = 60

class ReportWriter:
    """Accumulates ASA carriage-control report lines; flushes to file."""

    def __init__(self):
        self.lines    = []
        self.line_cnt = PAGE_LENGTH + 1

    def _page_eject(self):
        self.line_cnt = 0

    def write_titles(self, title1: str, title2: str = '', title3: str = ''):
        self._page_eject()
        self.lines.append('1' + title1)
        if title2:
            self.lines.append(' ' + title2)
        if title3:
            self.lines.append(' ' + title3)
        self.lines.append(' ')
        self.line_cnt += (3 if title3 else 2) + 1

    def write_line(self, text: str = '', asa: str = ' '):
        self.lines.append(asa + text)
        self.line_cnt += 1

    def blank(self):
        self.write_line()

    def flush(self, filepath: str):
        with open(filepath, 'w', encoding='utf-8') as fh:
            for ln in self.lines:
                fh.write(ln + '\n')


# =============================================================================
# DATE / UTILITY HELPERS
# =============================================================================

def sas_date_to_pydate(val) -> Optional[date]:
    if val is None or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, date):
        return val
    return None


def pydate_to_sasdate(d: date) -> int:
    return (d - date(1960, 1, 1)).days


def coalesce_f(val, default: float = 0.0) -> float:
    if val is None or (isinstance(val, float) and val != val):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def coalesce_s(val, default: str = '') -> str:
    return str(val).strip() if val is not None else default


def load_parquet(path: str) -> pl.DataFrame:
    if not os.path.exists(path):
        return pl.DataFrame()
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    return df


def load_parquet_where(path: str, where: str) -> pl.DataFrame:
    if not os.path.exists(path):
        return pl.DataFrame()
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT * FROM read_parquet('{path}') WHERE {where}"
    ).pl()
    con.close()
    return df

# =============================================================================
# REPORT DATE VARIABLES
# =============================================================================

def get_report_vars() -> dict:
    """
    DATA REPTDATE:
      REPTDATE = INPUT('01'||PUT(MONTH(TODAY()),Z2.)||PUT(YEAR(TODAY()),4.),DDMMYY8.) - 1
    Derives all macro variables.
    """
    today    = date.today()
    reptdate = date(today.year, today.month, 1) - timedelta(days=1)

    day = reptdate.day
    if day == 8:
        sdd = 1;  wk = '1'; wk1 = '4'; wk2 = '';  wk3 = ''
    elif day == 15:
        sdd = 9;  wk = '2'; wk1 = '1'; wk2 = '';  wk3 = ''
    elif day == 22:
        sdd = 16; wk = '3'; wk1 = '2'; wk2 = '';  wk3 = ''
    else:
        sdd = 23; wk = '4'; wk1 = '3'; wk2 = '2'; wk3 = '1'

    mm  = reptdate.month
    mm1 = (mm - 1) if wk != '1' else (mm - 1 if mm > 1 else 12)
    if mm1 == 0:
        mm1 = 12
    mm2 = mm - 1
    if mm2 == 0:
        mm2 = 12

    sdate     = date(reptdate.year, mm, sdd)
    reptmon   = str(mm).zfill(2)
    reptmon1  = str(mm1).zfill(2)
    reptmon2  = str(mm2).zfill(2)
    reptyear2 = reptdate.strftime('%y')    # YEAR2.
    ryear     = str(reptdate.year)         # YEAR4.
    reptday   = str(day).zfill(2)
    rdate     = reptdate.strftime('%d/%m/%y')   # DDMMYY8.
    sdate_s   = sdate.strftime('%d/%m/%y')
    mdate_int = pydate_to_sasdate(reptdate)     # Z5. value

    return {
        'reptdate':  reptdate,
        'sdate':     sdate,
        'wk':        wk, 'wk1': wk1, 'wk2': wk2, 'wk3': wk3,
        'mm':        mm, 'mm1': mm1, 'mm2': mm2, 'sdd': sdd,
        'reptmon':   reptmon,
        'reptmon1':  reptmon1,
        'reptmon2':  reptmon2,
        'reptyear':  reptyear2,
        'ryear':     ryear,
        'reptday':   reptday,
        'rdate':     rdate,
        'sdate_s':   sdate_s,
        'nowk':      wk,
        'mdate_int': mdate_int,
    }

# =============================================================================
# BUILD BASE LOAN DATASET
# =============================================================================

def build_loan_dataset(rv: dict) -> pl.DataFrame:
    """
    PROC SORT DATA=SASD.LOAN<MM>   OUT=DLOAN;
    PROC SORT DATA=BNM.LOAN<MM><WK>  OUT=MLOAN;
    PROC SORT DATA=BNM.LNWOF<MM><WK> OUT=LNWOF;
    PROC SORT DATA=BNM.LNWOD<MM><WK> OUT=LNWOD;
    PROC SORT DATA=BNM.LNWOF<MM2><WK> OUT=PLNWOF;
    PROC SORT DATA=BNM.LNWOD<MM2><WK> OUT=PLNWOD;
    DATA LOANDM: MERGE DLOAN(IN=A) MLOAN(IN=B); IF A AND NOT B;
    DATA LOAN<MM><WK>: MERGE PLNWOF PLNWOD LOANDM BNM.LOAN<MM2><WK> MLOAN LNWOF LNWOD;
    * IF ACCTYPE='OD' AND PRODUCT IN (150,151,152,181) THEN DELETE;
    """
    mm  = rv['reptmon'];  mm2 = rv['reptmon2'];  wk = rv['nowk']

    dloan    = load_parquet(f"{SASD_LOAN_PREFIX}{mm}.parquet")
    mloan    = load_parquet(f"{BNM_LOAN_PREFIX}{mm}{wk}.parquet")
    lnwof    = load_parquet(f"{BNM_LNWOF_PREFIX}{mm}{wk}.parquet")
    lnwod    = load_parquet(f"{BNM_LNWOD_PREFIX}{mm}{wk}.parquet")
    plnwof   = load_parquet(f"{BNM_LNWOF_PREFIX}{mm2}{wk}.parquet")
    plnwod   = load_parquet(f"{BNM_LNWOD_PREFIX}{mm2}{wk}.parquet")
    loan_prev= load_parquet(f"{BNM_LOAN_PREFIX}{mm2}{wk}.parquet")

    # DATA LOANDM: MERGE DLOAN(IN=A) MLOAN(IN=B); IF A AND NOT B
    key = ['acctno', 'noteno']
    if not dloan.is_empty() and not mloan.is_empty():
        marker = mloan.select(key).with_columns(pl.lit(True).alias('_b'))
        loandm = dloan.join(marker, on=key, how='left')
        loandm = loandm.filter(pl.col('_b').is_null()).drop('_b')
    elif not dloan.is_empty():
        loandm = dloan
    else:
        loandm = pl.DataFrame()

    # DATA LOAN<MM><WK>: MERGE PLNWOF PLNWOD LOANDM BNM.LOAN<MM2><WK> MLOAN LNWOF LNWOD
    # SAS merge by key: last non-null value wins; process sequentially
    frames = [f for f in [plnwof, plnwod, loandm, loan_prev, mloan, lnwof, lnwod]
              if not f.is_empty()]
    if not frames:
        return pl.DataFrame()

    base = frames[0]
    for f in frames[1:]:
        non_key = [c for c in f.columns if c not in key]
        merged  = base.join(f.select(key + non_key), on=key, how='outer', suffix='_r')
        for col in non_key:
            rc = f"{col}_r"
            if rc in merged.columns:
                merged = merged.with_columns(
                    pl.when(pl.col(rc).is_not_null())
                      .then(pl.col(rc))
                      .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                      .alias(col)
                ).drop(rc)
        base = merged

    # * IF ACCTYPE='OD' AND PRODUCT IN (150,151,152,181) THEN DELETE;
    return base

# =============================================================================
# BUILD DISPAY
# =============================================================================

def build_dispay(rv: dict, loan_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA DISPAY: SET DISPAY.DISPAYMTH<MM>;
      DISBURSE=ROUND(DISBURSE,0.01); REPAID=ROUND(REPAID,0.01);
      WHERE DISBURSE>0 OR REPAID>0;
    DATA DISPAY: MERGE LOAN(IN=A) DISPAY(IN=B); BY ACCTNO NOTENO; IF A & B;
    """
    path = f"{DISPAY_PREFIX}{rv['reptmon']}.parquet"
    raw  = load_parquet(path)
    if raw.is_empty() or loan_df.is_empty():
        return pl.DataFrame()

    dispay = raw.with_columns([
        pl.col('disburse').round(2),
        pl.col('repaid').round(2),
    ]).filter(
        (pl.col('disburse') > 0) | (pl.col('repaid') > 0)
    )

    # MERGE LOAN(IN=A) DISPAY(IN=B); IF A & B
    return loan_df.join(dispay, on=['acctno','noteno'], how='inner', suffix='_dp')

# =============================================================================
# BUILD CL_FEE AND AUGMENT LOAN
# =============================================================================

def build_cl_fee(rv: dict) -> pl.DataFrame:
    """
    PROC SORT DATA=FEE.LNFEE<MM><WK> OUT=M_FEE(KEEP=ACCTNO NOTENO DUETOTAL FEEPLAN);
    PROC SUMMARY: WHERE FEEPLAN='CL' AND DUETOTAL>0; SUM DUETOTAL by ACCTNO NOTENO;
    """
    path = f"{FEE_LNFEE_PREFIX}{rv['reptmon']}{rv['nowk']}.parquet"
    raw  = load_parquet(path)
    if raw.is_empty():
        return pl.DataFrame(schema={'acctno': pl.Int64, 'noteno': pl.Int64,
                                    'duetotal': pl.Float64})
    fee = raw.select([c for c in ['acctno','noteno','duetotal','feeplan']
                      if c in raw.columns])
    fee = fee.filter(
        (pl.col('feeplan') == 'CL') & (pl.col('duetotal') > 0)
    )
    if fee.is_empty():
        return pl.DataFrame(schema={'acctno': pl.Int64, 'noteno': pl.Int64,
                                    'duetotal': pl.Float64})
    return fee.group_by(['acctno','noteno']).agg(pl.col('duetotal').sum())


def merge_loan_cl_fee(loan_df: pl.DataFrame, cl_fee: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN: MERGE LOAN(IN=A) CL_FEE(IN=B); BY ACCTNO NOTENO; IF A;
    CLFEE = DUETOTAL * FORATE;
    DROP _TYPE_ _FREQ_;
    """
    if loan_df.is_empty():
        return loan_df
    if cl_fee.is_empty():
        if 'clfee' not in loan_df.columns:
            loan_df = loan_df.with_columns(pl.lit(0.0).alias('clfee'))
        return loan_df

    merged = loan_df.join(cl_fee, on=['acctno','noteno'], how='left', suffix='_fee')
    if 'duetotal_fee' in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col('duetotal_fee').is_not_null())
              .then(pl.col('duetotal_fee'))
              .otherwise(pl.col('duetotal') if 'duetotal' in merged.columns else pl.lit(None))
              .alias('duetotal')
        ).drop('duetotal_fee')

    forate_col = 'forate' if 'forate' in merged.columns else None
    if forate_col:
        merged = merged.with_columns(
            (pl.col('duetotal').fill_null(0.0) * pl.col(forate_col).fill_null(0.0)).alias('clfee')
        )
    else:
        merged = merged.with_columns(pl.lit(0.0).alias('clfee'))

    for col in ['_type_','_freq_']:
        if col in merged.columns:
            merged = merged.drop(col)
    return merged

# =============================================================================
# BUILD ALM (All Loans Master)
# =============================================================================

def build_alm(loan_raw: pl.DataFrame) -> tuple:
    """
    MERGE LOAN(RENAME BALANCE->ORIBAL, BAL_AFT_EIR->BALANCE) LNCOMM;
    BY ACCTNO COMMNO; IF A;
    Filters, NOACCT logic, ALMBT split.
    Returns (alm_df, almbt_df)
    """
    if loan_raw.is_empty():
        return pl.DataFrame(), pl.DataFrame()

    lncomm = load_parquet(LOAN_LNCOMM_PARQUET)
    if not lncomm.is_empty():
        lncomm_sel = lncomm.select([c for c in ['acctno','commno','cusedamt']
                                    if c in lncomm.columns])
    else:
        lncomm_sel = pl.DataFrame()

    # RENAME: BALANCE->ORIBAL, BAL_AFT_EIR->BALANCE
    renames = {}
    if 'balance' in loan_raw.columns:
        renames['balance'] = 'oribal'
    if 'bal_aft_eir' in loan_raw.columns:
        renames['bal_aft_eir'] = 'balance'
    if renames:
        loan_raw = loan_raw.rename(renames)

    # Merge LNCOMM by ACCTNO COMMNO
    if not lncomm_sel.is_empty() and 'commno' in loan_raw.columns:
        merged = loan_raw.join(lncomm_sel, on=['acctno','commno'], how='left', suffix='_lc')
        for col in lncomm_sel.columns:
            lc = f"{col}_lc"
            if lc in merged.columns:
                merged = merged.with_columns(
                    pl.when(pl.col(lc).is_not_null())
                      .then(pl.col(lc))
                      .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                      .alias(col)
                ).drop(lc)
    else:
        merged = loan_raw

    keep = ['acctno','noteno','fisspurp','product','noteterm','earnterm',
            'balance','paidind','apprdate','apprlim2','prodcd','custcd',
            'amtind','sectorcd','acctype','branch','cjfee','oribal',
            'dnbfisme','noacct','commno','cusedamt','rleasamt','clfee',
            'eir_adj','retailid']
    avail  = [c for c in keep if c in merged.columns]
    merged = merged.select(avail)

    rows     = merged.to_dicts()
    alm_rows = []
    almbt_rows = []

    for row in rows:
        paidind  = coalesce_s(row.get('paidind'))
        eir_adj  = row.get('eir_adj')
        oribal   = coalesce_f(row.get('oribal'))
        prodcd   = coalesce_s(row.get('prodcd'))
        acctype  = coalesce_s(row.get('acctype'))
        acctno   = int(row.get('acctno') or 0)
        noteno   = int(row.get('noteno') or 0)
        product  = int(row.get('product') or 0)
        commno   = int(row.get('commno') or 0)
        cusedamt = coalesce_f(row.get('cusedamt'))
        rleasamt = coalesce_f(row.get('rleasamt'))
        cjfee    = coalesce_f(row.get('cjfee'))
        clfee    = coalesce_f(row.get('clfee'))

        # IF PAIDIND NOT IN ('P','C') OR EIR_ADJ NE .
        eir_adj_set = (eir_adj is not None and
                       not (isinstance(eir_adj, float) and eir_adj != eir_adj))
        if paidind in ('P', 'C') and not eir_adj_set:
            continue

        # FORMAT BALX 14.2; XIND=' ';
        # IF ORIBAL=-.00 THEN XIND='Y'
        xind = ' '
        if oribal == -0.0 and str(oribal) in ('-0.0', '-0.00'):
            xind = 'Y'
        balx = round(oribal, 2)
        if balx in (0.0, -0.0):
            xind = 'Y'

        # * IF PAIDIND='P' OR XIND='Y' THEN DELETE;
        # IF XIND='Y' THEN DELETE
        if xind == 'Y':
            continue

        # IF SUBSTR(PRODCD,1,2) EQ '34' OR PRODCD EQ '54120'
        if not (prodcd[:2] == '34' or prodcd == '54120'):
            continue

        # * IF PRODUCT IN (150,151,152,181) THEN DELETE;

        # NOACCT logic for LN
        noacct = int(row.get('noacct') or 0)
        if acctype == 'LN':
            eligible = False
            if (rleasamt != 0.0 and paidind not in ('P','C') and
                    oribal > 0 and cjfee != oribal):
                eligible = True
            elif (rleasamt == 0.0 and paidind not in ('P','C') and
                  oribal > 0 and 600 <= product <= 699):
                eligible = True
            elif (rleasamt == 0.0 and paidind not in ('P','C') and
                  oribal > 0 and commno > 0 and cusedamt > 0):
                eligible = True
            if not eligible:
                noacct = 0
            # IF RLEASAMT^=0 AND ORIBAL=CLFEE THEN NOACCT=0
            if rleasamt != 0 and oribal == clfee:
                noacct = 0

        # IF PAIDIND NOT IN ('P','C') & CJFEE NE ORIBAL AND NOACCT^=0 AND ROUND(ORIBAL,.01)...
        if (paidind not in ('P','C') and cjfee != oribal and noacct != 0 and
                round(oribal, 2) not in (0.0, -0.0)):
            noacct = 1
        row['noacct'] = noacct

        # Split ALMBT vs ALM
        if ((2500000000 <= acctno <= 2599999999 and 40000 <= noteno <= 49999) or
                product == 321):
            almbt_rows.append(row)
        else:
            alm_rows.append(row)

    alm_df   = pl.from_dicts(alm_rows)   if alm_rows   else pl.DataFrame()
    almbt_df = pl.from_dicts(almbt_rows) if almbt_rows else pl.DataFrame()

    # DATA ALM: BY ACCTNO COMMNO — deduplicate NOACCT for revolving products
    if not alm_df.is_empty() and 'commno' in alm_df.columns:
        alm_df  = alm_df.sort(['acctno', 'commno'])
        rows2   = alm_df.to_dicts()
        prev_ac = None; prev_cm = None; unq = 0
        for row in rows2:
            ac = row['acctno']; cm = row.get('commno')
            pd_ = coalesce_s(row.get('prodcd'))
            if ac != prev_ac or cm != prev_cm:
                unq = 0
            if pd_ in ('34170','34190','34690'):
                unq += (row.get('noacct') or 0)  # *17-513
                if unq > 1:
                    row['noacct'] = 0
            prev_ac = ac; prev_cm = cm
        alm_df = pl.from_dicts(rows2)

    # DATA ALMBT: BY ACCTNO — first row per ACCTNO gets NOACCT=1, rest 0
    if not almbt_df.is_empty():
        almbt_df = almbt_df.sort('acctno')
        rows3    = almbt_df.to_dicts()
        prev_ac  = None
        for row in rows3:
            ac = row['acctno']
            row['noacct'] = 1 if ac != prev_ac else 0
            prev_ac = ac
        almbt_df = pl.from_dicts(rows3)

    # DATA ALM: SET ALM ALMBT
    alm_df = (pl.concat([alm_df, almbt_df], how='diagonal')
              if not almbt_df.is_empty() else alm_df)

    return alm_df

# =============================================================================
# APPLY PRODESC
# =============================================================================

def apply_prodesc(df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA ALM: SET ALM;
    Assign PRODESC based on ACCTYPE/PRODCD/PRODUCT logic.
    """
    if df.is_empty():
        return df
    rows = df.to_dicts()
    for row in rows:
        product = int(row.get('product') or 0)
        acctype = coalesce_s(row.get('acctype'))
        prodcd  = coalesce_s(row.get('prodcd'))

        prodesc = ''
        if (acctype == 'LN' and prodcd == '34111') or product in {678,679,993,996}:
            prodesc = 'HIRE PURCHASE'
        elif acctype == 'LN' and prodcd == '34120':
            prodesc = 'RETAIL HOUSING LOANS'
            if product in HLCORP:
                prodesc = 'CORP. BANKING HOUSING LOANS'
        elif acctype == 'OD' and prodcd in ('34180','34240') and product in ODCORP:
            prodesc = 'OD CORPORATE'
        elif acctype == 'OD' and prodcd in ('34180','34240') and product not in ODCORP:
            prodesc = 'OD RETAIL'
        elif (acctype == 'LN' and prodcd not in ('34111','34120','N','M') and
              product in FLCORP):
            prodesc = 'CORP. BANKING LOANS'
        elif (acctype == 'LN' and prodcd not in ('34111','34120','N','M') and
              product not in FLCORP):
            prodesc = 'OTHERS RETAIL'
        if acctype == 'LN' and prodcd == '34170':
            prodesc = 'FLOOR STOCKING LOANS'

        row['prodesc'] = prodesc
    return pl.from_dicts(rows)

# =============================================================================
# BUILD BTRADE DATASET
# =============================================================================

def build_btrade(rv: dict) -> tuple:
    """
    PROC SORT DATA=BTBNM.BTRAD<MM><WK> OUT=BTRAD1;
    WHERE DIRCTIND='D' AND CUSTCD NE ' ';
    ... OVC / MAST / ALMBT (Bills detail) / ALMLOAN summary / MFRS outputs
    Returns: (alm_bt_df, almloan_bt_df, mast_df)
    """
    mm = rv['reptmon']; wk = rv['nowk']
    path = f"{BTBNM_BTRAD_PREFIX}{mm}{wk}.parquet"

    btrad_raw = load_parquet_where(
        path, "dirctind = 'D' AND custcd IS NOT NULL AND custcd <> ' '"
    )
    if btrad_raw.is_empty():
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

    if 'apprlimt' in btrad_raw.columns:
        btrad1 = btrad_raw.sort(['acctno','apprlimt'], descending=[False, True])
    else:
        btrad1 = btrad_raw.sort('acctno')

    grp_key1 = [c for c in ['acctno','custcd','retailid','sectorcd','dnbfisme']
                if c in btrad1.columns]
    agg_v1   = [c for c in ['disburse','repaid'] if c in btrad1.columns]
    btrad2   = btrad1.group_by(grp_key1).agg(
        [pl.col(c).sum() for c in agg_v1]
    ) if grp_key1 and agg_v1 else pl.DataFrame()

    grp_key2 = [c for c in ['acctno','custcd','retailid','sectorcd']
                if c in btrad1.columns]
    btrad1_bal = pl.DataFrame()
    if 'apprlimt' in btrad1.columns and 'balance' in btrad1.columns and grp_key2:
        btrad1_bal = btrad1.filter(pl.col('apprlimt') > 0).group_by(grp_key2).agg(
            pl.col('balance').sum()
        )

    # DATA OVC MAST: MERGE BTRAD1(IN=A) BTRAD2(IN=B); BY ACCTNO CUSTCD RETAILID SECTORCD; IF B
    merge_key = [c for c in ['acctno','custcd','retailid','sectorcd']
                 if c in btrad2.columns]
    if not btrad1_bal.is_empty() and not btrad2.is_empty():
        mast_m = btrad2.join(btrad1_bal, on=merge_key, how='left', suffix='_bal')
        if 'balance_bal' in mast_m.columns:
            mast_m = mast_m.with_columns(
                pl.when(pl.col('balance_bal').is_not_null())
                  .then(pl.col('balance_bal'))
                  .otherwise(pl.col('balance') if 'balance' in mast_m.columns else pl.lit(None))
                  .alias('balance')
            ).drop('balance_bal')
    else:
        mast_m = btrad2

    mast_m = mast_m.with_columns([
        pl.when(pl.col('disburse') > 0).then(pl.lit(1)).otherwise(pl.lit(0)).alias('disbno'),
        pl.when(pl.col('repaid')   > 0).then(pl.lit(1)).otherwise(pl.lit(0)).alias('repayno'),
    ])
    # IF A AND ROUND(BALANCE,0.01) NOT IN (.,0) AND ACCTNO^=0 THEN NOACCT=1
    if 'balance' in mast_m.columns:
        mast_m = mast_m.with_columns(
            pl.when(
                pl.col('balance').round(2).is_not_null() &
                (pl.col('balance').round(2) != 0) &
                (pl.col('acctno') != 0)
            ).then(pl.lit(1)).otherwise(pl.lit(0)).alias('noacct')
        )

    ovc_df = mast_m.select([c for c in ['acctno','retailid'] if c in mast_m.columns])
    mast_keep = [c for c in ['acctno','custcd','balance','retailid','disbno','repayno',
                              'noacct','sectorcd','dnbfisme'] if c in mast_m.columns]
    mast_df = mast_m.select(mast_keep)

    # PROC SORT BTRAD WHERE SUBSTR(PRODCD,1,2)='34'
    alm_bt_raw = load_parquet_where(
        path, "SUBSTR(CAST(prodcd AS VARCHAR),1,2) = '34'"
    )
    if alm_bt_raw.is_empty():
        return pl.DataFrame(), pl.DataFrame(), mast_df

    bt_keep = [c for c in ['acctno','subacct','fisspurp','product','noteterm','balance',
                            'apprlim2','prodcd','custcd','amtind','transref','sectorcd',
                            'disburse','repaid','dnbfisme','retailid']
               if c in alm_bt_raw.columns]
    alm_bt = alm_bt_raw.select(bt_keep)

    # DATA ALM: MERGE OVC ALM(IN=A); BY ACCTNO; IF A
    if not ovc_df.is_empty():
        alm_bt = alm_bt.join(ovc_df, on='acctno', how='inner', suffix='_ovc')
        if 'retailid_ovc' in alm_bt.columns:
            alm_bt = alm_bt.with_columns(
                pl.when(pl.col('retailid_ovc').is_not_null())
                  .then(pl.col('retailid_ovc'))
                  .otherwise(pl.col('retailid') if 'retailid' in alm_bt.columns else pl.lit(None))
                  .alias('retailid')
            ).drop('retailid_ovc')

    # PROC SUMMARY ALMX: SUM BALANCE by ACCTNO TRANSREF CUSTCD FISSPURP SECTORCD
    bt_grp = [c for c in ['acctno','transref','custcd','fisspurp','sectorcd']
              if c in alm_bt.columns]
    if bt_grp and 'balance' in alm_bt.columns:
        almx   = alm_bt.group_by(bt_grp).agg(pl.col('balance').sum().alias('balance'))
        alm_bt = alm_bt.sort(bt_grp).unique(subset=bt_grp, keep='first')
        alm_bt = alm_bt.drop('balance').join(almx, on=bt_grp, how='left')

    alm_bt = alm_bt.with_columns([
        pl.col('disburse').cast(pl.Float64).fill_null(0.0) if 'disburse' in alm_bt.columns
        else pl.lit(0.0).alias('disburse'),
        pl.col('repaid').cast(pl.Float64).fill_null(0.0)   if 'repaid'   in alm_bt.columns
        else pl.lit(0.0).alias('repaid'),
        pl.col('balance').cast(pl.Float64).fill_null(0.0)  if 'balance'  in alm_bt.columns
        else pl.lit(0.0).alias('balance'),
    ])

    # DATA ALM: PRODESC assignment
    rows = alm_bt.to_dicts()
    for row in rows:
        retailid = coalesce_s(row.get('retailid'))
        row['prodesc'] = 'BILLS CORPORATE' if retailid == 'C' else 'BILLS RETAIL'
    alm_bt = pl.from_dicts(rows)

    # DATA MAST: PRODESC + MFRS.MAST_BR
    mast_rows  = mast_df.to_dicts()
    mast_br_list = []
    for row in mast_rows:
        retailid = coalesce_s(row.get('retailid'))
        row['prodesc'] = 'BILLS CORPORATE' if retailid == 'C' else 'BILLS RETAIL'
        try:
            acctno = int(str(row.get('acctno') or 0))
        except (ValueError, TypeError):
            acctno = 0
        row['acctno'] = acctno
        mast_br_list.append({'acctno': acctno,
                             'prodesc': row['prodesc'],
                             'noacct': row.get('noacct', 0)})

    mast_df = pl.from_dicts(mast_rows)
    if mast_br_list:
        pl.from_dicts(mast_br_list).write_parquet(MFRS_MAST_BR_PARQUET)

    # PROC SUMMARY ALM by PRODESC -> ALMLOAN
    agg_v = [c for c in ['disburse','repaid','balance'] if c in alm_bt.columns]
    almloan_bt = alm_bt.group_by('prodesc').agg(
        [pl.col(c).sum() for c in agg_v]
    ) if agg_v else pl.DataFrame()

    # PROC SUMMARY MAST by PRODESC -> MASTLOAN; merge
    mast_agg_v = [c for c in ['disbno','repayno','noacct'] if c in mast_df.columns]
    if mast_agg_v and not mast_df.is_empty():
        mastloan = mast_df.group_by('prodesc').agg(
            [pl.col(c).sum() for c in mast_agg_v]
        )
        if not almloan_bt.is_empty():
            almloan_bt = almloan_bt.join(mastloan, on='prodesc', how='left')
        else:
            almloan_bt = mastloan

    return alm_bt, almloan_bt, mast_df

# =============================================================================
# REPORT PRINT HELPERS
# =============================================================================

NUM_COLS = ['disburse','repaid','disbno','repayno','balance','noacct']

def fmt_num(val, decimals: int = 2) -> str:
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * (16 if decimals == 2 else 10)
    if decimals == 2:
        return f"{float(val):16.2f}"
    return f"{float(val):10.0f}"


def print_table(df: pl.DataFrame, rw: ReportWriter,
                title1: str, title2: str = ''):
    """Equivalent to PROC PRINT with SUM: DISBURSE REPAID DISBNO REPAYNO BALANCE NOACCT."""
    if df.is_empty():
        return

    rw.write_titles(title1, title2)

    hdr = (f"{'PRODESC':<35}"
           f"{'DISBURSE':>16}"
           f"{'REPAID':>16}"
           f"{'DISBNO':>10}"
           f"{'REPAYNO':>10}"
           f"{'BALANCE':>16}"
           f"{'NOACCT':>10}")
    rw.write_line(' ' + hdr)
    rw.write_line(' ' + '-' * len(hdr))

    tot = {c: 0.0 for c in NUM_COLS}
    for row in df.sort('prodesc').iter_rows(named=True):
        prodesc = coalesce_s(row.get('prodesc'))[:35]
        line = (f"{prodesc:<35}"
                f"{fmt_num(row.get('disburse'))}"
                f"{fmt_num(row.get('repaid'))}"
                f"{fmt_num(row.get('disbno'),  0)}"
                f"{fmt_num(row.get('repayno'), 0)}"
                f"{fmt_num(row.get('balance'))}"
                f"{fmt_num(row.get('noacct'),  0)}")
        rw.write_line(' ' + line)
        for c in NUM_COLS:
            v = row.get(c)
            if v is not None and not (isinstance(v, float) and v != v):
                tot[c] += float(v)

    rw.write_line(' ' + '=' * len(hdr))
    sum_line = (f"{'SUM':<35}"
                f"{fmt_num(tot['disburse'])}"
                f"{fmt_num(tot['repaid'])}"
                f"{fmt_num(tot['disbno'],  0)}"
                f"{fmt_num(tot['repayno'], 0)}"
                f"{fmt_num(tot['balance'])}"
                f"{fmt_num(tot['noacct'],  0)}")
    rw.write_line(' ' + sum_line)
    rw.blank()


def print_tabulate_sector(df: pl.DataFrame, rw: ReportWriter,
                           title1: str, title2: str):
    """
    PROC TABULATE: TABLE SECGROUP*(SECTYPE ALL) ALL, SUM*(BALANCE NOACCT) / BOX='SECTFISS'
    """
    if df.is_empty():
        return
    rw.write_titles(title1, title2)
    hdr = f"{'SECTFISS':<25}{'AMOUNT':>18}{'NO. OF ACCT':>12}"
    rw.write_line(' ' + hdr)
    rw.write_line(' ' + '-' * len(hdr))

    grp = df.group_by(['secgroup','sectype']).agg([
        pl.col('balance').sum() if 'balance' in df.columns else pl.lit(0.0).alias('balance'),
        pl.col('noacct').sum()  if 'noacct'  in df.columns else pl.lit(0.0).alias('noacct'),
    ]).sort(['secgroup','sectype'])

    grand_bal = 0.0; grand_noa = 0.0
    for sg_val in grp.select('secgroup').unique(maintain_order=True).to_series().to_list():
        sub = grp.filter(pl.col('secgroup') == sg_val).sort('sectype')
        sub_bal = 0.0; sub_noa = 0.0
        for row in sub.iter_rows(named=True):
            st  = coalesce_s(row.get('sectype'))[:25]
            bal = float(row.get('balance') or 0.0)
            noa = float(row.get('noacct')  or 0.0)
            rw.write_line(' ' + f"  {st:<23}{bal:18.2f}{noa:12.0f}")
            sub_bal += bal; sub_noa += noa
        rw.write_line(' ' + f"{'  SUB-TOTAL':<25}{sub_bal:18.2f}{sub_noa:12.0f}")
        grand_bal += sub_bal; grand_noa += sub_noa

    rw.write_line(' ' + '=' * len(hdr))
    rw.write_line(' ' + f"{'GRAND TOTAL':<25}{grand_bal:18.2f}{grand_noa:12.0f}")
    rw.blank()


def print_tabulate_product(df: pl.DataFrame, rw: ReportWriter,
                            title1: str, title2: str):
    """
    PROC TABULATE: TABLE TYPE='', SUM*(BALANCE NOACCT) / BOX='FACILITY'
    """
    if df.is_empty():
        return
    rw.write_titles(title1, title2)
    hdr = f"{'FACILITY':<25}{'AMOUNT':>18}{'NO. OF ACCT':>12}"
    rw.write_line(' ' + hdr)
    rw.write_line(' ' + '-' * len(hdr))

    grp = df.group_by('type').agg([
        pl.col('balance').sum() if 'balance' in df.columns else pl.lit(0.0).alias('balance'),
        pl.col('noacct').sum()  if 'noacct'  in df.columns else pl.lit(0.0).alias('noacct'),
    ]).sort('type')

    grand_bal = 0.0; grand_noa = 0.0
    for row in grp.iter_rows(named=True):
        t   = coalesce_s(row.get('type'))[:25]
        bal = float(row.get('balance') or 0.0)
        noa = float(row.get('noacct')  or 0.0)
        rw.write_line(' ' + f"{t:<25}{bal:18.2f}{noa:12.0f}")
        grand_bal += bal; grand_noa += noa

    rw.write_line(' ' + '=' * len(hdr))
    rw.write_line(' ' + f"{'GRAND TOTAL':<25}{grand_bal:18.2f}{grand_noa:12.0f}")
    rw.blank()


def summarise(df: pl.DataFrame, class_cols: list) -> pl.DataFrame:
    """PROC SUMMARY NWAY MISSING: sum NUM_COLS by class_cols."""
    if df.is_empty():
        return pl.DataFrame()
    agg_v = [c for c in NUM_COLS if c in df.columns]
    if not agg_v:
        return pl.DataFrame()
    return df.group_by(class_cols).agg([pl.col(c).sum() for c in agg_v])

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("EIMBNM01: Starting Public Bank Berhad loan summary reports...")

    rv    = get_report_vars()
    mm_yr = f"{rv['reptmon']}/{rv['ryear']}"
    print(f"  Report date: {rv['reptdate']}  MM={rv['reptmon']} YY={rv['ryear']} WK={rv['nowk']}")

    rw    = ReportWriter()
    RPT   = 'REPORT ID : EIMBNM01'

    # =========================================================================
    # Build base loan dataset
    # =========================================================================
    loan_base = build_loan_dataset(rv)

    # Build DISPAY
    dispay_df = build_dispay(rv, loan_base)

    # Build CL_FEE and merge into BNM.LOAN<MM><WK>
    mm = rv['reptmon']; wk = rv['nowk']
    bnm_loan = load_parquet(f"{BNM_LOAN_PREFIX}{mm}{wk}.parquet")
    cl_fee   = build_cl_fee(rv)
    bnm_loan = merge_loan_cl_fee(bnm_loan, cl_fee)

    # Build ALM
    alm_df = build_alm(bnm_loan)

    # Merge DISPAY into ALM (WHERE SUBSTR(PRODCD,1,2)='34' OR PRODUCT IN (678,679,993,996))
    if not dispay_df.is_empty() and not alm_df.is_empty():
        dp_filt = dispay_df
        if 'prodcd' in dp_filt.columns:
            dp_filt = dp_filt.filter(
                (pl.col('prodcd').str.slice(0,2) == '34') |
                pl.col('product').is_in([678,679,993,996])
            )
        dp_sel = dp_filt.select(
            [c for c in ['acctno','noteno','disburse','repaid'] if c in dp_filt.columns]
        )
        alm_df = alm_df.join(dp_sel, on=['acctno','noteno'], how='left', suffix='_dp')
        for c in ['disburse','repaid']:
            dc = f"{c}_dp"
            if dc in alm_df.columns:
                alm_df = alm_df.with_columns(
                    pl.when(pl.col(dc).is_not_null())
                      .then(pl.col(dc))
                      .otherwise(pl.col(c) if c in alm_df.columns else pl.lit(None))
                      .alias(c)
                ).drop(dc)

    alm_df = alm_df.with_columns([
        pl.when(pl.col('repaid')   > 0).then(pl.lit(1)).otherwise(pl.lit(0)).alias('repayno'),
        pl.when(pl.col('disburse') > 0).then(pl.lit(1)).otherwise(pl.lit(0)).alias('disbno'),
    ]) if not alm_df.is_empty() else alm_df

    # Apply PRODESC
    alm_df = apply_prodesc(alm_df)

    # =========================================================================
    # /***********************/
    # /*   FACTORING LOANS   */
    # /***********************/
    # %INC PGM(RDL2PBIF);
    pbif_df = build_pbif(
        reptdate  = rv['reptdate'],
        reptyear  = rv['reptyear'],
        reptmon   = rv['reptmon'],
        reptday   = rv['reptday'],
        mdate_int = rv['mdate_int'],
    )

    # DATA PBIF: SET PBIF; PRODESC='FACTORING'; REPAYNO/DISBNO/NOACCT derivation
    if not pbif_df.is_empty():
        rows = pbif_df.to_dicts()
        for row in rows:
            row['prodesc'] = 'FACTORING'
            repaid   = coalesce_f(row.get('repaid'))
            disburse = coalesce_f(row.get('disburse'))
            balance  = coalesce_f(row.get('balance'))
            noacct   = int(row.get('noacct') or 0)
            row['repayno'] = 1 if repaid   > 0 else 0
            row['disbno']  = 1 if disburse > 0 else 0
            if balance > 0 and noacct != 0:
                row['noacct'] = 1
        pbif_df = pl.from_dicts(rows)

    # =========================================================================
    # DATA ALMNEW: SET ALM PBIF
    # =========================================================================
    almnew_df = pl.concat(
        [f for f in [alm_df, pbif_df] if not f.is_empty()], how='diagonal'
    ) if any(not f.is_empty() for f in [alm_df, pbif_df]) else alm_df

    # PROC SUMMARY ALMNEW -> ALMLOAN; PROC PRINT: ALL LOANS
    almloan_df = summarise(almnew_df, ['prodesc'])
    print_table(almloan_df, rw,
                f"ALL LOANS AS AT {mm_yr}", RPT)

    # =========================================================================
    # DATA ALM2 COM3: SET ALM; WHERE PRODESC IN (OD RETAIL, OTHERS RETAIL, FLOOR STOCKING)
    # =========================================================================
    alm2_rows = []
    com3_rows = []
    if not alm_df.is_empty():
        for row in alm_df.filter(
            pl.col('prodesc').is_in(['OD RETAIL','OTHERS RETAIL','FLOOR STOCKING LOANS'])
        ).to_dicts():
            pd_   = coalesce_s(row.get('prodesc'))
            prod  = int(row.get('product') or 0)
            fiss  = coalesce_s(row.get('fisspurp'))
            tycode = 0

            if pd_ == 'OD RETAIL':
                if prod in ODRTLA and fiss in ODFISS:
                    row['prodesc'] = 'PURCHASE OF RESIDENTIAL PROPERTY'
                elif prod in ODRTLB:
                    row['prodesc'] = 'SHARE MARGIN FINANCING'
                else:
                    row['prodesc'] = 'TOTAL COMMERCIAL RETAILS'
                tycode = 1
                row['tycode'] = tycode
            elif pd_ == 'OTHERS RETAIL':
                if prod in OTRTLA:
                    row['prodesc'] = 'PERSONAL LOAN'
                elif prod in OTRTLB:
                    row['prodesc'] = 'STAFF LOAN'
                else:
                    row['prodesc'] = 'TOTAL COMMERCIAL RETAILS'
                tycode = 2
                row['tycode'] = tycode
            elif pd_ == 'FLOOR STOCKING LOANS':
                row['prodesc'] = 'TOTAL COMMERCIAL RETAILS'
                tycode = 3
                row['tycode'] = tycode

            alm2_rows.append(row)
            com3_rows.append(dict(row))  # COM3 is also SET ALM2 later

    alm2_df = pl.from_dicts(alm2_rows) if alm2_rows else pl.DataFrame()
    com3_df = pl.from_dicts(com3_rows) if com3_rows else pl.DataFrame()

    # DATA PBIF1: SET PBIF; PRODESC='FACTORING'->'TOTAL COMMERCIAL RETAILS'
    pbif1_df = pl.DataFrame()
    if not pbif_df.is_empty():
        rows = pbif_df.to_dicts()
        for row in rows:
            if coalesce_s(row.get('prodesc')) == 'FACTORING':
                row['prodesc'] = 'TOTAL COMMERCIAL RETAILS'
        pbif1_df = pl.from_dicts(rows)

    # DATA ALM2NEW ALM2CRL MFRS.ALM_CR: SET ALM2 PBIF1
    alm2new_src = pl.concat(
        [f for f in [alm2_df, pbif1_df] if not f.is_empty()], how='diagonal'
    ) if any(not f.is_empty() for f in [alm2_df, pbif1_df]) else pl.DataFrame()

    # MFRS.ALM_CR
    if not alm2new_src.is_empty():
        alm_cr_keep = [c for c in ['acctno','noteno','prodesc','noacct']
                       if c in alm2new_src.columns]
        alm2new_src.select(alm_cr_keep).write_parquet(MFRS_ALM_CR_PARQUET)

    alm2crl_rows = []
    if not alm2new_src.is_empty():
        for row in alm2new_src.to_dicts():
            if coalesce_s(row.get('prodesc')) == 'TOTAL COMMERCIAL RETAILS':
                custcd = coalesce_s(row.get('custcd'))
                r2     = dict(row)
                r2['prodesc'] = ('COMMERCIAL RETAIL - IND'
                                 if custcd in INDIV_CUSTCDS
                                 else 'COMMERCIAL RETAIL - NON IND')
                alm2crl_rows.append(r2)
    alm2crl_df = pl.from_dicts(alm2crl_rows) if alm2crl_rows else pl.DataFrame()

    # PROC PRINT: RETAIL LOANS
    almloan2_df = summarise(alm2new_src, ['prodesc'])
    print_table(almloan2_df, rw,
                f"RETAILS LOANS AS AT {mm_yr}", RPT)

    # PROC PRINT: COMMERCIAL RETAIL LOANS
    alm2crl_sum = summarise(alm2crl_df, ['prodesc'])
    print_table(alm2crl_sum, rw,
                f"COMMERCIAL RETAIL LOANS AS AT {mm_yr}", RPT)

    # =========================================================================
    # SME Datasets from ALM
    # =========================================================================
    def is_sme(row) -> bool:
        custcd = coalesce_s(row.get('custcd'))
        custcx = coalesce_s(row.get('custcx'))
        dnbfi  = coalesce_s(row.get('dnbfisme'))
        return custcd in SME_CUSTCDS or custcx in SME_CUSTCDS or dnbfi in DNBFI_VALS

    def is_dbe(row) -> bool:
        return (coalesce_s(row.get('custcd')) in DBE_CUSTCDS or
                coalesce_s(row.get('custcx')) in DBE_CUSTCDS)

    def is_fbe(row) -> bool:
        return coalesce_s(row.get('custcd')) in FBE_CUSTCDS

    def is_dnbfi(row) -> bool:
        return coalesce_s(row.get('dnbfisme')) in DNBFI_VALS

    # DATA ALMSME: SET ALM; SME filter
    almsme_rows = []
    if not alm_df.is_empty():
        for row in alm_df.to_dicts():
            custcd = coalesce_s(row.get('custcd'))
            dnbfi  = coalesce_s(row.get('dnbfisme'))
            if custcd in SME_CUSTCDS or dnbfi in DNBFI_VALS:
                almsme_rows.append(row)
    almsme_df = pl.from_dicts(almsme_rows) if almsme_rows else pl.DataFrame()

    # DATA SMEFAC: SET PBIF; CUSTCX SME filter
    smefac_rows = []
    if not pbif_df.is_empty():
        for row in pbif_df.to_dicts():
            custcx = coalesce_s(row.get('custcx'))
            if custcx in SME_CUSTCDS:
                smefac_rows.append(row)
    smefac_df = pl.from_dicts(smefac_rows) if smefac_rows else pl.DataFrame()

    # DATA ALMSME DBE FBE DNBFI: SET ALMSME SMEFAC
    almsme_all_src = pl.concat(
        [f for f in [almsme_df, smefac_df] if not f.is_empty()], how='diagonal'
    ) if any(not f.is_empty() for f in [almsme_df, smefac_df]) else pl.DataFrame()

    almsme_out = []; dbe_out = []; fbe_out = []; dnbfi_out = []
    if not almsme_all_src.is_empty():
        for row in almsme_all_src.to_dicts():
            almsme_out.append(row)
            if is_dbe(row):
                dbe_out.append(row)
            elif is_fbe(row):
                fbe_out.append(row)
            elif is_dnbfi(row):
                dnbfi_out.append(row)

    almsme_full = pl.from_dicts(almsme_out) if almsme_out else pl.DataFrame()
    dbe_df      = pl.from_dicts(dbe_out)    if dbe_out    else pl.DataFrame()
    fbe_df      = pl.from_dicts(fbe_out)    if fbe_out    else pl.DataFrame()
    dnbfi_df    = pl.from_dicts(dnbfi_out)  if dnbfi_out  else pl.DataFrame()

    # PROC PRINT: SME LOANS
    almloan_sme = summarise(almsme_full, ['prodesc'])
    print_table(almloan_sme, rw, f"SME LOANS AS AT {mm_yr}", RPT)

    # =========================================================================
    # DATA ALMSME2 DBE2 FBE2 DNBFI2: SET ALM2NEW (retail breakdown)
    # =========================================================================
    almsme2_out = []; dbe2_out = []; fbe2_out = []; dnbfi2_out = []
    if not alm2new_src.is_empty():
        for row in alm2new_src.to_dicts():
            custcd = coalesce_s(row.get('custcd'))
            custcx = coalesce_s(row.get('custcx'))
            dnbfi  = coalesce_s(row.get('dnbfisme'))
            if custcd in DBE_CUSTCDS or custcx in DBE_CUSTCDS:
                dbe2_out.append(row); almsme2_out.append(row)
            elif custcd in FBE_CUSTCDS:
                fbe2_out.append(row); almsme2_out.append(row)
            elif dnbfi in DNBFI_VALS:
                dnbfi2_out.append(row); almsme2_out.append(row)

    almsme2_df = pl.from_dicts(almsme2_out) if almsme2_out else pl.DataFrame()
    dbe2_df    = pl.from_dicts(dbe2_out)    if dbe2_out    else pl.DataFrame()
    fbe2_df    = pl.from_dicts(fbe2_out)    if fbe2_out    else pl.DataFrame()
    dnbfi2_df  = pl.from_dicts(dnbfi2_out)  if dnbfi2_out  else pl.DataFrame()

    # PROC PRINT: RETAIL SME LOANS
    print_table(summarise(almsme2_df, ['prodesc']), rw,
                f"RETAILS SME LOANS AS AT {mm_yr}", RPT)

    # PROC PRINT: SME DBE LOANS
    print_table(summarise(dbe_df, ['prodesc']), rw,
                f"OF WHICH : SME DBE LOANS AS AT {mm_yr}", RPT)

    # PROC PRINT: RETAIL SME DBE LOANS
    print_table(summarise(dbe2_df, ['prodesc']), rw,
                f"OF WHICH : RETAILS SME DBE LOANS AS AT {mm_yr}", RPT)

    # PROC PRINT: SME DNBFI LOANS
    print_table(summarise(dnbfi_df, ['prodesc']), rw,
                f"OF WHICH : SME DNBFI LOANS AS AT {mm_yr}", RPT)

    # PROC PRINT: RETAIL SME DNBFI LOANS
    print_table(summarise(dnbfi2_df, ['prodesc']), rw,
                f"OF WHICH : RETAILS SME DNBFI LOANS AS AT {mm_yr}", RPT)

    # PROC PRINT: SME FE LOANS
    print_table(summarise(fbe_df, ['prodesc']), rw,
                f"OF WHICH : SME FE LOANS AS AT {mm_yr}", RPT)

    # PROC PRINT: RETAIL SME FE LOANS
    print_table(summarise(fbe2_df, ['prodesc']), rw,
                f"OF WHICH : RETAILS SME FE LOANS AS AT {mm_yr}", RPT)

    # =========================================================================
    # BTRADE
    # =========================================================================
    alm_bt_df, almloan_bt_df, mast_bt_df = build_btrade(rv)

    # PROC PRINT: BANK TRADE
    print_table(almloan_bt_df, rw, f"BANK TRADE AS AT {mm_yr}", RPT)

    # SME BANK TRADE
    almsme_bt = []
    mastsme_bt = []
    if not alm_bt_df.is_empty():
        for row in alm_bt_df.to_dicts():
            custcd = coalesce_s(row.get('custcd'))
            dnbfi  = coalesce_s(row.get('dnbfisme'))
            if custcd in SME_CUSTCDS or dnbfi in DNBFI_VALS:
                almsme_bt.append(row)
    if not mast_bt_df.is_empty():
        for row in mast_bt_df.to_dicts():
            custcd = coalesce_s(row.get('custcd'))
            dnbfi  = coalesce_s(row.get('dnbfisme'))
            if custcd in SME_CUSTCDS or dnbfi in DNBFI_VALS:
                mastsme_bt.append(row)

    almsme_bt_df  = pl.from_dicts(almsme_bt)  if almsme_bt  else pl.DataFrame()
    mastsme_bt_df = pl.from_dicts(mastsme_bt) if mastsme_bt else pl.DataFrame()

    bt_grp_key = [c for c in ['prodesc','custcd','dnbfisme']
                  if c in (almsme_bt_df.columns if not almsme_bt_df.is_empty() else [])]

    almsme_bt_sum = pl.DataFrame(); mastsme_bt_sum = pl.DataFrame()
    if not almsme_bt_df.is_empty() and bt_grp_key:
        agg_v2 = [c for c in ['disburse','repaid','balance'] if c in almsme_bt_df.columns]
        if agg_v2:
            almsme_bt_sum = almsme_bt_df.group_by(bt_grp_key).agg(
                [pl.col(c).sum() for c in agg_v2]
            )
    if not mastsme_bt_df.is_empty():
        mbt_grp = [c for c in ['prodesc','custcd','dnbfisme']
                   if c in mastsme_bt_df.columns]
        agg_v3 = [c for c in ['disbno','repayno','noacct'] if c in mastsme_bt_df.columns]
        if mbt_grp and agg_v3:
            mastsme_bt_sum = mastsme_bt_df.group_by(mbt_grp).agg(
                [pl.col(c).sum() for c in agg_v3]
            )

    # Merge ALMSME + MASTSME
    if not almsme_bt_sum.is_empty() and not mastsme_bt_sum.is_empty():
        mk = [c for c in bt_grp_key
              if c in almsme_bt_sum.columns and c in mastsme_bt_sum.columns]
        almloan_sme_bt = almsme_bt_sum.join(mastsme_bt_sum, on=mk, how='outer')
    elif not almsme_bt_sum.is_empty():
        almloan_sme_bt = almsme_bt_sum
    else:
        almloan_sme_bt = mastsme_bt_sum

    dbebt_rows   = []; dnbfibt_rows = []; fbebt_rows = []
    if not almloan_sme_bt.is_empty():
        for row in almloan_sme_bt.to_dicts():
            custcd = coalesce_s(row.get('custcd'))
            dnbfi  = coalesce_s(row.get('dnbfisme'))
            if custcd in DBE_CUSTCDS:
                dbebt_rows.append(row)
            if dnbfi in DNBFI_VALS:
                dnbfibt_rows.append(row)
            if custcd in FBE_CUSTCDS:
                fbebt_rows.append(row)

    dbebt_df   = pl.from_dicts(dbebt_rows)   if dbebt_rows   else pl.DataFrame()
    dnbfibt_df = pl.from_dicts(dnbfibt_rows) if dnbfibt_rows else pl.DataFrame()
    fbebt_df   = pl.from_dicts(fbebt_rows)   if fbebt_rows   else pl.DataFrame()

    # PROC PRINT: SME BANK TRADE
    almloan_sme_bt_sum = summarise(almloan_sme_bt, ['prodesc'])
    print_table(almloan_sme_bt_sum, rw, f"SME BANK TRADE AS AT {mm_yr}", RPT)

    # PROC PRINT: SME DBE/DNBFI/FBE BANK TRADE
    for df_in, t1 in [
        (dbebt_df,   f"OF WHICH : SME DBE BANK TRADE AS AT {rv['reptmon']}/{rv['reptyear']}"),
        (dnbfibt_df, f"OF WHICH : SME DNBFI BANK TRADE AS AT {rv['reptmon']}/{rv['reptyear']}"),
        (fbebt_df,   f"OF WHICH : SME FE BANK TRADE AS AT {rv['reptmon']}/{rv['reptyear']}"),
    ]:
        print_table(summarise(df_in, ['prodesc']), rw, t1, RPT)

    # =========================================================================
    # /******************************************************/
    # /*   FACTORING DATA SECTORS & SUB-SECTORS BREAKDOWN   */
    # /******************************************************/
    if not pbif_df.is_empty():
        pbifsec_rows = pbif_df.to_dicts()
        for row in pbifsec_rows:
            row['sectype']  = format_fisstype(row.get('sectorcd'))
            row['secgroup'] = format_fissgroup(row.get('sectorcd'))
        pbifsec_df = pl.from_dicts(pbifsec_rows)
    else:
        pbifsec_df = pl.DataFrame()

    print_tabulate_sector(
        pbifsec_df, rw,
        'REPORT ID : EIMBNM01',
        f"OUTSTANDING FACTORING LOANS BY SECTORS AND SUB-SECTORS AS AT"
        f" {rv['reptmon']}{rv['ryear']}"
    )

    # =========================================================================
    # /*******************************************************************/
    # /* M&I COMMERCIAL RETAIL LOANS BY SECTORS & SUB-SECTORS BREAKDOWN  */
    # /*******************************************************************/
    comsec_df = pl.DataFrame()
    if not alm2_df.is_empty():
        comsec_rows = []
        for row in alm2_df.filter(
            pl.col('prodesc') == 'TOTAL COMMERCIAL RETAILS'
        ).to_dicts():
            row['sectype']  = format_fisstype(row.get('sectorcd'))
            row['secgroup'] = format_fissgroup(row.get('sectorcd'))
            comsec_rows.append(row)
        comsec_df = pl.from_dicts(comsec_rows) if comsec_rows else pl.DataFrame()

    print_tabulate_sector(
        comsec_df, rw,
        'REPORT ID : EIMBNM01',
        f"OUTSTANDING M&I COMMERCIAL RETAIL LOANS BY SECTORS AND"
        f" SUB-SECTORS AS AT {rv['reptmon']}{rv['ryear']}"
    )

    # =========================================================================
    # /*******************************************************/
    # /*  RETAIL BILLS BY SECTORS AND SUB-SECTORS BREAKDOWN  */
    # /*******************************************************/
    mast1_df = pl.DataFrame()
    almbt_sec_df = pl.DataFrame()
    if not mast_bt_df.is_empty():
        mast1_rows = []
        for row in mast_bt_df.filter(
            pl.col('prodesc') == 'BILLS RETAIL'
        ).to_dicts():
            row['sectype']  = format_fisstype(row.get('sectorcd'))
            row['secgroup'] = format_fissgroup(row.get('sectorcd'))
            mast1_rows.append(row)
        mast1_df = pl.from_dicts(mast1_rows) if mast1_rows else pl.DataFrame()

    if not alm_bt_df.is_empty():
        almbt_rows2 = []
        for row in alm_bt_df.filter(
            pl.col('prodesc') == 'BILLS RETAIL'
        ).to_dicts():
            row['sectype']  = format_fisstype(row.get('sectorcd'))
            row['secgroup'] = format_fissgroup(row.get('sectorcd'))
            almbt_rows2.append(row)
        almbt_sec_df = pl.from_dicts(almbt_rows2) if almbt_rows2 else pl.DataFrame()

    # PROC SUMMARY MAST1 by SECGROUP SECTYPE: NOACCT
    # PROC SUMMARY ALMBT by SECGROUP SECTYPE: BALANCE
    # MERGE MAST1(IN=A) ALMBT(IN=B); IF A
    rebsec_df = pl.DataFrame()
    if not mast1_df.is_empty():
        sg_key = [c for c in ['secgroup','sectype'] if c in mast1_df.columns]
        mast1_sum = mast1_df.group_by(sg_key).agg(
            pl.col('noacct').sum()
        ) if 'noacct' in mast1_df.columns else mast1_df

        if not almbt_sec_df.is_empty():
            sg_key2 = [c for c in ['secgroup','sectype'] if c in almbt_sec_df.columns]
            almbt_sec_sum = almbt_sec_df.group_by(sg_key2).agg(
                pl.col('balance').sum()
            ) if 'balance' in almbt_sec_df.columns else almbt_sec_df
            rebsec_df = mast1_sum.join(almbt_sec_sum, on=sg_key, how='left')
        else:
            rebsec_df = mast1_sum

    print_tabulate_sector(
        rebsec_df, rw,
        'REPORT ID : EIMBNM01',
        f"OUTSTANDING RETAIL BILLS BY SECTORS AND SUB-SECTORS"
        f" AS AT {rv['reptmon']}{rv['ryear']}"
    )

    # =========================================================================
    # /****************************************************************/
    # /*  COMBINE FACTORING, M&I AND BANK TRADE FOR SECTORS BREAKDOWN  */
    # /****************************************************************/
    combysec_df = pl.concat(
        [f for f in [pbifsec_df, rebsec_df, comsec_df] if not f.is_empty()],
        how='diagonal'
    ) if any(not f.is_empty() for f in [pbifsec_df, rebsec_df, comsec_df]) \
    else pl.DataFrame()

    print_tabulate_sector(
        combysec_df, rw,
        'REPORT ID : EIMBNM01',
        f"TOTAL COMMERCIAL RETAIL LOANS BY SECTORS AND SUB-SECTORS"
        f" AS AT {rv['reptmon']}{rv['ryear']}"
    )

    # =========================================================================
    # /************************************************/
    # /*  EXTRACT TOTAL COMMERCIAL RETAIL BY PRODUCT  */
    # /************************************************/
    # DATA COM1: SET PBIFSEC; TYPE='FIXED LOANS'
    com1_df = pl.DataFrame()
    if not pbifsec_df.is_empty():
        com1_rows = pbifsec_df.to_dicts()
        for row in com1_rows:
            row['type'] = 'FIXED LOANS'
        com1_df = pl.from_dicts(com1_rows)

    # DATA COM2: SET REBSEC; TYPE='BANKTRADE'
    com2_df = pl.DataFrame()
    if not rebsec_df.is_empty():
        com2_rows = rebsec_df.to_dicts()
        for row in com2_rows:
            row['type'] = 'BANKTRADE'
        com2_df = pl.from_dicts(com2_rows)

    # DATA COM3: SET COM3; WHERE PRODESC='TOTAL COMMERCIAL RETAILS'; TYPE by TYCODE
    com3_final_rows = []
    if not com3_df.is_empty():
        for row in com3_df.filter(
            pl.col('prodesc') == 'TOTAL COMMERCIAL RETAILS'
        ).to_dicts():
            tycode = int(row.get('tycode') or 0)
            if tycode == 1:
                row['type'] = 'OD'
            elif tycode == 2:
                row['type'] = 'FIXED LOANS'
            elif tycode == 3:
                row['type'] = 'FLOOR STOCKING'
            else:
                row['type'] = ''
            com3_final_rows.append(row)
    com3_final_df = pl.from_dicts(com3_final_rows) if com3_final_rows else pl.DataFrame()

    combyprod_df = pl.concat(
        [f for f in [com1_df, com2_df, com3_final_df] if not f.is_empty()],
        how='diagonal'
    ) if any(not f.is_empty() for f in [com1_df, com2_df, com3_final_df]) \
    else pl.DataFrame()

    print_tabulate_product(
        combyprod_df, rw,
        'REPORT ID : EIMBNM01',
        f"TOTAL COMMERCIAL RETAIL LOANS BY TYPE OF PRODUCT"
        f" AS AT {rv['reptmon']}{rv['ryear']}"
    )

    # =========================================================================
    # Flush report
    # =========================================================================
    rw.flush(REPORT_TXT)
    print(f"  Written: {REPORT_TXT}")
    print("EIMBNM01: Processing complete.")


if __name__ == '__main__':
    main()
