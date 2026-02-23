# !/usr/bin/env python3
"""
Program Name: EIIBNM01
Purpose: Public Islamic Bank Berhad - Monthly Loan Summary Reports
         Generates multiple PROC PRINT reports covering:
         - All Loans (disbursement, repayment, outstanding)
         - Retail Loans breakdown
         - Commercial Retail Loans (individual vs non-individual)
         - SME Loans (DBE, DNBFI, FBE sub-categories)
         - Bank Trade (Bills) summary
         - Total Commercial Retail by Facility and by Sector

ESMR: 2009-0744
ESMR: 2013-813 (JKA)
ESMR: 2014-632 (RST)
ESMR: 2014-2252 (TBC)
ESMR: 2015-1190 (TBC)
ESMR: 2015-1044 (TBC)
ESMR: 2016-630 (NSA)
ESMR: 2016-579 (TBC)
"""

import os
import sys
from datetime import date, timedelta
import calendar
from typing import Optional

import duckdb
import polars as pl

# %INC PGM(PBBLNFMT)
from PBBLNFMT import format_lnprod, format_lndenom  # (placeholder - not directly used here)

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

# Input parquet paths
BNM_LOAN_PREFIX       = "input/bnm/loan"          # loan<MM><WK>.parquet
BNM_LNWOF_PREFIX      = "input/bnm/lnwof"         # lnwof<MM><WK>.parquet
BNM_LNWOD_PREFIX      = "input/bnm/lnwod"         # lnwod<MM><WK>.parquet
ISASD_LOAN_PREFIX     = "input/isasd/loan"         # loan<MM>.parquet
DISPAY_PREFIX         = "input/dispay/idispaymth"  # idispaymth<MM>.parquet
LOAN_LNCOMM_PARQUET   = "input/loan/lncomm.parquet"
BTBNM_IBTRAD_PREFIX   = "input/btbnm/ibtrad"       # ibtrad<MM><WK>.parquet

# Output paths
OUTPUT_DIR            = "output/eiibnm01"
REPORT_TXT            = os.path.join(OUTPUT_DIR, "eiibnm01_report.txt")
MFRS_MAST_BR_PARQUET  = "output/mfrs/mast_br.parquet"
MFRS_ALM_CR_PARQUET   = "output/mfrs/alm_cr.parquet"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs("output/mfrs", exist_ok=True)

# =============================================================================
# PRODUCT/CUSTOMER CODE MACRO CONSTANTS
# =============================================================================

ODCORP = {50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,70,71,33}
ODFISS = {'0311','0312','0313','0314','0315','0316'}
FLCORP = {180,181,182,183,184,185,186,187,188,189,190,191,192,
          193,195,197,199,851,852,853,854,855,856,857,858,859,
          860,900,901,902,903,904,905,906,907,908,909,910,
          914,915,919,920,925,950,951,
          680,681,682,683,684,685,686,687,688,689,690}
HLWOF  = {423,650,651,664}
REWOF  = {425,654,655,656,657,658,659,660,661,662,663,665,666,667,421,671}
STFLN  = {102,103,104,105,106,107,108}

# Customer codes for SME sub-categories
DBE_CUSTCDS   = {'41','42','43','44','46','47','48','49','51','52','53','54'}
FBE_CUSTCDS   = {'87','88','89'}
DNBFI_VALS    = {'1','2','3'}
SME_CUSTCDS   = DBE_CUSTCDS | FBE_CUSTCDS
INDIV_CUSTCDS = {'77','78','95','96'}

# =============================================================================
# ASA CARRIAGE CONTROL & REPORT UTILITIES
# =============================================================================

PAGE_LENGTH = 60  # lines per page (default)

class ReportWriter:
    """Writes ASA carriage-control report lines to a file."""

    def __init__(self, filepath: str):
        self.filepath  = filepath
        self.lines     = []
        self.page_num  = 0
        self.line_cnt  = PAGE_LENGTH + 1  # force header on first write

    def _page_eject(self):
        """ASA '1' = skip to new page."""
        self.page_num += 1
        self.line_cnt  = 0

    def write_titles(self, title1: str, title2: str, title3: str = ''):
        """Write page-top titles with ASA page-eject on first line."""
        self._page_eject()
        self.lines.append('1' + title1.center(132))
        self.lines.append(' ' + title2.center(132))
        if title3:
            self.lines.append(' ' + title3.center(132))
        self.lines.append(' ' + '')
        self.line_cnt += 4

    def write_line(self, text: str = '', asa: str = ' '):
        if self.line_cnt >= PAGE_LENGTH:
            # no auto page break within proc print body; handled by write_titles
            pass
        self.lines.append(asa + text)
        self.line_cnt += 1

    def blank(self):
        self.write_line()

    def flush(self, fh):
        for ln in self.lines:
            fh.write(ln + '\n')
        self.lines = []


# =============================================================================
# FISS FORMAT HELPERS
# (Equivalent to $FISSTYPE. and $FISSGROUP. SAS formats from PBBLNFMT)
# Sector code -> type / group mapping (abbreviated representative set)
# Full mapping would come from PBBLNFMT; these cover the key ranges used here.
# =============================================================================

def format_fisstype(sectorcd) -> str:
    """$FISSTYPE. format — map FISS sector code to type string."""
    if sectorcd is None:
        return ''
    s = str(sectorcd).strip().zfill(4)
    # Representative mappings from FISS sector classification
    prefix = s[:2]
    mapping = {
        '01': 'AGRICULTURE', '02': 'MINING',     '03': 'MANUFACTURING',
        '04': 'ELECTRICITY', '05': 'WATER',       '06': 'CONSTRUCTION',
        '07': 'WHOLESALE',   '08': 'TRANSPORT',   '09': 'FINANCE',
        '10': 'REAL ESTATE', '11': 'EDUCATION',   '12': 'HEALTH',
        '13': 'ARTS',        '14': 'OTHER SVC',   '15': 'GOVERNMENT',
        '16': 'HOUSEHOLD',   '17': 'EXTRA-TERR',  '18': 'UNKNOWN',
    }
    return mapping.get(prefix, 'OTHERS')


def format_fissgroup(sectorcd) -> str:
    """$FISSGROUP. format — map FISS sector code to group string."""
    if sectorcd is None:
        return ''
    s = str(sectorcd).strip().zfill(4)
    prefix2 = s[:2]
    grp_map = {
        '01': 'A', '02': 'B', '03': 'C', '04': 'D', '05': 'D',
        '06': 'E', '07': 'F', '08': 'G', '09': 'H', '10': 'I',
        '11': 'J', '12': 'K', '13': 'L', '14': 'M', '15': 'N',
        '16': 'O', '17': 'P',
    }
    return grp_map.get(prefix2, 'Z')


# =============================================================================
# DATE CALCULATION
# =============================================================================

def get_report_vars() -> dict:
    """
    DATA REPTDATE:
    REPTDATE = INPUT('01'||PUT(MONTH(TODAY()),Z2.)||PUT(YEAR(TODAY()),4.), DDMMYY8.) - 1
    = last day of previous month.
    Derive WK, WK1, WK2, WK3, MM, MM1, MM2, SDD, SDATE, and string macro vars.
    """
    today    = date.today()
    # First day of current month minus 1 = last day of previous month
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

    sdate    = date(reptdate.year, mm, sdd)
    reptmon  = str(mm).zfill(2)
    reptmon1 = str(mm1).zfill(2)
    reptmon2 = str(mm2).zfill(2)
    reptyear = str(reptdate.year)
    reptday  = str(day).zfill(2)
    rdate    = reptdate.strftime('%d/%m/%y')     # DDMMYY8.
    sdate_s  = sdate.strftime('%d/%m/%y')

    return {
        'reptdate':  reptdate,
        'sdate':     sdate,
        'wk':        wk,
        'wk1':       wk1,
        'wk2':       wk2,
        'wk3':       wk3,
        'mm':        mm,
        'mm1':       mm1,
        'mm2':       mm2,
        'sdd':       sdd,
        'reptmon':   reptmon,
        'reptmon1':  reptmon1,
        'reptmon2':  reptmon2,
        'reptyear':  reptyear,
        'reptday':   reptday,
        'rdate':     rdate,
        'sdate_s':   sdate_s,
        'nowk':      wk,
        'nowk1':     wk1,
        'nowk2':     wk2,
        'nowk3':     wk3,
    }

# =============================================================================
# PARQUET LOADERS
# =============================================================================

def load_parquet(path: str, columns: list = None) -> pl.DataFrame:
    if not os.path.exists(path):
        return pl.DataFrame()
    con = duckdb.connect()
    col_str = ', '.join(columns) if columns else '*'
    df = con.execute(f"SELECT {col_str} FROM read_parquet('{path}')").pl()
    con.close()
    return df

def load_parquet_where(path: str, where: str) -> pl.DataFrame:
    if not os.path.exists(path):
        return pl.DataFrame()
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{path}') WHERE {where}").pl()
    con.close()
    return df

# =============================================================================
# BUILD BASE LOAN DATASET
# =============================================================================

def build_loan_dataset(rv: dict) -> pl.DataFrame:
    """
    PROC SORT DATA=ISASD.LOAN<MM>   OUT=DLOAN;
    PROC SORT DATA=BNM.LOAN<MM><WK>  OUT=MLOAN;
    PROC SORT DATA=BNM.LNWOF<MM><WK> OUT=LNWOF;
    PROC SORT DATA=BNM.LNWOD<MM><WK> OUT=LNWOD;
    PROC SORT DATA=BNM.LNWOF<MM2><WK> OUT=PLNWOF;
    PROC SORT DATA=BNM.LNWOD<MM2><WK> OUT=PLNWOD;

    DATA LOANDM: MERGE DLOAN(IN=A) MLOAN(IN=B); IF A AND NOT B;
    DATA LOAN<MM><WK>: MERGE PLNWOF PLNWOD LOANDM BNM.LOAN<MM2><WK> MLOAN LNWOF LNWOD;
    """
    mm   = rv['reptmon'];  mm2 = rv['reptmon2'];  wk = rv['nowk']

    dloan  = load_parquet(f"{ISASD_LOAN_PREFIX}{mm}.parquet")
    mloan  = load_parquet(f"{BNM_LOAN_PREFIX}{mm}{wk}.parquet")
    lnwof  = load_parquet(f"{BNM_LNWOF_PREFIX}{mm}{wk}.parquet")
    lnwod  = load_parquet(f"{BNM_LNWOD_PREFIX}{mm}{wk}.parquet")
    plnwof = load_parquet(f"{BNM_LNWOF_PREFIX}{mm2}{wk}.parquet")
    plnwod = load_parquet(f"{BNM_LNWOD_PREFIX}{mm2}{wk}.parquet")
    loan_prev = load_parquet(f"{BNM_LOAN_PREFIX}{mm2}{wk}.parquet")

    # DATA LOANDM: MERGE DLOAN(IN=A) MLOAN(IN=B); BY ACCTNO NOTENO; IF A AND NOT B
    if not dloan.is_empty() and not mloan.is_empty():
        key_cols = ['acctno','noteno']
        mloan_keys = mloan.select(key_cols).with_columns(pl.lit(True).alias('_in_b'))
        loandm = dloan.join(mloan_keys, on=key_cols, how='left')
        loandm = loandm.filter(pl.col('_in_b').is_null()).drop('_in_b')
    elif not dloan.is_empty():
        loandm = dloan
    else:
        loandm = pl.DataFrame()

    # DATA LOAN<MM><WK>: MERGE PLNWOF PLNWOD LOANDM BNM.LOAN<MM2><WK> MLOAN LNWOF LNWOD
    # Last writer wins in SAS MERGE without IN= conditions; stack all and keep last non-null
    frames = [f for f in [plnwof, plnwod, loandm, loan_prev, mloan, lnwof, lnwod]
              if not f.is_empty()]
    if not frames:
        return pl.DataFrame()

    # Collect all columns
    all_cols = set()
    for f in frames:
        all_cols.update(f.columns)

    # SAS merge by ACCTNO NOTENO — last value wins; implement via successive left-join overwrite
    key = ['acctno','noteno']
    base = frames[0]
    for f in frames[1:]:
        common_non_key = [c for c in f.columns if c not in key]
        merged = base.join(f.select(key + common_non_key), on=key, how='outer', suffix='_r')
        for col in common_non_key:
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
# BUILD DISPAY DATASET
# =============================================================================

def build_dispay(rv: dict, loan_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA DISPAY: SET DISPAY.IDISPAYMTH<MM>;
      DISBURSE=ROUND(DISBURSE,0.01); REPAID=ROUND(REPAID,0.01);
      WHERE DISBURSE>0 OR REPAID>0;
    DATA DISPAY: MERGE LOAN(IN=A) DISPAY(IN=B DROP=PRODCD); BY ACCTNO NOTENO; IF A & B;
    """
    path = f"{DISPAY_PREFIX}{rv['reptmon']}.parquet"
    raw  = load_parquet(path)
    if raw.is_empty():
        return pl.DataFrame()

    dispay = raw.with_columns([
        pl.col('disburse').round(2),
        pl.col('repaid').round(2),
    ]).filter(
        (pl.col('disburse') > 0) | (pl.col('repaid') > 0)
    )

    if 'prodcd' in dispay.columns:
        dispay = dispay.drop('prodcd')

    # MERGE LOAN(IN=A) DISPAY(IN=B); IF A & B
    merged = loan_df.join(dispay, on=['acctno','noteno'], how='inner', suffix='_dp')
    for col in dispay.columns:
        dc = f"{col}_dp"
        if dc in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(dc).is_not_null())
                  .then(pl.col(dc))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(dc)
    return merged

# =============================================================================
# BUILD ALM DATASET (All Loans Master)
# =============================================================================

def build_alm(loan_df: pl.DataFrame, rv: dict) -> pl.DataFrame:
    """
    PROC SORT DATA=LOAN.LNCOMM OUT=LNCOMM(KEEP=ACCTNO COMMNO CUSEDAMT); BY ACCTNO COMMNO;
    PROC SORT DATA=BNM.LOAN<MM><WK> OUT=LOAN; BY ACCTNO COMMNO;
    DATA ALM ALMBT: MERGE LOAN(IN=A RENAME=(BALANCE=ORIBAL BAL_AFT_EIR=BALANCE)) LNCOMM;
      BY ACCTNO COMMNO; IF A; ... complex filtering and NOACCT logic
    """
    mm  = rv['reptmon'];  wk  = rv['nowk']

    lncomm = load_parquet(LOAN_LNCOMM_PARQUET, ['acctno','commno','cusedamt'])

    loan_raw = load_parquet(f"{BNM_LOAN_PREFIX}{mm}{wk}.parquet")
    if loan_raw.is_empty():
        return pl.DataFrame(), pl.DataFrame()

    # RENAME: BALANCE->ORIBAL, BAL_AFT_EIR->BALANCE
    renames = {}
    if 'balance' in loan_raw.columns:
        renames['balance'] = 'oribal'
    if 'bal_aft_eir' in loan_raw.columns:
        renames['bal_aft_eir'] = 'balance'
    if renames:
        loan_raw = loan_raw.rename(renames)

    # Merge with LNCOMM by ACCTNO COMMNO
    if not lncomm.is_empty() and 'commno' in loan_raw.columns:
        merged = loan_raw.join(lncomm, on=['acctno','commno'], how='left', suffix='_lc')
        for col in lncomm.columns:
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

    # Apply filters row-wise
    keep_cols = ['acctno','noteno','fisspurp','product','noteterm','earnterm',
                 'balance','paidind','apprdate','apprlim2','prodcd','custcd',
                 'amtind','sectorcd','acctype','branch','oribal','dnbfisme',
                 'noacct','commno','cusedamt','rleasamt','cjfee','retailid',
                 'eir_adj']
    avail = [c for c in keep_cols if c in merged.columns]
    merged = merged.select(avail)

    rows     = merged.to_dicts()
    alm_rows = []
    almbt_rows = []

    for row in rows:
        paidind  = str(row.get('paidind') or '').strip()
        eir_adj  = row.get('eir_adj')
        oribal   = row.get('oribal') or 0.0
        balance  = row.get('balance') or 0.0
        prodcd   = str(row.get('prodcd') or '').strip()
        acctype  = str(row.get('acctype') or '').strip()
        acctno   = row.get('acctno') or 0
        noteno   = row.get('noteno') or 0
        product  = row.get('product') or 0
        commno   = row.get('commno') or 0
        cusedamt = row.get('cusedamt') or 0.0
        rleasamt = row.get('rleasamt') or 0.0
        cjfee    = row.get('cjfee') or 0.0

        # IF PAIDIND NOT IN ('P','C') OR EIR_ADJ NE .
        eir_adj_not_null = (eir_adj is not None and
                            not (isinstance(eir_adj, float) and eir_adj != eir_adj))
        if paidind in ('P', 'C') and not eir_adj_not_null:
            continue

        # *  IF ACCTYPE='OD' AND PRODUCT IN (150,151,152,181) THEN DELETE;
        # IF ORIBAL=-.00 THEN DELETE
        if oribal == -0.0 and str(oribal) in ('-0.0', '-0.00'):
            continue

        balx = round(oribal, 2)
        xind = ' '
        if balx in (0.0, -0.0):
            xind = 'Y'

        # *  IF PAIDIND IN ('P','C') OR XIND='Y' THEN DELETE;
        # IF XIND='Y' THEN DELETE
        if xind == 'Y':
            continue

        # IF SUBSTR(PRODCD,1,2) = '34' OR PRODCD = '54120'
        if not (prodcd[:2] == '34' or prodcd == '54120'):
            continue

        # NOACCT logic for LN
        noacct = row.get('noacct') or 0
        if acctype == 'LN':
            eligible = False
            if rleasamt != 0.0 and paidind not in ('P','C') and oribal > 0 and cjfee != oribal:
                eligible = True
            elif rleasamt == 0.0 and paidind not in ('P','C') and oribal > 0 and \
                 600 <= product <= 699:
                eligible = True
            elif rleasamt == 0.0 and paidind not in ('P','C') and oribal > 0 and \
                 commno > 0 and cusedamt > 0:
                eligible = True
            if not eligible:
                noacct = 0

        # IF PAIDIND NOT IN ('P','C') AND NOACCT^=0 AND ROUND(ORIBAL,.01) NOT IN (0,0) AND ORIBAL NE ' '
        if paidind not in ('P','C') and noacct != 0 and \
           round(oribal, 2) not in (0.0, -0.0) and oribal is not None:
            noacct = 1

        row['noacct'] = noacct

        # Split ALMBT vs ALM
        if (2850000000 <= acctno <= 2859999999 and 40000 <= noteno <= 49999) or product == 444:
            almbt_rows.append(row)
        else:
            alm_rows.append(row)

    alm_df   = pl.from_dicts(alm_rows)   if alm_rows   else pl.DataFrame()
    almbt_df = pl.from_dicts(almbt_rows) if almbt_rows else pl.DataFrame()

    # DATA ALM: BY ACCTNO COMMNO — deduplicate NOACCT for revolving products
    if not alm_df.is_empty() and 'commno' in alm_df.columns:
        alm_df = alm_df.sort(['acctno','commno'])
        rows2  = alm_df.to_dicts()
        prev_acctno = None; prev_commno = None; unq = 0
        for row in rows2:
            ac = row['acctno']; cm = row.get('commno')
            prodcd = str(row.get('prodcd') or '')
            if ac != prev_acctno or cm != prev_commno:
                unq = 0
            if prodcd in ('34170','34190','34690'):
                unq += (row.get('noacct') or 0)  # *17-513
                if unq > 1:
                    row['noacct'] = 0
            prev_acctno = ac; prev_commno = cm
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
    alm_df = pl.concat([alm_df, almbt_df], how='diagonal') if not almbt_df.is_empty() else alm_df

    return alm_df

# =============================================================================
# APPLY PRODESC, SECTTYPE, SECTGROUP
# =============================================================================

def apply_prodesc(df: pl.DataFrame) -> pl.DataFrame:
    """Apply PRODESC, SECTTYPE, SECTGROUP to ALM dataset."""
    if df.is_empty():
        return df

    rows = df.to_dicts()
    for row in rows:
        product = row.get('product') or 0
        acctype = str(row.get('acctype') or '').strip()
        prodcd  = str(row.get('prodcd')  or '').strip()
        scd     = row.get('sectorcd')

        p = product
        pd = prodesc = ''

        if p in {135,136,138,419,420,422,424,426,464,465,468,469,470,
                 441,443,475,477,482,483,490,491,492,493,496,497,498,
                 652,653,668,669,672,673,674,675,693}:
            prodesc = 'PERSONAL LOANS'
        elif (acctype == 'LN' and prodcd == '34111') or p in {698,699,983}:
            prodesc = 'HIRE PURCHASE'
        elif acctype == 'LN' and prodcd == '54120':
            prodesc = 'HOUSE FINANCING SOLD TO CAGAMAS'
        elif (acctype == 'LN' and prodcd == '34120') or p in HLWOF:
            prodesc = 'HOUSING LOANS'
        elif acctype == 'OD' and prodcd in ('34180','34240') and p in ODCORP:
            prodesc = 'OD CORPORATE'
        elif acctype == 'OD' and prodcd in ('34180','34240') and p not in ODCORP:
            prodesc = 'OD RETAIL'
        elif acctype == 'LN' and prodcd not in ('34111','34120','N','M') and p in FLCORP:
            prodesc = 'OTHERS CORPORATE'
        elif (acctype == 'LN' and prodcd not in ('34111','34120','N','M') and
              p not in FLCORP) or p in REWOF:
            prodesc = 'OTHERS RETAIL'

        row['prodesc']   = prodesc
        row['secttype']  = format_fisstype(scd)
        row['sectgroup'] = format_fissgroup(scd)

    return pl.from_dicts(rows)

# =============================================================================
# BUILD BTRADE DATASET
# =============================================================================

def build_btrade(rv: dict) -> tuple:
    """
    PROC SORT DATA=BTBNM.IBTRAD<MM><WK> OUT=BTRAD;
      WHERE DIRCTIND='D' AND CUSTCD NE ' '; BY ACCTNO DESCENDING APPRLIMT;
    PROC SUMMARY (DISBURSE REPAID by ACCTNO CUSTCD RETAILID SECTORCD DNBFISME) -> BTRAD1
    PROC SUMMARY (BALANCE where APPRLIMT>0 by ACCTNO CUSTCD RETAILID SECTORCD) -> BTRAD
    DATA OVC MAST: MERGE BTRAD BTRAD1; ...
    PROC SORT ALMBT (BTRADE detail); ...
    Returns: (almbt_df, almbtrd_df, mast_df)
    """
    mm = rv['reptmon'];  wk = rv['nowk']
    path = f"{BTBNM_IBTRAD_PREFIX}{mm}{wk}.parquet"

    raw = load_parquet_where(path, "dirctind = 'D' AND custcd IS NOT NULL AND custcd <> ' '")
    if raw.is_empty():
        empty = pl.DataFrame()
        return empty, empty, empty

    # Sort by ACCTNO DESC APPRLIMT (for nodupkey-like behavior later)
    if 'apprlimt' in raw.columns:
        btrad = raw.sort(['acctno', 'apprlimt'], descending=[False, True])
    else:
        btrad = raw.sort('acctno')

    # Re-sort by ACCTNO CUSTCD RETAILID
    grp_cols1 = [c for c in ['acctno','custcd','retailid','sectorcd','dnbfisme'] if c in btrad.columns]
    btrad     = btrad.sort(grp_cols1)

    # PROC SUMMARY: DISBURSE REPAID grouped by ACCTNO CUSTCD RETAILID SECTORCD DNBFISME
    agg_cols1  = [c for c in ['disburse','repaid'] if c in btrad.columns]
    grp_key1   = [c for c in ['acctno','custcd','retailid','sectorcd','dnbfisme'] if c in btrad.columns]
    if agg_cols1 and grp_key1:
        btrad1 = btrad.group_by(grp_key1).agg(
            [pl.col(c).sum().alias(c) for c in agg_cols1]
        )
    else:
        btrad1 = pl.DataFrame()

    # PROC SUMMARY: BALANCE where APPRLIMT>0 grouped by ACCTNO CUSTCD RETAILID SECTORCD
    grp_key2 = [c for c in ['acctno','custcd','retailid','sectorcd'] if c in btrad.columns]
    if 'apprlimt' in btrad.columns and 'balance' in btrad.columns and grp_key2:
        btrad_bal = btrad.filter(pl.col('apprlimt') > 0).group_by(grp_key2).agg(
            pl.col('balance').sum()
        )
    else:
        btrad_bal = pl.DataFrame()

    # DATA OVC MAST: MERGE BTRAD(IN=A) BTRAD1(IN=B); BY ACCTNO CUSTCD RETAILID SECTORCD; IF B
    merge_key = [c for c in ['acctno','custcd','retailid','sectorcd'] if c in btrad1.columns]
    if not btrad_bal.is_empty() and not btrad1.is_empty():
        mast_merged = btrad1.join(btrad_bal, on=merge_key, how='left', suffix='_bal')
        if 'balance_bal' in mast_merged.columns:
            mast_merged = mast_merged.with_columns(
                pl.when(pl.col('balance_bal').is_not_null())
                  .then(pl.col('balance_bal'))
                  .otherwise(pl.col('balance') if 'balance' in mast_merged.columns else pl.lit(None))
                  .alias('balance')
            ).drop('balance_bal')
    else:
        mast_merged = btrad1

    mast_merged = mast_merged.with_columns([
        pl.when(pl.col('disburse') > 0).then(pl.lit(1)).otherwise(pl.lit(0)).alias('disbno'),
        pl.when(pl.col('repaid')   > 0).then(pl.lit(1)).otherwise(pl.lit(0)).alias('repayno'),
    ])
    # IF A AND ROUND(BALANCE,0.01) NOT IN (.,0) AND NOACCT^=0 THEN NOACCT=1
    if 'balance' in mast_merged.columns:
        mast_merged = mast_merged.with_columns(
            pl.when(
                pl.col('balance').round(2).is_not_null() &
                (pl.col('balance').round(2) != 0)
            ).then(pl.lit(1)).otherwise(pl.lit(0)).alias('noacct')
        )

    # OVC: KEEP ACCTNO RETAILID
    ovc_df = mast_merged.select([c for c in ['acctno','retailid'] if c in mast_merged.columns])

    # MAST: KEEP ACCTNO CUSTCD BALANCE RETAILID DISBNO REPAYNO NOACCT SECTORCD DNBFISME
    mast_keep = [c for c in ['acctno','custcd','balance','retailid','disbno','repayno',
                               'noacct','sectorcd','dnbfisme'] if c in mast_merged.columns]
    mast_df   = mast_merged.select(mast_keep)

    # PROC SORT ALMBT from BTBNM with WHERE SUBSTR(PRODCD,1,2)='34'
    almbt_raw = load_parquet_where(path, "SUBSTR(CAST(prodcd AS VARCHAR), 1, 2) = '34'")
    if almbt_raw.is_empty():
        return pl.DataFrame(), pl.DataFrame(), mast_df

    almbt_keep = [c for c in ['acctno','subacct','fisspurp','product','noteterm','balance',
                               'apprlim2','prodcd','custcd','amtind','transref','sectorcd',
                               'disburse','repaid','dnbfisme'] if c in almbt_raw.columns]
    almbt_df = almbt_raw.select(almbt_keep)

    # DATA ALMBT: MERGE OVC ALMBT(IN=A); BY ACCTNO; IF A
    if not ovc_df.is_empty():
        almbt_df = almbt_df.join(ovc_df, on='acctno', how='inner', suffix='_ovc')
        if 'retailid_ovc' in almbt_df.columns:
            almbt_df = almbt_df.with_columns(
                pl.when(pl.col('retailid_ovc').is_not_null())
                  .then(pl.col('retailid_ovc'))
                  .otherwise(pl.col('retailid') if 'retailid' in almbt_df.columns else pl.lit(None))
                  .alias('retailid')
            ).drop('retailid_ovc')

    # PROC SUMMARY ALMBT -> ALMBTX (sum BALANCE by key)
    bt_grp = [c for c in ['acctno','transref','custcd','fisspurp','sectorcd'] if c in almbt_df.columns]
    if bt_grp and 'balance' in almbt_df.columns:
        almbtx = almbt_df.group_by(bt_grp).agg(pl.col('balance').sum().alias('balance'))
        almbt_df = almbt_df.sort(bt_grp).unique(subset=bt_grp, keep='first')
        almbt_df = almbt_df.drop('balance').join(almbtx, on=bt_grp, how='left')

    # ALMBT summary
    almbt_df = almbt_df.with_columns([
        pl.col('disburse').cast(pl.Float64) if 'disburse' in almbt_df.columns else pl.lit(0.0).alias('disburse'),
        pl.col('repaid').cast(pl.Float64)   if 'repaid'   in almbt_df.columns else pl.lit(0.0).alias('repaid'),
        pl.col('balance').cast(pl.Float64)  if 'balance'  in almbt_df.columns else pl.lit(0.0).alias('balance'),
    ])

    # DATA ALMBT: apply SECTTYPE/SECTGROUP/PRODESC
    rows = almbt_df.to_dicts()
    for row in rows:
        scd      = row.get('sectorcd')
        retailid = str(row.get('retailid') or '').strip()
        row['secttype']  = format_fisstype(scd)
        row['sectgroup'] = format_fissgroup(scd)
        row['prodesc']   = 'BILLS CORPORATE' if retailid == 'C' else 'BILLS RETAIL'
    almbt_df = pl.from_dicts(rows)

    # DATA MAST: apply SECTTYPE/SECTGROUP/PRODESC + output MFRS.MAST_BR
    mast_rows = mast_df.to_dicts()
    mast_out  = []
    mast_br_rows = []
    for row in mast_rows:
        scd      = row.get('sectorcd')
        retailid = str(row.get('retailid') or '').strip()
        acctno1  = row.get('acctno')
        row['secttype']  = format_fisstype(scd)
        row['sectgroup'] = format_fissgroup(scd)
        row['prodesc']   = 'BILLS CORPORATE' if retailid == 'C' else 'BILLS RETAIL'
        try:
            row['acctno'] = int(str(acctno1))
        except (ValueError, TypeError):
            row['acctno'] = 0
        mast_out.append(row)
        mast_br_rows.append({'acctno': row['acctno'],
                              'prodesc': row['prodesc'],
                              'noacct': row.get('noacct', 0)})

    mast_df = pl.from_dicts(mast_out)

    # Save MFRS.MAST_BR
    if mast_br_rows:
        pl.from_dicts(mast_br_rows).write_parquet(MFRS_MAST_BR_PARQUET)

    # PROC SUMMARY: ALMBT grouped by PRODESC
    almbtrd_df = pl.DataFrame()
    if not almbt_df.is_empty():
        agg_v  = [c for c in ['disburse','repaid','balance'] if c in almbt_df.columns]
        almbtrd = almbt_df.group_by('prodesc').agg(
            [pl.col(c).sum() for c in agg_v]
        )
        # Merge MASTLOAN counts
        mast_agg_v = [c for c in ['disbno','repayno','noacct'] if c in mast_df.columns]
        if mast_agg_v:
            mastloan = mast_df.group_by('prodesc').agg(
                [pl.col(c).sum() for c in mast_agg_v]
            )
            almbtrd = almbtrd.join(mastloan, on='prodesc', how='left')
        almbtrd_df = almbtrd

    return almbt_df, almbtrd_df, mast_df

# =============================================================================
# PROC PRINT EQUIVALENT — formatted report table
# =============================================================================

NUM_COLS = ['disburse','repaid','balance','disbno','repayno','noacct']

def fmt_num(val, decimals: int = 2) -> str:
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * (16 if decimals == 2 else 10)
    if decimals == 2:
        return f"{float(val):16.2f}"
    else:
        return f"{float(val):10.0f}"

def print_table(df: pl.DataFrame, rw: ReportWriter,
                title1: str, title2: str, title3: str,
                where_expr=None):
    """Equivalent to PROC PRINT with SUM statement."""
    if df.is_empty():
        return

    if where_expr is not None:
        df = df.filter(where_expr)
    if df.is_empty():
        return

    rw.write_titles(title1, title2, title3)

    # Header row
    hdr = (f"{'PRODESC':<35}"
           f"{'DISBURSE':>16}"
           f"{'REPAID':>16}"
           f"{'BALANCE':>16}"
           f"{'DISBNO':>10}"
           f"{'REPAYNO':>10}"
           f"{'NOACCT':>10}")
    rw.write_line(' ' + hdr)
    rw.write_line(' ' + '-' * len(hdr))

    # Totals accumulators
    tot = {c: 0.0 for c in NUM_COLS}

    for row in df.sort('prodesc').iter_rows(named=True):
        prodesc = str(row.get('prodesc') or '').strip()[:35]
        line = (f"{prodesc:<35}"
                f"{fmt_num(row.get('disburse'))}"
                f"{fmt_num(row.get('repaid'))}"
                f"{fmt_num(row.get('balance'))}"
                f"{fmt_num(row.get('disbno'), 0)}"
                f"{fmt_num(row.get('repayno'), 0)}"
                f"{fmt_num(row.get('noacct'), 0)}")
        rw.write_line(' ' + line)
        for c in NUM_COLS:
            v = row.get(c)
            if v is not None and not (isinstance(v, float) and v != v):
                tot[c] += float(v)

    # SUM line
    rw.write_line(' ' + '=' * len(hdr))
    sum_line = (f"{'SUM':<35}"
                f"{fmt_num(tot['disburse'])}"
                f"{fmt_num(tot['repaid'])}"
                f"{fmt_num(tot['balance'])}"
                f"{fmt_num(tot['disbno'], 0)}"
                f"{fmt_num(tot['repayno'], 0)}"
                f"{fmt_num(tot['noacct'], 0)}")
    rw.write_line(' ' + sum_line)
    rw.blank()


def print_tabulate_facility(df: pl.DataFrame, rw: ReportWriter,
                             title1: str, title2: str):
    """
    PROC TABULATE: TABLE TYPE ALL, SUM*(BALANCE NOACCT) / BOX='FACILITY'
    """
    if df.is_empty():
        return
    fdf = df.filter(
        (pl.col('prodesc') == 'TOTAL COMMERCIAL RETAILS') &
        pl.col('balance').is_not_null() & (pl.col('balance') != 0)
    )
    if fdf.is_empty():
        return

    rw.write_titles(title1, title2)

    grp_df = fdf.group_by('type').agg([
        pl.col('balance').sum().alias('balance'),
        pl.col('noacct').sum().alias('noacct'),
    ]).sort('type')

    hdr = f"{'FACILITY':<25}{'AMOUNT':>18}{'NO. OF ACCT':>12}"
    rw.write_line(' ' + hdr)
    rw.write_line(' ' + '-' * len(hdr))

    tot_bal = 0.0; tot_noacct = 0.0
    for row in grp_df.iter_rows(named=True):
        typ  = str(row.get('type') or '').strip()[:25]
        bal  = float(row.get('balance') or 0.0)
        noa  = float(row.get('noacct')  or 0.0)
        rw.write_line(' ' + f"{typ:<25}{bal:18.2f}{noa:12.0f}")
        tot_bal += bal; tot_noacct += noa

    rw.write_line(' ' + '=' * len(hdr))
    rw.write_line(' ' + f"{'GRAND TOTAL':<25}{tot_bal:18.2f}{tot_noacct:12.0f}")
    rw.blank()


def print_tabulate_sector(df: pl.DataFrame, rw: ReportWriter,
                           title1: str, title2: str):
    """
    PROC TABULATE: TABLE SECTGROUP*(SECTTYPE ALL) ALL, SUM*(BALANCE NOACCT) / BOX='SECTFISS'
    """
    if df.is_empty():
        return
    fdf = df.filter(
        (pl.col('prodesc') == 'TOTAL COMMERCIAL RETAILS') &
        pl.col('balance').is_not_null() & (pl.col('balance') != 0)
    )
    if fdf.is_empty():
        return

    rw.write_titles(title1, title2)

    hdr = f"{'SECTFISS':<25}{'AMOUNT':>18}{'NO. OF ACCT':>12}"
    rw.write_line(' ' + hdr)
    rw.write_line(' ' + '-' * len(hdr))

    grp = fdf.group_by(['sectgroup','secttype']).agg([
        pl.col('balance').sum(),
        pl.col('noacct').sum(),
    ]).sort(['sectgroup','secttype'])

    grand_bal = 0.0; grand_noa = 0.0
    for sg, sub_df in grp.group_by('sectgroup', maintain_order=True):
        sub_bal = 0.0; sub_noa = 0.0
        for row in sub_df.sort('secttype').iter_rows(named=True):
            st  = str(row.get('secttype') or '').strip()[:25]
            bal = float(row.get('balance') or 0.0)
            noa = float(row.get('noacct')  or 0.0)
            rw.write_line(' ' + f"  {st:<23}{bal:18.2f}{noa:12.0f}")
            sub_bal += bal; sub_noa += noa
        rw.write_line(' ' + f"{'  SUB-TOTAL':<25}{sub_bal:18.2f}{sub_noa:12.0f}")
        grand_bal += sub_bal; grand_noa += sub_noa

    rw.write_line(' ' + '=' * len(hdr))
    rw.write_line(' ' + f"{'GRAND TOTAL':<25}{grand_bal:18.2f}{grand_noa:12.0f}")
    rw.blank()

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("EIIBNM01: Starting Public Islamic Bank Berhad loan summary reports...")

    rv = get_report_vars()
    print(f"  Report date: {rv['reptdate']} MM={rv['reptmon']} YY={rv['reptyear']} WK={rv['nowk']}")

    rw = ReportWriter(REPORT_TXT)

    BANK   = 'PUBLIC ISLAMIC BANK BERHAD'
    RPT_ID = 'REPORT ID : EIIBNM01'
    mm_yy  = f"{rv['reptmon']}/{rv['reptyear']}"
    rdate  = rv['rdate']

    # -------------------------------------------------------------------------
    # Build base loan dataset
    # -------------------------------------------------------------------------
    loan_df = build_loan_dataset(rv)

    # -------------------------------------------------------------------------
    # Build DISPAY
    # -------------------------------------------------------------------------
    dispay_df = build_dispay(rv, loan_df)

    # -------------------------------------------------------------------------
    # Build ALM (all loans master)
    # -------------------------------------------------------------------------
    alm_df = build_alm(loan_df, rv)

    # Merge DISPAY into ALM (sorted by ACCTNO NOTENO)
    dispay_keep = [c for c in ['acctno','noteno','fisspurp','product','dnbfisme',
                                'prodcd','custcd','amtind','sectorcd','disburse',
                                'repaid','branch','acctype'] if c in dispay_df.columns]
    if not dispay_df.is_empty() and not alm_df.is_empty():
        dispay_filt = dispay_df.select(dispay_keep)
        if 'prodcd' in dispay_filt.columns:
            dispay_filt = dispay_filt.filter(
                pl.col('prodcd').str.slice(0,2) == '34' |
                (pl.col('prodcd') == '54120') |
                pl.col('product').is_in([698,699,983])
            )
        alm_merged = alm_df.join(
            dispay_filt.select(['acctno','noteno','disburse','repaid']),
            on=['acctno','noteno'], how='left', suffix='_dp'
        )
        for c in ['disburse','repaid']:
            dc = f"{c}_dp"
            if dc in alm_merged.columns:
                alm_merged = alm_merged.with_columns(
                    pl.when(pl.col(dc).is_not_null())
                      .then(pl.col(dc))
                      .otherwise(pl.col(c) if c in alm_merged.columns else pl.lit(None))
                      .alias(c)
                ).drop(dc)
        alm_df = alm_merged

    alm_df = alm_df.with_columns([
        pl.when(pl.col('repaid')   > 0).then(pl.lit(1)).otherwise(pl.lit(0)).alias('repayno'),
        pl.when(pl.col('disburse') > 0).then(pl.lit(1)).otherwise(pl.lit(0)).alias('disbno'),
    ]) if not alm_df.is_empty() else alm_df

    # Apply PRODESC
    alm_df = apply_prodesc(alm_df)

    # -------------------------------------------------------------------------
    # PROC SUMMARY ALM -> ALMLOAN (by PRODESC)
    # -------------------------------------------------------------------------
    agg_vars = [c for c in NUM_COLS if c in alm_df.columns]
    if not alm_df.is_empty() and agg_vars:
        almloan_df = alm_df.group_by('prodesc').agg(
            [pl.col(c).sum() for c in agg_vars]
        )
    else:
        almloan_df = pl.DataFrame()

    # -------------------------------------------------------------------------
    # Build BT (Bank Trade) datasets
    # -------------------------------------------------------------------------
    almbt_df, almbtrd_df, mast_df = build_btrade(rv)

    # -------------------------------------------------------------------------
    # Split ALMLOAN into non-HFSC and ALMHFSC
    # -------------------------------------------------------------------------
    if not almloan_df.is_empty():
        almhfsc_df = almloan_df.filter(pl.col('prodesc') == 'HOUSE FINANCING SOLD TO CAGAMAS')
        almloan_main = almloan_df.filter(pl.col('prodesc') != 'HOUSE FINANCING SOLD TO CAGAMAS')
    else:
        almhfsc_df   = pl.DataFrame()
        almloan_main = pl.DataFrame()

    # -------------------------------------------------------------------------
    # PROC PRINT: ALL LOANS
    # -------------------------------------------------------------------------
    print_table(almloan_main, rw, BANK, f"ALL LOANS AS AT {mm_yy}", RPT_ID)
    print_table(almhfsc_df,   rw, BANK, f"ALL LOANS AS AT {mm_yy}", RPT_ID)

    # -------------------------------------------------------------------------
    # ALM2: Retail OD/Others breakdown
    # -------------------------------------------------------------------------
    if not alm_df.is_empty():
        alm2_df = alm_df.filter(pl.col('prodesc').is_in(['OD RETAIL','OTHERS RETAIL']))
        rows2   = alm2_df.to_dicts()
        for row in rows2:
            pd_ = str(row.get('prodesc') or '')
            if pd_ == 'OD RETAIL':
                fiss = str(row.get('fisspurp') or '').strip()
                row['prodesc'] = 'PURCHASE OF RESIDENTIAL PROPERTY' if fiss in ODFISS \
                                 else 'TOTAL COMMERCIAL RETAILS'
                row['type'] = 'CASH LINE FACILITY'
            elif pd_ == 'OTHERS RETAIL':
                prod = row.get('product') or 0
                row['prodesc'] = 'STAFF FINANCING' if prod in STFLN \
                                 else 'TOTAL COMMERCIAL RETAILS'
                row['type'] = 'FIXED FINANCING'
        alm2_df = pl.from_dicts(rows2)
    else:
        alm2_df = pl.DataFrame()

    # -------------------------------------------------------------------------
    # ALMBTCR: Bills Retail -> Total Commercial Retails
    # -------------------------------------------------------------------------
    if not almbtrd_df.is_empty():
        almbtcr_df = almbtrd_df.filter(pl.col('prodesc') == 'BILLS RETAIL').with_columns([
            pl.lit('TOTAL COMMERCIAL RETAILS').alias('prodesc'),
            pl.lit('BANK TRADE').alias('type'),
        ])
    else:
        almbtcr_df = pl.DataFrame()

    # -------------------------------------------------------------------------
    # DATA ALMLOAN2 ALM2CRF MFRS.ALM_CR: SET ALM2 ALMBTCR
    # -------------------------------------------------------------------------
    almloan2_src = pl.concat(
        [f for f in [alm2_df, almbtcr_df] if not f.is_empty()], how='diagonal'
    ) if any(not f.is_empty() for f in [alm2_df, almbtcr_df]) else pl.DataFrame()

    # Save MFRS.ALM_CR (KEEP=ACCTNO NOTENO PRODESC NOACCT)
    if not almloan2_src.is_empty():
        alm_cr_keep = [c for c in ['acctno','noteno','prodesc','noacct']
                       if c in almloan2_src.columns]
        almloan2_src.select(alm_cr_keep).write_parquet(MFRS_ALM_CR_PARQUET)

    # ALM2CRF: TOTAL COMMERCIAL RETAILS -> split by CUSTCD
    alm2crf_rows = []
    if not almloan2_src.is_empty():
        for row in almloan2_src.to_dicts():
            if str(row.get('prodesc') or '') == 'TOTAL COMMERCIAL RETAILS':
                custcd = str(row.get('custcd') or '').strip()
                row2   = dict(row)
                if custcd in INDIV_CUSTCDS:
                    row2['prodesc'] = 'COMMERCIAL RETAIL - IND'
                else:
                    row2['prodesc'] = 'COMMERCIAL RETAIL - NON IND'
                alm2crf_rows.append(row2)
    alm2crf_df = pl.from_dicts(alm2crf_rows) if alm2crf_rows else pl.DataFrame()

    # PROC SUMMARY ALMLOAN2
    if not almloan2_src.is_empty():
        agg_v2 = [c for c in NUM_COLS if c in almloan2_src.columns]
        almloan2_df = almloan2_src.group_by('prodesc').agg([pl.col(c).sum() for c in agg_v2])
    else:
        almloan2_df = pl.DataFrame()

    # PROC PRINT: RETAIL LOANS
    print_table(almloan2_df, rw, BANK, f"RETAILS LOANS AS AT {mm_yy}", RPT_ID)

    # PROC SUMMARY ALM2CRF
    if not alm2crf_df.is_empty():
        agg_v3 = [c for c in NUM_COLS if c in alm2crf_df.columns]
        alm2crf_sum = alm2crf_df.group_by('prodesc').agg([pl.col(c).sum() for c in agg_v3])
    else:
        alm2crf_sum = pl.DataFrame()

    # PROC PRINT: COMMERCIAL RETAIL LOANS
    print_table(alm2crf_sum, rw, BANK, f"COMMERCIAL RETAIL LOANS AS AT {mm_yy}", RPT_ID)

    # -------------------------------------------------------------------------
    # SME datasets
    # -------------------------------------------------------------------------
    def is_sme(row) -> bool:
        custcd = str(row.get('custcd') or '').strip()
        dnbfi  = str(row.get('dnbfisme') or '').strip()
        return custcd in SME_CUSTCDS or dnbfi in DNBFI_VALS

    def is_dbe(row) -> bool:
        return str(row.get('custcd') or '').strip() in DBE_CUSTCDS

    def is_dnbfi(row) -> bool:
        return str(row.get('dnbfisme') or '').strip() in DNBFI_VALS

    def is_fbe(row) -> bool:
        return str(row.get('custcd') or '').strip() in FBE_CUSTCDS

    almsme_df = pl.DataFrame(); dbe_df   = pl.DataFrame()
    dnbfi_df  = pl.DataFrame(); fbe_df   = pl.DataFrame()
    if not alm_df.is_empty():
        rows_all = alm_df.to_dicts()
        almsme_r = [r for r in rows_all if is_sme(r)]
        dbe_r    = [r for r in rows_all if is_dbe(r)]
        dnbfi_r  = [r for r in rows_all if is_dnbfi(r)]
        fbe_r    = [r for r in rows_all if is_fbe(r)]
        almsme_df = pl.from_dicts(almsme_r) if almsme_r else pl.DataFrame()
        dbe_df    = pl.from_dicts(dbe_r)    if dbe_r    else pl.DataFrame()
        dnbfi_df  = pl.from_dicts(dnbfi_r)  if dnbfi_r  else pl.DataFrame()
        fbe_df    = pl.from_dicts(fbe_r)    if fbe_r    else pl.DataFrame()

    # PROC SUMMARY ALMSME -> ALMLOAN (by PRODESC)
    if not almsme_df.is_empty():
        agg_v = [c for c in NUM_COLS if c in almsme_df.columns]
        almloan_sme = almsme_df.group_by('prodesc').agg([pl.col(c).sum() for c in agg_v])
    else:
        almloan_sme = pl.DataFrame()

    # ALMSMEBT: SME filter on ALMBT
    almsmebt_df = pl.DataFrame()
    mastsme_df  = pl.DataFrame()
    if not almbt_df.is_empty():
        rows_bt = almbt_df.to_dicts()
        almsme_bt_r = [r for r in rows_bt if is_sme(r)]
        almsmebt_df = pl.from_dicts(almsme_bt_r) if almsme_bt_r else pl.DataFrame()
    if not mast_df.is_empty():
        rows_m = mast_df.to_dicts()
        mastsme_r = [r for r in rows_m if is_sme(r)]
        mastsme_df = pl.from_dicts(mastsme_r) if mastsme_r else pl.DataFrame()

    # PROC SUMMARY ALMSMEBT + MASTSME by PRODESC CUSTCD DNBFISME -> merge
    if not almsmebt_df.is_empty():
        agg_bt = [c for c in ['disburse','repaid','balance'] if c in almsmebt_df.columns]
        grp_bt = [c for c in ['prodesc','custcd','dnbfisme'] if c in almsmebt_df.columns]
        almsmebt_sum = almsmebt_df.group_by(grp_bt).agg([pl.col(c).sum() for c in agg_bt])
    else:
        almsmebt_sum = pl.DataFrame()

    if not mastsme_df.is_empty():
        agg_ms = [c for c in ['disbno','repayno','noacct'] if c in mastsme_df.columns]
        grp_ms = [c for c in ['prodesc','custcd','dnbfisme'] if c in mastsme_df.columns]
        mastsme_sum = mastsme_df.group_by(grp_ms).agg([pl.col(c).sum() for c in agg_ms])
    else:
        mastsme_sum = pl.DataFrame()

    # Merge ALMSMEBT + MASTSME
    if not almsmebt_sum.is_empty() and not mastsme_sum.is_empty():
        merge_key2 = [c for c in ['prodesc','custcd','dnbfisme']
                      if c in almsmebt_sum.columns and c in mastsme_sum.columns]
        almsmebt_merged = almsmebt_sum.join(mastsme_sum, on=merge_key2, how='outer')
    elif not almsmebt_sum.is_empty():
        almsmebt_merged = almsmebt_sum
    else:
        almsmebt_merged = mastsme_sum

    # Split merged into sub-categories
    dbebt_df    = pl.DataFrame(); dnbfibt_df = pl.DataFrame(); fbebt_df = pl.DataFrame()
    if not almsmebt_merged.is_empty():
        rows_m2  = almsmebt_merged.to_dicts()
        dbebt_r  = [r for r in rows_m2 if str(r.get('custcd') or '').strip() in DBE_CUSTCDS]
        dnbfibt_r= [r for r in rows_m2 if str(r.get('dnbfisme') or '').strip() in DNBFI_VALS]
        fbebt_r  = [r for r in rows_m2 if str(r.get('custcd') or '').strip() in FBE_CUSTCDS]
        dbebt_df  = pl.from_dicts(dbebt_r)  if dbebt_r  else pl.DataFrame()
        dnbfibt_df= pl.from_dicts(dnbfibt_r) if dnbfibt_r else pl.DataFrame()
        fbebt_df  = pl.from_dicts(fbebt_r)  if fbebt_r  else pl.DataFrame()

    # PROC SUMMARY ALMSMEBT -> ALMBTRD2
    if not almsmebt_merged.is_empty():
        agg_all = [c for c in NUM_COLS if c in almsmebt_merged.columns]
        almbtrd2_df = almsmebt_merged.group_by('prodesc').agg([pl.col(c).sum() for c in agg_all])
    else:
        almbtrd2_df = pl.DataFrame()

    # PROC PRINT: SME LOANS
    if not almloan_sme.is_empty():
        almloan_sme_main = almloan_sme.filter(pl.col('prodesc') != 'HOUSE FINANCING SOLD TO CAGAMAS')
        almloan_sme_hfsc = almloan_sme.filter(pl.col('prodesc') == 'HOUSE FINANCING SOLD TO CAGAMAS')
    else:
        almloan_sme_main = pl.DataFrame(); almloan_sme_hfsc = pl.DataFrame()

    print_table(almloan_sme_main, rw, BANK, f"SME LOANS AS AT {mm_yy}", RPT_ID)
    print_table(almloan_sme_hfsc, rw, BANK, f"SME LOANS AS AT {mm_yy}", RPT_ID)

    # -------------------------------------------------------------------------
    # ALMSME2 / DBE2 / DNBFI2 / FBE2 from ALM2
    # -------------------------------------------------------------------------
    almsme2_df = pl.DataFrame(); dbe2_df = pl.DataFrame()
    dnbfi2_df  = pl.DataFrame(); fbe2_df = pl.DataFrame()
    if not alm2_df.is_empty():
        rows_a2 = alm2_df.to_dicts()
        almsme2_r = [r for r in rows_a2 if is_sme(r)]
        dbe2_r    = [r for r in rows_a2 if is_dbe(r)]
        dnbfi2_r  = [r for r in rows_a2 if is_dnbfi(r)]
        fbe2_r    = [r for r in rows_a2 if is_fbe(r)]
        almsme2_df = pl.from_dicts(almsme2_r) if almsme2_r else pl.DataFrame()
        dbe2_df    = pl.from_dicts(dbe2_r)    if dbe2_r    else pl.DataFrame()
        dnbfi2_df  = pl.from_dicts(dnbfi2_r)  if dnbfi2_r  else pl.DataFrame()
        fbe2_df    = pl.from_dicts(fbe2_r)    if fbe2_r    else pl.DataFrame()

    # ALMBTCR2 / DBEBT2 from ALMSMEBT (Bills Retail)
    almbtcr2_df = pl.DataFrame(); dbebt2_df = pl.DataFrame()
    if not almsmebt_merged.is_empty():
        rows_bt2 = almsmebt_merged.to_dicts()
        cr2_r  = []
        dbt2_r = []
        for row in rows_bt2:
            if str(row.get('prodesc') or '') == 'BILLS RETAIL':
                r2 = dict(row); r2['prodesc'] = 'TOTAL COMMERCIAL RETAILS'
                cr2_r.append(r2)
                if str(r2.get('custcd') or '').strip() in DBE_CUSTCDS:
                    dbt2_r.append(r2)
        almbtcr2_df = pl.from_dicts(cr2_r)  if cr2_r  else pl.DataFrame()
        dbebt2_df   = pl.from_dicts(dbt2_r) if dbt2_r else pl.DataFrame()

    # DATA ALMLOAN2 (SME Retail): SET ALMSME2 ALMBTCR2
    almloan2_sme_src = pl.concat(
        [f for f in [almsme2_df, almbtcr2_df] if not f.is_empty()], how='diagonal'
    ) if any(not f.is_empty() for f in [almsme2_df, almbtcr2_df]) else pl.DataFrame()

    if not almloan2_sme_src.is_empty():
        agg_v4 = [c for c in NUM_COLS if c in almloan2_sme_src.columns]
        almloan2_sme = almloan2_sme_src.group_by('prodesc').agg([pl.col(c).sum() for c in agg_v4])
    else:
        almloan2_sme = pl.DataFrame()

    # PROC PRINT: RETAIL SME LOANS
    print_table(almloan2_sme, rw, BANK, f"RETAILS SME LOANS AS AT {mm_yy}", RPT_ID)

    # PROC SUMMARY / PRINT: DBE
    for df_in, title2_str in [
        (dbe_df,    f"OF WHICH : SME DBE AS AT {mm_yy}"),
    ]:
        if not df_in.is_empty():
            agg_v = [c for c in NUM_COLS if c in df_in.columns]
            dbe_sum = df_in.group_by('prodesc').agg([pl.col(c).sum() for c in agg_v])
            dbe_main = dbe_sum.filter(pl.col('prodesc') != 'HOUSE FINANCING SOLD TO CAGAMAS')
            dbe_hfsc = dbe_sum.filter(pl.col('prodesc') == 'HOUSE FINANCING SOLD TO CAGAMAS')
            print_table(dbe_main, rw, BANK, title2_str, RPT_ID)
            print_table(dbe_hfsc, rw, BANK, title2_str, RPT_ID)

    # DATA ALMLOAN3: SET DBE2 DBEBT2
    almloan3_src = pl.concat(
        [f for f in [dbe2_df, dbebt2_df] if not f.is_empty()], how='diagonal'
    ) if any(not f.is_empty() for f in [dbe2_df, dbebt2_df]) else pl.DataFrame()
    if not almloan3_src.is_empty():
        agg_v5 = [c for c in NUM_COLS if c in almloan3_src.columns]
        almloan3 = almloan3_src.group_by('prodesc').agg([pl.col(c).sum() for c in agg_v5])
    else:
        almloan3 = pl.DataFrame()
    print_table(almloan3, rw, BANK, f"OF WHICH : RETAILS SME DBE AS AT {mm_yy}", RPT_ID)

    # PROC SUMMARY / PRINT: DNBFI
    for df_in, t2 in [
        (dnbfi_df, f"OF WHICH : SME DNBFI AS AT {mm_yy}"),
    ]:
        if not df_in.is_empty():
            agg_v = [c for c in NUM_COLS if c in df_in.columns]
            dn_sum  = df_in.group_by('prodesc').agg([pl.col(c).sum() for c in agg_v])
            dn_main = dn_sum.filter(pl.col('prodesc') != 'HOUSE FINANCING SOLD TO CAGAMAS')
            dn_hfsc = dn_sum.filter(pl.col('prodesc') == 'HOUSE FINANCING SOLD TO CAGAMAS')
            print_table(dn_main, rw, BANK, t2, RPT_ID)
            print_table(dn_hfsc, rw, BANK, t2, RPT_ID)

    if not dnbfi2_df.is_empty():
        agg_vd = [c for c in NUM_COLS if c in dnbfi2_df.columns]
        dnbfi2_sum = dnbfi2_df.group_by('prodesc').agg([pl.col(c).sum() for c in agg_vd])
        print_table(dnbfi2_sum, rw, BANK, f"OF WHICH : RETAILS SME DNBFI AS AT {mm_yy}", RPT_ID)

    # PROC SUMMARY / PRINT: FBE
    for df_in, t2 in [
        (fbe_df, f"OF WHICH : SME FE AS AT {mm_yy}"),
    ]:
        if not df_in.is_empty():
            agg_v = [c for c in NUM_COLS if c in df_in.columns]
            fb_sum  = df_in.group_by('prodesc').agg([pl.col(c).sum() for c in agg_v])
            fb_main = fb_sum.filter(pl.col('prodesc') != 'HOUSE FINANCING SOLD TO CAGAMAS')
            fb_hfsc = fb_sum.filter(pl.col('prodesc') == 'HOUSE FINANCING SOLD TO CAGAMAS')
            print_table(fb_main, rw, BANK, t2, RPT_ID)
            print_table(fb_hfsc, rw, BANK, t2, RPT_ID)

    if not fbe2_df.is_empty():
        agg_vf = [c for c in NUM_COLS if c in fbe2_df.columns]
        fbe2_sum = fbe2_df.group_by('prodesc').agg([pl.col(c).sum() for c in agg_vf])
        print_table(fbe2_sum, rw, BANK, f"OF WHICH : RETAILS SME FE AS AT {mm_yy}", RPT_ID)

    # -------------------------------------------------------------------------
    # PROC PRINT: BANK TRADE
    # -------------------------------------------------------------------------
    print_table(almbtrd_df,  rw, BANK, f"BANK TRADE AS AT {mm_yy}",         RPT_ID)
    print_table(almbtrd2_df, rw, BANK, f"SME BANK TRADE AS AT {mm_yy}",     RPT_ID)

    # PROC SUMMARY / PRINT: DBEBT / DNBFIBT / FBEBT
    for df_in, t2 in [
        (dbebt_df,   f"OF WHICH : SME DBE BANK TRADE AS AT {mm_yy}"),
        (dnbfibt_df, f"OF WHICH : SME DNBFI BANK TRADE AS AT {mm_yy}"),
        (fbebt_df,   f"OF WHICH : SME FE BANK TRADE AS AT {mm_yy}"),
    ]:
        if not df_in.is_empty():
            agg_v = [c for c in NUM_COLS if c in df_in.columns]
            df_sum = df_in.group_by('prodesc').agg([pl.col(c).sum() for c in agg_v])
            print_table(df_sum, rw, BANK, t2, RPT_ID)

    # -------------------------------------------------------------------------
    # PROC TABULATE: RETAIL BILLS BY FACILITY
    # -------------------------------------------------------------------------
    almprod_df = pl.concat(
        [f for f in [alm2_df, almbtcr_df] if not f.is_empty()], how='diagonal'
    ) if any(not f.is_empty() for f in [alm2_df, almbtcr_df]) else pl.DataFrame()

    print_tabulate_facility(
        almprod_df, rw, BANK,
        f"TOTAL COMMERCIAL RETAIL FINANCING BY FACILITY AS AT {rdate}"
    )

    # -------------------------------------------------------------------------
    # PROC TABULATE: RETAIL BILLS BY SECTOR
    # -------------------------------------------------------------------------
    # PROC SUMMARY MAST by SECTGROUP SECTTYPE -> MASTSEC (NOACCT)
    # PROC SUMMARY ALMBT by SECTGROUP SECTTYPE -> ALMBTSEC (BALANCE)
    # MERGE MASTSEC + ALMBTSEC -> ALMBTSEC; PRODESC='TOTAL COMMERCIAL RETAILS'
    mastsec_df = pl.DataFrame(); almbtsec_df = pl.DataFrame()
    if not mast_df.is_empty():
        sg_key = [c for c in ['sectgroup','secttype'] if c in mast_df.columns]
        if sg_key and 'noacct' in mast_df.columns:
            mastsec_df = mast_df.group_by(sg_key).agg(pl.col('noacct').sum())

    if not almbt_df.is_empty():
        sg_key2 = [c for c in ['sectgroup','secttype'] if c in almbt_df.columns]
        if sg_key2 and 'balance' in almbt_df.columns:
            almbtsec_df = almbt_df.group_by(sg_key2).agg(pl.col('balance').sum())

    if not mastsec_df.is_empty():
        sg_join = [c for c in ['sectgroup','secttype']
                   if c in mastsec_df.columns and c in (almbtsec_df.columns if not almbtsec_df.is_empty() else [])]
        if sg_join and not almbtsec_df.is_empty():
            almbtsec_merged = mastsec_df.join(almbtsec_df, on=sg_join, how='left')
        else:
            almbtsec_merged = mastsec_df
        almbtsec_merged = almbtsec_merged.with_columns(
            pl.lit('TOTAL COMMERCIAL RETAILS').alias('prodesc')
        )
    else:
        almbtsec_merged = almbtsec_df.with_columns(
            pl.lit('TOTAL COMMERCIAL RETAILS').alias('prodesc')
        ) if not almbtsec_df.is_empty() else pl.DataFrame()

    almsec_df = pl.concat(
        [f for f in [alm2_df, almbtsec_merged] if not f.is_empty()], how='diagonal'
    ) if any(not f.is_empty() for f in [alm2_df, almbtsec_merged]) else pl.DataFrame()

    print_tabulate_sector(
        almsec_df, rw, BANK,
        f"TOTAL COMMERCIAL RETAIL FINANCING BY SECTOR AS AT {rdate}"
    )

    # -------------------------------------------------------------------------
    # Write report
    # -------------------------------------------------------------------------
    with open(REPORT_TXT, 'w', encoding='utf-8') as fh:
        rw.flush(fh)

    print(f"  Written: {REPORT_TXT}")
    print("EIIBNM01: Processing complete.")


if __name__ == '__main__':
    main()
