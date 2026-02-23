# !/usr/bin/env python3
"""
Program Name: EIBWPBC4
Purpose: Generate CCRIS (Central Credit Reference Information System) output files for PBB
         - OUTPUT1 (CCRISIND): Individual loan customer records (LRECL=1700)
         - OUTPUT2 (CCRISCOR): Corporate loan customer records (LRECL=700)
         - OUTPUT3 (CCRISOWN): Corporate ownership records (LRECL=600)
         - OUTDATE (DATE):     Report date file (LRECL=80)
"""

import duckdb
import polars as pl
from datetime import date, datetime
import os
import re

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

# Input parquet paths
PBIF_REPTDATE_PARQUET  = "input/pbif/reptdate.parquet"
PBIF_CLIEN_PREFIX      = "input/pbif/clien"         # clien<YY><MM><DD>.parquet
BNM_LNACCT_PARQUET     = "input/bnm/lnacct.parquet"
LIMIT_PARQUET          = "input/limit/limit.parquet"
DEPOSIT_PARQUET        = "input/deposit/deposit.parquet"
CISLN_LOAN_PARQUET     = "input/cisln/loan.parquet"
CISLN_OWNER_PARQUET    = "input/cisln/owner.parquet"
CISDP_OWNER_PARQUET    = "input/cisdp/owner.parquet"
CISOWN_PARQUET         = "input/cisown/cisown.parquet"
CISREL_RELATION_PARQUET= "input/cisrel/relation.parquet"

# Output paths
OUTPUT1_TXT  = "output/ccrisind.txt"    # SAP.PBIF.CCRIS1.CCRISIND  LRECL=1700
OUTPUT2_TXT  = "output/ccriscor.txt"    # SAP.PBIF.CCRIS1.CCRISCOR  LRECL=700
OUTPUT3_TXT  = "output/ccrisown.txt"    # SAP.PBIF.CCRIS1.CCRISOWN  LRECL=600
OUTDATE_TXT  = "output/date.txt"        # SAP.PBIF.CCRIS1.DATE      LRECL=80

os.makedirs("output", exist_ok=True)

# =============================================================================
# PROC FORMAT: $PROF.
# =============================================================================

PROF_FMT = {
    '001': 'ACCOUNTANT',
    '002': 'ARCHITECT',
    '003': 'BANKER',
    '004': 'OFFICE/BUSINESS EXEC',
    '005': 'CLERICAL/NONCLERICAL',
    '006': 'DIRECTOR',
    '007': 'DOCTOR',
    '008': 'DENTIST',
    '009': 'ENGINEER',
    '010': 'GOVERNMENT SERVANTS',
    '011': 'FACTORY WORKER',
    '012': 'HOUSEWIFE',
    '013': 'LAWYER',
    '014': 'MECHANICS',
    '015': 'SELF EMPLOYED',
    '016': 'STUDENT',
    '017': 'TEACHER/LECTURER',
    '018': 'TECHNICIAN',
    '019': 'SURVEYOR',
    '020': 'OTHER PROFESSIONAL',
    '021': 'ALL OTHERS',
    '022': 'NO AVAILABLE',
    '   ': 'NO AVAILABLE',
}

def fmt_prof(occupat):
    """Apply $PROF. format."""
    if occupat is None:
        key = '   '
    else:
        key = str(occupat).strip()
        key = key.zfill(3) if key.isdigit() else key
    return PROF_FMT.get(key, PROF_FMT.get('   ', 'NO AVAILABLE'))

# =============================================================================
# UTILITY HELPERS
# =============================================================================

def sas_date_to_pydate(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + __import__('datetime').timedelta(days=int(val))
    if isinstance(val, str):
        try:
            return datetime.strptime(val, '%Y-%m-%d').date()
        except ValueError:
            return None
    if isinstance(val, (date, datetime)):
        return val if isinstance(val, date) else val.date()
    return None

def fmt_ddmmyy8(d):
    if d is None:
        return ''
    return d.strftime('%d/%m/%y')

def coalesce_str(val, default=''):
    if val is None:
        return default
    return str(val)

def coalesce_int(val, default=0):
    if val is None or (isinstance(val, float) and val != val):
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default

def safe_str(val, width, fill=' '):
    """Return string left-justified in `width` chars."""
    s = coalesce_str(val)
    return s.ljust(width)[:width]

def safe_num_str(val, width):
    """Right-justify integer in `width` chars (blank if None)."""
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * width
    try:
        return str(int(val)).rjust(width)
    except (TypeError, ValueError):
        return ' ' * width

def safe_znum(val, width):
    """Zero-pad integer to `width` chars."""
    if val is None or (isinstance(val, float) and val != val):
        return '0' * width
    try:
        return str(int(val)).zfill(width)
    except (TypeError, ValueError):
        return '0' * width

def place_at(buf: bytearray, col: int, text: str, width: int):
    """
    Place text (truncated/padded to `width`) into buf at 1-indexed col.
    buf is a fixed-length bytearray.
    """
    col0 = col - 1
    encoded = text.ljust(width)[:width].encode('ascii', errors='replace')
    buf[col0: col0 + width] = encoded

def place_num(buf: bytearray, col: int, val, width: int, zero_pad: bool = False):
    """Place numeric value right-justified (or zero-padded) in buf."""
    if val is None or (isinstance(val, float) and val != val):
        s = '0' * width if zero_pad else ' ' * width
    else:
        try:
            iv = int(val)
            s = str(iv).zfill(width) if zero_pad else str(iv).rjust(width)
        except (TypeError, ValueError):
            s = '0' * width if zero_pad else ' ' * width
    s = s[:width]
    buf[col - 1: col - 1 + width] = s.encode('ascii', errors='replace')

def buf_to_line(buf: bytearray) -> str:
    return buf.decode('ascii', errors='replace')

# =============================================================================
# COMPRESS HELPERS  (SAS COMPRESS function)
# =============================================================================

def compress_chars(s: str, chars: str) -> str:
    """Remove all characters in `chars` from string s (SAS COMPRESS)."""
    if s is None:
        return ''
    return ''.join(c for c in str(s) if c not in chars)

def compress_keep_alnum(s: str) -> str:
    """Keep only alphanumeric chars (used for IC cleaning)."""
    if s is None:
        return ''
    return ''.join(c for c in str(s) if c.isalnum())

# =============================================================================
# IC CLEANING LOGIC (replicated from SAS DATA INDLOAN / CORLOAN)
# =============================================================================

_VALID_OLDIC = set('0123456789AKP')
_VALID_NEWIC  = set('0123456789')

def clean_ic_old(oldic: str) -> str:
    """Strip invalid chars from OLDIC (keep 0-9, A-Z after UPCASE)."""
    if not oldic or oldic.strip() == '':
        return ' '
    oldic = oldic.upper()
    kept = [c for c in oldic if c.isalpha() or c.isdigit()]
    result = ''.join(kept[:17]).ljust(17)
    return result

def clean_ic_new(newic: str) -> str:
    """Strip invalid chars from NEWIC (keep 0-9, A-Z after UPCASE)."""
    if not newic or newic.strip() == '':
        return ' '
    newic = newic.upper()
    kept = [c for c in newic if c.isalpha() or c.isdigit()]
    result = ''.join(kept[:17]).ljust(17)
    return result

def process_ic_fields(oldic_raw, newic_raw, cacccode=None, seccust=None, newicind=None, indorg=None):
    """
    Full IC processing logic from SAS DATA INDLOAN / CORLOAN step.
    Returns (oldic, newic) both as 20-char strings.
    """
    oldic = coalesce_str(oldic_raw, ' ').ljust(20)[:20]
    newic = coalesce_str(newic_raw, ' ').ljust(20)[:20]

    # IF OLDIC = '  ' AND INDORG = 'I' ...
    if oldic.strip() == '' and indorg == 'I':
        if cacccode in ('000', '020') and seccust == '901':
            if newicind == 'IC':
                oldic = newic

    # IF INPUT(OLDIC,$20.) IN ('0','.') THEN OLDIC = ' '
    if oldic.strip() in ('0', '.'):
        oldic = ' ' * 20

    # IF OLDIC = NEWIC THEN NEWIC = '  '
    if oldic.strip() == newic.strip() and oldic.strip() != '':
        newic = ' ' * 20

    # IF OLDIC = ' ' AND NEWIC NE ' ' THEN swap
    if oldic.strip() == '' and newic.strip() != '':
        oldic = newic
        newic = ' ' * 20

    # Clean OLDIC if it contains invalid chars
    if oldic.strip() != '':
        oldic_up = oldic.upper()
        check_chars = set('0123456789AKP')
        bad_pos = next((i for i, c in enumerate(oldic_up) if c not in check_chars and c != ' '), -1)
        if bad_pos >= 0:
            oldic = clean_ic_old(oldic_up).ljust(20)[:20]

    # NEWIC cleanup
    if newic.strip() == '-N/A-               '[:len(newic.strip())]:
        newic = ' ' * 20
    elif newic.strip().startswith('-N/A-'):
        newic = ' ' * 20
    elif newic.strip().startswith('N/A'):
        newic = ' ' * 20
    elif newic.strip() in ('0', '.'):
        newic = ' ' * 20

    # Clean NEWIC if it contains invalid chars
    if newic.strip() != '':
        newic_up = newic.upper()
        bad_pos = next((i for i, c in enumerate(newic_up) if not c.isdigit() and c != ' '), -1)
        if bad_pos >= 0:
            newic = clean_ic_new(newic_up).ljust(20)[:20]

    # REPLACE OLDIC WITH NEWIC IF OLDIC HAS TRUNCATED VALUE
    if oldic.strip() != '' and newic.strip() != '' and oldic.strip() != newic.strip():
        for ln in [7, 8, 9, 10]:
            if oldic[:ln] == newic[:ln]:
                oldic = ' ' * 20
                break

    if oldic.strip() == '':
        oldic = newic
        newic = ' ' * 20

    # NEWIC = LEFT(NEWIC)
    newic = newic.lstrip().ljust(20)[:20]

    return oldic[:20], newic[:20]

# =============================================================================
# REPORT DATE VARIABLES
# =============================================================================

def get_report_vars(reptdate_parquet: str) -> dict:
    """Read REPTDATE and compute all macro variables. Also writes OUTDATE file."""
    con = duckdb.connect()
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate = sas_date_to_pydate(row[0])
    day   = reptdate.day
    month = reptdate.month
    year  = reptdate.year

    if day == 8:
        wk = '1'; wk1 = '4'
    elif day == 15:
        wk = '2'; wk1 = '1'
    elif day == 22:
        wk = '3'; wk1 = '2'
    else:
        wk = '4'; wk1 = '3'

    mm  = month
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    sdate_d  = date(year, month, day)
    # CALL SYMPUT('SDATE', PUT(SDATE, Z5.)) — SAS date as 5-digit zero-padded integer
    sdate_sasint = (sdate_d - date(1960, 1, 1)).days
    sdate_z5     = str(sdate_sasint).zfill(5)

    reptyea2 = str(year)[-2:].zfill(2)
    reptmon  = str(mm).zfill(2)
    reptmon1 = str(mm1).zfill(2)
    reptday  = str(day).zfill(2)
    reptyear = str(year)
    rdate    = fmt_ddmmyy8(reptdate)

    return {
        'reptdate':  reptdate,
        'day':       day,
        'month':     month,
        'year':      year,
        'wk':        wk,
        'wk1':       wk1,
        'mm':        mm,
        'mm1':       mm1,
        'nowk':      wk,
        'nowk1':     wk1,
        'reptmon':   reptmon,
        'reptmon1':  reptmon1,
        'reptyear':  reptyear,
        'reptyea2':  reptyea2,
        'rdate':     rdate,
        'reptday':   reptday,
        'sdate':     sdate_z5,
        'sdate_d':   sdate_d,
    }

def write_outdate(rv: dict, output_path: str):
    """
    FILE OUTDATE;
    PUT @0001 DAYS Z2. @0003 '/' @0004 MONTHS Z2. @0006 '/' @0007 YEARS 4.;
    LRECL=80
    """
    days_s   = str(rv['day']).zfill(2)
    months_s = str(rv['month']).zfill(2)
    years_s  = str(rv['year']).ljust(4)[:4]

    buf = bytearray(b' ' * 80)
    buf[0:2]  = days_s.encode('ascii')
    buf[2:3]  = b'/'
    buf[3:5]  = months_s.encode('ascii')
    buf[5:6]  = b'/'
    buf[6:10] = years_s.encode('ascii')

    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        f.write(buf.decode('ascii') + '\n')

# =============================================================================
# DATA PREPARATION
# =============================================================================

def load_pbif(rv: dict) -> pl.DataFrame:
    """
    DATA PBIF: SET PBIF.CLIEN&REPTYEA2&REPTMON&REPTDAY;
    IF ENTITY='PBBH'; IF BRANCH > 0;
    """
    path = f"{PBIF_CLIEN_PREFIX}{rv['reptyea2']}{rv['reptmon']}{rv['reptday']}.parquet"
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    df = df.filter(
        (pl.col('entity') == 'PBBH') &
        (pl.col('branch') > 0)
    )
    return df

def load_btacc() -> pl.DataFrame:
    """PROC SORT DATA=BNM.LNACCT OUT=BTACC; BY ACCTNO;"""
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT * FROM read_parquet('{BNM_LNACCT_PARQUET}') ORDER BY acctno"
    ).pl()
    con.close()
    return df

def merge_pbif_btacc(pbif: pl.DataFrame, btacc: pl.DataFrame) -> pl.DataFrame:
    """
    DATA PBIF: MERGE PBIF(IN=A) BTACC(IN=B); BY ACCTNO; IF A AND B;
    NOTENO=0; PAIDIND=' '; REVERSED=' ';
    """
    merged = pbif.sort('acctno').join(btacc.sort('acctno'), on='acctno', how='inner', suffix='_bt')
    # Resolve duplicate columns (BTACC overwrites PBIF for matching columns)
    for col in btacc.columns:
        bt_col = f"{col}_bt"
        if bt_col in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(bt_col).is_not_null())
                  .then(pl.col(bt_col))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(bt_col)
    merged = merged.with_columns([
        pl.lit(0).alias('noteno'),
        pl.lit(' ').alias('paidind'),
        pl.lit(' ').alias('reversed'),
    ])
    return merged

def load_relation() -> pl.DataFrame:
    """
    PROC SORT DATA=CISREL.RELATION OUT=RELATION(KEEP=ACCTNO SPOUSE1 SPOUSE1I SPOUSE1D CUSTNO);
    BY CUSTNO ACCTNO;
    """
    con = duckdb.connect()
    df  = con.execute(f"""
        SELECT acctno, spouse1, spouse1i, spouse1d, custno
        FROM read_parquet('{CISREL_RELATION_PARQUET}')
        ORDER BY custno, acctno
    """).pl()
    con.close()
    return df

def load_cisloan() -> pl.DataFrame:
    """
    PROC SORT DATA=CISLN.LOAN OUT=CISLOAN(DROP=BRANCH); BY CUSTNO ACCTNO;
    WHERE CACCCODE NOT IN ('013','014','016','017','018','019','021','022','023','024','025','027');
    """
    excluded = ('013','014','016','017','018','019','021','022','023','024','025','027')
    con = duckdb.connect()
    df  = con.execute(f"""
        SELECT * FROM read_parquet('{CISLN_LOAN_PARQUET}')
        WHERE cacccode NOT IN {excluded}
        ORDER BY custno, acctno
    """).pl()
    con.close()
    if 'branch' in df.columns:
        df = df.drop('branch')
    return df

def build_loana(relation: pl.DataFrame, cisloan: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOANA: MERGE RELATION CISLOAN(IN=A); BY CUSTNO ACCTNO; IF A;
    """
    merged = cisloan.join(relation, on=['custno', 'acctno'], how='left', suffix='_rel')
    for col in relation.columns:
        rel_col = f"{col}_rel"
        if rel_col in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(rel_col).is_not_null())
                  .then(pl.col(rel_col))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(rel_col)
    return merged.sort('acctno')

def merge_loan_loana(pbif: pl.DataFrame, loana: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN: SET PBIF; PROC SORT; BY ACCTNO;
    DATA LOAN: MERGE LOAN(IN=A) LOANA(IN=B); BY ACCTNO; IF A AND B;
    """
    loan = pbif.sort('acctno')
    merged = loan.join(loana.sort('acctno'), on='acctno', how='inner', suffix='_la')
    for col in loana.columns:
        la_col = f"{col}_la"
        if la_col in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(la_col).is_not_null())
                  .then(pl.col(la_col))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(la_col)
    return merged

def build_loannew_nocustom(loan: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOANNEW(KEEP=ACCTNO NOCUSTOM): SET LOAN; BY ACCTNO;
    NOCUSTOM+1; IF LAST.ACCTNO THEN OUTPUT;
    Counts number of rows per ACCTNO.
    """
    return (
        loan.group_by('acctno')
            .agg(pl.count().alias('nocustom'))
    )

def build_final_loan(loan: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN: MERGE LOAN(IN=A) LOANNEW(IN=B); BY ACCTNO; IF A AND B; APCODE=000;
    """
    loannew = build_loannew_nocustom(loan)
    merged  = loan.join(loannew, on='acctno', how='inner', suffix='_new')
    if 'nocustom_new' in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col('nocustom_new').is_not_null())
              .then(pl.col('nocustom_new'))
              .otherwise(pl.col('nocustom') if 'nocustom' in merged.columns else pl.lit(None))
              .alias('nocustom')
        ).drop('nocustom_new')
    return merged.with_columns(pl.lit(0).alias('apcode'))

# =============================================================================
# SPLIT INTO INDLOAN / CORLOAN
# =============================================================================

def split_ind_cor(loan: pl.DataFrame) -> tuple:
    """
    DATA INDLOAN CORLOAN: SET LOAN;
    Filter: REVERSED NE 'Y' AND NOTENO NE . (AND PAIDIND NE 'P' commented out)
    INDORG='I' and CACCCODE conditions -> INDLOAN
    Otherwise -> CORLOAN
    Also applies full IC processing per row.
    """
    # Excluded CACCCODE for INDLOAN
    ind_excl = {'001','002','013','016','017','018','019','021','022','023','024','025','027'}

    rows     = loan.to_dicts()
    ind_rows = []
    cor_rows = []

    for row in rows:
        reversed_v = coalesce_str(row.get('reversed'), ' ').strip()
        noteno_v   = row.get('noteno')
        # paidind_v = coalesce_str(row.get('paidind'), ' ').strip()  # commented out in SAS

        # IF (REVERSED NE 'Y') AND (NOTENO NE .) /* AND (PAIDIND NE 'P') */
        if reversed_v == 'Y':
            continue
        if noteno_v is None:
            continue

        indorg   = coalesce_str(row.get('indorg'), ' ').strip()
        cacccode = coalesce_str(row.get('cacccode'), '').strip()
        seccust  = coalesce_str(row.get('seccust'), '').strip()
        newicind = coalesce_str(row.get('newicind'), '').strip()

        oldic, newic = process_ic_fields(
            row.get('oldic'), row.get('newic'),
            cacccode=cacccode, seccust=seccust,
            newicind=newicind, indorg=indorg
        )
        row['oldic'] = oldic
        row['newic'] = newic
        row['acty']  = 'LN'

        if indorg == 'I':
            if cacccode not in ind_excl:
                ind_rows.append(row)
        else:
            cor_rows.append(row)

    ind_df = pl.from_dicts(ind_rows) if ind_rows else pl.DataFrame()
    cor_df = pl.from_dicts(cor_rows) if cor_rows else pl.DataFrame()

    return ind_df, cor_df

# =============================================================================
# WRITE OUTPUT1 — INDCCRIS (LRECL=1700)
# =============================================================================

def write_indccris(indloan: pl.DataFrame, output_path: str):
    """
    DATA INDCCRIS: SET INDLOAN; compute derived fields; FILE OUTPUT1; PUT ...
    LRECL=1700, fixed-width.
    """
    LRECL = 1700

    def compute_bumi(row):
        custcode = coalesce_int(row.get('custcode'))
        custcdx  = coalesce_int(row.get('custcdx'))
        bumi = '9'
        if custcode > 80:
            pass  # RESIDENT='N' handled below, BUMI stays '9' unless overridden
        if custcode in (41,42,43,77):  bumi = '1'
        if custcode in (44,46,47,78):  bumi = '2'
        if custcode in (52,53,54):     bumi = '3'
        if bumi == '9':
            if custcdx in (41,42,43,77): bumi = '1'
            if custcdx in (44,46,47,78): bumi = '2'
            if custcdx in (52,53,54):    bumi = '3'
        if custcode in (48,49,51):     bumi = '9'
        return bumi

    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        for row in indloan.iter_rows(named=True):
            buf = bytearray(b' ' * LRECL)

            # Derived fields
            profess  = fmt_prof(row.get('occupat'))[:60].ljust(60)
            basicgc  = 11
            ficode   = coalesce_str(row.get('cacccode'), '   ')[:3]
            namekey  = '  '.ljust(140)
            district = ' '.ljust(20)
            resident = 'N' if coalesce_int(row.get('custcode')) > 80 else 'R'
            bumi     = compute_bumi(row)
            country  = coalesce_str(row.get('ctry'), '').strip()
            if country == '':
                country = 'MY'
            national = coalesce_str(row.get('citizen'), '  ')[:2]
            phone3   = ' ' * 20
            phone4   = ' ' * 20

            mailstat = coalesce_str(row.get('mailstat'), '00').strip()
            valid_ms = {str(i).zfill(2) for i in range(1, 17)}
            if mailstat not in valid_ms:
                mailstat = '00'

            gender = coalesce_str(row.get('gender'), ' ').strip()
            if gender == 'M':   gender = 'L'
            elif gender == 'F': gender = 'P'
            else:               gender = 'R'

            marital = coalesce_str(row.get('marital'), ' ').strip()
            if marital in ('M','P','W'):   marital = 'M'
            elif marital in ('S','D'):     marital = 'S'
            else:                          marital = 'U'

            spouse1i = coalesce_str(row.get('spouse1i'), ' ')
            spouse1d = coalesce_str(row.get('spouse1d'), ' ')
            if spouse1i.strip() != '':
                rm_chars = '=`&-/@#*+:().,"%\xa3!$?_ '
                spouse1i = compress_chars(spouse1i, rm_chars)
                spouse1i = compress_chars(spouse1i, "'")
            if spouse1d.strip() != '':
                rm_chars = '=`&-/@#*+:().,"%\xa3!$?_ '
                spouse1d = compress_chars(spouse1d, rm_chars)
                spouse1d = compress_chars(spouse1d, "'")
            if spouse1i.strip() == '' and spouse1d.strip() == '':
                spouse1_val = ' ' * 150
            else:
                spouse1_val = coalesce_str(row.get('spouse1'), ' ')[:150].ljust(150)

            custname = compress_chars(coalesce_str(row.get('custname')), '\\')

            # Numeric date components
            dob_dd = safe_znum(row.get('dobdd'), 2)
            dob_mm = safe_znum(row.get('dobmm'), 2)
            dob_cc = safe_znum(row.get('dobcc'), 2)
            dob_yy = safe_znum(row.get('dobyy'), 2)
            lmnt_dd = safe_znum(row.get('lstmntdd'), 2)
            lmnt_mm = safe_znum(row.get('lstmntmm'), 2)
            lmnt_cc = safe_znum(row.get('lstmntcc'), 2)
            lmnt_yy = safe_znum(row.get('lstmntyy'), 2)
            lchg_dd = safe_znum(row.get('lstchgdd'), 2)
            lchg_mm = safe_znum(row.get('lstchgmm'), 2)
            lchg_cc = safe_znum(row.get('lstchgcc'), 2)
            lchg_yy = safe_znum(row.get('lstchgyy'), 2)
            empl_dd = safe_znum(row.get('empldd'), 2)
            empl_mm = safe_znum(row.get('emplmm'), 2)
            empl_cc = safe_znum(row.get('emplcc'), 2)
            empl_yy = safe_znum(row.get('emplyy'), 2)

            # Build record
            # @0001 FICODE $3.
            buf[0:3]    = ficode.ljust(3)[:3].encode('ascii', errors='replace')
            # @0007 BRANCH Z3.
            buf[6:9]    = safe_znum(row.get('branch'), 3).encode('ascii')
            # @0010 APCODE Z3.
            buf[9:12]   = safe_znum(row.get('apcode'), 3).encode('ascii')
            # @0013 CUSTNO 20.
            buf[12:32]  = safe_num_str(row.get('custno'), 20).encode('ascii', errors='replace')
            # @0033 ACCTNO 10.
            buf[32:42]  = safe_num_str(row.get('acctno'), 10).encode('ascii', errors='replace')
            # @0043 NAMEKEY $140.
            buf[42:182] = namekey[:140].encode('ascii', errors='replace')
            # @0183 CUSTNAME $150.
            buf[182:332]= custname.ljust(150)[:150].encode('ascii', errors='replace')
            # @0333 NEWIC $20.
            newic_v = coalesce_str(row.get('newic'), ' ').ljust(20)[:20]
            buf[332:352]= newic_v.encode('ascii', errors='replace')
            # @0353 OLDIC $20.
            oldic_v = coalesce_str(row.get('oldic'), ' ').ljust(20)[:20]
            buf[352:372]= oldic_v.encode('ascii', errors='replace')
            # Date fields (2-char numeric each)
            buf[372:374] = dob_dd.encode('ascii')
            buf[374:376] = dob_mm.encode('ascii')
            buf[376:378] = dob_cc.encode('ascii')
            buf[378:380] = dob_yy.encode('ascii')
            # @0381 GENDER $1.
            buf[380:381] = gender[:1].encode('ascii', errors='replace')
            # @0382 NATIONAL $2.
            buf[381:383] = national.ljust(2)[:2].encode('ascii', errors='replace')
            # @0384 MARITAL $1.
            buf[383:384] = marital[:1].encode('ascii', errors='replace')
            # @0385 BUMI $1.
            buf[384:385] = bumi[:1].encode('ascii', errors='replace')
            # @0386 BASICGC 2.
            buf[385:387] = str(basicgc).rjust(2).encode('ascii')
            # @0388 LSTMNTDD 2. ... @0394 LSTMNTYY 2.
            buf[387:389] = lmnt_dd.encode('ascii')
            buf[389:391] = lmnt_mm.encode('ascii')
            buf[391:393] = lmnt_cc.encode('ascii')
            buf[393:395] = lmnt_yy.encode('ascii')
            # @0396 ADDRLN1 $40. @0436 ADDRLN2 $40. @0476 ADDRLN3 $40. @0516 ADDRLN4 $40.
            for i, akey in enumerate(['addrln1','addrln2','addrln3','addrln4']):
                start = 395 + i * 40
                aval  = coalesce_str(row.get(akey), ' ').ljust(40)[:40]
                buf[start:start+40] = aval.encode('ascii', errors='replace')
            # @0556 LSTCHGDD..LSTCHGYY
            buf[555:557] = lchg_dd.encode('ascii')
            buf[557:559] = lchg_mm.encode('ascii')
            buf[559:561] = lchg_cc.encode('ascii')
            buf[561:563] = lchg_yy.encode('ascii')
            # @0564 COUNTRY $2.
            buf[563:565] = country.ljust(2)[:2].encode('ascii', errors='replace')
            # @0566 MAILCODE 5.
            buf[565:570] = safe_num_str(row.get('mailcode'), 5).encode('ascii', errors='replace')
            # @0571 DISTRICT $20.
            buf[570:590] = district[:20].encode('ascii', errors='replace')
            # @0591 MAILSTAT $20.
            buf[590:610] = mailstat.ljust(20)[:20].encode('ascii', errors='replace')
            # @0611 PRIPHONE $20.
            buf[610:630] = coalesce_str(row.get('priphone'),' ').ljust(20)[:20].encode('ascii', errors='replace')
            # @0631 SECPHONE $20.
            buf[630:650] = coalesce_str(row.get('secphone'),' ').ljust(20)[:20].encode('ascii', errors='replace')
            # @0651 PHONE3 $20.
            buf[650:670] = phone3[:20].encode('ascii', errors='replace')
            # @0671 PHONE4 $20.
            buf[670:690] = phone4[:20].encode('ascii', errors='replace')
            # @0691 SPOUSE1 $150.
            buf[690:840] = spouse1_val[:150].encode('ascii', errors='replace')
            # @0841 SPOUSE1I $20.
            buf[840:860] = spouse1i.ljust(20)[:20].encode('ascii', errors='replace')
            # @0861 SPOUSE1D $20.
            buf[860:880] = spouse1d.ljust(20)[:20].encode('ascii', errors='replace')
            # @1451 EMPLNAME $150.  (col 1451 = index 1450)
            buf[1450:1600]= coalesce_str(row.get('emplname'),' ').ljust(150)[:150].encode('ascii', errors='replace')
            # @1601 PROFESS $60.
            buf[1600:1660]= profess[:60].encode('ascii', errors='replace')
            # @1661 EMPLDD..EMPLYY
            buf[1660:1662]= empl_dd.encode('ascii')
            buf[1662:1664]= empl_mm.encode('ascii')
            buf[1664:1666]= empl_cc.encode('ascii')
            buf[1666:1668]= empl_yy.encode('ascii')
            # @1669 RESIDENT $1.
            buf[1668:1669]= resident[:1].encode('ascii', errors='replace')
            # @1670 COSTCTR 4.
            buf[1669:1673]= safe_num_str(row.get('costctr'), 4).encode('ascii', errors='replace')

            f.write(buf.decode('ascii', errors='replace') + '\n')

# =============================================================================
# WRITE OUTPUT2 — CORCCRIS (LRECL=700)
# =============================================================================

def compute_cor_basicgc(row: dict) -> int:
    """Compute BASICGC for corporate records per SAS logic."""
    custcode = coalesce_int(row.get('custcode'))
    orgtype  = coalesce_str(row.get('orgtype'), ' ').strip()
    acty     = coalesce_str(row.get('acty'),    'LN').strip()
    purpose  = coalesce_str(row.get('purpose'),  ' ').strip()
    cacccode = coalesce_str(row.get('cacccode'), '').strip()

    basicgc = 91
    if custcode == 99:             basicgc = 91
    elif custcode == 98:           basicgc = 51
    elif custcode == 79:           basicgc = 43
    elif custcode == 35:           basicgc = 42
    elif custcode in (1, 74):      basicgc = 34
    elif custcode == 73:           basicgc = 33
    elif custcode == 72:           basicgc = 32
    elif custcode in (71,90,91,92): basicgc = 31

    grp_ac = {40,41,42,43,44,46,47,48,49,51,52,53,54,68,61,62,66,67,86}
    grp_c  = {2,3,4,5,6,11,12,13,17,20,30,31,32,33,34,37,38,39,
              40,41,42,43,44,46,47,48,49,51,52,53,54,61,62,66,67,
              68,57,59,63,64,69,75,82,83,84}

    if acty == 'LN':
        if custcode in (79,98,99) and orgtype == 'T':
            basicgc = 41
        if custcode in grp_ac and orgtype == 'I':
            basicgc = 21
        elif custcode in grp_ac and orgtype == 'P':
            basicgc = 22
        elif custcode in grp_ac and orgtype == 'L':
            basicgc = 23
        elif custcode in grp_c and orgtype == 'C':
            basicgc = 24
    elif acty == 'OD':
        # IF SPOUSE1 EQ ' ' (filter applied upstream if needed)
        if custcode in (79,98,99) and purpose == 'J':
            basicgc = 41
        if custcode in grp_ac and purpose == '7':
            basicgc = 21
        elif custcode in grp_ac and purpose == '8':
            basicgc = 22
        elif custcode in grp_ac and purpose == 'I':
            basicgc = 23
        elif custcode in grp_c and purpose in ('9','A','E'):
            basicgc = 24

    if cacccode in ('017','012'):
        basicgc = 24

    return basicgc

def compute_cor_bussreg(row: dict, basicgc: int, bumi: str) -> str:
    """Compute BUSSREG for corporate records."""
    orgtype  = coalesce_str(row.get('orgtype'),  ' ').strip()
    bussreg  = coalesce_str(row.get('bussreg'),  ' ')
    newic    = coalesce_str(row.get('newic'),     ' ')
    oldic    = coalesce_str(row.get('oldic'),     ' ')
    nocustom = coalesce_int(row.get('nocustom'),  0)
    guarend  = coalesce_str(row.get('guarend'),   ' ')
    citizen  = coalesce_str(row.get('citizen'),   ' ').strip()

    if basicgc in (23,41,42,43,51,91):
        if bussreg.strip() == '':
            bussreg = newic
        if newic.strip() == '':
            bussreg = oldic
    if bussreg.strip() == '':
        bussreg = newic
    if bussreg.strip() == '':
        bussreg = oldic
    if bussreg.strip() == '' and nocustom == 1:
        bussreg = guarend

    # Reformat BUSSREG based on ORGTYPE
    if (orgtype == 'C' and bussreg.strip() != '') or basicgc == 24:
        trimmed = bussreg.strip()
        tlen    = len(trimmed)
        if tlen <= 7 and tlen >= 2:
            vleft  = trimmed[:-1]
            vright = trimmed[-1]
            try:
                vnumic = int(vleft)
                vnewa  = str(vnumic).zfill(6)
            except ValueError:
                vnewa  = vleft
            bussreg = vnewa + vright
    elif orgtype in ('I','P') and bussreg.strip() != '' and basicgc in (21,22,23):
        trimmed = bussreg.strip()
        tlen    = len(trimmed)
        if tlen <= 10 and tlen >= 2:
            vleft  = trimmed[:-1]
            vright = trimmed[-1]
            try:
                vnumic = int(vleft)
                vnewa  = str(vnumic).zfill(9)
            except ValueError:
                vnewa  = vleft
            bussreg = vnewa + vright

    return bussreg

def write_corccris(corloan: pl.DataFrame, output_path: str):
    """
    DATA CORCCRIS: SET CORLOAN; compute derived fields; FILE OUTPUT2; PUT ...
    LRECL=700, fixed-width.
    """
    LRECL = 700

    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        for row in corloan.iter_rows(named=True):
            buf = bytearray(b' ' * LRECL)

            custcode = coalesce_int(row.get('custcode'))
            ficode   = coalesce_str(row.get('cacccode'), '   ')[:3]
            namekey  = '  '.ljust(140)
            district = ' ' * 20
            phone3   = ' ' * 20
            phone4   = ' ' * 20
            regcouty = 'MY'

            bumi = ' '
            if custcode in (41,42,43,61,66):                          bumi = '1'
            if custcode in (44,46,47,62,67):                          bumi = '2'
            if custcode in (52,53,54,57,59,64,69,75,68,0):            bumi = '3'
            if custcode in (1,35,71,72,73,74,79,82,83,84,86,
                            90,91,92,98,99):                           bumi = '9'

            resident = 'N' if custcode > 80 else 'R'
            country  = coalesce_str(row.get('ctry'), '').strip()
            if country == '':
                country = 'MY'

            nrcc = 9
            if custcode in (63, 68):
                nrcc = 2

            mailstat = coalesce_str(row.get('mailstat'), '00').strip()
            valid_ms = {str(i).zfill(2) for i in range(1, 17)}
            if mailstat not in valid_ms:
                mailstat = '00'

            basicgc = compute_cor_basicgc(row)

            if basicgc in (31,32,33,34,41,42,43,51,91):
                bumi = '9'

            citizen = coalesce_str(row.get('citizen'), '').strip()
            if citizen == 'MY' and basicgc == 24 and custcode != 63:
                nrcc = 1

            bussreg = compute_cor_bussreg(row, basicgc, bumi)

            custname = compress_chars(coalesce_str(row.get('custname')), '\\')

            # Date components
            dob_dd  = safe_znum(row.get('dobdd'),   2)
            dob_mm  = safe_znum(row.get('dobmm'),   2)
            dob_cc  = safe_znum(row.get('dobcc'),   2)
            dob_yy  = safe_znum(row.get('dobyy'),   2)
            lmnt_dd = safe_znum(row.get('lstmntdd'),2)
            lmnt_mm = safe_znum(row.get('lstmntmm'),2)
            lmnt_cc = safe_znum(row.get('lstmntcc'),2)
            lmnt_yy = safe_znum(row.get('lstmntyy'),2)
            lchg_dd = safe_znum(row.get('lstchgdd'),2)
            lchg_mm = safe_znum(row.get('lstchgmm'),2)
            lchg_cc = safe_znum(row.get('lstchgcc'),2)
            lchg_yy = safe_znum(row.get('lstchgyy'),2)

            # Build record
            buf[0:3]    = ficode.ljust(3)[:3].encode('ascii', errors='replace')
            buf[6:9]    = safe_znum(row.get('branch'),3).encode('ascii')
            buf[9:12]   = safe_znum(row.get('apcode'),3).encode('ascii')
            buf[12:32]  = safe_num_str(row.get('custno'),20).encode('ascii', errors='replace')
            buf[32:42]  = safe_num_str(row.get('acctno'),10).encode('ascii', errors='replace')
            buf[42:182] = namekey[:140].encode('ascii', errors='replace')
            buf[182:332]= custname.ljust(150)[:150].encode('ascii', errors='replace')
            # @0333 BUSSREG $20.
            buf[332:352]= bussreg.ljust(20)[:20].encode('ascii', errors='replace')
            buf[352:354]= dob_dd.encode('ascii')
            buf[354:356]= dob_mm.encode('ascii')
            buf[356:358]= dob_cc.encode('ascii')
            buf[358:360]= dob_yy.encode('ascii')
            # @0361 REGCOUTY $2.
            buf[360:362]= regcouty[:2].encode('ascii', errors='replace')
            # @0363 BUMI $1.
            buf[362:363]= bumi[:1].encode('ascii', errors='replace')
            # @0364 NRCC 1.
            buf[363:364]= str(nrcc)[:1].encode('ascii')
            # @0365 BASICGC 2.
            buf[364:366]= str(basicgc).rjust(2).encode('ascii')
            buf[366:368]= lmnt_dd.encode('ascii')
            buf[368:370]= lmnt_mm.encode('ascii')
            buf[370:372]= lmnt_cc.encode('ascii')
            buf[372:374]= lmnt_yy.encode('ascii')
            # @0375 ADDRLN1 $40. .. @0495 ADDRLN4 $40.
            for i, akey in enumerate(['addrln1','addrln2','addrln3','addrln4']):
                start = 374 + i * 40
                aval  = coalesce_str(row.get(akey),' ').ljust(40)[:40]
                buf[start:start+40] = aval.encode('ascii', errors='replace')
            buf[534:536]= lchg_dd.encode('ascii')
            buf[536:538]= lchg_mm.encode('ascii')
            buf[538:540]= lchg_cc.encode('ascii')
            buf[540:542]= lchg_yy.encode('ascii')
            # @0543 COUNTRY $2.
            buf[542:544]= country.ljust(2)[:2].encode('ascii', errors='replace')
            # @0545 MAILCODE 5.
            buf[544:549]= safe_num_str(row.get('mailcode'),5).encode('ascii', errors='replace')
            # @0550 DISTRICT $20.
            buf[549:569]= district[:20].encode('ascii', errors='replace')
            # @0570 MAILSTAT $20.
            buf[569:589]= mailstat.ljust(20)[:20].encode('ascii', errors='replace')
            # @0590 PRIPHONE $20.
            buf[589:609]= coalesce_str(row.get('priphone'),' ').ljust(20)[:20].encode('ascii', errors='replace')
            # @0610 SECPHONE $20.
            buf[609:629]= coalesce_str(row.get('secphone'),' ').ljust(20)[:20].encode('ascii', errors='replace')
            # @0630 PHONE3 $20.
            buf[629:649]= phone3[:20].encode('ascii', errors='replace')
            # @0650 PHONE4 $20.
            buf[649:669]= phone4[:20].encode('ascii', errors='replace')
            # @0670 RESIDENT $1.
            buf[669:670]= resident[:1].encode('ascii', errors='replace')
            # @0671 COSTCTR 4.
            buf[670:674]= safe_num_str(row.get('costctr'),4).encode('ascii', errors='replace')

            f.write(buf.decode('ascii', errors='replace') + '\n')

# =============================================================================
# WRITE OUTPUT3 — CCRISOWN (LRECL=600)
# =============================================================================

def build_ccrisown(ind_df: pl.DataFrame, cor_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA ALLLOAN: SET CORLOAN INDLOAN;
    DATA CCRISOWN: SET ALLLOAN;
    Load OWNER from CISLN and CISDP, PROC SORT BY CUSTNO / OWNER.
    MERGE CCRISOWN + OWNER (OWNER=CUSTNO), IF A AND B.
    Filter: (RELATCD1='049' AND RELATCD2='049') OR (RELATCD1='050' AND RELATCD2='050') AND INDORG='O'
    """
    frames = []
    if not cor_df.is_empty():
        frames.append(cor_df)
    if not ind_df.is_empty():
        frames.append(ind_df)
    allloan = pl.concat(frames, how='diagonal') if frames else pl.DataFrame()

    # PROC SORT DATA=CCRISOWN; BY CUSTNO;
    ccrisown = allloan.sort('custno')

    # DATA OWNER: SET CISLN.OWNER CISDP.OWNER; OUTPUT;
    con = duckdb.connect()
    cisln_owner = con.execute(f"SELECT * FROM read_parquet('{CISLN_OWNER_PARQUET}')").pl()
    cisdp_owner = con.execute(f"SELECT * FROM read_parquet('{CISDP_OWNER_PARQUET}')").pl()
    con.close()
    owner_df = pl.concat([cisln_owner, cisdp_owner], how='diagonal')

    # PROC SORT DATA=OWNER; BY OWNER;
    owner_df = owner_df.sort('owner')

    # MERGE CCRISOWN(IN=A) OWNER(IN=B DROP=CUSTNO RENAME=(OWNER=CUSTNO)) BY CUSTNO
    # Drop CUSTNO from owner, rename OWNER -> CUSTNO
    if 'custno' in owner_df.columns:
        owner_df = owner_df.drop('custno')
    owner_df = owner_df.rename({'owner': 'custno'})
    owner_df = owner_df.sort('custno')

    merged = ccrisown.join(owner_df, on='custno', how='inner', suffix='_ow')
    for col in owner_df.columns:
        ow_col = f"{col}_ow"
        if ow_col in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(ow_col).is_not_null())
                  .then(pl.col(ow_col))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(ow_col)

    # Filter: ((RELATCD1='049' AND RELATCD2='049') OR (RELATCD1='050' AND RELATCD2='050')) AND INDORG='O'
    merged = merged.filter(
        (
            ((pl.col('relatcd1') == '049') & (pl.col('relatcd2') == '049')) |
            ((pl.col('relatcd1') == '050') & (pl.col('relatcd2') == '050'))
        ) &
        (pl.col('indorg') == 'O')
    )
    return merged

def write_ccrisown(corpown: pl.DataFrame, output_path: str):
    """
    DATA CCRISOWN: SET CORPOWN; compute derived; FILE OUTPUT3; PUT ...
    LRECL=600, fixed-width.
    """
    LRECL = 600

    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        for row in corpown.iter_rows(named=True):
            buf = bytearray(b' ' * LRECL)

            ficodi   = ' ' * 6
            namekey  = '  '.ljust(140)
            custname = compress_chars(coalesce_str(row.get('custname')), '\\')

            oldic = coalesce_str(row.get('oldic'), ' ').ljust(20)[:20]
            newic = coalesce_str(row.get('newic'), ' ').ljust(20)[:20]
            if oldic.strip() == '':
                oldic = newic
                newic = ' ' * 20

            # @0001 FICODI $6.
            buf[0:6]    = ficodi[:6].encode('ascii', errors='replace')
            # @0007 BRANCH Z3.
            buf[6:9]    = safe_znum(row.get('branch'),3).encode('ascii')
            # @0010 APCODE Z3.
            buf[9:12]   = safe_znum(row.get('apcode'),3).encode('ascii')
            # @0013 CUSTNO 20.
            buf[12:32]  = safe_num_str(row.get('custno'),20).encode('ascii', errors='replace')
            # @0033 ACCTNO 10.
            buf[32:42]  = safe_num_str(row.get('acctno'),10).encode('ascii', errors='replace')
            # @0043 NAMEKEY $140.
            buf[42:182] = namekey[:140].encode('ascii', errors='replace')
            # @0183 CUSTNAME $150.
            buf[182:332]= custname.ljust(150)[:150].encode('ascii', errors='replace')
            # @0333 OLDIC $20.
            buf[332:352]= oldic[:20].encode('ascii', errors='replace')
            # @0353 OWNERNAM $150.
            buf[352:502]= coalesce_str(row.get('ownernam'),' ').ljust(150)[:150].encode('ascii', errors='replace')
            # @0503 OWNERID1 $20.
            buf[502:522]= coalesce_str(row.get('ownerid1'),' ').ljust(20)[:20].encode('ascii', errors='replace')
            # @0523 OWNERID2 $20.
            buf[522:542]= coalesce_str(row.get('ownerid2'),' ').ljust(20)[:20].encode('ascii', errors='replace')
            # @0543 OWNERDD $2.
            buf[542:544]= coalesce_str(row.get('ownerdd'),' ').ljust(2)[:2].encode('ascii', errors='replace')
            # @0545 OWNERMM $2.
            buf[544:546]= coalesce_str(row.get('ownermm'),' ').ljust(2)[:2].encode('ascii', errors='replace')
            # @0547 OWNERCC $2.
            buf[546:548]= coalesce_str(row.get('ownercc'),' ').ljust(2)[:2].encode('ascii', errors='replace')
            # @0549 OWNERYY $2.
            buf[548:550]= coalesce_str(row.get('owneryy'),' ').ljust(2)[:2].encode('ascii', errors='replace')
            # @0551 COSTCTR 4.
            buf[550:554]= safe_num_str(row.get('costctr'),4).encode('ascii', errors='replace')

            f.write(buf.decode('ascii', errors='replace') + '\n')

# =============================================================================
# PROC PRINT DATA=DATES (diagnostic / SASLIST output — written to stdout)
# =============================================================================

def print_dates(rv: dict):
    """PROC PRINT DATA=DATES — print report date info."""
    print("PROC PRINT DATA=DATES")
    print(f"  REPTDATE : {rv['reptdate']}")
    print(f"  WK       : {rv['wk']}")
    print(f"  WK1      : {rv['wk1']}")
    print(f"  REPTMON  : {rv['reptmon']}")
    print(f"  REPTMON1 : {rv['reptmon1']}")
    print(f"  REPTYEAR : {rv['reptyear']}")
    print(f"  REPTYEA2 : {rv['reptyea2']}")
    print(f"  RDATE    : {rv['rdate']}")
    print(f"  REPTDAY  : {rv['reptday']}")
    print(f"  SDATE    : {rv['sdate']}")

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("EIBWPBC4: Starting CCRIS output generation...")

    # DATA DATES: read REPTDATE, write OUTDATE file
    rv = get_report_vars(PBIF_REPTDATE_PARQUET)
    write_outdate(rv, OUTDATE_TXT)
    print(f"  Written: {OUTDATE_TXT}")

    # PROC PRINT DATA=DATES
    print_dates(rv)

    # DATA PBIF: load CLIEN file
    pbif = load_pbif(rv)
    print(f"  PBIF records: {len(pbif)}")

    # PROC SORT BNM.LNACCT -> BTACC; MERGE PBIF + BTACC; IF A AND B
    btacc = load_btacc()
    pbif  = merge_pbif_btacc(pbif, btacc)
    print(f"  PBIF after merge with BTACC: {len(pbif)}")

    # Load RELATION and CISLOAN; build LOANA
    relation = load_relation()
    cisloan  = load_cisloan()
    loana    = build_loana(relation, cisloan)
    print(f"  LOANA: {len(loana)}")

    # DATA LOAN: MERGE PBIF + LOANA; IF A AND B
    loan = merge_loan_loana(pbif, loana)
    print(f"  LOAN after merge with LOANA: {len(loan)}")

    # Build NOCUSTOM counts and final LOAN with APCODE=000
    loan = build_final_loan(loan)
    print(f"  LOAN final: {len(loan)}")

    # DATA INDLOAN / CORLOAN: split and apply IC processing
    indloan, corloan = split_ind_cor(loan)
    print(f"  INDLOAN: {len(indloan)}, CORLOAN: {len(corloan)}")

    # DATA ALLLOAN: SET CORLOAN INDLOAN (used for CCRISOWN)
    # DATA INDCCRIS: FILE OUTPUT1
    write_indccris(indloan, OUTPUT1_TXT)
    print(f"  Written: {OUTPUT1_TXT}")

    # DATA CORCCRIS: FILE OUTPUT2
    write_corccris(corloan, OUTPUT2_TXT)
    print(f"  Written: {OUTPUT2_TXT}")

    # PROC DATASETS LIB=WORK; DELETE CORCCRIS; (in-memory, no-op in Python)

    # DATA CCRISOWN + OWNER merge -> CORPOWN -> OUTPUT3
    corpown = build_ccrisown(indloan, corloan)
    print(f"  CORPOWN: {len(corpown)}")

    write_ccrisown(corpown, OUTPUT3_TXT)
    print(f"  Written: {OUTPUT3_TXT}")

    print("EIBWPBC4: Processing complete.")


if __name__ == '__main__':
    main()
