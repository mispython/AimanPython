# !/usr/bin/env python3
"""
Program Name: EIDETRTS
Purpose: Extract Remittance Transaction IFS for DETICA (AML system)
         - Reads RENTAS (Real-time Electronic Transfer of Funds and Securities) data
         - Classifies transactions into INWARD, OUTWARD, and BT (Bank Transfer)
         - Enriches with account, CIS customer, and branch information
         - Outputs pipe-delimited (0x1D) text file for DETICA AML system
         - 2017-2058: Filters out internal PBB/PIBB entity transactions
"""

import os
import re
from datetime import date, datetime
from typing import Optional

import duckdb
import polars as pl

# %INC PGM(PBBELF) — branch code format references
from PBBELF import BRCHCD_MAP, BRCHRVR_MAP, format_brchcd

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

# Input parquet paths
DP_REPTDATE_PARQUET       = "input/dp/reptdate.parquet"
RENTAS_PREFIX             = "input/rentas/rentas"       # rentas<MM><WK><YY>.parquet
LN_LNNOTE_PARQUET         = "input/ln/lnnote.parquet"
ILN_LNNOTE_PARQUET        = "input/iln/lnnote.parquet"
DP_CURRENT_PARQUET        = "input/dp/current.parquet"
DP_SAVING_PARQUET         = "input/dp/saving.parquet"
DP_FD_PARQUET             = "input/dp/fd.parquet"
DP_UMA_PARQUET            = "input/dp/uma.parquet"
DP_VOSTRO_PARQUET         = "input/dp/vostro.parquet"
IDP_CURRENT_PARQUET       = "input/idp/current.parquet"
IDP_SAVING_PARQUET        = "input/idp/saving.parquet"
IDP_FD_PARQUET            = "input/idp/fd.parquet"
IDP_UMA_PARQUET           = "input/idp/uma.parquet"
BT_MAST_PREFIX            = "input/bt/mast"            # mast<DD><MM>.parquet
BT_MAST2_PREFIX           = "input/bt/mast2"           # mast2<DD><MM>.parquet
IBT_IMAST_PREFIX          = "input/ibt/imast"          # imast<DD><MM>.parquet
IBT_IMAST2_PREFIX         = "input/ibt/imast2"         # imast2<DD><MM>.parquet
CIS_CUSTDLY_PARQUET       = "input/cis/custdly.parquet"

# Output paths
RENTRAN_TXT               = "output/detica/remtran_rentas_text.txt"
RENTRAN_BKP_TXT           = "output/detica/remtran_rentas_text_bkp.txt"

os.makedirs("output/detica", exist_ok=True)

# =============================================================================
# CONSTANTS
# =============================================================================

DELIM = '\x1d'   # '1D'X — ASCII group separator (pipe delimiter for DETICA)

# *2017-2058 — Internal PBB/PIBB entity name filter list
LIST_NAMES = {
    'PUBLIC BANK BHD COLOMBO BRANCH',
    'PUBLIC BANK VIETNAM LIMITED',
    'CAMBODIAN PUBLIC BANK PLC',
    'PUBLIC BANK VIENTIANE BR',
    'PB CARD SERVICES AC 1',
    'FIN DIV-BC NORM AC',
    'IBG COLLECTION ACCOUNT',
    'PUBLIC BANK (L) LTD',
    'PUBLIC MUTUAL BERHAD',
    'PB TRUSTEE SERVICES BERHAD',
    'PUBLIC BANK (HONG KONG) LIMITED',
    'AMANAHRAYA TRUSTEES BERHAD',
    'PUBLIC BANK BHD FOR COLLECTION A/C',
    'AKAUNTAN NEGARA MALAYSIA',
    'PUBLIC BANK',
    'PUBLIC BANK BERHAD',
    'PUBLIC BANK BHD',
    'PBB',
    'PUBLIC ISLAMIC BANK',
    'PUBLIC ISLAMIC BANK BERHAD',
    'PUBLIC ISLAMIC BANK BHD',
    'PIBB',
}

# 2022-1211 — Bank transactions to remove (zero-padded to 20 chars)
REMOVE_ORIGINATOR_IDS = {'0000000000000006463H', '0000000000000014328V'}

# =============================================================================
# UTILITY HELPERS
# =============================================================================

def coalesce_str(val, default: str = '') -> str:
    if val is None:
        return default
    return str(val)

def coalesce_num(val, default=0):
    if val is None or (isinstance(val, float) and val != val):
        return default
    return val

def sas_date_to_pydate(val) -> Optional[date]:
    if val is None or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + __import__('datetime').timedelta(days=int(val))
    if isinstance(val, (date, datetime)):
        return val if isinstance(val, date) else val.date()
    return None

def compress_str(s: str, remove_chars: str = '') -> str:
    """SAS COMPRESS(s, chars) — remove specified chars from string."""
    if not s:
        return ''
    for c in remove_chars:
        s = s.replace(c, '')
    return s

def compress_alnum(s: str) -> str:
    """Remove all non-alphanumeric chars (SAS COMPRESS with 'A' modifier keeps alnum)."""
    return re.sub(r'[^A-Za-z0-9]', '', s)

def strip_leading_zeros(s: str) -> str:
    """
    IFC(s=:'0', SUBSTR(s, VERIFY(s,'0')), s)
    Remove leading zeros. If result is empty, return original.
    """
    if not s:
        return s
    stripped = s.lstrip('0')
    return stripped if stripped else s

def catx(sep: str, *parts) -> str:
    """SAS CATX — concatenate non-blank parts with separator."""
    return sep.join(p for p in parts if p and str(p).strip())

def cats(*parts) -> str:
    """SAS CATS — concatenate all parts stripping trailing blanks."""
    return ''.join(str(p).strip() for p in parts if p is not None)

def left_pad_zero(s: str, width: int) -> str:
    """Left-pad string with zeros to given width."""
    return s.zfill(width)

def suppress_double_alias(s: str) -> str:
    """
    Remove '@@' double-alias occurrences:
    DO WHILE INDEX(s,'@@') > 0;
      ID_PART1 = SUBSTR(s, 1, pos-1);
      ID_PART2 = SUBSTR(s, pos+2, 40);
      s = CATX(' ', ID_PART1, ID_PART2);
    END;
    """
    while '@@' in s:
        idx = s.index('@@')
        part1 = s[:idx]
        part2 = s[idx + 2: idx + 2 + 40]
        s = catx(' ', part1, part2)
    return s

def fmt_brchcd(branch_id) -> str:
    """Apply $BRCHCD. format — branch number to branch abbreviation."""
    if branch_id is None:
        return ''
    try:
        key = int(branch_id)
    except (ValueError, TypeError):
        return coalesce_str(branch_id)
    return BRCHCD_MAP.get(key, '')

def fmt_brchrvr(branch_abbr: str) -> Optional[int]:
    """Apply $BRCHRVR. format — branch abbreviation to branch number."""
    if not branch_abbr:
        return None
    return BRCHRVR_MAP.get(branch_abbr.strip())

# =============================================================================
# GET REPORT DATE VARIABLES
# =============================================================================

def get_report_vars() -> dict:
    """Read REPTDATE from DP parquet, derive all macro variables."""
    con = duckdb.connect()
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{DP_REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate = sas_date_to_pydate(row[0])
    day      = reptdate.day
    month    = reptdate.month

    if day == 8:
        wk = '1'; wk1 = '4'
    elif day == 15:
        wk = '2'; wk1 = '1'
    elif day == 22:
        wk = '3'; wk1 = '2'
    else:
        wk = '4'; wk1 = '3'

    reptyear = reptdate.strftime('%y')   # YEAR2.
    reptmon  = str(month).zfill(2)       # Z2.
    reptday  = str(day).zfill(2)         # Z2.
    mm       = str(month).zfill(2)
    rdate    = reptdate.strftime('%Y%m%d')   # YYMMDDN8. -> YYYYMMDD 8-char numeric string
    yyyy     = reptdate.strftime('%Y')       # YEAR4.

    return {
        'reptdate': reptdate,
        'reptyear': reptyear,
        'reptmon':  reptmon,
        'reptday':  reptday,
        'mm':       mm,
        'nowk':     wk,
        'wk1':      wk1,
        'rdate':    rdate,
        'yyyy':     yyyy,
    }

# =============================================================================
# LOAD RENTAS SOURCE DATA
# =============================================================================

def load_rentas(rv: dict) -> pl.DataFrame:
    """SET RENTAS.RENTAS<MM><WK><YY>"""
    path = f"{RENTAS_PREFIX}{rv['reptmon']}{rv['nowk']}{rv['reptyear']}.parquet"
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    return df

# =============================================================================
# INITIAL RENTAS PROCESSING — derive common fields, split INWARD/OUTWARD/BT
# =============================================================================

def process_rentas(df: pl.DataFrame, rv: dict) -> tuple:
    """
    DATA INWARD OUTWARD BT: SET RENTAS.RENTAS...;
    Derive all common fields, apply LIST filter, split by ISTTYPE/STATUS.
    Returns: (inward_df, outward_df, bt_df)
    """
    rdate = rv['rdate']

    rows     = df.to_dicts()
    inward_rows  = []
    outward_rows = []
    bt_rows      = []

    for row in rows:
        # FORMAT / field initialisations
        row.setdefault('source_txn_unique_id', '')
        row.setdefault('run_timestamp',        '')
        row.setdefault('local_timestamp',      '')
        row.setdefault('trans_ref_desc',       '')
        row.setdefault('originator_name',      '')
        row.setdefault('beneficiary_name',     '')
        row.setdefault('org_unit_code',        '')

        # RUN_TIMESTAMP = COMPRESS(&RDATE || '000000')
        row['run_timestamp'] = compress_str(rdate + '000000')

        # SOURCE_TXN_UNIQUE_ID = COMPRESS('RM'||BANKNO||ISTTYPE||VALUEDTE||TRANSREF||UMRNO)
        bankno   = coalesce_str(row.get('bankno'))
        isttype  = coalesce_str(row.get('isttype'))
        valuedte = coalesce_str(row.get('valuedte'))
        transref = coalesce_str(row.get('transref'))
        umrno    = coalesce_str(row.get('umrno'))
        row['source_txn_unique_id'] = compress_str(
            'RM' + bankno + isttype + valuedte + transref + umrno
        )

        # BRANCH_ID = PUT(BRANCHABB, $BRCHRVR.)
        branchabb = coalesce_str(row.get('branchabb'))
        row['branch_id'] = fmt_brchrvr(branchabb)

        row['curcode']  = 'MYR'
        row['curbase']  = 'MYR'

        # ORIGINATION_DATE = VALUEDTE
        row['origination_date'] = valuedte

        # LOCAL_TIMESTAMP from TTIMESTAMP
        ttimestamp = coalesce_str(row.get('ttimestamp'))
        if len(ttimestamp) >= 18:
            yyyy_t = ttimestamp[0:4]
            mm_t   = ttimestamp[5:7]
            dd_t   = ttimestamp[8:10]
            hour_t = ttimestamp[11:13]
            min_t  = ttimestamp[14:16]
            sec_t  = ttimestamp[17:19]
            row['local_timestamp'] = compress_str(yyyy_t + mm_t + dd_t + hour_t + min_t + sec_t)
        else:
            row['local_timestamp'] = ''

        # POSTING_DATE from PTIMESTAMP
        ptimestamp = coalesce_str(row.get('ptimestamp'))
        if len(ptimestamp) >= 10:
            yyyy_p = ptimestamp[0:4]
            mm_p   = ptimestamp[5:7]
            dd_p   = ptimestamp[8:10]
            if yyyy_p == '0101':
                row['posting_date'] = row['local_timestamp'][0:8] if len(row['local_timestamp']) >= 8 else ''
            else:
                row['posting_date'] = compress_str(yyyy_p + mm_p + dd_p)
        else:
            row['posting_date'] = ''

        # TXN_AMOUNT_ORIG = AMOUNT
        row['txn_amount_orig'] = row.get('amount')

        # TRANS_REF_DESC = CATX(' ', PAYMODE, TRANSREF)
        paymode = coalesce_str(row.get('paymode'))
        row['trans_ref_desc'] = catx(' ', paymode, transref)

        row['mention']          = paymode
        row['txn_status_code']  = coalesce_str(row.get('status'))
        row['channel']          = 999

        # ORIGINATOR_NAME = CATX(',', APPLNAME, APPLNAME2)
        row['originator_name'] = catx(',',
            coalesce_str(row.get('applname')),
            coalesce_str(row.get('applname2'))
        )
        # BENEFICIARY_NAME = CATX(',', BENENAME, BENENAME2)
        row['beneficiary_name'] = catx(',',
            coalesce_str(row.get('benename')),
            coalesce_str(row.get('benename2'))
        )

        row['teller_id']               = coalesce_str(row.get('userid'))
        row['originator_id']           = coalesce_str(row.get('applid'))
        row['beneficiary_id']          = coalesce_str(row.get('trackcode'))
        row['remittance_ref_no']       = transref
        row['incoming_outgoing_trans'] = coalesce_str(row.get('status'))
        row['employee_id']             = 88888

        # *2017-2058 — Internal entity filter
        applname = coalesce_str(row.get('applname')).upper().strip()
        benename = coalesce_str(row.get('benename')).upper().strip()
        if applname in LIST_NAMES or benename in LIST_NAMES:
            continue

        # ORG_UNIT_CODE
        if branchabb in ('701', '702', 'IKB', 'IPJ'):
            row['org_unit_code'] = 'PIBBTRSRY'
        else:
            row['org_unit_code'] = 'PBBTRSRY'

        # Route to INWARD / OUTWARD / BT
        status = coalesce_str(row.get('status'))
        if isttype == 'RI' and status == 'TI':
            inward_rows.append(row)
        elif isttype == 'RO' and status == 'TO':
            outward_rows.append(row)
        elif isttype == 'BT' and status == 'TO':
            bt_rows.append(row)

    inward_df  = pl.from_dicts(inward_rows)  if inward_rows  else pl.DataFrame()
    outward_df = pl.from_dicts(outward_rows) if outward_rows else pl.DataFrame()
    bt_df      = pl.from_dicts(bt_rows)      if bt_rows      else pl.DataFrame()

    return inward_df, outward_df, bt_df

# =============================================================================
# INWARD PROCESSING — split into INWARD1, LOANS_INWARD, INWARD2
# =============================================================================

def process_inward(inward_df: pl.DataFrame) -> tuple:
    """
    DATA INWARD1 LOANS_INWARD INWARD2: SET INWARD;
    Determines ACCOUNT_SOURCE_UNIQUE_ID based on BENEACCTNO matching logic.
    Returns: (inward1_df, loans_inward_df, inward2_df)
    """
    if inward_df.is_empty():
        empty = pl.DataFrame()
        return empty, empty, empty

    rows           = inward_df.to_dicts()
    inward1_rows   = []
    loans_inward_rows = []
    inward2_rows   = []

    for row in rows:
        row['txn_code']              = 'RMT001'
        row['crdr']                  = 'C'
        row['beneficiary_bank']      = coalesce_str(row.get('branchabb'))
        row['incoming_outgoing_flg'] = 'I'
        row['bene_branch']           = coalesce_str(row.get('branchabb'))
        row['sender_branch']         = '0'

        beneacctno = coalesce_str(row.get('beneacctno'))
        compress_bene = compress_alnum(beneacctno)
        # IFC(compress_bene=:'0', SUBSTR(..., VERIFY(...,'0')), compress_bene)
        if compress_bene.startswith('0'):
            compress_bene = strip_leading_zeros(compress_bene)
        row['compress_bene'] = compress_bene
        bene_ac_len = len(compress_bene)
        row['bene_ac_len'] = bene_ac_len

        temp_bene_ac = compress_bene[:10]
        row['temp_bene_ac'] = temp_bene_ac

        acctno = compress_str(coalesce_str(row.get('acctno')))

        if temp_bene_ac == acctno:
            first_digit = temp_bene_ac[0:1]
            if first_digit in ('2', '8'):
                temp_acctcode = 'LN'
            else:
                temp_acctcode = 'DP'
            row['temp_acctcode'] = temp_acctcode
            row['account_source_unique_id'] = compress_str(temp_acctcode + temp_bene_ac)
            inward1_rows.append(row)
        else:
            if bene_ac_len == 19 or (bene_ac_len == 15 and
               beneacctno[0:1] == '2'):
                # LOANS WITH NOTENO AND TRAILING 0000 OR 0001
                if len(compress_bene) >= 6 and compress_bene[4:5] in ('2', '8'):
                    row['account_source_unique_id'] = compress_str(
                        'LN' + compress_bene[4:19]
                    )
                    loans_inward_rows.append(row)
                else:
                    inward2_rows.append(row)
            else:
                inward2_rows.append(row)  # REQUIRE FURTHER CHECKING

    inward1_df        = pl.from_dicts(inward1_rows)      if inward1_rows      else pl.DataFrame()
    loans_inward_df   = pl.from_dicts(loans_inward_rows) if loans_inward_rows else pl.DataFrame()
    inward2_df        = pl.from_dicts(inward2_rows)      if inward2_rows      else pl.DataFrame()

    return inward1_df, loans_inward_df, inward2_df

def process_inward2(inward2_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA INWARD2: SET INWARD2;
    IF TEMP_BENE_AC NOT IN ('0','') THEN build ACCOUNT_SOURCE_UNIQUE_ID.
    """
    if inward2_df.is_empty():
        return inward2_df

    rows = inward2_df.to_dicts()
    for row in rows:
        temp_bene_ac = coalesce_str(row.get('temp_bene_ac'))
        if temp_bene_ac not in ('0', ''):
            first_digit = temp_bene_ac[0:1]
            if first_digit in ('2', '8'):
                temp_acctcode = 'LN'
            else:
                temp_acctcode = 'DP'
            row['temp_acctcode'] = temp_acctcode
            row['account_source_unique_id'] = compress_str(temp_acctcode + temp_bene_ac)

    return pl.from_dicts(rows)

# =============================================================================
# OUTWARD PROCESSING
# =============================================================================

def process_outward(outward_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA OUTWARD: SET OUTWARD;
    Derive TXN_CODE, CRDR, ACCOUNT_SOURCE_UNIQUE_ID from APPLACCTNO.
    """
    if outward_df.is_empty():
        return outward_df

    rows = outward_df.to_dicts()
    for row in rows:
        row['txn_code']              = 'RMT002'
        row['crdr']                  = 'D'
        row['originator_bank']       = coalesce_str(row.get('branchabb'))
        row['incoming_outgoing_flg'] = 'O'
        row['bene_branch']           = '0'
        row['sender_branch']         = coalesce_str(row.get('branchabb'))

        applacctno = coalesce_str(row.get('applacctno'))
        compress_appl = compress_alnum(applacctno)
        if compress_appl.startswith('0'):
            compress_appl = strip_leading_zeros(compress_appl)
        row['compress_appl'] = compress_appl
        appl_ac_len = len(compress_appl)
        row['appl_ac_len'] = appl_ac_len

        if appl_ac_len == 10:
            first_char = applacctno[0:1] if applacctno else ''
            if first_char in ('1', '3', '4', '6'):
                temp_acctcode = 'DP'
            else:
                temp_acctcode = 'LN'
            row['temp_acctcode'] = temp_acctcode
            row['account_source_unique_id'] = compress_str(temp_acctcode + applacctno)
        # * ELSE USE HARDCODED ACCOUNT NO

    return pl.from_dicts(rows)

# =============================================================================
# BT PROCESSING
# =============================================================================

def process_bt_initial(bt_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA BT: SET BT;
    Derive fields, compute ACCTNOX.
    """
    if bt_df.is_empty():
        return bt_df

    rows = bt_df.to_dicts()
    for row in rows:
        row['txn_code']              = 'RMT002'
        row['crdr']                  = 'D'
        row['originator_bank']       = coalesce_str(row.get('branchabb'))
        row['incoming_outgoing_flg'] = 'O'
        row['bene_branch']           = '0'
        row['sender_branch']         = coalesce_str(row.get('branchabb'))

        applacctno = coalesce_str(row.get('applacctno'))
        compress_appl = compress_alnum(applacctno)
        if compress_appl.startswith('0'):
            compress_appl = strip_leading_zeros(compress_appl)
        row['compress_appl']  = compress_appl
        row['appl_ac_len']    = len(compress_appl)

        # Commented-out logic from SAS:
        # IF APPL_AC_LEN = 10 AND SUBSTR(APPLACCTNO,1,1) ^= '0' THEN DO;
        #   IF SUBSTR(APPLACCTNO,1,1) = '2' THEN TEMP_ACCTCODE='LN';
        #   ACCOUNT_SOURCE_UNIQUE_ID = COMPRESS(TEMP_ACCTCODE||APPLACCTNO);
        # END;

        # LENGTH ACCTNOX $10.; ACCTNOX derivation
        acctnox = ''
        if len(applacctno) >= 2 and applacctno[0:2] == '25':
            acctnox = applacctno[:10]
        elif len(applacctno) >= 3 and applacctno[0:3] == '285':
            acctnox = applacctno[:10]
        row['acctnox'] = acctnox

        row['account_source_unique_id']  = 'RMT00001A'
        row['customer_source_unique_id'] = 'RMT00001C'
        # * ELSE USE HARDCODED ACCOUNT NO

    return pl.from_dicts(rows)

def load_btmast(rv: dict) -> pl.DataFrame:
    """
    DATA BTMAST: SET BT.MAST<DD><MM> BT.MAST2<DD><MM>
                     IBT.IMAST<DD><MM> IBT.IMAST2<DD><MM>;
    IF SUBSTR(ACCTNOX,1,2)='25' OR SUBSTR(ACCTNOX,1,3)='285';
    ACCTBRCH = FICODE; KEEP ACCTNOX ACCTBRCH;
    PROC SORT NODUPKEY BY ACCTNOX;
    """
    dd, mm = rv['reptday'], rv['reptmon']
    paths  = [
        f"{BT_MAST_PREFIX}{dd}{mm}.parquet",
        f"{BT_MAST2_PREFIX}{dd}{mm}.parquet",
        f"{IBT_IMAST_PREFIX}{dd}{mm}.parquet",
        f"{IBT_IMAST2_PREFIX}{dd}{mm}.parquet",
    ]
    con    = duckdb.connect()
    frames = []
    for p in paths:
        if os.path.exists(p):
            frames.append(con.execute(f"SELECT * FROM read_parquet('{p}')").pl())
    con.close()

    if not frames:
        return pl.DataFrame(schema={'acctnox': pl.Utf8, 'acctbrch': pl.Utf8})

    btmast = pl.concat(frames, how='diagonal')
    # IF SUBSTR(ACCTNOX,1,2)='25' OR SUBSTR(ACCTNOX,1,3)='285'
    btmast = btmast.filter(
        pl.col('acctnox').str.starts_with('25') |
        pl.col('acctnox').str.starts_with('285')
    )
    btmast = btmast.with_columns(
        pl.col('ficode').alias('acctbrch')
    ).select(['acctnox', 'acctbrch'])

    return btmast.unique(subset=['acctnox'], keep='first').sort('acctnox')

def merge_bt_btmast(bt_df: pl.DataFrame, btmast: pl.DataFrame) -> pl.DataFrame:
    """
    DATA BT: MERGE BT(IN=A) BTMAST(IN=B); BY ACCTNOX; IF A;
    IF A AND B: BRANCH_ID=ACCTBRCH; SENDER_BRANCH=PUT(ACCTBRCH,BRCHCD.);
    """
    if bt_df.is_empty():
        return bt_df

    merged = bt_df.sort('acctnox').join(
        btmast.sort('acctnox'), on='acctnox', how='left', suffix='_bm'
    )
    if 'acctbrch_bm' in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col('acctbrch_bm').is_not_null())
              .then(pl.col('acctbrch_bm'))
              .otherwise(pl.col('acctbrch') if 'acctbrch' in merged.columns else pl.lit(None))
              .alias('acctbrch')
        ).drop('acctbrch_bm')

    # IF A AND B: update BRANCH_ID and SENDER_BRANCH
    rows = merged.to_dicts()
    for row in rows:
        acctbrch = row.get('acctbrch')
        if acctbrch is not None and acctbrch != '':
            row['branch_id']     = acctbrch
            row['sender_branch'] = fmt_brchcd(acctbrch)

    return pl.from_dicts(rows)

# =============================================================================
# BUILD ACCOUNT MASTER
# =============================================================================

def build_loan_acct() -> tuple:
    """
    DATA LOAN: SET LN.LNNOTE ILN.LNNOTE;
    MNI_ACCTCODE='LN'; PRODUCT=LOANTYPE; PROD=COMPRESS('LN'||PUT(LOANTYPE,Z3.));
    ACCTBRCH=NTBRCH;
    PROC SORT LOAN_ACCT NODUPKEY BY ACCTNO NOTENO;
    PROC SORT CTR NODUPKEY BY ACCTNO (DROP=NOTENO);
    Returns: (loan_acct_df, ctr_df)
    """
    con = duckdb.connect()
    frames = []
    for p in [LN_LNNOTE_PARQUET, ILN_LNNOTE_PARQUET]:
        if os.path.exists(p):
            frames.append(con.execute(f"""
                SELECT acctno, noteno, costctr, loantype, ntbrch
                FROM read_parquet('{p}')
            """).pl())
    con.close()

    if not frames:
        empty = pl.DataFrame(schema={
            'acctno': pl.Int64, 'noteno': pl.Int64, 'mni_acctcode': pl.Utf8,
            'product': pl.Int64, 'prod': pl.Utf8, 'acctbrch': pl.Utf8,
            'account_source_unique_id': pl.Utf8
        })
        return empty, empty

    loan = pl.concat(frames, how='diagonal')

    rows = loan.to_dicts()
    mni_l = []; prod_l = []; acctbrch_l = []; asid_l = []
    for row in rows:
        loantype = coalesce_num(row.get('loantype'), 0)
        noteno   = row.get('noteno')
        acctno   = coalesce_num(row.get('acctno'), 0)
        # MNI_ACCTCODE='LN'; PRODUCT=LOANTYPE
        # PROD=COMPRESS('LN'||PUT(LOANTYPE,Z3.))
        mni_l.append('LN')
        prod_l.append(compress_str('LN' + str(loantype).zfill(3)))
        acctbrch_l.append(coalesce_str(row.get('ntbrch')))
        # ACCOUNT_SOURCE_UNIQUE_ID built in ACCT step
        if noteno is not None:
            asid_l.append(compress_str('LN' + str(acctno) + str(int(noteno)).zfill(5)))
        else:
            asid_l.append(compress_str('LN' + str(acctno)))

    loan = loan.with_columns([
        pl.Series('mni_acctcode',              mni_l,     dtype=pl.Utf8),
        pl.Series('prod',                      prod_l,    dtype=pl.Utf8),
        pl.Series('acctbrch',                  acctbrch_l,dtype=pl.Utf8),
        pl.Series('account_source_unique_id',  asid_l,    dtype=pl.Utf8),
        pl.col('loantype').alias('product'),
    ])

    loan_acct = loan.unique(subset=['acctno','noteno'], keep='first').sort(['acctno','noteno'])
    ctr       = loan.drop('noteno').unique(subset=['acctno'], keep='first').sort('acctno')

    return loan_acct, ctr

def build_depo_acct() -> pl.DataFrame:
    """
    DATA DEPO_ACCT: SET DP.CURRENT IDP.CURRENT DP.SAVING IDP.SAVING
                        DP.FD IDP.FD DP.UMA IDP.UMA DP.VOSTRO;
    MNI_ACCTCODE='DP'; PROD=COMPRESS('DP'||PUT(PRODUCT,Z3.)); ACCTBRCH=BRANCH;
    """
    con    = duckdb.connect()
    paths  = [
        DP_CURRENT_PARQUET, IDP_CURRENT_PARQUET,
        DP_SAVING_PARQUET,  IDP_SAVING_PARQUET,
        DP_FD_PARQUET,      IDP_FD_PARQUET,
        DP_UMA_PARQUET,     IDP_UMA_PARQUET,
        DP_VOSTRO_PARQUET,
    ]
    frames = []
    for p in paths:
        if os.path.exists(p):
            frames.append(con.execute(f"SELECT * FROM read_parquet('{p}')").pl())
    con.close()

    if not frames:
        return pl.DataFrame(schema={
            'acctno': pl.Int64, 'mni_acctcode': pl.Utf8,
            'product': pl.Int64, 'prod': pl.Utf8,
            'acctbrch': pl.Utf8, 'account_source_unique_id': pl.Utf8
        })

    depo = pl.concat(frames, how='diagonal')

    rows  = depo.to_dicts()
    mni_l = []; prod_l = []; acctbrch_l = []; asid_l = []
    for row in rows:
        product = coalesce_num(row.get('product'), 0)
        acctno  = coalesce_num(row.get('acctno'),  0)
        mni_l.append('DP')
        prod_l.append(compress_str('DP' + str(product).zfill(3)))
        acctbrch_l.append(str(coalesce_num(row.get('branch'), 0)))
        asid_l.append(compress_str('DP' + str(acctno)))

    return depo.with_columns([
        pl.Series('mni_acctcode',             mni_l,     dtype=pl.Utf8),
        pl.Series('prod',                     prod_l,    dtype=pl.Utf8),
        pl.Series('acctbrch',                 acctbrch_l,dtype=pl.Utf8),
        pl.Series('account_source_unique_id', asid_l,    dtype=pl.Utf8),
    ])

def build_acct_master(depo_acct: pl.DataFrame, loan_acct: pl.DataFrame,
                      ctr: pl.DataFrame) -> pl.DataFrame:
    """
    DATA ACCT: SET DEPO_ACCT LOAN_ACCT CTR;
    Rebuild ACCOUNT_SOURCE_UNIQUE_ID; BANK_ACC_IND='Y';
    KEEP relevant columns.
    """
    keep = ['acctno','noteno','mni_acctcode','bank_acc_ind','org_unit_code',
            'account_source_unique_id','prod','acctbrch']

    frames = []
    for df in [depo_acct, loan_acct, ctr]:
        if not df.is_empty():
            frames.append(df)

    if not frames:
        return pl.DataFrame()

    acct = pl.concat(frames, how='diagonal')

    rows = acct.to_dicts()
    asid_l = []; bank_l = []
    for row in rows:
        mni    = coalesce_str(row.get('mni_acctcode'))
        acctno = coalesce_num(row.get('acctno'), 0)
        noteno = row.get('noteno')
        if noteno is not None and not (isinstance(noteno, float) and noteno != noteno):
            asid = compress_str(mni + str(acctno) + str(int(noteno)).zfill(5))
        else:
            asid = compress_str(mni + str(acctno))
        asid_l.append(asid)
        bank_l.append('Y')

    acct = acct.with_columns([
        pl.Series('account_source_unique_id', asid_l, dtype=pl.Utf8),
        pl.Series('bank_acc_ind',             bank_l, dtype=pl.Utf8),
    ])

    existing_keep = [c for c in keep if c in acct.columns]
    return acct.select(existing_keep)

# =============================================================================
# COMBINE INWARD/OUTWARD + ACCT JOIN
# =============================================================================

def combine_inward_outward(inward1: pl.DataFrame, loans_inward: pl.DataFrame,
                           inward2: pl.DataFrame, outward: pl.DataFrame) -> pl.DataFrame:
    """DATA INWARD_OUTWARD: SET INWARD1 LOANS_INWARD INWARD2 OUTWARD;"""
    frames = [df for df in [inward1, loans_inward, inward2, outward] if not df.is_empty()]
    return pl.concat(frames, how='diagonal') if frames else pl.DataFrame()

def join_acct(inward_outward: pl.DataFrame, acct: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SQL: CREATE TABLE INWARD_OUTWARD AS
    SELECT T1.*, T2.BANK_ACC_IND, T2.MNI_ACCTCODE, T2.PROD, T2.ACCTBRCH
    FROM INWARD_OUTWARD T1 LEFT JOIN ACCT T2
    ON T1.ACCOUNT_SOURCE_UNIQUE_ID = T2.ACCOUNT_SOURCE_UNIQUE_ID;
    """
    if inward_outward.is_empty() or acct.is_empty():
        return inward_outward

    acct_sel = acct.select([c for c in ['account_source_unique_id','bank_acc_ind',
                                         'mni_acctcode','prod','acctbrch']
                             if c in acct.columns])

    merged = inward_outward.join(acct_sel, on='account_source_unique_id',
                                 how='left', suffix='_ac')
    for col in ['bank_acc_ind','mni_acctcode','prod','acctbrch']:
        ac_col = f"{col}_ac"
        if ac_col in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(ac_col).is_not_null())
                  .then(pl.col(ac_col))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(ac_col)
    return merged

def apply_branch_override(inward_outward: pl.DataFrame) -> pl.DataFrame:
    """
    DATA INWARD_OUTWARD: SET INWARD_OUTWARD;
    IF (BRANCHABB='EBK' AND PAYMODE='DR A/C') OR
       (BRANCHABB='CPC' AND PAYMODE IN ('AUTO CR','INWARD'))
    THEN branch override logic.
    """
    if inward_outward.is_empty():
        return inward_outward

    rows = inward_outward.to_dicts()
    for row in rows:
        branchabb = coalesce_str(row.get('branchabb')).strip()
        paymode   = coalesce_str(row.get('paymode')).strip()
        acctbrch  = row.get('acctbrch')
        isttype   = coalesce_str(row.get('isttype'))

        if (branchabb == 'EBK' and paymode == 'DR A/C') or \
           (branchabb == 'CPC' and paymode in ('AUTO CR', 'INWARD')):
            if acctbrch is not None and acctbrch != '' and acctbrch != '0':
                row['branch_id'] = acctbrch
                if isttype == 'RI':
                    row['bene_branch']   = fmt_brchcd(acctbrch)
                elif isttype == 'RO':
                    row['sender_branch'] = fmt_brchcd(acctbrch)

    return pl.from_dicts(rows)

# =============================================================================
# MERGE BT INTO INWARD_OUTWARD AND APPLY RULES
# =============================================================================

def build_tran_inward_outward(inward_outward: pl.DataFrame,
                               bt_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA TRAN.INWARD_OUTWARD: SET INWARD_OUTWARD(IN=A) BT(IN=B);
    A: if BANK_ACC_IND ^= 'Y' -> set hardcoded IDs based on direction
    B: PROD='RT108'
    TEMP_ACCT_SOURCE = SUBSTR(ACCOUNT_SOURCE_UNIQUE_ID,1,12)
    """
    frames = []
    if not inward_outward.is_empty():
        inward_outward = inward_outward.with_columns(pl.lit('A').alias('_src'))
        frames.append(inward_outward)
    if not bt_df.is_empty():
        bt_df = bt_df.with_columns(pl.lit('B').alias('_src'))
        frames.append(bt_df)

    if not frames:
        return pl.DataFrame()

    combined = pl.concat(frames, how='diagonal')
    rows     = combined.to_dicts()

    for row in rows:
        src       = row.get('_src', 'A')
        bank_acc  = coalesce_str(row.get('bank_acc_ind'))
        flag_dir  = coalesce_str(row.get('incoming_outgoing_flg'))

        if src == 'A':
            if bank_acc != 'Y':
                # *ORG_UNIT_CODE = 'PBB'
                if flag_dir == 'O':
                    row['account_source_unique_id']  = 'RMT00001A'
                    row['customer_source_unique_id'] = 'RMT00001C'
                    row['prod'] = 'RT108'
                else:
                    row['account_source_unique_id']  = 'RMT00002A'
                    row['customer_source_unique_id'] = 'RMT00002C'
                    row['prod'] = 'RT109'
        elif src == 'B':
            row['prod'] = 'RT108'

        asid = coalesce_str(row.get('account_source_unique_id'))
        row['temp_acct_source'] = asid[:12]

    combined = pl.from_dicts(rows).drop('_src')
    return combined

# =============================================================================
# CIS CUSTOMER LOOKUP
# =============================================================================

def build_cis() -> pl.DataFrame:
    """
    DATA CIS: SET CIS.CUSTDLY(KEEP=ACCTCODE ACCTNO CUSTNO ALIAS PRISEC);
    WHERE PRISEC=901 AND ACCTCODE IN ('DP','LN');
    ACCOUNT_SOURCE_UNIQUE_ID = COMPRESS(ACCTCODE||ACCTNO);
    CIS = COMPRESS('CIS'||CUSTNO);
    PROC SORT NODUPKEY BY ACCTNO;
    """
    con = duckdb.connect()
    cis = con.execute(f"""
        SELECT acctcode, acctno, custno, alias, prisec
        FROM read_parquet('{CIS_CUSTDLY_PARQUET}')
        WHERE prisec = 901 AND acctcode IN ('DP', 'LN')
    """).pl()
    con.close()

    if cis.is_empty():
        return pl.DataFrame(schema={
            'acctno': pl.Int64, 'cis': pl.Utf8,
            'alias': pl.Utf8, 'account_source_unique_id': pl.Utf8
        })

    rows   = cis.to_dicts()
    asid_l = []; cis_l = []
    for row in rows:
        acctcode = coalesce_str(row.get('acctcode'))
        acctno   = coalesce_num(row.get('acctno'), 0)
        custno   = coalesce_num(row.get('custno'), 0)
        asid_l.append(compress_str(acctcode + str(acctno)))
        cis_l.append(compress_str('CIS' + str(custno)))

    cis = cis.with_columns([
        pl.Series('account_source_unique_id', asid_l, dtype=pl.Utf8),
        pl.Series('cis',                      cis_l,  dtype=pl.Utf8),
    ])
    return cis.unique(subset=['acctno'], keep='first').sort('acctno')

def join_cis(tran_df: pl.DataFrame, cis: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SQL: CREATE TABLE TRAN.RENTAS_CIS AS
    SELECT T1.*, T2.CIS, T2.ALIAS, T2.ACCOUNT_SOURCE_UNIQUE_ID AS CIS_ACCTNO
    FROM TRAN.INWARD_OUTWARD T1 LEFT JOIN CIS T2
    ON T1.TEMP_ACCT_SOURCE = T2.ACCOUNT_SOURCE_UNIQUE_ID;
    """
    if tran_df.is_empty() or cis.is_empty():
        return tran_df

    cis_sel = cis.select(['account_source_unique_id','cis','alias']).rename(
        {'account_source_unique_id': 'cis_acctno'}
    ).with_columns(pl.col('cis_acctno').alias('_join_key'))

    merged = tran_df.join(
        cis_sel.rename({'_join_key': 'temp_acct_source'}),
        on='temp_acct_source', how='left', suffix='_cis'
    )
    for col in ['cis','alias']:
        cc = f"{col}_cis"
        if cc in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(cc).is_not_null())
                  .then(pl.col(cc))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(cc)
    return merged

def apply_cis_fallback(tran_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA TRAN.RENTAS_CIS: SET TRAN.RENTAS_CIS;
    IF CIS ^= ' ' THEN CUSTOMER_SOURCE_UNIQUE_ID = CIS;
    ELSE: apply hardcoded IDs based on direction.
    """
    if tran_df.is_empty():
        return tran_df

    rows = tran_df.to_dicts()
    for row in rows:
        cis      = coalesce_str(row.get('cis'))
        flag_dir = coalesce_str(row.get('incoming_outgoing_flg'))

        if cis.strip():
            row['customer_source_unique_id'] = cis
        else:
            if flag_dir == 'O':
                row['customer_source_unique_id'] = 'RMT00001C'
                row['account_source_unique_id']  = 'RMT00001A'
                # *ORG_UNIT_CODE = 'PBB'
                row['prod'] = 'RT108'
            else:
                row['customer_source_unique_id'] = 'RMT00002C'
                row['account_source_unique_id']  = 'RMT00002A'
                # *ORG_UNIT_CODE = 'PBB'
                row['prod'] = 'RT109'

    return pl.from_dicts(rows)

# =============================================================================
# FINAL OUTPUT DATASET PREPARATION
# =============================================================================

def build_out(tran_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA OUT: SET TRAN.RENTAS_CIS;
    Populate BENEFICIARY_ID / ORIGINATOR_ID from ALIAS if empty;
    Fall back to NAME if still empty;
    DELETE if either is empty;
    Suppress '@@' double-alias;
    2022-1211 REMOVE BANK TRANSACTIONS;
    """
    if tran_df.is_empty():
        return tran_df

    rows    = tran_df.to_dicts()
    out     = []

    for row in rows:
        flag_dir     = coalesce_str(row.get('incoming_outgoing_flg'))
        alias        = coalesce_str(row.get('alias'))
        bene_id      = coalesce_str(row.get('beneficiary_id'))
        orig_id      = coalesce_str(row.get('originator_id'))
        bene_name    = coalesce_str(row.get('beneficiary_name'))
        orig_name    = coalesce_str(row.get('originator_name'))

        # Fill from ALIAS if blank
        if flag_dir == 'I' and not bene_id.strip():
            if alias.strip():
                bene_id = alias
        elif flag_dir == 'O' and not orig_id.strip():
            if alias.strip():
                orig_id = alias

        # Fallback to NAME
        if not bene_id.strip():
            bene_id = bene_name
        if not orig_id.strip():
            orig_id = orig_name

        # DELETE if either is blank
        if not bene_id.strip() or not orig_id.strip():
            continue

        # Suppress double alias '@@'
        orig_id = suppress_double_alias(orig_id)
        bene_id = suppress_double_alias(bene_id)

        row['beneficiary_id'] = bene_id
        row['originator_id']  = orig_id

        # 2022-1211 REMOVE BANK TRANSACTIONS
        padded_orig = left_pad_zero(orig_id, 20)
        if padded_orig in REMOVE_ORIGINATOR_IDS:
            continue

        out.append(row)

    return pl.from_dicts(out) if out else pl.DataFrame()

# =============================================================================
# WRITE OUTPUT — RENTRAN delimited file
# =============================================================================

def write_rentran(out_df: pl.DataFrame, rv: dict, output_path: str):
    """
    DATA _NULL_: SET OUT; FILE RENTRAN;
    DELIM = '1D'X;
    Write 75-field pipe (0x1D) delimited records.
    SOURCE_TXN_UNIQUE_ID regenerated as 'RTS'||RDATE||COUNT(Z10.)
    """
    if out_df.is_empty():
        with open(output_path, 'w', encoding='utf-8') as f:
            pass
        return

    rdate = rv['rdate']

    with open(output_path, 'w', encoding='utf-8', newline='\n') as f:
        for count, row in enumerate(out_df.iter_rows(named=True), start=1):
            # SOURCE_TXN_UNIQUE_ID = COMPRESS('RTS'||RDATE||PUT(COUNT,Z10.))
            source_txn_uid = compress_str('RTS' + rdate + str(count).zfill(10))

            def f_str(key, default=''):
                v = row.get(key)
                return str(v).strip() if v is not None else default

            def f_num(key, default=''):
                v = row.get(key)
                if v is None or (isinstance(v, float) and v != v):
                    return default
                return str(v)

            fields = [
                f_str('run_timestamp'),            #  1
                source_txn_uid,                    #  2
                source_txn_uid,                    #  3
                f_str('account_source_unique_id'), #  4
                f_str('account_source_unique_id'), #  5
                f_str('customer_source_unique_id'),#  6
                f_str('customer_source_unique_id'),#  7
                f_num('branch_id'),                #  8
                f_str('txn_code'),                 #  9
                '',                                # 10
                f_str('curcode'),                  # 11
                f_str('curbase'),                  # 12
                f_str('origination_date'),         # 13
                f_str('posting_date'),             # 14
                '',                                # 15
                '',                                # 16
                f_str('local_timestamp'),          # 17
                f_str('prod'),                     # 18
                '',                                # 19
                '',                                # 20
                f_num('amount'),                   # 21
                f_num('amount'),                   # 22
                f_str('crdr'),                     # 23
                f_str('mention'),                  # 24
                '',                                # 25
                '',                                # 26
                '',                                # 27
                '',                                # 28
                '',                                # 29
                '',                                # 30
                '',                                # 31
                f_num('channel'),                  # 32
                '',                                # 33
                '',                                # 34
                '',                                # 35
                f_str('org_unit_code'),            # 36
                '',                                # 37
                '',                                # 38
                '',                                # 39
                '',                                # 40
                f_num('employee_id'),              # 41
                '',                                # 42
                '',                                # 43
                '',                                # 44
                '',                                # 45
                '',                                # 46
                '',                                # 47
                '',                                # 48
                '',                                # 49
                '',                                # 50
                '',                                # 51
                '',                                # 52
                '',                                # 53
                '',                                # 54
                '',                                # 55
                '',                                # 56
                '',                                # 57
                '',                                # 58
                '',                                # 59
                f_str('originator_name'),          # 60
                f_str('beneficiary_name'),         # 61
                f_str('originator_bank'),          # 62
                f_str('beneficiary_bank'),         # 63
                f_str('userid'),                   # 64
                '',                                # 65
                '',                                # 66
                '',                                # 67
                '',                                # 68
                '',                                # 69
                f_str('beneficiary_id'),           # 70
                f_str('originator_id'),            # 71
                f_str('transref'),                 # 72
                f_str('incoming_outgoing_flg'),    # 73
                f_str('sender_branch'),            # 74
                f_str('bene_branch'),              # 75
            ]

            line = DELIM.join(fields)
            f.write(line + '\n')

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("EIDETRTS: Starting RENTAS remittance extraction for DETICA...")

    # -------------------------------------------------------------------------
    # Get report date variables
    # -------------------------------------------------------------------------
    rv = get_report_vars()
    print(f"  Report date: YYYY={rv['yyyy']} MM={rv['reptmon']} DD={rv['reptday']} "
          f"WK={rv['nowk']} RDATE={rv['rdate']}")

    # -------------------------------------------------------------------------
    # Load RENTAS source
    # -------------------------------------------------------------------------
    rentas_df = load_rentas(rv)
    print(f"  RENTAS rows: {len(rentas_df)}")

    # -------------------------------------------------------------------------
    # DATA INWARD OUTWARD BT — initial RENTAS processing
    # -------------------------------------------------------------------------
    inward_df, outward_df, bt_df = process_rentas(rentas_df, rv)
    print(f"  INWARD: {len(inward_df)}, OUTWARD: {len(outward_df)}, BT: {len(bt_df)}")

    # -------------------------------------------------------------------------
    # DATA INWARD1 LOANS_INWARD INWARD2
    # -------------------------------------------------------------------------
    inward1_df, loans_inward_df, inward2_df = process_inward(inward_df)
    inward2_df = process_inward2(inward2_df)
    print(f"  INWARD1: {len(inward1_df)}, LOANS_INWARD: {len(loans_inward_df)}, "
          f"INWARD2: {len(inward2_df)}")

    # -------------------------------------------------------------------------
    # DATA OUTWARD
    # -------------------------------------------------------------------------
    outward_df = process_outward(outward_df)

    # -------------------------------------------------------------------------
    # DATA BT — initial + BTMAST merge
    # -------------------------------------------------------------------------
    bt_df   = process_bt_initial(bt_df)
    btmast  = load_btmast(rv)
    bt_df   = merge_bt_btmast(bt_df, btmast)
    print(f"  BTMAST: {len(btmast)}")

    # -------------------------------------------------------------------------
    # Build LOAN_ACCT, CTR, DEPO_ACCT, ACCT master
    # -------------------------------------------------------------------------
    loan_acct_df, ctr_df = build_loan_acct()
    depo_acct_df         = build_depo_acct()
    acct_df              = build_acct_master(depo_acct_df, loan_acct_df, ctr_df)
    print(f"  ACCT master rows: {len(acct_df)}")

    # -------------------------------------------------------------------------
    # DATA INWARD_OUTWARD + ACCT join
    # -------------------------------------------------------------------------
    inward_outward_df = combine_inward_outward(inward1_df, loans_inward_df,
                                               inward2_df, outward_df)
    inward_outward_df = join_acct(inward_outward_df, acct_df)
    inward_outward_df = apply_branch_override(inward_outward_df)
    print(f"  INWARD_OUTWARD after acct join: {len(inward_outward_df)}")

    # -------------------------------------------------------------------------
    # DATA TRAN.INWARD_OUTWARD (combine with BT)
    # -------------------------------------------------------------------------
    tran_df = build_tran_inward_outward(inward_outward_df, bt_df)

    # -------------------------------------------------------------------------
    # CIS lookup and join
    # -------------------------------------------------------------------------
    cis_df  = build_cis()
    print(f"  CIS rows: {len(cis_df)}")
    tran_df = join_cis(tran_df, cis_df)
    tran_df = apply_cis_fallback(tran_df)

    # -------------------------------------------------------------------------
    # DATA OUT — final filtering and alias suppression
    # -------------------------------------------------------------------------
    out_df = build_out(tran_df)
    print(f"  OUT (final) rows: {len(out_df)}")

    # -------------------------------------------------------------------------
    # Write RENTRAN delimited output
    # -------------------------------------------------------------------------
    write_rentran(out_df, rv, RENTRAN_TXT)
    print(f"  Written: {RENTRAN_TXT}")

    # -------------------------------------------------------------------------
    # COPYFILE — backup (copy output to BKP path)
    # -------------------------------------------------------------------------
    import shutil
    shutil.copy2(RENTRAN_TXT, RENTRAN_BKP_TXT)
    print(f"  Backup written: {RENTRAN_BKP_TXT}")

    print("EIDETRTS: Processing complete.")


if __name__ == '__main__':
    main()
