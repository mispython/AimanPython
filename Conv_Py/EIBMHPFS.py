# !/usr/bin/env python3
"""
Program Name: EIBMHPFS
Purpose: HP FISS - Disbursement, Repayment, Approval reporting for BNM
         - Reads LOAN.REPTDATE for report period variables
         - Identifies newly issued / settled HP accounts from LOAN.LNNOTE
         - Merges with BNM.LOAN<MM><WK> for current balances
         - Builds RDAL fixed-width output file (LRECL=80) with BNM codes
           covering disbursement by: purpose code, custcd, sectorial code,
           count by custcd; with new sector mapping rollup
         - Produces EXCEPT and HPSNR exception/same-month release reports
           (ASA carriage-control, LRECL=133)

ESMR: 06-1485, 06-1762
"""

# %INC PGM(PBBLNFMT)
from PBBLNFMT import HPD_SET, NEWSECT_MAP, VALIDSE_MAP  # (placeholder)

import os
from datetime import date, timedelta
from typing import Optional

import duckdb
import polars as pl

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

LOAN_REPTDATE_PARQUET  = "input/loan/reptdate.parquet"
LOAN_LNNOTE_PARQUET    = "input/loan/lnnote.parquet"
BNM_LOAN_PREFIX        = "input/bnm/loan"        # loan<MM><WK>.parquet

OUTPUT_DIR             = "output/eibmhpfs"
RDAL_TXT               = os.path.join(OUTPUT_DIR, "hp_rdal.txt")
EXCEPT_TXT             = os.path.join(OUTPUT_DIR, "hp_except.txt")
HPSNR_TXT              = os.path.join(OUTPUT_DIR, "hp_snr.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# %INC PGM(PBBLNFMT) — HPD_SET, $NEWSECT., $VALIDSE. FORMAT MAPS
# =============================================================================

# %LET HPD macro from PBBLNFMT — HP product codes (placeholder set covering
# standard PBB HP products; full list lives in PBBLNFMT)
# from PBBLNFMT import HPD_SET  # (placeholder)
HPD_SET = {
    321, 322, 323, 324, 325, 326, 327, 328, 329, 330,
    331, 332, 333, 334, 335, 336, 337, 338, 339, 340,
    341, 342, 343, 344, 345, 346, 347, 348, 349, 350,
    351, 352, 353, 354, 355, 356, 357, 358, 359, 360,
    361, 362, 363, 364, 365, 366, 367, 368, 369, 370,
    371, 372, 373, 374, 375, 376, 377, 378, 379, 380,
    381, 382, 383, 384, 385, 386, 387, 388, 389, 390,
    391, 392, 393, 394, 395, 396, 397, 398, 399, 400,
    401, 402, 403, 404, 405, 406, 407, 408, 409, 410,
    411, 412, 413, 414, 415, 416, 417, 418,
    678, 679, 698, 699, 983, 993, 996,
}

# $NEWSECT. format — maps old sector codes to new sector codes
# from PBBLNFMT import NEWSECT_MAP  # (placeholder)
# Representative mapping; full map resides in PBBLNFMT
NEWSECT_MAP: dict[str, str] = {}   # key=old_sectorcd -> value=new_sectorcd

# $VALIDSE. format — marks sector codes as INVALID
# from PBBLNFMT import VALIDSE_MAP  # (placeholder)
VALIDSE_MAP: dict[str, str] = {}   # key=sectorcd -> value='INVALID' or ''

# =============================================================================
# PAGE / ASA CONSTANTS
# =============================================================================

PAGE_LENGTH = 60

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


def coalesce_s(val, default: str = '') -> str:
    return str(val).strip() if val is not None else default


def coalesce_i(val, default: int = 0) -> int:
    if val is None or (isinstance(val, float) and val != val):
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def coalesce_f(val, default: float = 0.0) -> float:
    if val is None or (isinstance(val, float) and val != val):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def parse_z11_to_date(z11_val) -> Optional[date]:
    """
    INPUT(SUBSTR(PUT(val, Z11.), 1, 8), MMDDYY8.)
    PUT(val, Z11.) -> zero-padded 11-digit integer string
    SUBSTR(…,1,8) -> first 8 chars = MMDDYYYY
    INPUT(…, MMDDYY8.) -> parse as month/day/year
    """
    if z11_val is None or (isinstance(z11_val, float) and z11_val != z11_val):
        return None
    try:
        s = str(int(z11_val)).zfill(11)[:8]   # MMDDYYYY
        mm = int(s[0:2]); dd = int(s[2:4]); yy = int(s[4:8])
        if mm < 1 or mm > 12 or dd < 1 or dd > 31:
            return None
        return date(yy, mm, dd)
    except (ValueError, TypeError):
        return None


def parse_ddmmyy8(s: str) -> Optional[date]:
    """Parse DD/MM/YY string (DDMMYY8. format)."""
    try:
        s = s.strip()
        dd = int(s[0:2]); mm = int(s[2:4]); yy = int(s[4:6])
        year = (2000 + yy) if yy < 50 else (1900 + yy)
        return date(year, mm, dd)
    except (ValueError, TypeError, IndexError):
        return None


def fmt_date9(d: Optional[date]) -> str:
    """Format date as DATE9. = DDMonYYYY e.g. 01JAN2024"""
    if d is None:
        return '         '
    months = ['JAN','FEB','MAR','APR','MAY','JUN',
              'JUL','AUG','SEP','OCT','NOV','DEC']
    return f"{d.day:02d}{months[d.month-1]}{d.year:04d}"


def fmt_comma15_2(val) -> str:
    """Format as COMMA15.2"""
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * 15
    v = float(val)
    s = f"{abs(v):,.2f}"
    if v < 0:
        s = '-' + s
    return s.rjust(15)

# =============================================================================
# REPORT DATE VARIABLES
# =============================================================================

def get_report_vars() -> dict:
    """
    DATA REPTDATE: SET LOAN.REPTDATE;
    SELECT(DAY(REPTDATE)):
      WHEN(8)  WK='1' WK1='4'
      WHEN(15) WK='2' WK1='1'
      WHEN(22) WK='3' WK1='2'
      OTHERWISE WK='4' WK1='3'
    MM = MONTH; MM1 = MM-1; YY1 adjusted if MM1=0;
    SDATE = MDY(MM,1,YEAR); PSDATE = MDY(MM1,1,YY1)
    """
    con  = duckdb.connect()
    row  = con.execute(
        f"SELECT reptdate FROM read_parquet('{LOAN_REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate = sas_date_to_pydate(row[0])
    day      = reptdate.day

    if day == 8:
        wk = '1'; wk1 = '4'
    elif day == 15:
        wk = '2'; wk1 = '1'
    elif day == 22:
        wk = '3'; wk1 = '2'
    else:
        wk = '4'; wk1 = '3'

    mm  = reptdate.month
    mm1 = mm - 1
    yy1 = reptdate.year
    if mm1 == 0:
        mm1 = 12
        yy1 -= 1

    sdate   = date(reptdate.year, mm,  1)
    psdate  = date(yy1,           mm1, 1)

    reptmon  = str(mm).zfill(2)
    reptmon1 = str(mm1).zfill(2)
    reptyear = str(reptdate.year)
    reptday  = str(day).zfill(2)
    rdate    = reptdate.strftime('%d/%m/%y')
    sdate_s  = sdate.strftime('%d/%m/%y')
    psdate_s = psdate.strftime('%d/%m/%y')

    return {
        'reptdate':  reptdate,
        'sdate':     sdate,
        'psdate':    psdate,
        'wk':        wk,
        'wk1':       wk1,
        'reptmon':   reptmon,
        'reptmon1':  reptmon1,
        'reptyear':  reptyear,
        'reptday':   reptday,
        'rdate':     rdate,
        'sdate_s':   sdate_s,
        'psdate_s':  psdate_s,
        'nowk':      wk,
        'nowk1':     wk1,
    }

# =============================================================================
# LOAD BNM LOAN
# =============================================================================

def load_bnm_loan(rv: dict) -> pl.DataFrame:
    path = f"{BNM_LOAN_PREFIX}{rv['reptmon']}{rv['nowk']}.parquet"
    if not os.path.exists(path):
        return pl.DataFrame()
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    return df

# =============================================================================
# BUILD HPSETTLE — settled HP accounts from LOAN.LNNOTE
# =============================================================================

def build_hpsettle(rv: dict) -> pl.DataFrame:
    """
    DATA HPSETTLE:
      SET LOAN.LNNOTE;
      IF (LOANTYPE IN &HPD OR LOANTYPE IN (15,20,63,71,72)) AND PAIDIND='P';
      CLOSEDTE = INPUT(SUBSTR(PUT(LASTTRAN,Z11.),1,8), MMDDYY8.);
      ISSDTE   = INPUT(SUBSTR(PUT(ISSUEDT, Z11.),1,8), MMDDYY8.);
    KEEP: ACCTNO NOTENO CUSTCODE SECTOR LOANTYPE NETPROC ISSDTE CRISPURP CLOSEDTE
    """
    con  = duckdb.connect()
    raw  = con.execute(
        f"SELECT * FROM read_parquet('{LOAN_LNNOTE_PARQUET}')"
    ).pl()
    con.close()

    if raw.is_empty():
        return pl.DataFrame()

    extra_hp = {15, 20, 63, 71, 72}
    keep = ['acctno','noteno','custcode','sector','loantype',
            'netproc','crispurp','lasttran','issuedt','paidind']
    avail = [c for c in keep if c in raw.columns]
    df    = raw.select(avail)

    rows     = df.to_dicts()
    out_rows = []
    for row in rows:
        loantype = coalesce_i(row.get('loantype'))
        paidind  = coalesce_s(row.get('paidind'))
        if not (loantype in HPD_SET or loantype in extra_hp):
            continue
        if paidind != 'P':
            continue
        closedte = parse_z11_to_date(row.get('lasttran'))
        issdte   = parse_z11_to_date(row.get('issuedt'))
        out = {
            'acctno':   row.get('acctno'),
            'noteno':   row.get('noteno'),
            'custcode': coalesce_s(row.get('custcode')),
            'sector':   coalesce_s(row.get('sector')),
            'loantype': loantype,
            'netproc':  coalesce_f(row.get('netproc')),
            'crispurp': coalesce_s(row.get('crispurp')),
            'issdte':   issdte,
            'closedte': closedte,
        }
        out_rows.append(out)

    return pl.from_dicts(out_rows) if out_rows else pl.DataFrame()

# =============================================================================
# BUILD HPSNR — settled and released same month
# =============================================================================

def build_hpsnr(hpsettle: pl.DataFrame, sdate: date, bnm_loan: pl.DataFrame) -> pl.DataFrame:
    """
    DATA HPSNR: SET HPSETTLE;
      IF ISSDTE >= SDATE AND CLOSEDTE >= SDATE;
    PROC SORT; BY ACCTNO NOTENO;
    DATA HPSNR(KEEP=...): MERGE HPSNR(IN=A) BNM.LOAN<MM><WK>; BY ACCTNO NOTENO; IF A;
    """
    if hpsettle.is_empty():
        return pl.DataFrame()

    out_rows = []
    for row in hpsettle.to_dicts():
        issdte   = row.get('issdte')
        closedte = row.get('closedte')
        if issdte is None or closedte is None:
            continue
        if issdte >= sdate and closedte >= sdate:
            out_rows.append(row)

    if not out_rows:
        return pl.DataFrame()

    hpsnr = pl.from_dicts(out_rows).sort(['acctno','noteno'])

    if bnm_loan.is_empty():
        return hpsnr

    # MERGE HPSNR(IN=A) BNM.LOAN; BY ACCTNO NOTENO; IF A
    loan_keep = [c for c in ['acctno','noteno','fisspurp','product','noteterm',
                              'earnterm','netproc','apprdate','apprlim2','prodcd',
                              'custcd','amtind','sectorcd','balance','issdte',
                              'acctype'] if c in bnm_loan.columns]
    loan_sel  = bnm_loan.select(loan_keep)

    merged = hpsnr.join(loan_sel, on=['acctno','noteno'], how='left', suffix='_ln')
    for c in [x for x in loan_keep if x not in ('acctno','noteno')]:
        lc = f"{c}_ln"
        if lc in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(lc).is_not_null())
                  .then(pl.col(lc))
                  .otherwise(pl.col(c) if c in merged.columns else pl.lit(None))
                  .alias(c)
            ).drop(lc)

    return merged.select([c for c in ['acctno','noteno','fisspurp','product',
                                       'noteterm','earnterm','netproc','apprdate',
                                       'apprlim2','prodcd','custcd','amtind',
                                       'sectorcd','balance','issdte','acctype']
                          if c in merged.columns])

# =============================================================================
# BUILD HP — new HP accounts issued this month (not settled)
# =============================================================================

def build_hp_new(bnm_loan: pl.DataFrame, sdate: date) -> pl.DataFrame:
    """
    DATA HP:
      SET BNM.LOAN<MM><WK>;
      IF CURBAL > 0 AND PRODCD IN ('34111') AND ISSDTE >= SDATE;
    KEEP: ACCTNO NOTENO FISSPURP PRODUCT NOTETERM EARNTERM NETPROC APPRDATE APPRLIM2
          PRODCD CUSTCD AMTIND SECTORCD BALANCE ISSDTE ACCTYPE
    """
    if bnm_loan.is_empty():
        return pl.DataFrame()

    keep = [c for c in ['acctno','noteno','fisspurp','product','noteterm',
                         'earnterm','netproc','apprdate','apprlim2','prodcd',
                         'custcd','amtind','sectorcd','balance','curbal',
                         'issdte','acctype'] if c in bnm_loan.columns]
    df   = bnm_loan.select(keep)

    out_rows = []
    for row in df.to_dicts():
        curbal  = coalesce_f(row.get('curbal'))
        prodcd  = coalesce_s(row.get('prodcd'))
        issdte  = row.get('issdte')

        # Parse ISSDTE if still integer (SAS date)
        if isinstance(issdte, (int, float)):
            issdte = sas_date_to_pydate(issdte)
        if issdte is None:
            continue
        if curbal > 0 and prodcd == '34111' and issdte >= sdate:
            r2 = dict(row)
            r2['issdte'] = issdte
            out_rows.append(r2)

    if not out_rows:
        return pl.DataFrame()

    result = pl.from_dicts(out_rows)
    drop_cols = [c for c in ['curbal'] if c in result.columns]
    if drop_cols:
        result = result.drop(drop_cols)
    return result

# =============================================================================
# MERGE HPSETTLE + HP — split into HP, EXCEPT, then add HPSNR
# =============================================================================

def merge_hp_settle(hpsettle_df: pl.DataFrame, hp_new: pl.DataFrame,
                    hpsnr_df: pl.DataFrame) -> tuple:
    """
    DATA HPSETTLE(RENAME=...): rename columns for settle dataset
    PROC SORT HPSETTLE NODUPKEYS; BY ACCTNO;
    PROC SORT HPSNR NODUPKEYS; BY ACCTNO;
    PROC SORT HP; BY ACCTNO;

    DATA HP EXCEPT:
      MERGE HPSETTLE(IN=B) HP(IN=A); BY ACCTNO;
      IF (A AND B): OUTPUT EXCEPT;
        IF SAME MONTH issdte: OUTPUT HP;
      IF A AND NOT B: OUTPUT HP;

    A/C SETTLE AND RELEASE ON SAME MONTH:
    DATA HPSNR: MERGE HPSNR(IN=B) HP(IN=A); BY ACCTNO; IF B AND NOT A;
    DATA HP: SET HP HPSNR;
    """
    # Rename hpsettle columns
    rename_map = {
        'noteno':   'onote',
        'custcode': 'ocustcd',
        'sector':   'osector',
        'loantype': 'oprod',
        'netproc':  'onet',
        'issdte':   'oissdte',
        'crispurp': 'ofiss',
        'closedte': 'oclose',
    }
    if not hpsettle_df.is_empty():
        cols_to_rename = {k: v for k, v in rename_map.items()
                          if k in hpsettle_df.columns}
        settle = hpsettle_df.rename(cols_to_rename)
        settle = settle.unique(subset=['acctno'], keep='first').sort('acctno')
    else:
        settle = pl.DataFrame()

    if not hpsnr_df.is_empty():
        hpsnr_dedup = hpsnr_df.unique(subset=['acctno'], keep='first').sort('acctno')
    else:
        hpsnr_dedup = pl.DataFrame()

    if not hp_new.is_empty():
        hp_sorted = hp_new.sort('acctno')
    else:
        hp_sorted = pl.DataFrame()

    hp_rows     = []
    except_rows = []

    if not hp_sorted.is_empty():
        if not settle.is_empty():
            merged = hp_sorted.join(settle, on='acctno', how='left', suffix='_st')
            for row in merged.to_dicts():
                in_b  = row.get('onote') is not None   # B indicator (settle present)
                issdte   = row.get('issdte')
                oissdte  = row.get('oissdte')

                if in_b:
                    except_rows.append(dict(row))
                    # IF MONTH(ISSDTE)=MONTH(OISSDTE) AND YEAR(ISSDTE)=YEAR(OISSDTE)
                    if (issdte is not None and oissdte is not None and
                            issdte.month == oissdte.month and
                            issdte.year  == oissdte.year):
                        hp_rows.append(dict(row))
                else:
                    hp_rows.append(dict(row))
        else:
            hp_rows = hp_sorted.to_dicts()

    hp_df     = pl.from_dicts(hp_rows)     if hp_rows     else pl.DataFrame()
    except_df = pl.from_dicts(except_rows) if except_rows else pl.DataFrame()

    # DATA HPSNR: MERGE HPSNR(IN=B) HP(IN=A); IF B AND NOT A
    final_hpsnr = pl.DataFrame()
    if not hpsnr_dedup.is_empty() and not hp_df.is_empty():
        hp_keys = hp_df.select('acctno').with_columns(pl.lit(True).alias('_in_a'))
        snr_m   = hpsnr_dedup.join(hp_keys, on='acctno', how='left')
        snr_m   = snr_m.filter(pl.col('_in_a').is_null()).drop('_in_a')
        final_hpsnr = snr_m
    elif not hpsnr_dedup.is_empty():
        final_hpsnr = hpsnr_dedup

    # DATA HP: SET HP HPSNR
    if not final_hpsnr.is_empty():
        hp_df = pl.concat([hp_df, final_hpsnr], how='diagonal') if not hp_df.is_empty() \
                else final_hpsnr

    return hp_df, except_df, final_hpsnr

# =============================================================================
# SECTOR MAPPING HELPERS ($NEWSECT. / $VALIDSE.)
# =============================================================================

def apply_newsect(sectorcd: str) -> str:
    """$NEWSECT. format — returns new sector code or '    ' if no mapping."""
    return NEWSECT_MAP.get(sectorcd, '    ')


def apply_validse(sectorcd: str) -> str:
    """$VALIDSE. format — returns 'INVALID' or '' ."""
    return VALIDSE_MAP.get(sectorcd, '')


def remap_invalid_sectcd(sectcd: str) -> str:
    """
    DATA ALM: IF SECVALID='INVALID' THEN DO;
    Map invalid sector codes to default codes based on first 1-2 chars.
    """
    p1 = sectcd[0:1] if sectcd else ''
    p2 = sectcd[0:2] if len(sectcd) >= 2 else ''

    if p1 == '1': return '1400'
    if p1 == '2': return '2900'
    if p1 == '3': return '3919'
    if p1 == '4': return '4010'
    if p1 == '5': return '5999'
    if p2 == '61': return '6120'
    if p2 == '62': return '6130'
    if p2 == '63': return '6310'
    if p2 in ('64','65','66','67','68','69'): return '6130'
    if p1 == '7': return '7199'
    if p2 in ('81','82'): return '8110'
    if p2 in ('83','84','85','86','87','88','89'): return '8999'
    if p2 == '91': return '9101'
    if p2 == '92': return '9410'
    if p2 in ('93','94','95'): return '9499'
    if p2 in ('96','97','98','99'): return '9999'
    return sectcd


def apply_sectcd_mapping(rows: list) -> list:
    """
    DATA ALM: SECTA=PUT(SECTORCD,$NEWSECT.); SECVALID=PUT(SECTORCD,$VALIDSE.);
    IF SECTA NE '    ' THEN SECTCD=SECTA; ELSE SECTCD=SECTORCD;
    Then remap invalid.
    """
    for row in rows:
        scd    = coalesce_s(row.get('sectorcd'))
        secta  = apply_newsect(scd)
        secvalid = apply_validse(scd)
        row['secvalid'] = secvalid
        sectcd = secta if secta.strip() else scd
        if secvalid == 'INVALID':
            sectcd = remap_invalid_sectcd(sectcd)
        row['sectcd'] = sectcd
    return rows


def expand_alm2_rows(row: dict) -> list:
    """
    DATA ALM2: SET ALM; — huge sector code expansion table.
    Each SECTCD value maps to one or more SECTORCD output rows.
    Returns list of dicts (one per OUTPUT statement in SAS).
    This implements the complete sector hierarchy expansion from the SAS program.
    """
    sectcd = coalesce_s(row.get('sectcd'))
    outputs = []

    def emit(scd: str):
        r = dict(row); r['sectorcd'] = scd; outputs.append(r)

    # 1xxx
    if sectcd in ('1111','1112','1113','1114','1115','1116','1117','1119','1120','1130','1140','1150'):
        if sectcd in ('1111','1113','1115','1117','1119'): emit('1110')
        emit('1100')
    # 2xxx
    if sectcd in ('2210','2220'): emit('2200')
    if sectcd in ('2301','2302','2303'):
        if sectcd in ('2301','2302'): emit('2300')
        emit('2300')
        if sectcd == '2303': emit('2302')
    # 3xxx
    if sectcd in ('3110','3115','3111','3112','3113','3114'):
        if sectcd in ('3110','3113','3114'): emit('3100')
        if sectcd in ('3115','3111','3112'): emit('3110')
    if sectcd in ('3211','3212','3219'): emit('3210')
    if sectcd in ('3221','3222'):        emit('3220')
    if sectcd in ('3231','3232'):        emit('3230')
    if sectcd in ('3241','3242'):        emit('3240')
    if sectcd in ('3270','3280','3290','3271','3272','3273','3311','3312','3313'):
        if sectcd in ('3270','3280','3290','3271','3272','3273'): emit('3260')
        if sectcd in ('3271','3272','3273'): emit('3270')
        if sectcd in ('3311','3312','3313'): emit('3310')
    if sectcd in ('3431','3432','3433'): emit('3430')
    if sectcd in ('3551','3552'):        emit('3550')
    if sectcd in ('3611','3619'):        emit('3610')
    if sectcd in ('3710','3720','3730','3720','3721','3731','3732'):
        emit('3700')
        if sectcd == '3721': emit('3720')
        if sectcd in ('3731','3732'): emit('3730')
    if sectcd in ('3811','3812'): emit('3800')
    if sectcd in ('3813','3814','3819'): emit('3812')
    if sectcd in ('3832','3834','3835','3833'):
        emit('3831')
        if sectcd == '3833': emit('3832')
    if sectcd in ('3842','3843','3844'): emit('3841')
    if sectcd in ('3851','3852','3853'): emit('3850')
    if sectcd in ('3861','3862','3863','3864','3865','3866'): emit('3860')
    if sectcd in ('3871','3872'): emit('3870')
    if sectcd in ('3891','3892','3893','3894'): emit('3890')
    if sectcd in ('3911','3919'): emit('3910')
    if sectcd in ('3951','3952','3953','3954','3955','3956','3957'):
        emit('3950')
        if sectcd in ('3952','3953'): emit('3951')
        if sectcd in ('3955','3956','3957'): emit('3954')
    # 5xxx
    if sectcd in ('5001','5002','5003','5004','5005','5006','5008'): emit('5010')
    # 6xxx
    if sectcd in ('6110','6120','6130'): emit('6100')
    if sectcd in ('6310','6320'):        emit('6300')
    # 7xxx
    if sectcd in ('7111','7112','7117','7113','7114','7115','7116'): emit('7110')
    if sectcd in ('7113','7114','7115','7116'): emit('7112')
    if sectcd in ('7112','7114'): emit('7113')
    if sectcd == '7116': emit('7115')
    if sectcd in ('7121','7123','7124'):
        emit('7120')
        if sectcd == '7124': emit('7123')
    if sectcd in ('7131','7132','7133','7134'): emit('7130')
    if sectcd in ('7191','7192','7193','7199'): emit('7190')
    if sectcd in ('7210','7220'): emit('7200')
    # 8xxx
    if sectcd in ('8110','8120','8130'): emit('8100')
    if sectcd in ('8310','8330','8340','8320','8331','8332'):
        emit('8300')
        if sectcd in ('8320','8331','8332'): emit('8330')
    if sectcd == '8321': emit('8320')
    if sectcd == '8333': emit('8332')
    if sectcd in ('8420','8411','8412','8413','8414','8415','8416'):
        emit('8400')
        if sectcd in ('8411','8412','8413','8414','8415','8416'): emit('8410')
    if sectcd[:2] == '89':
        emit('8900')
        if sectcd in ('8910','8911','8912','8913','8914'):
            if sectcd in ('8911','8912','8913','8914'): emit('8910')
            if sectcd == '8910': emit('8914')
        if sectcd in ('8921','8922','8920'):
            if sectcd in ('8921','8922'): emit('8920')
            if sectcd == '8920': emit('8922')
        if sectcd in ('8931','8932'): emit('8930')
        if sectcd in ('8991','8999'): emit('8990')
    # 9xxx
    if sectcd in ('9101','9102','9103'): emit('9100')
    if sectcd in ('9201','9202','9203'): emit('9200')
    if sectcd in ('9311','9312','9313','9314'): emit('9300')
    if sectcd[:2] == '94':
        emit('9400')
        if sectcd in ('9433','9434','9435','9432','9431'):
            if sectcd in ('9433','9434','9435'): emit('9432')
            emit('9430')
        if sectcd in ('9410','9420','9440','9450'): emit('9499')

    return outputs


def build_alma_rollup(rows: list) -> list:
    """
    DATA ALMA: SET ALM;
    Roll sub-sector codes up to major sector groupings (1000..9000).
    """
    rollup = {
        '1000': {'1100','1200','1300','1400'},
        '2000': {'2100','2200','2300','2400','2900'},
        '3000': {'3100','3120','3210','3220','3230','3240','3250','3260','3310',
                 '3430','3550','3610','3700','3800','3825','3831','3841','3850',
                 '3860','3870','3890','3910','3950','3960'},
        '4000': {'4010','4020','4030'},
        '5000': {'5010','5020','5030','5040','5050','5999'},
        '6000': {'6100','6300'},
        '7000': {'7110','7120','7130','7190','7200'},
        '8000': {'8100','8300','8400','8900'},
        '9000': {'9100','9200','9300','9400','9500','9600'},
    }
    out = []
    for row in rows:
        scd = coalesce_s(row.get('sectorcd'))
        for major, sub_set in rollup.items():
            if scd in sub_set:
                r2 = dict(row); r2['sectorcd'] = major
                out.append(r2)
                break
    return out


def build_alm_with_sector(hp_df: pl.DataFrame) -> pl.DataFrame:
    """
    Full sector mapping pipeline:
    1. PROC SUMMARY by SECTORCD AMTIND
    2. Apply $NEWSECT./$VALIDSE. -> SECTCD
    3. Expand ALM2 sub-sector rows
    4. DATA ALM: SET ALM(IN=A) ALM2; IF A THEN SECTORCD=SECTCD
    5. DATA ALMA: rollup to major sectors
    6. DATA ALM: SET ALM ALMA; fill blank SECTORCD='9999'
    """
    if hp_df.is_empty():
        return pl.DataFrame()

    filt = hp_df
    if 'sectorcd' in filt.columns:
        filt = filt.filter(pl.col('sectorcd') != '0410')

    agg_v = [c for c in ['netproc'] if c in filt.columns]
    grp_v = [c for c in ['sectorcd','amtind'] if c in filt.columns]
    if not agg_v or not grp_v:
        return pl.DataFrame()

    alm_sum = filt.group_by(grp_v).agg([pl.col(c).sum() for c in agg_v])
    rows    = alm_sum.to_dicts()

    # Apply $NEWSECT. / $VALIDSE.
    rows = apply_sectcd_mapping(rows)

    # DATA ALM: IF A THEN SECTORCD=SECTCD
    for row in rows:
        row['sectorcd'] = row['sectcd']

    # DATA ALM2 expansion
    alm2_rows = []
    for row in rows:
        alm2_rows.extend(expand_alm2_rows(row))

    # DATA ALM: SET ALM(IN=A) ALM2; IF A THEN SECTORCD=SECTCD
    alm_all = list(rows)  # IN=A: keep original SECTORCD=SECTCD (already set)
    for r in alm2_rows:   # ALM2 rows have SECTORCD already set by expand_alm2_rows
        alm_all.append(r)

    # DATA ALMA rollup
    alma_rows = build_alma_rollup(alm_all)

    # DATA ALM: SET ALM ALMA; blank -> '9999'
    alm_final = alm_all + alma_rows
    for row in alm_final:
        scd = coalesce_s(row.get('sectorcd'))
        if not scd.strip():
            row['sectorcd'] = '9999'

    return pl.from_dicts(alm_final) if alm_final else pl.DataFrame()


def build_alm_smi_with_sector(hp_df: pl.DataFrame) -> pl.DataFrame:
    """
    Same sector pipeline but CLASS SECTORCD CUSTCD AMTIND
    (used for SMI by sector breakdown).
    """
    if hp_df.is_empty():
        return pl.DataFrame()

    filt = hp_df
    if 'sectorcd' in filt.columns:
        filt = filt.filter(pl.col('sectorcd') != '0410')

    grp_v = [c for c in ['sectorcd','custcd','amtind'] if c in filt.columns]
    if not grp_v or 'netproc' not in filt.columns:
        return pl.DataFrame()

    alm_sum = filt.group_by(grp_v).agg(pl.col('netproc').sum())
    rows    = alm_sum.to_dicts()
    rows    = apply_sectcd_mapping(rows)
    for row in rows:
        row['sectorcd'] = row['sectcd']

    alm2_rows = []
    for row in rows:
        alm2_rows.extend(expand_alm2_rows(row))

    alm_all = list(rows) + alm2_rows
    alma_rows = build_alma_rollup(alm_all)
    alm_final = alm_all + alma_rows
    for row in alm_final:
        if not coalesce_s(row.get('sectorcd')).strip():
            row['sectorcd'] = '9999'

    return pl.from_dicts(alm_final) if alm_final else pl.DataFrame()

# =============================================================================
# BUILD LALM RECORDS — (BNMCODE, AMTIND, AMOUNT) tuples
# =============================================================================

def build_lalm(hp_df: pl.DataFrame) -> list:
    """
    Full LALM accumulation pipeline producing (bnmcode, amtind, amount) rows.
    Covers all five sections:
      1. Disbursement by PURPOSE CODE (FISSPURP)
      2. Disbursement SMI by FISSPURP + CUSTCD
      3. Disbursement by CUSTCD (custcd-only BNMCODEs)
      4. Disbursement / repayment by SECTORIAL CODE
      5. Disbursement SMI by SECTORIAL CODE + CUSTCD
      6. Count by CUSTCD / FISSPURP
    """
    lalm = []   # list of (bnmcode str, amtind str, amount float)

    if hp_df.is_empty():
        return lalm

    def add(bnmcode: str, amtind: str, amount: float):
        lalm.append((bnmcode[:14], amtind, amount))

    # ------------------------------------------------------------------
    # 1. DISBURSEMENT BY PURPOSE CODE
    # ------------------------------------------------------------------
    grp1 = [c for c in ['fisspurp','amtind'] if c in hp_df.columns]
    if grp1 and 'netproc' in hp_df.columns:
        sum1 = hp_df.group_by(grp1).agg(pl.col('netproc').sum())

        # ALMLOAN1: remap FISSPURP 0220/0230/0210/0211/0212 -> 0200
        rows1 = sum1.to_dicts()
        extra = []
        for row in rows1:
            fp = coalesce_s(row.get('fisspurp'))
            if fp in ('0220','0230','0210','0211','0212'):
                extra.append({**row, 'fisspurp': '0200'})
        rows1 = rows1 + extra

        for row in rows1:
            fp     = coalesce_s(row.get('fisspurp'))
            amtind = coalesce_s(row.get('amtind'))
            amt    = coalesce_f(row.get('netproc'))
            add('821510000' + fp + 'Y', amtind, amt)

    # ------------------------------------------------------------------
    # 2. DISBURSEMENT SMI BY FISSPURP + CUSTCD
    # ------------------------------------------------------------------
    grp2 = [c for c in ['fisspurp','custcd','amtind'] if c in hp_df.columns]
    if grp2 and 'netproc' in hp_df.columns:
        sum2 = hp_df.group_by(grp2).agg(pl.col('netproc').sum())

        rows2 = sum2.to_dicts()
        extra2 = []
        for row in rows2:
            fp = coalesce_s(row.get('fisspurp'))
            if fp in ('0220','0230','0210','0211','0212'):
                extra2.append({**row, 'fisspurp': '0200'})
        rows2 = rows2 + extra2

        smi_grp1  = {'41','42','43','44','46','47','48','49','51','52','53','54','77','78','79'}
        smi_grp11 = {'02','03','11','12'}
        smi_grp12 = {'20','13','17','30','32','33','34','35','36','37','38','39','40','04','05','06'}
        smi_grp17 = {'71','72','73','74'}
        smi_grp18 = {'81','82','83','84','85','86','90','91','92','95','96','98','99'}
        smi_g161  = {'41','42','43','61'}
        smi_g162  = {'62','44','46','47'}
        smi_g163  = {'63','48','49','51'}
        smi_g164  = {'64','52','53','54','59','75','57'}
        smi_g165  = {'41','42','43','44','46','47','48','49','51','52','53','54','61','62','63','64'}

        for row in rows2:
            fp     = coalesce_s(row.get('fisspurp'))
            cd     = coalesce_s(row.get('custcd'))
            amtind = coalesce_s(row.get('amtind'))
            amt    = coalesce_f(row.get('netproc'))

            if cd in smi_grp1:
                add(f'82151{cd}00{fp}Y', amtind, amt)
            if cd in smi_grp11:
                add(f'821511000{fp}Y', amtind, amt)
            if cd in smi_grp12:
                add(f'821512000{fp}Y', amtind, amt)
            if cd in smi_grp17:
                add(f'821517000{fp}Y', amtind, amt)
            if cd in smi_grp18:
                add(f'821518000{fp}Y', amtind, amt)
            if cd in smi_g161:
                add(f'821516100{fp}Y', amtind, amt)
            if cd in smi_g162:
                add(f'821516200{fp}Y', amtind, amt)
            if cd in smi_g163:
                add(f'821516300{fp}Y', amtind, amt)
            if cd in smi_g164:
                add(f'821516400{fp}Y', amtind, amt)
            if cd in smi_g165:
                add(f'821516500{fp}Y', amtind, amt)

    # ------------------------------------------------------------------
    # 3. DISBURSEMENT BY CUSTCD
    # ------------------------------------------------------------------
    grp3 = [c for c in ['custcd','amtind'] if c in hp_df.columns]
    if grp3 and 'netproc' in hp_df.columns:
        sum3 = hp_df.group_by(grp3).agg(pl.col('netproc').sum())
        for row in sum3.to_dicts():
            cd     = coalesce_s(row.get('custcd'))
            amtind = coalesce_s(row.get('amtind'))
            amt    = coalesce_f(row.get('netproc'))

            if cd in ('77','78'):
                add('8215176009700Y', amtind, amt)
            if cd == '77':
                add('8215177009700Y', amtind, amt)
            if cd == '78':
                add('8215178009700Y', amtind, amt)
            if cd in ('95','96'):
                add('8215185009700Y', amtind, amt)
                add('8215195009700Y', amtind, amt)
            if cd in ('80','81','82','83','84','85','86','87','88','89',
                      '90','91','92','93','94','97','98','99'):
                add('8215185009999Y', amtind, amt)
            if cd in ('77','78','95','96'):
                add('8215100009700Y', amtind, amt)

    # ------------------------------------------------------------------
    # 4. DISBURSEMENT BY SECTORIAL CODE (with sector mapping)
    # ------------------------------------------------------------------
    alm_sec = build_alm_with_sector(hp_df)
    if not alm_sec.is_empty():
        for row in alm_sec.to_dicts():
            scd    = coalesce_s(row.get('sectorcd'))
            amtind = coalesce_s(row.get('amtind'))
            amt    = coalesce_f(row.get('netproc'))
            add(f'821510000{scd}Y', amtind, amt)

    # ------------------------------------------------------------------
    # 5. DISBURSEMENT SMI BY SECTORIAL CODE + CUSTCD
    # ------------------------------------------------------------------
    alm_smi_sec = build_alm_smi_with_sector(hp_df)
    if not alm_smi_sec.is_empty():
        smi_grp1  = {'41','42','43','44','46','47','48','49','51','52','53','54','77','78','79'}
        smi_grp11 = {'02','03','11','12'}
        smi_grp12 = {'20','13','17','30','32','33','34','35','36','37','38','39','40','04','05','06'}
        smi_grp17 = {'71','72','73','74'}
        smi_grp18 = {'81','82','83','84','85','86','90','91','92','95','96','98','99'}
        smi_g161  = {'41','42','43','61'}
        smi_g162  = {'62','44','46','47'}
        smi_g163  = {'63','48','49','51'}
        smi_g164  = {'64','52','53','54','59','75','57'}
        smi_g165  = {'41','42','43','44','46','47','48','49','51','52','53','54','61','62','63','64'}

        for row in alm_smi_sec.to_dicts():
            scd    = coalesce_s(row.get('sectorcd'))
            cd     = coalesce_s(row.get('custcd'))
            amtind = coalesce_s(row.get('amtind'))
            amt    = coalesce_f(row.get('netproc'))

            if cd in smi_grp1:
                add(f'82151{cd}00{scd}Y', amtind, amt)
            if cd in smi_grp11:
                add(f'821511000{scd}Y', amtind, amt)
            if cd in smi_grp12:
                add(f'821512000{scd}Y', amtind, amt)
            if cd in smi_grp17:
                add(f'821517000{scd}Y', amtind, amt)
            if cd in smi_grp18 and scd != '9999':
                add(f'821518000{scd}Y', amtind, amt)
            if cd in smi_g161:
                add(f'821516100{scd}Y', amtind, amt)
            if cd in smi_g162:
                add(f'821516200{scd}Y', amtind, amt)
            if cd in smi_g163:
                add(f'821516300{scd}Y', amtind, amt)
            if cd in smi_g164:
                add(f'821516400{scd}Y', amtind, amt)
            if cd in smi_g165:
                add(f'821516500{scd}Y', amtind, amt)

    # ------------------------------------------------------------------
    # 6. COUNT BY CUSTCD / FISSPURP
    # PROC SUMMARY DATA=ALM; CLASS CUSTCD FISSPURP AMTIND; VAR NETPROC;
    # Uses _TYPE_ and _FREQ_ logic
    # ------------------------------------------------------------------
    if not hp_df.is_empty() and 'custcd' in hp_df.columns:
        # _TYPE_=1: CLASS=AMTIND only (all records count)
        if 'amtind' in hp_df.columns:
            type1 = hp_df.group_by('amtind').agg(
                pl.len().alias('_freq_')
            )
            for row in type1.to_dicts():
                amtind = coalesce_s(row.get('amtind'))
                freq   = coalesce_i(row.get('_freq_'))
                add('8015000000000Y', amtind, float(freq * 1000))

        # _TYPE_=5: CLASS=CUSTCD + AMTIND
        grp5 = [c for c in ['custcd','amtind'] if c in hp_df.columns]
        if grp5:
            type5 = hp_df.group_by(grp5).agg(pl.len().alias('_freq_'))
            for row in type5.to_dicts():
                cd     = coalesce_s(row.get('custcd'))
                amtind = coalesce_s(row.get('amtind'))
                freq   = coalesce_i(row.get('_freq_'))
                amt    = float(freq * 1000)

                if cd in ('41','42','43','61'):
                    add('8015061000000Y', amtind, amt)
                if cd != '61':
                    add(f'80150{cd}000000Y', amtind, amt)

        # _TYPE_=3: CLASS=FISSPURP + AMTIND
        grp3b = [c for c in ['fisspurp','amtind'] if c in hp_df.columns]
        if grp3b:
            type3 = hp_df.group_by(grp3b).agg(pl.len().alias('_freq_'))
            for row in type3.to_dicts():
                fp     = coalesce_s(row.get('fisspurp'))
                amtind = coalesce_s(row.get('amtind'))
                freq   = coalesce_i(row.get('_freq_'))
                amt    = float(freq * 1000)

                if fp == '0211':
                    add('8030000000211Y', amtind, amt)
                if fp == '0212':
                    add('8030000000212Y', amtind, amt)

    return lalm

# =============================================================================
# FINAL CONSOLIDATION AND WRITE RDAL
# =============================================================================

def write_rdal(lalm: list, rv: dict):
    """
    PROC SUMMARY DATA=TEMP.LALM NWAY: sum AMOUNT by BNMCODE AMTIND -> LALM1
    DATA _NULL_: SET TEMP.LALM1; BY BNMCODE AMTIND;
    Write RDAL file:
      Line 1: PHEAD = 'HPRDAL'||REPTDAY||REPTMON||REPTYEAR
      Line 2: 'AL'
      For each BNMCODE: BNMCODE;AMOUNTD;AMOUNTI
      AMOUNT = ROUND(AMOUNT/1000)
      AMOUNTD accumulates D records; AMOUNTI accumulates I records
      On LAST.BNMCODE: AMOUNTD += AMOUNTI; write line; reset.
    """
    if not lalm:
        with open(RDAL_TXT, 'w', encoding='utf-8') as f:
            pass
        return

    # PROC SUMMARY: sum by BNMCODE AMTIND
    from collections import defaultdict
    agg: dict[tuple, float] = defaultdict(float)
    for bnmcode, amtind, amount in lalm:
        agg[(bnmcode, amtind)] += amount

    # Sort by BNMCODE AMTIND
    sorted_keys = sorted(agg.keys())

    phead = f"HPRDAL{rv['reptday']}{rv['reptmon']}{rv['reptyear']}"
    lines = [phead, 'AL']

    # Group by BNMCODE; accumulate D and I
    from itertools import groupby as igrp
    by_bnm: dict[str, dict] = {}
    for (bnmcode, amtind), total in agg.items():
        if bnmcode not in by_bnm:
            by_bnm[bnmcode] = {'D': 0.0, 'I': 0.0}
        key = 'D' if amtind == 'D' else 'I'
        by_bnm[bnmcode][key] += total

    for bnmcode in sorted(by_bnm.keys()):
        vals    = by_bnm[bnmcode]
        amt_d   = round(vals['D'] / 1000)
        amt_i   = round(vals['I'] / 1000)
        amt_d  += amt_i           # AMOUNTD = AMOUNTD + AMOUNTI
        lines.append(f"{bnmcode};{int(amt_d)};{int(amt_i)}")

    with open(RDAL_TXT, 'w', encoding='utf-8', newline='\n') as f:
        for ln in lines:
            f.write(ln + '\n')

# =============================================================================
# EXCEPTION / HPSNR REPORT WRITERS (ASA carriage control)
# =============================================================================

def _report_header(title1: str, title2: str, title3: str = '') -> list:
    """Build ASA header lines for PROC PRINT equivalent."""
    lines = []
    lines.append('1' + title1)
    lines.append(' ' + title2)
    if title3:
        lines.append(' ' + title3)
    lines.append(' ')
    # Column header
    hdr = (f"{'ACCOUNT':>12} {'NOTE':>7} {'CUSTCODE':>8} {'SECTOR':>7}"
           f" {'PRODUCT':>8} {'NETPROC':>15} {'ISSUE DATE':>10} {'PURPOSE CODE':>12}")
    lines.append(' ' + hdr)
    lines.append(' ' + '-' * len(hdr))
    return lines


def write_exception_report(df: pl.DataFrame, filepath: str,
                            title1: str, title2: str, title3: str = ''):
    """
    PROC PRINT DATA=EXCEPT/HPSNR SPLIT='*';
    VAR ACCTNO NOTENO CUSTCD SECTORCD PRODUCT NETPROC ISSDTE FISSPURP;
    SUM NETPROC;
    FORMAT NETPROC COMMA15.2 ISSDTE DATE9.;
    """
    lines        = []
    line_cnt     = 0
    page_num     = 0
    tot_netproc  = 0.0

    def new_page():
        nonlocal line_cnt, page_num
        page_num += 1
        line_cnt  = 0
        for ln in _report_header(title1, title2, title3):
            lines.append(ln)
            line_cnt += 1

    new_page()

    if not df.is_empty():
        for row in df.sort(['acctno','noteno']).iter_rows(named=True):
            if line_cnt >= PAGE_LENGTH:
                new_page()

            acctno  = coalesce_i(row.get('acctno'))
            noteno  = coalesce_i(row.get('noteno'))
            custcd  = coalesce_s(row.get('custcd') or row.get('ocustcd'))
            sectorcd= coalesce_s(row.get('sectorcd') or row.get('osector'))
            product = coalesce_i(row.get('product') or row.get('oprod'))
            netproc = coalesce_f(row.get('netproc') or row.get('onet'))
            issdte  = row.get('issdte') or row.get('oissdte')
            fisspurp= coalesce_s(row.get('fisspurp') or row.get('ofiss'))

            if isinstance(issdte, (int, float)):
                issdte = sas_date_to_pydate(issdte)

            line = (f" {acctno:>12} {noteno:>7} {custcd:>8} {sectorcd:>7}"
                    f" {product:>8} {fmt_comma15_2(netproc):>15}"
                    f" {fmt_date9(issdte):>10} {fisspurp:>12}")
            lines.append(' ' + line)
            line_cnt += 1
            tot_netproc += netproc

    # SUM line
    lines.append(' ' + '=' * 90)
    sum_line = (f" {'SUM':>12} {'':>7} {'':>8} {'':>7} {'':>8}"
                f" {fmt_comma15_2(tot_netproc):>15}")
    lines.append(' ' + sum_line)

    with open(filepath, 'w', encoding='utf-8') as f:
        for ln in lines:
            f.write(ln + '\n')

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("EIBMHPFS: Starting HP FISS disbursement processing...")

    rv = get_report_vars()
    print(f"  Report date: {rv['reptdate']} MM={rv['reptmon']} YY={rv['reptyear']} WK={rv['nowk']}")

    # -------------------------------------------------------------------------
    # Build HPSETTLE
    # -------------------------------------------------------------------------
    hpsettle_df = build_hpsettle(rv)
    print(f"  HPSETTLE rows: {len(hpsettle_df)}")

    # -------------------------------------------------------------------------
    # Build HPSNR (settle and release same month)
    # -------------------------------------------------------------------------
    bnm_loan     = load_bnm_loan(rv)
    hpsnr_raw    = build_hpsnr(hpsettle_df, rv['sdate'], bnm_loan)
    print(f"  HPSNR rows: {len(hpsnr_raw)}")

    # -------------------------------------------------------------------------
    # Build HP (new disbursements this month)
    # -------------------------------------------------------------------------
    hp_new = build_hp_new(bnm_loan, rv['sdate'])
    print(f"  HP_NEW rows: {len(hp_new)}")

    # -------------------------------------------------------------------------
    # Merge HP + HPSETTLE; produce EXCEPT, HPSNR final, HP
    # -------------------------------------------------------------------------
    hp_df, except_df, hpsnr_df = merge_hp_settle(hpsettle_df, hp_new, hpsnr_raw)
    print(f"  HP (final): {len(hp_df)}, EXCEPT: {len(except_df)}, HPSNR: {len(hpsnr_df)}")

    # -------------------------------------------------------------------------
    # Build LALM records and write RDAL
    # -------------------------------------------------------------------------
    lalm = build_lalm(hp_df)
    print(f"  LALM records: {len(lalm)}")

    write_rdal(lalm, rv)
    print(f"  Written: {RDAL_TXT}")

    # -------------------------------------------------------------------------
    # Write EXCEPT report (ASA, LRECL=133)
    # -------------------------------------------------------------------------
    except_t1 = (f"EXCEPTION REPORT FOR HP DISBURSE FOR MONTH "
                 f"{rv['reptmon']}/{rv['reptyear']}")
    write_exception_report(
        except_df, EXCEPT_TXT,
        title1=except_t1,
        title2='REPORT ID : EIBMHPFS',
        title3='',
    )
    print(f"  Written: {EXCEPT_TXT}")

    # -------------------------------------------------------------------------
    # Write HPSNR report (ASA, LRECL=133)
    # *** A/C SETTLE AND RELEASE ON SAME MONTH ***
    # -------------------------------------------------------------------------
    hpsnr_t1 = (f"HP A/C RELEASED AND SETTLED ON SAME MONTH "
                f"{rv['reptmon']}/{rv['reptyear']}")
    write_exception_report(
        hpsnr_df, HPSNR_TXT,
        title1=hpsnr_t1,
        title2='REPORT ID : EIBMHPFS',
        title3='',
    )
    print(f"  Written: {HPSNR_TXT}")

    print("EIBMHPFS: Processing complete.")


if __name__ == '__main__':
    main()
