# !/usr/bin/env python3
"""
Program Name: EIBDLNW1
Purpose: Daily Extraction of Loans Information for Data Warehouse
         - Extracts daily loan data from BNM/BNM1 datasets
         - Processes LOAN, HP, HPWO, and INV datasets
         - Applies product classification (LNTYPE), date conversions,
              DAYARR/MTHARR computation, and RETAILID tagging
         - Outputs: LOAN SAS dataset, HP dataset, HPWO dataset,
                    INV (invalid location), DLNFTP/HPDFP/INVLOCFP
                    portable transport files (saved as Parquet)

ESMR: 06-1428
ESMR: 07-1195(RHA) - CHANGE 'PAYEFDT' FROM CHAR TO NUMERIC(SASDATE)
       07-1413
ESMR: 12-180 (MFM) - CREATE HPWO DATASET & EXCLUDE WO PRODUCT
                     FROM LN & HP.
ESMR: 14-2876(TBC) - TAG VB IN LOANS
ESMR: 15-2489(TBC) - TAG RSN IN LOANS
ESMR: 16-1430(TBC)
ESMR: 16-1845(NSA)
ESMR: 17-1823(IFA) - INCLUDE LOAN PRODUCT(392) IN HP DATASET
"""

import os
import struct
from datetime import date, datetime, timedelta
from typing import Optional

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# %INC PGM(PBBLNFMT,PBBELF)
# ---------------------------------------------------------------------------
from PBBLNFMT import (
    format_lnprod,
    format_lndenom,
    HP_ALL,
)
# from PBBELF import ...   # EL/ELI definitions used in other programs

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

# Input parquet paths
BNM1_REPTDATE_PARQUET      = "input/bnm1/reptdate.parquet"
BNM_LOAN_PREFIX            = "input/bnm/loan"          # loan<MM><DD>.parquet
BNM_LNWOD_PREFIX           = "input/bnm/lnwod"         # lnwod<MM><DD>.parquet
BNM_LNWOF_PREFIX           = "input/bnm/lnwof"         # lnwof<MM><DD>.parquet
BNM1_LNNOTE_PARQUET        = "input/bnm1/lnnote.parquet"
ODGP3_GP3_PARQUET          = "input/odgp3/gp3.parquet"
MNITB_CURRENT_PARQUET      = "input/mnitb/current.parquet"
ODGP3_OVERDFT_PARQUET      = "input/odgp3/overdft.parquet"
MISMLN_LNVG_PREFIX         = "input/mismln/lnvg"       # lnvg<MM>.parquet
REFNOTE_TXT                = "input/refnote/ccrisfl.txt"
LNFILE_TOTPAY_PARQUET      = "input/lnfile/totpay.parquet"
CISL_LOAN_PARQUET          = "input/cisl/loan.parquet"
PAYFI_TXT                  = "input/payfi/paysfile.txt"  # packed-decimal binary

# Output paths
LOAN_REPTDATE_PARQUET      = "output/loan/reptdate.parquet"
LOAN_LN_PREFIX             = "output/loan/ln"           # ln<YY><MM><DD>.parquet
HP_HP_PREFIX               = "output/hp/hp"             # hp<YY><MM><DD>.parquet
HP_HPWO_PREFIX             = "output/hp/hpwo"           # hpwo<YY><MM><DD>.parquet
INV_INVLOC_PARQUET         = "output/inv/pbb_invalid_loc.parquet"
DLNFTP_PARQUET             = "output/dlnftp/ln_transport.parquet"
HPDFP_PARQUET              = "output/hpdfp/hp_transport.parquet"
INVLOCFP_PARQUET           = "output/invlocfp/inv_transport.parquet"

os.makedirs("output/loan",    exist_ok=True)
os.makedirs("output/hp",      exist_ok=True)
os.makedirs("output/inv",     exist_ok=True)
os.makedirs("output/dlnftp",  exist_ok=True)
os.makedirs("output/hpdfp",   exist_ok=True)
os.makedirs("output/invlocfp",exist_ok=True)

# =============================================================================
# KEEP COLUMN LISTS  (mirrors SAS %LET macros)
# =============================================================================

DLN_KEEP = [
    'acctno','branch','name','product','acctyind','borstat',
    'sectfiss','statecd','custcode','noteno','secure','intrate',
    'noteterm','issdte','apprlimt','spread','loanstat','undrawn',
    'riskrte','appvalue','marketvl','bldate','bilpay','billtype',
    'remainmh','costctr','balmni','prodcd','amtind','closedte',
    'paytype','payamt','payfreq','remainmt','exprdate',
    'restbalc','apprlmtacct','flag3','payind','nxtbil','forate',
    'intearn','apprlim2','custfiss','fisspurp','fcybal','ccy',
    'bal_aft_eir','eir_adj','dnbfisme','dnbfi_ori','vb',
    'accrual','balance','feeamt','curbal','abm_hl','ia_lru',
    'unearned1','unearned2','unearned','ccris_instlamt',
    'ascore_perm','ascore_ltst','cano','intstdte',
    'fdacctno','fdcertno','earnterm','pointamt','ascore_comm',
    'industrial_sector_cd',
]

LOAN_KEEP = [
    'acctno','noteno','branch','costctr','product','custfiss',
    'sectfiss','fisspurp','balance','curbal','intearn','intamt',
    'feeamt','intearn2','intearn3','intearn4','rebate','accrual',
    'intrate','paidind','exprdate','closedte','lntype','amtind',
    'odintacc','balmni','newbal','costfund','payefdt','payamt',
    'apprlim2','netproc','noteterm','spread','issdte','lasttran',
    'lsttrncd','lsttrnam','cagatag','flag3','payind','nxtbil','cfindex',
    'bldate','borstat','dayarr','mtharr','assmdate','forate','ccy',
    'fcybal','cjfee','retailid','appvalue','dealerno','modeldes',
    'score2','user5','make','model','collyear','cashprice','siacctno',
    'census0','census1','census3','census4','census5','orgissdte',
    'ntindex','tot_migr','oldnotedayarr','oldnotebldate','loanstat',
    'dnbfisme','dnbfi_ori','score2ct','numcpns','rrcountdte',
    'reaccrual','usmargin','apprlimt','payfreq','exprdate',
    'f5acconv','vb','rsn','cp','bal_aft_eir','collage','commno',
    'corpcode','crispurp','custcode','delqcd','dlivrydt','eir_adj',
    'intinytd','lnuser2','nplcrr','ntapr','ntint','orgbal','origrate',
    'paytype','purpose','restind','riskrte','score1','sector',
    'secure','sitype','statecd','u2raceco','vinno','ytdearns','intbasis',
    'restbalc','nxduedt','rebind','abm_hl','ia_lru',
    'mniaplmt','mniapdte','ascore_perm','ascore_ltst',
    'cano','intstdte','mtdavbal_mis','commno_old','escrowrbal',
    'nacospadt','ptmnate','fdb','mostdte','moenddte',
    'mo_tag','mo_main_dt','mo_instl_arr',
    'cpnstdte','dayarr_mo','mtharr_mo','ln_utilise_locat_cd',
    'unearned1','unearned2','unearned','ccris_instlamt',
    'fdacctno','fdcertno','earnterm','pointamt',
    'early_settle_fee_charge_flg','lock_in_end_dt',
    'times_renewed','nurs_tag','nur_startdt','nurs_tagdt',
    'nurs_enddt','nurs_counter','wrioff_dt','wrioff_amt',
    'dsr','repay_source','repay_type_cd','mtd_repaid_amt',
    'valuation_dt','disposed_amt','industrial_sector_cd',
    'cum_wrioff','recover_cost','ascore_comm','f1relmod',
    'fullrel_dt','undrawn','postntrn','insolvency_ind',
    'prompt_pay_tracker','refinanc_ln','old_fi','old_macc_no',
    'old_subacc_no',
    'staff_free_int_ind','staff_free_int_loan_amt',
    'omnibus_facility_ind','marked_payment_amt',
    'marked_payment_ind','risk_grade_class',
    'repo_order_issue_dt','num_repo_order_issue',
    'court_order_apply_dt','court_order_obtain_dt',
    'repay_proposal_cd','refnoteno',
    'digital_rr_status_cd','digital_rr_status_dt',
    'ltst_mgb_score','akpk_status','tfa_nurs_tag',
    'tfa_nurs_tag_dt','tfa_nurs_start_dt',
    'tfa_nurs_end_dt','tfa_nurs_counter',
    'tfa_dig_status_cd','tfa_dig_status_dt',
    'lmostdate','lmoenddate','repay_proposal_dt',
    'manual_rr_tag','manual_rr_dt','auto_reprice_instl_amt',
    'bullet_repay_ind','balloon_repay_ind',
    'prop_develop_fin_ind','com_fee_notice_ind',
    'dia_past01_mth','dia_past02_mth','dia_past03_mth',
    'dia_past04_mth','dia_past05_mth','dia_past06_mth',
    'dia_past07_mth','dia_past08_mth','dia_past09_mth',
    'dia_past10_mth','dia_past11_mth','dia_past12_mth',
    'dia_past13_mth','dia_past14_mth','dia_past15_mth',
    'dia_past16_mth','dia_past17_mth','dia_past18_mth',
    'dia_past19_mth','dia_past20_mth','dia_past21_mth',
    'dia_past22_mth','dia_past23_mth','dia_past24_mth',
    'auto_ext_tag','auto_ext_tag_dt','orig_restind',
    'restind_end_dt','num_mora','auto_reprice_diff_instl_amt',
    'akpk_ra_tag','akpk_ra_tag_dt','akpk_ra_dig_status_cd',
    'akpk_ra_dig_status_dt','akpk_ra_start_dt','akpk_ra_end_dt',
    'akpk_ra_orig_spread','tra_eff_dt','akpk_ra_dly_int_accrual',
    'akpk_ra_mtd_int_accrual','akpk_ra_mth_int_waiver_amt',
    'akpk_ra_cumm_int_waiver_amt','akpk_ra_mth_int_cap_amt',
    'akpk_ra_cumm_int_cap_amt','akpk_ra_orig_ceiling_rt',
    'schbil_int_dt','schbil_instl_dt','lastbil_int_dt',
    'lastbil_instl_dt','num_pay_bil_int','num_pay_bil_instl',
    'mora_benchmark_amt','tra_rr_ind','tra_rr_accept_dt',
    'num_rr','dayarr_mora','hi_tag','hi_tag_dt','hi_dig_status_cd',
    'hi_dig_status_dt','repo_order_expiry_dt','flood_mo_tag',
    'flood_mo_dt','impaired_hp_tag',
]

HPWO_KEEP = [
    'acctno','noteno','payefdt','product','branch','appvalue',
    'noteterm','flag3','costctr','intrate','spread','rebate',
    'intearn','accrual','borstat','payamt','payind','feeamt',
    'bldate','exprdate','issdte','fisspurp','curbal','intamt',
    'balance','custfiss','sectfiss','assmdate','lsttrncd','nxtbil',
    'paidind','netproc','dealerno','intearn2','intearn3','intearn4',
    'lsttrnam','score2','collyear','modeldes','siacctno','cashprice',
    'make','model','dayarr','mtharr','user5','oldnotedayarr',
    'oldnotebldate','loanstat','vb','cp','ascore_perm','ascore_ltst',
    'escrowrbal','nacospadt','ptmnate','cagatag','ascore_comm',
    'valuation_dt','disposed_amt','lasttran','dlivrydt',
    'repay_proposal_cd','repo_order_issue_dt',
    'num_repo_order_issue','court_order_apply_dt',
    'court_order_obtain_dt','dnbfisme','census1','census4',
    'digital_rr_status_cd','digital_rr_status_dt',
    'postntrn','insolvency_ind','mostdte','moenddte',
    'mo_tag','mo_instl_arr','akpk_status',
    'repay_proposal_dt','manual_rr_tag','manual_rr_dt','num_rr',
    'repo_order_expiry_dt',
]

# =============================================================================
# UTILITY HELPERS
# =============================================================================

def coalesce(val, default=0):
    if val is None or (isinstance(val, float) and val != val):
        return default
    return val

def sas_date_to_pydate(val) -> Optional[date]:
    if val is None or val == '' or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, (date, datetime)):
        return val if isinstance(val, date) else val.date()
    return None

def pydate_to_sasdate(d: Optional[date]) -> Optional[int]:
    if d is None:
        return None
    return (d - date(1960, 1, 1)).days

def fmt_ddmmyy8(d: Optional[date]) -> str:
    if d is None:
        return ''
    return d.strftime('%d/%m/%y')

def fmt_year2(d: Optional[date]) -> str:
    if d is None:
        return ''
    return d.strftime('%y')

def fmt_month_z2(d: Optional[date]) -> str:
    if d is None:
        return ''
    return str(d.month).zfill(2)

def fmt_day_z2(d: Optional[date]) -> str:
    if d is None:
        return ''
    return str(d.day).zfill(2)

def parse_zdate_11(val) -> Optional[date]:
    """
    Parse SAS internal numeric date stored as Z11 integer: YYYYMMDD in first 8 chars.
    e.g. 20230115xxx -> 2023-01-15
    """
    if val is None or val == 0 or (isinstance(val, float) and val != val):
        return None
    try:
        s = str(int(val)).zfill(11)
        mmddyy = s[0:8]  # MMDDYYYY
        mm = int(mmddyy[0:2])
        dd = int(mmddyy[2:4])
        yyyy = int(mmddyy[4:8])
        return date(yyyy, mm, dd)
    except (ValueError, TypeError):
        return None

def parse_zdate_yyyymmdd(val) -> Optional[date]:
    """Parse numeric YYYYMMDD stored as Z11 (positions 1-8)."""
    if val is None or val == 0 or (isinstance(val, float) and val != val):
        return None
    try:
        s = str(int(val)).zfill(11)
        yyyy = int(s[0:4])
        mm   = int(s[7:9])
        dd   = int(s[9:11])
        return date(yyyy, mm, dd)
    except (ValueError, TypeError):
        return None

def parse_mmddyy8_from_z11(val) -> Optional[date]:
    """
    SAS: INPUT(SUBSTR(PUT(X,Z11.),1,8),MMDDYY8.)
    Z11 format -> 11-char string, take first 8 as MMDDYYYY.
    """
    if val is None or val == 0 or (isinstance(val, float) and val != val):
        return None
    try:
        s    = str(int(val)).zfill(11)
        part = s[0:8]   # MMDDYYYY
        mm   = int(part[0:2])
        dd   = int(part[2:4])
        yyyy = int(part[4:8])
        return date(yyyy, mm, dd)
    except (ValueError, TypeError):
        return None

def adjust_paydate(payd: int, paym: int, payy: int) -> Optional[date]:
    """Apply SAS end-of-month day clamping logic then MDY conversion."""
    if paym == 2:
        max_d = 29 if (payy % 4 == 0) else 28
        if payd > max_d:
            payd = max_d
    elif paym not in (2,):
        if payd > 31:
            if paym in (1,3,5,7,8,10,12):
                payd = 31
            elif paym in (4,6,9,11):
                payd = 30
    try:
        return date(payy, paym, payd)
    except ValueError:
        return None

def intnx_month_end(d: date) -> date:
    """INTNX('MONTH', date, 0, 'E') — last day of same month."""
    import calendar
    last = calendar.monthrange(d.year, d.month)[1]
    return date(d.year, d.month, last)

# =============================================================================
# MTHARR LOOKUP TABLE (SAS SELECT/WHEN)
# =============================================================================

def compute_mtharr(dayarr: Optional[float]) -> int:
    if dayarr is None or (isinstance(dayarr, float) and dayarr != dayarr):
        return 0
    d = int(dayarr)
    if   d > 729: return int((d / 365) * 12)
    elif d > 698: return 23
    elif d > 668: return 22
    elif d > 638: return 21
    elif d > 608: return 20
    elif d > 577: return 19
    elif d > 547: return 18
    elif d > 516: return 17
    elif d > 486: return 16
    elif d > 456: return 15
    elif d > 424: return 14
    elif d > 394: return 13
    elif d > 364: return 12
    elif d > 333: return 11
    elif d > 303: return 10
    elif d > 273: return 9
    elif d > 243: return 8
    elif d > 213: return 7
    elif d > 182: return 6
    elif d > 151: return 5
    elif d > 121: return 4
    elif d > 89:  return 3
    elif d > 59:  return 2
    elif d > 30:  return 1
    else:         return 0

# =============================================================================
# ARREARS MONTH CONVERSION (SAS NDAYS. informat equivalent)
# =============================================================================

def ndays_to_months(dayarr_mo: float) -> int:
    """
    ARREARS=INPUT(DAYARR_MO,NDAYS.);
    IF ARREARS=24 THEN ARREARS=INT((DAYARR_MO/365)*12);
    MTHARR_MO=ARREARS;
    NDAYS. informat: converts days to month buckets (same as compute_mtharr).
    """
    arrears = compute_mtharr(dayarr_mo)
    if arrears == 24:
        arrears = int((dayarr_mo / 365) * 12)
    return arrears

# =============================================================================
# GET REPORT DATE VARIABLES
# =============================================================================

def get_report_vars() -> dict:
    """Read REPTDATE from BNM1 parquet, return macro variable dict."""
    con = duckdb.connect()
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{BNM1_REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    con.close()

    reptdate    = sas_date_to_pydate(row[0])
    reptyear    = fmt_year2(reptdate)
    reptmon     = fmt_month_z2(reptdate)
    reptday     = fmt_day_z2(reptdate)
    rdate       = fmt_ddmmyy8(reptdate)
    reptdate_sd = pydate_to_sasdate(reptdate)
    lastdate    = intnx_month_end(reptdate)
    lastdt      = pydate_to_sasdate(lastdate)

    return {
        'reptdate':    reptdate,
        'reptdate_sd': reptdate_sd,
        'reptyear':    reptyear,
        'reptmon':     reptmon,
        'reptday':     reptday,
        'rdate':       rdate,
        'lastdt':      lastdt,
        'lastdate':    lastdate,
    }

# =============================================================================
# INV — PBB_INVALID_LOC
# =============================================================================

def build_inv(rv: dict) -> pl.DataFrame:
    """
    DATA INV.PBB_INVALID_LOC(KEEP=ACCTNO NOTENO BRANCH STATE):
      SET BNM.LOAN<MM><DD>; IF INVALID_LOC = 'Y';
    """
    path = f"{BNM_LOAN_PREFIX}{rv['reptmon']}{rv['reptday']}.parquet"
    con  = duckdb.connect()
    df   = con.execute(f"""
        SELECT acctno, noteno, branch, state
        FROM read_parquet('{path}')
        WHERE invalid_loc = 'Y'
    """).pl()
    con.close()
    return df

# =============================================================================
# INITIAL LOAN LOAD
# =============================================================================

def load_initial_loan(rv: dict) -> pl.DataFrame:
    """
    DATA LOAN &DLN:
      SET BNM.LOAN<MM><DD>(DROP=STATE)
          BNM.LNWOD<MM><DD>
          BNM.LNWOF<MM><DD>;
      CUSTFISS=CUSTCD;
      SECTFISS=SECTORCD;
    """
    mm, dd = rv['reptmon'], rv['reptday']
    paths  = [
        f"{BNM_LOAN_PREFIX}{mm}{dd}.parquet",
        f"{BNM_LNWOD_PREFIX}{mm}{dd}.parquet",
        f"{BNM_LNWOF_PREFIX}{mm}{dd}.parquet",
    ]
    con    = duckdb.connect()
    frames = []
    for i, p in enumerate(paths):
        drop_state = "EXCLUDE (state)" if i == 0 else ""
        q = f"SELECT * {drop_state} FROM read_parquet('{p}')" if drop_state else \
            f"SELECT * FROM read_parquet('{p}')"
        frames.append(con.execute(q).pl())
    con.close()

    loan = pl.concat(frames, how='diagonal')

    # CUSTFISS=CUSTCD; SECTFISS=SECTORCD;
    if 'custcd' in loan.columns:
        loan = loan.with_columns(pl.col('custcd').alias('custfiss'))
    if 'sectorcd' in loan.columns:
        loan = loan.with_columns(pl.col('sectorcd').alias('sectfiss'))

    # Keep only DLN columns that exist
    keep = [c for c in DLN_KEEP if c in loan.columns]
    return loan.select(keep)

# =============================================================================
# %GET_GP3 — merge GP3 RISKRTE on last-day-of-month only
# =============================================================================

def get_gp3(loan: pl.DataFrame, rv: dict) -> pl.DataFrame:
    """
    %MACRO GET_GP3: IF LASTDT EQ REPTDATE, merge GP3 RISKRTE.
    """
    if rv['lastdt'] != rv['reptdate_sd']:
        return loan

    con = duckdb.connect()
    gp3 = con.execute(
        f"SELECT acctno, riskcode FROM read_parquet('{ODGP3_GP3_PARQUET}')"
    ).pl()
    con.close()

    gp3 = gp3.with_columns(
        pl.col('riskcode').cast(pl.Utf8).str.slice(0, 1)
          .cast(pl.Float64).alias('riskrte')
    ).select(['acctno', 'riskrte'])

    if 'riskrte' in loan.columns:
        loan = loan.drop('riskrte')

    loan = loan.join(gp3, on='acctno', how='left')
    return loan

# =============================================================================
# OVERDRAFT PROCESSING
# =============================================================================

def build_overdft(rv: dict) -> pl.DataFrame:
    """
    PROC SORT DATA=MNITB.CURRENT WHERE CURBAL<0 BY ACCTNO;
    PROC SORT DATA=ODGP3.OVERDFT RENAME=APPRLIMT=APPRLIM2 NODUPKEY BY ACCTNO;
    DATA OVERDFT: MERGE CURRENT(IN=A) OVERDFT; BY ACCTNO; IF A;
    Compute ODDATE, DAYARR.
    """
    reptdate_sd = rv['reptdate_sd']
    con = duckdb.connect()

    current = con.execute(f"""
        SELECT * FROM read_parquet('{MNITB_CURRENT_PARQUET}')
        WHERE curbal < 0
        ORDER BY acctno
    """).pl()

    overdft_raw = con.execute(f"""
        SELECT *, apprlimt AS apprlim2
        FROM (
            SELECT DISTINCT ON (acctno) *
            FROM read_parquet('{ODGP3_OVERDFT_PARQUET}')
            ORDER BY acctno
        )
    """).pl()
    con.close()

    if 'apprlimt' in overdft_raw.columns:
        overdft_raw = overdft_raw.drop('apprlimt')

    merged = current.join(overdft_raw, on='acctno', how='left', suffix='_od')
    for col in overdft_raw.columns:
        oc = f"{col}_od"
        if oc in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(oc).is_not_null())
                  .then(pl.col(oc))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(oc)

    # Compute ODDATE and DAYARR row-wise
    rows      = merged.to_dicts()
    oddates   = []
    dayarrs   = []

    for row in rows:
        # If number is float, use 0.0; if int, use 0.
        # Assuming exoddate / tempoddt / curbal is numeric, we can use 0 for both cases.
        exoddate = coalesce(row.get('exoddate'), 0)
        tempoddt = coalesce(row.get('tempoddt'), 0)
        curbal   = coalesce(row.get('curbal'),   0)
        oddate   = None

        if (exoddate != 0 or tempoddt != 0) and curbal <= 0:
            exoddt = parse_mmddyy8_from_z11(exoddate) if exoddate != 0 else None
            tempdt = parse_mmddyy8_from_z11(tempoddt) if tempoddt != 0 else None

            exoddt_sd = pydate_to_sasdate(exoddt) if exoddt else None
            tempdt_sd = pydate_to_sasdate(tempdt) if tempdt else None

            if (exoddt_sd not in (None, 0)) and (tempdt_sd not in (None, 0)):
                oddate = exoddt if exoddt_sd < tempdt_sd else tempdt
            else:
                oddate = tempdt if exoddate == 0 else exoddt

        oddate_sd = pydate_to_sasdate(oddate) if oddate else None
        dayarr    = (reptdate_sd - oddate_sd + 1) if (oddate_sd and reptdate_sd) else None
        oddates.append(oddate_sd)
        dayarrs.append(dayarr)

    merged = merged.with_columns([
        pl.Series('oddate',  oddates, dtype=pl.Int64),
        pl.Series('dayarr',  dayarrs, dtype=pl.Float64),
    ])

    keep_cols = ['acctno','dayarr']
    for c in ['apprlim2','ascore_perm','ascore_ltst','ascore_comm','industrial_sector_cd']:
        if c in merged.columns:
            keep_cols.append(c)
    return merged.select([c for c in keep_cols if c in merged.columns])

def merge_loan_overdft(loan: pl.DataFrame, overdft: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN: MERGE LOAN(IN=A) OVERDFT; BY ACCTNO; IF A;
    IF APPRLIM2 EQ . THEN APPRLIM2 = 0;
    """
    loan = loan.join(overdft, on='acctno', how='left', suffix='_od')
    for col in overdft.columns:
        oc = f"{col}_od"
        if oc in loan.columns:
            loan = loan.with_columns(
                pl.when(pl.col(oc).is_not_null())
                  .then(pl.col(oc))
                  .otherwise(pl.col(col) if col in loan.columns else pl.lit(None))
                  .alias(col)
            ).drop(oc)
    if 'apprlim2' in loan.columns:
        loan = loan.with_columns(pl.col('apprlim2').fill_null(0.0))
    else:
        loan = loan.with_columns(pl.lit(0.0).alias('apprlim2'))
    return loan

# =============================================================================
# LNVG MERGE
# =============================================================================

def merge_lnvg(loan: pl.DataFrame, rv: dict) -> pl.DataFrame:
    """DATA LOAN: MERGE LOAN(IN=A) MISMLN.LNVG<MM>; BY ACCTNO NOTENO; IF A;"""
    path = f"{MISMLN_LNVG_PREFIX}{rv['reptmon']}.parquet"
    if not os.path.exists(path):
        return loan
    con    = duckdb.connect()
    lnvg   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    merged = loan.join(lnvg, on=['acctno','noteno'], how='left', suffix='_vg')
    for col in lnvg.columns:
        vc = f"{col}_vg"
        if vc in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(vc).is_not_null())
                  .then(pl.col(vc))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(vc)
    return merged

# =============================================================================
# REFNOTE — read fixed-width .txt (plain text, not packed-decimal)
# =============================================================================

def load_refnote() -> pl.DataFrame:
    """
    DATA REFNOTE:
      INFILE REFNOTE;
      INPUT @001 ACCTNO 11. @012 OLDNOTE 5. @017 NOTENO 5.;
    PROC SORT NODUPKEY; BY ACCTNO NOTENO;
    """
    records = []
    if not os.path.exists(REFNOTE_TXT):
        return pl.DataFrame(schema={'acctno': pl.Int64, 'oldnote': pl.Int64, 'noteno': pl.Int64})
    with open(REFNOTE_TXT, 'rb') as f:
        for line in f:
            rec = line.rstrip(b'\n\r')
            if len(rec) < 21:
                rec = rec.ljust(21)
            try:
                acctno  = int(rec[0:11].decode('ascii', errors='replace').strip() or 0)
                oldnote = int(rec[11:16].decode('ascii', errors='replace').strip() or 0)
                noteno  = int(rec[16:21].decode('ascii', errors='replace').strip() or 0)
                records.append({'acctno': acctno, 'oldnote': oldnote, 'noteno': noteno})
            except (ValueError, UnicodeDecodeError):
                continue
    df = pl.DataFrame(records) if records else \
         pl.DataFrame(schema={'acctno': pl.Int64, 'oldnote': pl.Int64, 'noteno': pl.Int64})
    return df.unique(subset=['acctno','noteno']).sort(['acctno','noteno'])

# =============================================================================
# LNNOTE MERGE + REFNOTE MERGE
# =============================================================================

def merge_lnnote_refnote(loan: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SORT DATA=BNM1.LNNOTE OUT=LNNOTE; BY ACCTNO NOTENO;
    DATA LOAN(DROP=OLDNOTE):
      MERGE LNNOTE(IN=B) LOAN(IN=A) REFNOTE; BY ACCTNO NOTENO;
      CAGATAG=PZIPCODE; SCORE2CT=CONTRTYPE; F1RELMOD=FLAG1; F5ACCONV=FLAG5;
      LNUSER2=USER5; CANO=ESCRACCT; ESCROWRBAL=ECSRRSRV;
      LN_UTILISE_LOCAT_CD=STATE;
      MTHARR_MO / REFNOTENO / BONUSANO logic
      IF A THEN OUTPUT;
    """
    con    = duckdb.connect()
    lnnote = con.execute(
        f"SELECT * FROM read_parquet('{BNM1_LNNOTE_PARQUET}') ORDER BY acctno, noteno"
    ).pl()
    con.close()

    refnote = load_refnote()

    # Merge LOAN + LNNOTE (left — keep loan rows)
    merged = loan.sort(['acctno','noteno']).join(
        lnnote.sort(['acctno','noteno']), on=['acctno','noteno'], how='left', suffix='_ln'
    )
    for col in lnnote.columns:
        lc = f"{col}_ln"
        if lc in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(lc).is_not_null())
                  .then(pl.col(lc))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(lc)

    # Merge REFNOTE
    merged = merged.join(
        refnote, on=['acctno','noteno'], how='left', suffix='_rn'
    )
    for col in refnote.columns:
        rc = f"{col}_rn"
        if rc in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(rc).is_not_null())
                  .then(pl.col(rc))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(rc)

    # Apply field mappings row-wise
    rows        = merged.to_dicts()
    cagatag_l   = []
    score2ct_l  = []
    f1relmod_l  = []
    f5acconv_l  = []
    lnuser2_l   = []
    cano_l      = []
    escrowrbal_l= []
    ln_locat_l  = []
    mtharr_mo_l = []
    refnoteno_l = []

    for row in rows:
        cagatag_l.append(row.get('pzipcode'))
        score2ct_l.append(row.get('contrtype'))
        f1relmod_l.append(row.get('flag1'))
        f5acconv_l.append(row.get('flag5'))
        lnuser2_l.append(row.get('user5'))
        cano_l.append(row.get('escracct'))
        escrowrbal_l.append(row.get('ecsrrsrv'))
        ln_locat_l.append(row.get('state'))

        dayarr_mo = coalesce(row.get('dayarr_mo'), 0)
        if dayarr_mo > 0:
            mtharr_mo = ndays_to_months(dayarr_mo)
        else:
            mtharr_mo = row.get('mtharr_mo')
        mtharr_mo_l.append(mtharr_mo)

        oldnote  = coalesce(row.get('oldnote'), 0)
        bonusano = row.get('bonusano')
        if oldnote not in (None, 0):
            refnoteno_l.append(oldnote)
        else:
            refnoteno_l.append(bonusano)

    merged = merged.with_columns([
        pl.Series('cagatag',            cagatag_l),
        pl.Series('score2ct',           score2ct_l),
        pl.Series('f1relmod',           f1relmod_l),
        pl.Series('f5acconv',           f5acconv_l),
        pl.Series('lnuser2',            lnuser2_l),
        pl.Series('cano',               cano_l),
        pl.Series('escrowrbal',         escrowrbal_l),
        pl.Series('ln_utilise_locat_cd', ln_locat_l),
        pl.Series('mtharr_mo',          mtharr_mo_l),
        pl.Series('refnoteno',          refnoteno_l),
    ])

    # Drop OLDNOTE column as in SAS DATA LOAN(DROP=OLDNOTE)
    if 'oldnote' in merged.columns:
        merged = merged.drop('oldnote')

    return merged

# =============================================================================
# TOTPAY MERGE
# =============================================================================

def merge_totpay(loan: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SORT DATA=LNFILE.TOTPAY OUT=TOTPAY(DROP=DATE); BY ACCTNO NOTENO;
    DATA LOAN: MERGE LOAN(IN=A) TOTPAY(IN=B); BY ACCTNO NOTENO; IF A;
    """
    con    = duckdb.connect()
    totpay = con.execute(f"""
        SELECT * EXCLUDE (date)
        FROM read_parquet('{LNFILE_TOTPAY_PARQUET}')
        ORDER BY acctno, noteno
    """).pl()
    con.close()

    merged = loan.sort(['acctno','noteno']).join(
        totpay, on=['acctno','noteno'], how='left', suffix='_tp'
    )
    for col in totpay.columns:
        tc = f"{col}_tp"
        if tc in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(tc).is_not_null())
                  .then(pl.col(tc))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(tc)
    return merged

# =============================================================================
# OVDFT SECTOR MERGE
# =============================================================================

def merge_ovdft_sector(loan: pl.DataFrame) -> pl.DataFrame:
    """
    DATA OVDFT(KEEP=ACCTNO ODINTACC SECTOR):
      SET MNITB.CURRENT(RENAME=(SECTOR=SECT));
      IF OPENIND NOT IN ('B','C','P') AND CURBAL LT 0;
      SECTOR=PUT(SECT,$4.);
    PROC SORT DATA=OVDFT; BY ACCTNO;
    DATA LOAN: MERGE LOAN(IN=A) OVDFT; BY ACCTNO; IF A;
    """
    con = duckdb.connect()
    raw = con.execute(f"""
        SELECT acctno, odintacc, sector AS sect
        FROM read_parquet('{MNITB_CURRENT_PARQUET}')
        WHERE openind NOT IN ('B','C','P') AND curbal < 0
        ORDER BY acctno
    """).pl()
    con.close()

    # SECTOR=PUT(SECT,$4.)  — left-justify / pad to 4 chars
    ovdft = raw.with_columns(
        pl.col('sect').cast(pl.Utf8).str.ljust(4).alias('sector')
    ).select(['acctno','odintacc','sector'])

    merged = loan.sort('acctno').join(ovdft, on='acctno', how='left', suffix='_ov')
    for col in ['odintacc','sector']:
        oc = f"{col}_ov"
        if oc in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(oc).is_not_null())
                  .then(pl.col(oc))
                  .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                  .alias(col)
            ).drop(oc)
    return merged

# =============================================================================
# LNTYPE + DATE CONVERSIONS (first DATA LOAN(DROP=LASTTRAN) step)
# =============================================================================

def apply_lntype_and_dates(loan: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN(DROP=LASTTRAN):
      SET LOAN;
      LNTYPE logic based on PRODUCT/PRODCD/BRANCH;
      Date conversion for PAYEFDTO, ISSDTE, ORGISSDTE, LASTRAN, ASSMDATE,
      RRCOUNTDTE, INTSTDTE, CPNSTDTE, VALUATION_DT;
      NEWBAL = BALANCE - (FEEAMT + ACCRUAL);
      FULLREL_DT = ORGISSDTE;
    """
    rows = loan.to_dicts()

    lntype_l      = []
    payefdto_l    = []
    newbal_l      = []
    issdte_l      = []
    orgissdte_l   = []
    lastran_l     = []
    assmdate_l    = []
    rrcountdte_l  = []
    intstdte_l    = []
    cpnstdte_l    = []
    valuation_l   = []
    fullrel_l     = []

    for row in rows:
        product = coalesce(row.get('product'), 0)
        prodcd  = str(row.get('prodcd') or '').strip()
        branch  = coalesce(row.get('branch'), 0)
        lntype  = str(row.get('lntype') or '').strip()

        # LNTYPE logic
        if product < 800 or product > 899:
            if branch != 998 and prodcd in ('34180','34240'):
                lntype = 'OD'
            if branch != 998 and product not in (500,520,199,983,993,996) \
               and prodcd not in ('34180','34240'):
                if prodcd == '34120':       lntype = 'HL'
                elif prodcd == '54120':     lntype = 'HL'
                elif prodcd == '34111':     lntype = 'HP'
                elif prodcd in ('34190',):  lntype = 'RC'
                elif prodcd in ('34170',):  lntype = 'ST'
                elif prodcd not in ('M','N'): lntype = 'FL'
        else:
            if prodcd == '34690':
                lntype = 'FR'
            if prodcd == '34600':
                if product in (800,801,805,807,809,811,813): lntype = 'FT'
                if product == 804:                           lntype = 'FS'
                if product not in (800,801,802,803,804,805,807,809,811,813):
                    lntype = 'FN'

        lntype_l.append(lntype)

        # PAYEFDTO
        payeffdt = coalesce(row.get('payeffdt'), 0)
        if payeffdt not in (None, 0):
            s = str(int(payeffdt)).zfill(11)
            # positions 10-11=DD, 8-9=MM, 3-4=YY (SAS Z11 layout)
            dd_s = s[9:11]
            mm_s = s[7:9]
            yy_s = s[2:4]
            payefdto_l.append(f"{dd_s}/{mm_s}/{yy_s}")
        else:
            payefdto_l.append(None)

        # NEWBAL
            # If balance / feeamt / accrual is missing, treat as 0
            # (same as SAS logic where missing numeric is . which behaves like 0 in arithmetic)
        balance = coalesce(row.get('balance'), 0.0)
        feeamt  = coalesce(row.get('feeamt'),  0.0)
        accrual = coalesce(row.get('accrual'), 0.0)
        newbal_l.append(balance - (feeamt + accrual))

        # ISSDTE
        issuedt = coalesce(row.get('issuedt'), 0)
        issdte_l.append(pydate_to_sasdate(parse_mmddyy8_from_z11(issuedt)) if issuedt != 0 else None)

        # ORGISSDTE
        freleas = coalesce(row.get('freleas'), 0)
        orgissdte_l.append(pydate_to_sasdate(parse_mmddyy8_from_z11(freleas)) if freleas != 0 else None)

        # LASTRAN (renamed from LASTTRAN)
        lasttran = coalesce(row.get('lasttran'), 0)
        lastran_l.append(pydate_to_sasdate(parse_mmddyy8_from_z11(lasttran)) if lasttran != 0 else None)

        # ASSMDATE
        assmdate = row.get('assmdate')
        if assmdate is not None and assmdate != '':
            assmdate_l.append(
                pydate_to_sasdate(parse_mmddyy8_from_z11(assmdate))
            )
        else:
            assmdate_l.append(None)

        # RRCOUNTDTE
        fclosuredt = coalesce(row.get('fclosuredt'), 0)
        rrcountdte_l.append(
            pydate_to_sasdate(parse_mmddyy8_from_z11(fclosuredt)) if fclosuredt != 0 else None
        )

        # INTSTDTE (string format YYYYMMDD in positions 2-11)
        intstdte_raw = row.get('intstdte')
        if intstdte_raw not in (None, 0, ''):
            try:
                s  = str(int(intstdte_raw)).zfill(11)
                dd = int(s[10:12]) if len(s) >= 12 else int(s[10:11])
                mm = int(s[8:10])
                yy = int(s[1:5])
                intstdte_l.append(pydate_to_sasdate(date(yy, mm, dd)))
            except (ValueError, IndexError):
                intstdte_l.append(None)
        else:
            intstdte_l.append(None)

        # CPNSTDTE
        cpnstdte = coalesce(row.get('cpnstdte'), 0)
        cpnstdte_l.append(
            pydate_to_sasdate(parse_mmddyy8_from_z11(cpnstdte)) if cpnstdte != 0 else None
        )

        # VALUATION_DT (DDMMYY8. from VALUEDTE string)
        valuedte = row.get('valuedte')
        if valuedte not in (None, 0, ''):
            try:
                s   = str(valuedte).strip()
                dd_ = int(s[0:2]); mm_ = int(s[2:4]); yy_ = int(s[4:8])
                valuation_l.append(pydate_to_sasdate(date(yy_, mm_, dd_)))
            except (ValueError, IndexError):
                valuation_l.append(None)
        else:
            valuation_l.append(None)

        # FULLREL_DT = ORGISSDTE
        fullrel_l.append(orgissdte_l[-1])

    loan = loan.with_columns([
        pl.Series('lntype',      lntype_l,      dtype=pl.Utf8),
        pl.Series('payefdto',    payefdto_l,    dtype=pl.Utf8),
        pl.Series('newbal',      newbal_l,      dtype=pl.Float64),
        pl.Series('issdte',      issdte_l,      dtype=pl.Int64),
        pl.Series('orgissdte',   orgissdte_l,   dtype=pl.Int64),
        pl.Series('lastran',     lastran_l,      dtype=pl.Int64),
        pl.Series('assmdate',    assmdate_l,    dtype=pl.Int64),
        pl.Series('rrcountdte',  rrcountdte_l,  dtype=pl.Int64),
        pl.Series('intstdte',    intstdte_l,    dtype=pl.Int64),
        pl.Series('cpnstdte',    cpnstdte_l,    dtype=pl.Int64),
        pl.Series('valuation_dt',valuation_l,   dtype=pl.Int64),
        pl.Series('fullrel_dt',  fullrel_l,      dtype=pl.Int64),
    ])

    if 'lasttran' in loan.columns:
        loan = loan.drop('lasttran')

    return loan

# =============================================================================
# PAYEFDT ADJUSTMENT (second DATA LOAN step)
# =============================================================================

def apply_payefdt(loan: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN: SET LOAN;
    PAYD/PAYM/PAYY from PAYEFDTO; apply day clamping; PAYEFDT=MDY(PAYM,PAYD,PAYY);
    """
    rows     = loan.to_dicts()
    payefdt_l = []

    for row in rows:
        payefdto = str(row.get('payefdto') or '').strip()
        if len(payefdto) >= 8:
            try:
                payd = int(payefdto[0:2])
                paym = int(payefdto[3:5])
                payy = int(payefdto[6:8])
                full_year = (1900 + payy) if payy >= 50 else (2000 + payy)
                d = adjust_paydate(payd, paym, full_year)
                payefdt_l.append(pydate_to_sasdate(d))
            except (ValueError, IndexError):
                payefdt_l.append(None)
        else:
            payefdt_l.append(None)

    return loan.with_columns(
        pl.Series('payefdt', payefdt_l, dtype=pl.Int64)
    )

# =============================================================================
# PAYFI — read packed-decimal binary file
# =============================================================================

def unpack_bcd(data: bytes, n_digits: int) -> Optional[int]:
    """Unpack IBM packed decimal (PD) bytes to integer."""
    if not data:
        return None
    digits = []
    for b in data:
        high = (b >> 4) & 0x0F
        low  = b & 0x0F
        digits.append(high)
        digits.append(low)
    # Last nibble is sign: C/F=positive, D=negative
    sign_nib = digits[-1]
    num_digits = digits[:-1]
    # Take only required digits
    num_digits = num_digits[-n_digits:] if len(num_digits) >= n_digits else num_digits
    val = int(''.join(str(d) for d in num_digits))
    return -val if sign_nib == 0xD else val

def load_payfi() -> pl.DataFrame:
    """
    DATA PAYFI: INFILE PAYFI;
    INPUT @001 ACCTNO PD6. @007 NOTENO PD3. @011 PAYEFDX PD6.;
    Compute PAYEFDT from PAYEFDX; sort NODUPKEY by ACCTNO NOTENO.
    PD6 = 6 bytes packed decimal = up to 11 significant digits
    PD3 = 3 bytes packed decimal = up to  5 significant digits
    """
    records = []
    if not os.path.exists(PAYFI_TXT):
        return pl.DataFrame(schema={
            'acctno': pl.Int64, 'noteno': pl.Int64, 'payefdt': pl.Int64
        })

    with open(PAYFI_TXT, 'rb') as f:
        rec_len = 16  # 6 + 3 + 1(pad) + 6 = 16 bytes
        while True:
            raw = f.read(rec_len)
            if not raw or len(raw) < 16:
                break
            acctno_bytes  = raw[0:6]
            noteno_bytes  = raw[6:9]
            payefdx_bytes = raw[10:16]

            acctno  = unpack_bcd(acctno_bytes,  11)
            noteno  = unpack_bcd(noteno_bytes,   5)
            payefdx = unpack_bcd(payefdx_bytes, 11)

            if acctno is None or noteno is None:
                continue

            payefdt = None
            if payefdx is not None and payefdx != 0:
                s     = str(int(payefdx)).zfill(11)
                paycy = int(s[0:4])
                paymm = int(s[7:9])
                paydd = int(s[9:11])
                d = adjust_paydate(paydd, paymm, paycy)
                payefdt = pydate_to_sasdate(d)

            records.append({'acctno': acctno, 'noteno': noteno, 'payefdt': payefdt})

    if not records:
        return pl.DataFrame(schema={
            'acctno': pl.Int64, 'noteno': pl.Int64, 'payefdt': pl.Int64
        })

    df = pl.DataFrame(records)
    # PROC SORT DESCENDING PAYEFDT; PROC SORT NODUPKEY — keep first (latest)
    df = df.sort(['acctno','noteno','payefdt'], descending=[False,False,True])
    return df.unique(subset=['acctno','noteno'], keep='first').sort(['acctno','noteno'])

def merge_payfi(loan: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN: MERGE PAYFI LN(IN=A); BY ACCTNO NOTENO; IF A;
    PAYFI overwrites PAYEFDT where present.
    """
    payfi = load_payfi()
    if payfi.is_empty():
        return loan

    # PAYFI PAYEFDT takes precedence over LOAN PAYEFDT
    loan = loan.sort(['acctno','noteno'])
    merged = loan.join(payfi, on=['acctno','noteno'], how='left', suffix='_pf')
    if 'payefdt_pf' in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col('payefdt_pf').is_not_null())
              .then(pl.col('payefdt_pf'))
              .otherwise(pl.col('payefdt'))
              .alias('payefdt')
        ).drop('payefdt_pf')
    return merged

# =============================================================================
# STAFF HL RECLASSIFICATION + LASTTRAN
# =============================================================================

def apply_staff_hl(loan: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN2: SET LOAN;
    IF LNTYPE='FL' AND PRODUCT IN (4,5,6,7,70,100,101) THEN LNTYPE='HL'; *** STAFF HL ***;
    LASTTRAN=LASTRAN;
    """
    loan = loan.with_columns(
        pl.when(
            (pl.col('lntype') == 'FL') &
            pl.col('product').is_in([4,5,6,7,70,100,101])
        ).then(pl.lit('HL'))
         .otherwise(pl.col('lntype'))
         .alias('lntype')
    )
    if 'lastran' in loan.columns:
        loan = loan.with_columns(pl.col('lastran').alias('lasttran'))
    return loan

# =============================================================================
# CIS RACE CODE MERGE
# =============================================================================

def merge_cis_race(loan: pl.DataFrame) -> pl.DataFrame:
    """
    DATA CIS: KEEP=ACCTNO U2RACECO; SET CISL.LOAN; IF SECCUST='901'; U2RACECO=RACE;
    PROC SORT NODUPKEY; BY ACCTNO;
    DATA LOAN2: MERGE CIS LOAN2(IN=A); BY ACCTNO; IF A;
    """
    con = duckdb.connect()
    cis = con.execute(f"""
        SELECT DISTINCT ON (acctno) acctno, race AS u2raceco
        FROM read_parquet('{CISL_LOAN_PARQUET}')
        WHERE seccust = '901'
        ORDER BY acctno
    """).pl()
    con.close()

    if 'u2raceco' in loan.columns:
        loan = loan.drop('u2raceco')

    merged = loan.sort('acctno').join(cis, on='acctno', how='left')
    return merged

# =============================================================================
# FINAL LOAN STEP: DAYARR, MTHARR, RETAILID, CENSUS, MAKE/MODEL
# =============================================================================

def apply_final_fields(loan: pl.DataFrame, rv: dict) -> pl.DataFrame:
    """
    DATA LOAN.LN...: SET LOAN2;
    MAKE/MODEL from COLLDESC; CENSUS0/1/3/4/5; THISDATE; DAYARR; MTHARR; RETAILID; CORPROD.
    """
    reptdate_sd = rv['reptdate_sd']
    thisdate    = reptdate_sd

    rows       = loan.to_dicts()
    make_l     = []; model_l     = []
    census0_l  = []; census1_l  = []; census3_l  = []
    census4_l  = []; census5_l  = []
    dayarr_l   = []; mtharr_l   = []
    collage_l  = []
    retailid_l = []

    corp_products = {31,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,70,71}
    corp_prods_2  = {180,181,182,183,184,193,900,901,902,903,904,905,906,907,908,909,
                     910,914,915,916,917,918,919,920,925,950,951,911,912,638,639,
                     188,189,190,688,689}

    for row in rows:
        colldesc = str(row.get('colldesc') or '').ljust(36)
        make_l.append(colldesc[0:16])
        model_l.append(colldesc[15:36])

        census_raw = row.get('census')
        if census_raw is not None:
            # PUT(CENSUS,7.2): format as xxxxxxx.xx (implied 2 decimal places)
            try:
                cv  = float(census_raw)
                s   = f"{cv:7.2f}".replace('.','')  # "XXXXXXXX" (7+2 chars without dot)
                # SAS PUT(CENSUS,7.2) gives "NNN.NN" — 6 chars total
                sv  = f"{cv:6.2f}"   # e.g. "123.45"
                sv2 = sv.replace('.','')  # "12345"
                census0_l.append(census_raw)
                census1_l.append(sv[0:2] if len(sv) >= 2 else sv)
                census3_l.append(sv[2:3] if len(sv) >= 3 else '')
                census4_l.append(sv[3:4] if len(sv) >= 4 else '')
                census5_l.append(sv[5:7] if len(sv) >= 7 else '')
            except (ValueError, TypeError):
                census0_l.append(None); census1_l.append(None)
                census3_l.append(None); census4_l.append(None); census5_l.append(None)
        else:
            census0_l.append(None); census1_l.append(None)
            census3_l.append(None); census4_l.append(None); census5_l.append(None)

        # DAYARR
        bldate        = coalesce(row.get('bldate'), 0)
        oldnotedayarr = coalesce(row.get('oldnotedayarr'), 0)
        noteno        = coalesce(row.get('noteno'), 0)
        dayarr_mo_v   = row.get('dayarr_mo')

        dayarr = None
        if bldate > 0:
            dayarr = thisdate - bldate
        if oldnotedayarr > 0 and 98000 <= noteno <= 98999:
            if dayarr is not None and dayarr < 0:
                dayarr = 0
            dayarr = (dayarr or 0) + oldnotedayarr
        if dayarr_mo_v is not None and dayarr_mo_v != '' and \
           not (isinstance(dayarr_mo_v, float) and dayarr_mo_v != dayarr_mo_v):
            dayarr = dayarr_mo_v

        dayarr_l.append(dayarr)

        # COLLAGE
        collyear = coalesce(row.get('collyear'), 0)
        if collyear > 0:
            rept_year = sas_date_to_pydate(thisdate).year if thisdate else date.today().year
            collage_l.append(round((rept_year - collyear) + 1, 1))
        else:
            collage_l.append(None)

        # MTHARR
        mtharr_l.append(compute_mtharr(dayarr))

        # RETAILID / CORPROD
        branch  = coalesce(row.get('branch'), 0)
        balance = coalesce(row.get('balance'), 0)       #If balance is missing, treat as 0.0
        lntype  = str(row.get('lntype') or '').strip()
        product = coalesce(row.get('product'), 0)
        acctno  = coalesce(row.get('acctno'), 0)
        retailid = ' '

        if branch != 998 and balance != 0 and lntype != '':
            if 3000000000 <= acctno <= 3999999999:
                if product in corp_products:
                    retailid = 'C'
                else:
                    retailid = 'R'
            elif (800 <= product <= 899) or \
                 (630 <= product <= 649) or \
                 product in corp_prods_2:
                retailid = 'C'
            else:
                retailid = 'R'

        retailid_l.append(retailid)

    return loan.with_columns([
        pl.Series('make',      make_l,      dtype=pl.Utf8),
        pl.Series('model',     model_l,     dtype=pl.Utf8),
        pl.Series('census0',   census0_l),
        pl.Series('census1',   census1_l,   dtype=pl.Utf8),
        pl.Series('census3',   census3_l,   dtype=pl.Utf8),
        pl.Series('census4',   census4_l,   dtype=pl.Utf8),
        pl.Series('census5',   census5_l,   dtype=pl.Utf8),
        pl.Series('dayarr',    dayarr_l,    dtype=pl.Float64),
        pl.Series('mtharr',    mtharr_l,    dtype=pl.Int64),
        pl.Series('collage',   collage_l,   dtype=pl.Float64),
        pl.Series('retailid',  retailid_l,  dtype=pl.Utf8),
    ])

# =============================================================================
# SPLIT INTO LN, HP, HPWO DATASETS
# =============================================================================

# HP products list  (from PBBLNFMT HP_ALL + product 392)
HP_PRODUCTS      = set(HP_ALL) | {392}
HP_WO_PRODUCTS   = {678, 679, 698, 699, 983, 993, 996}

def split_outputs(loan_final: pl.DataFrame) -> tuple:
    """
    DATA LOAN.LN... LN... HP.HP... HP.HPWO...:
      SET LOAN.LN...;
      IF PRODUCT IN &HP OR PRODUCT=392 THEN ...
        IF PRODUCT IN (678,679,698,699,983,993,996) -> HPWO  ELSE -> HP
      IF PRODUCT NOT IN (678,...996) -> LOAN.LN...
      OUTPUT LN... (always)
    Returns: (ln_df, hp_df, hpwo_df)
    """
    rows    = loan_final.to_dicts()
    ln_rows = []; hp_rows = []; hpwo_rows = []

    for row in rows:
        product = coalesce(row.get('product'), 0)
        is_hp   = product in HP_PRODUCTS

        if is_hp:
            if product in HP_WO_PRODUCTS:
                hpwo_rows.append(row)
            else:
                hp_rows.append(row)

        # EXCLUDE WRITTEN-OFF/DOWN PRODUCT from LOAN.LN
        if product not in HP_WO_PRODUCTS:
            pass  # will be included in loan_ln below

        # OUTPUT LN (always — includes WO products)
        ln_rows.append(row)

    ln_df   = pl.from_dicts(ln_rows)   if ln_rows   else pl.DataFrame()
    hp_df   = pl.from_dicts(hp_rows)   if hp_rows   else pl.DataFrame()
    hpwo_df = pl.from_dicts(hpwo_rows) if hpwo_rows else pl.DataFrame()

    # loan_ln excludes WO products
    loan_ln_rows = [r for r in ln_rows if coalesce(r.get('product'),0) not in HP_WO_PRODUCTS]
    loan_ln_df   = pl.from_dicts(loan_ln_rows) if loan_ln_rows else pl.DataFrame()

    return loan_ln_df, ln_df, hp_df, hpwo_df

def select_keep_cols(df: pl.DataFrame, keep: list) -> pl.DataFrame:
    """Select only columns that exist in df from keep list."""
    cols = [c for c in keep if c in df.columns]
    return df.select(cols) if cols else df

def drop_dayarr_mo_mtharr_mo(df: pl.DataFrame) -> pl.DataFrame:
    drops = [c for c in ['dayarr_mo','mtharr_mo'] if c in df.columns]
    return df.drop(drops) if drops else df

# =============================================================================
# WRITE PARQUET OUTPUTS
# =============================================================================

def save_parquet(df: pl.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.write_parquet(path)
    print(f"  Saved: {path} ({len(df)} rows)")

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("EIBDLNW1: Starting daily loan data warehouse extraction...")

    # -------------------------------------------------------------------------
    # GET REPTDATE
    # -------------------------------------------------------------------------
    rv = get_report_vars()
    print(f"  Report date: {rv['reptdate']} ({rv['rdate']}), "
          f"YY={rv['reptyear']} MM={rv['reptmon']} DD={rv['reptday']}")

    # Save REPTDATE dataset
    reptdate_df = pl.DataFrame({'reptdate': [rv['reptdate_sd']]})
    save_parquet(reptdate_df, LOAN_REPTDATE_PARQUET)

    # -------------------------------------------------------------------------
    # PROC PRINT DATA=LOAN.REPTDATE
    # -------------------------------------------------------------------------
    print(f"\nPROC PRINT DATA=LOAN.REPTDATE\n  reptdate = {rv['reptdate']}")

    # -------------------------------------------------------------------------
    # INV — PBB_INVALID_LOC
    # -------------------------------------------------------------------------
    inv_df = build_inv(rv)
    save_parquet(inv_df, INV_INVLOC_PARQUET)
    save_parquet(inv_df, INVLOCFP_PARQUET)

    # -------------------------------------------------------------------------
    # Initial LOAN load
    # -------------------------------------------------------------------------
    loan = load_initial_loan(rv)
    print(f"  Initial LOAN rows: {len(loan)}")

    # PROC SORT DATA=BNM1.LNNOTE BY ACCTNO NOTENO; handled inside merge_lnnote_refnote
    # PROC SORT DATA=LOAN BY ACCTNO NOTENO;
    loan = loan.sort(['acctno','noteno'])

    # -------------------------------------------------------------------------
    # %GET_GP3
    # -------------------------------------------------------------------------
    loan = get_gp3(loan, rv)

    # -------------------------------------------------------------------------
    # OD: CURRENT + OVERDFT -> DAYARR, APPRLIM2
    # -------------------------------------------------------------------------
    overdft = build_overdft(rv)
    loan    = merge_loan_overdft(loan, overdft)

    # -------------------------------------------------------------------------
    # LNVG merge
    # -------------------------------------------------------------------------
    loan = merge_lnvg(loan, rv)

    # -------------------------------------------------------------------------
    # REFNOTE load + LNNOTE + REFNOTE merge
    # -------------------------------------------------------------------------
    loan = merge_lnnote_refnote(loan)

    # -------------------------------------------------------------------------
    # PROC SORT LOAN BY ACCTNO NOTENO; TOTPAY merge
    # -------------------------------------------------------------------------
    loan = merge_totpay(loan)

    # -------------------------------------------------------------------------
    # OVDFT SECTOR merge
    # -------------------------------------------------------------------------
    loan = merge_ovdft_sector(loan)

    # -------------------------------------------------------------------------
    # LNTYPE + date conversions (DATA LOAN DROP=LASTTRAN)
    # -------------------------------------------------------------------------
    loan = apply_lntype_and_dates(loan)

    # -------------------------------------------------------------------------
    # PAYEFDT day-clamping (second DATA LOAN step)
    # -------------------------------------------------------------------------
    loan = apply_payefdt(loan)

    # -------------------------------------------------------------------------
    # PAYFI merge (packed-decimal binary input)
    # -------------------------------------------------------------------------
    loan = merge_payfi(loan)

    # -------------------------------------------------------------------------
    # Staff HL reclassification + LASTTRAN
    # -------------------------------------------------------------------------
    loan = apply_staff_hl(loan)

    # -------------------------------------------------------------------------
    # CIS race code merge; PROC SORT BY ACCTNO NOTENO
    # -------------------------------------------------------------------------
    loan = merge_cis_race(loan)
    loan = loan.sort(['acctno','noteno'])

    # -------------------------------------------------------------------------
    # Final fields: DAYARR, MTHARR, RETAILID, CENSUS, MAKE/MODEL (LOAN.LN step)
    # -------------------------------------------------------------------------
    loan_final = apply_final_fields(loan, rv)
    print(f"  LOAN final rows: {len(loan_final)}")

    # -------------------------------------------------------------------------
    # Split: LOAN.LN (no WO), LN (all), HP, HPWO
    # -------------------------------------------------------------------------
    loan_ln_df, ln_df, hp_df, hpwo_df = split_outputs(loan_final)

    # Apply DROP=DAYARR_MO MTHARR_MO
    loan_ln_df = drop_dayarr_mo_mtharr_mo(loan_ln_df)
    ln_df      = drop_dayarr_mo_mtharr_mo(ln_df)

    # HP: DROP=F5ACCONV CCRIS_INSTLAMT MO_MAIN_DT
    hp_drops   = [c for c in ['f5acconv','ccris_instlamt','mo_main_dt'] if c in hp_df.columns]
    hp_df      = hp_df.drop(hp_drops) if hp_drops else hp_df
    hp_df      = drop_dayarr_mo_mtharr_mo(hp_df)

    # HPWO: apply HPWO_KEEP
    hpwo_df    = select_keep_cols(hpwo_df, HPWO_KEEP)

    # -------------------------------------------------------------------------
    # Save outputs
    # -------------------------------------------------------------------------
    yy, mm, dd = rv['reptyear'], rv['reptmon'], rv['reptday']

    # LOAN.LN (excludes WO products) — with LOAN_KEEP columns
    loan_ln_df = select_keep_cols(loan_ln_df, LOAN_KEEP)
    save_parquet(loan_ln_df, f"{LOAN_LN_PREFIX}{yy}{mm}{dd}.parquet")

    # LN (WORK — all records, DLNFTP transport)
    ln_df = select_keep_cols(ln_df, LOAN_KEEP)
    save_parquet(ln_df, DLNFTP_PARQUET)

    # HP and HPWO
    save_parquet(hp_df,   f"{HP_HP_PREFIX}{yy}{mm}{dd}.parquet")
    save_parquet(hpwo_df, f"{HP_HPWO_PREFIX}{yy}{mm}{dd}.parquet")
    save_parquet(hp_df,   HPDFP_PARQUET)

    # -------------------------------------------------------------------------
    # PROC PRINT DATA=LOAN.LN... (OBS=50)
    # -------------------------------------------------------------------------
    print(f"\nPROC PRINT DATA=LOAN.LN{yy}{mm}{dd} (OBS=50)")
    preview_cols = ['acctno','noteno','branch','product','lntype','balance',
                    'dayarr','mtharr','retailid']
    avail = [c for c in preview_cols if c in loan_ln_df.columns]
    print(loan_ln_df.select(avail).head(50))

    print("\nEIBDLNW1: Processing complete.")


if __name__ == '__main__':
    main()
