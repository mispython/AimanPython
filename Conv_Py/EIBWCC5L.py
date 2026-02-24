# !/usr/bin/env python3
"""
Program Name: EIBWCC5L
Purpose: Process CCRIS collateral data from multiple sources (including ELDS/UCOLL records)
         and output to various fixed-width text files categorized by collateral type:
         property, motor vehicle, other vehicle, plant & machinery, concession,
         financial assets, other assets, financial guarantees, and DCCMS records.
         Each collateral type produces two output files: primary (COLLATER) and ELDS (UCOLL='Y').
"""

import duckdb
import polars as pl
import os
from datetime import date, datetime

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_INPUT_DIR  = "/data/input"
BASE_OUTPUT_DIR = "/data/output"

# Input parquet files
BTRD_MAST_PARQUET        = os.path.join(BASE_INPUT_DIR, "btrd_mast.parquet")           # BTRD.MAST&REPTDAY&REPTMON
BNM_LNACCT_PARQUET       = os.path.join(BASE_INPUT_DIR, "bnm_lnacct.parquet")          # BNM.LNACCT
BNM_REPTDATE_PARQUET     = os.path.join(BASE_INPUT_DIR, "bnm_reptdate.parquet")         # BNM.REPTDATE
COLL_COLLATER_PARQUET    = os.path.join(BASE_INPUT_DIR, "coll_collater.parquet")        # COLL.COLLATER (MNICOL)
CCRISP_OVERDFS_PARQUET   = os.path.join(BASE_INPUT_DIR, "ccrisp_overdfs.parquet")       # CCRISP.OVERDFS
CCRISP_LOAN_PARQUET      = os.path.join(BASE_INPUT_DIR, "ccrisp_loan.parquet")          # CCRISP.LOAN
CCRISP_ELDS_PARQUET      = os.path.join(BASE_INPUT_DIR, "ccrisp_elds.parquet")          # CCRISP.ELDS
CCRISP_SUMM1_PARQUET     = os.path.join(BASE_INPUT_DIR, "ccrisp_summ1.parquet")         # CCRISP.SUMM1

# Primary output text files
COLLATER_OUT  = os.path.join(BASE_OUTPUT_DIR, "COLLATER.txt")
DCCMS_OUT     = os.path.join(BASE_OUTPUT_DIR, "DCCMS.txt")
CPROPETY_OUT  = os.path.join(BASE_OUTPUT_DIR, "CPROPETY.txt")
CMTORVEH_OUT  = os.path.join(BASE_OUTPUT_DIR, "CMTORVEH.txt")
COTHVEHI_OUT  = os.path.join(BASE_OUTPUT_DIR, "COTHVEHI.txt")
CPLANTMA_OUT  = os.path.join(BASE_OUTPUT_DIR, "CPLANTMA.txt")
CCONCESS_OUT  = os.path.join(BASE_OUTPUT_DIR, "CCONCESS.txt")
CFINASST_OUT  = os.path.join(BASE_OUTPUT_DIR, "CFINASST.txt")
COTHASST_OUT  = os.path.join(BASE_OUTPUT_DIR, "COTHASST.txt")
CFINGUAR_OUT  = os.path.join(BASE_OUTPUT_DIR, "CFINGUAR.txt")

# ELDS (UCOLL='Y') output text files
COLLATEX_OUT  = os.path.join(BASE_OUTPUT_DIR, "COLLATER_ELDS.txt")
DCCMX_OUT     = os.path.join(BASE_OUTPUT_DIR, "DCCMS_ELDS.txt")
CPROPETX_OUT  = os.path.join(BASE_OUTPUT_DIR, "CPROPETY_ELDS.txt")
CMTORVEX_OUT  = os.path.join(BASE_OUTPUT_DIR, "CMTORVEH_ELDS.txt")
COTHVEHX_OUT  = os.path.join(BASE_OUTPUT_DIR, "COTHVEHI_ELDS.txt")
CPLANTMX_OUT  = os.path.join(BASE_OUTPUT_DIR, "CPLANTMA_ELDS.txt")
CCONCESX_OUT  = os.path.join(BASE_OUTPUT_DIR, "CCONCESS_ELDS.txt")
CFINASSX_OUT  = os.path.join(BASE_OUTPUT_DIR, "CFINASST_ELDS.txt")
COTHASSX_OUT  = os.path.join(BASE_OUTPUT_DIR, "COTHASST_ELDS.txt")
CFINGUAX_OUT  = os.path.join(BASE_OUTPUT_DIR, "CFINGUAR_ELDS.txt")

os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

# =============================================================================
# VALID ALPHA SET  (ALP macro)
# =============================================================================
ALP = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ")
ALP_UPPER = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def safe_str(val):
    if val is None:
        return ""
    return str(val)

def substr(s, start_1based, length):
    """SAS SUBSTR equivalent (1-based indexing)."""
    s = safe_str(s)
    return s[start_1based - 1: start_1based - 1 + length]

def fmt_num(val, width):
    """Format numeric value right-justified, space-padded integer."""
    try:
        v = int(round(float(val))) if val is not None else 0
    except (TypeError, ValueError):
        v = 0
    return str(v).rjust(width)[:width]

def fmt_z(val, width):
    """Format numeric as zero-padded integer string (SAS Zw. format)."""
    try:
        v = int(round(float(val))) if val is not None else 0
    except (TypeError, ValueError):
        v = 0
    return str(v).zfill(width)[:width]

def fmt_str(val, width):
    """Format string left-justified, space-padded."""
    s = safe_str(val)
    return s.ljust(width)[:width]

def fmt_num_dec(val, width, decimals):
    """Format numeric with decimals, right-justified, space-padded (SAS w.d format)."""
    try:
        v = float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        v = 0.0
    formatted = f"{v:.{decimals}f}"
    return formatted.rjust(width)[:width]

def compress_chars(s, chars=""):
    """SAS COMPRESS: remove specified characters from string."""
    if not s:
        return ""
    remove = set(chars)
    return "".join(c for c in s if c not in remove)

def sas_date_to_python(sas_date):
    """Convert SAS date number (days since 1960-01-01) to Python date."""
    if sas_date is None:
        return None
    try:
        n = int(float(sas_date))
        return date(1960, 1, 1) + __import__("datetime").timedelta(days=n)
    except Exception:
        return None

def python_date_to_sas(d):
    """Convert Python date to SAS date number."""
    if d is None:
        return None
    return (d - date(1960, 1, 1)).days

def ddmmyyn8(val):
    """Format date as DDMMYYYY (SAS DDMMYYN8. format)."""
    if val is None:
        return "        "
    if isinstance(val, (int, float)):
        d = sas_date_to_python(int(val))
    elif isinstance(val, datetime):
        d = val.date()
    elif isinstance(val, date):
        d = val
    else:
        return "        "
    if d is None:
        return "        "
    return d.strftime("%d%m%Y")

def val_date_components(valdt_str, reptday_int, bkmth, bkyyr, bkday):
    """
    Extract VTDD/VTMM/VTYY from a valdt string (DDMMYYYY format),
    applying backdate logic when reptday is in (8, 15, 22).
    """
    s = safe_str(valdt_str).ljust(8)
    try:
        vtdd = int(s[0:2])
        vtmm = int(s[2:4])
        vtyy = int(s[4:8])
    except ValueError:
        vtdd, vtmm, vtyy = 0, 0, 0

    if reptday_int in (8, 15, 22):
        if vtmm > bkmth and vtyy >= bkyyr:
            vtmm = bkmth
            vtyy = bkyyr
            vtdd = bkday

    return vtdd, vtmm, vtyy

def write_fixed(rec_list, length):
    """Join a list-of-chars record and return newline-terminated string."""
    return "".join(rec_list[:length]) + "\n"

def make_rec(length):
    return [" "] * length

def place(rec, start_1, s):
    """Place string s into rec starting at 1-based position start_1."""
    idx = start_1 - 1
    for i, c in enumerate(s):
        if idx + i < len(rec):
            rec[idx + i] = c

# =============================================================================
# STEP 1: READ REPTDATE AND DERIVE MACRO VARIABLES
# =============================================================================
con = duckdb.connect()

reptdate_row = con.execute(
    f"SELECT * FROM read_parquet('{BNM_REPTDATE_PARQUET}') LIMIT 1"
).fetchone()
reptdate_cols = [desc[0] for desc in con.description]
reptdate_dict = dict(zip(reptdate_cols, reptdate_row))

reptdate_val = reptdate_dict["REPTDATE"]
if isinstance(reptdate_val, (int, float)):
    reptdate = sas_date_to_python(int(reptdate_val))
elif isinstance(reptdate_val, datetime):
    reptdate = reptdate_val.date()
elif isinstance(reptdate_val, date):
    reptdate = reptdate_val
else:
    reptdate = datetime.strptime(str(reptdate_val), "%Y-%m-%d").date()

DAYS   = reptdate.day
MONTHS = reptdate.month
YEARS  = reptdate.year

# WK / WK1 / SDD logic
if DAYS == 8:
    SDD = 1;  WK = "1"; WK1 = "4"
elif DAYS == 15:
    SDD = 9;  WK = "2"; WK1 = "1"
elif DAYS == 22:
    SDD = 16; WK = "3"; WK1 = "2"
else:
    SDD = 23; WK = "4"; WK1 = "3"

MM = MONTHS
if WK == "1":
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
else:
    MM1 = MM

if WK == "4":
    MM2 = MM
else:
    MM2 = MM - 1
    if MM2 == 0:
        MM2 = 12

# SDATE = MDY(MONTHS,1,YEARS)  → first day of reporting month
sdate_py   = date(YEARS, MONTHS, 1)
sas_epoch  = date(1960, 1, 1)
SDATE_NUM  = (sdate_py - sas_epoch).days
MDATE_NUM  = (reptdate - sas_epoch).days

NOWK      = WK
NOWK1     = WK1
REPTMON   = f"{MM:02d}"
REPTMON1  = f"{MM1:02d}"
REPTMON2  = f"{MM2:02d}"
REPTYEAR  = str(YEARS)          # YEAR4.
RDATE     = reptdate.strftime("%d/%m/%y")
REPTDAY   = f"{DAYS:02d}"
SDATE_STR = str(SDATE_NUM).zfill(5)
MDATE_STR = str(MDATE_NUM).zfill(5)
REPTDAY_INT = DAYS

# BKDATE = MDATE - REPTDAY
bkdate_num = MDATE_NUM - REPTDAY_INT
bkdate     = sas_date_to_python(bkdate_num)
BKMTH = bkdate.month if bkdate else MONTHS
BKYYR = bkdate.year  if bkdate else YEARS
BKDAY = bkdate.day   if bkdate else 1

print(f"REPTDATE={reptdate}  WK={WK}  REPTMON={REPTMON}  REPTYEAR={REPTYEAR}  REPTDAY={REPTDAY}")
print(f"BKDATE={bkdate}  BKMTH={BKMTH}  BKYYR={BKYYR}  BKDAY={BKDAY}")
print(f"SDATE_NUM={SDATE_NUM}  MDATE_NUM={MDATE_NUM}")

# =============================================================================
# STEP 2: READ AND MERGE BTRD.MAST + BNM.LNACCT → BTRL
# =============================================================================
btrd_df = con.execute(
    f"SELECT * FROM read_parquet('{BTRD_MAST_PARQUET}') ORDER BY ACCTNO"
).pl()

bnm_lnacct_df = con.execute(
    f"SELECT * FROM read_parquet('{BNM_LNACCT_PARQUET}') ORDER BY ACCTNO"
).pl()

# MERGE BTRD(IN=A) BTRL(IN=B); IF A AND ACCTNO > 0 AND SETTLED NE 'S'
btrl_df = btrd_df.join(bnm_lnacct_df, on="ACCTNO", how="inner", suffix="_bnm")
btrl_df = btrl_df.filter(
    (pl.col("ACCTNO") > 0) &
    (pl.col("SETTLED") != "S")
)
btrl_df = btrl_df.with_columns([
    pl.col("FICODE").alias("BRANCH"),
    pl.lit(0).cast(pl.Int64).alias("LOANTYPE"),
])
btrl_df = btrl_df.with_columns([
    pl.when(pl.col("RETAILID") == "C")
      .then(pl.lit(999))
      .otherwise(pl.col("LOANTYPE"))
      .alias("LOANTYPE")
])

# =============================================================================
# STEP 3: LOAD CCRISP.ELDS, CCRISP.OVERDFS, CCRISP.LOAN
# =============================================================================
elds_df = con.execute(
    f"SELECT * FROM read_parquet('{CCRISP_ELDS_PARQUET}') ORDER BY ACCTNO"
).pl()
# DROP=SPOTRATE from ELDS
if "SPOTRATE" in elds_df.columns:
    elds_df = elds_df.drop("SPOTRATE")

overdfs_df = con.execute(
    f"SELECT * FROM read_parquet('{CCRISP_OVERDFS_PARQUET}') ORDER BY ACCTNO"
).pl()

loan_df = con.execute(
    f"SELECT * FROM read_parquet('{CCRISP_LOAN_PARQUET}') ORDER BY ACCTNO, NOTENO"
).pl()
# DROP=FDACCTNO, DROP=SPOTRATE from LOAN
for col in ("FDACCTNO", "SPOTRATE"):
    if col in loan_df.columns:
        loan_df = loan_df.drop(col)

# =============================================================================
# STEP 4: BUILD CCOLLAT (processed COLL.COLLATER)
# =============================================================================
ccollat_raw = con.execute(
    f"SELECT * FROM read_parquet('{COLL_COLLATER_PARQUET}')"
).pl()

def process_ccollat(df: pl.DataFrame) -> pl.DataFrame:
    """Apply EFF_DX / START_DX derivation and filter per SAS logic (18-2830)."""
    rows   = df.to_dicts()
    result = []
    for r in rows:
        eff_dx   = None
        coll_eff = r.get("COLLATERAL_EFF_DT")
        if coll_eff is not None and coll_eff != 0:
            try:
                s8 = str(int(coll_eff)).zfill(11)[:8]   # Z11. first 8 → MMDDYYYY
                eff_dx = datetime.strptime(s8, "%m%d%Y").date()
            except Exception:
                eff_dx = None

        start_dx    = None
        coll_start  = r.get("COLLATERAL_START_DT")
        coll_start_s = str(int(coll_start)).zfill(8) if (coll_start is not None and coll_start != 0) else "00000000"
        if coll_start_s != "00000000":
            try:
                start_dx = datetime.strptime(str(int(coll_start)).zfill(8), "%d%m%Y").date()
            except Exception:
                start_dx = None

        # IF EFF_DX >= '01JUL2018'D & START_DX IN (.,0) THEN DELETE  *18-2830
        if eff_dx is not None and eff_dx >= date(2018, 7, 1):
            if start_dx is None:
                continue

        r["_EFF_DX"]   = eff_dx
        r["_START_DX"] = start_dx
        result.append(r)

    return pl.DataFrame(result) if result else df.clear()

ccollat_df = process_ccollat(ccollat_raw)
ccollat_df = ccollat_df.sort(["ACCTNO", "NOTENO"])

# =============================================================================
# STEP 5: MERGE LOAN + CCOLLAT (inner, DROP BRANCH from CCOLLAT)
# =============================================================================
ccollat_for_loan = ccollat_df
if "BRANCH" in ccollat_for_loan.columns:
    ccollat_for_loan = ccollat_for_loan.drop("BRANCH")

loan_df = loan_df.join(ccollat_for_loan, on=["ACCTNO", "NOTENO"], how="inner", suffix="_col")

# =============================================================================
# STEP 6: MERGE OVERDFS + CCOLLAT (inner), filter
# =============================================================================
overdfs_df = overdfs_df.join(ccollat_df, on="ACCTNO", how="inner", suffix="_col")
overdfs_df = overdfs_df.filter(
    ~(
        (pl.col("APPRLIMT").is_null() | (pl.col("APPRLIMT") == 0)) &
        (~pl.col("ODSTATUS").is_in(["NI", "RI"]))
    )
)

# =============================================================================
# STEP 7: MERGE BTRL + CCOLLAT (inner)
# =============================================================================
btrl_df = btrl_df.sort("ACCTNO")
ccollat_acctno = ccollat_df.unique(subset=["ACCTNO"], keep="first")
btrl_df = btrl_df.join(ccollat_acctno, on="ACCTNO", how="inner", suffix="_col")
btrl_df = btrl_df.with_columns([pl.col("FICODE").alias("BRANCH")])

# =============================================================================
# STEP 8: MERGE ELDS + CCOLLAT (inner, DROP BRANCH AANO NOTENO from CCOLLAT)
#         IF UCOLL='Y' AND AADATE >= SDATE_NUM
# =============================================================================
ccollat_for_elds = ccollat_df
for col in ("BRANCH", "AANO", "NOTENO"):
    if col in ccollat_for_elds.columns:
        ccollat_for_elds = ccollat_for_elds.drop(col)

# Use ACCTNO as the merge key for ELDS (BY ACCTNO in SAS)
# Keep only first CCOLLAT record per ACCTNO for this merge
ccollat_elds_key = ccollat_for_elds.unique(subset=["ACCTNO"], keep="first")
elds_df = elds_df.join(ccollat_elds_key, on="ACCTNO", how="inner", suffix="_col")

# IF UCOLL='Y' AND AADATE >= &SDATE
elds_df = elds_df.filter(
    (pl.col("UCOLL") == "Y") &
    (pl.col("AADATE") >= SDATE_NUM)
)

# =============================================================================
# STEP 9: Combine LOAN + OVERDFS + BTRL + ELDS → COLLATER
# =============================================================================
if "NOTENO" not in overdfs_df.columns:
    overdfs_df = overdfs_df.with_columns(pl.lit(None).cast(pl.Float64).alias("NOTENO"))
if "NOTENO" not in btrl_df.columns:
    btrl_df = btrl_df.with_columns(pl.lit(None).cast(pl.Float64).alias("NOTENO"))
if "NOTENO" not in elds_df.columns:
    elds_df = elds_df.with_columns(pl.lit(None).cast(pl.Float64).alias("NOTENO"))

collater_df = pl.concat(
    [loan_df, overdfs_df, btrl_df, elds_df],
    how="diagonal"
)

# IF NOTENO=. THEN NOTENO=0
collater_df = collater_df.with_columns(
    pl.col("NOTENO").fill_null(0).alias("NOTENO")
)

# =============================================================================
# STEP 10: MERGE COLLATER + CCRISP.SUMM1  (BY AANO; keep COLLATER records)
# =============================================================================
collater_df = collater_df.sort("AANO")

summ1_df = con.execute(
    f"SELECT * FROM read_parquet('{CCRISP_SUMM1_PARQUET}')"
).pl()
summ1_df = summ1_df.sort("AANO")

collater_df = collater_df.join(summ1_df, on="AANO", how="left", suffix="_summ1")
collater_df = collater_df.sort(["ACCTNO", "NOTENO"])

# =============================================================================
# COLLATERAL CLASS → COLLATER CODE LOOKUP
# =============================================================================
# Note: EIBWCC5L has slightly different CCLASSC groupings vs EIIDCC5L:
# - '29' group includes '147'
# - '30' group includes 131-141 (motor-vehicle range moved here)
# - '40' group drops 131-141
# - '50' group drops '111'
# - COTHASST includes '123'
CCLASSC_LOOKUP = {}

for c in ('001','006','007','014','016','024','112','113','114','115','116','117','025','026','046','048','049','147','149'):
    CCLASSC_LOOKUP[c] = '29'
for c in ('000','011','012','013','017','018','019','021','027','028','029','030','124','031','105','106'):
    CCLASSC_LOOKUP[c] = '70'
for c in ('002','003','041','042','043','058','059','067','068','069','070','111','123','071','072','078','079','084','107'):
    CCLASSC_LOOKUP[c] = '90'
for c in ('004','005','127','128','129','142','143','131','132','133','134','135','136','137','138','139','141'):
    CCLASSC_LOOKUP[c] = '30'
for c in ('032','033','034','035','036','037','038','039','040','044','050','051','052','053','054','055','056','057','118','119','121','122','060','061','062'):
    CCLASSC_LOOKUP[c] = '10'
for c in ('065','066','075','076','082','083','093','094','095','096','097','098','101','102','103','104'):
    CCLASSC_LOOKUP[c] = '40'
for c in ('063','064','073','074','080','081'):
    CCLASSC_LOOKUP[c] = '60'
for c in ('010','085','086','087','088','089','090','125','126','144','091','092'):
    CCLASSC_LOOKUP[c] = '50'
for c in ('009','022','023'):
    CCLASSC_LOOKUP[c] = '00'
CCLASSC_LOOKUP['008'] = '21'
for c in ('045','047'):
    CCLASSC_LOOKUP[c] = '22'
CCLASSC_LOOKUP['015'] = '23'
CCLASSC_LOOKUP['020'] = '80'
for c in ('108','109'):
    CCLASSC_LOOKUP[c] = '81'
CCLASSC_LOOKUP['077'] = '99'

# Output routing sets (CCLASSC values → which detail file)
SET_CFINASST = {'001','006','007','014','016','024','112','113','114','115','116','117','025','026','046','048','049','147','149'}
SET_CFINGUAR = {'000','011','012','013','017','018','019','021','027','028','029','030','124','031','105','106'}
SET_COTHASST = {'002','003','041','042','043','058','123','059','067','068','069','070','111','071','072','078','079','084','107'}
SET_CMTORVEH = {'004','005','127','128','129','142','143','131','132','133','134','135','136','137','138','139','141'}
SET_CPROPETY = {'032','033','034','035','036','037','038','039','040','044','050','051','118','119','121','122','052','053','054','055','056','057','060','061','062'}
SET_COTHVEHI = {'065','066','075','076','082','083','093','094','095','096','097','098','101','102','103','104'}
SET_CPLANTMA = {'063','064','073','074','080','081'}
SET_CCONCESS = {'010','085','086','087','088','089','125','126','144','090','091','092'}

VALID_DIGITS = set("0123456789")

def get_collater_code(cclassc, cissuer):
    code = CCLASSC_LOOKUP.get(safe_str(cclassc).strip(), "  ")
    ciss = safe_str(cissuer)
    if ciss[:3] == 'KLM' or ciss[:2] == 'UT':
        code = '23'
    return code

# =============================================================================
# STEP 11: OPEN ALL OUTPUT FILES
# =============================================================================
f_collater  = open(COLLATER_OUT,  "w", encoding="latin-1")
f_collatex  = open(COLLATEX_OUT,  "w", encoding="latin-1")
f_dccms     = open(DCCMS_OUT,     "w", encoding="latin-1")
f_dccmx     = open(DCCMX_OUT,     "w", encoding="latin-1")
f_cpropety  = open(CPROPETY_OUT,  "w", encoding="latin-1")
f_cpropetx  = open(CPROPETX_OUT,  "w", encoding="latin-1")
f_cmtorveh  = open(CMTORVEH_OUT,  "w", encoding="latin-1")
f_cmtorvex  = open(CMTORVEX_OUT,  "w", encoding="latin-1")
f_cothvehi  = open(COTHVEHI_OUT,  "w", encoding="latin-1")
f_cothvehx  = open(COTHVEHX_OUT,  "w", encoding="latin-1")
f_cplantma  = open(CPLANTMA_OUT,  "w", encoding="latin-1")
f_cplantmx  = open(CPLANTMX_OUT,  "w", encoding="latin-1")
f_cconcess  = open(CCONCESS_OUT,  "w", encoding="latin-1")
f_cconcesx  = open(CCONCESX_OUT,  "w", encoding="latin-1")
f_cfinasst  = open(CFINASST_OUT,  "w", encoding="latin-1")
f_cfinassx  = open(CFINASSX_OUT,  "w", encoding="latin-1")
f_cothasst  = open(COTHASST_OUT,  "w", encoding="latin-1")
f_cothassx  = open(COTHASSX_OUT,  "w", encoding="latin-1")
f_cfinguar  = open(CFINGUAR_OUT,  "w", encoding="latin-1")
f_cfinguax  = open(CFINGUAX_OUT,  "w", encoding="latin-1")

# Accumulators for secondary-pass datasets
cpropety_rows = []
cmtorveh_rows = []
cothvehi_rows = []
cplantma_rows = []
cconcess_rows = []
cfinasst_rows = []
cothasst_rows = []
cfinguar_rows = []
dccms_rows    = []

# =============================================================================
# STEP 12: MAIN LOOP — COLLATER + DCCMS records
# =============================================================================
rows = collater_df.to_dicts()

for r in rows:
    purgeind = safe_str(r.get("PURGEIND", "")).strip()
    if purgeind == "N":
        continue   # IF PURGEIND ^= 'N' → skip rows where PURGEIND IS 'N'

    ucoll   = safe_str(r.get("UCOLL", "")).strip()

    # Derived fields
    branch  = int(r.get("BRANCH", 0) or 0)
    if branch == 0:
        branch = int(r.get("NTBRCH", 0) or 0)
    if branch == 0:
        branch = int(r.get("ACCBRCH", 0) or 0)

    collref = r.get("CCOLLNO", 0) or 0
    apcode  = int(r.get("LOANTYPE", 0) or 0)
    ficode  = branch

    cclassc       = safe_str(r.get("CCLASSC", "")).strip()
    cissuer       = safe_str(r.get("CISSUER", ""))
    collater_code = get_collater_code(cclassc, cissuer)

    if collater_code.strip() == "":
        continue   # IF COLLATER EQ '  ' THEN DELETE

    cdolarv  = float(r.get("CDOLARV", 0) or 0)
    cdolarvx = float(r.get("CDOLARVX", 0) or 0)
    collval  = cdolarv  * 100
    collvalx = cdolarvx * 100

    cguarnat = safe_str(r.get("CGUARNAT", ""))
    curcode  = safe_str(r.get("CURCODE", "")).strip()
    if cguarnat != "06" and curcode in ("", "MYR"):
        collvalx = 0.0

    # CINSTCL override  *18-359
    cinstcl = safe_str(r.get("CINSTCL", "")).strip()
    if cinstcl in ("19", "20"):
        cinstcl = "35"
        r["CINSTCL"] = "35"

    # FICODE overrides for specific APCODE / ACCTNO ranges
    acctno = int(r.get("ACCTNO", 0) or 0)
    if 800 <= apcode <= 899:
        if 2000000000 <= acctno <= 2999999999:
            ficode = 904
        if 8000000000 <= acctno <= 8999999999:
            ficode = 904

    # ACTUAL_SALE_VALUE
    succaucpc         = float(r.get("SUCCAUCPC", 0) or 0)
    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)
    if succaucpc > 0:
        actual_sale_value = succaucpc
    if actual_sale_value in (None,) or actual_sale_value == 0:
        actual_sale_value = 0.0

    cprforsv = float(r.get("CPRFORSV", 0) or 0)
    cpresval = float(r.get("CPRESVAL", 0) or 0)

    coll_start = r.get("COLLATERAL_START_DT", 0)
    if coll_start is not None and isinstance(coll_start, (int, float)) and coll_start < 0:
        coll_start = 0

    # Store derived values
    r["_FICODE"]        = ficode
    r["_APCODE"]        = apcode
    r["_COLLREF"]       = collref
    r["_COLLATER_CODE"] = collater_code
    r["_COLLVAL"]       = collval
    r["_COLLVALX"]      = collvalx
    r["_BRANCH"]        = branch
    r["_CINSTCL"]       = cinstcl
    r["ACTUAL_SALE_VALUE"] = actual_sale_value
    r["CPRFORSV"]          = cprforsv
    r["CPRESVAL"]          = cpresval
    r["COLLATERAL_START_DT"] = coll_start

    # --- DCCMS routing ---
    cprdisdt = safe_str(r.get("CPRDISDT", ""))
    if collater_code == "10":
        all_digits = bool(cprdisdt) and all(c in VALID_DIGITS for c in cprdisdt)
        if all_digits:
            dccms_rows.append(dict(r))

    # --- COLLATER record (200 bytes) ---
    rec = make_rec(200)
    place(rec, 1,   fmt_num(ficode, 9))
    place(rec, 10,  fmt_num(apcode, 3))
    place(rec, 13,  fmt_num(acctno, 10))
    place(rec, 43,  fmt_num(r.get("NOTENO", 0), 10))
    place(rec, 73,  fmt_num(collref, 11))
    place(rec, 103, fmt_str(collater_code, 2))
    place(rec, 105, fmt_z(collval, 16))
    place(rec, 121, fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(rec, 126, fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(rec, 130, fmt_str(r.get("AANO", ""), 13))
    place(rec, 143, fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(rec, 148, fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(rec, 154, fmt_num_dec(r.get("CGEXAMTG", 0) or 0, 15, 2))
    place(rec, 170, fmt_z(coll_start or 0, 8))
    place(rec, 178, fmt_str(r.get("COLLATERAL_END_DT", ""), 8))

    # IF UCOLL='Y' → COLLATEX, else COLLATER
    target_collater = f_collatex if ucoll == "Y" else f_collater
    target_collater.write(write_fixed(rec, 200))

    # --- Skip detail output for pure DCCMS records ---
    if collater_code == "10":
        all_digits = bool(cprdisdt) and all(c in VALID_DIGITS for c in cprdisdt)
        if all_digits:
            continue   # VERIFY=0 → output only to DCCMS, not detail files

    # --- Route to detail datasets ---
    if cclassc in SET_CFINASST:
        cfinasst_rows.append(dict(r))
    if cclassc in SET_CFINGUAR:
        cfinguar_rows.append(dict(r))
    if cclassc in SET_COTHASST:
        cothasst_rows.append(dict(r))
    if cclassc in SET_CMTORVEH:
        cmtorveh_rows.append(dict(r))
    if cclassc in SET_CPROPETY:
        cpropety_rows.append(dict(r))
    if cclassc in SET_COTHVEHI:
        cothvehi_rows.append(dict(r))
    if cclassc in SET_CPLANTMA:
        cplantma_rows.append(dict(r))
    if cclassc in SET_CCONCESS:
        cconcess_rows.append(dict(r))

f_collater.close()
f_collatex.close()

# =============================================================================
# STEP 13: WRITE DCCMS / DCCMX
# =============================================================================
for r in dccms_rows:
    ucoll = safe_str(r.get("UCOLL", "")).strip()
    rec = make_rec(200)
    place(rec, 1,   fmt_num(r.get("_FICODE", 0), 9))
    place(rec, 13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(rec, 43,  fmt_num(r.get("NOTENO", 0), 10))
    place(rec, 73,  fmt_num(r.get("_COLLREF", 0), 11))
    place(rec, 103, fmt_str(r.get("_COLLATER_CODE", ""), 2))
    place(rec, 105, fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(rec, 130, fmt_str(r.get("AANO", ""), 13))
    place(rec, 143, fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(rec, 148, fmt_str(safe_str(r.get("CPRDISDT", "")), 8))
    place(rec, 156, fmt_num(r.get("FACCODE", 0) or 0, 5))

    target = f_dccmx if ucoll == "Y" else f_dccms
    target.write(write_fixed(rec, 200))

f_dccms.close()
f_dccmx.close()

# =============================================================================
# STEP 14: WRITE CPROPETY / CPROPETX  (FOR COLLATERAL PROPERTY FILE)
# =============================================================================
# Sort: BY ACCTNO NOTENO DESCENDING CPRPARC1
cpropety_rows = sorted(
    cpropety_rows,
    key=lambda r: (
        int(r.get("ACCTNO", 0) or 0),
        int(r.get("NOTENO", 0) or 0),
        tuple(-ord(c) for c in safe_str(r.get("CPRPARC1", "")).ljust(40)[:40])
    )
)

for r in cpropety_rows:
    ucoll  = safe_str(r.get("UCOLL", "")).strip()
    ficode = r.get("_FICODE", 0) or 0
    apcode = r.get("_APCODE", 0) or 0

    cprforsv   = float(r.get("CPRFORSV", 0) or 0) * 100
    cpresval   = float(r.get("CPRESVAL", 0) or 0) * 100
    ltabtaucpc = float(r.get("LTABTAUCPC", 0) or 0) * 100
    succaucpc  = float(r.get("SUCCAUCPC", 0) or 0) * 100

    vtdd, vtmm, vtyy = val_date_components(
        r.get("CPRVALDT", ""), REPTDAY_INT, BKMTH, BKYYR, BKDAY
    )

    # LANDNREA
    landarea = safe_str(r.get("LANDAREA", "")).strip()
    if landarea in ("0", "", " "):
        landnrea = "0000"
    else:
        try:
            landnrea = str(int(float(landarea)))
        except Exception:
            landnrea = "0000"

    # CPRPAR1C..4C
    def _par(src_col, rec_col=None):
        raw = safe_str(r.get(src_col, "")).ljust(40)[:40]
        first = raw[0:1]
        if first and first.upper() not in ALP_UPPER:
            raw = raw[1:40].ljust(39)[:39].ljust(40)[:40]
        return raw

    cprpar1c = _par("CPRPARC1")
    cprpar2c = _par("CPRPARC2")
    cprpar3c = _par("CPRPARC3")
    cprpar4c = _par("CPRPARC4")

    # Data validations
    cplocat = safe_str(r.get("CPLOCAT", ""))
    if cplocat and cplocat[0].upper() not in ALP_UPPER:
        cplocat = " "

    ownocupy = safe_str(r.get("OWNOCUPY", ""))
    if ownocupy and ownocupy[0].upper() not in ALP_UPPER:
        ownocupy = " "

    cposcode = safe_str(r.get("CPOSCODE", ""))
    if not cposcode or cposcode[0] not in VALID_DIGITS:
        cposcode = "     "

    bltnarea = safe_str(r.get("BLTNAREA", ""))
    if not bltnarea or bltnarea[0] not in VALID_DIGITS:
        bltnarea = "    "

    bltnunit = safe_str(r.get("BLTNUNIT", ""))
    if bltnunit and bltnunit[0].upper() not in ALP_UPPER:
        bltnunit = " "

    landunit = safe_str(r.get("LANDUNIT", ""))
    if landunit and landunit[0].upper() not in ALP_UPPER:
        landunit = " "

    cpstate = safe_str(r.get("CPSTATE", ""))
    if len(cpstate) >= 2 and cpstate[1] not in VALID_DIGITS:
        cpstate = "  "

    # *****************************************************
    # *  READ DATA FROM CMS (PRIMARY) & ELDS (SECONDARY)  *
    # *  ESMR 2011-4011                                   *
    # *****************************************************
    clhseno = safe_str(r.get("CPHSENO", ""))   # HOUSE NO
    cljlnnm = cprpar2c.strip()                  # JALAN
    clbldnm = safe_str(r.get("CPBUILNO", ""))   # BUILDING NAME
    if not clhseno.strip():
        clhseno = safe_str(r.get("ADDRB02", ""))
    if not cljlnnm.strip():
        cljlnnm = safe_str(r.get("ADDRB03", ""))

    # ************************
    # *    ESMR 2012-2277    *
    # ************************
    openind  = safe_str(r.get("OPENIND", ""))
    paidind  = safe_str(r.get("PAIDIND", ""))
    acctstat = safe_str(r.get("ACCTSTAT", ""))
    if openind in ("B", "C", "P") or paidind == "P":
        acctstat = "S"

    # FIREDATE → FIREDATEX
    firedate = safe_str(r.get("FIREDATE", "0"))
    try:
        fd_int  = int(float(firedate))
        fd_str  = str(fd_int).zfill(8)
        frdd    = fd_str[0:2]
        frmm    = fd_str[2:4]
        fryy    = fd_str[4:8]
    except Exception:
        frdd = frmm = fryy = "00"
    firedatex = f"{fryy}-{frmm}-{frdd}"

    # CPOLYNUM compressed
    cpolynum = compress_chars(
        safe_str(r.get("CPOLYNUM", "")),
        "=`&/\\@#*+:().,\"%\xa3!$?_ '"
    )

    # /* REVISED CRITERIA FOR ESMR 2013-616 (commented in SAS)
    # IF ACCTSTAT EQ 'S' OR CDOLARV EQ 0.00 OR
    #    CPRPROPD IN (10,11,15,25,31,32,33,34,35,36,37,38,39) OR
    #    PRABANDT NE '00000000' OR CPRDISDT NE ' ' OR
    #    CPRSTAT EQ 'N' OR PAIDIND EQ 'P' OR
    #    INSURER NOT IN ('01','15','95','98','99') THEN DO;
    #       INSURER = ' '; CPOLYNUM = ' '; FIREDATEX = ' '; SUMINSUR = .;
    # END;
    # */

    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)

    reclen = 1130
    rec = make_rec(reclen)
    place(rec, 1,    fmt_num(ficode, 9))
    place(rec, 10,   fmt_num(apcode, 3))
    place(rec, 13,   fmt_num(r.get("ACCTNO", 0), 10))
    place(rec, 23,   fmt_num(r.get("CCOLLNO", 0), 11))
    place(rec, 36,   fmt_num(r.get("NOTENO", 0), 5))
    place(rec, 43,   fmt_str(r.get("_CINSTCL", ""), 2))
    place(rec, 45,   fmt_str(r.get("CPRRANKC", ""), 1))
    place(rec, 46,   fmt_str(r.get("CPRSHARE", ""), 1))
    place(rec, 47,   fmt_str(r.get("CPRLANDU", ""), 2))
    place(rec, 49,   fmt_str(r.get("CPRPROPD", ""), 2))
    place(rec, 51,   fmt_str(cprpar1c, 40))
    place(rec, 91,   fmt_str(cprpar2c, 40))
    place(rec, 131,  fmt_str(cprpar3c, 40))
    place(rec, 171,  fmt_str(cprpar4c, 30))
    place(rec, 201,  fmt_z(cprforsv, 16))
    place(rec, 217,  fmt_z(cpresval, 16))
    place(rec, 233,  fmt_z(vtdd, 2))
    place(rec, 235,  fmt_z(vtmm, 2))
    place(rec, 237,  fmt_z(vtyy, 4))
    place(rec, 241,  fmt_str(r.get("CPRVALU1", ""), 40))
    place(rec, 281,  fmt_str(r.get("CPRVALU2", ""), 40))
    place(rec, 321,  fmt_str(r.get("CPRVALU3", ""), 20))
    place(rec, 341,  fmt_z(r.get("CPRABNDT", 0) or 0, 8))
    place(rec, 349,  fmt_str(r.get("NAME1OWN", ""), 40))
    place(rec, 389,  fmt_str(ownocupy, 1))
    place(rec, 394,  fmt_str(landnrea, 4))
    place(rec, 398,  fmt_z(r.get("OLDBRH", 0) or 0, 5))
    place(rec, 403,  fmt_str(cplocat, 1))
    place(rec, 404,  fmt_str(cposcode, 5))
    place(rec, 409,  fmt_str(bltnarea, 4))
    place(rec, 413,  fmt_num(r.get("COSTCTR", 0) or 0, 4))
    # /* @0417 MASTTLNO $20. @0437 MASTTLMD $20. 2016-1859 */
    place(rec, 417,  fmt_str(r.get("TTLPARTCLR", ""), 1))
    place(rec, 457,  fmt_str(r.get("MASTOWNR", ""), 40))
    place(rec, 497,  fmt_str(r.get("TTLENO", ""), 40))
    place(rec, 537,  fmt_str(r.get("TTLMUKIM", ""), 40))
    place(rec, 577,  fmt_str(r.get("HOLDEXPD", ""), 1))
    place(rec, 578,  fmt_str(r.get("EXPDATE", ""), 8))
    place(rec, 586,  fmt_str(r.get("DEVNAME", ""), 40))
    place(rec, 626,  fmt_str(r.get("PRJCTNAM", ""), 40))
    cpstate_num = 0
    try:
        cpstate_num = int(cpstate.strip()) if cpstate.strip() else 0
    except Exception:
        cpstate_num = 0
    place(rec, 666,  fmt_num(cpstate_num, 2))
    place(rec, 668,  fmt_str(bltnunit, 1))
    place(rec, 669,  fmt_str(landunit, 1))
    place(rec, 670,  fmt_str(r.get("AANO", ""), 13))
    place(rec, 683,  fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(rec, 688,  fmt_num(r.get("ADLREFNO", 0) or 0, 25))
    place(rec, 713,  fmt_str(r.get("DEVNAME", ""), 60))
    place(rec, 773,  fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(rec, 779,  fmt_str(r.get("CPRSTAT", ""), 1))
    place(rec, 788,  fmt_str(clhseno, 11))    # HOUSE NO
    place(rec, 800,  fmt_str(cljlnnm, 60))    # JALAN NAME
    place(rec, 860,  fmt_str(clbldnm, 60))    # BUILDING NAME
    place(rec, 920,  fmt_str(r.get("INSURER", ""), 2))    # INSURER CODE
    place(rec, 923,  fmt_str(cpolynum, 16))               # POLICY NUMBER
    place(rec, 940,  fmt_str(firedatex, 10))              # EXPIRY DATE
    place(rec, 951,  fmt_num(r.get("SUMINSUR", 0) or 0, 16))  # SUM INSURED
    place(rec, 968,  fmt_str(r.get("CPRPARC3", ""), 40))
    place(rec, 1009, fmt_str(r.get("CPRPARC4", ""), 40))  # 2016-978
    place(rec, 1050, fmt_str(r.get("CTRYCODE", ""), 2))   # COUNTRY CODE
    place(rec, 1052, fmt_str(r.get("AUCIND", ""), 1))     # AUCTION INDICATOR
    place(rec, 1053, fmt_str(r.get("AUCCOMDT", ""), 8))   # AUCTION COMMENCEMENT DATE
    place(rec, 1061, fmt_num(r.get("NOAUC", 0) or 0, 2)) # NO. OF AUCTION
    place(rec, 1063, fmt_str(r.get("AUCSUCCIND", ""), 1)) # AUC SUCCESSFUL INDICATOR
    place(rec, 1064, fmt_num(ltabtaucpc, 10))              # LAST ABORTED AUCTION PRICE
    place(rec, 1074, fmt_str(r.get("SUCCAUCDT", ""), 8))  # SUCCESSFUL AUC DATE
    place(rec, 1082, fmt_num(succaucpc, 10))               # SUCCESSFUL AUC PRICE
    place(rec, 1093, fmt_str(r.get("CPTOWNN", ""), 20))   # PROPERTY TOWN NAME
    place(rec, 1113, fmt_num_dec(actual_sale_value, 16, 2))
    place(rec, 1130, fmt_str(r.get("PROPERTY_IND", ""), 1))

    target = f_cpropetx if ucoll == "Y" else f_cpropety
    target.write(write_fixed(rec, reclen))

f_cpropety.close()
f_cpropetx.close()

# =============================================================================
# STEP 15: WRITE CMTORVEH / CMTORVEX  (COLLATERAL MOTOR VEHICLE)
# =============================================================================
for r in cmtorveh_rows:
    ucoll  = safe_str(r.get("UCOLL", "")).strip()
    ficode = r.get("_FICODE", 0) or 0
    apcode = r.get("_APCODE", 0) or 0

    reclen = 358
    rec = make_rec(reclen)
    place(rec, 1,   fmt_num(ficode, 9))
    place(rec, 10,  fmt_num(apcode, 3))
    place(rec, 13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(rec, 23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(rec, 36,  fmt_num(r.get("NOTENO", 0), 5))
    place(rec, 43,  fmt_str(r.get("_CINSTCL", ""), 2))
    place(rec, 45,  fmt_str(r.get("CHPMAKE", ""), 40))
    place(rec, 85,  fmt_str(r.get("CHPENGIN", ""), 20))
    place(rec, 105, fmt_str(r.get("CHPCHASS", ""), 20))
    place(rec, 125, fmt_str(r.get("CHPVEHNO", ""), 12))
    place(rec, 137, fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(rec, 142, fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(rec, 146, fmt_str(r.get("AANO", ""), 13))
    place(rec, 159, fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(rec, 164, fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(rec, 170, fmt_str(r.get("LU_ADD1", ""), 40))
    place(rec, 210, fmt_str(r.get("LU_ADD2", ""), 40))
    place(rec, 250, fmt_str(r.get("LU_ADD3", ""), 40))
    place(rec, 290, fmt_str(r.get("LU_ADD4", ""), 40))
    place(rec, 330, fmt_str(r.get("LU_TOWN_CITY", ""), 20))
    place(rec, 350, fmt_str(r.get("LU_POSTCODE", ""), 5))
    place(rec, 355, fmt_str(r.get("LU_STATE_CD", ""), 2))
    place(rec, 357, fmt_str(r.get("LU_COUNTRY_CD", ""), 2))

    target = f_cmtorvex if ucoll == "Y" else f_cmtorveh
    target.write(write_fixed(rec, reclen))

f_cmtorveh.close()
f_cmtorvex.close()

# =============================================================================
# STEP 16: WRITE COTHVEHI / COTHVEHX  (COLLATERAL OTHER VEHICLE/CARRIER)
# =============================================================================
for r in cothvehi_rows:
    ucoll  = safe_str(r.get("UCOLL", "")).strip()
    ficode = r.get("_FICODE", 0) or 0
    apcode = r.get("_APCODE", 0) or 0
    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)

    vtdd, vtmm, vtyy = val_date_components(
        r.get("COVVALDT", ""), REPTDAY_INT, BKMTH, BKYYR, BKDAY
    )

    reclen = 332
    rec = make_rec(reclen)
    place(rec, 1,   fmt_num(ficode, 9))
    place(rec, 10,  fmt_num(apcode, 3))
    place(rec, 13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(rec, 23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(rec, 36,  fmt_num(r.get("NOTENO", 0), 5))
    place(rec, 43,  fmt_str(r.get("_CINSTCL", ""), 2))
    place(rec, 45,  fmt_str(r.get("COVDESCR", ""), 2))
    place(rec, 47,  fmt_z(vtdd, 2))
    place(rec, 49,  fmt_z(vtmm, 2))
    place(rec, 51,  fmt_z(vtyy, 4))
    place(rec, 55,  fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(rec, 60,  fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(rec, 64,  fmt_str(r.get("AANO", ""), 13))
    place(rec, 77,  fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(rec, 82,  fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(rec, 127, fmt_num_dec(actual_sale_value, 16, 2))
    place(rec, 144, fmt_str(r.get("LU_ADD1", ""), 40))
    place(rec, 184, fmt_str(r.get("LU_ADD2", ""), 40))
    place(rec, 224, fmt_str(r.get("LU_ADD3", ""), 40))
    place(rec, 264, fmt_str(r.get("LU_ADD4", ""), 40))
    place(rec, 304, fmt_str(r.get("LU_TOWN_CITY", ""), 20))
    place(rec, 324, fmt_str(r.get("LU_POSTCODE", ""), 5))
    place(rec, 329, fmt_str(r.get("LU_STATE_CD", ""), 2))
    place(rec, 331, fmt_str(r.get("LU_COUNTRY_CD", ""), 2))

    target = f_cothvehx if ucoll == "Y" else f_cothvehi
    target.write(write_fixed(rec, reclen))

f_cothvehi.close()
f_cothvehx.close()

# =============================================================================
# STEP 17: WRITE CPLANTMA / CPLANTMX  (COLLATERAL PLANT AND MACHINERY)
# =============================================================================
for r in cplantma_rows:
    ucoll  = safe_str(r.get("UCOLL", "")).strip()
    ficode = r.get("_FICODE", 0) or 0
    apcode = r.get("_APCODE", 0) or 0
    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)
    cprforsv          = float(r.get("CPRFORSV", 0) or 0)

    cpmdescr = safe_str(r.get("CPMDESCR", ""))
    cpmdesc1 = substr(cpmdescr, 2, 1)

    vtdd, vtmm, vtyy = val_date_components(
        r.get("CPMVALDT", ""), REPTDAY_INT, BKMTH, BKYYR, BKDAY
    )

    reclen = 348
    rec = make_rec(reclen)
    place(rec, 1,   fmt_num(ficode, 9))
    place(rec, 10,  fmt_num(apcode, 3))
    place(rec, 13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(rec, 23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(rec, 36,  fmt_num(r.get("NOTENO", 0), 5))
    place(rec, 43,  fmt_str(r.get("_CINSTCL", ""), 2))
    place(rec, 45,  fmt_str(cpmdesc1, 1))
    place(rec, 46,  fmt_z(vtdd, 2))
    place(rec, 48,  fmt_z(vtmm, 2))
    place(rec, 50,  fmt_z(vtyy, 4))
    place(rec, 54,  fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(rec, 59,  fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(rec, 63,  fmt_str(r.get("AANO", ""), 13))
    place(rec, 76,  fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(rec, 81,  fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(rec, 127, fmt_num_dec(actual_sale_value, 16, 2))
    place(rec, 144, fmt_num_dec(cprforsv, 16, 2))
    place(rec, 160, fmt_str(r.get("LU_ADD1", ""), 40))
    place(rec, 200, fmt_str(r.get("LU_ADD2", ""), 40))
    place(rec, 240, fmt_str(r.get("LU_ADD3", ""), 40))
    place(rec, 280, fmt_str(r.get("LU_ADD4", ""), 40))
    place(rec, 320, fmt_str(r.get("LU_TOWN_CITY", ""), 20))
    place(rec, 340, fmt_str(r.get("LU_POSTCODE", ""), 5))
    place(rec, 345, fmt_str(r.get("LU_STATE_CD", ""), 2))
    place(rec, 347, fmt_str(r.get("LU_COUNTRY_CD", ""), 2))

    target = f_cplantmx if ucoll == "Y" else f_cplantma
    target.write(write_fixed(rec, reclen))

f_cplantma.close()
f_cplantmx.close()

# =============================================================================
# STEP 18: WRITE CCONCESS / CCONCESX  (CONCESSION & CONTRACTUAL RIGHTS)
# =============================================================================
for r in cconcess_rows:
    ucoll  = safe_str(r.get("UCOLL", "")).strip()
    ficode = r.get("_FICODE", 0) or 0
    apcode = r.get("_APCODE", 0) or 0

    cccdescr = safe_str(r.get("CCCDESCR", ""))
    cccdesc1 = substr(cccdescr, 2, 1)

    vtdd, vtmm, vtyy = val_date_components(
        r.get("CCCVALDT", ""), REPTDAY_INT, BKMTH, BKYYR, BKDAY
    )

    reclen = 85
    rec = make_rec(reclen)
    place(rec, 1,  fmt_num(ficode, 9))
    place(rec, 10, fmt_num(apcode, 3))
    place(rec, 13, fmt_num(r.get("ACCTNO", 0), 10))
    place(rec, 23, fmt_num(r.get("CCOLLNO", 0), 11))
    place(rec, 36, fmt_num(r.get("NOTENO", 0), 5))
    place(rec, 43, fmt_str(r.get("_CINSTCL", ""), 2))
    place(rec, 45, fmt_str(cccdesc1, 1))
    place(rec, 46, fmt_z(vtdd, 2))
    place(rec, 48, fmt_z(vtmm, 2))
    place(rec, 50, fmt_z(vtyy, 4))
    place(rec, 54, fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(rec, 59, fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(rec, 63, fmt_str(r.get("AANO", ""), 13))
    place(rec, 76, fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(rec, 81, fmt_num(r.get("FACCODE", 0) or 0, 5))

    target = f_cconcesx if ucoll == "Y" else f_cconcess
    target.write(write_fixed(rec, reclen))

f_cconcess.close()
f_cconcesx.close()

# =============================================================================
# STEP 19: WRITE CFINASST / CFINASSX  (COLLATERAL OTHER FINANCIAL ASSETS)
# =============================================================================
for r in cfinasst_rows:
    ucoll    = safe_str(r.get("UCOLL", "")).strip()
    ficode   = r.get("_FICODE", 0) or 0
    apcode   = r.get("_APCODE", 0) or 0
    collvalx = float(r.get("_COLLVALX", 0) or 0)
    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)
    cprforsv          = float(r.get("CPRFORSV", 0) or 0)

    cclassc_r = safe_str(r.get("CCLASSC", "")).strip()
    cinstcl_r = safe_str(r.get("_CINSTCL", ""))
    cfddescr  = safe_str(r.get("CFDDESCR", ""))

    # IF CCLASSC = '147' THEN CINSTCL = '27'; CFDDESCR = '21';
    if cclassc_r == "147":
        cinstcl_r = "27"
        cfddescr  = "21"

    vtdd, vtmm, vtyy = val_date_components(
        r.get("CFDVALDT", ""), REPTDAY_INT, BKMTH, BKYYR, BKDAY
    )

    reclen = 160
    rec = make_rec(reclen)
    place(rec, 1,   fmt_num(ficode, 9))
    place(rec, 10,  fmt_num(apcode, 3))
    place(rec, 13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(rec, 23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(rec, 36,  fmt_num(r.get("NOTENO", 0), 5))
    place(rec, 43,  fmt_str(cinstcl_r, 2))
    place(rec, 45,  fmt_str(cfddescr, 2))
    place(rec, 47,  fmt_z(vtdd, 2))
    place(rec, 49,  fmt_z(vtmm, 2))
    place(rec, 51,  fmt_z(vtyy, 4))
    place(rec, 55,  fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(rec, 60,  fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(rec, 64,  fmt_str(r.get("CURCODE", ""), 3))
    place(rec, 67,  fmt_z(collvalx, 16))
    place(rec, 83,  fmt_num(r.get("FDACCTNO", 0) or 0, 10))
    place(rec, 93,  fmt_num(r.get("FDCDNO", 0) or 0, 10))
    place(rec, 103, fmt_str(r.get("AANO", ""), 13))
    place(rec, 116, fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(rec, 121, fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(rec, 127, fmt_num_dec(actual_sale_value, 16, 2))
    place(rec, 144, fmt_num_dec(cprforsv, 16, 2))

    target = f_cfinassx if ucoll == "Y" else f_cfinasst
    target.write(write_fixed(rec, reclen))

f_cfinasst.close()
f_cfinassx.close()

# =============================================================================
# STEP 20: WRITE COTHASST / COTHASSX  (COLLATERAL OTHER ASSETS)
# =============================================================================
for r in cothasst_rows:
    ucoll    = safe_str(r.get("UCOLL", "")).strip()
    ficode   = r.get("_FICODE", 0) or 0
    apcode   = r.get("_APCODE", 0) or 0
    cpresval = float(r.get("CPRESVAL", 0) or 0)
    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)
    debenture_rv      = float(r.get("DEBENTURE_REALISABLE_VALUE", 0) or 0)

    cdebdesc_orig = safe_str(r.get("CDEBDESC", ""))
    cdebdesc      = substr(cdebdesc_orig, 2, 1)

    vtdd, vtmm, vtyy = val_date_components(
        r.get("CDEBVALD", ""), REPTDAY_INT, BKMTH, BKYYR, BKDAY
    )

    reclen = 142
    rec = make_rec(reclen)
    place(rec, 1,   fmt_num(ficode, 9))
    place(rec, 10,  fmt_num(apcode, 3))
    place(rec, 13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(rec, 23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(rec, 36,  fmt_num(r.get("NOTENO", 0), 5))
    place(rec, 43,  fmt_str(r.get("_CINSTCL", ""), 2))
    place(rec, 45,  fmt_str(cdebdesc, 1))
    place(rec, 46,  fmt_z(vtdd, 2))
    place(rec, 48,  fmt_z(vtmm, 2))
    place(rec, 50,  fmt_z(vtyy, 4))
    place(rec, 54,  fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(rec, 59,  fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(rec, 63,  fmt_str(r.get("AANO", ""), 13))
    place(rec, 76,  fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(rec, 81,  fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(rec, 93,  fmt_num_dec(cpresval, 16, 2))
    place(rec, 110, fmt_num_dec(debenture_rv, 16, 2))
    place(rec, 127, fmt_num_dec(actual_sale_value, 16, 2))

    target = f_cothassx if ucoll == "Y" else f_cothasst
    target.write(write_fixed(rec, reclen))

f_cothasst.close()
f_cothassx.close()

# =============================================================================
# STEP 21: WRITE CFINGUAR / CFINGUAX  (COLLATERAL FINANCIAL GUARANTEES)
# =============================================================================
for r in cfinguar_rows:
    ucoll    = safe_str(r.get("UCOLL", "")).strip()
    ficode   = r.get("_FICODE", 0) or 0
    apcode   = r.get("_APCODE", 0) or 0
    collvalx = float(r.get("_COLLVALX", 0) or 0)

    cguarnat_orig = safe_str(r.get("CGUARNAT", ""))
    cguarna1      = substr(cguarnat_orig, 2, 1)

    # FORMAT CGEFFDAT1 CGEXPDAT1 DDMMYYN.  (DDMMYYYY 8-char format)
    cgeffdat  = r.get("CGEFFDAT")
    cgexpdat  = r.get("CGEXPDAT")
    cgeffdat1_str = ddmmyyn8(cgeffdat)
    cgexpdat1_str = ddmmyyn8(cgexpdat)

    reclen = 473
    rec = make_rec(reclen)
    place(rec, 1,   fmt_num(ficode, 9))
    place(rec, 10,  fmt_num(apcode, 3))
    place(rec, 13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(rec, 23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(rec, 36,  fmt_num(r.get("NOTENO", 0), 5))
    place(rec, 43,  fmt_str(r.get("_CINSTCL", ""), 2))
    place(rec, 45,  fmt_str(cguarna1, 1))
    place(rec, 46,  fmt_str(r.get("CGUARNAM", ""), 40))
    place(rec, 196, fmt_str(r.get("CGUARID", ""), 20))
    place(rec, 216, fmt_str(r.get("CGUARCTY", ""), 2))
    place(rec, 218, fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(rec, 223, fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(rec, 227, fmt_num(r.get("CGCGSR", 0) or 0, 3))
    place(rec, 230, fmt_num(r.get("CGCGUR", 0) or 0, 3))
    place(rec, 233, fmt_str(r.get("AANO", ""), 13))
    place(rec, 246, fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(rec, 251, fmt_str(r.get("CURCODE", ""), 3))
    place(rec, 254, fmt_z(collvalx, 16))
    place(rec, 270, fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(rec, 275, fmt_str(r.get("CGUARLG", ""), 40))
    place(rec, 315, fmt_str(cgeffdat1_str, 8))
    place(rec, 323, fmt_str(cgexpdat1_str, 8))
    place(rec, 331, fmt_num_dec(r.get("CGEXAMTG", 0) or 0, 15, 2))
    place(rec, 347, fmt_num_dec(r.get("GFEERS", 0) or 0, 6, 2))
    place(rec, 354, fmt_num_dec(r.get("GFEERU", 0) or 0, 6, 2))
    place(rec, 361, fmt_num_dec(r.get("GFAMTS", 0) or 0, 15, 2))
    place(rec, 377, fmt_num_dec(r.get("GFAMTU", 0) or 0, 15, 2))
    place(rec, 393, fmt_num_dec(r.get("GCAMTS", 0) or 0, 15, 2))
    place(rec, 409, fmt_num_dec(r.get("GCAMTU", 0) or 0, 15, 2))
    place(rec, 427, fmt_str(r.get("GUARANTOR_ENTITY_TYPE", ""), 2))
    place(rec, 430, fmt_str(r.get("GUARANTOR_BIRTH_REGISTER_DT", ""), 8))
    place(rec, 439, fmt_str(r.get("BNMASSIGNID", ""), 12))
    place(rec, 451, fmt_num(r.get("CUSTNO", 0) or 0, 11))
    place(rec, 462, fmt_str(r.get("NEW_SSM_ID_NO", ""), 12))

    target = f_cfinguax if ucoll == "Y" else f_cfinguar
    target.write(write_fixed(rec, reclen))

f_cfinguar.close()
f_cfinguax.close()

con.close()
print("EIBWCC5L: All output files written successfully.")
