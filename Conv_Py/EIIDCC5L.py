# !/usr/bin/env python3
"""
Program Name: EIIDCC5L
Purpose: Process CCRIS collateral data from multiple sources and output to various
            fixed-width text files categorized by collateral type (property, motor vehicle,
            other vehicle, plant & machinery, concession, financial assets, other assets,
            financial guarantees, and DCCMS records).
"""

import duckdb
import polars as pl
import os
import re
from datetime import date, datetime

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_INPUT_DIR  = "/data/input"
BASE_OUTPUT_DIR = "/data/output"

# Input parquet files
BTRD_IMAST_PARQUET = os.path.join(BASE_INPUT_DIR, "btrd_imast.parquet")       # BTRD.IMAST&REPTYEAR&REPTMON&REPTDAY
BNM_LNACCT_PARQUET = os.path.join(BASE_INPUT_DIR, "bnm_lnacct.parquet")       # BNM.LNACCT
BNM_REPTDATE_PARQUET = os.path.join(BASE_INPUT_DIR, "bnm_reptdate.parquet")   # BNM.REPTDATE
COLL_COLLATER_PARQUET = os.path.join(BASE_INPUT_DIR, "coll_collater.parquet") # COLL.COLLATER (MNICOL)
CCRISP_OVERDFS_PARQUET = os.path.join(BASE_INPUT_DIR, "ccrisp_overdfs.parquet") # CCRISP.OVERDFS
CCRISP_LOAN_PARQUET = os.path.join(BASE_INPUT_DIR, "ccrisp_loan.parquet")     # CCRISP.LOAN

# Output text files
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

os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

# =============================================================================
# VALID ALPHA SET  (ALP macro)
# =============================================================================
ALP = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ")

# =============================================================================
# HELPER FORMATTING FUNCTIONS
# =============================================================================

def fmt_num(val, width, decimals=0):
    """Format numeric value right-justified, space-padded, optional decimals."""
    try:
        v = float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        v = 0.0
    if decimals:
        s = f"{v:{'0' if False else ''}.{decimals}f}"
        # width includes the decimal point and decimal digits
        s = s.rjust(width)
    else:
        s = str(int(round(v))).rjust(width)
    return s[:width]

def fmt_z(val, width):
    """Format numeric as zero-padded integer string (SAS Zw. format)."""
    try:
        v = int(round(float(val))) if val is not None else 0
    except (TypeError, ValueError):
        v = 0
    return str(v).zfill(width)[:width]

def fmt_str(val, width):
    """Format string left-justified, space-padded."""
    s = str(val) if val is not None else ""
    return s.ljust(width)[:width]

def fmt_num_dec(val, width, decimals):
    """Format numeric with decimals, right-justified, space-padded (SAS w.d format)."""
    try:
        v = float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        v = 0.0
    formatted = f"{v:.{decimals}f}"
    return formatted.rjust(width)[:width]

def safe_str(val):
    if val is None:
        return ""
    return str(val)

def substr(s, start_1based, length):
    """SAS SUBSTR equivalent (1-based)."""
    s = safe_str(s)
    return s[start_1based - 1: start_1based - 1 + length]

def compress_chars(s, chars=""):
    """SAS COMPRESS: remove specified characters from string."""
    if not s:
        return ""
    remove = set(chars) if chars else set()
    return "".join(c for c in s if c not in remove)

def ddmmyyn(d):
    """Format date as DDMMYYYY (SAS DDMMYYN8. format)."""
    if d is None:
        return "        "
    if isinstance(d, (int, float)):
        # SAS date number: days since 1960-01-01
        try:
            d = date(1960, 1, 1) + __import__("datetime").timedelta(days=int(d))
        except Exception:
            return "        "
    if isinstance(d, (date, datetime)):
        return d.strftime("%d%m%Y")
    return "        "

def sas_date_to_python(sas_date):
    """Convert SAS date number to Python date."""
    if sas_date is None or sas_date == 0:
        return None
    try:
        return date(1960, 1, 1) + __import__("datetime").timedelta(days=int(sas_date))
    except Exception:
        return None

# =============================================================================
# STEP 1: READ REPTDATE AND DERIVE MACRO VARIABLES
# =============================================================================
con = duckdb.connect()

reptdate_df = con.execute(f"SELECT * FROM read_parquet('{BNM_REPTDATE_PARQUET}')").pl()
row = reptdate_df.row(0, named=True)

# REPTDATE is a SAS date integer or Python date
reptdate_val = row["REPTDATE"]
if isinstance(reptdate_val, (int, float)):
    reptdate = sas_date_to_python(int(reptdate_val))
elif isinstance(reptdate_val, (date, datetime)):
    reptdate = reptdate_val if isinstance(reptdate_val, date) else reptdate_val.date()
else:
    reptdate = datetime.strptime(str(reptdate_val), "%Y-%m-%d").date()

DAYS   = reptdate.day
MONTHS = reptdate.month
YEARS  = reptdate.year

# YEARCUTOFF=1950 affects 2-digit year interpretation; handled where needed.
# WK logic
if DAYS == 8:
    SDD = 1;  WK = "1"; WK1 = "4"
elif DAYS == 15:
    SDD = 9;  WK = "2"; WK1 = "1"
elif DAYS == 22:
    SDD = 16; WK = "3"; WK1 = "2"
else:
    SDD = 23; WK = "4"; WK1 = "3"

MM  = MONTHS
# MM1
if WK == "1":
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
else:
    MM1 = MM
# MM2
if WK == "4":
    MM2 = MM
else:
    MM2 = MM - 1
    if MM2 == 0:
        MM2 = 12

# Macro variables
NOWK     = WK
NOWK1    = WK1
REPTMON  = f"{MM:02d}"
REPTMON1 = f"{MM1:02d}"
REPTMON2 = f"{MM2:02d}"
REPTYEAR = str(YEARS)[-2:]    # YEAR2. = 2-digit year
RDATE    = reptdate.strftime("%d/%m/%Y")  # DDMMYY8. → DD/MM/YY actually DDMMYY8 = dd/mm/yy
REPTDAY  = f"{DAYS:02d}"

# SDATE: MDY(MONTHS,DAYS,YEARS) formatted as Z5. (SAS date number zero-padded 5 digits)
sdate_py = date(YEARS, MONTHS, DAYS)
sas_epoch = date(1960, 1, 1)
SDATE_NUM = (sdate_py - sas_epoch).days
MDATE_NUM = (reptdate - sas_epoch).days

SDATE_STR = str(SDATE_NUM).zfill(5)
MDATE_STR = str(MDATE_NUM).zfill(5)

# Numeric values for comparisons
REPTDAY_INT = DAYS

# BKDATE = MDATE - REPTDAY  (subtract day-of-month to get first-of-month-like date)
bkdate_num = MDATE_NUM - REPTDAY_INT
bkdate = sas_date_to_python(bkdate_num)
BKMTH = bkdate.month if bkdate else MONTHS
BKYYR = bkdate.year if bkdate else YEARS
BKDAY = bkdate.day if bkdate else 1

print(f"REPTDATE: {reptdate}, WK={WK}, REPTMON={REPTMON}, REPTYEAR={REPTYEAR}, REPTDAY={REPTDAY}")
print(f"BKDATE: {bkdate}, BKMTH={BKMTH}, BKYYR={BKYYR}, BKDAY={BKDAY}")

# =============================================================================
# STEP 2: READ AND MERGE BTRD + BNM.LNACCT  → BTRL
# =============================================================================
btrd_df = con.execute(f"""
    SELECT * FROM read_parquet('{BTRD_IMAST_PARQUET}')
    ORDER BY ACCTNON
""").pl()

# DROP=ACCTNO RENAME=(ACCTNON=ACCTNO)
if "ACCTNO" in btrd_df.columns:
    btrd_df = btrd_df.drop("ACCTNO")
btrd_df = btrd_df.rename({"ACCTNON": "ACCTNO"})

bnm_lnacct_df = con.execute(f"""
    SELECT * FROM read_parquet('{BNM_LNACCT_PARQUET}')
    ORDER BY ACCTNO
""").pl()

# MERGE BTRD(IN=A) BTRL(IN=B); IF A AND ACCTNO > 0 AND SETTLED NE 'S'
btrl_df = btrd_df.join(bnm_lnacct_df, on="ACCTNO", how="inner", suffix="_bnm")
btrl_df = btrl_df.filter(
    (pl.col("ACCTNO") > 0) &
    (pl.col("SETTLED") != "S")
)
btrl_df = btrl_df.with_columns([
    pl.col("FICODE").alias("BRANCH"),
    pl.lit(0).alias("LOANTYPE"),
])
# IF RETAILID='C' THEN LOANTYPE=999
btrl_df = btrl_df.with_columns([
    pl.when(pl.col("RETAILID") == "C")
      .then(pl.lit(999))
      .otherwise(pl.col("LOANTYPE"))
      .alias("LOANTYPE")
])

# =============================================================================
# STEP 3: READ CCRISP.OVERDFS, CCRISP.LOAN, COLL.COLLATER
# =============================================================================
overdfs_df = con.execute(f"""
    SELECT * FROM read_parquet('{CCRISP_OVERDFS_PARQUET}')
    ORDER BY ACCTNO
""").pl()

loan_df = con.execute(f"""
    SELECT * FROM read_parquet('{CCRISP_LOAN_PARQUET}')
    ORDER BY ACCTNO, NOTENO
""").pl()
# DROP=FDACCTNO from loan
if "FDACCTNO" in loan_df.columns:
    loan_df = loan_df.drop("FDACCTNO")

# CCOLLAT: process COLL.COLLATER
ccollat_df = con.execute(f"""
    SELECT * FROM read_parquet('{COLL_COLLATER_PARQUET}')
""").pl()

def process_ccollat(df):
    rows = df.to_dicts()
    result = []
    for r in rows:
        # EFF_DX: if COLLATERAL_EFF_DT not in (.,0)
        eff_dx = None
        coll_eff = r.get("COLLATERAL_EFF_DT")
        if coll_eff is not None and coll_eff != 0:
            try:
                s = str(int(coll_eff)).zfill(11)[:8]   # first 8 chars of Z11 representation
                # parse as MMDDYY8.
                eff_dx = datetime.strptime(s, "%m%d%Y").date()
            except Exception:
                eff_dx = None

        # START_DX: if COLLATERAL_START_DT NE '00000000'
        start_dx = None
        coll_start = r.get("COLLATERAL_START_DT")
        coll_start_str = str(int(coll_start)).zfill(8) if coll_start is not None and coll_start != 0 else "00000000"
        if coll_start_str != "00000000":
            try:
                start_dx = datetime.strptime(str(int(coll_start)).zfill(8), "%d%m%Y").date()
            except Exception:
                start_dx = None

        # IF EFF_DX >= '01JUL2018'D & START_DX IN (.,0) THEN DELETE
        cutoff = date(2018, 7, 1)
        if eff_dx is not None and eff_dx >= cutoff:
            if start_dx is None:
                continue   # delete this row

        r["_EFF_DX"]   = eff_dx
        r["_START_DX"] = start_dx
        result.append(r)
    return pl.DataFrame(result) if result else pl.DataFrame()

ccollat_df = process_ccollat(ccollat_df)
# Sort by ACCTNO, NOTENO
ccollat_df = ccollat_df.sort(["ACCTNO", "NOTENO"])

# =============================================================================
# STEP 4: MERGE LOAN + CCOLLAT → keep inner join (IF A AND B)
# =============================================================================
loan_df = loan_df.join(ccollat_df, on=["ACCTNO", "NOTENO"], how="inner", suffix="_col")

# =============================================================================
# STEP 5: MERGE OVERDFS + CCOLLAT → keep inner join, filter
# =============================================================================
overdfs_df = overdfs_df.join(ccollat_df, on="ACCTNO", how="inner", suffix="_col")
# IF APPRLIMT IN (.,0) AND ODSTATUS NOT IN ('NI','RI') THEN DELETE
overdfs_df = overdfs_df.filter(
    ~(
        (pl.col("APPRLIMT").is_null() | (pl.col("APPRLIMT") == 0)) &
        (~pl.col("ODSTATUS").is_in(["NI", "RI"]))
    )
)

# =============================================================================
# STEP 6: MERGE BTRL + CCOLLAT → inner join
# =============================================================================
btrl_df = btrl_df.sort("ACCTNO")
# ccollat sorted by ACCTNO; for BTRL merge we use ACCTNO only
ccollat_acctno = ccollat_df.unique(subset=["ACCTNO"], keep="first")
btrl_df = btrl_df.join(ccollat_acctno, on="ACCTNO", how="inner", suffix="_col")
btrl_df = btrl_df.with_columns([
    pl.col("FICODE").alias("BRANCH")
])

# =============================================================================
# STEP 7: Combine LOAN + OVERDFS + BTRL → COLLATER
# =============================================================================
# Ensure NOTENO exists in all frames
for df_name, df in [("loan_df", loan_df), ("overdfs_df", overdfs_df), ("btrl_df", btrl_df)]:
    pass  # columns already present from source

if "NOTENO" not in overdfs_df.columns:
    overdfs_df = overdfs_df.with_columns(pl.lit(None).cast(pl.Float64).alias("NOTENO"))
if "NOTENO" not in btrl_df.columns:
    btrl_df = btrl_df.with_columns(pl.lit(None).cast(pl.Float64).alias("NOTENO"))

# Get common columns for safe concat
all_cols = list(set(loan_df.columns) & set(overdfs_df.columns) & set(btrl_df.columns))

def align_df(df, cols):
    for c in cols:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))
    return df.select(cols)

collater_df = pl.concat([
    loan_df.select([c for c in loan_df.columns]),
    overdfs_df.select([c for c in overdfs_df.columns if c in loan_df.columns] + [c for c in overdfs_df.columns if c not in loan_df.columns]),
    btrl_df.select([c for c in btrl_df.columns])
], how="diagonal")

# IF NOTENO=. THEN NOTENO=0
collater_df = collater_df.with_columns([
    pl.col("NOTENO").fill_null(0).alias("NOTENO")
])

# Sort by ACCTNO, NOTENO
collater_df = collater_df.sort(["ACCTNO", "NOTENO"])

# =============================================================================
# STEP 8: MAIN DATA PROCESSING — COLLATER dataset and output routing
# =============================================================================

# CCLASSC to COLLATER mapping
CCLASSC_MAP = {
    frozenset(['001','006','007','014','016','024','112','113','114','115','116','117','025','026','046','048','049','149']): '29',
    frozenset(['000','011','012','013','017','018','019','021','027','028','029','030','124','031','105','106']): '70',
    frozenset(['002','003','041','042','043','058','059','067','068','069','070','111','123','071','072','078','079','084','107']): '90',
    frozenset(['004','005','127','128','129','142','143']): '30',
    frozenset(['032','033','034','035','036','037','038','039','040','044','050','051','052','053','054','055','056','057','118','119','121','122','060','061','062']): '10',
    frozenset(['065','066','075','076','082','083','093','094','095','096','097','098','131','132','133','134','135','136','137','138','139','141','101','102','103','104']): '40',
    frozenset(['063','064','073','074','080','081']): '60',
    frozenset(['010','085','086','087','088','089','090','111','125','126','144','091','092']): '50',
    frozenset(['009','022','023']): '00',
    frozenset(['008']): '21',
    frozenset(['045','047']): '22',
    frozenset(['015']): '23',
    frozenset(['020']): '80',
    frozenset(['108','109']): '81',
    frozenset(['077']): '99',
}

# Build fast lookup dict
CCLASSC_LOOKUP = {}
for codes_set, collater_code in CCLASSC_MAP.items():
    for code in codes_set:
        CCLASSC_LOOKUP[code] = collater_code

# Sets for output routing
SET_CFINASST = {'001','006','007','014','016','024','112','113','114','115','116','117','025','026','046','048','049','149'}
SET_CFINGUAR = {'000','011','012','013','017','018','019','021','027','028','029','030','124','031','105','106'}
SET_COTHASST = {'002','003','041','042','043','058','059','067','068','069','070','071','072','078','079','084','107'}
SET_CMTORVEH = {'004','005','127','128','129','142','143'}
SET_CPROPETY = {'032','033','034','035','036','037','038','039','040','044','050','051','118','119','121','122','052','053','054','055','056','057','060','061','062'}
SET_COTHVEHI = {'065','066','075','076','082','083','093','094','095','096','097','098','131','132','133','134','135','136','137','138','139','141','101','102','103','104'}
SET_CPLANTMA = {'063','064','073','074','080','081'}
SET_CCONCESS = {'010','085','086','087','088','089','111','125','126','144','090','091','092'}

def get_collater_code(cclassc, cissuer):
    code = CCLASSC_LOOKUP.get(safe_str(cclassc).strip(), "  ")
    # Override: IF SUBSTR(CISSUER,1,3)='KLM' OR SUBSTR(CISSUER,1,2)='UT' THEN COLLATER='23'
    ciss = safe_str(cissuer)
    if ciss[:3] == 'KLM' or ciss[:2] == 'UT':
        code = '23'
    return code

def build_collater_record(r, ficode, apcode, collref, collater_code, collval, collvalx):
    """Build COLLATER fixed-width record (200 bytes based on LRECL=200)."""
    # @0001 FICODE 9.  -> cols 1-9 (index 0-8)
    # @0010 APCODE 3.  -> cols 10-12
    # @0013 ACCTNO 10. -> cols 13-22
    # @0043 NOTENO 10. -> cols 43-52
    # @0073 COLLREF 11.-> cols 73-83
    # @0103 COLLATER $2.-> cols 103-104
    # @0105 COLLVAL Z16.-> cols 105-120
    # @0121 OLDBRH 5.  -> cols 121-125
    # @0126 COSTCTR 4. -> cols 126-129
    # @0130 AANO $13.  -> cols 130-142
    # @0143 FACILITY 5.-> cols 143-147
    # @0148 FACCODE 5. -> cols 148-152
    # @0154 CGEXAMTG 15.2-> cols 154-168
    # @0170 COLLATERAL_START_DT Z8. -> cols 170-177
    # @0178 COLLATERAL_END_DT $8.  -> cols 178-185
    rec = [" "] * 200

    def place(start_1, s):
        idx = start_1 - 1
        for i, c in enumerate(s):
            if idx + i < 200:
                rec[idx + i] = c

    place(1,   fmt_num(ficode, 9))
    place(10,  fmt_num(apcode, 3))
    place(13,  fmt_num(r.get("ACCTNO"), 10))
    place(43,  fmt_num(r.get("NOTENO"), 10))
    place(73,  fmt_num(collref, 11))
    place(103, fmt_str(collater_code, 2))
    place(105, fmt_z(collval, 16))
    place(121, fmt_num(r.get("OLDBRH", 0), 5))
    place(126, fmt_num(r.get("COSTCTR", 0), 4))
    place(130, fmt_str(r.get("AANO", ""), 13))
    place(143, fmt_num(r.get("FACILITY", 0), 5))
    place(148, fmt_num(r.get("FACCODE", 0), 5))
    place(154, fmt_num_dec(r.get("CGEXAMTG", 0), 15, 2))
    csd = r.get("COLLATERAL_START_DT", 0)
    if csd is None or (isinstance(csd, float) and csd < 0):
        csd = 0
    place(170, fmt_z(csd, 8))
    ced = safe_str(r.get("COLLATERAL_END_DT", ""))
    place(178, fmt_str(ced, 8))

    return "".join(rec)

# Open output files
f_collater  = open(COLLATER_OUT,  "w", encoding="latin-1")
f_dccms     = open(DCCMS_OUT,     "w", encoding="latin-1")
f_cpropety  = open(CPROPETY_OUT,  "w", encoding="latin-1")
f_cmtorveh  = open(CMTORVEH_OUT,  "w", encoding="latin-1")
f_cothvehi  = open(COTHVEHI_OUT,  "w", encoding="latin-1")
f_cplantma  = open(CPLANTMA_OUT,  "w", encoding="latin-1")
f_cconcess  = open(CCONCESS_OUT,  "w", encoding="latin-1")
f_cfinasst  = open(CFINASST_OUT,  "w", encoding="latin-1")
f_cothasst  = open(COTHASST_OUT,  "w", encoding="latin-1")
f_cfinguar  = open(CFINGUAR_OUT,  "w", encoding="latin-1")

# Accumulators for secondary processing datasets
cpropety_rows  = []
cmtorveh_rows  = []
cothvehi_rows  = []
cplantma_rows  = []
cconcess_rows  = []
cfinasst_rows  = []
cothasst_rows  = []
cfinguar_rows  = []
dccms_rows     = []

rows = collater_df.to_dicts()

for r in rows:
    purgeind = safe_str(r.get("PURGEIND", "")).strip()
    if purgeind == "N":
        continue  # IF PURGEIND ^= 'N' (skip if IS 'N')
    # Note: ^= 'N' means NOT equal to 'N', so we skip rows where PURGEIND == 'N'

    # Derived fields
    ficode  = r.get("BRANCH", 0) or 0
    collref = r.get("CCOLLNO", 0) or 0
    branch  = r.get("BRANCH", 0) or 0
    if branch == 0:
        branch = r.get("NTBRCH", 0) or 0
    if branch == 0:
        branch = r.get("ACCBRCH", 0) or 0
    apcode  = r.get("LOANTYPE", 0) or 0
    ficode  = branch   # reassign after branch fallback

    cclassc = safe_str(r.get("CCLASSC", "")).strip()
    cissuer  = safe_str(r.get("CISSUER", ""))
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
        collvalx = 0

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
    succaucpc = float(r.get("SUCCAUCPC", 0) or 0)
    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)
    if succaucpc > 0:
        actual_sale_value = succaucpc
    if actual_sale_value in (None, 0):
        actual_sale_value = 0.0
    r["ACTUAL_SALE_VALUE"] = actual_sale_value

    cprforsv = float(r.get("CPRFORSV", 0) or 0)
    cpresval = float(r.get("CPRESVAL", 0) or 0)
    r["CPRFORSV"] = cprforsv
    r["CPRESVAL"]  = cpresval

    coll_start = r.get("COLLATERAL_START_DT", 0)
    if coll_start is not None and isinstance(coll_start, (int, float)) and coll_start < 0:
        coll_start = 0
        r["COLLATERAL_START_DT"] = 0

    # Store derived values back
    r["_FICODE"]        = ficode
    r["_APCODE"]        = apcode
    r["_COLLREF"]       = collref
    r["_COLLATER_CODE"] = collater_code
    r["_COLLVAL"]       = collval
    r["_COLLVALX"]      = collvalx
    r["_BRANCH"]        = branch
    r["_CINSTCL"]       = cinstcl

    # Write COLLATER record
    collater_rec = build_collater_record(r, ficode, apcode, collref, collater_code, collval, collvalx)
    f_collater.write(collater_rec + "\n")

    # DCCMS: IF COLLATER='10' AND VERIFY(CPRDISDT,'0123456789')=0
    cprdisdt = safe_str(r.get("CPRDISDT", ""))
    if collater_code == "10":
        if cprdisdt and all(c in "0123456789" for c in cprdisdt) and cprdisdt != "":
            dccms_rows.append(dict(r))

    # Route to type-specific datasets
    # IF COLLATER='10' AND VERIFY(CPRDISDT,'0123456789')=1 (i.e. non-digit found or empty)
    # This second condition is the inverse: only continue to detail outputs if cprdisdt has non-digits (or is for non-10)
    if collater_code == "10":
        # verify=0 means all digits; verify=1 means first non-digit position (1-based)
        all_digits = cprdisdt and all(c in "0123456789" for c in cprdisdt)
        if all_digits:
            continue  # skip detail output for DCCMS records

    # Route to type-specific output datasets
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

# =============================================================================
# STEP 9: WRITE DCCMS
# =============================================================================
for r in dccms_rows:
    rec = [" "] * 200

    def place(start_1, s, rec=rec):
        idx = start_1 - 1
        for i, c in enumerate(s):
            if idx + i < len(rec):
                rec[idx + i] = c

    place(1,   fmt_num(r.get("_FICODE", 0), 9))
    place(13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(43,  fmt_num(r.get("NOTENO", 0), 10))
    place(73,  fmt_num(r.get("_COLLREF", 0), 11))
    place(103, fmt_str(r.get("_COLLATER_CODE", ""), 2))
    place(105, fmt_num(r.get("COSTCTR", 0), 4))
    place(130, fmt_str(r.get("AANO", ""), 13))
    place(143, fmt_num(r.get("FACILITY", 0), 5))
    place(148, fmt_str(safe_str(r.get("CPRDISDT", "")), 8))
    place(156, fmt_num(r.get("FACCODE", 0), 5))

    f_dccms.write("".join(rec) + "\n")

f_dccms.close()

# =============================================================================
# STEP 10: WRITE CPROPETY (property records - complex)
# =============================================================================

def val_date_components(valdt_str, reptday_int, bkmth, bkyyr, bkday):
    """Extract VTDD/VTMM/VTYY from a valdt string DDMMYYYY, apply backdate logic."""
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

# Sort cpropety: BY ACCTNO NOTENO DESCENDING CPRPARC1
cpropety_rows.sort(key=lambda r: (
    int(r.get("ACCTNO", 0) or 0),
    int(r.get("NOTENO", 0) or 0),
    safe_str(r.get("CPRPARC1", ""))
), reverse=False)

# We do the sort with DESCENDING CPRPARC1
cpropety_rows.sort(key=lambda r: (
    int(r.get("ACCTNO", 0) or 0),
    int(r.get("NOTENO", 0) or 0),
    [-ord(c) for c in safe_str(r.get("CPRPARC1", ""))]
))
# Simpler: sort tuple with reversed string
cpropety_rows.sort(key=lambda r: (
    int(r.get("ACCTNO", 0) or 0),
    int(r.get("NOTENO", 0) or 0),
))
# Actually replicate: sort ascending ACCTNO NOTENO, descending CPRPARC1
cpropety_rows = sorted(cpropety_rows, key=lambda r: (
    int(r.get("ACCTNO", 0) or 0),
    int(r.get("NOTENO", 0) or 0),
    safe_str(r.get("CPRPARC1", ""))
), reverse=False)
# For descending CPRPARC1 within group, we need a stable multi-key sort trick:
cpropety_rows = sorted(cpropety_rows, key=lambda r: (
    int(r.get("ACCTNO", 0) or 0),
    int(r.get("NOTENO", 0) or 0),
    # negate string: use tuple of negated ord values
    tuple(-ord(c) for c in safe_str(r.get("CPRPARC1", "")))
))

VALID_DIGITS = set("0123456789")
VALID_DIGITS_STR = '0123456789'

for r in cpropety_rows:
    ficode = r.get("_FICODE", 0) or 0
    apcode = r.get("_APCODE", 0) or 0
    cclassc = safe_str(r.get("CCLASSC", ""))

    cprforsv = float(r.get("CPRFORSV", 0) or 0) * 100
    cpresval = float(r.get("CPRESVAL", 0) or 0) * 100
    ltabtaucpc = float(r.get("LTABTAUCPC", 0) or 0) * 100
    succaucpc  = float(r.get("SUCCAUCPC", 0) or 0) * 100

    # VTDD/VTMM/VTYY
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
    def clean_par(val):
        s = safe_str(val).ljust(40)[:40]
        return s

    cprpar1c = clean_par(r.get("CPRPARC1", ""))
    cprpar2c = clean_par(r.get("CPRPARC2", ""))
    cprpar3c = clean_par(r.get("CPRPARC3", ""))
    cprpar4c = clean_par(r.get("CPRPARC4", ""))

    # IF SUBSTR(CPRPARCx,1,1) NOT IN ALP THEN CPRPARxC=SUBSTR(CPRPARCx,2,39)
    if r.get("CPRPARC1") and substr(r.get("CPRPARC1",""), 1, 1).upper() not in ALP:
        cprpar1c = substr(r.get("CPRPARC1",""), 2, 39).ljust(40)[:40]
    if r.get("CPRPARC2") and substr(r.get("CPRPARC2",""), 1, 1).upper() not in ALP:
        cprpar2c = substr(r.get("CPRPARC2",""), 2, 39).ljust(40)[:40]
    if r.get("CPRPARC3") and substr(r.get("CPRPARC3",""), 1, 1).upper() not in ALP:
        cprpar3c = substr(r.get("CPRPARC3",""), 2, 39).ljust(40)[:40]
    if r.get("CPRPARC4") and substr(r.get("CPRPARC4",""), 1, 1).upper() not in ALP:
        cprpar4c = substr(r.get("CPRPARC4",""), 2, 39).ljust(40)[:40]

    # Clean-up validations
    cplocat = safe_str(r.get("CPLOCAT", ""))
    if cplocat and cplocat[0].upper() not in ALP:
        cplocat = " "
    ownocupy = safe_str(r.get("OWNOCUPY", ""))
    if ownocupy and ownocupy[0].upper() not in ALP:
        ownocupy = " "
    cposcode = safe_str(r.get("CPOSCODE", ""))
    if not cposcode or cposcode[0] not in VALID_DIGITS_STR:
        cposcode = "     "
    bltnarea = safe_str(r.get("BLTNAREA", ""))
    if not bltnarea or bltnarea[0] not in VALID_DIGITS_STR:
        bltnarea = "    "
    bltnunit = safe_str(r.get("BLTNUNIT", ""))
    if bltnunit and bltnunit[0].upper() not in ALP:
        bltnunit = " "
    landunit = safe_str(r.get("LANDUNIT", ""))
    if landunit and landunit[0].upper() not in ALP:
        landunit = " "
    cpstate = safe_str(r.get("CPSTATE", ""))
    if len(cpstate) >= 2 and cpstate[1] not in VALID_DIGITS_STR:
        cpstate = "  "

    # *****************************************************
    # *  READ DATA FROM CMS (PRIMARY) & ELDS (SECONDARY)  *
    # *  ESMR 2011-4011                                   *
    # *****************************************************
    clhseno = safe_str(r.get("CPHSENO", ""))  # HOUSE NO
    cljlnnm = cprpar2c.strip()               # JALAN
    clbldnm = safe_str(r.get("CPBUILNO", ""))  # BUILDING NAME
    if not clhseno.strip():
        clhseno = safe_str(r.get("ADDRB02", ""))
    if not cljlnnm.strip():
        cljlnnm = safe_str(r.get("ADDRB03", ""))

    # ************************
    # *    ESMR 2012-2277    *
    # ************************
    openind = safe_str(r.get("OPENIND", ""))
    paidind = safe_str(r.get("PAIDIND", ""))
    acctstat = safe_str(r.get("ACCTSTAT", ""))
    if openind in ("B", "C", "P") or paidind == "P":
        acctstat = "S"

    # FIREDATE processing
    firedate = safe_str(r.get("FIREDATE", "0"))
    try:
        fd_int = int(float(firedate))
        fd_str = str(fd_int).zfill(8)
        frdd = fd_str[0:2]
        frmm = fd_str[2:4]
        fryy = fd_str[4:8]
    except Exception:
        frdd = frmm = fryy = "00"
    firedatex = f"{fryy}-{frmm}-{frdd}"

    # CPOLYNUM: COMPRESS removes special chars
    cpolynum = safe_str(r.get("CPOLYNUM", ""))
    cpolynum = compress_chars(cpolynum, "=`&/\\@#*+:().,\"%\xa3!$?_ '")

    # /* REVISED CRITERIA FOR ESMR 2013-616 (commented out in SAS)
    # IF ACCTSTAT EQ 'S' OR ...
    # */

    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)

    # Build CPROPETY record (1130 bytes based on last field @1130)
    reclen = 1130
    rec = [" "] * reclen

    def place(start_1, s):
        idx = start_1 - 1
        for i, c in enumerate(s):
            if idx + i < reclen:
                rec[idx + i] = c

    place(1,    fmt_num(ficode, 9))
    place(10,   fmt_num(apcode, 3))
    place(13,   fmt_num(r.get("ACCTNO", 0), 10))
    place(23,   fmt_num(r.get("CCOLLNO", 0), 11))
    place(36,   fmt_num(r.get("NOTENO", 0), 5))
    place(43,   fmt_str(r.get("_CINSTCL", cclassc), 2))
    place(45,   fmt_str(r.get("CPRRANKC", ""), 1))
    place(46,   fmt_str(r.get("CPRSHARE", ""), 1))
    place(47,   fmt_str(r.get("CPRLANDU", ""), 2))
    place(49,   fmt_str(r.get("CPRPROPD", ""), 2))
    place(51,   fmt_str(cprpar1c, 40))
    place(91,   fmt_str(cprpar2c, 40))
    place(131,  fmt_str(cprpar3c, 40))
    place(171,  fmt_str(cprpar4c, 30))
    place(201,  fmt_z(cprforsv, 16))
    place(217,  fmt_z(cpresval, 16))
    place(233,  fmt_z(vtdd, 2))
    place(235,  fmt_z(vtmm, 2))
    place(237,  fmt_z(vtyy, 4))
    place(241,  fmt_str(r.get("CPRVALU1", ""), 40))
    place(281,  fmt_str(r.get("CPRVALU2", ""), 40))
    place(321,  fmt_str(r.get("CPRVALU3", ""), 20))
    place(341,  fmt_z(r.get("CPRABNDT", 0) or 0, 8))
    place(349,  fmt_str(r.get("NAME1OWN", ""), 40))
    place(389,  fmt_str(ownocupy, 1))
    place(394,  fmt_str(landnrea, 4))
    place(398,  fmt_z(r.get("OLDBRH", 0) or 0, 5))
    place(403,  fmt_str(cplocat, 1))
    place(404,  fmt_str(cposcode, 5))
    place(409,  fmt_str(bltnarea, 4))
    place(413,  fmt_num(r.get("COSTCTR", 0) or 0, 4))
    # /* @0417 MASTTLNO $20. @0437 MASTTLMD $20. 2016-1859 */
    place(417,  fmt_str(r.get("TTLPARTCLR", ""), 1))
    place(457,  fmt_str(r.get("MASTOWNR", ""), 40))
    place(497,  fmt_str(r.get("TTLENO", ""), 40))
    place(537,  fmt_str(r.get("TTLMUKIM", ""), 40))
    place(577,  fmt_str(r.get("HOLDEXPD", ""), 1))
    place(578,  fmt_str(r.get("EXPDATE", ""), 8))
    place(586,  fmt_str(r.get("DEVNAME", ""), 40))
    place(626,  fmt_str(r.get("PRJCTNAM", ""), 40))
    place(666,  fmt_num(r.get("CPSTATE", 0) if isinstance(r.get("CPSTATE"), (int, float)) else 0, 2))
    place(668,  fmt_str(bltnunit, 1))
    place(669,  fmt_str(landunit, 1))
    place(670,  fmt_str(r.get("AANO", ""), 13))
    place(683,  fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(688,  fmt_num(r.get("ADLREFNO", 0) or 0, 25))
    place(713,  fmt_str(r.get("DEVNAME", ""), 60))
    place(773,  fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(779,  fmt_str(r.get("CPRSTAT", ""), 1))
    place(788,  fmt_str(clhseno, 11))   # HOUSE NO
    place(800,  fmt_str(cljlnnm, 60))   # JALAN NAME
    place(860,  fmt_str(clbldnm, 60))   # BUILDING NAME
    place(920,  fmt_str(r.get("INSURER", ""), 2))   # INSURER CODE
    place(923,  fmt_str(cpolynum, 16))              # POLICY NUMBER
    place(940,  fmt_str(firedatex, 10))             # EXPIRY DATE
    place(951,  fmt_num(r.get("SUMINSUR", 0) or 0, 16))  # SUM INSURED
    place(968,  fmt_str(r.get("CPRPARC3", ""), 40))
    place(1009, fmt_str(r.get("CPRPARC4", ""), 40))  # 2016-978
    place(1050, fmt_str(r.get("CTRYCODE", ""), 2))   # COUNTRY CODE
    place(1052, fmt_str(r.get("AUCIND", ""), 1))      # AUCTION INDICATOR
    place(1053, fmt_str(r.get("AUCCOMDT", ""), 8))   # AUCTION COMMENCEMENT DATE
    place(1061, fmt_num(r.get("NOAUC", 0) or 0, 2))  # NO. OF AUCTION
    place(1063, fmt_str(r.get("AUCSUCCIND", ""), 1)) # AUC SUCCESSFUL INDICATOR
    place(1064, fmt_num(ltabtaucpc, 10))              # LAST ABORTED AUCTION PRICE
    place(1074, fmt_str(r.get("SUCCAUCDT", ""), 8))  # SUCCESSFUL AUC DATE
    place(1082, fmt_num(succaucpc, 10))               # SUCCESSFUL AUC PRICE
    place(1093, fmt_str(r.get("CPTOWNN", ""), 20))   # PROPERTY TOWN NAME
    place(1113, fmt_num_dec(actual_sale_value, 16, 2))

    f_cpropety.write("".join(rec) + "\n")

f_cpropety.close()

# =============================================================================
# STEP 11: WRITE CMTORVEH (motor vehicle)
# =============================================================================
for r in cmtorveh_rows:
    ficode = r.get("_FICODE", 0) or 0
    apcode = r.get("_APCODE", 0) or 0
    reclen = 168
    rec = [" "] * reclen

    def place(start_1, s):
        idx = start_1 - 1
        for i, c in enumerate(s):
            if idx + i < reclen:
                rec[idx + i] = c

    place(1,   fmt_num(ficode, 9))
    place(10,  fmt_num(apcode, 3))
    place(13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(36,  fmt_num(r.get("NOTENO", 0), 5))
    place(43,  fmt_str(r.get("_CINSTCL", ""), 2))
    place(45,  fmt_str(r.get("CHPMAKE", ""), 40))
    place(85,  fmt_str(r.get("CHPENGIN", ""), 20))
    place(105, fmt_str(r.get("CHPCHASS", ""), 20))
    place(125, fmt_str(r.get("CHPVEHNO", ""), 12))
    place(137, fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(142, fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(146, fmt_str(r.get("AANO", ""), 13))
    place(159, fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(164, fmt_num(r.get("FACCODE", 0) or 0, 5))

    f_cmtorveh.write("".join(rec) + "\n")

f_cmtorveh.close()

# =============================================================================
# STEP 12: WRITE COTHVEHI (other vehicle/carrier)
# =============================================================================
for r in cothvehi_rows:
    ficode = r.get("_FICODE", 0) or 0
    apcode = r.get("_APCODE", 0) or 0
    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)

    vtdd, vtmm, vtyy = val_date_components(
        r.get("COVVALDT", ""), REPTDAY_INT, BKMTH, BKYYR, BKDAY
    )

    reclen = 142
    rec = [" "] * reclen

    def place(start_1, s):
        idx = start_1 - 1
        for i, c in enumerate(s):
            if idx + i < reclen:
                rec[idx + i] = c

    place(1,   fmt_num(ficode, 9))
    place(10,  fmt_num(apcode, 3))
    place(13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(36,  fmt_num(r.get("NOTENO", 0), 5))
    place(43,  fmt_str(r.get("_CINSTCL", ""), 2))
    place(45,  fmt_str(r.get("COVDESCR", ""), 2))
    place(47,  fmt_z(vtdd, 2))
    place(49,  fmt_z(vtmm, 2))
    place(51,  fmt_z(vtyy, 4))
    place(55,  fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(60,  fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(64,  fmt_str(r.get("AANO", ""), 13))
    place(77,  fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(81,  fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(127, fmt_num_dec(actual_sale_value, 16, 2))

    f_cothvehi.write("".join(rec) + "\n")

f_cothvehi.close()

# =============================================================================
# STEP 13: WRITE CPLANTMA (plant and machinery)
# =============================================================================
for r in cplantma_rows:
    ficode = r.get("_FICODE", 0) or 0
    apcode = r.get("_APCODE", 0) or 0
    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)
    cprforsv = float(r.get("CPRFORSV", 0) or 0)

    cpmdescr = safe_str(r.get("CPMDESCR", ""))
    cpmdesc1 = substr(cpmdescr, 2, 1)

    vtdd, vtmm, vtyy = val_date_components(
        r.get("CPMVALDT", ""), REPTDAY_INT, BKMTH, BKYYR, BKDAY
    )

    reclen = 160
    rec = [" "] * reclen

    def place(start_1, s):
        idx = start_1 - 1
        for i, c in enumerate(s):
            if idx + i < reclen:
                rec[idx + i] = c

    place(1,   fmt_num(ficode, 9))
    place(10,  fmt_num(apcode, 3))
    place(13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(36,  fmt_num(r.get("NOTENO", 0), 5))
    place(43,  fmt_str(r.get("_CINSTCL", ""), 2))
    place(45,  fmt_str(cpmdesc1, 1))
    place(46,  fmt_z(vtdd, 2))
    place(48,  fmt_z(vtmm, 2))
    place(50,  fmt_z(vtyy, 4))
    place(54,  fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(59,  fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(63,  fmt_str(r.get("AANO", ""), 13))
    place(76,  fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(81,  fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(127, fmt_num_dec(actual_sale_value, 16, 2))
    place(144, fmt_num_dec(cprforsv, 16, 2))

    f_cplantma.write("".join(rec) + "\n")

f_cplantma.close()

# =============================================================================
# STEP 14: WRITE CCONCESS (concession & contractual rights)
# =============================================================================
for r in cconcess_rows:
    ficode = r.get("_FICODE", 0) or 0
    apcode = r.get("_APCODE", 0) or 0

    cccdescr = safe_str(r.get("CCCDESCR", ""))
    cccdesc1 = substr(cccdescr, 2, 1)

    vtdd, vtmm, vtyy = val_date_components(
        r.get("CCCVALDT", ""), REPTDAY_INT, BKMTH, BKYYR, BKDAY
    )

    reclen = 85
    rec = [" "] * reclen

    def place(start_1, s):
        idx = start_1 - 1
        for i, c in enumerate(s):
            if idx + i < reclen:
                rec[idx + i] = c

    place(1,  fmt_num(ficode, 9))
    place(10, fmt_num(apcode, 3))
    place(13, fmt_num(r.get("ACCTNO", 0), 10))
    place(23, fmt_num(r.get("CCOLLNO", 0), 11))
    place(36, fmt_num(r.get("NOTENO", 0), 5))
    place(43, fmt_str(r.get("_CINSTCL", ""), 2))
    place(45, fmt_str(cccdesc1, 1))
    place(46, fmt_z(vtdd, 2))
    place(48, fmt_z(vtmm, 2))
    place(50, fmt_z(vtyy, 4))
    place(54, fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(59, fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(63, fmt_str(r.get("AANO", ""), 13))
    place(76, fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(81, fmt_num(r.get("FACCODE", 0) or 0, 5))

    f_cconcess.write("".join(rec) + "\n")

f_cconcess.close()

# =============================================================================
# STEP 15: WRITE CFINASST (other financial assets)
# =============================================================================
for r in cfinasst_rows:
    ficode   = r.get("_FICODE", 0) or 0
    apcode   = r.get("_APCODE", 0) or 0
    collvalx = float(r.get("_COLLVALX", 0) or 0)
    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)
    cprforsv = float(r.get("CPRFORSV", 0) or 0)

    vtdd, vtmm, vtyy = val_date_components(
        r.get("CFDVALDT", ""), REPTDAY_INT, BKMTH, BKYYR, BKDAY
    )

    reclen = 160
    rec = [" "] * reclen

    def place(start_1, s):
        idx = start_1 - 1
        for i, c in enumerate(s):
            if idx + i < reclen:
                rec[idx + i] = c

    place(1,   fmt_num(ficode, 9))
    place(10,  fmt_num(apcode, 3))
    place(13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(36,  fmt_num(r.get("NOTENO", 0), 5))
    place(43,  fmt_str(r.get("_CINSTCL", ""), 2))
    place(45,  fmt_str(r.get("CFDDESCR", ""), 2))
    place(47,  fmt_z(vtdd, 2))
    place(49,  fmt_z(vtmm, 2))
    place(51,  fmt_z(vtyy, 4))
    place(55,  fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(60,  fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(64,  fmt_str(r.get("CURCODE", ""), 3))
    place(67,  fmt_z(collvalx, 16))
    place(83,  fmt_num(r.get("FDACCTNO", 0) or 0, 10))
    place(93,  fmt_num(r.get("FDCDNO", 0) or 0, 10))
    place(103, fmt_str(r.get("AANO", ""), 13))
    place(116, fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(121, fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(127, fmt_num_dec(actual_sale_value, 16, 2))
    place(144, fmt_num_dec(cprforsv, 16, 2))

    f_cfinasst.write("".join(rec) + "\n")

f_cfinasst.close()

# =============================================================================
# STEP 16: WRITE COTHASST (other assets)
# =============================================================================
for r in cothasst_rows:
    ficode   = r.get("_FICODE", 0) or 0
    apcode   = r.get("_APCODE", 0) or 0
    cpresval = float(r.get("CPRESVAL", 0) or 0)
    actual_sale_value = float(r.get("ACTUAL_SALE_VALUE", 0) or 0)
    debenture_rv = float(r.get("DEBENTURE_REALISABLE_VALUE", 0) or 0)

    cdebdesc_orig = safe_str(r.get("CDEBDESC", ""))
    cdebdesc = substr(cdebdesc_orig, 2, 1)

    vtdd, vtmm, vtyy = val_date_components(
        r.get("CDEBVALD", ""), REPTDAY_INT, BKMTH, BKYYR, BKDAY
    )

    reclen = 142
    rec = [" "] * reclen

    def place(start_1, s):
        idx = start_1 - 1
        for i, c in enumerate(s):
            if idx + i < reclen:
                rec[idx + i] = c

    place(1,   fmt_num(ficode, 9))
    place(10,  fmt_num(apcode, 3))
    place(13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(36,  fmt_num(r.get("NOTENO", 0), 5))
    place(43,  fmt_str(r.get("_CINSTCL", ""), 2))
    place(45,  fmt_str(cdebdesc, 1))
    place(46,  fmt_z(vtdd, 2))
    place(48,  fmt_z(vtmm, 2))
    place(50,  fmt_z(vtyy, 4))
    place(54,  fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(59,  fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(63,  fmt_str(r.get("AANO", ""), 13))
    place(76,  fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(81,  fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(93,  fmt_num_dec(cpresval, 16, 2))
    place(110, fmt_num_dec(debenture_rv, 16, 2))
    place(127, fmt_num_dec(actual_sale_value, 16, 2))

    f_cothasst.write("".join(rec) + "\n")

f_cothasst.close()

# =============================================================================
# STEP 17: WRITE CFINGUAR (financial guarantees)
# =============================================================================
for r in cfinguar_rows:
    ficode   = r.get("_FICODE", 0) or 0
    apcode   = r.get("_APCODE", 0) or 0
    collvalx = float(r.get("_COLLVALX", 0) or 0)

    cguarnat_orig = safe_str(r.get("CGUARNAT", ""))
    cguarna1 = substr(cguarnat_orig, 2, 1)

    # CGEFFDAT1, CGEXPDAT1 formatted as DDMMYYN8. (8 digit date)
    # CGEFFDAT, CGEXPDAT are SAS date numbers
    cgeffdat = r.get("CGEFFDAT")
    cgexpdat = r.get("CGEXPDAT")
    cgeffdat1 = ddmmyyn(sas_date_to_python(cgeffdat) if isinstance(cgeffdat, (int, float)) else cgeffdat)
    cgexpdat1 = ddmmyyn(sas_date_to_python(cgexpdat) if isinstance(cgexpdat, (int, float)) else cgexpdat)

    reclen = 462
    rec = [" "] * reclen

    def place(start_1, s):
        idx = start_1 - 1
        for i, c in enumerate(s):
            if idx + i < reclen:
                rec[idx + i] = c

    place(1,   fmt_num(ficode, 9))
    place(10,  fmt_num(apcode, 3))
    place(13,  fmt_num(r.get("ACCTNO", 0), 10))
    place(23,  fmt_num(r.get("CCOLLNO", 0), 11))
    place(36,  fmt_num(r.get("NOTENO", 0), 5))
    place(43,  fmt_str(r.get("_CINSTCL", ""), 2))
    place(45,  fmt_str(cguarna1, 1))
    place(46,  fmt_str(r.get("CGUARNAM", ""), 40))
    place(196, fmt_str(r.get("CGUARID", ""), 20))
    place(216, fmt_str(r.get("CGUARCTY", ""), 2))
    place(218, fmt_num(r.get("OLDBRH", 0) or 0, 5))
    place(223, fmt_num(r.get("COSTCTR", 0) or 0, 4))
    place(227, fmt_num(r.get("CGCGSR", 0) or 0, 3))
    place(230, fmt_num(r.get("CGCGUR", 0) or 0, 3))
    place(233, fmt_str(r.get("AANO", ""), 13))
    place(246, fmt_num(r.get("FACILITY", 0) or 0, 5))
    place(251, fmt_str(r.get("CURCODE", ""), 3))
    place(254, fmt_z(collvalx, 16))
    place(270, fmt_num(r.get("FACCODE", 0) or 0, 5))
    place(275, fmt_str(r.get("CGUARLG", ""), 40))
    place(315, fmt_str(cgeffdat1, 8))
    place(323, fmt_str(cgexpdat1, 8))
    place(331, fmt_num_dec(r.get("CGEXAMTG", 0) or 0, 15, 2))
    place(347, fmt_num_dec(r.get("GFEERS", 0) or 0, 6, 2))
    place(354, fmt_num_dec(r.get("GFEERU", 0) or 0, 6, 2))
    place(361, fmt_num_dec(r.get("GFAMTS", 0) or 0, 15, 2))
    place(377, fmt_num_dec(r.get("GFAMTU", 0) or 0, 15, 2))
    place(393, fmt_num_dec(r.get("GCAMTS", 0) or 0, 15, 2))
    place(409, fmt_num_dec(r.get("GCAMTU", 0) or 0, 15, 2))
    place(427, fmt_str(r.get("GUARANTOR_ENTITY_TYPE", ""), 2))
    place(430, fmt_str(r.get("GUARANTOR_BIRTH_REGISTER_DT", ""), 8))
    place(439, fmt_str(r.get("BNMASSIGNID", ""), 12))
    place(451, fmt_num(r.get("CUSTNO", 0) or 0, 11))
    place(462, fmt_str(r.get("NEW_SSM_ID_NO", ""), 12))

    f_cfinguar.write("".join(rec) + "\n")

f_cfinguar.close()

print("All output files written successfully.")
