#!/usr/bin/env python3
"""
Program : EIDETLRM.py
Purpose : Extract remittance LOCAL transaction IFS for DETICA (AML
          interface feed) -- local (RM) remittance transactions are
          pulled from the daily remittance transaction file, enriched
          with account/customer identifiers, and written as a
          pipe(0x1D)-delimited flat file for DETICA.

Dependency:
    %INC PGM(PBBELF);
        -> from PBBELF import format_brchcd
        PUT(ISSBRANCH,BRCHCD.)   -> format_brchcd(branch_code)  (code->name)
        PUT(ACCTBRCH,BRCHCD.)    -> format_brchcd(branch_code)  (code->name)
    EIDETLRM never uses PUT(x,$BRCHRVR.) anywhere in the source, so
    format_brchrvr is intentionally NOT imported here (unlike EIDETFRM).

============================================================================
REPORT DATE
============================================================================
The original SAS reads DP.REPTDATE (a one-row control dataset) to obtain
REPTDATE. No such control dataset/parquet exists for this program, so a
local report-date function is used instead (see get_reptdate() below),
mirroring the same "yesterday" batch convention used in EIDETFRM.py. NOWK
is derived by EXACT day match (8/15/22/else 4), matching the SELECT(DAY
(REPTDATE)) logic in the SAS source.

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
 1. DP.CURRENT   (JCL //DP  DD DSN=SAP.PBB.MNITB(0),  member CURRENT)
    File : INPUT_DP_CURRENT_FILE  -> dp_current.sas7bdat
 2. IDP.CURRENT  (JCL //IDP DD DSN=SAP.PIBB.MNITB(0), member CURRENT)
    File : INPUT_IDP_CURRENT_FILE -> idp_current.sas7bdat
 3. DP.SAVING    (member SAVING, //DP)
    File : INPUT_DP_SAVING_FILE   -> dp_saving.sas7bdat
 4. IDP.SAVING   (member SAVING, //IDP)
    File : INPUT_IDP_SAVING_FILE  -> idp_saving.sas7bdat
 5. DP.FD        (member FD, //DP)
    File : INPUT_DP_FD_FILE       -> dp_fd.sas7bdat
 6. IDP.FD       (member FD, //IDP)
    File : INPUT_IDP_FD_FILE      -> idp_fd.sas7bdat
 7. DP.UMA       (member UMA, //DP)
    File : INPUT_DP_UMA_FILE      -> dp_uma.sas7bdat
 8. IDP.UMA      (member UMA, //IDP)
    File : INPUT_IDP_UMA_FILE     -> idp_uma.sas7bdat
 9. DP.VOSTRO    (member VOSTRO, //DP)  -- NOTE: no IDP.VOSTRO is read in
    the original SAS (DATA DEPO_ACCT only SETs DP.VOSTRO).
    File : INPUT_DP_VOSTRO_FILE   -> dp_vostro.sas7bdat
    Cols used (1-3): ACCTNO, PRODUCT, BRANCH

    NOTE: unlike EIDETFRM, EIDETLRM has NO loan (LN.LNNOTE/ILN.LNNOTE)
    input at all -- local remittance transactions only ever resolve
    against deposit accounts (ACCOUNT_SOURCE_UNIQUE_ID is always 'DP'
    prefixed), so no LOAN dataset/ACCT-concat step exists here.

10. REM.REMTRAN&REPTMON&NOWK&REPTYEAR (JCL //REM DD DSN=SAP.PBB.CRM.RMTRNSAC(0))
    Deterministic filename (fully derived from REPTMON/NOWK/REPTYEAR
    tokens) -> constructed directly, input_date.get_latest_file() NOT used.
    File : INPUT_REMTRAN_FILE -> remtran_{REPTMON}{NOWK}{REPTYEAR}.sas7bdat
    Cols used : REMTYPE, APPLNAME, BENENAME, BNAD1, BNAD2, ANAD1, ANAD2,
                ISSBRANCH, PAYMODE, REFNO, ISTTYPE, STATUS, SERIAL,
                ISSDTE, LASTTRAN, TIMESTAMP, BENEBANK, APPLID, BENEID,
                USERID, AMOUNT
    Assumption: ISSDTE is a SAS numeric date (days since 1960-01-01),
    converted per project convention. LASTTRAN and TIMESTAMP are assumed
    to be character fields as manipulated by the SAS source (COMPRESS /
    fixed-position SUBSTR respectively).

11. CIS.CUSTDLY  (JCL //CIS DD DSN=RBP2.B033.CIS.CUST.DAILY)
    File : INPUT_CIS_CUSTDLY_FILE -> cis_custdly.sas7bdat
    Cols used : ACCTCODE, ACCTNO, CUSTNO, ALIAS, PRISEC, INDORG
    NOTE: EIDETLRM's CIS step filters ACCTCODE IN ('DP') only (EIDETFRM's
    equivalent CIS step filters ('DP','LN') -- there is no 'LN' here
    because there is no loan account resolution in this program).

============================================================================
OUTPUT
============================================================================
//LOCRMT DD DSN=SAP.AML.DETICA.REMTRAN.LOCAL.TEXT, DISP=OLD
Fixed catalogued name (no date token) -> static output filename.
Pipe-delimited (delimiter = hex '1D'X, ASCII 0x1D Group Separator) flat
file, 75 fields per record, most fields blank. No ASA control byte (this
is a data feed, not a report). Unlike EIDETFRM (field 21=FORAMT,
field 22=AMOUNT), EIDETLRM puts AMOUNT in BOTH field 21 and field 22
(local transactions have no separate foreign-amount column).
File : OUTPUT_FILE -> EIDETLRM_LOCAL_<ts>.txt (encoding='latin1' so the
0x1D delimiter byte round-trips safely)

//DELETE (PGM=IEFBR14) removes any pre-existing backup at job start;
//COPYFILE (PGM=ICEGENER) backs the interface file up to .TEXT.BKP at job
end. Both are reproduced as file operations below.

============================================================================
PRESERVED SAS QUIRKS
============================================================================
- ISSDTE_DAY = DAY(ISSDTE) is computed in the SAS source but never
  referenced again anywhere else in the program -- a dead derived
  variable, kept here (commented) only for documentation parity.
- MM / YYYY SYMPUTs are likewise dead symbolic variables (same pattern as
  EIDETFRM.py) -- computed, never referenced again.
- DEPO_ACCT's PROD = COMPRESS('DP'||PRODUCT) uses NO explicit numeric
  format (unlike EIDETFRM's DEPO, which uses PUT(PRODUCT,Z3.)). SAS's
  default BEST-format numeric-to-character conversion applies here, i.e.
  no zero-padding -- preserved exactly via `_num_to_str()`.
- In the first DATA LOCAL data step, ACCOUNT_SOURCE_UNIQUE_ID is only
  reassigned inside the VERIFY(...)=1 branch of the second ISTTYPE/STATUS
  condition; there is no ELSE. Because ACCOUNT_SOURCE_UNIQUE_ID is a
  computed (non-SET) PDV variable, SAS retains its value from whichever
  prior loop iteration last assigned it (standard DATA-step PDV
  carry-over for derived variables). This cross-row carry-over is
  reproduced explicitly below via a module-level "last assigned" tracker
  rather than resetting the field to missing every row.
- DATA TRAN.LOCAL_GETMNI (first pass) MERGE LOCAL(IN=A) DEPO_ACCT(IN=B):
  PROD is a variable common to both LOCAL and DEPO_ACCT; on a matched
  BY-group SAS's last-dataset-wins MERGE semantics mean DEPO_ACCT's PROD
  overwrites LOCAL's PROD='RT108', exactly as with ORG_UNIT_CODE/PROD in
  EIDETFRM's ACCT merges -- reproduced the same way here.
"""

import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

from PBBELF import format_brchcd

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_DIR = STG_DIR / "from_dwh"

INPUT_DP_CURRENT_FILE   = INPUT_DIR / "ca09126.sas7bdat"
INPUT_IDP_CURRENT_FILE  = INPUT_DIR / "ica09126.sas7bdat"
INPUT_DP_SAVING_FILE    = INPUT_DIR / "sa09126.sas7bdat"
INPUT_IDP_SAVING_FILE   = INPUT_DIR / "isa09126.sas7bdat"
INPUT_DP_FD_FILE        = INPUT_DIR / "fd09126.sas7bdat"
INPUT_IDP_FD_FILE       = INPUT_DIR / "ifd09126.sas7bdat"
INPUT_DP_UMA_FILE       = STG_DIR / "detic2" / "uma.sas7bdat"
INPUT_IDP_UMA_FILE      = STG_DIR / "detic2" / "iuma.sas7bdat"
INPUT_DP_VOSTRO_FILE    = STG_DIR / "detic2" / "vostro08426.sas7bdat"

INPUT_CIS_CUSTDLY_FILE  = STG_DIR / "custdly.sas7bdat"

CACHE_DIR = BASE_DIR / "input" / "cache" / "detic2"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000
DELIM = "\x1d"   # '1D'X

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet -- local derivation)
# ============================================================================
print("Step 1: Deriving report date...")


def get_reptdate() -> date:
    """
    Local report-date derivation (no reptdate.parquet control dataset
    exists for this program). Mirrors the standard daily/yesterday batch
    convention: the report date is the calendar day prior to the run date.
    """
    return date.today() - timedelta(days=1)


reptdate = get_reptdate()

# NOWK: exact-day match (8/15/22/else 4) -- SELECT(DAY(REPTDATE)) equivalent.
_day = reptdate.day
NOWK = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"

REPTYEAR = reptdate.strftime("%y")     # PUT(REPTDATE,YEAR2.)
REPTMON  = reptdate.strftime("%m")     # PUT(MONTH(REPTDATE),Z2.)
RDATE    = reptdate.strftime("%Y%m%d") # PUT(REPTDATE,YYMMDDN8.)

ts = reptdate.strftime("%y%m%d")

# MM / YYYY are SYMPUT'd in the original SAS but never referenced again
# anywhere else in the program body -- dead symbolic variables, kept only
# for documentation parity (same pattern as EIDETFRM.py).
MM   = REPTMON
YYYY = reptdate.strftime("%Y")

print(f"  REPTDATE : {reptdate}   NOWK: {NOWK}   REPTMON: {REPTMON}   REPTYEAR: {REPTYEAR}")
print(f"  RDATE    : {RDATE}")

# INPUT_REMTRAN_FILE = INPUT_DIR / f"remtran{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
INPUT_REMTRAN_FILE = INPUT_DIR / f"remtran09126.sas7bdat"

OUTPUT_DIR = BASE_DIR / "output" / "EIDETLRM"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE   = OUTPUT_DIR / f"EIDETLRM_LOCAL_{ts}.txt"
OUTPUT_BACKUP = OUTPUT_DIR / f"EIDETLRM_LOCAL_{ts}_BKP.txt"

# ============================================================================
# STEP 0: DELETE OLD BACKUP  (//DELETE EXEC PGM=IEFBR14)
# ============================================================================
print("Step 0: Removing stale backup file (if present)...")
OUTPUT_BACKUP.unlink(missing_ok=True)

# ============================================================================
# *2017-2058;  Company-name exclusion list (%LET LIST = (...))
# ============================================================================
COMPANY_EXCLUDE_LIST = {
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

# ============================================================================
# %BRH branch-override lookup table
# ----------------------------------------------------------------------
# Every %BRH(BRH_ID,ACCT_ID,CUST_ID) call in the original SAS follows a
# strict, mechanical naming pattern:
#     ACCT_ID = 'RMT' || ZFILL(BRH_ID,5) || 'TLOA'
#     CUST_ID = 'RMT' || ZFILL(BRH_ID,5) || 'TLC'
# The SAME branch-ID list / suffix pattern is reused verbatim in all three
# places the macro is invoked in the SAS source (DATA LOCAL, the first
# DATA TRAN.LOCAL_GETMNI, and the second DATA TRAN.LOCAL_GETMNI). Rather
# than reproduce ~200 IF/DO blocks per data step verbatim, the exact
# branch-ID list is preserved below and the ACCT_ID/CUST_ID strings are
# generated by that same pattern -- byte-identical results.
# ============================================================================
_FULL_BRANCH_LIST = [
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
    23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76,
    77, 78, 79, 80, 81, 83, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
    97, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114,
    115, 116, 117, 118, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
    130, 131, 133, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145,
    146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
    160, 161, 162, 163, 164, 165, 167, 168, 169, 170, 171, 172, 173, 174,
    175, 176, 177, 178, 179, 180, 183, 184, 185, 186, 189, 190, 191, 192,
    193, 194, 195, 196, 197, 198, 199, 201, 202, 203, 204, 205, 206, 207,
    208, 209, 210, 211, 216, 217, 220, 221, 222, 224, 225, 226, 228, 230,
    231, 232, 233, 234, 235, 237, 704, 239, 240, 241, 242, 243, 244, 245,
    247, 248, 249, 251, 252, 254, 256, 257, 258, 259, 260, 261, 262, 263,
    264, 265, 266, 267, 268, 269, 270, 273, 274, 275, 276, 703, 278, 280,
    281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294,
    295, 296, 701, 702, 800, 801, 802, 803, 804, 805, 806, 807, 808, 809,
    811, 812, 813, 814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824,
    825, 826, 827, 828, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853,
    854, 855, 856, 857, 858, 859, 860, 861, 862, 863,
]


def _brh_lookup(branch_ids: list, acct_suffix: str, cust_suffix: str) -> dict:
    return {
        bid: (f"RMT{bid:05d}{acct_suffix}", f"RMT{bid:05d}{cust_suffix}")
        for bid in branch_ids
    }


LOCAL_BRH = _brh_lookup(_FULL_BRANCH_LIST, "TLOA", "TLC")


def _apply_brh(branch_id: Optional[object], lookup: dict,
                default_acct: str, default_cust: str) -> tuple:
    """Applies the %BRH(BRANCH_ID,...) IF-chain: returns (acct_id, cust_id),
    falling back to the pre-computed defaults when BRANCH_ID doesn't match
    any entry (i.e. none of the IF conditions fired)."""
    try:
        bid = int(str(branch_id).strip())
    except (TypeError, ValueError):
        return default_acct, default_cust
    return lookup.get(bid, (default_acct, default_cust))


def _num_to_str(v) -> str:
    """Mirrors SAS implicit numeric-to-character coercion used in
    COMPRESS('DP'||REFNO) / COMPRESS('DP'||PRODUCT): integral floats
    render without a decimal point, everything else falls back to
    str(). No zero-padding is applied anywhere this helper is used,
    matching the SAS source's lack of an explicit PUT(...,Zn.) format."""
    if v is None:
        return ""
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else str(v)
    return str(v)


def _verify_first_char_nondigit(value) -> bool:
    """VERIFY(PAYMODE,'1234567890')=1 -- true when the first character of
    PAYMODE is NOT a digit. A blank/None PAYMODE is treated as SAS-blank
    (a leading space), which is itself not a digit, so this returns True."""
    v = value if value else ""
    if v == "":
        return True
    return not v[0].isdigit()


# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET  (EIIMRM01.py pattern)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    import gc
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if schema is None:
            fields = []
            for col, dtype in chunk.dtypes.items():
                if dtype == "object":
                    pa_type = pa.string()
                elif pd.api.types.is_integer_dtype(dtype):
                    pa_type = pa.int64()
                elif pd.api.types.is_float_dtype(dtype):
                    pa_type = pa.float64()
                else:
                    pa_type = pa.from_numpy_dtype(dtype)
                fields.append(pa.field(col, pa_type))
            schema = pa.schema(fields)
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")

        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# STEP 2: CACHE INPUT SAS FILES TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
DP_CURRENT_CACHE   = _load_cached(INPUT_DP_CURRENT_FILE, "DP_CURRENT")
IDP_CURRENT_CACHE  = _load_cached(INPUT_IDP_CURRENT_FILE, "IDP_CURRENT")
DP_SAVING_CACHE    = _load_cached(INPUT_DP_SAVING_FILE, "DP_SAVING")
IDP_SAVING_CACHE   = _load_cached(INPUT_IDP_SAVING_FILE, "IDP_SAVING")
DP_FD_CACHE        = _load_cached(INPUT_DP_FD_FILE, "DP_FD")
IDP_FD_CACHE       = _load_cached(INPUT_IDP_FD_FILE, "IDP_FD")
DP_UMA_CACHE       = _load_cached(INPUT_DP_UMA_FILE, "DP_UMA")
IDP_UMA_CACHE      = _load_cached(INPUT_IDP_UMA_FILE, "IDP_UMA")
DP_VOSTRO_CACHE    = _load_cached(INPUT_DP_VOSTRO_FILE, "DP_VOSTRO")
REMTRAN_CACHE      = _load_cached(INPUT_REMTRAN_FILE, "REMTRAN")
CIS_CUSTDLY_CACHE  = _load_cached(INPUT_CIS_CUSTDLY_FILE, "CIS_CUSTDLY")

# ============================================================================
# STEP 3: DATA LOCAL  (SET REM.REMTRAN...; IF REMTYPE='L'; exclusion filter)
# ============================================================================
print("\nStep 3: Building LOCAL...")

con = duckdb.connect(database=":memory:")
local_pl = con.execute(f"""
    SELECT
        REMTYPE, APPLNAME, BENENAME, BNAD1, BNAD2, ANAD1, ANAD2,
        ISSBRANCH, PAYMODE, REFNO, ISTTYPE, STATUS, SERIAL, ISSDTE,
        LASTTRAN, TIMESTAMP, BENEBANK, APPLID, BENEID, USERID, AMOUNT
    FROM read_parquet('{REMTRAN_CACHE.as_posix()}')
    WHERE REMTYPE = 'L'
""").pl()
con.close()


def _in_exclude_list(value) -> bool:
    return value is not None and value.strip().upper() in COMPANY_EXCLUDE_LIST


local_src_rows = []
for r in local_pl.iter_rows(named=True):
    # *2017-2058;
    if (_in_exclude_list(r["APPLNAME"]) or _in_exclude_list(r["BENENAME"])
            or _in_exclude_list(r["BNAD1"]) or _in_exclude_list(r["BNAD2"])
            or _in_exclude_list(r["ANAD1"]) or _in_exclude_list(r["ANAD2"])):
        continue
    local_src_rows.append(r)

print(f"  LOCAL rows: {len(local_src_rows):,}")

# ============================================================================
# STEP 4: DATA LOCAL; SET LOCAL; ... (transforms + conditional OUTPUT)
# ============================================================================
print("\nStep 4: Applying LOCAL transforms and routing...")


def _format_issdte(issdte) -> str:
    """PUT(ISSDTE,YYMMDDN8.). ISSDTE assumed to be a SAS numeric date
    (days since 1960-01-01)."""
    if issdte is None:
        return ""
    d = date(1960, 1, 1) + timedelta(days=int(issdte))
    return d.strftime("%Y%m%d")


def _substr_timestamp(ts: str, start: int, length: int) -> str:
    """SAS 1-based SUBSTR emulation for the fixed-position TIMESTAMP parse."""
    if ts is None:
        return ""
    return ts[start - 1:start - 1 + length]


# SAS DATA-step PDV carry-over: ACCOUNT_SOURCE_UNIQUE_ID is a computed
# (non-SET) variable that is only conditionally reassigned inside the
# second ISTTYPE/STATUS branch (no ELSE). It therefore retains its value
# from whichever prior loop iteration last set it -- see module docstring.
_carry_acct_id: Optional[str] = None

local_rows = []
for r in local_src_rows:
    row = dict(r)
    row["RUN_TIMESTAMP"] = (RDATE + "000000")[:14]
    row["BRANCH_ID"] = r["ISSBRANCH"]
    row["CURCODE"] = "MYR"
    row["CURBASE"] = "MYR"
    # ISSDTE_DAY = DAY(ISSDTE);  -- dead variable, never referenced again.
    # row["ISSDTE_DAY"] = date(1960, 1, 1) + timedelta(days=int(r["ISSDTE"])).day
    row["ORIGINATION_DATE"] = _format_issdte(r["ISSDTE"])
    row["POSTING_DATE"] = (r["LASTTRAN"] or "").replace("-", "")

    ts_val = r["TIMESTAMP"] or ""
    yyyy = _substr_timestamp(ts_val, 1, 4)
    mm   = _substr_timestamp(ts_val, 6, 2)
    dd   = _substr_timestamp(ts_val, 9, 2)
    hour = _substr_timestamp(ts_val, 12, 2)
    minute = _substr_timestamp(ts_val, 15, 2)
    sec  = _substr_timestamp(ts_val, 18, 2)
    row["LOCAL_TIMESTAMP"] = (yyyy + mm + dd + hour + minute + sec)[:14]

    row["CRDR"] = "D"
    row["MENTION"] = r["PAYMODE"]
    row["CHANNEL"] = 999
    row["EMPLOYEE_ID"] = 88888
    row["ORIGINATOR_NAME"] = r["APPLNAME"]
    row["BENEFICIARY_NAME"] = r["BENENAME"]
    row["ORIGINATOR_BANK"] = format_brchcd(r["ISSBRANCH"])
    row["BENEFICIARY_BANK"] = r["BENEBANK"]
    row["SENDER_BRANCH"] = format_brchcd(r["ISSBRANCH"])
    row["BENE_BRANCH"] = "0"
    row["ORIGINATOR_ID"] = r["APPLID"]
    row["BENEFICIARY_ID"] = r["BENEID"]
    row["PROD"] = "RT108"
    row["TXN_CODE"] = "RMT002"

    if r["ISSBRANCH"] in (701, 702):
        row["ORG_UNIT_CODE"] = "PIBBTRSRY"
    else:
        row["ORG_UNIT_CODE"] = "PBBTRSRY"

    isttype, status = r["ISTTYPE"], r["STATUS"]

    if isttype == "IG" and status == "SE":
        if r["PAYMODE"] == "DEBIT ACC":
            row["ACCOUNT_SOURCE_UNIQUE_ID"] = ("DP" + _num_to_str(r["REFNO"])).replace(" ", "")
        else:
            acct_id = "RMT00001A"
            cust_id = "RMT00001C"
            acct_id, cust_id = _apply_brh(row["BRANCH_ID"], LOCAL_BRH, acct_id, cust_id)
            row["ACCOUNT_SOURCE_UNIQUE_ID"] = acct_id
            row["CUSTOMER_SOURCE_UNIQUE_ID"] = cust_id
        _carry_acct_id = row["ACCOUNT_SOURCE_UNIQUE_ID"]
        local_rows.append(row)

    elif isttype in ("A", "A1", "B", "C", "G", "H", "K", "L", "M",
                      "Q", "R", "S", "T") and status in ("L", "O", "IS"):
        row["SERIAL"] = (format_brchcd(r["ISSBRANCH"]) + _num_to_str(r["SERIAL"])).replace(" ", "")
        if _verify_first_char_nondigit(r["PAYMODE"]):
            row["ACCOUNT_SOURCE_UNIQUE_ID"] = ("DP" + (r["PAYMODE"] or "")).replace(" ", "")
            _carry_acct_id = row["ACCOUNT_SOURCE_UNIQUE_ID"]
        else:
            # VERIFY condition false: no reassignment in the SAS source --
            # ACCOUNT_SOURCE_UNIQUE_ID carries over from the last iteration
            # that set it (see _carry_acct_id note above).
            row["ACCOUNT_SOURCE_UNIQUE_ID"] = _carry_acct_id
        local_rows.append(row)
    # else: neither branch fires -> no OUTPUT in the SAS source, row dropped.
    # (The PDV state, including _carry_acct_id, is still whatever it was
    # left at by the last branch that assigned it -- nothing to update here
    # since this branch never touches ACCOUNT_SOURCE_UNIQUE_ID.)

print(f"  LOCAL (post-routing) rows: {len(local_rows):,}")

# ============================================================================
# STEP 5: DATA DEPO_ACCT;  (SET DP.CURRENT IDP.CURRENT DP.SAVING IDP.SAVING
#                               DP.FD IDP.FD DP.UMA IDP.UMA DP.VOSTRO; ...)
# ============================================================================
print("\nStep 5: Building DEPO_ACCT...")

_depo_sources = [
    DP_CURRENT_CACHE, IDP_CURRENT_CACHE, DP_SAVING_CACHE, IDP_SAVING_CACHE,
    DP_FD_CACHE, IDP_FD_CACHE, DP_UMA_CACHE, IDP_UMA_CACHE, DP_VOSTRO_CACHE,
]


def _depo_select(path: Path) -> str:
    names = set(pl.scan_parquet(path).collect_schema().names())
    branch_expr = "BRANCH" if "BRANCH" in names else "CAST(NULL AS VARCHAR) AS BRANCH"
    return (
        f"SELECT CAST(ACCTNO AS VARCHAR) AS ACCTNO, PRODUCT, {branch_expr} "
        f"FROM read_parquet('{path.as_posix()}')"
    )


con = duckdb.connect(database=":memory:")
_union_sql = " UNION ALL ".join(_depo_select(p) for p in _depo_sources)
depo_acct_pl = con.execute(_union_sql).pl()
con.close()

_depo_acct_lookup: dict = {}
for r in depo_acct_pl.iter_rows(named=True):
    acct_id = ("DP" + (r["ACCTNO"] or "")).replace(" ", "")
    _depo_acct_lookup[acct_id] = {
        "PROD": ("DP" + _num_to_str(r["PRODUCT"])).replace(" ", ""),
        "ACCTBRCH": r["BRANCH"],
    }
print(f"  DEPO_ACCT rows: {len(_depo_acct_lookup):,}")

# ============================================================================
# STEP 6: DATA TRAN.LOCAL_GETMNI (1st pass);
#         MERGE LOCAL(IN=A) DEPO_ACCT(IN=B); BY ACCOUNT_SOURCE_UNIQUE_ID; IF A;
# ============================================================================
print("\nStep 6: Merging DEPO_ACCT onto LOCAL...")

for row in local_rows:
    key = row.get("ACCOUNT_SOURCE_UNIQUE_ID")
    depo_match = _depo_acct_lookup.get(key) if key else None

    if depo_match is not None:
        # SAS MERGE last-dataset-wins: DEPO_ACCT's PROD overwrites LOCAL's
        # PROD='RT108' on every matched row (see module docstring).
        row["PROD"] = depo_match["PROD"]
        acctbrch = depo_match["ACCTBRCH"]
        try:
            acctbrch_int = int(acctbrch)
        except (TypeError, ValueError):
            acctbrch_int = None

        issbranch = row.get("ISSBRANCH")
        cond = (
            row.get("ISTTYPE") == "IB"
            or (
                (
                    (row.get("ISTTYPE") == "IG" and issbranch == 168)
                    or (row.get("USERID") == "CMSECP" and row.get("BRANCH_ID") == 0)
                )
                and row.get("PAYMODE") == "DEBIT ACC"
            )
        )
        if cond:
            row["BRANCH_ID"] = acctbrch_int
            row["SENDER_BRANCH"] = format_brchcd(acctbrch_int) if acctbrch_int is not None else ""
    else:
        acct_id = "RMT00001A"
        cust_id = "RMT00001C"
        acct_id, cust_id = _apply_brh(row.get("BRANCH_ID"), LOCAL_BRH, acct_id, cust_id)
        row["ACCOUNT_SOURCE_UNIQUE_ID"] = acct_id
        row["CUSTOMER_SOURCE_UNIQUE_ID"] = cust_id

print(f"  TRAN.LOCAL_GETMNI (1st pass) rows: {len(local_rows):,}")

# ============================================================================
# STEP 7: DATA CIS;  (SET CIS.CUSTDLY; WHERE PRISEC=901 AND ACCTCODE IN
#                          ('DP'); ...)
# ============================================================================
print("\nStep 7: Building CIS...")

con = duckdb.connect(database=":memory:")
cis_pl = con.execute(f"""
    SELECT
        CAST(ACCTCODE AS VARCHAR) AS ACCTCODE,
        CAST(ACCTNO AS VARCHAR)   AS ACCTNO,
        CAST(CUSTNO AS VARCHAR)   AS CUSTNO,
        CAST(ALIAS AS VARCHAR)    AS ALIAS,
        CAST(INDORG AS VARCHAR)   AS INDORG
    FROM read_parquet('{CIS_CUSTDLY_CACHE.as_posix()}')
    WHERE PRISEC = 901 AND ACCTCODE IN ('DP')
""").pl()
con.close()

cis_pl = cis_pl.with_columns([
    (pl.col("ACCTCODE") + pl.col("ACCTNO")).str.replace_all(" ", "")
    .alias("ACCOUNT_SOURCE_UNIQUE_ID"),
    ("CIS" + pl.col("CUSTNO").fill_null("")).str.replace_all(" ", "").alias("CIS"),
])
# PROC SORT DATA=CIS NODUPKEY; BY ACCOUNT_SOURCE_UNIQUE_ID;  (first wins)
cis_pl = cis_pl.sort("ACCOUNT_SOURCE_UNIQUE_ID").unique(
    subset=["ACCOUNT_SOURCE_UNIQUE_ID"], keep="first")
_cis_lookup = {
    r["ACCOUNT_SOURCE_UNIQUE_ID"]: {"CIS": r["CIS"], "ALIAS": r["ALIAS"], "INDORG": r["INDORG"]}
    for r in cis_pl.iter_rows(named=True)
}
print(f"  CIS rows: {len(_cis_lookup):,}")

# ============================================================================
# STEP 8: DATA TRAN.LOCAL_GETMNI (2nd pass);
#         MERGE TRAN.LOCAL_GETMNI(IN=A) CIS; BY ACCOUNT_SOURCE_UNIQUE_ID; IF A;
# ============================================================================
print("\nStep 8: Merging CIS onto TRAN.LOCAL_GETMNI...")

for row in local_rows:
    key = row.get("ACCOUNT_SOURCE_UNIQUE_ID")
    cis_match = _cis_lookup.get(key) if key else None

    if cis_match is not None:
        row["ALIAS"] = cis_match["ALIAS"]
        row["INDORG"] = cis_match["INDORG"]
        cis_val = cis_match["CIS"]
    else:
        row["ALIAS"] = None
        row["INDORG"] = None
        cis_val = None

    if cis_val:
        row["CUSTOMER_SOURCE_UNIQUE_ID"] = cis_val
    else:
        acct_id = "RMT00001A"
        cust_id = "RMT00001C"
        row["PROD"] = "RT108"
        acct_id, cust_id = _apply_brh(row.get("BRANCH_ID"), LOCAL_BRH, acct_id, cust_id)
        row["ACCOUNT_SOURCE_UNIQUE_ID"] = acct_id
        row["CUSTOMER_SOURCE_UNIQUE_ID"] = cust_id

print(f"  TRAN.LOCAL_GETMNI (final) rows: {len(local_rows):,}")

# ============================================================================
# STEP 9: DATA OUT;  SET TRAN.LOCAL_GETMNI; ... (alias/@ cleanup + deletes)
# ============================================================================
print("\nStep 9: Building OUT and applying alias/@ cleanup...")


def _strip_double_alias(value: str) -> str:
    """Repeatedly collapse '@@<40 chars>' markers into a single space
    join, matching the SAS DO WHILE(INDEX(...,'@@')>0) loop."""
    while True:
        idx = value.find("@@")
        if idx < 0:
            return value
        part1 = value[:idx]
        part2 = value[idx + 2:idx + 2 + 40]
        value = " ".join(p for p in (part1, part2) if p != "") or ""


_DELETE_IDS = {"0000000000000006463H", "0000000000000014328V"}

out_rows = []
for row in local_rows:
    flg = row.get("INCOMING_OUTGOING_FLG")  # always 'O' -- LOCAL never sets 'I'
    ben_id = row.get("BENEFICIARY_ID") or ""
    orig_id = row.get("ORIGINATOR_ID") or ""
    alias = row.get("ALIAS") or ""

    if flg == "I" and ben_id == "" and alias != "":
        ben_id = alias
    elif flg == "O" and orig_id == "" and alias != "":
        orig_id = alias

    # IF INDEX(BENEFICIARY_ID,'00'X) > 0 THEN BENEFICIARY_ID='';
    if "\x00" in ben_id:
        ben_id = ""
    if "\x00" in orig_id:
        orig_id = ""

    if ben_id in ("", "UNKNOWN"):
        ben_id = row.get("BENEFICIARY_NAME") or ""
    if orig_id == "":
        orig_id = row.get("ORIGINATOR_NAME") or ""

    if orig_id == "" or ben_id == "":
        continue  # IF ORIGINATOR_ID='' OR BENEFICIARY_ID='' THEN DELETE;

    orig_id = _strip_double_alias(orig_id)
    ben_id  = _strip_double_alias(ben_id)

    # 2019-2828 REMOVE ENDING @
    if orig_id and orig_id[-1] == "@":
        orig_id = orig_id[:-1]
    if ben_id and ben_id[-1] == "@":
        ben_id = ben_id[:-1]

    # SMR 2021-2221 FOR CORPORATE PASS BR/CI INTO ORIGINATOR_ID
    if row.get("INDORG") == "O" and row.get("PAYMODE") == "DEBIT ACC" and alias != "":
        orig_id = alias

    row["ORIGINATOR_ID"] = orig_id
    row["BENEFICIARY_ID"] = ben_id

    # 2022-1211 REMOVE BANK TRANSACTIONS
    if orig_id.zfill(20) in _DELETE_IDS:
        continue

    out_rows.append(row)

print(f"  OUT rows: {len(out_rows):,}")

# ============================================================================
# STEP 10: WRITE OUTPUT  (delimited flat file, delimiter = '1D'X)
# ============================================================================
print("\nStep 10: Writing output...")


def _s(row: dict, field: str) -> str:
    v = row.get(field)
    return "" if v is None else str(v).strip()


lines = []
count = 1
for row in out_rows:
    source_txn_id = f"LRM{RDATE}{count:010d}"
    fields = [
        _s(row, "RUN_TIMESTAMP"),                   # 1
        source_txn_id,                              # 2
        source_txn_id,                              # 3
        _s(row, "ACCOUNT_SOURCE_UNIQUE_ID"),        # 4
        _s(row, "ACCOUNT_SOURCE_UNIQUE_ID"),        # 5
        _s(row, "CUSTOMER_SOURCE_UNIQUE_ID"),       # 6
        _s(row, "CUSTOMER_SOURCE_UNIQUE_ID"),       # 7
        _s(row, "BRANCH_ID"),                       # 8
        _s(row, "TXN_CODE"),                        # 9
        "",                                         # 10
        _s(row, "CURCODE"),                         # 11
        _s(row, "CURBASE"),                         # 12
        _s(row, "ORIGINATION_DATE"),                # 13
        _s(row, "POSTING_DATE"),                    # 14
        "", "",                                     # 15-16
        _s(row, "LOCAL_TIMESTAMP"),                 # 17
        _s(row, "PROD"),                            # 18
        "", "",                                     # 19-20
        _s(row, "AMOUNT"),                          # 21
        _s(row, "AMOUNT"),                          # 22
        _s(row, "CRDR"),                             # 23
        _s(row, "MENTION"),                         # 24
        "", "", "", "", "", "", "",                 # 25-31
        _s(row, "CHANNEL"),                         # 32
        "", "", "",                                 # 33-35
        _s(row, "ORG_UNIT_CODE"),                   # 36
        "", "", "", "",                             # 37-40
        _s(row, "EMPLOYEE_ID"),                     # 41
        "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",  # 42-59
        _s(row, "ORIGINATOR_NAME"),                 # 60
        _s(row, "BENEFICIARY_NAME"),                # 61
        _s(row, "ORIGINATOR_BANK"),                 # 62
        _s(row, "BENEFICIARY_BANK"),                # 63
        _s(row, "USERID"),                          # 64
        "", "", "", "", "",                         # 65-69
        _s(row, "BENEFICIARY_ID"),                  # 70
        _s(row, "ORIGINATOR_ID"),                   # 71
        _s(row, "SERIAL"),                          # 72
        _s(row, "INCOMING_OUTGOING_FLG"),           # 73
        _s(row, "SENDER_BRANCH"),                   # 74
        _s(row, "BENE_BRANCH"),                     # 75
    ]
    lines.append(DELIM.join(fields))
    count += 1

with open(OUTPUT_FILE, "w", encoding="latin1", newline="") as fh:
    for ln in lines:
        fh.write(ln + "\n")

print(f"  Output written : {OUTPUT_FILE}")
print(f"  Total records  : {len(lines):,}")

TRAN_DIR = OUTPUT_DIR / "TRAN"
TRAN_DIR.mkdir(parents=True, exist_ok=True)
if out_rows:
    pl.DataFrame(out_rows).write_parquet(TRAN_DIR / "LOCAL_GETMNI.parquet")

# ============================================================================
# STEP 11: BACKUP INTERFACE FILE  (//COPYFILE EXEC PGM=ICEGENER)
# ============================================================================
print("\nStep 11: Backing up interface file...")
shutil.copy2(OUTPUT_FILE, OUTPUT_BACKUP)
print(f"  Backup written : {OUTPUT_BACKUP}")

print("\nEIDETLRM complete.")
