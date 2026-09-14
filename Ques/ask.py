#!/usr/bin/env python3
"""
Program : EIDETFRM
Purpose : Extract remittance foreign & local transaction IFS for DETICA
          (AML interface feed) -- TT (telegraphic transfer), WU (Western
          Union style wire), PBMT (PB money transfer) and BT (bank
          transfer) records are pulled from the daily foreign remittance
          transaction file, enriched with account/customer identifiers,
          and written as a pipe(0x1D)-delimited flat file for DETICA.

Dependency:
    %INC PGM(PBBELF);
        -> from PBBELF import format_brchrvr, format_brchcd
        PUT(BRANCHABB,$BRCHRVR.)  -> format_brchrvr(branch_name)  (name->code)
        PUT(ACCTBRCH,BRCHCD.)     -> format_brchcd(branch_code)   (code->name)
        PUT(BRANCH_ID*1,BRCHCD.)  -> format_brchcd(branch_code)   (code->name)

============================================================================
REPORT DATE
============================================================================
The original SAS reads DP.REPTDATE (a one-row control dataset) to obtain
REPTDATE. No such control dataset/parquet exists for this program, so a
local report-date function is used instead (see get_reptdate() below),
mirroring the standard "yesterday" batch convention used elsewhere in this
project. NOWK is derived by EXACT day match (8/15/22/else 4) -- same
divergent-from-REPTDATE.py convention documented in EIIMRM01.py.

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
 1. DP.REPTDATE           -> NOT READ. Replaced by get_reptdate() (see above).

 2. DP.CURRENT   (JCL //DP  DD DSN=SAP.PBB.MNITB(0),  member CURRENT)
    File : INPUT_DP_CURRENT_FILE  -> dp_current.sas7bdat
 3. IDP.CURRENT  (JCL //IDP DD DSN=SAP.PIBB.MNITB(0), member CURRENT)
    File : INPUT_IDP_CURRENT_FILE -> idp_current.sas7bdat
 4. DP.SAVING    (member SAVING, //DP)
    File : INPUT_DP_SAVING_FILE   -> dp_saving.sas7bdat
 5. IDP.SAVING   (member SAVING, //IDP)
    File : INPUT_IDP_SAVING_FILE  -> idp_saving.sas7bdat
 6. DP.FD        (member FD, //DP)
    File : INPUT_DP_FD_FILE       -> dp_fd.sas7bdat
 7. IDP.FD       (member FD, //IDP)
    File : INPUT_IDP_FD_FILE      -> idp_fd.sas7bdat
 8. DP.UMA       (member UMA, //DP)
    File : INPUT_DP_UMA_FILE      -> dp_uma.sas7bdat
 9. IDP.UMA      (member UMA, //IDP)
    File : INPUT_IDP_UMA_FILE     -> idp_uma.sas7bdat
10. DP.VOSTRO    (member VOSTRO, //DP)  -- NOTE: no IDP.VOSTRO is read in
    the original SAS (DATA DEPO only SETs DP.VOSTRO, no Islamic side).
    File : INPUT_DP_VOSTRO_FILE   -> dp_vostro.sas7bdat
    Cols used (2-10): ACCTNO, PRODUCT, BRANCH

11. LN.LNNOTE    (JCL //LN  DD DSN=SAP.PBB.MNILN(0))
    File : INPUT_LN_LNNOTE_FILE   -> ln_lnnote.sas7bdat
12. ILN.LNNOTE   (JCL //ILN DD DSN=SAP.PIBB.MNILN(0))
    File : INPUT_ILN_LNNOTE_FILE  -> iln_lnnote.sas7bdat
    Cols used : ACCTNO, NOTENO, COSTCTR, LOANTYPE

13. REM.REMTRAN&REPTMON&NOWK&REPTYEAR (JCL //REM DD DSN=SAP.PBB.CRM.RMTRNSAC(0))
    Deterministic filename (fully derived from REPTMON/NOWK/REPTYEAR
    tokens) -> constructed directly, input_date.get_latest_file() NOT used.
    File : INPUT_REMTRAN_FILE -> remtran_{REPTMON}{NOWK}{REPTYEAR}.sas7bdat
    Cols used : REMTYPE, APPLNAME, BENENAME, BNAD1, BNAD2, ANAD1, ANAD2,
                BRANCHABB, CURRENCY, PAYMODE, SERIAL, ISSDTE, LASTTRAN,
                TIMESTAMP, ISTTYPE, STATUS, NEWIC, SWIFTCODE, PAYREF,
                FORAMT, AMOUNT, USERID, ALIAS
    Assumption: ISSDTE is a SAS numeric date (days since 1960-01-01),
    converted per project convention. LASTTRAN and TIMESTAMP are assumed
    to be character fields as manipulated by the SAS source (COMPRESS /
    fixed-position SUBSTR respectively).

14. CIS.CUSTDLY  (JCL //CIS DD DSN=RBP2.B033.CIS.CUST.DAILY)
    File : INPUT_CIS_CUSTDLY_FILE -> cis_custdly.sas7bdat
    Cols used : ACCTCODE, ACCTNO, CUSTNO, ALIAS, PRISEC

============================================================================
OUTPUT
============================================================================
//FORRMT DD DSN=SAP.AML.DETICA.REMTRAN.FOREIGN.TEXT, DISP=OLD
Fixed catalogued name (no date token) -> static output filename.
Pipe-delimited (delimiter = hex '1D'X, ASCII 0x1D Group Separator) flat
file, 75 fields per record, most fields blank. No ASA control byte (this
is a data feed, not a report).
File : OUTPUT_FILE -> EIDETFRM_FOREIGN.txt  (encoding='latin1' so the 0x1D
delimiter byte round-trips safely)

//COPYFILE backs the interface file up to .TEXT.BKP; //DELETE at job start
removes any pre-existing backup. Both are reproduced as file operations
below.
"""

import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

from PBBELF import format_brchrvr, format_brchcd

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_DIR = STG_DIR / "sasdata"

INPUT_DP_CURRENT_FILE   = INPUT_DIR / "dp_current.sas7bdat"
INPUT_IDP_CURRENT_FILE  = INPUT_DIR / "idp_current.sas7bdat"
INPUT_DP_SAVING_FILE    = INPUT_DIR / "dp_saving.sas7bdat"
INPUT_IDP_SAVING_FILE   = INPUT_DIR / "idp_saving.sas7bdat"
INPUT_DP_FD_FILE        = INPUT_DIR / "dp_fd.sas7bdat"
INPUT_IDP_FD_FILE       = INPUT_DIR / "idp_fd.sas7bdat"
INPUT_DP_UMA_FILE       = INPUT_DIR / "dp_uma.sas7bdat"
INPUT_IDP_UMA_FILE      = INPUT_DIR / "idp_uma.sas7bdat"
INPUT_DP_VOSTRO_FILE    = INPUT_DIR / "dp_vostro.sas7bdat"

INPUT_LN_LNNOTE_FILE    = INPUT_DIR / "ln_lnnote.sas7bdat"
INPUT_ILN_LNNOTE_FILE   = INPUT_DIR / "iln_lnnote.sas7bdat"

INPUT_CIS_CUSTDLY_FILE  = INPUT_DIR / "cis_custdly.sas7bdat"

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIDETFRM"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIDETFRM"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE      = OUTPUT_DIR / "EIDETFRM_FOREIGN.txt"
OUTPUT_BACKUP    = OUTPUT_DIR / "EIDETFRM_FOREIGN.txt.bkp"

CHUNK_ROWS = 500_000
DELIM = "\x1d"   # '1D'X

# ============================================================================
# STEP 0: DELETE OLD BACKUP  (//DELETE EXEC PGM=IEFBR14)
# ============================================================================
print("Step 0: Removing stale backup file (if present)...")
OUTPUT_BACKUP.unlink(missing_ok=True)

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

# MM / YYYY are SYMPUT'd in the original SAS but never referenced again
# anywhere else in the program body -- dead symbolic variables, kept only
# for documentation parity.
MM   = REPTMON
YYYY = reptdate.strftime("%Y")

print(f"  REPTDATE : {reptdate}   NOWK: {NOWK}   REPTMON: {REPTMON}   REPTYEAR: {REPTYEAR}")
print(f"  RDATE    : {RDATE}")

INPUT_REMTRAN_FILE = INPUT_DIR / f"remtran_{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"

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
# %BRH branch-override lookup tables
# ----------------------------------------------------------------------
# Every %BRH(BRH_ID,ACCT_ID,CUST_ID) call in the original SAS follows a
# strict, mechanical naming pattern:
#     ACCT_ID = 'RMT' || ZFILL(BRH_ID,5) || <TYPE><DIR>A
#     CUST_ID = 'RMT' || ZFILL(BRH_ID,5) || <TYPE>C
# where TYPE in {TF, WF, PB} and DIR in {I, O}. Rather than reproduce
# ~300 IF/DO blocks per data step verbatim, the exact branch-ID list used
# by each macro-call block is preserved below and the ACCT_ID/CUST_ID
# strings are generated by that same pattern -- byte-identical results,
# far more maintainable.
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
# WU_INWARD / WU_OUTWARD macro blocks omit branch 32.
_BRANCH_LIST_NO_32 = [b for b in _FULL_BRANCH_LIST if b != 32]


def _brh_lookup(branch_ids: list, acct_suffix: str, cust_suffix: str) -> dict:
    return {
        bid: (f"RMT{bid:05d}{acct_suffix}", f"RMT{bid:05d}{cust_suffix}")
        for bid in branch_ids
    }


TT_INWARD_BRH  = _brh_lookup(_FULL_BRANCH_LIST, "TFIA", "TFC")
TT_OUTWARD_BRH = _brh_lookup(_FULL_BRANCH_LIST, "TFOA", "TFC")
WU_INWARD_BRH  = _brh_lookup(_BRANCH_LIST_NO_32, "WFIA", "WFC")
WU_OUTWARD_BRH = _brh_lookup(_BRANCH_LIST_NO_32, "WFOA", "WFC")
PBMT_BRH       = _brh_lookup(_FULL_BRANCH_LIST, "PBOA", "PBC")
BT_BRH         = TT_OUTWARD_BRH  # BT reuses the TFOA/TFC table exactly.


def _apply_brh(branch_id: Optional[str], lookup: dict,
                default_acct: str, default_cust: str) -> tuple:
    """Applies the %BRH(BRANCH_ID,...) IF-chain: returns (acct_id, cust_id),
    falling back to the pre-computed defaults when BRANCH_ID doesn't match
    any entry (i.e. none of the IF conditions fired)."""
    try:
        bid = int(str(branch_id).strip())
    except (TypeError, ValueError):
        return default_acct, default_cust
    return lookup.get(bid, (default_acct, default_cust))


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
LN_LNNOTE_CACHE    = _load_cached(INPUT_LN_LNNOTE_FILE, "LN_LNNOTE")
ILN_LNNOTE_CACHE   = _load_cached(INPUT_ILN_LNNOTE_FILE, "ILN_LNNOTE")
REMTRAN_CACHE      = _load_cached(INPUT_REMTRAN_FILE, "REMTRAN")
CIS_CUSTDLY_CACHE  = _load_cached(INPUT_CIS_CUSTDLY_FILE, "CIS_CUSTDLY")

# ============================================================================
# STEP 3: DATA FOREIGN  (SET REM.REMTRAN...; IF REMTYPE='F'; exclusion filter)
# ============================================================================
print("\nStep 3: Building FOREIGN...")

con = duckdb.connect(database=":memory:")
foreign_pl = con.execute(f"""
    SELECT
        REMTYPE, APPLNAME, BENENAME, BNAD1, BNAD2, ANAD1, ANAD2,
        BRANCHABB, CURRENCY, PAYMODE, SERIAL, ISSDTE, LASTTRAN,
        TIMESTAMP, ISTTYPE, STATUS, NEWIC, SWIFTCODE, PAYREF,
        FORAMT, AMOUNT, USERID, ALIAS
    FROM read_parquet('{REMTRAN_CACHE.as_posix()}')
    WHERE REMTYPE = 'F'
""").pl()
con.close()


def _in_exclude_list(value) -> bool:
    return value is not None and value.strip().upper() in COMPANY_EXCLUDE_LIST


foreign_rows = []
for r in foreign_pl.iter_rows(named=True):
    # *2017-2058;
    if (_in_exclude_list(r["APPLNAME"]) or _in_exclude_list(r["BENENAME"])
            or _in_exclude_list(r["BNAD1"]) or _in_exclude_list(r["BNAD2"])
            or _in_exclude_list(r["ANAD1"]) or _in_exclude_list(r["ANAD2"])):
        continue
    foreign_rows.append(r)

print(f"  FOREIGN rows: {len(foreign_rows):,}")

# ============================================================================
# STEP 4: DATA TT_INWARD TT_OUTWARD WU_OUTWARD WU_INWARD PBMT BT;
#         SET FOREIGN; ...
# ============================================================================
print("\nStep 4: Routing FOREIGN into TT/WU/PBMT/BT buckets...")


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


tt_inward, tt_outward, wu_outward, wu_inward, pbmt_rows, bt_rows = [], [], [], [], [], []

for r in foreign_rows:
    branchabb  = r["BRANCHABB"]
    row = dict(r)
    row["RUN_TIMESTAMP"] = (RDATE + "000000")[:14]
    row["BRANCH_ID"] = format_brchrvr(branchabb)  # PUT(BRANCHABB,$BRCHRVR.)
    row["ORIGINATOR_NAME"] = r["ANAD1"]
    row["BENEFICIARY_NAME"] = r["BNAD2"]
    row["CURCODE"] = r["CURRENCY"]
    row["CURBASE"] = "MYR"
    row["MENTION"] = r["PAYMODE"]
    row["CHANNEL"] = 999
    row["REMITTANCE_REF_NO"] = r["SERIAL"]
    row["EMPLOYEE_ID"] = 88888
    row["ORIGINATION_DATE"] = _format_issdte(r["ISSDTE"])
    row["POSTING_DATE"] = (r["LASTTRAN"] or "").replace("-", "")

    ts = r["TIMESTAMP"] or ""
    yyyy = _substr_timestamp(ts, 1, 4)
    mm   = _substr_timestamp(ts, 6, 2)
    dd   = _substr_timestamp(ts, 9, 2)
    hour = _substr_timestamp(ts, 12, 2)
    minute = _substr_timestamp(ts, 15, 2)
    sec  = _substr_timestamp(ts, 18, 2)
    row["LOCAL_TIMESTAMP"] = (yyyy + mm + dd + hour + minute + sec)[:14]

    if branchabb in ("701", "702", "IKB", "IPJ"):
        row["ORG_UNIT_CODE"] = "PIBBTRSRY"
    else:
        row["ORG_UNIT_CODE"] = "PBBTRSRY"

    isttype, status = r["ISTTYPE"], r["STATUS"]
    if isttype == "TF" and status == "TO":
        tt_outward.append(dict(row))
    if isttype == "DF" and status == "MO":
        tt_outward.append(dict(row))
    if isttype == "BK" and status == "TO":
        tt_outward.append(dict(row))
    if isttype == "TF" and status == "TI":
        tt_inward.append(dict(row))
    if isttype == "DF" and status == "PP":
        tt_inward.append(dict(row))
    if isttype == "WF" and status == "TO":
        wu_outward.append(dict(row))
    if isttype == "WF" and status == "TI":
        wu_inward.append(dict(row))
    if isttype == "BF" and status == "MO":
        pbmt_rows.append(dict(row))
    if isttype == "BT" and status == "IS":
        bt_rows.append(dict(row))

print(f"  TT_INWARD:{len(tt_inward):,}  TT_OUTWARD:{len(tt_outward):,}  "
      f"WU_INWARD:{len(wu_inward):,}  WU_OUTWARD:{len(wu_outward):,}  "
      f"PBMT:{len(pbmt_rows):,}  BT:{len(bt_rows):,}")

# ============================================================================
# STEP 5: DATA TT_INWARD; SET TT_INWARD; ... (per-dataset transforms)
# ============================================================================
print("\nStep 5: Applying per-bucket transforms...")

for row in tt_inward:
    row["INCOMING_OUTGOING_FLG"] = "I"
    row["TXN_CODE"] = "RMT003"
    row["BENEFICIARY_ID"] = row["NEWIC"]
    row["SENDER_BRANCH"] = "0"
    row["BENE_BRANCH"] = row["BRANCHABB"]
    row["ORIGINATOR_BANK"] = row["SWIFTCODE"]
    row["BENEFICIARY_BANK"] = row["BRANCHABB"]
    row["CRDR"] = "C"
    # temp_acct = "".join(ch for ch in (row["BNAD1"] or "") if ch.isalnum())
    temp_acct = "".join(ch for ch in (row["BNAD1"] or "") if ch in "0123456789")
    if len(temp_acct) != 10:
        temp_acct = ""
    first_digit = temp_acct[:1]
    temp_acctcode = "LN" if first_digit == "2" else "DP"
    if temp_acct != "":
        row["ACCOUNT_SOURCE_UNIQUE_ID"] = (temp_acctcode + temp_acct).replace(" ", "")
    else:
        row["ACCOUNT_SOURCE_UNIQUE_ID"] = None

# PROC SORT DATA=TT_INWARD; BY ACCOUNT_SOURCE_UNIQUE_ID;
tt_inward.sort(key=lambda r: (r["ACCOUNT_SOURCE_UNIQUE_ID"] is None,
                               r["ACCOUNT_SOURCE_UNIQUE_ID"] or ""))

for row in tt_outward:
    row["INCOMING_OUTGOING_FLG"] = "O"
    row["TXN_CODE"] = "RMT004"
    row["ORIGINATOR_ID"] = row["NEWIC"]
    row["SENDER_BRANCH"] = row["BRANCHABB"]
    row["BENE_BRANCH"] = "0"
    row["BENEFICIARY_NAME"] = row["BNAD1"]
    row["ORIGINATOR_BANK"] = row["BRANCHABB"]
    row["BENEFICIARY_BANK"] = row["SWIFTCODE"]
    row["ACCOUNT_SOURCE_UNIQUE_ID"] = "RMT00003A"
    row["CUSTOMER_SOURCE_UNIQUE_ID"] = "RMT00003C"
    row["ACCOUNT_SOURCE_UNIQUE_ID"], row["CUSTOMER_SOURCE_UNIQUE_ID"] = _apply_brh(
        row["BRANCH_ID"], TT_OUTWARD_BRH,
        row["ACCOUNT_SOURCE_UNIQUE_ID"], row["CUSTOMER_SOURCE_UNIQUE_ID"])
    row["PROD"] = "RT102"
    row["CRDR"] = "D"
    # *ORG_UNIT_CODE = 'PBB';   (commented out in source, no effect)

for row in wu_inward:
    row["INCOMING_OUTGOING_FLG"] = "I"
    row["TXN_CODE"] = "RMT005"
    row["BENEFICIARY_ID"] = row["NEWIC"]
    row["SENDER_BRANCH"] = "0"
    row["BENE_BRANCH"] = row["BRANCHABB"]
    row["BENEFICIARY_BANK"] = row["BRANCHABB"]
    row["BENEFICIARY_NAME"] = row["BNAD1"]
    row["CRDR"] = "C"
    if row["PAYMODE"] == "CR A/C":
        row["ACCOUNT_SOURCE_UNIQUE_ID"] = ("DP" + (row["PAYREF"] or "")).replace(" ", "")
    else:
        row["ACCOUNT_SOURCE_UNIQUE_ID"] = None

wu_inward.sort(key=lambda r: (r["ACCOUNT_SOURCE_UNIQUE_ID"] is None,
                               r["ACCOUNT_SOURCE_UNIQUE_ID"] or ""))

for row in wu_outward:
    row["INCOMING_OUTGOING_FLG"] = "O"
    row["TXN_CODE"] = "RMT006"
    row["ORIGINATOR_ID"] = row["NEWIC"]
    row["SENDER_BRANCH"] = row["BRANCHABB"]
    row["BENE_BRANCH"] = "0"
    row["BENEFICIARY_NAME"] = row["BNAD1"]
    row["ORIGINATOR_BANK"] = row["BRANCHABB"]
    row["CRDR"] = "D"
    if row["PAYMODE"] == "EBNK DEBIT":
        row["ACCOUNT_SOURCE_UNIQUE_ID"] = ("DP" + (row["PAYREF"] or "")).replace(" ", "")
    else:
        row["ACCOUNT_SOURCE_UNIQUE_ID"] = None

wu_outward.sort(key=lambda r: (r["ACCOUNT_SOURCE_UNIQUE_ID"] is None,
                                r["ACCOUNT_SOURCE_UNIQUE_ID"] or ""))

for row in pbmt_rows:
    row["INCOMING_OUTGOING_FLG"] = "O"
    row["TXN_CODE"] = "RMT008"
    row["ORIGINATOR_ID"] = row["NEWIC"]
    row["SENDER_BRANCH"] = row["BRANCHABB"]
    row["BENE_BRANCH"] = "0"
    row["ORIGINATOR_BANK"] = row["BRANCHABB"]
    row["BENEFICIARY_BANK"] = row["SWIFTCODE"]
    row["CRDR"] = "D"
    row["PROD"] = "RT107"
    row["CUSTOMER_SOURCE_UNIQUE_ID"] = "RMT00007C"
    row["ACCOUNT_SOURCE_UNIQUE_ID"] = "RMT00007A"
    row["BENEFICIARY_NAME"] = row["BNAD1"]
    row["ACCOUNT_SOURCE_UNIQUE_ID"], row["CUSTOMER_SOURCE_UNIQUE_ID"] = _apply_brh(
        row["BRANCH_ID"], PBMT_BRH,
        row["ACCOUNT_SOURCE_UNIQUE_ID"], row["CUSTOMER_SOURCE_UNIQUE_ID"])

for row in bt_rows:
    row["INCOMING_OUTGOING_FLG"] = "O"
    branch_id_numeric = "".join(ch for ch in str(row["BRANCHABB"]) if ch.isdigit() or ch == "-")
    row["BRANCH_ID"] = branch_id_numeric
    row["TXN_CODE"] = "RMT004"
    row["ORIGINATOR_ID"] = row["NEWIC"]
    try:
        bid_int = int(branch_id_numeric)
    except ValueError:
        bid_int = None
    row["SENDER_BRANCH"] = format_brchcd(bid_int) if bid_int is not None else ""
    row["BENE_BRANCH"] = "0"
    row["ORIGINATOR_BANK"] = format_brchcd(bid_int) if bid_int is not None else ""
    row["BENEBANK"] = row["SWIFTCODE"]  # NOTE: 'BENEBANK', not BENEFICIARY_BANK
    # -- typo preserved verbatim from the SAS source. BENEFICIARY_BANK is
    # therefore NEVER populated for BT records and stays blank in the
    # final output, exactly as in the original program.
    row["CRDR"] = "D"
    row["PROD"] = "RT102"  # * CHECK WITH USER;
    row["CUSTOMER_SOURCE_UNIQUE_ID"] = "RMT00003C"
    row["ACCOUNT_SOURCE_UNIQUE_ID"] = "RMT00003A"
    row["BENEFICIARY_NAME"] = row["BNAD1"]
    row["ACCOUNT_SOURCE_UNIQUE_ID"], row["CUSTOMER_SOURCE_UNIQUE_ID"] = _apply_brh(
        row["BRANCH_ID"], BT_BRH,
        row["ACCOUNT_SOURCE_UNIQUE_ID"], row["CUSTOMER_SOURCE_UNIQUE_ID"])

# ============================================================================
# STEP 6: DATA LOAN;  (SET LN.LNNOTE ILN.LNNOTE; ... NODUPKEY BY ACCTNO)
# ============================================================================
print("\nStep 6: Building LOAN (highest NOTENO per ACCTNO)...")

con = duckdb.connect(database=":memory:")
loan_pl = con.execute(f"""
    SELECT CAST(ACCTNO AS VARCHAR) AS ACCTNO, NOTENO, COSTCTR, LOANTYPE
    FROM read_parquet('{LN_LNNOTE_CACHE.as_posix()}')
    UNION ALL
    SELECT CAST(ACCTNO AS VARCHAR) AS ACCTNO, NOTENO, COSTCTR, LOANTYPE
    FROM read_parquet('{ILN_LNNOTE_CACHE.as_posix()}')
""").pl()
con.close()

# ORG_UNIT_CODE derivation is commented out in the original SAS (dead code):
#   IF (3000<=COSTCTR<=3999) THEN ORG_UNIT_CODE = 'PIBBLN';
#   ELSE                          ORG_UNIT_CODE = 'PBBLN ';
# ORG_UNIT_CODE is therefore never assigned here and stays blank/missing.
loan_pl = loan_pl.with_columns([
    pl.lit("LN").alias("MNI_ACCTCODE"),
    ("LN" + pl.col("LOANTYPE").cast(pl.Int64).cast(pl.Utf8).str.zfill(3)).alias("PROD"),
    pl.lit(None, dtype=pl.Utf8).alias("ORG_UNIT_CODE"),
])
# PROC SORT DATA=LOAN; BY ACCTNO DESCENDING NOTENO;
# PROC SORT DATA=LOAN NODUPKEY; BY ACCTNO;  -> keep highest NOTENO per ACCTNO.
loan_pl = (
    loan_pl.sort(["ACCTNO", "NOTENO"], descending=[False, True])
    .unique(subset=["ACCTNO"], keep="first")
    .select(["ACCTNO", "PROD", "MNI_ACCTCODE", "ORG_UNIT_CODE"])
)
print(f"  LOAN rows: {loan_pl.height:,}")

# ============================================================================
# STEP 7: DATA DEPO;  (SET DP.CURRENT IDP.CURRENT DP.SAVING IDP.SAVING
#                          DP.FD IDP.FD DP.UMA IDP.UMA DP.VOSTRO; ...)
# ============================================================================
print("\nStep 7: Building DEPO...")

# _depo_sources = [
#     DP_CURRENT_CACHE, IDP_CURRENT_CACHE, DP_SAVING_CACHE, IDP_SAVING_CACHE,
#     DP_FD_CACHE, IDP_FD_CACHE, DP_UMA_CACHE, IDP_UMA_CACHE, DP_VOSTRO_CACHE,
# ]
# con = duckdb.connect(database=":memory:")
# _union_sql = " UNION ALL ".join(
#     f"SELECT CAST(ACCTNO AS VARCHAR) AS ACCTNO, PRODUCT, BRANCH "
#     f"FROM read_parquet('{p.as_posix()}')"
#     for p in _depo_sources
# )
# depo_pl = con.execute(_union_sql).pl()
# con.close()

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
depo_pl = con.execute(_union_sql).pl()
con.close()

# ORG_UNIT_CODE is only assigned inside commented-out code in the SAS
# source (same PIBBDP/PBBDP pattern as LOAN above) -- KEEP references it
# but it is never actively set, so it stays blank/missing here, preserved
# exactly for downstream MERGE fidelity (see Step 10 note).
depo_pl = depo_pl.with_columns([
    ("DP" + pl.col("PRODUCT").cast(pl.Int64).cast(pl.Utf8).str.zfill(3)).alias("PROD"),
    pl.lit("DP").alias("MNI_ACCTCODE"),
    pl.lit(None, dtype=pl.Utf8).alias("ORG_UNIT_CODE"),
    pl.col("BRANCH").alias("ACCTBRCH"),
]).select(["ACCTNO", "PROD", "MNI_ACCTCODE", "ORG_UNIT_CODE", "ACCTBRCH"])
print(f"  DEPO rows: {depo_pl.height:,}")

# ============================================================================
# STEP 8: DATA ACCT;  (SET DEPO LOAN; ACCOUNT_SOURCE_UNIQUE_ID=...;)
# ============================================================================
print("\nStep 8: Building ACCT...")

acct_pl = pl.concat(
    [depo_pl.select(["ACCTNO", "MNI_ACCTCODE", "ORG_UNIT_CODE", "PROD"]),
     loan_pl.select(["ACCTNO", "MNI_ACCTCODE", "ORG_UNIT_CODE", "PROD"])],
    how="vertical_relaxed",
).with_columns([
    (pl.col("MNI_ACCTCODE") + pl.col("ACCTNO")).str.replace_all(" ", "")
    .alias("ACCOUNT_SOURCE_UNIQUE_ID"),
    pl.lit("Y").alias("BANK_ACC_IND"),
])
# PROC SORT DATA=ACCT; BY ACCOUNT_SOURCE_UNIQUE_ID;
acct_pl = acct_pl.sort("ACCOUNT_SOURCE_UNIQUE_ID")
_acct_lookup = {
    r["ACCOUNT_SOURCE_UNIQUE_ID"]: r
    for r in acct_pl.iter_rows(named=True)
}
print(f"  ACCT rows: {acct_pl.height:,}")

# ============================================================================
# STEP 9: DATA CIS;  (SET CIS.CUSTDLY; WHERE PRISEC=901 AND ACCTCODE IN...)
# ============================================================================
print("\nStep 9: Building CIS...")

con = duckdb.connect(database=":memory:")
cis_pl = con.execute(f"""
    SELECT
        CAST(ACCTCODE AS VARCHAR) AS ACCTCODE,
        CAST(ACCTNO AS VARCHAR)   AS ACCTNO,
        CAST(CUSTNO AS VARCHAR)   AS CUSTNO
    FROM read_parquet('{CIS_CUSTDLY_CACHE.as_posix()}')
    WHERE PRISEC = 901 AND ACCTCODE IN ('DP','LN')
""").pl()
con.close()

cis_pl = cis_pl.with_columns([
    (pl.col("ACCTCODE") + pl.col("ACCTNO")).str.replace_all(" ", "")
    .alias("ACCOUNT_SOURCE_UNIQUE_ID"),
    # ("CIS" + pl.col("CUSTNO")).str.replace_all(" ", "").alias("CIS"),
    ("CIS" + pl.col("CUSTNO").fill_null("")).str.replace_all(" ", "").alias("CIS"),
])
# PROC SORT DATA=CIS NODUPKEY; BY ACCOUNT_SOURCE_UNIQUE_ID;  (first wins)
cis_pl = cis_pl.sort("ACCOUNT_SOURCE_UNIQUE_ID").unique(
    subset=["ACCOUNT_SOURCE_UNIQUE_ID"], keep="first")
_cis_lookup = {
    r["ACCOUNT_SOURCE_UNIQUE_ID"]: r["CIS"]
    for r in cis_pl.iter_rows(named=True)
}
print(f"  CIS rows: {cis_pl.height:,}")

# ============================================================================
# STEP 10: DATA TRAN.TT_INWARD/TT_OUTWARD/WU_INWARD/WU_OUTWARD/PBMT/BT;
#          MERGE <bucket>(IN=A) ACCT CIS;  BY ACCOUNT_SOURCE_UNIQUE_ID;  IF A;
# ----------------------------------------------------------------------
# SAS MERGE last-dataset-wins semantics: for a matching BY-group, ACCT's
# ORG_UNIT_CODE value (always blank/None -- see Step 7/8 note) overwrites
# whatever ORG_UNIT_CODE was set earlier in Step 5, even though ACCT never
# actively assigns it. This blanking-on-match is preserved deliberately.
# ============================================================================
print("\nStep 10: Merging ACCT/CIS onto each bucket...")


# def _merge_acct_cis(bucket_rows, brh_lookup, default_acct, default_cust):
#     out = []
#     for row in bucket_rows:
#         key = row.get("ACCOUNT_SOURCE_UNIQUE_ID")
#         acct_match = _acct_lookup.get(key) if key else None
#         cis_value = _cis_lookup.get(key) if key else None
#
#         if acct_match is not None:
#             row["BANK_ACC_IND"] = acct_match["BANK_ACC_IND"]
#             # Last-dataset-wins: ACCT's (always-blank) ORG_UNIT_CODE
#             # overwrites the value Step 5 assigned for this row.
#             row["ORG_UNIT_CODE"] = acct_match["ORG_UNIT_CODE"]
#         else:
#             row["BANK_ACC_IND"] = None
#
#         row["CIS"] = cis_value
#
#         if row["BANK_ACC_IND"] != "Y":
#             row["ACCOUNT_SOURCE_UNIQUE_ID"] = default_acct
#             row["CUSTOMER_SOURCE_UNIQUE_ID"] = default_cust
#             row["PROD"] = row.get("PROD_OVERRIDE_ON_NOMATCH", row.get("PROD"))
#
#         if row["CIS"]:
#             row["CUSTOMER_SOURCE_UNIQUE_ID"] = row["CIS"]
#         else:
#             row["CUSTOMER_SOURCE_UNIQUE_ID"] = default_cust
#             row["ACCOUNT_SOURCE_UNIQUE_ID"] = default_acct
#
#         if row["BANK_ACC_IND"] != "Y" or not row["CIS"]:
#             row["ACCOUNT_SOURCE_UNIQUE_ID"], row["CUSTOMER_SOURCE_UNIQUE_ID"] = _apply_brh(
#                 row.get("BRANCH_ID"), brh_lookup,
#                 row["ACCOUNT_SOURCE_UNIQUE_ID"], row["CUSTOMER_SOURCE_UNIQUE_ID"])
#         out.append(row)
#     return out


def _merge_acct_cis(bucket_rows, brh_lookup, default_acct, default_cust, default_prod):
    out = []
    for row in bucket_rows:
        key = row.get("ACCOUNT_SOURCE_UNIQUE_ID")
        acct_match = _acct_lookup.get(key) if key else None
        cis_value = _cis_lookup.get(key) if key else None

        # SAS MERGE last-dataset-wins: ACCT is the later dataset, so its
        # ORG_UNIT_CODE (always blank) and PROD overwrite on EVERY row,
        # matched or not.
        if acct_match is not None:
            row["BANK_ACC_IND"] = acct_match["BANK_ACC_IND"]
            row["ORG_UNIT_CODE"] = acct_match["ORG_UNIT_CODE"]
            row["PROD"] = acct_match["PROD"]
        else:
            row["BANK_ACC_IND"] = None
            row["ORG_UNIT_CODE"] = None
            row["PROD"] = None

        row["CIS"] = cis_value

        if row["BANK_ACC_IND"] != "Y":
            row["ACCOUNT_SOURCE_UNIQUE_ID"] = default_acct
            row["CUSTOMER_SOURCE_UNIQUE_ID"] = default_cust
            row["PROD"] = default_prod

        if row["CIS"]:
            row["CUSTOMER_SOURCE_UNIQUE_ID"] = row["CIS"]
        else:
            row["CUSTOMER_SOURCE_UNIQUE_ID"] = default_cust
            row["ACCOUNT_SOURCE_UNIQUE_ID"] = default_acct
            row["PROD"] = default_prod

        if row["BANK_ACC_IND"] != "Y" or not row["CIS"]:
            row["ACCOUNT_SOURCE_UNIQUE_ID"], row["CUSTOMER_SOURCE_UNIQUE_ID"] = _apply_brh(
                row.get("BRANCH_ID"), brh_lookup,
                row["ACCOUNT_SOURCE_UNIQUE_ID"], row["CUSTOMER_SOURCE_UNIQUE_ID"])
        out.append(row)
    return out


# for row in tt_inward:
#     row["PROD"] = "RT101"
# tran_tt_inward = _merge_acct_cis(tt_inward, TT_INWARD_BRH, "RMT00004A", "RMT00004C")
#
# tran_tt_outward = _merge_acct_cis(tt_outward, TT_OUTWARD_BRH, "RMT00003A", "RMT00003C")
#
# for row in wu_inward:
#     row["PROD"] = "RT105"
# tran_wu_inward = _merge_acct_cis(wu_inward, WU_INWARD_BRH, "RMT00005A", "RMT00005C")
#
# for row in wu_outward:
#     row["PROD"] = "RT106"
# tran_wu_outward = _merge_acct_cis(wu_outward, WU_OUTWARD_BRH, "RMT00006A", "RMT00006C")
#
# tran_pbmt = _merge_acct_cis(pbmt_rows, PBMT_BRH, "RMT00007A", "RMT00007C")
# tran_bt   = _merge_acct_cis(bt_rows, BT_BRH, "RMT00003A", "RMT00003C")

tran_tt_inward  = _merge_acct_cis(tt_inward,  TT_INWARD_BRH,  "RMT00004A", "RMT00004C", "RT101")
tran_wu_inward  = _merge_acct_cis(wu_inward,  WU_INWARD_BRH,  "RMT00005A", "RMT00005C", "RT105")
tran_wu_outward = _merge_acct_cis(wu_outward, WU_OUTWARD_BRH, "RMT00006A", "RMT00006C", "RT106")

# TT_OUTWARD, PBMT and BT are `SET` (not MERGE) in SAS -> no ACCT/CIS join.
# Step 5 already applied every transformation they need.
tran_tt_outward = tt_outward
tran_pbmt       = pbmt_rows
tran_bt         = bt_rows

print(f"  TRAN.TT_INWARD:{len(tran_tt_inward):,}  TRAN.TT_OUTWARD:{len(tran_tt_outward):,}  "
      f"TRAN.WU_INWARD:{len(tran_wu_inward):,}  TRAN.WU_OUTWARD:{len(tran_wu_outward):,}  "
      f"TRAN.PBMT:{len(tran_pbmt):,}  TRAN.BT:{len(tran_bt):,}")

# ============================================================================
# STEP 11: DATA OUT;  SET TRAN.TT_INWARD TRAN.TT_OUTWARD TRAN.WU_INWARD
#                         TRAN.WU_OUTWARD TRAN.PBMT TRAN.BT;
# ============================================================================
print("\nStep 11: Combining into OUT and applying alias/@ cleanup...")


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
for row in (tran_tt_inward + tran_tt_outward + tran_wu_inward
            + tran_wu_outward + tran_pbmt + tran_bt):
    flg = row.get("INCOMING_OUTGOING_FLG")
    ben_id = row.get("BENEFICIARY_ID") or ""
    orig_id = row.get("ORIGINATOR_ID") or ""
    alias = row.get("ALIAS") or ""

    if flg == "I" and ben_id == "" and alias != "":
        ben_id = alias
    elif flg == "O" and orig_id == "" and alias != "":
        orig_id = alias

    if ben_id == "":
        ben_id = row.get("BENEFICIARY_NAME") or ""
    if orig_id == "":
        orig_id = row.get("ORIGINATOR_NAME") or ""

    if orig_id == "" or ben_id == "":
        continue  # IF ORIGINATOR_ID='' OR BENEFICIARY_ID='' THEN DELETE;

    orig_id = _strip_double_alias(orig_id)
    ben_id  = _strip_double_alias(ben_id)

    # 2019-2828 REMOVE ENDING @  (SAS SUBSTR quirk == "last char == '@'")
    if orig_id and orig_id[-1] == "@":
        orig_id = orig_id[:-1]
    if ben_id and ben_id[-1] == "@":
        ben_id = ben_id[:-1]

    row["ORIGINATOR_ID"] = orig_id
    row["BENEFICIARY_ID"] = ben_id

    # 2022-1211 REMOVE BANK TRANSACTIONS
    if orig_id.zfill(20) in _DELETE_IDS:
        continue

    out_rows.append(row)

print(f"  OUT rows: {len(out_rows):,}")

# ============================================================================
# STEP 12: WRITE OUTPUT  (delimited flat file, delimiter = '1D'X)
# ============================================================================
print("\nStep 12: Writing output...")


def _s(row: dict, field: str) -> str:
    v = row.get(field)
    return "" if v is None else str(v).strip()


lines = []
count = 1
for row in out_rows:
    source_txn_id = f"FRM{RDATE}{count:010d}"
    fields = [
        _s(row, "RUN_TIMESTAMP"),                # 1
        source_txn_id,                            # 2
        source_txn_id,                            # 3
        _s(row, "ACCOUNT_SOURCE_UNIQUE_ID"),       # 4
        _s(row, "ACCOUNT_SOURCE_UNIQUE_ID"),       # 5
        _s(row, "CUSTOMER_SOURCE_UNIQUE_ID"),      # 6
        _s(row, "CUSTOMER_SOURCE_UNIQUE_ID"),      # 7
        _s(row, "BRANCH_ID"),                      # 8
        _s(row, "TXN_CODE"),                       # 9
        "",                                        # 10
        _s(row, "CURCODE"),                        # 11
        _s(row, "CURBASE"),                        # 12
        _s(row, "ORIGINATION_DATE"),                # 13
        _s(row, "POSTING_DATE"),                    # 14
        "", "",                                     # 15-16
        _s(row, "LOCAL_TIMESTAMP"),                  # 17
        _s(row, "PROD"),                             # 18
        "", "",                                       # 19-20
        _s(row, "FORAMT"),                            # 21
        _s(row, "AMOUNT"),                            # 22
        _s(row, "CRDR"),                              # 23
        _s(row, "MENTION"),                           # 24
        "", "", "", "", "", "", "",                   # 25-31
        _s(row, "CHANNEL"),                           # 32
        "", "", "",                                    # 33-35
        _s(row, "ORG_UNIT_CODE"),                       # 36
        "", "", "", "",                                 # 37-40
        _s(row, "EMPLOYEE_ID"),                          # 41
        "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",  # 42-59
        _s(row, "ORIGINATOR_NAME"),                       # 60
        _s(row, "BENEFICIARY_NAME"),                      # 61
        _s(row, "ORIGINATOR_BANK"),                        # 62
        _s(row, "BENEFICIARY_BANK"),                       # 63
        _s(row, "USERID"),                                  # 64
        "", "", "", "", "",                                 # 65-69
        _s(row, "BENEFICIARY_ID"),                           # 70
        _s(row, "ORIGINATOR_ID"),                            # 71
        _s(row, "SERIAL"),                                    # 72
        _s(row, "INCOMING_OUTGOING_FLG"),                      # 73
        _s(row, "SENDER_BRANCH"),                               # 74
        _s(row, "BENE_BRANCH"),                                  # 75
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
for name, rows in [
    ("TT_INWARD", tran_tt_inward),
    ("TT_OUTWARD", tran_tt_outward),
    ("WU_INWARD", tran_wu_inward),
    ("WU_OUTWARD", tran_wu_outward),
    ("PBMT", tran_pbmt),
    ("BT", tran_bt),
]:
    if rows:
        pl.DataFrame(rows).write_parquet(TRAN_DIR / f"{name}.parquet")

# ============================================================================
# STEP 13: BACKUP INTERFACE FILE  (//COPYFILE EXEC PGM=ICEGENER)
# ============================================================================
print("\nStep 13: Backing up interface file...")
shutil.copy2(OUTPUT_FILE, OUTPUT_BACKUP)
print(f"  Backup written : {OUTPUT_BACKUP}")

print("\nEIDETFRM complete.")
