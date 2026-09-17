#!/usr/bin/env python3
"""
Program : EIDETRTS.py
Purpose : Extract remittance transaction IFS for DETICA (AML interface
          feed) -- RENTAS (RENTAS wholesale settlement) inward, outward
          and bank-transfer (BT) records are pulled from the daily RENTAS
          warehouse file, enriched with account/customer identifiers, and
          written as a pipe(0x1D)-delimited flat file for DETICA.

Dependency:
    %INC PGM(PBBELF);
        -> from PBBELF import format_brchrvr, format_brchcd
        PUT(BRANCHABB,$BRCHRVR.)  -> format_brchrvr(branch_name)  (name->code)
        PUT(ACCTBRCH,BRCHCD.)     -> format_brchcd(branch_code)   (code->name)

============================================================================
REPORT DATE
============================================================================
The original SAS reads DP.REPTDATE (a one-row control dataset) to obtain
REPTDATE, then derives WK/WK1 from an exact-day match (day 8/15/22/else),
with NOWK = WK. No such control dataset/parquet exists for this program,
so a local report-date function is used instead (see get_reptdate()
below), mirroring the same "yesterday" batch convention used in
EIDETFRM.py / EIDETLRM.py. WK1 is SYMPUT'd in the original SAS but never
referenced again anywhere else in the program -- a dead symbolic
variable, kept only for documentation parity (not reproduced below).

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
 1. LN.LNNOTE   (JCL //LN  DD DSN=SAP.PBB.MNILN(0))
    ILN.LNNOTE  (JCL //ILN DD DSN=SAP.PIBB.MNILN(0))
    File : INPUT_LN_LNNOTE_FILE   -> ln_lnnote.sas7bdat
    Cols used : ACCTNO, NOTENO, COSTCTR, LOANTYPE, NTBRCH
 2. DP.CURRENT  (JCL //DP  DD DSN=SAP.PBB.MNITB(0),  member CURRENT)
    File : INPUT_DP_CURRENT_FILE  -> ca09126.sas7bdat
 3. IDP.CURRENT (JCL //IDP DD DSN=SAP.PIBB.MNITB(0), member CURRENT)
    File : INPUT_IDP_CURRENT_FILE -> ica09126.sas7bdat
 4. DP.SAVING   (member SAVING, //DP)
    File : INPUT_DP_SAVING_FILE   -> sa09126.sas7bdat
 5. IDP.SAVING  (member SAVING, //IDP)
    File : INPUT_IDP_SAVING_FILE  -> isa09126.sas7bdat
 6. DP.FD       (member FD, //DP)
    File : INPUT_DP_FD_FILE       -> fd09126.sas7bdat
 7. IDP.FD      (member FD, //IDP)
    File : INPUT_IDP_FD_FILE      -> ifd09126.sas7bdat
 8. DP.UMA      (member UMA, //DP)
    File : INPUT_DP_UMA_FILE      -> uma.sas7bdat
 9. IDP.UMA     (member UMA, //IDP)
    File : INPUT_IDP_UMA_FILE     -> iuma.sas7bdat
10. DP.VOSTRO   (member VOSTRO, //DP)  -- NOTE: no IDP.VOSTRO is read in
    the original SAS (DATA DEPO_ACCT only SETs DP.VOSTRO).
    File : INPUT_DP_VOSTRO_FILE   -> vostro08426.sas7bdat
    Cols used (1-3): ACCTNO, PRODUCT, BRANCH

11. BT.MAST&REPTDAY&REPTMON   (JCL //BT  DD DSN=SAP.PBB.BTRADE.SASDATA)
    File : INPUT_BT_MAST_FILE     -> mast_{REPTDAY}{REPTMON}.sas7bdat
12. BT.MAST2&REPTDAY&REPTMON
    File : INPUT_BT_MAST2_FILE    -> mast2_{REPTDAY}{REPTMON}.sas7bdat
13. IBT.IMAST&REPTDAY&REPTMON (JCL //IBT DD DSN=SAP.PIBB.BTRADE.SASDATA)
    File : INPUT_IBT_IMAST_FILE   -> imast_{REPTDAY}{REPTMON}.sas7bdat
14. IBT.IMAST2&REPTDAY&REPTMON
    File : INPUT_IBT_IMAST2_FILE  -> imast2_{REPTDAY}{REPTMON}.sas7bdat
    Cols used (12-15): ACCTNOX, FICODE
    Deterministic filenames (fully derived from REPTDAY/REPTMON tokens)
    -> constructed directly, input_date.get_latest_file() NOT used.

15. RENTAS.RENTAS&REPTMON&NOWK&REPTYEAR (JCL //RENTAS DD DSN=SAP.PBB.RENTASWH)
    Deterministic filename (fully derived from REPTMON/NOWK/REPTYEAR
    tokens) -> constructed directly, input_date.get_latest_file() NOT used.
    File : INPUT_RENTAS_FILE -> rentas_{REPTMON}{NOWK}{REPTYEAR}.sas7bdat
    Cols used : BANKNO, ISTTYPE, VALUEDTE, TRANSREF, UMRNO, BRANCHABB,
                TTIMESTAMP, PTIMESTAMP, AMOUNT, PAYMODE, STATUS,
                APPLNAME, APPLNAME2, BENENAME, BENENAME2, USERID,
                APPLID, TRACKCODE, APPLACCTNO, BENEACCTNO, ACCTNO

16. CIS.CUSTDLY (JCL //CIS DD DSN=RBP2.B033.CIS.CUST.DAILY)
    File : INPUT_CIS_CUSTDLY_FILE -> cis_custdly.sas7bdat
    Cols used : ACCTCODE, ACCTNO, CUSTNO, ALIAS, PRISEC

============================================================================
OUTPUT
============================================================================
//RENTRAN DD DSN=SAP.AML.DETICA.REMTRAN.RENTAS.TEXT, DISP=OLD
Fixed catalogued name (no date token) -> static output filename.
Pipe-delimited (delimiter = hex '1D'X, ASCII 0x1D Group Separator) flat
file, 75 fields per record, most fields blank. No ASA control byte (this
is a data feed, not a report).
File : OUTPUT_FILE -> EIDETRTS_RENTAS_<ts>.txt (encoding='latin1' so the
0x1D delimiter byte round-trips safely)

//DELETE (PGM=IEFBR14) removes any pre-existing backup at job start;
//COPYFILE (PGM=ICEGENER) backs the interface file up to .TEXT.BKP at job
end. Both are reproduced as file operations below.

============================================================================
PRESERVED SAS QUIRKS
============================================================================
- COMPRESS(x,chars,'A') removes the listed punctuation/space characters
  AND all alphabetic characters, leaving only digits (and any character
  not in the explicit list that also is not a letter) -- same convention
  established in EIDETFRM.py's temp_acct derivation.
- IFC(x=:'0', SUBSTR(x,VERIFY(x,'0')), x) strips leading zeros only when
  the string starts with '0'; if the string is entirely zeros, VERIFY
  returns 0 and SAS's SUBSTR(x,0) is treated as SUBSTR(x,1) (a NOTE, not
  an error) -- reproduced as returning the string unchanged in that
  degenerate case.
- The OUT data step here has NO "remove ending @" logic and NO INDORG
  corporate-pass logic (unlike EIDETLRM.py) -- neither exists in the
  original EIDETRTS source, so neither is reproduced.
- DATA OUTWARD contains a stray duplicated RUN;RUN; in the original SAS
  source -- a harmless quirk with no behavioural effect, not reproduced.
"""

import gc
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from PBBELF import format_brchrvr, format_brchcd

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_DWH_DIR   = STG_DIR / "from_dwh"
INPUT_LN_DIR    = STG_DIR / "MNILN"
INPUT_BT_DIR    = STG_DIR / "detic2"    
INPUT_DETIC_DIR = STG_DIR / "detic2"

INPUT_LN_LNNOTE_FILE   = INPUT_LN_DIR / "enrh_ln_note_d08.sas7bdat"
# INPUT_ILN_LNNOTE_FILE  = INPUT_LN_DIR / "enrh_iln_note_d08.sas7bdat"

INPUT_DP_CURRENT_FILE  = INPUT_DWH_DIR / "ca09126.sas7bdat"
INPUT_IDP_CURRENT_FILE = INPUT_DWH_DIR / "ica09126.sas7bdat"
INPUT_DP_SAVING_FILE   = INPUT_DWH_DIR / "sa09126.sas7bdat"
INPUT_IDP_SAVING_FILE  = INPUT_DWH_DIR / "isa09126.sas7bdat"
INPUT_DP_FD_FILE       = INPUT_DWH_DIR / "fd09126.sas7bdat"
INPUT_IDP_FD_FILE      = INPUT_DWH_DIR / "ifd09126.sas7bdat"
INPUT_DP_UMA_FILE      = INPUT_DETIC_DIR / "uma.sas7bdat"
INPUT_IDP_UMA_FILE     = INPUT_DETIC_DIR / "iuma.sas7bdat"
INPUT_DP_VOSTRO_FILE   = INPUT_DETIC_DIR / "vostro08426.sas7bdat"

INPUT_CIS_CUSTDLY_FILE = STG_DIR / "custdly.sas7bdat"

CACHE_DIR = BASE_DIR / "input" / "cache" / "detic3"
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
    convention used by EIDETFRM.py / EIDETLRM.py: the report date is the
    calendar day prior to the run date.
    """
    return date.today() - timedelta(days=1)


reptdate = get_reptdate()

# WK / NOWK: exact-day match (8/15/22/else 4) -- SELECT(DAY(REPTDATE))
# equivalent. WK1 is SYMPUT'd in the original SAS but never referenced
# again anywhere else in the program -- a dead symbolic variable, not
# reproduced here.
_day = reptdate.day
NOWK = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"

REPTYEAR = reptdate.strftime("%y")     # PUT(REPTDATE,YEAR2.)
REPTMON  = reptdate.strftime("%m")     # PUT(MM,Z2.)
REPTDAY  = reptdate.strftime("%d")     # PUT(DAY(REPTDATE),Z2.)
RDATE    = reptdate.strftime("%Y%m%d") # PUT(REPTDATE,YYMMDDN8.)

ts = reptdate.strftime("%y%m%d")

# YYYY is SYMPUT'd in the original SAS but never referenced again anywhere
# else in the program body -- dead symbolic variable, kept only for
# documentation parity.
YYYY = reptdate.strftime("%Y")

print(f"  REPTDATE : {reptdate}   NOWK: {NOWK}   REPTMON: {REPTMON}   REPTDAY: {REPTDAY}")
print(f"  REPTYEAR : {REPTYEAR}   RDATE: {RDATE}")

# INPUT_RENTAS_FILE     = INPUT_DWH_DIR / f"rentas{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
# INPUT_BT_MAST_FILE    = INPUT_BT_DIR  / f"mast{REPTDAY}{REPTMON}.sas7bdat"
# INPUT_BT_MAST2_FILE   = INPUT_BT_DIR  / f"mast2{REPTDAY}{REPTMON}.sas7bdat"
# INPUT_IBT_IMAST_FILE  = INPUT_BT_DIR  / f"imast{REPTDAY}{REPTMON}.sas7bdat"
# INPUT_IBT_IMAST2_FILE = INPUT_BT_DIR  / f"imast2{REPTDAY}{REPTMON}.sas7bdat"

INPUT_RENTAS_FILE     = INPUT_DWH_DIR / f"rentas09126.sas7bdat"
INPUT_BT_MAST_FILE    = INPUT_BT_DIR  / f"mast0809.sas7bdat"
INPUT_BT_MAST2_FILE   = INPUT_BT_DIR  / f"mast20809.sas7bdat"
INPUT_IBT_IMAST_FILE  = INPUT_BT_DIR  / f"imast0809.sas7bdat"
INPUT_IBT_IMAST2_FILE = INPUT_BT_DIR  / f"imast20809.sas7bdat"

OUTPUT_DIR    = BASE_DIR / "output" / "EIDETRTS"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE   = OUTPUT_DIR / f"EIDETRTS_RENTAS_{ts}.txt"
OUTPUT_BACKUP = OUTPUT_DIR / f"EIDETRTS_RENTAS_{ts}_BKP.txt"

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


def _in_exclude_list(value) -> bool:
    return value is not None and str(value).strip().upper() in COMPANY_EXCLUDE_LIST


# ============================================================================
# HELPERS: SAS COMPRESS/IFC/VERIFY emulation
# ============================================================================
def _compress_digits_only(s: Optional[str], extra_strip: str = "") -> str:
    """COMPRESS(source, extra_strip, 'A'): removes the explicit
    extra_strip characters AND all alphabetic characters, leaving only
    digits (and any other non-letter character not in extra_strip)."""
    if not s:
        return ""
    out = []
    for ch in s:
        if ch.isalpha():
            continue
        if ch in extra_strip:
            continue
        out.append(ch)
    return "".join(out)


def _strip_leading_zeros_if_starts_with_zero(s: str) -> str:
    """IFC(x=:'0', SUBSTR(x,VERIFY(x,'0')), x). VERIFY(x,'0') returns the
    1-based position of the first character in x that is NOT '0'; if x is
    entirely zeros, VERIFY returns 0 and SAS's SUBSTR(x,0) is treated as
    SUBSTR(x,1) (a NOTE, not an error) -- preserved here as returning the
    string unchanged in that degenerate case."""
    if not s or not s.startswith("0"):
        return s
    for i, ch in enumerate(s):
        if ch != "0":
            return s[i:]
    return s


def _compress_blanks(s: Optional[str]) -> str:
    """COMPRESS(x) with no args: removes all blanks."""
    if s is None:
        return ""
    return str(s).replace(" ", "")


def _num_to_str(v) -> str:
    """Mirrors SAS implicit numeric-to-character coercion: integral
    floats render without a decimal point, everything else falls back to
    str()."""
    if v is None:
        return ""
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else str(v)
    return str(v)


def _substr_timestamp(ts_val: Optional[str], start: int, length: int) -> str:
    """SAS 1-based SUBSTR emulation for fixed-position timestamp parses."""
    if ts_val is None:
        return ""
    return ts_val[start - 1:start - 1 + length]


# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET  (EIIMRM01.py pattern)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    def _build_schema(df: "pd.DataFrame") -> pa.Schema:
        fields = []
        for col, dtype in df.dtypes.items():
            if dtype == "object":
                pa_type = pa.string()
            elif pd.api.types.is_integer_dtype(dtype):
                pa_type = pa.int64()
            elif pd.api.types.is_float_dtype(dtype):
                pa_type = pa.float64()
            else:
                pa_type = pa.from_numpy_dtype(dtype)
            fields.append(pa.field(col, pa_type))
        return pa.schema(fields)
    
    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if schema is None:
            schema = _build_schema(chunk)
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")

        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer is not None:
        writer.close()
    else:
        # Source file has 0 rows -- the chunked reader yielded nothing.
        # Read once without chunksize to obtain the schema, then write a
        # zero-row parquet so downstream read_parquet() finds the file.
        empty = pd.read_sas(sas_path, encoding="latin1")
        schema = _build_schema(empty)
        empty_table = pa.Table.from_pandas(empty, schema=schema, preserve_index=False)
        pq.write_table(empty_table, cache_path)

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
LN_LNNOTE_CACHE   = _load_cached(INPUT_LN_LNNOTE_FILE, "LN_LNNOTE")
# ILN_LNNOTE_CACHE  = _load_cached(INPUT_ILN_LNNOTE_FILE, "ILN_LNNOTE")
DP_CURRENT_CACHE  = _load_cached(INPUT_DP_CURRENT_FILE, "DP_CURRENT")
IDP_CURRENT_CACHE = _load_cached(INPUT_IDP_CURRENT_FILE, "IDP_CURRENT")
DP_SAVING_CACHE   = _load_cached(INPUT_DP_SAVING_FILE, "DP_SAVING")
IDP_SAVING_CACHE  = _load_cached(INPUT_IDP_SAVING_FILE, "IDP_SAVING")
DP_FD_CACHE       = _load_cached(INPUT_DP_FD_FILE, "DP_FD")
IDP_FD_CACHE      = _load_cached(INPUT_IDP_FD_FILE, "IDP_FD")
DP_UMA_CACHE      = _load_cached(INPUT_DP_UMA_FILE, "DP_UMA")
IDP_UMA_CACHE     = _load_cached(INPUT_IDP_UMA_FILE, "IDP_UMA")
DP_VOSTRO_CACHE   = _load_cached(INPUT_DP_VOSTRO_FILE, "DP_VOSTRO")
BT_MAST_CACHE     = _load_cached(INPUT_BT_MAST_FILE, "BT_MAST")
BT_MAST2_CACHE    = _load_cached(INPUT_BT_MAST2_FILE, "BT_MAST2")
IBT_IMAST_CACHE   = _load_cached(INPUT_IBT_IMAST_FILE, "IBT_IMAST")
IBT_IMAST2_CACHE  = _load_cached(INPUT_IBT_IMAST2_FILE, "IBT_IMAST2")
RENTAS_CACHE      = _load_cached(INPUT_RENTAS_FILE, "RENTAS")
CIS_CUSTDLY_CACHE = _load_cached(INPUT_CIS_CUSTDLY_FILE, "CIS_CUSTDLY")

# ============================================================================
# STEP 3: DATA INWARD OUTWARD BT;  SET RENTAS.RENTAS...; (routing + exclusion)
# ============================================================================
print("\nStep 3: Building INWARD / OUTWARD / BT from RENTAS...")

con = duckdb.connect(database=":memory:")
rentas_pl = con.execute(f"""
    SELECT
        BANKNO, ISTTYPE, VALUEDTE, TRANSREF, UMRNO, BRANCHABB,
        TTIMESTAMP, PTIMESTAMP, AMOUNT, PAYMODE, STATUS,
        APPLNAME, APPLNAME2, BENENAME, BENENAME2, USERID,
        APPLID, TRACKCODE, APPLACCTNO, BENEACCTNO, ACCTNO
    FROM read_parquet('{RENTAS_CACHE.as_posix()}')
""").pl()
con.close()

inward_rows, outward_rows, bt_rows = [], [], []

for r in rentas_pl.iter_rows(named=True):
    # *2017-2058;
    if _in_exclude_list(r["APPLNAME"]) or _in_exclude_list(r["BENENAME"]):
        continue

    row = dict(r)
    row["RUN_TIMESTAMP"] = _compress_blanks(RDATE + "000000")[:14]
    row["SOURCE_TXN_UNIQUE_ID"] = _compress_blanks(
        f"RM{_num_to_str(r['BANKNO'])}{r['ISTTYPE'] or ''}"
        f"{_num_to_str(r['VALUEDTE'])}{r['TRANSREF'] or ''}{_num_to_str(r['UMRNO'])}"
    )
    row["BRANCH_ID"] = format_brchrvr(r["BRANCHABB"])
    row["CURCODE"] = "MYR"
    row["CURBASE"] = "MYR"
    row["ORIGINATION_DATE"] = r["VALUEDTE"]

    tts = r["TTIMESTAMP"] or ""
    yyyy = _substr_timestamp(tts, 1, 4)
    mm   = _substr_timestamp(tts, 6, 2)
    dd   = _substr_timestamp(tts, 9, 2)
    hour = _substr_timestamp(tts, 12, 2)
    minute = _substr_timestamp(tts, 15, 2)
    sec  = _substr_timestamp(tts, 18, 2)
    row["LOCAL_TIMESTAMP"] = (yyyy + mm + dd + hour + minute + sec)[:14]

    pts = r["PTIMESTAMP"] or ""
    p_yyyy = _substr_timestamp(pts, 1, 4)
    p_mm   = _substr_timestamp(pts, 6, 2)
    p_dd   = _substr_timestamp(pts, 9, 2)
    if p_yyyy == "0101":
        row["POSTING_DATE"] = row["LOCAL_TIMESTAMP"][:8]
    else:
        row["POSTING_DATE"] = _compress_blanks(p_yyyy + p_mm + p_dd)

    row["TXN_AMOUNT_ORIG"] = r["AMOUNT"]
    trans_ref_desc_parts = [p for p in (r["PAYMODE"], r["TRANSREF"]) if p not in (None, "")]
    row["TRANS_REF_DESC"] = " ".join(trans_ref_desc_parts)
    row["MENTION"] = r["PAYMODE"]
    row["TXN_STATUS_CODE"] = r["STATUS"]
    row["CHANNEL"] = 999
    orig_name_parts = [p for p in (r["APPLNAME"], r["APPLNAME2"]) if p not in (None, "")]
    row["ORIGINATOR_NAME"] = ",".join(orig_name_parts)
    bene_name_parts = [p for p in (r["BENENAME"], r["BENENAME2"]) if p not in (None, "")]
    row["BENEFICIARY_NAME"] = ",".join(bene_name_parts)
    row["TELLER_ID"] = r["USERID"]
    row["ORIGINATOR_ID"] = r["APPLID"]
    row["BENEFICIARY_ID"] = r["TRACKCODE"]
    row["REMITTANCE_REF_NO"] = r["TRANSREF"]
    row["INCOMING_OUTGOING_TRANS"] = r["STATUS"]
    row["EMPLOYEE_ID"] = 88888

    if r["BRANCHABB"] in ("701", "702", "IKB", "IPJ"):
        row["ORG_UNIT_CODE"] = "PIBBTRSRY"
    else:
        row["ORG_UNIT_CODE"] = "PBBTRSRY"

    isttype, status = r["ISTTYPE"], r["STATUS"]
    if isttype == "RI" and status == "TI":
        inward_rows.append(row)
    elif isttype == "RO" and status == "TO":
        outward_rows.append(row)
    elif isttype == "BT" and status == "TO":
        bt_rows.append(row)

print(f"  INWARD:{len(inward_rows):,}  OUTWARD:{len(outward_rows):,}  BT:{len(bt_rows):,}")

# ============================================================================
# STEP 4: DATA INWARD1 LOANS_INWARD INWARD2;  SET INWARD; (account resolution)
# ============================================================================
print("\nStep 4: Resolving INWARD accounts (INWARD1 / LOANS_INWARD / INWARD2)...")

inward1_rows, loans_inward_rows, inward2_rows = [], [], []

for row in inward_rows:
    row["TXN_CODE"] = "RMT001"
    row["CRDR"] = "C"
    row["BENEFICIARY_BANK"] = row["BRANCHABB"]
    row["INCOMING_OUTGOING_FLG"] = "I"
    row["BENE_BRANCH"] = row["BRANCHABB"]
    row["SENDER_BRANCH"] = "0"

    compress_bene = _compress_digits_only(row["BENEACCTNO"], ",:.()-/ ")
    compress_bene = _strip_leading_zeros_if_starts_with_zero(compress_bene)
    bene_ac_len = len(compress_bene)
    temp_bene_ac = compress_bene[:10]
    row["COMPRESS_BENE"] = compress_bene
    row["BENE_AC_LEN"] = bene_ac_len
    row["TEMP_BENE_AC"] = temp_bene_ac

    acctno_compressed = _compress_blanks(row["ACCTNO"])

    if temp_bene_ac == acctno_compressed:
        first_digit = temp_bene_ac[:1]
        temp_acctcode = "LN" if first_digit in ("2", "8") else "DP"
        row["TEMP_ACCTCODE"] = temp_acctcode
        row["ACCOUNT_SOURCE_UNIQUE_ID"] = _compress_blanks(temp_acctcode + temp_bene_ac)
        inward1_rows.append(row)
    elif temp_bene_ac != acctno_compressed:
        bene_acctno = row["BENEACCTNO"] or ""
        if bene_ac_len == 19 or (bene_ac_len == 15 and bene_acctno[:1] == "2"):
            if compress_bene[4:5] in ("2", "8"):
                row["ACCOUNT_SOURCE_UNIQUE_ID"] = _compress_blanks(
                    "LN" + compress_bene[4:19])
                loans_inward_rows.append(row)
            else:
                inward2_rows.append(row)
        else:
            inward2_rows.append(row)

# DATA INWARD2; SET INWARD2; IF TEMP_BENE_AC NOT IN ('0','') THEN DO ...
for row in inward2_rows:
    temp_bene_ac = row.get("TEMP_BENE_AC", "")
    if temp_bene_ac not in ("0", ""):
        first_digit = temp_bene_ac[:1]
        temp_acctcode = "LN" if first_digit in ("2", "8") else "DP"
        row["TEMP_ACCTCODE"] = temp_acctcode
        row["ACCOUNT_SOURCE_UNIQUE_ID"] = _compress_blanks(temp_acctcode + temp_bene_ac)

print(f"  INWARD1:{len(inward1_rows):,}  LOANS_INWARD:{len(loans_inward_rows):,}  "
      f"INWARD2:{len(inward2_rows):,}")

# ============================================================================
# STEP 5: DATA OUTWARD;  SET OUTWARD; (account resolution)
# ============================================================================
print("\nStep 5: Resolving OUTWARD accounts...")

for row in outward_rows:
    row["TXN_CODE"] = "RMT002"
    row["CRDR"] = "D"
    row["ORIGINATOR_BANK"] = row["BRANCHABB"]
    row["INCOMING_OUTGOING_FLG"] = "O"
    row["BENE_BRANCH"] = "0"
    row["SENDER_BRANCH"] = row["BRANCHABB"]

    compress_appl = _compress_digits_only(row["APPLACCTNO"], ",:.()-/")
    compress_appl = _strip_leading_zeros_if_starts_with_zero(compress_appl)
    appl_ac_len = len(compress_appl)
    row["COMPRESS_APPL"] = compress_appl
    row["APPL_AC_LEN"] = appl_ac_len

    if appl_ac_len == 10:
        applacctno = row["APPLACCTNO"] or ""
        if applacctno[:1] in ("1", "3", "4", "6"):
            temp_acctcode = "DP"
        else:
            temp_acctcode = "LN"
        row["TEMP_ACCTCODE"] = temp_acctcode
        row["ACCOUNT_SOURCE_UNIQUE_ID"] = _compress_blanks(temp_acctcode + applacctno)
    # ELSE USE HARDCODED ACCOUNT NO;  (no assignment -- stays missing)

# ============================================================================
# STEP 6: DATA BT;  SET BT; (account resolution + ACCTNOX derivation)
# ============================================================================
print("\nStep 6: Resolving BT accounts and ACCTNOX...")

for row in bt_rows:
    row["TXN_CODE"] = "RMT002"
    row["CRDR"] = "D"
    row["ORIGINATOR_BANK"] = row["BRANCHABB"]
    row["INCOMING_OUTGOING_FLG"] = "O"
    row["BENE_BRANCH"] = "0"
    row["SENDER_BRANCH"] = row["BRANCHABB"]

    compress_appl = _compress_digits_only(row["APPLACCTNO"], ",:.()-/")
    compress_appl = _strip_leading_zeros_if_starts_with_zero(compress_appl)
    row["COMPRESS_APPL"] = compress_appl
    row["APPL_AC_LEN"] = len(compress_appl)

    # IF APPL_AC_LEN = 10 AND SUBSTR(APPLACCTNO,1,1) ^= '0' THEN DO;
    #    IF SUBSTR(APPLACCTNO,1,1) = '2' THEN TEMP_ACCTCODE='LN';
    #    ACCOUNT_SOURCE_UNIQUE_ID = COMPRESS(TEMP_ACCTCODE||APPLACCTNO);
    # END;   -- commented out in the original SAS, not reproduced.

    applacctno = row["APPLACCTNO"] or ""
    acctnox = None
    if applacctno[:2] == "25" or applacctno[:3] == "285":
        acctnox = applacctno
    row["ACCTNOX"] = acctnox

    row["ACCOUNT_SOURCE_UNIQUE_ID"] = "RMT00001A"
    row["CUSTOMER_SOURCE_UNIQUE_ID"] = "RMT00001C"
    # ELSE USE HARDCODED ACCOUNT NO;  (comment only, no further action)

# PROC SORT DATA=BT; BY ACCTNOX;
bt_rows.sort(key=lambda r: (r["ACCTNOX"] is None, r["ACCTNOX"] or ""))

# ============================================================================
# STEP 7: DATA BTMAST;  SET BT.MAST... BT.MAST2... IBT.IMAST... IBT.IMAST2...
# ============================================================================
print("\nStep 7: Building BTMAST (ACCTNOX -> ACCTBRCH)...")

con = duckdb.connect(database=":memory:")
btmast_pl = con.execute(f"""
    SELECT CAST(ACCTNOX AS VARCHAR) AS ACCTNOX, FICODE
    FROM read_parquet('{BT_MAST_CACHE.as_posix()}')
    UNION ALL
    SELECT CAST(ACCTNOX AS VARCHAR) AS ACCTNOX, FICODE
    FROM read_parquet('{BT_MAST2_CACHE.as_posix()}')
    UNION ALL
    SELECT CAST(ACCTNOX AS VARCHAR) AS ACCTNOX, FICODE
    FROM read_parquet('{IBT_IMAST_CACHE.as_posix()}')
    UNION ALL
    SELECT CAST(ACCTNOX AS VARCHAR) AS ACCTNOX, FICODE
    FROM read_parquet('{IBT_IMAST2_CACHE.as_posix()}')
""").pl()
con.close()

btmast_pl = btmast_pl.filter(
    (pl.col("ACCTNOX").str.slice(0, 2) == "25")
    | (pl.col("ACCTNOX").str.slice(0, 3) == "285")
).with_columns(pl.col("FICODE").alias("ACCTBRCH")).select(["ACCTNOX", "ACCTBRCH"])

# PROC SORT DATA=BTMAST NODUPKEY; BY ACCTNOX;  (first wins, source order preserved)
btmast_pl = btmast_pl.unique(subset=["ACCTNOX"], keep="first")
_btmast_lookup = {
    r["ACCTNOX"]: r["ACCTBRCH"] for r in btmast_pl.iter_rows(named=True)
}
print(f"  BTMAST rows: {len(_btmast_lookup):,}")

# ============================================================================
# STEP 8: DATA BT;  MERGE BT(IN=A) BTMAST(IN=B); BY ACCTNOX; IF A;
# ============================================================================
print("\nStep 8: Merging BTMAST onto BT...")

for row in bt_rows:
    acctbrch = _btmast_lookup.get(row.get("ACCTNOX"))
    if acctbrch is not None:
        row["BRANCH_ID"] = acctbrch
        try:
            acctbrch_int = int(acctbrch)
        except (TypeError, ValueError):
            acctbrch_int = None
        row["SENDER_BRANCH"] = format_brchcd(acctbrch_int) if acctbrch_int is not None else ""

# ============================================================================
# STEP 9: DATA LOAN;  SET LN.LNNOTE ILN.LNNOTE; (LOAN_ACCT / CTR)
# ============================================================================
print("\nStep 9: Building LOAN_ACCT / CTR from LN.LNNOTE + ILN.LNNOTE...")

# con = duckdb.connect(database=":memory:")
# loan_pl = con.execute(f"""
#     SELECT CAST(ACCTNO AS VARCHAR) AS ACCTNO, NOTENO, COSTCTR, LOANTYPE, NTBRCH
#     FROM read_parquet('{LN_LNNOTE_CACHE.as_posix()}')
#     UNION ALL
#     SELECT CAST(ACCTNO AS VARCHAR) AS ACCTNO, NOTENO, COSTCTR, LOANTYPE, NTBRCH
#     FROM read_parquet('{ILN_LNNOTE_CACHE.as_posix()}')
# """).pl()
# con.close()

# con = duckdb.connect(database=":memory:")
# loan_pl = con.execute(f"""
#     SELECT CAST(ACCTNO AS VARCHAR) AS ACCTNO, NOTENO, COSTCTR, LOANTYPE, NTBRCH,
#            CAST(ENTITY_CD AS VARCHAR) AS ENTITY_CD
#     FROM read_parquet('{LN_LNNOTE_CACHE.as_posix()}')
#     WHERE ENTITY_CD IS NOT NULL
# """).pl()
# con.close()

con = duckdb.connect(database=":memory:")
loan_pl = con.execute(f"""
    SELECT CAST(ACCTNO AS VARCHAR) AS ACCTNO,
           NOTENO,
           COSTCTR,
           LOANTYPE,
           NTBRCH,
           CAST(ENTITY_CD AS VARCHAR) AS ENTITY_CD
    FROM read_parquet('{LN_LNNOTE_CACHE.as_posix()}')
""").pl()
con.close()

# ORG_UNIT_CODE derivation from COSTCTR is commented out in the original
# SAS (dead code, same PIBBLN/PBBLN pattern as EIDETFRM.py) -- not
# reproduced.
loan_pl = loan_pl.with_columns([
    pl.lit("LN").alias("MNI_ACCTCODE"),
    ("LN" + pl.col("LOANTYPE").cast(pl.Int64).cast(pl.Utf8).str.zfill(3)).alias("PROD"),
    pl.col("NTBRCH").alias("ACCTBRCH"),
])

# PROC SORT DATA=LOAN OUT=LOAN_ACCT NODUPKEY; BY ACCTNO NOTENO;
loan_acct_pl = loan_pl.sort(["ACCTNO", "NOTENO"]).unique(
    subset=["ACCTNO", "NOTENO"], keep="first")

# PROC SORT DATA=LOAN(DROP=NOTENO) OUT=CTR NODUPKEY; BY ACCTNO;
ctr_pl = (
    loan_pl.drop("NOTENO")
    .sort(["ACCTNO"])
    .unique(subset=["ACCTNO"], keep="first")
    .with_columns(pl.lit(None, dtype=pl.Int64).alias("NOTENO"))
)
print(f"  LOAN_ACCT rows: {loan_acct_pl.height:,}   CTR rows: {ctr_pl.height:,}")

# ============================================================================
# STEP 10: DATA DEPO_ACCT;  SET DP.CURRENT IDP.CURRENT DP.SAVING IDP.SAVING
#                                DP.FD IDP.FD DP.UMA IDP.UMA DP.VOSTRO;
# ============================================================================
print("\nStep 10: Building DEPO_ACCT...")

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

# ORG_UNIT_CODE derivation from COSTCTR is commented out in the original
# SAS -- not reproduced.
depo_acct_pl = depo_acct_pl.with_columns([
    pl.lit("DP").alias("MNI_ACCTCODE"),
    ("DP" + pl.col("PRODUCT").cast(pl.Int64).cast(pl.Utf8).str.zfill(3)).alias("PROD"),
    pl.col("BRANCH").alias("ACCTBRCH"),
    pl.lit(None, dtype=pl.Int64).alias("NOTENO"),
])
print(f"  DEPO_ACCT rows: {depo_acct_pl.height:,}")

# ============================================================================
# STEP 11: DATA ACCT;  SET DEPO_ACCT LOAN_ACCT CTR;
# ============================================================================
print("\nStep 11: Building ACCT...")

acct_cols = ["ACCTNO", "NOTENO", "MNI_ACCTCODE", "PROD", "ACCTBRCH"]
acct_pl = pl.concat(
    [depo_acct_pl.select(acct_cols), loan_acct_pl.select(acct_cols), ctr_pl.select(acct_cols)],
    how="vertical_relaxed",
)

_acct_lookup: dict = {}
for r in acct_pl.iter_rows(named=True):
    acctno = r["ACCTNO"] or ""
    if r["NOTENO"] is not None:
        acct_id = _compress_blanks(
            r["MNI_ACCTCODE"] + acctno + f"{int(r['NOTENO']):05d}")
    else:
        acct_id = _compress_blanks(r["MNI_ACCTCODE"] + acctno)
    _acct_lookup[acct_id] = {
        "BANK_ACC_IND": "Y",
        "MNI_ACCTCODE": r["MNI_ACCTCODE"],
        "PROD": r["PROD"],
        "ACCTBRCH": r["ACCTBRCH"],
    }
print(f"  ACCT rows: {len(_acct_lookup):,}")

# ============================================================================
# STEP 12: DATA INWARD_OUTWARD TRAN.INOUT;  SET INWARD1 LOANS_INWARD
#                                                INWARD2 OUTWARD;
# ============================================================================
print("\nStep 12: Combining INWARD1/LOANS_INWARD/INWARD2/OUTWARD into INWARD_OUTWARD...")

inward_outward_rows = inward1_rows + loans_inward_rows + inward2_rows + outward_rows
print(f"  INWARD_OUTWARD rows: {len(inward_outward_rows):,}")

# ============================================================================
# STEP 13: PROC SQL;  LEFT JOIN INWARD_OUTWARD TO ACCT ON ACCOUNT_SOURCE_UNIQUE_ID
# ============================================================================
print("\nStep 13: Left-joining ACCT onto INWARD_OUTWARD...")

for row in inward_outward_rows:
    key = row.get("ACCOUNT_SOURCE_UNIQUE_ID")
    match = _acct_lookup.get(key) if key else None
    if match is not None:
        row["BANK_ACC_IND"] = match["BANK_ACC_IND"]
        row["MNI_ACCTCODE"] = match["MNI_ACCTCODE"]
        row["PROD"] = match["PROD"]
        row["ACCTBRCH"] = match["ACCTBRCH"]
    else:
        row["BANK_ACC_IND"] = None
        row["MNI_ACCTCODE"] = None
        row["PROD"] = None
        row["ACCTBRCH"] = None

# ============================================================================
# STEP 14: DATA INWARD_OUTWARD;  SET INWARD_OUTWARD; (EBK/CPC branch override)
# ============================================================================
print("\nStep 14: Applying EBK/CPC branch override...")

for row in inward_outward_rows:
    branchabb = row.get("BRANCHABB")
    paymode = row.get("PAYMODE")
    if (branchabb == "EBK" and paymode == "DR A/C") or (
            branchabb == "CPC" and paymode in ("AUTO CR", "INWARD")):
        acctbrch = row.get("ACCTBRCH")
        if acctbrch is not None:
            row["BRANCH_ID"] = acctbrch
            try:
                acctbrch_int = int(acctbrch)
            except (TypeError, ValueError):
                acctbrch_int = None
            if row.get("ISTTYPE") == "RI":
                row["BENE_BRANCH"] = format_brchcd(acctbrch_int) if acctbrch_int is not None else ""
            elif row.get("ISTTYPE") == "RO":
                row["SENDER_BRANCH"] = format_brchcd(acctbrch_int) if acctbrch_int is not None else ""

# ============================================================================
# STEP 15: DATA TRAN.INWARD_OUTWARD;  SET INWARD_OUTWARD(IN=A) BT(IN=B);
# ============================================================================
print("\nStep 15: Combining INWARD_OUTWARD + BT into TRAN.INWARD_OUTWARD...")

for row in inward_outward_rows:
    if row.get("BANK_ACC_IND") != "Y":
        if row.get("INCOMING_OUTGOING_FLG") == "O":
            row["ACCOUNT_SOURCE_UNIQUE_ID"] = "RMT00001A"
            row["CUSTOMER_SOURCE_UNIQUE_ID"] = "RMT00001C"
            row["PROD"] = "RT108"
        else:
            row["ACCOUNT_SOURCE_UNIQUE_ID"] = "RMT00002A"
            row["CUSTOMER_SOURCE_UNIQUE_ID"] = "RMT00002C"
            row["PROD"] = "RT109"
    row["TEMP_ACCT_SOURCE"] = (row.get("ACCOUNT_SOURCE_UNIQUE_ID") or "")[:12]

for row in bt_rows:
    row["PROD"] = "RT108"
    row["TEMP_ACCT_SOURCE"] = (row.get("ACCOUNT_SOURCE_UNIQUE_ID") or "")[:12]

tran_inward_outward_rows = inward_outward_rows + bt_rows
print(f"  TRAN.INWARD_OUTWARD rows: {len(tran_inward_outward_rows):,}")

# ============================================================================
# STEP 16: DATA CIS;  SET CIS.CUSTDLY; WHERE PRISEC=901 AND ACCTCODE IN
#                          ('DP','LN'); PROC SORT NODUPKEY BY ACCTNO;
# ============================================================================
print("\nStep 16: Building CIS...")

con = duckdb.connect(database=":memory:")
cis_pl = con.execute(f"""
    SELECT
        CAST(ACCTCODE AS VARCHAR) AS ACCTCODE,
        CAST(ACCTNO AS VARCHAR)   AS ACCTNO,
        CAST(CUSTNO AS VARCHAR)   AS CUSTNO,
        CAST(ALIAS AS VARCHAR)    AS ALIAS
    FROM read_parquet('{CIS_CUSTDLY_CACHE.as_posix()}')
    WHERE PRISEC = 901 AND ACCTCODE IN ('DP','LN')
""").pl()
con.close()

cis_pl = cis_pl.with_columns([
    (pl.col("ACCTCODE") + pl.col("ACCTNO")).str.replace_all(" ", "")
    .alias("ACCOUNT_SOURCE_UNIQUE_ID"),
    ("CIS" + pl.col("CUSTNO").fill_null("")).str.replace_all(" ", "").alias("CIS"),
])
# PROC SORT DATA=CIS NODUPKEY; BY ACCTNO;  (dedup key is ACCTNO, not
# ACCOUNT_SOURCE_UNIQUE_ID -- preserved exactly as in the original SAS.)
cis_pl = cis_pl.sort("ACCTNO").unique(subset=["ACCTNO"], keep="first")
_cis_lookup = {
    r["ACCOUNT_SOURCE_UNIQUE_ID"]: (r["CIS"], r["ALIAS"])
    for r in cis_pl.iter_rows(named=True)
}
print(f"  CIS rows: {cis_pl.height:,}")

# ============================================================================
# STEP 17: PROC SQL;  LEFT JOIN TRAN.INWARD_OUTWARD TO CIS ON
#                     TEMP_ACCT_SOURCE = CIS.ACCOUNT_SOURCE_UNIQUE_ID
# ============================================================================
print("\nStep 17: Left-joining CIS onto TRAN.INWARD_OUTWARD (RENTAS_CIS)...")

for row in tran_inward_outward_rows:
    key = row.get("TEMP_ACCT_SOURCE")
    hit = _cis_lookup.get(key) if key else None
    if hit is not None:
        row["CIS"], row["ALIAS"] = hit
        row["CIS_ACCTNO"] = key
    else:
        row["CIS"] = None
        row["ALIAS"] = None
        row["CIS_ACCTNO"] = None

# ============================================================================
# STEP 18: DATA TRAN.RENTAS_CIS;  SET TRAN.RENTAS_CIS; (CIS fallback)
# ============================================================================
print("\nStep 18: Applying CIS fallback...")

for row in tran_inward_outward_rows:
    if row.get("CIS"):
        row["CUSTOMER_SOURCE_UNIQUE_ID"] = row["CIS"]
    else:
        if row.get("INCOMING_OUTGOING_FLG") == "O":
            row["CUSTOMER_SOURCE_UNIQUE_ID"] = "RMT00001C"
            row["ACCOUNT_SOURCE_UNIQUE_ID"] = "RMT00001A"
            row["PROD"] = "RT108"
        else:
            row["CUSTOMER_SOURCE_UNIQUE_ID"] = "RMT00002C"
            row["ACCOUNT_SOURCE_UNIQUE_ID"] = "RMT00002A"
            row["PROD"] = "RT109"

# ============================================================================
# STEP 19: DATA OUT;  SET TRAN.RENTAS_CIS; (alias fallback + cleanup + delete)
# ============================================================================
print("\nStep 19: Building OUT and applying alias cleanup...")


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
for row in tran_inward_outward_rows:
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
    ben_id = _strip_double_alias(ben_id)

    row["ORIGINATOR_ID"] = orig_id
    row["BENEFICIARY_ID"] = ben_id

    # 2022-1211 REMOVE BANK TRANSACTIONS
    if orig_id.zfill(20) in _DELETE_IDS:
        continue

    out_rows.append(row)

print(f"  OUT rows: {len(out_rows):,}")

# ============================================================================
# STEP 20: WRITE OUTPUT  (delimited flat file, delimiter = '1D'X)
# ============================================================================
print("\nStep 20: Writing output...")


def _s(row: dict, field: str) -> str:
    v = row.get(field)
    return "" if v is None else str(v).strip()


lines = []
count = 1
for row in out_rows:
    # SOURCE_TXN_UNIQUE_ID recomputed here, overwriting the RM... value
    # assigned earlier in DATA INWARD/OUTWARD/BT.
    source_txn_id = f"RTS{RDATE}{count:010d}"
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
        _s(row, "TRANSREF"),                         # 72
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
    pl.DataFrame(out_rows).write_parquet(TRAN_DIR / "RENTAS_CIS.parquet")
if tran_inward_outward_rows:
    pl.DataFrame(tran_inward_outward_rows).write_parquet(TRAN_DIR / "INOUT.parquet")

# ============================================================================
# STEP 21: BACKUP INTERFACE FILE  (//COPYFILE EXEC PGM=ICEGENER)
# ============================================================================
print("\nStep 21: Backing up interface file...")
shutil.copy2(OUTPUT_FILE, OUTPUT_BACKUP)
print(f"  Backup written : {OUTPUT_BACKUP}")

print("\nEIDETRTS complete.")
