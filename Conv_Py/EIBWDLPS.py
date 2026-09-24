#!/usr/bin/env python3
"""
Program : EIBWDLPS.py
Purpose : Extraction of SAS customer data for the DLP (Data Loss Prevention)
          system. Consolidates identity/contact data from CRM deposit
          (PBB), CRM deposit-extract (CISBEXT.DP), loan-extract
          (CISBEXT.LN), and card-holder (CRM.CARD) sources, enriches with
          collateral numbers (PBB + PIBB MNICOL), masks sensitive fields,
          and writes a pipe-delimited customer extract split into
          5,000,000-row segments, plus an SFTP put-command list describing
          the segments.
          E-SMR : 2012-1555 (MFM)

Dependency:
    JCL //DELETE step (IEFBR14 + ADRDSSU DUMP DELETE PURGE of prior
    generations of SAP.PBB.DLP.**.TEXT) is pure dataset housekeeping with
    no data-transformation content. It has no Python equivalent here; the
    output directory is simply (re)written fresh on each run.

    BNM DD (DSN=SAP.PBB.MNITB(0)) is only ever used for `SET BNM.REPTDATE;`
    to derive the report-date tokens. No `reptdate.parquet` exists, so
    REPTDATE.py is the source of the report date instead, exactly as in
    the other converted programs in this project.

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
1. CARD.UNICARD&REPTYEAR&REPTMON&NOWK  (JCL //CARD DD DSN=SAP.PBB.CRM.CARD)
   Deterministic member name built directly from REPTYEAR/REPTMON/NOWK
   tokens (YEAR2./Z2./exact-day-match respectively) -- input_date.py's
   get_latest_file() is NOT used since the name is fully derivable, per
   this project's established convention for deterministic filenames.
   Cols used : CARDNO, CUSTNBR, NEWIC, OLDIC, BUSTELNO, HOMTELNO,
               HPHONENO, CLOSECD, RECLASS
   Assumption: physical file named from the SAS member pattern
   "unicard<REPTYEAR><REPTMON><WK>.sas7bdat" under the CRM.CARD staging
   path; not independently verified against a production naming table.

2. CISR.DEPOSIT  (JCL //CISR DD DSN=SAP.PBB.CRM.CISBEXT)
   File : INPUT_CISR_FILE -> crm_cisbext_deposit.sas7bdat
   Cols used : ACCTNO, CUSTNO, NEWIC, OLDIC, PRIPHONE, SECPHONE, MOBIPHON,
               BUSSREG, NEWICIND

3. CISD.DEPOSIT  (JCL //CISD DD DSN=SAP.PBB.CISBEXT.DP)
   File : INPUT_CISD_FILE -> cisbext_dp_deposit.sas7bdat
   Cols used : same as CISR.DEPOSIT

4. CISL.LOAN     (JCL //CISL DD DSN=SAP.PBB.CISBEXT.LN)
   File : INPUT_CISL_FILE -> cisbext_ln_loan.sas7bdat
   Cols used : same as CISR.DEPOSIT

5. COLL.COLLATER  (JCL //COLL DD DSN=SAP.PBB.MNICOL(0))
   File : INPUT_COLL_FILE -> pbb_mnicol_collater.sas7bdat
   Cols used : ACCTNO, CCOLLNO
   Fixed filename (GDG relative-0 "current generation", no date token in
   the member name), matching this project's "fixed output/input filename"
   convention.

6. ICOLL.COLLATER (JCL //ICOLL DD DSN=SAP.PIBB.MNICOL(0))
   File : INPUT_ICOLL_FILE -> pibb_mnicol_collater.sas7bdat
   Cols used : ACCTNO, CCOLLNO

============================================================================
OUTPUT
============================================================================
//DLP  DD DSN=SAP.PBB.DLP           (backing library for DLP.CUSTDATA;
        not itself a report/extract file -- superseded below)
//SFTP DD DSN=SAP.PBB.DLP.SFTP.TEXT, RECFM=FB, LRECL=80
        -> local file  : SFTP_OUTPUT_FILE (EIBWDLPS_SFTP.txt)
Split extract members SAP.PBB.DLP.C<NNN>.TEXT, RECFM=VB, LRECL=450,
one member per 5,000,000-row segment of DLP.CUSTDATA
        -> local files : OUTPUT_DIR / EIBWDLPS_C<NNN>.txt

None of these are printed reports (no titles/page headers/ASA control in
the original JCL -- RECFM=FB/VB, not FBA), so no ASA carriage-control byte
or page-break logic is produced; they are plain pipe-delimited / command
text files.
"""

import gc
import itertools
from math import ceil
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_CARD_DIR  = STG_DIR / "sasdata"
INPUT_CISR_DIR  = STG_DIR / "sasdata"
INPUT_CISD_DIR  = STG_DIR / "sasdata"
INPUT_CISL_DIR  = STG_DIR / "sasdata"
INPUT_COLL_DIR  = STG_DIR / "sasdata"
INPUT_ICOLL_DIR = STG_DIR / "sasdata"

INPUT_CISR_FILE  = INPUT_CISR_DIR / "crm_cisbext_deposit.sas7bdat"
INPUT_CISD_FILE  = INPUT_CISD_DIR / "cisbext_dp_deposit.sas7bdat"
INPUT_CISL_FILE  = INPUT_CISL_DIR / "cisbext_ln_loan.sas7bdat"
INPUT_COLL_FILE  = INPUT_COLL_DIR / "pbb_mnicol_collater.sas7bdat"
INPUT_ICOLL_FILE = INPUT_ICOLL_DIR / "pibb_mnicol_collater.sas7bdat"
# INPUT_CARD_FILE is built below once REPTYEAR/REPTMON/WK are known.

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBWDLPS"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIBWDLPS"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
SFTP_OUTPUT_FILE = OUTPUT_DIR / "EIBWDLPS_SFTP.txt"

CHUNK_ROWS = 500_000
OBSNUM     = 5_000_000   # DATA DLP; OBSNUM = 5000000; (segment size)

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet -- derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate

# DATA DATES; SET BNM.REPTDATE; SELECT(DAY(REPTDATE)); ... WHEN(8/15/22)
# OTHERWISE; -- exact-day matching, distinct from REPTDATE.py's own
# range-based NOWK derivation. Implemented locally, matching the same
# divergence already documented in EIIMRM01.py.
_day = reptdate.day
if _day == 8:
    SDD, WK, WK1 = 1, "01", "04"
elif _day == 15:
    SDD, WK, WK1 = 9, "02", "01"
elif _day == 22:
    SDD, WK, WK1 = 16, "03", "02"
else:
    SDD, WK, WK1 = 23, "04", "03"
# SDD and WK1 (CALL SYMPUT('NOWK1',...)) are computed and SYMPUT'd in the
# original SAS but never referenced again anywhere else in the program --
# both are dead and are kept here only for documentation parity.

REPTYEAR = reptdate.strftime("%y")   # CALL SYMPUT(...,PUT(REPTDATE,YEAR2.))
REPTMON  = reptdate.strftime("%m")   # CALL SYMPUT(...,PUT(MONTH(REPTDATE),Z2.))
REPTDAY  = reptdate.strftime("%d")   # CALL SYMPUT(...,PUT(DAY(REPTDATE),Z2.))
# REPTDAY, like SDD/WK1 above, is SYMPUT'd but never referenced again in
# the program body -- dead, kept for documentation parity only.

CARD_MEMBER = f"UNICARD{REPTYEAR}{REPTMON}{WK}"
INPUT_CARD_FILE = INPUT_CARD_DIR / f"{CARD_MEMBER.lower()}.sas7bdat"

print(f"  REPTYEAR/MON : {REPTYEAR}/{REPTMON}   NOWK: {WK}")
print(f"  CARD member  : {CARD_MEMBER}")

# ============================================================================
# MACRO-CONSTANT EQUIVALENTS
# ============================================================================
# %LET STRING = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ=`&-/\@#*+:().,"%<?>!$?_ ';
# The single character shown here as "<?>" is a non-ASCII/EBCDIC special
# character in the original SAS source listing that could not be reliably
# transcribed; it is preserved as the Unicode replacement character so the
# remainder of the literal set stays faithful to the original.
STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ=`&-/\\@#*+:().,\"%\ufffd!$?_ "
NUMBER = "0123456789"
ZERO   = "0"
FALSIC = (
    "1234567", "12345678", "123456789", "0123456789",
    "SA9999", "SA99999", "999999A", "999999W", "999999X",
)

MASK = "###**###"

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
CARD_CACHE  = _load_cached(INPUT_CARD_FILE, "CARD")
CISR_CACHE  = _load_cached(INPUT_CISR_FILE, "CISR")
CISD_CACHE  = _load_cached(INPUT_CISD_FILE, "CISD")
CISL_CACHE  = _load_cached(INPUT_CISL_FILE, "CISL")
COLL_CACHE  = _load_cached(INPUT_COLL_FILE, "COLL")
ICOLL_CACHE = _load_cached(INPUT_ICOLL_FILE, "ICOLL")

# ============================================================================
# SMALL SAS-SEMANTIC HELPERS
# ============================================================================
def _is_blank(s) -> bool:
    """SAS char IN ('',' ') / missing test."""
    return s is None or str(s).strip() == ""


def _compress_blanks(s) -> str:
    """COMPRESS(var) with no chars arg -- removes blanks only."""
    return (s or "").replace(" ", "")


def _compress_keep(s, keep_chars: str) -> str:
    """COMPRESS(var, chars, 'K') -- keep only characters in `keep_chars`."""
    return "".join(ch for ch in (s or "") if ch in keep_chars)


def _compress_remove(s, remove_chars: str) -> str:
    """COMPRESS(var, chars) -- remove characters that are in `remove_chars`."""
    return "".join(ch for ch in (s or "") if ch not in remove_chars)


def _leading_zero_str(raw) -> str:
    """Emulates: FORMAT PHONEn 10.; PHONEn = <char phone>;  (a numeric
    FORMAT declared before a variable's first assignment fixes its type as
    NUMERIC, per documented SAS behaviour, so this assignment triggers an
    implicit character->numeric conversion) ... PRxPHxN3 = LEFT(PHONEn);
    (numeric->character via the 10. format, then left-justified). The net,
    intentional effect (per the original "/*** REMOVE LEADING ZERO ***/"
    comment) is that leading zeros are stripped from the digit string.
    Only the resulting string's *content* is ever used downstream (for
    LENGTH() and COMPRESS() tests), so trailing-blank padding from the
    LEFT() step is immaterial and omitted here.
    """
    s = (raw or "").strip()
    if s == "":
        return "."   # SAS missing numeric formats to a bare '.'
    try:
        return str(int(s))
    except ValueError:
        return "."


def _sort_key(v):
    """Stable ascending sort key where SAS missing (None/blank) sorts first."""
    return (v is None or str(v).strip() == "", v if v is not None else "")


def _stable_sort(rows: list, field: str) -> list:
    return sorted(rows, key=lambda r: _sort_key(r.get(field)))


# ============================================================================
# STEP 3: CISRM / CISDP / CISLN  (PROC SORT ... KEEP=... BY ACCTNO)
# ============================================================================
print("\nStep 3: Loading + sorting CISR/CISD/CISL sources...")

_CIS_COLS = ["ACCTNO", "CUSTNO", "NEWIC", "OLDIC", "PRIPHONE",
             "SECPHONE", "MOBIPHON", "BUSSREG", "NEWICIND"]


def _load_cis_source(cache_path: Path, tag: str) -> list:
    con = duckdb.connect(database=":memory:")
    df = con.execute(f"""
        SELECT
            CAST(ACCTNO   AS VARCHAR) AS ACCTNO,
            CAST(CUSTNO   AS VARCHAR) AS CUSTNO,
            CAST(NEWIC    AS VARCHAR) AS NEWIC,
            CAST(OLDIC    AS VARCHAR) AS OLDIC,
            CAST(PRIPHONE AS VARCHAR) AS PRIPHONE,
            CAST(SECPHONE AS VARCHAR) AS SECPHONE,
            CAST(MOBIPHON AS VARCHAR) AS MOBIPHON,
            CAST(BUSSREG  AS VARCHAR) AS BUSSREG,
            CAST(NEWICIND AS VARCHAR) AS NEWICIND
        FROM read_parquet('{cache_path.as_posix()}')
    """).pl()
    con.close()
    rows = df.to_dicts()
    rows = _stable_sort(rows, "ACCTNO")
    print(f"  [{tag}] {len(rows):,} rows loaded and sorted by ACCTNO.")
    return rows


cisrm_rows = _load_cis_source(CISR_CACHE, "CISRM")
cisdp_rows = _load_cis_source(CISD_CACHE, "CISDP")
cisln_rows = _load_cis_source(CISL_CACHE, "CISLN")

# ============================================================================
# STEP 4: COLL  (DATA COLL; SET ICOLL.COLLATER COLL.COLLATER; PROC SORT)
# ============================================================================
print("\nStep 4: Building COLL (ICOLL + COLL, sorted by ACCTNO)...")


def _load_coll_source(cache_path: Path, tag: str) -> list:
    con = duckdb.connect(database=":memory:")
    df = con.execute(f"""
        SELECT CAST(ACCTNO AS VARCHAR) AS ACCTNO,
               CAST(CCOLLNO AS VARCHAR) AS CCOLLNO
        FROM read_parquet('{cache_path.as_posix()}')
    """).pl()
    con.close()
    rows = df.to_dicts()
    print(f"  [{tag}] {len(rows):,} rows loaded.")
    return rows


icoll_rows = _load_coll_source(ICOLL_CACHE, "ICOLL")
coll_rows  = _load_coll_source(COLL_CACHE, "COLL")
# SET ICOLL.COLLATER COLL.COLLATER -- Islamic (PIBB) rows stacked first,
# then conventional (PBB) rows, before the stable sort below.
coll_stacked = icoll_rows + coll_rows
coll_sorted  = _stable_sort(coll_stacked, "ACCTNO")
print(f"  COLL combined: {len(coll_sorted):,} rows.")

# ============================================================================
# GENERIC SAS MERGE SIMULATOR  (many-to-many + cross-group "leak" fidelity)
# ============================================================================
def _emit_group(pdv: dict, out: list, key, a_group: list, b_group: list,
                by_key: str, a_cols: list, b_cols: list, keep_only_a: bool) -> None:
    n = max(len(a_group), len(b_group))
    for pos in range(n):
        a_in = pos < len(a_group)
        b_in = pos < len(b_group)
        if a_in:
            pdv.update({c: a_group[pos].get(c) for c in a_cols})
        if b_in:
            pdv.update({c: b_group[pos].get(c) for c in b_cols})
        if keep_only_a and not a_in:
            continue
        row = dict(pdv)
        row[by_key] = key
        out.append(row)


def sas_merge_by_group(a_rows: list, b_rows: list, by_key: str,
                        a_cols: list, b_cols: list, keep_only_a: bool = False) -> list:
    """Emulates `MERGE a(IN=A) b(IN=B); BY by_key;`, preserving its two
    well-known SAS gotchas rather than "fixing" them:
      1. Many-to-many BY groups are paired POSITIONALLY (i-th obs of A
         with i-th obs of B), never cross-joined; once one side runs out
         for the group, its IN= flag reads false for the remaining
         positions (mirrors `keep_only_a` = a trailing `IF A;`).
      2. A variable is NOT reset to missing when its source data set does
         not contribute a fresh observation in the current iteration -- it
         silently retains whatever value was last read for it, even from
         an earlier, unrelated BY group. This cross-group "leak-forward"
         is intentional and preserved verbatim.
    a_rows / b_rows must already be sorted (stably) by `by_key`.
    """
    groups_a = [(k, list(g)) for k, g in itertools.groupby(a_rows, key=lambda r: r[by_key])]
    groups_b = [(k, list(g)) for k, g in itertools.groupby(b_rows, key=lambda r: r[by_key])]

    pdv = {c: None for c in a_cols + b_cols}
    out: list = []
    i = j = 0
    while i < len(groups_a) or j < len(groups_b):
        key_a = groups_a[i][0] if i < len(groups_a) else None
        key_b = groups_b[j][0] if j < len(groups_b) else None
        a_first = i < len(groups_a) and (j >= len(groups_b) or _sort_key(key_a) < _sort_key(key_b))
        b_first = j < len(groups_b) and (i >= len(groups_a) or _sort_key(key_b) < _sort_key(key_a))
        if a_first:
            _emit_group(pdv, out, key_a, groups_a[i][1], [], by_key, a_cols, b_cols, keep_only_a)
            i += 1
        elif b_first:
            _emit_group(pdv, out, key_b, [], groups_b[j][1], by_key, a_cols, b_cols, keep_only_a)
            j += 1
        else:
            _emit_group(pdv, out, key_a, groups_a[i][1], groups_b[j][1], by_key, a_cols, b_cols, keep_only_a)
            i += 1
            j += 1
    return out


# ============================================================================
# STEP 5: CISRM/CISDP/CISLN  MERGE  COLL   (each: IF A; -- keep_only_a=True)
# ============================================================================
print("\nStep 5: Merging CISRM/CISDP/CISLN with COLL (IF A;)...")

_A_COLS = ["CUSTNO", "NEWIC", "OLDIC", "PRIPHONE", "SECPHONE",
           "MOBIPHON", "BUSSREG", "NEWICIND"]
_B_COLS = ["CCOLLNO"]
# FORMAT ACCTNO1/CUSTNO1/COLLNO1 $10. in the original SAS is a display
# format only (no LENGTH statement accompanies it) and has no effect on
# the stored value or subsequent COMPRESS()/comparison logic, so the
# ACCTNO1/CUSTNO1/COLLNO1 -> ACCTNO/CUSTNO/CCOLLNO round-trip is a no-op
# here and the merged fields are carried straight through.

cisrm_merged = sas_merge_by_group(cisrm_rows, coll_sorted, "ACCTNO", _A_COLS, _B_COLS, keep_only_a=True)
cisdp_merged = sas_merge_by_group(cisdp_rows, coll_sorted, "ACCTNO", _A_COLS, _B_COLS, keep_only_a=True)
cisln_merged = sas_merge_by_group(cisln_rows, coll_sorted, "ACCTNO", _A_COLS, _B_COLS, keep_only_a=True)
print(f"  CISRM: {len(cisrm_merged):,}   CISDP: {len(cisdp_merged):,}   CISLN: {len(cisln_merged):,}")

# ============================================================================
# STEP 6: CIS  (SET CISRM CISDP CISLN; default NEWIC/NEWICIND)
# ============================================================================
print("\nStep 6: Stacking CIS and applying NEWIC default...")

cis_rows = cisrm_merged + cisdp_merged + cisln_merged
for r in cis_rows:
    if _is_blank(r.get("NEWIC")):
        r["NEWICIND"] = "OC"
        r["NEWIC"] = r.get("OLDIC")
print(f"  CIS rows: {len(cis_rows):,}")

# ============================================================================
# STEP 7: CCARD  (PROC SORT ... WHERE CLOSECD/RECLASS blank; BY CUSTNBR)
# ============================================================================
print("\nStep 7: Loading + filtering + sorting CARD.UNICARD...")

con = duckdb.connect(database=":memory:")
ccard_df = con.execute(f"""
    SELECT
        CAST(CARDNO   AS VARCHAR) AS CARDNO,
        CAST(CUSTNBR  AS VARCHAR) AS CUSTNBR,
        CAST(NEWIC    AS VARCHAR) AS NEWIC,
        CAST(OLDIC    AS VARCHAR) AS OLDIC,
        CAST(BUSTELNO AS VARCHAR) AS BUSTELNO,
        CAST(HOMTELNO AS VARCHAR) AS HOMTELNO,
        CAST(HPHONENO AS VARCHAR) AS HPHONENO
    FROM read_parquet('{CARD_CACHE.as_posix()}')
    WHERE COALESCE(TRIM(CLOSECD), '') = ''
      AND COALESCE(TRIM(RECLASS), '') = ''
""").pl()
con.close()
ccard_rows = _stable_sort(ccard_df.to_dicts(), "CUSTNBR")
print(f"  CCARD rows (post CLOSECD/RECLASS filter): {len(ccard_rows):,}")

# ============================================================================
# STEP 8: CARD FREQUENCY (&C, &CARD) + TRANSPOSE  (BASECARD padding folded in)
# ============================================================================
print("\nStep 8: Computing card-count width and building TRANPCARD...")

_freqs = [len(list(g)) for _, g in itertools.groupby(ccard_rows, key=lambda r: r["CUSTNBR"])]
_max_freq = max(_freqs) if _freqs else 0
N_CARDS = _max_freq + 2
# &C = max cards-per-customer + 2. DATA BASECARD (a DATA step with no
# SET/MERGE/INPUT, hence one iteration only) declares ARRAY CARD $16.
# CARD1-&C and is immediately subset out by "IF CARD1 NOT IN ('',' ');"
# (CARD1 is uninitialised/blank), so it contributes zero rows -- its only
# real effect is guaranteeing CARD1..CARD&C all exist (2 columns wider
# than the natural transpose maximum) once stacked with TRANPCARD. That
# padding is achieved directly below by padding every row to N_CARDS.
print(f"  Max cards/customer: {_max_freq}  ->  N_CARDS (&C): {N_CARDS}")


def build_tranpcard(rows: list, n_cards: int) -> list:
    """PROC TRANSPOSE ... BY CUSTNBR; VAR CARDNO; COPY NEWIC OLDIC BUSTELNO
    HOMTELNO HPHONENO; -- COPY variables take the FIRST observation's
    value in each BY group, per documented PROC TRANSPOSE behaviour.
    Then: default-NEWIC and RENAME CUSTNBR=CUSTNO HOMTELNO=PRIPHONE
    BUSTELNO=SECPHONE HPHONENO=MOBIPHON.
    `rows` must already be sorted (stably) by CUSTNBR.
    """
    out = []
    for custnbr, group in itertools.groupby(rows, key=lambda r: r["CUSTNBR"]):
        group = list(group)
        first = group[0]
        cards = [g["CARDNO"] for g in group][:n_cards]
        cards += [None] * (n_cards - len(cards))

        newic, oldic = first["NEWIC"], first["OLDIC"]
        if _is_blank(newic):
            newic = oldic

        row = {
            "CUSTNO": custnbr,
            "NEWIC": newic,
            "OLDIC": oldic,
            "PRIPHONE": first["HOMTELNO"],
            "SECPHONE": first["BUSTELNO"],
            "MOBIPHON": first["HPHONENO"],
        }
        for idx, c in enumerate(cards, start=1):
            row[f"CARD{idx}"] = c
        out.append(row)
    return out


tranpcard_rows = build_tranpcard(ccard_rows, N_CARDS)
print(f"  TRANPCARD rows (one per CUSTNBR): {len(tranpcard_rows):,}")

del ccard_rows, ccard_df
gc.collect()

# ============================================================================
# STEP 9: SORT CIS + TRANPCARD BY NEWIC, THEN MERGE (no IF filter)
# ============================================================================
print("\nStep 9: Merging CIS with TRANPCARD by NEWIC...")

cis_sorted = _stable_sort(cis_rows, "NEWIC")
tranp_sorted = _stable_sort(tranpcard_rows, "NEWIC")

_CISCARD_A_COLS = ["ACCTNO", "CUSTNO", "OLDIC", "PRIPHONE", "SECPHONE",
                    "MOBIPHON", "BUSSREG", "NEWICIND", "CCOLLNO"]
_CISCARD_B_COLS = ["CUSTNO", "OLDIC", "PRIPHONE", "SECPHONE", "MOBIPHON"] + \
                   [f"CARD{i}" for i in range(1, N_CARDS + 1)]
# CUSTNO/OLDIC/PRIPHONE/SECPHONE/MOBIPHON exist in BOTH data sets; per
# `MERGE CIS(IN=A) TRANPCARD(IN=B);` (B listed after A), B's value wins
# whenever B contributes freshly in a given iteration -- handled by
# sas_merge_by_group applying b_cols after a_cols, per iteration.
# ACCTNO/BUSSREG/NEWICIND/CCOLLNO exist only in CIS, and CARD1..CARDn
# exist only in TRANPCARD; unmatched-side "leak-forward" across BY groups
# (see sas_merge_by_group docstring) is preserved for both.

ciscard_rows = sas_merge_by_group(
    cis_sorted, tranp_sorted, "NEWIC", _CISCARD_A_COLS, _CISCARD_B_COLS, keep_only_a=False,
)
print(f"  CISCARD rows: {len(ciscard_rows):,}")

del cis_rows, cis_sorted, tranpcard_rows, tranp_sorted
gc.collect()

# ============================================================================
# STEP 10: PER-ROW TRANSFORM PIPELINE  (CISCARD compress -> leading-zero ->
#          blank masking -> NEWIC validation -> phone validation)
# ============================================================================
print("\nStep 10: Applying compress / mask / validation pipeline...")


def _stage_compress(row: dict) -> dict:
    """DATA CISCARD; SET CISCARD; ... COMPRESS(...) assignments."""
    row["NEWIC"] = _compress_blanks(row.get("NEWIC"))
    row["ACCTNO"] = _compress_blanks(row.get("ACCTNO"))
    row["CUSTNO"] = _compress_blanks(row.get("CUSTNO"))
    row["PRIPHONE"] = _compress_keep(row.get("PRIPHONE"), NUMBER)
    row["SECPHONE"] = _compress_keep(row.get("SECPHONE"), NUMBER)
    row["MOBIPHON"] = _compress_keep(row.get("MOBIPHON"), NUMBER)
    row["BUSSREG"] = _compress_blanks(row.get("BUSSREG"))
    row["CCOLLNO"] = _compress_blanks(row.get("CCOLLNO"))
    return row


def _stage_leading_zero(row: dict) -> dict:
    """/*** REMOVE LEADING ZERO ***/ DATA TEMP / CISCARD steps. PR1PH0N3 /
    S3CPH0N3 / M0B1PH0N are dropped from the final output, so only their
    string content (needed for later LENGTH()/COMPRESS() tests) is kept."""
    row["_PR1PH0N3"] = _leading_zero_str(row["PRIPHONE"])
    row["_S3CPH0N3"] = _leading_zero_str(row["SECPHONE"])
    row["_M0B1PH0N"] = _leading_zero_str(row["MOBIPHON"])
    return row


def _stage_mask_blanks(row: dict, n_cards: int) -> dict:
    """ARRAY CARD loop + IN('','.') masking, still inside the CISCARD step."""
    for idx in range(1, n_cards + 1):
        key = f"CARD{idx}"
        if _is_blank(row.get(key)):
            row[key] = MASK
    for key in ("NEWIC", "ACCTNO", "CUSTNO", "PRIPHONE", "SECPHONE",
                "MOBIPHON", "BUSSREG", "CCOLLNO"):
        if row.get(key) in (None, "", "."):
            row[key] = MASK
    return row


def _stage_newic_validate(row: dict) -> dict:
    """DLP.CUSTDATA NEWIC validation cascade -- sequential, each IF tests
    the (possibly already-masked-by-an-earlier-IF) current NEWIC value."""
    newic = row["NEWIC"]
    newicind = row.get("NEWICIND")
    numcheck = _compress_remove(newic, NUMBER)
    strcheck = _compress_remove(newic, STRING)
    zrocheck = _compress_remove(newic, ZERO)

    if newicind == "IC" and len(newic) < 12:
        newic = MASK
    if newicind == "OC" and len(newic) < 7:
        newic = MASK
    if newicind in ("SA", "PC", "BC"):
        newic = MASK
    if numcheck == "" and len(newic) < 6:
        newic = MASK
    if strcheck == "":
        newic = MASK
    if newic in FALSIC:
        newic = MASK
    if len(newic) < 4:
        newic = MASK
    if zrocheck == "":
        newic = MASK

    row["NEWIC"] = newic
    return row


def _stage_phone_validate(row: dict) -> dict:
    """DLP.CUSTDATA phone validation -- LENGTH()/ZRCHKx tests run against
    the pre-masking leading-zero-stripped values, but MASK is applied to
    PRIPHONE/SECPHONE/MOBIPHON (the post-compress digit values), exactly
    mirroring the original SAS variable targets."""
    pr1, s3c, m0b = row["_PR1PH0N3"], row["_S3CPH0N3"], row["_M0B1PH0N"]
    zrchkpri = _compress_remove(pr1, ZERO)
    zrchksec = _compress_remove(s3c, ZERO)
    zrchkmob = _compress_remove(m0b, ZERO)

    priphone, secphone, mobiphon = row["PRIPHONE"], row["SECPHONE"], row["MOBIPHON"]
    if len(pr1) < 8:
        priphone = MASK
    if len(s3c) < 8:
        secphone = MASK
    if len(m0b) < 8:
        mobiphon = MASK
    if zrchkpri == "" or len(zrchkpri) < 4:
        priphone = MASK
    if zrchksec == "" or len(zrchksec) < 4:
        secphone = MASK
    if zrchkmob == "" or len(zrchkmob) < 4:
        mobiphon = MASK

    row["PRIPHONE"], row["SECPHONE"], row["MOBIPHON"] = priphone, secphone, mobiphon
    return row


def process_row(row: dict, n_cards: int) -> dict:
    row = _stage_compress(row)
    row = _stage_leading_zero(row)
    row = _stage_mask_blanks(row, n_cards)
    row = _stage_newic_validate(row)
    row = _stage_phone_validate(row)
    return row


final_rows = [process_row(r, N_CARDS) for r in ciscard_rows]
print(f"  DLP.CUSTDATA rows: {len(final_rows):,}")

del ciscard_rows
gc.collect()

# ============================================================================
# STEP 11: WRITE SPLIT CUSTOMER EXTRACT + SFTP COMMAND LIST
# ============================================================================
print("\nStep 11: Writing split customer extract + SFTP file list...")


def _rtrim(s) -> str:
    return (s or "").rstrip(" ")


def _format_output_line(row: dict, n_cards: int) -> str:
    """PUT @001 NEWIC +(-1)'|' PRIPHONE +(-1)'|' SECPHONE +(-1)'|'
    MOBIPHON +(-1)'|' /* CUSTNO +(-1)'|' */ ACCTNO +(-1)'|'
    /* CCOLLNO +(-1)'|' */ BUSSREG +(-1)'|' @; ARRAY CARD loop.
    The trailing "+(-1)" after each list-style character item backs the
    column pointer over the single auto-inserted trailing blank that SAS
    list-style PUT appends, substituting the literal '|' delimiter in its
    place -- net effect: each field trimmed, pipe-delimited. CUSTNO and
    CCOLLNO are intentionally NOT written (commented out in the source)."""
    fields = [
        _rtrim(row["NEWIC"]),
        _rtrim(row["PRIPHONE"]),
        _rtrim(row["SECPHONE"]),
        _rtrim(row["MOBIPHON"]),
        # CUSTNO omitted from the extract -- commented out in the original
        # SAS PUT statement: /* CUSTNO +(-1)'|' */
        _rtrim(row["ACCTNO"]),
        # CCOLLNO omitted from the extract -- commented out in the
        # original SAS PUT statement: /* CCOLLNO +(-1)'|' */
        _rtrim(row["BUSSREG"]),
    ]
    for idx in range(1, n_cards + 1):
        fields.append(_rtrim(row.get(f"CARD{idx}")))
    return "|".join(fields)


NOBS = len(final_rows)
NUMFILE = ceil(NOBS / OBSNUM) if NOBS else 0
print(f"  NOBS: {NOBS:,}   OBSNUM: {OBSNUM:,}   NUMFILE: {NUMFILE}")

sftp_lines = []
for i in range(1, NUMFILE + 1):
    no = f"{i:03d}"
    start = (i - 1) * OBSNUM
    end = min(i * OBSNUM, NOBS)
    chunk = final_rows[start:end]

    out_path = OUTPUT_DIR / f"EIBWDLPS_C{no}.txt"
    with open(out_path, "w", encoding="latin1") as fh:
        for row in chunk:
            fh.write(_format_output_line(row, N_CARDS) + "\n")
    print(f"  Written {out_path.name} ({len(chunk):,} rows)")

    # DATA _NULL_; FILE SFTP; PUT @001 'PUT' @005 FILES;
    # FILES = CAT("//SAP.PBB.DLP.C",NO,".TEXT","  DLP",NO,".CSV");
    files_str = f"//SAP.PBB.DLP.C{no}.TEXT  DLP{no}.CSV"
    sftp_lines.append(f"PUT {files_str}")

with open(SFTP_OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in sftp_lines:
        fh.write(ln + "\n")
print(f"  Written {SFTP_OUTPUT_FILE.name} ({len(sftp_lines)} lines)")

print("\nEIBWDLPS complete.")
