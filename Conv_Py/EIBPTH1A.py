#!/usr/bin/env python3
"""
Program : EIBPTH1A.py
Purpose : JCL orchestrator (EIBMTH1A step) -- builds the weekly/monthly
          RDAL (Related Data Analysis Ledger) BNM submission. Combines a
          weekly-code snapshot (BNM.ALWKM + PBCS.CCLW), a loan/share-margin
          add-on (LOAN.LNNOTE, via PBBLNFMT), and treasury deal roll-ups
          from EIBMSAPC (KAPX) and KALMLIFE (K3FEI), producing two flat
          reports (RDALKM and NSRSKM) and an SFTP hand-off control file.

Dependencies (module-level execution, matching EIBDRBDP.py JCL pattern):
    from REPTDATE import get_monthly_reptdate_values
    from PBBLNFMT import format_lnprod, format_lndenom
    import KALMLIFE      -> KALMLIFE.K3FEI
    import EIBMSAPC      -> EIBMSAPC.KAPX

============================================================================
PHYSICAL INPUTS (each cached/resolved independently)
============================================================================
1. LOAN.LNNOTE       (JCL //LOAN DD DSN=SAP.PBB.MNILN(0))  -- GDG(0), latest
   generation -> non-deterministic filename -> input_date.get_latest_file().
   Cols used: PZIPCODE, LOANTYPE, BALANCE.

2. BNM.ALWKM&REPTMON&NOWK   (LIBNAME BNM "SAP.PBB.D&REPTYEAR")
   Deterministic filename built from REPTMON/NOWK/REPTYEAR -> constructed
   directly (per project convention), NOT via get_latest_file().
   Cols used: ITCODE, AMTIND, AMOUNT (post SAS-side aggregation already;
   summary-level dataset).

3. PBCS.CCLW&REPTMON&NOWK   (LIBNAME PBCS "SAP.PBB.RDAL.PBCS")
   Deterministic filename, constructed directly. Same column shape as #2.

Both #2 and #3 are stacked together (SAS: SET BNM.ALWKM... PBCS.CCLW...).

============================================================================
OUTPUTS
============================================================================
- RDALKM  (JCL //RDALKM DD DSN=SAP.PBB.KAPMNI.RDAL.PBCS, RECFM=FB LRECL=80)
  Fixed catalogued dataset recreated each run (DISP=NEW,CATLG,DELETE) --
  no date token in the name -> fixed output filename.
- NSRSKM  (JCL //NSRSKM DD DSN=SAP.PBB.NSRS.KAPMNI.RDAL.PBCS, RECFM=FB LRECL=80)
  Same: fixed output filename.
- SFTP01  control-file content embeds FDATE (DDMMYYYY, no separators) in the
  target filename text, not in the physical filename of the control file
  itself.

RECFM=FB (not FBA) on both report DDs -> plain fixed-width text, NO ASA
carriage-control byte (per project convention). Records are padded/
truncated to LRECL=80.
"""

from pathlib import Path
from datetime import date, timedelta

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import gc

from REPTDATE import get_monthly_reptdate_values
from PBBLNFMT_AII import format_lnprod, format_lndenom
# from input_date import get_latest_file

import KALMLIFE
import EIBMSAPC

# FLAG-03: EDW_TRANSFORMATION.py (get_sftp_info()) is referenced by project
# convention for SFTP uploads but was not supplied as an attached dependency
# for this conversion. Kept as a commented placeholder import.
# from EDW_TRANSFORMATION import get_sftp_info
# import paramiko

# ============================================================================
# STEP 0: REPORT-DATE / MACRO-VARIABLE CONTEXT
# (Paths below depend on REPTMON/NOWK, so this must run before path setup.)
# ============================================================================


def _derive_context() -> dict:
    monthly = get_monthly_reptdate_values(year_format="%Y")
    reptdate = monthly.reptdate  # last day of previous month

    day_of_month = reptdate.day
    # SELECT(DAY(REPTDATE)): REPTDATE is always a month-end date (28-31), so
    # WHEN(8)/WHEN(15)/WHEN(22) never fire in practice -- OTHERWISE always
    # applies. Dead branches preserved verbatim below.
    if day_of_month == 8:
        sdd, wk = 1, "1"
    elif day_of_month == 15:
        sdd, wk = 9, "2"
    elif day_of_month == 22:
        sdd, wk = 16, "3"
    else:
        sdd, wk = 23, "4"

    mm = reptdate.month
    if wk == "1":
        mm1 = mm - 1 if mm - 1 != 0 else 12
    else:
        mm1 = mm
    mm2 = mm - 1 if mm - 1 != 0 else 12

    return {
        "reptdate": reptdate,
        "reptyear": reptdate.strftime("%Y"),
        "reptyr": reptdate.strftime("%y"),
        "reptmon": reptdate.strftime("%m"),
        "reptmon1": f"{mm1:02d}",
        "reptmon2": f"{mm2:02d}",
        "reptday": f"{day_of_month:02d}",
        "rdate": reptdate.strftime("%d/%m/%y"),
        "fdate": reptdate.strftime("%d%m%Y"),
        "tdate": reptdate,
        "sdate": date(reptdate.year, mm, sdd),
        "sdesc": "PUBLIC BANK BERHAD",
        "nowk": wk,
        # BUG (preserved): the original SAS hardcodes NOWK1/2/3 to constants
        # '1'/'2'/'3' regardless of the computed WK1/WK2/WK3 values --
        # CALL SYMPUT('NOWK1',PUT('1',$1.)); etc. Neither NOWK1/2/3 nor
        # WK1/WK2/WK3 are referenced anywhere else in the visible program,
        # so this only affects dead macro-variable documentation, not output.
        "nowk1": "1",
        "nowk2": "2",
        "nowk3": "3",
    }

# Generate time stamp
reptdate = date.today() - timedelta(days=1)
ts = reptdate.strftime("%y%m%d")


_CTX = _derive_context()
REPTDATE = _CTX["reptdate"]
REPTYEAR = _CTX["reptyear"]
REPTMON = _CTX["reptmon"]
REPTDAY = _CTX["reptday"]
NOWK = _CTX["nowk"]
RDATE = _CTX["rdate"]
FDATE = _CTX["fdate"]

print("EIBPTH1A: Deriving report-date context...")
print(f"  REPTDATE : {REPTDATE.isoformat()}   RDATE : {RDATE}   FDATE : {FDATE}")
print(f"  REPTMON  : {REPTMON}   NOWK : {NOWK}   REPTYEAR : {REPTYEAR}")

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR = Path("/stgsrcsys/host/uat/AII")

INPUT_LNNOTE_DIR  = STG_DIR / "sasdata"
INPUT_LNNOTE_FILE = "enrh_ln_note_m08.sas7bdat"

INPUT_ALWKM_DIR  = STG_DIR / "EIBPTH1A"
# INPUT_ALWKM_FILE = INPUT_ALWKM_DIR / f"alwkm{REPTMON}{NOWK}.sas7bdat"
INPUT_ALWKM_FILE = INPUT_ALWKM_DIR / "alwkm084.sas7bdat"

INPUT_CCLW_DIR  = BASE_DIR / "EIBPTH1A"
# INPUT_CCLW_FILE = INPUT_CCLW_DIR / f"cclw{REPTMON}{NOWK}.sas7bdat"
INPUT_CCLW_FILE = INPUT_CCLW_DIR / "cclw084.sas7bdat"

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBPTH1A"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIBPTH1A"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RDALKM_FILE = OUTPUT_DIR / f"RDALKM_{ts}.txt"
NSRSKM_FILE = OUTPUT_DIR / f"NSRSKM_{ts}.txt"
SFTP01_FILE = OUTPUT_DIR / f"SFTP01_{ts}.txt"

CHUNK_ROWS = 500_000
LRECL = 80

# ============================================================================
# HELPERS: sas7bdat -> parquet cache
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


def _sas_round(x: float) -> float:
    if x >= 0:
        return float(int(x + 0.5))
    return float(-int(-x + 0.5))


def _group_sum_itcode_amtind(rows) -> list:
    """PROC SUMMARY NWAY; CLASS ITCODE AMTIND; VAR AMOUNT; SUM=;"""
    groups: dict = {}
    for r in rows:
        key = (r["ITCODE"], r["AMTIND"])
        groups[key] = (groups.get(key, 0.0) or 0.0) + (r["AMOUNT"] or 0.0)
    out = [{"ITCODE": k[0], "AMTIND": k[1], "AMOUNT": v} for k, v in groups.items()]
    out.sort(key=lambda r: (r["ITCODE"], r["AMTIND"]))
    return out


# ============================================================================
# STEP 1: CACHE LNNOTE / ALWKM / CCLW INPUTS
# ============================================================================
print("\nStep 1: Caching input SAS datasets to Parquet...")
LNNOTE_CACHE = _load_cached(INPUT_LNNOTE_FILE, "LNNOTE")
ALWKM_CACHE = _load_cached(INPUT_ALWKM_FILE, "ALWKM")
CCLW_CACHE = _load_cached(INPUT_CCLW_FILE, "CCLW")

# ============================================================================
# STEP 2: %WEEKLY -> DATA RDALKM; SET BNM.ALWKM... PBCS.CCLW...;
# Excludes rows whose ITCODE(1:5) falls within any of 4 code ranges.
# ============================================================================
print("\nStep 2: Building RDALKM base (ALWKM + CCLW, range-excluded)...")

_EXCLUDE_RANGES = [
    ("30221", "30228"),
    ("30231", "30238"),
    ("30091", "30098"),
    ("40151", "40158"),
]

con = duckdb.connect(database=":memory:")
alwkm_cclw = con.execute(f"""
    SELECT CAST(ITCODE AS VARCHAR) AS ITCODE,
           CAST(AMTIND AS VARCHAR) AS AMTIND,
           CAST(AMOUNT AS DOUBLE)  AS AMOUNT
    FROM read_parquet('{ALWKM_CACHE.as_posix()}')
    UNION ALL
    SELECT CAST(ITCODE AS VARCHAR) AS ITCODE,
           CAST(AMTIND AS VARCHAR) AS AMTIND,
           CAST(AMOUNT AS DOUBLE)  AS AMOUNT
    FROM read_parquet('{CCLW_CACHE.as_posix()}')
""").pl()
con.close()


def _in_excluded_range(itcode: str) -> bool:
    prefix5 = itcode[0:5]
    return any(lo <= prefix5 <= hi for lo, hi in _EXCLUDE_RANGES)


rdalkm_base = [
    r for r in alwkm_cclw.iter_rows(named=True) if not _in_excluded_range(r["ITCODE"])
]
print(f"  RDALKM base rows after range exclusion: {len(rdalkm_base):,}")

# ============================================================================
# STEP 3: DATA CAG; SET LOAN.LNNOTE; (PZIPCODE filter) -> PROC SUMMARY
# ============================================================================
print("\nStep 3: Building CAG from LOAN.LNNOTE (PZIPCODE filter)...")

_CAG_ZIPCODES = (
    2002, 2013, 3039, 3047, 800003098, 800003114, 800004016, 800004022,
    800004029, 800040050, 800040053, 800050024, 800060024, 800060045,
    800060081, 80060085,
)

con = duckdb.connect(database=":memory:")
zip_list_sql = ",".join(str(z) for z in _CAG_ZIPCODES)
lnnote_raw = con.execute(f"""
    SELECT CAST(LOANTYPE AS INTEGER) AS LOANTYPE,
           CAST(BALANCE  AS DOUBLE)  AS BALANCE
    FROM read_parquet('{LNNOTE_CACHE.as_posix()}')
    WHERE CAST(PZIPCODE AS BIGINT) IN ({zip_list_sql})
""").pl()
con.close()

cag_rows = []
for r in lnnote_raw.iter_rows(named=True):
    loantype = r["LOANTYPE"]
    # PRODCD computed (PUT(LOANTYPE,LNPROD.)) but never used in the
    # subsequent CLASS/summary -- dead column, computed only for parity.
    _prodcd = format_lnprod(loantype)
    amtind = format_lndenom(loantype)
    cag_rows.append({"ITCODE": "7511100000000Y", "AMTIND": amtind, "AMOUNT": r["BALANCE"]})

cag_summary = _group_sum_itcode_amtind(cag_rows)
print(f"  CAG summary rows: {len(cag_summary):,}")

# ============================================================================
# STEP 4: DATA RDALKM; SET RDALKM CAG; PROC SORT BY ITCODE AMTIND;
# ============================================================================
rdalkm_combined = rdalkm_base + cag_summary
rdalkm_combined.sort(key=lambda r: (r["ITCODE"], r["AMTIND"]))

# ============================================================================
# STEP 5: DATA AL OB SP; SET RDALKM; ... (split #1, pre '#'->'Y' transform)
# ============================================================================


def _split_al_ob_sp(rows):
    """
    WHERE SUBSTR(ITCODE,14,1) NOT IN ('F','#');
    IF AMTIND ^= ' ' THEN DO;
       IF SUBSTR(ITCODE,1,3) IN ('307') THEN OUTPUT SP;
       ELSE IF SUBSTR(ITCODE,1,1) ^= '5' THEN DO;
          IF SUBSTR(ITCODE,1,3) IN ('685','785') THEN OUTPUT SP;
          ELSE OUTPUT AL;
       END;
       ELSE OUTPUT OB; END;
    ELSE IF SUBSTR(ITCODE,2,1)='0' THEN OUTPUT SP;
    """
    al, ob, sp = [], [], []
    for r in rows:
        itcode = r["ITCODE"]
        if len(itcode) < 14 or itcode[13] in ("F", "#"):
            continue
        amtind = r["AMTIND"]
        if amtind and amtind != " ":
            if itcode[0:3] == "307":
                sp.append(r)
            elif itcode[0:1] != "5":
                if itcode[0:3] in ("685", "785"):
                    sp.append(r)
                else:
                    al.append(r)
            else:
                ob.append(r)
        else:
            if len(itcode) >= 2 and itcode[1:2] == "0":
                sp.append(r)
    return al, ob, sp


print("\nStep 5: Splitting RDALKM (pre-transform) into AL / OB / SP...")
al_1, ob_1, sp_1 = _split_al_ob_sp(rdalkm_combined)
print(f"  AL: {len(al_1):,}   OB: {len(ob_1):,}   SP: {len(sp_1):,}")

# ============================================================================
# STEP 6: Emit AL / OB / SP group lines (RDALKM report rounding rules)
# ============================================================================


def _emit_al_ob(rows, round_fn):
    """
    RETAIN AMOUNTD AMOUNTI AMOUNTF;
    <contribution accumulated per AMTIND bucket>
    IF LAST.ITCODE THEN DO;
       AMOUNTD=AMOUNTD+AMOUNTI+AMOUNTF;
       PUT ITCODE;AMOUNTD;AMOUNTI;AMOUNTF;
       reset;
    END;
    Rows assumed pre-sorted BY ITCODE AMTIND.
    """
    lines = []
    acc = {"D": 0.0, "I": 0.0, "F": 0.0}
    n = len(rows)
    for idx, r in enumerate(rows):
        itcode, amtind = r["ITCODE"], r["AMTIND"]
        contrib = round_fn(r["AMOUNT"], itcode)
        if amtind in acc:
            acc[amtind] += contrib
        is_last = (idx == n - 1) or (rows[idx + 1]["ITCODE"] != itcode)
        if is_last:
            total = acc["D"] + acc["I"] + acc["F"]
            lines.append(f"{itcode};{int(total)};{int(acc['I'])};{int(acc['F'])}")
            acc = {"D": 0.0, "I": 0.0, "F": 0.0}
    return lines


def _emit_sp(rows, round_fn, dead_recompute=False):
    """SP block only has D/F buckets (no I)."""
    lines = []
    acc = {"D": 0.0, "F": 0.0}
    n = len(rows)
    for idx, r in enumerate(rows):
        itcode, amtind = r["ITCODE"], r["AMTIND"]
        contrib = round_fn(r["AMOUNT"], itcode)
        if amtind in acc:
            acc[amtind] += contrib
        is_last = (idx == n - 1) or (rows[idx + 1]["ITCODE"] != itcode)
        if is_last:
            total = acc["D"] + acc["F"]
            # NSRSKM's SP block computes a further-scaled AMOUNT for '80'-
            # prefixed ITCODEs at this point but never actually prints it
            # (PUT still writes AMOUNTD, not AMOUNT) -- dead recompute,
            # preserved only as a no-op comment per source fidelity.
            if dead_recompute and itcode[0:2] == "80":
                _dead_amount = _sas_round(total / 1000)  # noqa: F841 (unused, matches SAS bug)
            lines.append(f"{itcode};{int(total)};{int(acc['F'])}")
            acc = {"D": 0.0, "F": 0.0}
    return lines


def _rdalkm_round(amount, _itcode):
    return _sas_round((amount or 0.0) / 1000)


print("\nStep 6: Emitting RDALKM report lines...")

phead = f"RDAL{REPTDAY}{REPTMON}{REPTYEAR}"
rdalkm_lines = [phead, "AL"]
rdalkm_lines += _emit_al_ob(al_1, _rdalkm_round)
rdalkm_lines.append("OB")
rdalkm_lines += _emit_al_ob(ob_1, _rdalkm_round)

# DATA SP; SET SP K3FEI KAPX; PROC SORT; BY ITCODE;
sp_1_combined = sp_1 + KALMLIFE.K3FEI.to_dicts() + EIBMSAPC.KAPX.to_dicts()
sp_1_combined.sort(key=lambda r: r["ITCODE"])
rdalkm_lines.append("SP")
rdalkm_lines += _emit_sp(sp_1_combined, _rdalkm_round)

with open(RDALKM_FILE, "w", encoding="latin1") as fh:
    for ln in rdalkm_lines:
        fh.write(ln.ljust(LRECL)[:LRECL] + "\n")

print(f"  RDALKM lines written: {len(rdalkm_lines):,} -> {RDALKM_FILE}")

# ============================================================================
# STEP 7: DATA RDALKM; SET RDALKM; '#'->'Y' sign-flip normalisation, then
#         re-summarise (PROC SUMMARY NWAY; CLASS ITCODE AMTIND; SUM=;)
# ============================================================================
print("\nStep 7: Applying '#'->'Y' normalisation and re-summarising...")

normalised_rows = []
for r in rdalkm_combined:
    itcode, amount = r["ITCODE"], r["AMOUNT"]
    if len(itcode) >= 14 and itcode[13] == "#":
        itcode = itcode[:13] + "Y"
        amount = (amount or 0.0) * -1
    normalised_rows.append({"ITCODE": itcode, "AMTIND": r["AMTIND"], "AMOUNT": amount})

rdalkm_normalised = _group_sum_itcode_amtind(normalised_rows)

# ============================================================================
# STEP 8: DATA AL OB SP; SET RDALKM; ... (split #2, post-transform, for NSRS)
# ============================================================================
print("\nStep 8: Splitting normalised RDALKM into AL / OB / SP (NSRS pass)...")
al_2, ob_2, sp_2 = _split_al_ob_sp(rdalkm_normalised)
print(f"  AL: {len(al_2):,}   OB: {len(ob_2):,}   SP: {len(sp_2):,}")

# ============================================================================
# STEP 9: Emit AL / OB / SP group lines (NSRSKM report rounding rules)
# ============================================================================


def _nsrskm_round_al(amount, itcode):
    amt = _sas_round(amount or 0.0)
    if itcode[0:2] == "80":
        amt = _sas_round(amt / 1000)
    return amt


def _nsrskm_round_ob(amount, itcode):
    amt = amount or 0.0
    if itcode[0:2] == "80":
        amt = _sas_round(amt / 1000)
    return _sas_round(amt)


def _nsrskm_round_sp(amount, _itcode):
    return _sas_round(amount or 0.0)


print("\nStep 9: Emitting NSRSKM report lines...")

nsrskm_lines = [phead, "AL"]
nsrskm_lines += _emit_al_ob(al_2, _nsrskm_round_al)
nsrskm_lines.append("OB")
nsrskm_lines += _emit_al_ob(ob_2, _nsrskm_round_ob)

# DATA SP; SET SP K3FEI KAPX; PROC SORT; BY ITCODE;   (second SET, per source)
sp_2_combined = sp_2 + KALMLIFE.K3FEI.to_dicts() + EIBMSAPC.KAPX.to_dicts()
sp_2_combined.sort(key=lambda r: r["ITCODE"])
nsrskm_lines.append("SP")
nsrskm_lines += _emit_sp(sp_2_combined, _nsrskm_round_sp, dead_recompute=True)

with open(NSRSKM_FILE, "w", encoding="latin1") as fh:
    for ln in nsrskm_lines:
        fh.write(ln.ljust(LRECL)[:LRECL] + "\n")

print(f"  NSRSKM lines written: {len(nsrskm_lines):,} -> {NSRSKM_FILE}")

# ============================================================================
# STEP 10: SFTP01 control file + SFTP hand-off
# ============================================================================
print("\nStep 10: Writing SFTP01 control file...")

sftp_line = (
    f'PUT //SAP.PBB.NSRS.KAPMNI.RDAL.PBCS  kapmni_EAB_PBCS_{FDATE}.txt'
)
with open(SFTP01_FILE, "w", encoding="latin1") as fh:
    fh.write(sftp_line.ljust(LRECL)[:LRECL] + "\n")

print(f"  SFTP01 control file -> {SFTP01_FILE}")

# FLAG-03: actual SFTP transfer (RUNSFTP / COZBATCH step) uses the project's
# EDW_TRANSFORMATION.get_sftp_info(HOST_DESC) + paramiko pattern. HOST_DESC
# for this program is not yet confirmed, so the transfer is left as a
# documented placeholder rather than guessed.
#
# host_info = get_sftp_info(HOST_DESC="<TO_BE_CONFIRMED>")
# with paramiko.Transport((host_info["host"], host_info["port"])) as t:
#     t.connect(username=host_info["user"], password=host_info["password"])
#     sftp = paramiko.SFTPClient.from_transport(t)
#     sftp.put(str(NSRSKM_FILE), f"FD-BNM REPORTING/PBB/BNM RPTG/EAB_PBCS/kapmni_EAB_PBCS_{FDATE}.txt")
#     sftp.close()

# //******************************************************************
# //* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)
# //******************************************************************
# //RUNSFTP  EXEC COZBATCH
# //CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)
# //           DD *
# lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
# lzopts linerule=$lr
# cd "FD-BNM REPORTING/PBB/BNM RPTG/EAB_PBCS"
# //           DD DISP=SHR,DSN=&&FTPPUT
# //           DD *
# EOB
# /*

print("\nEIBPTH1A complete.")
