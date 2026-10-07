#!/usr/bin/env python3
"""
Program : EIIMLIQP.py
Purpose : JCL orchestrator for the New Liquidity Framework (PIBB / Islamic)
          batch (originally JOB EIIMLIQP, EXEC SAS609). Declares every
          physical input dataset, converts each to Parquet (chunked,
          cached), derives the report date once, and drives the child
          programs in the same order as the original SYSIN:
              %INC PGM(DALWPBBD);
              %INC PGM(EIIMRLFM);
              %INC PGM(EIBMTOP5);

          Differences vs EIBMLIQP.py (PBB):
            - Datasets come from the SAP.PIBB.* family (MNITB / MNIFD /
              MNILN / KAPITI / PROVSUB / LNPAYSCH / RNID / SASDATA).
            - No FORATE and no DCIWH DD in this JCL (the PIBB report has
              no DCI section), so neither is declared or cached.
            - PAY member is ILNPAY (not LNPAY).
            - FD11TEXT / FD12TEXT / FD2TEXT are temporary datasets
              (&&INDV / &&CORP / &&SUBS, DISP=DELETE at job end); the
              converted programs still write them as text files.
            - LIBNAME WALK "SAP.PIBB.D&REPTYEAR" is declared in the SAS
              SYSIN but never referenced by any included program, so no
              Python equivalent is needed.
            - //BNMTBL1 //BNMTBL3 are not referenced by name in the SAS
              body (only the BNMK libref is used); no cache needed.

          Every physical dataset is declared and cached ONCE, here, and
          handed down to the child programs as Parquet paths.
"""
import gc
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from DALWPBBD import build_savg_curn_dept
from EIIMRLFM import run_eiimrlfm
from EIBMTOP5 import run_eibmtop5

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR = Path("/stgsrcsys/host/uat/AII")

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIMLIQP"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIIMLIQP"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000

# ---- Physical input datasets (per EIIMLIQP JCL DD statements) -------------
# //DEPOSIT DD DSN=SAP.PIBB.MNITB(0) -- members SAVING / CURRENT / FD
INPUT_SAVING_FILE     = STG_DIR / "EIIMLIQP" / "DEPOSIT" / "saving.sas7bdat"
INPUT_CURRENT_FILE    = STG_DIR / "EIIMLIQP" / "DEPOSIT" / "current.sas7bdat"
INPUT_DEPOSIT_FD_FILE = STG_DIR / "EIIMLIQP" / "DEPOSIT" / "fd.sas7bdat"      # DEPOSIT.FD

# //FD DD DSN=SAP.PIBB.MNIFD(0) -- member FD
INPUT_FD_FD_FILE = STG_DIR / "EIIMLIQP" / "FD" / "fd.sas7bdat"                # FD.FD

# //LOAN DD DSN=SAP.PIBB.MNILN(0) -- member LNCOMM
INPUT_LNCOMM_FILE = STG_DIR / "MNILN" / "PIBB" / "lncomm.sas7bdat"

# //CISLN DD DSN=SAP.PBB.CISBEXT.DP
INPUT_CISLN_DEPOSIT_FILE = STG_DIR / "CIS" / "CISLN" / "deposit.sas7bdat"

# //CISDP DD DSN=SAP.PBB.CRM.CISBEXT
INPUT_CISDP_DEPOSIT_FILE = STG_DIR / "CIS" / "CISDP" / "deposit.sas7bdat"

# //PROVSUB DD DSN=SAP.PIBB.CCRIS.PROVSUB(0) -- flat text file, kept as .txt
INPUT_PROVSUB_FILE = STG_DIR / "EIIMLIQP" / "PROVSUB_INPUT.TXT"

# //BNMK DD DSN=SAP.PIBB.KAPITI.SASDATA -- K1TBL / K3TBL members
INPUT_BNMK_DIR = STG_DIR / "EIIMLIQP" / "BNMK"

# //PAY DD DSN=SAP.PIBB.LNPAYSCH -- ILNPAY members
INPUT_PAY_DIR = STG_DIR / "EIIMLIQP" / "PAY"

# //NID DD DSN=SAP.PIBB.RNID.SASDATA
INPUT_NID_DIR = STG_DIR / "EIIMLIQP" / "NID"

# //BNM1 DD DSN=SAP.PIBB.SASDATA -- LOAN / ULOAN members
INPUT_BNM1_DIR = STG_DIR / "EIIMLIQP" / "BNM1"


# ============================================================================
# SAS7BDAT -> PARQUET CACHE
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


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


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

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
        # 0-row source: chunked reader yielded nothing; write an empty parquet.
        empty = pd.read_sas(sas_path, encoding="latin1")
        schema = _build_schema(empty)
        pq.write_table(pa.Table.from_pandas(empty, schema=schema, preserve_index=False),
                       cache_path)
    print(f"  [{tag}] Done - {total:,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    # Parent folder name is part of the cache name: several inputs share the
    # same member name in different libraries.
    cache_path = CACHE_DIR / f"{sas_path.parent.name}__{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# REPORT DATE CONTEXT
# ============================================================================
def _derive_reptdate_context() -> dict:
    """DATA BNM.REPTDATE; SET DEPOSIT.REPTDATE; SELECT(DAY(REPTDATE)) ...
    No reptdate.parquet exists for this job family -- the date value is
    sourced from REPTDATE.py, NOWK is derived locally with exact-day
    matching (8/15/22/else->4), matching the SAS source exactly."""
    values = get_reptdate_values(year_format="%Y")
    reptdate = values.reptdate

    # DEBUG - Need to remove for production run
    from datetime import date as _date
    reptdate = _date(2026, 9, 30)

    day = reptdate.day
    nowk = "1" if day == 8 else "2" if day == 15 else "3" if day == 22 else "4"
    rd_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if reptdate.year % 4 == 0:
        rd_days[1] = 29
    return {
        "reptdate": reptdate, "tdate": reptdate, "nowk": nowk,
        "reptyear": reptdate.strftime("%Y"), "reptyea2": reptdate.strftime("%y"),
        "reptmon": reptdate.strftime("%m"), "reptday": reptdate.strftime("%d"),
        "rdate": reptdate.strftime("%d/%m/%y"),
        "rpyr": reptdate.year, "rpmth": reptdate.month, "rpday": reptdate.day,
        "rd_days": rd_days,
    }
          

def main() -> None:
    ctx = _derive_reptdate_context()
    print(f"REPTDATE: {ctx['reptdate']}  NOWK: {ctx['nowk']}  RDATE: {ctx['rdate']}")
    reptmon, nowk, reptday, reptyea2 = ctx["reptmon"], ctx["nowk"], ctx["reptday"], ctx["reptyea2"]

    print("\nCaching physical inputs...")
    saving_cache        = _load_cached(INPUT_SAVING_FILE, "SAVING")
    current_cache       = _load_cached(INPUT_CURRENT_FILE, "CURRENT")
    deposit_fd_cache    = _load_cached(INPUT_DEPOSIT_FD_FILE, "DEPOSIT.FD")
    fd_fd_cache         = _load_cached(INPUT_FD_FD_FILE, "FD.FD")
    lncomm_cache        = _load_cached(INPUT_LNCOMM_FILE, "LNCOMM")
    cisln_deposit_cache = _load_cached(INPUT_CISLN_DEPOSIT_FILE, "CISLN.DEPOSIT")
    cisdp_deposit_cache = _load_cached(INPUT_CISDP_DEPOSIT_FILE, "CISDP.DEPOSIT")

    # Members whose names are fully derived from REPTMON / NOWK / REPTDAY / REPTYEA2
    k1tbl_cache      = _load_cached(INPUT_BNMK_DIR / f"k1tbl{reptmon}{nowk}.sas7bdat", "BNMK.K1TBL")
    k3tbl_cache      = _load_cached(INPUT_BNMK_DIR / f"k3tbl{reptmon}{nowk}.sas7bdat", "BNMK.K3TBL")
    lnpay_cache      = _load_cached(INPUT_PAY_DIR / f"ilnpay{reptmon}{nowk}{reptyea2}.sas7bdat", "PAY.ILNPAY")
    nid_rnid_cache   = _load_cached(INPUT_NID_DIR / f"rnid{reptday}.sas7bdat", "NID.RNID")
    bnm1_loan_cache  = _load_cached(INPUT_BNM1_DIR / f"loan{reptmon}{nowk}.sas7bdat", "BNM1.LOAN")
    bnm1_uloan_cache = _load_cached(INPUT_BNM1_DIR / f"uloan{reptmon}{nowk}.sas7bdat", "BNM1.ULOAN")

    provsub_txt_path = INPUT_PROVSUB_FILE  # flat file, no parquet conversion

    # ---- %INC PGM(DALWPBBD) -- build BNM_SAVG / BNM_CURN / BNM_DEPT ------
    print("\nBuilding BNM_SAVG / BNM_CURN / BNM_DEPT via DALWPBBD...")
    dalwpbbd_cache_dir = CACHE_DIR / "DALWPBBD"
    build_savg_curn_dept(
        saving_cache=saving_cache, current_cache=current_cache, cisdp_cache=cisdp_deposit_cache,
        reptmon=reptmon, nowk=nowk, output_cache_dir=dalwpbbd_cache_dir,
    )
    bnm_savg_cache = dalwpbbd_cache_dir / f"SAVG{reptmon}{nowk}.parquet"
    bnm_curn_cache = dalwpbbd_cache_dir / f"CURN{reptmon}{nowk}.parquet"

    # ---- %INC PGM(EIIMRLFM) ------------------------------------------------
    print("\nRunning EIIMRLFM...")
    run_eiimrlfm(
        bnm1_loan_cache=bnm1_loan_cache, bnm1_uloan_cache=bnm1_uloan_cache, lncomm_cache=lncomm_cache,
        provsub_txt_path=provsub_txt_path, lnpay_cache=lnpay_cache, fd_fd_cache=fd_fd_cache,
        bnm_savg_cache=bnm_savg_cache, bnm_curn_cache=bnm_curn_cache,
        deposit_current_cache=current_cache, deposit_fd_cache=deposit_fd_cache,
        nid_rnid_cache=nid_rnid_cache, k1tbl_cache=k1tbl_cache, k3tbl_cache=k3tbl_cache,
        cisln_deposit_cache=cisln_deposit_cache, cisdp_deposit_cache=cisdp_deposit_cache,
        ctx=ctx, output_dir=OUTPUT_DIR,
    )

    # ---- %INC PGM(EIBMTOP5) -------------------------------------------------
    print("\nRunning EIBMTOP5...")
    run_eibmtop5(
        cisln_deposit_cache=cisln_deposit_cache, deposit_current_cache=current_cache,
        cisdp_deposit_cache=cisdp_deposit_cache, deposit_fd_cache=deposit_fd_cache,
        rdate=ctx["rdate"], output_dir=OUTPUT_DIR,
    )

    print("\nEIIMLIQP complete.")


if __name__ == "__main__":
    main()
