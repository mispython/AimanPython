#!/usr/bin/env python3
"""
Program : EIIMLIQP.py
Purpose : JCL orchestrator for the Islamic-book (PIBB) New Liquidity
          Framework / Top-100/50 depositor batch (originally JOB
          EIIMLIQP, EXEC step name EIBMRLFM -- a historical/copy-paste
          step label; the actual %INC calls are DALWPBBD, EIIMRLFM,
          EIBMTOP5). This orchestrator never imports from EIBMLIQP.py or
          EIBMRLFM.py -- EIIMRLFM is its own independent program (see
          EIIMRLFM.py's docstring), and only EIBMTOP5 is genuinely shared
          between the two jobs (confirmed: the JCL's own %INC list ends
          with %INC PGM(EIBMTOP5), not an EIIMTOP5).

              %INC PGM(DALWPBBD);   -- reused unchanged (generic, cache-path driven)
              %INC PGM(EIIMRLFM);   -- Islamic-book variant, fully self-contained
              %INC PGM(EIBMTOP5);   -- same program as the PBB job, pointed at PIBB paths

          Physical inputs mostly move to SAP.PIBB.* (per the JCL DD
          statements), EXCEPT CISLN/CISDP which stay on the shared PBB CIS
          extract (//CISLN DD DSN=SAP.PBB.CISBEXT.DP, //CISDP DD
          DSN=SAP.PBB.CRM.CISBEXT -- unchanged from the PBB job's DD's).

          LIBNAME WALK "SAP.PIBB.D&REPTYEAR" DISP=SHR; is declared in the
          JCL/SYSIN but never referenced by name anywhere in the visible
          EIIMRLFM/EIBMTOP5 bodies -- kept as a comment only.

          There is no reptdate.parquet for this job family; the report
          date is sourced from REPTDATE.py and NOWK is derived locally
          with exact-day matching (8/15/22/else->4), matching the
          original DATA BNM.REPTDATE step.
"""
import gc
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
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
INPUT_SAVING_FILE     = STG_DIR / "PIBB" / "MNITB" / "saving.sas7bdat"
INPUT_CURRENT_FILE    = STG_DIR / "PIBB" / "MNITB" / "current.sas7bdat"
INPUT_DEPOSIT_FD_FILE = STG_DIR / "PIBB" / "MNITB" / "fd.sas7bdat"          # DEPOSIT.FD
# //FD DD DSN=SAP.PIBB.MNIFD(0) -- member FD
INPUT_FD_FD_FILE = STG_DIR / "PIBB" / "MNIFD" / "fd.sas7bdat"              # FD.FD
# //LOAN DD DSN=SAP.PIBB.MNILN(0) -- member LNCOMM
INPUT_LNCOMM_FILE = STG_DIR / "PIBB" / "MNILN" / "lncomm.sas7bdat"
# //CISLN DD DSN=SAP.PBB.CISBEXT.DP -- shared PBB extract (unchanged path)
INPUT_CISLN_DEPOSIT_FILE = STG_DIR / "CISBEXT" / "cisln_deposit.sas7bdat"
# //CISDP DD DSN=SAP.PBB.CRM.CISBEXT -- shared PBB extract (unchanged path)
INPUT_CISDP_DEPOSIT_FILE = STG_DIR / "CRM" / "cisdp_deposit.sas7bdat"
# //BNMTBL1 //BNMTBL3 DD DSN=SAP.PIBB.KAPITI1/KAPITI3 -- not referenced by
# name anywhere in the SAS body (only BNMK. libref is used); no cache needed.
# //PROVSUB DD DSN=SAP.PIBB.CCRIS.PROVSUB(0) -- flat text file, kept as .txt
INPUT_PROVSUB_FILE = STG_DIR / "PIBB" / "CCRIS" / "provsub.txt"
# //BNMK DD DSN=SAP.PIBB.KAPITI.SASDATA -- K1TBL / K3TBL members
INPUT_BNMK_DIR = STG_DIR / "PIBB" / "KAPITI"
# //PAY DD DSN=SAP.PIBB.LNPAYSCH -- PAY.ILNPAY&REPTMON&NOWK&REPTYEA2
INPUT_PAY_DIR = STG_DIR / "PIBB" / "LNPAYSCH"
# //NID DD DSN=SAP.PIBB.RNID.SASDATA
INPUT_NID_DIR = STG_DIR / "PIBB" / "RNID"
# //BNM1 DD DSN=SAP.PIBB.SASDATA -- LOAN / ULOAN members
INPUT_BNM1_DIR = STG_DIR / "PIBB" / "BNM1"


def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return sas_path.exists() and cache_path.exists() and cache_path.stat().st_mtime >= sas_path.stat().st_mtime


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer, schema, total = None, None, 0
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


def _derive_reptdate_context() -> dict:
    """DATA BNM.REPTDATE; SET LOAN.REPTDATE; SELECT(DAY(REPTDATE)) ...
    No reptdate.parquet exists for this job family -- the date value is
    sourced from REPTDATE.py, NOWK is derived locally with exact-day
    matching (8/15/22/else->4), matching the SAS source exactly."""
    values = get_reptdate_values(year_format="%Y")
    reptdate = values.reptdate
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
    saving_cache = _load_cached(INPUT_SAVING_FILE, "SAVING")
    current_cache = _load_cached(INPUT_CURRENT_FILE, "CURRENT")
    deposit_fd_cache = _load_cached(INPUT_DEPOSIT_FD_FILE, "DEPOSIT.FD")
    fd_fd_cache = _load_cached(INPUT_FD_FD_FILE, "FD.FD")
    lncomm_cache = _load_cached(INPUT_LNCOMM_FILE, "LNCOMM")
    cisln_deposit_cache = _load_cached(INPUT_CISLN_DEPOSIT_FILE, "CISLN.DEPOSIT")
    cisdp_deposit_cache = _load_cached(INPUT_CISDP_DEPOSIT_FILE, "CISDP.DEPOSIT")

    k1tbl_cache = _load_cached(INPUT_BNMK_DIR / f"k1tbl{reptmon}{nowk}.sas7bdat", "BNMK.K1TBL")
    k3tbl_cache = _load_cached(INPUT_BNMK_DIR / f"k3tbl{reptmon}{nowk}.sas7bdat", "BNMK.K3TBL")
    ilnpay_cache = _load_cached(INPUT_PAY_DIR / f"ilnpay{reptmon}{nowk}{reptyea2}.sas7bdat", "PAY.ILNPAY")
    nid_rnid_cache = _load_cached(INPUT_NID_DIR / f"rnid{reptday}.sas7bdat", "NID.RNID")
    bnm1_loan_cache = _load_cached(INPUT_BNM1_DIR / f"loan{reptmon}{nowk}.sas7bdat", "BNM1.LOAN")
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

    # ---- %INC PGM(EIIMRLFM) -------------------------------------------------
    print("\nRunning EIIMRLFM...")
    run_eiimrlfm(
        bnm1_loan_cache=bnm1_loan_cache, bnm1_uloan_cache=bnm1_uloan_cache, lncomm_cache=lncomm_cache,
        provsub_txt_path=provsub_txt_path, ilnpay_cache=ilnpay_cache, fd_fd_cache=fd_fd_cache,
        bnm_savg_cache=bnm_savg_cache, bnm_curn_cache=bnm_curn_cache,
        deposit_current_cache=current_cache, deposit_fd_cache=deposit_fd_cache,
        nid_rnid_cache=nid_rnid_cache, k1tbl_cache=k1tbl_cache, k3tbl_cache=k3tbl_cache,
        cisln_deposit_cache=cisln_deposit_cache, cisdp_deposit_cache=cisdp_deposit_cache,
        ctx=ctx, output_dir=OUTPUT_DIR,
    )

    # ---- %INC PGM(EIBMTOP5) -- same program as the PBB job -----------------
    print("\nRunning EIBMTOP5...")
    run_eibmtop5(
        cisln_deposit_cache=cisln_deposit_cache, deposit_current_cache=current_cache,
        cisdp_deposit_cache=cisdp_deposit_cache, deposit_fd_cache=deposit_fd_cache,
        rdate=ctx["rdate"], output_dir=OUTPUT_DIR,
    )

    print("\nEIIMLIQP complete.")


if __name__ == "__main__":
    main()
