#!/usr/bin/env python3
"""
Program : EIBMLIQI.py
Purpose : JCL orchestrator for the Islamic New Liquidity Framework (FISS)
          reporting job. Mirrors the original JCL sequence:
              %INC PGM(DALWPBBD);
              %INC PGM(EIBMRLFI);   (EIBMRLFI drives KALMLIQI)
          This program owns: the report date (ctl_dwh_batch_dttm, SOURCE_SYSTEM_CD
          'SAS'), every physical input path, the one-time conversion of each
          .sas7bdat input into a temporary Parquet file, and the output
          declaration. The converted Parquet files are passed to the %INC
          programs and are deleted when the job ends.

JCL COND=(4,LT) equivalent: any unhandled exception in the job stream is
caught here and results in a non-zero (RC=4) process exit.
"""

import gc
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import DALWPBBD    # %INC PGM(DALWPBBD) equivalent -- build_savg_curn_dept()
import EIBMRLFI    # %INC PGM(EIBMRLFI) equivalent -- main() drives KALMLIQI

# ============================================================
# PATH CONFIGURATION
# ============================================================
BATCH_DATE_CTL_FILE = Path("/sasdata/dwh/control/ctl_dwh_batch_dttm.sas7bdat")
BATCH_SOURCE = "SAS"    # SOURCE_SYSTEM_CD used for the report date

# Production Path
INPUT_DEPOSIT_INTG_DIR = Path("/sas/deposit/dwh/integration")
INPUT_DEPOSIT_ENRH_DIR = Path("/sas/deposit/dwh/enrichment")
INPUT_LNCOMM_DIR       = Path("/sas/ln/dwh/enrichment")
INPUT_LOAN_DIR         = Path("/dwh/dwh_m/tempsource/ln")
INPUT_KAPITI_DIR       = Path("/exdwh/host/kapiti")
INPUT_CISDP_DIR        = Path("/sas/cis/dwh/enrichment")    # CISDP DD (SAP.PBB.CRM.CISBEXT)

OUTPUT_DIR = Path("/host_pq/mis/output/report/EIBMLIQI")

# # Testing Path
# BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
# STG_DIR = Path("/stgsrcsys/host/uat/AII")
#
# INPUT_LOAN_DIR     = STG_DIR / "sasdata"
# INPUT_FD_DIR       = STG_DIR / "from_dwh"
# INPUT_CURRENT_DIR  = STG_DIR / "from_dwh"
# INPUT_LNCOMM_DIR   = STG_DIR / "sasdata"
# INPUT_KAPITI_DIR   = STG_DIR / "sasdata"
#
# INPUT_FD_FILE      = INPUT_FD_DIR      / "fdcd09126.sas7bdat"
# INPUT_CURRENT_FILE = INPUT_CURRENT_DIR / "ca09126.sas7bdat"
# INPUT_LNCOMM_FILE  = INPUT_LNCOMM_DIR  / "enrh_ln_comm_d08.sas7bdat"
# loan_sas = INPUT_LOAN_DIR / f"loan091.sas7bdat"                  # Test file
# k1_sas = INPUT_KAPITI_DIR / f"k1tbl091.sas7bdat"                 # Test file
# k3_sas = INPUT_KAPITI_DIR / f"k3tbl091.sas7bdat"                 # Test file
#
# OUTPUT_DIR = BASE_DIR / "output" / "EIBMLIQI"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TEMP_DIR   = None       # None = system temp; set a path if the default temp area is too small
CHUNK_ROWS = 500_000

# (input tag, exclude ENTITY_CD = 'PIBB' while converting)
CONVERSIONS = [
    ("LOAN",    True),
    ("MAIN_FD", False),   # carries ENTITY_CD + ACCTNO; used only to filter FD
    ("FD",      False),   # no ENTITY_CD column; filtered through MAIN_FD by ACCTNO
    ("SAVING",  True),
    ("CURRENT", True),
    ("CISDP",   False),   # customer-number lookup joined onto already filtered rows
    ("LNCOMM",  True),
    ("K1TBL",   False),   # KAPITI (BNMK) -- no ENTITY_CD filter
    ("K3TBL",   False),   # KAPITI (BNMK) -- no ENTITY_CD filter
]


# ============================================================
# REPORT DATE  (ctl_dwh_batch_dttm, SOURCE_SYSTEM_CD = 'SAS')
# ============================================================
def get_batch_date_dwh(source_system_cd: str) -> str:
    """Return BATCH_DTTM ("YYYY-MM-DD HH:MM:SS") for the given SOURCE_SYSTEM_CD
    from the DWH batch control file. Raises if the file or code is missing."""
    if not BATCH_DATE_CTL_FILE.exists():
        raise FileNotFoundError(
            f"Batch date control file not found: {BATCH_DATE_CTL_FILE}"
        )

    ctl = pd.read_sas(BATCH_DATE_CTL_FILE, format="sas7bdat", encoding="utf-8")
    ctl.columns = [str(c).upper().strip() for c in ctl.columns]

    rows = ctl.loc[ctl["SOURCE_SYSTEM_CD"] == source_system_cd]
    if rows.empty:
        raise RuntimeError(
            f"SOURCE_SYSTEM_CD '{source_system_cd}' not found in {BATCH_DATE_CTL_FILE}"
        )

    batch_dttm = pd.to_datetime(rows.iloc[0]["BATCH_DTTM"])
    return batch_dttm.strftime("%Y-%m-%d %H:%M:%S")


def get_report_date() -> date:
    """REPTDATE equivalent, taken from the batch control file."""
    batch_dt = datetime.strptime(get_batch_date_dwh(BATCH_SOURCE), "%Y-%m-%d %H:%M:%S")
    return batch_dt.date()


# ============================================================
# INPUT FILE RESOLUTION
# ============================================================
def resolve_inputs(reptdate: date) -> dict:
    """Build every physical input path from the report date.
    NOWK uses exact-day matching (8/15/22/else 4), as in SELECT(DAY(REPTDATE))."""
    day_ = reptdate.day
    nowk = "1" if day_ == 8 else "2" if day_ == 15 else "3" if day_ == 22 else "4"
    reptmon = reptdate.strftime("%m")
    reptday = reptdate.strftime("%d")

    inputs = {
        # Carry ENTITY_CD != 'PIBB' and ACCTNO
        "MAIN_FD": INPUT_DEPOSIT_INTG_DIR / f"intg_dp_acct_fd_d{reptday}.sas7bdat",
        # No ENTITY_CD -- merged with MAIN_FD by ACCTNO
        "FD":      INPUT_DEPOSIT_ENRH_DIR / f"enrh_dp_fd_cert_d{reptday}.sas7bdat",
        "CURRENT": INPUT_DEPOSIT_INTG_DIR / f"intg_dp_acct_current_d{reptday}.sas7bdat",
        "SAVING":  INPUT_DEPOSIT_INTG_DIR / f"intg_dp_acct_saving_d{reptday}.sas7bdat",
        "CISDP":   INPUT_CISDP_DIR / "CISDP_deposit.sas7bdat",
        "LNCOMM":  INPUT_LNCOMM_DIR / f"enrh_ln_comm_d{reptday}.sas7bdat",
        "LOAN":    INPUT_LOAN_DIR / f"loan{reptmon}{nowk}.sas7bdat",
        # k1_sas / k3_sas : BNMK.K1TBL&REPTMON&NOWK / BNMK.K3TBL&REPTMON&NOWK
        "K1TBL":   INPUT_KAPITI_DIR / f"k1tbl{reptmon}{nowk}.sas7bdat",
        "K3TBL":   INPUT_KAPITI_DIR / f"k3tbl{reptmon}{nowk}.sas7bdat",
    }

    missing = [str(p) for p in inputs.values() if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Expected input file(s) not found:\n  " + "\n  ".join(missing)
            + f"\nDerived from report date {reptdate} with NOWK={nowk}."
        )
    return inputs


# ============================================================
# STREAM .sas7bdat -> TEMPORARY PARQUET (converted once per run)
# ============================================================
def sas_to_parquet(sas_path: Path, parquet_path: Path, tag: str,
                   exclude_pibb: bool = False) -> None:
    """Convert a .sas7bdat to Parquet in streaming chunks. When exclude_pibb is
    True, rows with ENTITY_CD = 'PIBB' are dropped (missing ENTITY_CD is kept,
    matching SAS missing ^= 'PIBB')."""
    print(f"  [{tag}] Converting {sas_path.name} -> {parquet_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, format="sas7bdat", encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if exclude_pibb:
            entity = chunk["ENTITY_CD"].fillna("").astype(str).str.strip()
            chunk = chunk[entity != "PIBB"]

        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if schema is None:
            # Lock schema on first chunk, but never lock in a 'null' type
            # inferred from a chunk that happened to be blank for a text column.
            fields = []
            for field in table.schema:
                if pa.types.is_null(field.type):
                    fields.append(pa.field(field.name, pa.string()))
                else:
                    fields.append(field)
            schema = pa.schema(fields)
            table = table.cast(schema, safe=False)
            writer = pq.ParquetWriter(parquet_path, schema, compression="snappy")
        else:
            cast_arrays = []
            for field in schema:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        if pa.types.is_string(field.type):
                            col = col.cast(pa.string(), safe=False)
                        else:
                            print(f"  [{tag}] WARNING: Cannot cast '{field.name}' "
                                  f"from {col.type} to {field.type}: {e} — filling nulls")
                            col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)

        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done — {total:,} rows kept.")


# ============================================================
# FD: ATTACH ENTITY_CD THROUGH MAIN_FD (BY ACCTNO) AND KEEP PBB ONLY
# ============================================================
def build_pbb_fd_parquet(main_fd_parquet: Path, fd_parquet: Path, out_parquet: Path) -> None:
    """FD (fixed deposit certificate) has no ENTITY_CD, so ENTITY_CD is looked up
    from MAIN_FD by ACCTNO. MAIN_FD is collapsed to one row per ACCTNO first so
    the join can never multiply FD rows. FD rows of PIBB accounts are dropped."""
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"""
            COPY (
                SELECT f.*
                FROM read_parquet('{fd_parquet.as_posix()}') AS f
                LEFT JOIN (
                    SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO,
                           MAX(CASE WHEN TRIM(COALESCE(CAST(ENTITY_CD AS VARCHAR), '')) = 'PIBB'
                                    THEN 1 ELSE 0 END) AS IS_PIBB
                    FROM read_parquet('{main_fd_parquet.as_posix()}')
                    GROUP BY CAST(ACCTNO AS BIGINT)
                ) AS m
                  ON CAST(f.ACCTNO AS BIGINT) = m.ACCTNO
                WHERE COALESCE(m.IS_PIBB, 0) = 0
            ) TO '{out_parquet.as_posix()}' (FORMAT PARQUET)
        """)
    finally:
        con.close()


# ============================================================
# MAIN
# ============================================================
def main() -> int:
    print("EIBMLIQI: Starting Islamic New Liquidity Framework job stream...")
    try:
        reptdate = get_report_date()
        print(f"  Report date : {reptdate}")

        inputs = resolve_inputs(reptdate)
        output_file = OUTPUT_DIR / f"EIBMRLFI_LIQP_ISLM_{reptdate:%d%m%y}.txt"
        print(f"  Output file : {output_file}")

        # Temporary Parquet files live only inside this block and are removed
        # automatically after DALWPBBD / EIBMRLFI / KALMLIQI have finished.
        with tempfile.TemporaryDirectory(dir=TEMP_DIR) as tmp:
            tmp_dir = Path(tmp)

            print("\nConverting input SAS datasets to temporary Parquet (once each)...")
            parquet = {}
            for tag, exclude_pibb in CONVERSIONS:
                parquet[tag] = tmp_dir / f"{tag.lower()}.parquet"
                sas_to_parquet(inputs[tag], parquet[tag], tag, exclude_pibb)

            fd_pbb = tmp_dir / "fd_pbb.parquet"
            build_pbb_fd_parquet(parquet["MAIN_FD"], parquet["FD"], fd_pbb)

            # %INC PGM(DALWPBBD);
            bnm_savg, bnm_curn, _bnm_dept = DALWPBBD.build_savg_curn_dept(
                saving_cache=parquet["SAVING"],
                current_cache=parquet["CURRENT"],
                cisdp_cache=parquet["CISDP"],
            )

            # %INC PGM(EIBMRLFI);  (drives KALMLIQI)
            EIBMRLFI.main(
                reptdate=reptdate,
                loan_cache=parquet["LOAN"],
                fd_cache=fd_pbb,
                current_cache=parquet["CURRENT"],
                lncomm_cache=parquet["LNCOMM"],
                k1_cache=parquet["K1TBL"],
                k3_cache=parquet["K3TBL"],
                bnm_savg=bnm_savg,
                bnm_curn=bnm_curn,
                output_file=output_file,
            )
    except Exception as exc:
        print(f"EIBMLIQI: job failed - {exc}")
        return 4
    print("EIBMLIQI: Job completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
