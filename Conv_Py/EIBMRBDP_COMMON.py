#!/usr/bin/env python3
"""
Program : EIBMRBDP_COMMON.py
Purpose : Shared helpers for EIBMRBDP.py and EIBMRB01-09.py
          - report date = BATCH_DTTM of SOURCE_SYSTEM_CD 'DP' (ctl_dwh_batch_dttm)
          - dated output name  EIBMRBxx@DDMMYY.TXT  (as built by FTPMRBDP / ask.py)
          - SFTP of a report to the Data Report Repository (DRR)
          - temp folder for the .sas7bdat -> Parquet conversion (removed after the run)
          - ENTITY_CD filters that split PBB / PIBB inside a shared input file
"""

import tempfile
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import paramiko

# ============================================================================
# CONFIGURATION
# ============================================================================
CTL_BATCH_DTTM = Path("/sasdata/dwh/control/ctl_dwh_batch_dttm.sas7bdat")
CTL_SFTP_INFO  = Path("/sasdata/dwh/control/ctl_dwh_sftp_info.sas7bdat")
BATCH_SOURCE   = "DP"
DRR_HOST_DESC  = "Data Report Repository (DRR)"
SFTP_PORT      = 22
TEMP_DIR: Optional[str] = None          # None = system temp area

# Folder chain from ask.py: cd "WMTB-SA&S/DEP - SAS/BANK TOTAL", then "../MISC/...", etc.
DRR_ROOT = "WMTB-SA&S/DEP - SAS"
DRR_FOLDERS = {
    "EIBMRB01": f"{DRR_ROOT}/BANK TOTAL",
    "EIBMRB02": f"{DRR_ROOT}/BANK TOTAL",
    "EIBMRB09": f"{DRR_ROOT}/MISC/FD & FCY FD WITHDRAWAL",
    "EIBMRB8A": f"{DRR_ROOT}/MISC/FD & FCY FD WITHDRAWAL",
    "EIBMRB8B": f"{DRR_ROOT}/MISC/FD & FCY FD WITHDRAWAL",
    "EIBMRB03": f"{DRR_ROOT}/MISC/NEW ACT OPENED",
    "EIBMRB04": f"{DRR_ROOT}/MISC/NEW ACT OPENED",
    "EIBMRB06": f"{DRR_ROOT}/MISC/NEW ACT OPENED",
    "EIBMRB07": f"{DRR_ROOT}/MISC/NEW ACT OPENED",
    "EIBMRB05": f"{DRR_ROOT}/RM RPT/FCY & GIA/OS BALANCE",
}

# ENTITY_CD split (PBB = everything that is not PIBB, a null ENTITY_CD stays with PBB)
PBB_ENTITY_SQL  = "COALESCE(TRIM(CAST(ENTITY_CD AS VARCHAR)), '') <> 'PIBB'"
PIBB_ENTITY_SQL = "COALESCE(TRIM(CAST(ENTITY_CD AS VARCHAR)), '') = 'PIBB'"


# ============================================================================
# REPORT DATE
# ============================================================================
def get_batch_date(source_system_cd: str = BATCH_SOURCE) -> date:
    """Read BATCH_DTTM for the given SOURCE_SYSTEM_CD from the control file."""
    ctl = pd.read_sas(str(CTL_BATCH_DTTM), encoding="latin1")
    ctl["SOURCE_SYSTEM_CD"] = ctl["SOURCE_SYSTEM_CD"].astype(str).str.strip()
    hit = ctl.loc[ctl["SOURCE_SYSTEM_CD"] == source_system_cd, "BATCH_DTTM"]
    if hit.empty:
        raise ValueError(f"SOURCE_SYSTEM_CD '{source_system_cd}' not found in {CTL_BATCH_DTTM}")
    return pd.Timestamp(hit.iloc[0]).date()


def dated_output_name(report_id: str, reptdate: date) -> str:
    """EIBMRB01@DDMMYY.TXT  (ask.py: EIBMRB01@%TODD.%TOMM.%TOYY..TXT)."""
    return f"{report_id}@{reptdate:%d%m%y}.TXT"


# ============================================================================
# TEMP PARQUET AREA
# ============================================================================
def new_temp_cache() -> tempfile.TemporaryDirectory:
    """Temp folder for the Parquet conversion; removed by .cleanup() and at interpreter exit."""
    return tempfile.TemporaryDirectory(prefix="eibmrbdp_pq_", dir=TEMP_DIR)


# ============================================================================
# SFTP TO DRR
# ============================================================================
def get_sftp_info(host_desc: str = DRR_HOST_DESC) -> dict:
    """Credentials / host for the given HOST_DESC from ctl_dwh_sftp_info."""
    ctl = pd.read_sas(str(CTL_SFTP_INFO), encoding="latin1")
    for col in ("SFTP_ID", "SFTP_PW", "HOST_IP", "HOST_DESC"):
        ctl[col] = ctl[col].fillna("").astype(str).str.strip()
    hit = ctl.loc[ctl["HOST_DESC"] == host_desc]
    if hit.empty:
        raise ValueError(f"HOST_DESC '{host_desc}' not found in {CTL_SFTP_INFO}")
    row = hit.iloc[0]
    return {"user": row["SFTP_ID"], "password": row["SFTP_PW"], "host": row["HOST_IP"]}


def sftp_put(local_file: Path, report_id: str, remote_name: str) -> None:
    """Upload local_file into the DRR folder of report_id as remote_name."""
    remote_dir = DRR_FOLDERS[report_id]
    info = get_sftp_info()
    transport = paramiko.Transport((info["host"], SFTP_PORT))
    try:
        transport.connect(username=info["user"], password=info["password"])
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            sftp.chdir(remote_dir)
            sftp.put(str(local_file), remote_name)
        finally:
            sftp.close()
    finally:
        transport.close()
    print(f"  SFTP done      : {local_file.name} -> {info['host']}:{remote_dir}/{remote_name}")
