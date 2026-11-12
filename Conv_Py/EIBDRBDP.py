"""
Program : EIBDRBDP.py
Purpose : Driver job for the RM/FCY FD daily reports. Mirrors the JCL job
          EIBDRBDP, which performs no report logic itself — it only:
            1. Deletes previously catalogued output datasets (equivalent to
               the IEFBR14 DELETE step, DISP=(MOD,DELETE,DELETE)), so a
               stale copy from a prior run cannot linger.
            2. Executes step EIBDRB01, then step EIBDRB02, in that order.
          The two report programs are unrelated at the data level; they are
          only scheduled together under this driver.
"""

from datetime import date
from pathlib import Path

import pandas as pd

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Production Path
OUTPUT_DIR     = Path("/host_pq/mis/output/deposit/report/EIBDRBDP")
CTL_BATCH_DTTM = Path("/sasdata/dwh/control/ctl_dwh_batch_dttm.sas7bdat")
BATCH_SOURCE   = "DP"          # SOURCE_SYSTEM_CD used for the report date

# # Testing Path
# OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDRBDP")


def get_batch_date(source_system_cd: str) -> date:
    """Read BATCH_DTTM for the given SOURCE_SYSTEM_CD from the control file."""
    ctl = pd.read_sas(str(CTL_BATCH_DTTM), encoding="latin1")
    ctl["SOURCE_SYSTEM_CD"] = ctl["SOURCE_SYSTEM_CD"].astype(str).str.strip()
    hit = ctl.loc[ctl["SOURCE_SYSTEM_CD"] == source_system_cd, "BATCH_DTTM"]
    if hit.empty:
        raise ValueError(f"SOURCE_SYSTEM_CD '{source_system_cd}' not found in {CTL_BATCH_DTTM}")
    return pd.Timestamp(hit.iloc[0]).date()


REPTDATE = get_batch_date(BATCH_SOURCE)
MMYY     = REPTDATE.strftime("%m%y")       # %OMM%OYY
DDMMYY   = REPTDATE.strftime("%d%m%y")     # %ODD%OMM%OYY

# ============================================================================
# OUTPUT FILES CORRESPONDING TO THE ORIGINAL CATALOGUED DATASETS
# DD01 -> SAP.PBB.EIBDRB01  (EIBDRB01 SASLIST output)
# DD02 -> SAP.PBB.EIBDRB2A  (EIBDRB02 RMWDRAW output)
# DD03 -> SAP.PBB.EIBDRB2B  (EIBDRB02 FCYWDRAW output)
# ============================================================================
STALE_OUTPUTS = [
    OUTPUT_DIR / f"EIBDRB01@{MMYY}.TXT",     # DD01
    OUTPUT_DIR / f"EIBDRB2A@{DDMMYY}.TXT",   # DD02
    OUTPUT_DIR / f"EIBDRB2B@{DDMMYY}.TXT",   # DD03
]


def delete_stale_outputs() -> None:
    """Equivalent of the DELETE step (IEFBR14, DISP=(MOD,DELETE,DELETE))."""
    print("Step 0: Deleting stale catalogued outputs (if present)...")
    for path in STALE_OUTPUTS:
        if path.exists():
            path.unlink()
            print(f"  Deleted: {path}")
        else:
            print(f"  Not found (nothing to delete): {path}")


if __name__ == "__main__":
    delete_stale_outputs()

    # STEP EIBDRB01 - module-level code runs on import, AFTER the delete step
    import EIBDRB01  # noqa: F401,E402
    print("\n>>> EIBDRB01 step complete.")

    # STEP EIBDRB02 - module-level code runs on import
    import EIBDRB02  # noqa: F401,E402
    print("\n>>> EIBDRB02 step complete.")

    print("\nEIBDRBDP job complete.")
