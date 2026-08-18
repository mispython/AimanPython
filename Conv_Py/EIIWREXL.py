#!/usr/bin/env python3
"""
Program : EIIWREXL.py
Purpose : Extract detail transactions on other RM miscellaneous
          liabilities to non-residents for BNM REXL report (PIBB).

Original job : EIIWREXL
  DELETE step (SAP.PIBB.REXL.TEXT DISP=(MOD,DELETE,DELETE))
      -> handled by opening OUTPUT_FILE in 'w' mode (truncate/recreate)
  %INC PGM(DALWPBBD) / %INC PGM(FALWPBBD)
      -> module imports below (trigger their module-level execution)
  DATA _NULL_ extract step (SET SAVG CURN FDWKLY(RENAME=...) END=LAST)
      -> Steps 1-2 below
  SFTP01 DD + RUNSFTP job step
      -> Step 3 below (paramiko upload)

Dependency (chained %INC programs):
    %INC PGM(DALWPBBD);  -> already converted as DALWPBBD.py
        from DALWPBBD import BNM_SAVG, BNM_CURN   (module import — see
        Step 1; DALWPBBD.BNM_DEPT is built there but not referenced here)
    %INC PGM(FALWPBBD);  -> already converted as FALWPBBD.py
        from FALWPBBD import BNM_FDWKLY           (module import — see
        Step 1; FALWPBBD.BNM_UMA is built there but not referenced here)

Dependency note (PBBDPFMT):
    DALWPBBD.py / FALWPBBD.py each %INC PGM(PBBDPFMT) for their own
    format calls (SACUSTCD/STATECD/... and FDPROD/FDDENOM/...). This
    program (EIIWREXL) does not call any PBBDPFMT format directly, so
    PBBDPFMT is not imported here.

============================================================================
PHYSICAL INPUT DATASETS USED BY THIS PROGRAM
============================================================================
This program has NO physical .sas7bdat inputs of its own. All source data
is obtained indirectly through the two %INC'd programs listed above:
    - DALWPBBD.py physically reads DEPOSIT.SAVING, DEPOSIT.CURRENT, and
      CISDP.DEPOSIT (see DALWPBBD.py module docstring items 1-3).
    - FALWPBBD.py physically reads FD.FD and DEPOSIT.UMA (see
      FALWPBBD.py module docstring items 1-2).
Importing DALWPBBD / FALWPBBD below triggers those physical reads.

------------------------------------------------------------------------
NON-FILE / DERIVED / TEMPORARY DATA USED BY THIS PROGRAM
------------------------------------------------------------------------
- DALWPBBD.BNM_SAVG, DALWPBBD.BNM_CURN, FALWPBBD.BNM_FDWKLY : in-memory
  polars DataFrames produced as a side effect of importing the two
  dependency modules (see their own docstrings for "NON-FILE / DERIVED /
  TEMPORARY OUTPUTS"). This program reads them directly rather than via
  their persisted Parquet cache, since both modules are imported
  in-process (mirrors SAS %INC PGM() sharing work-library datasets across
  %INC'd code within the same job step).
- `combined` (Step 1) : the unioned/filtered SAVG+CURN+FDWKLY(renamed)
  working set, built here only, not written to disk — equivalent of the
  anonymous SET-statement working set in the original DATA _NULL_ step.

REPTDATE.py / no reptdate.parquet:
  DEPOSIT.REPTDATE has no physical Parquet/SAS equivalent in this
  project; REPTDATE/REPTMON/NOWK are derived from REPTDATE.py's
  get_reptdate_values() using the same exact-match SELECT(DAY(REPTDATE))
  logic (WHEN 8/15/22/OTHERWISE) as DALWPBBD.py / FALWPBBD.py, since all
  three programs share these macro variables within the same job step.
"""

from pathlib import Path

import polars as pl
import paramiko

from REPTDATE import get_reptdate_values

# Triggers module-level execution of the %INC'd programs, exposing their
# BNM_* DataFrames — mirrors EIIWREXL's %INC PGM(DALWPBBD) / %INC PGM(FALWPBBD).
import DALWPBBD
import FALWPBBD

# HOST_DESC lookup against ctl_dwh_sftp_info.sas7bdat is not yet confirmed
# for this DRR destination (same situation as EIIWKAPE.py Step 13), so the
# import is kept but the actual transfer call is guarded in a try/except
# below rather than assumed to succeed silently.
from EDW_TRANSFORMATION import get_sftp_info

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
OUTPUT_DIR = BASE_DIR / "output" / "EIIWREXL"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# SFTP target — mirrors JCL: cd "FD-BNM REPORTING/PIBB/BNM RPTG"
SFTP_REMOTE_DIR = "FD-BNM REPORTING/PIBB/BNM RPTG"

# HOST_DESC key for the DRR host is unconfirmed against
# ctl_dwh_sftp_info.sas7bdat (same open item as EIIWKAPE.py / EIWFRMCR).
HOST_DESC = "DRR"

LRECL = 1000   # RECFM=FB, LRECL=1000 -> each output line padded to 1000 chars
DLM   = chr(0x05)   # SAS '05'X delimiter

CUSTCD_NON_RESIDENT = [
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "95", "96", "98", "99",
]
PRODCD_ELIGIBLE = ["42132", "42199"]

BNMCODE = "4929980000000Y"

# ============================================================================
# STEP 0: REPORT DATE / WEEK NUMBER
# DATA REPTDATE; SET DEPOSIT.REPTDATE; SELECT(DAY(REPTDATE)) ...
# (no physical file — see module docstring)
# ============================================================================
print("Step 0: Deriving report date / week number...")

_reptdate_values = get_reptdate_values()
REPTDATE = _reptdate_values.reptdate

_day = REPTDATE.day
if _day == 8:
    NOWK = "1"
elif _day == 15:
    NOWK = "2"
elif _day == 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTMON = f"{REPTDATE.month:02d}"          # PUT(MONTH(REPTDATE),Z2.)
REPTDT  = REPTDATE.strftime("%d%m%y")      # PUT(REPTDATE,DDMMYYN6.)

print(f"  REPTDATE : {REPTDATE}")
print(f"  REPTMON  : {REPTMON}")
print(f"  NOWK     : {NOWK}")
print(f"  REPTDT   : {REPTDT}")

OUTPUT_FILE = OUTPUT_DIR / "SAP_PIBB_REXL.txt"
REMOTE_FILENAME = f"REXL_49299_{REPTDT}.XLS"


# ============================================================================
# STEP 1: BUILD COMBINED SET
# DATA _NULL_; SET BNM.SAVG&REPTMON&NOWK BNM.CURN&REPTMON&NOWK
#     BNM.FDWKLY(RENAME=(INTPAY=INTPAYBL CUSTCODE=CUSTCD BIC=PRODCD
#                        ACCTTYPE=PRODUCT)) END=LAST;
# WHERE PRODCD IN ('42132','42199') & CUSTCD IN (...) & INTPAYBL NOT IN (.,0);
# (inputs sourced from DALWPBBD.BNM_SAVG / DALWPBBD.BNM_CURN /
#  FALWPBBD.BNM_FDWKLY — see module docstring)
# ============================================================================
print("\nStep 1: Combining SAVG / CURN / FDWKLY (renamed) and filtering...")

_savg = DALWPBBD.BNM_SAVG.select(
    ["BRANCH", "ACCTNO", "CUSTCD", "PRODCD", "NAME", "PRODUCT", "INTPAYBL"]
)
_curn = DALWPBBD.BNM_CURN.select(
    ["BRANCH", "ACCTNO", "CUSTCD", "PRODCD", "NAME", "PRODUCT", "INTPAYBL"]
)
# RENAME=(INTPAY=INTPAYBL CUSTCODE=CUSTCD BIC=PRODCD ACCTTYPE=PRODUCT)
_fdwkly = FALWPBBD.BNM_FDWKLY.select([
    "BRANCH", "ACCTNO",
    pl.col("CUSTCODE").alias("CUSTCD"),
    pl.col("BIC").alias("PRODCD"),
    "NAME",
    pl.col("ACCTTYPE").alias("PRODUCT"),
    pl.col("INTPAY").alias("INTPAYBL"),
])

combined = pl.concat([_savg, _curn, _fdwkly])

combined = combined.filter(
    pl.col("PRODCD").is_in(PRODCD_ELIGIBLE)
    & pl.col("CUSTCD").is_in(CUSTCD_NON_RESIDENT)
    & pl.col("INTPAYBL").is_not_null()
    & (pl.col("INTPAYBL") != 0)
)

# _N_ is assigned in SET-read order AFTER the WHERE filter (WHERE on a SET
# restricts what is read into the DATA step, so _N_ counts only qualifying
# rows) — a simple 1-based row index over the filtered, unioned dataset.
combined = combined.with_row_index(name="OBS", offset=1)

# SPTF+INTPAYBL -- running (grand) total, printed once at the final row.
GRAND_TOTAL = combined["INTPAYBL"].sum() if len(combined) else 0.0

print(f"  Combined/filtered rows: {len(combined):,}")


# ============================================================================
# STEP 2: BUILD REPORT LINES  (DLM-delimited flat text, RECFM=FB LRECL=1000
# — plain delimited extract, NOT an ASA-controlled print report)
# ============================================================================
print("\nStep 2: Building report lines...")


def _comma_fmt(value, decimals: int = 4) -> str:
    """Mirror SAS COMMA13.4 numeric display format."""
    if value is None:
        return ""
    return f"{float(value):,.{decimals}f}"


def _num_str(value) -> str:
    """Default numeric display (avoids '78.0' float artefacts)."""
    if value is None:
        return ""
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)


def _pad(line: str) -> str:
    return line[:LRECL].ljust(LRECL)


output_lines: list[str] = []

# Header block (printed once, IF _N_=1)
output_lines.append(_pad("PUBLIC ISLAMIC BANK BERHAD"))
output_lines.append(_pad(
    "DETAIL TRANSACTIONS ON OTHER RM MISCELLANEOUS LIABILITIES TO NON-RESIDENTS"
))
output_lines.append(_pad(""))
header_fields = [
    "Obs", "BNMCODE", "CUSTCD", "PRODCD", "BRANCH", "ACCTNO", "NAME",
    "PRODUCT", "INTPAYBL",
]
output_lines.append(_pad(DLM.join(header_fields) + DLM))

for row in combined.iter_rows(named=True):
    fields = [
        _num_str(row["OBS"]),
        BNMCODE,
        row["CUSTCD"] or "",
        row["PRODCD"] or "",
        _num_str(row["BRANCH"]),
        _num_str(row["ACCTNO"]),
        row["NAME"] or "",
        _num_str(row["PRODUCT"]),
        _comma_fmt(row["INTPAYBL"]),
    ]
    output_lines.append(_pad(DLM.join(fields) + DLM))

# IF LAST -> total line: blank Obs, 'TOTAL' in BNMCODE slot, 6 blanks
# (CUSTCD/PRODCD/BRANCH/ACCTNO/NAME/PRODUCT), then SPTF in INTPAYBL slot.
if len(combined):
    total_fields = ["", "TOTAL", "", "", "", "", "", "", _comma_fmt(GRAND_TOTAL)]
    output_lines.append(_pad(DLM.join(total_fields) + DLM))

with open(OUTPUT_FILE, "w", encoding="latin1", newline="") as fh:
    for ln in output_lines:
        fh.write(ln + "\n")

print(f"  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")
print(f"  Grand total (SPTF): {_comma_fmt(GRAND_TOTAL)}")


# ============================================================================
# STEP 3: SFTP UPLOAD
# DATA _NULL_; FILE SFTP01; PUT @1 "PUT //SAP.PIBB.REXL.TEXT  REXL_49299_&REPTDT..XLS";
# RUNSFTP job step: cd "FD-BNM REPORTING/PIBB/BNM RPTG"
# ============================================================================
print("\nStep 3: Uploading via SFTP...")

try:
    sftp_info = get_sftp_info(HOST_DESC)
    transport = paramiko.Transport((sftp_info["HOST"], sftp_info.get("PORT", 22)))
    transport.connect(username=sftp_info["USER"], password=sftp_info["PASSWORD"])
    sftp = paramiko.SFTPClient.from_transport(transport)

    remote_path = f"{SFTP_REMOTE_DIR}/{REMOTE_FILENAME}"
    sftp.put(str(OUTPUT_FILE), remote_path)

    sftp.close()
    transport.close()
    print(f"  Uploaded {OUTPUT_FILE.name} -> {remote_path}")
except Exception as e:
    print(f"  SFTP upload failed: {e}")
    print(f"  Remote dir  : {SFTP_REMOTE_DIR}")
    print(f"  Remote name : {REMOTE_FILENAME}")
    print("  (HOST_DESC key unconfirmed against ctl_dwh_sftp_info.sas7bdat)")

print("\nEIIWREXL complete.")
