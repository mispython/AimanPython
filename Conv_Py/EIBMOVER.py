#!/usr/bin/env python3
"""
Program  : EIBMOVER.py
Purpose  : Convert ELDS override-account text file into a SAS warehouse
           dataset (Parquet equivalent) for downstream EDW consumption.
           Reads REPTDATE from the MNILN Parquet dataset, then parses the
           ELDS fixed-width text file (ELDSTXT) and writes a named Parquet
           dataset (ELOVER<MM><WK><YY>).

SAS Original : EIBMOVER / EIBOVERR
ESMR         : 06-1703
Continue by  : ELDSMOVEACC.SAS (SAS Server) after FTP
Migrated to  : Python / Polars

Inputs  : MNILN.REPTDATE  - SAP.PBB.MNILN(0) Parquet: REPTDATE dataset
                            Expected columns: REPTDATE (SAS date integer)
          ELDSTXT         - SAP.PBB.ELDS.EXCNPL.TEXT(0)  (fixed-width text)

Outputs : ELOVER<MM><WK><YY>.parquet  - <OUTPUT_DIR>/ELOVER<MM><WK><YY>.parquet
                                         Named dataset equivalent (Parquet).

Notes   :
  - PROC CPORT / FTP transfer (SAP.PBB.ELOVERWH.ELOVFTP -> EDW) is a
    mainframe infrastructure operation and is not replicated here;
    see stub below.
  - REPTDATE is stored as a SAS date integer (days since 1960-01-01);
    converted to Python date for macro variable derivation.
  - CALL SYMPUT('NOWK', PUT(4,$1.)) always produces '4' (literal constant).
  - COMMA13.0 informat: strip commas, parse as integer.
  - IF ICNO NE ' ' filters out records where ICNO is blank.
  - CUSTIND: 'C' if UPCASE(CUST)='COMMERCIAL', else 'I'.
"""

import sys
import logging
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import duckdb

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
INPUT_DIR  = Path("input")
OUTPUT_DIR = Path("output")

# SAP.PBB.MNILN(0) — Parquet file containing the REPTDATE dataset
MNILN_REPTDATE_PATH = INPUT_DIR / "MNILN_REPTDATE.parquet"

# SAP.PBB.ELDS.EXCNPL.TEXT(0) — fixed-width ELDS text file
ELDSTXT_PATH = INPUT_DIR / "EXCNPL.TEXT"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DATE HELPER
# ---------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_python(sas_days: int | float) -> date:
    """Convert a SAS date integer (days since 1960-01-01) to a Python date."""
    return SAS_EPOCH + timedelta(days=int(sas_days))


# ---------------------------------------------------------------------------
# STEP 0 – Read REPTDATE from MNILN Parquet dataset
# ---------------------------------------------------------------------------
# SAS:
#   DATA ELDS.REPTDATE;
#      SET MNILN.REPTDATE;
#      CALL SYMPUT('NOWK',    PUT(4, $1.));       /* always '4' (literal) */
#      CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR2.));
#      CALL SYMPUT('REPTMON',  PUT(MONTH(REPTDATE), Z2.));
#      CALL SYMPUT('REPDATE',  PUT(REPTDATE, DDMMYY8.));
#      CALL SYMPUT('RDATE',    PUT(REPTDATE, DDMMYY8.));
#   RUN;

def read_reptdate(path: Path) -> tuple[date, str, str, str, str, str]:
    """Read REPTDATE from the MNILN Parquet dataset and derive macro variables.

    Returns (reptdate, nowk, reptyear, reptmon, repdate, rdate) where:
      nowk     : always '4' (SAS literal PUT(4,$1.))
      reptyear : 2-digit year string (YEAR2. format)
      reptmon  : zero-padded 2-digit month string (Z2. format)
      repdate  : DDMMYY8. formatted date string (e.g. '10032009')
      rdate    : same as repdate
    """
    con = duckdb.connect()
    df = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{path}') LIMIT 1"
    ).pl()

    if df.is_empty():
        log.error("MNILN REPTDATE dataset is empty: %s", path)
        sys.exit(1)

    reptdate_raw = df["REPTDATE"][0]
    reptdate = sas_date_to_python(reptdate_raw)

    # CALL SYMPUT('NOWK', PUT(4,$1.)) → always the character '4'
    nowk     = "4"
    # YEAR2. format: last 2 digits of year, zero-padded
    reptyear = str(reptdate.year)[-2:].zfill(2)
    # Z2. format: zero-padded 2-digit month
    reptmon  = str(reptdate.month).zfill(2)
    # DDMMYY8. format: 'DDMMYYYY'
    repdate  = reptdate.strftime("%d%m%Y")
    rdate    = repdate

    log.info(
        "REPTDATE=%s  NOWK=%s  REPTYEAR=%s  REPTMON=%s  REPDATE=%s",
        reptdate.isoformat(), nowk, reptyear, reptmon, repdate,
    )
    return reptdate, nowk, reptyear, reptmon, repdate, rdate


# ---------------------------------------------------------------------------
# STEP 1 – Read ELDSTXT and produce ELOVER dataset
# ---------------------------------------------------------------------------
# SAS INPUT layout for ELDSTXT (1-based column positions, text file):
#   @001 ICNO     $12.         →  cols   1-12   width 12
#   @016 AANO     $13.         →  cols  16-28   width 13
#   @032 APPVDT   $10.         →  cols  32-41   width 10
#   @045 BRANCH    3.          →  cols  45-47   width  3
#   @051 NAME     $60.         →  cols  51-110  width 60
#   @114 FACILITY $30.         →  cols 114-143  width 30
#   @147 PRODUCT  $3.          →  cols 147-149  width  3
#   @153 APPVLIMT COMMA13.0    →  cols 153-165  width 13
#   @169 APPVBY   $3.          →  cols 169-171  width  3
#   @175 APPVNM   $60.         →  cols 175-234  width 60
#   @238 APPVDES  $60.         →  cols 238-297  width 60
#   @301 CRR      $10.         →  cols 301-310  width 10
#   @314 RECMNM   $60.         →  cols 314-373  width 60
#   @377 RECMDES  $60.         →  cols 377-436  width 60
#   @440 CUST     $10.         →  cols 440-449  width 10
#   @453 BADCCRIS $1.          →  cols 453-453  width  1
#   @457 SEGID    $1.          →  cols 457-457  width  1

def read_eldstxt(path: Path) -> list[dict]:
    """Read ELDSTXT fixed-width text file (FIRSTOBS=2) and return list of row dicts.

    Applies:
      - IF ICNO NE ' ' : filters out records where ICNO is blank.
      - CUSTIND derivation: 'C' if UPCASE(CUST)='COMMERCIAL', else 'I'.
    """
    rows = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        lines = fh.readlines()

    # FIRSTOBS=2 → skip line index 0 (header row)
    for raw_line in lines[1:]:
        # Pad to at least 457 characters to avoid index errors
        line = raw_line.rstrip("\n\r").ljust(457)

        # --- Read fields (1-based SAS col → 0-based Python: offset = col-1) ---
        icno     = line[0:12]                      # @001 $12.
        aano     = line[15:28]                     # @016 $13.
        appvdt   = line[31:41]                     # @032 $10.
        branch_raw = line[44:47]                   # @045  3.
        name     = line[50:110]                    # @051 $60.
        facility = line[113:143]                   # @114 $30.
        product  = line[146:149]                   # @147 $3.
        appvlimt_raw = line[152:165]               # @153 COMMA13.0
        appvby   = line[168:171]                   # @169 $3.
        appvnm   = line[174:234]                   # @175 $60.
        appvdes  = line[237:297]                   # @238 $60.
        crr      = line[300:310]                   # @301 $10.
        recmnm   = line[313:373]                   # @314 $60.
        recmdes  = line[376:436]                   # @377 $60.
        cust     = line[439:449]                   # @440 $10.
        badccris = line[452:453]                   # @453 $1.
        segid    = line[456:457]                   # @457 $1.

        # --- IF ICNO NE ' ' ---
        if not icno.strip():
            continue

        # --- BRANCH: numeric 3. informat ---
        try:
            branch = float(branch_raw.strip()) if branch_raw.strip() else None
        except ValueError:
            branch = None

        # --- APPVLIMT: COMMA13.0 informat (strip commas, parse as integer) ---
        try:
            appvlimt = float(appvlimt_raw.replace(",", "").strip()) if appvlimt_raw.strip() else None
        except ValueError:
            appvlimt = None

        # --- CUSTIND derivation ---
        # IF UPCASE(CUST) = 'COMMERCIAL' THEN CUSTIND = 'C';
        # ELSE CUSTIND = 'I';
        custind = "C" if cust.strip().upper() == "COMMERCIAL" else "I"

        rows.append({
            "ICNO"     : icno,
            "AANO"     : aano,
            "APPVDT"   : appvdt,
            "BRANCH"   : branch,
            "NAME"     : name,
            "FACILITY" : facility,
            "PRODUCT"  : product,
            "APPVLIMT" : appvlimt,
            "APPVBY"   : appvby,
            "APPVNM"   : appvnm,
            "APPVDES"  : appvdes,
            "CRR"      : crr,
            "RECMNM"   : recmnm,
            "RECMDES"  : recmdes,
            "CUST"     : cust,
            "BADCCRIS" : badccris,
            "SEGID"    : segid,
            "CUSTIND"  : custind,
        })

    log.info("ELDSTXT records read (excl. header, excl. blank ICNO): %d", len(rows))
    return rows


def build_elover_df(rows: list[dict]) -> pl.DataFrame:
    """Convert ELDSTXT row dicts into a typed Polars DataFrame."""
    schema = {
        "ICNO"     : pl.Utf8,
        "AANO"     : pl.Utf8,
        "APPVDT"   : pl.Utf8,
        "BRANCH"   : pl.Float64,
        "NAME"     : pl.Utf8,
        "FACILITY" : pl.Utf8,
        "PRODUCT"  : pl.Utf8,
        "APPVLIMT" : pl.Float64,
        "APPVBY"   : pl.Utf8,
        "APPVNM"   : pl.Utf8,
        "APPVDES"  : pl.Utf8,
        "CRR"      : pl.Utf8,
        "RECMNM"   : pl.Utf8,
        "RECMDES"  : pl.Utf8,
        "CUST"     : pl.Utf8,
        "BADCCRIS" : pl.Utf8,
        "SEGID"    : pl.Utf8,
        "CUSTIND"  : pl.Utf8,
    }
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema)


# ---------------------------------------------------------------------------
# PROC CPORT / FTP STUB
# ---------------------------------------------------------------------------
# SAS:
#   FILENAME TRANFILE 'SAP.PBB.ELOVERWH.ELOVFTP' DISP=OLD;
#   PROC CPORT LIBRARY=ELDS FILE=TRANFILE;
#   RUN;
#
# PROC CPORT serialises the ELDS library (containing REPTDATE and
# ELOVER<MM><WK><YY>) to the transport file SAP.PBB.ELOVERWH.ELOVFTP
# for subsequent SFTP transfer to the EDW landing zone.
# This is a mainframe-only infrastructure operation.  In the Python
# pipeline the output Parquet file serves as the equivalent and should
# be transferred to the EDW via the appropriate modern file-transfer
# mechanism (SFTP / Azure Blob / etc.).
# Downstream processing continues in ELDSMOVEACC.SAS on the SAS Server.


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("EIBMOVER started.")

    # --- Step 0: Read REPTDATE from MNILN Parquet dataset ---
    reptdate, nowk, reptyear, reptmon, repdate, rdate = read_reptdate(MNILN_REPTDATE_PATH)

    # --- Step 1: Read ELDSTXT and derive CUSTIND ---
    log.info("Reading ELDSTXT: %s", ELDSTXT_PATH)
    elover_rows = read_eldstxt(ELDSTXT_PATH)

    df = build_elover_df(elover_rows)

    # Dataset name mirrors SAS: ELDS.ELOVER&REPTMON&NOWK&REPTYEAR
    dataset_name = f"ELOVER{reptmon}{nowk}{reptyear}"
    parquet_path = OUTPUT_DIR / f"{dataset_name}.parquet"
    df.write_parquet(parquet_path)
    log.info("Parquet dataset written: %s  (%d rows)", parquet_path, len(df))

    # PROC PRINT DATA=ELDS.ELOVER<...> (OBS=50) — diagnostic only, not replicated

    # --- PROC CPORT / FTP: not replicated — see stub comment above ---

    log.info("EIBMOVER completed successfully.")


if __name__ == "__main__":
    main()
