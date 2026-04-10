#!/usr/bin/env python3
"""
Program  : EIBMMRGN.py
Purpose  : Generate a margin report based on fields captured from ELDS.
           Reads a fixed-width ELDS margin text file (MARGTXT), re-orders
           and reformats selected fields into a fixed-width output flat file
           (MARGOUT), then re-reads that flat file and writes a dated Parquet
           dataset (SHAREM<YY><MM><DD>) for EDW consumption.

SAS Original : EIBMMRGN / EIBMMGN2 (two-step JCL job)
ESMR         : 2009-9
Date         : 10-03-09 (HHH)
Migrated to  : Python / Polars

Inputs  : DATEFILE  - RBP2.SAS.B033.DATEFILE   (fixed-width text, LRECL=80)
          MARGTXT   - SAP.PBB.ELDS.SMDSIBC.TEXT(0)  (fixed-width text)

Outputs : MARGOUT          - <OUTPUT_DIR>/MARGOUT.txt
                             Re-ordered fixed-width flat file (LRECL=487).
          SHAREM<YY><MM><DD>.parquet
                           - <OUTPUT_DIR>/SHAREM<YY><MM><DD>.parquet
                             Named SAS-dataset equivalent (Parquet).

Notes   :
  - PROC CPORT / FTP transfer (SAP.PBB.MARGIFTP -> EDW) is a mainframe
    infrastructure operation and is not replicated here; see stub below.
  - YEARCUTOFF=1950 applies to MMDDYY8. date parsing in DATEFILE.
  - $UPCASE informats are applied at read time (str.upper()).
  - Numeric-string fields (OS_BAL, LIMIT, TOTLIMIT, TOT_OS) are
    right-justified strings with embedded commas; they are stripped and
    cast to float (equivalent to SAS RIGHT / COMPRESS / *1 chain).
"""

import sys
import logging
from datetime import date, timedelta
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
INPUT_DIR  = Path("input")
OUTPUT_DIR = Path("output")

DATEFILE_PATH = INPUT_DIR  / "DATEFILE.txt"          # RBP2.SAS.B033.DATEFILE
MARGTXT_PATH  = INPUT_DIR  / "SMDSIBC.TEXT"          # SAP.PBB.ELDS.SMDSIBC.TEXT(0)
MARGOUT_PATH  = OUTPUT_DIR / "MARGOUT.txt"            # SAP.PBB.MARGOUT (intermediate flat file)

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
# DATE HELPERS  (OPTIONS YEARCUTOFF=1950)
# ---------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)


def parse_mmddyy8(text: str) -> date | None:
    """Parse MMDDYY8. format (e.g. '03102009' or '031009').

    SAS YEARCUTOFF=1950: two-digit years >= 50 map to 1950-1999;
    00-49 map to 2000-2049.
    """
    text = text.strip()
    if len(text) < 6:
        return None
    try:
        mm = int(text[0:2])
        dd = int(text[2:4])
        yy_raw = text[4:]
        if len(yy_raw) == 2:
            yy = int(yy_raw)
            year = (1900 + yy) if yy >= 50 else (2000 + yy)
        else:
            year = int(yy_raw)
        return date(year, mm, dd)
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# STEP 0 – READ REPTDATE FROM DATEFILE
# ---------------------------------------------------------------------------
# SAS:
#   INFILE DATEFILE LRECL=80 OBS=1;
#   INPUT @01 EXTDATE 11.;
#   REPTDATE = INPUT(SUBSTR(PUT(EXTDATE, Z11.), 1, 8), MMDDYY8.);
#   MM = MONTH(REPTDATE);
#   CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR2.));   /* 2-digit year */
#   CALL SYMPUT('REPTMON',  PUT(MM, Z2.));
#   CALL SYMPUT('REPTDAY',  PUT(DAY(REPTDATE), Z2.));

def read_reptdate(path: Path) -> tuple[date, str, str, str]:
    """Read the first record of DATEFILE and derive report date macro variables.

    Returns (reptdate, reptyear, reptmon, reptday) where:
      reptyear : 2-digit year string (YEAR2. format)
      reptmon  : zero-padded 2-digit month string (Z2. format)
      reptday  : zero-padded 2-digit day string   (Z2. format)
    """
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        line = fh.readline()

    # @01 EXTDATE 11. → columns 1-11 (0-based: 0:11)
    extdate_raw = line[0:11].strip()
    try:
        extdate_int = int(extdate_raw)
    except ValueError:
        log.error("Cannot parse EXTDATE from DATEFILE: %r", extdate_raw)
        sys.exit(1)

    # PUT(EXTDATE, Z11.) zero-pads to 11 digits, then SUBSTR(...,1,8) takes first 8
    extdate_z11 = str(extdate_int).zfill(11)
    mmddyy8_str = extdate_z11[0:8]

    reptdate = parse_mmddyy8(mmddyy8_str)
    if reptdate is None:
        log.error("Cannot parse REPTDATE from MMDDYY8. string: %r", mmddyy8_str)
        sys.exit(1)

    # YEAR2. format: last 2 digits of year
    reptyear = str(reptdate.year)[-2:]
    reptmon  = str(reptdate.month).zfill(2)
    reptday  = str(reptdate.day).zfill(2)

    log.info(
        "REPTDATE=%s  REPTYEAR=%s  REPTMON=%s  REPTDAY=%s",
        reptdate.isoformat(), reptyear, reptmon, reptday,
    )
    return reptdate, reptyear, reptmon, reptday


# ---------------------------------------------------------------------------
# HELPERS – numeric-string conversion
# ---------------------------------------------------------------------------

def numstr_to_float(value: str) -> float | None:
    """Convert a right-justified, comma-embedded numeric string to float.

    Replicates the SAS chain:
      field = RIGHT(field);
      field = COMPRESS(field, ',');
      field = field * 1;
    """
    cleaned = value.replace(",", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def fmt_num_18(value: float | None) -> str:
    """Format a numeric value as a right-justified 18-character field.

    Replicates SAS PUT(field, 18.) output column layout.
    Integer display: no decimal point for whole numbers; SAS 18. uses
    best18. equivalent — write as integer if no fractional part.
    """
    if value is None:
        return " " * 18
    # SAS 18. format: right-justified, up to 18 significant digits
    if value == int(value):
        s = str(int(value))
    else:
        # Limit to 18 significant characters
        s = f"{value:.6f}".rstrip("0").rstrip(".")
    return s.rjust(18)


def fmt_num_5(value: float | None) -> str:
    """Format a numeric value as a right-justified 5-character field (SAS 5. format)."""
    if value is None:
        return " " * 5
    return str(int(value)).rjust(5)


def fmt_str(value: str, width: int, upcase: bool = False) -> str:
    """Left-justify a string in a field of given width, optionally uppercasing."""
    if upcase:
        value = value.upper()
    # Left-pad spaces already in value are preserved; truncate to width
    return value.ljust(width)[:width]


# ---------------------------------------------------------------------------
# STEP 1 (EIBMMRGN) – Read MARGTXT, transform, write MARGOUT flat file
# ---------------------------------------------------------------------------
# SAS INPUT layout for MARGTXT (1-based column positions, text file):
#   @001 RGNCODE   $8.
#   @009 NAME      $UPCASE80.
#   @089 IDNO1     $18.
#   @107 NATINLTY  $5.
#   @112 BSCGROUP  5.
#   @117 IDNO2     $18.
#   @135 FAC_DATE  $12.
#   @147 STATUS    $3.
#   @150 CAPACITY  $10.
#   @160 LENDTYPE  $5.
#   @165 MY_FRGN   $3.
#   @168 FACILITY  $18.
#   @186 OS_BAL    $18.    (right-justified numeric string with commas)
#   @204 LIMIT     $18.    (right-justified numeric string with commas)
#   @222 B_UPDATE  $12.
#   @234 TOT_OS    $18.    (right-justified numeric string with commas)
#   @252 TOTLIMIT  $18.    (right-justified numeric string with commas)
#   @270 FROM_YR   5.
#   @275 TO_YR     5.
#   @280 MTH_ODR   $13.
#   @293 COND_ACC  $13.
#   @306 AA_NO     $20.
#   @326 CRRGRADE  $UPCASE5.
#   @331 CONTACT1  $15.
#   @346 CONTACT2  $15.
#   @361 ADDRESS   $UPCASE130.
# Total input length: @361 + 130 - 1 = 490 bytes minimum

def read_margtxt(path: Path) -> list[dict]:
    """Read MARGTXT fixed-width text file (FIRSTOBS=2) and return list of row dicts."""
    rows = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        lines = fh.readlines()

    # FIRSTOBS=2 → skip line index 0 (header row)
    for raw_line in lines[1:]:
        # Pad to at least 490 characters to avoid index errors
        line = raw_line.rstrip("\n\r").ljust(490)

        # --- Read fields (1-based SAS → 0-based Python: offset = col-1) ---
        rgncode  = line[0:8]                        # @001 $8.
        name     = line[8:88].upper()               # @009 $UPCASE80.
        idno1    = line[88:106]                     # @089 $18.
        natinlty = line[106:111]                    # @107 $5.
        bscgroup_raw = line[111:116]                # @112  5.
        idno2    = line[116:134]                    # @117 $18.
        fac_date = line[134:146]                    # @135 $12.
        status   = line[146:149]                    # @147 $3.
        capacity = line[149:159]                    # @150 $10.
        lendtype = line[159:164]                    # @160 $5.
        my_frgn  = line[164:167]                    # @165 $3.
        facility = line[167:185]                    # @168 $18.
        os_bal_raw  = line[185:203]                 # @186 $18.
        limit_raw   = line[203:221]                 # @204 $18.
        b_update    = line[221:233]                 # @222 $12.
        tot_os_raw  = line[233:251]                 # @234 $18.
        totlimit_raw= line[251:269]                 # @252 $18.
        from_yr_raw = line[269:274]                 # @270  5.
        to_yr_raw   = line[274:279]                 # @275  5.
        mth_odr  = line[279:292]                    # @280 $13.
        cond_acc = line[292:305]                    # @293 $13.
        aa_no    = line[305:325]                    # @306 $20.
        crrgrade = line[325:330].upper()            # @326 $UPCASE5.
        contact1 = line[330:345]                    # @331 $15.
        contact2 = line[345:360]                    # @346 $15.
        address  = line[360:490].upper()            # @361 $UPCASE130.

        # --- Numeric conversions ---
        # BSCGROUP 5. informat: right-justified integer
        try:
            bscgroup = float(bscgroup_raw.strip()) if bscgroup_raw.strip() else None
        except ValueError:
            bscgroup = None

        # FROM_YR / TO_YR: numeric 5. informat
        try:
            from_yr = float(from_yr_raw.strip()) if from_yr_raw.strip() else None
        except ValueError:
            from_yr = None
        try:
            to_yr = float(to_yr_raw.strip()) if to_yr_raw.strip() else None
        except ValueError:
            to_yr = None

        # Numeric-string fields: RIGHT + COMPRESS(',') + *1
        os_bal   = numstr_to_float(os_bal_raw)
        limit    = numstr_to_float(limit_raw)
        totlimit = numstr_to_float(totlimit_raw)
        tot_os   = numstr_to_float(tot_os_raw)

        rows.append({
            "RGNCODE"  : rgncode,
            "NAME"     : name,
            "IDNO1"    : idno1,
            "NATINLTY" : natinlty,
            "BSCGROUP" : bscgroup,
            "IDNO2"    : idno2,
            "FAC_DATE" : fac_date,
            "STATUS"   : status,
            "CAPACITY" : capacity,
            "LENDTYPE" : lendtype,
            "MY_FRGN"  : my_frgn,
            "FACILITY" : facility,
            "OS_BAL"   : os_bal,
            "LIMIT"    : limit,
            "B_UPDATE" : b_update,
            "TOT_OS"   : tot_os,
            "TOTLIMIT" : totlimit,
            "FROM_YR"  : from_yr,
            "TO_YR"    : to_yr,
            "MTH_ODR"  : mth_odr,
            "COND_ACC" : cond_acc,
            "AA_NO"    : aa_no,
            "CRRGRADE" : crrgrade,
            "CONTACT1" : contact1,
            "CONTACT2" : contact2,
            "ADDRESS"  : address,
        })

    log.info("MARGTXT records read (excl. header): %d", len(rows))
    return rows


def write_margout(rows: list[dict], path: Path) -> None:
    """Write MARGOUT fixed-width flat file, replicating the SAS DATA MARGIN2 PUT step.

    SAS PUT layout (1-based column positions → field widths):
      @0001 RGNCODE   $8.          →  cols   1-8    width  8
      @0009 BSCGROUP  5.           →  cols   9-13   width  5
      @0014 AA_NO     $20.         →  cols  14-33   width 20
      @0034 CRRGRADE  $UPCASE5.    →  cols  34-38   width  5
      @0039 NAME      $UPCASE80.   →  cols  39-118  width 80
      @0119 IDNO1     $18.         →  cols 119-136  width 18
      @0137 IDNO2     $18.         →  cols 137-154  width 18  (note: @0137 = col137)
      @0152 CONTACT1  $15.         →  cols 152-166  width 15  (SAS @0152: IDNO2 @137+18=155, but SAS says @0152 — overlap gap filled with spaces)
      @0167 CONTACT2  $15.         →  cols 167-181  width 15
      @0182 ADDRESS   $UPCASE130.  →  cols 182-311  width 130
      @0312 NATINLTY  $5.          →  cols 312-316  width  5
      @0317 FAC_DATE  $12.         →  cols 317-328  width 12
      @0329 STATUS    $3.          →  cols 329-331  width  3
      @0332 CAPACITY  $10.         →  cols 332-341  width 10
      @0342 LENDTYPE  $5.          →  cols 342-346  width  5
      @0347 MY_FRGN   $3.          →  cols 347-349  width  3
      @0350 FACILITY  $18.         →  cols 350-367  width 18
      @0368 LIMIT     18.          →  cols 368-385  width 18 (right-justified numeric)
      @0386 OS_BAL    18.          →  cols 386-403  width 18
      @0404 B_UPDATE  $12.         →  cols 404-415  width 12
      @0416 TOT_OS    18.          →  cols 416-433  width 18
      @0434 TOTLIMIT  18.          →  cols 434-451  width 18
      @0452 FROM_YR   5.           →  cols 452-456  width  5
      @0457 TO_YR     5.           →  cols 457-461  width  5
      @0462 MTH_ODR   $13.         →  cols 462-474  width 13
      @0475 COND_ACC  $13.         →  cols 475-487  width 13
      Total record length: 487 characters.

    Note: SAS @0137 IDNO2 $18. occupies cols 137-154, but @0152 CONTACT1 $15.
    starts at col 152, overlapping by 3 bytes. In SAS FILE/PUT, a later @column
    can overwrite earlier content; CONTACT1 wins for cols 152-166.  We replicate
    this by building the record as a mutable buffer and writing each field at its
    specified offset, so later writes overwrite earlier ones exactly as SAS does.
    """
    # Record length = @0475 + 13 - 1 = 487
    RECLEN = 487

    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            # Build mutable buffer initialised to spaces
            buf = bytearray(b" " * RECLEN)

            def put_str(col1: int, width: int, value: str, upcase: bool = False) -> None:
                """Write a left-justified string field at 1-based column col1."""
                s = (value.upper() if upcase else value).ljust(width)[:width]
                start = col1 - 1
                buf[start:start + width] = s.encode("utf-8", errors="replace")[:width]

            def put_num(col1: int, width: int, value: float | None, decimals: int = 0) -> None:
                """Write a right-justified numeric field at 1-based column col1."""
                if value is None:
                    s = " " * width
                elif decimals:
                    s = f"{value:.{decimals}f}".rjust(width)
                else:
                    s = str(int(value)).rjust(width)
                start = col1 - 1
                buf[start:start + width] = s[:width].encode("utf-8", errors="replace")

            put_str(1,   8,   row["RGNCODE"])
            put_num(9,   5,   row["BSCGROUP"])
            put_str(14,  20,  row["AA_NO"])
            put_str(34,  5,   row["CRRGRADE"],  upcase=True)
            put_str(39,  80,  row["NAME"],       upcase=True)
            put_str(119, 18,  row["IDNO1"])
            put_str(137, 18,  row["IDNO2"])
            # @0152 CONTACT1 overwrites bytes 152-166 (SAS column-pointer behaviour)
            put_str(152, 15,  row["CONTACT1"])
            put_str(167, 15,  row["CONTACT2"])
            put_str(182, 130, row["ADDRESS"],    upcase=True)
            put_str(312, 5,   row["NATINLTY"])
            put_str(317, 12,  row["FAC_DATE"])
            put_str(329, 3,   row["STATUS"])
            put_str(332, 10,  row["CAPACITY"])
            put_str(342, 5,   row["LENDTYPE"])
            put_str(347, 3,   row["MY_FRGN"])
            put_str(350, 18,  row["FACILITY"])
            put_num(368, 18,  row["LIMIT"])
            put_num(386, 18,  row["OS_BAL"])
            put_str(404, 12,  row["B_UPDATE"])
            put_num(416, 18,  row["TOT_OS"])
            put_num(434, 18,  row["TOTLIMIT"])
            put_num(452, 5,   row["FROM_YR"])
            put_num(457, 5,   row["TO_YR"])
            put_str(462, 13,  row["MTH_ODR"])
            put_str(475, 13,  row["COND_ACC"])

            fh.write(buf.decode("utf-8", errors="replace") + "\n")

    log.info("MARGOUT written: %s  (%d records)", path, len(rows))


# ---------------------------------------------------------------------------
# STEP 2 (EIBMMGN2) – Re-read MARGOUT, write Parquet dataset SHAREM<YY><MM><DD>
# ---------------------------------------------------------------------------
# SAS reads MARGOUT back with the same PUT-layout offsets (DATA MARGIN3),
# then writes MARGIN1.SHAREM<YY><MM><DD> keeping all fields.

def read_margout(path: Path) -> list[dict]:
    """Re-read MARGOUT fixed-width flat file, mirroring DATA MARGIN3 INPUT layout."""
    rows = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n\r").ljust(487)

            # Read fields using same 1-based column positions as MARGOUT write layout
            rgncode  = line[0:8]                   # @0001 $8.
            bscgroup_raw = line[8:13]              # @0009  5.
            aa_no    = line[13:33]                 # @0014 $20.
            crrgrade = line[33:38]                 # @0034 $UPCASE5.  (already uppercased)
            name     = line[38:118]                # @0039 $UPCASE80.
            idno1    = line[118:136]               # @0119 $18.
            idno2    = line[136:154]               # @0137 $18.
            contact1 = line[151:166]               # @0152 $15.
            contact2 = line[166:181]               # @0167 $15.
            address  = line[181:311]               # @0182 $UPCASE130.
            natinlty = line[311:316]               # @0312 $5.
            fac_date = line[316:328]               # @0317 $12.
            status   = line[328:331]               # @0329 $3.
            capacity = line[331:341]               # @0332 $10.
            lendtype = line[341:346]               # @0342 $5.
            my_frgn  = line[346:349]               # @0347 $3.
            facility = line[349:367]               # @0350 $18.
            limit_raw   = line[367:385]            # @0368 18.
            os_bal_raw  = line[385:403]            # @0386 18.
            b_update    = line[403:415]            # @0404 $12.
            tot_os_raw  = line[415:433]            # @0416 18.
            totlimit_raw= line[433:451]            # @0434 18.
            from_yr_raw = line[451:456]            # @0452  5.
            to_yr_raw   = line[456:461]            # @0457  5.
            mth_odr  = line[461:474]               # @0462 $13.
            cond_acc = line[474:487]               # @0475 $13.

            def safe_num(s: str) -> float | None:
                s = s.strip()
                try:
                    return float(s) if s else None
                except ValueError:
                    return None

            rows.append({
                "RGNCODE"  : rgncode,
                "BSCGROUP" : safe_num(bscgroup_raw),
                "AA_NO"    : aa_no,
                "CRRGRADE" : crrgrade,
                "NAME"     : name,
                "IDNO1"    : idno1,
                "IDNO2"    : idno2,
                "CONTACT1" : contact1,
                "CONTACT2" : contact2,
                "ADDRESS"  : address,
                "NATINLTY" : natinlty,
                "FAC_DATE" : fac_date,
                "STATUS"   : status,
                "CAPACITY" : capacity,
                "LENDTYPE" : lendtype,
                "MY_FRGN"  : my_frgn,
                "FACILITY" : facility,
                "LIMIT"    : safe_num(limit_raw),
                "OS_BAL"   : safe_num(os_bal_raw),
                "B_UPDATE" : b_update,
                "TOT_OS"   : safe_num(tot_os_raw),
                "TOTLIMIT" : safe_num(totlimit_raw),
                "FROM_YR"  : safe_num(from_yr_raw),
                "TO_YR"    : safe_num(to_yr_raw),
                "MTH_ODR"  : mth_odr,
                "COND_ACC" : cond_acc,
            })

    log.info("MARGOUT re-read records: %d", len(rows))
    return rows


def build_sharem_df(rows: list[dict]) -> pl.DataFrame:
    """Convert row dicts from MARGOUT into a typed Polars DataFrame.

    Mirrors the KEEP statement in DATA MARGIN1.SHAREM<YY><MM><DD>:
      KEEP RGNCODE BSCGROUP AA_NO CRRGRADE NAME IDNO1 IDNO2 CONTACT1
           CONTACT2 ADDRESS NATINLTY FAC_DATE STATUS CAPACITY
           LENDTYPE MY_FRGN FACILITY LIMIT OS_BAL B_UPDATE TOT_OS
           TOTLIMIT FROM_YR TO_YR MTH_ODR COND_ACC;
    """
    schema = {
        "RGNCODE"  : pl.Utf8,
        "BSCGROUP" : pl.Float64,
        "AA_NO"    : pl.Utf8,
        "CRRGRADE" : pl.Utf8,
        "NAME"     : pl.Utf8,
        "IDNO1"    : pl.Utf8,
        "IDNO2"    : pl.Utf8,
        "CONTACT1" : pl.Utf8,
        "CONTACT2" : pl.Utf8,
        "ADDRESS"  : pl.Utf8,
        "NATINLTY" : pl.Utf8,
        "FAC_DATE" : pl.Utf8,
        "STATUS"   : pl.Utf8,
        "CAPACITY" : pl.Utf8,
        "LENDTYPE" : pl.Utf8,
        "MY_FRGN"  : pl.Utf8,
        "FACILITY" : pl.Utf8,
        "LIMIT"    : pl.Float64,
        "OS_BAL"   : pl.Float64,
        "B_UPDATE" : pl.Utf8,
        "TOT_OS"   : pl.Float64,
        "TOTLIMIT" : pl.Float64,
        "FROM_YR"  : pl.Float64,
        "TO_YR"    : pl.Float64,
        "MTH_ODR"  : pl.Utf8,
        "COND_ACC" : pl.Utf8,
    }
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema)


# ---------------------------------------------------------------------------
# PROC CPORT / FTP STUB
# ---------------------------------------------------------------------------
# SAS:
#   FILENAME TRANFILE 'SAP.PBB.MARGIFTP' DISP=OLD;
#   PROC CPORT LIBRARY=MARGIN1 FILE=TRANFILE;
#
# PROC CPORT serialises the MARGIN1 library to the transport file
# SAP.PBB.MARGIFTP for subsequent SFTP transfer to the EDW landing zone.
# This is a mainframe-only infrastructure operation.  In the Python
# pipeline the output Parquet file (SHAREM<YY><MM><DD>.parquet) serves
# as the equivalent and should be transferred to the EDW via the
# appropriate modern file-transfer mechanism (SFTP / Azure Blob / etc.).


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("EIBMMRGN started.")

    # --- Step 0: Read report date from DATEFILE (shared by both SAS steps) ---
    reptdate, reptyear, reptmon, reptday = read_reptdate(DATEFILE_PATH)

    # --- Step 1 (EIBMMRGN): Read MARGTXT and write MARGOUT ---
    log.info("Reading MARGTXT: %s", MARGTXT_PATH)
    margin_rows = read_margtxt(MARGTXT_PATH)

    log.info("Writing MARGOUT: %s", MARGOUT_PATH)
    write_margout(margin_rows, MARGOUT_PATH)

    # --- Step 2 (EIBMMGN2): Re-read MARGOUT, write Parquet dataset ---
    log.info("Re-reading MARGOUT for dataset creation ...")
    margin3_rows = read_margout(MARGOUT_PATH)

    df = build_sharem_df(margin3_rows)

    # Dataset name mirrors SAS: MARGIN1.SHAREM&REPTYEAR&REPTMON&REPTDAY
    dataset_name = f"SHAREM{reptyear}{reptmon}{reptday}"
    parquet_path = OUTPUT_DIR / f"{dataset_name}.parquet"
    df.write_parquet(parquet_path)
    log.info("Parquet dataset written: %s  (%d rows)", parquet_path, len(df))

    # --- PROC CPORT / FTP: not replicated — see stub comment above ---

    log.info("EIBMMRGN completed successfully.")


if __name__ == "__main__":
    main()
