#!/usr/bin/env python3
"""
Program  : EIBWSIBC.py
Purpose  : Build ELDS SIBC (BNMSIBC) warehouse dataset from two ELDS text
           files (BNMSIBC1 and BNMSIBC2), validate their extract dates,
           enrich with branch reference data, and write a named Parquet
           dataset (SIBC<MM><YY><WK>) for EDW consumption.

SAS Original : EIBWSIBC
SMR          : 2008-147 (ELDS FOLDER)
Migrated to  : Python / Polars / DuckDB

External Dependencies (JCL):
  FTPEL31D, FTPEL31C, FTPEL31B, FTPEL31A  (upstream FTP jobs, not replicated)

Inputs  : ELDSTXT  - SAP.PBB.ELDS.BNMSIBC1.TEXT(0)  (fixed-width text)
          ELDSTX2  - SAP.PBB.ELDS.BNMSIBC2.TEXT(0)  (fixed-width text)
          BRH      - SAP.RBP2.B033.PBB.BRANCH        (fixed-width text)

Outputs : SIBC<MM><YY><WK>.parquet  - <OUTPUT_DIR>/SIBC<MM><YY><WK>.parquet
                                       Named warehouse dataset (Parquet).

Notes   :
  - REPTDATE is derived from TODAY() using week-band logic (no file input).
  - YEARCUTOFF=1950 applies to all two-digit year parsing.
  - $UPCASE informats applied at read time via .upper().
  - COMMA15./COMMA25./COMMA11. informats: strip commas, parse as float.
  - TRUNCOVER on ELDSTXT: short lines are padded rather than causing errors.
  - ELN1/ELN2 are sorted and merged by AANO+STATUS; NODUPKEY dedup applied.
  - AMOUNT logic: for STATUS='APPLIED', if AMOUNT (=CHNGLMT) is null/0,
    fall back to AMOUNTX.
  - BRH merge: keep only SIBC records that match a branch (IN=A semantics).
  - PROC CPORT / FTP transfer is a mainframe infrastructure operation and
    is not replicated here; see stub below.
  - ELDS2.SIBC<...> (SAP.PBB.SIBC.SBDATAWH) is the permanent warehouse
    copy; represented as the single output Parquet file.
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

ELDSTXT_PATH = INPUT_DIR / "BNMSIBC1.TEXT"       # SAP.PBB.ELDS.BNMSIBC1.TEXT(0)
ELDSTX2_PATH = INPUT_DIR / "BNMSIBC2.TEXT"       # SAP.PBB.ELDS.BNMSIBC2.TEXT(0)
BRH_PATH     = INPUT_DIR / "BRANCH.txt"           # SAP.RBP2.B033.PBB.BRANCH

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


def parse_ddmmyy8(text: str) -> date | None:
    """Parse DDMMYY8. format (e.g. '01032009').

    SAS YEARCUTOFF=1950: two-digit years >= 50 map to 1950-1999;
    00-49 map to 2000-2049.
    """
    text = text.strip()
    if len(text) < 6:
        return None
    try:
        dd = int(text[0:2])
        mm = int(text[2:4])
        yy_raw = text[4:]
        if len(yy_raw) == 2:
            yy = int(yy_raw)
            year = (1900 + yy) if yy >= 50 else (2000 + yy)
        else:
            year = int(yy_raw)
        return date(year, mm, dd)
    except (ValueError, IndexError):
        return None


def parse_ddmmyy10(text: str) -> date | None:
    """Parse DDMMYY10. format (e.g. '01/03/2009' or '01-03-2009')."""
    text = text.strip()
    if not text:
        return None
    # Remove common separators: / - .
    cleaned = text.replace("/", "").replace("-", "").replace(".", "")
    return parse_ddmmyy8(cleaned)


def mdy(mm: int | None, dd: int | None, yy: int | None) -> date | None:
    """SAS MDY(month, day, year) equivalent. Returns None on invalid inputs."""
    if mm is None or dd is None or yy is None:
        return None
    try:
        return date(yy, mm, dd)
    except ValueError:
        return None


def safe_int(s: str, width: int | None = None) -> int | None:
    """Parse a fixed-width numeric string as integer; return None if blank/invalid."""
    val = s.strip() if s else ""
    if not val:
        return None
    try:
        return int(val)
    except ValueError:
        return None


def safe_comma_float(s: str) -> float | None:
    """Parse a COMMA-informat string (strip commas) as float."""
    cleaned = s.replace(",", "").strip() if s else ""
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# STEP 0 – Derive REPTDATE and macro variables from TODAY()
# ---------------------------------------------------------------------------
# SAS:
#   DATA REPTDATE;
#      SELECT;
#         WHEN( 8 <= DAY(TODAY()) <= 14) → REPTDATE = 8th of current month;  WK='1'
#         WHEN(15 <= DAY(TODAY()) <= 21) → REPTDATE = 15th of current month; WK='2'
#         WHEN(22 <= DAY(TODAY()) <= 27) → REPTDATE = 22nd of current month; WK='3'
#         OTHERWISE                      → REPTDATE = 1st of current month - 1 day;
#                                          (last day of previous month);     WK='4'
#      END;
#   CALL SYMPUT('NOWK',    PUT(WK,$1.));
#   CALL SYMPUT('RDATE',   PUT(REPTDATE, DDMMYY8.));
#   CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE), Z2.));
#   CALL SYMPUT('REPTYEAR',PUT(REPTDATE, YEAR2.));

def derive_reptdate() -> tuple[date, str, str, str, str]:
    """Derive REPTDATE and macro variables from today's date.

    Returns (reptdate, nowk, rdate, reptmon, reptyear).
    """
    today = date.today()
    day   = today.day

    if 8 <= day <= 14:
        reptdate = date(today.year, today.month, 8)
        wk = "1"
    elif 15 <= day <= 21:
        reptdate = date(today.year, today.month, 15)
        wk = "2"
    elif 22 <= day <= 27:
        reptdate = date(today.year, today.month, 22)
        wk = "3"
    else:
        # First of current month minus 1 day = last day of previous month
        reptdate = date(today.year, today.month, 1) - timedelta(days=1)
        wk = "4"

    nowk     = wk
    rdate    = reptdate.strftime("%d%m%Y")          # DDMMYY8. → 'DDMMYYYY'
    reptmon  = str(reptdate.month).zfill(2)         # Z2.
    reptyear = str(reptdate.year)[-2:].zfill(2)     # YEAR2.

    log.info(
        "REPTDATE=%s  NOWK=%s  RDATE=%s  REPTMON=%s  REPTYEAR=%s",
        reptdate.isoformat(), nowk, rdate, reptmon, reptyear,
    )
    return reptdate, nowk, rdate, reptmon, reptyear


# ---------------------------------------------------------------------------
# STEP 1 – Read BRH branch reference file
# ---------------------------------------------------------------------------
# SAS:
#   DATA BRH (DROP=BRSTAT);
#      INFILE BRH;
#      INPUT @02 BRANCH 3.
#            @06 BRCD   $3.
#            @50 BRSTAT $1.;
#      IF BRSTAT = 'C' THEN DELETE;

def read_brh(path: Path) -> pl.DataFrame:
    """Read fixed-width BRH branch file and return DataFrame with BRANCH and BRCD.

    Layout (1-based):
      @02 BRANCH 3.   → cols  2-4   width 3  (numeric)
      @06 BRCD   $3.  → cols  6-8   width 3  (string)
      @50 BRSTAT $1.  → col  50     width 1  (string; filtered out on 'C')
    """
    rows = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n\r").ljust(50)

            brstat = line[49:50]          # @50 $1.  (0-based: 49)
            # IF BRSTAT = 'C' THEN DELETE
            if brstat.strip() == "C":
                continue

            branch_raw = line[1:4]        # @02  3.  (0-based: 1:4)
            brcd       = line[5:8]        # @06 $3.  (0-based: 5:8)

            branch = safe_int(branch_raw)

            rows.append({
                "BRANCH" : branch,
                "BRCD"   : brcd,
            })

    schema = {"BRANCH": pl.Int64, "BRCD": pl.Utf8}
    if not rows:
        return pl.DataFrame(schema=schema)
    df = pl.DataFrame(rows, schema=schema)
    log.info("BRH records loaded (excl. BRSTAT=C): %d", len(df))
    return df


# ---------------------------------------------------------------------------
# STEP 2 – Extract date from first record of ELDSTXT / ELDSTX2
# ---------------------------------------------------------------------------
# SAS:
#   DATA ELDSDT1(DROP=DD MM YY);
#      INFILE ELDSTXT OBS=1;
#      INPUT @053 DD 2.
#            @056 MM 2.
#            @059 YY 4.;
#      ELDSDT1 = MDY(MM,DD,YY);
# Identical logic for ELDSTX2 → ELDSDT2.

def extract_elds_date(path: Path, label: str) -> date | None:
    """Read the first record of an ELDS text file and extract its extract date.

    Layout (1-based):
      @053 DD 2.  → cols 53-54  (0-based: 52:54)
      @056 MM 2.  → cols 56-57  (0-based: 55:57)
      @059 YY 4.  → cols 59-62  (0-based: 58:62)
    """
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        first_line = fh.readline().rstrip("\n\r").ljust(62)

    dd_raw = first_line[52:54]   # @053 DD 2.
    mm_raw = first_line[55:57]   # @056 MM 2.
    yy_raw = first_line[58:62]   # @059 YY 4.

    dd = safe_int(dd_raw)
    mm = safe_int(mm_raw)
    yy = safe_int(yy_raw)

    result = mdy(mm, dd, yy)
    log.info("%s extract date: %s", label, result.isoformat() if result else "(invalid)")
    return result


# ---------------------------------------------------------------------------
# STEP 3 – Read ELN1 from ELDSTXT (BNMSIBC1)
# ---------------------------------------------------------------------------
# SAS INPUT layout (1-based, FIRSTOBS=2, TRUNCOVER):
#   @001 AANO      $UPCASE13.     cols   1-13   width 13
#   @080 BRCD      $3.            cols  80-82   width  3
#   @087 FACCODE   $10.           cols  87-96   width 10
#   @100 FACILI    $30.           cols 100-129  width 30
#   @133 AMOUNTX   COMMA15.       cols 133-147  width 15
#   @151 BNMEFF    $8.            cols 151-158  width  8
#   @162 APPRIC    $UPCASE200.    cols 162-361  width 200
#   @365 AMTAPPLY  COMMA15.       cols 365-379  width 15
#   @383 AVPRIC    $UPCASE200.    cols 383-582  width 200
#   @586 PRICING   $8.            cols 586-593  width  8
#   @597 NEWIC     $12.           cols 597-608  width 12
#   @612 CPARTY    $3.            cols 612-614  width  3
#   @618 LNTYPE    $UPCASE15.     cols 618-632  width 15
#   @636 GINCOME   COMMA15.       cols 636-650  width 15
#   @651 SPAAMT    COMMA15.       cols 651-665  width 15
#   @666 CPRELAT   $UPCASE100.    cols 666-765  width 100
#   @766 CPRELAS   $UPCASE100.    cols 766-865  width 100
#   @890 CPSTAFF   $UPCASE50.     cols 890-939  width 50
#   @940 CPDITOR   $UPCASE3.      cols 940-942  width  3
#   @943 CPSTFID   $UPCASE5.      cols 943-947  width  5
#   @948 CPBRHO    $UPCASE11.     cols 948-958  width 11
#   @992 STATUS    $UPCASE25.     cols 992-1016 width 25

def read_eln1(path: Path) -> pl.DataFrame:
    """Read ELN1 records from ELDSTXT (BNMSIBC1), FIRSTOBS=2, TRUNCOVER."""
    rows = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        lines = fh.readlines()

    # FIRSTOBS=2 → skip line index 0
    for raw_line in lines[1:]:
        # TRUNCOVER: short lines are padded (no error for lines shorter than max col)
        line = raw_line.rstrip("\n\r").ljust(1016)

        aano     = line[0:13].upper()        # @001 $UPCASE13.
        brcd     = line[79:82]               # @080 $3.
        faccode  = line[86:96]               # @087 $10.
        facili   = line[99:129]              # @100 $30.
        amountx  = safe_comma_float(line[132:147])   # @133 COMMA15.
        bnmeff   = line[150:158]             # @151 $8.
        appric   = line[161:361].upper()     # @162 $UPCASE200.
        amtapply = safe_comma_float(line[364:379])   # @365 COMMA15.
        avpric   = line[382:582].upper()     # @383 $UPCASE200.
        pricing  = line[585:593]             # @586 $8.
        newic    = line[596:608]             # @597 $12.
        cparty   = line[611:614]             # @612 $3.
        lntype   = line[617:632].upper()     # @618 $UPCASE15.
        gincome  = safe_comma_float(line[635:650])   # @636 COMMA15.
        spaamt   = safe_comma_float(line[650:665])   # @651 COMMA15.
        cprelat  = line[665:765].upper()     # @666 $UPCASE100.
        cprelas  = line[765:865].upper()     # @766 $UPCASE100.
        cpstaff  = line[889:939].upper()     # @890 $UPCASE50.
        cpditor  = line[939:942].upper()     # @940 $UPCASE3.
        cpstfid  = line[942:947].upper()     # @943 $UPCASE5.
        cpbrho   = line[947:958].upper()     # @948 $UPCASE11.
        status   = line[991:1016].upper()    # @992 $UPCASE25.

        rows.append({
            "AANO"    : aano,
            "BRCD"    : brcd,
            "FACCODE" : faccode,
            "FACILI"  : facili,
            "AMOUNTX" : amountx,
            "BNMEFF"  : bnmeff,
            "APPRIC"  : appric,
            "AMTAPPLY": amtapply,
            "AVPRIC"  : avpric,
            "PRICING" : pricing,
            "NEWIC"   : newic,
            "CPARTY"  : cparty,
            "LNTYPE"  : lntype,
            "GINCOME" : gincome,
            "SPAAMT"  : spaamt,
            "CPRELAT" : cprelat,
            "CPRELAS" : cprelas,
            "CPSTAFF" : cpstaff,
            "CPDITOR" : cpditor,
            "CPSTFID" : cpstfid,
            "CPBRHO"  : cpbrho,
            "STATUS"  : status,
        })

    schema = {
        "AANO"    : pl.Utf8, "BRCD"    : pl.Utf8, "FACCODE" : pl.Utf8,
        "FACILI"  : pl.Utf8, "AMOUNTX" : pl.Float64, "BNMEFF"  : pl.Utf8,
        "APPRIC"  : pl.Utf8, "AMTAPPLY": pl.Float64, "AVPRIC"  : pl.Utf8,
        "PRICING" : pl.Utf8, "NEWIC"   : pl.Utf8, "CPARTY"  : pl.Utf8,
        "LNTYPE"  : pl.Utf8, "GINCOME" : pl.Float64, "SPAAMT"  : pl.Float64,
        "CPRELAT" : pl.Utf8, "CPRELAS" : pl.Utf8, "CPSTAFF" : pl.Utf8,
        "CPDITOR" : pl.Utf8, "CPSTFID" : pl.Utf8, "CPBRHO"  : pl.Utf8,
        "STATUS"  : pl.Utf8,
    }
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    log.info("ELN1 records read: %d", len(df))
    return df


# ---------------------------------------------------------------------------
# STEP 4 – Read ELN2 from ELDSTX2 (BNMSIBC2)
# ---------------------------------------------------------------------------
# SAS INPUT layout (1-based, FIRSTOBS=2):
#   @001 AANO       $UPCASE13.       cols   1-13
#   @017 DD           2.             cols  17-18
#   @020 MM           2.             cols  20-21
#   @023 YY           4.             cols  23-26
#   @030 FELIMIT    COMMA15.         cols  30-44
#   @048 TRLIMIT    COMMA15.         cols  48-62
#   @066 CUSTCODE     4.             cols  66-69
#   @073 SECTOR       4.             cols  73-76
#   @080 PCODCRIS     4.             cols  80-83
#   @087 PCODFISS     4.             cols  87-90
#   @094 SMESIZE      3.             cols  94-96
#   @100 NOEMPLO      4.             cols 100-103
#   @107 TURNOVER   COMMA11.         cols 107-117
#   @121 SDD           2.            cols 121-122  (SUBMISSION date parts)
#   @124 SMM           2.            cols 124-125
#   @127 SYY           4.            cols 127-130
#   @134 BDD           2.            cols 134-135  (BR FULL DOC RECEIVE date parts)
#   @137 BMM           2.            cols 137-138
#   @140 BYY           4.            cols 140-143
#   @147 RDD           2.            cols 147-148  (DESPATCH date parts)
#   @150 RMM           2.            cols 150-151
#   @153 RYY           4.            cols 153-156
#   @160 IDD           2.            cols 160-161  (DECISION date parts)
#   @163 IMM           2.            cols 163-164
#   @166 IYY           4.            cols 166-169
#   @173 LDD           2.            cols 173-174  (LOLR date parts)
#   @176 LMM           2.            cols 176-177
#   @179 LYY           4.            cols 179-182
#   @186 ADD           2.            cols 186-187  (DATE APPLIED/ACCEPTED BY)
#   @189 AMM           2.            cols 189-190
#   @192 AYY           4.            cols 192-195
#   @199 PDD           2.            cols 199-200  (APVDTE1)
#   @202 PMM           2.            cols 202-203
#   @205 PYY           4.            cols 205-208
#   @212 APVBY        $UPCASE60.     cols 212-271  width 60
#   @275 P2DD          2.            cols 275-276  (APVDTE2)
#   @278 P2MM          2.            cols 278-279
#   @281 P2YY          4.            cols 281-284
#   @288 APVBY2       $UPCASE60.     cols 288-347  width 60
#   @348 APVDES1      $UPCASE25.     cols 348-372  width 25
#   @373 APVDES2      $UPCASE25.     cols 373-397  width 25
#   @398 REASONS      $UPCASE200.    cols 398-597  width 200
#   @598 ICREASON     $UPCASE9.      cols 598-606  width  9
#   @622 DD4           2.            cols 622-623  (CONFIRM date)
#   @625 MM4           2.            cols 625-626
#   @628 YY4           4.            cols 628-631
#   @635 SMENAME1     $UPCASE60.     cols 635-694  width 60
#   @695 SMENAME2     $UPCASE60.     cols 695-754  width 60
#   @755 TRANBR       $3.            cols 755-757  width  3
#   @758 TRANBRNO      3.            cols 758-760  width  3
#   @761 TRANREG      $4.            cols 761-764  width  4
#   @780 ADVANCES     $1.            col  780      width  1
#   @784 PRODUCT      $3.            cols 784-786  width  3
#   @790 STATE        $3.            cols 790-792  width  3
#   @796 EXSTLMT     COMMA15.        cols 796-810  width 15
#   @814 CHNGLMT     COMMA15.        cols 814-828  width 15
#   @832 GREENTCO    $1.             col  832      width  1
#   @836 BIOTCO      $1.             col  836      width  1
#   @840 SMEIP       $1.             col  840      width  1
#   @844 SME1INCR    $1.             col  844      width  1
#   @848 SMEMSC      $1.             col  848      width  1
#   @852 STRUPCO_2YR $1.             col  852      width  1
#   @856 STATUS      $UPCASE25.      cols 856-880  width 25
#   @884 CTRY_INCORP $UPCASE2.       cols 884-885  width  2
#   @908 STRUPCO_3YR $2.             cols 908-909  width  2
#   @913 HDD          2.             cols 913-914  (HO FULL DOC RECEIVE date)
#   @916 HMM          2.             cols 916-917
#   @919 HYY          4.             cols 919-922
#   @926 NAME        $UPCASE150.     cols 926-1075 width 150
#   @1079 LN_UTILISE_LOCAT_CD $2.    cols 1079-1080 width 2
#   @1084 NEW_BUSS_REG_ID 12.        cols 1084-1095 width 12
#   @1099 CLIMATE_PRIN_TAXONOMY_CLASS $5.  cols 1099-1103 width 5
#   @1107 SOURCE_INCOME_CURRENCY_CD   $3.  cols 1107-1109 width 3
#   @1113 GRP_ANNL_SALES_FINANCIAL_DT DDMMYY10.  cols 1113-1122 width 10
#   @1126 GRP_ANNL_SALES_AMT          COMMA25.   cols 1126-1150 width 25
#
# Derived fields (after INPUT):
#   AMOUNT   = CHNGLMT
#   AADATE   = MDY(AMM, ADD, AYY)
#   SBDATE   = MDY(SMM, SDD, SYY)
#   DPDATE   = MDY(RMM, RDD, RYY)
#   IDDATE   = MDY(IMM, IDD, IYY)
#   LODATE   = MDY(LMM, LDD, LYY)
#   CMDATE   = MDY(MM4, DD4, YY4)
#   APVDTE1  = MDY(PMM, PDD, PYY)
#   APVDTE2  = MDY(P2MM, P2DD, P2YY)
#   BR_FULL_DOC_RECEIVE_DT  = MDY(BMM, BDD, BYY)
#   HOE_FULL_DOC_RECEIVE_DT = MDY(HMM, HDD, HYY)
#   DROP all raw date component fields.

def read_eln2(path: Path) -> pl.DataFrame:
    """Read ELN2 records from ELDSTX2 (BNMSIBC2), FIRSTOBS=2."""
    rows = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        lines = fh.readlines()

    # FIRSTOBS=2 → skip line index 0
    for raw_line in lines[1:]:
        line = raw_line.rstrip("\n\r").ljust(1150)

        # --- Read all raw fields ---
        aano        = line[0:13].upper()                   # @001 $UPCASE13.

        # AADATE components (DATE APPLIED/ACCEPTED BY)
        add_raw     = line[185:187]                        # @186 ADD 2.
        amm_raw     = line[188:190]                        # @189 AMM 2.
        ayy_raw     = line[191:195]                        # @192 AYY 4.

        felimit     = safe_comma_float(line[29:44])        # @030 COMMA15.
        trlimit     = safe_comma_float(line[47:62])        # @048 COMMA15.
        custcode    = safe_int(line[65:69])                # @066 4.
        sector      = safe_int(line[72:76])                # @073 4.
        pcodcris    = safe_int(line[79:83])                # @080 4.
        pcodfiss    = safe_int(line[86:90])                # @087 4.
        smesize     = safe_int(line[93:96])                # @094 3.
        noemplo     = safe_int(line[99:103])               # @100 4.
        turnover    = safe_comma_float(line[106:117])      # @107 COMMA11.

        # SBDATE: SUBMISSION DATE
        sdd_raw     = line[120:122]                        # @121 SDD 2.
        smm_raw     = line[123:125]                        # @124 SMM 2.
        syy_raw     = line[126:130]                        # @127 SYY 4.

        # BR_FULL_DOC_RECEIVE_DT
        bdd_raw     = line[133:135]                        # @134 BDD 2.
        bmm_raw     = line[136:138]                        # @137 BMM 2.
        byy_raw     = line[139:143]                        # @140 BYY 4.

        # DPDATE: DESPATCH DATE
        rdd_raw     = line[146:148]                        # @147 RDD 2.
        rmm_raw     = line[149:151]                        # @150 RMM 2.
        ryy_raw     = line[152:156]                        # @153 RYY 4.

        # IDDATE: DECISION DATE
        idd_raw     = line[159:161]                        # @160 IDD 2.
        imm_raw     = line[162:164]                        # @163 IMM 2.
        iyy_raw     = line[165:169]                        # @166 IYY 4.

        # LODATE: LOLR DATE
        ldd_raw     = line[172:174]                        # @173 LDD 2.
        lmm_raw     = line[175:177]                        # @176 LMM 2.
        lyy_raw     = line[178:182]                        # @179 LYY 4.

        # APVDTE1
        pdd_raw     = line[198:200]                        # @199 PDD 2.
        pmm_raw     = line[201:203]                        # @202 PMM 2.
        pyy_raw     = line[204:208]                        # @205 PYY 4.

        apvby       = line[211:271].upper()                # @212 $UPCASE60.

        # APVDTE2
        p2dd_raw    = line[274:276]                        # @275 P2DD 2.
        p2mm_raw    = line[277:279]                        # @278 P2MM 2.
        p2yy_raw    = line[280:284]                        # @281 P2YY 4.

        apvby2      = line[287:347].upper()                # @288 $UPCASE60.
        apvdes1     = line[347:372].upper()                # @348 $UPCASE25.
        apvdes2     = line[372:397].upper()                # @373 $UPCASE25.
        reasons     = line[397:597].upper()                # @398 $UPCASE200.
        icreason    = line[597:606].upper()                # @598 $UPCASE9.

        # CMDATE: CONFIRM DATE
        dd4_raw     = line[621:623]                        # @622 DD4 2.
        mm4_raw     = line[624:626]                        # @625 MM4 2.
        yy4_raw     = line[627:631]                        # @628 YY4 4.

        smename1    = line[634:694].upper()                # @635 $UPCASE60.
        smename2    = line[694:754].upper()                # @695 $UPCASE60.
        tranbr      = line[754:757]                        # @755 $3.
        tranbrno    = safe_int(line[757:760])              # @758  3.
        tranreg     = line[760:764]                        # @761 $4.
        advances    = line[779:780]                        # @780 $1.
        product     = line[783:786]                        # @784 $3.
        state       = line[789:792]                        # @790 $3.
        exstlmt     = safe_comma_float(line[795:810])      # @796 COMMA15.
        chnglmt     = safe_comma_float(line[813:828])      # @814 COMMA15.
        greentco    = line[831:832]                        # @832 $1.
        biotco      = line[835:836]                        # @836 $1.
        smeip       = line[839:840]                        # @840 $1.
        sme1incr    = line[843:844]                        # @844 $1.
        smemsc      = line[847:848]                        # @848 $1.
        strupco_2yr = line[851:852]                        # @852 $1.
        status      = line[855:880].upper()                # @856 $UPCASE25.
        ctry_incorp = line[883:885].upper()                # @884 $UPCASE2.
        strupco_3yr = line[907:909]                        # @908 $2.

        # HOE_FULL_DOC_RECEIVE_DT
        hdd_raw     = line[912:914]                        # @913 HDD 2.
        hmm_raw     = line[915:917]                        # @916 HMM 2.
        hyy_raw     = line[918:922]                        # @919 HYY 4.

        name        = line[925:1075].upper()               # @926 $UPCASE150.
        ln_utilise_locat_cd         = line[1078:1080]      # @1079 $2.
        new_buss_reg_id_raw         = line[1083:1095]      # @1084 12.
        climate_prin_taxonomy_class = line[1098:1103]      # @1099 $5.
        source_income_currency_cd   = line[1106:1109]      # @1107 $3.
        grp_annl_sales_financial_dt_raw = line[1112:1122]  # @1113 DDMMYY10.
        grp_annl_sales_amt = safe_comma_float(line[1125:1150])  # @1126 COMMA25.

        # --- Parse numeric fields ---
        new_buss_reg_id = safe_int(new_buss_reg_id_raw)

        # GRP_ANNL_SALES_FINANCIAL_DT: DDMMYY10. informat
        grp_annl_sales_financial_dt = parse_ddmmyy10(grp_annl_sales_financial_dt_raw)

        # --- Derive date fields via MDY() (DROP raw components per SAS DROP list) ---
        aadate   = mdy(safe_int(amm_raw), safe_int(add_raw), safe_int(ayy_raw))
        sbdate   = mdy(safe_int(smm_raw), safe_int(sdd_raw), safe_int(syy_raw))
        dpdate   = mdy(safe_int(rmm_raw), safe_int(rdd_raw), safe_int(ryy_raw))
        iddate   = mdy(safe_int(imm_raw), safe_int(idd_raw), safe_int(iyy_raw))
        lodate   = mdy(safe_int(lmm_raw), safe_int(ldd_raw), safe_int(lyy_raw))
        cmdate   = mdy(safe_int(mm4_raw), safe_int(dd4_raw), safe_int(yy4_raw))
        apvdte1  = mdy(safe_int(pmm_raw), safe_int(pdd_raw), safe_int(pyy_raw))
        apvdte2  = mdy(safe_int(p2mm_raw), safe_int(p2dd_raw), safe_int(p2yy_raw))
        br_full_doc_receive_dt  = mdy(safe_int(bmm_raw), safe_int(bdd_raw), safe_int(byy_raw))
        hoe_full_doc_receive_dt = mdy(safe_int(hmm_raw), safe_int(hdd_raw), safe_int(hyy_raw))

        # AMOUNT = CHNGLMT  (raw; AMOUNT logic finalised during SIBC merge step)
        amount = chnglmt

        rows.append({
            "AANO"                      : aano,
            "FELIMIT"                   : felimit,
            "TRLIMIT"                   : trlimit,
            "CUSTCODE"                  : custcode,
            "SECTOR"                    : sector,
            "PCODCRIS"                  : pcodcris,
            "PCODFISS"                  : pcodfiss,
            "SMESIZE"                   : smesize,
            "NOEMPLO"                   : noemplo,
            "TURNOVER"                  : turnover,
            "APVBY"                     : apvby,
            "APVBY2"                    : apvby2,
            "APVDES1"                   : apvdes1,
            "APVDES2"                   : apvdes2,
            "REASONS"                   : reasons,
            "ICREASON"                  : icreason,
            "SMENAME1"                  : smename1,
            "SMENAME2"                  : smename2,
            "TRANBR"                    : tranbr,
            "TRANBRNO"                  : tranbrno,
            "TRANREG"                   : tranreg,
            "ADVANCES"                  : advances,
            "PRODUCT"                   : product,
            "STATE"                     : state,
            "EXSTLMT"                   : exstlmt,
            "GREENTCO"                  : greentco,
            "BIOTCO"                    : biotco,
            "SMEIP"                     : smeip,
            "SME1INCR"                  : sme1incr,
            "SMEMSC"                    : smemsc,
            "STRUPCO_2YR"               : strupco_2yr,
            "STATUS"                    : status,
            "CTRY_INCORP"               : ctry_incorp,
            "STRUPCO_3YR"               : strupco_3yr,
            "NAME"                      : name,
            "LN_UTILISE_LOCAT_CD"       : ln_utilise_locat_cd,
            "NEW_BUSS_REG_ID"           : new_buss_reg_id,
            "CLIMATE_PRIN_TAXONOMY_CLASS": climate_prin_taxonomy_class,
            "SOURCE_INCOME_CURRENCY_CD" : source_income_currency_cd,
            "GRP_ANNL_SALES_FINANCIAL_DT": grp_annl_sales_financial_dt,
            "GRP_ANNL_SALES_AMT"        : grp_annl_sales_amt,
            # Derived date fields
            "AMOUNT"                    : amount,
            "AADATE"                    : aadate,
            "SBDATE"                    : sbdate,
            "DPDATE"                    : dpdate,
            "IDDATE"                    : iddate,
            "LODATE"                    : lodate,
            "CMDATE"                    : cmdate,
            "APVDTE1"                   : apvdte1,
            "APVDTE2"                   : apvdte2,
            "BR_FULL_DOC_RECEIVE_DT"    : br_full_doc_receive_dt,
            "HOE_FULL_DOC_RECEIVE_DT"   : hoe_full_doc_receive_dt,
        })

    schema = {
        "AANO": pl.Utf8, "FELIMIT": pl.Float64, "TRLIMIT": pl.Float64,
        "CUSTCODE": pl.Int64, "SECTOR": pl.Int64, "PCODCRIS": pl.Int64,
        "PCODFISS": pl.Int64, "SMESIZE": pl.Int64, "NOEMPLO": pl.Int64,
        "TURNOVER": pl.Float64, "APVBY": pl.Utf8, "APVBY2": pl.Utf8,
        "APVDES1": pl.Utf8, "APVDES2": pl.Utf8, "REASONS": pl.Utf8,
        "ICREASON": pl.Utf8, "SMENAME1": pl.Utf8, "SMENAME2": pl.Utf8,
        "TRANBR": pl.Utf8, "TRANBRNO": pl.Int64, "TRANREG": pl.Utf8,
        "ADVANCES": pl.Utf8, "PRODUCT": pl.Utf8, "STATE": pl.Utf8,
        "EXSTLMT": pl.Float64, "GREENTCO": pl.Utf8, "BIOTCO": pl.Utf8,
        "SMEIP": pl.Utf8, "SME1INCR": pl.Utf8, "SMEMSC": pl.Utf8,
        "STRUPCO_2YR": pl.Utf8, "STATUS": pl.Utf8, "CTRY_INCORP": pl.Utf8,
        "STRUPCO_3YR": pl.Utf8, "NAME": pl.Utf8,
        "LN_UTILISE_LOCAT_CD": pl.Utf8, "NEW_BUSS_REG_ID": pl.Int64,
        "CLIMATE_PRIN_TAXONOMY_CLASS": pl.Utf8, "SOURCE_INCOME_CURRENCY_CD": pl.Utf8,
        "GRP_ANNL_SALES_FINANCIAL_DT": pl.Date, "GRP_ANNL_SALES_AMT": pl.Float64,
        "AMOUNT": pl.Float64, "AADATE": pl.Date, "SBDATE": pl.Date,
        "DPDATE": pl.Date, "IDDATE": pl.Date, "LODATE": pl.Date,
        "CMDATE": pl.Date, "APVDTE1": pl.Date, "APVDTE2": pl.Date,
        "BR_FULL_DOC_RECEIVE_DT": pl.Date, "HOE_FULL_DOC_RECEIVE_DT": pl.Date,
    }
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    log.info("ELN2 records read: %d", len(df))
    return df


# ---------------------------------------------------------------------------
# STEP 5 – Merge ELN1 + ELN2, apply AMOUNT logic, dedup, join BRH
# ---------------------------------------------------------------------------
# SAS:
#   PROC SORT DATA=ELN1 OUT=ELN1; BY AANO STATUS;
#   PROC SORT DATA=ELN2 OUT=ELN2; BY AANO STATUS;
#   DATA SIBC;
#        MERGE ELN1 ELN2; BY AANO STATUS;
#        IF STATUS='APPLIED' THEN DO;
#           IF AMOUNT IN (.,0) THEN AMOUNT=AMOUNTX;
#        END;
#        DROP AMOUNTX CHNGLMT;
#   PROC SORT DATA=SIBC NODUPKEY; BY AANO STATUS;
#   PROC SORT; BY BRCD;
#   PROC SORT DATA=BRH; BY BRCD;
#   DATA SIBC;
#        MERGE SIBC(IN=A) BRH; BY BRCD;
#        IF A;
#        DROP BRCD;
#   PROC SORT DATA=SIBC OUT=ELDS1.SIBC...; BY AANO STATUS;

def build_sibc(
    eln1: pl.DataFrame,
    eln2: pl.DataFrame,
    brh: pl.DataFrame,
) -> pl.DataFrame:
    """Merge ELN1 and ELN2, apply AMOUNT logic, dedup, and join BRH.

    Replicates the SAS MERGE + PROC SORT NODUPKEY + BRH merge pipeline.
    """
    # Sort both by AANO, STATUS before merge (PROC SORT before MERGE)
    eln1_sorted = eln1.sort(["AANO", "STATUS"])
    eln2_sorted = eln2.sort(["AANO", "STATUS"])

    # SAS MERGE ELN1 ELN2 BY AANO STATUS:
    # Both datasets are sorted by the BY variables; matching rows are combined,
    # non-matching rows are retained with missing values from the absent dataset.
    # Polars full outer join on AANO+STATUS replicates this.
    sibc = eln1_sorted.join(
        eln2_sorted,
        on=["AANO", "STATUS"],
        how="full",
        suffix="_eln2",
        coalesce=True,
    )

    # AMOUNT logic:
    # IF STATUS='APPLIED' THEN DO;
    #    IF AMOUNT IN (.,0) THEN AMOUNT=AMOUNTX;
    # END;
    # AMOUNT came from ELN2 (=CHNGLMT); AMOUNTX came from ELN1.
    sibc = sibc.with_columns(
        pl.when(
            (pl.col("STATUS") == "APPLIED") &
            (pl.col("AMOUNT").is_null() | (pl.col("AMOUNT") == 0))
        )
        .then(pl.col("AMOUNTX"))
        .otherwise(pl.col("AMOUNT"))
        .alias("AMOUNT")
    )

    # DROP AMOUNTX CHNGLMT (CHNGLMT was already aliased as AMOUNT in ELN2,
    # so it is not a separate column; drop AMOUNTX only)
    sibc = sibc.drop(["AMOUNTX"])

    # PROC SORT NODUPKEY BY AANO STATUS
    # keep='first' after sort replicates NODUPKEY (first occurrence retained)
    sibc = sibc.sort(["AANO", "STATUS"]).unique(
        subset=["AANO", "STATUS"], keep="first", maintain_order=True
    )

    # PROC SORT SIBC BY BRCD; PROC SORT BRH BY BRCD;
    # DATA SIBC; MERGE SIBC(IN=A) BRH; BY BRCD; IF A; DROP BRCD;
    # → left join SIBC onto BRH by BRCD; keep only SIBC rows that match BRH
    sibc_with_brh = sibc.join(
        brh.drop("BRCD") if "BRCD" in brh.columns else brh,
        left_on="BRCD",
        right_on="BRCD" if "BRCD" in brh.columns else None,
        how="inner",    # IF A: keep only SIBC rows that have a matching BRH row
    )

    # Replicate the BRH join with BRCD available in BRH for the join key
    # Re-do: join SIBC by BRCD onto BRH; IF A means inner join; DROP BRCD after
    sibc_with_brh = sibc.join(
        brh,
        on="BRCD",
        how="inner",    # IF A: retain only rows where SIBC matches BRH (IN=A)
    ).drop("BRCD")

    # Final sort: PROC SORT DATA=SIBC OUT=ELDS1.SIBC...; BY AANO STATUS;
    sibc_final = sibc_with_brh.sort(["AANO", "STATUS"])

    log.info("SIBC records after merge + dedup + BRH join: %d", len(sibc_final))
    return sibc_final


# ---------------------------------------------------------------------------
# PROC CPORT / FTP STUB
# ---------------------------------------------------------------------------
# SAS:
#   FILENAME TRANFILE 'SAP.PBB.SIBC.SBDATAWH.SBFTP' DISP=OLD;
#   PROC CPORT LIBRARY=ELDS1 FILE=TRANFILE;
#   RUN;
#
# PROC CPORT serialises the ELDS1 library (containing SIBC<MM><YY><WK>)
# to the transport file SAP.PBB.SIBC.SBDATAWH.SBFTP for subsequent SFTP
# transfer to the EDW landing zone.
# This is a mainframe-only infrastructure operation.  In the Python
# pipeline the output Parquet file serves as the equivalent and should
# be transferred to the EDW via the appropriate modern file-transfer
# mechanism (SFTP / Azure Blob / etc.).
#
# ELDS2.SIBC<...> (SAP.PBB.SIBC.SBDATAWH) is the permanent warehouse
# library copy; represented here by the single output Parquet file.


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("EIBWSIBC started.")

    # --- Step 0: Derive REPTDATE from TODAY() ---
    reptdate, nowk, rdate, reptmon, reptyear = derive_reptdate()

    # --- Step 1: Read BRH branch reference ---
    log.info("Reading BRH: %s", BRH_PATH)
    brh_df = read_brh(BRH_PATH)

    # --- Step 2: Extract and log ELDS file dates ---
    log.info("Extracting date from ELDSTXT: %s", ELDSTXT_PATH)
    eldsdt1 = extract_elds_date(ELDSTXT_PATH, "ELDSDT1")

    log.info("Extracting date from ELDSTX2: %s", ELDSTX2_PATH)
    eldsdt2 = extract_elds_date(ELDSTX2_PATH, "ELDSDT2")

    # PROC PRINT DATA=ELDSDT2 FORMAT ELDSDT2 DATE8. — diagnostic only, not replicated

    # --- Step 3: Read ELN1 ---
    log.info("Reading ELN1 from ELDSTXT: %s", ELDSTXT_PATH)
    eln1_df = read_eln1(ELDSTXT_PATH)

    # --- Step 4: Read ELN2 ---
    log.info("Reading ELN2 from ELDSTX2: %s", ELDSTX2_PATH)
    eln2_df = read_eln2(ELDSTX2_PATH)

    # --- Step 5: Build SIBC via merge + dedup + BRH join ---
    log.info("Building SIBC dataset ...")
    sibc_df = build_sibc(eln1_df, eln2_df, brh_df)

    # Dataset name mirrors SAS: SIBC&REPTMON&REPTYEAR&NOWK
    dataset_name = f"SIBC{reptmon}{reptyear}{nowk}"
    parquet_path = OUTPUT_DIR / f"{dataset_name}.parquet"

    # Write ELDS1.SIBC<...> and ELDS2.SIBC<...> (both the same data;
    # represented by a single Parquet output)
    sibc_df.write_parquet(parquet_path)
    log.info("Parquet dataset written: %s  (%d rows)", parquet_path, len(sibc_df))

    # --- PROC CPORT / FTP: not replicated — see stub comment above ---

    log.info("EIBWSIBC completed successfully.")


if __name__ == "__main__":
    main()
