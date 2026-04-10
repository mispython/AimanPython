# !/usr/bin/env python3
"""
Program Name : EICRWELN.py
Purpose      : CRMS_eLDS (Weekly) - Read multiple fixed-width and CSV text input files
               (ELN01W to ELN33W), parse and transform each dataset, deduplicate,
               add sequence numbers where required, and write output parquet files
               named with the report date suffix. Also produces a CPORT-equivalent
               combined binary output file (TRANFILE / ELNFTPW).
"""

import os
import re
import struct
import pickle
from datetime import date, datetime

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
INPUT_DIR    = os.environ.get("INPUT_DIR",  "input")   # directory holding ELN##W.txt files
OUTPUT_DIR   = os.environ.get("OUTPUT_DIR", "output")  # directory for output parquet files
TRANFILE_PATH = os.environ.get("TRANFILE_PATH", os.path.join(OUTPUT_DIR, "ELNFTPW.dat"))

# Input text file paths (fixed-width or comma-delimited depending on dataset)
INPUT_FILES = {i: os.path.join(INPUT_DIR, f"ELN{i:02d}W.txt") for i in range(1, 34)}

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# HELPER UTILITIES
# ---------------------------------------------------------------------------

def _read_lines(path: str) -> list[str]:
    """Read all lines from a text file, returning them as a list."""
    with open(path, "r", encoding="latin-1") as fh:
        return fh.readlines()


def _slice(line: str, start: int, length: int) -> str:
    """Extract a substring from a fixed-width line (1-based SAS column positions)."""
    s = start - 1          # convert to 0-based
    raw = line[s:s + length] if len(line) >= s + length else line[s:].ljust(length)
    return raw


def _str(line: str, start: int, length: int) -> str:
    """Extract and uppercase a string field."""
    return _slice(line, start, length).strip().upper()


def _num(line: str, start: int, length: int) -> float | None:
    """Extract a plain numeric field (no comma formatting)."""
    raw = _slice(line, start, length).strip()
    if raw == "" or raw == ".":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _comma_num(line: str, start: int, length: int) -> float | None:
    """Extract a comma-formatted numeric field."""
    raw = _slice(line, start, length).replace(",", "").strip()
    if raw == "" or raw == ".":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _ddmmyy(line: str, start: int) -> date | None:
    """Parse a DDMMYY10. formatted date field (DD/MM/YYYY or DD-MM-YYYY)."""
    raw = _slice(line, start, 10).strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    return None


def _mdy(mm, dd, yy) -> date | None:
    """Construct a date from month, day, year (mirroring SAS MDY())."""
    try:
        if mm is None or dd is None or yy is None:
            return None
        m = int(mm); d = int(dd); y = int(yy)
        if m == 0 or d == 0 or y == 0:
            return None
        return date(y, m, d)
    except (ValueError, TypeError):
        return None


def _dedup(df: pl.DataFrame) -> pl.DataFrame:
    """Remove duplicate rows (NODUP equivalent)."""
    return df.unique()


def _write_parquet(df: pl.DataFrame, name: str) -> str:
    path = os.path.join(OUTPUT_DIR, f"{name}.parquet")
    df.write_parquet(path)
    return path


# ---------------------------------------------------------------------------
# STEP 1 – REPTDATE: read report date from first line of ELN01W
# ---------------------------------------------------------------------------

def read_reptdate() -> tuple[str, str]:
    """
    Read first line of ELDSAA1 (ELN01W.txt) to extract REPTDATE from column 53.
    Returns (DSNDT, RPTDT) macro-variable equivalents.
    """
    lines = _read_lines(INPUT_FILES[1])
    if not lines:
        raise ValueError("ELN01W.txt is empty")
    header = lines[0]
    reptdate = _ddmmyy(header, 53)
    if reptdate is None:
        raise ValueError(f"Cannot parse REPTDATE from header: {header!r}")
    dsndt  = f"{reptdate.day:02d}"           # PUT(DAY(REPTDATE),Z2.)
    rptdt  = reptdate.strftime("%Y%m%d")     # PUT(REPTDATE,YYMMDDN8.)
    return dsndt, rptdt


# ---------------------------------------------------------------------------
# DATASET READERS
# ---------------------------------------------------------------------------

def read_elna1(dsndt: str) -> pl.DataFrame:
    """
    ELNA1 – main application data (ELN01W.txt, FIRSTOBS=2, fixed-width).
    Drops intermediate date-part columns and derives calculated date fields.
    """
    lines = _read_lines(INPUT_FILES[1])
    rows = []
    for line in lines[1:]:           # FIRSTOBS=2
        line = line.rstrip("\n")
        if not line:
            continue

        dd  = _num(line, 136, 2);  mm  = _num(line, 139, 2);  yy  = _num(line, 142, 4)
        dd1 = _num(line, 371, 2);  mm1 = _num(line, 374, 2);  yy1 = _num(line, 377, 4)
        dd2 = _num(line, 475, 2);  mm2 = _num(line, 478, 2);  yy2 = _num(line, 481, 4)
        dd3 = _num(line, 579, 2);  mm3 = _num(line, 582, 2);  yy3 = _num(line, 585, 4)
        dd4 = _num(line, 660, 2);  mm4 = _num(line, 663, 2);  yy4 = _num(line, 666, 4)
        dd5 = _num(line, 841, 2);  mm5 = _num(line, 844, 2);  yy5 = _num(line, 847, 4)
        dd6 = _num(line, 854, 2);  mm6 = _num(line, 857, 2);  yy6 = _num(line, 860, 4)
        dd7 = _num(line, 867, 2);  mm7 = _num(line, 870, 2);  yy7 = _num(line, 873, 4)

        crrbrch = _str(line, 1016, 10)
        crrcod  = _str(line, 1029, 10)
        # CRR GRADE derivation
        if crrcod:
            crrgrade = crrcod.replace(" ", "")
        else:
            crrgrade = crrbrch.replace(" ", "")
        crrgrade = crrgrade[:5]  # LENGTH CRRGRADE $5.

        #  *AADATE = MDY(MM,DD,YY)  -- commented out in original
        lodate   = _mdy(mm1, dd1, yy1)
        apvdte1  = _mdy(mm2, dd2, yy2)
        apvdte2  = _mdy(mm3, dd3, yy3)
        icdate   = _mdy(mm4, dd4, yy4)
        lobefudt = _mdy(mm5, dd5, yy5)
        lobeapdt = _mdy(mm6, dd6, yy6)
        lobelodt = _mdy(mm7, dd7, yy7)
        aadate   = icdate   # AADATE = ICDATE

        rows.append({
            "AANO"      : _str(line,   1, 13),
            "NAME"      : _str(line,  17, 60),
            "BRANCH"    : _num(line,  95,  4),
            "CUSTCODE"  : _num(line, 102,  4),
            "SECTOR"    : _num(line, 109,  4),
            "STATE"     : _num(line, 116,  3),
            "PRODUCT"   : _num(line, 122,  3),
            "PRICING"   : _num(line, 128,  5),   # 5.2 – read as plain float
            "AMOUNT"    : _comma_num(line, 149, 15),
            "NOEMPLO"   : _num(line, 167,  4),
            "SECURITY"  : _num(line, 174,  1),
            "ADVANCES"  : _str(line, 178,  1),
            "PRODESC"   : _str(line, 182, 30),
            "PRESRATE"  : _str(line, 215,  7),
            "INSTRLF"   : _str(line, 225,  3),
            "COMPLETE"  : _str(line, 231, 11),
            "SMENAME1"  : _str(line, 245, 60),
            "SMENAME2"  : _str(line, 308, 60),
            "APVDES1"   : _str(line, 384, 25),
            "APVNME1"   : _str(line, 412, 60),
            "APVDES2"   : _str(line, 488, 25),
            "APVNME2"   : _str(line, 516, 60),
            "PCODCRIS"  : _str(line, 592,  4),   # PURPOSE CODE CCRIS
            "PCODFISS"  : _str(line, 599,  4),   # PURPOSE CODE FISS
            "TURNOVR1"  : _comma_num(line, 606, 11),
            "SMESIZE"   : _num(line, 620,  2),
            "CRRSCORE"  : _str(line, 631,  3),
            "SPAAMT"    : _comma_num(line, 637, 11),
            "EXEMPIND"  : _str(line, 651,  1),
            "ELDSPROD"  : _str(line, 655,  2),
            "ICREASON"  : _str(line, 673,  9),
            "MAKE"      : _str(line, 685, 20),
            "YEARMADE"  : _num(line, 708,  4),
            "VECHAGE"   : _str(line, 715, 30),
            "RATE"      : _num(line, 748, 10),   # 10.8
            "TERM"      : _num(line, 761,  3),
            "CASHPRIC"  : _comma_num(line, 767, 15),
            "ACCTNO"    : _num(line, 785, 10),   # MNI LOAN A/C NO
            "INTEARN2"  : _num(line, 798, 15),
            "TRANBR"    : _str(line, 816,  3),   # TRANSFER BR ABBR
            "TRANBRNO"  : _num(line, 822,  3),   # TRANSFER BR NO
            "TRANREG"   : _str(line, 828,  4),   # TRANSFER REGION
            "LOBE"      : _str(line, 835,  3),   # LOBE INDICATOR
            "MARGIN"    : _str(line, 880,  6),   # MARGIN OF ADVANCE
            "PMAGNNO"   : _str(line, 889,  8),   # PUBLIC MUTUAL AGENT CODE
            "PMAGNNM"   : _str(line, 900, 50),   # PUBLIC MUTUAL AGENT NAME
            "GINCOME"   : _comma_num(line, 953, 11),  # GROSS INCOME
            "PRITYPE"   : _str(line, 967,  5),   # TYPE OF PRICING
            "PRIRATE"   : _num(line, 975,  5),   # INTEREST RATE (5.2)
            "STATUS"    : _str(line, 983, 30),
            "NEWIC"     : _str(line, 1042, 17),  # NEW IC
            "HDLGFEE"   : _str(line, 1065, 15),  # HANDLING FEE - GST
            "HPPREMLIFE": _str(line, 1083, 15),  # HP PREMIUM LIFE
            "AACRTDBY"  : _str(line, 1101, 60),  # AA CREATED BY (MOBILE)
            "AACRTDTM"  : _str(line, 1164, 15),  # AA CREATED TIME (MOBILE)
            "AASBMTDBY" : _str(line, 1182, 60),  # AA SUBMITTED BY (MOBILE)
            "AASBMTDTM" : _str(line, 1245, 15),  # AA SUBMITTED TIME (MOBILE)
            "CRRGRADE"  : crrgrade,              # CRR GRADE (derived)
            "SECTORCODE": _str(line, 1271,  6),  # SECTOR CODE
            "SECTORDESC": _str(line, 1280, 200), # SECTOR CODE DESCRIPTION
            "SUBSECTOR1": _str(line, 1483, 160), # DESCRIPTION SUBSECTOR CODE
            "SUBSECTOR2": _str(line, 1646, 160), # DESCRIPTION SUBSECTOR CODE
            # Derived date fields
            "AADATE"    : aadate,
            "LODATE"    : lodate,
            "APVDTE1"   : apvdte1,
            "APVDTE2"   : apvdte2,
            "ICDATE"    : icdate,
            "LOBEFUDT"  : lobefudt,  # LOBE DATE OF FULL DOC.
            "LOBEAPDT"  : lobeapdt,  # LOBE DATE APR
            "LOBELODT"  : lobelodt,  # LOBE DATE LO
        })

    df = pl.DataFrame(rows)
    return _dedup(df)


def read_elna2() -> pl.DataFrame:
    """ELNA2 – reference / connected party data (ELN02W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[2])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "AANO"     : _str(line,   1, 13),
            "REFTYPE"  : _str(line,  17, 70),
            "NMREF1"   : _str(line,  90, 60),
            "NMREF2"   : _str(line, 153, 60),
            "CPARTY"   : _str(line, 216,  3),   # CONNECTED PARTIES
            "CPSTAFF"  : _str(line, 222, 50),   # CP STAFF NAME
            "CPDITOR"  : _str(line, 275,  3),   # CP BOARD DIRECTOR
            "CPSTFID"  : _str(line, 281,  5),   # CP STAFF ID
            "CPBRHO"   : _str(line, 289, 11),   # CP BRANCH/HO
            "CPBRID"   : _str(line, 303,  3),   # CP BRANCH ID
            "CPHQDIV"  : _str(line, 309, 30),   # CP HO DIVISION
            "CPRELAT"  : _str(line, 342,100),   # CP RELATIVES
            "CPRELAS"  : _str(line, 445,100),   # CP RELATIONSHIP
            "NEWESIT"  : _str(line, 548,  8),   # NEW/EXISTING
            "AMTAPPLY" : _comma_num(line, 559, 15),  # AMOUNT APPLIED
            "LNTERM"   : _num(line, 577,  3),   # LOAN TERMS
            "APPRIC"   : _str(line, 583,200),   # APPLIED PRICING
            "AVPRIC"   : _str(line, 786,200),   # APPROVED PRICING
            "REINPROD"  : _str(line, 989,  1),  # ADDITIONAL FACILITIES
            "STATUS2"  : _str(line, 993, 30),
        })
    return _dedup(pl.DataFrame(rows))


def read_elna3() -> pl.DataFrame:
    """ELNA3 – insurance / CRR / ASCORE data (ELN03W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[3])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "AANO"          : _str(line,    1, 13),  # AA NUM
            "MAANO"         : _str(line,   17, 13),  # MAIN AA NUM
            "APVBY"         : _str(line,   33,  2),  # APPBRHO
            "MRTAIND"       : _str(line,   38, 15),  # MRTA INDICATOR
            "NOMRTA"        : _str(line,   56,  2),  # MRTA NO OF POLICY
            "MRTAPREM"      : _comma_num(line, 61, 15),  # TOTAL PREMIUM MRTA
            "MRTAOPT"       : _str(line,   79, 15),  # FINANCE OPTION MRTA
            "MRTAINS"       : _str(line,   97, 30),  # MRTA INSURANCE COMPANY
            "ODLFIN"        : _str(line,  130, 15),  # FINANCE OPTION ODL
            "ODLPREM"       : _comma_num(line,148, 15),  # TOTAL PREMIUM ODL
            "ODLINS"        : _str(line,  166, 30),  # ODL INSURANCE COMPANY
            "MRPAFIN"       : _str(line,  199, 15),  # FINANCE OPTION MRPA
            "MRPAPREM"      : _comma_num(line,217, 15),  # TOTAL PREMIUM MRPA
            "MRPAINS"       : _str(line,  235, 30),  # MRPA INSURANCE COMPANY
            "BLDIND"        : _str(line,  268,  3),  # BLDTA INDICATOR
            "BLOIND"        : _str(line,  274,  3),  # BLOLTA INDICATOR
            "BLDCASE"       : _str(line,  280,  2),  # BLDTA NO OF INSURANCE
            "BLOCASE"       : _str(line,  285,  2),  # BLOLTA NO OF INSURANCE
            "BLDPREM"       : _comma_num(line,290, 15),  # BLDTA TOTAL PREMIUM
            "BLOPREM"       : _comma_num(line,308, 15),  # BLOLTA TOTAL PREMIUM
            "BLDINS"        : _str(line,  326, 30),  # BLDTA INSURANCE COMPANY
            "BLOINS"        : _str(line,  359, 30),  # BLOLTA INSURANCE COMPANY
            "CCIND"         : _str(line,  392,  3),  # CRECARD INDICATOR
            "CCREA"         : _str(line,  398,200),  # REASON FOR NO CRECARD
            "REFINANC"      : _str(line,  601,  1),  # REFINANCE
            "SECTCD2"       : _str(line,  605,  8),  # SECCODE2
            "FDTAIND"       : _str(line,  616,  3),  # FBLDTA INDICATOR
            "FOTAIND"       : _str(line,  622,  3),  # FBLOLTA INDICATOR
            "FDTANUM"       : _str(line,  628,  2),  # NO. INS POL FOR FBLDTA
            "FOTANUM"       : _str(line,  633,  2),  # NO. INS POL FOR FBLODLTA
            "FBLDTAP"       : _comma_num(line,638, 15),  # PREM AMT FOR FBLDTA
            "FBLOLTAP"      : _comma_num(line,656, 15),  # PREM AMT FOR FBLODLTA
            "FDTAINS"       : _str(line,  674, 30),  # INS CO. INDI FOR BLDTA
            "FOTAINS"       : _str(line,  707, 30),  # INS CO. INDI FOR BLOLTA
            "HPDTAIND"      : _str(line,  740,  3),  # HPDTA INDICATOR
            "HPDTASUM"      : _str(line,  746, 12),  # HPDTA SUM INSURED
            "HPDTAPRE"      : _str(line,  761, 12),  # HPDTA GROSS PREMIUM
            "HPDTAINS"      : _str(line,  776,  2),  # HPDTA PERIOD INSURED
            "COLLCODE"      : _str(line,  781,  2),  # COLLATERAL CODE
            "STATUS3"       : _str(line,  786, 30),  # STATUS
            "REFAMT"        : _str(line,  819, 15),  # REFINANCING AMOUNT
            "BUSID"         : _str(line,  837, 15),  # BUSINESS ID
            "FACGPTYPE"     : _str(line,  855,  1),  # FACILITY GROUP TYPE
            "BUSTYPEID"     : _str(line,  859,  2),  # BUSINESS TYPE ID
            "APPAMTWSC"     : _str(line,  864, 15),  # APP AMOUNT(W'THOUT SC)
            "FACCODE"       : _str(line,  882,  5),  # FACILITY CODE NSRS
            "ABMHL"         : _str(line,  890,  1),  # ABM HOUSING LOAN<500K
            "FACNAME"       : _str(line,  894, 60),  # FACILITY NAME
            "AP1GRADE"      : _str(line,  957,  2),  # AP1 GRADE
            "AP2GRADE"      : _str(line,  962,  2),  # AP2 GRADE
            "SMESIZE3"      : _str(line,  967,  1),  # SMESIZE3
            "FACCODE2"      : _str(line,  971, 10),  # FACILITY CODE
            "GREENTCO"      : _str(line,  984,  1),  # GREENTECHCO
            "BIOTCO"        : _str(line,  988,  1),  # BIOTECHCO
            "SMEIP"         : _str(line,  992,  1),  # SMEIPRIGHTS
            "SME1INCR"      : _str(line,  996,  1),  # SME1INNOCERT
            "SMEMSC"        : _str(line, 1000,  1),  # SMEMSCSTATUS
            "STRUPCO"       : _str(line, 1004,  1),  # STARTUPCO
            "PURLNMOD"      : _str(line, 1008,  1),  # PURPLOANMOD
            "RRTYPE"        : _str(line, 1012,  1),  # REINSTATEMENT INDICATOR
            "VB"            : _str(line, 1016,  1),  # VULNERABLE BORROWERS
            "PRESCSTATE"    : _num(line,  1020,  2),  # PRESCRIBE STATE
            "CCRIS_STATUS"  : _str(line, 1025,  1),  # CCRIS STATUS
            "CRRSCRBRANCH"  : _comma_num(line,1029,10),  # CRR Score Latest
            "ASCGRADEBRANCH": _str(line, 1042, 10),  # ASCORE Grade Branch
            "ASCGRADEHO"    : _str(line, 1055, 10),  # ASCORE Grade HO
            "ASCSCORE"      : _comma_num(line,1068,10),  # ASCORE Score
            "ASCPD"         : _str(line, 1081, 10),  # ASCORE PD
            "ASCSEQ"        : _str(line, 1094, 40),  # ASCORE SEQ
            "ASCPROXY"      : _str(line, 1137,  1),  # ASCORE Proxy
            "ASCSCOREID"    : _str(line, 1141, 50),  # ASCORE Score ID
            "ASCDEBT"       : _comma_num(line,1194,10),  # ASCORE DEBT
            "ASCINCOME"     : _comma_num(line,1207,15),  # ASCORE INCOME
            "ASCDSR"        : _comma_num(line,1225,10),  # ASCORE DSR
            "ASCCRLIMIT"    : _comma_num(line,1238,15),  # ASCORE CREDIT LIMIT
            "ASCSECURITY"   : _comma_num(line,1256,15),  # ASCORE SECURITY
            "ASCMA"         : _comma_num(line,1274,10),  # ASCORE MA
            "ASCGRADELATEST": _str(line, 1287, 10),  # ASCORE GRADE
            "ASCDSR10YR"    : _str(line, 1300, 10),  # ASCORE DSR 10 Yr
            "ASCEXLGLACT"   : _str(line, 1313,  3),  # ASCORE ExistingLegalAction
            "ASCUNSATISCCRIS": _str(line,1319,  3),  # ASCORE UnsatisfactoryCCRIS
            "ASCRESCHEAKPK" : _str(line, 1325,  3),  # ASCORE RescheduleAKPK
            "ASCOUTSTANDGLG": _str(line, 1331,  3),  # ASCORE OutstandingLG
            "ASCSPECATTN"   : _str(line, 1337,  3),  # ASCORE SpecialAttention
            "FINALGRADE"    : _str(line, 1343,  7),  # FINAL GRADE
            "POBNEW"        : _str(line, 1353,  3),
            "POBPRIME"      : _str(line, 1359,  3),
            "POBSETUP"      : _str(line, 1365,  3),
            "POBDORMANT"    : _str(line, 1371,  3),
            "POBSPV"        : _str(line, 1377,  3),
            "POBNONPROORG"  : _str(line, 1383,  3),
            "POBMANUTEXT"   : _str(line, 1389,  3),
            "POBMAINCORE"   : _str(line, 1395,  3),
            "POBPROXY"      : _str(line, 1401,  1),
            "POBRELACC"     : _comma_num(line,1405,15),
        })
    return _dedup(pl.DataFrame(rows))


def read_elna4() -> pl.DataFrame:
    """ELNA4 – applicant personal / income data (ELN04W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[4])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        dobb = _num(line, 289, 2)
        domm = _num(line, 292, 2)
        doyy = _num(line, 295, 4)
        dbirth = _mdy(domm, dobb, doyy)

        rows.append({
            "MAANO"       : _str(line,    1, 13),
            "APVBY"       : _str(line,   70, 10),
            "AGEC"        : _num(line,   83,  3),
            "COUNTRY"     : _str(line,   89, 55),
            "ICPP"        : _str(line,  147, 33),
            "MARRIED"     : _str(line,  183, 10),
            "JOB"         : _str(line,  196, 40),
            "RENT"        : _str(line,  239, 20),
            "YEARSS"      : _str(line,  262, 20),
            "YESNO"       : _str(line,  285,  1),
            "GENDER"      : _str(line,  302,  1),
            "SALARY"      : _comma_num(line, 306, 15),
            "EXPEND"      : _comma_num(line, 324, 15),
            "EXPEND1"     : _comma_num(line, 342, 15),
            "EXPEND2"     : _comma_num(line, 360, 15),
            "EXPEND3"     : _comma_num(line, 378, 15),
            "BALANCE"     : _comma_num(line, 396, 15),
            "EMPNAME"     : _str(line,  414, 60),
            "EMPLOCAT"    : _str(line,  477,200),
            # @680 EMPNATUR $UPCASE10. -- commented out in original SAS
            "OWNBUS"      : _str(line,  693,  3),  # OWN BUSINESS
            "DOCSIGH"     : _str(line,  699,  3),  # DOCUMENTARY EVID SIGHTED
            "IDTYPE"      : _str(line,  705,  2),  # TYPE OF ID
            "GAINEMP"     : _str(line,  710, 20),  # GAINFULLY/SELF EMPLOYED
            "ORGTYPE"     : _str(line,  733, 95),  # ORGANISATION TYPE
            "GUARANT"     : _str(line,  831,  3),  # GUARANTOR
            "BRISCTOS"    : _str(line,  837, 20),  # BRIS/CTOS-GUARANTOR CRR
            "CCRISDC"     : _str(line,  860, 30),  # CCRIS/DCHEQS-GUARANTOR CCR
            "LEGALAC"     : _str(line,  893,  3),  # EXISTING LEGAL ACTION
            "TOLSCORE"    : _str(line,  899,  4),  # GUARANTOR CRR TOTAL SCORE
            "CRRVAL"      : _str(line,  906,  3),  # GUARANTOR CRR VALUE
            "CRRGRD"      : _str(line,  912,  2),  # GUARANTOR CRR GRADE
            "CTHOUSE"     : _str(line,  917, 11),  # CONTACT NUMBER (HOUSE)
            "CTOFFICE"    : _str(line,  931, 11),  # CONTACT NUMBER (OFFICE)
            "CTMOBILE"    : _str(line,  945, 11),  # CONTACT NUMBER (MOBILE)
            "EMAIL"       : _str(line,  959, 60),  # EMAIL
            "BSICSLRY"    : _str(line, 1022, 15),  # BASIC SALARY
            "FIXALLWN"    : _str(line, 1040, 15),  # FIXED ALLOWANCE
            "VRINCOME"    : _str(line, 1058, 15),  # VARIABLE INCOME
            "NVINCOME"    : _str(line, 1076, 15),  # NET VARIABLE INCOME
            "COMMISN"     : _str(line, 1094, 15),  # COMMISION
            "OTALLW"      : _str(line, 1112, 15),  # OVERTIME/ALLOWANCES
            "FEE"         : _str(line, 1130, 15),  # FEE
            "INTRSTMT"    : _str(line, 1148, 15),  # DIVIDEND/INTEREST STATEMENT
            "NONCTRBN"    : _str(line, 1166, 15),  # NON-CONTRACTUAL BONUS
            "L1YRTAXR"    : _str(line, 1184, 15),  # LATEST 1 YEAR TAX RETURNS
            "CASA1"       : _str(line, 1202, 15),  # CASA STATEMENT-6 MTH @20%
            "CASA2"       : _str(line, 1220, 15),  # CASA STATEMENT-6 MTH @5%
            "OTHINCOM"    : _str(line, 1238, 15),  # OTHER INCOME
            "RENTAL"      : _str(line, 1256, 15),  # RENTAL, ETC
            "PENSION"     : _str(line, 1274, 15),  # PENSIONS/ANNUITIES ETC
            "OVSINCOM"    : _str(line, 1292, 15),  # OVERSEA INCOME
            "TOTGROSI"    : _str(line, 1310, 15),  # TOTAL GROSS SUBSTANTIATED
            "STADEDUC"    : _str(line, 1328, 15),  # STATUTORY DEDUCTIONS
            "TAX"         : _str(line, 1346, 15),  # TAX
            "EMPLEPF"     : _str(line, 1364, 15),  # EMPLOYEE'S EPF
            "SOCSO"       : _str(line, 1382, 15),  # SOCSO
            "ZAKAT"       : _str(line, 1400, 15),  # ZAKAT
            "TSTADEDC"    : _str(line, 1418, 15),  # TOTAL STATUTORY DEDUCTION
            "TNETINCM"    : _str(line, 1436, 15),  # TOTAL NET INCOME AFTER SD
            "NETINCOM"    : _str(line, 1454, 15),  # NET INCOME
            "NDISPINC"    : _str(line, 1472, 15),  # NET DISPOSABLE INCOME
            "HPDTAIND"    : _str(line, 1490,  3),  # HPDTA/HPRTT INDICATOR
            "RESSTAT"     : _str(line, 1496,  3),  # PR STATUS
            "RESSTATIDTYP": _str(line, 1502,  2),  # PR STATUS ID TYPE
            "RESSTATID"   : _str(line, 1507, 33),  # PR STATUS ID
            "RACE"        : _str(line, 1543,  1),  # RACE
            "CUSTLOC"     : _str(line, 1547, 10),  # LOCATION OF CUSTOMER
            "RELATION"    : _str(line, 1560, 33),  # RELATIONSHIP
            "APPLNTTYPE"  : _str(line, 1596,  3),  # APPL.CHARGOR/PROPOSER?
            "JOBTYPE"     : _str(line, 1695,  3),  # CONTRACT/TEMPORARY/PART
            "BUMICOM"     : _str(line, 1701, 20),  # BUMI/NON-BUMI COMPANY
            "LOCEMPCNTRY" : _str(line, 1724, 50),  # LOCATION OF EMPLOYER'S
            "GUARACCTNO"  : _num(line,  1777, 15),  # GUARANTOR ACCT NUMBER
            "OCCTYPE"     : _str(line, 1795, 20),  # OCCUPATION TYPE
            "JOBNATURE"   : _str(line, 1844,150),  # NATURE OF JOB
            "YRSERPREVEMP": _num(line,  1997,  2),  # YR OF SERVICE WTH PRE EMP
            "GAPEMPPRECUR": _num(line,  2002,  3),  # EMP GAP BTW CUR N PREV EMP
            "POSTCODER"   : _str(line, 2008,  7),  # POSTCODE (RESIDENTIAL)
            "CITYR"       : _str(line, 2018, 47),  # CITY (RESIDENTIAL)
            "STATER"      : _str(line, 2068, 47),  # STATE (RESIDENTIAL)
            "POSTCODEE"   : _str(line, 2118,  7),  # POSTCODE (EMPLOYER)
            "CITYE"       : _str(line, 2128, 47),  # CITY (EMPLOYER)
            "STATEE"      : _str(line, 2178, 47),  # STATE (EMPLOYER)
            "BNMSTRDINCDOC": _str(line,2228,  3),
            "DEVEPF"      : _str(line, 2234, 60),
            "DEVCASE"     : _str(line, 2297, 60),
            "DEVOTHS"     : _str(line, 2360, 60),
            "DEVPAYSLIP36": _str(line, 2423, 15),
            "DEVPAYSLIPYN": _str(line, 2441,  1),
            "DSR"         : _num(line,  2445,  8),
            "JOINTINC"    : _comma_num(line, 2456, 15),
            "JOINTINCHN"  : _str(line, 2474, 40),
            "JOINTDSR"    : _num(line,  2517,  8),
            "GROSSINCAMEND": _comma_num(line, 2528, 15),
            "DSRAMEND"    : _num(line,  2546,  8),
            "ASCSCORE"    : _comma_num(line, 2557, 10),
            "ASCPD"       : _str(line, 2570, 10),
            "ASCSEQ"      : _str(line, 2583, 40),
            "ASCPROXY"    : _str(line, 2626,  1),
            "ASCSCOREID"  : _str(line, 2630, 50),
            "ASCDEBT"     : _comma_num(line, 2683, 10),
            "ASCINCOME"   : _comma_num(line, 2696, 15),
            "ASCDSR"      : _comma_num(line, 2714, 10),
            "ASCCRLIMIT"  : _comma_num(line, 2727, 15),
            "ASCSECURITY" : _comma_num(line, 2745, 15),
            "ASCMA"       : _comma_num(line, 2763, 10),
            "ASCGRADELATEST": _str(line,2776, 10),
            "FINALGRADE"  : _str(line, 2789,  7),
            "APPLNAME"    : _str(line, 2799,150),  # LONG NAME
            "APPTGI"      : _comma_num(line, 2952, 15),
            "APPTGIA"     : _comma_num(line, 2970, 15),
            "CYSIYEAR"    : _num(line,  2988,  2),
            "CYSIMON"     : _num(line,  2993,  2),
            "PYSIMON"     : _num(line,  2998,  2),
            "YRSIMON"     : _num(line,  3003,  2),
            "EMPNATUR"    : _str(line, 3008,200),
            "DBIRTH"      : dbirth,
        })
    return _dedup(pl.DataFrame(rows))


def read_elna5() -> pl.DataFrame:
    """ELNA5 – CRR data (ELN05W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[5])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"   : _str(line,   1, 13),  # MAIN AA NO
            "DEBTSR"  : _str(line,  17, 40),  # DEBT SERVICE RATIO
            "NETWRTH5": _str(line,  60, 30),  # NET WORTH
            "ADVMARG" : _str(line,  93, 30),  # MARGIN OF ADVANCE
            "YRGDCBOR": _str(line, 126, 50),  # YR GOOD CONDUCT-BOR.
            "YRGDCRBO": _str(line, 179, 50),  # YR GOOD CONDUCT-REL BOR
            "YRBUSSI" : _str(line, 232, 40),  # YR IN BUSINESS
            "MGMEXPR" : _str(line, 275, 40),  # MANAGEMENT EXPERIENCE
            "TYPEIND" : _str(line, 318, 30),  # TYPE OF INDUSTRY
            "TURNOVR5": _str(line, 351, 30),  # TURNOVER GROWTH
            "NETPROFT": _str(line, 384, 30),  # NET PROFIT MARGIN
            "ACIDTSTR": _str(line, 417, 30),  # ACID TEST RATIO
            "LEVRATN" : _str(line, 450, 30),  # LEVERAGE RATIO
            "INTCOVRA": _str(line, 483, 30),  # INT COVERAGE RATIO
            "COLLPERD": _str(line, 516, 30),  # COLLECTION PERIOD
            "CASHFLOW": _str(line, 549, 75),  # CURR YEAR NET OP CASH
            "ACCONDUC": _str(line, 627, 75),  # CCRIS SRCH-UNSAT.COND
            "RACCONDU": _str(line, 705, 75),  # CCRIS SRCH-UNSAT COND REL
            "CREDITBAL": _str(line,783, 30),  # AVG FOR LAST 3 MTH
            "MTHDEPO" : _str(line, 816, 30),  # AVG MTHLY DEP TO TURNOVR
            "MTHPRFT" : _str(line, 849, 30),  # AVG MTHLY PROFIT TO TURN
            "OWNERSH" : _str(line, 882, 30),  # HOME/BUSS PREMISE OWNRSHP
            "INCLVLPA": _str(line, 915, 30),  # INCOME LEVEL PER ANNUM
            "CONDUCT" : _str(line, 948, 30),  # CONDUCT OF ACCOUNT
            "FIRSTAA" : _str(line, 981,  5),  # 1ST AA CRR
            "CRRTOTSC": _str(line, 989,  5),  # CRR TOTAL SCORE
            "DSR"     : _str(line, 997,  5),  # DSR
        })
    return _dedup(pl.DataFrame(rows))


def read_elna6() -> pl.DataFrame:
    """ELNA6 – Financial computation (ELN06W.txt, fixed-width).
    Adds sequence number SEQNO per MAANO."""
    lines = _read_lines(INPUT_FILES[6])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        findd = _num(line, 17, 2)
        finmm = _num(line, 20, 2)
        finyy = _num(line, 23, 4)
        fyrend = _mdy(finmm, findd, finyy)
        curratio_raw = _num(line, 268, 15)
        curratio = round(curratio_raw, 2) if curratio_raw is not None else None

        rows.append({
            "MAANO"     : _str(line,   1, 13),  # MAIN AA NO
            "FYREND"    : fyrend,               # Derived from FINDD/FINMM/FINYY
            "AUMDIND"   : _str(line,  30,  1),  # AUD/UNAUD/MGM/DRAFT IND
            "TURNOVR6"  : _comma_num(line, 34, 15),  # TURNOVER
            "PRETAXPR"  : _comma_num(line, 52, 15),  # PRE TAX PROFIT
            "PAIDUP"    : _comma_num(line, 70, 15),  # PAID UP CAPITAL
            "NETWRTH6"  : _comma_num(line, 88, 15),  # NETWORTH
            "TOTASST"   : _comma_num(line,106, 15),  # TOTAL CURRENT ASSETS
            "TOTLIAB"   : _comma_num(line,124, 15),  # TOTAL CURRENT LIAB
            "NETWRKC"   : _comma_num(line,142, 15),  # NET WORKING CAPITAL
            "STOCKTN"   : _comma_num(line,160, 15),  # STOCK TURNOVER DAYS
            "COLLECTP"  : _comma_num(line,178, 15),  # COLLECTION PERIOD
            "PAYPERIO"  : _comma_num(line,196, 15),  # PAY PERIOD
            "SALESGR"   : _str(line, 214, 15),  # SALES GROWTH (%)
            "GROSSMA"   : _str(line, 232, 15),  # GROSS PROFIT MARG(%)
            "NETMARG"   : _str(line, 250, 15),  # NET PROFIT MARGIN
            "CURRATIO"  : curratio,             # CURRENT RATIO (rounded)
            "GEARG"     : _num(line, 286, 15),  # GEARING
            "LEVERG"    : _num(line, 304, 15),  # LEVERAGE
            "GROPRO"    : _str(line, 322, 15),  # GROSS PROFIT
            "DEFLIA"    : _str(line, 340, 15),  # DEFERRED LIABILITY
            "NOPECASH"  : _str(line, 358, 15),  # NET OPERATING CASHFLOW
            "LNGLIA"    : _str(line, 376, 15),  # LONG TERM LIABILITY
            "ENBFINT"   : _str(line, 394, 15),  # EARNING B4 INTEREST & TAX
            "INTEXP"    : _str(line, 412, 15),  # INTEREST EXPENSES
            "STOCK"     : _str(line, 430, 15),  # STOCK
            "TRADEB"    : _str(line, 448, 15),  # TRADE DEBTORS
            "LOANDIC"   : _str(line, 466, 15),  # LOAN DIRECTOR
            "INTCOMP"   : _str(line, 484, 15),  # INTER COMPANY LOAN
            "ACTRATIO"  : _str(line, 502, 15),  # ACID TEST RATIO
            "DEEQRATIO" : _str(line, 520, 15),  # DEBT/EQUITY RATIO
            "INTCOVRAT" : _str(line, 538, 15),  # INTEREST COVERAGE RATIO
            "LNFRDIR"   : _str(line, 556, 15),  # LOAN FROM DIRECTOR
            "REVALRSV"  : _str(line, 574, 15),  # REVALUATION RESERVE
            "UNSTFYADT" : _str(line, 592,120),  # UNSATISFACTORY AUDITOR
            "EXTORDGL"  : _str(line, 715, 15),  # EXTRAORDINARY GAIN/LOSS
            "DEVEXP"    : _str(line, 733, 15),  # DEVELOPMENT EXPENDITURE
            "TANGNET"   : _comma_num(line,751, 15),
            "NETWORKCAP": _comma_num(line,769, 15),
            "EDITDA"    : _comma_num(line,787, 15),
            "DEPRECIAT" : _comma_num(line,805, 15),
            "AMORTISTN" : _comma_num(line,823, 15),
            "TOTLOANCOM": _comma_num(line,841, 15),
            "TOTINTLOAN": _comma_num(line,859, 15),
            "PD"        : _comma_num(line,877, 15),
            "PCTLE"     : _comma_num(line,895, 15),
            "OPSTOCK"   : _comma_num(line,913, 15),
            "CLOSTOCK"  : _comma_num(line,931, 15),
            "PURCHASES" : _comma_num(line,949, 15),
            "COSTOFGOOD": _comma_num(line,967, 15),
            "WORKCAPREQ": _comma_num(line,985, 15),
            "TRACDTOR"  : _comma_num(line,1003,15),
            "TOTTGBLE"  : _comma_num(line,1021,15),
            "TOTLIAB2"  : _comma_num(line,1039,15),
            "AUDITOR"   : _str(line, 1057, 30),
            "ADTORREGNO": _str(line, 1090, 10),
            "DSR"       : _comma_num(line,1103,15),
        })

    df = pl.DataFrame(rows)
    df = df.unique()
    # Sort by MAANO then assign SEQNO
    df = df.sort("MAANO")
    seqno = []
    prev_maano = None
    seq = 0
    for maano in df["MAANO"].to_list():
        if maano != prev_maano:
            seq = 1
            prev_maano = maano
        else:
            seq += 1
        seqno.append(seq)
    df = df.with_columns(pl.Series("SEQNO", seqno, dtype=pl.Int16))
    return df


def read_elna7() -> pl.DataFrame:
    """ELNA7 – Security data (ELN07W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[7])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"    : _str(line,   1, 13),   # MAIN AA NO
            "AANOSEC7" : _str(line,  17, 13),   # AA NO
            "INDNEWE"  : _str(line,  33,  8),   # NEW / EXISTING IND
            "LANDAREA" : _str(line,  92,  9),   # LAND AREA
            "LNAREAUN" : _str(line, 104,  8),   # LAND AREA UNIT
            "BUILTUP"  : _str(line, 115,  9),   # BUILT UP
            "BLTUPUN"  : _str(line, 127,  8),   # BUILT UP UNIT
            "FREELEAS" : _str(line, 138,  9),   # FREEHOLD/LEASEHOLD
            "USEDSA"   : _str(line, 150, 18),   # CURRENTLY USED AS
            "LNUSDCAT" : _str(line, 171, 20),   # LAND USE CATEGORY
            "SPAPRCE"  : _comma_num(line, 207, 15),  # SPA PRICE
            "CMVALUE"  : _comma_num(line, 225, 15),  # CUR MARKET VAL (RM)?
            "FSVALUE"  : _comma_num(line, 243, 15),  # FORCE SALE VAL (RM)?
            "VALUERI"  : _str(line, 261, 12),   # VALUER INDICATOR
            "VALUENM"  : _str(line, 276, 40),   # VALUER NAME
            "APPRDEV"  : _str(line, 332,  3),   # APPROVED DEV./PRO/PHA
            "ADLREFNO" : _str(line, 338, 25),   # DEVELOPER'S NAME
            "DEVNAME"  : _str(line, 366, 60),   # DEVELOPER'S NAME
            "PROJNAME" : _str(line, 429, 60),   # PROJECT NAME
            "SELLCMV"  : _num(line,  531,  1),  # SELLING PRICE IS CMV
            "SELLCROS" : _num(line,  535,  1),  # SELLING PRICE CROSS CHECK
            "NOAGREE"  : _num(line,  539,  1),  # NUMBER OF AGREEMENT
            "MARCMV"   : _str(line, 543,  3),   # MARGIN AGAINST CMV(%)
            "ADDRESS1" : _str(line, 549, 10),   # HSE/LOT NO/TITLE NO
            "ADDRESS2" : _str(line, 562, 30),   # BUILDING
            "ADDRESS3" : _str(line, 595, 40),   # JLN / ST
            "ADDRESS4" : _str(line, 638, 40),   # TMN / SEK / PREC
            "ADDRESS5" : _str(line, 681, 30),   # ADDRESS CONTINUATION
            "ADDRESS6" : _str(line, 714,  5),   # POSTCODE
            "LOCPROP"  : _str(line, 722, 40),   # MUKIM/DAERAH
            "PCTCOMP"  : _num(line,  765,  3),  # PERCENTAGE COMPLETION
            "AREALOC"  : _str(line, 771, 30),   # AREA LOCATION
            "ACCTNOC1" : _num(line,  804, 10),  # ACCOUNT NO
            "ACCTNOC2" : _num(line,  817, 10),  # ACCOUNT NO 2
            "ACCTAPP"  : _str(line, 830,  1),   # PRIMARY ACCOUNT APP
            "CCOLLNO"  : _str(line, 834, 11),   # COLLATERAL NO.
            "CCLASSC"  : _str(line, 848,  3),   # SECURITY CLASS CODE
            "CINSTCL"  : _str(line, 854,  2),   # INSTRUMENT OF CLAIM
            "TTLPART"  : _str(line, 859, 12),   # TITLE PARTICULARS
            "TTLENO"   : _str(line, 874, 40),   # TITLE NO
            "MASTOWNR" : _str(line, 917, 40),   # NAME OF MAST.TTL.OWN
            "TTLID"    : _str(line, 960, 15),   # ID NO.MAST.TTL.OWNER
            "EXPDATE"  : _str(line, 978, 10),   # FREE/LEASE EXP DATE
            "CTRYSTAT" : _str(line, 991,  3),   # CURRENCY
            "CPRCHARG" : _str(line, 997, 15),   # AMOUNT OF CHARGE
            "CPRSHARE" : _str(line,1015,  3),   # SHARED
            "INSURER"  : _str(line,1021,  2),   # INSURER
            "CPOLYNUM" : _str(line,1026, 16),   # POLICY NO
            "FIREDATE" : _str(line,1045, 10),   # EXPIRY DATE
            "SUMINSUR" : _str(line,1058, 15),   # SUM INSURED
            "QUITRENT" : _str(line,1076,  4),   # LATEST QUIT RENT
            "ASSESSDT" : _str(line,1083,  4),   # LATEST ASSESSMENT
            "CROSSCHRG": _str(line,1090,  3),   # PROPERTY CROSS CHARGE
            "ENCUMBER" : _str(line,1096,  1),
            "ENCUMTRN" : _str(line,1100, 40),
            "PROTYPPO" : _str(line,1143,  1),
            "STDDESG"  : _str(line,1147,  1),
            "VIND2"    : _str(line,1151,  1),
            "VNAME"    : _str(line,1155, 40),
            "PVRPOLICY": _str(line,1198,  1),
            "PVRWAIVE" : _str(line,1202,  1),
            "PVRREASON": _str(line,1206,  1),
            "PVRWVERRES": _str(line,1210, 1),
            "PROPERT"  : _str(line,1214,100),   # TYPE / DESC OF PROPERTY
            # Derived date fields
            "SPADT"    : _mdy(_num(line,197,2), _num(line,194,2), _num(line,200,4)),
            "VALUEDT"  : _mdy(_num(line,322,2), _num(line,319,2), _num(line,325,4)),
            "ADLAPRDT" : _mdy(_num(line,495,2), _num(line,492,2), _num(line,498,4)),
            "EXPAPRDT" : _mdy(_num(line,508,2), _num(line,505,2), _num(line,511,4)),
            "EXTAPRDT" : _mdy(_num(line,521,2), _num(line,518,2), _num(line,524,4)),
        })
    return _dedup(pl.DataFrame(rows))


def read_elna8() -> pl.DataFrame:
    """ELNA8 – Main form & sub form (ELN08W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[8])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"    : _str(line,   1, 13),  # MAIN AA NO
            "CISNO"    : _num(line,  17, 15),  # CIS NO
            "ACCTNO1"  : _str(line,  35, 15),  # ACCOUNT NO 1
            "ACCTNO2"  : _str(line,  53, 15),  # ACCOUNT NO 2
            "CUSTSNCE" : _str(line,  71, 10),  # CUSTOMER SINCE
            "CUSTSNCB" : _str(line,  84, 10),  # CUSTOMER SINCE -BORR
            "CAPEMPL"  : _comma_num(line,110,15),  # CAPITAL EMPLOYED
            "SHAREFND" : _comma_num(line,128,15),  # SHAREHOLDERS FUND
            "AVGMTH1"  : _str(line, 146, 20),  # AVERAGE MTH DEP 1
            "AVGMTH2"  : _str(line, 169, 20),  # AVERAGE MTH DEP 2
            "AVGMTH3D" : _str(line, 192, 20),  # AVERAGE MTH DEP 3
            "AVGDAY1"  : _str(line, 215, 20),  # AVERAGE DAILY BAL 1
            "AVGDAY2"  : _str(line, 238, 20),  # AVERAGE DAILY BAL 2
            "AVGDAY3"  : _str(line, 261, 20),  # AVERAGE DAILY BAL 3
            "PMARGIN"  : _num(line,  284,  3),  # PERCENTAGE OF MARGIN
            "RECONAME" : _str(line, 303, 60),  # NAME OF RECOMMENDED
            "COMPLIBY" : _str(line, 366, 60),  # COMPLIANCE CHECKLIST BY
            "COMPLIGR" : _str(line, 442, 15),  # COMPLIANCE CHCKLST GRADE
            "STAFFREF" : _str(line, 460, 10),  # REFERRING STAFF'S ID
            "STAFFLOA" : _str(line, 473,  3),  # STAFF LOAN INDICATOR
            "STAFFNAME": _str(line, 479, 50),  # STAFF LOAN-NAME
            "STAFFID"  : _str(line, 532,  5),  # STAFF LOAN-ID
            "STAFFBRAN": _str(line, 540, 11),  # STAFF LOAN-BRANCH/HO
            "STAFFBRID": _str(line, 554,  3),  # STAFF LOAN-BRANCH ID
            "STAFFDIV" : _str(line, 560, 30),  # STAFF LOAN-HO DIVISION
            "STRTDLOA" : _str(line, 593,  3),  # STAFF RTD LOAN INDICATOR
            "STRTDNAME": _str(line, 599, 50),  # STAFF RTD LOAN-NAME
            "STRTDID"  : _str(line, 652,  5),  # STAFF RTD LOAN-ID
            "STRTDBRAN": _str(line, 660, 11),  # STAFF RTD LOAN-BRANCH/HO
            "STRTDBRID": _str(line, 674,  3),  # STAFF RTD LOAN-BRANCH ID
            "STRTDHO"  : _str(line, 680, 30),  # STAFF RTD LOAN-HO DIVISION
            "STRTDREL" : _str(line, 713,100),  # STAFF RTD LOAN-RELATIVE
            "STRTDSHIP": _str(line, 816,100),  # STAFF RTD LOAN-RELATIONSHIP
            "REGION"   : _str(line, 919,  4),  # REGION
            "BEPTOT"   : _str(line, 926, 15),  # BEP-TOTAL
            "ACCTNO3"  : _str(line, 944, 15),  # ACCOUNT NO 3 (OD)
            "ADDPERILS": _str(line, 962, 15),  # ADDITIONAL PERILS(COVER)
            "SUMCOVER" : _str(line, 980, 20),  # SUM COVERED
            "APPRLVL"  : _str(line,1029,  1),  # APPROVING LEVEL
            "HRCIND"   : _str(line,1033,  1),  # HRC INDICATOR
            "DESGRECO" : _str(line,1037, 30),  # DESIGNATION OF RCMD PARTY
            "EXRATIO"  : _str(line,1070,  6),  # BPS% EXEMPTED RATIO
            "FRMBANKMTH": _num(line, 1079,  2),  # CUST SINCE BANKING(MTH)
            "FRMBORRMTH": _num(line, 1084,  2),  # CUST SINCE BORROWING(MTH)
            "REFID1"   : _str(line,1089, 20),  # REFERRAL ID1
            "REFID2"   : _str(line,1107, 20),  # REFERRAL ID2
            # Derived date fields
            "APPLDATE" : _mdy(_num(line,293,2), _num(line,290,2), _num(line,296,4)),
            "INCRDT"   : _mdy(_num(line,100,2), _num(line, 97,2), _num(line,103,4)),
            "COMPLIDT" : _mdy(_num(line,432,2), _num(line,429,2), _num(line,435,4)),
            "STARTDT"  : _mdy(_num(line,1006,2), _num(line,1003,2), _num(line,1009,4)),
            "ENDDT"    : _mdy(_num(line,1019,2), _num(line,1016,2), _num(line,1022,4)),
        })
    return _dedup(pl.DataFrame(rows))


def read_elna9() -> pl.DataFrame:
    """ELNA9 – Existing AA list (ELN09W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[9])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO": _str(line,  1, 13),  # MAIN AA NO
            "AA1"  : _str(line, 17, 13),  # EXISTING AA1
            "AA2"  : _str(line, 33, 13),  # EXISTING AA2
            "AA3"  : _str(line, 49, 13),  # EXISTING AA3
            "AA4"  : _str(line, 65, 13),  # EXISTING AA4
            "AA5"  : _str(line, 81, 13),  # EXISTING AA5
            "AA6"  : _str(line, 97, 13),  # EXISTING AA6
            "AA7"  : _str(line,113, 13),  # EXISTING AA7
            "AA8"  : _str(line,129, 13),  # EXISTING AA8
            "AA9"  : _str(line,145, 13),  # EXISTING AA9
            "AA10" : _str(line,161, 13),  # EXISTING AA10
            "AA11" : _str(line,177, 13),  # EXISTING AA11
            "AA12" : _str(line,193, 13),  # EXISTING AA12
            "AA13" : _str(line,209, 13),  # EXISTING AA13
            "AA14" : _str(line,225, 13),  # EXISTING AA14
            "AA15" : _str(line,241, 13),  # EXISTING AA15
        })
    return _dedup(pl.DataFrame(rows))


def read_elna10() -> pl.DataFrame:
    """ELNA10 – MISE 2-AA data (ELN10W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[10])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"     : _str(line,   1, 13),  # MAIN AA NO
            "APPL"      : _str(line,  17,  6),  # APPEAL
            "LINEBUS"   : _str(line,  26, 60),  # LINE OF BUSINESS
            "BUSSNCE"   : _str(line,  89,  4),  # BUSINESS SINCE
            "PLCEBUS"   : _str(line,  96,  6),  # PLACE OF BUSINESS
            "EXPSNCE"   : _str(line, 105,  4),  # EXPERIENCE SINCE
            "MRGADVE"   : _str(line, 112,  6),  # HOE-MARGIN OF ADVANCE
            "OVRCRD1"   : _str(line, 121,  1),  # OVERRIDE CREDIT1
            "OVRCRD2"   : _str(line, 125,  1),  # OVERRIDE CREDIT2
            "OVRCRD3"   : _str(line, 129,  1),  # OVERRIDE CREDIT3
            "OVRCRD4"   : _str(line, 133,  1),  # OVERRIDE CREDIT4
            "OVRCRD5"   : _str(line, 137,  1),  # OVERRIDE CREDIT5
            "OVRCRD6"   : _str(line, 141,  1),  # OVERRIDE CREDIT6
            "OVRCRD7"   : _str(line, 145,  1),  # OVERRIDE CREDIT7
            "OVRCRD8"   : _str(line, 149,  1),  # OVERRIDE CREDIT8
            "OVRCRD9"   : _str(line, 153,  1),  # OVERRIDE CREDIT9
            "OVRCRD10"  : _str(line, 157,  1),  # OVERRIDE CREDIT10
            "OVRCRD11"  : _str(line, 161,  1),  # OVERRIDE CREDIT11
            "OVRCRD12"  : _str(line, 165,  1),  # OVERRIDE CREDIT12
            "OVRCRD13"  : _str(line, 169,  1),  # OVERRIDE CREDIT13
            "OVROTH"    : _str(line, 173,150),  # OVERRIDE CREDIT-BASIS/OTH
            "GRPCODE"   : _str(line, 326, 55),  # BASIC GROUP CODE
            "AGGSAVE"   : _str(line, 384,  3),  # AGG SAVINGS/FD/CA/AVG
            "AGGTRST"   : _str(line, 390,  3),  # AGGREGATE UNIT TRUST
            "AGGSHRE"   : _str(line, 396,  3),  # AGGREGATE QUOTED SHARES
            "CABRISCK"  : _str(line, 402, 20),  # CON ACCT:RST BRIS/CTOS
            "CACCRSCK"  : _str(line, 425, 30),  # CON ACCT:RST CCRIS CHK
            "CADCHQCK"  : _str(line, 458, 30),  # CON ACCT:RST DCHEQS
            "CALGLACT"  : _str(line, 491,  3),  # CON ACCT:RST EXT LEGAL
            "CAOTAS"    : _str(line, 497,  3),  # CON ACCT:OTAS
            "CACOBF"    : _str(line, 503,  3),  # CON ACCT:COBF
            "CADCHEQS"  : _str(line, 509,  3),  # CON ACCT:DCHEQS
            "CACCRIS"   : _str(line, 515,  3),  # CON ACCT:CCRIS
            "CACRSFIY"  : _str(line, 521, 15),  # CCRIS PBB/OTR FI LOAN(Y)
            "CACRSFIN"  : _str(line, 539,  3),  # CCRIS PBB/OTR FI LOAN(N)
            "CACRSPB"   : _str(line, 545,  3),  # CN ACCT:CCRIS(PBB LOAN)
            "CACRSPBY"  : _str(line, 551, 15),  # CN ACCT:CCRIS(PBB LOAN)Y
            "RABRISCK"  : _str(line, 569, 20),  # REL ACCT:RLT BRIS/CTOS
            "RACCRIS"   : _str(line, 592, 30),  # REL ACCT:RLT CCRIS CHK
            "RADCHQCK"  : _str(line, 625, 30),  # REL ACCT:RSL DCHEQS CHK
            "RALGLACT"  : _str(line, 658,  3),  # REL ACCT:RSL LEGAL ACT
            "RAOTAS"    : _str(line, 664,  3),  # REL ACCT:OTAS
            "RACOBF"    : _str(line, 670,  3),  # REL ACCT:COBF
            "RADCHEQS"  : _str(line, 676,  3),  # REL ACCT:DCHEQS
            "RACRSFI"   : _str(line, 682,  3),  # REL ACCT:CCRIS(PBB/OTH)
            "RACRSFIY"  : _str(line, 688, 15),  # REL ACCT:CCRIS(PBB/OTH)Y
            "RACRSFIN"  : _str(line, 706,  3),  # REL ACCT:CCRIS(PBB/OTH)N
            "RACRSPB"   : _str(line, 712,  3),  # REL ACCT:CCRIS(PBB)
            "RACRSPBY"  : _str(line, 718, 15),  # REL ACCT:CCRIS(PBB)YES
            "CMV"       : _str(line, 736, 15),  # CMV(TOTAL)
            "DDASST"    : _str(line, 754, 15),  # DEED OF DEBEN-ASSETS
            "DDDEBVAL"  : _str(line, 772, 15),  # DEED OF DEBEN-NET DBT VL
            "BGAMNT"    : _str(line, 790, 15),  # SBLC/BG-AMOUNT
            "BGDRADV"   : _str(line, 808,  6),  # SBLC/BG:DIRECT ADVANCES
            "BGTOTADV"  : _str(line, 817,  6),  # SBLC/BG:TOTAL ADVANCES
            "MRGNEMB"   : _str(line, 826,  3),  # MARGIN OF EMBARKING
            "FDAMNT"    : _str(line, 832, 15),  # FIXED DEPOSIT RECEIPTS
            "APLMARGIN" : _str(line, 850,  6),  # APPEAL MARGIN OF ADVANCE
            "CUSTTYPE"  : _str(line, 859, 40),  # TYPE OF CUSTOMER
            "STAFFLNCOV": _str(line, 902,  3),  # CONVERSION FROM STAFF LN
            "ERMNAME"   : _str(line, 908, 50),  # EARMARKING ACCOUNT NAME
            "ERMACNO"   : _str(line, 961, 15),  # EARMARKING ACCOUNT NO.
            "ERMAMT"    : _str(line, 979, 20),  # EARMARKING ACCOUNT AMOUNT
            "REPCONTRY" : _str(line,1002,  2),  # REPORTING COUNTRY
            "ECM3RES"   : _str(line,1007, 15),  # ECM3RES
            "ECM8RES"   : _str(line,1025, 15),  # ECM8RES
            "RESSTAT"   : _str(line,1043,  1),  # RESIDENCY STATUS
            "OVERCCR1"  : _str(line,1047,  1),  # OVERALL CRR
            "CREDEXCP"  : _str(line,1051,  1),  # CREDIT EXCEPTION
            "CTRCODE"   : _str(line,1055,  3),
            "CTRCDDESC" : _str(line,1061,100),
            "CABCTOS"   : _str(line,1164,100),
            "RABCTOS"   : _str(line,1267,100),
            "FIN030RM"  : _str(line,1370, 15),
            "FIN030"    : _str(line,1388,  5),
            "FIN3160RM" : _str(line,1396, 15),
            "FIN3160"   : _str(line,1414,  5),
            "FIN6190RM" : _str(line,1422, 15),
            "FIN6190"   : _str(line,1440,  5),
            "FIN90RM"   : _str(line,1448, 15),
            "FIN90"     : _str(line,1466,  5),
            "FIN060RM"  : _str(line,1474, 15),
            "FIN060"    : _str(line,1492,  5),
            "FIN61120RM": _str(line,1500, 15),
            "FIN61120"  : _str(line,1518,  5),
            "FIN121180RM": _str(line,1526, 15),
            "FIN121180" : _str(line,1544,  5),
            "FIN180RM"  : _str(line,1552, 15),
            "FIN180"    : _str(line,1570,  5),
            "TOTALRM"   : _str(line,1578, 15),
            "TOTAL"     : _str(line,1596,  5),
            "CONDOFACC" : _str(line,1604,  1),
        })
    return _dedup(pl.DataFrame(rows))


def read_elna11() -> pl.DataFrame:
    """ELNA11 – HP data (ELN11W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[11])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        # SCAN equivalent: extract first token delimited by comma
        def _scan1(val: str) -> str:
            return val.split(",")[0].strip()

        appidno = _scan1(_str(line, 543, 30))
        gr1idno = _scan1(_str(line, 846, 20))
        gr2idno = _scan1(_str(line,1139, 20))
        gr3idno = _scan1(_str(line,1432, 20))

        rows.append({
            "MAANO"     : _str(line,   1, 13),  # MAIN AA NO
            "PRODTYPE"  : _str(line,  17, 50),  # PRODUCT TYPE
            "BRHVALUE"  : _str(line,  70, 20),  # BRANCH VALUATION
            "TOTCOST"   : _str(line,  93, 20),  # TOTAL COST
            "TOTDEPO"   : _str(line, 116, 20),  # TOTAL DEPOSIT
            "FISCHK"    : _str(line, 139, 10),  # FIS CHECKINGS
            "SNLCHK"    : _str(line, 152, 10),  # SNL CHECKINGS
            "CTOS"      : _str(line, 165, 10),  # CTOS
            "BRIS"      : _str(line, 178, 10),  # BRIS
            "FRAUDLS"   : _str(line, 191, 10),  # FRADULENT LIST
            "PVLCHK"    : _str(line, 204, 10),  # PVL CHECKING
            "CCRISCHK"  : _str(line, 217, 10),  # CCRIS CHECKING
            "GNTORID"   : _str(line, 230, 10),  # GRTR'ID MATCHED IN CCRIS
            "OUTCRDT"   : _str(line, 243,  3),  # REC OF OUTSTANDING CRDIT
            "ODEXC1"    : _str(line, 249, 15),  # FREQNCY OF OD EXCESS 1
            "ODEXC2"    : _str(line, 267, 15),  # FREQNCY OF OD EXCESS 2
            "ODEXC3"    : _str(line, 285, 15),  # FREQNCY OF OD EXCESS 3
            "OUTCHQ1"   : _str(line, 303, 15),  # OUTWARD CHEQUES RETURN 1
            "OUTCHQ2"   : _str(line, 321, 15),  # OUTWARD CHEQUES RETURN 2
            "OUTCHQ3"   : _str(line, 339, 15),  # OUTWARD CHEQUES RETURN 3
            "CRDTBLNC"  : _str(line, 357, 15),  # AVR DAILY CREDIT BALANCE
            "BALFD"     : _str(line, 375, 15),  # BAL OF SA/FD AMOUNT
            "BSTOTAL"   : _str(line, 393, 15),  # BNK STATEMENT ALY:TOTAL
            "HPLIAB"    : _str(line, 411, 20),  # AGGREGATE HP LIAB
            "EXTLIAB"   : _str(line, 434, 20),  # AGG OTH EXS LIABILITIES
            "CRDTLMT"   : _str(line, 457, 20),  # OVERALL CREDIT LIMIT
            "APPNAME"   : _str(line, 480, 60),  # APPLICANT NAME
            "APPIDNO"   : appidno,              # APPLICANT ID NO (first token)
            "APPSA"     : _str(line, 576, 20),  # APPLICANT SAVING A/C
            "APPCA"     : _str(line, 599, 20),  # APPLICANT CURRENT A/C
            "APPFD"     : _str(line, 622, 20),  # APPLICANT FIXED DEPOSIT
            "APPASB"    : _str(line, 645, 20),  # APPLICANT ASB
            "APPAWSB"   : _str(line, 668, 20),  # APPLICANT AWSB
            "APPUT"     : _str(line, 691, 20),  # APPLICANT UNIT TRUST
            "APPNBQS"   : _str(line, 714, 20),  # APP.NON-BLOCK QUOTED SHR
            "APPINV"    : _str(line, 737, 20),  # APP.INVESTMENT (@80% CMV)
            "APPTDP"    : _str(line, 760, 20),  # APPLICANT TOTAL DEPOSIT
            "GR1NAME"   : _str(line, 783, 60),  # GUARANTOR 1 NAME
            "GR1IDNO"   : gr1idno,              # GUARANTOR 1 ID NO
            "GR1SA"     : _str(line, 869, 20),  # GUARANTOR 1 SAVING A/C
            "GR1CA"     : _str(line, 892, 20),  # GUARANTOR 1 CURRENT A/C
            "GR1FD"     : _str(line, 915, 20),  # GUARANTOR 1 FIXED DEPOSIT
            "GR1ASB"    : _str(line, 938, 20),  # GUARANTOR 1 ASB
            "GR1AWSB"   : _str(line, 961, 20),  # GUARANTOR 1 AWSB
            "GR1UT"     : _str(line, 984, 20),  # GUARANTOR 1 UNIT TRUST
            "GR1NBQS"   : _str(line,1007, 20),  # G.1 NON-BLOCK QUOTED SHR
            "GR1INV"    : _str(line,1030, 20),  # G.1 INVESTMENT (@80% CMV)
            "GR1TDP"    : _str(line,1053, 20),  # GUARANTOR 1 TOTAL DEPOSIT
            "GR2NAME"   : _str(line,1076, 60),  # GUARANTOR 2 NAME
            "GR2IDNO"   : gr2idno,              # GUARANTOR 2 ID NO
            "GR2SA"     : _str(line,1162, 20),  # GUARANTOR 2 SAVING A/C
            "GR2CA"     : _str(line,1185, 20),  # GUARANTOR 2 CURRENT A/C
            "GR2FD"     : _str(line,1208, 20),  # GUARANTOR 2 FIXED DEPOSIT
            "GR2ASB"    : _str(line,1231, 20),  # GUARANTOR 2 ASB
            "GR2AWSB"   : _str(line,1254, 20),  # GUARANTOR 2 AWSB
            "GR2UT"     : _str(line,1277, 20),  # GUARANTOR 2 UNIT TRUST
            "GR2NBQS"   : _str(line,1300, 20),  # G.2 NON-BLOCK QUOTED SHR
            "GR2INV"    : _str(line,1323, 20),  # G.2 INVESTMENT (@80% CMV)
            "GR2TDP"    : _str(line,1346, 20),  # GUARANTOR 2 TOTAL DEPOSIT
            "GR3NAME"   : _str(line,1369, 60),  # GUARANTOR 3 NAME
            "GR3IDNO"   : gr3idno,              # GUARANTOR 3 ID NO
            "GR3SA"     : _str(line,1455, 20),  # GUARANTOR 3 SAVING A/C
            "GR3CA"     : _str(line,1478, 20),  # GUARANTOR 3 CURRENT A/C
            "GR3FD"     : _str(line,1501, 20),  # GUARANTOR 3 FIXED DEPOSIT
            "GR3ASB"    : _str(line,1524, 20),  # GUARANTOR 3 ASB
            "GR3AWSB"   : _str(line,1547, 20),  # GUARANTOR 3 AWSB
            "GR3UT"     : _str(line,1570, 20),  # GUARANTOR 3 UNIT TRUST
            "GR3NBQS"   : _str(line,1593, 20),  # G.3 NON-BLOCK QUOTED SHR
            "GR3INV"    : _str(line,1616, 20),  # G.3 INVESTMENT (@80% CMV)
            "GR3TDP"    : _str(line,1639, 20),  # GUARANTOR 3 TOTAL DEPOSIT
            "EWP"       : _str(line,1662, 20),  # EXTENDED WARANTY PERIOD
            "INTSDYAMT" : _str(line,1685, 20),  # INTEREST SUBSIDY AMOUNT
            "CASAIND"   : _str(line,1708, 20),  # CA SA INDICATOR
            "RNOTCASA"  : _str(line,1721, 20),  # REASON 4 NOT TAKEUP CASA
            "EIR"       : _str(line,1774,  5),  # EFFECTIVE INTEREST RATE
            "PREINST"   : _str(line,1782, 15),  # PREPAID INSTALMENTS
            "CASHDEP"   : _str(line,1800, 15),  # CASH DEPOSITS
            "OTHDEP"    : _str(line,1818, 15),  # OTHER DEPOSITS
            "INSCOMP"   : _str(line,1836, 50),  # INSURANCE COMPANY
            "INSCOMPADD": _str(line,1889,200),  # INSURANCE COMPANY ADDRESS
            "POLICYTYP" : _str(line,2092, 15),  # TYPE OF POLICY
            "COVERNT"   : _str(line,2110, 20),  # COVER NOTE NO.
            "POLICYNO"  : _str(line,2133, 20),  # POLICY NO.
            "INSURAMT"  : _comma_num(line,2156,15),  # AMOUNT INSURED
            "EXPDT"     : _str(line,2174, 10),  # EXPIRY DATE
            "IDENVSCRIS": _str(line,2187, 10),  # APPLICANT IN CCRIS DB
            "OSCREDIT"  : _str(line,2200,  3),  # APPLICANT ON OUTSTAND CR
            "INTRATE"   : _str(line,2206, 30),  # INTEREST RATE/INSTALMENT1
            "FDPLEDAMT" : _str(line,2239, 20),  # FD PLEDGE AMOUNT
            "PLEDDURLN" : _str(line,2262,  3),  # WHOLE HP TENURE
            "MTHINSTMNT": _comma_num(line,2268,15),  # MONTHLY INSTALMENT
            "FNLINSTMNT": _comma_num(line,2286,15),  # FINAL INSTALMENT
            "APPVLEVL"  : _str(line,2304, 40),  # APPROVAL LEVEL
            "ENDORSED"  : _str(line,2347, 60),  # EIR/EFR ENDORSED BY
            "CONFIRMED" : _str(line,2410, 60),  # EIR/EFR CONFIRMED BY
            "REMARK"    : _str(line,2473,100),  # EIR/EFR Remark
            "DATE"      : _ddmmyy(line, 2576),  # EIR/EFR Date
            "HANDFEETOT": _num(line, 2589, 18),  # Handling Fee Total
            "HANDFEEGST": _num(line, 2607, 18),  # Handling Fee Gst
        })
    return _dedup(pl.DataFrame(rows))


def read_elna12() -> pl.DataFrame:
    """ELNA12 – Asset/liability summary (ELN12W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[12])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"    : _str(line,   1, 13),  # MAIN AA NO
            "FNAME"    : _str(line,  17, 50),  # FULL NAME
            "PROP"     : _str(line,  70, 15),  # PROPERTY
            "SHRES"    : _str(line,  88, 15),  # SHARES
            "UTRSTS"   : _str(line, 106, 15),  # UNIT TRUST
            "SAVING"   : _str(line, 124, 15),  # SAVINGS/FD/CA
            "OTHERS"   : _str(line, 142, 15),  # OTHERS
            "TOTASET"  : _str(line, 160, 15),  # TOTAL ASSETS
            "TOTLIAB1" : _str(line, 178, 15),  # TOTAL LIABILITIES
            "BANK1"    : _str(line, 196, 33),  # ACCT FIN:NAME OF BANK 1
            "TYPEACCT1": _str(line, 232, 33),  # ACCT FIN:TYPE ACCT 1
            "ACCTNOF1" : _str(line, 268, 33),  # ACCT FIN:ACCT NO.1
            "TYPEFAC1" : _str(line, 304, 33),  # ACCT FIN:TYPE FACILITY 1
            "LMTAPP1"  : _str(line, 340, 15),  # ACCT FIN:LIMIT APPROVED 1
            "BALANCE1" : _str(line, 358, 33),  # ACCT FIN:BALANCE(RM)DATE1
            "BANK2"    : _str(line, 394, 33),  # ACCT FIN:NAME OF BANK 2
            "TYPEACCT2": _str(line, 430, 33),  # ACCT FIN:TYPE ACCT 2
            "ACCTNOF2" : _str(line, 466, 33),  # ACCT FIN:ACCT NO.2
            "TYPEFAC2" : _str(line, 502, 33),  # ACCT FIN:TYPE FACILITY 2
            "LMTAPP2"  : _str(line, 538, 15),  # ACCT FIN:LIMIT APPROVED 2
            "BALANCE2" : _str(line, 556, 33),  # ACCT FIN:BALANCE(RM)DATE2
            "OORACC1"  : _str(line, 592,  7),  # ACCT FIN:OWN/RELATED ACC1
            "OORACC2"  : _str(line, 602,  7),  # ACCT FIN:OWN/RELATED ACC2
            "TBANK1"   : _str(line, 612, 33),  # ACCT OTH:NAME OF BANK 1
            "TOORACC1" : _str(line, 648,  7),  # ACCT OTH:OWN/RELATED ACC1
            "TTYPEACC1": _str(line, 658, 33),  # ACCT OTH:TYPE ACC 1
            "TACCTNOF1": _str(line, 694, 33),  # ACCT OTH:ACCOUNT NO 1
            "TTYPEFAC1": _str(line, 730, 33),  # ACCT OTH:TYPE FACILITY 1
            "TLMTAPP1" : _str(line, 766, 15),  # ACCT OTH:LIMIT APPROVED 1
            "TBALANCE1": _str(line, 784, 33),  # ACCT OTH:BALANCE(RM)DATE1
            "TBANK2"   : _str(line, 820, 33),  # ACCT OTH:NAME OF BANK 2
            "TOORACC2" : _str(line, 856,  7),  # ACCT OTH:OWN/RELATED ACC2
            "TTYPEACC2": _str(line, 866, 33),  # ACCT OTH:TYPE ACC 2
            "TACCTNOF2": _str(line, 902, 33),  # ACCT OTH:ACCOUNT NO 2
            "TTYPEFAC2": _str(line, 938, 33),  # ACCT OTH:TYPE FACILITY 2
            "TLMTAPP2" : _str(line, 974, 15),  # ACCT OTH:LIMIT APPROVED 2
            "TBALANCE2": _str(line, 992, 33),  # ACCT OTH:BALANCE(RM)DATE2
            "CADBCR"   : _str(line,1028, 15),  # ASSETS: CA (ADB CR)
        })
    return _dedup(pl.DataFrame(rows))


def read_elna13() -> pl.DataFrame:
    """ELNA13 – Existing facilities (ELN13W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[13])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"    : _str(line,  1, 13),  # MAIN AA NO
            "EXTACCTNO": _str(line, 17, 14),  # EXISTING AA/ACCT NO.
            "TYPE"     : _str(line, 34, 40),  # TYPE
            "FACILITY1": _str(line, 77, 30),  # FACILITY
            "LIMIT"    : _str(line,110, 20),  # LIMIT
            "NEWAA"    : _str(line,133, 20),  # NEW/CHANGE
            "TOTAL"    : _str(line,156, 20),  # TOTAL/AMOUNT FINANCED
            "OUTSTND"  : _str(line,179,150),  # OUTSTANDING
            "OUTDATE"  : _str(line,332, 10),  # OUTSTANDING @ DATE
            "PRICE"    : _str(line,345,150),  # PRICING
            "CNDACCT"  : _str(line,498, 20),  # CONDUCT OF ACCOUNT
            "FFSFSPSUS": _str(line,521,  5),
            "STATUSOFR": _str(line,529,  5),
            "FACTYPE"  : _str(line,537,  1),
            "FACCODE"  : _str(line,541, 10),
        })
    return _dedup(pl.DataFrame(rows))


def read_elna14() -> pl.DataFrame:
    """ELNA14 – Borrower/account/facility summary (ELN14W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[14])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"   : _str(line,  1, 13),   # MAIN AA NO
            "BACCTNO" : _str(line, 17,100),   # BOR/CST/BRACH/ACCT NO.
            "FACILITY2": _str(line,120,120),  # FACILITY
            "OPLMT"   : _str(line,243,110),   # APPROVED OPERATIVE LIMIT
            "NATSEC"  : _str(line,356,110),   # NATURE OF SECURITY & CMV
            "PRICE1"  : _str(line,469,180),   # PRICING
            "CONACCT" : _str(line,652, 75),   # CONDUCT OF ACCOUNT
        })
    return _dedup(pl.DataFrame(rows))


def read_elna15() -> pl.DataFrame:
    """ELNA15 – Share/unit trust data (ELN15W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[15])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"  : _str(line,  1, 13),  # MAIN AA NO
            "SHREAC" : _str(line, 17, 30),  # NAME SHR CNTR/UNIT TRST
            "STCKCOD": _num(line,  50, 10),  # STOCK CODE
            "SHRENO" : _num(line,  63, 12),  # NO. OF SHARE/UNITS
            "QUOPRC" : _num(line,  78, 20),  # QTD PRC/UNT BUY'G PRC
            "QUODATE": _num(line, 101, 10),  # QTD PRC/UNT BUY'G PRC DT
            "MRKTVAL": _num(line, 114, 30),  # MARKET VALUE
            "MRKTDATE": _num(line, 147, 10), # MARKET VALUE DATE
        })
    return _dedup(pl.DataFrame(rows))


def read_elna16() -> pl.DataFrame:
    """ELNA16 – Other securities (ELN16W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[16])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"   : _str(line,  1, 13),  # MAIN AA NO
            "OTHSCR"  : _str(line, 17, 50),  # OTHER SECURITIES
            "ACCTNO16": _str(line, 70, 16),  # A/C NO.
            "AMOUNT1" : _comma_num(line, 89, 15),  # AMOUNT
            "DESC"    : _str(line,107,100),  # DESCRIPTION
        })
    return _dedup(pl.DataFrame(rows))


def read_elna17() -> pl.DataFrame:
    """ELNA17 – CRR form scores (ELN17W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[17])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"   : _str(line,   1, 13),  # MAIN AA NO
            "CRRFRM"  : _str(line,  17, 20),  # CRR FORM
            "TOTSCRT" : _num(line,   40,  4),  # TOL SCORE FOR TOL SCRTY
            "TOTOVCRR": _num(line,   47,  4),  # TOL SCORE FOR OVR CRR
            "ORGDECRR": _num(line,   54,  4),  # ORI DENOMINATOR FOR CRR
            "EXTSCR"  : _num(line,   61,  4),  # EXACT VALUE OVERALL CRR
            "GRDCRR"  : _str(line,  68,  4),  # GRADE FOR OVERALL CRR
            "ORGDECCR": _num(line,   75,  4),  # ORI DENOMINATOR FOR CUST
            "EXTVLCCR": _num(line,   82,  4),  # EXACT VALUE FOR CCR
            "GRDCCR"  : _str(line,  89,  4),  # GRADE FOR CCR
            "GRDSECR" : _num(line,   96,  1),  # GRADE FOR SECURITY RATING
            "TOTCRD"  : _comma_num(line,100,15),  # TOTAL CREDIT LIMIT
            "TOTSCVAL": _comma_num(line,118,15),  # TOTAL SECURITY VALUE
            "MA"      : _num(line,  136,  6),  # M/A
            "TOTLOAN" : _comma_num(line,145,15),  # EXACT VALUE FR TOL LOAN
            "GEAR"    : _num(line,  163,  5),  # EXACT VALUE FOR GEARING
            "GRRNGE"  : _str(line, 171, 40),  # GEARING : RANGE
            "GRSCR"   : _num(line,  214,  3),  # GEARING : SCORE
            "GRWGT"   : _num(line,  220,  3),  # GEARING : WEIGHT
            "GRTOTSCR": _num(line,  226,  3),  # GEARING : TOTAL SCORE
            "BBRNGE"  : _str(line, 232, 50),  # BORROWER BORROWING:RANGE
            "BBSCR"   : _num(line,  285,  3),  # BORROWER BORROWING:SCORE
            "BBWGT"   : _num(line,  291,  3),  # BORROWER BORROWING:WEIGHT
            "BBTOTSC" : _num(line,  297,  3),  # BORROWER BORROWING:TOL SC
            "BBYY"    : _num(line,  303,  4),  # BORROWER BORROWING:YEAR
            "BBMM"    : _str(line, 310, 10),  # BORROWER BORROWING:MONTH
            "ACRNGE1" : _str(line, 323, 50),  # ACCOUNT BORROWING:RANGE
            "ACSCR1"  : _num(line,  376,  3),  # ACCOUNT BORROWING:SCORE
            "ACWGT1"  : _num(line,  382,  3),  # ACCOUNT BORROWING:WEIGHT
            "ACTOTSC1": _num(line,  388,  3),  # ACCOUNT BORROWING:TOL SC
            "ACYY"    : _num(line,  394,  4),  # ACCOUNT BORROWING:YEAR
            "ACMM"    : _str(line, 401, 10),  # ACCOUNT BORROWING:MONTH
            "NWRNGE"  : _str(line, 414, 30),  # NET WORTH:RANGE
            "NWSCR"   : _num(line,  447,  3),  # NET WORTH:SCORE
            "NWWGT"   : _num(line,  453,  3),  # NET WORTH:WEIGHT
            "NWTOTSC" : _num(line,  459,  3),  # NET WORTH:TOTAL SCORE
            "AGERNGE" : _str(line, 465, 30),  # AGE-YEARS:RANGE
            "AGESCR"  : _num(line,  498,  3),  # AGE-YEARS:SCORE
            "AGEWGT"  : _num(line,  504,  3),  # AGE-YEARS:WEIGHT
            "AGETOTSC": _num(line,  510,  3),  # AGE-YEARS:TOTAL SCORE
            "TSRNGE"  : _str(line, 516,290),  # TYPE OF SECURITY:RANGE
            "TSSCR"   : _num(line,  809,  3),  # TYPE OF SECURITY:SCORE
            "TSWGT"   : _num(line,  815,  3),  # TYPE OF SECURITY:WEIGHT
            "TSTOTSC" : _num(line,  821,  3),  # TYPE OF SECURITY:TOL SC
            "DSDSR"   : _num(line,  827,  8),  # DEBT SER RATIO:EXT VALUE
            "DSRNGE"  : _str(line, 838, 40),  # DEBT SERVICE RATIO:RANGE
            "DSSCR"   : _num(line,  881,  3),  # DEBT SERVICE RATIO:SCORE
            "DSWGT"   : _num(line,  887,  3),  # DEBT SERVICE RATIO:WEIGHT
            "DSTOTSC" : _num(line,  893,  3),  # DEBT SERVICE RATIO:TOL SC
        })
    return _dedup(pl.DataFrame(rows))


def read_elna18() -> pl.DataFrame:
    """ELNA18 – CRR scoring sub-factors (ELN18W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[18])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"   : _str(line,   1, 13),  # MAIN AA NO
            "BARNGE"  : _str(line,  17, 75),  # BORROWERS ACCOUNT:RANGE
            "BATOTSC" : _num(line,   95,  3),  # BORROWER ACCOUNT:TOL SC
            "RARNGE"  : _str(line, 101, 75),  # RELATED ACCOUNT:RANGE
            "RATOTSC" : _num(line,  179,  3),  # RELATED ACCOUNT:TOTAL SC
            "ILRNGE"  : _str(line, 185, 30),  # INCOME LEVEL:RANGE
            "ILSCR"   : _num(line,  218,  3),  # INCOME LEVEL:SCORE
            "ILWGT"   : _num(line,  224,  3),  # INCOME LEVEL:WEIGHT
            "ILTOTSC" : _num(line,  230,  3),  # INCOME LEVEL:TOTAL SCORE
            "YBRNGE"  : _str(line, 236, 40),  # YEAR OF BUSINESS:RANGE
            "YBSCR"   : _num(line,  279,  3),  # YEAR OF BUSINESS:SCORE
            "YBWGT"   : _num(line,  285,  3),  # YEAR OF BUSINESS:WEIGHT
            "YBTOTSC" : _num(line,  291,  3),  # YEAR OF BUSINESS:TOL SC
            "YBYY"    : _str(line, 297,  4),  # YEAR OF BUSINESS:YEAR
            "YBMM"    : _str(line, 304, 10),  # YEAR OF BUSINESS:MONTH
            "MERNGE"  : _str(line, 317, 40),  # MNGM EXPERIENCE:RANGE
            "MESCR"   : _num(line,  360,  3),  # MNGM EXPERIENCE:SCORE
            "MEWGT"   : _num(line,  366,  3),  # MNGMT EXPERIENCE:WEIGHT
            "METOTSC" : _num(line,  372,  3),  # MNGMT EXPERIENCE:TOL SC
            "TIRNGE"  : _str(line, 378, 30),  # TYPE OF INDUSTRY:RANGE
            "TISCR"   : _num(line,  411,  3),  # TYPE OF INDUSTRY:SCORE
            "TIWGT"   : _num(line,  417,  3),  # TYPE OF INDUSTRY:WEIGHT
            "TITTOTSC": _num(line,  423,  3),  # TYPE OF INDUSTRY:TOL SC
            "TIBNMCD" : _str(line, 429,  4),  # TYPE OF INDUSTRY:BNM CODE
            "TGVAL"   : _num(line,  436,  5),  # TURNOVER GROWTH:VALUE
            "TGRNGE"  : _str(line, 444, 30),  # TURNOVER GROWTH:RANGE
            "TGSCR"   : _num(line,  477,  3),  # TURNOVER GROWTH:SCORE
            "TGWGT"   : _num(line,  483,  3),  # TURNOVER GROWTH:WEIGHT
            "TGTOTSC" : _num(line,  489,  3),  # TURNOVER GROWTH:TOTAL SC
            "NPVAL"   : _num(line,  495,  5),  # NET PROFIT:VALUE
            "NPRNGE"  : _str(line, 503, 30),  # NET PROFIT:RANGE
            "NPSCR"   : _num(line,  536,  3),  # NET PROFIT:SCORE
            "NPWGT"   : _num(line,  542,  3),  # NET PROFIT:WEIGHT
            "NPTOTSC" : _num(line,  548,  3),  # NET PROFIT:TOTAL SCORE
            "ATVAL"   : _num(line,  554,  9),  # ACID TEST RATIO:VALUE
            "ATRNGE"  : _str(line, 566, 30),  # ACID TEST RATIO:RANGE
            "ATSCR"   : _num(line,  599,  3),  # ACID TEST RATIO:SCORE
            "ATWGT"   : _num(line,  605,  3),  # ACID TEST RATIO: WEIGHT
            "ATTOTSC" : _num(line,  611,  3),  # ACID TEST RATIO:TOL SC
            "LRVAL"   : _num(line,  617,  5),  # LEVERAGE RATIO:VALUE
            "LRRNGE"  : _str(line, 625, 30),  # LEVERAGE RATIO:RANGE
            "LRSCR"   : _num(line,  658,  3),  # LEVERAGE RATIO:SCORE
            "LRWGT"   : _num(line,  664,  3),  # LEVERAGE RATIO:WEIGHT
            "LRTOTSC" : _num(line,  670,  3),  # LEVERAGE RATIO:TOTAL SC
            "ICVAL"   : _num(line,  676,  5),  # INTEREST COGE RT:VALUE
            "ICRNGE"  : _str(line, 684, 30),  # INTEREST COGE RT:RANGE
            "ICSCR"   : _num(line,  717,  3),  # INTEREST COGE RT:SCORE
            "ICWGT"   : _num(line,  723,  3),  # INTEREST COGE RT:WEIGHT
            "ICTOTSC" : _num(line,  729,  3),  # INTEREST COGE RT:TOL SC
            "ACVAL"   : _num(line,  735,  6),  # AVG COLL PERIOD:VALUE
            "ACRNGE2" : _str(line, 744, 30),  # AVG COLL PERIOD:RANGE
            "ACSCR2"  : _num(line,  777,  3),  # AVG COLL PERIOD:SCORE
            "ACWGT2"  : _num(line,  783,  3),  # AVG COLL PERIOD:WEIGHT
            "ACTOTSC2": _num(line,  789,  3),  # AVG COLL PERIOD:TOL SC
            "OPRNGE"  : _str(line, 795, 75),  # OPERATING CASHFLOW:RANGE
            "OPSCR"   : _num(line,  873,  3),  # OPERATING CASHFLOW:SCORE
            "OPWGT"   : _num(line,  879,  3),  # OPERATING CASHFLOW:WEIGHT
            "OPTOTSC" : _num(line,  885,  3),  # OPERATING CASHFLOW:TOL SC
            "BERNGE"  : _str(line, 891, 40),  # BUSINESS EXP:RANGE
            "BESCR"   : _num(line,  934,  3),  # BUSINESS EXP:SCORE
            "BEWGT"   : _num(line,  940,  3),  # BUSINESS EXP:WEIGHT
            "BETOTSC" : _num(line,  946,  3),  # BUSINESS EXP:TOTAL SC
            "BEYY"    : _str(line, 952,  4),  # BUSINESS EXP:YEAR
        })
    return _dedup(pl.DataFrame(rows))


def read_elna19() -> pl.DataFrame:
    """ELNA19 – Additional CRR scoring sub-factors (ELN19W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[19])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"  : _str(line,  1, 13),   # MAIN AA NO
            "CBRNGE" : _str(line, 17, 30),   # AVG CREDIT BALANCES:RANGE
            "CBSCR"  : _str(line, 50,  3),   # AVG CREDIT BALANCES:SCORE
            "CBWGT"  : _str(line, 56,  3),   # AVG CREDIT BALANCES:WEIGHT
            "CBTOTSC": _str(line, 62,  3),   # AVG CREDIT BALANCES:TOL SC
            "ADRNGE" : _str(line, 68, 30),   # AVG AMT DEPOSITED:RANGE
            "ADSCR"  : _str(line,101,  3),   # AVG AMT DEPOSITED:SCORE
            "ADWGT"  : _str(line,107,  3),   # AVG AMT DEPOSITED:WEIGHT
            "ADTOTSC": _str(line,113,  3),   # AVG AMT DEPOSITED:TOL SC
            "PORNGE" : _str(line,119, 60),   # PREMISES OWNERSHIP:RANGE
            "POSCR"  : _str(line,182,  3),   # PREMISES OWNERSHIP:SCORE
            "POWGT"  : _str(line,188,  3),   # PREMISES OWNERSHIP:WEIGHT
            "POTOTSC": _str(line,194,  3),   # PREMISES OWNERSHIP:TOL SC
            "MDEXTVL": _str(line,200, 11),   # EXT VAL FOR AVG MON DEPO
            "MTEXTVAL": _str(line,214, 11),  # EXT VAL FOR AVG MON TUR
            "MDRNGE" : _str(line,228, 30),   # AVG MON DEPOSITS:RANGE
            "MDSCR"  : _str(line,261,  3),   # AVG MON DEPOSITS:SCORE
            "MDWGT"  : _str(line,267,  3),   # AVG MON DEPOSITS:WEIGHT
            "MDTOTSC": _str(line,273,  3),   # AVG MONDEPOSITS:TOL SC
            "MPEXTVL": _str(line,279, 11),   # EXT VAL FR AVG MON PROFIT
            "MIEXTVAL": _str(line,293, 11),  # EXT VAL FR AVG MON INSTAL
            "MPRNGE" : _str(line,307, 30),   # AVG MONTHLY PROFIT:RANGE
            "MPSCR"  : _str(line,340,  3),   # AVG MONTHLY PROFIT:SCORE
            "MPWGT"  : _str(line,346,  3),   # AVG MONTHLY PROFIT:WEIGHT
            "MPTOTSC": _str(line,352,  3),   # AVG MONTHLY PROFIT:TOL SC
            "OCRNGE" : _str(line,358,  5),   # OCCUPATION:RANGE
            "OCSCR"  : _str(line,366,  3),   # OCCUPATION:SCORE
            "OCWHT"  : _str(line,372,  3),   # OCCUPATION:WEIGHT
            "OCTOTSC": _str(line,378,  3),   # OCCUPATION:TOTAL SCORE
            "SGRNGE" : _str(line,384, 20),   # SCH/UNSCH GOODS:RANGE
            "SGSCR"  : _str(line,407,  3),   # SCH/UNSCH GOODS:SCORE
            "SGWGT"  : _str(line,413,  3),   # SCH/UNSCH GOODS:WEIGHT
            "SGTOTSC": _str(line,419,  3),   # SCH/UNSCH GOODS:TOL SCORE
            "TVRNGE" : _str(line,425, 50),   # TYPE OF VEHICLE:RANGE
            "TVSCR"  : _str(line,478,  3),   # TYPE OF VEHICLE:SCORE
            "TVWGT"  : _str(line,484,  3),   # TYPE OF VEHICLE:WEIGHT
            "TVTOTSC": _str(line,490,  3),   # TYPE OF VEHICLE:TOTAL SC
            "TRRNGE" : _str(line,496, 30),   # TRACK RECORDS:RANGE
            "TRSCR"  : _str(line,529,  3),   # TRACK RECORDS:SCORE
            "TRWGT"  : _str(line,535,  3),   # TRACK RECORDS:WEIGHT
            "TRTOTSC": _str(line,541,  3),   # TRACK RECORDS:TOTAL SCORE
            "TRYY"   : _str(line,547,  4),   # TRACK RECORDS:YEAR
            "TRPRD"  : _str(line,554, 10),   # TRACKS RECORDS:PERIOD
            "CARNGE" : _str(line,567, 40),   # ACCT WITH OTHER FI: RANGE
            "CASCR"  : _str(line,610,  3),   # ACCT WITH OTHER FI:SCORE
            "CAWGT"  : _str(line,616,  3),   # ACCT WITH OTHER FI:WEIGHT
            "CATOTSC": _str(line,622,  3),   # ACCT WITH OTHER FI:TOL SC
            "LSRNGE" : _str(line,628, 60),   # LIQUIDITY OF SEC:RANGE
            "LSSCR"  : _str(line,691,  3),   # LIQUIDITY OF SEC:SCORE
            "LSWGT"  : _str(line,697,  3),   # LIQUIDITY OF SEC:WEIGHT
            "LSTOTSC": _str(line,703,  3),   # LIQUIDITY OF SEC:TOL SC
            "MSRNGE" : _str(line,709, 20),   # MARGIN OF ADV(SG):RANGE
            "MSSCR"  : _str(line,732,  3),   # MARGIN OF ADV(SG):SCORE
            "MSWGT"  : _str(line,738,  3),   # MARGIN OF ADV(SG):WEIGHT
            "MSTOTSC": _str(line,744,  3),   # MARGIN OF ADV(UG):TOT SC
            "MURNGE" : _str(line,750, 20),   # MARGIN OF ADV(UG):RANGE
            "MUSCR"  : _str(line,773,  3),   # MARGIN OF ADV(UG):SCORE
            "MUWGT"  : _str(line,779,  3),   # MARGIN OF ADV(UG):WEIGHT
            "MUTOTSC": _str(line,785,  3),   # MARGIN OF ADV(UG):TOL SC
            "CARNGE1": _str(line,791, 30),   # CONDUCT OF ACCT:RANGE
            "CASCR1" : _str(line,824,  3),   # CONDUCT OF ACCT:SCORE
            "CAWGT1" : _str(line,830,  3),   # CONDUCT OF ACCT:WEIGHT
            "CATOTSC1": _str(line,836, 3),   # CONDUCT OF ACCT:TOT SCORE
            "MSTRNGE": _str(line,842,  5),   # MNGMNT STRENGHT:RANGE
            "MSTSCR" : _str(line,850,  3),   # MNGMNT STRENGHT:SCORE
            "MSTWGT" : _str(line,856,  3),   # MNGMNT STRENGHT:WEIGHT
            "MSTTOSC": _str(line,862,  3),   # MNGMNT STRENGHT:TOL SCORE
            "GARNGE" : _str(line,868, 20),   # GROWTH IN NET ASS:RANGE
            "GASCR"  : _str(line,891,  3),   # GROWTH IN NET ASS:SCORE
            "GAWGT"  : _str(line,897,  3),   # GROWTH IN NET ASS:WEIGHT
            "GATOTSC": _str(line,903,  3),   # GROWTH IN NET ASS:TOT SC
            "CRRNGE" : _str(line,909, 20),   # CURRENT RATIO:RANGE
            "CRSCR"  : _str(line,932,  3),   # CURRENT RATIO:SCORE
            "CRWGT"  : _str(line,938,  3),   # CURRENT RATIO:WEIGHT
            "CRTOTSC": _str(line,944,  3),   # CURRENT RATIO:TOTAL SCORE
            "DCRNGE" : _str(line,950, 15),   # DEBT COVER RATIO:RANGE
            "DCSCR"  : _str(line,968,  3),   # DEBT COVER RATIO:SCORE
            "DCWGT"  : _str(line,974,  3),   # DEBT COVER RATIO:WEIGHT
            "DCTOTSC": _str(line,980,  3),   # DEBT COVER RATIO:TOL SC
            "TORNGE" : _str(line,986,  5),   # TURNOVER:RANGE
            "TOSCR"  : _str(line,994,  3),   # TURNOVER:SCORE
            "TOWGT"  : _str(line,1000, 3),   # TURNOVER:WEIGHT
            "TOTOTSC": _str(line,1006, 3),   # TURNOVER:TOTAL SCORE
        })
    return _dedup(pl.DataFrame(rows))


def read_elna20() -> pl.DataFrame:
    """ELNA20 – Additional margin/CRR scores (ELN20W.txt, fixed-width)."""
    lines = _read_lines(INPUT_FILES[20])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"      : _str(line,   1, 13),  # MAIN AA NO
            "MARNGE"     : _str(line,  17, 80),  # MARGIN OF ADVANCE: RANGE
            "MASCR"      : _str(line, 100,  3),  # MARGIN OF ADVANCE: SCORE
            "MAWGT"      : _str(line, 106,  3),  # MARGIN OF ADVANCE: WEIGHT
            "MATOTSC"    : _str(line, 112,  3),  # MARGIN OF ADVANCE:TOLSCORE
            "MARNGE2"    : _str(line, 118, 30),  # MARGIN OF ADVANCE 2:RANGE
            "MASCR2"     : _str(line, 151,  3),  # MARGIN OF ADVANCE 2:SCORE
            "MAWGT2"     : _str(line, 157,  3),  # MARGIN OF ADVANCE 2:WEIGHT
            "MATOTSC2"   : _str(line, 163,  3),  # MARGIN OF ADVANCE 2:TSCORE
            "NCRNGE"     : _str(line, 169, 80),  # NETCASHFLOW(AUD):RANGE
            "NCTOTSC"    : _str(line, 252,  3),  # NETCASHFLOW(AUD):TOL SCORE
            "CRRBRANCH"  : _str(line, 258,  5),  # CRR BRANCH
            "CRRCOD"     : _str(line, 266,  5),  # CRR COD/HEAD OFFICE
            "ASGIRANGE"  : _str(line, 274, 30),  # ANNUAL GROSS INCOME:RANGE
            "ASGISCORE"  : _str(line, 307,  3),  # ANNUAL GROSS INCOME:SCORE
            "ASGIWEIGHT" : _str(line, 313,  3),  # ANNUAL GROSS INCOME:WEIGHT
            "ASGITSCORE" : _str(line, 319,  3),  # ANNUAL GROSS INCOME:TSCORE
            "TYINRANGE"  : _str(line, 325, 50),  # TYPE OF INCOME:RANGE
            "TYINSCORE"  : _str(line, 378,  3),  # TYPE OF INCOME:SCORE
            "TYINWEIGHT" : _str(line, 384,  3),  # TYPE OF INCOME:WEIGHT
            "TYINTSCORE" : _str(line, 390,  3),  # TYPE OF INCOME:TOT SCORE
            "NTJBRANGE"  : _str(line, 396, 90),  # NATURAL OF JOB:RANGE
            "NTJBSCORE"  : _str(line, 489,  3),  # NATURAL OF JOB:SCORE
            "NTJBWEIGHT" : _str(line, 495,  3),  # NATURAL OF JOB:WEIGHT
            "NTJBTSCORE" : _str(line, 501,  3),  # NATURAL OF JOB:TOT SCORE
            "FINRANGE"   : _str(line, 507, 30),  # FINANCIAL RESOURCE:RANGE
            "FINSCORE"   : _str(line, 540,  3),  # FINANCIAL RESOURCE:SCORE
            "FINWEIGHT"  : _str(line, 546,  3),  # FINANCIAL RESOURCE:WEIGHT
            "FINTSCORE"  : _str(line, 552,  3),  # FINANCIAL RESOURCE:TSCORE
            "UTILRANGE"  : _str(line, 558, 30),  # UTILIZATION RATE CC:RANGE
            "UTILSCORE"  : _str(line, 591,  3),  # UTILIZATION RATE CC:SCORE
            "UTILWEIGHT" : _str(line, 597,  3),  # UTILIZATION RATE CC:WEIGHT
            "UTILTSCORE" : _str(line, 603,  3),  # UTILIZATION RATE CC:TSCORE
            "TLPLRANGE"  : _str(line, 609, 30),  # TOL OUTSTND BAL PL:RANGE
            "TLPLSCORE"  : _str(line, 642,  3),  # TOL OUTSTND BAL PL:SCORE
            "TLPLWEIGHT" : _str(line, 648,  3),  # TOL OUTSTND BAL PL:WEIGHT
            "TLPLTSCORE" : _str(line, 654,  3),  # TOL OUTSTND BAL PL:TSCORE
            "AUDITOPN"   : _str(line, 660,  1),  # ADVERSE AUDITOR OPINION
            "EMPAUDITOR" : _str(line, 664,  1),  # EMPHASIS OF MATTER BY AUD
            "AUDLIST"    : _str(line, 668,  1),  # AUD UNDER PBB REFER TO HO
            "LOSSINCUR1" : _str(line, 672,  1),  # LOSSES INCURR FOR 2 YEARS
            "LOSSINCUR2" : _str(line, 676,  1),  # LOSS INCURR MST RECENT YR
            "NEGCFLOW"   : _str(line, 680,  1),  # NEGATIVE CASHFLOW
        })
    return _dedup(pl.DataFrame(rows))


def read_elna21() -> pl.DataFrame:
    """ELNA21 – HP2 NEW / BSA (ELN21W.txt, fixed-width, MISSOVER)."""
    lines = _read_lines(INPUT_FILES[21])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"     : _str(line,   1, 13),  # MAIN AA NO
            "BANKBRC1"  : _str(line,  17, 60),  # BSN:BANK NAME& BRANCH 1
            "BANKBRC2"  : _str(line,  80, 60),  # BSN:BANK NAME& BRANCH 2
            "BANKBRC3"  : _str(line, 143, 60),  # BSN:BANK NAME& BRANCH 3
            "NAMEACC1"  : _str(line, 206, 60),  # BSN:NAME OF ACCT HOLDER1
            "NAMEACC2"  : _str(line, 269, 60),  # BSN:NAME OF ACCT HOLDER2
            "NAMEACC3"  : _str(line, 332, 60),  # BSN:NAME OF ACCT HOLDER3
            "PERIODF1"  : _str(line, 395, 10),  # BSN:PERIOD FROM 1
            "PERIODT1"  : _str(line, 408, 10),  # BSN:PERIOD TO 1
            "PERIODF2"  : _str(line, 421, 10),  # BSN:PERIOD FROM 2
            "PERIODT2"  : _str(line, 434, 10),  # BSN:PERIOD TO 2
            "PERIODF3"  : _str(line, 447, 10),  # BSN:PERIOD FROM 3
            "PERIODT3"  : _str(line, 460, 10),  # BSN:PERIOD TO 3
            "PERCENS1"  : _str(line, 473,  5),  # BSN:PERCENTAGE OF SHARE1
            "PERCENS2"  : _str(line, 481,  5),  # BSN:PERCENTAGE OF SHARE2
            "PERCENS3"  : _str(line, 489,  5),  # BSN:PERCENTAGE OF SHARE3
            "BALUT"     : _str(line, 497, 20),  # BSN:BALANCE OF UNITTRUST
            "MODEL"     : _str(line, 520, 20),  # MODEL
            "CATEGORY"  : _str(line, 543, 15),  # CATEGORY
            "GOODS"     : _str(line, 561, 15),  # GOODS TO BE USE
            "COLLACODE" : _str(line, 579, 70),  # COLLATERAL CODE
            "BDM"       : _str(line, 652,  7),  # BDM
            "VEHMAKE"   : _str(line, 662, 20),  # GOODS:VEHICLE MAKE
            "VEHRLIST"  : _str(line, 685,  3),  # GOODS:VEHICLE REFER LIST
            "AGEVEH"    : _str(line, 691, 30),  # GOODS:AGE OF VEHICLE
            "VERIFIAP"  : _str(line, 724,  3),  # GOODS:VERIFICATION OF AP
            "APHOLDER"  : _str(line, 730, 40),  # GOODS:AP HOLDER
            "PEKEMA"    : _str(line, 773,  3),  # GOODS:PEKEMA MEMBER
            "NOPEKEMA"  : _str(line, 779, 40),  # GOODS:IF NOT PEKEMA
            "COLLCOND"  : _str(line, 822, 25),  # GOODS:COLLATERAL COND.
            "EQUIPTYP"  : _str(line, 850, 30),  # GOODS:EQUIPMENT TYPE
            "CLASSIFI"  : _str(line, 883, 20),  # GOODS:CLASSIFICATION
            "CASHDEAL"  : _str(line, 906,  3),  # GOODS:CASH DEAL
            "YEARMADE"  : _str(line, 912,  4),  # GOODS:YEAR MADE
            "REGNO"     : _str(line, 919, 25),  # GOODS:REGISTRATION NO.
            "LGBOOKNO"  : _str(line, 947, 10),  # GOODS:LOG BOOK NUMBER
            "ENGINENO"  : _str(line, 960, 20),  # GOODS:ENGINE NO.
            "CHASSIS"   : _str(line, 983, 20),  # GOODS:CHASSIS/SERIAL NO
            "CC"        : _str(line,1006,  5),  # GOODS:CUBIC CAPACITY
            "REGDATE"   : _str(line,1014, 10),  # GOODS:REGISTRATION DATE
            "NEWVHTNC"  : _str(line,1027,  1),  # GOODS:NEW VEH.TERMS&COND
            "COLOUR"    : _str(line,1031, 15),  # GOODS:COLOUR
            "FUELTYPE"  : _str(line,1049, 15),  # GOODS:FUEL TYPE
            "TYPOARRG"  : _str(line,1067,  6),  # GOODS:TYPE OF ARRANGEMENT
            "ROADTEXP"  : _str(line,1076,  3),  # GOODS:ROADTAX EXPIRED
            "RDTEXPREM" : _str(line,1082, 60),  # GOODS:ROADTAX EXP.REMARK
            "REGCDSS"   : _str(line,1145,  3),  # GOODS:REG.CARD SAMB
            "REGCDSSRM" : _str(line,1151, 60),  # GOODS:REG.CARD SAMB REMK
            "DLNAME"    : _str(line,1214, 60),  # GOODS:SELLER/DEALER NAME
            "DLIDTYPE"  : _str(line,1277, 30),  # GOODS:SELLER/DLR ID TYPE
            "DLID"      : _str(line,1310, 40),  # GOODS:SELLER/DEALER ID
            "DLREGSS"   : _str(line,1353,  3),  # GOODS:S/D REG BIZ SBH&SW
            "LRNAME"    : _str(line,1359, 40),  # GOODS:LAST REG.OWNR NAME
            "LRIDTYPE"  : _str(line,1402, 30),  # GOODS:'' REG.OWNR ID TYP
            "LRID"      : _str(line,1435, 40),  # GOODS:'' REG.OWNR ID
            "ROREGSS"   : _str(line,1478,  3),  # GOODS:'' REG BIZ SBH&SWK
            "RELLRAPP"  : _str(line,1484, 40),  # GOODS:RELATION OF LR-APP
            "SRCBSNS"   : _str(line,1527, 20),  # GOODS:SOURCE OF BUSINESS
            "DLCODE"    : _str(line,1550, 15),  # GOODS:DEALER'S CODE
            "TYPEODL"   : _str(line,1568, 50),  # GOODS:TYPE OF DEALER
            "SRCVEH"    : _str(line,1621, 40),  # GOODS:SOURCE OF VEHICLE
            "SLNAME"    : _str(line,1664, 40),  # GOODS:SALESMAN NAME
            "SLIDTYPE"  : _str(line,1707, 10),  # GOODS:SALESMAN ID TYPE
            "SLID"      : _str(line,1720, 40),  # GOODS:SALESMAN ID
            "GDTBKA"    : _str(line,1763,120),  # GOODS:GOODS 2 BE KEPT AT
            "STATE"     : _str(line,1886, 20),  # GOODS:STATE
            "VEHSPEC"   : _str(line,1909, 60),  # VEHICLE SPEC
            "VEHREFLIST": _str(line,1972,  3),  # VEHICLE REFERLIST
            "AACRTDBY"  : _str(line,1978, 60),  # AACREATEDBY(MOBILE)
            "AACRTDTM"  : _str(line,2041, 60),  # AACREATEDTIME(MOBILE)
            "AASBMTBY"  : _str(line,2104, 60),  # AASUBMITTEDBY(MOBILE)
            "AASBMTTM"  : _str(line,2167, 60),  # AASUBMITTEDTIME(MOBILE)
            "AASBMISN"  : _str(line,2230,  5),  # TatAASUBMiSSION
        })
    return _dedup(pl.DataFrame(rows))


def read_elna22() -> pl.DataFrame:
    """ELNA22 – Sole proprietor / partners / directors (ELN22W.txt, fixed-width, MISSOVER)."""
    lines = _read_lines(INPUT_FILES[22])
    rows = []
    for line in lines[1:]:
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append({
            "MAANO"      : _str(line,   1, 13),  # MAIN AA NO
            "TYPEIDENT"  : _str(line,  80,  2),  # TYPE OF IDENTIFICATION
            "IDNO"       : _str(line,  85, 50),  # ID NO.
            "MONINCOME"  : _str(line, 138, 20),  # MONTHLY INCOME
            "DOB"        : _str(line, 161, 10),  # DATE OF BIRTH
            "READDRESS"  : _str(line, 174,150),  # RESIDENTIAL ADDRESS
            "NATION"     : _str(line, 327, 55),  # NATIONALITY
            "POHELD"     : _str(line, 385, 15),  # POSITION HELD
            "PERCENSHA"  : _str(line, 403,  5),  # PERCENTAGE OF SHAREHOLDING
            "AGE"        : _str(line, 411,  3),  # AGE
            "OCCUPATION" : _str(line, 417, 40),  # OCCUPATION
            "PLML"       : _str(line, 460, 10),  # PL/ML
            "INCOMEPA"   : _str(line, 473, 15),  # INCOME P.A
            "NETWORTH"   : _str(line, 491, 45),  # ESTIMATED NETWORTH
            "APPOINTDT"  : _str(line, 539, 10),  # DATE OF APPOINTMENT
            "SPPSIBITYPE": _str(line, 552,  3),
            "SPPSIBINUM" : _str(line, 557, 12),
            "DSR"        : _str(line, 572,  8),
            "GROSSINC"   : _str(line, 583, 15),
            "DSRAMEND"   : _str(line, 601,  8),
            "APPNAME"    : _str(line, 612,150),  # LONG NAME
        })
    return _dedup(pl.DataFrame(rows))


# ---------------------------------------------------------------------------
# CSV-DELIMITED READERS (ELNA23 – ELNA33)
# ---------------------------------------------------------------------------

def _read_csv_file(path: str, col_specs: list[tuple]) -> pl.DataFrame:
    """
    Read a comma-delimited file with DSD/MISSOVER semantics.
    col_specs: list of (name, dtype_hint) where dtype_hint is 'str', 'num', or 'date'.
    Returns a polars DataFrame with all columns as strings (date parsing done separately
    for columns with 'date' hint).
    """
    import csv
    with open(path, "r", encoding="latin-1") as fh:
        reader = csv.reader(fh, quotechar='"', skipinitialspace=True)
        rows_raw = list(reader)

    if len(rows_raw) < 2:
        return pl.DataFrame({name: pl.Series([], dtype=pl.Utf8) for name, _ in col_specs})

    data_rows = rows_raw[1:]   # FIRSTOBS=2
    col_names = [s[0] for s in col_specs]
    col_hints  = [s[1] for s in col_specs]
    n_cols = len(col_names)

    result: dict[str, list] = {c: [] for c in col_names}
    for raw in data_rows:
        for i, col in enumerate(col_names):
            val = raw[i].strip() if i < len(raw) else ""
            hint = col_hints[i]
            if hint == "date":
                parsed = None
                for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
                    try:
                        parsed = datetime.strptime(val, fmt).date()
                        break
                    except ValueError:
                        pass
                result[col].append(parsed)
            elif hint == "num":
                try:
                    result[col].append(float(val.replace(",", "")))
                except ValueError:
                    result[col].append(None)
            else:
                result[col].append(val.upper() if val else "")

    df = pl.DataFrame(result)
    return df


def read_elna23() -> pl.DataFrame:
    """ELNA23 – Appeal data (ELN23W.txt, CSV, comma-delimited)."""
    specs = [
        ("MAANO",               "str"),
        ("AANO",                "str"),
        ("APPL_ST",             "str"),
        ("FACILITY",            "str"),
        ("LIMIT",               "num"),
        ("BNM_EIR",             "str"),
        ("PRICING",             "str"),
        ("SYMBOL_SC",           "str"),
        ("STATUS",              "str"),
        ("APRV_NAME1",          "str"),
        ("APRV_DESG1",          "str"),
        ("STATUS_DT1",          "date"),
        ("APRV_NAME2",          "str"),
        ("APRV_DESG2",          "str"),
        ("STATUS_DT2",          "date"),
        ("APP_REJ_DT",          "date"),
        ("RETURN_DT",           "date"),
        ("CR_COM_MEET_DT",      "date"),
        ("NOT_VETOED_BY_BOARD", "date"),
        ("APRV_LEV",            "str"),
        ("PROCESS_OFFICER",     "str"),
        ("HOE_MARGIN",          "str"),
        ("EARMARKING_MARGIN",   "str"),
        ("APPEALNO",            "str"),
    ]
    return _dedup(_read_csv_file(INPUT_FILES[23], specs))


def read_elna24() -> pl.DataFrame:
    """ELNA24 – CRR AA data (ELN24W.txt, CSV)."""
    specs = [
        ("MAANO",           "str"),
        ("CRR_FORM",        "str"),
        ("CRR_LEVEL",       "str"),
        ("CRR_ITEM",        "str"),
        ("CRR_FACTOR",      "str"),
        ("CRR_VALUE",       "str"),
        ("CRR_RANGE",       "str"),
        ("CRR_SCORE",       "str"),
        ("CRR_WEIGHT",      "str"),
        ("CRR_TOTSCORE",    "str"),
        ("CRR_GRADE",       "str"),
        ("CRR_SUBSECSCORE", "str"),
        ("CRR_DENOMINATOR", "str"),
        ("CRR_PERCENT",     "str"),
        ("CRR_OWNER",       "str"),
        ("CRR_CATEGORY",    "str"),
        ("CMV",             "str"),
    ]
    return _dedup(_read_csv_file(INPUT_FILES[24], specs))


def read_elna25() -> pl.DataFrame:
    """ELNA25 – Application status (ELN25W.txt, CSV)."""
    specs = [
        ("MAANO",      "str"),
        ("APP_ST",     "str"),
        ("APP_ACTION", "str"),
        ("APP_BYWHO",  "str"),
        ("APP_DT",     "date"),
        ("APP_TIME",   "str"),
    ]
    return _dedup(_read_csv_file(INPUT_FILES[25], specs))


def read_elna26() -> pl.DataFrame:
    """ELNA26 – Support factor (ELN26W.txt, CSV)."""
    specs = [
        ("MAANO", "str"),
        ("FIELD", "str"),
        ("VALUE", "str"),
    ]
    return _dedup(_read_csv_file(INPUT_FILES[26], specs))


def read_elna27() -> pl.DataFrame:
    """ELNA27 – Loan repayment (ELN27W.txt, CSV)."""
    specs = [
        ("MAANO",              "str"),
        ("NAME",               "str"),
        ("ID",                 "str"),
        ("BORROWERTYPE",       "str"),
        ("NEWIND",             "str"),
        ("FACILITY",           "str"),
        ("LIMIT",              "num"),
        ("INTEREST_RT",        "num"),
        ("TENURE",             "str"),
        ("DERIVED_INSTALMENT", "num"),
        ("ACTUAL_INSTALMENT",  "num"),
        ("BNMINCDOC",          "str"),
        ("DEVEPF",             "str"),
        ("DEVCASA",            "str"),
        ("DEVOTHS",            "str"),
        ("DEVPAY",             "num"),
        ("DEVPAY2",            "str"),
        ("DEVPAY3",            "str"),
        ("DEVPAY4",            "str"),
        ("DEVPAY5",            "str"),
    ]
    return _dedup(_read_csv_file(INPUT_FILES[27], specs))


def read_elna28() -> pl.DataFrame:
    """ELNA28 – Account conduct (ELN28W.txt, CSV)."""
    specs = [
        ("MAANO",      "str"),
        ("NAME",       "str"),
        ("ID",         "str"),
        ("BRRWERTYPE", "str"),
        ("SINCEDT",    "str"),
        ("ACCNATURE",  "str"),
        ("ACCHOLDER",  "str"),
        ("ODLIMIT",    "num"),
        ("AMD",        "num"),
        ("ADB",        "str"),
        ("EXCESSFREQ", "str"),
        ("CHQRTRN",    "str"),
        ("ACCACTVTY",  "str"),
        ("CACONDUCT",  "str"),
        ("REPAYMENT",  "str"),
        ("BTCONDUCT",  "str"),
    ]
    return _dedup(_read_csv_file(INPUT_FILES[28], specs))


def read_elna29() -> pl.DataFrame:
    """ELNA29 – Reason AA (ELN29W.txt, CSV)."""
    specs = [
        ("MAANO",     "str"),
        ("REASONFOR", "str"),
        ("REASON",    "str"),
    ]
    return _dedup(_read_csv_file(INPUT_FILES[29], specs))


def read_elna30() -> pl.DataFrame:
    """ELNA30 – Checklist data (ELN30W.txt, CSV)."""
    specs = [
        ("MAANO",    "str"),
        ("CATEGORY", "str"),
        ("LISTNO",   "str"),
        ("DESC",     "str"),
        ("VALUE",    "str"),
    ]
    return _dedup(_read_csv_file(INPUT_FILES[30], specs))


def read_elna31() -> pl.DataFrame:
    """ELNA31 – Related company data (ELN31W.txt, CSV)."""
    specs = [
        ("MAANO",         "str"), ("NAMEOFCOMP",  "str"), ("TYPEOFID",    "str"),
        ("IDNO",          "str"), ("COUNTRY",     "str"), ("DATEOFREG",   "str"),
        ("SECCODE",       "str"), ("ADDRESS",     "str"), ("CONTACTNOHSE","str"),
        ("CONTACTNOOFF",  "str"), ("CONTACTNOMOB","str"), ("CONTACTNOOVER","str"),
        ("NEWINCORPCOMP", "str"), ("PRIMEMOVER",  "str"), ("GUARANTOR",   "str"),
        ("PROMOTER",      "str"), ("HOLDCOMP",    "str"), ("CCRISREP",    "str"),
        ("BUSSDESC",      "str"), ("BUSSSINCE",   "str"), ("PROXY",       "str"),
        ("NAMEOFPRIME",   "str"), ("RACE",        "str"), ("EXPSINCE",    "str"),
        ("APPBUSS",       "str"), ("GOODCOND",    "str"), ("MONTH",       "str"),
        ("YEAR",          "str"), ("GOODCOND2",   "str"), ("MONTH2",      "str"),
        ("YEAR2",         "str"), ("PXACC",       "str"), ("PXACCDES",    "str"),
        ("PXRLTDACC",     "str"), ("PXRLTDACCDES","str"), ("DPXACC",      "str"),
        ("DPXRLTDACC",    "str"), ("EPXACC",      "str"), ("EPXACCDES",   "str"),
        ("EPXRLTDACC",    "str"), ("EPXRLTDACCDES","str"),("CRRSCORE",    "str"),
        ("CRRGRADE",      "str"), ("DEBTOR",      "str"), ("RM030",       "str"),
        ("PCT030",        "str"), ("RM3160",      "str"), ("PCT3160",     "str"),
        ("RM6190",        "str"), ("PCT6190",     "str"), ("RM90",        "str"),
        ("PCT90",         "str"), ("RM060",       "str"), ("PCT060",      "str"),
        ("RM61120",       "str"), ("PCT61120",    "str"), ("RM121180",    "str"),
        ("PCT121180",     "str"), ("RM180",       "str"), ("PCT180",      "str"),
        ("TOTALRM",       "str"), ("TOTALPCT",    "str"), ("ADAUOPI",     "str"),
        ("EMMATAUD",      "str"), ("AUDUNDPBB",   "str"), ("CONDOFCA",    "str"),
        ("CONDREPAY",     "str"), ("CONDTRDBIL",  "str"), ("BRISCTOS",    "str"),
        ("BRISCTOSDES",   "str"), ("CCRIS",       "str"), ("DCHEQS",      "str"),
        ("EXLGLACT",      "str"), ("EXLGLACTDES", "str"), ("SOURCE",      "str"),
    ]
    return _dedup(_read_csv_file(INPUT_FILES[31], specs))


def read_elna32() -> pl.DataFrame:
    """ELNA32 – Related company financials (ELN32W.txt, CSV).
    Adds SEQNO per MAANO / IDNO / SOURCE."""
    specs = [
        ("MAANO",        "str"), ("NAMEOFCOMP",   "str"), ("TYPEOFID",     "str"),
        ("IDNO",         "str"), ("FINYRENDED",   "str"), ("AUMD",         "str"),
        ("AUDITOR",      "str"), ("AUDITORREGNO", "str"), ("TURNOVER",     "num"),
        ("PRETAXNET",    "num"), ("PAIDUPCAP",    "num"), ("NETWORTH",     "num"),
        ("TOTCURRASS",   "num"), ("TOTCURRLIAB",  "num"), ("NETWORKCAP",   "num"),
        ("STOCKTURN",    "num"), ("COLLPERIOD",   "num"), ("PAYPERIOD",    "num"),
        ("SALESGWTH",    "str"), ("GROSSPRO",     "str"), ("NETPROMRGN",   "str"),
        ("CURRRATIO",    "num"), ("GEARING",      "num"), ("LEVERAGE",     "num"),
        ("DSR",          "num"), ("CTURNOVER",    "num"), ("CGROSSPRO",    "num"),
        ("CPRETAX",      "num"), ("CEXGAINLOSS",  "num"), ("CPAIDUPCAP",   "num"),
        ("CNETWORTH",    "num"), ("CTANGNET",     "num"), ("CREVAREVER",   "num"),
        ("CCURRASSET",   "num"), ("CCURRLIAB",    "num"), ("CDEFFLIAB",    "num"),
        ("CNETOPCASH",   "num"), ("CNETWORKCAP",  "num"), ("CLNGTERMLIAB", "num"),
        ("CEBITDA",      "num"), ("CEBIT",        "num"), ("CDEPRECIAT",   "num"),
        ("CAMORTISTN",   "num"), ("CINTEXPENSE",  "num"), ("CTOTCOMM",     "num"),
        ("CTOTINTBRG",   "num"), ("COPENSTOCK",   "num"), ("CCLSSTOCK",    "num"),
        ("CSTOCK",       "num"), ("CPURCHASES",   "num"), ("CCOSTGOOD",    "num"),
        ("CWORKCAPREQ",  "num"), ("CTRDDBT",      "num"), ("CTRDCRDT",     "num"),
        ("CLNTODRCTOR",  "num"), ("CLNFRMDRCTOR", "num"), ("CINTERCOMPLN", "num"),
        ("CTOTTANGASS",  "num"), ("CTOTLIAB",     "num"), ("CDEVEXP",      "num"),
        ("SOURCE",       "str"),
    ]
    df = _read_csv_file(INPUT_FILES[32], specs)
    df = df.unique()
    df = df.sort(["MAANO", "IDNO", "SOURCE"])

    # SEQNO logic: mimics the SAS DATA step with FIRST.MAANO / FIRST.IDNO / FIRST.SOURCE
    # In SAS: each FIRST.x resets to 1; ELSE SEQNO+1
    # Effective behaviour: SEQNO = 1 whenever any of the three BY-keys changes
    seqno = []
    prev = (None, None, None)
    seq = 0
    for row in df.select(["MAANO", "IDNO", "SOURCE"]).iter_rows():
        maano, idno, source = row
        if maano != prev[0] or idno != prev[1] or source != prev[2]:
            seq = 1
        else:
            seq += 1
        seqno.append(seq)
        prev = row
    df = df.with_columns(pl.Series("SEQNO", seqno, dtype=pl.Int16))
    return df


def read_elna33() -> pl.DataFrame:
    """ELNA33 – Related company account conduct (ELN33W.txt, CSV)."""
    specs = [
        ("MAANO",       "str"),
        ("NAMEOFCOMP",  "str"),
        ("TYPEOFID",    "str"),
        ("IDNO",        "str"),
        ("BANKSINCE",   "str"),
        ("ODLIMIT",     "str"),
        ("AMD",         "str"),
        ("ADBNO",       "str"),
        ("ADBCRDR",     "str"),
        ("FREQOFEXCES", "str"),
        ("NOOFCHEQUE",  "str"),
        ("ACTOFACC",    "str"),
        ("SOURCE",      "str"),
    ]
    return _dedup(_read_csv_file(INPUT_FILES[33], specs))


# ---------------------------------------------------------------------------
# MAIN PROCESSING
# ---------------------------------------------------------------------------

def main():
    # Read report date
    dsndt, rptdt = read_reptdate()

    # Read and process all datasets
    datasets = {
        f"ELNA1_{dsndt}"            : read_elna1(dsndt),
        f"ELNA2_{dsndt}"            : read_elna2(),
        f"ELNA3_{dsndt}"            : read_elna3(),
        f"ELNA4_{dsndt}"            : read_elna4(),
        f"ELNA5_{dsndt}"            : read_elna5(),
        f"ELNA6_{dsndt}"            : read_elna6(),   # includes SEQNO
        f"ELNA7_{dsndt}"            : read_elna7(),
        f"ELNA8_{dsndt}"            : read_elna8(),
        f"ELNA9_{dsndt}"            : read_elna9(),
        f"ELNA10_{dsndt}"           : read_elna10(),
        f"ELNA11_{dsndt}"           : read_elna11(),
        f"ELNA12_{dsndt}"           : read_elna12(),
        f"ELNA13_{dsndt}"           : read_elna13(),
        f"ELNA14_{dsndt}"           : read_elna14(),
        f"ELNA15_{dsndt}"           : read_elna15(),
        f"ELNA16_{dsndt}"           : read_elna16(),
        f"ELNA17_{dsndt}"           : read_elna17(),
        f"ELNA18_{dsndt}"           : read_elna18(),
        f"ELNA19_{dsndt}"           : read_elna19(),
        f"ELNA20_{dsndt}"           : read_elna20(),
        f"ELNA21_{dsndt}"           : read_elna21(),
        f"ELNA22_{dsndt}"           : read_elna22(),
        f"APPEAL_AA_{dsndt}"        : read_elna23(),
        f"CRR_AA_{dsndt}"           : read_elna24(),
        f"APP_STATUS_AA_{dsndt}"    : read_elna25(),
        f"SUPPORT_FACTOR_AA_{dsndt}": read_elna26(),
        f"LOANREPAYMENT_AA_{dsndt}" : read_elna27(),
        f"ACC_CONDUCT_AA_{dsndt}"   : read_elna28(),
        f"REASON_AA_{dsndt}"        : read_elna29(),
        f"ELNA30_{dsndt}"           : read_elna30(),
        f"ELNA31_{dsndt}"           : read_elna31(),
        f"ELNA32_{dsndt}"           : read_elna32(),  # includes SEQNO
        f"ELNA33_{dsndt}"           : read_elna33(),
    }

    # Rename keys using RPTDT suffix (mirrors SAS CHANGE statement)
    # Only the subset that is selected in PROC DATASETS COPY is renamed
    rename_map = {
        f"ELNA1_{dsndt}"            : f"ELNA1_{rptdt}",
        f"ELNA2_{dsndt}"            : f"ELNA2_{rptdt}",
        f"ELNA3_{dsndt}"            : f"ELNA3_{rptdt}",
        f"ELNA4_{dsndt}"            : f"ELNA4_{rptdt}",
        f"ELNA6_{dsndt}"            : f"ELNA6_{rptdt}",
        f"ELNA7_{dsndt}"            : f"ELNA7_{rptdt}",
        f"ELNA8_{dsndt}"            : f"ELNA8_{rptdt}",
        f"ELNA10_{dsndt}"           : f"ELNA10_{rptdt}",
        f"ELNA12_{dsndt}"           : f"ELNA12_{rptdt}",
        f"ELNA13_{dsndt}"           : f"ELNA13_{rptdt}",
        f"ELNA15_{dsndt}"           : f"ELNA15_{rptdt}",
        f"ELNA16_{dsndt}"           : f"ELNA16_{rptdt}",
        f"ELNA17_{dsndt}"           : f"ELNA17_{rptdt}",
        f"ELNA22_{dsndt}"           : f"ELNA22_{rptdt}",
        f"APPEAL_AA_{dsndt}"        : f"APPEAL_AA_{rptdt}",
        f"CRR_AA_{dsndt}"           : f"CRR_AA_{rptdt}",
        f"APP_STATUS_AA_{dsndt}"    : f"APP_STATUS_AA_{rptdt}",
        f"SUPPORT_FACTOR_AA_{dsndt}": f"SUPPORT_FACTOR_AA_{rptdt}",
        f"LOANREPAYMENT_AA_{dsndt}" : f"LOANREPAYMENT_AA_{rptdt}",
        f"ACC_CONDUCT_AA_{dsndt}"   : f"ACC_CONDUCT_AA_{rptdt}",
        f"REASON_AA_{dsndt}"        : f"REASON_AA_{rptdt}",
        f"ELNA30_{dsndt}"           : f"ELNA30_{rptdt}",
        f"ELNA31_{dsndt}"           : f"ELNA31_{rptdt}",
        f"ELNA32_{dsndt}"           : f"ELNA32_{rptdt}",
        f"ELNA33_{dsndt}"           : f"ELNA33_{rptdt}",
    }

    # Write parquet outputs and collect the RPTDT-named datasets for TRANFILE
    tranfile_datasets: dict[str, pl.DataFrame] = {}

    for ds_name, df in datasets.items():
        # Write all datasets with the original DSNDT-suffixed name
        _write_parquet(df, ds_name)

        # Write renamed (RPTDT) parquet for the selected subset
        if ds_name in rename_map:
            rptdt_name = rename_map[ds_name]
            _write_parquet(df, rptdt_name)
            tranfile_datasets[rptdt_name] = df

    # -----------------------------------------------------------------------
    # PROC CPORT equivalent: serialize the selected & renamed datasets into
    # a single binary transport file (TRANFILE / ELNFTPW.dat).
    # SAS CPORT produces a proprietary binary format; here we use pickle as
    # a portable Python equivalent, since the downstream consumer of this
    # file must be updated accordingly.
    # -----------------------------------------------------------------------
    with open(TRANFILE_PATH, "wb") as fh:
        pickle.dump(tranfile_datasets, fh, protocol=pickle.HIGHEST_PROTOCOL)

    print(f"Processing complete. DSNDT={dsndt}, RPTDT={rptdt}")
    print(f"Output parquet files written to: {OUTPUT_DIR}")
    print(f"Transport file written to      : {TRANFILE_PATH}")


if __name__ == "__main__":
    main()
