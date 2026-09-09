#!/usr/bin/env python3
"""
Program : EIBMLI4I.py
Purpose : ADHOC (Islamic Part 4) - Stock of Liquefiable Assets report for
          BNM statutory liquidity-framework classification (Report ID:
          EIBMLIQ4). Classifies portfolio positions into liquidity classes
          A-F/R/X/Y, computes discount/pricing (Appendix 1 bond formulae),
          and reports market value, discounted value and remaining
          maturity by security type and BNM liquidity class.

Dependency:
    %INC PGM(MATDTEX);  -> from MATDTEX import calc_remmth
        Source-library member (//PGM DD DSN=SAP.BNM.PROGRAM), NOT a runtime
        data input. Textually inserted after PROC SORT in the original SAS;
        overwrites LIQCLASS.REMMTH (previously a continuous value) with a
        1-6 maturity-band classification. Converted to its own module,
        MATDTEX.py, and imported here.

============================================================================
INPUT CONFIRMATION -- FLAT FILE (NOT .sas7bdat)
============================================================================
//BNMTBL4 DD DSN=SAP.PBB.KAPITI4(0),DISP=SHR  -- GDG(0) = latest generation.
The SAS INFILE/INPUT statement reads this by ABSOLUTE BYTE OFFSET, with
several fields declared as PDw.d (packed-decimal / COMP-3 binary), e.g.
"@27 UTCPR PD6.7". This is unambiguous proof the physical input is a raw
mainframe flat file, not a .sas7bdat dataset -- per project convention this
requires byte-offset slicing and packed-decimal unpacking, never
read_parquet()/read_csv() on the raw file. The GDG(0) reference (no literal
date token in the DSN) means the "latest" physical copy must be resolved by
directory scan, so input_date.get_latest_file() is used here (per project
convention: deterministic dates are built directly; get_latest_file() is
reserved for exactly this GDG-style "scan for latest" case).

The parsed rows are still cached to Parquet (chunked read -> parse -> write,
DuckDB reads the cache afterwards) to avoid re-parsing the raw flat file on
every run, mirroring the EIBDLN1M / EIIMRM01 sas7bdat->Parquet cache pattern
-- adapted here to a raw byte-offset + packed-decimal source instead of a
pandas read_sas() source.

//PGM DD DSN=SAP.BNM.PROGRAM,DISP=SHR is the source-code library used only
for the %INC PGM(MATDTEX) compile-time include above; it is not read as
data at runtime and has no Python counterpart beyond the MATDTEX.py import.

============================================================================
REPORT DATE
============================================================================
DATA REPTDATE; INFILE BNMTBL4 OBS=1; INPUT @113 UTRPT $10.;
REPTDATE=INPUT(UTRPT,DDMMYY10.);
This reads ONLY the first physical record of the flat file and derives the
report date from the record's own embedded UTRPT field (byte 113, 10 chars).
This is the authoritative, data-driven "as-at" date for the whole batch.
REPTDATE.py's get_reptdate_values() ("today - 1") is intentionally NOT used
to compute this program's REPTDATE -- substituting a calendar default here
would silently diverge from the batch's actual as-of date embedded in the
source file. get_reptdate_values() is not imported, for the same reason
(there is nothing correct to import it for in this program).

============================================================================
OUTPUT
============================================================================
//SASLIST DD SYSOUT=X -- a printed report only (PROC PRINT + PROC TABULATE),
no dated output dataset. OPTIONS NOCENTER YEARCUTOFF=1950 PS=60 LS=132;
no explicit RECFM=FBA override -> default SAS print-file behaviour applies,
i.e. every output line carries a leading ASA carriage-control byte ('1' =
new page/form-feed, ' ' = single space). Since the filename has no date
token, output_date.build_output_file() is not applicable here (per project
convention: fixed/catalogued output names stay fixed) -- OUTPUT_FILE is a
plain fixed name, EIBMLI4I.txt.

============================================================================
KNOWN SAS SOURCE QUIRKS -- PRESERVED VERBATIM (dead-code preservation)
============================================================================
- "IF CLASS NE ' ' OR CLASS NE . ;" (near the top of the pricing DATA step)
  uses OR of two conditions that can never both be false for any value of a
  character variable -- this subsetting IF is always TRUE (a no-op, likely
  meant as AND). Preserved: no filtering happens at this point.
- "IF _N_=1 THEN DO; SET REPTDATE; RPYR=...; RPMTH=...; RPDAY=...; IF MOD
  (RPYR,4)=0 THEN RD2=29; END;" re-derives RPYR/RPMTH/RPDAY/RD2 from the
  one-row REPTDATE dataset, but ONLY for the first output row, and these
  variables are never referenced again anywhere else in the program. This
  block (and the near-identical RPYR/RPMTH/RPDAY/RD2 assignment earlier,
  from each row's own UTRPT) has zero effect on the final report and is
  intentionally NOT implemented as active logic, only documented here.
- WHEN('CB1','CNT','SMC','SAC') SLD dead-branch: 'SLD' also appears in the
  later "WHEN('SSD','SLD')" DISTAMT SELECT -- since SAS SELECT/WHEN takes
  the FIRST matching WHEN, 'SLD' always matches the earlier
  WHEN('MGS','CBB','SLD','CB1','CB2','CMB','PNB') DISTAMT formula; the SLD
  branch inside WHEN('SSD','SLD') is unreachable dead code. Preserved as
  coded (see _compute_distamt()).
"""

import gc
from pathlib import Path
from datetime import date, timedelta

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from input_date import get_latest_file
from MATDTEX import calc_remmth

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# Physical input #1: BNMTBL4 -- SAP.PBB.KAPITI4(0), raw flat file, GDG-latest.
INPUT_KAPITI4_DIR = STG_DIR / "sasdata"
INPUT_KAPITI4_PREFIX = "kapiti4"

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMLI4I"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIBMLI4I"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "EIBMLI4I.txt"

CHUNK_ROWS = 500_000
PAGE_SIZE  = 60    # OPTIONS PS=60
LINE_SIZE  = 132   # OPTIONS LS=132

# ============================================================================
# FLAT FILE RECORD LAYOUT (byte offsets are 1-indexed, as in the SAS INPUT
# statement; "pd" = packed-decimal / COMP-3, decoded then divided by 10**d)
# ============================================================================
# (name, start_col, length, type, decimals)
FIELDS = [
    ("UTOSD",   1,  10, "char", None),  # SETTLEMENT/TRANSACTION DATE
    ("UTSMN",  11,  16, "char", None),  # SECURITY MNEMONIC/STOCK CODE
    ("UTCPR",  27,   6, "pd",   7),     # DISCOUNT/COUPON RATE
    ("UTYLD",  33,   4, "pd",   4),     # YIELD
    ("UTFCV",  37,   8, "pd",   2),     # FACE VALUE
    ("UTDCV",  45,   8, "pd",   2),     # COST/CAPITAL VALUE
    ("UTMKV",  53,   8, "pd",   2),     # MARKET VALUE
    ("UTMDT",  61,  10, "char", None),  # MATURITY DATE
    ("UTINT",  71,   8, "pd",   2),     # INTEREST AMOUNT
    ("UTPLS",  79,   8, "pd",   2),     # PROFIT & LOSS
    ("UTSTY",  87,   3, "char", None),  # SECURITY TYPE
    ("UTTTY",  90,   1, "char", None),  # TRANSACTION TYPE
    ("UTITY",  91,   1, "char", None),  # INSTRUMENT TYPE
    ("UTIPI",  92,   1, "char", None),  # INTEREST PAYMENT INDICATOR
    ("UTLCD",  93,  10, "char", None),  # LAST INTEREST PAYMENT DATE
    ("UTNCD", 103,  10, "char", None),  # NEXT INTEREST PAYMENT DATE
    ("UTRPT", 113,  10, "char", None),  # REPORTING DATE
    ("UTSTS", 123,   5, "char", None),  # STATUS
    ("UTAMS", 128,   8, "pd",   2),     # SALES PROCEEDS
    ("UTCLS", 136,   1, "char", None),  # CLASS
    ("UTPGMID",137, 10, "char", None),  # PROGRAM ID
    ("UTIDT", 147,  10, "char", None),  # ISSUE DATE (MM-DD-YYYY)
    ("UTRMD", 157,  10, "char", None),  # REPO MAT DATE
    ("UTAMP", 167,   8, "pd",   2),     # PURCHASE PROCEEDS
    ("UTREF", 175,   4, "char", None),  # PORTFOLIO REF
    ("UTREF1",175,   1, "char", None),  # PORTFOLIO REF, redefinition: 1st char
    ("UTREF4",178,   1, "char", None),  # PORTFOLIO REF, redefinition: 4th char
    ("UTTYP", 191,   3, "char", None),  # PORTFOLIO TYP
]

# Highest referenced field extent (@191 UTTYP $3. -> ends at byte 193). No
# RECFM/LRECL is given explicitly in the JCL/INFILE, so this is the minimum
# record length implied by the layout and is used as-is (no padding assumed).
RECORD_LENGTH = 193

_FIELD_NAMES = [f[0] for f in FIELDS]


def _build_arrow_schema() -> pa.Schema:
    return pa.schema([
        pa.field(name, pa.float64() if ftype == "pd" else pa.string())
        for name, _, _, ftype, _ in FIELDS
    ])


def _unpack_pd(raw: bytes, decimals: int):
    """Unpack a COMP-3 / packed-decimal (PDw.d) field. Each byte holds two
    BCD digits except the last byte, whose low nibble is the sign
    (0xD = negative; 0xC/0xF = positive). Returns None (SAS missing) if the
    bytes are not valid packed decimal (e.g. blank-filled field)."""
    if not raw:
        return None
    digits = []
    for b in raw[:-1]:
        digits.append((b >> 4) & 0x0F)
        digits.append(b & 0x0F)
    last = raw[-1]
    digits.append((last >> 4) & 0x0F)
    sign_nibble = last & 0x0F
    if any(d > 9 for d in digits):
        return None
    value = 0
    for d in digits:
        value = value * 10 + d
    if sign_nibble == 0x0D:
        value = -value
    return value / (10 ** decimals)


def _parse_record(raw: bytes):
    """Parse one RECORD_LENGTH-byte record into a dict, applying the
    subsetting filter 'IF UTREF1 EQ 'I' AND UTREF4 NE '  ';'. Returns None
    if the record does not pass the filter."""
    rec = {}
    for name, start, length, ftype, decimals in FIELDS:
        segment = raw[start - 1:start - 1 + length]
        if ftype == "char":
            rec[name] = segment.decode("latin1")
        else:
            rec[name] = _unpack_pd(segment, decimals)

    if rec["UTREF1"].strip() != "I":
        return None
    # UTREF4 (1 char) compared against '  ' (2 blanks) in SAS: the shorter
    # operand is blank-padded for the comparison, so this is equivalent to
    # "UTREF4 is not blank".
    if rec["UTREF4"].strip() == "":
        return None
    return rec


def _parse_ddmmyy10(s: str):
    """DDMMYY10. informat -- a 10-char date string such as 'DD/MM/YYYY'
    (or '-'/'.' separated). Returns None for blank/unparseable input,
    mirroring SAS producing a missing value (with a log note) rather than
    aborting."""
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    for sep in ("/", "-", "."):
        if sep in s:
            parts = s.split(sep)
            if len(parts) == 3:
                dd, mm, yyyy = parts
                try:
                    return date(int(yyyy), int(mm), int(dd))
                except ValueError:
                    return None
    if len(s) == 8 and s.isdigit():
        try:
            return date(int(s[4:8]), int(s[2:4]), int(s[0:2]))
        except ValueError:
            return None
    return None


# ============================================================================
# SAFE ARITHMETIC HELPERS (SAS missing-value propagation: any missing
# operand -> missing result, rather than raising)
# ============================================================================
def _sadd(a, b): return None if a is None or b is None else a + b
def _ssub(a, b): return None if a is None or b is None else a - b
def _smul(a, b): return None if a is None or b is None else a * b
def _sdiv(a, b): return None if a is None or b is None or b == 0 else a / b
def _spow(a, b): return None if a is None or b is None else a ** b


def _sas_round(x, unit=1.0):
    """SAS ROUND(x, unit): rounds to nearest multiple of unit, halves away
    from zero (not Python's default banker's rounding)."""
    if x is None:
        return None
    scaled = x / unit
    r = int(scaled + 0.5) if scaled >= 0 else -int(-scaled + 0.5)
    return r * unit


def _days_between(a, b):
    """(a - b).days for two dates, propagating missing as None."""
    return None if a is None or b is None else (a - b).days


# ============================================================================
# %CALCIPD / %MTHENDDT (SAS program-local macros -- only ever called for
# this program's own DATA step; not a shared dependency, kept inline)
# ============================================================================
def _mthend_adjust(mthend, mthipd, yrtran, daytran):
    """%MTHENDDT macro."""
    if mthend != "Y" or mthipd is None or yrtran is None:
        return daytran
    if mthipd in (1, 3, 5, 7, 8, 10, 12):
        mthday = 31
    elif mthipd in (4, 6, 9, 11):
        mthday = 30
    else:
        mthday = 29 if (yrtran % 4 == 0) else 28
    if daytran is not None and daytran > mthday:
        daytran = mthday
    return daytran


def _mdy_safe(month, day, year):
    """SAS MDY() -- None (SAS missing) for an out-of-range month/day."""
    if month is None or day is None or year is None:
        return None
    month = int(month)
    if not (1 <= month <= 12):
        return None
    try:
        return date(int(year), month, int(day))
    except ValueError:
        return None


def _calc_ipd(matdt, reptdt):
    """%CALCIPD macro -- returns (preintdt, curintdt)."""
    if matdt is None or reptdt is None:
        return None, None

    mthend = "Y" if (matdt + timedelta(days=1)).day == 1 else "N"
    npay = 6

    daytran = matdt.day
    yrtran = matdt.year
    mthtran = matdt.month

    trandays = (matdt - reptdt).days
    numofn = int(trandays / (npay * 30))
    nummth = numofn * npay
    nyear = int(nummth / 12)
    nmth = nummth % 12
    mthipd = (mthtran - nmth) - npay
    if mthipd <= 0:
        mthipd += 12
        nyear += 1
    yrtran = yrtran - nyear

    daytran = _mthend_adjust(mthend, mthipd, yrtran, daytran)
    preintdt = _mdy_safe(mthipd, daytran, yrtran)

    curintdt = None
    if preintdt is not None:
        daytran = preintdt.day
        yrtran = preintdt.year
        mthipd = preintdt.month

        mthipd += npay
        if mthipd > 12:
            yrtran += 1
            mthipd = mthipd % 12

        daytran = _mthend_adjust(mthend, mthipd, yrtran, daytran)
        curintdt = _mdy_safe(mthipd, daytran, yrtran)

    return preintdt, curintdt


# ============================================================================
# PROC FORMAT EQUIVALENTS
# ============================================================================
def remfmt_label(value):
    """VALUE REMFMT. Ranges are inclusive; overlapping boundaries (e.g. 3
    appears in both '1-3' and '3-6') resolve to the FIRST listed range, as
    in SAS. SAS missing sorts as LOW, so None maps to the lowest bucket."""
    if value is None:
        value = float("-inf")
    if value <= 0.255:
        return "UP TO 1 WK     "
    if value <= 1:
        return ">1 WK - 1 MTH  "
    if value <= 3:
        return ">1 MTH - 3 MTHS"
    if value <= 6:
        return ">3 - 6 MTHS    "
    if value <= 12:
        return ">6 MTHS - 1 YR "
    return ">1 YEAR        "


def _remfmt_sort_key(value):
    """Ordering for REMMTH buckets in the by-REMMTH TABULATE reports,
    following the VALUE REMFMT declaration order."""
    order = ["UP TO 1 WK     ", ">1 WK - 1 MTH  ", ">1 MTH - 3 MTHS",
             ">3 - 6 MTHS    ", ">6 MTHS - 1 YR ", ">1 YEAR        "]
    label = remfmt_label(value)
    return order.index(label) if label in order else len(order)


# VALUE REMFMTA. is declared in the SAS source but never referenced by any
# PUT(...,REMFMTA.) call anywhere in this program -- intentionally not
# implemented, per project convention (dead PROC FORMAT declaration).

_CLASSF = {
    "A": "RM MKTBL SECUR/PAPERS ISSUED BY FED GOVT/BNM",
    "B": "CAGAMAS BONDS & NOTES",
    "C": "BAS ISSUED BY TIER1/AAA-RATED INST.",
    "D": "BAS ISSUED BY TIER2 & NON-AAA",
    "E": "NIDS ISSUED BY RATING",
    "F": "STOCK",
    "R": "REVERSE REPO",
    "X": "NIDS UDR REPO (LIABILITIES)",
    "Y": "NIDS UDR REPO (ASSETS)",
}

CHKDT = date(2004, 9, 4)          # CHKDT='04SEP04'D


# ============================================================================
# STEP 1: CACHE STAMP + STREAM RAW FLAT FILE -> PARQUET
# ============================================================================
def _cache_is_fresh(src_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= src_path.stat().st_mtime
    )


def _flat_file_to_parquet(flat_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Parsing {flat_path.name} -> {cache_path.name} ...")
    schema = _build_arrow_schema()
    writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
    total = 0
    kept = 0
    batch = []

    with open(flat_path, "rb") as fh:
        while True:
            block = fh.read(RECORD_LENGTH * CHUNK_ROWS)
            if not block:
                break
            n_full = len(block) // RECORD_LENGTH
            for i in range(n_full):
                raw = block[i * RECORD_LENGTH:(i + 1) * RECORD_LENGTH]
                total += 1
                rec = _parse_record(raw)
                if rec is not None:
                    batch.append(rec)
                    kept += 1
            if len(batch) >= CHUNK_ROWS:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                batch = []
                gc.collect()

    if batch:
        writer.write_table(pa.Table.from_pylist(batch, schema=schema))
    writer.close()
    print(f"  [{tag}] Done - {total:,} records read, {kept:,} rows kept "
          f"(UTREF1='I' AND UTREF4 not blank).")


def _load_cached_flatfile(flat_path: Path, tag: str) -> Path:
    cache_path = CACHE_DIR / f"{flat_path.stem}.parquet"
    if _cache_is_fresh(flat_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _flat_file_to_parquet(flat_path, cache_path, tag)
    return cache_path


def _read_reptdate(flat_path: Path) -> date:
    """DATA REPTDATE; INFILE BNMTBL4 OBS=1; INPUT @113 UTRPT $10.;
    REPTDATE=INPUT(UTRPT,DDMMYY10.); -- read only the FIRST physical
    record of the flat file."""
    with open(flat_path, "rb") as fh:
        raw = fh.read(RECORD_LENGTH)
    utrpt = raw[112:122].decode("latin1")   # @113, length 10 (0-indexed 112)
    reptdate = _parse_ddmmyy10(utrpt)
    if reptdate is None:
        raise ValueError(f"Could not derive REPTDATE from UTRPT={utrpt!r}")
    return reptdate


# ============================================================================
# LIQCLAS1 / LIQCLAS2 / LIQCLAS3 CLASSIFICATION
# ============================================================================
_CLASS_A2 = {"MGS", "MTB", "BNB", "BNN", "BMN", "BMC", "KHA", "MGI", "ITB"}
_CLASS_A3 = {"IDS", "DHB"}
_CLASS_A4 = {"DMB"}
_CLASS_B4A = {"CB2", "CF1", "CF2", "CMB", "PNB"}
_CLASS_B4B = {"CB1", "CNT", "SMC", "SAC"}


def _augment_base(rec, reptdate):
    """DATA LIQCLASS; SET LIQCLASS; AMOUNT=...; REPTDATE=...; STATUS=...;
    ISSDT/C2=... (see module docstring re. RPYR/RPMTH/RPDAY/RD2 dead code)."""
    rec = dict(rec)
    rec["AMOUNT"] = rec["UTMKV"] if rec["UTMKV"] is not None else 0.0
    rec["REPTDATE"] = reptdate

    rec["STATUS"] = rec["UTSTS"][:2]

    issdt = date(1959, 12, 31)   # ISSDT=0 (SAS day-0 default) when UTIDT blank
    utidt = rec["UTIDT"]
    if utidt.strip():
        try:
            issmm = int(utidt[0:2])
            issdd = int(utidt[3:5])
            issyy = int(utidt[6:10])
            issdt = date(issyy, issmm, issdd)
        except (ValueError, IndexError):
            issdt = date(1959, 12, 31)
    rec["C2"] = "R" if issdt <= CHKDT else "Y"
    return rec


def _build_liqclas1(rec):
    """DATA LIQCLAS1; SET LIQCLASS; SELECT(UTSTY) ... IF CLASS NE '   '
    THEN OUTPUT LIQCLAS1;"""
    utsty, utty = rec["UTSTY"], rec["UTTTY"]
    cls = slippage = None

    if utsty in _CLASS_A2:
        cls, slippage = "A", 2
        if utty == "X":
            cls = "R"
    elif utsty in _CLASS_A3:
        cls, slippage = "A", 3
        if utty == "X":
            cls = "R"
    elif utsty in _CLASS_A4:
        cls, slippage = "A", 4
        if utty == "X":
            cls = "R"
    elif utsty in _CLASS_B4A:
        cls, slippage = "B", 4
        if utsty == "CB2" and utty == "X" and rec["C2"] == "R":
            cls = "R"
    elif utsty in _CLASS_B4B:
        cls, slippage = "B", 4
        if utty == "X" and rec["C2"] == "R":
            cls = "R"
        if rec["C2"] == "Y":
            cls, slippage = "F", 6
    elif utsty == "SBA":
        if rec["STATUS"] in ("P1", "P2", "AA"):
            cls, slippage = "C", 4
        else:
            cls, slippage = "D", 6

    if cls is None:
        return None
    out = dict(rec)
    out["CLASS"] = cls
    out["SLIPPAGE"] = slippage
    return out


def _build_liqclas2(rec):
    """DATA LIQCLAS2; SET LIQCLASS; two independent OUTPUT conditions
    (UTSTY groups are disjoint, so at most one fires per row)."""
    out = []
    utsty, utsts = rec["UTSTY"], rec["UTSTS"]
    if utsty in ("SSD", "SDC") and utsts == "P1":
        r = dict(rec); r["CLASS"] = "E"; r["SLIPPAGE"] = 6
        out.append(r)
    if utsty in ("SLD", "SFD", "SZD") and utsts in ("AA", "AAA", "MARC1", "MARC2"):
        r = dict(rec); r["CLASS"] = "E"; r["SLIPPAGE"] = 6
        out.append(r)
    return out


def _build_liqclas3(liqclas2_rows):
    """DATA LIQCLAS3; SET LIQCLAS2; IF UTTTY='R' THEN DO; CLASS='X';
    OUTPUT; CLASS='Y'; OUTPUT; END; -- rows not matching UTTTY='R' produce
    no output (OUTPUT present in the DATA step suppresses auto-output)."""
    out = []
    for rec in liqclas2_rows:
        if rec["UTTTY"] == "R":
            r1 = dict(rec); r1["CLASS"] = "X"
            r2 = dict(rec); r2["CLASS"] = "Y"
            out.append(r1)
            out.append(r2)
    return out


# ============================================================================
# MAIN PRICING (Appendix 1 bond-discount formulae)
# ============================================================================
def _compute_pricing(rec):
    utsty, utty = rec["UTSTY"], rec["UTTTY"]
    utity, utipi = rec["UTITY"], rec["UTIPI"]
    cls, slippage = rec["CLASS"], rec["SLIPPAGE"]

    distyld = _sadd(rec["UTYLD"], slippage)
    p = _smul(_sdiv(distyld, 100), rec["UTMKV"])
    discount = p
    cpn = rec["UTCPR"]
    yld = distyld
    rv = 100.0

    reptdt = _parse_ddmmyy10(rec["UTRPT"])
    settledt = _parse_ddmmyy10(rec["UTOSD"])
    preintdt = _parse_ddmmyy10(rec["UTLCD"])
    curintdt = _parse_ddmmyy10(rec["UTNCD"])
    matdt = _parse_ddmmyy10(rec["UTMDT"])

    if utty == "X":
        matdt = _parse_ddmmyy10(rec["UTRMD"])
    if utty == "R":
        if cls == "X":
            matdt = _parse_ddmmyy10(rec["UTMDT"])
        if cls == "Y":
            matdt = _parse_ddmmyy10(rec["UTRMD"])

    tsm = _days_between(matdt, reptdt)

    if curintdt is None or curintdt == 0:
        tm = tsm
    else:
        tm = _days_between(curintdt, reptdt)

    remmth = _sas_round(_smul(_sdiv(tsm, 365), 12), 0.01) if tsm is not None else None
    if tsm is not None and tsm < 8:
        remmth = 0.1

    dsc = dcs = dcc = ndays = None
    npay = None   # per-row local -- SAS variable is not RETAINed

    qualifies = (
        (utity in ("I", "F") and utipi not in (None, " ", ""))
        or (utsty in ("SZD", "DHB", "DMB", "IDS", "KHA", "MGI"))
    )

    if qualifies:
        if utsty in ("DHB", "IDS", "KHA", "DMB", "MGI"):
            preintdt, curintdt = _calc_ipd(matdt, reptdt)

        dsc = _days_between(curintdt, reptdt)
        dcs = _days_between(reptdt, preintdt)
        dcc = _days_between(curintdt, preintdt)
        ndays = _days_between(matdt, curintdt)

        if utsty in ("SZD", "DHB", "DMB", "IDS", "KHA"):
            cpn = 0.0

        if remmth is not None and remmth < 6 and utity == "I":
            cpn2 = _sdiv(cpn, 100)
            yld = _sdiv(yld, 100)
            accint = _smul(cpn2, _sdiv(dcs, _smul(2, dcc)))
            denom = _sadd(1.0, _smul(yld, _sdiv(tsm, _smul(2, dcc))))
            numer = _sadd(1.0, _sdiv(cpn2, 2))
            discount = _ssub(_sdiv(numer, denom), accint)
            p = _smul(discount, 100)
        else:
            if utsty not in ("SFD", "CF1", "CF2", "CFB", "SSD"):
                if utipi == "Q":
                    npay = 3
                elif utipi == "H":
                    npay = 6
                elif utipi == "Y":
                    npay = 12
                # OTHERWISE: npay stays None (not retained across rows)

                nmth = _sas_round(_smul(_sdiv(ndays, 365), 12), 0.1) if ndays is not None else None
                n = int(_sdiv(nmth, npay)) + 1 if (nmth is not None and npay) else None

                if n is not None and dcc is not None and dsc is not None:
                    poweri = _sadd(n - 1, _sdiv(dsc, dcc))
                    i_val = _sdiv(rv, _spow(_sadd(1.0, _sdiv(yld, 200)), poweri))
                    ii_val = 0.0
                    for k in range(1, n + 1):
                        powerii = _sadd(k - 1, _sdiv(dsc, dcc))
                        term = _sdiv(_sdiv(cpn, 2), _spow(_sadd(1.0, _sdiv(yld, 200)), powerii))
                        if term is not None:
                            ii_val += term
                    iii_val = _smul(100, _smul(_sdiv(cpn, 200), _sdiv(dcs, dcc)))
                    if i_val is not None and iii_val is not None:
                        discount = _ssub(_sadd(i_val, ii_val), iii_val)
                        p = discount

    reptdays = _days_between(reptdt, preintdt)
    orgtenor = _days_between(matdt, preintdt)
    cpn2 = _sdiv(cpn, 100)

    distamt = _compute_distamt(utsty, rec["UTFCV"], p, cpn, cpn2, reptdays,
                                dcc, tm, orgtenor, tsm, yld)

    out = dict(rec)
    out.update({
        "DISTYLD": distyld, "DISCOUNT": discount, "CPN": cpn, "YLD": yld,
        "RV": rv, "DISTAMT": distamt, "REPTDT": reptdt, "SETTLEDT": settledt,
        "PREINTDT": preintdt, "CURINTDT": curintdt, "MATDT": matdt,
        "TSM": tsm, "TM": tm, "REMMTH": remmth,
    })
    return out


def _compute_distamt(utsty, utfcv, p, cpn, cpn2, reptdays, dcc, tm,
                      orgtenor, tsm, yld):
    """SELECT(UTSTY) DISTAMT formulae. Runs unconditionally for every row
    regardless of whether the pricing block above executed."""
    if utsty in ("MGS", "CBB", "SLD", "CB1", "CB2", "CMB", "PNB"):
        term = _sadd(_sdiv(p, 100), _sdiv(_smul(cpn2, reptdays), _smul(dcc, 2)))
        return _smul(utfcv, term)
    if utsty in ("CFB", "SFD", "CF1", "CF2"):
        numer = _sadd(36500, _smul(cpn, dcc))
        denom = _sadd(36500, _smul(yld, tm))
        return _smul(utfcv, _sdiv(numer, denom))
    if utsty in ("SZD", "KHA", "IDS", "DHB", "DMB"):
        return _smul(utfcv, _sdiv(p, 100))
    if utsty in ("SSD", "SLD"):
        # NOTE: 'SLD' here is unreachable (see module docstring) -- the
        # earlier WHEN('MGS','CBB','SLD',...) branch always matches first.
        numer = _sadd(36500, _smul(cpn, orgtenor))
        denom = _sadd(36500, _smul(yld, tsm))
        return _smul(utfcv, _sdiv(numer, denom))
    return _smul(utfcv, _ssub(1.0, _sdiv(_smul(yld, tsm), 36500)))


# ============================================================================
# PROC SUMMARY (NWAY, no MISSING option -> rows with any missing CLASS
# variable are dropped) + LIQASSET BY-group running accumulation
# ============================================================================
def _build_liqasset_summary(rows):
    groups = {}
    for r in rows:
        key = (r["UTSTY"], r["CLASS"], r["REMMTH"], r["UTTTY"])
        if any(k is None for k in key):
            continue   # PROC SUMMARY NWAY without MISSING drops these
        g = groups.setdefault(key, {"DISTAMT": None, "UTMKV": None, "UTAMP": None})
        for f in ("DISTAMT", "UTMKV", "UTAMP"):
            v = r.get(f)
            if v is not None:
                g[f] = (g[f] or 0.0) + v
    out = []
    for key, sums in groups.items():
        utsty, cls, remmth, utty = key
        out.append({"UTSTY": utsty, "CLASS": cls, "REMMTH": remmth,
                     "UTTTY": utty, **sums})
    out.sort(key=lambda r: (r["UTSTY"], r["CLASS"], r["REMMTH"], r["UTTTY"]))
    return out


def _build_liqasset(summary_rows):
    """DATA LIQASSET; SET LIQASSET; BY UTSTY CLASS REMMTH; ... (see SAS)."""
    out = []
    n = len(summary_rows)
    amtstk = amtrepo = amtrev = totdsct = stknid = repnid = 0.0

    for i, r in enumerate(summary_rows):
        utsty, cls, remmth, utty = r["UTSTY"], r["CLASS"], r["REMMTH"], r["UTTTY"]
        prev = summary_rows[i - 1] if i > 0 else None
        nxt = summary_rows[i + 1] if i < n - 1 else None

        first_class = prev is None or (prev["UTSTY"], prev["CLASS"]) != (utsty, cls)
        first_remmth = (prev is None
                         or (prev["UTSTY"], prev["CLASS"], prev["REMMTH"]) != (utsty, cls, remmth))
        last_utsty = nxt is None or nxt["UTSTY"] != utsty
        last_class = nxt is None or (nxt["UTSTY"], nxt["CLASS"]) != (utsty, cls)
        last_remmth = (nxt is None
                        or (nxt["UTSTY"], nxt["CLASS"], nxt["REMMTH"]) != (utsty, cls, remmth))

        if first_class or first_remmth:
            stknid = repnid = 0.0
            amtstk = amtrepo = amtrev = totdsct = 0.0

        utmkv = r["UTMKV"] or 0.0
        utamp = r["UTAMP"] or 0.0
        distamt = r["DISTAMT"] or 0.0

        if utty == "S":
            amtstk += utmkv
        elif utty == "R":
            amtrepo += utmkv
            if cls == "X":
                stknid += utamp
            if cls == "Y":
                repnid += utamp
        elif utty == "X":
            amtrev += utmkv
        # OTHERWISE: no-op

        if utsty in ("SSD", "SLD"):
            if utty == "R":
                totdsct -= distamt
            else:
                totdsct += distamt
        else:
            totdsct += distamt

        if last_utsty or last_class or last_remmth:
            out.append({
                "UTSTY": utsty, "CLASS": cls, "REMMTH": remmth,
                "AMTSTK": amtstk, "AMTREPO": amtrepo,
                "MKVBOOK": amtstk - amtrepo, "MKVREV": amtrev,
                "MKVNIDX": stknid, "MKVNIDY": repnid,
                "TOTDSCT": totdsct, "UTAMP": utamp,
            })
    return out


# ============================================================================
# REPORT RENDERING (RECFM=FBA -- leading ASA control byte per line)
# ============================================================================
def _fmt_comma(value, width=18, decimals=2):
    if value is None:
        value = 0.0
    return f"{value:,.{decimals}f}".rjust(width)


def _title_block(rdate_str: str):
    return [
        "REPORT ID: EIBMLIQ4",
        "PUBLIC BANK BERHAD - STATISTICS DEPARTMENT",
        "STOCK OF LIQUEFIABLE ASSETS (PART 4)",
        f"AS AT {rdate_str}",
        "",
    ]


def _render_print(detail_rows, rdate_str):
    """PROC PRINT DATA=LIQCLASS SPLIT='*'; VAR ...; SUM DISTAMT;
    FORMAT PREINTDT CURINTDT DDMMYY8.;
    (column headers simplified to plain variable names -- the SAS LABEL
    text is documented in the field-layout table above but is not rendered
    label-for-label here, a formatting simplification.)"""
    cols = ["UTOSD", "CPN", "YLD", "UTMKV", "UTMDT", "UTSTY", "UTFCV",
            "PREINTDT", "CURINTDT", "DISCOUNT", "DISTAMT", "UTTTY",
            "STATUS", "UTRMD", "MRNGE"]
    widths = {"UTOSD": 10, "CPN": 10, "YLD": 10, "UTMKV": 14, "UTMDT": 10,
              "UTSTY": 6, "UTFCV": 14, "PREINTDT": 10, "CURINTDT": 10,
              "DISCOUNT": 14, "DISTAMT": 14, "UTTTY": 6, "STATUS": 6,
              "UTRMD": 10, "MRNGE": 16}

    def _fmt_cell(col, val):
        if col in ("PREINTDT", "CURINTDT"):
            return (val.strftime("%d/%m/%y") if val else "").rjust(widths[col])
        if col in ("CPN", "YLD", "UTMKV", "UTFCV", "DISCOUNT", "DISTAMT"):
            return _fmt_comma(val, widths[col])
        return str(val if val is not None else "").ljust(widths[col])

    lines = [("1", t) for t in _title_block(rdate_str)]
    lines.append((" ", " ".join(c.ljust(widths[c]) for c in cols)))
    total_distamt = 0.0
    n_on_page = len(lines)
    for r in detail_rows:
        if n_on_page >= PAGE_SIZE:
            lines += [("1", t) for t in _title_block(rdate_str)]
            lines.append((" ", " ".join(c.ljust(widths[c]) for c in cols)))
            n_on_page = len(_title_block(rdate_str)) + 1
        row_text = " ".join(_fmt_cell(c, r.get(c)) for c in cols)
        lines.append((" ", row_text))
        total_distamt += r.get("DISTAMT") or 0.0
        n_on_page += 1
    lines.append((" ", "SUM".ljust(sum(widths[c] + 1 for c in cols) - widths["DISTAMT"])
                  + _fmt_comma(total_distamt, widths["DISTAMT"])))
    return lines


def _render_tabulate(liqasset_rows, class_filter, dim, box_title, measures,
                      rts, measure_width=18):
    """Generic emulation of:
        TABLE CLASS=' ',<dim> ALL, measure*F=COMMAw.d ... /RTS=rts BOX=...;
    FORMCHAR='           ' (all blank) means SAS itself draws no
    box/border characters here -- this renderer likewise uses plain
    whitespace-separated columns (no '+'/'-'/'|'), matching that FORMCHAR
    setting; exact SAS PROC TABULATE column-width arithmetic is
    approximated, as already accepted practice for TABULATE-style output
    in this project (see EIIMRM01.py)."""
    if isinstance(class_filter, str):
        class_filter = {class_filter}
    else:
        class_filter = set(class_filter)

    rows = [r for r in liqasset_rows if r["CLASS"] in class_filter]
    lines = [("1", box_title), (" ", "")]
    if not rows:
        lines.append((" ", "(no data)"))
        return lines

    dim_is_remmth = dim == "REMMTH"

    def _dim_label(v):
        return remfmt_label(v) if dim_is_remmth else (v or "")

    def _dim_sort(v):
        return _remfmt_sort_key(v) if dim_is_remmth else str(v)

    groups = {}
    for r in rows:
        key = (r["CLASS"], r[dim])
        g = groups.setdefault(key, {m: 0.0 for m in measures})
        for m in measures:
            g[m] += r.get(m) or 0.0

    header = " ".ljust(rts) + "".join(m.rjust(measure_width) for m in measures)
    lines.append((" ", header))

    keys_sorted = sorted(groups.keys(), key=lambda k: (k[0], _dim_sort(k[1])))
    prev_class = None
    class_total = {m: 0.0 for m in measures}
    grand_total = {m: 0.0 for m in measures}

    for key in keys_sorted:
        cls, dv = key
        if cls != prev_class:
            if prev_class is not None:
                lines.append((" ", "  ALL".ljust(rts)
                              + "".join(_fmt_comma(class_total[m], measure_width) for m in measures)))
            class_total = {m: 0.0 for m in measures}
            prev_class = cls
            lines.append((" ", _CLASSF.get(cls, cls).ljust(rts)))
        g = groups[key]
        for m in measures:
            class_total[m] += g[m]
            grand_total[m] += g[m]
        label = f"  {_dim_label(dv)}".ljust(rts)
        lines.append((" ", label + "".join(_fmt_comma(g[m], measure_width) for m in measures)))

    lines.append((" ", "  ALL".ljust(rts)
                  + "".join(_fmt_comma(class_total[m], measure_width) for m in measures)))
    lines.append((" ", "TOTAL".ljust(rts)
                  + "".join(_fmt_comma(grand_total[m], measure_width) for m in measures)))
    return lines


# ============================================================================
# MAIN
# ============================================================================
def main():
    print("Step 1: Resolving latest BNMTBL4 (KAPITI4) flat file...")
    kapiti4_path = get_latest_file(INPUT_KAPITI4_DIR, prefix=INPUT_KAPITI4_PREFIX)
    print(f"  Using: {kapiti4_path}")

    print("\nStep 2: Deriving REPTDATE from first record of BNMTBL4...")
    reptdate = _read_reptdate(kapiti4_path)
    rdate_str = reptdate.strftime("%d/%m/%y")   # PUT(REPTDATE,DDMMYY8.)
    print(f"  REPTDATE : {reptdate.isoformat()}   RDATE: {rdate_str}")

    print("\nStep 3: Caching BNMTBL4 to Parquet (byte-offset + packed-decimal parse)...")
    cache_path = _load_cached_flatfile(kapiti4_path, "BNMTBL4")

    print("\nStep 4: Loading cached rows via DuckDB...")
    con = duckdb.connect(database=":memory:")
    base_df = con.execute(f"SELECT * FROM read_parquet('{cache_path.as_posix()}')").pl()
    con.close()
    print(f"  Rows loaded: {len(base_df):,}")

    print("\nStep 5: Building LIQCLASS base rows (AMOUNT/STATUS/ISSDT/C2)...")
    base_rows = [_augment_base(r, reptdate) for r in base_df.iter_rows(named=True)]
    del base_df
    gc.collect()

    print("\nStep 6: Building LIQCLAS1 / LIQCLAS2 / LIQCLAS3...")
    liqclas1_rows = [r for r in (_build_liqclas1(rec) for rec in base_rows) if r is not None]
    liqclas2_rows = [r for rec in base_rows for r in _build_liqclas2(rec)]
    liqclas3_rows = _build_liqclas3(liqclas2_rows)
    liqclass_combined = liqclas1_rows + liqclas2_rows + liqclas3_rows
    print(f"  LIQCLAS1: {len(liqclas1_rows):,}  LIQCLAS2: {len(liqclas2_rows):,}  "
          f"LIQCLAS3: {len(liqclas3_rows):,}  Combined: {len(liqclass_combined):,}")

    print("\nStep 7: Computing pricing (Appendix 1 formulae) per row...")
    priced_rows = [_compute_pricing(r) for r in liqclass_combined]

    print("\nStep 8: PROC SORT BY UTSTY CLASS (stable)...")
    priced_rows.sort(key=lambda r: (r["UTSTY"], r["CLASS"]))

    print("\nStep 9: Applying MATDTEX (%INC PGM(MATDTEX)) REMMTH reclassification...")
    for r in priced_rows:
        r["REMMTH"] = calc_remmth(r["REPTDATE"], r["MATDT"])
        r["MRNGE"] = remfmt_label(r["REMMTH"])

    print("\nStep 10: Building LIQASSET (PROC SUMMARY + BY-group accumulation)...")
    liqasset_summary = _build_liqasset_summary(priced_rows)
    liqasset_rows = _build_liqasset(liqasset_summary)
    print(f"  LIQASSET rows: {len(liqasset_rows):,}")

    print("\nStep 11: Rendering report...")
    report_lines = []
    report_lines += _render_print(priced_rows, rdate_str)

    report_lines += _render_tabulate(liqasset_rows, ("A", "B"), "UTSTY",
                                      "CLASS-1 LIQUIFIABLE ASSETS",
                                      ["MKVBOOK", "MKVREV", "TOTDSCT"], 50)
    report_lines += _render_tabulate(liqasset_rows, "R", "UTSTY",
                                      "CLASS-1 LIQUIFIABLE ASSETS",
                                      ["MKVBOOK", "MKVREV", "TOTDSCT"], 50)
    report_lines += _render_tabulate(liqasset_rows, "R", "REMMTH",
                                      "CLASS-1 LIQUID ASSETS BY",
                                      ["MKVREV", "TOTDSCT"], 30)
    report_lines += _render_tabulate(liqasset_rows, "F", "UTSTY",
                                      "CLASS-2 LIQUIFIABLE ASSETS FOR CLASS F",
                                      ["MKVBOOK", "MKVREV", "TOTDSCT"], 50)
    report_lines += _render_tabulate(liqasset_rows, "F", "REMMTH",
                                      "CLASS-2 LIQUIFIABLE ASSETS FOR CLASS F",
                                      ["MKVBOOK", "MKVREV", "TOTDSCT"], 50)
    report_lines += _render_tabulate(liqasset_rows, ("C", "D", "E"), "UTSTY",
                                      "CLASS-2 LIQUID ASSETS & CREDIT LINES",
                                      ["MKVBOOK", "MKVREV", "TOTDSCT"], 50)
    report_lines += _render_tabulate(liqasset_rows, ("C", "D", "E"), "REMMTH",
                                      "CLASS-2 LIQUID ASSETS BY",
                                      ["MKVBOOK", "TOTDSCT"], 30)
    report_lines += _render_tabulate(liqasset_rows, "Y", "UTSTY",
                                      "CLASS-2 LIQUID ASSETS",
                                      ["MKVNIDY"], 50, measure_width=20)
    report_lines += _render_tabulate(liqasset_rows, "X", "REMMTH",
                                      "CLASS-2 LIQUID ASSETS BY",
                                      ["MKVNIDX"], 30, measure_width=20)
    report_lines += _render_tabulate(liqasset_rows, "Y", "REMMTH",
                                      "CLASS-2 LIQUID ASSETS BY",
                                      ["MKVNIDY"], 30, measure_width=20)

    print(f"\nStep 12: Writing output to {OUTPUT_FILE} ...")
    with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
        for asa, text in report_lines:
            fh.write(asa + text + "\n")

    print(f"\n  Output written : {OUTPUT_FILE}")
    print(f"  Total lines    : {len(report_lines):,}")
    print("\n--- Report preview (first 30 lines) ---")
    for asa, text in report_lines[:30]:
        print(f"[{asa}]{text}")

    print("\nEIBMLI4I complete.")


if __name__ == "__main__":
    main()
