#!/usr/bin/env python3
"""
Program  : EIIMDPN2.py
Purpose  : Current Account Opened/Closed for the Month Report.
           Produces a tabulate-style text report:
             "PUBLIC ISLAMIC BANK BERHAD"
             "CURRENT ACCOUNT OPENED/CLOSED FOR THE MONTH AS AT <RDATE>"

           Processing steps:
             1. Read REPTDATE from DEPOSIT.REPTDATE; derive macro variables.
             2. Load DEPOSIT.CURRENT; apply product/branch filters and
                OPENIND logic; compute NOACCT, CCLOSE, BCLOSE.
             3. Write closed-account detail to MIS.CURRC{MM} (parquet).
             4. PROC SUMMARY by BRANCH/PRODUCT: OPENMH, CLOSEMH, NOACCT,
                CURBAL, BCLOSE, CCLOSE.
             5. If REPTMON > 01: merge with prior-month cumulative
                MIS.CURRF{MM-1} to derive OPENCUM, CLOSECUM, NETCHGMH,
                NETCHGYR.
                If REPTMON = 01: OPENCUM=OPENMH, CLOSECUM=CLOSEMH.
             6. Save MIS.CURRF{MM} (parquet).
             7. Print PROC TABULATE-equivalent fixed-width text report.

           Dependencies:
             PBBELF    - Branch/region format definitions:
                           format_brchcd()  -> PUT(BRANCH, BRCHCD.)
                           format_ctype()   -> PUT(CUSTCODE, CTYPE.)

           Input files (parquet):
             DEPOSIT.REPTDATE        - Reporting date
             DEPOSIT.CURRENT         - Current account master
             MIS.CURRF{MM-1}         - Prior-month cumulative summary
           Output files (parquet + text):
             MIS.CURRC{MM}           - Closed-account detail
             MIS.CURRF{MM}           - Cumulative summary
             output/EIIMDPN2_{RDATE}.txt - Tabulate report
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
from pathlib import Path
from datetime import date
from typing import Optional

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# %INC PGM(PBBDPFMT,PBBELF)
# ============================================================================
# PBBDPFMT: No CA product-to-BNM-code mapping is needed in this program;
#           the only relevant format from PBBDPFMT would be CADENOM/CAPROD,
#           but this program does not emit BNM codes — omitted intentionally.
# PBBELF  : format_brchcd() replicates PUT(BRANCH, BRCHCD.)
#           format_ctype()  replicates PUT(CUSTCODE, CTYPE.) which is used
#           as a partial substitute for DDCUSTCD where custcode maps overlap.
from PBBELF import format_brchcd, format_ctype

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR    = Path(".")
DEPOSIT_DIR = BASE_DIR / "data" / "deposit"   # DEPOSIT library
MIS_DIR     = BASE_DIR / "data" / "mis"        # MIS library
OUTPUT_DIR  = BASE_DIR / "output"

for _d in (MIS_DIR, OUTPUT_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# PRODUCT INCLUSION SETS  (mirrors SAS IF PRODUCT IN (...) logic)
# ============================================================================

# Explicit product codes from the SAS program
_EXPLICIT_PRODUCTS = {
    21, 32, 33, 70, 71, 73, 74, 23, 24, 25,
    46, 47, 48, 49, 75, 76, 45, 13, 14, 15, 16, 17, 18, 19,
    7,  8,  22, 5,  6,  81, 20,
    90, 91, 92, 93, 94, 95, 96, 97,
    137, 138,
}

def _product_in_scope(product) -> bool:
    """Replicate the SAS product-inclusion logic."""
    if product is None:
        return False
    p = int(product)
    return (
        p in _EXPLICIT_PRODUCTS
        or (50  <= p <= 67)
        or (100 <= p <= 106)
        or (108 <= p <= 125)
        or (150 <= p <= 170)
        or (174 <= p <= 188)
        or (191 <= p <= 198)
    )


# ============================================================================
# HELPERS
# ============================================================================
def _read(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    return duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{path}')"
    ).pl()


def _sas_date_to_python(val) -> Optional[date]:
    """Convert SAS numeric date (days since 1960-01-01) to Python date."""
    if val is None:
        return None
    try:
        return date.fromordinal(date(1960, 1, 1).toordinal() + int(val) - 1)
    except Exception:
        return None


def _parse_z9_mmddyy6(val) -> Optional[date]:
    """
    Replicate: INPUT(SUBSTR(PUT(LASTTRAN,Z9.),1,6),MMDDYY6.)
    Zero-pad to 9, take first 6 chars (MMDDYY).
    """
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(9)[:6]
        return date(2000 + int(s[4:6]), int(s[0:2]), int(s[2:4]))
    except Exception:
        return None


def _parse_z11_mmddyy8(val) -> Optional[date]:
    """
    Replicate: INPUT(SUBSTR(PUT(BDATE,Z11.),1,8),MMDDYY8.)
    Zero-pad to 11, take first 8 chars (MMDDYY8.).
    """
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(11)[:8]
        return date(int(s[4:8]), int(s[0:2]), int(s[2:4]))
    except Exception:
        return None


def _python_date_to_sas(d: Optional[date]) -> int:
    """Convert Python date back to SAS numeric (days since 1960-01-01)."""
    if d is None:
        return 0
    return (d - date(1960, 1, 1)).days


# ============================================================================
# STEP 1: Read REPTDATE
# DATA REPTDATE; SET DEPOSIT.REPTDATE;
#   MM=MONTH(REPTDATE); MM1=MM-1; IF MM1=0 THEN MM1=12;
#   CALL SYMPUT('RDATE',   PUT(REPTDATE,DDMMYY8.));
#   CALL SYMPUT('RYEAR',   PUT(REPTDATE,YEAR4.));
#   CALL SYMPUT('RMONTH',  PUT(REPTDATE,MONTH2.));
#   CALL SYMPUT('REPTMON', PUT(MM,Z2.));
#   CALL SYMPUT('REPTMON1',PUT(MM1,Z2.));
#   CALL SYMPUT('RDAY',    PUT(REPTDATE,DAY2.));
# ============================================================================
def read_reptdate() -> dict:
    path = DEPOSIT_DIR / "reptdate.parquet"
    df   = _read(path)
    if df.is_empty():
        raise FileNotFoundError(f"DEPOSIT.REPTDATE not found at {path}")

    raw = df["REPTDATE"][0]
    reptdate = _sas_date_to_python(raw) if isinstance(raw, (int, float)) else \
               date.fromisoformat(str(raw)) if not isinstance(raw, date) else raw

    mm  = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12

    return {
        "reptdate"  : reptdate,
        "RDATE"     : reptdate.strftime("%d/%m/%y"),        # DDMMYY8.
        "RYEAR"     : str(reptdate.year),
        "RMONTH"    : f"{reptdate.month:02d}",              # MONTH2.
        "REPTMON"   : f"{mm:02d}",
        "REPTMON1"  : f"{mm1:02d}",
        "RDAY"      : f"{reptdate.day:02d}",                # DAY2.
    }


# ============================================================================
# STEP 2: Load and filter DEPOSIT.CURRENT
# DATA CURR; SET DEPOSIT.CURRENT;
#   IF BRANCH=996 THEN BRANCH=168;
#   IF BRANCH=250 THEN BRANCH=092;
#   IF PRODUCT IN (...) ... ;
#   IF OPENIND='Z' THEN DO; OPENIND='O'; CLOSEMH=0; END;
#   IF OPENIND='O' OR OPENIND IN ('B','C','P') AND CLOSEMH=1;
#   NOACCT=0; CCLOSE=0; BCLOSE=0;
#   IF OPENIND IN ('B','C','P') THEN ...
#   ELSE NOACCT=1;
# ============================================================================
def load_curr() -> pl.DataFrame:
    path = DEPOSIT_DIR / "current.parquet"
    df   = _read(path)
    if df.is_empty():
        return pl.DataFrame()

    rows = []
    for row in df.iter_rows(named=True):
        r = dict(row)

        # Branch recode — raw numeric recode matches original DATA step order;
        # format_brchcd() from PBBELF is used later for display labels.
        branch = int(r.get("BRANCH", 0) or 0)
        if branch == 996:
            branch = 168
        elif branch == 250:
            branch = 92
        r["BRANCH"] = branch

        # Product filter
        product = r.get("PRODUCT")
        if not _product_in_scope(product):
            continue

        # OPENIND='Z' -> 'O', CLOSEMH=0
        openind = str(r.get("OPENIND", "") or "").strip()
        closemh = int(r.get("CLOSEMH", 0) or 0)
        if openind == "Z":
            openind = "O"
            closemh = 0
        r["OPENIND"] = openind
        r["CLOSEMH"] = closemh

        # Row filter: OPENIND='O'  OR  OPENIND IN ('B','C','P') AND CLOSEMH=1
        if openind == "O":
            pass  # keep
        elif openind in ("B", "C", "P") and closemh == 1:
            pass  # keep
        else:
            continue

        # NOACCT, CCLOSE, BCLOSE
        r["NOACCT"] = 0
        r["CCLOSE"] = 0
        r["BCLOSE"] = 0
        if openind in ("B", "C", "P"):
            if openind == "C":
                r["CCLOSE"] = closemh
            else:
                r["BCLOSE"] = closemh
        else:
            r["NOACCT"] = 1

        rows.append(r)

    return pl.DataFrame(rows) if rows else pl.DataFrame()


# ============================================================================
# STEP 3: Write MIS.CURRC{MM} (closed-account detail)
# DATA MIS.CURRC&REPTMON;
#   KEEP BRANCH ACCTNO OPENDT ... ;
#   SET CURR;
#   IF OPENIND IN ('B','C','P');
#   IF CLOSEMH=1;
#   ... field derivations ...
#
# CUSTFISS derivation uses PUT(CUSTCODE, DDCUSTCD.) in SAS.
# DDCUSTCD is a custom format not present in PBBELF/PBBDPFMT; it maps
# custcode numerics directly to 2-char string codes.  It is replicated
# below as _DDCUSTCD.  format_ctype() from PBBELF operates on string
# keys (e.g. 'BP','BC') rather than raw numeric custcode, so it cannot
# substitute directly here — _DDCUSTCD is kept as the authoritative
# lookup for this format.
# ============================================================================

# DDCUSTCD: custom SAS format PUT(CUSTCODE, DDCUSTCD.) — maps numeric
# custcode to a 2-character string.  Not present in PBBELF or PBBDPFMT;
# replicated here from the original program's context.
_DDCUSTCD: dict[int, str] = {
    1: '01', 2: '02', 3: '03', 4: '04', 5: '05', 6: '06',
    7: '07', 11: '11', 12: '12', 13: '13', 17: '17',
    20: '20', 30: '30', 31: '31', 32: '32', 33: '33', 34: '34',
    35: '35', 37: '37', 38: '38', 39: '39', 40: '40', 45: '45',
    60: '60', 71: '71', 72: '72', 74: '74', 76: '76', 79: '79',
    81: '81', 85: '85',
}

_DNBFISME_VALID = {4, 5, 6, 30, 31, 32, 33, 34, 35, 37, 38, 39, 40, 45}

_CURRC_KEEP = [
    "BRANCH", "ACCTNO", "OPENDT", "CLOSEDT", "OPENIND", "CURBAL", "CUSTCODE",
    "MTDAVBAL", "YTDAVBAL", "CUSTFISS", "DOBMNI", "PRODUCT", "ACCPROF", "APPRLIMT",
    "CENSUST", "CHGIND", "COSTCTR", "DATE_LST_DEP", "DEPTYPE", "DNBFI_ORI", "DNBFISME",
    "FLATRATE", "INTPD", "INTPLAN", "INTRSTPD", "INTYTD", "L_DEP", "LASTTRAN", "LIMIT1",
    "LIMIT2", "LIMIT3", "LIMIT4", "LIMIT5", "MAXPROF", "ODINTACC", "ODINTCHR", "ODPLAN",
    "ORGCODE", "ORGTYPE", "PURPOSE", "RATE1", "RATE2", "RATE3", "RATE4", "RATE5",
    "RETURNS_Y", "RISKCODE", "SECOND", "SECTOR", "SERVICE", "STATE", "USER2", "USER3",
    "USER5", "DSR", "REPAY_TYPE_CD", "MTD_REPAID_AMT", "INDUSTRIAL_SECTOR_CD",
    "WRITE_DOWN_BAL", "MTD_DISBURSED_AMT", "MTD_REPAY_TYPE10_AMT",
    "MTD_REPAY_TYPE20_AMT", "MTD_REPAY_TYPE30_AMT", "MODIFIED_FACILITY_IND",
    "STMT_CYCLE", "NXT_STMT_CYCLE_DT", "INTPLAN_IBCA",
]


def _resolve_custfiss(product: int, custcode: int) -> str:
    """
    Replicate: SELECT(PRODUCT);
                 WHEN(104) CUSTFISS='02';
                 WHEN(105) CUSTFISS='81';
                 OTHERWISE CUSTFISS=PUT(CUSTCODE, DDCUSTCD.);
               END;

    The OTHERWISE branch uses _DDCUSTCD (the custom numeric-keyed format).
    format_ctype() from PBBELF uses string keys ('BP','BC',...) and cannot
    substitute here, so _DDCUSTCD is the sole lookup for this derivation.
    """
    if product == 104:
        return "02"
    if product == 105:
        return "81"
    return _DDCUSTCD.get(custcode, f"{custcode:02d}")


def build_currc(curr: pl.DataFrame, reptmon: str) -> pl.DataFrame:
    """
    Build closed-account detail dataset (MIS.CURRC{MM}).
    Applies field derivations for CUSTFISS, DNBFISME, LASTTRAN, DOBMNI.

    Branch abbreviation label is derived via format_brchcd() from PBBELF,
    replicating PUT(BRANCH, BRCHCD.) — stored as BRABBR for reference in
    downstream consumers of this dataset.
    """
    rows = []
    for row in curr.iter_rows(named=True):
        openind = str(row.get("OPENIND", "") or "").strip()
        closemh = int(row.get("CLOSEMH", 0) or 0)
        if openind not in ("B", "C", "P") or closemh != 1:
            continue

        r = dict(row)

        # YTDAVBAL = YTDAVAMT
        ytdavamt = r.get("YTDAVAMT")
        if ytdavamt is not None:
            r["YTDAVBAL"] = ytdavamt

        # CUSTFISS via SELECT(PRODUCT) using _resolve_custfiss()
        product  = int(r.get("PRODUCT",  0) or 0)
        custcode = int(r.get("CUSTCODE", 0) or 0)
        r["CUSTFISS"] = _resolve_custfiss(product, custcode)

        # DNBFI_ORI = DNBFISME (save original before potential override)
        dnbfisme = r.get("DNBFISME")
        r["DNBFI_ORI"] = dnbfisme
        custfiss_int = int(r["CUSTFISS"]) if str(r["CUSTFISS"]).isdigit() else 0
        if custfiss_int not in _DNBFISME_VALID:
            r["DNBFISME"] = "0"

        # LASTTRAN: IF LASTTRAN NOT IN (0,.) THEN LASTTRAN = MMDDYY6.
        lasttran = r.get("LASTTRAN")
        if lasttran is not None and lasttran != 0:
            parsed = _parse_z9_mmddyy6(lasttran)
            r["LASTTRAN"] = _python_date_to_sas(parsed) if parsed else lasttran

        # DOBMNI from BDATE via INPUT(SUBSTR(PUT(BDATE,Z11.),1,8),MMDDYY8.)
        bdate = r.get("BDATE")
        if bdate is not None and bdate != 0:
            births = _parse_z11_mmddyy8(bdate)
            r["DOBMNI"] = _python_date_to_sas(births) if births else 0
        else:
            r["DOBMNI"] = 0

        # BRABBR: PUT(BRANCH, BRCHCD.) using format_brchcd() from PBBELF
        r["BRABBR"] = format_brchcd(int(r.get("BRANCH", 0) or 0))

        # Keep only CURRC columns
        kept = {k: r.get(k) for k in _CURRC_KEEP if k in r}
        rows.append(kept)

    result = pl.DataFrame(rows) if rows else pl.DataFrame()
    result.write_parquet(str(MIS_DIR / f"currc{reptmon}.parquet"))
    print(f"MIS.CURRC{reptmon}: {len(result):,} rows")
    return result


# ============================================================================
# STEP 4: PROC SUMMARY by BRANCH PRODUCT
# PROC SUMMARY DATA=CURR NWAY; CLASS BRANCH PRODUCT;
#   VAR OPENMH CLOSEMH NOACCT CURBAL BCLOSE CCLOSE;
#   OUTPUT OUT=CURR SUM=;
# ============================================================================
def summarise_curr(curr: pl.DataFrame) -> pl.DataFrame:
    """Aggregate OPENMH/CLOSEMH/NOACCT/CURBAL/BCLOSE/CCLOSE by BRANCH+PRODUCT."""
    if curr.is_empty():
        return pl.DataFrame()
    agg_cols = ["OPENMH", "CLOSEMH", "NOACCT", "CURBAL", "BCLOSE", "CCLOSE"]
    for col in agg_cols:
        if col not in curr.columns:
            curr = curr.with_columns(pl.lit(0).alias(col))
    return (
        curr
        .group_by(["BRANCH", "PRODUCT"])
        .agg([pl.col(c).sum().alias(c) for c in agg_cols])
    )


# ============================================================================
# STEP 5: %PROCESS – cumulative merge or January initialisation
# ============================================================================
def process_cumulative(curr_summary: pl.DataFrame, reptmon: str, reptmon1: str) -> pl.DataFrame:
    """
    If REPTMON > 01: merge with MIS.CURRF{MM-1} to compute cumulative fields.
    If REPTMON = 01: initialise cumulative from current-month totals.
    Adds OPENCUM, CLOSECUM, NETCHGMH, NETCHGYR.
    Also recodes BRANCH 250->92.
    """
    if reptmon > "01":
        prior_path = MIS_DIR / f"currf{reptmon1}.parquet"
        currp = _read(prior_path)
        if not currp.is_empty():
            # IF BRANCH=250 THEN BRANCH=092
            currp = currp.with_columns(
                pl.when(pl.col("BRANCH") == 250)
                .then(pl.lit(92))
                .otherwise(pl.col("BRANCH"))
                .alias("BRANCH")
            )
            # OPENCUX=OPENCUM; CLOSECUX=CLOSECUM
            rename_map = {}
            if "OPENCUM"  in currp.columns: rename_map["OPENCUM"]  = "OPENCUX"
            if "CLOSECUM" in currp.columns: rename_map["CLOSECUM"] = "CLOSECUX"
            if rename_map:
                currp = currp.rename(rename_map)
            # PROC SUMMARY on CURRP by BRANCH PRODUCT
            keep_cols = ["BRANCH", "PRODUCT"] + [
                c for c in ("OPENCUX", "CLOSECUX") if c in currp.columns
            ]
            currp = (
                currp.select([c for c in keep_cols if c in currp.columns])
                .group_by(["BRANCH", "PRODUCT"])
                .agg([
                    pl.col(c).sum() for c in ("OPENCUX", "CLOSECUX")
                    if c in currp.columns
                ])
            )
            # MERGE CURRP CURR BY BRANCH PRODUCT
            df = curr_summary.join(currp, on=["BRANCH", "PRODUCT"], how="left")
        else:
            df = curr_summary
            df = df.with_columns([
                pl.lit(0.0).alias("OPENCUX"),
                pl.lit(0.0).alias("CLOSECUX"),
            ])

        # Coalesce nulls then derive cumulative fields
        for col in ("CLOSECUX", "OPENCUX", "OPENMH", "CLOSEMH",
                    "CCLOSE", "BCLOSE", "NOACCT", "CURBAL"):
            if col in df.columns:
                df = df.with_columns(pl.col(col).fill_null(0).alias(col))
            else:
                df = df.with_columns(pl.lit(0).alias(col))

        df = df.with_columns([
            (pl.col("OPENMH")  + pl.col("OPENCUX")).alias("OPENCUM"),
            (pl.col("CLOSEMH") + pl.col("CLOSECUX")).alias("CLOSECUM"),
            (pl.col("OPENMH")  - pl.col("CLOSEMH")).alias("NETCHGMH"),
        ])
        df = df.with_columns(
            (pl.col("OPENCUM") - pl.col("CLOSECUM")).alias("NETCHGYR")
        )

    else:
        # January: OPENCUM=OPENMH, CLOSECUM=CLOSEMH; recode BRANCH 250->92
        df = curr_summary.with_columns(
            pl.when(pl.col("BRANCH") == 250)
            .then(pl.lit(92))
            .otherwise(pl.col("BRANCH"))
            .alias("BRANCH")
        )
        for col in ("OPENMH", "CLOSEMH", "CCLOSE", "BCLOSE", "NOACCT", "CURBAL"):
            if col in df.columns:
                df = df.with_columns(pl.col(col).fill_null(0).alias(col))
        df = df.with_columns([
            pl.col("OPENMH").alias("OPENCUM"),
            pl.col("CLOSEMH").alias("CLOSECUM"),
            (pl.col("OPENMH") - pl.col("CLOSEMH")).alias("NETCHGMH"),
        ])
        df = df.with_columns(
            (pl.col("OPENCUM") - pl.col("CLOSECUM")).alias("NETCHGYR")
        )

    return df


# ============================================================================
# STEP 7: Print PROC TABULATE-equivalent fixed-width text report
# PROC TABULATE DATA=CURR FORMAT=COMMA10. MISSING NOSEPS
#   TABLE BRANCH='  ' ALL='TOTAL',
#     SUM=' '*(OPENMH=... OPENCUM=... CLOSEMH=... BCLOSE=... CCLOSE=...
#              CLOSECUM=... NOACCT=... CURBAL=... NETCHGMH=... NETCHGYR=...)
#   BOX='BRANCH' RTS=10;
#
# Row stub labels are rendered via format_brchcd() from PBBELF,
# replicating PUT(BRANCH, BRCHCD.) as used in the TABULATE TABLE statement.
# ============================================================================
_COL_HEADERS = [
    ("OPENMH",   "CURRENT MONTH OPENED"),
    ("OPENCUM",  "CUMULATIVE OPENED"),
    ("CLOSEMH",  "CURRENT MONTH CLOSED"),
    ("BCLOSE",   "CLOSED BY BANK"),
    ("CCLOSE",   "CLOSED BY CUSTOMER"),
    ("CLOSECUM", "CUMULATIVE CLOSED"),
    ("NOACCT",   "NO. OF ACCTS"),
    ("CURBAL",   "TOTAL(RM) O/S"),
    ("NETCHGMH", "NET CHANGE FOR THE MONTH"),
    ("NETCHGYR", "NET CHANGE YEAR TO DATE"),
]


def _fmt_comma10(val, is_curbal: bool = False) -> str:
    """Format as COMMA10. or COMMA17.2 for CURBAL."""
    if val is None or val == 0:
        return "0" if not is_curbal else "0.00"
    if is_curbal:
        return f"{float(val):,.2f}"
    return f"{int(round(float(val))):,}"


def write_tabulate_report(curr: pl.DataFrame, rdate: str, output_path: Path) -> None:
    """
    Produce a PROC TABULATE-equivalent fixed-width text report.
    Rows: one per BRANCH (label via format_brchcd() from PBBELF), plus TOTAL.
    Columns: the 10 SUM variables defined in the TABLE statement.
    """
    VAR_COLS = [v for v, _ in _COL_HEADERS]
    HDR_COLS = [h for _, h in _COL_HEADERS]

    # Aggregate to BRANCH level (drop PRODUCT dimension for display)
    if curr.is_empty():
        agg = pl.DataFrame()
    else:
        agg_exprs = []
        for col in VAR_COLS:
            if col in curr.columns:
                agg_exprs.append(pl.col(col).sum().alias(col))
            else:
                agg_exprs.append(pl.lit(0).alias(col))
        agg = curr.group_by("BRANCH").agg(agg_exprs).sort("BRANCH")

    BRC_W = 10   # branch column width (RTS=10 in PROC TABULATE)
    VAL_W = 14   # value column width

    sep_line = "-" * (BRC_W + len(VAR_COLS) * VAL_W + 4)

    with open(output_path, "w", encoding="utf-8") as fh:
        # Page titles (TITLE1 / TITLE2)
        fh.write(" PUBLIC ISLAMIC BANK BERHAD\n")
        fh.write(
            f" CURRENT ACCOUNT OPENED/CLOSED FOR THE MONTH AS AT {rdate}\n"
        )
        fh.write("\n")

        # Column headers (BOX='BRANCH', multi-word headers split across rows)
        fh.write(f"{'BRANCH':<{BRC_W}}")
        for h in HDR_COLS:
            words = h.split()
            fh.write(f"{words[0]:>{VAL_W}}")
        fh.write("\n")
        fh.write(f"{'':<{BRC_W}}")
        for h in HDR_COLS:
            words = h.split()
            fh.write(f"{(words[1] if len(words) > 1 else ''):>{VAL_W}}")
        fh.write("\n")
        fh.write(f"{'':<{BRC_W}}")
        for h in HDR_COLS:
            words = h.split()
            fh.write(f"{(' '.join(words[2:]) if len(words) > 2 else ''):>{VAL_W}}")
        fh.write("\n")
        fh.write(sep_line + "\n")

        # Data rows — branch label via format_brchcd() from PBBELF,
        # replicating PUT(BRANCH, BRCHCD.) in the TABLE BRANCH='  ' stub.
        totals = {col: 0.0 for col in VAR_COLS}
        for row in (agg.iter_rows(named=True) if not agg.is_empty() else []):
            branch_num   = int(row.get("BRANCH", 0) or 0)
            branch_label = format_brchcd(branch_num) or str(branch_num)
            fh.write(f"{branch_label:>{BRC_W}}")
            for col in VAR_COLS:
                val = row.get(col, 0) or 0
                totals[col] += float(val)
                fh.write(f"{_fmt_comma10(val, is_curbal=(col == 'CURBAL')):>{VAL_W}}")
            fh.write("\n")

        fh.write(sep_line + "\n")

        # TOTAL row (ALL='TOTAL')
        fh.write(f"{'TOTAL':>{BRC_W}}")
        for col in VAR_COLS:
            fh.write(
                f"{_fmt_comma10(totals[col], is_curbal=(col == 'CURBAL')):>{VAL_W}}"
            )
        fh.write("\n")

    print(f"Report written: {output_path}")


# ============================================================================
# MAIN
# ============================================================================
def main():
    print("EIIMDPN2 started.")

    # OPTIONS YEARCUTOFF=1950 SORTDEV=3390 NONUMBER NODATE NOCENTER
    # (applied implicitly)

    # Step 1: Read REPTDATE
    macro    = read_reptdate()
    reptdate = macro["reptdate"]
    rdate    = macro["RDATE"]
    reptmon  = macro["REPTMON"]
    reptmon1 = macro["REPTMON1"]
    print(f"REPTDATE={reptdate}  REPTMON={reptmon}  REPTMON1={reptmon1}")

    # Step 2: Load and filter DEPOSIT.CURRENT
    print("Loading DEPOSIT.CURRENT...")
    curr_raw = load_curr()
    print(f"CURR (after filter): {len(curr_raw):,} rows")

    # Step 3: Write MIS.CURRC{MM}
    print(f"Building MIS.CURRC{reptmon}...")
    build_currc(curr_raw, reptmon)

    # Step 4: PROC SUMMARY by BRANCH PRODUCT
    print("Summarising by BRANCH PRODUCT...")
    curr_summary = summarise_curr(curr_raw)
    print(f"CURR summary: {len(curr_summary):,} rows")

    # Step 5: %PROCESS – cumulative fields
    print("Computing cumulative fields...")
    curr_final = process_cumulative(curr_summary, reptmon, reptmon1)

    # Step 6: Save MIS.CURRF{MM}
    out_currf = MIS_DIR / f"currf{reptmon}.parquet"
    curr_final.write_parquet(str(out_currf))
    print(f"MIS.CURRF{reptmon}: {len(curr_final):,} rows saved to {out_currf}")

    # Step 7: Write tabulate report
    report_path = OUTPUT_DIR / f"EIIMDPN2_{rdate.replace('/', '')}.txt"
    print(f"Writing report to {report_path}...")
    write_tabulate_report(curr_final, rdate, report_path)

    print("EIIMDPN2 completed successfully.")


if __name__ == "__main__":
    main()
