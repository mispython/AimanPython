#!/usr/bin/env python3
"""
Program : EIMAR202.py
Purpose : Outstanding Loans Classified as NPL Report for CCD PFB
          (Month-End Version)
          - Filters LOANTEMP to NPL-flagged records: ARREAR > 6 months OR
            BORSTAT IN ('R','I','F').
          - Categorises the filtered records into CAT A/B/C/D (HP Direct-
            Conv, HP 380/381, AITAB, combined HPD) based on PRODUCT +
            CHECKDT -- identical categorisation logic to EIMAR201.
          - Merges with the branch lookup file.
          - Produces the same 17-bucket arrears-aging report layout as
            EIMAR201, but under an "NPL" title, and APPENDS it onto
            EIMAR201's own output file (SAP.PBB.CCDTXT3, DISP=MOD),
            since EIMAR202 is a direct continuation of EIMAR201's run.

Original JCL notes (kept for traceability):
    //EIMAR202  EXEC SAS609                -> no DELETE step here (unlike
                                                EIMAR201) -- CCDTXT3 is opened
                                                DISP=MOD (append), not created
                                                fresh.
    //LOAN      DD DSN=SAP.PBB.MNILN(0)     -> NOT referenced anywhere in the
                                                SAS program body; left as an
                                                unused placeholder DD (same as
                                                EIMAR201).
    //BRHFILE   DD DSN=RBP2.B033.PBB.BRANCH -> fixed-width flat file (BRHDATA)
    //BNM       DD DSN=SAP.PBB.CCDTEMP(0)   -> SAS library (GDG generation 0)
                                                holding REPTDATE and LOANTEMP.
                                                REPTDATE is replaced by
                                                REPTDATE.py per project
                                                convention; LOANTEMP is read
                                                as a .sas7bdat and cached to
                                                Parquet (EIBDLN1M pattern).
    //CCDTXT3   DD DSN=SAP.PBB.CCDTXT3,DISP=MOD
                                             -> APPEND target: the exact same
                                                physical dataset EIMAR201
                                                wrote via its SASLIST DD.
                                                Reproduced here by opening
                                                EIMAR201's own output file in
                                                "a" (append) mode, inheriting
                                                its DCB (LRECL=133,
                                                RECFM=FBA -- ASA carriage
                                                control).
    //PGM       DD DSN=SAP.BNM.PROGRAM      -> NOT referenced anywhere in the
                                                SAS program body; left as an
                                                unused placeholder DD.
    NOTE: This SAS program contains no %INC PGM(...) statement, so unlike
    EIMAR301 there is no PBBLNFMT/PBBELF (or similar) format-library
    dependency to import here. CAT/TYPE labels are literal assignments
    within the SAS DATA step itself (same convention as EIMAR201).
"""

import gc
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# from REPTDATE import get_reptdate_values
# from input_date import get_latest_file

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat")

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIMAR202"

# NOTE: output_date.py (build_output_file) is NOT used here. EIMAR202 does
# not create a new output dataset -- CCDTXT3 DD DISP=MOD means it appends
# onto the SAME physical file EIMAR201 produced (SAP.PBB.CCDTXT3). The
# output directory/filename below therefore intentionally match EIMAR201.py
# so the two programs share one physical output file, exactly as on the
# mainframe.
OUTPUT_DIR  = BASE_DIR / "output" / "EIMAR201"
OUTPUT_FILE = OUTPUT_DIR / "EIMAR201.txt"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LOANTEMP_FILE      = STG_DIR / "loantemp.sas7bdat"          # BNM.LOANTEMP GDG(0)
INPUT_BRANCH_DIR   = Path("/sasdata/rawdata/lookup")
INPUT_BRANCH_FILE  = INPUT_BRANCH_DIR / "LKP_BRANCH"         # BRHFILE - static flat file

CHUNK_ROWS = 500_000
LRECL      = 133          # Inherited DCB from EIMAR201's SASLIST: LRECL=133,
                           # RECFM=FBA -> byte 0 is the ASA carriage-control
                           # character; SAS content column N (as coded in the
                           # @N clauses below) maps to buf[N], leaving buf[0]
                           # free for ASA.
PAGE_SIZE    = 60          # lines per page (SAS FILE PRINT default)
HEADER_LINES = 8           # NEWPAGE label emits exactly 8 PUT lines

# ============================================================================
# STEP 1: REPORT DATE  (DATA _NULL_; SET BNM.REPTDATE; ...)
# ============================================================================
print("Step 1: Deriving report date...")

# reptdate_values = get_reptdate_values(year_format="%Y")
# reptdate        = reptdate_values.reptdate

reptdate = date.today() - timedelta(days=1)

# # Testing purposes
# reptdate = date(2026, 7, 31)

RDATE    = reptdate.strftime("%d/%m/%y")   # &RDATE    : DDMMYY8.
REPTYEAR = reptdate.strftime("%Y")         # &REPTYEAR : YEAR4.  (unused downstream, kept for parity)
REPTMON  = reptdate.strftime("%m")         # &REPTMON  : Z2.     (unused downstream, kept for parity)
REPTDAY  = reptdate.strftime("%d")         # &REPTDAY  : Z2.     (unused downstream, kept for parity)

print(f"  Report date : {RDATE}")
print(f"  Output file : {OUTPUT_FILE.name} (append mode)")

# ============================================================================
# STEP 2: RESOLVE LOANTEMP (.sas7bdat, BNM.LOANTEMP GDG(0))
# NOTE: input_date.py (get_latest_file) is NOT used here -- BNM.LOANTEMP is
# a fixed GDG "current generation" (0) reference with no date token embedded
# in the filename, same reasoning as EIMAR201.py.
# ============================================================================
print("\nStep 2: Resolving LOANTEMP file...")
print(f"  LOANTEMP : {LOANTEMP_FILE.name}")

LOANTEMP_CACHE = CACHE_DIR / f"{LOANTEMP_FILE.stem}.parquet"


def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a large .sas7bdat to Parquet in streaming chunks (EIBDLN1M pattern)."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total  = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if schema is None:
            schema = table.schema
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        else:
            cast_arrays = []
            for field in schema:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        print(f"  [{tag}] WARNING: cannot cast '{field.name}': {e} -- filling nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)

        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done -- {total:,} rows cached.")


if not _cache_is_fresh(LOANTEMP_FILE, LOANTEMP_CACHE):
    sas_to_parquet(LOANTEMP_FILE, LOANTEMP_CACHE, "LOANTEMP")
else:
    print("  [LOANTEMP] Cache fresh -- skipping conversion.")

# ============================================================================
# STEP 3: READ BRHFILE  (fixed-width flat file, LRECL=80)
# INPUT @2 BRANCH 3.  @6 BRHCODE $3.
# ============================================================================
print("\nStep 3: Reading BRHFILE (branch lookup)...")


def read_brhdata(path: Path) -> pl.DataFrame:
    rows = []
    with open(path, "rb") as fh:
        for raw in fh:
            line = raw.rstrip(b"\r\n")
            if len(line) < 8:
                continue
            branch  = int(line[1:4].decode("latin1").strip() or 0)   # @2 BRANCH 3.
            brhcode = line[5:8].decode("latin1")                     # @6 BRHCODE $3.
            rows.append({"BRANCH": branch, "BRHCODE": brhcode})
    return pl.DataFrame(rows, schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})


brhdata = read_brhdata(INPUT_BRANCH_FILE)
print(f"  BRHDATA rows: {len(brhdata):,}")

# ============================================================================
# STEP 4: BUILD LOAN1
# DATA LOAN1; FORMAT TYPE $13.; SET BNM.LOANTEMP;
#   IF ARREAR > 6 OR BORSTAT = 'R' OR BORSTAT = 'I' OR BORSTAT = 'F' THEN DO;
#      IF (PRODUCT IN (380,381,700,705)) AND CHECKDT=1 THEN DO CAT='A'; ...; OUTPUT; END;
#      IF (PRODUCT IN (380,381))         AND CHECKDT=1 THEN DO CAT='B'; ...; OUTPUT; END;
#      IF (PRODUCT IN (128,130))         AND CHECKDT=1 THEN DO CAT='C'; ...; OUTPUT; END;
#      IF (PRODUCT IN (128,130,380,381,700,705)) AND CHECKDT=1 THEN DO CAT='D'; ...; OUTPUT; END;
#   END;
# NOTE: same as EIMAR201, the four inner IF/OUTPUT blocks are INDEPENDENT
# (no ELSE) -- a row with PRODUCT IN (380,381) matches CAT 'A', 'B' AND 'D'
# simultaneously, producing THREE output rows. This duplication is preserved
# intentionally. The only difference from EIMAR201's LOANTEM2 build is the
# outer NPL qualifying condition (ARREAR > 6 OR BORSTAT IN ('R','I','F')),
# applied once before the CAT split.
# ============================================================================
print("\nStep 4: Building LOAN1 (NPL filter + duplicate-output semantics preserved)...")

CAT_TYPE_LABEL = {
    "A": "(HPD-C)",
    "B": "(HP 380/381)",
    "C": "(AITAB)",
    "D": "(-HPD-)",
}

con = duckdb.connect(database=":memory:")

loan1_cat = con.execute(f"""
    WITH base AS (
        SELECT
            CAST(BRANCH  AS INTEGER) AS BRANCH,
            CAST(PRODUCT AS INTEGER) AS PRODUCT,
            CAST(BALANCE AS DOUBLE)  AS BALANCE,
            CAST(ARREAR  AS INTEGER) AS ARREAR,
            CAST(BORSTAT AS VARCHAR) AS BORSTAT
        FROM read_parquet('{LOANTEMP_CACHE}')
        WHERE CAST(CHECKDT AS INTEGER) = 1
          AND (
                CAST(ARREAR AS INTEGER) > 6
                OR CAST(BORSTAT AS VARCHAR) IN ('R', 'I', 'F')
              )
    )
    SELECT BRANCH, BALANCE, ARREAR, 'A' AS CAT, '(HPD-C)'      AS TYPE
    FROM base WHERE PRODUCT IN (380,381,700,705)

    UNION ALL

    SELECT BRANCH, BALANCE, ARREAR, 'B' AS CAT, '(HP 380/381)' AS TYPE
    FROM base WHERE PRODUCT IN (380,381)

    UNION ALL

    SELECT BRANCH, BALANCE, ARREAR, 'C' AS CAT, '(AITAB)'      AS TYPE
    FROM base WHERE PRODUCT IN (128,130)

    UNION ALL

    SELECT BRANCH, BALANCE, ARREAR, 'D' AS CAT, '(-HPD-)'      AS TYPE
    FROM base WHERE PRODUCT IN (128,130,380,381,700,705)
""").pl()

con.close()
gc.collect()
print(f"  LOAN1 rows (with duplication): {len(loan1_cat):,}")

# ============================================================================
# STEP 5: BUILD LOAN1  (rejoin with branch data)
# PROC SORT DATA=LOAN1; BY BRANCH;
# DATA LOAN1; MERGE LOAN1(IN=PRESENT) BRHDATA; BY BRANCH;
#   IF PRESENT=1 THEN OUTPUT LOAN1;
# -> equivalent to a LEFT JOIN of LOAN1(cat) onto BRHDATA (unique per
#    BRANCH), keeping every LOAN1(cat) row regardless of a BRHDATA match.
#    The PROC SORT BY BRANCH itself is not reproduced as a physical sort
#    here (unnecessary for the join below); the sort that materially
#    affects report output (CAT, BRANCH) is applied further down.
# ============================================================================
print("\nStep 5: Rejoining LOAN1 with branch data...")

loan1 = loan1_cat.join(brhdata, on="BRANCH", how="left")
del loan1_cat
gc.collect()
print(f"  LOAN1 rows: {len(loan1):,}")

# ============================================================================
# STEP 6: AGGREGATE ARREAR BUCKETS  (drives the TRY-step array accumulation)
# DATA TRY; ARRAY BRHAMT{17}/NOACC{17}; SET LOAN1; BY CAT BRANCH;
#   IF BALANCE GT 0 THEN DO; BRHAMT(ARREAR)+BALANCE; NOACC(ARREAR)+1; END;
# Pre-aggregating SUM(BALANCE)/COUNT(*) per CAT+BRANCH+ARREAR reproduces the
# same per-branch bucket totals as the SAS array accumulation, without
# needing a manual row-by-row retained-array walk (same convention as
# EIMAR201.py).
# ============================================================================
print("\nStep 6: Aggregating arrears buckets per CAT/BRANCH/ARREAR...")

bucket_agg = (
    loan1.filter(pl.col("BALANCE") > 0)
    .group_by(["CAT", "BRANCH", "ARREAR"])
    .agg([
        pl.col("BALANCE").sum().alias("AMT"),
        pl.len().alias("CNT"),
    ])
)

# Every distinct CAT/BRANCH combination present in LOAN1 drives one
# LAST.BRANCH detail block, regardless of whether any BALANCE>0 rows exist
# for that branch (matches the SAS BY-group behaviour, which triggers on
# every BY-group break independent of the BALANCE condition).
branch_universe = (
    loan1.select(["CAT", "BRANCH", "BRHCODE"])
    .unique()
    .sort(["CAT", "BRANCH"])
)

print(f"  Bucket rows       : {len(bucket_agg):,}")
print(f"  CAT/BRANCH groups : {len(branch_universe):,}")

del loan1
gc.collect()

# ============================================================================
# STEP 7: REPORT FORMAT HELPERS  (ASA carriage control, LRECL=133)
# ============================================================================
print("\nStep 7: Generating report...")

DASH40 = "-" * 40
DASH10 = "-" * 10


def _new_buf() -> list[str]:
    return [" "] * LRECL


def _place(buf: list[str], col: int, text: str) -> None:
    """Write *text* into buf starting at logical (SAS) column *col*.
    buf[0] is reserved for the ASA control character, so logical column N
    maps directly to physical index N."""
    for i, ch in enumerate(text):
        pos = col + i
        if pos < len(buf):
            buf[pos] = ch


def _line(buf: list[str], asa: str = " ") -> str:
    buf[0] = asa
    return "".join(buf).rstrip()


def _fmt_comma(value, width: int, decimals: int = 0) -> str:
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    s = f"{v:,.{decimals}f}" if decimals > 0 else f"{int(round(v)):,}"
    return s.rjust(width)


def _fmt_z(value, width: int) -> str:
    """Z-format: zero-padded, no thousands separator."""
    if value is None:
        return " " * width
    try:
        return f"{int(value):0{width}d}"
    except (TypeError, ValueError):
        return " " * width


def _build_header(type_label: str, pagecnt: int) -> list[str]:
    """NEWPAGE label -- 8 PUT lines. First line carries the ASA '1' (page eject)."""
    lines: list[str] = []

    buf = _new_buf()
    _place(buf, 1, "PROGRAM-ID : EIMAR202")
    _place(buf, 43, "P U B L I C   B A N K   B E R H A D")
    _place(buf, 118, f"PAGE NO.: {pagecnt}")
    lines.append(_line(buf, "1"))

    buf = _new_buf()
    _place(buf, 32, "OUTSTANDING LOANS CLASSIFIED AS NPL ISSUED FROM 1 JAN 98")
    _place(buf, 90, f"{type_label:<13s}"[:13])
    _place(buf, 104, RDATE)
    lines.append(_line(buf))

    lines.append(_line(_new_buf()))   # PUT @1 ' ';

    buf = _new_buf()
    _place(buf, 1,   "BRH     NO         < 1 MTH")
    _place(buf, 34,  "NO     1 TO < 2 MTH")
    _place(buf, 59,  "NO     2 TO < 3 MTH")
    _place(buf, 84,  "NO      3 TO < 4 MTH")
    _place(buf, 111, "NO      4 TO < 5 MTH")
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 1,   "        NO    5 TO < 6 MTH")
    _place(buf, 34,  "NO     6 TO < 7 MTH")
    _place(buf, 59,  "NO     7 TO < 8 MTH")
    _place(buf, 84,  "NO      8 TO < 9 MTH")
    _place(buf, 111, "NO     9 TO < 10 MTH")
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 1,   "        NO  10 TO < 11 MTH")
    _place(buf, 34,  "NO   11 TO < 12 MTH")
    _place(buf, 59,  "NO   12 TO < 18 MTH")
    _place(buf, 84,  "NO    18 TO < 24 MTH")
    _place(buf, 111, "NO    24 TO < 36 MTH")
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 1,   "        NO        > 36 MTH")
    _place(buf, 34,  "NO          DEFICIT")
    _place(buf, 59,  "NO   SUBTOTAL >=3MTH")
    _place(buf, 84,  "NO   SUBTOTAL >=6MTH")
    _place(buf, 111, "NO             TOTAL")
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 1, DASH40); _place(buf, 41, DASH40)
    _place(buf, 81, DASH40); _place(buf, 121, DASH10)
    lines.append(_line(buf))

    return lines


def _branch_detail_lines(branch: int, brhcode: str, amt: dict, cnt: dict) -> list[str]:
    """LAST.BRANCH block -- 4 PUT lines (17 buckets + subtotal columns)."""
    subbrh = sum(amt.get(i, 0.0) for i in range(4, 18))
    subbr2 = subbrh - amt.get(4, 0.0) - amt.get(5, 0.0) - amt.get(6, 0.0)
    subacc = sum(cnt.get(i, 0) for i in range(4, 18))
    subac2 = subacc - cnt.get(4, 0) - cnt.get(5, 0) - cnt.get(6, 0)
    totbrh = subbrh + amt.get(1, 0.0) + amt.get(2, 0.0) + amt.get(3, 0.0)
    sotacc = subacc + cnt.get(1, 0) + cnt.get(2, 0) + cnt.get(3, 0)

    lines: list[str] = []

    buf = _new_buf()
    _place(buf, 1, _fmt_z(branch, 3))
    _place(buf, 5,   _fmt_comma(cnt.get(1, 0), 7, 0));  _place(buf, 13,  _fmt_comma(amt.get(1, 0.0), 16, 2))
    _place(buf, 30,  _fmt_comma(cnt.get(2, 0), 7, 0));  _place(buf, 38,  _fmt_comma(amt.get(2, 0.0), 15, 2))
    _place(buf, 54,  _fmt_comma(cnt.get(3, 0), 7, 0));  _place(buf, 62,  _fmt_comma(amt.get(3, 0.0), 15, 2))
    _place(buf, 78,  _fmt_comma(cnt.get(4, 0), 8, 0));  _place(buf, 87,  _fmt_comma(amt.get(4, 0.0), 17, 2))
    _place(buf, 105, _fmt_comma(cnt.get(5, 0), 8, 0));  _place(buf, 114, _fmt_comma(amt.get(5, 0.0), 17, 2))
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 1, (brhcode or "")[:3])
    _place(buf, 5,   _fmt_comma(cnt.get(6, 0), 7, 0));  _place(buf, 13,  _fmt_comma(amt.get(6, 0.0), 16, 2))
    _place(buf, 30,  _fmt_comma(cnt.get(7, 0), 7, 0));  _place(buf, 38,  _fmt_comma(amt.get(7, 0.0), 15, 2))
    _place(buf, 54,  _fmt_comma(cnt.get(8, 0), 7, 0));  _place(buf, 62,  _fmt_comma(amt.get(8, 0.0), 15, 2))
    _place(buf, 78,  _fmt_comma(cnt.get(9, 0), 8, 0));  _place(buf, 87,  _fmt_comma(amt.get(9, 0.0), 17, 2))
    _place(buf, 105, _fmt_comma(cnt.get(10, 0), 8, 0)); _place(buf, 114, _fmt_comma(amt.get(10, 0.0), 17, 2))
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 5,   _fmt_comma(cnt.get(11, 0), 7, 0)); _place(buf, 13,  _fmt_comma(amt.get(11, 0.0), 16, 2))
    _place(buf, 30,  _fmt_comma(cnt.get(12, 0), 7, 0)); _place(buf, 38,  _fmt_comma(amt.get(12, 0.0), 15, 2))
    _place(buf, 54,  _fmt_comma(cnt.get(13, 0), 7, 0)); _place(buf, 62,  _fmt_comma(amt.get(13, 0.0), 15, 2))
    _place(buf, 78,  _fmt_comma(cnt.get(14, 0), 8, 0)); _place(buf, 87,  _fmt_comma(amt.get(14, 0.0), 17, 2))
    _place(buf, 105, _fmt_comma(cnt.get(15, 0), 8, 0)); _place(buf, 114, _fmt_comma(amt.get(15, 0.0), 17, 2))
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 5,   _fmt_comma(cnt.get(16, 0), 7, 0)); _place(buf, 13,  _fmt_comma(amt.get(16, 0.0), 16, 2))
    _place(buf, 30,  _fmt_comma(cnt.get(17, 0), 7, 0)); _place(buf, 38,  _fmt_comma(amt.get(17, 0.0), 15, 2))
    _place(buf, 54,  _fmt_comma(subacc, 7, 0));         _place(buf, 62,  _fmt_comma(subbrh, 15, 2))
    _place(buf, 78,  _fmt_comma(subac2, 8, 0));         _place(buf, 87,  _fmt_comma(subbr2, 17, 2))
    _place(buf, 105, _fmt_comma(sotacc, 8, 0));         _place(buf, 114, _fmt_comma(totbrh, 17, 2))
    lines.append(_line(buf))

    return lines


def _grand_total_lines(totamt: dict, totacc: dict) -> list[str]:
    """LAST.CAT block -- dashes + TOT (4 PUT lines) + dashes + blank = 7 lines."""
    sgtotbrh = sum(totamt.get(i, 0.0) for i in range(4, 18))
    sgtotbr2 = sgtotbrh - totamt.get(4, 0.0) - totamt.get(5, 0.0) - totamt.get(6, 0.0)
    sgtotacc = sum(totacc.get(i, 0) for i in range(4, 18))
    sgtotac2 = sgtotacc - totacc.get(4, 0) - totacc.get(5, 0) - totacc.get(6, 0)
    gtotbrh  = sgtotbrh + totamt.get(1, 0.0) + totamt.get(2, 0.0) + totamt.get(3, 0.0)
    gtotacc  = sgtotacc + totacc.get(1, 0) + totacc.get(2, 0) + totacc.get(3, 0)

    lines: list[str] = []

    buf = _new_buf()
    _place(buf, 1, DASH40); _place(buf, 41, DASH40)
    _place(buf, 81, DASH40); _place(buf, 121, DASH10)
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 1, "TOT")
    _place(buf, 5,   _fmt_comma(totacc.get(1, 0), 7, 0));  _place(buf, 13,  _fmt_comma(totamt.get(1, 0.0), 16, 2))
    _place(buf, 30,  _fmt_comma(totacc.get(2, 0), 7, 0));  _place(buf, 38,  _fmt_comma(totamt.get(2, 0.0), 15, 2))
    _place(buf, 54,  _fmt_comma(totacc.get(3, 0), 7, 0));  _place(buf, 62,  _fmt_comma(totamt.get(3, 0.0), 15, 2))
    _place(buf, 78,  _fmt_comma(totacc.get(4, 0), 8, 0));  _place(buf, 87,  _fmt_comma(totamt.get(4, 0.0), 17, 2))
    _place(buf, 105, _fmt_comma(totacc.get(5, 0), 8, 0));  _place(buf, 114, _fmt_comma(totamt.get(5, 0.0), 17, 2))
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 5,   _fmt_comma(totacc.get(6, 0), 7, 0));  _place(buf, 13,  _fmt_comma(totamt.get(6, 0.0), 16, 2))
    _place(buf, 30,  _fmt_comma(totacc.get(7, 0), 7, 0));  _place(buf, 38,  _fmt_comma(totamt.get(7, 0.0), 15, 2))
    _place(buf, 54,  _fmt_comma(totacc.get(8, 0), 7, 0));  _place(buf, 62,  _fmt_comma(totamt.get(8, 0.0), 15, 2))
    _place(buf, 78,  _fmt_comma(totacc.get(9, 0), 8, 0));  _place(buf, 87,  _fmt_comma(totamt.get(9, 0.0), 17, 2))
    _place(buf, 105, _fmt_comma(totacc.get(10, 0), 8, 0)); _place(buf, 114, _fmt_comma(totamt.get(10, 0.0), 17, 2))
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 5,   _fmt_comma(totacc.get(11, 0), 7, 0)); _place(buf, 13,  _fmt_comma(totamt.get(11, 0.0), 16, 2))
    _place(buf, 30,  _fmt_comma(totacc.get(12, 0), 7, 0)); _place(buf, 38,  _fmt_comma(totamt.get(12, 0.0), 15, 2))
    _place(buf, 54,  _fmt_comma(totacc.get(13, 0), 7, 0)); _place(buf, 62,  _fmt_comma(totamt.get(13, 0.0), 15, 2))
    _place(buf, 78,  _fmt_comma(totacc.get(14, 0), 8, 0)); _place(buf, 87,  _fmt_comma(totamt.get(14, 0.0), 17, 2))
    _place(buf, 105, _fmt_comma(totacc.get(15, 0), 8, 0)); _place(buf, 114, _fmt_comma(totamt.get(15, 0.0), 17, 2))
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 5,   _fmt_comma(totacc.get(16, 0), 7, 0)); _place(buf, 13,  _fmt_comma(totamt.get(16, 0.0), 16, 2))
    _place(buf, 30,  _fmt_comma(totacc.get(17, 0), 7, 0)); _place(buf, 38,  _fmt_comma(totamt.get(17, 0.0), 15, 2))
    _place(buf, 54,  _fmt_comma(sgtotacc, 7, 0));          _place(buf, 62,  _fmt_comma(sgtotbrh, 15, 2))
    _place(buf, 78,  _fmt_comma(sgtotac2, 8, 0));          _place(buf, 87,  _fmt_comma(sgtotbr2, 17, 2))
    _place(buf, 105, _fmt_comma(gtotacc, 8, 0));           _place(buf, 114, _fmt_comma(gtotbrh, 17, 2))
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 1, DASH40); _place(buf, 41, DASH40)
    _place(buf, 81, DASH40); _place(buf, 121, DASH10)
    lines.append(_line(buf))

    lines.append(_line(_new_buf()))   # PUT; blank line

    return lines


# ============================================================================
# STEP 8: MAIN REPORT LOOP  (equivalent of DATA TRY; BY CAT BRANCH;)
# ============================================================================
output_lines: list[str] = []

cats_present = sorted(branch_universe["CAT"].unique().to_list())

for cat in cats_present:
    type_label = CAT_TYPE_LABEL.get(cat, "")
    branches_in_cat = (
        branch_universe.filter(pl.col("CAT") == cat)
        .sort("BRANCH")
        .to_dicts()
    )

    pagecnt = 0
    lines_on_page = 0
    totamt: dict[int, float] = {}
    totacc: dict[int, int] = {}

    def _print_header() -> None:
        nonlocal pagecnt, lines_on_page
        pagecnt += 1
        output_lines.extend(_build_header(type_label, pagecnt))
        lines_on_page = HEADER_LINES

    _print_header()   # IF FIRST.CAT THEN PUT _PAGE_;

    for b in branches_in_cat:
        branch  = b["BRANCH"]
        brhcode = b["BRHCODE"]

        if lines_on_page + 4 > PAGE_SIZE:
            _print_header()

        bucket_rows = bucket_agg.filter(
            (pl.col("CAT") == cat) & (pl.col("BRANCH") == branch)
        ).to_dicts()
        amt = {int(r["ARREAR"]): r["AMT"] for r in bucket_rows}
        cnt = {int(r["ARREAR"]): r["CNT"] for r in bucket_rows}

        output_lines.extend(_branch_detail_lines(branch, brhcode, amt, cnt))
        lines_on_page += 4

        for i in range(1, 18):
            totamt[i] = totamt.get(i, 0.0) + amt.get(i, 0.0)
            totacc[i] = totacc.get(i, 0) + cnt.get(i, 0)

    if lines_on_page + 7 > PAGE_SIZE:
        _print_header()

    output_lines.extend(_grand_total_lines(totamt, totacc))
    lines_on_page += 7   # PAGECNT reset to 0 happens implicitly (new CAT resets pagecnt above)

print(f"  Total report lines: {len(output_lines):,}")

# ============================================================================
# STEP 9: WRITE OUTPUT  (APPEND -- CCDTXT3 DD DISP=MOD onto EIMAR201's file)
# ============================================================================
with open(OUTPUT_FILE, "a", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(ln + "\n")

print(f"\n  Output appended to : {OUTPUT_FILE}")

print("\n[RESULT] Report content:")
for ln in output_lines:
    print(ln)

del bucket_agg, branch_universe
gc.collect()

print("\nEIMAR202 complete.")
