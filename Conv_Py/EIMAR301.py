#!/usr/bin/env python3
"""
Program : EIMAR301.py
Purpose : Details for NPL Accounts for CCD PFB (Monthly Report)
          - EIMAR301-A : NPL accounts >= 2 months arrears & paid <= 2 instalments
          - EIMAR301-B : Accounts with 3-8 months in arrears
          - EIMAR301-C : Summary on accounts with payment of 2 instalments & below
          - EIMAR301-D : Summary on accounts in arrear with 2 instalments paid only

Original JCL notes (kept for traceability):
    //DELETE   EXEC PGM=IEFBR14             -> old SASLIST output deleted;
                                                equivalent handled by opening
                                                the output file in "w" mode.
    //LOAN     DD DSN=SAP.PBB.MNILN(0)      -> NOT referenced anywhere in the
                                                SAS program body; left as an
                                                unused placeholder DD.
    //BRHFILE  DD DSN=RBP2.B033.PBB.BRANCH  -> fixed-width flat file (BRHDATA)
    //BNM      DD DSN=SAP.PBB.CCDTEMP(0)    -> SAS library (GDG generation 0)
                                                holding members REPTDATE and
                                                LOANTEMP. REPTDATE is replaced
                                                by REPTDATE.py per project
                                                convention; LOANTEMP is read as
                                                a .sas7bdat and cached to Parquet.
    //PGM      DD DSN=SAP.BNM.PROGRAM       -> NOT referenced anywhere in the
                                                SAS program body; left as an
                                                unused placeholder DD.
    %INC PGM(PBBLNFMT,PBBELF);              -> supplies ARRCLASS./CACBRCH. formats,
                                                now imported directly from
                                                PBBLNFMT.format_arrclass and
                                                PBBELF.format_cacbrch. &HPD is
                                                inferred from PBBLNFMT.HP_ACTIVE
"""

import gc
import re
import calendar
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
# from input_date import get_latest_file
from PBBLNFMT import HP_ACTIVE, format_arrclass
from PBBELF import format_cacbrch

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat")

CACHE_DIR    = BASE_DIR / "input" / "cache" / "EIMAR301"
OUTPUT_DIR   = BASE_DIR / "output" / "EIMAR301"

LOANTEMP_FILE = STG_DIR / "loantemp.sas7bdat"
INPUT_BRANCH_DIR= Path("/sasdata/rawdata/lookup")
INPUT_BRANCH_FILE = INPUT_BRANCH_DIR / "LKP_BRANCH"     # BRHFILE - static flat file

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000
LRECL      = 256          # DCB LRECL=256 (RECFM=FB -> no ASA control byte)
PAGE_LEN   = 60            # not directly used for row counting here (LINECNT drives it)

# ============================================================================
# STEP 1: REPORT DATE  (DATA _NULL_; SET BNM.REPTDATE; ...)
# ============================================================================
print("Step 1: Deriving report date...")

_rv       = get_reptdate_values(year_format="%Y")
reptdate  = _rv.reptdate

if reptdate.month == 1:
    _pmth  = 12
    _pyear = reptdate.year - 1
else:
    _pmth  = reptdate.month - 1
    _pyear = reptdate.year

# Get the last day of the calculated previous month
_last_day = calendar.monthrange(_pyear, _pmth)[1]
PREPTDTE = date(_pyear, _pmth, _last_day)

# PREPTDTE = date(_pyear, _pmth, 1)          # &PREPTDTE : first day of previous month
RDATE    = reptdate.strftime("%d/%m/%y")   # &RDATE    : DDMMYY8.
REPTYEAR = reptdate.strftime("%Y")         # &REPTYEAR : YEAR4.  (unused downstream, kept for parity)
REPTMON  = reptdate.strftime("%m")         # &REPTMON  : Z2.     (unused downstream, kept for parity)
REPTDAY  = reptdate.strftime("%d")         # &REPTDAY  : Z2.     (unused downstream, kept for parity)

print(f"  Report date : {RDATE}")
print(f"  PREPTDTE    : {PREPTDTE}")

OUTPUT_FILE = OUTPUT_DIR / f"EIMAR301_{reptdate.strftime('%y%m%d')}.txt"
print(f"  Output file : {OUTPUT_FILE.name}")

# ============================================================================
# &HPD macro (session-level %LET, not directly in PBBLNFMT/PBBELF) is inferred
# as HP_ACTIVE: the union of every HP-related PRODUCT code referenced later in
# LOAN1 (CAT='A' 380,381,700,705,720,725; CAT='B' 380,381; CAT='C' 128,130,131,132)
# is exactly {128,130,131,132,380,381,700,705,720,725}, which matches
# PBBLNFMT.HP_ACTIVE (HP - WITHOUT WOFF/WDOWN) verbatim.
# ============================================================================
HPD_PRODUCTS: tuple[int, ...] = tuple(HP_ACTIVE)

# ============================================================================
# STEP 2: RESOLVE LOANTEMP (.sas7bdat, BNM.LOANTEMP GDG(0))
# ============================================================================
print("\nStep 2: Resolving latest LOANTEMP file...")

# loantemp_path = get_latest_file(INPUT_DIR, prefix="loantemp")
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
# STEP 4: BUILD LNTEMP
# PROC SORT DATA=BNM.LOANTEMP OUT=LNTEMP
#   WHERE BALANCE>0 AND BORSTAT NE 'Z' AND PRODUCT IN &HPD; BY BRANCH;
# then MERGE LNTEMP(IN=PRESENT) BRHDATA; BY BRANCH; IF PRESENT=1 THEN OUTPUT;
#   -> equivalent to a LEFT JOIN of LNTEMP onto BRHDATA (unique per BRANCH),
#      keeping every LNTEMP row regardless of a BRHDATA match.
# NOTE: the PROC SORT BY BRANCH itself is not reproduced as a physical sort
# here (unnecessary -- the join below does not depend on input row order);
# the only sort that materially affects report output (LOAN1, by CAT/
# BRANCH/ARREAR2/BALANCE) is performed explicitly further down.
# ============================================================================
print("\nStep 4: Building LNTEMP...")


def build_lntemp(cache_path: Path, hpd_products: tuple[int, ...]) -> pl.DataFrame:
    product_filter = (
        f"CAST(PRODUCT AS INTEGER) IN ({','.join(str(p) for p in hpd_products)})"
        if hpd_products
        else "1=0"
    )

    con = duckdb.connect(database=":memory:")
    df = con.execute(f"""
        SELECT
            CAST(BRANCH   AS INTEGER) AS BRANCH,
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(BALANCE  AS DOUBLE)  AS BALANCE,
            CAST(BORSTAT  AS VARCHAR) AS BORSTAT,
            CAST(PRODUCT  AS INTEGER) AS PRODUCT,
            CAST(ARREAR2  AS DOUBLE)  AS ARREAR2,
            CAST(DATE '1960-01-01' + CAST(ISSDTE AS INTEGER)  AS DATE) AS ISSDTE,
            CAST(DAYDIFF  AS DOUBLE)  AS DAYDIFF,
            CAST(NAME     AS VARCHAR) AS NAME,
            CAST(CAST(NOTENO AS DOUBLE) AS BIGINT) AS NOTENO,   -- <-- changed
            CAST(DATE '1960-01-01' + CAST(LASTRAN AS INTEGER) AS DATE) AS LASTRAN,
            CAST(PAYAMT   AS DOUBLE)  AS PAYAMT,
            CAST(NOISTLPD AS DOUBLE)  AS NOISTLPD,
            CAST(DATE '1960-01-01' + CAST(MATURDT AS INTEGER) AS DATE) AS MATURDT,
            CAST(LSTTRNAM AS DOUBLE)  AS LSTTRNAM,
            CAST(DELQCD   AS VARCHAR) AS DELQCD,
            CAST(COLLDESC AS VARCHAR) AS COLLDESC
        FROM read_parquet('{cache_path}')
        WHERE CAST(BALANCE AS DOUBLE) > 0
          AND CAST(BORSTAT AS VARCHAR) <> 'Z'
          AND {product_filter}
    """).pl()
    con.close()
    return df


lntemp = build_lntemp(LOANTEMP_CACHE, HPD_PRODUCTS)
lntemp = lntemp.join(brhdata, on="BRANCH", how="left")
gc.collect()
print(f"  LNTEMP rows: {len(lntemp):,}")

# ============================================================================
# STEP 5: BUILD LOAN
# DATA LOAN; SET LNTEMP;
#   IF ARREAR2 GE 3 OR BORSTAT IN ('R','I','F','Y') THEN OUTPUT;
#   IF ISSDTE GE &PREPTDTE AND DAYDIFF >= 8 THEN OUTPUT;
# NOTE: these are two INDEPENDENT IF/OUTPUT statements (no ELSE). A row
# satisfying both conditions is output TWICE, faithfully reproducing this
# SAS duplication behaviour rather than silently de-duplicating.
# ============================================================================
print("\nStep 5: Building LOAN (duplicate-output semantics preserved)...")


def build_loan(lntemp_df: pl.DataFrame, preptdte: date) -> pl.DataFrame:
    cond1 = (pl.col("ARREAR2") >= 3) | pl.col("BORSTAT").is_in(["R", "I", "F", "Y"])
    cond2 = (pl.col("ISSDTE") >= preptdte) & (pl.col("DAYDIFF") >= 8)

    part1 = lntemp_df.filter(cond1)
    part2 = lntemp_df.filter(cond2)
    return pl.concat([part1, part2], how="vertical")


loan = build_loan(lntemp, PREPTDTE)
print(f"  LOAN rows (with duplication): {len(loan):,}")

# ============================================================================
# STEP 6: BUILD LOAN1
# DATA LOAN1; FORMAT TYPE $30.; SET LOAN;
#   IF BORSTAT='F' THEN ARREAR2=15;
#   ARREARS=PUT(ARREAR2,ARRCLASS.); CACBR=PUT(BRANCH,CACBRCH.);
#   IF PRODUCT IN (380,381,700,705,720,725) THEN DO CAT='A'; TYPE=...; OUTPUT; END;
#   IF PRODUCT IN (128,130,131,132)         THEN DO CAT='C'; TYPE=...; OUTPUT; END;
#   IF PRODUCT IN (380,381)                 THEN DO CAT='B'; TYPE=...; OUTPUT; END;
# NOTE: three INDEPENDENT IF/OUTPUT blocks. A row with PRODUCT in (380,381)
# matches both the first and third block, producing TWO output rows
# (CAT='A' and CAT='B'). This duplication is preserved intentionally.
# ============================================================================
print("\nStep 6: Building LOAN1 (duplicate-output semantics preserved)...")

_CAT_A_PRODUCTS = (380, 381, 700, 705, 720, 725)
_CAT_C_PRODUCTS = (128, 130, 131, 132)
_CAT_B_PRODUCTS = (380, 381)


def build_loan1(loan_df: pl.DataFrame) -> pl.DataFrame:
    base = loan_df.with_columns(
        pl.when(pl.col("BORSTAT") == "F").then(15).otherwise(pl.col("ARREAR2")).alias("ARREAR2")
    )
    base = base.with_columns([
        pl.col("ARREAR2").map_elements(format_arrclass, return_dtype=pl.Utf8).alias("ARREARS"),
        pl.col("BRANCH").map_elements(format_cacbrch, return_dtype=pl.Utf8).alias("CACBR"),
    ])

    blocks = []
    if len(base):
        rows = base.to_dicts()
        for label, products, type_label in (
            ("A", _CAT_A_PRODUCTS, "HP DIRECT(CONV) ".ljust(30)),
            ("C", _CAT_C_PRODUCTS, "AITAB ".ljust(30)),
            ("B", _CAT_B_PRODUCTS, "HP (380,381) ".ljust(30)),
        ):
            matched = [
                {**r, "CAT": label, "TYPE": type_label}
                for r in rows
                if r["PRODUCT"] in products
            ]
            if matched:
                blocks.append(pl.DataFrame(matched))

    if not blocks:
        return base.clear().with_columns([
            pl.lit(None, dtype=pl.Utf8).alias("CAT"),
            pl.lit(None, dtype=pl.Utf8).alias("TYPE"),
        ])
    return pl.concat(blocks, how="vertical_relaxed")


loan1 = build_loan1(loan)
print(f"  LOAN1 rows (with duplication): {len(loan1):,}")

# ============================================================================
# STEP 7: SORT LOAN1  (required for BY-group first./last. processing below)
# PROC SORT DATA=LOAN1 OUT=LOAN1; BY CAT BRANCH ARREAR2 DESCENDING BALANCE;
# ============================================================================
print("\nStep 7: Sorting LOAN1...")

loan1 = loan1.sort(["CAT", "BRANCH", "ARREAR2", "BALANCE"], descending=[False, False, False, True])

# ============================================================================
# BY-GROUP FIRST./LAST. HELPER
# ============================================================================
def compute_by_flags(rows: list[dict], by_keys: list[str]) -> list[dict]:
    """Attach hierarchical FIRST.<key>/LAST.<key> booleans, matching SAS BY-group logic."""
    n = len(rows)
    for i, row in enumerate(rows):
        prev = rows[i - 1] if i > 0 else None
        nxt  = rows[i + 1] if i < n - 1 else None

        broke_before = prev is None
        broke_after  = nxt is None
        for key in by_keys:
            broke_before = broke_before or (prev is not None and prev[key] != row[key])
            row[f"FIRST_{key}"] = broke_before
        for key in by_keys:
            broke_after = broke_after or (nxt is not None and nxt[key] != row[key])
            row[f"LAST_{key}"] = broke_after
    return rows


# ============================================================================
# COMMA / DATE FORMAT HELPERS
# ============================================================================
def _fmt_comma(value, width: int, decimals: int = 0) -> str:
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    s = f"{v:,.{decimals}f}" if decimals > 0 else f"{int(round(v)):,}"
    return s.rjust(width)


def _fmt_date(value, width: int = 8) -> str:
    if value is None:
        return " " * width
    return value.strftime("%d/%m/%y")


def _fmt_num(value, width: int) -> str:
    if value is None:
        return " " * width
    try:
        return f"{int(value):>{width}d}"
    except (TypeError, ValueError):
        return " " * width


def _place(buf: list[str], col: int, text: str) -> None:
    """Write *text* into buf starting at 1-based column *col*."""
    start = col - 1
    for i, ch in enumerate(text):
        pos = start + i
        if pos < len(buf):
            buf[pos] = ch


DASH40 = "-" * 40
DASH10 = "-" * 10


def _new_buf() -> list[str]:
    return [" "] * LRECL


def _line(buf: list[str]) -> str:
    return "".join(buf).rstrip()


# ============================================================================
# STEP 8: REPORT EIMAR301-A
# WHERE CACBR='000'; BY CAT BRANCH ARREAR2 DESCENDING BALANCE;
# ============================================================================
print("\nStep 8: Generating report EIMAR301-A...")


def _header_a(branch: int, pagecnt: int, type_label: str) -> list[str]:
    lines = []

    buf = _new_buf()
    _place(buf, 1, f"PROGRAM-ID:EIMAR301-A - BRANCH : {branch:>3d}")
    _place(buf, 43, "P U B L I C   B A N K   B E R H A D")
    _place(buf, 118, f"PAGE NO.: {pagecnt}")
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 28, f"{type_label}2 MTHS & ABOVE AND A/C PAID 2 ISTL AND BELOW AS AT {PREPTDTE}")
    lines.append(_line(buf))

    # lines.append("")  # PUT @1 ' ';

    buf = _new_buf()
    _place(buf, 1, "BRH")
    _place(buf, 5, "NAME")
    _place(buf, 25, "NOTENO")
    _place(buf, 34, "ISSUE DT")
    _place(buf, 45, "LST TR DT")
    _place(buf, 61, "ISTL AMT")
    _place(buf, 76, "NO ISTL PD")
    _place(buf, 87, "BORSTAT")
    _place(buf, 95, "ARREARS")
    _place(buf, 115, "BALANCE")
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 1, " ")
    _place(buf, 5, "ACC NO")
    _place(buf, 25, "PRODUCT")
    _place(buf, 34, "MATURE DT")
    _place(buf, 45, "LST TR AMT")
    _place(buf, 95, "DAYS ARR")
    _place(buf, 115, "DELQ REASON CODE")
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 5, "COLLATERAL DESC")
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 1, DASH40)
    _place(buf, 41, DASH40)
    _place(buf, 81, DASH40)
    _place(buf, 121, DASH10)
    lines.append(_line(buf))

    return lines


def _subtotal_block_a(label: str, count: int, amount: float) -> list[str]:
    lines = []
    buf = _new_buf()
    _place(buf, 41, DASH40); _place(buf, 81, DASH40); _place(buf, 121, DASH10)
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 5, label)
    _place(buf, 41, "NO OF A/C : ")
    _place(buf, 53, _fmt_comma(count, 12, 0))
    _place(buf, 114, _fmt_comma(amount, 17, 2))
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 41, DASH40); _place(buf, 81, DASH40); _place(buf, 121, DASH10)
    lines.append(_line(buf))

    # lines.append("")
    return lines


def generate_report_a(loan1_df: pl.DataFrame) -> list[str]:
    filtered = loan1_df.filter(pl.col("CACBR") == "000")
    filtered = filtered.sort(
        ["CAT", "BRANCH", "ARREAR2", "BALANCE"], descending=[False, False, False, True]
    )
    rows = compute_by_flags(filtered.to_dicts(), ["CAT", "BRANCH", "ARREAR2"])

    out: list[str] = []
    pagecnt = 0
    linecnt = 0
    total = totac = 0.0
    brharr = brharrac = brhamt = brhac = 0.0

    for row in rows:
        if row["FIRST_CAT"]:
            total = totac = 0

        if row["FIRST_BRANCH"]:
            brhamt = brhac = 0
            if pagecnt > 0:
                out.append("\f")                     # <-- skip on first page
                linecnt = 0
            pagecnt += 1
            # out.append("\f")  # RECFM=FB -> form-feed marks a new page, no ASA byte
            out.extend(_header_a(row["BRANCH"], pagecnt, row.get("TYPE") or ""))
            # linecnt = 6
            linecnt = 5

        if row["FIRST_ARREAR2"]:
            brharr = brharrac = 0

        buf = _new_buf()
        _place(buf, 1, str(row.get("BRHCODE") or "")[:3])
        _place(buf, 5, str(row.get("NAME") or "")[:20])
        _place(buf, 25, str(row.get("NOTENO") or "")[:9])
        _place(buf, 34, _fmt_date(row.get("ISSDTE")))
        _place(buf, 52, _fmt_date(row.get("LASTRAN")))
        _place(buf, 61, _fmt_comma(row.get("PAYAMT"), 15, 2))
        _place(buf, 77, _fmt_comma(row.get("NOISTLPD"), 8, 0))
        _place(buf, 87, str(row.get("BORSTAT") or "")[:8])
        _place(buf, 95, str(row.get("ARREARS") or "")[:19])
        _place(buf, 114, _fmt_comma(row.get("BALANCE"), 17, 2))
        out.append(_line(buf))

        buf = _new_buf()
        _place(buf, 5, _fmt_num(row.get("ACCTNO"), 20).lstrip().ljust(20))
        _place(buf, 25, _fmt_num(row.get("PRODUCT"), 9).lstrip().ljust(9))
        _place(buf, 34, _fmt_date(row.get("MATURDT")))
        _place(buf, 45, _fmt_comma(row.get("LSTTRNAM"), 15, 2))
        _place(buf, 95, _fmt_comma(row.get("DAYDIFF"), 8, 0))
        _place(buf, 114, str(row.get("DELQCD") or "")[:15])
        out.append(_line(buf))

        buf = _new_buf()
        _place(buf, 5, str(row.get("COLLDESC") or ""))
        out.append(_line(buf))

        # out.append("")  # PUT @1 ' ';
        # linecnt += 4
        linecnt += 3

        balance = row.get("BALANCE") or 0.0
        brharr += balance; brharrac += 1
        brhamt += balance; brhac += 1
        total += balance; totac += 1

        if linecnt > 56:
            out.append("\f")
            linecnt = 0

        if row["LAST_ARREAR2"]:
            out.extend(_subtotal_block_a("SUBTOTAL", brharrac, brharr))
            # linecnt += 4
            linecnt += 3

        if linecnt > 56:
            out.append("\f")
            linecnt = 0

        if row["LAST_BRANCH"]:
            out.extend(_subtotal_block_a("BRANCH TOTAL", brhac, brhamt))
            linecnt += 3

        if row["LAST_CAT"]:
            out.extend(_subtotal_block_a("GRAND TOTAL", totac, total))
            linecnt += 3

    return out


report_a_lines = generate_report_a(loan1)
print(f"  Report A lines: {len(report_a_lines):,}")

# ============================================================================
# STEP 9: REPORT EIMAR301-B
# WHERE (ARREAR2>=4 AND ARREAR2<10) AND (BORSTAT NE 'F' AND BORSTAT NE 'I'
#        AND BORSTAT NE 'R'); BY CAT BRANCH ARREAR2 DESCENDING BALANCE;
# ============================================================================
print("\nStep 9: Generating report EIMAR301-B...")


def _header_b(branch: int, pagecnt: int, type_label: str) -> list[str]:
    lines = []

    buf = _new_buf()
    _place(buf, 1, f"PROGRAM-ID:EIMAR301-B - BRANCH : {branch:>3d}")
    _place(buf, 43, "P U B L I C   B A N K   B E R H A D")
    _place(buf, 110, f"PAGE NO.: {pagecnt}")
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 28, f"{type_label}ACCOUNT WITH 3 - 8 MONTH IN ARREAR AS AT {PREPTDTE}")
    lines.append(_line(buf))

    # lines.append("")

    buf = _new_buf()
    _place(buf, 1, "BRH")
    _place(buf, 5, "ACCTNO")
    _place(buf, 16, "NAME")
    _place(buf, 40, "NOTENO")
    _place(buf, 50, "PRODUCT")
    _place(buf, 59, "BORSTAT")
    _place(buf, 68, "ISSUE DT")
    _place(buf, 78, "DAYS")
    _place(buf, 84, "ARREARS")
    _place(buf, 110, "BALANCE")
    _place(buf, 120, "NO ISTL PAID")
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 1, " ")
    _place(buf, 5, "LST TR DT")
    _place(buf, 16, "MAT. DATE")
    _place(buf, 36, "LST TR AMT")
    _place(buf, 49, "ISTL AMT")
    _place(buf, 59, "COLLATERAL DESCRIPTION")
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 1, DASH40); _place(buf, 41, DASH40); _place(buf, 81, DASH40); _place(buf, 121, DASH10)
    lines.append(_line(buf))

    return lines


def _subtotal_block_b(label: str, count: int, amount: float) -> list[str]:
    lines = []
    buf = _new_buf()
    _place(buf, 41, DASH40); _place(buf, 81, DASH40); _place(buf, 121, DASH10)
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 5, label)
    _place(buf, 41, "NO OF A/C : ")
    _place(buf, 53, _fmt_comma(count, 12, 0))
    _place(buf, 100, _fmt_comma(amount, 17, 2))
    lines.append(_line(buf))

    buf = _new_buf()
    _place(buf, 41, DASH40); _place(buf, 81, DASH40); _place(buf, 121, DASH10)
    lines.append(_line(buf))

    # lines.append("")
    return lines


def generate_report_b(loan1_df: pl.DataFrame) -> list[str]:
    filtered = loan1_df.filter(
        (pl.col("ARREAR2") >= 4) & (pl.col("ARREAR2") < 10)
        & (~pl.col("BORSTAT").is_in(["F", "I", "R"]))
    )
    filtered = filtered.sort(
        ["CAT", "BRANCH", "ARREAR2", "BALANCE"], descending=[False, False, False, True]
    )
    rows = compute_by_flags(filtered.to_dicts(), ["CAT", "BRANCH", "ARREAR2"])

    out: list[str] = []
    pagecnt = 0
    linecnt = 0
    total = totac = 0.0
    brharr = brharrac = brhamt = brhac = 0.0

    for row in rows:
        if row["FIRST_CAT"]:
            total = totac = 0

        if row["FIRST_BRANCH"]:
            pagecnt = 0
            brhamt = brhac = 0
            if pagecnt > 0:
                out.append("\f")                     # <-- skip on first page
                linecnt = 0
            pagecnt += 1
            # out.append("\f")
            out.extend(_header_b(row["BRANCH"], pagecnt, row.get("TYPE") or ""))
            # linecnt = 6
            linecnt = 5

        if row["FIRST_ARREAR2"]:
            brharr = brharrac = 0

        buf = _new_buf()
        _place(buf, 1, str(row.get("BRHCODE") or "")[:4])
        _place(buf, 5, _fmt_num(row.get("ACCTNO"), 11))
        _place(buf, 16, str(row.get("NAME") or "")[:25])
        _place(buf, 41, str(row.get("NOTENO") or "")[:13])
        _place(buf, 54, _fmt_num(row.get("PRODUCT"), 5))
        _place(buf, 59, str(row.get("BORSTAT") or "")[:9])
        _place(buf, 68, _fmt_date(row.get("ISSDTE")))
        _place(buf, 79, _fmt_num(row.get("DAYDIFF"), 5))
        _place(buf, 84, str(row.get("ARREARS") or "")[:16])
        _place(buf, 100, _fmt_comma(row.get("BALANCE"), 17, 2))
        _place(buf, 120, _fmt_comma(row.get("NOISTLPD"), 10, 0))
        out.append(_line(buf))

        buf = _new_buf()
        _place(buf, 5, _fmt_date(row.get("LASTRAN")))
        _place(buf, 16, _fmt_date(row.get("MATURDT")))
        _place(buf, 29, _fmt_comma(row.get("LSTTRNAM"), 17, 2))
        _place(buf, 46, _fmt_comma(row.get("PAYAMT"), 11, 2))
        _place(buf, 59, str(row.get("COLLDESC") or ""))
        out.append(_line(buf))

        # out.append("")
        linecnt += 3

        balance = row.get("BALANCE") or 0.0
        brharr += balance; brharrac += 1
        brhamt += balance; brhac += 1
        total += balance; totac += 1

        if linecnt > 56:
            out.append("\f")
            linecnt = 0

        if row["LAST_ARREAR2"]:
            out.extend(_subtotal_block_b("SUBTOTAL", brharrac, brharr))
            # linecnt += 4
            linecnt += 3

        if linecnt > 56:
            out.append("\f")
            linecnt = 0

        if row["LAST_BRANCH"]:
            out.extend(_subtotal_block_b("BRANCH TOTAL", brhac, brhamt))
            linecnt += 3

        if row["LAST_CAT"]:
            out.extend(_subtotal_block_b("GRAND TOTAL", totac, total))
            total = 0  # SAS: TOTAL = 0; after grand total on LAST.CAT
            linecnt += 3

    return out


report_b_lines = generate_report_b(loan1)
print(f"  Report B lines: {len(report_b_lines):,}")

# ============================================================================
# STEP 10: BUILD NEWREL & ACCARR  (from the merged LNTEMP)
# ============================================================================
print("\nStep 10: Building NEWREL / ACCARR summaries...")


def build_newrel(lntemp_df: pl.DataFrame, preptdte: date) -> pl.DataFrame:
    df = lntemp_df.filter(
        (pl.col("ISSDTE") >= preptdte) & (pl.col("DAYDIFF") >= 8)
    )
    return df.with_columns(
        pl.when(pl.col("NOISTLPD") < 1).then(pl.lit("NO PAYMENT"))
          .when((pl.col("NOISTLPD") >= 1) & (pl.col("NOISTLPD") < 2)).then(pl.lit("PAID 1 ISTL"))
          .otherwise(pl.lit("PAID 2 ISTL"))
          .alias("PAYDESC")
    )


def build_accarr(lntemp_df: pl.DataFrame) -> pl.DataFrame:
    df = lntemp_df.filter(
        (pl.col("NOISTLPD") >= 2) & (pl.col("NOISTLPD") < 3) & (pl.col("DAYDIFF") >= 8)
    )
    return df.with_columns(pl.lit("PAID 2 ISTL").alias("PAYDESC"))


def summarise(df: pl.DataFrame) -> pl.DataFrame:
    """PROC SUMMARY NWAY CLASS BRHCODE PAYDESC; VAR BALANCE; OUTPUT SUM= (freq->NOACCT)."""
    if len(df) == 0:
        return pl.DataFrame(schema={"BRHCODE": pl.Utf8, "PAYDESC": pl.Utf8, "NOACCT": pl.Int64, "BALANCE": pl.Float64})
    return (
        df.group_by(["BRHCODE", "PAYDESC"])
        .agg([pl.len().alias("NOACCT"), pl.col("BALANCE").sum().alias("BALANCE")])
    )


newrel_summary = summarise(build_newrel(lntemp, PREPTDTE))
accarr_summary = summarise(build_accarr(lntemp))
print(f"  NEWREL summary rows: {len(newrel_summary):,}")
print(f"  ACCARR summary rows: {len(accarr_summary):,}")

# ============================================================================
# STEP 11: PROC TABULATE-STYLE REPORTS (EIMAR301-C / EIMAR301-D)
#
# NOTE ON FIDELITY: mainframe PROC TABULATE dynamically computes column
# widths from the box/label text and applied formats; that sizing algorithm
# is not reproduced verbatim here. The layout below uses fixed widths
# derived from the declared formats (COMMA8.0 / COMMA15.2) and RTS=8 (row
# title size), which preserves the reported values and grouping exactly,
# but the column widths are an approximation rather than a byte-identical
# reproduction of the original mainframe listing.
# ============================================================================
print("\nStep 11: Generating tabulate-style reports (C/D)...")

ROW_TITLE_WIDTH = 8       # RTS=8
NOACCT_WIDTH    = 11
BALANCE_WIDTH   = 15
COL_GAP         = 2

PAYDESC_ORDER = ["NO PAYMENT", "PAID 1 ISTL", "PAID 2 ISTL"]


def generate_tabulate_report(
    summary: pl.DataFrame,
    title1: str,
    title2: str,
    title3: str,
    include_all_column: bool,
) -> list[str]:
    out: list[str] = []
    out.append("\f")
    out.append(title1)
    out.append(title2)
    out.append(title3)
    # out.append("")

    categories = [c for c in PAYDESC_ORDER if c in summary["PAYDESC"].unique().to_list()]
    if not categories:
        categories = summary["PAYDESC"].unique().sort().to_list()

    col_width = NOACCT_WIDTH + BALANCE_WIDTH + COL_GAP

    header1 = " " * ROW_TITLE_WIDTH
    header2 = " " * ROW_TITLE_WIDTH
    for cat in categories:
        header1 += cat.center(col_width)
        header2 += "NO OF A/C".rjust(NOACCT_WIDTH) + " " * COL_GAP + "O/S BALANCE".rjust(BALANCE_WIDTH)
    if include_all_column:
        header1 += "TOTAL".center(col_width)
        header2 += "NO OF A/C".rjust(NOACCT_WIDTH) + " " * COL_GAP + "O/S BALANCE".rjust(BALANCE_WIDTH)

    out.append("BRANCH".ljust(ROW_TITLE_WIDTH) + header1[ROW_TITLE_WIDTH:])
    out.append(header2)
    out.append("-" * len(header2))

    branches = summary["BRHCODE"].unique().sort().to_list()
    grand_noacct: dict[str, int] = {c: 0 for c in categories}
    grand_balance: dict[str, float] = {c: 0.0 for c in categories}

    for brh in branches:
        line = brh.ljust(ROW_TITLE_WIDTH)
        for cat in categories:
            match = summary.filter((pl.col("BRHCODE") == brh) & (pl.col("PAYDESC") == cat))
            noacct  = int(match["NOACCT"].sum())  if len(match) else 0
            balance = float(match["BALANCE"].sum()) if len(match) else 0.0
            grand_noacct[cat]  += noacct
            grand_balance[cat] += balance
            line += _fmt_comma(noacct, NOACCT_WIDTH, 0) + " " * COL_GAP + _fmt_comma(balance, BALANCE_WIDTH, 2)

        if include_all_column:
            match = summary.filter(pl.col("BRHCODE") == brh)
            noacct  = int(match["NOACCT"].sum())  if len(match) else 0
            balance = float(match["BALANCE"].sum()) if len(match) else 0.0
            line += _fmt_comma(noacct, NOACCT_WIDTH, 0) + " " * COL_GAP + _fmt_comma(balance, BALANCE_WIDTH, 2)

        out.append(line)

    # ALL='TOTAL' grand total row
    total_line = "TOTAL".ljust(ROW_TITLE_WIDTH)
    for cat in categories:
        total_line += _fmt_comma(grand_noacct[cat], NOACCT_WIDTH, 0) + " " * COL_GAP + _fmt_comma(grand_balance[cat], BALANCE_WIDTH, 2)
    if include_all_column:
        total_line += (
            _fmt_comma(sum(grand_noacct.values()), NOACCT_WIDTH, 0)
            + " " * COL_GAP
            + _fmt_comma(sum(grand_balance.values()), BALANCE_WIDTH, 2)
        )
    out.append(total_line)

    return out


report_c_lines = generate_tabulate_report(
    newrel_summary,
    "PROGRAM ID : EIMAR301-C",
    "PUBLIC BANLK BERHAD",
    f"SUMMARY ON AC WITH PAYMENT OF 2 ISTL & BELOW AS AT {PREPTDTE}",
    include_all_column=True,
)

report_d_lines = generate_tabulate_report(
    accarr_summary,
    "PROGRAM ID : EIMAR301-D",
    "PUBLIC BANLK BERHAD",
    f"SUMMARY ON A/C IN ARREAR WITH 2 ISTL PAID ONLY AS AT {PREPTDTE}",
    include_all_column=False,   # ALL*(...) column intentionally commented out in source
)

print(f"  Report C lines: {len(report_c_lines):,}")
print(f"  Report D lines: {len(report_d_lines):,}")

# ============================================================================
# STEP 12: WRITE OUTPUT  (single SASLIST-equivalent listing file)
# All four reports append to the same physical output, matching how
# FILE PRINT / PROC TABULATE both target the batch job's default listing
# destination (SASLIST DD) in the original JCL.
# ============================================================================
print("\nStep 12: Writing output file...")

all_lines = report_a_lines + report_b_lines + report_c_lines + report_d_lines

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in all_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(all_lines):,}")
# print("\n[RESULT] Report content:")
# for ln in all_lines:
#     print(ln)

del lntemp, loan, loan1
gc.collect()

print("\nEIMAR301 complete.")
