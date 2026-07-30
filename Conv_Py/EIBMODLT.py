#!/usr/bin/env python3
"""
Program : EIBMODLT.py
Purpose : OD (Overdraft) Listing Report
          - Listing of overdraft accounts by FISS Purpose Code (all customer
            codes) and by Sector Code (construction 5001-5999 / real estate
            8310, non-individual customers only)
          - Produces both banks covered by the original JOB:
              EIBMODLT step -> PBB  (Public Bank Berhad)
              EIBMODLI step -> PIBB (Public Islamic Bank Berhad)

Dependencies:
    REPTDATE.py   -> get_reptdate_values()  (report date / week derivation)
    input_date.py -> get_latest_file()      (latest LOAN / DEPOSIT file by date)
    output_date.py-> NOT USED. Original DD names (SAP.BANK.ODLIST.RPS/.TEXT,
                     SAP.PIBB.ODLIST.RPS/.TEXT) carry no date component in
                     the dataset name, so static output filenames are used
                     instead, per project convention.

NOTE ON MAINFRAME-ONLY STEPS (no SAS source available to convert):
    - The JOB's leading IEFBR14 DELETE steps merely purge old cataloged
      datasets before (re)creation. Python overwrites output files directly,
      so no equivalent action is required.
    - STEP02 / STEP04 (PGM=SPLIB136) is an external mainframe utility that
      post-processes the RPS-format report (inserting real ASA/printer
      control bytes and splitting it by branch region using
      RMDS.OPC.BANKCODE members). Its source is not provided, so it is left
      as a commented placeholder below.
"""

import gc
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
# from output_date import build_output_file  # NOT USED - output filenames carry no date component

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# Each of the 4 source files (PBB deposit, PBB loan, PIBB deposit, PIBB loan)
# comes from a distinct SAS dataset qualifier in the original JOB:
#   DEPOSIT DD DSN=SAP.PBB.MNITB(0)   -> PBB  deposit
#   LOAN    DD DSN=SAP.PBB.SASDATA    -> PBB  loan
#   DEPOSIT DD DSN=SAP.PIBB.MNITB(0)  -> PIBB deposit
#   LOAN    DD DSN=SAP.PIBB.SASDATA   -> PIBB loan
# so each file gets its own independent input/cache directory rather than
# sharing a folder with any other file.
INPUT_DIR_PBB_DEPOSIT   = BASE_DIR / "input" / "prod" / "EIBMODLT" / "PBB_DEPOSIT"
INPUT_DIR_PBB_LOAN      = BASE_DIR / "input" / "prod" / "EIBMODLT" / "PBB_LOAN"
INPUT_DIR_PIBB_DEPOSIT  = BASE_DIR / "input" / "prod" / "EIBMODLT" / "PIBB_DEPOSIT"
INPUT_DIR_PIBB_LOAN     = BASE_DIR / "input" / "prod" / "EIBMODLT" / "PIBB_LOAN"

CACHE_DIR_PBB_DEPOSIT   = INPUT_DIR_PBB_DEPOSIT
CACHE_DIR_PBB_LOAN      = INPUT_DIR_PBB_LOAN
CACHE_DIR_PIBB_DEPOSIT  = INPUT_DIR_PIBB_DEPOSIT
CACHE_DIR_PIBB_LOAN     = INPUT_DIR_PIBB_LOAN

OUTPUT_DIR = BASE_DIR / "output" / "EIBMODLT"

for _d in (
    INPUT_DIR_PBB_DEPOSIT, INPUT_DIR_PBB_LOAN,
    INPUT_DIR_PIBB_DEPOSIT, INPUT_DIR_PIBB_LOAN,
):
    _d.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000

# ============================================================================
# STEP: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date / week number...")

reptdate_values = get_reptdate_values()
reptdate = reptdate_values.reptdate
REPTMON  = reptdate_values.reptmon

# Original SAS derives WK/WK1/SDD from an exact-match WHEN(DAY(REPTDATE))
# structure (day 8/15/22/otherwise). REPTDATE.py's NOWK banding
# (1-8 / 9-15 / 16-22 / 23-31) covers the same four buckets, since these
# jobs only ever run on days 8, 15, 22, or month-end. NOWK is therefore
# used as the WK equivalent below.
NOWK = reptdate_values.nowk

# NOTE: SDD and WK1 are computed in the original SAS WHEN block but are
# never referenced anywhere else in the program, so they are not
# reproduced here (no behavioural effect is lost).

RDATE = reptdate.strftime("%d/%m/%y")  # PUT(REPTDATE, DDMMYY8.)

print(f"  Report date : {RDATE}")
print(f"  Report month: {REPTMON}  Week: {NOWK}")

# PROC FORMAT VALUE BANKFMT 33='PBB' 134='PFB'; -- defined in SAS but never
# applied via PUT(var, BANKFMT.) anywhere in this program, so it is not
# reproduced (no live import/usage exists to trace).


# ============================================================================
# CACHING HELPERS  (pattern follows EIBDLN1M.py)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a .sas7bdat file to Parquet in streaming chunks."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

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
                        print(f"  [{tag}] WARNING: cannot cast '{field.name}' "
                              f"from {col.type} to {field.type}: {e} - filling nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)

        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows cached.")


# ============================================================================
# FIXED-WIDTH FORMATTING HELPERS
# ============================================================================
def _line(width: int = 136) -> list:
    return [" "] * width


def _place(buf: list, col: int, text: str) -> None:
    """Place text into buf at a 1-based column position."""
    start = col - 1
    end = start + len(text)
    if end > len(buf):
        buf.extend([" "] * (end - len(buf)))
    buf[start:end] = list(text)


def _comma(value, width: int, decimals: int = 2) -> str:
    """COMMAw.d style: comma-separated, right-justified, sign if negative."""
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    return f"{v:,.{decimals}f}".rjust(width)


def _num(value, width: int) -> str:
    """Plain numeric PUT (e.g. 3., 10., 12.) - right-justified, no commas."""
    if value is None:
        return " " * width
    try:
        return f"{int(value)}".rjust(width)
    except (TypeError, ValueError):
        return " " * width


def _zpad(value, width: int) -> str:
    """Zn. style zero-padded numeric."""
    if value is None:
        return "0" * width
    return f"{int(value):0{width}d}"


def _char(value, width: int, right: bool = False) -> str:
    """Character PUT: $w. (left-justified) or $w.-R (right-justified)."""
    s = "" if value is None else str(value)
    s = s[:width]
    return s.rjust(width) if right else s.ljust(width)


def _write(fh, buf: list) -> None:
    fh.write("".join(buf) + "\n")


def _branch_no(branch) -> str:
    """SAS SELECT(BRANCH<10 / <100 / >99) -> BRNO computation."""
    b = int(branch or 0)
    if b < 10:
        return f"BR0{b}"
    if b < 100:
        return f"BR{b}"
    if b > 99:
        return f"B{b}"
    return ""  # unreachable in practice (mirrors SAS OTHERWISE; no assignment)


# ============================================================================
# STATIC HEADER LINE CONTENT  (shared identically between both reports)
# ============================================================================
def _blank_p001_line() -> str:
    buf = _line()
    _place(buf, 1, "P001")
    return "".join(buf)


def _ghijk_lines() -> list:
    lines = []

    buf = _line()
    _place(buf, 1, "P001")
    _place(buf, 62, "APPROVED")
    _place(buf, 76, "OUTSTANDING")
    _place(buf, 89, "FISS PURPOSE")
    _place(buf, 103, "SECTOR")
    _place(buf, 111, "CUST")
    _place(buf, 118, "STATE")
    lines.append("".join(buf))

    buf = _line()
    _place(buf, 1, "P001")
    _place(buf, 10, "A/C NO.")
    _place(buf, 19, "NAME OF CUSTOMER")
    _place(buf, 65, "LIMIT")
    _place(buf, 80, "BALANCE")
    _place(buf, 95, "CODE")
    _place(buf, 105, "CODE")
    _place(buf, 111, "CODE")
    _place(buf, 119, "CODE")
    _place(buf, 124, "FLAT RATE")
    lines.append("".join(buf))

    buf = _line()
    _place(buf, 1, "P001")
    _place(buf, 14, "LIMIT1")
    _place(buf, 31, "LIMIT2")
    _place(buf, 49, "LIMIT3")
    _place(buf, 67, "LIMIT4")
    _place(buf, 85, "LIMIT5")
    _place(buf, 95, "RATE1")
    _place(buf, 102, "RATE2")
    _place(buf, 109, "RATE3")
    _place(buf, 116, "RATE4")
    _place(buf, 123, "RATE5")
    lines.append("".join(buf))

    buf = _line()
    _place(buf, 1, "P001")
    _place(buf, 95, "COLL1")
    _place(buf, 102, "COLL2")
    _place(buf, 109, "COLL3")
    _place(buf, 116, "COLL4")
    _place(buf, 123, "COLL5")
    lines.append("".join(buf))

    buf = _line()
    _place(buf, 1, "P001")
    _place(buf, 5, "-" * 30)
    _place(buf, 35, "-" * 30)
    _place(buf, 65, "-" * 30)
    _place(buf, 95, "-" * 30)
    _place(buf, 125, "-" * 10)
    lines.append("".join(buf))

    return lines


GHIJK_LINES = _ghijk_lines()
BLANK_P001_LINE = _blank_p001_line()


def _line_c(pagecnt: int, bank_title: str) -> str:
    buf = _line()
    _place(buf, 1, "P000REPORT NO :  ODLIST")
    _place(buf, 44, bank_title)
    _place(buf, 122, f"PAGE NO : {pagecnt}")
    return "".join(buf)


def _header_lines_fisspurp(branch, pagecnt: int, bank_title: str) -> list:
    lines = [_line_c(pagecnt, bank_title)]

    buf = _line()
    _place(buf, 1, "P001BRANCH    :  ")
    _place(buf, 22, _zpad(branch, 3))
    _place(buf, 32, f"OD LISTING FOR FISS PURPOSE CODE (FOR ALL CUSTCODES) {RDATE}")
    lines.append("".join(buf))

    lines.append(BLANK_P001_LINE)
    lines.append(BLANK_P001_LINE)
    lines.extend(GHIJK_LINES)
    lines.append(BLANK_P001_LINE)
    lines.append(BLANK_P001_LINE)
    return lines


def _header_lines_sector(branch, pagecnt: int, bank_title: str) -> list:
    lines = [_line_c(pagecnt, bank_title)]

    buf = _line()
    _place(buf, 1, "P001BRANCH    :  ")
    _place(buf, 22, _zpad(branch, 3))
    _place(buf, 36, "OD LISTING BY CONSTRUCTION (SECTCODE 5001-5999) AND")
    lines.append("".join(buf))

    # NOTE: original SAS PUT statement for this second title line has no
    # terminating semicolon before the following PUT @1 'P001';, and it
    # carries no leading 'P001' control code of its own (starts directly at
    # @31). This is reproduced literally: columns 1-30 are blank.
    buf = _line()
    _place(buf, 31, f"REAL ESTATE (SECTCODE 8310) FOR NON-INDI. CUSTOMER FOR {RDATE}")
    lines.append("".join(buf))

    lines.append(BLANK_P001_LINE)
    lines.append(BLANK_P001_LINE)
    lines.extend(GHIJK_LINES)
    lines.append(BLANK_P001_LINE)
    lines.append(BLANK_P001_LINE)
    return lines


# ============================================================================
# DETAIL / SUBTOTAL / GRANDTOTAL / NEWPAGE WRITERS
# ============================================================================
def _write_detail(fh, row: dict) -> None:
    buf = _line()
    _place(buf, 1, "P001")
    _place(buf, 5, _num(row.get("ACCTNO"), 12))
    _place(buf, 19, _char(row.get("NAME"), 33))
    _place(buf, 52, _comma(row.get("APPRLIMT"), 18, 2))
    _place(buf, 72, _comma(row.get("BALANCE"), 15, 2))
    _place(buf, 95, _char(row.get("FISSPURP"), 4, right=True))
    _place(buf, 105, _char(row.get("SECTORCD"), 4, right=True))
    _place(buf, 111, _num(row.get("CUSTCODE"), 4))
    _place(buf, 117, _char(row.get("STATE"), 6, right=True))
    _place(buf, 125, _comma(row.get("FLATRATE"), 8, 2))
    _write(fh, buf)

    buf = _line()
    _place(buf, 1, "P001")
    _place(buf, 5, _comma(row.get("LIMIT1"), 15, 2))
    _place(buf, 22, _comma(row.get("LIMIT2"), 15, 2))
    _place(buf, 40, _comma(row.get("LIMIT3"), 15, 2))
    _place(buf, 58, _comma(row.get("LIMIT4"), 15, 2))
    _place(buf, 76, _comma(row.get("LIMIT5"), 15, 2))
    _place(buf, 94, _comma(row.get("RATE1"), 6, 2))
    _place(buf, 101, _comma(row.get("RATE2"), 6, 2))
    _place(buf, 108, _comma(row.get("RATE3"), 6, 2))
    _place(buf, 115, _comma(row.get("RATE4"), 6, 2))
    _place(buf, 122, _comma(row.get("RATE5"), 6, 2))
    _write(fh, buf)

    buf = _line()
    _place(buf, 1, "P001")
    _place(buf, 95, _char(row.get("COL1"), 7))
    _place(buf, 102, _char(row.get("COL2"), 7))
    _place(buf, 109, _char(row.get("COL3"), 7))
    _place(buf, 116, _char(row.get("COL4"), 7))
    _place(buf, 123, _char(row.get("COL5"), 7))
    _write(fh, buf)


def _write_subtotal(fh, label: str, minor_value, amount: float) -> None:
    buf = _line(); _place(buf, 1, "P001"); _place(buf, 72, "-" * 15); _write(fh, buf)

    buf = _line()
    _place(buf, 1, "P001")
    prefix = f"SUBTOTAL FOR {label} "
    _place(buf, 37, prefix)
    _place(buf, 37 + len(prefix), "" if minor_value is None else str(minor_value))
    _place(buf, 72, _comma(amount, 15, 2))
    _write(fh, buf)

    buf = _line(); _place(buf, 1, "P001"); _place(buf, 72, "=" * 15); _write(fh, buf)
    buf = _line(); _place(buf, 1, "P001"); _write(fh, buf)


def _write_grandtotal(fh, amount: float) -> None:
    buf = _line(); _place(buf, 1, "P001"); _place(buf, 72, "-" * 15); _write(fh, buf)
    buf = _line(); _place(buf, 1, "P001"); _place(buf, 37, "GRAND TOTAL "); _place(buf, 72, _comma(amount, 15, 2)); _write(fh, buf)
    buf = _line(); _place(buf, 1, "P001"); _place(buf, 72, "=" * 15); _write(fh, buf)
    buf = _line(); _place(buf, 1, "P001"); _write(fh, buf)


def _write_newpage(fh, branch, pagecnt: int, first_branch: bool,
                    header_builder, bank_title: str) -> int:
    pagecnt += 1

    buf = _line(); _place(buf, 1, "E255"); _write(fh, buf)

    if first_branch:
        buf = _line()
        _place(buf, 1, "P000PBBEDPPBBEDP")
        _place(buf, 133, _branch_no(branch))
        _write(fh, buf)

    for line_text in header_builder(branch, pagecnt, bank_title):
        fh.write(line_text + "\n")

    return pagecnt


def _process_group_report(fh, records: list, minor_col: str, label: str,
                           header_builder, bank_title: str) -> None:
    """
    Shared BY-group report engine, mirrors both DATA _NULL_ steps:
      - BY BRANCH FISSPURP ACCTNO (minor_col='FISSPURP', label='FISS PURPOSE')
      - BY BRANCH SECTORCD ACCTNO (minor_col='SECTORCD', label='SECTOR')
    """
    n = len(records)
    pagecnt = 0
    linecnt = 0
    brchamt = 0.0
    bnmamt = 0.0

    for i, row in enumerate(records):
        branch = row.get("BRANCH")
        is_first_branch = (i == 0) or (records[i - 1]["BRANCH"] != branch)
        is_first_minor = is_first_branch or (records[i - 1][minor_col] != row.get(minor_col))
        is_last_minor = (i == n - 1) or (records[i + 1]["BRANCH"] != branch) or \
            (records[i + 1][minor_col] != row.get(minor_col))
        is_last_branch = (i == n - 1) or (records[i + 1]["BRANCH"] != branch)

        if is_first_branch:
            pagecnt = 0
            brchamt = 0.0
            pagecnt = _write_newpage(fh, branch, pagecnt, True, header_builder, bank_title)
            linecnt = 13

        if is_first_minor:
            bnmamt = 0.0

        balance = row.get("BALANCE") or 0.0
        brchamt += balance
        bnmamt += balance

        _write_detail(fh, row)
        linecnt += 3

        if linecnt > 55:
            pagecnt = _write_newpage(fh, branch, pagecnt, False, header_builder, bank_title)
            linecnt = 13

        if is_last_minor:
            _write_subtotal(fh, label, row.get(minor_col), bnmamt)
            linecnt += 4

        if linecnt > 55:
            pagecnt = _write_newpage(fh, branch, pagecnt, False, header_builder, bank_title)
            linecnt = 13

        if is_last_branch:
            _write_grandtotal(fh, brchamt)
            # NOTE: original SAS does not increment LINECNT after the grand
            # total block; preserved as-is.


def _write_odld_line(fh, row: dict) -> None:
    buf = _line(80)
    _place(buf, 1, _num(row.get("BRANCH"), 3))
    _place(buf, 4, _num(row.get("ACCTNO"), 10))
    _place(buf, 14, _comma(row.get("APPRLIMT"), 15, 2))
    _place(buf, 31, _comma(row.get("BALANCE"), 15, 2))
    _write(fh, buf)


# ============================================================================
# BANK CONFIGURATION
# ============================================================================
@dataclass(frozen=True)
class BankConfig:
    bank_code: str
    deposit_dir: Path
    loan_dir: Path
    deposit_cache_dir: Path
    loan_cache_dir: Path
    deposit_prefix: str
    loan_prefix: str
    bank_title: str
    output_rps_name: str
    output_text_name: str


BANKS = [
    BankConfig(
        bank_code="PBB",
        deposit_dir=INPUT_DIR_PBB_DEPOSIT,
        loan_dir=INPUT_DIR_PBB_LOAN,
        deposit_cache_dir=CACHE_DIR_PBB_DEPOSIT,
        loan_cache_dir=CACHE_DIR_PBB_LOAN,
        deposit_prefix="dp_ca",
        loan_prefix="ln_ln",
        bank_title="P U B L I C   B A N K   B E R H A D",
        output_rps_name="ODLIST_RPS.txt",
        output_text_name="ODLIST_TEXT.txt",
    ),
    BankConfig(
        bank_code="PIBB",
        deposit_dir=INPUT_DIR_PIBB_DEPOSIT,
        loan_dir=INPUT_DIR_PIBB_LOAN,
        deposit_cache_dir=CACHE_DIR_PIBB_DEPOSIT,
        loan_cache_dir=CACHE_DIR_PIBB_LOAN,
        deposit_prefix="idp_ca",
        loan_prefix="idp_ln",
        bank_title="P U B L I C   I S L A M I C  B A N K   B E R H A D",
        output_rps_name="ODLISTI_RPS.txt",
        output_text_name="ODLISTI_TEXT.txt",
    ),
]


# ============================================================================
# MAIN PROCESSING — PHASE 1 (CACHE) AND PHASE 2 (REPORT)
# ============================================================================
# ============================================================================
# PHASE 1: SAS -> PARQUET CACHE CONVERSION  (runs for every bank first)
# ============================================================================
def ensure_bank_cache(cfg: BankConfig) -> tuple:
    """Resolve latest LOAN/DEPOSIT files for a bank and ensure their Parquet
    cache is fresh. Returns (loan_cache_path, deposit_cache_path)."""
    print(f"\n=== Caching {cfg.bank_code} ===")
    print(f"  Deposit input dir : {cfg.deposit_dir}")
    print(f"  Loan input dir    : {cfg.loan_dir}")

    loan_path = get_latest_file(cfg.loan_dir, prefix=cfg.loan_prefix)
    deposit_path = get_latest_file(cfg.deposit_dir, prefix=cfg.deposit_prefix)

    loan_cache = cfg.loan_cache_dir / f"{loan_path.stem}.parquet"
    deposit_cache = cfg.deposit_cache_dir / f"{deposit_path.stem}.parquet"

    if not _cache_is_fresh(loan_path, loan_cache):
        sas_to_parquet(loan_path, loan_cache, f"{cfg.bank_code}_LOAN")
    else:
        print(f"  [{cfg.bank_code}_LOAN] Cache fresh - skipping conversion.")

    if not _cache_is_fresh(deposit_path, deposit_cache):
        sas_to_parquet(deposit_path, deposit_cache, f"{cfg.bank_code}_DEPOSIT")
    else:
        print(f"  [{cfg.bank_code}_DEPOSIT] Cache fresh - skipping conversion.")

    return loan_cache, deposit_cache


# ============================================================================
# PHASE 2: REPORT GENERATION  (runs for every bank after ALL caching is done)
# ============================================================================
def generate_bank_report(cfg: BankConfig, loan_cache: Path, deposit_cache: Path) -> None:
    print(f"\n=== Reporting {cfg.bank_code} ===")

    con = duckdb.connect(database=":memory:")

    # DATA ODRAFT; SET DEPOSIT.CURRENT; IF CURBAL<0 AND CUSTCODE NE 81;
    # BALANCE = (-1)*CURBAL;
    odraft = con.execute(f"""
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(BRANCH   AS INTEGER) AS BRANCH,
            CAST(CUSTCODE AS INTEGER) AS CUSTCODE,
            CAST(APPRLIMT AS DOUBLE)  AS APPRLIMT,
            CAST(NAME     AS VARCHAR) AS NAME,
            CAST(LIMIT1   AS DOUBLE)  AS LIMIT1,
            CAST(LIMIT2   AS DOUBLE)  AS LIMIT2,
            CAST(LIMIT3   AS DOUBLE)  AS LIMIT3,
            CAST(LIMIT4   AS DOUBLE)  AS LIMIT4,
            CAST(LIMIT5   AS DOUBLE)  AS LIMIT5,
            CAST(RATE1    AS DOUBLE)  AS RATE1,
            CAST(RATE2    AS DOUBLE)  AS RATE2,
            CAST(RATE3    AS DOUBLE)  AS RATE3,
            CAST(RATE4    AS DOUBLE)  AS RATE4,
            CAST(RATE5    AS DOUBLE)  AS RATE5,
            CAST(COL1     AS VARCHAR) AS COL1,
            CAST(COL2     AS VARCHAR) AS COL2,
            CAST(COL3     AS VARCHAR) AS COL3,
            CAST(COL4     AS VARCHAR) AS COL4,
            CAST(COL5     AS VARCHAR) AS COL5,
            CAST(STATE    AS VARCHAR) AS STATE,
            CAST(FLATRATE AS DOUBLE)  AS FLATRATE,
            (-1) * CAST(CURBAL AS DOUBLE) AS BALANCE
        FROM read_parquet('{deposit_cache}')
        WHERE CAST(CURBAL AS DOUBLE) < 0
          AND COALESCE(CAST(CUSTCODE AS INTEGER), -1) <> 81
    """).pl()

    # PROC SORT DATA=LOAN.LOAN&REPTMON&NOWK OUT=LOAN(KEEP=ACCTNO SECTORCD FISSPURP);
    # BY ACCTNO; -- sort is only needed here to prepare the merge key; the
    # explicit join below achieves the same match-merge result, so the
    # physical sort is not reproduced.
    loan = con.execute(f"""
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(SECTORCD AS VARCHAR) AS SECTORCD,
            CAST(FISSPURP AS VARCHAR) AS FISSPURP
        FROM read_parquet('{loan_cache}')
    """).pl()

    con.close()
    gc.collect()

    # DATA ODRAFT1; MERGE ODRAFT(IN=A) LOAN(IN=B); BY ACCTNO; IF A AND B;
    odraft1 = odraft.join(loan, on="ACCTNO", how="inner")

    # DATA ODRAFT2; SET ODRAFT1;
    # IF CUSTCD NOT IN ('77','78','95','96') AND
    #   (SUBSTR(SECTORCD,1,1)='5' OR SECTORCD='8310') THEN OUTPUT;
    #
    # NOTE: CUSTCD is never present in ODRAFT1 - the LOAN dataset was kept
    # down to ACCTNO/SECTORCD/FISSPURP only, and ODRAFT (from DEPOSIT) has
    # no CUSTCD field either (it has CUSTCODE, a different field). In the
    # original SAS, referencing an undefined variable makes it missing for
    # every observation, and a missing value is never IN a list of
    # non-missing values, so "CUSTCD NOT IN (...)" is always TRUE and adds
    # no real filtering. Only the SECTORCD condition actually filters rows,
    # which is what is implemented below (preserves true program behaviour).
    odraft2 = odraft1.filter(
        (pl.col("SECTORCD").str.slice(0, 1) == "5") | (pl.col("SECTORCD") == "8310")
    )

    # PROC SORT DATA=ODRAFT1; BY BRANCH FISSPURP ACCTNO;
    odraft1_sorted = odraft1.sort(["BRANCH", "FISSPURP", "ACCTNO"])
    # PROC SORT DATA=ODRAFT2; BY BRANCH SECTORCD ACCTNO;
    odraft2_sorted = odraft2.sort(["BRANCH", "SECTORCD", "ACCTNO"])

    del odraft, loan, odraft1, odraft2
    gc.collect()

    output_text_path = OUTPUT_DIR / cfg.output_text_name
    output_rps_path = OUTPUT_DIR / cfg.output_rps_name

    # DATA ODLD; SET ODRAFT1; FILE ODLSD; PUT ...
    print(f"  Writing {output_text_path.name} ...")
    with open(output_text_path, "w", encoding="latin1") as fh:
        for row in odraft1_sorted.iter_rows(named=True):
            _write_odld_line(fh, row)

    # DATA _NULL_; SET ODRAFT1; BY BRANCH FISSPURP ACCTNO; FILE ODLST; ...
    # DATA _NULL_; SET ODRAFT2; BY BRANCH SECTORCD ACCTNO; FILE ODLST MOD; ...
    print(f"  Writing {output_rps_path.name} ...")
    with open(output_rps_path, "w", encoding="latin1") as fh:
        _process_group_report(
            fh, odraft1_sorted.to_dicts(), "FISSPURP", "FISS PURPOSE",
            _header_lines_fisspurp, cfg.bank_title,
        )
        _process_group_report(
            fh, odraft2_sorted.to_dicts(), "SECTORCD", "SECTOR",
            _header_lines_sector, cfg.bank_title,
        )

    print(f"  {cfg.bank_code} results:")
    print(f"    ODRAFT1 rows (all FISS purpose): {len(odraft1_sorted):,}")
    print(f"    ODRAFT2 rows (construction/real estate, non-indi): {len(odraft2_sorted):,}")
    print(f"    Output text file : {output_text_path}")
    print(f"    Output RPS file  : {output_rps_path}")


def main() -> None:
    # --------------------------------------------------------------------
    # PHASE 1 — convert ALL source files (PBB deposit/loan, PIBB
    # deposit/loan) to Parquet cache FIRST, before any reporting starts.
    # --------------------------------------------------------------------
    print("\n########## PHASE 1: SAS -> PARQUET CACHING (ALL BANKS) ##########")
    caches = {}
    for cfg in BANKS:
        caches[cfg.bank_code] = ensure_bank_cache(cfg)

    # --------------------------------------------------------------------
    # PHASE 2 — once every file is cached, generate the report for PBB,
    # then PIBB, in the same run.
    # --------------------------------------------------------------------
    print("\n########## PHASE 2: REPORT GENERATION (PBB, then PIBB) ##########")
    for cfg in BANKS:
        loan_cache, deposit_cache = caches[cfg.bank_code]
        generate_bank_report(cfg, loan_cache, deposit_cache)

    # --------------------------------------------------------------------
    # PLACEHOLDER: STEP02/STEP04 PGM=SPLIB136 (mainframe utility, source
    # not provided). This step inserts real ASA control characters into
    # the RPS report and splits it by branch region using
    # RMDS.OPC.BANKCODE(KLREGION/BGREGION/JBREGION/SBREGION/PPREGION/
    # TTREGION) reference members.
    # --------------------------------------------------------------------
    # import subprocess
    # subprocess.run(["splib136", str(output_rps_path), ...])

    print("\nEIBMODLT complete.")


if __name__ == "__main__":
    main()
