#!/usr/bin/env python3
"""
Program : EIBMODLM.py
Purpose : Report on Accounts with Overdraft Limits
          Generates two reports:
            1. Public Bank Berhad - Accounts with OD Limits (ODPLAN 100-105)
            2. Public Islamic Bank Berhad - Accounts with CLF-i Limits (ODPLAN 106)
          NAME column is resolved by joining lm{month}{week}{year}.sas7bdat ACCTNO
          against stg_dp_limit.sas7bdat ACCTNO and taking NAME.
          Accounts with no matching NAME will show a blank NAME field.
"""

import duckdb
import polars as pl
import pandas as pd
from pathlib import Path

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Testing Path
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR / "input" / "prod"
OUTPUT_DIR = BASE_DIR / "output" / "EIBMODLM"

# # Production Path
# INPUT_DIR  = Path("/dwh")
# OUTPUT_DIR = Path("/host/mis/output/report") / "EIBMODLM"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input paths - Public Bank
INPUT_PBB_CURRENT  = get_latest_file(INPUT_DIR, "ca")
INPUT_PBB_OVERDFT  = get_latest_file(INPUT_DIR, "lm")
# INPUT_PBB_CURRENT  = get_latest_file(INPUT_DIR / "dp_ca", "ca")       # File name example - ca05226.sas7bdat
# INPUT_PBB_OVERDFT  = get_latest_file(INPUT_DIR / "dp_lm", "lm")       # File name example - lm05226.sas7bdat

# Input paths - Islamic Bank
INPUT_PIBB_CURRENT  = get_latest_file(INPUT_DIR, "ica")
INPUT_PIBB_OVERDFT  = get_latest_file(INPUT_DIR, "ilm")
# INPUT_PIBB_CURRENT  = get_latest_file(INPUT_DIR / "idp_ca", "ica")      # File name example - ica05226.sas7bdat
# INPUT_PIBB_OVERDFT  = get_latest_file(INPUT_DIR / "idp_lm", "ilm")      # File name example - ilm05226.sas7bdat

# Shared customer name lookup file (ACCTNO -> NAME)
# INPUT_CUSTNAME     = get_latest_file(INPUT_DIR, "cisr1ca")
INPUT_CUSTNAME     = BASE_DIR / "input/uat" / "stg_dp_limit.sas7bdat"   # stg_dp_limit.sas7bdat
# INPUT_CUSTNAME     = get_latest_file(INPUT_DIR / "rsd_cis", "cisr1ca")  # File name example - cisr1ca05226.sas7bdat

# Output paths
OUTPUT_PBB_REPORT  = build_output_file(OUTPUT_DIR, "PBB_ODLIMIT_REPORT").with_suffix(".txt")
OUTPUT_PIBB_REPORT = build_output_file(OUTPUT_DIR, "PIBB_ODLIMIT_REPORT").with_suffix(".txt")
# Output example: OUTPUT_PBB_REPORT -> PBB_ODLIMIT_REPORT_180526.txt
# Output example: OUTPUT_PIBB_REPORT -> PIBB_ODLIMIT_REPORT_180526.txt

# Report configuration
PAGE_SIZE = 50  # PS=50 in OPTIONS

# The subtotal block is an indivisible group of 9 lines:
#   \n (1) + dashes (1) + approved\n\n (2) + accounts\n\n (2)
#   + operative\n (1) + dashes\n\n (2) = 9 lines total.
# _write_report_file uses this to decide whether the subtotal fits inline on
# the last data page, or needs a dedicated new page (title + headers only).
SUBTOTAL_LINES = 9


# ============================================================================
# REPORT DATE (from REPTDATE module - no reptdate.parquet file is read)
# ============================================================================
reptdate_values = get_reptdate_values()
REPTDATE    = reptdate_values.reptdate
REPTYEAR    = reptdate_values.reptyear
REPTMON     = reptdate_values.reptmon
REPTDAY     = reptdate_values.reptday
NOWK        = reptdate_values.nowk
REPORT_DATE = REPTDATE.strftime('%d/%m/%y')

# ============================================================================
# INPUT FILE EXISTENCE CHECK — fail fast before any processing
# ============================================================================
_REQUIRED_INPUTS = {
    "PBB  Current Accounts" : INPUT_PBB_CURRENT,
    "PBB  Overdraft Data"   : INPUT_PBB_OVERDFT,
    "PIBB Current Accounts" : INPUT_PIBB_CURRENT,
    "PIBB Overdraft Data"   : INPUT_PIBB_OVERDFT,
    "Customer Name Lookup"  : INPUT_CUSTNAME,
}

_missing = [
    f"  [{label}] {path}"
    for label, path in _REQUIRED_INPUTS.items()
    if not path.exists()
]
if _missing:
    raise FileNotFoundError(
        "The following required input files are missing:\n" + "\n".join(_missing)
    )


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read one .sas7bdat file and return a Polars DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")

    # >>>>>>>>>> Uncomment this -> For production <<<<<<<<<<
    pandas_df = pd.read_sas(
        path,
        format="sas7bdat",
        encoding="latin1",
    )

    # # >>>>>>>>>> Uncomment this -> For testing purposes <<<<<<<<<<
    # reader = pd.read_sas(
    #     path,
    #     format="sas7bdat",
    #     encoding="latin1",
    #     chunksize=1000,
    # )
    # pandas_df = next(reader)

    pandas_df.columns = [
        str(col).upper().strip()
        for col in pandas_df.columns
    ]

    # print(f"\nDEBUG COLUMN NAMES [{path.name}]:")
    # print(pandas_df.head(10))

    return pl.from_pandas(pandas_df)


def _build_odplan_condition(odplan_filter) -> str:
    if isinstance(odplan_filter, list):
        return f"ODPLAN IN ({','.join(map(str, odplan_filter))})"
    return f"ODPLAN = {odplan_filter}"


def _safe_float(value) -> float:
    return float(value) if value is not None else 0.0


def _safe_int(value) -> int:
    return int(value) if value is not None else 0


def _safe_text(value, length) -> str:
    return str(value)[:length] if value is not None else ''


def _format_brn(value) -> str:
    """Format branch code safely as 3-digit string without decimal suffix."""
    if value is None:
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = ''.join(ch for ch in text if ch.isdigit())
    if digits:
        return digits.zfill(3)[-3:]
    return text[:3]


def _get_report_titles(is_islamic: bool) -> tuple:
    if is_islamic:
        return (
            'P U B L I C   I S L A M I C  B A N K   B E R H A D',
            'REPORT TITLE: ACCOUNTS WITH CLF-i LIMITS',
            'CLF-i',
        )
    return (
        'P U B L I C   B A N K   B E R H A D',
        'REPORT TITLE: ACCOUNTS WITH OD LIMITS',
        'OD',
    )


def _load_custname_lookup(con: duckdb.DuckDBPyConnection) -> None:
    """Load stg_dp_limit.sas7bdat and register ACCTNO -> NAME lookup into DuckDB.

    Join logic:
        if ACCTNO in stg_dp_limit == ACCTNO in overdraft data,
        then NAME from stg_dp_limit is used.
    Unmatched overdraft accounts will carry a NULL / blank NAME.
    """
    custname_df = _read_sas7bdat(INPUT_CUSTNAME)

    required = {"ACCTNO", "NAME"}
    missing  = required - set(custname_df.columns)
    if missing:
        raise ValueError(
            f"{INPUT_CUSTNAME.name} is missing required column(s): {', '.join(sorted(missing))}"
        )

    # Deduplicate on ACCTNO before registering. If stg_dp_limit contains multiple
    # NAME rows for the same ACCTNO, the LEFT JOIN in _load_overdraft_data would
    # multiply overdraft rows, breaking _ROW_NUM ordering and shifting RCNT slots.
    lookup_pd = (
        custname_df.select(["ACCTNO", "NAME"])
        .to_pandas()
        .drop_duplicates(subset=["ACCTNO"], keep="first")
        .reset_index(drop=True)
    )
    con.register('custname_lookup', lookup_pd)


def _load_current_accounts(
    con: duckdb.DuckDBPyConnection,
    current_file: Path,
    odplan_filter,
) -> pd.DataFrame:
    """Load and filter current accounts from .sas7bdat.

    SAS equivalent:
        DATA CURRENT;
            KEEP ACCTNO BALANCE CRI;
            SET  DEPOSIT.CURRENT;
            IF   DEPTYPE  IN ('D','N');
            IF   APPRLIMT GT 1;
            IF   ODPLAN IN (100,101,102,103,104,105);  -- or = 106 for PIBB
            IF   CURBAL LT 0 THEN BALANCE=(-1)*CURBAL;
            ELSE DO; BALANCE=CURBAL; CRI='CR'; END;
    """
    current_df = _read_sas7bdat(current_file)
    con.register('current_raw', current_df.to_pandas())
    odplan_condition = _build_odplan_condition(odplan_filter)
    current = con.execute(f"""
        SELECT
            ACCTNO,
            CASE
                WHEN CURBAL < 0 THEN (-1) * CURBAL
                ELSE CURBAL
            END AS BALANCE,
            CASE
                WHEN CURBAL >= 0 THEN 'CR'
                ELSE NULL
            END AS CRI
        FROM current_raw
        WHERE DEPTYPE IN ('D', 'N')
          AND APPRLIMT > 1
          AND {odplan_condition}
    """).df()
    con.register('current', current)
    return current


def _load_overdraft_data(
    con: duckdb.DuckDBPyConnection,
    overdft_file: Path,
) -> pd.DataFrame:
    """Load and filter overdraft data from .sas7bdat, enrich with NAME via left-join.

    SAS equivalent:
        DATA OVDR;
            KEEP ACCTNO BRANCH LMTBASER LMTRATE LMTAMT LMTCOLL
                 NAME APPRLIMT ODSTATUS;
            SET  ODLIMIT.OVERDFT;
            IF   APPRLIMT GT 1;
            IF   LMTTYPE IN ('Y','A');
        PROC SORT DATA=OVDR; BY ACCTNO;

        DATA OVDR2; SET OVDR; BY ACCTNO;
            IF FIRST.ACCTNO THEN RCNT=1;
            OUTPUT; RCNT+1;
            IF LAST.ACCTNO THEN RCNT=0;

        DATA OVDR1; SET OVDR2;
            IF (1<=RCNT<=5) THEN OUTPUT;

    RCNT assignment replicates SAS BY-group row numbering:
        - Resets to 1 on FIRST.ACCTNO
        - Increments by 1 after each OUTPUT
        - Resets to 0 on LAST.ACCTNO
    Only rows with RCNT between 1 and 5 (inclusive) are kept.
    Accounts whose ACCTNO does not appear in stg_dp_limit will have NAME = ''.
    """
    ovdr_df = _read_sas7bdat(overdft_file)

    # Tag each row with its original file position BEFORE any filter or sort.
    # This is critical: SAS PROC SORT is a stable sort, so rows with the same
    # ACCTNO retain their original file sequence. Without this tag, ORDER BY
    # ACCTNO in DuckDB produces an arbitrary intra-group order, causing RCNT
    # to be assigned to the wrong rows and producing wrong LIMIT/RATE/COLL slots.
    ovdr_pd = ovdr_df.to_pandas()
    ovdr_pd["_ROW_NUM"] = range(len(ovdr_pd))
    con.register('ovdr_raw', ovdr_pd)

    # Filter + join NAME, then sort by (ACCTNO, _ROW_NUM) to replicate SAS stable sort
    ovdr = con.execute("""
        SELECT
            o.ACCTNO,
            o.BRANCH,
            o.LMTBASER,
            o.LMTRATE,
            o.LMTAMT,
            o.LMTCOLL,
            o.APPRLIMT,
            o.ODSTATUS,
            o._ROW_NUM,
            COALESCE(c.NAME, '') AS NAME
        FROM ovdr_raw o
        LEFT JOIN custname_lookup c
            ON o.ACCTNO = c.ACCTNO
        WHERE o.APPRLIMT > 1
          AND o.LMTTYPE IN ('Y', 'A')
        ORDER BY o.ACCTNO, o._ROW_NUM
    """).df()

    # SAS BY-group RCNT on stable-sorted data: resets to 1 on FIRST.ACCTNO,
    # increments after each OUTPUT. sort=False preserves ORDER BY applied above.
    ovdr["RCNT"] = ovdr.groupby("ACCTNO", sort=False).cumcount() + 1
    ovdr = ovdr[ovdr["RCNT"] <= 5].drop(columns=["_ROW_NUM"]).reset_index(drop=True)

    con.register('ovdr', ovdr)
    return ovdr


def _extract_limit_slot(ovdr: pd.DataFrame, n: int) -> pd.DataFrame:
    """Extract rows for a single RCNT slot and rename to LIMITn/RATEn/COLLn.

    SAS equivalent (e.g. for n=2):
        DATA ODS2; SET OVDR1;
            KEEP ACCTNO BRANCH LMTBASER NAME ODSTATUS APPRLIMT LIMIT2 RATE2 COLL2;
            IF RCNT=2 THEN DO;
                LIMIT2=LMTAMT; RATE2=LMTRATE; COLL2=LMTCOLL;
                OUTPUT;
            END;

    Only rows where RCNT equals n are selected, then LMTAMT/LMTRATE/LMTCOLL are
    renamed to LIMITn/RATEn/COLLn. This explicit per-slot filter is the correct
    approach — using pivot_table(aggfunc='first') is unreliable because it picks
    by DataFrame index order, not by RCNT value, which causes value swaps when
    the index does not align perfectly with RCNT ordering.
    """
    slot = ovdr.loc[ovdr["RCNT"] == n, ["ACCTNO", "LMTAMT", "LMTRATE", "LMTCOLL"]].copy()
    slot = slot.rename(columns={
        "LMTAMT":  f"LIMIT{n}",
        "LMTRATE": f"RATE{n}",
        "LMTCOLL": f"COLL{n}",
    })
    return slot.reset_index(drop=True)


def _build_odmerg(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Replicate SAS ODS1..ODS5 + ODMERG pattern exactly.

    SAS equivalent:
        DATA ODS1; SET OVDR1; IF RCNT=1 THEN DO; LIMIT1=...; RATE1=...; COLL1=...; OUTPUT; END;
        ...
        DATA ODMERG; MERGE ODS1 ODS2 ODS3 ODS4 ODS5; BY ACCTNO;

    Each ODSn subset contains exactly the rows where RCNT=n, renamed to LIMITn/RATEn/COLLn.
    The MERGE BY ACCTNO collapses these into one row per ACCTNO.

    Metadata (BRANCH, LMTBASER, NAME, ODSTATUS, APPRLIMT) is sourced from the LAST
    RCNT row per account. In SAS, MERGE ODS1..ODS5 BY ACCTNO overwrites non-slot
    columns with each contributing dataset's values in sequence — the last dataset
    that contributes a non-missing value wins. Since ODS1..ODS5 are processed in
    RCNT order, the highest RCNT present per account supplies the final values for
    metadata columns such as LMTBASER, BRANCH, ODSTATUS, and APPRLIMT.
    """
    ovdr = con.execute("SELECT * FROM ovdr").df()

    # Metadata from the LAST RCNT row per account (mirrors SAS MERGE last-value behaviour)
    last_rcnt_idx = ovdr.groupby("ACCTNO", sort=False)["RCNT"].idxmax()
    meta = (
        ovdr.loc[
            last_rcnt_idx,
            ["ACCTNO", "BRANCH", "LMTBASER", "NAME", "ODSTATUS", "APPRLIMT"]
        ]
        .copy()
        .reset_index(drop=True)
    )

    # Build ODS1..ODS5 via explicit slot extraction, then merge onto metadata
    odmerg = meta
    for n in range(1, 6):
        slot_df = _extract_limit_slot(ovdr, n)
        odmerg  = odmerg.merge(slot_df, on="ACCTNO", how="left")

    con.register("odmerg", odmerg)
    return odmerg


def _merge_current_with_overdraft(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Merge CURRENT (IN=A) with ODMERG and null-coalesce limit/rate fields.

    SAS equivalent:
        DATA OVDRM;
            MERGE CURRENT(IN=A) ODMERG; BY ACCTNO;
            IF A;
            IF LIMIT2=. THEN LIMIT2=0;
            ...
            NOACCT=1;
            IF FIRST.ACCTNO THEN LIMITS=0;
            LIMITS=SUM(LIMIT1,LIMIT2,LIMIT3,LIMIT4,LIMIT5);
    """
    ovdrm = con.execute("""
        SELECT
            c.ACCTNO,
            c.BALANCE,
            c.CRI,

            o.BRANCH,
            o.LMTBASER,
            o.NAME,
            o.ODSTATUS,
            o.APPRLIMT,

            -- LIMIT 1 (always present for matched accounts)
            COALESCE(o.LIMIT1, 0)   AS LIMIT1,
            COALESCE(o.RATE1,  0.0) AS RATE1,
            COALESCE(o.COLL1,  '')  AS COLL1,

            -- LIMIT 2
            COALESCE(o.LIMIT2, 0)   AS LIMIT2,
            COALESCE(o.RATE2,  0.0) AS RATE2,
            COALESCE(o.COLL2,  '')  AS COLL2,

            -- LIMIT 3
            COALESCE(o.LIMIT3, 0)   AS LIMIT3,
            COALESCE(o.RATE3,  0.0) AS RATE3,
            COALESCE(o.COLL3,  '')  AS COLL3,

            -- LIMIT 4
            COALESCE(o.LIMIT4, 0)   AS LIMIT4,
            COALESCE(o.RATE4,  0.0) AS RATE4,
            COALESCE(o.COLL4,  '')  AS COLL4,

            -- LIMIT 5
            COALESCE(o.LIMIT5, 0)   AS LIMIT5,
            COALESCE(o.RATE5,  0.0) AS RATE5,
            COALESCE(o.COLL5,  '')  AS COLL5,

            -- SAS: LIMITS = SUM(LIMIT1,...,LIMIT5)  (SAS SUM treats NULL as 0)
            (
                COALESCE(o.LIMIT1, 0) +
                COALESCE(o.LIMIT2, 0) +
                COALESCE(o.LIMIT3, 0) +
                COALESCE(o.LIMIT4, 0) +
                COALESCE(o.LIMIT5, 0)
            ) AS LIMITS,

            1 AS NOACCT

        FROM current c
        INNER JOIN odmerg o
            ON c.ACCTNO = o.ACCTNO
    """).df()

    con.register('ovdrm', ovdrm)
    return ovdrm


def _format_branch_codes(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Format BRANCH as zero-padded 3-character BRN string, then sort by BRN, ACCTNO.

    SAS equivalent:
        DATA BRNREF; SET OVDRM; BY BRANCH;
            LENGTH BRH1 $1. BRH2 $2. BRH3 BRN $3.;
            SELECT;
                WHEN (BRANCH < 10)       BRN='00'||TRIM(BRH1);
                WHEN (9 < BRANCH < 100)  BRN='0'||TRIM(BRH2);
                WHEN (BRANCH > 99)       BRN=TRIM(BRH3);
            END;
        PROC SORT DATA=BRNREF; BY BRN ACCTNO;
    """
    return con.execute("""
        SELECT *,
            CASE
                WHEN BRANCH < 10  THEN '00' || CAST(CAST(BRANCH AS INTEGER) AS VARCHAR)
                WHEN BRANCH < 100 THEN '0'  || CAST(CAST(BRANCH AS INTEGER) AS VARCHAR)
                ELSE CAST(CAST(BRANCH AS INTEGER) AS VARCHAR)
            END AS BRN
        FROM ovdrm
        ORDER BY BRN, ACCTNO
    """).df()


def _write_branch_subtotal(
    report_file,
    branch_total_limit: float,
    branch_account_count: int,
    branch_total_operative: float,
) -> None:
    """Write branch-level totals block (9 lines, always written as one indivisible unit).

    SAS equivalent (COMPUTE AFTER BRN):
        LINE @26 49*'-';
        LINE @26 'TOTAL APPROVED LIMITS  =' @55 APPRLIMT.SUM COMMA20.2;
        LINE @26 'TOTAL ACCOUNTS         =' @69 NOACCT.SUM 6.;
        LINE @26 'TOTAL OPERATIVE LIMITS =' @55 LIMITS.SUM COMMA20.2;
        LINE @26 49*'-';

    Line count breakdown (matches SUBTOTAL_LINES = 9):
        blank line          -> 1
        dashes line         -> 1
        approved + blank    -> 2
        accounts + blank    -> 2
        operative           -> 1
        dashes + blank      -> 2
    """
    label_width = 26
    value_width = 22

    report_file.write("\n")

    subtotal_line = " " * 26 + "-" * 49
    report_file.write(subtotal_line + "\n")

    report_file.write(
        f"{' ' * 26}{'TOTAL APPROVED LIMITS  =':<{label_width}} "
        f"{branch_total_limit:>{value_width},.2f}\n\n"
    )

    report_file.write(
        f"{' ' * 26}{'TOTAL ACCOUNTS         =':<{label_width}} "
        f"{branch_account_count:>{value_width},}\n\n"
    )

    report_file.write(
        f"{' ' * 26}{'TOTAL OPERATIVE LIMITS =':<{label_width}} "
        f"{branch_total_operative:>{value_width},.2f}\n"
    )

    report_file.write(subtotal_line + "\n\n")


def _build_title_lines(
    title1: str,
    title2: str,
    report_date: str,
    branch_code: str,
) -> list:
    return [
        f"1  {title1}\n",
        f"   {title2}\n",
        f"   REPORT AS AT {report_date}\n",
        "\n",
        "\n",
        f" BRN={_format_brn(branch_code)}\n",
        "\n",
    ]


def _build_primary_header_lines(od_label: str) -> list:
    header_line_1 = (
        f"{'':<44}"
        f"{'BASE':>5}"
        f"  {od_label:<5}"
        f"{'OUTSTANDING':>14}"
        f"{'APPROVED':>18}"
    )
    header_line_2 = (
        f"{'BRN':<5}"
        f"{'ACCOUNT NO':<12}"
        f"{'NAME OF CUSTOMER':<27}"
        f"{'RATE':>5}"
        f"  {'ST':<5}"
        f"{'BALANCE':>14}"
        f"{'LIMIT':>18}"
        f"{'LIMIT1':>14}"
        f"{'RATE1':>7}"
        f"{'COLL1':>7}"
        f"{'LIMIT2':>14}"
    )
    return [
        f"   {header_line_1}\n",
        f"   {header_line_2}\n",
        f"   {'-' * len(header_line_2)}\n",
    ]


def _build_secondary_header_lines() -> list:
    return [
        (
            f"\n   "
            f"{'RATE2':>5}{'COLL2':>7}{'LIMIT3':>14}{'RATE3':>7}{'COLL3':>7}"
            f"{'LIMIT4':>14}{'RATE4':>7}{'COLL4':>7}{'LIMIT5':>14}{'RATE5':>7}{'COLL5':>7}\n"
        ),
        f"   {'-' * 96}\n",
    ]


def _write_page(
    report_file,
    title_lines: list,
    header_lines: list,
    data_lines: list,
    add_form_feed: bool,
) -> bool:
    page_lines = title_lines + header_lines + data_lines

    if len(page_lines) > PAGE_SIZE:
        raise ValueError(
            f"PAGE_SIZE={PAGE_SIZE} exceeded: page has {len(page_lines)} lines."
        )

    if add_form_feed and page_lines:
        report_file.write("\f" + page_lines[0])
        remaining_lines = page_lines[1:]
    else:
        remaining_lines = page_lines

    for line in remaining_lines:
        report_file.write(line)

    return True


def _build_detail_line(row, show_brn: bool = True) -> str:
    """Build primary detail line for one account row."""
    brn_value = _format_brn(row['BRN']) if show_brn else ""

    return (
        f"   {brn_value:<5}"
        f"{_safe_int(row['ACCTNO']):<12}"
        f"{_safe_text(row['NAME'], 24):<25}"
        f"{_safe_float(row['LMTBASER']):>7.2f}"
        f"  {_safe_text(row['ODSTATUS'], 2):<5}"
        f"{_safe_float(row['BALANCE']):>14,.2f}"
        f"  {_safe_text(row['CRI'], 2):<2}"
        f"{_safe_float(row['APPRLIMT']):>14,.2f}"
        f"{_safe_float(row['LIMIT1']):>14,.2f}"
        f"{_safe_float(row['RATE1']):>7.2f}"
        f"{_safe_text(row['COLL1'], 5):>7}"
        f"{_safe_float(row['LIMIT2']):>14,.2f}\n"
    )


def _build_secondary_line(row) -> str:
    """Build secondary detail line (RATE2..COLL5) for one account row."""
    return (
        f"   {_safe_float(row['RATE2']):>5.2f}"
        f"{_safe_text(row['COLL2'], 5):>7}"
        f"{_safe_float(row['LIMIT3']):>14,.2f}"
        f"{_safe_float(row['RATE3']):>7.2f}"
        f"{_safe_text(row['COLL3'], 5):>7}"
        f"{_safe_float(row['LIMIT4']):>14,.2f}"
        f"{_safe_float(row['RATE4']):>7.2f}"
        f"{_safe_text(row['COLL4'], 5):>7}"
        f"{_safe_float(row['LIMIT5']):>14,.2f}"
        f"{_safe_float(row['RATE5']):>7.2f}"
        f"{_safe_text(row['COLL5'], 5):>7}\n"
    )


def _write_report_file(
    brnref: pd.DataFrame,
    output_file: Path,
    is_islamic: bool,
    report_date: str,
) -> None:
    title1, title2, od_label = _get_report_titles(is_islamic)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as report_file:
        add_form_feed = False

        for brn_code, branch_rows in brnref.groupby('BRN', sort=False):

            primary_header_lines   = _build_primary_header_lines(od_label)
            secondary_header_lines = _build_secondary_header_lines()

            rows       = list(branch_rows.iterrows())
            row_idx    = 0
            total_rows = len(rows)

            subtotal_args = (
                float(branch_rows['APPRLIMT'].sum()),
                int(len(branch_rows)),
                float(branch_rows['LIMITS'].sum()),
            )

            while row_idx < total_rows:

                title_lines = _build_title_lines(
                    title1, title2, report_date, brn_code
                )

                fixed_primary   = len(title_lines) + len(primary_header_lines)
                fixed_secondary = len(title_lines) + len(secondary_header_lines)
                fixed_lines     = max(fixed_primary, fixed_secondary)

                max_data_rows = PAGE_SIZE - fixed_lines

                if max_data_rows <= 0:
                    raise ValueError(
                        f"PAGE_SIZE={PAGE_SIZE} too small for report title/header blocks."
                    )

                # Always fill the page to its natural capacity.
                # Never trim rows off the bottom to make room for the subtotal —
                # the subtotal placement decision is made AFTER writing the data.
                rows_this_chunk = min(total_rows - row_idx, max_data_rows)
                chunk           = rows[row_idx: row_idx + rows_this_chunk]
                is_last_chunk   = (row_idx + rows_this_chunk) >= total_rows

                # ── PRIMARY TABLE ────────────────────────────────────────────
                primary_data_lines = [
                    _build_detail_line(row, show_brn=(idx == 0))
                    for idx, (_, row) in enumerate(chunk)
                ]

                add_form_feed = _write_page(
                    report_file,
                    title_lines,
                    primary_header_lines,
                    primary_data_lines,
                    add_form_feed,
                )

                if is_last_chunk:
                    # After writing all data rows, check remaining space on this
                    # page. If the subtotal block fits, write it here. Otherwise,
                    # open a new page (title + primary header, no data) and write
                    # it there — the data rows already printed stay where they are.
                    lines_used = fixed_primary + rows_this_chunk
                    if (PAGE_SIZE - lines_used) >= SUBTOTAL_LINES:
                        _write_branch_subtotal(report_file, *subtotal_args)
                    else:
                        add_form_feed = _write_page(
                            report_file, title_lines, primary_header_lines, [], add_form_feed,
                        )
                        _write_branch_subtotal(report_file, *subtotal_args)

                # ── SECONDARY TABLE ──────────────────────────────────────────
                secondary_data_lines = [
                    _build_secondary_line(row)
                    for _, row in chunk
                ]

                add_form_feed = _write_page(
                    report_file,
                    title_lines,
                    secondary_header_lines,
                    secondary_data_lines,
                    add_form_feed,
                )

                if is_last_chunk:
                    lines_used = fixed_secondary + rows_this_chunk
                    if (PAGE_SIZE - lines_used) >= SUBTOTAL_LINES:
                        _write_branch_subtotal(report_file, *subtotal_args)
                    else:
                        add_form_feed = _write_page(
                            report_file, title_lines, secondary_header_lines, [], add_form_feed,
                        )
                        _write_branch_subtotal(report_file, *subtotal_args)

                row_idx += rows_this_chunk


def generate_od_report(
    current_file: Path,
    overdft_file: Path,
    output_file: Path,
    is_islamic: bool = False,
    odplan_filter=None,
) -> bool:
    """
    Generate overdraft limit report with NAME resolved from stg_dp_limit.sas7bdat.

    NAME resolution logic:
        if ACCTNO in overdraft file == ACCTNO in stg_dp_limit.sas7bdat
        then NAME = NAME from stg_dp_limit.sas7bdat
        else NAME = '' (blank)

    Args:
        current_file:   Path to current accounts .sas7bdat file
        overdft_file:   Path to overdraft .sas7bdat file
        output_file:    Path to output report .txt file
        is_islamic:     Boolean indicating if this is Islamic bank report
        odplan_filter:  List of ODPLAN codes or single value

    Returns:
        True if the report was generated successfully, False otherwise.
    """
    print(f"\n{'=' * 70}")
    print(f"Generating {'Islamic Bank CLF-i' if is_islamic else 'Public Bank OD'} Limits Report")
    print(f"{'=' * 70}")
    print(f"\nReport Date: {REPORT_DATE}")

    # A fresh in-memory DuckDB connection is created for every report run
    # so that registered tables from the previous run cannot bleed through.
    con = duckdb.connect(database=':memory:')

    try:
        print("\nStep 1: Loading customer name lookup (stg_dp_limit)...")
        _load_custname_lookup(con)
        print("Customer name lookup registered.")

        print("\nStep 2: Processing current accounts...")
        current = _load_current_accounts(con, current_file, odplan_filter)
        print(f"Current accounts: {len(current):,}")

        print("\nStep 3: Processing overdraft data (with NAME join)...")
        ovdr = _load_overdraft_data(con, overdft_file)
        print(f"Overdraft records: {len(ovdr):,}")
        matched = (ovdr['NAME'] != '').sum()
        print(f"  NAME matched from stg_dp_limit : {matched:,} / {len(ovdr):,}")

        print("\nStep 4: Building limit slots (ODS1..ODS5 -> ODMERG)...")
        odmerg = _build_odmerg(con)
        print(f"Accounts with limit slots built: {len(odmerg):,}")

        print("\nStep 5: Merging current accounts with overdraft data...")
        ovdrm = _merge_current_with_overdraft(con)
        print(f"Merged records: {len(ovdrm):,}")

        print("\nStep 6: Formatting branch codes...")
        brnref = _format_branch_codes(con)
        print(f"Final records with branch codes: {len(brnref):,}")

        print("\nStep 7: Generating report...")
        _write_report_file(brnref, output_file, is_islamic, REPORT_DATE)
        print(f"Report saved: {output_file}")

        print("\nReport Statistics:")
        print(f"  Total Accounts : {len(brnref):,}")
        print(f"  Total Branches : {brnref['BRN'].nunique()}")
        if len(brnref) > 0:
            print(f"  Total Approved Limits  : {brnref['APPRLIMT'].sum():,.2f}")
            print(f"  Total Operative Limits : {brnref['LIMITS'].sum():,.2f}")

        print(f"\n========== PREVIEW: {output_file.name} ==========\n")
        with open(output_file, 'r', encoding='utf-8') as f:
            print(f.read())
        print(f"========== END PREVIEW ==========\n")

        return True

    except Exception as e:
        print(f"\n[ERROR] Report generation failed for {output_file.name}: {type(e).__name__}: {e}")
        return False

    finally:
        con.close()


# ============================================================================
# MAIN EXECUTION
# ============================================================================

print("=" * 70)
print("OVERDRAFT LIMITS REPORT GENERATION")
print("=" * 70)

results = {}

# ============================================================================
# PART 1: PUBLIC BANK - OD LIMITS (ODPLAN 100-105)
# ============================================================================

results["PBB"] = generate_od_report(
    current_file=INPUT_PBB_CURRENT,
    overdft_file=INPUT_PBB_OVERDFT,
    output_file=OUTPUT_PBB_REPORT,
    is_islamic=False,
    odplan_filter=[100, 101, 102, 103, 104, 105],
)

# ============================================================================
# PART 2: PUBLIC ISLAMIC BANK - CLF-i LIMITS (ODPLAN 106)
# ============================================================================

results["PIBB"] = generate_od_report(
    current_file=INPUT_PIBB_CURRENT,
    overdft_file=INPUT_PIBB_OVERDFT,
    output_file=OUTPUT_PIBB_REPORT,
    is_islamic=True,
    odplan_filter=106,
)

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("GENERATED REPORTS:")
print("=" * 70)

if results["PBB"]:
    print(f"  1. Public Bank OD Limits     : {OUTPUT_PBB_REPORT}")
else:
    print(f"  1. Public Bank OD Limits     : [FAILED]")

if results["PIBB"]:
    print(f"  2. Islamic Bank CLF-i Limits : {OUTPUT_PIBB_REPORT}")
else:
    print(f"  2. Islamic Bank CLF-i Limits : [FAILED]")

if all(results.values()):
    print("\nREPORT GENERATION COMPLETE")
else:
    print("\nREPORT GENERATION COMPLETED WITH ERRORS — review output above.")
