#!/usr/bin/env python3
"""
Program  : EIBWEPC1.py
Purpose  : CHEQUES ISSUED BY THE BANK - PUBLIC BANK BERHAD (PBB), WEEKLY
           (ESMR: 2013-181)
           Produces four tabular reports:
             1. Total cheques issued (number + value)
             2. All payments by number of cheques
             3. All payments by value of cheques
             4. All payments by branch (COSTCTR x PURPOSE cross-tab)

Notes
-----
* LOAN.REPTDATE no longer exists as a physical dataset/parquet file. The
  original SAS only used LOAN.REPTDATE to derive REPTDATE/PREVDATE macro
  variables (it is never used for any other data), so it is fully replaced
  here by REPTDATE.py.
* This job is scheduled to run only on the 8th, 15th, 22nd, and the last
  calendar day of the month (weekly cadence). The original SAS SELECT/WHEN
  branches on DAY(REPTDATE) = 8, 15, 22, OTHERWISE, mapping PREVDATE to the
  1st, 9th, 16th, and 23rd of the month respectively. Because the job only
  ever runs on those four days, this is exactly equivalent to
  REPTDATE.py's NOWK day-of-month bucket (1-8 / 9-15 / 16-22 / 23-31), so
  NOWK is used to derive PREVDATE below.
* Per project convention, all sas7bdat-origin inputs are assumed already
  converted to Parquet with columns matching the SAS column spec. DPLD.DPLD
  &REPTMON therefore maps to dpld<REPTMON>.parquet.
* DPLD's own column named REPTDATE (used in `IF (&PREVDT<=REPTDATE<=
  &REPTDT)`) is a field belonging to the DPLD record layout itself (the
  batch/report date carried on each disbursement record) -- it is a
  different value than the macro variable REPTDATE derived above, which
  merely happens to share the name. This is assumed present in the DPLD
  Parquet schema.
* LNLD (RBP2.B033.LN.BNM.BNKCHEQ.RPT) is a raw mainframe fixed-width flat
  file, kept as .txt and read via byte-offset slicing in chunks, never via
  read_parquet()/read_csv() -- same record layout as the quarterly variant.
* OPTIONS MISSING=0 causes SAS to print '0' instead of blank/'.' for
  missing numeric values; this is reflected in the Report 4 cross-tab where
  a COSTCTR/PURPOSE combination with no observations prints 0 / 0.00
  instead of being left blank.
"""

# ============================================================================
# IMPORTS
# ============================================================================
import os
import re
import textwrap
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import polars as pl

from REPTDATE import get_reptdate_values
from output_date import build_output_file

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = Path("/stgsrcsys/host/uat")
OUTPUT_DIR = BASE_DIR / "output" / "EIBWEPC1"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Chunked reading controls for the large LNLD flat file.
CHUNK_SIZE = 500_000
ROW_LIMIT = int(os.environ.get("ROW_LIMIT", "0")) or None  # test-mode cutoff

PAGE_LENGTH = 60  # ASA report page length (not specified in JCL -> default 60)

LRECL = 133  # SASLIST DD DCB LRECL=133 (see JCL)


def _pad_line(line: str, width: int = LRECL) -> str:
    return line[:width].ljust(width)


def _finalize_page(lines: list[str], page_no: int, width: int = LRECL) -> list[str]:
    """Pad every line to LRECL; print the automatic SAS page number
    right-justified on TITLE1 (page numbers print by default since
    OPTIONS did not specify NONUMBER)."""
    suffix = f"{page_no} "
    title = lines[0][: width - len(suffix)].ljust(width - len(suffix)) + suffix
    out = [title]
    out += [_pad_line(l, width) for l in lines[1:]]
    return out

# ============================================================================
# DATA REPTDATE STEP EQUIVALENT
# (Original: SET LOAN.REPTDATE; SELECT(DAY(REPTDATE)) ... -> PREVDATE)
# No reptdate.parquet exists; base REPTDATE comes from REPTDATE.py.
# ============================================================================
# Actual Configuration
_reptdate_values = get_reptdate_values(year_format="%Y")  # CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.))

# Testing configuration
# _reptdate_values = get_reptdate_values(run_date=date(2026, 7, 1), year_format="%Y")

REPTDATE_VAL: date = _reptdate_values.reptdate

# SELECT(DAY(REPTDATE)):
#   WHEN(08) PREVDATE = 1st of month   WHEN(15) PREVDATE = 9th of month
#   WHEN(22) PREVDATE = 16th of month  OTHERWISE PREVDATE = 23rd of month
# Equivalent via REPTDATE.py's NOWK day-of-month bucket (see module docstring
# note above for why this equivalence holds).
_NOWK_START_DAY = {"1": 1, "2": 9, "3": 16, "4": 23}
PREVDATE: date = REPTDATE_VAL.replace(day=_NOWK_START_DAY[_reptdate_values.nowk])

REPTYEAR = _reptdate_values.reptyear              # PUT(REPTDATE,YEAR4.)
REPTMON = _reptdate_values.reptmon                # PUT(MONTH(REPTDATE),Z2.)
REPTDAY = _reptdate_values.reptday                # PUT(DAY(REPTDATE),Z2.)
RDATE = REPTDATE_VAL.strftime("%d/%m/%Y")         # PUT(REPTDATE,DDMMYY10.)
REPTDT: date = REPTDATE_VAL                       # CALL SYMPUT('REPTDT',REPTDATE)
PREVDT: date = PREVDATE                           # CALL SYMPUT('PREVDT',PREVDATE)

# ============================================================================
# FORMAT DICTIONARIES  (PROC FORMAT equivalents)
# ============================================================================
DESC_FMT = {1: "CHEQUES ISSUED"}

TCODE_FMT = {
    310: "LOAN DISBURSEMENT",
    750: "PRINCIPAL INCREASE (PROGRESSIVE LOAN RELEASE)",
    752: "DEBITING FOR INSURANCE PREMIUM",
    753: "DEBITING FOR LEGAL FEE",
    754: "DEBITING FOR OTHER PAYMENTS",
    760: "MANUAL FEE ASSESSMENT FOR PAYMENT TO 3RD PARTY",
}

FEEFMT = {
    "QR": "QUIT RENT",
    "LF": "LEGAL FEE & DISBURSEMENT",
    "VA": "VALUATION FEE",
    "IP": "INSURANCE PREMIUM",
    "PA": "PROFESSIONAL/OTHERS",
    "AC": "ADVERTISEMENT FEE",
    "MC": "MAINTENANCE CHARGES",
    "RE": "REPOSSESION CHARGES",
    "RI": "REPAIR CHARGES",
    "SC": "STORAGE CHARGES",
    "SF": "SEARCH FEE",
    "TC": "TOWING CHARGES",
    "99": "MISCHELLANEOUS EXPENSES",
}

# ============================================================================
# FIELD PARSING HELPERS (fixed-width mainframe INPUT equivalents)
# ============================================================================
def _parse_int(raw: str) -> Optional[int]:
    s = raw.strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _parse_implied_decimal(raw: str, decimals: int) -> Optional[float]:
    """SAS w.d informat: divide by 10**d only when no explicit decimal point."""
    s = raw.strip()
    if not s:
        return None
    negative = s.startswith("-")
    if negative:
        s = s[1:]
    try:
        if "." in s:
            val = float(s)
        else:
            val = int(s) / (10 ** decimals)
    except ValueError:
        return None
    return -val if negative else val


def _parse_ddmmyy8(raw: str) -> Optional[date]:
    """DDMMYY8. informat, 8-digit field: DD(2)+MM(2)+YYYY(4), no delimiters.
    Confirmed against actual LN_BNM_BNKCHEQ_RPT.TXT sample data
    (e.g. '30062026' -> 30/06/2026). YEARCUTOFF=1930 does not apply here
    since the year is explicit 4-digit, not an ambiguous 2-digit value.
    """
    s = raw.strip()
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    if len(digits) != 8:
        return None
    dd, mm, yyyy = int(digits[0:2]), int(digits[2:4]), int(digits[4:8])
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None


# ============================================================================
# DATA BNM.DPLD
# SET DPLD.DPLD&REPTMON; IF (&PREVDT<=REPTDATE<=&REPTDT);
# DPLD.DPLD&REPTMON -> dpld<REPTMON>.sas7bdat (raw mainframe SAS dataset).
# The filter is applied on DPLD's own REPTDATE column (see module docstring).
# ============================================================================
DPLD_PATH = INPUT_DIR / f"dpld{REPTMON}.sas7bdat"


def _load_dpld() -> pl.DataFrame:
    pdf = pd.read_sas(DPLD_PATH, encoding="latin1")
    df = pl.from_pandas(pdf)
    return df.with_columns(
        [
            pl.col("ACCTNO").cast(pl.Int64),
            (pl.date(1960, 1, 1) + pl.duration(days=pl.col("TRANDT").cast(pl.Int64))).alias("TRANDT"),
            pl.col("TRANAMT").cast(pl.Float64).round(2),
            (pl.date(1960, 1, 1) + pl.duration(days=pl.col("REPTDATE").cast(pl.Int64))).alias("REPTDATE"),
        ]
    )


dpld_raw = _load_dpld()

dpld = dpld_raw.filter(
    (pl.col("REPTDATE") >= PREVDT) & (pl.col("REPTDATE") <= REPTDT)
)

# ============================================================================
# DATA BNM.LNLD
# INFILE LNLD MISSOVER; fixed-width mainframe flat file, read in chunks with
# byte-offset slicing (1-based SAS @col -> 0-based Python slice).
#
#   @001 ACCTNO    11.            -> [0:11]
#   @013 NOTENO     5.            -> [12:17]
#   @019 COSTCTR    7.            -> [18:25]
#   @027 NOTETYPE   3.            -> [26:29]
#   @031 TRANDT     DDMMYY8.      -> [30:38]
#   @047 TRANCODE   3.            -> [46:49]
#   @051 SEQNO      3.            -> [50:53]
#   @055 FEEPLAN    $2.  (TRANCODE=760 only) -> [54:56]
#   @057 FEENO      3.   (TRANCODE=760 only) -> [56:59]
#   @061 TRANAMT    18.2          -> [60:78]
#   @080 SOURCE     3.            -> [79:82]
#
# Subsetting IF (applied during the INPUT step, before PROC SORT):
#   (COSTCTR < 3000 OR COSTCTR > 3999) AND COSTCTR NOT IN (4043,4048)
# ============================================================================
_LNLD_SCHEMA = {
    "ACCTNO": pl.Int64,
    "NOTENO": pl.Int64,
    "COSTCTR": pl.Int64,
    "NOTETYPE": pl.Int64,
    "TRANDT": pl.Date,
    "TRANCODE": pl.Int64,
    "SEQNO": pl.Int64,
    "FEEPLAN": pl.Utf8,
    "FEENO": pl.Int64,
    "TRANAMT": pl.Float64,
    "SOURCE": pl.Int64,
}


def _load_lnld_fixed_width(path: Path) -> pl.DataFrame:
    records: list[dict] = []
    chunks: list[pl.DataFrame] = []
    total_read = 0

    with open(path, "r", encoding="latin1", errors="replace") as fh:
        for line in fh:
            raw = line.rstrip("\r\n")
            if not raw.strip():
                continue

            raw = raw.ljust(90)  # MISSOVER: short records treated as blank-padded

            acctno = _parse_int(raw[0:11])
            noteno = _parse_int(raw[12:17])
            costctr = _parse_int(raw[18:25])
            notetype = _parse_int(raw[26:29])
            trandt = _parse_ddmmyy8(raw[30:38])
            trancode = _parse_int(raw[46:49])
            seqno = _parse_int(raw[50:53])

            feeplan = None
            feeno = None
            if trancode == 760:
                feeplan = raw[54:56].strip() or None
                feeno = _parse_int(raw[56:59])

            tranamt = _parse_implied_decimal(raw[60:78], decimals=2)
            source = _parse_int(raw[79:82])

            # SAS missing numeric values sort lower than any real number, so a
            # missing COSTCTR satisfies "COSTCTR < 3000".
            cond_range = costctr is None or costctr < 3000 or costctr > 3999
            cond_excl = costctr not in (4043, 4048)
            if not (cond_range and cond_excl):
                continue

            records.append(
                {
                    "ACCTNO": acctno,
                    "NOTENO": noteno,
                    "COSTCTR": costctr,
                    "NOTETYPE": notetype,
                    "TRANDT": trandt,
                    "TRANCODE": trancode,
                    "SEQNO": seqno,
                    "FEEPLAN": feeplan,
                    "FEENO": feeno,
                    "TRANAMT": tranamt,
                    "SOURCE": source,
                }
            )

            total_read += 1
            if len(records) >= CHUNK_SIZE:
                chunks.append(pl.DataFrame(records, schema=_LNLD_SCHEMA))
                records = []

            if ROW_LIMIT is not None and total_read >= ROW_LIMIT:
                break

    if records:
        chunks.append(pl.DataFrame(records, schema=_LNLD_SCHEMA))

    if not chunks:
        return pl.DataFrame(schema=_LNLD_SCHEMA)

    return pl.concat(chunks, how="diagonal_relaxed")


# LNLD (RBP2.B033.LN.BNM.BNKCHEQ.RPT) -- weekly, non-generational flat file
LNLD_PATH = INPUT_DIR / "LN_BNM_BNKCHEQ_RPT.TXT"
lnld = _load_lnld_fixed_width(LNLD_PATH)

lnld = lnld.with_columns(
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANDT").cast(pl.Date),
    pl.col("TRANAMT").cast(pl.Float64).round(2),
)

# ============================================================================
# PROC SORT + DATA BNM.TRANX
# MERGE LNLD(IN=A) DPLD(IN=B); BY ACCTNO TRANDT TRANAMT; IF A & B;
# Equivalent to an inner join on the BY keys; the PROC SORTs are dropped
# since polars' join does not require pre-sorted inputs.
# ============================================================================
lnld = lnld.with_columns(
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANDT").cast(pl.Date),
    pl.col("TRANAMT").cast(pl.Float64).round(2),
)

print("DPLD rows :", dpld.height)
print("LNLD rows :", lnld.height)
print("DPLD REPTDATE range (pre-filter):", dpld_raw["REPTDATE"].min(), "to", dpld_raw["REPTDATE"].max())
print("Filter window:", PREVDT, "to", REPTDT)

print(
    "DPLD duplicate keys :",
    dpld.group_by(["ACCTNO", "TRANDT", "TRANAMT"])
        .len()
        .filter(pl.col("len") > 1)
        .height
)

print(
    "LNLD duplicate keys :",
    lnld.group_by(["ACCTNO", "TRANDT", "TRANAMT"])
        .len()
        .filter(pl.col("len") > 1)
        .height
)

tranx_bnm = lnld.join(
    dpld,
    on=["ACCTNO", "TRANDT", "TRANAMT"],
    how="inner"
)

print("TRANX rows :", tranx_bnm.height)

print("DPLD TRANDT range:", dpld["TRANDT"].min(), "to", dpld["TRANDT"].max())
print("LNLD TRANDT range:", lnld["TRANDT"].min(), "to", lnld["TRANDT"].max())
print("DPLD sample keys:", dpld.select(["ACCTNO", "TRANDT", "TRANAMT"]).head(5).to_dicts())
print("LNLD sample keys:", lnld.select(["ACCTNO", "TRANDT", "TRANAMT"]).head(5).to_dicts())

# ============================================================================
# DATA TRANX -- FOR PBB
# ============================================================================
def _trnxdesc(trancode: Optional[int], feeplan: Optional[str]) -> str:
    fp = (feeplan or "").strip()
    if trancode == 760 and fp:
        return FEEFMT.get(fp, fp)
    return TCODE_FMT.get(trancode, str(trancode) if trancode is not None else "")


tranx = (
    tranx_bnm
    .with_columns(
        [
            (pl.col("TRANAMT") / 1000).alias("TRANAMT1"),
            pl.lit(1).cast(pl.Int64).alias("VALUE"),  # CLASS VALUE (always 1)
        ]
    )
    .with_columns(
        pl.struct(["TRANCODE", "FEEPLAN"])
        .map_elements(
            lambda r: _trnxdesc(r["TRANCODE"], r["FEEPLAN"]),
            return_dtype=pl.String,
        )
        .alias("TRNXDESC")
    )
)

# ============================================================================
# ASA REPORT HELPERS (page length 60; RECFM=FBA-style leading control char)
# ============================================================================
def _box_row(cells, widths, aligns):
    parts = []
    for text, w, a in zip(cells, widths, aligns):
        if a == "L":
            parts.append(f"{text:<{w}}")
        elif a == "R":
            parts.append(f"{text:>{w}}")
        else:
            parts.append(text.center(w))
    return "|" + "|".join(parts) + "|"


def _render_report1_box(titles, label, n_val, sum_val):
    LW, NW, VW = 31, 16, 16
    lines = list(titles) + [""]
    lines.append("-" * (LW + NW + VW + 4))
    lines.append(_box_row(["", "NUMBER OF", "VALUE OF CHEQUES"], [LW, NW, VW], ["L", "C", "C"]))
    lines.append(_box_row(["", "CHEQUES", "(RM'000)"], [LW, NW, VW], ["L", "C", "C"]))
    lines.append("|" + "-" * LW + "+" + "-" * NW + "+" + "-" * VW + "|")
    lines.append(_box_row([label, f"{n_val:,.0f}", f"{sum_val:,.0f}"], [LW, NW, VW], ["L", "R", "R"]))
    lines.append("-" * (LW + NW + VW + 4))
    return lines


def _render_report23_box(titles, rows):
    NOW, PW, UW, VW = 15, 15, 16, 12
    total_w = NOW + PW + UW + VW + 5
    lines = list(titles) + [""]
    lines.append("-" * total_w)
    lines.append(_box_row(["", "CHEQUES ISSUED"], [NOW + PW + 1, UW + VW + 1], ["L", "C"]))
    lines.append("|" + " " * (NOW + PW + 1) + "|" + "-" * (UW + VW + 1) + "|")
    lines.append(_box_row(["", "", "VALUE"], [NOW + PW + 1, UW, VW], ["L", "L", "C"]))
    lines.append(_box_row(["", "UNIT", "(RM'000)"], [NOW + PW + 1, UW, VW], ["L", "C", "C"]))
    lines.append("|" + "-" * (NOW + PW + 1) + "+" + "-" * UW + "+" + "-" * VW + "|")
    lines.append(_box_row(["NO", "PURPOSE", "", ""], [NOW, PW, UW, VW], ["L", "L", "L", "L"]))
    lines.append("|" + "-" * NOW + "+" + "-" * PW + "|" + " " * UW + "|" + " " * VW + "|")
    for r in rows:
        wrapped = textwrap.wrap(str(r["TRNXDESC"]), PW) or [""]
        for i, text in enumerate(wrapped):
            no_cell = str(r["COUNT"]) if i == 0 else ""
            if i == len(wrapped) - 1:
                unit_cell, val_cell = f"{r['UNIT']:,.0f}", f"{r['VALUE']:,.2f}"
            else:
                unit_cell, val_cell = "", ""
            lines.append(_box_row([no_cell, text, unit_cell, val_cell], [NOW, PW, UW, VW], ["L", "L", "R", "R"]))
    lines.append("-" * total_w)
    return lines


def _render_report4_box(titles, costctrs, purposes, lookup):
    LABEL_W, UW, VW = 31, 16, 12

    full_sep_row = (
        "|" + "-" * LABEL_W
        + "".join("+" + "-" * UW + "+" + "-" * VW for _ in purposes)
        + "|"
    )
    total_w = len(full_sep_row)
    purposes_area_w = total_w - LABEL_W - 3

    lines = list(titles) + [""]
    lines.append("-" * total_w)

    lines.append("|" + " " * LABEL_W + "|" + "PURPOSE".center(purposes_area_w) + "|")
    lines.append("|" + " " * LABEL_W + "|" + "-" * purposes_area_w + "|")

    if len(purposes) == 1:
        lines.append("|" + " " * LABEL_W + "|" + purposes[0].center(purposes_area_w) + "|")
        lines.append("|" + " " * LABEL_W + "|" + "-" * purposes_area_w + "|")
    else:
        group_w = UW + 1 + VW
        row = "|" + " " * LABEL_W + "|"
        sep = "|" + " " * LABEL_W + "|"
        for p in purposes:
            row += p.center(group_w) + "|"
            sep += "-" * group_w + "|"
        lines.append(row)
        lines.append(sep)

    lines.append(
        "|" + " " * LABEL_W + "|"
        + "".join(" " * UW + "|" + "VALUE".center(VW) + "|" for _ in purposes)
    )
    lines.append(
        "|" + " " * LABEL_W + "|"
        + "".join("UNIT".center(UW) + "|" + "(RM'000)".center(VW) + "|" for _ in purposes)
    )
    lines.append(full_sep_row)

    lines.append(
        "|" + "COSTCTR".ljust(LABEL_W) + "|"
        + "".join(" " * UW + "|" + " " * VW + "|" for _ in purposes)
    )
    lines.append(
        "|" + "-" * LABEL_W + "|"
        + "".join(" " * UW + "|" + " " * VW + "|" for _ in purposes)
    )

    for idx, c in enumerate(costctrs):
        row = "|" + (str(c) if c is not None else "").ljust(LABEL_W) + "|"
        for p in purposes:
            unit, value = lookup.get((c, p), (0, 0.0))
            row += f"{unit:,.0f}".rjust(UW) + "|" + f"{value:,.2f}".rjust(VW) + "|"
        lines.append(row)
        if idx < len(costctrs) - 1:
            lines.append(full_sep_row)

    lines.append("-" * total_w)
    return lines


# ============================================================================
# REPORT 1: PROC TABULATE - TOTAL CHEQUES ISSUED
# ============================================================================
_total_n = tranx.height
_total_sum = tranx["TRANAMT1"].sum() or 0.0

_titles_1 = [
    "REPORT ID : EIBQEPC1",
    "PUBLIC BANK BERHAD",
    f"CHEQUES ISSUED BY THE BANK AS AT {RDATE}",
]

# ============================================================================
# REPORT 2: ALL PAYMENTS BY NUMBER OF CHEQUES
# PROC SUMMARY by TRNXDESC -> sort DESCENDING UNIT -> add COUNT rank
# ============================================================================
_tran1_n = (
    tranx.group_by("TRNXDESC")
    .agg([pl.len().alias("UNIT"), pl.col("TRANAMT1").sum().alias("TRANAMT1")])
    .sort("UNIT", descending=True)
    .with_row_index("COUNT", offset=1)
)

_xxx_n = _tran1_n.select(["COUNT", "TRNXDESC"])
_tranx1 = tranx.join(_xxx_n, on="TRNXDESC", how="left").sort("COUNT")

_tbl2 = (
    _tranx1.group_by(["COUNT", "TRNXDESC"])
    .agg([pl.len().alias("UNIT"), pl.col("TRANAMT1").sum().alias("VALUE")])
    .sort("COUNT")
)

_titles_2 = [
    "REPORT ID : EIBQEPC1",
    "PUBLIC BANK BERHAD",
    f"ALL PAYMENTS BY NUMBER OF CHEQUES AS AT {RDATE}",
]

# ============================================================================
# REPORT 3: ALL PAYMENTS BY VALUE OF CHEQUES
# PROC SUMMARY by TRNXDESC -> sort DESCENDING TRANAMT1 -> add COUNT rank
# ============================================================================
_tran1_v = (
    tranx.group_by("TRNXDESC")
    .agg([pl.len().alias("UNIT"), pl.col("TRANAMT1").sum().alias("TRANAMT1")])
    .sort("TRANAMT1", descending=True)
    .with_row_index("COUNT", offset=1)
)

_xxx_v = _tran1_v.select(["COUNT", "TRNXDESC"])
_tranx2 = tranx.join(_xxx_v, on="TRNXDESC", how="left").sort("COUNT")

_tbl3 = (
    _tranx2.group_by(["COUNT", "TRNXDESC"])
    .agg([pl.len().alias("UNIT"), pl.col("TRANAMT1").sum().alias("VALUE")])
    .sort("COUNT")
)

_titles_3 = [
    "REPORT ID : EIBQEPC1",
    "PUBLIC BANK BERHAD",
    f"ALL PAYMENTS BY VALUE OF CHEQUES AS AT {RDATE}",
]

# ============================================================================
# REPORT 4: ALL PAYMENTS BY BRANCH (COSTCTR x TRNXDESC cross-tab)
# CLASS TRNXDESC COSTCTR; VAR TRANAMT1;
# TABLE COSTCTR='COSTCTR', TRNXDESC='PURPOSE'*TRANAMT1=' '*(N='UNIT'*F=16. SUM="VALUE (RM'000)")
# ============================================================================
_cross = (
    tranx.group_by(["COSTCTR", "TRNXDESC"])
    .agg([pl.len().alias("UNIT"), pl.col("TRANAMT1").sum().alias("VALUE")])
)

_costctrs = sorted(_cross["COSTCTR"].unique().to_list(), key=lambda v: (v is None, v))
_purposes = sorted(_cross["TRNXDESC"].unique().to_list())

_cross_lookup = {
    (r["COSTCTR"], r["TRNXDESC"]): (r["UNIT"], r["VALUE"]) for r in _cross.to_dicts()
}

_titles_4 = [
    "REPORT ID : EIBQEPC1",
    "PUBLIC BANK BERHAD",
    f"ALL PAYMENTS BY BRANCH AS AT {RDATE}",
]

# ============================================================================
# ASSEMBLE FULL REPORT (ASA carriage control applied per section)
# ============================================================================
report_lines: list[str] = []
report_lines += _finalize_page(_render_report1_box(_titles_1, "CHEQUES ISSUED", _total_n, _total_sum), 1)
report_lines += _finalize_page(_render_report23_box(_titles_2, _tbl2.to_dicts()), 2)
report_lines += _finalize_page(_render_report23_box(_titles_3, _tbl3.to_dicts()), 3)
report_lines += _finalize_page(_render_report4_box(_titles_4, _costctrs, _purposes, _cross_lookup), 4)

# ============================================================================
# WRITE OUTPUT (output_date.py -> date-stamped filename, no extension)
# ============================================================================
output_path = build_output_file(OUTPUT_DIR, "EPCU_PART1_WEEKLY_PBB", date_format="ddmmyy")
report_file = output_path.with_suffix(".txt")

with open(report_file, "w", encoding="latin1") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"[OUTPUT] Report written to: {report_file}")
print("\n".join(report_lines))
