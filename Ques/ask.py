#!/usr/bin/env python3
"""
Program  : EIBQEPC1.py
Purpose  : CHEQUES ISSUED BY THE BANK - PUBLIC BANK BERHAD (PBB)
           To run quarterly after EIBDEPDP (ESMR: 2011-1379).
           Produces three tabular reports:
             1. Total cheques issued (number + value)
             2. Top five payments by number of cheques
             3. Top five payments by value of cheques

Notes
-----
* LOAN.REPTDATE no longer exists as a physical dataset/parquet file.
  The report date is derived from REPTDATE.py instead.
* This job runs on the 2nd calendar day of the month, and the DPLD member
  suffix (&REPTMON) must resolve to the *previous completed month*
  (e.g. running on 02-Jul must read dpld06.sas7bdat, not dpld07.sas7bdat).
  get_monthly_reptdate_values() (last day of previous month) reproduces
  this behaviour exactly, since REPTMON is taken directly from REPTDATE's
  own month in the original SAS DATA REPTDATE step.
* LNLD is a raw mainframe fixed-width flat file (~8GB) - read via
  byte-offset slicing in chunks, never via read_parquet()/read_csv().
"""

# ============================================================================
# IMPORTS
# ============================================================================
import os
import re
import textwrap
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import polars as pl

from REPTDATE import get_monthly_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS") 
INPUT_DIR   = Path("/stgsrcsys/host/uat")
OUTPUT_DIR  = BASE_DIR / "output" / "EIBQEPC1"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Chunked reading controls for the large LNLD flat file.
CHUNK_SIZE = 500_000
ROW_LIMIT = int(os.environ.get("ROW_LIMIT", "0")) or None  # test-mode cutoff

PAGE_LENGTH = 60  # ASA report page length (not specified in JCL -> default 60)

# ============================================================================
# DATA REPTDATE STEP EQUIVALENT
# (Original: SET LOAN.REPTDATE -> derive SREPTDATE/PRVRPTDATE/... )
# No reptdate.parquet exists; base REPTDATE comes from REPTDATE.py.
# ============================================================================
_reptdate_values = get_monthly_reptdate_values()
REPTDATE_VAL: date = _reptdate_values.reptdate            # last day of prev month

# SREPTDATE = MDY(MONTH(REPTDATE),1,YEAR(REPTDATE))
_sreptdate = REPTDATE_VAL.replace(day=1)
# PRVRPTDATE = SREPTDATE - 1
_prvrptdate = _sreptdate - timedelta(days=1)
# SSREPTDATE = MDY(MONTH(PRVRPTDATE),1,YEAR(PRVRPTDATE))
_ssreptdate = _prvrptdate.replace(day=1)
# PPRVRPTDATE = SSREPTDATE - 1
_pprvrptdate = _ssreptdate - timedelta(days=1)

REPTMON = f"{REPTDATE_VAL.month:02d}"              # CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE),Z2.))
REPTYEAR = _reptdate_values.reptyear               # CALL SYMPUT('REPTYEAR', PUT(REPTDATE,YEAR2.))
RDATE = _reptdate_values.ddmmyy8                   # CALL SYMPUT('RDATE', PUT(REPTDATE,DDMMYY8.))
PREPTMON = f"{_prvrptdate.month:02d}"              # CALL SYMPUT('PREPTMON', PUT(MONTH(PRVRPTDATE),Z2.))
PPREPTMON = f"{_pprvrptdate.month:02d}"            # CALL SYMPUT('PPREPTMON', PUT(MONTH(PPRVRPTDATE),Z2.))

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


def _parse_implied_decimal(raw: str, decimals: int) -> Optional[int]:
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
    """DDMMYY8. informat with OPTIONS YEARCUTOFF=1930 (delimited or plain)."""
    s = raw.strip()
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    if len(digits) != 6:
        return None
    dd, mm, yy = int(digits[0:2]), int(digits[2:4]), int(digits[4:6])
    year = 1900 + yy if yy >= 30 else 2000 + yy
    try:
        return date(year, mm, dd)
    except ValueError:
        return None


# ============================================================================
# DATA BNM.DPLD  -- combine three months of DPLD (SAP.PBB.EPCU.LOANDISB)
# DPLD.DPLD&REPTMON / &PREPTMON / &PPREPTMON  ->  dpld<mm>.sas7bdat
# ============================================================================
def _load_dpld(mon: str) -> pl.DataFrame:
    # path = INPUT_DIR / f"dpld{mon}.sas7bdat"
    path = INPUT_DIR / f"dpld06.sas7bdat"
    pdf = pd.read_sas(path, encoding="latin1")
    return pl.from_pandas(pdf)


dpld = pl.concat(
    [_load_dpld(REPTMON), _load_dpld(PREPTMON), _load_dpld(PPREPTMON)],
    how="diagonal_relaxed",
)

dpld = dpld.with_columns(
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANDT").cast(pl.Date),
    pl.col("TRANAMT").cast(pl.Float64),
)

# ============================================================================
# DATA BNM.LNLD
# INFILE LNLD MISSOVER; fixed-width mainframe flat file (~8GB), read in
# chunks with byte-offset slicing (1-based SAS @col -> 0-based Python slice).
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

# LNLD (BNM.BNKCHEQ.RPT.QRTR GDG) - large fixed-width flat file
lnld_path = INPUT_DIR / "BNKCHEQ_RPT_QRTR.txt"
lnld      = _load_lnld_fixed_width(lnld_path)

lnld = lnld.with_columns(
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANDT").cast(pl.Date),
    pl.col("TRANAMT").cast(pl.Float64),
)

# ============================================================================
# PROC SORT + DATA BNM.TRANX
# MERGE LNLD(IN=A) DPLD(IN=B); BY ACCTNO TRANDT TRANAMT; IF A & B;
# Equivalent to an inner join on the BY keys; PROC SORT is not required
# since polars' join does not depend on pre-sorted inputs.
# ============================================================================
tranx_bnm = lnld.join(dpld, on=["ACCTNO", "TRANDT", "TRANAMT"], how="inner")

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
def _fmt_num(value, width: int = 16, decimals: int = 0) -> str:
    if value is None:
        value = 0
    return f"{value:>{width},.{decimals}f}"


def _emit_section(
    all_lines: list[str],
    titles: list[str],
    col_header: list[str],
    data_lines: list[str],
) -> None:
    """Append an ASA-formatted section, repeating the header on page overflow."""
    header_block = titles + [""] + col_header
    idx = 0
    if not data_lines:
        data_lines = [""]

    while idx < len(data_lines):
        page_lines = header_block.copy()
        remaining_capacity = max(PAGE_LENGTH - len(page_lines), 1)
        chunk = data_lines[idx: idx + remaining_capacity]
        idx += len(chunk)
        page_lines.extend(chunk)

        for i, line in enumerate(page_lines):
            control = "1" if i == 0 else " "
            all_lines.append(control + line)

    all_lines.append(" ")  # blank separator line between report sections


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


def _apply_asa(lines):
    return [("1" if i == 0 else " ") + line for i, line in enumerate(lines)]


# report_lines: list[str] = []

# ============================================================================
# REPORT 1: PROC TABULATE - TOTAL CHEQUES ISSUED
# CLASS VALUE (formatted DESC.); VAR TRANAMT1;
# N and SUM both use *F=16. (0-decimal, width-16 display)
# ============================================================================
_total_n = tranx.height
_total_sum = tranx["TRANAMT1"].sum() or 0.0

_titles_1 = [
    "REPORT ID : EIBQEPC1",
    "PUBLIC BANK BERHAD",
    f"CHEQUES ISSUED BY THE BANK AS AT {RDATE}",
]

# # _col_hdr_1 = [f"{'':<20}{'NUMBER OF CHEQUES':>20}{chr(39) + 'S000':>0}"]
# # Column header rebuilt without stray characters:
# value_hdr = "VALUE OF CHEQUES (RM'000)"

# _col_hdr_1 = [f"{'':<20}{'NUMBER OF CHEQUES':>20}{value_hdr:>28}"]
# _data_1 = [
#     f"{DESC_FMT[1]:<20}{_fmt_num(_total_n, 20, 0)}{_fmt_num(_total_sum, 28, 0)}"
# ]

# _emit_section(report_lines, _titles_1, _col_hdr_1, _data_1)

# ============================================================================
# REPORT 2: TOP FIVE(5) PAYMENTS BY NUMBER OF CHEQUES
# PROC SUMMARY by TRNXDESC -> sort DESCENDING UNIT -> add COUNT rank
# N uses *F=16.; SUM uses tabulate's default (business/money -> 2 decimals)
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
    f"TOP FIVE(5) PAYMENTS BY NUMBER OF CHEQUES AS AT {RDATE}",
]

value_hdr = "VALUE (RM'000)"

# _col_hdr_2 = [f"{'NO':<4}{'PURPOSE':<50}{'UNIT':>16}{value_hdr:>18}"]
# _data_2 = [
#     f"{r['COUNT']:<4}{str(r['TRNXDESC']):<50}"
#     f"{_fmt_num(r['UNIT'], 16, 0)}{_fmt_num(r['VALUE'], 18, 2)}"
#     for r in _tbl2.to_dicts()
# ]

# _emit_section(report_lines, _titles_2, _col_hdr_2, _data_2)

# ============================================================================
# REPORT 3: TOP FIVE(5) PAYMENTS BY VALUE OF CHEQUES
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
    f"TOP FIVE(5) PAYMENTS BY VALUE OF CHEQUES AS AT {RDATE}",
]

value_hdr = "VALUE (RM'000)"

# _col_hdr_3 = [f"{'NO':<4}{'PURPOSE':<50}{'UNIT':>16}{value_hdr:>18}"]
# _data_3 = [
#     f"{r['COUNT']:<4}{str(r['TRNXDESC']):<50}"
#     f"{_fmt_num(r['UNIT'], 16, 0)}{_fmt_num(r['VALUE'], 18, 2)}"
#     for r in _tbl3.to_dicts()
# ]

# _emit_section(report_lines, _titles_3, _col_hdr_3, _data_3)


report_lines: list[str] = []
report_lines += _apply_asa(_render_report1_box(_titles_1, "CHEQUES ISSUED", _total_n, _total_sum))
report_lines.append(" ")
report_lines += _apply_asa(_render_report23_box(_titles_2, _tbl2.to_dicts()))
report_lines.append(" ")
report_lines += _apply_asa(_render_report23_box(_titles_3, _tbl3.to_dicts()))

# ============================================================================
# WRITE OUTPUT (output_date.py -> date-stamped filename, no extension)
# ============================================================================
output_path = build_output_file(OUTPUT_DIR, "EIBQEPC1", date_format="ddmmyy")
report_file = output_path.with_suffix(".txt")

with open(report_file, "w", encoding="latin1") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"[OUTPUT] Report written to: {report_file}")
print("\n".join(report_lines))
