#!/usr/bin/env python3
"""
Program  : EIIWEPC1.py
Purpose  : CHEQUES ISSUED BY THE BANK - PUBLIC ISLAMIC BANK BERHAD (PIBB), WEEKLY
           (ESMR: 2011-1379, ported PIBB variant of EIBWEPC1)
           Produces four tabular reports:
             1. Total cheques issued (number + value)
             2. All payments by number of cheques
             3. All payments by value of cheques
             4. All payments by branch (COSTCTR x PURPOSE cross-tab)

Notes
-----
* Per the original JCL, DPLD (SAP.PBB.EPCU.LOANDISB) and LNLD
  (RBP2.B033.LN.BNM.BNKCHEQ.RPT) are the SAME physical source files used by
  the PBB weekly job (EIBWEPC1) -- only the LOAN DD (SAP.PIBB.MNILN, used solely
  to derive REPTDATE) differs. DPLD is also the same file used by the quarterly
  jobs (EIBQEPC1 / EIIQEPC1); LNLD is NOT the QRTR file. The PIBB-specific
  behaviour is entirely in the COSTCTR inclusion filter, the report titles and
  the output locations.
* Report date comes from ctl_dwh_batch_dttm.sas7bdat (SOURCE_SYSTEM_CD = LN),
  not from REPTDATE.py / LOAN.REPTDATE. The job covers the week ending on the
  8th, 15th, 22nd or the last calendar day of the month; PREVDATE follows the
  original SELECT(DAY(REPTDATE)): 8 -> 1st, 15 -> 9th, 22 -> 16th,
  OTHERWISE -> 23rd.
* DPLD is read from dpld<REPTMON>.sas7bdat via a temporary parquet that is
  deleted after reading. Rows are kept when &PREVDT <= REPTDATE <= &REPTDT, where
  REPTDATE is DPLD's own column (the disbursement batch date carried on each DPLD
  record), distinct from the macro variable REPTDATE derived below.
* LNLD is a raw mainframe fixed-width flat file, kept as .txt and read via
  byte-offset slicing in chunks, never via read_parquet()/read_csv().
* OPTIONS MISSING=0 causes SAS to print '0' instead of blank/'.' for
  missing numeric values; reflected in Report 4's cross-tab, where an
  unobserved COSTCTR/PURPOSE combination prints 0 / 0.00 instead of blank.
* PIBB COSTCTR filter (applied at LNLD INPUT step, matching the DATA TRANX
  commented-out re-filter):
      IF (3000 < COSTCTR < 3999) OR COSTCTR IN (4043,4048);
  This is the inclusion-range counterpart of EIBWEPC1's exclusion filter.
* TITLE1 in the original SAS literally reads 'REPORT ID : EIIQEPC1' for
  Reports 1-3 and 'REPORT ID : EIBQEPC1' for Report 4 (an apparent
  copy-paste artefact carried over from the quarterly PBB program). Both
  are reproduced verbatim below for fidelity to the source.
* Outputs: BNM.DPLD / BNM.LNLD / BNM.TRANX (.sas7bdat via saspy, SAP.PIBB.EPCUWH.WEEKLY)
  and the text report (SAP.PIBB.EPCU.PART1.WEEKLY).
"""

# ============================================================================
# IMPORTS
# ============================================================================
import os
import re
import tempfile
import textwrap
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import saspy

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Production Path
BASE_DIR    = Path("/host_pq/mis")
INPUT_DIR   = BASE_DIR / "input" / "EPCU"
OUTPUT_DIR  = BASE_DIR
DPLD_DIR    = INPUT_DIR / "LOANDISB"                                        # SAP.PBB.EPCU.LOANDISB (same as quarterly / PBB)
LNLD_PATH   = INPUT_DIR / "textfile_WEEKLY" / "LN_BNM_BNKCHEQ_RPT.TXT"      # RBP2.B033.LN.BNM.BNKCHEQ.RPT(0) (shared with PBB weekly)
BNM_DIR     = OUTPUT_DIR / "input" / "EPCU" / "IEPCUWH_WEEKLY"              # SAP.PIBB.EPCUWH.WEEKLY(+1)
REPORT_DIR  = OUTPUT_DIR / "output" / "EPCU" / "report"                     # same folder as quarterly

# # Testing Path
# BASE_DIR    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
# INPUT_DIR   = Path("/stgsrcsys/host/uat")
# OUTPUT_DIR  = BASE_DIR / "output" / "EIIWEPC1"
# DPLD_DIR    = INPUT_DIR
# LNLD_PATH   = INPUT_DIR / "LN_BNM_BNKCHEQ_RPT.TXT"
# BNM_DIR     = OUTPUT_DIR / "EPCUWH_WEEKLY"                              # SAP.PIBB.EPCUWH.WEEKLY(+1) -> dpld / lnld / tranx .sas7bdat
# REPORT_DIR  = OUTPUT_DIR

CTL_BATCH_DTTM = Path("/sasdata/dwh/control/ctl_dwh_batch_dttm.sas7bdat")
BATCH_SOURCE   = "LN"
TEMP_DIR       = None   # None = system temp

for _d in (OUTPUT_DIR, BNM_DIR, REPORT_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# Chunked reading controls for the large LNLD flat file.
CHUNK_SIZE = 500_000
ROW_LIMIT = int(os.environ.get("ROW_LIMIT", "0")) or None  # test-mode cutoff

PAGE_LENGTH = 60  # ASA report page length (not specified in JCL -> default 60); the weekly report is paginated per report by _finalize_page

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
# Report date comes from ctl_dwh_batch_dttm (SOURCE_SYSTEM_CD = LN).
# ============================================================================
def get_batch_date(source_system_cd: str) -> date:
    ctl = pd.read_sas(str(CTL_BATCH_DTTM), encoding="latin1")
    ctl["SOURCE_SYSTEM_CD"] = ctl["SOURCE_SYSTEM_CD"].astype(str).str.strip()
    hit = ctl.loc[ctl["SOURCE_SYSTEM_CD"] == source_system_cd, "BATCH_DTTM"]
    if hit.empty:
        raise ValueError(f"SOURCE_SYSTEM_CD '{source_system_cd}' not found in {CTL_BATCH_DTTM}")
    return pd.Timestamp(hit.iloc[0]).date()


REPTDATE_VAL: date = get_batch_date(BATCH_SOURCE)
# REPTDATE_VAL: date = date(2026, 10, 22)             # UAT testing date

# SELECT(DAY(REPTDATE)):
#   WHEN(08) PREVDATE = 1st of month   WHEN(15) PREVDATE = 9th of month
#   WHEN(22) PREVDATE = 16th of month  OTHERWISE PREVDATE = 23rd of month
_PREV_START_DAY = {8: 1, 15: 9, 22: 16}
PREVDATE: date = REPTDATE_VAL.replace(day=_PREV_START_DAY.get(REPTDATE_VAL.day, 23))

REPTYEAR = REPTDATE_VAL.strftime("%Y")            # CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.))
REPTMON  = f"{REPTDATE_VAL.month:02d}"            # CALL SYMPUT('REPTMON',PUT(MONTH(REPTDATE),Z2.))
REPTDAY  = f"{REPTDATE_VAL.day:02d}"              # CALL SYMPUT('REPTDAY',PUT(DAY(REPTDATE),Z2.))
RDATE    = REPTDATE_VAL.strftime("%d/%m/%Y")      # CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY10.))
REPTDT: date = REPTDATE_VAL                       # CALL SYMPUT('REPTDT',REPTDATE)
PREVDT: date = PREVDATE                           # CALL SYMPUT('PREVDT',PREVDATE)

# SAP.PIBB.EPCU.PART1.WEEKLY(+1), inc. date in the name (job runs 4 times a month)
REPORT_FILE = REPORT_DIR / f"PIBB_EPCU_PART1_WEEKLY_{REPTDATE_VAL.strftime('%d%m%y')}.txt"

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
    """DDMMYY8. informat with YEARCUTOFF=1930 (dd/mm/yy, ddmmyy or ddmmyyyy)."""
    digits = re.sub(r"\D", "", raw.strip())
    try:
        if len(digits) == 8:
            return date(int(digits[4:8]), int(digits[2:4]), int(digits[0:2]))
        if len(digits) == 6:
            yy = int(digits[4:6])
            return date(1900 + yy if yy >= 30 else 2000 + yy, int(digits[2:4]), int(digits[0:2]))
    except ValueError:
        return None
    return None


# ============================================================================
# DATA BNM.DPLD
# SET DPLD.DPLD&REPTMON; IF (&PREVDT<=REPTDATE<=&REPTDT);
# DPLD.DPLD&REPTMON -> dpld<REPTMON>.sas7bdat (same physical source as PBB's
# EIBWEPC1 and the quarterly jobs -- the JCL's DPLD DD is SAP.PBB.EPCU.LOANDISB,
# unchanged for PIBB). The filter is applied on DPLD's own REPTDATE column.
# ============================================================================
DPLD_PATH = DPLD_DIR / f"dpld{REPTMON}.sas7bdat"


def _sas_to_parquet(src: Path, dst: Path, chunk_rows: int = CHUNK_SIZE) -> None:
    """Chunked .sas7bdat -> Parquet (schema locked on first chunk)."""
    writer, schema = None, None
    try:
        for chunk in pd.read_sas(str(src), encoding="latin1", chunksize=chunk_rows):
            for col in chunk.columns[chunk.dtypes == object]:
                chunk[col] = chunk[col].astype("string")
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                schema = table.schema
                writer = pq.ParquetWriter(str(dst), schema, compression="snappy")
            else:
                table = table.cast(schema, safe=False)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()


def _as_date(df: pl.DataFrame, col: str) -> pl.Expr:
    """SAS date column -> pl.Date (handles date, datetime or raw SAS day-number)."""
    dt = df.schema[col]
    if dt == pl.Date:
        return pl.col(col)
    if isinstance(dt, pl.Datetime):
        return pl.col(col).dt.date()
    return (pl.date(1960, 1, 1) + pl.duration(days=pl.col(col).cast(pl.Int64))).cast(pl.Date)


def _load_dpld() -> pl.DataFrame:
    with tempfile.TemporaryDirectory(dir=TEMP_DIR) as tmp:      # parquet is removed on exit
        tmp_pq = Path(tmp) / f"dpld{REPTMON}.parquet"
        _sas_to_parquet(DPLD_PATH, tmp_pq)
        con = duckdb.connect()
        try:
            df = con.execute(f"SELECT * FROM read_parquet('{tmp_pq.as_posix()}')").pl()
        finally:
            con.close()
    df = df.rename({c: c.upper() for c in df.columns})
    return df.with_columns(
        _as_date(df, "TRANDT").alias("TRANDT"),
        _as_date(df, "REPTDATE").alias("REPTDATE"),
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("TRANAMT").cast(pl.Float64).round(2),
    )


dpld = _load_dpld().filter(
    (pl.col("REPTDATE") >= PREVDT) & (pl.col("REPTDATE") <= REPTDT)
)

# ============================================================================
# DATA BNM.LNLD
# INFILE LNLD MISSOVER; fixed-width mainframe flat file, read in chunks with
# byte-offset slicing (1-based SAS @col -> 0-based Python slice). Same
# physical source file as EIBWEPC1 (RBP2.B033.LN.BNM.BNKCHEQ.RPT).
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
# Subsetting IF (applied during the INPUT step, before PROC SORT) -- PIBB
# inclusion filter (opposite range from PBB's exclusion filter):
#   (3000 < COSTCTR < 3999) OR COSTCTR IN (4043,4048)
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

            # PIBB inclusion filter: (3000 < COSTCTR < 3999) OR COSTCTR IN (4043,4048)
            # A missing COSTCTR satisfies neither condition, so it is excluded
            # (mirrors SAS: a missing numeric fails "3000 < . < 3999" and is
            # not in the explicit IN-list).
            cond_range = costctr is not None and 3000 < costctr < 3999
            cond_incl = costctr in (4043, 4048)
            if not (cond_range or cond_incl):
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


# LNLD (RBP2.B033.LN.BNM.BNKCHEQ.RPT) -- weekly flat file (not the QRTR file)
lnld = _load_lnld_fixed_width(LNLD_PATH)

lnld = lnld.with_columns(
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANDT").cast(pl.Date),
    pl.col("TRANAMT").cast(pl.Float64).round(2),
)

# ============================================================================
# PROC SORT + DATA BNM.TRANX
# MERGE LNLD(IN=A) DPLD(IN=B); BY ACCTNO TRANDT TRANAMT; IF A & B;
# SAS MERGE semantics: one-to-one pairing inside duplicate key groups, last row
# reused when one side runs out; common columns take the DPLD value.
# ============================================================================
_KEYS = ["ACCTNO", "TRANDT", "TRANAMT"]


def _sas_merge_ab(a_df: pl.DataFrame, b_df: pl.DataFrame, keys: list) -> pl.DataFrame:
    """MERGE A(IN=A) B(IN=B); BY keys; IF A & B;  (row order inside a key group = input order)"""
    a = a_df.with_columns(pl.int_range(pl.len()).over(keys).alias("_SA"),
                          pl.len().over(keys).alias("_NA"))
    b = b_df.with_columns(pl.int_range(pl.len()).over(keys).alias("_SB"),
                          pl.len().over(keys).alias("_NB"))
    j = a.join(b, on=keys, how="inner", suffix="_DPLD").filter(
        (pl.col("_SA") == pl.col("_SB"))
        | ((pl.col("_SA") == pl.col("_NA") - 1) & (pl.col("_SB") >= pl.col("_NA")))
        | ((pl.col("_SB") == pl.col("_NB") - 1) & (pl.col("_SA") >= pl.col("_NB")))
    )
    dup = [c for c in b_df.columns if c in a_df.columns and c not in keys]   # later dataset wins
    j = j.with_columns([pl.col(f"{c}_DPLD").alias(c) for c in dup])
    return (j.drop([f"{c}_DPLD" for c in dup] + ["_NA", "_NB"])
             .sort(keys + ["_SA", "_SB"])          # BNM.TRANX is saved sorted by the BY keys
             .drop(["_SA", "_SB"]))


tranx_bnm = _sas_merge_ab(lnld, dpld, _KEYS)


def _write_bnm_datasets(datasets: dict) -> None:
    """DATA BNM.DPLD / BNM.LNLD / BNM.TRANX -> .sas7bdat in BNM_DIR (SAP.PIBB.EPCUWH.WEEKLY)."""
    sas = saspy.SASsession()
    try:
        sas.saslib("BNM", path=str(BNM_DIR))
        for name, df in datasets.items():
            date_cols = [c for c, t in df.schema.items() if t == pl.Date]
            pdf = df.with_columns([pl.col(c).cast(pl.Datetime) for c in date_cols]).to_pandas()
            sas.df2sd(pdf, table=name, libref="BNM", datetimes={c: "date" for c in date_cols})
            print(f"[OUTPUT] BNM.{name}: {df.height:,} rows -> {BNM_DIR / (name.lower() + '.sas7bdat')}")
    finally:
        sas.endsas()


_write_bnm_datasets({"DPLD": dpld, "LNLD": lnld, "TRANX": tranx_bnm})

# ============================================================================
# DATA TRANX -- FOR PIBB
# *IF COSTCTR < 3000 OR COSTCTR > 3999;   (commented out in original SAS --
#  the PBB-style exclusion filter is superseded by the LNLD-stage PIBB
#  inclusion filter applied above, so it stays commented here as well)
# ============================================================================
def _trnxdesc(trancode: Optional[int], feeplan: Optional[str]) -> str:
    fp = (feeplan or "").strip()
    if trancode == 760 and fp:
        return FEEFMT.get(fp, fp)
    return TCODE_FMT.get(trancode, str(trancode) if trancode is not None else "")


tranx = (
    tranx_bnm
    # .filter((pl.col("COSTCTR") < 3000) | (pl.col("COSTCTR") > 3999))  # *IF COSTCTR < 3000 OR COSTCTR > 3999; (commented out in SAS)
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
# REPORT HELPERS (SASLIST: RECFM=FB, LRECL=133)
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
    "REPORT ID : EIIQEPC1",
    "PUBLIC ISLAMIC BANK BERHAD",
    f"CHEQUES ISSUED BY THE BANK AS AT {RDATE}",
]

# ============================================================================
# REPORT 2: ALL PAYMENTS BY NUMBER OF CHEQUES
# PROC SUMMARY by TRNXDESC -> sort DESCENDING UNIT -> add COUNT rank
# ============================================================================
_tran1_n = (
    tranx.group_by("TRNXDESC")
    .agg([pl.len().alias("UNIT"), pl.col("TRANAMT1").sum().alias("TRANAMT1")])
    .sort("TRNXDESC")
    .sort("UNIT", descending=True, maintain_order=True)
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
    "REPORT ID : EIIQEPC1",
    "PUBLIC ISLAMIC BANK BERHAD",
    f"ALL PAYMENTS BY NUMBER OF CHEQUES AS AT {RDATE}",
]

# ============================================================================
# REPORT 3: ALL PAYMENTS BY VALUE OF CHEQUES
# PROC SUMMARY by TRNXDESC -> sort DESCENDING TRANAMT1 -> add COUNT rank
# ============================================================================
_tran1_v = (
    tranx.group_by("TRNXDESC")
    .agg([pl.len().alias("UNIT"), pl.col("TRANAMT1").sum().alias("TRANAMT1")])
    .sort("TRNXDESC")
    .sort("TRANAMT1", descending=True, maintain_order=True)
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
    "REPORT ID : EIIQEPC1",
    "PUBLIC ISLAMIC BANK BERHAD",
    f"ALL PAYMENTS BY VALUE OF CHEQUES AS AT {RDATE}",
]

# ============================================================================
# REPORT 4: ALL PAYMENTS BY BRANCH (COSTCTR x TRNXDESC cross-tab)
# CLASS TRNXDESC COSTCTR; VAR TRANAMT1;
# TABLE COSTCTR='COSTCTR', TRNXDESC='PURPOSE'*TRANAMT1=' '*(N='UNIT'*F=16. SUM="VALUE (RM'000)")
# NOTE: original SAS TITLE1 for this report literally reads
# 'REPORT ID : EIBQEPC1' (copy-paste artefact from the PBB source);
# reproduced verbatim here for fidelity.
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
    "PUBLIC ISLAMIC BANK BERHAD",
    f"ALL PAYMENTS BY BRANCH AS AT {RDATE}",
]

# ============================================================================
# ASSEMBLE FULL REPORT (LRECL padding + page number applied per section)
# ============================================================================
report_lines: list[str] = []
report_lines += _finalize_page(_render_report1_box(_titles_1, "CHEQUES ISSUED", _total_n, _total_sum), 1)
report_lines += _finalize_page(_render_report23_box(_titles_2, _tbl2.to_dicts()), 2)
report_lines += _finalize_page(_render_report23_box(_titles_3, _tbl3.to_dicts()), 3)
report_lines += _finalize_page(_render_report4_box(_titles_4, _costctrs, _purposes, _cross_lookup), 4)

# ============================================================================
# WRITE OUTPUT
# Output DSNs: SAP.PIBB.EPCUWH.WEEKLY / SAP.PIBB.EPCU.PART1.WEEKLY (PIBB, differs from PBB)
# ============================================================================
with open(REPORT_FILE, "w", encoding="latin1") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"[OUTPUT] Report written to: {REPORT_FILE}")
print("\n".join(report_lines))
