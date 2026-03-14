#!/usr/bin/env python3
"""
Program  : EIBQEPC1.py
Purpose  : CHEQUES ISSUED BY THE BANK - PUBLIC BANK BERHAD (PBB)
           To run quarterly after EIBDEPDP (ESMR: 2011-1379).
           Produces three tabular reports:
             1. Total cheques issued (number + value)
             2. Top five payments by number of cheques
             3. Top five payments by value of cheques
"""

# ============================================================================
# IMPORTS
# ============================================================================
import os
from datetime import date, timedelta

import duckdb
import polars as pl

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
INPUT_DIR  = os.environ.get("INPUT_DIR",  "input")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")

# Input parquet paths
# DPLD.DPLD&REPTMON / &PREPTMON / &PPREPTMON  ->  dpld_<mm>.parquet
# LNLD flat file                              ->  already converted to parquet
# LOAN.REPTDATE                               ->  reptdate.parquet

REPTDATE_PQ = os.path.join(INPUT_DIR, "REPTDATE.parquet")
LNLD_PQ     = os.path.join(INPUT_DIR, "LNLD.parquet")        # BNM.LNLD output

# Output
REPORT_TXT  = os.path.join(OUTPUT_DIR, "EIBQEPC1.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()


def _load(path: str) -> pl.DataFrame:
    return con.execute(f"SELECT * FROM read_parquet('{path}')").pl()

# ============================================================================
# DERIVE MACRO VARIABLES FROM REPTDATE
# (DATA REPTDATE step)
# ============================================================================
_rd_df = _load(REPTDATE_PQ)
_reptdate_val: date = _rd_df["REPTDATE"][0]

# SREPTDATE  = first day of current report month
_sreptdate     = _reptdate_val.replace(day=1)
# PRVRPTDATE  = last day of previous month
_prvrptdate    = _sreptdate - timedelta(days=1)
# SSREPTDATE  = first day of two months prior
_ssreptdate    = _prvrptdate.replace(day=1)
# PPRVRPTDATE = last day of two months prior
_pprvrptdate   = _ssreptdate - timedelta(days=1)

REPTMON   = f"{_reptdate_val.month:02d}"
REPTYEAR  = str(_reptdate_val.year)[-2:]
RDATE     = _reptdate_val.strftime("%d/%m/%y")    # DDMMYY8. -> DD/MM/YY
PREPTMON  = f"{_prvrptdate.month:02d}"
PPREPTMON = f"{_pprvrptdate.month:02d}"

# ============================================================================
# FORMAT DICTIONARIES
# (PROC FORMAT equivalents)
# ============================================================================

# VALUE DESC: numeric -> label
DESC_FMT = {1: "CHEQUES ISSUED"}

# VALUE TCODE: trancode -> description
TCODE_FMT = {
    310: "LOAN DISBURSEMENT",
    750: "PRINCIPAL INCREASE (PROGRESSIVE LOAN RELEASE)",
    752: "DEBITING FOR INSURANCE PREMIUM",
    753: "DEBITING FOR LEGAL FEE",
    754: "DEBITING FOR OTHER PAYMENTS",
    760: "MANUAL FEE ASSESSMENT FOR PAYMENT TO 3RD PARTY",
}

# VALUE $FEEFMT: feeplan -> description
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
# DATA BNM.DPLD  -- combine three months of DPLD
# ============================================================================
def _load_dpld(mon: str) -> pl.DataFrame:
    path = os.path.join(INPUT_DIR, f"DPLD{mon}.parquet")
    return _load(path)

dpld = pl.concat([
    _load_dpld(REPTMON),
    _load_dpld(PREPTMON),
    _load_dpld(PPREPTMON),
], how="diagonal")

# ============================================================================
# DATA BNM.LNLD
# Original SAS reads a fixed-width flat file; here we read the pre-converted
# parquet. The COSTCTR filter for PBB: exclude if outside 3000-3999 and not in
# (4043, 4048)  i.e. keep only rows where:
#   (COSTCTR >= 3000 AND COSTCTR <= 3999) OR COSTCTR IN (4043, 4048)
# Columns retained: ACCTNO, NOTENO, COSTCTR, NOTETYPE, TRANDT, TRANCODE,
#                   SEQNO, FEEPLAN, FEENO, TRANAMT, SOURCE
# ============================================================================
lnld = (
    _load(LNLD_PQ)
    .filter(
        (pl.col("COSTCTR").is_between(3000, 3999)) |
        pl.col("COSTCTR").is_in([4043, 4048])
    )
)

# ============================================================================
# PROC SORT + DATA BNM.TRANX  -- inner-join LNLD x DPLD on ACCTNO TRANDT TRANAMT
# ============================================================================
dpld_s = dpld.sort(["ACCTNO", "TRANDT", "TRANAMT"])
lnld_s = lnld.sort(["ACCTNO", "TRANDT", "TRANAMT"])

tranx_bnm = lnld_s.join(dpld_s, on=["ACCTNO", "TRANDT", "TRANAMT"], how="inner")

# ============================================================================
# DATA TRANX  -- FOR PBB
# ============================================================================
def _trnxdesc(row: dict) -> str:
    """Derive TRNXDESC from TRANCODE and FEEPLAN."""
    tc = row.get("TRANCODE")
    fp = str(row.get("FEEPLAN") or "").strip()
    if tc == 760 and fp:
        return FEEFMT.get(fp, str(tc))
    return TCODE_FMT.get(tc, str(tc) if tc is not None else "")

tranx = (
    tranx_bnm
    .with_columns([
        (pl.col("TRANAMT") / 1000).alias("TRANAMT1"),
        pl.lit(1).cast(pl.Int64).alias("VALUE"),
    ])
    .with_columns(
        pl.struct(["TRANCODE", "FEEPLAN"])
          .map_elements(_trnxdesc, return_dtype=pl.String)
          .alias("TRNXDESC")
    )
)

# ============================================================================
# OUTPUT REPORT LINES
# ============================================================================
lines: list[str] = []

def _sep(char: str = "-", width: int = 80) -> str:
    return char * width

def _hdr(title3_suffix: str, report_id: str = "EIBQEPC1",
         bank: str = "PUBLIC BANK BERHAD") -> list[str]:
    return [
        f"REPORT ID : {report_id}",
        bank,
        f"{title3_suffix} {RDATE}",
        "",
    ]

# ============================================================================
# REPORT 1: PROC TABULATE - TOTAL CHEQUES ISSUED
# CLASS VALUE; VAR TRANAMT1;
# TABLE VALUE='', TRANAMT1=''*(N='NUMBER OF CHEQUES' SUM="VALUE OF CHEQUES (RM'000)")
# ============================================================================
_total_n   = tranx.height
_total_sum = tranx["TRANAMT1"].sum()

lines += _hdr("CHEQUES ISSUED BY THE BANK AS AT")
lines.append("VALUE OF CHEQUES HEADER")
lines.append(_sep())
lines.append(
    f"{'CHEQUES ISSUED':<30}{_total_n:>16}  {_total_sum:>25,.3f}"
)
lines.append("")
lines.append("")

# ============================================================================
# REPORT 2: TOP FIVE BY NUMBER OF CHEQUES
# PROC SUMMARY by TRNXDESC -> sort DESC by _FREQ_ -> add COUNT rank
# TABLE COUNT*TRNXDESC, TRANAMT1*(N='UNIT' SUM="VALUE (RM'000)")
# ============================================================================
_tran1_n = (
    tranx
    .group_by("TRNXDESC")
    .agg([
        pl.len().alias("UNIT"),
        pl.col("TRANAMT1").sum().alias("TRANAMT1"),
    ])
    .sort("UNIT", descending=True)
    .with_row_index("COUNT", offset=1)
)

# Merge COUNT back to TRANX for the top-5 table
_tranx1 = (
    tranx
    .join(_tran1_n.select(["TRNXDESC", "COUNT"]), on="TRNXDESC", how="left")
    .sort("COUNT")
)

# Build table: for each unique COUNT/TRNXDESC combo, compute N and SUM
_tbl2 = (
    _tranx1
    .group_by(["COUNT", "TRNXDESC"])
    .agg([
        pl.len().alias("UNIT"),
        pl.col("TRANAMT1").sum().alias("VALUE"),
    ])
    .sort("COUNT")
)

lines += _hdr("TOP FIVE(5) PAYMENTS BY NUMBER OF CHEQUES AS AT")
_hdr_val = "VALUE (RM'000)"
lines.append(f"{'NO':<4}{'PURPOSE':<50}{'UNIT':>16}  {_hdr_val:>16}")
lines.append(_sep())
for _r in _tbl2.to_dicts():
    lines.append(
        f"{_r['COUNT']:<4}{str(_r['TRNXDESC']):<50}"
        f"{_r['UNIT']:>16}  {_r['VALUE']:>16,.3f}"
    )
lines.append("")
lines.append("")

# ============================================================================
# REPORT 3: TOP FIVE BY VALUE OF CHEQUES
# PROC SUMMARY by TRNXDESC -> sort DESC by TRANAMT1 -> add COUNT rank
# TABLE COUNT*TRNXDESC, TRANAMT1*(N='UNIT' SUM="VALUE (RM'000)")
# ============================================================================
_tran1_v = (
    tranx
    .group_by("TRNXDESC")
    .agg([
        pl.len().alias("UNIT"),
        pl.col("TRANAMT1").sum().alias("TRANAMT1"),
    ])
    .sort("TRANAMT1", descending=True)
    .with_row_index("COUNT", offset=1)
)

_tranx2 = (
    tranx
    .join(_tran1_v.select(["TRNXDESC", "COUNT"]), on="TRNXDESC", how="left")
    .sort("COUNT")
)

_tbl3 = (
    _tranx2
    .group_by(["COUNT", "TRNXDESC"])
    .agg([
        pl.len().alias("UNIT"),
        pl.col("TRANAMT1").sum().alias("VALUE"),
    ])
    .sort("COUNT")
)

lines += _hdr("TOP FIVE(5) PAYMENTS BY VALUE OF CHEQUES AS AT")
_hdr_val = "VALUE (RM'000)"
lines.append(f"{'NO':<4}{'PURPOSE':<50}{'UNIT':>16}  {_hdr_val:>16}")
lines.append(_sep())
for _r in _tbl3.to_dicts():
    lines.append(
        f"{_r['COUNT']:<4}{str(_r['TRNXDESC']):<50}"
        f"{_r['UNIT']:>16}  {_r['VALUE']:>16,.3f}"
    )
lines.append("")

# ============================================================================
# WRITE OUTPUT
# ============================================================================
with open(REPORT_TXT, "w") as f:
    f.write("\n".join(lines) + "\n")

# Also print to stdout
print("\n".join(lines))

con.close()
