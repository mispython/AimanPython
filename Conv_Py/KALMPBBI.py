#!/usr/bin/env python3
"""
Program : KALMPBBI.py (ISLAMIC)
Date    : 29/09/05
Report  : RDAL PART II (KAPITI ITEMS)

Converts and processes Islamic bank capital market transactions from multiple
source tables (K1TBL, K2TBL, K3TBL), applies BNM code classification logic,
and produces a summarised KALM dataset by BNMCODE and AMTIND.

Dependencies:
    - PBBDPFMT : Provides fdrmmt_format (SAS: FDRMMT.) and fdorgmt_format (SAS: FDORGMT.)
    - KALMPBBF : Provides format_kremmth (SAS: KREMMTH.) and format_orimat (SAS: ORIMAT.)
                 Note: KALMPBBF formats (ORIMAT/KREMMTH) are for conventional bank.
                 This Islamic program uses FDRMMT (from PBBDPFMT) for K3TBL1 remain-month
                 bucketing, and FDORGMT (from PBBDPFMT) for K3TBL2 original-term bucketing.
"""

from __future__ import annotations

import struct
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# PBBDPFMT provides FDRMMT and FDORGMT format functions
# SAS: %INC PGM(PBBDPFMT);
from PBBDPFMT import fdrmmt_format, fdorgmt_format

# KALMPBBF is included for format completeness (ORIMAT, KREMMTH) but
# those formats are used in the conventional KALMPBB program, not here.
# The Islamic KALMPBBI program uses FDRMMT./FDORGMT. from PBBDPFMT instead.
# SAS: %INC PGM(KALMPBBF);
# from KALMPBBF import format_kremmth, format_orimat  # not called in this program

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Macro variable equivalents
# &REPTMON  — reporting month (YYYYMM string, e.g. "200509")
# &NOWK     — week suffix (e.g. "")
# &RDATE    — reporting date as SAS date integer (days since 1960-01-01)
REPTMON: str = "200509"
NOWK:    str = ""
RDATE:   int = 16709  # SAS date for 2005-09-30; override at runtime

# Input file paths (binary / parquet as per project convention)
BNMTBL2_PATH   = INPUT_DIR  / f"BNMTBL2.dat"          # raw binary input for K2TBL
K1TBL_PARQUET  = INPUT_DIR  / f"K1TBL{REPTMON}{NOWK}.parquet"   # BNMK.K1TBL
K3TBL_PARQUET  = INPUT_DIR  / f"K3TBL{REPTMON}{NOWK}.parquet"   # BNMK.K3TBL

# Output
KALM_OUTPUT    = OUTPUT_DIR / f"KALM{REPTMON}{NOWK}.parquet"
REPORT_OUTPUT  = OUTPUT_DIR / f"KALMPBBI_{REPTMON}{NOWK}.txt"

# Page layout for ASA report
PAGE_LENGTH = 60


# ---------------------------------------------------------------------------
# SAS date helpers
# ---------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)

def sas_date_to_python(sas_days: Optional[int]) -> Optional[date]:
    """Convert SAS date (days since 1960-01-01) to Python date."""
    if sas_days is None:
        return None
    from datetime import timedelta
    return SAS_EPOCH + __import__("datetime").timedelta(days=sas_days)

def python_date_to_sas(d: Optional[date]) -> Optional[int]:
    """Convert Python date to SAS date integer."""
    if d is None:
        return None
    return (d - SAS_EPOCH).days


# ---------------------------------------------------------------------------
# MACRO DCLVAR — days per month arrays
# ---------------------------------------------------------------------------
# RETAIN D1-D12 31 D4 D6 D9 D11 30
#        RD1-RD12 MD1-MD12 31 RD2 MD2 28 RD4 RD6 RD9 RD11
#        MD4 MD6 MD9 MD11 30 RPYR RPMTH RPDAY 0;
_DEFAULT_LDAY  = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]  # D1-D12
_DEFAULT_RPDAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]  # RD1-RD12


# ---------------------------------------------------------------------------
# MACRO REMMTH — calculate remaining months from report date to maturity
# ---------------------------------------------------------------------------
def calc_remmth(
    matdt: date,
    reptdate: date,
    rpdays_arr: list[int],
) -> float:
    """
    Python equivalent of %REMMTH macro.

    Computes fractional remaining months between reptdate and matdt.
    Mirrors SAS logic:
        REMY  = YEAR(MATDT)  - RPYR
        REMM  = MONTH(MATDT) - RPMTH
        REMD  = DAY(MATDT)   - RPDAY   (capped to RPDAYS(RPMTH))
        REMMTH = REMY*12 + REMM + REMD/RPDAYS(RPMTH)
    """
    mdyr  = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    # Local MDDAYS equivalent: days in maturity month
    # (MD2 adjusted for leap year of maturity year)
    mddays = _DEFAULT_LDAY[:]
    if mdmth == 2:
        mddays[1] = 29 if (mdyr % 4 == 0) else 28

    # RPDAYS for reporting month (RD2 adjusted for report year leap)
    rpdays = rpdays_arr[:]
    if rpyr % 4 == 0:
        rpdays[1] = 29

    # Cap MDDAY to RPDAYS(RPMTH)  [SAS: 1-indexed month]
    rp_mth_days = rpdays[rpmth - 1]
    if mdday > rp_mth_days:
        mdday = rp_mth_days

    remy = mdyr  - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    return remy * 12 + remm + remd / rp_mth_days


# ---------------------------------------------------------------------------
# Read BNMTBL2 binary file and build K2TBL DataFrame
# ---------------------------------------------------------------------------
def read_bnmtbl2(filepath: Path, reptmon: str, nowk: str) -> pl.DataFrame:
    """
    Read BNMTBL2 binary file and return K2TBL DataFrame.

    SAS layout (1-indexed column positions):
        @1   OMABD   $4     Deal account branch
        @5   OMAND   $6     Deal a/c basic number
        @11  OMASD   $3     Deal a/c suffix
        @16  OSBDT   $1     Basic deal type
        @17  ORINWP  PD16.16 (packed decimal 16 bytes)
        @33  SALEAMT PD16.10 (packed decimal 16 bytes)
        @49  ORINWR  PD16.16 (packed decimal 16 bytes)
        @65  PURCAMT PD16.10 (packed decimal 16 bytes)
        @90  GFCTP   $2     Customer type
        @94  GFSAC   $2     Sundry analysis code
        @96  GFCNAL  $2     Residence country
        @98  OMCCY   $3     CCY
        @105 OSDLP   $3     Deal type
        @135 OMMVT   $1     Movement type
        @136 OMMVTS  $1     Movement sub-type
        @161 OXPCCY  $3     Purchase CCY
        @164 OXSCCY  $3     Sale CCY

    First record (@109): REPTDATE YYMMDD6.
    """
    if not filepath.exists():
        # Return empty DataFrame with correct schema if file not present
        return pl.DataFrame({
            "OMABD":   pl.Series([], dtype=pl.Utf8),
            "OMAND":   pl.Series([], dtype=pl.Utf8),
            "OMASD":   pl.Series([], dtype=pl.Utf8),
            "OSBDT":   pl.Series([], dtype=pl.Utf8),
            "ORINWP":  pl.Series([], dtype=pl.Float64),
            "SALEAMT": pl.Series([], dtype=pl.Float64),
            "ORINWR":  pl.Series([], dtype=pl.Float64),
            "PURCAMT": pl.Series([], dtype=pl.Float64),
            "GFCTP":   pl.Series([], dtype=pl.Utf8),
            "GFSAC":   pl.Series([], dtype=pl.Utf8),
            "GFCNAL":  pl.Series([], dtype=pl.Utf8),
            "OMCCY":   pl.Series([], dtype=pl.Utf8),
            "OSDLP":   pl.Series([], dtype=pl.Utf8),
            "OMMVT":   pl.Series([], dtype=pl.Utf8),
            "OMMVTS":  pl.Series([], dtype=pl.Utf8),
            "OXPCCY":  pl.Series([], dtype=pl.Utf8),
            "OXSCCY":  pl.Series([], dtype=pl.Utf8),
        })

    def _unpack_pd(data: bytes, scale: int) -> float:
        """Unpack SAS packed-decimal (PD) field to float."""
        # Each nibble is a BCD digit; last nibble is sign (C/F=pos, D=neg)
        digits = []
        for byte in data:
            digits.append((byte >> 4) & 0x0F)
            digits.append(byte & 0x0F)
        sign_nibble = digits[-1]
        sign = -1 if sign_nibble == 0xD else 1
        int_digits = digits[:-1]
        value = 0
        for d in int_digits:
            value = value * 10 + d
        return sign * value / (10 ** scale)

    records = []
    reptdate_sas = None

    with open(filepath, "rb") as f:
        raw = f.read()

    # SAS fixed-length records; record length inferred from layout = 167 bytes minimum
    # First record is 109+ bytes for REPTDATE at col 109 (YYMMDD6. = 6 chars, 0-indexed col 108)
    # The INFILE likely uses LRECL; we assume LRECL=167 (max column used: @164 $3 = 166)
    LRECL = 167
    n_records = len(raw) // LRECL

    for i in range(n_records):
        rec = raw[i * LRECL:(i + 1) * LRECL]
        if len(rec) < LRECL:
            break

        if i == 0:
            # IF _N_=1 THEN INPUT @109 REPTDATE YYMMDD6.;
            # @109 is 1-indexed → 0-indexed [108:114]
            reptdate_str = rec[108:114].decode("ascii", errors="replace").strip()
            try:
                rd = datetime.strptime(reptdate_str, "%y%m%d").date()
                reptdate_sas = python_date_to_sas(rd)
            except ValueError:
                reptdate_sas = None
            continue  # first record only reads REPTDATE

        # Parse data records (0-indexed offsets = SAS col - 1)
        omabd  = rec[0:4].decode("ascii",   errors="replace").rstrip()
        omand  = rec[4:10].decode("ascii",  errors="replace").rstrip()
        omasd  = rec[10:13].decode("ascii", errors="replace").rstrip()
        osbdt  = rec[15:16].decode("ascii", errors="replace").rstrip()
        orinwp  = _unpack_pd(rec[16:32], 16)   # PD16.16
        saleamt = _unpack_pd(rec[32:48], 10)   # PD16.10
        orinwr  = _unpack_pd(rec[48:64], 16)   # PD16.16
        purcamt = _unpack_pd(rec[64:80], 10)   # PD16.10
        gfctp  = rec[89:91].decode("ascii",  errors="replace").rstrip()
        gfsac  = rec[93:95].decode("ascii",  errors="replace").rstrip()
        gfcnal = rec[95:97].decode("ascii",  errors="replace").rstrip()
        omccy  = rec[97:100].decode("ascii", errors="replace").rstrip()
        osdlp  = rec[104:107].decode("ascii",errors="replace").rstrip()
        ommvt  = rec[134:135].decode("ascii",errors="replace").rstrip()
        ommvts = rec[135:136].decode("ascii",errors="replace").rstrip()
        oxpccy = rec[160:163].decode("ascii",errors="replace").rstrip()
        oxsccy = rec[163:166].decode("ascii",errors="replace").rstrip()

        records.append({
            "OMABD":   omabd,
            "OMAND":   omand,
            "OMASD":   omasd,
            "OSBDT":   osbdt,
            "ORINWP":  orinwp,
            "SALEAMT": saleamt,
            "ORINWR":  orinwr,
            "PURCAMT": purcamt,
            "GFCTP":   gfctp,
            "GFSAC":   gfsac,
            "GFCNAL":  gfcnal,
            "OMCCY":   omccy,
            "OSDLP":   osdlp,
            "OMMVT":   ommvt,
            "OMMVTS":  ommvts,
            "OXPCCY":  oxpccy,
            "OXSCCY":  oxsccy,
        })

    if not records:
        return pl.DataFrame({
            "OMABD":   pl.Series([], dtype=pl.Utf8),
            "OMAND":   pl.Series([], dtype=pl.Utf8),
            "OMASD":   pl.Series([], dtype=pl.Utf8),
            "OSBDT":   pl.Series([], dtype=pl.Utf8),
            "ORINWP":  pl.Series([], dtype=pl.Float64),
            "SALEAMT": pl.Series([], dtype=pl.Float64),
            "ORINWR":  pl.Series([], dtype=pl.Float64),
            "PURCAMT": pl.Series([], dtype=pl.Float64),
            "GFCTP":   pl.Series([], dtype=pl.Utf8),
            "GFSAC":   pl.Series([], dtype=pl.Utf8),
            "GFCNAL":  pl.Series([], dtype=pl.Utf8),
            "OMCCY":   pl.Series([], dtype=pl.Utf8),
            "OSDLP":   pl.Series([], dtype=pl.Utf8),
            "OMMVT":   pl.Series([], dtype=pl.Utf8),
            "OMMVTS":  pl.Series([], dtype=pl.Utf8),
            "OXPCCY":  pl.Series([], dtype=pl.Utf8),
            "OXSCCY":  pl.Series([], dtype=pl.Utf8),
        })

    return pl.DataFrame(records)


# ---------------------------------------------------------------------------
# DATA K1TABL — from BNMK.K1TBL (conventional FX positions, Islamic branch filter)
# ---------------------------------------------------------------------------
def build_k1tabl(k1tbl_path: Path) -> pl.DataFrame:
    """
    DATA K1TABL (KEEP=BRANCH BNMCODE AMOUNT AMTIND);
    SET BNMK.K1TBL&REPTMON&NOWK (RENAME=(GWBALC=AMOUNT));

    Filters:
        - Exclude XAU currency (gold)
        - BRANCH > 2000 (Islamic branches)
        - GWCCY NE 'MYR' AND GWOCY NE 'MYR' AND GWMVT='P' AND GWMVTS='P'
    Assigns BNMCODE based on GWDLP (deal type) for FX spot/forward/swap products.
    Also outputs a rolled-up code for certain BNMCODE values.
    """
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{k1tbl_path}')").pl()
    con.close()

    # RENAME GWBALC -> AMOUNT
    if "GWBALC" in df.columns:
        df = df.rename({"GWBALC": "AMOUNT"})

    output_rows: list[dict] = []

    for row in df.iter_rows(named=True):
        # BRANCH = INPUT(SUBSTR(GWAB,1,4), 4.)
        gwab   = (row.get("GWAB") or "").ljust(4)
        try:
            branch = int(gwab[:4].strip())
        except ValueError:
            branch = 0

        # IF GWOCY='XAU' THEN DELETE;
        if (row.get("GWOCY") or "").strip() == "XAU":
            continue

        # AMTIND='I'
        amtind = "I"

        # IF BRANCH > 2000
        if branch <= 2000:
            continue

        gwccy  = (row.get("GWCCY")  or "").strip()
        gwocy  = (row.get("GWOCY")  or "").strip()
        gwmvt  = (row.get("GWMVT")  or "").strip()
        gwmvts = (row.get("GWMVTS") or "").strip()
        gwdlp  = (row.get("GWDLP")  or "").strip()
        amount = row.get("AMOUNT") or 0.0

        bnmcode = " "

        if gwccy != "MYR" and gwocy != "MYR" and gwmvt == "P" and gwmvts == "P":
            if gwdlp == "FXS":
                bnmcode = "5761000000000Y"
            elif gwdlp in ("FXO", "FXF"):
                bnmcode = "5762000000000Y"
            elif gwdlp in ("SF2", "FF1", "FF2"):
                bnmcode = "5763100000000Y"
            elif gwdlp in ("SF1", "TS1", "TS2"):
                bnmcode = "5763200000000Y"
            # OTHERWISE: bnmcode stays ' '

        if amount < 0:
            amount = abs(amount)

        if bnmcode != " ":
            output_rows.append({
                "BRANCH":  branch,
                "BNMCODE": bnmcode,
                "AMOUNT":  amount,
                "AMTIND":  amtind,
            })

        # IF BNMCODE IN ('5761000000000Y','5763100000000Y','5763200000000Y')
        # THEN DO; BNMCODE='5760000000000Y'; OUTPUT; END;
        if bnmcode in ("5761000000000Y", "5763100000000Y", "5763200000000Y"):
            output_rows.append({
                "BRANCH":  branch,
                "BNMCODE": "5760000000000Y",
                "AMOUNT":  amount,
                "AMTIND":  amtind,
            })

    if not output_rows:
        return pl.DataFrame({
            "BRANCH":  pl.Series([], dtype=pl.Int64),
            "BNMCODE": pl.Series([], dtype=pl.Utf8),
            "AMOUNT":  pl.Series([], dtype=pl.Float64),
            "AMTIND":  pl.Series([], dtype=pl.Utf8),
        })

    return pl.DataFrame(output_rows).with_columns(pl.col("BNMCODE").cast(pl.Utf8))


# ---------------------------------------------------------------------------
# BNM code assignment helpers for K2TABL
# ---------------------------------------------------------------------------
def _assign_k2_bnmcode_buy(osdlp: str, gfctp: str, gfsac: str, gfcnal: str) -> str:
    """
    Assigns BNM purchase-side code for K2TABL
    (OXPCCY NE 'MYR' AND OXSCCY EQ 'MYR', OMMVT='P', OMMVTS='P').
    """
    bnmcode = " "
    if osdlp == "FXS":
        if gfctp == "BC": bnmcode = "6850101000000Y"
        elif gfctp == "BB": bnmcode = "6850102000000Y"
        elif gfctp == "BI": bnmcode = "6850103000000Y"
        elif gfctp == "BM": bnmcode = "6850112000000Y"
        elif gfctp in ("BA", "BW", "BE"): bnmcode = "6850181000000Y"
        else:
            if not ("BA" <= gfctp <= "BZ") and gfsac != "UF" and gfcnal == "MY":
                bnmcode = "6850115000000Y"
            if gfsac == "UF":
                bnmcode = "6850185000000Y"
    elif osdlp in ("FXF", "FXO"):
        if gfctp == "BC": bnmcode = "6850201000000Y"
        elif gfctp == "BB": bnmcode = "6850202000000Y"
        elif gfctp == "BI": bnmcode = "6850203000000Y"
        elif gfctp == "BM": bnmcode = "6850212000000Y"
        elif gfctp in ("BA", "BW", "BE"): bnmcode = "6850281000000Y"
        else:
            if not ("BA" <= gfctp <= "BZ") and gfsac != "UF" and gfcnal == "MY":
                bnmcode = "6850215000000Y"
            if gfsac == "UF":
                bnmcode = "6850285000000Y"
    elif osdlp in ("TS1", "SF1", "FF1", "TS2", "SF2", "FF2"):
        if gfctp == "BC": bnmcode = "6850301000000Y"
        elif gfctp == "BB": bnmcode = "6850302000000Y"
        elif gfctp == "BI": bnmcode = "6850303000000Y"
        elif gfctp == "BM": bnmcode = "6850312000000Y"
        elif gfctp in ("BA", "BW", "BE"): bnmcode = "6850381000000Y"
        else:
            if not ("BA" <= gfctp <= "BZ") and gfsac != "UF" and gfcnal == "MY":
                bnmcode = "6850315000000Y"
            if gfsac == "UF":
                bnmcode = "6850385000000Y"
    return bnmcode


def _assign_k2_bnmcode_sell(osdlp: str, gfctp: str, gfsac: str, gfcnal: str) -> str:
    """
    Assigns BNM sale-side code for K2TABL
    (OXSCCY NE 'MYR' AND OXPCCY EQ 'MYR', OMMVT='P', OMMVTS='S').
    """
    bnmcode = " "
    if osdlp == "FXS":
        if gfctp == "BC": bnmcode = "7850101000000Y"
        elif gfctp == "BB": bnmcode = "7850102000000Y"
        elif gfctp == "BI": bnmcode = "7850103000000Y"
        elif gfctp == "BM": bnmcode = "7850112000000Y"
        elif gfctp in ("BA", "BW", "BE"): bnmcode = "7850181000000Y"
        else:
            if not ("BA" <= gfctp <= "BZ") and gfsac != "UF" and gfcnal == "MY":
                bnmcode = "7850115000000Y"
            if gfsac == "UF":
                bnmcode = "7850185000000Y"
    elif osdlp in ("FXF", "FXO"):
        if gfctp == "BC": bnmcode = "7850201000000Y"
        elif gfctp == "BB": bnmcode = "7850202000000Y"
        elif gfctp == "BI": bnmcode = "7850203000000Y"
        elif gfctp == "BM": bnmcode = "7850212000000Y"
        elif gfctp in ("BA", "BW", "BE"): bnmcode = "7850281000000Y"
        else:
            if not ("BA" <= gfctp <= "BZ") and gfsac != "UF" and gfcnal == "MY":
                bnmcode = "7850215000000Y"
            if gfsac == "UF":
                bnmcode = "7850285000000Y"
    elif osdlp in ("TS1", "SF1", "FF1", "TS2", "SF2", "FF2"):
        if gfctp == "BC": bnmcode = "7850301000000Y"
        elif gfctp == "BB": bnmcode = "7850302000000Y"
        elif gfctp == "BI": bnmcode = "7850303000000Y"
        elif gfctp == "BM": bnmcode = "7850312000000Y"
        elif gfctp in ("BA", "BW", "BE"): bnmcode = "7850381000000Y"
        else:
            if not ("BA" <= gfctp <= "BZ") and gfsac != "UF" and gfcnal == "MY":
                bnmcode = "7850315000000Y"
            if gfsac == "UF":
                bnmcode = "7850385000000Y"
    return bnmcode


# ---------------------------------------------------------------------------
# DATA K2TABL — from BNMTBL2 (K2TBL)
# ---------------------------------------------------------------------------
def build_k2tabl(k2tbl_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA K2TABL (KEEP=BRANCH BNMCODE AMOUNT AMTIND);
    SET K2TBL&REPTMON&NOWK;

    Filters BRANCH > 2000 and assigns BNM codes for buy/sell FX legs.
    """
    output_rows: list[dict] = []

    for row in k2tbl_df.iter_rows(named=True):
        # BRANCH = INPUT(SUBSTR(OMABD,1,4), 4.)
        omabd = (row.get("OMABD") or "").ljust(4)
        try:
            branch = int(omabd[:4].strip())
        except ValueError:
            branch = 0

        # IF BRANCH > 2000
        if branch <= 2000:
            continue

        amtind = "I"
        oxpccy = (row.get("OXPCCY") or "").strip()
        oxsccy = (row.get("OXSCCY") or "").strip()
        ommvt  = (row.get("OMMVT")  or "").strip()
        ommvts = (row.get("OMMVTS") or "").strip()
        osdlp  = (row.get("OSDLP")  or "").strip()
        gfctp  = (row.get("GFCTP")  or "").strip()
        gfsac  = (row.get("GFSAC")  or "").strip()
        gfcnal = (row.get("GFCNAL") or "").strip()
        purcamt = row.get("PURCAMT") or 0.0
        saleamt = row.get("SALEAMT") or 0.0

        # Purchase leg: OXPCCY NE 'MYR' AND OXSCCY EQ 'MYR', OMMVT='P', OMMVTS='P'
        if oxpccy != "MYR" and oxsccy == "MYR" and ommvt == "P" and ommvts == "P":
            bnmcode = _assign_k2_bnmcode_buy(osdlp, gfctp, gfsac, gfcnal)
            amount = purcamt
            if bnmcode != " ":
                output_rows.append({
                    "BRANCH":  branch,
                    "BNMCODE": bnmcode,
                    "AMOUNT":  amount,
                    "AMTIND":  amtind,
                })

        # Sale leg: OXSCCY NE 'MYR' AND OXPCCY EQ 'MYR', OMMVT='P', OMMVTS='S'
        if oxsccy != "MYR" and oxpccy == "MYR" and ommvt == "P" and ommvts == "S":
            bnmcode = _assign_k2_bnmcode_sell(osdlp, gfctp, gfsac, gfcnal)
            amount = saleamt
            if bnmcode != " ":
                output_rows.append({
                    "BRANCH":  branch,
                    "BNMCODE": bnmcode,
                    "AMOUNT":  amount,
                    "AMTIND":  amtind,
                })

    if not output_rows:
        return pl.DataFrame({
            "BRANCH":  pl.Series([], dtype=pl.Int64),
            "BNMCODE": pl.Series([], dtype=pl.Utf8),
            "AMOUNT":  pl.Series([], dtype=pl.Float64),
            "AMTIND":  pl.Series([], dtype=pl.Utf8),
        })

    return pl.DataFrame(output_rows)


# ---------------------------------------------------------------------------
# DATA K3TBL1 — Remaining maturity bucketing using FDRMMT format
# ---------------------------------------------------------------------------
def build_k3tbl1(k3tbl_path: Path, reptdate: date) -> pl.DataFrame:
    """
    DATA K3TBL1 (KEEP=BNMCODE AMOUNT AMTIND);
    SET BNMK.K3TBL&REPTMON&NOWK;

    Filters Islamic instrument types (UTSTY/UTREF = IFD/ILD/ISD/IZD, PFD/PLD/PSD/PZD).
    Computes remaining months from REPTDATE to MATDT.
    Applies FDRMMT. format (from PBBDPFMT) to bucket remaining months.
    BNMCODE = '4215000' || REMMT || '0000Y'
    """
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{k3tbl_path}')").pl()
    con.close()

    # REPTDATE = &RDATE  (macro variable, SAS date)
    reptdate_sas = python_date_to_sas(reptdate)

    # Build RPDAYS array for reporting month (leap-year aware)
    rpdays_arr = _DEFAULT_RPDAYS[:]
    if reptdate.year % 4 == 0:
        rpdays_arr[1] = 29

    utsty_valid = {"IFD", "ILD", "ISD", "IZD"}
    utref_valid = {"PFD", "PLD", "PSD", "PZD"}

    output_rows: list[dict] = []

    for row in df.iter_rows(named=True):
        # IF SUBSTR(UTREF,1,1) EQ 'I' AND SUBSTR(UTREF,1,4) NE '  '
        utref = (row.get("UTREF") or "").ljust(4)
        if utref[0] != "I":
            continue
        if utref[:4].strip() == "":
            continue

        amtind = "I"
        # AMOUNT = UTAMOC - UTDPF
        utamoc = row.get("UTAMOC") or 0.0
        utdpf  = row.get("UTDPF")  or 0.0
        amount = utamoc - utdpf

        # REPTDATE = &RDATE (SAS date as integer)
        # MATDT is a SAS date stored in parquet as integer
        matdt_sas = row.get("MATDT")

        bnmcode = " "

        utsty_val = (row.get("UTSTY") or "").strip()
        utref_val = utref[:3].strip()

        if utsty_val in utsty_valid and utref_val in utref_valid:
            if matdt_sas is not None and matdt_sas > reptdate_sas:
                matdt = sas_date_to_python(int(matdt_sas))
                remmth = calc_remmth(matdt, reptdate, rpdays_arr)
                # REMMT = PUT(REMMTH, FDRMMT.)  — from PBBDPFMT
                remmt = fdrmmt_format(remmth)
                bnmcode = "4215000" + remmt + "0000Y"

        if bnmcode != " ":
            output_rows.append({
                "BNMCODE": bnmcode,
                "AMOUNT":  amount,
                "AMTIND":  amtind,
            })

    if not output_rows:
        return pl.DataFrame({
            "BNMCODE": pl.Series([], dtype=pl.Utf8),
            "AMOUNT":  pl.Series([], dtype=pl.Float64),
            "AMTIND":  pl.Series([], dtype=pl.Utf8),
        })

    return pl.DataFrame(output_rows)


# ---------------------------------------------------------------------------
# DATA K3TBL2 — Original maturity bucketing using FDORGMT format
# (Weighted average NID rates for RDIR II)
# ---------------------------------------------------------------------------
def build_k3tbl2(k3tbl_path: Path) -> pl.DataFrame:
    """
    /* WEIGHTED AVERAGE NID RATES FOR RDIR II */
    DATA K3TBL2 (KEEP=BNMCODE AMOUNT AMTIND);
    SET BNMK.K3TBL&REPTMON&NOWK;

    Uses ISSDT as base date (issue date) instead of REPTDATE.
    Applies FDORGMT. format (from PBBDPFMT) to bucket original months.
    BNMCODE = '4215000' || ORIGMT || '0000Y'
    Excludes ORIGMT IN ('12','13').
    """
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{k3tbl_path}')").pl()
    con.close()

    utsty_valid = {"IFD", "ILD", "ISD", "IZD"}
    utref_valid = {"PFD", "PLD", "PSD", "PZD"}

    output_rows: list[dict] = []

    for row in df.iter_rows(named=True):
        # IF SUBSTR(UTREF,1,1) EQ 'I' AND SUBSTR(UTREF,1,4) NE '  '
        utref = (row.get("UTREF") or "").ljust(4)
        if utref[0] != "I":
            continue
        if utref[:4].strip() == "":
            continue

        amtind = "I"
        utamoc = row.get("UTAMOC") or 0.0
        utdpf  = row.get("UTDPF")  or 0.0
        amount = utamoc - utdpf

        # IF ISSDT=' ' THEN ISSDT=REPTDATE
        issdt_sas = row.get("ISSDT")
        if issdt_sas is None or issdt_sas == "" or issdt_sas == 0:
            # Fallback to REPTDATE global
            issdt_sas = RDATE
        issdt = sas_date_to_python(int(issdt_sas))

        # Build RPDAYS from ISSDT (issue date) for RPMTH/RPDAY reference
        rpdays_arr = _DEFAULT_RPDAYS[:]
        if issdt.year % 4 == 0:
            rpdays_arr[1] = 29

        matdt_sas = row.get("MATDT")

        utsty_val = (row.get("UTSTY") or "").strip()
        utref_val = utref[:3].strip()

        if utsty_val in utsty_valid and utref_val in utref_valid:
            if matdt_sas is not None:
                matdt = sas_date_to_python(int(matdt_sas))
                # %REMMTH computed from ISSDT as base (not REPTDATE)
                remmth = calc_remmth(matdt, issdt, rpdays_arr)
                # ORIGMT = PUT(REMMTH, FDORGMT.)  — from PBBDPFMT
                origmt = fdorgmt_format(remmth)
                # IF ORIGMT ^ IN ('12','13');
                if origmt not in ("12", "13"):
                    bnmcode = "4215000" + origmt + "0000Y"
                    output_rows.append({
                        "BNMCODE": bnmcode,
                        "AMOUNT":  amount,
                        "AMTIND":  amtind,
                    })

    if not output_rows:
        return pl.DataFrame({
            "BNMCODE": pl.Series([], dtype=pl.Utf8),
            "AMOUNT":  pl.Series([], dtype=pl.Float64),
            "AMTIND":  pl.Series([], dtype=pl.Utf8),
        })

    return pl.DataFrame(output_rows)


# ---------------------------------------------------------------------------
# Report formatter — PROC PRINT equivalent with ASA carriage control
# ---------------------------------------------------------------------------
def write_report(df: pl.DataFrame, reptdate: date, output_path: Path) -> None:
    """
    Produces a text report equivalent to:
        TITLE2 'ISLAMIC DOMESTIC ASSETS & LIABILITIES (KAPITI)-PART II';
        TITLE3 'REPORT DATE : ' &RDATE;
        PROC PRINT DATA=KALM&REPTMON&NOWK;
        FORMAT AMOUNT COMMA30.2;

    Output includes ASA carriage control characters.
    Page length: 60 lines per page.
    """
    title1 = ""
    title2 = "ISLAMIC DOMESTIC ASSETS & LIABILITIES (KAPITI)-PART II"
    title3 = f"REPORT DATE : {reptdate.strftime('%d%b%Y').upper()}"

    header_lines = [
        f" {title2}",
        f" {title3}",
        " ",
        f" {'OBS':<6} {'BNMCODE':<16} {'AMTIND':<8} {'AMOUNT':>30}",
        " " + "-" * 64,
    ]

    HEADER_ROWS = len(header_lines)
    DATA_ROWS_PER_PAGE = PAGE_LENGTH - HEADER_ROWS

    lines: list[str] = []
    obs = 0

    for row in df.iter_rows(named=True):
        obs += 1
        bnmcode = (row.get("BNMCODE") or "").ljust(14)
        amtind  = (row.get("AMTIND")  or "").ljust(1)
        amount  = row.get("AMOUNT") or 0.0
        # FORMAT AMOUNT COMMA30.2  — comma-formatted, 30 wide, 2 decimal places
        amount_str = f"{amount:>30,.2f}"
        lines.append(f" {obs:<6} {bnmcode:<16} {amtind:<8} {amount_str}")

    with open(output_path, "w", encoding="ascii", errors="replace") as f:
        page_num = 0
        i = 0
        total_lines = len(lines)

        while i <= total_lines:
            page_num += 1
            # ASA '1' = form feed / new page
            for h_idx, hline in enumerate(header_lines):
                asa = "1" if (h_idx == 0) else " "
                f.write(asa + hline + "\n")

            # Write data lines for this page
            page_data = lines[i:i + DATA_ROWS_PER_PAGE]
            for dline in page_data:
                f.write(" " + dline + "\n")

            i += DATA_ROWS_PER_PAGE

            if i >= total_lines:
                break


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # Resolve reporting date from RDATE macro variable
    reptdate = sas_date_to_python(RDATE)

    # -----------------------------------------------------------------------
    # DATA K2TBL — read BNMTBL2 binary file
    # -----------------------------------------------------------------------
    k2tbl_df = read_bnmtbl2(BNMTBL2_PATH, REPTMON, NOWK)

    # -----------------------------------------------------------------------
    # DATA K1TABL
    # -----------------------------------------------------------------------
    k1tabl = build_k1tabl(K1TBL_PARQUET)

    # -----------------------------------------------------------------------
    # DATA K2TABL
    # -----------------------------------------------------------------------
    k2tabl = build_k2tabl(k2tbl_df)

    # -----------------------------------------------------------------------
    # DATA K3TBL1 + K3TBL2 -> K3TABL
    # -----------------------------------------------------------------------
    k3tbl1 = build_k3tbl1(K3TBL_PARQUET, reptdate)
    k3tbl2 = build_k3tbl2(K3TBL_PARQUET)

    # DATA K3TABL; SET K3TBL1 K3TBL2;
    k3tabl = pl.concat([k3tbl1, k3tbl2], how="diagonal")

    # -----------------------------------------------------------------------
    # DATA KALM — SET K1TABL K2TABL K3TABL (DROP=BRANCH)
    # -----------------------------------------------------------------------
    # Align schemas: drop BRANCH from K1TABL and K2TABL before concat
    k1_nodrop = k1tabl.drop("BRANCH") if "BRANCH" in k1tabl.columns else k1tabl
    k2_nodrop = k2tabl.drop("BRANCH") if "BRANCH" in k2tabl.columns else k2tabl

    kalm = pl.concat([k1_nodrop, k2_nodrop, k3tabl], how="diagonal")

    # -----------------------------------------------------------------------
    # PROC APPEND DATA=KALM BASE=KALM  (no-op: appending dataset to itself
    # is a SAS artifact that has no effect in a fresh run; data already merged)
    # -----------------------------------------------------------------------

    # -----------------------------------------------------------------------
    # PROC SUMMARY DATA=KALM NWAY; CLASS BNMCODE AMTIND; VAR AMOUNT; SUM=AMOUNT
    # -----------------------------------------------------------------------
    kalm_summary = (
        kalm
        .group_by(["BNMCODE", "AMTIND"])
        .agg(pl.col("AMOUNT").sum().alias("AMOUNT"))
        .sort(["BNMCODE", "AMTIND"])
    )

    # Save output parquet
    kalm_summary.write_parquet(KALM_OUTPUT)
    print(f"Written: {KALM_OUTPUT}  ({kalm_summary.height} rows)")

    # -----------------------------------------------------------------------
    # PROC PRINT report with ASA carriage control
    # -----------------------------------------------------------------------
    write_report(kalm_summary, reptdate, REPORT_OUTPUT)
    print(f"Report : {REPORT_OUTPUT}")


if __name__ == "__main__":
    main()
