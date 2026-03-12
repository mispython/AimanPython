#!/usr/bin/env python3
"""
Program : KALWPBBI.py (ISLAMIC)
Date    : 29/09/05
Report  : RDAL PART I  (KAPITI ITEMS)
Report  : RDIR PART I  (KAPITI ITEMS)

Reads K1TBL (KAPITI weekly transaction table) and K3TBL (unit trust table),
applies BNM code classification rules, consolidates, and prints an Islamic
Domestic Assets and Liabilities report (Part I).

Dependencies:
    KALWPBBF  - provides the KALW parquet reference data (BNMCODE list).
                This program does not call KALWPBBF directly; the KALW
                reference table is read from its output parquet if needed.
                (Original SAS used %INC PGM(KALWPBBF) to load formats only.)
"""

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import polars as pl

# Input parquet directories
BNMK_DIR   = Path("data/BNMK")   # BNMK library  (K1TBL, K3TBL)

# Output
OUTPUT_DIR  = Path("output")
OUTPUT_FILE = OUTPUT_DIR / "KALWPBBI.txt"

# Runtime parameters – set before execution
REPTMON = "202412"    # Reporting month  (e.g. "202412")
NOWK    = "01"        # Week suffix      (e.g. "01")
RDATE   = "31DEC2024"

# Page length for ASA carriage-control report
PAGE_LINES = 60


# ============================================================================
# HELPERS – FILE PATHS
# ============================================================================

def _parquet(lib_dir: Path, name: str) -> Path:
    return lib_dir / f"{name}{REPTMON}{NOWK}.parquet"


# ============================================================================
# HELPER – COUNTMON  (SAS LINK COUNTMON equivalent)
# Counts the number of complete calendar months between GWSDT and GWMDT.
# Returns None if dates are invalid or GWMDT < GWSDT.
# ============================================================================

def _countmon(gwsdt: Optional[date], gwmdt: Optional[date]) -> Optional[int]:
    """
    Replicate the SAS COUNTMON subroutine.
    Counts calendar-aligned month increments from gwsdt until folmonth >= gwmdt.
    Returns the count (NUMMONTH) or None if dates are missing/invalid.
    """
    if gwsdt is None or gwmdt is None:
        return None
    if gwmdt < gwsdt:
        return None

    folmonth  = gwsdt
    nummonth  = 0
    start_day = gwsdt.day

    while gwmdt > folmonth:
        nummonth += 1
        next_mon  = folmonth.month + 1
        next_year = folmonth.year
        if next_mon > 12:
            next_mon  -= 12
            next_year += 1

        # Replicate SAS date arithmetic for end-of-month adjustments
        if start_day in (29, 30, 31) and next_mon == 2:
            # Last day of February
            first_march = date(next_year, 3, 1)
            folmonth = first_march - timedelta(days=1)
        elif start_day == 31 and next_mon in (4, 6, 9, 11):
            folmonth = date(next_year, next_mon, 30)
        elif (start_day == 30
              and gwsdt.month in (4, 6, 9, 11)
              and next_mon in (1, 3, 5, 7, 8, 10, 12)):
            folmonth = date(next_year, next_mon, 31)
        else:
            try:
                folmonth = date(next_year, next_mon, start_day)
            except ValueError:
                # Clamp to last valid day of month
                import calendar
                last = calendar.monthrange(next_year, next_mon)[1]
                folmonth = date(next_year, next_mon, last)

    return nummonth


# ============================================================================
# HELPER – BA-BZ range check
# ============================================================================

def _is_ba_bz(gwctp: Optional[str]) -> bool:
    """Return True if gwctp is in the range 'BA' <= gwctp <= 'BZ'."""
    if not gwctp:
        return False
    return "BA" <= gwctp <= "BZ"


# ============================================================================
# K1TABL PROCESSING  (DATA K1TABL step)
# ============================================================================

def _process_k1tabl(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply all BNM-code classification rules to K1TBL rows.
    Returns a DataFrame with columns: BRANCH, BNMCODE, AMOUNT, AMTIND.

    Rules are applied per-row in Python to faithfully reproduce the
    SAS LINK / RETURN / OUTPUT pattern, including multiple OUTPUTs per row.
    """
    results: list[dict] = []

    for row in df.iter_rows(named=True):
        # ---- Derived fields ------------------------------------------------
        gwab    = str(row.get("GWAB",   "") or "").strip()
        branch  = int(gwab[:4]) if gwab[:4].isdigit() else 0
        amount  = row.get("GWBALC", 0.0) or 0.0
        gwccy   = str(row.get("GWCCY",  "") or "").strip()
        gwmvt   = str(row.get("GWMVT",  "") or "").strip()
        gwmvts  = str(row.get("GWMVTS", "") or "").strip()
        gwdlp   = str(row.get("GWDLP",  "") or "").strip()
        gwctp   = str(row.get("GWCTP",  "") or "").strip()
        gwcnal  = str(row.get("GWCNAL", "") or "").strip()
        gwsac   = str(row.get("GWSAC",  "") or "").strip()
        gwact   = str(row.get("GWACT",  "") or "").strip()
        gwciac  = row.get("GWCIAC", 0.0) or 0.0
        gwdiac  = row.get("GWDIAC", 0.0) or 0.0
        gwocy   = str(row.get("GWOCY",  "") or "").strip()
        gwshn   = str(row.get("GWSHN",  "") or "").strip()
        gwan    = str(row.get("GWAN",   "") or "").strip()
        gwas    = str(row.get("GWAS",   "") or "").strip()

        # Parse SAS dates (stored as integers YYYYMMDD or SAS date integers)
        def _to_date(v) -> Optional[date]:
            if not v:
                return None
            try:
                s = str(int(v))
                if len(s) == 8:
                    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
            except Exception:
                pass
            return None

        gwsdt = _to_date(row.get("GWSDT"))
        gwmdt = _to_date(row.get("GWMDT"))

        gwdlp2 = gwdlp[1:3] if len(gwdlp) >= 3 else ""

        # AMTIND is always 'I' for this program
        amtind = "I"

        # Filters from DATA step
        if branch <= 2000:
            continue

        # ---- DP32000 --------------------------------------------------------
        if (gwccy == "MYR" and gwmvt == "P" and gwmvts == "M"
                and (gwdlp in ("FDA", "FDB", "FDL", "FDS", "LC", "LO")
                     or gwdlp2 in ("XI", "XT"))):

            bnmcode = ""

            if gwdlp in ("FDA", "FDB", "FDL", "FDS"):
                if not _is_ba_bz(gwctp) and gwcnal == "MY":
                    bnmcode = "3213020000000Y"
                elif gwctp == "BB":
                    bnmcode = "3213002000000Y"
                elif gwctp == "BQ":
                    bnmcode = "3213011000000Y"
                elif gwctp == "BM":
                    bnmcode = "3213012000000Y"

            if gwdlp2 in ("XI", "XT"):
                if not _is_ba_bz(gwctp) and gwcnal == "MY":
                    bnmcode = "3250020000000Y"
                elif gwctp == "BB":
                    bnmcode = "3250002000000Y"
                elif gwctp == "BI":
                    bnmcode = "3250003000000Y"
                elif gwctp == "BQ":
                    bnmcode = "3250011000000Y"
                elif gwctp == "BM":
                    bnmcode = "3250012000000Y"
                elif gwctp == "BN":
                    results.append({"BRANCH": branch, "BNMCODE": "3250013000000Y",
                                    "AMOUNT": amount, "AMTIND": amtind})
                    bnmcode = "3250020000000Y"
                elif gwctp == "BG":
                    results.append({"BRANCH": branch, "BNMCODE": "3250017000000Y",
                                    "AMOUNT": amount, "AMTIND": amtind})
                    bnmcode = "3250020000000Y"
                elif gwctp in ("BA", "BW", "BE"):
                    bnmcode = "3250081000000Y"

            if gwdlp == "LC" and gwctp == "BN":
                bnmcode = "3214013000000Y"

            if bnmcode:
                results.append({"BRANCH": branch, "BNMCODE": bnmcode,
                                 "AMOUNT": amount, "AMTIND": amtind})

        # ---- AF33000 --------------------------------------------------------
        if gwdlp in (" ", "LO", "LS", "LF") or gwact == "CN":
            bnmcode = ""

            if gwdlp in ("LO", "LS", "LF"):
                if gwccy == "MYR" and gwmvt == "P" and gwmvts == "M":
                    if gwctp == "BC":
                        bnmcode = "3314001000000Y"
                    elif gwctp == "BB":
                        bnmcode = "3314002000000Y"
                    elif gwctp == "BI":
                        bnmcode = "3314003000000Y"
                    elif gwctp == "BQ":
                        bnmcode = "3314011000000Y"
                    elif gwctp == "BM":
                        bnmcode = "3314012000000Y"
                    elif gwctp == "BN":
                        pass  # empty DO block in original
                    elif gwctp == "BG":
                        bnmcode = "3314017000000Y"
                    elif gwctp in ("BA", "BW"):
                        if gwdlp in ("LO", "LS"):
                            nm = _countmon(gwsdt, gwmdt)
                            if nm is not None and nm <= 12:
                                bnmcode = "3314081100000Y"
                        if gwdlp == "LF":
                            nm = _countmon(gwsdt, gwmdt)
                            if nm is not None and nm > 12:
                                bnmcode = "3314081200000Y"

                if gwccy != "MYR" and gwmvt == "P" and gwmvts == "M":
                    if gwctp == "BC":
                        bnmcode = "3364001000000Y"
                    elif gwctp == "BB":
                        bnmcode = "3364002000000Y"
                    elif gwctp == "BI":
                        bnmcode = "3364003000000Y"
                    elif gwctp == "BM":
                        bnmcode = "3364012000000Y"
                    elif gwctp in ("BA", "BW", "BE"):
                        if gwdlp == "LF":
                            nm = _countmon(gwsdt, gwmdt)
                            if nm is not None and nm > 12:
                                bnmcode = "3364081200000Y"

            # BN block (commented-out OUTPUT in original – preserved as no-op)
            if (gwdlp in ("LS", "LF", "LO") and gwccy == "MYR"
                    and gwmvt == "P" and gwmvts == "M" and gwctp == "BN"):
                nm = _countmon(gwsdt, gwmdt)
                if nm is not None and nm <= 12:
                    pass  # BNMCODE='3314013000000Y' OUTPUT; -- commented out

            if bnmcode:
                results.append({"BRANCH": branch, "BNMCODE": bnmcode,
                                 "AMOUNT": amount, "AMTIND": amtind})

            # ---- LN34000 ---------------------------------------------------
            bnmcode = ""
            if (gwdlp in ("LO", "LS", "LF") and gwccy != "MYR"
                    and gwmvt == "P" and gwmvts == "M"
                    and not _is_ba_bz(gwctp)
                    and gwsac != "UF" and gwcnal == "MY"):
                bnmcode = "3460015000000Y"
            if (gwdlp in ("LO", "LS", "LF") and gwccy != "MYR"
                    and gwmvt == "P" and gwmvts == "M"
                    and gwsac == "UF"):
                bnmcode = "3460085000000Y"
            if bnmcode:
                results.append({"BRANCH": branch, "BNMCODE": bnmcode,
                                 "AMOUNT": amount, "AMTIND": amtind})

        # ---- MA39000 -------------------------------------------------------
        # Block is entirely commented-out in original SAS; no output produced.

        # ---- DA42160 -------------------------------------------------------
        if (gwdlp2 in ("MI", "MT") and gwccy == "MYR"
                and gwmvt == "P" and gwmvts == "M"):
            bnmcode = ""

            if not _is_ba_bz(gwctp) and gwcnal == "MY":
                bnmcode = "4216020000000Y"
            elif not _is_ba_bz(gwctp) and gwcnal != "MY":
                bnmcode = "4216085000000Y"
            else:
                if gwctp == "BC":
                    bnmcode = "4216001000000Y"
                elif gwctp == "BB":
                    bnmcode = "4216002000000Y"
                elif gwctp == "BI":
                    bnmcode = "4216003000000Y"
                elif gwctp == "BQ":
                    bnmcode = "4216011000000Y"
                elif gwctp == "BM":
                    bnmcode = "4216012000000Y"
                elif gwctp == "BN":
                    results.append({"BRANCH": branch, "BNMCODE": "4216013000000Y",
                                    "AMOUNT": amount, "AMTIND": amtind})
                    bnmcode = "4216020000000Y"
                elif gwctp == "BG":
                    results.append({"BRANCH": branch, "BNMCODE": "4216017000000Y",
                                    "AMOUNT": amount, "AMTIND": amtind})
                    bnmcode = "4216020000000Y"
                elif gwctp == "YY":
                    bnmcode = "4216060000000Y"
                elif gwctp == "DE":
                    bnmcode = "4216071000000Y"
                elif gwctp == "DB":
                    bnmcode = "4216072000000Y"
                elif gwctp == "DF":
                    bnmcode = "4216073000000Y"
                elif gwctp == "DC":
                    bnmcode = "4216074000000Y"
                elif gwctp == "EA":
                    bnmcode = "4216076000000Y"
                elif gwctp in ("BA", "BW", "BE"):
                    bnmcode = "4216081000000Y"
                else:
                    if _is_ba_bz(gwctp):
                        bnmcode = "4216060000000Y"

            # Override rules
            if gwctp == "XX" and gwsac == "UF" and gwcnal == "MY":
                bnmcode = "4216079000000Y"
            if gwsac == "UF" and gwcnal != "MY":
                bnmcode = "4216085000000Y"

            if bnmcode:
                results.append({"BRANCH": branch, "BNMCODE": bnmcode,
                                 "AMOUNT": amount, "AMTIND": amtind})

        # ---- DA42600 -------------------------------------------------------
        if gwdlp in ("FDA", "FDB", "FDL", "FDS", "BO", "BF") or gwdlp == "":
            bnmcode = ""

            if (gwdlp == "" and gwact == "CV"
                    and not _is_ba_bz(gwctp)
                    and gwcnal == "MY" and gwsac != "UF"
                    and gwccy != "MYR"):
                bnmcode = "4261060000000Y"

            if (gwdlp in ("FDA", "FDB", "FDL", "FDS", "BO", "BF")
                    and gwccy != "MYR"
                    and not _is_ba_bz(gwctp)
                    and gwcnal == "MY"
                    and gwmvt == "P" and gwmvts == "M"
                    and gwsac != "UF"):
                bnmcode = "4263060000000Y"

            if (gwdlp in ("FDA", "FDB", "FDL", "FDS", "BO", "BF")
                    and gwccy != "MYR"
                    and gwctp == "BM"
                    and gwcnal == "MY"
                    and gwmvt == "P" and gwmvts == "M"
                    and gwshn[:3] == "FCY"):
                bnmcode = "4263012000000Y"

            if (gwdlp == "" and gwact == "CV"
                    and gwcnal != "MY"
                    and gwsac == "UO"
                    and gwccy != "MYR"
                    and gwan != "000612"
                    and gwas != "344"):
                bnmcode = "4263081000000Y"

            if bnmcode:
                results.append({"BRANCH": branch, "BNMCODE": bnmcode,
                                 "AMOUNT": amount, "AMTIND": amtind})

        # ---- ML49000 -------------------------------------------------------
        bnmcode = ""
        ml_amount = amount  # default; overridden below

        if ((gwccy == "MYR" and gwsac == "UO" and gwciac != 0
                and gwmvt == "P" and gwmvts == "M") or
                (gwccy == "MYR" and gwcnal != "MY" and gwciac != 0
                 and gwmvt == "P" and gwmvts == "M")):
            bnmcode   = "4911080000000Y"
            ml_amount = gwciac

        if (gwccy != "MYR" and gwsac != "UO" and gwciac != 0
                and gwcnal == "MY" and gwmvt == "P" and gwmvts == "M"):
            bnmcode   = "4961050000000Y"
            ml_amount = gwciac

        if ((gwsac == "UO" or gwcnal != "MY")
                and gwccy != "MYR" and gwciac != 0
                and gwmvt == "P" and gwmvts == "M"):
            bnmcode   = "4961080000000Y"
            ml_amount = gwciac

        if bnmcode:
            results.append({"BRANCH": branch, "BNMCODE": bnmcode,
                             "AMOUNT": ml_amount, "AMTIND": amtind})

        # ---- FX57000 -------------------------------------------------------
        if gwdlp in ("FXS", "FXO", "FXF", "SF1", "SF2", "TS1", "TS2",
                     "FF1", "FF2"):
            bnmcode = ""

            # GWMVTS = 'P' (purchase side)
            if gwocy == "MYR" and gwmvt == "P" and gwmvts == "P":
                if gwdlp == "FXS" and gwccy != "MYR":
                    _base = "5711"
                elif gwdlp in ("FXO", "FXF") and gwccy != "MYR":
                    _base = "5712"
                elif gwdlp in ("SF1", "SF2", "TS1", "TS2", "FF1", "FF2") and gwccy != "MYR":
                    _base = "5713"
                else:
                    _base = ""

                if _base:
                    if gwctp == "BC":
                        bnmcode = f"{_base}001000000Y"
                    elif gwctp == "BB":
                        bnmcode = f"{_base}002000000Y"
                    elif gwctp == "BI":
                        bnmcode = f"{_base}003000000Y"
                    elif gwctp == "BM":
                        bnmcode = f"{_base}012000000Y"
                    elif gwctp in ("BA", "BW", "BE"):
                        bnmcode = f"{_base}081000000Y"
                    else:
                        if not _is_ba_bz(gwctp) and gwcnal == "MY" and gwsac != "UF":
                            bnmcode = f"{_base}015000000Y"
                        if gwsac == "UF":
                            bnmcode = f"{_base}085000000Y"

            # GWMVTS = 'S' (sale side)
            if gwocy == "MYR" and gwmvt == "P" and gwmvts == "S":
                if gwdlp == "FXS" and gwccy != "MYR":
                    _base = "5741"
                elif gwdlp in ("FXO", "FXF") and gwccy != "MYR":
                    _base = "5742"
                elif gwdlp in ("SF1", "SF2", "TS1", "TS2", "FF1", "FF2") and gwccy != "MYR":
                    _base = "5743"
                else:
                    _base = ""

                if _base:
                    if gwctp == "BC":
                        bnmcode = f"{_base}001000000Y"
                    elif gwctp == "BB":
                        bnmcode = f"{_base}002000000Y"
                    elif gwctp == "BI":
                        bnmcode = f"{_base}003000000Y"
                    elif gwctp == "BM":
                        bnmcode = f"{_base}012000000Y"
                    elif gwctp in ("BA", "BW", "BE"):
                        bnmcode = f"{_base}081000000Y"
                    else:
                        if not _is_ba_bz(gwctp) and gwcnal == "MY" and gwsac != "UF":
                            bnmcode = f"{_base}015000000Y"
                        if gwsac == "UF":
                            bnmcode = f"{_base}085000000Y"

            if bnmcode:
                results.append({"BRANCH": branch, "BNMCODE": bnmcode,
                                 "AMOUNT": amount, "AMTIND": amtind})

    if not results:
        return pl.DataFrame(schema={"BRANCH": pl.Int64, "BNMCODE": pl.Utf8,
                                    "AMOUNT": pl.Float64, "AMTIND": pl.Utf8})
    return pl.DataFrame(results).with_columns([
        pl.col("BNMCODE").cast(pl.Utf8),
        pl.col("AMTIND").cast(pl.Utf8),
    ])


# ============================================================================
# K3TABL PROCESSING  (DATA K3TABL step)
# ============================================================================

def _process_k3tabl(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply BNM-code classification rules for K3TBL (unit trust) rows.
    Returns DataFrame with columns: BNMCODE, AMOUNT, AMTIND.
    """
    results: list[dict] = []

    for row in df.iter_rows(named=True):
        utref  = str(row.get("UTREF",  "") or "").strip()
        utsty  = str(row.get("UTSTY",  "") or "").strip()
        utctp  = str(row.get("UTCTP",  "") or "").strip()
        utamoc = row.get("UTAMOC", 0.0) or 0.0
        utdpf  = row.get("UTDPF",  0.0) or 0.0
        amount = utamoc - utdpf

        amtind = "I"

        # Filter: SUBSTR(UTREF,1,1) EQ 'I' AND SUBSTR(UTREF,1,4) NE '  '
        if not utref or utref[0] != "I" or utref[:4].strip() == "":
            continue

        bnmcode = ""

        if (utsty in ("IFD", "ILD", "ISD", "IZD")
                and utref in ("PFD", "PLD", "PSD", "PZD")):
            if utctp == "BB":
                bnmcode = "4215002000000Y"
            elif utctp == "BG":
                results.append({"BNMCODE": "4215017000000Y",
                                 "AMOUNT": amount, "AMTIND": amtind})
                bnmcode = "4215020000000Y"
            elif utctp == "BN":
                results.append({"BNMCODE": "4215013000000Y",
                                 "AMOUNT": amount, "AMTIND": amtind})
                bnmcode = "4215020000000Y"
            elif utctp in ("BA", "BW", "BE"):
                bnmcode = "4215081000000Y"
            elif utctp == "BQ":
                bnmcode = "4215011000000Y"
            elif utctp == "BM":
                bnmcode = "4215012000000Y"
            elif utctp in ("CA", "CB", "CC", "CD"):
                bnmcode = "4215060000000Y"
            else:
                if not _is_ba_bz(utctp):
                    bnmcode = "4215085000000Y"

        if bnmcode:
            results.append({"BNMCODE": bnmcode, "AMOUNT": amount,
                             "AMTIND": amtind})

    if not results:
        return pl.DataFrame(schema={"BNMCODE": pl.Utf8,
                                    "AMOUNT": pl.Float64, "AMTIND": pl.Utf8})
    return pl.DataFrame(results)


# ============================================================================
# REPORT BUILDER
# ============================================================================

def _format_amount(value: float) -> str:
    """Format as COMMA30.2 right-aligned in 30 characters."""
    return f"{value:,.2f}".rjust(30)


def _build_report(df: pl.DataFrame, title1: str, title2: str,
                  title3: str, title4: str = "",
                  page_lines: int = PAGE_LINES) -> str:
    """
    Build ASA carriage-control text report.
    '1' = new page,  ' ' = single space.
    """
    lines = []

    def new_page(text: str) -> str:
        return "1" + text

    def body(text: str) -> str:
        return " " + text

    # Header block
    lines.append(new_page(title1))
    lines.append(body(title2))
    lines.append(body(title3))
    lines.append(body(title4))   # blank TITLE4
    lines.append(body(""))

    col_hdr = f"{'OBS':<6} {'BNMCODE':<15} {'AMTIND':<8} {'AMOUNT':>30}"
    lines.append(body(col_hdr))
    lines.append(body("-" * len(col_hdr)))

    current_line = 7
    obs = 1

    for row in df.iter_rows(named=True):
        if current_line >= page_lines:
            lines.append(new_page(title1))
            lines.append(body(title2))
            lines.append(body(title3))
            lines.append(body(title4))
            lines.append(body(""))
            lines.append(body(col_hdr))
            lines.append(body("-" * len(col_hdr)))
            current_line = 7

        bnmcode = str(row.get("BNMCODE", "") or "").strip()
        amtind  = str(row.get("AMTIND",  "") or "").strip()
        amount  = row.get("AMOUNT", 0.0) or 0.0

        data_line = (f"{obs:<6} {bnmcode:<15} {amtind:<8}"
                     f" {_format_amount(amount)}")
        lines.append(body(data_line))
        current_line += 1
        obs += 1

    return "\n".join(lines) + "\n"


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------------
    # Load K1TBL
    # -----------------------------------------------------------------------
    k1tbl_path = _parquet(BNMK_DIR, "K1TBL")
    k1tbl = pl.read_parquet(k1tbl_path)

    # -----------------------------------------------------------------------
    # Process K1TABL
    # -----------------------------------------------------------------------
    k1tabl = _process_k1tabl(k1tbl)

    # -----------------------------------------------------------------------
    # Load K3TBL
    # -----------------------------------------------------------------------
    k3tbl_path = _parquet(BNMK_DIR, "K3TBL")
    k3tbl = pl.read_parquet(k3tbl_path)

    # -----------------------------------------------------------------------
    # Process K3TABL
    # -----------------------------------------------------------------------
    k3tabl = _process_k3tabl(k3tbl)

    # -----------------------------------------------------------------------
    # Combine K1TABL + K3TABL  → KALW (DATA + PROC APPEND equivalent)
    # Align schemas before concat: K1TABL has BRANCH which is dropped later
    # -----------------------------------------------------------------------
    k1_for_merge = k1tabl.select(["BNMCODE", "AMOUNT", "AMTIND"])
    kalw = pl.concat([k1_for_merge, k3tabl], how="diagonal_relaxed")

    # -----------------------------------------------------------------------
    # PROC SUMMARY: sum AMOUNT by BNMCODE, AMTIND
    # -----------------------------------------------------------------------
    kalw = (
        kalw.group_by(["BNMCODE", "AMTIND"])
            .agg(pl.col("AMOUNT").sum())
            .sort(["BNMCODE", "AMTIND"])
    )

    # -----------------------------------------------------------------------
    # AMOUNT = ABS(AMOUNT)
    # -----------------------------------------------------------------------
    kalw = kalw.with_columns(pl.col("AMOUNT").abs())

    # -----------------------------------------------------------------------
    # Build and write report
    # -----------------------------------------------------------------------
    report = _build_report(
        df     = kalw,
        title1 = "PUBLIC BANK BERHAD",
        title2 = "ISLAMIC DOMESTIC ASSETS AND LIABILITIES (KAPITI) - PART I",
        title3 = f"REPORT DATE : {RDATE}",
        title4 = "",
    )

    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        fh.write(report)

    print(f"Report written to: {OUTPUT_FILE}")
    print(f"Total KALW records: {len(kalw)}")


if __name__ == "__main__":
    main()
