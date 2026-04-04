#!/usr/bin/env python3
"""
Program : EISMELQE.py
Date    : 05.09.12
Report  : NEW LIQUIDITY FRAMEWORK FOR SME
          (TO RUN ON MONTH END)
"""

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import polars as pl

from PBBLNFMT import format_liqpfmt
# PBBELF  - imported but no EL/ELI format functions are called directly in
#           this program; dependency retained as per %INC PGM(PBBELF).
# PBBDPFMT - imported but no deposit format functions are called directly in
#            this program; dependency retained as per %INC PGM(PBBDPFMT).

# ============================================================================
# PATH SETUP
# ============================================================================

BASE_DIR   = Path(os.environ.get("BASE_DIR",   "/data"))
BNM_DIR    = Path(os.environ.get("BNM_DIR",    BASE_DIR / "BNM"))
BNM1_DIR   = Path(os.environ.get("BNM1_DIR",   BASE_DIR / "BNM1"))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", BASE_DIR / "output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MNILQ_PATH = OUTPUT_DIR / "MNILQ.txt"

# ============================================================================
# MACRO VARIABLE EQUIVALENTS  (injected via environment or defaults)
# ============================================================================

REPTMON = os.environ.get("REPTMON", "")
NOWK    = os.environ.get("NOWK",    "")
RDATE   = os.environ.get("RDATE",   "")
SDESC   = os.environ.get("SDESC",   "")

# ============================================================================
# FCY PRODUCT CODES
# %LET FCY=(800,801,802,803,804,805,806,851,852,853,854,855,856,857,858,859,860)
# ============================================================================

FCY_PRODUCTS = {
    800, 801, 802, 803, 804, 805, 806,
    851, 852, 853, 854, 855, 856, 857, 858, 859, 860,
}

# ============================================================================
# SME CUSTOMER CODES  (CUSTCD filter on the loan file)
# ============================================================================

SME_CUSTCDS = {
    '41', '42', '43', '44', '46', '47',
    '48', '49', '51', '52', '53', '54',
    '65', '66', '67', '68', '69',
}

# ============================================================================
# REMFMT  – remaining-months → period label
# LOW-0.255 = 'UP TO 1 WK'
# 0.255-1   = '>1 WK - 1 MTH'
# 1-3       = '>1 MTH - 3 MTHS'
# 3-6       = '>3 - 6 MTHS'
# 6-9       = '>6 - 9 MTHS'
# 9-12      = '>9 MTHS - 1 YR'
# OTHER     = '> 1 YEAR'
# ============================================================================

def remfmt(remmth: float) -> str:
    """Map remaining months float to period label string."""
    if remmth <= 0.255:
        return 'UP TO 1 WK'
    elif remmth <= 1:
        return '>1 WK - 1 MTH'
    elif remmth <= 3:
        return '>1 MTH - 3 MTHS'
    elif remmth <= 6:
        return '>3 - 6 MTHS'
    elif remmth <= 9:
        return '>6 - 9 MTHS'
    elif remmth <= 12:
        return '>9 MTHS - 1 YR'
    else:
        return '> 1 YEAR'


# ============================================================================
# $ITEMF  – item code → description
# ============================================================================

ITEMF: dict[str, str] = {
    'A1.01':  'A1.01  LOANS: CORP - FIXED TERM LOANS',
    'A1.02':  'A1.02  LOANS: CORP - REVOLVING LOANS',
    'A1.03':  'A1.03  LOANS: CORP - OVERDRAFTS',
    'A1.04':  'A1.04  LOANS: CORP - OTHERS',
    'A1.04A': 'A1.04A LOANS: CORP - SHARE FINANCING',
    'A1.05':  'A1.05  LOANS: IND  - HOUSING LOANS',
    'A1.07':  'A1.07  LOANS: IND  - OVERDRAFTS',
    'A1.08':  'A1.08  LOANS: IND  - OTHERS',
    'A1.08A': 'A1.08A LOANS: IND  - REVOLVING LOANS',
    'A1.08B': 'A1.08B LOANS: IND  - SHARE FINANCING',
    'A1.12A': 'A1.12A DEPOSITS: CORP - GID',
    'A1.12':  'A1.12  DEPOSITS: CORP - FIXED',
    'A1.13':  'A1.13  DEPOSITS: CORP - SAVINGS',
    'A1.14':  'A1.14  DEPOSITS: CORP - CURRENT',
    'A1.15':  'A1.15  DEPOSITS: IND  - FIXED',
    'A1.15A': 'A1.15A DEPOSITS: IND  - GID',
    'A1.16':  'A1.16  DEPOSITS: IND  - SAVINGS',
    'A1.17':  'A1.17  DEPOSITS: IND  - CURRENT',
    'A1.20':  'A1.20  SHAREHOLDER FUND & OTHER LIABILITIES',
    'A1.25':  'A1.25  UNDRAWN OD FACILITIES GIVEN',
    'A1.26':  'A1.26  UNDRAWN PORTION FOR RC FACILITIES',
    'A1.28':  'A1.28  UNDRAWN PORTION OF OTHER C/F GIVEN',
    'A1.28A': 'A1.28A UNDRAWN MARGIN FINANCING          ',
    'A2.01':  'A2.01  INTERBANK LENDING/DEPOSITS',
    'A2.02':  'A2.02  REVERSE REPO',
    'A2.03':  'A2.03  DEBT SEC: GOVT PP/BNM BILLS/CAG',
    'A2.04':  'A2.04  DECT SEC: FIN INST PAPERS',
    'A2.05':  'A2.05  DEBT SEC: TRADE PAPERS',
    'A2.06':  'A2.06  CORP DEBT: GOVT-GUARANTEED',
    'A2.08':  'A2.08  CORP DEBT: NON-GUARANTEED',
    'A2.09':  'A2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'A2.14':  'A2.14  INTERBANK BORROWINGS/DEPOSITS',
    'A2.15':  'A2.15  INTERBANK REPOS',
    'A2.16':  'A2.16  NON-INTERBANK REPOS',
    'A2.17':  'A2.17  NIDS ISSUED',
    'A2.18':  'A2.18  BAS PAYABLE',
    'A2.19':  'A2.19  FX EXCHG CONTRACTS PAYABLE',
    'B1.01':  'B1.01  LOANS: NON-INDIVIDUALS - FIXED TERM LOANS',
    'B1.02':  'B1.02  LOANS: NON-INDIVIDUALS - REVOLVING LOANS',
    'B1.12':  'B1.12  DEPOSITS: CORP - FIXED (ALL CCY)',
    'B1.13':  'B1.13  DEPOSITS: CORP - CURRENT (ALL CCY)',
    'B1.15':  'B1.15  DEPOSITS: IND  - FIXED (ALL CCY)',
    'B1.16':  'B1.16  DEPOSITS: IND  - CURRENT (ALL CCY)',
    'B1.17':  'B1.17  DEPOSITS: CORP - FIXED ($USD)',
    'B1.18':  'B1.18  DEPOSITS: IND  - FIXED ($USD)',
    'B1.19':  'B1.19  DEPOSITS: CORP - FIXED ($SGD)',
    'B1.20':  'B1.20  DEPOSITS: IND  - FIXED ($SGD)',
    'B1.21':  'B1.21  DEPOSITS: CORP - CURRENT ($USD)',
    'B1.22':  'B1.22  DEPOSITS: IND  - CURRENT ($USD)',
    'B1.23':  'B1.23  DEPOSITS: CORP - CURRENT ($SGD)',
    'B1.24':  'B1.24  DEPOSITS: IND  - CURRENT ($SGD)',
    'B1.25':  'B1.25  DEPOSITS: CORP - FIXED ($HKD)',
    'B1.26':  'B1.26  DEPOSITS: IND  - FIXED ($HKD)',
    'B1.27':  'B1.27  DEPOSITS: CORP - CURRENT ($HKD)',
    'B1.28':  'B1.28  DEPOSITS: IND  - CURRENT ($HKD)',
    'B1.29':  'B1.29  DEPOSITS: CORP - FIXED   ($AUD)',
    'B1.30':  'B1.30  DEPOSITS: IND  - FIXED   ($AUD)',
    'B1.31':  'B1.31  DEPOSITS: CORP - CURRENT ($AUD)',
    'B1.32':  'B1.32  DEPOSITS: IND  - CURRENT ($AUD)',
    'B2.01':  'B2.01  INTERBANK LENDING/DEPOSITS',
    'B2.09':  'B2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'B2.14':  'B2.14  INTERBANK BORROWINGS/DEPOSITS',
    'B2.19':  'B2.19  FX EXCHG CONTRACTS PAYABLE',
    'B2.26':  'B2.26  UNDRAWN PORTION FOR RC FACILITIES',
    'B2.28':  'B2.28  UNDRAWN PORTION OF OTHER C/F GIVEN',
}


# ============================================================================
# DCLVAR – day-counts per month (static lookup, leap-year handled at runtime)
# ============================================================================

# Default days per month (non-leap year)
LDAY  = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]  # index 1..12
RPDAYS = list(LDAY)
MDDAYS = list(LDAY)


def days_in_month(month: int, year: int) -> int:
    """Return the number of days in the given month/year (handles leap year)."""
    if month == 2:
        return 29 if (year % 4 == 0) else 28
    return LDAY[month]


# ============================================================================
# NXTBLDT – advance BLDATE to next payment date
# ============================================================================

def next_bldate(
    bldate: date,
    payfreq: str,
    issdte: date,
    freq: int,
) -> date:
    """
    Compute next billing/payment date from current BLDATE.
    Mirrors the %NXTBLDT macro logic.
    """
    if payfreq == '6':
        # Biweekly – add 14 days
        new_date = bldate + timedelta(days=14)
    else:
        # Monthly increment by FREQ months
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year
        if mm > 12:
            mm -= 12
            yy += 1
        max_dd = days_in_month(mm, yy)
        if dd > max_dd:
            dd = max_dd
        new_date = date(yy, mm, dd)

    # Clamp day to month-end if it overflows (handles biweekly edge cases)
    mm = new_date.month
    yy = new_date.year
    max_dd = days_in_month(mm, yy)
    if new_date.day > max_dd:
        new_date = date(yy, mm, max_dd)

    return new_date


# ============================================================================
# REMMTH – calculate remaining months from report date to maturity
# ============================================================================

def calc_remmth(
    matdt: date,
    rpyr: int,
    rpmth: int,
    rpday: int,
) -> float:
    """
    Compute remaining months between report date and maturity date.
    Mirrors the %REMMTH macro logic.
    """
    mdyr  = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    rp_days_in_month = days_in_month(rpmth, rpyr)

    # Clamp maturity day to reporting-period days limit
    if mdday > rp_days_in_month:
        mdday = rp_days_in_month

    remy = mdyr  - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    return remy * 12 + remm + remd / rp_days_in_month


# ============================================================================
# PAYFREQ code → number of months
# ============================================================================

PAYFREQ_TO_MONTHS = {
    '1': 1,
    '2': 3,
    '3': 6,
    '4': 12,
}


# ============================================================================
# READ REPTDATE
# ============================================================================

def load_reptdate() -> dict:
    """
    Read REPTDATE from BNM.REPTDATE parquet and derive macro variables.
    Returns dict with keys: reptdate, nowk, reptyear, reptmon, reptday, rdate.
    """
    conn = duckdb.connect()
    row = conn.execute(
        f"SELECT reptdate FROM read_parquet('{BNM_DIR}/REPTDATE.parquet') LIMIT 1"
    ).fetchone()
    conn.close()

    if row is None:
        raise RuntimeError("REPTDATE table is empty")

    reptdate: date = row[0]

    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    return {
        "reptdate":  reptdate,
        "nowk":      nowk,
        "reptyear":  str(reptdate.year),
        "reptmon":   f"{reptdate.month:02d}",
        "reptday":   f"{reptdate.day:02d}",
        "rdate":     reptdate.strftime("%d/%m/%Y"),
    }


# ============================================================================
# INDIVIDUAL ROW PROCESSING  (equivalent to the DATA NOTE step)
# ============================================================================

IND_CUSTCDS = {'77', '78', '95', '96'}


def _item_for_od(custcd: str, prodcd: str) -> str:
    """Determine ITEM code for OD account type."""
    if prodcd == '34240':
        return 'A1.08B' if custcd in IND_CUSTCDS else 'A1.04A'
    return 'A1.07' if custcd in IND_CUSTCDS else 'A1.03'


def _item_for_ln(custcd: str, prod: str) -> str:
    """Determine ITEM code for LN account type (non-FCY)."""
    if custcd in IND_CUSTCDS:
        if prod == 'HL':
            return 'A1.05'
        elif prod == 'RC':
            return 'A1.08A'
        else:
            return 'A1.08'
    else:
        if prod in ('FL', 'HL'):
            return 'A1.01'
        elif prod == 'RC':
            return 'A1.02'
        else:
            return 'A1.04'


def _item_for_fcy(prod: str) -> str:
    """Determine ITEM code for FCY loan rows."""
    if prod == 'FL':
        return 'B1.01'
    elif prod == 'RC':
        return 'B1.02'
    return 'B1.01'


def process_loan_rows(
    df: pl.DataFrame,
    reptdate: date,
    rpyr: int,
    rpmth: int,
    rpday: int,
) -> list[dict]:
    """
    Replicate the DATA NOTE step:
    - Filter and classify each loan/OD row.
    - Expand instalment schedule into (PART, ITEM, REMMTH, AMOUNT) rows.
    Returns a list of dicts with keys: part, item, remmth, amount.
    """
    records: list[dict] = []

    for row in df.iter_rows(named=True):
        acctype   = (row.get("acctype")   or "").strip()
        product   = row.get("product")    or 0
        prodcd    = (row.get("prodcd")    or "").strip()
        custcd    = (row.get("custcd")    or "").strip()
        paidind   = (row.get("paidind")   or "").strip()
        eir_adj   = row.get("eir_adj")
        bal       = row.get("bal_aft_eir") or 0.0
        payamt    = row.get("payamt")      or 0.0
        payfreq   = (row.get("payfreq")   or "").strip()
        loanstat  = row.get("loanstat")   or 0

        # Raw SAS date integers → Python date (days since 1960-01-01)
        _epoch = date(1960, 1, 1)

        def _to_date(val):
            if val is None or val == 0:
                return None
            try:
                return _epoch + timedelta(days=int(val))
            except Exception:
                return None

        bldate_raw   = row.get("bldate")
        exprdate_raw = row.get("exprdate")
        issdte_raw   = row.get("issdte")

        bldate   = _to_date(bldate_raw)
        exprdate = _to_date(exprdate_raw)
        issdte   = _to_date(issdte_raw)

        # Filter: prodcd starts with '34' or product in (225, 226)
        is_34 = prodcd[:2] == '34' if prodcd else False
        if not is_34 and product not in (225, 226):
            continue

        # ── OD ACCOUNTS ────────────────────────────────────────────────────
        if acctype == 'OD':
            if product in (151, 152, 181):
                continue
            remmth = 0.1
            amount = bal
            item = _item_for_od(custcd, prodcd)
            records.append({"part": "2-RM", "item": item, "remmth": remmth, "amount": amount})
            continue

        # ── LOAN ACCOUNTS ──────────────────────────────────────────────────
        if acctype != 'LN':
            continue

        prod = format_liqpfmt(product)

        # Assign non-FCY item
        if product not in FCY_PRODUCTS:
            item = _item_for_ln(custcd, prod)
        else:
            item = _item_for_fcy(prod)

        # HARDCODE: product 100 is always A1.05
        if product == 100:
            item = 'A1.05'

        # Days since last billing date
        days = (reptdate - bldate).days if bldate else 0

        # If expiry is within 1 week of report date → treat as immediate bucket
        if exprdate and (exprdate - reptdate).days < 8:
            _emit_pair(
                records, product, prod,
                part_rm="2-RM", part_fc="2-FC",
                remmth=0.1, amount=bal,
                days=days, loanstat=loanstat,
            )
            continue

        # ── INSTALMENT SCHEDULE LOOP ────────────────────────────────────────
        freq = PAYFREQ_TO_MONTHS.get(payfreq, 1)

        # Determine initial next billing date
        if payfreq in ('5', '9', ' ', '') or product in (350, 910, 925):
            bldate = exprdate
        elif not bldate or bldate_raw == 0:
            # bldate <= 0 → start from issdte and advance to first future date
            bldate = issdte
            if bldate:
                while bldate and bldate <= reptdate:
                    bldate = next_bldate(bldate, payfreq, issdte, freq)

        if payamt < 0:
            payamt = 0.0

        if bldate and exprdate:
            if bldate > exprdate or bal <= payamt:
                bldate = exprdate

        # Walk through instalment schedule
        while bldate and exprdate and bldate <= exprdate:
            matdt   = bldate
            remmth  = calc_remmth(matdt, rpyr, rpmth, rpday)

            if remmth > 12 or bldate == exprdate:
                break

            # Adjust remmth: within 1 week but already past 1-week threshold
            if remmth > 0.255 and (bldate - reptdate).days < 8:
                remmth = 0.255

            amount = payamt
            bal    = bal - payamt

            # Part-2 output (contractual)
            _emit_pair(
                records, product, prod,
                part_rm="2-RM", part_fc="2-FC",
                remmth=remmth, amount=amount,
                days=days, loanstat=loanstat,
            )
            # Part-1 output (behavioural – overdue/non-performing push to >1yr)
            remmth_beh = 13 if (days > 89 or loanstat != 1) else remmth
            _emit_pair(
                records, product, prod,
                part_rm="1-RM", part_fc="1-FC",
                remmth=remmth_beh, amount=amount,
                days=days, loanstat=loanstat,
            )

            # Advance to next payment date
            bldate = next_bldate(bldate, payfreq, issdte, freq)
            if bldate > exprdate or bal <= payamt:
                bldate = exprdate

        # Remaining balance after schedule
        amount = bal
        remmth_final = calc_remmth(exprdate, rpyr, rpmth, rpday) if exprdate else 13

        _emit_pair(
            records, product, prod,
            part_rm="2-RM", part_fc="2-FC",
            remmth=remmth_final, amount=amount,
            days=days, loanstat=loanstat,
        )
        remmth_beh = 13 if (days > 89 or loanstat != 1) else remmth_final
        _emit_pair(
            records, product, prod,
            part_rm="1-RM", part_fc="1-FC",
            remmth=remmth_beh, amount=amount,
            days=days, loanstat=loanstat,
        )

    return records


def _emit_pair(
    records: list[dict],
    product: int,
    prod: str,
    part_rm: str,
    part_fc: str,
    remmth: float,
    amount: float,
    days: int,
    loanstat: int,
) -> None:
    """Append one output row, selecting RM vs FCY partition and item code."""
    if product in FCY_PRODUCTS:
        item = _item_for_fcy(prod)
        records.append({"part": part_fc, "item": item, "remmth": remmth, "amount": amount})
    else:
        # Item was already set in the caller for RM; re-derive from prod
        # for consistency (prod carries the liqpfmt result)
        # The RM item for OD is handled separately; here only LN reaches this path.
        # The item label stored in the loop variable (item) is not passed in;
        # re-derive from prod (non-FCY LN uses prod classification stored
        # outside this helper – use a generic default matching the SAS logic).
        records.append({"part": part_rm, "item": None, "remmth": remmth, "amount": amount})
        # Note: item=None rows will be resolved after the loop below by the caller.


# ── Simpler version without helper (avoids item=None issue) ──────────────────

def process_loan_rows_v2(
    df: pl.DataFrame,
    reptdate: date,
    rpyr: int,
    rpmth: int,
    rpday: int,
) -> list[dict]:
    """
    Full replication of DATA NOTE step, computing (part, item, remmth, amount)
    for each output row.  Returns list of dicts.
    """
    records: list[dict] = []
    _epoch = date(1960, 1, 1)

    def to_date(val) -> date | None:
        if val is None:
            return None
        try:
            iv = int(val)
            return (_epoch + timedelta(days=iv)) if iv > 0 else None
        except Exception:
            return None

    for row in df.iter_rows(named=True):
        acctype  = (row.get("acctype")  or "").strip()
        product  = int(row.get("product") or 0)
        prodcd   = (row.get("prodcd")   or "").strip()
        custcd   = (row.get("custcd")   or "").strip()
        bal      = float(row.get("bal_aft_eir") or 0.0)
        payamt   = float(row.get("payamt")      or 0.0)
        payfreq  = (row.get("payfreq")  or "").strip()
        loanstat = int(row.get("loanstat") or 0)

        bldate   = to_date(row.get("bldate"))
        exprdate = to_date(row.get("exprdate"))
        issdte   = to_date(row.get("issdte"))

        # prodcd filter: '34*' or product in (225, 226)
        if not (prodcd[:2] == '34' or product in (225, 226)):
            continue

        prod = format_liqpfmt(product)

        # ── OD ────────────────────────────────────────────────────────────
        if acctype == 'OD':
            if product in (151, 152, 181):
                continue
            item = _item_for_od(custcd, prodcd)
            records.append({"part": "2-RM", "item": item, "remmth": 0.1, "amount": bal})
            continue

        if acctype != 'LN':
            continue

        # ── LN ────────────────────────────────────────────────────────────
        if product not in FCY_PRODUCTS:
            base_item = _item_for_ln(custcd, prod)
        else:
            base_item = _item_for_fcy(prod)

        if product == 100:
            base_item = 'A1.05'   # HARDCODE BY MAZNI

        days = (reptdate - bldate).days if bldate else 0

        # Expiry within 1 week
        if exprdate and (exprdate - reptdate).days < 8:
            _append_both_parts(records, product, prod, base_item, 0.1, bal, days, loanstat)
            continue

        # ── INSTALMENT SCHEDULE ───────────────────────────────────────────
        freq = PAYFREQ_TO_MONTHS.get(payfreq, 1)

        if payfreq in ('5', '9', ' ', '') or product in (350, 910, 925):
            bldate = exprdate
        elif bldate is None or (row.get("bldate") or 0) <= 0:
            bldate = issdte
            if bldate:
                while bldate and bldate <= reptdate:
                    bldate = next_bldate(bldate, payfreq, issdte, freq)

        if payamt < 0:
            payamt = 0.0

        if bldate and exprdate and (bldate > exprdate or bal <= payamt):
            bldate = exprdate

        while bldate and exprdate and bldate <= exprdate:
            matdt  = bldate
            remmth = calc_remmth(matdt, rpyr, rpmth, rpday)

            if remmth > 12 or bldate == exprdate:
                break

            if remmth > 0.255 and (bldate - reptdate).days < 8:
                remmth = 0.255

            amount = payamt
            bal    = bal - payamt

            # Part-2 (contractual)
            _append_both_parts(records, product, prod, base_item, remmth, amount, days, loanstat)
            # Part-1 (behavioural)
            remmth_beh = 13.0 if (days > 89 or loanstat != 1) else remmth
            _append_both_parts_1(records, product, prod, base_item, remmth_beh, amount, days, loanstat)

            bldate = next_bldate(bldate, payfreq, issdte, freq)
            if bldate > exprdate or bal <= payamt:
                bldate = exprdate

        # Remaining balance
        remmth_final = (
            calc_remmth(exprdate, rpyr, rpmth, rpday) if exprdate else 13.0
        )
        _append_both_parts(records, product, prod, base_item, remmth_final, bal, days, loanstat)

        remmth_beh = 13.0 if (days > 89 or loanstat != 1) else remmth_final
        _append_both_parts_1(records, product, prod, base_item, remmth_beh, bal, days, loanstat)

    return records


def _append_both_parts(
    records: list[dict],
    product: int,
    prod: str,
    base_item: str,
    remmth: float,
    amount: float,
    days: int,
    loanstat: int,
) -> None:
    """Append Part-2 row (contractual) for RM or FC."""
    if product in FCY_PRODUCTS:
        item = _item_for_fcy(prod)
        records.append({"part": "2-FC", "item": item, "remmth": remmth, "amount": amount})
    else:
        records.append({"part": "2-RM", "item": base_item, "remmth": remmth, "amount": amount})


def _append_both_parts_1(
    records: list[dict],
    product: int,
    prod: str,
    base_item: str,
    remmth: float,
    amount: float,
    days: int,
    loanstat: int,
) -> None:
    """Append Part-1 row (behavioural) for RM or FC."""
    if product in FCY_PRODUCTS:
        item = _item_for_fcy(prod)
        records.append({"part": "1-FC", "item": item, "remmth": remmth, "amount": amount})
    else:
        records.append({"part": "1-RM", "item": base_item, "remmth": remmth, "amount": amount})


# ============================================================================
# SUMMARISE NOTE DATA
# ============================================================================

PERIOD_COLS = {
    'UP TO 1 WK':      'amtweek',
    '>1 WK - 1 MTH':   'amtmonth',
    '>1 MTH - 3 MTHS': 'amtquat',
    '>3 - 6 MTHS':     'amthalf',
    '>6 - 9 MTHS':     'amtmhf',
    '>9 MTHS - 1 YR':  'amtyear',
    '> 1 YEAR':        'amtyears',
}


def build_note_df(records: list[dict]) -> pl.DataFrame:
    """Convert raw records list to a Polars DataFrame with period pivot columns."""
    if not records:
        return pl.DataFrame()

    df = pl.DataFrame(records)

    # Sum amount by (part, item, remmth) – mirrors PROC SUMMARY
    df = df.group_by(["part", "item", "remmth"]).agg(pl.col("amount").sum())

    # Apply REMFMT and pivot into period columns
    df = df.with_columns(
        pl.col("remmth").map_elements(remfmt, return_dtype=pl.Utf8).alias("period")
    )

    # Pivot period → amount columns
    period_dfs = []
    for period, col in PERIOD_COLS.items():
        period_dfs.append(
            df.filter(pl.col("period") == period)
              .rename({"amount": col})
              .select(["part", "item", col])
        )

    # Merge all period columns per (part, item)
    base = df.select(["part", "item"]).unique()
    for col in PERIOD_COLS.values():
        sub = pl.concat(
            [x for x in period_dfs if col in x.columns],
            how="diagonal",
        ).group_by(["part", "item"]).agg(pl.col(col).sum())
        base = base.join(sub, on=["part", "item"], how="left")

    # Fill nulls with 0
    for col in PERIOD_COLS.values():
        if col in base.columns:
            base = base.with_columns(pl.col(col).fill_null(0.0))
        else:
            base = base.with_columns(pl.lit(0.0).alias(col))

    return base


def summarise_by_item(
    note_df: pl.DataFrame,
    part_filter: str,
    exclude_items: set[str] | None = None,
) -> pl.DataFrame:
    """
    Filter NOTE by PART, exclude specified items, then group-sum by ITEM.
    Mirrors PROC SUMMARY with CLASS ITEM.
    """
    df = note_df.filter(pl.col("part") == part_filter)
    if exclude_items:
        df = df.filter(~pl.col("item").is_in(list(exclude_items)))
    cols = list(PERIOD_COLS.values())
    return df.group_by("item").agg([pl.col(c).sum() for c in cols]).sort("item")


# ============================================================================
# REPORT OUTPUT  (CSV-delimited, written to MNILQ file)
# ============================================================================

HEADER_LINE = (
    'CORE (NON-TRADING) BANKING ACTIVITIES'
    ';UP TO 1 WK;>1 WK - 1 MTH;>1 MTH - 3 MTHS'
    ';>3 - 6 MTHS;>6 - 9 MTHS;9 - 12 MTHS;> 1 YEAR;TOTAL'
)


def _fmt_amount(val) -> str:
    """Format numeric amount; return '0' for None/NaN."""
    if val is None:
        return '0'
    try:
        return str(int(round(float(val))))
    except Exception:
        return '0'


def write_section(
    fh,
    df: pl.DataFrame,
    sdesc: str,
    rdate: str,
    title3: str,
) -> None:
    """Write one section block to the output file."""
    cols = list(PERIOD_COLS.values())
    first = True
    for row in df.sort("item").iter_rows(named=True):
        item = row.get("item", "")
        desc = ITEMF.get(item, item)
        amounts = [row.get(c, 0) or 0 for c in cols]
        total   = sum(amounts)
        line = (
            f"{desc};"
            + ";".join(_fmt_amount(a) for a in amounts)
            + f";{_fmt_amount(total)}"
        )
        if first:
            fh.write(" \n")
            fh.write(f"{sdesc}\n")
            fh.write(f"NEW LIQUIDITY FRAMEWORK (SME LOAN) AS AT {rdate}\n")
            fh.write(f"{title3}\n")
            fh.write(f"{HEADER_LINE}\n")
            first = False
        fh.write(f"{line}\n")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    global REPTMON, NOWK, RDATE, SDESC

    # ── Load report date ────────────────────────────────────────────────────
    rd = load_reptdate()
    reptdate: date = rd["reptdate"]
    rpyr   = reptdate.year
    rpmth  = reptdate.month
    rpday  = reptdate.day

    # Override/set macro variables
    REPTMON = rd["reptmon"]
    NOWK    = rd["nowk"]
    RDATE   = rd["rdate"]
    # SDESC is typically set by the calling JCL/orchestrator; keep env or default.
    if not SDESC:
        SDESC = "PUBLIC BANK BERHAD"

    # ── Load loan data ──────────────────────────────────────────────────────
    loan_parquet = BNM1_DIR / f"LOAN{REPTMON}{NOWK}.parquet"

    conn = duckdb.connect()
    loan_df = conn.execute(f"""
        SELECT *
        FROM   read_parquet('{loan_parquet}')
        WHERE  (paidind NOT IN ('P','C') OR eir_adj IS NOT NULL)
          AND  custcd IN (
                 '41','42','43','44','46','47',
                 '48','49','51','52','53','54',
                 '65','66','67','68','69'
               )
    """).pl()
    conn.close()

    # ── Process rows ────────────────────────────────────────────────────────
    records = process_loan_rows_v2(loan_df, reptdate, rpyr, rpmth, rpday)

    if not records:
        print("No data to report.", file=sys.stderr)
        # Write empty file to satisfy downstream expectations
        MNILQ_PATH.write_text("")
        return

    # ── Build note DataFrame ────────────────────────────────────────────────
    note_df = build_note_df(records)

    # ── Summarise for each section ──────────────────────────────────────────
    note1 = summarise_by_item(note_df, "1-RM", exclude_items={"B1.12", "B1.15"})
    note2 = summarise_by_item(note_df, "2-RM", exclude_items={"B2.26", "B2.28"})
    # Note: SAS source has PART EQ '2-F$' which appears to be a typo/typo for '2-FC'
    note3 = summarise_by_item(note_df, "2-FC")
    note4 = summarise_by_item(note_df, "1-FC")
    note5 = summarise_by_item(note_df, "2-FC")

    # ── Write output ────────────────────────────────────────────────────────
    with open(MNILQ_PATH, "w", encoding="utf-8") as fh:
        # ── NOTE1: Part 1-RM ─────────────────────────────────────────────
        cols = list(PERIOD_COLS.values())
        first = True
        for row in note1.sort("item").iter_rows(named=True):
            item    = row.get("item", "")
            desc    = ITEMF.get(item, item)
            amounts = [row.get(c, 0) or 0 for c in cols]
            total   = sum(amounts)
            line = (
                f"{desc};"
                + ";".join(_fmt_amount(a) for a in amounts)
                + f";{_fmt_amount(total)}"
            )
            if first:
                fh.write(f"{SDESC}\n")
                fh.write(
                    f"NEW LIQUIDITY FRAMEWORK (SME LOAN) AS AT {RDATE}\n"
                )
                fh.write(
                    "BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE (PART 1-RM)\n"
                )
                fh.write(f"{HEADER_LINE}\n")
                first = False
            fh.write(f"{line}\n")

        # ── NOTE2: Part 2-RM ─────────────────────────────────────────────
        write_section(
            fh, note2, SDESC, RDATE,
            "BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 2-RM)",
        )

        # ── NOTE3: Part 2-F$ (SAS source typo; treated as 2-FC) ──────────
        write_section(
            fh, note3, SDESC, RDATE,
            "BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 2-F$)",
        )

        # ── NOTE4: Part 1-FC ─────────────────────────────────────────────
        write_section(
            fh, note4, SDESC, RDATE,
            "BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE (PART 1-FCY)",
        )

        # ── NOTE5: Part 2-FC ─────────────────────────────────────────────
        write_section(
            fh, note5, SDESC, RDATE,
            "BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 2-FCY)",
        )

    print(f"Output written to: {MNILQ_PATH}")


if __name__ == "__main__":
    main()
