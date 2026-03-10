#!/usr/bin/env python3
"""
Program : P124RDLB.py
Report  : RDAL weekly report – writes AL / OB / SP sections to RDALWK
           output text file with ASA carriage-control characters.

OPTIONS NOCENTER YEARCUTOFF=1950 applied where relevant.
Output file is a REPORT with ASA carriage-control characters.
Page length default: 60 lines per page.
"""

import duckdb
import polars as pl
import math
import os

# ── Path configuration ────────────────────────────────────────────────────────
BASE_DIR   = os.environ.get("BASE_DIR", "/data")
BNM_DIR    = os.path.join(BASE_DIR, "bnm")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

REPTMON   = os.environ.get("REPTMON",  "")   # e.g. "202401"
NOWK      = os.environ.get("NOWK",     "")   # e.g. "01"
REPTDAY   = os.environ.get("REPTDAY",  "")   # e.g. "08"
REPTYEAR  = os.environ.get("REPTYEAR", "")   # e.g. "2024"

# Input parquet (BNM.ALWWK&REPTMON&NOWK)
ALWWK_PARQUET = os.path.join(BNM_DIR, f"ALWWK{REPTMON}{NOWK}.parquet")

# Output report file (text / ASA carriage-control)
RDALWK_TXT    = os.path.join(OUTPUT_DIR, f"RDALWK{REPTMON}{NOWK}.txt")

os.makedirs(BNM_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── ASA carriage-control helpers ──────────────────────────────────────────────
# ' ' = single space (advance one line before printing)
# '0' = double space (advance two lines before printing)
# '1' = advance to top of next page before printing
# '+' = suppress spacing (overprint)
ASA_SPACE  = " "   # single space
ASA_DOUBLE = "0"   # double space
ASA_PAGE   = "1"   # new page
PAGE_LENGTH = 60   # lines per page (default)

# ── %MACRO WEEKLY – filter ALWWK ─────────────────────────────────────────────
def load_rdalwk(alwwk_parquet: str) -> pl.DataFrame:
    """
    %MACRO WEEKLY:
    Read BNM.ALWWK and exclude rows whose first 5 chars of ITCODE fall within:
      '30221'–'30228', '30231'–'30238', '30091'–'30098', '40151'–'40158'
    """
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT ITCODE, AMTIND, AMOUNT
        FROM read_parquet('{alwwk_parquet}')
    """).pl()

    def not_in_range(code: str) -> bool:
        p = code[:5]
        return not (
            ("30221" <= p <= "30228")
            or ("30231" <= p <= "30238")
            or ("30091" <= p <= "30098")
            or ("40151" <= p <= "40158")
        )

    df = df.filter(
        pl.struct(["ITCODE"]).map_elements(
            lambda s: not_in_range(s["ITCODE"]),
            return_dtype=pl.Boolean
        )
    )
    return df

# ── Split into AL / OB / SP datasets ─────────────────────────────────────────
def split_datasets(rdalwk: pl.DataFrame):
    """
    Mirrors the SAS DATA AL OB SP step:
      IF AMTIND ^= ' ' THEN DO;
        IF ITCODE[:3] IN ('307')           → SP
        ELSE IF ITCODE[:5] IN ('40190')    → SP
        ELSE IF ITCODE[:1] ^= '5' THEN DO;
          IF ITCODE[:3] IN ('685','785')   → SP
          ELSE                             → AL
        END;
        ELSE                               → OB
      END;
      ELSE IF ITCODE[2] = '0'             → SP   (1-based index 2 = 0-based 1)
    """
    al_rows = []
    ob_rows = []
    sp_rows = []

    for row in rdalwk.iter_rows(named=True):
        itcode = row["ITCODE"]
        amtind = row["AMTIND"]
        amount = row["AMOUNT"]

        r = {"ITCODE": itcode, "AMTIND": amtind, "AMOUNT": amount}

        if amtind != " " and amtind is not None:
            p1_3 = itcode[:3]
            p1_5 = itcode[:5]
            p1_1 = itcode[:1]
            if p1_3 == "307":
                sp_rows.append(r)
            elif p1_5 == "40190":
                sp_rows.append(r)
            elif p1_1 != "5":
                if p1_3 in ("685", "785"):
                    sp_rows.append(r)
                else:
                    al_rows.append(r)
            else:
                ob_rows.append(r)
        else:
            # ITCODE 2nd character (1-based) = 0-based index 1
            if len(itcode) > 1 and itcode[1] == "0":
                sp_rows.append(r)

    schema = {"ITCODE": pl.Utf8, "AMTIND": pl.Utf8, "AMOUNT": pl.Float64}
    al_df = pl.DataFrame(al_rows, schema=schema) if al_rows else pl.DataFrame(schema=schema)
    ob_df = pl.DataFrame(ob_rows, schema=schema) if ob_rows else pl.DataFrame(schema=schema)
    sp_df = pl.DataFrame(sp_rows, schema=schema) if sp_rows else pl.DataFrame(schema=schema)
    return al_df, ob_df, sp_df

# ── Write AL section ──────────────────────────────────────────────────────────
def write_al_section(al_df: pl.DataFrame, f, reptday: str, reptmon: str, reptyear: str):
    """
    DATA _NULL_ SET AL BY ITCODE AMTIND:
    - First record: write PHEAD then 'AL' header line.
    - Accumulate AMOUNTD (D) and AMOUNTI (I) per ITCODE group.
    - On LAST.ITCODE: AMOUNTD = AMOUNTD + AMOUNTI; write detail line.
    - PROCEED logic: skip row if REPTDAY IN ('08','22') AND
      ITCODE='4003000000000Y' AND ITCODE[:2] IN ('68','78').
      (Note: ITCODE='4003000000000Y' can never have prefix '68'/'78',
       so this condition effectively never fires; preserved as-is.)
    """
    al_sorted = al_df.sort(["ITCODE", "AMTIND"])

    phead = f"RDAL{reptday}{reptmon}{reptyear}"

    first = True
    amountd = 0
    amounti = 0
    prev_itcode = None

    rows = al_sorted.iter_rows(named=True)
    row_list = list(rows)

    # Determine last-of-group
    itcode_groups: dict[str, list] = {}
    for r in row_list:
        itcode_groups.setdefault(r["ITCODE"], []).append(r)

    n = 0
    for r in row_list:
        n += 1
        itcode = r["ITCODE"]
        amtind = r["AMTIND"]
        amount = r["AMOUNT"]

        if first:
            # PUT @1 PHEAD  (ASA ' ' = single space before first line of page)
            f.write(f"{ASA_PAGE}{phead}\n")
            # PUT @1 'AL'
            f.write(f"{ASA_SPACE}AL\n")
            amountd = 0
            amounti = 0
            first = False

        # PROCEED check
        proceed = True
        if reptday in ("08", "22"):
            if (itcode == "4003000000000Y"
                    and itcode[:2] in ("68", "78")):
                proceed = False

        if not proceed:
            continue

        amount_rounded = round(amount / 1000)
        if amtind == "D":
            amountd += amount_rounded
        elif amtind == "I":
            amounti += amount_rounded

        # Check LAST.ITCODE
        group = itcode_groups[itcode]
        is_last = (r is group[-1])

        if is_last:
            amountd = amountd + amounti
            # PUT @1 ITCODE +(-1) ';' AMOUNTD +(-1) ';' AMOUNTI
            f.write(f"{ASA_SPACE}{itcode};{amountd};{amounti}\n")
            amountd = 0
            amounti = 0

# ── Write OB section ──────────────────────────────────────────────────────────
def write_ob_section(ob_df: pl.DataFrame, f):
    """
    DATA _NULL_ SET OB BY ITCODE AMTIND (MOD – append to existing file):
    - First record: write 'OB' header.
    - Accumulate per ITCODE group; write on LAST.ITCODE.
    """
    ob_sorted = ob_df.sort(["ITCODE", "AMTIND"])

    first = True
    amountd = 0
    amounti = 0

    itcode_groups: dict[str, list] = {}
    for r in ob_sorted.iter_rows(named=True):
        itcode_groups.setdefault(r["ITCODE"], []).append(r)

    for r in ob_sorted.iter_rows(named=True):
        itcode = r["ITCODE"]
        amtind = r["AMTIND"]
        amount = r["AMOUNT"]

        if first:
            f.write(f"{ASA_SPACE}OB\n")
            amountd = 0
            amounti = 0
            first = False

        if amtind == "D":
            amountd += round(amount / 1000)
        elif amtind == "I":
            amounti += round(amount / 1000)

        group = itcode_groups[itcode]
        is_last = (r is group[-1])

        if is_last:
            amountd = amountd + amounti
            f.write(f"{ASA_SPACE}{itcode};{amountd};{amounti}\n")
            amountd = 0
            amounti = 0

# ── Write SP section ──────────────────────────────────────────────────────────
def write_sp_section(sp_df: pl.DataFrame, f):
    """
    DATA SP (SET SP) then PROC SORT BY ITCODE, then DATA _NULL_ (MOD):
    - First record: write 'SP' header.
    - Accumulate AMOUNTD (no AMOUNTI) per ITCODE group.
    - On LAST.ITCODE: AMOUNTD = ROUND(AMOUNTD/1000); write line.
    """
    # PROC SORT BY ITCODE only
    sp_sorted = sp_df.sort(["ITCODE"])

    first = True
    amountd = 0.0

    itcode_groups: dict[str, list] = {}
    for r in sp_sorted.iter_rows(named=True):
        itcode_groups.setdefault(r["ITCODE"], []).append(r)

    for r in sp_sorted.iter_rows(named=True):
        itcode = r["ITCODE"]
        amount = r["AMOUNT"]

        if first:
            f.write(f"{ASA_SPACE}SP\n")
            amountd = 0.0
            first = False

        amountd += amount

        group = itcode_groups[itcode]
        is_last = (r is group[-1])

        if is_last:
            amountd_out = round(amountd / 1000)
            f.write(f"{ASA_SPACE}{itcode};{amountd_out}\n")
            amountd = 0.0

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # %WEEKLY – load and filter RDALWK
    rdalwk = load_rdalwk(ALWWK_PARQUET)

    # PROC SORT DATA=RDALWK BY ITCODE AMTIND
    rdalwk = rdalwk.sort(["ITCODE", "AMTIND"])

    # Split into AL / OB / SP
    al_df, ob_df, sp_df = split_datasets(rdalwk)

    # Write report to RDALWK file (ASA carriage-control text)
    with open(RDALWK_TXT, "w") as f:
        write_al_section(al_df, f, REPTDAY, REPTMON, REPTYEAR)
        write_ob_section(ob_df, f)
        write_sp_section(sp_df, f)

    print(f"P124RDLB complete. Report written to: {RDALWK_TXT}")

if __name__ == "__main__":
    main()
