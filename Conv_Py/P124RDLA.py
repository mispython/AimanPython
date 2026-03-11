#!/usr/bin/env python3
"""
Program : P124RDLA.py
Purpose : RDAL monthly report – reads BNM.ALWKM&REPTMON&NOWK, appends
            a CAG (Cagamas) supplementary dataset, removes disbursement/
            repayment/rollover codes (68340/78340/82152 prefixes), splits
            into AL / OB / SP sections and writes to RDALKM output file
            with ASA carriage-control characters.

          OPTIONS NOCENTER YEARCUTOFF=1950 applied where relevant.
          Output file is a REPORT with ASA carriage-control characters.
          Page length default: 60 lines per page.

Dependencies:
  - PBBLNFMT.py : format functions (imported; %INC PGM(PBBLNFMT) in
                    original SAS makes formats globally available.
                  No PBBLNFMT format functions are directly called in
                    this program's DATA steps, but the %INC is preserved
                    structurally via this import.)
"""

import os
import duckdb
import polars as pl

# ── Dependency import from PBBLNFMT ──────────────────────────────────────────
# Note: %INC PGM(PBBLNFMT) is present in the original SAS source.
#       No specific PBBLNFMT format functions are directly called in this
#           program's DATA steps; the %INC is preserved structurally via this import.
from PBBLNFMT import (
    format_lndenom,
    format_lnprod,
    format_oddenom,
)

# ── Path configuration ────────────────────────────────────────────────────────
BASE_DIR   = os.environ.get("BASE_DIR", "/data")
BNM_DIR    = os.path.join(BASE_DIR, "bnm")
LOAN_DIR   = os.path.join(BASE_DIR, "loan")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

REPTMON   = os.environ.get("REPTMON",  "")   # e.g. "202401"
NOWK      = os.environ.get("NOWK",     "")   # e.g. "01"
REPTDAY   = os.environ.get("REPTDAY",  "")   # e.g. "08"
REPTYEAR  = os.environ.get("REPTYEAR", "")   # e.g. "2024"

# Input parquet paths
ALWKM_PARQUET  = os.path.join(BNM_DIR,  f"ALWKM{REPTMON}{NOWK}.parquet")
LNNOTE_PARQUET = os.path.join(LOAN_DIR, "LNNOTE.parquet")

# Output report file (text / ASA carriage-control)
RDALKM_TXT = os.path.join(OUTPUT_DIR, f"RDALKM{REPTMON}{NOWK}.txt")

os.makedirs(BNM_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── ASA carriage-control constants ────────────────────────────────────────────
# ' ' = single space (advance one line before printing)
# '0' = double space (advance two lines before printing)
# '1' = advance to top of next page before printing
# '+' = suppress spacing (overprint)
ASA_SPACE  = " "   # single space
ASA_PAGE   = "1"   # new page
PAGE_LENGTH = 60   # lines per page (default)

# ── Cagamas PZIPCODE filter set ───────────────────────────────────────────────
# DATA CAG: IF PZIPCODE IN (...)
CAGAMAS_ZIPCODES = {
    2002, 2013, 3039, 3047,
    800003098, 800003114,
    800004016, 800004022, 800004029,
    800040050, 800040053,
    800050024,
    800060024, 800060045, 800060081,
    80060085,
}


# ── %MACRO WEEKLY – filter ALWKM ─────────────────────────────────────────────
def load_rdalkm() -> pl.DataFrame:
    """
    %MACRO WEEKLY:
    Read BNM.ALWKM&REPTMON&NOWK and exclude rows whose first 5 chars of
    ITCODE fall within:
      '30221'–'30228', '30231'–'30238', '30091'–'30098', '40151'–'40158'
    """
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT ITCODE, AMTIND, AMOUNT
        FROM read_parquet('{ALWKM_PARQUET}')
    """).pl()

    def not_in_range(itcode: str) -> bool:
        p = (itcode or "")[:5]
        return not (
            ("30221" <= p <= "30228")
            or ("30231" <= p <= "30238")
            or ("30091" <= p <= "30098")
            or ("40151" <= p <= "40158")
        )

    df = df.filter(
        pl.struct(["ITCODE"]).map_elements(
            lambda s: not_in_range(s["ITCODE"]),
            return_dtype=pl.Boolean,
        )
    )
    return df


# ── Build CAG dataset from LOAN.LNNOTE ───────────────────────────────────────
def build_cag() -> pl.DataFrame:
    """
    DATA CAG:
      SET LOAN.LNNOTE;
      IF LOANTYPE IN (124,145);
      PRODCD='34120'; AMTIND='I';
      IF PZIPCODE IN (...);
      ITCODE='7511100000000Y';

    Then PROC SUMMARY NWAY by ITCODE, AMTIND; VAR BALANCE; SUM=AMOUNT.
    """
    con = duckdb.connect()

    if not os.path.exists(LNNOTE_PARQUET):
        return pl.DataFrame(schema={"ITCODE": pl.Utf8, "AMTIND": pl.Utf8, "AMOUNT": pl.Float64})

    df = con.execute(f"""
        SELECT LOANTYPE, PZIPCODE, BALANCE
        FROM read_parquet('{LNNOTE_PARQUET}')
        WHERE LOANTYPE IN (124, 145)
    """).pl()

    # IF PZIPCODE IN (...) – filter to Cagamas zip codes
    df = df.filter(
        pl.col("PZIPCODE").is_in(list(CAGAMAS_ZIPCODES))
    )

    # Assign fixed values
    df = df.with_columns([
        pl.lit("34120").alias("PRODCD"),
        pl.lit("I").alias("AMTIND"),
        pl.lit("7511100000000Y").alias("ITCODE"),
    ])

    # PROC SUMMARY NWAY by ITCODE, AMTIND; VAR BALANCE; SUM=AMOUNT
    cag = (
        df
        .group_by(["ITCODE", "AMTIND"])
        .agg(pl.col("BALANCE").sum().alias("AMOUNT"))
    )
    return cag


# ── Split into AL / OB / SP datasets ─────────────────────────────────────────
def split_datasets(rdalkm: pl.DataFrame):
    """
    Mirrors the SAS DATA AL OB SP step:

      IF AMTIND ^= ' ' THEN DO;
        IF ITCODE[:3] IN ('307')          → SP
        ELSE IF ITCODE[:1] ^= '5' THEN DO;
          IF ITCODE[:3] IN ('685','785')  → SP
          ELSE                            → AL
        END;
        ELSE                              → OB
      END;
      ELSE IF ITCODE[2] = '0'            → SP  (1-based idx 2 = 0-based idx 1)

    Note: unlike P124RDLB, there is NO 'ELSE IF ITCODE[:5] IN ('40190')'
    branch here. The original SAS source for P124RDLA omits that check.
    """
    al_rows = []
    ob_rows = []
    sp_rows = []

    for row in rdalkm.iter_rows(named=True):
        itcode = row["ITCODE"] or ""
        amtind = row["AMTIND"]
        amount = row["AMOUNT"]

        r = {"ITCODE": itcode, "AMTIND": amtind, "AMOUNT": amount}

        if amtind != " " and amtind is not None:
            p1_3 = itcode[:3]
            p1_1 = itcode[:1]
            if p1_3 == "307":
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
      - _N_=1: write PHEAD then 'AL' header.
      - PROCEED logic: if REPTDAY IN ('08','22') AND
        ITCODE='4003000000000Y' AND ITCODE[:2] IN ('68','78') → PROCEED='N'.
        (Note: ITCODE='4003000000000Y' can never start with '68'/'78';
         condition never fires in practice – preserved faithfully.)
      - Accumulate AMOUNTD (D) and AMOUNTI (I) per ITCODE group.
      - On LAST.ITCODE: AMOUNTD = AMOUNTD + AMOUNTI; write detail line.
    """
    al_sorted = al_df.sort(["ITCODE", "AMTIND"])
    row_list  = list(al_sorted.iter_rows(named=True))

    # Build last-of-group index
    itcode_groups: dict[str, list] = {}
    for r in row_list:
        itcode_groups.setdefault(r["ITCODE"], []).append(r)

    phead   = f"RDAL{reptday}{reptmon}{reptyear}"
    first   = True
    amountd = 0
    amounti = 0

    for r in row_list:
        itcode = r["ITCODE"]
        amtind = r["AMTIND"]
        amount = r["AMOUNT"]

        if first:
            f.write(f"{ASA_PAGE}{phead}\n")
            f.write(f"{ASA_SPACE}AL\n")
            amountd = 0
            amounti = 0
            first = False

        # PROCEED check
        proceed = True
        if reptday in ("08", "22"):
            if itcode == "4003000000000Y" and itcode[:2] in ("68", "78"):
                proceed = False

        if not proceed:
            continue

        amount_rounded = round(amount / 1000)
        if amtind == "D":
            amountd += amount_rounded
        elif amtind == "I":
            amounti += amount_rounded

        is_last = (r is itcode_groups[itcode][-1])
        if is_last:
            amountd = amountd + amounti
            f.write(f"{ASA_SPACE}{itcode};{amountd};{amounti}\n")
            amountd = 0
            amounti = 0


# ── Write OB section ──────────────────────────────────────────────────────────
def write_ob_section(ob_df: pl.DataFrame, f):
    """
    DATA _NULL_ SET OB BY ITCODE AMTIND (FILE RDALKM MOD):
      - _N_=1: write 'OB' header.
      - Accumulate AMOUNTD (D) and AMOUNTI (I) per ITCODE group.
      - On LAST.ITCODE: AMOUNTD = AMOUNTD + AMOUNTI; write detail line.
    """
    ob_sorted = ob_df.sort(["ITCODE", "AMTIND"])
    row_list  = list(ob_sorted.iter_rows(named=True))

    itcode_groups: dict[str, list] = {}
    for r in row_list:
        itcode_groups.setdefault(r["ITCODE"], []).append(r)

    first   = True
    amountd = 0
    amounti = 0

    for r in row_list:
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

        is_last = (r is itcode_groups[itcode][-1])
        if is_last:
            amountd = amountd + amounti
            f.write(f"{ASA_SPACE}{itcode};{amountd};{amounti}\n")
            amountd = 0
            amounti = 0


# ── Write SP section ──────────────────────────────────────────────────────────
def write_sp_section(sp_df: pl.DataFrame, f):
    """
    PROC SORT BY ITCODE; DATA _NULL_ SET SP BY ITCODE (FILE RDALKM MOD):
      - _N_=1: write 'SP' header.
      - Accumulate AMOUNTD (no AMOUNTI separation) per ITCODE group.
      - On LAST.ITCODE: AMOUNTD = ROUND(AMOUNTD/1000); write detail line.
    """
    # PROC SORT BY ITCODE only (no AMTIND in sort for SP)
    sp_sorted = sp_df.sort(["ITCODE"])
    row_list  = list(sp_sorted.iter_rows(named=True))

    itcode_groups: dict[str, list] = {}
    for r in row_list:
        itcode_groups.setdefault(r["ITCODE"], []).append(r)

    first   = True
    amountd = 0.0

    for r in row_list:
        itcode = r["ITCODE"]
        amount = r["AMOUNT"]

        if first:
            f.write(f"{ASA_SPACE}SP\n")
            amountd = 0.0
            first = False

        amountd += amount

        is_last = (r is itcode_groups[itcode][-1])
        if is_last:
            amountd_out = round(amountd / 1000)
            f.write(f"{ASA_SPACE}{itcode};{amountd_out}\n")
            amountd = 0.0


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # %INC PGM(PBBLNFMT) – formats imported at module level above

    # %MACRO WEEKLY – load and filter ALWKM
    rdalkm = load_rdalkm()

    # DATA CAG: build Cagamas supplement from LOAN.LNNOTE
    cag = build_cag()

    # DATA RDALKM: SET RDALKM CAG; DELETE rows with 68340/78340/82152 prefix
    # Merge RDALKM with CAG rows, then remove disbursement/repayment codes
    rdalkm = pl.concat([rdalkm, cag], how="diagonal")
    rdalkm = rdalkm.filter(
        ~pl.col("ITCODE").str.slice(0, 5).is_in(["68340", "78340", "82152"])
    )

    # PROC SORT DATA=RDALKM BY ITCODE AMTIND
    rdalkm = rdalkm.sort(["ITCODE", "AMTIND"])

    # DATA AL OB SP – split by AMTIND and ITCODE prefix rules
    al_df, ob_df, sp_df = split_datasets(rdalkm)

    # Write report with ASA carriage-control characters
    with open(RDALKM_TXT, "w") as f:
        write_al_section(al_df, f, REPTDAY, REPTMON, REPTYEAR)
        write_ob_section(ob_df, f)
        # PROC SORT SP BY ITCODE (already handled inside write_sp_section)
        write_sp_section(sp_df, f)

    print(f"P124RDLA complete. Report written to: {RDALKM_TXT}")


if __name__ == "__main__":
    main()
