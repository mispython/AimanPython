#!/usr/bin/env python3
"""
Program  : EIBWRDLA.py
Purpose  : Weekly RDAL/NSRS generation for BNM Asset/Liability reporting.
           Reads weekly BNM ALWKM data, filters unwanted items, augments with
            CAG loan note data, splits into AL/OB/SP sections, and writes two
            semicolon-delimited output files (RDALKM and NSRSKM) with a header
            line followed by AL, OB, and SP section blocks.
           ESMR : 06-1485
"""

# OPTIONS NOCENTER YEARCUTOFF=1950;
# %INC PGM(PBBLNFMT);

import os
import duckdb
import polars as pl
from PBBLNFMT import format_lnprod, format_lndenom

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR     = os.environ.get("BASE_DIR",     "/data")
BNM_DIR      = os.environ.get("BNM_DIR",      os.path.join(BASE_DIR, "bnm"))
LOAN_DIR     = os.environ.get("LOAN_DIR",     os.path.join(BASE_DIR, "loan"))
OUTPUT_DIR   = os.environ.get("OUTPUT_DIR",   os.path.join(BASE_DIR, "output"))

# ============================================================================
# REPORTING PERIOD CONSTANTS
# (Equivalent to SAS global macro variables &REPTMON, &NOWK, &REPTDAY,
#  &REPTYEAR pre-set in the session by the orchestrator)
# ============================================================================

REPTMON  = os.environ.get("REPTMON",  "")   # e.g. "01"
NOWK     = os.environ.get("NOWK",     "")   # e.g. "1"
REPTDAY  = os.environ.get("REPTDAY",  "")   # e.g. "08"
REPTYEAR = os.environ.get("REPTYEAR", "")   # e.g. "2024"

# ============================================================================
# INPUT / OUTPUT FILE PATHS
# ============================================================================

# Weekly BNM ALWKM input: BNM.ALWKM<REPTMON><NOWK>
ALWKM_FILE  = os.path.join(BNM_DIR,  f"ALWKM{REPTMON}{NOWK}.parquet")

# Loan notes source
LNNOTE_FILE = os.path.join(LOAN_DIR, "LNNOTE.parquet")

# K3FEI and KAPX supplementary SP datasets
K3FEI_FILE  = os.path.join(BASE_DIR, "K3FEI.parquet")
KAPX_FILE   = os.path.join(BASE_DIR, "KAPX.parquet")

# Output files
RDALKM_FILE = os.path.join(OUTPUT_DIR, f"RDALKM{REPTMON}{NOWK}.txt")
NSRSKM_FILE = os.path.join(OUTPUT_DIR, f"NSRSKM{REPTMON}{NOWK}.txt")

# ============================================================================
# HELPER: ROUTE ROWS INTO AL / OB / SP SECTIONS
# ============================================================================

def _classify_section(df: pl.DataFrame) -> pl.DataFrame:
    """
    Assign each row to section AL, OB, or SP following the SAS DATA AL OB SP
    split logic:
        IF AMTIND ^= ' ' THEN DO;
           IF SUBSTR(ITCODE,1,3) IN ('307') THEN OUTPUT SP;
           ELSE IF SUBSTR(ITCODE,1,1) ^= '5' THEN DO;
              IF SUBSTR(ITCODE,1,3) IN ('685','785') THEN OUTPUT SP;
              ELSE OUTPUT AL;
           END;
           ELSE OUTPUT OB; END;
        ELSE IF SUBSTR(ITCODE,2,1)='0' THEN OUTPUT SP;
    Rows that don't match any rule are dropped (same as SAS — no OUTPUT).
    """
    def _section(itcode: str, amtind: str) -> str | None:
        amtind = amtind.strip() if amtind else ""
        it3  = itcode[:3]  if len(itcode) >= 3 else ""
        it1  = itcode[:1]  if len(itcode) >= 1 else ""
        it2  = itcode[1:2] if len(itcode) >= 2 else ""
        if amtind != "":
            if it3 == "307":
                return "SP"
            elif it1 != "5":
                if it3 in ("685", "785"):
                    return "SP"
                else:
                    return "AL"
            else:
                return "OB"
        elif it2 == "0":
            return "SP"
        return None

    sections = [_section(r["ITCODE"], r["AMTIND"]) for r in df.iter_rows(named=True)]
    return df.with_columns(pl.Series("_SECTION", sections))


# ============================================================================
# HELPER: WRITE AL SECTION (RDALKM)
# ============================================================================

def _write_al_rdal(f, al_df: pl.DataFrame) -> None:
    """
    Write AL section to RDALKM output file.
    Amounts are divided by 1000, rounded, then accumulated by ITCODE.
    PROCEED filter: if REPTDAY in ('08','22'), skip rows where
        ITCODE='4003000000000Y' AND ITCODE[:2] in ('68','78').
    For each ITCODE group: AMOUNTD = AMOUNTD + AMOUNTI + AMOUNTF (total).
    Output line: ITCODE;AMOUNTD;AMOUNTI;AMOUNTF
    """
    # DATA _NULL_; SET AL; BY ITCODE AMTIND;
    f.write("AL\n")

    # Aggregate per ITCODE / AMTIND
    agg = (
        al_df
        .with_columns([
            pl.col("AMOUNT")
            .map_elements(lambda x: round(x / 1000), return_dtype=pl.Int64)
            .alias("AMOUNT_K")
        ])
        .group_by(["ITCODE", "AMTIND"])
        .agg(pl.col("AMOUNT_K").sum().alias("SUM_AMT"))
        .sort(["ITCODE", "AMTIND"])
    )

    # Apply PROCEED filter
    if REPTDAY in ("08", "22"):
        # IF ITCODE = '4003000000000Y' AND SUBSTR(ITCODE,1,2) IN ('68','78') THEN PROCEED='N'
        agg = agg.filter(
            ~(
                (pl.col("ITCODE") == "4003000000000Y") &
                (pl.col("ITCODE").str.slice(0, 2).is_in(["68", "78"]))
            )
        )

    for itcode, grp in agg.group_by("ITCODE", maintain_order=True):
        itcode_val = itcode[0] if isinstance(itcode, tuple) else itcode
        amt_d = amt_i = amt_f = 0
        for row in grp.iter_rows(named=True):
            ind = (row["AMTIND"] or "").strip()
            if ind == "D":
                amt_d += row["SUM_AMT"]
            elif ind == "I":
                amt_i += row["SUM_AMT"]
            elif ind == "F":
                amt_f += row["SUM_AMT"]
        amt_d_total = amt_d + amt_i + amt_f
        f.write(f"{itcode_val};{amt_d_total};{amt_i};{amt_f}\n")


# ============================================================================
# HELPER: WRITE OB SECTION (RDALKM)
# ============================================================================

def _write_ob_rdal(f, ob_df: pl.DataFrame) -> None:
    """
    Write OB section to RDALKM output file (appended).
    Amounts are divided by 1000, rounded, then accumulated by ITCODE.
    """
    # DATA _NULL_; SET OB; BY ITCODE AMTIND; FILE RDALKM MOD;
    f.write("OB\n")

    agg = (
        ob_df
        .with_columns([
            pl.col("AMOUNT")
            .map_elements(lambda x: round(x / 1000), return_dtype=pl.Int64)
            .alias("AMOUNT_K")
        ])
        .group_by(["ITCODE", "AMTIND"])
        .agg(pl.col("AMOUNT_K").sum().alias("SUM_AMT"))
        .sort(["ITCODE", "AMTIND"])
    )

    for itcode, grp in agg.group_by("ITCODE", maintain_order=True):
        itcode_val = itcode[0] if isinstance(itcode, tuple) else itcode
        amt_d = amt_i = amt_f = 0
        for row in grp.iter_rows(named=True):
            ind = (row["AMTIND"] or "").strip()
            if ind == "D":
                amt_d += row["SUM_AMT"]
            elif ind == "I":
                amt_i += row["SUM_AMT"]
            elif ind == "F":
                amt_f += row["SUM_AMT"]
        amt_d_total = amt_d + amt_i + amt_f
        f.write(f"{itcode_val};{amt_d_total};{amt_i};{amt_f}\n")


# ============================================================================
# HELPER: WRITE SP SECTION (RDALKM)
# ============================================================================

def _write_sp_rdal(f, sp_df: pl.DataFrame) -> None:
    """
    Write SP section to RDALKM output file (appended).
    Amounts are divided by 1000, rounded, accumulated by ITCODE.
    Only D and F denominations tracked; AMOUNTD = AMOUNTD + AMOUNTF (total).
    Output line: ITCODE;AMOUNTD;AMOUNTF
    """
    # DATA _NULL_; SET SP; BY ITCODE; FILE RDALKM MOD;
    f.write("SP\n")

    agg = (
        sp_df
        .with_columns([
            pl.col("AMOUNT")
            .map_elements(lambda x: round(x / 1000), return_dtype=pl.Int64)
            .alias("AMOUNT_K")
        ])
        .group_by(["ITCODE", "AMTIND"])
        .agg(pl.col("AMOUNT_K").sum().alias("SUM_AMT"))
        .sort("ITCODE")
    )

    for itcode, grp in agg.group_by("ITCODE", maintain_order=True):
        itcode_val = itcode[0] if isinstance(itcode, tuple) else itcode
        amt_d = amt_f = 0
        for row in grp.iter_rows(named=True):
            ind = (row["AMTIND"] or "").strip()
            if ind == "D":
                amt_d += row["SUM_AMT"]
            elif ind == "F":
                amt_f += row["SUM_AMT"]
        amt_d_total = amt_d + amt_f
        f.write(f"{itcode_val};{amt_d_total};{amt_f}\n")


# ============================================================================
# HELPER: WRITE AL SECTION (NSRSKM)
# ============================================================================

def _write_al_nsrs(f, al_df: pl.DataFrame) -> None:
    """
    Write AL section to NSRSKM output file.
    Amounts are rounded (no /1000 division) EXCEPT for items where
    SUBSTR(ITCODE,1,2) IN ('80') which are divided by 1000 after rounding.
    PROCEED filter same as RDALKM AL section.
    """
    # DATA _NULL_; SET AL; BY ITCODE AMTIND; FILE NSRSKM;
    f.write("AL\n")

    agg = (
        al_df
        .group_by(["ITCODE", "AMTIND"])
        .agg(pl.col("AMOUNT").sum().alias("SUM_AMT"))
        .sort(["ITCODE", "AMTIND"])
    )

    # Apply PROCEED filter
    if REPTDAY in ("08", "22"):
        agg = agg.filter(
            ~(
                (pl.col("ITCODE") == "4003000000000Y") &
                (pl.col("ITCODE").str.slice(0, 2).is_in(["68", "78"]))
            )
        )

    for itcode, grp in agg.group_by("ITCODE", maintain_order=True):
        itcode_val = itcode[0] if isinstance(itcode, tuple) else itcode
        is_80 = itcode_val[:2] == "80"
        amt_d = amt_i = amt_f = 0
        for row in grp.iter_rows(named=True):
            ind = (row["AMTIND"] or "").strip()
            # AMOUNT=ROUND(AMOUNT); IF SUBSTR(ITCODE,1,2)='80' THEN AMOUNT=ROUND(AMOUNT/1000)
            amt = round(row["SUM_AMT"])
            if is_80:
                amt = round(amt / 1000)
            if ind == "D":
                amt_d += amt
            elif ind == "I":
                amt_i += amt
            elif ind == "F":
                amt_f += amt
        amt_d_total = amt_d + amt_i + amt_f
        f.write(f"{itcode_val};{amt_d_total};{amt_i};{amt_f}\n")


# ============================================================================
# HELPER: WRITE OB SECTION (NSRSKM)
# ============================================================================

def _write_ob_nsrs(f, ob_df: pl.DataFrame) -> None:
    """
    Write OB section to NSRSKM output file (appended).
    Amounts are rounded; items where SUBSTR(ITCODE,1,2)='80' are divided /1000
    before rounding.
    """
    # DATA _NULL_; SET OB; BY ITCODE AMTIND; FILE NSRSKM MOD;
    f.write("OB\n")

    agg = (
        ob_df
        .group_by(["ITCODE", "AMTIND"])
        .agg(pl.col("AMOUNT").sum().alias("SUM_AMT"))
        .sort(["ITCODE", "AMTIND"])
    )

    for itcode, grp in agg.group_by("ITCODE", maintain_order=True):
        itcode_val = itcode[0] if isinstance(itcode, tuple) else itcode
        is_80 = itcode_val[:2] == "80"
        amt_d = amt_i = amt_f = 0
        for row in grp.iter_rows(named=True):
            ind = (row["AMTIND"] or "").strip()
            # IF SUBSTR(ITCODE,1,2) IN ('80') THEN AMOUNT=ROUND(AMOUNT/1000)
            # IF AMTIND='D' THEN AMOUNTD+ROUND(AMOUNT)
            raw = row["SUM_AMT"]
            if is_80:
                raw = raw / 1000
            amt = round(raw)
            if ind == "D":
                amt_d += amt
            elif ind == "I":
                amt_i += amt
            elif ind == "F":
                amt_f += amt
        amt_d_total = amt_d + amt_i + amt_f
        f.write(f"{itcode_val};{amt_d_total};{amt_i};{amt_f}\n")


# ============================================================================
# HELPER: WRITE SP SECTION (NSRSKM)
# ============================================================================

def _write_sp_nsrs(f, sp_df: pl.DataFrame) -> None:
    """
    Write SP section to NSRSKM output file (appended).
    Amounts are rounded (AMOUNT=ROUND(AMOUNT)).
    For ITCODE[:2]='80' items: the accumulated AMOUNTD is divided by 1000
    at output time (not AMOUNTF).
    Output line: ITCODE;AMOUNTD;AMOUNTF
    NOTE: In the SAS source the /1000 for '80' items in SP/NSRS applies to
    AMOUNTD at the LAST.ITCODE block but AMOUNTF is written as-is.
    """
    # DATA _NULL_; SET SP; BY ITCODE; FILE NSRSKM MOD;
    f.write("SP\n")

    agg = (
        sp_df
        .group_by(["ITCODE", "AMTIND"])
        .agg(pl.col("AMOUNT").sum().alias("SUM_AMT"))
        .sort("ITCODE")
    )

    for itcode, grp in agg.group_by("ITCODE", maintain_order=True):
        itcode_val = itcode[0] if isinstance(itcode, tuple) else itcode
        is_80 = itcode_val[:2] == "80"
        amt_d = amt_f = 0
        for row in grp.iter_rows(named=True):
            ind = (row["AMTIND"] or "").strip()
            amt = round(row["SUM_AMT"])
            if ind == "D":
                amt_d += amt
            elif ind == "F":
                amt_f += amt
        # IF SUBSTR(ITCODE,1,2) IN ('80') THEN AMOUNT=ROUND(AMOUNTD/1000)
        # Note: SAS assigns the /1000 result to AMOUNT but still uses AMOUNTD
        # in the PUT statement; the variable AMOUNT is unused after that line.
        # AMOUNTD is therefore written as accumulated (rounded whole units).
        amt_d_total = amt_d + amt_f
        f.write(f"{itcode_val};{amt_d_total};{amt_f}\n")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    con = duckdb.connect()

    # --------------------------------------------------------------------------
    # %MACRO WEEKLY — load ALWKM<REPTMON><NOWK> and filter unwanted items
    # --------------------------------------------------------------------------
    # DATA RDALKM; SET BNM.ALWKM&REPTMON&NOWK;
    #   IF NOT ('30221' <= SUBSTR(ITCODE,1,5) <= '30228') &
    #      NOT ('30231' <= SUBSTR(ITCODE,1,5) <= '30238') &
    #      NOT ('30091' <= SUBSTR(ITCODE,1,5) <= '30098') &
    #      NOT ('40151' <= SUBSTR(ITCODE,1,5) <= '40158');
    rdalkm = con.execute(f"""
        SELECT *
        FROM read_parquet('{ALWKM_FILE}')
        WHERE NOT (SUBSTR(ITCODE, 1, 5) BETWEEN '30221' AND '30228')
          AND NOT (SUBSTR(ITCODE, 1, 5) BETWEEN '30231' AND '30238')
          AND NOT (SUBSTR(ITCODE, 1, 5) BETWEEN '30091' AND '30098')
          AND NOT (SUBSTR(ITCODE, 1, 5) BETWEEN '40151' AND '40158')
    """).pl()

    # --------------------------------------------------------------------------
    # DATA CAG — derive CAG from LOAN.LNNOTE
    # --------------------------------------------------------------------------
    # DATA CAG; SET LOAN.LNNOTE;
    #   IF PZIPCODE IN (2002,2013,3039,3047,800003098,800003114,...);
    #   PRODCD=PUT(LOANTYPE,LNPROD.);
    #   * IF PRODCD='34120';
    #   AMTIND=PUT(LOANTYPE,LNDENOM.);
    #   ITCODE='7511100000000Y';

    CAG_ZIPCODES = {
        2002, 2013, 3039, 3047,
        800003098, 800003114,
        800004016, 800004022, 800004029, 800040050,
        800040053, 800050024, 800060024, 800060045,
        800060081, 80060085,
    }

    lnnote_raw = con.execute(f"""
        SELECT LOANTYPE, BALANCE
        FROM read_parquet('{LNNOTE_FILE}')
        WHERE PZIPCODE IN ({','.join(str(z) for z in CAG_ZIPCODES)})
    """).pl()

    cag = (
        lnnote_raw
        .with_columns([
            pl.col("LOANTYPE")
              .map_elements(format_lnprod, return_dtype=pl.Utf8)
              .alias("PRODCD"),
            pl.col("LOANTYPE")
              .map_elements(format_lndenom, return_dtype=pl.Utf8)
              .alias("AMTIND"),
            pl.lit("7511100000000Y").alias("ITCODE"),
        ])
        # * IF PRODCD='34120';   <- commented out in original SAS
        .rename({"BALANCE": "AMOUNT"})
    )

    # PROC SUMMARY DATA=CAG NWAY; CLASS ITCODE AMTIND; VAR BALANCE;
    # OUTPUT OUT=CAG (DROP=_FREQ_ _TYPE_) SUM=AMOUNT;
    cag = (
        cag
        .group_by(["ITCODE", "AMTIND"])
        .agg(pl.col("AMOUNT").sum())
    )

    # --------------------------------------------------------------------------
    # DATA RDALKM; SET RDALKM CAG;
    # --------------------------------------------------------------------------
    rdalkm = pl.concat([rdalkm, cag], how="diagonal")

    # PROC SORT DATA=RDALKM; BY ITCODE AMTIND;
    rdalkm = rdalkm.sort(["ITCODE", "AMTIND"])

    # --------------------------------------------------------------------------
    # DATA AL OB SP — first split (for RDALKM output)
    # WHERE SUBSTR(ITCODE,14,1) NOT IN ('F','#');
    # --------------------------------------------------------------------------
    rdalkm_filtered = rdalkm.filter(
        ~pl.col("ITCODE").str.slice(13, 1).is_in(["F", "#"])
    )

    classified = _classify_section(rdalkm_filtered)
    al1 = classified.filter(pl.col("_SECTION") == "AL").drop("_SECTION")
    ob1 = classified.filter(pl.col("_SECTION") == "OB").drop("_SECTION")
    sp1 = classified.filter(pl.col("_SECTION") == "SP").drop("_SECTION")

    # --------------------------------------------------------------------------
    # DATA SP; SET SP K3FEI KAPX; PROC SORT; BY ITCODE;
    # --------------------------------------------------------------------------
    k3fei = con.execute(f"SELECT * FROM read_parquet('{K3FEI_FILE}')").pl()
    kapx  = con.execute(f"SELECT * FROM read_parquet('{KAPX_FILE}')").pl()

    sp1_combined = pl.concat([sp1, k3fei, kapx], how="diagonal").sort("ITCODE")

    # --------------------------------------------------------------------------
    # Write RDALKM output file
    # --------------------------------------------------------------------------
    phead = f"RDAL{REPTDAY}{REPTMON}{REPTYEAR}"

    with open(RDALKM_FILE, "w") as f:
        f.write(f"{phead}\n")
        _write_al_rdal(f, al1)
        _write_ob_rdal(f, ob1)
        _write_sp_rdal(f, sp1_combined)

    # --------------------------------------------------------------------------
    # DATA RDALKM — process '#' sign rows (negate amount, change '#' to 'Y')
    # --------------------------------------------------------------------------
    # DATA RDALKM; SET RDALKM;
    #   IF SUBSTR(ITCODE,14,1) = '#' THEN DO;
    #      SUBSTR(ITCODE,14,1) = 'Y';
    #      AMOUNT = AMOUNT*(-1);
    #   END;
    def _fix_hash(itcode: str, amount: float) -> tuple:
        if len(itcode) >= 14 and itcode[13] == "#":
            return (itcode[:13] + "Y" + itcode[14:], amount * -1)
        return (itcode, amount)

    rows = [_fix_hash(r["ITCODE"], r["AMOUNT"]) for r in rdalkm.iter_rows(named=True)]
    rdalkm_fixed = rdalkm.with_columns([
        pl.Series("ITCODE", [r[0] for r in rows]),
        pl.Series("AMOUNT", [r[1] for r in rows]),
    ])

    # PROC SUMMARY DATA=RDALKM NWAY; CLASS ITCODE AMTIND; VAR AMOUNT;
    # OUTPUT OUT=RDALKM (DROP=_FREQ_ _TYPE_) SUM=;
    rdalkm_sum = (
        rdalkm_fixed
        .group_by(["ITCODE", "AMTIND"])
        .agg(pl.col("AMOUNT").sum())
        .sort(["ITCODE", "AMTIND"])
    )

    # --------------------------------------------------------------------------
    # DATA AL OB SP — second split (for NSRSKM output)
    # No WHERE filter on position 14 this time — uses full rdalkm_sum
    # --------------------------------------------------------------------------
    classified2 = _classify_section(rdalkm_sum)
    al2 = classified2.filter(pl.col("_SECTION") == "AL").drop("_SECTION")
    ob2 = classified2.filter(pl.col("_SECTION") == "OB").drop("_SECTION")
    sp2 = classified2.filter(pl.col("_SECTION") == "SP").drop("_SECTION")

    # DATA SP; SET SP K3FEI KAPX; PROC SORT; BY ITCODE;
    sp2_combined = pl.concat([sp2, k3fei, kapx], how="diagonal").sort("ITCODE")

    # --------------------------------------------------------------------------
    # Write NSRSKM output file
    # --------------------------------------------------------------------------
    with open(NSRSKM_FILE, "w") as f:
        f.write(f"{phead}\n")
        _write_al_nsrs(f, al2)
        _write_ob_nsrs(f, ob2)
        _write_sp_nsrs(f, sp2_combined)

    con.close()


if __name__ == "__main__":
    main()
