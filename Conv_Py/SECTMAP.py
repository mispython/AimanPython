#!/usr/bin/env python3
"""
Program : SECTMAP.py
Purpose : Sector code mapping and rollup for ALM dataset.
          Applies $NEWSECT. / $VALIDSE. format lookups, invalid-sector
          fallback rules, detailed sub-sector OUTPUT expansion (ALM2),
          and top-level sector rollup (ALMA), then consolidates back
          into ALM.

Notes:
  - $NEWSECT. and $VALIDSE. are SAS user-defined formats (loaded from a
        format catalogue).  Their lookup tables must be supplied as the
        dictionaries NEWSECT_MAP and VALIDSE_MAP below.  Populate them from
        the original SAS format source or an equivalent reference table.
  - The ALM dataset is read from / written to parquet (BNM library).
  - This program is designed to be %INC-included by an orchestrator
        (e.g. EIBQ1241.py); all path variables are resolved from os.environ.
"""

import os
import polars as pl
import duckdb

# ── Path configuration ────────────────────────────────────────────────────────
BASE_DIR  = os.environ.get("BASE_DIR", "/data")
BNM_DIR   = os.path.join(BASE_DIR, "bnm")

REPTMON   = os.environ.get("REPTMON",  "")
NOWK      = os.environ.get("NOWK",     "")

ALM_PARQUET = os.path.join(BNM_DIR, f"ALM{REPTMON}{NOWK}.parquet")

os.makedirs(BNM_DIR, exist_ok=True)

# ── Format lookup tables (replace with actual format catalogue values) ────────
# $NEWSECT. : maps SECTORCD → new 4-char sector code, or '    ' if no mapping.
# Placeholder – populate from SAS format source.
NEWSECT_MAP: dict[str, str] = {
    # "ORIG_SECTORCD": "NEW_SECTORCD",
}

# $VALIDSE. : maps SECTORCD → 'VALID' or 'INVALID'.
# Placeholder – populate from SAS format source.
VALIDSE_MAP: dict[str, str] = {
    # "SECTORCD": "VALID" | "INVALID",
}


def apply_newsect(sectorcd: str) -> str:
    """Equivalent to PUT(SECTORCD, $NEWSECT.) – returns mapped value or '    '."""
    return NEWSECT_MAP.get(sectorcd, "    ")


def apply_validse(sectorcd: str) -> str:
    """Equivalent to PUT(SECTORCD, $VALIDSE.) – returns 'VALID' or 'INVALID'."""
    return VALIDSE_MAP.get(sectorcd, "VALID")


# ── Step 1 : Apply $NEWSECT. and $VALIDSE. mappings ──────────────────────────
# DATA ALM; SET ALM;
#   SECTA    = PUT(SECTORCD, $NEWSECT.);
#   SECVALID = PUT(SECTORCD, $VALIDSE.);
#   IF SECTA NE '    ' THEN SECTCD = SECTA; ELSE SECTCD = SECTORCD;

def step1_map_sectors(df: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for row in df.iter_rows(named=True):
        sectorcd = row.get("SECTORCD", "") or ""
        secta    = apply_newsect(sectorcd)
        secvalid = apply_validse(sectorcd)
        sectcd   = secta if secta.strip() != "" else sectorcd
        rows.append({**row, "SECTA": secta, "SECVALID": secvalid, "SECTCD": sectcd})
    return pl.DataFrame(rows)


# ── Step 2 : Invalid-sector fallback rules ────────────────────────────────────
# DATA ALM; SET ALM;
#   IF SECVALID = 'INVALID' THEN DO; prefix-based SECTCD overrides END;

def step2_invalid_fallback(df: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for row in df.iter_rows(named=True):
        sectcd   = row.get("SECTCD", "") or ""
        secvalid = row.get("SECVALID", "") or ""
        if secvalid == "INVALID":
            p1 = sectcd[:1]
            p2 = sectcd[:2]
            if p1 == "1":
                sectcd = "1400"
            elif p1 == "2":
                sectcd = "2900"
            elif p1 == "3":
                sectcd = "3919"
            elif p1 == "4":
                sectcd = "4010"
            elif p1 == "5":
                sectcd = "5999"
            elif p2 == "61":
                sectcd = "6120"
            elif p2 == "62":
                sectcd = "6130"
            elif p2 == "63":
                sectcd = "6310"
            elif p2 in ("64", "65", "66", "67", "68", "69"):
                sectcd = "6130"
            elif p1 == "7":
                sectcd = "7199"
            elif p2 in ("81", "82"):
                sectcd = "8110"
            elif p2 in ("83", "84", "85", "86", "87", "88", "89"):
                sectcd = "8999"
            elif p2 == "91":
                sectcd = "9101"
            elif p2 == "92":
                sectcd = "9410"
            elif p2 in ("93", "94", "95"):
                sectcd = "9499"
            elif p2 in ("96", "97", "98", "99"):
                sectcd = "9999"
        rows.append({**row, "SECTCD": sectcd})
    return pl.DataFrame(rows)


# ── Step 3 : ALM2 – sub-sector OUTPUT expansion ───────────────────────────────
# DATA ALM2; SET ALM;
# Each SECTCD group may emit multiple rows with different SECTORCD values.

def step3_alm2_expansion(df: pl.DataFrame) -> list[dict]:
    """
    Mirrors the SAS DATA ALM2 step.
    Each branch tests SECTCD and may OUTPUT one or more rows with a
    (potentially different) SECTORCD value.
    Returns a flat list of row dicts.
    """
    out = []

    def emit(row: dict, sectorcd: str) -> None:
        out.append({**row, "SECTORCD": sectorcd})

    for row in df.iter_rows(named=True):
        sc = row.get("SECTCD", "") or ""

        # ── 1100-series ──────────────────────────────────────────────────────
        if sc in ("1111","1112","1113","1114","1115","1116","1117","1119",
                  "1120","1130","1140","1150"):
            if sc in ("1111","1113","1115","1117","1119"):
                emit(row, "1110")
            emit(row, "1100")

        # ── 2200-series ──────────────────────────────────────────────────────
        if sc in ("2210","2220"):
            emit(row, "2200")

        # ── 2300-series ──────────────────────────────────────────────────────
        if sc in ("2301","2302","2303"):
            if sc in ("2301","2302"):
                emit(row, "2300")
            emit(row, "2300")
            if sc == "2303":
                emit(row, "2302")

        # ── 3100-series ──────────────────────────────────────────────────────
        if sc in ("3110","3115","3111","3112","3113","3114"):
            if sc in ("3110","3113","3114"):
                emit(row, "3100")
            if sc in ("3115","3111","3112"):
                emit(row, "3110")

        # ── 3210-series ──────────────────────────────────────────────────────
        if sc in ("3211","3212","3219"):
            emit(row, "3210")

        # ── 3220-series ──────────────────────────────────────────────────────
        if sc in ("3221","3222"):
            emit(row, "3220")

        # ── 3230-series ──────────────────────────────────────────────────────
        if sc in ("3231","3232"):
            emit(row, "3230")

        # ── 3240-series ──────────────────────────────────────────────────────
        if sc in ("3241","3242"):
            emit(row, "3240")

        # ── 3270/3280/3290/3271-3273 / 3311-3313 ─────────────────────────────
        if sc in ("3270","3280","3290","3271","3272","3273",
                  "3311","3312","3313"):
            if sc in ("3270","3280","3290","3271","3272","3273"):
                emit(row, "3260")
            if sc in ("3271","3272","3273"):
                emit(row, "3270")
            if sc in ("3311","3312","3313"):
                emit(row, "3310")

        # ── 3430-series ──────────────────────────────────────────────────────
        if sc in ("3431","3432","3433"):
            emit(row, "3430")

        # ── 3550-series ──────────────────────────────────────────────────────
        if sc in ("3551","3552"):
            emit(row, "3550")

        # ── 3610-series ──────────────────────────────────────────────────────
        if sc in ("3611","3619"):
            emit(row, "3610")

        # ── 3710-3730 / 3720-3721 / 3731-3732 ────────────────────────────────
        if sc in ("3710","3720","3730","3720","3721","3731","3732"):
            emit(row, "3700")
            if sc == "3721":
                emit(row, "3720")
            if sc in ("3731","3732"):
                emit(row, "3730")

        # ── 3800-series ──────────────────────────────────────────────────────
        if sc in ("3811","3812"):
            emit(row, "3800")
        if sc in ("3813","3814","3819"):
            emit(row, "3812")

        # ── 3831-3835 ────────────────────────────────────────────────────────
        if sc in ("3832","3834","3835","3833"):
            emit(row, "3831")
            if sc == "3833":
                emit(row, "3832")

        # ── 3841-3844 ────────────────────────────────────────────────────────
        if sc in ("3842","3843","3844"):
            emit(row, "3841")

        # ── 3850-series ──────────────────────────────────────────────────────
        if sc in ("3851","3852","3853"):
            emit(row, "3850")

        # ── 3860-series ──────────────────────────────────────────────────────
        if sc in ("3861","3862","3863","3864","3865","3866"):
            emit(row, "3860")

        # ── 3870-series ──────────────────────────────────────────────────────
        if sc in ("3871","3872","3872"):   # note: '3872' duplicated in original
            emit(row, "3870")

        # ── 3890-series ──────────────────────────────────────────────────────
        if sc in ("3891","3892","3893","3894"):
            emit(row, "3890")

        # ── 3910-series ──────────────────────────────────────────────────────
        if sc in ("3911","3919"):
            emit(row, "3910")

        # ── 3950-series ──────────────────────────────────────────────────────
        if sc in ("3951","3952","3953","3954","3955","3956","3957"):
            emit(row, "3950")
            if sc in ("3952","3953"):
                emit(row, "3951")
            if sc in ("3955","3956","3957"):
                emit(row, "3954")

        # ── 5001-5008 ────────────────────────────────────────────────────────
        if sc in ("5001","5002","5003","5004","5005","5006","5008"):
            emit(row, "5010")

        # ── 6100-series ──────────────────────────────────────────────────────
        if sc in ("6110","6120","6130"):
            emit(row, "6100")

        # ── 6300-series ──────────────────────────────────────────────────────
        if sc in ("6310","6320"):
            emit(row, "6300")

        # ── 7110-series ──────────────────────────────────────────────────────
        if sc in ("7111","7112","7117","7113","7114","7115","7116"):
            emit(row, "7110")
        if sc in ("7113","7114","7115","7116"):
            emit(row, "7112")
        if sc in ("7112","7114"):
            emit(row, "7113")
        if sc == "7116":
            emit(row, "7115")

        # ── 7120-series ──────────────────────────────────────────────────────
        if sc in ("7121","7122","7123","7124"):
            emit(row, "7120")
            if sc == "7124":
                emit(row, "7123")
            if sc == "7122":
                emit(row, "7121")

        # ── 7130-series ──────────────────────────────────────────────────────
        if sc in ("7131","7132","7133","7134"):
            emit(row, "7130")

        # ── 7190-series ──────────────────────────────────────────────────────
        if sc in ("7191","7192","7193","7199"):
            emit(row, "7190")

        # ── 7200-series ──────────────────────────────────────────────────────
        if sc in ("7210","7220"):
            emit(row, "7200")

        # ── 8100-series ──────────────────────────────────────────────────────
        if sc in ("8110","8120","8130"):
            emit(row, "8100")

        # ── 8300-series ──────────────────────────────────────────────────────
        if sc in ("8310","8330","8340","8320","8331","8332"):
            emit(row, "8300")
            if sc in ("8320","8331","8332"):
                emit(row, "8330")
        if sc == "8321":
            emit(row, "8320")
        if sc == "8333":
            emit(row, "8332")

        # ── 8400-series ──────────────────────────────────────────────────────
        if sc in ("8420","8411","8412","8413","8414","8415","8416"):
            emit(row, "8400")
            if sc in ("8411","8412","8413","8414","8415","8416"):
                emit(row, "8410")

        # ── 8900-series (prefix '89') ────────────────────────────────────────
        if sc[:2] == "89":
            emit(row, "8900")
            if sc in ("8910","8911","8912","8913","8914"):
                if sc in ("8911","8912","8913","8914"):
                    emit(row, "8910")
                if sc == "8910":
                    emit(row, "8914")
            if sc in ("8921","8922","8920"):
                if sc in ("8921","8922"):
                    emit(row, "8920")
                if sc == "8920":
                    emit(row, "8922")
            if sc in ("8931","8932"):
                emit(row, "8930")
            if sc in ("8991","8999"):
                emit(row, "8990")

        # ── 9100-series ──────────────────────────────────────────────────────
        if sc in ("9101","9102","9103"):
            emit(row, "9100")

        # ── 9200-series ──────────────────────────────────────────────────────
        if sc in ("9201","9202","9203"):
            emit(row, "9200")

        # ── 9300-series ──────────────────────────────────────────────────────
        if sc in ("9311","9312","9313","9314"):
            emit(row, "9300")

        # ── 9400-series (prefix '94') ────────────────────────────────────────
        if sc[:2] == "94":
            emit(row, "9400")
            if sc in ("9433","9434","9435","9432","9431"):
                if sc in ("9433","9434","9435"):
                    emit(row, "9432")
                emit(row, "9430")
            if sc in ("9410","9420","9440","9450"):
                emit(row, "9499")

    return out


# ── Step 4 : Merge original ALM (A) with ALM2; if A then SECTORCD = SECTCD ───
# DATA ALM; SET ALM(IN=A) ALM2;
#   IF A THEN SECTORCD = SECTCD;

def step4_merge_alm_alm2(alm_df: pl.DataFrame,
                          alm2_rows: list[dict]) -> pl.DataFrame:
    # Original ALM rows: override SECTORCD with SECTCD
    alm_tagged = alm_df.with_columns(
        pl.col("SECTCD").alias("SECTORCD")
    ).with_columns(pl.lit(True).alias("_A"))

    alm2_df = pl.DataFrame(alm2_rows) if alm2_rows else pl.DataFrame(schema=alm_tagged.schema)
    alm2_df = alm2_df.with_columns(pl.lit(False).alias("_A"))

    combined = pl.concat([alm_tagged, alm2_df], how="diagonal")
    combined = combined.drop("_A")
    return combined


# ── Step 5 : ALMA – top-level sector rollup ───────────────────────────────────
# DATA ALMA; SET ALM;
# Maps groups of 4-digit sector codes to 4-digit parent codes (X000).

def step5_alma_rollup(df: pl.DataFrame) -> list[dict]:
    out = []

    def emit(row: dict, sectorcd: str) -> None:
        out.append({**row, "SECTORCD": sectorcd})

    for row in df.iter_rows(named=True):
        sc = row.get("SECTORCD", "") or ""

        if sc in ("1100","1200","1300","1400"):
            emit(row, "1000")
        elif sc in ("2100","2200","2300","2400","2900"):
            emit(row, "2000")
        elif sc in ("3100","3120","3210","3220","3230","3240",
                    "3250","3260","3310","3430","3550","3610",
                    "3700","3800","3825","3831","3841","3850",
                    "3860","3870","3890","3910","3950","3960"):
            emit(row, "3000")
        elif sc in ("4010","4020","4030"):
            emit(row, "4000")
        elif sc in ("5010","5020","5030","5040","5050","5999"):
            emit(row, "5000")
        elif sc in ("6100","6300"):
            emit(row, "6000")
        elif sc in ("7110","7120","7130","7190","7200"):
            emit(row, "7000")
        elif sc in ("8100","8300","8400","8900"):
            emit(row, "8000")
        elif sc in ("9100","9200","9300","9400","9500","9600"):
            emit(row, "9000")
        # rows with no matching rollup are not output (ELSE IF chain)

    return out


# ── Step 6 : Final ALM consolidation ─────────────────────────────────────────
# DATA ALM; SET ALM ALMA;
#   IF SECTORCD EQ '    ' THEN SECTORCD = '9999';

def step6_final_alm(alm_df: pl.DataFrame,
                    alma_rows: list[dict]) -> pl.DataFrame:
    alma_df = (
        pl.DataFrame(alma_rows)
        if alma_rows
        else pl.DataFrame(schema=alm_df.schema)
    )
    combined = pl.concat([alm_df, alma_df], how="diagonal")
    combined = combined.with_columns(
        pl.when(
            pl.col("SECTORCD").is_null()
            | (pl.col("SECTORCD").str.strip_chars() == "")
        )
        .then(pl.lit("9999"))
        .otherwise(pl.col("SECTORCD"))
        .alias("SECTORCD")
    )
    return combined


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    con = duckdb.connect()

    # Load ALM from parquet
    alm_df = con.execute(f"""
        SELECT * FROM read_parquet('{ALM_PARQUET}')
    """).pl()

    # Step 1: map sectors
    alm_df = step1_map_sectors(alm_df)

    # Step 2: invalid-sector fallback
    alm_df = step2_invalid_fallback(alm_df)

    # Step 3: build ALM2 expansion rows
    alm2_rows = step3_alm2_expansion(alm_df)

    # Step 4: merge original ALM (SECTORCD = SECTCD) with ALM2
    alm_df = step4_merge_alm_alm2(alm_df, alm2_rows)

    # Step 5: ALMA top-level rollup
    alma_rows = step5_alma_rollup(alm_df)

    # Step 6: append ALMA, default blank SECTORCD → '9999'
    alm_df = step6_final_alm(alm_df, alma_rows)

    # Drop intermediate working columns not in original ALM schema
    for col in ("SECTA", "SECVALID", "SECTCD"):
        if col in alm_df.columns:
            alm_df = alm_df.drop(col)

    # Persist final ALM back to parquet
    alm_df.write_parquet(ALM_PARQUET)
    print(f"SECTMAP complete. ALM written to: {ALM_PARQUET}")


if __name__ == "__main__":
    main()
