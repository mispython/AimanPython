# !/usr/bin/env python3
"""
Program Name: KAMLIQX
Purpose: Transform K1TBL FX/derivative data into BNM liquidity reporting items
            (BNMCODE 57100, 57400, 57600) with PART/ITEM assignment.
"""

import duckdb
import polars as pl

# ── Path configuration ────────────────────────────────────────────────────────
REPTMON    = "202401"
NOWK       = "01"
INPUT_DIR  = "data/input"
OUTPUT_DIR = "data/output"

INPUT_PARQUET = f"{INPUT_DIR}/K1TBL{REPTMON}{NOWK}.parquet"
OUTPUT_FILE   = f"{OUTPUT_DIR}/K1TBX.txt"

# ── Valid GWDLP values ────────────────────────────────────────────────────────
VALID_GWDLP = ('FXS', 'FXO', 'FXF', 'SF1', 'SF2', 'TS1', 'TS2',
               'FBP', 'FF1', 'FF2')

# ── Load base K1TBX from parquet via DuckDB ───────────────────────────────────
con = duckdb.connect()

query = f"""
    SELECT *,
           MATDT AS MATDT   -- GWMDT renamed to MATDT
    FROM (
        SELECT * RENAME (GWMDT AS MATDT)
        FROM read_parquet('{INPUT_PARQUET}')
    )
    WHERE GWMVT  = 'P'
      AND GWOCY <> 'XAU'
      AND GWCCY <> 'XAU'
      AND GWDLP IN {VALID_GWDLP}
"""

k1tbx_base = con.execute(query).pl().with_columns(
    pl.lit(" ").alias("BNMCODE"),
    # AMOUNT = GWBALA * GWEXR  (same for JPY and non-JPY per SAS logic)
    (pl.col("GWBALA") * pl.col("GWEXR")).alias("AMOUNT"),
)


# ── Helper: assign BNMCODE for B-class counterparty logic ────────────────────
def _b_class_code(code: str) -> str:
    """Return the BNMCODE string; used as a label — actual Polars logic below."""
    return code


def _assign_bnmcode_57100(df: pl.DataFrame) -> pl.DataFrame:
    """
    For GWOCY=MYR, GWMVT=P, GWMVTS=P block.
    Sets BNMCODE='57100' based on GWDLP and GWCTP/GWCNAL/GWSAC rules.
    """
    # Build a boolean mask for each applicable GWDLP group
    dlp_fxs_fbp  = pl.col("GWDLP").is_in(["FXS", "FBP"])
    dlp_fxo_fxf  = pl.col("GWDLP").is_in(["FXO", "FXF"])
    dlp_futures  = pl.col("GWDLP").is_in(["SF1", "SF2", "TS1", "TS2", "FF1", "FF2"])

    ccy_ne_myr = pl.col("GWCCY") != "MYR"

    # GWCTP direct matches
    ctp_direct = pl.col("GWCTP").is_in(["BC", "BB", "BI", "BM", "BA", "BE"])

    # OTHERWISE branch
    ctp_not_ba_bz = ~((pl.col("GWCTP") >= "BA") & (pl.col("GWCTP") <= "BZ"))
    otherwise_res = (ctp_not_ba_bz & (pl.col("GWCNAL") == "MY") & (pl.col("GWSAC") != "UF")) \
                  | (pl.col("GWSAC") == "UF")

    eligible_ctp = ctp_direct | otherwise_res

    condition_base = (
        pl.col("GWOCY")  == "MYR"
    ) & (
        pl.col("GWMVT")  == "P"
    ) & (
        pl.col("GWMVTS") == "P"
    ) & ccy_ne_myr & eligible_ctp

    # FBP: only GWCCY NE MYR, no CTP check
    condition_fbp = (
        pl.col("GWOCY")  == "MYR"
    ) & (
        pl.col("GWMVT")  == "P"
    ) & (
        pl.col("GWMVTS") == "P"
    ) & (pl.col("GWDLP") == "FBP") & ccy_ne_myr

    mask = (
        ((dlp_fxs_fbp & ~(pl.col("GWDLP") == "FBP")) | dlp_fxo_fxf | dlp_futures)
        & condition_base
    ) | (pl.col("GWDLP") == "FBP") & condition_fbp

    return df.with_columns(
        pl.when(mask).then(pl.lit("57100")).otherwise(pl.col("BNMCODE")).alias("BNMCODE")
    )


def _assign_bnmcode_57400(df: pl.DataFrame) -> pl.DataFrame:
    """
    For GWOCY=MYR, GWMVT=P, GWMVTS=S block.
    Sets BNMCODE='57400'.
    """
    dlp_fxs     = pl.col("GWDLP") == "FXS"
    dlp_fxo_fxf = pl.col("GWDLP").is_in(["FXO", "FXF"])
    dlp_futures  = pl.col("GWDLP").is_in(["SF1", "SF2", "TS1", "TS2", "FF1", "FF2"])

    ccy_ne_myr = pl.col("GWCCY") != "MYR"

    # FXS adds 'CE'; FXO/FXF adds CE in OTHERWISE
    ctp_direct_fxs = pl.col("GWCTP").is_in(["BC", "BB", "BI", "BM", "BA", "BE", "CE"])
    ctp_direct_std = pl.col("GWCTP").is_in(["BC", "BB", "BI", "BM", "BA", "BE"])

    ctp_not_ba_bz = ~((pl.col("GWCTP") >= "BA") & (pl.col("GWCTP") <= "BZ"))
    otherwise_base = (ctp_not_ba_bz & (pl.col("GWCNAL") == "MY") & (pl.col("GWSAC") != "UF")) \
                   | (pl.col("GWSAC") == "UF")
    # FXO/FXF OTHERWISE also allows CE
    otherwise_fxo  = otherwise_base | (pl.col("GWCTP") == "CE")

    base_cond = (
        (pl.col("GWOCY") == "MYR") &
        (pl.col("GWMVT") == "P") &
        (pl.col("GWMVTS") == "S") &
        ccy_ne_myr
    )

    mask = (
        (base_cond & dlp_fxs & (ctp_direct_fxs | otherwise_base)) |
        (base_cond & dlp_fxo_fxf & (ctp_direct_std | otherwise_fxo)) |
        (base_cond & dlp_futures  & (ctp_direct_std | otherwise_base))
    )

    return df.with_columns(
        pl.when(mask).then(pl.lit("57400")).otherwise(pl.col("BNMCODE")).alias("BNMCODE")
    )


# ── Build K1TBX1 ──────────────────────────────────────────────────────────────
k1tbx1 = (
    k1tbx_base
    .filter(
        (pl.col("GWOCY") != "XAT") &
        (pl.col("GWCCY") != "XAT")
    )
)

k1tbx1 = _assign_bnmcode_57100(k1tbx1)
k1tbx1 = _assign_bnmcode_57400(k1tbx1)
k1tbx1 = k1tbx1.filter(pl.col("BNMCODE") != " ")

# ──
# Build K1TBX2
# ──
k1tbx2 = (
    k1tbx_base
    .with_columns(pl.col("GWBALC").alias("AMOUNT"))
    .with_columns(pl.lit(" ").alias("BNMCODE"))
)

dlp_57600 = pl.col("GWDLP").is_in(
    ["FXS", "FXO", "FXF", "SF2", "FF1", "FF2", "SF1", "TS1", "TS2"]
)

mask_57600 = (
    (pl.col("GWCCY")  != "MYR") &
    (pl.col("GWOCY")  != "MYR") &
    (pl.col("GWMVT")  == "P") &
    (pl.col("GWMVTS") == "P") &
    (pl.col("GWCTP")  != "BW") &
    dlp_57600
)

k1tbx2 = k1tbx2.with_columns(
    pl.when(mask_57600).then(pl.lit("57600")).otherwise(pl.col("BNMCODE")).alias("BNMCODE")
).filter(pl.col("BNMCODE") != " ")

# ── Combine K1TBX1 + K1TBX2 and expand rows per BNMCODE ──────────────────────
KEEP_COLS = ["PART", "ITEM", "AMOUNT", "AMTUSD", "AMTSGD", "MATDT", "AMTHKD",
             "BNMCODE", "GWCCY", "GWOCY"]

combined = pl.concat(
    [
        k1tbx1.select([c for c in k1tbx1.columns if c in KEEP_COLS + ["GWCCY", "GWOCY", "BNMCODE", "AMOUNT", "MATDT"]]),
        k1tbx2.select([c for c in k1tbx2.columns if c in KEEP_COLS + ["GWCCY", "GWOCY", "BNMCODE", "AMOUNT", "MATDT"]]),
    ],
    how="diagonal"
)

output_rows = []

for row in combined.iter_rows(named=True):
    amount  = abs(row["AMOUNT"]) if row["AMOUNT"] < 0 else row["AMOUNT"]
    gwccy   = row["GWCCY"]
    gwocy   = row["GWOCY"]
    bnmcode = row["BNMCODE"]
    matdt   = row["MATDT"]

    amtusd  = amount if gwccy == "USD" else 0.0
    amtsgd  = amount if gwccy == "SGD" else 0.0
    amthkd  = 0.0

    base = dict(AMOUNT=amount, MATDT=matdt, AMTHKD=amthkd)

    if bnmcode == "57100":
        output_rows.append({**base, "PART": "96", "ITEM": "711",
                             "AMTUSD": amtusd, "AMTSGD": amtsgd})
        output_rows.append({**base, "PART": "95", "ITEM": "911",
                             "AMTUSD": 0.0, "AMTSGD": 0.0})

    elif bnmcode == "57400":
        output_rows.append({**base, "PART": "96", "ITEM": "911",
                             "AMTUSD": amtusd, "AMTSGD": amtsgd})
        output_rows.append({**base, "PART": "95", "ITEM": "711",
                             "AMTUSD": 0.0, "AMTSGD": 0.0})

    elif bnmcode == "57600":
        output_rows.append({**base, "PART": "96", "ITEM": "711",
                             "AMTUSD": amtusd, "AMTSGD": amtsgd})
        amtusd2 = amount if gwocy == "USD" else 0.0
        amtsgd2 = amount if gwocy == "SGD" else 0.0
        output_rows.append({**base, "PART": "96", "ITEM": "911",
                             "AMTUSD": amtusd2, "AMTSGD": amtsgd2})

final_df = pl.DataFrame(output_rows, schema={
    "PART":   pl.Utf8,
    "ITEM":   pl.Utf8,
    "AMOUNT": pl.Float64,
    "AMTUSD": pl.Float64,
    "AMTSGD": pl.Float64,
    "MATDT":  pl.Date,
    "AMTHKD": pl.Float64,
})

# ── Write output ──────────────────────────────────────────────────────────────
final_df.write_csv(OUTPUT_FILE, separator="\t")

print(f"K1TBX written to {OUTPUT_FILE} — {len(final_df)} rows.")
