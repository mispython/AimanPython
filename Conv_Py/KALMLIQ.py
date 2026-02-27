# !/usr/bin/env python3
"""
PROGRAM         : KALMLIQ
DATE            : 22.07.98
REPORT          : NEW LIQUIDITY FRAMEWORK (KAPITI ITEMS)
DATE MODIFIED   : 07-02-2002 (WBL)
SMR/OTHERS      : JS
CHANGES MADE    : INCLUDE NEW MARKETABLE SECURITIES PRODUCT :
                  'PNB' (9363600XX0000Y, 9563600XX0000Y)
"""

import duckdb
import polars as pl
from datetime import date, timedelta
import math

# ---------------------------------------------------------------------------
# Path configuration
# ---------------------------------------------------------------------------
INPUT_K1TBL_PARQUET = "data/K1TBL_{REPTMON}{NOWK}.parquet"   # resolved at runtime
INPUT_K3TBL_PARQUET = "data/K3TBL_{REPTMON}{NOWK}.parquet"   # resolved at runtime
REPTDATE_PARQUET    = "data/REPTDATE.parquet"                  # REPTDATE lookup table

OUTPUT_K1TBL_FILE   = "output/K1TBL.txt"
OUTPUT_K3TBL_FILE   = "output/K3TBL.txt"
OUTPUT_KTBL_FILE    = "output/KTBL.txt"
OUTPUT_KTBLALL_FILE = "output/KTBLALL.txt"

# Part 3 outputs
OUTPUT_K1TBL_PART3_FILE = "output/K1TBL_PART3.txt"

# ---------------------------------------------------------------------------
# Runtime parameters
# ---------------------------------------------------------------------------
REPTMON  = "202401"       # e.g. '202401'
NOWK     = "1"            # e.g. '1'
INST     = "PBB"          # e.g. 'PBB' or other institution code
REPTDATE = date(2024, 1, 31)  # reporting date

# ---------------------------------------------------------------------------
# Resolve actual input paths
# ---------------------------------------------------------------------------
input_k1tbl_path = INPUT_K1TBL_PARQUET.format(REPTMON=REPTMON, NOWK=NOWK)
input_k3tbl_path = INPUT_K3TBL_PARQUET.format(REPTMON=REPTMON, NOWK=NOWK)

# ---------------------------------------------------------------------------
# REMMTH macro equivalent:
# Computes remaining months from REPTDATE to MATDT.
# ---------------------------------------------------------------------------
def compute_remmth(matdt: date, reptdate: date) -> float:
    """
    Python equivalent of the %REMMTH SAS macro.
    Computes the remaining maturity bucket as a float label used in REMFMT format.
    Mirrors the SAS logic: difference in days bucketed into month bands.
    """
    diff_days = (matdt - reptdate).days

    if diff_days < 8:
        return 0.1

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    # RD2 logic (days in February for leap year check)
    rd2 = 29 if (rpyr % 4 == 0) else 28

    # Compute remaining months using SAS %REMMTH macro approximation
    # Standard SAS REMMTH logic: fractional months
    diff_months = diff_days / 30.0

    # Bucket to standard bands matching REMFMT. format
    if diff_months <= 1:
        return 1
    elif diff_months <= 2:
        return 2
    elif diff_months <= 3:
        return 3
    elif diff_months <= 6:
        return 6
    elif diff_months <= 12:
        return 12
    elif diff_months <= 24:
        return 24
    elif diff_months <= 36:
        return 36
    elif diff_months <= 60:
        return 60
    else:
        return 99


def format_remmth(remmth: float) -> str:
    """
    Equivalent of PUT(REMMTH, REMFMT.) in SAS.
    Returns a zero-padded 2-char string for use in BNMCODE construction.
    """
    mapping = {
        0.1: "00",
        1:   "01",
        2:   "02",
        3:   "03",
        6:   "06",
        12:  "12",
        24:  "24",
        36:  "36",
        60:  "60",
        99:  "99",
    }
    return mapping.get(remmth, "99")


# ===========================================================================
# PART 2: BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE
# ===========================================================================

# ---------------------------------------------------------------------------
# Load K1TBL source data via DuckDB
# ---------------------------------------------------------------------------
con = duckdb.connect()

k1tbl_raw = con.execute(f"""
    SELECT *,
           GWMDT  AS MATDT,
           GWBALC AS AMOUNT,
           GWSDT  AS ISSDT
    FROM read_parquet('{input_k1tbl_path}')
    WHERE GWMVT = 'P'
      AND GWOCY <> 'XAU'
      AND GWCCY <> 'XAU'
      AND GWOCY <> 'XAT'
      AND GWCCY <> 'XAT'
""").pl()

k3tbl_raw = con.execute(f"""
    SELECT *
    FROM read_parquet('{input_k3tbl_path}')
""").pl()

con.close()

# ---------------------------------------------------------------------------
# Build K1TBL (PART 2) — rows are emitted conditionally with ITEM assignment
# ---------------------------------------------------------------------------
KEEP_K1TBL = ["PART", "ITEM", "MATDT", "AMOUNT", "AMTUSD", "AMTSGD",
               "ISSDT", "GWCCY", "GWSHN", "GWC2R", "GWDLP", "GWDLR"]

output_k1tbl_rows = []

for row in k1tbl_raw.iter_rows(named=True):
    gwccy  = row["GWCCY"]
    gwmvts = row["GWMVTS"]
    gwdlp  = row["GWDLP"]
    gwctp  = row["GWCTP"] or ""
    gwshn  = row["GWSHN"] or ""
    amount = row["AMOUNT"]
    matdt  = row["MATDT"]
    issdt  = row["ISSDT"]
    gwc2r  = row.get("GWC2R", "")
    gwdlr  = row.get("GWDLR", "")

    base = dict(
        MATDT=matdt,
        AMOUNT=amount,
        ISSDT=issdt,
        GWCCY=gwccy,
        GWSHN=gwshn,
        GWC2R=gwc2r,
        GWDLP=gwdlp,
        GWDLR=gwdlr,
        ITEM=None,
        PART=None,
        AMTUSD=0.0,
        AMTSGD=0.0,
    )

    if gwccy == "MYR":
        base["PART"]   = "95"
        base["AMTUSD"] = 0.0
        base["AMTSGD"] = 0.0

        if gwmvts == "M":
            # BCD/BCI/BCS/BCQ/BCT/BCW/BQD -> ITEM='830'
            if gwdlp in ("BCD", "BCI", "BCS", "BCQ", "BCT", "BCW", "BQD"):
                output_k1tbl_rows.append({**base, "ITEM": "830"})

            # B-class CTP: LO/LC/LF/LS/... -> ITEM='610'
            if gwctp[:1] == "B":
                if gwdlp in ("LO", "LC", "LF", "LS", "LOI", "LSI", "LSC", "LSW",
                             "FDA", "FDB", "FDS", "FDL", "LOC", "LOW"):
                    output_k1tbl_rows.append({**base, "ITEM": "610"})
                elif gwdlp in ("BO", "BF", "BOI", "BFI", "BSC", "BSW", "BOC", "BOW"):
                    output_k1tbl_rows.append({**base, "ITEM": "810"})

            # SUBSTR(GWDLP,2,2) logic (SAS 1-based index 2, length 2 -> Python [1:3])
            dlp_mid = gwdlp[1:3]
            if dlp_mid in ("MI", "MT"):
                output_k1tbl_rows.append({**base, "ITEM": "820"})
            elif dlp_mid in ("XI", "XT"):
                output_k1tbl_rows.append({**base, "ITEM": "620"})

        '''
        ELSE IF GWDLP IN ('FXS','FXO','FXF','TS1','TS2','SF1','SF2',
           'FF1','FF2') THEN DO;
           IF GWMVTS = 'P' THEN ITEM = '711';
           ELSE IF GWMVTS = 'S' THEN ITEM = '911';
           OUTPUT;
        END;
        '''

    else:
        # PART 96 — FCY
        base["PART"]   = "96"
        base["AMTUSD"] = amount if gwccy == "USD" else 0.0
        base["AMTSGD"] = amount if gwccy == "SGD" else 0.0

        if gwmvts == "M":
            if gwctp[:1] == "B" and gwctp != "BW":
                if gwdlp in ("LO", "LC", "LS", "LF", "LOI", "LSI", "LSC", "LOC",
                             "FDA", "FDB", "FDS", "FDL", "LOW", "LSW"):
                    output_k1tbl_rows.append({**base, "ITEM": "610"})
                elif gwdlp in ("BC", "BF", "BO", "BSC", "BOW", "BSW"):
                    if gwshn[:6] != "FCY-FD":
                        output_k1tbl_rows.append({**base, "ITEM": "810"})
                elif gwdlp == "BOC":
                    output_k1tbl_rows.append({**base, "ITEM": "810"})

        '''
        ELSE IF GWDLP IN ('FXS','FXO','FXF','TS1','TS2','SF1','SF2',
           'FF1','FF2') AND GWACT NOT IN ('RV','RW') THEN DO;
           IF GWMVTS = 'P' THEN ITEM = '711';
           ELSE IF GWMVTS = 'S' THEN ITEM = '911';
           OUTPUT;
        END;
        '''

# ---------------------------------------------------------------------------
# %INC PGM(KAMLIQX) — inline logic from KAMLIQX.py
# Produces K1TBX rows (BNMCODE 57100, 57400, 57600) appended later.
# ---------------------------------------------------------------------------
# Reference: KAMLIQX.py — builds k1tbx_final (final_df) with PART/ITEM/AMOUNT
# The logic below replicates KAMLIQX inline.

VALID_GWDLP_X = ('FXS', 'FXO', 'FXF', 'SF1', 'SF2', 'TS1', 'TS2',
                 'FBP', 'FF1', 'FF2')

con2 = duckdb.connect()
k1tbx_base = con2.execute(f"""
    SELECT * RENAME (GWMDT AS MATDT)
    FROM read_parquet('{input_k1tbl_path}')
    WHERE GWMVT  = 'P'
      AND GWOCY <> 'XAU'
      AND GWCCY <> 'XAU'
      AND GWDLP IN {VALID_GWDLP_X}
""").pl().with_columns(
    pl.lit(" ").alias("BNMCODE"),
    (pl.col("GWBALA") * pl.col("GWEXR")).alias("AMOUNT"),
)
con2.close()


def _assign_bnmcode_57100(df: pl.DataFrame) -> pl.DataFrame:
    dlp_fxs_fbp  = pl.col("GWDLP").is_in(["FXS", "FBP"])
    dlp_fxo_fxf  = pl.col("GWDLP").is_in(["FXO", "FXF"])
    dlp_futures  = pl.col("GWDLP").is_in(["SF1", "SF2", "TS1", "TS2", "FF1", "FF2"])
    ccy_ne_myr   = pl.col("GWCCY") != "MYR"
    ctp_direct   = pl.col("GWCTP").is_in(["BC", "BB", "BI", "BM", "BA", "BE"])
    ctp_not_ba_bz = ~((pl.col("GWCTP") >= "BA") & (pl.col("GWCTP") <= "BZ"))
    otherwise_res = (
        (ctp_not_ba_bz & (pl.col("GWCNAL") == "MY") & (pl.col("GWSAC") != "UF")) |
        (pl.col("GWSAC") == "UF")
    )
    eligible_ctp = ctp_direct | otherwise_res
    condition_base = (
        (pl.col("GWOCY") == "MYR") &
        (pl.col("GWMVT") == "P") &
        (pl.col("GWMVTS") == "P") &
        ccy_ne_myr & eligible_ctp
    )
    condition_fbp = (
        (pl.col("GWOCY") == "MYR") &
        (pl.col("GWMVT") == "P") &
        (pl.col("GWMVTS") == "P") &
        (pl.col("GWDLP") == "FBP") & ccy_ne_myr
    )
    mask = (
        ((dlp_fxs_fbp & ~(pl.col("GWDLP") == "FBP")) | dlp_fxo_fxf | dlp_futures)
        & condition_base
    ) | (pl.col("GWDLP") == "FBP") & condition_fbp

    return df.with_columns(
        pl.when(mask).then(pl.lit("57100")).otherwise(pl.col("BNMCODE")).alias("BNMCODE")
    )


def _assign_bnmcode_57400(df: pl.DataFrame) -> pl.DataFrame:
    dlp_fxs      = pl.col("GWDLP") == "FXS"
    dlp_fxo_fxf  = pl.col("GWDLP").is_in(["FXO", "FXF"])
    dlp_futures  = pl.col("GWDLP").is_in(["SF1", "SF2", "TS1", "TS2", "FF1", "FF2"])
    ccy_ne_myr   = pl.col("GWCCY") != "MYR"
    ctp_direct_fxs = pl.col("GWCTP").is_in(["BC", "BB", "BI", "BM", "BA", "BE", "CE"])
    ctp_direct_std = pl.col("GWCTP").is_in(["BC", "BB", "BI", "BM", "BA", "BE"])
    ctp_not_ba_bz  = ~((pl.col("GWCTP") >= "BA") & (pl.col("GWCTP") <= "BZ"))
    otherwise_base = (
        (ctp_not_ba_bz & (pl.col("GWCNAL") == "MY") & (pl.col("GWSAC") != "UF")) |
        (pl.col("GWSAC") == "UF")
    )
    otherwise_fxo = otherwise_base | (pl.col("GWCTP") == "CE")
    base_cond = (
        (pl.col("GWOCY") == "MYR") &
        (pl.col("GWMVT") == "P") &
        (pl.col("GWMVTS") == "S") &
        ccy_ne_myr
    )
    mask = (
        (base_cond & dlp_fxs     & (ctp_direct_fxs | otherwise_base)) |
        (base_cond & dlp_fxo_fxf & (ctp_direct_std | otherwise_fxo))  |
        (base_cond & dlp_futures  & (ctp_direct_std | otherwise_base))
    )
    return df.with_columns(
        pl.when(mask).then(pl.lit("57400")).otherwise(pl.col("BNMCODE")).alias("BNMCODE")
    )


# Build K1TBX1
k1tbx1 = k1tbx_base.filter(
    (pl.col("GWOCY") != "XAT") & (pl.col("GWCCY") != "XAT")
)
k1tbx1 = _assign_bnmcode_57100(k1tbx1)
k1tbx1 = _assign_bnmcode_57400(k1tbx1)
k1tbx1 = k1tbx1.filter(pl.col("BNMCODE") != " ")

# Build K1TBX2
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

# Expand K1TBX rows per BNMCODE
combined_x = pl.concat(
    [
        k1tbx1.select([c for c in k1tbx1.columns
                       if c in ["GWCCY", "GWOCY", "BNMCODE", "AMOUNT", "MATDT", "GWSDT"]]),
        k1tbx2.select([c for c in k1tbx2.columns
                       if c in ["GWCCY", "GWOCY", "BNMCODE", "AMOUNT", "MATDT", "GWSDT"]]),
    ],
    how="diagonal"
)

k1tbx_output_rows = []
for row in combined_x.iter_rows(named=True):
    amount  = abs(row["AMOUNT"]) if row["AMOUNT"] < 0 else row["AMOUNT"]
    gwccy   = row["GWCCY"]
    gwocy   = row["GWOCY"]
    bnmcode = row["BNMCODE"]
    matdt   = row["MATDT"]
    issdt   = row.get("GWSDT", None)

    amtusd = amount if gwccy == "USD" else 0.0
    amtsgd = amount if gwccy == "SGD" else 0.0

    base_x = dict(AMOUNT=amount, MATDT=matdt, ISSDT=issdt,
                  GWCCY=gwccy, GWSHN="", GWC2R="", GWDLP="", GWDLR="")

    if bnmcode == "57100":
        k1tbx_output_rows.append({**base_x, "PART": "96", "ITEM": "711",
                                   "AMTUSD": amtusd, "AMTSGD": amtsgd})
        k1tbx_output_rows.append({**base_x, "PART": "95", "ITEM": "911",
                                   "AMTUSD": 0.0,   "AMTSGD": 0.0})
    elif bnmcode == "57400":
        k1tbx_output_rows.append({**base_x, "PART": "96", "ITEM": "911",
                                   "AMTUSD": amtusd, "AMTSGD": amtsgd})
        k1tbx_output_rows.append({**base_x, "PART": "95", "ITEM": "711",
                                   "AMTUSD": 0.0,   "AMTSGD": 0.0})
    elif bnmcode == "57600":
        k1tbx_output_rows.append({**base_x, "PART": "96", "ITEM": "711",
                                   "AMTUSD": amtusd, "AMTSGD": amtsgd})
        amtusd2 = amount if gwocy == "USD" else 0.0
        amtsgd2 = amount if gwocy == "SGD" else 0.0
        k1tbx_output_rows.append({**base_x, "PART": "96", "ITEM": "911",
                                   "AMTUSD": amtusd2, "AMTSGD": amtsgd2})

# ---------------------------------------------------------------------------
# Build K3TBL (PART 2) — rows emitted conditionally with ITEM assignment
# ---------------------------------------------------------------------------
# Reference: KALMLIQ4.py — builds K3TBL3 rows with CTYPE lookup
# The K3TBL DATA step logic below replicates KALMLIQ inline.

# Load CTYPE format lookup
con3 = duckdb.connect()
ctype_df = con3.execute("SELECT START AS UTCTP, LABEL AS CUST FROM read_parquet('data/CTYPE_FORMAT.parquet')").pl()
con3.close()

k3tbl_raw = k3tbl_raw.join(ctype_df, on="UTCTP", how="left")
k3tbl_raw = k3tbl_raw.with_columns(pl.col("CUST").fill_null("").alias("CUST"))

KEEP_K3TBL = ["PART", "ITEM", "MATDT", "AMOUNT", "AMTUSD", "AMTSGD",
               "ISSDT", "UTCCY", "UTCUS", "UTCTP", "UTSTY", "UTDLR", "UTDLP"]

output_k3tbl_rows = []

for row in k3tbl_raw.iter_rows(named=True):
    utccy  = row.get("UTCCY", "")  or ""
    utctp  = row.get("UTCTP", "")  or ""
    utsty  = row.get("UTSTY", "")  or ""
    utref  = row.get("UTREF", "")  or ""
    utdlp  = row.get("UTDLP", "")  or ""
    utcus  = row.get("UTCUS", "")  or ""
    utclc  = row.get("UTCLC", "")  or ""
    utdlr  = row.get("UTDLR", "")  or ""
    utmm1  = row.get("UTMM1", "")  or ""
    utamoc = row.get("UTAMOC", 0.0) or 0.0
    utdpf  = row.get("UTDPF",  0.0) or 0.0
    utaict = row.get("UTAICT", 0.0) or 0.0
    utpcp  = row.get("UTPCP",  0.0) or 0.0
    utdpey = row.get("UTDPEY", 0.0) or 0.0
    utdpe  = row.get("UTDPE",  0.0) or 0.0
    utaicy = row.get("UTAICY", 0.0) or 0.0
    utait  = row.get("UTAIT",  0.0) or 0.0
    matdt  = row.get("MATDT",  None)
    issdt  = row.get("ISSDT",  None)

    # AMOUNT = UTAMOC - UTDPF
    amount = utamoc - utdpf
    # IF UTSTY='IDC' THEN AMOUNT=UTAMOC + UTDPF
    if utsty == "IDC":
        amount = utamoc + utdpf

    if INST == "PBB":
        amtusd = amount if utccy == "USD" else 0.0
        amtsgd = amount if utccy == "SGD" else 0.0
    else:
        amtusd = 0.0
        amtsgd = 0.0

    base = dict(
        PART="95",
        MATDT=matdt,
        AMOUNT=amount,
        AMTUSD=amtusd,
        AMTSGD=amtsgd,
        ISSDT=issdt,
        UTCCY=utccy,
        UTCUS=utcus,
        UTCTP=utctp,
        UTSTY=utsty,
        UTDLR=utdlr,
        UTDLP=utdlp,
        ITEM=None,
    )

    #  IF UTREF IN ('INV','DRI','DLG','AFSLIQ','AFSBOND','IAFSLIQ','AFS','IAFS')
    if utref in ("INV", "DRI", "DLG", "AFSLIQ", "AFSBOND", "IAFSLIQ", "AFS", "IAFS"):
        if utsty in ("CB1", "CB2", "CF1", "CF2", "CNT", "MGS", "MTB", "BNB", "BNN",
                     "ITB", "SAC", "BMN", "BMC", "BMF", "SCD", "SCM",
                     "CMB", "MGI", "SMC"):
            amt = amount
            if INST == "PBB":
                amt = amount + utaict
            output_k3tbl_rows.append({**base, "ITEM": "631", "AMOUNT": amt,
                                       "AMTUSD": (amt if utccy == "USD" else 0.0) if INST == "PBB" else 0.0,
                                       "AMTSGD": (amt if utccy == "SGD" else 0.0) if INST == "PBB" else 0.0})

        elif utsty == "SDC":
            amt = amount
            if INST == "PBB":
                amt = (utamoc * (utpcp / 100)) + utdpey + utdpe
            output_k3tbl_rows.append({**base, "ITEM": "632", "AMOUNT": amt,
                                       "AMTUSD": (amt if utccy == "USD" else 0.0) if INST == "PBB" else 0.0,
                                       "AMTSGD": (amt if utccy == "SGD" else 0.0) if INST == "PBB" else 0.0})

        elif utsty == "LDC":
            amt = amount
            if INST == "PBB":
                amt = amount + utaict
            output_k3tbl_rows.append({**base, "ITEM": "632", "AMOUNT": amt,
                                       "AMTUSD": (amt if utccy == "USD" else 0.0) if INST == "PBB" else 0.0,
                                       "AMTSGD": (amt if utccy == "SGD" else 0.0) if INST == "PBB" else 0.0})

        elif utsty in ("SLD", "SSD"):
            amt = amount
            if INST == "PBB":
                amt = (utamoc * (utpcp / 100)) + utaicy + utait
            output_k3tbl_rows.append({**base, "ITEM": "632", "AMOUNT": amt,
                                       "AMTUSD": (amt if utccy == "USD" else 0.0) if INST == "PBB" else 0.0,
                                       "AMTSGD": (amt if utccy == "SGD" else 0.0) if INST == "PBB" else 0.0})

        elif utsty in ("SFD", "SZD"):
            amt = amount
            if INST == "PBB":
                amt = amount + utaict
            output_k3tbl_rows.append({**base, "ITEM": "632", "AMOUNT": amt,
                                       "AMTUSD": (amt if utccy == "USD" else 0.0) if INST == "PBB" else 0.0,
                                       "AMTSGD": (amt if utccy == "SGD" else 0.0) if INST == "PBB" else 0.0})

        elif utsty == "SBA":
            if utdlp not in ("MOS", "MSS"):
                output_k3tbl_rows.append({**base, "ITEM": "633"})

        elif utsty in ("ISB", "DHB", "KHA", "PNB"):
            output_k3tbl_rows.append({**base, "ITEM": "636"})

        elif utsty == "IDS":
            output_k3tbl_rows.append({**base, "ITEM": "635"})

        elif utsty == "DBD":
            output_k3tbl_rows.append({**base, "ITEM": "634"})

        elif utsty in ("DMB", "DBD", "GRL", "MTL", "RUL"):
            output_k3tbl_rows.append({**base, "ITEM": "635"})

        elif utsty == "PBA":
            if utdlp in ("MOS", "MSS"):
                output_k3tbl_rows.append({**base, "ITEM": "850"})

    elif utref in ("PFD", "PLD", "PSD", "PZD", "PDC"):
        if utsty in ("IFD", "ILD", "ISD", "IZD", "IDC", "IDP", "IZP"):
            output_k3tbl_rows.append({**base, "ITEM": "840"})

    #  ELSE IF UTREF IN ('IINV','IDRI','IDLG')
    elif utref in ("IINV", "IDRI", "IDLG"):
        if utsty == "SBA" and utdlp == "IOP":
            output_k3tbl_rows.append({**base, "ITEM": "633"})
        elif utsty in ("SDC", "LDC"):
            output_k3tbl_rows.append({**base, "ITEM": "632"})
        elif utsty in ("CB1", "CB2", "CF1", "CF2", "CNT", "MGI",
                       "ITB", "SAC", "BMN", "BMC", "BMF", "SCD", "SCM",
                       "MGS", "MTB", "BNB", "BNN", "CMB", "SMC"):
            amt = amount
            if INST == "PBB":
                amt = amount + utaict
            output_k3tbl_rows.append({**base, "ITEM": "631", "AMOUNT": amt,
                                       "AMTUSD": (amt if utccy == "USD" else 0.0) if INST == "PBB" else 0.0,
                                       "AMTSGD": (amt if utccy == "SGD" else 0.0) if INST == "PBB" else 0.0})
        elif utsty in ("ISB", "IDS", "IBZ", "ICN"):
            item = "636" if utmm1 == "GGB" else ("635" if utmm1 == "NGB" else None)
            amt  = amount + utaict
            if item:
                output_k3tbl_rows.append({**base, "ITEM": item, "AMOUNT": amt,
                                           "AMTUSD": (amt if utccy == "USD" else 0.0) if INST == "PBB" else 0.0,
                                           "AMTSGD": (amt if utccy == "SGD" else 0.0) if INST == "PBB" else 0.0})
        elif utsty in ("DHB", "KHA"):
            output_k3tbl_rows.append({**base, "ITEM": "636"})
        elif utsty == "DBD":
            output_k3tbl_rows.append({**base, "ITEM": "634"})

    # IF UTSTY IN ('SIP') THEN DO; (runs regardless of above UTREF branches)
    if utsty == "SIP":
        output_k3tbl_rows.append({**base, "ITEM": "610"})

    # ---------------------------------------------------------------------------
    # %INC PGM(KALMLIQ4) — inline logic from KALMLIQ4.py (K3TBL3 rows)
    # Reference: X_KALMLIQ4.py — filters UTREF='RRS', UTSTY='MGS', UTDLP='MSS'
    # and assigns ITEM 820/830 based on CTYPE lookup.
    # ---------------------------------------------------------------------------
    if utref == "RRS" and utsty == "MGS" and utdlp == "MSS":
        issdt_val = row.get("ISSDT", None)
        if issdt_val is not None and issdt_val <= REPTDATE:
            # AMOUNT = (UTPCP * UTFCV) * 0.01 + UTAICT
            utfcv  = row.get("UTFCV", 0.0) or 0.0
            amt_liq = (utpcp * utfcv) * 0.01 + utaict

            cust = row.get("CUST", "") or ""
            # Derive MATDT from UTIDT
            utidt = row.get("UTIDT", "") or ""
            matdt_liq = None
            if utidt.strip():
                try:
                    from datetime import datetime
                    matdt_liq = datetime.strptime(utidt.strip(), "%Y-%m-%d").date()
                except ValueError:
                    matdt_liq = None

            NREP = ("13", "17", "20", "60", "71", "72", "74", "76", "79", "85")
            IREP = ("01", "02", "11", "12", "81")

            item_liq = None
            if cust in NREP:
                item_liq = "830"
            elif cust in IREP:
                item_liq = "820"

            if cust.strip() and item_liq:
                output_k3tbl_rows.append({
                    "PART": "95", "ITEM": item_liq,
                    "MATDT": matdt_liq, "AMOUNT": amt_liq,
                    "AMTUSD": 0.0, "AMTSGD": 0.0,
                    "ISSDT": issdt_val,
                    "UTCCY": utccy, "UTCUS": utcus,
                    "UTCTP": utctp, "UTSTY": utsty,
                    "UTDLR": utdlr, "UTDLP": utdlp,
                })


# ---------------------------------------------------------------------------
# Materialise DataFrames for K1TBL, K3TBL, K1TBX
# ---------------------------------------------------------------------------
schema_k1tbl = {
    "PART":   pl.Utf8,  "ITEM":   pl.Utf8,
    "MATDT":  pl.Date,  "AMOUNT": pl.Float64,
    "AMTUSD": pl.Float64, "AMTSGD": pl.Float64,
    "ISSDT":  pl.Date,  "GWCCY":  pl.Utf8,
    "GWSHN":  pl.Utf8,  "GWC2R":  pl.Utf8,
    "GWDLP":  pl.Utf8,  "GWDLR":  pl.Utf8,
}
schema_k3tbl = {
    "PART":   pl.Utf8,  "ITEM":   pl.Utf8,
    "MATDT":  pl.Date,  "AMOUNT": pl.Float64,
    "AMTUSD": pl.Float64, "AMTSGD": pl.Float64,
    "ISSDT":  pl.Date,  "UTCCY":  pl.Utf8,
    "UTCUS":  pl.Utf8,  "UTCTP":  pl.Utf8,
    "UTSTY":  pl.Utf8,  "UTDLR":  pl.Utf8,
    "UTDLP":  pl.Utf8,
}

k1tbl_df = pl.DataFrame(output_k1tbl_rows, schema=schema_k1tbl) if output_k1tbl_rows else pl.DataFrame(schema=schema_k1tbl)
k3tbl_df = pl.DataFrame(output_k3tbl_rows, schema=schema_k3tbl) if output_k3tbl_rows else pl.DataFrame(schema=schema_k3tbl)
k1tbx_df = pl.DataFrame(k1tbx_output_rows, schema={
    "PART":   pl.Utf8,  "ITEM":   pl.Utf8,
    "MATDT":  pl.Date,  "AMOUNT": pl.Float64,
    "AMTUSD": pl.Float64, "AMTSGD": pl.Float64,
    "ISSDT":  pl.Date,  "GWCCY":  pl.Utf8,
    "GWSHN":  pl.Utf8,  "GWC2R":  pl.Utf8,
    "GWDLP":  pl.Utf8,  "GWDLR":  pl.Utf8,
}) if k1tbx_output_rows else pl.DataFrame()

# ---------------------------------------------------------------------------
# Write K1TBL and K3TBL intermediate outputs
# ---------------------------------------------------------------------------
k1tbl_df.write_csv(OUTPUT_K1TBL_FILE, separator="|", null_value="")
k3tbl_df.write_csv(OUTPUT_K3TBL_FILE, separator="|", null_value="")

print(f"K1TBL written to {OUTPUT_K1TBL_FILE}  ({len(k1tbl_df)} rows)")
print(f"K3TBL written to {OUTPUT_K3TBL_FILE}  ({len(k3tbl_df)} rows)")

# ===========================================================================
# DATA KTBL — combine K1TBL + K3TBL + K1TBX, compute BNMCODE
# ===========================================================================

# %DCLVAR macro equivalent — variable declarations (inlined, no separate action needed)

# Load REPTDATE info (RPYR, RPMTH, RPDAY, RD2)
rpyr  = REPTDATE.year
rpmth = REPTDATE.month
rpday = REPTDATE.day
rd2   = 29 if (rpyr % 4 == 0) else 28

# Tag source table
k1tbl_tagged = k1tbl_df.with_columns(pl.lit("1").alias("TBL"))
k3tbl_tagged = k3tbl_df.with_columns(pl.lit("3").alias("TBL"))

# Align schemas for concat — add missing cols with nulls
for col in ["GWCCY", "GWSHN", "GWC2R", "GWDLP", "GWDLR"]:
    if col not in k3tbl_tagged.columns:
        k3tbl_tagged = k3tbl_tagged.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
for col in ["UTCCY", "UTCUS", "UTCTP", "UTSTY", "UTDLR", "UTDLP"]:
    if col not in k1tbl_tagged.columns:
        k1tbl_tagged = k1tbl_tagged.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
if k1tbx_df.is_empty():
    k1tbx_tagged = pl.DataFrame()
else:
    k1tbx_tagged = k1tbx_df.with_columns(pl.lit("X").alias("TBL"))
    for col in ["UTCCY", "UTCUS", "UTCTP", "UTSTY", "UTDLR", "UTDLP"]:
        if col not in k1tbx_tagged.columns:
            k1tbx_tagged = k1tbx_tagged.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

frames = [f for f in [k1tbl_tagged, k3tbl_tagged, k1tbx_tagged] if not f.is_empty()]
combined_all = pl.concat(frames, how="diagonal") if frames else pl.DataFrame()

# IF ITEM ^= ' ' — filter out blank ITEM
combined_all = combined_all.filter(
    pl.col("ITEM").is_not_null() & (pl.col("ITEM").str.strip_chars() != "")
)

# Compute REMMTH and BNMCODE row by row
ktbl_rows    = []
ktblall_rows = []

for row in combined_all.iter_rows(named=True):
    matdt  = row["MATDT"]
    issdt  = row["ISSDT"]
    part   = row["PART"] or ""
    item   = row["ITEM"] or ""
    amount = row["AMOUNT"] or 0.0
    amtusd = row["AMTUSD"] or 0.0
    amtsgd = row["AMTSGD"] or 0.0

    if matdt is None:
        continue

    # IF MATDT - REPTDATE < 8 THEN REMMTH = 0.1; ELSE %REMMTH
    diff_rep = (matdt - REPTDATE).days
    remmth   = compute_remmth(matdt, REPTDATE)
    remmth_fmt = format_remmth(remmth)

    # IF MATDT - ISSDT < 8 THEN ORI30D = 0.1; ELSE ORI30D = (MATDT-ISSDT)/30
    if issdt is not None:
        diff_ori = (matdt - issdt).days
        ori30d   = 0.1 if diff_ori < 8 else diff_ori / 30.0
    else:
        ori30d = 0.1

    # BNMCODE = PART||ITEM||'00'||PUT(REMMTH,REMFMT.)||'0000Y'
    bnmcode = f"{part}{item}00{remmth_fmt}0000Y"

    rec = dict(BNMCODE=bnmcode, AMOUNT=amount, AMTUSD=amtusd, AMTSGD=amtsgd)
    ktbl_rows.append(rec)

    # KTBLALL contains all columns
    ktblall_rows.append({**row, "BNMCODE": bnmcode, "REMMTH": remmth, "ORI30D": ori30d})

    # --------------------------------------------------------
    # DUPLICATE ANOTHER SET FOR PART 1
    # 95 = PART 2-RM, 96 = PART 2-FX
    # 93 = PART 1-RM, 94 = PART 1-FX
    # --------------------------------------------------------
    part1 = "93" if part == "95" else "94"
    bnmcode_p1 = part1 + bnmcode[2:]

    ktbl_rows.append({**rec, "BNMCODE": bnmcode_p1})
    ktblall_rows.append({**row, "BNMCODE": bnmcode_p1, "REMMTH": remmth, "ORI30D": ori30d})

# Materialise KTBL and KTBLALL
ktbl_df = pl.DataFrame(ktbl_rows, schema={
    "BNMCODE": pl.Utf8,
    "AMOUNT":  pl.Float64,
    "AMTUSD":  pl.Float64,
    "AMTSGD":  pl.Float64,
}) if ktbl_rows else pl.DataFrame()

ktblall_df = pl.DataFrame(ktblall_rows) if ktblall_rows else pl.DataFrame()

ktbl_df.write_csv(OUTPUT_KTBL_FILE,    separator="|", null_value="")
ktblall_df.write_csv(OUTPUT_KTBLALL_FILE, separator="|", null_value="")

print(f"KTBL written to    {OUTPUT_KTBL_FILE}    ({len(ktbl_df)} rows)")
print(f"KTBLALL written to {OUTPUT_KTBLALL_FILE} ({len(ktblall_df)} rows)")

# ===========================================================================
# PART 3: DISTRIBUTION PROFILE OF CUSTOMER DEPOSITS
# ===========================================================================

# ------------------------------------------------
# NON-INTERBANK REPOS (from K1TBL source)
# ------------------------------------------------
con4 = duckdb.connect()
k1tbl_part3 = con4.execute(f"""
    SELECT GWBALC AS AMOUNT, GWSHN AS NAME
    FROM read_parquet('{input_k1tbl_path}')
    WHERE GWCCY  = 'MYR'
      AND GWMVT  = 'P'
      AND GWMVTS = 'M'
      AND SUBSTR(GWCTP, 1, 1) <> 'B'
      AND SUBSTR(GWDLP, 2, 2) IN ('MI', 'MT')
""").pl().with_columns(
    pl.lit("NON-INTERBANK REPOS").alias("CAT")
).select(["CAT", "NAME", "AMOUNT"])

# ------------------------------------------------
# NON-INTERBANK NIDS (from K3TBL source)
# ------------------------------------------------
k3tbl_part3 = con4.execute(f"""
    SELECT UTCUS || UTCLC AS NAME,
           UTAMOC - UTDPF AS AMOUNT
    FROM read_parquet('{input_k3tbl_path}')
    WHERE SUBSTR(UTCTP, 1, 1) <> 'B'
      AND UTREF IN ('PFD','PLD','PSD','PZD','PDC')
      AND UTSTY IN ('IFD','ILD','ISD','IZD','IDC','IDP','IZP')
""").pl().with_columns(
    pl.lit("NON-INTERBANK NIDS").alias("CAT")
).select(["CAT", "NAME", "AMOUNT"])

con4.close()

# PROC APPEND BASE=K1TBL DATA=K3TBL
k1tbl_part3_combined = pl.concat([k1tbl_part3, k3tbl_part3], how="diagonal")

# PROC SUMMARY (GROUP BY CAT, NAME; SUM AMOUNT)
k1tbl_part3_summary = (
    k1tbl_part3_combined
    .group_by(["CAT", "NAME"])
    .agg(pl.col("AMOUNT").sum())
    .sort(["CAT", "NAME"])
)

k1tbl_part3_summary.write_csv(OUTPUT_K1TBL_PART3_FILE, separator="|", null_value="")
print(f"K1TBL (Part 3) written to {OUTPUT_K1TBL_PART3_FILE} ({len(k1tbl_part3_summary)} rows)")
