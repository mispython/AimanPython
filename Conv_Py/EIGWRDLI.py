#!/usr/bin/env python3
"""
Program  : EIGWRDLI.py
Purpose  : Weekly/Monthly BNM RDAL (Asset & Liability) and NSRS reporting.
           Merges PBBRDAL reference ITCODE list with BNM ALW file,
            appends CAG (special postal code loan data), then splits
            into AL / OB / SP sections and writes RDAL and NSRS output files.

           Logic:
           - If NOWK == '4'  -> run MONTHLY path (PBBMRDLF)
           - Otherwise       -> run WEEKLY  path (PBBWRDLF)
           - Merge ALW BNM file with PBBRDAL1 by ITCODE
           - Append CAG (conventional cost-centre + postal-code filtered loans)
           - Route rows into AL / OB / SP based on ITCODE prefix rules
           - Write RDAL output  (amounts divided by 1000, rounded)
           - Write NSRS output  (amounts rounded to nearest 1, except '80' prefix -> /1000)
"""

import os
import duckdb
import polars as pl
from pathlib import Path

# PBBLNFMT is imported for format completeness per %INC PGM(PBBLNFMT) in SAS.
# No PBBLNFMT format functions are directly called in this program;
# the %INC was used to load macro variables / options in the SAS environment.
# from PBBLNFMT import ...

# PBBWRDLF and PBBMRDLF are imported as modules that produce PBBRDAL.parquet.
# They are invoked conditionally below based on NOWK.
import PBBWRDLF  # noqa: F401  -- run as side effect to generate PBBRDAL.parquet (WEEKLY)
import PBBMRDLF  # noqa: F401  -- run as side effect to generate PBBRDAL.parquet (MONTHLY)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR     = Path(__file__).resolve().parent
DATA_DIR     = BASE_DIR / "data"
OUTPUT_DIR   = BASE_DIR / "output"
BNM_DIR      = DATA_DIR / "bnm"       # BNM library (ALW parquet files)
LOAN_DIR     = DATA_DIR / "loan"      # LOAN library (LNNOTE parquet)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PBBRDAL_FILE = OUTPUT_DIR / "PBBRDAL.parquet"   # produced by PBBWRDLF / PBBMRDLF
LNNOTE_FILE  = LOAN_DIR   / "LNNOTE.parquet"

RDAL_OUT     = OUTPUT_DIR / "RDAL.txt"
NSRS_OUT     = OUTPUT_DIR / "NSRS.txt"

# ============================================================================
# RUNTIME PARAMETERS  (equivalent to SAS macro variables)
# Replace with actual values or load from config / command-line arguments.
# ============================================================================

REPTDAY   = "15"    # e.g. '08', '15', '22', '31'
REPTMON   = "03"    # e.g. '01' .. '12'
REPTYEAR  = "2026"  # e.g. '2026'
NOWK      = "1"     # '1','2','3' = weekly ; '4' = monthly (last week of month)

# ============================================================================
# STEP 1: WEEKLY / MONTHLY DISPATCH  (%GET_BICS / %WEEKLY / %MONTHLY)
# ============================================================================
# %IF "&NOWK" EQ "4" %THEN %MONTHLY; %ELSE %WEEKLY;
# Both PBBWRDLF and PBBMRDLF write PBBRDAL.parquet to OUTPUT_DIR.
# We re-execute the appropriate module's main logic here.

def run_weekly():
    """Equivalent to %MACRO WEEKLY -> %INC PGM(PBBWRDLF)"""
    # PBBWRDLF writes PBBRDAL.parquet on import/execution.
    # Re-create the parquet using the weekly ITCODE list.
    itcode_data = [
        "3313002000000Y", "3313003000000Y", "4017000000000Y", "4019000000000Y",
        "4216060000000Y", "4261076000000Y", "4261085000000Y", "4263076000000Y",
        "4263085000000Y", "4269981000000Y", "4313002000000Y", "4313003000000Y",
        "5422000000000Y", "7200000008310Y", "7300000003000Y", "7300000006100Y",
        "7300000008310Y", "7300000008320Y",
    ]
    df = pl.DataFrame({"ITCODE": itcode_data})
    df.write_parquet(PBBRDAL_FILE)


def run_monthly():
    """Equivalent to %MACRO MONTHLY -> %INC PGM(PBBMRDLF)"""
    # Re-create the parquet using the monthly ITCODE list.
    itcode_data = [
        "3313002000000Y", "3313003000000Y", "4019000000000Y", "4216060000000Y",
        "4261076000000Y", "4261085000000Y", "4263076000000Y", "4263085000000Y",
        "4269981000000Y", "4313002000000Y", "4313003000000Y", "7200000008310Y",
        "7300000003000Y", "7300000006100Y", "7300000008310Y", "7300000008320Y",
        "5422000000000Y", "4017000000000Y", "3051577000000Y", "3054077000000Y",
        "3055060000000Y", "3055061000000Y", "3055076000000Y", "3055077000000Y",
        "3056000000000Y", "3400010000310Y", "3400010008100Y", "3400020000100Y",
        "3400020000110Y", "3400000000132Y", "3400077000420Y", "3400078000132Y",
        "3415100000000Y", "3415200000000Y", "3415900000000Y", "3416000000000Y",
        "3420000000420Y", "7211500000000Y", "7312000000000Y", "7318000000000Y",
        "7411000000000Y", "7412000000000Y", "7413000000000Y", "7414000000000Y",
    ]
    df = pl.DataFrame({"ITCODE": itcode_data})
    df.write_parquet(PBBRDAL_FILE)


if NOWK == "4":
    run_monthly()
else:
    run_weekly()


# ============================================================================
# STEP 2: %MRGBIC — build PBBRDAL1, merge with BNM ALW file -> RDAL
# ============================================================================

# DATA PBBRDAL1: SET PBBRDAL; AMTIND='I'; AMOUNT=0;
pbbrdal = pl.read_parquet(PBBRDAL_FILE)
pbbrdal1 = pbbrdal.with_columns([
    pl.lit("I").alias("AMTIND"),
    pl.lit(0).cast(pl.Float64).alias("AMOUNT"),
])
# PROC SORT DATA=PBBRDAL1 BY ITCODE;  (sort before merge)
pbbrdal1 = pbbrdal1.sort("ITCODE")

# PROC SORT DATA=BNM.ALW&REPTMON&NOWK BY ITCODE;
alw_file = BNM_DIR / f"ALW{REPTMON}{NOWK}.parquet"
con = duckdb.connect()
alw = con.execute(f"SELECT * FROM read_parquet('{alw_file}') ORDER BY ITCODE").pl()
con.close()

# DATA RDAL: MERGE ALW (RENAME AMOUNT->AMT1) PBBRDAL1 (RENAME AMOUNT->AMT2) BY ITCODE;
# Full outer merge
rdal_merge = alw.rename({"AMOUNT": "AMT1"}).join(
    pbbrdal1.rename({"AMOUNT": "AMT2"}),
    on="ITCODE",
    how="full",
    suffix="_r",
)

# Resolve AMOUNT: A and B -> AMT1; NOT A and B -> AMT2; A and NOT B -> AMT1
def resolve_amount(row):
    amt1 = row.get("AMT1")
    amt2 = row.get("AMT2")
    in_a = amt1 is not None
    in_b = amt2 is not None
    if in_a and in_b:
        return amt1
    elif not in_a and in_b:
        return amt2
    else:  # in_a and not in_b
        return amt1

rdal_merge = rdal_merge.with_columns(
    pl.struct(["AMT1", "AMT2"]).map_elements(
        lambda s: s["AMT1"] if s["AMT1"] is not None else s["AMT2"],
        return_dtype=pl.Float64,
    ).alias("AMOUNT")
)

# IF NOT ('30221' <= SUBSTR(ITCODE,1,5) <= '30228') &
#    NOT ('30231' <= SUBSTR(ITCODE,1,5) <= '30238') &
#    NOT ('30091' <= SUBSTR(ITCODE,1,5) <= '30098') &
#    NOT ('40151' <= SUBSTR(ITCODE,1,5) <= '40158') &
#    SUBSTR(ITCODE,1,5) NOT IN ('NSSTS');
rdal = rdal_merge.filter(
    ~(
        (pl.col("ITCODE").str.slice(0, 5) >= "30221") &
        (pl.col("ITCODE").str.slice(0, 5) <= "30228")
    ) &
    ~(
        (pl.col("ITCODE").str.slice(0, 5) >= "30231") &
        (pl.col("ITCODE").str.slice(0, 5) <= "30238")
    ) &
    ~(
        (pl.col("ITCODE").str.slice(0, 5) >= "30091") &
        (pl.col("ITCODE").str.slice(0, 5) <= "30098")
    ) &
    ~(
        (pl.col("ITCODE").str.slice(0, 5) >= "40151") &
        (pl.col("ITCODE").str.slice(0, 5) <= "40158")
    ) &
    (pl.col("ITCODE").str.slice(0, 5) != "NSSTS")
).select(["ITCODE", "AMTIND", "AMOUNT"])

# ============================================================================
# STEP 3: CAG — special postal code loans (conventional cost centres only)
# ============================================================================

# DATA CAG: SET LOAN.LNNOTE;
#   IF PZIPCODE IN (...);
#   IF (3000<=COSTCTR<=3999) OR COSTCTR IN (4043,4048);
#   AMTIND='I'; ITCODE='7511100000000Y';
PZIPCODE_LIST = [
    2002, 2013, 3039, 3047,
    800003098, 800003114, 800004016, 800004022, 800004029,
    800040050, 800040053, 800050024, 800060024, 800060045,
    800060081, 80060085,
]

con2 = duckdb.connect()
lnnote = con2.execute(f"SELECT * FROM read_parquet('{LNNOTE_FILE}')").pl()
con2.close()

cag = lnnote.filter(
    pl.col("PZIPCODE").is_in(PZIPCODE_LIST) &
    (
        ((pl.col("COSTCTR") >= 3000) & (pl.col("COSTCTR") <= 3999)) |
        pl.col("COSTCTR").is_in([4043, 4048])
    )
).with_columns([
    pl.lit("I").alias("AMTIND"),
    pl.lit("7511100000000Y").alias("ITCODE"),
])

# PROC SUMMARY DATA=CAG NWAY; CLASS ITCODE AMTIND; VAR BALANCE;
# OUTPUT OUT=CAG (DROP=_FREQ_ _TYPE_) SUM=AMOUNT;
cag_summary = (
    cag.group_by(["ITCODE", "AMTIND"])
    .agg(pl.col("BALANCE").sum().alias("AMOUNT"))
)

# DATA RDAL: SET RDAL CAG;
rdal = pl.concat([rdal, cag_summary], how="diagonal_relaxed")

# PROC SORT DATA=RDAL BY ITCODE AMTIND;
rdal = rdal.sort(["ITCODE", "AMTIND"])

# ============================================================================
# STEP 4: Route RDAL rows into AL / OB / SP datasets
# ============================================================================

# DATA AL OB SP: SET RDAL;
#   IF ITCODE='4314020000000Y' THEN DO; AMOUNT=ABS(AMOUNT); IF AMTIND='D' THEN DELETE; END;
#   IF AMTIND ^= ' ' THEN DO;
#     IF SUBSTR(ITCODE,1,3) IN ('307') THEN OUTPUT SP;
#     ELSE IF SUBSTR(ITCODE,1,5) IN ('40190') THEN OUTPUT SP;
#     ELSE IF SUBSTR(ITCODE,1,5) IN ('40191') THEN OUTPUT SP;
#     ELSE IF SUBSTR(ITCODE,1,4) = 'SSTS' THEN DO; ITCODE='4017000000000Y'; OUTPUT SP; END;
#     ELSE IF SUBSTR(ITCODE,1,1) ^= '5' THEN DO;
#       IF SUBSTR(ITCODE,1,3) IN ('685','785') THEN OUTPUT SP;
#       ELSE OUTPUT AL;
#     END;
#     ELSE OUTPUT OB;
#   END;
#   ELSE IF SUBSTR(ITCODE,2,1)='0' THEN OUTPUT SP;

rows_al = []
rows_ob = []
rows_sp = []

for row in rdal.to_dicts():
    itcode  = str(row.get("ITCODE") or "")
    amtind  = str(row.get("AMTIND") or "")
    amount  = row.get("AMOUNT") or 0.0

    # IF ITCODE='4314020000000Y' THEN DO; AMOUNT=ABS(AMOUNT); IF AMTIND='D' THEN DELETE; END;
    if itcode == "4314020000000Y":
        amount = abs(amount)
        if amtind == "D":
            continue
        row["AMOUNT"] = amount

    if amtind != " " and amtind != "":
        p3  = itcode[:3]
        p5  = itcode[:5]
        p4  = itcode[:4]
        p1  = itcode[:1]
        if p3 == "307":
            rows_sp.append(row)
        elif p5 == "40190":
            rows_sp.append(row)
        elif p5 == "40191":
            rows_sp.append(row)
        elif p4 == "SSTS":
            row = dict(row)
            row["ITCODE"] = "4017000000000Y"
            rows_sp.append(row)
        elif p1 != "5":
            if p3 in ("685", "785"):
                rows_sp.append(row)
            else:
                rows_al.append(row)
        else:
            rows_ob.append(row)
    else:
        # SUBSTR(ITCODE,2,1)='0' -> SAS 1-based, char at position 2 = index 1
        if len(itcode) > 1 and itcode[1] == "0":
            rows_sp.append(row)

al = pl.DataFrame(rows_al) if rows_al else pl.DataFrame({"ITCODE": [], "AMTIND": [], "AMOUNT": []})
ob = pl.DataFrame(rows_ob) if rows_ob else pl.DataFrame({"ITCODE": [], "AMTIND": [], "AMOUNT": []})
sp = pl.DataFrame(rows_sp) if rows_sp else pl.DataFrame({"ITCODE": [], "AMTIND": [], "AMOUNT": []})

# PROC SORT DATA=SP BY ITCODE;
sp = sp.sort("ITCODE")

# ============================================================================
# STEP 5: Write RDAL output file
# ============================================================================

# Header: PHEAD = 'RDAL' || REPTDAY || REPTMON || REPTYEAR
PHEAD = f"RDAL{REPTDAY}{REPTMON}{REPTYEAR}"

with open(RDAL_OUT, "w") as f:
    # ---- AL section ----
    # DATA _NULL_: SET AL; BY ITCODE;
    f.write(PHEAD + "\n")
    f.write("AL\n")

    al_sorted = al.sort("ITCODE")
    amounti   = 0.0
    prev_itcode = None

    for row in al_sorted.to_dicts():
        itcode = str(row["ITCODE"])
        amount = float(row.get("AMOUNT") or 0)
        amtind = str(row.get("AMTIND") or "")

        # PROCEED logic: suppress 4003000000000Y with prefix 68/78 on days 08 & 22
        proceed = "Y"
        if REPTDAY in ("08", "22"):
            if itcode == "4003000000000Y" and itcode[:2] in ("68", "78"):
                proceed = "N"
        if proceed != "Y":
            continue

        # AMOUNT=ROUND(AMOUNT/1000); AMOUNTI+AMOUNT;
        amount_k = round(amount / 1000)
        amounti += amount_k

        # IF LAST.ITCODE THEN DO;
        #   PUT @1 ITCODE +(-1) ';' AMOUNTI +(-1) ';' AMOUNTI;
        #   AMOUNTI=0;
        # END;
        if itcode != prev_itcode and prev_itcode is not None:
            # flush previous group (we detect last by change of key)
            pass  # handled below

        prev_itcode = itcode

    # Re-iterate with proper group-by aggregation for LAST.ITCODE pattern
    al_grp = (
        al_sorted.with_columns(
            pl.col("AMOUNT").map_elements(lambda a: round(a / 1000), return_dtype=pl.Int64)
        )
    )

    # Filter PROCEED (days 08/22 suppression)
    if REPTDAY in ("08", "22"):
        al_grp = al_grp.filter(
            ~(
                (pl.col("ITCODE") == "4003000000000Y") &
                (pl.col("ITCODE").str.slice(0, 2).is_in(["68", "78"]))
            )
        )

    # Reopen file to rewrite AL section cleanly
    with open(RDAL_OUT, "w") as f:
        f.write(PHEAD + "\n")
        f.write("AL\n")

        al_agg = (
            al_grp.group_by("ITCODE", maintain_order=True)
            .agg(pl.col("AMOUNT").sum().alias("AMOUNTI"))
            .sort("ITCODE")
        )
        for row in al_agg.to_dicts():
            itcode  = str(row["ITCODE"])
            amounti = int(row["AMOUNTI"])
            # PUT @1 ITCODE +(-1) ';' AMOUNTI +(-1) ';' AMOUNTI;
            f.write(f"{itcode};{amounti};{amounti}\n")

        # ---- OB section ----
        # DATA _NULL_: SET OB; BY ITCODE AMTIND;
        f.write("OB\n")

        ob_sorted = ob.sort(["ITCODE", "AMTIND"])
        ob_grp = ob_sorted.with_columns(
            pl.col("AMOUNT").map_elements(lambda a: round(a / 1000), return_dtype=pl.Int64)
        )
        ob_agg = (
            ob_grp.group_by("ITCODE", maintain_order=True)
            .agg(pl.col("AMOUNT").sum().alias("AMOUNTI"))
            .sort("ITCODE")
        )
        for row in ob_agg.to_dicts():
            itcode  = str(row["ITCODE"])
            amounti = int(row["AMOUNTI"])
            # PUT @1 ITCODE +(-1) ';' AMOUNTI +(-1) ';' AMOUNTI;
            f.write(f"{itcode};{amounti};{amounti}\n")

        # ---- SP section ----
        # DATA _NULL_: SET SP; BY ITCODE;
        # AMOUNTI+AMOUNT; IF LAST.ITCODE THEN AMOUNTI=ROUND(AMOUNTI/1000);
        # PUT @1 ITCODE +(-1) ';' AMOUNTI +(-1);
        f.write("SP\n")

        sp_agg = (
            sp.group_by("ITCODE", maintain_order=True)
            .agg(pl.col("AMOUNT").sum().alias("AMOUNTI_RAW"))
            .sort("ITCODE")
        )
        for row in sp_agg.to_dicts():
            itcode     = str(row["ITCODE"])
            amounti    = round(row["AMOUNTI_RAW"] / 1000)
            # PUT @1 ITCODE +(-1) ';' AMOUNTI +(-1);
            f.write(f"{itcode};{amounti}\n")

print(f"RDAL output written -> {RDAL_OUT}")

# ============================================================================
# STEP 6: Write NSRS output file
# ============================================================================
# NSRS logic differs from RDAL:
#   AL  section: AMOUNT=ROUND(AMOUNT); if prefix '80' -> ROUND(AMOUNT/1000)
#   OB  section: AMOUNTI+ROUND(AMOUNT); if prefix '80' -> AMOUNT=ROUND(AMOUNT/1000)
#   SP  section: AMOUNTI+AMOUNT; if last -> AMOUNTI=ROUND(AMOUNTI);
#                               if prefix '80' -> AMOUNT=ROUND(AMOUNT/1000)

with open(NSRS_OUT, "w") as f:
    # ---- AL section (NSRS) ----
    f.write(PHEAD + "\n")
    f.write("AL\n")

    al_sorted_nsrs = al.sort("ITCODE")

    # Apply PROCEED filter same as RDAL
    if REPTDAY in ("08", "22"):
        al_sorted_nsrs = al_sorted_nsrs.filter(
            ~(
                (pl.col("ITCODE") == "4003000000000Y") &
                (pl.col("ITCODE").str.slice(0, 2).is_in(["68", "78"]))
            )
        )

    def nsrs_al_amount(amount: float, itcode: str) -> int:
        """AMOUNT=ROUND(AMOUNT); if prefix '80' -> ROUND(AMOUNT/1000)"""
        if itcode[:2] == "80":
            return round(amount / 1000)
        return round(amount)

    # Group by ITCODE, accumulate per-row rounded amounts
    nsrs_al_rows: dict = {}
    for row in al_sorted_nsrs.to_dicts():
        itcode = str(row["ITCODE"])
        amount = float(row.get("AMOUNT") or 0)
        contrib = nsrs_al_amount(amount, itcode)
        nsrs_al_rows[itcode] = nsrs_al_rows.get(itcode, 0) + contrib

    for itcode in sorted(nsrs_al_rows):
        amounti = nsrs_al_rows[itcode]
        # PUT @1 ITCODE +(-1) ';' AMOUNTI +(-1) ';' AMOUNTI;
        f.write(f"{itcode};{amounti};{amounti}\n")

    # ---- OB section (NSRS) ----
    # AMOUNTI+ROUND(AMOUNT); if prefix '80' -> AMOUNT=ROUND(AMOUNT/1000)
    # Note: SAS sets AMOUNT=ROUND(AMOUNT/1000) but then uses original AMOUNT for accumulation
    # The code reads: AMOUNTI+ROUND(AMOUNT); [then] IF '80' THEN AMOUNT=ROUND(AMOUNT/1000)
    # The AMOUNT assignment after accumulation does not affect AMOUNTI for that iteration.
    f.write("OB\n")

    ob_sorted_nsrs = ob.sort(["ITCODE", "AMTIND"])
    nsrs_ob_rows: dict = {}
    for row in ob_sorted_nsrs.to_dicts():
        itcode = str(row["ITCODE"])
        amount = float(row.get("AMOUNT") or 0)
        contrib = round(amount)  # AMOUNTI+ROUND(AMOUNT)
        nsrs_ob_rows[itcode] = nsrs_ob_rows.get(itcode, 0) + contrib

    for itcode in sorted(nsrs_ob_rows):
        amounti = nsrs_ob_rows[itcode]
        # PUT @1 ITCODE +(-1) ';' AMOUNTI +(-1) ';' AMOUNTI;
        f.write(f"{itcode};{amounti};{amounti}\n")

    # ---- SP section (NSRS) ----
    # AMOUNTI+AMOUNT; IF LAST.ITCODE THEN AMOUNTI=ROUND(AMOUNTI);
    # IF prefix '80' THEN AMOUNT=ROUND(AMOUNT/1000);  <- applied after AMOUNTI accumulation
    # PUT @1 ITCODE +(-1) ';' AMOUNTI +(-1);
    f.write("SP\n")

    sp_sorted_nsrs = sp.sort("ITCODE")
    nsrs_sp_rows: dict = {}
    for row in sp_sorted_nsrs.to_dicts():
        itcode = str(row["ITCODE"])
        amount = float(row.get("AMOUNT") or 0)
        nsrs_sp_rows[itcode] = nsrs_sp_rows.get(itcode, 0) + amount

    for itcode in sorted(nsrs_sp_rows):
        amounti_raw = nsrs_sp_rows[itcode]
        amounti     = round(amounti_raw)
        # IF prefix '80' -> AMOUNT=ROUND(AMOUNT/1000) (post-accumulation assignment; amounti unchanged)
        # PUT @1 ITCODE +(-1) ';' AMOUNTI +(-1);
        f.write(f"{itcode};{amounti}\n")

print(f"NSRS output written -> {NSRS_OUT}")
