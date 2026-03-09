#!/usr/bin/env python3
"""
Program  : EIBM0930.py
Purpose  : YUSNITA 2010-930 — Loans GL count report.
           Produces fixed-width GL interface file (GLINT) and
           unmatched product cross-match reports (XMATCH, IMATCH).

           Reads  : BNM.REPTDATE                 (parquet)
                    BNM.LOAN<REPTMON><NOWK>       (parquet) — PBB loans
                    BNMI.LOAN<REPTMON><NOWK>      (parquet) — PIBB loans
                    BTBNM.BTRAD<REPTMON><NOWK>    (parquet) — BT trade bills
                    IBTNM.IBTRAD<REPTMON><NOWK>   (parquet) — IBT trade bills
                    LNFMT   (text, fixed-width)  — loan product/LOB format
                    ODFMT   (text, fixed-width)  — OD product/LOB format
                    DRFMT   (CSV)               — branch driver mapping

           Writes : output/GLINT<REPTMON><NOWK>.txt  — GL interface (LRECL=500)
                    output/XMATCH.txt                 — PBB unmatched products
                    output/IMATCH.txt                 — PIBB unmatched products

           Dependencies: PBBLNFMT (imported but format functions used implicitly
                         via BNM.LOAN field derivations upstream)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import logging
from datetime import date
from pathlib import Path

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# Note: %INC PGM(PBBLNFMT) appears in the SAS header as a global include.
#       This program reads BNM.LOAN (already formatted upstream) and reads LNFMT/
#       ODFMT/DRFMT from flat files. No PBBLNFMT format functions are called here.
# ============================================================================

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path(".")
DATA_DIR   = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BNM_REPTDATE_PATH  = DATA_DIR / "bnm"   / "reptdate.parquet"
LNFMT_PATH         = DATA_DIR / "fmt"   / "lnfmt.txt"           # SAP.PBB.FMS.LN
ODFMT_PATH         = DATA_DIR / "fmt"   / "odfmt.txt"           # SAP.PBB.FMS.DP
DRFMT_PATH         = DATA_DIR / "fmt"   / "drfmt.csv"           # FDP.APPL.FMS.DRIVER.MAPPING
BNM_DATA_DIR       = DATA_DIR / "bnm"                            # BNM.LOAN<REPTMON><NOWK>
BNMI_DATA_DIR      = DATA_DIR / "bnmi"                           # BNMI.LOAN<REPTMON><NOWK>
BTBNM_DATA_DIR     = DATA_DIR / "btbnm"                          # BTBNM.BTRAD<REPTMON><NOWK>
IBTNM_DATA_DIR     = DATA_DIR / "ibtnm"                          # IBTNM.IBTRAD<REPTMON><NOWK>

GLINT_PATH  = OUTPUT_DIR / "GLINT.txt"      # updated with reptmon/nowk in main()
XMATCH_PATH = OUTPUT_DIR / "XMATCH.txt"
IMATCH_PATH = OUTPUT_DIR / "IMATCH.txt"

# ============================================================================
# CONSTANTS
# ============================================================================
GLINT_LRECL = 500

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ============================================================================
# HELPER: derive week number from day-of-month (same logic as EIBM0930)
# ============================================================================
def derive_nowk(day: int) -> str:
    if   1  <= day <= 8:  return '1'
    elif 9  <= day <= 15: return '2'
    elif 16 <= day <= 22: return '3'
    else:                 return '4'


# ============================================================================
# STEP 1: Load BNM.REPTDATE and derive macro variables
# ============================================================================
def load_reptdate() -> tuple[date, str, str, str, str, str, str, str]:
    """
    DATA REPTDATE; SET BNM.REPTDATE;
    Derives: NOWK, REPTMON, RDATE, REPTDAY, RMONTH, REPTYEAR, RYEAR.
    Returns (reptdate, nowk, reptmon, rdate, reptday, rmonth, reptyear, ryear).
    """
    df       = pl.read_parquet(BNM_REPTDATE_PATH)
    reptdate: date = df["REPTDATE"][0]

    nowk     = derive_nowk(reptdate.day)
    reptmon  = f"{reptdate.month:02d}"
    rdate    = reptdate.strftime("%d/%m/%Y")   # DDMMYY8.
    reptday  = f"{reptdate.day:02d}"
    rmonth   = reptmon
    reptyear = reptdate.strftime("%y")          # YEAR2.
    ryear    = str(reptdate.year)               # YEAR4.

    log.info("REPTDATE=%s NOWK=%s REPTMON=%s RDATE=%s RYEAR=%s",
             reptdate, nowk, reptmon, rdate, ryear)
    return reptdate, nowk, reptmon, rdate, reptday, rmonth, reptyear, ryear


# ============================================================================
# STEP 2: Load ODFMT / LNFMT (fixed-width product/LOB format files)
# ============================================================================
def load_odfmt() -> pl.DataFrame:
    """
    DATA ODFMT; INFILE ODFMT;
    INPUT @006 PRODUCT 3. @017 PRODUCX $7. @031 LOB $4.;
    ACCTYPE='OD';
    Columns are 1-based positions in SAS; Python uses 0-based slicing.
    """
    rows = []
    with open(ODFMT_PATH, "r", encoding="latin-1") as f:
        for line in f:
            if len(line) < 34:
                continue
            try:
                product = int(line[5:8].strip())   # @006, 3 chars
                producx = line[16:23].strip()       # @017, 7 chars
                lob     = line[30:34].strip()       # @031, 4 chars
                rows.append({"PRODUCT": product, "PRODUCX": producx,
                             "LOB": lob, "ACCTYPE": "OD"})
            except (ValueError, IndexError):
                continue
    return pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={"PRODUCT": pl.Int64, "PRODUCX": pl.Utf8,
                "LOB": pl.Utf8, "ACCTYPE": pl.Utf8}
    )


def load_lnfmt() -> pl.DataFrame:
    """
    DATA LNFMT; INFILE LNFMT;
    INPUT @006 PRODUCT 3. @027 PRODUCX $7. @040 LOB $4.;
    ACCTYPE='LN';
    """
    rows = []
    with open(LNFMT_PATH, "r", encoding="latin-1") as f:
        for line in f:
            if len(line) < 44:
                continue
            try:
                product = int(line[5:8].strip())    # @006, 3 chars
                producx = line[26:33].strip()        # @027, 7 chars
                lob     = line[39:43].strip()        # @040, 4 chars
                rows.append({"PRODUCT": product, "PRODUCX": producx,
                             "LOB": lob, "ACCTYPE": "LN"})
            except (ValueError, IndexError):
                continue
    return pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={"PRODUCT": pl.Int64, "PRODUCX": pl.Utf8,
                "LOB": pl.Utf8, "ACCTYPE": pl.Utf8}
    )


# ============================================================================
# STEP 3: Load driver/branch mapping (CSV)
# ============================================================================
def load_drfmt() -> pl.DataFrame:
    """
    DATA DRFMT(KEEP=BRANCH1 FMLOB);
    INFILE DRFMT DELIMITER=',' DSD;
    INPUT ENTITY $ BRANCH1 $ FMLOB $ FMPROD $;
    Then: BRANCX=BRANCH1; BRANCH2=BRANCX*1; rename BRANCH2→BRANCH1.
    Result: numeric BRANCH1 (int), FMLOB.
    """
    df = pl.read_csv(DRFMT_PATH, has_header=False,
                     new_columns=["ENTITY", "BRANCH1", "FMLOB", "FMPROD"])
    # BRANCX=BRANCH1 (as string), BRANCH2=BRANCX*1 (as int), rename BRANCH2→BRANCH1
    df = df.with_columns(
        pl.col("BRANCH1").cast(pl.Utf8).str.strip_chars()
          .cast(pl.Int64, strict=False).alias("BRANCH1_NUM")
    ).drop("BRANCH1").rename({"BRANCH1_NUM": "BRANCH1"})
    return df.select(["BRANCH1", "FMLOB"]).drop_nulls("BRANCH1")


# ============================================================================
# STEP 4: Build combined PLOB (OD + LN product LOB, dedup by ACCTYPE+PRODUCT)
# ============================================================================
def build_plob(odfmt: pl.DataFrame, lnfmt: pl.DataFrame) -> pl.DataFrame:
    """
    DATA PLOB; SET ODFMT LNFMT;
    PROC SORT DATA=PLOB NODUPKEY; BY ACCTYPE PRODUCT;
    """
    plob = pl.concat([odfmt, lnfmt], how="diagonal")
    return plob.unique(subset=["ACCTYPE", "PRODUCT"], keep="first")\
               .sort(["ACCTYPE", "PRODUCT"])


# ============================================================================
# STEP 5: Filter and prepare ALM (PBB loans)
# ============================================================================
def build_alm(loan_df: pl.DataFrame, rdate: date) -> pl.DataFrame:
    """
    DATA ALM; SET BNM.LOAN&REPTMON&NOWK;
      IF BALANCE=-0.00 THEN DELETE;
      BALX=ROUND(BALANCE,0.01);
      IF BALX IN (0.00,-0.00) THEN XIND='Y';
      IF PAIDIND IN ('P','C') OR XIND='Y' THEN DELETE;
      IF SUBSTR(PRODCD,1,2)='34';
      NOACCT=1;
      IF APPRDATE <= RDATE;
      AGLFLD='PBB ';
    """
    df = loan_df.with_columns([
        pl.col("BALANCE").round(2).alias("BALX"),
        pl.lit(" ").alias("XIND"),
        pl.lit("PBB ").alias("AGLFLD"),
        pl.lit(1).alias("NOACCT"),
    ])
    df = df.with_columns(
        pl.when(pl.col("BALX") == 0.0)
          .then(pl.lit("Y"))
          .otherwise(pl.col("XIND"))
          .alias("XIND")
    )
    df = df.filter(
        ~pl.col("PAIDIND").is_in(["P", "C"]) &
        (pl.col("XIND") != "Y") &
        pl.col("PRODCD").str.starts_with("34") &
        (pl.col("APPRDATE") <= rdate)
    )
    # Explicit check for BALANCE == -0.00 (already handled by BALX==0 above)
    return df


# ============================================================================
# STEP 5b: Merge ALM with PLOB — split matched (ALM) and unmatched (PLOBX)
# ============================================================================
def merge_alm_plob(alm: pl.DataFrame, plob: pl.DataFrame,
                   drfmt: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    DATA ALM PLOBX;
      MERGE PLOB(IN=A) ALM(IN=B); BY ACCTYPE PRODUCT;
      IF B THEN OUTPUT ALM;
      IF (B AND NOT A) THEN OUTPUT PLOBX;

    Then: ALM1 (LOB='PCCC') gets LOB replaced from DRFMT by BRANCH.
    Returns (alm_final, plobx).
    """
    # Full outer join to get IN=A and IN=B indicators
    merged = alm.join(plob, on=["ACCTYPE", "PRODUCT"], how="left", suffix="_PLOB")

    alm_out  = merged.clone()          # IF B → all ALM rows
    plobx    = merged.filter(          # IF B AND NOT A → no match in PLOB
        pl.col("PRODUCX").is_null()
    ).select(["PRODUCT", "PRODUCX", "LOB"])

    # For ALM rows without PLOB match, PRODUCX/LOB may be null — keep as-is
    plobx_dedup = plobx.unique(subset=["PRODUCT"], keep="first").sort("PRODUCT")

    # BRANCH1 = BRANCH; split ALM1 (LOB='PCCC') and ALM2 (others)
    alm_out = alm_out.with_columns(pl.col("BRANCH").alias("BRANCH1"))
    alm1 = alm_out.filter(pl.col("LOB") == "PCCC")
    alm2 = alm_out.filter(pl.col("LOB") != "PCCC")

    # ALM1: merge with DRFMT by BRANCH1; LOB = FMLOB; IF A
    if not alm1.is_empty():
        drfmt_sorted = drfmt.sort("BRANCH1")
        alm1_sorted  = alm1.sort("BRANCH1")
        alm1_merged  = alm1_sorted.join(drfmt_sorted, on="BRANCH1", how="left")
        alm1_merged  = alm1_merged.with_columns(
            pl.col("FMLOB").alias("LOB")
        ).drop("FMLOB")
        alm_final = pl.concat([alm1_merged, alm2], how="diagonal")
    else:
        alm_final = alm2

    return alm_final, plobx_dedup


# ============================================================================
# STEP 6: Build ALM for PIBB (BNMI.LOAN)
# ============================================================================
def build_almi(loan_df: pl.DataFrame, rdate: date) -> pl.DataFrame:
    """
    DATA ALMI; SET BNMI.LOAN&REPTMON&NOWK;
      IF APPRDATE <= RDATE;
      BALX=ROUND(BALANCE,0.01);
      IF BALX IN (0.00,-0.00) THEN XIND='Y';
      IF XIND='Y' THEN DELETE;
      IF SUBSTR(PRODCD,1,2)='34' OR PRODCD='54120';
      IF PAIDIND NE 'P';
      NOACCT=1;
      AGLFLD='PIBB ';
    """
    df = loan_df.with_columns([
        pl.col("BALANCE").round(2).alias("BALX"),
        pl.lit(" ").alias("XIND"),
        pl.lit("PIBB ").alias("AGLFLD"),
        pl.lit(1).alias("NOACCT"),
    ])
    df = df.with_columns(
        pl.when(pl.col("BALX") == 0.0)
          .then(pl.lit("Y"))
          .otherwise(pl.col("XIND"))
          .alias("XIND")
    )
    df = df.filter(
        (pl.col("APPRDATE") <= rdate) &
        (pl.col("XIND") != "Y") &
        (pl.col("PRODCD").str.starts_with("34") | (pl.col("PRODCD") == "54120")) &
        (pl.col("PAIDIND") != "P")
    )
    return df


# ============================================================================
# STEP 7: Build BT / IBT trade bill summary
# ============================================================================
def build_btrad(btrad_df: pl.DataFrame, ibtrad_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA IBTRAD1: BRANCH >= 3000 → BRANCH1 = BRANCH - 3000; rename BRANCH1→BRANCH.
    PROC SORT by ACCTNO DESC APPRLIMT; WHERE DIRCTIND='D' AND CUSTCD NE ' '
              AND APPRLIMT>0 AND ROUND(BALANCE,0.01) NE 0; NODUPKEYS by ACCTNO.
    DATA BTRAD1: SET IBTRAD1 + BTRAD1; AGLFLD; NOACCT=1;
              PRODUCX='LTF_OT  '; LOB='RETL        ';
              IF RETAILID='C' THEN LOB='CORP    ';
    """
    # IBTRAD: adjust BRANCH >= 3000
    ibt = ibtrad_df.with_columns(
        pl.when(pl.col("BRANCH") >= 3000)
          .then(pl.col("BRANCH") - 3000)
          .otherwise(pl.col("BRANCH"))
          .alias("BRANCH"),
        pl.lit("IBTRAD").alias("IND"),
    )
    ibt = (
        ibt.filter(
            (pl.col("DIRCTIND") == "D") &
            (pl.col("CUSTCD") != " ") &
            (pl.col("APPRLIMT") > 0) &
            (pl.col("BALANCE").round(2) != 0)
        )
        .sort(["ACCTNO", "APPRLIMT"], descending=[False, True])
        .unique(subset=["ACCTNO"], keep="first")
    )

    # BT
    bt = (
        btrad_df.filter(
            (pl.col("DIRCTIND") == "D") &
            (pl.col("CUSTCD") != " ") &
            (pl.col("APPRLIMT") > 0) &
            (pl.col("BALANCE").round(2) != 0)
        )
        .sort(["ACCTNO", "APPRLIMT"], descending=[False, True])
        .unique(subset=["ACCTNO"], keep="first")
    )

    # Combine
    bt_combined = pl.concat([ibt, bt], how="diagonal")
    bt_combined = bt_combined.with_columns([
        pl.when(pl.col("IND") == "IBTRAD")
          .then(pl.lit("PIBB"))
          .otherwise(pl.lit("PBB"))
          .alias("AGLFLD"),
        pl.lit(1).alias("NOACCT"),
        pl.lit("LTF_OT  ").alias("PRODUCX"),
        pl.when(pl.col("RETAILID") == "C")
          .then(pl.lit("CORP    "))
          .otherwise(pl.lit("RETL        "))
          .alias("LOB"),
    ])
    return bt_combined


# ============================================================================
# STEP 8: PROC SUMMARY — group by AGLFLD BRANCH PRODUCX LOB, SUM NOACCT
# ============================================================================
def summarise_alm(df: pl.DataFrame) -> pl.DataFrame:
    """PROC SUMMARY DATA=... NWAY MISSING; CLASS AGLFLD BRANCH PRODUCX LOB; VAR NOACCT; SUM="""
    if df.is_empty():
        return pl.DataFrame(schema={
            "AGLFLD": pl.Utf8, "BRANCH": pl.Int64,
            "PRODUCX": pl.Utf8, "LOB": pl.Utf8, "NOACCT": pl.Int64,
        })
    return (
        df.group_by(["AGLFLD", "BRANCH", "PRODUCX", "LOB"])
          .agg(pl.col("NOACCT").sum())
          .sort(["AGLFLD", "BRANCH", "PRODUCX", "LOB"])
    )


# ============================================================================
# STEP 9: Build FM (combined ALMS + ALMSI + BTALM) and format BRANCX
# ============================================================================
def build_fm(alms: pl.DataFrame, almsi: pl.DataFrame,
             btalm: pl.DataFrame, ryear: str, rmonth: str) -> pl.DataFrame:
    """
    DATA FM; SET ALMS ALMSI BTALM;
      IF (800<=BRANCH<=899) THEN BRANCX=PUT(BRANCH,HPCC.);
                            ELSE BRANCX='A'||PUT(BRANCH,Z3.);
      BGLFLD='EXT'; CGLFLD='C_LNS'; DGLFLD='ACTUAL';
      YM=&RYEAR&RMONTH;
    PROC SORT; BY AGLFLD BRANCX PRODUCX;
    DATA LNS; SET FM; CNT+1; NUMAC+NOACCT;
      IF SUBSTR(BRANCX,1,1)='A' THEN BRANCX=SUBSTR(BRANCX,2,3);
      IF SUBSTR(BRANCX,1,1)='H' THEN LOB='HPOP';
    """
    # HPCC. format: branch 800–899 → H + two-digit suffix (H01..H99)
    def hpcc(branch: int) -> str:
        return f"H{branch - 799:02d}"

    parts = [df for df in [alms, almsi, btalm] if not df.is_empty()]
    if not parts:
        return pl.DataFrame()
    fm = pl.concat(parts, how="diagonal")

    ym = int(f"{ryear}{rmonth}")

    result_rows = []
    cnt   = 0
    numac = 0
    for r in fm.sort(["AGLFLD", "BRANCH", "PRODUCX"]).to_dicts():
        branch  = r.get("BRANCH", 0) or 0
        noacct  = r.get("NOACCT", 0) or 0
        lob     = str(r.get("LOB", "") or "")

        # BRANCX derivation
        if 800 <= branch <= 899:
            brancx = hpcc(branch)
        else:
            brancx = f"A{branch:03d}"

        r["BGLFLD"] = "EXT"
        r["CGLFLD"] = "C_LNS"
        r["DGLFLD"] = "ACTUAL"
        r["YM"]     = ym
        r["BRANCX"] = brancx

        # CNT and NUMAC retain (SAS retain statement)
        cnt   += 1
        numac += noacct
        r["CNT"]   = cnt
        r["NUMAC"] = numac

        # If BRANCX starts with 'A' → strip leading 'A'
        if brancx.startswith("A"):
            brancx = brancx[1:]
        # If BRANCX starts with 'H' → LOB = 'HPOP'
        if brancx.startswith("H"):
            lob = "HPOP"

        r["BRANCX"] = brancx
        r["LOB"]    = lob
        result_rows.append(r)

    return pl.DataFrame(result_rows) if result_rows else pl.DataFrame()


# ============================================================================
# STEP 10: Write GLINT output file (LRECL=500, fixed-width)
# ============================================================================
def write_glint(lns: pl.DataFrame, glint_path: Path) -> None:
    """
    DATA LNS; SET LNS END=EOF; FILE GLINT;
    PUT fixed-width record (LRECL=500) with D, record fields, and T trailer.
    Positions are 1-based in SAS; converted to 0-based Python string formatting.
    """
    total_cnt   = int(lns["CNT"].max()  or 0) if not lns.is_empty() else 0
    total_numac = int(lns["NUMAC"].max() or 0) if not lns.is_empty() else 0

    def pad(val, width: int, right: bool = False) -> str:
        s = str(val).strip() if val is not None else ""
        return s.rjust(width) if right else s.ljust(width)

    lines = []
    rows  = lns.to_dicts()
    for i, r in enumerate(rows):
        aglfld  = pad(r.get("AGLFLD",  ""), 32)
        brancx  = pad(r.get("BRANCX",  ""), 32)
        producx = pad(r.get("PRODUCX", ""), 32)
        lob_val = pad(r.get("LOB",     ""), 32)
        ym      = pad(r.get("YM",      ""), 32)
        noacct  = str(r.get("NOACCT",  0) or 0).rjust(15)

        # PUT @001 'D,'
        # @003 AGLFLD @035 ','  → positions 003–034 (32 chars), comma @035
        # @036 'EXT'  @068 ','  → positions 036–067 (32 chars), comma @068
        # @069 CGLFLD @101 ','  → positions 069–100 (32 chars)
        # @102 'ACTUAL'@134 ',' → positions 102–133 (32 chars)
        # @135 'MYR'  @167 ','  → positions 135–166 (32 chars)
        # @168 BRANCX @200 ','  → positions 168–199 (32 chars)
        # @201 LOB    @233 ','  → positions 201–232 (32 chars)
        # @234 PRODUCX@266 ','  → positions 234–265 (32 chars)
        # @267 YM 6.  @299 ','  → positions 267–298 (YM as 6-digit right-justified)
        # @300 NOACCT 15.@315','→ positions 300–314 (15 chars right-justified)
        # @316 'Y,'
        # @318 ' '
        # remainder padded to LRECL

        line = (
            "D,"                                    # @001  2 chars
            + aglfld                                # @003  32 chars → @034
            + ","                                   # @035
            + pad("EXT", 32)                       # @036  32 chars → @067
            + ","                                   # @068
            + pad("C_LNS", 32)                     # @069  32 chars → @100
            + ","                                   # @101
            + pad("ACTUAL", 32)                    # @102  32 chars → @133
            + ","                                   # @134
            + pad("MYR", 32)                       # @135  32 chars → @166
            + ","                                   # @167
            + brancx                               # @168  32 chars → @199
            + ","                                   # @200
            + lob_val                              # @201  32 chars → @232
            + ","                                   # @233
            + producx                              # @234  32 chars → @265
            + ","                                   # @266
            + str(r.get("YM", "")).rjust(6).ljust(32)  # @267  32 chars → @298
            + ","                                   # @299
            + noacct                               # @300  15 chars → @314
            + ","                                   # @315
            + "Y,"                                 # @316  2 chars
            + " "                                  # @318
        )
        # Pad remaining commas at @350, @383, @416
        line = line.ljust(349) + ","
        line = line.ljust(382) + ","
        line = line.ljust(415) + ","
        line = line.ljust(GLINT_LRECL)
        lines.append(line)

    # Trailer record
    trailer = (
        "T,"
        + str(total_cnt).rjust(10)
        + ","
        + str(total_numac).rjust(15)
        + ","
    )
    trailer = trailer.ljust(GLINT_LRECL)
    lines.append(trailer)

    with open(glint_path, "w", encoding="latin-1") as f:
        f.write("\n".join(lines) + "\n")
    log.info("GLINT written: %s  (%d data rows + trailer)", glint_path, len(rows))


# ============================================================================
# STEP 11: Write XMATCH / IMATCH report files
# ============================================================================
def write_xmatch(plobx: pl.DataFrame, path: Path, label: str) -> None:
    """
    DATA REPORT; FILE XMATCH/IMATCH;
      IF TRN=0: 'DATA SUCCESSFULLY MATCHED'
      ELSE: PUT PRODUCT 3. PRODUCX $7. LOB $4. (positional)
    """
    rows = plobx.to_dicts() if not plobx.is_empty() else []
    trn  = len(rows)

    with open(path, "w", encoding="latin-1") as f:
        if trn == 0:
            f.write(f"{'DATA SUCCESSFULLY MATCHED':<500}\n")
        else:
            for r in rows:
                product = str(r.get("PRODUCT", "") or "").rjust(3)   # @001  3 chars
                producx = str(r.get("PRODUCX", "") or "").ljust(7)   # @013  7 chars
                lob_val = str(r.get("LOB",     "") or "").ljust(4)   # @025  4 chars
                line = " " * 0 + product    # @001
                line = line.ljust(12) + producx  # @013
                line = line.ljust(24) + lob_val  # @025
                line = line.ljust(GLINT_LRECL)
                f.write(line + "\n")
    log.info("%s written: %s  (TRN=%d)", label, path, trn)


# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    log.info("EIBM0930 started.")

    # ----------------------------------------------------------------
    # Load REPTDATE
    # ----------------------------------------------------------------
    reptdate, nowk, reptmon, rdate, reptday, rmonth, reptyear, ryear = load_reptdate()
    rdate_dt = reptdate   # already a date object

    # ----------------------------------------------------------------
    # Load format files
    # ----------------------------------------------------------------
    odfmt  = load_odfmt()
    lnfmt  = load_lnfmt()
    drfmt  = load_drfmt()
    plob   = build_plob(odfmt, lnfmt)
    log.info("PLOB rows: %d", len(plob))

    # ----------------------------------------------------------------
    # PBB: BNM.LOAN
    # ----------------------------------------------------------------
    bnm_loan_path = BNM_DATA_DIR / f"LOAN{reptmon}{nowk}.parquet"
    bnm_loan = pl.read_parquet(bnm_loan_path)
    alm_raw  = build_alm(bnm_loan, rdate_dt)
    alm, plobx = merge_alm_plob(alm_raw.sort(["ACCTYPE", "PRODUCT"]), plob, drfmt)
    alms = summarise_alm(alm)
    log.info("ALMS rows: %d  PLOBX rows: %d", len(alms), len(plobx))

    # XMATCH report
    write_xmatch(plobx, XMATCH_PATH, "XMATCH")

    # ----------------------------------------------------------------
    # PIBB: BNMI.LOAN
    # ----------------------------------------------------------------
    bnmi_loan_path = BNMI_DATA_DIR / f"LOAN{reptmon}{nowk}.parquet"
    bnmi_loan = pl.read_parquet(bnmi_loan_path)
    almi_raw  = build_almi(bnmi_loan, rdate_dt)
    almi, plobi = merge_alm_plob(almi_raw.sort(["ACCTYPE", "PRODUCT"]), plob, drfmt)
    almsi = summarise_alm(almi)
    log.info("ALMSI rows: %d  PLOBI rows: %d", len(almsi), len(plobi))

    # IMATCH report
    write_xmatch(plobi, IMATCH_PATH, "IMATCH")

    # ----------------------------------------------------------------
    # BT / IBT trade bills
    # ----------------------------------------------------------------
    btrad_path  = BTBNM_DATA_DIR  / f"BTRAD{reptmon}{nowk}.parquet"
    ibtrad_path = IBTNM_DATA_DIR  / f"IBTRAD{reptmon}{nowk}.parquet"
    btrad_df  = pl.read_parquet(btrad_path)
    ibtrad_df = pl.read_parquet(ibtrad_path)

    btrad1  = build_btrad(btrad_df, ibtrad_df)
    btalm   = summarise_alm(btrad1)
    log.info("BTALM rows: %d", len(btalm))

    # ----------------------------------------------------------------
    # Combine FM → LNS → write GLINT
    # ----------------------------------------------------------------
    lns = build_fm(alms, almsi, btalm, ryear, rmonth)
    log.info("LNS rows (FM combined): %d", len(lns))

    glint_path = OUTPUT_DIR / f"GLINT{reptmon}{nowk}.txt"
    write_glint(lns, glint_path)

    log.info("EIBM0930 completed.")


if __name__ == "__main__":
    main()
